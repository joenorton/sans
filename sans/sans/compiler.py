from __future__ import annotations
from typing import TextIO, Optional, Set, Dict, Any, Tuple, List
import json
import hashlib
from pathlib import Path
from time import perf_counter
from io import StringIO
import csv

from .frontend import detect_refusal, split_statements, segment_blocks, Block
from .preprocessor import preprocess_text, MacroError
from ._loc import Loc
from .recognizer import (
    recognize_data_block,
    recognize_proc_sort_block,
    recognize_proc_transpose_block,
    recognize_proc_sql_block,
    recognize_proc_format_block,
    recognize_proc_summary_block,
)
from .ir import IRDoc, Step, UnknownBlockStep, OpStep, TableFact
from .sans_script.ast import DatasourceDeclaration # New import
from . import __version__ as _engine_version
from .types import type_name, Type


from .hash_utils import compute_artifact_hash, compute_input_hash, compute_report_sha256
from .bundle import ensure_bundle_layout, bundle_relative_path, INPUTS_SOURCE, ARTIFACTS
from .graph import build_graph, write_graph_json
from .lineage import (
    build_var_graph,
    build_table_effects,
    write_vars_graph_json,
    write_table_effects_json,
)
from .sans_script import SansScriptError, lower_script, parse_sans_script
from .sans_script.canon import compute_step_id, compute_transform_id, compute_transform_class_id

def _loc_to_dict(loc) -> Dict[str, Any]:
    return {"file": Path(loc.file).as_posix(), "line_start": loc.line_start, "line_end": loc.line_end}

def _step_to_dict(step: Step) -> Dict[str, Any]:
    if isinstance(step, OpStep):
        t_id = compute_transform_id(step.op, step.params)
        t_class_id = compute_transform_class_id(step.op, step.params)
        return {
            "kind": "op",
            "loc": _loc_to_dict(step.loc),
            "op": step.op,
            "inputs": list(step.inputs),
            "outputs": list(step.outputs),
            "params": step.params,
            "transform_id": t_id,
            "transform_class_id": t_class_id,
            "step_id": compute_step_id(t_id, step.inputs, step.outputs),
        }
    if isinstance(step, UnknownBlockStep):
        return {
            "kind": "block",
            "loc": _loc_to_dict(step.loc),
            "code": step.code,
            "message": step.message,
            "severity": step.severity,
        }
    return {"kind": step.kind, "loc": _loc_to_dict(step.loc)}

def _irdoc_to_dict(doc: IRDoc) -> Dict[str, Any]:
    datasources: Dict[str, Any] = {}
    for name, ds in doc.datasources.items():
        entry: Dict[str, Any] = {"path": ds.path, "columns": ds.columns}
        if ds.column_types:
            entry["column_types"] = {k: type_name(ds.column_types[k]) for k in sorted(ds.column_types)}
        datasources[name] = entry
    return {
        "steps": [_step_to_dict(s) for s in doc.steps],
        "tables": sorted(list(doc.tables)),
        "table_facts": {
            name: {"sorted_by": fact.sorted_by}
            for name, fact in doc.table_facts.items()
        },
        "datasources": datasources,
    }

def _error_to_dict(err: UnknownBlockStep) -> Dict[str, Any]:
    return {
        "code": err.code,
        "message": err.message,
        "severity": err.severity,
        "loc": _loc_to_dict(err.loc),
    }

def _exit_bucket_for_code(code: Optional[str]) -> int:
    if not code:
        return 50
    if code.startswith("SANS_PARSE_") or code.startswith("SANS_BLOCK_"):
        return 30
    if code.startswith("SANS_VALIDATE_"):
        return 31
    if code.startswith("SANS_CAP_"):
        return 32
    if code.startswith("SANS_INTERNAL_"):
        return 50
    return 50

def _status_to_bucket(status: str, primary_code: Optional[str]) -> int:
    if status == "ok":
        return 0
    if status == "ok_warnings":
        return 10
    if status == "refused":
        return _exit_bucket_for_code(primary_code)
    if status == "failed":
        return 50
    return 50

def compile_script(
    text: str,
    file_name: str = "<string>",
    tables: Optional[Set[str]] = None,
    initial_table_facts: Optional[Dict[str, Dict[str, Any]]] = None,
    include_roots: Optional[List[Path]] = None,
    allow_absolute_includes: bool = False,
    allow_include_escape: bool = False,
    legacy_sas: bool = False,
) -> IRDoc:
    """
    Performs compilation of a SANS script into an IRDoc without validation.
    
    Args:
        text: The SANS script content as a string.
        file_name: The name of the file (for location tracking).
        tables: A set of pre-declared table names that exist before script execution.
        initial_table_facts: A dictionary mapping table names to their initial TableFact data (e.g., {"table_name": {"sorted_by": ["col1"]}}).
        
    Returns:
        An IRDoc object representing the compiled Intermediate Representation.
        
    """
    
    # 0. Refuse known-dangerous constructs before any preprocessing (SAS ingestion contract)
    refusal = detect_refusal(text, file_name)
    if refusal:
        tf_objects = {}
        if initial_table_facts:
            for table_name, facts_dict in initial_table_facts.items():
                tf_objects[table_name] = TableFact(**facts_dict)
        table_set = set(tables) if tables else set()
        return IRDoc(
            steps=[
                UnknownBlockStep(
                    code=refusal.code,
                    message=refusal.message,
                    loc=refusal.loc,
                )
            ],
            tables=table_set,
            table_facts=tf_objects,
        )

    # 1. Macro Preprocessing
    try:
        text = preprocess_text(
            text,
            file_name,
            include_roots=include_roots,
            allow_absolute_includes=allow_absolute_includes,
            allow_include_escape=allow_include_escape,
        )
    except MacroError as e:
        err_file = e.file or file_name
        err_line = e.line or 1
        return IRDoc(steps=[
            UnknownBlockStep(
                code="SANS_PARSE_MACRO_ERROR",
                message=str(e),
                loc=Loc(err_file, err_line, err_line),
            )
        ])

    # 2. split_statements()
    statements = list(split_statements(text, file_name))
    
    # 3. segment_blocks()
    blocks = segment_blocks(statements)
    
    # 4. Recognize blocks -> IR steps
    ir_steps: list[Step] = []
    for idx, block in enumerate(blocks):
        if block.kind == "data":
            # recognize_data_block returns a list of steps
            data_steps = recognize_data_block(block, legacy_sas=legacy_sas)
            ir_steps.extend(data_steps)
        elif block.kind == "proc":
            # For now, only 'proc sort' is supported. Refuse others.
            if block.header.text.lower().startswith("proc sort"):
                step = recognize_proc_sort_block(block)
                ir_steps.append(step)
            elif block.header.text.lower().startswith("proc transpose"):
                step = recognize_proc_transpose_block(block)
                ir_steps.append(step)
            elif block.header.text.lower().startswith("proc sql"):
                step = recognize_proc_sql_block(block, legacy_sas=legacy_sas)
                ir_steps.append(step)
            elif block.header.text.lower().startswith("proc format"):
                steps = recognize_proc_format_block(block)
                if isinstance(steps, list):
                    ir_steps.extend(steps)
                else:
                    ir_steps.append(steps)
            elif block.header.text.lower().startswith("proc summary"):
                step = recognize_proc_summary_block(block)
                ir_steps.append(step)
            else:
                proc_stmt_text = block.header.text
                if idx > 0:
                    prev_block = blocks[idx - 1]
                    if prev_block.kind == "data" and prev_block.end is None:
                        proc_stmt_text = f"{proc_stmt_text};"
                ir_steps.append(UnknownBlockStep(
                    code="SANS_PARSE_UNSUPPORTED_PROC",
                    message=(
                        f"Unsupported PROC statement: '{proc_stmt_text}'. "
                        "Hint: supported procs include SORT, TRANSPOSE, SQL, FORMAT, and SUMMARY."
                    ),
                    loc=block.header.loc,
                ))
        elif block.kind == "other":
            # For v0.1, any 'other' statement is an implicit refusal.
            ir_steps.append(UnknownBlockStep(
                code="SANS_PARSE_UNSUPPORTED_STATEMENT",
                message=(
                    f"Unsupported top-level statement: '{block.header.text}'. "
                    "Hint: use DATA steps or PROC SORT only."
                ),
                loc=block.header.loc,
            ))
        else:
            # Should not happen if block.kind is properly constrained
            ir_steps.append(UnknownBlockStep(
                code="SANS_INTERNAL_ERROR",
                message=f"Internal error: Unknown block kind '{block.kind}'",
                loc=block.loc_span,
            ))
    
    # Convert initial_table_facts dict to TableFact objects
    tf_objects: Dict[str, TableFact] = {}
    if initial_table_facts:
        for table_name, facts_dict in initial_table_facts.items():
            tf_objects[table_name] = TableFact(**facts_dict)

    # Initialize IRDoc with steps, pre-declared tables, and initial table facts
    if tables is None:
        return IRDoc(steps=ir_steps, table_facts=tf_objects)
    return IRDoc(steps=ir_steps, tables=tables, table_facts=tf_objects)


def compile_sans_script(
    text: str,
    file_name: str = "<string>",
    tables: Optional[Set[str]] = None,
    initial_table_facts: Optional[Dict[str, Dict[str, Any]]] = None,
    skip_type_validation: bool = False,
) -> IRDoc:
    """
    Compile a .sans script into an IRDoc.
    When skip_type_validation is True, skip validate_script (no type-checking of filter/derive/etc.);
    used for schema lock generation so we can discover datasources without needing typed pinning.
    """
    try:
        script = parse_sans_script(text, file_name)
        if not skip_type_validation:
            from .sans_script.validate import validate_script
            warnings = validate_script(script, tables or set())
        else:
            warnings = []
        steps, references = lower_script(script, file_name)

        # Add warnings as UnknownBlockSteps with warning severity
        for w in warnings:
            steps.append(UnknownBlockStep(
                code=w["code"],
                message=w["message"],
                loc=Loc(file_name, w["line"], w["line"]),
                severity="warning"
            ))
    except SansScriptError as err:
        table_set = set(tables) if tables else set()
        tf_objects: Dict[str, TableFact] = {}
        if initial_table_facts:
            for table_name, facts_dict in initial_table_facts.items():
                tf_objects[table_name] = TableFact(**facts_dict)
        return IRDoc(
            steps=[
                UnknownBlockStep(
                    code=err.code,
                    message=err.message,
                    loc=Loc(file_name, err.line, err.line),
                )
            ],
            tables=table_set,
            table_facts=tf_objects,
        )

    tf_objects: Dict[str, TableFact] = {}
    if initial_table_facts:
        for table_name, facts_dict in initial_table_facts.items():
            tf_objects[table_name] = TableFact(**facts_dict)
    table_set = set(tables) if tables else set()
    table_set.update(references)
    
    from sans.ir import DatasourceDecl

    ir_datasources: dict[str, DatasourceDecl] = {}
    for ast_ds in script.datasources.values():
        if ast_ds.kind == "csv":
            ir_ds = DatasourceDecl(
                kind="csv",
                path=ast_ds.path,
                columns=ast_ds.columns,
                column_types=ast_ds.column_types,
            )
        elif ast_ds.kind == "inline_csv":
            ir_ds = DatasourceDecl(
                kind="inline_csv",
                columns=ast_ds.columns,
                column_types=ast_ds.column_types,
                inline_text=ast_ds.inline_text,
                inline_sha256=ast_ds.inline_sha256,
            )
        else:
            raise AssertionError(f"Unknown datasource kind: {ast_ds.kind}")

        ir_datasources[ast_ds.name] = ir_ds

    return IRDoc(
        steps=steps,
        tables=table_set,
        table_facts=tf_objects,
        datasources=ir_datasources,
    )

def check_script(
    text: str,
    file_name: str = "<string>",
    tables: Optional[Set[str]] = None,
    initial_table_facts: Optional[Dict[str, Dict[str, Any]]] = None,
    include_roots: Optional[List[Path]] = None,
    allow_absolute_includes: bool = False,
    allow_include_escape: bool = False,
    legacy_sas: bool = False,
) -> IRDoc:
    """
    Performs a full compilation check of a SANS script (compile + validate).
    """
    initial_irdoc = compile_script(
        text=text,
        file_name=file_name,
        tables=tables,
        initial_table_facts=initial_table_facts,
        include_roots=include_roots,
        allow_absolute_includes=allow_absolute_includes,
        allow_include_escape=allow_include_escape,
        legacy_sas=legacy_sas,
    )
    validated_table_facts = initial_irdoc.validate()
    return IRDoc(steps=initial_irdoc.steps, tables=initial_irdoc.tables, table_facts=validated_table_facts)

def _referenced_datasource_names(irdoc: Any) -> set:
    """Return set of datasource names referenced by datasource steps (csv or inline_csv)."""
    from .ir import OpStep
    out = set()
    for step in irdoc.steps:
        if not isinstance(step, OpStep) or getattr(step, "op", None) != "datasource":
            continue
        params = getattr(step, "params", None) or {}
        if params.get("kind") not in ("csv", "inline_csv"):
            continue
        name = params.get("name")
        if not name:
            outputs = getattr(step, "outputs", None) or []
            if outputs and outputs[0].startswith("__datasource__"):
                name = outputs[0].replace("__datasource__", "", 1)
        if name:
            out.add(name)
    return out


def _assert_ingress_schemas_strict(irdoc: Any, file_name: str) -> None:
    """
    Enforce that every referenced csv/inline_csv datasource has concrete column types (no UNKNOWN).
    Raises UnknownBlockStep with E_SCHEMA_REQUIRED or E_SCHEMA_LOCK_INVALID before expression typing.
    """
    from .ir import Loc
    referenced = _referenced_datasource_names(irdoc)
    for name in sorted(referenced):
        ds = irdoc.datasources.get(name) if irdoc.datasources else None
        if not ds or ds.kind not in ("csv", "inline_csv"):
            continue
        if not ds.column_types:
            raise UnknownBlockStep(
                code="E_SCHEMA_REQUIRED",
                message=(
                    f"Datasource '{name}' has no typed columns. "
                    "Provide --schema-lock with an entry for this datasource or pin columns in the declaration."
                ),
                loc=Loc(file_name, 1, 1),
            )
        unknown_cols = [c for c, t in ds.column_types.items() if t == Type.UNKNOWN]
        if unknown_cols:
            raise UnknownBlockStep(
                code="E_SCHEMA_LOCK_INVALID",
                message=(
                    f"Datasource '{name}' has unknown column type(s): {', '.join(sorted(unknown_cols))}. "
                    "Lock and pinned columns must have concrete types (int, decimal, string, bool, date)."
                ),
                loc=Loc(file_name, 1, 1),
            )


def emit_check_artifacts(
    text: str,
    file_name: str = "<string>",
    tables: Optional[Set[str]] = None,
    initial_table_facts: Optional[Dict[str, Dict[str, Any]]] = None,
    out_dir: str | Path = ".",
    plan_name: str = "plan.ir.json",
    report_name: str = "report.json",
    strict: bool = True,
    allow_approx: bool = False,
    tolerance: Optional[Dict[str, Any]] = None,
    include_roots: Optional[List[Path]] = None,
    allow_absolute_includes: bool = False,
    allow_include_escape: bool = False,
    emit_vars_graph: bool = True,
    legacy_sas: bool = False,
    lock_generation_only: bool = False,
    schema_lock: Optional[Dict[str, Any]] = None,
    schema_lock_path_resolved: Optional[Path] = None,
) -> Tuple[IRDoc, Dict[str, Any]]:
    """
    Compile + validate, then emit plan and report artifacts.
    Returns the IRDoc (validated if possible) and report dict.
    When lock_generation_only is True: skip type validation and irdoc.validate() so we can get
    an IRDoc for schema lock generation without needing typed datasources; also skip infer_table_schema_types.
    """
    out_path = Path(out_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)
    ensure_bundle_layout(out_path)
    use_sans_script = Path(file_name).suffix.lower() == ".sans"

    if not use_sans_script:
        try:
            processed_text = preprocess_text(
                text,
                file_name,
                include_roots=include_roots,
                allow_absolute_includes=allow_absolute_includes,
                allow_include_escape=allow_include_escape,
            )
            (out_path / INPUTS_SOURCE / "preprocessed.sas").write_text(processed_text, encoding="utf-8")
        except MacroError:
            # We allow compilation to handle the error and report it in the IR
            pass

    compile_start = perf_counter()
    # When schema_lock is provided we will enrich irdoc after compile; skip AST type validation
    # so we don't fail on untyped datasources before enrichment, then irdoc.validate() will type-check.
    skip_ast_type_validation = lock_generation_only or (schema_lock is not None)
    if use_sans_script:
        irdoc = compile_sans_script(
            text=text,
            file_name=file_name,
            tables=tables,
            initial_table_facts=initial_table_facts,
            skip_type_validation=skip_ast_type_validation,
        )
    else:
        irdoc = compile_script(
            text=text,
            file_name=file_name,
            tables=tables,
            initial_table_facts=initial_table_facts,
            include_roots=include_roots,
            allow_absolute_includes=allow_absolute_includes,
            allow_include_escape=allow_include_escape,
            legacy_sas=legacy_sas,
        )
    compile_ms = int((perf_counter() - compile_start) * 1000)

    validation_error: Optional[UnknownBlockStep] = None
    validate_ms: Optional[int] = None
    diagnostics: List[Dict[str, Any]] = []
    schema_lock_applied: List[str] = []
    schema_lock_missing: List[str] = []

    # Apply schema lock to compile-time type env when provided (so filter/derive get correct types).
    if schema_lock is not None and not lock_generation_only and use_sans_script:
        from .ir import DatasourceDecl
        from .schema_lock import lock_by_name, lock_entry_to_column_types
        from .ir import Loc
        referenced = _referenced_datasource_names(irdoc)
        lock_map = lock_by_name(schema_lock)
        # Ungated refs: referenced datasources that have no pinned column_types
        untyped_refs = [
            n for n in referenced
            if irdoc.datasources.get(n) and not (irdoc.datasources[n].column_types)
        ]
        missing = [n for n in untyped_refs if n not in lock_map]
        if missing:
            validation_error = UnknownBlockStep(
                code="E_SCHEMA_LOCK_MISSING_DS",
                message=f"Schema lock missing datasource(s): {', '.join(sorted(missing))}",
                loc=Loc(file_name, 1, 1),
            )
            schema_lock_missing = sorted(missing)
        else:
            # Enrich irdoc.datasources with lock types; reject any lock entry with UNKNOWN column type
            invalid_lock_ds: Optional[str] = None
            invalid_cols: List[str] = []
            new_datasources: Dict[str, Any] = {}
            for name, ds in irdoc.datasources.items():
                if ds.column_types:
                    new_datasources[name] = ds
                elif name in lock_map:
                    entry = lock_map[name]
                    col_types = lock_entry_to_column_types(entry)
                    unknown_in_lock = [c for c, t in col_types.items() if t == Type.UNKNOWN]
                    if unknown_in_lock:
                        invalid_lock_ds = name
                        invalid_cols = sorted(unknown_in_lock)
                        break
                    lock_cols = [c["name"] for c in (entry.get("columns") or []) if c.get("name")]
                    columns = ds.columns if ds.columns else (lock_cols if lock_cols else None)
                    new_datasources[name] = DatasourceDecl(
                        kind=ds.kind,
                        path=ds.path,
                        columns=columns,
                        column_types=col_types,
                        inline_text=ds.inline_text,
                        inline_sha256=ds.inline_sha256,
                    )
                    schema_lock_applied.append(name)
                else:
                    new_datasources[name] = ds
            if invalid_lock_ds is not None:
                validation_error = UnknownBlockStep(
                    code="E_SCHEMA_LOCK_INVALID",
                    message=(
                        f"Schema lock datasource '{invalid_lock_ds}' has unknown column type(s): {', '.join(invalid_cols)}. "
                        "Lock columns must have concrete types (int, decimal, string, bool, date)."
                    ),
                    loc=Loc(file_name, 1, 1),
                )
            else:
                irdoc = IRDoc(
                    steps=irdoc.steps,
                    tables=irdoc.tables,
                    table_facts=irdoc.table_facts,
                    datasources=new_datasources,
                )
                schema_lock_applied = sorted(schema_lock_applied)

    # Ingress assertion: no run may proceed with UNKNOWN or uncovered datasources (before expression typing).
    if (
        validation_error is None
        and not lock_generation_only
        and strict
        and use_sans_script
    ):
        try:
            _assert_ingress_schemas_strict(irdoc, file_name)
        except UnknownBlockStep as err:
            validation_error = err

    if lock_generation_only:
        # No irdoc.validate() or type inference; we only need irdoc for datasource discovery
        validate_ms = 0
    elif validation_error is not None:
        validate_ms = 0
    elif strict:
        validate_start = perf_counter()
        try:
            validated_table_facts = irdoc.validate()
            irdoc = IRDoc(
                steps=irdoc.steps,
                tables=irdoc.tables,
                table_facts=validated_table_facts,
                datasources=irdoc.datasources,
            )
        except UnknownBlockStep as err:
            validation_error = err
        validate_ms = int((perf_counter() - validate_start) * 1000)
    else:
        # Validate only op steps to compute facts, but allow unknown blocks.
        op_steps = [s for s in irdoc.steps if isinstance(s, OpStep)]
        validate_start = perf_counter()
        try:
            validated_table_facts = IRDoc(
                steps=op_steps,
                tables=irdoc.tables,
                table_facts=irdoc.table_facts,
                datasources=irdoc.datasources,
            ).validate()
            irdoc = IRDoc(
                steps=irdoc.steps,
                tables=irdoc.tables,
                table_facts=validated_table_facts,
                datasources=irdoc.datasources,
            )

        except UnknownBlockStep as err:
            validation_error = err
        validate_ms = int((perf_counter() - validate_start) * 1000)
        for step in irdoc.steps:
            if isinstance(step, UnknownBlockStep):
                diagnostics.append(_error_to_dict(step))

    plan_path = out_path / ARTIFACTS / plan_name
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps(_irdoc_to_dict(irdoc), indent=2), encoding="utf-8")

    graph_path = out_path / ARTIFACTS / "graph.json"
    graph = build_graph(irdoc, producer={"name": "sans", "version": _engine_version})
    write_graph_json(graph, graph_path)

    initial_schema: Dict[str, List[str]] = {}
    for name, ds in irdoc.datasources.items():
        cols = ds.columns
        if not cols and ds.kind == "inline_csv" and ds.inline_text:
            reader = csv.reader(StringIO(ds.inline_text.strip()))
            try:
                cols = next(reader)
            except StopIteration:
                cols = []
        if cols:
            initial_schema[f"__datasource__{name}"] = list(cols)
    vars_graph_path = out_path / ARTIFACTS / "vars.graph.json"
    if emit_vars_graph:
        vars_graph = build_var_graph(irdoc, initial_schema=initial_schema)
    else:
        vars_graph = {"nodes": [], "edges": []}
    write_vars_graph_json(vars_graph, vars_graph_path)
    effects_path = out_path / ARTIFACTS / "table.effects.json"
    effects = build_table_effects(irdoc)
    write_table_effects_json(effects, effects_path)
    if lock_generation_only:
        schema_payload = {"schema_version": "0.1", "tables": {}}
    else:
        from sans.type_infer import infer_table_schema_types, schema_to_strings
        schema_types = infer_table_schema_types(irdoc)
        schema_tables = {}
        for name in sorted(schema_types.keys()):
            schema_tables[name] = schema_to_strings(schema_types[name])
        schema_payload = {"schema_version": "0.1", "tables": schema_tables}
    schema_path = out_path / ARTIFACTS / "schema.evidence.json"
    schema_path.write_text(json.dumps(schema_payload, indent=2, sort_keys=True), encoding="utf-8")

    source_basename = Path(file_name).name or "script"
    source_dest = out_path / INPUTS_SOURCE / source_basename
    source_path = Path(file_name)
    if file_name and source_path.exists() and source_path.is_file():
        source_dest.write_bytes(source_path.read_bytes())
    else:
        source_dest.write_text(text, encoding="utf-8")

    report_path = out_path / report_name

    primary_error: Optional[Dict[str, Any]] = None
    status = "ok"
    if validation_error:
        # In non-strict mode, allow parse/unknown blocks as warnings.
        if not strict and not validation_error.code.startswith("SANS_VALIDATE_"):
            status = "ok_warnings"
            diagnostics.append(_error_to_dict(validation_error))
        else:
            status = "refused"
            primary_error = _error_to_dict(validation_error)
            diagnostics = [primary_error]
    elif diagnostics:
        status = "ok_warnings"

    plan_rel = bundle_relative_path(plan_path, out_path)
    graph_rel = bundle_relative_path(graph_path, out_path)
    vars_graph_rel = bundle_relative_path(vars_graph_path, out_path)
    effects_rel = bundle_relative_path(effects_path, out_path)
    schema_rel = bundle_relative_path(schema_path, out_path)
    source_rel = bundle_relative_path(source_dest, out_path)
    inputs_list: List[Dict[str, Any]] = [
        {"role": "source", "name": source_basename, "path": source_rel, "sha256": compute_input_hash(source_dest) or ""}
    ]
    preprocessed_path = out_path / INPUTS_SOURCE / "preprocessed.sas"
    if preprocessed_path.exists():
        preprocessed_rel = bundle_relative_path(preprocessed_path, out_path)
        h = compute_input_hash(preprocessed_path)
        if h:
            inputs_list.append({"role": "preprocessed", "name": "preprocessed.sas", "path": preprocessed_rel, "sha256": h})

    report: Dict[str, Any] = {
        "report_schema_version": "0.3",
        "status": status,
        "exit_code_bucket": _status_to_bucket(status, primary_error["code"] if primary_error else None),
        "primary_error": primary_error,
        "diagnostics": diagnostics,
        "inputs": inputs_list,
        "artifacts": [
            {"name": plan_name, "path": plan_rel, "sha256": compute_artifact_hash(plan_path) or ""},
            {"name": "graph.json", "path": graph_rel, "sha256": compute_artifact_hash(graph_path) or ""},
            {"name": "vars.graph.json", "path": vars_graph_rel, "sha256": compute_artifact_hash(vars_graph_path) or ""},
            {"name": "table.effects.json", "path": effects_rel, "sha256": compute_artifact_hash(effects_path) or ""},
            {"name": "schema.evidence.json", "path": schema_rel, "sha256": compute_artifact_hash(schema_path) or ""},
        ],
        "outputs": [],
        "plan_path": plan_rel,
        "engine": {"name": "sans", "version": _engine_version},
        "settings": {
            "strict": strict,
            "allow_approx": allow_approx,
            "tolerance": tolerance,
            "tables": sorted(list(tables)) if tables else [],
            "datasources": sorted(list(irdoc.datasources.keys())),
        },
        "timing": {
            "compile_ms": compile_ms,
            "validate_ms": validate_ms,
            "execute_ms": None,
        },
    }

    if schema_lock_path_resolved is not None:
        report["schema_lock_used_path"] = str(Path(schema_lock_path_resolved).resolve())
        from .schema_lock import compute_lock_sha256
        report["schema_lock_sha256"] = compute_lock_sha256(schema_lock) if schema_lock else None
        report["schema_lock_applied_datasources"] = schema_lock_applied
        report["schema_lock_missing_datasources"] = schema_lock_missing

    report["report_sha256"] = compute_report_sha256(report, out_path)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return irdoc, report
