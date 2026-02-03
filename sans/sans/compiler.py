from __future__ import annotations
from typing import TextIO, Optional, Set, Dict, Any, Tuple, List
import json
import hashlib
from pathlib import Path
from time import perf_counter

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


from .hash_utils import compute_artifact_hash, compute_input_hash, compute_report_sha256
from .bundle import ensure_bundle_layout, bundle_relative_path, INPUTS_SOURCE, ARTIFACTS
from .graph import build_graph, write_graph_json
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
    return {
        "steps": [_step_to_dict(s) for s in doc.steps],
        "tables": sorted(list(doc.tables)),
        "table_facts": {
            name: {"sorted_by": fact.sorted_by}
            for name, fact in doc.table_facts.items()
        },
        "datasources": {
            name: {"path": ds.path, "columns": ds.columns}
            for name, ds in doc.datasources.items()
        }
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
            data_steps = recognize_data_block(block)
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
                step = recognize_proc_sql_block(block)
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
) -> IRDoc:
    """
    Compile a .sans script into an IRDoc.
    """
    try:
        script = parse_sans_script(text, file_name)
        from .sans_script.validate import validate_script
        warnings = validate_script(script, tables or set())
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
            )
        elif ast_ds.kind == "inline_csv":
            ir_ds = DatasourceDecl(
                kind="inline_csv",
                columns=ast_ds.columns,
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
    )
    validated_table_facts = initial_irdoc.validate()
    return IRDoc(steps=initial_irdoc.steps, tables=initial_irdoc.tables, table_facts=validated_table_facts)

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
) -> Tuple[IRDoc, Dict[str, Any]]:
    """
    Compile + validate, then emit plan and report artifacts.
    Returns the IRDoc (validated if possible) and report dict.
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
    if use_sans_script:
        irdoc = compile_sans_script(
            text=text,
            file_name=file_name,
            tables=tables,
            initial_table_facts=initial_table_facts,
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
        )
    compile_ms = int((perf_counter() - compile_start) * 1000)

    validation_error: Optional[UnknownBlockStep] = None
    validate_ms: Optional[int] = None
    diagnostics: List[Dict[str, Any]] = []

    if strict:
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

    report["report_sha256"] = compute_report_sha256(report, out_path)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return irdoc, report
