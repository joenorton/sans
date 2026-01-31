from __future__ import annotations
from typing import TextIO, Optional, Set, Dict, Any, Tuple, List
import json
import hashlib
from pathlib import Path
from time import perf_counter

from .frontend import detect_refusal, split_statements, segment_blocks, Block
from .recognizer import recognize_data_block, recognize_proc_sort_block, recognize_proc_transpose_block, recognize_proc_sql_block
from .ir import IRDoc, Step, UnknownBlockStep, OpStep, TableFact
from . import __version__ as _engine_version


def _loc_to_dict(loc) -> Dict[str, Any]:
    return {"file": loc.file, "line_start": loc.line_start, "line_end": loc.line_end}

def _step_to_dict(step: Step) -> Dict[str, Any]:
    if isinstance(step, OpStep):
        return {
            "kind": "op",
            "loc": _loc_to_dict(step.loc),
            "op": step.op,
            "inputs": list(step.inputs),
            "outputs": list(step.outputs),
            "params": step.params,
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
    }

def _error_to_dict(err: UnknownBlockStep) -> Dict[str, Any]:
    return {
        "code": err.code,
        "message": err.message,
        "severity": err.severity,
        "loc": _loc_to_dict(err.loc),
    }

def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _sha256_path(path: Path) -> Optional[str]:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data).hexdigest()

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
    
    # 1. detect_refusal() - Early exit for known dangerous constructs
    refusal = detect_refusal(text, file_name)
    if refusal:
        steps: list[Step] = [
            UnknownBlockStep(
                code=refusal.code,
                message=refusal.message,
                loc=refusal.loc,
            )
        ]
        # Convert initial_table_facts dict to TableFact objects
        tf_objects: Dict[str, TableFact] = {}
        if initial_table_facts:
            for table_name, facts_dict in initial_table_facts.items():
                tf_objects[table_name] = TableFact(**facts_dict)
        if tables is None:
            return IRDoc(steps=steps, table_facts=tf_objects)
        return IRDoc(steps=steps, tables=tables, table_facts=tf_objects)
        
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
            else:
                proc_stmt_text = block.header.text
                if idx > 0:
                    prev_block = blocks[idx - 1]
                    if prev_block.kind == "data" and prev_block.end is None:
                        proc_stmt_text = f"{proc_stmt_text};"
                ir_steps.append(UnknownBlockStep(
                    code="SANS_PARSE_UNSUPPORTED_PROC",
                    message=f"Unsupported PROC statement: '{proc_stmt_text}'. Hint: only PROC SORT is supported.",
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

def check_script(
    text: str,
    file_name: str = "<string>",
    tables: Optional[Set[str]] = None,
    initial_table_facts: Optional[Dict[str, Dict[str, Any]]] = None,
) -> IRDoc:
    """
    Performs a full compilation check of a SANS script (compile + validate).
    """
    initial_irdoc = compile_script(
        text=text,
        file_name=file_name,
        tables=tables,
        initial_table_facts=initial_table_facts,
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
) -> Tuple[IRDoc, Dict[str, Any]]:
    """
    Compile + validate, then emit plan and report artifacts.
    Returns the IRDoc (validated if possible) and report dict.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    compile_start = perf_counter()
    irdoc = compile_script(
        text=text,
        file_name=file_name,
        tables=tables,
        initial_table_facts=initial_table_facts,
    )
    compile_ms = int((perf_counter() - compile_start) * 1000)

    validation_error: Optional[UnknownBlockStep] = None
    validate_ms: Optional[int] = None
    diagnostics: List[Dict[str, Any]] = []

    if strict:
        validate_start = perf_counter()
        try:
            validated_table_facts = irdoc.validate()
            irdoc = IRDoc(steps=irdoc.steps, tables=irdoc.tables, table_facts=validated_table_facts)
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
            ).validate()
            irdoc = IRDoc(steps=irdoc.steps, tables=irdoc.tables, table_facts=validated_table_facts)
        except UnknownBlockStep as err:
            validation_error = err
        validate_ms = int((perf_counter() - validate_start) * 1000)
        for step in irdoc.steps:
            if isinstance(step, UnknownBlockStep):
                diagnostics.append(_error_to_dict(step))

    plan_path = out_path / plan_name
    report_path = out_path / report_name

    plan_path.write_text(json.dumps(_irdoc_to_dict(irdoc), indent=2), encoding="utf-8")

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

    report: Dict[str, Any] = {
        "status": status,
        "exit_code_bucket": _status_to_bucket(status, primary_error["code"] if primary_error else None),
        "primary_error": primary_error,
        "diagnostics": diagnostics,
        "inputs": [
            {"path": file_name, "sha256": _sha256_text(text)}
        ],
        "outputs": [
            {"path": str(plan_path), "sha256": _sha256_path(plan_path)},
            {"path": str(report_path), "sha256": None},
        ],
        "plan_path": str(plan_path),
        "engine": {"name": "sans", "version": _engine_version},
        "settings": {
            "strict": strict,
            "allow_approx": allow_approx,
            "tolerance": tolerance,
            "tables": sorted(list(tables)) if tables else [],
        },
        "timing": {
            "compile_ms": compile_ms,
            "validate_ms": validate_ms,
        },
    }

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report_sha = _sha256_path(report_path)
    if report_sha:
        report["outputs"][1]["sha256"] = report_sha
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return irdoc, report
