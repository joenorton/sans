from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import csv
from time import perf_counter

from .ir import IRDoc, OpStep, Step, UnknownBlockStep
from .compiler import emit_check_artifacts
import json


@dataclass
class RuntimeDiagnostic:
    code: str
    message: str
    loc: Optional[Dict[str, Any]] = None
    tokens: Optional[List[str]] = None
    hint: Optional[str] = None


@dataclass
class ExecutionResult:
    status: str  # "ok" | "failed"
    diagnostics: List[RuntimeDiagnostic]
    outputs: List[Dict[str, Any]]
    execute_ms: Optional[int]


class RuntimeFailure(Exception):
    def __init__(self, code: str, message: str, loc: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.loc = loc


def _loc_to_dict(loc) -> Dict[str, Any]:
    return {"file": loc.file, "line_start": loc.line_start, "line_end": loc.line_end}


def _parse_value(raw: str) -> Any:
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return []
        rows: List[Dict[str, Any]] = []
        for row in reader:
            row_dict: Dict[str, Any] = {}
            for i, col in enumerate(headers):
                row_dict[col] = _parse_value(row[i]) if i < len(row) else None
            rows.append(row_dict)
        return rows




def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    headers = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(["" if row.get(h) is None else row.get(h) for h in headers])


def _eval_expr(node: Dict[str, Any], row: Dict[str, Any]) -> Any:
    node_type = node.get("type")
    if node_type == "lit":
        return node.get("value")
    if node_type == "col":
        return row.get(node.get("name"))
    if node_type == "binop":
        op = node.get("op")
        left = _eval_expr(node.get("left"), row)
        right = _eval_expr(node.get("right"), row)
        if op in {"+", "-", "*", "/"}:
            if left is None or right is None:
                return None
            if op == "+":
                return left + right
            if op == "-":
                return left - right
            if op == "*":
                return left * right
            if op == "/":
                return left / right
        if op in {"=", "!=", "<", "<=", ">", ">="}:
            if left is None or right is None:
                return False
            if op == "=":
                return left == right
            if op == "!=":
                return left != right
            if op == "<":
                return left < right
            if op == "<=":
                return left <= right
            if op == ">":
                return left > right
            if op == ">=":
                return left >= right
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported binary operator '{op}'",
        )
    if node_type == "boolop":
        op = node.get("op")
        args = node.get("args") or []
        if op == "and":
            return all(bool(_eval_expr(a, row)) for a in args)
        if op == "or":
            return any(bool(_eval_expr(a, row)) for a in args)
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported boolean operator '{op}'",
        )
    if node_type == "unop":
        op = node.get("op")
        arg = _eval_expr(node.get("arg"), row)
        if op == "not":
            return not bool(arg)
        if op == "+":
            return +arg if arg is not None else None
        if op == "-":
            return -arg if arg is not None else None
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported unary operator '{op}'",
        )
    raise RuntimeFailure(
        "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
        f"Unsupported expression node type '{node_type}'",
    )


def execute_plan(ir_doc: IRDoc, bindings: Dict[str, str], out_dir: Path) -> ExecutionResult:
    start = perf_counter()
    diagnostics: List[RuntimeDiagnostic] = []
    outputs: List[Dict[str, Any]] = []

    try:
        tables: Dict[str, List[Dict[str, Any]]] = {}
        for name, path_str in bindings.items():
            path = Path(path_str)
            if not path.exists():
                raise RuntimeFailure(
                    "SANS_RUNTIME_INPUT_NOT_FOUND",
                    f"Input table '{name}' file not found: {path_str}",
                )
            tables[name] = _load_csv(path)

        for step in ir_doc.steps:
            if isinstance(step, UnknownBlockStep):
                raise RuntimeFailure(
                    "SANS_CAP_UNSUPPORTED_OP",
                    f"Unsupported block in runtime: {step.code}",
                    _loc_to_dict(step.loc),
                )
            if not isinstance(step, OpStep):
                raise RuntimeFailure(
                    "SANS_CAP_UNSUPPORTED_OP",
                    f"Unsupported step kind '{step.kind}'",
                    _loc_to_dict(step.loc),
                )

            op = step.op
            if op not in {"identity", "compute", "filter", "select", "rename"}:
                raise RuntimeFailure(
                    "SANS_CAP_UNSUPPORTED_OP",
                    f"Unsupported operation '{op}'",
                    _loc_to_dict(step.loc),
                )

            if not step.inputs:
                raise RuntimeFailure(
                    "SANS_RUNTIME_TABLE_UNDEFINED",
                    f"Operation '{op}' has no input table.",
                    _loc_to_dict(step.loc),
                )
            input_table = step.inputs[0]
            if input_table not in tables:
                raise RuntimeFailure(
                    "SANS_RUNTIME_TABLE_UNDEFINED",
                    f"Input table '{input_table}' not bound at runtime.",
                    _loc_to_dict(step.loc),
                )

            input_rows = tables[input_table]

            if op == "identity":
                output_rows = [dict(r) for r in input_rows]
            elif op == "compute":
                assigns = step.params.get("assign") or []
                output_rows = []
                for row in input_rows:
                    new_row = dict(row)
                    for assign in assigns:
                        col_name = assign.get("col")
                        expr = assign.get("expr")
                        new_row[col_name] = _eval_expr(expr, new_row)
                    output_rows.append(new_row)
            elif op == "filter":
                predicate = step.params.get("predicate")
                output_rows = []
                for row in input_rows:
                    keep = _eval_expr(predicate, row)
                    if bool(keep):
                        output_rows.append(dict(row))
            elif op == "select":
                keep = step.params.get("keep") or []
                drop = step.params.get("drop") or []
                output_rows = []
                for row in input_rows:
                    if keep:
                        new_row = {k: row.get(k) for k in keep}
                    else:
                        new_row = {k: v for k, v in row.items() if k not in drop}
                    output_rows.append(new_row)
            elif op == "rename":
                rename_map = step.params.get("map") or {}
                output_rows = []
                for row in input_rows:
                    new_row: Dict[str, Any] = {}
                    for key, value in row.items():
                        new_key = rename_map.get(key, key)
                        new_row[new_key] = value
                    output_rows.append(new_row)
            else:
                raise RuntimeFailure(
                    "SANS_CAP_UNSUPPORTED_OP",
                    f"Unsupported operation '{op}'",
                    _loc_to_dict(step.loc),
                )

            for output in step.outputs:
                tables[output] = output_rows

        # Determine terminal outputs (not consumed by any later step)
        produced = set()
        consumed = set()
        for step in ir_doc.steps:
            if isinstance(step, OpStep):
                produced.update(step.outputs)
                consumed.update(step.inputs)
        terminal_tables = [t for t in produced if t not in consumed]

        out_dir.mkdir(parents=True, exist_ok=True)
        for table_name in terminal_tables:
            out_path = out_dir / f"{table_name}.csv"
            table_rows = tables.get(table_name, [])
            _write_csv(out_path, table_rows)
            columns: List[str] = []
            if table_rows:
                columns = list(table_rows[0].keys())
            outputs.append(
                {
                    "table": table_name,
                    "path": str(out_path),
                    "rows": len(table_rows),
                    "columns": columns,
                }
            )

        return ExecutionResult(
            status="ok",
            diagnostics=[],
            outputs=outputs,
            execute_ms=int((perf_counter() - start) * 1000),
        )

    except RuntimeFailure as err:
        diagnostics.append(RuntimeDiagnostic(code=err.code, message=err.message, loc=err.loc))
        return ExecutionResult(
            status="failed",
            diagnostics=diagnostics,
            outputs=outputs,
            execute_ms=int((perf_counter() - start) * 1000),
        )


def run_script(
    text: str,
    file_name: str,
    bindings: Dict[str, str],
    out_dir: Path,
    strict: bool = True,
) -> Dict[str, Any]:
    irdoc, report = emit_check_artifacts(
        text=text,
        file_name=file_name,
        tables=set(bindings.keys()) if bindings else None,
        out_dir=out_dir,
        strict=strict,
    )

    # If compilation/validation refused, annotate runtime and exit.
    if report.get("status") == "refused":
        report["runtime"] = {"status": "refused", "outputs": [], "timing": {"execute_ms": None}}
        report_path = Path(out_dir) / "report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    result = execute_plan(irdoc, bindings, Path(out_dir))

    report["runtime"] = {
        "status": result.status,
        "outputs": result.outputs,
        "timing": {"execute_ms": result.execute_ms},
    }
    report["timing"]["execute_ms"] = result.execute_ms

    if result.status == "failed":
        report["status"] = "failed"
        report["exit_code_bucket"] = 40
        if result.diagnostics:
            primary = result.diagnostics[0]
            report["primary_error"] = {
                "code": primary.code,
                "message": primary.message,
                "loc": primary.loc,
            }
            report["diagnostics"] = [
                {"code": d.code, "message": d.message, "loc": d.loc}
                for d in result.diagnostics
            ]
    else:
        report["status"] = "ok"
        report["exit_code_bucket"] = 0
        report["primary_error"] = None

    # Extend outputs with runtime outputs and hashes if available.
    from .compiler import _sha256_path
    for out in result.outputs:
        path = Path(out["path"])
        report["outputs"].append(
            {"path": str(path), "sha256": _sha256_path(path)}
        )

    report_path = Path(out_dir) / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
