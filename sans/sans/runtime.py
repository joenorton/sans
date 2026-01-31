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
    if raw.isdigit() and len(raw) > 1 and raw.startswith("0"):
        return raw
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def _sort_key_value(value: Any) -> tuple[int, Any]:
    if value is None:
        return (1, None)
    return (0, value)


def _normalize_sort_by(by_spec: Any) -> list[tuple[str, bool]]:
    if not by_spec:
        return []
    if isinstance(by_spec, list) and by_spec and isinstance(by_spec[0], dict):
        return [(item.get("col"), bool(item.get("asc", True))) for item in by_spec]
    return [(col, True) for col in by_spec]


def _sort_rows(rows: List[Dict[str, Any]], by_spec: Any) -> List[Dict[str, Any]]:
    by_cols = _normalize_sort_by(by_spec)
    if not by_cols:
        return [dict(r) for r in rows]
    for _, asc in by_cols:
        if not asc:
            raise RuntimeFailure(
                "SANS_RUNTIME_SORT_UNSUPPORTED",
                "Descending sort is not supported in v0.1 runtime.",
            )
    def sort_key(row: Dict[str, Any]):
        return tuple(_sort_key_value(row.get(col)) for col, _ in by_cols)
    return sorted([dict(r) for r in rows], key=sort_key)


def _check_sorted(rows: List[Dict[str, Any]], by_cols: list[str]) -> bool:
    if not by_cols or len(rows) < 2:
        return True
    prev_key = None
    for row in rows:
        key = tuple(row.get(col) for col in by_cols)
        if prev_key is not None and tuple(_sort_key_value(v) for v in key) < tuple(_sort_key_value(v) for v in prev_key):
            return False
        prev_key = key
    return True


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


def _compute_by_flags(by_vars: list[str], prev_key: Optional[tuple[Any, ...]], curr_key: tuple[Any, ...], next_key: Optional[tuple[Any, ...]]) -> dict[str, bool]:
    flags: dict[str, bool] = {}
    for idx, var in enumerate(by_vars):
        first = prev_key is None or prev_key[:idx + 1] != curr_key[:idx + 1]
        last = next_key is None or next_key[:idx + 1] != curr_key[:idx + 1]
        flags[f"first.{var}"] = first
        flags[f"last.{var}"] = last
    return flags


def _execute_data_step(step: OpStep, tables: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    params = step.params or {}
    mode = params.get("mode")
    input_specs = params.get("inputs") or []
    by_vars = params.get("by") or []
    retain_vars = params.get("retain") or []
    keep_vars = params.get("keep") or []
    statements = params.get("statements") or []
    explicit_output = bool(params.get("explicit_output"))

    if not input_specs:
        raise RuntimeFailure(
            "SANS_RUNTIME_TABLE_UNDEFINED",
            "DATA step has no input tables.",
            _loc_to_dict(step.loc),
        )

    input_tables = [spec.get("table") for spec in input_specs]
    input_flags = {spec.get("table"): spec.get("in") for spec in input_specs}

    for table_name in input_tables:
        if table_name not in tables:
            raise RuntimeFailure(
                "SANS_RUNTIME_TABLE_UNDEFINED",
                f"Input table '{table_name}' not bound at runtime.",
                _loc_to_dict(step.loc),
            )

    retained: Dict[str, Any] = {name: None for name in retain_vars}
    outputs: List[Dict[str, Any]] = []

    def emit_row(row: Dict[str, Any]) -> None:
        if keep_vars:
            out_row = {k: row.get(k) for k in keep_vars}
        else:
            out_row = dict(row)
        outputs.append(out_row)

    def apply_action(action: Dict[str, Any], row: Dict[str, Any]) -> None:
        if action.get("type") == "assign":
            row[action["target"]] = _eval_expr(action["expr"], row)
            return
        if action.get("type") == "output":
            emit_row(row)
            return
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_DATASTEP",
            f"Unsupported DATA step action '{action.get('type')}'",
            _loc_to_dict(step.loc),
        )

    if mode == "set":
        input_rows = tables[input_tables[0]]
        if by_vars and not _check_sorted(input_rows, by_vars):
            raise RuntimeFailure(
                "SANS_RUNTIME_ORDER_REQUIRED",
                f"Input table '{input_tables[0]}' is not sorted by {by_vars}.",
                _loc_to_dict(step.loc),
            )
        keys = [tuple(row.get(col) for col in by_vars) for row in input_rows] if by_vars else [None] * len(input_rows)
        for idx, base_row in enumerate(input_rows):
            row = {name: retained.get(name) for name in retain_vars}
            row.update(base_row)
            if by_vars:
                prev_key = keys[idx - 1] if idx > 0 else None
                curr_key = keys[idx]
                next_key = keys[idx + 1] if idx + 1 < len(keys) else None
                row.update(_compute_by_flags(by_vars, prev_key, curr_key, next_key))

            dropped = False
            for stmt in statements:
                stmt_type = stmt.get("type")
                if stmt_type == "assign":
                    row[stmt["target"]] = _eval_expr(stmt["expr"], row)
                elif stmt_type == "filter":
                    if not bool(_eval_expr(stmt["predicate"], row)):
                        dropped = True
                        break
                elif stmt_type == "output":
                    emit_row(row)
                elif stmt_type == "if_then":
                    if bool(_eval_expr(stmt["predicate"], row)):
                        apply_action(stmt["then"], row)
                    elif stmt.get("else") is not None:
                        apply_action(stmt["else"], row)
                else:
                    raise RuntimeFailure(
                        "SANS_RUNTIME_UNSUPPORTED_DATASTEP",
                        f"Unsupported DATA step statement '{stmt_type}'",
                        _loc_to_dict(step.loc),
                    )

            for name in retain_vars:
                retained[name] = row.get(name)

            if not explicit_output and not dropped:
                emit_row(row)

    elif mode == "merge":
        input_rows = [tables[name] for name in input_tables]
        for table_name, rows in zip(input_tables, input_rows):
            if by_vars and not _check_sorted(rows, by_vars):
                raise RuntimeFailure(
                    "SANS_RUNTIME_ORDER_REQUIRED",
                    f"Input table '{table_name}' is not sorted by {by_vars}.",
                    _loc_to_dict(step.loc),
                )

        input_columns: Dict[str, list[str]] = {}
        for table_name, rows in zip(input_tables, input_rows):
            input_columns[table_name] = list(rows[0].keys()) if rows else []

        grouped: Dict[str, Dict[tuple[Any, ...], List[Dict[str, Any]]]] = {}
        all_keys: set[tuple[Any, ...]] = set()
        for table_name, rows in zip(input_tables, input_rows):
            groups: Dict[tuple[Any, ...], List[Dict[str, Any]]] = {}
            for row in rows:
                key = tuple(row.get(col) for col in by_vars)
                groups.setdefault(key, []).append(row)
                all_keys.add(key)
            grouped[table_name] = groups

        for key in all_keys:
            multi_tables = [
                table_name
                for table_name in input_tables
                if len(grouped[table_name].get(key, [])) > 1
            ]
            if len(multi_tables) > 1:
                key_parts = [f"{col}={val}" for col, val in zip(by_vars, key)]
                key_desc = ", ".join(key_parts) if key_parts else str(key)
                hint = "Hint: pre-aggregate or de-duplicate inputs so only one table has multiple rows per BY key."
                raise RuntimeFailure(
                    "SANS_RUNTIME_MERGE_MANY_MANY",
                    f"Many-to-many MERGE detected for key ({key_desc}) across tables {multi_tables}. {hint}",
                    _loc_to_dict(step.loc),
                )

        def key_sorter(key: tuple[Any, ...]) -> tuple[tuple[int, Any], ...]:
            return tuple(_sort_key_value(v) for v in key)

        ordered_keys = sorted(all_keys, key=key_sorter)
        for key in ordered_keys:
            counts = [len(grouped[name].get(key, [])) for name in input_tables]
            max_count = max(counts) if counts else 0
            for idx in range(max_count):
                row: Dict[str, Any] = {name: retained.get(name) for name in retain_vars}
                for table_name in input_tables:
                    for col in input_columns.get(table_name, []):
                        row.setdefault(col, None)
                for table_name, flag in input_flags.items():
                    if flag:
                        row[flag] = False
                for col_idx, col_name in enumerate(by_vars):
                    row[col_name] = key[col_idx]

                for table_name in input_tables:
                    group_rows = grouped[table_name].get(key, [])
                    in_flag = input_flags.get(table_name)
                    if group_rows:
                        if idx < len(group_rows):
                            src = group_rows[idx]
                        else:
                            src = group_rows[-1]
                        row.update(src)
                        if in_flag:
                            row[in_flag] = True
                    else:
                        if in_flag:
                            row[in_flag] = False

                if by_vars:
                    row.update(_compute_by_flags(by_vars, None if idx == 0 else key, key, None if idx == max_count - 1 else key))

                dropped = False
                for stmt in statements:
                    stmt_type = stmt.get("type")
                    if stmt_type == "assign":
                        row[stmt["target"]] = _eval_expr(stmt["expr"], row)
                    elif stmt_type == "filter":
                        if not bool(_eval_expr(stmt["predicate"], row)):
                            dropped = True
                            break
                    elif stmt_type == "output":
                        emit_row(row)
                    elif stmt_type == "if_then":
                        if bool(_eval_expr(stmt["predicate"], row)):
                            apply_action(stmt["then"], row)
                        elif stmt.get("else") is not None:
                            apply_action(stmt["else"], row)
                    else:
                        raise RuntimeFailure(
                            "SANS_RUNTIME_UNSUPPORTED_DATASTEP",
                            f"Unsupported DATA step statement '{stmt_type}'",
                            _loc_to_dict(step.loc),
                        )

                for name in retain_vars:
                    retained[name] = row.get(name)

                if not explicit_output and not dropped:
                    emit_row(row)

    else:
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_DATASTEP",
            f"Unsupported DATA step mode '{mode}'",
            _loc_to_dict(step.loc),
        )

    return outputs


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
            if op not in {"identity", "compute", "filter", "select", "rename", "sort", "data_step"}:
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
            elif op == "sort":
                output_rows = _sort_rows(input_rows, step.params.get("by"))
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
            elif op == "data_step":
                output_rows = _execute_data_step(step, tables)
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
