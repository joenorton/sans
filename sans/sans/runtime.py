from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import csv
import re
from time import perf_counter

from .ir import IRDoc, OpStep, Step, UnknownBlockStep
from .compiler import emit_check_artifacts
from .hash_utils import compute_artifact_hash, compute_raw_hash, compute_report_sha256
from .sans_script import irdoc_to_expanded_sans
from .sans_script.canon import compute_step_id, compute_transform_id
from .xpt import load_xpt_with_warnings, dump_xpt, XptError
from . import __version__ as _engine_version
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


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
    step_evidence: Optional[List[Dict[str, Any]]] = None


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
    # Preserve leading zeros as strings (often codes, ZIPs, etc.)
    if raw.isdigit() and len(raw) > 1 and raw.startswith("0"):
        return raw
    try:
        # Try as int first for speed/exactness
        if raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
            return int(raw)
        # Otherwise use Decimal for floating point
        return Decimal(raw)
    except (ValueError, InvalidOperation):
        return raw


def _compare_sas(left: Any, right: Any, op: str) -> bool:
    """Implements SAS-style comparison where None (missing) is smallest."""
    if op == "=": return left == right
    if op == "!=": return left != right
    
    # Missing value logic for <, <=, >, >=
    # If both are None, they are equal (not strictly less/greater)
    if left is None and right is None:
        return op in {"<=", ">="}
    if left is None: # None is smaller than anything
        return op in {"<", "<="}
    if right is None: # Anything is larger than None
        return op in {">", ">="}
    
    if op == "<": return left < right
    if op == "<=": return left <= right
    if op == ">": return left > right
    if op == ">=": return left >= right
    return False


def _sort_key_value(value: Any) -> tuple[int, Any]:
    if value is None:
        return (0, None)
    return (1, value)


def _normalize_sort_by(by_spec: Any) -> list[tuple[str, bool]]:
    """Expect canonical list[{"col": str, "desc": bool}] from IR. Returns [(col, asc), ...] for internal use."""
    if not by_spec:
        return []
    return [(item["col"], not item.get("desc", False)) for item in by_spec]


def _sort_rows(rows: List[Dict[str, Any]], by_spec: Any, nodupkey: bool = False) -> List[Dict[str, Any]]:
    by_cols = _normalize_sort_by(by_spec)
    if not by_cols:
        return [dict(r) for r in rows]
    for _, asc in by_cols:
        if not asc:
            raise RuntimeFailure(
                "SANS_RUNTIME_SORT_UNSUPPORTED",
                "Descending sort is not supported in v0.1 runtime.",
            )
    
    # To ensure stable sort in Python, we use the original index as a secondary key
    # though Python's sort is already stable.
    def sort_key(indexed_row: tuple[int, Dict[str, Any]]):
        idx, row = indexed_row
        return tuple(_sort_key_value(row.get(col)) for col, _ in by_cols)
    
    indexed_rows = list(enumerate(rows))
    sorted_indexed = sorted(indexed_rows, key=sort_key)
    
    if not nodupkey:
        return [dict(r) for _, r in sorted_indexed]

    deduped: List[Dict[str, Any]] = []
    last_key: Optional[tuple[Any, ...]] = None
    for _, row in sorted_indexed:
        key = tuple(row.get(col) for col, _ in by_cols)
        if last_key is not None and key == last_key:
            # Rule: Keep FIRST row encountered in sort order (which is first in input order if keys equal)
            continue
        else:
            deduped.append(dict(row))
            last_key = key
    return deduped


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
            # File is empty, return empty list
            return []
            
        if not headers:
            # File has one empty line? 
            return []

        rows: List[Dict[str, Any]] = []
        for row in reader:
            if not row: continue # Skip completely empty lines (no commas)
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
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(headers)
        for row in rows:
            writer.writerow(["" if row.get(h) is None else row.get(h) for h in headers])


def _sanitize_column_name(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", text)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        sanitized = "COL"
    if sanitized[0].isdigit():
        sanitized = f"COL_{sanitized}"
    return sanitized


def _apply_dataset_options(
    rows: List[Dict[str, Any]],
    options: Dict[str, Any],
    loc: Optional[Dict[str, Any]],
    formats: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    output_rows = rows

    where_expr = options.get("where")
    if where_expr is not None:
        filtered_rows: List[Dict[str, Any]] = []
        for row in output_rows:
            keep = _eval_expr(where_expr, row, formats)
            if bool(keep):
                filtered_rows.append(row)
        output_rows = filtered_rows

    keep_cols = options.get("keep")
    drop_cols = options.get("drop")
    if keep_cols and drop_cols:
        raise RuntimeFailure(
            "SANS_RUNTIME_DATASET_OPTION_CONFLICT",
            "KEEP and DROP cannot both be specified in dataset options.",
            loc,
        )

    if keep_cols:
        kept_rows: List[Dict[str, Any]] = []
        for row in output_rows:
            kept_rows.append({k: row.get(k) for k in keep_cols})
        output_rows = kept_rows
    elif drop_cols:
        dropped_rows: List[Dict[str, Any]] = []
        for row in output_rows:
            dropped_rows.append({k: v for k, v in row.items() if k not in drop_cols})
        output_rows = dropped_rows

    rename_map = options.get("rename") or {}
    if rename_map:
        renamed_rows = []
        for row in output_rows:
            new_row: Dict[str, Any] = {}
            for key, value in row.items():
                new_key = rename_map.get(key, key)
                new_row[new_key] = value
            renamed_rows.append(new_row)
        output_rows = renamed_rows

    return output_rows


def _lit_value_to_python(val: Any) -> Any:
    """Resolve IR lit value to Python value. Decimal constants stay as Decimal (exact)."""
    if isinstance(val, dict) and val.get("type") == "decimal" and isinstance(val.get("value"), str):
        return Decimal(val["value"])
    return val


def _eval_expr(node: Dict[str, Any], row: Dict[str, Any], formats: Optional[Dict[str, Dict[str, Any]]] = None) -> Any:
    node_type = node.get("type")
    if node_type == "lit":
        return _lit_value_to_python(node.get("value"))
    if node_type == "col":
        return row.get(node.get("name"))
    if node_type == "call":
        name = node.get("name")
        args = node.get("args") or []
        if name == "coalesce":
            for arg in args:
                val = _eval_expr(arg, row, formats)
                if val is not None:
                    return val
            return None
        if name == "if":
            if len(args) != 3:
                raise RuntimeFailure(
                    "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
                    "IF() requires three arguments.",
                )
            predicate = _eval_expr(args[0], row, formats)
            return _eval_expr(args[1], row, formats) if bool(predicate) else _eval_expr(args[2], row, formats)
        if name == "put":
            if len(args) != 2:
                raise RuntimeFailure(
                    "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
                    "PUT() requires two arguments.",
                )
            value = _eval_expr(args[0], row, formats)
            fmt_node = args[1]
            fmt_name = None
            if isinstance(fmt_node, dict) and fmt_node.get("type") == "lit" and isinstance(fmt_node.get("value"), str):
                fmt_name = fmt_node.get("value")
            elif isinstance(fmt_node, dict) and fmt_node.get("type") == "col":
                fmt_name = fmt_node.get("name")
            if not fmt_name:
                raise RuntimeFailure(
                    "SANS_RUNTIME_FORMAT_UNSUPPORTED",
                    "PUT() requires a literal format name (e.g., $sev.).",
                )
            fmt_name = fmt_name.lower()
            if fmt_name.endswith("."):
                fmt_name = fmt_name[:-1]
            if not formats or fmt_name not in formats:
                raise RuntimeFailure(
                    "SANS_RUNTIME_FORMAT_UNDEFINED",
                    f"Unknown format '{fmt_name}'.",
                )
            fmt = formats[fmt_name]
            mapping = fmt.get("map", {})
            default = fmt.get("other")
            if value is None:
                return default
            return mapping.get(str(value), default)
        if name == "input":
            if len(args) != 2:
                raise RuntimeFailure(
                    "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
                    "INPUT() requires two arguments.",
                )
            value = _eval_expr(args[0], row, formats)
            informat_node = args[1]
            informat = None
            if isinstance(informat_node, dict) and informat_node.get("type") == "lit" and isinstance(informat_node.get("value"), str):
                informat = informat_node.get("value")
            elif isinstance(informat_node, dict) and informat_node.get("type") == "col":
                informat = informat_node.get("name")
            if not informat:
                raise RuntimeFailure(
                    "SANS_RUNTIME_INFORMAT_UNSUPPORTED",
                    "INPUT() requires a literal informat (e.g., best.).",
                )
            informat = informat.lower()
            if informat.endswith("."):
                informat = informat[:-1]
            if informat != "best":
                raise RuntimeFailure(
                    "SANS_RUNTIME_INFORMAT_UNSUPPORTED",
                    f"Unsupported informat '{informat}'.",
                )
            if value is None:
                return None
            if isinstance(value, (int, float, Decimal)):
                return value
            try:
                text = str(value).strip()
                if text == "":
                    return None
                if text.isdigit() and len(text) > 1 and text.startswith("0"):
                    return Decimal(text)
                return int(text) if text.isdigit() else Decimal(text)
            except (ValueError, InvalidOperation):
                return None
    if node_type == "binop":
        op = node.get("op")
        left = _eval_expr(node.get("left"), row, formats)
        right = _eval_expr(node.get("right"), row, formats)
        if op in {"+", "-", "*", "/"}:
            if left is None or right is None:
                return None
            # Decimal semantics when either operand is Decimal (exact decimal; no Python float)
            if isinstance(left, Decimal) or isinstance(right, Decimal):
                if isinstance(left, float) or isinstance(right, float):
                    raise RuntimeFailure(
                        "SANS_RUNTIME_DECIMAL_NO_FLOAT",
                        "Python floats are not permitted in decimal arithmetic; use int or decimal literal.",
                        None,
                    )
                L = left if isinstance(left, Decimal) else (Decimal(str(left)) if isinstance(left, (int, str)) else left)
                R = right if isinstance(right, Decimal) else (Decimal(str(right)) if isinstance(right, (int, str)) else right)
                if not isinstance(L, Decimal) or not isinstance(R, Decimal):
                    raise RuntimeFailure(
                        "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
                        "Decimal ops require numeric operands (int, str, or decimal).",
                    )
                if op == "+":
                    return L + R
                if op == "-":
                    return L - R
                if op == "*":
                    return L * R
                if op == "/":
                    return L / R
            if op == "+":
                return left + right
            if op == "-":
                return left - right
            if op == "*":
                return left * right
            if op == "/":
                return left / right
        if op in {"=", "!=", "<", "<=", ">", ">="}:
            return _compare_sas(left, right, op)
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported binary operator '{op}'",
        )
    if node_type == "boolop":
        op = node.get("op")
        args = node.get("args") or []
        if op == "and":
            return all(bool(_eval_expr(a, row, formats)) for a in args)
        if op == "or":
            return any(bool(_eval_expr(a, row, formats)) for a in args)
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported boolean operator '{op}'",
        )
    if node_type == "unop":
        op = node.get("op")
        arg = _eval_expr(node.get("arg"), row, formats)
        if op == "not":
            return not bool(arg)
        if op == "+":
            return +arg if arg is not None else None
        if op == "-":
            if arg is None:
                return None
            if isinstance(arg, Decimal):
                return -arg
            return -arg
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported unary operator '{op}'",
        )
    raise RuntimeFailure(
        "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
        f"Unsupported expression node type '{node_type}'",
    )


def _eval_expr_assert(
    node: Dict[str, Any],
    tables: Dict[str, List[Dict[str, Any]]],
    formats: Optional[Dict[str, Dict[str, Any]]],
) -> Any:
    """Evaluate an assert predicate with access to tables (e.g. row_count(t))."""
    node_type = node.get("type")
    if node_type == "lit":
        return _lit_value_to_python(node.get("value"))
    if node_type == "col":
        # In assert context there is no row; column refs evaluate to None (e.g. for comparison).
        return _eval_expr(node, {}, formats)
    if node_type == "call":
        name = node.get("name")
        args = node.get("args") or []
        if name == "row_count" and len(args) == 1:
            arg = args[0]
            if isinstance(arg, dict) and arg.get("type") == "lit":
                table_name = str(arg.get("value", ""))
                return len(tables.get(table_name, []))
            if isinstance(arg, dict) and arg.get("type") == "col":
                table_name = str(arg.get("name", ""))
                return len(tables.get(table_name, []))
        # Delegate to _eval_expr with empty row for other calls (if, put, etc.).
        return _eval_expr(node, {}, formats)
    if node_type == "binop":
        op = node.get("op")
        left = _eval_expr_assert(node.get("left"), tables, formats)
        right = _eval_expr_assert(node.get("right"), tables, formats)
        if op in {"=", "!=", "<", "<=", ">", ">="}:
            return _compare_sas(left, right, op)
        if op in {"+", "-", "*", "/"}:
            if left is None or right is None:
                return None
            if op == "+": return left + right
            if op == "-": return left - right
            if op == "*": return left * right
            if op == "/": return left / right if right else None
        raise RuntimeFailure("SANS_RUNTIME_UNSUPPORTED_EXPR_NODE", f"Unsupported binop '{op}'", {})
    if node_type == "boolop":
        op = node.get("op")
        args = node.get("args") or []
        if op == "and":
            return all(bool(_eval_expr_assert(a, tables, formats)) for a in args)
        if op == "or":
            return any(bool(_eval_expr_assert(a, tables, formats)) for a in args)
        raise RuntimeFailure("SANS_RUNTIME_UNSUPPORTED_EXPR_NODE", f"Unsupported boolop '{op}'", {})
    if node_type == "unop":
        op = node.get("op")
        arg = _eval_expr_assert(node.get("arg"), tables, formats)
        if op == "not":
            return not bool(arg)
        if op in ("+", "-"):
            return +arg if op == "+" else (-arg if arg is not None else None)
        raise RuntimeFailure("SANS_RUNTIME_UNSUPPORTED_EXPR_NODE", f"Unsupported unop '{op}'", {})
    return _eval_expr(node, {}, formats)


def _resolve_sql_column(name: str, row: Dict[str, Any], col_map: Dict[str, list[str]]) -> Any:
    if "." in name:
        key = name.lower()
        if key not in row:
            raise RuntimeFailure(
                "SANS_RUNTIME_SQL_COLUMN_UNDEFINED",
                f"Unknown column '{name}' in SQL expression.",
            )
        return row.get(key)
    col_key = name.lower()
    candidates = col_map.get(col_key, [])
    if not candidates:
        raise RuntimeFailure(
            "SANS_RUNTIME_SQL_COLUMN_UNDEFINED",
            f"Unknown column '{name}' in SQL expression.",
        )
    if len(candidates) > 1:
        raise RuntimeFailure(
            "SANS_RUNTIME_SQL_AMBIGUOUS_COLUMN",
            f"Ambiguous column '{name}' in SQL expression; qualify the column.",
        )
    return row.get(candidates[0])


def _eval_expr_sql(node: Dict[str, Any], row: Dict[str, Any], col_map: Dict[str, list[str]]) -> Any:
    node_type = node.get("type")
    if node_type == "lit":
        return node.get("value")
    if node_type == "col":
        return _resolve_sql_column(node.get("name", ""), row, col_map)
    if node_type == "binop":
        op = node.get("op")
        left = _eval_expr_sql(node.get("left"), row, col_map)
        right = _eval_expr_sql(node.get("right"), row, col_map)
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
            return _compare_sas(left, right, op)
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported SQL binary operator '{op}'",
        )
    if node_type == "boolop":
        op = node.get("op")
        args = node.get("args") or []
        if op == "and":
            return all(bool(_eval_expr_sql(a, row, col_map)) for a in args)
        if op == "or":
            return any(bool(_eval_expr_sql(a, row, col_map)) for a in args)
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported SQL boolean operator '{op}'",
        )
    if node_type == "unop":
        op = node.get("op")
        arg = _eval_expr_sql(node.get("arg"), row, col_map)
        if op == "not":
            return not bool(arg)
        if op == "+":
            return +arg if arg is not None else None
        if op == "-":
            return -arg if arg is not None else None
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
            f"Unsupported SQL unary operator '{op}'",
        )
    raise RuntimeFailure(
        "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE",
        f"Unsupported SQL expression node type '{node_type}'",
    )


def _compute_by_flags(by_vars: list[str], prev_key: Optional[tuple[Any, ...]], curr_key: tuple[Any, ...], next_key: Optional[tuple[Any, ...]]) -> dict[str, bool]:
    flags: dict[str, bool] = {}
    for idx, var in enumerate(by_vars):
        first = prev_key is None or prev_key[:idx + 1] != curr_key[:idx + 1]
        last = next_key is None or next_key[:idx + 1] != curr_key[:idx + 1]
        flags[f"first.{var}"] = first
        flags[f"last.{var}"] = last
    return flags


def _execute_data_step(
    step: OpStep,
    tables: Dict[str, List[Dict[str, Any]]],
    formats: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
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
    
    # Track multiple output tables
    target_outputs: Dict[str, List[Dict[str, Any]]] = {}
    for out_table in step.outputs:
        target_outputs[out_table] = []

    def emit_row(row: Dict[str, Any], target: Optional[str] = None) -> None:
        if keep_vars:
            out_row = {k: row.get(k) for k in keep_vars}
        else:
            out_row = dict(row)
            
        # If target is specified, output to that table. Else to all?
        # Standard SAS: if target specified, ONLY to that. If not, to ALL listed in DATA stmt.
        if target:
            if target in target_outputs:
                target_outputs[target].append(out_row)
        else:
            for t in target_outputs:
                target_outputs[t].append(out_row)

    def execute_stmts(stmts: List[Dict[str, Any]], row: Dict[str, Any], depth: int = 0) -> bool:
        """Returns True if execution should continue, False if row was filtered."""
        if depth > 50:
            raise RuntimeFailure(
                "SANS_RUNTIME_CONTROL_DEPTH",
                "Control-flow nesting exceeds 50 levels.",
            )
        for stmt in stmts:
            stype = stmt.get("type")
            if stype == "assign":
                allow_overwrite = stmt.get("allow_overwrite", True)
                target = stmt["target"]
                if not allow_overwrite and target in row:
                    raise RuntimeFailure(
                        "SANS_RUNTIME_ASSIGN_OVERWRITE",
                        f"Assignment to existing column '{target}' requires update! in sans scripts.",
                    )
                row[target] = _eval_expr(stmt["expr"], row, formats)
            elif stype == "filter":
                if not bool(_eval_expr(stmt["predicate"], row, formats)):
                    return False
            elif stype == "output":
                emit_row(row, stmt.get("target"))
            elif stype == "if_then":
                if bool(_eval_expr(stmt["predicate"], row, formats)):
                    if not execute_stmts([stmt["then"]], row, depth + 1): return False
                elif stmt.get("else"):
                    if not execute_stmts([stmt["else"]], row, depth + 1): return False
            elif stype == "block":
                if not execute_stmts(stmt.get("body", []), row, depth + 1): return False
            elif stype == "do_loop":
                start_val = _eval_expr(stmt["start"], row, formats)
                end_val = _eval_expr(stmt["end"], row, formats)
                step_val = _eval_expr(stmt.get("step") or {"type": "lit", "value": 1}, row, formats)
                if start_val is None or end_val is None:
                    continue
                var = stmt["var"]
                if not isinstance(start_val, int) or not isinstance(end_val, int) or not isinstance(step_val, int):
                    raise RuntimeFailure(
                        "SANS_RUNTIME_LOOP_STEP_INVALID",
                        "DO loop bounds must be integers.",
                    )
                if step_val == 0:
                    raise RuntimeFailure(
                        "SANS_RUNTIME_LOOP_STEP_INVALID",
                        "DO loop step cannot be 0.",
                    )

                if step_val > 0:
                    if start_val > end_val:
                        continue
                    iterations = ((end_val - start_val) // step_val) + 1
                else:
                    if start_val < end_val:
                        continue
                    iterations = ((start_val - end_val) // abs(step_val)) + 1

                if iterations > 1000000:
                    raise RuntimeFailure("SANS_RUNTIME_LOOP_LIMIT", "Loop exceeded 1,000,000 iterations.")

                count = 0
                val = start_val
                while True:
                    if (step_val > 0 and val > end_val) or (step_val < 0 and val < end_val):
                        break
                    count += 1
                    row[var] = val
                    if not execute_stmts(stmt.get("body", []), row, depth + 1): return False
                    val += step_val
            elif stype == "select":
                matched = False
                for when in stmt.get("when", []):
                    if bool(_eval_expr(when["cond"], row, formats)):
                        if not execute_stmts([when["action"]], row, depth + 1): return False
                        matched = True
                        break
                if not matched:
                    if stmt.get("otherwise"):
                        if not execute_stmts([stmt["otherwise"]], row, depth + 1): return False
                    else:
                        # SAS select/when without otherwise is error if no match?
                        # Actually SAS just continues? No, it's an error.
                        raise RuntimeFailure("SANS_RUNTIME_SELECT_MISMATCH", "SELECT statement had no matching WHEN and no OTHERWISE.")
        return True

    if mode == "set":
        input_rows = _apply_dataset_options(
            tables[input_tables[0]],
            input_specs[0],
            _loc_to_dict(step.loc),
            formats,
        )
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

            if execute_stmts(statements, row):
                if not explicit_output:
                    emit_row(row)

            for name in retain_vars:
                retained[name] = row.get(name)

    elif mode == "merge":
        input_rows = [
            _apply_dataset_options(tables[name], spec, _loc_to_dict(step.loc), formats)
            for name, spec in zip(input_tables, input_specs)
        ]
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

                if execute_stmts(statements, row):
                    if not explicit_output:
                        emit_row(row)

                for name in retain_vars:
                    retained[name] = row.get(name)

    else:
        raise RuntimeFailure(
            "SANS_RUNTIME_UNSUPPORTED_DATASTEP",
            f"Unsupported DATA step mode '{mode}'",
            _loc_to_dict(step.loc),
        )

    # Return first output table as default?
    # No, we need to return all.
    # execute_plan will handle step.outputs.
    # I should update _execute_data_step signature to return multiple?
    # Actually execute_plan sets tables[output] = output_rows.
    # If I have multiple outputs, I should return a DICT or handle it in execute_plan.
    
    # I'll update execute_plan to handle multiple outputs from _execute_data_step.
    return target_outputs


def _execute_transpose(step: OpStep, tables: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    params = step.params or {}
    by_vars = params.get("by") or []
    id_var = params.get("id")
    var_var = params.get("var")

    if not id_var or not var_var:
        raise RuntimeFailure(
            "SANS_RUNTIME_TRANSPOSE_MISSING_ARGS",
            "PROC TRANSPOSE requires ID and VAR options.",
            _loc_to_dict(step.loc),
        )

    input_table = step.inputs[0]
    input_rows = tables.get(input_table, [])
    if by_vars and not _check_sorted(input_rows, by_vars):
        raise RuntimeFailure(
            "SANS_RUNTIME_ORDER_REQUIRED",
            f"Input table '{input_table}' is not sorted by {by_vars}.",
            _loc_to_dict(step.loc),
        )

    outputs: List[Dict[str, Any]] = []
    id_cols_order: List[str] = []
    id_col_values: Dict[str, Any] = {}

    current_key = None
    current_row: Optional[Dict[str, Any]] = None

    def flush_current():
        if current_row is not None:
            outputs.append(current_row)

    for row in input_rows:
        key = tuple(row.get(col) for col in by_vars) if by_vars else ("__all__",)
        if current_key is None or key != current_key:
            flush_current()
            current_key = key
            current_row = {col: key[idx] for idx, col in enumerate(by_vars)}

        id_val = row.get(id_var)
        if id_val is None or (isinstance(id_val, str) and id_val.strip() == ""):
            raise RuntimeFailure(
                "SANS_RUNTIME_TRANSPOSE_ID_MISSING",
                f"Missing ID value for column '{id_var}'.",
                _loc_to_dict(step.loc),
            )
        col_name = _sanitize_column_name(id_val)
        if col_name in id_col_values and id_col_values[col_name] != id_val:
            raise RuntimeFailure(
                "SANS_RUNTIME_TRANSPOSE_ID_COLLISION",
                f"ID value '{id_val}' collides with '{id_col_values[col_name]}' after sanitization.",
                _loc_to_dict(step.loc),
            )
        id_col_values.setdefault(col_name, id_val)
        if col_name not in id_cols_order:
            id_cols_order.append(col_name)

        current_row[col_name] = row.get(var_var)

    flush_current()

    columns = list(by_vars) + id_cols_order
    normalized_rows: List[Dict[str, Any]] = []
    for row in outputs:
        normalized_rows.append({col: row.get(col) for col in columns})
    return normalized_rows


def _execute_sql_select(step: OpStep, tables: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    params = step.params or {}
    base = params.get("from") or {}
    joins = params.get("joins") or []
    select_items = params.get("select") or []
    where_expr = params.get("where")
    group_by = params.get("group_by") or []

    base_table = base.get("table")
    base_alias = base.get("alias")
    if not base_table or not base_alias:
        raise RuntimeFailure(
            "SANS_RUNTIME_SQL_MALFORMED",
            "PROC SQL missing FROM table.",
            _loc_to_dict(step.loc),
        )
    if base_table not in tables:
        raise RuntimeFailure(
            "SANS_RUNTIME_TABLE_UNDEFINED",
            f"Input table '{base_table}' not bound at runtime.",
            _loc_to_dict(step.loc),
        )

    def qualify_rows(rows: List[Dict[str, Any]], alias: str) -> List[Dict[str, Any]]:
        qualified: List[Dict[str, Any]] = []
        for row in rows:
            new_row: Dict[str, Any] = {}
            for key, value in row.items():
                new_row[f"{alias}.{key.lower()}"] = value
            qualified.append(new_row)
        return qualified

    def columns_for(alias: str, rows: List[Dict[str, Any]]) -> list[str]:
        if not rows:
            return []
        return [f"{alias}.{col.lower()}" for col in rows[0].keys()]

    current_rows = qualify_rows(tables[base_table], base_alias)
    col_map: Dict[str, list[str]] = {}
    base_cols = columns_for(base_alias, tables[base_table])
    for col in base_cols:
        short = col.split(".", 1)[1]
        col_map.setdefault(short, []).append(col)

    for join in joins:
        join_type = join.get("type")
        right_table = join.get("table")
        right_alias = join.get("alias")
        on_expr = join.get("on")

        if right_table not in tables:
            raise RuntimeFailure(
                "SANS_RUNTIME_TABLE_UNDEFINED",
                f"Input table '{right_table}' not bound at runtime.",
                _loc_to_dict(step.loc),
            )
        right_rows = tables[right_table]
        qualified_right = qualify_rows(right_rows, right_alias)
        right_cols = columns_for(right_alias, right_rows)
        for col in right_cols:
            short = col.split(".", 1)[1]
            col_map.setdefault(short, []).append(col)

        # SQL join semantics: evaluate joins left-to-right; LEFT keeps all left rows
        # and fills unmatched right columns with nulls.
        joined: List[Dict[str, Any]] = []
        for left_row in current_rows:
            matched = False
            for right_row in qualified_right:
                combined = dict(left_row)
                combined.update(right_row)
                if bool(_eval_expr_sql(on_expr, combined, col_map)):
                    matched = True
                    joined.append(combined)
            if not matched and join_type == "left":
                combined = dict(left_row)
                for col in right_cols:
                    combined[col] = None
                joined.append(combined)
        current_rows = joined

    if where_expr is not None:
        filtered: List[Dict[str, Any]] = []
        for row in current_rows:
            if bool(_eval_expr_sql(where_expr, row, col_map)):
                filtered.append(row)
        current_rows = filtered

    agg_items = [item for item in select_items if item.get("type") == "agg"]
    non_agg_items = [item for item in select_items if item.get("type") == "col"]

    if group_by or agg_items:
        # GROUP BY semantics: form groups on key values and emit rows sorted by keys
        # for deterministic output ordering; aggregates ignore nulls.
        group_keys = [key.lower() for key in group_by]
        groups: Dict[tuple[Any, ...], List[Dict[str, Any]]] = {}
        for row in current_rows:
            key = tuple(_resolve_sql_column(k, row, col_map) for k in group_keys) if group_keys else ("__all__",)
            groups.setdefault(key, []).append(row)

        def group_sort_key(key: tuple[Any, ...]):
            if not group_keys:
                return (0,)
            return tuple(_sort_key_value(v) for v in key)

        output_rows: List[Dict[str, Any]] = []
        for key in sorted(groups.keys(), key=group_sort_key):
            rows = groups[key]
            out_row: Dict[str, Any] = {}
            for item in select_items:
                if item["type"] == "col":
                    value = _resolve_sql_column(item["name"], rows[0], col_map)
                    out_row[item["alias"]] = value
                else:
                    func = item["func"]
                    arg = item["arg"]
                    values: list[Any] = []
                    if arg == "*":
                        values = rows
                    else:
                        for row in rows:
                            values.append(_resolve_sql_column(arg, row, col_map))
                    if func == "count":
                        if arg == "*":
                            out_row[item["alias"]] = len(values)
                        else:
                            out_row[item["alias"]] = sum(1 for v in values if v is not None)
                    elif func == "sum":
                        nums = [v for v in values if v is not None]
                        out_row[item["alias"]] = sum(nums) if nums else None
                    elif func == "min":
                        nums = [v for v in values if v is not None]
                        out_row[item["alias"]] = min(nums) if nums else None
                    elif func == "max":
                        nums = [v for v in values if v is not None]
                        out_row[item["alias"]] = max(nums) if nums else None
                    elif func == "avg":
                        nums = [v for v in values if v is not None]
                        out_row[item["alias"]] = (sum(nums) / len(nums)) if nums else None
            output_rows.append(out_row)

        return output_rows

    output_rows = []
    for row in current_rows:
        out_row: Dict[str, Any] = {}
        for item in select_items:
            if item["type"] == "col":
                out_row[item["alias"]] = _resolve_sql_column(item["name"], row, col_map)
            else:
                raise RuntimeFailure(
                    "SANS_RUNTIME_SQL_MALFORMED",
                    "Aggregate select requires GROUP BY.",
                    _loc_to_dict(step.loc),
                )
        output_rows.append(out_row)
    return output_rows


def _execute_aggregate(step: OpStep, tables: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    params = step.params or {}
    group_by = params.get("group_by") or []
    metrics = params.get("metrics") or []

    input_table = step.inputs[0]
    input_rows = tables.get(input_table, [])

    groups: Dict[tuple[Any, ...], List[Dict[str, Any]]] = {}
    for row in input_rows:
        key = tuple(row.get(col) for col in group_by)
        groups.setdefault(key, []).append(row)

    def group_sort_key(key: tuple[Any, ...]):
        return tuple(_sort_key_value(v) for v in key)

    outputs: List[Dict[str, Any]] = []
    for key in sorted(groups.keys(), key=group_sort_key):
        rows = groups[key]
        out_row: Dict[str, Any] = {col: key[idx] for idx, col in enumerate(group_by)}
        for m in metrics:
            name = m.get("name", "")
            op = m.get("op", "mean")
            col = m.get("col", "")
            if op == "mean":
                values = [r.get(col) for r in rows if r.get(col) is not None]
                val = (sum(values) / len(values)) if values else None
            elif op == "sum":
                values = [r.get(col) for r in rows if r.get(col) is not None]
                val = sum(values) if values else None
            elif op in ("min", "max"):
                values = [r.get(col) for r in rows if r.get(col) is not None]
                val = (min(values) if op == "min" else max(values)) if values else None
            elif op in ("count", "n"):
                val = len(rows)
            else:
                val = None
            out_row[name] = val
        outputs.append(out_row)
    return outputs


def execute_plan(ir_doc: IRDoc, bindings: Dict[str, str], out_dir: Path, output_format: str = "csv") -> ExecutionResult:
    start = perf_counter()
    diagnostics: List[RuntimeDiagnostic] = []
    outputs: List[Dict[str, Any]] = []
    step_evidence: List[Dict[str, Any]] = []

    try:
        tables: Dict[str, List[Dict[str, Any]]] = {}
        formats: Dict[str, Dict[str, Any]] = {}
        for name, path_str in bindings.items():
            path = Path(path_str)
            if not path.exists():
                raise RuntimeFailure(
                    "SANS_RUNTIME_INPUT_NOT_FOUND",
                    f"Input table '{name}' file not found: {path_str}",
                )
            if path.suffix.lower() == ".xpt":
                try:
                    rows, warnings = load_xpt_with_warnings(path)
                except XptError as exc:
                    raise RuntimeFailure(exc.code, exc.message)
                tables[name] = rows
                for warn in warnings:
                    diagnostics.append(RuntimeDiagnostic(code=warn.code, message=warn.message))
            else:
                tables[name] = _load_csv(path)

        for step_idx, step in enumerate(ir_doc.steps):
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
            if op not in {"identity", "compute", "filter", "select", "rename", "sort", "data_step", "transpose", "sql_select", "format", "aggregate", "summary", "save", "assert", "let_scalar", "const"}:
                raise RuntimeFailure(
                    "SANS_CAP_UNSUPPORTED_OP",
                    f"Unsupported operation '{op}'",
                    _loc_to_dict(step.loc),
                )

            if not step.inputs and op not in ("format", "assert", "let_scalar", "const"):
                raise RuntimeFailure(
                    "SANS_RUNTIME_TABLE_UNDEFINED",
                    f"Operation '{op}' has no input table.",
                    _loc_to_dict(step.loc),
                )
            if step.inputs:
                input_table = step.inputs[0]
                if input_table not in tables:
                    raise RuntimeFailure(
                        "SANS_RUNTIME_TABLE_UNDEFINED",
                        f"Input table '{input_table}' not bound at runtime.",
                        _loc_to_dict(step.loc),
                    )

                input_rows = tables[input_table]

            try:
                if op == "identity":
                    output_rows = [dict(r) for r in input_rows]
                elif op == "sort":
                    output_rows = _sort_rows(input_rows, step.params.get("by"), bool(step.params.get("nodupkey")))
                elif op == "compute":
                    assigns = step.params.get("assignments") or step.params.get("assign") or []
                    output_rows = []
                    for row in input_rows:
                        new_row = dict(row)
                        for assign in assigns:
                            col_name = assign.get("target") or assign.get("col")
                            expr = assign.get("expr")
                            new_row[col_name] = _eval_expr(expr, new_row, formats)
                        output_rows.append(new_row)
                elif op == "filter":
                    predicate = step.params.get("predicate")
                    output_rows = []
                    for row in input_rows:
                        keep = _eval_expr(predicate, row, formats)
                        if bool(keep):
                            output_rows.append(dict(row))
                elif op == "select":
                    cols = step.params.get("cols") or []
                    drop = step.params.get("drop") or []
                    output_rows = []
                    for row in input_rows:
                        if cols:
                            new_row = {k: row.get(k) for k in cols}
                        else:
                            new_row = {k: v for k, v in row.items() if k not in drop}
                        output_rows.append(new_row)
                elif op == "rename":
                    mapping = step.params.get("mapping") or []
                    rename_map = {p["from"]: p["to"] for p in mapping}
                    output_rows = []
                    for row in input_rows:
                        new_row: Dict[str, Any] = {}
                        for key, value in row.items():
                            new_key = rename_map.get(key, key)
                            new_row[new_key] = value
                        output_rows.append(new_row)
                elif op == "data_step":
                    step_outputs = _execute_data_step(step, tables, formats)
                    # step_outputs is Dict[str, List[Dict[str, Any]]]
                    for out_table, out_rows in step_outputs.items():
                        tables[out_table] = out_rows
                    
                    t_id = compute_transform_id(op, step.params)
                    step_evidence.append({
                        "step_index": step_idx,
                        "step_id": compute_step_id(t_id, step.inputs, step.outputs),
                        "transform_id": t_id,
                        "op": op,
                        "inputs": list(step.inputs),
                        "outputs": list(step.outputs),
                        "row_counts": {t: len(rows) for t, rows in step_outputs.items()}
                    })
                    continue # Already updated tables
                elif op == "transpose":
                    output_rows = _execute_transpose(step, tables)
                elif op == "sql_select":
                    output_rows = _execute_sql_select(step, tables)
                elif op == "format":
                    name = (step.params.get("name") or "").lower()
                    if not name:
                        raise RuntimeFailure(
                            "SANS_RUNTIME_FORMAT_MALFORMED",
                            "FORMAT step missing name.",
                            _loc_to_dict(step.loc),
                        )
                    formats[name] = {
                        "map": dict(step.params.get("map") or {}),
                        "other": step.params.get("other"),
                    }
                    output_rows = []
                elif op in ("aggregate", "summary"):
                    output_rows = _execute_aggregate(step, tables)
                elif op == "save":
                    # Write input table to path (relative to out_dir).
                    path = step.params.get("path") or ""
                    name = step.params.get("name")
                    out_path = out_dir / path if path else out_dir / f"{input_table}.csv"
                    table_rows = tables.get(input_table, [])
                    columns = list(table_rows[0].keys()) if table_rows else []
                    _write_csv(out_path, table_rows)
                    outputs.append({
                        "table": name or input_table,
                        "path": str(out_path),
                        "rows": len(table_rows),
                        "columns": columns,
                    })
                    t_id = compute_transform_id(op, step.params)
                    step_evidence.append({
                        "step_index": step_idx,
                        "step_id": compute_step_id(t_id, step.inputs, step.outputs),
                        "transform_id": t_id,
                        "op": op,
                        "inputs": list(step.inputs),
                        "outputs": list(step.outputs),
                        "row_counts": {input_table: len(table_rows)},
                    })
                    continue
                elif op == "assert":
                    predicate = step.params.get("predicate")
                    # Evaluate with empty row; for row_count(t) we need tables - pass via formats placeholder or extend _eval_expr.
                    result = _eval_expr_assert(predicate, tables, formats)
                    step_evidence.append({
                        "step_index": step_idx,
                        "step_id": compute_step_id(compute_transform_id(op, step.params), step.inputs, step.outputs),
                        "transform_id": compute_transform_id(op, step.params),
                        "op": op,
                        "assert_result": result,
                    })
                    continue
                elif op == "let_scalar":
                    # No-op at runtime; binding is for compile-time substitution only.
                    continue
                elif op == "const":
                    # No-op at runtime; constants are for compile-time substitution only.
                    continue
                else:
                    raise RuntimeFailure(
                        "SANS_CAP_UNSUPPORTED_OP",
                        f"Unsupported operation '{op}'",
                        _loc_to_dict(step.loc),
                    )
            except RuntimeFailure:
                raise
            except Exception as exc:
                raise RuntimeFailure(
                    "SANS_RUNTIME_INTERNAL_ERROR",
                    f"Unexpected error during '{op}': {exc}",
                    _loc_to_dict(step.loc),
                )

            for output in step.outputs:
                tables[output] = output_rows
            
            t_id = compute_transform_id(op, step.params)
            step_evidence.append({
                "step_index": step_idx,
                "step_id": compute_step_id(t_id, step.inputs, step.outputs),
                "transform_id": t_id,
                "op": op,
                "inputs": list(step.inputs),
                "outputs": list(step.outputs),
                "row_counts": {t: len(output_rows) for t in step.outputs}
            })

        # Determine outputs to emit: all terminal tables plus explicit non-terminal tables
        # (skip compiler temp tables marked by "__" in their names).
        produced_order: List[str] = []
        consumed: set[str] = set()
        for step in ir_doc.steps:
            if isinstance(step, OpStep):
                produced_order.extend(step.outputs)
                consumed.update(step.inputs)

        terminal_tables = [t for t in produced_order if t not in consumed]
        non_terminal_tables = [t for t in produced_order if t in consumed]

        def is_temp_table(name: str) -> bool:
            return "__" in name

        emit_tables: List[str] = []
        for name in terminal_tables + [t for t in non_terminal_tables if not is_temp_table(t)]:
            if name not in emit_tables:
                emit_tables.append(name)

        out_dir.mkdir(parents=True, exist_ok=True)
        for table_name in emit_tables:
            table_rows = tables.get(table_name, [])
            columns: List[str] = []
            if table_rows:
                columns = list(table_rows[0].keys())

            if output_format.lower() == "xpt":
                out_path = out_dir / f"{table_name}.xpt"
                try:
                    dump_xpt(out_path, table_rows, columns, dataset_name=table_name.upper())
                except XptError as exc:
                    raise RuntimeFailure(exc.code, exc.message)
            else:
                out_path = out_dir / f"{table_name}.csv"
                _write_csv(out_path, table_rows)
            
            outputs.append(
                {
                    "table": table_name,
                    "path": str(out_path),
                    "rows": len(table_rows),
                    "columns": columns,
                }
            )

        status = "ok_warnings" if diagnostics else "ok"
        return ExecutionResult(
            status=status,
            diagnostics=diagnostics,
            outputs=outputs,
            execute_ms=int((perf_counter() - start) * 1000),
            step_evidence=step_evidence
        )

    except RuntimeFailure as err:
        diagnostics.append(RuntimeDiagnostic(code=err.code, message=err.message, loc=err.loc))
        return ExecutionResult(
            status="failed",
            diagnostics=diagnostics,
            outputs=outputs,
            execute_ms=int((perf_counter() - start) * 1000),
            step_evidence=step_evidence
        )


def run_script(
    text: str,
    file_name: str,
    bindings: Dict[str, str],
    out_dir: Path,
    strict: bool = True,
    output_format: str = "csv",
    include_roots: Optional[List[Path]] = None,
    allow_absolute_includes: bool = False,
    allow_include_escape: bool = False,
) -> Dict[str, Any]:
    irdoc, report = emit_check_artifacts(
        text=text,
        file_name=file_name,
        tables=set(bindings.keys()) if bindings else None,
        out_dir=out_dir,
        strict=strict,
        include_roots=include_roots,
        allow_absolute_includes=allow_absolute_includes,
        allow_include_escape=allow_include_escape,
    )

    # If compilation/validation refused, annotate runtime and exit.
    if report.get("status") == "refused":
        report["runtime"] = {"status": "refused", "outputs": [], "timing": {"execute_ms": None}}
        report_path = Path(out_dir) / "report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    # Write expanded.sans (canonical script form from IR) and add to report outputs
    expanded_path = Path(out_dir) / "expanded.sans"
    expanded_path.write_text(irdoc_to_expanded_sans(irdoc), encoding="utf-8")
    report["outputs"].append({"path": expanded_path.name, "sha256": compute_artifact_hash(expanded_path)})

    # Populate input hashes
    existing_inputs = {i["path"] for i in report.get("inputs", [])}
    if bindings:
        for name, path_str in bindings.items():
            p = Path(path_str)
            posix_path = p.as_posix()
            if posix_path not in existing_inputs:
                h = compute_artifact_hash(p)
                report["inputs"].append({"path": posix_path, "sha256": h, "table": name})

    result = execute_plan(irdoc, bindings, Path(out_dir), output_format=output_format)

    report["runtime"] = {
        "status": result.status,
        "outputs": result.outputs,
        "timing": {"execute_ms": result.execute_ms},
    }
    report["timing"]["execute_ms"] = result.execute_ms

    if result.status == "failed":
        report["status"] = "failed"
        report["exit_code_bucket"] = 50
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
    elif result.status == "ok_warnings":
        report["status"] = "ok_warnings"
        report["exit_code_bucket"] = 10
        report["primary_error"] = None
        report["diagnostics"] = [
            {"code": d.code, "message": d.message, "loc": d.loc}
            for d in result.diagnostics
        ]
    else:
        report["status"] = "ok"
        report["exit_code_bucket"] = 0
        report["primary_error"] = None

    # Extend outputs with runtime outputs and hashes if available.
    runtime_out_entries = []
    bundle_root = Path(out_dir).resolve()

    for out in result.outputs:
        abs_path = Path(out["path"]).resolve()

        # bundle-relative path for manifests (report/evidence)
        try:
            rel_path = abs_path.relative_to(bundle_root)
        except ValueError:
            # If something produced a path outside the bundle root, refuse loudly.
            raise RuntimeError(f"Runtime output path is outside bundle dir: {abs_path} (bundle={bundle_root})")

        rel_posix = rel_path.as_posix()

        entry = {"path": rel_posix, "sha256": compute_artifact_hash(abs_path)}
        report["outputs"].append(entry)

        runtime_out_entries.append({
            "name": out["table"],
            "path": rel_posix,
            "format": rel_path.suffix.lstrip(".").lower() or "csv",
            "bytes_sha256": compute_raw_hash(abs_path),
            "canonical_sha256": entry["sha256"],
            "row_count": out["rows"],
            "columns": out["columns"],
        })


    # 1. Emit registry.candidate.json
    registry = {
        "registry_version": "0.1",
        "transforms": [],
        "index": {}
    }
    seen_transforms = {}
    for step_idx, step in enumerate(irdoc.steps):
        if isinstance(step, OpStep):
            t_id = compute_transform_id(step.op, step.params)
            if t_id not in seen_transforms:
                transform_entry = {
                    "transform_id": t_id,
                    "kind": step.op,
                    "spec": {
                        "op": step.op,
                        "params": step.params # _canonicalize will be done by compute_transform_id internally for hashing, but for registry we should probably store it canonicalized
                    }
                }
                # Actually, let's use the same payload as hashed
                from .sans_script.canon import _canonicalize
                transform_entry["spec"]["params"] = _canonicalize(step.params or {})
                
                registry["transforms"].append(transform_entry)
                seen_transforms[t_id] = True
            registry["index"][str(step_idx)] = t_id

    registry_path = Path(out_dir) / "registry.candidate.json"
    registry_path.write_text(json.dumps(registry, indent=2), encoding="utf-8")
    report["outputs"].append({"path": registry_path.name, "sha256": compute_artifact_hash(registry_path)})

    # 2. Emit runtime.evidence.json
    evidence = {
        "sans_version": _engine_version,
        "run_id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "plan_ir": {
            "path": "plan.ir.json",
            "sha256": compute_raw_hash(Path(out_dir) / "plan.ir.json")
        },
        "bindings": {name: Path(p).name for name, p in bindings.items()},
        "inputs": [],
        "outputs": runtime_out_entries,
        "step_evidence": result.step_evidence or []
    }
    
    for inp in report.get("inputs", []):
        p = Path(inp["path"])
        evidence["inputs"].append({
            "name": inp.get("table"),
            "path": p.name,
            "format": p.suffix.lstrip(".").lower() or "csv",
            "bytes_sha256": compute_raw_hash(p),
            "canonical_sha256": inp.get("sha256")
        })

    evidence_path = Path(out_dir) / "runtime.evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    report["outputs"].append({"path": evidence_path.name, "sha256": compute_artifact_hash(evidence_path)})

    report_path = Path(out_dir) / "report.json"
    bundle_root = Path(out_dir).resolve()

    # Canonical self-hash: set report_sha256 before writing; report.json output entry stays sha256=None
    report["report_sha256"] = compute_report_sha256(report, bundle_root)
    report_path_posix = report_path.as_posix()
    for o in report["outputs"]:
        if (o.get("path") == report_path_posix or o.get("path") == report_path.name or
                (o.get("path") or "").replace("\\", "/").endswith("/report.json")):
            o["sha256"] = None
            break

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
