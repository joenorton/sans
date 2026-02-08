from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import csv
import hashlib
import re
from time import perf_counter

from .ir import IRDoc, OpStep, Step, UnknownBlockStep
from .compiler import emit_check_artifacts
from .hash_utils import compute_artifact_hash, compute_input_hash, compute_raw_hash, compute_report_sha256
from .bundle import (
    ensure_bundle_layout,
    bundle_relative_path,
    validate_save_path_under_outputs,
    INPUTS_SOURCE,
    INPUTS_DATA,
    ARTIFACTS,
    OUTPUTS,
)
from .sans_script import irdoc_to_expanded_sans
from .sans_script.canon import compute_step_id, compute_transform_id
from .xpt import load_xpt_with_warnings, dump_xpt, XptError
from .evidence import collect_table_evidence, DEFAULT_EVIDENCE_CONFIG
from .lineage import build_var_graph, write_vars_graph_json
from . import __version__ as _engine_version
from .types import Type, type_name
import json
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
    table_evidence: Optional[Dict[str, Any]] = None
    datasource_schemas: Optional[Dict[str, List[str]]] = None
    datasource_evidence: Optional[List[Dict[str, Any]]] = None
    coercion_diagnostics: Optional[List[Dict[str, Any]]] = None


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


COERCE_SAMPLE_LIMIT = 10


def _coerce_csv_token(raw: str, expected: Type) -> tuple[Any, Optional[str]]:
    """
    Coerce CSV token to expected type. Returns (value, failure_reason).
    failure_reason is None on success.
    """
    if raw == "":
        return None, None  # Preserve current ingestion: empty -> null
    if expected == Type.STRING:
        return raw, None
    if expected == Type.INT:
        s = raw.strip()
        if not s:
            return None, None
        try:
            return int(s), None
        except ValueError:
            return None, "invalid_int"
    if expected == Type.DECIMAL:
        s = raw.strip()
        if not s:
            return None, None
        try:
            return Decimal(s), None
        except (ValueError, InvalidOperation):
            return None, "invalid_decimal"
    if expected == Type.BOOL:
        s = raw.strip().lower()
        if not s:
            return None, None
        if s in ("true", "1", "yes"):
            return True, None
        if s in ("false", "0", "no"):
            return False, None
        return None, "invalid_bool"
    if expected == Type.NULL:
        s = raw.strip()
        if not s:
            return None, None
        return None, "invalid_null"
    return raw, None


def _coerce_csv_rows(
    reader: csv.reader,
    headers: List[str],
    column_types: Dict[str, Type],
    sample_cap: int,
) -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    stats: Dict[str, Dict[str, Any]] = {}
    for col in headers:
        if col in column_types:
            stats[col] = {
                "expected": column_types[col],
                "failure_count": 0,
                "sample_row_numbers": [],
                "sample_raw_values": [],
                "raw_seen": set(),
                "reason": None,
            }

    row_index = 0
    for row in reader:
        if not row:
            continue
        row_index += 1
        row_dict: Dict[str, Any] = {}
        for i, col in enumerate(headers):
            raw = row[i] if i < len(row) else ""
            expected = column_types.get(col)
            if expected is not None:
                value, reason = _coerce_csv_token(raw, expected)
                if reason:
                    stat = stats[col]
                    stat["failure_count"] += 1
                    if stat["reason"] is None:
                        stat["reason"] = reason
                    elif stat["reason"] != reason:
                        stat["reason"] = "mixed"
                    if len(stat["sample_row_numbers"]) < sample_cap:
                        stat["sample_row_numbers"].append(row_index)
                    raw_trim = raw.strip()
                    if raw_trim not in stat["raw_seen"] and len(stat["sample_raw_values"]) < sample_cap:
                        stat["sample_raw_values"].append(raw_trim)
                        stat["raw_seen"].add(raw_trim)
                    value = None
                row_dict[col] = value
            else:
                row_dict[col] = _parse_value(raw)
        rows.append(row_dict)

    failures: List[Dict[str, Any]] = []
    for col in headers:
        stat = stats.get(col)
        if not stat or stat["failure_count"] == 0:
            continue
        failures.append({
            "column": col,
            "expected_type": type_name(stat["expected"]),
            "failure_count": stat["failure_count"],
            "sample_row_numbers": stat["sample_row_numbers"],
            "sample_raw_values": stat["sample_raw_values"],
            "failure_reason": stat["reason"] or "unknown",
        })

    if not failures:
        return rows, None
    return rows, {
        "total_rows_scanned": row_index,
        "columns": failures,
        "truncated": False,
    }


def _compare_sas(left: Any, right: Any, op: str) -> bool:
    """Implements SAS-style comparison where None (missing) is smallest."""
    if op in {"=", "=="}: return left == right
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


def _load_csv_with_header(path: Path) -> tuple[List[Dict[str, Any]], List[str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            # File is empty
            return [], []

        if not headers:
            # File has one empty line? 
            return [], []

        rows: List[Dict[str, Any]] = []
        for row in reader:
            if not row:
                continue  # Skip completely empty lines (no commas)
            row_dict: Dict[str, Any] = {}
            for i, col in enumerate(headers):
                row_dict[col] = _parse_value(row[i]) if i < len(row) else None
            rows.append(row_dict)
        return rows, headers


def _load_csv_with_header_typed(
    path: Path,
    column_types: Dict[str, Type],
    sample_cap: int = COERCE_SAMPLE_LIMIT,
    required_columns: Optional[Iterable[str]] = None,
) -> tuple[List[Dict[str, Any]], List[str], Optional[Dict[str, Any]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return [], [], None

        if not headers:
            return [], [], None

        if required_columns is not None:
            required_set = set(required_columns)
            header_set = set(headers)
            missing = required_set - header_set
            if missing:
                return [], list(headers), {"missing_columns": sorted(missing)}

        rows, summary = _coerce_csv_rows(reader, headers, column_types, sample_cap)
        return rows, headers, summary


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    rows, _headers = _load_csv_with_header(path)
    return rows


def _parse_inline_csv_text(inline_text: str) -> tuple[List[Dict[str, Any]], List[str]]:
    from io import StringIO
    buf = StringIO(inline_text.strip())
    reader = csv.reader(buf)
    try:
        headers = next(reader)
    except StopIteration:
        return [], []
    if not headers:
        return [], []
    rows_d: List[Dict[str, Any]] = []
    for row in reader:
        if not row:
            continue
        row_dict: Dict[str, Any] = {}
        for i, col in enumerate(headers):
            row_dict[col] = _parse_value(row[i]) if i < len(row) else None
        rows_d.append(row_dict)
    return rows_d, headers


def _parse_inline_csv_text_typed(
    inline_text: str,
    column_types: Dict[str, Type],
    sample_cap: int = COERCE_SAMPLE_LIMIT,
    required_columns: Optional[Iterable[str]] = None,
) -> tuple[List[Dict[str, Any]], List[str], Optional[Dict[str, Any]]]:
    from io import StringIO
    buf = StringIO(inline_text.strip())
    reader = csv.reader(buf)
    try:
        headers = next(reader)
    except StopIteration:
        return [], [], None
    if not headers:
        return [], [], None
    if required_columns is not None:
        required_set = set(required_columns)
        header_set = set(headers)
        missing = required_set - header_set
        if missing:
            return [], list(headers), {"missing_columns": sorted(missing)}
    rows_d, summary = _coerce_csv_rows(reader, headers, column_types, sample_cap)
    return rows_d, headers, summary




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
        if op in {"=", "==", "!=", "<", "<=", ">", ">="}:
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
        if op in {"=", "==", "!=", "<", "<=", ">", ">="}:
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
        if op in {"=", "==", "!=", "<", "<=", ">", ">="}:
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


def _cast_value(
    val: Any,
    to_type: str,
    on_error: str,
    trim: bool,
) -> tuple[Any, Optional[str]]:
    """
    Convert val to target type. Returns (converted_value, error_message).
    error_message is None on success; on failure returns message for evidence.
    Trim: strip leading/trailing whitespace from string before parse.
    """
    if val is None:
        return (None, None)
    s = str(val)
    if trim:
        s = s.strip()

    if to_type == "str":
        return (str(val), None)

    if to_type == "int":
        s_parse = s.strip() if trim else s
        s_parse = s_parse.strip()
        if not s_parse:
            return (None, "empty") if on_error == "null" else (None, "empty")
        try:
            return (int(s_parse), None)
        except ValueError:
            return (None, f"not_int:{s_parse!r}") if on_error == "null" else (None, f"not_int:{s_parse!r}")

    if to_type == "decimal":
        s_parse = s.strip() if trim else s
        s_parse = s_parse.strip()
        if not s_parse:
            return (None, "empty") if on_error == "null" else (None, "empty")
        try:
            return (Decimal(s_parse), None)
        except (ValueError, InvalidOperation):
            return (None, f"not_decimal:{s_parse!r}") if on_error == "null" else (None, f"not_decimal:{s_parse!r}")

    if to_type == "bool":
        s_parse = s.strip().lower() if trim else s.lower()
        if s_parse in ("true", "1", "yes"):
            return (True, None)
        if s_parse in ("false", "0", "no", ""):
            return (False if s_parse != "" else None, None if s_parse != "" else "empty")
        return (None, f"not_bool:{s!r}") if on_error == "null" else (None, f"not_bool:{s!r}")

    if to_type == "date":
        s_parse = s.strip() if trim else s
        s_parse = s_parse.strip()
        if not s_parse:
            return (None, "empty") if on_error == "null" else (None, "empty")
        try:
            dt = datetime.fromisoformat(s_parse.replace("Z", "+00:00"))
            return (dt.date().isoformat(), None)
        except (ValueError, TypeError):
            return (None, f"not_date:{s_parse!r}") if on_error == "null" else (None, f"not_date:{s_parse!r}")

    if to_type == "datetime":
        s_parse = s.strip() if trim else s
        s_parse = s_parse.strip()
        if not s_parse:
            return (None, "empty") if on_error == "null" else (None, "empty")
        try:
            dt = datetime.fromisoformat(s_parse.replace("Z", "+00:00"))
            return (dt.isoformat(), None)
        except (ValueError, TypeError):
            return (None, f"not_datetime:{s_parse!r}") if on_error == "null" else (None, f"not_datetime:{s_parse!r}")

    return (None, f"unknown_type:{to_type}")


def _execute_cast(
    step: OpStep,
    tables: Dict[str, List[Dict[str, Any]]],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Execute cast step. Returns (output_rows, evidence_dict with cast_failures, nulled)."""
    params = step.params or {}
    casts = params.get("casts") or []
    input_table = step.inputs[0]
    input_rows = tables.get(input_table, [])

    cast_failures = 0
    nulled = 0
    output_rows: List[Dict[str, Any]] = []

    for row in input_rows:
        new_row = dict(row)
        for c in casts:
            col = c.get("col", "")
            to_type = c.get("to", "str")
            on_error = c.get("on_error", "fail")
            trim = c.get("trim", False)
            val = row.get(col)
            out_val, err = _cast_value(val, to_type, on_error, trim)
            if err is not None:
                if on_error == "fail":
                    raise RuntimeFailure(
                        "SANS_RUNTIME_CAST_FAILED",
                        f"Cast {col} -> {to_type} failed: {err}",
                        _loc_to_dict(step.loc),
                    )
                cast_failures += 1
                nulled += 1
                new_row[col] = None
            else:
                if out_val is None and val is not None:
                    nulled += 1
                new_row[col] = out_val
        output_rows.append(new_row)

    evidence = {"cast_failures": cast_failures, "nulled": nulled}
    return output_rows, evidence


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


def execute_plan(
    ir_doc: IRDoc,
    bindings: Dict[str, str],
    out_dir: Path,
    output_format: str = "csv",
    outputs_base: Optional[Path] = None,
    schema_lock: Optional[Dict[str, Any]] = None,
    bundle_mode: str = "full",
) -> ExecutionResult:
    start = perf_counter()
    diagnostics: List[RuntimeDiagnostic] = []
    outputs: List[Dict[str, Any]] = []
    step_evidence: List[Dict[str, Any]] = []
    table_evidence: Dict[str, Any] = {}
    datasource_schemas: Dict[str, List[str]] = {}
    datasource_evidence: List[Dict[str, Any]] = []
    coercion_diagnostics: List[Dict[str, Any]] = []

    lock_by_name: Dict[str, Dict[str, Any]] = {}
    if schema_lock:
        from .schema_lock import lock_by_name as _lock_by_name
        lock_by_name = _lock_by_name(schema_lock)

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
            if op not in {"identity", "compute", "filter", "select", "drop", "rename", "sort", "cast", "datasource", "data_step", "transpose", "sql_select", "format", "aggregate", "summary", "save", "assert", "let_scalar", "const"}:
                raise RuntimeFailure(
                    "SANS_CAP_UNSUPPORTED_OP",
                    f"Unsupported operation '{op}'",
                    _loc_to_dict(step.loc),
                )

            if not step.inputs and op not in ("format", "assert", "let_scalar", "const", "datasource"):
                raise RuntimeFailure(
                    "SANS_RUNTIME_TABLE_UNDEFINED",
                    f"Operation '{op}' has no input table.",
                    _loc_to_dict(step.loc),
                )
            if op == "datasource":
                params = step.params or {}
                kind = params.get("kind", "csv")
                out_name = step.outputs[0] if step.outputs else None
                if not out_name:
                    raise RuntimeFailure(
                        "SANS_RUNTIME_DATASOURCE_NO_OUTPUT",
                        "Datasource step has no output.",
                        _loc_to_dict(step.loc),
                    )
                ds_name = params.get("name") or out_name
                if isinstance(ds_name, str) and ds_name.startswith("__datasource__"):
                    ds_name = ds_name.replace("__datasource__", "", 1)
                ds_sha256_for_evidence = ""
                ds_size_for_evidence = 0
                pinned_cols = params.get("columns")
                if isinstance(pinned_cols, list) and not pinned_cols:
                    pinned_cols = None
                ds_decl = ir_doc.datasources.get(ds_name) if isinstance(ds_name, str) else None
                column_types = ds_decl.column_types if ds_decl and ds_decl.column_types else None
                required_columns: Optional[List[str]] = None
                if ds_name and lock_by_name and ds_name in lock_by_name:
                    from .schema_lock import lock_entry_to_column_types, lock_entry_required_columns
                    lock_entry = lock_by_name[ds_name]
                    if not column_types:
                        column_types = lock_entry_to_column_types(lock_entry)
                    required_columns = lock_entry_required_columns(lock_entry)
                thin = bundle_mode == "thin"
                if thin:
                    data_dir = None
                    materialized_path = None
                else:
                    data_dir = out_dir / INPUTS_DATA
                    data_dir.mkdir(parents=True, exist_ok=True)
                    materialized_path = data_dir / f"{ds_name}.csv"
                if kind == "inline_csv":
                    inline_text = params.get("inline_text") or ""
                    if column_types:
                        rows_d, headers, summary = _parse_inline_csv_text_typed(
                            inline_text, column_types, required_columns=required_columns
                        )
                    else:
                        rows_d, headers = _parse_inline_csv_text(inline_text)
                        summary = None
                    if summary and summary.get("missing_columns"):
                        raise RuntimeFailure(
                            "E_SCHEMA_MISSING_COL",
                            f"Datasource '{ds_name}' missing columns: {', '.join(summary['missing_columns'])}.",
                            _loc_to_dict(step.loc),
                        )
                    if pinned_cols is not None and headers != pinned_cols:
                        raise RuntimeFailure(
                            "SANS_RUNTIME_DATASOURCE_SCHEMA_MISMATCH",
                            (
                                f"Datasource '{ds_name}' columns do not match header. "
                                f"Pinned: {pinned_cols}; Header: {headers}."
                            ),
                            _loc_to_dict(step.loc),
                        )
                    if summary:
                        if not thin and materialized_path is not None:
                            materialized_path.write_text(inline_text, encoding="utf-8")
                        coercion_diagnostics.append({
                            "datasource": ds_name,
                            "path": bundle_relative_path(materialized_path, out_dir) if materialized_path else f"inline:{ds_name}",
                            "total_rows_scanned": summary["total_rows_scanned"],
                            "columns": summary["columns"],
                            "truncated": summary.get("truncated", False),
                        })
                        inline_bytes = inline_text.encode("utf-8")
                        datasource_evidence.append({
                            "name": ds_name,
                            "table_id": out_name,
                            "kind": kind,
                            "path": str(materialized_path) if materialized_path else f"inline:{ds_name}",
                            "columns": list(headers),
                            "sha256": hashlib.sha256(inline_bytes).hexdigest(),
                            "size_bytes": len(inline_bytes),
                        })
                        raise RuntimeFailure(
                            "E_CSV_COERCE",
                            f"Datasource '{ds_name}' failed typed CSV coercion.",
                            _loc_to_dict(step.loc),
                        )
                    tables[out_name] = rows_d
                    if not thin and materialized_path is not None:
                        if rows_d:
                            _write_csv(materialized_path, rows_d)
                        else:
                            materialized_path.write_text("", encoding="utf-8")
                    inline_bytes = inline_text.encode("utf-8")
                    ds_sha256_for_evidence = hashlib.sha256(inline_bytes).hexdigest()
                    ds_size_for_evidence = len(inline_bytes)
                    datasource_schemas[out_name] = list(headers)
                else:
                    path_str = params.get("path") or ""
                    if not path_str:
                        raise RuntimeFailure(
                            "SANS_RUNTIME_DATASOURCE_NO_PATH",
                            "Datasource step (non-inline) requires path.",
                            _loc_to_dict(step.loc),
                        )
                    path = Path(path_str)
                    if not path.exists():
                        raise RuntimeFailure(
                            "SANS_RUNTIME_INPUT_NOT_FOUND",
                            f"Datasource file not found: {path_str}",
                            _loc_to_dict(step.loc),
                        )
                    if path.suffix.lower() == ".xpt":
                        try:
                            rows_d, _ = load_xpt_with_warnings(path)
                        except XptError as exc:
                            raise RuntimeFailure(exc.code, exc.message)
                        headers = list(rows_d[0].keys()) if rows_d else []
                        if pinned_cols is not None and headers != pinned_cols:
                            raise RuntimeFailure(
                                "SANS_RUNTIME_DATASOURCE_SCHEMA_MISMATCH",
                                (
                                    f"Datasource '{ds_name}' columns do not match header. "
                                    f"Pinned: {pinned_cols}; Header: {headers}."
                                ),
                                _loc_to_dict(step.loc),
                            )
                        tables[out_name] = rows_d
                        if not thin and data_dir is not None:
                            materialized_path = data_dir / f"{ds_name}.xpt"
                            import shutil
                            shutil.copy2(path, materialized_path)
                        else:
                            materialized_path = path
                        datasource_schemas[out_name] = list(headers)
                    else:
                        if not thin and data_dir is not None and materialized_path is not None:
                            import shutil
                            shutil.copy2(path, materialized_path)
                        else:
                            materialized_path = path
                        if column_types:
                            rows_d, headers, summary = _load_csv_with_header_typed(
                                path, column_types, required_columns=required_columns
                            )
                        else:
                            rows_d, headers = _load_csv_with_header(path)
                            summary = None
                        if summary and summary.get("missing_columns"):
                            raise RuntimeFailure(
                                "E_SCHEMA_MISSING_COL",
                                f"Datasource '{ds_name}' missing columns: {', '.join(summary['missing_columns'])}.",
                                _loc_to_dict(step.loc),
                            )
                        if pinned_cols is not None and headers != pinned_cols:
                            raise RuntimeFailure(
                                "SANS_RUNTIME_DATASOURCE_SCHEMA_MISMATCH",
                                (
                                    f"Datasource '{ds_name}' columns do not match header. "
                                    f"Pinned: {pinned_cols}; Header: {headers}."
                                ),
                                _loc_to_dict(step.loc),
                            )
                        if summary:
                            try:
                                path_for_diag = bundle_relative_path(materialized_path, out_dir) if materialized_path else f"inline:{ds_name}"
                            except ValueError:
                                path_for_diag = str(materialized_path)
                            coercion_diagnostics.append({
                                "datasource": ds_name,
                                "path": path_for_diag,
                                "total_rows_scanned": summary["total_rows_scanned"],
                                "columns": summary["columns"],
                                "truncated": summary.get("truncated", False),
                            })
                            datasource_evidence.append({
                                "name": ds_name,
                                "table_id": out_name,
                                "kind": kind,
                                "path": str(materialized_path) if materialized_path else f"inline:{ds_name}",
                                "columns": list(headers),
                                "sha256": (compute_input_hash(path) or ""),
                                "size_bytes": path.stat().st_size,
                            })
                            raise RuntimeFailure(
                                "E_CSV_COERCE",
                                f"Datasource '{ds_name}' failed typed CSV coercion.",
                                _loc_to_dict(step.loc),
                            )
                        tables[out_name] = rows_d
                        datasource_schemas[out_name] = list(headers)
                        read_path = path if thin else materialized_path
                        if read_path is not None:
                            ds_sha256_for_evidence = compute_input_hash(read_path) or ""
                            ds_size_for_evidence = read_path.stat().st_size
                evidence_path_str = str(materialized_path) if materialized_path else f"inline:{ds_name}"
                datasource_evidence.append({
                    "name": ds_name,
                    "table_id": out_name,
                    "kind": kind,
                    "path": evidence_path_str,
                    "columns": list(datasource_schemas.get(out_name, [])),
                    "sha256": ds_sha256_for_evidence,
                    "size_bytes": ds_size_for_evidence,
                })
                t_id = compute_transform_id(op, step.params)
                step_evidence.append({
                    "step_index": step_idx,
                    "step_id": compute_step_id(t_id, step.inputs, step.outputs),
                    "transform_id": t_id,
                    "op": op,
                    "inputs": list(step.inputs),
                    "outputs": list(step.outputs),
                    "row_counts": {out_name: len(tables[out_name])},
                })
                continue
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
                elif op == "drop":
                    cols = step.params.get("cols") or []
                    drop_set = set(cols)
                    output_rows = []
                    for row in input_rows:
                        new_row = {k: v for k, v in row.items() if k not in drop_set}
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
                elif op == "cast":
                    output_rows, cast_evidence = _execute_cast(step, tables)
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
                        "row_counts": {t: len(output_rows) for t in step.outputs},
                        "cast_failures": cast_evidence.get("cast_failures", 0),
                        "nulled": cast_evidence.get("nulled", 0),
                    })
                    continue
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
                    path = step.params.get("path") or ""
                    name = step.params.get("name")
                    base = (outputs_base if outputs_base is not None else out_dir).resolve()
                    bundle_root = out_dir.resolve()
                    try:
                        out_path = validate_save_path_under_outputs(
                            path or f"{input_table}.csv",
                            base,
                            bundle_root,
                        )
                    except ValueError as e:
                        raise RuntimeFailure(
                            "SANS_RUNTIME_SAVE_PATH_INVALID",
                            str(e),
                            _loc_to_dict(step.loc),
                        ) from e
                    table_rows = tables.get(input_table, [])
                    columns = list(table_rows[0].keys()) if table_rows else []
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    _write_csv(out_path, table_rows)
                    output_name = name or input_table
                    table_evidence[output_name] = collect_table_evidence(
                        table_rows,
                        columns=columns,
                        config=DEFAULT_EVIDENCE_CONFIG,
                    )
                    outputs.append({
                        "table": output_name,
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
            
            if op == "compute":
                columns: List[str] = []
                if output_rows:
                    columns = list(output_rows[0].keys())
                computed_evidence = collect_table_evidence(
                    output_rows,
                    columns=columns,
                    config=DEFAULT_EVIDENCE_CONFIG,
                )
                for output in step.outputs:
                    table_evidence[output] = computed_evidence

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

        write_base = (outputs_base if outputs_base is not None else out_dir).resolve()
        write_base.mkdir(parents=True, exist_ok=True)
        for table_name in emit_tables:
            table_rows = tables.get(table_name, [])
            columns: List[str] = []
            if table_rows:
                columns = list(table_rows[0].keys())

            if output_format.lower() == "xpt":
                out_path = write_base / f"{table_name}.xpt"
                try:
                    dump_xpt(out_path, table_rows, columns, dataset_name=table_name.upper())
                except XptError as exc:
                    raise RuntimeFailure(exc.code, exc.message)
            else:
                out_path = write_base / f"{table_name}.csv"
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
            step_evidence=step_evidence,
            table_evidence=table_evidence,
            datasource_schemas=datasource_schemas,
            datasource_evidence=datasource_evidence,
            coercion_diagnostics=coercion_diagnostics,
        )

    except RuntimeFailure as err:
        diagnostics.append(RuntimeDiagnostic(code=err.code, message=err.message, loc=err.loc))
        return ExecutionResult(
            status="failed",
            diagnostics=diagnostics,
            outputs=outputs,
            execute_ms=int((perf_counter() - start) * 1000),
            step_evidence=step_evidence,
            table_evidence=table_evidence,
            datasource_schemas=datasource_schemas,
            datasource_evidence=datasource_evidence,
            coercion_diagnostics=coercion_diagnostics,
        )


def _referenced_csv_datasource_names(irdoc: IRDoc) -> set:
    """Return set of datasource names that are referenced by datasource steps (csv or inline_csv)."""
    out = set()
    for step in irdoc.steps:
        if not isinstance(step, OpStep) or getattr(step, "op", None) != "datasource":
            continue
        params = step.params or {}
        if params.get("kind") not in ("csv", "inline_csv"):
            continue
        name = params.get("name")
        if not name:
            outputs = step.outputs or []
            if outputs and outputs[0].startswith("__datasource__"):
                name = outputs[0].replace("__datasource__", "", 1)
        if name:
            out.add(name)
    return out


def _resolve_datasource_path_for_inference(file_name: str, ds: Any) -> Optional[Path]:
    """Resolve CSV datasource path for lock-generation inference. Absolute paths as-is; else relative to script dir."""
    if not ds or ds.kind != "csv":
        return None
    raw = ds.path or ""
    if not raw:
        return None
    p = Path(raw)
    if p.is_absolute():
        return p
    script_dir = Path(file_name).resolve().parent
    return script_dir / raw


def _is_untyped_referenced(irdoc: Any, ds_name: str, lock_map: Dict[str, Any]) -> bool:
    """True if this referenced datasource has no typed pinning and no lock entry."""
    ds = irdoc.datasources.get(ds_name) if irdoc.datasources else None
    if not ds or ds.kind not in ("csv", "inline_csv"):
        return False
    if ds.column_types:
        return False
    if ds_name in lock_map:
        return False
    return True


def generate_schema_lock_standalone(
    text: str,
    file_name: str,
    write_path: str | Path,
    out_dir: Optional[Path] = None,
    bindings: Optional[Dict[str, str]] = None,
    schema_lock_path: Optional[Path] = None,
    include_roots: Optional[List[Path]] = None,
    allow_absolute_includes: bool = False,
    allow_include_escape: bool = False,
    strict: bool = True,
    legacy_sas: bool = False,
) -> Dict[str, Any]:
    """
    Generate schema.lock.json without execution. Writes lock to write_path.
    If out_dir is set, also writes report.json and stages inputs under out_dir (mini bundle).
    """
    import shutil
    import tempfile
    work_dir = Path(out_dir).resolve() if out_dir else Path(tempfile.mkdtemp())
    try:
        irdoc, report = emit_check_artifacts(
            text=text,
            file_name=file_name,
            tables=set(bindings.keys()) if bindings else None,
            out_dir=work_dir,
            strict=strict,
            include_roots=include_roots,
            allow_absolute_includes=allow_absolute_includes,
            allow_include_escape=allow_include_escape,
            emit_vars_graph=False,
            legacy_sas=legacy_sas,
            lock_generation_only=True,
        )
        report["bundle_mode"] = "full"
        report["bundle_format_version"] = 1
        report["datasource_inputs"] = report.get("datasource_inputs") or []
        if report.get("status") == "refused":
            if out_dir:
                (work_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
            return report

        schema_lock: Optional[Dict[str, Any]] = None
        if schema_lock_path and Path(schema_lock_path).exists():
            from .schema_lock import load_schema_lock
            schema_lock = load_schema_lock(Path(schema_lock_path))
        from .schema_lock import lock_by_name
        lock_map = lock_by_name(schema_lock) if schema_lock else {}
        referenced = _referenced_csv_datasource_names(irdoc)
        untyped = [n for n in referenced if _is_untyped_referenced(irdoc, n, lock_map)]

        script_dir = Path(file_name).resolve().parent
        for ds_name in untyped:
            ds = irdoc.datasources.get(ds_name) if irdoc.datasources else None
            if not ds or ds.kind != "csv":
                continue
            resolved = _resolve_datasource_path_for_inference(file_name, ds)
            if not resolved or not resolved.exists():
                raise RuntimeFailure(
                    "SANS_LOCK_GEN_FILE_NOT_FOUND",
                    f"Datasource '{ds_name}' file not found for schema lock generation: {resolved}",
                    None,
                )
        from .schema_infer import DEFAULT_INFER_MAX_ROWS
        from .schema_lock import build_schema_lock, compute_lock_sha256, write_schema_lock
        lock_dict = build_schema_lock(
            irdoc,
            referenced,
            schema_lock_used=schema_lock,
            sans_version=_engine_version,
            script_dir=script_dir,
            infer_untyped=bool(untyped),
            max_infer_rows=DEFAULT_INFER_MAX_ROWS,
        )
        write_path_resolved = Path(write_path).resolve()
        write_schema_lock(lock_dict, write_path_resolved)
        report["schema_lock_sha256"] = compute_lock_sha256(lock_dict)
        report["schema_lock_mode"] = "generated_only"
        report["lock_only"] = True
        report["schema_lock_emit_path"] = str(write_path_resolved)
        try:
            report["schema_lock_path"] = bundle_relative_path(write_path_resolved, work_dir)
        except ValueError:
            report["schema_lock_path"] = str(write_path_resolved)
        report["status"] = "ok"
        report["exit_code_bucket"] = 0
        report["primary_error"] = None
        report["diagnostics"] = []
        report["outputs"] = []
        report["runtime"] = {"status": "ok", "timing": {"execute_ms": None}}
        report["timing"]["execute_ms"] = None

        if out_dir:
            ensure_bundle_layout(work_dir)
            expanded_path = work_dir / INPUTS_SOURCE / "expanded.sans"
            expanded_path.write_text(irdoc_to_expanded_sans(irdoc), encoding="utf-8")
            expanded_rel = bundle_relative_path(expanded_path, work_dir)
            report["inputs"].append({
                "role": "expanded",
                "name": "expanded.sans",
                "path": expanded_rel,
                "sha256": compute_input_hash(expanded_path) or "",
            })
            data_dir = work_dir / INPUTS_DATA
            data_dir.mkdir(parents=True, exist_ok=True)
            for ds_name in referenced:
                ds = irdoc.datasources.get(ds_name) if irdoc.datasources else None
                if not ds or ds.kind not in ("csv", "inline_csv"):
                    continue
                if ds.kind == "csv":
                    src = _resolve_datasource_path_for_inference(file_name, ds)
                    if src and src.exists():
                        dest = data_dir / f"{ds_name}{src.suffix}"
                        shutil.copy2(src, dest)
                        rel = bundle_relative_path(dest, work_dir)
                        report["datasource_inputs"].append({
                            "datasource": ds_name,
                            "name": f"{ds_name}{src.suffix}",
                            "embedded": True,
                            "sha256": compute_input_hash(dest) or "",
                            "size_bytes": src.stat().st_size,
                            "path": rel,
                        })
                elif ds.kind == "inline_csv" and (ds.inline_text or "").strip():
                    raw = (ds.inline_text or "").encode("utf-8")
                    h = hashlib.sha256(raw).hexdigest()
                    size_bytes = len(raw)
                    dest = data_dir / f"{ds_name}.csv"
                    dest.write_text(ds.inline_text or "", encoding="utf-8")
                    rel = bundle_relative_path(dest, work_dir)
                    report["datasource_inputs"].append({
                        "datasource": ds_name,
                        "name": f"{ds_name}.csv",
                        "embedded": True,
                        "sha256": compute_input_hash(dest) or h,
                        "size_bytes": size_bytes,
                        "path": rel,
                    })
            report["report_sha256"] = compute_report_sha256(report, work_dir)
            (work_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        if not out_dir:
            shutil.rmtree(work_dir, ignore_errors=True)


def _resolve_schema_lock_path(lock_path: Path, script_path: Path, cwd: Path) -> Path:
    """Resolve --schema-lock path: absolute as-is; relative resolved against script directory (not out_dir)."""
    p = Path(lock_path)
    if p.is_absolute():
        return p.resolve()
    script_dir = Path(script_path).resolve().parent
    return (script_dir / p).resolve()


def _resolve_emit_schema_lock_path(emit_path: Optional[Path], out_dir: Path) -> Optional[Path]:
    """Resolve --emit-schema-lock path: relative paths are resolved against out_dir, absolute as-is."""
    if emit_path is None:
        return None
    p = Path(emit_path)
    if p.is_absolute():
        return p
    return Path(out_dir).resolve() / p


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
    legacy_sas: bool = False,
    schema_lock_path: Optional[Path] = None,
    emit_schema_lock_path: Optional[Path] = None,
    lock_only: bool = False,
    bundle_mode: str = "full",
) -> Dict[str, Any]:
    out_path = Path(out_dir).resolve()
    resolved_emit_lock_path = _resolve_emit_schema_lock_path(emit_schema_lock_path, out_path)

    # When emitting a lock, do a light compile first (no type validation) so we can discover
    # untyped datasources without failing on E_TYPE_UNKNOWN in filter/derive/etc.
    lock_generation_only = bool(emit_schema_lock_path)

    # Load schema lock before compile when provided, so compiler can apply it for type inference.
    # When --schema-lock is not provided, autodiscover lock in script dir if any referenced datasource is untyped.
    schema_lock: Optional[Dict[str, Any]] = None
    resolved_schema_lock_path: Optional[Path] = None
    schema_lock_auto_discovered: bool = False
    if schema_lock_path is not None:
        resolved_schema_lock_path = _resolve_schema_lock_path(
            Path(schema_lock_path), Path(file_name), Path.cwd()
        )
        if not resolved_schema_lock_path.exists():
            ensure_bundle_layout(out_path)
            report = {
                "report_schema_version": "0.3",
                "bundle_mode": bundle_mode,
                "bundle_format_version": 1,
                "datasource_inputs": [],
                "status": "failed",
                "exit_code_bucket": 50,
                "primary_error": None,
                "diagnostics": [],
                "inputs": [],
                "artifacts": [],
                "outputs": [],
                "schema_lock_used_path": str(resolved_schema_lock_path),
                "schema_lock_sha256": None,
                "schema_lock_applied_datasources": [],
                "schema_lock_missing_datasources": [],
            }
            report["primary_error"] = {
                "code": "E_SCHEMA_LOCK_NOT_FOUND",
                "message": f"Schema lock file not found: {resolved_schema_lock_path}",
                "loc": None,
            }
            report["diagnostics"] = [report["primary_error"]]
            report["runtime"] = {"status": "failed", "timing": {"execute_ms": None}}
            report_path = Path(out_dir) / "report.json"
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            return report
        try:
            from .schema_lock import load_schema_lock
            schema_lock = load_schema_lock(resolved_schema_lock_path)
        except Exception as e:
            ensure_bundle_layout(out_path)
            report = {
                "report_schema_version": "0.3",
                "bundle_mode": bundle_mode,
                "bundle_format_version": 1,
                "datasource_inputs": [],
                "status": "failed",
                "exit_code_bucket": 50,
                "primary_error": None,
                "diagnostics": [],
                "inputs": [],
                "artifacts": [],
                "outputs": [],
                "schema_lock_used_path": str(resolved_schema_lock_path),
                "schema_lock_sha256": None,
                "schema_lock_applied_datasources": [],
                "schema_lock_missing_datasources": [],
            }
            report["primary_error"] = {
                "code": "E_SCHEMA_LOCK_NOT_FOUND",
                "message": f"Schema lock file invalid at {resolved_schema_lock_path}: {e}",
                "loc": None,
            }
            report["diagnostics"] = [report["primary_error"]]
            report["runtime"] = {"status": "failed", "timing": {"execute_ms": None}}
            report_path = Path(out_dir) / "report.json"
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            return report
        schema_lock_path = resolved_schema_lock_path
    elif not lock_generation_only and Path(file_name).suffix.lower() == ".sans":
        # Autodiscovery: if any referenced csv/inline_csv has no pins, look for lock in script dir.
        from .compiler import compile_sans_script
        try:
            irdoc_pre = compile_sans_script(
                text=text,
                file_name=file_name,
                tables=set(bindings.keys()) if bindings else None,
                skip_type_validation=True,
            )
        except Exception:
            # Parse/lower failed; let full compile run and report the real error.
            irdoc_pre = None
        if irdoc_pre is not None:
            refs = _referenced_csv_datasource_names(irdoc_pre)
            untyped_refs = [
                n for n in refs
                if irdoc_pre.datasources.get(n)
                and irdoc_pre.datasources[n].kind in ("csv", "inline_csv")
                and not irdoc_pre.datasources[n].column_types
            ]
            if untyped_refs:
                script_dir = Path(file_name).resolve().parent
                script_stem = Path(file_name).stem
                candidates = [
                    script_dir / f"{script_stem}.schema.lock.json",
                    script_dir / "schema.lock.json",
                ]
                found: Optional[Path] = None
                for p in candidates:
                    if p.exists():
                        found = p
                        break
                if found is not None:
                    from .schema_lock import load_schema_lock
                    schema_lock = load_schema_lock(found)
                    resolved_schema_lock_path = found
                    schema_lock_path = found
                    schema_lock_auto_discovered = True
                else:
                    ensure_bundle_layout(out_path)
                    paths_msg = ", ".join(str(p) for p in candidates)
                    script_name = Path(file_name).name or "script.sans"
                    report = {
                        "report_schema_version": "0.3",
                        "bundle_mode": bundle_mode,
                        "bundle_format_version": 1,
                        "datasource_inputs": [],
                        "status": "refused",
                        "exit_code_bucket": 50,
                        "primary_error": {
                            "code": "E_SCHEMA_REQUIRED",
                            "message": (
                                f"Schema lock required (untyped datasource(s): {', '.join(sorted(untyped_refs))}) but not found. "
                                f"Looked for: {paths_msg}. Generate with: sans schema-lock {script_name}"
                            ),
                            "loc": None,
                        },
                        "diagnostics": [],
                        "inputs": [],
                        "artifacts": [],
                        "outputs": [],
                        "schema_lock_auto_discovered": True,
                        "schema_lock_used_path": None,
                        "schema_lock_sha256": None,
                        "schema_lock_applied_datasources": [],
                        "schema_lock_missing_datasources": [],
                        "engine": {"name": "sans", "version": _engine_version},
                        "settings": {"strict": strict, "allow_approx": False, "tolerance": None, "tables": []},
                        "timing": {"compile_ms": None, "validate_ms": None, "execute_ms": None},
                    }
                    report["runtime"] = {"status": "refused", "timing": {"execute_ms": None}}
                    report["diagnostics"] = [report["primary_error"]]
                    report["report_sha256"] = compute_report_sha256(report, out_path)
                    report_path = out_path / "report.json"
                    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
                    return report

    # Reuse loaded lock when lock_generation_only so we don't load twice
    schema_lock_for_emit = schema_lock if (schema_lock is not None and not lock_generation_only) else None
    resolved_for_emit = resolved_schema_lock_path if (resolved_schema_lock_path is not None and not lock_generation_only) else None

    irdoc, report = emit_check_artifacts(
        text=text,
        file_name=file_name,
        tables=set(bindings.keys()) if bindings else None,
        out_dir=out_dir,
        strict=strict,
        include_roots=include_roots,
        allow_absolute_includes=allow_absolute_includes,
        allow_include_escape=allow_include_escape,
        emit_vars_graph=False,
        legacy_sas=legacy_sas,
        lock_generation_only=lock_generation_only,
        schema_lock=schema_lock_for_emit,
        schema_lock_path_resolved=resolved_for_emit,
    )
    # Autodiscovery visibility: report always has schema_lock_auto_discovered (true when lock was autodiscovered).
    # When a lock was used (explicit or autodiscovered), compiler already set schema_lock_used_path and schema_lock_sha256.
    report["schema_lock_auto_discovered"] = schema_lock_auto_discovered
    report["bundle_mode"] = bundle_mode
    report["bundle_format_version"] = 1
    report["datasource_inputs"] = report.get("datasource_inputs") or []

    # If compilation/validation refused, annotate runtime and exit.
    if report.get("status") == "refused":
        report["runtime"] = {"status": "refused", "timing": {"execute_ms": None}}
        report_path = Path(out_dir) / "report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    ensure_bundle_layout(out_path)

    # Write expanded.sans to inputs/source/ and add to report inputs (role=expanded)
    expanded_path = out_path / INPUTS_SOURCE / "expanded.sans"
    expanded_path.write_text(irdoc_to_expanded_sans(irdoc), encoding="utf-8")
    expanded_rel = bundle_relative_path(expanded_path, out_path)
    expanded_sha = compute_input_hash(expanded_path) or ""
    report["inputs"].append({
        "role": "expanded",
        "name": "expanded.sans",
        "path": expanded_rel,
        "sha256": expanded_sha,
    })

    # Materialize bindings to inputs/data/<logical_name> (logical name only)
    bindings_in: Dict[str, str] = {}
    try:
        referenced = _referenced_csv_datasource_names(irdoc)
        from .schema_lock import lock_by_name
        lock_map = lock_by_name(schema_lock) if schema_lock else {}
        untyped = [
            ds_name for ds_name in referenced
            if _is_untyped_referenced(irdoc, ds_name, lock_map)
        ]

        # Lock-only path: emit schema lock without executing (untyped refs, or --lock-only)
        if emit_schema_lock_path and (untyped or lock_only):
            import shutil
            from .schema_infer import DEFAULT_INFER_MAX_ROWS
            from .schema_lock import build_schema_lock, compute_lock_sha256, write_schema_lock
            script_dir = Path(file_name).resolve().parent
            for ds_name in untyped:
                ds = irdoc.datasources.get(ds_name) if irdoc.datasources else None
                if not ds:
                    continue
                if ds.kind == "csv":
                    resolved = _resolve_datasource_path_for_inference(file_name, ds)
                    if not resolved or not resolved.exists():
                        raise RuntimeFailure(
                            "SANS_LOCK_GEN_FILE_NOT_FOUND",
                            f"Datasource '{ds_name}' file not found for schema lock generation: {resolved}",
                            None,
                        )
            lock_dict = build_schema_lock(
                irdoc,
                referenced,
                schema_lock_used=schema_lock,
                sans_version=_engine_version,
                script_dir=script_dir,
                infer_untyped=bool(untyped),
                max_infer_rows=DEFAULT_INFER_MAX_ROWS,
            )
            write_schema_lock(lock_dict, resolved_emit_lock_path)
            report["schema_lock_sha256"] = compute_lock_sha256(lock_dict)
            report["schema_lock_mode"] = "generated_only"
            report["lock_only"] = True
            report["schema_lock_emit_path"] = str(resolved_emit_lock_path)
            try:
                report["schema_lock_path"] = bundle_relative_path(resolved_emit_lock_path, out_path)
            except ValueError:
                report["schema_lock_path"] = str(resolved_emit_lock_path)
            # Stage referenced datasource files into out_dir/inputs (or record fingerprints only in thin mode)
            data_dir = out_path / INPUTS_DATA
            if bundle_mode == "full":
                data_dir.mkdir(parents=True, exist_ok=True)
            for ds_name in referenced:
                ds = irdoc.datasources.get(ds_name) if irdoc.datasources else None
                if not ds or ds.kind not in ("csv", "inline_csv"):
                    continue
                if ds.kind == "csv":
                    src = _resolve_datasource_path_for_inference(file_name, ds)
                    if src and src.exists():
                        if bundle_mode == "full":
                            dest = data_dir / f"{ds_name}{src.suffix}"
                            shutil.copy2(src, dest)
                            rel = bundle_relative_path(dest, out_path)
                            report["datasource_inputs"].append({
                                "datasource": ds_name,
                                "name": f"{ds_name}{src.suffix}",
                                "embedded": True,
                                "sha256": compute_input_hash(dest) or "",
                                "size_bytes": src.stat().st_size,
                                "path": rel,
                            })
                        else:
                            h = compute_input_hash(src) or ""
                            report["datasource_inputs"].append({
                                "datasource": ds_name,
                                "name": f"{ds_name}{src.suffix}",
                                "embedded": False,
                                "sha256": h,
                                "size_bytes": src.stat().st_size,
                                "ref": f"sha256:{h}",
                            })
                elif ds.kind == "inline_csv" and (ds.inline_text or "").strip():
                    raw = (ds.inline_text or "").encode("utf-8")
                    h = hashlib.sha256(raw).hexdigest()
                    size_bytes = len(raw)
                    if bundle_mode == "full":
                        data_dir.mkdir(parents=True, exist_ok=True)
                        dest = data_dir / f"{ds_name}.csv"
                        dest.write_text(ds.inline_text or "", encoding="utf-8")
                        rel = bundle_relative_path(dest, out_path)
                        report["datasource_inputs"].append({
                            "datasource": ds_name,
                            "name": f"{ds_name}.csv",
                            "embedded": True,
                            "sha256": compute_input_hash(dest) or h,
                            "size_bytes": size_bytes,
                            "path": rel,
                        })
                    else:
                        report["datasource_inputs"].append({
                            "datasource": ds_name,
                            "name": f"{ds_name}.csv",
                            "embedded": False,
                            "sha256": h,
                            "size_bytes": size_bytes,
                            "ref": f"sha256:{h}",
                        })
            report["status"] = "ok"
            report["exit_code_bucket"] = 0
            report["primary_error"] = None
            report["diagnostics"] = []
            report["outputs"] = []
            report["runtime"] = {"status": "ok", "timing": {"execute_ms": None}}
            report["timing"]["execute_ms"] = None
            report["report_sha256"] = compute_report_sha256(report, out_path)
            report_path = out_path / "report.json"
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            return report

        # We had emit_schema_lock_path but no untyped refs; need full compile for execution
        if lock_generation_only:
            irdoc, report = emit_check_artifacts(
                text=text,
                file_name=file_name,
                tables=set(bindings.keys()) if bindings else None,
                out_dir=out_dir,
                strict=strict,
                include_roots=include_roots,
                allow_absolute_includes=allow_absolute_includes,
                allow_include_escape=allow_include_escape,
                emit_vars_graph=False,
                legacy_sas=legacy_sas,
                lock_generation_only=False,
            )
            report["bundle_mode"] = bundle_mode
            report["bundle_format_version"] = 1
            report["datasource_inputs"] = report.get("datasource_inputs") or []
            if report.get("status") == "refused":
                report["runtime"] = {"status": "refused", "timing": {"execute_ms": None}}
                report_path = out_path / "report.json"
                report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
                return report
            # Write expanded.sans and add to report inputs (compiler does not add expanded)
            expanded_path.write_text(irdoc_to_expanded_sans(irdoc), encoding="utf-8")
            expanded_sha = compute_input_hash(expanded_path) or ""
            report["inputs"].append({
                "role": "expanded",
                "name": "expanded.sans",
                "path": expanded_rel,
                "sha256": expanded_sha,
            })

        # Copy provided schema lock into out_dir so the bundle is self-contained
        if schema_lock_path and schema_lock:
            import shutil
            copy_dest = out_path / "schema.lock.json"
            shutil.copy2(Path(schema_lock_path), copy_dest)
            report["schema_lock_used_path"] = str(Path(schema_lock_path).resolve())
            report["schema_lock_copied_path"] = "schema.lock.json"

        for ds_name in referenced:
            ds = irdoc.datasources.get(ds_name) if irdoc.datasources else None
            if not ds or ds.kind not in ("csv", "inline_csv"):
                continue
            if ds.column_types:
                continue
            if not schema_lock:
                raise RuntimeFailure(
                    "E_SCHEMA_REQUIRED",
                    f"Datasource '{ds_name}' has no typed columns. Provide --schema-lock or typed columns(...).",
                    None,
                )
            if ds_name not in lock_map:
                raise RuntimeFailure(
                    "E_SCHEMA_REQUIRED",
                    f"Datasource '{ds_name}' has no typed columns and is not in schema lock. Provide --schema-lock with entry for '{ds_name}' or typed columns(...).",
                    None,
                )

        if bindings:
            data_dir = out_path / INPUTS_DATA
            if bundle_mode == "full":
                data_dir.mkdir(parents=True, exist_ok=True)
            for name, path_str in bindings.items():
                p = Path(path_str)
                if not p.exists():
                    raise RuntimeFailure(
                        "SANS_RUNTIME_INPUT_NOT_FOUND",
                        f"Input table '{name}' file not found: {path_str}",
                    )
                h = compute_input_hash(p) or ""
                size_bytes = p.stat().st_size
                if bundle_mode == "full":
                    import shutil
                    dest = data_dir / f"{name}{p.suffix}"
                    shutil.copy2(p, dest)
                    bindings_in[name] = str(dest)
                    rel = bundle_relative_path(dest, out_path)
                    report["datasource_inputs"].append({
                        "datasource": name,
                        "name": f"{name}{p.suffix}",
                        "embedded": True,
                        "sha256": compute_input_hash(dest) or h,
                        "size_bytes": size_bytes,
                        "path": rel,
                    })
                else:
                    bindings_in[name] = path_str
                    report["datasource_inputs"].append({
                        "datasource": name,
                        "name": f"{name}{p.suffix}",
                        "embedded": False,
                        "sha256": h,
                        "size_bytes": size_bytes,
                        "ref": f"sha256:{h}",
                    })

        result = execute_plan(
            irdoc,
            bindings_in,
            out_path,
            output_format=output_format,
            outputs_base=out_path / OUTPUTS,
            schema_lock=schema_lock,
            bundle_mode=bundle_mode,
        )
    except RuntimeFailure as err:
        report["runtime"] = {"status": "failed", "timing": {"execute_ms": None}}
        report["timing"]["execute_ms"] = None
        report["status"] = "failed"
        report["exit_code_bucket"] = 50
        report["primary_error"] = {
            "code": err.code,
            "message": err.message,
            "loc": getattr(err, "loc", None),
        }
        report["diagnostics"] = [report["primary_error"]]
        report["outputs"] = []
        report["report_sha256"] = compute_report_sha256(report, out_path)
        report_path = out_path / "report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report

    report["runtime"] = {
        "status": result.status,
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

    # Build report["outputs"] from result.outputs (bundle-relative paths, required sha256); dedupe by (name, path)
    bundle_root = out_path
    report["outputs"] = []
    seen_output_keys: set[tuple[str, str]] = set()
    for out in result.outputs:
        abs_path = Path(out["path"]).resolve()
        try:
            rel_posix = bundle_relative_path(abs_path, bundle_root)
        except ValueError:
            raise RuntimeError(
                f"Runtime output path is outside bundle: {abs_path} (bundle={bundle_root})"
            ) from None
        h = compute_artifact_hash(abs_path)
        if not h:
            h = compute_raw_hash(abs_path) or ""
        out_key = (out["table"], rel_posix)
        if out_key in seen_output_keys:
            continue
        seen_output_keys.add(out_key)
        report["outputs"].append({
            "name": out["table"],
            "path": rel_posix,
            "sha256": h,
            "rows": out["rows"],
            "columns": out["columns"],
        })

    # Populate datasource_inputs from execution when not already set by bindings (script-resolved datasources)
    existing_ds = {d["datasource"] for d in report.get("datasource_inputs", [])}
    if result.datasource_evidence:
        for e in sorted(result.datasource_evidence, key=lambda x: x.get("name", "")):
            if e["name"] in existing_ds:
                continue
            existing_ds.add(e["name"])
            entry = {
                "datasource": e["name"],
                "name": e["name"] + ".csv",
                "embedded": bundle_mode == "full",
                "sha256": e.get("sha256", ""),
                "size_bytes": e.get("size_bytes", 0),
            }
            if bundle_mode == "full":
                try:
                    entry["path"] = bundle_relative_path(Path(e["path"]), bundle_root)
                except (ValueError, TypeError):
                    entry["path"] = str(e.get("path", ""))
            else:
                entry["ref"] = f"sha256:{e.get('sha256', '')}"
            report["datasource_inputs"].append(entry)

    # Rebuild vars.graph.json with runtime-resolved datasource schemas (if available).
    vars_graph_path = out_path / ARTIFACTS / "vars.graph.json"
    initial_schema = {}
    if result.datasource_schemas:
        for table_id, cols in result.datasource_schemas.items():
            initial_schema[table_id] = list(cols)
    vars_graph = build_var_graph(irdoc, initial_schema=initial_schema)
    write_vars_graph_json(vars_graph, vars_graph_path)
    vars_graph_rel = bundle_relative_path(vars_graph_path, bundle_root)
    for artifact in report.get("artifacts", []):
        if artifact.get("name") == "vars.graph.json":
            artifact["path"] = vars_graph_rel
            artifact["sha256"] = compute_artifact_hash(vars_graph_path) or ""
            break

    # 1. Emit registry.candidate.json under artifacts/
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
                from .sans_script.canon import _canonicalize
                transform_entry = {
                    "transform_id": t_id,
                    "kind": step.op,
                    "spec": {
                        "op": step.op,
                        "params": _canonicalize(step.params or {}),
                    }
                }
                registry["transforms"].append(transform_entry)
                seen_transforms[t_id] = True
            registry["index"][str(step_idx)] = t_id

    registry_path = out_path / ARTIFACTS / "registry.candidate.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps(registry, indent=2), encoding="utf-8")
    registry_rel = bundle_relative_path(registry_path, bundle_root)
    report["artifacts"].append({
        "name": "registry.candidate.json",
        "path": registry_rel,
        "sha256": compute_artifact_hash(registry_path) or "",
    })

    # 2. Emit runtime.evidence.json under artifacts/
    plan_ir_path = out_path / ARTIFACTS / "plan.ir.json"
    tables_evidence = {}
    if result.table_evidence:
        for name in sorted(result.table_evidence.keys()):
            tables_evidence[name] = result.table_evidence[name]
    evidence = {
        "sans_version": _engine_version,
        "plan_ir": {
            "path": bundle_relative_path(plan_ir_path, bundle_root),
            "sha256": compute_raw_hash(plan_ir_path) or "",
        },
        "bindings": {name: Path(p).name for name, p in bindings_in.items()},
        "inputs": [],
        "outputs": [],
        "step_evidence": result.step_evidence or [],
        "tables": tables_evidence,
    }
    if result.datasource_evidence:
        datasources = {}
        for entry in sorted(result.datasource_evidence, key=lambda e: e.get("name", "")):
            abs_path = Path(entry["path"]).resolve()
            try:
                rel_path = bundle_relative_path(abs_path, bundle_root)
            except ValueError:
                # Thin mode: path is outside bundle; use reference placeholder
                rel_path = f"inputs/data/{entry.get('name', '')}.csv"
            datasources[entry["name"]] = {
                "kind": entry.get("kind"),
                "path": rel_path,
                "columns": entry.get("columns") or [],
            }
        evidence["datasources"] = datasources
    if result.coercion_diagnostics:
        evidence["coercion_diagnostics"] = sorted(
            result.coercion_diagnostics,
            key=lambda e: e.get("datasource", ""),
        )
    seen_outputs: set[tuple[str, str]] = set()
    for out in report["outputs"]:
        key = (out["name"], out["path"])
        if key in seen_outputs:
            continue
        seen_outputs.add(key)
        evidence["outputs"].append({
            "name": out["name"],
            "path": out["path"],
            "row_count": out["rows"],
            "columns": out["columns"],
        })
    for inp in report.get("inputs", []):
        if inp.get("role") == "datasource":
            evidence["inputs"].append({
                "name": inp.get("name"),
                "path": inp.get("path"),
                "sha256": inp.get("sha256"),
            })
    for d in report.get("datasource_inputs", []):
        evidence["inputs"].append({
            "name": d.get("datasource"),
            "path": d.get("path"),
            "sha256": d.get("sha256"),
        })

    evidence_path = out_path / ARTIFACTS / "runtime.evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    evidence_rel = bundle_relative_path(evidence_path, bundle_root)
    report["artifacts"].append({
        "name": "runtime.evidence.json",
        "path": evidence_rel,
        "sha256": compute_artifact_hash(evidence_path) or "",
    })

    if resolved_emit_lock_path is not None and result.status in ("ok", "ok_warnings"):
        from .schema_lock import build_schema_lock, write_schema_lock, compute_lock_sha256
        referenced = _referenced_csv_datasource_names(irdoc)
        lock_dict = build_schema_lock(irdoc, referenced, schema_lock_used=schema_lock, sans_version=_engine_version)
        write_schema_lock(lock_dict, resolved_emit_lock_path)
        report["schema_lock_sha256"] = compute_lock_sha256(lock_dict)
        report["schema_lock_mode"] = "ran_and_emitted"
        report["lock_only"] = False
        report["schema_lock_emit_path"] = str(resolved_emit_lock_path)
        try:
            report["schema_lock_path"] = bundle_relative_path(resolved_emit_lock_path, bundle_root)
        except ValueError:
            report["schema_lock_path"] = str(resolved_emit_lock_path)
    elif schema_lock:
        from .schema_lock import compute_lock_sha256
        report["schema_lock_sha256"] = compute_lock_sha256(schema_lock)

    report_path = out_path / "report.json"
    report["report_sha256"] = compute_report_sha256(report, bundle_root)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
