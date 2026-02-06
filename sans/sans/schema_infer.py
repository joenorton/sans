"""
Deterministic CSV schema inference for schema lock generation only.
Scans up to max_rows; infers a single type per column using a monotonic rule.
Empty/whitespace tokens are treated as null and ignored for inference.
"""
from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
from typing import Any, List, Optional, Tuple

from decimal import Decimal, InvalidOperation


DEFAULT_INFER_MAX_ROWS = 10_000
INFERENCE_POLICY_VERSION = 1


def _token_requires_string(s: str) -> bool:
    """True if token must be treated as string (e.g. leading zeros, non-numeric)."""
    t = s.strip()
    if not t:
        return False  # null, handled elsewhere
    # Leading zeros: "01", "00" etc. are often codes; treat as string
    if t.isdigit() and len(t) > 1 and t.startswith("0"):
        return True
    if t.startswith("-") and t[1:].isdigit() and len(t) > 2 and t[1] == "0":
        return True
    # Non-numeric (and not strict bool) => string
    if t.lower() in ("true", "false"):
        return False
    try:
        int(t)
        return False
    except ValueError:
        pass
    try:
        Decimal(t)
        return False
    except (ValueError, InvalidOperation):
        return True


def _token_kind(s: str) -> str:
    """Return one of: 'null', 'string', 'decimal', 'int', 'bool'. Null tokens are ignored for inference."""
    t = s.strip()
    if not t:
        return "null"
    if t.lower() in ("true", "false"):
        return "bool"
    if _token_requires_string(s):
        return "string"
    # Has decimal point or scientific => decimal
    if "." in t or "e" in t.lower():
        try:
            Decimal(t)
            return "decimal"
        except (ValueError, InvalidOperation):
            return "string"
    try:
        int(t)
        return "int"
    except ValueError:
        pass
    try:
        Decimal(t)
        return "decimal"
    except (ValueError, InvalidOperation):
        return "string"


def _infer_column_type(kinds: List[str]) -> str:
    """
    Monotonic rule: if any string => string; else if any decimal => decimal;
    else if any int => int; else if all non-null are bool => bool; else string.
    """
    non_null = [k for k in kinds if k != "null"]
    if not non_null:
        return "string"
    if "string" in non_null:
        return "string"
    if "decimal" in non_null:
        return "decimal"
    if "int" in non_null:
        return "int"
    if all(k == "bool" for k in non_null):
        return "bool"
    return "string"


def infer_csv_schema(
    path: Optional[Path] = None,
    content: Optional[str] = None,
    max_rows: int = DEFAULT_INFER_MAX_ROWS,
) -> Tuple[List[dict], int, bool]:
    """
    Infer column names and types from CSV (file or inline content).
    Exactly one of path or content must be provided.

    Returns:
        (columns: [{"name": str, "type": str}], rows_scanned: int, truncated: bool)
    """
    if (path is None) == (content is None):
        raise ValueError("Exactly one of path or content must be provided")
    if max_rows < 0:
        max_rows = 0

    if path is not None:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            return _infer_from_reader(reader, max_rows)
    else:
        reader = csv.reader(StringIO(content or ""))
        return _infer_from_reader(reader, max_rows)


def _infer_from_reader(
    reader: Any,
    max_rows: int,
) -> Tuple[List[dict], int, bool]:
    try:
        headers = next(reader)
    except StopIteration:
        return [], 0, False
    if not headers:
        return [], 0, False
    # Column names in header order; normalize empty names to avoid duplicates
    column_names = [h.strip() if h.strip() else f"_col{i}" for i, h in enumerate(headers)]
    # Per-column list of token kinds seen
    num_cols = len(column_names)
    column_kinds: List[List[str]] = [[] for _ in range(num_cols)]
    rows_scanned = 0
    truncated = False
    for row in reader:
        if rows_scanned >= max_rows:
            truncated = True
            break
        for i in range(num_cols):
            token = row[i] if i < len(row) else ""
            kind = _token_kind(token)
            column_kinds[i].append(kind)
        rows_scanned += 1
    inferred_types = [_infer_column_type(kinds) for kinds in column_kinds]
    columns = [{"name": name, "type": typ} for name, typ in zip(column_names, inferred_types)]
    return columns, rows_scanned, truncated
