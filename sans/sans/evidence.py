from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class EvidenceConfig:
    unique_cap: int = 2048
    topk: int = 5
    include_top_values: bool = True
    sample_cap: int = 100000


DEFAULT_EVIDENCE_CONFIG = EvidenceConfig()


def _normalize_decimal_string(s: str) -> str:
    s = s.strip()
    if s.startswith("+"):
        s = s[1:]
    if s.startswith("."):
        s = "0" + s
    elif s.startswith("-."):
        s = "-0." + s[2:]
    if s.endswith(".") and len(s) > 1:
        s = s[:-1]
    if "." in s:
        int_part, frac_part = s.split(".", 1)
    else:
        int_part, frac_part = s, ""
    if int_part.startswith("-"):
        sign = "-"
        int_part = int_part[1:]
    else:
        sign = ""
    int_part = int_part.lstrip("0") or "0"
    frac_part = frac_part.rstrip("0")
    result = sign + int_part + ("." + frac_part if frac_part else "")
    if result == "-0":
        return "0"
    return result


def _decimal_to_string(value: Decimal) -> str:
    text = format(value, "f")
    return _normalize_decimal_string(text)


def _normalize_value(value: Any) -> Tuple[Tuple[str, Any], Any, str, str]:
    if value is None:
        return ("null", None), None, "null", "null"
    if isinstance(value, bool):
        return ("bool", value), value, "bool", "true" if value else "false"
    if isinstance(value, int):
        return ("int", value), value, "int", str(value)
    if isinstance(value, Decimal):
        dec_str = _decimal_to_string(value)
        return ("decimal", dec_str), dec_str, "decimal", dec_str
    if isinstance(value, float):
        text = repr(value)
        return ("float", text), text, "unknown", text
    if isinstance(value, str):
        return ("string", value), value, "string", value
    text = str(value)
    return ("unknown", text), text, "unknown", text


class _ColumnCollector:
    def __init__(self, config: EvidenceConfig) -> None:
        self.config = config
        self.null_count = 0
        self.non_null_count = 0
        self.unique_overflow = False
        self.unique_keys: set[Tuple[str, Any]] = set()
        self.value_info: Dict[Tuple[str, Any], Tuple[Any, str]] = {}
        self.counts: Optional[Dict[Tuple[str, Any], int]] = {} if config.include_top_values and config.topk > 0 else None
        self.constant_key: Optional[Tuple[str, Any]] = None
        self.constant_value: Optional[Any] = None
        self.constant_broken = False
        self.type_tags: set[str] = set()

    def observe(self, value: Any) -> None:
        if value is None:
            self.null_count += 1
            return
        self.non_null_count += 1
        key, out_value, type_tag, sort_key = _normalize_value(value)
        self.type_tags.add(type_tag)

        if self.constant_key is None:
            self.constant_key = key
            self.constant_value = out_value
        elif key != self.constant_key:
            self.constant_broken = True

        if not self.unique_overflow:
            if key not in self.unique_keys:
                if len(self.unique_keys) >= self.config.unique_cap:
                    self.unique_overflow = True
                    self.counts = None
                else:
                    self.unique_keys.add(key)
                    self.value_info[key] = (out_value, sort_key)
            if not self.unique_overflow and self.counts is not None:
                self.counts[key] = self.counts.get(key, 0) + 1

    def to_dict(self) -> Dict[str, Any]:
        if self.unique_overflow:
            unique_count: Any = f">={self.config.unique_cap + 1}"
        else:
            unique_count = len(self.unique_keys)
        out: Dict[str, Any] = {
            "null_count": self.null_count,
            "non_null_count": self.non_null_count,
            "unique_count": unique_count,
            "unique_count_capped": bool(self.unique_overflow),
        }
        if (not self.unique_overflow) and unique_count == 1 and self.null_count == 0 and not self.constant_broken:
            out["constant_value"] = self.constant_value

        if self.counts is not None:
            items = []
            for key, count in self.counts.items():
                value, sort_key = self.value_info.get(key, (None, ""))
                items.append((count, sort_key, key, value))
            items.sort(key=lambda x: (-x[0], x[1], x[2][0]))
            top = []
            for count, _, _, value in items[: self.config.topk]:
                top.append({"value": value, "count": count})
            if top:
                out["top_values"] = top

        if not self.type_tags:
            out["type_hint"] = "null"
        elif len(self.type_tags) == 1:
            out["type_hint"] = next(iter(self.type_tags))
        else:
            out["type_hint"] = "unknown"
        return out


def _sample_indices(row_count: int, sample_cap: int) -> Tuple[Iterable[int], bool, int, int]:
    if row_count <= sample_cap:
        return range(row_count), False, row_count, 1
    step = max(1, row_count // sample_cap)
    return range(0, row_count, step), True, min(sample_cap, (row_count + step - 1) // step), step


def collect_table_evidence(
    rows: List[Dict[str, Any]],
    columns: Optional[List[str]] = None,
    config: EvidenceConfig = DEFAULT_EVIDENCE_CONFIG,
) -> Dict[str, Any]:
    row_count = len(rows)
    if columns is None:
        columns = list(rows[0].keys()) if rows else []

    indices, sampled, sample_size, sample_step = _sample_indices(row_count, config.sample_cap)
    collectors = {col: _ColumnCollector(config) for col in columns}

    seen = 0
    for idx in indices:
        if sampled and seen >= sample_size:
            break
        row = rows[idx]
        for col in columns:
            collectors[col].observe(row.get(col))
        seen += 1

    columns_evidence: Dict[str, Any] = {}
    for col in sorted(columns):
        columns_evidence[col] = collectors[col].to_dict()

    evidence: Dict[str, Any] = {
        "row_count": row_count,
        "columns": columns_evidence,
    }
    if sampled:
        evidence["sample"] = {
            "strategy": "stride",
            "cap": config.sample_cap,
            "size": sample_size,
            "step": sample_step,
        }
    return evidence
