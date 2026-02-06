from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
from decimal import Decimal


class Type(Enum):
    NULL = "null"
    BOOL = "bool"
    INT = "int"
    DECIMAL = "decimal"
    STRING = "string"
    UNKNOWN = "unknown"

TYPE_NAME_MAP: dict[str, Type] = {
    "null": Type.NULL,
    "bool": Type.BOOL,
    "int": Type.INT,
    "decimal": Type.DECIMAL,
    "string": Type.STRING,
    "str": Type.STRING,
    "unknown": Type.UNKNOWN,
}


def type_name(t: Type | None) -> str:
    if t is None:
        return "unknown"
    return t.value


def parse_type_name(name: str) -> Type:
    key = name.strip().lower()
    if key in TYPE_NAME_MAP:
        return TYPE_NAME_MAP[key]
    raise ValueError(f"unknown type name: {name}")


def is_numeric(t: Type | None) -> bool:
    return t in (Type.INT, Type.DECIMAL)


def is_unknown(t: Type | None) -> bool:
    return t is None or t == Type.UNKNOWN


def from_literal(value: Any) -> Type:
    if value is None:
        return Type.NULL
    if isinstance(value, bool):
        return Type.BOOL
    if isinstance(value, int):
        return Type.INT
    if isinstance(value, float):
        # No float type; treat as decimal.
        return Type.DECIMAL
    if isinstance(value, Decimal):
        return Type.DECIMAL
    if isinstance(value, str):
        return Type.STRING
    if isinstance(value, dict) and value.get("type") == "decimal" and isinstance(value.get("value"), str):
        return Type.DECIMAL
    return Type.UNKNOWN


def promote_numeric(left: Type, right: Type) -> Type:
    if left == Type.DECIMAL or right == Type.DECIMAL:
        return Type.DECIMAL
    return Type.INT


def unify(left: Type, right: Type, context: str = "if") -> Type:
    if left == right:
        return left
    if is_unknown(left) or is_unknown(right):
        return Type.UNKNOWN
    if context == "if":
        if left == Type.NULL:
            return right
        if right == Type.NULL:
            return left
    if is_numeric(left) and is_numeric(right):
        return promote_numeric(left, right)
    raise TypeError(f"cannot unify {left.value} with {right.value}")
