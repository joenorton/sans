from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sans.expr import ExprNode


@dataclass
class SourceSpan:
    start: int
    end: int


@dataclass
class FormatEntry:
    key: str
    value: str
    is_other: bool


@dataclass
class FormatStmt:
    name: str
    entries: List[FormatEntry]
    span: SourceSpan


@dataclass
class AssignmentStmt:
    target: str
    expr: ExprNode
    span: SourceSpan


@dataclass
class FilterStmt:
    predicate: ExprNode
    span: SourceSpan


@dataclass
class RenameStmt:
    mappings: Dict[str, str]
    span: SourceSpan


@dataclass
class KeepStmt:
    columns: List[str]
    span: SourceSpan


@dataclass
class DropStmt:
    columns: List[str]
    span: SourceSpan


@dataclass
class DataStmt:
    output: str
    table: str
    input_keep: List[str]
    input_drop: List[str]
    input_rename: Dict[str, str]
    input_where: ExprNode | None
    statements: List[Dict[str, Any]]
    keep: KeepStmt | None
    drop: DropStmt | None
    span: SourceSpan


@dataclass
class SortStmt:
    source: str
    target: str
    by: List[str]
    nodupkey: bool
    span: SourceSpan


@dataclass
class SummaryStmt:
    source: str
    target: str
    class_keys: List[str]
    vars: List[str]
    span: SourceSpan


@dataclass
class SelectStmt:
    source: str
    target: str
    keep: List[str]
    drop: List[str]
    span: SourceSpan


SansScriptStmt = FormatStmt | DataStmt | SortStmt | SummaryStmt | SelectStmt


@dataclass
class SansScript:
    statements: List[SansScriptStmt]
    span: SourceSpan
