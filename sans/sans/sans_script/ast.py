from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union


@dataclass
class SourceSpan:
    start: int
    end: int


@dataclass
class MapEntry:
    key: Optional[str]  # None for '_'
    value: Any # Should be ExprNode for map values, not Any
    span: SourceSpan


@dataclass
class MapExpr:
    entries: List[MapEntry]
    span: SourceSpan


@dataclass
class LetBinding:
    name: str
    expr: Union[ExprNode, MapExpr]
    span: SourceSpan


@dataclass
class TableTransform:
    kind: str  # 'select', 'filter', 'derive', 'rename', 'drop'
    params: Dict[str, Any]
    span: SourceSpan


@dataclass
class TableExpr:
    pass


@dataclass
class FromExpr(TableExpr):
    source: str
    span: SourceSpan


@dataclass
class TableNameExpr(TableExpr):
    name: str
    span: SourceSpan


@dataclass
class PipelineExpr(TableExpr):
    source: TableExpr
    steps: List[TableTransform]
    span: SourceSpan


@dataclass
class PostfixExpr(TableExpr):
    source: TableExpr
    transform: TableTransform
    span: SourceSpan


@dataclass
class BuilderExpr(TableExpr):
    kind: str  # 'sort', 'summary'
    source: TableExpr
    config: Dict[str, Any]
    span: SourceSpan


@dataclass
class TableBinding:
    name: str
    expr: TableExpr
    span: SourceSpan

@dataclass
class DatasourceDeclaration:
    name: str
    # discriminator
    kind: str                 # "csv" | "inline_csv"
    span: SourceSpan
    # csv-backed
    path: Optional[str] = None
    # shared / optional
    columns: Optional[List[str]] = None
    # inline_csv-backed
    inline_text: Optional[str] = None        # normalized CSV text
    inline_sha256: Optional[str] = None      # hash of normalized text

SansScriptStmt = Union[LetBinding, TableBinding, DatasourceDeclaration]


@dataclass
class SansScript:
    statements: List[SansScriptStmt]
    terminal_expr: Optional[TableExpr]
    span: SourceSpan
    datasources: Dict[str, DatasourceDeclaration]
