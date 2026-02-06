from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from sans.types import Type


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
    """Single scalar binding: let name = expr → let_scalar. No map-style; use ConstDecl for multiple literals."""
    name: str
    expr: Any  # ExprNode (single expression only)
    span: SourceSpan


@dataclass
class ConstDecl:
    """Multiple named scalar literals: const { a = 1, b = "x", pi = 3.14 }. Lowers to one IR op 'const'. Literals: int, decimal ({type, value}), str, bool, null."""
    bindings: Dict[str, Any]  # name -> literal (int, decimal dict, str, bool, None)
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
    source_kind: Optional[str] = None  # "table" | "datasource" (resolved in validation)


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
    kind: str  # 'sort', 'summary' (legacy), 'aggregate'
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
    column_types: Optional[Dict[str, Type]] = None
    # inline_csv-backed
    inline_text: Optional[str] = None        # normalized CSV text
    inline_sha256: Optional[str] = None      # hash of normalized text


@dataclass
class SaveStmt:
    """save table to "path" [as "name"]"""
    table: str
    path: str
    span: SourceSpan
    name: Optional[str] = None  # artifact name; default from path or table


@dataclass
class AssertStmt:
    """assert <predicate> (e.g. row_count(t) > 0)"""
    predicate: Any  # ExprNode dict
    span: SourceSpan


SansScriptStmt = Union[LetBinding, ConstDecl, TableBinding, DatasourceDeclaration, SaveStmt, AssertStmt]


@dataclass
class SansScript:
    statements: List[SansScriptStmt]
    terminal_expr: Optional[TableExpr]
    span: SourceSpan
    datasources: Dict[str, DatasourceDeclaration]
