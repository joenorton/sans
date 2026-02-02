from __future__ import annotations

from typing import Dict, List, Optional, Set, Union, Any
from .ast import (
    SansScript, SansScriptStmt, LetBinding, ConstDecl, TableBinding, TableExpr,
    FromExpr, TableNameExpr, PipelineExpr, PostfixExpr, BuilderExpr,
    TableTransform, DatasourceDeclaration, SaveStmt, AssertStmt,
)
from .errors import SansScriptError


def _is_const_literal(val: Any) -> bool:
    """True if val is an allowed const literal: int, str, bool, or None (null)."""
    return val is None or isinstance(val, (int, str, bool))


class SemanticValidator:
    def __init__(self, script: SansScript, initial_tables: Set[str]):
        self.script = script
        self.tables: Set[str] = set(initial_tables)
        self.datasources: Dict[str, DatasourceDeclaration] = {} # New: track datasources
        self.table_schemas: Dict[str, List[str]] = {}
        self.scalars: Dict[str, int] = {}  # name -> line_defined
        self.used_scalars: Set[str] = set()
        self.kinds: Dict[str, str] = {}  # name -> 'scalar' | 'table'
        self.warnings: List[Dict[str, Any]] = []

    def validate(self):
        for stmt in self.script.statements:
            self._validate_stmt(stmt)
        
        if self.script.terminal_expr:
            self._validate_table_expr(self.script.terminal_expr)

        # Check for unused scalars
        for name, line in self.scalars.items():
            if name not in self.used_scalars:
                self.warnings.append({
                    "code": "W_UNUSED_LET",
                    "message": f"Unused let binding '{name}'.",
                    "line": line
                })

    def _validate_stmt(self, stmt: SansScriptStmt):
        if isinstance(stmt, LetBinding):
            self._check_kind_lock(stmt.name, 'scalar', stmt.span.start)
            self._validate_scalar_expr(stmt.expr, stmt.span.start)
            self.scalars[stmt.name] = stmt.span.start
            self.kinds[stmt.name] = 'scalar'
        elif isinstance(stmt, ConstDecl):
            for name, val in stmt.bindings.items():
                self._check_kind_lock(name, 'scalar', stmt.span.start)
                if not _is_const_literal(val):
                    raise SansScriptError(
                        code="E_BAD_EXPR",
                        message=f"const allows only int, string, bool, null; got {type(val).__name__}.",
                        line=stmt.span.start,
                    )
                self.scalars[name] = stmt.span.start
                self.kinds[name] = 'scalar'
        elif isinstance(stmt, DatasourceDeclaration):
            self._check_kind_lock(stmt.name, 'datasource', stmt.span.start)
            if stmt.name in self.datasources:
                raise SansScriptError(
                    code="E_DUPLICATE_DATASOURCE",
                    message=f"Datasource '{stmt.name}' already declared.",
                    line=stmt.span.start
                )
            self.datasources[stmt.name] = stmt
            self.kinds[stmt.name] = 'datasource'
        elif isinstance(stmt, TableBinding):
            self._check_kind_lock(stmt.name, 'table', stmt.span.start)
            schema = self._validate_table_expr(stmt.expr)
            self.tables.add(stmt.name)
            if schema is not None:
                self.table_schemas[stmt.name] = schema
            self.kinds[stmt.name] = 'table'
        elif isinstance(stmt, SaveStmt):
            if stmt.table not in self.tables:
                raise SansScriptError(
                    code="E_UNDEFINED_TABLE",
                    message=f"Save references table '{stmt.table}' which is not defined.",
                    line=stmt.span.start,
                )
        elif isinstance(stmt, AssertStmt):
            self._validate_scalar_expr(stmt.predicate, stmt.span.start, None)

    def _check_kind_lock(self, name: str, kind: str, line: int):
        if name in self.kinds and self.kinds[name] != kind:
            raise SansScriptError(
                code="E_KIND_LOCK",
                message=f"Name '{name}' is already defined as a {self.kinds[name]} and cannot be redefined as a {kind}.",
                line=line
            )

    def _validate_table_expr(self, expr: TableExpr) -> Optional[List[str]]:
        """Returns the list of column names produced by this expression if known."""
        if isinstance(expr, FromExpr):
            if expr.source not in self.datasources:
                raise SansScriptError(
                    code="E_UNDECLARED_DATASOURCE",
                    message=f"Datasource '{expr.source}' is not declared.",
                    line=expr.span.start
                )
            # If datasource has explicit columns, use them as schema
            if self.datasources[expr.source].columns:
                return self.datasources[expr.source].columns
            
            # Default schema for demo purposes or unknown datasources
            if expr.source == "in": # Special handling for demo.sans
                return ["a", "b", "c"]
            return None # Schema unknown for other external sources without declaration

        elif isinstance(expr, TableNameExpr):
            if expr.name not in self.tables:
                raise SansScriptError(
                    code="E_UNDEFINED_TABLE",
                    message=f"Table '{expr.name}' is not defined.",
                    line=expr.span.start
                )
            return self.table_schemas.get(expr.name)
        elif isinstance(expr, PipelineExpr):
            curr_schema = self._validate_table_expr(expr.source)
            for step in expr.steps:
                curr_schema = self._validate_transform(step, curr_schema)
            return curr_schema
        elif isinstance(expr, PostfixExpr):
            curr_schema = self._validate_table_expr(expr.source)
            return self._validate_transform(expr.transform, curr_schema)
        elif isinstance(expr, BuilderExpr):
            self._validate_table_expr(expr.source)
            if expr.kind == "sort":
                if "by" not in expr.config:
                    raise SansScriptError(
                        code="E_SANS_VALIDATE_SORT_MISSING_BY",
                        message="SORT builder requires a .by() clause.",
                        line=expr.span.start
                    )
                return None # Schema unchanged
            elif expr.kind in ("summary", "aggregate"):
                class_cols = expr.config.get("class", [])
                var_cols = expr.config.get("var", [])
                stats = expr.config.get("stats", ["mean"])
                
                produced = list(class_cols)
                for v in var_cols:
                    for s in stats:
                        produced.append(f"{v}_{s}")
                return produced
        return None

    def _validate_transform(self, transform: TableTransform, schema: Optional[List[str]]) -> Optional[List[str]]:
        kind = transform.kind
        params = transform.params
        line = transform.span.start
        
        if kind == "select":
            keep = params.get("keep", [])
            if schema is not None:
                for col in keep:
                    if col not in schema:
                        raise SansScriptError(
                            code="E_UNKNOWN_COLUMN",
                            message=f"Column '{col}' is not produced by the preceding operation.",
                            line=line,
                            hint=f"Available columns: {', '.join(schema)}"
                        )
            return keep
        elif kind == "drop":
            drop = params.get("drop", [])
            if schema is not None:
                for col in drop:
                    if col not in schema:
                         self.warnings.append({
                            "code": "W_DROP_NONEXISTENT",
                            "message": f"Attempting to drop non-existent column '{col}'.",
                            "line": line
                        })
                return [c for c in schema if c not in drop]
            return None
        elif kind == "filter":
            self._validate_scalar_expr(params["predicate"], line, schema)
            return schema
        elif kind in ("derive", "update!"):
            # Rule: Sequential evaluation, no cycles, no implicit overwrites
            # We track "new" columns created in this derive block
            new_schema = list(schema) if schema is not None else [] # Start with empty schema if unknown
            if new_schema is None:
                # If schema is unknown, we can't do strict mutation checking
                # but we can still validate scalar expressions assuming columns exist
                pass

            for assign in params["assignments"]:
                target = assign["target"]
                expr = assign["expr"]
                allow_overwrite = assign.get("allow_overwrite", False)
                
                # Enforce overwrite rules
                if allow_overwrite:
                    # 'update!' assignment: target MUST exist
                    if target not in new_schema:
                        raise SansScriptError(
                            code="E_INVALID_UPDATE",
                            message=f"Attempted to update nonexistent column '{target}'. Use '{target} = ...' for new columns.",
                            line=line
                        )
                else:
                    # Plain assignment: target MUST NOT exist
                    if target in new_schema:
                        raise SansScriptError(
                            code="E_STRICT_MUTATION",
                            message=f"Column '{target}' already exists. Use 'update! {target} = ...' to overwrite.",
                            line=line
                        )
                
                # RHS sees columns created SO FAR in this block + previous schema
                # Pass a copy of new_schema for scalar expr validation
                self._validate_scalar_expr(expr, line, new_schema)
                
                if target not in new_schema:
                    new_schema.append(target)
            return new_schema
        elif kind == "rename":
            mappings = params["mappings"]
            if schema is not None:
                # rename(old -> new) is destructive
                final_schema = []
                old_cols_to_rename = set(mappings.keys())
                
                for c in schema:
                    if c in old_cols_to_rename:
                        final_schema.append(mappings[c])
                        old_cols_to_rename.remove(c) # Mark as successfully renamed
                    else:
                        final_schema.append(c)
                
                # Check if we tried to rename a non-existent column
                if old_cols_to_rename: # if set is not empty
                    non_existent = next(iter(old_cols_to_rename)) # Just pick one for the error message
                    raise SansScriptError(
                        code="E_UNKNOWN_COLUMN",
                        message=f"Cannot rename non-existent column '{non_existent}'.",
                        line=line,
                        hint=f"Available columns: {', '.join(schema)}"
                    )
                return final_schema
            return None
        return schema

    def _validate_map_expr(self, expr: MapExpr):
        for entry in expr.entries:
            self._validate_scalar_expr(entry.value, entry.span.start)

    def _validate_scalar_expr(self, expr: Any, line: int, schema: Optional[List[str]] = None):
        if not isinstance(expr, dict):
            return
        etype = expr.get("type")
        if etype == "col":
            name = expr.get("name")
            if name in self.kinds and self.kinds[name] == 'scalar':
                self.used_scalars.add(name)
            elif name in self.kinds and self.kinds[name] == 'table':
                 raise SansScriptError(
                    code="E_KIND_LOCK",
                    message=f"Table '{name}' cannot be used as a scalar.",
                    line=line
                )
            elif name in self.kinds and self.kinds[name] == 'datasource':
                 raise SansScriptError(
                    code="E_KIND_LOCK",
                    message=f"Datasource '{name}' cannot be used as a scalar or column.",
                    line=line
                )
            elif schema is not None:
                if name not in schema:
                    raise SansScriptError(
                        code="E_UNKNOWN_COLUMN",
                        message=f"Column '{name}' is not in scope.",
                        line=line,
                        hint=f"Available columns: {', '.join(schema)}"
                    )
        elif etype == "binop":
            self._validate_scalar_expr(expr.get("left"), line, schema)
            self._validate_scalar_expr(expr.get("right"), line, schema)
        elif etype == "boolop":
            for arg in expr.get("args", []):
                self._validate_scalar_expr(arg, line, schema)
        elif etype == "unop":
            self._validate_scalar_expr(expr.get("arg"), line, schema)
        elif etype == "call":
            name = expr.get("name")
            args = expr.get("args", [])
            if name == "if":
                # Ternary if: requires all 3 args
                if len(args) != 3:
                    raise SansScriptError(
                        code="E_BAD_EXPR",
                        message=f"if() requires 3 arguments, got {len(args)}.",
                        line=line
                    )
            # For map lookup m[key] desugared to put(key, map_name)
            elif name == "put":
                if len(args) != 2:
                     raise SansScriptError(
                        code="E_BAD_EXPR",
                        message=f"map lookup (put) requires 2 arguments, got {len(args)}.",
                        line=line
                    )
                map_arg = args[1] # This is the map name literal
                if not (isinstance(map_arg, dict) and map_arg.get("type") == "lit" and isinstance(map_arg.get("value"), str)):
                     raise SansScriptError(
                        code="E_BAD_EXPR",
                        message=f"Map name in lookup (put) must be a literal string.",
                        line=line
                    )
                map_name = map_arg["value"]
                if map_name not in self.kinds or self.kinds[map_name] != "scalar" or map_name not in self.scalars:
                     raise SansScriptError(
                        code="E_UNDEFINED_MAP",
                        message=f"Map '{map_name}' is not defined as a scalar map.",
                        line=line
                    )
                # Mark map as used
                self.used_scalars.add(map_name)

            for arg in args:
                self._validate_scalar_expr(arg, line, schema)

def validate_script(script: SansScript, initial_tables: Set[str]) -> List[Dict[str, Any]]:
    validator = SemanticValidator(script, initial_tables)
    validator.validate()
    return validator.warnings