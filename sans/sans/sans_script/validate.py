from __future__ import annotations

from typing import Dict, List, Optional, Set, Union, Any
from io import StringIO
import csv
from .ast import (
    SansScript, SansScriptStmt, LetBinding, ConstDecl, TableBinding, TableExpr,
    FromExpr, TableNameExpr, PipelineExpr, PostfixExpr, BuilderExpr,
    TableTransform, DatasourceDeclaration, SaveStmt, AssertStmt,
)
from .errors import SansScriptError
from sans.types import Type
from sans.type_infer import TypeInferenceError, infer_expr_type


def _is_const_literal(val: Any) -> bool:
    """True if val is an allowed const literal: int, str, bool, None (null), or decimal {type, value}."""
    if val is None or isinstance(val, (int, str, bool)):
        return True
    if isinstance(val, dict) and val.get("type") == "decimal" and isinstance(val.get("value"), str):
        return True
    return False


class SemanticValidator:
    def __init__(self, script: SansScript, initial_tables: Set[str]):
        self.script = script
        self.tables: Set[str] = set(initial_tables)
        self.datasources: Dict[str, DatasourceDeclaration] = {} # New: track datasources
        self.table_schemas: Dict[str, Dict[str, Type]] = {}
        self.scalars: Dict[str, int] = {}  # name -> line_defined
        self.scalar_types: Dict[str, Type] = {}
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
            expr_type = self._validate_scalar_expr(stmt.expr, stmt.span.start)
            self.scalars[stmt.name] = stmt.span.start
            self.scalar_types[stmt.name] = expr_type
            self.kinds[stmt.name] = 'scalar'
        elif isinstance(stmt, ConstDecl):
            for name, val in stmt.bindings.items():
                self._check_kind_lock(name, 'scalar', stmt.span.start)
                if not _is_const_literal(val):
                    raise SansScriptError(
                        code="E_BAD_EXPR",
                        message=f"const allows only int, decimal, string, bool, null; got {type(val).__name__}.",
                        line=stmt.span.start,
                    )
                self.scalars[name] = stmt.span.start
                self.scalar_types[name] = Type.UNKNOWN if val is None else Type.UNKNOWN
                try:
                    self.scalar_types[name] = infer_expr_type({"type": "lit", "value": val}, {})
                except TypeInferenceError as err:
                    raise SansScriptError(code=err.code, message=err.message, line=stmt.span.start)
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
            pred_type = self._validate_scalar_expr(stmt.predicate, stmt.span.start, None)
            if pred_type != Type.BOOL:
                code = "E_TYPE_UNKNOWN" if pred_type == Type.UNKNOWN else "E_TYPE"
                raise SansScriptError(
                    code=code,
                    message=f"Assert predicate must be bool, got {pred_type.value}.",
                    line=stmt.span.start,
                )

    def _check_kind_lock(self, name: str, kind: str, line: int):
        if name in self.kinds and self.kinds[name] != kind:
            raise SansScriptError(
                code="E_KIND_LOCK",
                message=f"Name '{name}' is already defined as a {self.kinds[name]} and cannot be redefined as a {kind}.",
                line=line
            )

    def _validate_table_expr(self, expr: TableExpr) -> Optional[Dict[str, Type]]:
        """Returns the column->type mapping produced by this expression if known."""
        if isinstance(expr, FromExpr):
            if expr.source in self.datasources:
                expr.source_kind = "datasource"
                # If datasource has explicit columns, use them as schema
                ds = self.datasources[expr.source]
                if ds.columns:
                    schema: Dict[str, Type] = {}
                    for col in ds.columns:
                        if ds.column_types and col in ds.column_types:
                            schema[col] = ds.column_types[col]
                        else:
                            schema[col] = Type.UNKNOWN
                    return schema

                # Infer inline_csv headers when available
                if ds.kind == "inline_csv" and ds.inline_text:
                    reader = csv.reader(StringIO(ds.inline_text.strip()))
                    try:
                        headers = next(reader)
                    except StopIteration:
                        headers = []
                    return {c: Type.UNKNOWN for c in headers}

                # Default schema for demo purposes or unknown datasources
                if expr.source == "in": # Special handling for demo.sans
                    return {c: Type.UNKNOWN for c in ["a", "b", "c"]}
                return None # Schema unknown for other external sources without declaration
            if expr.source in self.tables:
                expr.source_kind = "table"
                return self.table_schemas.get(expr.source)
            known_tables = sorted(self.tables)
            known_datasources = sorted(self.datasources.keys())
            tables_hint = ", ".join(known_tables) if known_tables else "<none>"
            ds_hint = ", ".join(known_datasources) if known_datasources else "<none>"
            raise SansScriptError(
                code="E_UNDECLARED_SOURCE",
                message=(
                    f"Source '{expr.source}' is not declared as a table or datasource. "
                    f"Known tables: {tables_hint}. Known datasources: {ds_hint}."
                ),
                line=expr.span.start,
            )

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
                return {c: Type.UNKNOWN for c in produced}
        return None

    def _validate_transform(self, transform: TableTransform, schema: Optional[Dict[str, Type]]) -> Optional[Dict[str, Type]]:
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
                            hint=f"Available columns: {', '.join(schema.keys())}"
                        )
            return {c: schema.get(c, Type.UNKNOWN) for c in keep} if schema is not None else None
        elif kind == "drop":
            drop = params.get("drop", [])
            if schema is not None:
                for col in drop:
                    if col not in schema:
                        raise SansScriptError(
                            code="E_COLUMN_NOT_FOUND",
                            message=f"Column '{col}' not found; cannot drop.",
                            line=line,
                            hint=f"Available columns: {', '.join(sorted(schema.keys()))}",
                        )
                return {c: t for c, t in schema.items() if c not in drop}
            return None
        elif kind == "filter":
            pred_type = self._validate_scalar_expr(params["predicate"], line, schema)
            if pred_type != Type.BOOL:
                code = "E_TYPE_UNKNOWN" if pred_type == Type.UNKNOWN else "E_TYPE"
                raise SansScriptError(
                    code=code,
                    message=f"Filter predicate must be bool, got {pred_type.value}.",
                    line=line,
                )
            return schema
        elif kind in ("derive", "update!"):
            # Rule: Sequential evaluation, no cycles, no implicit overwrites
            # We track "new" columns created in this derive block
            new_schema = dict(schema) if schema is not None else None

            for assign in params["assignments"]:
                target = assign["target"]
                expr = assign["expr"]
                allow_overwrite = assign.get("allow_overwrite", False)
                
                # Enforce overwrite rules
                if allow_overwrite:
                    # 'update!' assignment: target MUST exist
                    if new_schema is not None and target not in new_schema:
                        raise SansScriptError(
                            code="E_INVALID_UPDATE",
                            message=f"Attempted to update nonexistent column '{target}'. Use '{target} = ...' for new columns.",
                            line=line
                        )
                else:
                    # Plain assignment: target MUST NOT exist
                    if new_schema is not None and target in new_schema:
                        raise SansScriptError(
                            code="E_STRICT_MUTATION",
                            message=f"Column '{target}' already exists. Use 'update! {target} = ...' to overwrite.",
                            line=line
                        )
                
                # RHS sees columns created SO FAR in this block + previous schema
                # Pass a copy of new_schema for scalar expr validation
                expr_type = self._validate_scalar_expr(expr, line, new_schema)
                if new_schema is not None:
                    new_schema[target] = expr_type
            return new_schema
        elif kind == "rename":
            mappings = params["mappings"]
            if schema is not None:
                # rename(old -> new) is destructive
                final_schema: Dict[str, Type] = {}
                old_cols_to_rename = set(mappings.keys())
                
                for c, t in schema.items():
                    if c in old_cols_to_rename:
                        final_schema[mappings[c]] = t
                        old_cols_to_rename.remove(c) # Mark as successfully renamed
                    else:
                        final_schema[c] = t
                
                # Check if we tried to rename a non-existent column
                if old_cols_to_rename: # if set is not empty
                    non_existent = next(iter(old_cols_to_rename)) # Just pick one for the error message
                    raise SansScriptError(
                        code="E_UNKNOWN_COLUMN",
                        message=f"Cannot rename non-existent column '{non_existent}'.",
                        line=line,
                        hint=f"Available columns: {', '.join(schema.keys())}"
                    )
                return final_schema
            return None
        elif kind == "cast":
            casts = params.get("casts") or []
            if schema is not None:
                for c in casts:
                    col = c.get("col", "")
                    if col not in schema:
                        raise SansScriptError(
                            code="E_UNKNOWN_COLUMN",
                            message=f"Cannot cast non-existent column '{col}'.",
                            line=line,
                            hint=f"Available columns: {', '.join(schema.keys())}",
                        )
                    to = (c.get("to") or "").lower()
                    if to == "int":
                        schema[col] = Type.INT
                    elif to == "decimal":
                        schema[col] = Type.DECIMAL
                    elif to == "str":
                        schema[col] = Type.STRING
                    elif to == "bool":
                        schema[col] = Type.BOOL
                    else:
                        schema[col] = Type.UNKNOWN
            return schema
        return schema

    def _validate_map_expr(self, expr: MapExpr):
        for entry in expr.entries:
            self._validate_scalar_expr(entry.value, entry.span.start)

    def _validate_scalar_expr(self, expr: Any, line: int, schema: Optional[Dict[str, Type]] = None) -> Type:
        if not isinstance(expr, dict):
            return Type.UNKNOWN
        etype = expr.get("type")
        if etype == "col":
            name = expr.get("name")
            if not isinstance(name, str):
                return Type.UNKNOWN
            key = name.lower()
            if key in self.kinds and self.kinds[key] == 'scalar':
                self.used_scalars.add(key)
            elif key in self.kinds and self.kinds[key] == 'table':
                 raise SansScriptError(
                    code="E_KIND_LOCK",
                    message=f"Table '{name}' cannot be used as a scalar.",
                    line=line
                )
            elif key in self.kinds and self.kinds[key] == 'datasource':
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
                        hint=f"Available columns: {', '.join(schema.keys())}"
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
        env: Dict[str, Type] = {}
        if schema is not None:
            env.update(schema)
        env.update(self.scalar_types)
        try:
            return infer_expr_type(expr, env)
        except TypeInferenceError as err:
            raise SansScriptError(code=err.code, message=err.message, line=line)
        return Type.UNKNOWN

def validate_script(script: SansScript, initial_tables: Set[str]) -> List[Dict[str, Any]]:
    validator = SemanticValidator(script, initial_tables)
    validator.validate()
    return validator.warnings
