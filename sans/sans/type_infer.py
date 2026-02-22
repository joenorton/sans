from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from .expr import ExprNode
from .types import Type, from_literal, is_numeric, is_unknown, promote_numeric, type_name, unify


@dataclass
class TypeInferenceError(Exception):
    message: str
    code: str = "E_TYPE"
    loc: Any | None = None

    def __str__(self) -> str:
        return self.message


SchemaEnv = Dict[str, Type]


def _type_error(op: str, left: Type | None, right: Type | None, detail: str, code: str = "E_TYPE") -> TypeInferenceError:
    return TypeInferenceError(
        f"Type error for '{op}': {detail} (left={type_name(left)}, right={type_name(right)})",
        code=code,
    )


def _unknown_error(op: str, left: Type | None, right: Type | None, detail: str) -> TypeInferenceError:
    return _type_error(op, left, right, detail, code="E_TYPE_UNKNOWN")


def _require_bool(op: str, t: Type) -> Optional[TypeInferenceError]:
    if t == Type.BOOL:
        return None
    if is_unknown(t):
        return _unknown_error(op, t, t, "operand must be bool")
    return _type_error(op, t, t, "operand must be bool")


def infer_expr_type(expr: ExprNode, env: Optional[SchemaEnv] = None) -> Type:
    if not isinstance(expr, dict):
        return Type.UNKNOWN
    env = env or {}
    node_type = expr.get("type")
    if node_type == "lit":
        return from_literal(expr.get("value"))
    if node_type == "col":
        name = expr.get("name", "")
        if name in env:
            return env[name]
        lower = str(name).lower()
        return env.get(lower, Type.UNKNOWN)
    if node_type == "binop":
        op = expr.get("op", "")
        left_t = infer_expr_type(expr.get("left"), env)
        right_t = infer_expr_type(expr.get("right"), env)
        if op in {"+", "-", "*", "/"}:
            if left_t == Type.NULL or right_t == Type.NULL:
                raise _type_error(op, left_t, right_t, "null is not permitted in arithmetic")
            if is_unknown(left_t) or is_unknown(right_t):
                raise _unknown_error(op, left_t, right_t, "unknown is not permitted in arithmetic")
            if not is_numeric(left_t) or not is_numeric(right_t):
                raise _type_error(op, left_t, right_t, "arithmetic requires numeric operands")
            if op == "/":
                return Type.DECIMAL
            return promote_numeric(left_t, right_t)
        if op in {"==", "!="}:
            if is_unknown(left_t) or is_unknown(right_t):
                if (is_unknown(left_t) and is_unknown(right_t)) or (is_unknown(left_t) and right_t == Type.NULL) or (is_unknown(right_t) and left_t == Type.NULL):
                    return Type.BOOL
                raise _unknown_error(op, left_t, right_t, "unknown comparability")
            if left_t == Type.NULL or right_t == Type.NULL:
                return Type.BOOL
            if left_t == right_t:
                return Type.BOOL
            if is_numeric(left_t) and is_numeric(right_t):
                return Type.BOOL
            raise _type_error(op, left_t, right_t, "operands must be comparable")
        if op in {"<", "<=", ">", ">="}:
            if left_t == Type.NULL or right_t == Type.NULL:
                raise _type_error(op, left_t, right_t, "null is not permitted in ordered comparisons")
            if is_unknown(left_t) or is_unknown(right_t):
                raise _unknown_error(op, left_t, right_t, "unknown comparability")
            if is_numeric(left_t) and is_numeric(right_t):
                return Type.BOOL
            if left_t == right_t == Type.STRING:
                return Type.BOOL
            raise _type_error(op, left_t, right_t, "operands must be comparable")
        raise _type_error(op, left_t, right_t, "unsupported operator")
    if node_type == "boolop":
        op = expr.get("op", "")
        args = expr.get("args") or []
        for arg in args:
            t = infer_expr_type(arg, env)
            err = _require_bool(op, t)
            if err:
                raise err
        return Type.BOOL
    if node_type == "unop":
        op = expr.get("op", "")
        arg_t = infer_expr_type(expr.get("arg"), env)
        if op == "not":
            err = _require_bool(op, arg_t)
            if err:
                raise err
            return Type.BOOL
        if op in {"+", "-"}:
            if arg_t == Type.NULL:
                raise _type_error(op, arg_t, arg_t, "null is not permitted in arithmetic")
            if is_unknown(arg_t):
                raise _unknown_error(op, arg_t, arg_t, "unknown is not permitted in arithmetic")
            if not is_numeric(arg_t):
                raise _type_error(op, arg_t, arg_t, "arithmetic requires numeric operands")
            return arg_t
        raise TypeInferenceError(f"Type error for '{op}': unsupported unary operator")
    if node_type == "call":
        name = (expr.get("name") or "").lower()
        args = expr.get("args") or []
        if name == "if":
            if len(args) != 3:
                raise TypeInferenceError(f"Type error for 'if': expected 3 args, got {len(args)}")
            cond_t = infer_expr_type(args[0], env)
            err = _require_bool("if", cond_t)
            if err:
                raise err
            then_t = infer_expr_type(args[1], env)
            else_t = infer_expr_type(args[2], env)
            try:
                return unify(then_t, else_t, context="if")
            except TypeError:
                raise _type_error("if", then_t, else_t, "then/else types must unify")
        if name == "coalesce":
            if not args:
                raise TypeInferenceError("Type error for 'coalesce': expected at least 1 arg")
            result_t = None
            for arg in args:
                t = infer_expr_type(arg, env)
                if is_unknown(t):
                    return Type.UNKNOWN
                if result_t is None:
                    result_t = t
                else:
                    try:
                        result_t = unify(result_t, t, context="if")
                    except TypeError:
                        raise _type_error("coalesce", result_t, t, "argument types must unify")
            return result_t or Type.UNKNOWN
        if name in {"put", "input"}:
            return Type.UNKNOWN
        return Type.UNKNOWN
    return Type.UNKNOWN


def schema_to_strings(schema: Dict[str, Type]) -> Dict[str, str]:
    return {k: type_name(schema[k]) for k in sorted(schema)}


def infer_table_schema_types(irdoc: Any) -> Dict[str, Dict[str, Type]]:
    from io import StringIO
    import csv
    from .ir import OpStep, is_ds_input, ds_name_from_input

    schema_map: Dict[str, Optional[Dict[str, Type]]] = {}

    for name, ds in (irdoc.datasources or {}).items():
        cols = list(ds.columns) if ds.columns else None
        if not cols and ds.kind == "inline_csv" and ds.inline_text:
            reader = csv.reader(StringIO(ds.inline_text.strip()))
            try:
                cols = next(reader)
            except StopIteration:
                cols = []
        if cols:
            schema_map[f"__datasource__{name}"] = {
                c: (ds.column_types.get(c, Type.UNKNOWN) if ds.column_types else Type.UNKNOWN)
                for c in cols
            }

    for table_name in irdoc.tables:
        schema_map.setdefault(table_name, None)

    def _env_for(table_name: str) -> Dict[str, Type]:
        schema = schema_map.get(table_name)
        return dict(schema) if isinstance(schema, dict) else {}

    for step in irdoc.steps:
        if not isinstance(step, OpStep):
            continue
        op = step.op
        inputs = list(step.inputs or [])
        outputs = list(step.outputs or [])

        if op == "datasource":
            ds_name = step.params.get("name")
            ds_decl = irdoc.datasources.get(ds_name) if isinstance(ds_name, str) else None
            cols = list(ds_decl.columns) if ds_decl and ds_decl.columns else (step.params.get("columns") or [])
            if not cols and step.params.get("kind") == "inline_csv":
                inline_text = step.params.get("inline_text") or ""
                reader = csv.reader(StringIO(inline_text.strip()))
                try:
                    cols = next(reader)
                except StopIteration:
                    cols = []
            if cols and outputs:
                if ds_decl and ds_decl.column_types:
                    schema_map[outputs[0]] = {
                        c: ds_decl.column_types.get(c, Type.UNKNOWN) for c in cols
                    }
                else:
                    schema_map[outputs[0]] = {c: Type.UNKNOWN for c in cols}
            continue

        input_schema = schema_map.get(inputs[0]) if inputs else None

        if op in {"identity", "sort"}:
            out_schema = dict(input_schema) if isinstance(input_schema, dict) else None
            for out in outputs:
                schema_map[out] = dict(out_schema) if isinstance(out_schema, dict) else None
            continue

        if op == "filter":
            if isinstance(input_schema, dict):
                env = _env_for(inputs[0]) if inputs else {}
                pred = step.params.get("predicate")
                if pred is not None:
                    try:
                        t = infer_expr_type(pred, env)
                    except TypeInferenceError as err:
                        raise TypeInferenceError(err.message, code=err.code, loc=step.loc)
                    if t != Type.BOOL:
                        code = "E_TYPE_UNKNOWN" if t == Type.UNKNOWN else "E_TYPE"
                        raise TypeInferenceError(
                            f"Type error for 'filter': predicate must be bool, got {type_name(t)}",
                            code=code,
                            loc=step.loc,
                        )
            for out in outputs:
                schema_map[out] = dict(input_schema) if isinstance(input_schema, dict) else None
            continue

        if op == "assert":
            pred = step.params.get("predicate")
            if pred is not None:
                try:
                    t = infer_expr_type(pred, {})
                except TypeInferenceError as err:
                    raise TypeInferenceError(err.message, code=err.code, loc=step.loc)
                if t != Type.BOOL:
                    code = "E_TYPE_UNKNOWN" if t == Type.UNKNOWN else "E_TYPE"
                    raise TypeInferenceError(
                        f"Type error for 'assert': predicate must be bool, got {type_name(t)}",
                        code=code,
                        loc=step.loc,
                    )
            continue

        if op == "select":
            cols = step.params.get("cols")
            drop = step.params.get("drop")
            if isinstance(input_schema, dict):
                if cols:
                    out_schema = {c: input_schema.get(c, Type.UNKNOWN) for c in cols}
                elif drop:
                    out_schema = {c: t for c, t in input_schema.items() if c not in set(drop)}
                else:
                    out_schema = dict(input_schema)
            else:
                out_schema = None
            for out in outputs:
                schema_map[out] = dict(out_schema) if isinstance(out_schema, dict) else None
            continue

        if op == "drop":
            cols = step.params.get("cols") or []
            if isinstance(input_schema, dict):
                drop_set = set(cols)
                for c in cols:
                    if c not in input_schema:
                        raise TypeInferenceError(
                            f"Column '{c}' not found; cannot drop.",
                            code="E_COLUMN_NOT_FOUND",
                            loc=step.loc,
                        )
                out_schema = {c: t for c, t in input_schema.items() if c not in drop_set}
            else:
                out_schema = None
            for out in outputs:
                schema_map[out] = dict(out_schema) if isinstance(out_schema, dict) else None
            continue

        if op == "rename":
            mapping = step.params.get("mapping") or step.params.get("mappings") or []
            if isinstance(mapping, list):
                mapping = {m.get("from"): m.get("to") for m in mapping if isinstance(m, dict)}
            if isinstance(input_schema, dict):
                out_schema: Dict[str, Type] = {}
                for col, t in input_schema.items():
                    out_schema[mapping.get(col, col)] = t
            else:
                out_schema = None
            for out in outputs:
                schema_map[out] = dict(out_schema) if isinstance(out_schema, dict) else None
            continue

        if op == "cast":
            out_schema = dict(input_schema) if isinstance(input_schema, dict) else None
            casts = step.params.get("casts") or []
            if out_schema is not None:
                for c in casts:
                    col = c.get("col")
                    to = (c.get("to") or "").lower()
                    if not col:
                        continue
                    if to == "int":
                        out_schema[col] = Type.INT
                    elif to == "decimal":
                        out_schema[col] = Type.DECIMAL
                    elif to == "str":
                        out_schema[col] = Type.STRING
                    elif to == "bool":
                        out_schema[col] = Type.BOOL
                    else:
                        out_schema[col] = Type.UNKNOWN
            for out in outputs:
                schema_map[out] = dict(out_schema) if isinstance(out_schema, dict) else None
            continue

        if op == "compute":
            if isinstance(input_schema, dict):
                out_schema = dict(input_schema)
                env = dict(out_schema)
                assignments = step.params.get("assignments")
                if isinstance(assignments, list):
                    for assign in assignments:
                        target = assign.get("target")
                        expr = assign.get("expr")
                        if not target or expr is None:
                            continue
                        try:
                            t = infer_expr_type(expr, env)
                        except TypeInferenceError as err:
                            raise TypeInferenceError(err.message, code=err.code, loc=step.loc)
                        out_schema[target] = t
                        env[target] = t
            else:
                out_schema = None
            for out in outputs:
                schema_map[out] = dict(out_schema) if isinstance(out_schema, dict) else None
            continue

        if op in {"aggregate", "summary"}:
            group_by = step.params.get("group_by") or []
            metrics = step.params.get("metrics") or []
            out_schema: Dict[str, Type] = {}
            if isinstance(input_schema, dict):
                for col in group_by:
                    out_schema[col] = input_schema.get(col, Type.UNKNOWN)
            else:
                for col in group_by:
                    out_schema[col] = Type.UNKNOWN
            for m in metrics:
                name = m.get("name")
                if name:
                    out_schema[name] = Type.UNKNOWN
            for out in outputs:
                schema_map[out] = dict(out_schema) if out_schema else None
            continue

        # Default: propagate if possible, else unknown
        if outputs:
            if isinstance(input_schema, dict):
                out_schema = dict(input_schema)
            else:
                out_schema = None
            for out in outputs:
                schema_map[out] = dict(out_schema) if isinstance(out_schema, dict) else None

    result: Dict[str, Dict[str, Type]] = {}
    for name, schema in schema_map.items():
        if name.startswith("__datasource__"):
            continue
        if isinstance(schema, dict):
            result[name] = schema
    return result
