from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, ValidationError

from ..ir.schema import validate_sans_ir
from .diff import (
    build_assertion_diff,
    build_diagnostics,
    build_structural_diff,
    build_table_universe,
    derive_transform_id,
)
from .errors import (
    E_AMEND_ASSERTION_ID_COLLISION,
    E_AMEND_ASSERTION_ID_REQUIRED,
    E_AMEND_CAPABILITY_LIMIT,
    E_AMEND_EXPR_INVALID,
    E_AMEND_INDEX_OUT_OF_RANGE,
    E_AMEND_IR_INVALID,
    E_AMEND_OUTPUT_TABLE_COLLISION,
    E_AMEND_PATH_INVALID,
    E_AMEND_PATH_NOT_FOUND,
    E_AMEND_POLICY_DESTRUCTIVE_REFUSED,
    E_AMEND_POLICY_OUTPUT_REWIRE_REFUSED,
    E_AMEND_TARGET_AMBIGUOUS,
    E_AMEND_TARGET_MISMATCH,
    E_AMEND_TARGET_NOT_FOUND,
    E_AMEND_VALIDATION_SCHEMA,
    refusal,
)
from .schemas import (
    AmendmentRequestV1,
    HARD_CAP,
    REPLACE_OP_ALLOWLIST,
    validate_step_params_shape,
)

SansIR = Dict[str, Any]

ALLOWED_IR_TOP_LEVEL_KEYS = {"version", "datasources", "steps", "assertions", "tables"}
ALLOWED_BINOPS = {"==", "!=", "<", ">", "<=", ">=", "+", "-", "*", "/"}
ALLOWED_BOOLOPS = {"and", "or"}
ALLOWED_UNOPS = {"not", "+", "-"}
ALLOWED_CALLS = {"coalesce", "if", "put", "input"}


class MutationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "refused"]
    ir_out: Optional[Dict[str, Any]] = None
    diff_structural: Optional[Dict[str, Any]] = None
    diff_assertions: Optional[Dict[str, Any]] = None
    diagnostics: Dict[str, Any]


def _refused(code: str, message: str, *, meta: Optional[Dict[str, Any]] = None) -> MutationResult:
    refusal_payload = refusal(code, message, meta=meta)
    diagnostics = build_diagnostics(status="refused", refusals=[refusal_payload], warnings=[])
    return MutationResult(
        status="refused",
        ir_out=None,
        diff_structural=None,
        diff_assertions=None,
        diagnostics=diagnostics,
    )


def _decode_pointer_token(token: str) -> str:
    out: List[str] = []
    i = 0
    while i < len(token):
        if token[i] == "~":
            if i + 1 >= len(token):
                raise ValueError("invalid RFC6901 escape")
            nxt = token[i + 1]
            if nxt == "0":
                out.append("~")
            elif nxt == "1":
                out.append("/")
            else:
                raise ValueError("invalid RFC6901 escape")
            i += 2
            continue
        out.append(token[i])
        i += 1
    return "".join(out)


def _pointer_tokens(path: str) -> List[str]:
    if not isinstance(path, str) or not path or not path.startswith("/"):
        raise ValueError("invalid pointer")
    if path == "/":
        return []
    return [_decode_pointer_token(part) for part in path[1:].split("/")]


def _resolve_pointer_parent(root: Any, path: str) -> Tuple[Any, Any, Any]:
    tokens = _pointer_tokens(path)
    if not tokens:
        return None, None, root

    current = root
    for token in tokens[:-1]:
        if isinstance(current, dict):
            if token not in current:
                raise LookupError("path not found")
            current = current[token]
            continue
        if isinstance(current, list):
            if not token.isdigit():
                raise TypeError("invalid list index")
            index = int(token)
            if index < 0 or index >= len(current):
                raise LookupError("path not found")
            current = current[index]
            continue
        raise TypeError("path traverses non-container")

    last = tokens[-1]
    if isinstance(current, dict):
        if last not in current:
            raise LookupError("path not found")
        return current, last, current[last]
    if isinstance(current, list):
        if not last.isdigit():
            raise TypeError("invalid list index")
        index = int(last)
        if index < 0 or index >= len(current):
            raise LookupError("path not found")
        return current, index, current[index]
    raise TypeError("path traverses non-container")


def _set_pointer_value(root: Any, path: str, value: Any) -> Any:
    parent, key, _ = _resolve_pointer_parent(root, path)
    if parent is None:
        return value
    parent[key] = value
    return root


def _validate_expr(node: Any) -> None:
    if not isinstance(node, dict):
        raise ValueError("expr node must be object")
    node_type = node.get("type")
    if node_type == "lit":
        if set(node.keys()) != {"type", "value"}:
            raise ValueError("lit node shape invalid")
        return
    if node_type == "col":
        if set(node.keys()) != {"type", "name"} or not isinstance(node.get("name"), str):
            raise ValueError("col node shape invalid")
        return
    if node_type == "binop":
        if set(node.keys()) != {"type", "op", "left", "right"}:
            raise ValueError("binop node shape invalid")
        if node.get("op") not in ALLOWED_BINOPS:
            raise ValueError("binop operator invalid")
        _validate_expr(node["left"])
        _validate_expr(node["right"])
        return
    if node_type == "boolop":
        if set(node.keys()) != {"type", "op", "args"}:
            raise ValueError("boolop node shape invalid")
        if node.get("op") not in ALLOWED_BOOLOPS:
            raise ValueError("boolop operator invalid")
        args = node.get("args")
        if not isinstance(args, list) or len(args) < 2:
            raise ValueError("boolop args invalid")
        for arg in args:
            _validate_expr(arg)
        return
    if node_type == "unop":
        if set(node.keys()) != {"type", "op", "arg"}:
            raise ValueError("unop node shape invalid")
        if node.get("op") not in ALLOWED_UNOPS:
            raise ValueError("unop operator invalid")
        _validate_expr(node["arg"])
        return
    if node_type == "call":
        if set(node.keys()) != {"type", "name", "args"}:
            raise ValueError("call node shape invalid")
        if node.get("name") not in ALLOWED_CALLS:
            raise ValueError("call name invalid")
        args = node.get("args")
        if not isinstance(args, list):
            raise ValueError("call args invalid")
        for arg in args:
            _validate_expr(arg)
        return
    raise ValueError("unknown expr node type")


def _resolve_single_step_index(steps: List[Dict[str, Any]], selector: Any) -> Tuple[Optional[int], Optional[MutationResult]]:
    step_id = getattr(selector, "step_id", None)
    transform_id = getattr(selector, "transform_id", None)
    table = getattr(selector, "table", None)

    by_step_id: Optional[int] = None
    by_transform_id: Optional[int] = None
    by_table: Optional[int] = None

    if step_id is not None:
        matches = [i for i, step in enumerate(steps) if step.get("id") == step_id]
        if not matches:
            return None, _refused(E_AMEND_TARGET_NOT_FOUND, "step_id did not match any step")
        if len(matches) > 1:
            return None, _refused(E_AMEND_TARGET_AMBIGUOUS, "step_id matched multiple steps")
        by_step_id = matches[0]

    if transform_id is not None:
        matches = [
            i for i, step in enumerate(steps) if derive_transform_id(step) == transform_id
        ]
        if not matches:
            return None, _refused(
                E_AMEND_TARGET_NOT_FOUND, "transform_id did not match any step"
            )
        if len(matches) > 1:
            return None, _refused(
                E_AMEND_TARGET_AMBIGUOUS, "transform_id matched multiple steps"
            )
        by_transform_id = matches[0]

    if table is not None:
        matches = [
            i
            for i, step in enumerate(steps)
            if table in (step.get("outputs") or [])
        ]
        if not matches:
            return None, _refused(E_AMEND_TARGET_NOT_FOUND, "table did not match any producer")
        if len(matches) > 1:
            return None, _refused(
                E_AMEND_TARGET_AMBIGUOUS, "table matched multiple producer steps"
            )
        by_table = matches[0]

    resolved = [idx for idx in (by_step_id, by_transform_id, by_table) if idx is not None]
    if not resolved:
        return None, _refused(E_AMEND_TARGET_NOT_FOUND, "selector did not identify a step")
    if len(set(resolved)) != 1:
        return None, _refused(
            E_AMEND_TARGET_MISMATCH, "selector fields resolve to different steps"
        )
    return resolved[0], None


def _resolve_assertion_index(assertions: List[Dict[str, Any]], assertion_id: str) -> int:
    matches = [
        i
        for i, assertion in enumerate(assertions)
        if isinstance(assertion, dict) and assertion.get("assertion_id") == assertion_id
    ]
    if not matches:
        raise LookupError("assertion not found")
    if len(matches) > 1:
        raise RuntimeError("assertion ambiguous")
    return matches[0]


def _is_approx_step(step: Dict[str, Any]) -> bool:
    if step.get("soundness") == "approx":
        return True
    params = step.get("params")
    if isinstance(params, dict) and params.get("soundness") == "approx":
        return True
    return False


def _ir_validation_meta(exc: ValueError) -> Dict[str, Any]:
    message = str(exc).strip()
    short = message[:200]
    meta: Dict[str, Any] = {"reason": short}

    field_match = re.search(r"field '([^']+)'", message)
    if field_match:
        meta["field_path"] = [field_match.group(1)]
        return meta

    table_match = re.search(r"table '([^']+)'", message)
    if table_match:
        meta["field_path"] = ["steps", "*", "outputs"]
        meta["table"] = table_match.group(1)
        return meta

    step_match = re.search(r"step '([^']+)'", message)
    if step_match:
        meta["step_id"] = step_match.group(1)
    if "unknown input table" in message or "before it is produced" in message:
        meta["field_path"] = ["steps", "*", "inputs"]
    elif "invalid params" in message:
        meta["field_path"] = ["steps", "*", "params"]
    elif "invalid outputs" in message:
        meta["field_path"] = ["steps", "*", "outputs"]
    elif "invalid inputs" in message:
        meta["field_path"] = ["steps", "*", "inputs"]
    return meta


def _pydantic_error_meta(exc: ValidationError, prefix: List[str] | None = None) -> Dict[str, Any]:
    errors = exc.errors()
    if not errors:
        return {"reason": "validation error"}
    first = errors[0]
    loc = list(first.get("loc", ()))
    if prefix:
        loc = prefix + loc
    return {
        "reason": str(first.get("msg", "validation error"))[:200],
        "field_path": [str(item) for item in loc],
    }


def apply_amendment(ir_in: SansIR, req: AmendmentRequestV1 | Dict[str, Any]) -> MutationResult:
    """
    Apply amendment request to sans.ir as a pure, deterministic mutation.

    v0.1 guarantees:
    - Atomic application (first failure aborts).
    - Single-refusal diagnostics payload on refusal.
    - RFC6901 selector.path semantics relative to step.params ('/' means root).
    - add_step collision checks against a deterministic table-universe helper.
    """
    if not isinstance(ir_in, dict):
        return _refused(E_AMEND_IR_INVALID, "ir_in must be an object")

    unknown_keys = sorted(set(ir_in.keys()) - ALLOWED_IR_TOP_LEVEL_KEYS)
    if unknown_keys:
        return _refused(
            E_AMEND_IR_INVALID,
            "ir_in contains unknown top-level keys",
            meta={"unknown_keys": unknown_keys},
        )

    try:
        if isinstance(req, AmendmentRequestV1):
            request = req
        else:
            request = AmendmentRequestV1.model_validate(req)
    except ValidationError as exc:
        errors = exc.errors()
        is_cap_error = any(
            "exceeds cap" in str(item.get("msg", "")) or "exceeds cap" in str(item)
            for item in errors
        )
        return _refused(
            E_AMEND_CAPABILITY_LIMIT if is_cap_error else E_AMEND_VALIDATION_SCHEMA,
            "amendment request failed schema validation",
            meta={"errors": errors},
        )
    except ValueError as exc:
        message = str(exc)
        code = (
            E_AMEND_CAPABILITY_LIMIT
            if "exceeds cap" in message
            else E_AMEND_VALIDATION_SCHEMA
        )
        return _refused(code, message)

    cap = min(request.policy.max_ops, HARD_CAP)
    if len(request.ops) > cap:
        return _refused(E_AMEND_CAPABILITY_LIMIT, "amendment op count exceeds cap")

    op_ids = [op.op_id for op in request.ops]
    if len(set(op_ids)) != len(op_ids):
        return _refused(E_AMEND_VALIDATION_SCHEMA, "duplicate op_id found in request")

    work = copy.deepcopy(ir_in)
    assertions_before = copy.deepcopy(work.get("assertions") or [])
    work.setdefault("assertions", [])

    steps = work.get("steps")
    if not isinstance(steps, list):
        return _refused(E_AMEND_IR_INVALID, "ir_in.steps must be a list")
    assertions = work.get("assertions")
    if not isinstance(assertions, list):
        return _refused(E_AMEND_IR_INVALID, "ir_in.assertions must be a list")

    ops_applied: List[Dict[str, Any]] = []
    affected_steps: List[str] = []
    affected_tables: List[str] = []

    for op in request.ops:
        if op.kind == "add_step":
            new_step = copy.deepcopy(op.params.step.model_dump(exclude_none=True))
            try:
                validate_step_params_shape(new_step.get("op"), new_step.get("params", {}))
            except ValidationError as exc:
                return _refused(
                    E_AMEND_IR_INVALID,
                    "step params violate op schema",
                    meta=_pydantic_error_meta(exc, prefix=["steps", new_step.get("id", "<new>"), "params"]),
                )
            except ValueError as exc:
                return _refused(
                    E_AMEND_IR_INVALID,
                    "step params violate op schema",
                    meta={"reason": str(exc), "field_path": ["steps", new_step.get("id", "<new>"), "params"]},
                )

            if _is_approx_step(new_step) and not request.policy.allow_approx:
                return _refused(
                    "E_AMEND_POLICY_APPROX_REFUSED",
                    "add_step introduces approx without policy.allow_approx",
                )

            step_ids = {step.get("id") for step in steps if isinstance(step, dict)}
            if new_step.get("id") in step_ids:
                return _refused(
                    E_AMEND_VALIDATION_SCHEMA,
                    "add_step params.step.id collides with existing step id",
                )

            universe = build_table_universe(work)
            collisions = sorted(set(new_step.get("outputs", [])) & universe)
            if collisions:
                return _refused(
                    E_AMEND_OUTPUT_TABLE_COLLISION,
                    "add_step output table collision",
                    meta={"collisions": collisions},
                )

            if op.selector.index is not None:
                index = op.selector.index
                if index < 0 or index > len(steps):
                    return _refused(
                        E_AMEND_INDEX_OUT_OF_RANGE,
                        "add_step index out of range",
                        meta={"index": index, "len": len(steps)},
                    )
                steps.insert(index, new_step)
            elif op.selector.before_step_id is not None:
                matches = [
                    i
                    for i, step in enumerate(steps)
                    if step.get("id") == op.selector.before_step_id
                ]
                if not matches:
                    return _refused(E_AMEND_TARGET_NOT_FOUND, "before_step_id not found")
                steps.insert(matches[0], new_step)
            elif op.selector.after_step_id is not None:
                matches = [
                    i
                    for i, step in enumerate(steps)
                    if step.get("id") == op.selector.after_step_id
                ]
                if not matches:
                    return _refused(E_AMEND_TARGET_NOT_FOUND, "after_step_id not found")
                steps.insert(matches[0] + 1, new_step)

            affected_steps.append(new_step["id"])
            affected_tables.extend(new_step.get("outputs", []))

        elif op.kind == "remove_step":
            if not request.policy.allow_destructive:
                return _refused(
                    E_AMEND_POLICY_DESTRUCTIVE_REFUSED,
                    "remove_step requires policy.allow_destructive=true",
                )
            step_idx, refused = _resolve_single_step_index(steps, op.selector)
            if refused:
                return refused
            removed = steps.pop(step_idx)
            affected_steps.append(removed.get("id", ""))
            affected_tables.extend(removed.get("outputs", []))

        elif op.kind == "replace_step":
            step_idx, refused = _resolve_single_step_index(steps, op.selector)
            if refused:
                return refused
            existing = steps[step_idx]
            updated = copy.deepcopy(existing)
            updated["op"] = op.params.op
            updated["params"] = copy.deepcopy(op.params.params)
            try:
                validate_step_params_shape(updated.get("op"), updated.get("params", {}))
            except ValidationError as exc:
                return _refused(
                    E_AMEND_IR_INVALID,
                    "step params violate op schema",
                    meta=_pydantic_error_meta(
                        exc, prefix=["steps", updated.get("id", f"index:{step_idx}"), "params"]
                    ),
                )
            except ValueError as exc:
                return _refused(
                    E_AMEND_IR_INVALID,
                    "step params violate op schema",
                    meta={
                        "reason": str(exc),
                        "field_path": ["steps", updated.get("id", f"index:{step_idx}"), "params"],
                    },
                )
            if _is_approx_step(updated) and not request.policy.allow_approx:
                return _refused(
                    "E_AMEND_POLICY_APPROX_REFUSED",
                    "replace_step introduces approx without policy.allow_approx",
                )
            steps[step_idx] = updated
            affected_steps.append(updated.get("id", ""))
            affected_tables.extend(updated.get("outputs", []))

        elif op.kind == "rewire_inputs":
            step_idx, refused = _resolve_single_step_index(steps, op.selector)
            if refused:
                return refused
            steps[step_idx]["inputs"] = copy.deepcopy(op.params.inputs)
            affected_steps.append(steps[step_idx].get("id", ""))

        elif op.kind == "rewire_outputs":
            if not request.policy.allow_output_rewire:
                return _refused(
                    E_AMEND_POLICY_OUTPUT_REWIRE_REFUSED,
                    "rewire_outputs requires policy.allow_output_rewire=true",
                )
            step_idx, refused = _resolve_single_step_index(steps, op.selector)
            if refused:
                return refused
            universe = build_table_universe(work) - set(steps[step_idx].get("outputs", []))
            collisions = sorted(set(op.params.outputs) & universe)
            if collisions:
                return _refused(
                    E_AMEND_OUTPUT_TABLE_COLLISION,
                    "rewire_outputs causes table collision",
                    meta={"collisions": collisions},
                )
            steps[step_idx]["outputs"] = copy.deepcopy(op.params.outputs)
            affected_steps.append(steps[step_idx].get("id", ""))
            affected_tables.extend(op.params.outputs)

        elif op.kind == "rename_table":
            old_name = op.selector.table
            new_name = op.params.new_name
            universe = build_table_universe(work)
            if old_name not in universe:
                return _refused(E_AMEND_TARGET_NOT_FOUND, "rename_table source not found")
            if new_name in universe:
                return _refused(E_AMEND_OUTPUT_TABLE_COLLISION, "rename_table target already exists")
            for step in steps:
                step["inputs"] = [new_name if name == old_name else name for name in step.get("inputs", [])]
                step["outputs"] = [new_name if name == old_name else name for name in step.get("outputs", [])]
            for assertion in assertions:
                if assertion.get("table") == old_name:
                    assertion["table"] = new_name
            affected_tables.extend([old_name, new_name])

        elif op.kind == "set_params":
            step_idx, refused = _resolve_single_step_index(steps, op.selector)
            if refused:
                return refused
            step = steps[step_idx]
            try:
                updated_params = _set_pointer_value(
                    step.get("params", {}), op.selector.path, copy.deepcopy(op.params.value)
                )
            except ValueError:
                return _refused(E_AMEND_PATH_INVALID, "selector.path is invalid")
            except LookupError:
                return _refused(E_AMEND_PATH_NOT_FOUND, "selector.path not found")
            except TypeError:
                return _refused(E_AMEND_PATH_INVALID, "selector.path traverses invalid type")

            if not isinstance(updated_params, dict):
                return _refused(E_AMEND_PATH_INVALID, "selector.path must stay within step.params object")
            try:
                validate_step_params_shape(step.get("op"), updated_params)
            except ValidationError as exc:
                return _refused(
                    E_AMEND_IR_INVALID,
                    "set_params produced invalid step params",
                    meta=_pydantic_error_meta(
                        exc, prefix=["steps", step.get("id", f"index:{step_idx}"), "params"]
                    ),
                )
            except ValueError as exc:
                return _refused(
                    E_AMEND_IR_INVALID,
                    "set_params produced invalid step params",
                    meta={
                        "reason": str(exc),
                        "field_path": ["steps", step.get("id", f"index:{step_idx}"), "params"],
                    },
                )
            step["params"] = updated_params
            affected_steps.append(step.get("id", ""))

        elif op.kind == "replace_expr":
            step_idx, refused = _resolve_single_step_index(steps, op.selector)
            if refused:
                return refused
            step = steps[step_idx]
            try:
                _validate_expr(op.params.expr)
                updated_params = _set_pointer_value(
                    step.get("params", {}), op.selector.path, copy.deepcopy(op.params.expr)
                )
            except ValueError:
                return _refused(E_AMEND_EXPR_INVALID, "replacement expr is invalid")
            except LookupError:
                return _refused(E_AMEND_PATH_NOT_FOUND, "selector.path not found")
            except TypeError:
                return _refused(E_AMEND_PATH_INVALID, "selector.path traverses invalid type")
            step["params"] = updated_params
            affected_steps.append(step.get("id", ""))

        elif op.kind == "edit_expr":
            step_idx, refused = _resolve_single_step_index(steps, op.selector)
            if refused:
                return refused
            step = steps[step_idx]
            try:
                parent, key, current = _resolve_pointer_parent(step.get("params", {}), op.selector.path)
            except ValueError:
                return _refused(E_AMEND_PATH_INVALID, "selector.path is invalid")
            except LookupError:
                return _refused(E_AMEND_PATH_NOT_FOUND, "selector.path not found")
            except TypeError:
                return _refused(E_AMEND_PATH_INVALID, "selector.path traverses invalid type")

            if not isinstance(current, dict) or "type" not in current:
                return _refused(E_AMEND_PATH_INVALID, "selector.path does not point to expression node")

            edited = copy.deepcopy(current)
            if op.params.edit == "replace_literal":
                edited = {"type": "lit", "value": op.params.literal}
            elif op.params.edit == "replace_column_ref":
                edited = {"type": "col", "name": op.params.column_ref}
            elif op.params.edit == "replace_op":
                if edited.get("type") not in {"binop", "boolop", "unop"}:
                    return _refused(E_AMEND_PATH_INVALID, "replace_op target is not operator node")
                if op.params.op not in REPLACE_OP_ALLOWLIST:
                    return _refused(E_AMEND_EXPR_INVALID, "replace_op operator is unsupported")
                edited["op"] = op.params.op
            elif op.params.edit == "wrap_with_not":
                edited = {"type": "unop", "op": "not", "arg": edited}

            try:
                _validate_expr(edited)
            except ValueError:
                return _refused(E_AMEND_EXPR_INVALID, "edited expression is invalid")

            if parent is None:
                step["params"] = edited
            else:
                parent[key] = edited
            affected_steps.append(step.get("id", ""))

        elif op.kind == "add_assertion":
            assertion_payload = copy.deepcopy(op.params.assertion.model_dump(exclude_none=True))
            assertion_id = assertion_payload.get("assertion_id")
            if not isinstance(assertion_id, str) or not assertion_id:
                return _refused(
                    E_AMEND_ASSERTION_ID_REQUIRED, "add_assertion requires assertion_id"
                )
            if any(
                isinstance(item, dict) and item.get("assertion_id") == assertion_id
                for item in assertions
            ):
                return _refused(
                    E_AMEND_ASSERTION_ID_COLLISION,
                    "assertion_id already exists",
                    meta={"assertion_id": assertion_id},
                )
            assertion_payload["table"] = op.selector.table
            assertions.append(assertion_payload)
            affected_tables.append(op.selector.table)

        elif op.kind == "remove_assertion":
            if not request.policy.allow_destructive:
                return _refused(
                    E_AMEND_POLICY_DESTRUCTIVE_REFUSED,
                    "remove_assertion requires policy.allow_destructive=true",
                )
            try:
                assertion_idx = _resolve_assertion_index(assertions, op.selector.assertion_id)
            except LookupError:
                return _refused(E_AMEND_TARGET_NOT_FOUND, "assertion_id not found")
            except RuntimeError:
                return _refused(E_AMEND_TARGET_AMBIGUOUS, "assertion_id matched multiple assertions")
            assertions.pop(assertion_idx)

        elif op.kind == "replace_assertion":
            payload = copy.deepcopy(op.params.assertion.model_dump(exclude_none=True))
            if payload.get("assertion_id") != op.selector.assertion_id:
                return _refused(
                    E_AMEND_TARGET_MISMATCH,
                    "replace_assertion payload assertion_id must match selector",
                )
            try:
                assertion_idx = _resolve_assertion_index(assertions, op.selector.assertion_id)
            except LookupError:
                return _refused(E_AMEND_TARGET_NOT_FOUND, "assertion_id not found")
            except RuntimeError:
                return _refused(E_AMEND_TARGET_AMBIGUOUS, "assertion_id matched multiple assertions")
            assertions[assertion_idx] = payload

        else:
            return _refused("E_AMEND_CAPABILITY_UNSUPPORTED", f"unsupported op kind: {op.kind}")

        ops_applied.append({"op_id": op.op_id, "kind": op.kind, "status": "ok"})

    try:
        validate_sans_ir(work)
    except ValueError as exc:
        return _refused(
            E_AMEND_IR_INVALID,
            "mutated ir failed validation",
            meta=_ir_validation_meta(exc),
        )

    assertions_after = copy.deepcopy(assertions)
    diff_structural = build_structural_diff(
        ir_in=ir_in,
        ir_out=work,
        ops_applied=ops_applied,
        affected_steps=affected_steps,
        affected_tables=affected_tables,
    )
    diff_assertions = build_assertion_diff(assertions_before, assertions_after)
    diagnostics = build_diagnostics(status="ok", refusals=[], warnings=[])

    return MutationResult(
        status="ok",
        ir_out=work,
        diff_structural=diff_structural,
        diff_assertions=diff_assertions,
        diagnostics=diagnostics,
    )

