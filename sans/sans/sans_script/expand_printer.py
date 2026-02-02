"""
IR → expanded.sans printer.
Deterministic, byte-stable output; kernel vocabulary only; one statement per step.
"""
from __future__ import annotations

from typing import Any, Dict, List

from sans.ir import IRDoc, OpStep, UnknownBlockStep, is_ds_input, ds_name_from_input


def _expr_to_string(node: Any) -> str:
    """Serialize expression dict to deterministic sans expression string."""
    if not isinstance(node, dict):
        return str(node)
    node_type = node.get("type")
    if node_type == "lit":
        v = node.get("value")
        if isinstance(v, str):
            return f'"{v}"'
        if v is None:
            return "null"
        return str(v)
    if node_type == "col":
        return str(node.get("name", ""))
    if node_type == "binop":
        op = node.get("op", "")
        left = _expr_to_string(node.get("left"))
        right = _expr_to_string(node.get("right"))
        return f"({left} {op} {right})"
    if node_type == "boolop":
        op = node.get("op", "")
        args = node.get("args") or []
        inner = f" {op} ".join(_expr_to_string(a) for a in args)
        return f"({inner})"
    if node_type == "unop":
        op = node.get("op", "")
        arg = _expr_to_string(node.get("arg"))
        return f"({op} {arg})"
    if node_type == "call":
        name = node.get("name", "")
        args = node.get("args") or []
        args_str = ", ".join(_expr_to_string(a) for a in args)
        return f"{name}({args_str})"
    return "null"


def _input_ref(inp: str) -> str:
    """Canonical reference for expanded.sans: from(ds) for datasource, else table name."""
    if is_ds_input(inp):
        return f"from({ds_name_from_input(inp)})"
    return inp


def _sort_by_to_expanded(by: List[Dict[str, Any]]) -> str:
    """
    Emit canonical expanded.sans for sort 'by'. Expects canonical list[{"col": str, "desc": bool}] only.
    Output: by(col1, -col2) where desc uses leading '-'.
    """
    if not by:
        return ""
    parts: List[str] = []
    for item in by:
        col = item.get("col", "")
        desc = item.get("desc", False)
        parts.append(f"-{col}" if desc else col)
    return ", ".join(parts)


def _step_to_expanded(step: OpStep) -> List[str]:
    """Emit one or more lines for this step (kernel-only, deterministic)."""
    lines: List[str] = []
    op = step.op
    inputs = step.inputs
    outputs = step.outputs
    params = step.params or {}
    out = outputs[0] if outputs else None
    inp = inputs[0] if inputs else None
    inp_ref = _input_ref(inp) if inp else ""

    if op == "datasource":
        name = params.get("name", "")
        kind = params.get("kind", "csv")
        if kind == "inline_csv":
            path = params.get("path")
            cols = params.get("columns") or []
            cols_str = f", columns({', '.join(cols)})" if cols else ""
            lines.append(f'datasource {name} = inline_csv{cols_str} do')
            inline = params.get("inline_text", "")
            if inline:
                for line in inline.rstrip().split("\n"):
                    lines.append("  " + line.strip())
            lines.append("end")
        else:
            path = params.get("path") or ""
            cols = params.get("columns") or []
            cols_str = f", columns({', '.join(cols)})" if cols else ""
            lines.append(f'datasource {name} = csv("{path}"{cols_str})')
        return lines

    if op == "identity" and inp and out:
        lines.append(f"table {out} = {inp_ref}")
        return lines

    if op == "compute" and inp and out:
        mode = params.get("mode", "derive")
        assignments = params.get("assignments") or []
        parts = [f"{a.get('target', '')} = {_expr_to_string(a.get('expr'))}" for a in assignments]
        assign_str = ", ".join(parts)
        if mode == "update":
            lines.append(f"table {out} = {inp_ref} update!({assign_str})")
        else:
            lines.append(f"table {out} = {inp_ref} derive({assign_str})")
        return lines

    if op == "filter" and inp and out:
        pred = params.get("predicate")
        pred_str = _expr_to_string(pred) if pred else "true"
        lines.append(f"table {out} = {inp_ref} filter({pred_str})")
        return lines

    if op == "select" and inp and out:
        cols = params.get("cols") or []
        drop = params.get("drop") or []
        if cols:
            lines.append(f"table {out} = {inp_ref} select {', '.join(cols)}")
        elif drop:
            lines.append(f"table {out} = {inp_ref} drop {', '.join(drop)}")
        else:
            lines.append(f"table {out} = {inp_ref}")
        return lines

    if op == "rename" and inp and out:
        mapping = params.get("mapping") or []
        parts = [f"{p['from']} -> {p['to']}" for p in mapping]
        lines.append(f"table {out} = {inp_ref} rename({', '.join(parts)})")
        return lines

    if op == "sort" and inp and out:
        by = params.get("by") or []
        nodupkey = params.get("nodupkey", False)
        by_str = _sort_by_to_expanded(by)
        nodup = ".nodupkey(true)" if nodupkey else ""
        lines.append(f"table {out} = sort({inp_ref}).by({by_str}){nodup}")
        return lines

    if op == "aggregate" and inp and out:
        group_by = params.get("group_by") or []
        metrics = params.get("metrics") or []
        group_str = ", ".join(group_by)
        # Deterministic: group metrics by (col, op) then emit class/var/stats from canonical metrics
        cols_for_var = sorted(set(m["col"] for m in metrics))
        ops_for_stats = sorted(set(m["op"] for m in metrics))
        var_str = ", ".join(cols_for_var)
        stats_str = ", ".join(ops_for_stats)
        lines.append(f"table {out} = aggregate({inp_ref}).class({group_str}).var({var_str}).stats({stats_str})")
        return lines

    if op == "let_scalar":
        name = params.get("name", "")
        expr = params.get("expr")
        expr_str = _expr_to_string(expr) if expr else "null"
        lines.append(f"let {name} = {expr_str}")
        return lines

    if op == "const":
        # const { k = v, ... } with sorted keys for deterministic output; never emit map-style let.
        bindings = params.get("bindings") or {}
        parts = []
        for k in sorted(bindings.keys()):
            v = bindings[k]
            if v is None:
                v_str = "null"
            elif v is True:
                v_str = "true"
            elif v is False:
                v_str = "false"
            elif isinstance(v, str):
                v_str = f'"{v}"'
            elif isinstance(v, int):
                v_str = str(v)
            else:
                v_str = "null"
            parts.append(f"{k} = {v_str}")
        lines.append("const { " + ", ".join(parts) + " }")
        return lines

    if op == "assert":
        pred = params.get("predicate")
        pred_str = _expr_to_string(pred) if pred else "true"
        lines.append(f"assert {pred_str}")
        return lines

    if op == "save" and inputs:
        table = inputs[0]
        path = params.get("path") or ""
        name = params.get("name")
        if name:
            lines.append(f'save {table} to "{path}" as "{name}"')
        else:
            lines.append(f'save {table} to "{path}"')
        return lines

    if op == "format":
        # format from SAS proc format; not emitted in expanded.sans (no map-style let).
        return lines

    return lines


def irdoc_to_expanded_sans(doc: IRDoc) -> str:
    """Produce deterministic expanded.sans string from IRDoc."""
    header = "# sans 0.1\n"
    lines: List[str] = [header.strip()]
    for step in doc.steps:
        if isinstance(step, UnknownBlockStep):
            continue
        if isinstance(step, OpStep):
            for line in _step_to_expanded(step):
                if line:
                    lines.append(line)
    return "\n".join(lines) + "\n"
