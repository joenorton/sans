from __future__ import annotations

"""Variable lineage graph builder.

See docs/sprints/VARIABLE_LINEAGE_GRAPH.md for scope and invariants.
"""

from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from pathlib import Path
import hashlib
import json
import logging

from .ir import IRDoc, OpStep
from .sans_script.canon import _canonicalize, compute_step_id, compute_transform_id

logger = logging.getLogger(__name__)


class SchemaTracker:
    """Tracks known columns for a table.

    Invariant: when present, the list is an ordered, known subset of columns.
    """

    def __init__(self, initial: Optional[Dict[str, List[str]]] = None) -> None:
        self._schema: Dict[str, List[str]] = {}
        if initial:
            for table_id, cols in initial.items():
                if cols is not None:
                    self._schema[table_id] = list(cols)

    def get(self, table_id: str) -> Optional[List[str]]:
        return self._schema.get(table_id)

    def set(self, table_id: str, cols: Optional[List[str]]) -> None:
        if cols is None:
            self._schema.pop(table_id, None)
            return
        self._schema[table_id] = list(cols)

    def items(self) -> Iterable[Tuple[str, List[str]]]:
        return self._schema.items()


def canonical_json_bytes(payload: Any) -> bytes:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text.encode("utf-8")


def compute_sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_params_sha256(params: Optional[Dict[str, Any]]) -> str:
    canonical = _canonicalize(params or {})
    return compute_sha256_hex(canonical_json_bytes(canonical))


def compute_expr_sha256(expr: Any) -> str:
    canonical = _canonicalize(expr or {})
    return compute_sha256_hex(canonical_json_bytes(canonical))


def compute_order_sha256(order_by: Any) -> str:
    canonical = _canonicalize(order_by or [])
    return compute_sha256_hex(canonical_json_bytes(canonical))


def collect_expr_cols(expr: Any) -> Set[str]:
    cols: Set[str] = set()

    def visit(node: Any) -> None:
        if not isinstance(node, dict):
            if isinstance(node, list):
                for item in node:
                    visit(item)
            return
        node_type = node.get("type")
        if node_type == "col":
            name = node.get("name")
            if isinstance(name, str) and name:
                cols.add(name)
            return
        if node_type == "lit":
            return
        if node_type == "binop":
            visit(node.get("left"))
            visit(node.get("right"))
            return
        if node_type == "boolop":
            visit(node.get("args", []))
            return
        if node_type == "unop":
            visit(node.get("arg"))
            return
        if node_type == "call":
            visit(node.get("args", []))
            return
        # Fallback: walk any nested dict/list values to avoid dropping deps
        for value in node.values():
            visit(value)

    visit(expr)
    return cols


def _infer_schema_two_pass(
    irdoc: IRDoc, initial_schema: Dict[str, List[str]]
) -> Dict[str, Optional[List[str]]]:
    schema: Dict[str, Optional[List[str]]] = {k: list(v) for k, v in (initial_schema or {}).items()}
    supported_ops = {"identity", "compute", "filter", "select", "sort", "rename"}

    for step in irdoc.steps:
        if not isinstance(step, OpStep):
            continue
        if step.op not in supported_ops:
            continue
        if not step.inputs or not step.outputs:
            continue
        input_table = step.inputs[0]
        output_table = step.outputs[0]
        input_schema = schema.get(input_table)

        if step.op == "compute":
            mode = step.params.get("mode") or "derive"
            assignments = step.params.get("assignments") or step.params.get("assign") or []
            if input_schema is None:
                schema[output_table] = None
            else:
                output_schema = list(input_schema)
                if mode != "update":
                    for assign in assignments:
                        target = assign.get("target") or assign.get("col")
                        if target and target not in output_schema:
                            output_schema.append(target)
                schema[output_table] = output_schema
            continue

        if step.op == "rename":
            mapping = step.params.get("mapping") or []
            rename_map = {pair.get("from"): pair.get("to") for pair in mapping if pair.get("from")}
            if input_schema is None:
                schema[output_table] = None
            else:
                schema[output_table] = [rename_map.get(col, col) for col in input_schema]
            continue

        if step.op == "select":
            keep = step.params.get("cols") or []
            drop = step.params.get("drop") or []
            if keep:
                schema[output_table] = list(keep)
            elif input_schema is None:
                schema[output_table] = None
            else:
                schema[output_table] = [col for col in input_schema if col not in drop]
            continue

        if step.op in {"filter", "sort", "identity"}:
            schema[output_table] = list(input_schema) if input_schema is not None else None
            continue

    # Backward propagation for schema-preserving ops and explicit select keep.
    for step in reversed(irdoc.steps):
        if not isinstance(step, OpStep):
            continue
        if step.op not in supported_ops:
            continue
        if not step.inputs or not step.outputs:
            continue
        input_table = step.inputs[0]
        output_table = step.outputs[0]
        output_schema = schema.get(output_table)
        if output_schema is None:
            continue
        if schema.get(input_table) is not None:
            continue
        if step.op in {"filter", "sort", "identity"}:
            schema[input_table] = list(output_schema)
            continue
        if step.op == "select":
            keep = step.params.get("cols") or []
            if keep:
                schema[input_table] = list(output_schema)
            continue

    return schema


def build_var_graph(irdoc: IRDoc, initial_schema: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
    initial_schema = initial_schema or {}
    schema_map = _infer_schema_two_pass(irdoc, initial_schema)
    nodes_by_id: Dict[str, Dict[str, Any]] = {}
    edges: Set[Tuple[str, str, str]] = set()
    # Invariant: pass-through nodes never carry provenance. Only derived nodes do.
    # This keeps vars.graph stable across row/order-only changes (captured in table.effects).

    produced_tables: Set[str] = set()
    for step in irdoc.steps:
        if isinstance(step, OpStep) and step.outputs:
            produced_tables.update(step.outputs)

    def node_id(table_id: str, col: str) -> str:
        return f"v:{table_id}.{col}"

    def add_node(
        table_id: str,
        col: str,
        origin: str,
        step_id: Optional[str],
        transform_id: Optional[str],
        payload_sha256: Optional[str],
        expr_sha256: Optional[str] = None,
    ) -> None:
        nid = node_id(table_id, col)
        if nid in nodes_by_id:
            return
        node: Dict[str, Any] = {
            "id": nid,
            "kind": "variable",
            "table_id": table_id,
            "col": col,
            "origin": origin,
            "producing_step_id": step_id,
            "transform_id": transform_id,
            "payload_sha256": payload_sha256,
        }
        if expr_sha256 is not None:
            node["expr_sha256"] = expr_sha256
        nodes_by_id[nid] = node

    def ensure_input_node(table_id: str, col: str) -> None:
        nid = node_id(table_id, col)
        if nid in nodes_by_id:
            return
        origin = "pass_through" if table_id in produced_tables else "source"
        add_node(table_id, col, origin, None, None, None)

    def add_edge(src_table: str, src_col: str, dst_table: str, dst_col: str, kind: str = "flow") -> None:
        ensure_input_node(src_table, src_col)
        edges.add((node_id(src_table, src_col), node_id(dst_table, dst_col), kind))

    # Seed known source schemas
    for table_id, cols in initial_schema.items():
        if not cols:
            continue
        for col in cols:
            add_node(table_id, col, "source", None, None, None)

    supported_ops = {"identity", "compute", "filter", "select", "sort", "rename"}

    for step in irdoc.steps:
        if not isinstance(step, OpStep):
            continue
        if step.op not in supported_ops:
            continue
        if not step.inputs or not step.outputs:
            continue
        input_table = step.inputs[0]
        output_table = step.outputs[0]

        transform_id = compute_transform_id(step.op, step.params)
        step_id = compute_step_id(transform_id, step.inputs, step.outputs)
        payload_sha256 = compute_params_sha256(step.params)

        input_schema = schema_map.get(input_table)

        if step.op == "compute":
            assignments = step.params.get("assignments") or step.params.get("assign") or []
            assigned_cols: List[str] = []
            for assign in assignments:
                target = assign.get("target") or assign.get("col")
                if not target:
                    continue
                assigned_cols.append(target)
                expr = assign.get("expr")
                expr_sha256 = compute_expr_sha256(expr)
                for ref in sorted(collect_expr_cols(expr)):
                    add_edge(input_table, ref, output_table, target, kind="derivation")
                add_node(
                    output_table,
                    target,
                    "derived",
                    step_id,
                    transform_id,
                    payload_sha256,
                    expr_sha256=expr_sha256,
                )

            if input_schema is None:
                logger.warning(
                    "vars.graph: schema unknown for '%s' in compute; skipping pass-through edges",
                    input_table,
                )
                # schema unknown; no pass-through edges
            else:
                assigned_set = set(assigned_cols)
                for col in input_schema:
                    if col in assigned_set:
                        continue
                    add_node(output_table, col, "pass_through", None, None, None)
                    add_edge(input_table, col, output_table, col)
            continue

        if step.op == "rename":
            mapping = step.params.get("mapping") or []
            rename_map = {pair.get("from"): pair.get("to") for pair in mapping if pair.get("from")}
            for src, dst in rename_map.items():
                if not dst:
                    continue
                add_node(output_table, dst, "derived", step_id, transform_id, payload_sha256)
                add_edge(input_table, src, output_table, dst, kind="rename")

            if input_schema is None:
                logger.warning(
                    "vars.graph: schema unknown for '%s' in rename; skipping pass-through edges",
                    input_table,
                )
            else:
                for col in input_schema:
                    if col in rename_map:
                        continue
                    dst_col = col
                    add_node(output_table, dst_col, "pass_through", None, None, None)
                    add_edge(input_table, col, output_table, dst_col, kind="flow")
            continue

        if step.op == "select":
            keep = step.params.get("cols") or []
            drop = step.params.get("drop") or []
            if keep:
                for col in keep:
                    add_node(output_table, col, "pass_through", None, None, None)
                    add_edge(input_table, col, output_table, col, kind="flow")
            elif input_schema is None:
                logger.warning(
                    "vars.graph: schema unknown for '%s' in select(drop); skipping pass-through edges",
                    input_table,
                )
            else:
                for col in input_schema:
                    if col in drop:
                        continue
                    add_node(output_table, col, "pass_through", None, None, None)
                    add_edge(input_table, col, output_table, col, kind="flow")
            continue

        if step.op in {"filter", "sort", "identity"}:
            if input_schema is None:
                logger.warning(
                    "vars.graph: schema unknown for '%s' in %s; skipping pass-through edges",
                    input_table,
                    step.op,
                )
            else:
                for col in input_schema:
                    # Row/order effects do not belong in vars.graph; keep provenance null.
                    add_node(output_table, col, "pass_through", None, None, None)
                    add_edge(input_table, col, output_table, col, kind="flow")
            continue

    nodes = sorted(nodes_by_id.values(), key=lambda n: n["id"])
    edge_list = [{"src": src, "dst": dst, "kind": kind} for src, dst, kind in sorted(edges)]
    return {"nodes": nodes, "edges": edge_list}


def build_table_effects(irdoc: IRDoc) -> Dict[str, Any]:
    # v1 scope: only filter and sort (row/order effects).
    effects: List[Dict[str, Any]] = []

    def _normalize_by(by: Any) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for entry in by or []:
            if isinstance(entry, dict):
                col = entry.get("col")
                if not col:
                    continue
                if "asc" in entry:
                    asc = bool(entry.get("asc"))
                else:
                    asc = not bool(entry.get("desc", False))
                normalized.append({"col": col, "asc": asc})
            elif isinstance(entry, str):
                normalized.append({"col": entry, "asc": True})
        return normalized

    for step in irdoc.steps:
        if not isinstance(step, OpStep):
            continue
        if not step.inputs or not step.outputs:
            continue
        if step.op not in {"filter", "sort"}:
            continue

        transform_id = compute_transform_id(step.op, step.params)
        step_id = compute_step_id(transform_id, step.inputs, step.outputs)
        payload_sha256 = compute_params_sha256(step.params)
        input_table = step.inputs[0]
        output_table = step.outputs[0]

        if step.op == "filter":
            predicate = step.params.get("predicate")
            cols = sorted(collect_expr_cols(predicate))
            effects.append(
                {
                    "kind": "filter",
                    "in_table": input_table,
                    "out_table": output_table,
                    "producing_step_id": step_id,
                    "transform_id": transform_id,
                    "payload_sha256": payload_sha256,
                    "predicate_sha256": compute_expr_sha256(predicate),
                    "predicate_cols": cols,
                }
            )
        elif step.op == "sort":
            by = step.params.get("by") or []
            normalized_by = _normalize_by(by)
            effects.append(
                {
                    "kind": "sort",
                    "in_table": input_table,
                    "out_table": output_table,
                    "producing_step_id": step_id,
                    "transform_id": transform_id,
                    "payload_sha256": payload_sha256,
                    "order_sha256": compute_order_sha256(normalized_by),
                    "by": _canonicalize(normalized_by),
                    "by_cols": [entry["col"] for entry in normalized_by],
                }
            )

    effects.sort(key=lambda e: (e["out_table"], e["kind"], e["producing_step_id"]))
    return {
        "schema_version": 1,
        "stats": {"event_count": len(effects)},
        "effects": effects,
    }


def write_vars_graph_json(graph: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        graph, ensure_ascii=False, sort_keys=True, separators=(",", ":"), indent=None
    )
    path.write_text(f"{payload}\n", encoding="utf-8")


def write_table_effects_json(effects: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload_no_hash = dict(effects)
    payload_no_hash.pop("sha256", None)
    # sha256 is computed over canonical JSON without the sha field.
    payload_no_hash["sha256"] = compute_sha256_hex(canonical_json_bytes(payload_no_hash))
    payload = json.dumps(
        payload_no_hash, ensure_ascii=False, sort_keys=True, separators=(",", ":"), indent=None
    )
    path.write_text(f"{payload}\n", encoding="utf-8")
