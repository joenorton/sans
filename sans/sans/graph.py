from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import hashlib
import json

from .ir import IRDoc, OpStep
from .sans_script.canon import compute_step_id, compute_transform_id, _canonicalize


def _canonical_json_bytes(payload: Any) -> bytes:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text.encode("utf-8")


def compute_step_payload_sha256(step: OpStep) -> str:
    t_id = compute_transform_id(step.op, step.params)
    payload = {
        "op": step.op,
        "inputs": list(step.inputs),
        "outputs": list(step.outputs),
        "params": _canonicalize(step.params or {}),
        "transform_id": t_id,
    }
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def build_graph(irdoc: IRDoc, producer: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, str]] = []
    ordered_steps: List[Tuple[OpStep, str]] = []
    producer_by_table: Dict[str, str] = {}
    consumers_by_table: Dict[str, List[str]] = {}
    all_tables: set[str] = set(irdoc.tables or set())

    for step in irdoc.steps:
        if not isinstance(step, OpStep):
            continue
        t_id = compute_transform_id(step.op, step.params)
        step_id = compute_step_id(t_id, step.inputs, step.outputs)
        step_node_id = f"s:{step_id}"
        ordered_steps.append((step, step_id))
        nodes.append(
            {
                "id": step_node_id,
                "kind": "step",
                "op": step.op,
                "transform_id": t_id,
                "inputs": sorted([f"t:{t}" for t in step.inputs]),
                "outputs": sorted([f"t:{t}" for t in step.outputs]),
                "payload_sha256": compute_step_payload_sha256(step),
            }
        )

    for step, step_id in ordered_steps:
        step_node_id = f"s:{step_id}"
        for input_table in step.inputs:
            all_tables.add(input_table)
            consumers_by_table.setdefault(input_table, [])
            consumers_by_table[input_table].append(step_id)
            edges.append(
                {"src": f"t:{input_table}", "dst": step_node_id, "kind": "consumes"}
            )
        for output_table in step.outputs:
            all_tables.add(output_table)
            existing = producer_by_table.get(output_table)
            if existing and existing != step_id:
                raise ValueError(
                    f"Multiple producers for table '{output_table}': {existing}, {step_id}"
                )
            producer_by_table[output_table] = step_id
            edges.append(
                {"src": step_node_id, "dst": f"t:{output_table}", "kind": "produces"}
            )

    for table_id in sorted(all_tables):
        producer_step = producer_by_table.get(table_id)
        consumers = sorted(consumers_by_table.get(table_id, []))
        nodes.append(
            {
                "id": f"t:{table_id}",
                "kind": "table",
                "producer": f"s:{producer_step}" if producer_step else None,
                "consumers": [f"s:{c}" for c in consumers],
            }
        )

    nodes.sort(key=lambda n: n["id"])
    edges.sort(key=lambda e: (e["src"], e["dst"], e["kind"]))

    graph: Dict[str, Any] = {"schema_version": 1, "nodes": nodes, "edges": edges}
    if producer:
        graph["producer"] = producer
    return graph


def write_graph_json(graph: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        graph, ensure_ascii=False, sort_keys=True, separators=(",", ":"), indent=None
    )
    path.write_text(f"{payload}\n", encoding="utf-8")
