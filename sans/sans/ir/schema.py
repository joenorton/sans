from __future__ import annotations

import json
from typing import Any, Dict, List, Set

SANS_IR_VERSION = "0.1"


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) and item for item in value)


def validate_sans_ir(doc: Dict[str, Any], strict: bool = False) -> List[str]:
    if not isinstance(doc, dict):
        raise ValueError("sans.ir must be an object")
    if doc.get("version") != SANS_IR_VERSION:
        raise ValueError(f"sans.ir version must be {SANS_IR_VERSION!r}")
    if not isinstance(doc.get("datasources"), dict):
        raise ValueError("sans.ir requires object field 'datasources'")
    if not isinstance(doc.get("steps"), list):
        raise ValueError("sans.ir requires array field 'steps'")

    for ds_name, ds in doc["datasources"].items():
        if not isinstance(ds_name, str) or not ds_name:
            raise ValueError("datasource keys must be non-empty strings")
        if not isinstance(ds, dict):
            raise ValueError(f"datasource '{ds_name}' must be an object")
        if not isinstance(ds.get("kind"), str) or not ds["kind"]:
            raise ValueError(f"datasource '{ds_name}' requires non-empty 'kind'")
        if "path" in ds and not isinstance(ds["path"], str):
            raise ValueError(f"datasource '{ds_name}' path must be a string")
        if "columns" in ds:
            cols = ds["columns"]
            if not isinstance(cols, dict):
                raise ValueError(f"datasource '{ds_name}' columns must be an object")
            for col_name, col_type in cols.items():
                if not isinstance(col_name, str) or not col_name:
                    raise ValueError(f"datasource '{ds_name}' has invalid column name")
                if not isinstance(col_type, str) or not col_type:
                    raise ValueError(f"datasource '{ds_name}' has invalid type for column '{col_name}'")

    ids_seen: Set[str] = set()
    produced_tables: Set[str] = set()
    steps: List[Dict[str, Any]] = []
    for idx, step in enumerate(doc["steps"]):
        if not isinstance(step, dict):
            raise ValueError("each step must be an object")
        required = ("id", "op", "inputs", "outputs", "params")
        missing = [name for name in required if name not in step]
        if missing:
            raise ValueError(f"step missing required keys: {missing}")
        step_id = step["id"]
        if not isinstance(step_id, str) or not step_id:
            raise ValueError("step.id must be a non-empty string")
        if step_id in ids_seen:
            raise ValueError(f"duplicate step id: {step_id}")
        ids_seen.add(step_id)

        if not isinstance(step["op"], str) or not step["op"]:
            raise ValueError(f"step '{step_id}' has invalid op")
        if not _is_string_list(step["inputs"]):
            raise ValueError(f"step '{step_id}' has invalid inputs")
        if not _is_string_list(step["outputs"]):
            raise ValueError(f"step '{step_id}' has invalid outputs")
        if not isinstance(step["params"], dict):
            raise ValueError(f"step '{step_id}' has invalid params")

        # Topological reference invariant: each input table must already be produced,
        # unless it's a datasource pseudo-table.
        for inp in step["inputs"]:
            if inp.startswith("__datasource__"):
                continue
            if inp not in produced_tables:
                raise ValueError(
                    f"step '{step_id}' references input '{inp}' before it is produced"
                )
        for out in step["outputs"]:
            produced_tables.add(out)

        forbidden = {"transform_id", "transform_class_id", "step_id", "loc"}
        leaked = forbidden.intersection(step.keys())
        if leaked:
            raise ValueError(f"step '{step_id}' contains forbidden fields: {sorted(leaked)}")
        steps.append(step)

    return validate_sans_ir_structure(doc["datasources"], steps, strict=strict)


def validate_sans_ir_structure(
    datasources: Dict[str, Any], steps: List[Dict[str, Any]], strict: bool = False
) -> List[str]:
    """
    Structural invariants for sans.ir.

    Hard errors (always):
    - all inputs resolve to prior producers or declared datasource pseudo tables
    - no duplicate producers for the same output table
    - dependency graph is acyclic
    - at least one save step exists
    - every saved table is produced and traces back to a datasource

    Soft warnings (strict=False):
    - produced tables that are neither consumed nor saved
    - steps that do not contribute to any save

    strict=True upgrades soft warnings to errors.
    """
    producer_by_table: Dict[str, int] = {}
    non_save_consumers: Dict[str, int] = {}
    save_count_by_table: Dict[str, int] = {}
    save_pairs_seen: Set[tuple[str, str]] = set()
    step_deps: List[Set[int]] = []
    reverse_edges: Dict[int, Set[int]] = {}
    forward_edges: Dict[int, Set[int]] = {}
    save_step_indexes: List[int] = []
    warnings: List[str] = []

    # First pass: register all table producers to validate duplicate outputs.
    for idx, step in enumerate(steps):
        reverse_edges[idx] = set()
        forward_edges[idx] = set()
        for out in step["outputs"]:
            if out in producer_by_table:
                other_step = steps[producer_by_table[out]]["id"]
                step_id = step["id"]
                raise ValueError(
                    f"table '{out}' is produced by multiple steps: '{other_step}' and '{step_id}'"
                )
            producer_by_table[out] = idx

    # Second pass: validate references and build dependency graph.
    for idx, step in enumerate(steps):
        deps: Set[int] = set()
        step_id = step["id"]
        op = step["op"]
        inputs = step["inputs"]

        if op == "save":
            if len(inputs) != 1:
                raise ValueError(f"save step '{step_id}' must have exactly one input")
            save_step_indexes.append(idx)
            input_table = inputs[0]
            save_count_by_table[input_table] = save_count_by_table.get(input_table, 0) + 1
            save_path = str(step.get("params", {}).get("path", ""))
            pair = (input_table, save_path)
            if pair in save_pairs_seen:
                raise ValueError(
                    f"duplicate save destination for table '{input_table}' and path '{save_path}'"
                )
            save_pairs_seen.add(pair)

        for inp in inputs:
            if inp.startswith("__datasource__"):
                ds_name = inp[len("__datasource__") :]
                if ds_name not in datasources:
                    raise ValueError(
                        f"step '{step_id}' references undefined datasource '{ds_name}'"
                    )
                producer_idx = producer_by_table.get(inp)
                if producer_idx is None:
                    raise ValueError(
                        f"step '{step_id}' references datasource input '{inp}' without a datasource step"
                    )
                deps.add(producer_idx)
                reverse_edges[idx].add(producer_idx)
                forward_edges[producer_idx].add(idx)
                continue
            producer_idx = producer_by_table.get(inp)
            if producer_idx is None:
                raise ValueError(f"step '{step_id}' references unknown input table '{inp}'")
            deps.add(producer_idx)
            reverse_edges[idx].add(producer_idx)
            forward_edges[producer_idx].add(idx)
            if op != "save":
                non_save_consumers[inp] = non_save_consumers.get(inp, 0) + 1
        step_deps.append(deps)

    if not save_step_indexes:
        raise ValueError("sans.ir must contain at least one save step")

    # Cycle detection over step dependency graph, deterministic frontier by step id.
    deps_work = [set(deps) for deps in step_deps]
    indegree = [len(deps) for deps in deps_work]
    frontier = [idx for idx, deg in enumerate(indegree) if deg == 0]
    frontier.sort(key=lambda i: steps[i]["id"])
    topo_order: List[int] = []
    while frontier:
        cur = frontier.pop(0)
        topo_order.append(cur)
        for nxt, deps in enumerate(deps_work):
            if cur in deps:
                deps.remove(cur)
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    frontier.append(nxt)
        frontier.sort(key=lambda i: steps[i]["id"])
    if len(topo_order) != len(steps):
        raise ValueError("sans.ir contains cyclic step dependencies")

    # Saved tables define external boundary. Each saved table must be produced and
    # traceable back to at least one datasource.
    saved_tables = set(save_count_by_table.keys())
    if not saved_tables:
        raise ValueError("sans.ir must contain at least one saved table")
    for table in sorted(saved_tables):
        producer_idx = producer_by_table.get(table)
        if producer_idx is None:
            raise ValueError(f"saved table '{table}' is not produced by any step")
        stack = [producer_idx]
        seen: Set[int] = set()
        reaches_datasource = False
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            step = steps[cur]
            if step["op"] == "datasource":
                reaches_datasource = True
                break
            for dep in reverse_edges.get(cur, set()):
                stack.append(dep)
        if not reaches_datasource:
            raise ValueError(
                f"saved table '{table}' is not reachable from any datasource step"
            )

    # No dangling nodes: every produced table must be consumed by a downstream step or saved.
    for table in sorted(producer_by_table):
        if table.startswith("__datasource__"):
            continue
        if non_save_consumers.get(table, 0) == 0 and save_count_by_table.get(table, 0) == 0:
            warnings.append(f"dangling table '{table}' is neither consumed nor saved")

    # No unreachable steps: every step must contribute to at least one save.
    reachable: Set[int] = set()
    stack = list(save_step_indexes)
    while stack:
        idx = stack.pop()
        if idx in reachable:
            continue
        reachable.add(idx)
        for dep in reverse_edges.get(idx, set()):
            stack.append(dep)
    if len(reachable) != len(steps):
        unreachable = [steps[idx]["id"] for idx in range(len(steps)) if idx not in reachable]
        warnings.append(f"unreachable steps: {', '.join(unreachable)}")

    if strict and warnings:
        raise ValueError("; ".join(warnings))
    return sorted(warnings)


def canonical_json_dumps(doc: Dict[str, Any]) -> str:
    validate_sans_ir(doc)
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
