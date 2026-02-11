from __future__ import annotations

import hashlib
from typing import Any, Dict, List

from . import IRDoc, OpStep, UnknownBlockStep, is_ds_input, ds_name_from_input
from .schema import SANS_IR_VERSION
from ..types import type_name


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def _semantic_step_id(step: OpStep) -> str:
    if step.op == "datasource":
        name = step.params.get("name")
        if isinstance(name, str) and name:
            return f"ds:{name}"
        if step.outputs and is_ds_input(step.outputs[0]):
            return f"ds:{ds_name_from_input(step.outputs[0])}"
    if len(step.outputs) == 1:
        return f"out:{step.outputs[0]}"
    if step.op == "save" and step.inputs:
        return f"out:{step.inputs[0]}:save"
    payload = {
        "op": step.op,
        "inputs": list(step.inputs),
        "outputs": list(step.outputs),
        "params": _canonicalize(step.params or {}),
    }
    text = str(payload).encode("utf-8")
    return f"{step.op}:{hashlib.sha256(text).hexdigest()[:12]}"


def _topologically_sorted_steps(steps: List[OpStep]) -> List[OpStep]:
    producers: Dict[str, int] = {}
    for idx, step in enumerate(steps):
        for out in step.outputs:
            if out not in producers:
                producers[out] = idx

    indegree = [0] * len(steps)
    edges: Dict[int, List[int]] = {i: [] for i in range(len(steps))}
    for idx, step in enumerate(steps):
        deps = set()
        for inp in step.inputs:
            if inp.startswith("__datasource__"):
                continue
            prod = producers.get(inp)
            if prod is not None and prod != idx:
                deps.add(prod)
        indegree[idx] = len(deps)
        for dep in deps:
            edges[dep].append(idx)

    # Stable Kahn ordering by original index.
    queue = [idx for idx, d in enumerate(indegree) if d == 0]
    queue.sort()
    ordered: List[int] = []
    while queue:
        idx = queue.pop(0)
        ordered.append(idx)
        for nxt in sorted(edges[idx]):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
        queue.sort()

    if len(ordered) != len(steps):
        # Fall back to the original deterministic order when the graph has cycles.
        return list(steps)
    return [steps[idx] for idx in ordered]


def irdoc_to_sans_ir(doc: IRDoc) -> Dict[str, Any]:
    op_steps: List[OpStep] = []
    for step in doc.steps:
        if isinstance(step, UnknownBlockStep):
            raise ValueError(
                "Cannot normalize IRDoc containing UnknownBlockStep; canonical sans.ir is semantic-only."
            )
        if isinstance(step, OpStep):
            op_steps.append(step)

    sorted_steps = _topologically_sorted_steps(op_steps)

    datasources: Dict[str, Any] = {}
    for name in sorted(doc.datasources):
        ds = doc.datasources[name]
        columns: Dict[str, str] = {}
        if ds.column_types:
            columns = {col: type_name(ds.column_types[col]) for col in sorted(ds.column_types)}
        elif ds.columns:
            columns = {col: "string" for col in ds.columns}
        ds_entry: Dict[str, Any] = {"kind": ds.kind}
        if ds.path is not None:
            ds_entry["path"] = ds.path
        if columns:
            ds_entry["columns"] = columns
        if ds.inline_text:
            ds_entry["inline_text"] = ds.inline_text
        if ds.inline_sha256:
            ds_entry["inline_sha256"] = ds.inline_sha256
        datasources[name] = ds_entry

    steps: List[Dict[str, Any]] = []
    for step in sorted_steps:
        steps.append(
            {
                "id": _semantic_step_id(step),
                "op": step.op,
                "inputs": list(step.inputs),
                "outputs": list(step.outputs),
                "params": _canonicalize(step.params or {}),
            }
        )

    return {
        "version": SANS_IR_VERSION,
        "datasources": datasources,
        "steps": steps,
    }
