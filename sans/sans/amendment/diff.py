from __future__ import annotations

import copy
import hashlib
import json
from typing import Any, Dict, Iterable, List, Set


def canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def canonical_sha256(obj: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(obj)).hexdigest()


def derive_transform_id(step: Dict[str, Any]) -> str:
    payload = {"op": step.get("op"), "params": copy.deepcopy(step.get("params", {}))}
    return canonical_sha256(payload)


def build_table_universe(ir_doc: Dict[str, Any]) -> Set[str]:
    names: Set[str] = set()
    for step in ir_doc.get("steps", []):
        for out in step.get("outputs", []):
            if isinstance(out, str) and out and not out.startswith("__datasource__"):
                names.add(out)
    tables = ir_doc.get("tables")
    if isinstance(tables, list):
        for table_name in tables:
            if isinstance(table_name, str) and table_name:
                names.add(table_name)
    return names


def build_structural_diff(
    ir_in: Dict[str, Any],
    ir_out: Dict[str, Any],
    ops_applied: List[Dict[str, Any]],
    affected_steps: Iterable[str],
    affected_tables: Iterable[str],
) -> Dict[str, Any]:
    in_by_id = {
        step.get("id"): derive_transform_id(step)
        for step in ir_in.get("steps", [])
        if isinstance(step, dict) and isinstance(step.get("id"), str)
    }
    out_by_id = {
        step.get("id"): derive_transform_id(step)
        for step in ir_out.get("steps", [])
        if isinstance(step, dict) and isinstance(step.get("id"), str)
    }

    added_step_ids = sorted(set(out_by_id.keys()) - set(in_by_id.keys()))
    removed_step_ids = sorted(set(in_by_id.keys()) - set(out_by_id.keys()))
    common_step_ids = sorted(set(in_by_id.keys()) & set(out_by_id.keys()))

    transforms_added = sorted({out_by_id[step_id] for step_id in added_step_ids})
    transforms_removed = sorted({in_by_id[step_id] for step_id in removed_step_ids})
    transforms_changed = []
    for step_id in common_step_ids:
        before = in_by_id[step_id]
        after = out_by_id[step_id]
        if before != after:
            transforms_changed.append({"before": before, "after": after, "step_id": step_id})

    return {
        "format": "sans.mutation.diff.structural",
        "version": 1,
        "base_ir_sha256": canonical_sha256(ir_in),
        "mutated_ir_sha256": canonical_sha256(ir_out),
        "ops_applied": ops_applied,
        "affected": {
            "steps": sorted(set(affected_steps)),
            "tables": sorted(set(affected_tables)),
            "transforms_added": transforms_added,
            "transforms_removed": transforms_removed,
            "transforms_changed": transforms_changed,
        },
    }


def build_assertion_diff(
    assertions_before: List[Dict[str, Any]], assertions_after: List[Dict[str, Any]]
) -> Dict[str, Any]:
    before_by_id = {
        item.get("assertion_id"): item
        for item in assertions_before
        if isinstance(item, dict) and isinstance(item.get("assertion_id"), str)
    }
    after_by_id = {
        item.get("assertion_id"): item
        for item in assertions_after
        if isinstance(item, dict) and isinstance(item.get("assertion_id"), str)
    }
    before_ids = set(before_by_id.keys())
    after_ids = set(after_by_id.keys())

    added_ids = sorted(after_ids - before_ids)
    removed_ids = sorted(before_ids - after_ids)
    common_ids = sorted(before_ids & after_ids)

    modified = []
    for assertion_id in common_ids:
        if before_by_id[assertion_id] != after_by_id[assertion_id]:
            modified.append(
                {"before": before_by_id[assertion_id], "after": after_by_id[assertion_id]}
            )

    return {
        "format": "sans.mutation.diff.assertions",
        "version": 1,
        "added": [after_by_id[assertion_id] for assertion_id in added_ids],
        "removed": [before_by_id[assertion_id] for assertion_id in removed_ids],
        "modified": modified,
    }


def build_diagnostics(
    *,
    status: str,
    refusals: List[Dict[str, Any]] | None = None,
    warnings: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    return {
        "format": "sans.mutation.diagnostics",
        "version": 1,
        "status": status,
        "refusals": refusals or [],
        "warnings": warnings or [],
    }

