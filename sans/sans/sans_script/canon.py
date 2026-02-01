from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List

from sans.ir import OpStep


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def compute_transform_id(op: str, params: Dict[str, Any]) -> str:
    payload = {
        "op": op,
        "params": _canonicalize(params or {}),
    }
    text = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def compute_step_id(transform_id: str, inputs: List[str], outputs: List[str]) -> str:
    payload = {
        "transform_id": transform_id,
        "inputs": sorted(list(inputs)),
        "outputs": sorted(list(outputs)),
    }
    # Wait, the contract says:
    # step_payload = {
    #   "transform_id": <transform_id>,
    #   "inputs":  [<logical table names>],
    #   "outputs": [<logical table names>]
    # }
    # It doesn't explicitly say to sort them, but "stable list ordering is the order provided in the structure"
    # Actually, for step_id, the order of inputs/outputs might matter if the op cares about order (like merge or set).
    # Re-reading: "stable list ordering is the order provided in the structure (unless explicitly stated otherwise)"
    # So I should NOT sort them if they have inherent order. 
    # Inputs for MERGE or SET definitely have order.
    
    payload = {
        "transform_id": transform_id,
        "inputs": list(inputs),
        "outputs": list(outputs),
    }
    text = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def canonical_step_payload(step: OpStep) -> Dict[str, Any]:
    # Deprecated/Old format, but let's keep it for a moment if needed or update it.
    # The new contract uses transform_id and step_id.
    t_id = compute_transform_id(step.op, step.params)
    return {
        "transform_id": t_id,
        "step_id": compute_step_id(t_id, step.inputs, step.outputs),
        "op": step.op,
        "inputs": list(step.inputs),
        "outputs": list(step.outputs),
        "params": _canonicalize(step.params or {}),
    }
