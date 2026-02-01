from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from sans.ir import OpStep


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def canonical_step_payload(step: OpStep) -> Dict[str, Any]:
    return {
        "op": step.op,
        "inputs": list(step.inputs),
        "outputs": list(step.outputs),
        "params": _canonicalize(step.params or {}),
    }


def compute_step_id(step: OpStep) -> str:
    payload = canonical_step_payload(step)
    text = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()
