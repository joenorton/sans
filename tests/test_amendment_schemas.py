from __future__ import annotations

from sans.amendment.apply import apply_amendment


def _base_ir() -> dict:
    return {
        "version": "0.1",
        "datasources": {"lb": {"kind": "csv", "path": "lb.csv"}},
        "steps": [
            {
                "id": "ds:lb",
                "op": "datasource",
                "inputs": [],
                "outputs": ["__datasource__lb"],
                "params": {"name": "lb", "kind": "csv", "path": "lb.csv"},
            },
            {
                "id": "out:t1",
                "op": "identity",
                "inputs": ["__datasource__lb"],
                "outputs": ["t1"],
                "params": {"assign": [{"expr": {"type": "lit", "value": 1}}]},
            },
            {
                "id": "out:t1:save",
                "op": "save",
                "inputs": ["t1"],
                "outputs": [],
                "params": {"path": "t1.csv"},
            },
        ],
    }


def _refusal_code(result) -> str:
    return result.diagnostics["refusals"][0]["code"]


def test_schema_unknown_request_field_refused():
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [],
        "unknown": 1,
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_set_params_missing_path_refused():
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [
            {
                "op_id": "op1",
                "kind": "set_params",
                "selector": {"step_id": "out:t1"},
                "params": {"value": 2},
            }
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_duplicate_op_id_refused():
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [
            {
                "op_id": "dup",
                "kind": "remove_step",
                "selector": {"step_id": "out:t1"},
                "params": {},
            },
            {
                "op_id": "dup",
                "kind": "remove_step",
                "selector": {"step_id": "out:t1:save"},
                "params": {},
            },
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_add_step_anchor_xor_violation_refused():
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [
            {
                "op_id": "op1",
                "kind": "add_step",
                "selector": {"before_step_id": "out:t1", "index": 0},
                "params": {
                    "step": {
                        "id": "new:1",
                        "op": "identity",
                        "inputs": ["t1"],
                        "outputs": ["t2"],
                        "params": {},
                    }
                },
            }
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_selector_path_invalid_escape_refused():
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [
            {
                "op_id": "op1",
                "kind": "set_params",
                "selector": {"step_id": "out:t1", "path": "/~2"},
                "params": {"value": 2},
            }
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_set_params_selector_table_refused():
    """selector.table not allowed for set_params; requires step_id or transform_id."""
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [
            {
                "op_id": "op1",
                "kind": "set_params",
                "selector": {"table": "t1", "path": "/"},
                "params": {"value": {}},
            }
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_rewire_inputs_selector_path_refused():
    """selector.path not allowed for rewire_inputs."""
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [
            {
                "op_id": "op1",
                "kind": "rewire_inputs",
                "selector": {"step_id": "out:t1", "path": "/x"},
                "params": {"inputs": ["t1"]},
            }
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_rewire_inputs_selector_assertion_id_refused():
    """selector.assertion_id not allowed for rewire_inputs."""
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {},
        "ops": [
            {
                "op_id": "op1",
                "kind": "rewire_inputs",
                "selector": {"step_id": "out:t1", "assertion_id": "a1"},
                "params": {"inputs": ["t1"]},
            }
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"


def test_schema_remove_step_selector_table_only_refused():
    """selector.table not allowed for remove_step; requires step_id or transform_id."""
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {"allow_destructive": True},
        "ops": [
            {
                "op_id": "op1",
                "kind": "remove_step",
                "selector": {"table": "t1"},
                "params": {},
            }
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _refusal_code(result) == "E_AMEND_VALIDATION_SCHEMA"

