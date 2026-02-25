from __future__ import annotations

from sans.amendment.apply import apply_amendment
from sans.amendment.diff import build_table_universe, canonical_sha256, derive_transform_id


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
                "params": {},
            },
            {
                "id": "out:t2",
                "op": "compute",
                "inputs": ["t1"],
                "outputs": ["t2"],
                "params": {"assignments": [{"target": "x", "expr": {"type": "lit", "value": 2}}]},
            },
            {
                "id": "out:t2:save",
                "op": "save",
                "inputs": ["t2"],
                "outputs": [],
                "params": {"path": "t2.csv"},
            },
        ],
        "assertions": [{"assertion_id": "a1", "type": "row_count_bound", "table": "t2"}],
    }


def _req_with_op(op: dict, policy: dict | None = None) -> dict:
    return {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": policy or {},
        "ops": [op],
    }


def _code(result) -> str:
    return result.diagnostics["refusals"][0]["code"]


def test_ops_over_cap_refused():
    req = {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": {"max_ops": 1},
        "ops": [
            {
                "op_id": "op1",
                "kind": "set_params",
                "selector": {"step_id": "out:t1", "path": "/"},
                "params": {"value": {"assign": [{"expr": {"type": "lit", "value": 3}}]}},
            },
            {
                "op_id": "op2",
                "kind": "set_params",
                "selector": {"step_id": "out:t2", "path": "/"},
                "params": {"value": {"assign": [{"expr": {"type": "lit", "value": 4}}]}},
            },
        ],
    }
    result = apply_amendment(_base_ir(), req)
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_CAPABILITY_LIMIT"


def test_set_params_path_not_found_refused():
    op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:t2", "path": "/missing"},
        "params": {"value": 1},
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_PATH_NOT_FOUND"


def test_add_step_index_out_of_range_refused():
    op = {
        "op_id": "op1",
        "kind": "add_step",
        "selector": {"index": 99},
        "params": {
            "step": {
                "id": "new:step",
                "op": "identity",
                "inputs": ["t2"],
                "outputs": ["t3"],
                "params": {},
            }
        },
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_INDEX_OUT_OF_RANGE"


def test_add_step_output_collision_refused():
    op = {
        "op_id": "op1",
        "kind": "add_step",
        "selector": {"index": 2},
        "params": {
            "step": {
                "id": "new:step",
                "op": "identity",
                "inputs": ["t1"],
                "outputs": ["t2"],
                "params": {},
            }
        },
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_OUTPUT_TABLE_COLLISION"


def test_remove_assertion_without_policy_refused():
    op = {
        "op_id": "op1",
        "kind": "remove_assertion",
        "selector": {"assertion_id": "a1"},
        "params": {},
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_POLICY_DESTRUCTIVE_REFUSED"


def test_step_selector_mismatch_refused():
    ir_doc = _base_ir()
    save_step = ir_doc["steps"][3]
    transform_id = derive_transform_id(save_step)
    op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:t2", "transform_id": transform_id, "path": "/"},
        "params": {"value": {"assignments": [{"target": "x", "expr": {"type": "lit", "value": 9}}]}},
    }
    result = apply_amendment(ir_doc, _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_TARGET_MISMATCH"


def test_add_assertion_missing_assertion_id_refused():
    op = {
        "op_id": "op1",
        "kind": "add_assertion",
        "selector": {"table": "t2"},
        "params": {"assertion": {"type": "row_count_bound"}},
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_ASSERTION_ID_REQUIRED"


def test_add_assertion_assertion_id_collision_refused():
    op = {
        "op_id": "op1",
        "kind": "add_assertion",
        "selector": {"table": "t2"},
        "params": {"assertion": {"assertion_id": "a1", "type": "row_count_bound"}},
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_ASSERTION_ID_COLLISION"


def test_transform_id_selector_ambiguous_refused():
    ir_doc = _base_ir()
    # Same op+params => same derived transform_id by v0.1 rule.
    ir_doc["steps"][1]["op"] = "identity"
    ir_doc["steps"][1]["params"] = {}
    ir_doc["steps"][2]["op"] = "identity"
    ir_doc["steps"][2]["params"] = {}
    tid = derive_transform_id(ir_doc["steps"][1])
    op = {
        "op_id": "op1",
        "kind": "remove_step",
        "selector": {"transform_id": tid},
        "params": {},
    }
    result = apply_amendment(ir_doc, _req_with_op(op, policy={"allow_destructive": True}))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_TARGET_AMBIGUOUS"


def test_set_params_invalid_pointer_refused():
    op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:t2", "path": "/assignments/not-an-index"},
        "params": {"value": 1},
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_PATH_INVALID"


def test_canonical_hash_deterministic():
    obj = {"b": 1, "a": {"x": 2, "y": 3}}
    assert canonical_sha256(obj) == canonical_sha256({"a": {"y": 3, "x": 2}, "b": 1})


def test_transform_id_ignores_step_id_and_wiring():
    step_a = {"id": "a", "op": "identity", "inputs": ["x"], "outputs": ["y"], "params": {"k": 1}}
    step_b = {"id": "b", "op": "identity", "inputs": ["q"], "outputs": ["z"], "params": {"k": 1}}
    assert derive_transform_id(step_a) == derive_transform_id(step_b)


def test_set_params_schema_violation_refused_with_field_path():
    op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:t2", "path": "/assignments"},
        "params": {"value": "not-a-list"},
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_IR_INVALID"
    refusal_meta = result.diagnostics["refusals"][0]["meta"]
    assert refusal_meta.get("field_path")


def test_table_universe_helper_and_collision_refusal_with_tables_list():
    ir_doc = _base_ir()
    ir_doc["tables"] = ["reserved_out"]
    universe = build_table_universe(ir_doc)
    assert "reserved_out" in universe
    assert "__datasource__lb" not in universe

    op = {
        "op_id": "op1",
        "kind": "add_step",
        "selector": {"index": 3},
        "params": {
            "step": {
                "id": "new:step",
                "op": "identity",
                "inputs": ["t2"],
                "outputs": ["reserved_out"],
                "params": {},
            }
        },
    }
    result = apply_amendment(ir_doc, _req_with_op(op))
    assert result.status == "refused"
    assert _code(result) == "E_AMEND_OUTPUT_TABLE_COLLISION"


def test_refusal_emits_exactly_one_payload():
    op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:t1", "path": "/missing"},
        "params": {"value": 1},
    }
    result = apply_amendment(_base_ir(), _req_with_op(op))
    assert result.status == "refused"
    assert len(result.diagnostics["refusals"]) == 1

