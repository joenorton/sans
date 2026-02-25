from __future__ import annotations

import copy

from sans.amendment.apply import apply_amendment
from sans.amendment.diff import canonical_sha256, derive_transform_id


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
    }


def _req_with_op(op: dict, policy: dict | None = None) -> dict:
    return {
        "format": "sans.amendment_request",
        "version": 1,
        "contract_version": "0.1",
        "policy": policy or {},
        "ops": [op],
    }


def test_set_params_success_changes_only_target_leaf():
    ir_in = _base_ir()
    ir_before = copy.deepcopy(ir_in)
    op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:t2", "path": "/assignments/0/expr/value"},
        "params": {"value": 7},
    }
    result = apply_amendment(ir_in, _req_with_op(op))
    assert result.status == "ok"
    ir_out = result.ir_out
    assert canonical_sha256(ir_before) != canonical_sha256(ir_out)
    assert ir_out["steps"][2]["params"]["assignments"][0]["expr"]["value"] == 7
    assert ir_out["steps"][0] == ir_before["steps"][0]
    assert ir_out["steps"][1] == ir_before["steps"][1]
    assert ir_out["steps"][3] == ir_before["steps"][3]


def test_transform_id_stability_and_param_sensitivity():
    step_a = {"id": "a", "op": "identity", "inputs": ["x"], "outputs": ["y"], "params": {"k": 1}}
    step_b = {"id": "b", "op": "identity", "inputs": ["q"], "outputs": ["z"], "params": {"k": 1}}
    step_c = {"id": "c", "op": "identity", "inputs": ["q"], "outputs": ["z"], "params": {"k": 2}}
    assert derive_transform_id(step_a) == derive_transform_id(step_b)
    assert derive_transform_id(step_a) != derive_transform_id(step_c)


def test_add_step_success_insert_index_before_after_and_unique_ids():
    # insert at index
    ir_a = _base_ir()
    op_index = {
        "op_id": "op1",
        "kind": "add_step",
        "selector": {"index": 2},
        "params": {
            "step": {
                "id": "out:t1_5",
                "op": "identity",
                "inputs": ["t1"],
                "outputs": ["t1_5"],
                "params": {},
            }
        },
    }
    result_a = apply_amendment(ir_a, _req_with_op(op_index))
    assert result_a.status == "ok"
    assert result_a.ir_out["steps"][2]["id"] == "out:t1_5"

    # insert before step id
    ir_b = _base_ir()
    op_before = {
        "op_id": "op1",
        "kind": "add_step",
        "selector": {"before_step_id": "out:t2"},
        "params": {
            "step": {
                "id": "out:t1_before",
                "op": "identity",
                "inputs": ["t1"],
                "outputs": ["t1_before"],
                "params": {},
            }
        },
    }
    result_b = apply_amendment(ir_b, _req_with_op(op_before))
    assert result_b.status == "ok"
    ids_b = [step["id"] for step in result_b.ir_out["steps"]]
    assert ids_b.index("out:t1_before") == ids_b.index("out:t2") - 1

    # insert after step id
    ir_c = _base_ir()
    op_after = {
        "op_id": "op1",
        "kind": "add_step",
        "selector": {"after_step_id": "out:t1"},
        "params": {
            "step": {
                "id": "out:t1_after",
                "op": "identity",
                "inputs": ["t1"],
                "outputs": ["t1_after"],
                "params": {},
            }
        },
    }
    result_c = apply_amendment(ir_c, _req_with_op(op_after))
    assert result_c.status == "ok"
    ids_c = [step["id"] for step in result_c.ir_out["steps"]]
    assert ids_c.index("out:t1_after") == ids_c.index("out:t1") + 1
    assert len(ids_c) == len(set(ids_c))


def test_pointer_escape_and_root_set_success():
    ir_doc = _base_ir()
    ir_doc["steps"].insert(
        3,
        {
            "id": "out:c1",
            "op": "const",
            "inputs": [],
            "outputs": ["c1"],
            "params": {"name": "cfg", "value": {"a/b": {"x~y": 5}}},
        },
    )
    op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:c1", "path": "/value/a~1b/x~0y"},
        "params": {"value": 9},
    }
    result = apply_amendment(ir_doc, _req_with_op(op))
    assert result.status == "ok"
    const_step = next(step for step in result.ir_out["steps"] if step["id"] == "out:c1")
    assert const_step["params"]["value"]["a/b"]["x~y"] == 9

    root_op = {
        "op_id": "op2",
        "kind": "set_params",
        "selector": {"step_id": "out:t2", "path": "/"},
        "params": {"value": {"assignments": [{"target": "z", "expr": {"type": "lit", "value": 1}}]}},
    }
    result_root = apply_amendment(_base_ir(), _req_with_op(root_op))
    assert result_root.status == "ok"
    assert result_root.ir_out["steps"][2]["params"]["assignments"][0]["target"] == "z"


def test_structural_diff_transforms_minimal_truth():
    set_params_op = {
        "op_id": "op1",
        "kind": "set_params",
        "selector": {"step_id": "out:t2", "path": "/assignments/0/expr/value"},
        "params": {"value": 99},
    }
    result_changed = apply_amendment(_base_ir(), _req_with_op(set_params_op))
    assert result_changed.status == "ok"
    changed = result_changed.diff_structural["affected"]["transforms_changed"]
    assert changed
    assert any(item.get("step_id") == "out:t2" for item in changed)

    add_op = {
        "op_id": "op1",
        "kind": "add_step",
        "selector": {"index": 3},
        "params": {
            "step": {
                "id": "out:t3",
                "op": "identity",
                "inputs": ["t2"],
                "outputs": ["t3"],
                "params": {},
            }
        },
    }
    result_added = apply_amendment(_base_ir(), _req_with_op(add_op))
    assert result_added.status == "ok"
    assert result_added.diff_structural["affected"]["transforms_added"]

