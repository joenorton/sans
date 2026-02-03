import json
import hashlib
import pytest
from pathlib import Path
from sans.runtime import run_script
from sans.sans_script.canon import (
    compute_transform_id,
    compute_transform_class_id,
    compute_step_id,
    _canonicalize,
)

def test_contract_identities(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2", encoding="utf-8")
    script = "data out; set in; c = a + b; run;"
    out_dir = tmp_path / "out"
    
    run_script(script, "test.sas", {"in": str(in_csv)}, out_dir)
    
    plan_path = out_dir / "artifacts" / "plan.ir.json"
    registry_path = out_dir / "artifacts" / "registry.candidate.json"
    evidence_path = out_dir / "artifacts" / "runtime.evidence.json"
    
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    
    # Check plan steps have both IDs
    for step in plan["steps"]:
        if step["kind"] == "op":
            assert "transform_id" in step
            assert "transform_class_id" in step
            assert "step_id" in step
            
            # Invariant: recomputing transform_id from {op, params} must match emitted value
            expected_t_id = compute_transform_id(step["op"], step["params"])
            assert step["transform_id"] == expected_t_id

            # Invariant: recomputing transform_class_id from {op, param_shape} must match emitted value
            expected_tc_id = compute_transform_class_id(step["op"], step["params"])
            assert step["transform_class_id"] == expected_tc_id
            
            # Invariant: recomputing step_id from {transform_id, inputs, outputs} must match emitted value
            expected_s_id = compute_step_id(step["transform_id"], step["inputs"], step["outputs"])
            assert step["step_id"] == expected_s_id

    # Invariant: registry.index[i] == plan.steps[i].transform_id
    for i, step in enumerate(plan["steps"]):
        if step["kind"] == "op":
            assert registry["index"][str(i)] == step["transform_id"]

    # Invariant: transform entry contains spec = {op, params}
    for transform in registry["transforms"]:
        t_id = transform["transform_id"]
        # Find step in plan with this transform_id
        step = next(s for s in plan["steps"] if s.get("transform_id") == t_id)
        assert transform["spec"]["op"] == step["op"]
        assert transform["spec"]["params"] == _canonicalize(step["params"])

    # Check evidence
    for step_ev in evidence["step_evidence"]:
        assert "step_id" in step_ev
        assert "transform_id" in step_ev
        # Match with plan
        idx = step_ev["step_index"]
        plan_step = plan["steps"][idx]
        assert step_ev["step_id"] == plan_step["step_id"]
        assert step_ev["transform_id"] == plan_step["transform_id"]

def test_identity_stability(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2", encoding="utf-8")
    script = "data out; set in; c = a + b; run;"
    
    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    
    run_script(script, "test.sas", {"in": str(in_csv)}, out1)
    run_script(script, "test.sas", {"in": str(in_csv)}, out2)
    
    p1 = json.loads((out1 / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))
    p2 = json.loads((out2 / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))
    
    assert p1["steps"][0]["transform_id"] == p2["steps"][0]["transform_id"]
    assert p1["steps"][0]["step_id"] == p2["steps"][0]["step_id"]

def test_wiring_vs_transform_id(tmp_path):
    in1 = tmp_path / "in1.csv"
    in2 = tmp_path / "in2.csv"
    in1.write_text("a\n1", encoding="utf-8")
    in2.write_text("a\n2", encoding="utf-8")
    
    # Same transform (identity), different wiring (different input table names)
    script1 = "data out1; set in1; run;"
    script2 = "data out2; set in2; run;"
    
    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    
    run_script(script1, "s1.sas", {"in1": str(in1)}, out1)
    run_script(script2, "s2.sas", {"in2": str(in2)}, out2)
    
    p1 = json.loads((out1 / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))
    p2 = json.loads((out2 / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))
    
    # Transform ID should be SAME (both are 'data_step' with same params)
    assert p1["steps"][0]["transform_id"] == p2["steps"][0]["transform_id"]
    
    # Step ID should be DIFFERENT (different inputs/outputs)
    assert p1["steps"][0]["step_id"] != p2["steps"][0]["step_id"]

def test_params_change_transform_id(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1", encoding="utf-8")
    
    script1 = "data out; set in; x = 1; run;"
    script2 = "data out; set in; x = 2; run;"
    
    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    
    run_script(script1, "s1.sas", {"in": str(in_csv)}, out1)
    run_script(script2, "s2.sas", {"in": str(in_csv)}, out2)
    
    p1 = json.loads((out1 / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))
    p2 = json.loads((out2 / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))
    
    # Transform ID should be DIFFERENT
    assert p1["steps"][0]["transform_id"] != p2["steps"][0]["transform_id"]

def test_path_normalization(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1", encoding="utf-8")
    # Force a backslash if possible (though on Linux it won't matter, on Windows it will)
    in_path_str = str(in_csv)
    
    script = "data out; set in; run;"
    out_dir = tmp_path / "out"
    run_script(script, "test.sas", {"in": in_path_str}, out_dir)
    
    evidence = json.loads((out_dir / "artifacts" / "runtime.evidence.json").read_text(encoding="utf-8"))
    
    # All paths in evidence must use forward slashes
    for inp in evidence["inputs"]:
        assert "\\" not in inp["path"]
    for outp in evidence["outputs"]:
        assert "\\" not in outp["path"]
    for name, path in evidence["bindings"].items():
        assert "\\" not in path
    
    assert "\\" not in evidence["plan_ir"]["path"]
