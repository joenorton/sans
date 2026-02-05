import json
from pathlib import Path

from sans.runtime import run_script


def _load_plan(out_dir: Path) -> dict:
    return json.loads((out_dir / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))


def _load_graph(out_dir: Path) -> dict:
    return json.loads((out_dir / "artifacts" / "graph.json").read_text(encoding="utf-8"))


def _find_changed_step(plan_a: dict, plan_b: dict) -> tuple[dict, dict]:
    steps_a = [s for s in plan_a.get("steps", []) if s.get("kind") == "op"]
    steps_b = [s for s in plan_b.get("steps", []) if s.get("kind") == "op"]
    assert len(steps_a) == len(steps_b)
    for s_a, s_b in zip(steps_a, steps_b):
        if s_a.get("transform_id") != s_b.get("transform_id"):
            return s_a, s_b
    raise AssertionError("Expected at least one step transform_id to differ.")


def _graph_step_node(graph: dict, step_id: str) -> dict:
    step_node_id = f"s:{step_id}"
    for node in graph.get("nodes", []):
        if node.get("id") == step_node_id:
            return node
    raise AssertionError(f"Missing graph node for step_id={step_id}")


def test_literal_change_keeps_transform_class_id(tmp_path: Path) -> None:
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1\n", encoding="utf-8")

    script_a = "data out; set in; x = 250; run;"
    script_b = "data out; set in; x = 300; run;"

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    run_script(script_a, "s1.sas", {"in": str(in_csv)}, out1, legacy_sas=True)
    run_script(script_b, "s2.sas", {"in": str(in_csv)}, out2, legacy_sas=True)

    plan_a = _load_plan(out1)
    plan_b = _load_plan(out2)
    step_a, step_b = _find_changed_step(plan_a, plan_b)

    assert step_a["transform_id"] != step_b["transform_id"]
    assert step_a["transform_class_id"] == step_b["transform_class_id"]

    graph_a = _load_graph(out1)
    graph_b = _load_graph(out2)
    node_a = _graph_step_node(graph_a, step_a["step_id"])
    node_b = _graph_step_node(graph_b, step_b["step_id"])

    assert node_a["transform_class_id"] == node_b["transform_class_id"]
    assert node_a["payload_sha256"] != node_b["payload_sha256"]
