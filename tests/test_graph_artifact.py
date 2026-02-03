import json
from pathlib import Path

from sans.runtime import run_script


def _graph_bytes(out_dir: Path) -> bytes:
    return (out_dir / "artifacts" / "graph.json").read_bytes()


def _graph_json(out_dir: Path) -> dict:
    return json.loads((out_dir / "artifacts" / "graph.json").read_text(encoding="utf-8"))


def _graph_sha(report: dict) -> str:
    for art in report.get("artifacts", []):
        if art.get("path") == "artifacts/graph.json" or art.get("name") == "graph.json":
            return art.get("sha256") or ""
    return ""


def test_graph_determinism_same_script(tmp_path: Path) -> None:
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1\n", encoding="utf-8")
    script = "data out; set in; run;"

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    r1 = run_script(script, "s.sas", {"in": str(in_csv)}, out1)
    r2 = run_script(script, "s.sas", {"in": str(in_csv)}, out2)

    assert _graph_bytes(out1) == _graph_bytes(out2)
    assert _graph_sha(r1) == _graph_sha(r2)


def test_graph_ignores_whitespace_and_comments(tmp_path: Path) -> None:
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1\n", encoding="utf-8")
    script_a = "data out; set in; run;"
    script_b = "\n".join(
        [
            "/* leading comment */",
            "data   out;",
            "  set in;",
            "  /* inline comment */",
            "run;",
        ]
    )

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    run_script(script_a, "s.sas", {"in": str(in_csv)}, out1)
    run_script(script_b, "s.sas", {"in": str(in_csv)}, out2)

    assert _graph_bytes(out1) == _graph_bytes(out2)


def test_graph_changes_on_topology_change(tmp_path: Path) -> None:
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1\n", encoding="utf-8")
    script_a = "data out; set in; run;"
    script_b = "\n".join(
        [
            "data mid; set in; run;",
            "data out; set mid; run;",
        ]
    )

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    run_script(script_a, "s.sas", {"in": str(in_csv)}, out1)
    run_script(script_b, "s.sas", {"in": str(in_csv)}, out2)

    assert _graph_bytes(out1) != _graph_bytes(out2)


def test_graph_edges_reference_valid_nodes(tmp_path: Path) -> None:
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1\n", encoding="utf-8")
    script = "\n".join(
        [
            "data mid; set in; run;",
            "data out; set mid; run;",
        ]
    )
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)

    graph = _graph_json(out_dir)
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])
    node_ids = {n["id"] for n in nodes}
    for edge in edges:
        assert edge["src"] in node_ids
        assert edge["dst"] in node_ids

    # Node collections
    steps = {n["id"]: n for n in nodes if n.get("kind") == "step"}
    tables = {n["id"]: n for n in nodes if n.get("kind") == "table"}

    produces_edges = [e for e in edges if e.get("kind") == "produces"]
    consumes_edges = [e for e in edges if e.get("kind") == "consumes"]

    # Edge types are bipartite only
    for e in produces_edges:
        assert e["src"] in steps
        assert e["dst"] in tables
    for e in consumes_edges:
        assert e["src"] in tables
        assert e["dst"] in steps

    # Table nodes: producers/consumers match edges
    producer_map: dict[str, list[str]] = {}
    consumer_map: dict[str, list[str]] = {}
    for e in produces_edges:
        producer_map.setdefault(e["dst"], []).append(e["src"])
    for e in consumes_edges:
        consumer_map.setdefault(e["src"], []).append(e["dst"])

    for table_id, node in tables.items():
        producers = producer_map.get(table_id, [])
        assert len(producers) <= 1
        if node.get("producer") is None:
            assert producers == []
        else:
            assert producers == [node.get("producer")]
        assert node.get("consumers") == sorted(consumer_map.get(table_id, []))
        assert node.get("consumers") == sorted(node.get("consumers", []))

    # Step nodes: inputs/outputs agree with edges
    for step_id, node in steps.items():
        inputs = node.get("inputs") or []
        outputs = node.get("outputs") or []
        assert inputs == sorted(inputs)
        assert outputs == sorted(outputs)
        edge_inputs = sorted([e["src"] for e in consumes_edges if e["dst"] == step_id])
        edge_outputs = sorted([e["dst"] for e in produces_edges if e["src"] == step_id])
        assert inputs == edge_inputs
        assert outputs == edge_outputs

    # Edge ordering invariant
    assert edges == sorted(edges, key=lambda e: (e["src"], e["dst"], e["kind"]))
