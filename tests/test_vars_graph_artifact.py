import json
import logging
from pathlib import Path

from sans.runtime import run_script
from sans.lineage import (
    canonical_json_bytes,
    collect_expr_cols,
    compute_expr_sha256,
    compute_order_sha256,
    compute_params_sha256,
    compute_sha256_hex,
)


def _vars_graph(out_dir: Path) -> dict:
    return json.loads((out_dir / "artifacts" / "vars.graph.json").read_text(encoding="utf-8"))


def _table_effects(out_dir: Path) -> dict:
    return json.loads((out_dir / "artifacts" / "table.effects.json").read_text(encoding="utf-8"))


def test_vars_graph_basic(tmp_path: Path) -> None:
    script = "\n".join(
        [
            "# sans 0.1",
            "datasource in = inline_csv columns(a:int, b:int, c:int) do",
            "a,b,c",
            "1,2,3",
            "2,3,4",
            "end",
            "",
            "table t1 = from(in) derive(x = a + b)",
            "table t2 = t1 filter a > 1",
            "table t3 = t2 select a, x",
            "table out = sort(t3).by(a)",
            "save out to \"out.csv\"",
        ]
    )
    out_dir = tmp_path / "out"
    run_script(script, "test.sans", {}, out_dir, legacy_sas=True)

    graph = _vars_graph(out_dir)
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    assert nodes == sorted(nodes, key=lambda n: n["id"])
    assert edges == sorted(edges, key=lambda e: (e["src"], e["dst"], e["kind"]))

    expected_node_ids = {
        "v:__datasource__in.a",
        "v:__datasource__in.b",
        "v:__datasource__in.c",
        "v:__t2__.a",
        "v:__t2__.b",
        "v:__t2__.c",
        "v:t1.a",
        "v:t1.b",
        "v:t1.c",
        "v:t1.x",
        "v:t2.a",
        "v:t2.b",
        "v:t2.c",
        "v:t2.x",
        "v:t3.a",
        "v:t3.x",
        "v:out.a",
        "v:out.x",
    }
    assert {n["id"] for n in nodes} == expected_node_ids

    expected_edges = {
        ("v:__datasource__in.a", "v:__t2__.a"),
        ("v:__datasource__in.b", "v:__t2__.b"),
        ("v:__datasource__in.c", "v:__t2__.c"),
        ("v:__t2__.a", "v:t1.a"),
        ("v:__t2__.b", "v:t1.b"),
        ("v:__t2__.c", "v:t1.c"),
        ("v:__t2__.a", "v:t1.x"),
        ("v:__t2__.b", "v:t1.x"),
        ("v:t1.a", "v:t2.a"),
        ("v:t1.b", "v:t2.b"),
        ("v:t1.c", "v:t2.c"),
        ("v:t1.x", "v:t2.x"),
        ("v:t2.a", "v:t3.a"),
        ("v:t2.x", "v:t3.x"),
        ("v:t3.a", "v:out.a"),
        ("v:t3.x", "v:out.x"),
    }
    assert {(e["src"], e["dst"]) for e in edges} == expected_edges

    plan = json.loads((out_dir / "artifacts" / "plan.ir.json").read_text(encoding="utf-8"))
    compute_step = next(s for s in plan["steps"] if s.get("op") == "compute")
    expected_payload = compute_params_sha256(compute_step.get("params"))
    expected_expr = compute_expr_sha256(compute_step["params"]["assignments"][0]["expr"])

    compute_nodes = [n for n in nodes if n.get("producing_step_id") == compute_step["step_id"]]
    assert compute_nodes
    for node in compute_nodes:
        assert node["payload_sha256"] == expected_payload

    node_x = next(n for n in nodes if n["id"] == "v:t1.x")
    assert node_x["origin"] == "derived"
    assert node_x["expr_sha256"] == expected_expr

    node_a = next(n for n in nodes if n["id"] == "v:t1.a")
    assert node_a["origin"] == "pass_through"
    assert node_a["producing_step_id"] is None
    assert node_a["transform_id"] is None
    assert node_a["payload_sha256"] is None

    node_src = next(n for n in nodes if n["id"] == "v:__datasource__in.a")
    assert node_src["origin"] == "source"

    effects = _table_effects(out_dir)
    effect_list = effects.get("effects", [])
    assert effects.get("schema_version") == 1
    assert effects.get("stats", {}).get("event_count") == len(effect_list)
    assert effect_list == sorted(effect_list, key=lambda e: (e["out_table"], e["kind"], e["producing_step_id"]))

    filter_step = next(s for s in plan["steps"] if s.get("op") == "filter")
    sort_step = next(s for s in plan["steps"] if s.get("op") == "sort")

    expected_filter = {
        "kind": "filter",
        "in_table": filter_step["inputs"][0],
        "out_table": filter_step["outputs"][0],
        "producing_step_id": filter_step["step_id"],
        "transform_id": filter_step["transform_id"],
        "payload_sha256": compute_params_sha256(filter_step["params"]),
        "predicate_sha256": compute_expr_sha256(filter_step["params"]["predicate"]),
        "predicate_cols": sorted(collect_expr_cols(filter_step["params"]["predicate"])),
    }
    by_entry = sort_step["params"]["by"][0]
    normalized_by = [{"col": by_entry["col"], "asc": not bool(by_entry.get("desc"))}]
    expected_sort = {
        "kind": "sort",
        "in_table": sort_step["inputs"][0],
        "out_table": sort_step["outputs"][0],
        "producing_step_id": sort_step["step_id"],
        "transform_id": sort_step["transform_id"],
        "payload_sha256": compute_params_sha256(sort_step["params"]),
        "order_sha256": compute_order_sha256(normalized_by),
        "by": normalized_by,
        "by_cols": [entry["col"] for entry in normalized_by],
    }
    actual_by_kind = {e["kind"]: e for e in effect_list}
    assert actual_by_kind["filter"] == expected_filter
    assert actual_by_kind["sort"] == expected_sort

    effects_no_hash = dict(effects)
    effects_no_hash.pop("sha256", None)
    assert effects.get("sha256") == compute_sha256_hex(canonical_json_bytes(effects_no_hash))


def test_vars_graph_ignores_loc(tmp_path: Path) -> None:
    script_a = "\n".join(
        [
            "# sans 0.1",
            "datasource in = inline_csv columns(a:int, b:int) do",
            "a,b",
            "1,2",
            "end",
            "table out = from(in) derive(x = a + b)",
            "save out to \"out.csv\"",
        ]
    )
    script_b = "\n".join(
        [
            "",
            "# sans 0.1",
            "datasource   in   =   inline_csv   columns(a:int, b:int)   do",
            "a,b",
            "1,2",
            "end",
            "",
            "table   out =   from(in)   derive( x = a + b )",
            "save out to \"out.csv\"",
        ]
    )

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    run_script(script_a, "a.sans", {}, out1, legacy_sas=True)
    run_script(script_b, "b.sans", {}, out2, legacy_sas=True)

    assert (out1 / "artifacts" / "vars.graph.json").read_bytes() == (
        out2 / "artifacts" / "vars.graph.json"
    ).read_bytes()
    assert (out1 / "artifacts" / "table.effects.json").read_bytes() == (
        out2 / "artifacts" / "table.effects.json"
    ).read_bytes()


def test_vars_graph_unknown_schema_skips_pass_through(tmp_path: Path, caplog) -> None:
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1\n", encoding="utf-8")
    script = "data out; set in; x = a + 1; if a > 1; run;"

    out_dir = tmp_path / "out"
    with caplog.at_level(logging.WARNING):
        run_script(script, "s.sas", {"in": str(in_csv)}, out_dir, legacy_sas=True)

    graph = _vars_graph(out_dir)
    edges = {(e["src"], e["dst"]) for e in graph.get("edges", [])}
    # Unknown schema: no pass-through edges (same-column) should be emitted.
    assert ("v:in.a", "v:out__1.a") not in edges
    assert ("v:out__1.a", "v:out.a") not in edges
    assert any("schema unknown" in r.message for r in caplog.records)


def test_filter_pass_through_edges_explicit(tmp_path: Path) -> None:
    script = "\n".join(
        [
            "# sans 0.1",
            "datasource in = inline_csv columns(a:int, b:int) do",
            "a,b",
            "1,2",
            "end",
            "table out = from(in) filter a > 1",
            "save out to \"out.csv\"",
        ]
    )
    out_dir = tmp_path / "out"
    run_script(script, "test.sans", {}, out_dir, legacy_sas=True)

    graph = _vars_graph(out_dir)
    edges = {(e["src"], e["dst"]) for e in graph.get("edges", [])}
    for col in ["a", "b"]:
        assert (f"v:__datasource__in.{col}", f"v:__t2__.{col}") in edges
        assert (f"v:__t2__.{col}", f"v:out.{col}") in edges


def test_filter_pass_through_edges_after_select_infers_schema(tmp_path: Path) -> None:
    script = "\n".join(
        [
            "# sans 0.1",
            "datasource in = inline_csv columns(a:int, b:int) do",
            "a,b",
            "1,2",
            "end",
            "table filtered = from(in) filter a > 1",
            "table out = filtered select a, b",
            "save out to \"out.csv\"",
        ]
    )
    out_dir = tmp_path / "out"
    run_script(script, "test.sans", {}, out_dir, legacy_sas=True)

    graph = _vars_graph(out_dir)
    edges = {(e["src"], e["dst"]) for e in graph.get("edges", [])}
    for col in ["a", "b"]:
        assert (f"v:__datasource__in.{col}", f"v:__t2__.{col}") in edges
        assert (f"v:__t2__.{col}", f"v:filtered.{col}") in edges
        assert (f"v:filtered.{col}", f"v:out.{col}") in edges


def test_filter_predicate_change_affects_effects_not_vars_graph(tmp_path: Path) -> None:
    script_a = "\n".join(
        [
            "# sans 0.1",
            "datasource in = inline_csv columns(a:int, b:int) do",
            "a,b",
            "1,2",
            "end",
            "table out = from(in) filter a > 1",
            "save out to \"out.csv\"",
        ]
    )
    script_b = script_a.replace("a > 1", "a > 2")

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    run_script(script_a, "a.sans", {}, out1, legacy_sas=True)
    run_script(script_b, "b.sans", {}, out2, legacy_sas=True)

    assert (out1 / "artifacts" / "vars.graph.json").read_bytes() == (
        out2 / "artifacts" / "vars.graph.json"
    ).read_bytes()
    assert (out1 / "artifacts" / "table.effects.json").read_bytes() != (
        out2 / "artifacts" / "table.effects.json"
    ).read_bytes()


def test_sort_order_change_updates_effects(tmp_path: Path) -> None:
    script_a = "\n".join(
        [
            "# sans 0.1",
            "datasource in = inline_csv columns(a:int, b:int) do",
            "a,b",
            "1,2",
            "end",
            "table t = from(in)",
            "table out = sort(t).by(a)",
            "save out to \"out.csv\"",
        ]
    )
    script_b = script_a.replace("by(a)", "by(-a)")

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    run_script(script_a, "a.sans", {}, out1, legacy_sas=True)
    run_script(script_b, "b.sans", {}, out2, legacy_sas=True)

    assert (out1 / "artifacts" / "table.effects.json").read_bytes() != (
        out2 / "artifacts" / "table.effects.json"
    ).read_bytes()


def test_vars_graph_chain_explicit(tmp_path: Path) -> None:
    script = "\n".join(
        [
            "# sans 0.1",
            "datasource in = inline_csv columns(label:string, name:string, value:int) do",
            "label,name,value",
            "A,foo,1",
            "B,bar,2",
            "end",
            "table high_value__1 = from(in) derive(flag = value > 1)",
            "table high_value__2 = high_value__1 filter value > 1",
            "table high_value = high_value__2 select label, name, value",
            "table sorted_high = sort(high_value).by(label, name)",
            "save sorted_high to \"out.csv\"",
        ]
    )
    out_dir = tmp_path / "out"
    run_script(script, "test.sans", {}, out_dir, legacy_sas=True)

    graph = _vars_graph(out_dir)
    edges = {(e["src"], e["dst"]) for e in graph.get("edges", [])}
    cols = ["label", "name", "value"]
    for col in cols:
        assert (f"v:high_value__1.{col}", f"v:high_value__2.{col}") in edges
        assert (f"v:high_value__2.{col}", f"v:high_value.{col}") in edges
        assert (f"v:high_value.{col}", f"v:sorted_high.{col}") in edges
