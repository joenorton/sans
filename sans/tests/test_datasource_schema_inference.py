import json
import logging
from pathlib import Path

from sans.runtime import run_script


def _evidence(out_dir: Path) -> dict:
    path = out_dir / "artifacts" / "runtime.evidence.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _vars_graph(out_dir: Path) -> dict:
    path = out_dir / "artifacts" / "vars.graph.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _plan(out_dir: Path) -> dict:
    path = out_dir / "artifacts" / "plan.ir.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_csv_header_inference(tmp_path: Path) -> None:
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("A,B\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f"datasource lb = csv(\"{csv_path.as_posix()}\")\n"
        "table out = from(lb) select A, B\n"
        "save out to \"out.csv\"\n"
    )
    out_dir = tmp_path / "out"
    report = run_script(script, "test.sans", {}, out_dir)
    assert report["status"] == "ok"
    evidence = _evidence(out_dir)
    assert evidence["datasources"]["lb"]["columns"] == ["A", "B"]


def test_inline_csv_header_inference(tmp_path: Path) -> None:
    script = (
        "# sans 0.1\n"
        "datasource raw = inline_csv(\"a,b\\n1,2\\n\")\n"
        "table out = from(raw) select a, b\n"
        "save out to \"out.csv\"\n"
    )
    out_dir = tmp_path / "out"
    report = run_script(script, "test.sans", {}, out_dir)
    assert report["status"] == "ok"
    evidence = _evidence(out_dir)
    assert evidence["datasources"]["raw"]["columns"] == ["a", "b"]


def test_pinned_columns_validation(tmp_path: Path) -> None:
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("A,B\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f"datasource lb = csv(\"{csv_path.as_posix()}\", columns(A,C))\n"
        "table out = from(lb) select A, C\n"
        "save out to \"out.csv\"\n"
    )
    out_dir = tmp_path / "out"
    report = run_script(script, "test.sans", {}, out_dir)
    assert report["status"] == "failed"
    primary = report.get("primary_error") or {}
    assert primary.get("code") == "SANS_RUNTIME_DATASOURCE_SCHEMA_MISMATCH"
    assert "Pinned" in primary.get("message", "")
    assert "Header" in primary.get("message", "")


def test_vars_graph_uses_inferred_schema(tmp_path: Path, caplog) -> None:
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("A,B\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f"datasource lb = csv(\"{csv_path.as_posix()}\")\n"
        "table out = from(lb) do\n"
        "  filter A > 1\n"
        "  rename(A -> A1)\n"
        "  select A1, B\n"
        "end\n"
        "save out to \"out.csv\"\n"
    )
    out_dir = tmp_path / "out"
    with caplog.at_level(logging.WARNING):
        report = run_script(script, "test.sans", {}, out_dir)
    assert report["status"] == "ok"
    assert not any("schema unknown" in r.message for r in caplog.records)

    graph = _vars_graph(out_dir)
    edges = {(e["src"], e["dst"]) for e in graph.get("edges", [])}
    plan = _plan(out_dir)
    filter_step = next(s for s in plan["steps"] if s.get("op") == "filter")
    rename_step = next(s for s in plan["steps"] if s.get("op") == "rename")
    assert (f"v:{filter_step['inputs'][0]}.A", f"v:{filter_step['outputs'][0]}.A") in edges
    assert (f"v:{rename_step['inputs'][0]}.A", f"v:{rename_step['outputs'][0]}.A1") in edges
