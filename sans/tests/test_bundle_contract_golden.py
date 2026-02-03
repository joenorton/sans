"""
Golden / acceptance tests for bundle and report contract (v0.3).
Locks: paths canonical, report.json not in report, inputs/artifacts/outputs separation,
hashes required, report_schema_version, expanded in inputs.
"""
import hashlib
import json
import shutil
from pathlib import Path

import pytest

from sans.__main__ import main
from sans.runtime import run_script
from sans.hash_utils import compute_artifact_hash


def test_bundle_self_contained(tmp_path):
    """Run sans run with one binding and one save; move bundle; sans verify new path succeeds."""
    script = "data out; set in; c = a + b; run;"
    (tmp_path / "x.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    out_dir = tmp_path / "bundle1"
    out_dir.mkdir()
    ret = main(["run", str(tmp_path / "x.sas"), "--out", str(out_dir), "--tables", f"in={in_csv}"])
    assert ret == 0
    moved = tmp_path / "bundle2"
    shutil.copytree(out_dir, moved)
    ret = main(["verify", str(moved)])
    assert ret == 0


def test_report_paths_canonical(tmp_path):
    """Every path in inputs, artifacts, outputs uses forward slashes only; no leading slash."""
    script = "data out; set in; run;"
    (tmp_path / "s.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)
    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    bundle_root = out_dir.resolve()
    for inp in report.get("inputs", []):
        path = inp.get("path") or ""
        assert "\\" not in path, f"inputs path must not contain backslash: {path}"
        assert not path.startswith("/") or path.startswith("inputs/"), path
    for art in report.get("artifacts", []):
        path = art.get("path") or ""
        assert "\\" not in path, f"artifacts path must not contain backslash: {path}"
    for out in report.get("outputs", []):
        path = out.get("path") or ""
        assert "\\" not in path, f"outputs path must not contain backslash: {path}"
        assert not path.startswith("/") or path.startswith("outputs/"), path


def test_report_json_not_in_report(tmp_path):
    """No entry in inputs, artifacts, or outputs has path ending with report.json."""
    script = "data out; set in; run;"
    (tmp_path / "s.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    for inp in report.get("inputs", []):
        assert not (inp.get("path") or "").endswith("report.json")
    for art in report.get("artifacts", []):
        assert not (art.get("path") or "").endswith("report.json")
    for out in report.get("outputs", []):
        assert not (out.get("path") or "").endswith("report.json")


def test_inputs_artifacts_outputs_separation(tmp_path):
    """Paths in inputs start with inputs/; artifacts with artifacts/; outputs with outputs/. No path in more than one array."""
    script = "data out; set in; run;"
    (tmp_path / "s.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    input_paths = {i.get("path") for i in report.get("inputs", []) if i.get("path")}
    artifact_paths = {a.get("path") for a in report.get("artifacts", []) if a.get("path")}
    output_paths = {o.get("path") for o in report.get("outputs", []) if o.get("path")}
    assert input_paths & artifact_paths == set(), "no path in both inputs and artifacts"
    assert input_paths & output_paths == set(), "no path in both inputs and outputs"
    assert artifact_paths & output_paths == set(), "no path in both artifacts and outputs"
    for p in input_paths:
        assert p.startswith("inputs/"), f"input path must start with inputs/: {p}"
    for p in artifact_paths:
        assert p.startswith("artifacts/"), f"artifact path must start with artifacts/: {p}"
    for p in output_paths:
        assert p.startswith("outputs/"), f"output path must start with outputs/: {p}"


def test_outputs_have_hashes_and_table_facts(tmp_path):
    """For each entry in outputs, sha256 is non-null; rows and columns present; file exists and hash matches."""
    script = "data out; set in; run;"
    (tmp_path / "s.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    bundle_root = out_dir.resolve()
    for out in report.get("outputs", []):
        path = out.get("path")
        assert path, "output must have path"
        assert out.get("sha256"), f"output must have sha256: {out}"
        full_path = bundle_root / path
        assert full_path.exists(), f"output file must exist: {full_path}"
        actual_hash = compute_artifact_hash(full_path)
        assert actual_hash == out["sha256"], f"output hash mismatch for {path}"
        assert "rows" in out
        assert "columns" in out


def test_hashes_required(tmp_path):
    """Every entry in inputs, artifacts, outputs has non-null sha256."""
    script = "data out; set in; run;"
    (tmp_path / "s.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    for inp in report.get("inputs", []):
        assert inp.get("sha256"), f"input must have sha256: {inp}"
    for art in report.get("artifacts", []):
        assert art.get("sha256"), f"artifact must have sha256: {art}"
    for out in report.get("outputs", []):
        assert out.get("sha256"), f"output must have sha256: {out}"


def test_report_schema_version_and_expanded_in_inputs(tmp_path):
    """Report has report_schema_version (e.g. 0.3). expanded.sans appears in inputs[] with role=expanded, not in artifacts[]."""
    script = "data out; set in; run;"
    (tmp_path / "s.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    assert report.get("report_schema_version"), "report must have report_schema_version"
    expanded_inputs = [i for i in report.get("inputs", []) if i.get("role") == "expanded"]
    assert len(expanded_inputs) >= 1, "expanded.sans must appear in inputs with role=expanded"
    expanded_paths = [i.get("path") for i in expanded_inputs if "expanded.sans" in (i.get("path") or "")]
    assert expanded_paths, "at least one input path must contain expanded.sans"
    artifact_paths = [a.get("path") for a in report.get("artifacts", [])]
    assert not any("expanded.sans" in (p or "") for p in artifact_paths), "expanded.sans must not be in artifacts"


def test_runtime_has_no_outputs_array(tmp_path):
    """runtime object has status and timing only; no outputs array (use top-level outputs[])."""
    script = "data out; set in; run;"
    (tmp_path / "s.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n", encoding="utf-8")
    out_dir = tmp_path / "out"
    run_script(script, "s.sas", {"in": str(in_csv)}, out_dir)
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    runtime = report.get("runtime") or {}
    assert "outputs" not in runtime, "runtime must not have outputs array"


def test_source_input_hash_matches_bundle_bytes(tmp_path):
    fixture = Path(__file__).resolve().parent / "fixtures" / "analysis.sas"
    script_text = fixture.read_text(encoding="utf-8")
    normalized = script_text.replace("\r\n", "\n").replace("\r", "\n")
    crlf_bytes = normalized.replace("\n", "\r\n").encode("utf-8")

    script_path = tmp_path / "analysis.sas"
    script_path.write_bytes(crlf_bytes)
    out_dir = tmp_path / "out"

    ret = main(["check", str(script_path), "--out", str(out_dir), "--tables", "in"])
    assert ret == 0

    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    source_entries = [i for i in report.get("inputs", []) if i.get("role") == "source"]
    assert source_entries, "expected source input entry"
    source_entry = source_entries[0]

    bundle_path = out_dir / source_entry["path"]
    assert bundle_path.exists(), "materialized source file missing"
    assert bundle_path.read_bytes() == script_path.read_bytes(), "source bytes must be preserved in bundle"
    expected_hash = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    assert source_entry["sha256"] == expected_hash
