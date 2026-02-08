"""
Tests for thin bundle mode: verify passes for full, thin, and legacy bundles;
thin bundles omit datasource bytes but record fingerprints; negative and determinism tests.
"""
import json
from pathlib import Path

import pytest

from sans.__main__ import main
from sans.runtime import run_script


def _minimal_script_and_data(tmp_path):
    """Write minimal .sas script and CSV; return (script_path, in_csv_path)."""
    script = "data out; set in; run;"
    (tmp_path / "x.sas").write_text(script, encoding="utf-8")
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    return tmp_path / "x.sas", in_csv


def test_verify_full_bundle(tmp_path):
    """sans run --bundle-mode full then sans verify passes."""
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "full"
    out_dir.mkdir()
    ret = main([
        "run", str(script_path), "--out", str(out_dir),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "full",
    ])
    assert ret == 0
    ret = main(["verify", str(out_dir)])
    assert ret == 0


def test_verify_thin_bundle(tmp_path):
    """sans run --bundle-mode thin then sans verify passes."""
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "thin"
    out_dir.mkdir()
    ret = main([
        "run", str(script_path), "--out", str(out_dir),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "thin",
    ])
    assert ret == 0
    ret = main(["verify", str(out_dir)])
    assert ret == 0


def test_verify_legacy_bundle(tmp_path):
    """Legacy bundle (no bundle_mode) treated as full; verify passes."""
    from sans.hash_utils import compute_report_sha256
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "legacy"
    out_dir.mkdir()
    run_script(
        script_path.read_text(encoding="utf-8"),
        "x.sas",
        {"in": str(in_csv)},
        out_dir,
        legacy_sas=True,
        bundle_mode="full",
    )
    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report.pop("bundle_mode", None)
    report.pop("bundle_format_version", None)
    report["report_sha256"] = compute_report_sha256(report, out_dir)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    ret = main(["verify", str(out_dir)])
    assert ret == 0


def test_thin_bundle_has_no_datasource_files(tmp_path):
    """Thin bundle must not contain datasource files under inputs/data/."""
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "thin"
    out_dir.mkdir()
    ret = main([
        "run", str(script_path), "--out", str(out_dir),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "thin",
    ])
    assert ret == 0
    data_dir = out_dir / "inputs" / "data"
    if data_dir.exists():
        files = list(data_dir.iterdir())
        assert not [f for f in files if f.suffix.lower() in (".csv", ".xpt")], (
            "thin bundle must not contain datasource files in inputs/data/"
        )


def test_thin_bundle_has_fingerprints(tmp_path):
    """Thin bundle datasource_inputs have embedded=false, sha256, size_bytes."""
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "thin"
    out_dir.mkdir()
    ret = main([
        "run", str(script_path), "--out", str(out_dir),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "thin",
    ])
    assert ret == 0
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    ds_inputs = report.get("datasource_inputs") or []
    assert ds_inputs, "expected at least one datasource_inputs entry"
    for inp in ds_inputs:
        assert inp.get("embedded") is False, "thin datasource must have embedded=false"
        assert inp.get("sha256"), "thin datasource must have sha256"
        assert inp.get("size_bytes") is not None, "thin datasource must have size_bytes"


def test_full_bundle_has_embedded_datasources(tmp_path):
    """Full bundle datasource_inputs have embedded=true and files exist."""
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "full"
    out_dir.mkdir()
    ret = main([
        "run", str(script_path), "--out", str(out_dir),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "full",
    ])
    assert ret == 0
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    ds_inputs = report.get("datasource_inputs") or []
    assert ds_inputs, "expected at least one datasource_inputs entry"
    for inp in ds_inputs:
        assert inp.get("embedded") is True, "full datasource must have embedded=true"
        path_str = inp.get("path")
        assert path_str, "full datasource must have path"
        full_path = out_dir / path_str.replace("\\", "/")
        assert full_path.exists(), f"full bundle must contain datasource file: {path_str}"
    data_dir = out_dir / "inputs" / "data"
    assert data_dir.exists()
    assert list(data_dir.iterdir()), "full bundle inputs/data/ must contain files"


def test_verify_fails_thin_missing_sha256(tmp_path):
    """Thin bundle with datasource_inputs entry missing sha256 fails verify with explicit message."""
    from sans.hash_utils import compute_report_sha256
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "thin"
    out_dir.mkdir()
    ret = main([
        "run", str(script_path), "--out", str(out_dir),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "thin",
    ])
    assert ret == 0
    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    for inp in report.get("datasource_inputs", []):
        inp["sha256"] = ""
        break
    report["report_sha256"] = compute_report_sha256(report, out_dir)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    ret = main(["verify", str(out_dir)])
    assert ret != 0


def test_verify_fails_thin_missing_size_bytes(tmp_path):
    """Thin bundle with datasource_inputs entry missing size_bytes fails verify with explicit message."""
    from sans.hash_utils import compute_report_sha256
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out_dir = tmp_path / "thin"
    out_dir.mkdir()
    ret = main([
        "run", str(script_path), "--out", str(out_dir),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "thin",
    ])
    assert ret == 0
    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    for inp in report.get("datasource_inputs", []):
        inp.pop("size_bytes", None)
        break
    report["report_sha256"] = compute_report_sha256(report, out_dir)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    ret = main(["verify", str(out_dir)])
    assert ret != 0


def test_thin_bundle_determinism(tmp_path):
    """Thin runs with same script and data produce deterministic fingerprint data."""
    script_path, in_csv = _minimal_script_and_data(tmp_path)
    out1 = tmp_path / "thin1"
    out2 = tmp_path / "thin2"
    out1.mkdir()
    out2.mkdir()
    ret1 = main([
        "run", str(script_path), "--out", str(out1),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "thin",
    ])
    ret2 = main([
        "run", str(script_path), "--out", str(out2),
        "--tables", f"in={in_csv}", "--legacy-sas", "--bundle-mode", "thin",
    ])
    assert ret1 == 0 and ret2 == 0
    r1 = json.loads((out1 / "report.json").read_text(encoding="utf-8"))
    r2 = json.loads((out2 / "report.json").read_text(encoding="utf-8"))
    ds1 = {i["datasource"]: (i.get("sha256"), i.get("size_bytes"), i.get("embedded")) for i in r1.get("datasource_inputs", [])}
    ds2 = {i["datasource"]: (i.get("sha256"), i.get("size_bytes"), i.get("embedded")) for i in r2.get("datasource_inputs", [])}
    assert ds1 == ds2, "thin runs with same inputs must produce same datasource fingerprints"
