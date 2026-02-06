"""Tests for schema lock v0: boundary typing, lock generation, enforcement, determinism, report binding."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from sans.runtime import run_script
from sans.schema_lock import (
    build_schema_lock,
    compute_lock_sha256,
    load_schema_lock,
    lock_by_name,
    lock_entry_required_columns,
    write_schema_lock,
)


def _run(script: str, out_dir: Path, schema_lock_path: Optional[Path] = None, emit_schema_lock_path: Optional[Path] = None):
    return run_script(
        text=script,
        file_name="script.sans",
        bindings={},
        out_dir=out_dir,
        strict=True,
        schema_lock_path=schema_lock_path,
        emit_schema_lock_path=emit_schema_lock_path,
    )


# 1) Required boundary typing: script with datasource csv("x.csv") no typed columns and no schema-lock -> fails with E_SCHEMA_REQUIRED
def test_required_boundary_no_lock_fails(tmp_path: Path):
    csv_path = tmp_path / "x.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource x = csv("{csv_path.as_posix()}")\n'
        "table t = from(x) select a\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(script, tmp_path / "out")
    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "E_SCHEMA_REQUIRED"


# 2) Lock generation: run with --emit-schema-lock produces file; assert deterministic structure, version, and datasource/columns contents
def test_lock_generation(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}", columns(USUBJID:string, VISITNUM:int))\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "schema.lock.json"
    report = _run(script, tmp_path / "out", emit_schema_lock_path=lock_path)
    assert report["status"] == "ok"
    assert lock_path.exists()
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    assert lock.get("schema_lock_version") == 1
    assert "created_by" in lock
    assert "datasources" in lock
    ds_list = lock["datasources"]
    assert len(ds_list) == 1
    ds = ds_list[0]
    assert ds["name"] == "lb"
    assert ds["kind"] == "csv"
    assert "path" in ds
    cols = ds["columns"]
    assert len(cols) >= 2
    names = [c["name"] for c in cols]
    assert "USUBJID" in names
    assert "VISITNUM" in names
    assert ds.get("rules", {}).get("extra_columns") == "ignore"
    assert ds.get("rules", {}).get("missing_columns") == "error"


# 3) Lock enforcement success: run with --schema-lock succeeds on input with same columns + extra columns; extra columns ignored
def test_lock_enforcement_success_extra_columns_ignored(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b,extra\n1,2,x\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}")\n'
        "table t = from(ds) select a, b\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{
            "name": "ds",
            "kind": "csv",
            "path": "data.csv",
            "columns": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}],
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        }],
    }, indent=2), encoding="utf-8")
    report = _run(script, tmp_path / "out", schema_lock_path=lock_path)
    assert report["status"] == "ok"
    out_csv = tmp_path / "out" / "outputs" / "out.csv"
    assert out_csv.exists()
    content = out_csv.read_text(encoding="utf-8")
    assert "a,b" in content or "a," in content
    assert "1,2" in content


# 4) Missing column failure: input missing a locked column -> fails with E_SCHEMA_MISSING_COL and evidence mentions column name
def test_missing_column_fails(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a\n1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}")\n'
        "table t = from(ds) select a\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{
            "name": "ds",
            "kind": "csv",
            "path": "data.csv",
            "columns": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}],
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        }],
    }, indent=2), encoding="utf-8")
    report = _run(script, tmp_path / "out", schema_lock_path=lock_path)
    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "E_SCHEMA_MISSING_COL"
    assert "b" in report["primary_error"]["message"]


# 5) Type mismatch failure: locked int column contains "abc" -> fails with E_CSV_COERCE and coercion_diagnostics include expected_type and sample_raw_values
def test_type_mismatch_fails_with_coercion_diagnostics(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a\nabc\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}")\n'
        "table t = from(ds) select a\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{
            "name": "ds",
            "kind": "csv",
            "path": "data.csv",
            "columns": [{"name": "a", "type": "int"}],
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        }],
    }, indent=2), encoding="utf-8")
    report = _run(script, tmp_path / "out", schema_lock_path=lock_path)
    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "E_CSV_COERCE"
    evidence_path = tmp_path / "out" / "artifacts" / "runtime.evidence.json"
    assert evidence_path.exists()
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert "coercion_diagnostics" in evidence
    diag = evidence["coercion_diagnostics"][0]
    assert diag["datasource"] == "ds"
    cols = diag.get("columns", [])
    assert len(cols) == 1
    assert cols[0]["expected_type"] == "int"
    assert "abc" in cols[0].get("sample_raw_values", [])


# 6) Determinism: generating lock twice from same run yields identical JSON (byte-identical after canonical dump)
def test_lock_determinism(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}", columns(USUBJID:string, VISITNUM:int))\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    lock1_path = tmp_path / "lock1.json"
    lock2_path = tmp_path / "lock2.json"
    _run(script, tmp_path / "out1", emit_schema_lock_path=lock1_path)
    _run(script, tmp_path / "out2", emit_schema_lock_path=lock2_path)
    b1 = lock1_path.read_bytes()
    b2 = lock2_path.read_bytes()
    assert b1 == b2, "Lock files must be byte-identical for same run"


# 7) Report binding: report contains schema_lock_sha256 and verify recomputes/compares (if verify flag added)
def test_report_schema_lock_sha256(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}", columns(USUBJID:string, VISITNUM:int))\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "schema.lock.json"
    report = _run(script, tmp_path / "out", emit_schema_lock_path=lock_path)
    assert report["status"] == "ok"
    assert "schema_lock_sha256" in report
    lock_dict = load_schema_lock(lock_path)
    expected_sha = compute_lock_sha256(lock_dict)
    assert report["schema_lock_sha256"] == expected_sha


# Typed pinning without lock is acceptable
def test_typed_pinning_without_lock_succeeds(tmp_path: Path):
    csv_path = tmp_path / "x.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource x = csv("{csv_path.as_posix()}", columns(a:int, b:int))\n'
        "table t = from(x) select a, b\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(script, tmp_path / "out")
    assert report["status"] == "ok"


# schema_lock module: lock_by_name, lock_entry_required_columns
def test_schema_lock_helpers():
    lock = {
        "schema_lock_version": 1,
        "datasources": [
            {"name": "ds", "columns": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]},
        ],
    }
    by_name = lock_by_name(lock)
    assert "ds" in by_name
    entry = by_name["ds"]
    req = lock_entry_required_columns(entry)
    assert req == ["a", "b"]
