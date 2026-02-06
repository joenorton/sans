"""Tests for schema lock v0: boundary typing, lock generation, enforcement, determinism, report binding."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from sans.runtime import run_script, generate_schema_lock_standalone
from sans.schema_lock import (
    build_schema_lock,
    compute_lock_sha256,
    load_schema_lock,
    lock_by_name,
    lock_entry_required_columns,
    write_schema_lock,
)


def _run(
    script: str,
    out_dir: Path,
    schema_lock_path: Optional[Path] = None,
    emit_schema_lock_path: Optional[Path] = None,
    lock_only: bool = False,
):
    return run_script(
        text=script,
        file_name="script.sans",
        bindings={},
        out_dir=out_dir,
        strict=True,
        schema_lock_path=schema_lock_path,
        emit_schema_lock_path=emit_schema_lock_path,
        lock_only=lock_only,
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


# 8) Lock generation with untyped datasource: script with csv("lb.csv") and no columns(); --emit-schema-lock produces lock with inferred types
def test_lock_generation_untyped_datasource(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "schema.lock.json"
    report = _run(script, tmp_path / "out", emit_schema_lock_path=lock_path)
    assert report["status"] == "ok"
    assert lock_path.exists()
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    assert lock.get("schema_lock_version") == 1
    ds_list = lock["datasources"]
    assert len(ds_list) == 1
    ds = ds_list[0]
    assert ds["name"] == "lb"
    assert ds["kind"] == "csv"
    cols = ds["columns"]
    assert len(cols) == 2
    names = [c["name"] for c in cols]
    assert "USUBJID" in names
    assert "VISITNUM" in names
    types = {c["name"]: c["type"] for c in cols}
    assert types["USUBJID"] == "string"
    assert types["VISITNUM"] == "int"
    assert ds.get("inference_policy_version") == 1
    assert ds.get("rows_scanned") == 1
    assert ds.get("truncated") is False
    assert "schema_lock_sha256" in report


# 9) Enforcement unchanged: run untyped script without lock fails E_SCHEMA_REQUIRED; with emitted lock succeeds
def test_enforcement_requires_lock_or_pin(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    report_no_lock = _run(script, tmp_path / "out1")
    assert report_no_lock["status"] == "failed"
    assert report_no_lock["primary_error"]["code"] == "E_SCHEMA_REQUIRED"
    lock_path = tmp_path / "schema.lock.json"
    _run(script, tmp_path / "out2", emit_schema_lock_path=lock_path)
    assert lock_path.exists()
    report_with_lock = _run(script, tmp_path / "out3", schema_lock_path=lock_path)
    assert report_with_lock["status"] == "ok"
    out_csv = tmp_path / "out3" / "outputs" / "out.csv"
    assert out_csv.exists()


# 10) Determinism: lock generated twice from same untyped script and file content is byte-identical
def test_lock_determinism_untyped(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    lock1_path = tmp_path / "lock1.json"
    lock2_path = tmp_path / "lock2.json"
    _run(script, tmp_path / "out1", emit_schema_lock_path=lock1_path)
    _run(script, tmp_path / "out2", emit_schema_lock_path=lock2_path)
    b1 = lock1_path.read_bytes()
    b2 = lock2_path.read_bytes()
    assert b1 == b2, "Lock files must be byte-identical for same untyped script and data"


# 11) Inference rules: int vs decimal vs string promotion; bool only strict true/false; empty/whitespace as null
def test_inference_rules(tmp_path: Path):
    csv_path = tmp_path / "mixed.csv"
    csv_path.write_text(
        "id,value,label,flag\n"
        "1,2.5,hello,true\n"
        "2,3,world,false\n"
        ",,,\n"
        "3,4.0,xyz,true\n",
        encoding="utf-8",
    )
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}")\n'
        "table t = from(ds) select id, value, label, flag\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "lock.json"
    _run(script, tmp_path / "out", emit_schema_lock_path=lock_path)
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    ds = lock["datasources"][0]
    types = {c["name"]: c["type"] for c in ds["columns"]}
    assert types["id"] == "int"
    assert types["value"] == "decimal"
    assert types["label"] == "string"
    assert types["flag"] == "bool"
    report = _run(script, tmp_path / "out2", schema_lock_path=lock_path)
    assert report["status"] == "ok"


# 12) Lock-only generation with pipeline (filter/rename/derive/select): no E_TYPE_UNKNOWN; lock has inferred types
def test_emit_schema_lock_untyped_with_pipeline_succeeds(tmp_path: Path):
    """Untyped datasource + filter/rename/derive/select would raise E_TYPE_UNKNOWN during type-check;
    with --emit-schema-lock we skip type validation and produce lock from CSV inference only."""
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text(
        "USUBJID,VISITNUM,LBTESTCD,LBSTRESN,LBDTC\n"
        "S001,1,HBA1C,5.7,2020-01-01\n",
        encoding="utf-8",
    )
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table sorted_high = from(lb) do\n"
        "\tfilter(LBTESTCD == \"HBA1C\")\n"
        "\trename(LBSTRESN -> A1C)\n"
        "\tselect USUBJID, VISITNUM, LBDTC, A1C\n"
        "\tderive(label = \"HIGH\")\n"
        "\tselect USUBJID, VISITNUM, A1C, label\n"
        "end\n"
        'save sorted_high to "sorted_high.csv"\n'
    )
    lock_path = tmp_path / "schema.lock.json"
    report = _run(script, tmp_path / "out", emit_schema_lock_path=lock_path)
    assert report["status"] == "ok", (
        f"Expected lock-only generation to succeed; got {report.get('primary_error')}"
    )
    assert lock_path.exists()
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    ds_list = lock["datasources"]
    assert len(ds_list) == 1
    ds = ds_list[0]
    assert ds["name"] == "lb"
    cols = ds["columns"]
    names = [c["name"] for c in cols]
    assert "USUBJID" in names
    assert "LBTESTCD" in names
    assert "LBSTRESN" in names
    assert ds.get("inference_policy_version") == 1
    # Normal run without lock must fail (either refused at type-check or failed at runtime)
    report_no_lock = _run(script, tmp_path / "out2")
    assert report_no_lock["status"] in ("failed", "refused")
    if report_no_lock["status"] == "failed":
        assert report_no_lock["primary_error"]["code"] == "E_SCHEMA_REQUIRED"


# 13) Bool only when strictly true/false; non-strict bool tokens force string
def test_inference_bool_strict_only(tmp_path: Path):
    csv_path = tmp_path / "b.csv"
    csv_path.write_text("flag\ntrue\nfalse\nyes\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}")\n'
        "table t = from(ds) select flag\n"
        "save t to \"out.csv\"\n"
    )
    lock_path = tmp_path / "lock.json"
    _run(script, tmp_path / "out", emit_schema_lock_path=lock_path)
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    types = {c["name"]: c["type"] for c in lock["datasources"][0]["columns"]}
    assert types["flag"] == "string", "yes/true/false mix should promote to string"


# 14) Lock-only: relative --emit-schema-lock path resolved against out_dir; lock under out_dir; report fields
def test_lock_only_writes_into_out_dir(tmp_path: Path):
    out_dir = tmp_path / "out"
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(script, out_dir, emit_schema_lock_path=Path("schema.lock.json"))
    assert report["status"] == "ok"
    lock_file = out_dir / "schema.lock.json"
    assert lock_file.exists(), f"Lock should be under out_dir at {lock_file}"
    assert not (tmp_path / "schema.lock.json").exists(), "Lock must not be in cwd/repo root"
    assert report.get("lock_only") is True
    assert report.get("schema_lock_mode") == "generated_only"
    assert report.get("schema_lock_emit_path") == str(lock_file.resolve())
    assert "schema_lock_path" in report
    lock = json.loads(lock_file.read_text(encoding="utf-8"))
    assert len(lock["datasources"]) == 1


# 15) Run+emit: typed datasource + relative --emit-schema-lock; run executes, lock under out_dir
def test_run_and_emit_writes_into_out_dir(tmp_path: Path):
    out_dir = tmp_path / "out"
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}", columns(a:int, b:int))\n'
        "table t = from(ds) select a, b\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(script, out_dir, emit_schema_lock_path=Path("schema.lock.json"))
    assert report["status"] == "ok"
    lock_file = out_dir / "schema.lock.json"
    assert lock_file.exists()
    assert report.get("lock_only") is False
    assert report.get("schema_lock_mode") == "ran_and_emitted"
    out_csv = out_dir / "outputs" / "out.csv"
    assert out_csv.exists(), "Run should have produced outputs"
    assert (out_dir / "inputs" / "source" / "expanded.sans").exists()


# 16) Regression: normal run (no --emit-schema-lock) still stages inputs
def test_run_mode_still_stages_inputs(tmp_path: Path):
    out_dir = tmp_path / "out"
    csv_path = tmp_path / "x.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource x = csv("{csv_path.as_posix()}", columns(a:int, b:int))\n'
        "table t = from(x) select a, b\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(script, out_dir)
    assert report["status"] == "ok"
    expanded = out_dir / "inputs" / "source" / "expanded.sans"
    assert expanded.exists()
    inputs_in_report = [inp.get("name") for inp in report.get("inputs", [])]
    assert "expanded.sans" in inputs_in_report


# 17) Lock-only stages referenced datasource files into out_dir/inputs/data
def test_lock_only_stages_datasource_inputs(tmp_path: Path):
    out_dir = tmp_path / "out"
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(script, out_dir, emit_schema_lock_path=Path("schema.lock.json"))
    assert report["status"] == "ok"
    data_dir = out_dir / "inputs" / "data"
    assert data_dir.exists()
    staged_lb = data_dir / "lb.csv"
    assert staged_lb.exists(), "Referenced CSV should be staged in inputs/data"
    datasource_inputs = [inp for inp in report.get("inputs", []) if inp.get("role") == "datasource"]
    assert any(inp.get("name") == "lb" for inp in datasource_inputs)


# 18) Run with --schema-lock: lock is copied into out_dir; report has used/copied paths and sha256
def test_schema_lock_copied_into_out_dir(tmp_path: Path):
    out_dir = tmp_path / "out"
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    lock_source = tmp_path / "locks" / "lb.schema.lock.json"
    lock_source.parent.mkdir(parents=True, exist_ok=True)
    lock_content = json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{
            "name": "ds",
            "kind": "csv",
            "path": "data.csv",
            "columns": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}],
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        }],
    }, indent=2)
    lock_source.write_text(lock_content, encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}")\n'
        "table t = from(ds) select a, b\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(script, out_dir, schema_lock_path=lock_source)
    assert report["status"] == "ok"
    copied = out_dir / "schema.lock.json"
    assert copied.exists(), "Lock should be copied into out_dir"
    assert copied.read_bytes() == lock_source.read_bytes(), "Copied lock must be byte-identical"
    assert report.get("schema_lock_used_path") == str(lock_source.resolve())
    assert report.get("schema_lock_copied_path") == "schema.lock.json"
    assert "schema_lock_sha256" in report
    lock_dict = load_schema_lock(copied)
    assert compute_lock_sha256(lock_dict) == report["schema_lock_sha256"]


# 19) --lock-only with --emit-schema-lock: no execution, lock generated, report lock_only=true
def test_lock_only_command_does_not_execute(tmp_path: Path):
    out_dir = tmp_path / "out"
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}", columns(a:int, b:int))\n'
        "table t = from(ds) select a, b\n"
        "save t to \"out.csv\"\n"
    )
    report = _run(
        script,
        out_dir,
        emit_schema_lock_path=Path("schema.lock.json"),
        lock_only=True,
    )
    assert report["status"] == "ok"
    assert report.get("lock_only") is True
    assert report.get("schema_lock_mode") == "generated_only"
    assert not (out_dir / "outputs" / "out.csv").exists(), "No outputs when lock-only"
    lock_file = out_dir / "schema.lock.json"
    assert lock_file.exists()
    lock = json.loads(lock_file.read_text(encoding="utf-8"))
    assert len(lock["datasources"]) == 1
    assert (out_dir / "inputs" / "data" / "ds.csv").exists(), "Inputs should be staged in lock-only"


# 20) schema-lock subcommand without --out: only lock file written, no report or bundle
def test_schema_lock_subcommand_without_out_writes_lock_only(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    script_path = tmp_path / "script.sans"
    script_path.write_text(script, encoding="utf-8")
    lock_path = tmp_path / "schema.lock.json"
    report = generate_schema_lock_standalone(
        text=script,
        file_name=str(script_path),
        write_path=lock_path,
        out_dir=None,
        bindings=None,
    )
    assert report["status"] == "ok"
    assert lock_path.exists()
    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "inputs").exists()
    lock = json.loads(lock_path.read_text(encoding="utf-8"))
    assert lock.get("schema_lock_version") == 1
    assert len(lock["datasources"]) == 1
    assert lock["datasources"][0]["name"] == "lb"


# 21) schema-lock subcommand with --out: lock + report + staged inputs
def test_schema_lock_subcommand_with_out_writes_lock_and_bundle(tmp_path: Path):
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        "save t to \"out.csv\"\n"
    )
    script_path = tmp_path / "script.sans"
    script_path.write_text(script, encoding="utf-8")
    lock_path = tmp_path / "schema.lock.json"
    out_dir = tmp_path / "out"
    report = generate_schema_lock_standalone(
        text=script,
        file_name=str(script_path),
        write_path=lock_path,
        out_dir=out_dir,
        bindings=None,
    )
    assert report["status"] == "ok"
    assert lock_path.exists()
    assert (out_dir / "report.json").exists()
    assert (out_dir / "inputs" / "source" / "expanded.sans").exists()
    data_dir = out_dir / "inputs" / "data"
    assert data_dir.exists()
    assert (data_dir / "lb.csv").exists()
    out_report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    assert out_report.get("lock_only") is True
    assert out_report.get("schema_lock_mode") == "generated_only"


# 22) Run still requires --out (regression)
def test_run_still_requires_out(tmp_path: Path):
    import subprocess
    import sys
    script_path = tmp_path / "script.sans"
    script_path.write_text("# sans 0.1\ndatasource x = csv(\"x.csv\")\ntable t = from(x) select 1 as a\nsave t to \"out.csv\"\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-m", "sans", "run", str(script_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "out" in result.stderr.lower() or "required" in result.stderr.lower() or "error" in result.stderr.lower()


# 23) schema-lock default path: lock next to script, no out_dir artifacts
def test_schema_lock_default_path(tmp_path: Path):
    import subprocess
    import sys
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script_path = tmp_path / "demo_high.sans"
    script_path.write_text(
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.name}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        'save t to "out.csv"\n',
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "-m", "sans", "schema-lock", "demo_high.sans"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    lock_file = tmp_path / "demo_high.schema.lock.json"
    assert lock_file.exists(), f"Lock should be at {lock_file}"
    assert not (tmp_path / "report.json").exists()
    assert not (tmp_path / "inputs").exists()
    lock = json.loads(lock_file.read_text(encoding="utf-8"))
    assert lock.get("schema_lock_version") == 1
    assert len(lock["datasources"]) == 1
    assert lock["datasources"][0]["name"] == "lb"


# 24) schema-lock --write: relative path resolved against script directory
def test_schema_lock_write_override(tmp_path: Path):
    import subprocess
    import sys
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    script_path = tmp_path / "script.sans"
    script_path.write_text(
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.name}")\n'
        "table t = from(ds) select a, b\n"
        'save t to "out.csv"\n',
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "-m", "sans", "schema-lock", "script.sans", "--write", "locks/demo.schema.lock.json"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    lock_file = tmp_path / "locks" / "demo.schema.lock.json"
    assert lock_file.exists(), f"Lock should be at script_dir/locks/demo.schema.lock.json = {lock_file}"
    lock = json.loads(lock_file.read_text(encoding="utf-8"))
    assert len(lock["datasources"]) == 1


# 25) schema-lock with --out: lock at default (or --write), report + staged inputs under out_dir
def test_schema_lock_with_out_dir(tmp_path: Path):
    import subprocess
    import sys
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    script_path = tmp_path / "script.sans"
    script_path.write_text(
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.name}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        'save t to "out.csv"\n',
        encoding="utf-8",
    )
    out_dir = tmp_path / "tmpdir"
    result = subprocess.run(
        [sys.executable, "-m", "sans", "schema-lock", "script.sans", "--out", str(out_dir)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    default_lock = tmp_path / "script.schema.lock.json"
    assert default_lock.exists(), "Lock at default path next to script"
    assert (out_dir / "report.json").exists()
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    assert report.get("lock_only") is True
    assert report.get("schema_lock_mode") == "generated_only"
    assert (out_dir / "inputs" / "source" / "expanded.sans").exists()
    assert (out_dir / "inputs" / "data" / "lb.csv").exists()


# 26) Run with relative --schema-lock from script dir: lock resolved against script dir, types applied (no E_TYPE_UNKNOWN)
def test_run_with_relative_schema_lock_from_script_dir(tmp_path: Path):
    import subprocess
    import sys
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\nS002,2\n", encoding="utf-8")
    lock_path = tmp_path / "demo_high.schema.lock.json"
    lock_path.write_text(json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{
            "name": "lb",
            "kind": "csv",
            "path": "lb.csv",
            "columns": [{"name": "USUBJID", "type": "string"}, {"name": "VISITNUM", "type": "int"}],
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        }],
    }, indent=2), encoding="utf-8")
    script_path = tmp_path / "demo_high.sans"
    script_path.write_text(
        "# sans 0.1\n"
        'datasource lb = csv("lb.csv")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        'save t to "out.csv"\n',
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, "-m", "sans", "run", "demo_high.sans", "--out", "out", "--schema-lock", "demo_high.schema.lock.json"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    report_path = tmp_path / "out" / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "ok"
    assert report.get("primary_error") is None or report.get("primary_error", {}).get("code") != "E_TYPE_UNKNOWN"
    out_csv = tmp_path / "out" / "outputs" / "out.csv"
    assert out_csv.exists()
    assert (tmp_path / "out" / "schema.lock.json").exists(), "Lock should be copied into out_dir"


# 27) Run with relative --schema-lock from different cwd: resolution is script-dir based so lock is found
def test_run_with_relative_schema_lock_from_different_cwd(tmp_path: Path):
    import subprocess
    import sys
    script_dir = tmp_path / "sub"
    script_dir.mkdir()
    csv_path = script_dir / "lb.csv"
    csv_path.write_text("USUBJID,VISITNUM\nS001,1\n", encoding="utf-8")
    lock_path = script_dir / "demo_high.schema.lock.json"
    lock_path.write_text(json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{
            "name": "lb",
            "kind": "csv",
            "path": "lb.csv",
            "columns": [{"name": "USUBJID", "type": "string"}, {"name": "VISITNUM", "type": "int"}],
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        }],
    }, indent=2), encoding="utf-8")
    script_path = script_dir / "demo_high.sans"
    script_path.write_text(
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select USUBJID, VISITNUM\n"
        'save t to "out.csv"\n',
        encoding="utf-8",
    )
    # Run from parent (tmp_path); relative lock path resolved against script dir (sub/)
    result = subprocess.run(
        [sys.executable, "-m", "sans", "run", "sub/demo_high.sans", "--out", "sub/out", "--schema-lock", "demo_high.schema.lock.json"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    report_path = script_dir / "out" / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "ok"
    assert (script_dir / "out" / "schema.lock.json").exists()


# 28) Run with missing --schema-lock path fails with E_SCHEMA_LOCK_NOT_FOUND (not E_TYPE_UNKNOWN)
def test_run_with_missing_schema_lock_fails_with_E_SCHEMA_LOCK_NOT_FOUND(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource ds = csv("{csv_path.as_posix()}")\n'
        "table t = from(ds) select a, b\n"
        'save t to "out.csv"\n'
    )
    out_dir = tmp_path / "out"
    # Pass a relative path that does not exist (script dir = tmp_path, so tmp_path/nonexistent.lock.json)
    report = _run(script, out_dir, schema_lock_path=Path("nonexistent.lock.json"))
    assert report["status"] == "failed"
    assert report.get("primary_error", {}).get("code") == "E_SCHEMA_LOCK_NOT_FOUND"
    assert "E_TYPE_UNKNOWN" not in str(report.get("primary_error", {}))


# 29) Golden: run with abs lock (lock applied at compile time, no E_TYPE_UNKNOWN)
def test_schema_lock_then_run_with_abs_lock_succeeds(tmp_path: Path):
    import subprocess
    import sys
    csv_path = tmp_path / "lb.csv"
    csv_path.write_text(
        "USUBJID,VISITNUM,LBTESTCD,LBSTRESN,LBDTC\nS001,1,HBA1C,5.2,2020-01-01\n",
        encoding="utf-8",
    )
    script_path = tmp_path / "demo_high.sans"
    script_path.write_text(
        "# sans 0.1\n"
        'datasource lb = csv("lb.csv")\n'
        "table sorted_high = from(lb) do\n"
        '\tfilter(LBTESTCD == "HBA1C")\n'
        "\trename(LBSTRESN -> A1C)\n"
        "\tselect USUBJID, VISITNUM, LBDTC, A1C\n"
        '\tderive(label = "HIGH")\n'
        "\tselect USUBJID, VISITNUM, A1C, label\n"
        "end\n"
        'save sorted_high to "sorted_high.csv"\n',
        encoding="utf-8",
    )
    lock_path = tmp_path / "demo_high.schema.lock.json"
    lock_path.write_text(json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{
            "name": "lb",
            "kind": "csv",
            "path": "lb.csv",
            "columns": [
                {"name": "USUBJID", "type": "string"},
                {"name": "VISITNUM", "type": "int"},
                {"name": "LBTESTCD", "type": "string"},
                {"name": "LBSTRESN", "type": "decimal"},
                {"name": "LBDTC", "type": "string"},
            ],
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        }],
    }, indent=2), encoding="utf-8")
    out_dir = tmp_path / "dh_out"
    result = subprocess.run(
        [sys.executable, "-m", "sans", "run", "demo_high.sans", "--out", str(out_dir), "--schema-lock", str(lock_path.resolve())],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "ok"
    assert report.get("primary_error") is None or report.get("primary_error", {}).get("code") != "E_TYPE_UNKNOWN"
    assert (out_dir / "outputs" / "sorted_high.csv").exists()
    assert report.get("schema_lock_applied_datasources") == ["lb"]
    assert report.get("schema_lock_used_path") == str(lock_path.resolve())


# 30) Lock missing a referenced datasource fails with E_SCHEMA_LOCK_MISSING_DS (not E_TYPE_UNKNOWN)
def test_run_with_lock_missing_datasource_fails_E_SCHEMA_LOCK_MISSING_DS(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    lock_path = tmp_path / "lock.json"
    lock_path.write_text(json.dumps({
        "schema_lock_version": 1,
        "created_by": {"sans_version": "0.1", "git_sha": ""},
        "datasources": [{"name": "other", "kind": "csv", "path": "x.csv", "columns": [{"name": "x", "type": "int"}], "rules": {"extra_columns": "ignore", "missing_columns": "error"}}],
    }, indent=2), encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource lb = csv("{csv_path.as_posix()}")\n'
        "table t = from(lb) select a, b\n"
        'save t to "out.csv"\n'
    )
    out_dir = tmp_path / "out"
    report = _run(script, out_dir, schema_lock_path=lock_path)
    assert report["status"] == "refused"
    assert report.get("primary_error", {}).get("code") == "E_SCHEMA_LOCK_MISSING_DS"
    assert "lb" in (report.get("schema_lock_missing_datasources") or [])


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
