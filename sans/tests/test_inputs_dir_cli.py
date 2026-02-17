import argparse
import json
from pathlib import Path

import pytest

from sans.__main__ import TableFlagError, _scan_inputs_dir_csv_bindings, main, resolve_tables_from_flags


def test_inputs_dir_scan_is_deterministic_and_csv_only(tmp_path):
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    (inputs_dir / "z.csv").write_text("x\n1\n", encoding="utf-8")
    (inputs_dir / "a.csv").write_text("x\n2\n", encoding="utf-8")
    (inputs_dir / "notes.txt").write_text("ignore", encoding="utf-8")
    nested = inputs_dir / "nested"
    nested.mkdir()
    (nested / "skip.csv").write_text("x\n3\n", encoding="utf-8")

    first = _scan_inputs_dir_csv_bindings(str(inputs_dir))
    second = _scan_inputs_dir_csv_bindings(str(inputs_dir))

    assert first == second
    assert [name for name, _ in first] == ["a", "z"]
    assert [Path(path).name for _, path in first] == ["a.csv", "z.csv"]


def test_resolve_tables_from_flags_rejects_tables_plus_inputs_dir(tmp_path):
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    args = argparse.Namespace(tables="in=data.csv", inputs_dir=str(inputs_dir))

    with pytest.raises(TableFlagError, match="choose one: --tables or --inputs-dir"):
        resolve_tables_from_flags(args, mode="bindings")


def test_run_inputs_dir_expands_to_bindings(tmp_path):
    script_path = tmp_path / "script.sas"
    script_path.write_text("data out; set input; z = x + y; run;", encoding="utf-8")

    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    input_csv = inputs_dir / "input.csv"
    input_csv.write_text("x,y\n1,2\n3,4\n", encoding="utf-8")
    (inputs_dir / "extra.txt").write_text("ignored", encoding="utf-8")

    out_inputs_dir = tmp_path / "out_inputs_dir"
    out_tables = tmp_path / "out_tables"

    ret_inputs_dir = main(
        [
            "run",
            str(script_path),
            "--out",
            str(out_inputs_dir),
            "--inputs-dir",
            str(inputs_dir),
            "--legacy-sas",
        ]
    )
    assert ret_inputs_dir == 0

    ret_tables = main(
        [
            "run",
            str(script_path),
            "--out",
            str(out_tables),
            "--tables",
            f"input={input_csv}",
            "--legacy-sas",
        ]
    )
    assert ret_tables == 0

    out_inputs = (out_inputs_dir / "outputs" / "out.csv").read_text(encoding="utf-8")
    out_explicit = (out_tables / "outputs" / "out.csv").read_text(encoding="utf-8")
    assert out_inputs == out_explicit


def test_run_rejects_tables_plus_inputs_dir(tmp_path):
    script_path = tmp_path / "script.sas"
    script_path.write_text("data out; set input; run;", encoding="utf-8")

    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    input_csv = inputs_dir / "input.csv"
    input_csv.write_text("x\n1\n", encoding="utf-8")

    out_dir = tmp_path / "out"
    ret = main(
        [
            "run",
            str(script_path),
            "--out",
            str(out_dir),
            "--tables",
            f"input={input_csv}",
            "--inputs-dir",
            str(inputs_dir),
            "--legacy-sas",
        ]
    )
    assert ret == 50
    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    assert "choose one: --tables or --inputs-dir" in report["primary_error"]["message"]


def test_verify_inputs_dir_checks_external_thin_datasource(tmp_path, monkeypatch):
    script_path = tmp_path / "script.sas"
    script_path.write_text("data out; set input; run;", encoding="utf-8")

    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    input_csv = inputs_dir / "input.csv"
    input_csv.write_text("x\n1\n2\n", encoding="utf-8")

    out_dir = tmp_path / "thin_bundle"
    ret = main(
        [
            "run",
            str(script_path),
            "--out",
            str(out_dir),
            "--tables",
            f"input={input_csv}",
            "--legacy-sas",
            "--bundle-mode",
            "thin",
        ]
    )
    assert ret == 0

    unrelated_cwd = tmp_path / "other_cwd"
    unrelated_cwd.mkdir()
    monkeypatch.chdir(unrelated_cwd)

    ret_verify_inputs_dir = main(["verify", str(out_dir), "--inputs-dir", str(inputs_dir)])
    assert ret_verify_inputs_dir == 0

    ret_verify_tables = main(["verify", str(out_dir), "--tables", f"input={input_csv}"])
    assert ret_verify_tables == 0

    input_csv.write_text("x\n999\n", encoding="utf-8")
    ret_verify_after_tamper = main(["verify", str(out_dir), "--inputs-dir", str(inputs_dir)])
    assert ret_verify_after_tamper == 1


def test_run_inputs_alias_works(tmp_path):
    script_path = tmp_path / "script.sas"
    script_path.write_text("data out; set input; run;", encoding="utf-8")
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    (inputs_dir / "input.csv").write_text("x\n1\n", encoding="utf-8")
    out_dir = tmp_path / "out"

    ret = main(["run", str(script_path), "--out", str(out_dir), "--inputs", str(inputs_dir), "--legacy-sas"])
    assert ret == 0


def test_inputs_dir_duplicate_table_name_case_insensitive(tmp_path):
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    (inputs_dir / "lb.csv").write_text("x\n1\n", encoding="utf-8")
    (inputs_dir / "LB.csv").write_text("x\n2\n", encoding="utf-8")
    csv_names = [p.name for p in inputs_dir.iterdir() if p.suffix.lower() == ".csv"]
    if len(csv_names) < 2:
        pytest.skip("filesystem is case-insensitive and cannot represent lb.csv + LB.csv as distinct files")

    with pytest.raises(TableFlagError, match=r"duplicate table name from inputs-dir: lb"):
        _scan_inputs_dir_csv_bindings(str(inputs_dir))


def test_inputs_dir_nonexistent_raises(tmp_path):
    missing = tmp_path / "missing_inputs"
    with pytest.raises(TableFlagError, match="Inputs directory not found"):
        _scan_inputs_dir_csv_bindings(str(missing))


def test_inputs_dir_empty_errors_for_bindings_mode(tmp_path):
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    args = argparse.Namespace(tables="", inputs_dir=str(inputs_dir))

    with pytest.raises(TableFlagError, match="inputs-dir has no .csv files"):
        resolve_tables_from_flags(args, mode="bindings")


def test_inputs_dir_empty_errors_for_names_mode(tmp_path):
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    args = argparse.Namespace(tables="", inputs_dir=str(inputs_dir))

    with pytest.raises(TableFlagError, match="inputs-dir has no .csv files"):
        resolve_tables_from_flags(args, mode="names")


def test_inputs_dir_symlink_rejected(tmp_path):
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    target = tmp_path / "real.csv"
    target.write_text("x\n1\n", encoding="utf-8")
    link = inputs_dir / "lb.csv"
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not supported in this environment")

    with pytest.raises(TableFlagError, match="symlink not allowed in inputs-dir"):
        _scan_inputs_dir_csv_bindings(str(inputs_dir))


def test_verify_ignores_poison_report_in_cwd(tmp_path, monkeypatch):
    script_path = tmp_path / "script.sas"
    script_path.write_text("data out; set input; run;", encoding="utf-8")
    inputs_dir = tmp_path / "inputs"
    inputs_dir.mkdir()
    input_csv = inputs_dir / "input.csv"
    input_csv.write_text("x\n1\n2\n", encoding="utf-8")

    out_dir = tmp_path / "thin_bundle"
    ret = main(
        [
            "run",
            str(script_path),
            "--out",
            str(out_dir),
            "--tables",
            f"input={input_csv}",
            "--legacy-sas",
            "--bundle-mode",
            "thin",
        ]
    )
    assert ret == 0

    poison_cwd = tmp_path / "poison_cwd"
    poison_cwd.mkdir()
    (poison_cwd / "report.json").write_text("{not-json", encoding="utf-8")
    monkeypatch.chdir(poison_cwd)

    ret_verify = main(["verify", str(out_dir.resolve()), "--inputs-dir", str(inputs_dir.resolve())])
    assert ret_verify == 0
