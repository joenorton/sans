import json
import csv
import pytest
from pathlib import Path
from decimal import Decimal
from sans.runtime import run_script, _parse_value
from sans.__main__ import main

def test_exit_bucket_runtime_is_50(tmp_path):
    # Division by zero or similar runtime error
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1", encoding="utf-8")
    script = "data out; set in; x = 1/0; run;"
    report = run_script(script, "fail.sas", {"in": str(in_csv)}, tmp_path)
    assert report["exit_code_bucket"] == 50

def test_csv_newlines_lf(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2", encoding="utf-8")
    script = "data out; set in; run;"
    run_script(script, "test.sas", {"in": str(in_csv)}, tmp_path)
    out_csv = tmp_path / "out.csv"
    content = out_csv.read_bytes()
    assert b"\r\n" not in content
    assert b"\n" in content

def test_sort_missing_first(tmp_path):
    in_csv = tmp_path / "in.csv"
    # Use csv module to be safe about empty strings
    with open(in_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["a"])
        writer.writerow(["2"])
        writer.writerow([""])
        writer.writerow(["1"])
    
    script = "proc sort data=in out=out; by a; run;"
    run_script(script, "sort.sas", {"in": str(in_csv)}, tmp_path)
    rows = (tmp_path / "out.csv").read_text(encoding="utf-8").splitlines()
    # Missing sorts first (index 1 after header)
    assert rows[1] == '""'

def test_where_missing_comparisons(tmp_path):
    in_csv = tmp_path / "in.csv"
    with open(in_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["a"])
        writer.writerow([""])
        writer.writerow(["5"])
    script = "data out; set in; if a < 5; run;"
    run_script(script, "where.sas", {"in": str(in_csv)}, tmp_path)
    rows = (tmp_path / "out.csv").read_text(encoding="utf-8").splitlines()
    assert rows == ["a", '""']

def test_parse_leading_zero_is_string():
    val = _parse_value("0123")
    assert isinstance(val, str)
    assert val == "0123"

def test_decimal_precision_stable(tmp_path):
    in_csv = tmp_path / "in.csv"
    long_val = "12345678901234567890.1234567890"
    in_csv.write_text(f"a\n{long_val}", encoding="utf-8")
    script = "data out; set in; b = a; run;"
    run_script(script, "prec.sas", {"in": str(in_csv)}, tmp_path)
    content = (tmp_path / "out.csv").read_text(encoding="utf-8")
    assert long_val in content

def test_duplicate_table_binding_errors(tmp_path):
    script = tmp_path / "s.sas"
    script.write_text("data out; set a; run;")
    ret = main(["run", str(script), "--out", str(tmp_path), "--tables", "a=1.csv,a=2.csv"])
    assert ret == 50

def test_verify_detects_modified_report(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1", encoding="utf-8")
    script = tmp_path / "s.sas"
    script.write_text("data out; set in; run;")
    main(["run", str(script), "--out", str(tmp_path), "--tables", f"in={in_csv}"])
    
    report_path = tmp_path / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    
    # Modify something non-hash
    report["engine"]["version"] = "corrupted"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    
    ret = main(["verify", str(report_path)])
    assert ret != 0

def test_artifact_hashes_stable_for_same_inputs(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1", encoding="utf-8")
    script = "data out; set in; run;"
    
    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    
    r1 = run_script(script, "s.sas", {"in": str(in_csv)}, out1)
    r2 = run_script(script, "s.sas", {"in": str(in_csv)}, out2)
    
    h1 = [o["sha256"] for o in r1["outputs"] if "plan.ir.json" in o["path"]]
    h2 = [o["sha256"] for o in r2["outputs"] if "plan.ir.json" in o["path"]]
    assert h1 == h2

def test_double_run_determinism(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a,b\n1,2\n3,4", encoding="utf-8")
    script_path = tmp_path / "s.sas"
    script_path.write_text("data out; set in; c = a + b; run;", encoding="utf-8")

    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"

    ret1 = main(["run", str(script_path), "--out", str(out1), "--tables", f"in={in_csv}"])
    ret2 = main(["run", str(script_path), "--out", str(out2), "--tables", f"in={in_csv}"])
    assert ret1 == 0
    assert ret2 == 0

    # 1) semantic artifact determinism: plan.ir.json must match byte-for-byte.
    p1 = (out1 / "plan.ir.json").read_text(encoding="utf-8")
    p2 = (out2 / "plan.ir.json").read_text(encoding="utf-8")
    assert p1 == p2

    # optional: expanded.sans is your canonical human form; compare it too.
    expanded1 = out1 / "expanded.sans"
    expanded2 = out2 / "expanded.sans"
    assert expanded1.exists()
    assert expanded2.exists()
    assert expanded1.read_text(encoding="utf-8") == expanded2.read_text(encoding="utf-8")


    # 2) output artifact determinism: out.csv bytes must match.
    o1 = (out1 / "out.csv").read_bytes()
    o2 = (out2 / "out.csv").read_bytes()
    assert o1 == o2

    # 3) report determinism foundation: each run must verify under the canonical hash contract.
    # we do NOT compare report.json across runs; report is allowed to include env/timing/path noise.
    assert main(["verify", str(out1)]) == 0
    assert main(["verify", str(out2)]) == 0

