import csv
import json
from pathlib import Path
from decimal import Decimal
import pytest
from sans.runtime import run_script, _parse_value, _compare_sas

def test_exit_code_bucket_consistency(tmp_path):
    # Test that a runtime failure returns 50
    script = "data out; set in; x = 1/0; run;" # Note: Decimal division by zero raises decimal.DivisionByZero
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1", encoding="utf-8")
    
    # We expect a RuntimeFailure which should be bucketed as 50
    report = run_script(
        text=script,
        file_name="fail.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path
    )
    
    assert report["status"] == "failed"
    assert report["exit_code_bucket"] == 50

def test_windows_newline_parity(tmp_path):
    # Write a file with CRLF, verify hash is same as LF
    in_csv = tmp_path / "in.csv"
    content = "a,b\r\n1,2\r\n"
    in_csv.write_bytes(content.encode("utf-8"))
    
    script = "data out; set in; run;"
    report = run_script(
        text=script,
        file_name="test.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path
    )
    
    out_csv = tmp_path / "out.csv"
    out_content = out_csv.read_text(encoding="utf-8")
    # Verify our output always uses \n
    assert "\r\n" not in out_content
    assert out_content == "a,b\n1,2\n"

def test_sort_missing_values(tmp_path):
    in_csv = tmp_path / "in.csv"
    with open(in_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["a"])
        writer.writerow(["2"])
        writer.writerow([""])
        writer.writerow(["1"])
    
    script = "proc sort data=in out=out; by a; run;"
    run_script(text=script, file_name="sort.sas", bindings={"in": str(in_csv)}, out_dir=tmp_path)   

    out_csv = tmp_path / "out.csv"
    rows = out_csv.read_text(encoding="utf-8").splitlines()
    # Header, then None (empty), then 1, then 2
    # Note: csv.writer on single-column empty string writes '""'
    assert rows == ["a", '""', "1", "2"]

def test_where_missing_comparisons(tmp_path):
    in_csv = tmp_path / "in.csv"
    with open(in_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["a"])
        writer.writerow([""])
        writer.writerow(["5"])
    
    # if a < 5 should be true for None
    script = "data out; set in; if a < 5; run;"
    run_script(text=script, file_name="where.sas", bindings={"in": str(in_csv)}, out_dir=tmp_path)  

    out_csv = tmp_path / "out.csv"
    rows = out_csv.read_text(encoding="utf-8").splitlines()
    assert rows == ["a", '""'] # Only the None row remains if 5 is not < 5
def test_duplicate_table_binding_fails(tmp_path):
    from sans.__main__ import main
    # Use main to test CLI level
    ret = main(["run", "script.sas", "--out", str(tmp_path), "--tables", "a=1.csv,a=2.csv"])
    assert ret == 50
    
    report_path = tmp_path / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert "Duplicate table binding" in report["primary_error"]["message"]

def test_empty_csv_behavior(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("", encoding="utf-8") # Completely empty
    
    script = "data out; set in; run;"
    report = run_script(text=script, file_name="empty.sas", bindings={"in": str(in_csv)}, out_dir=tmp_path)
    
    assert report["status"] == "ok"
    out_csv = tmp_path / "out.csv"
    assert out_csv.read_text(encoding="utf-8") == ""

def test_decimal_precision(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1.00000000000000000001", encoding="utf-8")
    
    script = "data out; set in; b = a + 1; run;"
    run_script(text=script, file_name="prec.sas", bindings={"in": str(in_csv)}, out_dir=tmp_path)
    
    out_csv = tmp_path / "out.csv"
    content = out_csv.read_text(encoding="utf-8")
    assert "2.00000000000000000001" in content

def test_leading_zero_preservation(tmp_path):
    assert _parse_value("0123") == "0123"
    assert _parse_value("123") == 123
    assert isinstance(_parse_value("1.2"), Decimal)
