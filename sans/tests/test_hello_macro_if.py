"""
Macro %if tests. Per SAS ingestion contract (docs/SAS_INGESTION_CONTRACT.md)
%if/%then/%else that changes graph shape is rejected with SANS_REFUSAL_MACRO_GRAPH.
"""
import json
from pathlib import Path

from sans.__main__ import main


def test_sas_refuses_macro_if_then_else(tmp_path):
    """SAS script with %if/%then/%else is refused with SANS_REFUSAL_MACRO_GRAPH."""
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("x,y\n1,2\n", encoding="utf-8")

    script = """
    %let KEEPY = 1;
    data out;
      set in;
      %if &KEEPY = 1 %then keep x y; %else keep x;
    run;
    """
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"in={in_csv}", "--legacy-sas"])
    assert ret != 0

    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report.get("primary_error", {}).get("code") == "SANS_REFUSAL_MACRO_GRAPH"


def test_sas_refuses_macro_if_do_block(tmp_path):
    """SAS script with %if %then %do; ... %end; is refused with SANS_REFUSAL_MACRO_GRAPH."""
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("x,y\n1,2\n", encoding="utf-8")

    script = """
    %if 1 = 1 %then %do;
    data out;
      set in;
    run;
    %end;
    """
    script_path = tmp_path / "script_do.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_do"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"in={in_csv}", "--legacy-sas"])
    assert ret != 0

    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report.get("primary_error", {}).get("code") == "SANS_REFUSAL_MACRO_GRAPH"
