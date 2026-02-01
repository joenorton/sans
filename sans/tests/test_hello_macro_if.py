import csv
from sans.__main__ import main


def test_hello_macro_if_then_else(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("x,y\n1,2\n", encoding="utf-8")

    for flag, expected_headers in ((1, ["x", "y"]), (0, ["x"])):
        script = f"""
        %let KEEPY = {flag};
        data out;
          set in;
          %if &KEEPY = 1 %then keep x y; %else keep x;
        run;
        """
        script_path = tmp_path / f"script_{flag}.sas"
        script_path.write_text(script, encoding="utf-8")

        out_dir = tmp_path / f"out_{flag}"
        ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"in={in_csv}"])
        assert ret == 0

        out_csv = out_dir / "out.csv"
        with out_csv.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.reader(f))

        assert rows[0] == expected_headers


def test_macro_if_numeric_comparison(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("x,y\n1,2\n", encoding="utf-8")

    script = """
    %let THRESH = 5;
    data out;
      set in;
      %if &THRESH > 3 %then keep x y; %else keep x;
    run;
    """
    script_path = tmp_path / "script_numeric.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_numeric"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"in={in_csv}"])
    assert ret == 0

    out_csv = out_dir / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["x", "y"]


def test_macro_if_string_comparison(tmp_path):
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("x,y\n1,2\n", encoding="utf-8")

    script = """
    %let FLAG = "YES";
    data out;
      set in;
      %if &FLAG = "YES" %then keep x; %else keep x y;
    run;
    """
    script_path = tmp_path / "script_string.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_string"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"in={in_csv}"])
    assert ret == 0

    out_csv = out_dir / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["x"]


def test_macro_if_rejects_do_block(tmp_path):
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
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"in={in_csv}"])
    assert ret != 0

    report_path = out_dir / "report.json"
    report = report_path.read_text(encoding="utf-8")
    assert "SANS_PARSE_MACRO_ERROR" in report
    assert "%do" in report
