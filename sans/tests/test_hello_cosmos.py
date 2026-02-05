import csv
import math
import subprocess
import sys
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_cmd(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True)


def test_hello_cosmos_end_to_end(tmp_path: Path):
    script = "\n".join(
        [
            "proc sort data=lb out=lb_s;",
            "  by subjid lbdtc;",
            "run;",
            "",
            "proc sql;",
            "  create table lb_named as",
            "  select lb.subjid,",
            "         lb.lbdtc,",
            "         lb.lbtestcd,",
            "         dict.lbtest as lbtest,",
            "         lb.lbstresn",
            "  from lb_s as lb",
            "  left join lb_dict as dict",
            "    on lb.lbtestcd = dict.lbtestcd",
            "  where lb.lbdtc >= \"2023-01-12\";",
            "quit;",
            "",
            "proc sql;",
            "  create table lb_summary as",
            "  select subjid,",
            "         count(*) as nrec,",
            "         avg(lbstresn) as mean_res",
            "  from lb_named",
            "  group by subjid;",
            "quit;",
        ]
    )

    lb_csv = "\n".join(
        [
            "subjid,lbdtc,lbtestcd,lbstresn",
            "101,2023-01-10,GLUC,95",
            "101,2023-01-13,GLUC,110",
            "101,2023-01-13,ALT,44",
            "102,2023-01-12,GLUC,90",
            "102,2023-01-15,GLUC,91",
            "102,2023-01-15,ALT,39",
            "103,2023-01-15,GLUC,77",
        ]
    )

    lb_dict_csv = "\n".join(
        [
            "lbtestcd,lbtest",
            "GLUC,Glucose",
            "ALT,Alanine Aminotransferase",
        ]
    )

    script_path = tmp_path / "hello_cosmos.sas"
    lb_path = tmp_path / "lb.csv"
    dict_path = tmp_path / "lb_dict.csv"
    out_dir = tmp_path / "out"

    script_path.write_text(script, encoding="utf-8")
    lb_path.write_text(lb_csv, encoding="utf-8")
    dict_path.write_text(lb_dict_csv, encoding="utf-8")

    check_args = [
        sys.executable,
        "-m",
        "sans",
        "check",
        str(script_path),
        "--legacy-sas",
        "--out",
        str(out_dir),
        "--tables",
        "lb,lb_dict",
    ]
    check_result = _run_cmd(check_args, _project_root())
    assert check_result.returncode == 0, check_result.stdout + check_result.stderr

    run_args = [
        sys.executable,
        "-m",
        "sans",
        "run",
        str(script_path),
        "--legacy-sas",
        "--out",
        str(out_dir),
        "--tables",
        f"lb={lb_path},lb_dict={dict_path}",
    ]
    run_result = _run_cmd(run_args, _project_root())
    assert run_result.returncode == 0, run_result.stdout + run_result.stderr

    named_csv = out_dir / "outputs" / "lb_named.csv"
    summary_csv = out_dir / "outputs" / "lb_summary.csv"
    assert named_csv.exists()
    assert summary_csv.exists()

    with named_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        named_rows = list(reader)

    assert reader.fieldnames == ["subjid", "lbdtc", "lbtestcd", "lbtest", "lbstresn"]
    assert len(named_rows) == 6
    assert named_rows == [
        {"subjid": "101", "lbdtc": "2023-01-13", "lbtestcd": "GLUC", "lbtest": "Glucose", "lbstresn": "110"},
        {"subjid": "101", "lbdtc": "2023-01-13", "lbtestcd": "ALT", "lbtest": "Alanine Aminotransferase", "lbstresn": "44"},
        {"subjid": "102", "lbdtc": "2023-01-12", "lbtestcd": "GLUC", "lbtest": "Glucose", "lbstresn": "90"},
        {"subjid": "102", "lbdtc": "2023-01-15", "lbtestcd": "GLUC", "lbtest": "Glucose", "lbstresn": "91"},
        {"subjid": "102", "lbdtc": "2023-01-15", "lbtestcd": "ALT", "lbtest": "Alanine Aminotransferase", "lbstresn": "39"},
        {"subjid": "103", "lbdtc": "2023-01-15", "lbtestcd": "GLUC", "lbtest": "Glucose", "lbstresn": "77"},
    ]

    with summary_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        summary_rows = list(reader)

    assert reader.fieldnames == ["subjid", "nrec", "mean_res"]
    expected_summary = {
        "101": {"nrec": 2, "mean_res": 77.0},
        "102": {"nrec": 3, "mean_res": (90 + 91 + 39) / 3.0},
        "103": {"nrec": 1, "mean_res": 77.0},
    }
    assert [row["subjid"] for row in summary_rows] == ["101", "102", "103"]
    for row in summary_rows:
        expected = expected_summary[row["subjid"]]
        assert int(row["nrec"]) == expected["nrec"]
        assert math.isclose(float(row["mean_res"]), expected["mean_res"], rel_tol=0.0, abs_tol=1e-6)
