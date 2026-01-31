import csv
import subprocess
import sys
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_cmd(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True)


def test_hello_universe_end_to_end(tmp_path: Path):
    script = "\n".join(
        [
            "proc sort data=lb out=lb_s;",
            "  by subjid lbdtc;",
            "run;",
            "",
            "data lb_sub;",
            "  set lb_s(where=(lbdtc >= \"2023-01-12\") keep=subjid lbdtc lbtestcd lbstresn);",
            "run;",
            "",
            "proc transpose data=lb_sub out=lb_wide;",
            "  by subjid;",
            "  id lbtestcd;",
            "  var lbstresn;",
            "run;",
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
        ]
    )

    script_path = tmp_path / "hello_universe.sas"
    lb_path = tmp_path / "lb.csv"
    out_dir = tmp_path / "out"

    script_path.write_text(script, encoding="utf-8")
    lb_path.write_text(lb_csv, encoding="utf-8")

    check_args = [
        sys.executable,
        "-m",
        "sans",
        "check",
        str(script_path),
        "--out",
        str(out_dir),
        "--tables",
        "lb",
    ]
    check_result = _run_cmd(check_args, _project_root())
    assert check_result.returncode == 0, check_result.stdout + check_result.stderr

    run_args = [
        sys.executable,
        "-m",
        "sans",
        "run",
        str(script_path),
        "--out",
        str(out_dir),
        "--tables",
        f"lb={lb_path}",
    ]
    run_result = _run_cmd(run_args, _project_root())
    assert run_result.returncode == 0, run_result.stdout + run_result.stderr

    out_csv = out_dir / "lb_wide.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert reader.fieldnames == ["subjid", "GLUC", "ALT"]
    assert len(rows) == 2

    expected = {
        "101": {"GLUC": "110", "ALT": "44"},
        "102": {"GLUC": "91", "ALT": "39"},
    }
    for row in rows:
        subjid = row["subjid"]
        assert subjid in expected
        assert row["GLUC"] == expected[subjid]["GLUC"]
        assert row["ALT"] == expected[subjid]["ALT"]
