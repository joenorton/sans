import csv
import math
import subprocess
import sys
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_cmd(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True)


def test_hello_multiverse_end_to_end(tmp_path: Path):
    script = "\n".join(
        [
            "proc format;",
            "  value $sev",
            "    \"MILD\"=\"1\"",
            "    \"MODERATE\"=\"2\"",
            "    \"SEVERE\"=\"3\"",
            "    other=\"\";",
            "run;",
            "",
            "proc sort data=ae out=ae_s nodupkey;",
            "  by subjid aeterm aestdtc;",
            "run;",
            "",
            "data ae_m;",
            "  set ae_s;",
            "  aesevn = input(put(aesev, $sev.), best.);",
            "  if aesevn = . then aesevn = .;",
            "run;",
            "",
            "proc summary data=ae_m nway;",
            "  class subjid;",
            "  var aesevn;",
            "  output out=ae_sum mean= / autoname;",
            "run;",
        ]
    )

    ae_csv = "\n".join(
        [
            "subjid,aeterm,aestdtc,aesev",
            "101,HEADACHE,2023-01-12,MILD",
            "101,HEADACHE,2023-01-12,MILD",
            "101,NAUSEA,2023-01-15,MODERATE",
            "102,DIZZINESS,2023-01-13,MILD",
            "102,DIZZINESS,2023-01-13,SEVERE",
        ]
    )

    script_path = tmp_path / "hello_multiverse.sas"
    ae_path = tmp_path / "ae.csv"
    out_dir = tmp_path / "out"

    script_path.write_text(script, encoding="utf-8")
    ae_path.write_text(ae_csv, encoding="utf-8")

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
        "ae",
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
        f"ae={ae_path}",
    ]
    run_result = _run_cmd(run_args, _project_root())
    assert run_result.returncode == 0, run_result.stdout + run_result.stderr

    ae_s_csv = out_dir / "outputs" / "ae_s.csv"
    ae_sum_csv = out_dir / "outputs" / "ae_sum.csv"
    assert ae_s_csv.exists()
    assert ae_sum_csv.exists()

    with ae_s_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        ae_s_rows = list(reader)

    assert len(ae_s_rows) == 3
    dizziness_rows = [r for r in ae_s_rows if r["subjid"] == "102" and r["aeterm"] == "DIZZINESS"]
    assert len(dizziness_rows) == 1
    assert dizziness_rows[0]["aesev"] == "MILD"

    with ae_sum_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        sum_rows = list(reader)

    assert reader.fieldnames == ["subjid", "aesevn_mean"]
    expected = {"101": 1.5, "102": 1.0}
    assert [row["subjid"] for row in sum_rows] == ["101", "102"]
    for row in sum_rows:
        assert math.isclose(float(row["aesevn_mean"]), expected[row["subjid"]], rel_tol=0.0, abs_tol=1e-6)
