import csv
import subprocess
import sys
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_cmd(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True)


def test_hello_galaxy_end_to_end(tmp_path: Path):
    script = "\n".join(
        [
            "proc sort data=dm out=dm_s;",
            "  by subjid;",
            "run;",
            "",
            "proc sort data=ex out=ex_s;",
            "  by subjid exstdtc;",
            "run;",
            "",
            "proc sort data=lb out=lb_s;",
            "  by subjid lbdtc;",
            "run;",
            "",
            "data ex_first;",
            "  set ex_s;",
            "  by subjid;",
            "  if first.subjid then output;",
            "  keep subjid exstdtc;",
            "run;",
            "",
            "data subj;",
            "  merge dm_s(in=indm) ex_first(in=inex);",
            "  by subjid;",
            "  if indm and inex;",
            "  keep subjid siteid sex race exstdtc;",
            "run;",
            "",
            "data lb_pre;",
            "  merge lb_s(in=inlb) subj(in=insubj);",
            "  by subjid;",
            "  if inlb and insubj;",
            "  if lbdtc <= exstdtc;",
            "run;",
            "",
            "proc sort data=lb_pre out=lb_pre_s;",
            "  by subjid lbtestcd lbdtc;",
            "run;",
            "",
            "data base;",
            "  set lb_pre_s;",
            "  by subjid lbtestcd;",
            "  retain baseval;",
            "  if first.lbtestcd then baseval = .;",
            "  baseval = lbstresn;",
            "  if last.lbtestcd then output;",
            "  keep subjid lbtestcd baseval;",
            "run;",
            "",
            "data lb_final;",
            "  merge lb_s(in=inlb) subj(in=insubj) base(in=inbase);",
            "  by subjid;",
            "  if inlb and insubj and inbase;",
            "  if lbdtc > exstdtc;",
            "",
            "  chg = lbstresn - baseval;",
            "  if baseval ne 0 then pchg = (chg / baseval) * 100;",
            "  else pchg = .;",
            "",
            "  keep subjid siteid sex race exstdtc lbdtc lbtestcd lbstresn baseval chg pchg;",
            "run;",
        ]
    )

    dm_csv = "\n".join(
        [
            "subjid,siteid,sex,race",
            "101,001,M,WHITE",
            "102,001,F,BLACK OR AFRICAN AMERICAN",
            "103,002,M,ASIAN",
        ]
    )
    ex_csv = "\n".join(
        [
            "subjid,exstdtc",
            "101,2023-01-11",
            "102,2023-01-12",
        ]
    )
    lb_csv = "\n".join(
        [
            "subjid,lbdtc,lbtestcd,lbstresn",
            "101,2023-01-10,GLUC,95",
            "101,2023-01-11,GLUC,96",
            "101,2023-01-13,GLUC,110",
            "101,2023-01-20,GLUC,100",
            "102,2023-01-10,GLUC,88",
            "102,2023-01-12,GLUC,90",
            "102,2023-01-15,GLUC,91",
            "103,2023-01-10,GLUC,77",
        ]
    )

    script_path = tmp_path / "hello_galaxy.sas"
    dm_path = tmp_path / "dm.csv"
    ex_path = tmp_path / "ex.csv"
    lb_path = tmp_path / "lb.csv"
    out_dir = tmp_path / "out"

    script_path.write_text(script, encoding="utf-8")
    dm_path.write_text(dm_csv, encoding="utf-8")
    ex_path.write_text(ex_csv, encoding="utf-8")
    lb_path.write_text(lb_csv, encoding="utf-8")

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
        "dm,ex,lb",
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
        f"dm={dm_path},ex={ex_path},lb={lb_path}",
    ]
    run_result = _run_cmd(run_args, _project_root())
    assert run_result.returncode == 0, run_result.stdout + run_result.stderr

    out_csv = out_dir / "outputs" / "lb_final.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    expected_columns = [
        "subjid",
        "siteid",
        "sex",
        "race",
        "exstdtc",
        "lbdtc",
        "lbtestcd",
        "lbstresn",
        "baseval",
        "chg",
        "pchg",
    ]
    assert reader.fieldnames == expected_columns
    assert len(rows) == 3

    expected = {
        (101, "2023-01-13"): {
            "baseval": 96.0,
            "chg": 14.0,
            "pchg": 14.5833333333,
            "lbstresn": 110.0,
            "siteid": "001",
            "sex": "M",
            "race": "WHITE",
            "exstdtc": "2023-01-11",
        },
        (101, "2023-01-20"): {
            "baseval": 96.0,
            "chg": 4.0,
            "pchg": 4.1666666667,
            "lbstresn": 100.0,
            "siteid": "001",
            "sex": "M",
            "race": "WHITE",
            "exstdtc": "2023-01-11",
        },
        (102, "2023-01-15"): {
            "baseval": 90.0,
            "chg": 1.0,
            "pchg": 1.1111111111,
            "lbstresn": 91.0,
            "siteid": "001",
            "sex": "F",
            "race": "BLACK OR AFRICAN AMERICAN",
            "exstdtc": "2023-01-12",
        },
    }

    seen_subjids = set()
    for row in rows:
        subjid = int(row["subjid"])
        lbdtc = row["lbdtc"]
        key = (subjid, lbdtc)
        assert key in expected
        exp = expected[key]

        assert row["siteid"] == exp["siteid"]
        assert row["sex"] == exp["sex"]
        assert row["race"] == exp["race"]
        assert row["exstdtc"] == exp["exstdtc"]
        assert row["lbtestcd"] == "GLUC"

        assert abs(float(row["baseval"]) - exp["baseval"]) < 1e-6
        assert abs(float(row["chg"]) - exp["chg"]) < 1e-6
        assert abs(float(row["pchg"]) - exp["pchg"]) < 1e-6
        assert abs(float(row["lbstresn"]) - exp["lbstresn"]) < 1e-6

        seen_subjids.add(subjid)

    assert 103 not in seen_subjids
