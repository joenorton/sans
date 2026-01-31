import json
import shutil
import subprocess
import sys
from pathlib import Path
from uuid import uuid4


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _make_local_temp_dir() -> Path:
    base = Path(__file__).resolve().parent / ".tmp_validate"
    base.mkdir(parents=True, exist_ok=True)
    temp_dir = base / uuid4().hex
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    path.write_text("\n".join([",".join(row) for row in rows]), encoding="utf-8")


def test_validate_sdtm_ok():
    temp_dir = _make_local_temp_dir()
    try:
        dm = temp_dir / "dm.csv"
        ae = temp_dir / "ae.csv"
        lb = temp_dir / "lb.csv"
        out_dir = temp_dir / "out"

        _write_csv(
            dm,
            [
                ["DOMAIN", "USUBJID", "SUBJID", "SITEID", "SEX", "RACE"],
                ["DM", "STUDY-001", "001", "001", "M", "WHITE"],
            ],
        )
        _write_csv(
            ae,
            [
                ["DOMAIN", "USUBJID", "AEDECOD", "AESTDTC"],
                ["AE", "STUDY-001", "HEADACHE", "2023-01-01"],
            ],
        )
        _write_csv(
            lb,
            [
                ["DOMAIN", "USUBJID", "LBTESTCD", "LBDTC", "LBSTRESN"],
                ["LB", "STUDY-001", "GLUC", "2023-01-02", "95"],
            ],
        )

        args = [
            sys.executable,
            "-m",
            "sans",
            "validate",
            "--profile",
            "sdtm",
            "--out",
            str(out_dir),
            "--tables",
            f"dm={dm},ae={ae},lb={lb}",
        ]

        result = subprocess.run(args, cwd=_project_root(), capture_output=True, text=True)
        assert result.returncode == 0

        report = json.loads((out_dir / "validation.report.json").read_text(encoding="utf-8"))
        assert report["status"] == "ok"
        assert report["profile"] == "sdtm"
        assert report["summary"]["errors"] == 0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_validate_sdtm_reports_errors():
    temp_dir = _make_local_temp_dir()
    try:
        dm = temp_dir / "dm.csv"
        out_dir = temp_dir / "out"

        _write_csv(
            dm,
            [
                ["DOMAIN", "USUBJID", "SUBJID", "SITEID", "SEX", "RACE"],
                ["XX", "", "001", "001", "M", "WHITE"],
            ],
        )

        args = [
            sys.executable,
            "-m",
            "sans",
            "validate",
            "--profile",
            "sdtm",
            "--out",
            str(out_dir),
            "--tables",
            f"dm={dm}",
        ]

        result = subprocess.run(args, cwd=_project_root(), capture_output=True, text=True)
        assert result.returncode == 31

        report = json.loads((out_dir / "validation.report.json").read_text(encoding="utf-8"))
        assert report["status"] == "failed"
        assert report["profile"] == "sdtm"
        assert report["summary"]["errors"] >= 2

        diagnostics = report["diagnostics"]
        assert all("code" in d and "message" in d and "table" in d for d in diagnostics)
        assert any(d["code"] == "SDTM_DOMAIN_VALUE_INVALID" for d in diagnostics)
        assert any(d["code"] == "SDTM_USUBJID_MISSING" for d in diagnostics)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_validate_sdtm_unsupported_profile():
    temp_dir = _make_local_temp_dir()
    try:
        out_dir = temp_dir / "out"
        args = [
            sys.executable,
            "-m",
            "sans",
            "validate",
            "--profile",
            "other",
            "--out",
            str(out_dir),
        ]

        result = subprocess.run(args, cwd=_project_root(), capture_output=True, text=True)
        assert result.returncode == 31

        report = json.loads((out_dir / "validation.report.json").read_text(encoding="utf-8"))
        assert report["status"] == "failed"
        assert report["diagnostics"][0]["code"] == "SANS_VALIDATE_PROFILE_UNSUPPORTED"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_validate_sdtm_report_written_on_failure():
    temp_dir = _make_local_temp_dir()
    try:
        out_dir = temp_dir / "out"
        args = [
            sys.executable,
            "-m",
            "sans",
            "validate",
            "--profile",
            "sdtm",
            "--out",
            str(out_dir),
            "--tables",
            "dm=missing.csv",
        ]

        result = subprocess.run(args, cwd=_project_root(), capture_output=True, text=True)
        assert result.returncode == 31

        report_path = out_dir / "validation.report.json"
        assert report_path.exists()
        report = json.loads(report_path.read_text(encoding="utf-8"))
        assert report["status"] == "failed"
        assert report["summary"]["errors"] >= 1
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
