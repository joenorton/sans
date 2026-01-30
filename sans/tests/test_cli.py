import json
import shutil
import subprocess
import sys
from pathlib import Path
from uuid import uuid4


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _make_local_temp_dir() -> Path:
    base = Path(__file__).resolve().parent / ".tmp_cli"
    base.mkdir(parents=True, exist_ok=True)
    temp_dir = base / uuid4().hex
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def _run_check(script_text: str, out_dir: Path, tables: list[str] | None = None, strict: bool = True):
    script_path = out_dir / "script.sas"
    script_path.write_text(script_text, encoding="utf-8")

    args = [
        sys.executable,
        "-m",
        "sans",
        "check",
        str(script_path),
        "--out",
        str(out_dir),
    ]
    if tables:
        args += ["--tables", ",".join(tables)]
    if not strict:
        args.append("--no-strict")

    return subprocess.run(
        args,
        cwd=_project_root(),
        capture_output=True,
        text=True,
    )


def test_cli_check_ok_exit_code():
    temp_dir = _make_local_temp_dir()
    try:
        script = "\n".join(
            [
                "data out;",
                "  set in;",
                "  x = a + 1;",
                "run;",
            ]
        )
        result = _run_check(script, temp_dir, tables=["in"])
        assert result.returncode == 0
        assert "ok: wrote plan.ir.json report.json" in result.stdout

        report = json.loads((temp_dir / "report.json").read_text(encoding="utf-8"))
        assert report["status"] == "ok"
        assert report["exit_code_bucket"] == 0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_cli_check_refused_parse_exit_code():
    temp_dir = _make_local_temp_dir()
    try:
        script = "\n".join(
            [
                "proc sql;",
                "  select * from dm;",
                "quit;",
            ]
        )
        result = _run_check(script, temp_dir)
        assert result.returncode == 30
        assert "SANS_PARSE_SQL_DETECTED" in result.stdout

        report = json.loads((temp_dir / "report.json").read_text(encoding="utf-8"))
        assert report["status"] == "refused"
        assert report["exit_code_bucket"] == 30
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_cli_check_refused_validate_exit_code():
    temp_dir = _make_local_temp_dir()
    try:
        script = "\n".join(
            [
                "data out;",
                "  set missing;",
                "run;",
            ]
        )
        result = _run_check(script, temp_dir)
        assert result.returncode == 31
        assert "SANS_VALIDATE_TABLE_UNDEFINED" in result.stdout

        report = json.loads((temp_dir / "report.json").read_text(encoding="utf-8"))
        assert report["status"] == "refused"
        assert report["exit_code_bucket"] == 31
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
