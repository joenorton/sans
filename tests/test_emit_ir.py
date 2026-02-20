import json
import subprocess
import sys
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1] / "sans"


def _run_cmd(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True)


def _write_emit_ir_fixture(tmp_path: Path) -> tuple[Path, Path]:
    script_dir = tmp_path / "scripts"
    inputs_dir = tmp_path / "inputs"
    script_dir.mkdir(parents=True, exist_ok=True)
    inputs_dir.mkdir(parents=True, exist_ok=True)

    (inputs_dir / "dm.csv").write_text(
        "USUBJID,AGE\nS001,34\nS002,41\n",
        encoding="utf-8",
    )
    script_path = script_dir / "baseline.expanded.sans"
    script_path.write_text(
        "\n".join(
            [
                "# sans 0.1",
                'datasource dm = csv("dm.csv", columns(USUBJID:string, AGE:int))',
                "table baseline = from(dm) select USUBJID, AGE",
                'save baseline to "baseline.csv"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return script_path, inputs_dir


def test_emit_ir_emits_strict_valid_sans_ir(tmp_path: Path):
    script_path, inputs_dir = _write_emit_ir_fixture(tmp_path)
    out_path = tmp_path / "baseline.sans.ir"

    emit = _run_cmd(
        [
            sys.executable,
            "-m",
            "sans",
            "emit-ir",
            str(script_path),
            "--out",
            str(out_path),
            "--cwd",
            str(inputs_dir),
        ],
        cwd=_project_root(),
    )
    assert emit.returncode == 0, emit.stdout + emit.stderr
    assert out_path.exists()

    validate = _run_cmd(
        [
            sys.executable,
            "-m",
            "sans",
            "ir-validate",
            "--strict",
            str(out_path),
        ],
        cwd=_project_root(),
    )
    assert validate.returncode == 0, validate.stdout + validate.stderr


def test_emit_ir_is_byte_deterministic(tmp_path: Path):
    script_path, inputs_dir = _write_emit_ir_fixture(tmp_path)
    out_a = tmp_path / "a.sans.ir"
    out_b = tmp_path / "b.sans.ir"

    emit_a = _run_cmd(
        [
            sys.executable,
            "-m",
            "sans",
            "emit-ir",
            str(script_path),
            "--out",
            str(out_a),
            "--cwd",
            str(inputs_dir),
        ],
        cwd=_project_root(),
    )
    emit_b = _run_cmd(
        [
            sys.executable,
            "-m",
            "sans",
            "emit-ir",
            str(script_path),
            "--out",
            str(out_b),
            "--cwd",
            str(inputs_dir),
        ],
        cwd=_project_root(),
    )

    assert emit_a.returncode == 0, emit_a.stdout + emit_a.stderr
    assert emit_b.returncode == 0, emit_b.stdout + emit_b.stderr
    assert out_a.read_bytes() == out_b.read_bytes()


def test_emit_ir_supports_cwd_and_json_output(tmp_path: Path):
    script_path, inputs_dir = _write_emit_ir_fixture(tmp_path)
    out_path = tmp_path / "cwd.sans.ir"

    emit = _run_cmd(
        [
            sys.executable,
            "-m",
            "sans",
            "emit-ir",
            str(script_path),
            "--out",
            str(out_path),
            "--cwd",
            str(inputs_dir),
            "--json",
        ],
        cwd=_project_root(),
    )

    assert emit.returncode == 0, emit.stdout + emit.stderr
    payload = json.loads(emit.stdout)
    assert payload["ok"] is True
    assert payload["out_path"] == str(out_path.resolve())

    emitted = json.loads(out_path.read_text(encoding="utf-8"))
    assert emitted["datasources"]["dm"]["path"] == "dm.csv"


def test_emit_ir_with_schema_lock_succeeds(tmp_path: Path):
    """emit-ir with untyped csv ref succeeds when --schema-lock supplies types."""
    script_path = tmp_path / "demo.sans"
    script_path.write_text(
        "# sans 0.1\n"
        'datasource lb = csv("lb.csv")\n'
        "table t = from(lb) select x, y\n"
        'save t to "out.csv"\n',
        encoding="utf-8",
    )
    lock_path = tmp_path / "demo.schema.lock.json"
    lock_path.write_text(
        json.dumps(
            {
                "schema_lock_version": 1,
                "created_by": {"sans_version": "0.1", "git_sha": ""},
                "datasources": [
                    {
                        "name": "lb",
                        "kind": "csv",
                        "path": "lb.csv",
                        "columns": [
                            {"name": "x", "type": "int"},
                            {"name": "y", "type": "int"},
                        ],
                        "rules": {"extra_columns": "ignore", "missing_columns": "error"},
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    out_path = tmp_path / "out.sans.ir"

    emit = _run_cmd(
        [
            sys.executable,
            "-m",
            "sans",
            "emit-ir",
            str(script_path),
            "--out",
            str(out_path),
            "--schema-lock",
            "demo.schema.lock.json",
        ],
        cwd=_project_root(),
    )
    assert emit.returncode == 0, emit.stdout + emit.stderr
    assert out_path.exists()
    emitted = json.loads(out_path.read_text(encoding="utf-8"))
    assert "lb" in emitted.get("datasources", {})
