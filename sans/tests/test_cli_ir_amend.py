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


def _base_ir() -> dict:
    return {
        "version": "0.1",
        "datasources": {"lb": {"kind": "csv", "path": "lb.csv"}},
        "steps": [
            {
                "id": "ds:lb",
                "op": "datasource",
                "inputs": [],
                "outputs": ["__datasource__lb"],
                "params": {"name": "lb", "kind": "csv", "path": "lb.csv"},
            },
            {
                "id": "out:t1",
                "op": "identity",
                "inputs": ["__datasource__lb"],
                "outputs": ["t1"],
                "params": {},
            },
            {
                "id": "out:t2",
                "op": "compute",
                "inputs": ["t1"],
                "outputs": ["t2"],
                "params": {"assignments": [{"target": "x", "expr": {"type": "lit", "value": 2}}]},
            },
            {
                "id": "out:t2:save",
                "op": "save",
                "inputs": ["t2"],
                "outputs": [],
                "params": {"path": "t2.csv"},
            },
        ],
    }


def test_cli_ir_amend_exit_codes_and_shape():
    temp_dir = _make_local_temp_dir()
    try:
        ir_path = temp_dir / "in.sans.ir.json"
        ir_path.write_text(json.dumps(_base_ir()), encoding="utf-8")

        ok_req_path = temp_dir / "ok.req.json"
        ok_req_path.write_text(
            json.dumps(
                {
                    "format": "sans.amendment_request",
                    "version": 1,
                    "contract_version": "0.1",
                    "policy": {},
                    "ops": [
                        {
                            "op_id": "op1",
                            "kind": "set_params",
                            "selector": {"step_id": "out:t2", "path": "/assignments/0/expr/value"},
                            "params": {"value": 7},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        ok_out_path = temp_dir / "ok.out.json"
        ok_run = subprocess.run(
            [
                sys.executable,
                "-m",
                "sans",
                "ir-amend",
                "--ir",
                str(ir_path),
                "--req",
                str(ok_req_path),
                "--out",
                str(ok_out_path),
            ],
            cwd=_project_root(),
            capture_output=True,
            text=True,
        )
        assert ok_run.returncode == 0
        ok_payload = json.loads(ok_out_path.read_text(encoding="utf-8"))
        assert set(ok_payload.keys()) == {
            "status",
            "diagnostics",
            "diff_structural",
            "diff_assertions",
            "ir_out",
        }
        assert ok_payload["status"] == "ok"

        bad_req_path = temp_dir / "bad.req.json"
        bad_req_path.write_text(
            json.dumps(
                {
                    "format": "sans.amendment_request",
                    "version": 1,
                    "contract_version": "0.1",
                    "policy": {},
                    "ops": [
                        {
                            "op_id": "op1",
                            "kind": "set_params",
                            "selector": {"step_id": "out:t2", "path": "/assignments"},
                            "params": {"value": "not-a-list"},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        bad_out_path = temp_dir / "bad.out.json"
        bad_run = subprocess.run(
            [
                sys.executable,
                "-m",
                "sans",
                "ir-amend",
                "--ir",
                str(ir_path),
                "--req",
                str(bad_req_path),
                "--out",
                str(bad_out_path),
            ],
            cwd=_project_root(),
            capture_output=True,
            text=True,
        )
        assert bad_run.returncode == 1
        bad_payload = json.loads(bad_out_path.read_text(encoding="utf-8"))
        assert set(bad_payload.keys()) == {
            "status",
            "diagnostics",
            "diff_structural",
            "diff_assertions",
        }
        assert bad_payload["status"] == "refused"
        assert bad_payload["diagnostics"]["refusals"]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

