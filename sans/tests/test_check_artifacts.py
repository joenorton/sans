import json
import shutil
from pathlib import Path
from uuid import uuid4

from sans.compiler import emit_check_artifacts


def _make_local_temp_dir() -> Path:
    base = Path(__file__).resolve().parent / ".tmp_artifacts"
    base.mkdir(parents=True, exist_ok=True)
    temp_dir = base / uuid4().hex
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def test_emit_check_artifacts_success():
    temp_dir = _make_local_temp_dir()
    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  x = a + 1;",
            "run;",
            "proc sort data=out out=sorted;",
            "  by x;",
            "run;",
        ]
    )
    try:
        irdoc, report = emit_check_artifacts(
            script,
            "test.sas",
            tables={"in"},
            out_dir=temp_dir,
        )

        assert report["status"] == "ok"
        plan = json.loads((temp_dir / "plan.ir.json").read_text(encoding="utf-8"))
        report_json = json.loads((temp_dir / "report.json").read_text(encoding="utf-8"))

        assert report_json["status"] == "ok"
        assert len(plan["steps"]) == 2
        assert plan["steps"][1]["op"] == "sort"
        assert plan["steps"][1]["params"] == {"by": [{"col": "x", "desc": False}]}
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_emit_check_artifacts_refusal():
    temp_dir = _make_local_temp_dir()
    script = "title 'hello';"
    try:
        irdoc, report = emit_check_artifacts(
            script,
            "test.sas",
            out_dir=temp_dir,
        )

        assert report["status"] == "refused"
        assert report["primary_error"]["code"] == "SANS_PARSE_UNSUPPORTED_STATEMENT"

        plan = json.loads((temp_dir / "plan.ir.json").read_text(encoding="utf-8"))
        assert len(plan["steps"]) == 1
        assert plan["steps"][0]["kind"] == "block"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_emit_check_artifacts_refusal_plan_matches_report():
    temp_dir = _make_local_temp_dir()
    script = "\n".join(
        [
            "proc sql;",
            "  create table out as select * from (select * from dm);",
            "quit;",
        ]
    )
    try:
        irdoc, report = emit_check_artifacts(
            script,
            "test.sas",
            out_dir=temp_dir,
        )
        assert report["status"] == "refused"
        primary = report["primary_error"]
        plan = json.loads((temp_dir / "plan.ir.json").read_text(encoding="utf-8"))
        assert plan["steps"][0]["kind"] == "block"
        assert plan["steps"][0]["code"] == primary["code"]
        assert plan["steps"][0]["loc"] == primary["loc"]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_emit_check_artifacts_refusal_error_shape():
    temp_dir = _make_local_temp_dir()
    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  lag(value);",
            "run;",
        ]
    )
    try:
        _, report = emit_check_artifacts(
            script,
            "test.sas",
            tables={"in"},
            out_dir=temp_dir,
        )
        assert report["status"] == "refused"
        primary = report["primary_error"]
        assert set(primary.keys()) >= {"code", "message", "loc"}
        assert primary["loc"]["file"] == "test.sas"
        assert primary["loc"]["line_start"] == 1
        assert "lag" in primary["message"]
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
