import json
from pathlib import Path

import pytest

from sans.compiler import emit_check_artifacts


def _schema_path(out_dir: Path) -> Path:
    return out_dir / "artifacts" / "schema.evidence.json"


def test_schema_evidence_emitted(tmp_path: Path):
    csv_path = Path("fixtures/types/data/simple.csv").resolve()
    script = "\n".join(
        [
            "# sans 0.1",
            f'datasource src = csv("{csv_path.as_posix()}", columns(a:int, b:int))',
            "table out = from(src) do",
            "  derive(i = 1 + 2, d = 1 / 2, s = \"x\", flag = 1 == 1, mixed = if(1 == 1, 1, 2.0), n = null)",
            "  filter(flag)",
            "end",
            "save out to \"out.csv\"",
        ]
    )
    _, report = emit_check_artifacts(
        script,
        "types.sans",
        out_dir=tmp_path,
        tables=set(),
        strict=True,
    )
    assert report["status"] == "ok"
    schema_path = _schema_path(tmp_path)
    assert schema_path.exists()
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    expected = {
        "a": "int",
        "b": "int",
        "i": "int",
        "d": "decimal",
        "s": "string",
        "flag": "bool",
        "mixed": "decimal",
        "n": "null",
    }
    assert data["tables"]["out"] == expected

    # Deterministic JSON ordering
    assert schema_path.read_text(encoding="utf-8").strip() == json.dumps(
        data, indent=2, sort_keys=True
    ).strip()


def test_drop_excluded_from_schema_evidence(tmp_path: Path):
    """After drop step, schema.evidence.json excludes dropped columns."""
    script = "\n".join(
        [
            "# sans 0.1",
            "datasource src = inline_csv columns(a:int, b:int, c:int) do",
            "  a,b,c",
            "  1,2,3",
            "end",
            "table out = from(src) do",
            "  drop b",
            "end",
            "save out to \"out.csv\"",
        ]
    )
    from sans.compiler import emit_check_artifacts
    _, report = emit_check_artifacts(
        script,
        "drop_evidence.sans",
        out_dir=tmp_path,
        tables=set(),
        strict=True,
    )
    assert report["status"] == "ok"
    schema_path = _schema_path(tmp_path)
    assert schema_path.exists()
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    assert "out" in data["tables"]
    out_cols = set(data["tables"]["out"].keys())
    assert out_cols == {"a", "c"}
    assert "b" not in out_cols


def test_filter_requires_bool(tmp_path: Path):
    csv_path = Path("fixtures/types/data/simple.csv").resolve()
    script = "\n".join(
        [
            "# sans 0.1",
            f'datasource src = csv("{csv_path.as_posix()}", columns(a:int, b:int))',
            "table out = from(src) do",
            "  filter(a)",
            "end",
        ]
    )
    _, report = emit_check_artifacts(
        script,
        "bad_filter.sans",
        out_dir=tmp_path,
        tables=set(),
        strict=True,
    )
    assert report["status"] == "refused"
    assert report["primary_error"]["code"] == "E_TYPE"
