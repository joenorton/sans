import json
from pathlib import Path

from sans.compiler import emit_check_artifacts


def test_unknown_arithmetic_fails(tmp_path: Path):
    csv_path = Path("fixtures/types/data/simple.csv").resolve()
    script = "\n".join(
        [
            "# sans 0.1",
            f'datasource src = csv("{csv_path.as_posix()}", columns(a, b))',
            "table out = from(src) do",
            "  derive(x = a + 1)",
            "end",
        ]
    )
    _, report = emit_check_artifacts(
        script,
        "unknown_arith.sans",
        out_dir=tmp_path,
        tables=set(),
        strict=True,
    )
    assert report["status"] == "refused"
    assert report["primary_error"]["code"] == "E_TYPE_UNKNOWN"


def test_unknown_null_comparison_ok(tmp_path: Path):
    # In strict mode, ingress columns must be typed (pin or lock), so we use pinned types.
    # This test verifies that "column == null" is accepted for typed columns.
    csv_path = Path("fixtures/types/data/simple.csv").resolve()
    script = "\n".join(
        [
            "# sans 0.1",
            f'datasource src = csv("{csv_path.as_posix()}", columns(a:int, b:int))',
            "table out = from(src) do",
            "  filter(a == null)",
            "  select a, b",
            "end",
        ]
    )
    _, report = emit_check_artifacts(
        script,
        "unknown_null_ok.sans",
        out_dir=tmp_path,
        tables=set(),
        strict=True,
    )
    assert report["status"] == "ok"
    schema_path = tmp_path / "artifacts" / "schema.evidence.json"
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    assert data["tables"]["out"] == {"a": "int", "b": "int"}
