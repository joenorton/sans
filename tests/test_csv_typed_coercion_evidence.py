import json
from pathlib import Path

from sans.runtime import run_script


def _run(script: str, out_dir: Path) -> tuple[dict, dict]:
    report = run_script(
        text=script,
        file_name="coerce.sans",
        bindings={},
        out_dir=out_dir,
        strict=True,
        legacy_sas=True,
    )
    evidence_path = out_dir / "artifacts" / "runtime.evidence.json"
    assert evidence_path.exists()
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    return report, evidence


def _col(diag: dict, name: str) -> dict:
    return next(c for c in diag.get("columns", []) if c.get("column") == name)


def test_invalid_int_coercion_evidence(tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("a\n1\nx\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource src = csv("{csv_path.as_posix()}", columns(a:int))\n'
        "table out = from(src) select a\n"
        "save out to \"out.csv\"\n"
    )
    report, evidence = _run(script, tmp_path / "out")
    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "E_CSV_COERCE"

    diag = evidence["coercion_diagnostics"][0]
    col = _col(diag, "a")
    assert col["expected_type"] == "int"
    assert col["failure_count"] == 1
    assert col["sample_row_numbers"] == [2]
    assert col["sample_raw_values"] == ["x"]
    assert col["failure_reason"] == "invalid_int"


def test_invalid_decimal_coercion_evidence(tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("a\n1.2\nabc\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource src = csv("{csv_path.as_posix()}", columns(a:decimal))\n'
        "table out = from(src) select a\n"
    )
    report, evidence = _run(script, tmp_path / "out")
    assert report["status"] == "failed"
    diag = evidence["coercion_diagnostics"][0]
    col = _col(diag, "a")
    assert col["expected_type"] == "decimal"
    assert col["failure_reason"] == "invalid_decimal"


def test_invalid_bool_coercion_evidence(tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("a\ntrue\nmaybe\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource src = csv("{csv_path.as_posix()}", columns(a:bool))\n'
        "table out = from(src) select a\n"
    )
    report, evidence = _run(script, tmp_path / "out")
    assert report["status"] == "failed"
    diag = evidence["coercion_diagnostics"][0]
    col = _col(diag, "a")
    assert col["expected_type"] == "bool"
    assert col["failure_reason"] == "invalid_bool"


def test_empty_tokens_treated_as_null(tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("a\n \nx\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource src = csv("{csv_path.as_posix()}", columns(a:int))\n'
        "table out = from(src) select a\n"
    )
    report, evidence = _run(script, tmp_path / "out")
    assert report["status"] == "failed"
    diag = evidence["coercion_diagnostics"][0]
    col = _col(diag, "a")
    # Empty/whitespace token is treated as null; only the 'x' row fails.
    assert col["failure_count"] == 1
    assert col["sample_row_numbers"] == [2]
    assert col["sample_raw_values"] == ["x"]


def test_multi_column_failures_ordering(tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("a,b\nx,1.2\n2,abc\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource src = csv("{csv_path.as_posix()}", columns(a:int, b:decimal))\n'
        "table out = from(src) select a, b\n"
    )
    report, evidence = _run(script, tmp_path / "out")
    assert report["status"] == "failed"
    diag = evidence["coercion_diagnostics"][0]
    assert [c["column"] for c in diag["columns"]] == ["a", "b"]
    assert _col(diag, "a")["failure_reason"] == "invalid_int"
    assert _col(diag, "b")["failure_reason"] == "invalid_decimal"


def test_coercion_diagnostics_deterministic(tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text("a\n1\nx\n", encoding="utf-8")
    script = (
        "# sans 0.1\n"
        f'datasource src = csv("{csv_path.as_posix()}", columns(a:int))\n'
        "table out = from(src) select a\n"
    )
    report1, evidence1 = _run(script, tmp_path / "run1")
    report2, evidence2 = _run(script, tmp_path / "run2")
    assert report1["status"] == "failed"
    assert report2["status"] == "failed"
    assert evidence1["coercion_diagnostics"] == evidence2["coercion_diagnostics"]
