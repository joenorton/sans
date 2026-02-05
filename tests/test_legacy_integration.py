from pathlib import Path

from sans.runtime import run_script


def test_legacy_sas_flag_enables_translation(tmp_path: Path) -> None:
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("a\n1\n2\n", encoding="utf-8")
    script = "data out; set in; if a eq 1; run;"

    out_strict = tmp_path / "out_strict"
    report_strict = run_script(
        script,
        "s.sas",
        {"in": str(in_csv)},
        out_strict,
        legacy_sas=False,
    )
    assert report_strict["status"] == "refused"

    out_legacy = tmp_path / "out_legacy"
    report_legacy = run_script(
        script,
        "s.sas",
        {"in": str(in_csv)},
        out_legacy,
        legacy_sas=True,
    )
    assert report_legacy["status"] == "ok"
