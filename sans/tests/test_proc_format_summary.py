import csv
from pathlib import Path

import pytest

from sans.compiler import check_script
from sans.ir import UnknownBlockStep
from sans.runtime import run_script


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def test_proc_format_put_mapping(tmp_path: Path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "sev"],
            ["1", "MILD"],
            ["2", "SEVERE"],
            ["3", "UNKNOWN"],
        ],
    )

    script = "\n".join(
        [
            "proc format;",
            "  value $sev \"MILD\"=\"1\" \"SEVERE\"=\"3\" other=\"\";",
            "run;",
            "data out;",
            "  set in;",
            "  sev_m = put(sev, $sev.);",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="fmt.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "sev", "sev_m"]
    assert rows[1:] == [
        ["1", "MILD", "1"],
        ["2", "SEVERE", "3"],
        ["3", "UNKNOWN", ""],
    ]


def test_proc_format_unknown_format_fails(tmp_path: Path):
    in_csv = tmp_path / "in.csv"
    _write_csv(in_csv, [["id", "sev"], ["1", "MILD"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  sev_m = put(sev, $missing.);",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="fmt_missing.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_FORMAT_UNDEFINED"


def test_input_best_informat_parses_numeric(tmp_path: Path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["1", "10.5"],
            ["2", ""],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  num = input(val, best.);",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="input_best.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "val", "num"]
    assert rows[1:] == [
        ["1", "10.5", "10.5"],
        ["2", "", ""],
    ]


def test_input_unsupported_informat_errors(tmp_path: Path):
    in_csv = tmp_path / "in.csv"
    _write_csv(in_csv, [["id", "val"], ["1", "10"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  num = input(val, date.);",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="input_bad.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_INFORMAT_UNSUPPORTED"


def test_proc_summary_mean(tmp_path: Path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["A", "1"],
            ["A", "3"],
            ["B", "2"],
        ],
    )

    script = "\n".join(
        [
            "proc summary data=in nway;",
            "  class id;",
            "  var val;",
            "  output out=out mean= / autoname;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="summary.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "val_mean"]
    assert rows[1:] == [
        ["A", "2.0"],
        ["B", "2.0"],
    ]


def test_proc_sort_nodupkey_first_wins(tmp_path: Path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["1", "A"],
            ["1", "B"],
            ["2", "C"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=in out=out nodupkey;",
            "  by id;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="nodup.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "val"]
    assert rows[1:] == [
        ["1", "A"],
        ["2", "C"],
    ]


def test_proc_format_rejects_unsupported_statement():
    script = "\n".join(
        [
            "proc format;",
            "  picture pct (round) = '009.9%';",
            "run;",
        ]
    )
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas")
    assert exc_info.value.code == "SANS_PARSE_FORMAT_UNSUPPORTED_STATEMENT"
