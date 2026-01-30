import csv
from pathlib import Path

from sans.runtime import run_script


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def test_run_hello_world(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b"],
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  c = a + b;",
            "  if c > 20;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="hello.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    assert report["runtime"]["status"] == "ok"
    assert (tmp_path / "plan.ir.json").exists()
    assert (tmp_path / "report.json").exists()
    out_csv = tmp_path / "out.csv"
    assert out_csv.exists()

    runtime_outputs = report["runtime"]["outputs"]
    assert runtime_outputs[0]["table"] == "out"
    assert runtime_outputs[0]["rows"] == 2
    assert runtime_outputs[0]["columns"] == ["a", "b", "c"]

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["a", "b", "c"]
    assert rows[1:] == [
        ["2", "20", "22"],
        ["3", "30", "33"],
    ]


def test_run_unsupported_op_fails(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b"],
            ["1", "10"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=in out=out;",
            "  by a;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="unsupported.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_CAP_UNSUPPORTED_OP"


def test_run_unsupported_expr_node_fails(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b"],
            ["1", "10"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  c = coalesce(a, b);",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="unsupported_expr.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_UNSUPPORTED_EXPR_NODE"


def test_run_missing_input_file_fails(tmp_path):
    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  c = a + b;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="missing.sas",
        bindings={"in": str(tmp_path / "missing.csv")},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_INPUT_NOT_FOUND"
