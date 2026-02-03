import csv
from pathlib import Path

from sans.runtime import run_script


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    path.write_text("\n".join([",".join(row) for row in rows]), encoding="utf-8")


def _read_csv(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def test_gold_control_flow_do_end_nested(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["1", "2"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  do;",
            "    x = val + 1;",
            "    do;",
            "      y = x * 2;",
            "    end;",
            "  end;",
            "  keep id y;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_nested"
    report = run_script(
        text=script,
        file_name="gold_cf_nested.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "outputs" / "out.csv")
    assert rows == [
        ["id", "y"],
        ["1", "6"],
    ]


def test_gold_control_flow_select_exhaustive_error(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["1", "2"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  select;",
            "    when (val = 1) x = 10;",
            "  end;",
            "  keep x;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_select"
    report = run_script(
        text=script,
        file_name="gold_cf_select.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_SELECT_MISMATCH"


def test_gold_control_flow_loop_by_step(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id"], ["1"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  do i = 1 to 5 by 2;",
            "    output;",
            "  end;",
            "  keep i;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_loop_step"
    report = run_script(
        text=script,
        file_name="gold_cf_loop_step.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "outputs" / "out.csv")
    assert rows == [
        ["i"],
        ["1"],
        ["3"],
        ["5"],
    ]


def test_gold_control_flow_loop_negative_step(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id"], ["1"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  do i = 3 to 1 by -1;",
            "    output;",
            "  end;",
            "  keep i;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_loop_neg"
    report = run_script(
        text=script,
        file_name="gold_cf_loop_neg.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "outputs" / "out.csv")
    assert rows == [
        ["i"],
        ["3"],
        ["2"],
        ["1"],
    ]


def test_gold_control_flow_loop_bound_refusal(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["n"], ["3"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  do i = 1 to n;",
            "    output;",
            "  end;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_loop_bound"
    report = run_script(
        text=script,
        file_name="gold_cf_loop_bound.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "refused"
    assert report["primary_error"]["code"] == "SANS_PARSE_LOOP_BOUND_UNSUPPORTED"


def test_gold_control_flow_loop_step_zero_refusal(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id"], ["1"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  do i = 1 to 3 by 0;",
            "    output;",
            "  end;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_loop_zero"
    report = run_script(
        text=script,
        file_name="gold_cf_loop_zero.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "refused"
    assert report["primary_error"]["code"] == "SANS_PARSE_LOOP_BOUND_UNSUPPORTED"


def test_gold_control_flow_loop_cap(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id"], ["1"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  do i = 1 to 1000001;",
            "    output;",
            "  end;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_loop_cap"
    report = run_script(
        text=script,
        file_name="gold_cf_loop_cap.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_LOOP_LIMIT"


def test_gold_control_flow_nesting_depth_cap(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id"], ["1"]])

    lines = ["data out;", "  set in;"]
    for _ in range(51):
        lines.append("  do;")
    lines.append("    output;")
    for _ in range(51):
        lines.append("  end;")
    lines.append("run;")

    script = "\n".join(lines)
    out_dir = tmp_path / "out_depth"
    report = run_script(
        text=script,
        file_name="gold_cf_depth.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_CONTROL_DEPTH"
