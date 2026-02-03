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
    assert (tmp_path / "artifacts" / "plan.ir.json").exists()
    assert (tmp_path / "report.json").exists()
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    outputs_list = report.get("outputs", [])
    assert len(outputs_list) >= 1
    assert outputs_list[0]["name"] == "out"
    assert outputs_list[0]["rows"] == 2
    assert outputs_list[0]["columns"] == ["a", "b", "c"]

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["a", "b", "c"]
    assert rows[1:] == [
        ["2", "20", "22"],
        ["3", "30", "33"],
    ]


def test_run_proc_sort_ok(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b"],
            ["2", "x"],
            ["1", "y"],
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
        file_name="sort.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["a", "b"]
    assert rows[1:] == [
        ["1", "y"],
        ["2", "x"],
    ]


def test_run_coalesce_ok(tmp_path):
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

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["a", "b", "c"]
    assert rows[1:] == [
        ["1", "10", "1"],
    ]


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


def test_run_merge_by_carries_values(tmp_path):
    a_csv = tmp_path / "a.csv"
    b_csv = tmp_path / "b.csv"
    _write_csv(
        a_csv,
        [
            ["id", "aval"],
            ["1", "10"],
            ["2", "20"],
        ],
    )
    _write_csv(
        b_csv,
        [
            ["id", "bval"],
            ["1", "100"],
            ["1", "101"],
            ["2", "200"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=a out=a_s;",
            "  by id;",
            "run;",
            "proc sort data=b out=b_s;",
            "  by id;",
            "run;",
            "data out;",
            "  merge a_s(in=ina) b_s(in=inb);",
            "  by id;",
            "  if ina and inb;",
            "  keep id aval bval;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="merge.sas",
        bindings={"a": str(a_csv), "b": str(b_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "aval", "bval"]
    assert rows[1:] == [
        ["1", "10", "100"],
        ["1", "10", "101"],
        ["2", "20", "200"],
    ]


def test_run_by_first_outputs_only_first(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "seq"],
            ["1", "1"],
            ["1", "2"],
            ["2", "1"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=in out=sorted;",
            "  by id seq;",
            "run;",
            "data out;",
            "  set sorted;",
            "  by id;",
            "  if first.id then output;",
            "  keep id seq;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="first.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "seq"]
    assert rows[1:] == [
        ["1", "1"],
        ["2", "1"],
    ]


def test_run_if_then_else_with_ne(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["1", "0"],
            ["2", "5"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  if val ne 0 then flag = 1;",
            "  else flag = .;",
            "  keep id val flag;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="if_then.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "val", "flag"]
    assert rows[1:] == [
        ["1", "0", ""],
        ["2", "5", "1"],
    ]


def test_run_filter_on_last_flag(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "seq"],
            ["1", "1"],
            ["1", "2"],
            ["2", "1"],
            ["2", "2"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=in out=sorted;",
            "  by id seq;",
            "run;",
            "data out;",
            "  set sorted;",
            "  by id;",
            "  if last.id;",
            "  keep id seq;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="last_flag.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "seq"]
    assert rows[1:] == [
        ["1", "2"],
        ["2", "2"],
    ]


def test_run_comparison_keywords_in_filter(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b", "c"],
            ["1", "0", "4"],
            ["2", "-1", "3"],
            ["3", "1", "5"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  if a eq 2 or b lt 0 or c ge 5;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="compare_keywords.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["a", "b", "c"]
    assert rows[1:] == [
        ["2", "-1", "3"],
        ["3", "1", "5"],
    ]


def test_run_sort_is_stable(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b"],
            ["1", "first"],
            ["1", "second"],
            ["1", "third"],
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
        file_name="stable_sort.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["a", "b"]
    assert rows[1:] == [
        ["1", "first"],
        ["1", "second"],
        ["1", "third"],
    ]


def test_run_retain_persists_values(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["1", "10"],
            ["2", "15"],
            ["3", "20"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  retain prev;",
            "  diff = val - prev;",
            "  prev = val;",
            "  keep id val prev diff;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="retain.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "val", "prev", "diff"]
    assert rows[1:] == [
        ["1", "10", "10", ""],
        ["2", "15", "15", "5"],
        ["3", "20", "20", "5"],
    ]


def test_run_missing_comparison_filters_out(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["1", ""],
            ["2", "1"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  if val > 0;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="missing_compare.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "val"]
    assert rows[1:] == [
        ["2", "1"],
    ]


def test_run_is_deterministic(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["2", "20"],
            ["1", "10"],
            ["1", "11"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=in out=sorted;",
            "  by id;",
            "run;",
            "data out;",
            "  set sorted;",
            "  keep val id;",
            "run;",
        ]
    )

    out_a = tmp_path / "out_a"
    out_b = tmp_path / "out_b"

    report_a = run_script(
        text=script,
        file_name="determinism.sas",
        bindings={"in": str(in_csv)},
        out_dir=out_a,
        strict=True,
    )
    report_b = run_script(
        text=script,
        file_name="determinism.sas",
        bindings={"in": str(in_csv)},
        out_dir=out_b,
        strict=True,
    )

    assert report_a["status"] == "ok"
    assert report_b["status"] == "ok"

    out_csv_a = out_a / "outputs" / "out.csv"
    out_csv_b = out_b / "outputs" / "out.csv"
    assert out_csv_a.exists()
    assert out_csv_b.exists()

    with out_csv_a.open("r", encoding="utf-8", newline="") as f:
        rows_a = list(csv.reader(f))
    with out_csv_b.open("r", encoding="utf-8", newline="") as f:
        rows_b = list(csv.reader(f))

    assert rows_a == rows_b


def test_run_keep_preserves_column_order(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b", "c"],
            ["1", "2", "3"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  keep c a;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="keep_order.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["c", "a"]
    assert rows[1:] == [["3", "1"]]


def test_run_merge_in_flags(tmp_path):
    a_csv = tmp_path / "a.csv"
    b_csv = tmp_path / "b.csv"
    _write_csv(
        a_csv,
        [
            ["id", "aval"],
            ["1", "10"],
            ["2", "20"],
        ],
    )
    _write_csv(
        b_csv,
        [
            ["id", "bval"],
            ["1", "100"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=a out=a_s;",
            "  by id;",
            "run;",
            "proc sort data=b out=b_s;",
            "  by id;",
            "run;",
            "data out;",
            "  merge a_s(in=ina) b_s(in=inb);",
            "  by id;",
            "  keep id ina inb;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="merge_flags.sas",
        bindings={"a": str(a_csv), "b": str(b_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "ina", "inb"]
    assert rows[1:] == [
        ["1", "True", "True"],
        ["2", "True", "False"],
    ]


def test_run_by_group_multikey_last(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "testcd", "dt"],
            ["1", "A", "2023-01-01"],
            ["1", "A", "2023-01-02"],
            ["1", "B", "2023-01-01"],
            ["2", "A", "2023-01-03"],
            ["2", "A", "2023-01-04"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=in out=sorted;",
            "  by id testcd dt;",
            "run;",
            "data out;",
            "  set sorted;",
            "  by id testcd;",
            "  if last.testcd then output;",
            "  keep id testcd dt;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="by_multikey.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "testcd", "dt"]
    assert rows[1:] == [
        ["1", "A", "2023-01-02"],
        ["1", "B", "2023-01-01"],
        ["2", "A", "2023-01-04"],
    ]


def test_run_explicit_output_suppresses_default(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["1", "0"],
            ["2", "5"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  if val > 0 then output;",
            "  keep id val;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="explicit_output.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "val"]
    assert rows[1:] == [
        ["2", "5"],
    ]


def test_run_missing_arithmetic_yields_missing(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "val"],
            ["1", ""],
            ["2", "3"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  x = val + 1;",
            "  keep id x;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="missing_math.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "x"]
    assert rows[1:] == [
        ["1", ""],
        ["2", "4"],
    ]


def test_run_dataset_options_applied_at_read_time(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b", "c"],
            ["1", "10", "x"],
            ["2", "20", "y"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in(where=(a >= 2) keep=a b rename=(a=x));",
            "  keep x;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="dataset_opts.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["x"]
    assert rows[1:] == [["2"]]


def test_run_proc_transpose_last_wins(tmp_path):
    in_csv = tmp_path / "lb.csv"
    _write_csv(
        in_csv,
        [
            ["subjid", "lbdtc", "lbtestcd", "lbstresn"],
            ["101", "2023-01-10", "GLUC", "95"],
            ["101", "2023-01-12", "GLUC", "110"],
            ["101", "2023-01-12", "ALT", "44"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=lb out=lb_s;",
            "  by subjid lbdtc;",
            "run;",
            "proc transpose data=lb_s out=lb_wide;",
            "  by subjid;",
            "  id lbtestcd;",
            "  var lbstresn;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="transpose_last.sas",
        bindings={"lb": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "lb_wide.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["subjid", "GLUC", "ALT"]
    assert rows[1:] == [["101", "110", "44"]]


def test_run_merge_dataset_where_option(tmp_path):
    a_csv = tmp_path / "a.csv"
    b_csv = tmp_path / "b.csv"
    _write_csv(
        a_csv,
        [
            ["id", "aval"],
            ["1", "0"],
            ["1", "1"],
        ],
    )
    _write_csv(
        b_csv,
        [
            ["id", "bval"],
            ["1", "100"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=a out=a_s;",
            "  by id;",
            "run;",
            "proc sort data=b out=b_s;",
            "  by id;",
            "run;",
            "data out;",
            "  merge a_s(where=(aval > 0)) b_s;",
            "  by id;",
            "  keep id aval bval;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="merge_where.sas",
        bindings={"a": str(a_csv), "b": str(b_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "aval", "bval"]
    assert rows[1:] == [["1", "1", "100"]]


def test_run_dataset_options_drop_and_rename(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["a", "b", "c"],
            ["1", "10", "x"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in(drop=c rename=(a=x));",
            "  keep x b;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="drop_rename.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["x", "b"]
    assert rows[1:] == [["1", "10"]]


def test_run_dataset_where_string_and_missing(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "grp"],
            ["1", ""],
            ["2", "A"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in(where=(grp = \"A\"));",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="where_string.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "grp"]
    assert rows[1:] == [["2", "A"]]


def test_run_transpose_id_missing_fails(tmp_path):
    in_csv = tmp_path / "lb.csv"
    _write_csv(
        in_csv,
        [
            ["subjid", "lbtestcd", "lbstresn"],
            ["101", "", "10"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=lb out=lb_s;",
            "  by subjid;",
            "run;",
            "proc transpose data=lb_s out=lb_wide;",
            "  by subjid;",
            "  id lbtestcd;",
            "  var lbstresn;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="transpose_missing_id.sas",
        bindings={"lb": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_TRANSPOSE_ID_MISSING"


def test_run_transpose_id_collision_fails(tmp_path):
    in_csv = tmp_path / "lb.csv"
    _write_csv(
        in_csv,
        [
            ["subjid", "lbtestcd", "lbstresn"],
            ["101", "A-B", "10"],
            ["101", "A_B", "11"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=lb out=lb_s;",
            "  by subjid;",
            "run;",
            "proc transpose data=lb_s out=lb_wide;",
            "  by subjid;",
            "  id lbtestcd;",
            "  var lbstresn;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="transpose_collision.sas",
        bindings={"lb": str(in_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_TRANSPOSE_ID_COLLISION"


def test_run_merge_mixed_dataset_options(tmp_path):
    a_csv = tmp_path / "a.csv"
    b_csv = tmp_path / "b.csv"
    _write_csv(
        a_csv,
        [
            ["id", "aval", "extra"],
            ["1", "10", "x"],
        ],
    )
    _write_csv(
        b_csv,
        [
            ["id", "bval"],
            ["1", "100"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=a out=a_s;",
            "  by id;",
            "run;",
            "proc sort data=b out=b_s;",
            "  by id;",
            "run;",
            "data out;",
            "  merge a_s(keep=id aval) b_s(drop=bval);",
            "  by id;",
            "  keep id aval;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="merge_mixed_opts.sas",
        bindings={"a": str(a_csv), "b": str(b_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()

    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["id", "aval"]
    assert rows[1:] == [["1", "10"]]


def test_run_unsorted_by_fails_in_runtime(tmp_path):
    in_csv = tmp_path / "in.csv"
    _write_csv(
        in_csv,
        [
            ["id", "seq"],
            ["2", "1"],
            ["1", "1"],
        ],
    )

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  by id;",
            "  if first.id then output;",
            "  keep id seq;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="unsorted_runtime.sas",
        bindings={"in": str(in_csv)},
        out_dir=tmp_path,
        strict=False,
    )

    assert report["status"] == "refused"
    primary = report["primary_error"]
    assert primary["code"] == "SANS_VALIDATE_ORDER_REQUIRED"
    assert primary["loc"]["file"] == "unsorted_runtime.sas"


def test_run_merge_many_to_many_fails(tmp_path):
    a_csv = tmp_path / "a.csv"
    b_csv = tmp_path / "b.csv"
    _write_csv(
        a_csv,
        [
            ["id", "aval"],
            ["1", "10"],
            ["1", "20"],
        ],
    )
    _write_csv(
        b_csv,
        [
            ["id", "bval"],
            ["1", "100"],
            ["1", "200"],
            ["1", "300"],
        ],
    )

    script = "\n".join(
        [
            "proc sort data=a out=a_s;",
            "  by id;",
            "run;",
            "proc sort data=b out=b_s;",
            "  by id;",
            "run;",
            "data out;",
            "  merge a_s(in=ina) b_s(in=inb);",
            "  by id;",
            "  if ina and inb;",
            "  keep id aval bval;",
            "run;",
        ]
    )

    report = run_script(
        text=script,
        file_name="merge_many.sas",
        bindings={"a": str(a_csv), "b": str(b_csv)},
        out_dir=tmp_path,
        strict=True,
    )

    assert report["status"] == "failed"
    primary = report["primary_error"]
    assert primary["code"] == "SANS_RUNTIME_MERGE_MANY_MANY"
    assert primary["loc"]["file"] == "merge_many.sas"


def test_run_cast_sans(tmp_path):
    """Sans script with cast runs and emits cast evidence."""
    import json
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b\n"
        "  1,2.5\n"
        "  3,4\n"
        "end\n"
        "table t = from(in) do\n"
        "  cast(a -> int, b -> decimal on_error=null trim=true)\n"
        "end\n"
        "save t to \"out.csv\"\n"
    )
    report = run_script(
        text=script,
        file_name="cast.sans",
        bindings={},
        out_dir=tmp_path,
        strict=True,
    )
    assert report["status"] == "ok"
    assert report["runtime"]["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    assert out_csv.exists()
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["a", "b"]
    assert len(rows) == 3  # header + 2 data
    # a -> int: 1, 3; b -> decimal: 2.5, 4
    assert rows[1][0] == "1"
    assert rows[2][0] == "3"
    # step_evidence in runtime.evidence.json includes cast step with cast_failures and nulled
    evidence_path = tmp_path / "artifacts" / "runtime.evidence.json"
    assert evidence_path.exists()
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    step_evidence = evidence.get("step_evidence") or []
    cast_ev = [e for e in step_evidence if e.get("op") == "cast"]
    assert len(cast_ev) == 1
    assert "cast_failures" in cast_ev[0]
    assert "nulled" in cast_ev[0]
