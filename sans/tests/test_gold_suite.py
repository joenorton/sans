import csv
from pathlib import Path

from sans.runtime import run_script
from sans.hash_utils import compute_artifact_hash
from sans.validator_sdtm import validate_sdtm


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    path.write_text("\n".join([",".join(row) for row in rows]), encoding="utf-8")


def _read_csv(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def test_gold_merge_1_to_many(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    _write_csv(a, [["id", "val"], ["1", "A"], ["2", "B"]])
    _write_csv(b, [["id", "extra"], ["1", "X"], ["1", "Y"], ["2", "Z"]])

    script = "\n".join(
        [
            "proc sort data=a out=a_s;",
            "  by id;",
            "run;",
            "proc sort data=b out=b_s;",
            "  by id;",
            "run;",
            "data out;",
            "  merge a_s b_s;",
            "  by id;",
            "  keep id val extra;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_merge"
    report = run_script(
        text=script,
        file_name="gold_merge_1_to_many.sas",
        bindings={"a": str(a), "b": str(b)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "val", "extra"],
        ["1", "A", "X"],
        ["1", "A", "Y"],
        ["2", "B", "Z"],
    ]


def test_gold_merge_many_to_many_refusal(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    _write_csv(a, [["id", "val"], ["1", "A"], ["1", "B"]])
    _write_csv(b, [["id", "extra"], ["1", "X"], ["1", "Y"]])

    script = "\n".join(
        [
            "proc sort data=a out=a_s;",
            "  by id;",
            "run;",
            "proc sort data=b out=b_s;",
            "  by id;",
            "run;",
            "data out;",
            "  merge a_s b_s;",
            "  by id;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_merge_mm"
    report = run_script(
        text=script,
        file_name="gold_merge_many_many.sas",
        bindings={"a": str(a), "b": str(b)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_MERGE_MANY_MANY"


def test_gold_dataset_options_precedence(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val", "dropme"], ["1", "10", "X"]])

    script = "\n".join(
        [
            "data out;",
            "  set in(keep=id val rename=(val=score));",
            "  keep id score;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_opts"
    report = run_script(
        text=script,
        file_name="gold_dataset_options.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "score"],
        ["1", "10"],
    ]


def test_gold_missing_comparisons(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["1", ""], ["2", "0"], ["3", "5"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  if val > 0 then output;",
            "  keep id val;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_missing"
    report = run_script(
        text=script,
        file_name="gold_missing_cmp.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "val"],
        ["3", "5"],
    ]


def test_gold_missing_arithmetic(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["1", ""], ["2", "3"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  x = val + 1;",
            "  keep id x;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_missing_math"
    report = run_script(
        text=script,
        file_name="gold_missing_math.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "x"],
        ["1", ""],
        ["2", "4"],
    ]


def test_gold_sort_nodupkey_first_wins(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["2", "A"], ["2", "B"], ["1", "X"]])

    script = "\n".join(
        [
            "proc sort data=in out=out nodupkey;",
            "  by id;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_nodup"
    report = run_script(
        text=script,
        file_name="gold_nodup.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "val"],
        ["1", "X"],
        ["2", "A"],
    ]


def test_gold_transpose_duplicate_id_last_wins(tmp_path):
    inp = tmp_path / "lb.csv"
    _write_csv(
        inp,
        [
            ["subjid", "lbtestcd", "lbstresn"],
            ["101", "GLUC", "1"],
            ["101", "GLUC", "2"],
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
    out_dir = tmp_path / "out_transpose"
    report = run_script(
        text=script,
        file_name="gold_transpose_last.sas",
        bindings={"lb": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "lb_wide.csv")
    assert rows == [
        ["subjid", "GLUC"],
        ["101", "2"],
    ]


def test_gold_sql_left_join_null_fill(tmp_path):
    t1 = tmp_path / "t1.csv"
    t2 = tmp_path / "t2.csv"
    _write_csv(t1, [["id", "val"], ["1", "A"], ["2", "B"]])
    _write_csv(t2, [["id", "extra"], ["1", "X"]])

    script = "\n".join(
        [
            "proc sql;",
            "  create table out as",
            "  select a.id, a.val, b.extra as extra",
            "  from t1 as a",
            "  left join t2 as b on a.id = b.id;",
            "quit;",
        ]
    )
    out_dir = tmp_path / "out_sql_left"
    report = run_script(
        text=script,
        file_name="gold_sql_left.sas",
        bindings={"t1": str(t1), "t2": str(t2)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "val", "extra"],
        ["1", "A", "X"],
        ["2", "B", ""],
    ]


def test_gold_sql_groupby_deterministic_order(tmp_path):
    t1 = tmp_path / "t1.csv"
    _write_csv(t1, [["id", "val"], ["2", "10"], ["1", "20"], ["2", "30"]])

    script = "\n".join(
        [
            "proc sql;",
            "  create table out as",
            "  select id, count(*) as nrec",
            "  from t1",
            "  group by id;",
            "quit;",
        ]
    )
    out_dir = tmp_path / "out_sql_groupby"
    report = run_script(
        text=script,
        file_name="gold_sql_groupby.sas",
        bindings={"t1": str(t1)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "nrec"],
        ["1", "1"],
        ["2", "2"],
    ]


def test_gold_sql_ambiguous_column_error(tmp_path):
    t1 = tmp_path / "t1.csv"
    t2 = tmp_path / "t2.csv"
    _write_csv(t1, [["id", "val"], ["1", "A"]])
    _write_csv(t2, [["id", "extra"], ["1", "X"]])

    script = "\n".join(
        [
            "proc sql;",
            "  create table out as",
            "  select id, val, extra",
            "  from t1 as a",
            "  inner join t2 as b on a.id = b.id;",
            "quit;",
        ]
    )
    out_dir = tmp_path / "out_sql_ambig"
    report = run_script(
        text=script,
        file_name="gold_sql_ambig.sas",
        bindings={"t1": str(t1), "t2": str(t2)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_SQL_AMBIGUOUS_COLUMN"


def test_gold_structured_unsupported_error_shape(tmp_path):
    script = "proc print; run;"
    out_dir = tmp_path / "out_unsupported"
    report = run_script(
        text=script,
        file_name="gold_unsupported.sas",
        bindings={},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "refused"
    primary = report["primary_error"]
    assert set(primary.keys()) >= {"code", "message", "loc"}
    assert primary["loc"]["file"] == "gold_unsupported.sas"


def test_gold_input_best_informat(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["1", "3.5"], ["2", ""]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  x = input(val, best.);",
            "  keep id x;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_best"
    report = run_script(
        text=script,
        file_name="gold_best.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "x"],
        ["1", "3.5"],
        ["2", ""],
    ]


def test_gold_summary_mean_autoname(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["1", "2"], ["1", "4"], ["2", "6"]])

    script = "\n".join(
        [
            "proc summary data=in nway;",
            "  class id;",
            "  var val;",
            "  output out=out mean= / autoname;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_summary"
    report = run_script(
        text=script,
        file_name="gold_summary.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "val_mean"],
        ["1", "3.0"],
        ["2", "6.0"],
    ]


def test_gold_sdtm_validate_smoke(tmp_path):
    dm = tmp_path / "dm.csv"
    _write_csv(dm, [["DOMAIN", "USUBJID"], ["DM", "SUBJ1"]])

    report = validate_sdtm({"dm": str(dm)}, tmp_path / "out_validate")
    assert report["status"] == "failed"
    assert report["summary"]["errors"] >= 1
    assert any(d["code"] == "SDTM_REQUIRED_COLUMN_MISSING" for d in report["diagnostics"])


def test_gold_format_put_mapping(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "sev"], ["1", "MILD"], ["2", "UNKNOWN"]])

    script = "\n".join(
        [
            "proc format;",
            "  value $sev",
            '    "MILD"="1"',
            '    "SEVERE"="3"',
            '    other="";',
            "run;",
            "data out;",
            "  set in;",
            "  sevn = input(put(sev, $sev.), best.);",
            "  keep id sevn;",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_format"
    report = run_script(
        text=script,
        file_name="gold_format_put.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "ok"
    rows = _read_csv(out_dir / "out.csv")
    assert rows == [
        ["id", "sevn"],
        ["1", "1"],
        ["2", ""],
    ]


def test_gold_format_put_unknown_format_error(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "sev"], ["1", "MILD"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  sevn = input(put(sev, $unknown.), best.);",
            "run;",
        ]
    )
    out_dir = tmp_path / "out_format_unknown"
    report = run_script(
        text=script,
        file_name="gold_format_unknown.sas",
        bindings={"in": str(inp)},
        out_dir=out_dir,
        strict=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_FORMAT_UNDEFINED"


def test_gold_determinism_repeat_run(tmp_path):
    inp = tmp_path / "in.csv"
    _write_csv(inp, [["id", "val"], ["1", "2"], ["2", "3"]])

    script = "\n".join(
        [
            "data out;",
            "  set in;",
            "  x = val + 1;",
            "  keep id x;",
            "run;",
        ]
    )

    out_a = tmp_path / "out_a"
    out_b = tmp_path / "out_b"
    report_a = run_script(
        text=script,
        file_name="gold_determinism_a.sas",
        bindings={"in": str(inp)},
        out_dir=out_a,
        strict=True,
    )
    report_b = run_script(
        text=script,
        file_name="gold_determinism_b.sas",
        bindings={"in": str(inp)},
        out_dir=out_b,
        strict=True,
    )

    assert report_a["status"] == "ok"
    assert report_b["status"] == "ok"

    hash_a = compute_artifact_hash(out_a / "out.csv")
    hash_b = compute_artifact_hash(out_b / "out.csv")
    assert hash_a == hash_b
