import csv
import textwrap

import pytest

from sans.compiler import check_script, emit_check_artifacts
from sans.ir import OpStep, UnknownBlockStep
from sans.runtime import run_script


def _write_csv(path, rows):
    path.write_text("\n".join([",".join(row) for row in rows]), encoding="utf-8")


def test_proc_sql_parse_ok():
    script = textwrap.dedent("""
        proc sql;
          create table out as
          select a.id, b.val as bval
          from t1 as a
          left join t2 as b on a.id = b.id
          where a.id >= 1;
        quit;
    """)
    irdoc = check_script(script, "test.sas", tables={"t1", "t2"}, legacy_sas=True)
    assert len(irdoc.steps) == 1
    step = irdoc.steps[0]
    assert isinstance(step, OpStep)
    assert step.op == "sql_select"
    assert step.outputs == ["out"]
    assert step.params["from"]["table"] == "t1"
    assert step.params["joins"][0]["type"] == "left"


def test_proc_sql_subselect_refused():
    script = textwrap.dedent("""
        proc sql;
          create table out as select * from (select * from t1);
        quit;
    """)
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", tables={"t1"}, legacy_sas=True)
    assert exc_info.value.code == "SANS_PARSE_SQL_UNSUPPORTED_FORM"
    assert exc_info.value.loc.file == "test.sas"


def test_proc_sql_join_requires_explicit_type():
    script = textwrap.dedent("""
        proc sql;
          create table out as
          select a.id
          from t1 a
          join t2 b on a.id = b.id;
        quit;
    """)
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", tables={"t1", "t2"}, legacy_sas=True)
    assert exc_info.value.code == "SANS_PARSE_SQL_UNSUPPORTED_FORM"


def test_proc_sql_groupby_requires_selected_keys():
    script = textwrap.dedent("""
        proc sql;
          create table out as
          select id, val, count(*) as nrec
          from t1
          group by id;
        quit;
    """)
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", tables={"t1"}, legacy_sas=True)
    assert exc_info.value.code == "SANS_PARSE_SQL_UNSUPPORTED_FORM"


def test_proc_sql_unsupported_error_shape(tmp_path):
    script = textwrap.dedent("""
        proc sql;
          create table out as select * from (select * from t1);
        quit;
    """)
    _, report = emit_check_artifacts(
        script,
        "bad_sql.sas",
        tables={"t1"},
        out_dir=tmp_path,
        legacy_sas=True,
    )
    assert report["status"] == "refused"
    primary = report["primary_error"]
    assert set(primary.keys()) >= {"code", "message", "loc"}
    assert primary["loc"]["file"] == "bad_sql.sas"


def test_proc_sql_ambiguous_column_errors(tmp_path):
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

    report = run_script(
        text=script,
        file_name="ambig.sas",
        bindings={"t1": str(t1), "t2": str(t2)},
        out_dir=tmp_path,
        strict=True,
        legacy_sas=True,
    )

    assert report["status"] == "failed"
    assert report["primary_error"]["code"] == "SANS_RUNTIME_SQL_AMBIGUOUS_COLUMN"


def test_proc_sql_left_join_null_fill(tmp_path):
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

    report = run_script(
        text=script,
        file_name="left_join.sas",
        bindings={"t1": str(t1), "t2": str(t2)},
        out_dir=tmp_path,
        strict=True,
        legacy_sas=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "val", "extra"]
    assert rows[1:] == [
        ["1", "A", "X"],
        ["2", "B", ""],
    ]


def test_proc_sql_groupby_aggregate(tmp_path):
    t1 = tmp_path / "t1.csv"
    _write_csv(t1, [["id", "val"], ["1", "10"], ["1", "20"], ["2", "30"]])

    script = "\n".join(
        [
            "proc sql;",
            "  create table out as",
            "  select id, count(*) as nrec, avg(val) as mean_val",
            "  from t1",
            "  group by id;",
            "quit;",
        ]
    )

    report = run_script(
        text=script,
        file_name="groupby.sas",
        bindings={"t1": str(t1)},
        out_dir=tmp_path,
        strict=True,
        legacy_sas=True,
    )

    assert report["status"] == "ok"
    out_csv = tmp_path / "outputs" / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "nrec", "mean_val"]
    assert rows[1:] == [
        ["1", "2", "15.0"],
        ["2", "1", "30.0"],
    ]
