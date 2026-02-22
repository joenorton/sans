import json
from pathlib import Path

from sans.compiler import _irdoc_to_dict, compile_sans_script, UnknownBlockStep
from sans.ir import IRDoc, OpStep
from sans.sans_script import irdoc_to_expanded_sans


FIXTURE = Path("sans/tests/fixtures/hello.sans")
PLAN = Path("sans/tests/gold/hello.plan.ir.json")


def _compile_script(text: str) -> dict:
    irdoc = compile_sans_script(text, "sans/tests/fixtures/hello.sans", tables={"lb"})
    validated = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    irdoc = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated,
        datasources=irdoc.datasources,
    )
    return _irdoc_to_dict(irdoc)


def _normalize_plan(plan: dict) -> dict:
    for step in plan.get("steps", []):
        loc = step.get("loc")
        if isinstance(loc, dict) and "file" in loc:
            loc["file"] = loc["file"].replace("\\", "/")
    return plan


def _assert_block_error(text: str, expected_code: str):
    irdoc = compile_sans_script(text, "script.sans", tables={"bar"})
    assert irdoc.steps, "Expected steps even on refusal"
    assert isinstance(irdoc.steps[0], UnknownBlockStep)
    assert irdoc.steps[0].code == expected_code


def test_parse_lower_golden_hello_sans():
    plan = _normalize_plan(_compile_script(FIXTURE.read_text(encoding="utf-8")))
    expected = _normalize_plan(json.loads(PLAN.read_text(encoding="utf-8")))
    assert plan == expected


def test_sans_script_deterministic_plan():
    first = _compile_script(FIXTURE.read_text(encoding="utf-8"))
    second = _compile_script(FIXTURE.read_text(encoding="utf-8"))
    assert first == second


def test_expanded_sans_round_trip_byte():
    """expanded.sans → IR → expanded.sans is byte-identical."""
    text = FIXTURE.read_text(encoding="utf-8")
    irdoc = compile_sans_script(text, str(FIXTURE), tables={"lb"})
    validated = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    irdoc = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated,
        datasources=irdoc.datasources,
    )
    expanded = irdoc_to_expanded_sans(irdoc)
    irdoc2 = compile_sans_script(expanded, "expanded.sans", tables=set())
    assert not any(
        isinstance(s, UnknownBlockStep) and getattr(s, "severity", "") == "fatal"
        for s in irdoc2.steps
    ), "expanded.sans should compile without fatal errors"
    validated2 = IRDoc(
        steps=irdoc2.steps,
        tables=irdoc2.tables,
        table_facts=irdoc2.table_facts,
        datasources=irdoc2.datasources,
    ).validate()
    irdoc2 = IRDoc(
        steps=irdoc2.steps,
        tables=irdoc2.tables,
        table_facts=validated2,
        datasources=irdoc2.datasources,
    )
    expanded2 = irdoc_to_expanded_sans(irdoc2)
    assert expanded == expanded2, "expanded.sans → IR → expanded.sans should be byte-identical"


def test_expanded_sans_never_emits_summary():
    """expanded.sans printer must NEVER emit 'summary'; canonical op name is 'aggregate'."""
    text = FIXTURE.read_text(encoding="utf-8")
    irdoc = compile_sans_script(text, str(FIXTURE), tables={"lb"})
    validated = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    irdoc = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated,
        datasources=irdoc.datasources,
    )
    expanded = irdoc_to_expanded_sans(irdoc)
    assert "summary" not in expanded, "expanded.sans must not contain 'summary'; use 'aggregate'"


def test_sort_by_legacy_refused_in_validation():
    """Legacy shapes (list[str] or asc) are refused in validate(); canonical by=[{"col", "desc"}] passes."""
    import pytest
    from sans._loc import Loc
    from sans.ir import DatasourceDecl, TableFact, UnknownBlockStep

    # Legacy list[str] is refused (no normalization in nucleus)
    step_legacy_str = OpStep(
        op="sort",
        inputs=["__datasource__raw"],
        outputs=["s1"],
        params={"by": ["a", "b"]},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc_str = IRDoc(
        steps=[step_legacy_str],
        tables=set(),
        table_facts={},
        datasources={"raw": DatasourceDecl(kind="csv", path="x")},
    )
    with pytest.raises(UnknownBlockStep) as exc_info:
        irdoc_str.validate()
    assert exc_info.value.code == "SANS_IR_CANON_SHAPE_SORT"

    # list[dict] with "asc" is refused (canonical is desc only)
    step_legacy_asc = OpStep(
        op="sort",
        inputs=["t0"],
        outputs=["s2"],
        params={"by": [{"col": "x", "asc": True}, {"col": "y", "asc": False}], "nodupkey": False},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc_asc = IRDoc(steps=[step_legacy_asc], tables={"t0"}, table_facts={"t0": TableFact()}, datasources={})
    with pytest.raises(UnknownBlockStep) as exc_info2:
        irdoc_asc.validate()
    assert exc_info2.value.code == "SANS_IR_CANON_SHAPE_SORT"

    # Canonical by passes and is not mutated
    step_canon = OpStep(
        op="sort",
        inputs=["t0"],
        outputs=["s3"],
        params={"by": [{"col": "x", "desc": False}, {"col": "y", "desc": True}]},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc_canon = IRDoc(steps=[step_canon], tables={"t0"}, table_facts={"t0": TableFact()}, datasources={})
    irdoc_canon.validate()
    assert step_canon.params["by"] == [{"col": "x", "desc": False}, {"col": "y", "desc": True}]


def test_expanded_sans_sort_by_dict_keys_deterministic():
    """expanded.sans printer assumes canonical list[{"col", "desc"}]; emits by(col1, -col2)."""
    from sans._loc import Loc
    from sans.ir import DatasourceDecl, TableFact
    # Canonical after validate(): by=[{"col": "a", "desc": False}]
    step = OpStep(
        op="sort",
        inputs=["__datasource__raw"],
        outputs=["s1"],
        params={"by": [{"col": "a", "desc": False}], "nodupkey": False},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc = IRDoc(
        steps=[step],
        tables=set(),
        table_facts={},
        datasources={"raw": DatasourceDecl(kind="csv", path="x")},
    )
    expanded = irdoc_to_expanded_sans(irdoc)
    assert "sort(" in expanded
    assert "by(a)" in expanded
    # Desc: by=[{"col": "x", "desc": True}, {"col": "y", "desc": False}]
    step2 = OpStep(
        op="sort",
        inputs=["t0"],
        outputs=["s2"],
        params={"by": [{"col": "x", "desc": True}, {"col": "y", "desc": False}], "nodupkey": False},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc2 = IRDoc(steps=[step2], tables={"t0"}, table_facts={"t0": TableFact()}, datasources={})
    expanded2 = irdoc_to_expanded_sans(irdoc2)
    assert "by(-x, y)" in expanded2


def test_select_rename_aggregate_legacy_refused_in_validation():
    """Legacy shapes for select, rename, aggregate are refused in validate(); canonical passes."""
    import pytest
    from sans._loc import Loc
    from sans.ir import DatasourceDecl, TableFact, UnknownBlockStep

    # Select with legacy "keep" is refused
    step_keep = OpStep(
        op="select",
        inputs=["t0"],
        outputs=["s1"],
        params={"keep": ["a", "b"]},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc = IRDoc(
        steps=[step_keep],
        tables={"t0"},
        table_facts={"t0": TableFact()},
        datasources={},
    )
    with pytest.raises(UnknownBlockStep) as exc_info:
        irdoc.validate()
    assert exc_info.value.code == "SANS_IR_CANON_SHAPE_SELECT"

    # Rename with legacy "map" (dict) is refused
    step_rename = OpStep(
        op="rename",
        inputs=["t0"],
        outputs=["s3"],
        params={"map": {"b": "B", "a": "A"}},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc2 = IRDoc(steps=[step_rename], tables={"t0"}, table_facts={"t0": TableFact()}, datasources={})
    with pytest.raises(UnknownBlockStep) as exc_info2:
        irdoc2.validate()
    assert exc_info2.value.code == "SANS_IR_CANON_SHAPE_RENAME"

    # Aggregate with legacy class/var/stats is refused
    step_agg = OpStep(
        op="aggregate",
        inputs=["t0"],
        outputs=["s4"],
        params={"class": ["g"], "var": ["x"], "stats": ["mean"]},
        loc=Loc("test.sas", 1, 1),
    )
    irdoc3 = IRDoc(steps=[step_agg], tables={"t0"}, table_facts={"t0": TableFact()}, datasources={})
    with pytest.raises(UnknownBlockStep) as exc_info3:
        irdoc3.validate()
    assert exc_info3.value.code == "SANS_IR_CANON_SHAPE_AGGREGATE"

    # Canonical select/rename/aggregate pass validate() without mutation
    steps_canon = [
        OpStep(op="select", inputs=["t0"], outputs=["s1"], params={"cols": ["a", "b"]}, loc=Loc("test.sas", 1, 1)),
        OpStep(op="rename", inputs=["t0"], outputs=["s2"], params={"mapping": [{"from": "a", "to": "A"}]}, loc=Loc("test.sas", 1, 1)),
        OpStep(op="aggregate", inputs=["t0"], outputs=["s3"], params={"group_by": ["g"], "metrics": [{"name": "x_mean", "op": "mean", "col": "x"}]}, loc=Loc("test.sas", 1, 1)),
    ]
    irdoc_canon = IRDoc(steps=steps_canon, tables={"t0"}, table_facts={"t0": TableFact()}, datasources={})
    irdoc_canon.validate()
    assert steps_canon[0].params == {"cols": ["a", "b"]}
    assert steps_canon[1].params == {"mapping": [{"from": "a", "to": "A"}]}
    assert steps_canon[2].params["group_by"] == ["g"] and len(steps_canon[2].params["metrics"]) == 1


def test_expanded_sans_select_rename_aggregate_canonical_output():
    """expanded.sans emits deterministic canonical syntax for select, rename, aggregate."""
    from sans._loc import Loc
    from sans.ir import DatasourceDecl, TableFact

    steps = [
        OpStep(
            op="select",
            inputs=["__datasource__r"],
            outputs=["s1"],
            params={"cols": ["a", "b"]},
            loc=Loc("t.sas", 1, 1),
        ),
        OpStep(
            op="rename",
            inputs=["s1"],
            outputs=["s2"],
            params={"mapping": [{"from": "a", "to": "A"}, {"from": "b", "to": "B"}]},
            loc=Loc("t.sas", 1, 1),
        ),
        OpStep(
            op="aggregate",
            inputs=["s2"],
            outputs=["s3"],
            params={"group_by": ["A"], "metrics": [{"name": "B_mean", "op": "mean", "col": "B"}]},
            loc=Loc("t.sas", 1, 1),
        ),
    ]
    irdoc = IRDoc(steps=steps, tables=set(), table_facts={}, datasources={"r": DatasourceDecl(kind="csv", path="x")})
    expanded = irdoc_to_expanded_sans(irdoc)
    assert "select a, b" in expanded
    assert "rename(a -> A, b -> B)" in expanded
    assert "aggregate(" in expanded
    assert "class(A)" in expanded
    assert "var(B)" in expanded
    assert "stats(mean)" in expanded


def test_expanded_sans_round_trip_semantic():
    """IR → expanded.sans → IR is semantically identical (same ops, inputs, outputs, params)."""
    text = FIXTURE.read_text(encoding="utf-8")
    irdoc = compile_sans_script(text, str(FIXTURE), tables={"lb"})
    validated = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    irdoc = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated,
        datasources=irdoc.datasources,
    )
    expanded = irdoc_to_expanded_sans(irdoc)
    irdoc2 = compile_sans_script(expanded, "expanded.sans", tables=set())
    validated2 = IRDoc(
        steps=irdoc2.steps,
        tables=irdoc2.tables,
        table_facts=irdoc2.table_facts,
        datasources=irdoc2.datasources,
    ).validate()
    irdoc2 = IRDoc(
        steps=irdoc2.steps,
        tables=irdoc2.tables,
        table_facts=validated2,
        datasources=irdoc2.datasources,
    )
    # Compare steps (op, inputs, outputs, params) ignoring step_id/transform_id
    steps1 = [s for s in irdoc.steps if hasattr(s, "op")]
    steps2 = [s for s in irdoc2.steps if hasattr(s, "op")]
    assert len(steps1) == len(steps2)
    for s1, s2 in zip(steps1, steps2):
        assert s1.op == s2.op
        assert s1.inputs == s2.inputs
        assert s1.outputs == s2.outputs
        assert s1.params == s2.params


def test_missing_header_refused():
    script = "table foo = from(bar)\n"
    _assert_block_error(script, "E_MISSING_HEADER")


def test_malformed_end_refused():
    script = "# sans 0.1\ntable foo = from(bar) do\n  select a\n"
    _assert_block_error(script, "E_PARSE")


def test_unknown_clause_refused():
    script = (
        "# sans 0.1\n"
        "datasource bar = inline_csv do\n"
        "  a,b\n"
        "  6,7\n"
        "  3,2\n"
        "end\n"
        "table foo = from(bar) do\n"
        "  mystery\n"
        "end\n"
    )
    _assert_block_error(script, "E_PARSE")


def test_invalid_expression_refused():
    script = (
        "# sans 0.1\n"
        "datasource bar = inline_csv do\n"
        "  a,b\n"
        "  6,7\n"
        "  3,2\n"
        "end\n"
        "table foo = from(bar) do\n"
        "  filter (a > )\n"
        "end\n"
    )
    _assert_block_error(script, "E_BAD_EXPR")


def test_single_equals_refused():
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b\n"
        "  6,7\n"
        "  3,2\n"
        "end\n"
        "table foo = from(bar) do\n"
        "  filter a = 1\n"
        "end\n"
    )
    _assert_block_error(script, "E_BAD_EXPR")

def test_sort_missing_by_refused():
    script = (
        "# sans 0.1\n"
        "datasource bar = inline_csv do\n"
        "  a,b\n"
        "  6,7\n"
        "  3,2\n"
        "end\n"
        "table baz = sort(bar)\n"
    )
    irdoc = compile_sans_script(script, "script.sans", tables={"bar"})
    try:
        irdoc.validate()
        assert False, "Should have failed validation"
    except UnknownBlockStep as err:
        assert err.code == "E_SANS_VALIDATE_SORT_MISSING_BY"


def test_cast_parse_and_expanded():
    """Cast transform parses and prints canonically in expanded.sans."""
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b\n"
        "  1,2.5\n"
        "  3,4\n"
        "end\n"
        "table t = from(in) do\n"
        "  cast(a -> str, b -> decimal on_error=null trim=true)\n"
        "end\n"
    )
    irdoc = compile_sans_script(script, "cast.sans", tables=set())
    assert not any(
        isinstance(s, UnknownBlockStep) and getattr(s, "severity", "") == "fatal"
        for s in irdoc.steps
    )
    validated = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    irdoc = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated,
        datasources=irdoc.datasources,
    )
    expanded = irdoc_to_expanded_sans(irdoc)
    assert "cast(" in expanded
    assert "a -> str" in expanded
    assert "b -> decimal" in expanded
    assert "on_error=null" in expanded
    assert "trim=true" in expanded
    # Find cast step in IR
    cast_steps = [s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "cast"]
    assert len(cast_steps) == 1
    casts = cast_steps[0].params.get("casts") or []
    assert len(casts) >= 2
    col_to = {(c["col"], c["to"]): c for c in casts}
    assert ("a", "str") in col_to
    assert ("b", "decimal") in col_to
    assert col_to.get(("b", "decimal"), {}).get("on_error") == "null"
    assert col_to.get(("b", "decimal"), {}).get("trim") is True


def test_drop_in_pipeline_parses_and_expands():
    """drop in pipeline parses and expanded.sans emits 'drop a, b'."""
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b,c\n"
        "  1,2,3\n"
        "end\n"
        "table t = from(in) do\n"
        "  drop b\n"
        "end\n"
    )
    irdoc = compile_sans_script(script, "drop.sans", tables=set())
    assert not any(
        isinstance(s, UnknownBlockStep) and getattr(s, "severity", "") == "fatal"
        for s in irdoc.steps
    )
    validated = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    irdoc = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated,
        datasources=irdoc.datasources,
    )
    expanded = irdoc_to_expanded_sans(irdoc)
    assert "drop b" in expanded
    drop_steps = [s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "drop"]
    assert len(drop_steps) == 1
    assert drop_steps[0].params.get("cols") == ["b"]


def test_drop_empty_list_refused():
    """drop with empty column list fails at parse."""
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b\n"
        "  1,2\n"
        "end\n"
        "table t = from(in) do\n"
        "  drop\n"
        "end\n"
    )
    import pytest
    from sans.sans_script import SansScriptError, parse_sans_script
    with pytest.raises(SansScriptError) as exc_info:
        parse_sans_script(script, "drop_empty.sans")
    assert exc_info.value.code == "E_PARSE"
    assert "empty" in exc_info.value.message.lower() or "Column list" in exc_info.value.message or "cannot be empty" in exc_info.value.message.lower()


def test_drop_missing_column_refused():
    """Dropping a non-existent column fails with E_COLUMN_NOT_FOUND."""
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b\n"
        "  1,2\n"
        "end\n"
        "table t = from(in) do\n"
        "  drop z\n"
        "end\n"
    )
    irdoc = compile_sans_script(script, "drop_missing.sans", tables=set())
    assert irdoc.steps
    try:
        IRDoc(
            steps=irdoc.steps,
            tables=irdoc.tables,
            table_facts=irdoc.table_facts,
            datasources=irdoc.datasources,
        ).validate()
        assert False, "Expected E_COLUMN_NOT_FOUND"
    except UnknownBlockStep as err:
        assert err.code == "E_COLUMN_NOT_FOUND"
        assert "z" in err.message


def test_drop_outside_pipeline_fails():
    """Top-level 'drop a, b' (no table binding) fails: table 'drop' undefined or parse."""
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b\n"
        "  1,2\n"
        "end\n"
        "drop a, b\n"
    )
    irdoc = compile_sans_script(script, "drop_top.sans", tables=set())
    assert irdoc.steps
    try:
        IRDoc(
            steps=irdoc.steps,
            tables=irdoc.tables,
            table_facts=irdoc.table_facts,
            datasources=irdoc.datasources,
        ).validate()
        assert False, "Expected validation error"
    except UnknownBlockStep as err:
        assert err.code in ("SANS_VALIDATE_TABLE_UNDEFINED", "E_TYPE", "E_COLUMN_NOT_FOUND", "E_PARSE")


def test_cast_round_trip_byte():
    """expanded.sans with cast → IR → expanded.sans is byte-identical."""
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv do\n"
        "  a,b\n"
        "  1,2\n"
        "end\n"
        "table t = from(in) cast(a -> int, b -> str)\n"
    )
    irdoc = compile_sans_script(script, "cast_rt.sans", tables=set())
    validated = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    irdoc = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated,
        datasources=irdoc.datasources,
    )
    expanded = irdoc_to_expanded_sans(irdoc)
    irdoc2 = compile_sans_script(expanded, "expanded.sans", tables=set())
    assert not any(
        isinstance(s, UnknownBlockStep) and getattr(s, "severity", "") == "fatal"
        for s in irdoc2.steps
    )
    validated2 = IRDoc(
        steps=irdoc2.steps,
        tables=irdoc2.tables,
        table_facts=irdoc2.table_facts,
        datasources=irdoc2.datasources,
    ).validate()
    irdoc2 = IRDoc(
        steps=irdoc2.steps,
        tables=irdoc2.tables,
        table_facts=validated2,
        datasources=irdoc2.datasources,
    )
    expanded2 = irdoc_to_expanded_sans(irdoc2)
    assert expanded == expanded2, "cast round-trip should be byte-identical"
