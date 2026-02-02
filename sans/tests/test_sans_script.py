import json
from pathlib import Path

from sans.compiler import _irdoc_to_dict, compile_sans_script, UnknownBlockStep
from sans.ir import IRDoc


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
