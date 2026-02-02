"""
Hello-style test for explicit save (Sprint E).
Script with save compiles and run produces output artifact.
"""
from pathlib import Path

from sans.compiler import compile_sans_script
from sans.ir import IRDoc, UnknownBlockStep, OpStep
from sans.sans_script import irdoc_to_expanded_sans


FIXTURE = Path("sans/tests/fixtures/hello_save.sans")


def test_hello_save_compiles():
    """Script with save compiles to IR with save step."""
    text = FIXTURE.read_text(encoding="utf-8")
    irdoc = compile_sans_script(text, str(FIXTURE), tables=set())
    # No fatal refusal
    if irdoc.steps and isinstance(irdoc.steps[0], UnknownBlockStep):
        if getattr(irdoc.steps[0], "severity", "") == "fatal":
            raise AssertionError(f"Expected success, got refusal: {irdoc.steps[0].code}")
    # Has at least one save step
    save_steps = [s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "save"]
    assert len(save_steps) >= 1
    assert save_steps[0].inputs == ["t"]
    assert save_steps[0].params.get("path") == "out.csv"


def test_hello_save_expanded_round_trip():
    """Script with save round-trips via expanded.sans."""
    text = FIXTURE.read_text(encoding="utf-8")
    irdoc = compile_sans_script(text, str(FIXTURE), tables=set())
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
    assert "save t to \"out.csv\"" in expanded
