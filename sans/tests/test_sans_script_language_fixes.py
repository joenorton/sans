import csv
from pathlib import Path

from sans.compiler import _irdoc_to_dict, compile_sans_script, UnknownBlockStep
from sans.ir import IRDoc, OpStep
from sans.runtime import run_script


def _compile_plan(text: str, file_name: str = "script.sans") -> dict:
    irdoc = compile_sans_script(text, file_name, tables=set())
    assert not any(
        isinstance(s, UnknownBlockStep) and getattr(s, "severity", "") == "fatal"
        for s in irdoc.steps
    ), "Compilation should not fail"
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


def _plan_fingerprint(plan: dict) -> dict:
    steps = []
    for step in plan.get("steps", []):
        if step.get("kind") != "op":
            continue
        steps.append(
            {
                "op": step.get("op"),
                "inputs": step.get("inputs"),
                "outputs": step.get("outputs"),
                "params": step.get("params"),
                "transform_id": step.get("transform_id"),
                "step_id": step.get("step_id"),
            }
        )
    return {
        "steps": steps,
        "tables": plan.get("tables"),
        "datasources": plan.get("datasources"),
    }


def test_multiline_table_and_datasource_produce_same_ir():
    single_line = (
        "# sans 0.1\n"
        "datasource raw = inline_csv do\n"
        "  A,B\n"
        "  1,2\n"
        "end\n"
        "table t = from(raw) select A\n"
    )
    multi_line = (
        "# sans 0.1\n"
        "datasource raw =\n"
        "  inline_csv do\n"
        "  A,B\n"
        "  1,2\n"
        "end\n"
        "table t =\n"
        "  from(raw) select A\n"
    )
    plan_single = _plan_fingerprint(_compile_plan(single_line, "single.sans"))
    plan_multi = _plan_fingerprint(_compile_plan(multi_line, "multi.sans"))
    assert plan_single == plan_multi


def test_from_table_chain_resolves_and_emits_source_kind():
    script = (
        "# sans 0.1\n"
        "datasource raw = inline_csv do\n"
        "  a\n"
        "  1\n"
        "end\n"
        "table t1 = from(raw) select a\n"
        "table t2 = from(t1) select a\n"
        "save t2 to \"out.csv\"\n"
    )
    irdoc = compile_sans_script(script, "chain.sans", tables=set())
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
    identity_sources = [
        s.params.get("source")
        for s in irdoc.steps
        if isinstance(s, OpStep) and s.op == "identity"
    ]
    assert {"kind": "table", "name": "t1"} in identity_sources


def test_from_unknown_source_errors_with_known_lists():
    script = (
        "# sans 0.1\n"
        "datasource raw = inline_csv do\n"
        "  a\n"
        "  1\n"
        "end\n"
        "table t1 = from(raw) select a\n"
        "table t2 = from(typo) select a\n"
    )
    irdoc = compile_sans_script(script, "typo.sans", tables=set())
    assert irdoc.steps, "Expected error steps"
    assert isinstance(irdoc.steps[0], UnknownBlockStep)
    assert irdoc.steps[0].code == "E_UNDECLARED_SOURCE"
    assert "Known tables: t1" in irdoc.steps[0].message
    assert "Known datasources: raw" in irdoc.steps[0].message


def test_uppercase_headers_parse_and_execute(tmp_path: Path):
    script = (
        "# sans 0.1\n"
        "datasource raw = inline_csv columns(A,B) do\n"
        "  A,B\n"
        "  1,10\n"
        "  2,20\n"
        "end\n"
        "table t1 = from(raw) do\n"
        "  filter A > 1\n"
        "  rename(A -> A1)\n"
        "  select A1, B\n"
        "end\n"
        "save t1 to \"out.csv\"\n"
    )
    out_dir = tmp_path / "out"
    report = run_script(
        text=script,
        file_name="upper.sans",
        bindings={},
        out_dir=out_dir,
        strict=True,
    )
    assert report["status"] == "ok"
    out_csv = out_dir / "outputs" / "out.csv"
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["A1", "B"]
    assert rows[1:] == [["2", "20"]]
