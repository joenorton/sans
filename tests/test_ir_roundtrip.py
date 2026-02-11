import json
from pathlib import Path

from sans.compiler import compile_sans_script
from sans.ir import IRDoc, UnknownBlockStep
from sans.ir.adapter import sans_ir_to_irdoc
from sans.ir.normalize import irdoc_to_sans_ir
from sans.sans_script import irdoc_to_expanded_sans


def _validated(irdoc: IRDoc) -> IRDoc:
    validated_facts = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    return IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=validated_facts,
        datasources=irdoc.datasources,
    )


def test_ir_roundtrip_expanded_and_ir_equality():
    fixture = Path("sans/tests/fixtures/hello_save.sans")
    source_text = fixture.read_text(encoding="utf-8")
    # Start from canonical expanded text to avoid fixture whitespace variance.
    irdoc_a = _validated(compile_sans_script(source_text, str(fixture), tables=set()))
    expanded = irdoc_to_expanded_sans(irdoc_a)

    irdoc_a = _validated(compile_sans_script(expanded, str(fixture), tables=set()))
    sans_ir_a = irdoc_to_sans_ir(irdoc_a)

    irdoc_from_ir = sans_ir_to_irdoc(sans_ir_a, file_name="hello_save.sans.ir")
    expanded_prime = irdoc_to_expanded_sans(irdoc_from_ir)

    irdoc_b = compile_sans_script(expanded_prime, "expanded.sans", tables=set())
    assert not any(
        isinstance(step, UnknownBlockStep) and getattr(step, "severity", "") == "fatal"
        for step in irdoc_b.steps
    )
    sans_ir_b = irdoc_to_sans_ir(_validated(irdoc_b))

    assert expanded == expanded_prime
    assert sans_ir_a == sans_ir_b
    assert json.dumps(sans_ir_a, sort_keys=True) == json.dumps(sans_ir_b, sort_keys=True)
