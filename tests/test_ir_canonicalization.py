import json

from sans.compiler import compile_sans_script
from sans.ir import IRDoc
from sans.ir.normalize import irdoc_to_sans_ir
from sans.ir.schema import canonical_json_dumps


def _validated(irdoc: IRDoc) -> IRDoc:
    facts = IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=irdoc.table_facts,
        datasources=irdoc.datasources,
    ).validate()
    return IRDoc(
        steps=irdoc.steps,
        tables=irdoc.tables,
        table_facts=facts,
        datasources=irdoc.datasources,
    )


def test_sans_ir_canonicalization_strips_execution_fields():
    script = """# sans 0.1
datasource in = inline_csv columns(a:int, b:int) do
  a,b
  1,2
end

table t = from(in) do
  derive(c = a + b)
end

save t to "out.csv"
"""
    irdoc = _validated(compile_sans_script(script, "canonical.sans", tables=set()))
    sans_ir = irdoc_to_sans_ir(irdoc)

    for step in sans_ir["steps"]:
        assert "transform_id" not in step
        assert "transform_class_id" not in step
        assert "step_id" not in step
        assert "loc" not in step


def test_sans_ir_canonicalization_is_deterministic():
    script = """# sans 0.1
datasource in = inline_csv columns(a:int) do
  a
  1
end
table t = from(in) do
  derive(b = a + 1)
end
save t to "out.csv"
"""
    irdoc = _validated(compile_sans_script(script, "deterministic.sans", tables=set()))
    a = irdoc_to_sans_ir(irdoc)
    b = irdoc_to_sans_ir(irdoc)

    assert a == b
    assert canonical_json_dumps(a) == canonical_json_dumps(b)
    assert json.loads(canonical_json_dumps(a)) == a
