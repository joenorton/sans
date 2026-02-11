import json
from pathlib import Path

from sans.__main__ import main
from sans.compiler import compile_sans_script
from sans.ir import IRDoc
from sans.ir.normalize import irdoc_to_sans_ir


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


def _output_hashes(report_path: Path) -> dict[str, str]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return {o["path"]: o["sha256"] for o in report.get("outputs", [])}


def test_run_ir_equivalence_with_run(tmp_path: Path):
    script_path = tmp_path / "script.sans"
    script_text = """# sans 0.1
datasource in = inline_csv columns(a:int, b:int) do
  a,b
  6,7
  3,2
end

table t = from(in) do
  derive(base2 = a * 2)
  filter(base2 > 10)
  select a, base2
end

save t to "out.csv"
"""
    script_path.write_text(script_text, encoding="utf-8")

    ir_doc = _validated(compile_sans_script(script_text, str(script_path), tables=set()))
    sans_ir_path = tmp_path / "script.sans.ir"
    sans_ir_path.write_text(
        json.dumps(irdoc_to_sans_ir(ir_doc), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    out_run = tmp_path / "out_run"
    out_ir = tmp_path / "out_ir"
    rc_run = main(["run", str(script_path), "--out", str(out_run)])
    rc_ir = main(["run-ir", str(sans_ir_path), "--out", str(out_ir)])
    assert rc_run == 0
    assert rc_ir == 0

    report_run = json.loads((out_run / "report.json").read_text(encoding="utf-8"))
    report_ir = json.loads((out_ir / "report.json").read_text(encoding="utf-8"))
    assert report_run["status"] == report_ir["status"]
    assert _output_hashes(out_run / "report.json") == _output_hashes(out_ir / "report.json")
