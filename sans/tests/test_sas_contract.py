"""
SAS ingestion contract tests (Sprint D).
See docs/SAS_INGESTION_CONTRACT.md.
"""
from pathlib import Path

from sans.compiler import compile_script
from sans.ir import UnknownBlockStep, OpStep

KERNEL_OPS = frozenset({
    "datasource", "identity", "compute", "filter", "select", "rename",
    "sort", "aggregate", "sql_select", "format", "transpose",
    "data_step",  # SAS path may still emit data_step; recognizer uses it
    "save", "assert", "let_scalar",
})


def test_sas_refusal_macro_graph():
    """SAS script with %if/%then/%else is refused with SANS_REFUSAL_MACRO_GRAPH."""
    sas = """
data x;
  set in;
  a = 1;
run;
%if 1 %then %do;
data y;
  set x;
run;
%end;
"""
    irdoc = compile_script(sas, "script.sas", tables=set())
    assert irdoc.steps, "Expected steps (refusal step)"
    step = irdoc.steps[0]
    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_REFUSAL_MACRO_GRAPH"


def test_sas_acceptance_kernel_ops():
    """Accepted SAS script produces IR with only kernel ops (or data_step from recognizer)."""
    # Use a minimal SAS script that compiles (data step set + run)
    sas = """
data out;
  set in;
  keep a b;
run;
"""
    irdoc = compile_script(sas, "script.sas", tables={"in"})
    # Should not be a refusal
    if irdoc.steps and isinstance(irdoc.steps[0], UnknownBlockStep):
        if getattr(irdoc.steps[0], "severity", "") == "fatal":
            raise AssertionError(f"Expected acceptance, got refusal: {irdoc.steps[0].code}")
    for step in irdoc.steps:
        if isinstance(step, OpStep):
            assert step.op in KERNEL_OPS, f"Op {step.op} is not a kernel op"
