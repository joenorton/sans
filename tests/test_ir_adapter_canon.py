"""
Canonical-shape gate at IRDoc ingress: sans.ir with legacy param keys must be refused.
Validate purity: validate() must not mutate step.params.
"""
from __future__ import annotations

import copy
import pytest

from sans.ir import IRDoc, OpStep, UnknownBlockStep, TableFact
from sans.ir.adapter import sans_ir_to_irdoc
from sans._loc import Loc


def test_sans_ir_with_legacy_aggregate_keys_refused():
    """Loading sans.ir with aggregate step using legacy keys (class, var, stats) must raise SANS_IR_CANON_SHAPE_AGGREGATE."""
    doc = {
        "version": "0.1",
        "datasources": {"ds1": {"kind": "csv", "path": "x.csv"}},
        "steps": [
            {
                "id": "s1",
                "op": "datasource",
                "inputs": [],
                "outputs": ["__datasource__ds1"],
                "params": {"name": "ds1", "kind": "csv"},
            },
            {
                "id": "s2",
                "op": "aggregate",
                "inputs": ["__datasource__ds1"],
                "outputs": ["out"],
                "params": {"class": ["x"], "var": ["y"], "stats": ["mean"]},
            },
            {
                "id": "s3",
                "op": "save",
                "inputs": ["out"],
                "outputs": [],
                "params": {"path": "out.csv"},
            },
        ],
    }
    with pytest.raises(UnknownBlockStep) as exc_info:
        sans_ir_to_irdoc(doc, file_name="<test>")
    assert exc_info.value.code == "SANS_IR_CANON_SHAPE_AGGREGATE"


def test_validate_purity_no_mutation():
    """Calling validate() twice must not mutate step.params (validate is read-only)."""
    steps = [
        OpStep(
            op="select",
            inputs=["t0"],
            outputs=["s1"],
            params={"cols": ["a", "b"]},
            loc=Loc("test.sans", 1, 1),
        ),
        OpStep(
            op="sort",
            inputs=["s1"],
            outputs=["s2"],
            params={"by": [{"col": "a", "desc": False}]},
            loc=Loc("test.sans", 2, 2),
        ),
    ]
    irdoc = IRDoc(
        steps=steps,
        tables={"t0"},
        table_facts={"t0": TableFact()},
        datasources={},
    )
    snapshot = [copy.deepcopy(getattr(s, "params", {})) for s in irdoc.steps if isinstance(s, OpStep)]
    irdoc.validate()
    irdoc.validate()
    for i, step in enumerate(irdoc.steps):
        if isinstance(step, OpStep):
            assert step.params == snapshot[i], f"Step {i} params mutated by validate()"
