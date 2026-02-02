"""
Property-style invariant: no legacy param shapes survive IRDoc.validate().
Load plan.ir.json from compile+emit runs (SAS + SANS fixtures), rebuild IRDoc,
call validate(), then assert no FORBIDDEN_KEYS and per-op canonical shapes.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from sans.compiler import emit_check_artifacts
from sans.ir import (
    IRDoc,
    OpStep,
    Loc,
    TableFact,
    DatasourceDecl,
)


# Legacy keys that must not appear in any step.params after validate()
FORBIDDEN_KEYS = {
    "asc",
    "keep_raw",
    "drop_raw",
    "keep",
    "class",
    "var",
    "vars",
    "stats",
    "summary",
    "mappings",
    "map",
    "autoname",
    "naming",
}

# All current IR ops; update docs/IR_CANONICAL_PARAMS.md when adding ops
KNOWN_OPS = {
    "datasource",
    "compute",
    "filter",
    "select",
    "rename",
    "sort",
    "cast",
    "aggregate",
    "identity",
    "save",
    "assert",
    "let_scalar",
    "const",
    "data_step",
    "transpose",
    "sql_select",
    "format",
}


def _plan_dict_to_irdoc(plan: dict) -> IRDoc:
    """Build IRDoc from plan.ir.json dict (op steps only). Same contract as emit output."""
    steps = []
    for s in plan.get("steps", []):
        if s.get("kind") != "op":
            continue
        loc_d = s.get("loc") or {}
        loc = Loc(
            file=loc_d.get("file", ""),
            line_start=loc_d.get("line_start", 0),
            line_end=loc_d.get("line_end", 0),
        )
        steps.append(
            OpStep(
                loc=loc,
                op=s["op"],
                inputs=s.get("inputs", []),
                outputs=s.get("outputs", []),
                params=dict(s.get("params", {})),
            )
        )
    tables = set(plan.get("tables", []))
    # Only seed table_facts with pre-declared tables so validate() can add step outputs without collision
    table_facts = {
        k: TableFact(sorted_by=v.get("sorted_by"))
        for k, v in plan.get("table_facts", {}).items()
        if k in tables
    }
    datasources = {}
    for name, ds in plan.get("datasources", {}).items():
        datasources[name] = DatasourceDecl(
            kind=ds.get("kind", "csv"),
            path=ds.get("path"),
            columns=ds.get("columns"),
            inline_text=ds.get("inline_text"),
            inline_sha256=ds.get("inline_sha256"),
        )
    return IRDoc(
        steps=steps,
        tables=tables,
        table_facts=table_facts,
        datasources=datasources,
    )


def _assert_no_legacy_keys(irdoc: IRDoc) -> None:
    for i, step in enumerate(irdoc.steps):
        if not isinstance(step, OpStep):
            continue
        for key in step.params:
            assert key not in FORBIDDEN_KEYS, (
                f"Step {i} op={step.op!r} has forbidden param key {key!r} after validate(). "
                "Legacy keys must be normalized in IRDoc.validate()."
            )


def _assert_per_op_canonical_shapes(irdoc: IRDoc) -> None:
    for i, step in enumerate(irdoc.steps):
        if not isinstance(step, OpStep):
            continue
        params = step.params
        op = step.op

        if op == "sort":
            by = params.get("by")
            assert by is not None, f"Step {i} sort: missing 'by'"
            assert isinstance(by, list), f"Step {i} sort: 'by' must be list"
            for j, entry in enumerate(by):
                assert isinstance(entry, dict), f"Step {i} sort by[{j}]: must be dict"
                assert "col" in entry and "desc" in entry, f"Step {i} sort by[{j}]: must have col, desc"
                assert "asc" not in entry, f"Step {i} sort by[{j}]: must not have 'asc' (canonical is desc)"
                assert isinstance(entry["col"], str) and isinstance(entry["desc"], bool)

        elif op == "rename":
            mapping = params.get("mapping")
            assert mapping is not None, f"Step {i} rename: missing 'mapping'"
            assert isinstance(mapping, list), f"Step {i} rename: 'mapping' must be list"
            for j, entry in enumerate(mapping):
                assert isinstance(entry, dict), f"Step {i} rename mapping[{j}]: must be dict"
                assert "from" in entry and "to" in entry
                assert isinstance(entry["from"], str) and isinstance(entry["to"], str)

        elif op == "select":
            cols = params.get("cols")
            drop = params.get("drop")
            has_cols = cols is not None and (isinstance(cols, list) and len(cols) > 0)
            has_drop = drop is not None and (isinstance(drop, list) and len(drop) > 0)
            assert has_cols or has_drop, f"Step {i} select: exactly one of cols or drop (non-empty list) required"
            assert not (has_cols and has_drop), f"Step {i} select: cannot have both cols and drop"
            if has_cols:
                assert all(isinstance(c, str) for c in cols)
            if has_drop:
                assert all(isinstance(d, str) for d in drop)

        elif op == "aggregate":
            group_by = params.get("group_by")
            metrics = params.get("metrics")
            assert group_by is not None, f"Step {i} aggregate: missing 'group_by'"
            assert isinstance(group_by, list), f"Step {i} aggregate: 'group_by' must be list"
            assert metrics is not None, f"Step {i} aggregate: missing 'metrics'"
            assert isinstance(metrics, list), f"Step {i} aggregate: 'metrics' must be list"
            assert len(metrics) > 0, f"Step {i} aggregate: 'metrics' must be non-empty"
            for j, m in enumerate(metrics):
                assert isinstance(m, dict), f"Step {i} aggregate metrics[{j}]: must be dict"
                assert "name" in m and "op" in m and "col" in m
                assert isinstance(m["name"], str) and isinstance(m["op"], str) and isinstance(m["col"], str)

        elif op == "compute":
            mode = params.get("mode")
            assigns = params.get("assignments") or params.get("assign")
            if mode is not None:
                assert mode in ("derive", "update"), f"Step {i} compute: mode must be derive or update"
            assert assigns is not None and isinstance(assigns, list), (
                f"Step {i} compute: assignments or assign list required"
            )

        elif op == "cast":
            casts = params.get("casts")
            assert casts is not None, f"Step {i} cast: missing 'casts'"
            assert isinstance(casts, list), f"Step {i} cast: 'casts' must be list"
            assert len(casts) > 0, f"Step {i} cast: 'casts' must be non-empty"
            for j, c in enumerate(casts):
                assert isinstance(c, dict), f"Step {i} cast casts[{j}]: must be dict"
                assert "col" in c and "to" in c
                assert isinstance(c["col"], str) and isinstance(c["to"], str)
                assert c.get("on_error", "fail") in ("fail", "null")
                assert isinstance(c.get("trim", False), bool)


def _assert_known_ops(irdoc: IRDoc) -> None:
    for i, step in enumerate(irdoc.steps):
        if not isinstance(step, OpStep):
            continue
        assert step.op in KNOWN_OPS, (
            f"Step {i} has unknown op {step.op!r}. "
            "Update KNOWN_OPS in this test and docs/IR_CANONICAL_PARAMS.md."
        )


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
V1_CANON_SAS = FIXTURES_DIR / "v1_canon.sas"
V1_CANON_SANS = FIXTURES_DIR / "v1_canon.sans"


def test_no_legacy_params_after_validate_sas_and_sans(tmp_path):
    """
    Compile+emit one SAS and one SANS fixture; load each plan.ir.json, build IRDoc,
    call validate(), then assert no legacy keys and canonical shapes.
    """
    out_sas = tmp_path / "sas"
    out_sans = tmp_path / "sans"
    out_sas.mkdir()
    out_sans.mkdir()

    # SAS: compute, select, rename, sort, aggregate
    sas_text = V1_CANON_SAS.read_text(encoding="utf-8")
    _, report_sas = emit_check_artifacts(
        sas_text,
        "v1_canon.sas",
        tables={"in"},
        out_dir=out_sas,
    )
    assert report_sas["status"] == "ok", report_sas.get("primary_error", report_sas)

    # SANS: datasource, compute, filter, select, rename, sort, aggregate, save, const, let_scalar
    sans_text = V1_CANON_SANS.read_text(encoding="utf-8")
    _, report_sans = emit_check_artifacts(
        sans_text,
        "v1_canon.sans",
        tables=set(),
        out_dir=out_sans,
    )
    assert report_sans["status"] == "ok", report_sans.get("primary_error", report_sans)

    for out_dir in (out_sas, out_sans):
        plan_path = out_dir / "plan.ir.json"
        assert plan_path.exists(), f"Expected {plan_path}"
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        irdoc = _plan_dict_to_irdoc(plan)
        irdoc.validate()
        _assert_no_legacy_keys(irdoc)
        _assert_per_op_canonical_shapes(irdoc)
        _assert_known_ops(irdoc)
