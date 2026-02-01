from __future__ import annotations

from typing import List, Set, Tuple

from sans.ir import OpStep
from sans._loc import Loc

from .ast import (
    DataStmt,
    FormatStmt,
    SelectStmt,
    SortStmt,
    SourceSpan,
    SummaryStmt,
    SansScript,
)


def lower_script(script: SansScript, file_name: str) -> Tuple[List[OpStep], Set[str]]:
    steps: List[OpStep] = []
    referenced: Set[str] = set()
    produced: Set[str] = set()
    for stmt in script.statements:
        if isinstance(stmt, FormatStmt):
            steps.append(_lower_format(stmt, file_name))
            continue
        if isinstance(stmt, DataStmt):
            steps.append(_lower_data(stmt, file_name))
            if stmt.table not in produced:
                referenced.add(stmt.table)
            produced.add(stmt.output)
            continue
        if isinstance(stmt, SortStmt):
            steps.append(_lower_sort(stmt, file_name))
            if stmt.source not in produced:
                referenced.add(stmt.source)
            produced.add(stmt.target)
            continue
        if isinstance(stmt, SummaryStmt):
            steps.append(_lower_summary(stmt, file_name))
            if stmt.source not in produced:
                referenced.add(stmt.source)
            produced.add(stmt.target)
            continue
        if isinstance(stmt, SelectStmt):
            steps.append(_lower_select(stmt, file_name))
            if stmt.source not in produced:
                referenced.add(stmt.source)
            produced.add(stmt.target)
            continue
    return steps, referenced


def _loc_from_span(file_name: str, span: SourceSpan) -> Loc:
    return Loc(file=file_name, line_start=span.start, line_end=span.end)


def _lower_format(stmt: FormatStmt, file_name: str) -> OpStep:
    mapping: dict[str, str] = {}
    default: str | None = None
    for entry in stmt.entries:
        if entry.is_other:
            default = entry.value
        else:
            mapping[entry.key] = entry.value
    params = {"name": stmt.name, "map": mapping, "other": default}
    return OpStep(
        op="format",
        inputs=[],
        outputs=[f"__format__{stmt.name}"],
        params=params,
        loc=_loc_from_span(file_name, stmt.span),
    )


def _lower_data(stmt: DataStmt, file_name: str) -> OpStep:
    input_spec: dict[str, object] = {"table": stmt.table}
    if stmt.input_keep:
        input_spec["keep"] = list(stmt.input_keep)
    if stmt.input_drop:
        input_spec["drop"] = list(stmt.input_drop)
    if stmt.input_rename:
        input_spec["rename"] = dict(stmt.input_rename)
    if stmt.input_where is not None:
        input_spec["where"] = stmt.input_where

    keep_columns = stmt.keep.columns if stmt.keep else []
    drop_columns = stmt.drop.columns if stmt.drop else []
    params = {
        "mode": "set",
        "inputs": [input_spec],
        "by": [],
        "retain": [],
        "keep": keep_columns,
        "drop": drop_columns,
        "statements": list(stmt.statements),
        "explicit_output": False,
    }
    return OpStep(
        op="data_step",
        inputs=[stmt.table],
        outputs=[stmt.output],
        params=params,
        loc=_loc_from_span(file_name, stmt.span),
    )


def _lower_sort(stmt: SortStmt, file_name: str) -> OpStep:
    return OpStep(
        op="sort",
        inputs=[stmt.source],
        outputs=[stmt.target],
        params={"by": stmt.by, "nodupkey": stmt.nodupkey},
        loc=_loc_from_span(file_name, stmt.span),
    )


def _lower_summary(stmt: SummaryStmt, file_name: str) -> OpStep:
    return OpStep(
        op="summary",
        inputs=[stmt.source],
        outputs=[stmt.target],
        params={
            "class": stmt.class_keys,
            "vars": stmt.vars,
            "stat": "mean",
            "autoname": True,
        },
        loc=_loc_from_span(file_name, stmt.span),
    )


def _lower_select(stmt: SelectStmt, file_name: str) -> OpStep:
    return OpStep(
        op="select",
        inputs=[stmt.source],
        outputs=[stmt.target],
        params={"keep": stmt.keep, "drop": stmt.drop},
        loc=_loc_from_span(file_name, stmt.span),
    )
