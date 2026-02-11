from __future__ import annotations

from typing import Any, Dict, List, Optional

from .._loc import Loc
from ..types import parse_type_name
from . import IRDoc, OpStep, DatasourceDecl, TableFact
from .schema import validate_sans_ir


def sans_ir_to_irdoc(doc: Dict[str, Any], file_name: str = "<sans.ir>") -> IRDoc:
    validate_sans_ir(doc)

    datasources: Dict[str, DatasourceDecl] = {}
    for name in sorted(doc["datasources"]):
        ds = doc["datasources"][name]
        columns_map = ds.get("columns") or {}
        columns: Optional[List[str]] = list(sorted(columns_map.keys())) if columns_map else None
        column_types = (
            {col: parse_type_name(columns_map[col]) for col in sorted(columns_map)}
            if columns_map
            else None
        )
        datasources[name] = DatasourceDecl(
            kind=ds.get("kind", "csv"),
            path=ds.get("path"),
            columns=columns,
            column_types=column_types,
            inline_text=ds.get("inline_text"),
            inline_sha256=ds.get("inline_sha256"),
        )

    steps = []
    table_facts: Dict[str, TableFact] = {}
    for idx, raw in enumerate(doc["steps"], start=1):
        step_loc = Loc(file=file_name, line_start=idx, line_end=idx)
        step = OpStep(
            loc=step_loc,
            op=raw["op"],
            inputs=list(raw.get("inputs") or []),
            outputs=list(raw.get("outputs") or []),
            params=dict(raw.get("params") or {}),
        )
        steps.append(step)
        for out in step.outputs:
            table_facts[out] = TableFact()

    return IRDoc(
        steps=steps,
        tables=set(),
        table_facts=table_facts,
        datasources=datasources,
    )
