from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, List, Dict

from ._loc import Loc
from .type_infer import TypeInferenceError, infer_table_schema_types
from .types import Type


# -----------------------------------------------------------------------------
# Datasource pseudo-table helpers
# -----------------------------------------------------------------------------
DATASOURCE_PREFIX = "__datasource__"


def ds_input(name: str) -> str:
    return f"{DATASOURCE_PREFIX}{name}"


def is_ds_input(s: str) -> bool:
    return s.startswith(DATASOURCE_PREFIX)


def ds_name_from_input(s: str) -> str:
    return s[len(DATASOURCE_PREFIX):]


# -----------------------------------------------------------------------------
# Sort "by" canonical shape: list[{"col": str, "desc": bool}]
# -----------------------------------------------------------------------------
def normalize_sort_by(by: Any, loc: Loc) -> List[Dict[str, Any]]:
    """
    Normalize sort 'by' param to canonical form: list[{"col": str, "desc": bool}].
    Accepts: (a) legacy list[str] -> [{"col": s, "desc": False}, ...];
             (b) list[dict] with "col" and "desc" or "asc" -> canonical {"col", "desc"}.
    Rejects empty/missing col. Preserves list order.
    Raises UnknownBlockStep with loc on invalid input.
    """
    if by is None:
        raise UnknownBlockStep(
            code="SANS_VALIDATE_SORT_MISSING_BY",
            message="PROC SORT operation requires 'by' variables.",
            loc=loc,
        )
    if not isinstance(by, list):
        raise UnknownBlockStep(
            code="SANS_VALIDATE_SORT_BY_INVALID",
            message="Sort 'by' must be a list of column names or {col, desc} objects.",
            loc=loc,
        )
    result: List[Dict[str, Any]] = []
    for i, item in enumerate(by):
        if isinstance(item, str):
            col = item.strip() if item else ""
            if not col:
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_SORT_BY_EMPTY_COL",
                    message="Sort 'by' contains an empty column name.",
                    loc=loc,
                )
            result.append({"col": col, "desc": False})
        elif isinstance(item, dict):
            col = item.get("col")
            if col is None or (isinstance(col, str) and not col.strip()):
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_SORT_BY_EMPTY_COL",
                    message="Sort 'by' entry missing or empty 'col'.",
                    loc=loc,
                )
            col = col.strip() if isinstance(col, str) else str(col)
            desc = item.get("desc", not item.get("asc", True))
            result.append({"col": col, "desc": bool(desc)})
        else:
            raise UnknownBlockStep(
                code="SANS_VALIDATE_SORT_BY_INVALID",
                message=f"Sort 'by' entry must be a string or {{col, desc}} dict, got {type(item).__name__}.",
                loc=loc,
            )
    if not result:
        raise UnknownBlockStep(
            code="SANS_VALIDATE_SORT_MISSING_BY",
            message="PROC SORT operation requires at least one 'by' column.",
            loc=loc,
        )
    return result


# -----------------------------------------------------------------------------
# Select canonical: params["cols"] = list[str] (keep) or params["drop"] = list[str] (drop)
# -----------------------------------------------------------------------------
def normalize_select_cols(raw: Any, loc: Loc) -> List[str]:
    """
    Normalize select column list to canonical list[str].
    Accepts: list[str], comma-separated string "a,b,c", list[dict] with {"col": "a"}.
    Rejects empty, non-string col names. Preserves order.
    """
    if raw is None:
        raise UnknownBlockStep(
            code="SANS_VALIDATE_SELECT_MISSING_COLS",
            message="Select operation requires 'keep' or 'drop' column list.",
            loc=loc,
        )
    if isinstance(raw, str):
        raw = [s.strip() for s in raw.split(",") if s.strip()]
    if not isinstance(raw, list):
        raise UnknownBlockStep(
            code="SANS_VALIDATE_SELECT_COLS_INVALID",
            message="Select cols must be a list of column names or comma-separated string.",
            loc=loc,
        )
    result: List[str] = []
    for item in raw:
        if isinstance(item, str):
            col = item.strip()
            if not col:
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_SELECT_COLS_EMPTY",
                    message="Select contains an empty column name.",
                    loc=loc,
                )
            result.append(col)
        elif isinstance(item, dict):
            col = item.get("col")
            if col is None or (isinstance(col, str) and not col.strip()):
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_SELECT_COLS_EMPTY",
                    message="Select entry missing or empty 'col'.",
                    loc=loc,
                )
            result.append(col.strip() if isinstance(col, str) else str(col))
        else:
            raise UnknownBlockStep(
                code="SANS_VALIDATE_SELECT_COLS_INVALID",
                message=f"Select entry must be string or {{col}} dict, got {type(item).__name__}.",
                loc=loc,
            )
    return result


# -----------------------------------------------------------------------------
# Rename canonical: params["mapping"] = list[{"from": str, "to": str}] (ordered list)
# -----------------------------------------------------------------------------
def normalize_rename_mapping(raw: Any, loc: Loc) -> List[Dict[str, Any]]:
    """
    Normalize rename to canonical list[{"from": str, "to": str}].
    Accepts: dict {"old": "new", ...} -> list sorted by "from" (deterministic).
    Rejects empty; rejects empty from/to; rejects from==to.
    """
    if raw is None:
        raise UnknownBlockStep(
            code="SANS_VALIDATE_RENAME_MISSING_MAPPING",
            message="Rename operation requires a mapping.",
            loc=loc,
        )
    if isinstance(raw, dict):
        raw = [{"from": k, "to": v} for k, v in sorted(raw.items())]
    if not isinstance(raw, list):
        raise UnknownBlockStep(
            code="SANS_VALIDATE_RENAME_MAPPING_INVALID",
            message="Rename mapping must be a dict or list of {from, to}.",
            loc=loc,
        )
    result: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            fr = item.get("from") or item.get("old")
            to = item.get("to") or item.get("new")
            if fr is None or (isinstance(fr, str) and not fr.strip()):
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_RENAME_EMPTY_FROM",
                    message="Rename entry missing or empty 'from'.",
                    loc=loc,
                )
            if to is None or (isinstance(to, str) and not to.strip()):
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_RENAME_EMPTY_TO",
                    message="Rename entry missing or empty 'to'.",
                    loc=loc,
                )
            fr = fr.strip() if isinstance(fr, str) else str(fr)
            to = to.strip() if isinstance(to, str) else str(to)
            if fr == to:
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_RENAME_IDENTICAL",
                    message=f"Rename from and to must differ: '{fr}'.",
                    loc=loc,
                )
            result.append({"from": fr, "to": to})
        else:
            raise UnknownBlockStep(
                code="SANS_VALIDATE_RENAME_MAPPING_INVALID",
                message=f"Rename entry must be {{from, to}} dict, got {type(item).__name__}.",
                loc=loc,
            )
    if not result:
        raise UnknownBlockStep(
            code="SANS_VALIDATE_RENAME_MISSING_MAPPING",
            message="Rename mapping must not be empty.",
            loc=loc,
        )
    return result


# -----------------------------------------------------------------------------
# Aggregate canonical: params["group_by"] = list[str], params["metrics"] = list[{"name": str, "op": str, "col": str}]
# -----------------------------------------------------------------------------
AGGREGATE_ALLOWED_OPS = frozenset({"mean", "sum", "min", "max", "count", "n"})

# -----------------------------------------------------------------------------
# Cast canonical: params["casts"] = list[{"col": str, "to": str, "on_error": "fail"|"null", "trim": bool}]
# -----------------------------------------------------------------------------
CAST_ALLOWED_TYPES = frozenset({"str", "int", "decimal", "bool", "date", "datetime"})


def normalize_cast_params(params: Dict[str, Any], loc: Loc) -> None:
    """
    Normalize cast params in place to canonical:
    - params["casts"] = list[{"col": str, "to": str, "on_error": "fail"|"null", "trim": bool}]
    Rejects empty casts; validates "to" against CAST_ALLOWED_TYPES.
    Defaults: on_error="fail", trim=False.
    """
    raw = params.get("casts")
    if not isinstance(raw, list) or len(raw) == 0:
        raise UnknownBlockStep(
            code="SANS_VALIDATE_CAST_MISSING_CASTS",
            message="Cast operation requires non-empty 'casts' list.",
            loc=loc,
        )
    result: List[Dict[str, Any]] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise UnknownBlockStep(
                code="SANS_VALIDATE_CAST_ENTRY_INVALID",
                message=f"Cast entry must be {{col, to, on_error?, trim?}}, got {type(item).__name__}.",
                loc=loc,
            )
        col = item.get("col")
        if col is None or (isinstance(col, str) and not col.strip()):
            raise UnknownBlockStep(
                code="SANS_VALIDATE_CAST_EMPTY_COL",
                message="Cast entry missing or empty 'col'.",
                loc=loc,
            )
        col = col.strip() if isinstance(col, str) else str(col)
        to = item.get("to")
        if to is None or (isinstance(to, str) and not to.strip()):
            raise UnknownBlockStep(
                code="SANS_VALIDATE_CAST_EMPTY_TO",
                message="Cast entry missing or empty 'to' (target type).",
                loc=loc,
            )
        to = (to.strip() if isinstance(to, str) else str(to)).lower()
        if to not in CAST_ALLOWED_TYPES:
            raise UnknownBlockStep(
                code="SANS_VALIDATE_CAST_INVALID_TYPE",
                message=f"Cast target type must be one of {sorted(CAST_ALLOWED_TYPES)}, got '{to}'.",
                loc=loc,
            )
        on_error = item.get("on_error", "fail")
        if isinstance(on_error, str):
            on_error = on_error.strip().lower()
        if on_error not in ("fail", "null"):
            raise UnknownBlockStep(
                code="SANS_VALIDATE_CAST_ON_ERROR_INVALID",
                message="Cast on_error must be 'fail' or 'null'.",
                loc=loc,
            )
        trim = item.get("trim", False)
        if not isinstance(trim, bool):
            trim = str(trim).strip().lower() in ("true", "1", "yes")
        result.append({"col": col, "to": to, "on_error": on_error, "trim": bool(trim)})
    params["casts"] = result


def normalize_aggregate_params(params: Dict[str, Any], loc: Loc) -> None:
    """
    Normalize aggregate params in place to canonical:
    - params["group_by"] = list[str] (may be empty for global agg)
    - params["metrics"] = list[{"name": str, "op": str, "col": str}]
    Legacy: params["class"] -> group_by; params["var"] or params["vars"] + params["stats"] -> metrics.
    Rejects empty metrics; validates op against AGGREGATE_ALLOWED_OPS.
    Idempotent: if metrics (and group_by) already canonical, just pop legacy keys and validate.
    """
    existing_metrics = params.get("metrics")
    if isinstance(existing_metrics, list) and len(existing_metrics) > 0:
        # Already canonical; ensure group_by, pop legacy, validate metrics
        params["group_by"] = params.get("group_by") if params.get("group_by") is not None else []
        params.pop("class", None)
        params.pop("var", None)
        params.pop("vars", None)
        params.pop("stats", None)
        params.pop("autoname", None)
        params.pop("naming", None)
        for m in existing_metrics:
            if not isinstance(m, dict) or m.get("op") not in AGGREGATE_ALLOWED_OPS:
                raise UnknownBlockStep(
                    code="SANS_VALIDATE_AGGREGATE_OP_INVALID",
                    message=f"Aggregate metric must have op in {sorted(AGGREGATE_ALLOWED_OPS)}.",
                    loc=loc,
                )
        return
    class_vars = params.get("class") or params.get("group_by") or []
    var_vars = params.get("var") or params.get("vars") or []
    stats = params.get("stats") or ["mean"]
    if isinstance(class_vars, str):
        class_vars = [s.strip() for s in class_vars.split(",") if s.strip()]
    group_by: List[str] = []
    for c in class_vars:
        if isinstance(c, str) and c.strip():
            group_by.append(c.strip())
        elif isinstance(c, dict) and c.get("col"):
            group_by.append(str(c["col"]).strip())
    if isinstance(var_vars, str):
        var_vars = [s.strip() for s in var_vars.split(",") if s.strip()]
    var_list: List[str] = []
    for v in var_vars:
        if isinstance(v, str) and v.strip():
            var_list.append(v.strip())
        elif isinstance(v, dict) and v.get("col"):
            var_list.append(str(v["col"]).strip())
    if isinstance(stats, str):
        stats = [s.strip() for s in stats.split(",") if s.strip()]
    stat_list: List[str] = []
    for s in stats:
        if isinstance(s, str) and s.strip():
            stat_list.append(s.strip().lower())
    if not stat_list:
        stat_list = ["mean"]
    for op in stat_list:
        if op not in AGGREGATE_ALLOWED_OPS:
            raise UnknownBlockStep(
                code="SANS_VALIDATE_AGGREGATE_OP_INVALID",
                message=f"Aggregate op must be one of {sorted(AGGREGATE_ALLOWED_OPS)}, got '{op}'.",
                loc=loc,
            )
    if not var_list:
        raise UnknownBlockStep(
            code="SANS_VALIDATE_AGGREGATE_METRICS_EMPTY",
            message="Aggregate requires at least one metric variable (var/vars).",
            loc=loc,
        )
    metrics: List[Dict[str, Any]] = []
    for col in var_list:
        for op in stat_list:
            metrics.append({"name": f"{col}_{op}", "op": op, "col": col})
    params["group_by"] = group_by
    params["metrics"] = metrics
    params.pop("class", None)
    params.pop("var", None)
    params.pop("vars", None)
    params.pop("stats", None)
    params.pop("autoname", None)
    params.pop("naming", None)


@dataclass(frozen=True)
class DatasourceDecl:
    kind: str                 # "csv" | "inline_csv"
    path: Optional[str] = None
    columns: Optional[list[str]] = None
    column_types: Optional[dict[str, Type]] = None
    inline_text: Optional[str] = None   # normalized csv text for inline_csv
    inline_sha256: Optional[str] = None

@dataclass
class TableFact:
    """Stores metadata about a table."""
    sorted_by: Optional[List[str]] = None  # List of column names by which the table is sorted


@dataclass
class Step:
    # Base class for IR steps. Not intended to be instantiated directly.
    # Use OpStep or UnknownBlockStep.
    kind: str
    loc: Loc


@dataclass
class OpStep(Step):
    kind: str = field(default="op", init=False)
    op: str
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class UnknownBlockStep(Step, Exception):  # Inherit from both Step and Exception
    kind: str = field(default="block", init=False)
    code: str
    message: str
    severity: str = field(default="fatal")  # Currently always fatal for blocks


@dataclass(frozen=True)
class IRDoc:
    steps: list[Step] = field(default_factory=list)
    tables: set[str] = field(default_factory=set)  # Pre-declared tables
    table_facts: Dict[str, TableFact] = field(default_factory=dict)
    datasources: Dict[str, DatasourceDecl] = field(default_factory=dict)  # Declared datasources

    def __post_init__(self):
        # Initialize table_facts for any pre-declared tables
        for table_name in self.tables:
            if table_name not in self.table_facts:
                # Need to bypass frozen=True for modification in __post_init__
                object.__setattr__(self, "table_facts", {**self.table_facts, table_name: TableFact()})

    # Canonical params: select, rename, sort, aggregate are normalized here; all other ops
    # use ingestion shape as-is. Consumers (runtime, printer, hash, registry) assume canonical
    # only. See docs/IR_CANONICAL_PARAMS.md.

    def validate(self) -> Dict[str, TableFact]:
        """
        Validates the IRDoc for semantic correctness, including table existence and sortedness.
        Raises UnknownBlockStep if any validation rule is violated.
        Returns:
            A dictionary mapping table names to their validated TableFact instances.
        """
        # Create a mutable copy of table_facts for validation
        current_table_facts: Dict[str, TableFact] = {k: TableFact(v.sorted_by) for k, v in self.table_facts.items()}

        # Ensure all initial tables from self.tables are in current_table_facts
        for table_name in self.tables:
            if table_name not in current_table_facts:
                current_table_facts[table_name] = TableFact()

        for step in self.steps:
            if isinstance(step, UnknownBlockStep):
                # If an unknown block step is already in the IR and it's fatal, it's an error.
                if step.severity == "fatal":
                    raise step
                continue

            if isinstance(step, OpStep):
                # --- Input Validation ---
                for input_table in step.inputs:
                    if is_ds_input(input_table):
                        ds_name = ds_name_from_input(input_table)
                        if ds_name not in self.datasources:
                            raise UnknownBlockStep(
                                code="SANS_VALIDATE_DATASOURCE_UNDEFINED",
                                message=f"Datasource '{ds_name}' used by operation '{step.op}' is not defined.",
                                loc=step.loc,
                            )
                        # Datasource inputs don't exist as tables in current_table_facts
                        continue

                    if input_table not in current_table_facts:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_TABLE_UNDEFINED",
                            message=f"Input table '{input_table}' used by operation '{step.op}' is not defined.",
                            loc=step.loc,
                        )

                # --- Output Validation ---
                if not step.outputs and step.op not in ("save", "assert", "let_scalar", "const"):
                    # Most OpSteps produce an output table; save/assert/let_scalar do not.
                    raise UnknownBlockStep(
                        code="SANS_INTERNAL_COMPILER_ERROR",
                        message=f"Operation '{step.op}' does not define an output table.",
                        loc=step.loc,
                    )

                # Normalize params to canonical shapes (single choke point)
                if step.op == "select":
                    keep_raw = step.params.get("keep")
                    drop_raw = step.params.get("drop")
                    if keep_raw is not None and keep_raw:
                        step.params["cols"] = normalize_select_cols(keep_raw, step.loc)
                        step.params.pop("keep", None)
                        step.params.pop("drop", None)
                    elif drop_raw is not None and drop_raw:
                        step.params["drop"] = normalize_select_cols(drop_raw, step.loc)
                        step.params.pop("keep", None)
                        step.params.pop("cols", None)
                    elif not step.params.get("cols") and not step.params.get("drop"):
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_SELECT_MISSING_COLS",
                            message="Select operation requires non-empty keep or drop column list.",
                            loc=step.loc,
                        )
                elif step.op == "rename":
                    raw = step.params.get("mappings") or step.params.get("map") or step.params.get("mapping")
                    step.params["mapping"] = normalize_rename_mapping(raw, step.loc)
                    step.params.pop("mappings", None)
                    step.params.pop("map", None)
                elif step.op == "aggregate":
                    normalize_aggregate_params(step.params, step.loc)
                elif step.op == "cast":
                    normalize_cast_params(step.params, step.loc)
                elif step.op == "drop":
                    raw = step.params.get("cols") or step.params.get("drop")
                    step.params["cols"] = normalize_select_cols(raw, step.loc)
                    step.params.pop("drop", None)
                    if not step.params["cols"]:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_DROP_EMPTY",
                            message="Drop operation requires non-empty column list.",
                            loc=step.loc,
                        )

                # Determine sortedness for output tables based on the operation.
                # Choose the first *real table* input as the sortedness reference.
                input_sorted_by: Optional[List[str]] = None
                for t in step.inputs:
                    if is_ds_input(t):
                        continue
                    if t in current_table_facts:
                        input_sorted_by = current_table_facts[t].sorted_by
                        break

                output_sorted_by: Optional[List[str]] = None  # Default to unsorted

                if step.op == "sort":
                    # Single choke point: normalize to canonical list[{"col": str, "desc": bool}]
                    step.params["by"] = normalize_sort_by(step.params.get("by"), step.loc)
                    by_vars = step.params["by"]
                    output_sorted_by = [v["col"] for v in by_vars]

                elif step.op == "data_step":
                    by_vars = step.params.get("by") or []
                    keep = step.params.get("keep") or []
                    if by_vars:
                        # BY-group processing requires *table inputs* be sorted appropriately.
                        for input_table in step.inputs:
                            if is_ds_input(input_table):
                                continue
                            input_fact = current_table_facts.get(input_table)
                            input_sorted = input_fact.sorted_by if input_fact else None
                            if not input_sorted or input_sorted[: len(by_vars)] != by_vars:
                                raise UnknownBlockStep(
                                    code="SANS_VALIDATE_ORDER_REQUIRED",
                                    message=f"Input table '{input_table}' must be sorted by {by_vars} for BY-group processing.",
                                    loc=step.loc,
                                )
                        # Keep/drop can destroy the BY keys -> conservatively drop sortedness.
                        if keep and not all(k in keep for k in by_vars):
                            output_sorted_by = None
                        else:
                            output_sorted_by = list(by_vars)
                    else:
                        output_sorted_by = input_sorted_by

                elif step.op == "transpose":
                    by_vars = step.params.get("by") or []
                    id_var = step.params.get("id")
                    var_var = step.params.get("var")
                    if not by_vars:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_KEYS_REQUIRED",
                            message="PROC TRANSPOSE requires BY keys.",
                            loc=step.loc,
                        )
                    if not id_var or not var_var:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_KEYS_REQUIRED",
                            message="PROC TRANSPOSE requires ID and VAR options.",
                            loc=step.loc,
                        )
                    for input_table in step.inputs:
                        if is_ds_input(input_table):
                            continue
                        input_fact = current_table_facts.get(input_table)
                        input_sorted = input_fact.sorted_by if input_fact else None
                        if not input_sorted or input_sorted[: len(by_vars)] != by_vars:
                            raise UnknownBlockStep(
                                code="SANS_VALIDATE_ORDER_REQUIRED",
                                message=f"Input table '{input_table}' must be sorted by {by_vars} for PROC TRANSPOSE.",
                                loc=step.loc,
                            )
                    output_sorted_by = list(by_vars)

                elif step.op == "sql_select":
                    group_by = step.params.get("group_by") or []
                    output_sorted_by = list(group_by) if group_by else None

                elif step.op == "aggregate":
                    group_by = step.params.get("group_by") or []
                    output_sorted_by = list(group_by) if group_by else None

                elif step.op == "format":
                    output_sorted_by = None

                elif step.op == "select":
                    # select preserves sortedness if sort keys are not dropped
                    cols = step.params.get("cols") or []
                    drop = step.params.get("drop") or []
                    if input_sorted_by is None:
                        output_sorted_by = None
                    elif cols:
                        output_sorted_by = input_sorted_by if all(k in cols for k in input_sorted_by) else None
                    elif drop:
                        output_sorted_by = None if any(k in drop for k in input_sorted_by) else input_sorted_by
                    else:
                        output_sorted_by = input_sorted_by

                elif step.op == "filter":
                    # filter preserves sortedness
                    output_sorted_by = input_sorted_by

                elif step.op == "compute":
                    # compute preserves sortedness (doesn't change order)
                    output_sorted_by = input_sorted_by

                elif step.op == "rename":
                    # rename drops sortedness unless we can map keys (conservative: drop)
                    output_sorted_by = None

                elif step.op == "identity":
                    # identity preserves sortedness
                    output_sorted_by = input_sorted_by

                elif step.op == "cast":
                    # cast preserves order (doesn't change row order)
                    output_sorted_by = input_sorted_by

                elif step.op == "drop":
                    drop_cols = set(step.params.get("cols") or [])
                    if input_sorted_by is None:
                        output_sorted_by = None
                    elif any(k in drop_cols for k in input_sorted_by):
                        output_sorted_by = None
                    else:
                        output_sorted_by = input_sorted_by

                # --- Add/Update output table facts ---
                for output_table in step.outputs:
                    if is_ds_input(output_table):
                        # Datasources are not tables and should not produce TableFact.
                        continue

                    if output_table in current_table_facts:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_OUTPUT_TABLE_COLLISION",
                            message=f"Output table '{output_table}' produced by operation '{step.op}' already exists.",
                            loc=step.loc,
                        )
                    current_table_facts[output_table] = TableFact(sorted_by=output_sorted_by)

        # Enforce strict expression typing
        try:
            infer_table_schema_types(self)
        except TypeInferenceError as err:
            raise UnknownBlockStep(
                code=getattr(err, "code", "E_TYPE"),
                message=err.message,
                loc=err.loc or Loc("<string>", 1, 1),
            )

        return current_table_facts
