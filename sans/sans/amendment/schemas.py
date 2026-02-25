from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union, Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator

HARD_CAP = 50
EDIT_EXPR_ALLOWLIST = frozenset(
    {"replace_literal", "replace_column_ref", "replace_op", "wrap_with_not"}
)
REPLACE_OP_ALLOWLIST = frozenset(
    {"==", "!=", "<", ">", "<=", ">=", "+", "-", "*", "/", "and", "or", "not"}
)


def _validate_pointer(path: str) -> str:
    if path == "":
        raise ValueError("path must be non-empty when present")
    if not path.startswith("/"):
        raise ValueError("path must start with '/'")
    if path == "/":
        return path
    segments = path[1:].split("/")
    for segment in segments:
        i = 0
        while i < len(segment):
            if segment[i] == "~":
                if i + 1 >= len(segment) or segment[i + 1] not in {"0", "1"}:
                    raise ValueError("path contains invalid RFC6901 escape")
                i += 2
                continue
            i += 1
    return path


class Meta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: Optional[str] = None
    note: Optional[str] = None


class AmendmentPolicyV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    allow_destructive: bool = False
    allow_output_rewire: bool = False
    allow_approx: bool = False
    max_ops: int = 50


class SelectorV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    step_id: Optional[str] = None
    transform_id: Optional[str] = None
    table: Optional[str] = None
    assertion_id: Optional[str] = None
    path: Optional[str] = None

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        return _validate_pointer(value)


class TableSelectorV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str


class AssertionSelectorV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    assertion_id: str


class AddStepSelectorV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    before_step_id: Optional[str] = None
    after_step_id: Optional[str] = None
    index: Optional[int] = None

    @model_validator(mode="after")
    def validate_anchor_xor(self) -> "AddStepSelectorV1":
        present = sum(
            value is not None
            for value in (self.before_step_id, self.after_step_id, self.index)
        )
        if present != 1:
            raise ValueError(
                "add_step.selector must include exactly one of before_step_id, after_step_id, index"
            )
        return self


class StepPayloadV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    kind: Optional[Literal["op"]] = None
    op: str
    inputs: List[str]
    outputs: List[str]
    params: Dict[str, Any]
    soundness: Literal["sound", "approx"] = "sound"

    @model_validator(mode="after")
    def validate_step_params(self) -> "StepPayloadV1":
        validate_step_params_shape(self.op, self.params)
        return self


class AssertionSpecV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    assertion_id: Optional[str] = None
    type: str
    severity: Optional[str] = None
    table: Optional[str] = None
    column: Optional[str] = None
    transform_id: Optional[str] = None
    params: Dict[str, Any] = Field(default_factory=dict)


class AddStepParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    step: StepPayloadV1


class EmptyParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ReplaceStepParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    op: str
    params: Dict[str, Any]
    preserve_wiring: bool = True

    @model_validator(mode="after")
    def validate_step_params(self) -> "ReplaceStepParamsV1":
        validate_step_params_shape(self.op, self.params)
        return self


class RewireInputsParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    inputs: List[str]


class RewireOutputsParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    outputs: List[str]


class RenameTableParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    new_name: str


class SetParamsParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: Any


class ReplaceExprParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expr: Dict[str, Any]


class EditExprParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    edit: str
    literal: Optional[Any] = None
    column_ref: Optional[str] = None
    op: Optional[str] = None

    @model_validator(mode="after")
    def validate_edit(self) -> "EditExprParamsV1":
        if self.edit not in EDIT_EXPR_ALLOWLIST:
            raise ValueError("edit_expr.edit is unsupported")
        if self.edit == "replace_literal":
            return self
        if self.edit == "replace_column_ref":
            if not self.column_ref:
                raise ValueError("replace_column_ref requires column_ref")
            return self
        if self.edit == "replace_op":
            if self.op not in REPLACE_OP_ALLOWLIST:
                raise ValueError("replace_op requires supported operator")
            return self
        if self.edit == "wrap_with_not":
            return self
        return self


class AddAssertionParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    assertion: AssertionSpecV1


class ReplaceAssertionParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    assertion: AssertionSpecV1


class BaseOpV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    op_id: str


class AddStepOpV1(BaseOpV1):
    kind: Literal["add_step"]
    selector: AddStepSelectorV1
    params: AddStepParamsV1


class RemoveStepOpV1(BaseOpV1):
    kind: Literal["remove_step"]
    selector: SelectorV1
    params: EmptyParamsV1

    @model_validator(mode="after")
    def validate_selector(self) -> "RemoveStepOpV1":
        if not any([self.selector.step_id, self.selector.transform_id, self.selector.table]):
            raise ValueError("remove_step requires step_id or transform_id or table")
        return self


class ReplaceStepOpV1(BaseOpV1):
    kind: Literal["replace_step"]
    selector: SelectorV1
    params: ReplaceStepParamsV1

    @model_validator(mode="after")
    def validate_selector(self) -> "ReplaceStepOpV1":
        if not any([self.selector.step_id, self.selector.transform_id, self.selector.table]):
            raise ValueError("replace_step requires step selector")
        return self


class RewireInputsOpV1(BaseOpV1):
    kind: Literal["rewire_inputs"]
    selector: SelectorV1
    params: RewireInputsParamsV1

    @model_validator(mode="after")
    def validate_selector(self) -> "RewireInputsOpV1":
        if not any([self.selector.step_id, self.selector.transform_id, self.selector.table]):
            raise ValueError("rewire_inputs requires step selector")
        return self


class RewireOutputsOpV1(BaseOpV1):
    kind: Literal["rewire_outputs"]
    selector: SelectorV1
    params: RewireOutputsParamsV1

    @model_validator(mode="after")
    def validate_selector(self) -> "RewireOutputsOpV1":
        if not any([self.selector.step_id, self.selector.transform_id, self.selector.table]):
            raise ValueError("rewire_outputs requires step selector")
        return self


class RenameTableOpV1(BaseOpV1):
    kind: Literal["rename_table"]
    selector: TableSelectorV1
    params: RenameTableParamsV1


class SetParamsOpV1(BaseOpV1):
    kind: Literal["set_params"]
    selector: SelectorV1
    params: SetParamsParamsV1

    @model_validator(mode="after")
    def validate_selector(self) -> "SetParamsOpV1":
        if not any([self.selector.step_id, self.selector.transform_id, self.selector.table]):
            raise ValueError("set_params requires step selector")
        if self.selector.path is None:
            raise ValueError("set_params requires selector.path")
        return self


class ReplaceExprOpV1(BaseOpV1):
    kind: Literal["replace_expr"]
    selector: SelectorV1
    params: ReplaceExprParamsV1

    @model_validator(mode="after")
    def validate_selector(self) -> "ReplaceExprOpV1":
        if not any([self.selector.step_id, self.selector.transform_id, self.selector.table]):
            raise ValueError("replace_expr requires step selector")
        if self.selector.path is None:
            raise ValueError("replace_expr requires selector.path")
        return self


class EditExprOpV1(BaseOpV1):
    kind: Literal["edit_expr"]
    selector: SelectorV1
    params: EditExprParamsV1

    @model_validator(mode="after")
    def validate_selector(self) -> "EditExprOpV1":
        if not any([self.selector.step_id, self.selector.transform_id, self.selector.table]):
            raise ValueError("edit_expr requires step selector")
        if self.selector.path is None:
            raise ValueError("edit_expr requires selector.path")
        return self


class AddAssertionOpV1(BaseOpV1):
    kind: Literal["add_assertion"]
    selector: TableSelectorV1
    params: AddAssertionParamsV1


class RemoveAssertionOpV1(BaseOpV1):
    kind: Literal["remove_assertion"]
    selector: AssertionSelectorV1
    params: EmptyParamsV1


class ReplaceAssertionOpV1(BaseOpV1):
    kind: Literal["replace_assertion"]
    selector: AssertionSelectorV1
    params: ReplaceAssertionParamsV1


AmendOpV1 = Annotated[
    Union[
        AddStepOpV1,
        RemoveStepOpV1,
        ReplaceStepOpV1,
        RewireInputsOpV1,
        RewireOutputsOpV1,
        RenameTableOpV1,
        SetParamsOpV1,
        ReplaceExprOpV1,
        EditExprOpV1,
        AddAssertionOpV1,
        RemoveAssertionOpV1,
        ReplaceAssertionOpV1,
    ],
    Field(discriminator="kind"),
]


class AmendmentRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["sans.amendment_request"]
    version: Literal[1]
    contract_version: Literal["0.1"]
    meta: Optional[Meta] = None
    policy: AmendmentPolicyV1
    ops: List[AmendOpV1]

    @model_validator(mode="after")
    def validate_request(self) -> "AmendmentRequestV1":
        cap = min(self.policy.max_ops, HARD_CAP)
        if len(self.ops) > cap:
            raise ValueError(f"ops exceeds cap: {len(self.ops)} > {cap}")
        op_ids = [op.op_id for op in self.ops]
        if len(set(op_ids)) != len(op_ids):
            raise ValueError("duplicate op_id found in ops")
        return self


class DatasourceParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    kind: str
    path: Optional[str] = None
    columns: Optional[Dict[str, str]] = None
    inline_text: Optional[str] = None
    inline_sha256: Optional[str] = None


class SaveParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str


class IdentityParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FilterParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    predicate: Dict[str, Any]


class ComputeAssignmentV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target: str
    expr: Dict[str, Any]


class ComputeParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    assignments: List[ComputeAssignmentV1]


class SelectParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cols: Optional[List[str]] = None
    drop: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_cols_or_drop(self) -> "SelectParamsV1":
        has_cols = bool(self.cols)
        has_drop = bool(self.drop)
        if has_cols == has_drop:
            raise ValueError("select params require exactly one of cols or drop")
        return self


class RenameMappingEntryV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_: str = Field(alias="from")
    to: str


class RenameParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mapping: List[RenameMappingEntryV1]


class SortByEntryV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    col: str
    desc: bool


class SortParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    by: List[SortByEntryV1]


class CastEntryV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    col: str
    to: str
    on_error: Optional[Literal["fail", "null"]] = None
    trim: Optional[bool] = None


class CastParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    casts: List[CastEntryV1]


class AggregateMetricV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    op: str
    col: str


class AggregateParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    group_by: List[str] = Field(default_factory=list)
    metrics: List[AggregateMetricV1]


class DropParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cols: List[str]


class DataStepParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    by: Optional[List[str]] = None
    keep: Optional[List[str]] = None
    drop: Optional[List[str]] = None
    statements: Optional[List[Dict[str, Any]]] = None


class TransposeParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    by: List[str]
    id: str
    var: str


class SqlSelectParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    select: Optional[List[Dict[str, Any]]] = None
    where: Optional[Dict[str, Any]] = None
    group_by: Optional[List[str]] = None
    having: Optional[Dict[str, Any]] = None
    order_by: Optional[List[Dict[str, Any]]] = None
    joins: Optional[List[Dict[str, Any]]] = None


class FormatParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mapping: List[Dict[str, Any]]


class AssertParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    predicate: Dict[str, Any]
    severity: Optional[str] = None


class LetScalarParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    expr: Dict[str, Any]


class ConstParamsV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: Any


STEP_OP_PARAM_SCHEMAS = {
    "datasource": DatasourceParamsV1,
    "save": SaveParamsV1,
    "identity": IdentityParamsV1,
    "filter": FilterParamsV1,
    "compute": ComputeParamsV1,
    "select": SelectParamsV1,
    "rename": RenameParamsV1,
    "sort": SortParamsV1,
    "cast": CastParamsV1,
    "aggregate": AggregateParamsV1,
    "drop": DropParamsV1,
    "data_step": DataStepParamsV1,
    "transpose": TransposeParamsV1,
    "sql_select": SqlSelectParamsV1,
    "format": FormatParamsV1,
    "assert": AssertParamsV1,
    "let_scalar": LetScalarParamsV1,
    "const": ConstParamsV1,
}


def validate_step_params_shape(op: str, params: Dict[str, Any]) -> None:
    if op not in STEP_OP_PARAM_SCHEMAS:
        raise ValueError(f"unsupported step.op for amendment validation: {op}")
    if not isinstance(params, dict):
        raise ValueError("step params must be an object")
    STEP_OP_PARAM_SCHEMAS[op].model_validate(params)

