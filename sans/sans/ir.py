from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, List, Dict

from ._loc import Loc

@dataclass
class TableFact:
    """Stores metadata about a table."""
    sorted_by: Optional[List[str]] = None # List of column names by which the table is sorted

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
class UnknownBlockStep(Step, Exception): # Inherit from both Step and Exception
    kind: str = field(default="block", init=False)
    code: str
    message: str
    severity: str = field(default="fatal") # Currently always fatal for blocks

@dataclass(frozen=True)
class IRDoc:
    steps: list[Step] = field(default_factory=list)
    tables: set[str] = field(default_factory=set) # Pre-declared tables
    table_facts: Dict[str, TableFact] = field(default_factory=dict)

    def __post_init__(self):
        # Initialize table_facts for any pre-declared tables
        for table_name in self.tables:
            if table_name not in self.table_facts:
                # Need to bypass frozen=True for modification in __post_init__
                object.__setattr__(self, 'table_facts', {**self.table_facts, table_name: TableFact()})


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
                # If an unknown block step is already in the IR, it's a fatal error.
                raise step

            if isinstance(step, OpStep):
                # --- Input Table Validation ---
                for input_table in step.inputs:
                    if input_table not in current_table_facts:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_TABLE_UNDEFINED",
                            message=f"Input table '{input_table}' used by operation '{step.op}' is not defined.",
                            loc=step.loc,
                        )
                
                # --- Output Table Validation ---
                if not step.outputs:
                    # All OpSteps are expected to produce an output table.
                    # This implies an internal error in the compiler if outputs list is empty.
                    raise UnknownBlockStep(
                        code="SANS_INTERNAL_COMPILER_ERROR",
                        message=f"Operation '{step.op}' does not define an output table.",
                        loc=step.loc,
                    )

                # Determine sortedness for output tables based on the operation
                # For now, we assume single input/output for simplicity in data steps and sort
                input_sorted_by: Optional[List[str]] = None
                if step.inputs and step.inputs[0] in current_table_facts:
                    input_sorted_by = current_table_facts[step.inputs[0]].sorted_by
                
                output_sorted_by: Optional[List[str]] = None # Default to unsorted

                if step.op == "sort":
                    # proc sort sets the sorted_by property
                    by_vars = step.params.get("by")
                    if not by_vars:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_SORT_MISSING_BY",
                            message="PROC SORT operation requires 'by' variables.",
                            loc=step.loc,
                        )
                    if isinstance(by_vars, list) and by_vars and isinstance(by_vars[0], dict):
                        output_sorted_by = [v.get("col") for v in by_vars]
                    else:
                        output_sorted_by = list(by_vars)
                
                elif step.op == "data_step":
                    by_vars = step.params.get("by") or []
                    keep = step.params.get("keep") or []
                    if by_vars:
                        for input_table in step.inputs:
                            input_fact = current_table_facts.get(input_table)
                            input_sorted_by = input_fact.sorted_by if input_fact else None
                            if not input_sorted_by or input_sorted_by[:len(by_vars)] != by_vars:
                                raise UnknownBlockStep(
                                    code="SANS_VALIDATE_ORDER_REQUIRED",
                                    message=f"Input table '{input_table}' must be sorted by {by_vars} for BY-group processing.",
                                    loc=step.loc,
                                )
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
                        input_fact = current_table_facts.get(input_table)
                        input_sorted_by = input_fact.sorted_by if input_fact else None
                        if not input_sorted_by or input_sorted_by[:len(by_vars)] != by_vars:
                            raise UnknownBlockStep(
                                code="SANS_VALIDATE_ORDER_REQUIRED",
                                message=f"Input table '{input_table}' must be sorted by {by_vars} for PROC TRANSPOSE.",
                                loc=step.loc,
                            )
                    output_sorted_by = list(by_vars)

                elif step.op == "sql_select":
                    group_by = step.params.get("group_by") or []
                    if group_by:
                        output_sorted_by = list(group_by)
                    else:
                        output_sorted_by = None

                elif step.op == "summary":
                    class_vars = step.params.get("class") or []
                    output_sorted_by = list(class_vars) if class_vars else None

                elif step.op == "format":
                    output_sorted_by = None

                elif step.op == "select":
                    # select preserves sortedness if sort keys are not dropped
                    if input_sorted_by is None:
                        output_sorted_by = None
                    else:
                        keep = step.params.get("keep") or []
                        drop = step.params.get("drop") or []
                        if keep:
                            output_sorted_by = input_sorted_by if all(k in keep for k in input_sorted_by) else None
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
                    # TODO: Implement sophisticated mapping later if required.
                    output_sorted_by = None # Renaming can change sort key names, so drop for now
                
                elif step.op == "identity":
                    # identity preserves sortedness
                    output_sorted_by = input_sorted_by

                # Add/Update output table facts
                for output_table in step.outputs:
                    if output_table in current_table_facts:
                        raise UnknownBlockStep(
                            code="SANS_VALIDATE_OUTPUT_TABLE_COLLISION",
                            message=f"Output table '{output_table}' produced by operation '{step.op}' already exists.",
                            loc=step.loc,
                        )
                    current_table_facts[output_table] = TableFact(sorted_by=output_sorted_by)
        
        return current_table_facts
