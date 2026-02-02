from __future__ import annotations

from typing import List, Set, Tuple, Any, Dict, Optional

from sans.ir import OpStep
from sans._loc import Loc

from .ast import (
    BuilderExpr,
    FromExpr,
    LetBinding,
    MapExpr,
    PipelineExpr,
    PostfixExpr,
    SansScript,
    SansScriptStmt,
    SourceSpan,
    TableBinding,
    TableExpr,
    TableNameExpr,
    TableTransform,
    DatasourceDeclaration,
)


class Lowerer:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.steps: List[OpStep] = []
        self.referenced: Set[str] = set()
        self.produced: Set[str] = set()
        self.temp_count = 0

    def next_temp(self) -> str:
        self.temp_count += 1
        return f"__t{self.temp_count}__"

    def lower(self, script: SansScript) -> Tuple[List[OpStep], Set[str]]:
        for stmt in script.statements:
            if isinstance(stmt, LetBinding):
                self._lower_let(stmt)
            elif isinstance(stmt, DatasourceDeclaration): # New: lower datasource
                self._lower_datasource(stmt)
            elif isinstance(stmt, TableBinding):
                self._lower_table_binding(stmt)
        
        if script.terminal_expr:
            self._lower_table_expr(script.terminal_expr, output="__result__")
        
        # Filter out datasource pseudo-inputs from references
        real_references = {r for r in self.referenced if not r.startswith("__datasource__")}
        
        return self.steps, real_references

    def _lower_datasource(self, stmt: DatasourceDeclaration):
        params = {
            "name": stmt.name,
            "path": stmt.path,
            "columns": stmt.columns,
            "kind": "csv",
        }
        self.steps.append(OpStep(
            op="datasource",
            inputs=[],
            outputs=[f"__datasource__{stmt.name}"], # Datasource output is internal, not a table
            params=params,
            loc=self._loc(stmt.span),
        ))
        # Datasources are implicitly available, but we track them as "produced" internally
        self.produced.add(f"__datasource__{stmt.name}")


    def _lower_let(self, stmt: LetBinding):
        if isinstance(stmt.expr, MapExpr):
            self.steps.append(self._lower_map(stmt.name, stmt.expr))
        else:
            # Scalar let binding - currently we don't have a specific IR op for this 
            # unless it's a map. In sans, maps are the main complex scalar.
            # Other scalars are usually used inside expressions.
            # If needed, we could emit a 'compute' or similar, but for now let's skip
            # as they are just bindings for the compiler's expression evaluator.
            pass

    def _lower_map(self, name: str, expr: MapExpr) -> OpStep:
        mapping: Dict[str, Any] = {}
        default: Any = None
        for entry in expr.entries:
            val = entry.value
            # If it's a literal, extract the value for the IR
            if isinstance(val, dict) and val.get("type") == "lit":
                val = val.get("value")
            
            if entry.key is None:
                default = val
            else:
                mapping[entry.key] = val
        
        params = {"name": name, "map": mapping, "other": default}
        return OpStep(
            op="format",
            inputs=[],
            outputs=[f"__format__{name}"],
            params=params,
            loc=self._loc(expr.span),
        )

    def _lower_table_binding(self, stmt: TableBinding):
        self._lower_table_expr(stmt.expr, output=stmt.name)
        self.produced.add(stmt.name)

    def _lower_table_expr(self, expr: TableExpr, output: Optional[str] = None) -> str:
        final_output = output if output else self.next_temp()
        
        if isinstance(expr, FromExpr):
            # References the datasource, not a table produced by a step
            # The datasource itself is implicitly available, but its data needs to be "loaded"
            # So, the input to the next step will be the datasource's name.
            # We don't produce an 'identity' OpStep here if it's just 'from(ds)'
            self.referenced.add(f"__datasource__{expr.source}")
            return f"__datasource__{expr.source}"
        
        if isinstance(expr, TableNameExpr):
            # If a TableNameExpr refers to a datasource directly (e.g., 'table_name select ...')
            # without an explicit 'from(table_name)', we treat it as implicitly loading the datasource.
            if f"__datasource__{expr.name}" in self.produced or expr.name in self.referenced: # Check if it's a datasource or already referenced
                 # This should eventually be a compiler error if the table is not explicitly defined
                 # or referenced as datasource.
                 # For now, if it starts with '__datasource__', it refers to the datasource.
                 # If it's a TableNameExpr, it should refer to an already produced table,
                 # or be resolved to a datasource.
                 pass

            if expr.name not in self.produced: # If not a produced table, assume it's a datasource
                # This needs to be validated by the semantic validator.
                # If it's a datasource, reference it here.
                self.referenced.add(f"__datasource__{expr.name}")
                if output:
                    self.steps.append(OpStep(
                        op="identity",
                        inputs=[f"__datasource__{expr.name}"],
                        outputs=[final_output],
                        params={"source_name": expr.name},
                        loc=self._loc(expr.span)
                    ))
                return final_output
            # Normal table reference
            if output:
                self.steps.append(OpStep(
                    op="identity",
                    inputs=[expr.name],
                    outputs=[final_output],
                    loc=self._loc(expr.span)
                ))
            return expr.name

        if isinstance(expr, PipelineExpr):
            curr_input = self._lower_table_expr(expr.source)
            for i, step in enumerate(expr.steps):
                is_last = (i == len(expr.steps) - 1)
                step_output = final_output if (is_last and output) else self.next_temp()
                self._lower_transform(step, curr_input, step_output)
                curr_input = step_output
            return curr_input

        if isinstance(expr, PostfixExpr):
            curr_input = self._lower_table_expr(expr.source)
            self._lower_transform(expr.transform, curr_input, final_output)
            return final_output

        if isinstance(expr, BuilderExpr):
            curr_input = self._lower_table_expr(expr.source)
            if expr.kind == "sort":
                self.steps.append(OpStep(
                    op="sort",
                    inputs=[curr_input],
                    outputs=[final_output],
                    params={"by": expr.config.get("by", []), "nodupkey": expr.config.get("nodupkey", False)},
                    loc=self._loc(expr.span)
                ))
            elif expr.kind == "summary":
                 self.steps.append(OpStep(
                    op="summary",
                    inputs=[curr_input],
                    outputs=[final_output],
                    params={
                        "class": expr.config.get("class", []),
                        "vars": expr.config.get("var", []),
                        "stats": expr.config.get("stats", ["mean"]),
                        "naming": "{var}_{stat}",
                        "autoname": True,
                    },
                    loc=self._loc(expr.span)
                ))
            return final_output

        return final_output

    def _lower_transform(self, transform: TableTransform, input_table: str, output_table: str):
        if transform.kind == "select":
            self.steps.append(OpStep(
                op="select",
                inputs=[input_table],
                outputs=[output_table],
                params={"keep": transform.params.get("keep", []), "drop": transform.params.get("drop", [])},
                loc=self._loc(transform.span)
            ))
        elif transform.kind == "drop":
             self.steps.append(OpStep(
                op="select",
                inputs=[input_table],
                outputs=[output_table],
                params={"drop": transform.params.get("drop", [])},
                loc=self._loc(transform.span)
            ))
        elif transform.kind == "filter":
            self.steps.append(OpStep(
                op="filter",
                inputs=[input_table],
                outputs=[output_table],
                params={"predicate": transform.params["predicate"]},
                loc=self._loc(transform.span)
            ))
        elif transform.kind == "derive":
            # Map derive to a data_step for now as it handles multiple assignments well
            params = {
                "mode": "set",
                "inputs": [{"table": input_table}],
                "statements": transform.params["assignments"],
                "keep": [],
                "drop": [],
                "explicit_output": False,
            }
            self.steps.append(OpStep(
                op="data_step",
                inputs=[input_table],
                outputs=[output_table],
                params=params,
                loc=self._loc(transform.span)
            ))
        elif transform.kind == "rename":
            self.steps.append(OpStep(
                op="rename",
                inputs=[input_table],
                outputs=[output_table],
                params={"mappings": transform.params["mappings"]},
                loc=self._loc(transform.span)
            ))

    def _loc(self, span: SourceSpan) -> Loc:
        return Loc(file=self.file_name, line_start=span.start, line_end=span.end)


def lower_script(script: SansScript, file_name: str) -> Tuple[List[OpStep], Set[str]]:
    lowerer = Lowerer(file_name)
    return lowerer.lower(script)