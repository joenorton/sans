from __future__ import annotations

from typing import List, Set, Tuple, Any, Dict, Optional

from sans.ir import OpStep
from sans._loc import Loc

from .ast import (
    AssertStmt,
    BuilderExpr,
    ConstDecl,
    FromExpr,
    LetBinding,
    PipelineExpr,
    PostfixExpr,
    SansScript,
    SansScriptStmt,
    SaveStmt,
    SourceSpan,
    TableBinding,
    TableExpr,
    TableNameExpr,
    TableTransform,
    DatasourceDeclaration,
)


def _substitute_const_in_expr(expr: Any, const_bindings: Dict[str, Any]) -> Any:
    """Replace col nodes that reference a const name with lit nodes (compile-time substitution)."""
    if not isinstance(expr, dict):
        return expr
    node_type = expr.get("type")
    if node_type == "col":
        name = (expr.get("name") or "").lower()
        if name in const_bindings:
            return {"type": "lit", "value": const_bindings[name]}
        return expr
    if node_type == "lit":
        return expr
    if node_type in ("binop", "boolop"):
        out = dict(expr)
        if "left" in out:
            out["left"] = _substitute_const_in_expr(out["left"], const_bindings)
        if "right" in out:
            out["right"] = _substitute_const_in_expr(out["right"], const_bindings)
        if "args" in out:
            out["args"] = [_substitute_const_in_expr(a, const_bindings) for a in out["args"]]
        return out
    if node_type == "unop":
        out = dict(expr)
        if "arg" in out:
            out["arg"] = _substitute_const_in_expr(out["arg"], const_bindings)
        return out
    if node_type == "call":
        out = dict(expr)
        if "args" in out:
            out["args"] = [_substitute_const_in_expr(a, const_bindings) for a in out["args"]]
        return out
    return expr


class Lowerer:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.steps: List[OpStep] = []
        self.referenced: Set[str] = set()
        self.produced: Set[str] = set()
        self.temp_count = 0
        self.const_bindings: Dict[str, Any] = {}

    def next_temp(self) -> str:
        self.temp_count += 1
        return f"__t{self.temp_count}__"

    def lower(self, script: SansScript) -> Tuple[List[OpStep], Set[str]]:
        # Pre-pass: collect all const bindings for compile-time substitution into expressions
        for stmt in script.statements:
            if isinstance(stmt, ConstDecl):
                for k, v in stmt.bindings.items():
                    self.const_bindings[k.lower()] = v
        for stmt in script.statements:
            if isinstance(stmt, LetBinding):
                self._lower_let(stmt)
            elif isinstance(stmt, DatasourceDeclaration):
                self._lower_datasource(stmt)
            elif isinstance(stmt, TableBinding):
                self._lower_table_binding(stmt)
            elif isinstance(stmt, SaveStmt):
                self._lower_save(stmt)
            elif isinstance(stmt, AssertStmt):
                self._lower_assert(stmt)
            elif isinstance(stmt, ConstDecl):
                self._lower_const(stmt)
        
        if script.terminal_expr:
            # No implicit output: terminal expression lowers to a temp, not __result__.
            self._lower_table_expr(script.terminal_expr, output=None)
        
        # Filter out datasource pseudo-inputs from references
        real_references = {r for r in self.referenced if not r.startswith("__datasource__")}
        
        return self.steps, real_references

    def _lower_save(self, stmt: SaveStmt):
        self.steps.append(OpStep(
            op="save",
            inputs=[stmt.table],
            outputs=[],  # side effect: write artifact
            params={"path": stmt.path, "name": stmt.name},
            loc=self._loc(stmt.span),
        ))

    def _lower_assert(self, stmt: AssertStmt):
        self.steps.append(OpStep(
            op="assert",
            inputs=[],
            outputs=[],  # evidence only
            params={"predicate": stmt.predicate},
            loc=self._loc(stmt.span),
        ))

    def _lower_datasource(self, stmt: DatasourceDeclaration):
        params = {
            "name": stmt.name,
            "path": stmt.path,
            "columns": stmt.columns,
            "kind": stmt.kind,
        }
        if stmt.kind == "inline_csv":
            params["inline_text"] = stmt.inline_text
            params["inline_sha256"] = stmt.inline_sha256
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
        # Single scalar binding → IR op let_scalar (immutable, no side effect; substitution in expressions).
        self.steps.append(OpStep(
            op="let_scalar",
            inputs=[],
            outputs=[],
            params={"name": stmt.name, "expr": stmt.expr},
            loc=self._loc(stmt.span),
        ))

    def _lower_const(self, stmt: ConstDecl):
        # const { ... } → one IR op "const" with bindings (deterministic; one op for round-trip clarity).
        self.steps.append(OpStep(
            op="const",
            inputs=[],
            outputs=[],
            params={"bindings": dict(stmt.bindings)},
            loc=self._loc(stmt.span),
        ))

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
                curr_input = self._lower_transform(step, curr_input, step_output)
            return curr_input

        if isinstance(expr, PostfixExpr):
            curr_input = self._lower_table_expr(expr.source)
            return self._lower_transform(expr.transform, curr_input, final_output)

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
            elif expr.kind in ("summary", "aggregate"):
                # summary() is legacy input sugar; always lower to canonical op "aggregate".
                self.steps.append(OpStep(
                    op="aggregate",
                    inputs=[curr_input],
                    outputs=[final_output],
                    params={
                        "class": expr.config.get("class", []),
                        "vars": expr.config.get("var", []),
                        "stats": expr.config.get("stats", ["mean"]),
                        "naming": "{var}_{stat}",
                        "autoname": True,
                    },
                    loc=self._loc(expr.span),
                ))
            return final_output

        return final_output

    def _lower_transform(self, transform: TableTransform, input_table: str, output_table: str) -> str:
        if transform.kind == "select":
            self.steps.append(OpStep(
                op="select",
                inputs=[input_table],
                outputs=[output_table],
                params={"keep": transform.params.get("keep", []), "drop": transform.params.get("drop", [])},
                loc=self._loc(transform.span)
            ))
            return output_table
        elif transform.kind == "drop":
            self.steps.append(OpStep(
                op="select",
                inputs=[input_table],
                outputs=[output_table],
                params={"drop": transform.params.get("drop", [])},
                loc=self._loc(transform.span)
            ))
            return output_table
        elif transform.kind == "filter":
            self.steps.append(OpStep(
                op="filter",
                inputs=[input_table],
                outputs=[output_table],
                params={"predicate": transform.params["predicate"]},
                loc=self._loc(transform.span)
            ))
            return output_table
        elif transform.kind in ("derive", "update!"):
            # Lower to IR op compute with mode "derive" | "update" (one step per consecutive same-mode group).
            assignments = transform.params["assignments"]
            idx = 0
            while idx < len(assignments):
                allow_overwrite = assignments[idx].get("allow_overwrite", False)
                mode = "update" if allow_overwrite else "derive"
                group = []
                while idx < len(assignments) and assignments[idx].get("allow_overwrite", False) == allow_overwrite:
                    group.append(assignments[idx])
                    idx += 1
                step_output = output_table if idx >= len(assignments) else self.next_temp()
                params = {
                    "mode": mode,
                    "assignments": [
                        {"target": a["target"], "expr": _substitute_const_in_expr(a["expr"], self.const_bindings)}
                        for a in group
                    ],
                }
                self.steps.append(OpStep(
                    op="compute",
                    inputs=[input_table],
                    outputs=[step_output],
                    params=params,
                    loc=self._loc(transform.span),
                ))
                input_table = step_output
            return input_table
        elif transform.kind == "rename":
            self.steps.append(OpStep(
                op="rename",
                inputs=[input_table],
                outputs=[output_table],
                params={"mappings": transform.params["mappings"]},
                loc=self._loc(transform.span)
            ))
            return output_table
        return output_table

    def _loc(self, span: SourceSpan) -> Loc:
        return Loc(file=self.file_name, line_start=span.start, line_end=span.end)


def lower_script(script: SansScript, file_name: str) -> Tuple[List[OpStep], Set[str]]:
    lowerer = Lowerer(file_name)
    return lowerer.lower(script)