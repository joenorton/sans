from __future__ import annotations

from typing import List, Set, Tuple, Any, Dict, Optional

from sans.ir import OpStep, ds_input
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


def _to_str_list(raw: Any) -> List[str]:
    """Normalize to list[str] for cols/drop. Accept list or comma-separated string."""
    if raw is None:
        return []
    if isinstance(raw, str):
        return [s.strip() for s in raw.split(",") if s.strip()]
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return []


def _lower_select_params(keep: Any, drop: Any) -> Dict[str, Any]:
    """Frontend-only: convert keep/drop to canonical cols or drop (list[str])."""
    keep_list = _to_str_list(keep)
    drop_list = _to_str_list(drop)
    if keep_list:
        return {"cols": keep_list}
    if drop_list:
        return {"drop": drop_list}
    return {"cols": []}  # empty select; assert_canon may still refuse if required non-empty


def _lower_sort_by(by: Any) -> List[Dict[str, Any]]:
    """Frontend-only: convert by (list[str] or list[{col, asc?}]) to canonical list[{col, desc}]."""
    if not by:
        return []
    if not isinstance(by, list):
        return []
    result: List[Dict[str, Any]] = []
    for item in by:
        if isinstance(item, str):
            col = item.strip()
            if col:
                result.append({"col": col, "desc": False})
        elif isinstance(item, dict):
            col = item.get("col")
            if col is not None:
                col = str(col).strip()
                if col:
                    desc = item.get("desc", not item.get("asc", True))
                    result.append({"col": col, "desc": bool(desc)})
    return result


def _lower_aggregate_params(
    class_vars: Any,
    var_vars: Any,
    stats: Any,
    naming: Any,
    autoname: Any,
) -> Dict[str, Any]:
    """Frontend-only: convert class/var/stats to canonical group_by and metrics."""
    from sans.ir import AGGREGATE_ALLOWED_OPS
    group_list = _to_str_list(class_vars)
    var_list = _to_str_list(var_vars)
    stat_list = _to_str_list(stats) if stats else ["mean"]
    if not stat_list:
        stat_list = ["mean"]
    for op in stat_list:
        if op not in AGGREGATE_ALLOWED_OPS:
            stat_list = ["mean"]
            break
    metrics: List[Dict[str, Any]] = []
    for col in var_list:
        for op in stat_list:
            metrics.append({"name": f"{col}_{op}", "op": op, "col": col})
    return {"group_by": group_list, "metrics": metrics}


def _lower_compute_params(assign: Any, mode: str = "derive") -> Dict[str, Any]:
    """Frontend-only: convert assign (list[{col, expr}]) to canonical assignments (list[{target, expr}])."""
    if not assign:
        return {"mode": mode, "assignments": []}
    if not isinstance(assign, list):
        return {"mode": mode, "assignments": []}
    assignments = []
    for a in assign:
        if isinstance(a, dict):
            target = a.get("target") or a.get("col")
            expr = a.get("expr")
            if target is not None:
                assignments.append({"target": str(target), "expr": expr})
    return {"mode": mode, "assignments": assignments}


def _lower_rename_params(mappings: Any) -> Dict[str, Any]:
    """Frontend-only: convert mappings (dict or list) to canonical mapping list[{from, to}]."""
    if mappings is None:
        return {"mapping": []}
    if isinstance(mappings, dict):
        return {"mapping": [{"from": k, "to": v} for k, v in sorted(mappings.items())]}
    if isinstance(mappings, list):
        result = []
        for item in mappings:
            if isinstance(item, dict):
                fr = item.get("from") or item.get("old")
                to = item.get("to") or item.get("new")
                if fr is not None and to is not None:
                    result.append({"from": str(fr).strip(), "to": str(to).strip()})
            else:
                result.append({"from": "", "to": ""})  # will fail later if invalid
        return {"mapping": result}
    return {"mapping": []}


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
        self.datasources: Set[str] = set()
        self.temp_count = 0
        self.const_bindings: Dict[str, Any] = {}

    def next_temp(self) -> str:
        self.temp_count += 1
        return f"__t{self.temp_count}__"

    def lower(self, script: SansScript) -> Tuple[List[OpStep], Set[str]]:
        self.datasources = set(script.datasources.keys())
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
            kind = expr.source_kind
            if kind is None:
                if expr.source in self.produced:
                    kind = "table"
                elif expr.source in self.datasources:
                    kind = "datasource"
                else:
                    kind = "table"
            input_name = ds_input(expr.source) if kind == "datasource" else expr.source
            output_name = final_output if output else self.next_temp()
            self.steps.append(OpStep(
                op="identity",
                inputs=[input_name],
                outputs=[output_name],
                params={"source": {"kind": kind, "name": expr.source}},
                loc=self._loc(expr.span),
            ))
            return output_name
        
        if isinstance(expr, TableNameExpr):
            if output:
                self.steps.append(OpStep(
                    op="identity",
                    inputs=[expr.name],
                    outputs=[final_output],
                    loc=self._loc(expr.span)
                ))
                return final_output
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
                by_canon = _lower_sort_by(expr.config.get("by", []))
                self.steps.append(OpStep(
                    op="sort",
                    inputs=[curr_input],
                    outputs=[final_output],
                    params={"by": by_canon, "nodupkey": expr.config.get("nodupkey", False)},
                    loc=self._loc(expr.span)
                ))
            elif expr.kind in ("summary", "aggregate"):
                # summary() is legacy input sugar; always lower to canonical op "aggregate".
                cfg = expr.config
                params = _lower_aggregate_params(
                    cfg.get("class", []),
                    cfg.get("var", []) or cfg.get("vars", []),
                    cfg.get("stats", ["mean"]),
                    cfg.get("naming", "{var}_{stat}"),
                    cfg.get("autoname", True),
                )
                self.steps.append(OpStep(
                    op="aggregate",
                    inputs=[curr_input],
                    outputs=[final_output],
                    params=params,
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
                params=_lower_select_params(
                    transform.params.get("keep"),
                    transform.params.get("drop"),
                ),
                loc=self._loc(transform.span)
            ))
            return output_table
        elif transform.kind == "drop":
            drop_list = _to_str_list(transform.params.get("drop", []))
            self.steps.append(OpStep(
                op="drop",
                inputs=[input_table],
                outputs=[output_table],
                params={"cols": drop_list},
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
                params=_lower_rename_params(transform.params.get("mappings") or transform.params.get("mapping")),
                loc=self._loc(transform.span)
            ))
            return output_table
        elif transform.kind == "cast":
            casts = transform.params.get("casts") or []
            self.steps.append(OpStep(
                op="cast",
                inputs=[input_table],
                outputs=[output_table],
                params={"casts": casts},
                loc=self._loc(transform.span),
            ))
            return output_table
        return output_table

    def _loc(self, span: SourceSpan) -> Loc:
        return Loc(file=self.file_name, line_start=span.start, line_end=span.end)


def lower_script(script: SansScript, file_name: str) -> Tuple[List[OpStep], Set[str]]:
    lowerer = Lowerer(file_name)
    return lowerer.lower(script)
