from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from sans.expr import ExprNode
from sans.parser_expr import parse_expression_from_string, tokenize

from .ast import (
    DataStmt,
    DropStmt,
    FormatEntry,
    FormatStmt,
    KeepStmt,
    RenameStmt,
    SansScript,
    SansScriptStmt,
    SelectStmt,
    SortStmt,
    SourceSpan,
    SummaryStmt,
)
from .errors import SansScriptError
from .validate import ensure_true


class _Line:
    def __init__(self, text: str, number: int) -> None:
        self.text = text
        self.stripped = text.strip()
        self.number = number


class SansScriptParser:
    HEADER = "sans 1.0"

    def __init__(self, text: str) -> None:
        self._lines = [
            _Line(line.rstrip("\r"), idx + 1)
            for idx, line in enumerate(text.splitlines())
        ]
        self._idx = 0
        self._file_name: str | None = None

    def parse(self, file_name: str) -> SansScript:
        self._file_name = file_name
        header = self._next_content_line()
        if not header or header.stripped.lower() != self.HEADER:
            line_no = header.number if header else 1
            raise SansScriptError(
                code="E_MISSING_HEADER",
                message="Missing 'sans 1.0' header.",
                line=line_no,
                hint="Start the script with the literal 'sans 1.0'.",
            )
        statements: List[SansScriptStmt] = []
        span_end = header.number
        while True:
            line = self._next_content_line()
            if not line:
                break
            stmt = self._parse_statement(line)
            statements.append(stmt)
            span_end = max(span_end, stmt.span.end)
        span = SourceSpan(start=header.number, end=span_end)
        return SansScript(statements=statements, span=span)

    def _parse_statement(self, line: _Line) -> SansScriptStmt:
        text = line.stripped.rstrip(";")
        lower = text.lower()
        if lower.startswith("format "):
            return self._parse_format(line, text)
        if lower.startswith("data "):
            return self._parse_data(line, text)
        if lower.startswith("sort "):
            return self._parse_sort(line, text)
        if lower.startswith("summary "):
            return self._parse_summary(line, text)
        if lower.startswith("select "):
            return self._parse_select(line, text)
        raise SansScriptError(
            code="E_UNKNOWN_STMT",
            message=f"Unknown statement: '{text}'.",
            line=line.number,
            hint="Only format, data, sort, summary, and select are supported in v1.",
        )

    def _parse_format(self, line: _Line, text: str) -> FormatStmt:
        match = re.match(r"format\s+(\$?[a-zA-Z_]\w*)\s+do", text, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed format declaration.",
                line=line.number,
                hint="Use: format $name do ... end",
            )
        name = match.group(1).lower()
        body, end_line = self._collect_block(line)
        entries: List[FormatEntry] = []
        for body_line in body:
            text_line = body_line.stripped.rstrip(";")
            if not text_line or text_line.lower() == "run":
                continue
            if "=>" in text_line:
                raise SansScriptError(
                    code="E_PARSE",
                    message=f"FORMAT mappings must use '->', got '=>': '{text_line}'.",
                    line=body_line.number,
                    hint="Use: \"HIGH\" -> \"High risk\"",
                )
            if "->" in text_line:
                arrow = "->"
            else:
                raise SansScriptError(
                    code="E_PARSE",
                    message=f"Malformed mapping: '{text_line}'.",
                    line=body_line.number,
                )
            lhs, rhs = [part.strip() for part in text_line.split(arrow, 1)]
            is_other = lhs.lower() == "other"
            key = "" if is_other else self._strip_quotes(lhs, body_line.number)
            value = self._strip_quotes(rhs, body_line.number)
            entries.append(FormatEntry(key=key, value=value, is_other=is_other))
        ensure_true(
            bool(entries),
            code="E_PARSE",
            message="FORMAT block must declare at least one mapping.",
            line=line.number,
        )
        span = SourceSpan(start=line.number, end=end_line)
        return FormatStmt(name=name, entries=entries, span=span)

    def _parse_data(self, line: _Line, text: str) -> DataStmt:
        match = re.match(r"data\s+(\w+)\s+do", text, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed data declaration.",
                line=line.number,
                hint="Use: data <name> do ... end",
            )
        output = match.group(1).lower()
        body, end_line = self._collect_block(line)
        set_table: str | None = None
        input_keep: List[str] = []
        input_drop: List[str] = []
        input_rename: Dict[str, str] = {}
        input_where: ExprNode | None = None
        statements: List[Dict[str, Any]] = []
        keep_stmt: KeepStmt | None = None
        drop_stmt: DropStmt | None = None
        idx = 0
        while idx < len(body):
            body_line = body[idx]
            content = body_line.stripped.rstrip(";")
            if not content:
                idx += 1
                continue
            lowered = content.lower()
            if lowered.startswith("from "):
                if set_table:
                    raise SansScriptError(
                        code="E_BAD_SET",
                        message="Data step can contain only one FROM statement.",
                        line=body_line.number,
                    )
                (
                    set_table,
                    input_keep,
                    input_drop,
                    input_rename,
                    input_where,
                    next_idx,
                ) = self._parse_from_block(body, idx)
                idx = next_idx
                continue
            if lowered.startswith("keep"):
                if keep_stmt:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="Only one KEEP clause is allowed in a DATA block.",
                        line=body_line.number,
                    )
                keep_stmt = KeepStmt(
                    columns=self._parse_function_columns(content, "keep", body_line.number),
                    span=SourceSpan(body_line.number, body_line.number),
                )
                idx += 1
                continue
            if lowered.startswith("drop"):
                if drop_stmt:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="Only one DROP clause is allowed in a DATA block.",
                        line=body_line.number,
                    )
                drop_stmt = DropStmt(
                    columns=self._parse_function_columns(content, "drop", body_line.number),
                    span=SourceSpan(body_line.number, body_line.number),
                )
                idx += 1
                continue
            if lowered.startswith("where"):
                raise SansScriptError(
                    code="E_PARSE",
                    message="WHERE is only allowed inside FROM blocks.",
                    line=body_line.number,
                    hint="Use filter(...) for output filtering.",
                )
            if lowered.startswith("rename"):
                raise SansScriptError(
                    code="E_PARSE",
                    message="RENAME is only allowed inside FROM blocks.",
                    line=body_line.number,
                )
            stmt, next_idx = self._parse_flow_statement(body, idx)
            statements.append(stmt)
            idx = next_idx
        if not set_table:
            raise SansScriptError(
                code="E_BAD_SET",
                message="Data step missing FROM statement.",
                line=line.number,
                hint="Add one FROM <source> do ... end block to the data block.",
            )
        if keep_stmt and drop_stmt:
            raise SansScriptError(
                code="E_PARSE",
                message="KEEP and DROP cannot both be specified in a DATA block.",
                line=line.number,
            )
        span = SourceSpan(start=line.number, end=end_line)
        return DataStmt(
            output=output,
            table=set_table,
            input_keep=input_keep,
            input_drop=input_drop,
            input_rename=input_rename,
            input_where=input_where,
            statements=statements,
            keep=keep_stmt,
            drop=drop_stmt,
            span=span,
        )

    def _parse_sort(self, line: _Line, text: str) -> SortStmt:
        match = re.match(r"sort\s+(\w+)\s*->\s*(\w+)\s+by\s+(.+)$", text, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed SORT statement.",
                line=line.number,
                hint="Use: sort <in> -> <out> by <cols> [nodupkey <true|false>]",
            )
        source = match.group(1).lower()
        target = match.group(2).lower()
        remainder = match.group(3)
        nodupkey = False
        nodup_text = ""
        match = re.search(r"\s+nodupkey\s+", remainder, flags=re.IGNORECASE)
        if match:
            before = remainder[: match.start()]
            after = remainder[match.end() :]
            remainder = before
            nodup_text = after.strip()
        if nodup_text:
            nodupkey = nodup_text.split()[0].lower() == "true"
        by_cols = self._parse_columns(remainder, line.number)
        ensure_true(
            bool(by_cols),
            code="E_PARSE",
            message="SORT must specify at least one BY column.",
            line=line.number,
        )
        span = SourceSpan(start=line.number, end=line.number)
        return SortStmt(source=source, target=target, by=by_cols, nodupkey=nodupkey, span=span)

    def _parse_summary(self, line: _Line, text: str) -> SummaryStmt:
        match = re.match(r"summary\s+(\w+)\s*->\s*(\w+)\s+do", text, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed SUMMARY statement.",
                line=line.number,
            )
        source = match.group(1).lower()
        target = match.group(2).lower()
        body, end_line = self._collect_block(line)
        class_line = self._find_body_line(body, "class")
        var_line = self._find_body_line(body, "var")
        ensure_true(
            class_line is not None,
            code="E_PARSE",
            message="SUMMARY requires a CLASS clause.",
            line=line.number,
        )
        ensure_true(
            var_line is not None,
            code="E_PARSE",
            message="SUMMARY requires a VAR clause.",
            line=line.number,
        )
        class_keys = self._parse_columns(class_line.stripped[6:], class_line.number)
        vars_keys = self._parse_columns(var_line.stripped[4:], var_line.number)
        span = SourceSpan(start=line.number, end=end_line)
        return SummaryStmt(
            source=source,
            target=target,
            class_keys=class_keys,
            vars=vars_keys,
            span=span,
        )

    def _parse_select(self, line: _Line, text: str) -> SelectStmt:
        match = re.match(r"select\s+(\w+)\s*->\s*(\w+)(.*)$", text, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed SELECT statement.",
                line=line.number,
            )
        source = match.group(1).lower()
        target = match.group(2).lower()
        clause = match.group(3).strip()
        keep: List[str] = []
        drop: List[str] = []
        if clause.lower().startswith("keep"):
            keep = self._parse_function_columns(clause, "keep", line.number)
        elif clause.lower().startswith("drop"):
            drop = self._parse_function_columns(clause, "drop", line.number)
        else:
            raise SansScriptError(
                code="E_PARSE",
                message="SELECT requires KEEP or DROP clause.",
                line=line.number,
            )
        span = SourceSpan(start=line.number, end=line.number)
        return SelectStmt(source=source, target=target, keep=keep, drop=drop, span=span)

    def _parse_predicate_clause(self, content: str, keyword: str, line_no: int) -> ExprNode:
        lowered = content.lower()
        if not lowered.startswith(keyword):
            raise SansScriptError(
                code="E_PARSE",
                message=f"Expected {keyword} clause, got '{content}'.",
                line=line_no,
            )
        remainder = content[len(keyword):].strip()
        if remainder.startswith("(") and remainder.endswith(")"):
            remainder = remainder[1:-1].strip()
        if not remainder:
            raise SansScriptError(
                code="E_PARSE",
                message=f"{keyword.upper()} requires a predicate expression.",
                line=line_no,
            )
        return self._parse_expr(remainder, line_no)

    def _parse_assignment_stmt(self, content: str, line_no: int) -> Dict[str, Any] | None:
        allow_overwrite = False
        assign_text = content
        if content.lower().startswith("derive!"):
            allow_overwrite = True
            assign_text = content[len("derive!"):].strip()
            if not assign_text:
                raise SansScriptError(
                    code="E_PARSE",
                    message="DERIVE! requires an assignment target.",
                    line=line_no,
                )
        match = re.match(r"([a-zA-Z_][\w\.]*)\s*=\s*(.+)", assign_text)
        if not match:
            return None
        target = match.group(1).lower()
        expr_str = match.group(2).strip()
        return {
            "type": "assign",
            "target": target,
            "expr": self._parse_expr(expr_str, line_no),
            "allow_overwrite": allow_overwrite,
        }

    def _parse_inline_action(self, text: str, line_no: int) -> Dict[str, Any]:
        lowered = text.strip().lower()
        if lowered.startswith("filter"):
            predicate = self._parse_predicate_clause(text.strip(), "filter", line_no)
            return {"type": "filter", "predicate": predicate}
        assignment = self._parse_assignment_stmt(text.strip(), line_no)
        if assignment:
            return assignment
        raise SansScriptError(
            code="E_PARSE",
            message=f"Unsupported inline action: '{text}'.",
            line=line_no,
        )

    def _parse_if_block(self, body: List[_Line], start_idx: int) -> tuple[Dict[str, Any], int]:
        header_line = body[start_idx]
        header = header_line.stripped.rstrip(";")
        inline_match = re.match(
            r"if\s+(.+?)\s+then\s+(.+?)(?:\s+else\s+(.+?))?\s+end$",
            header,
            re.IGNORECASE,
        )
        if inline_match:
            predicate = self._parse_expr(inline_match.group(1).strip(), header_line.number)
            then_action = self._parse_inline_action(inline_match.group(2).strip(), header_line.number)
            else_action = None
            if inline_match.group(3):
                else_action = self._parse_inline_action(inline_match.group(3).strip(), header_line.number)
            return {"type": "if_then", "predicate": predicate, "then": then_action, "else": else_action}, start_idx + 1

        if not header.lower().startswith("if "):
            raise SansScriptError(
                code="E_PARSE",
                message=f"Malformed IF statement: '{header}'.",
                line=header_line.number,
            )
        condition = header[3:].strip()
        if condition.lower().endswith(" then"):
            condition = condition[:-5].strip()
        predicate = self._parse_expr(condition, header_line.number)
        idx = start_idx + 1
        then_body: List[Dict[str, Any]] = []
        else_body: List[Dict[str, Any]] = []
        in_else = False
        while idx < len(body):
            line = body[idx]
            content = line.stripped.rstrip(";")
            lowered = content.lower()
            if lowered == "end":
                if not then_body:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="IF block must contain at least one statement.",
                        line=header_line.number,
                    )
                then_stmt: Dict[str, Any] = then_body[0] if len(then_body) == 1 else {"type": "block", "body": then_body}
                else_stmt: Dict[str, Any] | None = None
                if else_body:
                    else_stmt = else_body[0] if len(else_body) == 1 else {"type": "block", "body": else_body}
                return {"type": "if_then", "predicate": predicate, "then": then_stmt, "else": else_stmt}, idx + 1
            if lowered == "else":
                in_else = True
                idx += 1
                continue
            stmt, next_idx = self._parse_flow_statement(body, idx)
            if in_else:
                else_body.append(stmt)
            else:
                then_body.append(stmt)
            idx = next_idx
        raise SansScriptError(
            code="E_PARSE",
            message="Missing 'end' for IF block.",
            line=header_line.number,
        )

    def _parse_flow_statement(self, body: List[_Line], start_idx: int) -> tuple[Dict[str, Any], int]:
        line = body[start_idx]
        content = line.stripped.rstrip(";")
        lowered = content.lower()
        if lowered.startswith("filter"):
            predicate = self._parse_predicate_clause(content, "filter", line.number)
            return {"type": "filter", "predicate": predicate}, start_idx + 1
        if lowered.startswith("if "):
            return self._parse_if_block(body, start_idx)
        if lowered.startswith("case "):
            raise SansScriptError(
                code="E_PARSE",
                message="CASE blocks are not supported yet.",
                line=line.number,
            )
        assignment = self._parse_assignment_stmt(content, line.number)
        if assignment:
            return assignment, start_idx + 1
        raise SansScriptError(
            code="E_UNKNOWN_STMT",
            message=f"Unsupported data step clause: '{content}'.",
            line=line.number,
        )

    def _parse_from_block(self, body: List[_Line], start_idx: int) -> tuple[
        str,
        List[str],
        List[str],
        Dict[str, str],
        ExprNode | None,
        int,
    ]:
        header_line = body[start_idx]
        text = header_line.stripped.rstrip(";")
        match = re.match(r"from\s+(\w+)\s+do", text, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_BAD_SET",
                message=f"Malformed FROM clause: '{text}'.",
                line=header_line.number,
            )
        table = match.group(1).lower()
        keep_cols: List[str] = []
        drop_cols: List[str] = []
        rename_map: Dict[str, str] = {}
        where_expr: ExprNode | None = None
        idx = start_idx + 1
        while idx < len(body):
            line = body[idx]
            content = line.stripped.rstrip(";")
            lowered = content.lower()
            if not content:
                idx += 1
                continue
            if lowered == "end":
                if keep_cols and drop_cols:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="KEEP and DROP cannot both be specified in a FROM block.",
                        line=line.number,
                    )
                return table, keep_cols, drop_cols, rename_map, where_expr, idx + 1
            if lowered.startswith("keep"):
                if keep_cols:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="Only one KEEP clause is allowed in a FROM block.",
                        line=line.number,
                    )
                keep_cols = self._parse_function_columns(content, "keep", line.number)
                idx += 1
                continue
            if lowered.startswith("drop"):
                if drop_cols:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="Only one DROP clause is allowed in a FROM block.",
                        line=line.number,
                    )
                drop_cols = self._parse_function_columns(content, "drop", line.number)
                idx += 1
                continue
            if lowered.startswith("rename"):
                rename_map.update(self._parse_rename(content, line.number).mappings)
                idx += 1
                continue
            if lowered.startswith("where"):
                if where_expr is not None:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="Only one WHERE clause is allowed in a FROM block.",
                        line=line.number,
                    )
                where_expr = self._parse_predicate_clause(content, "where", line.number)
                idx += 1
                continue
            if lowered.startswith("filter"):
                raise SansScriptError(
                    code="E_PARSE",
                    message="FILTER is only allowed at data step scope.",
                    line=line.number,
                )
            if lowered.startswith("if "):
                raise SansScriptError(
                    code="E_PARSE",
                    message="IF is only allowed at data step scope.",
                    line=line.number,
                )
            if lowered.startswith("case "):
                raise SansScriptError(
                    code="E_PARSE",
                    message="CASE is only allowed at data step scope.",
                    line=line.number,
                )
            raise SansScriptError(
                code="E_UNKNOWN_STMT",
                message=f"Unsupported FROM clause: '{content}'.",
                line=line.number,
            )
        raise SansScriptError(
            code="E_PARSE",
            message="Missing 'end' for FROM block.",
            line=header_line.number,
        )

    def _parse_function_columns(self, content: str, keyword: str, line_no: int) -> List[str]:
        inner = self._extract_parenthesized_content(content, keyword, line_no)
        return [col.strip().lower() for col in re.split(r"[\\s,]+", inner) if col.strip()]

    def _extract_parenthesized_content(self, content: str, keyword: str, line_no: int) -> str:
        lower = content.lower()
        if not lower.startswith(keyword):
            raise SansScriptError(
                code="E_PARSE",
                message=f"Expected '{keyword}()' syntax, got '{content}'.",
                line=line_no,
            )
        remainder = content[len(keyword):].strip()
        if not remainder.startswith("(") or not remainder.endswith(")"):
            raise SansScriptError(
                code="E_PARSE",
                message=f"Malformed {keyword.upper()} call: '{content}'.",
                line=line_no,
                hint=f"Use {keyword}(col1, col2, ...).",
            )
        return remainder[1:-1].strip()

    def _parse_rename(self, content: str, line_no: int) -> RenameStmt:
        inner = self._extract_parenthesized_content(content, "rename", line_no)
        rename_map: dict[str, str] = {}
        if not inner:
            raise SansScriptError(
                code="E_PARSE",
                message="RENAME requires at least one mapping.",
                line=line_no,
            )
        for token in [tok.strip() for tok in inner.split(",") if tok.strip()]:
            if "=>" in token:
                raise SansScriptError(
                    code="E_PARSE",
                    message=f"RENAME mappings must use '->', got '=>': '{token}'.",
                    line=line_no,
                    hint="Use: rename(old -> new)",
                )
            if "->" in token:
                arrow = "->"
            else:
                raise SansScriptError(
                    code="E_PARSE",
                    message=f"Malformed rename mapping: '{token}'.",
                    line=line_no,
                )
            old, new = [part.strip().lower() for part in token.split(arrow, 1)]
            rename_map[old] = new
        return RenameStmt(mappings=rename_map, span=SourceSpan(line_no, line_no))

    def _parse_columns(self, segment: str, line_no: int) -> List[str]:
        cols = [part.strip().strip(",") for part in re.split(r"[\s,]+", segment) if part.strip()]
        if not cols:
            raise SansScriptError(
                code="E_PARSE",
                message="Column list cannot be empty.",
                line=line_no,
            )
        return [col.lower() for col in cols]

    def _collect_block(self, header: _Line) -> tuple[List[_Line], int]:
        body: List[_Line] = []
        depth = 0
        while True:
            line = self._next_content_line()
            if not line:
                raise SansScriptError(
                    code="E_PARSE",
                    message="Missing 'end' for block.",
                    line=header.number,
                )
            cleaned = line.stripped.rstrip(";").lower()
            if cleaned == "end":
                if depth == 0:
                    return body, line.number
                depth -= 1
                body.append(line)
                continue
            if cleaned.startswith("case "):
                depth += 1
            elif cleaned.startswith("from ") and cleaned.endswith(" do"):
                depth += 1
            elif cleaned.startswith("if "):
                inline_if = re.match(r"if\s+.+\s+then\s+.+\s+end$", cleaned)
                if not inline_if:
                    depth += 1
            body.append(line)

    def _consume_case_block(
        self, body: List[_Line], start_idx: int, line_no: int
    ) -> int:
        idx = start_idx + 1
        depth = 0
        saw_when = False
        saw_else = False
        matched_end = False
        while idx < len(body):
            normalized = body[idx].stripped.rstrip(";").lower()
            if normalized.startswith("case "):
                depth += 1
            elif normalized.startswith("when "):
                saw_when = True
            elif normalized == "else":
                saw_else = True
            elif normalized == "end":
                if depth == 0:
                    matched_end = True
                    break
                depth -= 1
            idx += 1
        if not matched_end:
            raise SansScriptError(
                code="E_PARSE",
                message="CASE block missing closing END.",
                line=line_no,
            )
        ensure_true(
            saw_when,
            code="E_PARSE",
            message="CASE requires at least one WHEN clause.",
            line=line_no,
        )
        ensure_true(
            saw_else,
            code="E_INCOMPLETE_CASE",
            message="CASE requires an ELSE clause.",
            line=line_no,
        )
        return idx + 1

    def _find_body_line(self, body: List[_Line], prefix: str) -> Optional[_Line]:
        for line in body:
            if line.stripped.lower().startswith(prefix.lower() + " "):
                return line
        return None

    def _ensure_sans_expr_rules(self, text: str, line_no: int) -> None:
        if not self._file_name:
            return
        for token in tokenize(text, self._file_name):
            if token.type != "OPERATOR":
                continue
            op = token.value.lower()
            if op in {"=", "^=", "~=", "eq", "ne"}:
                raise SansScriptError(
                    code="E_BAD_EXPR",
                    message="Use '==' for equality and '!=' for inequality in sans scripts.",
                    line=line_no,
                )

    def _parse_expr(self, text: str, line_no: int) -> ExprNode:
        try:
            assert self._file_name is not None
            self._ensure_sans_expr_rules(text, line_no)
            return parse_expression_from_string(text, self._file_name)
        except ValueError as exc:
            raise SansScriptError(
                code="E_BAD_EXPR",
                message=str(exc),
                line=line_no,
            )

    def _next_content_line(self) -> Optional[_Line]:
        while self._idx < len(self._lines):
            line = self._lines[self._idx]
            self._idx += 1
            stripped = line.stripped
            if not stripped or stripped.startswith("#"):
                continue
            return line
        return None

    def _strip_quotes(self, token: str, line_no: int) -> str:
        value = token.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", "\""}:
            return value[1:-1]
        raise SansScriptError(
            code="E_PARSE",
            message=f"Expected quoted literal, got '{token}'.",
            line=line_no,
        )


def parse_sans_script(text: str, file_name: str) -> SansScript:
    parser = SansScriptParser(text)
    return parser.parse(file_name)
