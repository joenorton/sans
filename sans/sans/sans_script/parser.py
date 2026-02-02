from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Union

from sans.expr import ExprNode
from sans.parser_expr import parse_expression_from_string, tokenize

from .ast import (
    BuilderExpr,
    FromExpr,
    LetBinding,
    MapEntry,
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
    DatasourceDeclaration, # New import
)
from .errors import SansScriptError


class _Line:
    def __init__(self, text: str, number: int) -> None:
        self.text = text
        self.stripped = text.strip()
        self.number = number
    
    @property
    def raw(self) -> str:
        return self.text

import re
import hashlib

def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _normalize_inline_csv(lines: list[str]) -> str:
    # deterministic normalization: strip trailing whitespace, normalize newlines
    cleaned = [ln.rstrip() for ln in lines]
    # drop leading/trailing completely blank lines inside the block
    while cleaned and cleaned[0].strip() == "":
        cleaned.pop(0)
    while cleaned and cleaned[-1].strip() == "":
        cleaned.pop()
    return "\n".join(cleaned) + ("\n" if cleaned else "")

class SansScriptParser:
    HEADER_MARKER = "# sans "
    HEADER_VERSION = "0.1"

    def __init__(self, text: str) -> None:
        self._lines = [
            _Line(line.rstrip("\r"), idx + 1)
            for idx, line in enumerate(text.splitlines())
        ]
        self._idx = 0
        self._file_name: str | None = None

    def parse(self, file_name: str) -> SansScript:
        self._file_name = file_name
        # 1. Validate header
        header_line = None
        non_empty_seen = 0

        for line in self._lines:
            if not line.stripped:
                continue

            non_empty_seen += 1

            if line.stripped.startswith(self.HEADER_MARKER):
                version_part = line.stripped[len(self.HEADER_MARKER):].strip()
                if version_part == self.HEADER_VERSION:
                    header_line = line
                    break

            if non_empty_seen >= 5:
                break

        if not header_line:
            raise SansScriptError(
                code="E_MISSING_HEADER",
                message=f"Missing '{self.HEADER_MARKER}{self.HEADER_VERSION}' comment header within the first 5 non-empty lines.",
                line=1,
                hint="Start the script with a comment like '# sans 0.1'.",
            )

        # Establish script span start/end
        span_start = header_line.number
        span_end = span_start

        self._idx = 0
        statements: List[SansScriptStmt] = []
        datasources: dict[str, DatasourceDeclaration] = {}
        terminal_expr: Optional[TableExpr] = None
        while True:
            line = self._next_content_line()
            if not line:
                break
            
            # Check if it's a binding or a terminal expression
            if line.stripped.lower().startswith("let "):
                stmt = self._parse_let_binding(line)
                statements.append(stmt)
                span_end = max(span_end, stmt.span.end)
            elif line.stripped.lower().startswith("datasource "):
                ds = self._parse_datasource_declaration(line)
                # enforce unique names here (good error)
                if ds.name in datasources:
                    raise SansScriptError(
                        code="E_PARSE",
                        message=f"Duplicate datasource name '{ds.name}'.",
                        line=line.number,
                    )
                datasources[ds.name] = ds
                statements.append(ds)  # optional; keep if your AST treats it as a stmt
                span_end = max(span_end, ds.span.end)
            elif line.stripped.lower().startswith("table "):
                stmt = self._parse_table_binding(line)
                statements.append(stmt)
                span_end = max(span_end, stmt.span.end)
            else:
                # This must be the terminal table expression.
                # It must be the very last content line.
                next_content = self._peek_content_line()
                if next_content:
                    raise SansScriptError(
                        code="E_PARSE",
                        message=f"Unexpected statement: '{line.stripped}'. Only let, datasource, and table bindings are supported before the terminal expression.",
                        line=line.number,
                    )
                
                terminal_expr = self._parse_table_expr(line)
                span_end = max(span_end, terminal_expr.span.end)
                break

        span = SourceSpan(start=span_start, end=span_end)
        return SansScript(
            statements=statements,
            terminal_expr=terminal_expr,
            span=span,
            datasources=datasources,
        )

    def _parse_datasource_declaration(self, line: _Line) -> DatasourceDeclaration:
        s = line.stripped

        # 1) parse leading: datasource <name> = <rest>
        m = re.match(r"^datasource\s+([a-zA-Z_]\w*)\s*=\s*(.+)$", s, re.IGNORECASE)
        if not m:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed datasource declaration.",
                line=line.number,
                hint='Use: datasource name = csv("path/to/file.csv"[, columns(a, b)])  OR  datasource name = inline_csv do ... end',
            )

        name = m.group(1).lower()
        rhs = m.group(2).strip()

        # ------------------------------------------------------------------
        # csv("path"[, columns(...)])
        # ------------------------------------------------------------------
        m_csv = re.match(
            r'^csv\s*\(\s*"([^"]*)"\s*(?:,\s*columns\s*\(([^)]*)\)\s*)?\)$',
            rhs,
            re.IGNORECASE,
        )
        if m_csv:
            path = m_csv.group(1)
            columns_str = m_csv.group(2)
            columns = self._parse_columns(columns_str, line.number) if columns_str else []
            return DatasourceDeclaration(
                name=name,
                kind="csv",
                path=path,
                columns=columns,
                inline_text=None,
                inline_sha256=None,
                span=SourceSpan(line.number, line.number),
            )

        # ------------------------------------------------------------------
        # inline_csv [columns(...)] do ... end
        #
        # Accept:
        #   datasource in = inline_csv do
        #     a,b
        #     1,2
        #   end
        #
        # Or with optional schema pin:
        #   datasource in = inline_csv columns(a,b) do
        #     a,b
        #     1,2
        #   end
        # ------------------------------------------------------------------
        m_inline = re.match(
            r"^inline_csv(?:\s+columns\s*\(([^)]*)\))?\s+do\s*$",
            rhs,
            re.IGNORECASE,
        )
        if m_inline:
            columns_str = m_inline.group(1)
            columns = self._parse_columns(columns_str, line.number) if columns_str else []

            # read block lines until `end`
            body_lines: list[str] = []
            start_line = line.number

            while True:
                next_line = self._next_line_raw()  # <-- you need a helper that returns the next _Line without skipping comments
                if next_line is None:
                    raise SansScriptError(
                        code="E_PARSE",
                        message="Unterminated inline_csv block: expected 'end'.",
                        line=start_line,
                    )
                if next_line.stripped.lower() == "end":
                    end_line = next_line.number
                    break
                body_lines.append(next_line.raw)  # or next_line.text; whichever preserves original (minus \r)

            normalized = _normalize_inline_csv(body_lines)
            sha = _sha256_text(normalized)

            return DatasourceDeclaration(
                name=name,
                kind="inline_csv",
                path=None,
                columns=columns,
                inline_text=normalized,
                inline_sha256=sha,
                span=SourceSpan(start_line, end_line),
            )

        # If neither form matched:
        raise SansScriptError(
            code="E_PARSE",
            message="Malformed datasource declaration.",
            line=line.number,
            hint=(
                'Use: datasource name = csv("path/to/file.csv"[, columns(a, b)])\n'
                "Or:  datasource name = inline_csv [columns(...)] do\\n  ...csv lines...\\nend"
            ),
        )

    def _parse_let_binding(self, line: _Line) -> LetBinding:
        # let name = <scalar-expr> | map(...)
        match = re.match(r"let\s+([a-zA-Z_]\w*)\s*=\s*(.+)", line.stripped, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed let binding.",
                line=line.number,
                hint="Use: let name = expression",
            )
        name = match.group(1).lower()
        rhs = match.group(2).strip()

        if rhs.lower().startswith("map("):
            # Check if it's a multiline map or single line
            if rhs.lower().endswith(")") or ")" in rhs:
                # Try parsing as single line first
                expr = self._parse_map_expr(rhs, line.number)
                span = SourceSpan(line.number, line.number)
            else:
                 # Multiline map
                body, end_line = self._collect_block_by_parens(line, "map")
                # Reconstruct RHS for map parsing
                full_rhs = rhs + "\n" + "\n".join(l.text for l in body) + "\n)"
                expr = self._parse_map_expr(full_rhs, line.number)
                span = SourceSpan(line.number, end_line)
        else:
            expr = self._parse_expr(rhs, line.number)
            span = SourceSpan(line.number, line.number)

        return LetBinding(name=name, expr=expr, span=span)

    def _parse_table_binding(self, line: _Line) -> TableBinding:
        # table name = <table-expr>
        match = re.match(r"table\s+([a-zA-Z_]\w*)\s*=\s*(.+)", line.stripped, re.IGNORECASE)
        if not match:
            raise SansScriptError(
                code="E_PARSE",
                message="Malformed table binding.",
                line=line.number,
                hint="Use: table name = table_expression",
            )
        name = match.group(1).lower()
        rhs_start = match.group(2).strip()
        
        # We need to handle the case where <table-expr> might involve a block (do...end)
        expr = self._parse_table_expr(line, rhs_override=rhs_start)
        return TableBinding(name=name, expr=expr, span=expr.span)

    def _parse_table_expr(self, line: _Line, rhs_override: Optional[str] = None) -> TableExpr:
        text = rhs_override if rhs_override is not None else line.stripped
        
        primary: TableExpr
        remainder: str = ""
        end_line = line.number

        if text.lower().startswith("from("):
            # from(datasource_name)
            inner, remainder = self._extract_balanced_parens(text[4:], line.number)
            source = inner.strip().lower()
            primary = FromExpr(source=source, span=SourceSpan(line.number, line.number))
            
            if remainder.lower().startswith("do") or remainder.lower() == "do":
                body, end_line = self._collect_block(line)
                steps = self._parse_pipeline_steps(body)
                primary = PipelineExpr(source=primary, steps=steps, span=SourceSpan(line.number, end_line))
                remainder = ""
        elif text.lower().startswith("sort("):
            inner, remainder = self._extract_balanced_parens(text[4:], line.number)
            # Recursively parse inner as table expr
            source = self._parse_table_expr(line, rhs_override=inner)
            primary, remainder, end_line = self._parse_builder("sort", source, remainder, line)
        elif text.lower().startswith("summary("):
            inner, remainder = self._extract_balanced_parens(text[7:], line.number)
            source = self._parse_table_expr(line, rhs_override=inner)
            primary, remainder, end_line = self._parse_builder("summary", source, remainder, line)
        else:
            # Table name
            name_match = re.match(r"([a-zA-Z_]\w*)(.*)", text)
            if not name_match:
                raise SansScriptError(
                    code="E_PARSE",
                    message=f"Expected table expression, got '{text}'.",
                    line=line.number,
                )
            name = name_match.group(1).lower()
            remainder = name_match.group(2).strip()
            primary = TableNameExpr(name=name, span=SourceSpan(line.number, line.number))

        return self._parse_postfix_clauses(primary, remainder, end_line)

    def _extract_balanced_parens(self, text: str, line_no: int) -> tuple[str, str]:
        """Expects text to start after an opening '('. Returns (inner, remainder)."""
        # Text starts with '(...'
        if not text.startswith("("):
             raise SansScriptError(code="E_PARSE", message="Expected '(", line=line_no)
        
        depth = 0
        for i, char in enumerate(text):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return text[1:i], text[i+1:].strip()
        
        raise SansScriptError(code="E_PARSE", message="Unbalanced parentheses", line=line_no)

    def _parse_pipeline_steps(self, body: List[_Line]) -> List[TableTransform]:
        steps: List[TableTransform] = []
        idx = 0
        while idx < len(body):
            line = body[idx]
            content = line.stripped.rstrip(";")
            idx += 1
            if not content: continue
            
            lowered = content.lower()
            
            # Identify keyword and initial args
            kw = None
            kw_len = 0
            
            processed_content = content
            processed_lowered = lowered

            for k in ["select", "filter", "derive", "rename", "drop"]:
                if processed_lowered.startswith(k + " ") or processed_lowered.startswith(k + "("):
                    kw = k
                    kw_len = len(k)
                    break
            
            if kw:
                args = processed_content[kw_len:].strip()
                
                # Check for 'do' block
                if args.lower() == "do" or args.lower().endswith(" do"):
                     block_lines = []
                     temp_idx = idx
                     depth = 0
                     found_end = False
                     
                     while temp_idx < len(body):
                         l = body[temp_idx]
                         cleaned = l.stripped.rstrip(";").lower()
                         if cleaned == "end":
                             if depth == 0:
                                 found_end = True
                                 temp_idx += 1
                                 break
                             depth -= 1
                         elif cleaned.endswith(" do"):
                             depth += 1
                         
                         block_lines.append(l.stripped.rstrip(";"))
                         temp_idx += 1
                     
                     if not found_end:
                         raise SansScriptError(code="E_PARSE", message="Missing 'end' for nested block.", line=line.number)
                     
                     idx = temp_idx
                     block_content = ", ".join(block_lines)
                     
                     if args.lower() == "do":
                         args = block_content
                     else:
                         prefix = args[:-2].strip()
                         args = (prefix + ", " + block_content) if prefix else block_content

                # Multiline parens logic for args
                elif args.startswith("(") and not args.endswith(")"):
                    # This implies the multiline parsing should be handled here, 
                    # consuming multiple 'line' objects from 'body'
                    
                    # Store current idx to restore if no match, or consume lines
                    current_idx_in_body = idx - 1 # Current 'line' is body[idx-1]
                    matched_parens = False
                    
                    full_args_lines = []
                    temp_idx = current_idx_in_body
                    temp_depth = 0
                    
                    # Find the opening paren in the 'args' part of the current line
                    paren_start_pos = args.find("(")
                    if paren_start_pos != -1:
                        temp_depth += 1
                        full_args_lines.append(args[paren_start_pos:])
                        
                        while temp_idx < len(body):
                            if temp_idx > current_idx_in_body: # Only process subsequent lines
                                stripped = body[temp_idx].stripped.rstrip(";")
                                full_args_lines.append(stripped)
                            else:
                                stripped = args[paren_start_pos + 1:]
                                
                            for char in stripped:
                                if char == "(": temp_depth += 1
                                elif char == ")": temp_depth -= 1
                            
                            if temp_depth == 0:
                                matched_parens = True
                                # Consume lines up to here
                                idx = temp_idx + 1 
                                break
                            temp_idx += 1
                    
                    if matched_parens:
                        all_text = " ".join(full_args_lines)
                        # The args should be the content INSIDE the outermost balanced parens
                        start_paren = all_text.find("(")
                        end_paren = all_text.rfind(")")
                        if start_paren != -1 and end_paren != -1:
                            args = all_text[start_paren:end_paren+1]
                        else:
                            raise SansScriptError(code="E_PARSE", message="Malformed multiline parenthesized arguments.", line=line.number)
                    else:
                        raise SansScriptError(code="E_PARSE", message="Unbalanced parentheses in multiline argument.", line=line.number)


                transform = self._parse_transform_clause(kw, args, line.number)
                steps.append(transform)

            elif processed_lowered.startswith("sort()") or processed_lowered.startswith("summary()"):
                 raise SansScriptError(code="E_NOT_IMPL", message="Implicit source builders in pipeline not yet implemented.", line=line.number)
            else:
                raise SansScriptError(
                    code="E_PARSE",
                    message=f"Unknown pipeline transform: '{content}'.",
                    line=line.number,
                )
        return steps

    def _parse_builder(self, kind: str, source: TableExpr, remainder: str, line: _Line) -> tuple[BuilderExpr, str, int]:
        config: Dict[str, Any] = {}
        curr_rem = remainder
        curr_line = line.number
        
        while True:
            if not curr_rem:
                # Peek at next line
                next_line = self._peek_content_line()
                if next_line and next_line.stripped.startswith("."):
                    self._next_content_line() # consume it
                    curr_rem = next_line.stripped
                    curr_line = next_line.number
                else:
                    break

            match = re.match(r"\.([a-zA-Z_]\w*)\(([^)]*)\)(.*)", curr_rem)
            if not match:
                break
            method = match.group(1).lower()
            args_str = match.group(2).strip()
            curr_rem = match.group(3).strip()
            
            if kind == "sort":
                if method == "by":
                    config["by"] = self._parse_columns(args_str, curr_line)
                elif method == "nodupkey":
                    config["nodupkey"] = args_str.lower() == "true"
                else:
                    raise SansScriptError(code="E_PARSE", message=f"Unknown sort builder method: {method}", line=curr_line)
            elif kind == "summary":
                if method == "class":
                    config["class"] = self._parse_columns(args_str, curr_line)
                elif method == "var":
                    config["var"] = self._parse_columns(args_str, curr_line)
                elif method == "stats":
                    config["stats"] = self._parse_columns(args_str, curr_line)
                else:
                    raise SansScriptError(code="E_PARSE", message=f"Unknown summary builder method: {method}", line=curr_line)
        
        return BuilderExpr(kind=kind, source=source, config=config, span=SourceSpan(line.number, curr_line)), curr_rem, curr_line

    def _parse_postfix_clauses(self, primary: TableExpr, remainder: str, end_line: int) -> TableExpr:
        curr_expr = primary
        curr_rem = remainder
        curr_end_line = end_line
        
        while True:
            if not curr_rem:
                # Peek at next line for postfix keywords or bracket sugar
                next_line = self._peek_content_line()
                if next_line:
                    s = next_line.stripped.lower()
                    # Check if it looks like a postfix clause or bracket sugar
                    if s.startswith("[") or any(s.startswith(kw + " ") or s.startswith(kw + "(") for kw in ["select", "filter", "derive", "rename", "drop"]):
                        self._next_content_line() # consume it
                        curr_rem = next_line.stripped
                        curr_end_line = next_line.number
                    else:
                        break
                else:
                    break

            if curr_rem.startswith("["):
                # Bracket sugar for select
                match = re.match(r"\[([^\]]+)\](.*)", curr_rem)
                if not match:
                    break
                cols = self._parse_columns(match.group(1), curr_end_line)
                transform = TableTransform(kind="select", params={"keep": cols}, span=SourceSpan(curr_end_line, curr_end_line))
                curr_expr = PostfixExpr(source=curr_expr, transform=transform, span=SourceSpan(primary.span.start, curr_end_line))
                curr_rem = match.group(2).strip()
                continue

            match = re.match(r"(select|filter|derive|rename|drop)(?:\s+|\()", curr_rem, re.IGNORECASE)
            if not match:
                break
            
            kind = match.group(1).lower()
            # We need to find the full args_part. 
            # If it started with '(', we need to find the matching ')'
            if curr_rem[len(kind)] == "(":
                 inner, next_rem = self._extract_balanced_parens(curr_rem[len(kind):], curr_end_line)
                 this_args = "(" + inner + ")"
                 curr_rem = next_rem
            else:
                 args_part = curr_rem[len(kind):].strip()
                 # Simple heuristic: split by next keyword
                 next_kw_match = re.search(r"\s+(select|filter|derive|rename|drop)(?:\s+|\()", args_part, re.IGNORECASE)
                 if next_kw_match:
                     this_args = args_part[:next_kw_match.start()].strip()
                     curr_rem = args_part[next_kw_match.start():].strip()
                 else:
                     this_args = args_part
                     curr_rem = ""
            
            transform = self._parse_transform_clause(kind, this_args, curr_end_line)
            curr_expr = PostfixExpr(source=curr_expr, transform=transform, span=SourceSpan(primary.span.start, curr_end_line))
            
        if curr_rem.strip():
             raise SansScriptError(
                code="E_PARSE",
                message=f"Unexpected trailing content: '{curr_rem.strip()}'.",
                line=curr_end_line,
            )
            
        return curr_expr

    def _parse_transform_clause(self, kind: str, args_str: str, line_no: int) -> TableTransform:
        params: Dict[str, Any] = {}
        if kind == "select":
            params["keep"] = self._parse_columns(args_str, line_no)
        elif kind == "drop":
            params["drop"] = self._parse_columns(args_str, line_no)
        elif kind == "filter":
            # Strip parens if present
            if args_str.startswith("(") and args_str.endswith(")"):
                args_str = args_str[1:-1].strip()
            params["predicate"] = self._parse_expr(args_str, line_no)
        elif kind == "derive":
            # derive(a = 1, update! b = 2)
            if args_str.startswith("(") and args_str.endswith(")"):
                args_str = args_str[1:-1].strip()
            params["assignments"] = self._parse_derive_assignments(args_str, line_no)
        elif kind == "rename":
            if args_str.startswith("(") and args_str.endswith(")"):
                args_str = args_str[1:-1].strip()
            params["mappings"] = self._parse_rename_mappings(args_str, line_no)
        
        return TableTransform(kind=kind, params=params, span=SourceSpan(line_no, line_no))

    def _parse_derive_assignments(self, text: str, line_no: int) -> List[Dict[str, Any]]:
        parts = self._split_by_comma_respecting_parens(text)
        assignments = []
        for part in parts:
            part = part.strip()
            allow_overwrite = False
            if part.lower().startswith("update!"):
                allow_overwrite = True
                part = part[7:].strip()
            
            match = re.match(r"([a-zA-Z_]\w*)\s*=\s*(.+)", part)
            if not match:
                raise SansScriptError(code="E_PARSE", message=f"Malformed derive assignment: {part}", line=line_no)
            
            assignments.append({
                "type": "assign",
                "target": match.group(1).lower(),
                "expr": self._parse_expr(match.group(2).strip(), line_no),
                "allow_overwrite": allow_overwrite
            })
        return assignments

    def _parse_rename_mappings(self, text: str, line_no: int) -> Dict[str, str]:
        parts = text.split(",")
        mappings = {}
        for part in parts:
            part = part.strip()
            if not part: continue
            if "->" not in part:
                 raise SansScriptError(code="E_PARSE", message=f"Rename mapping must use '->': {part}", line=line_no)
            old, new = part.split("->")
            mappings[old.strip().lower()] = new.strip().lower()
        return mappings

    def _parse_map_expr(self, text: str, line_no: int) -> MapExpr:
        # map("A" -> "B", _ -> "C")
        inner = text.strip()
        if inner.lower().startswith("map("):
            inner = inner[4:].strip()
            if inner.endswith(")"):
                inner = inner[:-1].strip()
        
        entries: List[MapEntry] = []
        parts = self._split_by_comma_respecting_parens(inner)
        for part in parts:
            part = part.strip()
            if not part: continue
            if "->" not in part:
                 raise SansScriptError(code="E_PARSE", message=f"Map entry must use '->': {part}", line=line_no)
            lhs, rhs = part.split("->", 1)
            lhs = lhs.strip()
            rhs = rhs.strip()
            
            key: Optional[str] = None
            if lhs == "_":
                key = None
            else:
                key = self._strip_quotes(lhs, line_no)
            
            value = self._parse_expr(rhs, line_no)
            entries.append(MapEntry(key=key, value=value, span=SourceSpan(line_no, line_no)))
        
        return MapExpr(entries=entries, span=SourceSpan(line_no, line_no))

    def _split_by_comma_respecting_parens(self, text: str) -> List[str]:
        parts = []
        curr = ""
        depth = 0
        for char in text:
            if char == "(": depth += 1
            elif char == ")": depth -= 1
            elif char == "," and depth == 0:
                parts.append(curr)
                curr = ""
                continue
            curr += char
        parts.append(curr)
        return [p.strip() for p in parts if p.strip()]

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
            if cleaned.endswith(" do"):
                depth += 1
            body.append(line)

    def _collect_block_by_parens(self, header: _Line, start_word: str) -> tuple[List[_Line], int]:
        body: List[_Line] = []
        depth = 1 # We assumed we saw the opening '('
        while True:
            line = self._next_content_line()
            if not line:
                raise SansScriptError(
                    code="E_PARSE",
                    message=f"Missing ')' for {start_word}.",
                    line=header.number,
                )
            for char in line.text:
                if char == "(": depth += 1
                elif char == ")": depth -= 1
            
            if depth == 0:
                # We need to be careful if there's trailing content on the same line as ')'
                return body, line.number
            body.append(line)

    def _ensure_sans_expr_rules(self, text: str, line_no: int) -> None:
        if not self._file_name:
            return
        # Basic check for == and !=
        # This is a bit naive if they are inside strings, but tokenize should handle it.
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

    def _peek_content_line(self) -> Optional[_Line]:
        idx = self._idx
        while idx < len(self._lines):
            line = self._lines[idx]
            stripped = line.stripped
            if not stripped or stripped.startswith("#"):
                idx += 1
                continue
            return line
        return None

    def _next_content_line(self) -> Optional[_Line]:
        while self._idx < len(self._lines):
            line = self._lines[self._idx]
            self._idx += 1
            stripped = line.stripped
            if not stripped or stripped.startswith("#"):
                continue
            return line
        return None

    def _next_line_raw(self) -> _Line | None:
        if self._idx >= len(self._lines):
            return None
        line = self._lines[self._idx]
        self._idx += 1
        return line

    def _strip_quotes(self, token: str, line_no: int) -> str:
        value = token.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            return value[1:-1]
        if not value: # Handle empty string case
            return ""
        raise SansScriptError(
            code="E_PARSE",
            message=f"Expected quoted literal, got '{token}'.",
            line=line_no,
        )


def parse_sans_script(text: str, file_name: str) -> SansScript:
    parser = SansScriptParser(text)
    return parser.parse(file_name)
