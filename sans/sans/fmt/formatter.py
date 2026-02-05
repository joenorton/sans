from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
import re

from sans.sans_script import SansScriptError, parse_sans_script


FMT_STYLE_ID = "v0"

POSTFIX_KEYWORDS = ("select", "filter", "derive", "rename", "drop", "update!", "cast")
PIPELINE_KEYWORDS = ("select", "filter", "derive", "rename", "drop", "update!", "cast")
BUILDER_METHODS = ("by", "nodupkey", "class", "var", "stats")


def normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def split_inline_comment(line: str) -> tuple[str, Optional[str]]:
    in_single = False
    in_double = False
    in_triple_single = False
    in_triple_double = False
    escape = False
    i = 0
    while i < len(line):
        ch = line[i]
        if in_triple_single:
            if line.startswith("'''", i):
                in_triple_single = False
                i += 3
                continue
            i += 1
            continue
        if in_triple_double:
            if line.startswith('"""', i):
                in_triple_double = False
                i += 3
                continue
            i += 1
            continue
        if in_single:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_double = False
            i += 1
            continue

        if line.startswith("'''", i):
            in_triple_single = True
            i += 3
            continue
        if line.startswith('"""', i):
            in_triple_double = True
            i += 3
            continue
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == '"':
            in_double = True
            i += 1
            continue
        if ch == "#":
            return line[:i], line[i:]
        i += 1
    return line, None


def split_string_segments(text: str) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    i = 0
    start = 0
    while i < len(text):
        ch = text[i]
        if ch in ("'", '"'):
            if start < i:
                segments.append((text[start:i], False))
            quote = ch
            string_start = i
            i += 1
            escape = False
            while i < len(text):
                c = text[i]
                if escape:
                    escape = False
                elif c == "\\":
                    escape = True
                elif c == quote:
                    i += 1
                    break
                i += 1
            segments.append((text[string_start:i], True))
            start = i
            continue
        i += 1
    if start < len(text):
        segments.append((text[start:], False))
    return segments


def replace_outside_strings(text: str, replacements: Iterable[tuple[str, str]]) -> str:
    segments = split_string_segments(text)
    out: list[str] = []
    for segment, is_string in segments:
        if is_string:
            out.append(segment)
            continue
        updated = segment
        for pattern, repl in replacements:
            updated = re.sub(pattern, repl, updated)
        out.append(updated)
    return "".join(out)


def normalize_spacing(text: str) -> str:
    if not text:
        return ""
    replacements = [
        (r"\s*->\s*", " -> "),
        (r"\s*==\s*", " == "),
        (r"\s*!=\s*", " != "),
        (r"(?<![<>=!])\s*=\s*(?![=])", " = "),
        (r"\s*,\s*", ", "),
        (r"\(\s+", "("),
        (r"\s+\)", ")"),
        (r"\[\s+", "["),
        (r"\s+\]", "]"),
    ]
    text = replace_outside_strings(text, replacements)
    text = replace_outside_strings(text, [(r"\s+\(", "(")])
    text = replace_outside_strings(text, [(r"\s*\.\s*", ".")])
    text = replace_outside_strings(text, [(r"[ \t]+", " ")])
    return text.strip()


def normalize_expr_spacing(text: str) -> str:
    if not text:
        return ""
    replacements = [
        (r"\s*==\s*", " == "),
        (r"\s*!=\s*", " != "),
        (r"\s*,\s*", ", "),
        (r"\(\s+", "("),
        (r"\s+\)", ")"),
        (r"\[\s+", "["),
        (r"\s+\]", "]"),
    ]
    text = replace_outside_strings(text, replacements)
    text = replace_outside_strings(text, [(r"\s+\(", "(")])
    text = replace_outside_strings(text, [(r"[ \t]+", " ")])
    return text.strip()


def split_by_comma_respecting_parens(text: str) -> list[str]:
    parts: list[str] = []
    curr = ""
    depth = 0
    in_single = False
    in_double = False
    escape = False
    for ch in text:
        if in_single:
            curr += ch
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "'":
                in_single = False
            continue
        if in_double:
            curr += ch
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_double = False
            continue
        if ch == "'":
            in_single = True
            curr += ch
            continue
        if ch == '"':
            in_double = True
            curr += ch
            continue
        if ch == "(":
            depth += 1
            curr += ch
            continue
        if ch == ")":
            depth -= 1
            curr += ch
            continue
        if ch == "," and depth == 0:
            parts.append(curr)
            curr = ""
            continue
        curr += ch
    parts.append(curr)
    return [p for p in (part.strip() for part in parts) if p]


def format_columns_list(text: str) -> str:
    cols = [part.strip() for part in re.split(r"[\s,]+", text.strip()) if part.strip()]
    return ", ".join(cols)


def format_expr(text: str) -> str:
    return normalize_expr_spacing(text)


def format_assignments_list(text: str) -> str:
    parts = split_by_comma_respecting_parens(text)
    formatted: list[str] = []
    for part in parts:
        part = part.strip()
        allow_overwrite = False
        if part.lower().startswith("update!"):
            allow_overwrite = True
            part = part[7:].strip()
        match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)", part)
        if not match:
            formatted.append(normalize_spacing(part))
            continue
        name = match.group(1)
        expr = format_expr(match.group(2).strip())
        prefix = "update! " if allow_overwrite else ""
        formatted.append(f"{prefix}{name} = {expr}")
    return ", ".join(formatted)


def format_rename_mappings(text: str) -> str:
    parts = split_by_comma_respecting_parens(text)
    formatted: list[str] = []
    for part in parts:
        part = part.strip()
        if "->" not in part:
            formatted.append(normalize_spacing(part))
            continue
        old, new = part.split("->", 1)
        formatted.append(f"{old.strip()} -> {new.strip()}")
    return ", ".join(formatted)


def format_cast_specs(text: str) -> str:
    parts = split_by_comma_respecting_parens(text)
    formatted: list[str] = []
    for part in parts:
        part = part.strip()
        if "->" not in part:
            formatted.append(normalize_spacing(part))
            continue
        left, right = part.split("->", 1)
        formatted.append(f"{left.strip()} -> {normalize_spacing(right.strip())}")
    return ", ".join(formatted)


def _match_call_start(text: str, keyword: str) -> Optional[int]:
    match = re.match(rf"(?i)^{keyword}\s*\(", text)
    if not match:
        return None
    return text.find("(", match.start())


def extract_balanced(text: str, start_idx: int = 0, open_char: str = "(", close_char: str = ")") -> tuple[str, str]:
    if start_idx >= len(text) or text[start_idx] != open_char:
        raise ValueError("Expected opening delimiter for balanced parse.")
    depth = 0
    i = start_idx
    in_single = False
    in_double = False
    escape = False
    while i < len(text):
        ch = text[i]
        if in_single:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_double = False
            i += 1
            continue
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == '"':
            in_double = True
            i += 1
            continue
        if ch == open_char:
            depth += 1
        elif ch == close_char:
            depth -= 1
            if depth == 0:
                inner = text[start_idx + 1 : i]
                remainder = text[i + 1 :]
                return inner, remainder
        i += 1
    raise ValueError("Unbalanced delimiters in text.")


def format_builder_args(method: str, text: str) -> str:
    method_lower = method.lower()
    if method_lower in ("by", "class", "var", "stats"):
        return format_columns_list(text)
    if method_lower == "nodupkey":
        return text.strip().lower()
    return normalize_spacing(text)


def format_primary_expr(text: str) -> tuple[str, str]:
    stripped = text.strip()
    if not stripped:
        return "", ""

    idx = _match_call_start(stripped, "from")
    if idx is not None:
        inner, remainder = extract_balanced(stripped, idx)
        source = inner.strip()
        primary = f"from({source})"
        return primary, remainder

    for kind in ("sort", "aggregate", "summary"):
        idx = _match_call_start(stripped, kind)
        if idx is not None:
            inner, remainder = extract_balanced(stripped, idx)
            inner_fmt = format_table_expr_inline(inner)
            primary = f"{kind}({inner_fmt})"
            primary, remainder = format_builder_chain(primary, remainder)
            return primary, remainder

    match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", stripped)
    if not match:
        return normalize_spacing(stripped), ""
    name = match.group(1)
    remainder = stripped[match.end():]
    return name, remainder


def format_builder_chain(primary: str, remainder: str) -> tuple[str, str]:
    rem = remainder
    while True:
        rem = rem.lstrip()
        if not rem.startswith("."):
            break
        match = re.match(r"\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(", rem)
        if not match:
            break
        method = match.group(1).lower()
        paren_idx = rem.find("(", match.start())
        inner, rest = extract_balanced(rem, paren_idx)
        args_fmt = format_builder_args(method, inner)
        primary += f".{method}({args_fmt})"
        rem = rest
    return primary, rem


def find_next_postfix_start(text: str) -> Optional[int]:
    depth = 0
    in_single = False
    in_double = False
    escape = False
    for i, ch in enumerate(text):
        if in_single:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "'":
                in_single = False
            continue
        if in_double:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_double = False
            continue
        if ch == "'":
            in_single = True
            continue
        if ch == '"':
            in_double = True
            continue
        if ch == "(" or ch == "[":
            depth += 1
            continue
        if ch == ")" or ch == "]":
            depth = max(depth - 1, 0)
            continue
        if depth == 0 and ch.isspace():
            for kw in POSTFIX_KEYWORDS:
                if text[i + 1 :].lower().startswith(kw):
                    after = i + 1 + len(kw)
                    if after >= len(text) or text[after].isspace() or text[after] == "(":
                        return i + 1
    return None


def format_postfix_args(keyword: str, args: str) -> str:
    kw = keyword.lower()
    if kw in ("select", "drop"):
        return format_columns_list(args)
    if kw == "filter":
        return format_expr(args)
    if kw in ("derive", "update!"):
        return format_assignments_list(args)
    if kw == "rename":
        return format_rename_mappings(args)
    if kw == "cast":
        return format_cast_specs(args)
    return normalize_spacing(args)


def format_table_expr_inline(text: str) -> str:
    primary, remainder = format_primary_expr(text)
    rem = remainder.strip()
    while rem:
        lower_rem = rem.lower()
        if lower_rem.startswith("update!") and (len(rem) == 7 or rem[7].isspace() or rem[7] == "("):
            kw = "update!"
            rest = rem[7:].lstrip()
        else:
            match = re.match(r"(?i)^(select|filter|derive|rename|drop|cast)\b", rem)
            if not match:
                if rem:
                    primary = normalize_spacing(f"{primary} {rem}")
                return primary
            kw = match.group(1).lower()
            rest = rem[match.end():].lstrip()
        args = ""
        paren = False
        if rest.startswith("("):
            inner, tail = extract_balanced(rest, 0)
            args = inner
            paren = True
            rem = tail.strip()
        else:
            next_idx = find_next_postfix_start(rest)
            if next_idx is None:
                args = rest
                rem = ""
            else:
                args = rest[:next_idx]
                rem = rest[next_idx:].strip()
        formatted_args = format_postfix_args(kw, args)
        if kw in ("select", "drop"):
            primary = f"{primary} {kw} {formatted_args}"
        else:
            if paren:
                primary = f"{primary} {kw}({formatted_args})"
            else:
                primary = f"{primary} {kw} {formatted_args}"
    return primary


def format_pipeline_line(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return ""
    if stripped.lower().startswith("update!") and (len(stripped) == 7 or stripped[7].isspace() or stripped[7] == "("):
        kw = "update!"
        rest = stripped[7:].strip()
        if not rest:
            return kw
        if rest.lower() == "do":
            return f"{kw} do"
        if rest.lower().endswith(" do"):
            prefix = rest[:-3].strip()
            formatted_prefix = format_postfix_args(kw, prefix) if prefix else ""
            return f"{kw} {formatted_prefix} do".strip()
        if rest.startswith("("):
            inner, tail = extract_balanced(rest, 0)
            formatted_args = format_postfix_args(kw, inner)
            if tail.strip():
                tail_fmt = normalize_spacing(tail.strip())
                return f"{kw}({formatted_args}) {tail_fmt}".strip()
            return f"{kw}({formatted_args})"
        formatted_args = format_postfix_args(kw, rest)
        return f"{kw} {formatted_args}"
    match = re.match(r"(?i)^(select|drop)\s+(.+)$", stripped)
    if match:
        kw = match.group(1).lower()
        args = format_columns_list(match.group(2))
        return f"{kw} {args}"

    match = re.match(r"(?i)^(filter|derive|rename|cast)\b(.*)$", stripped)
    if not match:
        return normalize_spacing(stripped)
    kw = match.group(1).lower()
    rest = match.group(2).strip()
    if not rest:
        return kw
    if rest.lower() == "do":
        return f"{kw} do"
    if rest.lower().endswith(" do"):
        prefix = rest[:-3].strip()
        formatted_prefix = format_postfix_args(kw, prefix) if prefix else ""
        return f"{kw} {formatted_prefix} do".strip()
    if rest.startswith("("):
        inner, tail = extract_balanced(rest, 0)
        formatted_args = format_postfix_args(kw, inner)
        if tail.strip():
            tail_fmt = normalize_spacing(tail.strip())
            return f"{kw}({formatted_args}) {tail_fmt}".strip()
        return f"{kw}({formatted_args})"
    formatted_args = format_postfix_args(kw, rest)
    return f"{kw} {formatted_args}"


def format_const_inline(text: str) -> str:
    match = re.match(r"(?i)^const\s*\{(.*)\}\s*$", text)
    if not match:
        return normalize_spacing(text)
    inner = match.group(1).strip()
    if not inner:
        return "const { }"
    parts = split_by_comma_respecting_parens(inner)
    formatted_parts: list[str] = []
    for part in parts:
        if "=" not in part:
            formatted_parts.append(normalize_spacing(part))
            continue
        name, _, value = part.partition("=")
        formatted_parts.append(f"{name.strip()} = {value.strip()}")
    return f"const {{ {', '.join(formatted_parts)} }}"


def format_const_entry_line(text: str) -> str:
    stripped = text.strip()
    trailing_comma = stripped.endswith(",")
    core = stripped[:-1].rstrip() if trailing_comma else stripped
    if "=" not in core:
        formatted = normalize_spacing(core)
        return f"{formatted}," if trailing_comma else formatted
    name, _, value = core.partition("=")
    formatted = f"{name.strip()} = {value.strip()}"
    return f"{formatted}," if trailing_comma else formatted


def format_datasource_rhs(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return ""
    lower = stripped.lower()
    if lower.startswith("csv"):
        idx = _match_call_start(stripped, "csv")
        if idx is None:
            return normalize_spacing(stripped)
        inner, _ = extract_balanced(stripped, idx)
        args = split_by_comma_respecting_parens(inner)
        if not args:
            return "csv()"
        parts = [args[0].strip()]
        if len(args) > 1:
            m = re.match(r"(?i)^columns\s*\((.*)\)\s*$", args[1].strip())
            if m:
                cols = format_columns_list(m.group(1))
                parts.append(f"columns({cols})")
            else:
                parts.append(normalize_spacing(args[1]))
        return f"csv({', '.join(parts)})"
    if lower.startswith("inline_csv("):
        idx = _match_call_start(stripped, "inline_csv")
        if idx is None:
            return normalize_spacing(stripped)
        inner, _ = extract_balanced(stripped, idx)
        args = split_by_comma_respecting_parens(inner)
        if not args:
            return "inline_csv()"
        parts = [args[0].strip()]
        if len(args) > 1:
            m = re.match(r"(?i)^columns\s*\((.*)\)\s*$", args[1].strip())
            if m:
                cols = format_columns_list(m.group(1))
                parts.append(f"columns({cols})")
            else:
                parts.append(normalize_spacing(args[1]))
        return f"inline_csv({', '.join(parts)})"
    inline_match = re.match(r"(?i)^inline_csv(\s+columns\s*\(([^)]*)\))?\s+do$", stripped)
    if inline_match:
        cols = inline_match.group(2)
        if cols is not None:
            cols_fmt = format_columns_list(cols)
            return f"inline_csv columns({cols_fmt}) do"
        return "inline_csv do"
    return normalize_spacing(stripped)


def format_save_line(text: str) -> str:
    match = re.match(r"(?i)^save\s+([A-Za-z_][A-Za-z0-9_]*)\s+to\s+(.+)$", text.strip())
    if not match:
        return normalize_spacing(text)
    table = match.group(1)
    rest = match.group(2).strip()
    path_lit, remaining = extract_quoted_literal(rest)
    if not path_lit:
        return normalize_spacing(text)
    remaining = remaining.strip()
    if remaining:
        m_as = re.match(r"(?i)^as\s+(.+)$", remaining)
        if m_as:
            name_lit, _ = extract_quoted_literal(m_as.group(1).strip())
            if name_lit:
                return f"save {table} to {path_lit} as {name_lit}"
    return f"save {table} to {path_lit}"


def extract_quoted_literal(text: str) -> tuple[str, str]:
    i = 0
    while i < len(text):
        ch = text[i]
        if ch in ("'", '"'):
            quote = ch
            start = i
            i += 1
            escape = False
            while i < len(text):
                c = text[i]
                if escape:
                    escape = False
                elif c == "\\":
                    escape = True
                elif c == quote:
                    i += 1
                    return text[start:i], text[i:]
                i += 1
            return "", text
        i += 1
    return "", text


def format_assert_line(text: str) -> str:
    match = re.match(r"(?i)^assert\s+(.+)$", text.strip())
    if not match:
        return normalize_spacing(text)
    predicate = format_expr(match.group(1))
    return f"assert {predicate}"


def format_table_expr_line(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return ""
    lower = stripped.lower()
    if lower.endswith(" do"):
        base = stripped[:-3].rstrip()
        return f"{format_table_expr_inline(base)} do"
    return format_table_expr_inline(stripped)


def format_line_code(code: str) -> str:
    stripped = code.strip()
    if not stripped:
        return ""
    lower = stripped.lower()
    if lower == "end":
        return "end"
    if lower == "do":
        return "do"

    match = re.match(r"(?i)^let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$", stripped)
    if match:
        name = match.group(1)
        expr = format_expr(match.group(2))
        return f"let {name} = {expr}"

    if re.match(r"(?i)^const\b", stripped):
        return format_const_inline(stripped)

    match = re.match(r"(?i)^datasource\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", stripped)
    if match:
        name = match.group(1)
        rhs = match.group(2).strip()
        if rhs:
            return f"datasource {name} = {format_datasource_rhs(rhs)}"
        return f"datasource {name} ="

    match = re.match(r"(?i)^table\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", stripped)
    if match:
        name = match.group(1)
        rhs = match.group(2).strip()
        if rhs:
            return f"table {name} = {format_table_expr_line(rhs)}"
        return f"table {name} ="

    if stripped.lower().startswith("save "):
        return format_save_line(stripped)

    if stripped.lower().startswith("assert "):
        return format_assert_line(stripped)

    if stripped.lower().startswith("update!") and (len(stripped) == 7 or stripped[7].isspace() or stripped[7] == "("):
        return format_pipeline_line(stripped)
    match = re.match(r"(?i)^(select|filter|derive|rename|drop|cast)\b", stripped)
    if match:
        return format_pipeline_line(stripped)

    if stripped.lower().startswith("inline_csv"):
        return format_datasource_rhs(stripped)

    return format_table_expr_line(stripped)


@dataclass
class _Block:
    kind: str
    indent: bool


def _is_inline_csv_opener(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    if re.match(r"(?i)^inline_csv(\s+columns\s*\([^)]*\))?\s+do$", stripped):
        return True
    match = re.match(r"(?i)^datasource\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*(.+)$", stripped)
    if match:
        rhs = match.group(1).strip()
        return re.match(r"(?i)^inline_csv(\s+columns\s*\([^)]*\))?\s+do$", rhs) is not None
    return False


def format_canonical(text: str) -> str:
    normalized = normalize_newlines(text)
    lines = normalized.split("\n")
    out: list[str] = []
    block_stack: list[_Block] = []
    indent_level = 0
    pending_blank = False
    saw_content = False

    def push_block(kind: str, indent: bool) -> None:
        nonlocal indent_level
        block_stack.append(_Block(kind, indent))
        if indent:
            indent_level += 1

    def pop_block(kind: Optional[str] = None) -> Optional[_Block]:
        nonlocal indent_level
        if not block_stack:
            return None
        blk = block_stack.pop()
        if blk.indent:
            indent_level = max(indent_level - 1, 0)
        return blk

    for raw_line in lines:
        # Inline CSV blocks: preserve raw lines.
        if block_stack and block_stack[-1].kind == "inline_csv":
            code_inline, comment_inline = split_inline_comment(raw_line)
            if code_inline.strip().lower() == "end":
                if pending_blank:
                    out.append("")
                    pending_blank = False
                indent = "\t" * indent_level
                line = "end"
                if comment_inline:
                    line = f"{line}  {comment_inline}"
                out.append(f"{indent}{line}")
                pop_block()
                saw_content = True
            else:
                out.append(raw_line)
            continue

        code, comment = split_inline_comment(raw_line)
        code_stripped = code.strip()

        if not code_stripped and comment is None:
            if not saw_content:
                continue
            if pending_blank:
                continue
            pending_blank = True
            continue

        if not code_stripped and comment is not None:
            if pending_blank:
                out.append("")
                pending_blank = False
            indent = "\t" * indent_level
            out.append(f"{indent}{comment}")
            saw_content = True
            continue

        if pending_blank:
            out.append("")
            pending_blank = False

        code_lower = code_stripped.rstrip().lower()
        is_const_end = block_stack and block_stack[-1].kind == "const" and code_lower.startswith("}")
        if is_const_end:
            pop_block()
            indent = "\t" * indent_level
            formatted = "}"
            if comment:
                formatted = f"{formatted}  {comment}"
            out.append(f"{indent}{formatted}")
            saw_content = True
            continue

        if block_stack and block_stack[-1].kind == "const":
            formatted_code = format_const_entry_line(code_stripped)
            indent = "\t" * indent_level
            if comment:
                formatted_code = f"{formatted_code}  {comment}"
            out.append(f"{indent}{formatted_code}")
            saw_content = True
            continue

        if code_lower == "end":
            pop_block()
            indent = "\t" * indent_level
            formatted_code = "end"
            if comment:
                formatted_code = f"{formatted_code}  {comment}"
            out.append(f"{indent}{formatted_code}")
            saw_content = True
            continue

        formatted_code = format_line_code(code_stripped)

        indent = "\t" * indent_level
        if comment:
            formatted_code = f"{formatted_code}  {comment}"
        out.append(f"{indent}{formatted_code}")
        saw_content = True

        # Block openers
        if formatted_code.lower().startswith("const {") and "}" not in formatted_code:
            push_block("const", True)
            continue

        if _is_inline_csv_opener(code_stripped):
            push_block("inline_csv", False)
            continue

        if formatted_code.lower().endswith(" do") or formatted_code.lower() == "do":
            push_block("do", True)

    return "\n".join(out).rstrip("\n") + ("\n" if out else "")


def format_text(
    text: str,
    *,
    mode: str = "canonical",
    style: str = FMT_STYLE_ID,
    file_name: str = "<string>",
) -> str:
    normalized = normalize_newlines(text)
    # Validate via parser (no semantic expansion).
    parse_sans_script(normalized, file_name)
    if style != FMT_STYLE_ID:
        raise ValueError(f"Unsupported fmt style '{style}'. Expected '{FMT_STYLE_ID}'.")
    if mode == "identity":
        return normalized
    if mode != "canonical":
        raise ValueError(f"Unknown formatting mode '{mode}'")
    return format_canonical(normalized)
