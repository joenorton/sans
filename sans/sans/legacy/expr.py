from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Tuple

from sans.expr import ExprNode
from sans.parser_expr import parse_expression_from_string


@dataclass
class LegacyExprError(ValueError):
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


_LEGACY_WORD_OPS = {
    "eq": "==",
    "ne": "!=",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
}

_WORD_OP_RE = re.compile(r"\b(eq|ne|lt|le|gt|ge)\b", re.IGNORECASE)
_SINGLE_EQ_RE = re.compile(r"(?<![<>=!^~])=(?![=])")
_UNSUPPORTED_OP_RE = re.compile(r"<>")


def _split_string_segments(text: str) -> List[Tuple[str, bool]]:
    segments: List[Tuple[str, bool]] = []
    buf: List[str] = []
    in_single = False
    in_double = False
    escape = False

    def flush(is_string: bool) -> None:
        if buf:
            segments.append(("".join(buf), is_string))
            buf.clear()

    for ch in text:
        if in_single:
            buf.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "'":
                in_single = False
                flush(True)
            continue
        if in_double:
            buf.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_double = False
                flush(True)
            continue

        if ch == "'":
            flush(False)
            in_single = True
            buf.append(ch)
            continue
        if ch == '"':
            flush(False)
            in_double = True
            buf.append(ch)
            continue

        buf.append(ch)

    if in_single or in_double:
        raise LegacyExprError(
            code="E_LEGACY_EXPR",
            message="Unterminated string literal in legacy expression.",
        )

    flush(False)
    return segments


def _replace_word_ops(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        return _LEGACY_WORD_OPS[match.group(1).lower()]

    return _WORD_OP_RE.sub(repl, text)


def _translate_segment(text: str) -> str:
    # Reject unsupported legacy operator tokens early.
    if _UNSUPPORTED_OP_RE.search(text):
        raise LegacyExprError(
            code="E_LEGACY_EXPR",
            message="Unsupported legacy operator '<>' in expression.",
        )

    translated = _replace_word_ops(text)
    translated = translated.replace("^=", "!=").replace("~=", "!=")
    translated = _SINGLE_EQ_RE.sub("==", translated)
    return translated


def find_legacy_tokens(text: str) -> List[str]:
    tokens: List[str] = []
    for segment, is_string in _split_string_segments(text):
        if is_string:
            continue
        tokens.extend(m.group(1).lower() for m in _WORD_OP_RE.finditer(segment))
        tokens.extend(m.group(0) for m in re.finditer(r"\^=|~=", segment))
        tokens.extend(m.group(0) for m in _SINGLE_EQ_RE.finditer(segment))
        tokens.extend(m.group(0) for m in _UNSUPPORTED_OP_RE.finditer(segment))
    return tokens


def translate_legacy_predicate(
    text: str,
    file_name: str = "<string>",
    *,
    validate: bool = True,
) -> str:
    try:
        segments = _split_string_segments(text)
    except LegacyExprError:
        raise
    except Exception as exc:  # defensive
        raise LegacyExprError(
            code="E_LEGACY_EXPR",
            message=f"Failed to scan legacy expression: {exc}",
        ) from exc

    out_parts: List[str] = []
    for segment, is_string in segments:
        if is_string:
            out_parts.append(segment)
            continue
        out_parts.append(_translate_segment(segment))

    translated = "".join(out_parts)

    # Ensure no legacy tokens remain outside strings.
    remaining = find_legacy_tokens(translated)
    if remaining:
        tokens = ", ".join(sorted(set(remaining)))
        raise LegacyExprError(
            code="E_LEGACY_EXPR",
            message=f"Unsupported legacy tokens in expression: {tokens}",
        )

    if validate:
        try:
            parse_expression_from_string(translated, file_name)
        except ValueError as exc:
            raise LegacyExprError(
                code="E_LEGACY_EXPR",
                message=f"Malformed legacy expression: {exc}",
            ) from exc

    return translated


def parse_legacy_predicate(text: str, file_name: str = "<string>") -> ExprNode:
    translated = translate_legacy_predicate(text, file_name, validate=False)
    try:
        return parse_expression_from_string(translated, file_name)
    except ValueError as exc:
        raise LegacyExprError(
            code="E_LEGACY_EXPR",
            message=f"Malformed legacy expression: {exc}",
        ) from exc
