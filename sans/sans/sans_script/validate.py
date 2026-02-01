from __future__ import annotations

from .errors import SansScriptError


def ensure_true(condition: bool, code: str, message: str, line: int, hint: str | None = None) -> None:
    if not condition:
        raise SansScriptError(code=code, message=message, line=line, hint=hint)


def ensure_single_statement(count: int, code: str, message: str, line: int) -> None:
    if count != 1:
        raise SansScriptError(code=code, message=message, line=line)
