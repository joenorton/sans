from __future__ import annotations

from dataclasses import dataclass


def _normalize_code(code: str) -> str:
    return code if code.startswith("E_") else f"E_{code}"


@dataclass
class SansScriptError(Exception):
    code: str
    message: str
    line: int
    hint: str | None = None

    def __post_init__(self):
        self.code = _normalize_code(self.code)

    def __str__(self) -> str:
        hint = f" Hint: {self.hint}" if self.hint else ""
        return f"{self.code} at line {self.line}: {self.message}{hint}"
