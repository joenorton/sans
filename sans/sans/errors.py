# sans/sans/errors.py
from __future__ import annotations
from ._loc import Loc

class SansError(Exception):
    """Base class for all errors in the sans application."""
    def __init__(self, message: str, loc: Loc | None = None):
        super().__init__(message)
        self.loc = loc

    def __str__(self) -> str:
        if self.loc:
            return f"{super().__str__()} [{self.loc}]"
        return super().__str__()

class SansParsingError(SansError):
    """An error that occurs during statement splitting or block segmentation."""
    pass
