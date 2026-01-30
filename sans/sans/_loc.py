# sans/sans/_loc.py
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class Loc:
    """A location in a source file."""
    file: str
    line_start: int
    line_end: int

    def __str__(self) -> str:
        if self.line_start == self.line_end:
            return f"{self.file}:{self.line_start}"
        return f"{self.file}:{self.line_start}-{self.line_end}"

    def merge(self, other: Loc) -> Loc:
        """Merges two locations into a single span."""
        if self.file != other.file:
            # This would be a weird error, but let's be safe
            raise ValueError("Cannot merge locations from different files")
        return Loc(
            file=self.file,
            line_start=min(self.line_start, other.line_start),
            line_end=max(self.line_end, other.line_end),
        )
