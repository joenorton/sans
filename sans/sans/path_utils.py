from __future__ import annotations

import posixpath
from pathlib import Path


def fs_path_from_report(path_val: str | None) -> Path:
    """Normalize a report path for filesystem access across platforms."""
    if not path_val:
        return Path("")
    s = str(path_val).replace("\\", "/")
    s = posixpath.normpath(s)
    if s == ".":
        s = ""
    return Path(s)
