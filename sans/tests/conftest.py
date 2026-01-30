from __future__ import annotations

from pathlib import Path
from uuid import uuid4
import pytest


@pytest.fixture
def tmp_path():
    base = Path(__file__).resolve().parent / ".tmp_pytest"
    base.mkdir(parents=True, exist_ok=True)
    path = base / uuid4().hex
    path.mkdir(parents=True, exist_ok=True)
    return path
