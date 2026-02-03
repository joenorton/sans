"""
Bundle layout and path canonicalization for SANS report/bundle contract.
All paths in report and evidence are bundle-relative, forward slashes only.
Paths outside the bundle are never stored; callers must error if a path would be outside.
"""
from __future__ import annotations

from pathlib import Path

# Subdir names under out_dir (bundle root)
INPUTS_SOURCE = "inputs/source"
INPUTS_DATA = "inputs/data"
ARTIFACTS = "artifacts"
OUTPUTS = "outputs"


def ensure_bundle_layout(out_dir: Path) -> None:
    """Create standard bundle directory structure under out_dir."""
    out_dir = Path(out_dir).resolve()
    (out_dir / INPUTS_SOURCE).mkdir(parents=True, exist_ok=True)
    (out_dir / INPUTS_DATA).mkdir(parents=True, exist_ok=True)
    (out_dir / ARTIFACTS).mkdir(parents=True, exist_ok=True)
    (out_dir / OUTPUTS).mkdir(parents=True, exist_ok=True)


def bundle_relative_path(physical_path: Path, bundle_root: Path) -> str:
    """
    Return bundle-relative path with forward slashes only.
    Raises ValueError if physical_path is not under bundle_root.
    Report and evidence must never contain paths outside the bundle.
    """
    physical_path = Path(physical_path).resolve()
    bundle_root = Path(bundle_root).resolve()
    try:
        rel = physical_path.relative_to(bundle_root)
    except ValueError:
        raise ValueError(
            f"Path is outside bundle: {physical_path} (bundle_root={bundle_root})"
        ) from None
    return rel.as_posix()


def validate_save_path_under_outputs(path: str, outputs_base: Path, bundle_root: Path) -> Path:
    """
    Resolve save-step path under outputs/. Allow subpaths; forbid absolute and .. traversal.
    Returns resolved Path under outputs_base. Raises ValueError if path would escape.
    """
    path = path.strip() if path else ""
    if not path:
        raise ValueError("Save path cannot be empty")
    # Forbid absolute
    if Path(path).is_absolute():
        raise ValueError(f"Save path must not be absolute: {path}")
    # Forbid .. traversal
    parts = Path(path).parts
    if ".." in parts:
        raise ValueError(f"Save path must not contain ..: {path}")
    resolved = (outputs_base / path).resolve()
    bundle_root = Path(bundle_root).resolve()
    try:
        resolved.relative_to(bundle_root)
    except ValueError:
        raise ValueError(
            f"Save path would escape bundle: {path} (resolved={resolved})"
        ) from None
    # Must be under outputs_base
    try:
        resolved.relative_to(Path(outputs_base).resolve())
    except ValueError:
        raise ValueError(
            f"Save path would escape outputs/: {path} (resolved={resolved})"
        ) from None
    return resolved
