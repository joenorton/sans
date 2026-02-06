"""
Schema lock v0: read/write schema.lock.json for typed CSV ingestion.
Deterministic JSON; column order preserved; paths normalized to forward slashes.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .types import Type, type_name

SCHEMA_LOCK_VERSION = 1


def _git_sha() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            cwd=Path(__file__).resolve().parent.parent.parent,
        )
        if out.returncode == 0 and out.stdout:
            return out.stdout.strip()[:40]
    except Exception:
        pass
    return ""


def load_schema_lock(path: Path) -> Dict[str, Any]:
    """Load and validate schema.lock.json. Returns the raw dict."""
    data = json.loads(path.read_text(encoding="utf-8"))
    version = data.get("schema_lock_version")
    if version != 1:
        raise ValueError(f"Unsupported schema_lock_version: {version}")
    return data


def lock_entry_to_column_types(entry: Dict[str, Any]) -> Dict[str, Type]:
    """Convert a lock datasource entry to name -> Type for runtime."""
    columns = entry.get("columns") or []
    result: Dict[str, Type] = {}
    for col in columns:
        name = col.get("name")
        typ = (col.get("type") or "unknown").strip().lower()
        if name is not None:
            from .types import TYPE_NAME_MAP
            result[name] = TYPE_NAME_MAP.get(typ, Type.UNKNOWN)
    return result


def lock_entry_required_columns(entry: Dict[str, Any]) -> List[str]:
    """Return ordered list of column names required by this lock entry."""
    columns = entry.get("columns") or []
    return [c["name"] for c in columns if c.get("name") is not None]


def lock_by_name(lock_dict: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return map datasource name -> lock entry for enforcement."""
    by_name: Dict[str, Dict[str, Any]] = {}
    for entry in lock_dict.get("datasources") or []:
        name = entry.get("name")
        if name is not None:
            by_name[name] = entry
    return by_name


def build_schema_lock(
    irdoc: Any,
    referenced_names: Set[str],
    schema_lock_used: Optional[Dict[str, Any]] = None,
    sans_version: str = "",
) -> Dict[str, Any]:
    """
    Build schema lock payload for referenced CSV/inline_csv datasources.
    Types: from irdoc.column_types if present; else from schema_lock_used entry.
    Path: from irdoc.datasources[name].path, normalized to /.
    """
    from .ir import OpStep

    lock_entries = lock_by_name(schema_lock_used) if schema_lock_used else {}
    datasources_list: List[Dict[str, Any]] = []

    for name in sorted(referenced_names):
        ds = irdoc.datasources.get(name) if irdoc.datasources else None
        if not ds:
            continue
        if ds.kind not in ("csv", "inline_csv"):
            continue
        path_str = (ds.path or "").replace("\\", "/") or f"{name}.csv"
        columns_order: List[str] = []
        column_types: Dict[str, str] = {}

        if ds.column_types:
            columns_order = list(ds.columns) if ds.columns else sorted(ds.column_types.keys())
            column_types = {c: type_name(ds.column_types[c]) for c in columns_order if c in ds.column_types}
            for c in columns_order:
                if c not in column_types:
                    column_types[c] = "unknown"
        elif name in lock_entries:
            entry = lock_entries[name]
            columns_order = lock_entry_required_columns(entry)
            for col in entry.get("columns") or []:
                n = col.get("name")
                t = col.get("type", "unknown")
                if n:
                    column_types[n] = str(t).lower()
        else:
            if ds.columns:
                columns_order = list(ds.columns)
                column_types = {c: "unknown" for c in columns_order}

        if not columns_order:
            continue

        columns_payload = [{"name": c, "type": column_types.get(c, "unknown")} for c in columns_order]
        datasources_list.append({
            "columns": columns_payload,
            "kind": ds.kind,
            "name": name,
            "path": path_str,
            "rules": {"extra_columns": "ignore", "missing_columns": "error"},
        })

    created_by: Dict[str, str] = {"sans_version": sans_version, "git_sha": _git_sha()}
    return {
        "created_by": created_by,
        "datasources": datasources_list,
        "schema_lock_version": SCHEMA_LOCK_VERSION,
    }


def _canonical_lock_json(lock_dict: Dict[str, Any]) -> str:
    """Canonical JSON for hashing: sort top-level keys; datasources list order preserved; each entry sorted by key."""
    created_by = lock_dict.get("created_by") or {}
    created_by = dict(sorted(created_by.items()))
    datasources = lock_dict.get("datasources") or []
    out_entries = []
    for entry in datasources:
        # Preserve column order; sort other keys
        columns = entry.get("columns") or []
        rules = entry.get("rules") or {}
        rules = dict(sorted(rules.items()))
        out_entries.append({
            "columns": columns,
            "kind": entry.get("kind", "csv"),
            "name": entry.get("name", ""),
            "path": entry.get("path", ""),
            "rules": rules,
        })
    payload = {
        "created_by": created_by,
        "datasources": out_entries,
        "schema_lock_version": lock_dict.get("schema_lock_version", 1),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_lock_sha256(lock_dict: Dict[str, Any]) -> str:
    """SHA-256 of canonical lock JSON."""
    canonical = _canonical_lock_json(lock_dict)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def write_schema_lock(lock_dict: Dict[str, Any], path: Path) -> None:
    """Write deterministic schema.lock.json. Sorted keys; datasources sorted by name; column order preserved."""
    path.parent.mkdir(parents=True, exist_ok=True)
    canonical = _canonical_lock_json(lock_dict)
    path.write_text(canonical, encoding="utf-8")
