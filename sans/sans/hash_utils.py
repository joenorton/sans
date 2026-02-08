import hashlib
import json
import copy
import csv
import posixpath
import re
from pathlib import Path
from typing import Any, Dict, Optional

def _sha256_text(text: str) -> str:
    # Normalize line endings to \n before hashing
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _canonicalize_csv(path: Path) -> bytes:
    """
    Reads a CSV and returns a canonical byte representation for hashing.
    Canonicalization rules:
    - Decode as UTF-8
    - Parse using csv.reader
    - Re-serialize using csv.writer with lineterminator='\n'
    - Encode as UTF-8
    """
    # Read and parse
    rows = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(list(row))

    # Re-serialize to memory
    import io
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerows(rows)
    
    return output.getvalue().encode("utf-8")

def _canonicalize_text(path: Path) -> bytes:
    """
    Reads a text file and returns canonical bytes (UTF-8, LF only).
    """
    text = path.read_text(encoding="utf-8")
    # Normalize line endings to \n
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text.encode("utf-8")

def compute_canonical_json_sha256(path: Path) -> Optional[str]:
    """
    Compute SHA-256 for a JSON artifact using canonical JSON serialization:
    - parse as UTF-8 JSON
    - json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    - UTF-8 encode
    """
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def compute_raw_hash(path: Path) -> Optional[str]:
    """Computes the raw SHA-256 hash of a file's bytes."""
    if not path.exists():
        return None
    try:
        data = path.read_bytes()
        return hashlib.sha256(data).hexdigest()
    except OSError:
        return None

def compute_input_hash(path: Path) -> Optional[str]:
    """
    Compute SHA-256 for report inputs.
    - JSON: canonical JSON hash (v0.3 contract)
    - Non-JSON: raw bytes (no canonicalization)
    """
    if not path.exists():
        return None
    if path.suffix.lower() == ".json":
        try:
            canonical_hash = compute_canonical_json_sha256(path)
            if canonical_hash:
                return canonical_hash
        except Exception:
            pass
    return compute_raw_hash(path)

def compute_artifact_hash(path: Path) -> Optional[str]:
    """
    Computes a deterministic hash of a file.
    - If CSV: canonicalize content then hash.
    - If JSON: canonicalize JSON (sort_keys=True, separators=(",", ":"), ensure_ascii=False) then hash.
    - If text (sas, txt...): canonicalize line endings.
    - If other: hash raw bytes.
    """
    if not path.exists():
        return None
    
    suffix = path.suffix.lower()
    
    if suffix == ".csv":
        try:
            data = _canonicalize_csv(path)
            return hashlib.sha256(data).hexdigest()
        except Exception:
            # Fallback to text canonicalization if CSV parsing fails
            pass

    if suffix == ".json":
        try:
            canonical_hash = compute_canonical_json_sha256(path)
            if canonical_hash:
                return canonical_hash
        except Exception:
            # Fallback to text or raw bytes
            pass
            
    if suffix in {".sas", ".txt", ".md", ".toml", ".yaml", ".yml"}:
        try:
            data = _canonicalize_text(path)
            return hashlib.sha256(data).hexdigest()
        except Exception:
             # Fallback to raw bytes
            pass
            
    try:
        data = path.read_bytes()
        return hashlib.sha256(data).hexdigest()
    except OSError:
        return None


_DRIVE_RE = re.compile(r"^([A-Za-z]):(.*)$")
_SELF_HASH_KEYS = {"report_sha256", "report_hash"}
# Diagnostic-only: host-specific paths; excluded from canonical payload so report_sha256 is stable across machines
_DIAGNOSTIC_ONLY_KEYS = {"schema_lock_used_path", "schema_lock_emit_path"}
_PATH_LIST_KEYS = {"inputs", "artifacts", "outputs", "datasource_inputs"}
_PATH_KEY_NAMES = {"path", "plan_path", "file"}


def _normalize_path_string(path_val: str) -> str:
    """Normalize a path string independent of host OS."""
    if path_val is None:
        return ""
    s = str(path_val)
    if not s:
        return s
    s = s.replace("\\", "/")

    drive = ""
    rest = s
    m = _DRIVE_RE.match(s)
    if m:
        drive = m.group(1).lower() + ":"
        rest = m.group(2)

    is_abs = False
    if drive:
        if rest.startswith("/"):
            is_abs = True
    else:
        if rest.startswith("/") or rest.startswith("//"):
            is_abs = True

    if rest == "":
        norm_rest = ""
    else:
        norm_rest = posixpath.normpath(rest)
        if norm_rest == ".":
            norm_rest = ""

    if drive:
        if is_abs:
            if norm_rest == "":
                norm_rest = "/"
            elif not norm_rest.startswith("/"):
                norm_rest = "/" + norm_rest
            return drive + norm_rest
        return drive + norm_rest

    if is_abs and norm_rest == "":
        return "/"
    if is_abs and not norm_rest.startswith("/"):
        return "/" + norm_rest
    return norm_rest


def _split_norm_path(path_norm: str) -> tuple[str, list[str]]:
    if not path_norm:
        return "", []
    m = _DRIVE_RE.match(path_norm)
    if m:
        drive = m.group(1).lower() + ":"
        rest = m.group(2)
        if rest.startswith("/"):
            return drive + "/", [p for p in rest.lstrip("/").split("/") if p]
        return drive, [p for p in rest.split("/") if p]

    if path_norm.startswith("//"):
        parts = [p for p in path_norm.lstrip("/").split("/") if p]
        if len(parts) >= 2:
            root = f"//{parts[0]}/{parts[1]}"
            return root, parts[2:]
        return f"//{'/'.join(parts)}", []

    if path_norm.startswith("/"):
        return "/", [p for p in path_norm.lstrip("/").split("/") if p]

    return "", [p for p in path_norm.split("/") if p]


def _relativize_if_under(path_norm: str, bundle_norm: str) -> Optional[str]:
    path_root, path_parts = _split_norm_path(path_norm)
    bundle_root, bundle_parts = _split_norm_path(bundle_norm)
    if path_root != bundle_root:
        return None
    if path_parts[: len(bundle_parts)] != bundle_parts:
        return None
    rel_parts = path_parts[len(bundle_parts):]
    if not rel_parts:
        return "."
    return "/".join(rel_parts)


def _normalize_path_for_hash(path_val: str, bundle_root: Optional[Path]) -> str:
    """Normalize path and make bundle-relative when possible."""
    norm = _normalize_path_string(path_val)
    if bundle_root is None or norm == "":
        return norm
    bundle_norm = _normalize_path_string(str(bundle_root))
    rel = _relativize_if_under(norm, bundle_norm)
    return rel if rel is not None else norm


def _is_path_key(key: str) -> bool:
    return key in _PATH_KEY_NAMES or key.endswith("_path")


def _input_sort_key(item: Any) -> tuple:
    """Stable sort key for inputs: path or empty, then name (thin mode may omit path)."""
    if not isinstance(item, dict):
        return ("", "")
    return (item.get("path") or "", item.get("name") or "")


def _canonicalize_report_value(
    value: Any, bundle_root: Optional[Path], report_root: Optional[Dict[str, Any]] = None
) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        is_root = report_root is None
        root = value if is_root else report_root
        skip_diagnostic = root is not None and root.get("bundle_mode") is not None
        for key, item in value.items():
            if key in _SELF_HASH_KEYS:
                continue
            if skip_diagnostic and key in _DIAGNOSTIC_ONLY_KEYS:
                continue
            if _is_path_key(key) and item is not None:
                out[key] = _normalize_path_for_hash(str(item), bundle_root)
            else:
                out[key] = _canonicalize_report_value(item, bundle_root, report_root)
        for list_key in _PATH_LIST_KEYS:
            if list_key in out and isinstance(out[list_key], list):
                if list_key == "inputs":
                    out[list_key] = sorted(out[list_key], key=_input_sort_key)
                elif list_key == "datasource_inputs":
                    out[list_key] = sorted(
                        out[list_key],
                        key=lambda x: (x.get("datasource") if isinstance(x, dict) else ""),
                    )
                else:
                    out[list_key] = sorted(
                        out[list_key],
                        key=lambda x: (x.get("path") if isinstance(x, dict) else ""),
                    )
        return out
    if isinstance(value, list):
        return [_canonicalize_report_value(v, bundle_root, report_root) for v in value]
    return value


def canonicalize_report(report: Dict[str, Any], bundle_root: Path) -> Dict[str, Any]:
    """
    Produce a deep copy of report suitable for deterministic hashing.
    - Removes self-hash fields (report_sha256/report_hash).
    - Excludes diagnostic-only keys (e.g. schema_lock_used_path) for v2 bundles (when bundle_mode is set).
    - Normalizes all path-like fields (path, *_path, file) to bundle-relative forward slashes.
    - Canonically sorts inputs by (path or "", name); artifacts, outputs by path.
    """
    bundle = Path(bundle_root).resolve()
    return _canonicalize_report_value(copy.deepcopy(report), bundle, report)


def canonicalize_report_for_hash(report: Dict[str, Any], bundle_root: Path) -> str:
    """Canonical JSON payload used for report hashing."""
    canonical = canonicalize_report(report, bundle_root)
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_report_sha256(report: Dict[str, Any], bundle_root: Path) -> str:
    """SHA-256 of canonical report payload: json.dumps(sort_keys=True, separators=(',', ':'), ensure_ascii=False)."""
    payload = canonicalize_report_for_hash(report, bundle_root)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
