import hashlib
import json
import copy
import csv
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


def _normalize_path(path_val: str, bundle_root: Path) -> str:
    """Normalize path to posix, relative to bundle_root if under it; else posix (for determinism)."""
    p = Path(path_val)
    if not p.is_absolute():
        p = (bundle_root / p).resolve()
    else:
        p = p.resolve()
    bundle = bundle_root.resolve()
    try:
        rel = p.relative_to(bundle)
        return rel.as_posix()
    except ValueError:
        return p.as_posix()


def canonicalize_report(report: Dict[str, Any], bundle_root: Path) -> Dict[str, Any]:
    """
    Produce a deep copy of report suitable for deterministic hashing.
    - Removes report_sha256.
    - Normalizes all paths to bundle-relative forward slashes.
    - Canonically sorts inputs, artifacts, outputs by path for determinism.
    - report.json is not listed in any array; no special-case null-ing.
    """
    out = copy.deepcopy(report)
    bundle = Path(bundle_root).resolve()

    out.pop("report_sha256", None)

    if "plan_path" in out and out["plan_path"]:
        out["plan_path"] = _normalize_path(str(out["plan_path"]), bundle)

    for inp in out.get("inputs", []):
        if inp.get("path"):
            inp["path"] = _normalize_path(str(inp["path"]), bundle)

    for art in out.get("artifacts", []):
        if art.get("path"):
            art["path"] = _normalize_path(str(art["path"]), bundle)

    for o in out.get("outputs", []):
        if o.get("path"):
            o["path"] = _normalize_path(str(o["path"]), bundle)

    if "inputs" in out:
        out["inputs"] = sorted(out["inputs"], key=lambda x: (x.get("path") or ""))
    if "artifacts" in out:
        out["artifacts"] = sorted(out["artifacts"], key=lambda x: (x.get("path") or ""))
    if "outputs" in out:
        out["outputs"] = sorted(out["outputs"], key=lambda x: (x.get("path") or ""))

    return out


def compute_report_sha256(report: Dict[str, Any], bundle_root: Path) -> str:
    """SHA-256 of canonical report payload: json.dumps(sort_keys=True, separators=(',', ':'), ensure_ascii=False)."""
    canonical = canonicalize_report(report, bundle_root)
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
