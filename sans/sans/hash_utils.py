import hashlib
import csv
from pathlib import Path
from typing import Optional

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

def compute_raw_hash(path: Path) -> Optional[str]:
    """Computes the raw SHA-256 hash of a file's bytes."""
    if not path.exists():
        return None
    try:
        data = path.read_bytes()
        return hashlib.sha256(data).hexdigest()
    except OSError:
        return None

def compute_artifact_hash(path: Path) -> Optional[str]:
    """
    Computes a deterministic hash of a file.
    - If CSV: canonicalize content then hash.
    - If text (sas, json, txt...): canonicalize line endings.
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
            
    if suffix in {".sas", ".json", ".txt", ".md", ".toml", ".yaml", ".yml"}:
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
