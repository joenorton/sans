import struct
import math
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Union, Tuple
from pathlib import Path

# --- Constants ---
BLOCK_SIZE = 80
HEADER_RECORD_LIB = b'HEADER RECORD*******LIBRARY HEADER RECORD!!!!!!!'
HEADER_RECORD_MEM = b'HEADER RECORD*******MEMBER  HEADER RECORD!!!!!!!'
HEADER_RECORD_DSC = b'HEADER RECORD*******DESCRIPTOR HEADER RECORD!!!!!!!'
HEADER_RECORD_OBS = b'HEADER RECORD*******OBS     HEADER RECORD!!!!!!!'
MAX_CHAR_LEN = 200


class XptError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class XptWarning:
    code: str
    message: str

# --- IBM Float Conversion ---

def _ibm_to_ieee(ibm_bytes: bytes) -> Optional[float]:
    if len(ibm_bytes) != 8:
        return None 
    if ibm_bytes == b' ' * 8:
        return None
    if ibm_bytes == b'\x2E\x00\x00\x00\x00\x00\x00\x00':
        return None
    int_val = struct.unpack(">Q", ibm_bytes)[0]
    if int_val == 0:
        return 0.0
    sign = 1 if (ibm_bytes[0] & 0x80) else 0
    exponent = (ibm_bytes[0] & 0x7F) - 64
    mantissa_int = int_val & 0x00FFFFFFFFFFFFFF
    mantissa = mantissa_int / 72057594037927936.0 # 2^56
    return ((-1.0)**sign) * mantissa * (16.0 ** exponent)

def _ieee_to_ibm(val: Optional[Union[float, int]]) -> bytes:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return b'\x2E\x00\x00\x00\x00\x00\x00\x00'
    if val == 0.0:
        return b'\x00' * 8
    sign = 0
    if val < 0:
        sign = 1
        val = -val
    try:
        exponent = int(math.floor(math.log(val, 16))) + 1
    except ValueError:
        return b'\x00' * 8
    mantissa = val / (16.0 ** exponent)
    mant_int = int(mantissa * 72057594037927936.0) # 2^56
    exp_field = exponent + 64
    if exp_field > 127 or exp_field < 0:
        return b'\x2E\x00\x00\x00\x00\x00\x00\x00'
    first_byte = (sign << 7) | exp_field
    packed_mant = struct.pack(">Q", mant_int)[1:]
    return struct.pack("B", first_byte) + packed_mant

# --- Helpers ---

def _pad_str(s: str, length: int) -> bytes:
    return s.encode("ascii", errors="replace").ljust(length, b" ")

def _read_str(b: bytes) -> str:
    return b.decode("ascii", errors="replace").rstrip()

class XptReader:
    def __init__(self, path: Path):
        self.data = path.read_bytes()
        self.pos = 0
        self.warnings: List[XptWarning] = []
        
    def warn(self, code: str, message: str) -> None:
        self.warnings.append(XptWarning(code=code, message=message))
        
    def find_header(self, prefix: bytes) -> bool:
        # Search for prefix at block boundaries
        while self.pos < len(self.data):
            if self.data[self.pos:self.pos+len(prefix)] == prefix:
                return True
            self.pos += BLOCK_SIZE
        return False

    def read_block(self) -> bytes:
        block = self.data[self.pos:self.pos+BLOCK_SIZE]
        self.pos += BLOCK_SIZE
        return block

    def read_dataset(self) -> List[Dict[str, Any]]:
        if len(self.data) < BLOCK_SIZE:
            raise XptError("SANS_RUNTIME_XPT_INVALID", "XPT file too small to contain headers.")
        if self.data[:len(HEADER_RECORD_LIB)] != HEADER_RECORD_LIB:
            raise XptError("SANS_RUNTIME_XPT_INVALID", "Missing LIBRARY header in XPT file.")

        if not self.find_header(HEADER_RECORD_MEM):
            raise XptError("SANS_RUNTIME_XPT_INVALID", "Missing MEMBER header in XPT file.")
        self.pos += BLOCK_SIZE # Skip MEM header block
        
        if not self.find_header(HEADER_RECORD_DSC):
            raise XptError("SANS_RUNTIME_XPT_INVALID", "Missing DESCRIPTOR header in XPT file.")
        self.pos += BLOCK_SIZE # Skip DSC header block
        
        mem_info = self.read_block() # REC 3
        if not mem_info:
            raise XptError("SANS_RUNTIME_XPT_INVALID", "Missing MEMBER descriptor record.")
        
        # Parse nvar at 54:58
        try:
            nvar_str = mem_info[54:58].decode('ascii', errors='replace').strip()
            nvar = int(nvar_str)
        except ValueError:
            nvar = 0
            
        self.read_block() # REC 4
        
        if nvar == 0:
            raise XptError("SANS_RUNTIME_XPT_INVALID", "XPT file has no variables defined.")
            
        # Namestrs (140 bytes each)
        total_namestr_bytes = nvar * 140
        blocks_needed = (total_namestr_bytes + BLOCK_SIZE - 1) // BLOCK_SIZE
        raw_namestrs = self.data[self.pos : self.pos + blocks_needed * BLOCK_SIZE]
        self.pos += blocks_needed * BLOCK_SIZE
        if len(raw_namestrs) < total_namestr_bytes:
            raise XptError("SANS_RUNTIME_XPT_INVALID", "XPT namestr section is incomplete.")
        
        variables = []
        for i in range(nvar):
            chunk = raw_namestrs[i*140 : i*140 + 140]
            if len(chunk) < 140: break
            ntype = struct.unpack(">H", chunk[0:2])[0]
            nlength = struct.unpack(">H", chunk[4:6])[0]
            nname = _read_str(chunk[8:16])
            if ntype not in (1, 2):
                raise XptError("SANS_RUNTIME_XPT_UNSUPPORTED", f"Unsupported XPT variable type {ntype} for '{nname}'.")
            label_bytes = chunk[16:56]
            format_bytes = chunk[56:64]
            if (label_bytes.strip() or format_bytes.strip()):
                self.warn(
                    "SANS_RUNTIME_XPT_LABEL_FORMAT_IGNORED",
                    "XPT labels/formats are ignored for parity.",
                )
            variables.append({"name": nname, "type": "numeric" if ntype == 1 else "char", "length": nlength})
            
        if not self.find_header(HEADER_RECORD_OBS):
            raise XptError("SANS_RUNTIME_XPT_INVALID", "Missing OBS header in XPT file.")
        self.pos += BLOCK_SIZE # Skip OBS header block
        
        row_len = sum(v["length"] for v in variables)
        if row_len == 0:
            raise XptError("SANS_RUNTIME_XPT_INVALID", "XPT row length is 0.")
        
        # The data starts here and continues in blocks
        # We process until end of data or next header? 
        # Standard XPT has no trailer for member data.
        
        rows = []
        cursor = 0
        remaining = self.data[self.pos:]
        if len(remaining) == 0:
            return []
        remainder = len(remaining) % row_len
        if remainder:
            trailing = remaining[-remainder:]
            if trailing.strip(b" "):
                raise XptError("SANS_RUNTIME_XPT_INVALID", "XPT data section length is invalid.")
            remaining = remaining[:-remainder]
        count = len(remaining) // row_len
        
        for _ in range(count):
            row_bytes = remaining[cursor : cursor + row_len]
            row = {}
            row_cursor = 0
            for var in variables:
                val_bytes = row_bytes[row_cursor : row_cursor + var["length"]]
                row_cursor += var["length"]
                if var["type"] == "numeric":
                    row[var["name"]] = _ibm_to_ieee(val_bytes)
                else:
                    row[var["name"]] = _read_str(val_bytes)
            rows.append(row)
            cursor += row_len

        def is_all_missing(r: Dict[str, Any]) -> bool:
            for var in variables:
                val = r.get(var["name"])
                if var["type"] == "numeric":
                    if val is not None:
                        return False
                else:
                    if val != "":
                        return False
            return True

        # Trim trailing padding rows that are all-missing.
        while rows and is_all_missing(rows[-1]):
            rows.pop()

        return rows

class XptWriter:
    def __init__(self, path: Path):
        self.f = path.open("wb")
        
    def write_block(self, data: bytes):
        self.f.write(data.ljust(BLOCK_SIZE, b' '))
        
    def write(self, rows: List[Dict[str, Any]], columns: List[str], dataset_name: str = "DATASET"):
        if not columns and rows:
            columns = list(rows[0].keys())
        vars_def = []

        def infer_type_and_length(col: str) -> tuple[str, int]:
            saw_value = False
            saw_str = False
            max_len = 1
            for r in rows:
                val = r.get(col)
                if val is None:
                    continue
                saw_value = True
                if isinstance(val, str):
                    saw_str = True
                    max_len = max(max_len, len(val))
                elif isinstance(val, (int, float)):
                    max_len = max(max_len, len(str(val)))
                else:
                    saw_str = True
                    max_len = max(max_len, len(str(val)))
            if saw_str:
                if max_len > MAX_CHAR_LEN:
                    raise XptError(
                        "SANS_RUNTIME_XPT_LENGTH_EXCEEDED",
                        f"XPT char length {max_len} exceeds cap {MAX_CHAR_LEN} for column '{col}'.",
                    )
                return ("char", max_len)
            if not saw_value:
                return ("char", 8)
            return ("numeric", 8)

        for col in columns:
            col_type, col_len = infer_type_and_length(col)
            if col_type == "char":
                vars_def.append({"name": col, "type": "char", "length": max(1, col_len)})
            else:
                vars_def.append({"name": col, "type": "numeric", "length": 8})

        date_str = b"01JAN20:00:00:00"
        self.write_block(HEADER_RECORD_LIB)
        self.write_block(b"SAS     SAS     SASLIB  6.06    " + b" " * 48)
        self.write_block(date_str + b" " * (80 - len(date_str)))
        
        self.write_block(HEADER_RECORD_MEM)
        self.write_block(HEADER_RECORD_DSC)
        
        name_pad = _pad_str(dataset_name[:8], 8)
        nvar_str = f"{len(vars_def):04d}".encode("ascii")
        rec3 = (b"SAS     " + name_pad + b"        " * 3 + date_str[:8] + b"        " + date_str[:8] + b"        " + b" " * 8)
        rec3 = rec3[:54] + nvar_str + rec3[58:]
        self.write_block(rec3)
        self.write_block(b"SAS     SAS     " + b" " * 64)
        
        raw_namestrs = b""
        for i, var in enumerate(vars_def):
            ntype = 1 if var["type"] == "numeric" else 2
            vdata = struct.pack(">H", ntype) + b"\x00\x00" + struct.pack(">H", var["length"]) + struct.pack(">H", i+1)
            vdata += _pad_str(var["name"][:8], 8)
            vdata += _pad_str("", 40) + _pad_str("", 8) + struct.pack(">H", 0) + struct.pack(">H", 0) + _pad_str("", 8) + struct.pack(">H", 0) + struct.pack(">H", 0) + struct.pack(">l", 0)
            vdata += b" " * (140 - len(vdata))
            raw_namestrs += vdata
        
        pad_len = (BLOCK_SIZE - (len(raw_namestrs) % BLOCK_SIZE)) % BLOCK_SIZE
        raw_namestrs += b" " * pad_len
        for i in range(0, len(raw_namestrs), BLOCK_SIZE):
            self.write_block(raw_namestrs[i:i+BLOCK_SIZE])
            
        self.write_block(HEADER_RECORD_OBS)
        
        raw_data = b""
        for row in rows:
            for var in vars_def:
                if var["type"] == "numeric":
                    raw_data += _ieee_to_ibm(row.get(var["name"]))
                else:
                    val = row.get(var["name"])
                    text = "" if val is None else str(val)
                    raw_data += _pad_str(text, var["length"])
        
        pad_len = (BLOCK_SIZE - (len(raw_data) % BLOCK_SIZE)) % BLOCK_SIZE
        raw_data += b" " * pad_len
        for i in range(0, len(raw_data), BLOCK_SIZE):
            self.write_block(raw_data[i:i+BLOCK_SIZE])
        self.f.close()

def load_xpt_with_warnings(path: Path) -> Tuple[List[Dict[str, Any]], List[XptWarning]]:
    reader = XptReader(path)
    rows = reader.read_dataset()
    return rows, reader.warnings


def load_xpt(path: Path) -> List[Dict[str, Any]]:
    rows, _warnings = load_xpt_with_warnings(path)
    return rows
        
def dump_xpt(path: Path, rows: List[Dict[str, Any]], columns: Optional[List[str]] = None, dataset_name: str = "DATASET") -> None:
    if columns is None and rows: columns = list(rows[0].keys())
    elif columns is None: columns = []
    writer = XptWriter(path)
    writer.write(rows, columns, dataset_name=dataset_name)
