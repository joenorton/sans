# RUNTIME_SEMANTICS & DETERMINISM (v1)

### Missing Value Semantics
- Ordering: Missing values (nulls) sort before all other values (ascending).
- Comparisons: `null < [any value]` is TRUE.
- Joins: Nulls in BY keys match other nulls in MERGE/SQL joins.

### CSV I/O Normalization
- Newlines: Enforced `\n` (LF) for all outputs regardless of Host OS.
- Quoting: Minimum quoting (only when necessary or for single-column empty strings).
- Column Order: Preserved from input or defined by SELECT/KEEP; stable across runs.

### Type Parsing Rules
- Numeric: Parsed as `Decimal` or `int` (no float precision loss).
- Strings: Preserved exactly.
- Leading Zeros: Strings like `"0123"` remain strings; they are not coerced to integers.

### Exit Code Bucket Mapping
- `0`: Success (ok)
- `10`: Success with warnings (ok_warnings)
- `30`: Frontend Refusal (Parse error)
- `31`: Validation Refusal (Metadata/Contract error)
- `32`: Capability Refusal (Unsupported feature)
- `50`: Execution Failure (Runtime/IO error)

### Artifact Set & Naming
- `plan.ir.json`: Stable IR representation.
- `report.json`: Execution summary with SHA-256 hashes for all inputs and outputs.
- Output Hashes: SHA-256 computed on canonicalized content (LF-only, CSV re-serialized).

