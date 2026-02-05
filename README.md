# sans

**Small, deterministic compiler and executor for a strict SAS‑like batch subset.**

`sans` compiles SAS‑like scripts into a machine-readable IR (`plan.ir.json`), executes them against tabular data, and emits a detailed execution report (`report.json`). It is built for auditability, reproducibility, and strict safety.

- **Strict by Default**: Unsupported constructs refuse the entire script with stable error codes.
- **Deterministic**: Bit‑identical outputs (CSV/XPT) across Windows and Linux.
- **Audit‑Ready**: Every run generates a signed manifest (SHA‑256) of all inputs and outputs.
- **Portable**: No SAS installation required; zero‑dependency runtime (except `pydantic` for schema).

---

## Installation

```bash
pip install -e .
```
This installs the `sans` CLI command. You can also use `python -m sans`.

---

## Quickstart

### 1. Create a pipeline
`sans` supports a modern `.sans` DSL or a strict SAS‑like subset.

```sans
# example.sans
# sans 0.1

datasource in = inline_csv do
  a,b
  6,7
  3,2
end

table t = from(in) do
  derive(base2 = a * 2)
  filter(base2 > 10)
  select a, base2
end

save t to "out.csv"
```

### 2. Compile and Check
Verify a script without executing it. Emits the execution plan (`plan.ir.json`) and a refusal/ok report.
```bash
sans check example.sans --out out
```

### 3. Execute
Compile, validate, and run. Emits output tables (CSV/XPT) and the final signed manifest.
```bash
sans run example.sans --out out
```

### 4. Verify
Verify that a previously generated report matches the current state of files on disk.
```bash
sans verify out/report.json
```

### 5. Format
Canonicalize `.sans` formatting (presentation only).
```bash
sans fmt example.sans
sans fmt example.sans --check
sans fmt example.sans --in-place
```

---

## FMT Usage

`sans fmt` is a pure formatter: it changes presentation only and guarantees parse‑equivalence and idempotence.

**Modes**
1. `canonical` (default): applies the canonical v0 style.
2. `identity`: preserves bytes (except newline normalization to `\n`).

**Flags**
1. `--check`: exit non‑zero if formatting would change the file.
2. `--in-place`: rewrite the file atomically (writes a temp file, then replaces).

Examples:
```bash
sans fmt script.sans
sans fmt script.sans --mode identity
sans fmt script.sans --check
sans fmt script.sans --in-place
```

---

## Native `.sans` DSL

The native DSL provides a clean, linear syntax for data pipelines. It is safer than SAS, with strict rules for column creation and overwrites.

- **Additive by default**: Use `derive(col = expr)` to create new columns only (error if column exists).
- **Explicit overwrites**: Use `update!(col = expr)` to modify existing columns only (error if missing).
- **Explicit output**: Outputs are defined only via **save**; there is no implicit "last table wins."
- **Explicit cast**: Use `cast(col -> type [on_error=null] [trim=true], ...)` for deterministic type conversion; target types: `str`, `int`, `decimal`, `bool`, `date`, `datetime`. Evidence (cast_failures, nulled) is emitted in runtime.evidence.json.
- **Stable ties**: Sorting is stable; `nodupkey` preserves the first encountered row.

**expanded.sans** is the canonical human-readable form (fully explicit, no blocks, kernel vocabulary only); scripts are sugar that lower to the same IR. Compiling expanded.sans must reproduce the same plan.ir.json (byte-identical aside from quarantined metadata).

```sans
# process.sans
# sans 0.1
datasource raw = csv("raw.csv")

table enriched = from(raw) do
  derive(base_val = a + 1)
  filter(base_val > 0)
  update!(base_val = base_val * 10)
  derive(risk = if(base_val > 100, "HIGH", "LOW"))
  cast(base_val -> str)
  select(subjid, base_val, risk)
end

save enriched to "enriched.csv"
```

---

## Supported SAS Subset

- **DATA Step**: `set`, `merge` (with `in=`), `by` (first./last.), `retain`, `if/then/else`, `keep/drop/rename`.
- **Dataset Options**: `(keep= drop= rename= where=)`.
- **Procs**: 
  - `proc sort` (`nodupkey`)
  - `proc transpose` (`by`, `id`, `var`)
  - `proc sql` (Inner/Left joins, `where`, `group by`, aggregates)
  - `proc format` (Value mappings + `put()` lookups)
  - `proc summary` (Class means with `autoname`)
- **Macro‑lite**: `%let`, `%include`, `&var`, single‑line `%if/%then/%else`.

---

## Determinism & Runtime Semantics

`sans` guarantees stability through strict runtime rules:
- **Missing Values**: Nulls sort *before* all data and satisfy `null < [value]`.
- **Numeric Precision**: Uses `Decimal` to prevent float precision loss.
- **I/O Normalization**: Enforces LF (`\n`) and deterministic CSV quoting.
- **Stable Hashes**: Artifact hashes are invariant across OS platforms.

See [DETERMINISM.md](./DETERMINISM.md) for the sacred v1 invariants.

---

## Deep References

- **Specs**: [SUBSET_SPEC.md](./docs/SUBSET_SPEC.md) | [REPORT_CONTRACT.md](./docs/REPORT_CONTRACT.md) | [IR_CANONICAL_PARAMS.md](./docs/IR_CANONICAL_PARAMS.md)
- **Internals**: [ARCHITECTURE.md](./docs/ARCHITECTURE.md) | [IR_SPEC.md](./docs/IR_SPEC.md)
- **Guidance**: [ERROR_CODES.md](./docs/ERROR_CODES.md) | [BIG_PIC.md](./docs/BIG_PIC.md)
