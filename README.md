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

### 1. Compile and Check
Verify a script without executing it. Emits the execution plan and a refusal/ok report.
```bash
sans check path/to/script.sas --out out_dir --tables in_table
```

### 2. Execute
Compile, validate, and run the script. Emits output tables (CSV/XPT) and the final report.
```bash
sans run path/to/script.sas --out out_dir --tables source=data.csv --format csv
```

### 3. Verify Reproducibility
Verify that a previously generated report matches the current state of files on disk.
```bash
sans verify out_dir/report.json
```

---

## Native `.sans` DSL

`sans` includes a native, modern DSL for data pipelines that compiles to the same deterministic IR. Use it to avoid SAS syntax quirks while maintaining full audit parity.

```sans
# example.sans
data target {
  from source do
    x = a + 1
    filter(x > 10)
    derive! y = x * 2
  end
}
```
Compile with: `sans check example.sans --out out`

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

- **Specs**: [SUBSET_SPEC.md](./docs/SUBSET_SPEC.md) | [REPORT_CONTRACT.md](./docs/REPORT_CONTRACT.md)
- **Internals**: [ARCHITECTURE.md](./docs/ARCHITECTURE.md) | [IR_SPEC.md](./docs/IR_SPEC.md)
- **Guidance**: [ERROR_CODES.md](./docs/ERROR_CODES.md) | [BIG_PIC.md](./docs/BIG_PIC.md)