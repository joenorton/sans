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
datasource in = csv("data.csv")

table filtered = from(in) do
  derive do
    x = a + 1
    y = x * 2
  end
  filter(y > 10)
end

filtered
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

---

## Native `.sans` DSL

The native DSL provides a clean, linear syntax for data pipelines. It is safer than SAS, with strict rules for column creation and overwrites.

- **Additive by default**: Use `derive(col = expr)` to create new columns.
- **Explicit Overwrites**: Use `update! col = expr` to modify existing columns.
- **Stable Ties**: Sorting is stable; `nodupkey` preserves the first encountered row.

```sans
# process.sans
# sans 0.1
datasource raw = csv("raw.csv")

table enriched = from(raw) do
  derive(base_val = a + 1)
  filter(base_val > 0)
  derive do
    update! base_val = base_val * 10
    risk = if(base_val > 100, "HIGH", "LOW")
  end
end

enriched select subjid, base_val, risk
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

- **Specs**: [SUBSET_SPEC.md](./docs/SUBSET_SPEC.md) | [REPORT_CONTRACT.md](./docs/REPORT_CONTRACT.md)
- **Internals**: [ARCHITECTURE.md](./docs/ARCHITECTURE.md) | [IR_SPEC.md](./docs/IR_SPEC.md)
- **Guidance**: [ERROR_CODES.md](./docs/ERROR_CODES.md) | [BIG_PIC.md](./docs/BIG_PIC.md)