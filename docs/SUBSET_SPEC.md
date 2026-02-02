# SANS SAS-Subset Execution Specification

**Version:** 0.1.2
**Status:** Draft
**Scope:** Deterministic execution of a clinically-relevant subset of SAS sufficient to bypass SAS in SDTM-oriented data pipelines.

---

## 1. Purpose and Non-Goals

### 1.1 Purpose

SANS implements a **strict, deterministic subset of SAS** focused on clinical data wrangling. The goal is to execute the majority of CRO transformation workflows (raw → SDTM, and later ADaM) **without requiring SAS**, while improving determinism, inspectability, and testability.

### 1.2 Explicit Non-Goals

SANS is **not**:

* A full SAS interpreter
* A macro-complete SAS clone
* A byte-compatible replacement for SAS output
* A codebase scanner or inference engine

SANS executes **only** code explicitly provided by the user, under a well-defined subset contract.

---

## 2. Execution Model

### 2.1 Compilation Pipeline

Execution proceeds in distinct phases:

1. **Parse**: SAS-subset source → AST
2. **Plan**: AST → deterministic IR (intermediate representation)
3. **Execute**: IR → materialized datasets
4. **Validate (optional)**: outputs checked against profiles (e.g. SDTM)

Parser, planner, and runtime are intentionally separated.

### 2.2 Determinism Guarantees

Given:

* identical inputs
* identical SAS-subset source
* identical SANS version

SANS guarantees:

* identical output datasets (after canonicalization)
* identical validation results
* identical artifact hashes

No implicit randomness, time-based behavior, or environment-dependent semantics are permitted.

---

## 3. Supported Data Types and Semantics

### 3.1 Scalar Types

* **Numeric**: IEEE float (stored internally)
* **String**: UTF-8
* **Missing**:

  * numeric missing: `.` (represented as `null` in IR/JSON, `None` in Python)
  * string missing: empty string

### 3.2 Type Rules

* No silent coercions
* Arithmetic on missing → missing
* Comparison involving missing → false
* ISO-8601 date strings compare lexicographically

---

## 4. Supported Statements and Procedures

### 4.1 DATA Step

#### 4.1.1 Supported Constructs

* `data <out>; … run;`
* `set <table> [(dataset-options)];`
* `merge <table>(in=flag) …; by <keys>;`
* `by <keys>;`
* assignments: `x = expr;`
* conditionals:

  * `if expr;`
  * `if expr then stmt; else stmt;`
  * `else if` chains
* grouping state:

  * `first.<key>`
  * `last.<key>`
* state:

  * `retain <var>;`
* output control:

  * `output;`
  * `output <table>;`
* projection:

  * `keep <vars>;`
  * `drop <vars>;`
* control flow:

  * `do; … end;`
  * `do i = <int> to <int> [by <int>]; … end;`
  * `select; when(expr) … otherwise … end;`

Control‑flow rules:

* `do` loops require integer literal bounds (after macro substitution).
* `by` is optional and must be a non‑zero integer.
* `do while` / `do until` are unsupported.
* Control‑flow nesting depth cap: 50.
* Loop iteration cap: 1,000,000.

#### 4.1.2 Native `.sans` DSL (front-end)

SANS also supports a native `.sans` front-end that lowers deterministically into the same IR. It is intentionally strict:

* `from <table> do ... end` contains input modifiers only: `keep`, `drop`, `rename`, `where`.
* Output projection is `keep(...)` or `drop(...)` at data scope (single clause).
* Row filtering uses `filter <expr>` or `filter(<expr>)` at data scope.
* `if` is control-flow only.
* Equality is `==`; assignment is `=`.
* Mappings use `->` only (no `=>`).
* Overwrite requires `derive! col = expr`.

See `sans/sans/sans_script/docs/grammar.md` for the canonical grammar.

#### 4.1.3 Dataset Options (Read-Time)

Supported on `set` and `merge`:

* `keep=`
* `drop=`
* `rename=(a=b …)`
* `where=(expr)`
* `in=`

Precedence:

1. Dataset options (read-time)
2. Data step logic
3. Statement-level `keep/drop`

#### 4.1.3 Merge Semantics

* Inputs **must** be sorted by BY keys
* Supported:

  * 1:1
  * 1:many
* Many:many merges → **error**
* Merge alignment is deterministic
* `in=` flags are set explicitly per row

---

### 4.2 PROC SORT

Supported:

```sas
proc sort data=<in> out=<out> [nodup | nodupkey];
  by <keys...>;
run;
```

Semantics:

* Stable sort
* `nodupkey`: remove duplicates by BY keys, keep **first** row encountered in input order
* Explicit tie-breaking policy is enforced

---

### 4.3 PROC SQL (Bounded Subset)

Supported:

```sas
proc sql;
  create table <out> as
  select <cols | aggregates>
  from <table> [as alias]
  [inner|left] join <table> [as alias] on <expr>
  [where <expr>]
  [group by <cols>];
quit;
```

Allowed:

* joins: inner, left
* aggregates: `count`, `sum`, `min`, `max`, `avg`
* expressions: comparisons, boolean logic, literals
* column aliases

Disallowed (error):

* subqueries
* window functions
* unions
* correlated queries
* implicit joins

Output ordering is deterministic (sorted by group keys when grouping).

---

### 4.4 PROC TRANSPOSE

Supported subset:

```sas
proc transpose data=<in> out=<out>;
  by <keys>;
  id <column>;
  var <column>;
run;
```

Semantics:

* One output row per BY group
* Columns created from ID values
* Duplicate ID within group:

  * **last value wins** (after sort)
* Collision policy is explicit and documented

---

### 4.5 PROC SUMMARY / MEANS

Supported subset:

```sas
proc summary data=<in> nway;
  class <keys>;
  var <vars>;
  output out=<out> mean= / autoname;
run;
```

Semantics:

* Group by CLASS keys
* Supported stats: `mean`, `sum`, `min`, `max`, `n`, `nmiss`
* Deterministic output ordering

---

### 4.6 PROC FORMAT + PUT()

Supported:

```sas
proc format;
  value $name "A"="X" "B"="Y" other="";
run;
```

Usage:

```sas
new = put(old, $name.);
```

Semantics:

* Formats stored in runtime context
* `other=` applied when no match
* Missing input → `other`

---

## 5. Macro Support (Macro-Lite)

Supported:

* `%let`
* `%include`
* macro variables: `&VAR`
* `%if / %then / %else` (single-line, simple expressions only)

Explicitly unsupported:

* `%do / %end`
* `%macro / %mend`
* `%sysfunc`
* macro functions
* dynamic code generation
* symbol table introspection

Macros are expanded **before parsing**.

Macro control flow rules:

* `%if` must be single-line with `%then` (and optional `%else`) on the same line.
* `%do / %end` is not supported.

Include policy:

* Paths are resolved relative to the main script directory and any `--include-root` entries.
* Absolute paths are rejected unless `--allow-absolute-include` is provided.
* Traversal escaping include roots is rejected unless `--allow-include-escape` is provided.

---

## 6. Input / Output Formats

### 6.1 Supported Formats

* **CSV**: Standard UTF-8 CSV.
* **XPT**: SAS Transport Format (v5). Mandatory for clinical use.
* **Parquet**: (Planned) Deterministic columnar format.

### 6.2 Canonicalization

SANS enforces strict canonicalization rules for I/O to ensure determinism:

* **Strings**: Trailing spaces are trimmed on read (padding removal). Empty strings represent missing values.
* **Numerics**: Missing values are normalized to `.` (IBM float `0x2E...` in XPT).
* **Dates**: ISO-8601 strings.
* **XPT Emission**: Fixed timestamps used in headers to ensure byte-for-byte identity.
* **XPT Types**:
  * numeric -> float
  * char -> string
  * if all values are missing, default to **char** (length 8)
* **XPT Lengths**:
  * char length inferred from max observed value, capped at 200
  * overflow above cap -> error
* **XPT Padding**:
  * internal representation trims trailing padding
  * emitted XPT pads to fixed length with spaces
* **XPT Labels/Formats**:
  * parsed and ignored (warning only); parity not claimed

---

## 7. Validation Profiles

Validation is optional and explicit.

Example:

```bash
sans validate --profile sdtm
```

Profiles define:

* required variables
* key constraints
* format checks
* cross-domain invariants

Validation produces:

* machine-readable JSON
* deterministic rule IDs
* no side effects on execution

---

## 8. Error Handling

All unsupported constructs:

* fail fast
* include file + line number
* include construct name
* include suggestion when possible

No silent fallbacks.

---

## 9. Versioning and Compatibility

* Subset spec is versioned independently
* Behavior changes require spec bump
* Execution semantics are tied to spec version
* Reproducibility artifacts include spec version

---

## 10. Explicitly Unsupported (Non-Exhaustive)

* Full SAS macro language
* GRAPH / REPORT procs
* ODS
* DATA step hash objects
* CALL routines
* Interactive features
* Environment introspection

---

## 11. Design Philosophy (Normative)

SANS favors:

* explicit rules over historical quirks
* determinism over permissiveness
* execution clarity over feature completeness

### 11.1 Column Ordering Policy

Deterministic column order is guaranteed based on the following precedence:

1.  **Explicit Projection**: `keep`, `select`, or `var` statements define the order exactly as listed.
2.  **Single Input**: Preserves source column order.
3.  **Joins/Merges**: Without explicit projection, columns are ordered as: `Left source columns`, then `Right source columns` (preserving source order, excluding duplicates).
4.  **Derived Columns**: Appended to the end in order of assignment in the script.

If SAS behavior is ambiguous, SANS chooses a single documented policy and enforces it.

---

## 12. Reproducibility and Verification

### 12.1 Repro Bundle

The reproducibility bundle consists of:

* **Manifest**: `report.json` containing metadata, input hashes, output hashes, and plan hash.
* **Artifacts**: Original input files, generated plan (`plan.ir.json`), and output files.

### 12.2 Hashing

* Algorithm: **SHA-256**
* Hash is computed on **canonicalized** content.

### 12.3 Canonicalization for Hashing

* **Text files** (`.sas`, `.json`, `.txt`, `.md`, `.toml`, `.yaml`, `.yml`):
  * Line endings normalized to LF (`\n`).
  * Encoded as UTF-8.
* **CSV files**:
  * Parsed as CSV.
  * Line endings normalized to LF (`\n`).
  * Encoded as UTF-8.
  * Field values preserved (no whitespace trimming).
* **XPT files**:
  * Hashed by raw bytes (deterministic emission required).
* **Other files**:
  * Raw bytes.

### 12.4 Verification

The `sans verify <bundle_path>` command:

1. Loads the manifest (`report.json`).
2. Verifies existence of all inputs and outputs.
3. Re-computes hashes of all artifacts using canonicalization rules.
4. Compares against manifest hashes.
5. Fails if any mismatch is detected.

For `report.json` self-verification, the hash is compared against the content of the file with the self-hash field set to `null`.

---

## 13. Capability Matrix (v0.1.2)

| Feature | Support | Note |
| :--- | :--- | :--- |
| **DATA Step** | | |
| `set` | Yes | |
| `merge` | Yes | 1:1, 1:Many only. Sorted input required. |
| `by` | Yes | |
| `output` | Yes | Explicit output supported. |
| `keep`/`drop` | Yes | |
| `retain` | Yes | |
| `first.`, `last.` | Yes | |
| **Procedures** | | |
| `PROC SORT` | Yes | `nodupkey` supported (first wins). Stable sort. |
| `PROC SQL` | Limited | Inner/Left Join, Aggregates. No subqueries. |
| `PROC TRANSPOSE` | Yes | `id`, `var` required. |
| `PROC SUMMARY` | Yes | `nway` implied. |
| `PROC FORMAT` | Yes | Value mapping only. |
| **Functions** | | |
| `coalesce` | Yes | |
| `input` | Limited | `best.` only. |
| `put` | Yes | User-defined formats only. |
| **I/O Formats** | | |
| `CSV` | Yes | |
| `XPT` | Yes | SAS Transport v5. |
| **System** | | |
| `sans check` | Yes | |
| `sans run` | Yes | |
| `sans validate` | Yes | SDTM profile. |
| `sans verify` | Yes | Repro bundle verification. |

## Changelog

### v0.1.2
*   Implemented XPT (SAS Transport v5) read/write support.
*   Added `output_format` support to `sans run`.
*   Formalized Column Ordering Policy (Section 11.1).

### v0.1.1
*   Added Capability Matrix.
*   Added Reproducibility and Verification (Section 12).
*   Added `PROC SUMMARY` section.
*   Frozen Error Code list.
