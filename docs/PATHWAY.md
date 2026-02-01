# SANS Development Pathway

**Purpose:** Guide incremental development of SANS into a deterministic, clinically-relevant SAS-subset execution engine that can bypass SAS in SDTM pipelines.

This document is **normative for contributors** (human or AI). It defines *what comes next*, *why*, and *what must not happen*.

---

## 0. First Principles (Do Not Violate)

1. **Subset, not simulation**
   We do not aim to “be SAS.” We execute a *strict, documented subset* that covers real clinical workflows.

2. **Determinism over permissiveness**
   If SAS behavior is ambiguous, SANS chooses one policy, documents it, and enforces it.

3. **Explicit execution only**
   SANS executes only code and data explicitly provided by the user.
   No scanning, crawling, inference, or background inspection.

4. **Clinical reality first**
   Priority is given to constructs that appear in real CRO SDTM pipelines, not language completeness.

5. **Tests are the gate**
   Every new capability requires:

   * 1 end-to-end `hello_*` integration test
   * microtests for edge semantics
   * deterministic outputs

---

## 1. Current State (Baseline)

As of `SUBSET_SPEC v0.1`, SANS supports:

* DATA step (set, merge, by, retain, keep/drop, dataset options, control flow)
* PROC SORT (incl. nodupkey)
* PROC SQL (bounded subset)
* PROC TRANSPOSE (bounded)
* PROC SUMMARY / MEANS (bounded)
* PROC FORMAT + `put()`
* Structured unsupported-feature errors
* Minimal `validate --profile sdtm`
* Deterministic execution model

This is sufficient to execute **non-trivial CRO wrangling code**.

---

## 2. Pathway Overview (High Level)

The pathway is deliberately linear:

1. **Clinical I/O parity**
2. **Control-flow completeness**
3. **Macro tolerance (macro-lite)**
4. **Remaining CRO workhorse procs**
5. **SQL hardening**
6. **SDTM validation depth**
7. **Stability, reproducibility, trust**

Each phase assumes the previous phase is *complete and frozen*.

---

## 3. Phase v0.2 — Clinical I/O Parity

### Goal

Allow SANS to sit in an existing clinical pipeline without format translation glue.

### Scope

* First-class **XPT read and write**
* Stable round-tripping semantics
* Parse-but-ignore metadata statements so real scripts don’t bounce

### Deliverables

* Read `.xpt` as input tables
* Emit `.xpt` as output tables
* Canonical internal representation with documented serialization rules
* Tests for XPT → run → XPT determinism

### Required Hello Test

`hello_xpt`
Reads XPT inputs, performs a simple transform, emits XPT, asserts invariants.

### Hard Rules

* Internal canonicalization is allowed; byte-for-byte identity is not required
* Missing semantics must match spec
* No silent coercions

---

## 4. Phase v0.3 — DATA Step Control Flow + Macro-Lite M0

### Goal

Execute the *shape* of real CRO data steps without constant unsupported errors.

### Scope

#### DATA Step

* `do / end`
* `select / when / otherwise`
* bounded `do i = 1 to n` loops
* multiple `output` targets
* `else if` chains

#### Macro-Lite M0 (Preprocessor)

* `%let`
* `%include`
* `&VAR`
* `%if / %then / %else` (simple expressions only)

Explicitly unsupported:

* `%sysfunc`
* macro functions
* dynamic code generation

### Deliverables

* Preprocessor stage with explicit artifact (`preprocessed.sas`)
* Clear failure on unsupported macro constructs

### Required Hello Test

`hello_macro_m0`
Uses `%let`, `%include`, and macro variables to assemble a clinical-style transform.

### Hard Rules

* Macro expansion is *pure preprocessing*
* No runtime macro logic
* Expansion must be reproducible and inspectable

---

## 5. Phase v0.4 — CRO Workhorse Procs

### Goal

Cover the procs that dominate SDTM pipelines beyond SQL.

### Scope

* PROC FREQ (bounded: 1-way, maybe 2-way)
* PROC MEANS / SUMMARY (expanded stats)
* Vertical concatenation:

  * `set a b c;`
  * `proc append`
* PROC DATASETS-lite:

  * rename / delete datasets in workspace

### Deliverables

* Deterministic category ordering
* Explicit missing-category handling

### Required Hello Test

`hello_workhorses`
Concatenation + summary + freq on clinical-like data.

---

## 6. Phase v0.5 — SQL Idioms People Actually Write

### Goal

Stop losing on common PROC SQL patterns without opening the door to full SQL.

### Scope

* `case when`
* `coalesce`
* `distinct`
* `having`
* Stronger join diagnostics (early detection of row blowups)

### Deliverables

* Clear error on ambiguous column references
* Deterministic grouped output ordering

### Required Hello Test

`hello_lookup_and_case`
Dictionary joins + case mapping into SDTM-like flags.

---

## 7. Phase v0.6 — SDTM Profile Becomes Serious

### Goal

Move from “we ran it” to “we ran it and it is SDTM-sane.”

### Scope

* Expand SDTM rulepack:

  * DM, AE, LB, VS, EX, CM (solid)
  * SV, DS, MH (next)
* Cross-domain checks:

  * USUBJID consistency
  * key uniqueness expectations
  * controlled terminology (table-driven)
  * date shape and basic chronology

### Deliverables

* Stable rule catalog with IDs
* JSON validation reports
* Configurable severity handling

### Required Hello Test

`hello_sdtm_profile`
Generate small domain set and validate under SDTM profile.

---

## 8. Phase v0.7 — Macro-Lite M1 (Optional, Only If Needed)

### Goal

Tolerate *structural* macro usage without becoming macro-SAS.

### Scope

* `%macro / %mend` with parameter substitution
* simple `%do / %end` repetition

Still unsupported:

* macro functions
* system calls
* introspection

---

## 9. Stability and Trust (Always On)

These are not phases; they apply continuously.

* Artifact logging (inputs, plan/IR, outputs, validation)
* Deterministic hashing
* Versioned semantics tied to `SUBSET_SPEC`
* Reproducibility guarantees

---

## 10. How to Use This Document (for Codex)

* Treat each **phase** as a closed sprint
* Do not pull features from later phases “because it’s easy”
* Every phase ends with:

  * updated subset spec (if needed)
  * at least one new `hello_*`
  * microtests for edge semantics

If a change does not clearly advance the current phase’s goal, **do not implement it**.

---

## Final Note (Normative)

SANS wins by being:

* narrower than SAS
* stricter than SAS
* clearer than SAS

Every decision should make that more true.

---
