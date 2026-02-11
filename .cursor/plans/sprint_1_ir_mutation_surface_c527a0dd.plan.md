---
name: Sprint 1 IR Mutation Surface (Revised)
overview: Establish sans.ir as the single canonical editable IR. Remove all legacy/obsolete IR schema references. Implement deterministic normalization from IRDoc → sans.ir and sans.ir → expanded.sans printing via existing printer. Add run-ir CLI that executes sans.ir through the existing runtime core. No alternate schemas. No save splitting. Stable step handles derived from semantic anchors (not positional indices).
todos: []
isProject: false
---

# Sprint 1 — IR as the Mutation Surface (Authoritative Version)

## Critical Clarification

There is **one canonical IR format going forward: `sans.ir`.**

Any pre-existing IR schema documentation (e.g. [docs/sans-ir-v0.1-schema](docs/sans-ir-v0.1-schema)) is obsolete and must either:

- be **removed**, or
- be **renamed** explicitly to describe `plan.ir.json` (execution IR with loc + transform ids)

There must be **no competing IR definitions** in the repo after this sprint.

---

## Core Principles

1. `sans.ir` is the mutation surface.
2. `sans.ir` contains *only semantic data*.
3. Execution continues to use in-memory `IRDoc`.
4. Both `run` (text) and `run-ir` (IR file) feed the same `execute_plan`.
5. **No positional step IDs.** No s1/s2 numbering — use semantic anchors only.

---

## 1. Define Canonical `sans.ir` Schema

**Location:** `sans/ir/` package.

Convert existing [sans/sans/ir.py](sans/sans/ir.py) into package:

```
sans/ir/
  __init__.py   # exports IRDoc + related types (move current ir.py content here or to doc.py)
  schema.py     # sans.ir schema definitions
  normalize.py  # IRDoc → sans.ir
  adapter.py    # sans.ir → IRDoc
```

**Do not add** `printer.py` — the printer is the existing `irdoc_to_expanded_sans(IRDoc)`.

---

### sans.ir Structure (Authoritative)

```json
{
  "version": "0.1",
  "datasources": {
    "lb": {
      "kind": "csv",
      "path": "lb.csv",
      "columns": {
        "USUBJID": "string",
        "VISITNUM": "int"
      }
    }
  },
  "steps": [
    {
      "id": "out:__t2__",
      "op": "identity",
      "inputs": ["__datasource__lb"],
      "outputs": ["__t2__"],
      "params": {}
    },
    {
      "id": "out:sorted_high",
      "op": "save",
      "inputs": ["sorted_high"],
      "outputs": [],
      "params": { "path": "sorted_high.csv" }
    }
  ]
}
```

---

### Hard Rules

**Excluded fields (must NOT appear in sans.ir):**

- transform_id
- transform_class_id
- step_id
- loc
- any runtime fingerprint
- registry ids

**Step identity rule (semantic anchors only):**

- If step produces **one output table:** `id = "out:<output_table_name>"`
- For **datasource steps:** `id = "ds:<datasource_name>"`

This gives stable anchors and avoids cascade renumbering on insertion (mutation-friendly).

**Save steps:** Do **not** split saves into a top-level `saves[]`. `save` remains a normal step in `steps[]`. One semantic representation only.

**Canonical ordering:**

- datasources: sorted by key
- steps: topologically sorted
- params: keys sorted (recursively for nested structures)
- JSON serialization: deterministic; no UUIDs; no timestamps

---

## 2. Normalization: IRDoc → sans.ir

**File:** [sans/ir/normalize.py](sans/ir/normalize.py)

- **Input:** IRDoc  
- **Output:** canonical sans.ir dict

**Responsibilities:**

- Strip all execution-only fields.
- Generate **semantic** step ids per rule above (`out:<table>`, `ds:<name>`).
- Preserve step structure exactly.
- Do **not** collapse identity steps.
- Do **not** rewrite logic or perform semantic optimizations.
- Canonicalize param key ordering recursively (e.g. reuse [sans/sans_script/canon.py](sans/sans_script/canon.py) style).

Normalization must be **deterministic**.

---

## 3. Adapter: sans.ir → IRDoc

**File:** [sans/ir/adapter.py](sans/ir/adapter.py)

- **Input:** sans.ir dict  
- **Output:** IRDoc

**Responsibilities:**

- Rebuild `OpStep` objects (and datasources, table_facts as needed).
- Provide **synthetic Loc** objects if runtime/validator require them.
- Preserve step order exactly as in sans.ir.
- No mutation or inference of logic.

Target invariant:

```
expanded.sans → compile → IRDoc
IRDoc → normalize → sans.ir
sans.ir → adapter → IRDoc
IRDoc → irdoc_to_expanded_sans → expanded.sans
```

must be lossless (byte-identical expanded.sans and semantically identical sans.ir).

---

## 4. Printer

**Do not** implement a new printer.

Use existing [sans/sans_script/expand_printer.py](sans/sans_script/expand_printer.py):

```
sans.ir → adapter → IRDoc → irdoc_to_expanded_sans(IRDoc)
```

No alternate formatting logic.

---

## 5. run-ir CLI

**Subcommand:**

```
sans run-ir input.sans.ir --out outdir
```

**Implementation:**

- Load sans.ir JSON from `input.sans.ir`.
- Convert via adapter → IRDoc.
- Pass IRDoc to existing `execute_plan` (same path as `sans run`).
- Produce identical bundle shape as `sans run` (report.json, artifacts, outputs).

Execution logic must **not** fork. Register in [sans/**main**.py](sans/sans/__main__.py).

---

## 6. Obsolete IR Schema Cleanup

- **Remove or rename** [docs/sans-ir-v0.1-schema](docs/sans-ir-v0.1-schema):
  - Either delete it, or
  - Rename and repurpose to document **plan.ir.json** (execution IR: loc, transform_id, step_id, etc.) so it is explicit that it is *not* the canonical mutation IR.

After this sprint there must be **exactly one** canonical IR definition (sans.ir); no competing docs.

---

## 7. Tests

### A. IR round-trip

**File:** [tests/test_ir_roundtrip.py](tests/test_ir_roundtrip.py)

**Pipeline:**

1. `expanded.sans` → compile → IRDoc → normalize → **sans.ir A**
2. **sans.ir A** → adapter → IRDoc → printer → **expanded.sans'**
3. **expanded.sans'** → compile → IRDoc → normalize → **sans.ir B**

**Assertions:**

- `expanded.sans == expanded.sans'` (byte-identical).
- `A == B` (canonical IR equality).

Both assertions are required.

---

### B. Execution equivalence

**File:** [tests/test_ir_execution_equivalence.py](tests/test_ir_execution_equivalence.py)

- Run `sans run script.sans` and `sans run-ir script.sans.ir` (script.sans.ir produced from script.sans via normalize).
- Assert: report.json identical status; output artifact hashes identical.

---

### C. Canonicalization

**File:** [tests/test_ir_canonicalization.py](tests/test_ir_canonicalization.py)

- No `transform_id` in sans.ir.
- No `loc` in sans.ir.
- Deterministic JSON ordering.
- Multiple normalizations of same IRDoc produce identical sans.ir JSON.

---

## 8. Exit Criteria

Sprint complete when:

- There is **exactly one** canonical mutation IR (`sans.ir`).
- No obsolete IR schema remains (removed or renamed to plan.ir.json only).
- Round-trip is **byte-stable** (expanded.sans) and **canonical IR equality** (sans.ir A == B).
- run-ir produces **identical execution results** (status + output hashes) to run for same logic.
- No execution-only artifacts (transform_id, loc, step_id, etc.) appear in sans.ir.

---

## 9. Non-Goals

- No optimizer logic.
- No move system.
- No IR rewriting or mapping logic.

This sprint establishes the foundation only. All mutation work depends on this being correct.

---

## 10. File-Level Summary


| Action        | Path                                                                                                                                                                                                               |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Refactor      | `sans/ir.py` → `sans/ir/` package with `__init__.py`, `schema.py`, `normalize.py`, `adapter.py`                                                                                                                    |
| Modify        | [sans/**main**.py](sans/sans/__main__.py): register `run-ir` subcommand                                                                                                                                            |
| Remove/rename | [docs/sans-ir-v0.1-schema](docs/sans-ir-v0.1-schema) — remove or rename to plan.ir.json documentation                                                                                                              |
| Add tests     | [tests/test_ir_roundtrip.py](tests/test_ir_roundtrip.py), [tests/test_ir_execution_equivalence.py](tests/test_ir_execution_equivalence.py), [tests/test_ir_canonicalization.py](tests/test_ir_canonicalization.py) |


