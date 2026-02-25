# Mutation-Safe Execution Substrate

## Sprint Plan

---

# 0. Guiding Objective

We are optimizing for:

* Deterministic IR as intent surface (`sans.ir`)
* Structured amendment application
* Assertion-enforced safety
* Fast simulation loop
* High-signal diff feedback for agents

Every sprint must strengthen one of those.

---

# Phase I — Formalize Mutation as a First-Class Primitive

**(2–3 sprints)**

---

## Sprint 1 — Mutation Contract & Targeting Rules

### Goal

Make amendment application formally defined, deterministic, and refusal-first.

### Deliverables

#### 1. `MUTATION_CONTRACT.md`

Defines:

* Amendment request schema (v1)
* Target selectors:

  * `step_id`
  * `transform_id`
  * `table_name`
  * `column_name`
* Resolution rules
* Ambiguity refusal rules
* Pre-state vs post-state semantics
* Structural invariants

#### 2. Kernel Primitive

Implement:

```python
apply_amendment(sans_ir, amendment_request) -> sans_ir'
```

Constraints:

* Pure function
* Deterministic
* Versioned
* No side effects
* Refuses on ambiguity

#### 3. Structural Diff Output

Produce:

```json
{
  "structural_changes": [...],
  "affected_steps": [...],
  "new_transform_ids": [...],
  "removed_transform_ids": [...]
}
```

This is not execution diff — only IR-level.

---

## Sprint 2 — IR Validator Hardening

### Goal

Ensure mutated IR cannot enter unsafe states.

### Add Validation Invariants

* Table existence before use
* No orphaned outputs
* No duplicate output tables
* Type consistency across transforms
* Explicit join key enforcement
* No hidden implicit ordering

### Assertion Enforcement

Clarify:

* Assertions bind to:

  * table
  * column
  * transform
* Assertions evaluated pre and post mutation
* Stable error payloads

Add new assertion types:

* `row_count_bound`
* `type_lock`
* `cardinality_assertion`
* `domain_narrowing_only`

---

## Sprint 3 — Simulation Mode (No Execution Required)

### Goal

Enable fast agent iteration without data execution.

### Implement

`simulate_mutation(sans_ir, amendment_request)`

Performs:

* apply_amendment
* structural validation
* assertion evaluation
* produces:

  * structural diff
  * assertion delta report
  * impact summary (graph-based, not row-based)

Must complete in milliseconds.

---

# Phase II — High-Signal Feedback for Agents

**(2–3 sprints)**

---

## Sprint 4 — Execution Diff Semantics

### Goal

Make execution-level changes visible and structured.

After full run:

Emit:

```json
{
  "row_deltas": {
    "table": {
      "rows_added": N,
      "rows_removed": M,
      "rows_modified": K
    }
  },
  "column_deltas": {
    "col": {
      "null_delta": +N,
      "unique_delta": +M,
      "distribution_shift": ...
    }
  }
}
```

No heuristics. Deterministic sampling allowed.

This becomes core agent feedback.

---

## Sprint 5 — Transform Impact Graph

### Goal

Enable mutation impact awareness without execution.

Add to simulation:

* Downstream step impact analysis
* Transform dependency traversal
* “blast radius” estimate

Output:

```json
{
  "impact_radius": {
    "steps": [...],
    "tables": [...],
    "assertions_at_risk": [...]
  }
}
```

This allows agent to optimize for minimal change.

---

## Sprint 6 — Mutation Scoring API

### Goal

Make mutation evaluable.

Define scoring structure:

```json
{
  "score": float,
  "assertion_failures": int,
  "invariant_violations": int,
  "impact_size": float,
  "determinism_confidence": 1.0
}
```

Workspace will use this to guide search.

Kernel just emits structured data.

---

# Phase III — Workspace Loop Stabilization

**(2–3 sprints)**

---

## Sprint 7 — PI Loop Integration (Minimal Version)

### Goal

Wire kernel mutation + simulation into Pi harness.

Workspace loop:

1. Load `sans.ir`
2. Agent proposes amendment_request
3. simulate_mutation
4. If safe → optional run
5. Evaluate score
6. Iterate

No fancy planning yet.

Just deterministic loop.

---

## Sprint 8 — Deterministic Caching Layer

### Goal

Speed matters.

Implement:

* Hash of sans.ir
* Hash of amendment_request
* Cached simulation results
* Cached execution diffs

Agents must iterate cheaply.

---

## Sprint 9 — Guardrails Against Agent Chaos

### Goal

Ensure system remains safe under adversarial proposals.

Add:

* Amendment rate limiting
* Max structural change threshold
* Assertion downgrade refusal
* No transform deletion without explicit override

Refuse silently dangerous moves.

---

# Phase IV — Domain Profiles (Optional, Thin Adapters)

**Only after mutation loop is solid.**

---

## Sprint 10 — Domain Assertion Profile

Example: Clinical Profile

Add:

* Controlled terminology assertion
* Required column existence sets
* Domain key uniqueness templates

This must be:

* Adapter module
* Pluggable
* Not pollute kernel core

---

# What We Explicitly Do NOT Build in This Window

* SAS macro resolution
* Full SAS ingestion
* UI polish
* Enterprise workflow layers
* Regulatory compliance scaffolding
* Vertical-specific orchestration

Those are downstream concerns.

---

# Definition of Success After Phase III

You can:

* Load a sans.ir
* Have an agent mutate it safely
* Reject unsafe proposals deterministically
* Simulate blast radius instantly
* Execute deterministically
* Produce structured diff
* Iterate autonomously

At that point:

You are a mutation-safe execution substrate.

Everything else becomes adapter layers.

---

# The North Star Test

If OpenAI releases an autonomous data agent tomorrow:

Can it plug into SANS as a safe mutation core?

If yes, you’re on track.

If not, re-evaluate every sprint.
