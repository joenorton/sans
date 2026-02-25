---
name: sans kernel mutation v0.1.1
overview: Extend the kernel mutation substrate with deterministic blast radius in the structural diff, schema-level selector legality per op, and no-op mutation detection (E_AMEND_NO_OP), plus tests for each—without changing workspace, runtime, or mutation op set.
todos: []
isProject: false
---

# sans kernel mutation v0.1.1 (agent-signal hardening)

## Scope (kernel-only)

- No workspace changes; no runtime execution changes; no new mutation ops; no "best effort" mode.
- Preserve: pure ir→ir, atomic refusal-first, single-refusal emission.

---

## 1) Deterministic blast radius in `diff_structural`

### Goal

Emit deterministic graph-derived impact signals (`blast_radius_direct`, `blast_radius_downstream`, `touched`) for agents, without heuristics or runtime data.

### Current state

- [sans/sans/amendment/diff.py](sans/sans/amendment/diff.py): `build_structural_diff(ir_in, ir_out, ops_applied, affected_steps, affected_tables)` builds `affected.steps` and `affected.tables` (and transforms_*). No blast radius or `touched` yet.
- [sans/sans/amendment/apply.py](sans/sans/amendment/apply.py): Collects `affected_steps` and `affected_tables` per op and passes them into `build_structural_diff` after all ops succeed.

### Spec addition to `diff_structural["affected"]`

```json
{
  "blast_radius_direct": { "steps": ["..."], "tables": ["..."] },
  "blast_radius_downstream": { "steps": ["..."], "tables": ["..."] },
  "touched": [
    { "op_id": "op7", "kind": "set_params", "step_id": "s1", "table": null, "path": "/predicate/right" }
  ]
}
```

### Rules

- **Direct steps**: steps explicitly modified by ops (already captured as `affected_steps` in apply). **Direct tables**: union of `outputs[]` of those steps in `ir_out` (post-mutation), plus any renamed table names involved (already in `affected_tables`; use step outputs from `ir_out` for consistency).
- **Downstream steps**: build directed graph from `ir_out`: edge A → B iff any table in `A.outputs` is in `B.inputs`. Downstream set = transitive closure from direct steps, **excluding** direct (disjoint sets). **Downstream tables**: union of outputs of downstream steps.
- **Determinism**: all output lists sorted lexicographically. When building the downstream closure, iterate over steps in **the stored step order** (order in `ir_out["steps"]`), not by sorted step ids; then sort final direct/downstream step and table lists. This avoids dependence on Python dict insertion order in intermediate maps.
- **touched**: one entry per applied op: `op_id`, `kind`, `step_id` (resolved, or null for e.g. rename_table), `table` (from selector when relevant, or null), `path` (from selector when relevant, or null). Emit **sorted by op_id**; op_id uniqueness is already enforced by the request validator, so op_id sort is stable.

### Implementation

- **apply.py**: While applying ops, build a list `touched: List[Dict]`. For each op append `{"op_id": op.op_id, "kind": op.kind, "step_id": <resolved id or None>, "table": getattr(op.selector, "table", None), "path": getattr(op.selector, "path", None)}`. For ops that use `SelectorV1` and resolve to a step, set `step_id` from the resolved step; for `rename_table` use `TableSelectorV1` so `step_id` is null, `table` is the old name. For `add_step`, the “touched” step is the newly added step’s id. Pass `touched` into `build_structural_diff`.
- **diff.py**: Extend `build_structural_diff(..., touched: List[Dict] | None = None)`. Compute:
  - `blast_radius_direct.steps`: `sorted(set(affected_steps))`.
  - `blast_radius_direct.tables`: sorted union of outputs of those steps in `ir_out` (and optionally include any table in `affected_tables` that appears as renamed; spec says “plus any renamed table names involved” — can derive from `affected_tables` for simplicity).
  - Build graph: iterate steps in **stored order** (`ir_out["steps"]`); for each step index by step id and find consumers (steps whose `inputs` intersect this step’s `outputs`). Compute downstream = transitive closure from direct step ids minus direct. **Sort** the final downstream step list (and downstream tables) lexicographically.
  - `blast_radius_downstream.tables`: sorted union of outputs of downstream steps.
  - Add `touched` to `affected` sorted by `op_id` (uniqueness already enforced by request validator → stable).
- **Backward compatibility**: Existing `affected.steps` and `affected.tables` remain; new keys are additive.

---

## 2) Tighten selector legality per op

### Goal

Prevent using `table` as a universal step selector; restrict `path` and `assertion_id` to ops that use them.

### Rules (schema-level → `E_AMEND_VALIDATION_SCHEMA`)


| Selector field | Allowed only for                          |
| -------------- | ----------------------------------------- |
| `table`        | `rename_table`, `add_assertion`           |
| `assertion_id` | `remove_assertion`, `replace_assertion`   |
| `path`         | `set_params`, `replace_expr`, `edit_expr` |


- Step-targeting ops (`remove_step`, `replace_step`, `rewire_inputs`, `rewire_outputs`, `set_params`, `replace_expr`, `edit_expr`): require `step_id` and/or `transform_id`; **forbid** `table`. (`add_step` uses `AddStepSelectorV1`, no `table`.)
- For `set_params`, `replace_expr`, `edit_expr`: require `path` (already enforced); for all other ops using `SelectorV1`, forbid `path`.
- For ops using `SelectorV1`, forbid `assertion_id` (only assertion ops use `AssertionSelectorV1`).

### Current state

- [sans/sans/amendment/schemas.py](sans/sans/amendment/schemas.py): `SelectorV1` has optional `step_id`, `transform_id`, `table`, `assertion_id`, `path`. Per-op validators currently require “at least one of step_id/transform_id/table” for step ops and “path” for set_params/replace_expr/edit_expr; they do not yet **forbid** `table` or `path` or `assertion_id` where disallowed.

### Implementation

- In **schemas.py**, add or extend `@model_validator(mode="after")` on each op that uses `SelectorV1`:
  - **RemoveStepOpV1, ReplaceStepOpV1, RewireInputsOpV1, RewireOutputsOpV1**: require `(step_id or transform_id)` and forbid `table` and `path` and `assertion_id` (raise `ValueError` with a message that maps to schema validation).
  - **SetParamsOpV1, ReplaceExprOpV1, EditExprOpV1**: require `(step_id or transform_id)` and `path`; forbid `table` and `assertion_id`.
  - Ensure **RenameTableOpV1** and **AddAssertionOpV1** use `TableSelectorV1` (no `path`/`step_id`/`assertion_id`); **RemoveAssertionOpV1** and **ReplaceAssertionOpV1** use `AssertionSelectorV1` (no `path`/`table`/`step_id`).
- Map all new violations to schema validation failure so apply layer returns `E_AMEND_VALIDATION_SCHEMA` (already the case for Pydantic/ValueError in the request validation path in apply.py).

---

## 3) No-op mutation detection

### Goal

Refuse amendments that produce no IR change (same canonical hash) to avoid wasted agent iterations.

### Behavior

After applying all ops successfully and validating `work` with `validate_sans_ir`, and before building diffs and returning ok:

- Compute `base_ir_sha256 = canonical_sha256(ir_in)` and `mutated_ir_sha256 = canonical_sha256(work)` (existing `canonical_sha256` in diff.py).
- If they are equal:
  - Return `status="refused"`, code `E_AMEND_NO_OP`, message `"mutation produced no changes"`, diagnostics only (no `ir_out`).

### Implementation

- **errors.py**: Add and export `E_AMEND_NO_OP = "E_AMEND_NO_OP"`.
- **apply.py**: After the `validate_sans_ir(work)` block and before building `diff_structural`, add: if `canonical_sha256(ir_in) == canonical_sha256(work)`: return `_refused(E_AMEND_NO_OP, "mutation produced no changes")`. Import `canonical_sha256` from diff (or use the one already used for diff building).

---

## 4) Tests

### Blast radius (new or in existing success/diff test file)

- **Minimal chain**: IR with steps s1→t1, s2→t2, s3→t3 (s2 inputs t1, s3 inputs t2). Apply one op that touches s1 only (e.g. `set_params` on s1). Assert:
  - `blast_radius_direct.steps == ["s1"]` (or the actual step id used),
  - `blast_radius_downstream.steps` contains s2 and s3 (and no s1),
  - Direct/downstream tables match outputs of those steps.
- **add_step**: Add a step that produces `t_new` and is consumed by a downstream step; assert downstream closure includes the new consumer(s) and `touched` includes the new step.

### Selector legality (schema tests)

- `set_params` with `selector.table` (and no step_id/transform_id) → `E_AMEND_VALIDATION_SCHEMA`.
- `add_assertion` with `path` in selector → not applicable (add_assertion uses TableSelectorV1 which has no path); instead test e.g. `rewire_inputs` with `selector.path` → schema error.
- `rewire_inputs` (or another step op) with `selector.assertion_id` → `E_AMEND_VALIDATION_SCHEMA`.
- Optionally: `remove_step` / `replace_step` with `selector.table` only → `E_AMEND_VALIDATION_SCHEMA`.

### No-op

- `set_params` sets a leaf to the **same** value it already has (e.g. step with `params.assignments[0].expr.value == 5`, set to 5) → same hash → `E_AMEND_NO_OP`, status refused, no `ir_out`.

---

## Definition of done

- `diff_structural["affected"]` includes deterministic `touched`, `blast_radius_direct`, `blast_radius_downstream`; tests pin behavior.
- Selector legality enforced in schemas; tests pin schema refusals for disallowed selector combinations.
- No-op mutations refuse with `E_AMEND_NO_OP`; test pins it.
- All tests green; no workspace or runtime behavior change outside amendment kernel.

---

## File summary


| Area              | Files to change                                                                                                                                                                                                                                                                                                          |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Blast radius      | [sans/sans/amendment/diff.py](sans/sans/amendment/diff.py) (compute direct/downstream/touched); [sans/sans/amendment/apply.py](sans/sans/amendment/apply.py) (build `touched`, pass to diff)                                                                                                                             |
| Selector legality | [sans/sans/amendment/schemas.py](sans/sans/amendment/schemas.py) (per-op validators)                                                                                                                                                                                                                                     |
| No-op             | [sans/sans/amendment/errors.py](sans/sans/amendment/errors.py) (`E_AMEND_NO_OP`); [sans/sans/amendment/apply.py](sans/sans/amendment/apply.py) (hash check before building diff)                                                                                                                                         |
| Tests             | [tests/test_apply_amendment_success.py](tests/test_apply_amendment_success.py) or new test file for blast radius; [tests/test_amendment_schemas.py](tests/test_amendment_schemas.py) for selector legality; [tests/test_apply_amendment_refusals.py](tests/test_apply_amendment_refusals.py) for no-op (or success file) |


