good. those four decisions remove 80% of future ambiguity.

below is the tightened v0.1 spec, incorporating:

* `assertion_id` required and stable
* `selector.path` is json pointer **relative to `step.params`**
* `add_step` output table collision = default refusal
* strict pydantic schemas everywhere (no garbage smuggling)

---

# amendment_request v1 (contract-aligned, tightened)

## 1) top-level

```json
{
  "format": "sans.amendment_request",
  "version": 1,
  "contract_version": "0.1",
  "meta": {
    "request_id": "optional",
    "note": "optional"
  },
  "policy": {
    "allow_destructive": false,
    "allow_output_rewire": false,
    "allow_approx": false,
    "max_ops": 50
  },
  "ops": [ ... ]
}
```

### rules

* unknown top-level keys forbidden (`extra="forbid"`).
* `policy.max_ops` may be lower than kernel cap; kernel enforces `min(policy.max_ops, HARD_CAP)`.
* `meta` is non-semantic; never influences ids/hashes.

---

## 2) selector (v0.1)

selectors are strict. no patterns. no fuzzy matching.

```json
{
  "step_id": "optional",
  "transform_id": "optional",
  "table": "optional",
  "assertion_id": "optional",
  "path": "optional json pointer relative to step.params"
}
```

### resolution invariants

* for step-targeting ops: must resolve to **exactly one step**.
* if both `step_id` and `transform_id` provided, they must resolve to the same step else `E_AMEND_TARGET_MISMATCH`.
* `table` as sugar is allowed only when it resolves unambiguously (else `E_AMEND_TARGET_AMBIGUOUS`).
* `path` semantics:

  * json pointer (rfc6901) relative to `step.params`
  * `/` points at the params root object
  * kernel refuses if `path` missing when required
  * kernel refuses if `path` does not exist (`E_AMEND_PATH_NOT_FOUND`)
  * v0.1 forbids “create missing” writes

---

## 3) assertion identity (hard rule)

every assertion in `sans.ir` has:

* `assertion_id: string` (stable within the ir; unique)
* `type: ...` (discriminant)
* `severity: "warn"|"fatal"` (or whatever your existing severity enum is)

mutation ops addressing assertions MUST use `assertion_id` for remove/replace. no “remove by shape”.

---

# op schemas (v0.1)

each op is a discriminated union on `kind`. unknown kind => `E_AMEND_CAPABILITY_UNSUPPORTED`.

## base op envelope

```json
{
  "op_id": "required unique string",
  "kind": "…",
  "selector": { … },
  "params": { … }
}
```

`op_id` uniqueness is enforced at validation time.

---

## a) add_step

```json
{
  "op_id": "op1",
  "kind": "add_step",
  "selector": {
    "before_step_id": "optional",
    "after_step_id": "optional",
    "index": 12
  },
  "params": {
    "step": {
      "kind": "op",
      "op": "compute",
      "inputs": ["t_in"],
      "outputs": ["t_out"],
      "params": { ... },
      "soundness": "sound"
    }
  }
}
```

rules:

* exactly one of `before_step_id|after_step_id|index` must be present.
* outputs collision: if any `outputs[]` already exist as a table name in the ir, refuse with `E_AMEND_OUTPUT_TABLE_COLLISION` (default; no override in v0.1).
* kernel recomputes ids; request may not supply step_id/transform_id.

---

## b) remove_step (destructive gate)

```json
{
  "op_id": "op2",
  "kind": "remove_step",
  "selector": { "step_id": "…" },
  "params": {}
}
```

rules:

* requires `policy.allow_destructive=true` else `E_AMEND_POLICY_DESTRUCTIVE_REFUSED`.

---

## c) replace_step

```json
{
  "op_id": "op3",
  "kind": "replace_step",
  "selector": { "step_id": "…" },
  "params": {
    "op": "compute",
    "params": { ... },
    "preserve_wiring": true
  }
}
```

rules:

* if `preserve_wiring=true`, request cannot change inputs/outputs.
* if `preserve_wiring=false`, then:

  * input rewires are allowed
  * output rewires require `policy.allow_output_rewire=true` else `E_AMEND_POLICY_OUTPUT_REWIRE_REFUSED`

---

## d) rewire_inputs

```json
{
  "op_id": "op4",
  "kind": "rewire_inputs",
  "selector": { "step_id": "…" },
  "params": { "inputs": ["new_in"] }
}
```

---

## e) rewire_outputs (policy gate)

```json
{
  "op_id": "op5",
  "kind": "rewire_outputs",
  "selector": { "step_id": "…" },
  "params": { "outputs": ["new_out"] }
}
```

rules:

* requires `policy.allow_output_rewire=true` else refusal.

---

## f) rename_table

```json
{
  "op_id": "op6",
  "kind": "rename_table",
  "selector": { "table": "old" },
  "params": { "new_name": "new" }
}
```

rules:

* refuse if `old` missing or `new` exists.

---

## g) set_params (path write)

```json
{
  "op_id": "op7",
  "kind": "set_params",
  "selector": { "step_id": "…", "path": "/assign/0" },
  "params": { "value": { ... } }
}
```

rules:

* `path` required.
* path is relative to `step.params`.
* value must validate against the op-specific param schema post-write; else `E_AMEND_IR_INVALID`.

---

## h) replace_expr

```json
{
  "op_id": "op8",
  "kind": "replace_expr",
  "selector": { "step_id": "…", "path": "/predicate" },
  "params": { "expr": { ...ast... } }
}
```

rules:

* `path` must point to an expr-typed location per schema; otherwise `E_AMEND_PATH_INVALID`.
* expr ast is typed and validated; no string expressions.

---

## i) edit_expr (typed micro-edits)

```json
{
  "op_id": "op9",
  "kind": "edit_expr",
  "selector": { "step_id": "…", "path": "/predicate/right" },
  "params": {
    "edit": "replace_literal",
    "literal": { "lit_type": "number", "value": 15 }
  }
}
```

v0.1 edits allowlist:

* `replace_literal`
* `replace_column_ref`
* `replace_op` (bounded operator enum)
* `wrap_with_not`

anything else => unsupported.

---

## j) add_assertion

```json
{
  "op_id": "op10",
  "kind": "add_assertion",
  "selector": { "table": "t_out" },
  "params": {
    "assertion": {
      "assertion_id": "a_001",
      "type": "unique_key",
      "columns": ["id"],
      "severity": "fatal"
    }
  }
}
```

rules:

* `assertion_id` required; must be unique.
* if your existing system wants kernel-generated ids, fine, but then require either:

  * `assertion_id` supplied by caller (recommended, simplest), or
  * `assertion_id` omitted but `op_id` is promoted into deterministic id. (not doing this in v0.1 unless you insist.)

---

## k) remove_assertion

```json
{
  "op_id": "op11",
  "kind": "remove_assertion",
  "selector": { "assertion_id": "a_001" },
  "params": {}
}
```

policy note:

* you can treat this as destructive and require `allow_destructive`; i recommend yes (default refuse), but it’s your call.

---

## l) replace_assertion

```json
{
  "op_id": "op12",
  "kind": "replace_assertion",
  "selector": { "assertion_id": "a_001" },
  "params": {
    "assertion": { "assertion_id": "a_001", "type": "...", ... }
  }
}
```

rule:

* selector assertion_id must match payload assertion_id else mismatch refusal.

---

# strict pydantic modeling (recommended layout)

## core models

* `AmendmentRequestV1`

  * `policy: AmendmentPolicy`
  * `ops: list[AmendOp]` where `AmendOp` is a discriminated union on `kind`
  * `extra="forbid"`

* `Selector`

  * forbid unknown keys
  * custom validator: disallow empty selector where required by op type

* `OpStepDraft`

  * validated against the same step schema used in `sans.ir` (minus ids)

* `ExprAst` union

  * literals, refs, unary, binary, call (allowlist), etc.

* `AssertionSpec` union

  * includes required `assertion_id`

## validation passes

1. pydantic schema validation -> `E_AMEND_VALIDATION_SCHEMA`
2. `op_id` uniqueness + `policy.max_ops` caps -> `E_AMEND_CAPABILITY_LIMIT`
3. selector resolution (requires `ir_in`) -> target errors
4. apply ops sequentially (in-memory)
5. post-mutation ir validation -> `E_AMEND_IR_INVALID`

atomic: any failure aborts; no partial output.

---

# new refusal codes to add (since you chose collision refusal)

* `E_AMEND_OUTPUT_TABLE_COLLISION`
* `E_AMEND_ASSERTION_ID_COLLISION`
* `E_AMEND_ASSERTION_ID_REQUIRED`
* `E_AMEND_POLICY_ASSERTION_REMOVAL_REFUSED` (if you gate removals)



## deltas (v0.1 pinned)

* `remove_assertion` is **destructive** ⇒ requires `policy.allow_destructive=true`, else `E_AMEND_POLICY_DESTRUCTIVE_REFUSED`.
* `add_step.selector.index` is **0-based**.

## final addenda to the spec

### remove_assertion (final)

```json
{
  "op_id": "op11",
  "kind": "remove_assertion",
  "selector": { "assertion_id": "a_001" },
  "params": {}
}
```

rule:

* if `policy.allow_destructive != true` ⇒ refusal `E_AMEND_POLICY_DESTRUCTIVE_REFUSED`.

### add_step insertion (final)

* `index` is 0-based into `ir.steps[]`
* valid range for insert:

  * `0 <= index <= len(steps)` (insert-at-end allowed)
* out of range ⇒ `E_AMEND_INDEX_OUT_OF_RANGE` (or fold into schema error; i recommend a specific code)

## new refusal code (recommend)

* `E_AMEND_INDEX_OUT_OF_RANGE`
