# mutation_contract.md (v0.1)

## purpose

define the **only** allowed way to mutate a pipeline: a deterministic, refusal-first transformation from `sans.ir` → `sans.ir'`, with stable diagnostics and machine-readable diffs. this contract is the agent-facing “edit api.”

## scope

* **input surface:** `sans.ir` only. `plan.ir` is **witness-only** and **never accepted** as mutation input.
* mutation is **ir-to-ir**, deterministic, side-effect free.
* no “helpful” inference. ambiguity is refusal.

## terms

* **intent ir (`sans.ir`)**: authoritative plan-of-intent; allowed as input; mutable via contract.
* **witness ir (`plan.ir`)**: executed plan witness; output-only; used for verification/audit; immutable.
* **amendment**: structured request describing one or more edits to `sans.ir`.
* **target**: the specific ir element(s) the amendment acts on.
* **selector**: the mechanism for identifying targets (ids or symbolic references).
* **refusal**: deterministic rejection with stable error code + payload.

## required invariants

all successful mutations MUST preserve:

1. **determinism invariants**

   * ids computed only from canonical json of semantic content (no timestamps, paths, host info).
   * stable ordering rules preserved (sort/null semantics remain explicit).
2. **explicitness**

   * no implicit “current table” or hidden state introduced by mutation.
   * all rewiring is explicit in ir.
3. **soundness discipline**

   * if mutation introduces `approx`, it must be explicit and gated by policy (kernel can refuse by default).
4. **refusal over guessing**

   * ambiguous targets, ambiguous column refs, ambiguous rewires => refusal.

## mutation pipeline (kernel primitive)

mutation is defined as this pure function:

`apply_amendment(ir_in: sans.ir, amendment: AmendmentRequest) -> MutationResult`

where:

* `MutationResult` contains either:

  * `status="ok"` with `ir_out` + diff artifacts, or
  * `status="refused"` with stable error payload(s)

no file io. no network. no randomness.

## selectors and targeting

mutation targets MUST be resolved deterministically. resolution MUST fail (refuse) if:

* a selector matches 0 targets (unless op explicitly allows “create if missing”)
* a selector matches >1 target and the op is not explicitly multi-target
* resolution depends on runtime data

### primary selectors (preferred)

* `step_id` (application identity)
* `transform_id` (semantic identity)

these are unambiguous and stable under reformatting.

### secondary selectors (allowed as sugar)

* `table_name`
* `column_name`

secondary selectors MUST resolve to ids during mutation and are subject to ambiguity refusal (e.g., column exists in multiple tables in scope).

### selector resolution order

if multiple selectors are provided for the same op, all MUST agree; otherwise refusal.

example: specifying both `step_id` and `table_name` must point to the same resolved step; mismatch => refusal.

## mutation operations (v0.1 allowlist)

mutation is an allowlist. anything not listed is unsupported (capability refusal).

### structural ops

* **add_step**

  * adds a new step with explicit inputs/outputs and params
  * requires explicit insertion point (before/after a step_id) or explicit index
* **remove_step**

  * removes a step (requires policy gate; default refuse unless `allow_destructive=true`)
* **replace_step**

  * replaces a step’s transform spec while preserving wiring (inputs/outputs unchanged) unless explicitly allowed
* **rewire_inputs**

  * change a step’s `inputs[]`
* **rewire_outputs**

  * change a step’s `outputs[]` (high risk; policy-gated)
* **rename_table**

  * renames a logical table; updates references across steps

### param/expr ops

* **set_params**

  * sets specific op params by path (json pointer-like)
* **replace_expr**

  * replaces a whole expression ast at a specified param path
* **edit_expr**

  * small, typed edits to ast nodes (e.g. replace literal, replace column ref)
  * must preserve ast validity; no stringly-typed eval

### assertion ops

* **add_assertion**
* **remove_assertion**
* **replace_assertion**
* **set_assertion_policy**

  * e.g., severity / enforcement mode if your assertion model supports it

## hard prohibitions

mutation MUST refuse if it would:

* introduce dynamic execution (`eval`, macro expansion, runtime codegen)
* introduce references to unknown tables/columns when schema is known and required
* weaken determinism guarantees (e.g., unspecified sort tie behavior)
* change identity computation rules
* silently coerce types without explicit cast op
* introduce implicit ordering dependencies without explicit sortedness facts/assertions

## validation stages

mutation processing has three stages:

1. **schema validation (request shape)**

   * amendment request parses, discriminated unions resolve, caps enforced.
   * errors here are `E_AMEND_VALIDATION_*` style (stable).
2. **target resolution**

   * resolve selectors to concrete ir entities.
   * ambiguity => refusal.
3. **post-mutation ir validation**

   * validate the mutated `sans.ir'` using the same invariants as compile-time validation:

     * tables exist before use
     * columns exist when schema known
     * outputs don’t collide
     * join/compare require keys
     * order-dependent ops require explicit order facts/assertions
   * if validation fails, the mutation is refused and `ir_out` is not emitted.

## outputs (mutation result artifact contract)

on success, mutation returns:

* `ir_out` (`sans.ir'`)
* `diff.structural.json` (required)
* `diff.assertions.json` (required; may be empty)
* `diagnostics.json` (required; warnings allowed)

### diff.structural.json (minimum)

must include:

```json
{
  "format": "sans.mutation.diff.structural",
  "version": 1,
  "base_ir_sha256": "<hex>",
  "mutated_ir_sha256": "<hex>",
  "ops_applied": [
    { "op_id": "...", "kind": "replace_expr", "target": { ... }, "status": "ok" }
  ],
  "affected": {
    "steps": ["<step_id>", "..."],
    "tables": ["<name>", "..."],
    "transforms_added": ["<transform_id>", "..."],
    "transforms_removed": ["<transform_id>", "..."],
    "transforms_changed": [
      { "before": "<transform_id>", "after": "<transform_id>" }
    ]
  }
}
```

notes:

* `base_ir_sha256` and `mutated_ir_sha256` are hashes of canonical json of `sans.ir`.
* `transforms_changed` is semantic; it must reflect semantic id changes, not formatting.

### diff.assertions.json (minimum)

```json
{
  "format": "sans.mutation.diff.assertions",
  "version": 1,
  "added": [ ... ],
  "removed": [ ... ],
  "modified": [
    { "before": { ... }, "after": { ... } }
  ]
}
```

### diagnostics.json (minimum)

```json
{
  "format": "sans.mutation.diagnostics",
  "version": 1,
  "status": "ok|refused",
  "refusals": [
    { "code": "...", "message": "...", "loc": { ... }, "hint": "...", "meta": { ... } }
  ],
  "warnings": [
    { "code": "...", "message": "...", "loc": { ... }, "meta": { ... } }
  ]
}
```

## refusal codes (contract-grade)

refusals MUST use stable string codes. exit codes are coarse; string codes are the contract.

minimum set (v0.1):

* `E_AMEND_VALIDATION_SCHEMA` (request doesn’t parse / violates schema)
* `E_AMEND_CAPABILITY_UNSUPPORTED` (op not in allowlist / feature gated)
* `E_AMEND_TARGET_NOT_FOUND`
* `E_AMEND_TARGET_AMBIGUOUS`
* `E_AMEND_TARGET_MISMATCH` (multiple selectors disagree)
* `E_AMEND_POLICY_DESTRUCTIVE_REFUSED` (remove/rewire outputs blocked)
* `E_AMEND_IR_INVALID` (post-mutation ir validation failed)
* `E_AMEND_IR_INVARIANT_BREACH` (determinism/identity/soundness invariant would be violated)

## versioning

* this contract is versioned independently: `sans.mutation.contract = 0.1`.
* `amendment_request` must declare `contract_version`.
* any breaking change increments minor at least; ideally semver.

## canonicalization requirements

all hashing and identity comparisons MUST use:

* utf-8
* json canonical encoding: `sort_keys=true`, separators `(",", ":")`
* stable list ordering as stored
* paths and `loc` are non-semantic and MUST NOT affect semantic ids

## success criteria

a mutation system is “correct” when:

* same `sans.ir` + same amendment_request => byte-identical `sans.ir'` and diffs
* ambiguity always refuses
* post-mutation ir is always valid or refused
* no mutation can smuggle nondeterminism or hidden state
