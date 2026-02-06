# sans → cheshbon bundle contract (v0.1 strong)

## purpose

a `sans` run emits a **bundle** of JSON witnesses that allow `cheshbon` to deterministically:

* verify integrity (hashes + internal consistency)
* materialize a lineage graph
* promote a strong transform registry (content-addressed by semantics)

this contract is designed to be:

* deterministic across os (path normalization rules)
* explicit (no inference)
* strong enough to support registry diffs, impact analysis, and ledger provenance

---

## bundle layout

a bundle is a directory containing:

* `report.json` at bundle root (only file at root besides directory structure)
* `inputs/source/` — analysis script, preprocessed.sas (if any), expanded.sans (if any)
* `inputs/data/` — materialized datasource files (by logical name)
* `artifacts/` — plan.ir.json, schema.evidence.json, graph.json, vars.graph.json, table.effects.json, registry.candidate.json, runtime.evidence.json
* `outputs/` — user-facing table files (csv/xpt) from save step or emit

report and evidence must **never** contain paths outside the bundle; if any file would be outside, the run errors (no exceptions).

cheshbon ingests with:

```
cheshbon ingest sans --bundle <dir> --out <dir>/cheshbon
cheshbon verify --bundle <dir>
```

---

## canonical json definition

all ids and fingerprints rely on the same canonical json encoding:

* utf-8
* `json.dumps(..., separators=(",", ":"), sort_keys=True)`
* no extra whitespace
* stable list ordering is the order provided in the structure (unless explicitly stated otherwise)

---

## identity model (non-negotiable)

### transform identity (semantic, reusable)

**transform_id** identifies a transform purely by its semantics:

```
transform_payload = {
  "op": <op>,
  "params": <canonicalized params>
}

transform_id = sha256(canonical_json(transform_payload))
```

* MUST NOT include concrete table names (`inputs`, `outputs`)
* MUST NOT include file paths, row counts, hashes, timestamps, or `loc`

### transform class identity (structural, literal-agnostic)

**transform_class_id** identifies a transform by structure while ignoring literal values:

```
param_shape = params with every {"type":"lit","value":...} replaced by {"type":"lit","lit_type":"number|string|decimal|bool|null"}

transform_class_payload = {
  "op": <op>,
  "param_shape": <param_shape>
}

transform_class_id = sha256(canonical_json(transform_class_payload))
```

* MUST preserve column names, operator tokens, and AST structure
* MUST ignore literal values only (not column names or operator tokens)

### step identity (application, wiring-specific)

**step_id** identifies a specific *application* of a transform in a plan:

```
step_payload = {
  "transform_id": <transform_id>,
  "inputs":  [<logical table names>],
  "outputs": [<logical table names>]
}

step_id = sha256(canonical_json(step_payload))
```

* MAY include table logical names (because it is wiring-specific)
* MUST NOT include paths/hashes/row counts/timestamps

---

## file: `plan.ir.json`

represents the executed plan (semantic wiring + transform specs).

### required top-level

* `steps`: ordered list
* `tables`: list of input table logical names
* `table_facts`: optional, non-semantic hints (e.g. `sorted_by`)
* `datasources`: mapping of datasource name → `{path, columns, column_types?}` where `column_types` maps column name → type string (`null|bool|int|decimal|string|unknown`)

### step object

each element of `steps` MUST contain:

* `kind`: `"op"`
* `op`: string (e.g. `compute|filter|select|sort`)
* `params`: op-specific params (see below)
* `transform_id`: semantic id (see identity model)
* `transform_class_id`: structural id (see identity model; literal-agnostic)
* `inputs`: list of logical table names
* `outputs`: list of logical table names
* `step_id`: application id (see identity model)
* `loc`: optional `{file, line_start, line_end}` (non-semantic; for trace/debug only)

#### example (based on your current plan shape, but upgraded)

your current plan uses `step_id` as the only id and includes wiring in the hash; this contract requires both ids and changes `step_id` definition. your current step format resembles this: 

---

## file: `registry.candidate.json`

a candidate transform registry for the run; strong registry entries include `spec`.

### required top-level

* `registry_version`: string
* `transforms`: list of transform entries
* `index`: mapping of `step_index` → `transform_id`

### transform entry

* `transform_id`: string (sha256 hex) — MUST match plan steps’ `transform_id`
* `kind`: string (e.g. `op.compute`, `op.filter`, or `compute`)
* `version`: string (optional; default `"0.1"`)
* `spec`: REQUIRED; canonical semantic spec, identical in content to the hashed payload:

```
spec = {
  "op": <op>,
  "params": <canonicalized params>
}
```

* optional `io_signature`: role-level only, not concrete table names (e.g. `["table"] -> ["table"]`)
* optional `impl_fingerprint`: allowed but non-semantic; MUST NOT be used for transform identity

**IMPORTANT:** your current candidate registry is weak (builtin ref + generic signature) ; under this contract it must carry `spec`.

---

## file: `runtime.evidence.json`

execution-time witness data for integrity, not semantics.

### required top-level

* `sans_version`: string
* `plan_ir`: `{path, sha256}` where `sha256` is raw bytes hash of `plan.ir.json`
* `bindings`: mapping of logical input table name → path
* `inputs`: list of input table evidence
* `outputs`: list of output table evidence
* `step_evidence`: list of per-step evidence objects (preferred) OR dict keyed by step_index (allowed only if you choose; but pick one and standardize—v0.1 recommends list)
* `tables`: mapping of saved output table name -> table evidence (see below)

note: runtime evidence MUST be deterministic and environment-blind; timestamps, UUIDs, and host-specific data MUST NOT be included.


### table evidence object

for both `inputs[]` and `outputs[]`:

* `name`: logical table name (string)
* `path`: path to the table file (string)
* `format`: e.g. `csv|xpt`
* `bytes_sha256`: raw bytes hash of the file
* `canonical_sha256`: canonical content hash (if defined for that format)

for outputs, also:

* `row_count`: integer
* `columns`: list of strings

### tables: per-table runtime evidence (saved outputs only)

`tables` is a mapping of **saved output table name** to a table evidence object:

```
tables[table_name] = {
  "row_count": <int>,
  "columns": {
    <column_name>: {
      "null_count": <int>,
      "non_null_count": <int>,
      "unique_count": <int | ">=N">,
      "unique_count_capped": <bool>,
      "constant_value": <scalar> (only when unique_count == 1 and null_count == 0),
      "top_values": [{ "value": <scalar>, "count": <int> }, ...] (optional),
      "type_hint": "string|int|decimal|bool|null|unknown" (optional)
    }
  },
  "sample": { "strategy": "stride", "cap": <int>, "size": <int>, "step": <int> } (optional)
}
```

* `tables` only includes outputs written via **save** (not implicit terminal tables).
* all values are **runtime evidence** of what happened; **plan.ir literals are not treated as runtime value evidence**.
* when datasets are large, evidence MAY be computed on a deterministic sample; if so, `sample` MUST be present.

### typed CSV coercion diagnostics (optional)

When typed CSV ingestion fails, `runtime.evidence.json` includes:

```
coercion_diagnostics = [
  {
    "datasource": "<name>",
    "path": "<bundle-relative path>",
    "total_rows_scanned": <int>,  # 1-based data rows, header excluded
    "truncated": <bool>,
    "columns": [
      {
        "column": "<col>",
        "expected_type": "null|bool|int|decimal|string|unknown",
        "failure_count": <int>,
        "sample_row_numbers": [<int>, ...],  # first N row numbers (ascending)
        "sample_raw_values": ["<raw>", ...], # first N distinct tokens (trimmed)
        "failure_reason": "invalid_int|invalid_decimal|invalid_bool|unexpected_empty|mixed"
      }
    ]
  }
]
```


### step evidence object (list form, recommended)

each element MUST include:

* `step_index`: int
* `step_id`: string
* `transform_id`: string
* optional: `op` (string)
* optional: `row_counts`: mapping of logical output table name → row_count
* optional: warnings/errors diagnostics

note: intermediates MAY omit bytes/canonical hashes if not materialized, but row_counts are strongly recommended.

your current evidence example uses dict step_evidence keyed by `"0"` ; the contract recommends list-of-objects for ease of extension.

---

## path normalization

* all stored paths in evidence and emitted cheshbon artifacts MUST use forward slashes `/`
* cheshbon MUST normalize incoming windows paths `\` to `/` for storage and hashing
* paths are non-semantic and MUST NOT influence transform_id or step_id

---

## cheshbon outputs

cheshbon ingests the bundle and emits:

### `cheshbon/graph.json` (format `cheshbon.graph`, version `0.1`)

* nodes:

  * `id`: `table:<logical_name>` (v0.1)
  * `name`: logical name
  * `evidence`: from runtime evidence if present
* edges:

  * `id`: deterministic hash of `{step_id, transform_id, inputs, outputs}` (or equivalent)
  * `transform_id`: semantic transform id (NOT step id)
  * `step_id`: application id
  * `inputs`: node ids
  * `outputs`: node ids

your current graph uses `transform_id` values that are actually step hashes (`t:<old_step_id>`) ; this contract corrects that.

### `cheshbon/registry.json` (format `cheshbon.registry`, version `0.1`)

* promoted copy of `registry.candidate.json` with `spec` preserved
* must not include concrete table wiring in transform signatures (that belongs in the graph)

your current promoted registry stores concrete wiring in `signature` and has no `spec` ; this contract forbids that and requires `spec`.

### `cheshbon/run.json` (format `cheshbon.run`, version `0.1`)

* includes `run_id`, `created_at`
* includes `witnesses`: raw sha256 for the three witness JSON files
* includes `fingerprint`: sha256 of canonical json over:

  * plan sha256
  * ordered list of `(step_id, transform_id)`
  * input canonical hashes keyed by logical name
  * output canonical hashes keyed by logical name

your current run.json has witnesses and a fingerprint field ; update fingerprint definition to incorporate step_id/transform_id and both input/output hashes.

---

## verification rules (cheshbon verify)

cheshbon MUST fail verification if any of the following hold:

1. `sha256(plan.ir.json bytes) != runtime.evidence.plan_ir.sha256`
2. for any step index `i`: missing `registry.index[i]`
3. for any step `i`: `registry.index[i] != plan.steps[i].transform_id`
4. for any step `i`: `plan.steps[i].step_id != sha256(canon({transform_id, inputs, outputs}))`
5. for any table evidence entry: referenced file missing OR hash mismatch
6. any transform_id referenced by `registry.index` is absent from `registry.transforms`
7. any registry transform missing `spec`

---

## op params (semantics)

cheshbon treats `op` and `params` as opaque except for canonicalization. examples (from your current plan shape) :

* compute:

  * `params.assign = [{col, expr(AST)}]`
* filter:

  * `params.predicate = expr(AST)`
* select:

  * `params.keep`, `params.drop`
* sort:

  * `params.by = [{col, asc}]`

expression ast is part of semantics and MUST be preserved in `spec.params`.

---

## migration note (since you said no compat)

this contract intentionally breaks the earlier step-id scheme where ids hashed inputs/outputs/params together. going forward:

* `step_id` becomes the application id
* `transform_id` becomes the semantic id
* registry becomes strong by construction

---
