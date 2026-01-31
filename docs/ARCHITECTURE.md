# sans architecture — master plan (v0.1)

## purpose

sans is a small, deterministic batch execution engine for tabular transformation scripts, designed to run a sharply bounded set of operations with high integrity: it either (a) produces correct, explainable outputs or (b) refuses to run with precise diagnostics. it does not “mostly work.” it is a compiler + validator + executor wrapped in a single executable.

## guiding doctrine

* **truth over coverage**: unsupported constructs halt; approximate behavior is opt-in and labeled.
* **parse != execute**: data is untouched until the whole script is understood and validated.
* **determinism by construction**: ordering, null behavior, type coercions, tolerances are explicit.
* **artifacts are the product**: plan, report, and outputs are hashable, diffable, reviewable.
* **multi-table is first-class**: every step names explicit input and output tables; hidden “current table” state is forbidden (parser convenience can compile away).

## non-goals

* full compatibility with any legacy system
* macro languages, dynamic code execution, user-defined functions
* database/network connectors, interactive repl
* plots, charts, exploration
* “helpful” guesses (formats, keys, semantics). guessing is how you become wrong.

---

## system overview

sans has a single pipeline with hard stage boundaries:

1. **ingest**: read script + optional symbol table
2. **normalize**: strip comments, split into statements, preserve source locations
3. **segment**: group statements into blocks (`data ... run;`, `proc ... run;`, other)
4. **recognize/compile**: pattern-compile each block into a flat IR step list *or* an unknown-block refusal
5. **validate**: build facts, verify invariants, refuse unsafe plans
6. **execute**: run IR steps against a dataframe engine
7. **emit**: outputs + execution receipt + diagnostics

the entire point is that stage (5) is where lies go to die.

---

## internal architecture

a “structured monolith” (single binary) with four packages:

### 1) `sans.ingest`

* loads text with explicit encoding policy
* optionally applies single-pass variable substitution (`&NAME` etc) using a provided `symbols.macros` map
* **detects** macro/control-flow constructs and refuses early (you are not building a macro interpreter today)

outputs:

* `SourceText` with raw content and file metadata (path, hash if available)

### 2) `sans.frontend` (normalizer + segmenter + recognizer)

**2.1 normalizer**

* removes block comments `/* ... */` and simple statement comments `* ... ;` (conservatively)
* splits into **statements** by semicolon outside quotes
* produces: `Statement{text_norm, loc{file,line_start,line_end}}[]`

**2.2 segmenter**

* identifies blocks:

  * `data <out>; ... run;`
  * `proc <name>; ... run;`
  * everything else as single-statement blocks
* produces: `Block{kind, header, body[], end?, loc_span}`

**2.3 recognizer / pattern compiler**

* applies a library of recognizers (ordered by specificity)
* each block compiles to either:

  * `Step(kind="op", ...)[]` or
  * `Step(kind="block", severity=fatal, reason=...)`
* no partial compilation of a block: either the whole block is understood within the supported subset, or it becomes one refusal unit with a clean span.

this is how you “reduce false positives dramatically”: you do not run fragments.

### 3) `sans.ir`

the IR is the contract artifact. it is deliberately flat:

* top-level metadata (engine version, settings, sources)
* `tables{ name -> origin + schema_hint }`
* `steps[]` where each step has:

  * `id`, `loc`, `kind`
  * for `op`: `op`, `inputs[]`, `outputs[]`, `params{}`, `soundness`, `effects{}`
  * for `block`: `reason{code,message,tokens}`, `severity`, `raw_excerpt`
* optional `assertions[]`, `diagnostics[]`

the IR must be:

* serializable as stable json
* suitable for hashing
* sufficiently explicit that a validator can reason about it without executing it

### 4) `sans.validator`

the validator is the bouncer with a rulebook. it never “tries anyway.”

**facts tracked per table**

* schema (known/unknown; columns + dtypes when known)
* sortedness (`is_sorted_by`)
* uniqueness hints (`keys_unique`)
* provenance (`produced_by_step`, source file)

**invariants enforced**

* tables must exist before use (`SANS_VALIDATE_TABLE_UNDEFINED`)
* columns referenced must exist when schema is known (`SANS_VALIDATE_COLUMN_UNDEFINED`)
* outputs must not collide (`SANS_VALIDATE_OUTPUT_TABLE_COLLISION`)
* order-dependent ops require established order (`SANS_VALIDATE_ORDER_REQUIRED`)
* join/compare/dedup require explicit keys (`SANS_VALIDATE_KEYS_REQUIRED`)
* merge/join require explicit duplicate policy (`SANS_VALIDATE_DUP_POLICY_REQUIRED`)
* strict mode halts on any fatal unknown block or unsupported step (`SANS_CAP_UNSUPPORTED_FEATURE`)

**soundness policy**

* `sound`: ok
* `approx`: allowed only with `--allow-approx`, and run is marked as approximate
* `unsupported`: always refusal in strict mode

validator output:

* either `ValidatedPlan` (IR + derived facts) or a refusal report (stable error payloads)

### 5) `sans.runtime`

execution is intentionally boring:

* iterate `steps[]`
* dispatch each `op` to an engine adapter (polars-first recommended)
* never executes `block` steps
* emits structured runtime events: row counts, schema diffs, timings, warnings

**engine adapter responsibilities**

* enforce stable sort and explicit null ordering
* compile expression AST into native engine expressions (do not `eval`)
* manage type casting rules explicitly (including failure behavior)
* provide “guard rails” ops (optional):

  * uniqueness checks
  * join explosion detection
  * schema expectation checks

### 6) `sans.emit`

always produces:

* outputs (files)
* a **receipt artifact** (execution report) with:

  * engine versions
  * script + input hashes
  * settings (tolerance, null semantics, sort semantics)
  * list of steps executed / refused
  * per-step metrics (optional)
  * output hashes

this makes the run reproducible and auditable. it’s also catnip for any higher-level orchestration system.

---

## supported surface area (v0.1)

### operations (IR `op` names)

* `load`, `save`
* `select`, `rename`, `cast`
* `compute` (expression AST)
* `filter` (predicate AST)
* `sort` (stable, null order explicit)
* `group_agg` (means/freq-like)
* `join` (strict keys + dup policy)
* `concat_rows`
* `deduplicate`
* `compare` (keys required; explicit tolerance)

### sas recognizer coverage (v0.1)

* `proc sort` (strict option allowlist)
* data step `set` with:

  * assignments, keep/drop/rename, filter-only `if`, limited `if-then-else` assignment
  * dataset options on inputs: `keep/drop/rename/where`
* stateful data step subset:

  * `set`/`merge` with `by`, `retain`, `first./last.`, `output`, `if/then/else`, `keep`, and `in=` flags
* `proc transpose` (by/id/var)
* `proc compare` with `id` required
* `proc means` basic stats via `class/var` (strict allowlist)
* `proc freq` minimal (one-way) if you must; otherwise defer

hard-fail tokens/constructs (v0.1):

* macro language (`%macro`, `%do`, `%if`, `%include`, etc.)
* `proc sql`
* data step features still unsupported: `lag`, `do/end` blocks, arrays
* io-ish statements: `infile`, `input`, `put`, `call execute`

the refusal is a feature, not a bug.

---

## expression system

no python eval. no “simple string to runtime.” expressions are ASTs.

**minimum AST nodes**

* literals, column refs
* unary `+/-/not`
* binary arithmetic, comparisons
* boolean `and/or`
* whitelisted calls (small set): `coalesce`, `if`, `substr`, `upper/lower/strip`, strict `parse_date` if you choose

**type policy**

* explicit casting step for nontrivial coercions
* missing numeric `.` maps to null in sas ingestion; empty string is not null by default
* any ambiguous date parsing is refused unless an explicit format is provided

---

## determinism policy (non-negotiable)

* sort is stable, and null ordering is explicit
* group/agg output ordering is specified (either sorted by keys or explicitly preserved; pick one and enforce)
* compare requires keys; tolerance is explicit and limited to compare unless otherwise requested
* any op that depends on row order must be explicit in IR; if not representable, refuse

---

## cli and modes

### `sans check <script>`

* runs ingest → compile → validate only
* emits:

  * `plan.ir.json` (even if refused, includes block steps)
  * `report.json` (diagnostics, stable error payloads)
* exit codes reflect refusal category

### `sans run <script> --inputs ... --out ...`

* performs full pipeline
* emits:

  * outputs
  * `report.json` (receipt)
  * optional `plan.ir.json` if you want to preserve the plan used

flags worth having early:

* `--strict/--no-strict` (default strict)
* `--allow-approx`
* `--macros KEY=VAL` (or file) for trivial substitution
* `--tolerance abs=...` for compare
* `--emit-plan` / `--emit-map`

---

## stable errors: contract-grade diagnostics

errors are stable strings (machine-parseable), with a structured payload:

* `code`, `message`, `loc`, `tokens[]`, `hint`

exit codes are coarse buckets; the string code is the real contract.

recommended exit codes:

* `0` ok
* `10` ok w/ warnings
* `20` ok w/ approx
* `30` parse/recognition refusal
* `31` validation refusal
* `32` capability refusal (unsupported under strict)
* `40` runtime/data error
* `50` internal error

example refusal payload:

```json
{
  "code": "SANS_BLOCK_STATEFUL_TOKEN",
  "message": "lag/do/end/array not supported in v0.1; refusing to execute",
  "loc": { "file": "script.sas", "line_start": 210, "line_end": 289 },
  "tokens": ["lag"]
}
```

---

## testing strategy (how it stays rock solid)

### conformance suite (owned)

you do not rely on “public scripts” as your truth. you build a conformance suite that encodes:

* expression semantics
* null rules
* ordering guarantees
* join duplicates policy
* compare tolerance behavior
* parser/recognizer coverage of supported forms
* refusal behavior for forbidden constructs

### golden fixtures (curated)

a smaller set of real-world-ish scripts with known outputs, used for regression.

### invariants as tests

validator rules are tests. every new feature adds:

* at least one “should run” fixture
* at least one “should refuse” fixture with stable error code

---

## integration posture (external orchestrator / higher-level system)

sans is happiest as a subordinate engine:

* input: IR plan + pointers to data
* output: result tables + receipt + diagnostics
* higher layer handles:

  * artifact versioning
  * diffing plans
  * impact analysis across plans
  * review workflow for unknown blocks / approximations
  * provenance and audit trails

sans remains deliberately small and deterministic; the orchestrator remains the place where “bigger workflows” live.

---

## roadmap (disciplined, not delusional)

**v0.1**

* IR + schema + stable errors
* recognizer: sort + simple data-step set + compare (keys) + minimal means
* validator facts/invariants
* executor on polars
* receipts + hashes

**v0.2**

* strict join/merge subset (1:1 only) with explicit assertions for sortedness/uniqueness
* broader proc means/freq coverage (still strict allowlists)
* optional “plan map” artifact (source loc → IR step ids)

**v0.3+ (only if demanded)**

* explicit by-group boundaries as IR ops (if you want to touch first./last. honestly)
* more date/time utilities (still explicit formats)
* never: full macro system, full sas compatibility theater

---

## what the agent should build today

if you’re putting an agent on it immediately, don’t ask it to “build sans.” ask it to build **the spine**:

1. implement statement splitting with loc mapping (semicolon outside quotes)
2. implement block segmentation
3. implement IR emitter + json schema validation in tests
4. implement recognizer for `proc sort` + simple `data/set` (assign/keep/drop/filter-only)
5. implement validator rules (table defined, columns defined when known, strict refusal)
6. implement `sans check` producing `plan.ir.json` + `report.json`

once that exists, everything else is just adding operations, not inventing architecture mid-flight like a clown.
