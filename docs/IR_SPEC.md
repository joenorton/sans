## IR_SPEC (semantics)

Purpose
- Define what IR fields mean and what invariants are assumed.
- Make validation deterministic and auditable.

Core semantics
- `steps[]` is an ordered plan; no hidden state or implicit tables.
- `inputs[]` and `outputs[]` are explicit; each op must declare its outputs.
- Tables only exist if produced by a prior step or predeclared by the caller.
- Canonical IR is the single representation for execution and auditing. SAS and other frontends may lower to canonical ops; cheshbon-complete and determinism guarantees apply only once the plan is in canonical IR. Legacy param shapes are not accepted in the nucleus; see IR_CANON_RULES.md.

Unknown blocks
- Represented as `UnknownBlockStep` with `kind="block"`.
- Any unknown block is a fatal refusal in strict mode.
- Unknown blocks should not be executed; they exist only for diagnostics.

Location (`loc`) semantics
- `loc` spans the source lines that justify the step or refusal.
- For data steps, all emitted ops may use the block span (`data ...` through `run;`).
- Whole-block refusals should use the data-step block span.

Expression AST (v0.1)
- Expression nodes are JSON-serializable dicts.
- Core node shapes:
  - `{"type":"lit","value":...}`
  - `{"type":"col","name":"..."}`
  - `{"type":"binop","op":"...","left":...,"right":...}`
  - `{"type":"boolop","op":"and|or","args":[...]}`
  - `{"type":"unop","op":"not|+|-","arg":...}`
  - `{"type":"call","name":"coalesce|if|put|input","args":[...]}`

Data step op (stateful subset)
- Op name: `data_step`
- Params:
  - `mode`: `"set"` or `"merge"`
  - `inputs`: list of `{table, in}` dataset specs
  - `by`: list of BY keys (empty if no BY)
  - `retain`: list of retained variables
  - `keep`: list of output columns to keep (empty means keep all)
  - `statements`: ordered list of statement descriptors (`assign`, `filter`, `if_then`, `output`)
  - `explicit_output`: bool (true if any `output;` or output action appears)
- Semantics:
  - Statements execute in order.
  - BY groups compute `first.<key>` / `last.<key>` flags.
  - `merge` sets `in=` flags per input dataset.
- Params and statement shapes are part of the canonical data_step op; frontends (e.g. SAS) must emit this shape.

Transpose op (minimal subset)
- Op name: `transpose`
- Params:
  - `by`: list of BY keys
  - `id`: ID column name
  - `var`: value column name
  - `last_wins`: bool (when duplicate ID appears within a BY group)

SQL select op (subset)
- Op name: `sql_select`
- Params:
  - `from`: `{table, alias}` base table spec
  - `joins`: list of `{type, table, alias, on}` (type is `inner` or `left`)
  - `select`: list of `{type, name, alias}` for column refs and `{type, func, arg, alias}` for aggregates
  - `where`: expression AST or null
  - `group_by`: list of column names (possibly empty)
- Semantics:
  - Joins are evaluated left-to-right; `left` preserves base rows and fills unmatched right columns with nulls.
  - `where` filters rows after joins.
  - When `group_by` or aggregates are present, output is grouped by the keys and sorted by those keys for determinism.

Determinism rules (v0.1)
- `sort` is stable; null ordering is explicit in runtime settings.
- `sort` may apply `nodupkey` with first-wins after sorting by keys.
- Group output ordering (if/when added) must be explicit.

Cast op (v0.1)
- Op name: `cast`
- Params:
  - `casts`: list of `{col: str, to: str, on_error: "fail"|"null", trim: bool}`
  - `to` ∈ {str, int, decimal, bool, date, datetime}
  - `on_error`: `"fail"` (default) — first failure fails the step; `"null"` — put null for non-convertible values
  - `trim`: optional; if true, strip leading/trailing whitespace before parse
- Semantics:
  - Standalone IR step; one input table, one output table.
  - Row order preserved. Evidence: step_evidence includes `cast_failures` and `nulled` counts.

Format op (minimal subset)
- Op name: `format`
- Params:
  - `name`: format name (lowercase, optional `$` prefix)
  - `map`: mapping of input strings to output strings
  - `other`: default output when no match
- Semantics:
  - Registers a format mapping for `put()` calls in expressions.

Summary (frontend-only)
- `summary` is not a canonical op. If present in a frontend (e.g. PROC SUMMARY), it lowers to `aggregate` with canonical `group_by` and `metrics`. See IR_CANON_RULES.md and frontend lowering.

Validator invariants (summary)
- validate() is pure: it does not mutate step.params.
- Only canonical param shapes are allowed; legacy shapes are refused at ingress (see IR_CANON_RULES.md), not normalized in validate().
- No use of undefined input tables.
- No output table collisions.
- `sort` requires explicit `by` list (canonical: list[{col, desc}]).
- `data_step` with BY requires inputs sorted by the BY keys.
