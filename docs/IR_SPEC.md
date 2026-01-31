## IR_SPEC (semantics)

Purpose
- Define what IR fields mean and what invariants are assumed.
- Make validation deterministic and auditable.

Core semantics
- `steps[]` is an ordered plan; no hidden state or implicit tables.
- `inputs[]` and `outputs[]` are explicit; each op must declare its outputs.
- Tables only exist if produced by a prior step or predeclared by the caller.

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
  - `{"type":"call","name":"coalesce|if","args":[...]}`

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

Determinism rules (v0.1)
- `sort` is stable; null ordering is explicit in runtime settings.
- Group output ordering (if/when added) must be explicit.

Validator invariants (summary)
- No use of undefined input tables.
- No output table collisions.
- `sort` requires explicit `by` list.
- `data_step` with BY requires inputs sorted by the BY keys.
