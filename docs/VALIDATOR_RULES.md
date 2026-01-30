## VALIDATOR_RULES (the refusal engine)

Purpose
- Enforce semantic invariants after parsing/recognition.
- Refuse invalid plans before any data is touched.

What the validator sees
- Only IR steps. Parser/recognizer decisions are upstream.
- Validation rules apply to the IR as emitted.

Facts tracked per table
- `sorted_by`: list of column names or None.
- Future: `schema_known`, `keys_unique` (placeholders for v0.2+).

Sortedness inference rules (current)
- `sort` sets `sorted_by` to the BY list.
- `select` preserves `sorted_by` only when keep/drop proves keys are retained.
- `filter` preserves `sorted_by`.
- `compute` preserves `sorted_by`.
- `identity` preserves `sorted_by`.
- `rename` drops `sorted_by` (conservative; no key remap yet).

Validation rules (current)
- Input tables must exist before use:
  - `SANS_VALIDATE_TABLE_UNDEFINED`
- Output table names must not collide:
  - `SANS_VALIDATE_OUTPUT_TABLE_COLLISION`
- Each op must declare outputs (compiler contract):
  - `SANS_INTERNAL_COMPILER_ERROR`
- `sort` ops must include `by` in params:
  - `SANS_VALIDATE_SORT_MISSING_BY`

Parse vs validate (important boundary)
- Missing BY in a `proc sort` source block is a parse refusal:
  - `SANS_PARSE_SORT_MISSING_BY`
- The validator-level missing-BY rule exists for malformed or hand-constructed IR.

Location policy
- Validation errors are attributed to the IR step's `loc`.
- The recognizer is responsible for ensuring meaningful step spans.
