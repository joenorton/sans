## RECOGNIZER_PATTERNS (supported SAS shapes)

Purpose
- Prevent silent broadening of supported syntax.
- Ensure a block is either fully compiled or refused.

Scope (v0.1)
- `data ...; set/merge ...; ... run;`
- `proc sort data=... out=...; by ...; run;`
- `proc transpose data=... out=...; by ...; id ...; var ...; run;`
- `proc sql; create table ... as select ... from ... [join ...] [where ...] [group by ...]; quit;`

Block segmentation (summary)
- Blocks start at `data` or `proc` statements.
- A block ends at `run;` or implicitly when a new `data`/`proc` starts.
- `run;` is not part of the block body; it is tracked as the block end locator.

Data step: supported skeletons

1) Simple (stateless) data step
- Exactly one `set <input_table>(options);` in the body.
- Optional statements (at most one each):
  - `keep ...;` or `drop ...;`
  - `rename old=new ...;`
  - assignment statements: `name = <expr>;`
  - `if <predicate>;` (filter only)
- Compilation order is canonical:
  1) rename
  2) compute (assignments batch)
  3) filter (if)
  4) select (keep/drop)
- If no operations are present after `set`, compile to `identity`.
- If operations exist and there is no `if`, the final step output is rewritten to the data step's target table (no extra identity op).

2) Stateful data step (BY-group / MERGE subset)
- Exactly one `set <table>(options);` or `merge <t1>(in=flag options) <t2>(in=flag options) ...;`
- Optional statements:
  - `by <keys...>;` (required if `merge` or any `first./last.` usage)
  - `retain <vars...>;` (persist values across rows)
  - `keep <vars...>;` (applied at end of step)
  - assignments: `name = <expr>;`
  - `if <predicate>;` (filter)
  - `if <predicate> then <assignment|output>;`
  - `else <assignment|output>;`
  - `output;`
- Statements execute in order (no canonical reordering).
- `first.<key>` and `last.<key>` are available in expressions when BY is active.

Dataset options (SET/MERGE inputs)
- Supported: `keep=`, `drop=`, `rename=(a=b c=d)`, `where=(expr)`, and `in=` (MERGE only).
- Options apply at read-time to that input stream (where -> keep/drop -> rename).
- Unknown options refuse the data step.

Forbidden tokens inside data step bodies
- The recognizer rejects statements that begin with:
  - `do`, `end`, `lag(`, `array`, `call`, `infile`, `input`, `proc`, `%`
- Tokens like `merge`, `by`, `retain`, `first.`, `last.`, `output`, and `else` are allowed only in the stateful data-step subset.
- Token detection is statement-leading only (to avoid false positives like `input_table`).

Refusal diagnostics (data step)
- Forbidden tokens -> `SANS_BLOCK_STATEFUL_TOKEN`
  - Location uses the data-step block span.
- Malformed set/rename -> specific parse codes.
- Extra or unknown statements -> `SANS_PARSE_UNSUPPORTED_DATASTEP_FORM` at the data-step block span.

Proc sort pattern (v0.1)
- Header requires `data=` and `out=`; any other header option is refused.
- Exactly one `by` statement in the body.
- Any other body statement is refused.
- Missing `by` is a parse refusal (`SANS_PARSE_SORT_MISSING_BY`).

Proc transpose pattern (v0.1)
- Header requires `data=` and `out=`; any other header option is refused.
- Exactly one each: `by`, `id`, `var` statements.
- Rows are grouped by BY keys; ID values become columns; VAR provides values.

Proc sql pattern (v0.1)
- Only `create table <out> as select ... from ...` is supported.
- Join types: `inner join` or `left join` with `on` predicate (explicit keyword required).
- Optional `where` and `group by` clauses.
- Select list supports column refs + aggregates (count/sum/min/max/avg).
- Group-by rule: non-aggregate select columns must appear in GROUP BY.
