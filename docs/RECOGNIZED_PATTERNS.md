## RECOGNIZER_PATTERNS (supported SAS shapes)

Purpose
- Prevent silent broadening of supported syntax.
- Ensure a block is either fully compiled or refused.

Scope (v0.1)
- `data ...; set ...; ... run;`
- `proc sort data=... out=...; by ...; run;`

Block segmentation (summary)
- Blocks start at `data` or `proc` statements.
- A block ends at `run;` or implicitly when a new `data`/`proc` starts.
- `run;` is not part of the block body; it is tracked as the block end locator.

Data step: supported skeleton
- Exactly one `set <input_table>;` in the body.
- Optional statements (at most one each):
  - `keep ...;` or `drop ...;`
  - `rename old=new ...;`
  - assignment statements: `name = <expr>;`
  - `if <predicate>;`
- Compilation order is canonical:
  1) select (keep/drop)
  2) rename
  3) compute (assignments batch)
  4) filter (if)
- If no operations are present after `set`, compile to `identity`.
- If operations exist and there is no `if`, the final step output is rewritten to the data step's target table (no extra identity op).

Forbidden tokens inside data step bodies
- The recognizer rejects statements that begin with:
  - `do`, `end`, `retain`, `lag(`, `first.`, `last.`, `array`, `call`,
  - `output`, `by`, `merge`, `infile`, `input`, `proc`, `%`
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
