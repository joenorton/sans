# sans script grammar

## Header
- `sans 1.0` must appear as the first non-comment line.
- Comments begin with `#` and continue to end of line.

## Statements
Supported statements map directly to the existing IR operations:
- `format $name do ... end`
- `data <out> do ... end`
- `sort <in> -> <out> by <cols...> [nodupkey <true|false>]`
- `summary <in> -> <out> do ... end`
- `select <in> -> <out> keep|drop ...`

## DATA blocks
- Each `data` block must contain exactly one `from <source> do ... end` block.
- **Input modifiers** live only inside the `from` block:
  - `keep(col1, col2, ...)` / `drop(col1, col2, ...)`
  - `rename(old_col -> new_col, ...)`
  - `where <expr>` or `where(<expr>)`
- **Output projection** is at data scope: `keep(...)` or `drop(...)` (only one).
- **Row filtering** at data scope uses `filter <expr>` or `filter(<expr>)`.
- **Assignments** use `=` (e.g. `new_col = expr`). Overwrite requires `derive!` (e.g. `derive! col = expr`).
- `if` is **control-flow only**, not a filter.

## Expressions
Expressions reuse the existing SAS-lite expression parser with stricter rules:
- equality: `==` only
- inequality: `!=` only
- `=` is reserved for assignment

## Output
The compiler lowers every statement into an IR step with deterministic `step_id`s,
and emits the same `plan.ir.json` + `report.json` artifacts that `sans check`
produces for SAS scripts.
