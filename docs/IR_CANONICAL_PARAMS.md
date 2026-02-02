# IR canonical params

**Rules:**
- Consumers (runtime, printer, hash, registry) assume **canonical params only**.
- Any ingestion sugar must be **normalized in `IRDoc.validate()`**.
- Decimal constants: represented as `{type: "decimal", value: "<string>"}` (exact decimal; no Python float). Hashing/canonicalization uses the string value.

| op | canonical params shape | normalized in validate? | legacy/sugar accepted (where normalized) |
|----|------------------------|-------------------------|----------------------------------------|
| datasource | `name`, `path`?, `columns`?, `kind`, `inline_text`?, `inline_sha256`? | n/a | — |
| compute | `mode` ∈ {derive, update}, `assignments` list[{target, expr}] (or `assign` from SAS) | n/a | — |
| filter | `predicate` (expr tree) | n/a | — |
| select | `cols` = list[str] **or** `drop` = list[str] | ✅ | keep/drop raw (string, list[str], list[dict]); in validate() |
| rename | `mapping` = list[{from: str, to: str}] | ✅ | dict or mappings/map; in validate() |
| sort | `by` = list[{col: str, desc: bool}] | ✅ | list[str] or list[{col, asc}]; in validate() |
| aggregate | `group_by` = list[str], `metrics` = list[{name, op, col}] | ✅ | class, var/vars, stats, autoname, naming; in validate() |
| identity | (none or empty) | n/a | — |
| save | (output binding / path) | n/a | — |
| assert | (predicate / table) | n/a | — |
| let_scalar | (name, expr) | n/a | — |
| const | `bindings`: name → literal (int, str, bool, null, or decimal) | n/a | Decimal: `{type: "decimal", value: "<string>"}` (exact decimal; no exponent). |
| data_step | `by`?, keep/drop?, `assign`? (SAS subset) | n/a | — |
| transpose | `by`, `id`, `var` | n/a | — |
| sql_select | (query / group_by etc.) | n/a | — |
| format | (format spec) | n/a | — |
