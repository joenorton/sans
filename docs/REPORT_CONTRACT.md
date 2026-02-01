## REPORT_CONTRACT (report.json)

Purpose
- Define the stable, machine-actionable report payload emitted by `sans check`.
- Keep downstream consumers insulated from internal changes.

When it is written
- Always. `sans check` writes both `plan.ir.json` and `report.json`.

Top-level fields
- `status`: one of `ok`, `ok_warnings`, `refused`, `failed`.
- `exit_code_bucket`: one of `0`, `10`, `20`, `30`, `31`, `32`, `50`.
- `primary_error`: `{code, message, loc}` or `null`.
- `diagnostics[]`: list of `{code, message, loc}` entries.
- `inputs[]`: list of `{path, sha256?}`.
- `outputs[]`: list of `{path, sha256?}`.
- `plan_path`: path to the emitted `plan.ir.json`.
- `engine`: `{name, version}`.
- `settings`: effective settings used for this run.
- `timing`: `{compile_ms, validate_ms, execute_ms}` (values may be `null`).
- `runtime`: `{status, outputs, timing}` when `sans run` executes.

Runtime outputs (v0.1)
- `runtime.outputs[]` entries include:
  - `table` (name)
  - `path` (csv path)
  - `rows` (row count)
  - `columns` (column list)

Status and exit buckets
- `ok` -> `0`
- `ok_warnings` -> `10`
- `refused` -> `30`/`31`/`32` depending on the primary error code:
  - `SANS_PARSE_*` or `SANS_BLOCK_*` -> `30`
  - `SANS_VALIDATE_*` -> `31`
  - `SANS_CAP_*` -> `32`
- `failed` -> `50`

Error payload shape
- `code`: stable machine string (e.g., `SANS_PARSE_SQL_DETECTED`).
- `message`: human-readable message.
- `loc`: `{file, line_start, line_end}` or `null` when unavailable.

Settings
Minimum fields currently emitted:
- `strict` (bool)
- `allow_approx` (bool)
- `tolerance` (object or null)
- `tables` (list of predeclared input table names)

Notes
- `diagnostics` may include non-fatal items in `ok_warnings`.
- `outputs` always includes both `plan.ir.json` and `report.json` paths; hashes are optional.
