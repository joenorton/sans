## REPORT_CONTRACT (report.json)

Purpose
- Define the stable, machine-actionable report payload emitted by `sans check` and `sans run`.
- Keep downstream consumers insulated from internal changes.

When it is written
- Always. `sans check` writes both `plan.ir.json` (under `artifacts/`) and `report.json` at bundle root.

Top-level fields
- `report_schema_version`: string (e.g. `"0.3"`); bumped for breaking changes.
- `status`: one of `ok`, `ok_warnings`, `refused`, `failed`.
- `exit_code_bucket`: one of `0`, `10`, `20`, `30`, `31`, `32`, `50`.
- `primary_error`: `{code, message, loc}` or `null`.
- `diagnostics[]`: list of `{code, message, loc}` entries.
- `inputs[]`: list of `{role, name, path, sha256}`. Role is one of `source`, `preprocessed`, `expanded`, `datasource`. All paths bundle-relative, forward slashes only. **sha256 required** (non-null).
- `artifacts[]`: list of `{name, path, sha256}` (e.g. plan.ir.json, registry.candidate.json, runtime.evidence.json). **sha256 required** (non-null). report.json is **not** listed in any array.
- `outputs[]`: list of `{name, path, sha256, rows?, columns?}` (user-facing table files only). Path under `outputs/`; subpaths preserved. **sha256 required** (non-null).
- `plan_path`: bundle-relative path to plan (e.g. `artifacts/plan.ir.json`).
- `report_sha256`: SHA-256 of the canonical report payload (used by `sans verify` for self-check).
- `engine`: `{name, version}`.
- `settings`: effective settings used for this run.
- `timing`: `{compile_ms, validate_ms, execute_ms}` (values may be `null`).
- `runtime`: `{status, timing}` when `sans run` executes. **No** `runtime.outputs`; use top-level `outputs[]` only.

Paths
- All paths in report and evidence are **bundle-relative**, forward slashes only. Report and evidence must **never** contain paths outside the bundle; if any file would be outside, the run errors (no exceptions).

Determinism
- Report arrays `inputs`, `artifacts`, `outputs` are canonically sorted (e.g. by path) in `canonicalize_report` for determinism.
- For any artifact or output with a `.json` suffix, `sha256` is the canonical JSON hash: parse JSON as UTF-8, serialize with `sort_keys=True`, `separators=(",", ":")`, `ensure_ascii=False`, UTF-8 encode, and hash.
- Non-JSON artifacts keep existing hashing behavior (CSV canonicalization; other files hash raw bytes or canonicalized text when defined).

Status and exit buckets
- `ok` -> `0`
- `ok_warnings` -> `10`
- `refused` -> `30`/`31`/`32` depending on the primary error code.
- `failed` -> `50`

Error payload shape
- `code`: stable machine string (e.g., `SANS_PARSE_SQL_DETECTED`).
- `message`: human-readable message.
- `loc`: `{file, line_start, line_end}` or `null` when unavailable.

Bundle layout (v0.3)
- `report.json` at bundle root (only file at root besides directory structure).
- `inputs/source/`: analysis script, preprocessed.sas (if any), expanded.sans (if any).
- `inputs/data/`: materialized datasource files (by logical name).
- `artifacts/`: plan.ir.json, registry.candidate.json, runtime.evidence.json.
- `outputs/`: user-facing table files (e.g. out.csv from save step).

Notes
- `diagnostics` may include non-fatal items in `ok_warnings`.
- `report.json` is not listed in `inputs`, `artifacts`, or `outputs`; use `report_sha256` for self-check.
