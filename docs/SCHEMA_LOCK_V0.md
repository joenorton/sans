# Schema Lock v0

Schema lock v0 provides typed CSV ingestion without requiring every column to be hand-typed in the script. A run must have either **(a)** a valid schema lock file, or **(b)** explicit typed column pinning in the datasource declaration.

## Requirement: typed pinning OR schema lock

- If a datasource has **typed columns** pinned (e.g. `columns(a:int, b:decimal)`), it is acceptable without a lock.
- If a datasource does **not** have typed pinning, then `--schema-lock` must be provided and the lock must include that datasource.
- Otherwise the run fails early with **E_SCHEMA_REQUIRED**: *"Provide --schema-lock or typed columns(...)"*.

## Generating a schema lock

You can emit a lock file in two ways.

### The `sans schema-lock` subcommand (recommended)

Generate a schema lock **without running the pipeline** and **without requiring `--out`**:

```bash
sans schema-lock script.sans
```

- **Default lock path**: the lock is written to `<script_dir>/<script_stem>.schema.lock.json`. For example, `sans schema-lock demo_high.sans` creates `demo_high.schema.lock.json` next to the script.
- **Override destination**: use `--write PATH` or `-o PATH`. If `PATH` is relative, it is resolved relative to the **script directory** (not the current working directory). If `PATH` is absolute, it is used as-is.
- **Optional bundle**: add `--out DIR` to also write `report.json` and stage inputs under `DIR/inputs` (same as lock-only staging). The lock file is still written to the default or `--write` path; it is not moved into `DIR` unless you pass e.g. `--write DIR/schema.lock.json`.
- Supports `--tables`, `--include-root`, `--allow-absolute-include`, `--allow-include-escape`, `--schema-lock` (existing lock to merge), and `--legacy-sas` as needed to resolve datasources.
- No runtime execution; exit 0 on success.

**1. After a successful run** (when all datasources are typed or covered by a lock):

```bash
sans run script.sans --out out --emit-schema-lock schema.lock.json
```

The lock path is resolved against the output directory: a **relative** path (e.g. `schema.lock.json`) is written under `--out` (e.g. `out/schema.lock.json`); an **absolute** path is used as-is. Types come from typed pinning in the script or from the lock used for that run. The report includes `schema_lock_mode` (`"ran_and_emitted"`), `lock_only` (false), and `schema_lock_path` / `schema_lock_emit_path`.

**2. Lock-only (no execution)**  
You can force lock-only mode with **--lock-only** (together with **--emit-schema-lock**): the tool generates the lock and stages inputs but never runs transforms, even if all datasources are already typed.  
When you use `--emit-schema-lock` and the script has CSV/inline_csv datasources **without** typed columns (and without `--schema-lock`), the tool also runs in **lock-only** mode: it compiles the script (without type-checking pipeline expressions), discovers referenced datasources, infers column names and types by scanning the CSV (or inline content) up to a bounded number of rows, writes the lock file, and stages referenced inputs under `{out_dir}/inputs`. It then exits **without** running transforms. The report includes `schema_lock_mode` (`"generated_only"`), `lock_only` (true), and `schema_lock_path` / `schema_lock_emit_path`. The lock path is resolved the same way (relative to `--out`, or absolute as-is).

- CSV paths are resolved relative to the script directory (or absolute paths as-is). Each referenced CSV file must exist.
- Inferred types use a deterministic, monotonic rule: empty/whitespace is ignored; if any non-null value requires string → string; else if any requires decimal → decimal; else if any int → int; else if all non-null are strict `true`/`false` → bool; else string. Values that look numeric but have **leading zeros** (e.g. subject IDs like `00123`) are inferred as **string** so they are not normalized; this is a deliberate policy choice.
- The lock records per-datasource inference metadata when used: `rows_scanned`, `truncated`, and `inference_policy_version`. If a datasource has explicit typed pinning, that overrides inference. To override an inferred type (e.g. a column inferred as string that you want as int), you can edit the lock file directly (change the `type` in the `columns` array); a future override flag may also be added.

## Enforcing a schema lock

To run with an existing lock (so you don’t need to type all columns in the script):

```bash
sans run script.sans --out out --schema-lock schema.lock.json
```

The lock is used to supply column names and types for any CSV/inline_csv datasource that does not have typed pinning. **Path resolution**: a relative path is resolved against the **script directory** (not the current working directory or `--out`); an absolute path is used as-is. If the file is not found or cannot be parsed, the run fails immediately with **E_SCHEMA_LOCK_NOT_FOUND** (no execution).

When you provide `--schema-lock`, the lock file is **copied into the output directory** at `{out_dir}/schema.lock.json` so the bundle is self-contained and auditable. The report includes **schema_lock_used_path** (the path you passed) and **schema_lock_copied_path** (`"schema.lock.json"`). The copy is byte-identical and **schema_lock_sha256** matches the canonical lock hash.

### Autodiscovery (2-command workflow)

If you **do not** pass `--schema-lock` and the script has at least one untyped CSV/inline_csv datasource, `sans run` looks for a lock file in the **script directory** only:

1. `<script_stem>.schema.lock.json` (e.g. `demo_high.schema.lock.json` next to `demo_high.sans`)
2. `schema.lock.json`

If found, that lock is loaded and applied as if you had passed `--schema-lock`; the report has **schema_lock_auto_discovered** `true` and **schema_lock_used_path** set. The lock is still copied into `{out_dir}/schema.lock.json`. If no lock is found, the run refuses with **E_SCHEMA_REQUIRED** and the error message lists the paths that were searched. If all referenced datasources have typed columns (pinned), autodiscovery is skipped and **schema_lock_auto_discovered** is `false`.

**Example:** `sans schema-lock demo_high.sans` writes `demo_high.schema.lock.json` next to the script; then `sans run demo_high.sans --out dh_out` (no `--schema-lock`) auto-uses that file and runs successfully.

## Behavior when enforcing a lock

- **Extra columns in input**: Allowed. Columns in the CSV that are not in the lock are ignored for type enforcement (they remain in row data but are not required or type-checked by the lock).
- **Missing columns**: If the input is missing any column that is in the lock, the run fails with **E_SCHEMA_MISSING_COL** and the message lists the missing column(s).
- **Type mismatch**: If a locked column value cannot be coerced to its locked type, the run fails with **E_CSV_COERCE**. Details (expected type, sample raw values) are in `runtime.evidence.json` under `coercion_diagnostics`.
- **Column order**: Not enforced; matching is by **column name** only.

## Where the lock file is written

**`sans schema-lock`**
- **Default**: `<script_dir>/<script_stem>.schema.lock.json` (e.g. `demo_high.sans` → `demo_high.schema.lock.json` next to the script).
- **`--write` / `-o`**: relative paths are resolved against the script directory; absolute paths are used as-is.

**`sans run --emit-schema-lock`**
- **Relative path** (e.g. `schema.lock.json`): resolved against the `--out` directory. Example: `sans run script.sans --out out --emit-schema-lock schema.lock.json` writes `out/schema.lock.json`.
- **Absolute path**: used as-is.
- Stdout indicates the mode: `ok: wrote schema lock to <path> (lock-only)` or `(after run)`.

## Report fields when a schema lock is emitted

When `--emit-schema-lock` is used, `report.json` includes:

- **schema_lock_mode**: `"generated_only"` (lock-only, no execution) or `"ran_and_emitted"` (lock written after a successful run).
- **lock_only**: `true` if no execution was performed, `false` otherwise.
- **schema_lock_emit_path**: absolute path where the lock file was written.
- **schema_lock_path**: bundle-relative path if the lock is under the output directory; otherwise the absolute path.
- **schema_lock_sha256**: SHA-256 of the canonical lock JSON (for verification).

When you provide **--schema-lock** on a normal run (no lock-only), the report also includes **schema_lock_used_path** (path you passed) and **schema_lock_copied_path** (bundle-relative path of the copy in `out_dir`).

## Lock file format (v0)

- `schema_lock_version`: `1`
- `created_by`: `{ "sans_version": "...", "git_sha": "..." }`
- `datasources`: list of entries, each with `name`, `kind`, `path`, `columns`: [ `{ "name", "type" }` ], `rules`: `{ "extra_columns": "ignore", "missing_columns": "error" }`. When a datasource was inferred (no typed pinning), an entry may also include `inference_policy_version` (integer), `rows_scanned` (integer), and `truncated` (boolean).
- Paths use forward slashes; lock JSON is deterministic for hashing and diffing.

## Report binding (optional)

When a run uses or emits a schema lock, the report may include **schema_lock_sha256** (SHA-256 of the canonical lock JSON). You can then verify that a given lock file matches the run:

```bash
sans verify out --schema-lock schema.lock.json
```

If `--schema-lock` is given, `sans verify` checks that the lock file’s hash equals `report.schema_lock_sha256`.

## Error codes

| Code | When |
|------|------|
| **E_SCHEMA_REQUIRED** | A referenced CSV datasource has no typed columns and no entry in the provided schema lock. |
| **E_SCHEMA_MISSING_COL** | Input CSV is missing one or more columns required by the lock. |
| **E_SCHEMA_LOCK_NOT_FOUND** | `--schema-lock` was supplied but the file was not found or could not be parsed at the resolved path (relative paths are resolved against the script directory). |
| **E_SCHEMA_LOCK_MISSING_DS** | `--schema-lock` was supplied and read, but the lock does not contain an entry for one or more referenced untyped datasources. |
| **E_SCHEMA_LOCK_INVALID** | A schema-lock entry (or pinned columns) contains an unknown or invalid column type (e.g. `unknown`). All ingress columns must have concrete types (int, decimal, string, bool, date, etc.). |
| **E_CSV_COERCE** | A locked or typed column value could not be coerced to the expected type (see `coercion_diagnostics` in runtime.evidence.json). |
| **SANS_LOCK_GEN_FILE_NOT_FOUND** | Lock-only generation: a referenced CSV file was not found at the resolved path. |

## Summary

- Use **typed pinning** in the script when you want to declare types directly.
- Use **--emit-schema-lock** to generate a lock file: either after a successful run (with types or an existing lock), or from untyped datasources (lock-only: compile, infer from CSV, write lock, no execution).
- Use **--schema-lock** to run without typing all columns; the lock supplies names and types.
- Normal runs still require either typed pinning or `--schema-lock`; inference is used only for lock generation.
- Extra columns in input are ignored; missing columns or type mismatches fail with clear codes and evidence.
