# Schema Lock v0

Schema lock v0 provides typed CSV ingestion without requiring every column to be hand-typed in the script. A run must have either **(a)** a valid schema lock file, or **(b)** explicit typed column pinning in the datasource declaration.

## Requirement: typed pinning OR schema lock

- If a datasource has **typed columns** pinned (e.g. `columns(a:int, b:decimal)`), it is acceptable without a lock.
- If a datasource does **not** have typed pinning, then `--schema-lock` must be provided and the lock must include that datasource.
- Otherwise the run fails early with **E_SCHEMA_REQUIRED**: *"Provide --schema-lock or typed columns(...)"*.

## Generating a schema lock

After a successful run, emit a lock file:

```bash
sans run script.sans --out out --emit-schema-lock schema.lock.json
```

The lock file is written to the path you give. It includes only datasources that were referenced in the run. Types in the lock come from typed pinning in the script or from the lock used for that run.

## Enforcing a schema lock

To run with an existing lock (so you don’t need to type all columns in the script):

```bash
sans run script.sans --out out --schema-lock schema.lock.json
```

The lock is used to supply column names and types for any CSV/inline_csv datasource that does not have typed pinning.

## Behavior when enforcing a lock

- **Extra columns in input**: Allowed. Columns in the CSV that are not in the lock are ignored for type enforcement (they remain in row data but are not required or type-checked by the lock).
- **Missing columns**: If the input is missing any column that is in the lock, the run fails with **E_SCHEMA_MISSING_COL** and the message lists the missing column(s).
- **Type mismatch**: If a locked column value cannot be coerced to its locked type, the run fails with **E_CSV_COERCE**. Details (expected type, sample raw values) are in `runtime.evidence.json` under `coercion_diagnostics`.
- **Column order**: Not enforced; matching is by **column name** only.

## Lock file format (v0)

- `schema_lock_version`: `1`
- `created_by`: `{ "sans_version": "...", "git_sha": "..." }`
- `datasources`: list of `{ "name", "kind", "path", "columns": [ { "name", "type" } ], "rules": { "extra_columns": "ignore", "missing_columns": "error" } }`
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
| **E_CSV_COERCE** | A locked or typed column value could not be coerced to the expected type (see `coercion_diagnostics` in runtime.evidence.json). |

## Summary

- Use **typed pinning** in the script when you want to declare types directly.
- Use **--emit-schema-lock** after a good run to generate a lock file.
- Use **--schema-lock** to run without typing all columns; the lock supplies names and types.
- Extra columns in input are ignored; missing columns or type mismatches fail with clear codes and evidence.
