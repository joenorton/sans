# graph.json (schema v1)

`graph.json` is an internal bundle artifact under `artifacts/graph.json`. It is not a user-facing output table.

## Schema (v1)
```
{
  "schema_version": 1,
  "producer": {"name": "sans", "version": "<semver>"},
  "nodes": [
    {
      "id": "s:<step_id>",
      "kind": "step",
      "op": "<op>",
      "transform_id": "<transform_id>",
      "inputs": ["t:<table_id>", ...],
      "outputs": ["t:<table_id>", ...],
      "payload_sha256": "<sha256>"
    },
    {
      "id": "t:<table_id>",
      "kind": "table",
      "producer": "s:<step_id>" | null,
      "consumers": ["s:<step_id>", ...]
    }
  ],
  "edges": [
    { "src": "s:<step_id>", "dst": "t:<table_id>", "kind": "produces" },
    { "src": "t:<table_id>", "dst": "s:<step_id>", "kind": "consumes" }
  ]
}
```

`producer` is included when engine version is available.

## Invariants
- `nodes` sorted by `id` ascending.
- `edges` sorted by `(src, dst, kind)` ascending.
- All list fields are sorted.
- No file paths, loc/line numbers, timestamps, or env data.
- Each table node has at most one producer; multi-producer tables are illegal and must be expressed via explicit union/merge steps.

## Hashing
- `payload_sha256` is computed from a canonical JSON payload:
  - `{"op": ..., "inputs": [...], "outputs": [...], "params": ..., "transform_id": ...}`
  - Canonical JSON: `sort_keys=True`, `separators=(",", ":")`, `ensure_ascii=False`, UTF-8 encoded, then SHA-256.
- The `report.json` artifact entry for `graph.json` uses the same canonical JSON hashing rules as other `.json` artifacts.
 - payload_sha256 is computed from the canonical json serialization of {op, inputs, outputs, params, transform_id} with no loc, step_id, or environment data.
