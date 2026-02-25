# MUTATION_CONTRACT_V0_1

Frozen contract reference for kernel mutation v0.1.

Source document: `docs/MUTATION_CONTRACT.md`.
Version marker: `sans.mutation.contract = 0.1`.

This file is intentionally version-pinned and should only change on explicit contract updates.

## Canonical source snapshot

The authoritative v0.1 contract text is maintained in:

- `docs/MUTATION_CONTRACT.md`

At this sprint boundary, kernel implementation in `sans.amendment` is aligned to that contract:

- input surface is `sans.ir` only
- pure, deterministic ir→ir mutation
- refusal-first ambiguity handling
- contract-shaped `diff.structural`, `diff.assertions`, and diagnostics
- stable refusal code family `E_AMEND_*`

## Pinned table-universe rule (collision checks)

For `E_AMEND_OUTPUT_TABLE_COLLISION`, the collision universe is fixed to:

- all names appearing in existing `step.outputs`
- plus explicit top-level `tables[]` names when present
- excluding datasource pseudo names (for example `__datasource__*`)

Any new output table name intersecting that universe is refused.

