# AMENDMENT_REQUEST_V1

Frozen request-schema reference for kernel mutation v0.1.

Source document: `docs/amendment_request_v1.md`.
Version marker: `format = sans.amendment_request`, `version = 1`, `contract_version = 0.1`.

This file is intentionally version-pinned and should only change on explicit request-contract updates.

## Canonical source snapshot

The authoritative v1 request contract text is maintained in:

- `docs/amendment_request_v1.md`

At this sprint boundary, kernel implementation in `sans.amendment` aligns to the pinned decisions:

- canonical JSON hashing for mutation artifacts
- transform id derivation from `{"op","params"}` only
- RFC6901 selector paths relative to `step.params` (`"/"` targets params root)
- strict discriminated op schemas with `extra=forbid`
- deterministic, sequential, atomic application with single-refusal output

