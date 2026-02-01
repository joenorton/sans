# AGENTS.md

Purpose: keep automated agents aligned with the SANS roadmap and prevent semantic creep.

Primary references
- docs/BIG_PIC.md
- docs/PATHWAY.md
- docs/SUBSET_SPEC.md
- docs/sprints/README.md
- docs/sprints/TODO_TESTS.md

Rules of engagement (mandatory)
- Do not implement features outside the current sprint doc.
- Every change must be deterministic and documented.
- Any behavior change requires a spec bump decision (no/minor/major).
- If tests fail, stop and fix before proceeding.

Current sprint
- Use the most recent sprint doc under docs/sprints/ and follow its Definition of Done.

Spec bump policy
- Every sprint doc ends with “Spec bump required?” and must be followed.
- If minor/major: update docs/SUBSET_SPEC.md and add a new hello_* test.

Test tiers
- Tier 0 (blocking): hello_* integration + core microtests.
  - Run via scripts/run_tier0.ps1 or scripts/run_tier0.sh.
- Tier 1 (nightly): larger fixtures, perf regressions, expanded SDTM rulepacks.

Output ordering policy (summary)
- Explicit projection order wins (keep/select/var).
- Single input dataset preserves source column order.
- Joins/merges without explicit projection: left columns then right columns, source order, no duplicates.
- Derived columns appended in assignment order.

Stop conditions
- Unsupported constructs must produce structured errors (file/line/construct).
- If a change introduces ambiguity, refuse it or make a single documented policy choice.
