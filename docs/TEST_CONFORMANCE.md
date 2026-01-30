## 6) `TEST_CONFORMANCE.md` (how correctness is measured)

**purpose:** keep it from shipping untested semantics.

must include:

* the conformance suite rule: every feature adds a “runs” and a “refuses” test
* deterministic output normalization rules for comparisons
* minimum fixture set for v0.1:

  * expression semantics
  * null handling
  * sort stability
  * compare tolerance
  * refusal cases (macro, sql, retain)