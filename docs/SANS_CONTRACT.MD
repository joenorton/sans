## 1) `SANS_CONTRACT.md` (the commandments, 1 page)

**purpose:** stop scope creep and semantic freelancing.

must include:

* the non-negotiables:

  * parse → ir → validate → execute (no interleaving)
  * strict refusal policy (unknown constructs fatal by default)
  * no eval, no macro engine, no guessing formats/keys
  * deterministic sort/null semantics
* definition of “sound / approx / unsupported”
* allowed op list (v0.1) + forbidden constructs list
* exit code buckets + “stable error code strings are the contract”

this doc is what you paste into every agent prompt.