pure tool, no sales layer, but it must **ingest and execute enough sas to displace sas** in the clinical pipeline. that’s coherent—just accept that “bypass sas” doesn’t mean “implement sas,” it means “implement the clinically-relevant subset with a hard spec and relentless test suite.”

here’s the roadmap in that frame, with monetization deleted and scanning deleted.

## north star

an open-source **sas-subset execution engine** + **clinical-friendly i/o** + **sdtm/adám validation hooks**, able to run typical CRO transformation code against sas datasets and produce sdtm tabulations (and later adam) deterministically.

---

## phase 0: freeze the subset spec

**deliverable:** `SUBSET_SPEC.md` (versioned, authoritative)

* supported statements/procs (exact)
* runtime semantics (merge, missing, sort ties, type coercion)
* determinism guarantees
* explicit “unsupported” list
* compatibility knobs (if any), default behavior

why: otherwise you’ll drift into “whatever scripts we saw last week,” which is how you accidentally re-create SAS’s ambiguity.

---

## phase 1: clinical i/o parity

you don’t bypass SAS if you can’t read/write the artifacts everyone uses.

* **read**: `.xpt` (mandatory), optionally `.sas7bdat` (nice-to-have; often messy in the wild)
* **write**: `.xpt` (mandatory), plus csv/parquet
* preserve:

  * variable names/case
  * string padding rules (canonicalize internally, serialize predictably)
  * missing value semantics (including char missing)

deliverables:

* xpt round-trip tests
* fixtures based on public pilot data (not as “sas oracle,” as “realistic artifacts”)

---

## phase 2: “cro daily-driver” execution completeness

target the constructs that dominate sdtmlike pipelines.

### data step

* `do/end`, `else if`, `select/when/otherwise` (common)
* arrays (limited): `array x[*] var1-varN;` (used for repetitive transforms)
* `length/format/attrib/label` parse+ignore or store as metadata (don’t block)
* multiple outputs: `output a; output b;`
* dataset options everywhere: `keep/drop/rename/where/firstobs/obs`

### procs

* `proc sort` full subset: nodupkey, nodup, sort stability defined
* `proc sql` bounded subset (joins, group by, case/when, coalesce, distinct)
* `proc summary/means/freq` (counts and basic stats)
* `proc transpose` (you started; finish the common forms)
* `proc format` + `put()` mapping (lookup normalization is everywhere)

deliverables:

* a growing “hello_*” suite + 50–150 microtests representing real idioms
* a “supported coverage” checklist (not repo scan—just spec coverage)

---

## phase 3: macro reality without macro apocalypse

you will hit macros. CRO codebases are macro farms.

but you can still bypass SAS with a **macro-lite** approach:

* support macro vars (`&X`), `%let`, `%include`
* support `%if/%then/%else` with simple boolean expressions
* optionally: `%macro/%mend` with parameter substitution only (no macro functions)
* hard fail on:

  * `%sysfunc`, heavy macro functions, dynamic codegen tricks

deliverables:

* a preprocessor stage with excellent error messages
* a “macro-lite” appendix in `SUBSET_SPEC.md`

---

## phase 4: SDTM-aware validation as a profile

execution is necessary; validation is what makes it safe to use.

* `validate --profile sdtm` expands to:

  * domain required vars
  * key uniqueness expectations
  * ISO8601 shape checks
  * cross-domain referential checks (USUBJID consistency)
  * controlled terminology checks (configurable tables)
* output: json report + optional human summary

deliverables:

* rule id catalog
* regression tests that pin rule behavior

---

## phase 5: reproducibility and trust mechanics

this is the OSS “bypass SAS” credibility layer.

* artifact logging: inputs, plan/ir, outputs, validation report, runtime version
* deterministic hashing of artifacts
* “same inputs + same version = same outputs” guarantee
* clear semantic versioning tied to subset spec changes

deliverables:

* `sans run` emits a reproducibility bundle (not surveillance; it’s user-invoked output)
* `sans verify` replays and checks determinism

---

### the hard truth you should embrace

you won’t replace SAS by chasing feature breadth randomly. you replace SAS by:

* nailing the subset that clinical pipelines actually use,
* handling the real file formats,
* and being deterministic and auditable where SAS is opaque.
