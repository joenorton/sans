
## sprint: add `drop` pipeline step (strict, end-to-end)

### goal

introduce an explicit `drop` operation to remove columns from a table, with strict validation and full propagation through:

* grammar / parser
* IR
* type inference
* runtime execution
* schema evidence
* fmt / expanded.sans
* cheshbon-relevant artifacts (table effects, lineage)

no sugar, no wildcards, no silent ignores.

---

## semantics (locked in)

* syntax:

  ```
  drop col1, col2, col3
  ```
* allowed only inside table pipelines (`from … do … end`)
* strict behavior:

  * every listed column **must exist** at that point in the pipeline
  * otherwise: fail at compile time with `E_COLUMN_NOT_FOUND`
* effect:

  * remove listed columns
  * preserve original column order minus dropped columns
* no flags, no regex, no “ignore missing”

---

## work breakdown

### 1. grammar + parser

* grammar.md:

  * add `drop` statement to pipeline grammar
  * reuse column list production used by `select`
* parser:

  * parse into a new AST node (e.g. `DropStep(columns: list[str])`)
  * enforce placement rules (only in table pipeline)

**tests**

* parse success: `drop a, b`
* parse failure: `drop` outside pipeline
* parse failure: empty list

---

### 2. IR + expanded.sans

* IR:

  * add `DropOp` / `drop` step to IR model
  * include dropped column names explicitly
* expanded.sans:

  * emit as `drop a, b`
  * do **not** desugar into `select`

**tests**

* IR round-trip
* expanded.sans matches canonical formatting

---

### 3. compile-time validation

* validation step:

  * at the point of `drop`, check current schema
  * if any column missing → `E_COLUMN_NOT_FOUND`
* error must occur **before runtime**

**tests**

* dropping existing column succeeds
* dropping non-existent column fails with correct error + line number

---

### 4. type inference / schema evolution

* schema transform:

  * input schema → output schema = input minus dropped columns
* ensure:

  * no `unknown` introduced
  * downstream steps see reduced schema

**tests**

* drop reduces schema correctly
* drop + derive/select interaction
* drop reflected in `schema.evidence.json`

---

### 5. runtime execution

* runtime table op:

  * remove columns from dataframe / row dicts
  * preserve row order
* runtime must **assume** compile-time validation already happened

**tests**

* simple csv → drop → save
* runtime output has correct headers

---

### 6. evidence + lineage

* schema.evidence.json:

  * show column removed at that step
* table_effects / lineage:

  * mark dropped columns as terminal
  * ensure no downstream dependency edges reference them

(this is important for cheshbon diffs)

**tests**

* evidence shows column disappearance
* lineage graph has no outgoing edges for dropped columns

---

### 7. fmt support

* fmt:

  * canonical formatting for `drop` (same style as `select`)
  * ordering preserved
* ensure idempotence:

  * `sans fmt` round-trips scripts with `drop`

**tests**

* fmt ok / ugly fixtures including drop

---

### 8. docs

* update grammar.md
* update any pipeline reference docs
* add one example showing why `drop` is preferable to verbose `select`

---

## non-goals (explicitly out of scope)

* `drop *`
* `drop except`
* optional / lenient mode
* runtime inference of missing columns

---

## definition of done

* `python -m pytest` passes
* `drop` appears in:

  * expanded.sans
  * IR
  * runtime output
  * schema.evidence.json
* invalid drops fail at compile time, never at runtime
* no behavior changes to existing scripts

---
