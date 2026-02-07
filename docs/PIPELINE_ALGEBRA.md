---

# pipeline algebra

this document defines the **semantic core** of sans table pipelines: the minimal set of transformation operators and the invariants they obey. it is intentionally small, strict, and deterministic.

## model

a pipeline transforms a table through a sequence of steps:

```
from(ds) do
  step1
  step2
  ...
end
```

each step transforms a table with a **schema** (ordered column list with types) and a **rowset** (rows). sans enforces correctness at compile time whenever possible; runtime should not be the first place you learn you spelled a column wrong.

## invariants

### determinism

* no runtime schema inference during execution
* all referenced datasources must be fully typed via either:

  * pinned column types in the datasource declaration, or
  * a schema lock
* ingress schemas must be total: no `unknown` types are allowed for datasource columns during a run

### strict column references

* steps that reference columns are **strict**:

  * referencing a missing column is an error (`E_COLUMN_NOT_FOUND`)
* “ignore missing” is not supported (by design)

### order and stability

* column order is stable unless explicitly changed by an operator:

  * `rename` preserves position
  * `drop` preserves relative order of remaining columns
  * `select` defines a new order explicitly
* row order is preserved unless a step explicitly reorders rows (e.g., `sort`)

## operators

### select

**syntax**

```
select col1, col2, col3
```

**effect**

* projects the table to exactly the listed columns, in the listed order

**requirements**

* each listed column must exist at that point in the pipeline
* duplicates in the list are invalid (if currently permitted, they should be rejected)

**schema algebra**

* output schema = `[col1, col2, col3]` (types preserved)

**notes**

* use `select` when you want explicit order and an explicit whitelist

---

### drop

**syntax**

```
drop col1, col2, col3
```

**effect**

* removes the listed columns from the table

**requirements**

* each listed column must exist at that point in the pipeline
* empty drop list is invalid

**schema algebra**

* output schema = input schema minus `{col1, col2, col3}`
* remaining columns keep their original relative order

**notes**

* `drop` is not `select` sugar. it is its own operator and appears as such in IR and evidence.
* use `drop` when you want “everything except a few columns” without rewriting a long `select`

---

### rename

**syntax**

```
rename old1 -> new1, old2 -> new2
```

**effect**

* renames columns without changing row values

**requirements**

* each `old` must exist
* each `new` must not collide with an existing column name after renaming (collision behavior must be strict)

**schema algebra**

* output schema is input schema with names rewritten in-place
* types preserved, positions preserved

**notes**

* rename is not a projection; it does not change which columns exist
* rename is reflected in lineage as an identity edge from old to new

---

### derive

**syntax**

```
derive new1 = expr1, new2 = expr2
```

**effect**

* adds new columns computed from expressions

**requirements**

* each new column name must not exist at that point (derive creates, it does not overwrite)
* expressions are type-checked; invalid ops fail with explicit errors (`E_TYPE`, `E_TYPE_UNKNOWN`, etc.)

**schema algebra**

* output schema = input schema + new columns appended (unless your implementation preserves insertion points; append is simplest and deterministic)
* types inferred from expressions

**notes**

* `derive` is additive; use `update!` to overwrite existing columns

---

### update!

**syntax**

```
update!( col1 = expr1, col2 = expr2 )
```

**effect**

* overwrites existing columns with computed values

**requirements**

* each target column must exist
* expressions are type-checked
* column type changes are allowed (current policy) and recorded in schema evidence

**schema algebra**

* output schema has the same column order as input
* target column types become the inferred expression types

---

### filter

**syntax**

```
filter(expr)
```

**effect**

* removes rows for which the predicate evaluates false

**requirements**

* predicate must type-check to `bool`
* strict expression contract applies (`==`/`!=` only for equality, no legacy tokens unless explicitly in legacy mode)

**schema algebra**

* schema unchanged

**notes**

* filter never changes columns; it only changes rowset cardinality

## composition rules

pipelines are evaluated left-to-right. schema evolution is computed stepwise:

```
S0 = schema(from(ds))
S1 = step1(S0)
S2 = step2(S1)
...
```

errors are raised at the first step where requirements are violated.

## evidence and artifacts

### schema evidence

`sans` emits deterministic schema evidence mapping tables to `{col: type}`. this is the authoritative record of schema evolution and is used by downstream tooling (e.g., cheshbon) for diffing.

### lineage

lineage edges represent column derivations. projection operators (`select`, `drop`) restrict pass-through edges to the surviving columns; dropped columns have no downstream edges.

## non-goals

this document does not specify:

* join semantics
* group/aggregate semantics beyond their own operators
* sorting stability rules (those belong to sort/aggregate docs)
* legacy sas operator translation (explicitly quarantined elsewhere)

---
