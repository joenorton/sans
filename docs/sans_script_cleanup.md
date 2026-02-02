# sans script cleanup & language tightening plan

## goals

* eliminate sas-style ambiguity (implicit scope, mode-based behavior).
* preserve readability without sacrificing determinism.
* make table pipelines first-class, linear, and hashable.
* reduce ceremony at script boundaries.
* replace special-case constructs (`format`) with general primitives.

non-goals:

* backward compatibility.
* feature expansion beyond syntax/semantics cleanup.

---

## 1. blocks & scope (hard rule)

### decision

* **`do … end` is the only scope delimiter.**
* indentation is cosmetic.
* blocks are only allowed where a statement list is semantically required.

---

## 2. tables as first-class expressions

### core type split

* `let` → scalars, maps, configs
* `table` → dataframe-like values

### binding rules

```sans
table enriched = <table-expr>
let risk_map = <scalar-expr>
```

* a name may not change kind (`let` → `table`, or vice versa).
* table expressions are immutable at the language level.

---

## 3. pipeline blocks (fixing readability)

### solution

**pipeline block**: linear, ordered, explicit.

```sans
table enriched = from(in) do
  rename(b -> base)
  mutate(base2 = a * 2)
  filter(base2 > 10)
  select a, base, base2
end
```

### semantics

* each line is a discrete transform step.
* compiler desugars block to chained table expressions.
* pipeline context provides implicit input (`__pipe__`).

---

## 4. replace `format` with maps

### decision

* remove `format` as a language primitive.
* replace with **typed map literals**.

```sans
let risk_label = map(
  "HIGH" -> "High risk",
  "LOW"  -> "Low risk",
  _      -> "Other"
)
```

---

## 5. fluent builders for config-heavy ops

### solution

builder pattern with dot chaining.

```sans
table enriched_s = sort(enriched).by(a).nodupkey(true)
table stats = summary(enriched_s).class(a).var(base2)
```

---

## 6. postfix table clauses

### decision

support **postfix table clauses** for transforms.

```sans
stats select a, base2_mean
stats filter base2_mean > 5
```

---

## 7. semantic invariants (enforceable rules)

1.  **Linear Scoping:** Each statement in a pipeline block or sequence of postfix clauses sees the table schema as produced by the *immediately preceding* statement.
2.  **Column Shadowing:** Once a column is renamed or dropped, its original name is removed from the active scope of that pipeline. No "shadow" references to original names are allowed.
3.  **Strict Mutation:** `mutate` creates new columns. Attempting to assign to a column that already exists in the current schema is a compile-time error. Explicit `derive!` or `overwrite` must be used for modifications.
4.  **Ternary `if`:** The `if(cond, then, else)` expression is a pure scalar ternary. It requires all three arguments, and both result branches must unify to the same base type (String or Numeric).
5.  **Total Maps:** Maps defined with a default case (`_ -> ...`) are total and guaranteed to return a value. Maps without a default are partial; lookups for missing keys trigger a terminal runtime error.
6.  **Sort Stability:** Sorting is stable. When `nodupkey(true)` is specified, the first row encountered in the sorted sequence for each unique key tuple is preserved; others are discarded.
7.  **Deterministic Nulls:** Null values (`null` or `.`) are treated as the smallest possible value for the purpose of sorting and comparisons.
8.  **Aggregate Naming:** `summary` produces aggregate columns using the canonical naming convention: `<input_var>_<stat_name>` (e.g., `lbstresn_mean`).
9.  **Kind Locking:** A name bound via `let` is a scalar/map and can never be used where a table is expected. A name bound via `table` is a dataframe and cannot be used in scalar expressions.
10. **Terminality:** A script must evaluate to exactly one terminal table. Scalar terminals are invalid. No statements may follow the terminal table expression.