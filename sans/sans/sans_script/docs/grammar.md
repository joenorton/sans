# sans script grammar

## header

* the version marker **must** appear as a comment:

  ```
  # sans 0.1
  ```
* it must appear within the first **5 non-empty lines** of the file.
* comments begin with `#` and continue to end of line.

---

## core concepts

* the language distinguishes **scalar values** and **table values**.
* scope is created only by `do … end`; indentation is cosmetic.
* table transformations are **linear, explicit, and deterministic**.
* **semantic invariants** govern all transformations (see below).

---

## bindings

### scalar bindings

* scalar bindings use `let`:

  ```
  let name = <scalar-expr>
  ```

### table bindings

* table bindings use `table`:

  ```
  table name = <table-expr>
  ```

### kind locking

* a name may not change kind (scalar ↔ table).

---

## datasource declarations

datasources declare external inputs and make them explicitly available to `from(...)`.

```
datasource name = csv("path/to/file.csv")
```

optional schema pinning:

```
datasource name = csv("path/to/file.csv", columns(a, b, c))
```

rules:

* datasource names live in a global file scope.
* datasource declarations are immutable.
* referencing an undeclared datasource is a compile-time error.
* datasource declarations are part of the deterministic IR inputs.
* column projection/filtering does **not** occur at the datasource level unless schema is explicitly pinned.

---

## table expressions

### sources

* table sources are declared datasources:

  ```
  from(datasource_name)
  ```

### pipeline blocks

```
from(datasource_name) do
  <pipeline-statement>*
end
```

* pipeline blocks define a linear sequence of table transforms.
* each statement sees the schema produced by all prior statements.

### postfix clauses

```
<table-expr> select a, b
<table-expr> filter expr
```

### builders

```
sort(table).by(a)
summary(table).class(a).var(b)
```

---

## pipeline statements

valid inside pipeline blocks and as postfix clauses:

* `rename(old -> new, ...)`
* `mutate(col = expr, ...)`
* `filter(expr)`
* `select col1, col2, ...`
* `drop col1, col2, ...`

rules:

* pipeline statements execute **top-to-bottom**.
* renamed or dropped columns leave scope immediately.
* assignments inside `mutate` are evaluated **sequentially**.
* cyclic dependencies inside a single `mutate` are compile-time errors.
* overwriting an existing column is disallowed without explicit override (e.g. `derive!`).

---

## semantic invariants

1. **linear scoping**: each statement sees the schema produced by the previous statement.
2. **column shadowing**: renamed/dropped columns are removed from scope immediately.
3. **strict mutation**: `mutate` cannot overwrite existing columns without explicit override.
4. **ternary `if`**: `if(cond, then, else)` requires 3 arguments and unified types.
5. **total maps**: `map` with `_` is total; partial maps error on missing keys.
6. **sort stability**: sorting is stable; `nodupkey(true)` keeps the first row.
7. **deterministic nulls**: nulls are smallest for sorting/comparison.
8. **aggregate naming**: `summary` uses `<input>_<stat>` (e.g. `x_mean`).
9. **kind locking**: `let` and `table` names are distinct and immutable by kind.
10. **terminality**: script must end with exactly one unnamed table expression.

---

## configuration builders (fluent ops)

some operations use a configuration builder pattern.

### sort

```
sort(table).by(col1, col2).nodupkey(true|false)
```

### summary

```
summary(table).class(col1, col2).var(col3, col4).stats(mean, sum)
```

rules:

* `sort(table)` and `summary(table)` return builder objects.
* fluent methods configure the operation only.
* default stats: `mean` if not specified.
* aggregate naming: `<var>_<stat>`.
* no data access occurs during configuration.
* each completed builder lowers to **one IR step**.

---

## expressions

* equality: `==` only.
* inequality: `!=` only.
* `=` is for assignment.
* lookup: `map_name[key]`

  * if key exists → value
  * if key missing and `_` exists → default
  * otherwise → runtime error

---
