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

* single scalar binding uses `let`:

  ```
  let name = <scalar-expr>
  ```

* multiple named scalar **literals** use `const` (compile-time only; int, decimal, string, bool, null). Decimal literals are exact (e.g. 3.14); no exponent notation. Canonical IR: `{type: "decimal", value: "<string>"}`.

  ```
  const { name = literal, ... }
  ```

* `let` is for one binding; for multiple literals use `const { ... }`. Map-style `let` is not in the kernel.

### table bindings

* table bindings use `table`:

  ```
  table name = <table-expr>
  ```
* whitespace/newlines around `=` are flexible; the table expression may start on the next non-empty line.

### save (explicit output)

* outputs are created only via **save**:

  ```
  save table_name to "path"
  save table_name to "path" as "name"
  ```

* only **save** creates output artifacts; there is no implicit "last table wins".

### assert

* assertions evaluate a predicate and emit evidence (no side effect on data):

  ```
  assert <predicate>
  ```

* e.g. `assert row_count(t) > 0`, `assert not_null(t, (a,b))` (when supported).

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

typed schema pinning (optional):

```
datasource name = csv("path/to/file.csv", columns(a:int, b:string, c:decimal))
```

allowed type names: `null`, `bool`, `int`, `decimal`, `string` (`str` is accepted as an alias for `string`).

rules:

* datasource names live in a global file scope.
* datasource declarations are immutable.
* referencing an undeclared datasource is a compile-time error.
* datasource declarations are part of the deterministic IR inputs.
* column projection/filtering does **not** occur at the datasource level unless schema is explicitly pinned.
* whitespace/newlines around `=` are flexible; the datasource expression may start on the next non-empty line.

---

## table expressions

### sources

* table sources are declared datasources **or** previously bound tables:

  ```
  from(datasource_name)
  from(table_name)
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
aggregate(table).class(a).var(b)
```

---

## pipeline statements

valid inside pipeline blocks and as postfix clauses:

* `rename(old -> new, ...)`
* `derive(col = expr, ...)` — new columns only
* `update!(col = expr, ...)` — existing columns only (overwrite)
* `filter(expr)`
* `select col1, col2, ...`
* `drop col1, col2, ...`

rules:

* pipeline statements execute **top-to-bottom**.
* renamed or dropped columns leave scope immediately.
* **derive** creates new columns only; target must not already exist (compile-time error if it does).
* **update!** overwrites existing columns only; target must exist (compile-time error if it does).
* assignments inside a single derive/update! statement are evaluated **sequentially**.
* cyclic dependencies inside a single statement are compile-time errors.
* block form `derive do ... end` is sugar for a sequence of derive/update! steps; expanded/canonical form uses one statement per column op.

---

## semantic invariants

1. **linear scoping**: each statement sees the schema produced by the previous statement.
2. **column shadowing**: renamed/dropped columns are removed from scope immediately.
3. **compute primitives**: column creation and overwrite use **derive** (new only) and **update!** (existing only); no other construct may create or overwrite columns.
4. **ternary `if`**: `if(cond, then, else)` requires 3 arguments and unified types.
5. **total maps**: `map` with `_` is total; partial maps error on missing keys.
6. **sort stability**: sorting is stable; `nodupkey(true)` keeps the first row.
7. **deterministic nulls**: nulls are smallest for sorting/comparison.
8. **aggregate naming**: `aggregate` uses `<input>_<stat>` (e.g. `x_mean`).
9. **kind locking**: `let`, `const`, and `table` names are distinct and immutable by kind.
10. **explicit output**: there is no implicit output; outputs are defined only via **save**. Scripts may end with zero or more named bindings and optional terminal expression; a terminal expression alone does not produce an output artifact.

---

## configuration builders (fluent ops)

some operations use a configuration builder pattern.

### sort

```
sort(table).by(col1, col2).nodupkey(true|false)
```

### aggregate

```
aggregate(table).class(col1, col2).var(col3, col4).stats(mean, sum)
```

rules:

* `sort(table)` and `aggregate(table)` return builder objects.
* fluent methods configure the operation only.
* default stats: `mean` if not specified.
* aggregate naming: `<var>_<stat>`.
* no data access occurs during configuration.
* each completed builder lowers to **one IR step**.
* `summary(...)` may exist as legacy input sugar only; it lowers to `aggregate`. Expanded form uses **aggregate** only.

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

## canonical (expanded) form

**expanded.sans** is the canonical human-readable form: fully explicit, no macros, no dataset options, no procs, no implicit defaults.

* **Kernel vocabulary only**: `from(datasource)`, `derive(...)`, `update!(...)`, `select`, `drop`, `rename`, `filter`, `sort`, `aggregate`, `const`, `let`, `assert`, `save`. No aliases (e.g. no `mutate`, no `compute`, no `summary` in expanded output).
* **No block form in expanded**: each compute is a single statement; block `derive do ... end` is expanded to one statement per column op.
* **No postfix-only at top level**: linear chain of named tables, e.g. `table s1 = from(raw)`, `table s2 = s1 derive(...)`, etc.
* **Explicit output**: only **save** creates output artifacts; no implicit "last table wins".

---
