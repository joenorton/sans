# Sans Types v0 (Strict)

This document defines the strict type system for sans expressions and schema evolution.

**Type Set**

`null`, `bool`, `int`, `decimal`, `string`, `unknown`

`unknown` is used when a column's type is not known (e.g., from CSV headers without a typed schema). It **does not** bypass strict checks and does not enable implicit coercions.

**Numeric Promotion**

- `int + int` → `int` (same for `-`, `*`)
- `int + decimal` → `decimal`
- `decimal + decimal` → `decimal`
- `/` always returns `decimal` when operands are numeric

**Null Rules**

- Arithmetic with `null` → `E_TYPE`
- Comparisons:
  - `==`/`!=` allow `null` against any type → `bool`
  - `<` `<=` `>` `>=` with `null` → `E_TYPE`
- Boolean ops with `null` → `E_TYPE`

**Boolean Rules**

- `and` / `or` / `not` require `bool` operands
- No truthiness for numeric/string types

**if(cond, then, else)**

- `cond` must be `bool`
- `then` and `else` must unify:
  - same type → that type
  - `int` + `decimal` → `decimal`
  - `null` + `T` → `T` (only for `if()` unification)
  - otherwise → `E_TYPE`

**Unknown Discipline**

- Arithmetic with `unknown` â†’ `E_TYPE_UNKNOWN`
- Ordered comparisons (`<`, `<=`, `>`, `>=`) with `unknown` â†’ `E_TYPE_UNKNOWN`
- Equality:
  - `unknown == null` / `unknown != null` â†’ `bool`
  - `unknown == unknown` / `unknown != unknown` â†’ `bool`
  - `unknown == T` where `T` is not `null`/`unknown` â†’ `E_TYPE_UNKNOWN`
- Boolean ops with `unknown` â†’ `E_TYPE_UNKNOWN`
- `if(cond, then, else)`:
  - `cond` must be `bool`; `unknown` cond â†’ `E_TYPE_UNKNOWN`
  - `unify(unknown, T)` â†’ `unknown` (no silent promotion)

**Schema Evolution**

- `derive`/`update!`: result column type is inferred from expression
- `rename`: preserves types
- `drop`/`select`: project types accordingly
- `filter`/`assert`: predicate must be `bool`

**Typed Datasource Pinning**

You can optionally pin column types at datasource declaration:

```
datasource name = csv("path.csv", columns(a:int, b:string, c:decimal))
datasource name = inline_csv columns(a:int, b:int) do
  a,b
  1,2
end
```

Unannotated columns default to `unknown`.
