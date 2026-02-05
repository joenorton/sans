
# SANS Formatting Contract (FMT_CONTRACT.md)

## purpose

`sans fmt` is a **pure formatter**.

its sole responsibility is to normalize the *presentation* of a valid sans script without changing its meaning. it exists to improve readability, diff stability, and tooling ergonomics. it is **not** a semantic transformer.

this contract defines what `sans fmt` **guarantees**, **may do**, and **must never do**.

---

## core invariants (non-negotiable)

for any valid sans script `x`:

```
parse(x)  == parse(fmt(x))
expand(x) == expand(fmt(x))
```

if either invariant is violated, `fmt` is incorrect.

these invariants take precedence over all stylistic concerns.

---

## guarantees

`sans fmt` guarantees the following:

1. **non-semantic behavior**
   formatting does not change:

   * bindings
   * evaluation order
   * control flow
   * scoping
   * data dependencies
   * emitted artifacts

2. **idempotence**
   applying `fmt` repeatedly is stable:

   ```
   fmt(fmt(x)) == fmt(x)
   ```

3. **parse preservation**
   any script accepted by the parser before formatting is accepted after formatting, and produces an equivalent parsed structure.

4. **comment preservation**
   all comments are preserved verbatim:

   * comment text is unchanged
   * comment position (line vs inline) is preserved
   * fmt may adjust surrounding whitespace only as required by style rules

5. **scope preservation**
   indentation is cosmetic only; fmt does not introduce, remove, or reinterpret scope.

---

## allowed transformations

`sans fmt` **may** perform the following transformations:

### whitespace normalization

* normalize line endings to `\n`
* strip trailing whitespace
* collapse runs of blank lines to at most one
* ensure consistent indentation inside `do … end` blocks

### keyword casing

* normalize all language keywords and built-in operators to lowercase

### spacing normalization

* normalize spaces around:

  * assignment (`=`)
  * comparison (`==`, `!=`)
  * arrows (`->`)
  * commas (`, `)
* remove unnecessary spaces inside parentheses

### indentation

* apply consistent indentation using tabs (`\t`) inside block constructs
* indentation changes are purely presentational

### layout stabilization

* ensure one statement per line
* preserve existing multiline constructs unless explicitly normalized by the style spec

---

## forbidden transformations

`sans fmt` **must never**:

1. **change structure**

   * introduce or remove bindings
   * split or merge statements
   * reorder statements
   * reorder arguments or lists

2. **change semantics**

   * expand or desugar syntax
   * lower aliases to kernel vocabulary
   * introduce intermediate tables
   * rewrite postfix expressions into bindings
   * rewrite block forms into linear form

3. **change expressions**

   * add, remove, or reorder parentheses
   * rewrite literals
   * fold constants
   * normalize numeric representations
   * change quote style or string contents

4. **interpret or validate**

   * fmt does not perform semantic validation
   * fmt does not fix errors
   * fmt does not guess intent

5. **invent defaults**

   * fmt does not add missing headers
   * fmt does not insert implicit options
   * fmt does not infer omitted clauses

any transformation that affects meaning belongs to **expand / normalize**, not fmt.

---

## error handling

* if input fails parsing, `fmt` fails with a clear diagnostic
* fmt does not attempt partial formatting of invalid scripts
* fmt does not suppress or reinterpret parse errors

---

## modes

`sans fmt` may support multiple modes, all subject to this contract:

* **default mode**: canonical presentation per style spec
* **identity mode**: byte-preserving round-trip (except newline normalization)
* **check mode**: no output; exits non-zero if formatting would change output

all modes must preserve the core invariants.

---

## relationship to other tools

* `fmt` is **presentation only**
* `expand` is **semantic lowering**
* `check` is **validation**

these responsibilities must not overlap.

if a change would violate the invariants in this document, it does not belong in `fmt`.

---

## guiding principle

> **fmt arranges; it never explains.**

any code change that makes `fmt` “smarter” at the cost of safety is a regression.

---
