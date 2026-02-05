
## sans fmt canonical style v0 (grammar-aligned)

### 0. header

* ensure version marker exists and is within first 5 non-empty lines:

  * `# sans 0.1`
* fmt does not invent a different version; it can insert the current known version if missing only if you explicitly allow that (i’d default to **error** in `--check`, optional `--fix-header`).

### 1. newlines + whitespace

* normalize line endings to `\n`
* strip trailing whitespace
* collapse runs of >1 blank line to 1 blank line
* no blank lines at start/end of file

### 2. indentation: tabs, but cosmetic

since scope is only `do … end`, indentation cannot affect semantics. still, fmt should produce stable indentation for readability/diffs:

* indent with **tabs** (`\t`)
* indent level increases by 1 inside any `do … end` block
* `end` aligns with the construct that opened the block

### 3. keyword casing

* all keywords and builtins emitted by fmt are lowercase:

  * `datasource`, `table`, `from`, `do`, `end`, `derive`, `update!`, `rename`, `filter`, `select`, `drop`, `sort`, `aggregate`, `const`, `let`, `assert`, `save`, `to`, `as`, builder methods `by`, `nodupkey`, `class`, `var`, `stats`
* identifiers (names of tables, datasources, cols, scalars) are not case-normalized

### 4. spaces around operators and punctuation

* assignment: `let x = expr`, `table t = expr`, `datasource d = expr`

  * exactly one space on both sides of `=`
* equality/inequality in expressions: spaces around `==` and `!=`
* commas: no space before, one space after: `a, b, c`
* arrows in rename: **one space** around `->`: `rename(a -> b, c -> d)`
* parentheses: no internal padding: `from(raw)`, `filter(x == 1)`
* bang token is lexical: `update!(` no spaces inserted: keep `update!` glued

### 5. top-level forms (canonical layout)

fmt should print each top-level statement on its own line:

* `const { ... }`:

  * if fits on one line under a hard cap (say 100–120 chars), keep single-line:

    * `const { a = 1, b = "x" }`
  * else expand:

    ```
    const {
    	a = 1,
    	b = "x",
    }
    ```
  * trailing comma in multiline is allowed/encouraged for diffs

* `let`:

  * always single-line unless the scalar expr is already multiline (fmt should not wrap scalar exprs yet):

    * `let name = <scalar-expr>`

* `datasource`:

  * prefer single-line:

    * `datasource raw = csv("x.csv")`
    * `datasource raw = csv("x.csv", columns(a, b, c))`
  * if the rhs starts on the next line in input, fmt may pull it back up if it fits, otherwise:

    ```
    datasource raw =
    	csv("x.csv", columns(a, b, c))
    ```

* `table` binding:

  * prefer `table name = <table-expr>` on one line when possible
  * if table-expr is a pipeline block or would exceed line cap, break after `=`:

    ```
    table t =
    	from(raw) do
    		...
    	end
    ```

* `save`:

  * single-line:

    * `save t to "path"`
    * `save t to "path" as "name"`

* `assert`:

  * single-line: `assert predicate`

### 6. table expressions and pipelines

#### `from(...) do ... end` block

canonical form:

```
from(raw) do
	<pipeline-statement>
	<pipeline-statement>
end
```

pipeline statements:

* `rename(...)`, `derive(...)`, `update!(...)`, `filter(expr)`, `select a, b`, `drop a, b`
* canonicalize sugar if you already lower it elsewhere? **fmt should not** change sugar vs expanded unless you explicitly decide fmt is “expander-lite”.

  * i recommend: `fmt` does **not** desugar; that’s `sans expand`.

#### postfix clauses

your grammar allows:

* `<table-expr> select a, b`
* `<table-expr> filter expr`

fmt rule:

* keep postfix on same line if possible:

  * `table t = from(raw) select a, b`
* if it runs long, break with a hanging indent (still cosmetic):

  ```
  table t =
  	from(raw)
  		select a, b
  ```

but: because postfix can chain, you need a deterministic wrapping rule. simplest: **one postfix per line when multiline**.

### 7. builders

canonical:

* `sort(t).by(a, b).nodupkey(true)`
* `aggregate(t).class(a).var(b, c).stats(mean, sum)`

rules:

* no spaces around `.`
* method args formatted like function args: `a, b`
* if chained call exceeds cap, break before `.` with hanging indent:

  ```
  table s =
  	sort(t)
  		.by(a, b)
  		.nodupkey(true)
  ```

### 8. comments

* preserve comment text exactly
* full-line comments keep their indentation level
* inline comments: ensure **two spaces** before `#` if there is code before it:

  * `let x = 1  # note`

### 9. explicit non-goals (still)

* no reordering of lists (`select`, `drop`, `columns(...)`, builder args)
* no rewriting expressions (no added/removed parentheses, no constant folding)
* no converting block `derive do ... end` into expanded per-column statements (that’s `expand`)
* no changing quote style or escaping

---