# Sans Expression Contract (Strict)

This document defines the **strict** sans expression contract. Legacy SAS operators are not supported in strict mode.

**Supported Operators**

| Category | Tokens |
| --- | --- |
| Arithmetic | `+` `-` `*` `/` |
| Comparison | `==` `!=` `<` `<=` `>` `>=` |
| Boolean | `and` `or` `not` |

**Precedence (Low → High)**

1. `or`
2. `and`
3. `not` (unary)
4. comparisons (`==` `!=` `<` `<=` `>` `>=`)
5. `+` `-`
6. `*` `/`

All binary operators are left-associative. Parentheses can override precedence.

**Forbidden Tokens**

- Assignment equality: `=`
- Legacy SAS comparisons: `eq` `ne` `lt` `le` `gt` `ge`
- Legacy not-equal: `^=` `~=`
- ANSI not-equal: `<>`

**Other Allowed Tokens**

- Identifiers (column references), numeric literals, string literals, `null` / `.`
- Function calls: `coalesce`, `if`, `put`, `input`

