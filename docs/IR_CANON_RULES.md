# IR canonical rules (param-compat hot zone)

**Revoke previous doctrine (IR_CANONICAL_PARAMS.md):** Remove "sugar must be normalized in validate." **Refused at ingress; lowering only in frontends.** No normalization inside the nucleus.

## Scope

- sans.ir and IRDoc are **canonical editable IR**; no legacy/sugar shapes inside the nucleus.
- **harden_irdoc** is the **canonical-shape gate** (refuse-only). It runs at every IRDoc ingress and refuses legacy keys/shapes with `SANS_IR_CANON_*`.
- **validate()**: Semantic checks only; **must not normalize**. It may **assert** canonical shape for safety (read-only) but must never **rewrite** `step.params`.
- **assert_canon_params(op, params, loc)**: Refuse-only helper used by harden_irdoc. Raises `UnknownBlockStep` with `SANS_IR_CANON_*` on forbidden key or wrong shape. No "returns canonical dict" in nucleus; any such helper is frontend-only (e.g. `lower_*`).
- **Lowering**: Allowed **only** at frontends (e.g. SAS recognizer). Frontends must produce canonical ops/params before building IRDoc. No lowering inside `IRDoc.validate()`.
- **Harden gate placement:** Enforce by construction. The adapter (e.g. single entry that builds IRDoc from sans.ir) calls harden_irdoc internally. Compile returns IRDoc only via a function that calls harden_irdoc. Do not sprinkle "remember to call it" at each caller.

## Per-op canonical params (hot zone only)

Only the ops that previously normalized in validate are listed here. All other ops: **see IR_SPEC** for semantics.

| op        | canonical keys       | shape                                                       | notes                                      |
| --------- | -------------------- | ----------------------------------------------------------- | ------------------------------------------ |
| select    | cols **or** drop      | list[str]                                                   | exactly one, non-empty; no keep             |
| rename    | mapping              | list[{from: str, to: str}]                                  | no dict, mappings, map                      |
| sort      | by                   | list[{col: str, desc: bool}]                               | no list[str], no asc                        |
| aggregate | group_by, metrics     | group_by = list[str]; metrics = list[{name, op, col}]       | no class, var, vars, stats, autoname, naming; see metrics ordering below |
| cast      | casts                | list[{col, to, on_error, trim}]                             | to ∈ allowed types                         |
| drop      | cols                 | list[str]                                                   | no `drop` alias                             |
| compute   | mode, assignments     | mode ∈ {derive, update}; assignments = list[{target, expr}] | no `assign`                                 |

*format.map* remains canonical for the **format** op (not an alias for rename). Other ops: **see IR_SPEC**; not in this allowlist for this sprint.

### aggregate.metrics ordering (contract)

When frontends lower summary/aggregate sugar to canonical `aggregate`, the **metrics** list order is fixed for determinism and testability:

- **Order:** vars (columns) first, then stats (ops). For each column in the var list, emit one metric per stat in order: `for col in vars: for op in stats: append {name: f"{col}_{op}", op, col}`.
- **Example:** vars = [x, y], stats = [mean] → metrics = [{"name":"x_mean","op":"mean","col":"x"}, {"name":"y_mean","op":"mean","col":"y"}]. vars = [x], stats = [mean, sum] → metrics = [{"name":"x_mean",...}, {"name":"x_sum",...}].
- Producers (SANS lower, SAS recognizer) must emit metrics in this order. Consumers (runtime, hash, tests) may rely on it.

## Forbidden legacy keys (nucleus refusal)

- **Global (any op):** `keep_raw`, `drop_raw`, `summary` (if used as aggregate alias).
- **select:** `keep`
- **rename:** `mappings`, `map` (for rename only), dict root
- **sort:** `asc` (in by[i]); list[str] for `by`
- **aggregate:** `class`, `var`, `vars`, `stats`, `autoname`, `naming`
- **drop:** `drop` (key for column list)
- **compute:** `assign`

## Refusal codes

| Code | Use |
|------|-----|
| **SANS_IR_CANON_*** | Canonical-shape violations (forbidden key, wrong shape). Do not use for semantic validation. |
| SANS_IR_CANON_FORBIDDEN_KEY | Step has a forbidden param key for this op. |
| SANS_IR_CANON_SHAPE_SELECT | select: missing or invalid cols/drop; or has keep. |
| SANS_IR_CANON_SHAPE_RENAME | rename: missing/invalid mapping; or has mappings/map/dict. |
| SANS_IR_CANON_SHAPE_SORT | sort: missing/invalid by; list[str] or asc. |
| SANS_IR_CANON_SHAPE_AGGREGATE | aggregate: missing/invalid group_by or metrics; or has class/var/vars/stats/autoname/naming. |
| SANS_IR_CANON_SHAPE_CAST | cast: missing/invalid casts. |
| SANS_IR_CANON_SHAPE_DROP | drop: missing/invalid cols; or has drop key. |
| SANS_IR_CANON_SHAPE_COMPUTE | compute: missing/invalid assignments; or has assign. |

| Code | Use |
|------|-----|
| **SANS_VALIDATE_*** | Semantic validation failures (undefined table, output collision, sort missing by, BY order, type inference). Do not reuse for canonical-shape violations. |

## Decimal constants

Decimal constants: `{type: "decimal", value: "<string>"}` (exact decimal; no Python float). Hashing/canonicalization uses the string value.
