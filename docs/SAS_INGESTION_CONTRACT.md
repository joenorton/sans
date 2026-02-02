# SAS ingestion contract

SANS accepts SAS-like syntax only insofar as it compiles into a **static DAG** of kernel ops. This document lists allowed constructs (and how they lower), rejected constructs, and restricted constructs.

## Invariants

- **Static DAG only**: no runtime graph mutation.
- **Explicit I/O**: inputs = datasources/predeclared tables; outputs = only those named in **save** steps.
- **Determinism**: expressions pure; no time/random/env; ordering and nulls defined in [DETERMINISM.md](../DETERMINISM.md).

## Allowed SAS constructs (and lowering)

| SAS construct | Lowers to |
|---------------|-----------|
| DATA step `set` | identity / kernel table ops |
| DATA step `keep`/`drop`/`rename` | select, rename |
| DATA step `where` | filter |
| DATA step `if/then/else` (row-level) | case inside compute |
| `first.`/`last.`, `retain` | only when they lower to explicit sort + window semantics |
| Dataset options `(keep=, rename=, where=)` | explicit **select**, **rename**, **filter** steps (no options in expanded.sans) |
| PROC SORT | sort |
| PROC SUMMARY | summary (aggregate) |
| PROC SQL | sql_select (join, group by, etc.) |
| PROC FORMAT | format (let_scalar / map) |
| PROC TRANSPOSE | transpose |
| `%let` | let_scalar (when inlined) |

## Rejected SAS constructs

| SAS feature | Policy | Rationale |
|-------------|--------|-----------|
| Implicit output / "last dataset wins" | **Reject** | All persistence via **save** only. |
| `%if`/`%then`/`%else` (macro) that changes graph shape | **Reject** | Static DAG only; no conditional steps. |
| `%include` undeclared / unhashed | **Reject or restrict** | Only declared, hashed, inlined includes. |
| Non-determinism (time, random, env, discovery) | **Reject** | Expressions pure and deterministic. |

## Restricted SAS constructs

| SAS feature | Policy | Rationale |
|-------------|--------|-----------|
| Dataset options `(keep=, rename=, where=)` | **Restrict** | Accept in ingestion; **must** lower to explicit **select**, **rename**, **filter** steps (no options in expanded.sans). |
| Data step `if/then/else` (row-level) | **Restrict** | Expression-level only; lower to `case` inside compute. |
| `first.`/`last.`, `retain` | **Restrict** | Only when they lower to explicit **sort** + window/semantics. |
| Procs (sort, summary, sql, etc.) | **Restrict** | Redefined in SANS kernel terms; stable semantics; lower to kernel ops (sort, aggregate, join, etc.). |

## Refusal codes

When a rejected construct is detected, the compiler returns an IRDoc whose first step is an `UnknownBlockStep` with a stable `code`:

- **SANS_REFUSAL_MACRO_GRAPH** — `%if`/`%then`/`%else` or other macro that mutates graph shape.
- **SANS_REFUSAL_IMPLICIT_OUTPUT** — Implicit output expectation (SANS script path: use **save**).
- **SANS_REFUSAL_INCLUDE_UNDECLARED** — `%include` undeclared or unhashed.
- **SANS_REFUSAL_NONDETERMINISM** — time/random/env or discovery.

## Acceptance

Accepted SAS scripts produce IR that has only kernel ops: datasource, identity, compute, filter, select, rename, sort, summary, sql_select, format, transpose, save, assert, let_scalar. No hidden dataset options; options are lowered to explicit steps.
