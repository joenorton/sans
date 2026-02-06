# SANS Error Codes

This document lists the stable error codes used by SANS.

## Namespace Overview

| Namespace | Exit Code | Description |
| :--- | :--- | :--- |
| `SANS_PARSE_*` | 30 | Syntax errors, malformed constructs, or unsupported grammar. |
| `SANS_BLOCK_*` | 30 | Errors related to block segmentation (e.g., missing run). |
| `SANS_VALIDATE_*` | 31 | IR validation errors (e.g., missing inputs, table undefined). |
| `SANS_CAP_*` | 32 | Supported syntax but unsupported capability/operation. |
| `SANS_RUNTIME_*` | 40 | Runtime execution errors (e.g., file not found, type mismatch). |
| `SANS_IO_*` | 50 | High-level I/O or system errors. |
| `SANS_INTERNAL_*` | 50 | Unexpected internal compiler/runtime faults. |

## Error Code Catalog

### Parse Errors (`SANS_PARSE_*`)

*   `SANS_PARSE_INVALID_DATA_BLOCK_HEADER`: DATA statement is malformed.
*   `SANS_PARSE_INVALID_PROC_SORT_HEADER`: PROC SORT statement is malformed.
*   `SANS_PARSE_INVALID_PROC_TRANSPOSE_HEADER`: PROC TRANSPOSE statement is malformed.
*   `SANS_PARSE_INVALID_PROC_SQL_HEADER`: PROC SQL statement is malformed.
*   `SANS_PARSE_INVALID_PROC_FORMAT_HEADER`: PROC FORMAT statement is malformed.
*   `SANS_PARSE_INVALID_PROC_SUMMARY_HEADER`: PROC SUMMARY statement is malformed.
*   `SANS_PARSE_DATASET_SPEC_MALFORMED`: Dataset specification (table name/options) is invalid.
*   `SANS_PARSE_DATASET_OPTION_UNKNOWN`: Unknown dataset option (e.g., other than keep/drop/rename/where/in).
*   `SANS_PARSE_DATASET_OPTION_MALFORMED`: Syntax error within dataset options.
*   `SANS_PARSE_EXPRESSION_ERROR`: Expression syntax error.
*   `SANS_PARSE_LOOP_BOUND_UNSUPPORTED`: DO loop bounds are not supported (non-integer, step=0, or malformed).
*   `SANS_PARSE_UNSUPPORTED_DATASTEP_FORM`: Unsupported statement inside DATA step.
*   `SANS_PARSE_UNSUPPORTED_PROC`: Unsupported PROC (e.g., PROC PRINT).
*   `SANS_PARSE_UNSUPPORTED_STATEMENT`: Unsupported top-level statement (e.g., standalone assignment).
*   `SANS_PARSE_SET_STATEMENT_MALFORMED`: Invalid SET statement.
*   `SANS_PARSE_MERGE_STATEMENT_MALFORMED`: Invalid MERGE statement.
*   `SANS_PARSE_BY_STATEMENT_MALFORMED`: Invalid BY statement.
*   `SANS_PARSE_RETAIN_STATEMENT_MALFORMED`: Invalid RETAIN statement.
*   `SANS_PARSE_KEEP_STATEMENT_MALFORMED`: Invalid KEEP statement.
*   `SANS_PARSE_RENAME_MALFORMED`: Invalid RENAME statement.
*   `SANS_PARSE_DATASTEP_MISSING_BY`: Missing BY statement when required (e.g., for MERGE).
*   `SANS_PARSE_SORT_UNSUPPORTED_OPTION`: Unsupported option in PROC SORT.
*   `SANS_PARSE_SORT_MISSING_DATA`: Missing `data=` in PROC SORT.
*   `SANS_PARSE_SORT_MISSING_OUT`: Missing `out=` in PROC SORT.
*   `SANS_PARSE_SORT_MISSING_BY`: Missing BY statement in PROC SORT.
*   `SANS_PARSE_SORT_UNSUPPORTED_BODY_STATEMENT`: Unsupported statement inside PROC SORT.
*   `SANS_PARSE_TRANSPOSE_MISSING_VAR`: Missing VAR statement in PROC TRANSPOSE.
*   `SANS_PARSE_TRANSPOSE_MISSING_ID`: Missing ID statement in PROC TRANSPOSE.
*   `SANS_PARSE_SQL_UNSUPPORTED_FORM`: Unsupported SQL syntax (e.g., subquery, select *).
*   `SANS_PARSE_SQL_TABLE_MALFORMED`: Invalid table reference in SQL.

### Block Errors (`SANS_BLOCK_*`)

*   `SANS_BLOCK_STATEFUL_TOKEN`: Usage of stateful token (e.g., `first.x`) outside valid context.

### Validation Errors (`SANS_VALIDATE_*`)

*   `SANS_VALIDATE_TABLE_UNDEFINED`: Referenced table does not exist in context.
*   `SANS_VALIDATE_OUTPUT_TABLE_COLLISION`: Multiple steps write to the same output table.
*   `SANS_VALIDATE_ORDER_REQUIRED`: Input table not sorted by required keys.
*   `SANS_VALIDATE_KEYS_REQUIRED`: Missing BY keys.
*   `SANS_VALIDATE_PROFILE_UNSUPPORTED`: Validation profile (e.g., SDTM) not supported.

### Capability Errors (`SANS_CAP_*`)

*   `SANS_CAP_UNSUPPORTED_OP`: IR operation not supported by current runtime (should be rare).

### Runtime Errors (`SANS_RUNTIME_*`)

*   `SANS_RUNTIME_INPUT_NOT_FOUND`: Input file not found.
*   `SANS_RUNTIME_TABLE_UNDEFINED`: Table binding missing.
*   `SANS_RUNTIME_ORDER_REQUIRED`: Runtime check failed for sorted input.
*   `SANS_RUNTIME_SORT_UNSUPPORTED`: Sort direction/collation not supported.
*   `SANS_RUNTIME_DATASET_OPTION_CONFLICT`: Conflicting options (e.g., keep and drop).
*   `SANS_RUNTIME_UNSUPPORTED_EXPR_NODE`: Expression node type not supported.
*   `SANS_RUNTIME_UNSUPPORTED_DATASTEP`: Unsupported logic in DATA step execution.
*   `SANS_RUNTIME_MERGE_MANY_MANY`: Many-to-many merge detected (fatal).
*   `SANS_RUNTIME_TRANSPOSE_MISSING_ARGS`: Runtime check for missing args.
*   `SANS_RUNTIME_TRANSPOSE_ID_MISSING`: Missing ID value in row.
*   `SANS_RUNTIME_TRANSPOSE_ID_COLLISION`: Duplicate ID value in group.
*   `SANS_RUNTIME_SQL_MALFORMED`: Malformed SQL execution state.
*   `SANS_RUNTIME_LOOP_LIMIT`: Loop exceeded maximum iteration cap.
*   `SANS_RUNTIME_LOOP_STEP_INVALID`: Loop step/bounds invalid at runtime.
*   `SANS_RUNTIME_CONTROL_DEPTH`: Control-flow nesting exceeded maximum depth.
*   `SANS_RUNTIME_SQL_COLUMN_UNDEFINED`: Column not found in SQL source.
*   `SANS_RUNTIME_SQL_AMBIGUOUS_COLUMN`: Ambiguous column reference in SQL.
*   `SANS_RUNTIME_FORMAT_UNSUPPORTED`: Unsupported format usage.
*   `SANS_RUNTIME_FORMAT_UNDEFINED`: Format not found.
*   `SANS_RUNTIME_FORMAT_MALFORMED`: Format definition invalid.
*   `SANS_RUNTIME_INFORMAT_UNSUPPORTED`: Informat not supported.
*   `SANS_RUNTIME_ASSIGN_OVERWRITE`: Assignment overwrote an existing column without explicit permission.
*   `SANS_RUNTIME_XPT_INVALID`: XPT is corrupted or missing required headers.
*   `SANS_RUNTIME_XPT_UNSUPPORTED`: XPT uses unsupported features or types.
*   `SANS_RUNTIME_XPT_LENGTH_EXCEEDED`: Char length exceeds XPT policy cap.
*   `SANS_RUNTIME_XPT_LABEL_FORMAT_IGNORED`: XPT labels/formats ignored (warning).

### Schema Lock / CSV Ingestion (`E_*`)

*   `E_SCHEMA_REQUIRED`: A referenced CSV datasource has no typed columns and no entry in the provided schema lock. Provide `--schema-lock` or typed `columns(...)` in the datasource declaration.
*   `E_SCHEMA_MISSING_COL`: Input CSV is missing one or more columns required by the schema lock.
*   `E_SCHEMA_LOCK_NOT_FOUND`: `--schema-lock` was supplied but the file was not found or could not be parsed (relative paths are resolved against the script directory).
*   `E_SCHEMA_LOCK_MISSING_DS`: `--schema-lock` was supplied and read, but the lock does not contain an entry for one or more referenced untyped datasources.
*   `E_SCHEMA_LOCK_INVALID`: A schema-lock entry (or pinned columns) contains an unknown or invalid column type (e.g. `unknown`). All ingress columns must have concrete types (int, decimal, string, bool, date, etc.).
*   `E_CSV_COERCE`: A locked or typed column value could not be coerced to the expected type (see `coercion_diagnostics` in runtime.evidence.json).

## Canonical Examples

### Macro Error (Refused)

```json
{
  "code": "SANS_PARSE_MACRO_ERROR",
  "message": "Unsupported macro control flow: %do.",
  "loc": {"file": "script.sas", "line_start": 5, "line_end": 5}
}
```

### Unsupported Procedure

```json
{
  "code": "SANS_PARSE_UNSUPPORTED_PROC",
  "message": "Unsupported PROC statement: 'proc print;'. Hint: supported procs include SORT, TRANSPOSE, SQL, FORMAT, and SUMMARY.",
  "loc": {"file": "script.sas", "line_start": 10, "line_end": 10}
}
```

### Missing Keys for Merge

```json
{
  "code": "SANS_PARSE_DATASTEP_MISSING_BY",
  "message": "MERGE statement requires a BY statement.",
  "loc": {"file": "script.sas", "line_start": 3, "line_end": 3}
}
```

### Order Required (Runtime)

```json
{
  "code": "SANS_RUNTIME_ORDER_REQUIRED",
  "message": "Input table 'sorted' is not sorted by ['id'].",
  "loc": {"file": "script.sas", "line_start": 4, "line_end": 6}
}
```
