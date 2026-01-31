from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import re

from .runtime import _load_csv


ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class ValidationIssue:
    code: str
    message: str
    table: str
    column: Optional[str] = None
    row: Optional[int] = None
    value: Optional[Any] = None


def _add_issue(
    issues: List[ValidationIssue],
    code: str,
    message: str,
    table: str,
    column: Optional[str] = None,
    row: Optional[int] = None,
    value: Optional[Any] = None,
) -> None:
    issues.append(
        ValidationIssue(
            code=code,
            message=message,
            table=table,
            column=column,
            row=row,
            value=value,
        )
    )


def _required_columns(table: str) -> list[str]:
    if table == "DM":
        return ["DOMAIN", "USUBJID", "SUBJID", "SITEID", "SEX", "RACE"]
    if table == "AE":
        return ["DOMAIN", "USUBJID", "AEDECOD", "AESTDTC"]
    if table == "LB":
        return ["DOMAIN", "USUBJID", "LBTESTCD", "LBDTC", "LBSTRESN"]
    return []


def _date_columns(table: str, columns: list[str]) -> list[str]:
    if table == "AE":
        return ["AESTDTC"] if "AESTDTC" in columns else []
    if table == "LB":
        return ["LBDTC"] if "LBDTC" in columns else []
    if table == "DM":
        return ["RFSTDTC"] if "RFSTDTC" in columns else []
    return []


def validate_sdtm(bindings: Dict[str, str], out_dir: Path) -> Dict[str, Any]:
    issues: List[ValidationIssue] = []
    tables_seen: List[str] = []

    for table_name, path_str in bindings.items():
        table_upper = table_name.upper()
        tables_seen.append(table_upper)
        if table_upper not in {"DM", "AE", "LB"}:
            continue

        path = Path(path_str)
        if not path.exists():
            _add_issue(
                issues,
                code="SANS_VALIDATE_INPUT_NOT_FOUND",
                message=f"Input table '{table_upper}' file not found: {path_str}",
                table=table_upper,
            )
            continue

        rows = _load_csv(path)
        columns = list(rows[0].keys()) if rows else []

        for col in _required_columns(table_upper):
            if col not in columns:
                _add_issue(
                    issues,
                    code="SDTM_REQUIRED_COLUMN_MISSING",
                    message=f"Required column '{col}' missing from {table_upper}.",
                    table=table_upper,
                    column=col,
                )

        if "DOMAIN" in columns:
            for idx, row in enumerate(rows, start=1):
                value = row.get("DOMAIN")
                if value != table_upper:
                    _add_issue(
                        issues,
                        code="SDTM_DOMAIN_VALUE_INVALID",
                        message=f"DOMAIN value '{value}' does not match {table_upper}.",
                        table=table_upper,
                        column="DOMAIN",
                        row=idx,
                        value=value,
                    )

        if "USUBJID" in columns:
            for idx, row in enumerate(rows, start=1):
                value = row.get("USUBJID")
                if value is None or (isinstance(value, str) and value.strip() == ""):
                    _add_issue(
                        issues,
                        code="SDTM_USUBJID_MISSING",
                        message="USUBJID is empty or missing.",
                        table=table_upper,
                        column="USUBJID",
                        row=idx,
                        value=value,
                    )

        for col in _date_columns(table_upper, columns):
            for idx, row in enumerate(rows, start=1):
                value = row.get(col)
                if value is None or not ISO_DATE_RE.match(str(value)):
                    _add_issue(
                        issues,
                        code="SDTM_DATE_INVALID",
                        message=f"{col} is not ISO8601 date (YYYY-MM-DD).",
                        table=table_upper,
                        column=col,
                        row=idx,
                        value=value,
                    )

    status = "ok" if not issues else "failed"
    report = {
        "status": status,
        "exit_code_bucket": 0 if status == "ok" else 31,
        "profile": "sdtm",
        "tables": tables_seen,
        "diagnostics": [
            {
                "code": issue.code,
                "message": issue.message,
                "table": issue.table,
                "column": issue.column,
                "row": issue.row,
                "value": issue.value,
            }
            for issue in issues
        ],
        "summary": {"errors": len(issues)},
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "validation.report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
