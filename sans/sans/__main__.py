from __future__ import annotations

import argparse
import sys
import json
from pathlib import Path

from .compiler import emit_check_artifacts
from .runtime import run_script
from .validator_sdtm import validate_sdtm
from . import __version__ as _engine_version


def _parse_tables(tables_arg: str | None) -> set[str] | None:
    if not tables_arg:
        return None
    tables = [t.strip() for t in tables_arg.split(",") if t.strip()]
    return set(tables) if tables else None


def _write_failed_report(out_dir: Path, message: str) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    plan_path = out_dir / "plan.ir.json"
    report_path = out_dir / "report.json"

    plan_path.write_text(json.dumps({"steps": [], "tables": [], "table_facts": {}}, indent=2), encoding="utf-8")
    report = {
        "status": "failed",
        "exit_code_bucket": 50,
        "primary_error": {"code": "SANS_IO_ERROR", "message": message, "loc": None},
        "diagnostics": [],
        "inputs": [],
        "outputs": [
            {"path": str(plan_path), "sha256": None},
            {"path": str(report_path), "sha256": None},
        ],
        "plan_path": str(plan_path),
        "engine": {"name": "sans", "version": _engine_version},
        "settings": {"strict": True, "allow_approx": False, "tolerance": None, "tables": []},
        "timing": {"compile_ms": None, "validate_ms": None},
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"failed: {message}")
    return 50


def _write_failed_validation_report(out_dir: Path, message: str) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "validation.report.json"
    report = {
        "status": "failed",
        "exit_code_bucket": 31,
        "profile": None,
        "tables": [],
        "diagnostics": [
            {"code": "SANS_VALIDATE_PROFILE_UNSUPPORTED", "message": message}
        ],
        "summary": {"errors": 1},
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"failed: {message}")
    return 31


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sans", description="SANS compiler/checker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="Compile and validate a script")
    check_parser.add_argument("script", help="Path to the script file")
    check_parser.add_argument("--out", required=True, help="Output directory for plan/report")
    check_parser.add_argument("--tables", default="", help="Comma-separated list of predeclared tables")
    check_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    check_parser.add_argument("--no-strict", dest="strict", action="store_false")

    run_parser = subparsers.add_parser("run", help="Compile, validate, and execute a script")
    run_parser.add_argument("script", help="Path to the script file")
    run_parser.add_argument("--out", required=True, help="Output directory for plan/report and outputs")
    run_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")
    run_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    run_parser.add_argument("--no-strict", dest="strict", action="store_false")

    validate_parser = subparsers.add_parser("validate", help="Validate tables against a profile")
    validate_parser.add_argument("--profile", required=True, help="Validation profile (e.g., sdtm)")
    validate_parser.add_argument("--out", required=True, help="Output directory for validation report")
    validate_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")

    args = parser.parse_args(argv)

    if args.command == "check":
        script_path = Path(args.script)
        out_dir = Path(args.out)
        tables = _parse_tables(args.tables)
        try:
            text = script_path.read_text(encoding="utf-8")
        except OSError as exc:
            return _write_failed_report(out_dir, str(exc))

        irdoc, report = emit_check_artifacts(
            text=text,
            file_name=str(script_path),
            tables=tables,
            out_dir=out_dir,
            strict=args.strict,
        )

        status = report.get("status")
        if status == "refused":
            primary = report.get("primary_error") or {}
            loc = primary.get("loc") or {}
            loc_str = f"{loc.get('file')}:{loc.get('line_start')}" if loc else ""
            print(f"refused: {primary.get('code')} at {loc_str}".rstrip())
        else:
            print("ok: wrote plan.ir.json report.json")
        return int(report.get("exit_code_bucket", 50))

    if args.command == "run":
        script_path = Path(args.script)
        out_dir = Path(args.out)
        bindings = {}
        if args.tables:
            for item in args.tables.split(","):
                if not item.strip():
                    continue
                if "=" not in item:
                    return _write_failed_report(out_dir, f"Invalid table binding '{item}'")
                name, path = item.split("=", 1)
                bindings[name.strip()] = path.strip()
        try:
            text = script_path.read_text(encoding="utf-8")
        except OSError as exc:
            return _write_failed_report(out_dir, str(exc))

        report = run_script(
            text=text,
            file_name=str(script_path),
            bindings=bindings,
            out_dir=out_dir,
            strict=args.strict,
        )

        status = report.get("status")
        if status == "refused":
            primary = report.get("primary_error") or {}
            loc = primary.get("loc") or {}
            loc_str = f"{loc.get('file')}:{loc.get('line_start')}" if loc else ""
            print(f"refused: {primary.get('code')} at {loc_str}".rstrip())
        elif status == "failed":
            primary = report.get("primary_error") or {}
            loc = primary.get("loc") or {}
            loc_str = f"{loc.get('file')}:{loc.get('line_start')}" if loc else ""
            print(f"failed: {primary.get('code')} at {loc_str}".rstrip())
        else:
            print("ok: wrote plan.ir.json report.json")
        return int(report.get("exit_code_bucket", 50))

    if args.command == "validate":
        out_dir = Path(args.out)
        if args.profile.lower() != "sdtm":
            return _write_failed_validation_report(out_dir, f"Unsupported profile '{args.profile}'")

        bindings = {}
        if args.tables:
            for item in args.tables.split(","):
                if not item.strip():
                    continue
                if "=" not in item:
                    return _write_failed_validation_report(out_dir, f"Invalid table binding '{item}'")
                name, path = item.split("=", 1)
                bindings[name.strip()] = path.strip()

        report = validate_sdtm(bindings, out_dir)
        if report.get("status") == "failed":
            print("failed: validation.report.json")
        else:
            print("ok: wrote validation.report.json")
        return int(report.get("exit_code_bucket", 50))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
