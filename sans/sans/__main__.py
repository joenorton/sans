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
    parser.add_argument("--version", action="version", version=f"%(prog)s {_engine_version}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="Compile and validate a script")
    check_parser.add_argument("script", help="Path to the script file")
    check_parser.add_argument("--out", required=True, help="Output directory for plan/report")
    check_parser.add_argument("--tables", default="", help="Comma-separated list of predeclared tables")
    check_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    check_parser.add_argument("--no-strict", dest="strict", action="store_false")
    check_parser.add_argument("--include-root", action="append", default=[], help="Additional include root (repeatable)")
    check_parser.add_argument("--allow-absolute-include", action="store_true", default=False)
    check_parser.add_argument("--allow-include-escape", action="store_true", default=False)

    run_parser = subparsers.add_parser("run", help="Compile, validate, and execute a script")
    run_parser.add_argument("script", help="Path to the script file")
    run_parser.add_argument("--out", required=True, help="Output directory for plan/report and outputs")
    run_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")
    run_parser.add_argument("--format", default="csv", choices=["csv", "xpt"], help="Output format (csv, xpt)")
    run_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    run_parser.add_argument("--no-strict", dest="strict", action="store_false")
    run_parser.add_argument("--include-root", action="append", default=[], help="Additional include root (repeatable)")
    run_parser.add_argument("--allow-absolute-include", action="store_true", default=False)
    run_parser.add_argument("--allow-include-escape", action="store_true", default=False)

    validate_parser = subparsers.add_parser("validate", help="Validate tables against a profile")
    validate_parser.add_argument("--profile", required=True, help="Validation profile (e.g., sdtm)")
    validate_parser.add_argument("--out", required=True, help="Output directory for validation report")
    validate_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")

    verify_parser = subparsers.add_parser("verify", help="Verify a repro bundle")
    verify_parser.add_argument("bundle", help="Path to report.json or bundle directory")

    args = parser.parse_args(argv)

    if args.command == "verify":
        from .hash_utils import compute_artifact_hash, _sha256_text
        bundle_path = Path(args.bundle)
        if bundle_path.is_dir():
            report_path = bundle_path / "report.json"
        else:
            report_path = bundle_path
            
        if not report_path.exists():
            print(f"failed: report not found at {report_path}")
            return 1
            
        try:
            report_text = report_path.read_text(encoding="utf-8")
            report = json.loads(report_text)
        except Exception as e:
            print(f"failed: invalid json in report: {e}")
            return 1

        # Check inputs
        for inp in report.get("inputs", []):
            path = Path(inp.get("path"))
            expected = inp.get("sha256")
            if not path.exists():
                print(f"failed: input file missing: {path}")
                return 1
            actual = compute_artifact_hash(path)
            if actual != expected:
                print(f"failed: input hash mismatch for {path}")
                return 1
                
        # Check outputs
        plan_verified = False
        reported_plan_path = report.get("plan_path")
        for out in report.get("outputs", []):
            path_str = out.get("path")
            path = Path(path_str)
            
            if reported_plan_path and path_str == reported_plan_path:
                if not path.exists():
                    print(f"failed: plan file missing at {path}")
                    return 1
                plan_verified = True
            expected = out.get("sha256")
            if expected is None:
                continue

            # Special handling for report.json self-verification
            # We assume report.path in json matches the file we are reading
            # Or we check if path matches report_path
            is_report = False
            try:
                if path.resolve() == report_path.resolve():
                    is_report = True
            except OSError:
                pass
            
            if is_report:
                # To verify report.json, we must revert the self-hash to None (or whatever it was before writing)
                # In compiler.py, it sets output[1]["sha256"] = hash. 
                # We need to find the entry for report.json in the loaded report and set it to None, then serialize.
                # But serialization must be exact (indent=2).
                
                # Deep copy report to modify
                report_copy = json.loads(report_text) 
                # Find the self entry
                found = False
                for item in report_copy.get("outputs", []):
                    if item.get("path") == path_str:
                        item["sha256"] = None
                        found = True
                        break
                
                if found:
                    # Re-serialize exactly as compiler.py does
                    # compiler.py uses json.dumps(report, indent=2)
                    # Python's json.dumps with indent=2 adds trailing spaces? No.
                    # separators? default is (', ', ': ')
                    expected_bytes = json.dumps(report_copy, indent=2).encode("utf-8")
                    actual_hash = _sha256_text(expected_bytes.decode("utf-8")) # hash_utils._sha256_text takes str
                    
                    # Wait, hash_utils._sha256_text encodes as utf-8 then hashes.
                    # So passing the string is correct.
                    
                    if actual_hash != expected:
                        print(f"failed: report hash mismatch")
                        return 1
                continue

            if not path.exists():
                print(f"failed: output file missing: {path}")
                return 1
            actual = compute_artifact_hash(path)
            if actual != expected:
                print(f"failed: output hash mismatch for {path}")
                return 1
                
        print("ok: verified")
        return 0

    if args.command == "check":
        script_path = Path(args.script)
        out_dir = Path(args.out)
        tables = _parse_tables(args.tables)
        include_roots = [Path(p) for p in args.include_root] if args.include_root else None
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
            include_roots=include_roots,
            allow_absolute_includes=args.allow_absolute_include,
            allow_include_escape=args.allow_include_escape,
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
                name = name.strip()
                if name in bindings:
                    return _write_failed_report(out_dir, f"Duplicate table binding for '{name}'")
                bindings[name] = path.strip()
        try:
            text = script_path.read_text(encoding="utf-8")
        except OSError as exc:
            return _write_failed_report(out_dir, str(exc))

        include_roots = [Path(p) for p in args.include_root] if args.include_root else None
        report = run_script(
            text=text,
            file_name=str(script_path),
            bindings=bindings,
            out_dir=out_dir,
            strict=args.strict,
            output_format=args.format,
            include_roots=include_roots,
            allow_absolute_includes=args.allow_absolute_include,
            allow_include_escape=args.allow_include_escape,
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
                name = name.strip()
                if name in bindings:
                    return _write_failed_validation_report(out_dir, f"Duplicate table binding for '{name}'")
                bindings[name] = path.strip()

        report = validate_sdtm(bindings, out_dir)
        if report.get("status") == "failed":
            print("failed: validation.report.json")
        else:
            print("ok: wrote validation.report.json")
        return int(report.get("exit_code_bucket", 50))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
