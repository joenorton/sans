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
    from .bundle import ensure_bundle_layout, bundle_relative_path, ARTIFACTS
    from .hash_utils import compute_artifact_hash, compute_report_sha256
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ensure_bundle_layout(out_dir)
    plan_path = out_dir / ARTIFACTS / "plan.ir.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps({"steps": [], "tables": [], "table_facts": {}}, indent=2), encoding="utf-8")
    report_path = out_dir / "report.json"
    plan_rel = bundle_relative_path(plan_path, out_dir)
    report = {
        "report_schema_version": "0.2",
        "status": "failed",
        "exit_code_bucket": 50,
        "primary_error": {"code": "SANS_IO_ERROR", "message": message, "loc": None},
        "diagnostics": [],
        "inputs": [],
        "artifacts": [
            {"name": "plan.ir.json", "path": plan_rel, "sha256": compute_artifact_hash(plan_path) or ""},
        ],
        "outputs": [],
        "plan_path": plan_rel,
        "engine": {"name": "sans", "version": _engine_version},
        "settings": {"strict": True, "allow_approx": False, "tolerance": None, "tables": []},
        "timing": {"compile_ms": None, "validate_ms": None, "execute_ms": None},
    }
    report["report_sha256"] = compute_report_sha256(report, out_dir)
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
        from .hash_utils import compute_artifact_hash, compute_report_sha256
        bundle_path = Path(args.bundle)
        if bundle_path.is_dir():
            report_path = bundle_path / "report.json"
            bundle_root = bundle_path.resolve()
        else:
            report_path = bundle_path
            bundle_root = report_path.parent.resolve()

        if not report_path.exists():
            print(f"failed: report not found at {report_path}")
            return 1

        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"failed: invalid json in report: {e}")
            return 1

        # Canonical report self-hash check
        expected_sha = report.get("report_sha256")
        if expected_sha is not None:
            actual_sha = compute_report_sha256(report, bundle_root)
            if actual_sha != expected_sha:
                print("failed: report hash mismatch")
                return 1

        # Check inputs (bundle-relative paths only)
        for inp in report.get("inputs", []):
            path_str = inp.get("path") or ""
            if Path(path_str).is_absolute():
                print(f"failed: input path must be bundle-relative: {path_str}")
                return 1
            path = bundle_root / path_str
            expected = inp.get("sha256")
            if not path.exists():
                print(f"failed: input file missing: {path}")
                return 1
            if expected:
                actual = compute_artifact_hash(path)
                if actual != expected:
                    print(f"failed: input hash mismatch for {path}")
                    return 1

        # Check artifacts
        for art in report.get("artifacts", []):
            path_str = art.get("path") or ""
            if Path(path_str).is_absolute():
                print(f"failed: artifact path must be bundle-relative: {path_str}")
                return 1
            path = bundle_root / path_str
            expected = art.get("sha256")
            if not path.exists():
                print(f"failed: artifact file missing: {path}")
                return 1
            if expected:
                actual = compute_artifact_hash(path)
                if actual != expected:
                    print(f"failed: artifact hash mismatch for {path}")
                    return 1

        # Check outputs (report.json is not listed in any array)
        for out in report.get("outputs", []):
            path_str = out.get("path") or ""
            if Path(path_str).is_absolute():
                print(f"failed: output path must be bundle-relative: {path_str}")
                return 1
            path = bundle_root / path_str
            expected = out.get("sha256")
            if not expected:
                print(f"failed: output entry missing sha256: {path_str}")
                return 1
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
            print("ok: wrote plan.ir.json report.json registry.candidate.json runtime.evidence.json")
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
            print("ok: wrote plan.ir.json report.json registry.candidate.json runtime.evidence.json")
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
