from __future__ import annotations

import argparse
import sys
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .compiler import emit_check_artifacts
from .runtime import run_script, RuntimeFailure
from .validator_sdtm import validate_sdtm
from .fmt import FMT_STYLE_ID, format_text, normalize_newlines
from . import __version__ as _engine_version
from .ir.adapter import sans_ir_to_irdoc
from .ir.normalize import irdoc_to_sans_ir
from .ir.schema import validate_sans_ir, canonical_json_dumps
from .sans_script import irdoc_to_expanded_sans


class TableFlagError(ValueError):
    pass


def _parse_table_names(tables_arg: str | None) -> set[str] | None:
    if not tables_arg:
        return None
    tables = [t.strip() for t in tables_arg.split(",") if t.strip()]
    return set(tables) if tables else None


def _parse_table_bindings(tables_arg: str | None) -> dict[str, str]:
    bindings: dict[str, str] = {}
    if not tables_arg:
        return bindings
    for item in tables_arg.split(","):
        if not item.strip():
            continue
        if "=" not in item:
            raise TableFlagError(f"Invalid table binding '{item}'")
        name, path = item.split("=", 1)
        name = name.strip()
        if name in bindings:
            raise TableFlagError(f"Duplicate table binding for '{name}'")
        bindings[name] = path.strip()
    return bindings


def _scan_inputs_dir_csv_bindings(inputs_dir_arg: str) -> list[tuple[str, str]]:
    inputs_dir = Path(inputs_dir_arg).expanduser().resolve()
    if not inputs_dir.exists():
        raise TableFlagError(f"Inputs directory not found: {inputs_dir}")
    if not inputs_dir.is_dir():
        raise TableFlagError(f"Inputs directory is not a directory: {inputs_dir}")

    bindings: list[tuple[str, str]] = []
    seen_names: dict[str, str] = {}
    for entry in sorted(inputs_dir.iterdir(), key=lambda p: p.name):
        if entry.is_symlink():
            raise TableFlagError(f"symlink not allowed in inputs-dir: {entry.name}")
        if not entry.is_file():
            continue
        if entry.suffix.lower() != ".csv":
            continue
        table_name = entry.stem
        key = table_name.casefold()
        if key in seen_names:
            raise TableFlagError(f"duplicate table name from inputs-dir: {key}")
        seen_names[key] = entry.name
        bindings.append((table_name, str(entry.resolve())))
    if not bindings:
        raise TableFlagError(f"inputs-dir has no .csv files: {inputs_dir}")
    return bindings


def resolve_tables_from_flags(args: argparse.Namespace, mode: str) -> set[str] | dict[str, str] | None:
    tables_arg = getattr(args, "tables", "")
    inputs_dir_arg = getattr(args, "inputs_dir", None)

    if tables_arg and inputs_dir_arg:
        raise TableFlagError("choose one: --tables or --inputs-dir")

    if mode == "names":
        if inputs_dir_arg:
            scanned = _scan_inputs_dir_csv_bindings(str(inputs_dir_arg))
            return {name for name, _ in scanned}
        return _parse_table_names(tables_arg)

    if mode == "bindings":
        if inputs_dir_arg:
            scanned = _scan_inputs_dir_csv_bindings(str(inputs_dir_arg))
            return {name: path for name, path in scanned}
        return _parse_table_bindings(tables_arg)

    raise ValueError(f"unknown table resolution mode: {mode}")


def _resolve_verify_datasource_expectations(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for entry in report.get("datasource_inputs") or []:
        name = entry.get("datasource")
        if isinstance(name, str) and name:
            expected[name] = dict(entry)
    if expected:
        return expected

    # Legacy fallback: use role=datasource entries from report.inputs.
    for entry in report.get("inputs") or []:
        if entry.get("role") != "datasource":
            continue
        raw_name = entry.get("name")
        if not isinstance(raw_name, str) or not raw_name:
            continue
        name = Path(raw_name).stem
        expected[name] = {
            "datasource": name,
            "sha256": entry.get("sha256"),
        }
    return expected


@contextmanager
def _temporary_cwd(new_cwd: Path | None):
    if new_cwd is None:
        yield
        return
    previous = Path.cwd()
    os.chdir(new_cwd)
    try:
        yield
    finally:
        os.chdir(previous)


def _write_failed_report(out_dir: Path, message: str) -> int:
    from .bundle import ensure_bundle_layout, bundle_relative_path, ARTIFACTS
    from .hash_utils import compute_artifact_hash, compute_report_sha256
    from .graph import write_graph_json
    from .lineage import write_vars_graph_json, write_table_effects_json
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ensure_bundle_layout(out_dir)
    plan_path = out_dir / ARTIFACTS / "plan.ir.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps({"steps": [], "tables": [], "table_facts": {}}, indent=2), encoding="utf-8")
    graph_path = out_dir / ARTIFACTS / "graph.json"
    write_graph_json(
        {"schema_version": 1, "producer": {"name": "sans", "version": _engine_version}, "nodes": [], "edges": []},
        graph_path,
    )
    vars_graph_path = out_dir / ARTIFACTS / "vars.graph.json"
    write_vars_graph_json({"nodes": [], "edges": []}, vars_graph_path)
    effects_path = out_dir / ARTIFACTS / "table.effects.json"
    write_table_effects_json({"effects": []}, effects_path)
    report_path = out_dir / "report.json"
    plan_rel = bundle_relative_path(plan_path, out_dir)
    graph_rel = bundle_relative_path(graph_path, out_dir)
    vars_graph_rel = bundle_relative_path(vars_graph_path, out_dir)
    effects_rel = bundle_relative_path(effects_path, out_dir)
    report = {
        "report_schema_version": "0.3",
        "status": "failed",
        "exit_code_bucket": 50,
        "primary_error": {"code": "SANS_IO_ERROR", "message": message, "loc": None},
        "diagnostics": [],
        "inputs": [],
        "artifacts": [
            {"name": "plan.ir.json", "path": plan_rel, "sha256": compute_artifact_hash(plan_path) or ""},
            {"name": "graph.json", "path": graph_rel, "sha256": compute_artifact_hash(graph_path) or ""},
            {"name": "vars.graph.json", "path": vars_graph_rel, "sha256": compute_artifact_hash(vars_graph_path) or ""},
            {"name": "table.effects.json", "path": effects_rel, "sha256": compute_artifact_hash(effects_path) or ""},
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
    check_parser.add_argument(
        "--inputs-dir",
        "--inputs",
        dest="inputs_dir",
        default=None,
        metavar="dir",
        help="Directory of .csv files (non-recursive). Expands to predeclared table names by filename stem.",
    )
    check_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    check_parser.add_argument("--no-strict", dest="strict", action="store_false")
    check_parser.add_argument("--include-root", action="append", default=[], help="Additional include root (repeatable)")
    check_parser.add_argument("--allow-absolute-include", action="store_true", default=False)
    check_parser.add_argument("--allow-include-escape", action="store_true", default=False)
    check_parser.add_argument("--legacy-sas", action="store_true", default=False, help="Enable legacy SAS expression operators for .sas")

    run_parser = subparsers.add_parser("run", help="Compile, validate, and execute a script")
    run_parser.add_argument("script", help="Path to the script file")
    run_parser.add_argument("--out", required=True, help="Output directory for plan/report and outputs")
    run_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")
    run_parser.add_argument(
        "--inputs-dir",
        "--inputs",
        dest="inputs_dir",
        default=None,
        metavar="dir",
        help="Directory of .csv files (non-recursive). Expands to --tables <stem>=<path>.",
    )
    run_parser.add_argument("--format", default="csv", choices=["csv", "xpt"], help="Output format (csv, xpt)")
    run_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    run_parser.add_argument("--no-strict", dest="strict", action="store_false")
    run_parser.add_argument("--include-root", action="append", default=[], help="Additional include root (repeatable)")
    run_parser.add_argument("--allow-absolute-include", action="store_true", default=False)
    run_parser.add_argument("--allow-include-escape", action="store_true", default=False)
    run_parser.add_argument("--legacy-sas", action="store_true", default=False, help="Enable legacy SAS expression operators for .sas")
    run_parser.add_argument("--schema-lock", metavar="path", default=None, help="Path to schema.lock.json to enforce when ingesting datasources")
    run_parser.add_argument("--emit-schema-lock", metavar="path", default=None, help="After successful run, write schema.lock.json to this path")
    run_parser.add_argument("--lock-only", action="store_true", help="With --emit-schema-lock: generate lock only, do not execute (even if all datasources are typed)")
    run_parser.add_argument("--bundle-mode", choices=["full", "thin"], default="full", help="Bundle mode: full (embed datasource bytes) or thin (fingerprints only)")

    run_ir_parser = subparsers.add_parser("run-ir", help="Execute a canonical sans.ir file")
    run_ir_parser.add_argument("script", help="Path to the sans.ir file")
    run_ir_parser.add_argument("--out", required=True, help="Output directory for plan/report and outputs")
    run_ir_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")
    run_ir_parser.add_argument("--format", default="csv", choices=["csv", "xpt"], help="Output format (csv, xpt)")
    run_ir_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    run_ir_parser.add_argument("--no-strict", dest="strict", action="store_false")
    run_ir_parser.add_argument("--schema-lock", metavar="path", default=None, help="Path to schema.lock.json to enforce when ingesting datasources")
    run_ir_parser.add_argument("--emit-schema-lock", metavar="path", default=None, help="After successful run, write schema.lock.json to this path")
    run_ir_parser.add_argument("--lock-only", action="store_true", help="With --emit-schema-lock: generate lock only, do not execute (even if all datasources are typed)")
    run_ir_parser.add_argument("--bundle-mode", choices=["full", "thin"], default="full", help="Bundle mode: full (embed datasource bytes) or thin (fingerprints only)")

    emit_ir_parser = subparsers.add_parser("emit-ir", help="Compile/check and emit canonical sans.ir")
    emit_ir_parser.add_argument("script", help="Path to the script file")
    emit_ir_parser.add_argument("--out", required=True, help="Path to write canonical sans.ir")
    emit_ir_parser.add_argument("--cwd", default=None, help="Working directory for compile/check path resolution")
    emit_ir_parser.add_argument("--strict", dest="strict", action="store_true", default=True)
    emit_ir_parser.add_argument("--no-strict", dest="strict", action="store_false")
    emit_ir_parser.add_argument("--json", action="store_true", default=False, help="Print machine-readable result JSON")

    ir_validate_parser = subparsers.add_parser("ir-validate", help="Validate sans.ir structure only")
    ir_validate_parser.add_argument("script", help="Path to the sans.ir file")
    ir_validate_parser.add_argument("--strict", action="store_true", default=False, help="Treat structural warnings as errors")

    schema_lock_parser = subparsers.add_parser("schema-lock", help="Generate schema.lock.json without execution (no --out required)")
    schema_lock_parser.add_argument("script", help="Path to the script file")
    schema_lock_parser.add_argument("--write", "-o", dest="write", default=None, metavar="path", help="Lock output path (default: <script_dir>/<script_stem>.schema.lock.json); relative paths resolved against script dir")
    schema_lock_parser.add_argument("--out", default=None, metavar="dir", help="Optional: also write report.json and stage inputs under this directory")
    schema_lock_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")
    schema_lock_parser.add_argument(
        "--inputs-dir",
        "--inputs",
        dest="inputs_dir",
        default=None,
        metavar="dir",
        help="Directory of .csv files (non-recursive). Expands to --tables <stem>=<path>.",
    )
    schema_lock_parser.add_argument("--include-root", action="append", default=[], help="Additional include root (repeatable)")
    schema_lock_parser.add_argument("--allow-absolute-include", action="store_true", default=False)
    schema_lock_parser.add_argument("--allow-include-escape", action="store_true", default=False)
    schema_lock_parser.add_argument("--schema-lock", metavar="path", default=None, help="Existing lock to merge or supply types for untyped refs")
    schema_lock_parser.add_argument("--legacy-sas", action="store_true", default=False, help="Accept legacy SAS syntax for lock generation")

    validate_parser = subparsers.add_parser("validate", help="Validate tables against a profile")
    validate_parser.add_argument("--profile", required=True, help="Validation profile (e.g., sdtm)")
    validate_parser.add_argument("--out", required=True, help="Output directory for validation report")
    validate_parser.add_argument("--tables", default="", help="Comma-separated table bindings name=path.csv")
    validate_parser.add_argument(
        "--inputs-dir",
        "--inputs",
        dest="inputs_dir",
        default=None,
        metavar="dir",
        help="Directory of .csv files (non-recursive). Expands to --tables <stem>=<path>.",
    )

    verify_parser = subparsers.add_parser("verify", help="Verify a repro bundle")
    verify_parser.add_argument("bundle", help="Path to report.json or bundle directory")
    verify_parser.add_argument("--schema-lock", metavar="path", default=None, help="Path to schema.lock.json; verify its hash matches report.schema_lock_sha256")
    verify_parser.add_argument("--tables", default="", help="Comma-separated datasource bindings name=path.csv for external input verification")
    verify_parser.add_argument(
        "--inputs-dir",
        "--inputs",
        dest="inputs_dir",
        default=None,
        metavar="dir",
        help="Directory of .csv files (non-recursive). Expands to --tables <stem>=<path>.",
    )

    fmt_parser = subparsers.add_parser("fmt", help="Format a .sans script")
    fmt_parser.add_argument("script", help="Path to the script file or directory")
    fmt_parser.add_argument("--mode", default="canonical", choices=["canonical", "identity"], help="Formatting mode")
    fmt_parser.add_argument("--style", default=FMT_STYLE_ID, help="Formatting style version")
    fmt_parser.add_argument("--check", action="store_true", help="Check if formatting is needed")
    fmt_parser.add_argument("--in-place", action="store_true", dest="in_place", help="Rewrite the file in place")

    args = parser.parse_args(argv)

    if args.command == "verify":
        from .hash_utils import compute_artifact_hash, compute_input_hash, compute_report_sha256
        from .path_utils import fs_path_from_report
        try:
            bindings = resolve_tables_from_flags(args, mode="bindings")
        except TableFlagError as exc:
            print(f"failed: {exc}")
            return 1
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

        if getattr(args, "schema_lock", None):
            from .schema_lock import load_schema_lock, compute_lock_sha256
            lock_path = Path(args.schema_lock)
            if not lock_path.exists():
                print(f"failed: schema lock file not found: {lock_path}")
                return 1
            lock_dict = load_schema_lock(lock_path)
            lock_sha = compute_lock_sha256(lock_dict)
            report_lock_sha = report.get("schema_lock_sha256")
            if report_lock_sha is None:
                print("failed: report has no schema_lock_sha256 (run did not use or emit a schema lock)")
                return 1
            if lock_sha != report_lock_sha:
                print("failed: schema lock hash mismatch")
                return 1

        # Check inputs (mode-aware: thin mode does not require datasource files in bundle)
        bundle_mode = report.get("bundle_mode")
        is_thin = bundle_mode == "thin"
        for inp in report.get("inputs", []):
            if inp.get("role") == "datasource":
                # Datasource inventory is in datasource_inputs; skip here (checked below)
                continue
            # Non-datasource: require bundle-relative path and file
            path_str = inp.get("path") or ""
            if not path_str:
                print("failed: input entry missing path (full bundle)")
                return 1
            rel_path = fs_path_from_report(path_str)
            if rel_path.is_absolute():
                print(f"failed: input path must be bundle-relative: {path_str}")
                return 1
            path = bundle_root / rel_path
            expected = inp.get("sha256")
            if not path.exists():
                print(f"failed: input file missing (full bundle): {path}")
                return 1
            if expected:
                actual = compute_input_hash(path)
                if actual != expected:
                    print(f"failed: input hash mismatch for {path}")
                    return 1

        # Check datasource_inputs (required for v2 bundles; legacy may have only inputs with role=datasource)
        datasource_inputs = report.get("datasource_inputs") or []
        settings_datasources = report.get("settings") or {}
        settings_ds_list = settings_datasources.get("datasources") or []
        if bindings:
            expected_ds = _resolve_verify_datasource_expectations(report)
            for name, path_str in sorted(bindings.items()):
                if name not in expected_ds:
                    # Ignore extra external files; only verify files that correspond to report datasources.
                    continue
                path = Path(path_str)
                if not path.exists():
                    print(f"failed: input table '{name}' file not found: {path}")
                    return 1
                expected = expected_ds[name]
                expected_sha = expected.get("sha256")
                if expected_sha:
                    actual_sha = compute_input_hash(path)
                    if actual_sha != expected_sha:
                        print(f"failed: datasource hash mismatch for binding '{name}'")
                        return 1
                expected_size = expected.get("size_bytes")
                if expected_size is not None:
                    actual_size = path.stat().st_size
                    if int(actual_size) != int(expected_size):
                        print(f"failed: datasource size mismatch for binding '{name}'")
                        return 1

        if is_thin:
            if not datasource_inputs:
                print("failed: thin bundle must have datasource_inputs array")
                return 1
            for ds_name in settings_ds_list:
                found = next((d for d in datasource_inputs if d.get("datasource") == ds_name), None)
                if not found:
                    print(f"failed: thin bundle missing datasource_inputs entry for: {ds_name}")
                    return 1
            for d in datasource_inputs:
                if d.get("embedded") is not False:
                    print(f"failed: thin bundle datasource_inputs entry must have embedded=false: {d.get('datasource')}")
                    return 1
                if not d.get("sha256"):
                    print("failed: thin bundle datasource entry missing sha256")
                    return 1
                if d.get("size_bytes") is None:
                    print("failed: thin bundle datasource entry missing size_bytes")
                    return 1
            data_dir = bundle_root / "inputs" / "data"
            if data_dir.exists():
                files_in_data = [f for f in data_dir.iterdir() if f.is_file()]
                if files_in_data:
                    print("failed: thin bundle must not contain files in inputs/data/")
                    return 1
        else:
            for d in datasource_inputs:
                if d.get("embedded") is True:
                    path_str = d.get("path") or ""
                    if not path_str:
                        print("failed: full bundle datasource_inputs entry missing path")
                        return 1
                    rel_path = fs_path_from_report(path_str)
                    path = bundle_root / rel_path
                    if not path.exists():
                        print(f"failed: datasource file missing: {path_str}")
                        return 1
                    expected = d.get("sha256")
                    if expected:
                        actual = compute_input_hash(path)
                        if actual != expected:
                            print(f"failed: datasource hash mismatch for {path_str}")
                            return 1
        # Legacy: inputs with role=datasource (no datasource_inputs)
        for inp in report.get("inputs", []):
            if inp.get("role") != "datasource":
                continue
            path_str = inp.get("path") or ""
            if not path_str:
                print("failed: input entry missing path (full bundle)")
                return 1
            rel_path = fs_path_from_report(path_str)
            path = bundle_root / rel_path
            if not path.exists():
                print(f"failed: input file missing: {path}")
                return 1
            expected = inp.get("sha256")
            if expected:
                actual = compute_input_hash(path)
                if actual != expected:
                    print(f"failed: input hash mismatch for {path}")
                    return 1

        # Check artifacts
        for art in report.get("artifacts", []):
            path_str = art.get("path") or ""
            rel_path = fs_path_from_report(path_str)
            if rel_path.is_absolute():
                print(f"failed: artifact path must be bundle-relative: {path_str}")
                return 1
            path = bundle_root / rel_path
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
            rel_path = fs_path_from_report(path_str)
            if rel_path.is_absolute():
                print(f"failed: output path must be bundle-relative: {path_str}")
                return 1
            path = bundle_root / rel_path
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
        try:
            tables = resolve_tables_from_flags(args, mode="names")
        except TableFlagError as exc:
            return _write_failed_report(out_dir, str(exc))
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
            legacy_sas=args.legacy_sas,
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
        try:
            bindings = resolve_tables_from_flags(args, mode="bindings")
        except TableFlagError as exc:
            return _write_failed_report(out_dir, str(exc))
        if bindings is None:
            bindings = {}
        try:
            text = script_path.read_text(encoding="utf-8")
        except OSError as exc:
            return _write_failed_report(out_dir, str(exc))

        include_roots = [Path(p) for p in args.include_root] if args.include_root else None
        schema_lock_path = Path(args.schema_lock) if args.schema_lock else None
        emit_schema_lock_path = Path(args.emit_schema_lock) if args.emit_schema_lock else None
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
            legacy_sas=args.legacy_sas,
            schema_lock_path=schema_lock_path,
            emit_schema_lock_path=emit_schema_lock_path,
            lock_only=getattr(args, "lock_only", False),
            bundle_mode=getattr(args, "bundle_mode", "full"),
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
            emit_path = report.get("schema_lock_emit_path")
            if emit_path:
                mode = report.get("schema_lock_mode", "")
                suffix = " (lock-only)" if mode == "generated_only" else " (after run)"
                print(f"ok: wrote schema lock to {emit_path}{suffix}")
        return int(report.get("exit_code_bucket", 50))

    if args.command == "run-ir":
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
            sans_ir = json.loads(script_path.read_text(encoding="utf-8"))
            # Execution gate: strict by default for optimizer/runtime safety.
            validate_sans_ir(sans_ir, strict=bool(getattr(args, "strict", True)))
            ir_doc = sans_ir_to_irdoc(sans_ir, file_name=str(script_path))
        except OSError as exc:
            return _write_failed_report(out_dir, str(exc))
        except Exception as exc:
            return _write_failed_report(out_dir, f"Invalid sans.ir: {exc}")

        # Reuse existing run path and runtime core by rendering canonical expanded.sans
        # from IR, then executing as a normal .sans script.
        expanded = irdoc_to_expanded_sans(ir_doc)
        virtual_script_path = script_path.with_suffix(".expanded.sans")
        schema_lock_path = Path(args.schema_lock) if args.schema_lock else None
        emit_schema_lock_path = Path(args.emit_schema_lock) if args.emit_schema_lock else None
        report = run_script(
            text=expanded,
            file_name=str(virtual_script_path),
            bindings=bindings,
            out_dir=out_dir,
            strict=args.strict,
            output_format=args.format,
            include_roots=None,
            allow_absolute_includes=False,
            allow_include_escape=False,
            legacy_sas=False,
            schema_lock_path=schema_lock_path,
            emit_schema_lock_path=emit_schema_lock_path,
            lock_only=getattr(args, "lock_only", False),
            bundle_mode=getattr(args, "bundle_mode", "full"),
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

    if args.command == "emit-ir":
        invoked_cwd = Path.cwd()
        compile_cwd = Path(args.cwd).resolve() if args.cwd else None
        if compile_cwd is not None and not compile_cwd.is_dir():
            message = f"Working directory not found: {compile_cwd}"
            if args.json:
                print(json.dumps({"ok": False, "error": message}, separators=(",", ":"), ensure_ascii=True))
            else:
                print(f"failed: {message}")
            return 1

        script_arg = Path(args.script)
        if script_arg.is_absolute():
            script_path = script_arg
        else:
            script_path = (invoked_cwd / script_arg)
            if not script_path.exists() and compile_cwd is not None:
                script_path = compile_cwd / script_arg
        script_path = script_path.resolve()

        out_arg = Path(args.out)
        out_path = out_arg if out_arg.is_absolute() else (invoked_cwd / out_arg).resolve()

        with _temporary_cwd(compile_cwd):
            try:
                text = script_path.read_text(encoding="utf-8")
            except OSError as exc:
                if args.json:
                    print(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":"), ensure_ascii=True))
                else:
                    print(f"failed: {exc}")
                return 1

            with tempfile.TemporaryDirectory(prefix="sans-emit-ir-") as temp_out:
                irdoc, report = emit_check_artifacts(
                    text=text,
                    file_name=str(script_path),
                    tables=None,
                    out_dir=temp_out,
                    strict=bool(getattr(args, "strict", True)),
                    include_roots=None,
                    allow_absolute_includes=False,
                    allow_include_escape=False,
                    legacy_sas=False,
                )

            status = report.get("status")
            if status != "ok":
                primary = report.get("primary_error") or {}
                code = primary.get("code")
                message = primary.get("message") or "compile/check failed"
                if code and code not in message:
                    message = f"{code}: {message}"
                if args.json:
                    print(json.dumps({"ok": False, "error": message}, separators=(",", ":"), ensure_ascii=True))
                else:
                    print(f"failed: {message}")
                return int(report.get("exit_code_bucket", 50)) or 1

            try:
                emitted = irdoc_to_sans_ir(irdoc)
                validate_sans_ir(emitted, strict=bool(getattr(args, "strict", True)))
            except Exception as exc:
                message = f"Invalid sans.ir: {exc}"
                if args.json:
                    print(json.dumps({"ok": False, "error": message}, separators=(",", ":"), ensure_ascii=True))
                else:
                    print(f"failed: {message}")
                return 2

            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(canonical_json_dumps(emitted), encoding="utf-8")
            if args.json:
                print(
                    json.dumps(
                        {"ok": True, "out_path": str(out_path), "warnings": []},
                        separators=(",", ":"),
                        ensure_ascii=True,
                    )
                )
            else:
                print(f"ok: wrote {out_path}")
            return 0

    if args.command == "ir-validate":
        script_path = Path(args.script)
        try:
            sans_ir = json.loads(script_path.read_text(encoding="utf-8"))
            warnings = validate_sans_ir(sans_ir, strict=bool(getattr(args, "strict", False)))
        except OSError as exc:
            print(f"invalid: {exc}", file=sys.stderr)
            return 2
        except Exception as exc:
            print(f"invalid: {exc}", file=sys.stderr)
            return 2
        if warnings:
            print(f"ok: valid sans.ir ({len(warnings)} warning(s))", file=sys.stderr)
        else:
            print("ok: valid sans.ir", file=sys.stderr)
        return 0

    if args.command == "schema-lock":
        from .runtime import generate_schema_lock_standalone
        script_path = Path(args.script)
        if not script_path.exists():
            print(f"failed: script not found: {script_path}")
            return 50
        script_dir = script_path.resolve().parent
        script_stem = script_path.stem
        if args.write:
            write_arg = Path(args.write)
            write_path = (script_dir / write_arg).resolve() if not write_arg.is_absolute() else write_arg.resolve()
        else:
            write_path = (script_dir / f"{script_stem}.schema.lock.json").resolve()
        try:
            bindings = resolve_tables_from_flags(args, mode="bindings")
        except TableFlagError as exc:
            print(f"failed: {exc}")
            return 50
        if bindings is None:
            bindings = {}
        try:
            text = script_path.read_text(encoding="utf-8")
        except OSError as exc:
            print(f"failed: {exc}")
            return 50
        include_roots = [Path(p) for p in args.include_root] if args.include_root else None
        out_dir = Path(args.out).resolve() if args.out else None
        schema_lock_path = Path(args.schema_lock) if args.schema_lock else None
        try:
            report = generate_schema_lock_standalone(
                text=text,
                file_name=str(script_path),
                write_path=write_path,
                out_dir=out_dir,
                bindings=bindings or None,
                schema_lock_path=schema_lock_path,
                include_roots=include_roots,
                allow_absolute_includes=args.allow_absolute_include,
                allow_include_escape=args.allow_include_escape,
                strict=True,
                legacy_sas=args.legacy_sas,
            )
        except RuntimeFailure as e:
            print(f"failed: {e.code} {e.message}")
            return 50
        status = report.get("status")
        if status == "refused":
            primary = report.get("primary_error") or {}
            loc = primary.get("loc") or {}
            loc_str = f"{loc.get('file')}:{loc.get('line_start')}" if loc else ""
            print(f"refused: {primary.get('code')} at {loc_str}".rstrip())
        else:
            print(f"ok: wrote schema lock to {write_path}")
            if out_dir:
                print("ok: wrote report.json and staged inputs to", out_dir)
        return int(report.get("exit_code_bucket", 0 if status == "ok" else 50))

    if args.command == "validate":
        out_dir = Path(args.out)
        if args.profile.lower() != "sdtm":
            return _write_failed_validation_report(out_dir, f"Unsupported profile '{args.profile}'")

        try:
            bindings = resolve_tables_from_flags(args, mode="bindings")
        except TableFlagError as exc:
            return _write_failed_validation_report(out_dir, str(exc))
        if bindings is None:
            bindings = {}

        report = validate_sdtm(bindings, out_dir)
        if report.get("status") == "failed":
            print("failed: validation.report.json")
        else:
            print("ok: wrote validation.report.json")
        return int(report.get("exit_code_bucket", 50))

    if args.command == "fmt":
        script_path = Path(args.script)
        if args.check and args.in_place:
            print("failed: --check and --in-place are mutually exclusive")
            return 1
        if args.style != FMT_STYLE_ID:
            print(f"failed: unsupported style '{args.style}' (expected '{FMT_STYLE_ID}')")
            return 1

        paths: list[Path]
        if script_path.is_dir():
            paths = sorted(script_path.rglob("*.sans"))
            if not paths:
                if args.check:
                    print("ok: no .sans files found")
                    return 0
                if args.in_place:
                    print("ok: no .sans files found")
                    return 0
                print("failed: no .sans files found (use --check or --in-place for directories)")
                return 1
            if not (args.check or args.in_place):
                print("failed: directory formatting requires --check or --in-place")
                return 1
        else:
            paths = [script_path]

        any_changed = False
        for path in paths:
            try:
                text = path.read_text(encoding="utf-8")
            except OSError as exc:
                print(f"failed: {exc}")
                return 1
            try:
                formatted = format_text(text, mode=args.mode, style=args.style, file_name=str(path))
            except Exception as exc:
                if hasattr(exc, "code") and hasattr(exc, "line"):
                    print(f"failed: {exc.code} at {path}:{exc.line}")
                else:
                    print(f"failed: {exc}")
                return 1

            if args.check:
                if formatted != normalize_newlines(text):
                    print(f"needs format: {path}")
                    any_changed = True
                continue

            if args.in_place:
                if formatted != normalize_newlines(text):
                    tmp_path = path.with_suffix(path.suffix + ".tmp")
                    tmp_path.write_text(formatted, encoding="utf-8")
                    tmp_path.replace(path)
                    print(f"ok: wrote {path}")
                continue

            print(formatted, end="")
            return 0

        if args.check:
            return 1 if any_changed else 0
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
