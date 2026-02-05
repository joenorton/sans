import copy
import json
import shutil
import sys
from pathlib import Path
import pytest
from sans.__main__ import main

def test_hello_verify(tmp_path):
    # Setup
    script_content = """
    data out;
      set input;
      z = x + y;
    run;
    """
    input_csv = "x,y\n1,2\n3,4\n"
    
    script_path = tmp_path / "script.sas"
    script_path.write_text(script_content, encoding="utf-8")
    
    input_path = tmp_path / "input.csv"
    input_path.write_text(input_csv, encoding="utf-8")
    
    out_dir = tmp_path / "out"
    
    # Run
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"input={input_path}", "--legacy-sas"])
    assert ret == 0
    
    report_path = out_dir / "report.json"
    assert report_path.exists()
    
    # Verify Success
    ret = main(["verify", str(report_path)])
    assert ret == 0
    
    # Verify with directory
    ret = main(["verify", str(out_dir)])
    assert ret == 0
    
    # Tamper Output (outputs now under outputs/)
    out_csv = out_dir / "outputs" / "out.csv"
    original_out = out_csv.read_text(encoding="utf-8")
    out_csv.write_text("tampered", encoding="utf-8")
    
    ret = main(["verify", str(report_path)])
    assert ret == 1
    
    # Restore Output
    out_csv.write_text(original_out, encoding="utf-8")
    ret = main(["verify", str(report_path)])
    assert ret == 0
    
    # Tamper Input (materialized copy is under inputs/data/)
    materialized_input = out_dir / "inputs" / "data" / "input.csv"
    original_in = materialized_input.read_text(encoding="utf-8")
    materialized_input.write_text("tampered", encoding="utf-8")
    
    ret = main(["verify", str(report_path)])
    assert ret == 1
    
    # Restore Input
    materialized_input.write_text(original_in, encoding="utf-8")
    ret = main(["verify", str(report_path)])
    assert ret == 0
    
    # Tamper Report (Self-Check): modify an output hash to mismatch
    original_report = report_path.read_text(encoding="utf-8")
    report_data = json.loads(original_report)
    # outputs[] is the canonical list; tamper first output's sha256
    assert len(report_data.get("outputs", [])) > 0
    report_data["outputs"][0]["sha256"] = "deadbeef"
    report_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")
    
    ret = main(["verify", str(report_path)])
    assert ret == 1  # Should fail because out.csv hash mismatch
    
    # Tamper Report Self-Hash
    # To test self-hash check, we need to modify the report content but KEEP the self-hash same
    # This effectively changes the computed hash but the stored hash matches the OLD content.
    # So we write original report, then append a space? No, json load ignores whitespace.
    # We need to change something that affects JSON serialization.
    # Actually, if we change the file content at all (e.g. changing a value), the self-hash verification should fail
    # because the self-hash was computed on the ORIGINAL content (with None placeholder).
    # If we change content, the computed hash (with None placeholder) changes.
    
    # Let's restore
    report_path.write_text(original_report, encoding="utf-8")
    
    # Now modify report.json in a way that json load is same but hash is different?
    # No, we parse json, set self-hash to None, dump, hash.
    # So if we change formatting (whitespace), the json load is same.
    # But `verify` logic loads json, sets None, dumps with indent=2. 
    # So formatting changes in the file are ignored IF they don't affect parsed data.
    # Wait.
    # compiler.py: dump(indent=2) -> hash -> set hash -> dump(indent=2).
    # verify: load -> set None -> dump(indent=2) -> hash.
    # So if I manually edit report.json to add extra newline at end, `load` ignores it.
    # `dump` produces standard output. Hash matches.
    # So `verify` validates the semantic content of `report.json`, not the exact bytes on disk!
    # Except for the self-hash value itself.
    
    # If I change a value in report.json (e.g. status "ok" -> "failed")
    report_data = json.loads(original_report)
    report_data["status"] = "failed"  # Fake change
    # report.json is not in outputs[]; verify fails on content hash mismatch
    
    report_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")
    
    # Now verify should fail because the computed hash of this NEW content (with None) 
    # will differ from `old_hash`.
    ret = main(["verify", str(report_path)])
    assert ret == 1


def test_run_writes_expanded_sans_and_stable(tmp_path):
    """sans run writes expanded.sans to out dir; it is in report outputs and stable across runs."""
    script_content = "data out; set input; z = x + y; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script_content, encoding="utf-8")
    input_path = tmp_path / "input.csv"
    input_path.write_text("x,y\n1,2\n3,4\n", encoding="utf-8")
    out_dir_1 = tmp_path / "out1"
    out_dir_2 = tmp_path / "out2"

    ret1 = main(["run", str(script_path), "--out", str(out_dir_1), "--tables", f"input={input_path}", "--legacy-sas"])
    assert ret1 == 0
    expanded_path_1 = out_dir_1 / "inputs" / "source" / "expanded.sans"
    assert expanded_path_1.exists(), "run must write expanded.sans to inputs/source/"
    content_1 = expanded_path_1.read_text(encoding="utf-8")

    ret2 = main(["run", str(script_path), "--out", str(out_dir_2), "--tables", f"input={input_path}", "--legacy-sas"])
    assert ret2 == 0
    expanded_path_2 = out_dir_2 / "inputs" / "source" / "expanded.sans"
    assert expanded_path_2.exists()
    content_2 = expanded_path_2.read_text(encoding="utf-8")
    assert content_1 == content_2, "expanded.sans must be stable across runs (same inputs)"

    # Verify(dir); expanded.sans is in inputs[] with role=expanded (not in outputs[])
    ret = main(["verify", str(out_dir_1)])
    assert ret == 0
    report = json.loads((out_dir_1 / "report.json").read_text(encoding="utf-8"))
    expanded_entries = [i for i in report.get("inputs", []) if i.get("role") == "expanded" and "expanded.sans" in (i.get("path") or "")]
    assert len(expanded_entries) >= 1, "expanded.sans must appear in inputs with role=expanded"
    expanded_entry = expanded_entries[0]
    assert expanded_entry.get("sha256") is not None


def test_verify_whitespace_invariance(tmp_path):
    """Verify passes when report.json is rewritten with different indentation or key order.
    We hash the canonical payload (parsed + canonicalize), not file bytes."""
    script_content = "data out; set input; z = x + y; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script_content, encoding="utf-8")
    input_path = tmp_path / "input.csv"
    input_path.write_text("x,y\n1,2\n3,4\n", encoding="utf-8")
    out_dir = tmp_path / "out"

    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"input={input_path}", "--legacy-sas"])
    assert ret == 0

    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report.get("report_sha256")

    # Rewrite with different indent (no indent) — same semantic content
    report_path.write_text(json.dumps(report, separators=(",", ":")), encoding="utf-8")
    ret = main(["verify", str(report_path)])
    assert ret == 0

    # Rewrite with different indent and key order (no sort_keys)
    report_path.write_text(json.dumps(report, indent=4), encoding="utf-8")
    ret = main(["verify", str(report_path)])
    assert ret == 0


def test_verify_path_normalization_invariance(tmp_path):
    """Verify passes when paths in report use backslashes vs slashes; canonicalize normalizes to posix."""
    script_content = "data out; set input; z = x + y; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script_content, encoding="utf-8")
    input_path = tmp_path / "input.csv"
    input_path.write_text("x,y\n1,2\n3,4\n", encoding="utf-8")
    out_dir = tmp_path / "out"

    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"input={input_path}", "--legacy-sas"])
    assert ret == 0

    report_path = out_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report.get("report_sha256")

    # Rewrite paths with backslashes (Windows-style)
    def backslash_paths(obj):
        if isinstance(obj, dict):
            return {k: backslash_paths(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [backslash_paths(x) for x in obj]
        if isinstance(obj, str) and ("/" in obj or "\\" in obj):
            return obj.replace("/", "\\")
        return obj

    report_bs = backslash_paths(copy.deepcopy(report))
    report_path.write_text(json.dumps(report_bs, indent=2), encoding="utf-8")
    ret = main(["verify", str(report_path)])
    assert ret == 0

