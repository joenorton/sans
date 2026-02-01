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
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"input={input_path}"])
    assert ret == 0
    
    report_path = out_dir / "report.json"
    assert report_path.exists()
    
    # Verify Success
    ret = main(["verify", str(report_path)])
    assert ret == 0
    
    # Verify with directory
    ret = main(["verify", str(out_dir)])
    assert ret == 0
    
    # Tamper Output
    out_csv = out_dir / "out.csv"
    original_out = out_csv.read_text(encoding="utf-8")
    out_csv.write_text("tampered", encoding="utf-8")
    
    ret = main(["verify", str(report_path)])
    assert ret == 1
    
    # Restore Output
    out_csv.write_text(original_out, encoding="utf-8")
    ret = main(["verify", str(report_path)])
    assert ret == 0
    
    # Tamper Input
    original_in = input_path.read_text(encoding="utf-8")
    input_path.write_text("tampered", encoding="utf-8")
    
    ret = main(["verify", str(report_path)])
    assert ret == 1
    
    # Restore Input
    input_path.write_text(original_in, encoding="utf-8")
    ret = main(["verify", str(report_path)])
    assert ret == 0
    
    # Tamper Report (Self-Check)
    original_report = report_path.read_text(encoding="utf-8")
    report_data = json.loads(original_report)
    # Modify a hash in the report to mismatch
    report_data["outputs"][0]["sha256"] = "deadbeef"
    report_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")
    
    ret = main(["verify", str(report_path)])
    assert ret == 1 # Should fail because out.csv hash mismatch
    
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
    report_data["status"] = "failed" # Fake change
    # Keep the old hash for report.json!
    # find report.json hash
    old_hash = None
    for o in report_data["outputs"]:
        if o["path"] == str(report_path):
            old_hash = o["sha256"]
            # We don't change it here, so it remains correct for the OLD content
    
    report_path.write_text(json.dumps(report_data, indent=2), encoding="utf-8")
    
    # Now verify should fail because the computed hash of this NEW content (with None) 
    # will differ from `old_hash`.
    ret = main(["verify", str(report_path)])
    assert ret == 1
    
