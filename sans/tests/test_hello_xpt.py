import struct
from pathlib import Path
from sans.__main__ import main
import csv
from sans.xpt import load_xpt

def test_hello_xpt_roundtrip(tmp_path):
    # 1. Create Input CSV with trailing spaces, missing numeric, long string
    in_csv = tmp_path / "source.csv"
    in_csv.write_text(
        "id,val,msg\n"
        "1,10,\"ABC  \"\n"
        "2,,\"LONGSTRING1234567890\"\n",
        encoding="utf-8",
    )
    
    # 2. Run Step 1: CSV -> XPT
    script1 = """
    data out;
      set source;
      val = val * 2;
    run;
    """
    script1_path = tmp_path / "step1.sas"
    script1_path.write_text(script1, encoding="utf-8")
    
    out1_dir = tmp_path / "out1"
    
    ret = main([
        "run", str(script1_path), 
        "--out", str(out1_dir), 
        "--tables", f"source={in_csv}",
        "--format", "xpt"
    ])
    assert ret == 0
    
    out1_xpt = out1_dir / "outputs" / "out.xpt"
    assert out1_xpt.exists()
    
    # Verify XPT content (canonicalized string trim, missing numeric)
    data = load_xpt(out1_xpt)
    assert len(data) == 2
    assert data[0]["id"] == 1.0
    assert data[0]["val"] == 20.0
    assert data[0]["msg"] == "ABC"
    assert data[1]["id"] == 2.0
    assert data[1]["val"] is None
    assert data[1]["msg"] == "LONGSTRING1234567890"
    
    # 3. Run Step 2: XPT -> CSV
    script2 = """
    data final;
      set intermediate;
      val = val + 1;
    run;
    """
    script2_path = tmp_path / "step2.sas"
    script2_path.write_text(script2, encoding="utf-8")
    
    out2_dir = tmp_path / "out2"
    
    ret = main([
        "run", str(script2_path),
        "--out", str(out2_dir),
        "--tables", f"intermediate={out1_xpt}"
    ])
    assert ret == 0
    
    out2_csv = out2_dir / "outputs" / "final.csv"
    assert out2_csv.exists()
    
    with out2_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
        
    assert rows[0] == ["id", "val", "msg"]
    assert rows[1] == ["1.0", "21.0", "ABC"]
    assert rows[2] == ["2.0", "", "LONGSTRING1234567890"]

    # 4. Verify bundle
    ret = main(["verify", str(out2_dir)])
    assert ret == 0

def test_xpt_determinism(tmp_path):
    # Verify two runs produce identical bytes
    in_csv = tmp_path / "source.csv"
    in_csv.write_text("id\n1\n", encoding="utf-8")
    
    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")
    
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    
    main(["run", str(script_path), "--out", str(out_a), "--tables", f"source={in_csv}", "--format", "xpt"])
    main(["run", str(script_path), "--out", str(out_b), "--tables", f"source={in_csv}", "--format", "xpt"])
    
    bytes_a = (out_a / "outputs" / "out.xpt").read_bytes()
    bytes_b = (out_b / "outputs" / "out.xpt").read_bytes()
    
    assert bytes_a == bytes_b
