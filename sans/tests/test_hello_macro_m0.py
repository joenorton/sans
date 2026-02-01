import json
from pathlib import Path
from sans.__main__ import main
import csv

def test_macro_m0_and_control_flow(tmp_path):
    # 1. Setup Include File
    inc_path = tmp_path / "inc.sas"
    inc_path.write_text("z = x + y + &CONST.;", encoding="utf-8")
    
    # 2. Main Script
    script_content = """
    %let CONST = 100;
    
    data out;
      set in;
      %include "inc.sas";
      
      if z > 110 then category = "high";
      else if z > 105 then category = "mid";
      else category = "low";
      
      select;
        when (category = "high") status = "H";
        when (category = "mid") status = "M";
        otherwise status = "L";
      end;
      
      if category = "high" then do i = 1 to 2;
        output;
      end;
      else output;
      
      keep x y z category status;
    run;
    """
    script_path = tmp_path / "main.sas"
    script_path.write_text(script_content, encoding="utf-8")
    
    in_csv = tmp_path / "in.csv"
    in_csv.write_text("x,y\n1,2\n5,6\n10,20\n", encoding="utf-8")
    
    out_dir = tmp_path / "out"
    
    # 3. Run
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"in={in_csv}"])
    assert ret == 0
    
    # 4. Verify preprocessed.sas exists
    preprocessed_path = out_dir / "preprocessed.sas"
    assert preprocessed_path.exists()
    prep_text = preprocessed_path.read_text(encoding="utf-8")
    assert "z = x + y + 100;" in prep_text
    
    # 5. Verify Output
    out_csv = out_dir / "out.csv"
    assert out_csv.exists()
    
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
        
    # Input rows:
    # 1,2   -> z=103 -> low  -> 1 row
    # 5,6   -> z=111 -> high -> 2 rows (loop 1 to 2)
    # 10,20 -> z=130 -> high -> 2 rows (loop 1 to 2)
    # Total rows: 1 + 2 + 2 = 5
    
    assert rows[0] == ["x", "y", "z", "category", "status"]
    # Check some rows
    data_rows = rows[1:]
    assert len(data_rows) == 5
    
    # Row 1: 1,2,103,low,L
    assert data_rows[0] == ["1", "2", "103", "low", "L"]
    
    # Row 2 & 3: 5,6,111,high,H
    assert data_rows[1] == ["5", "6", "111", "high", "H"]
    assert data_rows[2] == ["5", "6", "111", "high", "H"]
    
    # Row 4 & 5: 10,20,130,high,H
    assert data_rows[3] == ["10", "20", "130", "high", "H"]
    assert data_rows[4] == ["10", "20", "130", "high", "H"]

def test_macro_undefined_var(tmp_path):
    # Standard SAS keeps &VAR if undefined
    script = "data out; x = &UNDEFINED.; run;"
    script_path = tmp_path / "undef.sas"
    script_path.write_text(script, encoding="utf-8")
    
    out_dir = tmp_path / "out"
    # This should fail during compilation (expression error) because &UNDEFINED. remains
    # and is not valid SAS syntax for an expression unless it's a variable name, 
    # but the dot makes it &UNDEFINED. which is invalid.
    
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", "in=in.csv"])
    # It might fail with SANS_PARSE_EXPRESSION_ERROR
    assert ret != 0
