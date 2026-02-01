import struct
from pathlib import Path
from sans.xpt import load_xpt, dump_xpt, _ieee_to_ibm, _ibm_to_ieee

def test_ibm_float_conversion():
    # Test a few known values
    # 1.0
    b1 = _ieee_to_ibm(1.0)
    v1 = _ibm_to_ieee(b1)
    assert v1 == 1.0
    
    # 0.0
    b0 = _ieee_to_ibm(0.0)
    v0 = _ibm_to_ieee(b0)
    assert v0 == 0.0
    
    # Missing (.)
    bm = _ieee_to_ibm(None)
    vm = _ibm_to_ieee(bm)
    assert vm is None
    
    # Negative
    bn = _ieee_to_ibm(-123.456)
    vn = _ibm_to_ieee(bn)
    assert vn == -123.456

def test_xpt_metadata_and_missing(tmp_path):
    path = tmp_path / "test.xpt"
    rows = [
        {"char_var": "hello", "num_var": 1.23},
        {"char_var": "", "num_var": None},
        {"char_var": "world", "num_var": 0.0}
    ]
    columns = ["char_var", "num_var"]
    
    dump_xpt(path, rows, columns)
    
    # Read back
    data = load_xpt(path)
    assert len(data) == 3
    
    assert data[0]["char_var"] == "hello"
    assert data[0]["num_var"] == 1.23
    
    # SAS missing char is empty string/spaces. My reader strips.
    assert data[1]["char_var"] == ""
    assert data[1]["num_var"] is None
    
    assert data[2]["char_var"] == "world"
    assert data[2]["num_var"] == 0.0

def test_xpt_long_char(tmp_path):
    path = tmp_path / "long.xpt"
    long_str = "A" * 100
    rows = [{"msg": long_str}]
    dump_xpt(path, rows)
    
    data = load_xpt(path)
    assert data[0]["msg"] == long_str
