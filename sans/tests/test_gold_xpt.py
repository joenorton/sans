from pathlib import Path

from sans.__main__ import main
from sans.xpt import load_xpt


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    path.write_text("\n".join([",".join(row) for row in rows]), encoding="utf-8")


def test_gold_xpt_missing_numeric(tmp_path):
    in_csv = tmp_path / "source.csv"
    _write_csv(in_csv, [["id", "val"], ["1", ""], ["2", "3"]])

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_missing"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret == 0

    out_xpt = out_dir / "out.xpt"
    data = load_xpt(out_xpt)
    assert data[0]["val"] is None
    assert data[1]["val"] == 3.0


def test_gold_xpt_char_width_preserve(tmp_path):
    in_csv = tmp_path / "source.csv"
    long_str = "ABCDEFGHIJKLMNO"
    _write_csv(in_csv, [["id", "msg"], ["1", long_str]])

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_long"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret == 0

    out_xpt = out_dir / "out.xpt"
    data = load_xpt(out_xpt)
    assert data[0]["msg"] == long_str


def test_gold_xpt_mixed_numeric_missing(tmp_path):
    in_csv = tmp_path / "source.csv"
    _write_csv(in_csv, [["id", "val"], ["1", "1.25"], ["2", ""], ["3", "2"]])

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_mixed"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret == 0

    out_xpt = out_dir / "out.xpt"
    data = load_xpt(out_xpt)
    assert data[0]["val"] == 1.25
    assert data[1]["val"] is None
    assert data[2]["val"] == 2.0


def test_gold_xpt_deterministic_bytes(tmp_path):
    in_csv = tmp_path / "source.csv"
    _write_csv(in_csv, [["id"], ["1"]])

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_a = tmp_path / "out_a"
    out_b = tmp_path / "out_b"

    main(["run", str(script_path), "--out", str(out_a), "--tables", f"source={in_csv}", "--format", "xpt"])
    main(["run", str(script_path), "--out", str(out_b), "--tables", f"source={in_csv}", "--format", "xpt"])

    bytes_a = (out_a / "out.xpt").read_bytes()
    bytes_b = (out_b / "out.xpt").read_bytes()
    assert bytes_a == bytes_b


def test_gold_xpt_char_trailing_spaces_trimmed(tmp_path):
    in_csv = tmp_path / "source.csv"
    in_csv.write_text("id,msg\n1,\"A  \"\n", encoding="utf-8")

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_trim"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret == 0

    data = load_xpt(out_dir / "out.xpt")
    assert data[0]["msg"] == "A"


def test_gold_xpt_column_order_preserved(tmp_path):
    in_csv = tmp_path / "source.csv"
    in_csv.write_text("b,a\n2,1\n", encoding="utf-8")

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_order"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret == 0

    data = load_xpt(out_dir / "out.xpt")
    assert list(data[0].keys()) == ["b", "a"]


def test_gold_xpt_char_missing_roundtrip(tmp_path):
    in_csv = tmp_path / "source.csv"
    in_csv.write_text("id,msg\n1,\n", encoding="utf-8")

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_char_missing"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret == 0

    data = load_xpt(out_dir / "out.xpt")
    assert data[0]["msg"] == ""


def test_gold_xpt_length_cap_error(tmp_path):
    in_csv = tmp_path / "source.csv"
    long_str = "X" * 201
    in_csv.write_text(f"id,msg\n1,{long_str}\n", encoding="utf-8")

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_len_cap"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret != 0


def test_gold_xpt_corrupt_header_error(tmp_path):
    bad = tmp_path / "bad.xpt"
    bad.write_bytes(b"not an xpt file")

    script = "data out; set bad; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_bad"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"bad={bad}"])
    assert ret != 0


def test_gold_xpt_label_format_warning(tmp_path):
    in_csv = tmp_path / "source.csv"
    in_csv.write_text("id,val\n1,2\n", encoding="utf-8")

    script = "data out; set source; run;"
    script_path = tmp_path / "script.sas"
    script_path.write_text(script, encoding="utf-8")

    out_dir = tmp_path / "out_warn"
    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={in_csv}", "--format", "xpt"])
    assert ret == 0

    xpt_path = out_dir / "out.xpt"
    data = bytearray(xpt_path.read_bytes())
    # Patch a label-like field in the first NAMESTR chunk (writer layout: 7 blocks before namestrs)
    start = 7 * 80
    if start + 24 < len(data):
        data[start + 16:start + 21] = b"LABEL"
        xpt_path.write_bytes(bytes(data))

    ret = main(["run", str(script_path), "--out", str(out_dir), "--tables", f"source={xpt_path}"])
    assert ret == 10
