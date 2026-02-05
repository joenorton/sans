from sans.fmt import format_text
from sans.sans_script import parse_sans_script


def test_fmt_preserves_inline_comments_and_hash_in_strings() -> None:
    script = (
        "# sans 0.1\n"
        "datasource raw = csv(\"a#b.csv\") # source\n"
        "let s = \"x#y\" # inline\n"
        "table t = from(raw) do\n"
        "\tfilter(s == \"x#y\")\n"
        "end\n"
    )
    parsed = parse_sans_script(script, "script.sans")
    assert parsed is not None
    formatted = format_text(script, file_name="script.sans")
    assert "csv(\"a#b.csv\")" in formatted
    assert "\"x#y\"" in formatted
    assert "csv(\"a#b.csv\")  # source" in formatted
    assert "let s = \"x#y\"  # inline" in formatted
