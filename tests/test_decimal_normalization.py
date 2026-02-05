import pytest

from sans.sans_script.ast import ConstDecl
from sans.sans_script.errors import SansScriptError
from sans.sans_script.parser import normalize_decimal_string, parse_sans_script


@pytest.mark.parametrize(
    "raw, expected",
    [
        (".5", "0.5"),
        ("-.5", "-0.5"),
        ("0003.1400", "3.14"),
        ("  +0003.1400  ", "3.14"),
        ("5.", "5"),
        ("0000", "0"),
        ("0.0000", "0"),
        ("-0.0", "0"),
        ("-00.000", "0"),
    ],
)
def test_normalize_decimal_string_success(raw, expected):
    assert normalize_decimal_string(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "1.2.3",
        "--1.0",
        "+",
        ".",
        "-",
        "",
        "  ",
        "1e-3",
        "1E3",
        "abc",
        "0x10",
        "01_2",
    ],
)
def test_normalize_decimal_string_rejects_invalid(raw):
    with pytest.raises(ValueError):
        normalize_decimal_string(raw)


def _parse_single_const(line: str) -> ConstDecl:
    script = f"# sans 0.1\n{line}\n"
    parsed = parse_sans_script(script, "test.sans")
    assert len(parsed.statements) == 1
    stmt = parsed.statements[0]
    assert isinstance(stmt, ConstDecl)
    return stmt


def test_const_decimal_canonicalizes_leading_dot():
    stmt = _parse_single_const("const { a = .5 }")
    assert stmt.bindings["a"] == {"type": "decimal", "value": "0.5"}


def test_const_decimal_canonicalizes_trailing_dot():
    stmt = _parse_single_const("const { a = 5. }")
    assert stmt.bindings["a"] == {"type": "decimal", "value": "5"}


def test_const_decimal_rejects_exponent():
    script = "# sans 0.1\nconst { a = 1e-3 }\n"
    with pytest.raises(SansScriptError) as excinfo:
        parse_sans_script(script, "test.sans")
    err = excinfo.value
    assert err.code == "E_PARSE"
    assert "exponent" in err.message.lower()
