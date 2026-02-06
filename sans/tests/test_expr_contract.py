import re

import pytest

from sans.expr import binop, boolop, col, unop
from sans.expr_contract import STRICT_KEYWORDS, STRICT_OPERATORS, STRICT_PRECEDENCE
from sans.parser_expr import parse_expression_from_string
from sans.sans_script import SansScriptError, parse_sans_script
from sans.sans_script.expand_printer import _expr_to_string


def _round_trip(expr_text: str):
    ast = parse_expression_from_string(expr_text)
    emitted = _expr_to_string(ast)
    ast2 = parse_expression_from_string(emitted)
    assert ast2 == ast
    _assert_no_legacy_tokens(emitted)


def _assert_no_legacy_tokens(text: str) -> None:
    assert "^=" not in text
    assert "~=" not in text
    assert "<>" not in text
    assert re.search(r"(?<![!<>=])=(?![=])", text) is None
    assert re.search(r"\b(eq|ne|lt|le|gt|ge)\b", text, flags=re.IGNORECASE) is None


def test_contract_tokens_locked():
    assert STRICT_OPERATORS == {"+", "-", "*", "/", "==", "!=", "<", "<=", ">", ">="}
    assert STRICT_KEYWORDS == {"and", "or", "not"}


def test_contract_precedence_table():
    # Lowest to highest precedence
    assert STRICT_PRECEDENCE == [
        ("or",),
        ("and",),
        ("not",),
        ("!=", "<", "<=", "==", ">", ">="),
        ("+", "-"),
        ("*", "/"),
    ]


def test_contract_doc_lists_all_tokens():
    doc = open("docs/EXPR_CONTRACT.md", "r", encoding="utf-8").read()
    for token in sorted(STRICT_OPERATORS | STRICT_KEYWORDS):
        assert token in doc


@pytest.mark.parametrize(
    "expr_text",
    [
        "a + b",
        "a - b",
        "a * b",
        "a / b",
        "a == b",
        "a != b",
        "a < b",
        "a <= b",
        "a > b",
        "a >= b",
        "a == b and c == d",
        "a == b or c == d",
        "not (a == b)",
    ],
)
def test_strict_operator_round_trip(expr_text):
    _round_trip(expr_text)


def test_precedence_arithmetic():
    assert parse_expression_from_string("a + b * c") == binop(
        "+",
        col("a"),
        binop("*", col("b"), col("c")),
    )
    assert parse_expression_from_string("(a + b) * c") == binop(
        "*",
        binop("+", col("a"), col("b")),
        col("c"),
    )


def test_precedence_boolean():
    assert parse_expression_from_string("a == b or c == d and e == f") == boolop(
        "or",
        [
            binop("==", col("a"), col("b")),
            boolop(
                "and",
                [
                    binop("==", col("c"), col("d")),
                    binop("==", col("e"), col("f")),
                ],
            ),
        ],
    )
    assert parse_expression_from_string("not a == b") == unop(
        "not",
        binop("==", col("a"), col("b")),
    )


@pytest.mark.parametrize(
    "bad_expr",
    [
        "a = b",
        "a eq b",
        "a ne b",
        "a lt b",
        "a le b",
        "a gt b",
        "a ge b",
        "a ^= b",
        "a ~= b",
        "a <> b",
        "a === b",
    ],
)
def test_strict_rejects_legacy_tokens(bad_expr):
    with pytest.raises(SansScriptError) as exc_info:
        parse_sans_script(
            "# sans 0.1\n"
            f"table t = from(in) filter {bad_expr}\n",
            "bad_expr.sans",
        )
    assert exc_info.value.code == "E_BAD_EXPR"
