import pytest
from sans.parser_expr import parse_expression_from_string
from sans.expr import lit, col, binop, boolop, unop, call

def test_parse_literal_number():
    assert parse_expression_from_string("123") == lit(123)
    assert parse_expression_from_string("123.45") == lit(123.45)
    assert parse_expression_from_string("-10") == unop("-", lit(10))

def test_parse_literal_string():
    assert parse_expression_from_string("'hello'") == lit("hello")
    assert parse_expression_from_string('"world"') == lit("world")

def test_parse_literal_null():
    assert parse_expression_from_string(".") == lit(None)
    assert parse_expression_from_string("null") == lit(None)

def test_parse_column_ref():
    assert parse_expression_from_string("my_column") == col("my_column")
    assert parse_expression_from_string("col_1") == col("col_1")
    assert parse_expression_from_string("first.subjid") == col("first.subjid")
    assert parse_expression_from_string("last.visit") == col("last.visit")
    assert parse_expression_from_string("a.col") == col("a.col")

def test_parse_binary_operators_precedence():
    # x = a + b * 2 parses as a + (b*2)
    assert parse_expression_from_string("a + b * 2") == binop(
        "+",
        col("a"),
        binop("*", col("b"), lit(2)),
    )
    # 1 + 2 * 3 - 4 / 5 == (1 + (2 * 3)) - (4 / 5)
    assert parse_expression_from_string("1 + 2 * 3 - 4 / 5") == binop(
        "-",
        binop(
            "+",
            lit(1),
            binop("*", lit(2), lit(3)),
        ),
        binop("/", lit(4), lit(5)),
    )

def test_parse_comparisons():
    assert parse_expression_from_string("a = b") == binop("=", col("a"), col("b"))
    assert parse_expression_from_string("a == b") == binop("=", col("a"), col("b"))
    assert parse_expression_from_string("a > 10") == binop(">", col("a"), lit(10))
    assert parse_expression_from_string("x ~= y") == binop("!=", col("x"), col("y"))
    assert parse_expression_from_string("x != y") == binop("!=", col("x"), col("y"))
    assert parse_expression_from_string("a ne b") == binop("!=", col("a"), col("b"))
    assert parse_expression_from_string("a eq b") == binop("=", col("a"), col("b"))
    assert parse_expression_from_string("a lt b") == binop("<", col("a"), col("b"))
    assert parse_expression_from_string("a le b") == binop("<=", col("a"), col("b"))
    assert parse_expression_from_string("a gt b") == binop(">", col("a"), col("b"))
    assert parse_expression_from_string("a ge b") == binop(">=", col("a"), col("b"))

def test_parse_logical_operators_precedence():
    # if a > 1 and b < 2 works
    assert parse_expression_from_string("a > 1 and b < 2") == boolop(
        "and",
        [
            binop(">", col("a"), lit(1)),
            binop("<", col("b"), lit(2)),
        ],
    )
    assert parse_expression_from_string("not a or b") == boolop(
        "or",
        [
            unop("not", col("a")),
            col("b"),
        ],
    )
    assert parse_expression_from_string("a and not b or c") == boolop(
        "or",
        [
            boolop("and", [col("a"), unop("not", col("b"))]),
            col("c"),
        ],
    )

def test_parse_parentheses():
    assert parse_expression_from_string("(a + b) * c") == binop(
        "*",
        binop("+", col("a"), col("b")),
        col("c"),
    )
    assert parse_expression_from_string("a * (b + c)") == binop(
        "*",
        col("a"),
        binop("+", col("b"), col("c")),
    )

def test_parse_function_call():
    assert parse_expression_from_string("coalesce(a, b)") == call(
        "coalesce",
        [col("a"), col("b")],
    )
    assert parse_expression_from_string("if(x > 0, 1, 0)") == call(
        "if",
        [
            binop(">", col("x"), lit(0)),
            lit(1),
            lit(0),
        ],
    )
    assert parse_expression_from_string("put(aesev, $sev.)") == call(
        "put",
        [col("aesev"), col("$sev.")],
    )
    assert parse_expression_from_string("input(put(a, $fmt.), best.)") == call(
        "input",
        [
            call("put", [col("a"), col("$fmt.")]),
            col("best."),
        ],
    )

def test_parse_unsupported_function():
    with pytest.raises(ValueError, match="Unsupported function 'unknown_func'"):
        parse_expression_from_string("unknown_func(a)")

def test_parse_unexpected_token():
    with pytest.raises(ValueError, match="Unexpected token\\(s\\) after expression"):
        parse_expression_from_string("a + b c")

def test_parse_malformed_expression():
    with pytest.raises(ValueError, match="Expected token of type RPAREN, got EOF"):
        parse_expression_from_string("(a + b")
    with pytest.raises(ValueError, match="Unexpected EOF while parsing expression"):
        parse_expression_from_string("a +")
