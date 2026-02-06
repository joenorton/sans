import pytest

from sans.expr import binop, boolop, call, col, lit, unop
from sans.type_infer import TypeInferenceError, infer_expr_type
from sans.types import Type


def test_unknown_arithmetic_rejected():
    env = {"a": Type.UNKNOWN}
    with pytest.raises(TypeInferenceError) as exc_info:
        infer_expr_type(binop("+", col("a"), lit(1)), env)
    assert exc_info.value.code == "E_TYPE_UNKNOWN"


def test_unknown_comparisons():
    env = {"a": Type.UNKNOWN, "b": Type.UNKNOWN}
    with pytest.raises(TypeInferenceError) as exc_info:
        infer_expr_type(binop("<", col("a"), lit(1)), env)
    assert exc_info.value.code == "E_TYPE_UNKNOWN"

    assert infer_expr_type(binop("==", col("a"), lit(None)), env) == Type.BOOL
    assert infer_expr_type(binop("!=", col("a"), lit(None)), env) == Type.BOOL
    assert infer_expr_type(binop("==", col("a"), col("b")), env) == Type.BOOL

    with pytest.raises(TypeInferenceError) as exc_info:
        infer_expr_type(binop("==", col("a"), lit(1)), env)
    assert exc_info.value.code == "E_TYPE_UNKNOWN"


def test_unknown_boolean_ops():
    env = {"a": Type.UNKNOWN}
    with pytest.raises(TypeInferenceError) as exc_info:
        infer_expr_type(boolop("and", [col("a"), lit(True)]), env)
    assert exc_info.value.code == "E_TYPE_UNKNOWN"

    with pytest.raises(TypeInferenceError) as exc_info:
        infer_expr_type(unop("not", col("a")), env)
    assert exc_info.value.code == "E_TYPE_UNKNOWN"


def test_unknown_if_rules():
    env = {"a": Type.UNKNOWN}
    assert infer_expr_type(call("if", [lit(True), col("a"), lit(1)]), env) == Type.UNKNOWN
    assert infer_expr_type(call("if", [lit(True), col("a"), lit("x")]), env) == Type.UNKNOWN
    with pytest.raises(TypeInferenceError) as exc_info:
        infer_expr_type(call("if", [col("a"), lit(1), lit(2)]), env)
    assert exc_info.value.code == "E_TYPE_UNKNOWN"
