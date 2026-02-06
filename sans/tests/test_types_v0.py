import pytest

from sans.expr import binop, boolop, call, lit, unop
from sans.type_infer import TypeInferenceError, infer_expr_type
from sans.types import Type, unify


def test_unify_if_rules():
    assert unify(Type.INT, Type.INT, context="if") == Type.INT
    assert unify(Type.INT, Type.DECIMAL, context="if") == Type.DECIMAL
    assert unify(Type.NULL, Type.STRING, context="if") == Type.STRING
    assert unify(Type.STRING, Type.NULL, context="if") == Type.STRING


def test_if_inference():
    expr = call("if", [lit(True), lit(1), lit(2)])
    assert infer_expr_type(expr) == Type.INT
    expr = call("if", [lit(True), lit(1), lit(2.0)])
    assert infer_expr_type(expr) == Type.DECIMAL
    with pytest.raises(TypeInferenceError):
        infer_expr_type(call("if", [lit(True), lit(1), lit("x")]))
    with pytest.raises(TypeInferenceError):
        infer_expr_type(call("if", [lit(1), lit(1), lit(2)]))


def test_numeric_inference():
    assert infer_expr_type(binop("+", lit(1), lit(2))) == Type.INT
    assert infer_expr_type(binop("+", lit(1), lit(2.0))) == Type.DECIMAL
    assert infer_expr_type(binop("/", lit(1), lit(2))) == Type.DECIMAL


def test_null_comparisons():
    assert infer_expr_type(binop("==", lit(None), lit(1))) == Type.BOOL
    with pytest.raises(TypeInferenceError):
        infer_expr_type(binop("<", lit(None), lit(1)))


def test_boolean_ops():
    assert infer_expr_type(unop("not", lit(True))) == Type.BOOL
    with pytest.raises(TypeInferenceError):
        infer_expr_type(unop("not", lit(1)))
    assert infer_expr_type(boolop("and", [lit(True), lit(False)])) == Type.BOOL
    with pytest.raises(TypeInferenceError):
        infer_expr_type(boolop("and", [lit(1), lit(True)]))
