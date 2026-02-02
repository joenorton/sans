import json
import pytest
from sans.compiler import compile_sans_script, UnknownBlockStep
from sans.ir import OpStep
from sans.runtime import _eval_expr, RuntimeFailure
from pathlib import Path

DEMO_SANS = Path("demo/sans_script/demo.sans")

def test_const_decimal_canonical():
    """const { pi = 3.14 } parses and produces canonical decimal in IR (no Python float)."""
    script = """# sans 0.1
datasource in = csv("in.csv")
const { pi = 3.14 }
table out = from(in) do
  derive(x = 1)
end
out
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    const_step = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "const")
    bindings = const_step.params.get("bindings") or {}
    assert "pi" in bindings
    assert bindings["pi"] == {"type": "decimal", "value": "3.14"}
    assert not any(isinstance(s, UnknownBlockStep) and s.severity == "fatal" for s in irdoc.steps)


def test_const_decimal_normalization():
    """Decimal literal strings are normalized at parse-time so equal values hash identically."""
    # .5 -> 0.5
    irdoc = compile_sans_script(
        """# sans 0.1
const { a = .5 }
datasource in = csv("x.csv")
table t = from(in) do
  derive(x = 1)
end
t
""", "test.sans", tables={"in"}
    )
    const_step = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "const")
    bindings = const_step.params.get("bindings") or {}
    assert bindings.get("a") == {"type": "decimal", "value": "0.5"}

    # 3.140 -> 3.14
    irdoc = compile_sans_script(
        """# sans 0.1
const { a = 3.140 }
datasource in = csv("x.csv")
table t = from(in) do
  derive(x = 1)
end
t
""", "test.sans", tables={"in"}
    )
    bindings = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "const").params.get("bindings") or {}
    assert bindings.get("a") == {"type": "decimal", "value": "3.14"}

    # 3.0 -> 3
    irdoc = compile_sans_script(
        """# sans 0.1
const { a = 3.0 }
datasource in = csv("x.csv")
table t = from(in) do
  derive(x = 1)
end
t
""", "test.sans", tables={"in"}
    )
    bindings = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "const").params.get("bindings") or {}
    assert bindings.get("a") == {"type": "decimal", "value": "3"}

    # -0.00 -> 0
    irdoc = compile_sans_script(
        """# sans 0.1
const { a = -0.00 }
datasource in = csv("x.csv")
table t = from(in) do
  derive(x = 1)
end
t
""", "test.sans", tables={"in"}
    )
    bindings = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "const").params.get("bindings") or {}
    assert bindings.get("a") == {"type": "decimal", "value": "0"}

    # Normal cases: preserve sign and digits
    irdoc = compile_sans_script(
        """# sans 0.1
const { a = -12.34 }
datasource in = csv("x.csv")
table t = from(in) do
  derive(x = 1)
end
t
""", "test.sans", tables={"in"}
    )
    bindings = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "const").params.get("bindings") or {}
    assert bindings.get("a") == {"type": "decimal", "value": "-12.34"}


def test_decimal_arithmetic_rejects_float():
    """Runtime raises SANS_RUNTIME_DECIMAL_NO_FLOAT when mixing Decimal with Python float."""
    # expr: decimal_lit + col("x") with row x=1.5 (float)
    node = {
        "type": "binop",
        "op": "+",
        "left": {"type": "lit", "value": {"type": "decimal", "value": "3.14"}},
        "right": {"type": "col", "name": "x"},
    }
    row = {"x": 1.5}
    with pytest.raises(RuntimeFailure) as exc_info:
        _eval_expr(node, row, None)
    assert exc_info.value.code == "SANS_RUNTIME_DECIMAL_NO_FLOAT"
    assert "float" in exc_info.value.message.lower()


def test_summary_default_naming_and_ordering():
    script = """# sans 0.1
datasource in = csv("in.csv")
table stats = summary(from(in))
  .class(grp)
  .var(x, y)
table out = stats select grp, x_mean, y_mean
out
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    # Check IR lowering
    summary_step = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "aggregate")
    assert summary_step.params["vars"] == ["x", "y"]
    assert summary_step.params["stats"] == ["mean"]
    
    # Check that validation passed (no errors)
    assert not any(isinstance(s, UnknownBlockStep) and s.severity == "fatal" for s in irdoc.steps)

def test_summary_unknown_column_error():
    script = """# sans 0.1
datasource in = csv("in.csv")
table stats = summary(from(in))
  .class(grp)
  .var(x)
table out = stats select grp, x_sum
out
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    # Should have a fatal error for unknown column x_sum
    error = next(s for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.code == "E_UNKNOWN_COLUMN")
    assert "x_sum" in error.message

def test_unused_let_warning():
    script = """# sans 0.1
datasource in = csv("in.csv")
let unused = 42
let used = 10
table out = from(in) do
  derive(val = used)
end
out
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    # Should have a warning for 'unused'
    warning = next(s for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.severity == "warning")
    assert warning.code == "W_UNUSED_LET"
    assert "unused" in warning.message
    
    # 'used' should NOT have a warning
    assert not any(f"binding 'used'" in s.message for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.severity == "warning")

def test_summary_with_explicit_stats():
    script = """# sans 0.1
datasource in = csv("in.csv")
table stats = summary(from(in))
  .class(grp)
  .var(x)
  .stats(mean, sum)
table out = stats select grp, x_mean, x_sum
out
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    assert not any(isinstance(s, UnknownBlockStep) and s.severity == "fatal" for s in irdoc.steps)
    summary_step = next(s for s in irdoc.steps if isinstance(s, OpStep) and s.op == "aggregate")
    assert summary_step.params["stats"] == ["mean", "sum"]

def test_demo_sans_semantic_acceptance():
    if not DEMO_SANS.exists():
        return
    text = DEMO_SANS.read_text(encoding="utf-8")
    irdoc = compile_sans_script(text, str(DEMO_SANS), tables={"in"})
    # Should pass all semantic checks
    errors = [s for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.severity == "fatal"]
    assert not errors, f"Demo script failed validation: {errors[0].message if errors else ''}"

def test_rename_destructive_semantic_error():
    script = """# sans 0.1
    datasource in = csv("in.csv")
    table t1 = from(in) do
      derive(update! a = 1)
    end
    table out = t1 rename(a -> b) derive(new_c = a + 1)
    out
    """
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    error = next(s for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.code == "E_UNKNOWN_COLUMN")
    assert "a" in error.message

def test_strict_mutation_error():
    script = """# sans 0.1
datasource in = csv("in.csv")
table t1 = from(in) do
  derive(new_a = 1)
end
table t2 = t1 derive(new_a = 2)
t2
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    error = next(s for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.code == "E_STRICT_MUTATION")
    assert "a" in error.message

def test_ternary_if_requires_3_args():
    script = """# sans 0.1
datasource in = csv("in.csv")
table out = from(in) do
  derive(new_c = if(a > 0, 1))
end
out
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    error = next(s for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.code == "E_BAD_EXPR")
    assert "if()" in error.message

def test_invalid_update_error():
    script = """# sans 0.1
datasource in = csv("in.csv")
table out = from(in) do
  derive(update! mystery = 42)
end
out
"""
    irdoc = compile_sans_script(script, "test.sans", tables={"in"})
    error = next(s for s in irdoc.steps if isinstance(s, UnknownBlockStep) and s.code == "E_INVALID_UPDATE")
    assert "mystery" in error.message