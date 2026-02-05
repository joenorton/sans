import pytest

from sans.legacy import LegacyExprError, translate_legacy_predicate


def test_translate_legacy_predicate_basic():
    assert translate_legacy_predicate("a eq 2") == "a == 2"
    assert translate_legacy_predicate("b lt 0") == "b < 0"
    assert translate_legacy_predicate("c ge 5") == "c >= 5"


def test_translate_legacy_predicate_preserves_strings():
    assert translate_legacy_predicate('a eq "x#y"') == 'a == "x#y"'


def test_translate_legacy_predicate_mixed():
    text = "a eq 2 or b lt 0 or c ge 5"
    assert translate_legacy_predicate(text) == "a == 2 or b < 0 or c >= 5"


def test_translate_legacy_predicate_errors():
    with pytest.raises(LegacyExprError) as exc_info:
        translate_legacy_predicate("a <> 1")
    assert exc_info.value.code == "E_LEGACY_EXPR"

    with pytest.raises(LegacyExprError) as exc_info:
        translate_legacy_predicate("a eq")
    assert exc_info.value.code == "E_LEGACY_EXPR"
