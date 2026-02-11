import pytest

from sans.ir.schema import validate_sans_ir, validate_sans_ir_structure


def _valid_ir() -> dict:
    return {
        "version": "0.1",
        "datasources": {
            "lb": {
                "kind": "csv",
                "path": "lb.csv",
                "columns": {"USUBJID": "string"},
            }
        },
        "steps": [
            {
                "id": "ds:lb",
                "op": "datasource",
                "inputs": [],
                "outputs": ["__datasource__lb"],
                "params": {"name": "lb", "kind": "csv", "path": "lb.csv"},
            },
            {
                "id": "out:t1",
                "op": "identity",
                "inputs": ["__datasource__lb"],
                "outputs": ["t1"],
                "params": {},
            },
            {
                "id": "out:t1:save",
                "op": "save",
                "inputs": ["t1"],
                "outputs": [],
                "params": {"path": "t1.csv"},
            },
        ],
    }


def test_structural_validator_accepts_valid_ir():
    doc = _valid_ir()
    validate_sans_ir(doc)


def test_structural_validator_rejects_unknown_input_reference():
    doc = _valid_ir()
    doc["steps"][1]["inputs"] = ["missing_table"]
    with pytest.raises(ValueError, match="unknown input table|before it is produced"):
        validate_sans_ir(doc)


def test_structural_validator_allows_dangling_table_as_warning():
    doc = _valid_ir()
    doc["steps"].insert(
        2,
        {
            "id": "out:dead",
            "op": "identity",
            "inputs": ["t1"],
            "outputs": ["dead"],
            "params": {},
        },
    )
    warnings = validate_sans_ir(doc)
    assert any("dangling table 'dead'" in w for w in warnings)


def test_structural_validator_rejects_cycles():
    doc = _valid_ir()
    doc["steps"][1]["inputs"] = ["t1"]
    with pytest.raises(ValueError, match="cyclic step dependencies|before it is produced"):
        validate_sans_ir(doc)


def test_structural_validator_allows_multiple_saves_for_same_table():
    doc = _valid_ir()
    doc["steps"].append(
        {
            "id": "out:t1:save:dup",
            "op": "save",
            "inputs": ["t1"],
            "outputs": [],
            "params": {"path": "dup.csv"},
        }
    )
    validate_sans_ir(doc)


def test_structural_validator_rejects_duplicate_save_destination_for_same_table():
    doc = _valid_ir()
    doc["steps"].append(
        {
            "id": "out:t1:save:dup-path",
            "op": "save",
            "inputs": ["t1"],
            "outputs": [],
            "params": {"path": "t1.csv"},
        }
    )
    with pytest.raises(ValueError, match="duplicate save destination"):
        validate_sans_ir(doc)


def test_structural_validator_rejects_zero_saves():
    doc = _valid_ir()
    doc["steps"] = [s for s in doc["steps"] if s["op"] != "save"]
    with pytest.raises(ValueError, match="at least one save"):
        validate_sans_ir(doc)


def test_structural_validator_unreachable_is_warning_by_default():
    doc = _valid_ir()
    doc["steps"].insert(
        2,
        {
            "id": "out:orphan",
            "op": "identity",
            "inputs": ["__datasource__lb"],
            "outputs": ["orphan"],
            "params": {},
        },
    )
    warnings = validate_sans_ir(doc)
    assert any("unreachable steps" in w for w in warnings)


def test_structural_validator_unreachable_fails_in_strict_mode():
    doc = _valid_ir()
    doc["steps"].insert(
        2,
        {
            "id": "out:orphan",
            "op": "identity",
            "inputs": ["__datasource__lb"],
            "outputs": ["orphan"],
            "params": {},
        },
    )
    with pytest.raises(ValueError, match="unreachable steps"):
        validate_sans_ir(doc, strict=True)


def test_structure_warnings_are_deterministic_for_independent_siblings():
    doc = _valid_ir()
    # Two independent orphan branches; warning ordering should be deterministic.
    doc["steps"].insert(
        2,
        {
            "id": "out:sib_b",
            "op": "identity",
            "inputs": ["__datasource__lb"],
            "outputs": ["sib_b"],
            "params": {},
        },
    )
    doc["steps"].insert(
        2,
        {
            "id": "out:sib_a",
            "op": "identity",
            "inputs": ["__datasource__lb"],
            "outputs": ["sib_a"],
            "params": {},
        },
    )
    first = validate_sans_ir(doc)
    second = validate_sans_ir(doc)
    assert first == second

    steps = doc["steps"]
    warnings1 = validate_sans_ir_structure(doc["datasources"], steps, strict=False)
    warnings2 = validate_sans_ir_structure(doc["datasources"], steps, strict=False)
    assert warnings1 == warnings2
