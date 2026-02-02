import pytest
from dataclasses import asdict
from sans.ir import IRDoc, OpStep, UnknownBlockStep, TableFact
from sans._loc import Loc

def test_irdoc_serialization():
    # Create some dummy IR steps
    op_step = OpStep(
        op="example_op",
        inputs=["in_table"],
        outputs=["out_table"],
        params={"key": "value"},
        loc=Loc("test.sas", 1, 5)
    )
    unknown_step = UnknownBlockStep(
        code="SANS_TEST_UNKNOWN",
        message="Unknown block type encountered",
        loc=Loc("test.sas", 6, 6)
    )

    # Create an IRDoc
    irdoc = IRDoc(steps=[op_step, unknown_step])

    # Serialize to dictionary
    serialized_irdoc = asdict(irdoc)

    # Define expected structure
    expected_irdoc = {
        "steps": [
            {
                "kind": "op",
                "op": "example_op",
                "inputs": ["in_table"],
                "outputs": ["out_table"],
                "params": {"key": "value"},
                "loc": {"file": "test.sas", "line_start": 1, "line_end": 5},
            },
            {
                "kind": "block",
                "code": "SANS_TEST_UNKNOWN",
                "message": "Unknown block type encountered",
                "severity": "fatal",
                "loc": {"file": "test.sas", "line_start": 6, "line_end": 6},
            },
        ],
        "tables": set(),
        "table_facts": {},
        "datasources": {},
    }

    # Assert that the serialized IRDoc matches the expected structure
    assert serialized_irdoc == expected_irdoc

def test_irdoc_validate_happy_path():
    # data a; set x; run; (x is defined by previous step)
    # data b; set a; run;
    step1 = OpStep(op="create_x", inputs=[], outputs=["x"], loc=Loc("test.sas", 1, 1))
    step2 = OpStep(op="identity", inputs=["x"], outputs=["a"], loc=Loc("test.sas", 2, 4))
    step3 = OpStep(op="identity", inputs=["a"], outputs=["b"], loc=Loc("test.sas", 5, 7))
    irdoc = IRDoc(steps=[step1, step2, step3])
    # Should not raise any error
    validated_facts = irdoc.validate()
    assert "x" in validated_facts
    assert "a" in validated_facts
    assert "b" in validated_facts

def test_irdoc_validate_table_undefined_error():
    # data a; set b; run; (b is not defined)
    step1 = OpStep(op="identity", inputs=["b"], outputs=["a"], loc=Loc("test.sas", 1, 3))
    irdoc = IRDoc(steps=[step1])

    with pytest.raises(UnknownBlockStep) as exc_info:
        irdoc.validate()
    
    assert exc_info.value.code == "SANS_VALIDATE_TABLE_UNDEFINED"
    assert "Input table 'b' used by operation 'identity' is not defined." in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 1, 3)

def test_irdoc_validate_output_table_collision_error():
    # data a; set x; run;
    # data y_def; set z; run; (define y)
    # data a; set y_def; run; (a is redefined, no undefined inputs)
    step1 = OpStep(op="create_x", inputs=[], outputs=["x"], loc=Loc("test.sas", 1, 1))
    step2 = OpStep(op="create_y", inputs=[], outputs=["y"], loc=Loc("test.sas", 2, 2)) # Define 'y'
    step3 = OpStep(op="identity", inputs=["x"], outputs=["a"], loc=Loc("test.sas", 3, 5)) 
    step4 = OpStep(op="identity", inputs=["y"], outputs=["a"], loc=Loc("test.sas", 6, 8)) # Colliding output 'a'
    irdoc = IRDoc(steps=[step1, step2, step3, step4])

    with pytest.raises(UnknownBlockStep) as exc_info:
        irdoc.validate()
    
    assert exc_info.value.code == "SANS_VALIDATE_OUTPUT_TABLE_COLLISION"
    assert "Output table 'a' produced by operation 'identity' already exists." in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 6, 8) # Loc will be for the step that caused the collision
def test_irdoc_validate_reraise_unknown_block_step():
    # If IRDoc already contains an UnknownBlockStep, validate should re-raise it
    unknown_step = UnknownBlockStep(
        code="SANS_TEST_UNKNOWN_PRE_EXISTING",
        message="A pre-existing unknown block",
        loc=Loc("test.sas", 1, 1)
    )
    irdoc = IRDoc(steps=[unknown_step])

    with pytest.raises(UnknownBlockStep) as exc_info:
        irdoc.validate()
    
    assert exc_info.value.code == "SANS_TEST_UNKNOWN_PRE_EXISTING"
    assert "A pre-existing unknown block" in exc_info.value.message