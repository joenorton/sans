import pytest
import textwrap

from sans.compiler import check_script
from sans.ir import IRDoc, OpStep, UnknownBlockStep
from sans._loc import Loc

def test_check_script_happy_path():
    script = (
        "data mydata;\n"
        "    set otherdata;\n"
        "run;\n"
        "proc sort data=mydata out=sorted;\n"
        "    by somevar;\n"
        "run;\n"
        "title \"hello\";"
    ) # This other statement should cause a refusal

    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", tables={"otherdata"}, legacy_sas=True)

    # The expected refusal is for the "title" statement, as it's an "other" block
    assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_STATEMENT"
    assert "Unsupported top-level statement: 'title \"hello\"'" in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 7, 7) # Loc of the title statement

def test_check_script_refuses_proc_sql():
    script = "proc sql;\n  create table out as select * from (select * from dm);\nquit;"
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", legacy_sas=True)
    assert exc_info.value.code == "SANS_PARSE_SQL_UNSUPPORTED_FORM"
    assert "Unsupported" in exc_info.value.message or "unsupported" in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 2, 2)

def test_check_script_refuses_table_undefined():
    script = "data a;\n  set b;\nrun;"
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", legacy_sas=True)
    assert exc_info.value.code == "SANS_VALIDATE_TABLE_UNDEFINED"
    assert exc_info.value.loc == Loc("test.sas", 1, 3)

def test_check_script_refuses_output_table_collision():
    script = ("data input_x;\n  set initial_source;\nrun;\n" # Defines input_x
              "data input_y;\n  set initial_source;\nrun;\n" # Defines input_y
              "data output_a;\n  set input_x;\nrun;\n"        # Defines output_a (first time)
              "data output_a;\n  set input_y;\nrun;")        # Collides on output_a
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", tables={"initial_source"}, legacy_sas=True)
    assert exc_info.value.code == "SANS_VALIDATE_OUTPUT_TABLE_COLLISION"
    assert "Output table 'output_a' produced by operation 'identity' already exists." in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 10, 12) # Loc of the colliding step

def test_check_script_refuses_table_undefined_for_complex_chain():
    script = ("data x;\n  set input_source;\nrun;\n" # Defines x using input_source
              "data a;\n  set x;\nrun;\n"           # Defines a using x
              "data a;\n  set x;\nrun;")           # Collides on a, but input_source is undefined
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", legacy_sas=True)
    assert exc_info.value.code == "SANS_VALIDATE_TABLE_UNDEFINED"
    assert "Input table 'input_source' used by operation 'identity' is not defined." in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 1, 3) # Loc of the step that uses input_source
def test_check_script_refuses_unsupported_proc():
    script = "proc other;\nrun;"
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", legacy_sas=True)
    assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_PROC"
    assert "Unsupported PROC statement: 'proc other'" in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 1, 1)

def test_check_script_refuses_unsupported_other_statement():
    script = "title 'hello';"
    with pytest.raises(UnknownBlockStep) as exc_info:
        check_script(script, "test.sas", legacy_sas=True)
    assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_STATEMENT"
    assert "Unsupported top-level statement: 'title 'hello''" in exc_info.value.message
    assert exc_info.value.loc == Loc("test.sas", 1, 1)

def test_check_script_happy_path_with_multiple_steps():
    script = (
        "data temp_data;\n"
        "    set raw_data;\n"
        "run;\n"
        "proc sort data=temp_data out=final_data;\n"
        "    by id;\n"
        "run;"
    )
    
    # raw_data is now declared as an existing table
    irdoc = check_script(script, "test.sas", tables={"raw_data"}, legacy_sas=True)
    
    assert isinstance(irdoc, IRDoc)
    assert len(irdoc.steps) == 2
    
    assert isinstance(irdoc.steps[0], OpStep)
    assert irdoc.steps[0].op == "identity"
    assert irdoc.steps[0].inputs == ["raw_data"]
    assert irdoc.steps[0].outputs == ["temp_data"]
    assert irdoc.steps[0].params == {}
    assert irdoc.steps[0].loc == Loc("test.sas", 1, 3)

    assert isinstance(irdoc.steps[1], OpStep)
    assert irdoc.steps[1].op == "sort"
    assert irdoc.steps[1].inputs == ["temp_data"]
    assert irdoc.steps[1].outputs == ["final_data"]
    assert irdoc.steps[1].params == {"by": [{"col": "id", "desc": False}]}
    assert irdoc.steps[1].loc == Loc("test.sas", 4, 6)
