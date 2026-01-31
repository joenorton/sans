import pytest
import textwrap

from sans.frontend import Statement, Block, split_statements, segment_blocks
from sans.ir import OpStep, UnknownBlockStep
from sans.recognizer import recognize_data_block, recognize_proc_sort_block, recognize_proc_transpose_block
from sans._loc import Loc

# Helper function to create a block.
# This assumes segment_blocks correctly extracts a single data block.
def create_data_block(script: str, file_name: str = "test.sas") -> Block:
    statements = list(split_statements(script, file_name))
    blocks = list(filter(lambda b: b.kind == "data", segment_blocks(statements)))
    assert len(blocks) == 1, f"Expected 1 data block, got {len(blocks)} for script:\n{script}"
    return blocks[0]

def create_proc_block(script: str, file_name: str = "test.sas") -> Block:
    statements = list(split_statements(script, file_name))
    blocks = list(filter(lambda b: b.kind == "proc", segment_blocks(statements)))
    assert len(blocks) == 1, f"Expected 1 proc block, got {len(blocks)} for script:\n{script}"
    return blocks[0]

def test_recognize_data_block_happy_path():
    script = "data output_table;\nset input_table;\nrun;"
    block = create_data_block(script)

    steps = recognize_data_block(block)
    assert len(steps) == 1
    step = steps[0]

    assert isinstance(step, OpStep)
    assert step.op == "identity" # Expect 'identity' for simple data/set/run
    assert step.inputs == ["input_table"]
    assert step.outputs == ["output_table"]
    assert step.params == {}
    assert step.loc == Loc("test.sas", 1, 3)

def test_recognize_data_block_missing_set():
    script = "data output_table;\nrun;"
    block = create_data_block(script)

    steps = recognize_data_block(block)
    assert len(steps) == 1
    step = steps[0]

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_UNSUPPORTED_DATASTEP_FORM"
    assert "Data step must contain exactly one SET statement." in step.message # Updated message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 2)

def test_recognize_data_block_multiple_set():
    script = "data output_table;\nset input_table_1;\nset input_table_2;\nrun;"
    block = create_data_block(script)

    steps = recognize_data_block(block)
    assert len(steps) == 1
    step = steps[0]

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_UNSUPPORTED_DATASTEP_FORM"
    assert "Data step must contain exactly one SET statement." in step.message # Updated message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 4)

def test_recognize_data_block_other_statements_inside():
    script = "data output_table;\nset input_table;\nx=1;\nrun;"
    block = create_data_block(script)

    steps = recognize_data_block(block)
    assert len(steps) == 1
    step = steps[0]

    assert isinstance(step, OpStep)
    assert step.op == "compute"
    assert step.inputs == ["input_table"]
    assert step.outputs == ["output_table"]
    assert step.loc == Loc("test.sas", 1, 4)

# Removed test_recognize_data_block_malformed_data_header as it's not a valid test
# for recognize_data_block which expects a 'data' block.

def test_recognize_data_block_malformed_set_statement():
    script = "data output_table;\nset;\nrun;"
    block = create_data_block(script)

    steps = recognize_data_block(block)
    assert len(steps) == 1
    step = steps[0]

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SET_STATEMENT_MALFORMED"
    assert "Malformed SET statement" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 2, 2)

def test_recognize_proc_sort_block_happy_path():
    script = "proc sort data=input_table out=output_table;\nby var1 var2;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, OpStep)
    assert step.op == "sort"
    assert step.inputs == ["input_table"]
    assert step.outputs == ["output_table"]
    assert step.params == {"by": [{"col": "var1", "asc": True}, {"col": "var2", "asc": True}]}
    assert step.loc == Loc("test.sas", 1, 3)

def test_recognize_proc_sort_block_missing_by():
    script = "proc sort data=input_table out=output_table;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SORT_MISSING_BY"
    assert "exactly one BY statement" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 2)

def test_recognize_proc_sort_block_multiple_by():
    script = "proc sort data=input_table out=output_table;\nby var1;\nby var2;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SORT_MISSING_BY"
    assert "exactly one BY statement" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 4)

def test_recognize_proc_sort_block_unsupported_option_header():
    script = "proc sort data=input_table out=output_table nodupkey;\nby var1;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SORT_UNSUPPORTED_OPTION"
    assert "Unsupported options in PROC SORT header" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 1)

def test_recognize_proc_sort_block_missing_data_option():
    script = "proc sort out=output_table;\nby var1;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SORT_MISSING_DATA"
    assert "requires a DATA= option" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 1)

def test_recognize_proc_sort_block_missing_out_option():
    script = "proc sort data=input_table;\nby var1;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SORT_MISSING_OUT"
    assert "requires an OUT= option" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 1)

def test_recognize_proc_sort_block_unsupported_body_statement():
    script = "proc sort data=input_table out=output_table;\nby var1;\nx=1;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SORT_UNSUPPORTED_BODY_STATEMENT"
    assert "unsupported statements in its body" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 1, 4)

def test_recognize_proc_sort_block_malformed_by_statement():
    script = "proc sort data=input_table out=output_table;\nby;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_sort_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_SORT_BY_MALFORMED"
    assert "Malformed BY statement" in step.message
    assert step.severity == "fatal"
    assert step.loc == Loc("test.sas", 2, 2)


def test_recognize_proc_transpose_block_happy_path():
    script = "proc transpose data=lb out=lb_t;\nby subjid;\nid lbtestcd;\nvar lbstresn;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_transpose_block(block)

    assert isinstance(step, OpStep)
    assert step.op == "transpose"
    assert step.inputs == ["lb"]
    assert step.outputs == ["lb_t"]
    assert step.params == {"by": ["subjid"], "id": "lbtestcd", "var": "lbstresn", "last_wins": True}


def test_recognize_proc_transpose_block_missing_var():
    script = "proc transpose data=lb out=lb_t;\nby subjid;\nid lbtestcd;\nrun;"
    block = create_proc_block(script)

    step = recognize_proc_transpose_block(block)

    assert isinstance(step, UnknownBlockStep)
    assert step.code == "SANS_PARSE_TRANSPOSE_MISSING_VAR"
