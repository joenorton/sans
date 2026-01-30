# sans/tests/test_frontend.py
import pytest
import textwrap
from sans.frontend import Statement, split_statements, detect_refusal, Block, segment_blocks
from sans._loc import Loc

def test_split_simple_statements():
    script = "data one; set two; run; proc sort; by var; run;"
    statements = list(split_statements(script, "test.sas"))
    assert len(statements) == 6
    assert statements[0] == Statement("data one", Loc("test.sas", 1, 1))
    assert statements[1] == Statement("set two", Loc("test.sas", 1, 1))
    assert statements[2] == Statement("run", Loc("test.sas", 1, 1))
    assert statements[3] == Statement("proc sort", Loc("test.sas", 1, 1))
    assert statements[4] == Statement("by var", Loc("test.sas", 1, 1))
    assert statements[5] == Statement("run", Loc("test.sas", 1, 1))

def test_split_with_quotes_and_newlines():
    # This test was previously flawed. It now reflects correct syntax
    # where each assignment is its own statement.
    script = """
        data one;
            x = \"a;b;c\";
            y = 'd;e;f';
        run;
    """
    statements = list(split_statements(script, "test.sas"))
    assert len(statements) == 4
    assert statements[0] == Statement("data one", Loc("test.sas", 2, 2))
    assert statements[1] == Statement('x = \"a;b;c\"', Loc("test.sas", 3, 3))
    assert statements[2] == Statement("y = 'd;e;f'", Loc("test.sas", 4, 4))
    assert statements[3] == Statement("run", Loc("test.sas", 5, 5))


def test_multiline_statements():
    script = """
        proc sort data=two
            out=three;
            by descending x;
        run;
    """
    statements = list(split_statements(script, "test.sas"))
    assert len(statements) == 3
    # The buffer now joins with a space if a newline was there.
    assert statements[0].text == "proc sort data=two\n            out=three"
    assert statements[0].loc == Loc("test.sas", 2, 3)
    assert statements[1].text == "by descending x"
    assert statements[1].loc == Loc("test.sas", 4, 4)
    assert statements[2].text == "run"
    assert statements[2].loc == Loc("test.sas", 5, 5)

def test_comment_stripping():
    script = "/* block comment on line 1 */\ndata one; /* mid-line */\n* line comment;\nset two;\nrun;"
    statements = list(split_statements(script, "test.sas"))
    assert [s.text for s in statements] == ["data one", "set two", "run"]
    assert statements[0].loc == Loc("test.sas", 2, 2)
    assert statements[1].loc == Loc("test.sas", 4, 4)
    assert statements[2].loc == Loc("test.sas", 5, 5)

def test_empty_statements():
    script = "data one;; set two; ; run;"
    statements = list(split_statements(script, "test.sas"))
    assert len(statements) == 3
    assert [s.text for s in statements] == ["data one", "set two", "run"]

def test_no_trailing_semicolon():
    script = "data one; set two"
    statements = list(split_statements(script, "test.sas"))
    assert len(statements) == 2
    assert statements[1].text == "set two"
    assert statements[1].loc.line_start == 1

def test_mixed_quotes_and_comments():
    script = textwrap.dedent("""\
        proc sql;
            create table example as
            select name,
                   "species; /* not a comment */" as type
            from pets;
        quit;
    """)
    statements = list(split_statements(script, "test.sas"))
    assert len(statements) == 3
    assert statements[0].text == "proc sql"
    assert "/* not a comment */" in statements[1].text
    assert statements[2].text == "quit"

def test_refuses_proc_sql():
    script = "proc sql;\n  select * from dm;\nquit;"
    refusal = detect_refusal(script, "test.sas")
    assert refusal is not None
    assert refusal.code == "SANS_PARSE_SQL_DETECTED"
    assert refusal.loc == Loc("test.sas", 1, 1)

def test_segment_data_and_proc_blocks():
    script = ("data mydata;\n"
              "    set otherdata;\n"
              "run;\n"
              "proc mysort data=mydata out=sorted;\n"
              "    by somevar;\n"
              "run;\n"
              "title \"hello\";")
    statements = list(split_statements(script, "test.sas"))
    assert all(";" not in s.text for s in statements)
    blocks = segment_blocks(statements)

    assert len(blocks) == 3

    # Data block
    assert blocks[0].kind == "data"
    assert blocks[0].header.text == "data mydata"
    assert len(blocks[0].body) == 1
    assert blocks[0].body[0].text == "set otherdata"
    assert blocks[0].end.text == "run"
    assert blocks[0].loc_span == Loc("test.sas", 1, 3)

    # Proc block
    assert blocks[1].kind == "proc"
    assert blocks[1].header.text == "proc mysort data=mydata out=sorted"
    assert len(blocks[1].body) == 1
    assert blocks[1].body[0].text == "by somevar"
    assert blocks[1].end.text == "run"
    assert blocks[1].loc_span == Loc("test.sas", 4, 6)

    # Other block
    assert blocks[2].kind == "other"
    assert blocks[2].header.text == 'title "hello"'
    assert len(blocks[2].body) == 0
    assert blocks[2].end is None
    assert blocks[2].loc_span == Loc("test.sas", 7, 7)

def test_segment_block_without_run():
    script = ("data mydata;\n"
              "    set otherdata;\n"
              "proc sort;\n"
              "    by x;")
    statements = list(split_statements(script, "test.sas"))
    blocks = segment_blocks(statements)

    assert len(blocks) == 2

    # Data block (ends at end of script since no run;)
    assert blocks[0].kind == "data"
    assert blocks[0].header.text == "data mydata"
    assert len(blocks[0].body) == 1
    assert blocks[0].body[0].text == "set otherdata"
    assert blocks[0].end is None
    assert blocks[0].loc_span == Loc("test.sas", 1, 2)

    # Proc block (ends at end of script since no run;)
    assert blocks[1].kind == "proc"
    assert blocks[1].header.text == "proc sort"
    assert len(blocks[1].body) == 1
    assert blocks[1].body[0].text == "by x"
    assert blocks[1].end is None
    assert blocks[1].loc_span == Loc("test.sas", 3, 4)

def test_segment_single_other_statement():
    script = "libname mylib 'path';"
    statements = list(split_statements(script, "test.sas"))
    blocks = segment_blocks(statements)

    assert len(blocks) == 1
    assert blocks[0].kind == "other"
    assert blocks[0].header.text == "libname mylib 'path'"
    assert len(blocks[0].body) == 0
    assert blocks[0].end is None
    assert blocks[0].loc_span == Loc("test.sas", 1, 1)

def test_segment_empty_script():
    script = ""
    statements = list(split_statements(script, "test.sas"))
    blocks = segment_blocks(statements)
    assert len(blocks) == 0

def test_segment_only_run_statement():
    script = "run;"
    statements = list(split_statements(script, "test.sas"))
    blocks = segment_blocks(statements)
    assert len(blocks) == 1
    assert blocks[0].kind == "other"
    assert blocks[0].header.text == "run"
    assert blocks[0].loc_span == Loc("test.sas", 1, 1)
