import pytest
import textwrap

from sans.compiler import check_script
from sans.ir import IRDoc, OpStep, UnknownBlockStep
from sans.expr import lit, col, binop, boolop, unop, call
from sans._loc import Loc

# Helper to create a Loc object for testing
def L(file, start, end):
    return Loc(file, start, end)

class TestDataStepCompiler:

    # --- Happy Path Data Step Compilation ---

    def test_basic_data_set_run(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 1
        step = irdoc.steps[0]
        assert isinstance(step, OpStep)
        assert step.op == "identity"
        assert step.inputs == ["in_table"]
        assert step.outputs == ["out_table"]
        assert step.params == {}
        assert step.loc == L("test.sas", 2, 4)

    def test_data_step_with_keep(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              keep col1 col2;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 1
        
        select_step = irdoc.steps[0]
        assert isinstance(select_step, OpStep)
        assert select_step.op == "select"
        assert select_step.inputs == ["in_table"]
        assert select_step.outputs == ["out_table"]
        assert select_step.params == {"cols": ["col1", "col2"]}
        assert select_step.loc == L("test.sas", 2, 5)

    def test_data_step_with_drop(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              drop colA colB;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 1
        
        select_step = irdoc.steps[0]
        assert isinstance(select_step, OpStep)
        assert select_step.op == "select"
        assert select_step.inputs == ["in_table"]
        assert select_step.outputs == ["out_table"]
        assert select_step.params == {"drop": ["colA", "colB"]}
        assert select_step.loc == L("test.sas", 2, 5)

    def test_data_step_with_rename(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              rename old1=new1 old2=new2;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 1
        
        rename_step = irdoc.steps[0]
        assert isinstance(rename_step, OpStep)
        assert rename_step.op == "rename"
        assert rename_step.inputs == ["in_table"]
        assert rename_step.outputs == ["out_table"]
        assert rename_step.params == {"mapping": [{"from": "old1", "to": "new1"}, {"from": "old2", "to": "new2"}]}
        assert rename_step.loc == L("test.sas", 2, 5)

    def test_data_step_with_assignments(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              x = a + b;
              y = 10 * c;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 1
        
        compute_step = irdoc.steps[0]
        assert isinstance(compute_step, OpStep)
        assert compute_step.op == "compute"
        assert compute_step.inputs == ["in_table"]
        assert compute_step.outputs == ["out_table"]
        
        expected_assignments = [
            {"col": "x", "expr": binop("+", col("a"), col("b"))},
            {"col": "y", "expr": binop("*", lit(10), col("c"))},
        ]
        # Compare assign params without relying on exact dict equality for complex objects
        assert len(compute_step.params["assign"]) == len(expected_assignments)
        for i, assignment in enumerate(compute_step.params["assign"]):
            assert assignment["col"] == expected_assignments[i]["col"]
            # Deep comparison of expression nodes
            assert assignment["expr"] == expected_assignments[i]["expr"]
        
        # Loc should span the entire data step block
        assert compute_step.loc == L("test.sas", 2, 6)

    def test_data_step_with_filter(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              if a > 10;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 1
        
        filter_step = irdoc.steps[0]
        assert isinstance(filter_step, OpStep)
        assert filter_step.op == "filter"
        assert filter_step.inputs == ["in_table"]
        assert filter_step.outputs == ["out_table"]
        assert filter_step.params["predicate"] == binop(">", col("a"), lit(10))
        assert filter_step.loc == L("test.sas", 2, 5)

    def test_data_step_with_dataset_options(self):
        script = textwrap.dedent("""
            data out;
              set in(where=(a > 0) keep=a b rename=(a=x));
              y = x + 1;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 4

        assert irdoc.steps[0].op == "filter"
        assert irdoc.steps[1].op == "select"
        assert irdoc.steps[2].op == "rename"
        assert irdoc.steps[3].op == "compute"
        assert irdoc.steps[3].outputs == ["out"]

    def test_data_step_unknown_dataset_option_refused(self):
        script = textwrap.dedent("""
            data out;
              set in(foo=bar);
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_DATASET_OPTION_UNKNOWN"

    def test_data_step_dataset_option_case_normalized(self):
        script = textwrap.dedent("""
            data out;
              set IN(where=(A > 0));
              keep a;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert irdoc.steps[0].inputs == ["in"]

    def test_data_step_all_operations_canonical_order(self):
        script = textwrap.dedent("""
            data final_out;
              set initial_in;
              keep colX colY;
              rename colA=colB;
              new_col = colX + 5;
              if new_col > 100 or colY ^= "test";
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"initial_in"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 4  # rename, compute, filter, select

        # 1. Rename
        rename_step = irdoc.steps[0]
        assert rename_step.op == "rename"
        assert rename_step.inputs == ["initial_in"]
        assert rename_step.outputs[0].startswith("final_out__") # temp output
        assert rename_step.loc == L("test.sas", 2, 8)
        temp1 = rename_step.outputs[0]

        # 2. Compute
        compute_step = irdoc.steps[1]
        assert compute_step.op == "compute"
        assert compute_step.inputs == [temp1]
        assert compute_step.outputs[0].startswith("final_out__") # temp output
        assert compute_step.params["assign"][0]["col"] == "new_col"
        assert compute_step.params["assign"][0]["expr"] == binop("+", col("colX"), lit(5))
        assert compute_step.loc == L("test.sas", 2, 8)
        temp2 = compute_step.outputs[0]

        # 3. Filter
        filter_step = irdoc.steps[2]
        assert filter_step.op == "filter"
        assert filter_step.inputs == [temp2]
        assert filter_step.outputs[0].startswith("final_out__")
        assert filter_step.params["predicate"] == boolop(
            "or",
            [
                binop(">", col("new_col"), lit(100)),
                binop("!=", col("colY"), lit("test")),
            ],
        )
        assert filter_step.loc == L("test.sas", 2, 8)

        # 4. Select
        select_step = irdoc.steps[3]
        assert select_step.op == "select"
        assert select_step.inputs == [filter_step.outputs[0]]
        assert select_step.outputs == ["final_out"]  # Final output
        assert select_step.params == {"cols": ["colX", "colY"]}
        assert select_step.loc == L("test.sas", 2, 8)

    # --- Forbidden Token Checks ---
    @pytest.mark.parametrize("input_script_part, detected_token, line_num", [
        ("lag(col1);", "lag", 4), ("array arr[5];", "array", 4),
        ("call symput('a', 'b');", "call", 4), ("infile 'file.txt';", "infile", 4),
        ("input var1 var2;", "input", 4), ("proc print;", "proc", 4), ("%macro;", "%", 4)
    ])
    def test_forbidden_token_in_data_step(self, input_script_part, detected_token, line_num):
        script = textwrap.dedent(f"""
            data out_table;
              set in_table;
              {input_script_part}
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        
        if detected_token == "proc":
            assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_PROC"
            assert "Unsupported PROC statement" in exc_info.value.message
        else:
            assert exc_info.value.code == "SANS_BLOCK_STATEFUL_TOKEN"
            assert f"Forbidden token '{detected_token}' detected in data step" in exc_info.value.message
            assert exc_info.value.loc == L("test.sas", 2, 5)

    # --- Expression Parsing Precedence ---
    def test_expression_precedence_multiplication_addition(self):
        script = textwrap.dedent("""
            data out;
              set in;
              x = a + b * 2;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        compute_step = irdoc.steps[0]
        expected_expr = binop("+", col("a"), binop("*", col("b"), lit(2)))
        assert compute_step.params["assign"][0]["expr"] == expected_expr

    def test_expression_precedence_logical_and_or(self):
        script = textwrap.dedent("""
            data out;
              set in;
              if a > 1 and b < 2 or c = 5;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        filter_step = irdoc.steps[0]
        # Expected: (a > 1 and b < 2) or c = 5
        expected_predicate = boolop(
            "or",
            [
                boolop(
                    "and",
                    [
                        binop(">", col("a"), lit(1)),
                        binop("<", col("b"), lit(2)),
                    ],
                ),
                binop("==", col("c"), lit(5)),
            ],
        )
        assert filter_step.params["predicate"] == expected_predicate

    def test_expression_parentheses(self):
        script = textwrap.dedent("""
            data out;
              set in;
              x = (a + b) * c;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        compute_step = irdoc.steps[0]
        expected_expr = binop("*", binop("+", col("a"), col("b")), col("c"))
        assert compute_step.params["assign"][0]["expr"] == expected_expr

    def test_expression_functions(self):
        script = textwrap.dedent("""
            data out;
              set in;
              x = coalesce(a, b);
              y = if(z > 0, 1, 0);
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        compute_step = irdoc.steps[0]
        
        expected_assigns = [
            {"col": "x", "expr": call("coalesce", [col("a"), col("b")])},
            {"col": "y", "expr": call("if", [
                binop(">", col("z"), lit(0)),
                lit(1),
                lit(0),
            ])},
        ]
        assert len(compute_step.params["assign"]) == len(expected_assigns)
        for i, assignment in enumerate(compute_step.params["assign"]):
            assert assignment["col"] == expected_assigns[i]["col"]
            assert assignment["expr"] == expected_assigns[i]["expr"]
    
    # --- Malformed Statements ---
    def test_malformed_set_statement(self):
        script = textwrap.dedent("""
            data out;
              set ;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_SET_STATEMENT_MALFORMED"

    def test_malformed_rename_statement(self):
        script = textwrap.dedent("""
            data out;
              set in;
              rename a b;  # Missing '='
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_RENAME_MALFORMED"

    # --- Multiple of same statement ---
    def test_multiple_keep_statements_refused(self):
        script = textwrap.dedent("""
            data out;
              set in;
              keep a;
              keep b;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_DATASTEP_FORM"
        assert "at most one KEEP or DROP statement" in exc_info.value.message
    
    def test_keep_and_drop_statements_refused(self):
        script = textwrap.dedent("""
            data out;
              set in;
              keep a;
              drop b;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_DATASTEP_FORM"
        assert "at most one KEEP or DROP statement" in exc_info.value.message

    def test_multiple_if_statements_refused(self):
        script = textwrap.dedent("""
            data out;
              set in;
              if a > 0;
              if b < 10;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_DATASTEP_FORM"
        assert "at most one IF statement for filtering" in exc_info.value.message

    def test_unsupported_statement_in_data_step_body(self):
        script = textwrap.dedent("""
            data out;
              set in;
              unknown_statement;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_UNSUPPORTED_DATASTEP_FORM"
        assert "Unsupported statement or unparsed content in data step: 'unknown_statement'" in exc_info.value.message
        assert exc_info.value.loc == L("test.sas", 2, 5)

    def test_merge_without_by_refused(self):
        script = textwrap.dedent("""
            data out;
              merge a b;
              if 1 = 1 then output;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"a", "b"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_DATASTEP_MISSING_BY"
        assert "requires a BY statement" in exc_info.value.message

    # --- Proc Sort Tests ---
    def test_proc_sort_unsupported_option(self):
        script = textwrap.dedent("""
            proc sort data=in out=out dupout=dups;
              by col1;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_SORT_UNSUPPORTED_OPTION"
        assert "Unsupported options in PROC SORT header: dupout=dups" in exc_info.value.message
        assert exc_info.value.loc == L("test.sas", 2, 2)

    def test_proc_sort_missing_by(self):
        script = textwrap.dedent("""
            proc sort data=in out=out;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_SORT_MISSING_BY"
        assert "PROC SORT requires exactly one BY statement." in exc_info.value.message
    
    def test_proc_sort_with_extra_body_statement(self):
        script = textwrap.dedent("""
            proc sort data=in out=out;
              by col1;
              where x > 0;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_PARSE_SORT_UNSUPPORTED_BODY_STATEMENT"
        assert "PROC SORT contains unsupported statements in its body." in exc_info.value.message

    def test_proc_sort_data_step_chain_validates(self):
        script = textwrap.dedent("""
            data temp;
              set raw;
              x = a + 1;
            run;
            proc sort data=temp out=sorted_temp;
              by x;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"raw"}, legacy_sas=True)
        assert isinstance(irdoc, IRDoc)
        assert len(irdoc.steps) == 2

        data_step = irdoc.steps[0]
        assert data_step.op == "compute"
        assert data_step.outputs == ["temp"]

        sort_step = irdoc.steps[1]
        assert sort_step.op == "sort"
        assert sort_step.inputs == ["temp"]
        assert sort_step.outputs == ["sorted_temp"]
        assert sort_step.params == {"by": [{"col": "x", "desc": False}]}
        
class TestSortednessFacts:

    def test_proc_sort_sets_sorted_by(self):
        script = textwrap.dedent("""
            proc sort data=in_table out=sorted_table;
              by col1 col2;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert irdoc.table_facts["sorted_table"].sorted_by == ["col1", "col2"]
        assert irdoc.steps[0].loc == L("test.sas", 2, 4)  # Loc of the sort step

    def test_identity_preserves_sorted_by(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
            run;
        """)
        # Simulate in_table being sorted,
        irdoc = check_script(script, "test.sas", tables={"in_table"}, initial_table_facts={"in_table": {"sorted_by": ["key"]}}, legacy_sas=True)
        assert irdoc.table_facts["out_table"].sorted_by == ["key"]
        assert irdoc.steps[0].loc == L("test.sas", 2, 4)

    def test_select_preserves_sorted_by(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              keep id date colA;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, initial_table_facts={"in_table": {"sorted_by": ["id", "date"]}}, legacy_sas=True)
        assert irdoc.table_facts["out_table"].sorted_by == ["id", "date"]
        assert irdoc.steps[0].loc == L("test.sas", 2, 5)

    def test_filter_preserves_sorted_by(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              if value > 10;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, initial_table_facts={"in_table": {"sorted_by": ["id"]}}, legacy_sas=True)
        assert irdoc.table_facts["out_table"].sorted_by == ["id"]
        assert irdoc.steps[0].loc == L("test.sas", 2, 5)

    def test_compute_preserves_sorted_by(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              new_col = old_col * 2;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, initial_table_facts={"in_table": {"sorted_by": ["category"]}}, legacy_sas=True)
        assert irdoc.table_facts["out_table"].sorted_by == ["category"]
        assert irdoc.steps[0].loc == L("test.sas", 2, 5)

    def test_rename_drops_sorted_by(self):
        script = textwrap.dedent("""
            data out_table;
              set in_table;
              rename old_id=new_id;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"in_table"}, initial_table_facts={"in_table": {"sorted_by": ["old_id"]}}, legacy_sas=True)
        assert irdoc.table_facts["out_table"].sorted_by is None
        assert irdoc.steps[0].loc == L("test.sas", 2, 5)

    def test_chain_preserves_and_drops_sortedness(self):
        script = textwrap.dedent("""
            proc sort data=raw out=temp_sorted;
              by order_id;
            run;
            data temp_filtered;
              set temp_sorted;
              if amount > 100;
            run;
            data final_renamed;
              set temp_filtered;
              rename old_amt=new_amt;
            run;
        """)
        irdoc = check_script(script, "test.sas", tables={"raw"}, legacy_sas=True)
        assert irdoc.table_facts["temp_sorted"].sorted_by == ["order_id"]
        assert irdoc.table_facts["temp_filtered"].sorted_by == ["order_id"]  # Preserved through filter
        assert irdoc.table_facts["final_renamed"].sorted_by is None # Dropped by rename

    def test_proc_sort_validation_missing_by(self):
        irdoc = IRDoc(
            steps=[
                OpStep(
                    op="sort",
                    inputs=["in"],
                    outputs=["out"],
                    params={},  # Missing "by" on purpose to exercise validator rule
                    loc=L("test.sas", 2, 3),
                )
            ],
            tables={"in"},
        )
        with pytest.raises(UnknownBlockStep) as exc_info:
            irdoc.validate()
        assert exc_info.value.code == "SANS_VALIDATE_SORT_MISSING_BY"
        assert "PROC SORT operation requires 'by' variables." in exc_info.value.message
        assert exc_info.value.loc == L("test.sas", 2, 3)

    def test_data_step_requires_sorted_input_for_by(self):
        script = textwrap.dedent("""
            data out;
              set in_table;
              by id;
              if first.id then output;
            run;
        """)
        with pytest.raises(UnknownBlockStep) as exc_info:
            check_script(script, "test.sas", tables={"in_table"}, legacy_sas=True)
        assert exc_info.value.code == "SANS_VALIDATE_ORDER_REQUIRED"
        assert "must be sorted by ['id']" in exc_info.value.message
        assert exc_info.value.loc == L("test.sas", 2, 6)
