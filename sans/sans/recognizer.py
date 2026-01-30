from __future__ import annotations
import re
from typing import Optional, Any

from .frontend import Block, Statement
from .ir import OpStep, UnknownBlockStep, Step # Import Step explicitly
from ._loc import Loc
from .parser_expr import parse_expression_from_string # New import


def recognize_data_block(block: Block) -> list[OpStep | UnknownBlockStep]:
    steps: list[OpStep | UnknownBlockStep] = []

    if not block.header.text.lower().startswith("data "):
        return [UnknownBlockStep(
            code="SANS_PARSE_INVALID_DATA_BLOCK_HEADER",
            message=f"Expected data block to start with 'data', got '{block.header.text}'",
            loc=block.header.loc,
        )]

    # 1. Parse header: data <out>;
    header_match = re.match(r"data\s+(\S+)", block.header.text.lower())
    if not header_match:
        return [UnknownBlockStep(
            code="SANS_PARSE_DATA_HEADER_MALFORMED",
            message=f"Malformed data block header: '{block.header.text}'",
            loc=block.header.loc,
        )]
    final_output_table = header_match.group(1)

    # 2. Parse set statement: set <in>;
    set_statements = [s for s in block.body if s.text.lower().startswith("set")]
    if len(set_statements) != 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step must contain exactly one SET statement.",
            loc=block.loc_span,
        )]
    set_stmt = set_statements[0]
    set_match = re.match(r"set\s+(\S+)", set_stmt.text.lower())
    if not set_match:
        return [UnknownBlockStep(
            code="SANS_PARSE_SET_STATEMENT_MALFORMED",
            message=f"Malformed SET statement: '{set_stmt.text}'",
            loc=set_stmt.loc,
        )]
    current_input_table = set_match.group(1)

    # Hard fail for forbidden tokens (statement-leading keywords only)
    def starts_with_token(stmt_text: str, token: str, extra_follow: str = "") -> bool:
        if not stmt_text.startswith(token):
            return False
        if len(stmt_text) == len(token):
            return True
        next_ch = stmt_text[len(token)]
        return next_ch.isspace() or next_ch in extra_follow

    def find_forbidden_token(stmt_text: str) -> Optional[str]:
        s = stmt_text.strip().lower()
        if not s:
            return None
        if s.startswith("%"):
            return "%"
        # Order matters for clearer error messages
        if starts_with_token(s, "proc"):
            return "proc"
        if starts_with_token(s, "do"):
            return "do"
        if starts_with_token(s, "end"):
            return "end"
        if starts_with_token(s, "retain"):
            return "retain"
        if starts_with_token(s, "lag", extra_follow="("):
            return "lag"
        if s.startswith("first."):
            return "first."
        if s.startswith("last."):
            return "last."
        if starts_with_token(s, "array"):
            return "array"
        if starts_with_token(s, "call"):
            return "call"
        if starts_with_token(s, "output"):
            return "output"
        if starts_with_token(s, "by"):
            return "by"
        if starts_with_token(s, "merge"):
            return "merge"
        if starts_with_token(s, "infile"):
            return "infile"
        if starts_with_token(s, "input"):
            return "input"
        return None

    for stmt in block.body:
        token = find_forbidden_token(stmt.text)
        if token:
            return [UnknownBlockStep(
                code="SANS_BLOCK_STATEFUL_TOKEN",
                message=f"Forbidden token '{token}' detected in data step: '{stmt.text}'",
                loc=block.loc_span,
            )]

    # Temporary outputs for the pipeline
    temp_idx = 0
    def generate_temp_output():
        nonlocal temp_idx
        temp_idx += 1
        return f"{final_output_table}__{temp_idx}"

    # Initialize pipeline with the input table from SET statement
    pipeline_input_table = current_input_table
    
    # Process body statements in canonical order: select, rename, compute, filter
    
    # ----------------------------------------------------------------------
    # 1. Select (keep/drop)
    # ----------------------------------------------------------------------
    select_statements = [s for s in block.body if s.text.lower().startswith("keep ") or s.text.lower().startswith("drop ")]
    if len(select_statements) > 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step can have at most one KEEP or DROP statement.",
            loc=block.loc_span,
        )]
    if select_statements:
        select_stmt = select_statements[0]
        select_params = {"keep": [], "drop": []}
        if select_stmt.text.lower().startswith("keep "):
            cols_str = select_stmt.text[len("keep "):].strip()
            select_params["keep"] = re.split(r'\s+', cols_str)
        else: # drop
            cols_str = select_stmt.text[len("drop "):].strip()
            select_params["drop"] = re.split(r'\s+', cols_str)
        
        pipeline_output_table = generate_temp_output()
        steps.append(OpStep(
            op="select",
            inputs=[pipeline_input_table],
            outputs=[pipeline_output_table],
            params=select_params,
            loc=block.loc_span,
        ))
        pipeline_input_table = pipeline_output_table

    # ----------------------------------------------------------------------
    # 2. Rename
    # ----------------------------------------------------------------------
    rename_statements = [s for s in block.body if s.text.lower().startswith("rename ")]
    if len(rename_statements) > 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step can have at most one RENAME statement.",
            loc=block.loc_span,
        )]
    if rename_statements:
        rename_stmt = rename_statements[0]
        map_str = rename_stmt.text[len("rename "):].strip()
        rename_map = {}
        for pair in re.split(r'\s+', map_str):
            parts = pair.split("=")
            if len(parts) != 2:
                return [UnknownBlockStep(
                    code="SANS_PARSE_RENAME_MALFORMED",
                    message=f"Malformed RENAME pair: '{pair}' in '{rename_stmt.text}'",
                    loc=rename_stmt.loc,
                )]
            rename_map[parts[0]] = parts[1]
        
        pipeline_output_table = generate_temp_output()
        steps.append(OpStep(
            op="rename",
            inputs=[pipeline_input_table],
            outputs=[pipeline_output_table],
            params={"map": rename_map},
            loc=block.loc_span,
        ))
        pipeline_input_table = pipeline_output_table

    # ----------------------------------------------------------------------
    # 3. Compute (assignments)
    # ----------------------------------------------------------------------
    # Collect all assignments first to handle batch
    assignments = []
    # regex for x = <expr>;
    assignment_regex = re.compile(r"^([a-zA-Z_]\w*)\s*=\s*(.+)$") 
    assignment_statements: list[Statement] = []
    for stmt in block.body:
        assign_match = assignment_regex.match(stmt.text)
        if assign_match:
            assignment_statements.append(stmt)
            col_name = assign_match.group(1)
            expr_str = assign_match.group(2)
            try:
                expr_ast = parse_expression_from_string(expr_str, block.header.loc.file)
                assignments.append({"col": col_name, "expr": expr_ast})
            except ValueError as e:
                return [UnknownBlockStep(
                    code="SANS_PARSE_EXPRESSION_ERROR",
                    message=f"Error parsing assignment expression for '{col_name}': {e}",
                    loc=stmt.loc,
                )]
    if assignments:
        pipeline_output_table = generate_temp_output()
        steps.append(OpStep(
            op="compute",
            inputs=[pipeline_input_table],
            outputs=[pipeline_output_table],
            params={"assign": assignments},
            loc=block.loc_span,
        ))
        pipeline_input_table = pipeline_output_table
    
    # ----------------------------------------------------------------------
    # 4. Filter (if <predicate>)
    # ----------------------------------------------------------------------
    filter_statements = [s for s in block.body if s.text.lower().startswith("if ")]
    if len(filter_statements) > 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step can have at most one IF statement for filtering.",
            loc=block.loc_span,
        )]
    if filter_statements:
        filter_stmt = filter_statements[0]
        predicate_str = filter_stmt.text[len("if "):].strip()
        try:
            predicate_ast = parse_expression_from_string(predicate_str, block.header.loc.file)
        except ValueError as e:
            return [UnknownBlockStep(
                code="SANS_PARSE_EXPRESSION_ERROR",
                message=f"Error parsing IF predicate: {e}",
                loc=filter_stmt.loc,
            )]
        
        # The filter is the last step to produce the final output table
        steps.append(OpStep(
            op="filter",
            inputs=[pipeline_input_table],
            outputs=[final_output_table],
            params={"predicate": predicate_ast},
            loc=block.loc_span,
        ))
        # No more pipeline_input_table update, as this is the final output
    else:
        # If no filter, the last operation's output becomes the final_output_table
        # Or if no operations after set, then set's input is directly output
        if not steps: # Only 'set' and 'run', no intermediate ops were generated
             steps.append(OpStep(
                op="identity", # A pass-through op
                inputs=[pipeline_input_table],
                outputs=[final_output_table],
                loc=block.loc_span, # Loc of the entire data block
            ))
        else:
            # If there were intermediate steps, ensure the last one outputs to final_output_table
            # This is a bit hacky, but avoids creating an unnecessary identity step
            last_step = steps[-1]
            # Since OpStep is not frozen, we can modify its outputs directly
            if isinstance(last_step, OpStep): # Should always be OpStep here
                last_step.outputs = [final_output_table]

    # ----------------------------------------------------------------------
    # Check for unparsed statements or other disallowed statements
    # ----------------------------------------------------------------------
    # All body statements except the set, run, keep/drop, rename, if, and assignments
    parsed_statement_texts = set()
    for stmt_list in [set_statements, select_statements, rename_statements, filter_statements]:
        for stmt in stmt_list:
            parsed_statement_texts.add(stmt.text)
    
    # Check assignments
    assignment_statement_texts = set()
    for stmt in block.body:
        if assignment_regex.match(stmt.text):
            assignment_statement_texts.add(stmt.text)
    
    # Filter out parsed_statement_texts and assignments from block.body
    for stmt in block.body:
        if stmt.text == "run": # Run statement is handled by block segmentation, not parsed within
            continue
        if stmt.text in parsed_statement_texts:
            continue
        if stmt.text in assignment_statement_texts:
            continue
        
        # Any remaining statement in body is unsupported
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message=f"Unsupported statement or unparsed content in data step: '{stmt.text}'",
            loc=block.loc_span,
        )]

    return steps


def recognize_proc_sort_block(block: Block) -> OpStep | UnknownBlockStep:
    if not block.header.text.lower().startswith("proc sort"):
        return UnknownBlockStep(
            code="SANS_PARSE_INVALID_PROC_SORT_HEADER",
            message=f"Expected PROC SORT block to start with 'proc sort', got '{block.header.text}'",
            loc=block.header.loc,
        )

    # Extract data and out tables from header: proc sort data=<in> out=<out>;
    header_lower = block.header.text.lower()
    data_match = re.search(r"data\s*=\s*(\S+)", header_lower)
    out_match = re.search(r"out\s*=\s*(\S+)", header_lower)
    
    input_table = data_match.group(1) if data_match else None
    output_table = out_match.group(1) if out_match else None

    # Check for unsupported options in header
    # We only support data= and out= specifically
    unsupported_options = []
    # This regex looks for any word=value pair not matching data= or out=
    # It also looks for any standalone word that is not 'proc' or 'sort'
    # Simplified check: grab everything after 'proc sort' and split by space, then check for known options
    header_options_str = header_lower[len("proc sort"):].strip()
    header_words = re.findall(r'(\S+)', header_options_str) # Split by space
    
    # We are looking for "key=value" pairs OR standalone keywords like "nodupkey"
    # For v0.1, we only expect 'data=X' and 'out=Y'. Any other 'word=value' or 'word' is unsupported.
    supported_keywords = ["data=", "out="] # Note the trailing '=' to distinguish from just 'data' or 'out'

    for word in header_words:
        is_supported = False
        for sk in supported_keywords:
            if word.startswith(sk):
                is_supported = True
                break
        if not is_supported:
            unsupported_options.append(word)

    if unsupported_options:
        return UnknownBlockStep(
            code="SANS_PARSE_SORT_UNSUPPORTED_OPTION",
            message=f"Unsupported options in PROC SORT header: {', '.join(unsupported_options)}",
            loc=block.header.loc,
        )

    # Check if data= and out= are present
    if not input_table:
        return UnknownBlockStep(
            code="SANS_PARSE_SORT_MISSING_DATA",
            message="PROC SORT requires a DATA= option.",
            loc=block.header.loc,
        )
    if not output_table:
        return UnknownBlockStep(
            code="SANS_PARSE_SORT_MISSING_OUT",
            message="PROC SORT requires an OUT= option.",
            loc=block.header.loc,
        )

    # Check for BY statement
    by_statements = [s for s in block.body if s.text.lower().startswith("by")]
    if len(by_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_SORT_MISSING_BY",
            message="PROC SORT requires exactly one BY statement.",
            loc=block.loc_span,
        )
    by_stmt = by_statements[0]
    by_vars_match = re.match(r"by\s+(.+)", by_stmt.text.lower())
    if not by_vars_match:
        return UnknownBlockStep(
            code="SANS_PARSE_SORT_BY_MALFORMED",
            message=f"Malformed BY statement: '{by_stmt.text}'",
            loc=by_stmt.loc,
        )
    by_vars_str = by_vars_match.group(1).strip()
    by_vars = re.split(r'\s+', by_vars_str) # Split by one or more spaces

    # Check for other statements in body (excluding the BY statement) and the "run" statement
    other_statements_in_body = [
        s for s in block.body
        if not s.text.lower().startswith("by ") and s.text.lower() != "run"
    ]
    if other_statements_in_body:
        return UnknownBlockStep(
            code="SANS_PARSE_SORT_UNSUPPORTED_BODY_STATEMENT",
            message="PROC SORT contains unsupported statements in its body.",
            loc=block.loc_span,
        )

    return OpStep(
        op="sort",
        inputs=[input_table],
        outputs=[output_table],
        params={"by": [{"col": v, "asc": True} for v in by_vars]},
        loc=block.loc_span,
    )
