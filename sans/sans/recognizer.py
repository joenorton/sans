from __future__ import annotations
import re
from typing import Optional, Any

from .frontend import Block, Statement
from .ir import OpStep, UnknownBlockStep, Step # Import Step explicitly
from ._loc import Loc
from .parser_expr import parse_expression_from_string


def _starts_with_token(stmt_text: str, token: str, extra_follow: str = "") -> bool:
    if not stmt_text.startswith(token):
        return False
    if len(stmt_text) == len(token):
        return True
    next_ch = stmt_text[len(token)]
    return next_ch.isspace() or next_ch in extra_follow


def _expr_uses_by_flag(expr: Any) -> bool:
    if not isinstance(expr, dict):
        return False
    node_type = expr.get("type")
    if node_type == "col":
        name = expr.get("name", "")
        return name.startswith("first.") or name.startswith("last.")
    if node_type == "binop":
        return _expr_uses_by_flag(expr.get("left")) or _expr_uses_by_flag(expr.get("right"))
    if node_type == "boolop":
        return any(_expr_uses_by_flag(arg) for arg in expr.get("args", []))
    if node_type == "unop":
        return _expr_uses_by_flag(expr.get("arg"))
    if node_type == "call":
        return any(_expr_uses_by_flag(arg) for arg in expr.get("args", []))
    return False


def _split_tokens_outside_parens(text: str) -> list[str]:
    tokens: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch.isspace() and depth == 0:
            if buf:
                tokens.append("".join(buf))
                buf = []
            continue
        buf.append(ch)
    if buf:
        tokens.append("".join(buf))
    return tokens


def _split_sql_list(text: str) -> list[str]:
    items: list[str] = []
    buf: list[str] = []
    depth = 0
    in_sq = False
    in_dq = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif not in_sq and not in_dq:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == "," and depth == 0:
                item = "".join(buf).strip()
                if item:
                    items.append(item)
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1
    if buf:
        item = "".join(buf).strip()
        if item:
            items.append(item)
    return items


def _find_keyword_outside(text: str, keyword: str) -> int:
    lower = text.lower()
    kw = keyword.lower()
    depth = 0
    in_sq = False
    in_dq = False
    i = 0
    while i <= len(text) - len(kw):
        ch = text[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif not in_sq and not in_dq:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            if depth == 0 and lower.startswith(kw, i):
                before = text[i - 1] if i > 0 else " "
                after = text[i + len(kw)] if i + len(kw) < len(text) else " "
                if before.isspace() and after.isspace():
                    return i
        i += 1
    return -1


def _parse_sql_table_spec(spec_text: str, loc: Loc) -> dict[str, str] | UnknownBlockStep:
    parts = [p for p in spec_text.strip().split() if p]
    if not parts:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_TABLE_MALFORMED",
            message="Empty table spec in PROC SQL.",
            loc=loc,
        )
    if len(parts) == 1:
        table = parts[0].lower()
        return {"table": table, "alias": table}
    if len(parts) == 2:
        if parts[0].lower() == "as":
            return UnknownBlockStep(
                code="SANS_PARSE_SQL_TABLE_MALFORMED",
                message=f"Malformed table alias in '{spec_text}'",
                loc=loc,
            )
        return {"table": parts[0].lower(), "alias": parts[1].lower()}
    if len(parts) == 3 and parts[1].lower() == "as":
        return {"table": parts[0].lower(), "alias": parts[2].lower()}
    return UnknownBlockStep(
        code="SANS_PARSE_SQL_TABLE_MALFORMED",
        message=f"Malformed table alias in '{spec_text}'",
        loc=loc,
    )


def _parse_sql_select_item(item_text: str, loc: Loc) -> dict[str, Any] | UnknownBlockStep:
    s = item_text.strip()
    alias = None
    alias_match = re.match(r"(.+?)\s+as\s+([a-zA-Z_]\w*)$", s, re.IGNORECASE)
    if alias_match:
        s = alias_match.group(1).strip()
        alias = alias_match.group(2).lower()

    agg_match = re.match(r"(count|sum|min|max|avg)\s*\((.*)\)$", s, re.IGNORECASE)
    if agg_match:
        func = agg_match.group(1).lower()
        arg = agg_match.group(2).strip()
        if func == "count" and arg == "*":
            arg = "*"
        else:
            if not arg or arg == "*":
                return UnknownBlockStep(
                    code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
                    message=f"Malformed aggregate expression '{item_text}'",
                    loc=loc,
                )
            arg = arg.lower()
        if not alias:
            if arg == "*":
                alias = func
            else:
                alias = f"{func}_{arg.split('.')[-1]}"
        return {"type": "agg", "func": func, "arg": arg, "alias": alias}

    if not re.match(r"^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)?$", s):
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message=f"Unsupported select expression '{item_text}'",
            loc=loc,
        )
    col_name = s.lower()
    if not alias:
        alias = col_name.split(".")[-1]
    return {"type": "col", "name": col_name, "alias": alias}


def _parse_dataset_spec(spec_text: str, loc: Loc, allow_in_flag: bool) -> dict[str, Any] | UnknownBlockStep:
    s = spec_text.strip()
    if not s:
        return UnknownBlockStep(
            code="SANS_PARSE_DATASET_SPEC_MALFORMED",
            message="Empty dataset spec.",
            loc=loc,
        )
    name_match = re.match(r"^([a-zA-Z_]\w*)", s)
    if not name_match:
        return UnknownBlockStep(
            code="SANS_PARSE_DATASET_SPEC_MALFORMED",
            message=f"Malformed dataset spec: '{spec_text}'",
            loc=loc,
        )
    table_name = name_match.group(1).lower()
    idx = name_match.end(1)
    rest = s[idx:].strip()

    options_text = ""
    if rest:
        if not rest.startswith("("):
            return UnknownBlockStep(
                code="SANS_PARSE_DATASET_SPEC_MALFORMED",
                message=f"Malformed dataset options in '{spec_text}'",
                loc=loc,
            )
        depth = 0
        end_idx = None
        for j, ch in enumerate(rest):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end_idx = j
                    break
        if end_idx is None:
            return UnknownBlockStep(
                code="SANS_PARSE_DATASET_SPEC_MALFORMED",
                message=f"Unclosed dataset options in '{spec_text}'",
                loc=loc,
            )
        options_text = rest[1:end_idx].strip()
        trailing = rest[end_idx + 1:].strip()
        if trailing:
            return UnknownBlockStep(
                code="SANS_PARSE_DATASET_SPEC_MALFORMED",
                message=f"Unexpected text after dataset options in '{spec_text}'",
                loc=loc,
            )

    spec: dict[str, Any] = {
        "table": table_name,
        "in": None,
        "keep": None,
        "drop": None,
        "rename": None,
        "where": None,
    }

    if not options_text:
        return spec

    tokens = _split_tokens_outside_parens(options_text)
    known_options = {"keep", "drop", "rename", "where", "in"}
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if "=" not in token:
            return UnknownBlockStep(
                code="SANS_PARSE_DATASET_OPTION_UNKNOWN",
                message=f"Unknown dataset option token '{token}' in '{spec_text}'",
                loc=loc,
            )
        key, value = token.split("=", 1)
        key_lower = key.lower()
        if key_lower not in known_options:
            return UnknownBlockStep(
                code="SANS_PARSE_DATASET_OPTION_UNKNOWN",
                message=f"Unknown dataset option '{key}' in '{spec_text}'",
                loc=loc,
            )

        if key_lower == "in":
            if not allow_in_flag:
                return UnknownBlockStep(
                    code="SANS_PARSE_DATASET_OPTION_UNKNOWN",
                    message=f"IN= option not allowed here: '{spec_text}'",
                    loc=loc,
                )
            if not value:
                return UnknownBlockStep(
                    code="SANS_PARSE_DATASET_OPTION_MALFORMED",
                    message=f"Malformed IN= option in '{spec_text}'",
                    loc=loc,
                )
            spec["in"] = value
            i += 1
            continue

        if key_lower in {"keep", "drop"}:
            if key_lower == "keep" and spec["drop"]:
                return UnknownBlockStep(
                    code="SANS_PARSE_DATASET_OPTION_MALFORMED",
                    message="KEEP and DROP cannot both be specified in dataset options.",
                    loc=loc,
                )
            if key_lower == "drop" and spec["keep"]:
                return UnknownBlockStep(
                    code="SANS_PARSE_DATASET_OPTION_MALFORMED",
                    message="KEEP and DROP cannot both be specified in dataset options.",
                    loc=loc,
                )
            values: list[str] = []
            if value:
                if value.startswith("(") and value.endswith(")"):
                    inner = value[1:-1].strip()
                    values.extend([v for v in re.split(r"\s+", inner) if v])
                    i += 1
                else:
                    values.append(value)
                    i += 1
                    while i < len(tokens):
                        nxt = tokens[i]
                        if "=" in nxt and nxt.split("=", 1)[0].lower() in known_options:
                            break
                        values.append(nxt)
                        i += 1
            else:
                i += 1
                while i < len(tokens):
                    nxt = tokens[i]
                    if "=" in nxt and nxt.split("=", 1)[0].lower() in known_options:
                        break
                    values.append(nxt)
                    i += 1
            spec[key_lower] = values
            continue

        if key_lower == "rename":
            if not value or not (value.startswith("(") and value.endswith(")")):
                return UnknownBlockStep(
                    code="SANS_PARSE_DATASET_OPTION_MALFORMED",
                    message=f"Malformed RENAME option in '{spec_text}'",
                    loc=loc,
                )
            inner = value[1:-1].strip()
            rename_map: dict[str, str] = {}
            for pair in re.split(r"\s+", inner):
                if not pair:
                    continue
                parts = pair.split("=")
                if len(parts) != 2:
                    return UnknownBlockStep(
                        code="SANS_PARSE_DATASET_OPTION_MALFORMED",
                        message=f"Malformed RENAME pair '{pair}' in '{spec_text}'",
                        loc=loc,
                    )
                rename_map[parts[0]] = parts[1]
            spec["rename"] = rename_map
            i += 1
            continue

        if key_lower == "where":
            if not value or not (value.startswith("(") and value.endswith(")")):
                return UnknownBlockStep(
                    code="SANS_PARSE_DATASET_OPTION_MALFORMED",
                    message=f"Malformed WHERE option in '{spec_text}'",
                    loc=loc,
                )
            inner = value[1:-1].strip()
            try:
                spec["where"] = parse_expression_from_string(inner, loc.file)
            except ValueError as e:
                return UnknownBlockStep(
                    code="SANS_PARSE_EXPRESSION_ERROR",
                    message=f"Error parsing WHERE predicate: {e}",
                    loc=loc,
                )
            i += 1
            continue

    return spec


def _is_stateful_data_step(block: Block) -> bool:
    for stmt in block.body:
        s = stmt.text.strip().lower()
        if not s:
            continue
        if s.startswith("merge "):
            return True
        if s.startswith("by "):
            return True
        if s.startswith("retain "):
            return True
        if s.startswith("output"):
            return True
        if s.startswith("else "):
            return True
        if s.startswith("if ") and " then " in s:
            return True
        if "first." in s or "last." in s:
            return True
    return False


def recognize_data_block(block: Block) -> list[OpStep | UnknownBlockStep]:
    if _is_stateful_data_step(block):
        return _recognize_stateful_data_block(block)
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

    # 2. Parse set statement: set <in>(options);
    set_statements = [s for s in block.body if s.text.lower().startswith("set")]
    if len(set_statements) != 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step must contain exactly one SET statement.",
            loc=block.loc_span,
        )]
    set_stmt = set_statements[0]
    set_match = re.match(r"set\s+(.+)$", set_stmt.text, re.IGNORECASE)
    if not set_match:
        return [UnknownBlockStep(
            code="SANS_PARSE_SET_STATEMENT_MALFORMED",
            message=f"Malformed SET statement: '{set_stmt.text}'",
            loc=set_stmt.loc,
        )]
    set_spec_text = set_match.group(1).strip()
    set_spec = _parse_dataset_spec(set_spec_text, set_stmt.loc, allow_in_flag=False)
    if isinstance(set_spec, UnknownBlockStep):
        return [set_spec]
    current_input_table = set_spec["table"]

    # Hard fail for forbidden tokens (statement-leading keywords only)
    def find_forbidden_token(stmt_text: str) -> Optional[str]:
        s = stmt_text.strip().lower()
        if not s:
            return None
        if s.startswith("%"):
            return "%"
        # Order matters for clearer error messages
        if _starts_with_token(s, "proc"):
            return "proc"
        if _starts_with_token(s, "do"):
            return "do"
        if _starts_with_token(s, "end"):
            return "end"
        if _starts_with_token(s, "retain"):
            return "retain"
        if _starts_with_token(s, "lag", extra_follow="("):
            return "lag"
        if s.startswith("first."):
            return "first."
        if s.startswith("last."):
            return "last."
        if _starts_with_token(s, "array"):
            return "array"
        if _starts_with_token(s, "call"):
            return "call"
        if _starts_with_token(s, "output"):
            return "output"
        if _starts_with_token(s, "by"):
            return "by"
        if _starts_with_token(s, "merge"):
            return "merge"
        if _starts_with_token(s, "infile"):
            return "infile"
        if _starts_with_token(s, "input"):
            return "input"
        return None

    for stmt in block.body:
        token = find_forbidden_token(stmt.text)
        if token:
            return [UnknownBlockStep(
                code="SANS_BLOCK_STATEFUL_TOKEN",
                message=(
                    f"Forbidden token '{token}' detected in data step: '{stmt.text}'. "
                    "Hint: this subset only supports simple SET + keep/rename/assign/filter steps."
                ),
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
    # Apply dataset options at read-time (where -> keep/drop -> rename)
    if set_spec.get("where"):
        pipeline_output_table = generate_temp_output()
        steps.append(OpStep(
            op="filter",
            inputs=[pipeline_input_table],
            outputs=[pipeline_output_table],
            params={"predicate": set_spec["where"]},
            loc=set_stmt.loc,
        ))
        pipeline_input_table = pipeline_output_table

    if set_spec.get("keep") or set_spec.get("drop"):
        pipeline_output_table = generate_temp_output()
        steps.append(OpStep(
            op="select",
            inputs=[pipeline_input_table],
            outputs=[pipeline_output_table],
            params={"keep": set_spec.get("keep") or [], "drop": set_spec.get("drop") or []},
            loc=set_stmt.loc,
        ))
        pipeline_input_table = pipeline_output_table

    if set_spec.get("rename"):
        pipeline_output_table = generate_temp_output()
        steps.append(OpStep(
            op="rename",
            inputs=[pipeline_input_table],
            outputs=[pipeline_output_table],
            params={"map": set_spec["rename"]},
            loc=set_stmt.loc,
        ))
        pipeline_input_table = pipeline_output_table
    
    # Process body statements in canonical order: rename, compute, filter, select
    
    # ----------------------------------------------------------------------
    # 1. Select (keep/drop) - applied last in the pipeline
    # ----------------------------------------------------------------------
    select_statements = [s for s in block.body if s.text.lower().startswith("keep ") or s.text.lower().startswith("drop ")]
    if len(select_statements) > 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step can have at most one KEEP or DROP statement.",
            loc=block.loc_span,
        )]
    select_params = None
    if select_statements:
        select_stmt = select_statements[0]
        select_params = {"keep": [], "drop": []}
        if select_stmt.text.lower().startswith("keep "):
            cols_str = select_stmt.text[len("keep "):].strip()
            select_params["keep"] = re.split(r'\s+', cols_str)
        else: # drop
            cols_str = select_stmt.text[len("drop "):].strip()
            select_params["drop"] = re.split(r'\s+', cols_str)

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
        
        # The filter is the last step unless select is present
        pipeline_output_table = final_output_table
        if select_params:
            pipeline_output_table = generate_temp_output()
        steps.append(OpStep(
            op="filter",
            inputs=[pipeline_input_table],
            outputs=[pipeline_output_table],
            params={"predicate": predicate_ast},
            loc=block.loc_span,
        ))
        pipeline_input_table = pipeline_output_table
    else:
        # No filter; output handled after optional select
        if not steps and not select_params: # Only 'set' and 'run', no intermediate ops were generated
            steps.append(OpStep(
                op="identity", # A pass-through op
                inputs=[pipeline_input_table],
                outputs=[final_output_table],
                loc=block.loc_span, # Loc of the entire data block
            ))

    # ----------------------------------------------------------------------
    # 5. Select (keep/drop) applied last if present
    # ----------------------------------------------------------------------
    if select_params:
        steps.append(OpStep(
            op="select",
            inputs=[pipeline_input_table],
            outputs=[final_output_table],
            params=select_params,
            loc=block.loc_span,
        ))
    elif steps:
        # If there were intermediate steps and no select, ensure the last one outputs final_output_table
        last_step = steps[-1]
        if isinstance(last_step, OpStep):
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
            message=(
                f"Unsupported statement or unparsed content in data step: '{stmt.text}'. "
                "Hint: remove the statement or rewrite using assignments + if filters."
            ),
            loc=block.loc_span,
        )]

    return steps


def _recognize_stateful_data_block(block: Block) -> list[OpStep | UnknownBlockStep]:
    if not block.header.text.lower().startswith("data "):
        return [UnknownBlockStep(
            code="SANS_PARSE_INVALID_DATA_BLOCK_HEADER",
            message=f"Expected data block to start with 'data', got '{block.header.text}'",
            loc=block.header.loc,
        )]

    header_match = re.match(r"data\s+(\S+)", block.header.text.lower())
    if not header_match:
        return [UnknownBlockStep(
            code="SANS_PARSE_DATA_HEADER_MALFORMED",
            message=f"Malformed data block header: '{block.header.text}'",
            loc=block.header.loc,
        )]
    final_output_table = header_match.group(1)

    set_statements = [s for s in block.body if s.text.lower().startswith("set ")]
    merge_statements = [s for s in block.body if s.text.lower().startswith("merge ")]
    if len(set_statements) + len(merge_statements) != 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step must contain exactly one SET or MERGE statement.",
            loc=block.loc_span,
        )]

    by_statements = [s for s in block.body if s.text.lower().startswith("by ")]
    if len(by_statements) > 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step can have at most one BY statement.",
            loc=block.loc_span,
        )]

    retain_statements = [s for s in block.body if s.text.lower().startswith("retain ")]
    if len(retain_statements) > 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step can have at most one RETAIN statement.",
            loc=block.loc_span,
        )]

    keep_statements = [s for s in block.body if s.text.lower().startswith("keep ")]
    if len(keep_statements) > 1:
        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message="Data step can have at most one KEEP statement.",
            loc=block.loc_span,
        )]

    # Parse set/merge inputs.
    input_specs: list[dict[str, Any]] = []
    mode: str
    if set_statements:
        mode = "set"
        set_stmt = set_statements[0]
        set_match = re.match(r"set\s+(.+)$", set_stmt.text, re.IGNORECASE)
        if not set_match:
            return [UnknownBlockStep(
                code="SANS_PARSE_SET_STATEMENT_MALFORMED",
                message=f"Malformed SET statement: '{set_stmt.text}'",
                loc=set_stmt.loc,
            )]
        set_spec_text = set_match.group(1).strip()
        set_spec = _parse_dataset_spec(set_spec_text, set_stmt.loc, allow_in_flag=False)
        if isinstance(set_spec, UnknownBlockStep):
            return [set_spec]
        input_specs.append(set_spec)
    else:
        mode = "merge"
        merge_stmt = merge_statements[0]
        merge_match = re.match(r"merge\s+(.+)", merge_stmt.text, re.IGNORECASE)
        merge_body = merge_match.group(1).strip() if merge_match else ""
        if not merge_body:
            return [UnknownBlockStep(
                code="SANS_PARSE_MERGE_STATEMENT_MALFORMED",
                message=f"Malformed MERGE statement: '{merge_stmt.text}'",
                loc=merge_stmt.loc,
            )]
        parts = _split_tokens_outside_parens(merge_body)
        in_flags: set[str] = set()
        for part in parts:
            if not part:
                continue
            spec = _parse_dataset_spec(part, merge_stmt.loc, allow_in_flag=True)
            if isinstance(spec, UnknownBlockStep):
                return [spec]
            if spec.get("in"):
                flag = spec["in"]
                if flag in in_flags:
                    return [UnknownBlockStep(
                        code="SANS_PARSE_MERGE_STATEMENT_MALFORMED",
                        message=f"Duplicate IN= flag '{flag}' in MERGE.",
                        loc=merge_stmt.loc,
                    )]
                in_flags.add(flag)
            input_specs.append(spec)

    by_vars: list[str] = []
    if by_statements:
        by_stmt = by_statements[0]
        by_match = re.match(r"by\s+(.+)", by_stmt.text.lower())
        if not by_match:
            return [UnknownBlockStep(
                code="SANS_PARSE_BY_STATEMENT_MALFORMED",
                message=f"Malformed BY statement: '{by_stmt.text}'",
                loc=by_stmt.loc,
            )]
        by_vars = [v for v in re.split(r"\s+", by_match.group(1).strip()) if v]

    retain_vars: list[str] = []
    if retain_statements:
        retain_stmt = retain_statements[0]
        retain_body = retain_stmt.text[len("retain "):].strip()
        if not retain_body:
            return [UnknownBlockStep(
                code="SANS_PARSE_RETAIN_STATEMENT_MALFORMED",
                message=f"Malformed RETAIN statement: '{retain_stmt.text}'",
                loc=retain_stmt.loc,
            )]
        retain_vars = [v for v in re.split(r"\s+", retain_body) if v]

    keep_vars: list[str] = []
    if keep_statements:
        keep_stmt = keep_statements[0]
        keep_body = keep_stmt.text[len("keep "):].strip()
        if not keep_body:
            return [UnknownBlockStep(
                code="SANS_PARSE_KEEP_STATEMENT_MALFORMED",
                message=f"Malformed KEEP statement: '{keep_stmt.text}'",
                loc=keep_stmt.loc,
            )]
        keep_vars = [v for v in re.split(r"\s+", keep_body) if v]

    # Parse executable statements in order.
    statements: list[dict[str, Any]] = []
    explicit_output = False
    needs_by = False
    pending_if: Optional[dict[str, Any]] = None

    assignment_regex = re.compile(r"^([a-zA-Z_]\w*)\s*=\s*(.+)$")

    def parse_action(action_text: str, loc: Loc) -> dict[str, Any] | UnknownBlockStep:
        nonlocal needs_by
        action = action_text.strip()
        action_lower = action.lower()
        if action_lower.startswith("output"):
            if action_lower != "output":
                return UnknownBlockStep(
                    code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
                    message=f"Unsupported OUTPUT form: '{action_text}'",
                    loc=loc,
                )
            return {"type": "output"}
        assign_match = assignment_regex.match(action)
        if assign_match:
            col_name = assign_match.group(1)
            expr_str = assign_match.group(2)
            try:
                expr_ast = parse_expression_from_string(expr_str, block.header.loc.file)
            except ValueError as e:
                return UnknownBlockStep(
                    code="SANS_PARSE_EXPRESSION_ERROR",
                    message=f"Error parsing assignment expression for '{col_name}': {e}",
                    loc=loc,
                )
            if _expr_uses_by_flag(expr_ast):
                needs_by = True
            return {"type": "assign", "target": col_name, "expr": expr_ast}
        return UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message=f"Unsupported statement in data step: '{action_text}'",
            loc=loc,
        )

    def find_forbidden_token_stateful(stmt_text: str) -> Optional[str]:
        s = stmt_text.strip().lower()
        if not s:
            return None
        if s.startswith("%"):
            return "%"
        if _starts_with_token(s, "proc"):
            return "proc"
        if _starts_with_token(s, "do"):
            return "do"
        if _starts_with_token(s, "end"):
            return "end"
        if _starts_with_token(s, "lag", extra_follow="("):
            return "lag"
        if _starts_with_token(s, "array"):
            return "array"
        if _starts_with_token(s, "call"):
            return "call"
        if _starts_with_token(s, "infile"):
            return "infile"
        if _starts_with_token(s, "input"):
            return "input"
        return None

    for stmt in block.body:
        s = stmt.text.strip()
        s_lower = s.lower()

        token = find_forbidden_token_stateful(s)
        if token:
            return [UnknownBlockStep(
                code="SANS_BLOCK_STATEFUL_TOKEN",
                message=(
                    f"Forbidden token '{token}' detected in data step: '{stmt.text}'. "
                    "Hint: this subset does not support macros, do/end blocks, arrays, or lag."
                ),
                loc=block.loc_span,
            )]

        if s_lower.startswith("set ") or s_lower.startswith("merge "):
            continue
        if s_lower.startswith("by "):
            continue
        if s_lower.startswith("retain "):
            continue
        if s_lower.startswith("keep "):
            continue

        if pending_if and not s_lower.startswith("else "):
            pending_if = None

        if s_lower.startswith("if "):
            if " then " in s_lower:
                match = re.match(r"if\s+(.+?)\s+then\s+(.+)$", s, re.IGNORECASE)
                if not match:
                    return [UnknownBlockStep(
                        code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
                        message=f"Malformed IF/THEN statement: '{stmt.text}'",
                        loc=stmt.loc,
                    )]
                predicate_str = match.group(1)
                action_str = match.group(2)
                try:
                    predicate_ast = parse_expression_from_string(predicate_str, block.header.loc.file)
                except ValueError as e:
                    return [UnknownBlockStep(
                        code="SANS_PARSE_EXPRESSION_ERROR",
                        message=f"Error parsing IF predicate: {e}",
                        loc=stmt.loc,
                    )]
                if _expr_uses_by_flag(predicate_ast):
                    needs_by = True
                action = parse_action(action_str, stmt.loc)
                if isinstance(action, UnknownBlockStep):
                    return [action]
                if action.get("type") == "output":
                    explicit_output = True
                if_stmt = {"type": "if_then", "predicate": predicate_ast, "then": action, "else": None}
                statements.append(if_stmt)
                pending_if = if_stmt
                continue
            predicate_str = s[len("if "):].strip()
            try:
                predicate_ast = parse_expression_from_string(predicate_str, block.header.loc.file)
            except ValueError as e:
                return [UnknownBlockStep(
                    code="SANS_PARSE_EXPRESSION_ERROR",
                    message=f"Error parsing IF predicate: {e}",
                    loc=stmt.loc,
                )]
            if _expr_uses_by_flag(predicate_ast):
                needs_by = True
            statements.append({"type": "filter", "predicate": predicate_ast})
            continue

        if s_lower.startswith("else "):
            if not pending_if:
                return [UnknownBlockStep(
                    code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
                    message=f"ELSE without matching IF: '{stmt.text}'",
                    loc=stmt.loc,
                )]
            if pending_if.get("else") is not None:
                return [UnknownBlockStep(
                    code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
                    message=f"Multiple ELSE clauses for IF: '{stmt.text}'",
                    loc=stmt.loc,
                )]
            action_str = s[len("else "):].strip()
            if action_str.lower().startswith("if "):
                return [UnknownBlockStep(
                    code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
                    message=f"ELSE IF not supported in data step: '{stmt.text}'",
                    loc=stmt.loc,
                )]
            action = parse_action(action_str, stmt.loc)
            if isinstance(action, UnknownBlockStep):
                return [action]
            if action.get("type") == "output":
                explicit_output = True
            pending_if["else"] = action
            pending_if = None
            continue

        if s_lower.startswith("output"):
            if s_lower != "output":
                return [UnknownBlockStep(
                    code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
                    message=f"Unsupported OUTPUT form: '{stmt.text}'",
                    loc=stmt.loc,
                )]
            explicit_output = True
            statements.append({"type": "output"})
            continue

        # Assignment
        assign_match = assignment_regex.match(s)
        if assign_match:
            col_name = assign_match.group(1)
            expr_str = assign_match.group(2)
            try:
                expr_ast = parse_expression_from_string(expr_str, block.header.loc.file)
            except ValueError as e:
                return [UnknownBlockStep(
                    code="SANS_PARSE_EXPRESSION_ERROR",
                    message=f"Error parsing assignment expression for '{col_name}': {e}",
                    loc=stmt.loc,
                )]
            if _expr_uses_by_flag(expr_ast):
                needs_by = True
            statements.append({"type": "assign", "target": col_name, "expr": expr_ast})
            continue

        return [UnknownBlockStep(
            code="SANS_PARSE_UNSUPPORTED_DATASTEP_FORM",
            message=(
                f"Unsupported statement or unparsed content in data step: '{stmt.text}'. "
                "Hint: only assignment, if/then/else, output, retain, keep, by, set/merge are supported."
            ),
            loc=block.loc_span,
        )]

    if (mode == "merge" or needs_by) and not by_vars:
        return [UnknownBlockStep(
            code="SANS_PARSE_DATASTEP_MISSING_BY",
            message="Data step uses MERGE/first./last. and requires a BY statement.",
            loc=block.loc_span,
        )]

    return [
        OpStep(
            op="data_step",
            inputs=[spec["table"] for spec in input_specs],
            outputs=[final_output_table],
            params={
                "mode": mode,
                "inputs": input_specs,
                "by": by_vars,
                "retain": retain_vars,
                "keep": keep_vars,
                "statements": statements,
                "explicit_output": explicit_output,
            },
            loc=block.loc_span,
        )
    ]


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
    # Supported keywords: data=, out=, nodupkey
    supported_keywords = ["data=", "out=", "nodupkey"] # Note the trailing '=' to distinguish from just 'data' or 'out'
    nodupkey = False

    for word in header_words:
        is_supported = False
        for sk in supported_keywords:
            if word.startswith(sk):
                is_supported = True
                if word == "nodupkey":
                    nodupkey = True
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

    params: dict[str, Any] = {"by": [{"col": v, "asc": True} for v in by_vars]}
    if nodupkey:
        params["nodupkey"] = True

    return OpStep(
        op="sort",
        inputs=[input_table],
        outputs=[output_table],
        params=params,
        loc=block.loc_span,
    )


def recognize_proc_format_block(block: Block) -> list[OpStep] | UnknownBlockStep:
    if not block.header.text.lower().startswith("proc format"):
        return UnknownBlockStep(
            code="SANS_PARSE_INVALID_PROC_FORMAT_HEADER",
            message=f"Expected PROC FORMAT block to start with 'proc format', got '{block.header.text}'",
            loc=block.header.loc,
        )

    value_statements = [s for s in block.body if s.text.lower().startswith("value ")]
    other_statements = [
        s for s in block.body
        if not s.text.lower().startswith("value ")
        and s.text.lower() != "run"
    ]
    if other_statements:
        return UnknownBlockStep(
            code="SANS_PARSE_FORMAT_UNSUPPORTED_STATEMENT",
            message=f"Unsupported PROC FORMAT statement: '{other_statements[0].text}'",
            loc=other_statements[0].loc,
        )
    if not value_statements:
        return UnknownBlockStep(
            code="SANS_PARSE_FORMAT_MISSING_VALUE",
            message="PROC FORMAT requires at least one VALUE statement.",
            loc=block.loc_span,
        )

    steps: list[OpStep] = []
    for stmt in value_statements:
        stmt_text = stmt.text.strip()
        match = re.match(r"value\s+(\$?[a-zA-Z_]\w*)\s+(.+)$", stmt_text, re.IGNORECASE | re.DOTALL)
        if not match:
            return UnknownBlockStep(
                code="SANS_PARSE_FORMAT_VALUE_MALFORMED",
                message=f"Malformed VALUE statement: '{stmt.text}'",
                loc=stmt.loc,
            )
        fmt_name = match.group(1).lower()
        mappings_text = match.group(2)

        pairs = []
        for m in re.finditer(r"(\"[^\"]*\"|'[^']*'|other)\s*=\s*(\"[^\"]*\"|'[^']*')", mappings_text, re.IGNORECASE):
            key = m.group(1)
            val = m.group(2)
            pairs.append((key, val))

        if not pairs:
            return UnknownBlockStep(
                code="SANS_PARSE_FORMAT_VALUE_MALFORMED",
                message=f"No mappings found in VALUE statement: '{stmt.text}'",
                loc=stmt.loc,
            )

        mapping: dict[str, str] = {}
        default_other = None
        for key_token, val_token in pairs:
            val = val_token[1:-1]
            if key_token.lower() == "other":
                default_other = val
                continue
            key = key_token[1:-1]
            mapping[key] = val

        steps.append(
            OpStep(
                op="format",
                inputs=[],
                outputs=[f"__format__{fmt_name}"],
                params={"name": fmt_name, "map": mapping, "other": default_other},
                loc=stmt.loc,
            )
        )

    return steps


def recognize_proc_summary_block(block: Block) -> OpStep | UnknownBlockStep:
    if not block.header.text.lower().startswith("proc summary"):
        return UnknownBlockStep(
            code="SANS_PARSE_INVALID_PROC_SUMMARY_HEADER",
            message=f"Expected PROC SUMMARY block to start with 'proc summary', got '{block.header.text}'",
            loc=block.header.loc,
        )

    header_lower = block.header.text.lower()
    data_match = re.search(r"data\s*=\s*(\S+)", header_lower)
    input_table = data_match.group(1) if data_match else None

    header_options_str = header_lower[len("proc summary"):].strip()
    header_words = re.findall(r"(\S+)", header_options_str)
    supported_keywords = ["data=", "nway"]
    unsupported_options = []
    nway = False

    for word in header_words:
        is_supported = False
        for sk in supported_keywords:
            if word.startswith(sk):
                is_supported = True
                if word == "nway":
                    nway = True
                break
        if not is_supported:
            unsupported_options.append(word)

    if unsupported_options:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_UNSUPPORTED_OPTION",
            message=f"Unsupported options in PROC SUMMARY header: {', '.join(unsupported_options)}",
            loc=block.header.loc,
        )

    if not input_table:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_MISSING_DATA",
            message="PROC SUMMARY requires a DATA= option.",
            loc=block.header.loc,
        )

    if not nway:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_MISSING_NWAY",
            message="PROC SUMMARY requires the NWAY option.",
            loc=block.header.loc,
        )

    class_statements = [s for s in block.body if s.text.lower().startswith("class ")]
    var_statements = [s for s in block.body if s.text.lower().startswith("var ")]
    output_statements = [s for s in block.body if s.text.lower().startswith("output ")]

    if len(class_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_MISSING_CLASS",
            message="PROC SUMMARY requires exactly one CLASS statement.",
            loc=block.loc_span,
        )
    if len(var_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_MISSING_VAR",
            message="PROC SUMMARY requires exactly one VAR statement.",
            loc=block.loc_span,
        )
    if len(output_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_MISSING_OUTPUT",
            message="PROC SUMMARY requires exactly one OUTPUT statement.",
            loc=block.loc_span,
        )

    class_match = re.match(r"class\s+(.+)", class_statements[0].text.lower())
    if not class_match:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_CLASS_MALFORMED",
            message=f"Malformed CLASS statement: '{class_statements[0].text}'",
            loc=class_statements[0].loc,
        )
    class_vars = [v for v in re.split(r"\s+", class_match.group(1).strip()) if v]

    var_match = re.match(r"var\s+(.+)", var_statements[0].text.lower())
    if not var_match:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_VAR_MALFORMED",
            message=f"Malformed VAR statement: '{var_statements[0].text}'",
            loc=var_statements[0].loc,
        )
    var_vars = [v for v in re.split(r"\s+", var_match.group(1).strip()) if v]

    output_stmt = output_statements[0].text.strip()
    output_match = re.match(
        r"output\s+out\s*=\s*(\S+)\s+mean\s*=\s*/\s*autoname",
        output_stmt,
        re.IGNORECASE,
    )
    if not output_match:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_OUTPUT_MALFORMED",
            message=f"Malformed OUTPUT statement: '{output_stmt}'",
            loc=output_statements[0].loc,
        )
    output_table = output_match.group(1).lower()

    other_statements_in_body = [
        s for s in block.body
        if not s.text.lower().startswith("class ")
        and not s.text.lower().startswith("var ")
        and not s.text.lower().startswith("output ")
        and s.text.lower() != "run"
    ]
    if other_statements_in_body:
        return UnknownBlockStep(
            code="SANS_PARSE_SUMMARY_UNSUPPORTED_BODY_STATEMENT",
            message="PROC SUMMARY contains unsupported statements in its body.",
            loc=block.loc_span,
        )

    return OpStep(
        op="summary",
        inputs=[input_table],
        outputs=[output_table],
        params={"class": class_vars, "vars": var_vars, "stat": "mean", "autoname": True},
        loc=block.loc_span,
    )


def recognize_proc_transpose_block(block: Block) -> OpStep | UnknownBlockStep:
    if not block.header.text.lower().startswith("proc transpose"):
        return UnknownBlockStep(
            code="SANS_PARSE_INVALID_PROC_TRANSPOSE_HEADER",
            message=f"Expected PROC TRANSPOSE block to start with 'proc transpose', got '{block.header.text}'",
            loc=block.header.loc,
        )

    header_lower = block.header.text.lower()
    data_match = re.search(r"data\s*=\s*(\S+)", header_lower)
    out_match = re.search(r"out\s*=\s*(\S+)", header_lower)

    input_table = data_match.group(1) if data_match else None
    output_table = out_match.group(1) if out_match else None

    header_options_str = header_lower[len("proc transpose"):].strip()
    header_words = re.findall(r'(\S+)', header_options_str)
    supported_keywords = ["data=", "out="]

    unsupported_options = []
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
            code="SANS_PARSE_TRANSPOSE_UNSUPPORTED_OPTION",
            message=f"Unsupported options in PROC TRANSPOSE header: {', '.join(unsupported_options)}",
            loc=block.header.loc,
        )

    if not input_table:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_MISSING_DATA",
            message="PROC TRANSPOSE requires a DATA= option.",
            loc=block.header.loc,
        )
    if not output_table:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_MISSING_OUT",
            message="PROC TRANSPOSE requires an OUT= option.",
            loc=block.header.loc,
        )

    by_statements = [s for s in block.body if s.text.lower().startswith("by ")]
    id_statements = [s for s in block.body if s.text.lower().startswith("id ")]
    var_statements = [s for s in block.body if s.text.lower().startswith("var ")]

    if len(by_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_MISSING_BY",
            message="PROC TRANSPOSE requires exactly one BY statement.",
            loc=block.loc_span,
        )
    if len(id_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_MISSING_ID",
            message="PROC TRANSPOSE requires exactly one ID statement.",
            loc=block.loc_span,
        )
    if len(var_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_MISSING_VAR",
            message="PROC TRANSPOSE requires exactly one VAR statement.",
            loc=block.loc_span,
        )

    by_match = re.match(r"by\s+(.+)", by_statements[0].text.lower())
    if not by_match:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_BY_MALFORMED",
            message=f"Malformed BY statement: '{by_statements[0].text}'",
            loc=by_statements[0].loc,
        )
    by_vars = [v for v in re.split(r'\s+', by_match.group(1).strip()) if v]

    id_match = re.match(r"id\s+(\S+)", id_statements[0].text.lower())
    if not id_match:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_ID_MALFORMED",
            message=f"Malformed ID statement: '{id_statements[0].text}'",
            loc=id_statements[0].loc,
        )
    id_var = id_match.group(1)

    var_match = re.match(r"var\s+(\S+)", var_statements[0].text.lower())
    if not var_match:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_VAR_MALFORMED",
            message=f"Malformed VAR statement: '{var_statements[0].text}'",
            loc=var_statements[0].loc,
        )
    var_var = var_match.group(1)

    other_statements_in_body = [
        s for s in block.body
        if not s.text.lower().startswith("by ")
        and not s.text.lower().startswith("id ")
        and not s.text.lower().startswith("var ")
        and s.text.lower() != "run"
    ]
    if other_statements_in_body:
        return UnknownBlockStep(
            code="SANS_PARSE_TRANSPOSE_UNSUPPORTED_BODY_STATEMENT",
            message="PROC TRANSPOSE contains unsupported statements in its body.",
            loc=block.loc_span,
        )

    return OpStep(
        op="transpose",
        inputs=[input_table],
        outputs=[output_table],
        params={"by": by_vars, "id": id_var, "var": var_var, "last_wins": True},
        loc=block.loc_span,
    )


def recognize_proc_sql_block(block: Block) -> OpStep | UnknownBlockStep:
    if not block.header.text.lower().startswith("proc sql"):
        return UnknownBlockStep(
            code="SANS_PARSE_INVALID_PROC_SQL_HEADER",
            message=f"Expected PROC SQL block to start with 'proc sql', got '{block.header.text}'",
            loc=block.header.loc,
        )

    create_statements = [s for s in block.body if s.text.lower().startswith("create table")]
    other_statements = [
        s for s in block.body
        if not s.text.lower().startswith("create table")
        and s.text.lower() != "quit"
    ]
    if other_statements:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message=f"Unsupported PROC SQL statement: '{other_statements[0].text}'",
            loc=other_statements[0].loc,
        )
    if len(create_statements) != 1:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message="PROC SQL must contain exactly one CREATE TABLE AS SELECT statement.",
            loc=block.loc_span,
        )

    stmt = create_statements[0]
    stmt_text = stmt.text.strip()
    create_match = re.match(
        r"create\s+table\s+(\S+)\s+as\s+select\s+(.+)$",
        stmt_text,
        re.IGNORECASE | re.DOTALL,
    )
    if not create_match:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message=f"Malformed CREATE TABLE AS SELECT statement: '{stmt_text}'",
            loc=stmt.loc,
        )

    out_table = create_match.group(1).lower()
    remainder = create_match.group(2).strip()

    from_idx = _find_keyword_outside(remainder, "from")
    if from_idx == -1:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message="PROC SQL SELECT missing FROM clause.",
            loc=stmt.loc,
        )

    select_clause = remainder[:from_idx].strip()
    remainder = remainder[from_idx + len("from"):].strip()

    where_idx = _find_keyword_outside(remainder, "where")
    group_idx = _find_keyword_outside(remainder, "group by")
    if group_idx != -1 and where_idx != -1 and group_idx < where_idx:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message="GROUP BY must appear after WHERE in PROC SQL.",
            loc=stmt.loc,
        )

    cut_points = [idx for idx in [where_idx, group_idx] if idx != -1]
    from_end = min(cut_points) if cut_points else len(remainder)
    from_clause = remainder[:from_end].strip()

    where_clause = None
    if where_idx != -1:
        where_start = where_idx + len("where")
        where_end = group_idx if group_idx != -1 else len(remainder)
        where_clause = remainder[where_start:where_end].strip()

    group_clause = None
    if group_idx != -1:
        group_start = group_idx + len("group by")
        group_clause = remainder[group_start:].strip()

    if not select_clause:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message="PROC SQL SELECT list is empty.",
            loc=stmt.loc,
        )

    select_items: list[dict[str, Any]] = []
    for item in _split_sql_list(select_clause):
        parsed = _parse_sql_select_item(item, stmt.loc)
        if isinstance(parsed, UnknownBlockStep):
            return parsed
        select_items.append(parsed)

    if not from_clause:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message="PROC SQL FROM clause is empty.",
            loc=stmt.loc,
        )

    join_keywords = ["left join", "inner join"]
    join_start = -1
    for kw in join_keywords:
        idx = _find_keyword_outside(from_clause, kw)
        if idx != -1 and (join_start == -1 or idx < join_start):
            join_start = idx
    if join_start == -1 and _find_keyword_outside(from_clause, "join") != -1:
        return UnknownBlockStep(
            code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
            message="JOIN clauses require explicit INNER or LEFT keyword.",
            loc=stmt.loc,
        )
    base_spec_text = from_clause if join_start == -1 else from_clause[:join_start].strip()
    base_spec = _parse_sql_table_spec(base_spec_text, stmt.loc)
    if isinstance(base_spec, UnknownBlockStep):
        return base_spec

    joins: list[dict[str, Any]] = []
    remaining = from_clause[join_start:].strip() if join_start != -1 else ""
    while remaining:
        remaining_lower = remaining.lower()
        if remaining_lower.startswith("left join"):
            join_type = "left"
            remaining = remaining[len("left join"):].strip()
        elif remaining_lower.startswith("inner join"):
            join_type = "inner"
            remaining = remaining[len("inner join"):].strip()
        else:
            return UnknownBlockStep(
                code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
                message="JOIN clauses require explicit INNER or LEFT keyword.",
                loc=stmt.loc,
            )

        on_idx = _find_keyword_outside(remaining, "on")
        if on_idx == -1:
            return UnknownBlockStep(
                code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
                message="JOIN clause missing ON expression.",
                loc=stmt.loc,
            )
        table_spec_text = remaining[:on_idx].strip()
        on_and_rest = remaining[on_idx + len("on"):].strip()

        next_join = -1
        for kw in join_keywords:
            idx = _find_keyword_outside(on_and_rest, kw)
            if idx != -1 and (next_join == -1 or idx < next_join):
                next_join = idx
        on_expr_text = on_and_rest if next_join == -1 else on_and_rest[:next_join].strip()
        remaining = "" if next_join == -1 else on_and_rest[next_join:].strip()

        join_spec = _parse_sql_table_spec(table_spec_text, stmt.loc)
        if isinstance(join_spec, UnknownBlockStep):
            return join_spec
        try:
            on_expr = parse_expression_from_string(on_expr_text, stmt.loc.file)
        except ValueError as e:
            return UnknownBlockStep(
                code="SANS_PARSE_EXPRESSION_ERROR",
                message=f"Error parsing JOIN ON predicate: {e}",
                loc=stmt.loc,
            )
        joins.append(
            {
                "type": join_type,
                "table": join_spec["table"],
                "alias": join_spec["alias"],
                "on": on_expr,
            }
        )

    where_expr = None
    if where_clause:
        try:
            where_expr = parse_expression_from_string(where_clause, stmt.loc.file)
        except ValueError as e:
            return UnknownBlockStep(
                code="SANS_PARSE_EXPRESSION_ERROR",
                message=f"Error parsing WHERE predicate: {e}",
                loc=stmt.loc,
            )

    group_by: list[str] = []
    if group_clause:
        group_by = [g.strip().lower() for g in _split_sql_list(group_clause)]

    non_agg_cols = [item["name"] for item in select_items if item["type"] == "col"]
    agg_items = [item for item in select_items if item["type"] == "agg"]
    if group_by:
        for col in non_agg_cols:
            if col not in group_by:
                return UnknownBlockStep(
                    code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
                    message="Non-aggregate columns in SELECT must appear in GROUP BY.",
                    loc=stmt.loc,
                )
        for col in group_by:
            if col not in non_agg_cols:
                return UnknownBlockStep(
                    code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
                    message="GROUP BY columns must appear in SELECT.",
                    loc=stmt.loc,
                )
    else:
        if agg_items and non_agg_cols:
            return UnknownBlockStep(
                code="SANS_PARSE_SQL_UNSUPPORTED_FORM",
                message="SELECT with aggregates requires GROUP BY for non-aggregate columns.",
                loc=stmt.loc,
            )

    inputs = [base_spec["table"]] + [j["table"] for j in joins]
    return OpStep(
        op="sql_select",
        inputs=inputs,
        outputs=[out_table],
        params={
            "from": base_spec,
            "joins": joins,
            "select": select_items,
            "where": where_expr,
            "group_by": group_by,
        },
        loc=block.loc_span,
    )
