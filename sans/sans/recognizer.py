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
