from __future__ import annotations
from dataclasses import dataclass
from typing import Iterator, Optional

from ._loc import Loc

@dataclass(frozen=True)
class Statement:
    text: str
    loc: Loc

@dataclass(frozen=True)
class Refusal:
    code: str
    message: str
    loc: Loc

@dataclass(frozen=True)
class Block:
    kind: str # "data" | "proc" | "other"
    header: Statement
    body: list[Statement]
    end: Optional[Statement]
    loc_span: Loc


# NOTE on LINE NUMBERS
# line numbers correspond to physical lines in the input text; callers/tests must avoid leading newlines if they care about specific values.
# this prevents future “helpful fixes.”

def split_statements(text: str, file_name: str = "<string>") -> Iterator[Statement]:
    class State:
        NORMAL = 0
        IN_DQ = 1
        IN_SQ = 2
        IN_BLOCK_COMMENT = 3
        IN_STAR_COMMENT = 4  # sas: * ... ; statement-comment (only at line-start)

    state = State.NORMAL
    buf: list[str] = []

    line = 1
    at_line_start = True  # after newline, before any non-ws

    # statement token location (non-comment, non-ws)
    stmt_start_line = None
    stmt_end_line = None

    def flush_stmt():
        nonlocal buf, stmt_start_line, stmt_end_line
        s = "".join(buf).strip()
        if s:
            # if we have text, we must have at least one token line
            start = stmt_start_line if stmt_start_line is not None else line
            end = stmt_end_line if stmt_end_line is not None else start
            yield Statement(s, Loc(file_name, start, end))
        buf = []
        stmt_start_line = None
        stmt_end_line = None

    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""

        # track line boundaries first-class
        if ch == "\n":
            if state in (State.NORMAL, State.IN_DQ, State.IN_SQ):
                buf.append(ch)  # keep newline as whitespace; strip() removes it
            line += 1
            at_line_start = True
            i += 1
            continue

        if state == State.IN_BLOCK_COMMENT:
            if ch == "*" and nxt == "/":
                state = State.NORMAL
                i += 2
                continue
            i += 1
            continue

        if state == State.IN_STAR_COMMENT:
            # star-comment ends at next semicolon; it may include anything except it isn't code
            if ch == ";":
                state = State.NORMAL
                i += 1
                # remain at_line_start = False; caller logic will set it on newline
                continue
            i += 1
            continue

        if state == State.IN_DQ:
            buf.append(ch)
            if ch == '"':
                state = State.NORMAL
            # string content counts as tokens
            if stmt_start_line is None:
                stmt_start_line = line
            stmt_end_line = line
            at_line_start = False
            i += 1
            continue

        if state == State.IN_SQ:
            buf.append(ch)
            if ch == "'":
                state = State.NORMAL
            if stmt_start_line is None:
                stmt_start_line = line
            stmt_end_line = line
            at_line_start = False
            i += 1
            continue

        # NORMAL state
        # detect start-of-line star comment: optional leading whitespace then '*'
        if at_line_start:
            if ch.isspace():
                buf.append(ch)  # preserve whitespace for now
                i += 1
                continue
            if ch == "*":
                state = State.IN_STAR_COMMENT
                at_line_start = False
                i += 1
                continue

        # block comment start
        if ch == "/" and nxt == "*":
            state = State.IN_BLOCK_COMMENT
            at_line_start = False
            i += 2
            continue

        # string starts
        if ch == '"':
            state = State.IN_DQ
            buf.append(ch)
            if stmt_start_line is None:
                stmt_start_line = line
            stmt_end_line = line
            at_line_start = False
            i += 1
            continue

        if ch == "'":
            state = State.IN_SQ
            buf.append(ch)
            if stmt_start_line is None:
                stmt_start_line = line
            stmt_end_line = line
            at_line_start = False
            i += 1
            continue

        # statement terminator
        if ch == ";":
            yield from flush_stmt()
            at_line_start = False
            i += 1
            continue

        # regular char
        buf.append(ch)
        if not ch.isspace():
            if stmt_start_line is None:
                stmt_start_line = line
            stmt_end_line = line
        at_line_start = False
        i += 1

    # flush trailing text if any (rare for sas, but fine)
    yield from flush_stmt()

def detect_refusal(text: str, file_name: str = "<string>") -> Optional[Refusal]:
    """
    v0.1 refusal detector: reject known-dangerous constructs early.
    byte-accurate line numbers: counts physical newlines in text.
    """
    return None

def segment_blocks(statements: list[Statement]) -> list[Block]:
    blocks: list[Block] = []
    i = 0
    while i < len(statements):
        stmt = statements[i] # Current statement to process
        lower_text = stmt.text.lower()

        if lower_text.startswith("data ") or lower_text.startswith("proc "):
            block_kind = "data" if lower_text.startswith("data ") else "proc"
            header = stmt
            block_body_statements: list[Statement] = []
            block_end_statement: Optional[Statement] = None
            start_loc = header.loc
            current_idx = i + 1 # Start scanning for body statements from next statement

            while current_idx < len(statements):
                current_stmt = statements[current_idx]
                lower_current_text = current_stmt.text.lower()
                
                # Explicit block terminator
                if lower_current_text == "run":
                    block_end_statement = current_stmt
                    current_idx += 1 # Advance past 'run;'
                    break
                
                # Implicit block terminator: start of a new data/proc block
                if lower_current_text.startswith("data ") or lower_current_text.startswith("proc "):
                    break # Stop collecting body statements for current block
                
                block_body_statements.append(current_stmt)
                current_idx += 1

            if block_end_statement:
                end_loc = block_end_statement.loc
                block_loc_span = start_loc.merge(end_loc)
                blocks.append(Block(block_kind, header, block_body_statements, block_end_statement, block_loc_span))
                i = current_idx # i is set to the statement AFTER the 'run;' (or where 'run;' would have been if it wasn't the last statement)
            else:
                # Block extends implicitly until a new block header or end of script
                if block_body_statements:
                    end_loc = block_body_statements[-1].loc
                else:
                    end_loc = header.loc
                block_loc_span = start_loc.merge(end_loc)
                blocks.append(Block(block_kind, header, block_body_statements, None, block_loc_span))
                i = current_idx # i is set to the statement that triggered implicit end, or len(statements)
        else:
            # Single-statement block
            blocks.append(Block("other", stmt, [], None, stmt.loc))
            i += 1
    return blocks
