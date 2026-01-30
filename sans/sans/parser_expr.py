# sans/sans/parser_expr.py
from __future__ import annotations
import re
from typing import Any, Iterator, Optional, Literal

from .expr import ExprNode, lit, col, binop, boolop, unop, call
from ._loc import Loc # For error reporting, though for expr AST, Loc might be more granular.

# --- Tokenization ---
TOKEN_PATTERNS = [
    (r"\s+", None), # Whitespace to ignore - MUST be first
    (r"\"[^\"]*\"|\'[^\']*\'", "STRING"), # String literals (double or single quotes) - Moved higher
    (r"\b(and|or|not)\b", "KEYWORD_LOGICAL"),
    (r"\b(coalesce|if)\b", "KEYWORD_FUNCTION"), # Allowlisted functions
    (r"\bnull\b|\.", "NULL"), # Null literal (handle both 'null' keyword and '.' for SAS)
    (r"\b[a-zA-Z_]\w*\b", "IDENTIFIER"), # Column references / variable names
    (r"\d+(?:\.\d*)?", "NUMBER"), # Numeric literals (int or float)
    (r"<=|>=|\^=|~=|=|<|>|\+|-|\*|\/", "OPERATOR"), # Multi-char operators first, then single-char
    (r"\(", "LPAREN"),
    (r"\)", "RPAREN"),
    (r",", "COMMA"),
]

class Token:
    def __init__(self, type: str, value: str, loc: Loc):
        self.type = type
        self.value = value
        self.loc = loc

    def __repr__(self):
        return f"Token({self.type!r}, {self.value!r}, {self.loc})"

def tokenize(text: str, file_name: str = "<string>") -> Iterator[Token]:
    pos = 0
    line_num = 1
    col_num = 1 # Not strictly needed for Loc, but good for error reporting
    while pos < len(text):
        match = None
        for pattern, token_type in TOKEN_PATTERNS:
            regex = re.compile(pattern)
            m = regex.match(text, pos)
            if m:
                value = m.group(0)
                if token_type: # Not whitespace
                    yield Token(token_type, value, Loc(file_name, line_num, line_num))
                
                # Update line/col number
                new_line_count = value.count('\n')
                line_num += new_line_count
                if new_line_count > 0:
                    col_num = len(value) - value.rfind('\n')
                else:
                    col_num += len(value)
                
                pos = m.end(0)
                match = True
                break
        if not match:
            # Need to handle this better, maybe Refusal here
            raise ValueError(f"Unexpected character at {file_name}:{line_num}, pos {pos} (col {col_num}): '{text[pos]}'")

# --- Expression Parsing (Recursive Descent / Pratt-like) ---
# Operator precedence and associativity (higher binds tighter)
# Ref: https://en.wikipedia.org/wiki/Order_of_operations
PRECEDENCE = {
    'or': 1,
    'and': 2,
    'not': 3, # Unary operator
    '=': 4, '<': 4, '>': 4, '<=': 4, '>=': 4, '^=': 4, '~=': 4,
    '+': 5, '-': 5,
    '*': 6, '/': 6,
}

class Parser:
    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0

    def current_token(self) -> Optional[Token]:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None
    
    def peek_token(self, offset: int = 0) -> Optional[Token]:
        if self.pos + offset < len(self.tokens):
            return self.tokens[self.pos + offset]
        return None

    def advance(self) -> Token:
        token = self.current_token()
        if token:
            self.pos += 1
        return token

    def consume(self, expected_type: str) -> Token:
        token = self.advance()
        if not token or token.type != expected_type:
            raise ValueError(f"Expected token of type {expected_type}, got {token.type if token else 'EOF'}")
        return token
    
    def parse_expression(self, min_precedence: int = 0) -> ExprNode:
        # Debug prints removed for brevity in future iterations, add back if needed
        # print(f"parse_expression(min_precedence={min_precedence}) - Entry - current_token: {self.current_token()}")
        # NUD (Null Denotation) for prefix operators and atoms
        token = self.advance() # Get the current token
        if token is None:
            raise ValueError("Unexpected EOF while parsing expression")

        left_expr: ExprNode
        if token.type == "NUMBER":
            left_expr = lit(float(token.value) if '.' in token.value else int(token.value))
        elif token.type == "STRING":
            left_expr = lit(token.value[1:-1]) # Strip quotes
        elif token.type == "NULL":
            left_expr = lit(None)
        elif token.type == "IDENTIFIER" or token.type == "KEYWORD_FUNCTION": # Handle KEYWORD_FUNCTION for function calls
            # Check for function call
            if self.current_token() and self.current_token().type == "LPAREN":
                function_name = token.value.lower()
                self.consume("LPAREN")
                args: list[ExprNode] = []
                if self.current_token() and self.current_token().type != "RPAREN": # Handle empty args
                    args.append(self.parse_expression())
                    while self.current_token() and self.current_token().type == "COMMA":
                        self.consume("COMMA")
                        args.append(self.parse_expression())
                self.consume("RPAREN")
                # Basic allowlist check for functions (redundant if KEYWORD_FUNCTION is already limited)
                if function_name not in ["coalesce", "if"]:
                     raise ValueError(f"Unsupported function '{function_name}'")
                left_expr = call(function_name, args)
            else:
                # If not a function call, then it's a column ref (only if IDENTIFIER)
                if token.type == "IDENTIFIER":
                    left_expr = col(token.value)
                else: # Must be a KEYWORD_FUNCTION that's not a function call, which is an error
                    raise ValueError(f"Unexpected keyword '{token.value}'")
        elif token.type == "OPERATOR" and token.value in ["+", "-"]:
            right_expr = self.parse_expression(PRECEDENCE[token.value]) # Unary operators bind tighter
            left_expr = unop(token.value, right_expr)
        elif token.type == "KEYWORD_LOGICAL" and token.value == "not": # Unary not
            # token is already "not" from advance().
            right_expr = self.parse_expression(PRECEDENCE['not'])
            left_expr = unop("not", right_expr)
        elif token.type == "LPAREN":
            left_expr = self.parse_expression() # Parse inner expression
            self.consume("RPAREN")
        else:
            raise ValueError(f"Unexpected token: {token.value} ({token.type})")
        
        # LED (Left Denotation) for infix operators
        while self.current_token():
            op_token = self.current_token()
            # print(f"  LED loop - op_token: {op_token}, min_precedence: {min_precedence}")
            if op_token.type not in ["OPERATOR", "KEYWORD_LOGICAL"]: # Only these can be infix operators
                 break
            
            op_value_lower = op_token.value.lower()
            op_precedence = PRECEDENCE.get(op_value_lower, 0)
            
            if op_precedence < min_precedence:
                # print(f"  Breaking LED loop: {op_precedence} < {min_precedence}")
                break

            self.advance() # Consume the operator
            # print(f"  Consumed op: {op_token.value}. Next token for right_expr: {self.current_token()}")
            right_expr = self.parse_expression(op_precedence + 1) # All binary ops here are left-associative or non-associative.
            if op_value_lower in ["and", "or"]:
                args = []
                if left_expr.get("type") == "boolop" and left_expr.get("op") == op_value_lower:
                    args.extend(left_expr.get("args", []))
                else:
                    args.append(left_expr)
                if right_expr.get("type") == "boolop" and right_expr.get("op") == op_value_lower:
                    args.extend(right_expr.get("args", []))
                else:
                    args.append(right_expr)
                left_expr = boolop(op_value_lower, args)
            else:
                op_norm = "!=" if op_value_lower in ["^=", "~="] else op_value_lower
                left_expr = binop(op_norm, left_expr, right_expr)
            
        return left_expr

def parse_expression_from_string(text: str, file_name: str = "<string>") -> ExprNode:
    tokens = list(tokenize(text, file_name))
    parser = Parser(tokens)
    expr = parser.parse_expression()
    if parser.current_token():
        raise ValueError(f"Unexpected token(s) after expression: {parser.current_token().value}")
    return expr
