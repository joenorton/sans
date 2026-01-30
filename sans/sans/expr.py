# sans/sans/expr.py
from __future__ import annotations
from typing import Any, Dict, List

# Expression nodes are JSON-serializable dicts.
ExprNode = Dict[str, Any]

def lit(value: Any) -> ExprNode:
    return {"type": "lit", "value": value}

def col(name: str) -> ExprNode:
    return {"type": "col", "name": name}

def binop(op: str, left: ExprNode, right: ExprNode) -> ExprNode:
    return {"type": "binop", "op": op, "left": left, "right": right}

def boolop(op: str, args: List[ExprNode]) -> ExprNode:
    return {"type": "boolop", "op": op, "args": args}

def unop(op: str, arg: ExprNode) -> ExprNode:
    return {"type": "unop", "op": op, "arg": arg}

def call(name: str, args: List[ExprNode]) -> ExprNode:
    return {"type": "call", "name": name, "args": args}
