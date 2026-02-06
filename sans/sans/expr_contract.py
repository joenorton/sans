from __future__ import annotations

from typing import Dict, Iterable, List, Tuple

from .parser_expr import PRECEDENCE

STRICT_KEYWORDS = {"and", "or", "not"}
STRICT_OPERATORS = {token for token in PRECEDENCE.keys() if token not in STRICT_KEYWORDS}


def strict_precedence_table(precedence: Dict[str, int] | None = None) -> List[Tuple[str, ...]]:
    """
    Return precedence levels from lowest to highest.
    Each level is a tuple of tokens at the same precedence.
    """
    prec = precedence or PRECEDENCE
    levels: Dict[int, List[str]] = {}
    for token, rank in prec.items():
        levels.setdefault(rank, []).append(token)
    return [tuple(sorted(levels[rank])) for rank in sorted(levels)]


STRICT_PRECEDENCE = strict_precedence_table()

