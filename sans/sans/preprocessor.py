import re
from pathlib import Path
from typing import Dict, Optional, List, Iterable

class MacroError(Exception):
    def __init__(self, message: str, file: Optional[str] = None, line: Optional[int] = None):
        super().__init__(message)
        self.file = file
        self.line = line

def _is_under_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True

class Preprocessor:
    def __init__(
        self,
        initial_vars: Optional[Dict[str, str]] = None,
        include_roots: Optional[Iterable[Path]] = None,
        allow_absolute_includes: bool = False,
        allow_include_escape: bool = False,
    ):
        self.vars = initial_vars or {}
        self.include_stack: List[str] = []
        self.include_roots = [r.resolve(strict=False) for r in (include_roots or [])]
        self.allow_absolute_includes = allow_absolute_includes
        self.allow_include_escape = allow_include_escape

    def _resolve_include(
        self,
        inc_path: str,
        current_file: Optional[str],
        line_no: int,
    ) -> Path:
        raw_path = Path(inc_path)
        candidates: List[Path] = []

        if raw_path.is_absolute():
            if not self.allow_absolute_includes:
                raise MacroError(
                    f"Absolute %include paths are not allowed: {inc_path}",
                    current_file,
                    line_no,
                )
            candidates.append(raw_path)
        else:
            if current_file:
                candidates.append(Path(current_file).resolve(strict=False).parent / raw_path)
            for root in self.include_roots:
                candidates.append(root / raw_path)

        if not candidates:
            raise MacroError(
                f"Relative %include path requires an include root: {inc_path}",
                current_file,
                line_no,
            )

        for candidate in candidates:
            resolved = candidate.resolve(strict=False)
            if self.include_roots and not self.allow_include_escape:
                if not any(_is_under_root(resolved, root) for root in self.include_roots):
                    continue
            if resolved.exists():
                return resolved

        raise MacroError(
            f"Included file not found or outside include roots: {inc_path}",
            current_file,
            line_no,
        )

    def _evaluate_condition(self, cond: str) -> bool:
        substituted = self.substitute(cond)
        def coerce(value: str):
            value = value.strip()
            if len(value) >= 2 and ((value[0] == value[-1] == "'") or (value[0] == value[-1] == '"')):
                value = value[1:-1]
            try:
                if "." in value:
                    return float(value)
                return int(value)
            except ValueError:
                return value

        for op in ("<=", ">=", "!=", "==", "=", "<", ">"):
            if op in substituted:
                left, right = substituted.split(op, 1)
                left_val = coerce(left)
                right_val = coerce(right)
                if op == "==":
                    return left_val == right_val
                if op == "=":
                    return left_val == right_val
                if op == "!=":
                    return left_val != right_val
                if op == "<":
                    return left_val < right_val
                if op == "<=":
                    return left_val <= right_val
                if op == ">":
                    return left_val > right_val
                if op == ">=":
                    return left_val >= right_val
        val = substituted.strip().lower()
        if val in {"0", "false", ""}:
            return False
        return True

    def substitute(self, text: str) -> str:
        def repl(match):
            name = match.group(1).lower()
            return self.vars.get(name, match.group(0))
        return re.sub(r"&([a-zA-Z_]\w*)\.?", repl, text)

    def process(self, text: str, current_file: Optional[str] = None) -> str:
        if current_file:
            self.include_stack.append(str(Path(current_file).resolve(strict=False)))
            
        lines = text.splitlines()
        output = []
        i = 0
        while i < len(lines):
            raw_line = lines[i]
            line_stripped = raw_line.strip()
            line_no = i + 1
            
            # %let
            let_match = re.match(r"^\s*%let\s+([a-zA-Z_]\w*)\s*=\s*(.*?);", line_stripped, re.IGNORECASE)
            if let_match:
                name = let_match.group(1).lower()
                val = self.substitute(let_match.group(2).strip())
                self.vars[name] = val
                output.append("") 
                i += 1
                continue
                
            # %include
            if line_stripped.lower().startswith("%include"):
                # Manual parse to avoid quote issues in tool
                quote_char = "'" if "'" in line_stripped else '"'
                parts = line_stripped.split(quote_char)
                if len(parts) >= 3:
                    inc_path = parts[1]
                    resolved = self._resolve_include(inc_path, current_file, line_no)
                    resolved_str = str(resolved)
                    if resolved_str in self.include_stack:
                        raise MacroError(f"Recursive %include detected: {inc_path}", current_file, line_no)
                    inc_text = resolved.read_text(encoding="utf-8")
                    inc_processed = self.process(inc_text, resolved_str)
                    output.append(inc_processed)
                    i += 1
                    continue
                raise MacroError(f"Malformed %include statement: {line_stripped}", current_file, line_no)

            if re.match(r"^%if\b", line_stripped, re.IGNORECASE):
                m_if = re.match(r"^%if\b(.*)$", line_stripped, re.IGNORECASE)
                if not m_if:
                    raise MacroError("Malformed %if statement.", current_file, line_no)
                remainder = m_if.group(1).strip()
                m_then = re.search(r"%then\b", remainder, re.IGNORECASE)
                if not m_then:
                    raise MacroError("Malformed %if: missing %then.", current_file, line_no)
                cond_str = remainder[:m_then.start()].strip()
                after_then = remainder[m_then.end():].strip()
                if not after_then:
                    raise MacroError("Malformed %if: missing THEN statement.", current_file, line_no)
                m_else = re.search(r"%else\b", after_then, re.IGNORECASE)
                if m_else:
                    then_part = after_then[:m_else.start()].strip()
                    else_part = after_then[m_else.end():].strip()
                else:
                    then_part = after_then
                    else_part = None
                if not then_part:
                    raise MacroError("Malformed %if: missing THEN statement.", current_file, line_no)
                if re.match(r"^%do\b", then_part, re.IGNORECASE):
                    raise MacroError("Unsupported macro control flow: %do.", current_file, line_no)
                if else_part is not None:
                    if not else_part:
                        raise MacroError("Malformed %if: missing ELSE statement.", current_file, line_no)
                    if re.match(r"^%do\b", else_part, re.IGNORECASE):
                        raise MacroError("Unsupported macro control flow: %do.", current_file, line_no)
                chosen = then_part if self._evaluate_condition(cond_str) else else_part
                if chosen:
                    lines[i] = chosen
                    continue
                output.append("")
                i += 1
                continue
            if re.match(r"^%else\b", line_stripped, re.IGNORECASE):
                raise MacroError("Unexpected %else without matching %if.", current_file, line_no)
            if re.match(r"^%then\b", line_stripped, re.IGNORECASE):
                raise MacroError("Unexpected %then without matching %if.", current_file, line_no)
            if re.match(r"^%do\b", line_stripped, re.IGNORECASE):
                raise MacroError("Unsupported macro control flow: %do.", current_file, line_no)
            if re.match(r"^%end\b", line_stripped, re.IGNORECASE):
                raise MacroError("Unsupported macro control flow: %end.", current_file, line_no)

            output.append(self.substitute(raw_line))
            i += 1
            
        if current_file:
            self.include_stack.pop()
            
        return "\n".join(output)

def preprocess_text(
    text: str,
    file_name: Optional[str] = None,
    include_roots: Optional[Iterable[Path]] = None,
    allow_absolute_includes: bool = False,
    allow_include_escape: bool = False,
) -> str:
    roots: List[Path] = []
    if file_name and file_name != "<string>":
        try:
            roots.append(Path(file_name).resolve(strict=False).parent)
        except OSError:
            roots.append(Path(file_name).parent)
    for root in include_roots or []:
        roots.append(Path(root))
    seen = set()
    deduped: List[Path] = []
    for root in roots:
        resolved = root.resolve(strict=False)
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(resolved)

    p = Preprocessor(
        include_roots=deduped,
        allow_absolute_includes=allow_absolute_includes,
        allow_include_escape=allow_include_escape,
    )
    return p.process(text, file_name)
