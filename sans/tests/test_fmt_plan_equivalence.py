from pathlib import Path

import pytest

from sans.compiler import _irdoc_to_dict
from sans.fmt import format_text, normalize_newlines
from sans.ir import DatasourceDecl, IRDoc
from sans.sans_script import lower_script, parse_sans_script


FIXTURES = Path("fixtures/fmt")
OK_DIR = FIXTURES / "ok"
UGLY_DIR = FIXTURES / "ugly"


def _fixture_paths(dir_path: Path) -> list[Path]:
    return sorted(dir_path.glob("*.sans"))


def _normalize_plan(plan: dict) -> dict:
    for step in plan.get("steps", []):
        if "loc" in step:
            step.pop("loc", None)
    return plan


def _compile_plan(text: str, file_name: str) -> dict:
    script = parse_sans_script(text, file_name)
    steps, references = lower_script(script, file_name)
    ir_datasources: dict[str, DatasourceDecl] = {}
    for ast_ds in script.datasources.values():
        if ast_ds.kind == "csv":
            ir_ds = DatasourceDecl(
                kind="csv",
                path=ast_ds.path,
                columns=ast_ds.columns,
            )
        elif ast_ds.kind == "inline_csv":
            ir_ds = DatasourceDecl(
                kind="inline_csv",
                columns=ast_ds.columns,
                inline_text=ast_ds.inline_text,
                inline_sha256=ast_ds.inline_sha256,
            )
        else:
            raise AssertionError(f"Unknown datasource kind: {ast_ds.kind}")
        ir_datasources[ast_ds.name] = ir_ds
    irdoc = IRDoc(
        steps=steps,
        tables=set(references),
        table_facts={},
        datasources=ir_datasources,
    )
    return _normalize_plan(_irdoc_to_dict(irdoc))


@pytest.mark.parametrize("path", _fixture_paths(OK_DIR) + _fixture_paths(UGLY_DIR))
def test_fmt_compiled_plan_equivalence(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    formatted = format_text(text, mode="canonical", file_name=str(path))
    original = _compile_plan(normalize_newlines(text), str(path))
    formatted_plan = _compile_plan(formatted, str(path))
    assert formatted_plan == original
