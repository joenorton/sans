from pathlib import Path

import pytest

from sans.__main__ import main as sans_main
from sans.fmt import format_text, normalize_newlines
from sans.sans_script import parse_sans_script


FIXTURES = Path("fixtures/fmt")
OK_DIR = FIXTURES / "ok"
UGLY_DIR = FIXTURES / "ugly"
BAD_DIR = FIXTURES / "bad"
BAD_FUTURE_DIR = FIXTURES / "bad_future"


def _fixture_paths(dir_path: Path) -> list[Path]:
    return sorted(dir_path.glob("*.sans"))


@pytest.mark.parametrize("path", _fixture_paths(OK_DIR) + _fixture_paths(UGLY_DIR))
def test_fmt_idempotent(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    formatted = format_text(text, mode="canonical", file_name=str(path))
    assert format_text(formatted, mode="canonical", file_name=str(path)) == formatted


@pytest.mark.parametrize("path", _fixture_paths(OK_DIR) + _fixture_paths(UGLY_DIR))
def test_fmt_parse_equivalence(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    formatted = format_text(text, mode="canonical", file_name=str(path))
    original_ast = parse_sans_script(normalize_newlines(text), str(path))
    formatted_ast = parse_sans_script(formatted, str(path))
    assert formatted_ast == original_ast


@pytest.mark.parametrize("path", _fixture_paths(OK_DIR) + _fixture_paths(UGLY_DIR))
def test_fmt_identity_roundtrip(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert format_text(text, mode="identity", file_name=str(path)) == normalize_newlines(text)


@pytest.mark.parametrize("path", _fixture_paths(BAD_DIR))
def test_fmt_bad_fixtures_fail(path: Path, tmp_path: Path) -> None:
    ret = sans_main(["fmt", str(path)])
    assert ret != 0
    out_dir = tmp_path / "out"
    ret_check = sans_main(["check", str(path), "--out", str(out_dir)])
    assert ret_check != 0


@pytest.mark.parametrize("path", _fixture_paths(BAD_FUTURE_DIR))
def test_fmt_bad_future_expected(path: Path, tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    ret_check = sans_main(["check", str(path), "--out", str(out_dir)])
    if ret_check == 0:
        pytest.xfail("Validator does not enforce this semantic error yet.")


def test_fmt_style_flag() -> None:
    path = OK_DIR / "const_single.sans"
    ret_ok = sans_main(["fmt", str(path), "--check", "--style", "v0"])
    assert ret_ok == 0
    ret_bad = sans_main(["fmt", str(path), "--check", "--style", "v1"])
    assert ret_bad != 0


@pytest.mark.parametrize("path", _fixture_paths(UGLY_DIR))
def test_fmt_check_ugly_then_format(path: Path, tmp_path: Path) -> None:
    ret_check = sans_main(["fmt", str(path), "--check"])
    assert ret_check != 0

    formatted = format_text(path.read_text(encoding="utf-8"), mode="canonical", file_name=str(path))
    tmp_file = tmp_path / path.name
    tmp_file.write_text(formatted, encoding="utf-8")

    ret_check_formatted = sans_main(["fmt", str(tmp_file), "--check"])
    assert ret_check_formatted == 0
