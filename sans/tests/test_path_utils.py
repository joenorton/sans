from pathlib import Path

from sans.path_utils import fs_path_from_report


def test_fs_path_from_report_backslashes_normalized():
    path = fs_path_from_report(r"inputs\source\script.sas")
    assert path.as_posix() == "inputs/source/script.sas"
    assert path == Path("inputs/source/script.sas")


def test_fs_path_from_report_dot_segments_normalized():
    path = fs_path_from_report("inputs/source/../source/./script.sas")
    assert path.as_posix() == "inputs/source/script.sas"
