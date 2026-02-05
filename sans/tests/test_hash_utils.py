import copy
import json

from sans.hash_utils import canonicalize_report_for_hash, compute_artifact_hash, compute_report_sha256


def test_json_artifact_hash_canonical(tmp_path):
    obj_a = {"b": 2, "a": 1, "nested": {"z": 3, "y": [1, 2]}}
    obj_b = {"nested": {"y": [1, 2], "z": 3}, "a": 1, "b": 2}

    p1 = tmp_path / "a.json"
    p2 = tmp_path / "b.json"

    p1.write_text(json.dumps(obj_a, indent=2), encoding="utf-8")
    p2.write_text(json.dumps(obj_b, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")

    h1 = compute_artifact_hash(p1)
    h2 = compute_artifact_hash(p2)

    assert h1 is not None
    assert h1 == h2


def _swap_slashes(obj):
    if isinstance(obj, dict):
        return {k: _swap_slashes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_swap_slashes(v) for v in obj]
    if isinstance(obj, str) and ("/" in obj or "\\" in obj):
        return obj.replace("/", "\\")
    return obj


def test_report_hash_canonicalization_path_invariance(tmp_path):
    report = {
        "report_schema_version": "0.3",
        "status": "ok",
        "inputs": [{"path": "inputs/data/../data/input.csv", "sha256": "a"}],
        "artifacts": [{"path": "artifacts/./plan.ir.json", "sha256": "b"}],
        "outputs": [
            {"path": "outputs/z.csv", "sha256": "z"},
            {"path": "outputs/../outputs/a.csv", "sha256": "a"},
        ],
        "plan_path": "artifacts/../artifacts/plan.ir.json",
        "diagnostics": [{"loc": {"file": "scripts/../scripts/main.sas"}}],
        "report_sha256": "ignore-me",
        "report_hash": "ignore-me-too",
    }

    report_bs = _swap_slashes(copy.deepcopy(report))

    canonical_a = canonicalize_report_for_hash(report, tmp_path)
    canonical_b = canonicalize_report_for_hash(report_bs, tmp_path)
    assert canonical_a == canonical_b

    h1 = compute_report_sha256(report, tmp_path)
    h2 = compute_report_sha256(report_bs, tmp_path)
    assert h1 == h2

    parsed = json.loads(canonical_a)
    assert "report_sha256" not in parsed
    assert "report_hash" not in parsed
    assert parsed["inputs"][0]["path"] == "inputs/data/input.csv"
    assert parsed["artifacts"][0]["path"] == "artifacts/plan.ir.json"
    assert parsed["plan_path"] == "artifacts/plan.ir.json"
    assert parsed["diagnostics"][0]["loc"]["file"] == "scripts/main.sas"
    assert [o["path"] for o in parsed["outputs"]] == ["outputs/a.csv", "outputs/z.csv"]


def test_report_hash_canonicalization_drive_letters(tmp_path):
    report = {
        "report_schema_version": "0.3",
        "status": "ok",
        "outputs": [{"path": r"C:\bundle\outputs\out.csv", "sha256": "x"}],
    }
    report_posix = copy.deepcopy(report)
    report_posix["outputs"][0]["path"] = "c:/bundle/outputs/out.csv"

    canonical_a = canonicalize_report_for_hash(report, tmp_path)
    canonical_b = canonicalize_report_for_hash(report_posix, tmp_path)
    assert canonical_a == canonical_b

    parsed = json.loads(canonical_a)
    assert parsed["outputs"][0]["path"] == "c:/bundle/outputs/out.csv"
