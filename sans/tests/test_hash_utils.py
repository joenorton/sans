import json

from sans.hash_utils import compute_artifact_hash


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
