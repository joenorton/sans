import json
from pathlib import Path

import pytest

from sans.runtime import run_script
from sans.evidence import EvidenceConfig


def _run(script: str, out_dir: Path) -> dict:
    report = run_script(
        text=script,
        file_name="script.sans",
        bindings={},
        out_dir=out_dir,
        strict=True,
        legacy_sas=True,
    )
    assert report["status"] == "ok"
    evidence_path = out_dir / "artifacts" / "runtime.evidence.json"
    assert evidence_path.exists()
    return json.loads(evidence_path.read_text(encoding="utf-8"))


def test_constant_flip_and_non_constant_column(tmp_path: Path):
    base = (
        "# sans 0.1\n"
        "datasource in = inline_csv columns(id, A1C) do\n"
        "  id,A1C\n"
        "  1,7.1\n"
        "  2,6.8\n"
        "  3,8.2\n"
        "end\n"
        "table out = from(in) do\n"
        "  derive(label = \"{label}\")\n"
        "end\n"
        "save out to \"out.csv\"\n"
    )
    ev_high = _run(base.format(label="HIGH"), tmp_path / "high")
    ev_low = _run(base.format(label="LOW"), tmp_path / "low")

    col_high = ev_high["tables"]["out"]["columns"]["label"]
    col_low = ev_low["tables"]["out"]["columns"]["label"]
    assert col_high["unique_count"] == 1
    assert col_high["null_count"] == 0
    assert col_high["constant_value"] == "HIGH"
    assert col_low["constant_value"] == "LOW"

    a1c = ev_high["tables"]["out"]["columns"]["A1C"]
    assert a1c["unique_count"] != 1
    assert "constant_value" not in a1c


def test_runtime_evidence_deterministic(tmp_path: Path):
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv columns(id, val) do\n"
        "  id,val\n"
        "  1,10\n"
        "  2,20\n"
        "end\n"
        "table out = from(in) select id, val\n"
        "save out to \"out.csv\"\n"
    )
    ev1 = _run(script, tmp_path / "run1")
    ev2 = _run(script, tmp_path / "run2")
    assert json.dumps(ev1, sort_keys=True) == json.dumps(ev2, sort_keys=True)


def test_runtime_evidence_outputs_deduped(tmp_path: Path):
    script = (
        "# sans 0.1\n"
        "datasource in = inline_csv columns(id, val) do\n"
        "  id,val\n"
        "  1,10\n"
        "  2,20\n"
        "end\n"
        "table out = from(in) select id, val\n"
        "save out to \"out.csv\"\n"
    )
    ev = _run(script, tmp_path / "dedupe")
    outputs = ev.get("outputs") or []
    keys = {(o.get("name"), o.get("path")) for o in outputs}
    assert len(outputs) == len(keys)


def test_demo_high_low_compute_table_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    repo_root = Path(__file__).resolve().parents[2]
    demo_dir = repo_root / "demo" / "run_diff_clin_demo"
    script_path = demo_dir / "demo_low.sans"
    assert script_path.exists()
    script_text = script_path.read_text(encoding="utf-8")

    monkeypatch.chdir(demo_dir)
    ev = _run(script_text, tmp_path / "demo_low")
    label_col = ev["tables"]["__t6__"]["columns"]["label"]
    assert label_col["constant_value"] == "LOW"


def test_unique_cap_behavior(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    small_cfg = EvidenceConfig(unique_cap=5, topk=5, include_top_values=True, sample_cap=1000)
    monkeypatch.setattr("sans.runtime.DEFAULT_EVIDENCE_CONFIG", small_cfg)

    lines = ["# sans 0.1", "datasource in = inline_csv columns(id) do", "  id"]
    for i in range(1, 15):
        lines.append(f"  {i}")
    lines.extend([
        "end",
        "table out = from(in) select id",
        "save out to \"out.csv\"",
    ])
    script = "\n".join(lines) + "\n"

    ev = _run(script, tmp_path / "cap")
    col = ev["tables"]["out"]["columns"]["id"]
    assert col["unique_count_capped"] is True
    assert isinstance(col["unique_count"], str) and col["unique_count"].startswith(">=")
    assert "constant_value" not in col
