import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pytest

from v14 import precompute as pc


def _fixture(tmp_path, monkeypatch):
    root, output = tmp_path / "root", tmp_path / "output"
    rawdir = root / "data/1m"
    audits = root / "data/symbol_audits"
    rawdir.mkdir(parents=True)
    audits.mkdir()
    universe = {"sessions": {"2020-06-01": {"symbols": ["AAA", "BBB", "CCC"]}}}
    pc.research.write_json(root / "data/universe_manifest.json", universe)
    files = []
    for symbol, stamp in [("AAA", "2020-06-02T12:00:00+05:30"), ("BBB", "2020-06-02T11:00:00+05:30"), ("CCC", "2020-06-02T12:10:00+05:30")]:
        pd.DataFrame({"symbol": [symbol], "value": [1]}).to_parquet(rawdir / f"{symbol}.parquet", index=False)
        digest = pc.research.sha256(rawdir / f"{symbol}.parquet")
        pc.research.write_json(audits / f"{symbol}.json", {"symbol": symbol, "updated_at": stamp, "sha256": digest})
        files.append({"symbol": symbol, "sha256": digest})
    pc.research.write_json(root / "data/manifest.json", {"study_sessions": ["2020-06-01"], "created_at": "2020-06-02T13:00:00+05:30", "files": files, "universe_sha256": pc.research.sha256(root / "data/universe_manifest.json")})
    pc.research.write_json(root / "data/progress.json", {"status": "COMPLETE", "updated_at": "2020-06-02T13:00:01+05:30"})
    monkeypatch.setattr(pc.research, "feature_code_signature", lambda: "fixed-signature")
    monkeypatch.setattr(pc.research, "load_nifty_context", lambda days, **kwargs: pd.DataFrame({"day": days, "nifty_first_bar_return_pct": [-.1] * len(days)}))
    return root, output


def _fake_worker(task):
    _, cache_text, symbol, *_ = task
    cache = Path(cache_text)
    frame = pd.DataFrame({"symbol": [symbol], "feature": [10.]})
    target = cache / f"{symbol}.parquet"
    frame.to_parquet(target, index=False)
    audit = {"symbol": symbol, "strict_candidates": 1}
    pc.research.write_json(cache / f"{symbol}.json", {"key": pc._task_key(task), "sha256": pc.research.sha256(target), "audit": audit})
    return frame, audit


def test_once_uses_only_new_verified_audits_and_cache_key_matches_worker(tmp_path, monkeypatch):
    root, output = _fixture(tmp_path, monkeypatch)
    calls = []
    def worker(task):
        calls.append(task[2])
        return _fake_worker(task)
    monkeypatch.setattr(pc.research, "_feature_worker", worker)
    # CCC's new raw data has not received its matching completed audit yet.
    pd.DataFrame({"symbol": ["CCC"], "value": [2]}).to_parquet(root / "data/1m/CCC.parquet", index=False)
    result = pc.run_precompute(root, output, "2020-06-02T11:30:00+05:30", once=True, executor_factory=ThreadPoolExecutor)
    assert result["status"] == "COMPLETE"
    assert result["completed_symbols"] == 1
    assert result["stale_attempts"] == 1
    assert calls == ["AAA"]
    assert (output / "feature_cache/AAA.parquet").exists()
    assert not (output / "feature_cache/BBB.parquet").exists()
    assert not (output / "precompute.lock").exists()
    assert not list((output / "feature_cache/.precompute_locks").glob("*.lock"))
    # Running again reuses the exact committed cache, without recomputation.
    again = pc.run_precompute(root, output, "2020-06-02T11:30:00+05:30", once=True, executor_factory=ThreadPoolExecutor)
    assert again["cached"] == 1
    assert calls == ["AAA"]


def test_source_change_during_computation_cannot_publish(tmp_path, monkeypatch):
    root, output = _fixture(tmp_path, monkeypatch)
    cache = output / "feature_cache"
    cache.mkdir(parents=True)
    source = root / "data/1m/AAA.parquet"
    task = (str(root), str(cache), "AAA", ["2020-06-01"], {"2020-06-01": -.1}, pc.research.sha256(source), "fixed-signature")
    def worker(task):
        result = _fake_worker(task)
        pd.DataFrame({"changed": [True]}).to_parquet(source, index=False)
        return result
    monkeypatch.setattr(pc.research, "_feature_worker", worker)
    result = pc._run_task(task)
    assert result["status"] == "SOURCE_CHANGED"
    assert not (cache / "AAA.parquet").exists()
    assert not (cache / "AAA.json").exists()
    assert not list((cache / ".precompute_work").iterdir())


def test_expected_worker_input_race_is_retriable_but_unrelated_errors_fail(tmp_path, monkeypatch):
    root, output = _fixture(tmp_path, monkeypatch)
    cache = output / "feature_cache"
    cache.mkdir(parents=True)
    source = root / "data/1m/AAA.parquet"
    task = (str(root), str(cache), "AAA", ["2020-06-01"], {"2020-06-01": -.1}, pc.research.sha256(source), "fixed-signature")
    def input_race(task):
        raise RuntimeError("Cash input changed while loading features: AAA")
    monkeypatch.setattr(pc.research, "_feature_worker", input_race)
    assert pc._run_task(task)["status"] == "SOURCE_CHANGED"
    def bad_computation(task):
        raise RuntimeError("Unexpected indicator computation failure")
    monkeypatch.setattr(pc.research, "_feature_worker", bad_computation)
    with pytest.raises(RuntimeError, match="Unexpected indicator"):
        pc._run_task(task)


def test_download_completion_requires_fresh_matching_manifest_and_no_writer(tmp_path, monkeypatch):
    root, _ = _fixture(tmp_path, monkeypatch)
    cutoff = pc._stamp("2020-06-02T11:30:00+05:30")
    assert pc._download_complete(root, cutoff)
    (root / "data/fetch.lock").write_text("123")
    assert not pc._download_complete(root, cutoff)
    (root / "data/fetch.lock").unlink()
    audit_path = root / "data/symbol_audits/AAA.json"
    audit = json.loads(audit_path.read_text())
    audit["sha256"] = "0" * 64
    pc.research.write_json(audit_path, audit)
    assert not pc._download_complete(root, cutoff)


def test_watch_mode_finishes_when_fresh_download_and_caches_are_complete(tmp_path, monkeypatch):
    root, output = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(pc.research, "_feature_worker", _fake_worker)
    result = pc.run_precompute(root, output, "2020-06-02T11:30:00+05:30", once=False, poll_seconds=.05, executor_factory=ThreadPoolExecutor)
    assert result["status"] == "COMPLETE"
    assert result["completed_symbols"] == 2
    assert set(json.loads((output / "precompute_manifest.json").read_text())["cache_keys"]) == {"AAA", "CCC"}


def test_concurrent_output_writer_is_rejected(tmp_path, monkeypatch):
    root, output = _fixture(tmp_path, monkeypatch)
    output.mkdir()
    (output / "precompute.lock").write_text("existing-pid")
    with pytest.raises(RuntimeError, match="writer lock exists"):
        pc.run_precompute(root, output, "2020-06-02T11:30:00+05:30", once=True, executor_factory=ThreadPoolExecutor)
    assert (output / "precompute.lock").read_text() == "existing-pid"


def test_ready_task_scan_reuses_unchanged_audits_and_revisits_changed_file(tmp_path, monkeypatch):
    root, output = _fixture(tmp_path, monkeypatch)
    days, memberships, context = pc._inputs(root)
    cutoff = pc._stamp("2020-06-02T11:30:00+05:30")
    original_read = pc._read_json
    reads = []
    def track(path):
        reads.append(path.name)
        return original_read(path)
    monkeypatch.setattr(pc, "_read_json", track)
    memo = {}
    args = (root, output / "feature_cache", cutoff, days, memberships, context, "fixed-signature")
    first = pc._ready_tasks(*args, audit_cache=memo)
    assert set(first) == {"AAA", "CCC"}
    assert len(reads) == 3
    second = pc._ready_tasks(*args, audit_cache=memo)
    assert second == first
    assert len(reads) == 3
    # A newly completed BBB audit becomes ready without re-reading other files.
    path = root / "data/symbol_audits/BBB.json"
    changed = original_read(path)
    changed["updated_at"] = "2020-06-02T12:30:00+05:30"
    changed["new_metadata_field"] = True
    pc.research.write_json(path, changed)
    third = pc._ready_tasks(*args, audit_cache=memo)
    assert set(third) == {"AAA", "BBB", "CCC"}
    assert len(reads) == 4
