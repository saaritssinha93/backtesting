"""Synthetic staged-data integration: membership, cache integrity and gaps.

The research/feature/execution implementations run unchanged.  Only external
NIFTY reads and the process-pool transport are replaced with deterministic
local fixtures; no real market files, old strategy files or broker are used.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, research
from v14.features import IST
from v14.path_evidence import EVIDENCE_VERSION

STUDY_DAYS = ["2026-08-27", "2026-09-10", "2026-09-11"]


class _SerialPool:
    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def map(self, function, tasks, **kwargs):
        return map(function, tasks)


def _read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _make_staged(tmp_path, monkeypatch, *, defect=None):
    root = tmp_path / "v14_runtime"
    bars_dir = root / "data/1m"
    audits = root / "data/symbol_audits"
    bars_dir.mkdir(parents=True)
    audits.mkdir()
    output = root / "runs/synthetic"
    output.mkdir(parents=True)
    warmup = pd.bdate_range(end="2026-08-26", periods=20).strftime("%Y-%m-%d").tolist()
    days = warmup + STUDY_DAYS
    files = []
    for symbol, volume_scale in (("ALPHA", 1.05), ("BETA", 1.0)):
        parts = []
        for day_index, day in enumerate(days):
            ts = pd.date_range(f"{day} 09:16", periods=375, freq="min", tz=IST)
            counter = np.arange(day_index * 375, (day_index + 1) * 375, dtype=float)
            close = 100. * np.power(1.0008, counter + 1)
            opening = close / 1.0008
            volume = np.full(375, (100. if day in warmup else 1000.) * volume_scale)
            confirmation_minute = (ts.hour * 100 + ts.minute).isin([926, 931, 936, 941, 946, 951, 956, 1001, 1121])
            volume[confirmation_minute] *= 2.
            part = pd.DataFrame(dict(ts=ts, open=opening, high=close * 1.00002,
                                      low=opening * .99998, close=close, volume=volume))
            if defect == "missing_selected_path" and symbol == "ALPHA" and day == STUDY_DAYS[-1]:
                # Make the09:25 setup eligible before the09:27 gap. Later
                # cumulative-volume candidates appropriately become unavailable.
                part.loc[part.ts.dt.strftime("%H:%M").le("09:26"), "volume"] *= 5.
                part = part.loc[part.ts.dt.strftime("%H:%M").ne("09:27")]
            if defect == "missing_after_terminal" and symbol == "ALPHA" and day == STUDY_DAYS[-1]:
                part = part.loc[part.ts.dt.strftime("%H:%M").ne("12:00")]
            if defect == "outage" and day == STUDY_DAYS[-1]:
                part = part.iloc[:0]
            parts.append(part)
        minute = pd.concat(parts, ignore_index=True)
        path = bars_dir / f"{symbol}.parquet"
        minute.to_parquet(path, index=False)
        coverage = {day: dict(observed_minutes=int(minute.ts.dt.strftime("%Y-%m-%d").eq(day).sum()),
                              complete=int(minute.ts.dt.strftime("%Y-%m-%d").eq(day).sum()) == 375)
                    for day in STUDY_DAYS}
        research.write_json(audits / f"{symbol}.json", dict(coverage=coverage))
        files.append(dict(symbol=symbol, sha256=research.sha256(path)))
    memberships = {day: dict(symbols=["ALPHA", "BETA"] if day != STUDY_DAYS[1] else ["BETA"])
                   for day in STUDY_DAYS}
    universe_path = root / "data/universe_manifest.json"
    research.write_json(universe_path, dict(sessions=memberships))
    research.write_json(root / "data/manifest.json", dict(study_sessions=STUDY_DAYS, symbols=["ALPHA", "BETA"],
                        symbol_count=2, files=files, universe_sha256=research.sha256(universe_path),
                        limitations=["SYNTHETIC_TEST_FIXTURE"]))
    state = {"nifty_return": -.1, "calls": []}

    def index_loader(requested_days, **kwargs):
        state["calls"].append(list(requested_days))
        return pd.DataFrame(dict(day=requested_days, nifty_first_bar_return_pct=state["nifty_return"],
                                 quality_status="PASS_SYNTHETIC", source_sha256="test"))

    monkeypatch.setattr(research, "load_nifty_context", index_loader)
    monkeypatch.setattr(research, "ProcessPoolExecutor", _SerialPool)
    return root, output, state


def test_staged_worker_enforces_each_days_membership_without_mutating_inputs(tmp_path, monkeypatch):
    root, output, state = _make_staged(tmp_path, monkeypatch)
    original = {p: research.sha256(p) for p in (root / "data").rglob("*") if p.is_file()}
    research.preregister(output)
    pool, manifest, days = research.build_dataset(root, output)
    assert days == STUDY_DAYS
    assert state["calls"] == [STUDY_DAYS]
    assert set(pool.tradingsymbol) == {"ALPHA", "BETA"}
    assert not ((pool.tradingsymbol == "ALPHA") & (pool.day == STUDY_DAYS[1])).any()
    assert pool.loc[pool.day == STUDY_DAYS[1], "tradingsymbol"].eq("BETA").all()
    assert set(pool.day) == set(STUDY_DAYS)
    assert pool.sid.is_unique
    assert pool.common_feature_coverage.all()
    assert not {"oi", "prev_oi", "oi_change_pct"}.intersection(pool.columns)
    assert original == {p: research.sha256(p) for p in original}
    coverage = pd.read_csv(output / "session_data_coverage.csv")
    assert coverage.expected_symbols.tolist() == [2, 1, 2]
    assert coverage.presence_fraction.eq(1).all()
    assert (output / "feature_cache/ALPHA.json").is_file()
    pd.testing.assert_frame_equal(pool, pd.read_parquet(output / "feature_ledger.parquet"))
    assert _read_json(output / "protocol.json")["protocol"]["execution_authority"] is False


def test_nifty_context_change_invalidates_both_global_and_worker_caches(tmp_path, monkeypatch):
    root, output, state = _make_staged(tmp_path, monkeypatch)
    before, _, _ = research.build_dataset(root, output)
    key = _read_json(output / "feature_cache/ALPHA.json")["key"]
    state["nifty_return"] = .25
    after, _, _ = research.build_dataset(root, output)
    assert before.nifty_first_bar_return_pct.eq(-.1).all()
    assert after.nifty_first_bar_return_pct.eq(.25).all()
    assert _read_json(output / "feature_cache/ALPHA.json")["key"] != key
    pd.testing.assert_frame_equal(before.drop(columns="nifty_first_bar_return_pct"),
                                  after.drop(columns="nifty_first_bar_return_pct"))


def test_cached_dataset_rejects_changed_cash_or_universe_inputs(tmp_path, monkeypatch):
    root, output, _ = _make_staged(tmp_path, monkeypatch)
    research.build_dataset(root, output)
    source = root / "data/1m/ALPHA.parquet"
    with source.open("ab") as stream:
        stream.write(b"CHANGED")
    with pytest.raises(RuntimeError, match="Data changed since the manifest"):
        research.build_dataset(root, output)
    universe = root / "data/universe_manifest.json"
    universe.write_text("{}", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Data and universe manifests differ"):
        research.build_dataset(root, output)


@pytest.mark.parametrize("defect", ["missing_selected_path", "outage"])
def test_full_study_excludes_unknown_dates_for_every_variant_and_never_refills(tmp_path, monkeypatch, defect):
    root, output, _ = _make_staged(tmp_path, monkeypatch, defect=defect)
    original = {p: research.sha256(p) for p in (root / "data").rglob("*") if p.is_file()}
    result = research.run_study(root, output)
    coverage = _read_json(output / "evaluation_coverage.json")
    assert coverage["evaluated_days"] == STUDY_DAYS[:2]
    assert coverage["excluded_days"] == [STUDY_DAYS[-1]]
    assert result["holdout"]["sessions"] == 0
    assert not result["selection"]["selection_used_test"]
    assert result["execution_authority"] is False
    selected = pd.read_csv(output / "selected_no_oi_control.csv")
    if defect == "missing_selected_path":
        # ALPHA is chosen at09:26 before its missing first execution bar.
        # Complete BETA data cannot take its slot after that future gap is seen.
        assert ((selected.day == STUDY_DAYS[-1]) & (selected.tradingsymbol == "ALPHA")).any()
        unknown = pd.read_csv(output / "unknown_outcomes.csv")
        assert set(unknown.day) == {STUDY_DAYS[-1]}
        assert set(unknown.symbol) == {"ALPHA"}
    else:
        assert coverage["insufficient_cash_coverage_days"] == [STUDY_DAYS[-1]]
        assert pd.read_csv(output / "unknown_outcomes.csv").empty
    comparisons = pd.read_csv(output / "variant_comparison.csv")
    assert comparisons.loc[comparisons.split == "TEST", "sessions"].eq(0).all()
    for variant in research.variants():
        ledger = pd.read_csv(output / f"trades_{variant.name}.csv")
        assert not ledger.day.eq(STUDY_DAYS[-1]).any()
    assert pd.read_csv(output / "daily_results.csv").day.tolist() == STUDY_DAYS[:2]
    assert (output / "report.html").is_file()
    assert original == {p: research.sha256(p) for p in original}
    assert _read_json(root / "selected_profile.json")["execution_authority"] is False
    with pytest.raises(FileExistsError, match="immutable"):
        research.run_study(root, output)


def test_late_gap_retains_already_proven_terminal_trades_and_persists_evidence(tmp_path, monkeypatch):
    root, output, _ = _make_staged(tmp_path, monkeypatch, defect="missing_after_terminal")
    original = {p: research.sha256(p) for p in (root / "data").rglob("*") if p.is_file()}
    result = research.run_study(root, output)
    coverage = _read_json(output / "evaluation_coverage.json")
    assert coverage["evaluated_days"] == STUDY_DAYS
    assert coverage["excluded_days"] == []
    assert result["holdout"]["sessions"] == 1
    assert result["holdout"]["trades"] > 0
    assert pd.read_csv(output / "unknown_outcomes.csv").empty
    assert _read_json(output / "protocol.json")["protocol"]["path_policy"] == EVIDENCE_VERSION
    assert _read_json(output / "selected_profile.json")["path_policy"] == EVIDENCE_VERSION
    evidence = _read_json(output / "path_evidence.json")
    records = [record.get("evidence", record) for record in evidence.values()]
    after_terminal = [record for record in records if record.get("first_interruption")
                      and record["first_interruption"].get("expected_ts", "").startswith(STUDY_DAYS[-1] + " 12:00")]
    assert after_terminal
    for proof in after_terminal:
        assert proof["version"] == EVIDENCE_VERSION
        assert proof["outcome"]["exit_reason"] == "TARGET"
        assert proof["required_minutes"] < proof["continuous_valid_minutes"]
        assert not proof["synthetic_bars"]
        assert pd.Timestamp(proof["proof_available_ts"]) < pd.Timestamp(STUDY_DAYS[-1] + " 12:00", tz=IST)
        assert len(proof["prefix_sha256"]) == 64
    assert original == {p: research.sha256(p) for p in original}
    assert pd.read_csv(output / "daily_results.csv").day.tolist() == STUDY_DAYS

    # Independently generate a complete source fixture, then invoke the
    # unchanged strict full-path simulator for the same selected control book.
    # Financial fields must be identical despite the omitted post-exit candle.
    complete_root, _, _ = _make_staged(tmp_path / "complete_reference", monkeypatch)
    orders = pd.read_csv(output / "selected_no_oi_control.csv")
    paths = {}
    for symbol, group in orders.groupby("tradingsymbol"):
        minute = pd.read_parquet(complete_root / "data/1m" / f"{symbol}.parquet")
        for row in group.itertuples(index=False):
            paths[int(row.sid)] = baseline.native_paths(minute, row.confirmation_ts, row.day)
    complete = baseline.simulate(orders, paths)
    partial = pd.read_csv(output / "trades_no_oi_control.csv")
    for field in ("entry_price", "exit_price", "net_return_pct", "portfolio_net_profit_rupees",
                  "portfolio_cost_rupees", "portfolio_reserved_capital_before_rupees"):
        np.testing.assert_allclose(partial[field], complete[field], atol=1e-8, rtol=1e-12, equal_nan=True)
    for field in ("entry_ts", "exit_ts"):
        assert pd.to_datetime(partial[field], utc=True).tolist() == pd.to_datetime(complete[field], utc=True).tolist()
