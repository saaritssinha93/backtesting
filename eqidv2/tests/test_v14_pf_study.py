"""Runner orchestration: real native fills and cost accounting on synthetic data."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, pf_indicators, pf_study, selection


class InlineWorkers:
    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def map(self, function, tasks, **kwargs):
        return map(function, tasks)


def candidate(day, sid):
    stamp = pd.Timestamp(day + " 09:30", tz=baseline.IST)
    return dict(sid=sid, day=day, tradingsymbol="SYNTHETIC", signal_ts=stamp,
        confirmation_ts=stamp + pd.Timedelta(minutes=1), side="LONG", hhmm_int=930,
        price_change_pct=.8, volume_ratio=3., body_ratio=.8, wick_ratio=.1,
        traded_value=2e7, v9_1m_volume_ratio=1.2, trigger=100.)


def path(row):
    times = pd.date_range(row.confirmation_ts + pd.Timedelta(minutes=1),
                          row.confirmation_ts.normalize() + pd.Timedelta(hours=15, minutes=15), freq="min")
    # Four targets then one stop. Entry is always100 at the first next bar.
    loss = row.sid % 5 == 0
    return dict(timestamp_ns=times.asi8, open=np.full(len(times), 100.),
                high=np.full(len(times), 100.1 if loss else 104.),
                low=np.full(len(times), 98. if loss else 99.9), close=np.full(len(times), 100.))


def test_runner_cost_override_shared_exclusions_calendar_and_empty_arm(tmp_path, monkeypatch):
    root, source, output = (tmp_path / name for name in ("data_root", "source", "output"))
    source.mkdir()
    (root / "data").mkdir(parents=True)
    calendar = pd.bdate_range("2026-01-01", periods=31).strftime("%Y-%m-%d").tolist()
    manifest = dict(study_sessions=calendar, files=[dict(symbol="SYNTHETIC", sha256="mock-source")])
    (root / "data/manifest.json").write_text(json.dumps(manifest))
    pool = pd.DataFrame([candidate(day, sid) for sid, day in enumerate(calendar)])
    pool.to_parquet(source / "feature_ledger.parquet", index=False)
    pd.DataFrame(dict(day=calendar, nifty_first_bar_return_pct=0.)).to_csv(source / "nifty_context.csv", index=False)
    pd.DataFrame(dict(day=calendar, presence_fraction=1.)).to_csv(source / "session_data_coverage.csv", index=False)
    arms = [pf_indicators.Variant("guard_control"), pf_indicators.Variant("empty_arm")]
    monkeypatch.setattr(pf_study, "ProcessPoolExecutor", InlineWorkers)
    monkeypatch.setattr(pf_study, "verify_source", lambda root, source: manifest)
    monkeypatch.setattr(pf_study, "build_pool", lambda *args: pool.copy())
    monkeypatch.setattr(pf_study.pf_indicators, "variants", lambda: arms)
    monkeypatch.setattr(pf_study.pf_indicators, "participation_mask",
        lambda rows, variant: pd.Series(variant.name == "guard_control", index=rows.index))

    def execution(task):
        _, symbol, rows, _ = task
        paths, unknown = {}, []
        for row in rows.itertuples(index=False):
            if row.sid == 21:
                unknown.append(dict(sid=row.sid, day=row.day, symbol=symbol, setup_id=row.setup_id, reason="SYNTHETIC_MISSING_BAR"))
            else:
                paths[row.sid] = path(row)
        return paths, [], unknown

    monkeypatch.setattr(pf_study, "_execution_worker", execution)
    summary = pf_study.run(root, source, output)
    comparison = pd.read_csv(output / "variant_comparison.csv")
    control = pd.read_csv(output / "trades_guard_control.csv")
    filled = control.loc[control.portfolio_executed]
    np.testing.assert_allclose(filled.portfolio_cost_rupees, 500.)  #10bps×₹500,000.
    np.testing.assert_allclose(filled.portfolio_gross_profit_rupees - filled.portfolio_net_profit_rupees, 500.)
    metrics = comparison.loc[comparison.variant.eq("guard_control") & comparison.scope.eq("ALL_COMMON")].set_index("additional_roundtrip_bps")
    assert metrics.loc[5., "net_pnl"] == pytest.approx(metrics.loc[0., "net_pnl"] - len(filled) * 250.)
    assert metrics.loc[10., "net_pnl"] == pytest.approx(metrics.loc[0., "net_pnl"] - len(filled) * 500.)
    assert not control.day.eq(calendar[21]).any()
    empty = comparison.loc[comparison.variant.eq("empty_arm")]
    assert empty.trades.eq(0).all() and empty.net_pf.isna().all()
    assert empty.sessions.min() == 10  #31 requested minus20training minus one unknown.
    decisions = json.loads((output / "walk_forward_decisions.json").read_text())
    assert [len(row["test_days"]) for row in decisions] == [5, 5, 1]
    assert decisions[0]["test_days"] == calendar[20:25]
    assert decisions[0]["evaluated_test_days"] == [calendar[i] for i in (20, 22, 23, 24)]
    assert all(row["selected_variant"] == "guard_control" for row in decisions)
    assert summary["walk_forward_assessment"]["cost_stress"][0]["sessions"] == 10
    assert not summary["walk_forward_assessment"]["sample_sufficient"]
    assert summary["primary_roundtrip_bps"] == 10.
    assert summary["slot_selection_policy"] == selection.UNLIMITED_SELECTION
    assert json.loads((output / "next_research_candidate.json").read_text())["slot_selection_policy"] == selection.UNLIMITED_SELECTION
    assert json.loads((output / "protocol.json").read_text())["protocol"]["slot_selection_policy"] == selection.UNLIMITED_SELECTION
    assert not summary["default_profile_promoted"] and not summary["fresh_holdout"]
    assert (output / "report.html").is_file()


def test_preregister_rejects_source_or_protocol_drift(tmp_path, monkeypatch):
    source, output = tmp_path / "source", tmp_path / "output"
    source.mkdir()
    output.mkdir()
    (source / "feature_ledger.parquet").write_bytes(b"fixed-features")
    first = pf_study.preregister(source, output)
    assert first["slot_selection_policy"] == selection.UNLIMITED_SELECTION
    assert json.loads((output / "protocol.json").read_text())["source_sha256"]["selection.py"] == pf_study.sha256(Path(selection.__file__))
    assert pf_study.preregister(source, output) == first
    (source / "feature_ledger.parquet").write_bytes(b"changed-features")
    with pytest.raises(RuntimeError, match="Protocol or implementation changed"):
        pf_study.preregister(source, output)


def test_feature_worker_detects_source_change_before_reusing_cache(tmp_path):
    root, cache = tmp_path / "root", tmp_path / "cache"
    (root / "data/1m").mkdir(parents=True)
    cache.mkdir()
    (root / "data/1m/SYNTHETIC.parquet").write_bytes(b"changed-source")
    candidates = pd.DataFrame([candidate("2026-01-01", 0)])
    with pytest.raises(RuntimeError, match="changed before indicator"):
        pf_study._feature_worker((str(root), str(cache), "SYNTHETIC", candidates, "old-hash", "code-hash"))


def test_empty_base_pool_is_a_valid_zero_candidate_study(tmp_path, monkeypatch):
    root, source, output = (tmp_path / name for name in ("root", "source", "output"))
    source.mkdir()
    output.mkdir()
    (root / "data").mkdir(parents=True)
    (root / "data/manifest.json").write_text("{}")
    pool = pd.DataFrame([candidate("2026-01-01", 0)]).assign(common_feature_coverage=True)
    pool.to_parquet(source / "feature_ledger.parquet", index=False)
    monkeypatch.setattr(pf_study, "ProcessPoolExecutor", InlineWorkers)
    monkeypatch.setattr(pf_study, "diagnostic_features", lambda frame, **kwargs:
                        pd.DataFrame(dict(capacity_pass=False, anti_chase_pass=False), index=frame.index))
    result = pf_study.build_pool(root, source, output, dict(files=[]))
    assert result.empty
    assert set(pf_indicators.COMMON_FEATURES).issubset(result.columns)
