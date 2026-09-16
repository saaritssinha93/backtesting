"""Causal selection parity and rejection boundaries for retained G live routing."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_live_config as config


def candidate(symbol="CORE", price=-.25, **overrides):
    values = dict(tradingsymbol=symbol, side="SHORT", signal_end="09:30",
                  signal_timestamp="2026-09-11T09:30:00+05:30",
                  confirmation_timestamp="2026-09-11T09:31:00+05:30",
                  v9_1m_feature_ts="2026-09-11T09:31:00+05:30",
                  signal_close=100., price_change_pct=price, oi_change_pct=.3,
                  oi=100300., prev_oi=100000., ema9=99., ema20=100., ema50=101.,
                  volume_ratio=2., body_ratio=.6, wick_ratio=.1, traded_value=100000.,
                  v9_1m_volume_ratio=1.2, confirmed=True)
    values.update(overrides)
    return values


def test_setup_book_and_exits_match_active_backtest():
    import fno_v13_v10_g_backtest as backtest
    settings = config.load_frozen_config()
    change = backtest.SelectionChange(**settings["selection_change"])
    originals = backtest.v9.v5.profile_setups(backtest.v9.v5.PROFILES["higher_frequency"])
    by_id = {setup.setup_id: setup for setup in config.ACTIVE_SETUPS}
    for original in originals:
        _, expected = backtest.setup_pair(original, change)
        live = by_id[expected.setup_id]
        for field in ("signal_end", "confirmation_end", "side", "max_entries", "picker",
                      "price_change_pct", "oi_change_pct", "volume_ratio", "body_ratio",
                      "max_wick_ratio", "min_traded_value"):
            assert getattr(live, field) == getattr(expected, field)
        assert dict(stop_pct=live.stop_pct, target_pct=live.target_pct) == settings["exit"]["setups"][live.setup_id]
    assert len(by_id) == 14
    assert config.setup_for("09:50", "LONG") is None
    assert config.setup_for("09:55", "SHORT") is None
    assert config.setup_for("10:00", "SHORT") is None


def test_core_precedes_more_liquid_expanded_candidate():
    setup = config.setup_for("09:35", "SHORT")
    common = dict(signal_end="09:35", signal_timestamp="2026-09-11T09:35:00+05:30",
                  confirmation_timestamp="2026-09-11T09:36:00+05:30",
                  v9_1m_feature_ts="2026-09-11T09:36:00+05:30", oi_change_pct=.6)
    core = candidate("CORE", price=-.55, **common)
    expansion_a = candidate("EXPANSION_A", price=-.4, traded_value=3e6, **common)
    expansion_b = candidate("EXPANSION_B", price=-.4, traded_value=2e6, **common)
    ranked = config.rank_candidates([expansion_b, expansion_a, core], setup)
    assert [r["tradingsymbol"] for r in ranked] == ["CORE", "EXPANSION_A"]
    assert [r["v10_g_f_core"] for r in ranked] == [True, False]


@pytest.mark.parametrize("value", [None, np.nan, np.inf, -np.inf, 1.199999])
def test_confirmation_volume_fails_closed_before_top_n(value):
    row = candidate("BIG", price=-.8, traded_value=1e9, v9_1m_volume_ratio=value)
    ranked = config.rank_candidates([row, candidate()], config.setup_for("09:30", "SHORT"))
    assert [r["tradingsymbol"] for r in ranked] == ["CORE"]


@pytest.mark.parametrize("field,value", [
    ("v9_1m_feature_ts", "2026-09-11T09:32:00+05:30"),
    ("v9_1m_feature_ts", "2026-09-11T09:30:00+05:30"),
    ("confirmation_timestamp", "2026-09-11T09:32:00+05:30"),
    ("signal_timestamp", "2026-09-10T09:30:00+05:30"),
    ("signal_end", "09:35"), ("ema9", np.inf), ("oi_change_pct", 1.000001),
    ("prev_oi", 100301.), ("confirmed", False), ("body_ratio", np.nan),
])
def test_noncausal_or_invalid_candidate_cannot_be_selected(field, value):
    row = candidate(**{field: value})
    assert config.rank_candidates([row], config.setup_for("09:30", "SHORT")) == []


def test_nifty_gate_is_exact_first_bar_and_only_first_short_slot():
    row = candidate()
    assert config.base_signal_side(row, "09:25") is None
    assert config.base_signal_side(row, "09:25", -.05) == "SHORT"
    assert config.base_signal_side(row, "09:25", -.04999) is None
    assert config.base_signal_side(row, "09:30") == "SHORT"
    frame = pd.DataFrame(dict(timestamp=["2026-09-11T09:20:00+05:30", "2026-09-11T09:25:00+05:30"],
                              open=[100., 100.], close=[99.9, 105.]))
    assert config.nifty_context_from_bars(frame, date(2026, 9, 11)) == pytest.approx(-.1)
    assert np.isnan(config.nifty_context_from_bars(frame, date(2026, 9, 10)))


def test_volume_snapshot_excludes_current_and_future_and_has_prior_day_history():
    stamps = pd.date_range("2026-09-10T15:01:00+05:30", periods=15, freq="min").append(
        pd.date_range("2026-09-11T09:16:00+05:30", periods=16, freq="min"))
    history = pd.DataFrame(dict(ts=stamps, volume=100.))
    bar = dict(timestamp="2026-09-11T09:20:00+05:30", volume=120.)
    history.loc[history.ts.ge(pd.Timestamp(bar["timestamp"])), "volume"] = 1e9
    observed = config.annotate_confirmation_volume(bar, history)
    assert observed["v9_1m_volume_ratio"] == 1.2
    assert observed["confirmation_prior_volume_count"] == 19
    assert observed["confirmation_prior_volume_last_ts"] == "2026-09-11T09:19:00+05:30"
    changed = history.copy()
    changed.loc[changed.ts.ge(pd.Timestamp(bar["timestamp"])), "volume"] *= 3
    assert config.annotate_confirmation_volume(bar, changed) == observed


def test_volume_snapshot_uses_last20_and_requires_min5():
    history = pd.DataFrame(dict(ts=pd.date_range("2026-09-11T09:01:00+05:30", periods=30, freq="min"),
                                volume=np.arange(1, 31)))
    bar = dict(timestamp="2026-09-11T09:31:00+05:30", volume=41.)
    assert config.annotate_confirmation_volume(bar, history)["v9_1m_volume_ratio"] == 2.
    assert config.annotate_confirmation_volume(bar, history.iloc[-4:])["v9_1m_volume_ratio"] is None


def test_confirmation_requires_exact_next_minute_and_volume():
    row = candidate()
    bar = dict(timestamp="2026-09-11T09:31:00+05:30", open=100., high=100.1, low=99., close=99.2,
               volume=120., v9_1m_volume_ratio=1.2, v9_1m_feature_ts="2026-09-11T09:31:00+05:30")
    valid = config.confirmation_metrics(row, bar)
    assert valid["confirmed"]
    assert valid["trigger"] == 99.
    assert valid["body_ratio"] == pytest.approx(.8 / 1.1)
    assert config.confirmation_metrics(row, {**bar, "v9_1m_volume_ratio": None})["confirmed"] is False
    assert config.confirmation_metrics(row, {**bar, "timestamp": "2026-09-11T09:32:00+05:30"})["confirmed"] is False
    assert config.confirmation_metrics(row, {**bar, "high": 99.5})["confirmed"] is False
    assert config.confirmation_metrics(row, {**bar, "close": 100.05})["confirmed"] is False


def test_execution_capital_deadlines_and_pins(tmp_path):
    assert config.size_position(250., 1, live=True).quantity == 2000
    assert config.PORTFOLIO_CAPITAL_RS == 1e6
    assert config.MAX_POSITIONS is None
    day = date(2026, 9, 11)
    assert config.activation_deadline(day, "09:26").strftime("%H:%M:%S") == "09:36:00"
    assert config.confirmation_deadline(day, "09:26").strftime("%H:%M:%S") == "09:27:30"
    assert config.SQUARE_OFF == "15:15"
    altered = tmp_path / "altered.json"
    altered.write_text('{}')
    with pytest.raises(ValueError, match="configuration"):
        config.load_frozen_config(altered)
    assert config.attest_selected_backtest()["fills"] == 66


def test_selection_ignores_outcome_fields():
    setup = config.setup_for("09:30", "SHORT")
    rows = [candidate("B", price=-.3), candidate("A", price=-.3)]
    expected = [r["tradingsymbol"] for r in config.rank_candidates(rows, setup)]
    for i, row in enumerate(rows):
        row.update(net_return_pct=1e9 * i, portfolio_executed=not i, future_high=1e12)
    assert [r["tradingsymbol"] for r in config.rank_candidates(rows, setup)] == expected == ["A"]


@pytest.mark.parametrize("name,value", [
    ("LIVE_CONFIRMATION_VOLUME_REQUIRED_PRIOR", 19),
    ("LIVE_CONFIRMATION_HISTORY_DAYS", 6),
    ("LIVE_CONFIRMATION_HISTORY_FALLBACK_DAYS", 30),
    ("LIVE_CONFIRMATION_FIRST_MINUTE_END", "09:15"),
    ("LIVE_CONFIRMATION_LAST_MINUTE_END", "15:31"),
])
def test_live_warmup_policy_is_explicit_and_fingerprint_locked(monkeypatch, name, value):
    payload = config.strategy_payload()["live_confirmation_volume_warmup"]
    assert payload["required_prior_observations"] == 20
    assert payload["research_min_periods"] == 5
    assert payload["insufficient_history_action"] == "FAIL_CLOSED"
    fingerprint = config.strategy_fingerprint()
    monkeypatch.setattr(config, name, value)
    assert config.strategy_fingerprint() != fingerprint


def test_all31_sessions_selection_parity_with_frozen_backtest():
    import fno_v13_v10_g_backtest as backtest
    dataset_path = backtest.DEFAULT_SOURCE / "dataset/signals.parquet"
    if not dataset_path.is_file():
        pytest.skip("Frozen research dataset unavailable in this environment")
    source = pd.read_parquet(dataset_path)
    settings = config.load_frozen_config()
    base = backtest.v9.V9Config(portfolio_capital_rupees=1e6, capital_per_entry_rupees=1e5,
                               leverage_factor=5., max_positions=None)
    expected = backtest.select_orders(source, base, backtest.SelectionChange(**settings["selection_change"]))
    live = source.copy()
    for dst, src in (("ema9", "v9_5m_ema9"), ("ema20", "v9_5m_ema20"), ("ema50", "v9_5m_ema50"),
                     ("signal_timestamp", "signal_ts"), ("confirmation_timestamp", "confirmation_ts")):
        live[dst] = live[src]
    live["signal_end"] = pd.to_datetime(live.signal_ts).dt.strftime("%H:%M")
    live["confirmed"] = True  # Dataset is the native strict confirmation superset.
    selected = []
    for (_, clock), group in live.groupby(["day", "signal_end"], sort=False):
        for side in ("LONG", "SHORT"):
            setup = config.setup_for(clock, side)
            if setup:
                selected.extend((int(r["sid"]), setup.setup_id) for r in config.rank_candidates(group.to_dict("records"), setup))
    assert len(selected) == 73
    assert set(selected) == set(zip(expected.sid.astype(int), expected.setup_id))
