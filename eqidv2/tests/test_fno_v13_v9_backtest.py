from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

import fno_v13_v9_backtest as v9


DAY = "2026-08-03"


def _signal(sid: int, *, time: str = "09:25", symbol: str | None = None,
            liquidity: float = 1_000_000_000.0, body: float = 0.8,
            wick: float = 0.1, side: str = "LONG") -> dict:
    signal = pd.Timestamp(f"{DAY} {time}", tz=v9.v5.common.IST)
    confirmation = signal + pd.Timedelta(minutes=1)
    sign = 1 if side == "LONG" else -1
    return {
        "sid": sid, "day": DAY, "tradingsymbol": symbol or f"STOCK{sid}",
        "signal_ts": signal, "confirmation_ts": confirmation,
        "hhmm_int": int(time.replace(":", "")), "side": side,
        "price_change_pct": sign * 2.0, "oi_change_pct": 2.0,
        "volume_ratio": 3.0, "body_ratio": body, "wick_ratio": wick,
        "traded_value": liquidity, "trigger": 100.0,
        "v9_5m_feature_ts": signal, "v9_1m_feature_ts": confirmation,
        "v9_5m_body_ratio": 0.8, "v9_1m_body_ratio": body,
        "v9_5m_volume_ratio": 3.0, "v9_5m_range_pct": 0.5,
        "v9_5m_distance_vwap_pct": 0.5 * sign,
        "v9_1m_range_pct": 0.2, "v9_1m_ema9": 100.0 + sign,
        "v9_1m_ema20": 100.0, "v9_5m_ema_spread_pct": 0.2 * sign,
        "v9_5m_ema9": 100.0 + sign, "v9_5m_ema20": 100.0,
        "signal_close": 100.0,
    }


def _path(row: dict, *, outcome: str = "winner", event_index: int = 1) -> dict:
    confirmation = row["confirmation_ts"]
    timestamps = pd.date_range(confirmation + pd.Timedelta(minutes=1),
        confirmation.normalize() + pd.Timedelta(hours=15, minutes=15), freq="min")
    length = len(timestamps)
    result = {"timestamp_ns": timestamps.asi8.copy(),
              "open": np.full(length, 100.0), "high": np.full(length, 100.2),
              "low": np.full(length, 99.8), "close": np.full(length, 100.0)}
    if outcome == "winner":
        i = event_index
        result["open"][i:i+2] = [100.1, 101.1]
        result["high"][i:i+2] = [101.2, 102.7]
        result["low"][i:i+2] = [100.1, 101.1]
        result["close"][i:i+2] = [101.1, 102.6]
    elif outcome == "loser":
        result["low"][event_index] = 98.0
        result["close"][event_index] = 99.0
    elif outcome == "unfilled":
        for field in ("open", "high", "low", "close"):
            result[field] -= 5
    return result


def _market():
    rows = [_signal(1, liquidity=2e9, body=0.65, wick=0.5),
            _signal(2, liquidity=1e9, body=0.9, wick=0.1),
            _signal(3, time="09:30", wick=0.1)]
    paths = {1: _path(rows[0], outcome="loser", event_index=150),
             2: _path(rows[1]), 3: _path(rows[2])}
    return pd.DataFrame(rows), paths


def test_default_exact_native_selection_exit_and_portfolio_parity():
    signals, paths = _market()
    original = v9.v5.select_orders(signals, v9.v5.profile_setups(v9.v5.PROFILES["higher_frequency"]))
    original = v9.v5.simulate_scaleout(original, paths,
        v9.v5.PROFILES["higher_frequency"].exit, cost_bps=5)
    original = v9.v5.apply_fixed_capital_model(original, 100_000, 5)
    expected_portfolio, _ = v9.v6.apply_portfolio_constraints(original,
        v9.v6.PortfolioConfig(portfolio_capital_rupees=300_000, max_positions=3))
    observed, observed_portfolio, summary = v9.evaluate(signals, paths, [DAY])
    assert original["sid"].tolist() == observed["sid"].tolist()
    assert v9.assert_control_parity(original, observed)["passed"]
    assert v9.assert_control_parity(expected_portfolio, observed_portfolio)["passed"]
    assert summary["net_profit_rupees"] == pytest.approx(expected_portfolio["portfolio_net_profit_rupees"].sum())
    assert len(v9.v5.profile_setups(v9.v5.PROFILES["higher_frequency"])) == 14
    assert observed["initial_stop_pct"].eq(1.5).all()
    assert observed["first_target_pct"].eq(1.075).all()
    assert observed["partial_pct"].eq(0.1).all()
    assert observed["runner_target_pct"].eq(2.6).all()


def test_filter_precedes_top_n_and_new_portfolio_replay_releases_capital():
    signals, paths = _market()
    control = v9.V9Config(max_positions=1)
    candidate = replace(control, name="WICK", max_1m_wick_ratio=0.35)
    _, old_ledger, _ = v9.evaluate(signals, paths, [DAY], control)
    selected, new_ledger, summary = v9.evaluate(signals, paths, [DAY], candidate)
    assert selected["sid"].tolist() == [2, 3]
    assert old_ledger.set_index("sid").loc[3, "portfolio_status"] == "REJECTED"
    assert new_ledger["portfolio_executed"].all()
    assert summary["portfolio_executed_trades"] == 2
    decisions = v9.selection_audit(signals, candidate).set_index("sid")
    assert decisions.loc[1, "v9_decision"] == "V9_FILTER_REJECTED"
    assert decisions.loc[2, "v9_decision"] == "SELECTED"
    assert decisions.loc[2, "v9_rank_in_setup_day"] == 1


def test_ranking_is_causal_stable_and_ignores_arbitrary_outcomes():
    signals, _ = _market()
    cfg = v9.V9Config(ranking="1m_body")
    expected = v9.select_orders(signals, cfg)["sid"].tolist()
    poisoned = signals.copy()
    for name in ("net_profit_rupees", "exit_price", "mfe_pct", "mae_pct", "forward_return", "filled"):
        poisoned[name] = [1e12, -1e12, np.nan]
    assert expected == [2, 3]
    assert v9.select_orders(poisoned.sample(frac=1, random_state=7), cfg)["sid"].tolist() == expected
    with pytest.raises(ValueError, match="outcome"):
        v9.V9Config(ranking="net_profit_rupees").validate()


@pytest.mark.parametrize("timeframe", ["1m", "5m"])
def test_future_or_undocumented_feature_timestamp_is_rejected(timeframe):
    signals, _ = _market()
    cfg = v9.V9Config(ranking="1m_body" if timeframe == "1m" else "5m_ema_strength")
    field = f"v9_{timeframe}_feature_ts"
    signals[field] += pd.Timedelta(minutes=1)
    with pytest.raises(ValueError, match="Noncausal"):
        v9.select_orders(signals, cfg)
    signals[field] = pd.NaT
    with pytest.raises(ValueError, match="undocumented"):
        v9.select_orders(signals, cfg)


def test_missing_feature_rejects_before_selection_instead_of_using_future_fallback():
    signals, _ = _market()
    signals.loc[0, "v9_1m_body_ratio"] = np.nan
    cfg = v9.V9Config(min_1m_body_ratio=0.6)
    assert v9.select_orders(signals, cfg)["sid"].tolist() == [2, 3]


def test_one_minute_alignment_uses_only_ema9_20_and_side():
    rows = [_signal(1, side="LONG"), _signal(2, side="SHORT")]
    signals = pd.DataFrame(rows)
    signals["v9_1m_ema50"] = [200, 0]  # Deliberately fails a three-EMA stack.
    cfg = v9.V9Config(require_1m_ema_alignment=True)
    assert set(v9.select_orders(signals, cfg)["sid"]) == {1, 2}
    signals.loc[1, "v9_1m_ema9"] = 101
    assert v9.select_orders(signals, cfg)["sid"].tolist() == [1]


def test_vwap_extension_is_signed_for_shorts():
    signals = pd.DataFrame([_signal(1, side="SHORT"), _signal(2, side="SHORT")])
    signals["v9_5m_distance_vwap_pct"] = [-2.0, 2.0]
    cfg = v9.V9Config(max_signed_5m_vwap_extension_pct=1.5)
    assert v9.select_orders(signals, cfg)["sid"].tolist() == [2]


def test_five_minute_ema_rank_uses_9_20_spread_over_close():
    signals, _ = _market()
    signals.loc[0, "v9_5m_ema9"] = 100.5
    signals.loc[1, "v9_5m_ema9"] = 102
    signals["v9_5m_ema_spread_pct"] = [10000, -10000, 10000]
    decisions = v9.selection_audit(signals, v9.V9Config(ranking="5m_ema_strength"))
    assert decisions.loc[decisions["v9_selected"], "sid"].tolist() == [2, 3]
    assert decisions.set_index("sid").loc[2, "v9_rank_score"] == pytest.approx(2)


@pytest.mark.parametrize("mutation", ["missing_path", "gap", "confirmation_bar", "no_cutoff", "invalid_ohlc"])
def test_incomplete_or_mistimed_execution_paths_fail_closed(mutation):
    signals, paths = _market()
    if mutation == "missing_path":
        del paths[1]
    elif mutation == "gap":
        for field in paths[1]:
            paths[1][field] = np.delete(paths[1][field], 20)
    elif mutation == "confirmation_bar":
        paths[1]["timestamp_ns"] -= pd.Timedelta(minutes=1).value
    elif mutation == "no_cutoff":
        for field in paths[1]:
            paths[1][field] = paths[1][field][:-1]
    else:
        paths[1]["high"][0] = 90
    with pytest.raises(RuntimeError, match="sid=1"):
        v9.evaluate(signals, paths, [DAY])


def test_no_selections_and_no_fills_produce_valid_zero_results():
    signals, paths = _market()
    _, ledger, summary = v9.evaluate(signals, paths, ["2026-08-04"])
    assert ledger.empty
    assert summary["sessions"] == 1
    assert summary["net_profit_rupees"] == 0
    paths = {int(row["sid"]): _path(row, outcome="unfilled") for row in signals.to_dict("records")}
    audit, ledger, summary = v9.evaluate(signals, paths, [DAY])
    assert not audit["filled"].any()
    assert ledger["portfolio_status"].eq("SOURCE_UNFILLED").all()
    assert summary["net_profit_rupees"] == 0


def test_entry_window_remains_confirmation_plus_ten_minutes():
    row = _signal(1)
    path = _path(row, outcome="unfilled")
    for field in ("open", "high", "low", "close"):
        path[field][10:] += 5  # First touch confirmation+11; too late.
    audit = v9.replay_candidates(pd.DataFrame([row]), {1: path})
    assert not audit.loc[0, "filled"]
    for field in ("open", "high", "low", "close"):
        path[field][9] += 5  # Confirmation+10 is inclusive.
    audit = v9.replay_candidates(pd.DataFrame([row]), {1: path})
    assert audit.loc[0, "filled"]
    assert audit.loc[0, "entry_path_index"] == 9


def test_source_drift_and_parity_drift_fail_loudly(monkeypatch):
    assert v9.validate_configuration() == v9.EXPECTED_SOURCE_HASHES
    monkeypatch.setattr(v9, "source_hashes", lambda: {**v9.EXPECTED_SOURCE_HASHES,
        "fno_v13_corrected_v5_backtest.py": "changed"})
    with pytest.raises(RuntimeError, match="Immutable V13 source drift"):
        v9.validate_configuration()
    signals, paths = _market()
    original = v9.replay_candidates(v9.select_orders(signals), paths)
    changed = original.copy()
    changed.loc[0, "net_profit_rupees"] += 0.01
    with pytest.raises(RuntimeError, match="net_profit_rupees"):
        v9.assert_control_parity(original, changed)


def test_cost_and_1x_comparisons_preserve_trade_sets():
    signals, paths = _market()
    original, _, s5 = v9.evaluate(signals, paths, [DAY])
    one, _, s1 = v9.evaluate(signals, paths, [DAY], v9.V9Config(leverage_factor=1))
    stressed, _, s9 = v9.evaluate(signals, paths, [DAY], v9.V9Config(cost_bps=9))
    assert original["sid"].tolist() == one["sid"].tolist() == stressed["sid"].tolist()
    assert s1["net_profit_rupees"] == pytest.approx(s5["net_profit_rupees"] / 5)
    assert s5["net_profit_rupees"] - s9["net_profit_rupees"] == pytest.approx(2 * 500_000 * 4 / 10_000)


def test_registry_contains_only_eight_independent_selection_changes():
    configs = v9.experiment_configs()
    assert len(configs) == 8
    for cfg in configs.values():
        cfg.validate()
        assert cfg.maximum_holding_minutes is None
        assert cfg.uses_features
