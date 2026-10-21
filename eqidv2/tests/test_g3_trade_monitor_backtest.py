"""Causal exits, original event priority, allocation and sealed report integrity."""
from datetime import date, timedelta
import json

import numpy as np
import pandas as pd
import pytest

from research import g3_trade_monitor_backtest as monitor


DAY = date(2026, 10, 13)


def inputs(count=1, side="LONG", profit=False):
    stamp = pd.Timestamp(f"{DAY} 12:00", tz="Asia/Kolkata")
    timestamps = pd.date_range(stamp + pd.Timedelta(minutes=1),
        pd.Timestamp(f"{DAY} 15:15", tz="Asia/Kolkata"), freq="min")
    orders, paths = [], {}
    for sid in range(count):
        prices = np.full(len(timestamps), 100.)
        if sid == 0:
            values = [100., 100.1, 100.7, 100.6, 100.5, 100.4, 100.25, 100.2] if profit else [100., 100.02, 99.96, 99.88, 99.80, 99.71, 99.60, 99.55]
            prices[:len(values)] = values
            prices[len(values):] = values[-1]
        if side == "SHORT":
            prices = 200-prices
        opening = prices.copy()
        if sid == 0:
            opening[7] = 100.22 if profit else 99.57
            if side == "SHORT":
                opening[7] = 200-opening[7]
        paths[sid] = dict(timestamp_ns=timestamps.asi8, open=opening,
            high=np.maximum(opening, prices)+.01, low=np.minimum(opening, prices)-.01, close=prices)
        if sid == 10:
            paths[sid] = {key: value[10:] for key, value in paths[sid].items()}
        orders.append(dict(sid=sid, day=str(DAY), signal_id=f"source-{sid}", setup_id="1331_"+side,
            tradingsymbol=f"EXAMPLE{sid:02}", side=side,
            confirmation_ts=stamp + pd.Timedelta(minutes=10 if sid == 10 else 0), trigger=100.,
            picker="max_liquidity", traded_value=1000-sid, native_target_pct=1.17, native_stop_pct=1.25))
    orders = pd.DataFrame(orders)
    baseline = monitor.history._simulate(orders, paths, [DAY], monitor.history.contract)
    return orders, paths, baseline


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_shared_trend_rule_uses_six_full_bars_then_next_open(side):
    _, paths, baseline = inputs(side=side)
    first, signals = monitor.trade_advisories(baseline.iloc[0], paths[0])
    event = first["TREND_FAILURE"]
    assert event["execution_path_index"] == 7
    assert event["completed_bar_count"] == 6
    assert event["decision_at_ist"].startswith(f"{DAY}T12:07")
    assert event["modeled_execution_at_ist"].startswith(f"{DAY}T12:08")
    assert event["next_bar_open_price"] == paths[0]["open"][7]
    assert event["next_bar_open_price"] != paths[0]["close"][6]
    ledgers, _ = monitor.evaluate_policies(baseline, paths)
    actual = ledgers["trend_failure"].iloc[0]
    assert actual.exit_price == event["next_bar_open_price"]
    assert pd.Timestamp(actual.exit_execution_ts) == pd.Timestamp(event["next_bar_open_at_ist"])
    assert pd.Timestamp(actual.exit_ts) - pd.Timestamp(actual.exit_execution_ts) == pd.Timedelta(minutes=1)
    assert actual.cost_pct == .05
    assert actual.strategy_fingerprint == monitor.fingerprint()
    assert actual.source_strategy_fingerprint == monitor.history.contract.strategy_fingerprint()
    assert actual.execution_authority == False


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_profit_giveback_shared_rule_cuts_only_after_three_adverse_closes(side):
    _, paths, baseline = inputs(side=side, profit=True)
    first, _ = monitor.trade_advisories(baseline.iloc[0], paths[0])
    assert first["PROFIT_GIVEBACK"]["execution_path_index"] == 7
    assert "TREND_FAILURE" not in first


@pytest.mark.parametrize("reason", ["STOP", "TARGET", "SQUARE_OFF"])
def test_original_exit_at_or_before_actual_open_has_precedence(reason):
    _, paths, baseline = inputs()
    row = baseline.iloc[0].copy()
    row["exit_ts"] = pd.Timestamp(f"{DAY} 12:07", tz="Asia/Kolkata")
    row["exit_reason"] = reason
    first, signals = monitor.trade_advisories(row, paths[0])
    assert first == {}
    assert signals == []


def test_execution_bar_future_extreme_does_not_enter_advisory_or_excursions():
    _, paths, baseline = inputs()
    first, _ = monitor.trade_advisories(baseline.iloc[0], paths[0])
    paths[0]["high"][7] = 120.
    second, _ = monitor.trade_advisories(baseline.iloc[0], paths[0])
    assert second["TREND_FAILURE"] == first["TREND_FAILURE"]
    raw = baseline.copy()
    monitor._apply_monitor_exit(raw, 0, first["TREND_FAILURE"], paths[0], "trend_failure")
    assert raw.loc[0, "mfe_pct"] < 1


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("future_event", ["STOP", "TARGET"])
def test_recomputed_native_intrabar_outcome_cannot_cancel_pending_open_exit(side, future_event):
    orders, paths, baseline = inputs(side=side)
    before = monitor.evaluate_policies(baseline, paths)[0]["trend_failure"].iloc[0]
    favorable = future_event == "TARGET"
    upper = favorable == (side == "LONG")
    paths[0]["high" if upper else "low"][7] = 102.0 if upper else 98.0
    # Recompute native outcomes: testing against the old baseline hides the leak.
    regenerated = monitor.history._simulate(orders, paths, [DAY], monitor.history.contract)
    assert regenerated.iloc[0].exit_reason == future_event
    assert regenerated.iloc[0].exit_event == "INTRABAR"
    after = monitor.evaluate_policies(regenerated, paths)[0]["trend_failure"].iloc[0]
    assert after.monitor_exit
    assert after.exit_price == before.exit_price
    assert after.exit_execution_ts == before.exit_execution_ts
    assert after.portfolio_net_profit_rupees == pytest.approx(before.portfolio_net_profit_rupees)
    assert after.mfe_pct == pytest.approx(before.mfe_pct)
    assert after.mae_pct == pytest.approx(before.mae_pct)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_gap_stop_at_open_retains_native_priority(side):
    orders, paths, _ = inputs(side=side)
    is_long = side == "LONG"
    paths[0]["open"][7] = 98.5 if is_long else 101.5
    paths[0]["low" if is_long else "high"][7] = 98.4 if is_long else 101.6
    baseline = monitor.history._simulate(orders, paths, [DAY], monitor.history.contract)
    native = baseline.iloc[0]
    assert native.exit_event == "OPEN"
    actual = monitor.evaluate_policies(baseline, paths)[0]["trend_failure"].iloc[0]
    assert not actual.monitor_exit
    assert actual.exit_price == native.exit_price
    assert actual.exit_ts == native.exit_ts


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_pending_market_target_gap_convention_ignores_later_stop_first_outcome(side):
    orders, paths, _ = inputs(side=side)
    is_long = side == "LONG"
    opening = 102.0 if is_long else 98.0
    paths[0]["open"][7] = opening
    paths[0]["high" if is_long else "low"][7] = 102.1 if is_long else 97.9
    target_baseline = monitor.history._simulate(orders, paths, [DAY], monitor.history.contract)
    assert target_baseline.iloc[0].exit_reason == "TARGET"
    first = monitor.evaluate_policies(target_baseline, paths)[0]["trend_failure"].iloc[0]
    paths[0]["low" if is_long else "high"][7] = 98.0 if is_long else 102.0
    stop_baseline = monitor.history._simulate(orders, paths, [DAY], monitor.history.contract)
    assert stop_baseline.iloc[0].exit_reason == "STOP"
    second = monitor.evaluate_policies(stop_baseline, paths)[0]["trend_failure"].iloc[0]
    assert first.monitor_exit and second.monitor_exit
    assert first.exit_price == second.exit_price == opening
    assert first.portfolio_net_profit_rupees == pytest.approx(second.portfolio_net_profit_rupees)


def test_partial_entry_bar_is_excluded_and_missing_interval_blocks_rule():
    _, paths, baseline = inputs()
    row = baseline.iloc[0].copy()
    row["entry_ts"] += pd.Timedelta(seconds=30)
    first, _ = monitor.trade_advisories(row, paths[0])
    assert first["TREND_FAILURE"]["execution_path_index"] >= 8
    # The six-bar trend cannot exist if the first decline spans a missing candle.
    shortened = {key: np.delete(value, 4) for key, value in paths[0].items()}
    first, _ = monitor.trade_advisories(baseline.iloc[0], shortened)
    assert "TREND_FAILURE" not in first


def test_released_capital_admits_previously_rejected_native_fill():
    _, paths, baseline = inputs(count=11)
    assert baseline.portfolio_executed.sum() == 10
    assert baseline.loc[baseline.sid.eq(10), "filled"].item()
    assert not baseline.loc[baseline.sid.eq(10), "portfolio_executed"].item()
    ledgers, _ = monitor.evaluate_policies(baseline, paths)
    assert ledgers["trend_failure"].portfolio_executed.sum() == 11
    assert ledgers["trend_failure"].loc[lambda f:f.sid.eq(10), "portfolio_executed"].item()
    assert ledgers["baseline"].portfolio_executed.sum() == 10
    assert len(ledgers["combined"]) == 11


def test_changed_shared_thresholds_cannot_silently_change_fixed_study(monkeypatch):
    monkeypatch.setitem(monitor.rules.MONITOR_CONFIG, "minimum_bars", 5)
    with pytest.raises(ValueError, match="preregistered"):
        monitor.configuration()


@pytest.mark.parametrize('mismatched_retained_path', [False, True])
def test_current_0926_reconstruction_preserves_retained_paths_and_adds_new_selection(
        monkeypatch, mismatched_retained_path):
    from research import g3_0926_volume_backtest as volume

    days = [DAY-timedelta(days=1), DAY]
    original = pd.DataFrame([
        dict(sid=1, day=str(days[0]), setup_id='0926_LONG'),
        dict(sid=2, day=str(days[1]), setup_id='0951_LONG'),
    ])
    added = pd.DataFrame([dict(sid=3, day=str(days[1]), setup_id='0926_LONG')])
    current = pd.concat([original, added], ignore_index=True)
    revised = pd.concat([original.iloc[:1], added], ignore_index=True)
    original_paths = {
        sid: dict(timestamp_ns=np.array([sid], dtype=np.int64), close=np.array([100.+sid]))
        for sid in (1, 2)
    }
    retained_path = {key: value.copy() for key, value in original_paths[1].items()}
    if mismatched_retained_path:
        retained_path['close'][0] += 1.
    added_path = dict(timestamp_ns=np.array([3], dtype=np.int64), close=np.array([103.]))
    proof = dict(threshold_5m_volume_ratio=1.2, revised_selected=2)

    monkeypatch.setattr(monitor.history, '_baseline',
        lambda root: (original, None, days, [], {'baseline': 'sealed'}))
    monkeypatch.setattr(monitor.history, '_revised_0941_orders',
        lambda *args: (original.iloc[:0], {}, {}))
    monkeypatch.setattr(monitor.history, '_historical_paths',
        lambda requested_days: (original_paths, {'historical': 'sealed'}))

    def reselect(orders, requested_days):
        pd.testing.assert_frame_equal(orders, original)
        assert requested_days == days
        return revised, {1: retained_path, 3: added_path}, proof, pd.DataFrame()

    monkeypatch.setattr(volume, 'reselect', reselect)
    if mismatched_retained_path:
        with pytest.raises(ValueError, match='09:26 execution path differs'):
            monitor._paths_for_source(None, current, days)
    else:
        paths, provenance = monitor._paths_for_source(None, current, days)
        assert set(paths) == {1, 2, 3}
        assert paths[1] is original_paths[1]
        assert paths[3] is added_path
        assert provenance['revised_0926_long_volume'] == proof


@pytest.mark.parametrize('session_count', [50, 52])
def test_end_to_end_research_package_roundtrip_and_tamper_detection(tmp_path, monkeypatch, session_count):
    orders, paths, baseline = inputs(count=11)
    days = [DAY-timedelta(days=session_count-1-i) for i in range(session_count)]
    source = tmp_path / "canonical" / "run"
    source.mkdir(parents=True)
    orders.to_csv(source / "selected_orders.csv", index=False)
    baseline.to_csv(source / "portfolio_trades.csv", index=False)
    (source / "manifest.json").write_text("{}")
    summary = dict(strategy_version=monitor.history.contract.STRATEGY_VERSION,
        strategy_fingerprint=monitor.history.contract.strategy_fingerprint(), sessions=session_count,
        active_setup_count=len(monitor.history.contract.ACTIVE_SETUPS), days=[str(d) for d in days])
    monkeypatch.setattr(monitor, "_resolve_source", lambda _: (source.parent, source, summary, {}))
    monkeypatch.setattr(monitor, "_paths_for_source", lambda *args: (paths, {"test": "frozen actual minute fixture"}))
    monkeypatch.setattr(monitor.history, "verified_package", lambda *args: (summary, {}))
    root = tmp_path / "monitor"
    directory, actual = monitor.run(source.parent, root)
    loaded = monitor.load_latest(root)
    assert isinstance(loaded["policy_results"], list)
    assert len(loaded["policy_results"]) == (12 if session_count == 50 else 16)
    assert len(loaded["daily_comparison"]) == session_count * 4
    assert actual['evaluation_last_date'] == str(days[49])
    assert actual['daily_extension_sessions'] == session_count - 50
    if session_count > 50:
        assert 'Later daily extensions: fixed policies' in (directory / 'report.html').read_text(encoding='utf-8')
    assert actual["subminute_ohlc_fabricated"] is False
    assert actual["configuration_promoted"] is False
    assert actual["baseline_native_parity"] is True
    assert "not untouched strategy holdout" in (directory / "report.html").read_text(encoding="utf-8")
    with (directory / "signals.csv").open("a") as handle:
        handle.write("tampered")
    with pytest.raises(ValueError, match="hash|changed|integrity"):
        monitor.load_latest(root)
