from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import pullbacks

ARCHIVE = Path(r"C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1")


def _original_monitor():
    source = Path(__file__).resolve().parents[1] / "research/g3_pullback_monitor.py"
    names = {"features", "warning_features", "label_outcome", "make_predictions", "alert_episodes"}
    tree = ast.parse(source.read_text(encoding="utf-8"))
    node = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[])
    scope = {"pd": pd, "np": np, "SPECS": {5: (.30, (.20, .40)), 30: (.50, (.40, .75))}}
    exec(compile(node, str(source), "exec"), scope)
    return scope


def test_warning_features_are_past_only_and_exact_original_math():
    times = pd.date_range("2026-10-08 09:16", periods=80, freq="min", tz=pullbacks.IST)
    close = 100. + np.sin(np.arange(80) / 6)
    minute = pd.DataFrame(dict(ts=times, open=close + .1, high=close + .3, low=close - .3,
                               close=close, volume=np.arange(80) + 100.))
    original = _original_monitor()
    result = pullbacks.features(minute)
    pd.testing.assert_frame_equal(result, original["features"](minute))
    changed = minute.copy()
    changed.loc[changed.index > 50, ["open", "high", "low", "close", "volume"]] *= 2
    pd.testing.assert_frame_equal(result.iloc[:51], pullbacks.features(changed).iloc[:51])
    for sign in (1, -1):
        assert pullbacks.warning_features(result.iloc[50], sign, 101.) == original["warning_features"](result.iloc[50], sign, 101.)


def test_unknown_exit_bar_order_is_not_a_resolved_failure():
    decision = pd.Timestamp("2026-10-08 10:00", tz=pullbacks.IST)
    minute = pd.DataFrame(dict(ts=[decision + pd.Timedelta(minutes=1)],
        open=[100.], high=[102.], low=[99.], close=[100.5], volume=[100.]))
    outcome = pullbacks.label_outcome(minute, decision, 100., 1, 5, .3,
        decision + pd.Timedelta(minutes=1), decision + pd.Timedelta(minutes=1), 101., "INTRABAR")
    assert not outcome["event"]
    assert not outcome["outcome_available"]
    assert outcome["unknown_reason"] == "UNKNOWN_INTRABAR_ORDER"


def test_unfilled_or_missing_context_keeps_trades_unchanged():
    ledger = pd.DataFrame(dict(sid=[1], day=["2026-10-08"], tradingsymbol=["CASH"], setup_id=["0931_LONG"],
        side=["LONG"], entry_ts=["2026-10-08 09:32+05:30"], exit_ts=["2026-10-08 10:00+05:30"],
        exit_bar_end_ts=["2026-10-08 10:00+05:30"], exit_event=["INTRABAR"],
        entry_price=[100.], exit_price=[101.], portfolio_executed=[True], portfolio_net_profit_rupees=[4750.]))
    result, observations = pullbacks.annotate_trades(ledger, {})
    pd.testing.assert_frame_equal(result[ledger.columns], ledger)
    assert result.monitor_status.iloc[0] == "MONITOR_UNAVAILABLE"
    assert result.monitor_unavailable_reason.iloc[0] == "MISSING_SYMBOL_MINUTE_CONTEXT"
    assert observations.empty
    assert pullbacks.summarize(observations).grid_windows.sum() == 0
    ledger["portfolio_executed"] = False
    result, observations = pullbacks.annotate_trades(ledger, {})
    assert result.monitor_status.iloc[0] == "NOT_EXECUTED"


@pytest.mark.skipif(not ARCHIVE.exists(), reason="Local sealed G-3 monitoring context unavailable")
def test_real_trade_monitor_matches_original_grids_and_warning_episodes():
    archive = pd.read_csv(ARCHIVE / "trades.csv")
    executed = archive.loc[archive.portfolio_executed.eq(True)]
    # One long and one short with a substantial observable path.
    trades = executed.sort_values("holding_minutes", ascending=False).groupby("side").head(1).reset_index(drop=True)
    minutes = pd.read_parquet(ARCHIVE / "minute_context.parquet")
    wanted = set(zip(trades.day.astype(str), trades.tradingsymbol))
    minutes = minutes.loc[[tuple(pair) in wanted for pair in zip(minutes.day.astype(str), minutes.tradingsymbol)]]
    sources = {str(symbol): group.drop_duplicates("ts").sort_values("ts")
               for symbol, group in minutes.groupby("tradingsymbol")}
    observed, windows = pullbacks.annotate_trades(trades, sources)
    pd.testing.assert_frame_equal(observed[trades.columns], trades)
    reference = _original_monitor()
    dense, grid, details = reference["make_predictions"](trades, minutes, sorted(trades.day.unique()))
    grid = grid.loc[((grid.horizon_minutes.eq(5) & grid.threshold_pct.eq(.30))
                     | (grid.horizon_minutes.eq(30) & grid.threshold_pct.eq(.50)))]
    alerts = reference["alert_episodes"](dense)
    keys = ["trade_id", "decision_ts", "horizon_minutes"]
    columns = keys + ["warning", "event", "outcome_available", "max_future_adverse_pct"]
    actual_grid = windows.loc[windows.is_grid, columns].sort_values(keys).reset_index(drop=True)
    expected_grid = grid[columns].sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_grid, expected_grid)
    actual_alerts = windows.loc[windows.emitted_warning, columns].sort_values(keys).reset_index(drop=True)
    expected_alerts = alerts[columns].sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_alerts, expected_alerts)
    assert observed.monitoring_minutes.tolist() == details.monitoring_minutes.tolist()
