"""Regression checks for observer counts, abstention states and frozen P&L."""
import math

import pandas as pd
import pytest

from research.g3_daywise_backtest import (
    build_daily_results,
    build_trade_results,
    detail_rows,
    horizon_stats,
    local_time,
)


DAY = "2026-07-29"
META = {"train_days": [DAY, "2026-07-30"], "check_days": [], "evaluation_days": []}


def prediction(**changes):
    row = dict(
        trade_id=f"{DAY}|setup|TEST|1", day=DAY, tradingsymbol="TEST", side="LONG",
        horizon_minutes=5, threshold_pct=.30, decision_close=100.,
        decision_ts=pd.Timestamp(DAY + "T04:15:00Z"),
        event_ts=pd.Timestamp(DAY + "T04:17:00Z"),
        is_grid=True, outcome_available=True, event=False,
        research_watch=False, alert_RESEARCH_WATCH=False,
        alert_HIGH_CONFIDENCE=False, max_future_adverse_pct=.1,
        signal_state="NO_QUALIFYING_SIGNAL", day_split="FIT20",
        lead_minutes=2., early_exit=False, full_horizon_eligible=True,
        observed_exposure_minutes=5., risk_score=.2, unknown_reason="",
        trade_age_minutes=1., model_stage="ENTRY_RISK",
    )
    row.update(changes)
    return row


def executed(sid=1, day=DAY, **changes):
    row = dict(
        day=day, setup_id="setup", tradingsymbol="TEST", sid=sid, side="LONG",
        entry_ts=pd.Timestamp(day + "T04:14:00Z"),
        exit_ts=pd.Timestamp(day + "T04:45:00Z"),
        entry_price=100., exit_price=101., exit_reason="TARGET",
        initial_stop_pct=1., active_stop_pct_at_exit=1., first_target_pct=1.,
        runner_target_pct=2., portfolio_gross_profit_rupees=105.,
        portfolio_cost_rupees=5., portfolio_net_profit_rupees=100.,
    )
    row.update(changes)
    return row


def test_grid_event_recall_is_separate_from_dense_cooldown_alert_precision():
    rows = pd.DataFrame([
        # Watch at the grid close counts as a caught evaluation window even
        # when cooldown suppresses a new emitted alert at that instant.
        prediction(event=True, research_watch=True, max_future_adverse_pct=.5),
        prediction(event=True, research_watch=False, max_future_adverse_pct=.8),
        prediction(event=False),
        prediction(is_grid=False, event=True, research_watch=True,
                   alert_RESEARCH_WATCH=True, max_future_adverse_pct=1.5),
        prediction(is_grid=False, event=False, research_watch=True,
                   alert_RESEARCH_WATCH=True),
    ])
    stats = horizon_stats(rows)
    assert stats["evaluated_windows"] == 3
    assert stats["pullback_windows"] == 2
    assert stats["caught_windows"] == stats["missed_windows"] == 1
    assert stats["watch_alerts"] == 2
    assert stats["correct_alerts"] == stats["false_alerts"] == 1
    assert stats["unknown_alerts"] == stats["unknown_windows"] == 0
    assert stats["max_adverse_pct"] == .8  # Fixed grid only, not dense maximum.


def test_unknown_windows_and_alerts_cannot_count_as_success_or_failure():
    rows = pd.DataFrame([
        prediction(outcome_available=False, event=True, research_watch=True,
                   alert_RESEARCH_WATCH=True, max_future_adverse_pct=20.),
        prediction(outcome_available=False, event=False, research_watch=True,
                   alert_RESEARCH_WATCH=True),
        prediction(is_grid=False, event=True, research_watch=True,
                   alert_RESEARCH_WATCH=True),
    ])
    stats = horizon_stats(rows)
    assert stats["evaluated_windows"] == stats["pullback_windows"] == 0
    assert stats["caught_windows"] == stats["missed_windows"] == 0
    assert stats["unknown_windows"] == stats["unknown_alerts"] == 2
    assert stats["watch_alerts"] == 3
    assert stats["correct_alerts"] == 1
    assert stats["false_alerts"] == 0
    assert math.isnan(stats["max_adverse_pct"])
    assert stats["first_pullback_time_ist"] == ""


def test_empty_horizon_reports_zero_counts_without_invented_outcomes():
    stats = horizon_stats(pd.DataFrame([prediction()]).iloc[:0])
    assert all(value == 0 for key, value in stats.items()
               if key not in ("max_adverse_pct", "first_pullback_time_ist"))
    assert math.isnan(stats["max_adverse_pct"])
    assert stats["first_pullback_time_ist"] == ""


def test_trade_states_distinguish_no_signal_unavailable_unresolved_and_event():
    ledger = pd.DataFrame([executed(sid=n) for n in range(1, 5)])
    pred = pd.DataFrame([
        prediction(),
        # The same observation appears at both horizons but is one minute.
        prediction(horizon_minutes=30),
        prediction(trade_id=f"{DAY}|setup|TEST|3", outcome_available=False,
                   signal_state="INSUFFICIENT_EVIDENCE"),
        prediction(trade_id=f"{DAY}|setup|TEST|4", event=True),
    ])
    result = build_trade_results(ledger, pred, META).set_index("trade_id")
    first = result.loc[f"{DAY}|setup|TEST|1"]
    assert first.signal_status == "NO_QUALIFYING_SIGNAL"
    assert first.pullback_status == "NO_DEFINED_PULLBACK_IN_EVALUATED_WINDOWS"
    assert first.monitoring_minutes == 1
    second = result.loc[f"{DAY}|setup|TEST|2"]
    assert second.signal_status == second.pullback_status == "MONITOR_UNAVAILABLE"
    assert second.monitoring_minutes == 0
    third = result.loc[f"{DAY}|setup|TEST|3"]
    assert third.pullback_status == "UNRESOLVED_WINDOWS"
    assert third.insufficient_evidence_minutes == 1
    fourth = result.loc[f"{DAY}|setup|TEST|4"]
    assert fourth.signal_status == "NO_QUALIFYING_SIGNAL"
    assert fourth.pullback_status == "DEFINED_PULLBACK_OBSERVED"
    assert fourth.fast_missed_windows == 1
    assert result.net_pnl.sum() == ledger.portfolio_net_profit_rupees.sum()


def test_detail_exports_keep_grid_outcomes_and_dense_alerts_distinct():
    pred = pd.DataFrame([
        prediction(event=True, research_watch=True, alert_RESEARCH_WATCH=True),
        prediction(event=False, research_watch=True, alert_RESEARCH_WATCH=True),
        prediction(event=True, outcome_available=False, research_watch=True,
                   alert_RESEARCH_WATCH=True),
        prediction(is_grid=False, event=True, research_watch=True,
                   alert_RESEARCH_WATCH=True),
    ])
    grid, actual, alerts = detail_rows(pred)
    assert len(grid) == 3
    assert len(actual) == 1
    assert actual.event.all() and actual.outcome_available.all()
    assert len(alerts) == 4
    assert list(alerts.alert_outcome) == ["SUCCESS", "FALSE_ALARM", "UNKNOWN", "SUCCESS"]
    assert actual.iloc[0].decision_time_ist == DAY + " 09:45:00"
    assert actual.iloc[0].window_end_time_ist == DAY + " 09:50:00"


def test_daily_results_preserve_zero_trade_sessions_and_frozen_accounting():
    trades = build_trade_results(pd.DataFrame([executed()]), pd.DataFrame([prediction()]), META)
    source = pd.DataFrame([
        dict(day="2026-07-30", trades=0, net_pnl=0., cost=0.),
        dict(day=DAY, trades=1, net_pnl=100., cost=5.),
    ])
    days = build_daily_results(source, trades, META)
    assert list(days.day) == [DAY, "2026-07-30"]
    assert list(days.cumulative_net_pnl) == [100., 100.]
    assert days.iloc[1].signal_status == "NO_TRADES"
    assert days.iloc[1].trades == days.iloc[1].net_pnl == days.iloc[1].cost == 0
    assert days.iloc[0].no_signal_trades == 1
    assert days.gross_pnl.sum() - days.cost.sum() == days.net_pnl.sum()


@pytest.mark.parametrize("field,wrong", [("trades", 2), ("net_pnl", 101.), ("cost", 6.)])
def test_daily_reconciliation_rejects_ledger_drift(field, wrong):
    trades = build_trade_results(pd.DataFrame([executed()]), pd.DataFrame([prediction()]), META)
    source = dict(day=DAY, trades=1, net_pnl=100., cost=5.)
    source[field] = wrong
    with pytest.raises(ValueError, match="reconciliation|cost drift"):
        build_daily_results(pd.DataFrame([source]), trades, META)


def test_local_timestamps_require_explicit_timezone():
    assert local_time(pd.Timestamp(DAY + "T04:00:00Z")) == DAY + " 09:30:00"
    assert local_time(pd.NaT) == ""
    with pytest.raises(ValueError, match="aware source"):
        local_time(pd.Timestamp(DAY + "T04:00:00"))
