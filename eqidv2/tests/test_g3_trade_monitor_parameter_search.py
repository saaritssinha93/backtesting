"""Focused invariants for the bounded G3 monitor parameter search."""
from __future__ import annotations

from datetime import date
import json

import numpy as np
import pandas as pd
import pytest

from research import g3_trade_monitor_backtest as fixed
from research import g3_trade_monitor_parameter_search as search


DAY = date(2026, 10, 13)


def _inputs(count: int = 1, side: str = "LONG", *, profit: bool = False):
    """Small genuine-minute fixture matching the fixed monitor tests."""
    stamp = pd.Timestamp(f"{DAY} 12:00", tz="Asia/Kolkata")
    timestamps = pd.date_range(
        stamp + pd.Timedelta(minutes=1),
        pd.Timestamp(f"{DAY} 15:15", tz="Asia/Kolkata"),
        freq="min",
    )
    orders, paths = [], {}
    for sid in range(count):
        closes = np.full(len(timestamps), 100.0)
        if sid == 0:
            values = (
                [100.0, 100.1, 100.7, 100.6, 100.5, 100.4, 100.25, 100.2]
                if profit
                else [100.0, 100.02, 99.96, 99.88, 99.80, 99.71, 99.60, 99.55]
            )
            closes[: len(values)] = values
            closes[len(values) :] = values[-1]
        if side == "SHORT":
            closes = 200.0 - closes
        opens = closes.copy()
        if sid == 0:
            opens[7] = 100.22 if profit else 99.57
            if side == "SHORT":
                opens[7] = 200.0 - opens[7]
        paths[sid] = {
            "timestamp_ns": timestamps.asi8,
            "open": opens,
            "high": np.maximum(opens, closes) + 0.01,
            "low": np.minimum(opens, closes) - 0.01,
            "close": closes,
        }
        if sid == 10:
            paths[sid] = {key: values[10:] for key, values in paths[sid].items()}
        orders.append(
            {
                "sid": sid,
                "day": str(DAY),
                "signal_id": f"source-{sid}",
                "setup_id": f"1331_{side}",
                "tradingsymbol": f"EXAMPLE{sid:02}",
                "side": side,
                "confirmation_ts": stamp + pd.Timedelta(minutes=10 if sid == 10 else 0),
                "trigger": 100.0,
                "picker": "max_liquidity",
                "traded_value": 1000 - sid,
                "native_target_pct": 1.17,
                "native_stop_pct": 1.25,
            }
        )
    orders = pd.DataFrame(orders)
    baseline = search.history._simulate(orders, paths, [DAY], search.history.contract)
    return orders, paths, baseline


def test_registry_is_deterministic_64_candidates_and_contains_current_controls():
    first = search.candidate_registry()
    second = search.candidate_registry()

    assert first == second
    assert len(first) == 64
    assert [item["grid_order"] for item in first] == list(range(1, 65))
    assert len({item["candidate_id"] for item in first}) == 64
    assert len({item["candidate_sha256"] for item in first}) == 64
    assert all(item["candidate_sha256"] == search._candidate_hash(item) for item in first)
    assert sum(item["family"] == "trend_failure" for item in first) == 51
    assert sum(item["family"] == "profit_giveback" for item in first) == 13
    assert search.configuration()["search"]["coarse_candidates"] == 64

    trend = search._current_candidate("trend_failure")
    giveback = search._current_candidate("profit_giveback")
    expected = search.EXPECTED_SHARED_CONFIG
    assert (
        trend["minimum_bars"],
        trend["trend_adverse_closes"],
        trend["trend_momentum_5m_pct"],
        trend["trend_entry_move_pct"],
    ) == (
        expected["minimum_bars"],
        expected["adverse_closes"],
        expected["trend_momentum_5m_pct"],
        expected["trend_entry_move_pct"],
    )
    assert giveback["candidate_id"] == "giveback_current_absolute"
    assert giveback["profit_activation_target_fraction"] is None
    assert (
        giveback["profit_adverse_closes"],
        giveback["profit_activation_pct"],
        giveback["profit_giveback_pct"],
    ) == (
        expected["adverse_closes"],
        expected["profit_activation_pct"],
        expected["profit_giveback_pct"],
    )


def _feature_row(sid: int, target: float, mfe: float) -> dict:
    decision = pd.Timestamp(f"{DAY} 12:07", tz="Asia/Kolkata")
    execution = decision + pd.Timedelta(minutes=1)
    return {
        "sid": sid,
        "source_signal_id": f"source-{sid}",
        "day": str(DAY),
        "setup_id": "TEST_LONG",
        "tradingsymbol": f"TEST{sid}",
        "side": "LONG",
        "decision_at_ist": decision.isoformat(),
        "completed_bar_count": 6,
        "consecutive_adverse_closes": 3,
        "momentum_5m_pct": 0.0,
        "signed_move_pct": mfe - 0.4,
        "mfe_pct": mfe,
        "giveback_pct": 0.4,
        "next_bar_open_at_ist": decision.isoformat(),
        "modeled_execution_at_ist": execution.isoformat(),
        "execution_path_index": 7,
        "next_bar_open_price": 100.0,
        "original_exit_at_ist": pd.Timestamp(f"{DAY} 15:15", tz="Asia/Kolkata").isoformat(),
        "native_target_pct": target,
    }


def test_target_scaled_profit_activation_uses_floor_and_each_trades_target():
    frame = pd.DataFrame(
        [
            _feature_row(1, 0.56, 0.51),  # max(0.50, 0.8 * 0.56) = 0.50
            _feature_row(2, 0.75, 0.59),  # activation is 0.60: does not pass
            # The mathematically equal target-scaled boundary is inclusive.
            _feature_row(3, 0.75, 0.60),  # 0.8 * 0.75 = 0.60
            _feature_row(4, 1.50, 1.19),  # activation is 1.20: does not pass
            _feature_row(5, 1.50, 1.21),  # activation is 1.20: passes
        ],
        columns=search.FEATURE_COLUMNS,
    )
    cache = search.DecisionCache(frame)
    scaled = next(
        item
        for item in search.candidate_registry()
        if item["family"] == "profit_giveback"
        and item["profit_adverse_closes"] == 3
        and item["profit_activation_target_fraction"] == 0.8
        and item["profit_giveback_pct"] == 0.4
    )
    assert [event["sid"] for event in cache.events(scaled)] == [1, 3, 5]

    current = search._current_candidate("profit_giveback")
    assert [event["sid"] for event in cache.events(current)] == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize(
    "profit,family,flag",
    [
        (False, "trend_failure", "TREND_FAILURE"),
        (True, "profit_giveback", "PROFIT_GIVEBACK"),
    ],
)
def test_feature_cache_first_event_matches_slow_shared_rule(side, profit, family, flag):
    _, paths, baseline = _inputs(side=side, profit=profit)
    features = search.build_feature_cache(baseline, paths)
    fast = search.DecisionCache(features).events(search._current_candidate(family))
    slow, _ = fixed.trade_advisories(baseline.iloc[0], paths[0])

    assert len(fast) == 1
    expected = slow[flag]
    actual = fast[0]
    assert json.loads(actual["flags"]) == [flag]
    for field in (
        "sid",
        "decision_at_ist",
        "completed_bar_count",
        "modeled_execution_at_ist",
        "execution_path_index",
        "next_bar_open_at_ist",
        "original_exit_at_ist",
    ):
        assert actual[field] == expected[field]
    assert actual["next_bar_open_price"] == pytest.approx(expected["next_bar_open_price"])


def test_feature_cache_uses_actual_open_for_original_exit_precedence():
    _, paths, baseline = _inputs()
    row = baseline.iloc[0].copy()
    candidate = search._current_candidate("trend_failure")

    row["exit_ts"] = pd.Timestamp(f"{DAY} 12:07", tz="Asia/Kolkata")
    equal = pd.DataFrame(search.decision_features(row, paths[0]), columns=search.FEATURE_COLUMNS)
    assert search.DecisionCache(equal).events(candidate) == []
    slow_equal, signals = fixed.trade_advisories(row, paths[0])
    assert slow_equal == {}
    assert signals == []

    row["exit_ts"] += pd.Timedelta(microseconds=1)
    later = pd.DataFrame(search.decision_features(row, paths[0]), columns=search.FEATURE_COLUMNS)
    fast_event = search.DecisionCache(later).events(candidate)[0]
    slow_later, _ = fixed.trade_advisories(row, paths[0])
    assert fast_event["execution_path_index"] == slow_later["TREND_FAILURE"]["execution_path_index"] == 7
    assert pd.Timestamp(fast_event["next_bar_open_at_ist"]) < pd.Timestamp(row["exit_ts"])


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("future_event", ["STOP", "TARGET"])
def test_feature_cache_and_fast_book_are_invariant_to_later_intrabar_outcome(side, future_event):
    orders, paths, baseline = _inputs(side=side)
    candidate = search._current_candidate("trend_failure")
    before_events = search.DecisionCache(search.build_feature_cache(baseline, paths)).events(candidate)
    before = search.FastBook(baseline).outcome(before_events)
    upper = (future_event == "TARGET") == (side == "LONG")
    paths[0]["high" if upper else "low"][7] = 102.0 if upper else 98.0
    regenerated = search.history._simulate(orders, paths, [DAY], search.history.contract)
    assert regenerated.iloc[0].exit_reason == future_event
    events = search.DecisionCache(search.build_feature_cache(regenerated, paths)).events(candidate)
    assert len(events) == 1
    assert events[0]["execution_path_index"] == before_events[0]["execution_path_index"]
    assert events[0]["next_bar_open_price"] == before_events[0]["next_bar_open_price"]
    book = search.FastBook(regenerated)
    after = book.outcome(events)
    np.testing.assert_allclose(after["net"], before["net"])
    ledger = search._canonical_ledger(regenerated, paths, candidate, events, "causal_test")
    search._assert_fast_parity(book, after, ledger, "causal_test")


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_feature_cache_preserves_known_open_gap_stop(side):
    orders, paths, _ = _inputs(side=side)
    is_long = side == "LONG"
    paths[0]["open"][7] = 98.5 if is_long else 101.5
    paths[0]["low" if is_long else "high"][7] = 98.4 if is_long else 101.6
    baseline = search.history._simulate(orders, paths, [DAY], search.history.contract)
    assert baseline.iloc[0].exit_event == "OPEN"
    cache = search.DecisionCache(search.build_feature_cache(baseline, paths))
    assert cache.events(search._current_candidate("trend_failure")) == []


def test_fast_book_matches_canonical_baseline_and_candidate_and_admits_eleventh_order():
    _, paths, baseline = _inputs(count=11)
    book = search.FastBook(baseline)

    baseline_ledger = search._canonical_ledger(baseline, paths, None, [], "baseline")
    search.history._same_native(baseline_ledger, baseline)
    search.history._same_allocation(baseline_ledger, baseline)
    search._assert_fast_parity(book, book.baseline, baseline_ledger, "baseline")
    assert int(book.baseline["accepted"].sum()) == int(baseline.portfolio_executed.sum()) == 10
    assert not book.baseline["accepted"][10]

    candidate = search._current_candidate("trend_failure")
    cache = search.DecisionCache(search.build_feature_cache(baseline, paths))
    events = cache.events(candidate)
    outcome = book.outcome(events)
    candidate_ledger = search._canonical_ledger(
        baseline, paths, candidate, events, "trend_failure_test"
    )
    search._assert_fast_parity(book, outcome, candidate_ledger, "trend_failure_test")
    assert int(outcome["accepted"].sum()) == 11
    assert outcome["accepted"][10]
    assert candidate_ledger.loc[candidate_ledger.sid.eq(10), "portfolio_executed"].item()
    assert candidate_ledger.search_candidate_id.eq(candidate["candidate_id"]).all()
    assert candidate_ledger.search_candidate_sha256.eq(candidate["candidate_sha256"]).all()


def _ledger_row(
    sid: int,
    day: date,
    net: float,
    *,
    executed: bool,
    reason: str,
    monitor_exit: bool = False,
) -> dict:
    cost = 250.0 if executed else 0.0
    return {
        "sid": sid,
        "day": str(day),
        "filled": True,
        "portfolio_executed": executed,
        "portfolio_net_profit_rupees": net if executed else 0.0,
        "portfolio_gross_profit_rupees": net + cost if executed else 0.0,
        "portfolio_cost_rupees": cost,
        "exit_reason": reason,
        "monitor_exit": monitor_exit,
    }


def test_extended_ledger_metrics_are_period_scoped_and_include_selection_guards():
    next_day = date(2026, 10, 14)
    baseline = pd.DataFrame(
        [
            _ledger_row(1, DAY, -1000.0, executed=True, reason="STOP"),
            _ledger_row(2, DAY, -500.0, executed=True, reason="TIGHTENED_STOP"),
            _ledger_row(3, DAY, 1000.0, executed=True, reason="TARGET"),
            _ledger_row(4, DAY, 300.0, executed=True, reason="TARGET"),
            _ledger_row(5, DAY, 0.0, executed=False, reason="TARGET"),
            _ledger_row(6, DAY, 250.0, executed=True, reason="TARGET"),
            # This extreme result must not leak into the requested first day.
            _ledger_row(7, next_day, -1e9, executed=True, reason="STOP"),
        ]
    )
    candidate = pd.DataFrame(
        [
            _ledger_row(1, DAY, -400.0, executed=True, reason="MONITOR", monitor_exit=True),
            _ledger_row(2, DAY, -700.0, executed=True, reason="MONITOR", monitor_exit=True),
            _ledger_row(3, DAY, 400.0, executed=True, reason="MONITOR", monitor_exit=True),
            _ledger_row(4, DAY, -100.0, executed=True, reason="MONITOR", monitor_exit=True),
            _ledger_row(5, DAY, 200.0, executed=True, reason="TARGET"),
            _ledger_row(6, DAY, 0.0, executed=False, reason="TARGET"),
            _ledger_row(7, next_day, 1e9, executed=True, reason="TARGET", monitor_exit=True),
        ]
    )

    metrics = search._ledger_extended_metrics(candidate, baseline, [DAY])
    assert metrics["selected_orders"] == 6
    assert metrics["executed_trades"] == 5
    assert metrics["wins"] == 2
    assert metrics["losses"] == 3
    assert metrics["win_rate_pct"] == pytest.approx(40.0)
    assert metrics["net_pnl_rs"] == pytest.approx(-600.0)
    assert metrics["net_pnl_delta_vs_baseline_rs"] == pytest.approx(-650.0)
    assert metrics["realized_loss_reduction_rs"] == pytest.approx(300.0)

    expected_guards = {
        "newly_admitted",
        "newly_rejected",
        "stopped_trades_improved",
        "stop_loss_saved_rs",
        "stopped_trades_worsened",
        "baseline_winners_cut",
        "winner_profit_reduced_rs",
        "winners_turned_nonpositive",
        "losing_days",
        "baseline_losing_days",
    }
    assert expected_guards <= set(metrics)
    assert metrics["newly_admitted"] == 1
    assert metrics["newly_rejected"] == 1
    assert metrics["stopped_trades_improved"] == 1
    assert metrics["stop_loss_saved_rs"] == pytest.approx(600.0)
    assert metrics["stopped_trades_worsened"] == 1
    assert metrics["baseline_winners_cut"] == 2
    assert metrics["winner_profit_reduced_rs"] == pytest.approx(1000.0)
    assert metrics["winners_turned_nonpositive"] == 1
    # The metrics payload is directly consumable by the selection gate.
    failures = search._qualification_failures(metrics, "evaluation15")
    assert "baseline_execution_displaced" in failures


def _selection_rows(candidates):
    rows = []
    development_scores = {
        candidates[0]["candidate_id"]: 1000.0,
        candidates[1]["candidate_id"]: 900.0,
    }
    for candidate in candidates:
        for period in ("calibration25", "validation10", "development35", "evaluation15"):
            stress = development_scores[candidate["candidate_id"]]
            if period == "calibration25":
                stress /= 4
            elif period == "validation10":
                stress /= 2
            elif period == "evaluation15":
                # Deliberately favors the weaker development candidate.
                stress = 0.0 if candidate is candidates[0] else 1_000_000.0
            rows.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "candidate_sha256": candidate["candidate_sha256"],
                    "family": candidate["family"],
                    "period": period,
                    "monitor_exit_count": 15,
                    "executed_trades": 20,
                    "wins": 14,
                    "net_pnl_delta_vs_baseline_rs": 500.0,
                    "stress_10bps_net_pnl_delta_vs_baseline_rs": stress,
                    "realized_loss_reduction_rs": 100.0,
                    "max_drawdown_rs": 90.0,
                    "baseline_max_drawdown_rs": 100.0,
                    "losing_days": 2,
                    "baseline_losing_days": 3,
                    "newly_rejected": 0,
                }
            )
    return pd.DataFrame(rows)


def test_finalist_selection_uses_development_only_and_marks_diagnostic_fallback():
    candidates = search.candidate_registry()[:2]
    results = _selection_rows(candidates)
    first = search._choose_finalist(results, candidates)
    assert first["locked_candidate_id"] == candidates[0]["candidate_id"]
    assert first["parameter_change_locked"] is True

    mutated = results.copy()
    evaluation = mutated.period.eq("evaluation15")
    mutated.loc[evaluation, "stress_10bps_net_pnl_delta_vs_baseline_rs"] *= -1_000_000
    mutated.loc[evaluation, "net_pnl_delta_vs_baseline_rs"] = -1e12
    second = search._choose_finalist(
        mutated.sample(frac=1.0, random_state=7).reset_index(drop=True),
        list(reversed(candidates)),
    )
    assert second == first

    failed = results.copy()
    failed.loc[failed.period.eq("calibration25"), "net_pnl_delta_vs_baseline_rs"] = 0.0
    fallback = search._choose_finalist(failed, candidates)
    assert fallback["locked_candidate_id"] == candidates[0]["candidate_id"]
    assert fallback["locked_candidate_sha256"] == candidates[0]["candidate_sha256"]
    assert fallback["parameter_change_locked"] is True
    assert fallback["development_qualified"] is False
    assert fallback["qualified_candidate_ids"] == []
    assert fallback["lock_reason"].endswith("DIAGNOSTIC_EVALUATION_ONLY")
