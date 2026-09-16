from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_options_backtest as backtest


IST = "Asia/Kolkata"


def _portfolio_row(trade_id, entry, observed, *, entry_price=10., exit_price=12.,
                   quantity=3, entry_costs=1., exit_costs=2., status="CLOSED"):
    return {
        "trade_id": trade_id, "setup_id": "0926_LONG", "side": "LONG",
        "day": "2026-09-01", "entry_ts": pd.Timestamp(f"2026-09-01 {entry}", tz=IST),
        "exit_ts": pd.Timestamp(f"2026-09-01 {entry}", tz=IST),
        "exit_observed_ts": pd.Timestamp(f"2026-09-01 {observed}", tz=IST) if observed else pd.NaT,
        "status": status, "quantity": quantity, "entry_price": entry_price,
        "exit_price": exit_price if status == "CLOSED" else np.nan,
        "entry_costs": entry_costs, "exit_costs": exit_costs if status == "CLOSED" else 0.,
        "gross_pnl": (exit_price-entry_price)*quantity if status == "CLOSED" else np.nan,
        "net_pnl": (exit_price-entry_price)*quantity-entry_costs-exit_costs if status == "CLOSED" else np.nan,
    }


def test_capital_cannot_reuse_intrabar_exit_until_close_but_can_at_equal_boundary():
    rows = [
        _portfolio_row("A", "09:30", "09:35"),
        _portfolio_row("B", "09:30", "09:35"),
        _portfolio_row("C", "09:35", "09:40"),
    ]
    # Feed reverse order to verify chronology and deterministic simultaneous priority.
    ledger, summary, events = backtest.capital_replay(pd.DataFrame(rows[::-1]), 50.)
    statuses = ledger.set_index("trade_id").portfolio_status.to_dict()
    assert statuses == {"A": "ADMITTED", "B": "REJECTED_PREMIUM_CAPITAL", "C": "ADMITTED"}
    assert [(event["trade_id"], event["event"]) for event in events] == [
        ("A", "BUY"), ("A", "SELL"), ("C", "BUY"), ("C", "SELL"),
    ]
    assert summary["ending_free_cash"] == pytest.approx(56.)
    assert summary["peak_reserved_premium_and_fees"] == pytest.approx(31.)
    assert summary["capital_rejections"] == 1
    assert summary["unresolved_reserved_premium_and_fees"] == 0.


def test_unresolved_trade_reserves_premium_and_fees_and_is_not_hidden_from_metrics():
    rows = [
        _portfolio_row("A", "09:30", None, status="UNRESOLVED"),
        _portfolio_row("B", "09:35", "09:40", entry_price=6., exit_price=7., exit_costs=1.),
        _portfolio_row("C", "09:45", "09:50"),
    ]
    ledger, summary, events = backtest.capital_replay(pd.DataFrame(rows), 50.)
    assert ledger.portfolio_status.tolist() == ["ADMITTED", "ADMITTED", "REJECTED_PREMIUM_CAPITAL"]
    assert summary["unresolved_reserved_premium_and_fees"] == pytest.approx(31.)
    assert summary["ending_free_cash"] == pytest.approx(20.)
    assert not any(event["trade_id"] == "A" and event["event"] == "SELL" for event in events)
    metrics = backtest.metrics(ledger)
    assert metrics["entered"] == 2
    assert metrics["closed"] == 1
    assert metrics["unresolved"] == 1
    assert metrics["net_pnl"] == pytest.approx(1.)
    assert metrics["net_pnl_full_premium_loss_bound"] == pytest.approx(-30.)
    assert backtest.score(ledger) < 0


def _training_frame(net_pnl, future_net_pnl=0., train_count=8):
    rows = []
    days = [f"2026-09-{day:02}" for day in (1, 2, 3, 4)]
    for i in range(train_count + 4):
        row = _portfolio_row(f"T{i}", "09:30", "09:35", entry_price=100., quantity=75)
        row["day"] = days[i % 4] if i < train_count else "2026-09-09"
        row["portfolio_status"] = "ADMITTED"
        row["net_pnl"] = net_pnl if i < train_count else future_net_pnl
        rows.append(row)
    return pd.DataFrame(rows)


def test_profile_selection_cannot_see_future_outcomes_or_future_admissions():
    alternative = (.10, .20)
    sweeps = {
        backtest.BASELINE: _training_frame(50., future_net_pnl=1e9),
        alternative: _training_frame(500., future_net_pnl=-1e9),
    }
    setups = ["0926_LONG", "0926_SHORT"]
    policy, rules = backtest.choose_profile(sweeps, "2026-09-04", setups)
    assert policy["default"] == alternative
    assert set(rules.selection_scope) == {"TRAIN_GLOBAL"}
    changed = {pair: frame.copy() for pair, frame in sweeps.items()}
    for pair, frame in changed.items():
        future = frame.day.gt("2026-09-04")
        frame.loc[future, "net_pnl"] = -1e12 if pair == backtest.BASELINE else 1e12
        frame.loc[future, "status"] = "UNRESOLVED"
        frame.loc[future, "portfolio_status"] = "REJECTED_PREMIUM_CAPITAL"
        frame.loc[future, "entry_price"] = 1e12
    changed_policy, changed_rules = backtest.choose_profile(changed, "2026-09-04", setups)
    assert changed_policy == policy
    pd.testing.assert_frame_equal(changed_rules, rules)


def test_sparse_training_uses_predeclared_baseline_even_if_an_alternative_looks_best():
    alternative = (.10, .20)
    sweeps = {
        backtest.BASELINE: _training_frame(-100., train_count=7),
        alternative: _training_frame(1e9, train_count=7),
    }
    policy, rules = backtest.choose_profile(sweeps, "2026-09-04", ["0926_LONG"])
    assert policy == {"default": backtest.BASELINE, "0926_LONG": backtest.BASELINE}
    assert rules.iloc[0].selection_scope == "PREDECLARED_BASELINE_SPARSE_TRAIN"
    assert rules.iloc[0].setup_training_trades == 7


def test_parent_replay_always_requests_three_full_lots_and_does_not_downsize_for_cash():
    entry = pd.Timestamp("2026-09-01 09:30", tz=IST)
    mapped = pd.DataFrame([{
        "trade_id": "G1", "day": "2026-09-01", "setup_id": "0926_LONG", "side": "LONG",
        "entry_ts": entry, "lot_size": 25, "tick_size": .05,
        "signal_status": "READY", "mapping_status": "MAPPED_CAUSAL",
    }])
    candles = pd.DataFrame({
        "timestamp": [entry-pd.Timedelta(minutes=5), entry],
        "open": [100., 100.], "high": [102., 130.], "low": [99., 99.],
        "close": [100., 125.], "volume": [10000, 10000],
    })
    frame, _, summary, events = backtest.replay(
        mapped, {"G1": candles}, {"default": (.10, .20)}, capital=10000., slippage_bps=0.,
    )
    assert frame.iloc[0].lots == 3
    assert frame.iloc[0].quantity == 75
    assert frame.iloc[0].portfolio_status == "ADMITTED"
    assert summary["ending_free_cash"] == pytest.approx(10000.+frame.iloc[0].net_pnl)
    assert len(events) == 2
    rejected, _, small_summary, small_events = backtest.replay(
        mapped, {"G1": candles}, {"default": (.10, .20)}, capital=5000., slippage_bps=0.,
    )
    assert rejected.iloc[0].quantity == 75
    assert rejected.iloc[0].portfolio_status == "REJECTED_PREMIUM_CAPITAL"
    assert small_summary["ending_free_cash"] == 5000.
    assert small_events.empty
    assert backtest.metrics(rejected)["closed"] == 0


def test_replay_keeps_unfilled_and_unmapped_orders_in_attempts_and_exclusion_reasons():
    rows = pd.DataFrame([
        {"trade_id": "NO_TRIGGER", "day": "2026-09-01", "setup_id": "0926_LONG",
         "side": "LONG", "entry_ts": pd.NaT, "signal_status": "UNDERLYING_UNFILLED",
         "mapping_status": "UNDERLYING_UNFILLED"},
        {"trade_id": "NO_CONTRACT", "day": "2026-09-01", "setup_id": "0926_SHORT",
         "side": "SHORT", "entry_ts": pd.Timestamp("2026-09-01 09:30", tz=IST),
         "signal_status": "READY", "mapping_status": "MISSING_MONTHLY_OPTION_METADATA"},
    ])
    frame, bars, summary, events = backtest.replay(rows, {}, {"default": backtest.BASELINE})
    assert frame.trade_id.tolist() == ["NO_TRIGGER", "NO_CONTRACT"]
    assert frame.reason.tolist() == ["UNDERLYING_UNFILLED", "MISSING_MONTHLY_OPTION_METADATA"]
    assert frame.portfolio_status.tolist() == ["NOT_ENTERED", "NOT_ENTERED"]
    assert backtest.metrics(frame)["attempts"] == 2
    assert backtest.metrics(frame)["entered"] == 0
    assert summary["ending_free_cash"] == 1_500_000.
    assert bars.empty and events.empty
