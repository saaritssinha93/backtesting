from __future__ import annotations

import math

import pandas as pd
import pytest

from fno_v13_v10_g_options_execution import option_order_costs, simulate_trade


def _row(entry="2026-09-01 09:30", **overrides):
    row = {"trade_id": "ABC-CE-001", "day": "2026-09-01", "entry_ts": entry,
           "lot_size": 100, "tick_size": .05}
    row.update(overrides)
    return row


def _bar(timestamp, open=100, high=101, low=99, close=100, volume=10000):
    return {"timestamp": timestamp, "open": open, "high": high,
            "low": low, "close": close, "volume": volume}


def _candles(*bars, previous_volume=10000):
    return pd.DataFrame([_bar("2026-09-01 09:25", volume=previous_volume), *bars])


def test_opening_target_gap_precedes_later_stop_touch_and_is_limit_fill():
    candles = _candles(_bar("2026-09-01 09:30"),
                       _bar("2026-09-01 09:35", open=125, high=130, low=70, close=80))
    trade, audit = simulate_trade(_row(), candles, .10, .20, slippage_bps=0)
    assert trade["status"] == "CLOSED"
    assert trade["exit_reason"] == "TARGET_GAP"
    assert trade["exit_price"] == 120
    assert trade["gross_pnl"] == 6000
    assert trade["quantity"] == 300
    assert trade["exit_observed_ts"] == pd.Timestamp("2026-09-01 09:35", tz="Asia/Kolkata")
    assert not trade["ambiguous_stop_target"]
    assert audit[-1]["event"] == "TARGET_GAP"


def test_entry_bar_ambiguous_stop_target_is_stop_first():
    candles = _candles(_bar("2026-09-01 09:30", high=125, low=85))
    trade, audit = simulate_trade(_row(), candles, .10, .20, slippage_bps=0)
    assert trade["exit_reason"] == "STOP"
    assert trade["ambiguous_stop_target"]
    assert trade["exit_price"] == 90
    assert trade["gross_pnl"] == -3000
    assert trade["exit_ts"] == pd.Timestamp("2026-09-01 09:30", tz="Asia/Kolkata")
    assert trade["exit_observed_ts"] == trade["exit_ts"] + pd.Timedelta(minutes=5)
    assert audit[-1]["is_entry_bar"]
    assert audit[-1]["ambiguous_stop_target"]


def test_stop_gap_fills_at_open_with_adverse_slippage_not_at_stop():
    candles = _candles(_bar("2026-09-01 09:30"),
                       _bar("2026-09-01 09:35", open=70, high=75, low=65, close=72))
    trade, _ = simulate_trade(_row(), candles, .10, .20)
    assert trade["entry_price"] == 100.10
    assert trade["stop_price"] == 90.10
    assert trade["exit_reason"] == "STOP_GAP"
    assert trade["exit_raw_price"] == 70
    assert trade["exit_price"] == 69.90
    assert trade["exit_observed_ts"] == trade["exit_ts"]


def test_resting_target_receives_no_slippage_below_limit():
    candles = _candles(_bar("2026-09-01 09:30", high=125))
    trade, audit = simulate_trade(_row(), candles, .10, .20)
    assert trade["entry_price"] == 100.1
    assert trade["target_price"] == 120.15
    assert trade["exit_price"] == trade["target_price"]
    assert audit[-1]["fill_is_limit"]
    assert trade["exit_observed_ts"] == trade["exit_ts"] + pd.Timedelta(minutes=5)


def test_floating_point_exact_tick_does_not_add_a_tick():
    candles = _candles(_bar("2026-09-01 09:30", high=112))
    trade, _ = simulate_trade(_row(), candles, .10, .10, slippage_bps=0)
    assert trade["target_price"] == 110
    assert trade["stop_price"] == 90


def test_1515_time_exit_uses_open_before_later_high_low():
    candles = pd.DataFrame([
        _bar("2026-09-01 15:05"), _bar("2026-09-01 15:10"),
        _bar("2026-09-01 15:15", open=105, high=130, low=70, close=75),
    ])
    trade, audit = simulate_trade(_row("2026-09-01 15:10"), candles, .10, .20)
    assert trade["exit_reason"] == "TIME_EXIT_1515"
    assert trade["exit_raw_price"] == 105
    assert trade["exit_price"] == 104.85
    assert trade["exit_observed_ts"] == pd.Timestamp("2026-09-01 15:15", tz="Asia/Kolkata")
    assert not trade["ambiguous_stop_target"]
    assert len(audit) == 3


def test_missing_held_interval_cannot_be_bridged_by_later_profitable_bar():
    candles = _candles(_bar("2026-09-01 09:30"),
                       _bar("2026-09-01 09:40", open=125, high=130, low=120, close=127))
    trade, audit = simulate_trade(_row(), candles, .10, .20, slippage_bps=0)
    assert trade["status"] == "UNRESOLVED"
    assert trade["reason"] == "HELD_BAR_MISSING"
    assert trade["entered"]
    assert math.isnan(trade["net_pnl"])
    assert math.isnan(trade["gross_pnl"])
    assert trade["entry_costs"] > 0
    assert trade["total_costs"] == trade["entry_costs"]
    assert trade["net_cash_flow"] == -30000 - trade["entry_costs"]
    assert trade["net_pnl_lower_bound"] == trade["net_cash_flow"]
    assert pd.isna(trade["exit_ts"])
    assert audit[-1]["timestamp"] == pd.Timestamp("2026-09-01 09:35", tz="Asia/Kolkata")
    assert audit[-1]["state"] == "UNRESOLVED"


def test_missing_1515_is_unresolved_and_retains_entry_costs():
    candles = pd.DataFrame([_bar("2026-09-01 15:05"), _bar("2026-09-01 15:10")])
    trade, _ = simulate_trade(_row("2026-09-01 15:10"), candles, .10, .20)
    assert trade["status"] == "UNRESOLVED"
    assert trade["reason"] == "TIME_EXIT_BAR_MISSING"
    assert trade["net_cash_flow"] < -trade["entry_premium_outlay"]


def test_missing_future_intervals_after_closed_trade_have_no_effect():
    candles = _candles(_bar("2026-09-01 09:30", high=125))
    trade, _ = simulate_trade(_row(), candles, .10, .20, slippage_bps=0)
    assert trade["status"] == "CLOSED"
    assert trade["bars_held"] == 1


def test_pretrade_liquidity_only_uses_completed_previous_bar():
    candles = _candles(_bar("2026-09-01 09:30", high=125, volume=1000000), previous_volume=2000)
    trade, audit = simulate_trade(_row(), candles, .10, .20)
    assert trade["status"] == "SKIPPED"
    assert trade["reason"] == "PREVIOUS_VOLUME_INSUFFICIENT"
    assert not trade["entered"]
    assert trade["quantity"] == 300
    assert trade["net_cash_flow"] == 0
    assert len(audit) == 1


def test_low_actual_volume_flags_capacity_without_reducing_or_dropping_trade():
    low = _candles(_bar("2026-09-01 09:30", high=125, volume=600))
    high = _candles(_bar("2026-09-01 09:30", high=125, volume=10000))
    low_trade, low_audit = simulate_trade(_row(), low, .10, .20, slippage_bps=0)
    high_trade, _ = simulate_trade(_row(), high, .10, .20, slippage_bps=0)
    assert low_trade["status"] == high_trade["status"] == "CLOSED"
    assert low_trade["entry_capacity_breach"]
    assert low_trade["exit_capacity_breach"]
    assert not high_trade["capacity_breach"]
    assert low_trade["quantity"] == high_trade["quantity"] == 300
    assert low_trade["net_pnl"] == high_trade["net_pnl"]
    assert low_audit[-1]["quantity_exceeds_bar_capacity"]


def test_zero_volume_entry_is_explicit_ex_post_unfilled_without_costs():
    candles = _candles(_bar("2026-09-01 09:30", high=125, volume=0))
    trade, audit = simulate_trade(_row(), candles, .10, .20)
    assert trade["status"] == "SKIPPED"
    assert trade["reason"] == "ENTRY_ZERO_VOLUME_UNFILLED"
    assert not trade["entered"]
    assert trade["total_costs"] == 0
    assert audit[-1]["state"] == "EX_POST_UNFILLED"


@pytest.mark.parametrize("volume,reason", [(0, "EXIT_ZERO_VOLUME"), (299, "EXIT_INSUFFICIENT_TOTAL_VOLUME"),
                                           (math.nan, "EXIT_VOLUME_UNKNOWN")])
def test_insufficient_exit_volume_retains_entered_trade_as_unresolved(volume, reason):
    candles = _candles(_bar("2026-09-01 09:30"),
                       _bar("2026-09-01 09:35", open=125, high=125, low=125, close=125, volume=volume))
    trade, _ = simulate_trade(_row(), candles, .10, .20, slippage_bps=0)
    assert trade["status"] == "UNRESOLVED"
    assert trade["reason"] == reason
    assert trade["exit_physical_volume_insufficient"]
    assert trade["attempted_exit_reason"] == "TARGET_GAP"
    assert math.isnan(trade["net_pnl"])
    assert trade["entry_costs"] > 0
    assert trade["exit_costs"] == 0
    assert trade["net_cash_flow"] < -30000


def test_same_bar_entry_exit_requires_combined_total_volume():
    candles = _candles(_bar("2026-09-01 09:30", high=125, volume=599))
    trade, audit = simulate_trade(_row(), candles, .10, .20, slippage_bps=0)
    assert trade["entered"]
    assert trade["status"] == "UNRESOLVED"
    assert trade["reason"] == "SAME_BAR_EXIT_INSUFFICIENT_TOTAL_VOLUME"
    assert trade["entry_premium_outlay"] == 30000
    assert audit[-1]["required_execution_volume"] == 600


@pytest.mark.parametrize("volume,reason", [(299, "EX_POST_INSUFFICIENT_TOTAL_VOLUME"),
                                           (math.nan, "ENTRY_VOLUME_UNKNOWN_UNFILLED")])
def test_entry_total_volume_failure_is_ex_post_unfilled_and_never_resized(volume, reason):
    candles = _candles(_bar("2026-09-01 09:30", high=125, volume=volume))
    trade, audit = simulate_trade(_row(), candles, .10, .20)
    assert trade["status"] == "SKIPPED"
    assert trade["reason"] == reason
    assert trade["quantity"] == 300
    assert not trade["entered"]
    assert trade["net_cash_flow"] == 0
    assert audit[-1]["state"] == "EX_POST_UNFILLED"


def test_previous_volume_check_can_be_explicitly_disabled():
    candles = pd.DataFrame([_bar("2026-09-01 09:30", high=125)])
    screened, _ = simulate_trade(_row(), candles, .10, .20)
    unscreened, _ = simulate_trade(_row(check_previous_volume=False), candles, .10, .20)
    assert screened["reason"] == "PREVIOUS_BAR_MISSING"
    assert unscreened["status"] == "CLOSED"
    assert not unscreened["previous_volume_check_enabled"]


@pytest.mark.parametrize("bad", [
    {"open": 0}, {"close": -1}, {"high": 99}, {"low": 101}, {"close": math.nan},
])
def test_bad_entry_ohlc_is_skipped_but_bad_held_ohlc_retains_purchase(bad):
    broken = _bar("2026-09-01 09:30")
    broken.update(bad)
    skipped, _ = simulate_trade(_row(), _candles(broken), .10, .20)
    broken["timestamp"] = "2026-09-01 09:35"
    unresolved, _ = simulate_trade(_row(), _candles(_bar("2026-09-01 09:30"), broken), .10, .20)
    assert skipped["status"] == "SKIPPED"
    assert not skipped["entered"]
    assert unresolved["status"] == "UNRESOLVED"
    assert unresolved["entered"]
    assert unresolved["entry_costs"] > 0


def test_conflicting_duplicate_held_bars_are_unresolved_not_cherry_picked():
    candles = _candles(_bar("2026-09-01 09:30"),
                       _bar("2026-09-01 09:35", high=125),
                       _bar("2026-09-01 09:35", low=80))
    trade, _ = simulate_trade(_row(), candles, .10, .20)
    assert trade["status"] == "UNRESOLVED"
    assert trade["reason"] == "HELD_BAR_DUPLICATE"


def test_timezone_aware_utc_candles_match_ist_entry():
    candles = _candles(_bar("2026-09-01 09:30", high=125))
    candles["timestamp"] = pd.to_datetime(candles["timestamp"]).dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
    trade, _ = simulate_trade(_row(), candles, .10, .20)
    assert trade["status"] == "CLOSED"
    assert str(trade["entry_ts"].tz) == "Asia/Kolkata"


def test_order_cost_components_and_roundtrip_cash_reconcile():
    buy = option_order_costs(100, 300, "BUY")
    sell = option_order_costs(120, 300, "SELL")
    assert buy["brokerage"] == sell["brokerage"] == 20
    assert buy["stt"] == 0
    assert sell["stt"] == 54
    assert buy["exchange"] == pytest.approx(10.659)
    assert buy["sebi"] == pytest.approx(.03)
    assert buy["stamp"] == pytest.approx(.9)
    assert sell["stamp"] == 0
    assert buy["ipft"] == pytest.approx(.00003)
    assert buy["gst"] == pytest.approx(.18 * (20 + 10.659 + .03 + .00003))
    trade, _ = simulate_trade(_row(), _candles(_bar("2026-09-01 09:30", high=125)), .10, .20, slippage_bps=0)
    assert trade["total_costs"] == pytest.approx(buy["total"] + sell["total"])
    assert trade["net_pnl"] == pytest.approx(6000 - buy["total"] - sell["total"])
    assert trade["net_cash_flow"] == trade["net_pnl"]


@pytest.mark.parametrize("lots", [1, 2, 4, 3.5])
def test_cannot_silently_change_three_lot_norm(lots):
    with pytest.raises(ValueError, match="exactly 3 lots"):
        simulate_trade(_row(), _candles(_bar("2026-09-01 09:30")), .1, .2, lots=lots)


@pytest.mark.parametrize("entry", ["2026-09-01 09:31", "2026-09-01 15:15", "2026-09-02 09:30"])
def test_entry_must_be_explicit_valid_session_bar_start(entry):
    with pytest.raises(ValueError):
        simulate_trade(_row(entry), _candles(_bar("2026-09-01 09:30")), .1, .2)
