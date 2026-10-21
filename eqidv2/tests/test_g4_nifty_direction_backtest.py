"""No future index bars, symmetric side vetoes and portfolio reallocation."""
import numpy as np
import pandas as pd
import pytest

from research import g4_nifty_direction_backtest as study


def frame():
    stamps = pd.date_range("2026-10-19 09:20", periods=4, freq="5min", tz="Asia/Kolkata")
    return pd.DataFrame(dict(ts=stamps, open=[100., 102., 101., 99.],
                             close=[102., 101., 99., 105.], quality_state="VALID"))


@pytest.mark.parametrize("side,change,expected", [
    ("LONG", 1., True), ("SHORT", 1., False), ("LONG", -1., False),
    ("SHORT", -1., True), ("LONG", 0., True), ("SHORT", 0., True),
])
def test_all_sides_direction_and_flat(side, change, expected):
    assert study.allow_direction(side, change) is expected


@pytest.mark.parametrize("side,change", [("BAD", 1.), ("LONG", np.nan), ("SHORT", np.inf)])
def test_invalid_input_cannot_be_neutral(side, change):
    with pytest.raises(ValueError):
        study.allow_direction(side, change)


def test_confirmation_only_reads_latest_completed_five_minute_bar():
    raw = frame()
    decision = pd.Timestamp("2026-10-19 09:26", tz="Asia/Kolkata")
    before = study.direction_at(raw, decision)
    assert pd.Timestamp(before["nifty_bar_ts"]) == raw.iloc[1].ts
    assert before["slot_candle"] < 0
    assert before["close_to_close"] < 0
    assert before["session_open"] > 0
    raw.loc[raw.ts.gt(decision), ["open", "close"]] = 1e9
    assert study.direction_at(raw, decision) == before


def test_missing_exact_bar_cannot_use_a_stale_observation():
    raw = frame().drop(index=1)
    result = study.direction_at(raw, pd.Timestamp("2026-10-19 09:26", tz="Asia/Kolkata"))
    assert result["status"] == "MISSING_EXACT_NIFTY_BAR"
    assert np.isnan(result["slot_candle"])


def test_first_slot_uses_open_as_close_to_close_reference():
    result = study.direction_at(frame(), pd.Timestamp("2026-10-19 09:21", tz="Asia/Kolkata"))
    assert result["status"] == "OK"
    assert result["slot_candle"] == result["close_to_close"] == result["session_open"]


@pytest.mark.parametrize("value", ["26SEP", "2026-09", "26sep"])
def test_equivalent_sealed_month_formats_select_same_contract(value):
    assert study.normalize_month(value) == "26SEP"


def test_invalid_quality_state_rejected():
    raw = frame()
    raw.loc[1, "quality_state"] = "STALE"
    result = study.direction_at(raw, pd.Timestamp("2026-10-19 09:26", tz="Asia/Kolkata"))
    assert result["status"] == "INVALID_NIFTY_BAR"


def test_veto_frees_capacity_for_later_allowed_order():
    rows = []
    for sid, side, minute in [(1, "SHORT", 30), (2, "LONG", 31)]:
        rows.append(dict(sid=sid, day="2026-10-19", tradingsymbol=str(sid), side=side,
            filled=True, confirmation_ts=f"2026-10-19 09:{minute-1}:00+05:30",
            entry_ts=f"2026-10-19 09:{minute}:00+05:30", exit_ts="2026-10-19 15:15:00+05:30",
            gross_return_pct=1., cost_pct=.05, net_return_pct=.95, initial_stop_pct=1.2,
            picker="max_move", price_change_pct=1.))
    original = pd.DataFrame(rows)
    audit = pd.DataFrame(dict(sid=[1, 2], slot_candle_allowed=[False, True]))
    config = dict(capital_per_entry_rs=100000., leverage=5., portfolio_capital_rs=100000.)
    baseline, _ = study.filter_book(original, audit, "control", config)
    assert baseline.portfolio_executed.tolist() == [True, False]
    filtered, _ = study.filter_book(original, audit, "slot_candle", config)
    assert filtered.portfolio_executed.tolist() == [False, True]
    assert filtered.iloc[0].portfolio_status == "NIFTY_DIRECTION_BLOCKED"
    assert filtered.portfolio_net_profit_rupees.sum() == pytest.approx(4750.)
