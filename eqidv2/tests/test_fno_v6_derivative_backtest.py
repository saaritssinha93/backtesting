from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

import fno_v6_derivative_backtest as engine


IST = "Asia/Kolkata"


def _candles() -> pd.DataFrame:
    return engine.normalize_candles(
        pd.DataFrame(
            [
                {"date": "2026-09-01 09:27:00+05:30", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 0},
                {"date": "2026-09-01 09:28:00+05:30", "open": 12, "high": 13, "low": 11, "close": 12.5, "volume": 25},
                {"date": "2026-09-01 15:29:00+05:30", "open": 20, "high": 22, "low": 19, "close": 21, "volume": 10},
            ]
        )
    )


def test_option_direction_contract() -> None:
    assert engine.option_type_for_side("LONG") == "CE"
    assert engine.option_type_for_side("SHORT") == "PE"
    with pytest.raises(ValueError):
        engine.option_type_for_side("FLAT")


def test_atm_selection_uses_nearest_expiry_and_lower_strike_tie() -> None:
    master = pd.DataFrame(
        [
            {"name": "ABC", "expiry": "2026-09-29", "strike": 95, "instrument_type": "CE", "instrument_token": 1, "tradingsymbol": "ABC95CE", "lot_size": 50},
            {"name": "ABC", "expiry": "2026-09-29", "strike": 105, "instrument_type": "CE", "instrument_token": 2, "tradingsymbol": "ABC105CE", "lot_size": 50},
            {"name": "ABC", "expiry": "2026-10-27", "strike": 100, "instrument_type": "CE", "instrument_token": 3, "tradingsymbol": "ABC100OCTCE", "lot_size": 50},
            {"name": "ABC", "expiry": "2026-09-29", "strike": 100, "instrument_type": "PE", "instrument_token": 4, "tradingsymbol": "ABC100PE", "lot_size": 50},
        ]
    )
    selected = engine.select_atm_option_contract(
        master,
        underlying="ABC",
        session_date=date(2026, 9, 1),
        cash_entry_price=100.0,
        side="LONG",
    )
    assert selected["tradingsymbol"] == "ABC95CE"
    assert bool(selected["atm_tie"])
    assert selected["atm_tie_candidate_strikes"] == "95,105"


def test_causal_price_rounds_seconds_and_skips_zero_volume() -> None:
    fill = engine.causal_execution_price(
        _candles(), "2026-09-01T09:27:10+05:30", max_delay_minutes=5
    )
    assert fill is not None
    assert fill["price"] == 12
    assert fill["execution_ts"] == pd.Timestamp("2026-09-01T09:28:00+05:30")


def test_eod_close_is_explicit_and_requires_volume() -> None:
    fill = engine.causal_execution_price(
        _candles(),
        "2026-09-01T15:30:00+05:30",
        max_delay_minutes=0,
        allow_eod_close=True,
    )
    assert fill is not None
    assert fill["price"] == 21
    assert fill["price_field"] == "EOD_CLOSE"


def test_one_lot_option_pnl_and_charges() -> None:
    row = engine._execute_derivative_row(
        {
            "side": "SHORT",
            "cash_entry_event_ts": pd.Timestamp("2026-09-01T09:27:00+05:30"),
            "cash_exit_event_ts": pd.Timestamp("2026-09-01T15:30:00+05:30"),
        },
        instrument="OPTIONS",
        derivative_symbol="ABC100PE",
        derivative_token=123,
        expiry="2026-09-29",
        strike=100.0,
        option_type="PE",
        lot_size=50,
        tick_size=0.05,
        candles=_candles(),
        max_delay_minutes=5,
    )
    assert row["execution_status"] == "EXECUTED"
    assert row["quantity"] == 50
    assert row["gross_pnl_rs"] == pytest.approx((21 - 12) * 50)
    assert row["estimated_net_pnl_rs"] < row["gross_pnl_rs"]


def test_missing_positive_volume_fails_closed() -> None:
    candles = _candles().copy()
    candles["volume"] = 0
    assert engine.causal_execution_price(candles, "2026-09-01T09:27:00+05:30") is None
