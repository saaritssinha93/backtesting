from __future__ import annotations

import pandas as pd

import research_v12_hourly_pullback_long_backtest as replay


IST = "Asia/Kolkata"


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz=IST)


def _row(
    time: str,
    open_: float,
    high: float,
    low: float,
    close: float,
    *,
    return_pct: float,
    raw_trigger: bool = False,
) -> dict:
    return {
        "ticker": "ABC",
        "trade_date": "2026-08-03",
        "slot_ist": _ts("2026-08-03 09:20"),
        "selection_rank": 10,
        "signal_time_ist": _ts(f"2026-08-03 {time}"),
        "signal_open": open_,
        "signal_high": high,
        "signal_low": low,
        "signal_close": close,
        "signal_volume": 1_000.0 if time != "10:05" else 700.0,
        "signal_atr": 1.0,
        "signal_adx": 26.0,
        "gap_filled": 0,
        "return_5m_close_pct": return_pct,
        "return_pair_exact": True,
        "session_vwap_causal": 100.8,
        "vwap_dist_atr": close - 100.8,
        "vwap_slope_3": 0.1,
        "volume_ratio20": 1.3,
        "traded_value_rs": close * 20_000,
        "close_location": (close - low) / (high - low),
        "upper_wick_fraction": (high - max(open_, close)) / (high - low),
        "range_atr": high - low,
        "plus_di": 30.0,
        "minus_di": 15.0,
        "adx_rising_3": True,
        "stoch_rising": True,
        "stoch_bullish": True,
        "nifty_aligned": True,
        "raw_two_bar_trigger": raw_trigger,
        "common_gate_pass": True,
        "context_score": 6,
    }


def test_pullback_reclaim_is_causal_and_uses_completed_confirmation() -> None:
    frame = pd.DataFrame(
        [
            _row("09:55", 100.0, 100.9, 99.9, 100.7, return_pct=0.7),
            _row(
                "10:00", 100.7, 101.7, 100.6, 101.5,
                return_pct=0.7944, raw_trigger=True,
            ),
            _row("10:05", 101.5, 101.55, 101.0, 101.1, return_pct=-0.3941),
            _row("10:10", 101.1, 101.8, 101.05, 101.75, return_pct=0.6429),
        ]
    )

    found = replay.find_pullback_reclaims(frame)

    assert len(found) == 1
    assert found.iloc[0]["signal_time_ist"] == _ts("2026-08-03 10:10")
    assert found.iloc[0]["pullback_bars"] == 1
    assert found.iloc[0]["pullback_low"] == 101.0


def test_membership_gap_cancels_pending_pullback_watch() -> None:
    frame = pd.DataFrame(
        [
            _row("09:55", 100.0, 100.9, 99.9, 100.7, return_pct=0.7),
            _row(
                "10:00", 100.7, 101.7, 100.6, 101.5,
                return_pct=0.7944, raw_trigger=True,
            ),
            _row("10:05", 101.5, 101.55, 101.0, 101.1, return_pct=-0.3941),
            _row("10:15", 101.1, 101.8, 101.05, 101.75, return_pct=0.6429),
        ]
    )

    assert replay.find_pullback_reclaims(frame).empty
