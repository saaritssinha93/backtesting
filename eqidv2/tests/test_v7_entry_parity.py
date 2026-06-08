"""Entry-time contract tests for the active V7 T+1 path."""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_entry_lag_and_scheduler_are_t1():
    import eqidv2_entry_engine_1min_v5_id as ee

    assert ee.ENTRY_SIGNAL_TO_ENTRY_LAG_MIN == 1
    assert ee.ENTRY_DELAY_SEC == 60
    now = pd.Timestamp("2026-06-07 10:00:30+05:30").to_pydatetime()
    assert ee._next_entry_run_after(now) == pd.Timestamp("2026-06-07 10:01:00+05:30")


def test_scheduler_grace_reserves_two_seconds_before_t1_30_deadline():
    import eqidv2_entry_engine_1min_v5_id as ee

    assert ee.MAX_SIGNAL_HANDOFF_LAG_SEC == 30
    assert ee.ENTRY_PROCESS_RESERVE_SEC == 2
    assert ee.ENTRY_DUE_GRACE_SEC == 28
    now = pd.Timestamp("2026-06-07 10:01:28+05:30").to_pydatetime()
    assert ee._next_entry_run_after(now) == pd.Timestamp("2026-06-07 10:01:00+05:30")


def test_entry_rows_use_t1():
    import eqidv2_entry_engine_1min_v5_id as ee

    candidates = pd.DataFrame(
        [
            {
                "ticker": "RELIANCE",
                "side": "LONG",
                "setup": "C_OR_BREAKOUT",
                "signal_time_ist": "2026-06-07 10:00:00+05:30",
                "quality_score": 80.0,
                "rs_pct": 0.5,
                "market_ret_pct": 0.1,
                "regime": "BULL",
                "vol_ratio": 2.0,
                "atr_pct": 0.004,
                "body_pct": 0.60,
                "close_loc": 0.70,
                "vwap_dist_atr": 1.0,
                "candidate_id": "TEST_PARITY",
                "scan_session": "test",
                "selection_mode": "v8_setup_compatible",
                "reason": "test",
                "signal_open": 2490.0,
                "signal_high": 2510.0,
                "signal_low": 2480.0,
                "signal_close": 2505.0,
                "signal_volume": 5000,
                "ranker_score": 0.75,
            }
        ]
    )
    raw_by_ticker = {
        "RELIANCE": pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-06-07 10:00:00+05:30"),
                    "open": 2490.0,
                    "high": 2500.0,
                    "low": 2485.0,
                    "close": 2498.0,
                },
                {
                    "date": pd.Timestamp("2026-06-07 10:01:00+05:30"),
                    "open": 2505.0,
                    "high": 2510.0,
                    "low": 2500.0,
                    "close": 2508.0,
                },
            ]
        )
    }

    rows = ee._build_entry_rows(candidates, raw_by_ticker)

    assert len(rows) == 1
    assert "10:01" in str(rows.iloc[0]["intended_entry_ist"])
    assert "10:01" in str(rows.iloc[0]["entry_time_ist"])
    assert float(rows.iloc[0]["entry_price"]) == 2505.0
