"""Entry-time contract tests for the active V7 T+1 path."""

import os
import sys
import tempfile
from pathlib import Path

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


def test_startup_backfill_before_first_slot_is_empty():
    import eqidv2_entry_engine_1min_v5_id as ee

    assert ee._startup_expired_slot_keys(pd.Timestamp("2026-06-07 09:09:00+05:30")) == set()
    assert ee._startup_expired_slot_keys(pd.Timestamp("2026-06-07 09:19:00+05:30")) == set()


def test_startup_backfill_marks_only_expired_entry_deadlines():
    import eqidv2_entry_engine_1min_v5_id as ee

    assert ee._startup_expired_slot_keys(pd.Timestamp("2026-06-07 09:21:28+05:30")) == set()
    assert ee._startup_expired_slot_keys(pd.Timestamp("2026-06-07 09:21:29+05:30")) == {
        "20260607_0920"
    }


def test_fno_ban_helper_uses_module_datetime_binding():
    import eqidv2_entry_engine_1min_v5_id as ee

    old_enabled = ee.FNO_BAN_FILTER_ENABLED
    old_local_file = ee.FNO_BAN_LOCAL_FILE
    old_url = ee.FNO_BAN_URL
    old_cache = dict(ee._fno_ban_cache)
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file = Path(tmp_dir) / "fo_secban.csv"
            local_file.write_text("SYMBOL\nRELIANCE\n", encoding="utf-8")
            ee.FNO_BAN_FILTER_ENABLED = True
            ee.FNO_BAN_LOCAL_FILE = str(local_file)
            ee.FNO_BAN_URL = "file:///not-used.csv"
            ee._fno_ban_cache.clear()

            assert "RELIANCE" in ee._get_fno_banned_tickers()
    finally:
        ee.FNO_BAN_FILTER_ENABLED = old_enabled
        ee.FNO_BAN_LOCAL_FILE = old_local_file
        ee.FNO_BAN_URL = old_url
        ee._fno_ban_cache.clear()
        ee._fno_ban_cache.update(old_cache)


def test_fno_ban_http_fallback_is_bounded_fail_open():
    import eqidv2_entry_engine_1min_v5_id as ee

    old_enabled = ee.FNO_BAN_FILTER_ENABLED
    old_local_file = ee.FNO_BAN_LOCAL_FILE
    old_url = ee.FNO_BAN_URL
    old_timeout = ee.FNO_BAN_FETCH_TIMEOUT_SEC
    old_urlopen = ee._urlrequest.urlopen
    old_cache = dict(ee._fno_ban_cache)
    seen = {}

    def fake_urlopen(url, timeout=None):
        seen["url"] = url
        seen["timeout"] = timeout
        raise TimeoutError("simulated NSE stall")

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            ee.FNO_BAN_FILTER_ENABLED = True
            ee.FNO_BAN_LOCAL_FILE = str(Path(tmp_dir) / "missing_fo_secban.csv")
            ee.FNO_BAN_URL = "https://example.invalid/fo_secban.csv"
            ee.FNO_BAN_FETCH_TIMEOUT_SEC = 0.75
            ee._urlrequest.urlopen = fake_urlopen
            ee._fno_ban_cache.clear()

            assert ee._get_fno_banned_tickers() == frozenset()
            request = seen["url"]
            assert getattr(request, "full_url", request) == ee.FNO_BAN_URL
            assert seen["timeout"] == 0.75
    finally:
        ee.FNO_BAN_FILTER_ENABLED = old_enabled
        ee.FNO_BAN_LOCAL_FILE = old_local_file
        ee.FNO_BAN_URL = old_url
        ee.FNO_BAN_FETCH_TIMEOUT_SEC = old_timeout
        ee._urlrequest.urlopen = old_urlopen
        ee._fno_ban_cache.clear()
        ee._fno_ban_cache.update(old_cache)


def test_adv_cap_reject_does_not_require_external_cand_id_binding():
    import eqidv2_entry_engine_1min_v5_id as ee

    old_adv_enabled = ee.ADV_CAP_ENABLED
    old_get_adv_rs = ee._get_adv_rs
    try:
        ee.ADV_CAP_ENABLED = True
        ee._get_adv_rs = lambda ticker: 1.0
        candidates = pd.DataFrame(
            [
                {
                    "ticker": "RELIANCE",
                    "side": "LONG",
                    "setup": "C_OR_BREAKOUT",
                    "signal_time_ist": "2026-06-07 10:00:00+05:30",
                    "quality_score": 80.0,
                    "atr_pct": 0.004,
                    "vwap_dist_atr": 2.5,
                    "candidate_id": "ADV_REJECT_TEST",
                    "signal_close": 2505.0,
                }
            ]
        )
        raw_by_ticker = {
            "RELIANCE": pd.DataFrame(
                [
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

        rows, adv_rejected, fno_rejected = ee._build_entry_rows(candidates, raw_by_ticker)

        assert rows.empty
        assert len(adv_rejected) == 1
        assert adv_rejected.iloc[0]["reject_reason"] == "adv_cap"
        assert fno_rejected.empty
    finally:
        ee.ADV_CAP_ENABLED = old_adv_enabled
        ee._get_adv_rs = old_get_adv_rs


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
                "vwap_dist_atr": 2.5,
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

    rows, adv_rejected, fno_rejected = ee._build_entry_rows(candidates, raw_by_ticker)

    assert len(rows) == 1
    assert adv_rejected.empty
    assert fno_rejected.empty
    assert "10:01" in str(rows.iloc[0]["intended_entry_ist"])
    assert "10:01" in str(rows.iloc[0]["entry_time_ist"])
    assert float(rows.iloc[0]["entry_price"]) == 2505.0
