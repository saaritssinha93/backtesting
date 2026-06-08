"""Paper executor freshness tests for the universal 15-second contract."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_all_setups_return_universal_threshold():
    import avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7 as ex

    for setup in (
        "A_MOD_BREAK_C1_HIGH",
        "B_HUGE_RED_FAILED_BOUNCE",
        "C_OR_BREAKOUT",
        "D_EMA20_BOUNCE",
        "UNKNOWN_SETUP",
    ):
        assert ex._late_lag_threshold_for_setup(setup) == 15


def test_per_setup_threshold_dict_is_removed():
    import avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7 as ex

    assert not hasattr(ex, "_LATE_LAG_THRESHOLDS_BY_SETUP")


def test_append_late_skipped_csv_accepts_none_lag(tmp_path, monkeypatch):
    import csv
    import glob

    import avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7 as ex

    monkeypatch.setattr(ex, "SIGNAL_DIR", str(tmp_path))
    signal = {
        "ticker": "RELIANCE",
        "side": "SHORT",
        "setup": "C_OR_BREAKOUT",
        "signal_id": "abc123",
        "detected_time_ist": None,
    }
    ex._append_late_skipped_csv(signal, None, 15)

    files = glob.glob(str(tmp_path / "late_skipped_*.csv"))
    assert len(files) == 1
    with open(files[0], newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["lag_sec"] == ""


def test_default_lag_is_15():
    import avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7 as ex

    assert ex.LATE_DETECTION_MAX_LAG_SEC == 15


def test_explicit_contract_deadline_is_authoritative():
    import avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7 as ex

    signal = {
        "deadline_ist": "2026-06-07 10:01:15+05:30",
        "detected_time_ist": "2026-06-07 10:01:10+05:30",
    }
    start = ex.IST.localize(ex.datetime(2026, 6, 7, 10, 1, 10))
    forced = ex.IST.localize(ex.datetime(2026, 6, 7, 15, 20, 0))
    deadline = ex._entry_retry_deadline(signal, start, forced)
    assert deadline == ex.IST.localize(ex.datetime(2026, 6, 7, 10, 1, 15))
