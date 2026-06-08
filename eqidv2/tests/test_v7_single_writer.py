"""
Phase 0 characterization tests: single writer enforcement.

Confirms:
  - Legacy scanner's _write_side_signals_csv raises RuntimeError when
    LEGACY_LIVE_CSV_WRITE_ENABLED=False (the production default).
  - eqidv2_live_signal_writer.write_side_signals() stamps writer_name correctly.
  - A signal written by a non-authorised writer is distinguishable by writer_name.

Never writes to C:\\TradingData.  Uses a temp directory.
"""

import sys
import os
import tempfile
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import pytest

import eqidv2_live_combined_analyser_csv_id_5min_v7_persistent as v7_persistent
import eqidv2_live_signal_writer as signal_writer


def _minimal_entry_df():
    return pd.DataFrame([{
        "ticker": "RELIANCE",
        "side": "SHORT",
        "setup": "S_BB_SQUEEZE_SHORT",
        "bar_time_ist": "2026-06-07 10:00:00+05:30",
        "signal_time_ist": "2026-06-07 10:00:00+05:30",
        "intended_entry_ist": "2026-06-07 10:01:00+05:30",
        "entry_time_ist": "2026-06-07 10:01:00+05:30",
        "entry_price": 2500.0,
        "sl_price": 2518.75,
        "target_price": 2462.5,
        "quantity": 20,
        "quality_score": 90.0,
        "score": 90.0,
        "atr_pct": 0.004,
        "signal_close": 2500.0,
        "candidate_id": "TEST123",
        "diagnostics_json": "{}",
    }])


# ---------------------------------------------------------------------------
# Legacy writer raises when production flag is off
# ---------------------------------------------------------------------------

def test_legacy_writer_raises_when_disabled(monkeypatch):
    monkeypatch.setattr(v7_persistent, "LEGACY_LIVE_CSV_WRITE_ENABLED", False)
    with pytest.raises(RuntimeError, match="entry_engine_1min_v5_id"):
        v7_persistent._write_side_signals_csv(
            pd.DataFrame(), side="SHORT", signal_day_str="2026-06-07"
        )


def test_legacy_writer_does_not_raise_when_enabled(monkeypatch, tmp_path):
    # With the flag on it should attempt to run (may fail on missing CSV dir —
    # we just confirm no RuntimeError is raised by the guard itself).
    monkeypatch.setattr(v7_persistent, "LEGACY_LIVE_CSV_WRITE_ENABLED", True)
    monkeypatch.setattr(v7_persistent, "LIVE_SIGNALS_DIR", str(tmp_path))
    try:
        v7_persistent._write_side_signals_csv(
            pd.DataFrame(), side="SHORT", signal_day_str="2026-06-07"
        )
    except RuntimeError as exc:
        if "entry_engine_1min_v5_id" in str(exc):
            pytest.fail("RuntimeError guard should NOT fire when flag is enabled")


# ---------------------------------------------------------------------------
# Neutral writer stamps writer_name
# ---------------------------------------------------------------------------

def test_neutral_writer_stamps_writer_name(tmp_path, monkeypatch):
    # Intercept base_v15.now_ist so lag is predictable (write happens at entry time + 10s)
    import eqidv2_live_combined_analyser_csv_v15 as base_v15
    fixed_now = pd.Timestamp("2026-06-07 10:01:10+05:30")
    monkeypatch.setattr(base_v15, "now_ist", lambda: fixed_now)

    df = _minimal_entry_df()
    result = signal_writer.write_side_signals(
        df,
        side="SHORT",
        signal_day_str="2026-06-07",
        live_signals_dir=tmp_path,
        writer_name="entry_engine_1min_v5_id",
        writer_pid=12345,
        source_session="test",
        validate_timing=False,   # skip timing so test isn't time-sensitive
    )

    assert result.written == 1, f"Expected 1 written, got {result.written}"

    csv_path = tmp_path / "signals_2026-06-07_id_5min_v7_short.csv"
    assert csv_path.exists()
    written_df = pd.read_csv(csv_path)
    assert "writer_name" in written_df.columns
    assert written_df["writer_name"].iloc[0] == "entry_engine_1min_v5_id"
    assert str(written_df["writer_pid"].iloc[0]) == "12345"


def test_neutral_writer_rejects_duplicate_ticker_same_day(tmp_path, monkeypatch):
    import eqidv2_live_combined_analyser_csv_v15 as base_v15
    fixed_now = pd.Timestamp("2026-06-07 10:01:10+05:30")
    monkeypatch.setattr(base_v15, "now_ist", lambda: fixed_now)

    df = _minimal_entry_df()
    r1 = signal_writer.write_side_signals(
        df, side="SHORT", signal_day_str="2026-06-07",
        live_signals_dir=tmp_path, writer_name="entry_engine_1min_v5_id",
        validate_timing=False,
    )
    r2 = signal_writer.write_side_signals(
        df, side="SHORT", signal_day_str="2026-06-07",
        live_signals_dir=tmp_path, writer_name="entry_engine_1min_v5_id",
        validate_timing=False,
    )
    assert r1.written == 1
    assert r2.written == 0
    assert r2.skipped_intraday_ticker == 1
