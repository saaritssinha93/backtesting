from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_options_signals as signals


def fixture_order(*, hit_minute=27, filled=True):
    clock = pd.date_range("2026-09-01 09:27", periods=20, freq="min", tz=signals.IST)
    high = np.full(20, 99.)
    if hit_minute is not None:
        high[hit_minute - 27] = 101.
    path = {
        "timestamp_ns": clock.asi8,
        "high": high,
        "low": np.full(20, 97.),
        "close": np.arange(20, dtype=float) + 98.,
    }
    orders = pd.DataFrame([{
        "sid": 1, "day": "2026-09-01", "setup_id": "0926_LONG", "side": "LONG",
        "tradingsymbol": "STOCK", "signal_ts": "2026-09-01 09:25:00+05:30",
        "confirmation_ts": "2026-09-01 09:26:00+05:30", "trigger": 100.,
        "filled": filled,
        "entry_ts": f"2026-09-01 09:{hit_minute:02}:00+05:30" if filled else None,
    }])
    return orders, {1: path}


def test_next_five_minute_open_and_atm_use_completed_underlying_close():
    orders, paths = fixture_order()
    result = signals.causal_signal_rows(orders, paths).iloc[0]
    assert result.underlying_trigger_observed_ts == pd.Timestamp("2026-09-01 09:27", tz=signals.IST)
    assert result.entry_ts == pd.Timestamp("2026-09-01 09:30", tz=signals.IST)
    assert result.atm_spot_ts == result.entry_ts
    assert result.atm_spot == 101.
    assert result.signal_status == "READY"
    # Extreme future paths and stock outcome columns cannot affect the entry.
    paths[1]["close"][4:] = 10000.
    paths[1]["high"][4:] = 10001.
    orders["exit_ts"] = "2026-09-01 09:28:00+05:30"
    orders["portfolio_executed"] = False
    orders["net_profit_rupees"] = -999999.
    changed = signals.causal_signal_rows(orders, paths).iloc[0]
    pd.testing.assert_series_equal(result, changed)
    assert "exit_ts" not in changed.index


def test_equal_boundary_entry_is_allowed_with_zero_latency_assumption():
    orders, paths = fixture_order(hit_minute=30)
    result = signals.causal_signal_rows(orders, paths).iloc[0]
    assert result.entry_ts == result.underlying_trigger_observed_ts
    assert result.entry_ts.minute == 30
    assert result.atm_spot == 101.


def test_late_touch_does_not_resurrect_unfilled_stock_order():
    orders, paths = fixture_order(hit_minute=37, filled=False)
    result = signals.causal_signal_rows(orders, paths).iloc[0]
    assert result.signal_status == "UNDERLYING_UNFILLED"
    assert pd.isna(result.entry_ts)
    assert pd.isna(result.atm_spot)


def test_short_uses_low_trigger():
    orders, paths = fixture_order(hit_minute=None, filled=False)
    orders.loc[0, ["side", "setup_id", "trigger", "filled", "entry_ts"]] = [
        "SHORT", "0926_SHORT", 96., True, "2026-09-01 09:29:00+05:30",
    ]
    paths[1]["low"][2] = 95.
    result = signals.causal_signal_rows(orders, paths).iloc[0]
    assert result.underlying_trigger_observed_ts.minute == 29
    assert result.entry_ts.minute == 30


def test_trigger_parity_failure_blocks_result():
    orders, paths = fixture_order()
    orders["filled"] = False
    with pytest.raises(RuntimeError, match="fill flag"):
        signals.causal_signal_rows(orders, paths)
    orders["filled"] = True
    orders["entry_ts"] = "2026-09-01 09:28:00+05:30"
    with pytest.raises(RuntimeError, match="trigger time"):
        signals.causal_signal_rows(orders, paths)


def test_missing_exact_atm_close_cannot_use_a_future_or_stale_close():
    orders, paths = fixture_order(hit_minute=36)
    # Entry rounds to 09:40, beyond the ten-minute trigger validation window.
    paths[1]["timestamp_ns"][13] += 30_000_000_000
    with pytest.raises(RuntimeError, match="Missing exact completed underlying ATM minute"):
        signals.causal_signal_rows(orders, paths)


def test_frozen_source_hash_drift_is_rejected(tmp_path):
    path = tmp_path / "frozen.csv"
    path.write_text("original", encoding="utf-8")
    expected = signals._sha256(path)
    records = []
    signals._verified_file(path, expected, records)
    assert records[0]["verified"]
    path.write_text("changed", encoding="utf-8")
    with pytest.raises(RuntimeError, match="source drift"):
        signals._verified_file(path, expected, records)
