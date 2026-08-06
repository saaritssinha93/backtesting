from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

import research_v12_late_compression_simple_long_backtest as strategy


IST = "Asia/Kolkata"


def _signal_states() -> tuple[pd.DataFrame, pd.DataFrame]:
    times = pd.to_datetime(
        ["2026-07-01 14:00", "2026-07-01 14:05"]
    ).tz_localize(IST)
    states = pd.DataFrame(
        {
            "ticker": ["TEST", "TEST"],
            "slot_ist": pd.to_datetime(
                ["2026-07-01 13:20", "2026-07-01 13:20"]
            ).tz_localize(IST),
            "signal_time_ist": times,
            "selection_rank": [10, 10],
            "valid": [True, True],
            "compression_history_complete": [True, True],
            "signal_source_1m_complete": [True, True],
            "signal_open": [100.0, 101.0],
            "signal_high": [101.0, 102.0],
            "signal_low": [99.8, 100.8],
            "signal_close": [100.8, 101.8],
            "signal_volume": [20_000, 20_000],
            "traded_value_rs": [2_016_000.0, 2_036_000.0],
            "bb_compressed_literal": [True, True],
            "breakout_level": [100.5, 101.5],
            "session_vwap": [100.0, 101.0],
            "relative_volume": [1.5, 1.5],
        }
    )
    nifty = pd.DataFrame(
        {
            "signal_time_ist": times,
            "nifty_above_vwap": [True, True],
            "nifty_close": [25000.0, 25010.0],
            "nifty_session_vwap": [24990.0, 24995.0],
        }
    )
    return states, nifty


def test_signal_gate_uses_only_frozen_rules_and_first_attempt() -> None:
    states, nifty = _signal_states()
    evaluated, armed, funnel = strategy.apply_frozen_signal_gate(states, nifty)

    assert evaluated["frozen_signal_pass"].tolist() == [True, True]
    assert len(armed) == 1
    assert armed.iloc[0]["signal_time_ist"] == states.iloc[0]["signal_time_ist"]
    assert funnel[-1]["before"] == 2
    assert funnel[-1]["after"] == 1
    assert not any(
        name in evaluated.columns
        for name in ("adx", "rsi", "stoch_k", "confirmation_score")
    )


def test_signal_gate_requires_strict_break_and_mandatory_rvol() -> None:
    states, nifty = _signal_states()
    states.loc[0, "signal_close"] = states.loc[0, "breakout_level"]
    states.loc[1, "relative_volume"] = 1.2499

    evaluated, armed, _ = strategy.apply_frozen_signal_gate(states, nifty)

    assert not evaluated["frozen_signal_pass"].any()
    assert armed.empty


def _entry_bars() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2026-07-01 14:01",
                    "2026-07-01 14:02",
                    "2026-07-01 14:03",
                ]
            ).tz_localize(IST),
            "open": [100.0, 100.0, 100.0],
            "high": [100.0, 100.10, 100.10],
            "low": [99.8, 99.8, 99.8],
            "close": [100.0, 100.05, 100.05],
        }
    )


def test_entry_is_next_three_minutes_cancel_first_and_gap_guarded() -> None:
    bars = _entry_bars()
    signal = pd.Timestamp("2026-07-01 14:00", tz=IST)

    filled = strategy.resolve_entry_strict(bars, signal, 100.0, 99.5, 99.6)
    assert filled.reason == "filled"
    assert filled.entry_time == pd.Timestamp("2026-07-01 14:02", tz=IST)
    assert filled.fill_raw == pytest.approx(100.05)

    cancelled_bars = bars.copy()
    cancelled_bars.loc[1, "low"] = 99.6
    cancelled = strategy.resolve_entry_strict(
        cancelled_bars, signal, 100.0, 99.5, 99.6
    )
    assert cancelled.reason == "cancel_before_or_ambiguous_trigger"

    gap_bars = bars.copy()
    gap_bars.loc[1, "open"] = 100.30
    gap_bars.loc[1, "high"] = 100.35
    gap = strategy.resolve_entry_strict(gap_bars, signal, 100.0, 99.5, 99.6)
    assert gap.reason == "entry_gap_over_0p20pct"


def test_trigger_normalises_float32_noise_before_adding_one_tick() -> None:
    noisy_exchange_high = float(np.float32(868.40))
    assert noisy_exchange_high > 868.40
    assert strategy.next_exchange_tick(noisy_exchange_high) == pytest.approx(868.45)
    assert strategy.next_exchange_tick(100.00) == pytest.approx(100.05)
    assert strategy.next_exchange_tick(100.01) == pytest.approx(100.05)


def test_causal_sizing_caps_at_two_percent_of_expected_one_minute_volume() -> None:
    sizing = strategy.causal_sizing(signal_volume=20_000, entry_price=100.0)
    assert sizing.notional_quantity == 1_000
    assert sizing.expected_one_minute_volume == pytest.approx(4_000)
    assert sizing.capacity_quantity == 80
    assert sizing.quantity == 80

    liquid = strategy.causal_sizing(signal_volume=1_000_000, entry_price=100.0)
    assert liquid.capacity_quantity == 4_000
    assert liquid.quantity == 1_000


def test_entry_fails_closed_when_any_search_minute_is_missing() -> None:
    bars = _entry_bars().drop(index=1)
    result = strategy.resolve_entry_strict(
        bars,
        pd.Timestamp("2026-07-01 14:00", tz=IST),
        100.0,
        99.5,
        99.6,
    )
    assert result.reason == "missing_entry_minute"


def test_exit_is_stop_first_and_non_target_slippage_is_adverse() -> None:
    bars = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-07-01 14:02"]).tz_localize(IST),
            "open": [100.0],
            "high": [100.80],
            "low": [99.20],
            "close": [100.0],
        }
    )
    result = strategy.resolve_fixed_exit(
        bars, pd.Timestamp("2026-07-01 14:02", tz=IST), 100.0
    )
    assert result is not None
    assert result["outcome"] == "SL"
    assert result["target_stop_tie"] is True
    assert result["raw_exit_price"] == pytest.approx(99.30)
    assert result["exit_price"] == pytest.approx(99.30 * 0.9995)


def test_literal_bb_reference_excludes_tested_previous_width() -> None:
    dates = pd.date_range(
        "2026-07-01 09:20", periods=70, freq="5min", tz=IST
    )
    base = np.linspace(100.0, 103.0, len(dates))
    raw = pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.05,
            "high": base + 0.10,
            "low": base - 0.10,
            "close": base + np.sin(np.arange(len(dates)) / 3.0) * 0.10,
            "volume": np.full(len(dates), 10_000),
            "source_1m_count": np.full(len(dates), 5),
            "gap_filled": np.zeros(len(dates)),
            "opening_snapshot": np.zeros(len(dates), dtype=bool),
        }
    )
    out = strategy.add_distilled_features(raw)
    i = 60
    expected = out["bb_width"].iloc[i - 21 : i - 1].quantile(0.25)

    assert len(out["bb_width"].iloc[i - 21 : i - 1]) == 20
    assert out.iloc[i]["bb_q25_preceding20"] == pytest.approx(expected)
    assert out.iloc[i]["previous_bb_width"] == pytest.approx(
        out.iloc[i - 1]["bb_width"]
    )


def test_relative_volume_reference_excludes_invalid_completed_bars() -> None:
    dates = pd.to_datetime(
        [
            "2026-06-01 14:00",
            "2026-06-02 14:00",
            "2026-06-03 14:00",
            "2026-06-04 14:00",
            "2026-06-05 14:00",
            "2026-06-08 14:00",
            "2026-06-09 14:00",
        ]
    ).tz_localize(IST)
    volumes = [10, 20, 35, 30, 40, 50, 60]
    raw = pd.DataFrame(
        {
            "date": dates,
            "open": np.full(7, 100.0),
            "high": np.full(7, 101.0),
            "low": np.full(7, 99.0),
            "close": np.full(7, 100.0),
            "volume": volumes,
            "source_1m_count": np.full(7, 5),
            "gap_filled": [False, False, True, False, False, False, False],
            "opening_snapshot": np.zeros(7, dtype=bool),
        }
    )
    out = strategy.add_distilled_features(raw)
    # The invalid volume 35 is excluded: median(10,20,30,40,50) == 30.
    assert out.iloc[-1]["relative_volume_reference_count"] == 5
    assert out.iloc[-1]["rel_volume"] == pytest.approx(2.0)


def test_remove_best_five_days_removes_days_not_trades() -> None:
    trades = pd.DataFrame(
        {
            "trade_date": [f"2026-07-{day:02d}" for day in range(1, 8)],
            "net_pnl_rs": [700, 600, 500, 400, 300, 200, -100],
        }
    )
    remaining, result = strategy.remove_best_five_days(trades)
    assert result["removed_days"] == [
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
        "2026-07-05",
    ]
    assert remaining["net_pnl_rs"].tolist() == [200, -100]
    assert math.isclose(result["profit_factor"], 2.0)
