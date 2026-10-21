"""Causality and native-priority checks for G4 reversal exits."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.g4_reversal_exit import simulate_reversal_exit as simulate
from research import g4_reversal_sweep as sweep


def path(candles):
    stamps = pd.date_range("2026-10-16 15:08", periods=len(candles), freq="min",
                           tz="Asia/Kolkata").as_unit("ns").astype("int64")
    return dict(timestamp_ns=np.asarray(stamps),
                **{field: np.asarray([c[index] for c in candles], dtype=float)
                   for index, field in enumerate(("open", "high", "low", "close"))})


ADVERSE = dict(kind="adverse_persistence", loss_threshold_pct=.25,
               consecutive_closes=2)
PULLBACK = dict(kind="profitable_pullback", activation_profit_pct=.7,
                pullback_pct=.35, min_signal_profit_pct=.2)


def falling_candles():
    return [(100, 100, 100, 100), (100, 100, 99.7, 99.7),
            (99.7, 99.8, 99.6, 99.6), (99.55, 99.6, 99.4, 99.4),
            (99.4, 99.4, 98.7, 98.7), (98.7, 98.7, 98.7, 98.7),
            (98.7, 98.7, 98.7, 98.7), (98.7, 98.7, 98.7, 98.7)]


def test_catalog_fixed_and_unique():
    items = sweep.catalog()
    assert len(items) == 18
    assert len({item["policy"] for item in items}) == 18
    assert {item["family"] for item in items} == {
        "adverse_persistence", "profitable_pullback"}


def test_adverse_signal_requires_two_completed_closes_and_fills_next_open():
    result = simulate(path(falling_candles()), 0, 100., True, 2., ADVERSE)
    assert result["exit_reason"] == "REVERSAL_EXIT"
    assert result["exit_path_index"] == 3
    assert result["exit_price"] == pytest.approx(99.55)
    assert result["reversal_signal_path_index"] == 2
    assert result["reversal_exit_applied"]


def test_three_closes_cannot_use_stop_candle_close():
    candles = falling_candles()
    candles[3] = (99.55, 99.6, 98.7, 98.7)
    result = simulate(path(candles), 0, 100., True, 2.,
                      dict(ADVERSE, consecutive_closes=3))
    assert result["exit_reason"] == "STOP"
    assert not result["reversal_exit_applied"]


def test_profitable_pullback_exits_only_after_peak_and_retrace():
    candles = [(100, 100, 100, 100), (100, 100.8, 100, 100.8),
               (100.8, 100.8, 100.4, 100.4), (100.3, 100.3, 100.2, 100.2),
               (100.2, 100.2, 98.7, 98.7), (98.7, 98.7, 98.7, 98.7),
               (98.7, 98.7, 98.7, 98.7), (98.7, 98.7, 98.7, 98.7)]
    result = simulate(path(candles), 0, 100., True, 2., PULLBACK)
    assert result["exit_reason"] == "REVERSAL_EXIT"
    assert result["exit_path_index"] == 3
    assert result["exit_price"] == pytest.approx(100.3)
    assert result["gross_return_pct"] > 0


def test_short_adverse_reversal_is_directionally_symmetric():
    candles = [(100, 100, 100, 100), (100, 100.3, 100, 100.3),
               (100.3, 100.4, 100.3, 100.4), (100.45, 100.6, 100.45, 100.6),
               (100.6, 101.3, 100.6, 101.3), (101.3, 101.3, 101.3, 101.3),
               (101.3, 101.3, 101.3, 101.3), (101.3, 101.3, 101.3, 101.3)]
    result = simulate(path(candles), 0, 100., False, 2., ADVERSE)
    assert result["exit_reason"] == "REVERSAL_EXIT"
    assert result["exit_price"] == pytest.approx(100.45)
    assert result["reversal_signal_path_index"] == 2


def test_short_profitable_pullback_uses_best_low_close():
    candles = [(100, 100, 100, 100), (100, 100, 99.2, 99.2),
               (99.2, 99.6, 99.2, 99.6), (99.7, 99.8, 99.7, 99.8),
               (99.8, 101.3, 99.8, 101.3), (101.3, 101.3, 101.3, 101.3),
               (101.3, 101.3, 101.3, 101.3), (101.3, 101.3, 101.3, 101.3)]
    result = simulate(path(candles), 0, 100., False, 2., PULLBACK)
    assert result["exit_reason"] == "REVERSAL_EXIT"
    assert result["exit_price"] == pytest.approx(99.7)
    assert result["gross_return_pct"] > 0


def test_native_gap_stop_takes_precedence_over_pending_reversal():
    candles = falling_candles()
    candles[3] = (98.5, 98.5, 98.4, 98.4)
    candles[4:] = [(98.4, 98.4, 98.4, 98.4)] * 4
    result = simulate(path(candles), 0, 100., True, 2., ADVERSE)
    assert result["exit_reason"] == "STOP"
    assert result["exit_price"] == 98.5
    assert not result["reversal_exit_applied"]


def test_native_target_on_signal_candle_takes_precedence():
    candles = falling_candles()
    candles[2] = (99.7, 102., 99.6, 99.6)
    result = simulate(path(candles), 0, 100., True, 2., ADVERSE)
    assert result["exit_reason"] == "TARGET"
    assert not result["reversal_exit_applied"]


def test_invalid_signal_parameters_rejected():
    with pytest.raises(ValueError, match="Invalid adverse"):
        simulate(path(falling_candles()), 0, 100., True, 2.,
                 dict(ADVERSE, consecutive_closes=1))
