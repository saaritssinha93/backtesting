"""Causality and selection checks for research-only G4 percentage trailing."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from research.g4_percent_trail_exit import simulate_percentage_trailing_exit as simulate
from research import g4_percent_trail_sweep as sweep


def path(candles):
    stamps = pd.date_range("2026-10-16 15:10", periods=len(candles), freq="min",
                           tz="Asia/Kolkata").as_unit("ns").astype("int64")
    return dict(timestamp_ns=np.asarray(stamps),
                **{field: np.asarray([c[index] for c in candles], dtype=float)
                   for index, field in enumerate(("open", "high", "low", "close"))})


BASE = dict(initial_stop_pct=1.2, tighten_after_minutes=120,
            tightened_stop_pct=1., activation_floor_pct=.4,
            activation_target_fraction=.5, trail_distance_pct=.25,
            protected_profit_pct=None, cost_bps_round_trip=5.)


def test_catalog_is_predeclared_and_has_both_families():
    items = sweep.catalog()
    assert len(items) == 72
    assert len({item["policy"] for item in items}) == 72
    assert {item["family"] for item in items} == {
        "percentage_only", "percentage_with_profit_floor"}
    assert all(item["parameters"]["cost_bps_round_trip"] == 5. for item in items)


def test_long_trail_observes_close_then_exits_next_candle():
    candles = [(100, 100, 100, 100), (100, 101.2, 100, 101.2),
               (101.1, 101.1, 100.8, 100.9), (100.9, 100.9, 100.9, 100.9),
               (100.9, 100.9, 100.9, 100.9), (100.9, 100.9, 100.9, 100.9)]
    result = simulate(path(candles), 0, 100., True, 2., BASE)
    assert result["exit_reason"] == "PERCENT_TRAILING_STOP"
    assert result["exit_path_index"] == 2
    assert result["exit_price"] == pytest.approx(101.2 * .9975)
    assert result["protection_armed_path_index"] == 1
    assert result["protection_effective_path_index"] == 2
    assert not result["exit_gap_through"]


def test_short_trail_is_directionally_symmetric():
    candles = [(100, 100, 100, 100), (100, 100, 98.8, 98.8),
               (98.9, 99.2, 98.9, 99.1), (99.1, 99.1, 99.1, 99.1),
               (99.1, 99.1, 99.1, 99.1), (99.1, 99.1, 99.1, 99.1)]
    result = simulate(path(candles), 0, 100., False, 2., BASE)
    assert result["exit_reason"] == "PERCENT_TRAILING_STOP"
    assert result["exit_price"] == pytest.approx(98.8 * 1.0025)
    assert result["gross_return_pct"] > 0


def test_gap_through_fills_open_not_resting_stop():
    candles = [(100, 100, 100, 100), (100, 101.2, 100, 101.2),
               (100.5, 100.5, 100.4, 100.4), (100.4, 100.4, 100.4, 100.4),
               (100.4, 100.4, 100.4, 100.4), (100.4, 100.4, 100.4, 100.4)]
    result = simulate(path(candles), 0, 100., True, 2., BASE)
    assert result["exit_event"] == "OPEN"
    assert result["exit_price"] == 100.5
    assert result["exit_gap_through"]


def test_trail_ratchets_and_never_loosens():
    candles = [(100, 100, 100, 100), (100, 101.2, 100, 101.2),
               (101.2, 101.5, 101.1, 101.5), (101.5, 101.5, 101.2, 101.3),
               (101.3, 101.3, 101.3, 101.3), (101.3, 101.3, 101.3, 101.3)]
    result = simulate(path(candles), 0, 100., True, 2., BASE)
    updates = json.loads(result["stop_updates_json"])
    assert len(updates) == 2
    assert updates[1]["ratcheted_stop"] > updates[0]["ratcheted_stop"]
    assert result["exit_price"] == pytest.approx(101.5 * .9975)


def test_optional_profit_floor_changes_stop_without_early_activation():
    config = dict(BASE, trail_distance_pct=1.2, protected_profit_pct=.2)
    candles = [(100, 100, 100, 100), (100, 101.2, 100, 101.2),
               (101, 101, 100.1, 100.1), (100.1, 100.1, 100.1, 100.1),
               (100.1, 100.1, 100.1, 100.1), (100.1, 100.1, 100.1, 100.1)]
    result = simulate(path(candles), 0, 100., True, 2., config)
    assert result["exit_price"] == pytest.approx(100.2)
    assert result["protection_armed_path_index"] == 1


def test_invalid_distance_rejected():
    candles = [(100, 100, 100, 100)] * 6
    with pytest.raises(ValueError, match="Invalid percentage"):
        simulate(path(candles), 0, 100., True, 2., dict(BASE, trail_distance_pct=0))
