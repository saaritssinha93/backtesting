"""Isolation and chronological-selection checks for the G4 research runner."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

import fno_v13_replay_source as source
from research.g4_late_slot_backtest import DEFAULT_CONFIG, configuration, sweep
from research.g4_late_slot_source import (BASE_G3_V2_CLOCKS, LATE_CLOCKS,
                                          expanded_source_clocks)


def test_g4_raw_source_uses_pinned_g3_v2_clocks_and_restores_global_map():
    original = source.config.SIGNAL_TO_CONFIRMATION
    with expanded_source_clocks():
        assert set(source.config.SIGNAL_TO_CONFIRMATION) == set(BASE_G3_V2_CLOCKS) | set(LATE_CLOCKS)
        assert "09:20" not in source.config.SIGNAL_TO_CONFIRMATION
        assert source.config.SIGNAL_TO_CONFIRMATION["10:30"] == "10:31"
    assert source.config.SIGNAL_TO_CONFIRMATION is original


def test_g4_variant_choice_does_not_use_later_sessions():
    config = configuration(DEFAULT_CONFIG)
    base = {"price_change_pct": .1, "oi_change_pct": .05,
            "five_minute_volume_ratio_min": 1.3, "one_minute_body_ratio_min": .5,
            "one_minute_wick_ratio_max": .5, "one_minute_volume_ratio_min": .6}
    strict = dict(base, price_change_pct=.2)
    config["candidate_variants"]["LONG"] = [
        {"name": "base", "thresholds": base},
        {"name": "strict", "thresholds": strict},
    ]
    days = [date(2026, 1, 1) + timedelta(days=n) for n in range(49)]
    rows = []

    def candidate(day, symbol, price, value, win):
        rows.append(dict(day=str(day), tradingsymbol=symbol, hhmm_int=1005, side="LONG",
                         price_change_pct=price, oi_change_pct=.1, volume_ratio=2.,
                         body_ratio=.8, wick_ratio=.2, v9_1m_volume_ratio=1.5,
                         traded_value=value, filled=True, research_win=win,
                         net_return_pct=1. if win else -1.))

    for index in range(6):
        candidate(days[index], f"T{index}", .1 if index == 0 else .2,
                  100., index != 5)
    # On later days the strict option looks perfect while the base option
    # selects a higher-liquidity loss. Those outcomes must not tune the choice.
    for index in range(38, 41):
        candidate(days[index], f"H{index}", .1, 200., False)
        candidate(days[index], f"W{index}", .2, 100., True)
    _, chosen, decisions = sweep(pd.DataFrame(rows), days, config)
    assert chosen["10:05_LONG"] == 0
    decision = decisions.set_index("setup").loc["10:05_LONG"]
    assert decision.train_fills == 6
    assert decision.train_win_rate_pct > 70
    assert decision.holdout_win_rate_pct == 0
    assert not decision.qualified
