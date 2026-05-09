"""v17D setup library — registry of 20 candidate detectors.

Public API:
    ALL_DETECTORS: dict[str, Callable]       (setup_id -> detect function)
    DETECTORS_BY_FAMILY: dict[str, list[str]] (family -> setup_ids)
    scan_all(df, ticker, cfg=None) -> list[Signal]
        Run every detector and return all signals.
"""
from __future__ import annotations

from typing import Callable

import pandas as pd

from .common import Signal
from .family_trend_continuation import (
    detect_tc_1_pullback_ema20,
    detect_tc_2_pullback_ema50,
    detect_tc_3_hh_hl_momentum,
    detect_tc_4_trendday_vwap_pullback,
)
from .family_mean_reversion import (
    detect_mr_1_bb_rsi_extreme,
    detect_mr_2_vwap_fade,
    detect_mr_3_overext_ema_reversion,
    detect_mr_4_three_bar_drive,
)
from .family_breakout import (
    detect_bo_1_donchian_break,
    detect_bo_2_squeeze_release,
    detect_bo_3_htf_level_break,
    detect_bo_4_or15_break,
)
from .family_pattern import (
    detect_pt_1_gap_and_go,
    detect_pt_2_gap_fill_fade,
    detect_pt_3_inside_bar_break,
)
from .family_volume import (
    detect_vo_1_climax_reversal,
    detect_vo_2_low_drift_accel,
    detect_vo_3_obv_divergence,
)
from .family_timeofday import (
    detect_td_1_opening_drive,
    detect_td_2_late_day_reversal,
)

ALL_DETECTORS: dict[str, Callable] = {
    "TC_1_PULLBACK_EMA20":             detect_tc_1_pullback_ema20,
    "TC_2_PULLBACK_EMA50":             detect_tc_2_pullback_ema50,
    "TC_3_HH_HL_MOMENTUM":             detect_tc_3_hh_hl_momentum,
    "TC_4_TRENDDAY_VWAP_PULLBACK":     detect_tc_4_trendday_vwap_pullback,
    "MR_1_BB_RSI_EXTREME":             detect_mr_1_bb_rsi_extreme,
    "MR_2_VWAP_FADE":                  detect_mr_2_vwap_fade,
    "MR_3_OVEREXT_EMA_REVERSION":      detect_mr_3_overext_ema_reversion,
    "MR_4_THREE_BAR_DRIVE":            detect_mr_4_three_bar_drive,
    "BO_1_DONCHIAN_BREAK":             detect_bo_1_donchian_break,
    "BO_2_SQUEEZE_RELEASE":            detect_bo_2_squeeze_release,
    "BO_3_HTF_LEVEL_BREAK":            detect_bo_3_htf_level_break,
    "BO_4_OR15_BREAK":                 detect_bo_4_or15_break,
    "PT_1_GAP_AND_GO":                 detect_pt_1_gap_and_go,
    "PT_2_GAP_FILL_FADE":              detect_pt_2_gap_fill_fade,
    "PT_3_INSIDE_BAR_BREAK":           detect_pt_3_inside_bar_break,
    "VO_1_CLIMAX_REVERSAL":            detect_vo_1_climax_reversal,
    "VO_2_LOW_DRIFT_ACCEL":            detect_vo_2_low_drift_accel,
    "VO_3_OBV_DIVERGENCE":             detect_vo_3_obv_divergence,
    "TD_1_OPENING_DRIVE":              detect_td_1_opening_drive,
    "TD_2_LATE_DAY_REVERSAL":          detect_td_2_late_day_reversal,
}

DETECTORS_BY_FAMILY: dict[str, list[str]] = {
    "trend_continuation":  ["TC_1_PULLBACK_EMA20", "TC_2_PULLBACK_EMA50",
                            "TC_3_HH_HL_MOMENTUM", "TC_4_TRENDDAY_VWAP_PULLBACK"],
    "mean_reversion":      ["MR_1_BB_RSI_EXTREME", "MR_2_VWAP_FADE",
                            "MR_3_OVEREXT_EMA_REVERSION", "MR_4_THREE_BAR_DRIVE"],
    "breakout":            ["BO_1_DONCHIAN_BREAK", "BO_2_SQUEEZE_RELEASE",
                            "BO_3_HTF_LEVEL_BREAK", "BO_4_OR15_BREAK"],
    "pattern":             ["PT_1_GAP_AND_GO", "PT_2_GAP_FILL_FADE",
                            "PT_3_INSIDE_BAR_BREAK"],
    "volume":              ["VO_1_CLIMAX_REVERSAL", "VO_2_LOW_DRIFT_ACCEL",
                            "VO_3_OBV_DIVERGENCE"],
    "time_of_day":         ["TD_1_OPENING_DRIVE", "TD_2_LATE_DAY_REVERSAL"],
}

assert sum(len(v) for v in DETECTORS_BY_FAMILY.values()) == len(ALL_DETECTORS) == 20


def scan_all(
    df: pd.DataFrame,
    ticker: str,
    cfg: dict | None = None,
    setups: list[str] | None = None,
) -> list[Signal]:
    """Run requested detectors (default: all) and return all signals."""
    targets = setups if setups else list(ALL_DETECTORS.keys())
    out: list[Signal] = []
    for setup_id in targets:
        fn = ALL_DETECTORS.get(setup_id)
        if fn is None:
            continue
        sub_cfg = (cfg or {}).get(setup_id) if cfg else None
        try:
            signals = fn(df, ticker, sub_cfg)
        except Exception as exc:
            print(f"[v17D scan] {setup_id} on {ticker} raised: {exc}")
            continue
        out.extend(signals)
    return out


__all__ = [
    "Signal", "ALL_DETECTORS", "DETECTORS_BY_FAMILY", "scan_all",
]
