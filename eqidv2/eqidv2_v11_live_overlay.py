"""
Shared v11 backtesting strategy overlay for the v7 live pipeline.

The live pipeline has two stages: 5-minute candidate discovery and the
1-minute entry engine.  This module keeps the v11 selected-strategy profile,
Tier123 add-on candidates, and v11 exit overrides in one place so both stages
can apply the same setup rules.
"""

from __future__ import annotations

import math
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_candidate_scan as candidate_scan


IST_TZ = "Asia/Kolkata"
V11_LIVE_OVERLAY_VERSION = "v11_backtesting_tier123_balanced_2026_06_03"
TIER123_BALANCED_PROFILE = "production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced"
DEFAULT_SELECTED_STRATEGY_PROFILE = TIER123_BALANCED_PROFILE

V7_SIGNAL_MARGIN_RS = 20_000.0
V7_INTRADAY_LEVERAGE = 5.0
V7_SIGNAL_NOTIONAL_RS = V7_SIGNAL_MARGIN_RS * V7_INTRADAY_LEVERAGE

AB_PROBATION_SETUPS = tuple(
    setup for setup in sorted(v6.SETUP_EXIT_RULES)
    if str(setup).startswith(("A_", "B_"))
)
AB_GOOD_RESULT_SETUPS = (
    "A_MOD_BREAK_C1_LOW",
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
)
AB_FILTERED_RELAXED_SETUPS = (
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
)
AB_MAX_PNL_LOW_VALID_SETUPS = (
    "A_MOD_BREAK_C1_HIGH",
    "A_MOD_BREAK_C1_LOW",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_AVWAP_RECLAIM_REVERSAL",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
)

AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT = 10.7025
AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT = 0.8354
MAX_PNL_SBB_MIN_MARKET_RET_PCT = 0.53680868
MAX_PNL_SBB_MIN_NOTIONAL_RS = 99971.74
MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR = 0.60356498
MAX_PNL_A_MOD_CLOSE_MIN_SIGNAL_RANGE_PCT = 2.1930941
MAX_PNL_A_MOD_CLOSE_MAX_NOTIONAL_RS = 99576.0
MAX_PNL_A_MOD_C1_HIGH_MAX_MARKET_ABS_RET_PCT = 0.2565259
MAX_PNL_A_MOD_C1_HIGH_MAX_VOL_RATIO = 2.0147274
MAX_PNL_A_MOD_C1_LOW_MIN_RS_ABS_PCT = 9.2370116
MAX_PNL_A_MOD_C1_LOW_MIN_VOL_RATIO = 1.7928383
MAX_PNL_D_EMA20_REJECTION_MIN_BODY_PCT = 0.89474022
MAX_PNL_D_EMA20_REJECTION_MIN_RANKER_SCORE = 0.388264
MAX_PNL_E_VWAP_BAND_MIN_ATR_PCT = 0.0058985269
MAX_PNL_E_VWAP_BAND_MAX_SIGNAL_MINUTE = 690
MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR = -1.2528935
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_SIGNAL_MINUTE = 780.0
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_SIGNAL_MINUTE = 825.0
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_BODY_PCT = 0.92592279
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_WICK_SKEW_PCT = -0.064893645
RESIDUAL_OVERLAY_SBB_MAX_SIGNAL_MINUTE = 704.5

TIER123_BALANCED_SETUPS = (
    "T_TREND_DAY_EMA_STAIR_SHORT",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT",
)
TIER123_BALANCED_EXIT_RULES = {
    "T_TREND_DAY_EMA_STAIR_SHORT": (0.70, 1.00),
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": (0.70, 0.80),
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": (0.70, 0.80),
}
TIER123_T_STAIR_SHORT_MAX_MARKET_RET_PCT = -0.388704
TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE = 780.0
TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE = 840.0
TIER123_MR_FADE_LONG_MIN_VOL_RATIO = 2.473917
TIER123_MR_FADE_SHORT_MAX_VOL_RATIO = 1.698991
TIER123_MR_FADE_SHORT_MIN_QUALITY_SCORE = 60.674989
TIER123_MAX_RAW_PER_SETUP = 2500
TIER123_LIVE_SCAN_WORKERS = max(
    1,
    int(
        os.getenv(
            "EQIDV2_SIGNAL_DISCOVERY_V7_TIER123_SCAN_WORKERS",
            os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_WORKERS", "8"),
        )
    ),
)

CONSERVATIVE_SHORT_SHADOW_V1 = "conservative_short_shadow_v1"
# Setups permitted in the shadow profile (SHORT only, shadow output only).
CONSERVATIVE_SHORT_SHADOW_V1_SETUPS = frozenset({
    "S_BB_SQUEEZE_SHORT",
    "D_EMA20_REJECTION",
    "E_ORB_BREAKOUT_SHORT",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT",
})
# Setups explicitly excluded (would pollute the shadow if merged candidates slip through).
CONSERVATIVE_SHORT_SHADOW_V1_EXCLUDED = frozenset({
    "C_OR_BREAKOUT",
    "T_TREND_DAY_EMA_STAIR_SHORT",
})

SELECTED_STRATEGY_PROFILE_CHOICES = (
    "none",
    "production_core",
    "production_core_tiny",
    "production_core_ab_probe",
    "ab_only_probe",
    "production_core_ab_good_probe",
    "ab_good_only_probe",
    "production_core_ab_filtered_relaxed",
    "ab_filtered_relaxed_only",
    "production_core_ab_max_pnl_low_valid",
    "production_core_ab_max_pnl_low_valid_residual_overlay",
    TIER123_BALANCED_PROFILE,
    CONSERVATIVE_SHORT_SHADOW_V1,
)
AB_SELECTED_STRATEGY_PROFILES = {
    "production_core_ab_probe",
    "ab_only_probe",
    "production_core_ab_good_probe",
    "ab_good_only_probe",
    "production_core_ab_filtered_relaxed",
    "ab_filtered_relaxed_only",
    "production_core_ab_max_pnl_low_valid",
    "production_core_ab_max_pnl_low_valid_residual_overlay",
    TIER123_BALANCED_PROFILE,
}
AB_GATE_PROFILE_CHOICES = ("off", "quality_top_slot")
DEFAULT_AB_GATE_PROFILE = "quality_top_slot"
DEFAULT_AB_GATE_MIN_QUALITY = 250.0
DEFAULT_AB_GATE_MAX_PER_SIDE = 1
DEFAULT_AB_GATE_MAX_PER_SLOT = 2

SELECTED_STRATEGY_RULE_LABELS = {
    "C_OR_BREAKOUT": "production_core: broad C_OR_BREAKOUT kept after honest holdout test",
    "D_EMA20_BOUNCE": "production_core: vol_ratio <= 1.5975512 OR vwap_dist_atr >= -0.38557115, and signal_minute <= 705",
    "E_ORB_BREAKOUT_LONG": "production_core: v7_signal_notional_rs >= 99937.32",
    "E_ORB_BREAKOUT_SHORT": "production_core: market_ret_pct >= -0.63438346, quality_score >= 97.873364, upper_wick_pct <= 0.014647435; exit 0.80/1.50",
    "L_BB_SQUEEZE_LONG": "production_core: market_abs_ret_pct <= 0.74284715 OR vol_ratio <= 3.0227043, and ranker_score >= 0.7332456",
    "E_VWAP_LOSE_EARLY_SHORT": (
        "production_core_ab_max_pnl_low_valid: vwap_dist_atr >= "
        f"{MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR:.7f}"
    ),
    "S_BB_SQUEEZE_SHORT": (
        "max_pnl_low_valid/residual_overlay: market_ret/notional gate, plus residual morning S_BB"
    ),
    "B_AVWAP_RECLAIM_REVERSAL": "max_pnl_low_valid: vwap_dist_atr gate",
    "A_MOD_CLOSE_CONTINUATION_BREAK": "max_pnl_low_valid: signal_range/notional gate",
    "A_MOD_BREAK_C1_LOW": "max_pnl_low_valid: abs(rs_pct) and vol_ratio gate",
    "A_MOD_BREAK_C1_HIGH": "max_pnl_low_valid: market_abs_ret and vol_ratio gate",
    "D_EMA20_REJECTION": "max_pnl_low_valid/residual_overlay: body/ranker or late residual gate",
    "E_VWAP_BAND_FADE": "max_pnl_low_valid: atr_pct and signal_minute gate",
    "T_TREND_DAY_EMA_STAIR_SHORT": "tier123_balanced: late bearish trend-day EMA stair short",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": "tier123_balanced: controlled VWAP extreme fade long",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": "tier123_balanced: controlled VWAP extreme fade short",
}
for _setup in AB_PROBATION_SETUPS:
    SELECTED_STRATEGY_RULE_LABELS.setdefault(
        _setup,
        "ab/max-pnl probation setup admitted by v11 selected profile and A/B top-slot gate",
    )

SELECTED_STRATEGY_EXIT_OVERRIDES = {
    "production_core": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_tiny": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_probe": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_good_probe": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_filtered_relaxed": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_max_pnl_low_valid": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_max_pnl_low_valid_residual_overlay": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    TIER123_BALANCED_PROFILE: {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
}

V11_PROFILE_SETUP_UNIVERSE = {
    "C_OR_BREAKOUT",
    "D_EMA20_BOUNCE",
    "E_ORB_BREAKOUT_LONG",
    "E_ORB_BREAKOUT_SHORT",
    "L_BB_SQUEEZE_LONG",
    "E_VWAP_LOSE_EARLY_SHORT",
    "S_BB_SQUEEZE_SHORT",
    "B_AVWAP_RECLAIM_REVERSAL",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "A_MOD_BREAK_C1_LOW",
    "A_MOD_BREAK_C1_HIGH",
    "D_EMA20_REJECTION",
    "E_VWAP_BAND_FADE",
    *AB_FILTERED_RELAXED_SETUPS,
    *TIER123_BALANCED_SETUPS,
}


def _normalise_selected_strategy_profile(profile: str | None) -> str:
    out = str(profile or DEFAULT_SELECTED_STRATEGY_PROFILE).strip().lower()
    if out not in SELECTED_STRATEGY_PROFILE_CHOICES:
        raise ValueError(
            f"unknown v11 selected strategy profile {profile!r}; "
            f"choose one of {', '.join(SELECTED_STRATEGY_PROFILE_CHOICES)}"
        )
    return out


def _normalise_ab_gate_profile(profile: str | None) -> str:
    out = str(profile or DEFAULT_AB_GATE_PROFILE).strip().lower()
    if out not in AB_GATE_PROFILE_CHOICES:
        raise ValueError(f"unknown v11 A/B gate profile {profile!r}")
    return out


def _normalise_ts(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize(IST_TZ)
    return ts.tz_convert(IST_TZ)


def _fmt_ist(value: Any) -> str:
    ts = _normalise_ts(value)
    if pd.isna(ts):
        return ""
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _selected_strategy_numeric(frame: pd.DataFrame, col: str) -> pd.Series:
    if col in frame.columns:
        return pd.to_numeric(frame[col], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _estimate_v7_notional_from_signal_close(close: pd.Series) -> pd.Series:
    close_num = pd.to_numeric(close, errors="coerce")
    qty = np.floor(V7_SIGNAL_NOTIONAL_RS / close_num.replace(0, np.nan))
    return close_num * qty


def selected_strategy_features(
    signals: pd.DataFrame,
    *,
    estimate_missing_notional: bool = True,
) -> pd.DataFrame:
    work = signals.copy()
    for col in [
        "vol_ratio",
        "vwap_dist_atr",
        "v7_signal_notional_rs",
        "market_ret_pct",
        "quality_score",
        "ranker_score",
        "body_pct",
        "atr_pct",
        "signal_open",
        "signal_high",
        "signal_low",
        "signal_close",
    ]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    if "ranker_score" not in work.columns and "live_rank_score" in work.columns:
        work["ranker_score"] = pd.to_numeric(work["live_rank_score"], errors="coerce")

    if "v7_signal_notional_rs" not in work.columns:
        work["v7_signal_notional_rs"] = np.nan
    if estimate_missing_notional:
        close_for_notional = _selected_strategy_numeric(work, "signal_close")
        estimated = _estimate_v7_notional_from_signal_close(close_for_notional)
        work["v7_signal_notional_rs"] = pd.to_numeric(work["v7_signal_notional_rs"], errors="coerce").fillna(estimated)

    signal_minutes: list[float] = []
    for _, row in work.iterrows():
        ts = pd.NaT
        for col in ("signal_time_ist", "signal_datetime", "signal_ts", "bar_time_ist", "scan_slot_ist"):
            if col in row.index and pd.notna(row.get(col)):
                ts = _normalise_ts(row.get(col))
                if pd.notna(ts):
                    break
        signal_minutes.append(float(ts.hour * 60 + ts.minute) if pd.notna(ts) else np.nan)
    work["signal_minute"] = signal_minutes

    open_px = _selected_strategy_numeric(work, "signal_open")
    high_px = _selected_strategy_numeric(work, "signal_high")
    low_px = _selected_strategy_numeric(work, "signal_low")
    close_px = _selected_strategy_numeric(work, "signal_close")
    close_safe = close_px.replace(0, np.nan)
    body_top = pd.concat([open_px, close_px], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close_px], axis=1).min(axis=1)
    work["upper_wick_pct"] = (high_px - body_top) / close_safe * 100.0
    work["lower_wick_pct"] = (body_bottom - low_px) / close_safe * 100.0
    work["wick_skew_pct"] = work["upper_wick_pct"] - work["lower_wick_pct"]
    work["signal_range_pct"] = (high_px - low_px) / close_safe * 100.0
    work["market_abs_ret_pct"] = _selected_strategy_numeric(work, "market_ret_pct").abs()
    return work


def selected_strategy_mask(
    signals: pd.DataFrame,
    profile: str | None = None,
    *,
    estimate_missing_notional: bool = True,
) -> pd.Series:
    profile_n = _normalise_selected_strategy_profile(profile)
    if signals is None or signals.empty:
        return pd.Series(False, index=pd.RangeIndex(0))
    if profile_n == "none":
        return pd.Series(True, index=signals.index)

    work = selected_strategy_features(signals, estimate_missing_notional=estimate_missing_notional)
    setup = work.get("setup", pd.Series("", index=work.index)).astype(str)
    ab_mask = setup.isin(AB_PROBATION_SETUPS)
    ab_good_mask = setup.isin(AB_GOOD_RESULT_SETUPS)
    rs_pct = _selected_strategy_numeric(work, "rs_pct")
    market_abs = _selected_strategy_numeric(work, "market_abs_ret_pct")
    ab_filtered_relaxed_mask = (
        setup.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK")
        & (rs_pct <= AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT)
    ) | (
        setup.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW")
        & (market_abs <= AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT)
    )
    if profile_n == "ab_only_probe":
        return ab_mask.fillna(False)
    if profile_n == "ab_good_only_probe":
        return ab_good_mask.fillna(False)
    if profile_n == "ab_filtered_relaxed_only":
        return ab_filtered_relaxed_mask.fillna(False)

    vol_ratio = _selected_strategy_numeric(work, "vol_ratio")
    vwap_dist_atr = _selected_strategy_numeric(work, "vwap_dist_atr")
    notional = _selected_strategy_numeric(work, "v7_signal_notional_rs")
    market_ret = _selected_strategy_numeric(work, "market_ret_pct")
    quality_score = _selected_strategy_numeric(work, "quality_score")
    ranker_score = _selected_strategy_numeric(work, "ranker_score")
    body_pct = _selected_strategy_numeric(work, "body_pct")
    atr_pct = _selected_strategy_numeric(work, "atr_pct")
    signal_minute = _selected_strategy_numeric(work, "signal_minute")
    upper_wick_pct = _selected_strategy_numeric(work, "upper_wick_pct")
    wick_skew_pct = _selected_strategy_numeric(work, "wick_skew_pct")
    signal_range_pct = _selected_strategy_numeric(work, "signal_range_pct")
    regime = work.get("regime", pd.Series("", index=work.index)).astype(str).str.upper()

    max_pnl_low_valid_mask = ab_filtered_relaxed_mask.copy()
    max_pnl_low_valid_mask |= setup.eq("S_BB_SQUEEZE_SHORT") & (
        (market_ret >= MAX_PNL_SBB_MIN_MARKET_RET_PCT)
        | (notional >= MAX_PNL_SBB_MIN_NOTIONAL_RS)
    )
    max_pnl_low_valid_mask |= setup.eq("B_AVWAP_RECLAIM_REVERSAL") & (
        vwap_dist_atr >= MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR
    )
    max_pnl_low_valid_mask |= setup.eq("A_MOD_CLOSE_CONTINUATION_BREAK") & (
        (signal_range_pct >= MAX_PNL_A_MOD_CLOSE_MIN_SIGNAL_RANGE_PCT)
        | (notional <= MAX_PNL_A_MOD_CLOSE_MAX_NOTIONAL_RS)
    )
    max_pnl_low_valid_mask |= setup.eq("A_MOD_BREAK_C1_LOW") & (
        (rs_pct.abs() >= MAX_PNL_A_MOD_C1_LOW_MIN_RS_ABS_PCT)
        & (vol_ratio >= MAX_PNL_A_MOD_C1_LOW_MIN_VOL_RATIO)
    )
    max_pnl_low_valid_mask |= setup.eq("A_MOD_BREAK_C1_HIGH") & (
        (market_abs <= MAX_PNL_A_MOD_C1_HIGH_MAX_MARKET_ABS_RET_PCT)
        & (vol_ratio <= MAX_PNL_A_MOD_C1_HIGH_MAX_VOL_RATIO)
    )
    max_pnl_low_valid_mask |= setup.eq("D_EMA20_REJECTION") & (
        (body_pct >= MAX_PNL_D_EMA20_REJECTION_MIN_BODY_PCT)
        & (ranker_score >= MAX_PNL_D_EMA20_REJECTION_MIN_RANKER_SCORE)
    )
    max_pnl_low_valid_mask |= setup.eq("E_VWAP_BAND_FADE") & (
        (atr_pct >= MAX_PNL_E_VWAP_BAND_MIN_ATR_PCT)
        & (signal_minute <= MAX_PNL_E_VWAP_BAND_MAX_SIGNAL_MINUTE)
    )
    max_pnl_low_valid_mask |= setup.eq("E_VWAP_LOSE_EARLY_SHORT") & (
        vwap_dist_atr >= MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR
    )
    residual_late_d_mask = setup.eq("D_EMA20_REJECTION") & (
        (signal_minute > RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_SIGNAL_MINUTE)
        & (signal_minute <= RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_SIGNAL_MINUTE)
        & (
            (body_pct >= RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_BODY_PCT)
            | (wick_skew_pct <= RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_WICK_SKEW_PCT)
        )
    )
    residual_morning_sbb_mask = setup.eq("S_BB_SQUEEZE_SHORT") & (
        signal_minute <= RESIDUAL_OVERLAY_SBB_MAX_SIGNAL_MINUTE
    )
    max_pnl_residual_overlay_mask = (
        max_pnl_low_valid_mask
        | residual_late_d_mask
        | residual_morning_sbb_mask
    )
    tier123_balanced_mask = (
        setup.eq("T_TREND_DAY_EMA_STAIR_SHORT")
        & (market_ret <= TIER123_T_STAIR_SHORT_MAX_MARKET_RET_PCT)
        & (signal_minute >= TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE)
        & (signal_minute <= TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE)
    )
    tier123_balanced_mask |= setup.eq("MR_CONTROLLED_VWAP_EXTREME_FADE_LONG") & (
        vol_ratio >= TIER123_MR_FADE_LONG_MIN_VOL_RATIO
    )
    tier123_balanced_mask |= setup.eq("MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT") & (
        (vol_ratio <= TIER123_MR_FADE_SHORT_MAX_VOL_RATIO)
        & (quality_score >= TIER123_MR_FADE_SHORT_MIN_QUALITY_SCORE)
    )

    mask = setup.eq("C_OR_BREAKOUT")
    mask |= setup.eq("D_EMA20_BOUNCE") & (
        ((vol_ratio <= 1.5975512) | (vwap_dist_atr >= -0.38557115))
        & (signal_minute <= 705)
    )
    mask |= setup.eq("E_ORB_BREAKOUT_LONG") & (notional >= 99937.32)
    mask |= setup.eq("E_ORB_BREAKOUT_SHORT") & (
        (market_ret >= -0.63438346)
        & (quality_score >= 97.873364)
        & (upper_wick_pct <= 0.014647435)
    )
    mask |= setup.eq("L_BB_SQUEEZE_LONG") & (
        ((market_abs <= 0.74284715) | (vol_ratio <= 3.0227043))
        & (ranker_score >= 0.7332456)
    )
    if profile_n == "production_core_tiny":
        mask |= setup.eq("E_VWAP_LOSE_EARLY_SHORT") & regime.eq("BEAR")
    if profile_n == "production_core_ab_probe":
        mask |= ab_mask
    if profile_n == "production_core_ab_good_probe":
        mask |= ab_good_mask
    if profile_n == "production_core_ab_filtered_relaxed":
        mask |= ab_filtered_relaxed_mask
    if profile_n == "production_core_ab_max_pnl_low_valid":
        mask |= max_pnl_low_valid_mask
    if profile_n == "production_core_ab_max_pnl_low_valid_residual_overlay":
        mask |= max_pnl_residual_overlay_mask
    if profile_n == TIER123_BALANCED_PROFILE:
        mask |= max_pnl_residual_overlay_mask
        mask |= tier123_balanced_mask

    if profile_n == CONSERVATIVE_SHORT_SHADOW_V1:
        # Shadow-only short profile.  Uses existing per-setup thresholds from
        # the current production profile; never merges into the entry-engine
        # input path.  Promotion requires 30 independent live days on a fresh holdout.
        shadow_mask = setup.isin(CONSERVATIVE_SHORT_SHADOW_V1_SETUPS)
        shadow_mask &= ~setup.isin(CONSERVATIVE_SHORT_SHADOW_V1_EXCLUDED)
        # Reuse production thresholds for the setups that already have them.
        shadow_sbb = setup.eq("S_BB_SQUEEZE_SHORT") & (
            (market_ret >= MAX_PNL_SBB_MIN_MARKET_RET_PCT)
            | (notional >= MAX_PNL_SBB_MIN_NOTIONAL_RS)
        )
        shadow_d_rej = setup.eq("D_EMA20_REJECTION") & (
            (body_pct >= MAX_PNL_D_EMA20_REJECTION_MIN_BODY_PCT)
            & (ranker_score >= MAX_PNL_D_EMA20_REJECTION_MIN_RANKER_SCORE)
        )
        shadow_orb = setup.eq("E_ORB_BREAKOUT_SHORT") & (
            (market_ret >= -0.63438346)
            & (quality_score >= 97.873364)
            & (upper_wick_pct <= 0.014647435)
        )
        shadow_mr = setup.eq("MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT") & (
            (vol_ratio <= TIER123_MR_FADE_SHORT_MAX_VOL_RATIO)
            & (quality_score >= TIER123_MR_FADE_SHORT_MIN_QUALITY_SCORE)
        )
        return (shadow_mask & (shadow_sbb | shadow_d_rej | shadow_orb | shadow_mr)).fillna(False)

    return mask.fillna(False)


def apply_selected_strategy_profile(
    signals: pd.DataFrame,
    profile: str | None = None,
    *,
    estimate_missing_notional: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    profile_n = _normalise_selected_strategy_profile(profile)
    if signals is None:
        signals = pd.DataFrame()
    if signals.empty:
        stats = {
            "selected_strategy_profile": profile_n,
            "selected_strategy_input_signals": 0,
            "selected_strategy_signals": 0,
            "selected_strategy_rejects": 0,
        }
        return signals.copy(), pd.DataFrame(), stats

    work = selected_strategy_features(signals, estimate_missing_notional=estimate_missing_notional)
    mask = selected_strategy_mask(work, profile_n, estimate_missing_notional=estimate_missing_notional)
    accepted = work.loc[mask].copy().reset_index(drop=True)
    rejected = work.loc[~mask].copy().reset_index(drop=True)
    if not accepted.empty:
        accepted["v11_selected_strategy_profile"] = profile_n
        accepted["v11_selected_strategy_rule"] = accepted["setup"].astype(str).map(SELECTED_STRATEGY_RULE_LABELS).fillna("")
        accepted["v11_live_overlay_version"] = V11_LIVE_OVERLAY_VERSION
    if not rejected.empty:
        rejected["v11_selected_strategy_profile"] = profile_n
        rejected["v11_selected_strategy_reject_reason"] = "not_in_selected_strategy_profile_or_rule_failed"
        rejected["v11_live_overlay_version"] = V11_LIVE_OVERLAY_VERSION

    stats = {
        "selected_strategy_profile": profile_n,
        "selected_strategy_input_signals": int(len(work)),
        "selected_strategy_signals": int(len(accepted)),
        "selected_strategy_rejects": int(len(rejected)),
    }
    return accepted, rejected, stats


def v11_override_setup_universe(profile: str | None = None) -> set[str]:
    profile_n = _normalise_selected_strategy_profile(profile)
    if profile_n == "none":
        return set()
    return set(V11_PROFILE_SETUP_UNIVERSE)


def ab_gate_setups_for_profile(profile: str | None = None) -> tuple[str, ...]:
    profile_n = _normalise_selected_strategy_profile(profile)
    if profile_n in {
        "production_core_ab_max_pnl_low_valid",
        "production_core_ab_max_pnl_low_valid_residual_overlay",
        TIER123_BALANCED_PROFILE,
    }:
        return AB_MAX_PNL_LOW_VALID_SETUPS
    if profile_n in {"production_core_ab_filtered_relaxed", "ab_filtered_relaxed_only"}:
        return AB_FILTERED_RELAXED_SETUPS
    if profile_n in {"production_core_ab_good_probe", "ab_good_only_probe"}:
        return AB_GOOD_RESULT_SETUPS
    return AB_PROBATION_SETUPS


def apply_ab_probation_gate(
    ranked_slot: pd.DataFrame,
    *,
    profile: str | None = DEFAULT_AB_GATE_PROFILE,
    selected_strategy_profile: str | None = DEFAULT_SELECTED_STRATEGY_PROFILE,
    min_quality: float = DEFAULT_AB_GATE_MIN_QUALITY,
    max_per_side: int = DEFAULT_AB_GATE_MAX_PER_SIDE,
    max_per_slot: int = DEFAULT_AB_GATE_MAX_PER_SLOT,
) -> tuple[pd.DataFrame, Dict[str, Any]]:
    profile_n = _normalise_ab_gate_profile(profile)
    allowed = ab_gate_setups_for_profile(selected_strategy_profile)
    stats = {
        "ab_gate_profile": profile_n,
        "ab_gate_enabled": bool(profile_n != "off"),
        "ab_gate_allowed_setups": int(len(allowed)),
        "ab_gate_min_quality": float(min_quality),
        "ab_gate_max_per_side": int(max(1, max_per_side)),
        "ab_gate_max_per_slot": int(max(1, max_per_slot)),
        "ab_gate_candidates": 0,
        "ab_gate_quality_passed": 0,
        "ab_gate_quality_rejected": 0,
        "ab_gate_accepted": 0,
    }
    if profile_n == "off" or ranked_slot is None or ranked_slot.empty:
        return pd.DataFrame() if ranked_slot is None else ranked_slot.iloc[0:0].copy(), stats

    work = ranked_slot.copy()
    setup = work.get("setup", pd.Series("", index=work.index)).astype(str)
    ab = work.loc[setup.isin(allowed)].copy()
    stats["ab_gate_candidates"] = int(len(ab))
    if ab.empty:
        return ab, stats

    ab["_ab_quality_num"] = pd.to_numeric(ab.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    ab["_ab_ranker_num"] = pd.to_numeric(ab.get("ranker_score", 0.0), errors="coerce").fillna(0.0)
    ab["side"] = ab.get("side", pd.Series("", index=ab.index)).astype(str).str.upper().str.strip()
    ab = ab.loc[ab["_ab_quality_num"] >= float(min_quality)].copy()
    stats["ab_gate_quality_passed"] = int(len(ab))
    stats["ab_gate_quality_rejected"] = int(stats["ab_gate_candidates"] - len(ab))
    if ab.empty:
        return ab.drop(columns=["_ab_quality_num", "_ab_ranker_num"], errors="ignore"), stats

    side_cap = int(max(1, max_per_side))
    slot_cap = int(max(1, max_per_slot))
    selected = (
        ab.sort_values(
            ["side", "_ab_quality_num", "_ab_ranker_num", "ticker", "setup"],
            ascending=[True, False, False, True, True],
        )
        .groupby("side", group_keys=False)
        .head(side_cap)
        .sort_values(["_ab_quality_num", "_ab_ranker_num", "ticker", "setup"], ascending=[False, False, True, True])
        .head(slot_cap)
        .drop(columns=["_ab_quality_num", "_ab_ranker_num"], errors="ignore")
        .reset_index(drop=True)
    )
    stats["ab_gate_accepted"] = int(len(selected))
    if not selected.empty:
        rule = (
            f"ab_quality_top_slot|min_quality>={float(min_quality):.6g}|"
            f"max_per_side={side_cap}|max_per_slot={slot_cap}"
        )
        selected["v8_live_gate_status"] = "AB_PROBATION_PASSED"
        selected["v8_live_gate_rule"] = rule
        selected["v8_live_gate_version"] = V11_LIVE_OVERLAY_VERSION
        selected["research_live_filter_status"] = "AB_PROBATION_BYPASS"
        selected["research_live_filter_reason"] = "explicit_v11_ab_gate_profile"
        selected["research_live_filter_version"] = V11_LIVE_OVERLAY_VERSION
        selected["ab_gate_profile"] = profile_n
        selected["ab_gate_rule"] = rule
    return selected, stats


def _ensure_candidate_id(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    if "candidate_id" not in out.columns:
        out["candidate_id"] = ""
    missing = out["candidate_id"].astype(str).str.strip().eq("")
    if missing.any():
        out.loc[missing, "candidate_id"] = (
            out.loc[missing, "ticker"].astype(str).str.upper().str.strip()
            + "|"
            + out.loc[missing, "side"].astype(str).str.upper().str.strip()
            + "|"
            + out.loc[missing, "setup"].astype(str)
            + "|"
            + out.loc[missing, "signal_time_ist"].astype(str)
        )
    return out


def apply_live_candidate_overlay(
    raw_candidates: pd.DataFrame,
    *,
    profile: str | None = DEFAULT_SELECTED_STRATEGY_PROFILE,
    ab_gate_profile: str | None = DEFAULT_AB_GATE_PROFILE,
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    profile_n = _normalise_selected_strategy_profile(profile)
    universe = v11_override_setup_universe(profile_n)
    stats: Dict[str, Any] = {
        "v11_live_overlay_enabled": True,
        "v11_live_overlay_version": V11_LIVE_OVERLAY_VERSION,
        "v11_live_overlay_profile": profile_n,
        "v11_live_overlay_input_candidates": int(0 if raw_candidates is None else len(raw_candidates)),
        "v11_live_overlay_candidate_universe": 0,
        "v11_live_overlay_selected": 0,
        "v11_live_overlay_rejected": 0,
    }
    if raw_candidates is None or raw_candidates.empty or not universe:
        return pd.DataFrame(), pd.DataFrame(), stats

    work = _ensure_candidate_id(raw_candidates)
    work["setup"] = work["setup"].astype(str)
    work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    work["side"] = work["side"].astype(str).str.upper().str.strip()
    in_universe = work["setup"].isin(universe)
    v11_work = work.loc[in_universe].copy()
    stats["v11_live_overlay_candidate_universe"] = int(len(v11_work))
    if v11_work.empty:
        return pd.DataFrame(), pd.DataFrame(), stats

    ab_allowed = set(ab_gate_setups_for_profile(profile_n))
    ab_mask = v11_work["setup"].isin(ab_allowed)
    ab_selected, ab_stats = apply_ab_probation_gate(
        v11_work,
        profile=ab_gate_profile,
        selected_strategy_profile=profile_n,
    )
    non_ab = v11_work.loc[~ab_mask].copy()
    pool = pd.concat([non_ab, ab_selected], ignore_index=True, sort=False)
    pool = _ensure_candidate_id(pool).drop_duplicates(subset=["candidate_id"], keep="first").reset_index(drop=True)

    accepted, profile_rejected, profile_stats = apply_selected_strategy_profile(
        pool,
        profile_n,
        estimate_missing_notional=True,
    )
    rejected_frames = []
    if ab_mask.any():
        ab_selected_ids = set(ab_selected.get("candidate_id", pd.Series(dtype=str)).astype(str))
        ab_rejected = v11_work.loc[ab_mask & ~v11_work["candidate_id"].astype(str).isin(ab_selected_ids)].copy()
        if not ab_rejected.empty:
            ab_rejected["v11_selected_strategy_profile"] = profile_n
            ab_rejected["v11_selected_strategy_reject_reason"] = "ab_quality_top_slot_gate_failed"
            rejected_frames.append(ab_rejected)
    if not profile_rejected.empty:
        rejected_frames.append(profile_rejected)
    rejected = pd.concat(rejected_frames, ignore_index=True, sort=False) if rejected_frames else v11_work.iloc[0:0].copy()

    if not accepted.empty:
        accepted["v11_live_overlay_status"] = "PASSED"
        accepted["v11_live_overlay_source"] = "v11_backtesting_selected_strategy"
        accepted["selection_mode"] = "v11_backtesting_profile_overlay"
        accepted["candidate_family"] = "V11_BACKTESTING_OVERLAY"
        accepted["status"] = "V11_BACKTESTING_PASSED"
    if not rejected.empty:
        rejected["v11_live_overlay_status"] = "REJECTED"
        rejected["v11_live_overlay_source"] = "v11_backtesting_selected_strategy"
        rejected["status"] = "V11_BACKTESTING_REJECTED"

    stats.update(ab_stats)
    stats.update(
        {
            "v11_live_overlay_selected": int(len(accepted)),
            "v11_live_overlay_rejected": int(len(rejected)),
            **{f"v11_live_overlay_{k}": v for k, v in profile_stats.items()},
        }
    )
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True), stats


def merge_v7_and_v11_candidates(
    v7_candidates: pd.DataFrame,
    v11_candidates: pd.DataFrame,
    *,
    profile: str | None = DEFAULT_SELECTED_STRATEGY_PROFILE,
) -> pd.DataFrame:
    if v7_candidates is None:
        v7_candidates = pd.DataFrame()

    universe = v11_override_setup_universe(profile)
    v7 = v7_candidates.copy()
    if not v7.empty and "setup" in v7.columns:
        v7 = v7.loc[~v7["setup"].astype(str).isin(universe)].copy()
    if v11_candidates is None or v11_candidates.empty:
        return v7.reset_index(drop=True)
    v11 = v11_candidates.copy()

    combined = pd.concat([v11, v7], ignore_index=True, sort=False)
    if combined.empty:
        return combined
    combined = _ensure_candidate_id(combined)
    combined["ticker"] = combined["ticker"].astype(str).str.upper().str.strip()
    combined["signal_time_ist"] = combined["signal_time_ist"].astype(str)
    status_col = combined["v11_live_overlay_status"] if "v11_live_overlay_status" in combined.columns else pd.Series("", index=combined.index)
    combined["_v11_priority"] = status_col.astype(str).eq("PASSED").astype(int)
    combined["_score_num"] = pd.to_numeric(combined.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    combined = (
        combined.sort_values(["_v11_priority", "_score_num", "ticker", "setup"], ascending=[False, False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .drop(columns=["_v11_priority", "_score_num"], errors="ignore")
        .reset_index(drop=True)
    )
    return combined


def selected_exit_override(setup: str, profile: str | None = DEFAULT_SELECTED_STRATEGY_PROFILE) -> Optional[Tuple[float, float]]:
    setup_s = str(setup)
    if setup_s in TIER123_BALANCED_EXIT_RULES:
        return TIER123_BALANCED_EXIT_RULES[setup_s]
    profile_n = _normalise_selected_strategy_profile(profile)
    override = SELECTED_STRATEGY_EXIT_OVERRIDES.get(profile_n, {}).get(setup_s)
    return tuple(override) if override is not None else None


def exit_rule_for_setup(
    setup: str,
    base_rules: Dict[str, Tuple[float, float]],
    profile: str | None = DEFAULT_SELECTED_STRATEGY_PROFILE,
) -> Optional[Tuple[float, float]]:
    override = selected_exit_override(setup, profile)
    if override is not None:
        return override
    rule = base_rules.get(str(setup))
    return tuple(rule) if rule is not None else None


def live_exit_rules(
    base_rules: Dict[str, Tuple[float, float]],
    profile: str | None = DEFAULT_SELECTED_STRATEGY_PROFILE,
) -> Dict[str, Tuple[float, float]]:
    rules = dict(base_rules)
    for setup, rule in TIER123_BALANCED_EXIT_RULES.items():
        rules[setup] = rule
    profile_n = _normalise_selected_strategy_profile(profile)
    for setup, rule in SELECTED_STRATEGY_EXIT_OVERRIDES.get(profile_n, {}).items():
        rules[setup] = rule
    return rules


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _tier123_prepare_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if getattr(out["date"].dt, "tz", None) is None:
        out["date"] = out["date"].dt.tz_localize(IST_TZ)
    else:
        out["date"] = out["date"].dt.tz_convert(IST_TZ)
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    out["date_only"] = out["date"].dt.strftime("%Y-%m-%d")
    if "ATR" not in out.columns or out["ATR"].isna().all() or "VWAP" not in out.columns or out["VWAP"].isna().all():
        out = candidate_scan.v2._prepare_5m(out)
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        if getattr(out["date"].dt, "tz", None) is None:
            out["date"] = out["date"].dt.tz_localize(IST_TZ)
        else:
            out["date"] = out["date"].dt.tz_convert(IST_TZ)
        out["date_only"] = out["date"].dt.strftime("%Y-%m-%d")
        return out

    out["Volume_SMA20"] = out.groupby("date_only")["volume"].transform(
        lambda s: s.shift(1).rolling(candidate_scan.v2.VWAP_LOOKBACK, min_periods=8).mean()
    )
    out["traded_value_rs"] = out["close"] * out["volume"]
    out["day_value_so_far_rs"] = out.groupby("date_only")["traded_value_rs"].cumsum()
    out["range"] = out["high"] - out["low"]
    range_nonzero = out["range"].replace(0, np.nan)
    out["body_pct"] = (out["close"] - out["open"]).abs() / range_nonzero
    out["close_loc"] = (out["close"] - out["low"]) / range_nonzero
    out["upper_wick_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / range_nonzero
    out["lower_wick_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / range_nonzero
    out["vol_ratio"] = out["volume"] / out["Volume_SMA20"].replace(0, np.nan)
    out["atr_pct"] = out["ATR"] / out["close"].replace(0, np.nan)
    out["vwap_dist_atr"] = (out["close"] - out["VWAP"]) / out["ATR"].replace(0, np.nan)
    return out


def _tier123_bar_context(market_ctx: Dict[str, Dict[str, Any]], day: str, ts: pd.Timestamp) -> tuple[float, str]:
    try:
        ret, regime = candidate_scan.v2._bar_context(market_ctx, str(day), ts)
        return float(ret), str(regime)
    except Exception:
        return 0.0, "UNKNOWN"


def _tier123_quality(side: str, setup: str, row: pd.Series, rs_pct: float, market_ret: float, regime: str) -> float:
    close_loc = _safe_float(row.get("close_loc"), 0.5)
    vol_ratio = _safe_float(row.get("vol_ratio"), 1.0)
    vwap_dist = _safe_float(row.get("vwap_dist_atr"), 0.0)
    body_pct = _safe_float(row.get("body_pct"), 0.0)
    adx = _safe_float(row.get("ADX"), 0.0)
    score = 20.0
    if side == "LONG":
        score += 18.0 * max(rs_pct, 0.0)
        score += 15.0 * max(close_loc - 0.45, 0.0)
        score += 5.0 if market_ret >= -0.20 else -8.0
        score += 5.0 if regime != "BEAR" else -10.0
    else:
        score += 18.0 * max(-rs_pct, 0.0)
        score += 15.0 * max(0.55 - close_loc, 0.0)
        score += 5.0 if market_ret <= 0.20 else -8.0
        score += 5.0 if regime != "BULL" else -10.0
    score += 8.0 * min(max(vol_ratio - 1.0, 0.0), 3.0)
    score += 8.0 * body_pct
    score += 0.35 * max(adx - 15.0, 0.0)
    if "FADE" in setup:
        score += 4.0 * min(abs(vwap_dist), 3.5)
    elif side == "LONG":
        score += 5.0 if vwap_dist >= -0.25 else -5.0
    else:
        score += 5.0 if vwap_dist <= 0.25 else -5.0
    return float(score)


def _tier123_candidate(
    ticker: str,
    setup: str,
    side: str,
    row: pd.Series,
    rs_pct: float,
    market_ret: float,
    regime: str,
    reason: str,
) -> dict:
    ts = _normalise_ts(row["date"])
    score = _tier123_quality(side, setup, row, rs_pct, market_ret, regime)
    return {
        "candidate_id": f"{ticker}|{side}|{setup}|{_fmt_ist(ts)}",
        "scan_session": "v11_tier123_balanced_addon_live",
        "selection_mode": "tier123_balanced_addon_gate",
        "candidate_family": "V11_TIER123_BALANCED_ADDON",
        "scan_slot_ist": _fmt_ist(ts),
        "signal_time_ist": _fmt_ist(ts),
        "bar_time_ist": _fmt_ist(ts),
        "ticker": ticker,
        "side": side,
        "setup": setup,
        "signal_open": float(row["open"]),
        "signal_high": float(row["high"]),
        "signal_low": float(row["low"]),
        "signal_close": float(row["close"]),
        "signal_volume": float(row.get("volume", 0.0)),
        "quality_score": score,
        "ranker_score": score,
        "score": score,
        "rs_pct": float(rs_pct),
        "market_ret_pct": float(market_ret),
        "regime": regime,
        "vol_ratio": float(row.get("vol_ratio", np.nan)),
        "atr_pct": float(row.get("atr_pct", np.nan)),
        "body_pct": float(row.get("body_pct", np.nan)),
        "close_loc": float(row.get("close_loc", np.nan)),
        "vwap_dist_atr": float(row.get("vwap_dist_atr", np.nan)),
        "reason": reason,
        "status": "TIER123_BALANCED_RAW",
        "created_at_ist": _fmt_ist(pd.Timestamp.now(tz=IST_TZ)),
    }


def _scan_tier123_ticker_slot(
    ticker: str,
    slot: pd.Timestamp,
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
) -> list[dict]:
    df = candidate_scan._load_live_5m(ticker)
    if df is None or df.empty or "date" not in df.columns:
        return []
    slot_ts = _normalise_ts(slot).floor("min")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize(IST_TZ)
    else:
        df["date"] = df["date"].dt.tz_convert(IST_TZ)
    day_start = slot_ts.normalize()
    df = df.loc[(df["date"] >= day_start) & (df["date"] <= slot_ts)].copy()
    if df.empty:
        return []
    prepared = _tier123_prepare_5m(df)
    day_df = prepared
    if len(day_df) < 15:
        return []
    day_df = day_df.reset_index(drop=True)
    idxs = np.where(day_df["date"].dt.floor("min").eq(slot_ts))[0]
    if len(idxs) == 0:
        return []
    i = int(idxs[-1])
    if i < 4:
        return []
    row = day_df.iloc[i]
    prev = day_df.iloc[i - 1]
    ts = _normalise_ts(row["date"])
    minute = ts.hour * 60 + ts.minute
    if minute < 570 or minute > 900:
        return []

    close = _safe_float(row.get("close"))
    open_px = _safe_float(row.get("open"))
    high = _safe_float(row.get("high"))
    atr = _safe_float(row.get("ATR"))
    vwap = _safe_float(row.get("VWAP"))
    ema20 = _safe_float(row.get("EMA_20"))
    ema50 = _safe_float(row.get("EMA_50"))
    adx = _safe_float(row.get("ADX"))
    rsi = _safe_float(row.get("RSI"))
    stoch = _safe_float(row.get("Stoch_%K"))
    vol_ratio = _safe_float(row.get("vol_ratio"))
    close_loc = _safe_float(row.get("close_loc"))
    lower_wick = _safe_float(row.get("lower_wick_pct"))
    upper_wick = _safe_float(row.get("upper_wick_pct"))
    vwap_dist = _safe_float(row.get("vwap_dist_atr"))
    rng = _safe_float(row.get("range"))
    if not np.isfinite(atr) or atr <= 0 or not np.isfinite(vol_ratio) or vol_ratio < 1.0:
        return []
    if close < 25 or _safe_float(row.get("day_value_so_far_rs"), 0.0) < 20_000_000:
        return []
    if np.isfinite(rng) and rng > 3.5 * atr:
        return []

    day_open = _safe_float(day_df.iloc[0].get("open"))
    market_ret, regime = _tier123_bar_context(market_ctx or {}, slot_ts.strftime("%Y-%m-%d"), ts)
    stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
    rs_pct = stock_ret - market_ret
    below_vwap = np.isfinite(vwap) and close < vwap
    short_struct = close < open_px and close_loc <= 0.45
    controlled = regime in {"NEUTRAL", "UNKNOWN"} or (np.isfinite(adx) and adx <= 22)
    rows: list[dict] = []

    ema20_slope = _safe_float(day_df["EMA_20"].iloc[i] - day_df["EMA_20"].iloc[max(0, i - 3)]) if np.isfinite(ema20) else np.nan
    if (
        np.isfinite(ema20)
        and np.isfinite(ema50)
        and np.isfinite(ema20_slope)
        and minute >= TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE
        and minute <= TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE
        and regime in {"BEAR", "TREND"}
        and ema20_slope < 0
        and close < ema20 <= ema50
        and high >= ema20 - 0.25 * atr
        and short_struct
        and below_vwap
        and rs_pct <= -0.05
    ):
        rows.append(_tier123_candidate(
            str(ticker).upper(),
            "T_TREND_DAY_EMA_STAIR_SHORT",
            "SHORT",
            row,
            rs_pct,
            market_ret,
            regime,
            "tier123_late_bearish_trend_day_ema_stair_short",
        ))

    if (
        minute >= 660
        and controlled
        and vwap_dist <= -2.0
        and lower_wick >= 0.35
        and close > _safe_float(prev.get("high"))
        and (not np.isfinite(rsi) or rsi <= 38)
        and (not np.isfinite(stoch) or stoch <= 35)
    ):
        rows.append(_tier123_candidate(
            str(ticker).upper(),
            "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG",
            "LONG",
            row,
            rs_pct,
            market_ret,
            regime,
            "tier123_high_volume_controlled_vwap_extreme_fade_long",
        ))

    if (
        minute >= 660
        and controlled
        and vwap_dist >= 2.0
        and upper_wick >= 0.35
        and close < _safe_float(prev.get("low"))
        and (not np.isfinite(rsi) or rsi >= 62)
        and (not np.isfinite(stoch) or stoch >= 65)
    ):
        rows.append(_tier123_candidate(
            str(ticker).upper(),
            "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT",
            "SHORT",
            row,
            rs_pct,
            market_ret,
            regime,
            "tier123_quiet_volume_quality_controlled_vwap_extreme_fade_short",
        ))
    return rows


_TIER123_WORKER_MARKET_CTX: Dict[str, Dict[str, Any]] = {}


def _tier123_worker_init(market_ctx: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
    global _TIER123_WORKER_MARKET_CTX
    _TIER123_WORKER_MARKET_CTX = market_ctx or {}


def _tier123_worker_scan(payload: tuple[str, str]) -> list[dict]:
    ticker, slot_iso = payload
    try:
        return _scan_tier123_ticker_slot(
            str(ticker).upper(),
            pd.Timestamp(slot_iso),
            _TIER123_WORKER_MARKET_CTX,
        )
    except Exception:
        return []


def _tier123_probe_gate(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame() if raw is None else raw
    work = raw.copy()
    work["_day"] = pd.to_datetime(work["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
    work["_score"] = pd.to_numeric(work["quality_score"], errors="coerce").fillna(0.0)
    work = (
        work.sort_values(["setup", "ticker", "_day", "_score"], ascending=[True, True, True, False])
        .drop_duplicates(subset=["setup", "ticker", "_day"], keep="first")
    )
    frames = []
    for _, group in work.groupby("setup", sort=True):
        frames.append(group.sort_values("_score", ascending=False).head(TIER123_MAX_RAW_PER_SETUP))
    out = pd.concat(frames, ignore_index=True) if frames else work.iloc[0:0].copy()
    return out.drop(columns=["_day", "_score"], errors="ignore").reset_index(drop=True)


def scan_tier123_live_slot(
    slot: Any,
    tickers: Iterable[str],
    *,
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
    profile: str | None = DEFAULT_SELECTED_STRATEGY_PROFILE,
    max_workers: Optional[int] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    profile_n = _normalise_selected_strategy_profile(profile)
    stats: Dict[str, Any] = {
        "v11_tier123_live_enabled": bool(profile_n == TIER123_BALANCED_PROFILE),
        "v11_tier123_live_scan_workers": 0,
        "v11_tier123_live_scan_mode": "disabled",
        "v11_tier123_live_tickers_scanned": 0,
        "v11_tier123_live_scan_elapsed_sec": 0.0,
        "v11_tier123_live_raw_candidates": 0,
        "v11_tier123_live_probe_gated": 0,
        "v11_tier123_live_selected": 0,
        "v11_tier123_live_rejected": 0,
    }
    if profile_n != TIER123_BALANCED_PROFILE:
        return pd.DataFrame(), pd.DataFrame(), stats

    slot_ts = _normalise_ts(slot).floor("min")
    started = pd.Timestamp.now()
    signal_minute = float(slot_ts.hour * 60 + slot_ts.minute)
    tier123_first_signal_minute = min(660.0, TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE)
    if signal_minute < tier123_first_signal_minute:
        stats.update(
            {
                "v11_tier123_live_scan_mode": "time_window_skipped",
                "v11_tier123_live_skip_reason": (
                    f"signal_minute {signal_minute:.0f} before tier123 first eligible minute "
                    f"{tier123_first_signal_minute:.0f}"
                ),
                "v11_tier123_live_scan_elapsed_sec": round(
                    (pd.Timestamp.now() - started).total_seconds(),
                    3,
                ),
            }
        )
        return pd.DataFrame(), pd.DataFrame(), stats
    eligible_tickers = [
        str(ticker).strip().upper()
        for ticker in tickers
        if str(ticker).strip()
        and not str(ticker).strip().upper().startswith("NIFTY")
        and not str(ticker).strip().upper().endswith("BEES")
    ]
    configured_workers = int(max_workers if max_workers is not None else TIER123_LIVE_SCAN_WORKERS)
    workers = min(max(1, configured_workers), max(1, len(eligible_tickers)))
    stats.update(
        {
            "v11_tier123_live_scan_workers": int(workers),
            "v11_tier123_live_scan_mode": "parallel_process_pool" if workers > 1 else "serial",
            "v11_tier123_live_tickers_scanned": int(len(eligible_tickers)),
        }
    )
    rows: list[dict] = []
    if workers <= 1:
        for ticker_s in eligible_tickers:
            try:
                rows.extend(_scan_tier123_ticker_slot(ticker_s, slot_ts, market_ctx))
            except Exception:
                continue
    else:
        payloads = [(ticker_s, slot_ts.isoformat()) for ticker_s in eligible_tickers]
        try:
            with ProcessPoolExecutor(
                max_workers=workers,
                initializer=_tier123_worker_init,
                initargs=(market_ctx or {},),
            ) as ex:
                for result in ex.map(_tier123_worker_scan, payloads, chunksize=24):
                    if result:
                        rows.extend(result)
        except Exception as exc:
            rows = []
            stats["v11_tier123_live_scan_mode"] = "serial_fallback"
            stats["v11_tier123_live_parallel_error"] = f"{type(exc).__name__}: {exc}"
            for ticker_s in eligible_tickers:
                try:
                    rows.extend(_scan_tier123_ticker_slot(ticker_s, slot_ts, market_ctx))
                except Exception:
                    continue
    stats["v11_tier123_live_scan_elapsed_sec"] = round(
        (pd.Timestamp.now() - started).total_seconds(),
        3,
    )
    raw = pd.DataFrame(rows)
    stats["v11_tier123_live_raw_candidates"] = int(len(raw))
    gated = _tier123_probe_gate(raw)
    stats["v11_tier123_live_probe_gated"] = int(len(gated))
    if gated.empty:
        return gated, gated.copy(), stats

    accepted, rejected, selected_stats = apply_selected_strategy_profile(
        gated,
        profile_n,
        estimate_missing_notional=True,
    )
    if not accepted.empty:
        accepted["v11_live_overlay_status"] = "PASSED"
        accepted["v11_live_overlay_source"] = "v11_tier123_balanced_addon"
        accepted["selection_mode"] = "tier123_balanced_addon_gate"
        accepted["candidate_family"] = "V11_TIER123_BALANCED_ADDON"
        accepted["status"] = "V11_TIER123_BALANCED_PASSED"
    if not rejected.empty:
        rejected["v11_live_overlay_status"] = "REJECTED"
        rejected["v11_live_overlay_source"] = "v11_tier123_balanced_addon"
        rejected["status"] = "V11_TIER123_BALANCED_REJECTED"
    stats.update({
        "v11_tier123_live_selected": int(len(accepted)),
        "v11_tier123_live_rejected": int(len(rejected)),
        **{f"v11_tier123_live_{k}": v for k, v in selected_stats.items()},
    })
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True), stats
