"""
AVWAP ID 5-min v11 live-strategy backtester.

v11 keeps the v8 backtesting output format, but the candidate source and
filters mirror the current v7 live scanner. Each historical slot is scanned,
ranked, passed through the live gate and research filters, then resolved with
the next available 1-minute open after the 5-minute signal time and
setup-specific 1-minute exits.

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v11_5min

Default historical 5-minute source:
  C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import eqidv2_signal_discovery_v7_5min_id_persistent as live_discovery
import avwap_5min_ID_v7_candidate_scan as candidate_scan
import avwap_5min_ID_v5_backtesting as v5
import avwap_5min_ID_v6_backtesting as v6
import v17D_exit_resolver as er


OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_5min")
LIVE_CANDIDATE_JSON_DIR = Path(r"C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json")
LIVE_PAPER_DIR = Path(r"C:\TradingData\eqidv2\live_signals")
HISTORICAL_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_CANDIDATE_5M_DIR = HISTORICAL_5M_DIR
EXCLUDED_SETUPS = candidate_scan.EXCLUDED_SETUPS

# Mirrors the current v7 live chain:
#   signal discovery -> 1-minute entry engine -> PAPER_TRADE_TRUE executor.
V7_ENTRY_SEARCH_MAX_DELAY_MIN = 5
V7_SIGNAL_MARGIN_RS = 20_000.0
V7_INTRADAY_LEVERAGE = 5.0
V7_SIGNAL_NOTIONAL_RS = V7_SIGNAL_MARGIN_RS * V7_INTRADAY_LEVERAGE
V7_PAPER_SLIPPAGE_PCT = 0.0005

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
TIER123_BALANCED_PROFILE = "production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced"
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
v6.SETUP_EXIT_RULES.update(TIER123_BALANCED_EXIT_RULES)
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
)
SELECTED_STRATEGY_DEFAULT_PROFILE = "production_core"
SELECTED_STRATEGY_RULE_LABELS = {
    "C_OR_BREAKOUT": "production_core: broad C_OR_BREAKOUT kept after honest holdout test",
    "D_EMA20_BOUNCE": "production_core: vol_ratio <= 1.5975512 OR vwap_dist_atr >= -0.38557115, and signal_minute <= 705",
    "E_ORB_BREAKOUT_LONG": "production_core: v7_signal_notional_rs >= 99937.32",
    "E_ORB_BREAKOUT_SHORT": "production_core: market_ret_pct >= -0.63438346, quality_score >= 97.873364, upper_wick_pct <= 0.014647435; exit 0.80/1.50",
    "L_BB_SQUEEZE_LONG": "production_core: market_abs_ret_pct <= 0.74284715 OR vol_ratio <= 3.0227043, and ranker_score >= 0.7332456",
    "E_VWAP_LOSE_EARLY_SHORT": (
        "profile-specific: production_core_tiny uses regime == BEAR; "
        f"production_core_ab_max_pnl_low_valid uses vwap_dist_atr >= {MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR:.7f}"
    ),
}
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        setup: (
            "ab_probe: explicit A/B probation gate; admitted by top-slot quality only, "
            "requires separate train/holdout PF validation"
        )
        for setup in AB_PROBATION_SETUPS
    }
)
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        setup: (
            "ab_good_probe: selected from 2026-06-01 A/B diagnostic as a positive-PnL "
            "A/B watchlist setup; still probation, not production"
        )
        for setup in AB_GOOD_RESULT_SETUPS
    }
)
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        "B_HUGE_C1_CLOSE_RECLAIM_BREAK": (
            "ab_filtered_relaxed: B_HUGE_C1_CLOSE_RECLAIM_BREAK with "
            f"rs_pct <= {AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT:.4f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_PULLBACK_C2_THEN_BREAK_C2_LOW": (
            "ab_filtered_relaxed: A_PULLBACK_C2_THEN_BREAK_C2_LOW with "
            f"market_abs_ret_pct <= {AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT:.4f}; "
            "A/B quality_top_slot probation gate still required"
        ),
    }
)
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        "S_BB_SQUEEZE_SHORT": (
            "max_pnl_low_valid: relaxed S_BB_SQUEEZE_SHORT with "
            f"market_ret_pct >= {MAX_PNL_SBB_MIN_MARKET_RET_PCT:.8f} OR "
            f"v7_signal_notional_rs >= {MAX_PNL_SBB_MIN_NOTIONAL_RS:.2f}; "
            "residual_overlay profile additionally admits residual morning S_BB with "
            f"signal_minute <= {RESIDUAL_OVERLAY_SBB_MAX_SIGNAL_MINUTE:.1f}"
        ),
        "B_AVWAP_RECLAIM_REVERSAL": (
            "max_pnl_low_valid: B_AVWAP_RECLAIM_REVERSAL with "
            f"vwap_dist_atr >= {MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR:.8f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_MOD_CLOSE_CONTINUATION_BREAK": (
            "max_pnl_low_valid: A_MOD_CLOSE_CONTINUATION_BREAK with "
            f"signal_range_pct >= {MAX_PNL_A_MOD_CLOSE_MIN_SIGNAL_RANGE_PCT:.7f} OR "
            f"v7_signal_notional_rs <= {MAX_PNL_A_MOD_CLOSE_MAX_NOTIONAL_RS:.0f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_MOD_BREAK_C1_LOW": (
            "max_pnl_low_valid micro-probe: A_MOD_BREAK_C1_LOW with "
            f"abs(rs_pct) >= {MAX_PNL_A_MOD_C1_LOW_MIN_RS_ABS_PCT:.7f} AND "
            f"vol_ratio >= {MAX_PNL_A_MOD_C1_LOW_MIN_VOL_RATIO:.7f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_MOD_BREAK_C1_HIGH": (
            "max_pnl_low_valid add-on: A_MOD_BREAK_C1_HIGH with "
            f"market_abs_ret_pct <= {MAX_PNL_A_MOD_C1_HIGH_MAX_MARKET_ABS_RET_PCT:.7f} AND "
            f"vol_ratio <= {MAX_PNL_A_MOD_C1_HIGH_MAX_VOL_RATIO:.7f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "D_EMA20_REJECTION": (
            "max_pnl_low_valid add-on: D_EMA20_REJECTION with "
            f"body_pct >= {MAX_PNL_D_EMA20_REJECTION_MIN_BODY_PCT:.8f} AND "
            f"ranker_score >= {MAX_PNL_D_EMA20_REJECTION_MIN_RANKER_SCORE:.6f}; "
            "residual_overlay profile additionally admits late-session residual D_EMA20_REJECTION "
            f"when {RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_SIGNAL_MINUTE:.0f} < signal_minute <= "
            f"{RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_SIGNAL_MINUTE:.0f} and "
            f"(body_pct >= {RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_BODY_PCT:.8f} OR "
            f"wick_skew_pct <= {RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_WICK_SKEW_PCT:.9f})"
        ),
        "E_VWAP_BAND_FADE": (
            "max_pnl_low_valid: filtered E_VWAP_BAND_FADE count add with "
            f"atr_pct >= {MAX_PNL_E_VWAP_BAND_MIN_ATR_PCT:.10f} AND "
            f"signal_minute <= {MAX_PNL_E_VWAP_BAND_MAX_SIGNAL_MINUTE}"
        ),
        "T_TREND_DAY_EMA_STAIR_SHORT": (
            "tier123_balanced: late bearish trend-day EMA stair short with "
            f"market_ret_pct <= {TIER123_T_STAIR_SHORT_MAX_MARKET_RET_PCT:.6f} AND "
            f"{TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE:.1f} <= signal_minute <= "
            f"{TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE:.1f}; "
            "resolved as non-overlap add-on after the protected residual-overlay book"
        ),
        "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": (
            "tier123_balanced: controlled VWAP extreme fade long with "
            f"vol_ratio >= {TIER123_MR_FADE_LONG_MIN_VOL_RATIO:.6f}; "
            "resolved as non-overlap add-on after the protected residual-overlay book"
        ),
        "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": (
            "tier123_balanced: controlled VWAP extreme fade short with "
            f"vol_ratio <= {TIER123_MR_FADE_SHORT_MAX_VOL_RATIO:.6f} AND "
            f"quality_score >= {TIER123_MR_FADE_SHORT_MIN_QUALITY_SCORE:.6f}; "
            "resolved as non-overlap add-on after the protected residual-overlay book"
        ),
    }
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


@lru_cache(maxsize=None)
def _load_1m_with_open(ticker: str) -> pd.DataFrame | None:
    path = v6.DATA_1M_DIR / f"{str(ticker).upper()}_stocks_indicators_1min.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df.set_index("date")


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(value)
        if not np.isfinite(out):
            return default
        return out
    except Exception:
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _signal_time(row: pd.Series) -> pd.Timestamp:
    for col in ("signal_time_ist", "signal_ts", "bar_time_ist"):
        if col in row.index and pd.notna(row.get(col)):
            ts = _normalise_ts(row.get(col))
            if pd.notna(ts):
                return ts
    entry = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    ts = _normalise_ts(entry)
    if pd.notna(ts):
        return ts - pd.Timedelta(minutes=5)
    return pd.NaT


def _first_1m_entry(
    bars: pd.DataFrame,
    signal_ts: pd.Timestamp,
    *,
    max_delay_minutes: int = 1,
) -> tuple[pd.Timestamp, float] | None:
    sig = _normalise_ts(signal_ts)
    if pd.isna(sig):
        return None
    latest_allowed = sig + pd.Timedelta(minutes=max_delay_minutes)
    sub = bars[(bars.index > sig) & (bars.index <= latest_allowed)]
    if sub.empty:
        return None
    entry_ts = pd.Timestamp(sub.index[0])
    entry_px = float(sub.iloc[0]["open"])
    if not np.isfinite(entry_px) or entry_px <= 0:
        return None
    return entry_ts, entry_px


def _resolve_trade_1m_entry(row: pd.Series, cost_bps: float) -> dict | None:
    setup = str(row["setup"])
    if setup not in v6.SETUP_EXIT_RULES:
        raise ValueError(f"missing v11 exit rule for setup: {setup}")
    sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup]
    if sl_pct < v6.MIN_SL_PCT:
        raise ValueError(f"SL for {setup} is below {v6.MIN_SL_PCT:.2f}%: {sl_pct}")

    bars = _load_1m_with_open(str(row["ticker"]))
    if bars is None or bars.empty:
        return None

    sig_ts = _signal_time(row)
    entry = _first_1m_entry(bars, sig_ts)
    if entry is None:
        return None
    entry_ts, entry_px = entry

    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=entry_px,
        entry_time_ist=entry_ts,
        sl_pct=sl_pct,
        tgt_pct=target_pct,
    )
    if res is None:
        return None

    net, gross, cost = v6._net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
    old_entry_ts = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    old_entry_px = row.get("entry_price_v6", row.get("entry_price", row.get("entry_px", np.nan)))
    rec = row.to_dict()
    rec.update({
        "signal_time_v8": sig_ts,
        "old_entry_time_v7": old_entry_ts,
        "old_entry_price_v7": old_entry_px,
        "entry_time_v6": entry_ts,
        "entry_price_v6": float(entry_px),
        "trade_date": str(entry_ts.date()),
        "v8_entry_model": "NEXT_1MIN_OPEN_AFTER_5MIN_SIGNAL",
        "v8_entry_delay_minutes": float((entry_ts - sig_ts).total_seconds() / 60.0) if pd.notna(sig_ts) else np.nan,
        "v6_sl_pct": sl_pct,
        "v6_target_pct": target_pct,
        "v6_outcome": res.outcome,
        "v6_exit_price": float(res.exit_price),
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(gross),
        "v6_cost_rs": float(cost),
        "v6_net_pnl_rs": float(net),
        "capital_per_trade_rs": v6.CAPITAL_PER_TRADE,
        "leverage": v6.LEVERAGE,
        "notional_exposure_rs": v6.EFFECTIVE_NOTIONAL,
    })
    return rec


def _price_pnl_rs(side: str, entry_price: float, exit_price: float, quantity: int) -> float:
    if str(side).upper() == "SHORT":
        return float((entry_price - exit_price) * quantity)
    return float((exit_price - entry_price) * quantity)


def _side_exit_pcts(side: str, entry_price: float, stop_price: float, target_price: float) -> tuple[float, float]:
    side = str(side).upper()
    if entry_price <= 0 or stop_price <= 0 or target_price <= 0:
        return np.nan, np.nan
    if side == "LONG":
        return (
            float((entry_price - stop_price) / entry_price * 100.0),
            float((target_price - entry_price) / entry_price * 100.0),
        )
    return (
        float((stop_price - entry_price) / entry_price * 100.0),
        float((entry_price - target_price) / entry_price * 100.0),
    )


def _selected_strategy_numeric(frame: pd.DataFrame, col: str) -> pd.Series:
    if col in frame.columns:
        return pd.to_numeric(frame[col], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _selected_strategy_features(signals: pd.DataFrame) -> pd.DataFrame:
    work = signals.copy()
    for col in [
        "vol_ratio",
        "vwap_dist_atr",
        "v7_signal_notional_rs",
        "market_ret_pct",
        "quality_score",
        "ranker_score",
        "body_pct",
        "signal_open",
        "signal_high",
        "signal_low",
        "signal_close",
    ]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

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


def _normalise_selected_strategy_profile(profile: str | None) -> str:
    out = str(profile or SELECTED_STRATEGY_DEFAULT_PROFILE).strip().lower()
    if out not in SELECTED_STRATEGY_PROFILE_CHOICES:
        raise SystemExit(
            f"[v11 selected_strategy] unknown profile {profile!r}; "
            f"choose one of {', '.join(SELECTED_STRATEGY_PROFILE_CHOICES)}"
        )
    return out


def _selected_strategy_mask(signals: pd.DataFrame, profile: str) -> pd.Series:
    profile = _normalise_selected_strategy_profile(profile)
    if signals is None or signals.empty:
        return pd.Series(False, index=pd.RangeIndex(0))
    if profile == "none":
        return pd.Series(True, index=signals.index)

    work = _selected_strategy_features(signals)
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
    if profile == "ab_only_probe":
        return ab_mask.fillna(False)
    if profile == "ab_good_only_probe":
        return ab_good_mask.fillna(False)
    if profile == "ab_filtered_relaxed_only":
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
    if profile == "production_core_tiny":
        mask |= setup.eq("E_VWAP_LOSE_EARLY_SHORT") & regime.eq("BEAR")
    if profile == "production_core_ab_probe":
        mask |= ab_mask
    if profile == "production_core_ab_good_probe":
        mask |= ab_good_mask
    if profile == "production_core_ab_filtered_relaxed":
        mask |= ab_filtered_relaxed_mask
    if profile == "production_core_ab_max_pnl_low_valid":
        mask |= max_pnl_low_valid_mask
    if profile == "production_core_ab_max_pnl_low_valid_residual_overlay":
        mask |= max_pnl_residual_overlay_mask
    if profile == TIER123_BALANCED_PROFILE:
        mask |= max_pnl_residual_overlay_mask
        mask |= tier123_balanced_mask

    return mask.fillna(False)


def _apply_selected_strategy_profile(
    signals: pd.DataFrame,
    profile: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    profile = _normalise_selected_strategy_profile(profile)
    if signals is None:
        signals = pd.DataFrame()
    if signals.empty:
        stats = {
            "selected_strategy_profile": profile,
            "selected_strategy_input_signals": 0,
            "selected_strategy_signals": 0,
            "selected_strategy_rejects": 0,
        }
        return signals.copy(), pd.DataFrame(), stats

    work = _selected_strategy_features(signals)
    mask = _selected_strategy_mask(work, profile)
    accepted = work.loc[mask].copy().reset_index(drop=True)
    rejected = work.loc[~mask].copy().reset_index(drop=True)
    if not accepted.empty:
        accepted["v11_selected_strategy_profile"] = profile
        accepted["v11_selected_strategy_rule"] = accepted["setup"].astype(str).map(SELECTED_STRATEGY_RULE_LABELS).fillna("")
    if not rejected.empty:
        rejected["v11_selected_strategy_profile"] = profile
        rejected["v11_selected_strategy_reject_reason"] = "not_in_selected_strategy_profile_or_rule_failed"

    stats = {
        "selected_strategy_profile": profile,
        "selected_strategy_input_signals": int(len(work)),
        "selected_strategy_signals": int(len(accepted)),
        "selected_strategy_rejects": int(len(rejected)),
    }
    return accepted, rejected, stats


def _selected_exit_override(setup: str, profile: str) -> tuple[float, float] | None:
    profile = _normalise_selected_strategy_profile(profile)
    return SELECTED_STRATEGY_EXIT_OVERRIDES.get(profile, {}).get(str(setup), None)


def _normalise_ab_gate_profile(profile: str | None) -> str:
    out = str(profile or "off").strip().lower()
    if out not in AB_GATE_PROFILE_CHOICES:
        raise SystemExit(
            f"[v11 ab_gate] unknown profile {profile!r}; "
            f"choose one of {', '.join(AB_GATE_PROFILE_CHOICES)}"
        )
    return out


def _ab_gate_enabled(profile: str | None) -> bool:
    return _normalise_ab_gate_profile(profile) != "off"


def _ab_gate_setups_for_selected_profile(profile: str | None) -> tuple[str, ...]:
    profile = _normalise_selected_strategy_profile(profile)
    if profile in {
        "production_core_ab_max_pnl_low_valid",
        "production_core_ab_max_pnl_low_valid_residual_overlay",
        TIER123_BALANCED_PROFILE,
    }:
        return AB_MAX_PNL_LOW_VALID_SETUPS
    if profile in {"production_core_ab_filtered_relaxed", "ab_filtered_relaxed_only"}:
        return AB_FILTERED_RELAXED_SETUPS
    if profile in {"production_core_ab_good_probe", "ab_good_only_probe"}:
        return AB_GOOD_RESULT_SETUPS
    return AB_PROBATION_SETUPS


def _ab_gate_base_stats(
    *,
    profile: str,
    min_quality: float,
    max_per_side: int,
    max_per_slot: int,
    allowed_setups: tuple[str, ...] | None = None,
) -> dict:
    profile = _normalise_ab_gate_profile(profile)
    allowed = tuple(allowed_setups or AB_PROBATION_SETUPS)
    return {
        "ab_gate_profile": profile,
        "ab_gate_enabled": bool(profile != "off"),
        "ab_gate_allowed_setups": int(len(allowed)),
        "ab_gate_min_quality": float(min_quality),
        "ab_gate_max_per_side": int(max(1, max_per_side)),
        "ab_gate_max_per_slot": int(max(1, max_per_slot)),
        "ab_gate_candidates": 0,
        "ab_gate_quality_passed": 0,
        "ab_gate_quality_rejected": 0,
        "ab_gate_accepted": 0,
    }


def _apply_ab_probation_gate(
    ranked_slot: pd.DataFrame,
    *,
    profile: str,
    min_quality: float,
    max_per_side: int,
    max_per_slot: int,
    allowed_setups: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, dict]:
    profile = _normalise_ab_gate_profile(profile)
    allowed = tuple(allowed_setups or AB_PROBATION_SETUPS)
    stats = _ab_gate_base_stats(
        profile=profile,
        min_quality=min_quality,
        max_per_side=max_per_side,
        max_per_slot=max_per_slot,
        allowed_setups=allowed,
    )
    if profile == "off" or ranked_slot is None or ranked_slot.empty:
        return pd.DataFrame() if ranked_slot is None else ranked_slot.iloc[0:0].copy(), stats

    work = _ensure_candidate_id(ranked_slot)
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
        selected["v8_live_gate_version"] = "v11_ab_probe_v1"
        selected["research_live_filter_status"] = "AB_PROBATION_BYPASS"
        selected["research_live_filter_reason"] = "explicit_ab_gate_profile"
        selected["research_live_filter_version"] = "v11_ab_probe_v1"
        selected["ab_gate_profile"] = profile
        selected["ab_gate_rule"] = rule
    return selected, stats


def _v7_signal_id(ticker: str, side: str, bar_time: str, setup: str) -> str:
    raw = f"{str(ticker).upper()}|{str(side).upper()}|{str(bar_time)}|{str(setup)}"
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _v7_entry_engine_candidate_time(row: pd.Series) -> pd.Timestamp:
    return _candidate_time(row, "signal_time_ist", "signal_ts", "bar_time_ist", "scan_slot_ist")


def _v7_entry_engine_raw_rows(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build rows the v7 1-minute entry engine would hand to the signal CSV writer.

    This mirrors eqidv2_entry_engine_1min_v5_id.py without fetching from Kite:
    it uses historical 1-minute OHLC, the next available 1-minute open after the
    5-minute signal, v6.SETUP_EXIT_RULES, and v7's Rs 20k margin / 5x sizing.
    """
    if candidates is None or candidates.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows: list[dict] = []
    rejects: list[dict] = []
    work = _ensure_candidate_id(candidates)
    work = _candidate_sort_frame(work)
    work["_v11_entry_signal_ts"] = [_v7_entry_engine_candidate_time(row) for _, row in work.iterrows()]
    work["_v11_entry_day"] = [
        ts.strftime("%Y-%m-%d") if pd.notna(ts) else ""
        for ts in work["_v11_entry_signal_ts"]
    ]
    work["_v11_entry_slot"] = [
        _fmt_ist(ts) if pd.notna(ts) else ""
        for ts in work["_v11_entry_signal_ts"]
    ]
    if "quality_score" in work.columns:
        score_source = work["quality_score"]
    elif "live_rank_score" in work.columns:
        score_source = work["live_rank_score"]
    else:
        score_source = pd.Series(0.0, index=work.index)
    work["_v11_score"] = pd.to_numeric(score_source, errors="coerce").fillna(0.0)
    work = work.sort_values(["_v11_entry_day", "_v11_entry_signal_ts", "_v11_input_order"]).reset_index(drop=True)

    for _, row in work.iterrows():
        ticker = str(row.get("ticker", "")).upper().strip()
        side = str(row.get("side", "")).upper().strip()
        setup = str(row.get("setup", "")).strip()
        signal_ts = row.get("_v11_entry_signal_ts", pd.NaT)
        reject_base = {
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "signal_time_ist": _fmt_ist(signal_ts) if pd.notna(signal_ts) else "",
            "candidate_id": str(row.get("candidate_id", "")),
        }
        if not ticker or side not in {"LONG", "SHORT"} or pd.isna(signal_ts):
            rejects.append({**reject_base, "reject_reason": "invalid_required_fields"})
            continue
        if setup not in v6.SETUP_EXIT_RULES:
            rejects.append({**reject_base, "reject_reason": "missing_v8_setup_exit_rule"})
            continue
        bars = _load_1m_with_open(ticker)
        if bars is None or bars.empty:
            rejects.append({**reject_base, "reject_reason": "missing_1min_file"})
            continue
        entry = _first_1m_entry(
            bars,
            signal_ts,
            max_delay_minutes=V7_ENTRY_SEARCH_MAX_DELAY_MIN,
        )
        if entry is None:
            rejects.append({**reject_base, "reject_reason": "missing_1min_entry_bar"})
            continue

        entry_ts, signal_entry_price_raw = entry
        signal_entry_price = round(float(signal_entry_price_raw), 2)
        if signal_entry_price <= 0:
            rejects.append({**reject_base, "reject_reason": "invalid_entry_price"})
            continue

        sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup]
        if side == "LONG":
            stop_price = signal_entry_price * (1.0 - sl_pct / 100.0)
            target_price = signal_entry_price * (1.0 + target_pct / 100.0)
        else:
            stop_price = signal_entry_price * (1.0 + sl_pct / 100.0)
            target_price = signal_entry_price * (1.0 - target_pct / 100.0)
        stop_price = round(float(stop_price), 2)
        target_price = round(float(target_price), 2)
        quantity = max(1, int(V7_SIGNAL_NOTIONAL_RS / signal_entry_price))
        bar_time = _fmt_ist(signal_ts)
        score = float(row.get("_v11_score", 0.0))
        out = row.drop(labels=[c for c in row.index if str(c).startswith("_v11_")], errors="ignore").to_dict()
        out.update({
            "signal_id": _v7_signal_id(ticker, side, bar_time, setup),
            "signal_datetime": bar_time,
            "signal_time_ist": bar_time,
            "bar_time_ist": bar_time,
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "score": score,
            "quality_score": score,
            "v7_signal_entry_time_ist": _fmt_ist(entry_ts),
            "v7_signal_entry_price": signal_entry_price,
            "v7_signal_stop_price": stop_price,
            "v7_signal_target_price": target_price,
            "v7_signal_sl_pct": float(sl_pct),
            "v7_signal_target_pct": float(target_pct),
            "quantity": int(quantity),
            "signal_entry_datetime_ist": bar_time,
            "signal_bar_time_ist": bar_time,
            "v7_entry_engine_model": "next_1min_open_after_5min_signal",
            "v7_signal_notional_rs": float(signal_entry_price * quantity),
        })
        rows.append(out)

    return pd.DataFrame(rows), pd.DataFrame(rejects)


def _select_v7_entry_engine_signals(raw_entries: pd.DataFrame) -> pd.DataFrame:
    if raw_entries is None or raw_entries.empty:
        return pd.DataFrame()

    df = raw_entries.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["bar_time_ist"] = df["bar_time_ist"].astype(str)
    df["_score_num"] = pd.to_numeric(df.get("score", df.get("quality_score", 0.0)), errors="coerce").fillna(0.0)
    df["_signal_ts"] = pd.to_datetime(df["bar_time_ist"], errors="coerce")
    if getattr(df["_signal_ts"].dt, "tz", None) is None:
        df["_signal_ts"] = df["_signal_ts"].dt.tz_localize("Asia/Kolkata")
    else:
        df["_signal_ts"] = df["_signal_ts"].dt.tz_convert("Asia/Kolkata")
    df["_day"] = df["_signal_ts"].dt.strftime("%Y-%m-%d")
    df["_slot"] = df["_signal_ts"].map(_fmt_ist)

    selected_frames: list[pd.DataFrame] = []
    for _, slot_df in df.sort_values(["_day", "_signal_ts"]).groupby(["_day", "_slot"], sort=True, dropna=False):
        selected = (
            slot_df.sort_values(["_score_num", "setup"], ascending=[False, True])
            .drop_duplicates(subset=["bar_time_ist", "ticker"], keep="first")
            .copy()
        )
        selected_frames.append(selected)
    if not selected_frames:
        return pd.DataFrame()

    selected_all = pd.concat(selected_frames, ignore_index=True)
    keep: list[dict] = []
    for _, day_df in selected_all.sort_values(["_day", "_signal_ts"]).groupby("_day", sort=True, dropna=False):
        used_tickers: set[str] = set()
        for _, slot_df in day_df.groupby("_slot", sort=True, dropna=False):
            batch: set[str] = set()
            slot_sorted = slot_df.sort_values(["_score_num", "ticker"], ascending=[False, True])
            for _, row in slot_sorted.iterrows():
                ticker = str(row.get("ticker", "")).upper().strip()
                if not ticker or ticker in used_tickers or ticker in batch:
                    continue
                keep.append(row.to_dict())
                used_tickers.add(ticker)
                batch.add(ticker)

    out = pd.DataFrame(keep)
    return out.drop(columns=["_score_num", "_signal_ts", "_day", "_slot"], errors="ignore").reset_index(drop=True)


def _build_v7_entry_engine_signals(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_entries, rejects = _v7_entry_engine_raw_rows(candidates)
    selected = _select_v7_entry_engine_signals(raw_entries)
    return selected, raw_entries, rejects


def _resolve_v7_entry_engine_signal(
    row: pd.Series,
    *,
    label: str,
    entry_fill_model: str,
    selected_strategy_profile: str = "none",
) -> dict | None:
    ticker = str(row.get("ticker", "")).upper().strip()
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", "")).strip()
    bars = _load_1m_with_open(ticker)
    if bars is None or bars.empty:
        return None

    signal_ts = _normalise_ts(row.get("signal_time_ist", row.get("signal_datetime", pd.NaT)))
    entry_ts = _normalise_ts(row.get("v7_signal_entry_time_ist", pd.NaT))
    signal_entry = _safe_float(row.get("v7_signal_entry_price"), 0.0)
    stop_price = _safe_float(row.get("v7_signal_stop_price"), 0.0)
    target_price = _safe_float(row.get("v7_signal_target_price"), 0.0)
    quantity = _safe_int(row.get("quantity"), 0)
    if pd.isna(entry_ts) or side not in {"LONG", "SHORT"} or signal_entry <= 0 or stop_price <= 0 or target_price <= 0 or quantity <= 0:
        return None

    fill_model = str(entry_fill_model or "ltp_on_signal_1m_open").strip().lower()
    if fill_model == "signal_bar":
        entry_price = signal_entry
        entry_source_used = "signal_bar"
    else:
        if side == "LONG":
            entry_price = round(signal_entry * (1.0 + V7_PAPER_SLIPPAGE_PCT), 2)
        else:
            entry_price = round(signal_entry * (1.0 - V7_PAPER_SLIPPAGE_PCT), 2)
        stop_mult = stop_price / signal_entry
        target_mult = target_price / signal_entry
        stop_price = round(entry_price * stop_mult, 2)
        target_price = round(entry_price * target_mult, 2)
        entry_source_used = "ltp_on_signal_1m_open"

    exit_override = _selected_exit_override(setup, selected_strategy_profile)
    exit_override_applied = exit_override is not None
    if exit_override_applied:
        sl_pct, target_pct = exit_override
        if side == "LONG":
            stop_price = round(entry_price * (1.0 - (sl_pct / 100.0)), 2)
            target_price = round(entry_price * (1.0 + (target_pct / 100.0)), 2)
        else:
            stop_price = round(entry_price * (1.0 + (sl_pct / 100.0)), 2)
            target_price = round(entry_price * (1.0 - (target_pct / 100.0)), 2)
        exit_rule_source = "selected_strategy_exit_override"
    else:
        sl_pct, target_pct = _side_exit_pcts(side, entry_price, stop_price, target_price)
        exit_rule_source = "v7_signal_stop_target"
    if not np.isfinite(sl_pct) or not np.isfinite(target_pct) or sl_pct <= 0 or target_pct <= 0:
        return None

    res = er.resolve(
        bars=bars,
        side=side,
        entry_price=entry_price,
        entry_time_ist=entry_ts,
        sl_pct=sl_pct,
        tgt_pct=target_pct,
    )
    if res is None:
        return None

    pnl_rs = _price_pnl_rs(side, entry_price, float(res.exit_price), quantity)
    rec = row.to_dict()
    rec.update({
        "signal_time_v8": signal_ts,
        "old_entry_time_v7": entry_ts,
        "old_entry_price_v7": signal_entry,
        "entry_time_v6": entry_ts,
        "entry_price_v6": float(entry_price),
        "trade_date": str(entry_ts.date()),
        "v8_entry_model": f"V7_ENTRY_ENGINE_NEXT_1MIN_OPEN__PAPER_{entry_source_used.upper()}",
        "v8_entry_delay_minutes": float((entry_ts - signal_ts).total_seconds() / 60.0) if pd.notna(signal_ts) else np.nan,
        "v6_sl_pct": float(sl_pct),
        "v6_target_pct": float(target_pct),
        "v6_outcome": res.outcome,
        "v6_exit_price": float(res.exit_price),
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(pnl_rs),
        "v6_cost_rs": 0.0,
        "v6_net_pnl_rs": float(pnl_rs),
        "capital_per_trade_rs": V7_SIGNAL_MARGIN_RS,
        "leverage": V7_INTRADAY_LEVERAGE,
        "notional_exposure_rs": float(entry_price * quantity),
        "v7_resolution_source": label,
        "v8_resolution_source": label,
        "v7_executor_entry_price_source": entry_source_used,
        "v7_live_stop_price": float(stop_price),
        "v7_live_target_price": float(target_price),
        "v11_selected_strategy_profile": _normalise_selected_strategy_profile(selected_strategy_profile),
        "v11_exit_override_applied": bool(exit_override_applied),
        "v11_exit_rule_source": exit_rule_source,
    })
    return rec


def _resolve_v7_entry_engine_signals(
    signals: pd.DataFrame,
    *,
    label: str,
    entry_fill_model: str,
    selected_strategy_profile: str = "none",
) -> pd.DataFrame:
    if signals is None or signals.empty:
        raise SystemExit(f"[v11 {label}] no v7 entry-engine signals to resolve")
    selected_strategy_profile = _normalise_selected_strategy_profile(selected_strategy_profile)
    print(
        f"[v11 {label}] resolving {len(signals):,} v7 entry-engine signals with 1-minute exits "
        f"profile={selected_strategy_profile}"
    )
    rows: list[dict] = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(signals.iterrows(), 1):
        rec = _resolve_v7_entry_engine_signal(
            row,
            label=label,
            entry_fill_model=entry_fill_model,
            selected_strategy_profile=selected_strategy_profile,
        )
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 250 == 0 or i == len(signals):
            print(f"  [v11 {label} {i:5d}/{len(signals)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")
    if not rows:
        raise SystemExit(f"[v11 {label}] no entry-engine signals resolved")
    out = pd.DataFrame(rows)
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["_entry_sort"] = pd.to_datetime(out["entry_time_v6"], errors="coerce")
    return (
        out.sort_values(["trade_date", "_entry_sort", "ticker"], ascending=[True, True, True])
        .drop(columns=["_entry_sort"], errors="ignore")
        .reset_index(drop=True)
    )


def _resolve_trades(trades: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = v6._normalise_trades(trades)
    normalised = normalised.loc[~normalised["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    missing = sorted(set(normalised["setup"].astype(str)) - set(v6.SETUP_EXIT_RULES))
    if missing:
        print(f"[v11] skipping {label} setups without setup-specific exits: {missing}")
        normalised = normalised.loc[normalised["setup"].astype(str).isin(v6.SETUP_EXIT_RULES)].copy()

    print(f"[v11] resolving {len(normalised):,} {label} trades with next-1min entry + 1-min exits")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 500 == 0 or i == len(normalised):
            print(f"  [v11 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")

    if not rows:
        raise SystemExit(f"[v11] no {label} trades resolved")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    return out


def _load_live_candidate_snapshots(json_dir: Path, day: str = "") -> pd.DataFrame:
    day_key = "".join(str(day or "").split("-"))
    raw_patterns = []
    fallback_patterns = []
    if day_key:
        raw_patterns.append(f"raw_candidate_tickers_{day_key}_*.json")
        fallback_patterns.append(f"candidate_tickers_{day_key}_*.json")
    else:
        raw_patterns.append("raw_candidate_tickers_*.json")
        fallback_patterns.append("candidate_tickers_*.json")

    raw_files = [path for pattern in raw_patterns for path in sorted(json_dir.glob(pattern))]
    fallback_files = [path for pattern in fallback_patterns for path in sorted(json_dir.glob(pattern))]
    files = raw_files or fallback_files

    rows: list[dict] = []
    seen_files: set[Path] = set()
    for path in files:
        if path in seen_files:
            continue
        seen_files.add(path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in payload.get("candidates", []) or []:
            if isinstance(row, dict):
                rows.append(row)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if day:
        sig = pd.to_datetime(df.get("signal_time_ist"), errors="coerce")
        df = df.loc[sig.dt.strftime("%Y-%m-%d").eq(str(day))].copy()
    return df.reset_index(drop=True)


def _parse_hhmm(text: str) -> int:
    try:
        hh_text, mm_text = str(text).strip().split(":", 1)
        hh = int(hh_text)
        mm = int(mm_text)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"[v11 historical_full_day] invalid time {text!r}; expected HH:MM") from exc
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise SystemExit(f"[v11 historical_full_day] invalid time {text!r}; expected HH:MM")
    return hh * 60 + mm


def _slot_range_for_day(day: str, start_time: str, end_time: str) -> list[pd.Timestamp]:
    if not str(day or "").strip():
        raise SystemExit("[v11 historical_full_day] --historical_date is required")
    day_date = pd.to_datetime(day, errors="coerce")
    if pd.isna(day_date):
        raise SystemExit(f"[v11 historical_full_day] invalid --historical_date={day!r}")
    start_min = _parse_hhmm(start_time)
    end_min = _parse_hhmm(end_time)
    if end_min < start_min:
        raise SystemExit("[v11 historical_full_day] --end_time must be >= --start_time")

    base = pd.Timestamp(
        year=int(day_date.year),
        month=int(day_date.month),
        day=int(day_date.day),
        tz="Asia/Kolkata",
    )
    return [base + pd.Timedelta(minutes=minute) for minute in range(start_min, end_min + 1, 5)]


def _fmt_ist(ts: pd.Timestamp) -> str:
    t = _normalise_ts(ts)
    if pd.isna(t):
        return ""
    offset = t.strftime("%z")
    return f"{t.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _write_candidate_snapshot(
    df: pd.DataFrame,
    slot: pd.Timestamp,
    path: Path,
    *,
    output_label: str,
    payload_extra: dict | None = None,
) -> None:
    rows = [] if df is None or df.empty else df.to_dict("records")
    side_counts = {"LONG": 0, "SHORT": 0}
    setup_counts: dict[str, int] = {}
    for row in rows:
        side = str(row.get("side", "")).upper()
        setup = str(row.get("setup", ""))
        side_counts[side] = side_counts.get(side, 0) + 1
        setup_counts[setup] = setup_counts.get(setup, 0) + 1

    payload = {
        "session": "V11 exact v7 live-strategy historical regeneration",
        "output_label": output_label,
        "slot_ist": _fmt_ist(slot),
        "created_at_ist": _fmt_ist(pd.Timestamp.now(tz="Asia/Kolkata")),
        "total_candidates": int(len(rows)),
        "long_candidates": int(side_counts.get("LONG", 0)),
        "short_candidates": int(side_counts.get("SHORT", 0)),
        "setup_counts": setup_counts,
        "candidates": rows,
        **(payload_extra or {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


_V11_WORKER_MARKET_CTX: dict | None = None


def _set_candidate_5m_dir(data_root: str | Path) -> Path:
    root = Path(data_root)
    candidate_scan.LIVE_5M_DIR = root
    candidate_scan.v2.DATA_ROOT_5M = root
    try:
        candidate_scan._MARKET_CTX_CACHE = {}
    except Exception:
        pass
    return root


def _v11_worker_init(data_root: str = "") -> None:
    global _V11_WORKER_MARKET_CTX
    if data_root:
        _set_candidate_5m_dir(data_root)
    if _V11_WORKER_MARKET_CTX is None:
        candidate_scan.v2._init_worker({
            "ENABLE_NOISY_ADVANCED_SHORTS": True,
            "ENABLE_NATIVE_V2_MINED_FILTER": False,
        })
        try:
            _V11_WORKER_MARKET_CTX = candidate_scan.v2._load_market_context()
        except Exception:
            _V11_WORKER_MARKET_CTX = {}


def _normalise_date_series(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize("Asia/Kolkata")
    return ts.dt.tz_convert("Asia/Kolkata")


def _scan_one_ticker_day_candidates(payload: tuple) -> list[dict]:
    ticker, day, start_min, end_min = payload[:4]
    include_ab_excluded = bool(payload[4]) if len(payload) >= 5 else False
    global _V11_WORKER_MARKET_CTX
    if _V11_WORKER_MARKET_CTX is None:
        _v11_worker_init()
    market_ctx = _V11_WORKER_MARKET_CTX or {}

    try:
        df = candidate_scan._load_live_5m(ticker)
        if df is None or df.empty:
            return []
        prepared = candidate_scan.v2._prepare_5m(df)
    except Exception:
        return []

    day_date = pd.to_datetime(day, errors="coerce")
    if pd.isna(day_date):
        return []
    if "date_only" not in prepared.columns:
        prepared["date_only"] = _normalise_date_series(prepared["date"]).dt.date

    day_df = prepared[prepared["date_only"] == day_date.date()].copy().reset_index(drop=True)
    if day_df.empty:
        return []
    try:
        day_df["date"] = _normalise_date_series(day_df["date"])
    except Exception:
        return []
    day_df = day_df.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if day_df.empty:
        return []

    minutes = day_df["date"].dt.hour * 60 + day_df["date"].dt.minute
    slot_df = day_df.loc[(minutes >= int(start_min)) & (minutes <= int(end_min))].copy()
    if slot_df.empty:
        return []
    slot_times = sorted({pd.Timestamp(ts).floor("min") for ts in slot_df["date"]})
    slot_set = set(slot_times)
    signal_map = {
        pd.Timestamp(row["date"]).floor("min"): row.to_dict()
        for _, row in slot_df.iterrows()
    }

    scan_df = candidate_scan._append_synthetic_successor(day_df, slot_times[-1])
    candidates: list = []
    try:
        candidates.extend(candidate_scan.v2._scan_day(scan_df, str(ticker).upper(), market_ctx) or [])
    except Exception:
        pass
    if candidate_scan.EARLY_MODE_ENABLE:
        for slot in slot_times:
            try:
                candidates.extend(candidate_scan._scan_early_slot_candidates(scan_df, str(ticker).upper(), slot, market_ctx) or [])
            except Exception:
                pass

    by_slot: dict[pd.Timestamp, list[tuple]] = {}
    for c in candidates:
        try:
            c_ts = _normalise_ts(c.signal_ts).floor("min")
        except Exception:
            continue
        if c_ts not in slot_set:
            continue
        setup = str(c.setup)
        if setup in candidate_scan.EXCLUDED_SETUPS:
            if not (include_ab_excluded and setup in AB_PROBATION_SETUPS):
                continue
        if candidate_scan.FILTER_TO_V8_EXIT_SETUPS and setup not in candidate_scan.ALLOWED_SETUPS:
            continue
        signal_row = signal_map.get(c_ts)
        if signal_row is None:
            continue
        by_slot.setdefault(c_ts, []).append((c, signal_row))

    frames: list[pd.DataFrame] = []
    for slot, rows in sorted(by_slot.items()):
        frame = candidate_scan.candidates_to_dataframe(rows, slot)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return []
    return pd.concat(frames, ignore_index=True).to_dict("records")


def _scan_historical_full_day_candidates(
    *,
    day: str,
    start_time: str,
    end_time: str,
    workers: int,
    snapshot_dir: Path,
    data_root: Path,
    include_ab_excluded: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    data_root = _set_candidate_5m_dir(data_root)
    slots = _slot_range_for_day(day, start_time, end_time)
    try:
        universe = candidate_scan.v2._load_universe()
    except Exception as exc:
        raise SystemExit(f"[v11 historical_full_day] universe load failed: {type(exc).__name__}: {exc}") from exc

    tickers = sorted({str(t).strip().upper() for t in universe if str(t).strip()})
    if not tickers:
        raise SystemExit("[v11 historical_full_day] universe is empty")

    worker_count = max(1, int(workers))
    print(
        f"[v11 historical_full_day] scanning {len(slots)} slots from {start_time} to {end_time} "
        f"for {day} across {len(tickers)} tickers with day-wide ticker workers={worker_count} "
        f"data_root={data_root} include_ab_excluded={bool(include_ab_excluded)}",
        flush=True,
    )
    t0 = time.time()
    start_min = _parse_hhmm(start_time)
    end_min = _parse_hhmm(end_time)
    payloads = [(ticker, day, start_min, end_min, bool(include_ab_excluded)) for ticker in tickers]
    rows: list[dict] = []
    if worker_count <= 1:
        _v11_worker_init(str(data_root))
        for i, payload in enumerate(payloads, 1):
            rows.extend(_scan_one_ticker_day_candidates(payload))
            if i % 250 == 0 or i == len(payloads):
                print(
                    f"  [v11 historical_full_day] tickers={i:,}/{len(payloads):,} raw_rows={len(rows):,} "
                    f"elapsed={time.time() - t0:.1f}s",
                    flush=True,
                )
    else:
        with ProcessPoolExecutor(max_workers=worker_count, initializer=_v11_worker_init, initargs=(str(data_root),)) as ex:
            for i, result in enumerate(ex.map(_scan_one_ticker_day_candidates, payloads, chunksize=8), 1):
                if result:
                    rows.extend(result)
                if i % 250 == 0 or i == len(payloads):
                    print(
                        f"  [v11 historical_full_day] tickers={i:,}/{len(payloads):,} raw_rows={len(rows):,} "
                        f"elapsed={time.time() - t0:.1f}s",
                        flush=True,
                    )

    raw = pd.DataFrame(rows)
    if not raw.empty:
        raw = candidate_scan._dedupe_candidate_frame(raw)
    day_key = "".join(str(day).split("-"))
    slot_rows: list[dict] = []
    for slot in slots:
        if raw.empty or "scan_slot_ist" not in raw.columns:
            df = pd.DataFrame()
        else:
            df = raw.loc[raw["scan_slot_ist"].astype(str).eq(_fmt_ist(slot))].copy()
        raw_path = snapshot_dir / f"raw_candidate_tickers_{day_key}_{slot.strftime('%H%M')}.json"
        _write_candidate_snapshot(
            df,
            slot,
            raw_path,
            output_label="raw_pre_gate",
            payload_extra={"candidate_source": "day_wide_candidate_scan_v11_live_parity", "candidate_5m_dir": str(data_root)},
        )
        long_count = int(0 if df.empty else (df["side"].astype(str).str.upper() == "LONG").sum())
        short_count = int(0 if df.empty else (df["side"].astype(str).str.upper() == "SHORT").sum())
        slot_rows.append({
            "slot_ist": _fmt_ist(slot),
            "raw_candidate_count": int(len(df)),
            "long_candidates": long_count,
            "short_candidates": short_count,
            "elapsed_sec": round(time.time() - t0, 3),
            "snapshot_path": str(raw_path),
        })

    print(
        f"[v11 historical_full_day] finished {day}: raw={len(raw)} elapsed={time.time() - t0:.1f}s",
        flush=True,
    )
    return raw.reset_index(drop=True), pd.DataFrame(slot_rows)


def _available_historical_dates(data_root: Path, start_date: str = "", end_date: str = "") -> list[str]:
    root = Path(data_root)
    files = sorted(root.glob("*_stocks_indicators_5min.parquet"))
    if not files:
        raise SystemExit(f"[v11 all_available] no 5-minute parquet files found under {root}")

    end_text = str(end_date or "").strip() or pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    start_text = str(start_date or "").strip()
    end_day = pd.to_datetime(end_text, errors="coerce")
    start_day = pd.to_datetime(start_text, errors="coerce") if start_text else pd.NaT
    if pd.isna(end_day):
        raise SystemExit(f"[v11 all_available] invalid --end_date={end_date!r}")
    if start_text and pd.isna(start_day):
        raise SystemExit(f"[v11 all_available] invalid --start_date={start_date!r}")

    seen: set[str] = set()
    for i, fp in enumerate(files, 1):
        try:
            dates = pd.read_parquet(fp, columns=["date"])["date"]
        except Exception:
            continue
        ts = pd.to_datetime(dates, errors="coerce")
        if ts.empty:
            continue
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize("Asia/Kolkata")
        else:
            ts = ts.dt.tz_convert("Asia/Kolkata")
        days = ts.dropna().dt.strftime("%Y-%m-%d").unique().tolist()
        seen.update(days)
        if i % 250 == 0:
            print(f"[v11 all_available] indexed dates from {i:,}/{len(files):,} files", flush=True)

    out = []
    end_key = end_day.strftime("%Y-%m-%d")
    start_key = start_day.strftime("%Y-%m-%d") if pd.notna(start_day) else ""
    for day in sorted(seen):
        if start_key and day < start_key:
            continue
        if day > end_key:
            continue
        out.append(day)
    return out


def _candidate_roots(primary: Path, fallback: Path | None = None) -> list[Path]:
    roots: list[Path] = []
    for root in (primary, fallback):
        if root is None:
            continue
        path = Path(root)
        if not path.exists():
            continue
        if any(path == existing for existing in roots):
            continue
        roots.append(path)
    return roots


def _available_historical_dates_with_roots(
    primary: Path,
    fallback: Path | None,
    start_date: str = "",
    end_date: str = "",
) -> tuple[list[str], dict[str, Path]]:
    date_to_root: dict[str, Path] = {}
    for root in _candidate_roots(primary, fallback):
        try:
            root_dates = _available_historical_dates(root, start_date, end_date)
        except SystemExit as exc:
            print(f"[v11 all_available] skipping candidate root {root}: {exc}", flush=True)
            continue
        for day in root_dates:
            date_to_root.setdefault(day, root)
    dates = sorted(date_to_root)
    return dates, date_to_root


_TIER123_WORKER_DATA_ROOT: Path | None = None
_TIER123_WORKER_MARKET_CTX: dict[str, dict[pd.Timestamp, dict]] = {}
_TIER123_WORKER_DATES: set[str] = set()


def _tier123_profile_enabled(profile: str | None) -> bool:
    return _normalise_selected_strategy_profile(profile) == TIER123_BALANCED_PROFILE


def _tier123_prepare_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ATR" not in out.columns or out["ATR"].isna().all() or "VWAP" not in out.columns or out["VWAP"].isna().all():
        return candidate_scan.v2._prepare_5m(out)
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


def _tier123_prev_day_levels(prepared: pd.DataFrame) -> pd.DataFrame:
    out = prepared.drop(columns=["prev_day_high", "prev_day_low", "prev_day_close_calc"], errors="ignore")
    daily = out.groupby("date_only", sort=True).agg(
        day_high=("high", "max"),
        day_low=("low", "min"),
        day_close=("close", "last"),
    )
    daily["prev_day_high"] = daily["day_high"].shift(1)
    daily["prev_day_low"] = daily["day_low"].shift(1)
    daily["prev_day_close_calc"] = daily["day_close"].shift(1)
    return out.join(daily[["prev_day_high", "prev_day_low", "prev_day_close_calc"]], on="date_only")


def _tier123_read_5m(ticker: str, data_root: Path, date_keys: set[str] | None = None) -> pd.DataFrame | None:
    path = Path(data_root) / f"{str(ticker).upper()}_stocks_indicators_5min.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df.empty or "date" not in df.columns:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df["date_only"] = df["date"].dt.strftime("%Y-%m-%d")
    if date_keys:
        df = df.loc[df["date_only"].isin(date_keys)].copy()
    if df.empty:
        return None
    return _tier123_prev_day_levels(_tier123_prepare_5m(df))


def _tier123_market_context(data_root: Path, date_keys: set[str]) -> dict[str, dict[pd.Timestamp, dict]]:
    for ticker in ["NIFTY", "NIFTY50", "NIFTY_50", "NIFTYBEES"]:
        df = _tier123_read_5m(ticker, data_root, date_keys)
        if df is None or df.empty:
            continue
        ctx: dict[str, dict[pd.Timestamp, dict]] = {}
        for day, g in df.groupby("date_only", sort=True):
            g = g.reset_index(drop=True)
            day_open = float(g["open"].iloc[0])
            by_ts: dict[pd.Timestamp, dict] = {}
            for _, row in g.iterrows():
                ts = _normalise_ts(row["date"])
                close = float(row["close"])
                ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
                vwap = float(row.get("VWAP", np.nan))
                adx = float(row.get("ADX", np.nan))
                if np.isfinite(vwap) and close > vwap and ret >= 0.20:
                    regime = "BULL"
                elif np.isfinite(vwap) and close < vwap and ret <= -0.20:
                    regime = "BEAR"
                elif np.isfinite(adx) and adx >= 25:
                    regime = "TREND"
                else:
                    regime = "NEUTRAL"
                by_ts[ts] = {"market_ret_pct": float(ret), "regime": regime}
            ctx[str(day)] = by_ts
        return ctx
    return {}


def _tier123_bar_context(ctx: dict[str, dict[pd.Timestamp, dict]], day: str, ts: pd.Timestamp) -> tuple[float, str]:
    by_ts = ctx.get(str(day), {})
    if not by_ts:
        return 0.0, "UNKNOWN"
    if ts in by_ts:
        rec = by_ts[ts]
        return float(rec["market_ret_pct"]), str(rec["regime"])
    keys = [key for key in by_ts if key <= ts]
    if not keys:
        return 0.0, "UNKNOWN"
    rec = by_ts[max(keys)]
    return float(rec["market_ret_pct"]), str(rec["regime"])


def _tier123_quality(side: str, setup: str, row: pd.Series, rs_pct: float, market_ret: float, regime: str) -> float:
    close_loc = float(row.get("close_loc", 0.5))
    vol_ratio = float(row.get("vol_ratio", 1.0))
    vwap_dist = float(row.get("vwap_dist_atr", 0.0))
    body_pct = float(row.get("body_pct", 0.0))
    adx = float(row.get("ADX", 0.0))
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
        "candidate_id": f"{ticker}|{side}|{setup}|{ts.isoformat()}",
        "scan_session": "v11_tier123_balanced_addon",
        "selection_mode": "tier123_balanced_addon_gate",
        "candidate_family": "Tier123 balanced add-on",
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
        "created_at_ist": _fmt_ist(pd.Timestamp.now(tz="Asia/Kolkata")),
    }


def _tier123_scan_ticker_dates(
    ticker: str,
    data_root: Path,
    date_keys: set[str],
    market_ctx: dict[str, dict[pd.Timestamp, dict]],
) -> list[dict]:
    df = _tier123_read_5m(ticker, data_root, date_keys)
    if df is None or df.empty:
        return []
    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        if str(day) not in date_keys:
            continue
        g = group.reset_index(drop=True)
        if len(g) < 15:
            continue
        day_open = float(g["open"].iloc[0])
        for i in range(4, len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = _normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if minute < 570 or minute > 900:
                continue
            close = float(row["close"])
            open_px = float(row["open"])
            high = float(row["high"])
            atr = float(row.get("ATR", np.nan))
            vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan))
            ema50 = float(row.get("EMA_50", np.nan))
            adx = float(row.get("ADX", np.nan))
            rsi = float(row.get("RSI", np.nan))
            stoch = float(row.get("Stoch_%K", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            close_loc = float(row.get("close_loc", np.nan))
            lower_wick = float(row.get("lower_wick_pct", np.nan))
            upper_wick = float(row.get("upper_wick_pct", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            rng = float(row.get("range", np.nan))
            if not np.isfinite(atr) or atr <= 0 or not np.isfinite(vol_ratio) or vol_ratio < 1.0:
                continue
            if close < 25 or float(row.get("day_value_so_far_rs", 0.0)) < 20_000_000:
                continue
            if np.isfinite(rng) and rng > 3.5 * atr:
                continue

            market_ret, regime = _tier123_bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            below_vwap = np.isfinite(vwap) and close < vwap
            above_vwap = np.isfinite(vwap) and close > vwap
            short_struct = close < open_px and close_loc <= 0.45

            ema20_slope = float(g["EMA_20"].iloc[i] - g["EMA_20"].iloc[max(0, i - 3)]) if np.isfinite(ema20) else np.nan
            if (
                np.isfinite(ema20)
                and np.isfinite(ema50)
                and np.isfinite(ema20_slope)
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
                    ticker,
                    "T_TREND_DAY_EMA_STAIR_SHORT",
                    "SHORT",
                    row,
                    rs_pct,
                    market_ret,
                    regime,
                    "tier123_late_bearish_trend_day_ema_stair_short",
                ))

            controlled = regime in {"NEUTRAL", "UNKNOWN"} or (np.isfinite(adx) and adx <= 22)
            if (
                minute >= 660
                and controlled
                and vwap_dist <= -2.0
                and lower_wick >= 0.35
                and close > float(prev["high"])
                and (not np.isfinite(rsi) or rsi <= 38)
                and (not np.isfinite(stoch) or stoch <= 35)
            ):
                rows.append(_tier123_candidate(
                    ticker,
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
                and close < float(prev["low"])
                and (not np.isfinite(rsi) or rsi >= 62)
                and (not np.isfinite(stoch) or stoch >= 65)
            ):
                candidate = _tier123_candidate(
                    ticker,
                    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT",
                    "SHORT",
                    row,
                    rs_pct,
                    market_ret,
                    regime,
                    "tier123_quiet_volume_quality_controlled_vwap_extreme_fade_short",
                )
                rows.append(candidate)
    return rows


def _tier123_probe_gate(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw
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


def _tier123_load_universe(data_root: Path) -> list[str]:
    try:
        from filtered_stocks_NSE_futures_only import selected_stocks as futures_stocks

        universe = sorted({str(x).upper().replace(".NS", "").strip() for x in futures_stocks if str(x).strip()})
    except Exception:
        try:
            universe = sorted({str(x).upper().replace(".NS", "").strip() for x in candidate_scan.v2._load_universe() if str(x).strip()})
        except Exception:
            universe = []
    return [
        ticker
        for ticker in universe
        if ticker
        and not ticker.startswith("NIFTY")
        and not ticker.endswith("BEES")
        and (Path(data_root) / f"{ticker}_stocks_indicators_5min.parquet").exists()
    ]


def _tier123_worker_init(data_root: str, dates: tuple[str, ...], market_ctx: dict[str, dict[pd.Timestamp, dict]]) -> None:
    global _TIER123_WORKER_DATA_ROOT, _TIER123_WORKER_DATES, _TIER123_WORKER_MARKET_CTX
    _TIER123_WORKER_DATA_ROOT = Path(data_root)
    _TIER123_WORKER_DATES = set(dates)
    _TIER123_WORKER_MARKET_CTX = market_ctx


def _tier123_scan_ticker_job(ticker: str) -> tuple[str, list[dict], str | None]:
    try:
        if _TIER123_WORKER_DATA_ROOT is None:
            return ticker, [], "tier123_worker_data_root_not_initialised"
        rows = _tier123_scan_ticker_dates(
            str(ticker).upper(),
            _TIER123_WORKER_DATA_ROOT,
            _TIER123_WORKER_DATES,
            _TIER123_WORKER_MARKET_CTX,
        )
        return ticker, rows, None
    except Exception as exc:
        return ticker, [], repr(exc)


def _scan_tier123_balanced_candidates_for_dates(
    dates: list[str],
    date_to_root: dict[str, Path],
    *,
    workers: int,
    out_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not dates:
        return pd.DataFrame(), pd.DataFrame()
    frames: list[pd.DataFrame] = []
    stats_rows: list[dict] = []
    worker_count = max(1, int(workers))
    by_root: dict[Path, list[str]] = {}
    for day in dates:
        root = Path(date_to_root.get(day, Path(DEFAULT_CANDIDATE_5M_DIR)))
        by_root.setdefault(root, []).append(day)

    for data_root, root_dates in sorted(by_root.items(), key=lambda item: str(item[0])):
        date_keys = set(root_dates)
        market_ctx = _tier123_market_context(data_root, date_keys)
        universe = _tier123_load_universe(data_root)
        rows: list[dict] = []
        errors: list[dict] = []
        t0 = time.time()
        print(
            f"[v11 tier123] scanning balanced add-on dates={len(root_dates):,} "
            f"tickers={len(universe):,} workers={worker_count} data_root={data_root}",
            flush=True,
        )
        if worker_count <= 1:
            _tier123_worker_init(str(data_root), tuple(sorted(date_keys)), market_ctx)
            for i, ticker in enumerate(universe, 1):
                _, ticker_rows, err = _tier123_scan_ticker_job(ticker)
                if err:
                    errors.append({"ticker": ticker, "error": err})
                rows.extend(ticker_rows)
                if i % 25 == 0 or i == len(universe):
                    print(
                        f"  [v11 tier123] tickers={i:,}/{len(universe):,} raw_rows={len(rows):,} "
                        f"elapsed={time.time() - t0:.1f}s",
                        flush=True,
                    )
        else:
            with ProcessPoolExecutor(
                max_workers=worker_count,
                initializer=_tier123_worker_init,
                initargs=(str(data_root), tuple(sorted(date_keys)), market_ctx),
            ) as ex:
                for i, (ticker, ticker_rows, err) in enumerate(ex.map(_tier123_scan_ticker_job, universe, chunksize=4), 1):
                    if err:
                        errors.append({"ticker": ticker, "error": err})
                    rows.extend(ticker_rows)
                    if i % 25 == 0 or i == len(universe):
                        print(
                            f"  [v11 tier123] tickers={i:,}/{len(universe):,} raw_rows={len(rows):,} "
                            f"elapsed={time.time() - t0:.1f}s",
                            flush=True,
                        )
        raw = pd.DataFrame(rows)
        gated = _tier123_probe_gate(raw) if not raw.empty else raw
        if not gated.empty:
            gated["v11_tier123_data_root"] = str(data_root)
            frames.append(gated)
        stats_rows.append({
            "stage": "tier123_balanced_candidate_scan",
            "candidate_5m_dir": str(data_root),
            "dates": len(root_dates),
            "first_date": min(root_dates),
            "last_date": max(root_dates),
            "tickers": len(universe),
            "raw_candidates": int(len(raw)),
            "gated_candidates": int(len(gated)),
            "scan_errors": int(len(errors)),
            "elapsed_sec": round(time.time() - t0, 3),
        })
        if errors:
            pd.DataFrame(errors).to_csv(out_dir / f"tier123_balanced_scan_errors_{abs(hash(str(data_root))) % 100000}.csv", index=False)

    candidates = _concat_frames(frames)
    if not candidates.empty:
        candidates = _tier123_probe_gate(candidates)
    return candidates.reset_index(drop=True), pd.DataFrame(stats_rows)


def _trade_key_frame(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype="object")
    return df["ticker"].astype(str).str.upper().str.strip() + "|" + df["trade_date"].astype(str).str[:10]


def _sort_resolved_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    out["_entry_sort"] = pd.to_datetime(out.get("entry_time_v6", out.get("old_entry_time_v7")), errors="coerce")
    if "trade_date" in out.columns:
        out["_trade_date_sort"] = out["trade_date"].astype(str)
    else:
        out["_trade_date_sort"] = ""
    return (
        out.sort_values(["_trade_date_sort", "_entry_sort", "ticker", "setup"], ascending=[True, True, True, True])
        .drop(columns=["_entry_sort", "_trade_date_sort"], errors="ignore")
        .reset_index(drop=True)
    )


def _apply_tier123_balanced_addon(
    *,
    base_final: pd.DataFrame,
    dates: list[str],
    date_to_root: dict[str, Path],
    args: argparse.Namespace,
    out_dir: Path,
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not _tier123_profile_enabled(str(args.selected_strategy_profile)):
        return base_final, pd.DataFrame()

    candidates, scan_stats = _scan_tier123_balanced_candidates_for_dates(
        dates,
        date_to_root,
        workers=int(args.workers),
        out_dir=out_dir,
    )
    candidates.to_csv(out_dir / f"{label}_tier123_balanced_raw_candidates.csv", index=False)
    scan_stats.to_csv(out_dir / f"{label}_tier123_balanced_scan_stats.csv", index=False)
    if candidates.empty:
        stats = pd.DataFrame([{
            "stage": f"{label}_tier123_balanced_addon",
            "tier123_balanced_raw_candidates": 0,
            "tier123_balanced_entry_signals": 0,
            "tier123_balanced_selected_signals": 0,
            "tier123_balanced_resolved_trades": 0,
            "tier123_balanced_non_overlap_trades": 0,
            "tier123_balanced_overlap_trades": 0,
        }])
        return base_final, stats

    selected, selected_rejects, selected_stats = _apply_selected_strategy_profile(
        candidates,
        str(args.selected_strategy_profile),
    )
    selected.to_csv(out_dir / f"{label}_tier123_balanced_selected_strategy_signals.csv", index=False)
    selected_rejects.to_csv(out_dir / f"{label}_tier123_balanced_selected_strategy_rejects.csv", index=False)

    entry_signals, entry_raw, entry_rejects = _build_v7_entry_engine_signals(selected)
    entry_raw.to_csv(out_dir / f"{label}_tier123_balanced_entry_engine_raw_entries.csv", index=False)
    entry_rejects.to_csv(out_dir / f"{label}_tier123_balanced_entry_engine_rejects.csv", index=False)
    entry_signals.to_csv(out_dir / f"{label}_tier123_balanced_entry_engine_signals.csv", index=False)

    addon_final = pd.DataFrame()
    if not entry_signals.empty:
        addon_final = _resolve_v7_entry_engine_signals(
            entry_signals,
            label=f"{label}_tier123_balanced_addon",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )
    addon_final.to_csv(out_dir / f"{label}_tier123_balanced_resolved_trades.csv", index=False)

    if addon_final.empty:
        stats = pd.DataFrame([{
            "stage": f"{label}_tier123_balanced_addon",
            "tier123_balanced_raw_candidates": int(len(candidates)),
            "tier123_balanced_entry_signals": int(len(entry_signals)),
            **selected_stats,
            "tier123_balanced_resolved_trades": 0,
            "tier123_balanced_non_overlap_trades": 0,
            "tier123_balanced_overlap_trades": 0,
        }])
        return base_final, stats

    base_keys = set(_trade_key_frame(base_final))
    addon_keys = _trade_key_frame(addon_final)
    non_overlap = addon_final.loc[~addon_keys.isin(base_keys)].copy()
    overlap = addon_final.loc[addon_keys.isin(base_keys)].copy()
    non_overlap.to_csv(out_dir / f"{label}_tier123_balanced_non_overlap_trades.csv", index=False)
    overlap.to_csv(out_dir / f"{label}_tier123_balanced_overlap_rejected_trades.csv", index=False)
    combined = _sort_resolved_trades(_concat_frames([base_final, non_overlap]))
    stats = pd.DataFrame([{
        "stage": f"{label}_tier123_balanced_addon",
        "tier123_balanced_raw_candidates": int(len(candidates)),
        "tier123_balanced_entry_raw_entries": int(len(entry_raw)),
        "tier123_balanced_entry_rejects": int(len(entry_rejects)),
        "tier123_balanced_entry_signals": int(len(entry_signals)),
        **selected_stats,
        "tier123_balanced_resolved_trades": int(len(addon_final)),
        "tier123_balanced_non_overlap_trades": int(len(non_overlap)),
        "tier123_balanced_overlap_trades": int(len(overlap)),
        "tier123_balanced_combined_trades": int(len(combined)),
    }])
    return combined, stats


def _candidate_time(row: pd.Series, *columns: str) -> pd.Timestamp:
    for col in columns:
        if col in row.index and pd.notna(row.get(col)):
            ts = _normalise_ts(row.get(col))
            if pd.notna(ts):
                return ts
    return pd.NaT


def _candidate_sort_frame(candidates: pd.DataFrame, date_hint: str = "") -> pd.DataFrame:
    out = candidates.copy()
    if out.empty:
        return out
    out["_v11_input_order"] = np.arange(len(out))
    out["_v11_slot_ts"] = [
        _candidate_time(row, "scan_slot_ist", "signal_time_ist", "bar_time_ist")
        for _, row in out.iterrows()
    ]
    out["_v11_signal_ts"] = [
        _candidate_time(row, "signal_time_ist", "signal_ts", "bar_time_ist", "scan_slot_ist")
        for _, row in out.iterrows()
    ]
    hint_day = str(date_hint or "").strip()
    out["_v11_day"] = [
        (ts.strftime("%Y-%m-%d") if pd.notna(ts) else hint_day)
        for ts in out["_v11_signal_ts"]
    ]
    out["_v11_slot_key"] = [
        _fmt_ist(ts) if pd.notna(ts) else ""
        for ts in out["_v11_slot_ts"]
    ]
    return out


def _drop_v11_temp_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    return df.drop(columns=[c for c in df.columns if str(c).startswith("_v11_")], errors="ignore")


def _ensure_candidate_id(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty or "candidate_id" in out.columns:
        return out
    for col in ("ticker", "side", "setup", "signal_time_ist"):
        if col not in out.columns:
            out[col] = ""
    out["candidate_id"] = (
        out["ticker"].astype(str).str.upper().str.strip()
        + "|"
        + out["side"].astype(str).str.upper().str.strip()
        + "|"
        + out["setup"].astype(str).str.strip()
        + "|"
        + out["signal_time_ist"].astype(str)
    )
    return out


def _tag_pipeline_frame(df: pd.DataFrame, *, day: str, slot_key: str, stage: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    out["v11_pipeline_day"] = day
    out["v11_pipeline_slot_ist"] = slot_key
    out["v11_pipeline_stage"] = stage
    return out


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _live_like_daily_dedupe(candidates: pd.DataFrame, date_hint: str = "") -> tuple[pd.DataFrame, dict]:
    if candidates is None or candidates.empty:
        return pd.DataFrame() if candidates is None else candidates.copy(), {"live_like_written": 0, "live_like_duplicates": 0}

    work = _ensure_candidate_id(candidates)
    if "_v11_day" not in work.columns or "_v11_live_order" not in work.columns:
        work = _candidate_sort_frame(work, date_hint)
        work["_v11_live_order"] = np.arange(len(work))

    rows: list[dict] = []
    duplicates = 0
    for _, day_df in work.sort_values(["_v11_day", "_v11_live_order"]).groupby("_v11_day", sort=True, dropna=False):
        existing_ids: set[str] = set()
        existing_tickers: set[str] = set()
        for _, row in day_df.iterrows():
            cid = str(row.get("candidate_id", ""))
            ticker = str(row.get("ticker", "")).upper().strip()
            if not cid or not ticker or cid in existing_ids or ticker in existing_tickers:
                duplicates += 1
                continue
            item = row.to_dict()
            rows.append(item)
            existing_ids.add(cid)
            existing_tickers.add(ticker)

    out = pd.DataFrame(rows)
    return _drop_v11_temp_cols(out), {
        "live_like_written": int(len(out)),
        "live_like_duplicates": int(duplicates),
    }


def _apply_v7_live_strategy(
    raw_candidates: pd.DataFrame,
    date_hint: str = "",
    *,
    ab_gate_profile: str = "off",
    ab_gate_min_quality: float = 250.0,
    ab_gate_max_per_side: int = 1,
    ab_gate_max_per_slot: int = 2,
    ab_gate_setups: tuple[str, ...] | None = None,
) -> dict:
    ab_gate_profile = _normalise_ab_gate_profile(ab_gate_profile)
    ab_gate_setups = tuple(ab_gate_setups or AB_PROBATION_SETUPS)
    if raw_candidates is None or raw_candidates.empty:
        empty_stats = {
            "slots_processed": 0,
            "raw_candidates": 0,
            "ranked_raw_candidates": 0,
            "v8_gated_candidates": 0,
            "regular_v8_gated_candidates": 0,
            "research_rejected_candidates": 0,
            "pre_dedupe_live_candidates": 0,
            "live_like_candidates": 0,
            "live_like_duplicates": 0,
            **_ab_gate_base_stats(
                profile=ab_gate_profile,
                min_quality=ab_gate_min_quality,
                max_per_side=ab_gate_max_per_side,
                max_per_slot=ab_gate_max_per_slot,
                allowed_setups=ab_gate_setups,
            ),
        }
        return {
            "ranked_raw_candidates": pd.DataFrame(),
            "v8_gated_candidates": pd.DataFrame(),
            "research_rejected_candidates": pd.DataFrame(),
            "pre_dedupe_live_candidates": pd.DataFrame(),
            "live_like_candidates": pd.DataFrame(),
            "slot_audit": pd.DataFrame(),
            "stats": empty_stats,
        }

    work = _candidate_sort_frame(raw_candidates, date_hint)
    work = work.sort_values(["_v11_day", "_v11_slot_ts", "_v11_input_order"]).reset_index(drop=True)

    ranked_frames: list[pd.DataFrame] = []
    gated_frames: list[pd.DataFrame] = []
    rejected_frames: list[pd.DataFrame] = []
    accepted_frames: list[pd.DataFrame] = []
    slot_rows: list[dict] = []
    live_order = 0

    for day, day_df in work.groupby("_v11_day", sort=True, dropna=False):
        day_text = str(day or date_hint or "").strip()
        for slot_key, slot_df in day_df.groupby("_v11_slot_key", sort=True, dropna=False):
            slot_key_text = str(slot_key or "")
            slot_t0 = time.time()
            raw_slot = _drop_v11_temp_cols(slot_df)

            ranked = live_discovery.add_live_ranker_scores(raw_slot, day_text)
            gated, gate_stats = live_discovery.apply_v8_live_gate(ranked)
            ab_accepted, ab_stats = _apply_ab_probation_gate(
                ranked,
                profile=ab_gate_profile,
                min_quality=ab_gate_min_quality,
                max_per_side=ab_gate_max_per_side,
                max_per_slot=ab_gate_max_per_slot,
                allowed_setups=ab_gate_setups,
            )
            accepted, rejected, research_stats = live_discovery.apply_research_live_filters(gated, day_text)
            gated_output = _concat_frames([gated, ab_accepted])
            if not gated_output.empty:
                gated_output = _ensure_candidate_id(gated_output).drop_duplicates(subset=["candidate_id"], keep="first").reset_index(drop=True)
            accepted_combined = _concat_frames([accepted, ab_accepted])
            if not accepted_combined.empty:
                accepted_combined = (
                    _ensure_candidate_id(accepted_combined)
                    .drop_duplicates(subset=["candidate_id"], keep="first")
                    .reset_index(drop=True)
                )

            if not ranked.empty:
                ranked_frames.append(_tag_pipeline_frame(ranked, day=day_text, slot_key=slot_key_text, stage="ranked_raw"))
            if not gated_output.empty:
                gated_frames.append(_tag_pipeline_frame(gated_output, day=day_text, slot_key=slot_key_text, stage="v8_or_ab_live_gate"))
            if not rejected.empty:
                rejected_frames.append(_tag_pipeline_frame(rejected, day=day_text, slot_key=slot_key_text, stage="research_live_filter_rejected"))
            if not accepted_combined.empty:
                accepted_combined = accepted_combined.copy()
                accepted_combined["_v11_day"] = day_text
                accepted_combined["_v11_live_order"] = np.arange(live_order, live_order + len(accepted_combined))
                live_order += len(accepted_combined)
                accepted_frames.append(_tag_pipeline_frame(accepted_combined, day=day_text, slot_key=slot_key_text, stage="live_accepted_pre_dedupe"))

            slot_rows.append({
                "day": day_text,
                "slot_ist": slot_key_text,
                "raw_candidates": int(len(raw_slot)),
                "ranked_raw_candidates": int(len(ranked)),
                "v8_gated_candidates": int(len(gated_output)),
                "regular_v8_gated_candidates": int(len(gated)),
                "research_rejected_candidates": int(len(rejected)),
                "pre_dedupe_live_candidates": int(len(accepted_combined)),
                "elapsed_sec": round(time.time() - slot_t0, 3),
                **gate_stats,
                **ab_stats,
                **research_stats,
            })

    ranked_raw = _concat_frames(ranked_frames)
    v8_gated = _concat_frames(gated_frames)
    research_rejected = _concat_frames(rejected_frames)
    pre_dedupe = _concat_frames(accepted_frames)
    live_like, dedupe_stats = _live_like_daily_dedupe(pre_dedupe, date_hint)
    slot_audit = pd.DataFrame(slot_rows)
    stats = {
        "slots_processed": int(len(slot_audit)),
        "raw_candidates": int(len(raw_candidates)),
        "ranked_raw_candidates": int(len(ranked_raw)),
        "v8_gated_candidates": int(len(v8_gated)),
        "regular_v8_gated_candidates": int(slot_audit["regular_v8_gated_candidates"].sum()) if "regular_v8_gated_candidates" in slot_audit.columns else 0,
        "research_rejected_candidates": int(len(research_rejected)),
        "pre_dedupe_live_candidates": int(len(pre_dedupe)),
        "live_like_candidates": int(len(live_like)),
        "live_like_duplicates": int(dedupe_stats.get("live_like_duplicates", 0)),
        **_ab_gate_base_stats(
            profile=ab_gate_profile,
            min_quality=ab_gate_min_quality,
            max_per_side=ab_gate_max_per_side,
            max_per_slot=ab_gate_max_per_slot,
            allowed_setups=ab_gate_setups,
        ),
    }
    for col in ("ab_gate_candidates", "ab_gate_quality_passed", "ab_gate_quality_rejected", "ab_gate_accepted"):
        if col in slot_audit.columns:
            stats[col] = int(pd.to_numeric(slot_audit[col], errors="coerce").fillna(0).sum())
    return {
        "ranked_raw_candidates": _drop_v11_temp_cols(ranked_raw),
        "v8_gated_candidates": _drop_v11_temp_cols(v8_gated),
        "research_rejected_candidates": _drop_v11_temp_cols(research_rejected),
        "pre_dedupe_live_candidates": _drop_v11_temp_cols(pre_dedupe),
        "live_like_candidates": live_like,
        "slot_audit": slot_audit,
        "stats": stats,
    }


def _normalise_live_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return pd.DataFrame()
    out = candidates.copy()
    required = {"ticker", "side", "setup", "signal_time_ist"}
    if not required.issubset(out.columns):
        missing = sorted(required - set(out.columns))
        raise SystemExit(f"[v11 live_parity] missing candidate columns: {missing}")
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    out["setup"] = out["setup"].astype(str).str.strip()
    out["signal_time_ist"] = pd.to_datetime(out["signal_time_ist"], errors="coerce")
    if getattr(out["signal_time_ist"].dt, "tz", None) is None:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_localize("Asia/Kolkata")
    else:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_convert("Asia/Kolkata")
    out = out.dropna(subset=["ticker", "side", "setup", "signal_time_ist"]).copy()
    out = out.loc[~out["setup"].isin(EXCLUDED_SETUPS)].copy()
    out = out.loc[out["setup"].isin(v6.SETUP_EXIT_RULES)].copy()
    out["signal_time_ist"] = out["signal_time_ist"].map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S%z"))
    out["signal_time_ist"] = out["signal_time_ist"].str.replace(r"(\+\d{2})(\d{2})$", r"\1:\2", regex=True)
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    if "candidate_id" not in out.columns:
        out["candidate_id"] = (
            out["ticker"].astype(str)
            + "|"
            + out["side"].astype(str)
            + "|"
            + out["setup"].astype(str)
            + "|"
            + out["signal_time_ist"].astype(str)
        )
    out = (
        out.sort_values(["signal_time_ist", "quality_score", "ticker", "setup"], ascending=[True, False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )
    return out


def _resolve_live_parity_candidates(candidates: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = _normalise_live_candidates(candidates)
    print(f"[v11 {label}] resolving {len(normalised):,} live-like candidates")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 250 == 0 or i == len(normalised):
            print(f"  [v11 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")
    if not rows:
        detail = _format_1m_miss_diagnostics(normalised)
        raise SystemExit(f"[v11 {label}] no trades resolved; {detail}")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["_entry_sort"] = pd.to_datetime(out["entry_time_v6"], errors="coerce")
    out["_score_sort"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    out = (
        out.sort_values(["trade_date", "_entry_sort", "_score_sort", "ticker"], ascending=[True, True, False, True])
        .drop_duplicates(subset=["trade_date", "ticker"], keep="first")
        .drop(columns=["_entry_sort", "_score_sort"], errors="ignore")
        .reset_index(drop=True)
    )
    return out


def _load_v7_live_paper_trades(live_paper_dir: Path, start_date: str, end_date: str) -> pd.DataFrame:
    start = pd.to_datetime(start_date, errors="coerce")
    end = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        raise SystemExit("[v11 v7_live_paper_replay] --start_date and --end_date are required as YYYY-MM-DD")
    rows: list[dict] = []
    for path in sorted(live_paper_dir.glob("paper_trades_*_id_5min_v7.csv")):
        name = path.name
        if not name.startswith("paper_trades_") or not name.endswith("_id_5min_v7.csv"):
            continue
        day_text = name.replace("paper_trades_", "").replace("_id_5min_v7.csv", "")
        day = pd.to_datetime(day_text, errors="coerce")
        if pd.isna(day) or day < start or day > end:
            continue
        frame = pd.read_csv(path)
        if frame.empty or "ticker" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            ticker = str(row.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            side = str(row.get("side", "")).strip().upper()
            setup = str(row.get("setup", "")).strip()
            entry_ts = _normalise_ts(row.get("entry_time"))
            signal_ts = _normalise_ts(
                row.get("signal_datetime", row.get("signal_entry_datetime_ist", pd.NaT))
            )
            if pd.isna(entry_ts) or side not in {"LONG", "SHORT"} or not setup:
                continue
            entry_price = _safe_float(row.get("entry_price"))
            stop_price = _safe_float(row.get("stop_price"))
            target_price = _safe_float(row.get("target_price"))
            quantity = _safe_int(row.get("quantity"), 0)
            if entry_price <= 0 or stop_price <= 0 or target_price <= 0 or quantity <= 0:
                continue
            if side == "LONG":
                sl_pct = (entry_price - stop_price) / entry_price * 100.0
                target_pct = (target_price - entry_price) / entry_price * 100.0
            else:
                sl_pct = (stop_price - entry_price) / entry_price * 100.0
                target_pct = (entry_price - target_price) / entry_price * 100.0
            rows.append({
                "candidate_id": (
                    f"{ticker}|{side}|{setup}|"
                    f"{signal_ts if pd.notna(signal_ts) else entry_ts}|{row.get('signal_id', '')}"
                ),
                "scan_session": "v7_live_paper_replay",
                "selection_mode": "actual_v7_live_paper_trade",
                "candidate_family": "V7_LIVE_PAPER_EXECUTED",
                "signal_time_ist": signal_ts,
                "signal_time_v8": signal_ts,
                "signal_datetime": row.get("signal_datetime", ""),
                "signal_entry_datetime_ist": row.get("signal_entry_datetime_ist", row.get("signal_datetime", "")),
                "old_entry_time_v7": entry_ts,
                "old_entry_price_v7": entry_price,
                "entry_time_v6": entry_ts,
                "entry_price_v6": entry_price,
                "trade_date": day.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "side": side,
                "setup": setup,
                "quantity": quantity,
                "live_trade_id": row.get("trade_id", ""),
                "live_signal_id": row.get("signal_id", ""),
                "live_exit_time": _normalise_ts(row.get("exit_time")),
                "live_exit_price": _safe_float(row.get("exit_price")),
                "live_outcome": str(row.get("outcome", "")).strip().upper(),
                "live_pnl_rs": _safe_float(row.get("pnl_rs")),
                "quality_score": _safe_float(row.get("quality_score"), 0.0),
                "v7_live_stop_price": stop_price,
                "v7_live_target_price": target_price,
                "v7_live_sl_pct": sl_pct,
                "v7_live_target_pct": target_pct,
                "v8_entry_model": "ACTUAL_V7_LIVE_PAPER_ENTRY",
                "v8_entry_delay_minutes": (
                    float((entry_ts - signal_ts).total_seconds() / 60.0) if pd.notna(signal_ts) else np.nan
                ),
            })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    return out.sort_values(["trade_date", "entry_time_v6", "ticker"]).reset_index(drop=True)


def _resolve_v7_live_paper_trade(row: pd.Series) -> dict | None:
    bars = _load_1m_with_open(str(row["ticker"]))
    if bars is None or bars.empty:
        return None
    sl_pct = float(row["v7_live_sl_pct"])
    target_pct = float(row["v7_live_target_pct"])
    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=float(row["entry_price_v6"]),
        entry_time_ist=row["entry_time_v6"],
        sl_pct=sl_pct,
        tgt_pct=target_pct,
    )
    if res is None:
        return None

    quantity = int(row["quantity"])
    entry_price = float(row["entry_price_v6"])
    exit_price = float(res.exit_price)
    if str(row["side"]).upper() == "SHORT":
        pnl_rs = (entry_price - exit_price) * quantity
    else:
        pnl_rs = (exit_price - entry_price) * quantity

    rec = row.to_dict()
    rec.update({
        "v6_sl_pct": float(sl_pct),
        "v6_target_pct": float(target_pct),
        "v6_outcome": res.outcome,
        "v6_exit_price": exit_price,
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(pnl_rs),
        "v6_cost_rs": 0.0,
        "v6_net_pnl_rs": float(pnl_rs),
        "capital_per_trade_rs": np.nan,
        "leverage": np.nan,
        "notional_exposure_rs": float(entry_price * quantity),
        "v7_resolution_source": "v7_live_paper_replay",
        "v8_resolution_source": "v7_live_paper_replay",
    })
    return rec


def _run_v7_live_paper_replay(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    live_paper_dir = Path(args.live_paper_dir)
    start_date = str(args.start_date or args.live_date or "").strip()
    end_date = str(args.end_date or args.live_date or start_date).strip()
    if not start_date or not end_date:
        raise SystemExit("[v11 v7_live_paper_replay] provide --start_date and --end_date")

    source_trades = _load_v7_live_paper_trades(live_paper_dir, start_date, end_date)
    source_trades.to_csv(out_dir / "v7_live_paper_replay_source_trades.csv", index=False)
    accepted = pd.DataFrame([{
        "stage": "v7_live_paper_replay",
        "source": str(live_paper_dir),
        "start_date": start_date,
        "end_date": end_date,
        "source_trades": int(len(source_trades)),
    }])

    if source_trades.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(live_paper_dir),
            accepted_filename="v7_live_paper_replay_rules_used.csv",
            reason="[v11 v7_live_paper_replay] no v7 live paper trades found for requested range",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=v7_live_paper_replay\nlive_paper_dir={live_paper_dir}\n"
            f"start_date={start_date}\nend_date={end_date}\n",
            encoding="utf-8",
        )
        return 0

    rows = []
    misses = 0
    print(f"[v11 v7_live_paper_replay] resolving {len(source_trades):,} actual v7 live paper trades")
    for i, (_, row) in enumerate(source_trades.iterrows(), 1):
        rec = _resolve_v7_live_paper_trade(row)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 100 == 0 or i == len(source_trades):
            print(f"  [v11 v7_live_paper_replay {i:4d}/{len(source_trades)}] resolved={len(rows)} misses={misses}")
    if not rows:
        raise SystemExit("[v11 v7_live_paper_replay] no trades resolved from 1-minute data")

    final = pd.DataFrame(rows).sort_values(["trade_date", "entry_time_v6", "ticker"]).reset_index(drop=True)
    accepted.loc[0, "resolved_trades"] = int(len(final))
    accepted.loc[0, "misses"] = int(misses)
    _write_outputs(
        out_dir,
        final,
        accepted,
        str(live_paper_dir),
        accepted_filename="v7_live_paper_replay_rules_used.csv",
        strategy_description="actual v7 live paper trades re-resolved with 1-minute exits",
    )
    (out_dir / "inputs.txt").write_text(
        f"mode=v7_live_paper_replay\nlive_paper_dir={live_paper_dir}\n"
        f"start_date={start_date}\nend_date={end_date}\n"
        f"data_1m_dir={v6.DATA_1M_DIR}\n"
        "entry_model=ACTUAL_V7_LIVE_PAPER_ENTRY\n"
        "exit_model=v7 live stop/target from paper trade rows, resolved on 1-minute OHLC to 15:20\n"
        "pnl_model=actual live quantity, price-only, no backtest costs\n",
        encoding="utf-8",
    )
    print(f"[v11 v7_live_paper_replay] wrote {out_dir}")
    return 0


def _format_1m_miss_diagnostics(candidates: pd.DataFrame) -> str:
    if candidates is None or candidates.empty:
        return "no normalised candidates remained after v11 live-strategy filtering"

    signal_times = pd.to_datetime(candidates.get("signal_time_ist"), errors="coerce")
    if getattr(signal_times.dt, "tz", None) is None:
        signal_times = signal_times.dt.tz_localize("Asia/Kolkata")
    else:
        signal_times = signal_times.dt.tz_convert("Asia/Kolkata")
    valid_signals = signal_times.dropna()
    min_signal = valid_signals.min() if not valid_signals.empty else pd.NaT
    max_signal = valid_signals.max() if not valid_signals.empty else pd.NaT

    missing_files = 0
    stale_before_signal = 0
    latest_seen = pd.NaT
    examples: list[str] = []
    for _, row in candidates.iterrows():
        ticker = str(row.get("ticker", "")).upper().strip()
        sig = _normalise_ts(row.get("signal_time_ist"))
        bars = _load_1m_with_open(ticker)
        if bars is None or bars.empty:
            missing_files += 1
            if len(examples) < 3:
                examples.append(f"{ticker}: no 1m file")
            continue
        latest = pd.Timestamp(bars.index.max())
        latest_seen = latest if pd.isna(latest_seen) else max(latest_seen, latest)
        if pd.notna(sig) and latest < sig:
            stale_before_signal += 1
            if len(examples) < 3:
                examples.append(f"{ticker}: latest 1m {_fmt_ist(latest)} before signal {_fmt_ist(sig)}")

    parts = [
        f"candidates={len(candidates)}",
        f"missing_1m_files={missing_files}",
        f"stale_before_signal={stale_before_signal}",
    ]
    if pd.notna(min_signal) and pd.notna(max_signal):
        parts.append(f"signal_window={_fmt_ist(min_signal)}..{_fmt_ist(max_signal)}")
    if pd.notna(latest_seen):
        parts.append(f"latest_1m_seen={_fmt_ist(latest_seen)}")
    parts.append(f"data_1m_dir={v6.DATA_1M_DIR}")
    if examples:
        parts.append("examples=[" + "; ".join(examples) + "]")
    return ", ".join(parts)


def _run_live_parity(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.live_candidate_json_dir)
    raw_candidates = _load_live_candidate_snapshots(source_dir, str(args.live_date or ""))
    if raw_candidates.empty:
        raise SystemExit(f"[v11 live_parity] no live candidate snapshots found in {source_dir} for date={args.live_date!r}")

    pipeline = _apply_v7_live_strategy(
        raw_candidates,
        str(args.live_date or ""),
        ab_gate_profile=str(args.ab_gate_profile),
        ab_gate_min_quality=float(args.ab_gate_min_quality),
        ab_gate_max_per_side=int(args.ab_gate_max_per_side),
        ab_gate_max_per_slot=int(args.ab_gate_max_per_slot),
        ab_gate_setups=_ab_gate_setups_for_selected_profile(str(args.selected_strategy_profile)),
    )
    live_like_candidates = pipeline["live_like_candidates"]
    raw_candidates.to_csv(out_dir / "live_parity_raw_candidates.csv", index=False)
    pipeline["ranked_raw_candidates"].to_csv(out_dir / "live_parity_ranked_raw_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "live_parity_v8_gated_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "live_parity_gated_candidates.csv", index=False)
    pipeline["research_rejected_candidates"].to_csv(out_dir / "live_parity_research_rejected_candidates.csv", index=False)
    pipeline["pre_dedupe_live_candidates"].to_csv(out_dir / "live_parity_pre_dedupe_live_candidates.csv", index=False)
    live_like_candidates.to_csv(out_dir / "live_parity_live_like_candidates.csv", index=False)
    pipeline["slot_audit"].to_csv(out_dir / "live_parity_live_pipeline_slot_audit.csv", index=False)
    pd.DataFrame([pipeline["stats"]]).to_csv(out_dir / "live_parity_pipeline_stats.csv", index=False)

    accepted = pd.DataFrame([{
        "stage": "live_parity",
        "source": str(source_dir),
        "date": str(args.live_date or ""),
        **pipeline["stats"],
    }])
    if live_like_candidates.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(source_dir),
            accepted_filename="live_parity_rules_used.csv",
            reason="[v11 live_parity] no candidates survived the exact v7 live pipeline",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=live_parity\nlive_candidate_json_dir={source_dir}\nlive_date={args.live_date}\n"
            f"ab_gate_profile={args.ab_gate_profile}\n"
            f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
            f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
            f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
            "entry_model=NEXT_1MIN_OPEN_AFTER_5MIN_SIGNAL\n"
            "candidate_source=signal_discovery_v7_5mins_ID snapshots\n"
            "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
            "dedupe=one ticker per trade_date, matching live entry-engine intraday ticker guard\n",
            encoding="utf-8",
        )
        print(f"[v11 live_parity] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = _resolve_live_parity_candidates(live_like_candidates, float(args.cost_bps), "live_parity")
    _write_outputs(out_dir, final, accepted, str(source_dir), accepted_filename="live_parity_rules_used.csv")
    (out_dir / "inputs.txt").write_text(
        f"mode=live_parity\nlive_candidate_json_dir={source_dir}\nlive_date={args.live_date}\n"
        f"ab_gate_profile={args.ab_gate_profile}\n"
        f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
        f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
        f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
        "entry_model=NEXT_1MIN_OPEN_AFTER_5MIN_SIGNAL\n"
        "candidate_source=signal_discovery_v7_5mins_ID snapshots\n"
        "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
        "dedupe=one ticker per trade_date, matching live entry-engine intraday ticker guard\n",
        encoding="utf-8",
    )
    print(f"[v11 live_parity] wrote {out_dir}")
    return 0


def _run_historical_full_day(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    historical_date = str(args.historical_date or args.live_date or "").strip()
    snapshot_dir = out_dir / "generated_candidate_snapshots"
    primary_root = Path(args.candidate_5m_dir)
    fallback_root = Path(args.fallback_candidate_5m_dir) if str(args.fallback_candidate_5m_dir or "").strip() else None
    _, one_day_roots = _available_historical_dates_with_roots(
        primary_root,
        fallback_root,
        historical_date,
        historical_date,
    )
    data_root = one_day_roots.get(historical_date, primary_root)

    raw_candidates, slot_summary = _scan_historical_full_day_candidates(
        day=historical_date,
        start_time=str(args.start_time),
        end_time=str(args.end_time),
        workers=int(args.workers),
        snapshot_dir=snapshot_dir,
        data_root=data_root,
        include_ab_excluded=_ab_gate_enabled(str(args.ab_gate_profile)),
    )
    slot_summary.to_csv(out_dir / "historical_full_day_slot_summary.csv", index=False)
    raw_candidates.to_csv(out_dir / "historical_full_day_raw_candidates.csv", index=False)

    pipeline = _apply_v7_live_strategy(
        raw_candidates,
        historical_date,
        ab_gate_profile=str(args.ab_gate_profile),
        ab_gate_min_quality=float(args.ab_gate_min_quality),
        ab_gate_max_per_side=int(args.ab_gate_max_per_side),
        ab_gate_max_per_slot=int(args.ab_gate_max_per_slot),
        ab_gate_setups=_ab_gate_setups_for_selected_profile(str(args.selected_strategy_profile)),
    )
    live_like_candidates = pipeline["live_like_candidates"]
    entry_engine_signals, entry_engine_raw, entry_engine_rejects = _build_v7_entry_engine_signals(
        pipeline["pre_dedupe_live_candidates"]
    )
    selected_strategy_signals, selected_strategy_rejects, selected_strategy_stats = _apply_selected_strategy_profile(
        entry_engine_signals,
        str(args.selected_strategy_profile),
    )
    pipeline["ranked_raw_candidates"].to_csv(out_dir / "historical_full_day_ranked_raw_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "historical_full_day_v8_gated_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "historical_full_day_gated_candidates.csv", index=False)
    pipeline["research_rejected_candidates"].to_csv(out_dir / "historical_full_day_research_rejected_candidates.csv", index=False)
    pipeline["pre_dedupe_live_candidates"].to_csv(out_dir / "historical_full_day_pre_dedupe_live_candidates.csv", index=False)
    live_like_candidates.to_csv(out_dir / "historical_full_day_live_like_candidates.csv", index=False)
    entry_engine_raw.to_csv(out_dir / "historical_full_day_entry_engine_raw_entries.csv", index=False)
    entry_engine_rejects.to_csv(out_dir / "historical_full_day_entry_engine_rejects.csv", index=False)
    entry_engine_signals.to_csv(out_dir / "historical_full_day_entry_engine_signals.csv", index=False)
    selected_strategy_signals.to_csv(out_dir / "historical_full_day_selected_strategy_signals.csv", index=False)
    selected_strategy_rejects.to_csv(out_dir / "historical_full_day_selected_strategy_rejects.csv", index=False)
    pipeline["slot_audit"].to_csv(out_dir / "historical_full_day_live_pipeline_slot_audit.csv", index=False)
    full_day_stats = {
        **pipeline["stats"],
        "entry_engine_raw_entries": int(len(entry_engine_raw)),
        "entry_engine_rejects": int(len(entry_engine_rejects)),
        "entry_engine_signals": int(len(entry_engine_signals)),
        **selected_strategy_stats,
    }
    pd.DataFrame([full_day_stats]).to_csv(out_dir / "historical_full_day_pipeline_stats.csv", index=False)

    if not live_like_candidates.empty and "signal_time_ist" in live_like_candidates.columns:
        slot_keys = pd.to_datetime(live_like_candidates["signal_time_ist"], errors="coerce").dt.strftime("%H%M")
        for slot_key, group in live_like_candidates.groupby(slot_keys):
            if not str(slot_key) or str(slot_key) == "NaT":
                continue
            slot = _normalise_ts(group["signal_time_ist"].iloc[0])
            _write_candidate_snapshot(
                group,
                slot,
                snapshot_dir / f"gated_candidate_tickers_{''.join(historical_date.split('-'))}_{slot_key}.json",
                output_label="live_like_for_entry_engine",
                payload_extra={"candidate_source": "historical_full_day_raw_candidates", **pipeline["stats"]},
            )

    accepted = pd.DataFrame([{
        "stage": "historical_full_day",
        "source": "candidate_scan.scan_slot_candidates",
        "date": historical_date,
        "start_time": str(args.start_time),
        "end_time": str(args.end_time),
        "slots_scanned": int(len(slot_summary)),
        "raw_candidates": int(len(raw_candidates)),
        **full_day_stats,
    }])
    tier123_enabled = _tier123_profile_enabled(str(args.selected_strategy_profile))
    if selected_strategy_signals.empty and not tier123_enabled:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(out_dir / "historical_full_day_raw_candidates.csv"),
            accepted_filename="historical_full_day_rules_used.csv",
            reason="[v11 historical_full_day] no v7 entry-engine signals survived the selected strategy profile",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=historical_full_day\nhistorical_date={historical_date}\n"
            f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
            f"candidate_snapshot_dir={snapshot_dir}\n"
            f"candidate_5m_dir={data_root}\n"
            f"entry_fill_model={args.entry_fill_model}\n"
            f"selected_strategy_profile={args.selected_strategy_profile}\n"
            f"ab_gate_profile={args.ab_gate_profile}\n"
            f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
            f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
            f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
            "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
            "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
            "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
            "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
            "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
            "pnl_model=v7 signal quantity, price-only, no backtest costs\n",
            encoding="utf-8",
        )
        print(f"[v11 historical_full_day] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = pd.DataFrame()
    if not selected_strategy_signals.empty:
        final = _resolve_v7_entry_engine_signals(
            selected_strategy_signals,
            label="historical_full_day",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )
    final, tier123_stats = _apply_tier123_balanced_addon(
        base_final=final,
        dates=[historical_date],
        date_to_root={historical_date: data_root},
        args=args,
        out_dir=out_dir,
        label="historical_full_day",
    )
    tier123_stats.to_csv(out_dir / "historical_full_day_tier123_balanced_addon_stats.csv", index=False)
    accepted_out = _concat_frames([accepted, tier123_stats])
    if final.empty:
        _write_empty_outputs(
            out_dir,
            accepted_out,
            str(out_dir / "historical_full_day_raw_candidates.csv"),
            accepted_filename="historical_full_day_rules_used.csv",
            reason="[v11 historical_full_day] no base or Tier123 balanced add-on trades resolved",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=historical_full_day\nhistorical_date={historical_date}\n"
            f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
            f"candidate_snapshot_dir={snapshot_dir}\n"
            f"candidate_5m_dir={data_root}\n"
            f"entry_fill_model={args.entry_fill_model}\n"
            f"selected_strategy_profile={args.selected_strategy_profile}\n"
            f"ab_gate_profile={args.ab_gate_profile}\n"
            f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
            f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
            f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
            f"tier123_balanced_addon_enabled={tier123_enabled}\n",
            encoding="utf-8",
        )
        print(f"[v11 historical_full_day] wrote empty no-trade outputs to {out_dir}")
        return 0
    _write_outputs(
        out_dir,
        final,
        accepted_out,
        str(out_dir / "historical_full_day_raw_candidates.csv"),
        accepted_filename="historical_full_day_rules_used.csv",
        strategy_description=(
            "current v7 live scanner + v7 1-minute entry engine + PAPER_TRUE-style "
            "historical fill + selected honest production profile + optional Tier123 balanced non-overlap add-on "
            "+ setup-specific 1-minute exits"
        ),
    )
    (out_dir / "inputs.txt").write_text(
        f"mode=historical_full_day\nhistorical_date={historical_date}\n"
        f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
        f"candidate_snapshot_dir={snapshot_dir}\n"
        f"candidate_5m_dir={data_root}\n"
        f"entry_fill_model={args.entry_fill_model}\n"
        f"selected_strategy_profile={args.selected_strategy_profile}\n"
        f"ab_gate_profile={args.ab_gate_profile}\n"
        f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
        f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
        f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
        f"tier123_balanced_addon_enabled={tier123_enabled}\n"
        "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
        "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
        "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
        "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
        "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
        "pnl_model=v7 signal quantity, price-only, no backtest costs\n",
        encoding="utf-8",
    )
    print(f"[v11 historical_full_day] wrote {out_dir}")
    return 0


def _run_historical_all_available(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    data_root = Path(args.candidate_5m_dir)
    fallback_root = Path(args.fallback_candidate_5m_dir) if str(args.fallback_candidate_5m_dir or "").strip() else None
    dates, date_to_root = _available_historical_dates_with_roots(
        data_root,
        fallback_root,
        str(args.start_date or ""),
        str(args.end_date or ""),
    )
    if not dates:
        raise SystemExit("[v11 all_available] no available historical dates matched the requested range")

    day_root = out_dir / "historical_all_available_days"
    raw_frames: list[pd.DataFrame] = []
    slot_summary_frames: list[pd.DataFrame] = []
    ranked_frames: list[pd.DataFrame] = []
    gated_frames: list[pd.DataFrame] = []
    rejected_frames: list[pd.DataFrame] = []
    pre_dedupe_frames: list[pd.DataFrame] = []
    live_like_frames: list[pd.DataFrame] = []
    entry_signal_frames: list[pd.DataFrame] = []
    selected_signal_frames: list[pd.DataFrame] = []
    selected_reject_frames: list[pd.DataFrame] = []
    entry_raw_frames: list[pd.DataFrame] = []
    entry_reject_frames: list[pd.DataFrame] = []
    slot_audit_frames: list[pd.DataFrame] = []
    accepted_rows: list[dict] = []

    print(
        f"[v11 all_available] running {len(dates)} dates from {dates[0]} to {dates[-1]} "
        f"with workers={int(args.workers)} primary_data_root={data_root} fallback_data_root={fallback_root or ''}",
        flush=True,
    )
    started = time.time()
    for i, day in enumerate(dates, 1):
        day_dir = day_root / day
        day_dir.mkdir(parents=True, exist_ok=True)
        snapshot_dir = day_dir / "generated_candidate_snapshots"
        day_data_root = date_to_root.get(day, data_root)
        print(f"[v11 all_available {i:02d}/{len(dates):02d}] date={day} data_root={day_data_root}", flush=True)

        raw_candidates, slot_summary = _scan_historical_full_day_candidates(
            day=day,
            start_time=str(args.start_time),
            end_time=str(args.end_time),
            workers=int(args.workers),
            snapshot_dir=snapshot_dir,
            data_root=day_data_root,
            include_ab_excluded=_ab_gate_enabled(str(args.ab_gate_profile)),
        )
        if not raw_candidates.empty:
            raw_candidates = raw_candidates.copy()
            raw_candidates["v11_source_day"] = day
        if not slot_summary.empty:
            slot_summary = slot_summary.copy()
            slot_summary.insert(0, "date", day)

        raw_candidates.to_csv(day_dir / "raw_candidates.csv", index=False)
        slot_summary.to_csv(day_dir / "slot_summary.csv", index=False)
        raw_frames.append(raw_candidates)
        slot_summary_frames.append(slot_summary)

        pipeline = _apply_v7_live_strategy(
            raw_candidates,
            day,
            ab_gate_profile=str(args.ab_gate_profile),
            ab_gate_min_quality=float(args.ab_gate_min_quality),
            ab_gate_max_per_side=int(args.ab_gate_max_per_side),
            ab_gate_max_per_slot=int(args.ab_gate_max_per_slot),
            ab_gate_setups=_ab_gate_setups_for_selected_profile(str(args.selected_strategy_profile)),
        )
        entry_engine_signals, entry_engine_raw, entry_engine_rejects = _build_v7_entry_engine_signals(
            pipeline["pre_dedupe_live_candidates"]
        )
        selected_strategy_signals, selected_strategy_rejects, selected_strategy_stats = _apply_selected_strategy_profile(
            entry_engine_signals,
            str(args.selected_strategy_profile),
        )
        for key, filename, bucket in [
            ("ranked_raw_candidates", "ranked_raw_candidates.csv", ranked_frames),
            ("v8_gated_candidates", "v8_gated_candidates.csv", gated_frames),
            ("research_rejected_candidates", "research_rejected_candidates.csv", rejected_frames),
            ("pre_dedupe_live_candidates", "pre_dedupe_live_candidates.csv", pre_dedupe_frames),
            ("live_like_candidates", "live_like_candidates.csv", live_like_frames),
            ("slot_audit", "live_pipeline_slot_audit.csv", slot_audit_frames),
        ]:
            frame = pipeline[key]
            if frame is not None and not frame.empty:
                frame = frame.copy()
                if "v11_source_day" not in frame.columns:
                    frame["v11_source_day"] = day
            frame.to_csv(day_dir / filename, index=False)
            bucket.append(frame)

        for frame, filename, bucket in [
            (entry_engine_raw, "entry_engine_raw_entries.csv", entry_raw_frames),
            (entry_engine_rejects, "entry_engine_rejects.csv", entry_reject_frames),
            (entry_engine_signals, "entry_engine_signals.csv", entry_signal_frames),
            (selected_strategy_signals, "selected_strategy_signals.csv", selected_signal_frames),
            (selected_strategy_rejects, "selected_strategy_rejects.csv", selected_reject_frames),
        ]:
            if frame is not None and not frame.empty:
                frame = frame.copy()
                frame["v11_source_day"] = day
            frame.to_csv(day_dir / filename, index=False)
            bucket.append(frame)

        stats = {
            "stage": "historical_all_available_day",
            "date": day,
            "candidate_5m_dir": str(day_data_root),
            "start_time": str(args.start_time),
            "end_time": str(args.end_time),
            "slots_scanned": int(len(slot_summary)),
            **pipeline["stats"],
            "entry_engine_raw_entries": int(len(entry_engine_raw)),
            "entry_engine_rejects": int(len(entry_engine_rejects)),
            "entry_engine_signals": int(len(entry_engine_signals)),
            **selected_strategy_stats,
        }
        accepted_rows.append(stats)
        pd.DataFrame([stats]).to_csv(day_dir / "pipeline_stats.csv", index=False)
        print(
            f"[v11 all_available {i:02d}/{len(dates):02d}] "
            f"raw={stats['raw_candidates']} live_like={stats['live_like_candidates']} "
            f"entry_signals={stats['entry_engine_signals']} selected={stats['selected_strategy_signals']} "
            f"dupes={stats['live_like_duplicates']} elapsed_total={time.time() - started:.1f}s",
            flush=True,
        )

    raw_all = _concat_frames(raw_frames)
    slot_summary_all = _concat_frames(slot_summary_frames)
    ranked_all = _concat_frames(ranked_frames)
    gated_all = _concat_frames(gated_frames)
    rejected_all = _concat_frames(rejected_frames)
    pre_dedupe_all = _concat_frames(pre_dedupe_frames)
    live_like_all = _concat_frames(live_like_frames)
    entry_raw_all = _concat_frames(entry_raw_frames)
    entry_reject_all = _concat_frames(entry_reject_frames)
    entry_signals_all = _concat_frames(entry_signal_frames)
    selected_signals_all = _concat_frames(selected_signal_frames)
    selected_rejects_all = _concat_frames(selected_reject_frames)
    slot_audit_all = _concat_frames(slot_audit_frames)
    accepted = pd.DataFrame(accepted_rows)

    raw_all.to_csv(out_dir / "historical_all_available_raw_candidates.csv", index=False)
    slot_summary_all.to_csv(out_dir / "historical_all_available_slot_summary.csv", index=False)
    ranked_all.to_csv(out_dir / "historical_all_available_ranked_raw_candidates.csv", index=False)
    gated_all.to_csv(out_dir / "historical_all_available_v8_gated_candidates.csv", index=False)
    gated_all.to_csv(out_dir / "historical_all_available_gated_candidates.csv", index=False)
    rejected_all.to_csv(out_dir / "historical_all_available_research_rejected_candidates.csv", index=False)
    pre_dedupe_all.to_csv(out_dir / "historical_all_available_pre_dedupe_live_candidates.csv", index=False)
    live_like_all.to_csv(out_dir / "historical_all_available_live_like_candidates.csv", index=False)
    entry_raw_all.to_csv(out_dir / "historical_all_available_entry_engine_raw_entries.csv", index=False)
    entry_reject_all.to_csv(out_dir / "historical_all_available_entry_engine_rejects.csv", index=False)
    entry_signals_all.to_csv(out_dir / "historical_all_available_entry_engine_signals.csv", index=False)
    selected_signals_all.to_csv(out_dir / "historical_all_available_selected_strategy_signals.csv", index=False)
    selected_rejects_all.to_csv(out_dir / "historical_all_available_selected_strategy_rejects.csv", index=False)
    slot_audit_all.to_csv(out_dir / "historical_all_available_live_pipeline_slot_audit.csv", index=False)
    accepted.to_csv(out_dir / "historical_all_available_pipeline_stats_by_day.csv", index=False)
    pd.DataFrame({"date": dates}).to_csv(out_dir / "historical_all_available_dates.csv", index=False)

    inputs_text = (
        "mode=historical_all_available\n"
        f"dates={len(dates)}\nfirst_date={dates[0]}\nlast_date={dates[-1]}\n"
        f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
        f"candidate_5m_dir={data_root}\n"
        f"fallback_candidate_5m_dir={fallback_root or ''}\n"
        "candidate_5m_dir_selection=primary root when date exists there, else fallback root\n"
        f"data_1m_dir={v6.DATA_1M_DIR}\n"
        f"entry_fill_model={args.entry_fill_model}\n"
        f"selected_strategy_profile={args.selected_strategy_profile}\n"
        f"ab_gate_profile={args.ab_gate_profile}\n"
        f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
        f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
        f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
        "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
        "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
        "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
        "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
        "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
        "pnl_model=v7 signal quantity, price-only, no backtest costs\n"
    )

    tier123_enabled = _tier123_profile_enabled(str(args.selected_strategy_profile))
    if selected_signals_all.empty and not tier123_enabled:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(out_dir / "historical_all_available_raw_candidates.csv"),
            accepted_filename="historical_all_available_rules_used.csv",
            reason="[v11 all_available] no v7 entry-engine signals survived the selected strategy profile",
        )
        (out_dir / "inputs.txt").write_text(inputs_text, encoding="utf-8")
        print(f"[v11 all_available] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = pd.DataFrame()
    if not selected_signals_all.empty:
        final = _resolve_v7_entry_engine_signals(
            selected_signals_all,
            label="historical_all_available",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )
    final, tier123_stats = _apply_tier123_balanced_addon(
        base_final=final,
        dates=dates,
        date_to_root=date_to_root,
        args=args,
        out_dir=out_dir,
        label="historical_all_available",
    )
    tier123_stats.to_csv(out_dir / "historical_all_available_tier123_balanced_addon_stats.csv", index=False)
    accepted_out = _concat_frames([accepted, tier123_stats])
    if final.empty:
        _write_empty_outputs(
            out_dir,
            accepted_out,
            str(out_dir / "historical_all_available_raw_candidates.csv"),
            accepted_filename="historical_all_available_rules_used.csv",
            reason="[v11 all_available] no base or Tier123 balanced add-on trades resolved",
        )
        (out_dir / "inputs.txt").write_text(inputs_text + f"tier123_balanced_addon_enabled={tier123_enabled}\n", encoding="utf-8")
        print(f"[v11 all_available] wrote empty no-trade outputs to {out_dir}")
        return 0
    _write_outputs(
        out_dir,
        final,
        accepted_out,
        str(out_dir / "historical_all_available_raw_candidates.csv"),
        accepted_filename="historical_all_available_rules_used.csv",
        strategy_description=(
            "current v7 live scanner + v7 1-minute entry engine + PAPER_TRUE-style "
            "historical fill + selected honest production profile + optional Tier123 balanced non-overlap add-on "
            "+ setup-specific 1-minute exits"
        ),
    )
    (out_dir / "inputs.txt").write_text(inputs_text + f"tier123_balanced_addon_enabled={tier123_enabled}\n", encoding="utf-8")
    print(f"[v11 all_available] wrote {out_dir}")
    return 0


def _cached_entry_signal_path(source_dir: Path) -> Path:
    candidates = [
        source_dir / "all_setups_entry_engine_signals.csv",
        source_dir / "historical_all_available_entry_engine_signals.csv",
        source_dir / "entry_engine_signals.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise SystemExit(
        f"[v11 cached_all_setups] no cached entry-engine signals found under {source_dir}; "
        "expected all_setups_entry_engine_signals.csv or historical_all_available_entry_engine_signals.csv"
    )


def _dates_for_cached_replay(args: argparse.Namespace, cached_signals: pd.DataFrame) -> tuple[list[str], dict[str, Path]]:
    primary_root = Path(args.candidate_5m_dir)
    fallback_root = Path(args.fallback_candidate_5m_dir) if str(args.fallback_candidate_5m_dir or "").strip() else None
    if str(args.start_date or "").strip() or str(args.end_date or "").strip():
        dates, date_to_root = _available_historical_dates_with_roots(
            primary_root,
            fallback_root,
            str(args.start_date or ""),
            str(args.end_date or ""),
        )
        if dates:
            return dates, date_to_root

    signal_col = "signal_time_ist" if "signal_time_ist" in cached_signals.columns else "bar_time_ist"
    ts = pd.to_datetime(cached_signals.get(signal_col), errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("Asia/Kolkata")
    else:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    dates = sorted(ts.dropna().dt.strftime("%Y-%m-%d").unique().tolist())
    return dates, {day: primary_root for day in dates}


def _run_historical_cached_all_setups(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.cached_all_setups_dir)
    signal_path = _cached_entry_signal_path(source_dir)
    print(f"[v11 cached_all_setups] loading cached entry-engine signals from {signal_path}", flush=True)
    cached_signals = pd.read_csv(signal_path)
    dates, date_to_root = _dates_for_cached_replay(args, cached_signals)
    if not dates:
        raise SystemExit("[v11 cached_all_setups] no dates available for cached replay")
    print(
        f"[v11 cached_all_setups] dates={len(dates):,} first={dates[0]} last={dates[-1]} "
        f"profile={args.selected_strategy_profile}",
        flush=True,
    )

    selected_signals, selected_rejects, selected_stats = _apply_selected_strategy_profile(
        cached_signals,
        str(args.selected_strategy_profile),
    )
    selected_signals.to_csv(out_dir / "cached_all_setups_selected_strategy_signals.csv", index=False)
    selected_rejects.to_csv(out_dir / "cached_all_setups_selected_strategy_rejects.csv", index=False)
    pd.DataFrame([{
        "stage": "historical_cached_all_setups",
        "source_entry_signal_path": str(signal_path),
        "cached_entry_signals": int(len(cached_signals)),
        **selected_stats,
    }]).to_csv(out_dir / "cached_all_setups_pipeline_stats.csv", index=False)

    final = pd.DataFrame()
    if not selected_signals.empty:
        final = _resolve_v7_entry_engine_signals(
            selected_signals,
            label="historical_cached_all_setups",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )

    tier123_enabled = _tier123_profile_enabled(str(args.selected_strategy_profile))
    final, tier123_stats = _apply_tier123_balanced_addon(
        base_final=final,
        dates=dates,
        date_to_root=date_to_root,
        args=args,
        out_dir=out_dir,
        label="historical_cached_all_setups",
    )
    tier123_stats.to_csv(out_dir / "historical_cached_all_setups_tier123_balanced_addon_stats.csv", index=False)
    accepted = _concat_frames([
        pd.DataFrame([{
            "stage": "historical_cached_all_setups",
            "source_entry_signal_path": str(signal_path),
            "dates": int(len(dates)),
            "first_date": dates[0],
            "last_date": dates[-1],
            "cached_entry_signals": int(len(cached_signals)),
            **selected_stats,
        }]),
        tier123_stats,
    ])

    inputs_text = (
        "mode=historical_cached_all_setups\n"
        f"cached_all_setups_dir={source_dir}\n"
        f"source_entry_signal_path={signal_path}\n"
        f"dates={len(dates)}\nfirst_date={dates[0]}\nlast_date={dates[-1]}\n"
        f"candidate_5m_dir={args.candidate_5m_dir}\n"
        f"fallback_candidate_5m_dir={args.fallback_candidate_5m_dir}\n"
        f"data_1m_dir={v6.DATA_1M_DIR}\n"
        f"entry_fill_model={args.entry_fill_model}\n"
        f"selected_strategy_profile={args.selected_strategy_profile}\n"
        f"tier123_balanced_addon_enabled={tier123_enabled}\n"
        "candidate_source=cached full v11 entry-engine signals plus current Tier123 balanced add-on scanner\n"
        "pnl_model=v7 signal quantity, PAPER_TRUE-style next 1-minute open, price-only, no backtest costs\n"
    )

    if final.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(signal_path),
            accepted_filename="historical_cached_all_setups_rules_used.csv",
            reason="[v11 cached_all_setups] no base or Tier123 balanced add-on trades resolved",
        )
        (out_dir / "inputs.txt").write_text(inputs_text, encoding="utf-8")
        print(f"[v11 cached_all_setups] wrote empty no-trade outputs to {out_dir}")
        return 0

    _write_outputs(
        out_dir,
        final,
        accepted,
        str(signal_path),
        accepted_filename="historical_cached_all_setups_rules_used.csv",
        strategy_description=(
            "cached full v11 entry-engine replay + selected honest production profile + "
            "optional Tier123 balanced non-overlap add-on + setup-specific 1-minute exits"
        ),
    )
    (out_dir / "inputs.txt").write_text(inputs_text, encoding="utf-8")
    print(f"[v11 cached_all_setups] wrote {out_dir}")
    return 0


def _write_empty_outputs(
    out_dir: Path,
    accepted: pd.DataFrame,
    source: str,
    *,
    accepted_filename: str,
    reason: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trade_cols = [
        "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
        "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
        "trade_date", "v8_entry_model", "v8_entry_delay_minutes",
        "v6_sl_pct", "v6_target_pct", "v6_outcome", "v6_exit_price",
        "v6_exit_time_ist", "v6_bars_held", "v6_pnl_pct_price",
        "v6_gross_pnl_rs", "v6_cost_rs", "v6_net_pnl_rs",
        "capital_per_trade_rs", "leverage", "notional_exposure_rs",
        "v7_resolution_source", "v8_resolution_source",
    ]
    pd.DataFrame(columns=trade_cols).to_csv(out_dir / "trades.csv", index=False)
    pd.DataFrame(columns=["trade_date", "net_pnl_rs", "cum_pnl_rs", "drawdown_rs"]).to_csv(
        out_dir / "daily.csv",
        index=False,
    )
    pd.DataFrame(
        columns=[
            "side", "setup", "trades", "win_rate_pct", "target_rate_pct",
            "sl_rate_pct", "eod_rate_pct", "pnl_rs", "sl_pct", "target_pct",
        ]
    ).to_csv(out_dir / "by_setup.csv", index=False)
    accepted.to_csv(out_dir / accepted_filename, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)
    pd.DataFrame(
        columns=[
            "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
            "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
            "v8_entry_delay_minutes", "v6_outcome", "v6_net_pnl_rs",
        ]
    ).to_csv(out_dir / "entry_timing_audit.csv", index=False)

    text = "\n".join([
        "=" * 100,
        "AVWAP ID 5-min v11 backtest",
        "current v7 live signal flow + next 1-minute open entry + setup-specific 1-minute exits",
        f"Input: {source}",
        "=" * 100,
        "Trades              : 0",
        "Trading days        : 0",
        "Avg trades/day      : 0.00",
        "Win rate            : 0.00%",
        "Target / SL / EOD   : 0.00% / 0.00% / 0.00%",
        "Profit factor       : NA",
        "Net PnL             : Rs 0.00",
        "Day win rate        : 0.00%",
        "Max drawdown        : Rs 0.00",
        "LONG trades/PnL     : 0 / Rs 0.00",
        "SHORT trades/PnL    : 0 / Rs 0.00",
        "",
        f"No-trade reason     : {reason}",
        "=" * 100,
    ])
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def _write_outputs(
    out_dir: Path,
    trades: pd.DataFrame,
    accepted: pd.DataFrame,
    source: str,
    accepted_filename: str = "accepted_rules.csv",
    strategy_description: str = "current v7 live signal flow + next 1-minute open entry + setup-specific 1-minute exits",
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_dir / "trades.csv", index=False)
    summary, daily, by_setup = v6._metrics(trades)
    daily.rename(columns={"_pnl": "net_pnl_rs"}).to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    accepted.to_csv(out_dir / accepted_filename, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)

    entry_audit = trades[[
        c for c in [
            "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
            "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
            "v8_entry_delay_minutes", "v6_outcome", "v6_net_pnl_rs",
        ] if c in trades.columns
    ]].copy()
    entry_audit.to_csv(out_dir / "entry_timing_audit.csv", index=False)

    text = v6._summary_text(summary, by_setup, Path(source))
    text = text.replace("AVWAP ID 5-min v6 backtest", "AVWAP ID 5-min v11 backtest")
    text = text.replace(
        "v5 trades re-resolved on 1-minute bars with setup-specific SL/target exits",
        strategy_description,
    )
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v11 exact current-v7 live strategy backtest")
    ap.add_argument(
        "--mode",
        choices=[
            "live_parity",
            "v7_live_paper_replay",
            "historical_full_day",
            "historical_all_available",
            "historical_cached_all_setups",
        ],
        default="historical_full_day",
    )
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument(
        "--cached_all_setups_dir",
        type=str,
        default=r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250",
        help=(
            "For --mode historical_cached_all_setups: directory containing a completed full v11 "
            "all_setups_entry_engine_signals.csv or historical_all_available_entry_engine_signals.csv."
        ),
    )
    ap.add_argument("--live_candidate_json_dir", type=str, default=str(LIVE_CANDIDATE_JSON_DIR))
    ap.add_argument("--live_paper_dir", type=str, default=str(LIVE_PAPER_DIR))
    ap.add_argument("--candidate_5m_dir", type=str, default=str(DEFAULT_CANDIDATE_5M_DIR))
    ap.add_argument("--fallback_candidate_5m_dir", type=str, default=str(HISTORICAL_5M_DIR))
    ap.add_argument("--live_date", type=str, default="")
    ap.add_argument("--historical_date", type=str, default="")
    ap.add_argument("--start_date", type=str, default="")
    ap.add_argument("--end_date", type=str, default="")
    ap.add_argument("--start_time", type=str, default="09:15")
    ap.add_argument("--end_time", type=str, default="15:00")
    ap.add_argument("--workers", type=int, default=v5.v2.DEFAULT_WORKERS)
    ap.add_argument("--cost_bps", type=float, default=v6.DEFAULT_COST_BPS)
    ap.add_argument(
        "--entry_fill_model",
        choices=["ltp_on_signal_1m_open", "signal_bar"],
        default="ltp_on_signal_1m_open",
        help=(
            "Historical approximation of PAPER_TRADE_TRUE fills. "
            "ltp_on_signal_1m_open mirrors the live bat's ltp_on_signal mode using "
            "the next 1-minute open plus v7 slippage/rebased SL-target."
        ),
    )
    ap.add_argument(
        "--selected_strategy_profile",
        choices=list(SELECTED_STRATEGY_PROFILE_CHOICES),
        default=SELECTED_STRATEGY_DEFAULT_PROFILE,
        help=(
            "Post-v7-entry strategy profile for historical modes. "
            "production_core is the honest holdout-improved profile; "
            "production_core_tiny adds the tiny BEAR-only E_VWAP_LOSE_EARLY_SHORT rule; "
            "production_core_ab_probe adds all explicit A/B probation setups; "
            "production_core_ab_good_probe adds only the positive A/B watchlist setups "
            f"{', '.join(AB_GOOD_RESULT_SETUPS)}; "
            "production_core_ab_filtered_relaxed adds the current best filtered A/B probation "
            f"rules: B_HUGE rs_pct <= {AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT:.4f} and "
            "A_PULLBACK market_abs_ret_pct <= "
            f"{AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT:.4f}; "
            "production_core_ab_max_pnl_low_valid adds the 2026-06-02 max-PnL profile "
            "with relaxed S_BB, filtered B_AVWAP, filtered A_MOD_CLOSE, "
            "A_MOD_BREAK_C1_LOW micro-probe, A_MOD_BREAK_C1_HIGH add-on, "
            "D_EMA20_REJECTION add-on, filtered E_VWAP_BAND, and low-validation E_VWAP_LOSE; "
            "production_core_ab_max_pnl_low_valid_residual_overlay keeps that profile unchanged "
            "and additionally admits the residual late-session D_EMA20_REJECTION and residual "
            "morning S_BB_SQUEEZE_SHORT probation overlays; "
            f"{TIER123_BALANCED_PROFILE} keeps the residual-overlay profile protected and appends "
            "the Tier123 balanced non-overlap add-on: late bearish EMA stair short plus controlled "
            "VWAP extreme fade long/short; "
            "ab_only_probe tests all explicit A/B probation setups; "
            "ab_good_only_probe tests only the positive A/B watchlist setups; "
            "ab_filtered_relaxed_only tests only the current best filtered A/B probation rules; "
            "none keeps raw v11/v7 entry-engine signals."
        ),
    )
    ap.add_argument(
        "--ab_gate_profile",
        choices=list(AB_GATE_PROFILE_CHOICES),
        default="off",
        help=(
            "Explicit A/B setup admission gate before the v7 entry engine. "
            "off keeps exact production live-gate behavior; quality_top_slot admits only "
            "top quality A_/B_ candidates per slot for probation research."
        ),
    )
    ap.add_argument(
        "--ab_gate_min_quality",
        type=float,
        default=250.0,
        help="Minimum quality_score for --ab_gate_profile quality_top_slot.",
    )
    ap.add_argument(
        "--ab_gate_max_per_side",
        type=int,
        default=1,
        help="Maximum A/B probation candidates per side per 5-minute slot.",
    )
    ap.add_argument(
        "--ab_gate_max_per_slot",
        type=int,
        default=2,
        help="Maximum A/B probation candidates across both sides per 5-minute slot.",
    )
    args = ap.parse_args()
    args.selected_strategy_profile = _normalise_selected_strategy_profile(args.selected_strategy_profile)
    args.ab_gate_profile = _normalise_ab_gate_profile(args.ab_gate_profile)
    if args.selected_strategy_profile in AB_SELECTED_STRATEGY_PROFILES and args.ab_gate_profile == "off":
        args.ab_gate_profile = "quality_top_slot"
        print(
            "[v11 ab_gate] auto-enabled quality_top_slot because selected_strategy_profile "
            f"is {args.selected_strategy_profile}",
            flush=True,
        )

    if args.mode == "live_parity":
        return _run_live_parity(args)
    if args.mode == "v7_live_paper_replay":
        return _run_v7_live_paper_replay(args)
    if args.mode == "historical_full_day":
        return _run_historical_full_day(args)
    if args.mode == "historical_all_available":
        return _run_historical_all_available(args)
    if args.mode == "historical_cached_all_setups":
        return _run_historical_cached_all_setups(args)
    raise SystemExit(f"[v11] unsupported mode: {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
