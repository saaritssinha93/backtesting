# -*- coding: utf-8 -*-
"""
V17m 5-min combined runner.

Builds on v17k, not v17l, because v17l improved PF/DD but removed too much
volume from the new long setups.

Main changes vs v17k
--------------------
1. SHORT target is reduced from 1.00% to 0.80%.
2. Setup-specific post-scan filters are applied only to setups whose v17k
   realized win rate was below 75%.
3. Filters are calibrated from the v17k 20260425_175439 trade CSV to keep
   roughly 72-89% of each affected setup while improving PF and reducing DD.

V17k CSV replay estimate before the SHORT target re-resolve:
  - combined trades: 5964 -> 4509 (75.6% retained)
  - combined win rate: 68.9% -> 73.1%
  - combined PF: 1.65 -> 2.02
  - combined max DD: ~Rs.24.3k -> ~Rs.9.8k

Outputs go to outputs_v17m_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

from . import v17k_5min as _v17k  # cascade, intentionally skips v17l
from . import v16_5min as _base


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


V17M_FILTERS_ENABLED = _env_bool("EQIDV17M_FILTERS_ENABLED", True)
V17M_SHORT_TARGET_PCT = _env_float("EQIDV17M_SHORT_TARGET_PCT", 0.0080)

# LONG rules: calibrated on v17k low-win setups.
V17M_LONG_A_MOD_MIN_ATR_PCT = _env_float("EQIDV17M_LONG_A_MOD_MIN_ATR_PCT", 0.00355)
V17M_LONG_A_MOD_MAX_BARS_FROM_OPEN = _env_float("EQIDV17M_LONG_A_MOD_MAX_BARS_FROM_OPEN", 17.0)
V17M_LONG_A_CCB_MAX_ATR_PCT = _env_float("EQIDV17M_LONG_A_CCB_MAX_ATR_PCT", 0.00770)
V17M_LONG_A_CCB_MIN_QS = _env_float("EQIDV17M_LONG_A_CCB_MIN_QS", 5.70)
V17M_LONG_C_OR_MAX_VOL_RATIO = _env_float("EQIDV17M_LONG_C_OR_MAX_VOL_RATIO", 3.76)
V17M_LONG_C_OR_MIN_NIFTY_RS = _env_float("EQIDV17M_LONG_C_OR_MIN_NIFTY_RS", 1.075)
V17M_LONG_D_EMA_MAX_VOL_RATIO = _env_float("EQIDV17M_LONG_D_EMA_MAX_VOL_RATIO", 4.35)
V17M_LONG_D_EMA_MIN_AVWAP_DIST = _env_float("EQIDV17M_LONG_D_EMA_MIN_AVWAP_DIST", 0.75)
V17M_LONG_G_HH_MIN_NIFTY_RS = _env_float("EQIDV17M_LONG_G_HH_MIN_NIFTY_RS", 1.138)
V17M_LONG_G_HH_MIN_QS = _env_float("EQIDV17M_LONG_G_HH_MIN_QS", 0.324)

# SHORT rules: calibrated on v17k low-win setups; target is re-resolved at 0.80%.
V17M_SHORT_A_MOD_MIN_ENTRY_HOUR = _env_float("EQIDV17M_SHORT_A_MOD_MIN_ENTRY_HOUR", 9.6667)
V17M_SHORT_A_MOD_MAX_EMA20_GAP = _env_float("EQIDV17M_SHORT_A_MOD_MAX_EMA20_GAP", 3.27)
V17M_SHORT_C_OR_MAX_ENTRY_HOUR = _env_float("EQIDV17M_SHORT_C_OR_MAX_ENTRY_HOUR", 10.25)
V17M_SHORT_C_OR_MIN_ADX = _env_float("EQIDV17M_SHORT_C_OR_MIN_ADX", 26.45)
V17M_SHORT_D_AVWAP_MIN_ADX = _env_float("EQIDV17M_SHORT_D_AVWAP_MIN_ADX", 31.60)
V17M_SHORT_D_EMA_MIN_RSI = _env_float("EQIDV17M_SHORT_D_EMA_MIN_RSI", 41.17)
V17M_SHORT_D_EMA_MAX_EMA20_GAP = _env_float("EQIDV17M_SHORT_D_EMA_MAX_EMA20_GAP", 1.32)
V17M_SHORT_E_VWAP_MIN_ADX = _env_float("EQIDV17M_SHORT_E_VWAP_MIN_ADX", 28.13)
V17M_SHORT_G_LL_MIN_AVWAP_DIST = _env_float("EQIDV17M_SHORT_G_LL_MIN_AVWAP_DIST", 0.192)


# ---------------------------------------------------------------------------
# Change 1: SHORT target override.
# ---------------------------------------------------------------------------
_base.TEST_SHORT_TARGET_PCT = float(V17M_SHORT_TARGET_PCT)


# ---------------------------------------------------------------------------
# Change 2: route outputs to outputs_v17m_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17m = _base.runtime_dir


def _v17m_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17m_5min", "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min",
            "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min",
            "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17m_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17m(*tuple(new_parts))


_base.runtime_dir = _v17m_runtime_dir


# ---------------------------------------------------------------------------
# Change 3: post-scan setup cleanup.
# ---------------------------------------------------------------------------
_v17k_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17k_get_filter_reason = _base.get_v16_filter_reason


def _num(work: pd.DataFrame, col: str) -> pd.Series:
    if col not in work.columns:
        return pd.Series(np.nan, index=work.index, dtype="float64")
    return pd.to_numeric(work[col], errors="coerce")


def _entry_hour(work: pd.DataFrame) -> pd.Series:
    if "entry_time_ist" in work.columns:
        dt = pd.to_datetime(work["entry_time_ist"], errors="coerce")
        return dt.dt.hour + (dt.dt.minute / 60.0)
    if "entry_time" in work.columns:
        dt = pd.to_datetime(work["entry_time"].astype(str), format="%H:%M:%S", errors="coerce")
        return dt.dt.hour + (dt.dt.minute / 60.0)
    return pd.Series(np.nan, index=work.index, dtype="float64")


def _drop_by_setup(
    work: pd.DataFrame,
    setup_name: str,
    keep_condition: pd.Series,
    drop_mask: pd.Series,
    dropped: dict,
) -> pd.Series:
    setup = work["setup"].astype(str).str.upper().str.strip()
    in_setup = setup.eq(setup_name)
    fail = in_setup & ~keep_condition.fillna(False)
    dropped[setup_name] = int(fail.sum())
    return drop_mask | fail


def _v17m_apply_long_filters(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df is None or long_df.empty or "setup" not in long_df.columns:
        return long_df

    work = long_df.copy()
    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    atr = _num(work, "atr_pct_signal")
    bfo = _num(work, "bars_from_open")
    qs = _num(work, "quality_score")
    vol = _num(work, "entry_bar_vol_ratio")
    nrs = _num(work, "nifty_rel_strength_pct")
    avwap = _num(work, "avwap_dist_atr_signal")

    drop_mask = _drop_by_setup(
        work,
        "A_MOD_BREAK_C1_HIGH",
        (atr >= V17M_LONG_A_MOD_MIN_ATR_PCT) & (bfo <= V17M_LONG_A_MOD_MAX_BARS_FROM_OPEN),
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "A_MOD_CLOSE_CONTINUATION_BREAK",
        (atr <= V17M_LONG_A_CCB_MAX_ATR_PCT) & (qs >= V17M_LONG_A_CCB_MIN_QS),
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "C_OR_BREAKOUT",
        (vol <= V17M_LONG_C_OR_MAX_VOL_RATIO) & (nrs >= V17M_LONG_C_OR_MIN_NIFTY_RS),
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "D_EMA20_BOUNCE",
        (vol <= V17M_LONG_D_EMA_MAX_VOL_RATIO) & (avwap >= V17M_LONG_D_EMA_MIN_AVWAP_DIST),
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "G_HIGHER_HIGH_BREAK",
        (nrs >= V17M_LONG_G_HH_MIN_NIFTY_RS) & (qs >= V17M_LONG_G_HH_MIN_QS),
        drop_mask,
        dropped,
    )

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {setup}" for setup, cnt in dropped.items() if cnt > 0)
    print(f"[V17M_FILTER] LONG low-win setup cleanup: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


def _v17m_apply_short_filters(short_df: pd.DataFrame) -> pd.DataFrame:
    if short_df is None or short_df.empty or "setup" not in short_df.columns:
        return short_df

    work = short_df.copy()
    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    hour = _entry_hour(work)
    ema_gap = _num(work, "ema20_gap_atr_signal")
    adx = _num(work, "adx_signal")
    rsi = _num(work, "rsi_signal")
    avwap = _num(work, "avwap_dist_atr_signal")

    drop_mask = _drop_by_setup(
        work,
        "A_MOD_BREAK_C1_LOW",
        (hour >= V17M_SHORT_A_MOD_MIN_ENTRY_HOUR) & (ema_gap <= V17M_SHORT_A_MOD_MAX_EMA20_GAP),
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "C_OR_BREAKDOWN",
        (hour <= V17M_SHORT_C_OR_MAX_ENTRY_HOUR) & (adx >= V17M_SHORT_C_OR_MIN_ADX),
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "D_AVWAP_LOSE_REVERSAL",
        adx >= V17M_SHORT_D_AVWAP_MIN_ADX,
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "D_EMA20_REJECTION",
        (rsi >= V17M_SHORT_D_EMA_MIN_RSI) & (ema_gap <= V17M_SHORT_D_EMA_MAX_EMA20_GAP),
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "E_VWAP_BAND_FADE",
        adx >= V17M_SHORT_E_VWAP_MIN_ADX,
        drop_mask,
        dropped,
    )
    drop_mask = _drop_by_setup(
        work,
        "G_LOWER_LOW_BREAK",
        avwap >= V17M_SHORT_G_LL_MIN_AVWAP_DIST,
        drop_mask,
        dropped,
    )

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {setup}" for setup, cnt in dropped.items() if cnt > 0)
    print(f"[V17M_FILTER] SHORT low-win setup cleanup: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


def _v17m_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17k_apply_post_scan_filters(short_df, long_df)

    if not V17M_FILTERS_ENABLED:
        return short_df, long_df

    long_df = _v17m_apply_long_filters(long_df)
    short_df = _v17m_apply_short_filters(short_df)
    return short_df, long_df


def _row_float(row: dict, col: str) -> float:
    try:
        return float(row.get(col, float("nan")))
    except (TypeError, ValueError):
        return float("nan")


def _row_entry_hour(row: dict) -> float:
    raw = row.get("entry_time_ist", None)
    if raw is None:
        raw = row.get("entry_time", None)
    if raw is None:
        return float("nan")
    try:
        ts = pd.to_datetime(raw, errors="coerce")
        if pd.isna(ts):
            return float("nan")
        return float(ts.hour) + float(ts.minute) / 60.0
    except Exception:
        return float("nan")


def _fail(msg: str, value: float, threshold: float) -> str:
    return f"{msg}: value={value:.4f}, threshold={threshold:.4f}"


def _v17m_get_filter_reason(row: dict, side: str):
    reason = _v17k_get_filter_reason(row, side)
    if reason is not None:
        return reason
    if not V17M_FILTERS_ENABLED:
        return None

    setup = str(row.get("setup", "")).upper().strip()
    side_u = str(side).upper().strip()

    atr = _row_float(row, "atr_pct_signal")
    bfo = _row_float(row, "bars_from_open")
    qs = _row_float(row, "quality_score")
    vol = _row_float(row, "entry_bar_vol_ratio")
    nrs = _row_float(row, "nifty_rel_strength_pct")
    avwap = _row_float(row, "avwap_dist_atr_signal")
    hour = _row_entry_hour(row)
    ema_gap = _row_float(row, "ema20_gap_atr_signal")
    adx = _row_float(row, "adx_signal")
    rsi = _row_float(row, "rsi_signal")

    if side_u == "LONG":
        if setup == "A_MOD_BREAK_C1_HIGH":
            if not (np.isfinite(atr) and atr >= V17M_LONG_A_MOD_MIN_ATR_PCT):
                return _fail("v17m LONG A_MOD low ATR", atr, V17M_LONG_A_MOD_MIN_ATR_PCT)
            if not (np.isfinite(bfo) and bfo <= V17M_LONG_A_MOD_MAX_BARS_FROM_OPEN):
                return _fail("v17m LONG A_MOD late bars_from_open", bfo, V17M_LONG_A_MOD_MAX_BARS_FROM_OPEN)
        elif setup == "A_MOD_CLOSE_CONTINUATION_BREAK":
            if not (np.isfinite(atr) and atr <= V17M_LONG_A_CCB_MAX_ATR_PCT):
                return _fail("v17m LONG A_CCB high ATR", atr, V17M_LONG_A_CCB_MAX_ATR_PCT)
            if not (np.isfinite(qs) and qs >= V17M_LONG_A_CCB_MIN_QS):
                return _fail("v17m LONG A_CCB low quality_score", qs, V17M_LONG_A_CCB_MIN_QS)
        elif setup == "C_OR_BREAKOUT":
            if not (np.isfinite(vol) and vol <= V17M_LONG_C_OR_MAX_VOL_RATIO):
                return _fail("v17m LONG C_OR high entry volume", vol, V17M_LONG_C_OR_MAX_VOL_RATIO)
            if not (np.isfinite(nrs) and nrs >= V17M_LONG_C_OR_MIN_NIFTY_RS):
                return _fail("v17m LONG C_OR weak nifty_rs", nrs, V17M_LONG_C_OR_MIN_NIFTY_RS)
        elif setup == "D_EMA20_BOUNCE":
            if not (np.isfinite(vol) and vol <= V17M_LONG_D_EMA_MAX_VOL_RATIO):
                return _fail("v17m LONG D_EMA high entry volume", vol, V17M_LONG_D_EMA_MAX_VOL_RATIO)
            if not (np.isfinite(avwap) and avwap >= V17M_LONG_D_EMA_MIN_AVWAP_DIST):
                return _fail("v17m LONG D_EMA weak avwap distance", avwap, V17M_LONG_D_EMA_MIN_AVWAP_DIST)
        elif setup == "G_HIGHER_HIGH_BREAK":
            if not (np.isfinite(nrs) and nrs >= V17M_LONG_G_HH_MIN_NIFTY_RS):
                return _fail("v17m LONG G_HH weak nifty_rs", nrs, V17M_LONG_G_HH_MIN_NIFTY_RS)
            if not (np.isfinite(qs) and qs >= V17M_LONG_G_HH_MIN_QS):
                return _fail("v17m LONG G_HH low quality_score", qs, V17M_LONG_G_HH_MIN_QS)

    if side_u == "SHORT":
        if setup == "A_MOD_BREAK_C1_LOW":
            if not (np.isfinite(hour) and hour >= V17M_SHORT_A_MOD_MIN_ENTRY_HOUR):
                return _fail("v17m SHORT A_MOD too early", hour, V17M_SHORT_A_MOD_MIN_ENTRY_HOUR)
            if not (np.isfinite(ema_gap) and ema_gap <= V17M_SHORT_A_MOD_MAX_EMA20_GAP):
                return _fail("v17m SHORT A_MOD high ema20 gap", ema_gap, V17M_SHORT_A_MOD_MAX_EMA20_GAP)
        elif setup == "C_OR_BREAKDOWN":
            if not (np.isfinite(hour) and hour <= V17M_SHORT_C_OR_MAX_ENTRY_HOUR):
                return _fail("v17m SHORT C_OR late entry", hour, V17M_SHORT_C_OR_MAX_ENTRY_HOUR)
            if not (np.isfinite(adx) and adx >= V17M_SHORT_C_OR_MIN_ADX):
                return _fail("v17m SHORT C_OR low ADX", adx, V17M_SHORT_C_OR_MIN_ADX)
        elif setup == "D_AVWAP_LOSE_REVERSAL":
            if not (np.isfinite(adx) and adx >= V17M_SHORT_D_AVWAP_MIN_ADX):
                return _fail("v17m SHORT D_AVWAP low ADX", adx, V17M_SHORT_D_AVWAP_MIN_ADX)
        elif setup == "D_EMA20_REJECTION":
            if not (np.isfinite(rsi) and rsi >= V17M_SHORT_D_EMA_MIN_RSI):
                return _fail("v17m SHORT D_EMA low RSI", rsi, V17M_SHORT_D_EMA_MIN_RSI)
            if not (np.isfinite(ema_gap) and ema_gap <= V17M_SHORT_D_EMA_MAX_EMA20_GAP):
                return _fail("v17m SHORT D_EMA high ema20 gap", ema_gap, V17M_SHORT_D_EMA_MAX_EMA20_GAP)
        elif setup == "E_VWAP_BAND_FADE":
            if not (np.isfinite(adx) and adx >= V17M_SHORT_E_VWAP_MIN_ADX):
                return _fail("v17m SHORT E_VWAP low ADX", adx, V17M_SHORT_E_VWAP_MIN_ADX)
        elif setup == "G_LOWER_LOW_BREAK":
            if not (np.isfinite(avwap) and avwap >= V17M_SHORT_G_LL_MIN_AVWAP_DIST):
                return _fail("v17m SHORT G_LL weak avwap distance", avwap, V17M_SHORT_G_LL_MIN_AVWAP_DIST)

    return None


_base._apply_v16_post_scan_filters = _v17m_apply_post_scan_filters
_base.get_v16_filter_reason = _v17m_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17m 5-min runner: v17k + SHORT target 0.80% + setup-wise cleanup")
    print("  Output dir: outputs_v17m_5min")
    print("--- Targets ---")
    print(f"  LONG  target = {_base.TEST_LONG_TARGET_PCT * 100:.2f}% (inherited)")
    print(f"  SHORT target = {_base.TEST_SHORT_TARGET_PCT * 100:.2f}% (v17m override)")
    print("--- LONG low-win setup filters ---")
    print(
        "  A_MOD_BREAK_C1_HIGH: atr_pct>="
        f"{V17M_LONG_A_MOD_MIN_ATR_PCT:.5f}, bars_from_open<={V17M_LONG_A_MOD_MAX_BARS_FROM_OPEN:.1f}"
    )
    print(
        "  A_MOD_CLOSE_CONTINUATION_BREAK: atr_pct<="
        f"{V17M_LONG_A_CCB_MAX_ATR_PCT:.5f}, quality_score>={V17M_LONG_A_CCB_MIN_QS:.2f}"
    )
    print(
        "  C_OR_BREAKOUT: entry_vol<="
        f"{V17M_LONG_C_OR_MAX_VOL_RATIO:.2f}, nifty_rs>={V17M_LONG_C_OR_MIN_NIFTY_RS:.3f}"
    )
    print(
        "  D_EMA20_BOUNCE: entry_vol<="
        f"{V17M_LONG_D_EMA_MAX_VOL_RATIO:.2f}, avwap_dist>={V17M_LONG_D_EMA_MIN_AVWAP_DIST:.2f}"
    )
    print(
        "  G_HIGHER_HIGH_BREAK: nifty_rs>="
        f"{V17M_LONG_G_HH_MIN_NIFTY_RS:.3f}, quality_score>={V17M_LONG_G_HH_MIN_QS:.3f}"
    )
    print("--- SHORT low-win setup filters ---")
    print(
        "  A_MOD_BREAK_C1_LOW: entry_hour>="
        f"{V17M_SHORT_A_MOD_MIN_ENTRY_HOUR:.4f}, ema20_gap<={V17M_SHORT_A_MOD_MAX_EMA20_GAP:.2f}"
    )
    print(
        "  C_OR_BREAKDOWN: entry_hour<="
        f"{V17M_SHORT_C_OR_MAX_ENTRY_HOUR:.2f}, adx>={V17M_SHORT_C_OR_MIN_ADX:.2f}"
    )
    print(f"  D_AVWAP_LOSE_REVERSAL: adx>={V17M_SHORT_D_AVWAP_MIN_ADX:.2f}")
    print(
        "  D_EMA20_REJECTION: rsi>="
        f"{V17M_SHORT_D_EMA_MIN_RSI:.2f}, ema20_gap<={V17M_SHORT_D_EMA_MAX_EMA20_GAP:.2f}"
    )
    print(f"  E_VWAP_BAND_FADE: adx>={V17M_SHORT_E_VWAP_MIN_ADX:.2f}")
    print(f"  G_LOWER_LOW_BREAK: avwap_dist>={V17M_SHORT_G_LL_MIN_AVWAP_DIST:.3f}")
    print("--- Inherits all v17k / v17j / v17i / v17h / v17g / v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
