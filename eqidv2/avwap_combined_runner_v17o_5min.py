# -*- coding: utf-8 -*-
"""
V17o 5-min combined runner — Option A surgical per-setup filters on top
of v17n. One feature filter per setup, picked as the highest win-rate lift
from offline v17m CSV bucket analysis at the 15th/85th percentile threshold.

Stage 1 inherits: v17m's per-setup low-win cleanup
Stage 1.5 inherits: v17n's codex per-setup filters
Stage 1.6 (new): 8 per-setup "top cut" filters (this version)

Filters applied (offline projection: -13% trades, +2.6pp win, PF 2.20 -> 2.40)
-----------------------------------------------------------------------------
LONG.A_MOD_BREAK_C1_HIGH       : atr_pct_signal >= 0.004      (+3.57pp win)
LONG.B_AVWAP_RECLAIM_REVERSAL  : nifty_rel_strength_pct >= 1.167  (+2.61pp)
LONG.B_HUGE_C1_RECLAIM_BREAK   : rsi_signal >= 70.82            (+2.94pp)
LONG.C_OR_BREAKOUT             : entry_bar_vol_ratio <= 3.26    (+2.90pp)
LONG.E_VWAP_BAND_FADE          : nifty_rel_strength_pct >= 1.10 (+2.86pp)
SHORT.C_OR_BREAKDOWN           : quality_score <= 0.84          (+2.68pp)
SHORT.D_AVWAP_LOSE_REVERSAL    : adx_signal >= 33.13            (+4.21pp)
SHORT.E_VWAP_BAND_FADE         : stochk_signal >= 64.71         (+2.38pp)

All env-toggleable. Outputs go to outputs_v17o_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17n_5min as _v17n  # cascade
import avwap_combined_runner_v16_5min as _base


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None: return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None: return float(default)
    try: return float(raw)
    except (TypeError, ValueError): return float(default)


# ---------------------------------------------------------------------------
# V17o env toggles.
# ---------------------------------------------------------------------------
V17O_FILTERS_ENABLED = _env_bool("EQIDV17O_FILTERS_ENABLED", True)

# LONG thresholds
V17O_LONG_A_MOD_MIN_ATR_PCT      = _env_float("EQIDV17O_LONG_A_MOD_MIN_ATR_PCT", 0.004)
V17O_LONG_B_AVWAP_MIN_NIFTY_RS   = _env_float("EQIDV17O_LONG_B_AVWAP_MIN_NIFTY_RS", 1.167)
V17O_LONG_B_HUGE_MIN_RSI         = _env_float("EQIDV17O_LONG_B_HUGE_MIN_RSI", 70.82)
V17O_LONG_C_OR_MAX_VOL_RATIO     = _env_float("EQIDV17O_LONG_C_OR_MAX_VOL_RATIO", 3.26)
V17O_LONG_E_VWAP_MIN_NIFTY_RS    = _env_float("EQIDV17O_LONG_E_VWAP_MIN_NIFTY_RS", 1.10)

# SHORT thresholds
V17O_SHORT_C_OR_MAX_QS           = _env_float("EQIDV17O_SHORT_C_OR_MAX_QS", 0.84)
V17O_SHORT_D_AVWAP_MIN_ADX       = _env_float("EQIDV17O_SHORT_D_AVWAP_MIN_ADX", 33.13)
V17O_SHORT_E_VWAP_MIN_STOCHK     = _env_float("EQIDV17O_SHORT_E_VWAP_MIN_STOCHK", 64.71)


# ---------------------------------------------------------------------------
# Output dir routing.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17o = _base.runtime_dir


def _v17o_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17o_5min", "v17n_5min", "v17m_5min", "v17l_5min", "v17k_5min",
            "v17j_5min", "v17i_5min", "v17h_5min", "v17g_5min", "v17f_5min",
            "v17d_5min", "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17o_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17o(*tuple(new_parts))


_base.runtime_dir = _v17o_runtime_dir


# ---------------------------------------------------------------------------
# Per-setup filters (post-scan, applied AFTER v17n's codex+dedup).
# ---------------------------------------------------------------------------
def _num(work: pd.DataFrame, col: str) -> pd.Series:
    if col not in work.columns:
        return pd.Series(np.nan, index=work.index, dtype="float64")
    return pd.to_numeric(work[col], errors="coerce")


def _v17o_apply_long_filters(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df is None or long_df.empty or "setup" not in long_df.columns:
        return long_df
    work = long_df.copy()
    setup = work["setup"].astype(str).str.upper().str.strip()

    atr_pct = _num(work, "atr_pct_signal")
    nrs = _num(work, "nifty_rel_strength_pct")
    rsi = _num(work, "rsi_signal")
    vol = _num(work, "entry_bar_vol_ratio")

    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    # A_MOD_BREAK_C1_HIGH: atr_pct >= 0.004
    in_set = setup.eq("A_MOD_BREAK_C1_HIGH")
    fail = in_set & ~(atr_pct >= V17O_LONG_A_MOD_MIN_ATR_PCT).fillna(False)
    dropped["A_MOD_BREAK"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # B_AVWAP_RECLAIM_REVERSAL: nifty_rs >= 1.167
    in_set = setup.eq("B_AVWAP_RECLAIM_REVERSAL")
    fail = in_set & ~(nrs >= V17O_LONG_B_AVWAP_MIN_NIFTY_RS).fillna(False)
    dropped["B_AVWAP"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # B_HUGE_C1_RECLAIM_BREAK: rsi >= 70.82
    in_set = setup.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK")
    fail = in_set & ~(rsi >= V17O_LONG_B_HUGE_MIN_RSI).fillna(False)
    dropped["B_HUGE"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # C_OR_BREAKOUT: vol <= 3.26
    in_set = setup.eq("C_OR_BREAKOUT")
    fail = in_set & ~(vol <= V17O_LONG_C_OR_MAX_VOL_RATIO).fillna(False)
    dropped["C_OR"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # E_VWAP_BAND_FADE: nifty_rs >= 1.10
    in_set = setup.eq("E_VWAP_BAND_FADE")
    fail = in_set & ~(nrs >= V17O_LONG_E_VWAP_MIN_NIFTY_RS).fillna(False)
    dropped["E_VWAP"] = int(fail.sum())
    drop_mask = drop_mask | fail

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {s}" for s, cnt in dropped.items() if cnt > 0)
    print(f"[V17O_FILTER] LONG top-cut filters: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


def _v17o_apply_short_filters(short_df: pd.DataFrame) -> pd.DataFrame:
    if short_df is None or short_df.empty or "setup" not in short_df.columns:
        return short_df
    work = short_df.copy()
    setup = work["setup"].astype(str).str.upper().str.strip()

    qs = _num(work, "quality_score")
    adx = _num(work, "adx_signal")
    stochk = _num(work, "stochk_signal")

    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    # C_OR_BREAKDOWN: qs <= 0.84
    in_set = setup.eq("C_OR_BREAKDOWN")
    fail = in_set & ~(qs <= V17O_SHORT_C_OR_MAX_QS).fillna(False)
    dropped["C_OR"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # D_AVWAP_LOSE_REVERSAL: adx >= 33.13
    in_set = setup.eq("D_AVWAP_LOSE_REVERSAL")
    fail = in_set & ~(adx >= V17O_SHORT_D_AVWAP_MIN_ADX).fillna(False)
    dropped["D_AVWAP"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # E_VWAP_BAND_FADE: stochk >= 64.71
    in_set = setup.eq("E_VWAP_BAND_FADE")
    fail = in_set & ~(stochk >= V17O_SHORT_E_VWAP_MIN_STOCHK).fillna(False)
    dropped["E_VWAP"] = int(fail.sum())
    drop_mask = drop_mask | fail

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {s}" for s, cnt in dropped.items() if cnt > 0)
    print(f"[V17O_FILTER] SHORT top-cut filters: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


# ---------------------------------------------------------------------------
# Wire post-scan: v17n runs first (which runs v17m, codex, dedup), then v17o.
# ---------------------------------------------------------------------------
_v17n_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17n_get_filter_reason       = _base.get_v16_filter_reason


def _v17o_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17n_apply_post_scan_filters(short_df, long_df)
    if not V17O_FILTERS_ENABLED:
        return short_df, long_df
    long_df = _v17o_apply_long_filters(long_df)
    short_df = _v17o_apply_short_filters(short_df)
    return short_df, long_df


def _v17o_get_filter_reason(row: dict, side: str):
    reason = _v17n_get_filter_reason(row, side)
    if reason is not None:
        return reason
    if not V17O_FILTERS_ENABLED:
        return None

    setup = str(row.get("setup", "")).upper().strip()
    side_u = str(side).upper().strip()

    def _f(c):
        try: return float(row.get(c, float("nan")))
        except (TypeError, ValueError): return float("nan")

    if side_u == "LONG":
        if setup == "A_MOD_BREAK_C1_HIGH":
            v = _f("atr_pct_signal")
            if not (np.isfinite(v) and v >= V17O_LONG_A_MOD_MIN_ATR_PCT):
                return f"v17o LONG A_MOD top-cut: atr_pct={v:.4f} (need >= {V17O_LONG_A_MOD_MIN_ATR_PCT})"
        elif setup == "B_AVWAP_RECLAIM_REVERSAL":
            v = _f("nifty_rel_strength_pct")
            if not (np.isfinite(v) and v >= V17O_LONG_B_AVWAP_MIN_NIFTY_RS):
                return f"v17o LONG B_AVWAP top-cut: nrs={v:.3f} (need >= {V17O_LONG_B_AVWAP_MIN_NIFTY_RS})"
        elif setup == "B_HUGE_C1_CLOSE_RECLAIM_BREAK":
            v = _f("rsi_signal")
            if not (np.isfinite(v) and v >= V17O_LONG_B_HUGE_MIN_RSI):
                return f"v17o LONG B_HUGE top-cut: rsi={v:.2f} (need >= {V17O_LONG_B_HUGE_MIN_RSI})"
        elif setup == "C_OR_BREAKOUT":
            v = _f("entry_bar_vol_ratio")
            if not (np.isfinite(v) and v <= V17O_LONG_C_OR_MAX_VOL_RATIO):
                return f"v17o LONG C_OR top-cut: vol={v:.2f} (need <= {V17O_LONG_C_OR_MAX_VOL_RATIO})"
        elif setup == "E_VWAP_BAND_FADE":
            v = _f("nifty_rel_strength_pct")
            if not (np.isfinite(v) and v >= V17O_LONG_E_VWAP_MIN_NIFTY_RS):
                return f"v17o LONG E_VWAP top-cut: nrs={v:.3f} (need >= {V17O_LONG_E_VWAP_MIN_NIFTY_RS})"

    if side_u == "SHORT":
        if setup == "C_OR_BREAKDOWN":
            v = _f("quality_score")
            if not (np.isfinite(v) and v <= V17O_SHORT_C_OR_MAX_QS):
                return f"v17o SHORT C_OR top-cut: qs={v:.3f} (need <= {V17O_SHORT_C_OR_MAX_QS})"
        elif setup == "D_AVWAP_LOSE_REVERSAL":
            v = _f("adx_signal")
            if not (np.isfinite(v) and v >= V17O_SHORT_D_AVWAP_MIN_ADX):
                return f"v17o SHORT D_AVWAP top-cut: adx={v:.2f} (need >= {V17O_SHORT_D_AVWAP_MIN_ADX})"
        elif setup == "E_VWAP_BAND_FADE":
            v = _f("stochk_signal")
            if not (np.isfinite(v) and v >= V17O_SHORT_E_VWAP_MIN_STOCHK):
                return f"v17o SHORT E_VWAP top-cut: stochk={v:.2f} (need >= {V17O_SHORT_E_VWAP_MIN_STOCHK})"

    return None


_base._apply_v16_post_scan_filters = _v17o_apply_post_scan_filters
_base.get_v16_filter_reason = _v17o_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17o 5-min runner: Option A surgical per-setup top-cut filters on top of v17n")
    print("  Output dir: outputs_v17o_5min")
    print(f"--- Filters enabled = {V17O_FILTERS_ENABLED} ---")
    print(f"  LONG.A_MOD_BREAK_C1_HIGH      : atr_pct >= {V17O_LONG_A_MOD_MIN_ATR_PCT}")
    print(f"  LONG.B_AVWAP_RECLAIM_REVERSAL : nrs >= {V17O_LONG_B_AVWAP_MIN_NIFTY_RS}")
    print(f"  LONG.B_HUGE_C1_RECLAIM_BREAK  : rsi >= {V17O_LONG_B_HUGE_MIN_RSI}")
    print(f"  LONG.C_OR_BREAKOUT            : vol <= {V17O_LONG_C_OR_MAX_VOL_RATIO}")
    print(f"  LONG.E_VWAP_BAND_FADE         : nrs >= {V17O_LONG_E_VWAP_MIN_NIFTY_RS}")
    print(f"  SHORT.C_OR_BREAKDOWN          : qs <= {V17O_SHORT_C_OR_MAX_QS}")
    print(f"  SHORT.D_AVWAP_LOSE_REVERSAL   : adx >= {V17O_SHORT_D_AVWAP_MIN_ADX}")
    print(f"  SHORT.E_VWAP_BAND_FADE        : stochk >= {V17O_SHORT_E_VWAP_MIN_STOCHK}")
    print("--- Inherits v17n / v17m / v17k / ... behavior ---")
    print("=" * 78)
    _base.main()
