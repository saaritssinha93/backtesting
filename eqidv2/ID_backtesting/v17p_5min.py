# -*- coding: utf-8 -*-
"""
V17p 5-min combined runner — Stages 1 + 2 on top of v17o.

Stage 1 (codex's 3 light filters, post-scan):
  LONG.A_MOD_CCB             : rsi_signal <= 82.3 AND quality_score <= 8.26
  SHORT.A_MOD_BREAK_C1_LOW   : stochk_signal >= 3.65
  SHORT.G_LOWER_LOW_BREAK    : atr_pct_signal <= 0.0085

Stage 2 (per-setup position sizing, applied as post-scan pnl scaling):
  Each trade's pnl_pct, pnl_pct_gross, pnl_rs, position_size_rs, and
  notional_exposure_rs are multiplied by the setup's size_multiplier.
  This is mathematically equivalent to deploying N x normal capital on
  the trade in the live executor — same per-trade ROI, scaled rupee P&L
  and aggregate-portfolio impact.

Tiers (calibrated from v17o per-setup PF):
  Elite     (PF > 4):    E_VWAP_BAND_FADE                            1.50x
  Excellent (PF 2.6-3.5): B_AVWAP_RECLAIM_REVERSAL, C_OR_BREAKDOWN,
                          B_HUGE_C1_RECLAIM, D_AVWAP_LOSE_REVERSAL,
                          A_MOD_BREAK_C1_LOW                         1.25x
  Strong    (PF 2.3-2.7): A_MOD_BREAK_C1_HIGH, C_OR_BREAKOUT,
                          D_EMA20_BOUNCE, A_MOD_CCB                  1.10x
  Good      (PF 2.0-2.3): G_HIGHER_HIGH_BREAK                        1.00x
  Marginal  (PF < 2.0):  D_EMA20_REJECTION, G_LOWER_LOW_BREAK        0.50x

All env-toggleable. Outputs go to outputs_v17p_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

from . import v17o_5min as _v17o  # cascade
from . import v16_5min as _base


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
# V17p env toggles.
# ---------------------------------------------------------------------------
V17P_STAGE1_ENABLED = _env_bool("EQIDV17P_STAGE1_ENABLED", True)
V17P_STAGE2_ENABLED = _env_bool("EQIDV17P_STAGE2_ENABLED", True)

# Stage 0 — one-ticker-per-day enforcement (drops 2nd+ trades on same ticker
# same side same date; keeps highest-priority setup). Default ON in v17p.
V17P_ONE_TICKER_PER_DAY = _env_bool("EQIDV17P_ONE_TICKER_PER_DAY", True)

# Override v17n dedup window from 10min default to 30min so it catches
# sequential same-ticker same-side trades within a 30-min span.
V17P_DEDUP_WINDOW_MIN = int(_env_float("EQIDV17P_DEDUP_WINDOW_MIN", 30))
from . import v17n_5min as _v17n_mod
_v17n_mod.V17N_DEDUP_WINDOW_MIN = int(V17P_DEDUP_WINDOW_MIN)

# Stage 1 thresholds
V17P_LONG_A_CCB_MAX_RSI    = _env_float("EQIDV17P_LONG_A_CCB_MAX_RSI", 82.3)
V17P_LONG_A_CCB_MAX_QS     = _env_float("EQIDV17P_LONG_A_CCB_MAX_QS", 8.26)
V17P_SHORT_A_MOD_MIN_STOCHK = _env_float("EQIDV17P_SHORT_A_MOD_MIN_STOCHK", 3.65)
V17P_SHORT_G_LL_MAX_ATR_PCT = _env_float("EQIDV17P_SHORT_G_LL_MAX_ATR_PCT", 0.0085)

# Stage 2 size multipliers (calibrated from v17o per-setup PF)
SIZE_MULTIPLIERS = {
    # Elite (PF > 4)
    "E_VWAP_BAND_FADE":                 1.50,
    # Excellent (PF 2.6-3.5)
    "B_AVWAP_RECLAIM_REVERSAL":         1.25,
    "C_OR_BREAKDOWN":                   1.25,
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":    1.25,
    "D_AVWAP_LOSE_REVERSAL":            1.25,
    "A_MOD_BREAK_C1_LOW":               1.25,
    # Strong (PF 2.3-2.7)
    "A_MOD_BREAK_C1_HIGH":              1.10,
    "C_OR_BREAKOUT":                    1.10,
    "D_EMA20_BOUNCE":                   1.10,
    "A_MOD_CLOSE_CONTINUATION_BREAK":   1.10,
    # Good (PF 2.0-2.3)
    "G_HIGHER_HIGH_BREAK":              1.00,
    # Marginal (PF < 2.0)
    "D_EMA20_REJECTION":                0.50,
    "G_LOWER_LOW_BREAK":                0.50,
}
SIZE_DEFAULT = 1.00


# ---------------------------------------------------------------------------
# Output dir routing.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17p = _base.runtime_dir


def _v17p_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17p_5min", "v17o_5min", "v17n_5min", "v17m_5min", "v17l_5min",
            "v17k_5min", "v17j_5min", "v17i_5min", "v17h_5min", "v17g_5min",
            "v17f_5min", "v17d_5min", "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17p_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17p(*tuple(new_parts))


_base.runtime_dir = _v17p_runtime_dir


# ---------------------------------------------------------------------------
# Stage 1: codex's 3 additional light filters.
# ---------------------------------------------------------------------------
def _num(work: pd.DataFrame, col: str) -> pd.Series:
    if col not in work.columns:
        return pd.Series(np.nan, index=work.index, dtype="float64")
    return pd.to_numeric(work[col], errors="coerce")


def _v17p_apply_stage1_long(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df is None or long_df.empty or "setup" not in long_df.columns:
        return long_df
    work = long_df.copy()
    setup = work["setup"].astype(str).str.upper().str.strip()
    rsi = _num(work, "rsi_signal")
    qs = _num(work, "quality_score")

    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    # A_MOD_CCB: rsi <= 82.3 AND qs <= 8.26 (cap overheated signals)
    in_set = setup.eq("A_MOD_CLOSE_CONTINUATION_BREAK")
    cond = (rsi <= V17P_LONG_A_CCB_MAX_RSI) & (qs <= V17P_LONG_A_CCB_MAX_QS)
    fail = in_set & ~cond.fillna(False)
    dropped["A_CCB"] = int(fail.sum())
    drop_mask = drop_mask | fail

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {s}" for s, cnt in dropped.items() if cnt > 0)
    print(f"[V17P_STAGE1] LONG light filters: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


def _v17p_apply_stage1_short(short_df: pd.DataFrame) -> pd.DataFrame:
    if short_df is None or short_df.empty or "setup" not in short_df.columns:
        return short_df
    work = short_df.copy()
    setup = work["setup"].astype(str).str.upper().str.strip()
    stochk = _num(work, "stochk_signal")
    atr_pct = _num(work, "atr_pct_signal")

    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    # A_MOD_BREAK_C1_LOW: stochk >= 3.65
    in_set = setup.eq("A_MOD_BREAK_C1_LOW")
    fail = in_set & ~(stochk >= V17P_SHORT_A_MOD_MIN_STOCHK).fillna(False)
    dropped["A_MOD"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # G_LOWER_LOW_BREAK: atr_pct <= 0.0085 (drop high-vol noisy bars)
    in_set = setup.eq("G_LOWER_LOW_BREAK")
    fail = in_set & ~(atr_pct <= V17P_SHORT_G_LL_MAX_ATR_PCT).fillna(False)
    dropped["G_LL"] = int(fail.sum())
    drop_mask = drop_mask | fail

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {s}" for s, cnt in dropped.items() if cnt > 0)
    print(f"[V17P_STAGE1] SHORT light filters: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


# ---------------------------------------------------------------------------
# Stage 2: per-setup position sizing — scale pnl/position columns by mult.
#
# This is the math-equivalent of "wire sizing into executor" without
# changing the executor: each trade's actual fill happened with normal
# capital, but in the portfolio book we account for it as if N x capital
# had been deployed. PF/MaxDD/sumPnL are computed on the scaled book.
# ---------------------------------------------------------------------------
def _v17p_apply_stage2_sizing(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or "setup" not in df.columns:
        return df

    work = df.copy()
    setup = work["setup"].astype(str).str.upper().str.strip()
    mult = setup.map(SIZE_MULTIPLIERS).fillna(SIZE_DEFAULT).astype(float)

    # Always overwrite size_multiplier column with v17p value
    work["size_multiplier"] = mult.values

    # Scale pnl-related columns
    for col in ("pnl_pct", "pnl_pct_gross", "pnl_pct_price", "pnl_pct_gross_price",
                "pnl_rs", "pnl_rs_gross", "position_size_rs", "notional_exposure_rs"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce") * mult.values

    # Per-setup mult breakdown for log
    counts = mult.value_counts().sort_index().to_dict()
    summary = ", ".join(f"{k:.2f}x:{v}" for k, v in counts.items())
    print(f"[V17P_STAGE2] {side_label} per-setup sizing applied: n={len(work)} ({summary})")
    return work


# ---------------------------------------------------------------------------
# Stage 0 — One-ticker-per-day enforcement.
# Group by (trade_date, ticker, side); keep only the highest-priority setup.
# Tiebreak: quality_score desc, then signal_time_ist asc.
# ---------------------------------------------------------------------------
def _v17p_apply_one_ticker_per_day(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or "setup" not in df.columns or "ticker" not in df.columns:
        return df
    if "trade_date" not in df.columns:
        return df

    work = df.copy()
    setup_norm = work["setup"].astype(str).str.upper().str.strip()
    # Reuse v17n's SETUP_PRIORITY ladder
    work["_v17p_priority"] = setup_norm.map(_v17n_mod.SETUP_PRIORITY).fillna(0).astype(int)
    work["_v17p_qs"] = _num(work, "quality_score").fillna(0.0)
    ts_col = "signal_time_ist" if "signal_time_ist" in work.columns else "entry_time_ist"
    work["_v17p_ts"] = pd.to_datetime(work[ts_col], errors="coerce")
    work["_v17p_orig_idx"] = np.arange(len(work))

    # Sort so the row to keep per (date, ticker) is the first one
    work = work.sort_values(
        by=["trade_date", "ticker", "_v17p_priority", "_v17p_qs", "_v17p_ts", "_v17p_orig_idx"],
        ascending=[True, True, False, False, True, True],
        kind="mergesort",
    )
    keep = ~work.duplicated(subset=["trade_date", "ticker"], keep="first")
    before = len(work)
    work = work.loc[keep].copy()
    after = len(work)

    work = work.sort_values(by="_v17p_orig_idx", kind="mergesort")
    work = work.drop(columns=["_v17p_priority", "_v17p_qs", "_v17p_ts", "_v17p_orig_idx"])

    print(f"[V17P_STAGE0] {side_label} one-ticker-per-day: {before}->{after} (-{before-after})")
    return work


# ---------------------------------------------------------------------------
# Wire post-scan: v17o runs first (which runs v17n + v17m chain),
# then v17p stage 1 + stage 2.
# ---------------------------------------------------------------------------
_v17o_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17o_get_filter_reason       = _base.get_v16_filter_reason


def _v17p_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17o_apply_post_scan_filters(short_df, long_df)

    # Stage 1: light additional filters
    if V17P_STAGE1_ENABLED:
        long_df = _v17p_apply_stage1_long(long_df)
        short_df = _v17p_apply_stage1_short(short_df)

    # Stage 0 (runs late, after all filtering, before sizing): enforce
    # one-ticker-per-day. Note: name "Stage 0" is conceptual (it's a structural
    # cut, not a feature filter), but it must execute after Stage 1 so we
    # pick the survivor from the filtered pool.
    if V17P_ONE_TICKER_PER_DAY:
        long_df = _v17p_apply_one_ticker_per_day(long_df, "LONG")
        short_df = _v17p_apply_one_ticker_per_day(short_df, "SHORT")

    # Stage 2: per-setup position sizing (must run AFTER all filtering)
    if V17P_STAGE2_ENABLED:
        long_df = _v17p_apply_stage2_sizing(long_df, "LONG")
        short_df = _v17p_apply_stage2_sizing(short_df, "SHORT")

    return short_df, long_df


def _v17p_get_filter_reason(row: dict, side: str):
    reason = _v17o_get_filter_reason(row, side)
    if reason is not None:
        return reason
    if not V17P_STAGE1_ENABLED:
        return None

    setup = str(row.get("setup", "")).upper().strip()
    side_u = str(side).upper().strip()

    def _f(c):
        try: return float(row.get(c, float("nan")))
        except (TypeError, ValueError): return float("nan")

    if side_u == "LONG":
        if setup == "A_MOD_CLOSE_CONTINUATION_BREAK":
            rsi = _f("rsi_signal"); qs = _f("quality_score")
            if not (np.isfinite(rsi) and rsi <= V17P_LONG_A_CCB_MAX_RSI):
                return f"v17p LONG A_CCB: rsi={rsi:.2f} (need <= {V17P_LONG_A_CCB_MAX_RSI})"
            if not (np.isfinite(qs) and qs <= V17P_LONG_A_CCB_MAX_QS):
                return f"v17p LONG A_CCB: qs={qs:.2f} (need <= {V17P_LONG_A_CCB_MAX_QS})"
    if side_u == "SHORT":
        if setup == "A_MOD_BREAK_C1_LOW":
            v = _f("stochk_signal")
            if not (np.isfinite(v) and v >= V17P_SHORT_A_MOD_MIN_STOCHK):
                return f"v17p SHORT A_MOD: stochk={v:.2f} (need >= {V17P_SHORT_A_MOD_MIN_STOCHK})"
        elif setup == "G_LOWER_LOW_BREAK":
            v = _f("atr_pct_signal")
            if not (np.isfinite(v) and v <= V17P_SHORT_G_LL_MAX_ATR_PCT):
                return f"v17p SHORT G_LL: atr_pct={v:.4f} (need <= {V17P_SHORT_G_LL_MAX_ATR_PCT})"
    return None


_base._apply_v16_post_scan_filters = _v17p_apply_post_scan_filters
_base.get_v16_filter_reason = _v17p_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17p 5-min runner: Stage 1 (light filters) + Stage 2 (per-setup sizing) on v17o")
    print("  + Stage 0 (one-ticker-per-day) + widened v17n dedup window")
    print("  Output dir: outputs_v17p_5min")
    print(f"--- Stage 0 (one-ticker-per-day) enabled = {V17P_ONE_TICKER_PER_DAY} ---")
    print(f"  v17n dedup window widened to {V17P_DEDUP_WINDOW_MIN} min")
    print(f"--- Stage 1 (codex light filters) enabled = {V17P_STAGE1_ENABLED} ---")
    print(f"  LONG.A_MOD_CCB           : rsi <= {V17P_LONG_A_CCB_MAX_RSI} AND qs <= {V17P_LONG_A_CCB_MAX_QS}")
    print(f"  SHORT.A_MOD_BREAK_C1_LOW : stochk >= {V17P_SHORT_A_MOD_MIN_STOCHK}")
    print(f"  SHORT.G_LOWER_LOW_BREAK  : atr_pct <= {V17P_SHORT_G_LL_MAX_ATR_PCT}")
    print(f"--- Stage 2 (per-setup sizing) enabled = {V17P_STAGE2_ENABLED} ---")
    for setup, mult in sorted(SIZE_MULTIPLIERS.items(), key=lambda x: -x[1]):
        print(f"  {setup:<35} -> {mult:.2f}x")
    print("--- Inherits v17o / v17n / v17m / ... behavior ---")
    print("=" * 78)
    _base.main()
