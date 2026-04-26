# -*- coding: utf-8 -*-
"""
V17n 5-min combined runner — codex's per-setup filters + same-bar dedup
on top of v17m.

Stage 1 (codex's 6 lite filters): applied AFTER v17m's existing per-setup
cleanup filters. These add per-setup feature constraints on indicators
that v17m doesn't filter on.

Stage 2 (same-bar dedup): when 2+ setups fire on the same (ticker, side)
within a 10-min window, keep ONLY the highest-priority setup. Cuts
correlated losers without cutting any single setup's edge.

Codex Stage-1 filters
---------------------
LONG.G_HIGHER_HIGH_BREAK    : quality_score >= 1.254
LONG.D_EMA20_BOUNCE         : nifty_rel_strength_pct >= 1.083 AND
                              entry_bar_vol_ratio <= 4.081
LONG.A_MOD_CCB              : rsi_signal >= 65.54
SHORT.G_LOWER_LOW_BREAK     : avwap_dist_atr_signal >= 1.117
SHORT.D_EMA20_REJECTION     : ema20_gap_atr_signal <= 0.941
SHORT.C_OR_BREAKDOWN        : rsi_signal <= 47.36

Stage-2 dedup priority (highest first; based on v17m post-filter PFs)
-------------------------------------------------------------------
E_VWAP_BAND_FADE > C_OR_BREAKDOWN > A_MOD_BREAK_C1_LOW > B_AVWAP_RECLAIM_REVERSAL
> B_HUGE_C1_RECLAIM > A_MOD_CCB > C_OR_BREAKOUT > D_AVWAP_LOSE_REVERSAL
> A_MOD_BREAK_C1_HIGH > D_EMA20_BOUNCE > G_HIGHER_HIGH_BREAK
> D_EMA20_REJECTION > G_LOWER_LOW_BREAK

Each toggle independently env-controlled.

Outputs go to outputs_v17n_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17m_5min as _v17m  # cascade
import avwap_combined_runner_v16_5min as _base


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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return int(default)
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return int(default)


# ---------------------------------------------------------------------------
# V17n env toggles.
# ---------------------------------------------------------------------------
V17N_CODEX_FILTERS_ENABLED = _env_bool("EQIDV17N_CODEX_FILTERS_ENABLED", True)
V17N_DEDUP_ENABLED         = _env_bool("EQIDV17N_DEDUP_ENABLED", True)
V17N_DEDUP_WINDOW_MIN      = _env_int("EQIDV17N_DEDUP_WINDOW_MIN", 10)  # group signals within N min

# Codex Stage-1 thresholds
V17N_LONG_G_HH_MIN_QS              = _env_float("EQIDV17N_LONG_G_HH_MIN_QS", 1.254)
V17N_LONG_D_EMA_MIN_NIFTY_RS       = _env_float("EQIDV17N_LONG_D_EMA_MIN_NIFTY_RS", 1.083)
V17N_LONG_D_EMA_MAX_VOL_RATIO      = _env_float("EQIDV17N_LONG_D_EMA_MAX_VOL_RATIO", 4.081)
V17N_LONG_A_CCB_MIN_RSI            = _env_float("EQIDV17N_LONG_A_CCB_MIN_RSI", 65.54)
V17N_SHORT_G_LL_MIN_AVWAP_DIST     = _env_float("EQIDV17N_SHORT_G_LL_MIN_AVWAP_DIST", 1.117)
V17N_SHORT_D_EMA_MAX_EMA20_GAP     = _env_float("EQIDV17N_SHORT_D_EMA_MAX_EMA20_GAP", 0.941)
V17N_SHORT_C_OR_MAX_RSI            = _env_float("EQIDV17N_SHORT_C_OR_MAX_RSI", 47.36)


# Stage-2 priority ladder (higher = preferred when same-bar collision)
SETUP_PRIORITY = {
    "E_VWAP_BAND_FADE":               100,
    "C_OR_BREAKDOWN":                  90,
    "A_MOD_BREAK_C1_LOW":              88,
    "B_AVWAP_RECLAIM_REVERSAL":        85,
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":   80,
    "A_MOD_CLOSE_CONTINUATION_BREAK":  75,
    "C_OR_BREAKOUT":                   70,
    "D_AVWAP_LOSE_REVERSAL":           68,
    "A_MOD_BREAK_C1_HIGH":             65,
    "D_EMA20_BOUNCE":                  60,
    "G_HIGHER_HIGH_BREAK":             55,
    "D_EMA20_REJECTION":               40,
    "G_LOWER_LOW_BREAK":               30,
}


# ---------------------------------------------------------------------------
# Output dir routing.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17n = _base.runtime_dir


def _v17n_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17n_5min", "v17m_5min", "v17l_5min", "v17k_5min", "v17j_5min",
            "v17i_5min", "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min",
            "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17n_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17n(*tuple(new_parts))


_base.runtime_dir = _v17n_runtime_dir


# ---------------------------------------------------------------------------
# Stage 1: Codex's per-setup filters (post-scan, applied after v17m's).
# ---------------------------------------------------------------------------
def _num(work: pd.DataFrame, col: str) -> pd.Series:
    if col not in work.columns:
        return pd.Series(np.nan, index=work.index, dtype="float64")
    return pd.to_numeric(work[col], errors="coerce")


def _v17n_apply_codex_long(long_df: pd.DataFrame) -> pd.DataFrame:
    if long_df is None or long_df.empty or "setup" not in long_df.columns:
        return long_df
    work = long_df.copy()
    setup = work["setup"].astype(str).str.upper().str.strip()

    qs = _num(work, "quality_score")
    nrs = _num(work, "nifty_rel_strength_pct")
    vol = _num(work, "entry_bar_vol_ratio")
    rsi = _num(work, "rsi_signal")

    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    # G_HIGHER_HIGH_BREAK: qs >= 1.254
    in_set = setup.eq("G_HIGHER_HIGH_BREAK")
    fail = in_set & ~(qs >= V17N_LONG_G_HH_MIN_QS).fillna(False)
    dropped["G_HH"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # D_EMA20_BOUNCE: nifty_rs >= 1.083 AND vol <= 4.081
    in_set = setup.eq("D_EMA20_BOUNCE")
    cond = (nrs >= V17N_LONG_D_EMA_MIN_NIFTY_RS) & (vol <= V17N_LONG_D_EMA_MAX_VOL_RATIO)
    fail = in_set & ~cond.fillna(False)
    dropped["D_EMA"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # A_MOD_CCB: rsi >= 65.54
    in_set = setup.eq("A_MOD_CLOSE_CONTINUATION_BREAK")
    fail = in_set & ~(rsi >= V17N_LONG_A_CCB_MIN_RSI).fillna(False)
    dropped["A_CCB"] = int(fail.sum())
    drop_mask = drop_mask | fail

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {s}" for s, cnt in dropped.items() if cnt > 0)
    print(f"[V17N_CODEX] LONG codex filters: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


def _v17n_apply_codex_short(short_df: pd.DataFrame) -> pd.DataFrame:
    if short_df is None or short_df.empty or "setup" not in short_df.columns:
        return short_df
    work = short_df.copy()
    setup = work["setup"].astype(str).str.upper().str.strip()

    avwap = _num(work, "avwap_dist_atr_signal")
    ema_gap = _num(work, "ema20_gap_atr_signal")
    rsi = _num(work, "rsi_signal")

    drop_mask = pd.Series(False, index=work.index)
    dropped = {}

    # G_LOWER_LOW_BREAK: avwap_dist >= 1.117
    in_set = setup.eq("G_LOWER_LOW_BREAK")
    fail = in_set & ~(avwap >= V17N_SHORT_G_LL_MIN_AVWAP_DIST).fillna(False)
    dropped["G_LL"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # D_EMA20_REJECTION: ema20_gap <= 0.941
    in_set = setup.eq("D_EMA20_REJECTION")
    fail = in_set & ~(ema_gap <= V17N_SHORT_D_EMA_MAX_EMA20_GAP).fillna(False)
    dropped["D_EMA"] = int(fail.sum())
    drop_mask = drop_mask | fail

    # C_OR_BREAKDOWN: rsi <= 47.36
    in_set = setup.eq("C_OR_BREAKDOWN")
    fail = in_set & ~(rsi <= V17N_SHORT_C_OR_MAX_RSI).fillna(False)
    dropped["C_OR"] = int(fail.sum())
    drop_mask = drop_mask | fail

    before = len(work)
    work = work.loc[~drop_mask].copy()
    details = ", ".join(f"-{cnt} {s}" for s, cnt in dropped.items() if cnt > 0)
    print(f"[V17N_CODEX] SHORT codex filters: {before}->{len(work)} ({details if details else 'no drops'})")
    return work


# ---------------------------------------------------------------------------
# Stage 2: Same-bar dedup. Group by (ticker, side, signal_bar floor),
# keep only the highest-priority setup per group. Tiebreak by quality_score
# descending, then by signal_time_ist ascending.
# ---------------------------------------------------------------------------
def _v17n_dedup_one_side(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or "setup" not in df.columns or "ticker" not in df.columns:
        return df

    ts_col = None
    for c in ("signal_time_ist", "entry_time_ist", "entry_time"):
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        # No timestamp column to bucket on — skip dedup safely
        return df

    work = df.copy()
    ts = pd.to_datetime(work[ts_col], errors="coerce")
    win_min = max(int(V17N_DEDUP_WINDOW_MIN), 1)
    work["_v17n_bucket"] = ts.dt.floor(f"{win_min}min")

    setup_norm = work["setup"].astype(str).str.upper().str.strip()
    work["_v17n_priority"] = setup_norm.map(SETUP_PRIORITY).fillna(0).astype(int)
    qs_series = _num(work, "quality_score").fillna(0.0)
    work["_v17n_qs"] = qs_series
    # Stable tiebreak: original index
    work["_v17n_orig_idx"] = np.arange(len(work))

    # Sort so the row to keep per group is the first one
    work = work.sort_values(
        by=["ticker", "_v17n_bucket", "_v17n_priority", "_v17n_qs", "_v17n_orig_idx"],
        ascending=[True, True, False, False, True],
        kind="mergesort",
    )
    keep = ~work.duplicated(subset=["ticker", "_v17n_bucket"], keep="first")
    before = len(work)
    work = work.loc[keep].copy()
    after = len(work)

    # Restore original ordering and clean helpers
    work = work.sort_values(by="_v17n_orig_idx", kind="mergesort")
    work = work.drop(columns=["_v17n_bucket", "_v17n_priority", "_v17n_qs", "_v17n_orig_idx"])

    print(f"[V17N_DEDUP] {side_label} same-bar dedup ({win_min}min window): {before}->{after} (-{before-after})")
    return work


# ---------------------------------------------------------------------------
# Wire post-scan: v17m runs first, then codex, then dedup.
# ---------------------------------------------------------------------------
_v17m_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17m_get_filter_reason       = _base.get_v16_filter_reason


def _v17n_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17m_apply_post_scan_filters(short_df, long_df)

    if V17N_CODEX_FILTERS_ENABLED:
        long_df = _v17n_apply_codex_long(long_df)
        short_df = _v17n_apply_codex_short(short_df)

    if V17N_DEDUP_ENABLED:
        long_df = _v17n_dedup_one_side(long_df, "LONG")
        short_df = _v17n_dedup_one_side(short_df, "SHORT")

    return short_df, long_df


def _v17n_get_filter_reason(row: dict, side: str):
    reason = _v17m_get_filter_reason(row, side)
    if reason is not None:
        return reason
    if not V17N_CODEX_FILTERS_ENABLED:
        return None

    setup = str(row.get("setup", "")).upper().strip()
    side_u = str(side).upper().strip()

    def _f(c):
        try: return float(row.get(c, float("nan")))
        except (TypeError, ValueError): return float("nan")

    if side_u == "LONG":
        if setup == "G_HIGHER_HIGH_BREAK":
            qs = _f("quality_score")
            if not (np.isfinite(qs) and qs >= V17N_LONG_G_HH_MIN_QS):
                return f"v17n LONG G_HH codex: qs={qs:.3f} (need >= {V17N_LONG_G_HH_MIN_QS})"
        elif setup == "D_EMA20_BOUNCE":
            nrs = _f("nifty_rel_strength_pct"); vol = _f("entry_bar_vol_ratio")
            if not (np.isfinite(nrs) and nrs >= V17N_LONG_D_EMA_MIN_NIFTY_RS):
                return f"v17n LONG D_EMA codex: nrs={nrs:.3f} (need >= {V17N_LONG_D_EMA_MIN_NIFTY_RS})"
            if not (np.isfinite(vol) and vol <= V17N_LONG_D_EMA_MAX_VOL_RATIO):
                return f"v17n LONG D_EMA codex: vol={vol:.3f} (need <= {V17N_LONG_D_EMA_MAX_VOL_RATIO})"
        elif setup == "A_MOD_CLOSE_CONTINUATION_BREAK":
            rsi = _f("rsi_signal")
            if not (np.isfinite(rsi) and rsi >= V17N_LONG_A_CCB_MIN_RSI):
                return f"v17n LONG A_CCB codex: rsi={rsi:.2f} (need >= {V17N_LONG_A_CCB_MIN_RSI})"

    if side_u == "SHORT":
        if setup == "G_LOWER_LOW_BREAK":
            avwap = _f("avwap_dist_atr_signal")
            if not (np.isfinite(avwap) and avwap >= V17N_SHORT_G_LL_MIN_AVWAP_DIST):
                return f"v17n SHORT G_LL codex: avwap_dist={avwap:.3f} (need >= {V17N_SHORT_G_LL_MIN_AVWAP_DIST})"
        elif setup == "D_EMA20_REJECTION":
            eg = _f("ema20_gap_atr_signal")
            if not (np.isfinite(eg) and eg <= V17N_SHORT_D_EMA_MAX_EMA20_GAP):
                return f"v17n SHORT D_EMA codex: ema20_gap={eg:.3f} (need <= {V17N_SHORT_D_EMA_MAX_EMA20_GAP})"
        elif setup == "C_OR_BREAKDOWN":
            rsi = _f("rsi_signal")
            if not (np.isfinite(rsi) and rsi <= V17N_SHORT_C_OR_MAX_RSI):
                return f"v17n SHORT C_OR codex: rsi={rsi:.2f} (need <= {V17N_SHORT_C_OR_MAX_RSI})"

    return None


_base._apply_v16_post_scan_filters = _v17n_apply_post_scan_filters
_base.get_v16_filter_reason = _v17n_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17n 5-min runner: codex stage-1 filters + same-bar dedup on top of v17m")
    print("  Output dir: outputs_v17n_5min")
    print(f"--- Stage 1 (codex per-setup filters) enabled = {V17N_CODEX_FILTERS_ENABLED} ---")
    print(f"  LONG.G_HH: qs >= {V17N_LONG_G_HH_MIN_QS}")
    print(f"  LONG.D_EMA20_BOUNCE: nrs >= {V17N_LONG_D_EMA_MIN_NIFTY_RS}, vol <= {V17N_LONG_D_EMA_MAX_VOL_RATIO}")
    print(f"  LONG.A_MOD_CCB: rsi >= {V17N_LONG_A_CCB_MIN_RSI}")
    print(f"  SHORT.G_LL: avwap_dist >= {V17N_SHORT_G_LL_MIN_AVWAP_DIST}")
    print(f"  SHORT.D_EMA20_REJECTION: ema20_gap <= {V17N_SHORT_D_EMA_MAX_EMA20_GAP}")
    print(f"  SHORT.C_OR_BREAKDOWN: rsi <= {V17N_SHORT_C_OR_MAX_RSI}")
    print(f"--- Stage 2 (same-bar dedup) enabled = {V17N_DEDUP_ENABLED}, window = {V17N_DEDUP_WINDOW_MIN}min ---")
    print("--- Inherits all v17m / v17k / v17j / v17i / v17h / v17g / v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
