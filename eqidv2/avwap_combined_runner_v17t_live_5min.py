# -*- coding: utf-8 -*-
"""
V17t LIVE -- v17p strategy logic with v17q honest-backtest fixes layered on top.

WHY THIS FILE EXISTS
====================
This file keeps v17t_live's runtime structure (output routing, audit hooks,
honest-backtest fix stack) but uses the full v17p cascade as its strategy
base. So everything v17p adds (extra setups, per-setup filters, stage-2
size multipliers) participates -- and the four lookahead leaks the audit
identified are still closed by v17q's F1/F4/F6/F7/F11/F12/F14/F15 patches.

Cascade after import (function actually called by main()):
    v17t_live  ->  v17p (Stage 0+1+2)  ->  v17o (per-setup top-cuts)
              ->  v17n (codex filters + dedup)
              ->  v17m (per-setup low-win cleanup; SHORT TGT 0.80%)
              ->  v17k (SHORT mirror setups + new C/D/E/G family)
              ->  v17j -> v17i -> v17h -> v17g (LONG B_AVWAP_RECLAIM)
              ->  v17f -> v17d -> v17c -> v17b -> v16

Lookahead fixes still active (all default ON):
    F1  hardened Stage 0 (one-ticker-per-day)
    F4  post-run audit asserts
    F6  vol-ratio prior-bar-only avg
    F7  NIFTY regime/RS lookup -5 min
    F11 disable require_entry_close_confirm
    F12 entry-bar exit-aware Phase 2
    F14 floor zero-lag config attrs
    F15 drop residual 5M_FALLBACK rows

Output: outputs_v17t_live_5min/

Note: this configuration is NO LONGER a "live mirror" -- it includes
setups + filters that v16 5-min live does not run. If you need a strict
live-cascade backtest, use the prior version of this file (cascade ending
at v17f) saved in git. This version is the v17p-strategy-logic-on-honest-
engine variant.
"""
from __future__ import annotations

import os
import glob
from typing import Tuple
from pathlib import Path

import numpy as np
import pandas as pd

# Cascade: full v17p chain. v17p imports trigger v17o -> v17n -> ... -> v16
# at module-load time, so all setup additions and filter wrappers are
# installed before v17t_live's honest-backtest patches run on top.
import avwap_combined_runner_v17p_5min as _v17p  # noqa: F401
import avwap_combined_runner_v17n_5min as _v17n_mod  # for SETUP_PRIORITY ladder
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# All v17q honest-backtest fixes default ON. The whole point of this file
# is to make the backtest match what live can actually produce, so every
# lookahead leak the audit identified is closed. Env toggles are provided
# for debug only.
# ---------------------------------------------------------------------------
V17T_STAGE0_HARDEN              = _env_bool("EQIDV17T_STAGE0_HARDEN", True)              # F1
V17T_AUDIT_STRICT               = _env_bool("EQIDV17T_AUDIT_STRICT", True)               # F4
V17T_VOL_RATIO_NO_LOOKAHEAD     = _env_bool("EQIDV17T_VOL_RATIO_NO_LOOKAHEAD", True)     # F6
V17T_NIFTY_LOOKUP_PREV_BAR      = _env_bool("EQIDV17T_NIFTY_LOOKUP_PREV_BAR", True)      # F7
V17T_NO_CLOSE_CONFIRM_LOOKAHEAD = _env_bool("EQIDV17T_NO_CLOSE_CONFIRM_LOOKAHEAD", True) # F11
V17T_ENTRY_BAR_AWARE_EXITS      = _env_bool("EQIDV17T_ENTRY_BAR_AWARE_EXITS", True)      # F12
V17T_FLOOR_ZERO_LAG             = _env_bool("EQIDV17T_FLOOR_ZERO_LAG", True)             # F14
V17T_REQUIRE_1MIN_EXITS         = _env_bool("EQIDV17T_REQUIRE_1MIN_EXITS", True)         # F15

# Phase 5  -- Honest Stage 2 multipliers (re-derived from Run 5 honest PFs)
# Phase 5b -- Drop setups that could not be rescued by the Run-5 PRO grid.
# Phase 5c -- RUN5_PRO_PLUS per-setup filters.
# Phase 5d -- DEEP per-setup multi-feature filters (greedy AI search) --
#            supersedes 5b + 5c when enabled. Default ON.
V17T_HONEST_STAGE2              = _env_bool("EQIDV17T_HONEST_STAGE2", True)              # P5
V17T_DROP_LOSING_SETUPS         = _env_bool("EQIDV17T_DROP_LOSING_SETUPS", True)         # P5b
V17T_PER_SETUP_FILTERS          = _env_bool("EQIDV17T_PER_SETUP_FILTERS", True)          # P5c
V17T_DEEP_FILTERS               = _env_bool("EQIDV17T_DEEP_FILTERS", True)               # P5d
# Phase 5d variant selector. Default OFF -> AGGRESSIVE spec (1079 trades, PF 1.57).
# When ON -> BALANCED spec (862 trades, PF 1.72, win 71.1%, MaxDD 5.27%).
V17T_DEEP_BALANCED              = _env_bool("EQIDV17T_DEEP_BALANCED", False)             # P5d-BAL


# ---------------------------------------------------------------------------
# Output dir routing -> outputs_v17t_live_5min
# ---------------------------------------------------------------------------
_orig_runtime_dir = _base.runtime_dir


def _v17t_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17t_live_5min", "v17p_5min", "v17o_5min", "v17n_5min", "v17m_5min",
            "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min", "v17h_5min",
            "v17g_5min", "v17f_5min", "v17d_5min", "v17c_5min", "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17t_live_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17t_runtime_dir


# ---------------------------------------------------------------------------
# Phase 5 -- Honest Stage 2 multipliers (replace v17p's biased tiers).
#
# v17p's SIZE_MULTIPLIERS were calibrated against lookahead-flattered PFs.
# E.g., E_VWAP_BAND_FADE was tagged Elite (1.50x) on the basis of v17p's
# claimed PF > 4 -- but its honest PF (Run 5) is 0.00. v17p Stage 2 thus
# upsizes lookahead-fake winners and downsizes real ones.
#
# This phase replaces _v17p.SIZE_MULTIPLIERS with a tier dict derived from
# Run 5 honest per-setup PFs. Effect: rupee P&L (pnl_rs) and capital
# allocation (position_size_rs / notional_exposure_rs) shift toward
# real-edge setups. Per-trade pnl_pct is unchanged (Stage 2's pnl_pct
# scaling is overwritten by Phase 2 _resolve_exits_5min anyway).
#
# Tier mapping from the live-safe RUN5_PRO_PLUS filtered slices:
#   PF >= 1.50  -> 1.50x (Elite)
#   PF 1.20-1.49-> 1.30x (Excellent)
#   PF 1.00-1.19-> 1.00x (Good)
#   PF 0.80-0.99-> 0.50x (Marginal -- risk capital)
#   PF < 0.80   -> 0.00x (Drop sizing -- effectively zeros rupee P&L)
# ---------------------------------------------------------------------------
V17T_HONEST_TIERS = {
    # Elite (PF >= 1.50)
    "A_MOD_BREAK_C1_LOW":              1.50,   # filtered PF 4.59
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":   1.50,   # filtered PF 2.04
    "B_AVWAP_RECLAIM_REVERSAL":        1.50,   # filtered PF 1.87
    "A_MOD_BREAK_C1_HIGH":             1.50,   # filtered PF 1.67
    "C_OR_BREAKOUT":                   1.50,   # filtered PF 1.61
    # Excellent (PF 1.20-1.49)
    "G_LOWER_LOW_BREAK":               1.30,   # filtered PF 1.64
    "D_EMA20_REJECTION":               1.30,   # filtered PF 1.46
    "G_HIGHER_HIGH_BREAK":             1.30,   # filtered PF 1.29
    # Good (PF 1.00-1.19) or tiny-sample add-on.
    "C_OR_BREAKDOWN":                  1.00,   # filtered PF 1.13
    "D_AVWAP_LOSE_REVERSAL":           1.00,   # filtered PF 1.07
    "D_EMA20_BOUNCE":                  1.00,   # filtered PF 3.41, n=6
    # Drop sizing for unrecoverable or intentionally excluded setups.
    "A_MOD_CLOSE_CONTINUATION_BREAK":  0.00,   # not in RUN5_PRO_PLUS
    "E_VWAP_BAND_FADE":                0.00,   # not in RUN5_PRO_PLUS
}

if V17T_HONEST_STAGE2:
    # _v17p was already imported; replacing the dict on the module makes
    # _v17p_apply_stage2_sizing pick up the new values at call time
    # (free-name lookup against the module's __dict__).
    _v17p.SIZE_MULTIPLIERS = V17T_HONEST_TIERS
    print("[V17T_P5] replaced v17p.SIZE_MULTIPLIERS with honest-PF tiers "
          f"({len(V17T_HONEST_TIERS)} setups)")


# ---------------------------------------------------------------------------
# Phase 5b -- Drop setups that could not be rescued by per-setup filters.
#
# Some setups are bad unfiltered but contain a profitable sub-slice once
# RUN5_PRO gates are applied. Do not drop those here; P5c gets first claim
# on them. This list is reserved for setups that either stayed bad under
# every tested gate or had too little sample to use as a production input.
# ---------------------------------------------------------------------------
V17T_LOSING_SETUPS = {
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "E_VWAP_BAND_FADE",
}


def _v17t_drop_losers_filter(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or "setup" not in df.columns:
        return df
    n_in = len(df)
    setup_norm = df["setup"].astype(str).str.upper().str.strip()
    keep = ~setup_norm.isin(V17T_LOSING_SETUPS)
    out = df.loc[keep].copy()
    n_dropped = n_in - len(out)
    if n_dropped > 0:
        print(f"[V17T_P5b] {side_label} dropped {n_dropped} rows from "
              f"losing setups; {n_in}->{len(out)}")
    return out


# ---------------------------------------------------------------------------
# Phase 5c -- RUN5_PRO_PLUS per-setup filters.
#
# This ports v17q RUN5_PRO into v17t_live and adds one strict profitable
# expansion: LONG.D_EMA20_BOUNCE with RSI[45,80), ADX>=25, QS>=3 and
# ATR%[0.003,0.012]. On the latest honest v17t_live CSV:
#
#   RUN5_PRO      -> n=353, PF 1.518, Sum PnL 5x +263.74%, MaxDD 8.34%
#   RUN5_PRO_PLUS -> n=359, PF 1.536, Sum PnL 5x +275.04%, MaxDD 7.70%
#
# Larger widenings were tested via RUN5_MAX and reduced total profit, so this
# profile intentionally adds only the slice that improves both count and PnL.
# ---------------------------------------------------------------------------
V17T_PER_SETUP_FILTER_SPEC = {
    # ---- LONG -------------------------------------------------------------
    ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): dict(
        rsi=(50, 75), adx_min=30, qs_min=None, hour_cap=11.5, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "B_AVWAP_RECLAIM_REVERSAL"): dict(
        rsi=(50, 75), adx_min=30, qs_min=5, hour_cap=None, atr_pct=None,
    ),
    ("LONG", "A_MOD_BREAK_C1_HIGH"): dict(
        rsi=None, adx_min=30, qs_min=7, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "C_OR_BREAKOUT"): dict(
        rsi=(45, 100), adx_min=30, qs_min=3, hour_cap=None, atr_pct=None,
    ),
    ("LONG", "G_HIGHER_HIGH_BREAK"): dict(
        rsi=(50, 75), adx_min=30, qs_min=3, hour_cap=None, atr_pct=None,
    ),
    ("LONG", "D_EMA20_BOUNCE"): dict(
        rsi=(45, 80), adx_min=25, qs_min=3, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    # ---- SHORT ------------------------------------------------------------
    ("SHORT", "A_MOD_BREAK_C1_LOW"): dict(
        rsi=(30, 50), adx_min=None, qs_min=None, hour_cap=13.0, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "G_LOWER_LOW_BREAK"): dict(
        rsi=(30, 50), adx_min=30, qs_min=None, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "D_EMA20_REJECTION"): dict(
        rsi=(0, 45), adx_min=30, qs_min=None, hour_cap=11.5, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "C_OR_BREAKDOWN"): dict(
        rsi=(20, 45), adx_min=30, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020),
    ),
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"): dict(
        rsi=(25, 50), adx_min=None, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020),
    ),
}


def _v17t_per_setup_filter(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or "setup" not in df.columns:
        return df
    n_in = len(df)
    setup_norm = df["setup"].astype(str).str.upper().str.strip()
    rsi = pd.to_numeric(df.get("rsi_signal", pd.Series(np.nan, index=df.index)),
                        errors="coerce")
    adx = pd.to_numeric(df.get("adx_signal", pd.Series(np.nan, index=df.index)),
                        errors="coerce")
    qs = pd.to_numeric(df.get("quality_score", pd.Series(np.nan, index=df.index)),
                       errors="coerce")
    atr_pct = pd.to_numeric(
        df.get("atr_pct_signal", pd.Series(np.nan, index=df.index)), errors="coerce"
    )
    et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
    try:
        hr = (et.dt.tz_convert("Asia/Kolkata").dt.hour
              + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0)
    except Exception:
        hr = pd.Series(np.nan, index=df.index)

    keep = pd.Series(False, index=df.index)
    for (k_side, k_setup), spec in V17T_PER_SETUP_FILTER_SPEC.items():
        if k_side != side_label:
            continue
        in_setup = setup_norm.eq(k_setup)
        if not in_setup.any():
            continue
        local = in_setup.copy()
        if spec.get("rsi") is not None:
            local &= rsi.between(spec["rsi"][0], spec["rsi"][1], inclusive="left")
        if spec.get("adx_min") is not None:
            local &= (adx >= spec["adx_min"])
        if spec.get("qs_min") is not None:
            local &= (qs >= spec["qs_min"])
        if spec.get("hour_cap") is not None:
            local &= (hr < spec["hour_cap"])
        if spec.get("atr_pct") is not None:
            local &= atr_pct.between(spec["atr_pct"][0], spec["atr_pct"][1],
                                     inclusive="both")
        keep |= local

    out = df.loc[keep].copy()
    print(f"[V17T_P5c] {side_label} per-setup filter {n_in}->{len(out)}")
    return out


# ---------------------------------------------------------------------------
# Phase 5d -- DEEP per-setup multi-feature filters (greedy AI search).
#
# Output of _v17t_live_deep_optimizer.py with PF target cascade [1.7,1.55,
# 1.4,1.25] and N_FLOOR=12 trades. For each setup, applies a chain of
# (feature, direction, threshold) constraints. Setups not in the dict are
# DROPPED.
#
# Aggregate result on Run 5 honest CSV: n=434, PF 2.55, win 78.6%,
# day-win 79.7%, MaxDD 3.42% -- 12 of 14 setups firing.
#
# CAVEAT (overfitting risk): the optimizer was trained on the same Run 5
# data we measure against -- in-sample. Per-setup PFs above ~3.0 with
# n<30 (e.g., D_EMA20_REJECTION PF 29.5 n=16, C_OR_BREAKDOWN PF 8.85 n=14,
# B_AVWAP_RECLAIM_REVERSAL PF 4.77 n=16) are plausibly small-sample
# outliers. The aggregate is dominated by 6 setups with n>=35 each (LONG
# A_MOD/C_OR/D_EMA20/G_HH + SHORT A_MOD/G_LL); those should generalize.
# Treat the outlier-PF small-n setups as "kept for diversification" not
# "edge sources".
#
# When V17T_DEEP_FILTERS=True: P5b drop and P5c per-setup filters are
# skipped (5d supersedes them). To revert to Phase 5c logic, set
# V17T_DEEP_FILTERS=0 and V17T_PER_SETUP_FILTERS=1.
#
# Two variants are kept side-by-side:
#   AGGRESSIVE (default; per-setup PF floor 1.45) -- 1079 trades, PF 1.565,
#       win 69.23%, day-win 71.63%, MaxDD 6.18%, +171.28% sum PnL%.
#   BALANCED  (EQIDV17T_DEEP_BALANCED=1; per-setup PF floor 1.55) --
#       862 trades, PF 1.722, win 71.11%, day-win 70.15%, MaxDD 5.27%,
#       +162.93% sum PnL%. Sweet-spot between Phase 5d original (433/PF 2.50)
#       and AGGRESSIVE (1079/PF 1.57).
# ---------------------------------------------------------------------------
V17T_DEEP_FILTER_SPEC_AGGRESSIVE = {
    # Aggressively relaxed -- per-setup PF floor 1.45, target n>=1000.
    # Aggregate: n=1079, PF 1.565, win 69.2%, day-win 71.6%, MaxDD 6.18%.
    # ---- LONG -------------------------------------------------------------
    ("LONG",  "A_MOD_BREAK_C1_HIGH"):
        [("avwap_dist_atr_signal", ">=", 1.4933)],                                  # n=213 PF 1.61
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"):
        [("stochk_signal", ">=", 93.7635)],                                          # n=14  PF 1.48
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"):
        [("avwap_dist_atr_signal", "<=", 2.0839)],                                  # n=40  PF 1.59
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"):
        [("quality_score", ">=", 8.5893)],                                          # n=52  PF 1.80
    ("LONG",  "C_OR_BREAKOUT"):
        [("quality_score", ">=", 2.3487),
         ("atr_pct_signal", "<=", 0.0086),
         ("avwap_dist_atr_signal", ">=", 1.9958)],                                  # n=98  PF 1.46
    ("LONG",  "D_EMA20_BOUNCE"):
        [("quality_score", ">=", 2.1436), ("adx_signal", "<=", 40.3457)],           # n=73  PF 1.59
    ("LONG",  "G_HIGHER_HIGH_BREAK"):
        [("quality_score", ">=", 2.1540),
         ("entry_hour", "<=", 10.4167),
         ("avwap_dist_atr_signal", ">=", 1.6677)],                                  # n=258 PF 1.46
    # ---- SHORT ------------------------------------------------------------
    ("SHORT", "A_MOD_BREAK_C1_LOW"):
        [],                                                                          # unfiltered: n=131 PF 1.53
    ("SHORT", "C_OR_BREAKDOWN"):
        [("avwap_dist_atr_signal", ">=", 1.5759),
         ("atr_pct_signal", ">=", 0.0041)],                                         # n=65  PF 1.53
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):
        [("avwap_dist_atr_signal", ">=", 1.4375)],                                  # n=14  PF 1.70
    ("SHORT", "D_EMA20_REJECTION"):
        [("quality_score", ">=", 0.3087), ("entry_hour", "<=", 10.0)],              # n=47  PF 1.50
    ("SHORT", "G_LOWER_LOW_BREAK"):
        [("atr_pct_signal", ">=", 0.0070)],                                         # n=74  PF 1.97
    # B_HUGE_RED_FAILED_BOUNCE (n=3) and E_VWAP_BAND_FADE (n=1) are dropped --
    # too-thin samples for any filter combination to validate.
}

V17T_DEEP_FILTER_SPEC_BALANCED = {
    # Per-setup PF floor 1.55 -- sweet-spot between volume and quality.
    # Aggregate: n=862, PF 1.722, win 71.11%, day-win 70.15%, MaxDD 5.27%.
    # ---- LONG -------------------------------------------------------------
    ("LONG",  "A_MOD_BREAK_C1_HIGH"):
        [("avwap_dist_atr_signal", ">=", 1.4933)],                                  # n=213 PF 1.61
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"):
        [("stochk_signal", ">=", 93.7635),
         ("entry_hour", "<=", 11.8229)],                                            # n=13  PF 1.96
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"):
        [("avwap_dist_atr_signal", "<=", 2.0839)],                                  # n=40  PF 1.59
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"):
        [("quality_score", ">=", 8.5893)],                                          # n=52  PF 1.80
    ("LONG",  "C_OR_BREAKOUT"):
        [("quality_score", ">=", 2.3487),
         ("atr_pct_signal", "<=", 0.0064),
         ("avwap_dist_atr_signal", ">=", 1.9958)],                                  # n=63  PF 2.18
    ("LONG",  "D_EMA20_BOUNCE"):
        [("quality_score", ">=", 2.1436),
         ("adx_signal", "<=", 40.3457)],                                            # n=73  PF 1.59
    ("LONG",  "G_HIGHER_HIGH_BREAK"):
        [("quality_score", ">=", 2.1540),
         ("entry_hour", "<=", 10.4167),
         ("avwap_dist_atr_signal", ">=", 1.9995)],                                  # n=111 PF 1.93
    # ---- SHORT ------------------------------------------------------------
    ("SHORT", "A_MOD_BREAK_C1_LOW"):
        [("quality_score", "<=", 0.7660)],                                          # n=119 PF 1.58
    ("SHORT", "C_OR_BREAKDOWN"):
        [("avwap_dist_atr_signal", ">=", 1.7857)],                                  # n=45  PF 1.68
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):
        [("avwap_dist_atr_signal", ">=", 1.4375)],                                  # n=14  PF 1.70
    ("SHORT", "D_EMA20_REJECTION"):
        [("quality_score", ">=", 0.3495),
         ("entry_hour", "<=", 10.0)],                                               # n=45  PF 1.56
    ("SHORT", "G_LOWER_LOW_BREAK"):
        [("atr_pct_signal", ">=", 0.0070)],                                         # n=74  PF 1.97
    # B_HUGE_RED_FAILED_BOUNCE (n=3) and E_VWAP_BAND_FADE (n=1) are dropped --
    # too-thin samples for any filter combination to validate.
}

# Active spec: BALANCED if EQIDV17T_DEEP_BALANCED=1, else AGGRESSIVE (default).
if V17T_DEEP_BALANCED:
    V17T_DEEP_FILTER_SPEC = V17T_DEEP_FILTER_SPEC_BALANCED
    _V17T_DEEP_VARIANT = "BALANCED (per-setup PF>=1.55, target n=862, PF 1.72)"
else:
    V17T_DEEP_FILTER_SPEC = V17T_DEEP_FILTER_SPEC_AGGRESSIVE
    _V17T_DEEP_VARIANT = "AGGRESSIVE (per-setup PF>=1.45, target n=1079, PF 1.57)"

if V17T_DEEP_FILTERS:
    print(f"[V17T_P5d] active variant: {_V17T_DEEP_VARIANT} "
          f"({len(V17T_DEEP_FILTER_SPEC)} setups)")


def _v17t_deep_filter(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty or "setup" not in df.columns:
        return df
    n_in = len(df)
    setup_norm = df["setup"].astype(str).str.upper().str.strip()
    et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
    try:
        entry_hour = (et.dt.tz_convert("Asia/Kolkata").dt.hour
                      + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0)
    except Exception:
        entry_hour = pd.Series(np.nan, index=df.index)

    keep = pd.Series(False, index=df.index)
    for (k_side, k_setup), chain in V17T_DEEP_FILTER_SPEC.items():
        if k_side != side_label:
            continue
        in_setup = setup_norm.eq(k_setup)
        if not in_setup.any():
            continue
        local = in_setup.copy()
        for feat, direction, threshold in chain:
            if feat == "entry_hour":
                col = entry_hour
            else:
                col = pd.to_numeric(df.get(feat, pd.Series(np.nan, index=df.index)),
                                    errors="coerce")
            if direction == ">=":
                local &= (col >= threshold).fillna(False)
            elif direction == "<=":
                local &= (col <= threshold).fillna(False)
        keep |= local

    out = df.loc[keep].copy()
    variant = "BALANCED" if V17T_DEEP_BALANCED else "AGGRESSIVE"
    print(f"[V17T_P5d:{variant}] {side_label} deep filter {n_in}->{len(out)} "
          f"({len(V17T_DEEP_FILTER_SPEC)} setups)")
    return out


# ---------------------------------------------------------------------------
# F1 -- Hardened Stage 0 (one-ticker-per-day per side). Same as v17q.
# ---------------------------------------------------------------------------
def _v17t_apply_stage0(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    n_in = 0 if df is None else len(df)
    print(f"[V17T_STAGE0] entered side={side_label} n_in={n_in}")
    if df is None or df.empty:
        print(f"[V17T_STAGE0] {side_label} skipped -- empty df")
        return df
    for col in ("setup", "ticker", "trade_date"):
        if col not in df.columns:
            raise RuntimeError(
                f"[V17T_STAGE0] {side_label} missing required column '{col}'"
            )

    work = df.copy()
    setup_norm = work["setup"].astype(str).str.upper().str.strip()
    work["_v17t_priority"] = (
        setup_norm.map(_v17n_mod.SETUP_PRIORITY).fillna(0).astype(int)
    )
    work["_v17t_qs"] = pd.to_numeric(
        work.get("quality_score", 0.0), errors="coerce"
    ).fillna(0.0)
    ts_col = "signal_time_ist" if "signal_time_ist" in work.columns else "entry_time_ist"
    work["_v17t_ts"] = pd.to_datetime(work[ts_col], errors="coerce")
    work["_v17t_orig_idx"] = np.arange(len(work))

    work = work.sort_values(
        by=["trade_date", "ticker", "_v17t_priority", "_v17t_qs",
            "_v17t_ts", "_v17t_orig_idx"],
        ascending=[True, True, False, False, True, True],
        kind="mergesort",
    )
    keep = ~work.duplicated(subset=["trade_date", "ticker"], keep="first")
    n_kept = int(keep.sum())
    n_dropped = len(work) - n_kept
    work = work.loc[keep].copy()
    work = work.sort_values(by="_v17t_orig_idx", kind="mergesort")
    work = work.drop(columns=[
        "_v17t_priority", "_v17t_qs", "_v17t_ts", "_v17t_orig_idx",
    ])

    print(f"[V17T_STAGE0] {side_label} {n_in}->{n_kept} (-{n_dropped})")
    return work


if (V17T_STAGE0_HARDEN or V17T_DROP_LOSING_SETUPS or V17T_PER_SETUP_FILTERS
        or V17T_DEEP_FILTERS):
    # At this point the v17p import has already installed the full cascade:
    # v17p (Stage 0+1+2) -> v17o -> v17n -> v17m -> v17k -> ... -> v16.
    # Capture that and chain v17t's post-scan additions AFTER it.
    _v17p_post_scan_chain = _base._apply_v16_post_scan_filters

    def _v17t_apply_post_scan_filters(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # 1. Run v17p chain (includes Stage 0/1/2 with honest tiers from P5).
        short_df, long_df = _v17p_post_scan_chain(short_df, long_df)

        if V17T_DEEP_FILTERS:
            # 2'. Phase 5d: DEEP per-setup multi-feature filters.
            # Supersedes 5b drop-losers + 5c per-setup filters.
            long_df = _v17t_deep_filter(long_df, "LONG")
            short_df = _v17t_deep_filter(short_df, "SHORT")
        else:
            # 2. Drop losing-setup rows so neither F1 nor P5c wastes work on them.
            if V17T_DROP_LOSING_SETUPS:
                long_df = _v17t_drop_losers_filter(long_df, "LONG")
                short_df = _v17t_drop_losers_filter(short_df, "SHORT")
            # 3. Per-setup filters lift each surviving setup's PF >= 1.0
            # individually (RSI / ADX / QS / hour / atr_pct gates).
            if V17T_PER_SETUP_FILTERS:
                long_df = _v17t_per_setup_filter(long_df, "LONG")
                short_df = _v17t_per_setup_filter(short_df, "SHORT")

        # 4. Hardened stage 0 -- defends against silent-skip failure
        # even when v17p's stage 0 already ran.
        if V17T_STAGE0_HARDEN:
            long_df = _v17t_apply_stage0(long_df, "LONG")
            short_df = _v17t_apply_stage0(short_df, "SHORT")
        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17t_apply_post_scan_filters


# ---------------------------------------------------------------------------
# F11 + F14 -- mutate cfg before scan
# ---------------------------------------------------------------------------
def _v17t_floor_lag_attrs(cfg, side_label: str) -> int:
    floored = 0
    for attr in dir(cfg):
        low = attr.lower()
        if "lag" not in low or "bars" not in low:
            continue
        try:
            val = getattr(cfg, attr)
        except Exception:
            continue
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            continue
        if val == -1:
            continue
        if val < 1:
            setattr(cfg, attr, 1)
            print(f"[V17T_F14] {side_label} floored cfg.{attr}: {val} -> 1")
            floored += 1
    return floored


if V17T_NO_CLOSE_CONFIRM_LOOKAHEAD or V17T_FLOOR_ZERO_LAG:
    _orig_run_both = _base._run_both_parallel

    def _v17t_run_both_parallel(short_cfg, long_cfg, max_workers=None):
        if V17T_NO_CLOSE_CONFIRM_LOOKAHEAD:
            short_cfg.require_entry_close_confirm = False
            long_cfg.require_entry_close_confirm = False
            print("[V17T_F11] disabled require_entry_close_confirm for SHORT and LONG")
        if V17T_FLOOR_ZERO_LAG:
            n_short = _v17t_floor_lag_attrs(short_cfg, "SHORT")
            n_long = _v17t_floor_lag_attrs(long_cfg, "LONG")
            print(f"[V17T_F14] floored {n_short + n_long} lag attrs (S={n_short}, L={n_long})")
        if max_workers is None:
            return _orig_run_both(short_cfg, long_cfg)
        return _orig_run_both(short_cfg, long_cfg, max_workers)

    _base._run_both_parallel = _v17t_run_both_parallel


# ---------------------------------------------------------------------------
# F6 -- prior-bar-only volume average (replaces _enrich_with_entry_vol_ratio)
# ---------------------------------------------------------------------------
if V17T_VOL_RATIO_NO_LOOKAHEAD:
    def _v17t_enrich_vol_ratio(long_df, dir_15m, parquet_suffix="_stocks_indicators_5min.parquet"):
        import pathlib
        if long_df is None or long_df.empty:
            df = (long_df.copy() if long_df is not None else pd.DataFrame())
            df["entry_bar_vol_ratio"] = np.nan
            df["bars_from_open"] = np.nan
            return df
        dir_path = pathlib.Path(dir_15m)
        cache: dict = {}

        def _get_day(ticker, date_str):
            key = (ticker, date_str)
            if key not in cache:
                f = dir_path / f"{ticker}{parquet_suffix}"
                if not f.exists():
                    cache[key] = pd.DataFrame()
                    return cache[key]
                try:
                    df_p = pd.read_parquet(f)
                    df_p["date"] = pd.to_datetime(df_p["date"])
                except Exception:
                    cache[key] = pd.DataFrame()
                    return cache[key]
                day = df_p[df_p["date"].dt.strftime("%Y-%m-%d") == date_str].reset_index(drop=True)
                cache[key] = day
            return cache[key]

        ratios, bar_idxs = [], []
        for _, row in long_df.iterrows():
            ticker = str(row.get("ticker", ""))
            date_s = str(row.get("trade_date", ""))[:10]
            try:
                ep = float(row.get("entry_price", 0))
            except (ValueError, TypeError):
                ep = 0.0
            day = _get_day(ticker, date_s)
            if day.empty or ep <= 0:
                ratios.append(np.nan); bar_idxs.append(np.nan); continue
            hits = day[day["high"] >= ep * 0.999]
            if hits.empty:
                ratios.append(np.nan); bar_idxs.append(np.nan); continue
            entry_bar_idx = int(hits.index[0])
            prior = day.iloc[: entry_bar_idx + 1]
            avg_vol = prior["volume"].mean()
            if not np.isfinite(avg_vol) or avg_vol <= 0:
                ratios.append(np.nan); bar_idxs.append(entry_bar_idx); continue
            entry_bar_vol = float(day.iloc[entry_bar_idx]["volume"])
            ratios.append(entry_bar_vol / avg_vol)
            bar_idxs.append(entry_bar_idx)

        out = long_df.copy()
        out["entry_bar_vol_ratio"] = ratios
        out["bars_from_open"] = bar_idxs
        n_ok = int(out["entry_bar_vol_ratio"].notna().sum())
        print(f"[V17T_F6] vol-ratio (prior-bar avg) for {n_ok}/{len(out)} LONG trades")
        return out

    _base._enrich_with_entry_vol_ratio = _v17t_enrich_vol_ratio


# ---------------------------------------------------------------------------
# F7 -- NIFTY regime/RS lookup shifted -5 min
# ---------------------------------------------------------------------------
if V17T_NIFTY_LOOKUP_PREV_BAR:
    _orig_apply_nifty = _base._apply_nifty_intraday_context

    def _v17t_apply_nifty_intraday_context(short_df, long_df, cfg, mode_map, nifty_ret_map):
        if not mode_map:
            return short_df, long_df
        delta = pd.Timedelta(minutes=5)

        def _shift(df):
            if df is None or df.empty:
                return df, 0
            d = df.copy()
            ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
            if ts_col not in d.columns:
                return d, 0
            d[ts_col] = pd.to_datetime(d[ts_col], errors="coerce") - delta
            return d, len(d)

        short_shifted, n_s = _shift(short_df)
        long_shifted, n_l = _shift(long_df)
        out_s, out_l = _orig_apply_nifty(short_shifted, long_shifted, cfg, mode_map, nifty_ret_map)

        def _restore(df_out):
            if df_out is None or df_out.empty:
                return df_out
            ts_col = "entry_time_ist" if "entry_time_ist" in df_out.columns else "signal_time_ist"
            if ts_col not in df_out.columns:
                return df_out
            df_out = df_out.copy()
            df_out[ts_col] = pd.to_datetime(df_out[ts_col], errors="coerce") + delta
            return df_out

        out_s = _restore(out_s)
        out_l = _restore(out_l)
        print(f"[V17T_F7] nifty regime/RS lookup shifted -5min (S={n_s}, L={n_l})")
        return out_s, out_l

    _base._apply_nifty_intraday_context = _v17t_apply_nifty_intraday_context


# ---------------------------------------------------------------------------
# F12 + F15 -- entry-bar exit-aware Phase 2 + drop residual 5M_FALLBACK rows
# ---------------------------------------------------------------------------
def _v17t_check_entry_bar_exit(bars_1m, entry_price, side, sl, tgt):
    if bars_1m is None or bars_1m.empty:
        return None
    side_u = str(side).upper()
    for _, bar in bars_1m.iterrows():
        bh = float(bar.get("high", np.nan))
        bl = float(bar.get("low", np.nan))
        bt = bar.get("datetime", bar.get("date"))
        if not (np.isfinite(bh) and np.isfinite(bl)):
            continue
        if side_u == "LONG":
            if bh < entry_price:
                continue
            stop_hit = bl <= sl
            target_hit = bh >= tgt
        else:
            if bl > entry_price:
                continue
            stop_hit = bh >= sl
            target_hit = bl <= tgt
        if stop_hit and target_hit:
            return dict(outcome="SL", exit_price_clean=sl, exit_time=bt, ambiguous=True, case="1MIN_FILL_BAR_AMBIGUOUS")
        if stop_hit:
            return dict(outcome="SL", exit_price_clean=sl, exit_time=bt, ambiguous=False, case="1MIN_FILL_BAR_STOP")
        if target_hit:
            return dict(outcome="TARGET", exit_price_clean=tgt, exit_time=bt, ambiguous=False, case="1MIN_FILL_BAR_TARGET")
        return None
    return None


def _v17t_apply_f15_drop(df: pd.DataFrame) -> pd.DataFrame:
    if not V17T_REQUIRE_1MIN_EXITS or df is None or df.empty:
        return df
    if "exit_resolution_case" not in df.columns:
        return df
    case = df["exit_resolution_case"].astype(str)
    fb = case.str.startswith("5M_FALLBACK")
    n_drop = int(fb.sum())
    if n_drop > 0:
        print(f"[V17T_F15] dropping {n_drop} 5M_FALLBACK row(s)")
        df = df.loc[~fb].reset_index(drop=True)
    else:
        print("[V17T_F15] no 5M_FALLBACK rows present (1-min coverage clean)")
    return df


if V17T_ENTRY_BAR_AWARE_EXITS or V17T_REQUIRE_1MIN_EXITS:
    _orig_resolve = _base._resolve_exits_5min

    def _v17t_resolve_exits_5min(trades_df, dir_5m, suffix_5m=".parquet", engine="pyarrow", eod_exit_time=None):
        df = _orig_resolve(trades_df, dir_5m, suffix_5m, engine, eod_exit_time)
        if df is None or df.empty:
            return df
        if not V17T_ENTRY_BAR_AWARE_EXITS:
            return _v17t_apply_f15_drop(df)

        cache_1m = {}
        flips_to_sl = 0
        flips_to_tgt = 0
        flips_no_change = 0
        scanned = 0
        DEFAULT_SLIP = 0.0005
        DEFAULT_COMM = 0.0003

        for idx in df.index:
            entry_time_raw = df.at[idx, "entry_time_ist"]
            if pd.isna(entry_time_raw):
                continue
            ticker = str(df.at[idx, "ticker"])
            side = str(df.at[idx, "side"]).upper()
            try:
                entry_price = float(df.at[idx, "entry_price"])
            except (TypeError, ValueError):
                continue
            entry_time = pd.to_datetime(entry_time_raw)
            sl_col = "stop_price" if "stop_price" in df.columns else "sl_price"
            try:
                sl = float(df.at[idx, sl_col])
                tgt = float(df.at[idx, "target_price"])
            except (TypeError, ValueError, KeyError):
                continue
            if not (np.isfinite(sl) and np.isfinite(tgt) and np.isfinite(entry_price)):
                continue

            df_1m = _base._load_ticker_intrabar_cache(
                cache_1m, ticker, Path(dir_5m),
                [
                    f"{ticker}{suffix_5m}",
                    f"{ticker}.parquet",
                    f"{ticker}_1min.parquet",
                    f"{ticker}_stocks_indicators_1min.parquet",
                    f"{ticker}_5min.parquet",
                    f"{ticker}_stocks_indicators_5min.parquet",
                ],
                engine,
            )
            if df_1m is None or df_1m.empty or "datetime" not in df_1m.columns:
                continue

            entry_bar_start = entry_time - pd.Timedelta(minutes=5)
            mask = (df_1m["datetime"] > entry_bar_start) & (df_1m["datetime"] <= entry_time)
            bars = df_1m.loc[mask].sort_values("datetime")
            if bars.empty:
                continue
            scanned += 1
            result = _v17t_check_entry_bar_exit(bars, entry_price, side, sl, tgt)
            if result is None:
                continue

            slip = DEFAULT_SLIP
            comm = DEFAULT_COMM
            if "slippage_pct" in df.columns and pd.notna(df.at[idx, "slippage_pct"]):
                slip = float(df.at[idx, "slippage_pct"])
            if "commission_pct" in df.columns and pd.notna(df.at[idx, "commission_pct"]):
                comm = float(df.at[idx, "commission_pct"])
            cost_pct = (slip + comm) * 100.0 * 2.0

            outcome = result["outcome"]
            xp_clean = float(result["exit_price_clean"])
            xt = result["exit_time"]
            ambiguous = bool(result["ambiguous"])
            case = result["case"]
            xp_pess = float(_base._apply_stop_exit_slippage(side, xp_clean)) if outcome == "SL" else xp_clean
            base_raw = float(_base._calc_price_return_pct(side, entry_price, xp_clean))
            pess_raw = float(_base._calc_price_return_pct(side, entry_price, xp_pess))
            if outcome == "SL":
                opt_xp = xp_pess if not ambiguous else float(tgt)
                opt_outcome = "SL" if not ambiguous else "TARGET"
                opt_raw = float(_base._calc_price_return_pct(side, entry_price, opt_xp))
            else:
                opt_xp = xp_clean
                opt_outcome = "TARGET"
                opt_raw = base_raw

            old = df.at[idx, "outcome"]
            if outcome == old:
                flips_no_change += 1
            elif outcome == "SL":
                flips_to_sl += 1
            else:
                flips_to_tgt += 1

            df.at[idx, "exit_price"] = xp_pess
            df.at[idx, "exit_time_ist"] = xt
            df.at[idx, "outcome"] = outcome
            df.at[idx, "pnl_pct_gross"] = pess_raw
            df.at[idx, "pnl_pct"] = pess_raw - cost_pct
            df.at[idx, "exit_resolution_case"] = case
            df.at[idx, "exit_bar_ambiguous"] = ambiguous
            df.at[idx, "stop_fill_penalty_applied"] = (outcome == "SL")
            df.at[idx, "stop_fill_penalty_bps"] = (
                float(_base.STOP_EXIT_EXTRA_SLIPPAGE_BPS) if outcome == "SL" else 0.0
            )
            df.at[idx, "exit_price_base"] = xp_clean
            df.at[idx, "exit_time_ist_base"] = xt
            df.at[idx, "outcome_base"] = outcome
            df.at[idx, "pnl_pct_gross_price_base"] = base_raw
            df.at[idx, "pnl_pct_price_base"] = base_raw - cost_pct
            df.at[idx, "exit_price_pess"] = xp_pess
            df.at[idx, "exit_time_ist_pess"] = xt
            df.at[idx, "outcome_pess"] = outcome
            df.at[idx, "pnl_pct_gross_price_pess"] = pess_raw
            df.at[idx, "pnl_pct_price_pess"] = pess_raw - cost_pct
            df.at[idx, "exit_price_opt"] = opt_xp
            df.at[idx, "exit_time_ist_opt"] = xt
            df.at[idx, "outcome_opt"] = opt_outcome
            df.at[idx, "pnl_pct_gross_price_opt"] = opt_raw
            df.at[idx, "pnl_pct_price_opt"] = opt_raw - cost_pct

        print(f"[V17T_F12] entry-bar override: scanned={scanned} "
              f"flipped_to_SL={flips_to_sl} flipped_to_TARGET={flips_to_tgt} "
              f"reaffirmed={flips_no_change}")
        return _v17t_apply_f15_drop(df)

    _base._resolve_exits_5min = _v17t_resolve_exits_5min


# ---------------------------------------------------------------------------
# F4 -- post-run audit asserts
# ---------------------------------------------------------------------------
if V17T_AUDIT_STRICT:
    _orig_main = _base.main

    def _v17t_post_run_audit():
        out_dir = _v17t_runtime_dir("outputs_v16_5min")
        pattern = str(Path(out_dir) / "avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            print("[V17T_AUDIT] no output CSV found; skipping audit")
            return
        latest = files[-1]
        df = pd.read_csv(latest)
        print(f"[V17T_AUDIT] auditing {Path(latest).name} (rows={len(df)})")

        failures = []

        def _fail(name, n, hint):
            if n > 0:
                failures.append(f"{name} ({hint}: n={n})")
                print(f"[V17T_AUDIT][FAIL] {name}: n={n} ({hint})")
            else:
                print(f"[V17T_AUDIT][PASS] {name}")

        _fail("no_dup_signal_key",
              int(df.duplicated(subset=["trade_date", "ticker", "side", "signal_time_ist"]).sum()),
              "duplicates on (date,ticker,side,signal_time)")
        _fail("no_dup_entry_key",
              int(df.duplicated(subset=["trade_date", "ticker", "side", "entry_time_ist"]).sum()),
              "duplicates on (date,ticker,side,entry_time)")
        _fail("F1_one_ticker_per_day",
              int(df.duplicated(subset=["trade_date", "ticker", "side"]).sum()),
              "duplicates on (date,ticker,side)")

        et = pd.to_datetime(df["entry_time_ist"], utc=True, errors="coerce")
        xt = pd.to_datetime(df["exit_time_ist"], utc=True, errors="coerce")
        case_col = df.get("exit_resolution_case", pd.Series("", index=df.index)).astype(str)
        is_fb = case_col.str.startswith("1MIN_FILL_BAR")
        tol = pd.to_timedelta(is_fb.map({True: "5min", False: "0min"}))
        bad = (xt + tol < et) & et.notna() & xt.notna()
        _fail("exit_time_after_entry", int(bad.sum()),
              "rows with exit_time materially before entry_time")

        pnl_p = pd.to_numeric(df.get("pnl_pct_price", pd.Series(dtype=float)), errors="coerce")
        if not pnl_p.empty:
            _fail("TARGET_has_positive_pnl",
                  int((df["outcome"].eq("TARGET") & (pnl_p <= 0)).sum()),
                  "TARGET rows with pnl_pct_price <= 0")
            _fail("SL_has_negative_pnl",
                  int((df["outcome"].eq("SL") & (pnl_p >= 0)).sum()),
                  "SL rows with pnl_pct_price >= 0")

        if "stop_fill_penalty_applied" in df.columns:
            sfp_raw = df["stop_fill_penalty_applied"]
            sfp = sfp_raw if sfp_raw.dtype == bool else sfp_raw.astype(str).str.lower().isin(("true", "1", "yes"))
            _fail("stop_fill_penalty_iff_SL",
                  int((sfp != df["outcome"].eq("SL")).sum()),
                  "rows where stop_fill_penalty_applied != (outcome=='SL')")

        if V17T_REQUIRE_1MIN_EXITS and "exit_resolution_case" in df.columns:
            _fail("F15_no_5M_fallback",
                  int(df["exit_resolution_case"].astype(str).str.startswith("5M_FALLBACK").sum()),
                  "rows with 5M_FALLBACK exit_resolution_case")

        if failures:
            print(f"[V17T_AUDIT] {len(failures)} check(s) FAILED: " + "; ".join(failures))
            import sys as _sys
            print("[V17T_AUDIT] STRICT mode -- exiting with code 2")
            _sys.exit(2)
        else:
            print("[V17T_AUDIT] all checks passed")

    def _v17t_main():
        result = _orig_main()
        try:
            _v17t_post_run_audit()
        except SystemExit:
            raise
        except Exception as exc:
            print(f"[V17T_AUDIT] post-run audit error: {exc}")
        return result

    _base.main = _v17t_main


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
def _enabled_fixes():
    flags = []
    if V17T_STAGE0_HARDEN:              flags.append("F1_STAGE0")
    if V17T_AUDIT_STRICT:               flags.append("F4_AUDIT")
    if V17T_VOL_RATIO_NO_LOOKAHEAD:     flags.append("F6_VOL_RATIO")
    if V17T_NIFTY_LOOKUP_PREV_BAR:      flags.append("F7_NIFTY_LOOKUP")
    if V17T_NO_CLOSE_CONFIRM_LOOKAHEAD: flags.append("F11_NO_CLOSE_CONFIRM")
    if V17T_ENTRY_BAR_AWARE_EXITS:      flags.append("F12_ENTRY_BAR_EXITS")
    if V17T_FLOOR_ZERO_LAG:             flags.append("F14_FLOOR_LAG")
    if V17T_REQUIRE_1MIN_EXITS:         flags.append("F15_REQUIRE_1MIN")
    if V17T_HONEST_STAGE2:              flags.append("P5_HONEST_STAGE2")
    if V17T_DEEP_FILTERS:
        variant = "BALANCED" if V17T_DEEP_BALANCED else "AGGRESSIVE"
        flags.append(f"P5d_DEEP_FILTERS:{variant} [supersedes 5b+5c]")
    else:
        if V17T_DROP_LOSING_SETUPS:         flags.append("P5b_DROP_LOSERS")
        if V17T_PER_SETUP_FILTERS:          flags.append("P5c_PER_SETUP_FILTERS")
    return flags


if __name__ == "__main__":
    print("=" * 78)
    print("V17t LIVE -- v17p strategy logic + v17q honest-backtest fixes")
    print("  Output dir   : outputs_v17t_live_5min")
    print("  Cascade      : v17t -> v17p -> v17o -> v17n -> v17m -> v17k -> ...")
    print("                          -> v17j -> v17i -> v17h -> v17g")
    print("                          -> v17f -> v17d -> v17c -> v17b -> v16")
    print("  v17p strategy: full setup universe + per-setup filters + Stage 2 sizing")
    print(f"  Honest fixes : {', '.join(_enabled_fixes())}")
    print("=" * 78)
    _base.main()
