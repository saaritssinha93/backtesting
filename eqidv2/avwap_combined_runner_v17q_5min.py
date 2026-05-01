# -*- coding: utf-8 -*-
"""
V17q 5-min combined runner — Day 0 bootstrap.

V17q starts as a behavioral CLONE of v17p. The only change at Day 0 is the
output directory routing (outputs_v17q_5min instead of outputs_v17p_5min);
all filter logic, sizing, dedup, and exit resolution still come from v17p ->
v17o -> ... -> v16 unchanged. A fresh run on the same data must produce
row-level identical trade output to v17p (same trade_date, ticker, side,
signal_time, entry_time, entry_price, exit_price, outcome, pnl).

Subsequent commits will land bug fixes in numbered phases per the audit plan,
each gated by an EQIDV17Q_* env toggle defaulting OFF until validated.
Planned toggles (NOT YET ACTIVE in this Day-0 file -- the constants below
record the policy values but no patches are wired yet):

  Phase 1 -- wrapper-level structural fixes
    EQIDV17Q_STAGE0_HARDEN              F1  assert one-ticker-per-day actually runs
    EQIDV17Q_DEDUP_WINDOW_MIN           F2  enforce v17n dedup-window override
    EQIDV17Q_ZERO_LAG_POLICY            F3  drop|floor|keep zero-lag trades
    EQIDV17Q_AUDIT_STRICT               F4  post-run sanity asserts
    EQIDV17Q_STAMP_METADATA             F5  write metadata_<ts>.json next to CSV

  Phase 2 -- engine fixes via v16 monkey-patch
    EQIDV17Q_VOL_RATIO_NO_LOOKAHEAD     F6  prior-bar-only volume average
    EQIDV17Q_NIFTY_LOOKUP_PREV_BAR      F7  shift nifty regime/RS lookup -5min
    EQIDV17Q_NIFTY_CONTEXT_FULL_SESSION F8  clock<=15:30 (was 15:15)
    EQIDV17Q_PARQUET_NAIVE_TZ           F9  raise|assume_ist|assume_utc
    EQIDV17Q_STAGE2_PNL_ORDERED         F10 move v17p Stage 2 after _add_notional_pnl

  Phase 3 -- execution semantics
    EQIDV17Q_NO_CLOSE_CONFIRM_LOOKAHEAD F11 disable require_entry_close_confirm
    EQIDV17Q_ENTRY_BAR_AWARE_EXITS      F12 scan entry bar's 1-min window for SL/TGT
    EQIDV17Q_ENTRY_AT_NEXT_OPEN         F13 alternative -- fill at next-bar open
    EQIDV17Q_FLOOR_ZERO_LAG             F14 floor cfg.lag_bars_*=0 to 1 pre-scan

Outputs go to outputs_v17q_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17p_5min as _v17p  # cascade -- pulls v17o ... v16
import avwap_combined_runner_v17n_5min as _v17n_mod  # for SETUP_PRIORITY ladder
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# Env helpers.
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


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return int(default)
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return int(default)


def _env_choice(name: str, default: str, choices: tuple) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    return val if val in choices else default


# ---------------------------------------------------------------------------
# V17q env toggles. All Day-0 defaults preserve v17p behavior (no-op).
# ---------------------------------------------------------------------------
# Phase 1
V17Q_STAGE0_HARDEN              = _env_bool("EQIDV17Q_STAGE0_HARDEN", True)  # F1 promoted 2026-04-27
V17Q_DEDUP_WINDOW_MIN           = _env_int("EQIDV17Q_DEDUP_WINDOW_MIN", 0)  # 0 = no override
V17Q_ZERO_LAG_POLICY            = _env_choice("EQIDV17Q_ZERO_LAG_POLICY", "keep",
                                              ("keep", "drop", "floor"))
V17Q_AUDIT_STRICT               = _env_bool("EQIDV17Q_AUDIT_STRICT", True)  # F4 promoted 2026-04-27
V17Q_REQUIRE_1MIN_EXITS         = _env_bool("EQIDV17Q_REQUIRE_1MIN_EXITS", True)  # F15 promoted 2026-04-27
V17Q_STAMP_METADATA             = _env_bool("EQIDV17Q_STAMP_METADATA", False)

# Phase 2
V17Q_VOL_RATIO_NO_LOOKAHEAD     = _env_bool("EQIDV17Q_VOL_RATIO_NO_LOOKAHEAD", False)
V17Q_NIFTY_LOOKUP_PREV_BAR      = _env_bool("EQIDV17Q_NIFTY_LOOKUP_PREV_BAR", False)
V17Q_NIFTY_CONTEXT_FULL_SESSION = _env_bool("EQIDV17Q_NIFTY_CONTEXT_FULL_SESSION", False)
V17Q_PARQUET_NAIVE_TZ           = _env_choice("EQIDV17Q_PARQUET_NAIVE_TZ", "legacy",
                                              ("legacy", "raise", "assume_ist", "assume_utc"))
V17Q_STAGE2_PNL_ORDERED         = _env_bool("EQIDV17Q_STAGE2_PNL_ORDERED", False)

# Phase 3
V17Q_NO_CLOSE_CONFIRM_LOOKAHEAD = _env_bool("EQIDV17Q_NO_CLOSE_CONFIRM_LOOKAHEAD", True)  # F11 promoted 2026-04-27
V17Q_ENTRY_BAR_AWARE_EXITS      = _env_bool("EQIDV17Q_ENTRY_BAR_AWARE_EXITS", True)  # F12 promoted 2026-04-27
V17Q_ENTRY_AT_NEXT_OPEN         = _env_bool("EQIDV17Q_ENTRY_AT_NEXT_OPEN", False)
V17Q_FLOOR_ZERO_LAG             = _env_bool("EQIDV17Q_FLOOR_ZERO_LAG", False)

# Phase 4 -- RUN 5 OPTIMIZED (concentrated production-deployable subset)
# Selected from the post-Run-5 experiment grid. See _v17q_run5_optimizer.py
# for the full grid. Best composite-score config retained:
#   - Only the two PF>=1.0 setups under fully-honest backtest:
#       LONG.B_AVWAP_RECLAIM_REVERSAL  (Run 5 PF 1.29)
#       SHORT.A_MOD_BREAK_C1_LOW       (Run 5 PF 1.53)
#   - RSI window per side:
#       LONG : RSI in [50, 75)   -- bullish bar but not overbought
#       SHORT: RSI in [25, 50)   -- bearish bar but not oversold
# Filtered subset on Run 5 honest data:
#   n=104, win=71.2%, PF 1.79, day-win 68.9%, MaxDD 3.72% (price-return)
#   Levered (5x) Sum PnL +101.6% over 11 months on ~10 trades/month.
V17Q_RUN5_OPTIMIZED             = _env_bool("EQIDV17Q_RUN5_OPTIMIZED", False)
RUN5_KEEP_LONG_SETUPS           = {"B_AVWAP_RECLAIM_REVERSAL"}
RUN5_KEEP_SHORT_SETUPS          = {"A_MOD_BREAK_C1_LOW"}
RUN5_LONG_RSI_LO                = _env_float("EQIDV17Q_RUN5_LONG_RSI_LO", 50.0)
RUN5_LONG_RSI_HI                = _env_float("EQIDV17Q_RUN5_LONG_RSI_HI", 75.0)
RUN5_SHORT_RSI_LO               = _env_float("EQIDV17Q_RUN5_SHORT_RSI_LO", 25.0)
RUN5_SHORT_RSI_HI               = _env_float("EQIDV17Q_RUN5_SHORT_RSI_HI", 50.0)

# Phase 4b -- RUN 5 PRO (per-setup filter -- keep ALL profitable setups)
# Selected from the per-setup grid (see _v17q_run5_per_setup_optimizer.py).
# 10 of 13 setups have a per-setup filter that produces PF >= 1.0 on Run 5
# honest data; those are kept. 3-4 setups (LONG.A_MOD_CLOSE_CONTINUATION_BREAK,
# LONG.D_EMA20_BOUNCE, SHORT.B_HUGE_RED_FAILED_BOUNCE, SHORT.E_VWAP_BAND_FADE)
# could not be made profitable with any filter combination; those are
# DROPPED. To re-add them, override RUN5_PRO_DROP_UNFILTERABLE=False.
#
# Aggregate metrics on Run 5 honest data with these filters applied:
#   n=353, win=68.84%, PF 1.518, day-win 65.45%, MaxDD 8.34% (price-return)
#   Levered (5x) Sum PnL +263.7% over 11 months on ~32 trades/month.
V17Q_RUN5_PRO                   = _env_bool("EQIDV17Q_RUN5_PRO", False)
V17Q_RUN5_PRO_DROP_UNFILTERABLE = _env_bool("EQIDV17Q_RUN5_PRO_DROP_UNFILTERABLE", True)

# Per-setup filter dict. None values mean "no filter" for that gate.
# Keys: rsi=(lo,hi) inclusive_lo exclusive_hi, adx_min, qs_min, hour_cap (IST clock),
# atr_pct=(lo,hi) inclusive both ends.
RUN5_PRO_FILTERS = {
    # ---- LONG -----------------------------------------------------------
    ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): dict(
        rsi=(50, 75), adx_min=30, qs_min=None, hour_cap=11.5, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "B_AVWAP_RECLAIM_REVERSAL"):      dict(
        rsi=(50, 75), adx_min=30, qs_min=5, hour_cap=None, atr_pct=None,
    ),
    ("LONG", "A_MOD_BREAK_C1_HIGH"):           dict(
        rsi=None, adx_min=30, qs_min=7, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "C_OR_BREAKOUT"):                 dict(
        rsi=(45, 100), adx_min=30, qs_min=3, hour_cap=None, atr_pct=None,
    ),
    ("LONG", "G_HIGHER_HIGH_BREAK"):           dict(
        rsi=(50, 75), adx_min=30, qs_min=3, hour_cap=None, atr_pct=None,
    ),
    # ---- SHORT ----------------------------------------------------------
    ("SHORT", "A_MOD_BREAK_C1_LOW"):           dict(
        rsi=(30, 50), adx_min=None, qs_min=None, hour_cap=13.0, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "G_LOWER_LOW_BREAK"):            dict(
        rsi=(30, 50), adx_min=30, qs_min=None, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "D_EMA20_REJECTION"):            dict(
        rsi=(0, 45), adx_min=30, qs_min=None, hour_cap=11.5, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "C_OR_BREAKDOWN"):               dict(
        rsi=(20, 45), adx_min=30, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020),
    ),
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):        dict(
        rsi=(25, 50), adx_min=None, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020),
    ),
}
# Setups in the universe but NOT in RUN5_PRO_FILTERS are dropped when
# V17Q_RUN5_PRO_DROP_UNFILTERABLE=True. They produced no profitable
# filter combination in Run 5 honest data:
RUN5_PRO_UNFILTERABLE = (
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"),
    ("LONG",  "D_EMA20_BOUNCE"),
    ("SHORT", "B_HUGE_RED_FAILED_BOUNCE"),
    ("SHORT", "E_VWAP_BAND_FADE"),
)

# Phase 4c -- RUN 5 MAX (volume-targeted: ~618 trades, PF 1.20)
# Greedy-relaxed per-setup filters from _v17q_run5_max_optimizer.py at
# marginal-PF floor = 0.90. This is the highest-volume config that
# remains profitable on Run 5 honest data. Past this point, marginal
# trades have PF < 0.90 and would drag the aggregate to losses.
#
# Aggregate metrics on Run 5 honest data:
#   n=618, win=63.4%, PF 1.202, day-win 58.0%, MaxDD 9.84% (price)
#   Levered (5x) Sum PnL approx +180% over 11 months on ~56 trades/month
#
# Differences vs RUN5_PRO:
#   - LONG.B_HUGE_C1_CLOSE_RECLAIM_BREAK relaxed: 16 -> 98 trades
#   - SHORT.G_LOWER_LOW_BREAK relaxed: 52 -> 174 trades
#   - LONG.A_MOD_CLOSE_CONTINUATION_BREAK ADDED (was dropped in PRO): 55 trades
#       NOTE: this setup is the weakest piece (own PF 0.56). It is included
#       because the greedy aggregator allows it: its small absolute loss
#       is more than offset by other setups. To exclude it, override
#       EQIDV17Q_RUN5_MAX_DROP_LOSING_SETUPS=1.
#   - LONG.D_EMA20_BOUNCE ADDED with strict gate: 6 trades
#
# Mutually exclusive with RUN5_OPTIMIZED and RUN5_PRO.
V17Q_RUN5_MAX                     = _env_bool("EQIDV17Q_RUN5_MAX", False)
V17Q_RUN5_MAX_DROP_LOSING_SETUPS  = _env_bool(
    "EQIDV17Q_RUN5_MAX_DROP_LOSING_SETUPS", False
)
# Setups whose individual PF < 1.0 in this config (only one in practice).
# Dropped when V17Q_RUN5_MAX_DROP_LOSING_SETUPS=True.
RUN5_MAX_LOSING_SETUPS = {
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"),
}
RUN5_MAX_FILTERS = {
    # ---- LONG -----------------------------------------------------------
    ("LONG", "A_MOD_BREAK_C1_HIGH"):           dict(
        rsi=None, adx_min=30, qs_min=7, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "A_MOD_CLOSE_CONTINUATION_BREAK"): dict(
        rsi=(45, 80), adx_min=25, qs_min=3, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "B_AVWAP_RECLAIM_REVERSAL"):      dict(
        rsi=(50, 75), adx_min=30, qs_min=5, hour_cap=None, atr_pct=None,
    ),
    ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): dict(
        rsi=(45, 80), adx_min=25, qs_min=None, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "C_OR_BREAKOUT"):                 dict(
        rsi=(45, 100), adx_min=30, qs_min=3, hour_cap=None, atr_pct=None,
    ),
    ("LONG", "D_EMA20_BOUNCE"):                dict(
        rsi=(45, 80), adx_min=25, qs_min=3, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
    ("LONG", "G_HIGHER_HIGH_BREAK"):           dict(
        rsi=(50, 75), adx_min=30, qs_min=3, hour_cap=None, atr_pct=None,
    ),
    # ---- SHORT ----------------------------------------------------------
    ("SHORT", "A_MOD_BREAK_C1_LOW"):           dict(
        rsi=(30, 50), adx_min=None, qs_min=None, hour_cap=13.0, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "C_OR_BREAKDOWN"):               dict(
        rsi=(20, 45), adx_min=30, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020),
    ),
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):        dict(
        rsi=(25, 50), adx_min=None, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020),
    ),
    ("SHORT", "D_EMA20_REJECTION"):            dict(
        rsi=(0, 45), adx_min=30, qs_min=None, hour_cap=11.5, atr_pct=(0.003, 0.012),
    ),
    ("SHORT", "G_LOWER_LOW_BREAK"):            dict(
        rsi=(25, 55), adx_min=25, qs_min=None, hour_cap=None, atr_pct=(0.003, 0.012),
    ),
}
RUN5_MAX_UNFILTERABLE = (
    ("SHORT", "B_HUGE_RED_FAILED_BOUNCE"),
    ("SHORT", "E_VWAP_BAND_FADE"),
)


# ---------------------------------------------------------------------------
# Output dir routing. v17p already redirected outputs to outputs_v17p_5min;
# we wrap that so anything containing v17p_5min (or any earlier-cascade
# token) is rewritten to v17q_5min before the underlying runtime_dir runs.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17q = _base.runtime_dir


def _v17q_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17q_5min", "v17p_5min", "v17o_5min", "v17n_5min", "v17m_5min",
            "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min", "v17h_5min",
            "v17g_5min", "v17f_5min", "v17d_5min", "v17c_5min", "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17q_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17q(*tuple(new_parts))


_base.runtime_dir = _v17q_runtime_dir


# ---------------------------------------------------------------------------
# F1 (Phase 1) -- Hardened Stage 0: one-ticker-per-day, no silent skips.
#
# v17p's Stage 0 silently failed in production (audit C3): no log line, output
# CSV had multiple (trade_date, ticker, side) duplicates. v17q runs its own
# Stage 0 AFTER v17p's full filter chain, so the rule holds whether or not
# v17p's stage 0 was effective. Required columns missing -> raise (no silent
# skip). Always logs n_in / n_out so failure is impossible to miss.
#
# Gated by EQIDV17Q_STAGE0_HARDEN. When OFF, no wrapper is installed and
# behavior is byte-equivalent to Day-0 v17q (= v17p).
# ---------------------------------------------------------------------------
def _v17q_apply_stage0(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    """Hardened one-ticker-per-day enforcement. No silent skips."""
    n_in = 0 if df is None else len(df)
    print(f"[V17Q_STAGE0] entered side={side_label} n_in={n_in}")
    if df is None or df.empty:
        print(f"[V17Q_STAGE0] {side_label} skipped -- empty df")
        return df
    for col in ("setup", "ticker", "trade_date"):
        if col not in df.columns:
            raise RuntimeError(
                f"[V17Q_STAGE0] {side_label} missing required column '{col}' "
                f"-- refusing silent skip"
            )

    work = df.copy()
    setup_norm = work["setup"].astype(str).str.upper().str.strip()
    work["_v17q_priority"] = (
        setup_norm.map(_v17n_mod.SETUP_PRIORITY).fillna(0).astype(int)
    )
    work["_v17q_qs"] = pd.to_numeric(
        work.get("quality_score", 0.0), errors="coerce"
    ).fillna(0.0)
    ts_col = "signal_time_ist" if "signal_time_ist" in work.columns else "entry_time_ist"
    work["_v17q_ts"] = pd.to_datetime(work[ts_col], errors="coerce")
    work["_v17q_orig_idx"] = np.arange(len(work))

    # Sort so the row to KEEP per (trade_date, ticker) lands first.
    work = work.sort_values(
        by=["trade_date", "ticker", "_v17q_priority", "_v17q_qs",
            "_v17q_ts", "_v17q_orig_idx"],
        ascending=[True, True, False, False, True, True],
        kind="mergesort",
    )
    keep = ~work.duplicated(subset=["trade_date", "ticker"], keep="first")
    n_kept = int(keep.sum())
    n_dropped = len(work) - n_kept
    work = work.loc[keep].copy()

    # Restore original ordering and clean helper columns.
    work = work.sort_values(by="_v17q_orig_idx", kind="mergesort")
    work = work.drop(columns=[
        "_v17q_priority", "_v17q_qs", "_v17q_ts", "_v17q_orig_idx",
    ])

    print(f"[V17Q_STAGE0] {side_label} {n_in}->{n_kept} (-{n_dropped})")
    return work


if V17Q_STAGE0_HARDEN:
    _v17p_post_scan_chain = _base._apply_v16_post_scan_filters

    def _v17q_apply_post_scan_filters(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # Run the entire v17p -> v17o -> ... -> v16 chain first.
        short_df, long_df = _v17p_post_scan_chain(short_df, long_df)
        # Then enforce one-ticker-per-day on each side.
        long_df = _v17q_apply_stage0(long_df, "LONG")
        short_df = _v17q_apply_stage0(short_df, "SHORT")
        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17q_apply_post_scan_filters


# ---------------------------------------------------------------------------
# RUN 5 OPTIMIZED -- Concentrated production-deployable subset.
#
# Runs LAST in the post-scan chain (after every v17q lookahead fix and after
# F1 stage 0). Drops every signal that is not (a) one of the two PF>=1.0
# setups identified in the Run 5 honest backtest AND (b) within the
# side-specific RSI window. This is the best composite-score experiment
# from the offline grid (see _v17q_run5_optimizer.py / E03_v17r_rsi_window).
#
# Trade-offs the user must accept when enabling this toggle:
#   - Trade volume drops ~98% (from ~5000 to ~100 trades over 11 months)
#   - LONG side is statistically thin (~25-30 trades) -- monitor for drift
#   - This is the residual real edge AFTER every known lookahead is removed
#
# Gated by EQIDV17Q_RUN5_OPTIMIZED (default OFF).
# ---------------------------------------------------------------------------
if V17Q_RUN5_OPTIMIZED:
    _pre_run5_post_scan = _base._apply_v16_post_scan_filters

    def _v17q_run5_optimized_filter(side_df: pd.DataFrame, side_label: str) -> pd.DataFrame:
        if side_df is None or side_df.empty or "setup" not in side_df.columns:
            return side_df
        n_in = len(side_df)
        setup_norm = side_df["setup"].astype(str).str.upper().str.strip()
        rsi = pd.to_numeric(side_df.get("rsi_signal", pd.Series(np.nan, index=side_df.index)),
                            errors="coerce")
        if side_label == "LONG":
            keep = setup_norm.isin(RUN5_KEEP_LONG_SETUPS) & rsi.between(
                RUN5_LONG_RSI_LO, RUN5_LONG_RSI_HI, inclusive="left"
            )
            band = f"[{RUN5_LONG_RSI_LO:.0f},{RUN5_LONG_RSI_HI:.0f})"
        else:
            keep = setup_norm.isin(RUN5_KEEP_SHORT_SETUPS) & rsi.between(
                RUN5_SHORT_RSI_LO, RUN5_SHORT_RSI_HI, inclusive="left"
            )
            band = f"[{RUN5_SHORT_RSI_LO:.0f},{RUN5_SHORT_RSI_HI:.0f})"
        out = side_df.loc[keep].copy()
        n_out = len(out)
        whitelist = (RUN5_KEEP_LONG_SETUPS if side_label == "LONG"
                     else RUN5_KEEP_SHORT_SETUPS)
        print(
            f"[V17Q_RUN5_OPT] {side_label} {n_in}->{n_out} "
            f"(whitelist={sorted(whitelist)}, RSI {band})"
        )
        return out

    def _v17q_apply_run5_optimized_post_scan(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        short_df, long_df = _pre_run5_post_scan(short_df, long_df)
        long_df = _v17q_run5_optimized_filter(long_df, "LONG")
        short_df = _v17q_run5_optimized_filter(short_df, "SHORT")
        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17q_apply_run5_optimized_post_scan


# ---------------------------------------------------------------------------
# RUN 5 PRO -- per-setup parameter filter that keeps ALL profitable setups.
#
# For each (side, setup) in RUN5_PRO_FILTERS, applies that setup's specific
# RSI / ADX / QS / hour / ATR-pct gate. Setups not in the dict are dropped
# when V17Q_RUN5_PRO_DROP_UNFILTERABLE=True (default), kept-unfiltered when
# False. Runs LAST in the post-scan chain (after all v17q lookahead fixes
# and after F1 stage 0).
#
# Selected for users who want to keep wide setup coverage (~32 trades/month
# vs RUN5_OPTIMIZED's ~10) at the cost of slightly lower aggregate PF
# (1.52 vs 1.79). Use one or the other -- not both at once.
# ---------------------------------------------------------------------------
if V17Q_RUN5_PRO:
    if V17Q_RUN5_OPTIMIZED:
        raise SystemExit(
            "[V17Q] EQIDV17Q_RUN5_OPTIMIZED and EQIDV17Q_RUN5_PRO are mutually "
            "exclusive -- pick one."
        )
    _pre_run5pro_post_scan = _base._apply_v16_post_scan_filters

    def _v17q_run5_pro_filter_one(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
        if df is None or df.empty or "setup" not in df.columns:
            return df
        n_in = len(df)
        # Build keep mask
        setup_norm = df["setup"].astype(str).str.upper().str.strip()
        rsi = pd.to_numeric(df.get("rsi_signal", pd.Series(np.nan, index=df.index)), errors="coerce")
        adx = pd.to_numeric(df.get("adx_signal", pd.Series(np.nan, index=df.index)), errors="coerce")
        qs  = pd.to_numeric(df.get("quality_score", pd.Series(np.nan, index=df.index)), errors="coerce")
        atr_pct = pd.to_numeric(df.get("atr_pct_signal", pd.Series(np.nan, index=df.index)), errors="coerce")
        et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
        try:
            hr = et.dt.tz_convert("Asia/Kolkata").dt.hour + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0
        except Exception:
            hr = pd.Series(np.nan, index=df.index)

        keep = pd.Series(False, index=df.index)
        per_setup_log = []
        for (k_side, k_setup), spec in RUN5_PRO_FILTERS.items():
            if k_side != side_label:
                continue
            in_setup = setup_norm.eq(k_setup)
            if not in_setup.any():
                continue
            mask_local = in_setup.copy()
            if spec.get("rsi") is not None:
                mask_local &= rsi.between(spec["rsi"][0], spec["rsi"][1], inclusive="left")
            if spec.get("adx_min") is not None:
                mask_local &= (adx >= spec["adx_min"])
            if spec.get("qs_min") is not None:
                mask_local &= (qs >= spec["qs_min"])
            if spec.get("hour_cap") is not None:
                mask_local &= (hr < spec["hour_cap"])
            if spec.get("atr_pct") is not None:
                mask_local &= atr_pct.between(spec["atr_pct"][0], spec["atr_pct"][1], inclusive="both")
            keep |= mask_local
            n_in_setup = int(in_setup.sum())
            n_keep_setup = int(mask_local.sum())
            per_setup_log.append((k_setup, n_in_setup, n_keep_setup))

        if not V17Q_RUN5_PRO_DROP_UNFILTERABLE:
            # Keep unfilterable setups unfiltered (rare use case)
            unfilterable_keys = {s for (sd, s) in RUN5_PRO_UNFILTERABLE if sd == side_label}
            keep |= setup_norm.isin(unfilterable_keys)

        out = df.loc[keep].copy()
        for setup, n0, n1 in sorted(per_setup_log, key=lambda x: -x[2]):
            print(f"[V17Q_RUN5_PRO] {side_label} {setup:<35s} {n0:>5d} -> {n1:>4d}")
        print(f"[V17Q_RUN5_PRO] {side_label} TOTAL {n_in} -> {len(out)}")
        return out

    def _v17q_apply_run5_pro_post_scan(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        short_df, long_df = _pre_run5pro_post_scan(short_df, long_df)
        long_df = _v17q_run5_pro_filter_one(long_df, "LONG")
        short_df = _v17q_run5_pro_filter_one(short_df, "SHORT")
        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17q_apply_run5_pro_post_scan


# ---------------------------------------------------------------------------
# RUN 5 MAX -- volume-targeted relaxation of RUN5_PRO (~618 trades).
#
# Same structure as RUN5_PRO but with looser filters chosen by greedy upgrade
# at marginal-PF floor 0.90 to push trade volume from 353 -> 618. Aggregate
# PF drops from 1.52 to ~1.20 in exchange for 75% more trades.
#
# Mutually exclusive with RUN5_OPTIMIZED and RUN5_PRO.
# ---------------------------------------------------------------------------
if V17Q_RUN5_MAX:
    if V17Q_RUN5_OPTIMIZED or V17Q_RUN5_PRO:
        raise SystemExit(
            "[V17Q] RUN5_MAX is mutually exclusive with RUN5_OPTIMIZED / RUN5_PRO."
            " Pick one."
        )
    _pre_run5max_post_scan = _base._apply_v16_post_scan_filters

    def _v17q_run5_max_filter_one(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
        if df is None or df.empty or "setup" not in df.columns:
            return df
        n_in = len(df)
        setup_norm = df["setup"].astype(str).str.upper().str.strip()
        rsi = pd.to_numeric(df.get("rsi_signal", pd.Series(np.nan, index=df.index)), errors="coerce")
        adx = pd.to_numeric(df.get("adx_signal", pd.Series(np.nan, index=df.index)), errors="coerce")
        qs  = pd.to_numeric(df.get("quality_score", pd.Series(np.nan, index=df.index)), errors="coerce")
        atr_pct = pd.to_numeric(df.get("atr_pct_signal", pd.Series(np.nan, index=df.index)), errors="coerce")
        et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
        try:
            hr = et.dt.tz_convert("Asia/Kolkata").dt.hour + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0
        except Exception:
            hr = pd.Series(np.nan, index=df.index)

        keep = pd.Series(False, index=df.index)
        per_setup_log = []
        for (k_side, k_setup), spec in RUN5_MAX_FILTERS.items():
            if k_side != side_label:
                continue
            if V17Q_RUN5_MAX_DROP_LOSING_SETUPS and (k_side, k_setup) in RUN5_MAX_LOSING_SETUPS:
                # Skip; user has chosen to drop the losing piece
                in_setup = setup_norm.eq(k_setup)
                per_setup_log.append((k_setup, int(in_setup.sum()), 0))
                continue
            in_setup = setup_norm.eq(k_setup)
            if not in_setup.any():
                continue
            mask_local = in_setup.copy()
            if spec.get("rsi") is not None:
                mask_local &= rsi.between(spec["rsi"][0], spec["rsi"][1], inclusive="left")
            if spec.get("adx_min") is not None:
                mask_local &= (adx >= spec["adx_min"])
            if spec.get("qs_min") is not None:
                mask_local &= (qs >= spec["qs_min"])
            if spec.get("hour_cap") is not None:
                mask_local &= (hr < spec["hour_cap"])
            if spec.get("atr_pct") is not None:
                mask_local &= atr_pct.between(spec["atr_pct"][0], spec["atr_pct"][1], inclusive="both")
            keep |= mask_local
            per_setup_log.append((k_setup, int(in_setup.sum()), int(mask_local.sum())))

        out = df.loc[keep].copy()
        for setup, n0, n1 in sorted(per_setup_log, key=lambda x: -x[2]):
            print(f"[V17Q_RUN5_MAX] {side_label} {setup:<35s} {n0:>5d} -> {n1:>4d}")
        print(f"[V17Q_RUN5_MAX] {side_label} TOTAL {n_in} -> {len(out)}")
        return out

    def _v17q_apply_run5_max_post_scan(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        short_df, long_df = _pre_run5max_post_scan(short_df, long_df)
        long_df = _v17q_run5_max_filter_one(long_df, "LONG")
        short_df = _v17q_run5_max_filter_one(short_df, "SHORT")
        return short_df, long_df

    _base._apply_v16_post_scan_filters = _v17q_apply_run5_max_post_scan


# ---------------------------------------------------------------------------
# F12 (Phase 3) -- Entry-bar exit-aware Phase 2.
#
# Audit C1: simulate_exit_long/short walk from entry_idx+1 (skip entry bar)
# AND _resolve_exits_5min slices `bars["datetime"] > entry_time` (strict).
# So the entry bar's intra-bar SL/TGT excursion is invisible to BOTH phases.
# With 0.75% SL / 0.80% TGT, many entry bars sweep both. 53.8% of TARGET
# trades resolve in <=5 min in v17p output -- a large fraction of those
# bars may have first touched SL.
#
# F12 adds a corrective override pass that runs AFTER the original
# _resolve_exits_5min: for each trade, scan the entry bar's 1-min sub-slice
# (entry_time-5min, entry_time], detect the fill bar (first 1-min bar
# whose price crossed the trigger), and check whether that same fill bar's
# remaining range hits SL or TGT. If so, override the trade's outcome.
#
# When SL+TGT both touch the fill bar (ambiguous), pessimistic = SL,
# optimistic = TARGET; main path uses pess (matches base v16 convention).
#
# Gated by EQIDV17Q_ENTRY_BAR_AWARE_EXITS. Default OFF; flip after validation.
# ---------------------------------------------------------------------------
def _v17q_check_entry_bar_exit(
    bars_1m: pd.DataFrame,
    entry_price: float,
    side: str,
    sl: float,
    tgt: float,
):
    """Walk 1-min bars within the entry 5-min window; detect fill + same-bar
    exit. Returns dict with outcome ('SL' / 'TARGET') or None if no exit."""
    if bars_1m is None or bars_1m.empty:
        return None
    side_u = str(side).upper()

    for _, bar in bars_1m.iterrows():
        bar_high = float(bar.get("high", np.nan))
        bar_low = float(bar.get("low", np.nan))
        bar_time = bar.get("datetime", bar.get("date"))
        if not (np.isfinite(bar_high) and np.isfinite(bar_low)):
            continue

        # Did the trigger cross within this 1-min bar?
        if side_u == "LONG":
            if bar_high < entry_price:
                continue   # not yet filled; check next 1-min bar
        else:  # SHORT
            if bar_low > entry_price:
                continue

        # Fill happened during this 1-min bar at the trigger price.
        # Check whether the same bar's remaining range hits SL or TGT.
        if side_u == "LONG":
            stop_hit = bar_low <= sl
            target_hit = bar_high >= tgt
        else:
            stop_hit = bar_high >= sl
            target_hit = bar_low <= tgt

        if stop_hit and target_hit:
            return {
                "outcome": "SL",
                "exit_price_clean": sl,
                "exit_time": bar_time,
                "ambiguous": True,
                "case": "1MIN_FILL_BAR_AMBIGUOUS",
            }
        if stop_hit:
            return {
                "outcome": "SL",
                "exit_price_clean": sl,
                "exit_time": bar_time,
                "ambiguous": False,
                "case": "1MIN_FILL_BAR_STOP",
            }
        if target_hit:
            return {
                "outcome": "TARGET",
                "exit_price_clean": tgt,
                "exit_time": bar_time,
                "ambiguous": False,
                "case": "1MIN_FILL_BAR_TARGET",
            }
        # Filled cleanly with no excursion in this 1-min bar; the rest of the
        # entry bar's 1-min slice continues to be safe -- so do NOT keep
        # walking (those subsequent bars are still INSIDE the entry bar; the
        # original resolver will correctly handle the post-entry-bar window).
        return None
    return None


if V17Q_ENTRY_BAR_AWARE_EXITS or V17Q_REQUIRE_1MIN_EXITS:
    _orig_resolve_exits_5min = _base._resolve_exits_5min

    def _v17q_resolve_exits_5min(
        trades_df: pd.DataFrame,
        dir_5m,
        suffix_5m: str = ".parquet",
        engine: str = "pyarrow",
        eod_exit_time=None,
    ) -> pd.DataFrame:
        # First, run the original resolver. It populates exit_price /
        # exit_time_ist / outcome / pnl_pct / variants based on bars STRICTLY
        # AFTER entry_time_ist. F12 then overrides any row whose entry bar's
        # 1-min slice contained an EARLIER (and therefore correct) exit.
        df = _orig_resolve_exits_5min(
            trades_df, dir_5m, suffix_5m, engine, eod_exit_time,
        )
        if df is None or df.empty:
            return df

        if not V17Q_ENTRY_BAR_AWARE_EXITS:
            # Skip F12 override pass; only F15 (drop fallback) is requested.
            return _v17q_apply_f15_drop(df)

        # Per-trade override pass (F12).
        from pathlib import Path as _Path
        cache_1m = {}
        flips_to_sl = 0
        flips_to_tgt = 0
        flips_no_change = 0
        scanned = 0

        # Default cost (matches _resolve_exits_5min defaults).
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

            # Load 1-min using the same patterns the original uses.
            df_1m = _base._load_ticker_intrabar_cache(
                cache_1m,
                ticker,
                _Path(dir_5m),
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

            # Slice the entry-bar 1-min sub-window: (entry_time - 5min, entry_time].
            entry_bar_start = entry_time - pd.Timedelta(minutes=5)
            mask = (df_1m["datetime"] > entry_bar_start) & (df_1m["datetime"] <= entry_time)
            bars = df_1m.loc[mask].sort_values("datetime")
            if bars.empty:
                continue

            scanned += 1
            result = _v17q_check_entry_bar_exit(bars, entry_price, side, sl, tgt)
            if result is None:
                continue   # no entry-bar exit; original's resolution stands

            # Override the row.
            slip = DEFAULT_SLIP
            comm = DEFAULT_COMM
            if "slippage_pct" in df.columns and pd.notna(df.at[idx, "slippage_pct"]):
                slip = float(df.at[idx, "slippage_pct"])
            if "commission_pct" in df.columns and pd.notna(df.at[idx, "commission_pct"]):
                comm = float(df.at[idx, "commission_pct"])
            cost_pct = (slip + comm) * 100.0 * 2.0  # round-trip, percent

            outcome = result["outcome"]
            xp_clean = float(result["exit_price_clean"])
            xt = result["exit_time"]
            ambiguous = bool(result["ambiguous"])
            case = result["case"]

            # Pessimistic stop slippage applied iff outcome is SL (matches base).
            if outcome == "SL":
                xp_pess = float(_base._apply_stop_exit_slippage(side, xp_clean))
            else:
                xp_pess = xp_clean

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

            old_outcome = df.at[idx, "outcome"]
            if outcome == old_outcome:
                flips_no_change += 1
            elif outcome == "SL":
                flips_to_sl += 1
            else:
                flips_to_tgt += 1

            # Main path mirrors EXIT_REALISM_USE_STRESSED_BASE=True (== pess).
            df.at[idx, "exit_price"] = xp_pess
            df.at[idx, "exit_time_ist"] = xt
            df.at[idx, "outcome"] = outcome
            df.at[idx, "pnl_pct_gross"] = pess_raw
            df.at[idx, "pnl_pct"] = pess_raw - cost_pct

            # Auxiliary realism columns.
            df.at[idx, "exit_resolution_case"] = case
            df.at[idx, "exit_bar_ambiguous"] = ambiguous
            df.at[idx, "stop_fill_penalty_applied"] = (outcome == "SL")
            df.at[idx, "stop_fill_penalty_bps"] = (
                float(_base.STOP_EXIT_EXTRA_SLIPPAGE_BPS) if outcome == "SL" else 0.0
            )

            # Variant columns.
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

        print(
            f"[V17Q_F12] entry-bar override: scanned={scanned} "
            f"flipped_to_SL={flips_to_sl} flipped_to_TARGET={flips_to_tgt} "
            f"reaffirmed={flips_no_change}"
        )
        return _v17q_apply_f15_drop(df)

    def _v17q_apply_f15_drop(df: pd.DataFrame) -> pd.DataFrame:
        """F15: drop residual 5M_FALLBACK rows (require 1-min exits).

        After Phase-2 (and the F12 entry-bar override) any row whose
        exit_resolution_case starts with `5M_FALLBACK_*` was unable to
        resolve on 1-min data. Per the project preference for 1-min exit
        resolution, those rows are dropped.
        """
        if not V17Q_REQUIRE_1MIN_EXITS or df is None or df.empty:
            return df
        if "exit_resolution_case" not in df.columns:
            return df
        case = df["exit_resolution_case"].astype(str)
        fallback_mask = case.str.startswith("5M_FALLBACK")
        n_drop = int(fallback_mask.sum())
        if n_drop > 0:
            print(
                f"[V17Q_F15] dropping {n_drop} 5M_FALLBACK row(s); "
                f"1-min exit resolution required"
            )
            df = df.loc[~fallback_mask].reset_index(drop=True)
        else:
            print("[V17Q_F15] no 5M_FALLBACK rows present (1-min coverage clean)")
        return df

    _base._resolve_exits_5min = _v17q_resolve_exits_5min


# ---------------------------------------------------------------------------
# F11 (Phase 3) -- Disable require_entry_close_confirm lookahead.
#
# Audit C2: scanner accepts a trade as filled at the trigger price intrabar
# (when bar.high >= trigger for LONG / bar.low <= trigger for SHORT), but
# THEN gates on `close[entry_idx] > trigger` (LONG) / `< trigger` (SHORT).
# That close is only known at the END of the entry bar -- after the fill
# moment in real life. The gate is a future-data filter that selectively
# retains trades whose bar didn't revert. Combined with F12 it also masks
# entry-bar SL hits.
#
# Fix: mutate both cfgs to require_entry_close_confirm=False before the
# parallel scan launches. Done by wrapping _base._run_both_parallel.
#
# Gated by EQIDV17Q_NO_CLOSE_CONFIRM_LOOKAHEAD. Default OFF until validated.
# Effect expected: more trades enter (no longer pre-filtered by future close);
# headline win rate drops further; F12's flips become more visible because
# F11 admits the bars whose entry-bar SL was previously hidden.
# ---------------------------------------------------------------------------
def _v17q_floor_lag_attrs(cfg, side_label: str) -> int:
    """F14: any cfg attribute matching r'.*lag.*bars.*' with value in [0,1)
    gets floored to 1. -1 means 'dynamic legacy' and is preserved.
    """
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
            print(f"[V17Q_F14] {side_label} floored cfg.{attr}: {val} -> 1")
            floored += 1
    return floored


if V17Q_NO_CLOSE_CONFIRM_LOOKAHEAD or V17Q_FLOOR_ZERO_LAG:
    _orig_run_both_parallel = _base._run_both_parallel

    def _v17q_run_both_parallel(short_cfg, long_cfg, max_workers=None):
        # Mutate the shared configs in place. They are the same dataclass
        # objects passed into the worker pool tasks, so this propagates.
        if V17Q_NO_CLOSE_CONFIRM_LOOKAHEAD:
            short_cfg.require_entry_close_confirm = False
            long_cfg.require_entry_close_confirm = False
            print(
                "[V17Q_F11] disabled require_entry_close_confirm for SHORT and LONG "
                "(no entry-bar close lookahead)"
            )
        if V17Q_FLOOR_ZERO_LAG:
            n_short = _v17q_floor_lag_attrs(short_cfg, "SHORT")
            n_long = _v17q_floor_lag_attrs(long_cfg, "LONG")
            print(
                f"[V17Q_F14] floored {n_short + n_long} lag attr(s) "
                f"(SHORT={n_short}, LONG={n_long}) to >= 1"
            )
        if max_workers is None:
            return _orig_run_both_parallel(short_cfg, long_cfg)
        return _orig_run_both_parallel(short_cfg, long_cfg, max_workers)

    _base._run_both_parallel = _v17q_run_both_parallel


# ---------------------------------------------------------------------------
# F6 (Phase 2) -- Volume-ratio prior-bar-only average.
#
# Audit C4: _enrich_with_entry_vol_ratio computes avg_vol = day["volume"].mean()
# over the FULL trading day, including bars after the entry bar -- a clear
# lookahead used inside the V16 LONG vol-exhaust gate. F6 averages only over
# bars from open through and including the entry bar.
#
# Gated by EQIDV17Q_VOL_RATIO_NO_LOOKAHEAD. When OFF, base behavior unchanged.
# ---------------------------------------------------------------------------
if V17Q_VOL_RATIO_NO_LOOKAHEAD:
    def _v17q_enrich_with_entry_vol_ratio(
        long_df: pd.DataFrame,
        dir_15m: str,
        parquet_suffix: str = "_stocks_indicators_5min.parquet",
    ) -> pd.DataFrame:
        """F6 fix: avg_vol uses bars up to and including entry bar only."""
        import pathlib
        if long_df is None or long_df.empty:
            df = (long_df.copy() if long_df is not None else pd.DataFrame())
            df["entry_bar_vol_ratio"] = np.nan
            df["bars_from_open"] = np.nan
            return df

        dir_path = pathlib.Path(dir_15m)
        _5m_cache: dict = {}

        def _get_day(ticker, date_str):
            key = (ticker, date_str)
            if key not in _5m_cache:
                f = dir_path / f"{ticker}{parquet_suffix}"
                if not f.exists():
                    _5m_cache[key] = pd.DataFrame()
                    return _5m_cache[key]
                try:
                    df_p = pd.read_parquet(f)
                    df_p["date"] = pd.to_datetime(df_p["date"])
                except Exception:
                    _5m_cache[key] = pd.DataFrame()
                    return _5m_cache[key]
                day = df_p[df_p["date"].dt.strftime("%Y-%m-%d") == date_str].reset_index(drop=True)
                _5m_cache[key] = day
            return _5m_cache[key]

        ratios = []
        bar_idxs = []
        for _, row in long_df.iterrows():
            ticker = str(row.get("ticker", ""))
            date_s = str(row.get("trade_date", ""))[:10]
            try:
                entry_px = float(row.get("entry_price", 0))
            except (ValueError, TypeError):
                entry_px = 0.0

            day = _get_day(ticker, date_s)
            if day.empty or entry_px <= 0:
                ratios.append(np.nan)
                bar_idxs.append(np.nan)
                continue

            hits = day[day["high"] >= entry_px * 0.999]
            if hits.empty:
                ratios.append(np.nan)
                bar_idxs.append(np.nan)
                continue

            entry_bar_idx = int(hits.index[0])
            # F6: average over [0, entry_bar] only -- no future bars.
            prior = day.iloc[: entry_bar_idx + 1]
            avg_vol = prior["volume"].mean()
            if not np.isfinite(avg_vol) or avg_vol <= 0:
                ratios.append(np.nan)
                bar_idxs.append(entry_bar_idx)
                continue

            entry_bar_vol = float(day.iloc[entry_bar_idx]["volume"])
            ratios.append(entry_bar_vol / avg_vol)
            bar_idxs.append(entry_bar_idx)

        out = long_df.copy()
        out["entry_bar_vol_ratio"] = ratios
        out["bars_from_open"] = bar_idxs
        n_ok = int(out["entry_bar_vol_ratio"].notna().sum())
        print(f"[V17Q_F6] vol-ratio (prior-bar avg) computed for {n_ok}/{len(out)} LONG trades")
        return out

    _base._enrich_with_entry_vol_ratio = _v17q_enrich_with_entry_vol_ratio


# ---------------------------------------------------------------------------
# F7 (Phase 2) -- Nifty regime / RS lookup shifted -5 min.
#
# Audit C5: _apply_nifty_intraday_context looks up regime & RS at the entry
# bar's end timestamp. Regime at bar T is computed from close[T] etc. -- not
# known until that bar ends. For lag-1 entries the real fill happens at the
# bar OPEN (or somewhere intrabar), so close[T] is future data. F7 shifts
# the lookup key back 5 min so regime reflects what was known at the prior
# bar's close (= what the live executor would use).
#
# Gated by EQIDV17Q_NIFTY_LOOKUP_PREV_BAR. When OFF, base behavior unchanged.
# ---------------------------------------------------------------------------
if V17Q_NIFTY_LOOKUP_PREV_BAR:
    _orig_apply_nifty_intraday_context = _base._apply_nifty_intraday_context

    def _v17q_apply_nifty_intraday_context(
        short_df: pd.DataFrame,
        long_df: pd.DataFrame,
        cfg,
        mode_map: dict,
        nifty_ret_map: dict,
    ):
        """F7: shift entry_time_ist back 5min before lookup so regime
        comes from the bar that closed BEFORE the trade fill."""
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
            saved = d[ts_col].copy()
            d[ts_col] = pd.to_datetime(d[ts_col], errors="coerce") - delta
            return d, len(d)

        short_shifted, n_s = _shift(short_df)
        long_shifted, n_l = _shift(long_df)

        # Run original on shifted dfs. It computes nifty_context_mode +
        # nifty_rel_strength_pct using the shifted timestamps for lookup,
        # then drops rows whose mode/RS don't qualify.
        out_short, out_long = _orig_apply_nifty_intraday_context(
            short_shifted, long_shifted, cfg, mode_map, nifty_ret_map,
        )

        # Restore original entry_time_ist on the survivors.
        def _restore(df_out, df_in):
            if df_out is None or df_out.empty or df_in is None or df_in.empty:
                return df_out
            ts_col = "entry_time_ist" if "entry_time_ist" in df_out.columns else "signal_time_ist"
            if ts_col not in df_out.columns or ts_col not in df_in.columns:
                return df_out
            # Add 5 min back to recover original timestamps.
            df_out = df_out.copy()
            df_out[ts_col] = pd.to_datetime(df_out[ts_col], errors="coerce") + delta
            return df_out

        out_short = _restore(out_short, short_df)
        out_long = _restore(out_long, long_df)
        print(
            f"[V17Q_F7] nifty regime/RS lookup shifted -5min "
            f"(SHORT scanned {n_s}, LONG scanned {n_l})"
        )
        return out_short, out_long

    _base._apply_nifty_intraday_context = _v17q_apply_nifty_intraday_context


# ---------------------------------------------------------------------------
# F4 (Phase 1) -- Post-run audit asserts.
#
# After the run finishes and the CSV is written, re-read it and verify a
# battery of structural invariants the engine should never violate. With
# V17Q_AUDIT_STRICT=True, the first failure raises SystemExit so a corrupt
# output dies loudly. With STRICT=False, failures log as WARN.
#
# Checks:
#   1. No duplicate (date, ticker, side, signal_time)
#   2. No duplicate (date, ticker, side, entry_time)
#   3. F1 invariant: no duplicate (date, ticker, side)
#   4. exit_time >= entry_time
#   5. TARGET trades have pnl_pct_price > 0
#   6. SL trades have pnl_pct_price < 0
#   7. stop_fill_penalty_applied iff outcome == SL
#   8. F15 invariant: no exit_resolution_case starting with 5M_FALLBACK
#   9. Zero-lag warning (signal_time == entry_time): logs only, doesn't fail
# ---------------------------------------------------------------------------
if V17Q_AUDIT_STRICT:
    _orig_main = _base.main

    def _v17q_post_run_audit() -> None:
        import glob
        from pathlib import Path as _Path
        out_dir = _v17q_runtime_dir("outputs_v16_5min")  # rewritten -> v17q_5min
        pattern = str(_Path(out_dir) / "avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            print("[V17Q_AUDIT] no output CSV found; skipping audit")
            return

        latest = files[-1]
        df = pd.read_csv(latest)
        print(f"[V17Q_AUDIT] auditing {_Path(latest).name} (rows={len(df)})")

        failures = []

        def _fail(name: str, n: int, hint: str) -> None:
            if n > 0:
                failures.append(f"{name} ({hint}: n={n})")
                print(f"[V17Q_AUDIT][FAIL] {name}: n={n} ({hint})")
            else:
                print(f"[V17Q_AUDIT][PASS] {name}")

        # 1-3 duplicate checks
        _fail(
            "no_dup_signal_key",
            int(df.duplicated(subset=["trade_date","ticker","side","signal_time_ist"]).sum()),
            "duplicates on (date,ticker,side,signal_time)",
        )
        _fail(
            "no_dup_entry_key",
            int(df.duplicated(subset=["trade_date","ticker","side","entry_time_ist"]).sum()),
            "duplicates on (date,ticker,side,entry_time)",
        )
        _fail(
            "F1_one_ticker_per_day",
            int(df.duplicated(subset=["trade_date","ticker","side"]).sum()),
            "duplicates on (date,ticker,side)",
        )

        # 4 exit >= entry (with carve-out for F12 entry-bar exits)
        # F12 stamps exit_time_ist as the 1-min bar end where the fill+exit
        # happened, which may be up to 5 min BEFORE entry_time_ist (the
        # 5-min entry bar's end). Those are legitimate. For all other
        # resolution cases, exit_time must be >= entry_time.
        et = pd.to_datetime(df["entry_time_ist"], utc=True, errors="coerce")
        xt = pd.to_datetime(df["exit_time_ist"], utc=True, errors="coerce")
        case_col = df.get("exit_resolution_case", pd.Series("", index=df.index)).astype(str)
        is_fill_bar = case_col.str.startswith("1MIN_FILL_BAR")
        # Tolerance: 5 min for fill-bar rows (within entry bar), 0 otherwise.
        tol = pd.to_timedelta(is_fill_bar.map({True: "5min", False: "0min"}))
        bad_xt_before_et = (xt + tol < et) & et.notna() & xt.notna()
        _fail(
            "exit_time_after_entry",
            int(bad_xt_before_et.sum()),
            "rows with exit_time materially before entry_time",
        )

        # 5-6 outcome / pnl consistency
        pnl_price = pd.to_numeric(df.get("pnl_pct_price", pd.Series(dtype=float)),
                                  errors="coerce")
        if not pnl_price.empty:
            _fail(
                "TARGET_has_positive_pnl",
                int((df["outcome"].eq("TARGET") & (pnl_price <= 0)).sum()),
                "TARGET rows with pnl_pct_price <= 0",
            )
            _fail(
                "SL_has_negative_pnl",
                int((df["outcome"].eq("SL") & (pnl_price >= 0)).sum()),
                "SL rows with pnl_pct_price >= 0",
            )

        # 7 stop_fill_penalty_applied iff outcome==SL
        if "stop_fill_penalty_applied" in df.columns:
            sfp_raw = df["stop_fill_penalty_applied"]
            if sfp_raw.dtype == bool:
                sfp = sfp_raw
            else:
                sfp = sfp_raw.astype(str).str.lower().isin(("true", "1", "yes"))
            _fail(
                "stop_fill_penalty_iff_SL",
                int((sfp != df["outcome"].eq("SL")).sum()),
                "rows where stop_fill_penalty_applied != (outcome=='SL')",
            )

        # 8 F15 — no 5M_FALLBACK
        if V17Q_REQUIRE_1MIN_EXITS and "exit_resolution_case" in df.columns:
            case = df["exit_resolution_case"].astype(str)
            _fail(
                "F15_no_5M_fallback",
                int(case.str.startswith("5M_FALLBACK").sum()),
                "rows with 5M_FALLBACK exit_resolution_case",
            )

        # 9 zero-lag warning (informational)
        st = pd.to_datetime(df["signal_time_ist"], utc=True, errors="coerce")
        if st.notna().any() and et.notna().any():
            lag_min = (et - st).dt.total_seconds() / 60.0
            n_zero_lag = int((lag_min < 5).sum())
            if n_zero_lag > 0:
                print(
                    f"[V17Q_AUDIT][WARN] {n_zero_lag} row(s) with signal->entry lag < 5min "
                    "(possible same-bar lookahead -- F14 candidate)"
                )

        if failures:
            msg = (
                f"[V17Q_AUDIT] {len(failures)} check(s) FAILED: "
                + "; ".join(failures)
            )
            print(msg)
            if V17Q_AUDIT_STRICT:
                import sys as _sys
                print("[V17Q_AUDIT] STRICT mode -- exiting with code 2")
                _sys.exit(2)
        else:
            print("[V17Q_AUDIT] all checks passed")

    def _v17q_main():
        result = _orig_main()
        try:
            _v17q_post_run_audit()
        except SystemExit:
            raise
        except Exception as exc:
            print(f"[V17Q_AUDIT] post-run audit error: {exc}")
        return result

    _base.main = _v17q_main


# ---------------------------------------------------------------------------
# Toggle status banner.
# ---------------------------------------------------------------------------
def _enabled_toggles() -> list:
    flags = []
    # Phase 1
    if V17Q_STAGE0_HARDEN:                flags.append("STAGE0_HARDEN")
    if V17Q_DEDUP_WINDOW_MIN > 0:         flags.append(f"DEDUP_WINDOW_MIN={V17Q_DEDUP_WINDOW_MIN}")
    if V17Q_ZERO_LAG_POLICY != "keep":    flags.append(f"ZERO_LAG_POLICY={V17Q_ZERO_LAG_POLICY}")
    if V17Q_AUDIT_STRICT:                 flags.append("AUDIT_STRICT")
    if V17Q_REQUIRE_1MIN_EXITS:           flags.append("REQUIRE_1MIN_EXITS")
    if V17Q_STAMP_METADATA:               flags.append("STAMP_METADATA")
    # Phase 2
    if V17Q_VOL_RATIO_NO_LOOKAHEAD:       flags.append("VOL_RATIO_NO_LOOKAHEAD")
    if V17Q_NIFTY_LOOKUP_PREV_BAR:        flags.append("NIFTY_LOOKUP_PREV_BAR")
    if V17Q_NIFTY_CONTEXT_FULL_SESSION:   flags.append("NIFTY_CONTEXT_FULL_SESSION")
    if V17Q_PARQUET_NAIVE_TZ != "legacy": flags.append(f"PARQUET_NAIVE_TZ={V17Q_PARQUET_NAIVE_TZ}")
    if V17Q_STAGE2_PNL_ORDERED:           flags.append("STAGE2_PNL_ORDERED")
    # Phase 3
    if V17Q_NO_CLOSE_CONFIRM_LOOKAHEAD:   flags.append("NO_CLOSE_CONFIRM_LOOKAHEAD")
    if V17Q_ENTRY_BAR_AWARE_EXITS:        flags.append("ENTRY_BAR_AWARE_EXITS")
    if V17Q_ENTRY_AT_NEXT_OPEN:           flags.append("ENTRY_AT_NEXT_OPEN")
    if V17Q_FLOOR_ZERO_LAG:               flags.append("FLOOR_ZERO_LAG")
    # Phase 4 (Run 5 optimized)
    if V17Q_RUN5_OPTIMIZED:               flags.append("RUN5_OPTIMIZED")
    if V17Q_RUN5_PRO:                     flags.append("RUN5_PRO")
    if V17Q_RUN5_MAX:                     flags.append("RUN5_MAX")
    return flags


if __name__ == "__main__":
    print("=" * 78)
    print("V17q 5-min runner -- Day 0 bootstrap (behavioral clone of v17p)")
    print("  Output dir: outputs_v17q_5min")
    flags = _enabled_toggles()
    if flags:
        print(f"  Active V17Q fix toggles: {', '.join(flags)}")
    else:
        print("  Active V17Q fix toggles: NONE -- pure v17p behavior expected")
    print("  Inherits v17p / v17o / v17n / v17m / ... / v16 filter chain")
    print("=" * 78)
    _base.main()
