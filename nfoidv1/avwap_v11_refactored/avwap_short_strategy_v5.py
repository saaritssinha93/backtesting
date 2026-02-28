# -*- coding: utf-8 -*-
"""
avwap_short_strategy_v5.py — SHORT side logic for AVWAP v11, Version 5
=======================================================================

Extends avwap_short_strategy with two new directional-confirmation indicators:

NEW INDICATORS (v5):
  1. Supertrend (period=10, multiplier=3.0)
     - SHORT entry requires SUPERTREND_DIR == -1 (price below upper band → bearish)

  2. MACD (fast=12, slow=26, signal=9)
     - SHORT entry requires MACD histogram < 0  OR  histogram is falling vs prev bar
     - "falling" catches early bear momentum turns before histogram crosses zero

Both checks are added on top of the existing _trend_filter_short (ADX + RSI + Stoch + EMA + AVWAP).

Architecture:
  - compute_supertrend and compute_macd are imported from avwap_long_strategy_v5
    (shared implementation — avoids code duplication)
  - _trend_filter_short_v5(df_day, i, c1, cfg) — wraps base + Supertrend/MACD
  - scan_one_day_v5(ticker, df_day, day_str, cfg) — replicated scan with v5 filter
  - scan_all_days_for_ticker_v5(ticker, df_full, cfg) — adds indicator pre-computation

All shared infrastructure comes from avwap_common.
"""

from __future__ import annotations

import os
from typing import List, Optional

import numpy as np
import pandas as pd

from avwap_v11_refactored.avwap_common import (
    IST,
    StrategyConfig,
    Trade,
    default_short_config,
    read_15m_parquet,
    list_tickers_15m,
    in_session,
    in_signal_window,
    entry_buffer,
    prepare_indicators,
    compute_day_avwap,
    twice_increasing,
    twice_reducing,
    adx_slope_ok,
    max_consecutive_true,
    compute_quality_score_short,
    compute_pnl_pct,
    trades_to_df,
    apply_topn_per_day,
    volume_filter_pass,
)

# Import base SHORT functions that stay unchanged
from avwap_v11_refactored.avwap_short_strategy import (
    classify_red_impulse,
    avwap_rejection_pass,
    avwap_distance_pass,
    simulate_exit_short,
    _trend_filter_short,         # base filter (ADX + RSI + Stoch + EMA + AVWAP)
)

# Import shared indicator functions from long v5 (avoid duplication)
from avwap_v11_refactored.avwap_long_strategy_v5 import (
    compute_supertrend,
    compute_macd,
    _COL_ST_DIR,
    _COL_ST_VAL,
    _COL_MACD_H,
    _COL_MACD_L,
    _COL_MACD_SIG,
)


# ===========================================================================
# V5 TREND FILTER (SHORT) — base filter + Supertrend + MACD
# ===========================================================================
def _trend_filter_short_v5(
    df_day: pd.DataFrame, i: int, c1: pd.Series, cfg: StrategyConfig
) -> bool:
    """
    Returns True if ALL directional conditions pass for SHORT (v5).

    Base conditions (unchanged from v1):
      • ADX ≥ adx_min, twice_increasing, slope ≥ adx_slope_min
      • RSI ≤ rsi_max_short, twice_reducing
      • StochK ≤ stochk_max, K < D, twice_reducing
      • EMA20 < EMA50, close < EMA20, close < AVWAP

    New v5 conditions:
      1. Supertrend direction == -1 at bar i (price below upper Supertrend band → bearish)
      2. MACD histogram < 0  OR  MACD histogram falling vs previous bar
         (catches early bear momentum turns before histogram crosses zero)
    """
    # ---- Base filter (must pass first) ----
    if not _trend_filter_short(df_day, i, c1, cfg):
        return False

    # ---- Supertrend: require bearish (-1) ----
    if _COL_ST_DIR not in df_day.columns:
        return False  # indicator not pre-computed → skip trade (safe default)
    st_dir = df_day.at[i, _COL_ST_DIR]
    if not (np.isfinite(float(st_dir)) and int(st_dir) == -1):
        return False

    # ---- MACD histogram: <0 OR falling ----
    if _COL_MACD_H not in df_day.columns:
        return False
    hist_i = float(df_day.at[i, _COL_MACD_H]) if np.isfinite(df_day.at[i, _COL_MACD_H]) else np.nan
    if not np.isfinite(hist_i):
        return False

    hist_ok = hist_i < 0.0
    if not hist_ok and i >= 1:
        hist_prev = float(df_day.at[i - 1, _COL_MACD_H]) if np.isfinite(df_day.at[i - 1, _COL_MACD_H]) else np.nan
        hist_ok = np.isfinite(hist_prev) and (hist_i < hist_prev)

    return hist_ok


# ===========================================================================
# SCAN ONE DAY — V5 (SHORT)
# ===========================================================================
def scan_one_day_v5(
    ticker: str, df_day: pd.DataFrame, day_str: str, cfg: StrategyConfig
) -> List[Trade]:
    """
    Identical to avwap_short_strategy.scan_one_day but calls _trend_filter_short_v5
    instead of _trend_filter_short.

    Expects df_day to already contain SUPERTREND_DIR, MACD_HIST columns.
    """
    if len(df_day) < int(cfg.min_bars_for_scan):
        return []

    trades: List[Trade] = []
    i = 2

    tail_guard = 1 if cfg.allow_incomplete_tail else 3
    while i < len(df_day) - tail_guard:
        if len(trades) >= cfg.max_trades_per_ticker_per_day:
            break

        c1  = df_day.iloc[i]
        ts1 = c1["date"]

        if not in_signal_window(ts1, cfg):
            i += 1
            continue

        impulse = classify_red_impulse(c1, cfg)
        if impulse == "":
            i += 1
            continue

        # ATR sanity
        atr1   = float(c1["ATR15"]) if np.isfinite(c1.get("ATR15", np.nan)) else np.nan
        close1 = float(c1["close"])
        if not (np.isfinite(atr1) and atr1 > 0 and np.isfinite(close1) and close1 > 0):
            i += 1
            continue

        if cfg.use_atr_pct_filter and (atr1 / close1) < cfg.atr_pct_min:
            i += 1
            continue

        if not volume_filter_pass(c1, cfg):
            i += 1
            continue

        # ---- V5: use extended trend filter ----
        if not _trend_filter_short_v5(df_day, i, c1, cfg):
            i += 1
            continue

        # Diagnostic values at signal
        adx1  = float(df_day.at[i, "ADX15"])   if "ADX15"   in df_day.columns else 0.0
        rsi1  = float(df_day.at[i, "RSI15"])   if "RSI15"   in df_day.columns else 0.0
        k1    = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
        avwap1    = float(c1["AVWAP"])  if np.isfinite(c1.get("AVWAP", np.nan))  else 0.0
        ema20     = float(c1["EMA20"])  if np.isfinite(c1.get("EMA20", np.nan))  else 0.0
        atr_pct   = atr1 / close1
        avwap_dist_atr = (avwap1 - close1) / atr1
        ema_gap_atr    = (ema20 - close1) / atr1
        quality = compute_quality_score_short(adx1, avwap_dist_atr, ema_gap_atr, impulse)

        low1 = float(c1["low"])

        def _bars_left_ok(eidx: int) -> bool:
            return (len(df_day) - 1 - eidx) >= cfg.min_bars_left_after_entry

        def _close_confirm_ok(eidx: int, trigger: float) -> bool:
            if not cfg.require_entry_close_confirm:
                return True
            cl = float(df_day.at[eidx, "close"])
            return np.isfinite(cl) and cl < trigger

        def _make_trade(entry_idx: int, entry_price: float, setup: str) -> tuple:
            exit_idx, exit_time, exit_price, outcome = simulate_exit_short(
                df_day, entry_idx, entry_price, cfg
            )
            net_pnl, gross_pnl = compute_pnl_pct(entry_price, exit_price, "SHORT", cfg)
            return Trade(
                trade_date=day_str,
                ticker=ticker,
                side="SHORT",
                setup=setup,
                impulse_type=impulse,
                signal_time_ist=ts1,
                entry_time_ist=df_day.at[entry_idx, "date"],
                entry_price=entry_price,
                sl_price=entry_price * (1.0 + cfg.stop_pct),
                target_price=entry_price * (1.0 - cfg.target_pct),
                exit_time_ist=exit_time,
                exit_price=exit_price,
                outcome=outcome,
                pnl_pct=net_pnl,
                pnl_pct_gross=gross_pnl,
                adx_signal=adx1,
                rsi_signal=rsi1,
                stochk_signal=k1,
                avwap_dist_atr_signal=avwap_dist_atr,
                ema20_gap_atr_signal=ema_gap_atr,
                atr_pct_signal=atr_pct,
                quality_score=quality,
            ), exit_idx

        # ---------------------------------------------------------------
        # SETUP A (MODERATE): break C1 low, or pullback + break C2 low
        # ---------------------------------------------------------------
        if impulse == "MODERATE":
            c2   = df_day.iloc[i + 1]
            buf1 = entry_buffer(low1, cfg)
            trigger1 = low1 - buf1

            lag1 = int(cfg.lag_bars_short_a_mod_break_c1_low)
            entry_idx_1 = i + lag1 if lag1 >= 0 else (i + 1)
            if entry_idx_1 < len(df_day) and float(df_day.at[entry_idx_1, "low"]) < trigger1:
                entry_idx = entry_idx_1
                entry_ts  = df_day.at[entry_idx, "date"]

                if (
                    in_signal_window(entry_ts, cfg)
                    and _bars_left_ok(entry_idx)
                    and _close_confirm_ok(entry_idx, trigger1)
                    and avwap_rejection_pass(df_day, i, entry_idx, cfg)
                    and avwap_distance_pass(df_day, entry_idx, cfg)
                ):
                    trade, exit_idx = _make_trade(entry_idx, trigger1, "A_MOD_BREAK_C1_LOW")
                    trades.append(trade)
                    i = exit_idx + 1
                    continue

            # Option 2: small green pullback C2 below AVWAP, then break C2 low
            c2o, c2c  = float(c2["open"]), float(c2["close"])
            c2_body   = abs(c2c - c2o)
            c2_atr    = float(c2.get("ATR15", atr1)) if np.isfinite(c2.get("ATR15", atr1)) else atr1
            c2_avwap  = float(c2.get("AVWAP", np.nan)) if np.isfinite(c2.get("AVWAP", np.nan)) else np.nan

            c2_small_green  = (c2c > c2o) and np.isfinite(c2_atr) and (c2_body <= cfg.small_counter_max_atr * c2_atr)
            c2_below_avwap  = np.isfinite(c2_avwap) and (c2c < c2_avwap)

            if c2_small_green and c2_below_avwap and (i + 2 < len(df_day)):
                low2     = float(c2["low"])
                buf2     = entry_buffer(low2, cfg)
                trigger2 = low2 - buf2

                lag2 = int(cfg.lag_bars_short_a_pullback_c2_break_c2_low)
                entry_idx_2 = i + lag2 if lag2 >= 0 else (i + 2)
                if entry_idx_2 < len(df_day) and float(df_day.at[entry_idx_2, "low"]) < trigger2:
                    entry_idx = entry_idx_2
                    entry_ts  = df_day.at[entry_idx, "date"]

                    if (
                        in_signal_window(entry_ts, cfg)
                        and _bars_left_ok(entry_idx)
                        and _close_confirm_ok(entry_idx, trigger2)
                        and avwap_rejection_pass(df_day, i, entry_idx, cfg)
                        and avwap_distance_pass(df_day, entry_idx, cfg)
                    ):
                        trade, exit_idx = _make_trade(
                            entry_idx, trigger2, "A_PULLBACK_C2_THEN_BREAK_C2_LOW"
                        )
                        trades.append(trade)
                        i = exit_idx + 1
                        continue

            i += 1
            continue

        # ---------------------------------------------------------------
        # SETUP B (HUGE): bounce fails, break bounce low
        # ---------------------------------------------------------------
        if impulse == "HUGE":
            bounce_end = min(i + 3, len(df_day) - 1)
            bounce     = df_day.iloc[i + 1 : bounce_end + 1].copy()
            if bounce.empty:
                i += 1
                continue

            closes      = pd.to_numeric(bounce["close"], errors="coerce")
            opens       = pd.to_numeric(bounce["open"],  errors="coerce")
            bounce_atr  = pd.to_numeric(bounce.get("ATR15", atr1), errors="coerce").fillna(atr1)
            bounce_body = (closes - opens).abs()
            bounce_green = closes > opens
            bounce_small = bounce_body <= (cfg.small_counter_max_atr * bounce_atr)

            if not bool((bounce_green & bounce_small).any()):
                i += 1
                continue

            if cfg.require_avwap_rule and cfg.avwap_touch:
                avwaps    = pd.to_numeric(bounce["AVWAP"], errors="coerce")
                highs     = pd.to_numeric(bounce["high"],  errors="coerce")
                touch_fail = bool(((highs >= avwaps) & (closes < avwaps)).fillna(False).any())
                if not touch_fail:
                    i += 1
                    continue

            bounce_low = float(pd.to_numeric(bounce["low"], errors="coerce").min())
            if not np.isfinite(bounce_low):
                i += 1
                continue

            buf       = entry_buffer(bounce_low, cfg)
            trigger_b = bounce_low - buf
            entered   = False

            lag_huge = int(cfg.lag_bars_short_b_huge_failed_bounce)
            if lag_huge >= 0:
                j_fixed = i + lag_huge
                j_iter  = [j_fixed] if (bounce_end + 1 <= j_fixed < len(df_day)) else []
            else:
                j_iter = range(bounce_end + 1, len(df_day))

            for j in j_iter:
                tsj = df_day.at[j, "date"]
                if not in_signal_window(tsj, cfg):
                    continue
                if not _bars_left_ok(j):
                    continue

                closej = float(df_day.at[j, "close"])
                avwapj = float(df_day.at[j, "AVWAP"]) if np.isfinite(df_day.at[j, "AVWAP"]) else np.nan
                if np.isfinite(avwapj) and closej >= avwapj:
                    break

                if float(df_day.at[j, "low"]) < trigger_b:
                    if not _close_confirm_ok(j, trigger_b):
                        continue
                    if not avwap_distance_pass(df_day, j, cfg):
                        continue
                    if not avwap_rejection_pass(df_day, i, j, cfg):
                        continue

                    trade, exit_idx = _make_trade(j, trigger_b, "B_HUGE_RED_FAILED_BOUNCE")
                    trades.append(trade)
                    i = exit_idx + 1
                    entered = True
                    break

            if entered:
                continue
            i += 1
            continue

        i += 1

    return trades


# ===========================================================================
# SCAN ALL DAYS — V5 (SHORT)
# ===========================================================================
def scan_all_days_for_ticker_v5(
    ticker: str, df_full: pd.DataFrame, cfg: StrategyConfig
) -> List[Trade]:
    """
    Like scan_all_days_for_ticker but with Supertrend + MACD pre-computed.
    """
    if df_full.empty:
        return []

    for c in ["open", "high", "low", "close"]:
        if c not in df_full.columns:
            return []

    df = df_full[df_full["date"].apply(lambda ts: in_session(ts, cfg))].copy()
    if df.empty:
        return []

    df = df.sort_values("date").reset_index(drop=True)
    df = prepare_indicators(df, cfg)

    # ---- Compute Supertrend + MACD on full filtered DataFrame ----
    _, st_dir_series      = compute_supertrend(df, period=10, multiplier=3.0)
    _, _, macd_hist_series = compute_macd(df, fast=12, slow=26, signal_period=9)

    df[_COL_ST_DIR] = st_dir_series.values
    df[_COL_MACD_H] = macd_hist_series.values

    all_trades: List[Trade] = []
    for day_val, df_day in df.groupby("day", sort=True):
        df_day = df_day.copy().reset_index(drop=True)
        if len(df_day) < int(cfg.min_bars_for_scan):
            continue
        df_day["AVWAP"] = compute_day_avwap(df_day)
        trades = scan_one_day_v5(ticker, df_day, str(day_val), cfg)
        if trades:
            all_trades.extend(trades)

    return all_trades


# ===========================================================================
# RUN ALL TICKERS — V5 (called by combined runner v5)
# ===========================================================================
def run_short_scan_v5(cfg: Optional[StrategyConfig] = None) -> List[Trade]:
    if cfg is None:
        cfg = default_short_config()

    tickers = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    print(f"[SHORT-v5] Tickers found: {len(tickers)}")

    all_trades: List[Trade] = []
    for k, t in enumerate(tickers, start=1):
        path = os.path.join(cfg.dir_15m, f"{t}{cfg.end_15m}")
        from avwap_v11_refactored.avwap_common import read_15m_parquet
        df = read_15m_parquet(path, cfg.parquet_engine)
        if df.empty:
            continue

        trades = scan_all_days_for_ticker_v5(t, df, cfg)
        if trades:
            all_trades.extend(trades)

        if k % 50 == 0:
            print(f"  [SHORT-v5] scanned {k}/{len(tickers)} | trades_so_far={len(all_trades)}")

    return all_trades
