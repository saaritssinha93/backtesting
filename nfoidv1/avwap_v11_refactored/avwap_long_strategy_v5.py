# -*- coding: utf-8 -*-
"""
avwap_long_strategy_v5.py — LONG side logic for AVWAP v11, Version 5
======================================================================

Extends avwap_long_strategy with two new directional-confirmation indicators:

NEW INDICATORS (v5):
  1. Supertrend (period=10, multiplier=3.0)
     - Computed on the full day DataFrame before scanning
     - LONG entry requires SUPERTREND_DIR == +1 (price above lower band → bullish)

  2. MACD (fast=12, slow=26, signal=9)
     - Computed on full day DataFrame
     - LONG entry requires MACD histogram > 0  OR  histogram is rising vs prev bar
     - "rising" catches early momentum turns before histogram crosses zero

Both checks are added on top of the existing _trend_filter_long (ADX + RSI + Stoch + EMA + AVWAP).

Architecture:
  - compute_supertrend(df, period, multiplier) → (Series, Series)
  - compute_macd(df, fast, slow, signal_period) → (Series, Series, Series)
  - _trend_filter_long_v5(df_day, i, c1, cfg) — wraps base + Supertrend/MACD
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
    default_long_config,
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
    compute_quality_score_long,
    compute_pnl_pct,
    trades_to_df,
    apply_topn_per_day,
    volume_filter_pass,
)

# Import the base LONG functions we still need unchanged
from avwap_v11_refactored.avwap_long_strategy import (
    classify_green_impulse,
    avwap_support_pass,
    simulate_exit_long,
    _trend_filter_long,         # base trend filter (ADX + RSI + Stoch + EMA + AVWAP)
)

# ---------------------------------------------------------------------------
# Supertrend / MACD column names
# ---------------------------------------------------------------------------
_COL_ST_DIR   = "SUPERTREND_DIR"
_COL_ST_VAL   = "SUPERTREND"
_COL_MACD_H   = "MACD_HIST"
_COL_MACD_L   = "MACD_LINE"
_COL_MACD_SIG = "MACD_SIGNAL"


# ===========================================================================
# NEW INDICATORS
# ===========================================================================

def compute_supertrend(
    df: pd.DataFrame,
    period: int = 10,
    multiplier: float = 3.0,
) -> tuple:
    """
    Compute Supertrend on a DataFrame with high/low/close columns.

    Returns
    -------
    supertrend : pd.Series  — the dynamic stop level
    direction  : pd.Series  — +1 (bullish) or -1 (bearish), int dtype
    """
    high  = pd.to_numeric(df["high"],  errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    low   = pd.to_numeric(df["low"],   errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    n = len(close)

    # True Range
    prev_c = np.empty_like(close)
    prev_c[0] = close[0]
    prev_c[1:] = close[:-1]
    tr = np.maximum(
        np.abs(high - low),
        np.maximum(np.abs(high - prev_c), np.abs(low - prev_c)),
    )

    # Wilder-smoothed ATR
    atr = np.zeros(n)
    if n >= period:
        atr[period - 1] = np.nanmean(tr[:period])
        for idx in range(period, n):
            atr[idx] = (atr[idx - 1] * (period - 1) + tr[idx]) / period

    hl2 = (high + low) / 2.0
    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    supertrend  = np.full(n, np.nan)
    direction   = np.zeros(n, dtype=np.int8)

    # Seed first bar as bearish (price starts "under" the upper band)
    supertrend[0] = final_upper[0]
    direction[0]  = -1

    for idx in range(1, n):
        # Upper band ratchets down unless new high breaks above previous or close crosses
        if basic_upper[idx] < final_upper[idx - 1] or close[idx - 1] > final_upper[idx - 1]:
            final_upper[idx] = basic_upper[idx]
        else:
            final_upper[idx] = final_upper[idx - 1]

        # Lower band ratchets up unless new low breaks below previous or close crosses
        if basic_lower[idx] > final_lower[idx - 1] or close[idx - 1] < final_lower[idx - 1]:
            final_lower[idx] = basic_lower[idx]
        else:
            final_lower[idx] = final_lower[idx - 1]

        if direction[idx - 1] == -1:
            # Was bearish: flip to bullish if close crosses above upper band
            if close[idx] > final_upper[idx]:
                direction[idx] = 1
                supertrend[idx] = final_lower[idx]
            else:
                direction[idx] = -1
                supertrend[idx] = final_upper[idx]
        else:
            # Was bullish: flip to bearish if close drops below lower band
            if close[idx] < final_lower[idx]:
                direction[idx] = -1
                supertrend[idx] = final_upper[idx]
            else:
                direction[idx] = 1
                supertrend[idx] = final_lower[idx]

    return (
        pd.Series(supertrend,            index=df.index, name=_COL_ST_VAL),
        pd.Series(direction.astype(int), index=df.index, name=_COL_ST_DIR),
    )


def compute_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple:
    """
    Compute MACD on a DataFrame with a 'close' column.

    Returns
    -------
    macd_line   : pd.Series  — EMA(fast) - EMA(slow)
    macd_signal : pd.Series  — EMA(signal_period) of macd_line
    macd_hist   : pd.Series  — macd_line - macd_signal  (positive = bullish momentum)
    """
    close = pd.to_numeric(df["close"], errors="coerce")
    ema_fast = close.ewm(span=fast,   adjust=False, min_periods=fast).mean()
    ema_slow = close.ewm(span=slow,   adjust=False, min_periods=slow).mean()
    macd_line   = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=signal_period, adjust=False, min_periods=signal_period).mean()
    macd_hist   = macd_line - macd_signal
    macd_line.name   = _COL_MACD_L
    macd_signal.name = _COL_MACD_SIG
    macd_hist.name   = _COL_MACD_H
    return macd_line, macd_signal, macd_hist


# ===========================================================================
# V5 TREND FILTER (LONG) — base filter + Supertrend + MACD
# ===========================================================================
def _trend_filter_long_v5(
    df_day: pd.DataFrame, i: int, c1: pd.Series, cfg: StrategyConfig
) -> bool:
    """
    Returns True if ALL directional conditions pass for LONG (v5).

    Base conditions (unchanged from v1):
      • ADX ≥ adx_min, twice_increasing, slope ≥ adx_slope_min
      • RSI ≥ rsi_min_long, twice_increasing
      • StochK in range, K > D, twice_increasing
      • EMA20 > EMA50, close > EMA20, close > AVWAP

    New v5 conditions:
      1. Supertrend direction == +1 at bar i (price above lower Supertrend band)
      2. MACD histogram > 0  OR  MACD histogram rising vs previous bar
         (catches early bull momentum turns before histogram crosses zero)
    """
    # ---- Base filter (must pass first) ----
    if not _trend_filter_long(df_day, i, c1, cfg):
        return False

    # ---- Supertrend: require bullish (+1) ----
    if _COL_ST_DIR not in df_day.columns:
        return False  # indicator not pre-computed → skip trade (safe default)
    st_dir = df_day.at[i, _COL_ST_DIR]
    if not (np.isfinite(float(st_dir)) and int(st_dir) == 1):
        return False

    # ---- MACD histogram: >0 OR rising ----
    if _COL_MACD_H not in df_day.columns:
        return False
    hist_i = float(df_day.at[i, _COL_MACD_H]) if np.isfinite(df_day.at[i, _COL_MACD_H]) else np.nan
    if not np.isfinite(hist_i):
        return False

    hist_ok = hist_i > 0.0
    if not hist_ok and i >= 1:
        hist_prev = float(df_day.at[i - 1, _COL_MACD_H]) if np.isfinite(df_day.at[i - 1, _COL_MACD_H]) else np.nan
        hist_ok = np.isfinite(hist_prev) and (hist_i > hist_prev)

    return hist_ok


# ===========================================================================
# SCAN ONE DAY — V5 (LONG)
# ===========================================================================
def scan_one_day_v5(
    ticker: str, df_day: pd.DataFrame, day_str: str, cfg: StrategyConfig
) -> List[Trade]:
    """
    Identical to avwap_long_strategy.scan_one_day but calls _trend_filter_long_v5
    instead of _trend_filter_long.

    Expects df_day to already contain SUPERTREND_DIR, MACD_HIST columns
    (added by scan_all_days_for_ticker_v5 before the day loop).
    """
    if len(df_day) < int(cfg.min_bars_for_scan):
        return []

    trades: List[Trade] = []
    i = 2

    tail_guard = 1 if cfg.allow_incomplete_tail else 3
    while i < len(df_day) - tail_guard:
        if len(trades) >= cfg.max_trades_per_ticker_per_day:
            break

        c1 = df_day.iloc[i]

        if not in_signal_window(c1["date"], cfg):
            i += 1
            continue

        impulse = classify_green_impulse(c1, cfg)
        if impulse == "":
            i += 1
            continue

        # ATR sanity
        atr1 = float(c1["ATR15"]) if np.isfinite(c1.get("ATR15", np.nan)) else np.nan
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
        if not _trend_filter_long_v5(df_day, i, c1, cfg):
            i += 1
            continue

        # Diagnostic values
        adx1 = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
        rsi1 = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
        k1   = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
        ema20 = float(c1["EMA20"]) if np.isfinite(c1.get("EMA20", np.nan)) else 0.0
        atr_pct = atr1 / close1

        signal_time = c1["date"]
        high1 = float(c1["high"])
        open1 = float(c1["open"])

        def _bars_left_ok(eidx: int) -> bool:
            return (len(df_day) - 1 - eidx) >= cfg.min_bars_left_after_entry

        def _close_confirm_ok(eidx: int, trigger: float) -> bool:
            if not cfg.require_entry_close_confirm:
                return True
            cl = float(df_day.at[eidx, "close"])
            return np.isfinite(cl) and cl > trigger

        def _make_trade(
            entry_idx: int, entry_price: float, setup: str, avwap_dist_atr: float
        ) -> tuple:
            exit_idx, exit_time, exit_price, outcome = simulate_exit_long(
                df_day, entry_idx, entry_price, cfg
            )
            net_pnl, gross_pnl = compute_pnl_pct(entry_price, exit_price, "LONG", cfg)
            adx_slope2 = (
                float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"]) if i >= 2 else 0.0
            )
            ema_gap_atr = (close1 - ema20) / atr1 if atr1 > 0 else 0.0
            qscore = compute_quality_score_long(
                adx1, adx_slope2, avwap_dist_atr, ema_gap_atr, impulse
            )
            return (
                Trade(
                    trade_date=day_str,
                    ticker=ticker,
                    side="LONG",
                    setup=setup,
                    impulse_type=impulse,
                    signal_time_ist=signal_time,
                    entry_time_ist=df_day.at[entry_idx, "date"],
                    entry_price=entry_price,
                    sl_price=entry_price * (1.0 - cfg.stop_pct),
                    target_price=entry_price * (1.0 + cfg.target_pct),
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
                    quality_score=qscore,
                ),
                exit_idx,
            )

        # ---------------------------------------------------------------
        # SETUP A (MODERATE): break C1 high, or pullback + break C2 high
        # ---------------------------------------------------------------
        if impulse == "MODERATE":
            c2    = df_day.iloc[i + 1]
            buf1  = entry_buffer(high1, cfg)
            trigger = high1 + buf1

            lag1 = int(cfg.lag_bars_long_a_mod_break_c1_high)
            entry_idx_1 = i + lag1 if lag1 >= 0 else (i + 1)
            if entry_idx_1 < len(df_day) and float(df_day.at[entry_idx_1, "high"]) > trigger:
                entry_idx = entry_idx_1
                entry_ts  = df_day.at[entry_idx, "date"]

                if not in_signal_window(entry_ts, cfg):
                    i += 1
                    continue
                if not _close_confirm_ok(entry_idx, trigger):
                    i += 1
                    continue
                if not _bars_left_ok(entry_idx):
                    i += 1
                    continue

                atr_entry = float(df_day.at[entry_idx, "ATR15"])
                ok_support, avwap_dist_atr = avwap_support_pass(
                    df_day, i, entry_idx, atr_entry, cfg
                )
                if not ok_support:
                    i += 1
                    continue

                trade, exit_idx = _make_trade(
                    entry_idx, trigger, "A_MOD_BREAK_C1_HIGH", avwap_dist_atr
                )
                trades.append(trade)
                i = exit_idx + 1
                continue

            # Option 2: small red pullback C2 above AVWAP, then break C2 high
            c2o, c2c = float(c2["open"]), float(c2["close"])
            c2_body = abs(c2c - c2o)
            c2_atr  = float(c2["ATR15"]) if np.isfinite(c2["ATR15"]) else atr1
            c2_avwap = float(c2["AVWAP"]) if np.isfinite(c2["AVWAP"]) else np.nan

            c2_small_red   = (c2c < c2o) and np.isfinite(c2_atr) and (c2_body <= cfg.small_counter_max_atr * c2_atr)
            c2_above_avwap = np.isfinite(c2_avwap) and (c2c > c2_avwap)

            if cfg.enable_setup_a_pullback_c2_break and c2_small_red and c2_above_avwap and (i + 2 < len(df_day)):
                high2   = float(c2["high"])
                buf2    = entry_buffer(high2, cfg)
                trigger2 = high2 + buf2

                lag2 = int(cfg.lag_bars_long_a_pullback_c2_break_c2_high)
                entry_idx_2 = i + lag2 if lag2 >= 0 else (i + 2)
                if entry_idx_2 < len(df_day) and float(df_day.at[entry_idx_2, "high"]) > trigger2:
                    entry_idx = entry_idx_2
                    entry_ts  = df_day.at[entry_idx, "date"]

                    if not in_signal_window(entry_ts, cfg):
                        i += 1
                        continue
                    if not _close_confirm_ok(entry_idx, trigger2):
                        i += 1
                        continue
                    if not _bars_left_ok(entry_idx):
                        i += 1
                        continue

                    atr_entry = float(df_day.at[entry_idx, "ATR15"])
                    ok_support, avwap_dist_atr = avwap_support_pass(
                        df_day, i, entry_idx, atr_entry, cfg
                    )
                    if not ok_support:
                        i += 1
                        continue

                    trade, exit_idx = _make_trade(
                        entry_idx, trigger2, "A_PULLBACK_C2_THEN_BREAK_C2_HIGH", avwap_dist_atr
                    )
                    trades.append(trade)
                    i = exit_idx + 1
                    continue

            i += 1
            continue

        # ---------------------------------------------------------------
        # SETUP B (HUGE): pullback holds, then break pullback high
        # ---------------------------------------------------------------
        if impulse == "HUGE":
            pull_end = min(i + 3, len(df_day) - 1)
            pull     = df_day.iloc[i + 1 : pull_end + 1].copy()
            if pull.empty:
                i += 1
                continue

            mid_body = (open1 + close1) / 2.0

            pull_atr  = pd.to_numeric(pull["ATR15"], errors="coerce").fillna(atr1)
            pull_body = (
                pd.to_numeric(pull["close"], errors="coerce")
                - pd.to_numeric(pull["open"], errors="coerce")
            ).abs()
            pull_red   = pd.to_numeric(pull["close"], errors="coerce") < pd.to_numeric(pull["open"], errors="coerce")
            pull_small = pull_body <= (cfg.small_counter_max_atr * pull_atr)

            if not bool((pull_red & pull_small).any()):
                i += 1
                continue

            lows   = pd.to_numeric(pull["low"],   errors="coerce")
            closes = pd.to_numeric(pull["close"], errors="coerce")
            avwaps = pd.to_numeric(pull["AVWAP"], errors="coerce")

            hold_mid   = bool((lows   > mid_body).fillna(False).all())
            hold_avwap = bool((closes > avwaps).fillna(False).all())
            if not (hold_mid or hold_avwap):
                i += 1
                continue

            pull_high = float(pd.to_numeric(pull["high"], errors="coerce").max())
            if not np.isfinite(pull_high):
                i += 1
                continue

            trigger = pull_high + entry_buffer(pull_high, cfg)
            entered = False

            lag_huge = int(cfg.lag_bars_long_b_huge_pullback_hold_break)
            if lag_huge >= 0:
                j_fixed = i + lag_huge
                j_iter  = [j_fixed] if (pull_end + 1 <= j_fixed < len(df_day)) else []
            else:
                j_iter = range(pull_end + 1, len(df_day))

            for j in j_iter:
                tsj = df_day.at[j, "date"]
                if not in_signal_window(tsj, cfg):
                    continue

                closej = float(df_day.at[j, "close"])
                avwapj = (
                    float(df_day.at[j, "AVWAP"])
                    if np.isfinite(df_day.at[j, "AVWAP"])
                    else np.nan
                )
                if np.isfinite(avwapj) and closej <= avwapj:
                    break

                if float(df_day.at[j, "high"]) > trigger:
                    if not _close_confirm_ok(j, trigger):
                        continue
                    if not _bars_left_ok(j):
                        continue

                    atr_entry = float(df_day.at[j, "ATR15"])
                    ok_support, avwap_dist_atr = avwap_support_pass(
                        df_day, i, j, atr_entry, cfg
                    )
                    if not ok_support:
                        continue

                    trade, exit_idx = _make_trade(
                        j, trigger, "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK", avwap_dist_atr
                    )
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
# SCAN ALL DAYS — V5 (LONG)
# Adds Supertrend + MACD to each day slice before calling scan_one_day_v5
# ===========================================================================
def scan_all_days_for_ticker_v5(
    ticker: str, df_full: pd.DataFrame, cfg: StrategyConfig
) -> List[Trade]:
    """
    Like scan_all_days_for_ticker but:
      1. Computes Supertrend + MACD on the full session DataFrame
      2. Passes pre-computed columns into each day slice
      3. Calls scan_one_day_v5 (which checks _trend_filter_long_v5)
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
    _, st_dir_series     = compute_supertrend(df, period=10, multiplier=3.0)
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
def run_long_scan_v5(cfg: Optional[StrategyConfig] = None) -> List[Trade]:
    if cfg is None:
        cfg = default_long_config()

    tickers = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    print(f"[LONG-v5] Tickers found: {len(tickers)}")

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
            print(f"  [LONG-v5] scanned {k}/{len(tickers)} | trades_so_far={len(all_trades)}")

    return all_trades
