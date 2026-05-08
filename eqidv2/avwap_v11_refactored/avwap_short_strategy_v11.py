# -*- coding: utf-8 -*-
"""
avwap_short_strategy.py â€” SHORT side logic for AVWAP v11
========================================================

Only contains direction-specific code:
- Red impulse classification
- AVWAP rejection checks (price below AVWAP)
- Short exit simulation
- Short-specific signal validation
- scan_one_day / scan_all_days entry points

All shared infrastructure comes from avwap_common.
"""

from __future__ import annotations

import dataclasses
import os
from typing import List, Optional

import numpy as np
import pandas as pd

from avwap_v11_refactored.avwap_common_v11 import (
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
    get_vix_scale,
    has_recent_liquidity_sweep,
    avwap_no_trade_zone_block,
    market_regime_pass,
    vwap_side_persistence_pass,
    swing_structure_pass,
    compute_risk_position_size_rs,
    select_day_mode,
    prepare_session_bars_for_scan,
)


# ===========================================================================
# IMPULSE CLASSIFICATION (RED CANDLES)
# ===========================================================================
def classify_red_impulse(row: pd.Series, cfg: StrategyConfig) -> str:
    o = float(row["open"])
    c = float(row["close"])
    h = float(row["high"])
    low = float(row["low"])
    atr = float(row["ATR15"])

    if not np.isfinite(atr) or atr <= 0:
        return ""
    if c >= o:  # must be red
        return ""

    body = abs(c - o)
    rng = h - low
    if not np.isfinite(rng) or rng <= 0:
        return ""

    close_near_low = ((c - low) / rng) <= cfg.close_near_extreme_max

    if (body >= cfg.huge_impulse_min_atr * atr) or (
        rng >= cfg.huge_impulse_min_range_atr * atr
    ):
        return "HUGE"

    if (
        body >= cfg.mod_impulse_min_atr * atr
        and body <= cfg.mod_impulse_max_atr * atr
        and close_near_low
    ):
        return "MODERATE"

    return ""


# ===========================================================================
# AVWAP REJECTION (SHORT: price must be BELOW AVWAP)
# ===========================================================================
def avwap_rejection_pass(
    df_day: pd.DataFrame, impulse_idx: int, entry_idx: int, cfg: StrategyConfig
) -> bool:
    if not cfg.require_avwap_rule:
        return True
    if "AVWAP" not in df_day.columns:
        return False
    if entry_idx <= impulse_idx:
        return False

    win = df_day.iloc[impulse_idx + 1 : entry_idx + 1]
    if win.empty:
        return False

    # Touch evidence: high >= AVWAP and close < AVWAP
    touch_ok = False
    if cfg.avwap_touch:
        hi = pd.to_numeric(win["high"], errors="coerce")
        cl = pd.to_numeric(win["close"], errors="coerce")
        av = pd.to_numeric(win["AVWAP"], errors="coerce")
        touch_ok = bool(((hi >= av) & (cl < av)).fillna(False).any())

    # Consecutive closes below AVWAP
    consec_ok = False
    n = cfg.avwap_min_consec_closes
    if n > 0:
        win_full = df_day.iloc[impulse_idx : entry_idx + 1]
        cl_f = pd.to_numeric(win_full["close"], errors="coerce")
        av_f = pd.to_numeric(win_full["AVWAP"], errors="coerce")
        below = ((cl_f < av_f) & np.isfinite(cl_f) & np.isfinite(av_f)).to_numpy(
            dtype=bool
        )
        consec_ok = max_consecutive_true(below) >= n

    mode = cfg.avwap_mode.strip().lower()
    if mode == "both":
        return bool(touch_ok and consec_ok)
    return bool(touch_ok or consec_ok)


def avwap_distance_pass(
    df_day: pd.DataFrame, idx: int, cfg: StrategyConfig
) -> bool:
    mult = cfg.avwap_dist_atr_mult
    if mult <= 0:
        return True
    if "AVWAP" not in df_day.columns:
        return False

    av = float(df_day.at[idx, "AVWAP"])
    cl = float(df_day.at[idx, "close"])
    atr = float(df_day.at[idx, "ATR15"])

    if not (np.isfinite(av) and np.isfinite(cl) and np.isfinite(atr) and atr > 0):
        return False

    return (av - cl) >= (mult * atr)


# ===========================================================================
# EXIT SIMULATION (SHORT)
# ===========================================================================
def simulate_exit_short(
    df_day: pd.DataFrame, entry_idx: int, entry_price: float, cfg: StrategyConfig
) -> tuple:
    """
    Walk forward within day until TARGET / SL / BE / TRAIL / EOD.

    Supports optional partial exits:
    - partial size at T1 (fraction of full target distance)
    - remaining runner shifts SL to entry and continues to full target / BE / SL / EOD
    """
    sl0 = entry_price * (1.0 + cfg.stop_pct)
    tgt_full = entry_price * (1.0 - cfg.target_pct)
    tgt_t1 = entry_price * (1.0 - (cfg.target_pct * cfg.partial_target_fraction))

    sl = sl0
    be_armed = False
    be_trigger = entry_price * (1.0 - cfg.be_trigger_pct)
    be_sl = entry_price * (1.0 + cfg.be_pad_pct)
    best_price = entry_price  # best (lowest) price seen for short

    partial_enabled = bool(cfg.enable_partial_exit)
    partial_weight = float(np.clip(cfg.partial_exit_fraction, 0.0, 1.0))
    partial_taken = False
    realized_weighted_exit = 0.0
    runner_weight = 1.0

    for k in range(entry_idx + 1, len(df_day)):
        hi = float(df_day.at[k, "high"])
        lo = float(df_day.at[k, "low"])
        ts = df_day.at[k, "date"]

        # Track best favorable price (lowest for SHORT)
        if np.isfinite(lo) and lo < best_price:
            best_price = lo

        if cfg.enable_breakeven and (not be_armed) and np.isfinite(lo) and lo <= be_trigger:
            be_armed = True
            sl = min(sl, be_sl)

        if be_armed and cfg.enable_trailing_stop:
            trail_sl = best_price * (1.0 + cfg.trail_pct)
            sl = min(sl, trail_sl)  # trailing stop can only move down (tighter)

        if not partial_taken:
            hit_sl = np.isfinite(hi) and (hi >= sl)
            hit_t1 = partial_enabled and np.isfinite(lo) and (lo <= tgt_t1)
            hit_tfull = np.isfinite(lo) and (lo <= tgt_full)

            if hit_sl and (hit_t1 or hit_tfull):
                return k, ts, float(sl), "SL", partial_taken
            if hit_sl:
                return k, ts, float(sl), "SL", partial_taken

            if hit_tfull:
                return k, ts, float(tgt_full), "TARGET", partial_taken

            if hit_t1:
                partial_taken = True
                realized_weighted_exit = partial_weight * float(tgt_t1)
                runner_weight = 1.0 - partial_weight
                if runner_weight <= 1e-9:
                    return k, ts, float(tgt_t1), "TARGET", partial_taken
                if cfg.move_sl_to_entry_after_partial:
                    be_armed = True
                    sl = min(sl, be_sl)
                continue
            continue

        # Runner leg after partial
        hit_sl = np.isfinite(hi) and (hi >= sl)
        hit_tfull = np.isfinite(lo) and (lo <= tgt_full)

        if hit_sl and hit_tfull:
            runner_px = float(sl)
            eff_exit = realized_weighted_exit + (runner_weight * runner_px)
            runner_outcome = "BE" if runner_px <= be_sl else "SL"
            return k, ts, float(eff_exit), runner_outcome, partial_taken
        if hit_sl:
            runner_px = float(sl)
            eff_exit = realized_weighted_exit + (runner_weight * runner_px)
            runner_outcome = "BE" if runner_px <= be_sl else "SL"
            return k, ts, float(eff_exit), runner_outcome, partial_taken
        if hit_tfull:
            eff_exit = realized_weighted_exit + (runner_weight * float(tgt_full))
            return k, ts, float(eff_exit), "TARGET", partial_taken

    last = len(df_day) - 1
    eod_px = float(df_day.at[last, "close"])
    if partial_taken:
        eod_px = realized_weighted_exit + (runner_weight * eod_px)
    return last, df_day.at[last, "date"], float(eod_px), "EOD", partial_taken


# ===========================================================================
# TREND FILTER VALIDATION (SHORT)
# ===========================================================================
def _trend_filter_short(
    df_day: pd.DataFrame, i: int, c1: pd.Series, cfg: StrategyConfig
) -> bool:
    """Returns True if all Option-A trend conditions pass for SHORT."""
    adx1 = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    rsi1 = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    k1 = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else np.nan
    d1 = float(df_day.at[i, "STOCHD15"]) if "STOCHD15" in df_day.columns else np.nan

    adx_ok = (
        np.isfinite(adx1)
        and adx1 >= cfg.adx_min
        and twice_increasing(df_day, i, "ADX15")
        and adx_slope_ok(df_day, i, "ADX15", cfg.adx_slope_min)
    )
    rsi_ok = (
        np.isfinite(rsi1) and rsi1 <= cfg.rsi_max_short and twice_reducing(df_day, i, "RSI15")
    )
    stoch_ok = (
        np.isfinite(k1)
        and np.isfinite(d1)
        and k1 <= cfg.stochk_max
        and k1 < d1
        and twice_reducing(df_day, i, "STOCHK15")
    )

    if not (adx_ok and rsi_ok and stoch_ok):
        return False

    if not vwap_side_persistence_pass(df_day, i, "SHORT", cfg):
        return False
    if not swing_structure_pass(df_day, i, "SHORT", cfg):
        return False

    # Strict EMA + AVWAP
    close1 = float(c1["close"])
    avwap1 = float(c1["AVWAP"]) if np.isfinite(c1.get("AVWAP", np.nan)) else np.nan
    ema20 = float(c1["EMA20"]) if np.isfinite(c1.get("EMA20", np.nan)) else np.nan
    ema50 = float(c1["EMA50"]) if np.isfinite(c1.get("EMA50", np.nan)) else np.nan
    ema200 = float(c1["EMA200"]) if np.isfinite(c1.get("EMA200", np.nan)) else np.nan

    if not (np.isfinite(avwap1) and np.isfinite(ema20) and np.isfinite(ema50)):
        return False

    trend_ok = (ema20 < ema50) and (close1 < ema20) and (close1 < avwap1)
    if cfg.enable_ema200_filter:
        if not np.isfinite(ema200):
            return False
        trend_ok = trend_ok and (close1 < ema200)
    return bool(trend_ok)


def _reversal_filter_short(
    df_day: pd.DataFrame, i: int, c1: pd.Series, cfg: StrategyConfig
) -> bool:
    """
    Reversal filter for SHORT:
    - reclaim/reject under AVWAP
    - momentum flip (K < D and RSI softening)
    """
    close1 = float(c1.get("close", np.nan))
    open1 = float(c1.get("open", np.nan))
    high1 = float(c1.get("high", np.nan))
    avwap1 = float(c1.get("AVWAP", np.nan))
    atr1 = float(c1.get("ATR15", np.nan))
    k1 = float(c1.get("STOCHK15", np.nan))
    d1 = float(c1.get("STOCHD15", np.nan))
    rsi1 = float(c1.get("RSI15", np.nan))

    if not (
        np.isfinite(close1)
        and np.isfinite(open1)
        and np.isfinite(high1)
        and np.isfinite(avwap1)
        and np.isfinite(atr1)
        and atr1 > 0
    ):
        return False

    reject_avwap = (high1 >= avwap1) and (close1 < avwap1)
    body_ok = abs(close1 - open1) >= (0.30 * atr1)
    stoch_flip = np.isfinite(k1) and np.isfinite(d1) and (k1 < d1)
    rsi_soft = np.isfinite(rsi1) and (rsi1 <= max(55.0, cfg.rsi_max_short + 5.0))
    return bool(reject_avwap and body_ok and stoch_flip and rsi_soft)


# ===========================================================================
# D_AVWAP_LOSE_REVERSAL (v17j) — SHORT mirror of LONG B_AVWAP_RECLAIM_REVERSAL.
# Price was above AVWAP in the last 1-2 bars; current bar closes below AVWAP
# with a decisive red body, falling RSI, K<D, ADX>=threshold, above-avg volume,
# and the bar is before short_reversal_max_hour_ist (IST). Entry on the
# configured lag bar via a breakdown below the lose-bar's low.
# ===========================================================================
def _scan_reversal_at_short(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
) -> tuple:
    """Try the AVWAP-lose reversal SHORT setup at bar index ``i``.

    Returns (trade, exit_idx) on success, (None, -1) on miss.
    """
    if i < 2 or (i + 2) >= len(df_day):
        return None, -1

    R = df_day.iloc[i]

    # Time-of-day gate (IST)
    R_ts = R["date"]
    try:
        R_hour = R_ts.tz_convert(IST).hour
    except Exception:
        R_hour = getattr(R_ts, "hour", 0)
    if R_hour >= int(getattr(cfg, "short_reversal_max_hour_ist", 13)):
        return None, -1

    R_close = float(R["close"])
    R_open = float(R["open"])
    R_high = float(R["high"])
    R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))
    R_avwap = float(R.get("AVWAP", np.nan))

    if not (
        np.isfinite(R_atr)
        and R_atr > 0
        and np.isfinite(R_avwap)
        and np.isfinite(R_close)
        and np.isfinite(R_open)
        and np.isfinite(R_high)
        and np.isfinite(R_low)
        and (R_high - R_low) > 0
        and R_close > 0
    ):
        return None, -1

    # Lose: close below AVWAP after at least one prior bar had high above.
    if R_close >= R_avwap:
        return None, -1

    require_both = bool(getattr(cfg, "short_reversal_require_both_prior_bars", True))
    prior_bars_ok = True if require_both else False
    seen_any = False
    for k in (i - 1, i - 2):
        if k < 0:
            prior_bars_ok = False if require_both else prior_bars_ok
            continue
        try:
            prev_high = float(df_day.at[k, "high"])
            prev_avwap = float(df_day.at[k, "AVWAP"])
        except (KeyError, ValueError, TypeError):
            if require_both:
                prior_bars_ok = False
            continue
        above = (
            np.isfinite(prev_high)
            and np.isfinite(prev_avwap)
            and prev_high >= prev_avwap
        )
        if require_both:
            if not above:
                prior_bars_ok = False
                break
            seen_any = True
        else:
            if above:
                prior_bars_ok = True
                break
    if require_both and not seen_any:
        prior_bars_ok = False
    if not prior_bars_ok:
        return None, -1

    R_rng = R_high - R_low
    R_body = abs(R_close - R_open)
    body_min = float(getattr(cfg, "short_reversal_body_atr_min", 0.50))
    if (R_body / R_atr) < body_min:
        return None, -1
    lower_pct = float(getattr(cfg, "short_reversal_close_lower_pct", 0.40))
    # close must be in the lower `lower_pct` of the bar range
    if R_close > (R_low + lower_pct * R_rng):
        return None, -1

    # Momentum: RSI below threshold and falling
    rsi_max = float(getattr(cfg, "short_reversal_rsi_max", 50.0))
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_prev = float(df_day.at[i - 1, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    if not (
        np.isfinite(rsi_now)
        and np.isfinite(rsi_prev)
        and rsi_now <= rsi_max
        and rsi_now < rsi_prev
    ):
        return None, -1

    adx_min = float(getattr(cfg, "short_reversal_adx_min", 28.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1

    # Trend context: close below EMA20
    if bool(getattr(cfg, "short_reversal_require_close_lt_ema20", True)):
        ema20_now = float(df_day.at[i, "EMA20"]) if "EMA20" in df_day.columns else np.nan
        if not (np.isfinite(ema20_now) and R_close < ema20_now):
            return None, -1

    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else np.nan
    d_now = float(df_day.at[i, "STOCHD15"]) if "STOCHD15" in df_day.columns else np.nan
    if not (np.isfinite(k_now) and np.isfinite(d_now) and k_now < d_now):
        return None, -1

    # Volume
    vol_now = (
        float(R.get("volume", 0.0))
        if np.isfinite(R.get("volume", np.nan))
        else 0.0
    )
    vol_sma = (
        float(R.get("VOL_SMA20", 0.0))
        if np.isfinite(R.get("VOL_SMA20", np.nan))
        else 0.0
    )
    if vol_sma > 0 and vol_now < float(cfg.short_reversal_volume_min_ratio) * vol_sma:
        return None, -1
    vol_cap_ratio = float(getattr(cfg, "short_reversal_volume_max_ratio", 0.0) or 0.0)
    if vol_cap_ratio > 0.0 and vol_sma > 0 and vol_now > vol_cap_ratio * vol_sma:
        return None, -1

    # Entry: breakdown below R.low on lag bar
    buf_rev = entry_buffer(R_low, cfg)
    trigger_rev = R_low - buf_rev
    lag_rev = int(cfg.lag_bars_short_d_avwap_lose_reversal)
    rev_entry_idx = -1

    if lag_rev >= 0:
        cand = i + lag_rev
        if (
            cand < len(df_day)
            and in_signal_window(df_day.at[cand, "date"], cfg)
        ):
            lo_e = float(df_day.at[cand, "low"])
            cl_e = float(df_day.at[cand, "close"])
            if (
                np.isfinite(lo_e)
                and np.isfinite(cl_e)
                and lo_e < trigger_rev
                and (not cfg.require_entry_close_confirm or cl_e < trigger_rev)
            ):
                rev_entry_idx = cand
    else:
        for jj in range(i + 1, min(len(df_day), i + 4)):
            if not in_signal_window(df_day.at[jj, "date"], cfg):
                continue
            lo_jj = float(df_day.at[jj, "low"])
            cl_jj = float(df_day.at[jj, "close"])
            if (
                np.isfinite(lo_jj)
                and np.isfinite(cl_jj)
                and lo_jj < trigger_rev
                and (not cfg.require_entry_close_confirm or cl_jj < trigger_rev)
            ):
                rev_entry_idx = jj
                break

    if rev_entry_idx < 0:
        return None, -1
    if (len(df_day) - 1 - rev_entry_idx) < int(cfg.min_bars_left_after_entry):
        return None, -1

    # Entry price selection — SHORT side uses trigger price by default
    # (mirrors the existing SHORT _make_trade pattern in scan_one_day).
    if bool(getattr(cfg, "entry_at_next_open", False)):
        nxt = rev_entry_idx + 1
        if nxt >= len(df_day):
            return None, -1
        ep_rev = float(df_day.at[nxt, "open"])
    elif bool(getattr(cfg, "entry_at_bar_close", False)):
        ep_rev = float(df_day.at[rev_entry_idx, "close"])
    else:
        ep_rev = float(trigger_rev)

    max_slip = float(getattr(cfg, "max_entry_slip_pct", 0.0))
    if max_slip > 0.0 and ep_rev < trigger_rev * (1.0 - max_slip):
        return None, -1

    # AVWAP distance at entry — SHORT convention: positive = below AVWAP
    atr_entry_rev = float(df_day.at[rev_entry_idx, "ATR15"])
    avwap_entry = (
        float(df_day.at[rev_entry_idx, "AVWAP"])
        if np.isfinite(df_day.at[rev_entry_idx, "AVWAP"])
        else np.nan
    )
    close_entry = float(df_day.at[rev_entry_idx, "close"])
    if (
        np.isfinite(atr_entry_rev)
        and atr_entry_rev > 0
        and np.isfinite(avwap_entry)
    ):
        avwap_dist_atr_rev = (avwap_entry - close_entry) / atr_entry_rev
    else:
        avwap_dist_atr_rev = 0.0

    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr_rev > cap:
        return None, -1
    dist_min = float(getattr(cfg, "short_reversal_avwap_dist_atr_min", 0.0) or 0.0)
    if dist_min > 0.0 and avwap_dist_atr_rev < dist_min:
        return None, -1

    # Diagnostics for Trade row
    ema20_R = float(R.get("EMA20", np.nan))
    ema_gap_atr_rev = (
        (ema20_R - R_close) / R_atr
        if (np.isfinite(ema20_R) and R_atr > 0)
        else 0.0
    )
    qscore_rev = compute_quality_score_short(
        adx_now, avwap_dist_atr_rev, ema_gap_atr_rev, "REVERSAL"
    )
    atr_pct_rev = (R_atr / R_close) if R_close > 0 else 0.0

    (
        exit_idx_rev,
        exit_time_rev,
        exit_price_rev,
        outcome_rev,
        partial_taken_rev,
    ) = simulate_exit_short(df_day, rev_entry_idx, ep_rev, cfg)
    net_pnl_rev, gross_pnl_rev = compute_pnl_pct(
        ep_rev, exit_price_rev, "SHORT", cfg
    )

    trade = Trade(
        trade_date=day_str,
        ticker=ticker,
        side="SHORT",
        setup="D_AVWAP_LOSE_REVERSAL",
        impulse_type="REVERSAL",
        signal_time_ist=R_ts,
        entry_time_ist=df_day.at[rev_entry_idx, "date"],
        entry_price=ep_rev,
        sl_price=ep_rev * (1.0 + cfg.stop_pct),
        target_price=ep_rev * (1.0 - cfg.target_pct),
        exit_time_ist=exit_time_rev,
        exit_price=exit_price_rev,
        outcome=outcome_rev,
        pnl_pct=net_pnl_rev,
        pnl_pct_gross=gross_pnl_rev,
        signal_price=R_close,
        partial_exit_taken=bool(partial_taken_rev),
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr_rev,
        ema20_gap_atr_signal=ema_gap_atr_rev,
        atr_pct_signal=atr_pct_rev,
        quality_score=qscore_rev,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx_rev)


# ===========================================================================
# C_OR_BREAKDOWN (v17k) — Opening-range breakdown SHORT (mirror of LONG OR_BO)
# ===========================================================================
def _scan_or_breakdown_at(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
    or_high: float,
    or_low: float,
    or_width_pct: float,
) -> tuple:
    if i < 2 or (i + 2) >= len(df_day):
        return None, -1
    R = df_day.iloc[i]
    R_ts = R["date"]
    try:
        R_hour = R_ts.tz_convert(IST).hour
    except Exception:
        R_hour = getattr(R_ts, "hour", 0)
    if R_hour >= int(getattr(cfg, "or_breakdown_max_hour_ist", 11)):
        return None, -1

    R_close = float(R["close"]); R_open = float(R["open"])
    R_high = float(R["high"]); R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))

    if not (np.isfinite(R_atr) and R_atr > 0 and (R_high - R_low) > 0 and R_close > 0):
        return None, -1
    if not (
        float(getattr(cfg, "or_breakdown_min_width_pct", 0.0)) <= or_width_pct
        <= float(getattr(cfg, "or_breakdown_max_width_pct", 99.0))
    ):
        return None, -1
    # Breakdown: bar low breaks OR low and close confirms
    if not (R_low < or_low and R_close < or_low):
        return None, -1

    adx_min = float(getattr(cfg, "or_breakdown_adx_min", 22.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1

    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    if vol_sma > 0 and vol_now < float(cfg.or_breakdown_volume_min_ratio) * vol_sma:
        return None, -1

    buf = entry_buffer(R_low, cfg)
    trigger = R_low - buf
    lag = int(cfg.or_breakdown_lag_bars)
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    lo_e = float(df_day.at[cand, "low"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(lo_e) and np.isfinite(cl_e) and lo_e < trigger
            and (not cfg.require_entry_close_confirm or cl_e < trigger)):
        return None, -1
    entry_idx = cand
    if (len(df_day) - 1 - entry_idx) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep = float(trigger)
    atr_entry = float(df_day.at[entry_idx, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx, "AVWAP"]) else np.nan
    )
    close_entry = float(df_day.at[entry_idx, "close"])
    avwap_dist_atr = (
        (avwap_entry - close_entry) / atr_entry
        if (np.isfinite(atr_entry) and atr_entry > 0 and np.isfinite(avwap_entry)) else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr > cap:
        return None, -1

    ema20 = float(R.get("EMA20", np.nan))
    ema_gap_atr = (ema20 - R_close) / R_atr if (np.isfinite(ema20) and R_atr > 0) else 0.0
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    qscore = compute_quality_score_short(adx_now, avwap_dist_atr, ema_gap_atr, "OR")
    atr_pct = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx, exit_time, exit_price, outcome, partial_taken = simulate_exit_short(
        df_day, entry_idx, ep, cfg
    )
    net_pnl, gross_pnl = compute_pnl_pct(ep, exit_price, "SHORT", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="SHORT",
        setup="C_OR_BREAKDOWN", impulse_type="OR",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx, "date"],
        entry_price=ep, sl_price=ep * (1.0 + cfg.stop_pct),
        target_price=ep * (1.0 - cfg.target_pct),
        exit_time_ist=exit_time, exit_price=exit_price, outcome=outcome,
        pnl_pct=net_pnl, pnl_pct_gross=gross_pnl,
        signal_price=R_close, partial_exit_taken=bool(partial_taken),
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr,
        ema20_gap_atr_signal=ema_gap_atr,
        atr_pct_signal=atr_pct,
        quality_score=qscore,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx)


# ===========================================================================
# D_EMA20_REJECTION (v17k) — Pullback to EMA20 from below + bearish reject
# ===========================================================================
def _scan_ema20_rejection_at(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
) -> tuple:
    if i < 2 or (i + 2) >= len(df_day):
        return None, -1
    R = df_day.iloc[i]
    R_ts = R["date"]
    try:
        R_hour = R_ts.tz_convert(IST).hour
    except Exception:
        R_hour = getattr(R_ts, "hour", 0)
    if R_hour >= int(getattr(cfg, "ema20_rejection_max_hour_ist", 14)):
        return None, -1

    R_close = float(R["close"]); R_open = float(R["open"])
    R_high = float(R["high"]); R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))
    R_ema20 = float(R.get("EMA20", np.nan))

    if not (
        np.isfinite(R_atr) and R_atr > 0 and np.isfinite(R_ema20)
        and (R_high - R_low) > 0 and R_close > 0
    ):
        return None, -1

    # Bar high must touch EMA20 (from below, within proximity)
    proximity = float(getattr(cfg, "ema20_rejection_atr_proximity", 0.30)) * R_atr
    if not (R_high >= R_ema20 - proximity and R_high < R_ema20 + proximity * 2.0):
        return None, -1
    # Must close below EMA20 (rejection downward)
    if R_close >= R_ema20:
        return None, -1
    # Bearish bar with strong body
    if R_close >= R_open:
        return None, -1
    body_min = float(getattr(cfg, "ema20_rejection_body_atr_min", 0.40))
    if abs(R_close - R_open) / R_atr < body_min:
        return None, -1
    # Close in lower half
    lower_pct = float(getattr(cfg, "ema20_rejection_close_lower_pct", 0.50))
    if R_close > R_low + lower_pct * (R_high - R_low):
        return None, -1

    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_prev = float(df_day.at[i - 1, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_max = float(getattr(cfg, "ema20_rejection_rsi_max", 50.0))
    if not (np.isfinite(rsi_now) and np.isfinite(rsi_prev) and rsi_now <= rsi_max and rsi_now < rsi_prev):
        return None, -1
    adx_min = float(getattr(cfg, "ema20_rejection_adx_min", 22.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1

    if bool(getattr(cfg, "ema20_rejection_require_close_lt_ema50", True)):
        ema50 = float(df_day.at[i, "EMA_50"]) if "EMA_50" in df_day.columns else np.nan
        if not (np.isfinite(ema50) and R_close < ema50):
            return None, -1

    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    vol_min = float(getattr(cfg, "ema20_rejection_volume_min_ratio", 1.20))
    if vol_sma > 0 and vol_now < vol_min * vol_sma:
        return None, -1

    buf = entry_buffer(R_low, cfg)
    trigger = R_low - buf
    lag = int(getattr(cfg, "ema20_rejection_lag_bars", 1))
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    lo_e = float(df_day.at[cand, "low"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(lo_e) and np.isfinite(cl_e) and lo_e < trigger
            and (not cfg.require_entry_close_confirm or cl_e < trigger)):
        return None, -1
    entry_idx = cand
    if (len(df_day) - 1 - entry_idx) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep = float(trigger)
    atr_entry = float(df_day.at[entry_idx, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx, "AVWAP"]) else np.nan
    )
    close_entry = float(df_day.at[entry_idx, "close"])
    avwap_dist_atr = (
        (avwap_entry - close_entry) / atr_entry
        if (np.isfinite(atr_entry) and atr_entry > 0 and np.isfinite(avwap_entry)) else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr > cap:
        return None, -1

    ema_gap_atr = (R_ema20 - R_close) / R_atr
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    qscore = compute_quality_score_short(adx_now, avwap_dist_atr, ema_gap_atr, "EMA20")
    atr_pct = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx, exit_time, exit_price, outcome, partial_taken = simulate_exit_short(
        df_day, entry_idx, ep, cfg
    )
    net_pnl, gross_pnl = compute_pnl_pct(ep, exit_price, "SHORT", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="SHORT",
        setup="D_EMA20_REJECTION", impulse_type="EMA20",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx, "date"],
        entry_price=ep, sl_price=ep * (1.0 + cfg.stop_pct),
        target_price=ep * (1.0 - cfg.target_pct),
        exit_time_ist=exit_time, exit_price=exit_price, outcome=outcome,
        pnl_pct=net_pnl, pnl_pct_gross=gross_pnl,
        signal_price=R_close, partial_exit_taken=bool(partial_taken),
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr,
        ema20_gap_atr_signal=ema_gap_atr,
        atr_pct_signal=atr_pct,
        quality_score=qscore,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx)


# ===========================================================================
# E_VWAP_BAND_FADE (v17k) — Upper Bollinger touch + bearish reject (SHORT).
# Mean-reversion play: bar high pierces Upper_Band, closes back below with
# bearish body; overbought RSI; entry on lag=1 break of bar low.
# ===========================================================================
def _scan_vwap_band_fade_short_at(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
) -> tuple:
    if i < 2 or (i + 2) >= len(df_day):
        return None, -1
    R = df_day.iloc[i]
    R_ts = R["date"]
    try:
        R_hour = R_ts.tz_convert(IST).hour
    except Exception:
        R_hour = getattr(R_ts, "hour", 0)
    if R_hour >= int(getattr(cfg, "vwap_band_fade_max_hour_ist", 14)):
        return None, -1

    R_close = float(R["close"]); R_open = float(R["open"])
    R_high = float(R["high"]); R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))
    if not (np.isfinite(R_atr) and R_atr > 0 and (R_high - R_low) > 0 and R_close > 0):
        return None, -1

    # Upper_Band touch from below + reject back below
    R_upper_band = float(R.get("Upper_Band", np.nan))
    if not (np.isfinite(R_upper_band) and R_high >= R_upper_band and R_close < R_upper_band):
        return None, -1
    # Bearish bar with strong body
    if R_close >= R_open:
        return None, -1
    body_min = float(getattr(cfg, "vwap_band_fade_body_atr_min", 0.40))
    if abs(R_close - R_open) / R_atr < body_min:
        return None, -1
    lower_pct = float(getattr(cfg, "vwap_band_fade_close_lower_pct", 0.50))
    if R_close > R_low + lower_pct * (R_high - R_low):
        return None, -1

    # Overbought RSI
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_min = float(getattr(cfg, "vwap_band_fade_rsi_min", 60.0))
    if not (np.isfinite(rsi_now) and rsi_now >= rsi_min):
        return None, -1

    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    vol_min = float(getattr(cfg, "vwap_band_fade_volume_min_ratio", 1.50))
    if vol_sma > 0 and vol_now < vol_min * vol_sma:
        return None, -1

    buf = entry_buffer(R_low, cfg)
    trigger = R_low - buf
    lag = int(getattr(cfg, "vwap_band_fade_lag_bars", 1))
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    lo_e = float(df_day.at[cand, "low"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(lo_e) and np.isfinite(cl_e) and lo_e < trigger
            and (not cfg.require_entry_close_confirm or cl_e < trigger)):
        return None, -1
    entry_idx = cand
    if (len(df_day) - 1 - entry_idx) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep = float(trigger)
    atr_entry = float(df_day.at[entry_idx, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx, "AVWAP"]) else np.nan
    )
    close_entry = float(df_day.at[entry_idx, "close"])
    avwap_dist_atr = (
        (avwap_entry - close_entry) / atr_entry
        if (np.isfinite(atr_entry) and atr_entry > 0 and np.isfinite(avwap_entry)) else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr > cap:
        return None, -1

    R_ema20 = float(R.get("EMA20", np.nan))
    ema_gap_atr = (R_ema20 - R_close) / R_atr if (np.isfinite(R_ema20) and R_atr > 0) else 0.0
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    qscore = compute_quality_score_short(adx_now, avwap_dist_atr, ema_gap_atr, "VWAP_BAND")
    atr_pct = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx, exit_time, exit_price, outcome, partial_taken = simulate_exit_short(
        df_day, entry_idx, ep, cfg
    )
    net_pnl, gross_pnl = compute_pnl_pct(ep, exit_price, "SHORT", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="SHORT",
        setup="E_VWAP_BAND_FADE", impulse_type="VWAP_BAND",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx, "date"],
        entry_price=ep, sl_price=ep * (1.0 + cfg.stop_pct),
        target_price=ep * (1.0 - cfg.target_pct),
        exit_time_ist=exit_time, exit_price=exit_price, outcome=outcome,
        pnl_pct=net_pnl, pnl_pct_gross=gross_pnl,
        signal_price=R_close, partial_exit_taken=bool(partial_taken),
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr,
        ema20_gap_atr_signal=ema_gap_atr,
        atr_pct_signal=atr_pct,
        quality_score=qscore,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx)


# ===========================================================================
# G_LOWER_LOW_BREAK (v17k) — Break below N-bar swing low (SHORT).
# Mirror of LONG G_HIGHER_HIGH_BREAK.
# ===========================================================================
def _scan_lower_low_break_at(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
) -> tuple:
    if i < 2 or (i + 2) >= len(df_day):
        return None, -1
    R = df_day.iloc[i]
    R_ts = R["date"]
    try:
        R_hour = R_ts.tz_convert(IST).hour
    except Exception:
        R_hour = getattr(R_ts, "hour", 0)
    if R_hour >= int(getattr(cfg, "g_ll_max_hour_ist", 14)):
        return None, -1

    R_close = float(R["close"]); R_open = float(R["open"])
    R_high = float(R["high"]); R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))
    if not (np.isfinite(R_atr) and R_atr > 0 and (R_high - R_low) > 0 and R_close > 0):
        return None, -1

    lookback = int(getattr(cfg, "g_ll_lookback_bars", 5))
    if i < lookback:
        return None, -1
    prior_lows = df_day["low"].iloc[i - lookback:i]
    swing_low = float(prior_lows.min())
    if not np.isfinite(swing_low):
        return None, -1
    # Break below swing low + close below
    if not (R_low < swing_low and R_close < swing_low):
        return None, -1

    # Bearish bar with body
    if R_close >= R_open:
        return None, -1
    body_min = float(getattr(cfg, "g_ll_body_atr_min", 0.40))
    if abs(R_close - R_open) / R_atr < body_min:
        return None, -1
    lower_pct = float(getattr(cfg, "g_ll_close_lower_pct", 0.40))
    if R_close > R_low + lower_pct * (R_high - R_low):
        return None, -1

    adx_min = float(getattr(cfg, "g_ll_adx_min", 22.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1
    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    vol_min = float(getattr(cfg, "g_ll_volume_min_ratio", 1.30))
    if vol_sma > 0 and vol_now < vol_min * vol_sma:
        return None, -1

    buf = entry_buffer(R_low, cfg)
    trigger = R_low - buf
    lag = int(getattr(cfg, "g_ll_lag_bars", 1))
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    lo_e = float(df_day.at[cand, "low"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(lo_e) and np.isfinite(cl_e) and lo_e < trigger
            and (not cfg.require_entry_close_confirm or cl_e < trigger)):
        return None, -1
    entry_idx = cand
    if (len(df_day) - 1 - entry_idx) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep = float(trigger)
    atr_entry = float(df_day.at[entry_idx, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx, "AVWAP"]) else np.nan
    )
    close_entry = float(df_day.at[entry_idx, "close"])
    avwap_dist_atr = (
        (avwap_entry - close_entry) / atr_entry
        if (np.isfinite(atr_entry) and atr_entry > 0 and np.isfinite(avwap_entry)) else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr > cap:
        return None, -1

    R_ema20 = float(R.get("EMA20", np.nan))
    ema_gap_atr = (R_ema20 - R_close) / R_atr if (np.isfinite(R_ema20) and R_atr > 0) else 0.0
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    qscore = compute_quality_score_short(adx_now, avwap_dist_atr, ema_gap_atr, "STRUCTURE")
    atr_pct = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx, exit_time, exit_price, outcome, partial_taken = simulate_exit_short(
        df_day, entry_idx, ep, cfg
    )
    net_pnl, gross_pnl = compute_pnl_pct(ep, exit_price, "SHORT", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="SHORT",
        setup="G_LOWER_LOW_BREAK", impulse_type="STRUCTURE",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx, "date"],
        entry_price=ep, sl_price=ep * (1.0 + cfg.stop_pct),
        target_price=ep * (1.0 - cfg.target_pct),
        exit_time_ist=exit_time, exit_price=exit_price, outcome=outcome,
        pnl_pct=net_pnl, pnl_pct_gross=gross_pnl,
        signal_price=R_close, partial_exit_taken=bool(partial_taken),
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr,
        ema20_gap_atr_signal=ema_gap_atr,
        atr_pct_signal=atr_pct,
        quality_score=qscore,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx)


# ===========================================================================
# H_FAILED_BREAKOUT_TRAP (Phase 2b) -- shorts trapped breakout buyers.
#
# Rejection bar `i` must:
#   * sweep above either Upper_Band[i] or Recent_High[i-1]
#   * fail to hold (close below the level it broke)
#   * have a large upper wick (>= cfg.fbt_min_upper_wick_pct, default 40%)
#   * close below VWAP[i]
#   * have hot MFI rolling over: MFI[i] >= cfg.fbt_min_mfi (70)
#                                AND MFI[i] < MFI[i-3]
#   * have decreasing MACD_Hist: MACD_Hist[i] < MACD_Hist[i-1]
#   * have volume burst: volume[i] / VOL_SMA20[i] >= cfg.fbt_min_vol_ratio (1.2)
#   * fire by entry_hour <= cfg.fbt_max_hour_ist (10)
#
# Entry on next bar break below rejection_low - buffer.
#
# Returns (None, -1) silently if any required indicator column is absent
# from df_day -- this lets the engine run unchanged on parquets that
# lack MFI / OBV / MACD_Hist / Upper_Band / Recent_High / VWAP.
# ===========================================================================
_FBT_REQUIRED_COLS = ("MFI", "MACD_Hist", "Upper_Band", "Recent_High", "VWAP")
_FBT_WARNED_MISSING_COLS = False


def _scan_failed_breakout_trap_at(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
) -> tuple:
    global _FBT_WARNED_MISSING_COLS
    if i < 4 or (i + 2) >= len(df_day):
        return None, -1

    missing = [c for c in _FBT_REQUIRED_COLS if c not in df_day.columns]
    if missing:
        if not _FBT_WARNED_MISSING_COLS:
            print(f"[FBT_WARN] required cols missing from prepared df: {missing}; "
                  f"FAILED_BREAKOUT_TRAP detection will be a no-op")
            _FBT_WARNED_MISSING_COLS = True
        return None, -1

    R = df_day.iloc[i]
    R_ts = R["date"]
    try:
        R_hour_dec = R_ts.tz_convert(IST).hour + R_ts.tz_convert(IST).minute / 60.0
    except Exception:
        R_hour_dec = float(getattr(R_ts, "hour", 0)) + float(getattr(R_ts, "minute", 0)) / 60.0
    fbt_max_hour = float(getattr(cfg, "fbt_max_hour_ist", 10.75))
    if R_hour_dec > fbt_max_hour:
        return None, -1

    R_open  = float(R["open"]);   R_close = float(R["close"])
    R_high  = float(R["high"]);   R_low   = float(R["low"])
    R_atr   = float(R.get("ATR15", np.nan))
    R_vwap  = float(R.get("VWAP", np.nan))
    R_mfi   = float(R.get("MFI", np.nan))
    R_mh    = float(R.get("MACD_Hist", np.nan))
    R_ub    = float(R.get("Upper_Band", np.nan))
    R_rh_prev = float(df_day.at[i - 1, "Recent_High"]) if "Recent_High" in df_day.columns else np.nan

    rng = R_high - R_low
    if not (np.isfinite(R_atr) and R_atr > 0 and rng > 0 and R_close > 0):
        return None, -1
    if not np.isfinite(R_vwap) or not np.isfinite(R_mfi) or not np.isfinite(R_mh):
        return None, -1

    # 1. Sweep + fail-to-hold (above Upper_Band OR above prior-bar Recent_High)
    swept_ub = np.isfinite(R_ub) and R_high > R_ub and R_close < R_ub
    swept_rh = np.isfinite(R_rh_prev) and R_high > R_rh_prev and R_close < R_rh_prev
    if not (swept_ub or swept_rh):
        return None, -1

    # 2. Large upper wick
    upper_wick_pct = (R_high - max(R_open, R_close)) / rng * 100.0
    if upper_wick_pct < float(getattr(cfg, "fbt_min_upper_wick_pct", 40.0)):
        return None, -1

    # 3. Close below session VWAP
    if R_close >= R_vwap:
        return None, -1

    # 4. Hot MFI rolling over
    if R_mfi < float(getattr(cfg, "fbt_min_mfi", 70.0)):
        return None, -1
    mfi_3back = float(df_day.at[i - 3, "MFI"]) if "MFI" in df_day.columns else np.nan
    if not (np.isfinite(mfi_3back) and R_mfi < mfi_3back):
        return None, -1

    # 5. MACD_Hist decreasing
    mh_prev = float(df_day.at[i - 1, "MACD_Hist"]) if "MACD_Hist" in df_day.columns else np.nan
    if not (np.isfinite(mh_prev) and R_mh < mh_prev):
        return None, -1

    # 6. Volume burst
    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    if vol_sma > 0 and vol_now < float(getattr(cfg, "fbt_min_vol_ratio", 1.2)) * vol_sma:
        return None, -1

    # Entry trigger: next bar must break below rejection_low - buffer
    buf = entry_buffer(R_low, cfg)
    trigger = R_low - buf
    lag = int(getattr(cfg, "fbt_lag_bars", 1))
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    lo_e = float(df_day.at[cand, "low"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(lo_e) and np.isfinite(cl_e) and lo_e < trigger
            and (not cfg.require_entry_close_confirm or cl_e < trigger)):
        return None, -1
    entry_idx = cand
    if (len(df_day) - 1 - entry_idx) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep = float(trigger)
    atr_entry = float(df_day.at[entry_idx, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx, "AVWAP"]) else np.nan
    )
    close_entry = float(df_day.at[entry_idx, "close"])
    avwap_dist_atr = (
        (avwap_entry - close_entry) / atr_entry
        if (np.isfinite(atr_entry) and atr_entry > 0 and np.isfinite(avwap_entry)) else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr > cap:
        return None, -1

    ema20 = float(R.get("EMA20", np.nan))
    ema_gap_atr = (ema20 - R_close) / R_atr if (np.isfinite(ema20) and R_atr > 0) else 0.0
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
    k_now   = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    qscore  = compute_quality_score_short(adx_now, avwap_dist_atr, ema_gap_atr, "FBT")
    atr_pct = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx, exit_time, exit_price, outcome, partial_taken = simulate_exit_short(
        df_day, entry_idx, ep, cfg
    )
    net_pnl, gross_pnl = compute_pnl_pct(ep, exit_price, "SHORT", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="SHORT",
        setup="H_FAILED_BREAKOUT_TRAP", impulse_type="TRAP",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx, "date"],
        entry_price=ep, sl_price=ep * (1.0 + cfg.stop_pct),
        target_price=ep * (1.0 - cfg.target_pct),
        exit_time_ist=exit_time, exit_price=exit_price, outcome=outcome,
        pnl_pct=net_pnl, pnl_pct_gross=gross_pnl,
        signal_price=R_close, partial_exit_taken=bool(partial_taken),
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr,
        ema20_gap_atr_signal=ema_gap_atr,
        atr_pct_signal=atr_pct,
        quality_score=qscore,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx)


# ===========================================================================
# SCAN ONE DAY (SHORT)
# ===========================================================================
def scan_one_day(
    ticker: str,
    df_day: pd.DataFrame,
    day_str: str,
    cfg: StrategyConfig,
    prev_close: Optional[float] = None,
) -> List[Trade]:
    if len(df_day) < int(cfg.min_bars_for_scan):
        return []

    day_vix = float(cfg.vix_daily.get(day_str, 0.0)) if cfg.vix_daily else 0.0
    if (
        cfg.max_vix_for_entries > 0
        and np.isfinite(day_vix)
        and day_vix > cfg.max_vix_for_entries
    ):
        return []

    # Apply VIX-dynamic scaling: replace cfg locally for this day only
    _vix_scale = get_vix_scale(cfg, day_str)
    if _vix_scale != 1.0:
        cfg = dataclasses.replace(
            cfg,
            stop_pct=cfg.stop_pct * _vix_scale if cfg.vix_scale_sl else cfg.stop_pct,
            target_pct=cfg.target_pct * _vix_scale if cfg.vix_scale_target else cfg.target_pct,
        )

    day_mode, gap_pct_open, opening_range_width_pct = select_day_mode(
        df_day=df_day,
        cfg=cfg,
        prev_close=prev_close,
    )
    if (
        float(getattr(cfg, "min_opening_range_width_pct", 0.0)) > 0
        and float(opening_range_width_pct) < float(cfg.min_opening_range_width_pct)
    ):
        return []

    trades: List[Trade] = []
    i = 2
    day_sl_count = 0
    day_r_total = 0.0
    cooldown_until_idx = -1

    # v17k — Compute opening range once per day for C_OR_BREAKDOWN.
    _v17k_or_high = float("nan")
    _v17k_or_low = float("nan")
    _v17k_or_width_pct = 0.0
    _v17k_or_end_idx = -1
    if getattr(cfg, "enable_setup_c_or_breakdown", False):
        _or_n = int(getattr(cfg, "or_breakdown_window_bars", 6))
        if len(df_day) > _or_n:
            _or_slice = df_day.iloc[:_or_n]
            _v17k_or_high = float(_or_slice["high"].max())
            _v17k_or_low = float(_or_slice["low"].min())
            _ref_close = float(df_day.iloc[0]["close"])
            if _ref_close > 0 and np.isfinite(_v17k_or_high) and np.isfinite(_v17k_or_low):
                _v17k_or_width_pct = (_v17k_or_high - _v17k_or_low) / _ref_close
            _v17k_or_end_idx = _or_n

    tail_guard = 1 if cfg.allow_incomplete_tail else 3
    while i < len(df_day) - tail_guard:
        if len(trades) >= cfg.max_trades_per_ticker_per_day:
            break

        if cfg.enable_risk_guardrails:
            if i <= cooldown_until_idx:
                i += 1
                continue
            if day_sl_count >= int(cfg.daily_loss_lock_sl_count):
                break
            if day_r_total <= float(cfg.daily_loss_lock_r_multiple):
                break

        c1 = df_day.iloc[i]
        ts1 = c1["date"]

        if not in_signal_window(ts1, cfg):
            i += 1
            continue

        # v17k — C_OR_BREAKDOWN
        if (
            getattr(cfg, "enable_setup_c_or_breakdown", False)
            and i >= _v17k_or_end_idx
            and np.isfinite(_v17k_or_high)
            and np.isfinite(_v17k_or_low)
        ):
            or_trade, or_exit_idx = _scan_or_breakdown_at(
                df_day, i, ticker, day_str, cfg, _v17k_or_high, _v17k_or_low, _v17k_or_width_pct
            )
            if or_trade is not None:
                trades.append(or_trade)
                risk_pct_or = max(cfg.stop_pct * 100.0, 1e-9)
                day_r_total += float(or_trade.pnl_pct) / risk_pct_or
                if str(or_trade.outcome).upper() == "SL":
                    day_sl_count += 1
                    cooldown_until_idx = max(
                        cooldown_until_idx, int(or_exit_idx) + int(cfg.sl_cooldown_bars)
                    )
                i = int(or_exit_idx) + 1
                continue

        # v17k — D_EMA20_REJECTION
        if getattr(cfg, "enable_setup_d_ema20_rejection", False):
            ema_trade, ema_exit_idx = _scan_ema20_rejection_at(
                df_day, i, ticker, day_str, cfg
            )
            if ema_trade is not None:
                trades.append(ema_trade)
                risk_pct_ema = max(cfg.stop_pct * 100.0, 1e-9)
                day_r_total += float(ema_trade.pnl_pct) / risk_pct_ema
                if str(ema_trade.outcome).upper() == "SL":
                    day_sl_count += 1
                    cooldown_until_idx = max(
                        cooldown_until_idx, int(ema_exit_idx) + int(cfg.sl_cooldown_bars)
                    )
                i = int(ema_exit_idx) + 1
                continue

        # v17k — E_VWAP_BAND_FADE: Upper BB touch + bearish reject
        if getattr(cfg, "enable_setup_e_vwap_band_fade", False):
            v_trade, v_exit_idx = _scan_vwap_band_fade_short_at(
                df_day, i, ticker, day_str, cfg
            )
            if v_trade is not None:
                trades.append(v_trade)
                risk_pct_v = max(cfg.stop_pct * 100.0, 1e-9)
                day_r_total += float(v_trade.pnl_pct) / risk_pct_v
                if str(v_trade.outcome).upper() == "SL":
                    day_sl_count += 1
                    cooldown_until_idx = max(
                        cooldown_until_idx, int(v_exit_idx) + int(cfg.sl_cooldown_bars)
                    )
                i = int(v_exit_idx) + 1
                continue

        # v17k — G_LOWER_LOW_BREAK: break N-bar swing low.
        if getattr(cfg, "enable_setup_g_lower_low_break", False):
            g_trade, g_exit_idx = _scan_lower_low_break_at(
                df_day, i, ticker, day_str, cfg
            )
            if g_trade is not None:
                trades.append(g_trade)
                risk_pct_g = max(cfg.stop_pct * 100.0, 1e-9)
                day_r_total += float(g_trade.pnl_pct) / risk_pct_g
                if str(g_trade.outcome).upper() == "SL":
                    day_sl_count += 1
                    cooldown_until_idx = max(
                        cooldown_until_idx, int(g_exit_idx) + int(cfg.sl_cooldown_bars)
                    )
                i = int(g_exit_idx) + 1
                continue

        # Phase 2b — H_FAILED_BREAKOUT_TRAP: trapped breakout buyers.
        if getattr(cfg, "enable_setup_failed_breakout_trap", False):
            fbt_trade, fbt_exit_idx = _scan_failed_breakout_trap_at(
                df_day, i, ticker, day_str, cfg
            )
            if fbt_trade is not None:
                trades.append(fbt_trade)
                risk_pct_fbt = max(cfg.stop_pct * 100.0, 1e-9)
                day_r_total += float(fbt_trade.pnl_pct) / risk_pct_fbt
                if str(fbt_trade.outcome).upper() == "SL":
                    day_sl_count += 1
                    cooldown_until_idx = max(
                        cooldown_until_idx, int(fbt_exit_idx) + int(cfg.sl_cooldown_bars)
                    )
                i = int(fbt_exit_idx) + 1
                continue

        # v17j — D_AVWAP_LOSE_REVERSAL: independent of impulse, runs first.
        # If a reversal entry fires, advance past the exit and skip the
        # impulse path for this iteration.
        if getattr(cfg, "enable_setup_d_avwap_lose_reversal", False):
            rev_trade, rev_exit_idx = _scan_reversal_at_short(
                df_day, i, ticker, day_str, cfg
            )
            if rev_trade is not None:
                trades.append(rev_trade)
                risk_pct_rev = max(cfg.stop_pct * 100.0, 1e-9)
                day_r_total += float(rev_trade.pnl_pct) / risk_pct_rev
                if str(rev_trade.outcome).upper() == "SL":
                    day_sl_count += 1
                    cooldown_until_idx = max(
                        cooldown_until_idx,
                        int(rev_exit_idx) + int(cfg.sl_cooldown_bars),
                    )
                i = int(rev_exit_idx) + 1
                continue

        impulse = classify_red_impulse(c1, cfg)
        if impulse == "":
            i += 1
            continue

        if not market_regime_pass(ts1, "SHORT", cfg):
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

        # Volume confirmation: impulse bar should have above-average volume
        if not volume_filter_pass(c1, cfg):
            i += 1
            continue

        if day_mode == "trend":
            if not _trend_filter_short(df_day, i, c1, cfg):
                i += 1
                continue
        else:
            if not _reversal_filter_short(df_day, i, c1, cfg):
                i += 1
                continue

        has_sweep_ctx = False
        require_sweep = bool(
            cfg.enable_liquidity_sweep_filter
            or (day_mode == "reversal" and cfg.reversal_requires_sweep)
        )
        if require_sweep:
            sweep_cfg = cfg
            if not cfg.enable_liquidity_sweep_filter:
                sweep_cfg = dataclasses.replace(cfg, enable_liquidity_sweep_filter=True)
            has_sweep_ctx = has_recent_liquidity_sweep(df_day, i, "SHORT", sweep_cfg)
            if not has_sweep_ctx:
                i += 1
                continue
        if avwap_no_trade_zone_block(c1, impulse, has_sweep_ctx, cfg):
            i += 1
            continue

        # Diagnostic values at signal
        adx1 = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
        rsi1 = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
        k1 = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
        avwap1 = float(c1["AVWAP"]) if np.isfinite(c1.get("AVWAP", np.nan)) else 0.0
        ema20 = float(c1["EMA20"]) if np.isfinite(c1.get("EMA20", np.nan)) else 0.0
        atr_pct = atr1 / close1
        avwap_dist_atr = (avwap1 - close1) / atr1
        ema_gap_atr = (ema20 - close1) / atr1
        quality = compute_quality_score_short(adx1, avwap_dist_atr, ema_gap_atr, impulse)
        if (
            float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0)) > 0
            and np.isfinite(avwap_dist_atr)
            and avwap_dist_atr > float(cfg.signal_avwap_dist_atr_max)
        ):
            i += 1
            continue

        low1 = float(c1["low"])

        def _bars_left_ok(eidx: int) -> bool:
            return (len(df_day) - 1 - eidx) >= cfg.min_bars_left_after_entry

        def _entry_time_ok(ts: pd.Timestamp) -> bool:
            cutoff = getattr(cfg, "entry_time_cutoff", None)
            if cutoff is None:
                return True
            ts_pd = pd.Timestamp(ts)
            if ts_pd.tzinfo is None:
                ts_pd = ts_pd.tz_localize(IST)
            else:
                ts_pd = ts_pd.tz_convert(IST)
            return ts_pd.timetz().replace(tzinfo=None) < cutoff

        def _close_confirm_ok(eidx: int, trigger: float) -> bool:
            if not cfg.require_entry_close_confirm:
                return True
            cl = float(df_day.at[eidx, "close"])
            return np.isfinite(cl) and cl < trigger

        def _make_trade(entry_idx: int, entry_price: float, setup: str) -> Trade:
            exit_idx, exit_time, exit_price, outcome, partial_exit_taken = simulate_exit_short(
                df_day, entry_idx, entry_price, cfg
            )
            net_pnl, gross_pnl = compute_pnl_pct(entry_price, exit_price, "SHORT", cfg)
            sl_price = entry_price * (1.0 + cfg.stop_pct)
            target_price = entry_price * (1.0 - cfg.target_pct)
            position_size_rs, risk_rs = compute_risk_position_size_rs(
                entry_price=entry_price,
                stop_price=sl_price,
                cfg=cfg,
            )
            return Trade(
                trade_date=day_str,
                ticker=ticker,
                side="SHORT",
                setup=setup,
                impulse_type=impulse,
                signal_time_ist=ts1,
                entry_time_ist=df_day.at[entry_idx, "date"],
                entry_price=entry_price,
                sl_price=sl_price,
                target_price=target_price,
                exit_time_ist=exit_time,
                exit_price=exit_price,
                outcome=outcome,
                pnl_pct=net_pnl,
                pnl_pct_gross=gross_pnl,
                signal_price=close1,
                position_size_rs=position_size_rs,
                risk_per_trade_rs=risk_rs,
                day_mode=day_mode,
                gap_pct_open=gap_pct_open,
                opening_range_width_pct=opening_range_width_pct,
                partial_exit_taken=bool(partial_exit_taken),
                adx_signal=adx1,
                rsi_signal=rsi1,
                stochk_signal=k1,
                avwap_dist_atr_signal=avwap_dist_atr,
                ema20_gap_atr_signal=ema_gap_atr,
                atr_pct_signal=atr_pct,
                quality_score=quality,
                india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
            ), exit_idx

        def _register_trade_result(trade: Trade, exit_idx: int) -> None:
            nonlocal day_sl_count, day_r_total, cooldown_until_idx
            trades.append(trade)
            risk_pct = max(cfg.stop_pct * 100.0, 1e-9)
            day_r_total += float(trade.pnl_pct) / risk_pct
            if str(trade.outcome).upper() == "SL":
                day_sl_count += 1
                cooldown_until_idx = max(
                    cooldown_until_idx, int(exit_idx) + int(cfg.sl_cooldown_bars)
                )

        # ---------------------------------------------------------------
        # SETUP A (MODERATE): break C1 low, or pullback + break C2 low
        # ---------------------------------------------------------------
        if impulse == "MODERATE":
            c2 = df_day.iloc[i + 1]
            buf1 = entry_buffer(low1, cfg)
            trigger1 = low1 - buf1

            # Option 1: break C1 low on configured lag bar
            lag1 = int(cfg.lag_bars_short_a_mod_break_c1_low)
            entry_idx_1 = i + lag1 if lag1 >= 0 else (i + 1)
            if entry_idx_1 < len(df_day) and float(df_day.at[entry_idx_1, "low"]) < trigger1:
                entry_idx = entry_idx_1
                entry_ts = df_day.at[entry_idx, "date"]

                if (
                    in_signal_window(entry_ts, cfg)
                    and _entry_time_ok(entry_ts)
                    and _bars_left_ok(entry_idx)
                    and _close_confirm_ok(entry_idx, trigger1)
                    and avwap_rejection_pass(df_day, i, entry_idx, cfg)
                    and avwap_distance_pass(df_day, entry_idx, cfg)
                ):
                    trade, exit_idx = _make_trade(entry_idx, trigger1, "A_MOD_BREAK_C1_LOW")
                    _register_trade_result(trade, exit_idx)
                    i = exit_idx + 1
                    continue

            # Option 2: small green pullback C2, then break C2 low on C3
            c2o, c2c = float(c2["open"]), float(c2["close"])
            c2_body = abs(c2c - c2o)
            c2_atr = float(c2.get("ATR15", atr1)) if np.isfinite(c2.get("ATR15", atr1)) else atr1
            c2_avwap = float(c2.get("AVWAP", np.nan)) if np.isfinite(c2.get("AVWAP", np.nan)) else np.nan

            c2_small_green = (c2c > c2o) and np.isfinite(c2_atr) and (c2_body <= cfg.small_counter_max_atr * c2_atr)
            c2_below_avwap = np.isfinite(c2_avwap) and (c2c < c2_avwap)

            if c2_small_green and c2_below_avwap and (i + 2 < len(df_day)):
                low2 = float(c2["low"])
                buf2 = entry_buffer(low2, cfg)
                trigger2 = low2 - buf2

                lag2 = int(cfg.lag_bars_short_a_pullback_c2_break_c2_low)
                entry_idx_2 = i + lag2 if lag2 >= 0 else (i + 2)
                if entry_idx_2 < len(df_day) and float(df_day.at[entry_idx_2, "low"]) < trigger2:
                    entry_idx = entry_idx_2
                    entry_ts = df_day.at[entry_idx, "date"]

                    if (
                        in_signal_window(entry_ts, cfg)
                        and _entry_time_ok(entry_ts)
                        and _bars_left_ok(entry_idx)
                        and _close_confirm_ok(entry_idx, trigger2)
                        and avwap_rejection_pass(df_day, i, entry_idx, cfg)
                        and avwap_distance_pass(df_day, entry_idx, cfg)
                    ):
                        trade, exit_idx = _make_trade(
                            entry_idx, trigger2, "A_PULLBACK_C2_THEN_BREAK_C2_LOW"
                        )
                        _register_trade_result(trade, exit_idx)
                        i = exit_idx + 1
                        continue

            i += 1
            continue

        # ---------------------------------------------------------------
        # SETUP B (HUGE): bounce fails, break bounce low
        # ---------------------------------------------------------------
        if impulse == "HUGE" and cfg.enable_setup_b_huge_failed_bounce:
            bounce_end = min(i + 3, len(df_day) - 1)
            bounce = df_day.iloc[i + 1 : bounce_end + 1].copy()
            if bounce.empty:
                i += 1
                continue

            # Require at least one small green bounce candle
            closes = pd.to_numeric(bounce["close"], errors="coerce")
            opens = pd.to_numeric(bounce["open"], errors="coerce")
            bounce_atr = pd.to_numeric(bounce.get("ATR15", atr1), errors="coerce").fillna(atr1)
            bounce_body = (closes - opens).abs()
            bounce_green = closes > opens
            bounce_small = bounce_body <= (cfg.small_counter_max_atr * bounce_atr)

            if not bool((bounce_green & bounce_small).any()):
                i += 1
                continue

            # AVWAP touch-fail evidence in bounce window
            if cfg.require_avwap_rule and cfg.avwap_touch:
                avwaps = pd.to_numeric(bounce["AVWAP"], errors="coerce")
                highs = pd.to_numeric(bounce["high"], errors="coerce")
                touch_fail = bool(((highs >= avwaps) & (closes < avwaps)).fillna(False).any())
                if not touch_fail:
                    i += 1
                    continue

            bounce_low = float(pd.to_numeric(bounce["low"], errors="coerce").min())
            if not np.isfinite(bounce_low):
                i += 1
                continue

            buf = entry_buffer(bounce_low, cfg)
            trigger_b = bounce_low - buf
            entered = False

            lag_huge = int(cfg.lag_bars_short_b_huge_failed_bounce)
            if lag_huge >= 0:
                j_fixed = i + lag_huge
                j_iter = [j_fixed] if (bounce_end + 1 <= j_fixed < len(df_day)) else []
            else:
                j_iter = range(bounce_end + 1, len(df_day))

            for j in j_iter:
                tsj = df_day.at[j, "date"]
                if not in_signal_window(tsj, cfg):
                    continue
                if not _entry_time_ok(tsj):
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
                    _register_trade_result(trade, exit_idx)
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
# SCAN ALL DAYS FOR ONE TICKER
# ===========================================================================
def scan_all_days_for_ticker(
    ticker: str, df_full: pd.DataFrame, cfg: StrategyConfig
) -> List[Trade]:
    df = prepare_session_bars_for_scan(df_full, cfg)
    return scan_all_days_for_ticker_prepared(ticker, df, cfg)


def scan_all_days_for_ticker_prepared(
    ticker: str, df: pd.DataFrame, cfg: StrategyConfig
) -> List[Trade]:
    if df.empty:
        return []

    all_trades: List[Trade] = []
    prev_close: Optional[float] = None
    for day_val, df_day in df.groupby("day", sort=True):
        df_day = df_day.copy().reset_index(drop=True)
        if len(df_day) < int(cfg.min_bars_for_scan):
            if len(df_day):
                prev_close = float(pd.to_numeric(df_day["close"], errors="coerce").iloc[-1])
            continue
        prev_close_for_day = (
            prev_close if bool(getattr(cfg, "use_prev_close_for_day_mode", True)) else None
        )
        trades = scan_one_day(ticker, df_day, str(day_val), cfg, prev_close=prev_close_for_day)
        if trades:
            all_trades.extend(trades)
        prev_close = float(pd.to_numeric(df_day["close"], errors="coerce").iloc[-1])

    return all_trades


# ===========================================================================
# RUN ALL TICKERS (called by combined runner)
# ===========================================================================
def run_short_scan(cfg: Optional[StrategyConfig] = None) -> List[Trade]:
    if cfg is None:
        cfg = default_short_config()

    tickers = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    print(f"[SHORT] Tickers found: {len(tickers)}")

    all_trades: List[Trade] = []
    for k, t in enumerate(tickers, start=1):
        path = os.path.join(cfg.dir_15m, f"{t}{cfg.end_15m}")
        df = read_15m_parquet(path, cfg.parquet_engine)
        if df.empty:
            continue

        trades = scan_all_days_for_ticker(t, df, cfg)
        if trades:
            all_trades.extend(trades)

        if k % 50 == 0:
            print(f"  [SHORT] scanned {k}/{len(tickers)} | trades_so_far={len(all_trades)}")

    return all_trades


