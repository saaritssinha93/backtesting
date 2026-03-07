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

    trades: List[Trade] = []
    i = 2
    day_sl_count = 0
    day_r_total = 0.0
    cooldown_until_idx = -1

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

        low1 = float(c1["low"])

        def _bars_left_ok(eidx: int) -> bool:
            return (len(df_day) - 1 - eidx) >= cfg.min_bars_left_after_entry

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

    all_trades: List[Trade] = []
    prev_close: Optional[float] = None
    for day_val, df_day in df.groupby("day", sort=True):
        df_day = df_day.copy().reset_index(drop=True)
        if len(df_day) < int(cfg.min_bars_for_scan):
            if len(df_day):
                prev_close = float(pd.to_numeric(df_day["close"], errors="coerce").iloc[-1])
            continue
        df_day["AVWAP"] = compute_day_avwap(df_day)
        trades = scan_one_day(ticker, df_day, str(day_val), cfg, prev_close=prev_close)
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


