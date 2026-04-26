# -*- coding: utf-8 -*-
"""
avwap_long_strategy.py — LONG side logic for AVWAP v11
======================================================

Only contains direction-specific code:
- Green impulse classification
- AVWAP support checks (price above AVWAP)
- Long exit simulation
- Long-specific signal validation
- scan_one_day / scan_all_days entry points

All shared infrastructure comes from avwap_common.
"""

from __future__ import annotations

import dataclasses
import os
from typing import List, Optional

import numpy as np
import pandas as pd

from avwap_v11_refactored.avwap_common_v7_sweep import (
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
    get_vix_scale,
    has_recent_liquidity_sweep,
    avwap_no_trade_zone_block,
    market_regime_pass,
)


def _prepare_session_bars_for_scan(
    df_full: pd.DataFrame,
    cfg: StrategyConfig,
) -> pd.DataFrame:
    if df_full.empty:
        return pd.DataFrame()

    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(set(df_full.columns)):
        return pd.DataFrame()

    dt = pd.to_datetime(df_full["date"], errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt = dt.dt.tz_convert(IST)

    local_time = dt.dt.time
    mask = (local_time >= cfg.session_start) & (local_time <= cfg.session_end)
    if not bool(mask.any()):
        return pd.DataFrame(columns=df_full.columns)

    df = df_full.loc[mask].copy()
    if df.empty:
        return df

    df["date"] = dt.loc[mask]
    df = df.sort_values("date").reset_index(drop=True)
    df = prepare_indicators(df, cfg)
    if df.empty:
        return df

    per_day: List[pd.DataFrame] = []
    for _, g in df.groupby("day", sort=True):
        if g.empty:
            continue
        g2 = g.copy().reset_index(drop=True)
        g2["AVWAP"] = compute_day_avwap(g2)
        per_day.append(g2)

    if not per_day:
        return df.iloc[0:0].copy()
    return pd.concat(per_day, ignore_index=True)


# ===========================================================================
# IMPULSE CLASSIFICATION (GREEN CANDLES)
# ===========================================================================
def classify_green_impulse(row: pd.Series, cfg: StrategyConfig) -> str:
    o = float(row["open"])
    c = float(row["close"])
    h = float(row["high"])
    low = float(row["low"])
    atr = float(row["ATR15"])

    if not np.isfinite(atr) or atr <= 0:
        return ""
    if c <= o:  # must be green
        return ""

    body = abs(c - o)
    rng = h - low
    if not np.isfinite(rng) or rng <= 0:
        return ""

    close_near_high = ((h - c) / rng) <= cfg.close_near_extreme_max

    if (body >= cfg.huge_impulse_min_atr * atr) or (
        rng >= cfg.huge_impulse_min_range_atr * atr
    ):
        return "HUGE"

    if (
        body >= cfg.mod_impulse_min_atr * atr
        and body <= cfg.mod_impulse_max_atr * atr
        and close_near_high
    ):
        return "MODERATE"

    return ""


# ===========================================================================
# AVWAP SUPPORT (LONG: price must be ABOVE AVWAP, with support evidence)
# ===========================================================================
def avwap_support_pass(
    df_day: pd.DataFrame,
    impulse_idx: int,
    entry_idx: int,
    atr_entry: float,
    cfg: StrategyConfig,
) -> tuple:
    """
    Returns (ok: bool, avwap_dist_atr: float).
    Evidence: touch-support (low <= AVWAP and close > AVWAP) + consecutive closes above.
    """
    if not cfg.require_avwap_rule:
        return True, 0.0
    if entry_idx <= impulse_idx:
        return False, 0.0

    seg = df_day.iloc[impulse_idx + 1 : entry_idx + 1].copy()
    if seg.empty:
        return False, 0.0

    av = pd.to_numeric(seg["AVWAP"], errors="coerce")
    lo = pd.to_numeric(seg["low"], errors="coerce")
    cl = pd.to_numeric(seg["close"], errors="coerce")

    # Touch support: low <= AVWAP and close > AVWAP
    touch_ok = False
    if cfg.avwap_touch:
        touch_ok = bool(((lo <= av) & (cl > av)).fillna(False).any())

    # Consecutive closes above AVWAP
    consec_ok = False
    n = cfg.avwap_min_consec_closes
    if n > 0:
        consec_ok = _count_max_consec_above(cl, av) >= n

    mode = cfg.avwap_mode.strip().lower()
    if mode == "both":
        evidence_ok = touch_ok and consec_ok
    else:
        evidence_ok = touch_ok or consec_ok

    # Distance from AVWAP at entry
    entry_close = float(df_day.at[entry_idx, "close"])
    entry_avwap = float(df_day.at[entry_idx, "AVWAP"])
    if not (
        np.isfinite(entry_close)
        and np.isfinite(entry_avwap)
        and np.isfinite(atr_entry)
        and atr_entry > 0
    ):
        return False, 0.0

    avwap_dist = entry_close - entry_avwap
    avwap_dist_atr = avwap_dist / atr_entry
    dist_ok = avwap_dist >= (cfg.avwap_dist_atr_mult * atr_entry)

    return bool(evidence_ok and dist_ok), float(avwap_dist_atr)


def _count_max_consec_above(close_s: pd.Series, avwap_s: pd.Series) -> int:
    cond = (close_s > avwap_s).fillna(False).astype(bool).tolist()
    best = 0
    cur = 0
    for v in cond:
        if v:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


# ===========================================================================
# EXIT SIMULATION (LONG)
# ===========================================================================
def simulate_exit_long(
    df_day: pd.DataFrame, entry_idx: int, entry_price: float, cfg: StrategyConfig
) -> tuple:
    """Walk forward within day until TARGET / SL / BE / TRAIL / EOD.

    Exit priority: SL wins ties over TARGET (conservative).
    Trailing stop: after BE trigger, trail from best (highest) price seen.
    """
    sl = entry_price * (1.0 - cfg.stop_pct)
    tgt = entry_price * (1.0 + cfg.target_pct)

    sl_curr = float(sl)
    be_armed = False
    be_level = entry_price * (1.0 + cfg.be_pad_pct)
    best_price = entry_price  # best (highest) price seen for long

    for k in range(entry_idx + 1, len(df_day)):
        hi = float(df_day.at[k, "high"])
        lo = float(df_day.at[k, "low"])
        ts = df_day.at[k, "date"]

        # Track best favorable price (highest for LONG)
        if np.isfinite(hi) and hi > best_price:
            best_price = hi

        if (
            cfg.enable_breakeven
            and not be_armed
            and np.isfinite(hi)
            and hi >= entry_price * (1.0 + cfg.be_trigger_pct)
        ):
            be_armed = True
            sl_curr = max(sl_curr, be_level)

        # Trailing stop: after BE armed, trail from best price
        if be_armed and cfg.enable_trailing_stop:
            trail_sl = best_price * (1.0 - cfg.trail_pct)
            sl_curr = max(sl_curr, trail_sl)  # trailing stop can only move up (tighter)

        hit_sl = np.isfinite(lo) and lo <= sl_curr
        hit_tg = np.isfinite(hi) and hi >= tgt

        if hit_sl and hit_tg:
            if be_armed:
                return k, ts, float(sl_curr), "BE"
            return k, ts, float(sl_curr), "SL"
        if hit_sl:
            if be_armed:
                return k, ts, float(sl_curr), "BE"
            return k, ts, float(sl_curr), "SL"
        if hit_tg:
            return k, ts, float(tgt), "TARGET"

    last = len(df_day) - 1
    return last, df_day.at[last, "date"], float(df_day.at[last, "close"]), "EOD"


# ===========================================================================
# TREND FILTER VALIDATION (LONG)
# ===========================================================================
def _trend_filter_long(
    df_day: pd.DataFrame, i: int, c1: pd.Series, cfg: StrategyConfig
) -> bool:
    """Returns True if all Option-A trend conditions pass for LONG."""
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
        np.isfinite(rsi1)
        and rsi1 >= cfg.rsi_min_long
        and twice_increasing(df_day, i, "RSI15")
    )
    stoch_ok = (
        np.isfinite(k1)
        and np.isfinite(d1)
        and k1 >= cfg.stochk_min
        and k1 <= cfg.stochk_max
        and k1 > d1
        and twice_increasing(df_day, i, "STOCHK15")
    )

    if not (adx_ok and rsi_ok and stoch_ok):
        return False

    # Strict EMA + AVWAP for LONG
    close1 = float(c1["close"])
    ema20 = float(c1["EMA20"]) if np.isfinite(c1.get("EMA20", np.nan)) else np.nan
    ema50 = float(c1["EMA50"]) if np.isfinite(c1.get("EMA50", np.nan)) else np.nan
    avwap1 = float(c1["AVWAP"]) if np.isfinite(c1.get("AVWAP", np.nan)) else np.nan

    if not (np.isfinite(ema20) and np.isfinite(ema50) and np.isfinite(avwap1)):
        return False

    return (ema20 > ema50) and (close1 > ema20) and (close1 > avwap1)


# ===========================================================================
# v17g — B_AVWAP_RECLAIM_REVERSAL (regime-flexible reversal entry)
# ===========================================================================
def _scan_reversal_at(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
) -> tuple:
    """Try the AVWAP-reclaim reversal setup at bar index ``i``.

    Returns (trade, exit_idx) on success, (None, -1) on miss. Independent of
    impulse classification: triggers when prior 1-2 bars dipped below AVWAP
    and the current bar reclaims with bullish body, rising RSI, K>D, ADX>=22,
    volume >= reversal_volume_min_ratio * VOL_SMA20, and the bar is before
    reversal_max_hour_ist (IST). Entry is on the configured lag bar via a
    breakout above the reclaim bar's high.
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
    if R_hour >= int(cfg.reversal_max_hour_ist):
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

    # Reclaim: close above AVWAP after at least one prior bar dipped below.
    if R_close <= R_avwap:
        return None, -1

    require_both = bool(getattr(cfg, "reversal_require_both_prior_bars", True))
    prior_bars_ok = True if require_both else False
    seen_any = False
    for k in (i - 1, i - 2):
        if k < 0:
            prior_bars_ok = False if require_both else prior_bars_ok
            continue
        try:
            prev_low = float(df_day.at[k, "low"])
            prev_avwap = float(df_day.at[k, "AVWAP"])
        except (KeyError, ValueError, TypeError):
            if require_both:
                prior_bars_ok = False
            continue
        below = (
            np.isfinite(prev_low)
            and np.isfinite(prev_avwap)
            and prev_low <= prev_avwap
        )
        if require_both:
            if not below:
                prior_bars_ok = False
                break
            seen_any = True
        else:
            if below:
                prior_bars_ok = True
                break
    if require_both and not seen_any:
        prior_bars_ok = False
    if not prior_bars_ok:
        return None, -1

    R_rng = R_high - R_low
    R_body = abs(R_close - R_open)
    body_min = float(getattr(cfg, "reversal_body_atr_min", 0.50))
    if (R_body / R_atr) < body_min:
        return None, -1
    upper_pct = float(getattr(cfg, "reversal_close_upper_pct", 0.40))
    # close must be in the upper `upper_pct` of the bar range
    if R_close < (R_low + (1.0 - upper_pct) * R_rng):
        return None, -1

    # Momentum
    rsi_min = float(getattr(cfg, "reversal_rsi_min", 50.0))
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_prev = float(df_day.at[i - 1, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    if not (
        np.isfinite(rsi_now)
        and np.isfinite(rsi_prev)
        and rsi_now >= rsi_min
        and rsi_now > rsi_prev
    ):
        return None, -1

    adx_min = float(getattr(cfg, "reversal_adx_min", 28.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1

    # Trend context: close above EMA20 (NEW)
    if bool(getattr(cfg, "reversal_require_close_gt_ema20", True)):
        ema20_now = float(df_day.at[i, "EMA20"]) if "EMA20" in df_day.columns else np.nan
        if not (np.isfinite(ema20_now) and R_close > ema20_now):
            return None, -1

    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else np.nan
    d_now = float(df_day.at[i, "STOCHD15"]) if "STOCHD15" in df_day.columns else np.nan
    if not (np.isfinite(k_now) and np.isfinite(d_now) and k_now > d_now):
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
    if vol_sma > 0 and vol_now < float(cfg.reversal_volume_min_ratio) * vol_sma:
        return None, -1
    vol_cap_ratio = float(getattr(cfg, "reversal_volume_max_ratio", 0.0) or 0.0)
    if vol_cap_ratio > 0.0 and vol_sma > 0 and vol_now > vol_cap_ratio * vol_sma:
        return None, -1

    # Determine entry bar from lag (or dynamic lookahead when lag<0).
    buf_rev = entry_buffer(R_high, cfg)
    trigger_rev = R_high + buf_rev
    lag_rev = int(cfg.lag_bars_long_b_avwap_reclaim_reversal)
    rev_entry_idx = -1

    if lag_rev >= 0:
        cand = i + lag_rev
        if (
            cand < len(df_day)
            and in_signal_window(df_day.at[cand, "date"], cfg)
        ):
            hi_e = float(df_day.at[cand, "high"])
            cl_e = float(df_day.at[cand, "close"])
            if (
                np.isfinite(hi_e)
                and np.isfinite(cl_e)
                and hi_e > trigger_rev
                and (not cfg.require_entry_close_confirm or cl_e > trigger_rev)
            ):
                rev_entry_idx = cand
    else:
        for jj in range(i + 1, min(len(df_day), i + 4)):
            if not in_signal_window(df_day.at[jj, "date"], cfg):
                continue
            hi_jj = float(df_day.at[jj, "high"])
            cl_jj = float(df_day.at[jj, "close"])
            if (
                np.isfinite(hi_jj)
                and np.isfinite(cl_jj)
                and hi_jj > trigger_rev
                and (not cfg.require_entry_close_confirm or cl_jj > trigger_rev)
            ):
                rev_entry_idx = jj
                break

    if rev_entry_idx < 0:
        return None, -1
    if (len(df_day) - 1 - rev_entry_idx) < int(cfg.min_bars_left_after_entry):
        return None, -1

    # Entry price selection (mirror of _make_trade in scan_one_day)
    if cfg.entry_at_next_open:
        nxt = rev_entry_idx + 1
        if nxt >= len(df_day):
            return None, -1
        ep_rev = float(df_day.at[nxt, "open"])
    elif cfg.entry_at_bar_close:
        ep_rev = float(df_day.at[rev_entry_idx, "close"])
    else:
        ep_rev = float(trigger_rev)

    if cfg.max_entry_slip_pct > 0.0 and ep_rev > trigger_rev * (
        1.0 + cfg.max_entry_slip_pct
    ):
        return None, -1

    # AVWAP distance at entry — reversal does not require support evidence
    # (the reclaim itself is the evidence) but must respect Change-5 cap.
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
        avwap_dist_atr_rev = (close_entry - avwap_entry) / atr_entry_rev
    else:
        avwap_dist_atr_rev = 0.0

    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr_rev > cap:
        return None, -1
    dist_min = float(getattr(cfg, "reversal_avwap_dist_atr_min", 0.0) or 0.0)
    if dist_min > 0.0 and avwap_dist_atr_rev < dist_min:
        return None, -1

    # Diagnostics for Trade row
    ema20_R = float(R.get("EMA20", np.nan))
    ema_gap_atr_rev = (
        (R_close - ema20_R) / R_atr
        if (np.isfinite(ema20_R) and R_atr > 0)
        else 0.0
    )
    adx_slope2_rev = (
        float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"])
        if i >= 2 and "ADX15" in df_day.columns
        else 0.0
    )
    qscore_rev = compute_quality_score_long(
        adx_now, adx_slope2_rev, avwap_dist_atr_rev, ema_gap_atr_rev, "REVERSAL"
    )
    atr_pct_rev = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx_rev, exit_time_rev, exit_price_rev, outcome_rev = simulate_exit_long(
        df_day, rev_entry_idx, ep_rev, cfg
    )
    net_pnl_rev, gross_pnl_rev = compute_pnl_pct(
        ep_rev, exit_price_rev, "LONG", cfg
    )

    trade = Trade(
        trade_date=day_str,
        ticker=ticker,
        side="LONG",
        setup="B_AVWAP_RECLAIM_REVERSAL",
        impulse_type="REVERSAL",
        signal_time_ist=R_ts,
        entry_time_ist=df_day.at[rev_entry_idx, "date"],
        entry_price=ep_rev,
        sl_price=ep_rev * (1.0 - cfg.stop_pct),
        target_price=ep_rev * (1.0 + cfg.target_pct),
        exit_time_ist=exit_time_rev,
        exit_price=exit_price_rev,
        outcome=outcome_rev,
        pnl_pct=net_pnl_rev,
        pnl_pct_gross=gross_pnl_rev,
        signal_price=R_close,
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
# C_OR_BREAKOUT (v17k) — Opening-range breakout LONG.
# After the first or_breakout_window_bars complete, signal fires on the first
# bar whose high breaks the OR high with adequate volume + ADX + width gate.
# Entry on lag=1 via standard breakout trigger.
# ===========================================================================
def _scan_or_breakout_at(
    df_day: pd.DataFrame,
    i: int,
    ticker: str,
    day_str: str,
    cfg: StrategyConfig,
    or_high: float,
    or_low: float,
    or_width_pct: float,
) -> tuple:
    """Try the OR-breakout LONG setup at bar index ``i`` (post-OR bar)."""
    if i < 2 or (i + 2) >= len(df_day):
        return None, -1
    R = df_day.iloc[i]

    # Time gate
    R_ts = R["date"]
    try:
        R_hour = R_ts.tz_convert(IST).hour
    except Exception:
        R_hour = getattr(R_ts, "hour", 0)
    if R_hour >= int(getattr(cfg, "or_breakout_max_hour_ist", 11)):
        return None, -1

    R_close = float(R["close"])
    R_open = float(R["open"])
    R_high = float(R["high"])
    R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))

    if not (np.isfinite(R_atr) and R_atr > 0 and (R_high - R_low) > 0 and R_close > 0):
        return None, -1

    # Width gate
    if not (
        float(getattr(cfg, "or_breakout_min_width_pct", 0.0)) <= or_width_pct
        <= float(getattr(cfg, "or_breakout_max_width_pct", 99.0))
    ):
        return None, -1

    # Breakout: bar high breaks OR high (close confirms above OR high)
    if not (R_high > or_high and R_close > or_high):
        return None, -1

    # ADX, volume gates
    adx_min = float(getattr(cfg, "or_breakout_adx_min", 22.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1

    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    if vol_sma > 0 and vol_now < float(cfg.or_breakout_volume_min_ratio) * vol_sma:
        return None, -1

    # Entry on next bar via standard breakout
    buf_or = entry_buffer(R_high, cfg)
    trigger_or = R_high + buf_or
    lag_or = int(cfg.or_breakout_lag_bars)
    entry_idx_or = -1
    cand = i + max(lag_or, 1)
    if cand < len(df_day) and in_signal_window(df_day.at[cand, "date"], cfg):
        hi_e = float(df_day.at[cand, "high"])
        cl_e = float(df_day.at[cand, "close"])
        if (
            np.isfinite(hi_e)
            and np.isfinite(cl_e)
            and hi_e > trigger_or
            and (not cfg.require_entry_close_confirm or cl_e > trigger_or)
        ):
            entry_idx_or = cand
    if entry_idx_or < 0:
        return None, -1
    if (len(df_day) - 1 - entry_idx_or) < int(cfg.min_bars_left_after_entry):
        return None, -1

    # Entry price: trigger
    ep_or = float(trigger_or)

    # Diagnostics
    atr_entry_or = float(df_day.at[entry_idx_or, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx_or, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx_or, "AVWAP"])
        else np.nan
    )
    close_entry = float(df_day.at[entry_idx_or, "close"])
    avwap_dist_atr_or = (
        (close_entry - avwap_entry) / atr_entry_or
        if (np.isfinite(atr_entry_or) and atr_entry_or > 0 and np.isfinite(avwap_entry))
        else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr_or > cap:
        return None, -1

    ema20_R = float(R.get("EMA20", np.nan))
    ema_gap_atr_or = (ema20_R - R_close) / R_atr if (np.isfinite(ema20_R) and R_atr > 0) else 0.0
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    adx_slope2 = (
        float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"])
        if i >= 2 and "ADX15" in df_day.columns
        else 0.0
    )
    qscore_or = compute_quality_score_long(
        adx_now, adx_slope2, avwap_dist_atr_or, ema_gap_atr_or, "MODERATE"
    )
    atr_pct_or = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx_or, exit_time_or, exit_price_or, outcome_or = simulate_exit_long(
        df_day, entry_idx_or, ep_or, cfg
    )
    net_pnl_or, gross_pnl_or = compute_pnl_pct(ep_or, exit_price_or, "LONG", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="LONG",
        setup="C_OR_BREAKOUT", impulse_type="OR",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx_or, "date"],
        entry_price=ep_or, sl_price=ep_or * (1.0 - cfg.stop_pct),
        target_price=ep_or * (1.0 + cfg.target_pct),
        exit_time_ist=exit_time_or, exit_price=exit_price_or, outcome=outcome_or,
        pnl_pct=net_pnl_or, pnl_pct_gross=gross_pnl_or,
        signal_price=R_close,
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr_or,
        ema20_gap_atr_signal=ema_gap_atr_or,
        atr_pct_signal=atr_pct_or,
        quality_score=qscore_or,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx_or)


# ===========================================================================
# D_EMA20_BOUNCE (v17k) — Pullback to EMA20 + bullish reject + break high.
# Trigger: bar low touches EMA20 (within atr_proximity), close upper-half,
# RSI rising, ADX>=threshold, volume>=ratio. Entry on lag=1 break of bar high.
# ===========================================================================
def _scan_ema20_bounce_at(
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
    if R_hour >= int(getattr(cfg, "ema20_bounce_max_hour_ist", 14)):
        return None, -1

    R_close = float(R["close"])
    R_open = float(R["open"])
    R_high = float(R["high"])
    R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))
    R_ema20 = float(R.get("EMA20", np.nan))

    if not (
        np.isfinite(R_atr) and R_atr > 0
        and np.isfinite(R_ema20)
        and (R_high - R_low) > 0 and R_close > 0
    ):
        return None, -1

    # Bar low must touch within atr_proximity of EMA20 (from above)
    proximity = float(getattr(cfg, "ema20_bounce_atr_proximity", 0.30)) * R_atr
    if not (R_low <= R_ema20 + proximity and R_low > R_ema20 - proximity * 2.0):
        return None, -1
    # Must close above EMA20 (rejection upward)
    if R_close <= R_ema20:
        return None, -1
    # Bar must be bullish with strong body
    if R_close <= R_open:
        return None, -1
    body_min = float(getattr(cfg, "ema20_bounce_body_atr_min", 0.40))
    if abs(R_close - R_open) / R_atr < body_min:
        return None, -1
    # Close in upper half of range
    upper_pct = float(getattr(cfg, "ema20_bounce_close_upper_pct", 0.50))
    if R_close < R_low + (1.0 - upper_pct) * (R_high - R_low):
        return None, -1

    # Momentum
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_prev = float(df_day.at[i - 1, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_min = float(getattr(cfg, "ema20_bounce_rsi_min", 50.0))
    if not (np.isfinite(rsi_now) and np.isfinite(rsi_prev) and rsi_now >= rsi_min and rsi_now > rsi_prev):
        return None, -1
    adx_min = float(getattr(cfg, "ema20_bounce_adx_min", 22.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1

    # Trend agreement: close > EMA50 (broader trend up)
    if bool(getattr(cfg, "ema20_bounce_require_close_gt_ema50", True)):
        ema50 = float(df_day.at[i, "EMA_50"]) if "EMA_50" in df_day.columns else np.nan
        if not (np.isfinite(ema50) and R_close > ema50):
            return None, -1

    # Volume
    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    vol_min = float(getattr(cfg, "ema20_bounce_volume_min_ratio", 1.20))
    if vol_sma > 0 and vol_now < vol_min * vol_sma:
        return None, -1

    # Entry on lag bar via break of R.high
    buf = entry_buffer(R_high, cfg)
    trigger = R_high + buf
    lag = int(getattr(cfg, "ema20_bounce_lag_bars", 1))
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    hi_e = float(df_day.at[cand, "high"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(hi_e) and np.isfinite(cl_e) and hi_e > trigger
            and (not cfg.require_entry_close_confirm or cl_e > trigger)):
        return None, -1
    entry_idx_e = cand
    if (len(df_day) - 1 - entry_idx_e) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep_e = float(trigger)

    atr_entry_e = float(df_day.at[entry_idx_e, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx_e, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx_e, "AVWAP"])
        else np.nan
    )
    close_entry = float(df_day.at[entry_idx_e, "close"])
    avwap_dist_atr_e = (
        (close_entry - avwap_entry) / atr_entry_e
        if (np.isfinite(atr_entry_e) and atr_entry_e > 0 and np.isfinite(avwap_entry))
        else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr_e > cap:
        return None, -1

    ema_gap_atr_e = (R_ema20 - R_close) / R_atr
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    adx_slope2 = (
        float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"])
        if i >= 2 and "ADX15" in df_day.columns else 0.0
    )
    qscore_e = compute_quality_score_long(
        adx_now, adx_slope2, avwap_dist_atr_e, ema_gap_atr_e, "MODERATE"
    )
    atr_pct_e = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx_e, exit_time_e, exit_price_e, outcome_e = simulate_exit_long(
        df_day, entry_idx_e, ep_e, cfg
    )
    net_pnl_e, gross_pnl_e = compute_pnl_pct(ep_e, exit_price_e, "LONG", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="LONG",
        setup="D_EMA20_BOUNCE", impulse_type="EMA20",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx_e, "date"],
        entry_price=ep_e, sl_price=ep_e * (1.0 - cfg.stop_pct),
        target_price=ep_e * (1.0 + cfg.target_pct),
        exit_time_ist=exit_time_e, exit_price=exit_price_e, outcome=outcome_e,
        pnl_pct=net_pnl_e, pnl_pct_gross=gross_pnl_e,
        signal_price=R_close,
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr_e,
        ema20_gap_atr_signal=ema_gap_atr_e,
        atr_pct_signal=atr_pct_e,
        quality_score=qscore_e,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx_e)


# ===========================================================================
# E_VWAP_BAND_FADE (v17k) — Lower Bollinger touch + bullish reject (LONG).
# Mean-reversion play: bar low pierces Lower_Band, closes back above with
# bullish body; oversold RSI; entry on lag=1 break of bar high.
# ===========================================================================
def _scan_vwap_band_fade_long_at(
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

    # Lower_Band touch from above + reject back above
    R_lower_band = float(R.get("Lower_Band", np.nan))
    if not (np.isfinite(R_lower_band) and R_low <= R_lower_band and R_close > R_lower_band):
        return None, -1
    # Bullish bar with strong body
    if R_close <= R_open:
        return None, -1
    body_min = float(getattr(cfg, "vwap_band_fade_body_atr_min", 0.40))
    if abs(R_close - R_open) / R_atr < body_min:
        return None, -1
    upper_pct = float(getattr(cfg, "vwap_band_fade_close_upper_pct", 0.50))
    if R_close < R_low + (1.0 - upper_pct) * (R_high - R_low):
        return None, -1

    # Oversold RSI
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
    rsi_max = float(getattr(cfg, "vwap_band_fade_rsi_max", 40.0))
    if not (np.isfinite(rsi_now) and rsi_now <= rsi_max):
        return None, -1

    # Volume
    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    vol_min = float(getattr(cfg, "vwap_band_fade_volume_min_ratio", 1.50))
    if vol_sma > 0 and vol_now < vol_min * vol_sma:
        return None, -1

    # Entry on lag bar via break of R.high
    buf = entry_buffer(R_high, cfg)
    trigger = R_high + buf
    lag = int(getattr(cfg, "vwap_band_fade_lag_bars", 1))
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    hi_e = float(df_day.at[cand, "high"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(hi_e) and np.isfinite(cl_e) and hi_e > trigger
            and (not cfg.require_entry_close_confirm or cl_e > trigger)):
        return None, -1
    entry_idx_v = cand
    if (len(df_day) - 1 - entry_idx_v) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep_v = float(trigger)

    # Diagnostics
    atr_entry_v = float(df_day.at[entry_idx_v, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx_v, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx_v, "AVWAP"])
        else np.nan
    )
    close_entry = float(df_day.at[entry_idx_v, "close"])
    avwap_dist_atr_v = (
        (close_entry - avwap_entry) / atr_entry_v
        if (np.isfinite(atr_entry_v) and atr_entry_v > 0 and np.isfinite(avwap_entry))
        else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr_v > cap:
        return None, -1

    R_ema20 = float(R.get("EMA20", np.nan))
    ema_gap_atr_v = (R_ema20 - R_close) / R_atr if (np.isfinite(R_ema20) and R_atr > 0) else 0.0
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    adx_slope2 = (
        float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"])
        if i >= 2 and "ADX15" in df_day.columns else 0.0
    )
    qscore_v = compute_quality_score_long(
        adx_now, adx_slope2, avwap_dist_atr_v, ema_gap_atr_v, "MODERATE"
    )
    atr_pct_v = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx_v, exit_time_v, exit_price_v, outcome_v = simulate_exit_long(
        df_day, entry_idx_v, ep_v, cfg
    )
    net_pnl_v, gross_pnl_v = compute_pnl_pct(ep_v, exit_price_v, "LONG", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="LONG",
        setup="E_VWAP_BAND_FADE", impulse_type="VWAP_BAND",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx_v, "date"],
        entry_price=ep_v, sl_price=ep_v * (1.0 - cfg.stop_pct),
        target_price=ep_v * (1.0 + cfg.target_pct),
        exit_time_ist=exit_time_v, exit_price=exit_price_v, outcome=outcome_v,
        pnl_pct=net_pnl_v, pnl_pct_gross=gross_pnl_v,
        signal_price=R_close,
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr_v,
        ema20_gap_atr_signal=ema_gap_atr_v,
        atr_pct_signal=atr_pct_v,
        quality_score=qscore_v,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx_v)


# ===========================================================================
# G_HIGHER_HIGH_BREAK (v17k) — Break above N-bar swing high (LONG).
# Computes intraday rolling N-bar high, fires when current bar's high pierces
# above it AND closes above it with body + vol confirm. Entry on lag=1 bar.
# ===========================================================================
def _scan_higher_high_break_at(
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
    if R_hour >= int(getattr(cfg, "g_hh_max_hour_ist", 14)):
        return None, -1

    R_close = float(R["close"]); R_open = float(R["open"])
    R_high = float(R["high"]); R_low = float(R["low"])
    R_atr = float(R.get("ATR15", np.nan))
    if not (np.isfinite(R_atr) and R_atr > 0 and (R_high - R_low) > 0 and R_close > 0):
        return None, -1

    # N-bar swing high (intraday): highest high of prior N bars
    lookback = int(getattr(cfg, "g_hh_lookback_bars", 5))
    if i < lookback:
        return None, -1
    prior_highs = df_day["high"].iloc[i - lookback:i]
    swing_high = float(prior_highs.max())
    if not np.isfinite(swing_high):
        return None, -1
    # Bar must break above prior swing high AND close above it
    if not (R_high > swing_high and R_close > swing_high):
        return None, -1

    # Bullish bar with body
    if R_close <= R_open:
        return None, -1
    body_min = float(getattr(cfg, "g_hh_body_atr_min", 0.40))
    if abs(R_close - R_open) / R_atr < body_min:
        return None, -1
    upper_pct = float(getattr(cfg, "g_hh_close_upper_pct", 0.40))
    if R_close < R_low + (1.0 - upper_pct) * (R_high - R_low):
        return None, -1

    # ADX & volume
    adx_min = float(getattr(cfg, "g_hh_adx_min", 22.0))
    adx_now = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
    if not (np.isfinite(adx_now) and adx_now >= adx_min):
        return None, -1
    vol_now = float(R.get("volume", 0.0)) if np.isfinite(R.get("volume", np.nan)) else 0.0
    vol_sma = float(R.get("VOL_SMA20", 0.0)) if np.isfinite(R.get("VOL_SMA20", np.nan)) else 0.0
    vol_min = float(getattr(cfg, "g_hh_volume_min_ratio", 1.30))
    if vol_sma > 0 and vol_now < vol_min * vol_sma:
        return None, -1

    # Entry on lag bar via standard breakout above R.high
    buf = entry_buffer(R_high, cfg)
    trigger = R_high + buf
    lag = int(getattr(cfg, "g_hh_lag_bars", 1))
    cand = i + max(lag, 1)
    if cand >= len(df_day) or not in_signal_window(df_day.at[cand, "date"], cfg):
        return None, -1
    hi_e = float(df_day.at[cand, "high"])
    cl_e = float(df_day.at[cand, "close"])
    if not (np.isfinite(hi_e) and np.isfinite(cl_e) and hi_e > trigger
            and (not cfg.require_entry_close_confirm or cl_e > trigger)):
        return None, -1
    entry_idx_g = cand
    if (len(df_day) - 1 - entry_idx_g) < int(cfg.min_bars_left_after_entry):
        return None, -1

    ep_g = float(trigger)
    atr_entry_g = float(df_day.at[entry_idx_g, "ATR15"])
    avwap_entry = (
        float(df_day.at[entry_idx_g, "AVWAP"])
        if "AVWAP" in df_day.columns and np.isfinite(df_day.at[entry_idx_g, "AVWAP"]) else np.nan
    )
    close_entry = float(df_day.at[entry_idx_g, "close"])
    avwap_dist_atr_g = (
        (close_entry - avwap_entry) / atr_entry_g
        if (np.isfinite(atr_entry_g) and atr_entry_g > 0 and np.isfinite(avwap_entry)) else 0.0
    )
    cap = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0) or 0.0)
    if cap > 0.0 and avwap_dist_atr_g > cap:
        return None, -1

    R_ema20 = float(R.get("EMA20", np.nan))
    ema_gap_atr_g = (R_ema20 - R_close) / R_atr if (np.isfinite(R_ema20) and R_atr > 0) else 0.0
    rsi_now = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
    k_now = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
    adx_slope2 = (
        float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"])
        if i >= 2 and "ADX15" in df_day.columns else 0.0
    )
    qscore_g = compute_quality_score_long(
        adx_now, adx_slope2, avwap_dist_atr_g, ema_gap_atr_g, "MODERATE"
    )
    atr_pct_g = (R_atr / R_close) if R_close > 0 else 0.0

    exit_idx_g, exit_time_g, exit_price_g, outcome_g = simulate_exit_long(
        df_day, entry_idx_g, ep_g, cfg
    )
    net_pnl_g, gross_pnl_g = compute_pnl_pct(ep_g, exit_price_g, "LONG", cfg)

    trade = Trade(
        trade_date=day_str, ticker=ticker, side="LONG",
        setup="G_HIGHER_HIGH_BREAK", impulse_type="STRUCTURE",
        signal_time_ist=R_ts, entry_time_ist=df_day.at[entry_idx_g, "date"],
        entry_price=ep_g, sl_price=ep_g * (1.0 - cfg.stop_pct),
        target_price=ep_g * (1.0 + cfg.target_pct),
        exit_time_ist=exit_time_g, exit_price=exit_price_g, outcome=outcome_g,
        pnl_pct=net_pnl_g, pnl_pct_gross=gross_pnl_g,
        signal_price=R_close,
        adx_signal=adx_now if np.isfinite(adx_now) else 0.0,
        rsi_signal=rsi_now if np.isfinite(rsi_now) else 0.0,
        stochk_signal=k_now if np.isfinite(k_now) else 0.0,
        avwap_dist_atr_signal=avwap_dist_atr_g,
        ema20_gap_atr_signal=ema_gap_atr_g,
        atr_pct_signal=atr_pct_g,
        quality_score=qscore_g,
        india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
    )
    return trade, int(exit_idx_g)


# ===========================================================================
# SCAN ONE DAY (LONG)
# ===========================================================================
def scan_one_day(
    ticker: str, df_day: pd.DataFrame, day_str: str, cfg: StrategyConfig
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

    trades: List[Trade] = []
    i = 2
    day_sl_count = 0
    day_r_total = 0.0
    cooldown_until_idx = -1

    # v17k — Compute opening range once per day for C_OR_BREAKOUT.
    or_high = float("nan")
    or_low = float("nan")
    or_width_pct = 0.0
    or_end_idx = -1
    if getattr(cfg, "enable_setup_c_or_breakout", False):
        or_n = int(getattr(cfg, "or_breakout_window_bars", 6))
        if len(df_day) > or_n:
            or_slice = df_day.iloc[:or_n]
            or_high = float(or_slice["high"].max())
            or_low = float(or_slice["low"].min())
            ref_close = float(df_day.iloc[0]["close"])
            if ref_close > 0 and np.isfinite(or_high) and np.isfinite(or_low):
                or_width_pct = (or_high - or_low) / ref_close
            or_end_idx = or_n  # first index AFTER OR window

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

        if not in_signal_window(c1["date"], cfg):
            i += 1
            continue

        # v17k — C_OR_BREAKOUT: only fire post-OR window.
        if (
            getattr(cfg, "enable_setup_c_or_breakout", False)
            and i >= or_end_idx
            and np.isfinite(or_high)
            and np.isfinite(or_low)
        ):
            or_trade, or_exit_idx = _scan_or_breakout_at(
                df_day, i, ticker, day_str, cfg, or_high, or_low, or_width_pct
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

        # v17k — D_EMA20_BOUNCE: pullback-to-EMA20 bullish reject.
        if getattr(cfg, "enable_setup_d_ema20_bounce", False):
            ema_trade, ema_exit_idx = _scan_ema20_bounce_at(
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

        # v17k — E_VWAP_BAND_FADE: Lower BB touch + bullish reject (mean revert).
        if getattr(cfg, "enable_setup_e_vwap_band_fade", False):
            v_trade, v_exit_idx = _scan_vwap_band_fade_long_at(
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

        # v17k — G_HIGHER_HIGH_BREAK: break N-bar swing high.
        if getattr(cfg, "enable_setup_g_higher_high_break", False):
            g_trade, g_exit_idx = _scan_higher_high_break_at(
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

        # v17g — B_AVWAP_RECLAIM_REVERSAL: independent of impulse, runs first.
        # If a reversal entry fires, advance past the exit and skip the
        # impulse path for this iteration.
        if getattr(cfg, "enable_setup_b_avwap_reclaim_reversal", False):
            rev_trade, rev_exit_idx = _scan_reversal_at(
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

        impulse = classify_green_impulse(c1, cfg)
        if impulse == "":
            i += 1
            continue

        if not market_regime_pass(c1["date"], "LONG", cfg):
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

        if not _trend_filter_long(df_day, i, c1, cfg):
            i += 1
            continue

        has_sweep_ctx = has_recent_liquidity_sweep(df_day, i, "LONG", cfg)
        if not has_sweep_ctx:
            i += 1
            continue
        if avwap_no_trade_zone_block(c1, impulse, has_sweep_ctx, cfg):
            i += 1
            continue

        # Diagnostic values
        adx1 = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else 0.0
        rsi1 = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else 0.0
        k1 = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else 0.0
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
            # --- Realistic entry price selection ---
            # entry_at_next_open: enter at open of bar entry_idx+1.
            #   simulate_exit_long(entry_idx, ...) starts the walk from entry_idx+1,
            #   so the same bar as the open price is correctly evaluated intrabar.
            # entry_at_bar_close: enter at close of confirmation bar.
            #   Exit walk starts from entry_idx+1 (skip the entry bar, consistent with
            #   the standard "bar just closed" assumption).
            # Default: trigger price (backtest-ideal intrabar fill).
            if cfg.entry_at_next_open:
                next_idx = entry_idx + 1
                if next_idx >= len(df_day):
                    return None, entry_idx
                ep = float(df_day.at[next_idx, "open"])
                # simulate_exit_long starts from entry_idx+1 which equals next_idx — correct.
            elif cfg.entry_at_bar_close:
                ep = float(df_day.at[entry_idx, "close"])
            else:
                ep = float(entry_price)

            # Max-slip gate: skip trade when actual fill price deviates too far from trigger.
            if cfg.max_entry_slip_pct > 0.0 and float(entry_price) > 0.0:
                if ep > float(entry_price) * (1.0 + cfg.max_entry_slip_pct):
                    return None, entry_idx

            # --- Quality gates (skip low-confidence entries before costly exit sim) ---
            # Backed by 518-trade data analysis: EMA gap 1.0–1.5 ATR = 91% win,
            # QS<5 trades are near-breakeven drag, AVWAP dist <0.5 ATR = weak momentum.
            adx_slope2 = (
                float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"]) if i >= 2 else 0.0
            )
            ema_gap_atr = (close1 - ema20) / atr1 if atr1 > 0 else 0.0
            qscore = compute_quality_score_long(
                adx1, adx_slope2, avwap_dist_atr, ema_gap_atr, impulse
            )
            if cfg.signal_avwap_dist_atr_min > 0.0 and avwap_dist_atr < cfg.signal_avwap_dist_atr_min:
                return None, entry_idx
            if cfg.ema_gap_atr_min > 0.0 and ema_gap_atr < cfg.ema_gap_atr_min:
                return None, entry_idx
            if cfg.quality_score_min > 0.0 and qscore < cfg.quality_score_min:
                return None, entry_idx

            exit_idx, exit_time, exit_price, outcome = simulate_exit_long(
                df_day, entry_idx, ep, cfg
            )
            net_pnl, gross_pnl = compute_pnl_pct(ep, exit_price, "LONG", cfg)

            return (
                Trade(
                    trade_date=day_str,
                    ticker=ticker,
                    side="LONG",
                    setup=setup,
                    impulse_type=impulse,
                    signal_time_ist=signal_time,
                    entry_time_ist=df_day.at[entry_idx, "date"],
                    entry_price=ep,
                    sl_price=ep * (1.0 - cfg.stop_pct),
                    target_price=ep * (1.0 + cfg.target_pct),
                    exit_time_ist=exit_time,
                    exit_price=exit_price,
                    outcome=outcome,
                    pnl_pct=net_pnl,
                    pnl_pct_gross=gross_pnl,
                    signal_price=close1,
                    adx_signal=adx1,
                    rsi_signal=rsi1,
                    stochk_signal=k1,
                    avwap_dist_atr_signal=avwap_dist_atr,
                    ema20_gap_atr_signal=ema_gap_atr,
                    atr_pct_signal=atr_pct,
                    quality_score=qscore,
                    india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
                ),
                exit_idx,
            )

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
        # SETUP A (MODERATE): break C1 high, or pullback + break C2 high
        # ---------------------------------------------------------------
        if impulse == "MODERATE":
            c2 = df_day.iloc[i + 1]
            buf1 = entry_buffer(high1, cfg)
            trigger = high1 + buf1

            # Option 1: break C1 high on configured lag bar
            lag1 = int(cfg.lag_bars_long_a_mod_break_c1_high)
            entry_idx_1 = i + lag1 if lag1 >= 0 else (i + 1)
            if entry_idx_1 < len(df_day) and float(df_day.at[entry_idx_1, "high"]) > trigger:
                entry_idx = entry_idx_1
                entry_ts = df_day.at[entry_idx, "date"]

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

                _result = _make_trade(
                    entry_idx, trigger, "A_MOD_BREAK_C1_HIGH", avwap_dist_atr
                )
                if _result[0] is None:
                    i += 1
                    continue
                trade, exit_idx = _result
                _register_trade_result(trade, exit_idx)
                i = exit_idx + 1
                continue

            # Option 3 (optional): continuation break above C1 close.
            # This is a faster/frequent variant than strict C1-high break.
            if cfg.enable_setup_a_close_continuation_break:
                close_trigger = close1 + entry_buffer(close1, cfg)
                lag3 = int(cfg.lag_bars_long_a_close_continuation_break)
                entry_idx_3 = i + lag3 if lag3 >= 0 else (i + 1)
                if entry_idx_3 < len(df_day) and float(df_day.at[entry_idx_3, "high"]) > close_trigger:
                    entry_idx = entry_idx_3
                    entry_ts = df_day.at[entry_idx, "date"]

                    if not in_signal_window(entry_ts, cfg):
                        i += 1
                        continue
                    if not _close_confirm_ok(entry_idx, close_trigger):
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

                    _result = _make_trade(
                        entry_idx, close_trigger, "A_MOD_CLOSE_CONTINUATION_BREAK", avwap_dist_atr
                    )
                    if _result[0] is None:
                        i += 1
                        continue
                    trade, exit_idx = _result
                    _register_trade_result(trade, exit_idx)
                    i = exit_idx + 1
                    continue

            # Option 2 (optional): small red pullback C2 (above AVWAP), then break C2 high on C3
            # Controlled by cfg.enable_setup_a_pullback_c2_break (default False)
            c2o, c2c = float(c2["open"]), float(c2["close"])
            c2_body = abs(c2c - c2o)
            c2_atr = float(c2["ATR15"]) if np.isfinite(c2["ATR15"]) else atr1
            c2_avwap = float(c2["AVWAP"]) if np.isfinite(c2["AVWAP"]) else np.nan

            c2_small_red = (
                (c2c < c2o)
                and np.isfinite(c2_atr)
                and (c2_body <= cfg.small_counter_max_atr * c2_atr)
            )
            c2_above_avwap = np.isfinite(c2_avwap) and (c2c > c2_avwap)

            if cfg.enable_setup_a_pullback_c2_break and c2_small_red and c2_above_avwap and (i + 2 < len(df_day)):
                high2 = float(c2["high"])
                buf2 = entry_buffer(high2, cfg)
                trigger2 = high2 + buf2

                lag2 = int(cfg.lag_bars_long_a_pullback_c2_break_c2_high)
                entry_idx_2 = i + lag2 if lag2 >= 0 else (i + 2)
                if entry_idx_2 < len(df_day) and float(df_day.at[entry_idx_2, "high"]) > trigger2:
                    entry_idx = entry_idx_2
                    entry_ts = df_day.at[entry_idx, "date"]

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

                    _result = _make_trade(
                        entry_idx, trigger2, "A_PULLBACK_C2_THEN_BREAK_C2_HIGH", avwap_dist_atr
                    )
                    if _result[0] is None:
                        i += 1
                        continue
                    trade, exit_idx = _result
                    _register_trade_result(trade, exit_idx)
                    i = exit_idx + 1
                    continue

            i += 1
            continue

        # ---------------------------------------------------------------
        # SETUP B (HUGE): pullback holds, then break pullback high
        # ---------------------------------------------------------------
        if impulse == "HUGE":
            pull_end = min(i + 3, len(df_day) - 1)
            pull = df_day.iloc[i + 1 : pull_end + 1].copy()
            if pull.empty:
                i += 1
                continue

            mid_body = (open1 + close1) / 2.0

            pull_atr = pd.to_numeric(pull["ATR15"], errors="coerce").fillna(atr1)
            pull_body = (
                pd.to_numeric(pull["close"], errors="coerce")
                - pd.to_numeric(pull["open"], errors="coerce")
            ).abs()
            pull_red = pd.to_numeric(pull["close"], errors="coerce") < pd.to_numeric(
                pull["open"], errors="coerce"
            )
            pull_small = pull_body <= (cfg.small_counter_max_atr * pull_atr)

            if not bool((pull_red & pull_small).any()):
                i += 1
                continue

            lows = pd.to_numeric(pull["low"], errors="coerce")
            closes = pd.to_numeric(pull["close"], errors="coerce")
            avwaps = pd.to_numeric(pull["AVWAP"], errors="coerce")

            hold_mid = bool((lows > mid_body).fillna(False).all())
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
                j_iter = [j_fixed] if (pull_end + 1 <= j_fixed < len(df_day)) else []
            else:
                j_iter = range(pull_end + 1, min(len(df_day), pull_end + 4))  # V9: cap at 3 bars

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

                    _result = _make_trade(
                        j, trigger, "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK", avwap_dist_atr
                    )
                    if _result[0] is None:
                        continue  # slip rejected — try next j
                    trade, exit_idx = _result
                    _register_trade_result(trade, exit_idx)
                    i = exit_idx + 1
                    entered = True
                    break

            # Optional HUGE continuation setup: reclaim/break above C1 close.
            if (not entered) and cfg.enable_setup_b_huge_c1_close_reclaim_break:
                trigger_c1_close = close1 + entry_buffer(close1, cfg)
                lag_reclaim = int(cfg.lag_bars_long_b_huge_c1_close_reclaim_break)
                if lag_reclaim >= 0:
                    j_fixed = i + lag_reclaim
                    j_iter2 = [j_fixed] if ((i + 1) <= j_fixed < len(df_day)) else []
                else:
                    # Dynamic reclaim within a bounded lookahead to avoid very late-day entries.
                    j_iter2 = range(i + 1, min(len(df_day), i + 7))

                for j in j_iter2:
                    tsj = df_day.at[j, "date"]
                    if not in_signal_window(tsj, cfg):
                        continue
                    if not _bars_left_ok(j):
                        continue

                    closej = float(df_day.at[j, "close"])
                    avwapj = (
                        float(df_day.at[j, "AVWAP"])
                        if np.isfinite(df_day.at[j, "AVWAP"])
                        else np.nan
                    )
                    if np.isfinite(avwapj) and closej <= avwapj:
                        continue

                    if float(df_day.at[j, "high"]) > trigger_c1_close:
                        if not _close_confirm_ok(j, trigger_c1_close):
                            continue

                        atr_entry = float(df_day.at[j, "ATR15"])
                        ok_support, avwap_dist_atr = avwap_support_pass(
                            df_day, i, j, atr_entry, cfg
                        )
                        if not ok_support:
                            continue

                        _result = _make_trade(
                            j, trigger_c1_close, "B_HUGE_C1_CLOSE_RECLAIM_BREAK", avwap_dist_atr
                        )
                        if _result[0] is None:
                            continue  # slip rejected — try next j
                        trade, exit_idx = _result
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
    df = _prepare_session_bars_for_scan(df_full, cfg)
    return scan_all_days_for_ticker_prepared(ticker, df, cfg)


def scan_all_days_for_ticker_prepared(
    ticker: str, df: pd.DataFrame, cfg: StrategyConfig
) -> List[Trade]:
    if df.empty:
        return []

    all_trades: List[Trade] = []
    for day_val, df_day in df.groupby("day", sort=True):
        df_day = df_day.copy().reset_index(drop=True)
        if len(df_day) < int(cfg.min_bars_for_scan):
            continue
        trades = scan_one_day(ticker, df_day, str(day_val), cfg)
        if trades:
            all_trades.extend(trades)

    return all_trades

# ===========================================================================
# RUN ALL TICKERS (called by combined runner)
# ===========================================================================
def run_long_scan(cfg: Optional[StrategyConfig] = None) -> List[Trade]:
    if cfg is None:
        cfg = default_long_config()

    tickers = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    print(f"[LONG] Tickers found: {len(tickers)}")

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
            print(f"  [LONG] scanned {k}/{len(tickers)} | trades_so_far={len(all_trades)}")

    return all_trades
