# -*- coding: utf-8 -*-
"""
eqidv5_short_strategy.py -- SHORT side: Volume Profile + AVWAP + Liquidity Sweep
=================================================================================

SHORT setup logic (all 15-min timeframe) -- mirror of LONG:
  1. Prior-day Volume Profile: compute POC / VAH / VAL from previous session.
  2. Bias check: close <= session AVWAP (within 0.2% tolerance) = bearish bias.
  3. Sweep detection: bar.high > recent_swing_high  AND  bar.close < recent_swing_high
     (stop-hunt above a swing high; price fails to hold above == bear reversal signal).
  4. Volume confirmation: bar volume >= sweep_vol_ratio * SMA20.
  5. Absorption proxy: upper wick (sweep wick) is large relative to body.
  6. Quality score >= min_quality_score (out of 12, PDF Section 8).
  7. R:R >= min_rr.

Entry  : close of sweep bar.
SL     : sweep wick (bar.high) * (1 + sl_buffer_pct).
T1     : session AVWAP (if below entry).
T2     : prior-day POC  (if below entry).
T3     : prior-day VAL  (if below entry).
Exit   : walk forward bar-by-bar; first SL / target / BE / TRAIL / EOD hit.
"""

from __future__ import annotations

import os
import sys
from dataclasses import asdict
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Make sibling imports work when run directly
_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from eqidv5_strategy_common import (
    IST,
    Eqidv5Config,
    Trade,
    apply_topn_per_day,
    compute_pnl_pct,
    compute_session_avwap,
    compute_volume_profile,
    default_short_config,
    detect_swing_highs,
    list_tickers_15m,
    prepare_indicators,
    read_15m_parquet,
    trades_to_df,
)


# ===========================================================================
# TIMEZONE HELPERS
# ===========================================================================
def _to_ist_ts(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _to_ist_series(values: pd.Series) -> pd.Series:
    s = pd.to_datetime(values, errors="coerce")
    if getattr(s.dt, "tz", None) is None:
        return s.dt.tz_localize(IST)
    return s.dt.tz_convert(IST)


# ===========================================================================
# QUALITY SCORE (SHORT) -- PDF Section 8 scoring, max 12 points, trade if >= 6
# ===========================================================================
# Score thresholds (PDF Section 8):
#   9-12  High conviction -- full size
#   6-8   Standard trade -- normal size
#   4-5   Reduce 50% or skip
#   < 4   DO NOT TRADE
def compute_quality_score_short(
    avwap_bias: bool,          # +1  price below Daily AVWAP (bias aligned)
    near_poc: bool,            # +2  price at Volume Profile POC
    near_vah: bool,            # +1  price at VAH (resistance zone)
    avwap_vp_confluent: bool,  # +1  AVWAP confluent with VP zone
    sweep_confirmed: bool,     # +2  liquidity sweep clearly on 15 Min
    absorption_ok: bool,       # +2  order flow absorption on 5 Min (rejection)
    cvd_divergence: bool,      # +1  CVD divergence present (bearish at sweep high)
    lvn_below: bool,           # +1  LVN in trade direction (below for SHORT = fast target)
    high_vol_sweep: bool,      # +1  high relative volume on sweep candle
) -> int:
    score = 0
    if avwap_bias:         score += 1
    if near_poc:           score += 2
    if near_vah:           score += 1
    if avwap_vp_confluent: score += 1
    if sweep_confirmed:    score += 2
    if absorption_ok:      score += 2
    if cvd_divergence:     score += 1
    if lvn_below:          score += 1
    if high_vol_sweep:     score += 1
    return score  # max = 12


def _prepare_5m_features(df_5m_day: pd.DataFrame) -> pd.DataFrame:
    if df_5m_day.empty:
        return df_5m_day

    d = df_5m_day.copy()
    d["date"] = _to_ist_series(d["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    if d.empty:
        return d

    candle_dir = np.sign(d["close"] - d["open"])
    d["signed_vol"] = candle_dir * d["volume"].fillna(0.0)
    d["abs_signed_sma20"] = d["signed_vol"].abs().rolling(20, min_periods=5).mean()
    d["cvd_proxy"] = d["signed_vol"].cumsum()
    return d


def _confirm_entry_short_5m(
    df_5m_day: pd.DataFrame,
    signal_ts: pd.Timestamp,
    sweep_level: float,
    cfg: Eqidv5Config,
) -> Optional[Tuple[pd.Timestamp, float, Dict[str, float]]]:
    """
    5-min proxy for order-flow confirmation (PDF):
      - opposite-side delta spike (buy pressure),
      - CVD proxy turns down,
      - sweep-zone retest/failure,
      - first red confirmation candle gives entry.
    """
    if df_5m_day.empty:
        return None

    d = _prepare_5m_features(df_5m_day)
    if d.empty:
        return None

    end_ts = signal_ts + timedelta(minutes=int(cfg.confirm_window_min))
    cands = d[(d["date"] > signal_ts) & (d["date"] <= end_ts)].copy()
    if cands.empty:
        return None

    for idx in cands.index:
        row = cands.loc[idx]
        o = float(row["open"])
        c = float(row["close"])
        if not np.isfinite(o) or not np.isfinite(c) or c >= o:
            continue

        pre = d[(d["date"] >= signal_ts) & (d["date"] <= row["date"])].copy()
        if len(pre) < 2:
            continue

        delta_thr = float(cfg.delta_spike_mult) * pd.to_numeric(
            pre["abs_signed_sma20"], errors="coerce"
        ).fillna(0.0)
        delta_spike = bool(((pre["signed_vol"] > 0) & (pre["signed_vol"].abs() >= delta_thr)).any())

        lb = max(int(cfg.cvd_lookback_bars), 2)
        tail = pre.tail(lb)
        cvd_turn_down = bool(len(tail) >= 2 and float(tail["cvd_proxy"].iloc[-1]) < float(tail["cvd_proxy"].iloc[0]))

        # 3) Reclaim/accept below swept level.
        price_accept = bool(c < float(sweep_level) * (1.0 - 0.0002))

        score = int(delta_spike) + int(cvd_turn_down) + int(price_accept)
        if score >= int(cfg.confirm_min_score):
            return _to_ist_ts(row["date"]), c, {
                "delta_spike": float(delta_spike),
                "cvd_turn_down": float(cvd_turn_down),
                "price_accept": float(price_accept),
                "score": float(score),
            }

    return None


# ===========================================================================
# EXIT SIMULATION (SHORT)
# ===========================================================================
def simulate_exit_short(
    df_day: pd.DataFrame,
    entry_idx: int,
    entry_ts: pd.Timestamp,
    entry_price: float,
    sl_price: float,
    t1_price: float,
    t2_price: float,
    t3_price: float,
    cfg: Eqidv5Config,
) -> Tuple[pd.Timestamp, float, str]:
    """
    Walk forward bar-by-bar after entry (SHORT).

    Consolidated target = weighted average of T1/T2/T3 (those below entry).
    Priority: SL > TARGET (conservative -- SL wins if both triggered same bar).

    Returns (exit_timestamp, exit_price, outcome).
    outcome in {"TARGET", "SL", "BE", "TIME", "EOD"}
    """
    # Build composite target from T-levels that are below entry price
    valid_targets = [
        (t1_price, cfg.t1_pct),
        (t2_price, cfg.t2_pct),
        (t3_price, cfg.t3_pct),
    ]
    valid_targets = [(p, w) for p, w in valid_targets if np.isfinite(p) and p < entry_price]

    if valid_targets:
        total_w = sum(w for _, w in valid_targets)
        target_price = sum(p * w for p, w in valid_targets) / total_w
    else:
        # Fallback: 1.5x SL distance as target (downward)
        risk = sl_price - entry_price
        target_price = entry_price - cfg.min_rr * risk

    sl_curr = float(sl_price)
    be_armed = False
    be_level = entry_price * (1.0 - cfg.be_pad_pct)
    best_price = entry_price  # best (lowest) price seen for SHORT

    last_ts = _to_ist_ts(df_day.at[entry_idx, "date"])
    last_close = entry_price
    time_stop_ts = entry_ts + timedelta(minutes=int(cfg.scalp_time_stop_minutes))

    for k in range(entry_idx + 1, len(df_day)):
        hi = float(df_day.at[k, "high"])
        lo = float(df_day.at[k, "low"])
        close_k = float(df_day.at[k, "close"])
        ts = _to_ist_ts(df_day.at[k, "date"])
        bar_time = ts.time()

        if not (np.isfinite(hi) and np.isfinite(lo)):
            last_ts = ts
            last_close = close_k
            continue

        # Track best (lowest) favorable price for SHORT
        if lo < best_price:
            best_price = lo

        # Arm breakeven (price moved cfg.be_trigger_pct below entry)
        if (
            cfg.enable_breakeven
            and not be_armed
            and lo <= entry_price * (1.0 - cfg.be_trigger_pct)
        ):
            be_armed = True
            sl_curr = min(sl_curr, be_level)

        # Trailing stop (after BE, trail from lowest price seen)
        if be_armed and cfg.enable_trailing_stop:
            trail_sl = best_price * (1.0 + cfg.trail_pct)
            sl_curr = min(sl_curr, trail_sl)

        # SL + Target: use bar direction to infer which came first on same bar
        sl_hit = hi >= sl_curr
        tgt_hit = lo <= target_price
        if sl_hit and tgt_hit:
            # Both triggered: bearish bar → target first; bullish bar → SL first
            bar_open = float(df_day.at[k, "open"])
            if close_k <= bar_open:
                return ts, target_price, "TARGET"
            else:
                outcome = "BE" if be_armed else "SL"
                return ts, sl_curr, outcome
        elif sl_hit:
            outcome = "BE" if be_armed else "SL"
            return ts, sl_curr, outcome
        elif tgt_hit:
            return ts, target_price, "TARGET"

        # Time stop for stalled setups.
        if ts >= time_stop_ts:
            return ts, close_k, "TIME"

        # EOD: session end
        if bar_time >= cfg.session_end:
            return ts, close_k, "EOD"

        last_ts = ts
        last_close = close_k

    return last_ts, last_close, "EOD"


# ===========================================================================
# SCAN ONE DAY (SHORT)
# ===========================================================================
def scan_one_day_short(
    ticker: str,
    df_day: pd.DataFrame,
    df_5m_day: Optional[pd.DataFrame],
    prior_poc: float,
    prior_vah: float,
    prior_val: float,
    prior_lvn_levels: List[float],
    cfg: Eqidv5Config,
) -> List[Trade]:
    """
    Scan a single trading day for SHORT liquidity-sweep setups.

    Returns a list of Trade objects (typically 0 or 1 per ticker per day).
    """
    trades: List[Trade] = []

    min_bars = max(cfg.swing_lookback + 3, 10)
    if len(df_day) < min_bars:
        return trades

    # Session-anchored VWAP
    avwap_series = compute_session_avwap(df_day)

    # Raw arrays
    lows_arr    = pd.to_numeric(df_day["low"],     errors="coerce").values
    highs_arr   = pd.to_numeric(df_day["high"],    errors="coerce").values
    closes_arr  = pd.to_numeric(df_day["close"],   errors="coerce").values
    opens_arr   = pd.to_numeric(df_day["open"],    errors="coerce").values
    volumes_arr = pd.to_numeric(
        df_day["volume"] if "volume" in df_day.columns else pd.Series(0, index=df_day.index),
        errors="coerce",
    ).values
    atrs_arr    = pd.to_numeric(df_day["ATR15"],   errors="coerce").values
    rsis_arr    = pd.to_numeric(df_day["RSI15"],   errors="coerce").values
    adxs_arr    = pd.to_numeric(df_day["ADX15"],   errors="coerce").values
    emas_arr    = pd.to_numeric(df_day["EMA20"],   errors="coerce").values
    vsma_arr    = pd.to_numeric(df_day["VOL_SMA20"], errors="coerce").values
    ts_series   = _to_ist_series(df_day["date"])

    # Pre-compute swing highs over the full day
    swing_high_arr = detect_swing_highs(highs_arr, cfg.swing_lookback)

    # Pre-process 5-min data once for CVD divergence checks
    df_5m_prep: pd.DataFrame = pd.DataFrame()
    if df_5m_day is not None and not df_5m_day.empty:
        df_5m_prep = _prepare_5m_features(df_5m_day)

    for i in range(cfg.swing_lookback + 2, len(df_day)):
        if cfg.max_trades_per_ticker_per_day > 0 and len(trades) >= cfg.max_trades_per_ticker_per_day:
            break

        ts_raw = ts_series.iloc[i]
        if pd.isna(ts_raw):
            continue
        ts_pd = _to_ist_ts(ts_raw)
        bar_time = ts_pd.time()

        if bar_time < cfg.entry_start or bar_time > cfg.entry_end:
            continue

        l   = lows_arr[i]
        h   = highs_arr[i]
        c   = closes_arr[i]
        o   = opens_arr[i]
        v   = volumes_arr[i]
        atr = atrs_arr[i]
        rsi = rsis_arr[i]
        adx = adxs_arr[i]
        ema20 = emas_arr[i]
        vol_sma = vsma_arr[i]
        session_avwap = float(avwap_series.iloc[i]) if i < len(avwap_series) else np.nan

        if not all(np.isfinite(x) for x in (l, h, c, o, atr)) or atr <= 0:
            continue

        atr_pct = (atr / c * 100.0) if np.isfinite(c) and c > 0 else np.nan
        if np.isfinite(atr_pct) and atr_pct < float(cfg.min_atr_pct):
            continue

        swing_high = swing_high_arr[i]
        if not np.isfinite(swing_high):
            continue

        # -----------------------------------------------------------------
        # CORE SIGNAL: sweep above swing high, close-back below
        # -----------------------------------------------------------------
        sweep_confirmed = bool(h > swing_high and c < swing_high)
        if not sweep_confirmed:
            continue

        # -----------------------------------------------------------------
        # AVWAP BIAS: close <= session AVWAP (or within 0.2% tolerance)
        # -----------------------------------------------------------------
        below_avwap = bool(
            np.isfinite(session_avwap) and c <= session_avwap * 1.002
        )
        if cfg.require_avwap_bias and not below_avwap:
            continue

        # -----------------------------------------------------------------
        # AVWAP SLOPE: session AVWAP must be falling for SHORT
        # -----------------------------------------------------------------
        if cfg.require_avwap_slope:
            slope_start = i - cfg.avwap_slope_bars
            if slope_start >= 0 and slope_start < len(avwap_series):
                av_start = float(avwap_series.iloc[slope_start])
                avwap_falling = np.isfinite(av_start) and np.isfinite(session_avwap) and session_avwap < av_start
            else:
                avwap_falling = True  # not enough bars yet, allow
            if not avwap_falling:
                continue

        # -----------------------------------------------------------------
        # CLOSE QUALITY: sweep bar must close in bottom X% of bar (strong rejection)
        # -----------------------------------------------------------------
        if cfg.require_close_quality:
            bar_range = h - l + 1e-9
            close_pct_from_high = (h - c) / bar_range
            if close_pct_from_high < cfg.close_quality_min_pct:
                continue

        # -----------------------------------------------------------------
        # VOLUME PROFILE PROXIMITY
        # -----------------------------------------------------------------
        near_poc = (
            np.isfinite(prior_poc) and atr > 0
            and abs(h - prior_poc) <= cfg.near_level_atr_mult * atr
        )
        near_vah = (
            np.isfinite(prior_vah) and atr > 0
            and abs(h - prior_vah) <= cfg.near_level_atr_mult * atr
        )
        if cfg.require_vp_confluence and not (near_poc or near_vah):
            continue

        # -----------------------------------------------------------------
        # VOLUME SPIKE
        # -----------------------------------------------------------------
        vol_sma_ok = np.isfinite(vol_sma) and vol_sma > 0
        vol_ratio  = float(v / vol_sma) if vol_sma_ok else 1.0
        vol_spike  = vol_sma_ok and vol_ratio >= cfg.sweep_vol_ratio
        vol_extra  = vol_sma_ok and vol_ratio >= 2.0

        if not vol_spike:
            continue

        # -----------------------------------------------------------------
        # ABSORPTION / REJECTION PROXY: big upper wick, small body
        # -----------------------------------------------------------------
        body     = abs(c - o)
        wick_up  = abs(h - max(c, o))
        wick_atr_ratio = wick_up / atr if atr > 0 else 0.0
        absorption_ok = (
            wick_up >= cfg.sweep_min_wick_atr_mult * atr
            and body < 0.60 * (h - l + 1e-9)
        )
        if cfg.require_absorption and not absorption_ok:
            continue

        # -----------------------------------------------------------------
        # TREND FILTERS
        # -----------------------------------------------------------------
        adx_ok = np.isfinite(adx) and adx >= cfg.adx_min
        # For SHORT sweep, RSI should not be oversold (sweep is at resistance)
        rsi_ok = np.isfinite(rsi) and rsi > 35.0
        ema_ok = np.isfinite(ema20) and c <= ema20 * (1.0 + cfg.ema_bias_tolerance_pct)
        if cfg.require_adx_filter and not adx_ok:
            continue
        if cfg.require_rsi_filter and not rsi_ok:
            continue
        if cfg.require_ema_alignment and not ema_ok:
            continue

        # -----------------------------------------------------------------
        # QUALITY SCORE (PDF Section 8, max 12)
        # -----------------------------------------------------------------
        # AVWAP confluent with VP zone: session AVWAP within 1 ATR of POC or VAH
        avwap_vp_confluent = bool(
            np.isfinite(session_avwap) and atr > 0 and (
                (np.isfinite(prior_poc) and abs(session_avwap - prior_poc) <= atr) or
                (np.isfinite(prior_vah) and abs(session_avwap - prior_vah) <= atr)
            )
        )

        # LVN below entry price (within 3 ATR) -- fast-move zone in trade direction
        lvn_below = bool(
            any(
                lvn_p < c and lvn_p >= c - 3.0 * atr
                for lvn_p in prior_lvn_levels
            )
        ) if prior_lvn_levels else False

        # High relative volume on sweep candle (>= 2x SMA = extra conviction)
        high_vol_sweep = vol_extra  # vol_ratio >= 2.0

        # CVD divergence: price rose to sweep high but CVD was falling = sellers distributing
        cvd_divergence = False
        if not df_5m_prep.empty:
            pre_sweep = df_5m_prep[df_5m_prep["date"] <= ts_pd].tail(6)
            if len(pre_sweep) >= 2:
                price_higher = float(pre_sweep["close"].iloc[-1]) >= float(pre_sweep["close"].iloc[0])
                cvd_lower = float(pre_sweep["cvd_proxy"].iloc[-1]) < float(pre_sweep["cvd_proxy"].iloc[0])
                cvd_divergence = bool(price_higher and cvd_lower)

        quality = compute_quality_score_short(
            avwap_bias=below_avwap,
            near_poc=near_poc,
            near_vah=near_vah,
            avwap_vp_confluent=avwap_vp_confluent,
            sweep_confirmed=True,
            absorption_ok=absorption_ok,
            cvd_divergence=cvd_divergence,
            lvn_below=lvn_below,
            high_vol_sweep=high_vol_sweep,
        )

        if quality < cfg.min_quality_score:
            continue

        # -----------------------------------------------------------------
        # ENTRY TIMING (optionally confirm on 5-min, PDF 3TF)
        # -----------------------------------------------------------------
        entry_ts = ts_pd
        entry_price = float(c)
        if cfg.use_5m_confirmation:
            conf = _confirm_entry_short_5m(
                df_5m_day=df_5m_day if df_5m_day is not None else pd.DataFrame(),
                signal_ts=ts_pd,
                sweep_level=float(swing_high),
                cfg=cfg,
            )
            if conf is None and cfg.require_5m_confirmation:
                continue
            if conf is not None:
                entry_ts, entry_price, _ = conf

        # -----------------------------------------------------------------
        # SL + TARGETS
        # -----------------------------------------------------------------
        sl_price    = float(h) * (1.0 + cfg.sl_buffer_pct)

        # T1: session AVWAP (if below entry), else ATR-based fallback
        t1_price = (
            float(session_avwap)
            if np.isfinite(session_avwap) and session_avwap < entry_price
            else entry_price - 1.5 * atr
        )
        # T2: prior-day POC (if below entry), else ATR-based fallback
        t2_price = (
            float(prior_poc)
            if np.isfinite(prior_poc) and prior_poc < entry_price
            else entry_price - 2.5 * atr
        )
        # T3: prior-day VAL (if below entry), else ATR-based fallback
        t3_price = (
            float(prior_val)
            if np.isfinite(prior_val) and prior_val < entry_price
            else entry_price - 4.0 * atr
        )

        # Composite target for R:R check
        valid_tgts = [
            (t1_price, cfg.t1_pct),
            (t2_price, cfg.t2_pct),
            (t3_price, cfg.t3_pct),
        ]
        valid_tgts = [(p, w) for p, w in valid_tgts if p < entry_price]
        if valid_tgts:
            total_w = sum(w for _, w in valid_tgts)
            target_price = sum(p * w for p, w in valid_tgts) / total_w
        else:
            continue  # no valid targets below entry

        risk   = sl_price - entry_price
        reward = entry_price - target_price
        if risk <= 0 or (reward / risk) < cfg.min_rr:
            continue

        # -----------------------------------------------------------------
        # EXIT SIMULATION
        # -----------------------------------------------------------------
        exit_ts, exit_price, outcome = simulate_exit_short(
            df_day=df_day,
            entry_idx=i,
            entry_ts=entry_ts,
            entry_price=entry_price,
            sl_price=sl_price,
            t1_price=t1_price,
            t2_price=t2_price,
            t3_price=t3_price,
            cfg=cfg,
        )

        pnl_net, pnl_gross = compute_pnl_pct(entry_price, exit_price, "SHORT", cfg)
        trade_date_str = ts_pd.strftime("%Y-%m-%d")

        trades.append(Trade(
            trade_date=trade_date_str,
            ticker=ticker,
            side="SHORT",
            signal_time_ist=ts_pd,
            entry_time_ist=entry_ts,
            entry_price=entry_price,
            sl_price=sl_price,
            t1_price=t1_price,
            t2_price=t2_price,
            t3_price=t3_price,
            target_price=target_price,
            exit_time_ist=exit_ts,
            exit_price=exit_price,
            outcome=outcome,
            pnl_pct=pnl_net,
            pnl_pct_gross=pnl_gross,
            prior_poc=float(prior_poc) if np.isfinite(prior_poc) else 0.0,
            prior_vah=float(prior_vah) if np.isfinite(prior_vah) else 0.0,
            prior_val=float(prior_val) if np.isfinite(prior_val) else 0.0,
            session_avwap=float(session_avwap) if np.isfinite(session_avwap) else 0.0,
            swing_level=float(swing_high),
            quality_score=float(quality),
            adx_signal=float(adx) if np.isfinite(adx) else 0.0,
            rsi_signal=float(rsi) if np.isfinite(rsi) else 0.0,
            volume_ratio=vol_ratio,
            wick_atr_ratio=wick_atr_ratio,
        ))

    return trades


# ===========================================================================
# SCAN ALL DAYS FOR ONE TICKER (SHORT)
# ===========================================================================
def scan_all_days_for_ticker(
    ticker: str,
    df_full: pd.DataFrame,
    cfg: Eqidv5Config,
) -> List[Trade]:
    """
    Iterate over every trading day in df_full.
    For each day D compute prior-day VP, then run scan_one_day_short.

    This is the entry point called by the parallel runner.
    """
    if df_full.empty:
        return []

    df = prepare_indicators(df_full.copy(), cfg)
    df["date"] = _to_ist_series(df["date"])
    df = df.dropna(subset=["date"]).reset_index(drop=True)
    df["_date"] = df["date"].dt.date
    dates = sorted(df["_date"].unique())

    # Optional 5-min data for entry confirmation.
    path_5m = os.path.join(cfg.dir_5m, f"{ticker}{cfg.end_5m}")
    df_5m_full = read_15m_parquet(path_5m, cfg.parquet_engine)
    if not df_5m_full.empty:
        df_5m_full["date"] = _to_ist_series(df_5m_full["date"])
        df_5m_full = df_5m_full.dropna(subset=["date"]).reset_index(drop=True)
        df_5m_full["_date"] = df_5m_full["date"].dt.date

    all_trades: List[Trade] = []

    for idx, trade_date in enumerate(dates):
        df_day = (
            df[df["_date"] == trade_date].copy().reset_index(drop=True)
        )
        df_5m_day = (
            df_5m_full[df_5m_full["_date"] == trade_date].copy().reset_index(drop=True)
            if not df_5m_full.empty
            else pd.DataFrame()
        )

        if len(df_day) < max(cfg.swing_lookback + 3, 10):
            continue

        # Prior-day VP
        if idx == 0:
            prior_poc = prior_vah = prior_val = np.nan
            prior_lvn_levels: List[float] = []
        else:
            prior_date = dates[idx - 1]
            df_prior = df[df["_date"] == prior_date]
            if df_prior.empty:
                prior_poc = prior_vah = prior_val = np.nan
                prior_lvn_levels = []
            else:
                prior_poc, prior_vah, prior_val, _, prior_lvn_levels = compute_volume_profile(
                    df_prior,
                    bins=cfg.vp_bins,
                    value_area_pct=cfg.vp_value_area_pct,
                )

        day_trades = scan_one_day_short(
            ticker=ticker,
            df_day=df_day,
            df_5m_day=df_5m_day,
            prior_poc=prior_poc,
            prior_vah=prior_vah,
            prior_val=prior_val,
            prior_lvn_levels=prior_lvn_levels,
            cfg=cfg,
        )
        all_trades.extend(day_trades)

    return all_trades


# ===========================================================================
# STANDALONE TEST ENTRY POINT
# ===========================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="eqidv5 SHORT strategy -- single ticker test"
    )
    parser.add_argument("ticker", help="Ticker symbol (e.g. RELIANCE)")
    parser.add_argument(
        "--dir15m",
        default="stocks_indicators_15min_eq",
        help="Directory containing 15-min parquet files",
    )
    args = parser.parse_args()

    cfg = default_short_config(dir_15m=args.dir15m)
    ticker = args.ticker.upper()
    path = os.path.join(
        cfg.dir_15m, f"{ticker}_stocks_indicators_15min.parquet"
    )
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        print(f"[ERROR] No data for {ticker} at {path}")
        sys.exit(1)

    print(f"[INFO] Loaded {len(df)} bars for {ticker}")
    trades = scan_all_days_for_ticker(ticker, df, cfg)
    df_out = trades_to_df(trades)
    print(f"[INFO] {len(df_out)} SHORT trades found")
    if not df_out.empty:
        print(df_out[["trade_date", "entry_price", "exit_price", "outcome", "pnl_pct", "quality_score"]].to_string(index=False))
