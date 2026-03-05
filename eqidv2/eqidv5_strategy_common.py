# -*- coding: utf-8 -*-
"""
eqidv5_strategy_common.py -- Shared infrastructure for eqidv5 backtester
=========================================================================

Strategy: Volume Profile + Anchored VWAP + Liquidity Sweeps (3-TF)
  - Daily bias: prior-day Volume Profile (POC/VAH/VAL)
  - 15-min signal: liquidity sweep (wick past swing H/L, close-back)
  - 15-min entry: sweep bar close

Contains:
  - Eqidv5Config  dataclass
  - Trade         dataclass
  - BacktestMetrics dataclass + compute_backtest_metrics / print_metrics
  - IO helpers: read_15m_parquet, list_tickers_15m
  - Indicator computation: ATR14, RSI14, ADX14, EMA
  - compute_session_avwap
  - compute_volume_profile  (POC / VAH / VAL / HVN / LVN)
  - detect_swing_lows / detect_swing_highs
  - prepare_indicators
  - compute_pnl_pct  (with slippage + commission)
  - apply_topn_per_day
  - trades_to_df
"""

from __future__ import annotations

import glob
import math
import os
from dataclasses import asdict, dataclass, field
from datetime import time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------
IST = pytz.timezone("Asia/Kolkata")


def now_ist():
    from datetime import datetime
    return datetime.now(IST)


# ===========================================================================
# CONFIG DATACLASS
# ===========================================================================
@dataclass
class Eqidv5Config:
    """All tuneable parameters for the eqidv5 liquidity-sweep backtester."""

    # Direction
    side: str = "LONG"  # "LONG" or "SHORT"

    # Data paths
    dir_15m: str = "stocks_indicators_15min_eq"
    end_15m: str = "_stocks_indicators_15min.parquet"
    dir_5m: str = "stocks_indicators_5min_eq"
    end_5m: str = "_stocks_indicators_5min.parquet"
    parquet_engine: str = "pyarrow"

    # Session timing (IST)
    session_start: dtime = field(default_factory=lambda: dtime(9, 15, 0))
    session_end: dtime = field(default_factory=lambda: dtime(15, 30, 0))
    entry_start: dtime = field(default_factory=lambda: dtime(9, 45, 0))
    entry_end: dtime = field(default_factory=lambda: dtime(15, 15, 0))

    # Swing detection
    swing_lookback: int = 5  # number of prior bars to look back for swing H/L

    # Volume Profile
    vp_value_area_pct: float = 0.70  # 70% value area
    vp_bins: int = 100              # price-level granularity

    # Sweep detection thresholds
    sweep_min_wick_atr_mult: float = 0.30   # wick >= 0.3x ATR to count as a sweep
    sweep_vol_ratio: float = 1.50           # volume >= 1.5x SMA20 for basic spike
    near_level_atr_mult: float = 1.00       # price within 1 ATR of VP level
    min_atr_pct: float = 0.25               # avoid ultra-low-volatility setups

    # Risk management
    sl_buffer_pct: float = 0.0010   # 0.10% buffer beyond sweep wick
    min_rr: float = 2.0             # minimum reward:risk

    # Target allocation weights (must sum to 1.0)
    t1_pct: float = 0.40   # session AVWAP target
    t2_pct: float = 0.40   # prior-day POC target
    t3_pct: float = 0.20   # VAH (LONG) or VAL (SHORT) target

    # Breakeven
    enable_breakeven: bool = True
    be_trigger_pct: float = 0.0050   # arm BE when price moves 0.5% in our favour
    be_pad_pct: float = 0.0030       # BE exit level = entry + 0.30%

    # Trailing stop (activates after BE trigger)
    enable_trailing_stop: bool = True
    trail_pct: float = 0.0040   # trail 0.40% from best price

    # Slippage + commission
    slippage_pct: float = 0.0005    # 5 bps one-way
    commission_pct: float = 0.0003  # 3 bps round-trip

    # Trend / momentum filters
    adx_min: float = 20.0
    ema_bias_tolerance_pct: float = 0.0015  # 0.15% tolerance around EMA20
    require_avwap_bias: bool = True
    require_vp_confluence: bool = True
    require_absorption: bool = True
    require_adx_filter: bool = True
    require_rsi_filter: bool = True
    require_ema_alignment: bool = True

    # Quality score gate
    min_quality_score: int = 6   # out of 12 max (PDF Section 8: min 6 to trade)

    # Volume filter
    volume_sma_period: int = 20

    # 5-min confirmation (PDF 3TF entry timing)
    use_5m_confirmation: bool = True
    require_5m_confirmation: bool = True
    confirm_window_min: int = 30
    delta_spike_mult: float = 1.3
    cvd_lookback_bars: int = 6
    confirm_min_score: int = 1
    sweep_retest_tolerance_pct: float = 0.0020

    # AVWAP slope filter: require session AVWAP trending in our direction
    require_avwap_slope: bool = False
    avwap_slope_bars: int = 8   # compare AVWAP[i] vs AVWAP[i - N]

    # Close quality: sweep bar close must reclaim strongly into trade direction
    # LONG: close >= low + close_quality_min_pct*(high-low)  i.e. top X% of bar
    # SHORT: close <= high - close_quality_min_pct*(high-low)  i.e. bottom X% of bar
    require_close_quality: bool = False
    close_quality_min_pct: float = 0.55  # close in top/bottom 55% of bar range

    # Time stop controls
    scalp_time_stop_minutes: int = 90

    # Max trades per ticker per day (<=0 means unlimited; no hard cap)
    max_trades_per_ticker_per_day: int = 0

    # Top-N per day pruning (disabled by default to avoid hard daily caps)
    enable_topn_per_day: bool = False
    topn_per_day: int = 0
    # Adaptive quality filter (no fixed cap; threshold per day)
    enable_quality_quantile_filter: bool = False
    quality_quantile_per_day: float = 0.90

    # Output directory
    outputs_dir: Path = field(
        default_factory=lambda: Path("outputs_eqidv5")
    )


# ===========================================================================
# TRADE DATACLASS
# ===========================================================================
@dataclass
class Trade:
    trade_date: str
    ticker: str
    side: str               # "LONG" or "SHORT"
    signal_time_ist: Any    # pd.Timestamp
    entry_time_ist: Any
    entry_price: float
    sl_price: float
    t1_price: float         # session AVWAP target
    t2_price: float         # prior-day POC target
    t3_price: float         # VAH (LONG) / VAL (SHORT) target
    target_price: float     # weighted composite target used for R:R check
    exit_time_ist: Any
    exit_price: float
    outcome: str            # "TARGET" / "SL" / "BE" / "EOD"
    pnl_pct: float          # net (after slippage + commission)
    pnl_pct_gross: float    # before costs

    # Diagnostics
    prior_poc: float = 0.0
    prior_vah: float = 0.0
    prior_val: float = 0.0
    session_avwap: float = 0.0
    swing_level: float = 0.0    # swing low (LONG) or swing high (SHORT) swept
    quality_score: float = 0.0
    adx_signal: float = 0.0
    rsi_signal: float = 0.0
    volume_ratio: float = 0.0
    wick_atr_ratio: float = 0.0  # sweep wick size in ATR units


def trades_to_df(trades: List[Trade]) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()
    return pd.DataFrame([asdict(t) for t in trades])


# ===========================================================================
# IO HELPERS
# ===========================================================================
def _require_pyarrow() -> None:
    try:
        import pyarrow  # noqa: F401
    except Exception as exc:
        raise RuntimeError(
            "Parquet support requires 'pyarrow' (pip install pyarrow)."
        ) from exc


def read_15m_parquet(path: str, engine: str = "pyarrow") -> pd.DataFrame:
    _require_pyarrow()
    if not os.path.exists(path):
        return pd.DataFrame()

    df = pd.read_parquet(path, engine=engine)
    if "date" not in df.columns:
        return pd.DataFrame()

    dt = pd.to_datetime(df["date"], errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt = dt.dt.tz_convert(IST)

    df = df.copy()
    df["date"] = dt
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def list_tickers_15m(dir_15m: str, end_15m: str) -> List[str]:
    pattern = os.path.join(dir_15m, f"*{end_15m}")
    files = glob.glob(pattern)
    out = []
    for f in files:
        base = os.path.basename(f)
        if base.endswith(end_15m):
            out.append(base[: -len(end_15m)].upper())
    return sorted(set(out))


# ===========================================================================
# INDICATOR COMPUTATION
# ===========================================================================
def compute_atr14(df: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(14).mean()


def compute_rsi14(df: pd.DataFrame) -> pd.Series:
    close = pd.to_numeric(df["close"], errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def compute_adx14(df: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean().replace(0, np.nan)

    plus_di = 100.0 * (
        pd.Series(plus_dm, index=df.index).ewm(alpha=1 / 14, adjust=False).mean() / atr
    )
    minus_di = 100.0 * (
        pd.Series(minus_dm, index=df.index).ewm(alpha=1 / 14, adjust=False).mean() / atr
    )
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1 / 14, adjust=False).mean()


def compute_session_avwap(df_day: pd.DataFrame) -> pd.Series:
    """Anchored VWAP from first bar of the session."""
    high = pd.to_numeric(df_day["high"], errors="coerce")
    low = pd.to_numeric(df_day["low"], errors="coerce")
    close = pd.to_numeric(df_day["close"], errors="coerce")
    vol = pd.to_numeric(
        df_day["volume"] if "volume" in df_day.columns else pd.Series(0, index=df_day.index),
        errors="coerce",
    ).fillna(0.0)

    tp = (high + low + close) / 3.0
    pv = tp * vol
    cum_pv = pv.cumsum()
    cum_v = vol.cumsum().replace(0, np.nan)
    return (cum_pv / cum_v).reset_index(drop=True)


def compute_volume_profile(
    df_day: pd.DataFrame,
    bins: int = 100,
    value_area_pct: float = 0.70,
) -> Tuple[float, float, float, List[float], List[float]]:
    """
    Compute Volume Profile for one day's 15-min bars.

    Returns:
        (poc, vah, val, hvn_levels, lvn_levels)
        poc  -- Point of Control: price with highest cumulative volume
        vah  -- Value Area High: upper bound of 70% volume zone
        val  -- Value Area Low:  lower bound of 70% volume zone
        hvn  -- High Volume Nodes (price levels > 1.5x average bin volume)
        lvn  -- Low  Volume Nodes (thin price zones, < 0.3x average bin volume)
    """
    high = pd.to_numeric(df_day["high"], errors="coerce")
    low = pd.to_numeric(df_day["low"], errors="coerce")
    vol = pd.to_numeric(
        df_day["volume"] if "volume" in df_day.columns else pd.Series(0, index=df_day.index),
        errors="coerce",
    ).fillna(0.0)

    if high.isna().all() or vol.sum() == 0:
        return np.nan, np.nan, np.nan, [], []

    price_min = float(low.min())
    price_max = float(high.max())
    if price_min >= price_max:
        mid = (price_min + price_max) / 2.0
        return mid, mid, mid, [], []

    bin_edges = np.linspace(price_min, price_max, bins + 1)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    bin_vols = np.zeros(bins)

    for i in range(len(df_day)):
        h = high.iloc[i]
        l = low.iloc[i]
        v = vol.iloc[i]
        if not (np.isfinite(h) and np.isfinite(l) and np.isfinite(v) and v > 0):
            continue
        bar_bins = np.where((bin_edges[1:] >= l) & (bin_edges[:-1] <= h))[0]
        if len(bar_bins) > 0:
            bin_vols[bar_bins] += v / len(bar_bins)

    # POC
    poc_idx = int(np.argmax(bin_vols))
    poc = float(bin_centers[poc_idx])

    # Value Area (expand outward from POC until we reach value_area_pct of total volume)
    total_vol = float(np.sum(bin_vols))
    target_vol = total_vol * value_area_pct
    va_low_idx = poc_idx
    va_high_idx = poc_idx
    va_vol = float(bin_vols[poc_idx])

    while va_vol < target_vol:
        can_up = va_high_idx + 1 < bins
        can_dn = va_low_idx - 1 >= 0
        if not can_up and not can_dn:
            break
        vol_up = float(bin_vols[va_high_idx + 1]) if can_up else 0.0
        vol_dn = float(bin_vols[va_low_idx - 1]) if can_dn else 0.0
        if vol_up >= vol_dn and can_up:
            va_high_idx += 1
            va_vol += float(bin_vols[va_high_idx])
        else:
            va_low_idx -= 1
            va_vol += float(bin_vols[va_low_idx])

    vah = float(bin_centers[va_high_idx])
    val = float(bin_centers[va_low_idx])

    # HVN / LVN
    active_vols = bin_vols[bin_vols > 0]
    avg_vol = float(np.mean(active_vols)) if len(active_vols) else 0.0
    hvn_levels = [float(bin_centers[i]) for i in range(bins) if bin_vols[i] >= 1.5 * avg_vol]
    lvn_levels = [
        float(bin_centers[i])
        for i in range(bins)
        if 0 < bin_vols[i] < 0.30 * avg_vol
    ]

    return poc, vah, val, hvn_levels, lvn_levels


def prepare_indicators(df: pd.DataFrame, cfg: Eqidv5Config) -> pd.DataFrame:
    """Add ATR15, RSI15, ADX15, EMA20, VOL_SMA20, and day columns."""
    out = df.copy()
    if "date" in out.columns:
        dts = pd.to_datetime(out["date"], errors="coerce")
        if getattr(dts.dt, "tz", None) is None:
            dts = dts.dt.tz_localize(IST)
        else:
            dts = dts.dt.tz_convert(IST)
        out["date"] = dts

    # ATR -- use pre-computed if present
    if "ATR" in out.columns:
        out["ATR15"] = pd.to_numeric(out["ATR"], errors="coerce")
    else:
        out["ATR15"] = compute_atr14(out)

    # RSI
    if "RSI" in out.columns:
        out["RSI15"] = pd.to_numeric(out["RSI"], errors="coerce")
    else:
        out["RSI15"] = compute_rsi14(out)

    # ADX
    if "ADX" in out.columns:
        out["ADX15"] = pd.to_numeric(out["ADX"], errors="coerce")
    else:
        out["ADX15"] = compute_adx14(out)

    # EMA20
    if "EMA_20" in out.columns:
        out["EMA20"] = pd.to_numeric(out["EMA_20"], errors="coerce")
    else:
        close = pd.to_numeric(out["close"], errors="coerce")
        out["EMA20"] = close.ewm(span=20, adjust=False).mean()

    # Volume SMA
    if "volume" in out.columns:
        vol = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
        out["VOL_SMA20"] = vol.rolling(
            cfg.volume_sma_period, min_periods=1
        ).mean()
    else:
        out["VOL_SMA20"] = np.nan

    out["day"] = out["date"].dt.date
    return out


# ===========================================================================
# SWING HIGH / LOW DETECTION
# ===========================================================================
def detect_swing_lows(prices_low: np.ndarray, lookback: int = 5) -> np.ndarray:
    """
    For each bar i, return the minimum low of the PRIOR `lookback` bars
    (not including bar i itself).  This is the "swing low" that could be swept.
    """
    n = len(prices_low)
    swing_lows = np.full(n, np.nan)
    for i in range(lookback, n):
        window = prices_low[max(0, i - lookback): i]
        if len(window) > 0:
            swing_lows[i] = float(np.nanmin(window))
    return swing_lows


def detect_swing_highs(prices_high: np.ndarray, lookback: int = 5) -> np.ndarray:
    """
    For each bar i, return the maximum high of the PRIOR `lookback` bars
    (not including bar i itself).  This is the "swing high" that could be swept.
    """
    n = len(prices_high)
    swing_highs = np.full(n, np.nan)
    for i in range(lookback, n):
        window = prices_high[max(0, i - lookback): i]
        if len(window) > 0:
            swing_highs[i] = float(np.nanmax(window))
    return swing_highs


# ===========================================================================
# SLIPPAGE + COMMISSION
# ===========================================================================
def compute_pnl_pct(
    entry_price: float,
    exit_price: float,
    side: str,
    cfg: Eqidv5Config,
) -> Tuple[float, float]:
    """
    Returns (net_pnl_pct, gross_pnl_pct).
    Net  = price return % minus round-trip slippage + commission.
    Gross = raw price return %.
    """
    if side == "LONG":
        gross = (exit_price - entry_price) / entry_price * 100.0
    else:
        gross = (entry_price - exit_price) / entry_price * 100.0

    # Round-trip cost: slippage * 2 (buy + sell) + commission
    cost_pct = (cfg.slippage_pct * 2.0 + cfg.commission_pct) * 100.0
    net = gross - cost_pct
    return float(net), float(gross)


# ===========================================================================
# BACKTEST METRICS
# ===========================================================================
@dataclass
class BacktestMetrics:
    total_trades: int = 0
    unique_days: int = 0
    target_count: int = 0
    sl_count: int = 0
    be_count: int = 0
    eod_count: int = 0
    hit_rate_pct: float = 0.0
    sl_rate_pct: float = 0.0
    be_rate_pct: float = 0.0
    eod_rate_pct: float = 0.0
    avg_pnl_pct: float = 0.0
    sum_pnl_pct: float = 0.0
    avg_pnl_pct_gross: float = 0.0
    sum_pnl_pct_gross: float = 0.0
    profit_factor: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    avg_quality_score: float = 0.0
    avg_volume_ratio: float = 0.0
    avg_rr_achieved: float = 0.0


def compute_backtest_metrics(df: pd.DataFrame) -> BacktestMetrics:
    m = BacktestMetrics()
    if df.empty:
        return m

    d = df.copy()
    d["pnl_pct"] = pd.to_numeric(d["pnl_pct"], errors="coerce").fillna(0.0)
    d["pnl_pct_gross"] = pd.to_numeric(
        d.get("pnl_pct_gross", d["pnl_pct"]), errors="coerce"
    ).fillna(0.0)

    n = len(d)
    m.total_trades = n
    m.unique_days = int(d["trade_date"].nunique()) if "trade_date" in d.columns else 0

    if "outcome" in d.columns:
        m.target_count = int((d["outcome"] == "TARGET").sum())
        m.sl_count = int((d["outcome"] == "SL").sum())
        m.be_count = int((d["outcome"] == "BE").sum())
        m.eod_count = int((d["outcome"] == "EOD").sum())

    m.hit_rate_pct = (m.target_count / n * 100.0) if n else 0.0
    m.sl_rate_pct = (m.sl_count / n * 100.0) if n else 0.0
    m.be_rate_pct = (m.be_count / n * 100.0) if n else 0.0
    m.eod_rate_pct = (m.eod_count / n * 100.0) if n else 0.0

    pnl = d["pnl_pct"].values
    m.avg_pnl_pct = float(np.nanmean(pnl))
    m.sum_pnl_pct = float(np.nansum(pnl))
    m.avg_pnl_pct_gross = float(np.nanmean(d["pnl_pct_gross"].values))
    m.sum_pnl_pct_gross = float(np.nansum(d["pnl_pct_gross"].values))

    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    gross_profit = float(np.sum(wins)) if len(wins) else 0.0
    gross_loss = float(np.abs(np.sum(losses))) if len(losses) else 0.0
    m.profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")
    m.avg_win_pct = float(np.mean(wins)) if len(wins) else 0.0
    m.avg_loss_pct = float(np.mean(losses)) if len(losses) else 0.0

    cum = np.cumsum(pnl)
    running_max = np.maximum.accumulate(cum)
    dd = running_max - cum
    m.max_drawdown_pct = float(np.max(dd)) if len(dd) else 0.0

    if n > 1:
        daily_mean = float(np.mean(pnl))
        daily_std = float(np.std(pnl, ddof=1))
        m.sharpe_ratio = (
            (daily_mean / daily_std) * math.sqrt(250) if daily_std > 0 else 0.0
        )
        downside = pnl[pnl < 0]
        dn_std = float(np.std(downside, ddof=1)) if len(downside) > 1 else 0.0
        m.sortino_ratio = (
            (daily_mean / dn_std) * math.sqrt(250) if dn_std > 0 else 0.0
        )

    m.calmar_ratio = (
        float(m.sum_pnl_pct / m.max_drawdown_pct)
        if m.max_drawdown_pct > 0
        else (float("inf") if m.sum_pnl_pct > 0 else 0.0)
    )

    m.max_consecutive_wins = _max_consecutive(pnl > 0)
    m.max_consecutive_losses = _max_consecutive(pnl < 0)

    if "quality_score" in d.columns:
        m.avg_quality_score = float(
            pd.to_numeric(d["quality_score"], errors="coerce").mean()
        )
    if "volume_ratio" in d.columns:
        m.avg_volume_ratio = float(
            pd.to_numeric(d["volume_ratio"], errors="coerce").mean()
        )

    return m


def _max_consecutive(mask: np.ndarray) -> int:
    best = run = 0
    for v in mask:
        if v:
            run += 1
            best = max(best, run)
        else:
            run = 0
    return best


def print_metrics(title: str, m: BacktestMetrics) -> None:
    sep = "=" * (42 + len(title))
    print(f"\n{'=' * 20} {title} {'=' * 20}")
    print(f"Total trades                  : {m.total_trades}")
    print(f"Unique trade days             : {m.unique_days}")
    print(f"TARGET hits                   : {m.target_count}  | hit-rate  = {m.hit_rate_pct:.2f}%")
    print(f"SL hits                       : {m.sl_count}  | sl-rate   = {m.sl_rate_pct:.2f}%")
    print(f"BE exits                      : {m.be_count}  | be-rate   = {m.be_rate_pct:.2f}%")
    print(f"EOD exits                     : {m.eod_count}  | eod-rate  = {m.eod_rate_pct:.2f}%")
    print(f"Avg PnL % (net, per trade)    : {m.avg_pnl_pct:.4f}%")
    print(f"Sum PnL % (net, all trades)   : {m.sum_pnl_pct:.4f}%")
    print(f"Avg PnL % (gross, per trade)  : {m.avg_pnl_pct_gross:.4f}%")
    print(f"Sum PnL % (gross, all trades) : {m.sum_pnl_pct_gross:.4f}%")
    print(f"Profit factor                 : {m.profit_factor:.3f}")
    print(f"Avg winning trade             : {m.avg_win_pct:.4f}%")
    print(f"Avg losing trade              : {m.avg_loss_pct:.4f}%")
    print(f"Max drawdown (cumul PnL %)    : {m.max_drawdown_pct:.4f}%")
    print(f"Sharpe ratio (annualized)     : {m.sharpe_ratio:.3f}")
    print(f"Sortino ratio (annualized)    : {m.sortino_ratio:.3f}")
    print(f"Calmar ratio                  : {m.calmar_ratio:.3f}")
    print(f"Max consecutive wins          : {m.max_consecutive_wins}")
    print(f"Max consecutive losses        : {m.max_consecutive_losses}")
    print(f"Avg quality score             : {m.avg_quality_score:.2f}/12")
    print(f"Avg volume ratio              : {m.avg_volume_ratio:.2f}x SMA20")
    print(sep)


# ===========================================================================
# TOP-N PER DAY FILTER
# ===========================================================================
def apply_topn_per_day(df: pd.DataFrame, cfg: Eqidv5Config) -> pd.DataFrame:
    if df.empty:
        return df

    req = {"trade_date", "quality_score"}
    if not req.issubset(df.columns):
        return df

    d = df.copy()
    d["quality_score"] = pd.to_numeric(d["quality_score"], errors="coerce").fillna(0.0)

    for c in ["entry_time_ist", "signal_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")

    d = d.sort_values(
        ["trade_date", "quality_score", "ticker"],
        ascending=[True, False, True],
    )

    if cfg.enable_quality_quantile_filter:
        q = float(cfg.quality_quantile_per_day)
        q = min(max(q, 0.0), 1.0)
        if q > 0.0:
            thr = (
                d.groupby("trade_date", sort=False)["quality_score"]
                 .transform(lambda s: s.quantile(q))
            )
            d = d[d["quality_score"] >= thr].reset_index(drop=True)

    if not cfg.enable_topn_per_day or cfg.topn_per_day <= 0:
        return d

    d = (
        d.groupby("trade_date", sort=False, as_index=False)
        .head(int(cfg.topn_per_day))
        .reset_index(drop=True)
    )
    return d


# ===========================================================================
# DEFAULT CONFIGS
# ===========================================================================
def default_long_config(**overrides) -> Eqidv5Config:
    """LONG side defaults for the eqidv5 liquidity-sweep strategy."""
    base = dict(
        side="LONG",
        swing_lookback=5,
        entry_start=dtime(9, 50, 0),
        entry_end=dtime(15, 0, 0),
        sweep_min_wick_atr_mult=0.35,
        sweep_vol_ratio=2.20,
        near_level_atr_mult=1.30,
        min_atr_pct=0.20,
        sl_buffer_pct=0.0010,
        min_rr=1.80,
        be_trigger_pct=0.0040,
        be_pad_pct=0.0015,
        trail_pct=0.0035,
        adx_min=25.0,
        min_quality_score=6,  # PDF Section 8: min 6 to trade
        require_avwap_bias=True,
        require_vp_confluence=True,
        require_absorption=True,
        require_adx_filter=True,
        require_rsi_filter=True,
        require_ema_alignment=True,
        use_5m_confirmation=True,
        require_5m_confirmation=True,
        confirm_window_min=30,
        delta_spike_mult=1.3,
        cvd_lookback_bars=6,
        confirm_min_score=1,
        sweep_retest_tolerance_pct=0.0020,
        scalp_time_stop_minutes=90,
        max_trades_per_ticker_per_day=0,
        enable_topn_per_day=False,
        topn_per_day=0,
        enable_quality_quantile_filter=False,
        quality_quantile_per_day=0.90,
    )
    base.update(overrides)
    return Eqidv5Config(**base)


def default_short_config(**overrides) -> Eqidv5Config:
    """SHORT side defaults for the eqidv5 liquidity-sweep strategy."""
    base = dict(
        side="SHORT",
        swing_lookback=5,
        entry_start=dtime(9, 50, 0),
        entry_end=dtime(15, 0, 0),
        sweep_min_wick_atr_mult=0.35,
        sweep_vol_ratio=2.20,
        near_level_atr_mult=1.30,
        min_atr_pct=0.20,
        sl_buffer_pct=0.0010,
        min_rr=1.80,
        be_trigger_pct=0.0040,
        be_pad_pct=0.0015,
        trail_pct=0.0035,
        adx_min=25.0,
        min_quality_score=6,  # PDF Section 8: min 6 to trade
        require_avwap_bias=True,
        require_vp_confluence=True,
        require_absorption=True,
        require_adx_filter=True,
        require_rsi_filter=True,
        require_ema_alignment=True,
        use_5m_confirmation=True,
        require_5m_confirmation=True,
        confirm_window_min=30,
        delta_spike_mult=1.3,
        cvd_lookback_bars=6,
        confirm_min_score=1,
        sweep_retest_tolerance_pct=0.0020,
        scalp_time_stop_minutes=90,
        max_trades_per_ticker_per_day=0,
        enable_topn_per_day=False,
        topn_per_day=0,
        enable_quality_quantile_filter=False,
        quality_quantile_per_day=0.90,
    )
    base.update(overrides)
    return Eqidv5Config(**base)
