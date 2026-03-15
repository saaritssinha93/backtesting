# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v15.py - AVWAP v15 COMBINED runner: V11 SHORT + V9 LONG (hybrid)
==================================================================================

Changes from v14:
1. NIFTY_CONTEXT_OR_END_TIME: 10:15 -> 9:45  (30-min standard Indian market OR)
2. NIFTY_CONTEXT_CONFIRM_TIME: 10:30 -> 10:00 (45-min session -- gap fills largely done)
3. NIFTY_CONTEXT_MIN_DAYMOVE_PCT: 0.20 -> 0.35 (filter noise-level NIFTY moves)
4. NIFTY_RS_LOOKBACK_BARS: 3 -> 4             (60-min RS window, more stable signal)
5. NIFTY_RS_THRESHOLD_PCT: 0.15 -> 0.20       (above round-trip cost, genuine RS edge)
6. NIFTY_RS_BOTH_MODE_ENABLED = True (NEW)    (apply relaxed RS filter in BOTH mode too)
7. NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = 0.08    (half the directional threshold)
   Rationale: BOTH mode (previously blind/unfiltered) now requires stock to
   outperform/underperform NIFTY by >=0.08% -- removes weakest counter-trend signals
   without aggressively cutting neutral-day trade volume.

Earlier changes inherited from v14:
1. All outputs saved to outputs_v15/
2. Entry signals: 15-min data; exits: 1-min data (stocks_indicators_1min_eq)
3. Expanded charting suite
4. Normal Python imports (no importlib hacks)
5. Unified Trade dataclass -- both sides produce identical columns
6. Parallel ticker scanning via ProcessPoolExecutor
7. Slippage + commission model baked into P&L
8. Comprehensive backtest metrics (Sharpe, Sortino, Calmar, drawdown, profit factor)
9. All config via StrategyConfig dataclass -- no module-level globals
10. Cash-constrained portfolio sim uses itertuples() instead of iterrows()

Usage:
    python -m avwap_v11_refactored.avwap_combined_runner
    # or
    python avwap_v11_refactored/avwap_combined_runner.py
"""

from __future__ import annotations

import heapq
import os
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd

# ===========================================================================
# CONSOLE OUTPUT TEE (stdout/stderr -> console + outputs/*.txt)
# ===========================================================================
class _Tee:
    """Write to multiple streams (e.g., console + log file)."""

    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]

    def write(self, data):
        for s in self.streams:
            try:
                s.write(data)
            except Exception:
                pass

    def flush(self):
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass

    def isatty(self):
        return False

# Ensure the package is importable when running this file directly
_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from avwap_v11_refactored.avwap_common_v11_v15 import (
    IST,
    StrategyConfig,
    Trade,
    BacktestMetrics,
    default_short_config,
    default_long_config,
    now_ist,
    trades_to_df,
    apply_topn_per_day,
    compute_backtest_metrics,
    print_metrics,
    read_15m_parquet,
    list_tickers_15m,
    generate_backtest_charts,
    build_market_regime_map,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker as scan_short,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker as scan_long,
)
from avwap_v11_refactored.avwap_common_v7_sweep_v15 import (
    default_long_config as default_long_config_v9,
)


# ===========================================================================
# RUNNER CONFIG (top-level orchestration settings)
# ===========================================================================
POSITION_SIZE_RS_SHORT = 50_000
POSITION_SIZE_RS_LONG = 50_000

# Intraday leverage (margin). Position sizes above are *capital/margin per trade*.
# Notional exposure = capital * leverage. Set leverage=1.0 to disable leverage effects.
INTRADAY_LEVERAGE_SHORT = 5.0
INTRADAY_LEVERAGE_LONG = 5.0

ENABLE_CASH_CONSTRAINED_PORTFOLIO_SIM = False

# If True, force min_bars_left_after_entry=0 for BOTH sides (live-signal parity).
# This makes entry counts comparable to eqidv2_* live/daily scanners.
FORCE_LIVE_PARITY_MIN_BARS_LEFT = True

# If True, disable Top-N pruning on both sides so runner output does not
# unintentionally suppress one side on a given day versus live/daily scanners.
FORCE_LIVE_PARITY_DISABLE_TOPN = True

'''
# Final signal-window override (applied last in main()).
# Edit these windows here to override defaults from avwap_common/default_*_config.
FINAL_SIGNAL_WINDOW_OVERRIDE = True
FINAL_SHORT_USE_TIME_WINDOWS = True
FINAL_SHORT_SIGNAL_WINDOWS = [
    (dtime(9, 30, 0), dtime(12, 0, 0)),
    (dtime(13, 30, 0), dtime(15, 15, 0)),
]
FINAL_LONG_USE_TIME_WINDOWS = True
FINAL_LONG_SIGNAL_WINDOWS = [
    (dtime(9, 30, 0), dtime(12, 0, 0)),
    (dtime(13, 30, 0), dtime(15, 15, 0)),
]

# Final signal-window override (applied last in main()).
# Edit these windows here to override defaults from avwap_common/default_*_config.
FINAL_SIGNAL_WINDOW_OVERRIDE = True
FINAL_SHORT_USE_TIME_WINDOWS = True
FINAL_SHORT_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(14, 00, 0))
]
FINAL_LONG_USE_TIME_WINDOWS = True
FINAL_LONG_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(14, 00, 0))
]
'''

# Final signal-window override (applied last in main()).
# Edit these windows here to override defaults from avwap_common/default_*_config.
FINAL_SIGNAL_WINDOW_OVERRIDE = True
FINAL_SHORT_USE_TIME_WINDOWS = True
FINAL_SHORT_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(15, 0, 0))
]
FINAL_LONG_USE_TIME_WINDOWS = True
FINAL_LONG_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(14, 30, 0))
]
V15_EOD_EXIT_TIME = dtime(15, 20, 0)

# Per-setup signal->entry lag (in 15-min bars).
# Edit these to manually control (entry_time_ist - signal_time_ist) behavior.
# HUGE setup: use -1 for legacy dynamic "first valid bar" behavior.
SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW = 1
SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW = 2
SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE = -1
LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH = 1
LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH = 2
# Set high to effectively disable the weak HUGE pullback-hold long setup in V14.
LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK = 999

PORTFOLIO_START_CAPITAL_RS = 1_000_000
DISALLOW_BOTH_SIDES_SAME_TICKER_DAY = False

# Parallelism: set to 1 for serial, >1 for multi-process
MAX_WORKERS = 4

# Setup controls
# Disable the weak short HUGE failed-bounce branch in V14.
PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE = False
PACK2_SHORT_MAX_VIX_FOR_ENTRIES = 0.0
PACK2_LONG_MAX_VIX_FOR_ENTRIES = 0.0

# V12: V11 playbook SHORT + V9 s5 LONG (hybrid)
ENABLE_PLAYBOOK_V11_PROFILE = True   # controls SHORT side only in V12
ENABLE_V9_LONG_PROFILE = True        # controls LONG side: V9 s5 + close-confirm gate

# NIFTY intraday context + relative-strength filter (V14)
NIFTY_CONTEXT_ENABLED = True
NIFTY_CONTEXT_TICKERS: Tuple[str, ...] = (
    "NIFTYBEES",
    "NIFTY50",
    "NIFTY_50",
    "NIFTY",
)
NIFTY_CONTEXT_OR_END_TIME = dtime(9, 45, 0)    # v15: 30-min OR (was 10:15)
NIFTY_CONTEXT_CONFIRM_TIME = dtime(10, 0, 0)   # v15: confirm at 10:00 (was 10:30)
NIFTY_CONTEXT_MIN_DAYMOVE_PCT = 0.35           # v15: raised from 0.20 to filter noise
NIFTY_RS_FILTER_ENABLED = True
NIFTY_RS_LOOKBACK_BARS = 4                     # v15: 60-min window (was 3 bars/45min)
NIFTY_RS_THRESHOLD_PCT = 0.20                  # v15: raised from 0.15 (above round-trip cost)
# v15 NEW: apply a relaxed RS filter even in BOTH mode (previously blind/unfiltered)
NIFTY_RS_BOTH_MODE_ENABLED = True
NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = 0.08        # half the directional threshold

# ===========================================================================
# TARGET TEST — disabled in V12 (each side uses its own calibrated targets)
# ===========================================================================
TEST_TARGET_OVERRIDE   = True
TEST_SHORT_TARGET_PCT  = 0.01100
TEST_LONG_TARGET_PCT   = 0.01100


def apply_live_parity_profile(
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
) -> Tuple[StrategyConfig, StrategyConfig]:
    """
    Apply the tuned v15 runner profile to existing configs.

    Live wrappers call into the shared live parity builder, which starts from the
    default strategy configs. This helper ports the same v15 tuning block used in
    the backtest runner so live signal generation stays aligned with v15.
    """
    if ENABLE_PLAYBOOK_V11_PROFILE:
        short_cfg.enable_liquidity_sweep_filter = False
        short_cfg.reversal_requires_sweep = True
        short_cfg.enable_avwap_no_trade_zone = False
        short_cfg.enable_mode_selector = True
        short_cfg.use_time_windows = False
        short_cfg.min_bars_left_after_entry = 0
        short_cfg.enable_ema200_filter = False
        short_cfg.require_vwap_side_persistence = False
        short_cfg.vwap_side_lookback_bars = 5
        short_cfg.vwap_side_min_count = 3
        short_cfg.require_structure_filter = False
        short_cfg.structure_lookback_bars = 30
        short_cfg.adx_min = 17.0
        short_cfg.adx_slope_min = 0.40
        short_cfg.volume_min_ratio = 0.95
        short_cfg.rsi_max_short = 62.0
        short_cfg.stochk_max = 90.0
        short_cfg.stop_pct = 0.0084
        short_cfg.target_pct = 0.01100
        short_cfg.be_trigger_pct = 0.0042
        short_cfg.trail_pct = 0.0023
        short_cfg.enable_partial_exit = True
        short_cfg.partial_exit_fraction = 0.50
        short_cfg.partial_target_fraction = 0.50
        short_cfg.enable_risk_based_position_sizing = False
        short_cfg.risk_per_trade_pct_of_capital = 0.0035
        short_cfg.max_trades_per_ticker_per_day = 5
        short_cfg.enable_topn_per_day = False
        short_cfg.topn_per_day = 0

        long_cfg.require_entry_close_confirm = True
        long_cfg.enable_liquidity_sweep_filter = False
        long_cfg.enable_avwap_no_trade_zone = False
        long_cfg.adx_min = 17.0
        long_cfg.adx_slope_min = 0.50
        long_cfg.volume_min_ratio = 0.95
        long_cfg.rsi_min_long = 38.0
        long_cfg.stochk_min = 15.0
        long_cfg.stochk_max = 95.0
        long_cfg.atr_pct_min = 0.0025
        long_cfg.enable_setup_a_close_continuation_break = False
        long_cfg.enable_setup_b_huge_c1_close_reclaim_break = True
        long_cfg.stop_pct = 0.0075
        long_cfg.target_pct = 0.0110
        long_cfg.be_trigger_pct = 0.0055
        long_cfg.trail_pct = 0.0028
        long_cfg.min_bars_left_after_entry = 0
        long_cfg.max_vix_for_entries = 13.0
        long_cfg.max_trades_per_ticker_per_day = 4
        long_cfg.enable_topn_per_day = False
        long_cfg.topn_per_day = 0

    short_cfg.lag_bars_short_a_mod_break_c1_low = int(SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW)
    short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = int(
        SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW
    )
    short_cfg.lag_bars_short_b_huge_failed_bounce = int(SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE)
    short_cfg.enable_setup_b_huge_failed_bounce = bool(
        PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE
    )
    short_cfg.max_vix_for_entries = float(PACK2_SHORT_MAX_VIX_FOR_ENTRIES)
    long_cfg.max_vix_for_entries = float(PACK2_LONG_MAX_VIX_FOR_ENTRIES)
    long_cfg.lag_bars_long_a_mod_break_c1_high = int(LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH)
    long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(
        LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH
    )
    long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(
        LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK
    )

    if FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        short_cfg.min_bars_left_after_entry = 0
        long_cfg.min_bars_left_after_entry = 0

    if FORCE_LIVE_PARITY_DISABLE_TOPN:
        short_cfg.enable_topn_per_day = False
        long_cfg.enable_topn_per_day = False

    if FINAL_SIGNAL_WINDOW_OVERRIDE:
        short_cfg.use_time_windows = bool(FINAL_SHORT_USE_TIME_WINDOWS)
        long_cfg.use_time_windows = bool(FINAL_LONG_USE_TIME_WINDOWS)
        short_cfg.signal_windows = list(FINAL_SHORT_SIGNAL_WINDOWS)
        long_cfg.signal_windows = list(FINAL_LONG_SIGNAL_WINDOWS)

    if TEST_TARGET_OVERRIDE:
        short_cfg.target_pct = TEST_SHORT_TARGET_PCT
        long_cfg.target_pct = TEST_LONG_TARGET_PCT

    short_cfg.market_regime_tickers = tuple(NIFTY_CONTEXT_TICKERS)
    long_cfg.market_regime_tickers = tuple(NIFTY_CONTEXT_TICKERS)

    return short_cfg, long_cfg

# ===========================================================================
# VIX DYNAMIC SCALING — set VIX_SCALE_ENABLED=True to scale SL/target with VIX
# Set VIX_SCALE_ENABLED=False to use the old fixed stop_pct / target_pct (no VIX).
# Requires india_vix.parquet in the project root (run fetch_india_vix.py once).
#
# Calibrated from data analysis on 132 trading days (Aug 2025 - Mar 2026):
#   Actual VIX range = 9.15 - 21.14  |  Median = 11.41  |  Mean = 11.61
#   Best zone: VIX 12.0-12.7 ? Win=66.7%, AvgPnL=+1.26%  (scale 1.04-1.12x)
#   Worst zone: VIX 11.1-11.9 ? Win=57.9%, AvgPnL=+0.74% (scale ~1.0x, dead zone)
#   Low VIX:  9.2-10.1 ? Win=64.2%, AvgPnL=+1.19%       (scale 0.80-0.86x, tighter targets)
#
# Scale formula: clamp(india_vix / VIX_BASELINE, VIX_SCALE_MIN, VIX_SCALE_MAX)
# With baseline=11.5: 36% of days scale DOWN, 31% neutral, 33% scale UP
# ===========================================================================
VIX_SCALE_ENABLED = False    # True = dynamic VIX scaling; False = old fixed behaviour
VIX_BASELINE      = 11.5   # Actual median VIX of this dataset — neutral point
VIX_SCALE_MIN     = 0.75   # Floor: VIX=8.6 ? 0.75x  (SHORT TGT 1.2%?0.90%, SL 0.8%?0.60%)
VIX_SCALE_MAX     = 1.50   # Cap:  VIX=17.25 ? 1.50x (SHORT TGT 1.2%?1.80%, SL 0.8%?1.20%)
VIX_SCALE_TARGET  = True   # scale target_pct
VIX_SCALE_SL      = True   # scale stop_pct (keeps R:R ratio constant)

# 1-min data directory for exit resolution
DIR_5MIN = None  # Will be resolved dynamically at runtime

# Exit realism controls (v15 only).
# Set EXIT_REALISM_BAND_ENABLED=False to disable ambiguity/stress analysis
# entirely and keep the legacy 1-min SL-first resolver only.
EXIT_REALISM_BAND_ENABLED = True
# If True, promote the pessimistic stressed path to the main exported result.
# If False, keep the legacy exact 1-min result as the main output and print the
# stressed pessimistic/optimistic bands separately.
EXIT_REALISM_USE_STRESSED_BASE = True
STOP_EXIT_EXTRA_SLIPPAGE_BPS = 3.0


# ===========================================================================
# DATA DIRECTORY RESOLUTION
# ===========================================================================
def _resolve_15m_dir() -> Path:
    """
    Resolve the 15-min parquet directory across the repo layouts used here.

    Prefer the first directory that actually contains matching parquet files.
    This avoids accidentally binding to an empty sibling folder when the
    populated dataset lives under `eqidv2/stocks_indicators_15min_eq`.
    """
    _script_dir = Path(__file__).resolve().parent
    if _script_dir.name == "avwap_v11_refactored":
        _proj = _script_dir.parent
    else:
        _proj = _script_dir

    candidates = [
        _proj / "stocks_indicators_15min_eq",
        _proj.parent / "stocks_indicators_15min_eq",
        Path.cwd() / "stocks_indicators_15min_eq",
    ]

    ranked: List[Tuple[int, Path]] = []
    seen: set[str] = set()
    for cand in candidates:
        cand_abs = cand.resolve()
        key = str(cand_abs).lower()
        if key in seen:
            continue
        seen.add(key)
        file_count = 0
        if cand_abs.is_dir():
            file_count = sum(1 for _ in cand_abs.glob("*_stocks_indicators_15min.parquet"))
        ranked.append((file_count, cand_abs))

    if not ranked:
        return candidates[0].resolve()

    best_count, best_path = max(ranked, key=lambda item: (item[0], -len(str(item[1]))))
    if best_count > 0:
        return best_path

    for _, cand_abs in ranked:
        if cand_abs.is_dir():
            return cand_abs

    return ranked[0][1]


def _fmt_pct_exact(value: float) -> str:
    """Format tuned percentage values without hiding precision via rounding."""
    txt = f"{value * 100:.3f}".rstrip("0").rstrip(".")
    return f"{txt}%"


def _describe_regime_source_availability(cfg: StrategyConfig) -> Tuple[List[str], List[str]]:
    """Return (found_tickers, missing_tickers) for configured regime parquet aliases."""
    found: List[str] = []
    missing: List[str] = []
    base_dir = Path(cfg.dir_15m)
    for ticker in tuple(cfg.market_regime_tickers or ()):
        p = base_dir / f"{ticker}{cfg.end_15m}"
        if p.exists():
            found.append(str(ticker))
        else:
            missing.append(str(ticker))
    return found, missing


# ===========================================================================
# 1-MINUTE DATA READER
# ===========================================================================
def _resolve_5min_dir() -> Path:
    """
    Resolve the 1-min data directory relative to the algo_trading project root.
    Looks for a directory named 'stocks_indicators_1min_eq' in the data path.
    """
    _script_dir = Path(__file__).resolve().parent
    if _script_dir.name == "avwap_v11_refactored":
        _proj = _script_dir.parent
    else:
        _proj = _script_dir

    # Common locations to search for 1-min data
    candidates = [
        _proj / "data" / "stocks_indicators_1min_eq",
        _proj / "stocks_indicators_1min_eq",
        _proj.parent / "data" / "stocks_indicators_1min_eq",
        _proj.parent / "stocks_indicators_1min_eq",
    ]
    for c in candidates:
        if c.is_dir():
            return c
    # Fallback: return the first candidate (will be created/handled later)
    return candidates[0]


def _load_india_vix(project_root: Path) -> dict:
    """Load India VIX parquet â†’ {date_str: float}.

    Returns empty dict if VIX_SCALE_ENABLED=False or file not found.
    Run fetch_india_vix.py once to create india_vix.parquet.
    """
    if not VIX_SCALE_ENABLED:
        return {}
    vix_path = project_root / "india_vix.parquet"
    if not vix_path.exists():
        print("[WARN] india_vix.parquet not found â€” VIX scaling disabled.")
        print("       Run 'python fetch_india_vix.py' to generate it.")
        return {}
    df = pd.read_parquet(vix_path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    vix_map = dict(zip(df["date"], df["india_vix"].astype(float)))
    return vix_map


def read_5m_parquet(path: str, engine: str = "pyarrow") -> pd.DataFrame:
    """
    Read a 5-minute parquet file for a ticker.
    Returns empty DataFrame if file not found or read fails.
    """
    try:
        p = Path(path)
        if not p.exists():
            return pd.DataFrame()
        df = pd.read_parquet(p, engine=engine)
        if df.empty:
            return df
        # Normalize common datetime column/index variants to a canonical "datetime".
        dt_col = None
        for cand in ("datetime", "date", "DateTime", "timestamp", "Timestamp"):
            if cand in df.columns:
                dt_col = cand
                break

        if dt_col is None and (df.index.name == "datetime" or isinstance(df.index, pd.DatetimeIndex)):
            df = df.reset_index()
            dt_col = "datetime" if "datetime" in df.columns else None

        if dt_col is not None and dt_col != "datetime":
            df = df.rename(columns={dt_col: "datetime"})

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def list_tickers_5m(dir_5m: Path, suffix: str = ".parquet") -> List[str]:
    """List ticker symbols available in the 1-min data directory."""
    if not dir_5m.is_dir():
        return []
    tickers = []
    for f in dir_5m.iterdir():
        if f.name.endswith(suffix):
            tickers.append(f.stem if not suffix else f.name.replace(suffix, ""))
    return sorted(set(tickers))


def _calc_price_return_pct(side: str, entry_price: float, exit_price: float) -> float:
    if not np.isfinite(entry_price) or not np.isfinite(exit_price) or entry_price == 0:
        return 0.0
    if str(side).upper() == "SHORT":
        return (entry_price - exit_price) / entry_price * 100.0
    return (exit_price - entry_price) / entry_price * 100.0


def _apply_stop_exit_slippage(side: str, stop_price: float) -> float:
    """
    Stress stop fills by a small adverse move to approximate intraminute gap,
    latency, and queue-position risk that 1-min OHLC cannot see.
    """
    if not np.isfinite(stop_price):
        return stop_price
    slip = float(STOP_EXIT_EXTRA_SLIPPAGE_BPS) / 10000.0
    if slip <= 0:
        return stop_price
    if str(side).upper() == "SHORT":
        return stop_price * (1.0 + slip)
    return stop_price * (1.0 - slip)


def _daily_win_stats(df: pd.DataFrame) -> Tuple[float, int, int]:
    if df.empty or "trade_date" not in df.columns or "pnl_pct" not in df.columns:
        return 0.0, 0, 0
    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    d["pnl_pct"] = pd.to_numeric(d["pnl_pct"], errors="coerce").fillna(0.0)
    day_pnl = d.groupby("trade_date", sort=True)["pnl_pct"].sum()
    total_days = int(len(day_pnl))
    if total_days == 0:
        return 0.0, 0, 0
    win_days = int((day_pnl > 0).sum())
    return float(win_days / total_days * 100.0), win_days, total_days


def _materialize_exit_variant(df: pd.DataFrame, variant: str) -> pd.DataFrame:
    """
    Build a metrics-ready DataFrame for one exit-resolution scenario.
    `variant` must be one of: base, pess, opt.
    """
    if df.empty:
        return df.copy()

    d = df.copy()

    def _fallback_text(primary: str, fallback: str) -> pd.Series:
        if primary in d.columns:
            out = d[primary].copy()
            if fallback in d.columns:
                out = out.where(out.notna(), d[fallback])
            return out
        if fallback in d.columns:
            return d[fallback].copy()
        return pd.Series([None] * len(d), index=d.index, dtype="object")

    def _fallback_num(primary: str, fallback: str) -> pd.Series:
        if primary in d.columns:
            out = pd.to_numeric(d[primary], errors="coerce")
            if fallback in d.columns:
                out = out.fillna(pd.to_numeric(d[fallback], errors="coerce"))
            return out.fillna(0.0)
        if fallback in d.columns:
            return pd.to_numeric(d[fallback], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=d.index, dtype="float64")

    price_col = f"pnl_pct_price_{variant}"
    gross_col = f"pnl_pct_gross_price_{variant}"
    outcome_col = f"outcome_{variant}"
    exit_price_col = f"exit_price_{variant}"
    exit_time_col = f"exit_time_ist_{variant}"

    d["pnl_pct_price"] = _fallback_num(price_col, "pnl_pct_price" if "pnl_pct_price" in d.columns else "pnl_pct")
    d["pnl_pct_gross_price"] = _fallback_num(
        gross_col,
        "pnl_pct_gross_price" if "pnl_pct_gross_price" in d.columns else "pnl_pct_gross",
    )
    d["outcome"] = _fallback_text(outcome_col, "outcome")
    if exit_price_col in d.columns:
        d["exit_price"] = pd.to_numeric(d[exit_price_col], errors="coerce").fillna(
            pd.to_numeric(d.get("exit_price", np.nan), errors="coerce")
        )
    if exit_time_col in d.columns:
        d["exit_time_ist"] = pd.to_datetime(d[exit_time_col], errors="coerce").fillna(
            pd.to_datetime(d.get("exit_time_ist", pd.NaT), errors="coerce")
        )

    return _add_notional_pnl(d)


def _print_exit_realism_band(label: str, df: pd.DataFrame) -> None:
    if df.empty:
        print(f"[EXIT_REALISM] {label}: no trades")
        return

    resolved_mask = pd.Series(False, index=df.index)
    if "exit_resolution_case" in df.columns:
        resolved_mask = df["exit_resolution_case"].astype(str).str.len().gt(0)
    resolved = int(resolved_mask.sum())
    fallback = int(len(df) - resolved)

    ambiguous = int(pd.to_numeric(df.get("exit_bar_ambiguous", 0), errors="coerce").fillna(0).astype(bool).sum())
    total = int(len(df))
    ambiguous_pct = (ambiguous / resolved * 100.0) if resolved else 0.0
    stressed_stop = int(pd.to_numeric(df.get("stop_fill_penalty_applied", 0), errors="coerce").fillna(0).astype(bool).sum())

    base_df = _materialize_exit_variant(df, "base")
    pess_df = _materialize_exit_variant(df, "pess")
    opt_df = _materialize_exit_variant(df, "opt")

    base_m = compute_backtest_metrics(base_df)
    pess_m = compute_backtest_metrics(pess_df)
    opt_m = compute_backtest_metrics(opt_df)
    base_day_win, base_win_days, base_total_days = _daily_win_stats(base_df)
    pess_day_win, _, _ = _daily_win_stats(pess_df)
    opt_day_win, _, _ = _daily_win_stats(opt_df)

    print(
        f"[EXIT_REALISM] {label}: resolved={resolved}/{total} | fallback={fallback} | "
        f"ambiguous={ambiguous}/{resolved} ({ambiguous_pct:.2f}%) | stressed_stop_exits={stressed_stop} | "
        f"extra_stop_slip={STOP_EXIT_EXTRA_SLIPPAGE_BPS:.1f}bps"
    )
    print(
        f"[EXIT_REALISM] {label} base        : pnl={base_m.sum_pnl_pct:.4f}% | "
        f"pf={base_m.profit_factor:.3f} | day-win={base_day_win:.2f}% ({base_win_days}/{base_total_days}) | "
        f"maxdd={base_m.max_drawdown_pct:.4f}%"
    )
    print(
        f"[EXIT_REALISM] {label} pessimistic : pnl={pess_m.sum_pnl_pct:.4f}% | "
        f"pf={pess_m.profit_factor:.3f} | day-win={pess_day_win:.2f}% | "
        f"maxdd={pess_m.max_drawdown_pct:.4f}%"
    )
    print(
        f"[EXIT_REALISM] {label} optimistic  : pnl={opt_m.sum_pnl_pct:.4f}% | "
        f"pf={opt_m.profit_factor:.3f} | day-win={opt_day_win:.2f}% | "
        f"maxdd={opt_m.max_drawdown_pct:.4f}%"
    )


# ===========================================================================
# 5-MIN EXIT RESOLUTION
# ===========================================================================
def _resolve_exits_5min(
    trades_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
    eod_exit_time: Optional[dtime] = None,
) -> pd.DataFrame:
    """
    Re-evaluate exit prices, exit times, and outcomes using 1-min data
    for higher-resolution SL/target tracking.

    Entry signals and entry prices remain from 15-min scanning.
    Only the exit side is recalculated at 1-min granularity.
    """
    if trades_df.empty:
        return trades_df

    if not dir_5m.is_dir():
        print(f"[WARN] 1-min data directory not found: {dir_5m}")
        print("[WARN] Falling back to 15-min exit resolution.")
        return trades_df

    df = trades_df.copy()

    # Backward-compat: unified Trade dataclass uses `sl_price`.
    # Keep `stop_price` as canonical within this function for downstream logic.
    if "stop_price" not in df.columns and "sl_price" in df.columns:
        df["stop_price"] = df["sl_price"]

    # Ensure required columns exist
    required = {"ticker", "side", "entry_price", "entry_time_ist", "stop_price", "target_price"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        print(f"[WARN] Missing columns for 1-min resolution: {missing}")
        return df

    # Convert timestamps
    for c in ["entry_time_ist", "exit_time_ist", "signal_time_ist"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Cache 1-min data per ticker to avoid re-reads
    _cache_5m: Dict[str, pd.DataFrame] = {}
    _cache_15m: Dict[str, pd.DataFrame] = {}
    dir_15m = _resolve_15m_dir()

    def _resolve_from_bars(
        bars: pd.DataFrame,
        side_val: str,
        stop_val: float,
        target_val: float,
        resolution_prefix: str,
    ) -> Optional[Dict[str, Any]]:
        if bars is None or bars.empty:
            return None

        base_exit_price = None
        base_exit_time = None
        base_outcome = None
        pess_exit_price = None
        pess_exit_time = None
        pess_outcome = None
        opt_exit_price = None
        opt_exit_time = None
        opt_outcome = None
        exit_resolution_case = ""
        exit_bar_ambiguous = False
        stop_fill_penalty_applied = False

        time_col = "datetime" if "datetime" in bars.columns else "date"
        high_col = "high" if "high" in bars.columns else "High"
        low_col = "low" if "low" in bars.columns else "Low"
        close_col = "close" if "close" in bars.columns else "Close"

        for _, bar in bars.iterrows():
            bar_high = float(bar.get(high_col, np.nan))
            bar_low = float(bar.get(low_col, np.nan))
            bar_time = bar[time_col]

            if np.isnan(bar_high) or np.isnan(bar_low):
                continue

            if side_val == "SHORT":
                stop_hit = bar_high >= stop_val
                target_hit = bar_low <= target_val
            else:
                stop_hit = bar_low <= stop_val
                target_hit = bar_high >= target_val

            if stop_hit and target_hit:
                exit_resolution_case = f"{resolution_prefix}_AMBIGUOUS_BOTH_IN_BAR"
                exit_bar_ambiguous = True
                base_exit_price = stop_val
                base_exit_time = bar_time
                base_outcome = "SL"
                pess_exit_price = _apply_stop_exit_slippage(side_val, stop_val)
                pess_exit_time = bar_time
                pess_outcome = "SL"
                opt_exit_price = target_val
                opt_exit_time = bar_time
                opt_outcome = "TARGET"
                stop_fill_penalty_applied = True
                break
            if stop_hit:
                stressed_stop_price = _apply_stop_exit_slippage(side_val, stop_val)
                exit_resolution_case = f"{resolution_prefix}_STOP_ONLY"
                base_exit_price = stop_val
                base_exit_time = bar_time
                base_outcome = "SL"
                pess_exit_price = stressed_stop_price
                pess_exit_time = bar_time
                pess_outcome = "SL"
                opt_exit_price = stressed_stop_price
                opt_exit_time = bar_time
                opt_outcome = "SL"
                stop_fill_penalty_applied = True
                break
            if target_hit:
                exit_resolution_case = f"{resolution_prefix}_TARGET_ONLY"
                base_exit_price = target_val
                base_exit_time = bar_time
                base_outcome = "TARGET"
                pess_exit_price = target_val
                pess_exit_time = bar_time
                pess_outcome = "TARGET"
                opt_exit_price = target_val
                opt_exit_time = bar_time
                opt_outcome = "TARGET"
                break

        if base_exit_price is None:
            last_bar = bars.iloc[-1]
            eod_exit_price = float(last_bar.get(close_col, entry_price))
            eod_exit_time_val = last_bar[time_col]
            exit_resolution_case = f"{resolution_prefix}_EOD_CLOSE"
            base_exit_price = eod_exit_price
            base_exit_time = eod_exit_time_val
            base_outcome = "EOD"
            pess_exit_price = eod_exit_price
            pess_exit_time = eod_exit_time_val
            pess_outcome = "EOD"
            opt_exit_price = eod_exit_price
            opt_exit_time = eod_exit_time_val
            opt_outcome = "EOD"

        return {
            "base_exit_price": base_exit_price,
            "base_exit_time": base_exit_time,
            "base_outcome": base_outcome,
            "pess_exit_price": pess_exit_price,
            "pess_exit_time": pess_exit_time,
            "pess_outcome": pess_outcome,
            "opt_exit_price": opt_exit_price,
            "opt_exit_time": opt_exit_time,
            "opt_outcome": opt_outcome,
            "exit_resolution_case": exit_resolution_case,
            "exit_bar_ambiguous": bool(exit_bar_ambiguous),
            "stop_fill_penalty_applied": bool(stop_fill_penalty_applied),
        }

    updated_rows = 0
    fallback_rows = 0
    total_rows = len(df)

    for idx in df.index:
        ticker = str(df.at[idx, "ticker"])
        side = str(df.at[idx, "side"]).upper()
        entry_price = float(df.at[idx, "entry_price"])
        entry_time = df.at[idx, "entry_time_ist"]
        stop_price = float(df.at[idx, "stop_price"]) if pd.notna(df.at[idx, "stop_price"]) else None
        target_price = float(df.at[idx, "target_price"]) if pd.notna(df.at[idx, "target_price"]) else None

        if pd.isna(entry_time) or stop_price is None or target_price is None:
            continue

        # Load 1-min data for this ticker (cached)
        if ticker not in _cache_5m:
            # Try common naming patterns
            found = False
            for pattern in [
                f"{ticker}{suffix_5m}",
                f"{ticker}.parquet",
                f"{ticker}_5min.parquet",
                f"{ticker}_stocks_indicators_5min.parquet",
                f"{ticker}_1min.parquet",
                f"{ticker}_stocks_indicators_1min.parquet",
            ]:
                fpath = dir_5m / pattern
                if fpath.exists():
                    _cache_5m[ticker] = read_5m_parquet(str(fpath), engine)
                    found = True
                    break
            if not found:
                _cache_5m[ticker] = pd.DataFrame()

        df_5m = _cache_5m[ticker]
        # Get the trade date for EOD cutoff
        trade_date = pd.Timestamp(entry_time).normalize()
        eod_cutoff = None
        if eod_exit_time is not None:
            eod_cutoff = IST.localize(datetime.combine(trade_date.date(), eod_exit_time))

        resolved = None
        if not df_5m.empty:
            mask = (df_5m["datetime"] > entry_time) & (df_5m["datetime"].dt.normalize() == trade_date)
            if eod_cutoff is not None:
                mask = mask & (df_5m["datetime"] <= eod_cutoff)
            bars = df_5m.loc[mask].sort_values("datetime")
            resolved = _resolve_from_bars(bars, side, stop_price, target_price, "1MIN")

        if resolved is None:
            if ticker not in _cache_15m:
                fpath_15m = dir_15m / f"{ticker}_stocks_indicators_15min.parquet"
                if fpath_15m.exists():
                    _cache_15m[ticker] = read_15m_parquet(str(fpath_15m), engine)
                else:
                    _cache_15m[ticker] = pd.DataFrame()

            df_15m = _cache_15m[ticker]
            if not df_15m.empty:
                time_col = "date"
                mask_15m = (df_15m[time_col] > entry_time) & (df_15m[time_col].dt.normalize() == trade_date)
                if eod_cutoff is not None:
                    mask_15m = mask_15m & (df_15m[time_col] <= eod_cutoff)
                bars_15m = df_15m.loc[mask_15m].sort_values(time_col)
                if bars_15m.empty and eod_cutoff is not None:
                    same_day = df_15m.loc[
                        (df_15m[time_col].dt.normalize() == trade_date)
                        & (df_15m[time_col] <= eod_cutoff)
                    ].sort_values(time_col)
                    bars_15m = same_day.tail(1)
                resolved = _resolve_from_bars(bars_15m, side, stop_price, target_price, "15M_FALLBACK")
                if resolved is not None:
                    fallback_rows += 1

        if resolved is None:
            continue

        base_exit_price = resolved["base_exit_price"]
        base_exit_time = resolved["base_exit_time"]
        base_outcome = resolved["base_outcome"]
        pess_exit_price = resolved["pess_exit_price"]
        pess_exit_time = resolved["pess_exit_time"]
        pess_outcome = resolved["pess_outcome"]
        opt_exit_price = resolved["opt_exit_price"]
        opt_exit_time = resolved["opt_exit_time"]
        opt_outcome = resolved["opt_outcome"]
        exit_resolution_case = resolved["exit_resolution_case"]
        exit_bar_ambiguous = bool(resolved["exit_bar_ambiguous"])
        stop_fill_penalty_applied = bool(resolved["stop_fill_penalty_applied"])

        # Apply slippage + commission (read from existing columns or use defaults)
        slippage_pct = float(df.at[idx, "slippage_pct"]) if "slippage_pct" in df.columns and pd.notna(
            df.at[idx, "slippage_pct"]) else 0.0005
        commission_pct = float(df.at[idx, "commission_pct"]) if "commission_pct" in df.columns and pd.notna(
            df.at[idx, "commission_pct"]) else 0.0003
        cost_pct = (slippage_pct + commission_pct) * 100.0 * 2  # round-trip

        base_raw_pct = _calc_price_return_pct(side, entry_price, base_exit_price)
        pess_raw_pct = _calc_price_return_pct(side, entry_price, pess_exit_price)
        opt_raw_pct = _calc_price_return_pct(side, entry_price, opt_exit_price)

        # Keep the legacy base 1-min path as the default main output unless
        # EXIT_REALISM_USE_STRESSED_BASE=True is explicitly enabled.
        selected_variant = "pess" if (EXIT_REALISM_BAND_ENABLED and EXIT_REALISM_USE_STRESSED_BASE) else "base"
        if selected_variant == "pess":
            selected_exit_price = pess_exit_price
            selected_exit_time = pess_exit_time
            selected_outcome = pess_outcome
            selected_raw_pct = pess_raw_pct
        else:
            selected_exit_price = base_exit_price
            selected_exit_time = base_exit_time
            selected_outcome = base_outcome
            selected_raw_pct = base_raw_pct

        # Update the trade row using the selected main path.
        df.at[idx, "exit_price"] = selected_exit_price
        df.at[idx, "exit_time_ist"] = selected_exit_time
        df.at[idx, "outcome"] = selected_outcome
        df.at[idx, "pnl_pct_gross"] = selected_raw_pct
        df.at[idx, "pnl_pct"] = selected_raw_pct - cost_pct

        if EXIT_REALISM_BAND_ENABLED:
            df.at[idx, "exit_resolution_case"] = exit_resolution_case
            df.at[idx, "exit_bar_ambiguous"] = bool(exit_bar_ambiguous)
            df.at[idx, "stop_fill_penalty_applied"] = bool(stop_fill_penalty_applied)
            df.at[idx, "stop_fill_penalty_bps"] = float(STOP_EXIT_EXTRA_SLIPPAGE_BPS) if stop_fill_penalty_applied else 0.0

            df.at[idx, "exit_price_base"] = base_exit_price
            df.at[idx, "exit_time_ist_base"] = base_exit_time
            df.at[idx, "outcome_base"] = base_outcome
            df.at[idx, "pnl_pct_gross_price_base"] = base_raw_pct
            df.at[idx, "pnl_pct_price_base"] = base_raw_pct - cost_pct

            df.at[idx, "exit_price_pess"] = pess_exit_price
            df.at[idx, "exit_time_ist_pess"] = pess_exit_time
            df.at[idx, "outcome_pess"] = pess_outcome
            df.at[idx, "pnl_pct_gross_price_pess"] = pess_raw_pct
            df.at[idx, "pnl_pct_price_pess"] = pess_raw_pct - cost_pct

            df.at[idx, "exit_price_opt"] = opt_exit_price
            df.at[idx, "exit_time_ist_opt"] = opt_exit_time
            df.at[idx, "outcome_opt"] = opt_outcome
            df.at[idx, "pnl_pct_gross_price_opt"] = opt_raw_pct
            df.at[idx, "pnl_pct_price_opt"] = opt_raw_pct - cost_pct

        updated_rows += 1

    print(
        f"[1MIN] Re-resolved exits for {updated_rows}/{total_rows} trades using 1-min data."
        + (f" 15m_fallback={fallback_rows}." if fallback_rows else "")
    )
    return df


# ===========================================================================
# WORKER FUNCTIONS (for parallel scanning â€” still uses 15-min for entry signals)
# ===========================================================================
def _scan_one_ticker_short(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    """Scan one ticker on the SHORT side. Returns list of Trade dicts."""
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_short(ticker, df, cfg)
    return [asdict(t) for t in trades]


def _scan_one_ticker_long(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    """Scan one ticker on the LONG side. Returns list of Trade dicts."""
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_long(ticker, df, cfg)
    return [asdict(t) for t in trades]


# ===========================================================================
# PARALLEL SCAN RUNNER
# ===========================================================================
def _run_side_parallel(
    side: str,
    cfg: StrategyConfig,
    max_workers: int = MAX_WORKERS,
) -> pd.DataFrame:
    """
    Scan all tickers for one side using ProcessPoolExecutor.
    Falls back to serial if max_workers <= 1.
    """
    tickers = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    print(f"[{side}] Tickers found: {len(tickers)}")

    worker_fn = _scan_one_ticker_short if side == "SHORT" else _scan_one_ticker_long
    task_args = [
        (t, os.path.join(cfg.dir_15m, f"{t}{cfg.end_15m}"), cfg)
        for t in tickers
    ]

    all_dicts: List[dict] = []
    scan_errors: List[Tuple[str, str]] = []  # (ticker, error_msg)

    if max_workers <= 1:
        # Serial fallback
        for k, args in enumerate(task_args, 1):
            try:
                result = worker_fn(args)
                all_dicts.extend(result)
            except Exception as e:
                scan_errors.append((args[0], str(e)))
            if k % 50 == 0:
                print(f"  [{side}] scanned {k}/{len(tickers)} | trades={len(all_dicts)}")
    else:
        # Parallel
        done_count = 0
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(worker_fn, a): a[0] for a in task_args}
            for future in as_completed(futures):
                done_count += 1
                try:
                    result = future.result()
                    all_dicts.extend(result)
                except Exception as e:
                    ticker = futures[future]
                    scan_errors.append((ticker, str(e)))

                if done_count % 100 == 0:
                    print(
                        f"  [{side}] scanned {done_count}/{len(tickers)} | trades={len(all_dicts)}"
                    )

    if scan_errors:
        print(f"  [{side}] {len(scan_errors)} ticker(s) skipped (bad/missing data): "
              f"{', '.join(t for t, _ in scan_errors)}")

    if not all_dicts:
        return pd.DataFrame()

    out = pd.DataFrame(all_dicts)

    # Apply Top-N per day
    out = apply_topn_per_day(out, cfg)

    # Ensure datetime columns
    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    sort_cols = [c for c in ["trade_date", "ticker", "entry_time_ist"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)

    return out


# ===========================================================================
# NOTIONAL P&L
# ===========================================================================
def _add_notional_pnl(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add capital/notional P&L columns and apply intraday leverage correctly.

    Strategy logic computes pnl_pct as *price-return %* (unlevered).
    In intraday (e.g., 5x), your *capital/margin* stays the same (POSITION_SIZE_RS_*),
    but your *notional exposure* is capital * leverage.

    We therefore:
      - preserve unlevered price-return % in pnl_pct_price / pnl_pct_gross_price
      - compute ROI% on capital (levered) in pnl_pct / pnl_pct_gross
      - compute rupee P&L on notional exposure in pnl_rs / pnl_rs_gross
    """
    if df.empty:
        return df

    d = df.copy()

    # ---- Ensure price-return % columns exist (unlevered) ----
    if "pnl_pct_price" not in d.columns:
        d["pnl_pct_price"] = pd.to_numeric(d.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)

    if "pnl_pct_gross_price" not in d.columns:
        if "pnl_pct_gross" in d.columns:
            d["pnl_pct_gross_price"] = pd.to_numeric(d["pnl_pct_gross"], errors="coerce").fillna(0.0)
        elif {"entry_price", "exit_price", "side"}.issubset(d.columns):
            ep = pd.to_numeric(d["entry_price"], errors="coerce")
            xp = pd.to_numeric(d["exit_price"], errors="coerce")
            s = d["side"].astype(str).str.upper()
            denom = ep.replace(0, np.nan)
            gross = np.where(s.eq("SHORT"), (ep - xp) / denom * 100.0, (xp - ep) / denom * 100.0)
            d["pnl_pct_gross_price"] = pd.to_numeric(gross, errors="coerce").fillna(0.0)
        else:
            d["pnl_pct_gross_price"] = 0.0

    # Normalize side
    side_u = d["side"].astype(str).str.upper() if "side" in d.columns else pd.Series([""] * len(d))

    # Capital/margin per trade (Rs.)
    if "position_size_rs" not in d.columns:
        d["position_size_rs"] = np.nan
    d["position_size_rs"] = pd.to_numeric(d["position_size_rs"], errors="coerce")
    size_missing = d["position_size_rs"].isna() | (d["position_size_rs"] <= 0)
    d.loc[side_u.eq("SHORT") & size_missing, "position_size_rs"] = float(POSITION_SIZE_RS_SHORT)
    d.loc[~side_u.eq("SHORT") & size_missing, "position_size_rs"] = float(POSITION_SIZE_RS_LONG)
    d["position_size_rs"] = pd.to_numeric(d["position_size_rs"], errors="coerce").fillna(0.0)

    # Leverage per trade
    if "leverage" not in d.columns:
        d["leverage"] = np.nan
    d.loc[side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_SHORT)
    d.loc[~side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_LONG)
    d["leverage"] = pd.to_numeric(d["leverage"], errors="coerce").fillna(1.0)

    # Notional exposure (Rs.)
    d["notional_exposure_rs"] = d["position_size_rs"] * d["leverage"]

    # ROI% on capital (levered)
    d["pnl_pct"] = pd.to_numeric(d["pnl_pct_price"], errors="coerce").fillna(0.0) * d["leverage"]
    d["pnl_pct_gross"] = pd.to_numeric(d["pnl_pct_gross_price"], errors="coerce").fillna(0.0) * d["leverage"]

    # Rupee P&L on notional exposure
    d["pnl_rs"] = (pd.to_numeric(d["pnl_pct_price"], errors="coerce").fillna(0.0) / 100.0) * d["notional_exposure_rs"]
    d["pnl_rs_gross"] = (pd.to_numeric(d["pnl_pct_gross_price"], errors="coerce").fillna(0.0) / 100.0) * d["notional_exposure_rs"]

    return d


def _sort_trades_for_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep exports deterministic and day-level readable so LONG/SHORT rows are
    naturally interleaved by date/time instead of appearing in side blocks.
    """
    if df.empty:
        return df

    d = df.copy()

    if "trade_date" in d.columns:
        d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce")

    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")

    sort_cols = [
        c
        for c in ["trade_date", "entry_time_ist", "signal_time_ist", "ticker", "side"]
        if c in d.columns
    ]
    if sort_cols:
        d = d.sort_values(sort_cols).reset_index(drop=True)

    return d


def _print_day_side_mix(df: pd.DataFrame) -> None:
    """
    Print how many dates contain only LONG, only SHORT, or both.
    """
    if df.empty or not {"trade_date", "side"}.issubset(df.columns):
        return

    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    d["side"] = d["side"].astype(str).str.upper()

    pivot = d.groupby(["trade_date", "side"]).size().unstack(fill_value=0)
    short_s = pivot["SHORT"] if "SHORT" in pivot.columns else pd.Series(0, index=pivot.index)
    long_s = pivot["LONG"] if "LONG" in pivot.columns else pd.Series(0, index=pivot.index)

    only_short = int(((short_s > 0) & (long_s == 0)).sum())
    only_long = int(((long_s > 0) & (short_s == 0)).sum())
    both = int(((short_s > 0) & (long_s > 0)).sum())
    total_days = int(len(pivot))

    print(
        f"[INFO] Day-side mix: both={both} | short_only={only_short} | "
        f"long_only={only_long} | total_days={total_days}"
    )




def _ts_to_key_local(ts: pd.Timestamp) -> str:
    ts_pd = pd.Timestamp(ts)
    if ts_pd.tzinfo is None:
        ts_pd = ts_pd.tz_localize("UTC")
    return ts_pd.tz_convert(IST).isoformat()


def _load_first_nifty_source(
    cfg: StrategyConfig, tickers: Tuple[str, ...]
) -> Tuple[pd.DataFrame, str]:
    for ticker_found in tickers:
        p = Path(cfg.dir_15m) / f"{ticker_found}{cfg.end_15m}"
        if not p.exists():
            continue
        try:
            df = read_15m_parquet(str(p), cfg.parquet_engine)
            if not df.empty:
                df = df.sort_values("date").reset_index(drop=True)
                return df, ticker_found
        except Exception:
            continue
    return pd.DataFrame(), ""


def _intraday_anchor_from_close_volume(close_s: pd.Series, volume_s: pd.Series) -> pd.Series:
    vol = pd.to_numeric(volume_s, errors="coerce").fillna(0.0)
    close = pd.to_numeric(close_s, errors="coerce")
    if bool((vol > 0).any()):
        numer = (close * vol).cumsum()
        denom = vol.cumsum().replace(0, np.nan)
        return numer / denom
    return close.expanding(min_periods=1).mean()


def _build_nifty_intraday_context(
    cfg: StrategyConfig,
) -> Tuple[Dict[str, str], Dict[str, float], str, Dict[str, int]]:
    """
    Build a no-lookahead intraday NIFTY context map keyed by 15m timestamp.

    After the first hour settles, classify each bar as:
    - LONG_ONLY: NIFTY is above first-hour range, above intraday anchor, and up on the day
    - SHORT_ONLY: NIFTY is below first-hour range, below intraday anchor, and down on the day
    - BOTH: otherwise
    """
    idx, ticker_found = _load_first_nifty_source(cfg, NIFTY_CONTEXT_TICKERS)
    if idx.empty:
        return {}, {}, "", {}

    try:
        idx = idx[idx["date"].apply(lambda ts: pd.Timestamp(ts).tz_convert(IST).time() <= dtime(15, 15, 0))].copy()
        if idx.empty:
            return {}, {}, "", {}

        dt = pd.to_datetime(idx["date"], errors="coerce")
        idx = idx.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert(IST)
        idx["date"] = dt.dt.tz_convert(IST)
        idx = idx.sort_values("date").reset_index(drop=True)
        idx["day"] = idx["date"].dt.strftime("%Y-%m-%d")
        idx["clock"] = idx["date"].dt.time

        idx["close"] = pd.to_numeric(idx["close"], errors="coerce")
        idx["high"] = pd.to_numeric(idx["high"], errors="coerce")
        idx["low"] = pd.to_numeric(idx["low"], errors="coerce")
        if "volume" not in idx.columns:
            idx["volume"] = 0.0
        idx["volume"] = pd.to_numeric(idx["volume"], errors="coerce").fillna(0.0)

        idx["intraday_anchor"] = (
            idx.groupby("day", group_keys=False)
            .apply(lambda g: _intraday_anchor_from_close_volume(g["close"], g["volume"]))
            .reset_index(level=0, drop=True)
        )
        idx["ret_lookback_pct"] = idx.groupby("day")["close"].pct_change(NIFTY_RS_LOOKBACK_BARS) * 100.0

        prev_day_close = idx.groupby("day", as_index=True)["close"].last().shift(1)
        idx["prev_day_close"] = idx["day"].map(prev_day_close)
        idx["day_move_pct"] = (
            (idx["close"] - idx["prev_day_close"]) / idx["prev_day_close"].replace(0, np.nan)
        ) * 100.0

        first_hour = idx[idx["clock"] <= NIFTY_CONTEXT_OR_END_TIME].copy()
        or_high = first_hour.groupby("day", as_index=True)["high"].max()
        or_low = first_hour.groupby("day", as_index=True)["low"].min()
        idx["or_high"] = idx["day"].map(or_high)
        idx["or_low"] = idx["day"].map(or_low)

        after_confirm = idx["clock"].apply(lambda t: t >= NIFTY_CONTEXT_CONFIRM_TIME)
        long_only = (
            after_confirm
            & idx["day_move_pct"].ge(float(NIFTY_CONTEXT_MIN_DAYMOVE_PCT))
            & idx["close"].gt(idx["intraday_anchor"])
            & idx["close"].gt(idx["or_high"])
        )
        short_only = (
            after_confirm
            & idx["day_move_pct"].le(-float(NIFTY_CONTEXT_MIN_DAYMOVE_PCT))
            & idx["close"].lt(idx["intraday_anchor"])
            & idx["close"].lt(idx["or_low"])
        )
        modes = np.where(long_only, "LONG_ONLY", np.where(short_only, "SHORT_ONLY", "BOTH"))

        mode_map: Dict[str, str] = {}
        ret_map: Dict[str, float] = {}
        counts = {"LONG_ONLY": 0, "SHORT_ONLY": 0, "BOTH": 0}
        for ts, mode, ret in zip(idx["date"], modes, idx["ret_lookback_pct"]):
            key = _ts_to_key_local(ts)
            mode_map[key] = str(mode)
            if np.isfinite(ret):
                ret_map[key] = float(ret)
            counts[str(mode)] = counts.get(str(mode), 0) + 1
        return mode_map, ret_map, ticker_found, counts
    except Exception:
        return {}, {}, "", {}


def _build_stock_return_map(
    ticker: str,
    cfg: StrategyConfig,
    cache: Dict[str, Dict[str, float]],
) -> Dict[str, float]:
    ticker_u = str(ticker).strip().upper()
    if ticker_u in cache:
        return cache[ticker_u]

    out: Dict[str, float] = {}
    p = Path(cfg.dir_15m) / f"{ticker_u}{cfg.end_15m}"
    if not p.exists():
        cache[ticker_u] = out
        return out

    try:
        df = read_15m_parquet(str(p), cfg.parquet_engine)
        if df.empty:
            cache[ticker_u] = out
            return out
        df = df.sort_values("date").reset_index(drop=True)
        dt = pd.to_datetime(df["date"], errors="coerce")
        df = df.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert(IST)
        df["date"] = dt.dt.tz_convert(IST)
        df["day"] = df["date"].dt.strftime("%Y-%m-%d")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["ret_lookback_pct"] = df.groupby("day")["close"].pct_change(NIFTY_RS_LOOKBACK_BARS) * 100.0
        for ts, ret in zip(df["date"], df["ret_lookback_pct"]):
            if np.isfinite(ret):
                out[_ts_to_key_local(ts)] = float(ret)
    except Exception:
        out = {}
    cache[ticker_u] = out
    return out


def _apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg: StrategyConfig,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply no-lookahead NIFTY context and relative-strength filter to trades."""
    if not mode_map:
        return short_df, long_df

    stock_ret_cache: Dict[str, Dict[str, float]] = {}

    def _apply_side(df: pd.DataFrame, side: str) -> Tuple[pd.DataFrame, int, int]:
        if df.empty:
            return df, 0, 0

        d = df.copy()
        ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
        ts = pd.to_datetime(d[ts_col], errors="coerce")
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(IST)
        else:
            ts = ts.dt.tz_convert(IST)
        d["ts_key_local"] = ts.map(_ts_to_key_local)
        d["nifty_context_mode"] = d["ts_key_local"].map(mode_map).fillna("BOTH")

        before = len(d)
        if side.upper() == "SHORT":
            d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
        else:
            d = d[d["nifty_context_mode"].ne("SHORT_ONLY")].copy()
        mode_removed = before - len(d)

        if not NIFTY_RS_FILTER_ENABLED or d.empty:
            if "ts_key_local" in d.columns:
                d = d.drop(columns=["ts_key_local"])
            return d, mode_removed, 0

        keep_mask: List[bool] = []
        rel_vals: List[float] = []
        rs_removed = 0
        side_u = side.upper()
        for row in d.itertuples(index=False):
            ts_key = getattr(row, "ts_key_local")
            mode = getattr(row, "nifty_context_mode", "BOTH")
            rel_val = np.nan
            keep = True

            # Determine which RS threshold applies for this bar's mode:
            # - Directional modes (LONG_ONLY / SHORT_ONLY): use full NIFTY_RS_THRESHOLD_PCT
            # - BOTH mode: use relaxed NIFTY_RS_BOTH_MODE_THRESHOLD_PCT (v15 new)
            if mode != "BOTH":
                rs_thresh = float(NIFTY_RS_THRESHOLD_PCT)
                apply_rs = True
            elif NIFTY_RS_BOTH_MODE_ENABLED:
                rs_thresh = float(NIFTY_RS_BOTH_MODE_THRESHOLD_PCT)
                apply_rs = True
            else:
                rs_thresh = 0.0
                apply_rs = False

            if apply_rs:
                stock_ret_map = _build_stock_return_map(getattr(row, "ticker"), cfg, stock_ret_cache)
                stock_ret = stock_ret_map.get(ts_key, np.nan)
                nifty_ret = nifty_ret_map.get(ts_key, np.nan)
                if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                    rel_val = float(stock_ret - nifty_ret)
                    if side_u == "LONG":
                        keep = rel_val >= rs_thresh
                    else:
                        keep = rel_val <= -rs_thresh

            keep_mask.append(bool(keep))
            rel_vals.append(rel_val)
            if not keep:
                rs_removed += 1

        d["nifty_rel_strength_pct"] = rel_vals
        d = d[pd.Series(keep_mask, index=d.index)].copy()
        if "ts_key_local" in d.columns:
            d = d.drop(columns=["ts_key_local"])
        return d, mode_removed, rs_removed

    s, s_mode_removed, s_rs_removed = _apply_side(short_df, "SHORT")
    l, l_mode_removed, l_rs_removed = _apply_side(long_df, "LONG")

    both_mode_rs_note = (
        f" [BOTH-mode RS>={NIFTY_RS_BOTH_MODE_THRESHOLD_PCT}% active]"
        if NIFTY_RS_BOTH_MODE_ENABLED else ""
    )
    print(
        "[NIFTY_CONTEXT] Applied intraday filter: "
        f"SHORT {len(short_df)}->{len(s)} (mode_removed={s_mode_removed}, rs_removed={s_rs_removed}) | "
        f"LONG {len(long_df)}->{len(l)} (mode_removed={l_mode_removed}, rs_removed={l_rs_removed})"
        f"{both_mode_rs_note}"
    )
    return s, l


def _print_signal_entry_lag_summary(df: pd.DataFrame) -> None:
    """
    Print signal->entry lag stats by side/setup/impulse to debug execution gaps.
    Lag is measured in minutes: entry_time_ist - signal_time_ist.
    """
    if df.empty:
        return

    required = {"signal_time_ist", "entry_time_ist"}
    if not required.issubset(df.columns):
        missing = sorted(required - set(df.columns))
        print(f"[INFO] Lag summary skipped (missing columns: {missing})")
        return

    d = df.copy()
    d["signal_time_ist"] = pd.to_datetime(d["signal_time_ist"], errors="coerce")
    d["entry_time_ist"] = pd.to_datetime(d["entry_time_ist"], errors="coerce")
    d = d.dropna(subset=["signal_time_ist", "entry_time_ist"]).copy()
    if d.empty:
        print("[INFO] Lag summary skipped (no valid signal/entry timestamps).")
        return

    for c in ["side", "setup", "impulse_type"]:
        if c not in d.columns:
            d[c] = ""
        d[c] = d[c].fillna("").astype(str)
    d["side"] = d["side"].str.upper()

    d["lag_min"] = (
        d["entry_time_ist"] - d["signal_time_ist"]
    ).dt.total_seconds() / 60.0

    grouped = d.groupby(["side", "setup", "impulse_type"], dropna=False)["lag_min"]
    lag_summary = grouped.agg(
        count="size",
        min="min",
        p50="median",
        mean="mean",
        p90=lambda s: s.quantile(0.90),
        max="max",
    ).reset_index()

    lag_summary["p50_bars_15m"] = lag_summary["p50"] / 15.0
    lag_summary = lag_summary.sort_values(["side", "setup", "impulse_type"]).reset_index(drop=True)

    for col in ["min", "p50", "mean", "p90", "max", "p50_bars_15m"]:
        lag_summary[col] = pd.to_numeric(lag_summary[col], errors="coerce").round(2)

    neg_rows = int((d["lag_min"] < 0).sum())
    zero_rows = int((d["lag_min"] == 0).sum())

    print("\n[DEBUG] Signal->Entry lag by setup (minutes)")
    print(
        f"[DEBUG] Rows={len(d)} | negative_lag_rows={neg_rows} | "
        f"same_timestamp_rows={zero_rows}"
    )
    print(lag_summary.to_string(index=False))


# ===========================================================================
# CASH-CONSTRAINED PORTFOLIO SIM (optimized with itertuples)
# ===========================================================================
def _simulate_cash_constrained(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if df.empty:
        return df, {
            "start_capital": PORTFOLIO_START_CAPITAL_RS,
            "taken": 0,
            "skipped": 0,
            "net_pnl_rs": 0.0,
            "final_equity": float(PORTFOLIO_START_CAPITAL_RS),
            "roi_pct": 0.0,
            "max_concurrent": 0,
            "min_cash": float(PORTFOLIO_START_CAPITAL_RS),
        }

    # Ensure datetime
    d = df.copy()
    for c in ["entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")

    d = d.sort_values(["entry_time_ist", "exit_time_ist", "ticker", "side"]).reset_index(
        drop=True
    )

    cash = float(PORTFOLIO_START_CAPITAL_RS)
    open_heap: list = []  # (exit_time, size, pnl_rs)
    seen_ticker_day: set = set()

    taken_flags = np.zeros(len(d), dtype=bool)
    cash_before_arr = np.zeros(len(d))
    cash_after_arr = np.zeros(len(d))
    pos_sizes_arr = np.zeros(len(d))
    pnl_rs_sim_arr = np.zeros(len(d))

    taken = 0
    skipped = 0
    max_conc = 0
    min_cash = cash

    # Use itertuples for ~5-10x speedup over iterrows
    for row in d.itertuples():
        idx = row.Index
        entry_ts = row.entry_time_ist
        exit_ts = row.exit_time_ist

        # Release closed positions
        while open_heap and open_heap[0][0] <= entry_ts:
            _, size, pnl_rs = heapq.heappop(open_heap)
            cash += size + pnl_rs

        cb = cash
        side = str(row.side).upper()
        ticker = str(row.ticker)
        day = str(row.trade_date)

        pos = float(POSITION_SIZE_RS_SHORT if side == "SHORT" else POSITION_SIZE_RS_LONG)
        pnl = float(getattr(row, "pnl_rs", 0.0))

        take = True
        if DISALLOW_BOTH_SIDES_SAME_TICKER_DAY:
            key = (ticker, day)
            if key in seen_ticker_day:
                take = False

        if cash < pos:
            take = False

        if take:
            cash -= pos
            heapq.heappush(open_heap, (exit_ts, pos, pnl))
            taken += 1
            seen_ticker_day.add((ticker, day))
        else:
            skipped += 1
            pos = 0.0
            pnl = 0.0

        taken_flags[idx] = take
        cash_before_arr[idx] = cb
        cash_after_arr[idx] = cash
        pos_sizes_arr[idx] = pos
        pnl_rs_sim_arr[idx] = pnl

        max_conc = max(max_conc, len(open_heap))
        min_cash = min(min_cash, cash)

    # Drain remaining positions
    while open_heap:
        _, size, pnl_rs = heapq.heappop(open_heap)
        cash += size + pnl_rs

    final_equity = cash
    net_pnl = final_equity - float(PORTFOLIO_START_CAPITAL_RS)
    roi = (net_pnl / float(PORTFOLIO_START_CAPITAL_RS) * 100.0) if PORTFOLIO_START_CAPITAL_RS > 0 else 0.0

    d["taken"] = taken_flags
    d["cash_before"] = cash_before_arr
    d["cash_after"] = cash_after_arr
    d["position_size_rs_sim"] = pos_sizes_arr
    d["pnl_rs_sim"] = pnl_rs_sim_arr

    stats = {
        "start_capital": float(PORTFOLIO_START_CAPITAL_RS),
        "taken": int(taken),
        "skipped": int(skipped),
        "net_pnl_rs": float(net_pnl),
        "final_equity": float(final_equity),
        "roi_pct": float(roi),
        "max_concurrent": int(max_conc),
        "min_cash": float(min_cash),
    }
    return d, stats


def _print_portfolio(stats: Dict[str, Any]) -> None:
    print("\n================ PORTFOLIO SUMMARY (cash-constrained) ================")
    print(f"Start capital                 : Rs.{stats['start_capital']:,.2f}")
    print(f"Taken trades                  : {stats['taken']}")
    print(f"Skipped trades                : {stats['skipped']}")
    print(f"Net P&L                       : Rs.{stats['net_pnl_rs']:,.2f}")
    print(f"Final equity                  : Rs.{stats['final_equity']:,.2f}")
    print(f"ROI on start capital          : {stats['roi_pct']:.2f}%")
    print(f"Max concurrent positions      : {stats['max_concurrent']}")
    print(f"Minimum cash during run       : Rs.{stats['min_cash']:,.2f}")
    print("=" * 69)


# ===========================================================================
# NOTIONAL P&L SUMMARY
# ===========================================================================
def _print_notional_pnl(combined: pd.DataFrame) -> None:
    if "pnl_rs" not in combined.columns:
        return
    pnl_short = float(combined.loc[combined["side"].eq("SHORT"), "pnl_rs"].sum())
    pnl_long = float(combined.loc[combined["side"].eq("LONG"), "pnl_rs"].sum())
    pnl_all = float(combined["pnl_rs"].sum())

    print(f"\n{'=' * 20} NOTIONAL P&L SUMMARY (Rs.) {'=' * 20}")
    print(f"SHORT notional P&L            : Rs.{pnl_short:,.2f}")
    print(f"LONG  notional P&L            : Rs.{pnl_long:,.2f}")
    print(f"TOTAL notional P&L            : Rs.{pnl_all:,.2f}")
    print("=" * 61)


# ===========================================================================
# RECENT DAILY BREAKDOWN
# ===========================================================================
def _print_recent_daily_breakdown(df: pd.DataFrame, n_weeks: int = 2) -> None:
    """
    Print a day-by-day P&L / win-rate table for the most recent `n_weeks` trading weeks.

    Columns per row:
      Date | L | S | Trades | Wins | Loss | Win% | SumPnL% | AvgPnL% | Rs.PnL | Outcomes
    """
    if df.empty or "trade_date" not in df.columns:
        return

    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    d = d.dropna(subset=["trade_date"])
    if d.empty:
        return

    d["pnl_pct"]   = pd.to_numeric(d.get("pnl_pct", 0), errors="coerce").fillna(0.0)
    d["pnl_rs"]    = pd.to_numeric(d.get("pnl_rs", 0), errors="coerce").fillna(0.0)
    d["side"]      = d["side"].astype(str).str.upper()
    d["outcome"]   = d.get("outcome", pd.Series("", index=d.index)).astype(str).str.upper()
    _has_vix = "india_vix" in d.columns and d["india_vix"].astype(float).gt(0).any()
    if _has_vix:
        d["india_vix"] = pd.to_numeric(d["india_vix"], errors="coerce").fillna(0.0)

    # Keep only the last n_weeks*5 unique trading dates
    all_dates = sorted(d["trade_date"].unique())
    n_days = n_weeks * 5
    recent_dates = all_dates[-n_days:] if len(all_dates) >= n_days else all_dates
    d = d[d["trade_date"].isin(recent_dates)]
    if d.empty:
        return

    _vix_col = f"{'VIX':>5} " if _has_vix else ""
    hdr = (
        f"{'Date':<12} {'L':>3} {'S':>3} {'Tot':>4} "
        f"{'W':>4} {'L_':>4} {'Win%':>6} "
        f"{'SumPnL%':>10} {'AvgPnL%':>10} {'Rs.PnL':>11} "
        f"{_vix_col}{'Outcomes'}"
    )
    _width = max(72, len(hdr))
    sep = "-" * _width

    print(f"\n{'='*_width}")
    print(f"  Day-wise Breakdown â€” last {n_weeks} weeks ({len(recent_dates)} trading days)")
    print(f"{'='*_width}")
    print(hdr)
    print(sep)

    totals = dict(trades=0, longs=0, shorts=0, wins=0, losses=0,
                  sum_pnl=0.0, sum_rs=0.0)

    for dt in sorted(recent_dates):
        day = d[d["trade_date"] == dt]
        n       = len(day)
        n_long  = int((day["side"] == "LONG").sum())
        n_short = int((day["side"] == "SHORT").sum())
        wins    = int((day["pnl_pct"] > 0).sum())
        losses  = int((day["pnl_pct"] < 0).sum())
        win_pct = wins / n * 100 if n else 0.0
        sum_pnl = float(day["pnl_pct"].sum())
        avg_pnl = float(day["pnl_pct"].mean()) if n else 0.0
        sum_rs  = float(day["pnl_rs"].sum())

        # outcome mini-summary: T=target, S=sl, B=be, E=eod
        oc = day["outcome"].value_counts()
        oc_parts = []
        for code, label in [("TARGET", "T"), ("SL", "S"), ("BE", "B"), ("EOD", "E")]:
            if code in oc:
                oc_parts.append(f"{label}:{oc[code]}")
        oc_str = " ".join(oc_parts) if oc_parts else "-"

        # VIX: take first non-zero value for the day (it's a daily value)
        if _has_vix:
            _day_vix = day["india_vix"][day["india_vix"] > 0]
            _vix_val = float(_day_vix.iloc[0]) if not _day_vix.empty else 0.0
            _vix_str = f"{_vix_val:>5.1f} "
        else:
            _vix_str = ""

        print(
            f"{str(dt):<12} {n_long:>3} {n_short:>3} {n:>4} "
            f"{wins:>4} {losses:>4} {win_pct:>5.0f}% "
            f"{sum_pnl:>+10.2f}% {avg_pnl:>+10.2f}% "
            f"{sum_rs:>+11,.0f}  "
            f"{_vix_str}{oc_str}"
        )

        totals["trades"] += n
        totals["longs"]  += n_long
        totals["shorts"] += n_short
        totals["wins"]   += wins
        totals["losses"] += losses
        totals["sum_pnl"] += sum_pnl
        totals["sum_rs"]  += sum_rs

    # Summary row
    print(sep)
    t = totals
    tw = t["wins"] / t["trades"] * 100 if t["trades"] else 0.0
    avg_day_pnl = t["sum_pnl"] / len(recent_dates) if recent_dates else 0.0
    _total_vix_str = "  --- " if _has_vix else ""
    print(
        f"{'TOTAL':<12} {t['longs']:>3} {t['shorts']:>3} {t['trades']:>4} "
        f"{t['wins']:>4} {t['losses']:>4} {tw:>5.0f}% "
        f"{t['sum_pnl']:>+10.2f}% {avg_day_pnl:>+10.2f}% "
        f"{t['sum_rs']:>+11,.0f}  "
        f"{_total_vix_str}(avg/day)"
    )
    print(f"{'='*_width}\n")


# ===========================================================================
# ENHANCED CHARTING SUITE
# ===========================================================================
def generate_enhanced_charts(
    combined: pd.DataFrame,
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    save_dir: Path,
    ts_label: str = "",
) -> List[str]:
    """
    Generate a comprehensive set of backtest analysis charts.
    Returns list of saved file paths.

    Charts generated:
      1.  Cumulative P&L (combined, short, long) â€” line
      2.  Daily P&L bar chart (combined)
      3.  Drawdown curve (combined equity)
      4.  Win rate by side â€” bar chart
      5.  P&L distribution histogram (combined)
      6.  P&L distribution by side (overlay histograms)
      7.  Outcome breakdown â€” pie chart (TARGET / SL / EOD)
      8.  Outcome breakdown by side â€” grouped bar chart
      9.  Monthly P&L heatmap
      10. Weekday P&L analysis
      11. Hourly entry time distribution
      12. Rolling Sharpe ratio (20-trade rolling)
      13. Trade duration distribution
      14. Top 10 winners & losers
      15. Cumulative trade count over time
      16. P&L by setup/impulse type (if available)
      17. Quality score vs P&L scatter (if available)
      18. Win rate by month
      19. Average P&L by hour of day
      20. Risk-reward realized scatter
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.gridspec import GridSpec
        import matplotlib.ticker as mticker
    except ImportError:
        print("[WARN] matplotlib not available â€” skipping chart generation.")
        return []

    warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")

    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    saved: List[str] = []

    # ------------ Utility helpers ----------------
    def _safe_col(df_: pd.DataFrame, col: str) -> pd.Series:
        if col in df_.columns:
            return pd.to_numeric(df_[col], errors="coerce").fillna(0.0)
        return pd.Series(np.zeros(len(df_)), index=df_.index)

    def _save(fig, name: str):
        p = save_dir / f"{name}_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        saved.append(str(p))

    # Prepare common series
    combined_sorted = combined.copy()
    if "trade_date" in combined_sorted.columns:
        combined_sorted["trade_date"] = pd.to_datetime(combined_sorted["trade_date"], errors="coerce")
        combined_sorted = combined_sorted.sort_values("trade_date").reset_index(drop=True)

    pnl_pct = _safe_col(combined_sorted, "pnl_pct")
    pnl_rs = _safe_col(combined_sorted, "pnl_rs")
    cum_pnl = pnl_rs.cumsum()

    # ========== CHART 1: Cumulative P&L (Combined + Short + Long) ==========
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(cum_pnl.values, label="Combined", linewidth=2, color="#2563EB")
    if not short_df.empty:
        s_pnl = _safe_col(short_df.sort_values("trade_date") if "trade_date" in short_df.columns else short_df, "pnl_rs")
        ax.plot(s_pnl.cumsum().values, label="Short", linewidth=1.5, color="#DC2626", alpha=0.8)
    if not long_df.empty:
        l_pnl = _safe_col(long_df.sort_values("trade_date") if "trade_date" in long_df.columns else long_df, "pnl_rs")
        ax.plot(l_pnl.cumsum().values, label="Long", linewidth=1.5, color="#16A34A", alpha=0.8)
    ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
    ax.fill_between(range(len(cum_pnl)), cum_pnl.values, 0,
                    where=cum_pnl.values >= 0, alpha=0.15, color="#2563EB")
    ax.fill_between(range(len(cum_pnl)), cum_pnl.values, 0,
                    where=cum_pnl.values < 0, alpha=0.15, color="#DC2626")
    ax.set_title("Cumulative P&L (Rs.) â€” Combined / Short / Long", fontsize=14, fontweight="bold")
    ax.set_xlabel("Trade #")
    ax.set_ylabel("Cumulative P&L (Rs.)")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))
    _save(fig, "01_cumulative_pnl")

    # ========== CHART 2: Daily P&L Bar Chart ==========
    if "trade_date" in combined_sorted.columns:
        daily = combined_sorted.groupby("trade_date")["pnl_rs"].sum()
        fig, ax = plt.subplots(figsize=(14, 5))
        colors = ["#16A34A" if v >= 0 else "#DC2626" for v in daily.values]
        ax.bar(range(len(daily)), daily.values, color=colors, alpha=0.85, width=0.8)
        ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
        ax.set_title("Daily Net P&L (Rs.)", fontsize=14, fontweight="bold")
        ax.set_xlabel("Trading Day")
        ax.set_ylabel("P&L (Rs.)")
        ax.grid(True, alpha=0.3, axis="y")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))
        # Show date labels for first, middle, last
        n = len(daily)
        tick_positions = [0, n // 4, n // 2, 3 * n // 4, n - 1] if n > 5 else list(range(n))
        ax.set_xticks(tick_positions)
        ax.set_xticklabels([str(daily.index[i])[:10] for i in tick_positions], rotation=30, fontsize=8)
        _save(fig, "02_daily_pnl")

    # ========== CHART 3: Drawdown Curve ==========
    if len(cum_pnl) > 0:
        running_max = cum_pnl.cummax()
        drawdown = cum_pnl - running_max
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), height_ratios=[2, 1], sharex=True)
        ax1.plot(cum_pnl.values, linewidth=2, color="#2563EB", label="Equity Curve")
        ax1.plot(running_max.values, linewidth=1, color="#9CA3AF", linestyle="--", label="High Watermark")
        ax1.fill_between(range(len(cum_pnl)), cum_pnl.values, running_max.values, alpha=0.2, color="#DC2626")
        ax1.set_title("Equity Curve & Drawdown", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Cumulative P&L (Rs.)")
        ax1.legend(fontsize=10)
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))

        ax2.fill_between(range(len(drawdown)), drawdown.values, 0, color="#DC2626", alpha=0.4)
        ax2.plot(drawdown.values, color="#DC2626", linewidth=1)
        ax2.set_ylabel("Drawdown (Rs.)")
        ax2.set_xlabel("Trade #")
        ax2.grid(True, alpha=0.3)
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))
        plt.tight_layout()
        _save(fig, "03_drawdown_curve")

    # ========== CHART 4: Win Rate by Side ==========
    if "side" in combined_sorted.columns:
        sides_data = []
        for s_name, s_df in [("SHORT", short_df), ("LONG", long_df), ("COMBINED", combined_sorted)]:
            if s_df.empty:
                continue
            s_pnl = _safe_col(s_df, "pnl_pct")
            wins = (s_pnl > 0).sum()
            losses = (s_pnl < 0).sum()
            be = (s_pnl == 0).sum()
            total = len(s_pnl)
            wr = wins / total * 100 if total > 0 else 0
            sides_data.append({"side": s_name, "win_rate": wr, "wins": wins, "losses": losses, "breakeven": be, "total": total})

        if sides_data:
            fig, ax = plt.subplots(figsize=(10, 6))
            x_pos = range(len(sides_data))
            colors_wr = ["#DC2626", "#16A34A", "#2563EB"][:len(sides_data)]
            bars = ax.bar(x_pos, [d["win_rate"] for d in sides_data], color=colors_wr, alpha=0.85, width=0.5)
            for bar, d in zip(bars, sides_data):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                        f'{d["win_rate"]:.1f}%\n({d["wins"]}W/{d["losses"]}L/{d["breakeven"]}BE)',
                        ha="center", va="bottom", fontsize=10, fontweight="bold")
            ax.set_xticks(x_pos)
            ax.set_xticklabels([d["side"] for d in sides_data], fontsize=12)
            ax.set_ylim(0, max(d["win_rate"] for d in sides_data) + 15)
            ax.set_title("Win Rate by Side", fontsize=14, fontweight="bold")
            ax.set_ylabel("Win Rate (%)")
            ax.grid(True, alpha=0.3, axis="y")
            _save(fig, "04_win_rate_by_side")

    # ========== CHART 5: P&L Distribution Histogram (Combined) ==========
    fig, ax = plt.subplots(figsize=(12, 6))
    pnl_vals = pnl_pct.dropna()
    if len(pnl_vals) > 0:
        n_bins = min(80, max(20, len(pnl_vals) // 10))
        ax.hist(pnl_vals, bins=n_bins, color="#2563EB", alpha=0.7, edgecolor="white", linewidth=0.5)
        ax.axvline(pnl_vals.mean(), color="#DC2626", linestyle="--", linewidth=2, label=f"Mean: {pnl_vals.mean():.2f}%")
        ax.axvline(pnl_vals.median(), color="#F59E0B", linestyle="--", linewidth=2, label=f"Median: {pnl_vals.median():.2f}%")
        ax.axvline(0, color="grey", linewidth=1, linestyle="-")
        ax.set_title("P&L Distribution (%) â€” All Trades", fontsize=14, fontweight="bold")
        ax.set_xlabel("P&L (%)")
        ax.set_ylabel("Frequency")
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis="y")
    _save(fig, "05_pnl_distribution_combined")

    # ========== CHART 6: P&L Distribution by Side (Overlay) ==========
    fig, ax = plt.subplots(figsize=(12, 6))
    if not short_df.empty:
        s_pnl = _safe_col(short_df, "pnl_pct").dropna()
        if len(s_pnl) > 0:
            ax.hist(s_pnl, bins=50, color="#DC2626", alpha=0.5, edgecolor="white", linewidth=0.3, label=f"Short (Î¼={s_pnl.mean():.2f}%)")
    if not long_df.empty:
        l_pnl = _safe_col(long_df, "pnl_pct").dropna()
        if len(l_pnl) > 0:
            ax.hist(l_pnl, bins=50, color="#16A34A", alpha=0.5, edgecolor="white", linewidth=0.3, label=f"Long (Î¼={l_pnl.mean():.2f}%)")
    ax.axvline(0, color="grey", linewidth=1, linestyle="-")
    ax.set_title("P&L Distribution by Side (Overlay)", fontsize=14, fontweight="bold")
    ax.set_xlabel("P&L (%)")
    ax.set_ylabel("Frequency")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, axis="y")
    _save(fig, "06_pnl_distribution_by_side")

    # ========== CHART 7: Outcome Breakdown â€” Pie Chart ==========
    if "outcome" in combined_sorted.columns:
        outcome_counts = combined_sorted["outcome"].value_counts()
        fig, ax = plt.subplots(figsize=(8, 8))
        colors_pie = {"TARGET": "#16A34A", "SL": "#DC2626", "EOD": "#F59E0B", "TRAIL": "#6366F1"}
        pie_colors = [colors_pie.get(o, "#9CA3AF") for o in outcome_counts.index]
        wedges, texts, autotexts = ax.pie(
            outcome_counts.values, labels=outcome_counts.index, autopct="%1.1f%%",
            colors=pie_colors, startangle=90, textprops={"fontsize": 12},
        )
        for t in autotexts:
            t.set_fontweight("bold")
        ax.set_title("Trade Outcome Breakdown", fontsize=14, fontweight="bold")
        _save(fig, "07_outcome_pie")

    # ========== CHART 8: Outcome Breakdown by Side â€” Grouped Bar ==========
    if "outcome" in combined_sorted.columns and "side" in combined_sorted.columns:
        cross = pd.crosstab(combined_sorted["outcome"], combined_sorted["side"])
        fig, ax = plt.subplots(figsize=(10, 6))
        cross.plot(kind="bar", ax=ax, color=["#16A34A", "#DC2626"], alpha=0.85, edgecolor="white")
        ax.set_title("Outcome Breakdown by Side", fontsize=14, fontweight="bold")
        ax.set_xlabel("Outcome")
        ax.set_ylabel("Count")
        ax.legend(title="Side", fontsize=10)
        ax.grid(True, alpha=0.3, axis="y")
        plt.xticks(rotation=0)
        _save(fig, "08_outcome_by_side")

    # ========== CHART 9: Monthly P&L Heatmap ==========
    if "trade_date" in combined_sorted.columns:
        combined_sorted["_month"] = combined_sorted["trade_date"].dt.to_period("M")
        monthly = combined_sorted.groupby("_month")["pnl_rs"].sum()
        if len(monthly) > 1:
            fig, ax = plt.subplots(figsize=(14, 5))
            colors_monthly = ["#16A34A" if v >= 0 else "#DC2626" for v in monthly.values]
            bars = ax.bar(range(len(monthly)), monthly.values, color=colors_monthly, alpha=0.85, width=0.7)
            for bar, v in zip(bars, monthly.values):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + abs(monthly.values).max() * 0.02,
                        f"â‚¹{v:,.0f}", ha="center", va="bottom", fontsize=8, rotation=45)
            ax.set_xticks(range(len(monthly)))
            ax.set_xticklabels([str(m) for m in monthly.index], rotation=45, fontsize=9)
            ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
            ax.set_title("Monthly P&L (Rs.)", fontsize=14, fontweight="bold")
            ax.set_ylabel("P&L (Rs.)")
            ax.grid(True, alpha=0.3, axis="y")
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))
            _save(fig, "09_monthly_pnl")
        combined_sorted.drop(columns=["_month"], inplace=True, errors="ignore")

    # ========== CHART 10: Weekday P&L Analysis ==========
    if "trade_date" in combined_sorted.columns:
        combined_sorted["_weekday"] = combined_sorted["trade_date"].dt.day_name()
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        weekday_pnl = combined_sorted.groupby("_weekday")["pnl_rs"].agg(["sum", "mean", "count"])
        weekday_pnl = weekday_pnl.reindex(day_order).dropna()
        if len(weekday_pnl) > 0:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            colors_wd = ["#16A34A" if v >= 0 else "#DC2626" for v in weekday_pnl["sum"].values]
            ax1.bar(weekday_pnl.index, weekday_pnl["sum"].values, color=colors_wd, alpha=0.85)
            ax1.set_title("Total P&L by Weekday", fontsize=12, fontweight="bold")
            ax1.set_ylabel("P&L (Rs.)")
            ax1.grid(True, alpha=0.3, axis="y")
            ax1.tick_params(axis="x", rotation=30)
            ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))

            colors_wd2 = ["#16A34A" if v >= 0 else "#DC2626" for v in weekday_pnl["mean"].values]
            ax2.bar(weekday_pnl.index, weekday_pnl["mean"].values, color=colors_wd2, alpha=0.85)
            ax2.set_title("Avg P&L per Trade by Weekday", fontsize=12, fontweight="bold")
            ax2.set_ylabel("Avg P&L (Rs.)")
            ax2.grid(True, alpha=0.3, axis="y")
            ax2.tick_params(axis="x", rotation=30)
            ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))
            plt.tight_layout()
            _save(fig, "10_weekday_pnl")
        combined_sorted.drop(columns=["_weekday"], inplace=True, errors="ignore")

    # ========== CHART 11: Hourly Entry Time Distribution ==========
    if "entry_time_ist" in combined_sorted.columns:
        entry_times = pd.to_datetime(combined_sorted["entry_time_ist"], errors="coerce")
        hours = entry_times.dt.hour.dropna()
        if len(hours) > 0:
            fig, ax = plt.subplots(figsize=(12, 5))
            hour_counts = hours.value_counts().sort_index()
            ax.bar(hour_counts.index, hour_counts.values, color="#6366F1", alpha=0.85, width=0.7)
            ax.set_title("Trade Entry Distribution by Hour (IST)", fontsize=14, fontweight="bold")
            ax.set_xlabel("Hour of Day (IST)")
            ax.set_ylabel("Number of Trades")
            ax.set_xticks(range(9, 16))
            ax.grid(True, alpha=0.3, axis="y")
            _save(fig, "11_hourly_entry_distribution")

    # ========== CHART 12: Rolling Sharpe Ratio (20-trade window) ==========
    if len(pnl_pct) >= 20:
        rolling_window = 20
        rolling_mean = pnl_pct.rolling(rolling_window).mean()
        rolling_std = pnl_pct.rolling(rolling_window).std()
        rolling_sharpe = rolling_mean / rolling_std.replace(0, np.nan) * np.sqrt(252)
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(rolling_sharpe.values, linewidth=1.5, color="#6366F1", alpha=0.8)
        ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
        ax.axhline(rolling_sharpe.mean(), color="#F59E0B", linewidth=1, linestyle="--",
                   label=f"Avg: {rolling_sharpe.mean():.2f}")
        ax.fill_between(range(len(rolling_sharpe)), rolling_sharpe.values, 0,
                        where=rolling_sharpe.values >= 0, alpha=0.15, color="#16A34A")
        ax.fill_between(range(len(rolling_sharpe)), rolling_sharpe.values, 0,
                        where=rolling_sharpe.values < 0, alpha=0.15, color="#DC2626")
        ax.set_title(f"Rolling Sharpe Ratio ({rolling_window}-trade window)", fontsize=14, fontweight="bold")
        ax.set_xlabel("Trade #")
        ax.set_ylabel("Sharpe Ratio (annualized)")
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        _save(fig, "12_rolling_sharpe")

    # ========== CHART 13: Trade Duration Distribution ==========
    if {"entry_time_ist", "exit_time_ist"}.issubset(combined_sorted.columns):
        entry_t = pd.to_datetime(combined_sorted["entry_time_ist"], errors="coerce")
        exit_t = pd.to_datetime(combined_sorted["exit_time_ist"], errors="coerce")
        durations_min = (exit_t - entry_t).dt.total_seconds() / 60.0
        durations_min = durations_min.dropna()
        durations_min = durations_min[durations_min > 0]
        if len(durations_min) > 0:
            fig, ax = plt.subplots(figsize=(12, 5))
            ax.hist(durations_min, bins=min(50, max(10, len(durations_min) // 5)),
                    color="#0EA5E9", alpha=0.7, edgecolor="white")
            ax.axvline(durations_min.mean(), color="#DC2626", linestyle="--", linewidth=2,
                       label=f"Mean: {durations_min.mean():.1f} min")
            ax.axvline(durations_min.median(), color="#F59E0B", linestyle="--", linewidth=2,
                       label=f"Median: {durations_min.median():.1f} min")
            ax.set_title("Trade Duration Distribution (minutes)", fontsize=14, fontweight="bold")
            ax.set_xlabel("Duration (minutes)")
            ax.set_ylabel("Frequency")
            ax.legend(fontsize=11)
            ax.grid(True, alpha=0.3, axis="y")
            _save(fig, "13_trade_duration_dist")

    # ========== CHART 14: Top 10 Winners & Losers ==========
    if "pnl_rs" in combined_sorted.columns and "ticker" in combined_sorted.columns:
        top_win = combined_sorted.nlargest(10, "pnl_rs")[["ticker", "trade_date", "side", "pnl_rs"]]
        top_loss = combined_sorted.nsmallest(10, "pnl_rs")[["ticker", "trade_date", "side", "pnl_rs"]]
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

        if len(top_win) > 0:
            labels_w = [f"{r.ticker}\n{str(r.trade_date)[:10]}" for r in top_win.itertuples()]
            ax1.barh(range(len(top_win)), top_win["pnl_rs"].values, color="#16A34A", alpha=0.85)
            ax1.set_yticks(range(len(top_win)))
            ax1.set_yticklabels(labels_w, fontsize=9)
            ax1.set_title("Top 10 Winners (Rs.)", fontsize=12, fontweight="bold")
            ax1.set_xlabel("P&L (Rs.)")
            ax1.grid(True, alpha=0.3, axis="x")
            ax1.invert_yaxis()

        if len(top_loss) > 0:
            labels_l = [f"{r.ticker}\n{str(r.trade_date)[:10]}" for r in top_loss.itertuples()]
            ax2.barh(range(len(top_loss)), top_loss["pnl_rs"].values, color="#DC2626", alpha=0.85)
            ax2.set_yticks(range(len(top_loss)))
            ax2.set_yticklabels(labels_l, fontsize=9)
            ax2.set_title("Top 10 Losers (Rs.)", fontsize=12, fontweight="bold")
            ax2.set_xlabel("P&L (Rs.)")
            ax2.grid(True, alpha=0.3, axis="x")
            ax2.invert_yaxis()

        plt.tight_layout()
        _save(fig, "14_top_winners_losers")

    # ========== CHART 15: Cumulative Trade Count Over Time ==========
    if "trade_date" in combined_sorted.columns:
        daily_count = combined_sorted.groupby("trade_date").size().cumsum()
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(range(len(daily_count)), daily_count.values, linewidth=2, color="#6366F1")
        ax.fill_between(range(len(daily_count)), daily_count.values, alpha=0.15, color="#6366F1")
        ax.set_title("Cumulative Trade Count Over Time", fontsize=14, fontweight="bold")
        ax.set_xlabel("Trading Day")
        ax.set_ylabel("Total Trades")
        ax.grid(True, alpha=0.3)
        n_dc = len(daily_count)
        if n_dc > 5:
            tick_positions = [0, n_dc // 4, n_dc // 2, 3 * n_dc // 4, n_dc - 1]
            ax.set_xticks(tick_positions)
            ax.set_xticklabels([str(daily_count.index[i])[:10] for i in tick_positions], rotation=30, fontsize=8)
        _save(fig, "15_cumulative_trade_count")

    # ========== CHART 16: P&L by Setup / Impulse Type (if available) ==========
    for col_name, chart_num, title in [
        ("setup", "16a", "P&L by Setup Type"),
        ("impulse_type", "16b", "P&L by Impulse Type"),
    ]:
        if col_name in combined_sorted.columns:
            grp = combined_sorted.groupby(col_name)["pnl_rs"].agg(["sum", "mean", "count"])
            grp = grp.sort_values("sum", ascending=True)
            if len(grp) > 0 and len(grp) <= 20:
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(5, len(grp) * 0.5)))
                colors_setup = ["#16A34A" if v >= 0 else "#DC2626" for v in grp["sum"].values]
                ax1.barh(range(len(grp)), grp["sum"].values, color=colors_setup, alpha=0.85)
                ax1.set_yticks(range(len(grp)))
                ax1.set_yticklabels(grp.index, fontsize=9)
                ax1.set_title(f"Total {title} (Rs.)", fontsize=12, fontweight="bold")
                ax1.set_xlabel("Total P&L (Rs.)")
                ax1.grid(True, alpha=0.3, axis="x")

                colors_setup2 = ["#16A34A" if v >= 0 else "#DC2626" for v in grp["mean"].values]
                ax2.barh(range(len(grp)), grp["mean"].values, color=colors_setup2, alpha=0.85)
                ax2.set_yticks(range(len(grp)))
                ax2.set_yticklabels(grp.index, fontsize=9)
                ax2.set_title(f"Avg {title} per Trade (Rs.)", fontsize=12, fontweight="bold")
                ax2.set_xlabel("Avg P&L (Rs.)")
                ax2.grid(True, alpha=0.3, axis="x")
                plt.tight_layout()
                _save(fig, f"{chart_num}_pnl_by_{col_name}")

    # ========== CHART 17: Quality Score vs P&L Scatter (if available) ==========
    if "quality_score" in combined_sorted.columns:
        qs = pd.to_numeric(combined_sorted["quality_score"], errors="coerce")
        pnl_scatter = _safe_col(combined_sorted, "pnl_pct")
        mask = qs.notna()
        if mask.sum() > 5:
            fig, ax = plt.subplots(figsize=(10, 8))
            sides_col = combined_sorted.loc[mask, "side"].astype(str).str.upper() if "side" in combined_sorted.columns else pd.Series(["COMBINED"] * mask.sum())
            for s_name, color in [("SHORT", "#DC2626"), ("LONG", "#16A34A")]:
                s_mask = sides_col == s_name
                if s_mask.any():
                    ax.scatter(qs[mask][s_mask], pnl_scatter[mask][s_mask],
                               alpha=0.5, s=30, color=color, label=s_name, edgecolors="white", linewidth=0.3)
            # Trendline
            from numpy.polynomial.polynomial import polyfit
            valid = mask & qs.notna() & pnl_scatter.notna()
            if valid.sum() > 2:
                coeffs = np.polyfit(qs[valid], pnl_scatter[valid], 1)
                x_line = np.linspace(qs[valid].min(), qs[valid].max(), 100)
                ax.plot(x_line, np.polyval(coeffs, x_line), "--", color="#F59E0B", linewidth=2,
                        label=f"Trend (slope={coeffs[0]:.3f})")
            ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
            ax.set_title("Quality Score vs P&L (%)", fontsize=14, fontweight="bold")
            ax.set_xlabel("Quality Score")
            ax.set_ylabel("P&L (%)")
            ax.legend(fontsize=10)
            ax.grid(True, alpha=0.3)
            _save(fig, "17_quality_score_vs_pnl")

    # ========== CHART 18: Win Rate by Month ==========
    if "trade_date" in combined_sorted.columns:
        combined_sorted["_month_str"] = combined_sorted["trade_date"].dt.to_period("M").astype(str)
        months = combined_sorted["_month_str"].unique()
        wr_monthly = []
        for m in sorted(months):
            m_df = combined_sorted[combined_sorted["_month_str"] == m]
            m_pnl = _safe_col(m_df, "pnl_pct")
            wins = (m_pnl > 0).sum()
            total = len(m_pnl)
            wr_monthly.append({"month": m, "win_rate": wins / total * 100 if total > 0 else 0, "total": total})
        if len(wr_monthly) > 1:
            fig, ax = plt.subplots(figsize=(14, 5))
            wr_df = pd.DataFrame(wr_monthly)
            ax.bar(range(len(wr_df)), wr_df["win_rate"].values, color="#0EA5E9", alpha=0.85, width=0.7)
            ax.axhline(50, color="grey", linewidth=0.8, linestyle="--", label="50% line")
            for i, row in wr_df.iterrows():
                ax.text(i, row["win_rate"] + 1, f'{row["win_rate"]:.0f}%\n(n={row["total"]})',
                        ha="center", fontsize=8)
            ax.set_xticks(range(len(wr_df)))
            ax.set_xticklabels(wr_df["month"].values, rotation=45, fontsize=9)
            ax.set_title("Win Rate by Month", fontsize=14, fontweight="bold")
            ax.set_ylabel("Win Rate (%)")
            ax.set_ylim(0, 100)
            ax.legend(fontsize=10)
            ax.grid(True, alpha=0.3, axis="y")
            _save(fig, "18_win_rate_by_month")
        combined_sorted.drop(columns=["_month_str"], inplace=True, errors="ignore")

    # ========== CHART 19: Average P&L by Hour of Day ==========
    if "entry_time_ist" in combined_sorted.columns:
        entry_t = pd.to_datetime(combined_sorted["entry_time_ist"], errors="coerce")
        combined_sorted["_entry_hour"] = entry_t.dt.hour
        hourly_pnl = combined_sorted.groupby("_entry_hour")["pnl_rs"].agg(["mean", "sum", "count"])
        hourly_pnl = hourly_pnl[(hourly_pnl.index >= 9) & (hourly_pnl.index <= 15)]
        if len(hourly_pnl) > 0:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            colors_h1 = ["#16A34A" if v >= 0 else "#DC2626" for v in hourly_pnl["mean"].values]
            ax1.bar(hourly_pnl.index, hourly_pnl["mean"].values, color=colors_h1, alpha=0.85, width=0.6)
            ax1.set_title("Avg P&L per Trade by Entry Hour", fontsize=12, fontweight="bold")
            ax1.set_xlabel("Hour (IST)")
            ax1.set_ylabel("Avg P&L (Rs.)")
            ax1.grid(True, alpha=0.3, axis="y")

            colors_h2 = ["#16A34A" if v >= 0 else "#DC2626" for v in hourly_pnl["sum"].values]
            ax2.bar(hourly_pnl.index, hourly_pnl["sum"].values, color=colors_h2, alpha=0.85, width=0.6)
            ax2.set_title("Total P&L by Entry Hour", fontsize=12, fontweight="bold")
            ax2.set_xlabel("Hour (IST)")
            ax2.set_ylabel("Total P&L (Rs.)")
            ax2.grid(True, alpha=0.3, axis="y")
            ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"â‚¹{x:,.0f}"))
            plt.tight_layout()
            _save(fig, "19_avg_pnl_by_hour")
        combined_sorted.drop(columns=["_entry_hour"], inplace=True, errors="ignore")

    # ========== CHART 20: Realized Risk-Reward Scatter ==========
    stop_col = "stop_price" if "stop_price" in combined_sorted.columns else "sl_price"
    if {"entry_price", "exit_price", stop_col, "side"}.issubset(combined_sorted.columns):
        ep = pd.to_numeric(combined_sorted["entry_price"], errors="coerce")
        xp = pd.to_numeric(combined_sorted["exit_price"], errors="coerce")
        sp = pd.to_numeric(combined_sorted[stop_col], errors="coerce")
        side_col = combined_sorted["side"].astype(str).str.upper()

        # Risk = distance from entry to stop, Reward = distance from entry to exit
        risk = np.where(side_col == "SHORT", sp - ep, ep - sp)
        reward = np.where(side_col == "SHORT", ep - xp, xp - ep)
        risk = pd.to_numeric(risk, errors="coerce")
        reward = pd.to_numeric(reward, errors="coerce")

        valid_rr = (risk > 0) & pd.notna(risk) & pd.notna(reward)
        if valid_rr.sum() > 5:
            rr_ratio = reward[valid_rr] / risk[valid_rr]
            fig, ax = plt.subplots(figsize=(10, 8))
            colors_rr = np.where(reward[valid_rr] > 0, "#16A34A", "#DC2626")
            ax.scatter(risk[valid_rr], reward[valid_rr], c=colors_rr, alpha=0.5, s=25, edgecolors="white", linewidth=0.3)
            # 1:1 line
            max_val = max(risk[valid_rr].max(), abs(reward[valid_rr]).max()) * 1.1
            ax.plot([0, max_val], [0, max_val], "--", color="#9CA3AF", linewidth=1, label="1:1 R:R")
            ax.plot([0, max_val], [0, max_val * 2], "--", color="#F59E0B", linewidth=1, alpha=0.5, label="1:2 R:R")
            ax.axhline(0, color="grey", linewidth=0.8, linestyle="-")
            ax.set_title("Realized Risk vs Reward (per trade)", fontsize=14, fontweight="bold")
            ax.set_xlabel("Risk (entryâ†’stop distance)")
            ax.set_ylabel("Reward (entryâ†’exit distance)")
            ax.legend(fontsize=10)
            ax.grid(True, alpha=0.3)
            _save(fig, "20_risk_reward_scatter")

    print(f"[CHARTS] Generated {len(saved)} charts in {save_dir}/")
    return saved


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    # Outputs dir: always under the algo_trading project root
    _script_dir = Path(__file__).resolve().parent
    if _script_dir.name == "avwap_v11_refactored":
        _project_root = _script_dir.parent
    else:
        _project_root = _script_dir
    _outputs_dir = _project_root / "outputs_v15"
    _outputs_dir.mkdir(parents=True, exist_ok=True)

    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"avwap_combined_runner_{ts}.txt"

    # Tee all console output to outputs/*.txt
    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)

        try:
            print("=" * 70)
            print("AVWAP v15 COMBINED runner — V11 SHORT + V9 LONG (hybrid)")
            print("  - Entry signals: 15-min data")
            print("  - Exit resolution: 1-min data (stocks_indicators_1min_eq)")
            print("  - Outputs: */algo_trading/outputs")
            print("  - Intraday leverage: "
                  f"SHORT={INTRADAY_LEVERAGE_SHORT}x | LONG={INTRADAY_LEVERAGE_LONG}x")
            print("  - P&L% reported = ROI% on *capital/margin* (levered)")
            print("    (unlevered price-return% is saved as pnl_pct_price)")
            print("=" * 70)

            # Resolve 15-min data directory
            dir_15m = _resolve_15m_dir()
            print(f"[INFO] 15-min data directory: {dir_15m}")
            if dir_15m.is_dir():
                n_files_15m = len(list(dir_15m.glob("*_stocks_indicators_15min.parquet")))
                print(f"[INFO] 15-min parquet files found: {n_files_15m}")
            else:
                print("[WARN] 15-min data directory not found.")

            # Resolve 1-min data directory
            dir_5m = _resolve_5min_dir()
            print(f"[INFO] 1-min data directory: {dir_5m}")
            if dir_5m.is_dir():
                n_files = len(list(dir_5m.glob("*.parquet")))
                print(f"[INFO] 1-min parquet files found: {n_files}")
            else:
                print("[WARN] 1-min data directory not found â€” will fall back to 15-min exits.")

            short_cfg = default_short_config(
                reports_dir=_outputs_dir,
            )
            # V12: LONG side uses V9's avwap_common_v7_sweep config
            long_cfg = default_long_config_v9(
                reports_dir=_outputs_dir,
            )
            short_cfg.dir_15m = str(dir_15m)
            long_cfg.dir_15m = str(dir_15m)
            short_cfg.market_regime_tickers = tuple(NIFTY_CONTEXT_TICKERS)
            long_cfg.market_regime_tickers = tuple(NIFTY_CONTEXT_TICKERS)

            if ENABLE_PLAYBOOK_V11_PROFILE:
                # Frequency-focused V14 base profile:
                # loosen the stronger short branches enough to lift participation,
                # while keeping the weaker long add-ons disabled for quality.
                short_cfg.enable_liquidity_sweep_filter = False
                short_cfg.reversal_requires_sweep = True
                short_cfg.enable_avwap_no_trade_zone = False
                short_cfg.enable_mode_selector = True
                short_cfg.use_time_windows = False
                short_cfg.min_bars_left_after_entry = 0
                short_cfg.enable_ema200_filter = False
                short_cfg.require_vwap_side_persistence = False
                short_cfg.vwap_side_lookback_bars = 5
                short_cfg.vwap_side_min_count = 3
                short_cfg.require_structure_filter = False
                short_cfg.structure_lookback_bars = 30
                short_cfg.adx_min = 17.0
                short_cfg.adx_slope_min = 0.40
                short_cfg.volume_min_ratio = 0.95
                short_cfg.rsi_max_short = 62.0
                short_cfg.stochk_max = 90.0
                short_cfg.stop_pct = 0.0084
                short_cfg.target_pct = 0.01100
                short_cfg.be_trigger_pct = 0.0042
                short_cfg.trail_pct = 0.0023
                short_cfg.enable_partial_exit = True
                short_cfg.partial_exit_fraction = 0.50
                short_cfg.partial_target_fraction = 0.50
                short_cfg.enable_risk_based_position_sizing = False  # fixed Rs.50,000/trade via runner constant
                short_cfg.risk_per_trade_pct_of_capital = 0.0035
                short_cfg.max_trades_per_ticker_per_day = 5
                short_cfg.enable_topn_per_day = False
                short_cfg.topn_per_day = 0

                # Long side kept loose enough for higher participation, but
                # the weakest add-on setups are rolled back for better quality.
                long_cfg.require_entry_close_confirm = True
                long_cfg.enable_liquidity_sweep_filter = False
                long_cfg.enable_avwap_no_trade_zone = False
                long_cfg.adx_min = 17.0
                long_cfg.adx_slope_min = 0.50
                long_cfg.volume_min_ratio = 0.95
                long_cfg.rsi_min_long = 38.0
                long_cfg.stochk_min = 15.0
                long_cfg.stochk_max = 95.0
                long_cfg.atr_pct_min = 0.0025
                long_cfg.enable_setup_a_close_continuation_break = False
                long_cfg.enable_setup_b_huge_c1_close_reclaim_break = True
                long_cfg.stop_pct = 0.0075
                long_cfg.target_pct = 0.0110
                long_cfg.be_trigger_pct = 0.0055
                long_cfg.trail_pct = 0.0028
                long_cfg.min_bars_left_after_entry = 0
                long_cfg.max_vix_for_entries = 13.0
                long_cfg.max_trades_per_ticker_per_day = 4
                long_cfg.enable_topn_per_day = False
                long_cfg.topn_per_day = 0

                print("[PROFILE] V15: expanded SHORT participation + cleaned-up LONG profile.")

            # Apply per-setup signal->entry lag controls
            short_cfg.lag_bars_short_a_mod_break_c1_low = int(SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW)
            short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = int(SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW)
            short_cfg.lag_bars_short_b_huge_failed_bounce = int(SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE)
            short_cfg.enable_setup_b_huge_failed_bounce = bool(
                PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE
            )
            short_cfg.max_vix_for_entries = float(PACK2_SHORT_MAX_VIX_FOR_ENTRIES)
            long_cfg.max_vix_for_entries = float(PACK2_LONG_MAX_VIX_FOR_ENTRIES)
            long_cfg.lag_bars_long_a_mod_break_c1_high = int(LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH)
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH)
            long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)

            if FORCE_LIVE_PARITY_MIN_BARS_LEFT:
                short_cfg.min_bars_left_after_entry = 0
                long_cfg.min_bars_left_after_entry = 0

            if FORCE_LIVE_PARITY_DISABLE_TOPN:
                short_cfg.enable_topn_per_day = False
                long_cfg.enable_topn_per_day = False

            # Apply final signal-window override LAST across all profiles.
            if FINAL_SIGNAL_WINDOW_OVERRIDE:
                short_cfg.use_time_windows = bool(FINAL_SHORT_USE_TIME_WINDOWS)
                long_cfg.use_time_windows = bool(FINAL_LONG_USE_TIME_WINDOWS)
                short_cfg.signal_windows = list(FINAL_SHORT_SIGNAL_WINDOWS)
                long_cfg.signal_windows = list(FINAL_LONG_SIGNAL_WINDOWS)

            if TEST_TARGET_OVERRIDE:
                short_cfg.target_pct = TEST_SHORT_TARGET_PCT
                long_cfg.target_pct  = TEST_LONG_TARGET_PCT
                print(
                    "[TEST] Target override active: "
                    f"SHORT={_fmt_pct_exact(TEST_SHORT_TARGET_PCT)}, "
                    f"LONG={_fmt_pct_exact(TEST_LONG_TARGET_PCT)}"
                )

            # --- VIX dynamic scaling ---
            _vix_map = _load_india_vix(_project_root)
            short_cfg.vix_scale_enabled = VIX_SCALE_ENABLED
            long_cfg.vix_scale_enabled  = VIX_SCALE_ENABLED
            if VIX_SCALE_ENABLED and _vix_map:
                short_cfg.vix_daily       = _vix_map
                short_cfg.vix_baseline    = VIX_BASELINE
                short_cfg.vix_scale_min   = VIX_SCALE_MIN
                short_cfg.vix_scale_max   = VIX_SCALE_MAX
                short_cfg.vix_scale_target = VIX_SCALE_TARGET
                short_cfg.vix_scale_sl    = VIX_SCALE_SL
                long_cfg.vix_daily        = _vix_map
                long_cfg.vix_baseline     = VIX_BASELINE
                long_cfg.vix_scale_min    = VIX_SCALE_MIN
                long_cfg.vix_scale_max    = VIX_SCALE_MAX
                long_cfg.vix_scale_target = VIX_SCALE_TARGET
                long_cfg.vix_scale_sl     = VIX_SCALE_SL
                print(f"[VIX] Scaling ENABLED â€” {len(_vix_map)} daily values loaded. "
                      f"baseline={VIX_BASELINE}, range=[{VIX_SCALE_MIN}x, {VIX_SCALE_MAX}x]")
            elif VIX_SCALE_ENABLED:
                print("[VIX] Scaling ENABLED but no VIX data â€” using fixed SL/target.")
            else:
                print("[VIX] Scaling DISABLED — fixed SL/target used (old behaviour).")

            regime_map, regime_source = build_market_regime_map(short_cfg)
            if regime_map:
                short_cfg.market_regime_map = regime_map
                long_cfg.market_regime_map = regime_map
                short_cfg.enable_market_regime_filter = True
                long_cfg.enable_market_regime_filter = True
                print(f"[REGIME] Enabled market-bias filter using {regime_source}: {len(regime_map)} bars mapped.")
            else:
                short_cfg.enable_market_regime_filter = False
                long_cfg.enable_market_regime_filter = False
                regime_found, regime_missing = _describe_regime_source_availability(short_cfg)
                if regime_found:
                    print(
                        "[REGIME] Disabled: market index parquet found but no usable regime map was built. "
                        f"Found={','.join(regime_found)}"
                    )
                else:
                    print(
                        "[REGIME] Disabled: no market index parquet found in 15m data directory. "
                        f"Checked={','.join(regime_missing)}"
                    )

            print(
                f"[INFO] SHORT config: SL={_fmt_pct_exact(short_cfg.stop_pct)}, "
                f"TGT={_fmt_pct_exact(short_cfg.target_pct)}, "
                f"slippage={short_cfg.slippage_pct*10000:.0f}bps, comm={short_cfg.commission_pct*10000:.0f}bps"
            )
            print(
                f"[INFO] LONG  config: SL={_fmt_pct_exact(long_cfg.stop_pct)}, "
                f"TGT={_fmt_pct_exact(long_cfg.target_pct)}, "
                f"slippage={long_cfg.slippage_pct*10000:.0f}bps, comm={long_cfg.commission_pct*10000:.0f}bps"
            )
            print(
                "[INFO] Lag bars SHORT: "
                f"A_MOD={short_cfg.lag_bars_short_a_mod_break_c1_low}, "
                f"A_PULLBACK={short_cfg.lag_bars_short_a_pullback_c2_break_c2_low}, "
                f"B_HUGE={short_cfg.lag_bars_short_b_huge_failed_bounce}"
            )
            print(
                "[INFO] Pack2 SHORT filters: "
                f"enable_setup_B_HUGE={short_cfg.enable_setup_b_huge_failed_bounce} | "
                f"max_vix_for_entries={short_cfg.max_vix_for_entries:.2f}"
            )
            print(
                "[INFO] Lag bars LONG : "
                f"A_MOD={long_cfg.lag_bars_long_a_mod_break_c1_high}, "
                f"A_PULLBACK={long_cfg.lag_bars_long_a_pullback_c2_break_c2_high}, "
                f"B_HUGE={long_cfg.lag_bars_long_b_huge_pullback_hold_break}"
            )
            print(
                f"[INFO] Final signal-window override -> {FINAL_SIGNAL_WINDOW_OVERRIDE}"
            )
            print(
                "[INFO] SHORT windows: "
                f"use_time_windows={short_cfg.use_time_windows} | "
                + (
                    ", ".join([f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}" for a, b in short_cfg.signal_windows])
                    if short_cfg.use_time_windows
                    else "disabled"
                )
            )
            print(
                "[INFO] LONG  windows: "
                f"use_time_windows={long_cfg.use_time_windows} | "
                + (
                    ", ".join([f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}" for a, b in long_cfg.signal_windows])
                    if long_cfg.use_time_windows
                    else "disabled"
                )
            )

            short_notional = POSITION_SIZE_RS_SHORT * INTRADAY_LEVERAGE_SHORT
            long_notional = POSITION_SIZE_RS_LONG * INTRADAY_LEVERAGE_LONG
            print(
                f"[INFO] Capital/margin per trade: SHORT=Rs.{POSITION_SIZE_RS_SHORT:,.0f} | LONG=Rs.{POSITION_SIZE_RS_LONG:,.0f}"
            )
            print(
                f"[INFO] Notional exposure per trade: SHORT=Rs.{short_notional:,.0f} | LONG=Rs.{long_notional:,.0f}"
            )
            print(
                "[INFO] Final reported exit policy: TARGET / SL / EOD only | "
                f"EOD cutoff={V15_EOD_EXIT_TIME.strftime('%H:%M')} | "
                "15m/1m fallback removes residual BE outcomes"
            )
            print(f"[INFO] Live parity: min_bars_left=0 -> {FORCE_LIVE_PARITY_MIN_BARS_LEFT}")
            print(f"[INFO] Live parity: disable_topn_per_day -> {FORCE_LIVE_PARITY_DISABLE_TOPN}")
            print(
                "[INFO] Exit realism band: "
                f"enabled={EXIT_REALISM_BAND_ENABLED} | "
                f"use_stressed_base={EXIT_REALISM_USE_STRESSED_BASE} | "
                f"stop_extra_slip={STOP_EXIT_EXTRA_SLIPPAGE_BPS:.1f}bps"
            )
            print(f"[INFO] Parallelism: max_workers={MAX_WORKERS}")
            print(f"[INFO] Output directory: {_outputs_dir}")
            print(f"[INFO] Console log: {log_path}")
            print("-" * 70)

            # ---- PHASE 1: Scan for entry signals using 15-min data ----
            print("\n[PHASE 1] Scanning for entry signals using 15-min data...")
            short_df = _run_side_parallel("SHORT", short_cfg, MAX_WORKERS)
            long_df = _run_side_parallel("LONG", long_cfg, MAX_WORKERS)

            if NIFTY_CONTEXT_ENABLED:
                mode_map, nifty_ret_map, context_src, context_counts = _build_nifty_intraday_context(short_cfg)
                if mode_map:
                    both_rs_info = (
                        f" | BOTH-mode RS>={NIFTY_RS_BOTH_MODE_THRESHOLD_PCT:.2f}%"
                        if NIFTY_RS_BOTH_MODE_ENABLED else " | BOTH-mode RS=disabled"
                    )
                    print(
                        "[NIFTY_CONTEXT] "
                        f"Source={context_src} | OR_end={NIFTY_CONTEXT_OR_END_TIME.strftime('%H:%M')} | "
                        f"confirm={NIFTY_CONTEXT_CONFIRM_TIME.strftime('%H:%M')} | "
                        f"daymove>={NIFTY_CONTEXT_MIN_DAYMOVE_PCT:.2f}% | "
                        f"RS({NIFTY_RS_LOOKBACK_BARS} bars)>={NIFTY_RS_THRESHOLD_PCT:.2f}%"
                        f"{both_rs_info} | "
                        f"LONG_ONLY={int(context_counts.get('LONG_ONLY', 0))}, "
                        f"SHORT_ONLY={int(context_counts.get('SHORT_ONLY', 0))}, "
                        f"BOTH={int(context_counts.get('BOTH', 0))}"
                    )
                    short_df, long_df = _apply_nifty_intraday_context(
                        short_df,
                        long_df,
                        short_cfg,
                        mode_map,
                        nifty_ret_map,
                    )
                else:
                    print("[NIFTY_CONTEXT] Enabled but no valid NIFTY parquet found; skipped.")

            if short_df.empty and long_df.empty:
                print("[DONE] No trades found.")
                return

            # ---- PHASE 2: Re-resolve exits using 1-min data ----
            print("\n[PHASE 2] Re-resolving exits using 1-min data for higher precision...")

            # Determine 1-min file suffix by inspecting the directory
            suffix_5m = ".parquet"
            if dir_5m.is_dir():
                sample_files = list(dir_5m.glob("*"))[:5]
                for sf in sample_files:
                    if sf.suffix:
                        suffix_5m = sf.suffix
                        break

            if not short_df.empty:
                print(f"  [SHORT] {len(short_df)} trades to re-resolve...")
                short_df = _resolve_exits_5min(
                    short_df,
                    dir_5m,
                    suffix_5m,
                    short_cfg.parquet_engine,
                    eod_exit_time=V15_EOD_EXIT_TIME,
                )

            if not long_df.empty:
                print(f"  [LONG] {len(long_df)} trades to re-resolve...")
                long_df = _resolve_exits_5min(
                    long_df,
                    dir_5m,
                    suffix_5m,
                    long_cfg.parquet_engine,
                    eod_exit_time=V15_EOD_EXIT_TIME,
                )

            # ---- Apply leverage-aware P&L (capital ROI + notional rupees) ----
            if not short_df.empty:
                short_df = _add_notional_pnl(short_df)
                short_df = _sort_trades_for_output(short_df)
            if not long_df.empty:
                long_df = _add_notional_pnl(long_df)
                long_df = _sort_trades_for_output(long_df)

            combined = pd.concat([short_df, long_df], ignore_index=True)
            combined = _add_notional_pnl(combined)
            combined = _sort_trades_for_output(combined)
            short_df = _sort_trades_for_output(
                combined[combined["side"].astype(str).str.upper().eq("SHORT")].copy()
            )
            long_df = _sort_trades_for_output(
                combined[combined["side"].astype(str).str.upper().eq("LONG")].copy()
            )
            _print_day_side_mix(combined)
            _print_signal_entry_lag_summary(combined)

            # --- Comprehensive metrics ---
            print_metrics("SHORT (net of slippage+comm, 1-min exits)", compute_backtest_metrics(short_df))
            print_metrics("LONG (net of slippage+comm, 1-min exits)", compute_backtest_metrics(long_df))
            print_metrics("COMBINED (net of slippage+comm, 1-min exits)", compute_backtest_metrics(combined))
            if EXIT_REALISM_BAND_ENABLED:
                primary_variant = (
                    "pessimistic stressed path" if EXIT_REALISM_USE_STRESSED_BASE
                    else "legacy 1-min base path"
                )
                print(
                    "\n[EXIT_REALISM] Primary reported path: "
                    f"{primary_variant} (use_stressed_base={EXIT_REALISM_USE_STRESSED_BASE})."
                )
                _print_exit_realism_band("SHORT", short_df)
                _print_exit_realism_band("LONG", long_df)
                _print_exit_realism_band("COMBINED", combined)

            _print_notional_pnl(combined)
            _print_recent_daily_breakdown(combined, n_weeks=2)

            # --- Optional portfolio sim ---
            if ENABLE_CASH_CONSTRAINED_PORTFOLIO_SIM:
                sim_df, pstats = _simulate_cash_constrained(combined)
                _print_portfolio(pstats)
                combined = _sort_trades_for_output(sim_df)
                _print_day_side_mix(combined)

            # --- Save CSV ---
            out_csv = _outputs_dir / f"avwap_longshort_trades_v15_ALL_DAYS_{ts}.csv"
            combined.to_csv(out_csv, index=False)

            # --- Generate Legacy Charts (from avwap_common) ---
            print("\n[INFO] Generating legacy backtest charts...")
            chart_dir_legacy = _outputs_dir / "charts" / "legacy"
            chart_files_legacy = generate_backtest_charts(
                combined, short_df, long_df, save_dir=chart_dir_legacy, ts_label=ts,
            )
            if chart_files_legacy:
                print(f"[INFO] {len(chart_files_legacy)} legacy charts saved to {chart_dir_legacy}/")

            # --- Generate Enhanced Charts ---
            print("\n[INFO] Generating enhanced analysis charts...")
            chart_dir_enhanced = _outputs_dir / "charts" / "enhanced"
            chart_files_enhanced = generate_enhanced_charts(
                combined, short_df, long_df, save_dir=chart_dir_enhanced, ts_label=ts,
            )
            if chart_files_enhanced:
                print(f"[INFO] {len(chart_files_enhanced)} enhanced charts saved to {chart_dir_enhanced}/")
                for cf in chart_files_enhanced:
                    print(f"  -> {Path(cf).name}")
            else:
                print("[WARN] No enhanced charts generated (matplotlib may not be installed).")

            total_charts = len(chart_files_legacy or []) + len(chart_files_enhanced or [])
            print(f"\n[INFO] Total charts generated: {total_charts}")

            # --- Sample output ---
            cols = [
                c
                for c in [
                    "trade_date", "ticker", "side", "setup", "impulse_type",
                    "quality_score", "entry_price", "exit_price", "outcome",
                    "exit_resolution_case", "exit_bar_ambiguous",
                    # ROI% on capital (levered) + price-return% (unlevered)
                    "pnl_pct", "pnl_pct_price",
                    "leverage", "position_size_rs", "notional_exposure_rs",
                    "pnl_rs",
                ]
                if c in combined.columns
            ]
            print("\n=============== SAMPLE (first 30 rows) ===============")
            print(combined.head(30)[cols].to_string(index=False))
            print(f"\n[FILE SAVED] {out_csv}")
            print(f"[OUTPUTS DIR] {_outputs_dir}")
            print(f"[CONSOLE LOG] {log_path}")
            print("[DONE]")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr

    # Also print after restoring, for convenience when running interactively
    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()





