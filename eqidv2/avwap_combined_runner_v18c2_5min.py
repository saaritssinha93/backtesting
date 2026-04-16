# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v18c2_5min.py - AVWAP v18c2 COMBINED runner on 5-minute signals
===================================================================================

V18c = V18b + 7 targeted improvements to push trades/day from ~3 → ~5–7
---------------------------------------------------------------------------

CHANGES FROM V18b
-----------------
1. EXPANDED SIGNAL WINDOWS (both sides):
   SHORT: 09:20-11:30, 12:00-14:45   (was 09:30-11:00, 13:00-14:30)
   LONG:  09:20-11:30, 11:15-12:45 (bridge), 12:00-14:45
   — 09:20 captures the first 5-min AVWAP break of the day (was excluded)
   — Bridge window 11:15-12:45 LONG only: midday AVWAP reclaim after morning run
   — Bridge signals (11:30-12:00) classified to morning shortlist; overlap (12:00-12:45)
     classified to afternoon shortlist
   — Extended afternoon end 13:30→14:45 adds the pre-close continuation zone

2. NIFTY BOTH-MODE LONG RS THRESHOLD relaxed: 0.65% → 0.50%
   — RS 0.50-0.65% bucket had ~70% day-win in V16 data; was being excluded on flat days
   — SHORT BOTH-mode threshold unchanged at 0.65%

3. V16 LONG AVWAP DEAD ZONE narrowed:
   1.10-1.35 ATR → 1.05-1.20 ATR  (recovers the 1.20-1.35 range, marginal not dead)
   — Removed 216 trades in v18b; tighter band loses fewer good entries

4. V16 LONG QS ABSOLUTE MAX raised: 10.0 → 11.0
   — QS 10.0-11.0 is borderline (not the 30%-win exhausted zone; that starts ~11+)
   — Recovers a segment of high-setup-quality HUGE reclaim trades

5. V17C SHORT CONTINUATION GATES relaxed (all-AND chain was killing 71.7% of SHORTs):
   close_bottom_frac  : 0.25 → 0.32
   opposing_wick_frac : 0.20 → 0.28
   range_expansion    : 1.05 → 0.95  (allow slight compression — trend momentum enough)
   volume_min_ratio   : 0.90 → 0.75
   adx_slope_min      : 0.40 → 0.25

6. SHORT TARGET PCT raised: 0.80% → 0.95%
   — R:R improves from 1.07x to 1.27x; if target hit-rate drops 63%→58%, PF still +20%
   — Makes short side structurally more robust to small hit-rate variation

7. LONG CLOSE CONTINUATION BREAK setup enabled: False → True
   — Required for bridge window A_MOD_CLOSE_CONTINUATION_BREAK signals
   — Already confirmed in V18a (42 trades, PF 1.67)

SHORTLIST REBUILD REQUIRED FOR FULL VOLUME GAIN:
   Re-run two_session_shortlist_history_builder.py with:
   V18C_SHORTLIST_CONFIG = {
       'morning':   {'rvol_min': 0.70, 'orp_atr_min': 0.07, 'clv_min': 0.55, 'tv_min_rs': 2_000_000},
       'afternoon': {'rvol_min': 0.60, 'orp_atr_min': 0.04, 'clv_min': 0.45, 'tv_min_rs': 1_500_000},
   }
   Expected: ~1.5-1.6x more shortlisted tickers per session.

Earlier changes inherited from v18b:
1. All outputs saved to outputs_v18c2_5min/
2. Entry signals: 5-min data; exits: intrabar data (1-min when available, else 5-min)
3. Expanded charting suite
4. Normal Python imports (no importlib hacks)
5. Unified Trade dataclass -- both sides produce identical columns
6. Parallel ticker scanning via ProcessPoolExecutor
7. Slippage + commission model baked into P&L
8. Comprehensive backtest metrics (Sharpe, Sortino, Calmar, drawdown, profit factor)
9. All config via StrategyConfig dataclass -- no module-level globals
10. Cash-constrained portfolio sim uses itertuples() instead of iterrows()

Usage:
    python avwap_combined_runner_v18c_5min.py
"""

from __future__ import annotations

import heapq
import multiprocessing as mp
import os
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import DATA_5M_DIR as RUNTIME_DATA_5M_DIR
from eqidv2_runtime_paths import DATA_1MIN_DIR as RUNTIME_DATA_1MIN_DIR
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR
from eqidv2_runtime_paths import runtime_dir

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


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)

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
    prepare_session_bars_for_scan,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker as scan_short,
    scan_all_days_for_ticker_prepared as scan_short_prepared,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker as scan_long,
    scan_all_days_for_ticker_prepared as scan_long_prepared,
)
from avwap_v11_refactored.avwap_common_v7_sweep_v15 import (
    default_long_config as default_long_config_v9,
)
import avwap_v11_refactored.avwap_short_strategy_v11 as short_mod
import avwap_v11_refactored.avwap_long_strategy_v9_sweep as long_mod

try:
    from filtered_stocks_two_session_history_v1 import (
        get_shortlist as get_two_session_shortlist,
        get_shortlist_entries as get_two_session_shortlist_entries,
    )
except ImportError:
    from filtered_stocks_two_session_history_v1 import get_shortlist as get_two_session_shortlist

    get_two_session_shortlist_entries = None


# ===========================================================================
# RUNNER CONFIG (top-level orchestration settings)
# ===========================================================================
POSITION_SIZE_RS_SHORT = 20_000
POSITION_SIZE_RS_LONG = 20_000

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

# If True, replace the current IST trading day's backtest entries with the
# exact V15 live-slot replay path used by the live shard scanners.
# Disable for the 5-minute variant because live replay inputs are 15-minute.
SYNC_CURRENT_DAY_WITH_LIVE_PARITY = False

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

# Final signal-window override — V18c expanded windows.
# SHORT: 09:20-11:30 (was 09:30-11:30) + 12:00-14:45 (was 13:00-14:45)
# LONG:  09:20-11:30 + bridge 11:15-12:45 (NEW) + 12:00-14:45
FINAL_SIGNAL_WINDOW_OVERRIDE = True
FINAL_SHORT_USE_TIME_WINDOWS = True
FINAL_SHORT_SIGNAL_WINDOWS = [
    (dtime(9, 20, 0), dtime(11, 30, 0)),   # V18c: earlier start captures first AVWAP break
    (dtime(12, 0, 0), dtime(14, 45, 0)),   # V18c: full afternoon including 12:00-13:00 gap
]
FINAL_LONG_USE_TIME_WINDOWS = True
FINAL_LONG_SIGNAL_WINDOWS = [
    (dtime(9, 20, 0), dtime(11, 30, 0)),   # V18c: earlier start
    (dtime(11, 15, 0), dtime(12, 45, 0)),  # V18c NEW: midday AVWAP reclaim bridge (LONG only)
    (dtime(12, 0, 0), dtime(14, 45, 0)),   # V18c: full afternoon
]
# Session classification windows for shortlist gating.
# Bridge zone (11:30-12:00) maps to morning shortlist — premarket context still dominant.
V18_MORNING_SESSION_WINDOW   = (dtime(9, 20, 0), dtime(11, 30, 0))
V18C_BRIDGE_ZONE_END         = dtime(12, 0, 0)   # 11:30-12:00 → morning shortlist
V18_AFTERNOON_SESSION_WINDOW = (dtime(12, 0, 0), dtime(14, 45, 0))
V15_EOD_EXIT_TIME = dtime(15, 20, 0)

# ---------------------------------------------------------------------------
# V18b staged rollout controls
# Stage 1: fast prebuilt shortlist + later short participation
# Stage 2: mild core-gate relaxation
# Stage 3: enable session-native continuation expansion
# Stage 4: rank-aware shortlist rescue / tiering
# ---------------------------------------------------------------------------
V18B_SHORT_PRIMARY_SHORTLIST_SIDE = "short"
V18B_SHORT_EXPANDED_SHORTLIST_SIDE = "combined"
V18B_LONG_PRIMARY_SHORTLIST_SIDE = "long"
V18B_LONG_EXPANDED_SHORTLIST_SIDE = "combined"
V18B_ENABLE_AFTERNOON_MORNING_CARRY = False
V18B_SHORT_CARRY_MAX_RANK_PCT = 0.45
V18B_LONG_CARRY_MAX_RANK_PCT = 0.35

V18B_TIER1_MAX_RANK_PCT = 0.20
V18B_TIER2_MAX_RANK_PCT = 0.55
V18B_SHORT_COMBINED_MAX_RANK_PCT = 1.00
V18B_LONG_COMBINED_RESCUE_MAX_RANK_PCT = 0.55
V18B_LONG_COMBINED_RESCUE_MIN_QS = 4.0

V18B_SHORT_TIER3_ALLOWED_SETUPS = {
    "A_MOD_BREAK_C1_LOW",
    "C_SHORT_CONTINUATION_BREAK",
    "B_HUGE_RED_FAILED_BOUNCE",
}
V18B_LONG_COMBINED_RESCUE_SETUPS = {
    "A_MOD_BREAK_C1_HIGH",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
    "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK",
}

V18B_SHORT_ADX_MIN = 28.0
V18B_SHORT_VOLUME_MIN_RATIO = 0.80
V18B_SHORT_RSI_MAX = 60.0
V18B_SHORT_ENTRY_CUTOFF = dtime(14, 45, 0)
V18B_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = 2.10

V18B_LONG_ADX_MIN = 22.0
V18B_LONG_VOLUME_MIN_RATIO = 0.80
V18B_LONG_RSI_MIN = 50.0
V18B_LONG_QS_MIN = 4.0

# V18c2 cleanup package:
# optimizer-backed trimming of the weak micro-regimes that inflated v18c variants
# trade count without adding enough edge.
V18C2_ENABLE_SHORT_PULLBACK = False
V18C2_ENABLE_LONG_CLOSE_CONTINUATION = False
V18C2_SHORT_CONT_LATE_CUTOFF = dtime(13, 45, 0)
V18C2_LONG_RESCUE_MAX_TIER = 1
V18C2_LONG_LATE_A_MOD_CUTOFF = dtime(13, 45, 0)
V18C2_LONG_LATE_A_MOD_BLOCK_TIERS = {1, 3}
V18C2_MID_START = dtime(10, 30, 0)
V18C2_MID_END = dtime(11, 59, 59)
V18C2_PM_START = dtime(12, 0, 0)
V18C2_PM_END = dtime(13, 44, 59)
V18C2_EARLY_END = dtime(10, 29, 59)

# Per-setup signal->entry lag (in 5-min bars for this runner).
# Edit these to manually control (entry_time_ist - signal_time_ist) behavior.
# HUGE setup: use -1 for legacy dynamic "first valid bar" behavior.
SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW = 1
SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW = int(
    os.getenv("EQIDV17C_SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW", "2")
)
SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE = -1
LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH = 1
LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH = 2
# Re-enable HUGE pullback-hold long setup cautiously in v17c.
LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK = 1
LONG_LAG_BARS_B_HUGE_C1_CLOSE_RECLAIM_BREAK = 2

PORTFOLIO_START_CAPITAL_RS = 1_000_000
DISALLOW_BOTH_SIDES_SAME_TICKER_DAY = False   # V16 Run3: allow both sides (volume increase)

# Parallelism:
# - v18b backtests are intentionally aggressive about worker fan-out
# - default to the full visible CPU count for faster scan completion
# - prefer process pools by default; env vars can still override this
DEFAULT_MAX_WORKERS = 16
MAX_WORKERS = max(
    1,
    int(
        os.getenv(
            "EQIDV18B_5MIN_MAX_WORKERS",
            os.getenv("EQIDV17C_5MIN_MAX_WORKERS", str(DEFAULT_MAX_WORKERS)),
        )
    ),
)
EXECUTOR_MODE = str(
    os.getenv(
        "EQIDV18B_5MIN_EXECUTOR",
        os.getenv("EQIDV17C_5MIN_EXECUTOR", "process"),
    )
).strip().lower()
ENABLE_LEGACY_CHARTS = _env_flag("EQIDV17C_5MIN_ENABLE_LEGACY_CHARTS", True)
ENABLE_ENHANCED_CHARTS = _env_flag("EQIDV17C_5MIN_ENABLE_ENHANCED_CHARTS", True)

# Setup controls
# Disable the weak short HUGE failed-bounce branch in V14.
PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE = True   # V16 Run6: enabled — adds HUGE failed bounce shorts
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
NIFTY_CONTEXT_OR_END_TIME = dtime(9, 20, 0)    # v17c: first completed 5-min bar only, no lookahead
NIFTY_CONTEXT_CONFIRM_TIME = dtime(9, 20, 0)   # v17c: earlier confirmation
NIFTY_CONTEXT_MIN_DAYMOVE_PCT = 0.25           # v18b tuned: widen eligible days modestly
NIFTY_RS_FILTER_ENABLED = True
NIFTY_RS_LOOKBACK_BARS = 4                     # v15: 60-min window (was 3 bars/45min)
NIFTY_RS_THRESHOLD_PCT = 0.20                  # v15: raised from 0.15 (above round-trip cost)
# v15 NEW: apply BOTH-mode RS filtering with a moderately strict short threshold.
NIFTY_RS_BOTH_MODE_ENABLED = True
NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT = 0.50   # V18c: relaxed 0.55→0.50; RS 0.50-0.55% = ~70% day-win
NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT = 0.65  # V18c: SHORT kept stricter than LONG on flat days
# Backward-compatibility alias for older live-parity callers that still expect
# one shared BOTH-mode threshold constant.
NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT

# =======================================================================
# V17c CONTEXT / SETUP UPGRADES
# =======================================================================
V17C_RS_ATR_NORM_ENABLED = True
V17C_RS_ATR_NORM_THRESH_SHORT_BOTH = _env_float(
    "EQIDV17C_RS_ATR_NORM_THRESH_SHORT_BOTH", 0.50
)
V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT = _env_float(
    "EQIDV17C_RS_ATR_DIRECTIONAL_THRESH_SHORT", 0.35
)
V17C_RS_ATR_FALLBACK_PCT_SHORT = _env_float(
    "EQIDV17C_RS_ATR_FALLBACK_PCT_SHORT", 0.40
)
V17C_RS_ATR_CONTINUATION_THRESH_SHORT = _env_float(
    "EQIDV17C_RS_ATR_CONTINUATION_THRESH_SHORT", 0.50
)
V17C_LONG_HUGE_PULLBACK_RS_BOTH_PCT = _env_float(
    "EQIDV17C_LONG_HUGE_PULLBACK_RS_BOTH_PCT", 0.90
)
V17C_LONG_HUGE_PULLBACK_RS_DIRECTIONAL_PCT = _env_float(
    "EQIDV17C_LONG_HUGE_PULLBACK_RS_DIRECTIONAL_PCT", 0.35
)
V17C_LONG_STRONG_CLOSE_MAX_FRACTION = _env_float(
    "EQIDV17C_LONG_STRONG_CLOSE_MAX_FRACTION", 0.20
)
V17C_SHORT_STRONG_CLOSE_MAX_FRACTION = _env_float(
    "EQIDV17C_SHORT_STRONG_CLOSE_MAX_FRACTION", 0.32  # V18c: relaxed 0.25→0.32
)
V17C_OPPOSING_WICK_MAX_FRACTION = _env_float(
    "EQIDV17C_OPPOSING_WICK_MAX_FRACTION", 0.28       # V18c: relaxed 0.20→0.28
)
V17C_RANGE_EXPANSION_MIN = _env_float(
    "EQIDV17C_RANGE_EXPANSION_MIN", 0.95              # V18c: relaxed 1.05→0.95 (allow slight compression)
)
V17C_HUGE_PULLBACK_ADX_SLOPE_MIN = _env_float(
    "EQIDV17C_HUGE_PULLBACK_ADX_SLOPE_MIN", 0.60
)
V17C_HUGE_PULLBACK_RSI_MIN = _env_float(
    "EQIDV17C_HUGE_PULLBACK_RSI_MIN", 54.0
)
V17C_HUGE_PULLBACK_VOL_RATIO_MIN = _env_float(
    "EQIDV17C_HUGE_PULLBACK_VOL_RATIO_MIN", 1.05
)
V17C_HUGE_PULLBACK_AVWAP_DIST_ATR_MAX = _env_float(
    "EQIDV17C_HUGE_PULLBACK_AVWAP_DIST_ATR_MAX", 2.10
)
V17C_SHORT_CONTINUATION_ENABLED = _env_flag(
    "EQIDV17C_SHORT_CONTINUATION_ENABLED", True
)
V17C_SHORT_CONTINUATION_ADX_MIN = _env_float(
    "EQIDV17C_SHORT_CONTINUATION_ADX_MIN", 26.0
)
V17C_SHORT_CONTINUATION_ADX_SLOPE_MIN = _env_float(
    "EQIDV17C_SHORT_CONTINUATION_ADX_SLOPE_MIN", 0.25  # V18c: relaxed from v18b value
)
V17C_SHORT_CONTINUATION_VOLUME_MIN_RATIO = _env_float(
    "EQIDV17C_SHORT_CONTINUATION_VOLUME_MIN_RATIO", 0.75  # V18c: keep at 0.75 (already relaxed)
)
V17C_SHORT_CONTINUATION_AVWAP_DIST_ATR_MAX = _env_float(
    "EQIDV17C_SHORT_CONTINUATION_AVWAP_DIST_ATR_MAX", 2.25
)
V17C_SHORT_CONTINUATION_ATR_PCT_MAX = _env_float(
    "EQIDV17C_SHORT_CONTINUATION_ATR_PCT_MAX", 0.0060
)
V17C_SHORT_CONTINUATION_ENTRY_CUTOFF = dtime(14, 45, 0)
V17C_SHORT_ADX_MIN = _env_float("EQIDV17C_SHORT_ADX_MIN", 30.0)
V17C_SHORT_VOLUME_MIN_RATIO = _env_float("EQIDV17C_SHORT_VOLUME_MIN_RATIO", 0.90)
V17C_SHORT_RSI_MAX = _env_float("EQIDV17C_SHORT_RSI_MAX", 58.0)
V17C_SHORT_STOP_PCT = _env_float("EQIDV17C_SHORT_STOP_PCT", 0.0075)
V17C_SHORT_TARGET_PCT = _env_float("EQIDV17C_SHORT_TARGET_PCT", 0.0095)  # V18c: raised 0.80%→0.95% — R:R 1.07x→1.27x
V17C_SHORT_ENABLE_BREAKEVEN = _env_flag("EQIDV17C_SHORT_ENABLE_BREAKEVEN", False)
V17C_SHORT_BE_TRIGGER_PCT = _env_float("EQIDV17C_SHORT_BE_TRIGGER_PCT", 0.0060)
V17C_SHORT_ENABLE_TRAILING_STOP = _env_flag("EQIDV17C_SHORT_ENABLE_TRAILING_STOP", True)
V17C_SHORT_TRAIL_PCT = _env_float("EQIDV17C_SHORT_TRAIL_PCT", 0.0035)
V17C_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = _env_float("EQIDV17C_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX", 2.00)

# Strict Wave 2 short-quality gates.
V15_SHORT_ENTRY_CUTOFF = V18B_SHORT_ENTRY_CUTOFF
V15_SHORT_MIN_OPENING_RANGE_WIDTH_PCT = 1.00
V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = 2.10

# ===========================================================================
# V16 ANTI-EXHAUSTION POST-SCAN FILTERS
# Applied after Nifty RS filter, before portfolio simulation.
# These are the core V16 additions over V15.
# ===========================================================================
# SHORT RSI dead zone: specifically 35-40 (38.5% win) — NOT all RSI<40 (RSI<35 = 66.7% win, keep!)
V16_SHORT_RSI_DEAD_ZONE_LO  = 37.0  # v18b tuned: keep only the weakest short pocket blocked
V16_SHORT_RSI_DEAD_ZONE_HI  = 38.5
# LONG QS: two-band filter — block the 7.5-8.0 dead zone (50% win) AND the >10.0 exhausted zone (30% win)
# QS 8.0-10.0 is ALLOWED (75% win confirmed in V15R3 data — Run2 incorrectly blocked all QS>7.5)
V16_LONG_QS_DEAD_LO         = 7.80  # narrow dead zone (only the weakest QS pocket)
V16_LONG_QS_DEAD_HI         = 7.95
V16_LONG_QS_ABS_MAX         = 11.0  # V18c: raised 10.0→11.0 — QS 10-11 is borderline, not exhausted
V16_LONG_AVWAP_DIST_ATR_MAX = 3.25  # allow slightly wider but still controlled chase distance
V16_SHORT_RS_EXHAUSTION_CAP = -2.0  # RS exhaustion guard applied via NIFTY_RS threshold
# Opening Range (OR) gate: require price to have broken the opening range before entry.
# Run11: SHORT OR gate disabled (minimal benefit, killed 90 trades); LONG kept (PF +24.9%, MaxDD halved)
# Run11: OR narrowed to first bar only (09:15 candle) — narrower range, more entries qualify
V16_OR_GATE_ENABLED        = False      # Run12: disabled — OR gate was blocking high-quality LONG trades (PF 1.669->1.773, DayWin 64.7%->80.0%)
V16_OR_GATE_SHORT_ENABLED  = False      # Run11: disabled for SHORT (marginal PF gain not worth -90 trades)
V16_OR_GATE_FIRST_BAR_ONLY = True       # Run11: use only 09:15 bar for OR (not 09:15-09:30 full range)
V16_OR_GATE_TIME           = dtime(9, 30, 0)   # upper bound for OR bar selection (bars with time < this)
# LONG AVWAP dead zone: 1.0-1.5 ATR band = break-even (59% TGT, avg +0.16%) — 39 trades wasted in Run7
V16_LONG_AVWAP_DIST_DEAD_LO = 1.05  # V18c: tightened 1.15→1.05 — only block the true dead sub-band
V16_LONG_AVWAP_DIST_DEAD_HI = 1.20  # V18c: tightened 1.25→1.20 — recover 1.20-1.25 range
# Run14: Volume exhaustion filter — climax buying at entry = reject
# Reject LONG if entry bar volume > N x day-average AND entry is >= M bars from open
# Backtest result: vol>4x(bars>=3) filters 42 trades (23W/19L), LONG PF 1.776->1.961, DayWin 79.5%->81.6%, PnL flat
V16_LONG_ENTRY_VOL_EXHAUST_ENABLED  = True
V16_LONG_ENTRY_VOL_EXHAUST_MULT     = 6.0  # v18b tuned: keep extreme climaxes blocked, admit more valid momentum
V16_LONG_ENTRY_VOL_EXHAUST_MIN_BARS = 3    # only apply when entry is >= 3 bars from open (not OR-bar entries)


def _enrich_with_or_levels(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    dir_15m: str,
    parquet_suffix: str = "_stocks_indicators_5min.parquet",
) -> tuple:
    """
    Compute the Opening Range (OR) high/low for each (ticker, trade_date) and merge
    into short_df and long_df as columns: or_high, or_low.

    OR is defined as the high/low of all 5-min bars whose timestamp is STRICTLY
    before V16_OR_GATE_TIME (09:30).  For this runner that means the single 09:15
    bar (the first 5-min candle of the day, closing at 09:20 in NSE data).

    Only called when V16_OR_GATE_ENABLED is True.
    """
    import pathlib

    dir_path = pathlib.Path(dir_15m)
    if not dir_path.is_dir():
        print(f"[OR_ENRICH] dir_15m not found ({dir_15m}); skipping OR enrichment.")
        for df in (short_df, long_df):
            if not df.empty:
                df["or_high"] = float("nan")
                df["or_low"]  = float("nan")
        return short_df, long_df

    # Collect all (ticker, date) pairs we need OR levels for
    def _needed_pairs(df):
        if df.empty:
            return set()
        tickers = df["ticker"].unique() if "ticker" in df.columns else []
        dates   = df["trade_date"].unique() if "trade_date" in df.columns else []
        return {(t, d) for t in tickers for d in dates}

    all_pairs = _needed_pairs(short_df) | _needed_pairs(long_df)
    if not all_pairs:
        return short_df, long_df

    # Load parquet per ticker, extract OR levels for relevant dates
    or_records: list[dict] = []
    tickers_needed = {t for t, _ in all_pairs}
    for ticker in tickers_needed:
        fpath = dir_path / f"{ticker}{parquet_suffix}"
        if not fpath.exists():
            continue
        try:
            pq = pd.read_parquet(str(fpath))
        except Exception:
            continue
        # Normalise datetime index / column
        if isinstance(pq.index, pd.DatetimeIndex):
            pq = pq.reset_index().rename(columns={pq.index.name or "index": "datetime"})
        elif "datetime" not in pq.columns:
            dt_candidates = [c for c in pq.columns if "time" in c.lower() or "date" in c.lower()]
            if dt_candidates:
                pq = pq.rename(columns={dt_candidates[0]: "datetime"})
            else:
                continue
        pq["datetime"] = pd.to_datetime(pq["datetime"], errors="coerce")
        pq = pq.dropna(subset=["datetime"])
        pq["_date"] = pq["datetime"].dt.date
        pq["_time"] = pq["datetime"].dt.time

        # Dates needed for this ticker
        dates_needed = {d for t, d in all_pairs if t == ticker}
        for raw_date in dates_needed:
            try:
                d = pd.Timestamp(raw_date).date() if not isinstance(raw_date, type(pd.Timestamp("2000-01-01").date())) else raw_date
            except Exception:
                d = raw_date
            day_bars = pq[pq["_date"] == d]
            if V16_OR_GATE_FIRST_BAR_ONLY:
                # Run11: only the 09:15 candle (first 5-min bar) — narrower OR, more entries qualify
                or_bars = day_bars[day_bars["_time"] == dtime(9, 15, 0)]
                if or_bars.empty:
                    # fallback: any bar before OR gate time
                    or_bars = day_bars[day_bars["_time"] < V16_OR_GATE_TIME]
            else:
                or_bars  = day_bars[day_bars["_time"] < V16_OR_GATE_TIME]
            if or_bars.empty:
                continue
            hi_col = next((c for c in ("high", "High", "HIGH") if c in or_bars.columns), None)
            lo_col = next((c for c in ("low",  "Low",  "LOW")  if c in or_bars.columns), None)
            if hi_col is None or lo_col is None:
                continue
            or_records.append({
                "ticker":    ticker,
                "trade_date": raw_date,
                "or_high":   float(or_bars[hi_col].max()),
                "or_low":    float(or_bars[lo_col].min()),
            })

    if not or_records:
        print("[OR_ENRICH] No OR levels computed (empty parquet data or no matching bars).")
        for df in (short_df, long_df):
            if not df.empty:
                df["or_high"] = float("nan")
                df["or_low"]  = float("nan")
        return short_df, long_df

    or_df = pd.DataFrame(or_records)
    print(f"[OR_ENRICH] Computed OR levels for {len(or_df)} (ticker, date) pairs.")

    def _merge_or(df):
        if df.empty:
            return df
        df = df.copy()
        df = df.merge(or_df[["ticker", "trade_date", "or_high", "or_low"]], on=["ticker", "trade_date"], how="left")
        return df

    short_df = _merge_or(short_df)
    long_df  = _merge_or(long_df)
    return short_df, long_df


def _enrich_with_entry_vol_ratio(
    long_df: pd.DataFrame,
    dir_15m: str,
    parquet_suffix: str = "_stocks_indicators_5min.parquet",
) -> pd.DataFrame:
    """
    For each LONG trade, load the 5-min parquet for that (ticker, date) and compute:
      entry_bar_vol_ratio  — entry bar volume / day-average volume
      bars_from_open       — index of the entry bar (0 = first bar of day)

    The entry bar is the first 5-min bar whose high >= entry_price * 0.999.
    These columns are used by the volume exhaustion filter in _apply_v16_post_scan_filters.

    Only called when V16_LONG_ENTRY_VOL_EXHAUST_ENABLED is True.
    """
    import pathlib
    import numpy as np

    if long_df.empty:
        long_df = long_df.copy()
        long_df["entry_bar_vol_ratio"] = np.nan
        long_df["bars_from_open"]      = np.nan
        return long_df

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
                df = pd.read_parquet(f)
                df["date"] = pd.to_datetime(df["date"])
            except Exception:
                _5m_cache[key] = pd.DataFrame()
                return _5m_cache[key]
            day = df[df["date"].dt.strftime("%Y-%m-%d") == date_str].reset_index(drop=True)
            _5m_cache[key] = day
        return _5m_cache[key]

    ratios   = []
    bar_idxs = []

    for _, row in long_df.iterrows():
        ticker  = str(row.get("ticker", ""))
        date_s  = str(row.get("trade_date", ""))[:10]
        try:
            entry_px = float(row.get("entry_price", 0))
        except (ValueError, TypeError):
            entry_px = 0.0

        day = _get_day(ticker, date_s)
        if day.empty or entry_px <= 0:
            ratios.append(np.nan)
            bar_idxs.append(np.nan)
            continue

        avg_vol = day["volume"].mean()
        hits = day[day["high"] >= entry_px * 0.999]
        if hits.empty or avg_vol <= 0:
            ratios.append(np.nan)
            bar_idxs.append(np.nan)
            continue

        entry_bar_vol = float(hits.iloc[0]["volume"])
        ratios.append(entry_bar_vol / avg_vol)
        bar_idxs.append(int(hits.index[0]))

    long_df = long_df.copy()
    long_df["entry_bar_vol_ratio"] = ratios
    long_df["bars_from_open"]      = bar_idxs
    print(f"[VOL_ENRICH] Computed entry vol ratio for {long_df['entry_bar_vol_ratio'].notna().sum()}/{len(long_df)} LONG trades.")
    return long_df


def _apply_v16_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> tuple:
    """
    V16 anti-exhaustion post-scan filters.
    Applied AFTER Nifty context filter, BEFORE portfolio sim.

    SHORT: blocks ONLY RSI 35-40 dead zone (38.5% win) — keeps RSI<35 (66.7% win) and RSI>40 (75%+ win)
    LONG:  quality_score two-band filter:
             block QS 7.5-8.0 (50% win dead zone)
             allow QS 8.0-10.0 (75% win — was wrongly blocked in Run2)
             block QS > 10.0  (30% win exhausted momentum zone)
    LONG:  avwap_dist_atr cap ≤ 3.0 (dist 2.5-3.0 = 92% win allowed; 3.0+ = 50% win blocked)
    """
    short_before = len(short_df)
    long_before  = len(long_df)

    # --- SHORT: block ONLY the RSI 35-40 dead zone (not all RSI<40) ---
    short_rsi_removed = 0
    if not short_df.empty and "rsi_signal" in short_df.columns:
        rsi_col = pd.to_numeric(short_df["rsi_signal"], errors="coerce").fillna(-1.0)
        mask_dead_rsi = (rsi_col >= V16_SHORT_RSI_DEAD_ZONE_LO) & (rsi_col < V16_SHORT_RSI_DEAD_ZONE_HI)
        short_rsi_removed = int(mask_dead_rsi.sum())
        short_df = short_df[~mask_dead_rsi].copy()

    # --- LONG: quality score two-band filter ---
    # Block QS 7.5-8.0 (50% win dead zone) AND QS>10.0 (30% win exhausted zone)
    # Allow QS 8.0-10.0 (75% win — V15R3 confirmed: QS 8-9 = 75%, QS 9-10 = 75%)
    long_qs_removed = 0
    if not long_df.empty and "quality_score" in long_df.columns:
        qs_col = pd.to_numeric(long_df["quality_score"], errors="coerce").fillna(0.0)
        mask_bad_qs = (
            ((qs_col > V16_LONG_QS_DEAD_LO) & (qs_col <= V16_LONG_QS_DEAD_HI))
            | (qs_col > V16_LONG_QS_ABS_MAX)
        )
        long_qs_removed = int(mask_bad_qs.sum())
        long_df = long_df[~mask_bad_qs].copy()

    # --- LONG: AVWAP distance cap (anti-chase) ---
    long_dist_removed = 0
    if not long_df.empty and "avwap_dist_atr_signal" in long_df.columns:
        dist_col = pd.to_numeric(long_df["avwap_dist_atr_signal"], errors="coerce").fillna(0.0)
        mask_too_far = (dist_col > 0) & (dist_col > V16_LONG_AVWAP_DIST_ATR_MAX)
        long_dist_removed = int(mask_too_far.sum())
        long_df = long_df[~mask_too_far].copy()

    # --- LONG: AVWAP dead zone (1.0-1.5 ATR = break-even, 59% TGT) ---
    long_avwap_dead_removed = 0
    if not long_df.empty and "avwap_dist_atr_signal" in long_df.columns:
        dist_col2 = pd.to_numeric(long_df["avwap_dist_atr_signal"], errors="coerce").fillna(0.0)
        mask_dead_dist = (dist_col2 >= V16_LONG_AVWAP_DIST_DEAD_LO) & (dist_col2 < V16_LONG_AVWAP_DIST_DEAD_HI)
        long_avwap_dead_removed = int(mask_dead_dist.sum())
        long_df = long_df[~mask_dead_dist].copy()

    # --- LONG: Volume exhaustion filter (Run14) — climax buying at entry bar = reject ---
    long_vol_exhaust_removed = 0
    if V16_LONG_ENTRY_VOL_EXHAUST_ENABLED and not long_df.empty and "entry_bar_vol_ratio" in long_df.columns:
        vr_col  = pd.to_numeric(long_df["entry_bar_vol_ratio"], errors="coerce")
        bfo_col = pd.to_numeric(long_df["bars_from_open"],      errors="coerce")
        mask_exhaust = (
            vr_col.notna()
            & (vr_col > V16_LONG_ENTRY_VOL_EXHAUST_MULT)
            & (bfo_col >= V16_LONG_ENTRY_VOL_EXHAUST_MIN_BARS)
        )
        long_vol_exhaust_removed = int(mask_exhaust.sum())
        long_df = long_df[~mask_exhaust].copy()

    # --- OR Gate: require confirmed directional break of the 09:15-09:30 range ---
    short_or_removed = 0
    long_or_removed  = 0
    if V16_OR_GATE_ENABLED:
        # SHORT: OR gate disabled in Run11 (marginal benefit, cost -90 trades)
        if V16_OR_GATE_SHORT_ENABLED and not short_df.empty and "or_low" in short_df.columns and "entry_price" in short_df.columns:
            ep   = pd.to_numeric(short_df["entry_price"], errors="coerce")
            orl  = pd.to_numeric(short_df["or_low"],      errors="coerce")
            sig_time = pd.to_datetime(short_df.get("signal_time_ist", pd.Series(dtype=str)), errors="coerce")
            is_post_or = sig_time.dt.time >= V16_OR_GATE_TIME
            or_valid   = orl.notna()
            mask_or_fail_short = is_post_or & or_valid & (ep >= orl)
            short_or_removed = int(mask_or_fail_short.sum())
            short_df = short_df[~mask_or_fail_short].copy()

        # LONG: entry_price must be ABOVE or_high (confirmed breakout)
        if not long_df.empty and "or_high" in long_df.columns and "entry_price" in long_df.columns:
            ep2  = pd.to_numeric(long_df["entry_price"], errors="coerce")
            orh  = pd.to_numeric(long_df["or_high"],     errors="coerce")
            sig_time2 = pd.to_datetime(long_df.get("signal_time_ist", pd.Series(dtype=str)), errors="coerce")
            is_post_or2 = sig_time2.dt.time >= V16_OR_GATE_TIME
            or_valid2   = orh.notna()
            mask_or_fail_long = is_post_or2 & or_valid2 & (ep2 <= orh)
            long_or_removed = int(mask_or_fail_long.sum())
            long_df = long_df[~mask_or_fail_long].copy()

    print(
        f"[V16_FILTER] SHORT: {short_before}->{len(short_df)} "
        f"(-{short_rsi_removed} RSI {V16_SHORT_RSI_DEAD_ZONE_LO:.0f}-{V16_SHORT_RSI_DEAD_ZONE_HI:.0f} dead zone, "
        f"-{short_or_removed} OR gate) | "
        f"LONG: {long_before}->{len(long_df)} "
        f"(-{long_qs_removed} QS {V16_LONG_QS_DEAD_LO:.1f}-{V16_LONG_QS_DEAD_HI:.1f}dead+QS>{V16_LONG_QS_ABS_MAX:.0f}, "
        f"-{long_dist_removed} dist>{V16_LONG_AVWAP_DIST_ATR_MAX:.1f}ATR, "
        f"-{long_avwap_dead_removed} dist {V16_LONG_AVWAP_DIST_DEAD_LO:.1f}-{V16_LONG_AVWAP_DIST_DEAD_HI:.1f}ATR dead, "
        f"-{long_vol_exhaust_removed} vol>{V16_LONG_ENTRY_VOL_EXHAUST_MULT:.0f}x exhaust, "
        f"-{long_or_removed} OR gate)"
    )
    return short_df, long_df


def _apply_v17c_setup_quality_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg: StrategyConfig,
) -> tuple:
    """
    v17c setup-specific quality filters.

    Uses bar-shape and recent-range context instead of extra lag on the setups
    we want to accelerate, and applies extra activation discipline to the
    re-enabled HUGE pullback-hold long branch.
    """
    ctx_cache: Dict[str, pd.DataFrame] = {}
    short_before = len(short_df)
    long_before = len(long_df)

    def _feature_bundle(row) -> Optional[Dict[str, float]]:
        ts_val = row.get("signal_time_ist", row.get("entry_time_ist"))
        ctx = _get_signal_context_row(row.get("ticker"), ts_val, cfg, ctx_cache)
        if ctx is None:
            return None
        out = {
            "close_top_frac": float(ctx.get("close_top_frac", np.nan)),
            "close_bottom_frac": float(ctx.get("close_bottom_frac", np.nan)),
            "upper_wick_frac": float(ctx.get("upper_wick_frac", np.nan)),
            "lower_wick_frac": float(ctx.get("lower_wick_frac", np.nan)),
            "range_vs_median5": float(ctx.get("range_vs_median5", np.nan)),
            "volume_ratio20": float(ctx.get("volume_ratio20", np.nan)),
            "adx_slope2": float(ctx.get("adx_slope2", np.nan)),
        }
        return out

    def _row_ok_long(row) -> bool:
        setup = str(row.get("setup", "")).upper()
        feats = _feature_bundle(row)
        if setup == "A_MOD_CLOSE_CONTINUATION_BREAK":
            if feats is None:
                return False
            return (
                np.isfinite(feats["close_top_frac"])
                and feats["close_top_frac"] <= max(V17C_LONG_STRONG_CLOSE_MAX_FRACTION, 0.35)
                and np.isfinite(feats["upper_wick_frac"])
                and feats["upper_wick_frac"] <= max(V17C_OPPOSING_WICK_MAX_FRACTION, 0.35)
                and np.isfinite(feats["range_vs_median5"])
                and feats["range_vs_median5"] >= min(V17C_RANGE_EXPANSION_MIN, 0.90)
            )
        if setup == "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK":
            if feats is None:
                return False
            avwap_dist = pd.to_numeric(pd.Series([row.get("avwap_dist_atr_signal")]), errors="coerce").iloc[0]
            rsi_val = pd.to_numeric(pd.Series([row.get("rsi_signal")]), errors="coerce").iloc[0]
            return (
                np.isfinite(feats["adx_slope2"])
                and feats["adx_slope2"] >= V17C_HUGE_PULLBACK_ADX_SLOPE_MIN
                and np.isfinite(rsi_val)
                and float(rsi_val) >= V17C_HUGE_PULLBACK_RSI_MIN
                and np.isfinite(feats["volume_ratio20"])
                and feats["volume_ratio20"] >= V17C_HUGE_PULLBACK_VOL_RATIO_MIN
                and np.isfinite(feats["close_top_frac"])
                and feats["close_top_frac"] <= V17C_LONG_STRONG_CLOSE_MAX_FRACTION
                and np.isfinite(feats["upper_wick_frac"])
                and feats["upper_wick_frac"] <= V17C_OPPOSING_WICK_MAX_FRACTION
                and np.isfinite(feats["range_vs_median5"])
                and feats["range_vs_median5"] >= V17C_RANGE_EXPANSION_MIN
                and np.isfinite(avwap_dist)
                and float(avwap_dist) <= V17C_HUGE_PULLBACK_AVWAP_DIST_ATR_MAX
            )
        return True

    def _row_ok_short(row) -> bool:
        setup = str(row.get("setup", "")).upper()
        if setup != "C_SHORT_CONTINUATION_BREAK":
            return True
        # Let the shortlist/rank gate decide which continuation names are worth keeping.
        # v18b uses stronger session shortlist metadata than the generic setup-quality
        # filter, so a hard pre-shortlist rejection here was starving the branch.
        return True

    short_removed = 0
    long_removed = 0
    if not short_df.empty:
        mask = short_df.apply(_row_ok_short, axis=1)
        short_removed = int((~mask).sum())
        short_df = short_df[mask].copy()
    if not long_df.empty:
        mask = long_df.apply(_row_ok_long, axis=1)
        long_removed = int((~mask).sum())
        long_df = long_df[mask].copy()

    print(
        f"[V17C_FILTER] SHORT: {short_before}->{len(short_df)} "
        f"(-{short_removed} short-continuation quality) | "
        f"LONG: {long_before}->{len(long_df)} "
        f"(-{long_removed} continuation/huge-pullback quality)"
    )
    return short_df, long_df


def _normalize_scan_date_key(value) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.notna(ts):
        return ts.date().isoformat()
    raw = str(value).strip()
    return raw[:10] if raw else None


def _classify_v18_session(ts_val) -> Optional[str]:
    ts = pd.to_datetime(ts_val, errors="coerce")
    if pd.isna(ts):
        return None
    bar_time = ts.time()
    # Primary morning window (09:20-11:30)
    if V18_MORNING_SESSION_WINDOW[0] <= bar_time <= V18_MORNING_SESSION_WINDOW[1]:
        return "morning"
    # Primary afternoon window (12:00-14:45)
    if V18_AFTERNOON_SESSION_WINDOW[0] <= bar_time <= V18_AFTERNOON_SESSION_WINDOW[1]:
        return "afternoon"
    # V18c bridge zone (11:30-12:00): LONG bridge window signals use morning shortlist context
    # (premarket gap/momentum context is still the relevant qualifier for midday AVWAP reclaims)
    if V18_MORNING_SESSION_WINDOW[1] < bar_time < V18C_BRIDGE_ZONE_END:
        return "morning"
    return None


def _v18b_tier_from_rank_pct(rank_pct: float) -> int:
    if not np.isfinite(rank_pct):
        return 3
    if rank_pct <= V18B_TIER1_MAX_RANK_PCT:
        return 1
    if rank_pct <= V18B_TIER2_MAX_RANK_PCT:
        return 2
    return 3


def _v18b_shortlist_entry_map(
    date_key: str,
    session_name: str,
    side_key: str,
    cache: Dict[Tuple[str, str, str], Dict[str, Dict[str, float]]],
) -> Dict[str, Dict[str, float]]:
    cache_key = (date_key, session_name, side_key)
    if cache_key in cache:
        return cache[cache_key]

    entries_raw = []
    if callable(get_two_session_shortlist_entries):
        entries_raw = list(get_two_session_shortlist_entries(date_key, session_name, side=side_key) or [])
    if not entries_raw:
        values = list(get_two_session_shortlist(date_key, session_name, side_key, as_set=False) or [])
        total = len(values)
        entries_raw = [
            {
                "ticker": ticker,
                "score": 0.0,
                "rank": idx,
                "rank_pct": (idx / total) if total else 1.0,
            }
            for idx, ticker in enumerate(values, start=1)
        ]

    entry_map: Dict[str, Dict[str, float]] = {}
    for idx, entry in enumerate(entries_raw, start=1):
        ticker = str(entry.get("ticker", "")).strip().upper()
        if not ticker:
            continue
        rank = int(entry.get("rank", idx))
        rank_pct = float(entry.get("rank_pct", (rank / len(entries_raw)) if entries_raw else 1.0))
        entry_map[ticker] = {
            "score": float(entry.get("score", 0.0)),
            "rank": rank,
            "rank_pct": rank_pct,
            "tier": int(_v18b_tier_from_rank_pct(rank_pct)),
        }

    cache[cache_key] = entry_map
    return entry_map


def _apply_v18_session_shortlists(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    """
    Keep only trades whose ticker is shortlisted for that trade_date/session in
    filtered_stocks_two_session_history.py.

    The v18 runner still scans with the shared v17c ticker architecture, then
    applies the historical two-session shortlist as a no-lookahead universe gate
    before exit resolution and metrics.
    """
    if df.empty:
        print(f"[V18C_SHORTLIST] {side_label}: no rows to filter.")
        return df

    side_upper = str(side_label).upper()
    shortlist_cache: Dict[Tuple[str, str, str], Dict[str, Dict[str, float]]] = {}
    keep_mask: List[bool] = []
    shortlist_score: List[float] = []
    shortlist_rank: List[int] = []
    shortlist_rank_pct: List[float] = []
    shortlist_tier: List[int] = []
    shortlist_source: List[str] = []
    shortlist_session: List[str] = []
    outside_window = 0
    no_date = 0
    missing_shortlist = 0
    not_shortlisted = 0
    removed = 0
    kept_primary = 0
    kept_rescue = 0

    for row in df.itertuples(index=False):
        date_key = _normalize_scan_date_key(getattr(row, "trade_date", None))
        if not date_key:
            keep_mask.append(False)
            shortlist_score.append(np.nan)
            shortlist_rank.append(np.nan)
            shortlist_rank_pct.append(np.nan)
            shortlist_tier.append(np.nan)
            shortlist_source.append("")
            shortlist_session.append("")
            no_date += 1
            removed += 1
            continue

        session_name = _classify_v18_session(
            getattr(row, "signal_time_ist", None) or getattr(row, "entry_time_ist", None)
        )
        if session_name is None:
            keep_mask.append(False)
            shortlist_score.append(np.nan)
            shortlist_rank.append(np.nan)
            shortlist_rank_pct.append(np.nan)
            shortlist_tier.append(np.nan)
            shortlist_source.append("")
            shortlist_session.append("")
            outside_window += 1
            removed += 1
            continue

        ticker_val = str(getattr(row, "ticker", "")).strip().upper()
        setup_val = str(getattr(row, "setup", "")).strip().upper()
        qs_val = pd.to_numeric(pd.Series([getattr(row, "quality_score", np.nan)]), errors="coerce").iloc[0]

        if side_upper == "LONG":
            primary_key = V18B_LONG_PRIMARY_SHORTLIST_SIDE
            expanded_key = V18B_LONG_EXPANDED_SHORTLIST_SIDE
        else:
            primary_key = V18B_SHORT_PRIMARY_SHORTLIST_SIDE
            expanded_key = V18B_SHORT_EXPANDED_SHORTLIST_SIDE

        primary_map = _v18b_shortlist_entry_map(date_key, session_name, primary_key, shortlist_cache)
        expanded_map = (
            primary_map
            if expanded_key == primary_key
            else _v18b_shortlist_entry_map(date_key, session_name, expanded_key, shortlist_cache)
        )

        meta = primary_map.get(ticker_val)
        source = ""
        keep = False

        if meta is not None:
            keep = True
            source = primary_key
            kept_primary += 1
        else:
            meta = expanded_map.get(ticker_val)
            if meta is not None:
                if side_upper == "SHORT":
                    if meta["rank_pct"] <= V18B_SHORT_COMBINED_MAX_RANK_PCT:
                        if (
                            (setup_val == "A_MOD_BREAK_C1_LOW" and meta["rank_pct"] <= 1.00)
                            or (setup_val == "C_SHORT_CONTINUATION_BREAK" and meta["rank_pct"] <= 0.55)
                            or (setup_val == "B_HUGE_RED_FAILED_BOUNCE" and meta["rank_pct"] <= 0.70)
                            or (setup_val == "A_PULLBACK_C2_THEN_BREAK_C2_LOW" and meta["rank_pct"] <= 0.35)
                            or (meta["tier"] < 3 and setup_val in V18B_SHORT_TIER3_ALLOWED_SETUPS)
                        ):
                            keep = True
                            source = expanded_key
                else:
                    if (
                        meta["rank_pct"] <= V18B_LONG_COMBINED_RESCUE_MAX_RANK_PCT
                        and (
                            setup_val in V18B_LONG_COMBINED_RESCUE_SETUPS
                            or (np.isfinite(qs_val) and float(qs_val) >= V18B_LONG_COMBINED_RESCUE_MIN_QS)
                        )
                    ):
                        keep = True
                        source = f"{expanded_key}_rescue"

        if (
            not keep
            and V18B_ENABLE_AFTERNOON_MORNING_CARRY
            and session_name == "afternoon"
        ):
            carry_map = _v18b_shortlist_entry_map(
                date_key,
                "morning",
                expanded_key,
                shortlist_cache,
            )
            carry_meta = carry_map.get(ticker_val)
            if carry_meta is not None:
                if side_upper == "SHORT":
                    if (
                        carry_meta["rank_pct"] <= V18B_SHORT_CARRY_MAX_RANK_PCT
                        and setup_val in {
                            "A_MOD_BREAK_C1_LOW",
                            "C_SHORT_CONTINUATION_BREAK",
                            "B_HUGE_RED_FAILED_BOUNCE",
                        }
                    ):
                        keep = True
                        meta = carry_meta
                        source = "morning_carry"
                else:
                    if (
                        carry_meta["rank_pct"] <= V18B_LONG_CARRY_MAX_RANK_PCT
                        and (
                            setup_val in V18B_LONG_COMBINED_RESCUE_SETUPS
                            or (np.isfinite(qs_val) and float(qs_val) >= V18B_LONG_COMBINED_RESCUE_MIN_QS)
                        )
                    ):
                        keep = True
                        meta = carry_meta
                        source = "morning_carry"

        keep_mask.append(keep)
        if keep and meta is not None:
            shortlist_score.append(float(meta["score"]))
            shortlist_rank.append(int(meta["rank"]))
            shortlist_rank_pct.append(float(meta["rank_pct"]))
            shortlist_tier.append(int(meta["tier"]))
            shortlist_source.append(source)
            shortlist_session.append(session_name)
            if source != primary_key:
                kept_rescue += 1
        else:
            shortlist_score.append(np.nan)
            shortlist_rank.append(np.nan)
            shortlist_rank_pct.append(np.nan)
            shortlist_tier.append(np.nan)
            shortlist_source.append("")
            shortlist_session.append(session_name)

        if not keep:
            removed += 1
            if not primary_map and not expanded_map:
                missing_shortlist += 1
            else:
                not_shortlisted += 1

    filtered = df.loc[keep_mask].copy()
    if not filtered.empty:
        filtered["shortlist_score"] = np.array(shortlist_score, dtype=float)[keep_mask]
        filtered["shortlist_rank"] = np.array(shortlist_rank, dtype=float)[keep_mask]
        filtered["shortlist_rank_pct"] = np.array(shortlist_rank_pct, dtype=float)[keep_mask]
        filtered["shortlist_tier"] = np.array(shortlist_tier, dtype=float)[keep_mask]
        filtered["shortlist_source"] = np.array(shortlist_source, dtype=object)[keep_mask]
        filtered["shortlist_session"] = np.array(shortlist_session, dtype=object)[keep_mask]
    print(
        f"[V18C_SHORTLIST] {side_label}: {len(df)}->{len(filtered)} "
        f"(-{removed} removed | outside_window={outside_window}, no_date={no_date}, "
        f"missing_shortlist={missing_shortlist}, not_shortlisted={not_shortlisted}, "
        f"primary_kept={kept_primary}, rescue_kept={kept_rescue})"
    )
    return filtered


def _apply_v18c2_trade_cleanup(df: pd.DataFrame, side_label: str) -> pd.DataFrame:
    if df is None or df.empty:
        print(f"[V18C2_CLEANUP] {side_label}: no rows to clean.")
        return df

    filtered = df.copy()
    removed_short_pullback = 0
    removed_short_cont_late = 0
    removed_short_cont_tier3_all = 0
    removed_short_cont_t2_short_mid = 0
    removed_short_cont_t2_combined_early = 0
    removed_short_amod_t2_short_early = 0
    removed_short_amod_t1_combined_mid = 0
    removed_short_amod_t2_combined_pm = 0
    removed_long_close_cont = 0
    removed_long_rescue = 0
    removed_long_late_amod = 0
    removed_long_amod_t1_long_mid = 0
    removed_long_amod_rescue_pm = 0
    removed_long_amod_t3_long_pm = 0
    removed_long_bhuge_rescue_late = 0
    removed_long_bhuge_t3_long_late = 0
    removed_long_bhuge_t2_long_pm = 0

    def _refresh_views(frame: pd.DataFrame):
        if "entry_time_ist" in frame.columns:
            entry_view = _normalize_ist_series(frame["entry_time_ist"])
        elif "entry_time" in frame.columns:
            entry_view = _normalize_ist_series(frame["entry_time"])
        else:
            entry_view = pd.Series(pd.NaT, index=frame.index)
        setup_view = (
            frame["setup"].astype(str).str.upper()
            if "setup" in frame.columns
            else pd.Series("", index=frame.index)
        )
        source_view = (
            frame["shortlist_source"].astype(str).str.lower()
            if "shortlist_source" in frame.columns
            else pd.Series("", index=frame.index)
        )
        tier_view = pd.to_numeric(frame.get("shortlist_tier", np.nan), errors="coerce")
        entry_time_view = entry_view.dt.time
        return entry_view, setup_view, source_view, tier_view, entry_time_view

    entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

    if str(side_label).upper() == "SHORT":
        if not V18C2_ENABLE_SHORT_PULLBACK:
            pullback_mask = setup_u.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW")
            removed_short_pullback = int(pullback_mask.sum())
            if removed_short_pullback:
                filtered = filtered.loc[~pullback_mask].copy()
                entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        cont_mask = setup_u.eq("C_SHORT_CONTINUATION_BREAK")
        cont_late_mask = cont_mask & entry_time_s.ge(V18C2_SHORT_CONT_LATE_CUTOFF)
        removed_short_cont_late = int(cont_late_mask.sum())
        if removed_short_cont_late:
            filtered = filtered.loc[~cont_late_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        cont_mask = setup_u.eq("C_SHORT_CONTINUATION_BREAK")
        cont_tier3_mask = cont_mask & tier_s.ge(3)
        removed_short_cont_tier3_all = int(cont_tier3_mask.sum())
        if removed_short_cont_tier3_all:
            filtered = filtered.loc[~cont_tier3_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        cont_mask = setup_u.eq("C_SHORT_CONTINUATION_BREAK")
        cont_t2_short_mid_mask = (
            cont_mask
            & source_s.eq("short")
            & tier_s.eq(2)
            & entry_time_s.ge(V18C2_MID_START)
            & entry_time_s.le(V18C2_MID_END)
        )
        removed_short_cont_t2_short_mid = int(cont_t2_short_mid_mask.sum())
        if removed_short_cont_t2_short_mid:
            filtered = filtered.loc[~cont_t2_short_mid_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        cont_mask = setup_u.eq("C_SHORT_CONTINUATION_BREAK")
        cont_t2_combined_early_mask = (
            cont_mask
            & source_s.eq("combined")
            & tier_s.eq(2)
            & entry_time_s.le(V18C2_EARLY_END)
        )
        removed_short_cont_t2_combined_early = int(cont_t2_combined_early_mask.sum())
        if removed_short_cont_t2_combined_early:
            filtered = filtered.loc[~cont_t2_combined_early_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        amod_mask = setup_u.eq("A_MOD_BREAK_C1_LOW")
        amod_t2_short_early_mask = (
            amod_mask
            & source_s.eq("short")
            & tier_s.eq(2)
            & entry_time_s.le(V18C2_EARLY_END)
        )
        removed_short_amod_t2_short_early = int(amod_t2_short_early_mask.sum())
        if removed_short_amod_t2_short_early:
            filtered = filtered.loc[~amod_t2_short_early_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        amod_mask = setup_u.eq("A_MOD_BREAK_C1_LOW")
        amod_t1_combined_mid_mask = (
            amod_mask
            & source_s.eq("combined")
            & tier_s.eq(1)
            & entry_time_s.ge(V18C2_MID_START)
            & entry_time_s.le(V18C2_MID_END)
        )
        removed_short_amod_t1_combined_mid = int(amod_t1_combined_mid_mask.sum())
        if removed_short_amod_t1_combined_mid:
            filtered = filtered.loc[~amod_t1_combined_mid_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        amod_mask = setup_u.eq("A_MOD_BREAK_C1_LOW")
        amod_t2_combined_pm_mask = (
            amod_mask
            & source_s.eq("combined")
            & tier_s.eq(2)
            & entry_time_s.ge(V18C2_PM_START)
        )
        removed_short_amod_t2_combined_pm = int(amod_t2_combined_pm_mask.sum())
        if removed_short_amod_t2_combined_pm:
            filtered = filtered.loc[~amod_t2_combined_pm_mask].copy()

    else:
        if not V18C2_ENABLE_LONG_CLOSE_CONTINUATION:
            close_cont_mask = setup_u.eq("A_MOD_CLOSE_CONTINUATION_BREAK")
            removed_long_close_cont = int(close_cont_mask.sum())
            if removed_long_close_cont:
                filtered = filtered.loc[~close_cont_mask].copy()
                entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        rescue_mask = source_s.eq("combined_rescue") & tier_s.gt(float(V18C2_LONG_RESCUE_MAX_TIER))
        removed_long_rescue = int(rescue_mask.sum())
        if removed_long_rescue:
            filtered = filtered.loc[~rescue_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        late_amod_mask = (
            setup_u.eq("A_MOD_BREAK_C1_HIGH")
            & tier_s.isin(sorted(V18C2_LONG_LATE_A_MOD_BLOCK_TIERS))
            & entry_time_s.ge(V18C2_LONG_LATE_A_MOD_CUTOFF)
        )
        removed_long_late_amod = int(late_amod_mask.sum())
        if removed_long_late_amod:
            filtered = filtered.loc[~late_amod_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        amod_mask = setup_u.eq("A_MOD_BREAK_C1_HIGH")
        amod_t1_long_mid_mask = (
            amod_mask
            & source_s.eq("long")
            & tier_s.eq(1)
            & entry_time_s.ge(V18C2_MID_START)
            & entry_time_s.le(V18C2_MID_END)
        )
        removed_long_amod_t1_long_mid = int(amod_t1_long_mid_mask.sum())
        if removed_long_amod_t1_long_mid:
            filtered = filtered.loc[~amod_t1_long_mid_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        amod_mask = setup_u.eq("A_MOD_BREAK_C1_HIGH")
        amod_rescue_pm_mask = (
            amod_mask
            & source_s.eq("combined_rescue")
            & entry_time_s.ge(V18C2_PM_START)
            & entry_time_s.le(V18C2_PM_END)
        )
        removed_long_amod_rescue_pm = int(amod_rescue_pm_mask.sum())
        if removed_long_amod_rescue_pm:
            filtered = filtered.loc[~amod_rescue_pm_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        amod_mask = setup_u.eq("A_MOD_BREAK_C1_HIGH")
        amod_t3_long_pm_mask = (
            amod_mask
            & source_s.eq("long")
            & tier_s.eq(3)
            & entry_time_s.ge(V18C2_PM_START)
            & entry_time_s.le(V18C2_PM_END)
        )
        removed_long_amod_t3_long_pm = int(amod_t3_long_pm_mask.sum())
        if removed_long_amod_t3_long_pm:
            filtered = filtered.loc[~amod_t3_long_pm_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        bhuge_mask = setup_u.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK")
        bhuge_rescue_late_mask = (
            bhuge_mask
            & source_s.eq("combined_rescue")
            & entry_time_s.ge(V18C2_MID_START)
        )
        removed_long_bhuge_rescue_late = int(bhuge_rescue_late_mask.sum())
        if removed_long_bhuge_rescue_late:
            filtered = filtered.loc[~bhuge_rescue_late_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        bhuge_mask = setup_u.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK")
        bhuge_t3_long_late_mask = (
            bhuge_mask
            & source_s.eq("long")
            & tier_s.eq(3)
            & entry_time_s.ge(V18C2_MID_START)
        )
        removed_long_bhuge_t3_long_late = int(bhuge_t3_long_late_mask.sum())
        if removed_long_bhuge_t3_long_late:
            filtered = filtered.loc[~bhuge_t3_long_late_mask].copy()
            entry_ts, setup_u, source_s, tier_s, entry_time_s = _refresh_views(filtered)

        bhuge_mask = setup_u.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK")
        bhuge_t2_long_pm_mask = (
            bhuge_mask
            & source_s.eq("long")
            & tier_s.eq(2)
            & entry_time_s.ge(V18C2_PM_START)
        )
        removed_long_bhuge_t2_long_pm = int(bhuge_t2_long_pm_mask.sum())
        if removed_long_bhuge_t2_long_pm:
            filtered = filtered.loc[~bhuge_t2_long_pm_mask].copy()

    print(
        f"[V18C2_CLEANUP] {side_label}: {len(df)}->{len(filtered)} "
        f"(-{len(df) - len(filtered)} removed | "
        f"short_pullback={removed_short_pullback}, "
        f"short_cont_late={removed_short_cont_late}, "
        f"short_cont_tier3_all={removed_short_cont_tier3_all}, "
        f"short_cont_t2_short_mid={removed_short_cont_t2_short_mid}, "
        f"short_cont_t2_combined_early={removed_short_cont_t2_combined_early}, "
        f"short_amod_t2_short_early={removed_short_amod_t2_short_early}, "
        f"short_amod_t1_combined_mid={removed_short_amod_t1_combined_mid}, "
        f"short_amod_t2_combined_pm={removed_short_amod_t2_combined_pm}, "
        f"long_close_cont={removed_long_close_cont}, "
        f"long_rescue={removed_long_rescue}, "
        f"long_late_amod={removed_long_late_amod}, "
        f"long_amod_t1_long_mid={removed_long_amod_t1_long_mid}, "
        f"long_amod_rescue_pm={removed_long_amod_rescue_pm}, "
        f"long_amod_t3_long_pm={removed_long_amod_t3_long_pm}, "
        f"long_bhuge_rescue_late={removed_long_bhuge_rescue_late}, "
        f"long_bhuge_t3_long_late={removed_long_bhuge_t3_long_late}, "
        f"long_bhuge_t2_long_pm={removed_long_bhuge_t2_long_pm})"
    )
    return filtered


# ===========================================================================
# TARGET TEST — disabled in V12 (each side uses its own calibrated targets)
# ===========================================================================
TEST_TARGET_OVERRIDE   = False
TEST_SHORT_TARGET_PCT  = 0.00950
TEST_LONG_TARGET_PCT   = 0.01000


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
        short_cfg.use_prev_close_for_day_mode = False
        short_cfg.use_time_windows = False
        short_cfg.min_bars_left_after_entry = 0
        short_cfg.enable_ema200_filter = False
        short_cfg.require_vwap_side_persistence = False
        short_cfg.vwap_side_lookback_bars = 5
        short_cfg.vwap_side_min_count = 3
        short_cfg.require_structure_filter = False
        short_cfg.structure_lookback_bars = 30
        short_cfg.adx_min = V18B_SHORT_ADX_MIN
        short_cfg.adx_slope_min = 0.40
        short_cfg.volume_min_ratio = V18B_SHORT_VOLUME_MIN_RATIO
        short_cfg.mod_impulse_min_atr = 0.30  # V16 Run6: relaxed 0.45→0.30 — matches LONG, more C1 qualify
        short_cfg.avwap_min_consec_closes = 1  # V16 Run6: relaxed 2→1 — 1 close below AVWAP sufficient
        short_cfg.rsi_max_short = V18B_SHORT_RSI_MAX
        short_cfg.stochk_max = 90.0
        short_cfg.stop_pct = V17C_SHORT_STOP_PCT
        short_cfg.target_pct = V17C_SHORT_TARGET_PCT
        short_cfg.enable_breakeven = V17C_SHORT_ENABLE_BREAKEVEN
        short_cfg.be_trigger_pct = V17C_SHORT_BE_TRIGGER_PCT if V17C_SHORT_ENABLE_BREAKEVEN else 0.0
        short_cfg.enable_trailing_stop = V17C_SHORT_ENABLE_TRAILING_STOP
        short_cfg.trail_pct = V17C_SHORT_TRAIL_PCT if V17C_SHORT_ENABLE_TRAILING_STOP else 0.0
        short_cfg.enable_partial_exit = False  # SL/TARGET/EOD only
        short_cfg.partial_exit_fraction = 0.50
        short_cfg.partial_target_fraction = 0.50
        short_cfg.enable_risk_based_position_sizing = False
        short_cfg.risk_per_trade_pct_of_capital = 0.0035
        short_cfg.max_trades_per_ticker_per_day = 6   # V16 Run3: raised 4→6 for volume increase
        short_cfg.enable_topn_per_day = False
        short_cfg.topn_per_day = 0
        short_cfg.entry_time_cutoff = V18B_SHORT_ENTRY_CUTOFF
        short_cfg.min_opening_range_width_pct = V15_SHORT_MIN_OPENING_RANGE_WIDTH_PCT
        short_cfg.signal_avwap_dist_atr_max = V18B_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX

        long_cfg.require_entry_close_confirm = True
        long_cfg.enable_liquidity_sweep_filter = False
        long_cfg.enable_avwap_no_trade_zone = False
        long_cfg.adx_min = V18B_LONG_ADX_MIN
        long_cfg.adx_slope_min = 0.50
        long_cfg.volume_min_ratio = V18B_LONG_VOLUME_MIN_RATIO
        long_cfg.avwap_min_consec_closes = 1  # V16 Run6: relaxed 2→1 — 1 close above AVWAP sufficient
        long_cfg.rsi_min_long = V18B_LONG_RSI_MIN
        long_cfg.quality_score_min = V18B_LONG_QS_MIN
        long_cfg.stochk_min = 15.0
        long_cfg.stochk_max = 95.0
        long_cfg.atr_pct_min = 0.0025
        long_cfg.enable_setup_a_pullback_c2_break = True
        long_cfg.enable_setup_a_close_continuation_break = V18C2_ENABLE_LONG_CLOSE_CONTINUATION
        long_cfg.enable_setup_b_huge_c1_close_reclaim_break = True
        long_cfg.stop_pct = 0.0075           # v17c breakout baseline long SL: 0.75%
        long_cfg.target_pct = 0.01000        # v17c breakout baseline long TGT: 1.00%
        long_cfg.enable_breakeven = False
        long_cfg.be_trigger_pct = 0.0
        long_cfg.enable_trailing_stop = False
        long_cfg.trail_pct = 0.0
        long_cfg.min_bars_left_after_entry = 0
        long_cfg.max_vix_for_entries = 13.0
        long_cfg.max_trades_per_ticker_per_day = 5   # V16 Run3: raised 3→5 for volume increase
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
    # Keep the shared helper aligned with the effective backtest main() config.
    long_cfg.lag_bars_long_a_close_continuation_break = 1
    long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = 1
    long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(
        LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK
    )
    long_cfg.lag_bars_long_b_huge_c1_close_reclaim_break = 2

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
def _read_parquet_date_bounds(parquet_path: Path) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Return the earliest/latest IST timestamps found in a parquet `date` column."""
    try:
        df = pd.read_parquet(parquet_path, columns=["date"])
        if df.empty or "date" not in df.columns:
            return None, None
        dt = pd.to_datetime(df["date"], errors="coerce").dropna()
        if dt.empty:
            return None, None
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize(IST)
        else:
            dt = dt.dt.tz_convert(IST)
        return dt.min(), dt.max()
    except Exception:
        return None, None


def _candidate_5m_sample_paths(cand_abs: Path, parquet_files: List[Path]) -> List[Path]:
    """Prefer continuous NIFTY aliases when estimating dataset coverage."""
    sample_paths: List[Path] = []
    for ticker in NIFTY_CONTEXT_TICKERS:
        p = cand_abs / f"{ticker}_stocks_indicators_5min.parquet"
        if p.exists():
            sample_paths.append(p)
    if sample_paths:
        return sample_paths
    return parquet_files[: min(5, len(parquet_files))]


def _score_15m_dir(cand_abs: Path) -> Tuple[int, int, int]:
    """
    Score a 5-minute signal-data directory by (latest_end_ts, earliest_start_ts, file_count).

    We still require fresh market coverage, but when two directories are similarly
    current we prefer the one with the broader historical span so backtests use the
    full runtime dataset instead of a shorter local snapshot.
    """
    if not cand_abs.is_dir():
        return (-1, -1, 0)

    parquet_files = list(cand_abs.glob("*_stocks_indicators_5min.parquet"))
    file_count = len(parquet_files)
    if file_count <= 0:
        return (-1, -1, 0)

    sample_paths = _candidate_5m_sample_paths(cand_abs, parquet_files)
    latest_end_ns = -1
    earliest_start_ns = -1

    for sample_path in sample_paths:
        min_dt, max_dt = _read_parquet_date_bounds(sample_path)
        if min_dt is not None:
            min_ns = int(min_dt.value)
            earliest_start_ns = min_ns if earliest_start_ns < 0 else min(earliest_start_ns, min_ns)
        if max_dt is not None:
            latest_end_ns = max(latest_end_ns, int(max_dt.value))

    if latest_end_ns < 0:
        try:
            latest_end_ns = max(int(p.stat().st_mtime_ns) for p in sample_paths)
        except Exception:
            latest_end_ns = -1

    return (latest_end_ns, earliest_start_ns, file_count)


def _describe_15m_dir_range(cand_abs: Path) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], str]:
    """Describe the effective market-date span of a resolved 5-minute dataset directory."""
    if not cand_abs.is_dir():
        return None, None, ""
    parquet_files = list(cand_abs.glob("*_stocks_indicators_5min.parquet"))
    if not parquet_files:
        return None, None, ""

    sample_paths = _candidate_5m_sample_paths(cand_abs, parquet_files)
    min_seen: Optional[pd.Timestamp] = None
    max_seen: Optional[pd.Timestamp] = None
    labels: List[str] = []
    for sample_path in sample_paths:
        min_dt, max_dt = _read_parquet_date_bounds(sample_path)
        if min_dt is None or max_dt is None:
            continue
        min_seen = min_dt if min_seen is None else min(min_seen, min_dt)
        max_seen = max_dt if max_seen is None else max(max_seen, max_dt)
        labels.append(sample_path.stem.replace("_stocks_indicators_5min", ""))
    return min_seen, max_seen, ",".join(labels[:4])


def _resolve_15m_dir() -> Path:
    """
    Resolve the 5-min signal parquet directory across the repo layouts used here.

    Prefer the freshest valid dataset, but when multiple directories are current
    prefer the one with the widest real market-date coverage.
    """
    _script_dir = Path(__file__).resolve().parent
    if _script_dir.name == "avwap_v11_refactored":
        _proj = _script_dir.parent
    else:
        _proj = _script_dir

    candidates = [
        RUNTIME_DATA_5M_DIR,
        _proj / "stocks_indicators_5min_eq",
        _proj.parent / "stocks_indicators_5min_eq",
        Path.cwd() / "stocks_indicators_5min_eq",
    ]

    ranked: List[Tuple[int, int, int, int, Path]] = []
    seen: set[str] = set()
    for idx, cand in enumerate(candidates):
        cand_abs = cand.resolve()
        key = str(cand_abs).lower()
        if key in seen:
            continue
        seen.add(key)
        latest_end_ns, earliest_start_ns, file_count = _score_15m_dir(cand_abs)
        coverage_rank = -(earliest_start_ns if earliest_start_ns >= 0 else (2**63 - 1))
        ranked.append((latest_end_ns, coverage_rank, file_count, -idx, cand_abs))

    if not ranked:
        return candidates[0].resolve()

    best_latest_end, _, best_count, _, best_path = max(
        ranked, key=lambda item: (item[0], item[1], item[2], item[3])
    )
    if best_count > 0 or best_latest_end >= 0:
        return best_path

    for _, _, _, _, cand_abs in ranked:
        if cand_abs.is_dir():
            return cand_abs

    return ranked[0][4]


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
# INTRABAR DATA READER
# ===========================================================================
def _resolve_intrabar_dir(runtime_dir_hint: Path, folder_names: Tuple[str, ...]) -> Path:
    """Resolve an intrabar data directory from common runtime/project locations."""
    _script_dir = Path(__file__).resolve().parent
    if _script_dir.name == "avwap_v11_refactored":
        _proj = _script_dir.parent
    else:
        _proj = _script_dir

    candidates = [runtime_dir_hint]
    for folder_name in folder_names:
        candidates.extend(
            [
                _proj / "data" / folder_name,
                _proj / folder_name,
                _proj.parent / "data" / folder_name,
                _proj.parent / folder_name,
            ]
        )
    for c in candidates:
        if c.is_dir():
            return c
    return candidates[0]


def _resolve_1min_dir() -> Path:
    """Resolve the 1-minute intrabar exit directory."""
    return _resolve_intrabar_dir(RUNTIME_DATA_1MIN_DIR, ("stocks_indicators_1min_eq",))


def _resolve_5min_dir() -> Path:
    """Backward-compatible alias used by existing sweep scripts."""
    return _resolve_1min_dir()


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


def read_intrabar_parquet(path: str, engine: str = "pyarrow") -> pd.DataFrame:
    """Read intrabar parquet and normalize its timestamp column to `datetime`."""
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


def read_5m_parquet(path: str, engine: str = "pyarrow") -> pd.DataFrame:
    """Backward-compatible intrabar reader used by existing sweep scripts."""
    return read_intrabar_parquet(path, engine=engine)


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


def _load_ticker_intrabar_cache(
    cache: Dict[str, pd.DataFrame],
    ticker: str,
    dir_path: Path,
    patterns: List[str],
    engine: str,
) -> pd.DataFrame:
    """Load and cache intrabar parquet for one ticker from common file-name patterns."""
    if ticker in cache:
        return cache[ticker]

    loaded = pd.DataFrame()
    if dir_path.is_dir():
        for pattern in patterns:
            fpath = dir_path / pattern
            if fpath.exists():
                loaded = read_intrabar_parquet(str(fpath), engine)
                break
    cache[ticker] = loaded
    return loaded


def _slice_trade_day_bars(
    bars: pd.DataFrame,
    entry_time: pd.Timestamp,
    trade_date: pd.Timestamp,
    eod_cutoff: Optional[pd.Timestamp],
) -> pd.DataFrame:
    """Slice post-entry bars for the trade date, optionally bounded by the EOD cutoff."""
    if bars is None or bars.empty or "datetime" not in bars.columns:
        return pd.DataFrame()

    mask = (bars["datetime"] > entry_time) & (bars["datetime"].dt.normalize() == trade_date)
    if eod_cutoff is not None:
        mask = mask & (bars["datetime"] <= eod_cutoff)
    return bars.loc[mask].sort_values("datetime")


# ===========================================================================
# 5-MIN EXIT RESOLUTION
# ===========================================================================
def _v17c_setup_exit_profile(side_val: str, setup_val: str) -> Dict[str, float]:
    side_u = str(side_val or "").upper()
    setup_u = str(setup_val or "").upper()

    if side_u == "SHORT":
        return {
            "stop_pct": V17C_SHORT_STOP_PCT,
            "target_pct": V17C_SHORT_TARGET_PCT,
            "be_trigger_pct": V17C_SHORT_BE_TRIGGER_PCT if V17C_SHORT_ENABLE_BREAKEVEN else 0.0,
            "trail_pct": V17C_SHORT_TRAIL_PCT if V17C_SHORT_ENABLE_TRAILING_STOP else 0.0,
            "be_pad_pct": 0.0001,
            "enable_breakeven": V17C_SHORT_ENABLE_BREAKEVEN,
            "enable_trailing_stop": V17C_SHORT_ENABLE_TRAILING_STOP,
        }

    profile = {
        "stop_pct": 0.0075,
        "target_pct": 0.0100,
        "be_trigger_pct": 0.0,
        "trail_pct": 0.0,
        "be_pad_pct": 0.0001,
        "enable_breakeven": False,
        "enable_trailing_stop": False,
    }
    if setup_u in {"B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"}:
        profile.update(
            stop_pct=0.0085,
            target_pct=0.0120,
            be_trigger_pct=0.0,
            trail_pct=0.0,
        )
    return profile


def _resolve_exits_5min(
    trades_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
    eod_exit_time: Optional[dtime] = None,
) -> pd.DataFrame:
    """
    Re-evaluate exit prices, exit times, and outcomes using intrabar data
    for higher-resolution SL/target tracking.

    Entry signals and entry prices remain from 5-min scanning.
    Exit resolution preference is:
      1. 1-minute data (`stocks_indicators_1min_eq`)
      2. 5-minute fallback from the signal dataset
    """
    if trades_df.empty:
        return trades_df

    if not dir_5m.is_dir():
        print(f"[WARN] 1-min data directory not found: {dir_5m}")
        print("[WARN] Falling back to 5-min exit resolution.")
        return trades_df

    df = trades_df.copy()
    dir_1m = dir_5m

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
            df[c] = _normalize_ist_series(df[c])

    # Cache intrabar data per ticker to avoid re-reads
    _cache_1m: Dict[str, pd.DataFrame] = {}
    _cache_5m_fallback: Dict[str, pd.DataFrame] = {}
    dir_15m = _resolve_15m_dir()

    def _resolve_from_bars(
        bars: pd.DataFrame,
        side_val: str,
        exit_profile: Dict[str, float],
        resolution_prefix: str,
    ) -> Optional[Dict[str, Any]]:
        if bars is None or bars.empty:
            return None

        stop_pct = float(exit_profile.get("stop_pct", 0.0))
        target_pct = float(exit_profile.get("target_pct", 0.0))
        be_trigger_pct = float(exit_profile.get("be_trigger_pct", 0.0))
        trail_pct = float(exit_profile.get("trail_pct", 0.0))
        be_pad_pct = float(exit_profile.get("be_pad_pct", 0.0001))
        enable_breakeven = bool(exit_profile.get("enable_breakeven", True))
        enable_trailing_stop = bool(exit_profile.get("enable_trailing_stop", True))

        if side_val == "SHORT":
            stop_val = entry_price * (1.0 + stop_pct)
            target_val = entry_price * (1.0 - target_pct)
            stop_curr = float(stop_val)
            be_level = entry_price * (1.0 + be_pad_pct)
            be_trigger = entry_price * (1.0 - be_trigger_pct)
        else:
            stop_val = entry_price * (1.0 - stop_pct)
            target_val = entry_price * (1.0 + target_pct)
            stop_curr = float(stop_val)
            be_level = entry_price * (1.0 + be_pad_pct)
            be_trigger = entry_price * (1.0 + be_trigger_pct)

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
        best_price = float(entry_price)
        be_armed = False

        for _, bar in bars.iterrows():
            bar_high = float(bar.get(high_col, np.nan))
            bar_low = float(bar.get(low_col, np.nan))
            bar_time = bar[time_col]

            if np.isnan(bar_high) or np.isnan(bar_low):
                continue

            if side_val == "SHORT":
                if np.isfinite(bar_low) and bar_low < best_price:
                    best_price = bar_low
                if enable_breakeven and (not be_armed) and np.isfinite(bar_low) and bar_low <= be_trigger:
                    be_armed = True
                    stop_curr = min(stop_curr, be_level)
                if be_armed and enable_trailing_stop:
                    trail_sl = best_price * (1.0 + trail_pct)
                    stop_curr = min(stop_curr, trail_sl)
                stop_hit = bar_high >= stop_curr
                target_hit = bar_low <= target_val
            else:
                if np.isfinite(bar_high) and bar_high > best_price:
                    best_price = bar_high
                if enable_breakeven and (not be_armed) and np.isfinite(bar_high) and bar_high >= be_trigger:
                    be_armed = True
                    stop_curr = max(stop_curr, be_level)
                if be_armed and enable_trailing_stop:
                    trail_sl = best_price * (1.0 - trail_pct)
                    stop_curr = max(stop_curr, trail_sl)
                stop_hit = bar_low <= stop_curr
                target_hit = bar_high >= target_val

            if stop_hit and target_hit:
                exit_resolution_case = f"{resolution_prefix}_AMBIGUOUS_BOTH_IN_BAR"
                exit_bar_ambiguous = True
                base_exit_price = stop_curr
                base_exit_time = bar_time
                base_outcome = "BE" if be_armed else "SL"
                pess_exit_price = _apply_stop_exit_slippage(side_val, stop_curr)
                pess_exit_time = bar_time
                pess_outcome = "BE" if be_armed else "SL"
                opt_exit_price = target_val
                opt_exit_time = bar_time
                opt_outcome = "TARGET"
                stop_fill_penalty_applied = True
                break
            if stop_hit:
                stressed_stop_price = _apply_stop_exit_slippage(side_val, stop_curr)
                exit_resolution_case = f"{resolution_prefix}_STOP_ONLY"
                base_exit_price = stop_curr
                base_exit_time = bar_time
                base_outcome = "BE" if be_armed else "SL"
                pess_exit_price = stressed_stop_price
                pess_exit_time = bar_time
                pess_outcome = "BE" if be_armed else "SL"
                opt_exit_price = stressed_stop_price
                opt_exit_time = bar_time
                opt_outcome = "BE" if be_armed else "SL"
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
            "stop_price_resolved": float(stop_val),
            "target_price_resolved": float(target_val),
        }

    updated_rows = 0
    resolved_1min_rows = 0
    fallback_rows = 0
    total_rows = len(df)

    for idx in df.index:
        ticker = str(df.at[idx, "ticker"])
        side = str(df.at[idx, "side"]).upper()
        setup = str(df.at[idx, "setup"]) if "setup" in df.columns and pd.notna(df.at[idx, "setup"]) else ""
        entry_price = float(df.at[idx, "entry_price"])
        entry_time = df.at[idx, "entry_time_ist"]
        exit_profile = _v17c_setup_exit_profile(side, setup)
        stop_price = entry_price * (1.0 + exit_profile["stop_pct"]) if side == "SHORT" else entry_price * (1.0 - exit_profile["stop_pct"])
        target_price = entry_price * (1.0 - exit_profile["target_pct"]) if side == "SHORT" else entry_price * (1.0 + exit_profile["target_pct"])
        df.at[idx, "stop_price"] = stop_price
        df.at[idx, "target_price"] = target_price
        df.at[idx, "sl_price"] = stop_price
        df.at[idx, "exit_stop_pct_cfg"] = float(exit_profile["stop_pct"])
        df.at[idx, "exit_target_pct_cfg"] = float(exit_profile["target_pct"])
        df.at[idx, "exit_be_trigger_pct_cfg"] = float(exit_profile["be_trigger_pct"])
        df.at[idx, "exit_trail_pct_cfg"] = float(exit_profile["trail_pct"])

        if pd.isna(entry_time) or stop_price is None or target_price is None:
            continue

        # Get the trade date for EOD cutoff
        trade_date = pd.Timestamp(entry_time).normalize()
        eod_cutoff = None
        if eod_exit_time is not None:
            eod_cutoff = IST.localize(datetime.combine(trade_date.date(), eod_exit_time))

        resolved = None
        df_1m = _load_ticker_intrabar_cache(
            _cache_1m,
            ticker,
            dir_1m,
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
        bars_1m = _slice_trade_day_bars(df_1m, entry_time, trade_date, eod_cutoff)
        resolved = _resolve_from_bars(bars_1m, side, exit_profile, "1MIN")
        if resolved is not None:
            resolved_1min_rows += 1

        if resolved is None:
            if ticker not in _cache_5m_fallback:
                fpath_15m = dir_15m / f"{ticker}_stocks_indicators_5min.parquet"
                if fpath_15m.exists():
                    _cache_5m_fallback[ticker] = read_15m_parquet(str(fpath_15m), engine)
                else:
                    _cache_5m_fallback[ticker] = pd.DataFrame()

            df_15m = _cache_5m_fallback[ticker]
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
                resolved = _resolve_from_bars(bars_15m, side, exit_profile, "5M_FALLBACK")
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
        df.at[idx, "stop_price"] = float(resolved["stop_price_resolved"])
        df.at[idx, "target_price"] = float(resolved["target_price_resolved"])
        df.at[idx, "sl_price"] = float(resolved["stop_price_resolved"])

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
        f"[INTRABAR] Re-resolved exits for {updated_rows}/{total_rows} trades."
        f" 1min={resolved_1min_rows}"
        + (f" 5m_fallback={fallback_rows}" if fallback_rows else "")
        + "."
    )
    return df


# ===========================================================================
# V17c EXTRA SHORT SETUP
# ===========================================================================
def _merge_trade_lists_without_overlap(
    base_trades: List[Trade],
    extra_trades: List[Trade],
    max_trades_per_day: int = 0,
) -> List[Trade]:
    if not extra_trades:
        return list(base_trades)

    rank = {
        "A_MOD_BREAK_C1_LOW": 0,
        "B_HUGE_RED_FAILED_BOUNCE": 1,
        "C_SHORT_CONTINUATION_BREAK": 2,
    }
    merged = sorted(
        list(base_trades) + list(extra_trades),
        key=lambda t: (
            str(t.trade_date),
            pd.Timestamp(t.entry_time_ist),
            rank.get(str(t.setup).upper(), 9),
        ),
    )
    out: List[Trade] = []
    seen_keys = set()
    last_exit_by_day: Dict[Tuple[str, str, str], pd.Timestamp] = {}
    counts_by_day: Dict[Tuple[str, str, str], int] = {}
    for trade in merged:
        day_key = (str(trade.trade_date), str(trade.ticker).upper(), str(trade.side).upper())
        entry_ts = pd.Timestamp(trade.entry_time_ist)
        dedupe_key = day_key + (entry_ts,)
        if dedupe_key in seen_keys:
            continue
        if max_trades_per_day > 0 and counts_by_day.get(day_key, 0) >= max_trades_per_day:
            continue
        last_exit = last_exit_by_day.get(day_key)
        if last_exit is not None and entry_ts < last_exit:
            continue
        out.append(trade)
        seen_keys.add(dedupe_key)
        last_exit_by_day[day_key] = pd.Timestamp(trade.exit_time_ist)
        counts_by_day[day_key] = counts_by_day.get(day_key, 0) + 1
    return out


def _scan_v17c_short_continuation_one_day(
    ticker: str,
    df_day: pd.DataFrame,
    day_str: str,
    cfg: StrategyConfig,
) -> List[Trade]:
    trades: List[Trade] = []
    if len(df_day) < max(int(cfg.min_bars_for_scan), 8):
        return trades

    i = 2
    while i < len(df_day) - 1:
        ts1 = df_day.at[i, "date"]
        ts1_local = pd.Timestamp(ts1)
        if ts1_local.tzinfo is None:
            ts1_local = ts1_local.tz_localize(IST)
        else:
            ts1_local = ts1_local.tz_convert(IST)
        if not short_mod.in_signal_window(ts1_local, cfg):
            i += 1
            continue
        if ts1_local.time() < NIFTY_CONTEXT_CONFIRM_TIME:
            i += 1
            continue
        if ts1_local.time() >= V17C_SHORT_CONTINUATION_ENTRY_CUTOFF:
            i += 1
            continue

        o1 = float(df_day.at[i, "open"])
        h1 = float(df_day.at[i, "high"])
        l1 = float(df_day.at[i, "low"])
        c1 = float(df_day.at[i, "close"])
        atr1 = float(df_day.at[i, "ATR15"]) if np.isfinite(df_day.at[i, "ATR15"]) else np.nan
        adx1 = float(df_day.at[i, "ADX15"]) if "ADX15" in df_day.columns else np.nan
        rsi1 = float(df_day.at[i, "RSI15"]) if "RSI15" in df_day.columns else np.nan
        k1 = float(df_day.at[i, "STOCHK15"]) if "STOCHK15" in df_day.columns else np.nan
        avwap1 = float(df_day.at[i, "AVWAP"]) if np.isfinite(df_day.at[i, "AVWAP"]) else np.nan
        ema20 = float(df_day.at[i, "EMA20"]) if np.isfinite(df_day.at[i, "EMA20"]) else np.nan
        rng1 = h1 - l1
        if not (np.isfinite(rng1) and rng1 > 0 and np.isfinite(atr1) and atr1 > 0):
            i += 1
            continue

        close_bottom_frac = (c1 - l1) / rng1
        lower_wick_frac = (min(o1, c1) - l1) / rng1
        atr_pct = atr1 / c1 if np.isfinite(c1) and c1 > 0 else np.nan
        adx_slope2 = (
            float(df_day.at[i, "ADX15"] - df_day.at[i - 2, "ADX15"])
            if i >= 2 and "ADX15" in df_day.columns
            else np.nan
        )
        prior_ranges = pd.to_numeric(
            df_day.loc[max(0, i - 5): i - 1, "high"] - df_day.loc[max(0, i - 5): i - 1, "low"],
            errors="coerce",
        )
        range_med = prior_ranges.median()
        range_vs_median = (rng1 / range_med) if np.isfinite(range_med) and range_med > 0 else np.nan
        vol_now = float(df_day.at[i, "volume"]) if "volume" in df_day.columns else np.nan
        vol_avg = pd.to_numeric(
            df_day.loc[max(0, i - 20): i - 1, "volume"] if "volume" in df_day.columns else pd.Series(dtype=float),
            errors="coerce",
        ).mean()
        vol_ratio = (vol_now / vol_avg) if np.isfinite(vol_avg) and vol_avg > 0 else np.nan
        avwap_dist_atr = ((avwap1 - c1) / atr1) if np.isfinite(avwap1) else np.nan

        bearish_structure = (
            c1 < o1
            and np.isfinite(avwap1)
            and c1 < avwap1
            and (not np.isfinite(ema20) or c1 < ema20)
            and np.isfinite(adx1)
            and adx1 >= V17C_SHORT_CONTINUATION_ADX_MIN
            and np.isfinite(adx_slope2)
            and adx_slope2 >= V17C_SHORT_CONTINUATION_ADX_SLOPE_MIN
            and np.isfinite(close_bottom_frac)
            and close_bottom_frac <= V17C_SHORT_STRONG_CLOSE_MAX_FRACTION
            and np.isfinite(lower_wick_frac)
            and lower_wick_frac <= V17C_OPPOSING_WICK_MAX_FRACTION
            and np.isfinite(range_vs_median)
            and range_vs_median >= V17C_RANGE_EXPANSION_MIN
            and np.isfinite(vol_ratio)
            and vol_ratio >= V17C_SHORT_CONTINUATION_VOLUME_MIN_RATIO
            and np.isfinite(avwap_dist_atr)
            and avwap_dist_atr >= 0.25
            and avwap_dist_atr <= V17C_SHORT_CONTINUATION_AVWAP_DIST_ATR_MAX
            and np.isfinite(atr_pct)
            and atr_pct <= V17C_SHORT_CONTINUATION_ATR_PCT_MAX
            and i >= 1
            and float(df_day.at[i, "close"]) < float(df_day.at[i - 1, "close"])
            and float(df_day.at[i, "low"]) < float(df_day.at[i - 1, "low"])
        )
        if not bearish_structure:
            i += 1
            continue

        high_touch_avwap = np.isfinite(avwap1) and h1 >= avwap1 and c1 < avwap1
        recent_below_avwap = False
        if np.isfinite(avwap1) and i >= 1:
            prev_close = float(df_day.at[i - 1, "close"])
            prev_avwap = float(df_day.at[i - 1, "AVWAP"]) if np.isfinite(df_day.at[i - 1, "AVWAP"]) else np.nan
            recent_below_avwap = np.isfinite(prev_avwap) and prev_close < prev_avwap and c1 < avwap1
        if not (high_touch_avwap or recent_below_avwap):
            i += 1
            continue

        entry_idx = i + 1
        if entry_idx >= len(df_day):
            break
        trigger = l1 - short_mod.entry_buffer(l1, cfg)
        if float(df_day.at[entry_idx, "low"]) >= trigger:
            i += 1
            continue
        if cfg.require_entry_close_confirm and float(df_day.at[entry_idx, "close"]) >= trigger:
            i += 1
            continue

        exit_idx, exit_time, exit_price, outcome, partial_exit_taken = short_mod.simulate_exit_short(
            df_day, entry_idx, trigger, cfg
        )
        net_pnl, gross_pnl = short_mod.compute_pnl_pct(trigger, exit_price, "SHORT", cfg)
        sl_price = trigger * (1.0 + cfg.stop_pct)
        target_price = trigger * (1.0 - cfg.target_pct)
        position_size_rs, risk_rs = short_mod.compute_risk_position_size_rs(
            entry_price=trigger,
            stop_price=sl_price,
            cfg=cfg,
        )
        ema_gap_atr = ((ema20 - c1) / atr1) if np.isfinite(ema20) else 0.0
        trades.append(
            Trade(
                trade_date=day_str,
                ticker=ticker,
                side="SHORT",
                setup="C_SHORT_CONTINUATION_BREAK",
                impulse_type="MODERATE",
                signal_time_ist=ts1_local,
                entry_time_ist=df_day.at[entry_idx, "date"],
                entry_price=trigger,
                sl_price=sl_price,
                target_price=target_price,
                exit_time_ist=exit_time,
                exit_price=exit_price,
                outcome=outcome,
                pnl_pct=net_pnl,
                pnl_pct_gross=gross_pnl,
                position_size_rs=position_size_rs,
                risk_per_trade_rs=risk_rs,
                day_mode="trend",
                gap_pct_open=0.0,
                opening_range_width_pct=0.0,
                partial_exit_taken=bool(partial_exit_taken),
                adx_signal=adx1,
                rsi_signal=rsi1,
                stochk_signal=k1,
                avwap_dist_atr_signal=avwap_dist_atr,
                ema20_gap_atr_signal=ema_gap_atr,
                atr_pct_signal=atr_pct,
                quality_score=short_mod.compute_quality_score_short(
                    adx1, avwap_dist_atr, ema_gap_atr, "MODERATE"
                ),
                india_vix=float(cfg.vix_daily.get(day_str, 0.0)),
            )
        )
        i = exit_idx + 1
    return trades


def _scan_v17c_short_continuation_prepared(
    ticker: str,
    df: pd.DataFrame,
    cfg: StrategyConfig,
) -> List[Trade]:
    if not V17C_SHORT_CONTINUATION_ENABLED or df.empty:
        return []
    all_trades: List[Trade] = []
    for day_val, df_day in df.groupby("day", sort=True):
        df_day = df_day.copy().reset_index(drop=True)
        day_trades = _scan_v17c_short_continuation_one_day(ticker, df_day, str(day_val), cfg)
        if day_trades:
            all_trades.extend(day_trades)
    return all_trades


# ===========================================================================
# WORKER FUNCTIONS (for parallel scanning — uses 5-min for entry signals)
# ===========================================================================
def _scan_one_ticker_short(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    """Scan one ticker on the SHORT side. Returns list of Trade dicts."""
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_short(ticker, df, cfg)
    trades = _merge_trade_lists_without_overlap(
        trades,
        _scan_v17c_short_continuation_prepared(ticker, prepare_session_bars_for_scan(df, cfg), cfg),
        int(getattr(cfg, "max_trades_per_ticker_per_day", 0)),
    )
    return [asdict(t) for t in trades]


def _scan_one_ticker_long(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    """Scan one ticker on the LONG side. Returns list of Trade dicts."""
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_long(ticker, df, cfg)
    return [asdict(t) for t in trades]


def _scan_one_ticker_both(
    args: Tuple[str, str, StrategyConfig, StrategyConfig, bool]
) -> Tuple[List[dict], List[dict]]:
    """
    Scan one ticker for SHORT + LONG together.

    The expensive parquet read, session filtering, indicator preparation, and
    per-day AVWAP build are shared once whenever both sides are compatible.
    """
    ticker, path, short_cfg, long_cfg, share_prep = args
    df = read_15m_parquet(path, short_cfg.parquet_engine)
    if df.empty:
        return [], []

    if share_prep:
        df_prepared = prepare_session_bars_for_scan(df, short_cfg)
        if df_prepared.empty:
            return [], []
        short_trades = scan_short_prepared(ticker, df_prepared, short_cfg)
        short_trades = _merge_trade_lists_without_overlap(
            short_trades,
            _scan_v17c_short_continuation_prepared(ticker, df_prepared, short_cfg),
            int(getattr(short_cfg, "max_trades_per_ticker_per_day", 0)),
        )
        long_trades = scan_long_prepared(ticker, df_prepared, long_cfg)
    else:
        short_trades = scan_short(ticker, df, short_cfg)
        short_trades = _merge_trade_lists_without_overlap(
            short_trades,
            _scan_v17c_short_continuation_prepared(
                ticker,
                prepare_session_bars_for_scan(df, short_cfg),
                short_cfg,
            ),
            int(getattr(short_cfg, "max_trades_per_ticker_per_day", 0)),
        )
        long_trades = scan_long(ticker, df, long_cfg)

    return [asdict(t) for t in short_trades], [asdict(t) for t in long_trades]


# ===========================================================================
# PARALLEL SCAN RUNNER
# ===========================================================================
def _main_module_supports_process_pool() -> bool:
    main_mod = sys.modules.get("__main__")
    main_file = getattr(main_mod, "__file__", "")
    if not main_file:
        return False
    main_path = str(main_file).strip()
    main_name = Path(main_path).name
    return not (main_name.startswith("<") and main_name.endswith(">"))


def _resolve_executor_mode(max_workers: int) -> str:
    if max_workers <= 1:
        return "serial"

    aliases = {
        "threads": "thread",
        "threadpool": "thread",
        "processes": "process",
        "processpool": "process",
    }
    requested = aliases.get(EXECUTOR_MODE, EXECUTOR_MODE)
    if requested in {"serial", "thread", "process"}:
        return requested
    if requested not in {"", "auto"}:
        warnings.warn(
            f"Unknown EQIDV17C_5MIN_EXECUTOR={EXECUTOR_MODE!r}; falling back to auto mode.",
            RuntimeWarning,
        )
    return "process" if _main_module_supports_process_pool() else "thread"


def _build_executor(executor_mode: str, max_workers: int):
    if executor_mode == "process":
        kwargs: Dict[str, Any] = {"max_workers": max_workers}
        if os.name == "nt":
            kwargs["mp_context"] = mp.get_context("spawn")
        return ProcessPoolExecutor(**kwargs)
    if executor_mode == "thread":
        return ThreadPoolExecutor(max_workers=max_workers)
    raise ValueError(f"Unsupported executor mode: {executor_mode}")


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
    executor_mode = _resolve_executor_mode(max_workers)

    if executor_mode == "serial":
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
        print(f"  [{side}] executor={executor_mode} | workers={max_workers}")
        with _build_executor(executor_mode, max_workers) as executor:
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


def _finalize_side_scan_df(out: pd.DataFrame, cfg: StrategyConfig) -> pd.DataFrame:
    if out.empty:
        return out

    out = apply_topn_per_day(out, cfg)

    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    sort_cols = [c for c in ["trade_date", "ticker", "entry_time_ist"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def _configs_share_combined_scan_prep(
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
) -> bool:
    keys = (
        "dir_15m",
        "end_15m",
        "parquet_engine",
        "session_start",
        "session_end",
    )
    return all(getattr(short_cfg, key) == getattr(long_cfg, key) for key in keys)


def _run_both_parallel(
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    max_workers: int = MAX_WORKERS,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scan all tickers once and generate both SHORT and LONG entries.

    This avoids reading the same parquet and rebuilding the same indicators twice
    for every ticker during v15 backtests.
    """
    tickers = list_tickers_15m(short_cfg.dir_15m, short_cfg.end_15m)
    print(f"[COMBINED] Tickers found: {len(tickers)}")

    share_prep = _configs_share_combined_scan_prep(short_cfg, long_cfg)
    if not share_prep:
        print("[COMBINED] Configs differ on scan-prep inputs; falling back to side-specific prep inside each worker.")

    task_args = [
        (
            t,
            os.path.join(short_cfg.dir_15m, f"{t}{short_cfg.end_15m}"),
            short_cfg,
            long_cfg,
            share_prep,
        )
        for t in tickers
    ]

    short_dicts: List[dict] = []
    long_dicts: List[dict] = []
    scan_errors: List[Tuple[str, str]] = []
    executor_mode = _resolve_executor_mode(max_workers)

    if executor_mode == "serial":
        for k, args in enumerate(task_args, 1):
            try:
                short_rows, long_rows = _scan_one_ticker_both(args)
                short_dicts.extend(short_rows)
                long_dicts.extend(long_rows)
            except Exception as exc:
                scan_errors.append((args[0], str(exc)))
            if k % 50 == 0:
                print(
                    f"  [COMBINED] scanned {k}/{len(tickers)} | "
                    f"short_trades={len(short_dicts)} | long_trades={len(long_dicts)}"
                )
    else:
        done_count = 0
        print(f"  [COMBINED] executor={executor_mode} | workers={max_workers}")
        with _build_executor(executor_mode, max_workers) as executor:
            futures = {executor.submit(_scan_one_ticker_both, a): a[0] for a in task_args}
            for future in as_completed(futures):
                done_count += 1
                try:
                    short_rows, long_rows = future.result()
                    short_dicts.extend(short_rows)
                    long_dicts.extend(long_rows)
                except Exception as exc:
                    ticker = futures[future]
                    scan_errors.append((ticker, str(exc)))

                if done_count % 100 == 0:
                    print(
                        f"  [COMBINED] scanned {done_count}/{len(tickers)} | "
                        f"short_trades={len(short_dicts)} | long_trades={len(long_dicts)}"
                    )

    if scan_errors:
        print(
            f"  [COMBINED] {len(scan_errors)} ticker(s) skipped (bad/missing data): "
            f"{', '.join(t for t, _ in scan_errors)}"
        )

    short_df = _finalize_side_scan_df(pd.DataFrame(short_dicts), short_cfg)
    long_df = _finalize_side_scan_df(pd.DataFrame(long_dicts), long_cfg)
    return short_df, long_df


def _trade_date_mask(df: pd.DataFrame, date_str: str) -> pd.Series:
    if df is None or df.empty or "trade_date" not in df.columns:
        return pd.Series(False, index=df.index if df is not None else pd.Index([]))
    trade_dates = pd.to_datetime(df["trade_date"], errors="coerce")
    return trade_dates.dt.strftime("%Y-%m-%d").eq(str(date_str))


def _coerce_ist_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _normalize_ist_series(values: Any) -> pd.Series:
    if isinstance(values, pd.Series):
        ser = values.copy()
    else:
        ser = pd.Series(values)
    return ser.apply(_coerce_ist_timestamp)


def _lag_bars_for_setup(side: str, setup: str) -> int:
    side_u = str(side or "").strip().upper()
    setup_u = str(setup or "").strip().upper()

    if side_u == "SHORT":
        if setup_u == "A_MOD_BREAK_C1_LOW":
            return int(SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW)
        if setup_u == "A_PULLBACK_C2_THEN_BREAK_C2_LOW":
            return int(SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW)
        if setup_u == "B_HUGE_FAILED_BOUNCE":
            return int(SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE)
    elif side_u == "LONG":
        if setup_u == "A_MOD_BREAK_C1_HIGH":
            return int(LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH)
        if setup_u == "A_PULLBACK_C2_THEN_BREAK_C2_HIGH":
            return int(LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH)
        if setup_u == "B_HUGE_PULLBACK_HOLD_BREAK":
            return int(LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)
        if setup_u == "B_HUGE_C1_CLOSE_RECLAIM_BREAK":
            return int(LONG_LAG_BARS_B_HUGE_C1_CLOSE_RECLAIM_BREAK)

    return 0


def _infer_signal_time_from_entry(entry_ts: Any, side: str, setup: str) -> pd.Timestamp:
    ts = _coerce_ist_timestamp(entry_ts)
    if pd.isna(ts):
        return pd.NaT

    lag_bars = _lag_bars_for_setup(side, setup)
    if lag_bars < 0 or lag_bars > 16:
        lag_bars = 0
    return ts - pd.Timedelta(minutes=5 * lag_bars)


def _convert_live_replay_rows_to_backtest_df(replay_df: pd.DataFrame, side: str) -> pd.DataFrame:
    if replay_df is None or replay_df.empty:
        return pd.DataFrame()

    d = replay_df.copy()
    # Live signal CSVs use `stop_price`; normalize it for the backtest schema.
    if "sl_price" not in d.columns and "stop_price" in d.columns:
        d["sl_price"] = d["stop_price"]
    entry_col = "signal_entry_datetime_ist" if "signal_entry_datetime_ist" in d.columns else "signal_bar_time_ist"
    entry_source = d[entry_col] if entry_col in d.columns else pd.Series(pd.NaT, index=d.index)
    d["entry_time_ist"] = _normalize_ist_series(entry_source)
    if "date" in d.columns:
        d["trade_date"] = pd.to_datetime(d["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        d["trade_date"] = d["entry_time_ist"].apply(
            lambda ts: ts.strftime("%Y-%m-%d") if pd.notna(ts) else pd.NA
        )
    d["ticker"] = d.get("ticker", "").astype(str).str.upper()
    d["side"] = str(side).upper()
    d["setup"] = d.get("setup", "").astype(str)
    d["impulse_type"] = d.get("impulse_type", "").astype(str)
    d["signal_time_ist"] = _normalize_ist_series([
        _infer_signal_time_from_entry(entry_ts, side, setup)
        for entry_ts, setup in zip(d["entry_time_ist"], d["setup"])
    ])

    for col in ["entry_price", "sl_price", "target_price", "quality_score", "adx", "rsi", "stochk", "atr_pct"]:
        if col not in d.columns:
            d[col] = np.nan
        d[col] = pd.to_numeric(d[col], errors="coerce")

    d["exit_time_ist"] = d["entry_time_ist"]
    d["exit_price"] = d["entry_price"]
    d["outcome"] = "PENDING"
    d["pnl_pct"] = 0.0
    d["pnl_pct_gross"] = 0.0
    d["position_size_rs"] = 0.0
    d["risk_per_trade_rs"] = 0.0
    d["day_mode"] = "trend"
    d["gap_pct_open"] = 0.0
    d["opening_range_width_pct"] = 0.0
    d["partial_exit_taken"] = False
    d["adx_signal"] = d["adx"]
    d["rsi_signal"] = d["rsi"]
    d["stochk_signal"] = d["stochk"]
    d["avwap_dist_atr_signal"] = 0.0
    d["ema20_gap_atr_signal"] = 0.0
    d["atr_pct_signal"] = d["atr_pct"]
    d["india_vix"] = 0.0

    ordered_cols = [
        "trade_date", "ticker", "side", "setup", "impulse_type",
        "signal_time_ist", "entry_time_ist", "entry_price", "sl_price",
        "target_price", "exit_time_ist", "exit_price", "outcome",
        "pnl_pct", "pnl_pct_gross", "position_size_rs", "risk_per_trade_rs",
        "day_mode", "gap_pct_open", "opening_range_width_pct",
        "partial_exit_taken", "adx_signal", "rsi_signal", "stochk_signal",
        "avwap_dist_atr_signal", "ema20_gap_atr_signal", "atr_pct_signal",
        "quality_score", "india_vix",
    ]
    return d[ordered_cols].dropna(subset=["entry_time_ist", "entry_price", "sl_price", "target_price"]).reset_index(drop=True)


def _load_live_parity_replay_df(side: str, date_str: str) -> pd.DataFrame:
    side_u = str(side or "").strip().upper()
    if side_u not in {"SHORT", "LONG"}:
        return pd.DataFrame()

    side_l = side_u.lower()
    csv_candidates = [
        RUNTIME_LIVE_SIGNALS_DIR / f"signals_{date_str}_v15_new_{side_l}.csv",
        RUNTIME_LIVE_SIGNALS_DIR / f"signals_{date_str}_v15_{side_l}.csv",
    ]
    for csv_path in csv_candidates:
        try:
            if csv_path.exists():
                csv_df = pd.read_csv(csv_path)
                if csv_df is not None and not csv_df.empty:
                    print(f"[LIVE_PARITY] Using signal CSV for {side_u} {date_str}: {csv_path.name}")
                    return csv_df
        except Exception as exc:
            print(f"[LIVE_PARITY] WARN: unable to read {csv_path.name}: {exc}")

    try:
        if side_u == "SHORT":
            import eqidv2_live_combined_analyser_csv_v15_short as replay_mod
            replay_mod._apply_v15_short_overrides()
            replay_mod._refresh_v15_nifty_context()
        else:
            import eqidv2_live_combined_analyser_csv_v15_long as replay_mod
            replay_mod._apply_v15_long_overrides()
            replay_mod._refresh_v15_nifty_context()
    except Exception as exc:
        print(f"[LIVE_PARITY] WARN: unable to import {side_u} replay module: {exc}")
        return pd.DataFrame()

    try:
        replay_df = replay_mod.v2.run_replay_for_date(str(date_str))
    except Exception as exc:
        print(f"[LIVE_PARITY] WARN: {side_u} replay failed for {date_str}: {exc}")
        return pd.DataFrame()

    if replay_df is None or replay_df.empty:
        return pd.DataFrame()
    return replay_df


def _should_sync_side_with_live_parity(df: pd.DataFrame, side: str, date_str: str) -> bool:
    if bool(_trade_date_mask(df, date_str).any()):
        return True

    side_u = str(side or "").strip().lower()
    side_csv_candidates = (
        RUNTIME_LIVE_SIGNALS_DIR / f"signals_{date_str}_v15_new_{side_u}.csv",
        RUNTIME_LIVE_SIGNALS_DIR / f"signals_{date_str}_v15_{side_u}.csv",
    )
    return any(p.exists() for p in side_csv_candidates)


def _replace_current_day_with_live_parity(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not bool(SYNC_CURRENT_DAY_WITH_LIVE_PARITY):
        return short_df, long_df

    today_str = now_ist().strftime("%Y-%m-%d")
    sync_short = _should_sync_side_with_live_parity(short_df, "short", today_str)
    sync_long = _should_sync_side_with_live_parity(long_df, "long", today_str)
    if not (sync_short or sync_long):
        return short_df, long_df

    old_short = int(_trade_date_mask(short_df, today_str).sum())
    old_long = int(_trade_date_mask(long_df, today_str).sum())
    print(
        f"[LIVE_PARITY] Rebuilding {today_str} entries from live-slot replay "
        f"(SHORT old={old_short}, LONG old={old_long})"
    )

    replay_short = (
        _convert_live_replay_rows_to_backtest_df(
            _load_live_parity_replay_df("SHORT", today_str),
            "SHORT",
        )
        if sync_short else pd.DataFrame()
    )
    replay_long = (
        _convert_live_replay_rows_to_backtest_df(
            _load_live_parity_replay_df("LONG", today_str),
            "LONG",
        )
        if sync_long else pd.DataFrame()
    )

    short_keep = short_df.loc[~_trade_date_mask(short_df, today_str)].copy() if not short_df.empty else pd.DataFrame()
    long_keep = long_df.loc[~_trade_date_mask(long_df, today_str)].copy() if not long_df.empty else pd.DataFrame()
    short_out = pd.concat([short_keep, replay_short], ignore_index=True, sort=False)
    long_out = pd.concat([long_keep, replay_long], ignore_index=True, sort=False)

    print(
        f"[LIVE_PARITY] Applied {today_str} replay override: "
        f"SHORT {old_short}->{len(replay_short)} | LONG {old_long}->{len(replay_long)}"
    )
    return short_out, long_out


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
            d[c] = _normalize_ist_series(d[c])

    if "entry_time_ist" in d.columns:
        entry_ts = pd.to_datetime(d["entry_time_ist"], errors="coerce")
        d["entry_date"] = entry_ts.dt.strftime("%Y-%m-%d").where(entry_ts.notna(), "")
        d["entry_time"] = entry_ts.dt.strftime("%H:%M:%S").where(entry_ts.notna(), "")

    sort_cols = [
        c
        for c in ["trade_date", "entry_time_ist", "signal_time_ist", "ticker", "side"]
        if c in d.columns
    ]
    if sort_cols:
        d = d.sort_values(sort_cols).reset_index(drop=True)

    if "entry_time_ist" in d.columns and {"entry_date", "entry_time"}.issubset(d.columns):
        ordered_cols: List[str] = []
        seen = set()
        for col in d.columns:
            if col == "entry_time_ist":
                for extra in ("entry_date", "entry_time_ist", "entry_time"):
                    if extra in d.columns and extra not in seen:
                        ordered_cols.append(extra)
                        seen.add(extra)
            elif col not in seen:
                ordered_cols.append(col)
                seen.add(col)
        d = d[ordered_cols]

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

    After the opening range settles, classify each bar as:
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


def _build_stock_atr_map(
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
        atr_col = next((c for c in df.columns if c.lower() == "atr15"), None)
        if atr_col is None:
            atr_col = next((c for c in df.columns if c.lower() == "atr"), None)
        if atr_col is None:
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
        df["close_num"] = pd.to_numeric(df["close"], errors="coerce")
        df["atr_num"] = pd.to_numeric(df[atr_col], errors="coerce")
        df["atr_pct"] = df["atr_num"] / df["close_num"].replace(0, np.nan) * 100.0
        for ts, atr_pct in zip(df["date"], df["atr_pct"]):
            if np.isfinite(atr_pct) and atr_pct > 0:
                out[_ts_to_key_local(ts)] = float(atr_pct)
    except Exception:
        out = {}

    cache[ticker_u] = out
    return out


def _load_ticker_signal_context_df(
    ticker: str,
    cfg: StrategyConfig,
    cache: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    ticker_u = str(ticker).strip().upper()
    if ticker_u in cache:
        return cache[ticker_u]

    p = Path(cfg.dir_15m) / f"{ticker_u}{cfg.end_15m}"
    if not p.exists():
        cache[ticker_u] = pd.DataFrame()
        return cache[ticker_u]

    try:
        df = read_15m_parquet(str(p), cfg.parquet_engine)
        if df.empty:
            cache[ticker_u] = pd.DataFrame()
            return cache[ticker_u]

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
        for col in ("open", "high", "low", "close", "volume", "ADX15", "AVWAP"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        rng = (df["high"] - df["low"]).replace(0, np.nan)
        df["close_top_frac"] = (df["high"] - df["close"]) / rng
        df["close_bottom_frac"] = (df["close"] - df["low"]) / rng
        df["upper_wick_frac"] = (df["high"] - df[["open", "close"]].max(axis=1)) / rng
        df["lower_wick_frac"] = (df[["open", "close"]].min(axis=1) - df["low"]) / rng
        df["range_abs"] = rng
        df["range_med_prev5"] = (
            df.groupby("day")["range_abs"]
            .transform(lambda s: s.shift(1).rolling(5, min_periods=3).median())
        )
        df["range_vs_median5"] = df["range_abs"] / df["range_med_prev5"].replace(0, np.nan)
        if "volume" in df.columns:
            df["volume_sma20_day"] = (
                df.groupby("day")["volume"]
                .transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
            )
            df["volume_ratio20"] = df["volume"] / df["volume_sma20_day"].replace(0, np.nan)
        else:
            df["volume_ratio20"] = np.nan
        if "ADX15" in df.columns:
            df["adx_slope2"] = df.groupby("day")["ADX15"].diff(2)
        else:
            df["adx_slope2"] = np.nan
        df["ts_key_local"] = df["date"].map(_ts_to_key_local)
    except Exception:
        df = pd.DataFrame()

    cache[ticker_u] = df
    return df


def _get_signal_context_row(
    ticker: str,
    ts_val: Any,
    cfg: StrategyConfig,
    cache: Dict[str, pd.DataFrame],
) -> Optional[pd.Series]:
    if pd.isna(ts_val):
        return None
    df = _load_ticker_signal_context_df(ticker, cfg, cache)
    if df.empty:
        return None
    key = _ts_to_key_local(pd.Timestamp(ts_val))
    hit = df.loc[df["ts_key_local"] == key]
    if hit.empty:
        return None
    return hit.iloc[-1]


def _apply_short_v17b_context(
    short_df: pd.DataFrame,
    cfg: StrategyConfig,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, int, int]:
    if short_df.empty:
        return short_df, 0, 0

    d = short_df.copy()
    ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
    ts_series = pd.to_datetime(d[ts_col], errors="coerce")
    if getattr(ts_series.dt, "tz", None) is None:
        ts_series = ts_series.dt.tz_localize(IST)
    else:
        ts_series = ts_series.dt.tz_convert(IST)
    d["ts_key_local"] = ts_series.map(_ts_to_key_local)
    d["nifty_context_mode"] = d["ts_key_local"].map(mode_map).fillna("BOTH")

    before = len(d)
    d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
    mode_removed = before - len(d)

    if not NIFTY_RS_FILTER_ENABLED or d.empty:
        if "ts_key_local" in d.columns:
            d = d.drop(columns=["ts_key_local"])
        return d, mode_removed, 0

    stock_ret_cache: Dict = {}
    atr_cache: Dict = {}
    keep_mask: List[bool] = []
    rel_vals: List[float] = []
    rs_removed = 0

    for row in d.itertuples(index=False):
        ts_key = getattr(row, "ts_key_local")
        mode = getattr(row, "nifty_context_mode", "BOTH")
        rel_val = np.nan
        keep = True
        apply_rs = (mode != "BOTH") or NIFTY_RS_BOTH_MODE_ENABLED

        if apply_rs:
            stock_ret_map = _build_stock_return_map(
                getattr(row, "ticker"), cfg, stock_ret_cache
            )
            stock_ret = stock_ret_map.get(ts_key, np.nan)
            nifty_ret = nifty_ret_map.get(ts_key, np.nan)

            if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                raw_rs = float(stock_ret - nifty_ret)
                rel_val = raw_rs
                if V17C_RS_ATR_NORM_ENABLED:
                    atr_map = _build_stock_atr_map(getattr(row, "ticker"), cfg, atr_cache)
                    atr_pct = atr_map.get(ts_key, np.nan)
                    if np.isfinite(atr_pct) and atr_pct > 0:
                        rs_norm = raw_rs / atr_pct
                        thresh = (
                            V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT
                            if mode != "BOTH"
                            else V17C_RS_ATR_NORM_THRESH_SHORT_BOTH
                        )
                        keep = rs_norm <= -thresh
                    else:
                        keep = raw_rs <= -V17C_RS_ATR_FALLBACK_PCT_SHORT
                else:
                    thresh = (
                        float(NIFTY_RS_THRESHOLD_PCT)
                        if mode != "BOTH"
                        else float(NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT)
                    )
                    keep = raw_rs <= -thresh

        keep_mask.append(bool(keep))
        rel_vals.append(rel_val)
        if not keep:
            rs_removed += 1

    d["nifty_rel_strength_pct"] = rel_vals
    d = d[pd.Series(keep_mask, index=d.index)].copy()
    if "ts_key_local" in d.columns:
        d = d.drop(columns=["ts_key_local"])
    return d, mode_removed, rs_removed


def _apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg: StrategyConfig,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply no-lookahead NIFTY context and v17c side-aware relative-strength filters."""
    if not mode_map:
        return short_df, long_df

    def _prepare(df: pd.DataFrame, side: str) -> Tuple[pd.DataFrame, int]:
        if df.empty:
            return df, 0
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
        if side == "SHORT":
            d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
        else:
            d = d[d["nifty_context_mode"].ne("SHORT_ONLY")].copy()
        return d, before - len(d)

    short_d, _ = _prepare(short_df, "SHORT")
    long_d, l_mode_removed = _prepare(long_df, "LONG")
    short_d, s_mode_removed, s_rs_removed = _apply_short_v17b_context(
        short_df, cfg, mode_map, nifty_ret_map
    )

    l_rs_removed = 0
    stock_ret_cache: Dict[str, Dict[str, float]] = {}

    def _row_raw_rs(row) -> Tuple[float, float, float]:
        ts_key = getattr(row, "ts_key_local")
        stock_ret_map = _build_stock_return_map(getattr(row, "ticker"), cfg, stock_ret_cache)
        stock_ret = stock_ret_map.get(ts_key, np.nan)
        nifty_ret = nifty_ret_map.get(ts_key, np.nan)
        raw_rs = np.nan
        if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
            raw_rs = float(stock_ret - nifty_ret)
        return stock_ret, nifty_ret, raw_rs

    if not long_d.empty and NIFTY_RS_FILTER_ENABLED:
        keep_mask = []
        rel_vals = []
        for row in long_d.itertuples(index=False):
            mode = getattr(row, "nifty_context_mode", "BOTH")
            setup = str(getattr(row, "setup", "")).upper()
            _, _, raw_rs = _row_raw_rs(row)
            rel_vals.append(raw_rs)
            keep = True
            if np.isfinite(raw_rs):
                if mode != "BOTH":
                    rs_thresh = float(NIFTY_RS_THRESHOLD_PCT)
                elif NIFTY_RS_BOTH_MODE_ENABLED:
                    rs_thresh = float(NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT)
                else:
                    rs_thresh = 0.0
                if setup == "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK":
                    rs_thresh = (
                        float(V17C_LONG_HUGE_PULLBACK_RS_DIRECTIONAL_PCT)
                        if mode != "BOTH"
                        else float(V17C_LONG_HUGE_PULLBACK_RS_BOTH_PCT)
                    )
                keep = raw_rs >= rs_thresh
            keep_mask.append(bool(keep))
            if not keep:
                l_rs_removed += 1
        long_d["nifty_rel_strength_pct"] = rel_vals
        long_d = long_d[pd.Series(keep_mask, index=long_d.index)].copy()

    for d in (short_d, long_d):
        if not d.empty and "ts_key_local" in d.columns:
            d.drop(columns=["ts_key_local"], inplace=True, errors="ignore")

    print(
        "[NIFTY_CONTEXT] Applied intraday filter (v17c hybrid): "
        f"SHORT {len(short_df)}->{len(short_d)} "
        f"(mode_removed={s_mode_removed}, rs_removed={s_rs_removed}, "
        f"v17b ATR both/directional={V17C_RS_ATR_NORM_THRESH_SHORT_BOTH:.2f}/{V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT:.2f}, "
        f"fallback={V17C_RS_ATR_FALLBACK_PCT_SHORT:.2f}%) | "
        f"LONG {len(long_df)}->{len(long_d)} "
        f"(mode_removed={l_mode_removed}, rs_removed={l_rs_removed}, "
        f"huge_pullback RS both/directional={V17C_LONG_HUGE_PULLBACK_RS_BOTH_PCT:.2f}/{V17C_LONG_HUGE_PULLBACK_RS_DIRECTIONAL_PCT:.2f})"
    )
    return short_d, long_d


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
            d[c] = _normalize_ist_series(d[c])

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
def _build_daily_breakdown_df(
    df: pd.DataFrame,
    trade_dates: Optional[List[Any]] = None,
    include_total: bool = True,
) -> pd.DataFrame:
    """
    Build a day-wise breakdown table matching the console summary format.

    Returned columns:
      Date | L | S | Tot | W | L_ | Win% | SumPnL% | AvgPnL% | Rs.PnL | Outcomes
      plus optional VIX, RowType, and Notes helper columns.
    """
    if df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()

    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    d = d.dropna(subset=["trade_date"])
    if d.empty:
        return pd.DataFrame()

    d["pnl_pct"] = pd.to_numeric(d.get("pnl_pct", 0), errors="coerce").fillna(0.0)
    d["pnl_rs"] = pd.to_numeric(d.get("pnl_rs", 0), errors="coerce").fillna(0.0)
    d["side"] = d["side"].astype(str).str.upper()
    d["outcome"] = d.get("outcome", pd.Series("", index=d.index)).astype(str).str.upper()

    has_vix = "india_vix" in d.columns and pd.to_numeric(
        d["india_vix"], errors="coerce"
    ).fillna(0.0).gt(0).any()
    if has_vix:
        d["india_vix"] = pd.to_numeric(d["india_vix"], errors="coerce").fillna(0.0)

    all_dates = sorted(d["trade_date"].unique())
    if trade_dates is None:
        use_dates = all_dates
    else:
        use_dates = sorted(
            set(pd.to_datetime(pd.Series(list(trade_dates)), errors="coerce").dt.date.dropna().tolist())
        )

    if not use_dates:
        return pd.DataFrame()

    d = d[d["trade_date"].isin(use_dates)].copy()
    rows: List[Dict[str, Any]] = []
    totals = dict(trades=0, longs=0, shorts=0, wins=0, losses=0, sum_pnl=0.0, sum_rs=0.0)

    for dt in use_dates:
        day = d[d["trade_date"] == dt]
        n = len(day)
        n_long = int((day["side"] == "LONG").sum())
        n_short = int((day["side"] == "SHORT").sum())
        wins = int((day["pnl_pct"] > 0).sum())
        losses = int((day["pnl_pct"] < 0).sum())
        win_pct = wins / n * 100.0 if n else 0.0
        sum_pnl = float(day["pnl_pct"].sum())
        avg_pnl = float(day["pnl_pct"].mean()) if n else 0.0
        sum_rs = float(day["pnl_rs"].sum())

        oc = day["outcome"].value_counts()
        oc_parts = []
        for code, label in [("TARGET", "T"), ("SL", "S"), ("BE", "B"), ("EOD", "E")]:
            if code in oc:
                oc_parts.append(f"{label}:{int(oc[code])}")
        oc_str = " ".join(oc_parts) if oc_parts else "-"

        row: Dict[str, Any] = {
            "Date": str(dt),
            "L": n_long,
            "S": n_short,
            "Tot": n,
            "W": wins,
            "L_": losses,
            "Win%": round(win_pct, 2),
            "SumPnL%": round(sum_pnl, 4),
            "AvgPnL%": round(avg_pnl, 4),
            "Rs.PnL": round(sum_rs, 2),
            "Outcomes": oc_str,
            "RowType": "DAY",
            "Notes": "",
        }
        if has_vix:
            day_vix = day["india_vix"][day["india_vix"] > 0]
            row["VIX"] = round(float(day_vix.iloc[0]), 2) if not day_vix.empty else 0.0
        rows.append(row)

        totals["trades"] += n
        totals["longs"] += n_long
        totals["shorts"] += n_short
        totals["wins"] += wins
        totals["losses"] += losses
        totals["sum_pnl"] += sum_pnl
        totals["sum_rs"] += sum_rs

    if include_total:
        total_win_pct = totals["wins"] / totals["trades"] * 100.0 if totals["trades"] else 0.0
        avg_day_pnl = totals["sum_pnl"] / len(use_dates) if use_dates else 0.0
        total_row: Dict[str, Any] = {
            "Date": "TOTAL",
            "L": totals["longs"],
            "S": totals["shorts"],
            "Tot": totals["trades"],
            "W": totals["wins"],
            "L_": totals["losses"],
            "Win%": round(total_win_pct, 2),
            "SumPnL%": round(float(totals["sum_pnl"]), 4),
            "AvgPnL%": round(float(avg_day_pnl), 4),
            "Rs.PnL": round(float(totals["sum_rs"]), 2),
            "Outcomes": "",
            "RowType": "TOTAL",
            "Notes": "(avg/day)",
        }
        if has_vix:
            total_row["VIX"] = np.nan
        rows.append(total_row)

    order = ["Date", "L", "S", "Tot", "W", "L_", "Win%", "SumPnL%", "AvgPnL%", "Rs.PnL"]
    if has_vix:
        order.append("VIX")
    order.extend(["Outcomes", "RowType", "Notes"])
    return pd.DataFrame(rows)[order]


def _recent_trading_dates_from_runtime(n_days: int) -> List[Any]:
    """
    Infer the latest trading days from runtime 5-minute market-data coverage.

    Prefer the configured NIFTY context aliases, because they are expected to
    have continuous market-day coverage even when strategy trade output is empty.
    """
    n_days = max(0, int(n_days))
    if n_days <= 0:
        return []

    dir_15m = _resolve_15m_dir()
    candidates = list(NIFTY_CONTEXT_TICKERS)

    try:
        extra = list_tickers_15m(str(dir_15m), "_stocks_indicators_5min.parquet")
        for ticker in extra:
            if ticker not in candidates:
                candidates.append(ticker)
    except Exception:
        pass

    for ticker in candidates:
        try:
            p = dir_15m / f"{ticker}_stocks_indicators_5min.parquet"
            if not p.exists():
                continue
            df_idx = read_15m_parquet(str(p), "pyarrow")
            if df_idx.empty or "date" not in df_idx.columns:
                continue
            dates = (
                pd.to_datetime(df_idx["date"], errors="coerce")
                .dt.tz_convert(IST)
                .dt.date
                .dropna()
                .drop_duplicates()
                .sort_values()
                .tolist()
            )
            if dates:
                return dates[-n_days:]
        except Exception:
            continue
    return []


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

    n_days = n_weeks * 5
    runtime_recent_dates = _recent_trading_dates_from_runtime(n_days)
    all_trade_dates = sorted(d["trade_date"].unique())
    if runtime_recent_dates:
        recent_dates = runtime_recent_dates
    else:
        recent_dates = all_trade_dates[-n_days:] if len(all_trade_dates) >= n_days else all_trade_dates
    summary_df = _build_daily_breakdown_df(d, trade_dates=recent_dates, include_total=True)
    if summary_df.empty:
        return

    has_vix = "VIX" in summary_df.columns
    _vix_col = f"{'VIX':>5} " if has_vix else ""
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

    day_rows = summary_df[summary_df["RowType"] == "DAY"]
    total_rows = summary_df[summary_df["RowType"] == "TOTAL"]
    for _, row in day_rows.iterrows():
        vix_str = ""
        if has_vix:
            vix_val = pd.to_numeric(pd.Series([row.get("VIX")]), errors="coerce").iloc[0]
            vix_str = f"{float(vix_val):>5.1f} " if pd.notna(vix_val) and float(vix_val) > 0 else ""
        print(
            f"{str(row['Date']):<12} {int(row['L']):>3} {int(row['S']):>3} {int(row['Tot']):>4} "
            f"{int(row['W']):>4} {int(row['L_']):>4} {float(row['Win%']):>5.0f}% "
            f"{float(row['SumPnL%']):>+10.2f}% {float(row['AvgPnL%']):>+10.2f}% "
            f"{float(row['Rs.PnL']):>+11,.0f}  "
            f"{vix_str}{str(row['Outcomes']) if str(row['Outcomes']) else '-'}"
        )

    print(sep)
    if not total_rows.empty:
        row = total_rows.iloc[0]
        total_vix_str = "  --- " if has_vix else ""
        notes = str(row.get("Notes", "")).strip()
        print(
            f"{str(row['Date']):<12} {int(row['L']):>3} {int(row['S']):>3} {int(row['Tot']):>4} "
            f"{int(row['W']):>4} {int(row['L_']):>4} {float(row['Win%']):>5.0f}% "
            f"{float(row['SumPnL%']):>+10.2f}% {float(row['AvgPnL%']):>+10.2f}% "
            f"{float(row['Rs.PnL']):>+11,.0f}  "
            f"{total_vix_str}{notes}"
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
    _outputs_dir = runtime_dir("outputs_v18c2_5min")
    _outputs_dir.mkdir(parents=True, exist_ok=True)

    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"avwap_combined_runner_v18c2_{ts}.txt"

    # Tee all console output to outputs/*.txt
    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)

        try:
            run_started = time.perf_counter()
            print("=" * 70)
            print("AVWAP v18c2_5min COMBINED runner — optimizer-backed cleanup package")
            print("  - Entry signals: 5-min data")
            print("  - Exit resolution: 1-min if available, else 5-min fallback")
            print("  - Outputs: */algo_trading/outputs")
            print("  - Intraday leverage: "
                  f"SHORT={INTRADAY_LEVERAGE_SHORT}x | LONG={INTRADAY_LEVERAGE_LONG}x")
            print("  - P&L% reported = ROI% on *capital/margin* (levered)")
            print("    (unlevered price-return% is saved as pnl_pct_price)")
            print("=" * 70)

            # Resolve 5-min signal data directory
            dir_15m = _resolve_15m_dir()
            print(f"[INFO] 5-min signal data directory: {dir_15m}")
            if dir_15m.is_dir():
                n_files_15m = len(list(dir_15m.glob("*_stocks_indicators_5min.parquet")))
                print(f"[INFO] 5-min parquet files found: {n_files_15m}")
                data_start_5m, data_end_5m, data_range_src = _describe_15m_dir_range(dir_15m)
                if data_start_5m is not None and data_end_5m is not None:
                    src_txt = f" | source={data_range_src}" if data_range_src else ""
                    print(
                        "[INFO] 5-min dataset coverage: "
                        f"{data_start_5m.strftime('%Y-%m-%d %H:%M:%S %Z')} -> "
                        f"{data_end_5m.strftime('%Y-%m-%d %H:%M:%S %Z')}{src_txt}"
                    )
            else:
                print("[WARN] 5-min signal data directory not found.")

            # Resolve intrabar exit data directories
            dir_1m = _resolve_1min_dir()
            print(f"[INFO] 1-min data directory: {dir_1m}")
            if dir_1m.is_dir():
                n_files = len(list(dir_1m.glob("*.parquet")))
                print(f"[INFO] 1-min parquet files found: {n_files}")
            else:
                print("[WARN] 1-min data directory not found â€” will fall back to 5-min exits.")

            short_cfg = default_short_config(
                reports_dir=_outputs_dir,
            )
            # V12: LONG side uses V9's avwap_common_v7_sweep config
            long_cfg = default_long_config_v9(
                reports_dir=_outputs_dir,
            )
            short_cfg.dir_15m = str(dir_15m)
            long_cfg.dir_15m = str(dir_15m)
            short_cfg.end_15m = "_stocks_indicators_5min.parquet"
            long_cfg.end_15m = "_stocks_indicators_5min.parquet"
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
                # Keep historical backtests aligned with the live-parity short path.
                short_cfg.use_prev_close_for_day_mode = False
                short_cfg.use_time_windows = False
                short_cfg.min_bars_left_after_entry = 0
                short_cfg.enable_ema200_filter = False
                short_cfg.require_vwap_side_persistence = False
                short_cfg.vwap_side_lookback_bars = 5
                short_cfg.vwap_side_min_count = 3
                short_cfg.require_structure_filter = False
                short_cfg.structure_lookback_bars = 30
                short_cfg.adx_min = V18B_SHORT_ADX_MIN
                short_cfg.adx_slope_min = 0.40
                short_cfg.volume_min_ratio = V18B_SHORT_VOLUME_MIN_RATIO
                short_cfg.mod_impulse_min_atr = 0.30  # V16 Run6: relaxed 0.45→0.30 — matches LONG, more C1 qualify
                short_cfg.avwap_min_consec_closes = 1  # V16 Run6: relaxed 2→1 — 1 close below AVWAP sufficient
                short_cfg.rsi_max_short = V18B_SHORT_RSI_MAX
                short_cfg.stochk_max = 90.0
                short_cfg.stop_pct = V17C_SHORT_STOP_PCT
                short_cfg.target_pct = V17C_SHORT_TARGET_PCT
                short_cfg.enable_breakeven = V17C_SHORT_ENABLE_BREAKEVEN
                short_cfg.be_trigger_pct = V17C_SHORT_BE_TRIGGER_PCT if V17C_SHORT_ENABLE_BREAKEVEN else 0.0
                short_cfg.enable_trailing_stop = V17C_SHORT_ENABLE_TRAILING_STOP
                short_cfg.trail_pct = V17C_SHORT_TRAIL_PCT if V17C_SHORT_ENABLE_TRAILING_STOP else 0.0
                short_cfg.enable_partial_exit = False  # SL/TARGET/EOD only
                short_cfg.partial_exit_fraction = 0.50
                short_cfg.partial_target_fraction = 0.50
                short_cfg.enable_risk_based_position_sizing = False  # fixed Rs.50,000/trade via runner constant
                short_cfg.risk_per_trade_pct_of_capital = 0.0035
                short_cfg.max_trades_per_ticker_per_day = 6   # V16 Run3: raised 4→6 for volume increase
                short_cfg.enable_topn_per_day = False
                short_cfg.topn_per_day = 0
                short_cfg.entry_time_cutoff = V18B_SHORT_ENTRY_CUTOFF
                short_cfg.min_opening_range_width_pct = V15_SHORT_MIN_OPENING_RANGE_WIDTH_PCT
                short_cfg.signal_avwap_dist_atr_max = V18B_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX

                # Long side: optimized profile from 15min analysis (2026-03-26)
                long_cfg.require_entry_close_confirm = True
                long_cfg.enable_liquidity_sweep_filter = False
                long_cfg.enable_avwap_no_trade_zone = False
                long_cfg.adx_min = V18B_LONG_ADX_MIN
                long_cfg.adx_slope_min = 0.50
                long_cfg.volume_min_ratio = V18B_LONG_VOLUME_MIN_RATIO
                long_cfg.avwap_min_consec_closes = 1  # V16 Run6: relaxed 2→1 — 1 close above AVWAP sufficient
                long_cfg.rsi_min_long = V18B_LONG_RSI_MIN
                long_cfg.quality_score_min = V18B_LONG_QS_MIN
                long_cfg.stochk_min = 15.0
                long_cfg.stochk_max = 95.0
                long_cfg.atr_pct_min = 0.0025
                long_cfg.enable_setup_a_pullback_c2_break = True
                long_cfg.enable_setup_a_close_continuation_break = V18C2_ENABLE_LONG_CLOSE_CONTINUATION
                long_cfg.enable_setup_b_huge_c1_close_reclaim_break = True
                long_cfg.stop_pct = 0.0075           # v17c breakout baseline long SL: 0.75%
                long_cfg.target_pct = 0.01000        # v17c breakout baseline long TGT: 1.00%
                long_cfg.enable_breakeven = False
                long_cfg.be_trigger_pct = 0.0
                long_cfg.enable_trailing_stop = False
                long_cfg.trail_pct = 0.0
                long_cfg.min_bars_left_after_entry = 0
                long_cfg.max_vix_for_entries = 13.0
                long_cfg.max_trades_per_ticker_per_day = 5   # V16 Run3: raised 3→5 for volume increase
                long_cfg.enable_topn_per_day = False
                long_cfg.topn_per_day = 0

                print("[PROFILE] V18c2: optimizer-backed cleanup of weak shortlist/setup buckets.")

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
            long_cfg.lag_bars_long_a_close_continuation_break = 1
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = 1
            long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)
            long_cfg.lag_bars_long_b_huge_c1_close_reclaim_break = 2

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
                        "[REGIME] Disabled: no market index parquet found in 5m data directory. "
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
                "[INFO] Wave2 SHORT filters: "
                f"entry_cutoff=<{short_cfg.entry_time_cutoff.strftime('%H:%M')} | "
                f"min_OR_width={short_cfg.min_opening_range_width_pct:.2f}% | "
                f"signal_avwap_dist_atr_max={short_cfg.signal_avwap_dist_atr_max:.2f} | "
                f"v17b ATR_RS both/directional={V17C_RS_ATR_NORM_THRESH_SHORT_BOTH:.2f}/{V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT:.2f} | "
                f"fallback_raw_RS<=-{V17C_RS_ATR_FALLBACK_PCT_SHORT:.2f}%"
            )
            print(
                "[INFO] Lag bars LONG : "
                f"A_MOD={long_cfg.lag_bars_long_a_mod_break_c1_high}, "
                f"A_CONT={long_cfg.lag_bars_long_a_close_continuation_break}, "
                f"A_PULLBACK={long_cfg.lag_bars_long_a_pullback_c2_break_c2_high}, "
                f"B_HUGE_HOLD={long_cfg.lag_bars_long_b_huge_pullback_hold_break}, "
                f"B_RECLAIM={long_cfg.lag_bars_long_b_huge_c1_close_reclaim_break}"
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
                f"SHORT_BE={int(V17C_SHORT_ENABLE_BREAKEVEN)} "
                f"(trigger={V17C_SHORT_BE_TRIGGER_PCT*100:.2f}%) | "
                f"SHORT_TRAIL={int(V17C_SHORT_ENABLE_TRAILING_STOP)} "
                f"(trail={V17C_SHORT_TRAIL_PCT*100:.2f}%)"
            )
            print(f"[INFO] Live parity: min_bars_left=0 -> {FORCE_LIVE_PARITY_MIN_BARS_LEFT}")
            print(f"[INFO] Live parity: disable_topn_per_day -> {FORCE_LIVE_PARITY_DISABLE_TOPN}")
            print(
                "[INFO] Exit realism band: "
                f"enabled={EXIT_REALISM_BAND_ENABLED} | "
                f"use_stressed_base={EXIT_REALISM_USE_STRESSED_BASE} | "
                f"stop_extra_slip={STOP_EXIT_EXTRA_SLIPPAGE_BPS:.1f}bps"
            )
            print(
                f"[INFO] Parallelism: max_workers={MAX_WORKERS} | "
                f"executor={_resolve_executor_mode(MAX_WORKERS)}"
            )
            print(
                f"[INFO] Chart generation: legacy={ENABLE_LEGACY_CHARTS} | "
                f"enhanced={ENABLE_ENHANCED_CHARTS}"
            )
            print(f"[INFO] Output directory: {_outputs_dir}")
            print(f"[INFO] Console log: {log_path}")
            print("-" * 70)

            # ---- PHASE 1: Scan for entry signals using 5-min data ----
            print("\n[PHASE 1] Scanning for entry signals using 5-min data...")
            phase1_started = time.perf_counter()
            short_df, long_df = _run_both_parallel(short_cfg, long_cfg, MAX_WORKERS)

            if NIFTY_CONTEXT_ENABLED:
                mode_map, nifty_ret_map, context_src, context_counts = _build_nifty_intraday_context(short_cfg)
                if mode_map:
                    both_rs_info = (
                        f" | BOTH-mode LONG RS>={NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT:.2f}%"
                        f", SHORT RS<=-{NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT:.2f}%"
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

            short_df, long_df = _replace_current_day_with_live_parity(short_df, long_df)

            # ---- V16: Enrich with Opening Range levels (required for OR gate) ----
            if V16_OR_GATE_ENABLED:
                print("\n[V16] Computing Opening Range levels for OR gate...")
                short_df, long_df = _enrich_with_or_levels(
                    short_df, long_df,
                    dir_15m=str(dir_15m),
                    parquet_suffix=short_cfg.end_15m,
                )

            # ---- V16 Run14: Enrich LONG trades with entry bar volume ratio ----
            if V16_LONG_ENTRY_VOL_EXHAUST_ENABLED:
                print("\n[V16] Computing entry bar volume ratios for exhaustion filter...")
                long_df = _enrich_with_entry_vol_ratio(
                    long_df,
                    dir_15m=str(dir_15m),
                    parquet_suffix=short_cfg.end_15m,
                )

            # ---- V16: Apply anti-exhaustion post-scan filters ----
            print("\n[V16] Applying anti-exhaustion post-scan filters...")
            short_df, long_df = _apply_v16_post_scan_filters(short_df, long_df)
            print("\n[V17C] Applying setup-specific quality filters...")
            short_df, long_df = _apply_v17c_setup_quality_filters(short_df, long_df, short_cfg)
            print("\n[V18c2] Applying session-wise historical shortlist universe...")
            short_df = _apply_v18_session_shortlists(short_df, "SHORT")
            long_df = _apply_v18_session_shortlists(long_df, "LONG")
            print("\n[V18c2] Applying post-shortlist trade cleanup...")
            short_df = _apply_v18c2_trade_cleanup(short_df, "SHORT")
            long_df = _apply_v18c2_trade_cleanup(long_df, "LONG")
            print(f"[TIMING] Phase 1 completed in {time.perf_counter() - phase1_started:.1f}s")

            if short_df.empty and long_df.empty:
                print("[DONE] No trades found.")
                return

            # ---- PHASE 2: Re-resolve exits using intrabar data ----
            print("\n[PHASE 2] Re-resolving exits using 1-min intrabar data...")
            phase2_started = time.perf_counter()

            # Determine intrabar file suffix by inspecting the 1-min directory
            suffix_5m = ".parquet"
            if dir_1m.is_dir():
                sample_files = list(dir_1m.glob("*"))[:5]
                for sf in sample_files:
                    if sf.suffix:
                        suffix_5m = sf.suffix
                        break

            if not short_df.empty:
                print(f"  [SHORT] {len(short_df)} trades to re-resolve...")
                short_df = _resolve_exits_5min(
                    short_df,
                    dir_1m,
                    suffix_5m,
                    short_cfg.parquet_engine,
                    eod_exit_time=V15_EOD_EXIT_TIME,
                )

            if not long_df.empty:
                print(f"  [LONG] {len(long_df)} trades to re-resolve...")
                long_df = _resolve_exits_5min(
                    long_df,
                    dir_1m,
                    suffix_5m,
                    long_cfg.parquet_engine,
                    eod_exit_time=V15_EOD_EXIT_TIME,
                )
            print(f"[TIMING] Phase 2 completed in {time.perf_counter() - phase2_started:.1f}s")

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
            print_metrics("SHORT (net of slippage+comm, intrabar exits)", compute_backtest_metrics(short_df))
            print_metrics("LONG (net of slippage+comm, intrabar exits)", compute_backtest_metrics(long_df))
            print_metrics("COMBINED (net of slippage+comm, intrabar exits)", compute_backtest_metrics(combined))
            if EXIT_REALISM_BAND_ENABLED:
                primary_variant = (
                    "pessimistic stressed path" if EXIT_REALISM_USE_STRESSED_BASE
                    else "legacy base intrabar path"
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
            out_csv = _outputs_dir / f"avwap_longshort_trades_v18c2_5min_ALL_DAYS_{ts}.csv"
            combined.to_csv(out_csv, index=False)
            out_daywise_csv = _outputs_dir / f"avwap_daywise_breakdown_v18c2_5min_ALL_DAYS_{ts}.csv"
            _build_daily_breakdown_df(combined, include_total=True).to_csv(out_daywise_csv, index=False)

            charts_started = time.perf_counter()
            chart_files_legacy: List[str] = []
            chart_files_enhanced: List[str] = []

            # --- Generate Legacy Charts (from avwap_common) ---
            if ENABLE_LEGACY_CHARTS:
                print("\n[INFO] Generating legacy backtest charts...")
                chart_dir_legacy = _outputs_dir / "charts" / "legacy"
                chart_files_legacy = generate_backtest_charts(
                    combined, short_df, long_df, save_dir=chart_dir_legacy, ts_label=ts,
                )
                if chart_files_legacy:
                    print(f"[INFO] {len(chart_files_legacy)} legacy charts saved to {chart_dir_legacy}/")
            else:
                print("\n[INFO] Skipping legacy backtest charts (EQIDV17C_5MIN_ENABLE_LEGACY_CHARTS=0).")

            # --- Generate Enhanced Charts ---
            if ENABLE_ENHANCED_CHARTS:
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
            else:
                print("[INFO] Skipping enhanced analysis charts (EQIDV17C_5MIN_ENABLE_ENHANCED_CHARTS=0).")

            total_charts = len(chart_files_legacy or []) + len(chart_files_enhanced or [])
            print(f"\n[INFO] Total charts generated: {total_charts}")
            print(f"[TIMING] Chart generation completed in {time.perf_counter() - charts_started:.1f}s")

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
            print(f"[DAYWISE CSV] {out_daywise_csv}")
            print(f"[OUTPUTS DIR] {_outputs_dir}")
            print(f"[CONSOLE LOG] {log_path}")
            print(f"[TIMING] Total runtime: {time.perf_counter() - run_started:.1f}s")
            print("[DONE]")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr

    # Also print after restoring, for convenience when running interactively
    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()
