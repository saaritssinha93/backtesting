# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v8_sweep.py  -  AVWAP V8 Sweep Runner
============================================================

V8 improvements over V7:
  1. ATR-based targets (T1 / T2 / T3 as multiples of per-trade ATR)
  2. Partial exit simulation  (50 % T1, 30 % T2, 20 % T3 runner)
  3. SL moves to breakeven after T1 hit  (T2 / T3 phase)
  4. Minimum R:R gate  (drop signal if T1 / SL ratio < threshold)
  5. Short anti-chase limit queue  (mirrors V5 long pending logic)
  6. Market regime filter  REQUIRED
  7. Day-loss guard  REQUIRED  (threshold is a sweep axis)
  8. 20-config sweep covering SL, target, R:R, timing, VIX

Sweep groups
  S01-S02   Baselines (V7 fixed-% vs V8 standard)
  S03-S05   SL magnitude  (tight / wide / ATR-based)
  S06-S08   Single-target ATR multiplier  (1.5x / 2.0x / 2.5x)
  S09-S12   Partial exit structure
  S13-S14   Min R:R gate strength
  S15-S16   Signal time window
  S17-S18   Day-loss guard threshold
  S19-S20   VIX scaling + best-estimated combo

Usage
  python avwap_combined_runner_v8_sweep.py
"""

from __future__ import annotations

import heapq
import os
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Console tee  (stdout + stderr → console + log file)
# ---------------------------------------------------------------------------
class _Tee:
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


# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent if _this_dir.name == "avwap_v11_refactored" else _this_dir
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from avwap_v11_refactored.avwap_common_v7_sweep import (
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
from avwap_v11_refactored.avwap_short_strategy_v7_sweep import (
    scan_all_days_for_ticker as scan_short,
)
from avwap_v11_refactored.avwap_long_strategy_v7_sweep import (
    scan_all_days_for_ticker as scan_long,
)


# ===========================================================================
# TOP-LEVEL RUNNER CONSTANTS  (infrastructure, not swept)
# ===========================================================================
POSITION_SIZE_RS_SHORT      = 50_000
POSITION_SIZE_RS_LONG       = 100_000
INTRADAY_LEVERAGE_SHORT     = 5.0
INTRADAY_LEVERAGE_LONG      = 5.0
PORTFOLIO_START_CAPITAL_RS  = 1_000_000
MAX_WORKERS                 = 4

FORCE_LIVE_PARITY_MIN_BARS_LEFT   = True
FORCE_LIVE_PARITY_DISABLE_TOPN    = True

FINAL_SIGNAL_WINDOW_OVERRIDE  = True
SHORT_LAG_BARS_A_MOD           = 1
SHORT_LAG_BARS_A_PULLBACK      = 2
SHORT_LAG_BARS_B_HUGE          = -1
LONG_LAG_BARS_A_MOD            = 1
LONG_LAG_BARS_A_PULLBACK       = 2
LONG_LAG_BARS_B_HUGE           = -1

PACK2_ENABLE_B_HUGE_SHORT      = True   # enable huge-failed-bounce short setups
PACK2_SHORT_MAX_VIX            = 99.0  # effectively disabled — scan is inclusive

# Sweep outputs go here
SWEEP_OUTPUTS_SUBDIR = "outputs_v8_sweep"

# If True, generate charts only for the top-ranked sweep config.
CHARTS_FOR_BEST_ONLY = True


# ===========================================================================
# V8 SWEEP CONFIG DATACLASS
# ===========================================================================
@dataclass
class V8SweepConfig:
    """All tunable parameters for one sweep run."""

    name: str
    description: str

    # --- SL parameters ---
    short_sl_pct: float  = 0.0075    # 0.75 % above entry
    long_sl_pct:  float  = 0.0060    # 0.60 % below entry
    sl_atr_based: bool   = False     # True → SL = sl_atr_mult * ATR
    sl_atr_mult:  float  = 1.0       # used when sl_atr_based=True

    # --- Target parameters ---
    use_atr_targets:   bool  = True    # False → use fixed_target_pct
    short_target_pct:  float = 0.0090  # fixed target (use_atr_targets=False)
    long_target_pct:   float = 0.0110  # fixed target (use_atr_targets=False)
    t1_atr_mult:       float = 1.5     # T1 = entry ± t1 * ATR
    t2_atr_mult:       float = 2.5     # T2 = entry ± t2 * ATR
    t3_atr_mult:       float = 4.0     # T3 = entry ± t3 * ATR

    # --- Partial exit fractions (must sum to 1.0) ---
    use_partial_exits: bool  = True
    frac_t1: float = 0.50
    frac_t2: float = 0.30
    frac_t3: float = 0.20

    # --- Min R:R gate (0 = disabled) ---
    min_rr: float = 0.0

    # --- Quality score gate (post-scan; 0 = disabled) ---
    # NOTE: short and long quality scores use DIFFERENT scales:
    #   - compute_quality_score_short → [0.0, 1.0]  (normalized)
    #   - compute_quality_score_long  → [0.0, ~12]  (raw)
    # quality_score_min applies to LONG side (0-12 scale).
    # short_quality_score_min applies to SHORT side (0-1 scale).
    #   When short_quality_score_min is None, it is auto-derived as
    #   quality_score_min / 12.0 so both gates are equivalent.
    quality_score_min: float = 4.0
    short_quality_score_min: Optional[float] = None  # None → quality_score_min / 12

    # --- Daily trade count control ---
    # Target: 5-15 entries per day (long + short combined).
    # max_trades_per_day: hard cap — keep top-N by quality_score per day.
    # max_per_side_per_day: sub-cap per direction (prevents one side monopolising).
    # min_trades_per_day_warn: warn if average falls below this (not a hard filter).
    max_trades_per_day:      int   = 15
    max_per_side_per_day:    int   = 8   # max shorts OR longs in one day
    min_trades_per_day_warn: int   = 5   # warn if avg < this

    # --- Short anti-chase ---
    short_anticache_offset_pct: float = 0.005  # +0.5 % anti-chase
    short_anticache_bars:       int   = 2      # expire after N bars

    # --- Day-loss guard ---
    day_loss_guard_pct: float = -6.0   # halt day if cum % < this

    # --- Signal time window ---
    signal_start: dtime = field(default_factory=lambda: dtime(9, 15))
    signal_end:   dtime = field(default_factory=lambda: dtime(14, 30))

    # --- VIX dynamic scaling ---
    vix_scale_enabled: bool  = False
    vix_baseline:      float = 11.5
    vix_scale_min:     float = 0.75
    vix_scale_max:     float = 1.50
    vix_scale_target:  bool  = True
    vix_scale_sl:      bool  = True


# ===========================================================================
# 20 SWEEP CONFIGURATIONS
# ===========================================================================
SWEEP_CONFIGS: List[V8SweepConfig] = [

    # ---- Group A: Baselines ------------------------------------------------
    V8SweepConfig(
        name="S01_V7_BASELINE",
        description="V7 style: fixed SL/TGT, no partial, no min-RR, QS>=3, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=False, short_target_pct=0.0090, long_target_pct=0.0110,
        use_partial_exits=False, min_rr=0.0, quality_score_min=3.0,
        day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S02_V8_STANDARD",
        description="V8 standard: ATR partial 50/30/20, SL=0.75/0.60, QS>=4, min_rr=1.5, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),

    # ---- Group B: SL magnitude (all with QS>=4, partial, min_rr=1.5) ------
    V8SweepConfig(
        name="S03_TIGHT_SL",
        description="Tight SL 0.50/0.40%, QS>=4, partial 50/30/20, max15/day",
        short_sl_pct=0.0050, long_sl_pct=0.0040,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S04_WIDE_SL",
        description="Wide SL 1.00/0.80%, QS>=4, partial 50/30/20, max15/day",
        short_sl_pct=0.0100, long_sl_pct=0.0080,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S05_ATR_SL",
        description="ATR-based SL 1.0*ATR, QS>=4, partial 50/30/20, max15/day",
        sl_atr_based=True, sl_atr_mult=1.0,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),

    # ---- Group C: Single-target ATR multiplier sweep ----------------------
    V8SweepConfig(
        name="S06_SINGLE_T1_1p5x",
        description="Single 100% exit at T1=1.5*ATR, QS>=4, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5,
        use_partial_exits=False, min_rr=1.5, quality_score_min=4.0,
        day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S07_SINGLE_T1_2p0x",
        description="Single 100% exit at T1=2.0*ATR, QS>=4, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=2.0,
        use_partial_exits=False, min_rr=1.5, quality_score_min=4.0,
        day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S08_SINGLE_T1_2p5x",
        description="Single 100% exit at T1=2.5*ATR, QS>=5, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=2.5,
        use_partial_exits=False, min_rr=2.0, quality_score_min=5.0,
        day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),

    # ---- Group D: Partial exit structure variations -----------------------
    V8SweepConfig(
        name="S09_PARTIAL_50_50",
        description="2-target 50/50: T1=1.5x, T2=3.0x, QS>=4, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=3.0, t3_atr_mult=3.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.50, frac_t3=0.00,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S10_PARTIAL_60_40",
        description="2-target 60/40: T1=1.5x, T2=2.5x, QS>=4, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=2.5,
        use_partial_exits=True, frac_t1=0.60, frac_t2=0.40, frac_t3=0.00,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S11_PARTIAL_AGGR_T1",
        description="Partial 50/30/20, fast T1=1.2x, QS>=3 (volume), max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.2, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.2, quality_score_min=3.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S12_PARTIAL_CONSRV_T1",
        description="Partial 50/30/20, slow T1=2.0x, QS>=5 (high quality), max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=2.0, t2_atr_mult=3.0, t3_atr_mult=4.5,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=2.0, quality_score_min=5.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),

    # ---- Group E: Quality score gate + Min R:R combos ---------------------
    V8SweepConfig(
        name="S13_QS3_MINRR_1p2",
        description="Lower quality bar: QS>=3, min_rr=1.2 → more trades, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.2, quality_score_min=3.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S14_QS5_MINRR_2p0",
        description="High quality bar: QS>=5, min_rr=2.0 → fewer, better trades, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=2.0, quality_score_min=5.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),

    # ---- Group F: Signal time window + count variations -------------------
    V8SweepConfig(
        name="S15_MORNING_ONLY",
        description="Morning 09:15-12:30, QS>=4, partial, max10/day (5 per side)",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=10, max_per_side_per_day=5, min_trades_per_day_warn=5,
        signal_start=dtime(9, 15), signal_end=dtime(12, 30),
    ),
    V8SweepConfig(
        name="S16_RESTRICTED_WIN",
        description="Restricted 09:45-14:00, QS>=4, partial, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
        signal_start=dtime(9, 45), signal_end=dtime(14, 0),
    ),

    # ---- Group G: Day-loss guard threshold variations ---------------------
    V8SweepConfig(
        name="S17_GUARD_TIGHT",
        description="Day-loss guard -4%, QS>=4, partial, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-4.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),
    V8SweepConfig(
        name="S18_GUARD_LOOSE",
        description="Day-loss guard -8%, QS>=4, partial, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-8.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
    ),

    # ---- Group H: VIX scaling + best-estimated combo ----------------------
    V8SweepConfig(
        name="S19_VIX_SCALING",
        description="VIX dynamic scaling ON, QS>=4, partial, max15/day",
        short_sl_pct=0.0075, long_sl_pct=0.0060,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=4.0, day_loss_guard_pct=-6.0,
        max_trades_per_day=15, max_per_side_per_day=8, min_trades_per_day_warn=5,
        vix_scale_enabled=True, vix_baseline=11.5, vix_scale_min=0.75, vix_scale_max=1.50,
    ),
    V8SweepConfig(
        name="S20_BEST_COMBO",
        description="Tight SL + T1=1.5x partial + QS>=5 + min_rr=1.5 + 09:15-13:30 + guard=-5% + max12/day",
        short_sl_pct=0.0055, long_sl_pct=0.0045,
        use_atr_targets=True, t1_atr_mult=1.5, t2_atr_mult=2.5, t3_atr_mult=4.0,
        use_partial_exits=True, frac_t1=0.50, frac_t2=0.30, frac_t3=0.20,
        min_rr=1.5, quality_score_min=5.0, day_loss_guard_pct=-5.0,
        max_trades_per_day=12, max_per_side_per_day=6, min_trades_per_day_warn=5,
        signal_start=dtime(9, 15), signal_end=dtime(13, 30),
        vix_scale_enabled=False,
    ),
]


# ===========================================================================
# HELPERS: ATR EXTRACTION
# ===========================================================================
def _get_atr_series(df: pd.DataFrame, entry_col: str = "entry_price") -> pd.Series:
    """
    Extract per-trade ATR value in absolute price terms.
    Priority: 'atr' col (absolute) -> 'atr_pct' col (% of close) -> SL-proxy fallback.
    """
    if "atr" in df.columns:
        atr = pd.to_numeric(df["atr"], errors="coerce")
        if atr.gt(0).any():
            return atr.fillna(0)

    if "atr_pct_signal" in df.columns and entry_col in df.columns:
        ep = pd.to_numeric(df[entry_col], errors="coerce").fillna(1.0)
        # atr_pct_signal is a ratio (atr/close), NOT a percentage — multiply directly
        atr = pd.to_numeric(df["atr_pct_signal"], errors="coerce").fillna(0) * ep
        if atr.gt(0).any():
            return atr

    # Fallback: use SL distance as ATR proxy
    sl_col = "stop_price" if "stop_price" in df.columns else "sl_price"
    if sl_col in df.columns and entry_col in df.columns:
        ep = pd.to_numeric(df[entry_col], errors="coerce").fillna(1.0)
        sp = pd.to_numeric(df[sl_col], errors="coerce").fillna(ep)
        atr = (ep - sp).abs()
        return atr.clip(lower=1e-6)

    return pd.Series(np.zeros(len(df)), index=df.index)


# ===========================================================================
# ATR-BASED TARGET COMPUTATION
# ===========================================================================
def _apply_atr_targets(df: pd.DataFrame, cfg: V8SweepConfig) -> pd.DataFrame:
    """
    Add / replace target_price with ATR-based T1.
    Also add t1_price, t2_price, t3_price columns for partial exit use.
    If use_atr_targets=False, keep existing target_price unchanged.
    """
    if df.empty:
        return df
    d = df.copy()
    side = d["side"].astype(str).str.upper()
    ep = pd.to_numeric(d["entry_price"], errors="coerce").fillna(0)
    atr = _get_atr_series(d)

    # ATR-based SL override
    if cfg.sl_atr_based:
        sl_col = "stop_price" if "stop_price" in d.columns else "sl_price"
        sl_dist = atr * cfg.sl_atr_mult
        d.loc[side.eq("SHORT"), sl_col] = (ep + sl_dist)[side.eq("SHORT")]
        d.loc[side.eq("LONG"),  sl_col] = (ep - sl_dist)[side.eq("LONG")]
    else:
        sl_col = "stop_price" if "stop_price" in d.columns else "sl_price"
        d.loc[side.eq("SHORT"), sl_col] = ep[side.eq("SHORT")] * (1 + cfg.short_sl_pct)
        d.loc[side.eq("LONG"),  sl_col] = ep[side.eq("LONG")]  * (1 - cfg.long_sl_pct)

    if cfg.use_atr_targets:
        t1_short = ep - atr * cfg.t1_atr_mult
        t1_long  = ep + atr * cfg.t1_atr_mult
        t2_short = ep - atr * cfg.t2_atr_mult
        t2_long  = ep + atr * cfg.t2_atr_mult
        t3_short = ep - atr * cfg.t3_atr_mult
        t3_long  = ep + atr * cfg.t3_atr_mult

        d["t1_price"] = np.where(side.eq("SHORT"), t1_short, t1_long)
        d["t2_price"] = np.where(side.eq("SHORT"), t2_short, t2_long)
        d["t3_price"] = np.where(side.eq("SHORT"), t3_short, t3_long)
        d["target_price"] = d["t1_price"]
    else:
        # Fixed % targets
        t1_short = ep * (1 - cfg.short_target_pct)
        t1_long  = ep * (1 + cfg.long_target_pct)
        d["t1_price"] = np.where(side.eq("SHORT"), t1_short, t1_long)
        d["t2_price"] = d["t1_price"]
        d["t3_price"] = d["t1_price"]
        d["target_price"] = d["t1_price"]

    return d


# ===========================================================================
# MIN R:R GATE
# ===========================================================================
def _apply_min_rr_filter(df: pd.DataFrame, min_rr: float) -> Tuple[pd.DataFrame, int]:
    """
    Drop trades where (T1 - entry) / (entry - SL) < min_rr.
    Returns (filtered_df, n_dropped).
    """
    if df.empty or min_rr <= 0:
        return df, 0
    d = df.copy()
    ep = pd.to_numeric(d["entry_price"], errors="coerce").fillna(0)
    sl_col = "stop_price" if "stop_price" in d.columns else "sl_price"
    sp = pd.to_numeric(d[sl_col], errors="coerce").fillna(ep)
    t1 = pd.to_numeric(d.get("t1_price", d.get("target_price", ep)), errors="coerce").fillna(ep)
    side = d["side"].astype(str).str.upper()

    reward = np.where(side.eq("SHORT"), ep - t1, t1 - ep)
    risk   = np.where(side.eq("SHORT"), sp - ep, ep - sp)
    risk   = np.where(risk > 0, risk, 1e-9)

    rr = reward / risk
    mask = pd.Series(rr >= min_rr, index=d.index)
    n_dropped = int((~mask).sum())
    return d[mask].copy(), n_dropped


# ===========================================================================
# SHORT ANTI-CHASE LIMIT QUEUE  (new in V8)
# ===========================================================================
def _apply_short_anticache(df: pd.DataFrame, cfg: V8SweepConfig) -> Tuple[pd.DataFrame, int]:
    """
    Simulate short anti-chase: a short signal is only filled if price
    rises to limit_price = entry * (1 + offset) within N bars.

    Since we're backtesting without tick data, we approximate:
    - Use the HIGH of the entry bar + 1 bars forward.
    - If bar HIGH within the wait window >= limit_price, signal is accepted.
    - Otherwise it expires.

    NOTE: This requires an 'entry_bar_high' or 'signal_bar_high' column.
    If unavailable, the filter is skipped (short signals accepted as-is).
    """
    if df.empty or cfg.short_anticache_offset_pct <= 0:
        return df, 0

    shorts = df["side"].astype(str).str.upper().eq("SHORT")
    if not shorts.any():
        return df, 0

    # Check if we have enough data to simulate
    high_col = None
    for c in ["entry_bar_high", "signal_bar_high", "bar_high"]:
        if c in df.columns:
            high_col = c
            break

    if high_col is None:
        # Cannot simulate — accept all shorts
        return df, 0

    d = df.copy()
    ep = pd.to_numeric(d["entry_price"], errors="coerce").fillna(0)
    limit_price = ep * (1 + cfg.short_anticache_offset_pct)
    bar_high    = pd.to_numeric(d[high_col], errors="coerce").fillna(0)

    # SHORT: filled only if bar_high >= limit_price within wait window
    filled = bar_high >= limit_price
    # For non-shorts, always accept
    keep = (~shorts) | (shorts & filled)
    n_expired = int((shorts & ~filled).sum())
    return d[keep].copy(), n_expired


# ===========================================================================
# PARTIAL EXIT SIMULATION  (core V8 logic)
# ===========================================================================
def _walk_bars_to_exit(
    bars: pd.DataFrame,
    side: str,
    stop_price: float,
    target_price: float,
    entry_price: float,
) -> Tuple[str, float, Optional[pd.Timestamp]]:
    """
    Walk 5-min bars in order.  Return (outcome, exit_price, exit_time).
    outcome: 'TARGET' | 'SL' | 'EOD'
    """
    for _, bar in bars.iterrows():
        bh = float(bar.get("high", bar.get("High", np.nan)))
        bl = float(bar.get("low",  bar.get("Low",  np.nan)))
        bc = float(bar.get("close", bar.get("Close", entry_price)))
        bt = bar["datetime"]
        if np.isnan(bh) or np.isnan(bl):
            continue
        if side == "SHORT":
            if bh >= stop_price:
                return "SL", stop_price, bt
            if bl <= target_price:
                return "TARGET", target_price, bt
        else:
            if bl <= stop_price:
                return "SL", stop_price, bt
            if bh >= target_price:
                return "TARGET", target_price, bt

    # EOD: use last bar close
    if not bars.empty:
        last = bars.iloc[-1]
        eod_price = float(last.get("close", last.get("Close", entry_price)))
        return "EOD", eod_price, last["datetime"]
    return "EOD", entry_price, None


def _resolve_one_trade_partial(
    entry_price: float,
    stop_price:  float,
    side:        str,
    entry_time:  pd.Timestamp,
    trade_date:  pd.Timestamp,
    bars_5m_all: pd.DataFrame,
    t1:          float,
    t2:          float,
    t3:          float,
    frac_t1:     float,
    frac_t2:     float,
    frac_t3:     float,
    use_partial: bool,
) -> Tuple[float, float, str, Optional[pd.Timestamp]]:
    """
    Walk 5-min bars for one trade through T1 → T2 → T3 phases.

    Returns (pnl_pct_price, exit_price_weighted, composite_outcome, final_exit_time).
    pnl_pct_price = unlevered price-return % (weighted across partial exits).
    """
    if bars_5m_all.empty:
        return 0.0, entry_price, "NO_DATA", None

    mask = (
        (bars_5m_all["datetime"] > entry_time) &
        (bars_5m_all["datetime"].dt.normalize() == trade_date)
    )
    bars = bars_5m_all[mask].sort_values("datetime").copy()

    if bars.empty:
        return 0.0, entry_price, "EOD", None

    def pct_ret(exit_px: float) -> float:
        if entry_price == 0:
            return 0.0
        if side == "SHORT":
            return (entry_price - exit_px) / entry_price * 100.0
        return (exit_px - entry_price) / entry_price * 100.0

    if not use_partial or frac_t2 == 0.0:
        # Single-target exit
        outcome, xp, xt = _walk_bars_to_exit(bars, side, stop_price, t1, entry_price)
        return pct_ret(xp), xp, outcome, xt

    # ---- Phase 1: hunt T1 (SL = original stop) ----------------------------
    out1, xp1, xt1 = _walk_bars_to_exit(bars, side, stop_price, t1, entry_price)

    if out1 != "TARGET":
        # T1 not hit → all fractions exit at SL or EOD
        pnl = pct_ret(xp1)
        return pnl, xp1, out1, xt1

    # T1 hit: frac_t1 exits here
    pnl_t1 = pct_ret(xp1) * frac_t1

    if frac_t2 == 0.0:
        return pnl_t1, xp1, "T1", xt1

    # ---- Phase 2: hunt T2 (SL moves to entry = breakeven) -----------------
    bars2 = bars[bars["datetime"] > xt1] if xt1 is not None else bars
    sl2   = entry_price   # breakeven
    out2, xp2, xt2 = _walk_bars_to_exit(bars2, side, sl2, t2, entry_price)

    pnl_t2 = pct_ret(xp2) * frac_t2

    if frac_t3 == 0.0 or out2 != "TARGET":
        # T2 is the last target, or T2 was not hit
        total_pnl = pnl_t1 + pnl_t2
        composite  = f"T1+{out2}"
        return total_pnl, xp2, composite, xt2

    # ---- Phase 3: hunt T3 (SL trails to T1 level) -------------------------
    bars3 = bars[bars["datetime"] > xt2] if xt2 is not None else bars
    sl3   = xp1   # trail SL to T1 price (locks in T1 level on runner)
    out3, xp3, xt3 = _walk_bars_to_exit(bars3, side, sl3, t3, entry_price)

    pnl_t3    = pct_ret(xp3) * frac_t3
    total_pnl = pnl_t1 + pnl_t2 + pnl_t3
    composite  = f"T1+T2+{out3}"
    return total_pnl, xp3, composite, xt3


def _resolve_exits_partial(
    trades_df: pd.DataFrame,
    dir_5m:    Path,
    cfg:       V8SweepConfig,
    engine:    str = "pyarrow",
) -> pd.DataFrame:
    """
    Re-resolve exits for all trades using 5-min data + partial exit logic.
    Replaces exit_price, outcome, pnl_pct_gross with partial-exit versions.
    """
    if trades_df.empty:
        return trades_df
    if not dir_5m.is_dir():
        print(f"[WARN] 5-min dir not found: {dir_5m}  — using 15-min exit fallback.")
        return trades_df

    d = trades_df.copy()
    sl_col = "stop_price" if "stop_price" in d.columns else "sl_price"
    required = {"ticker", "side", "entry_price", "entry_time_ist", sl_col, "t1_price"}
    if not required.issubset(d.columns):
        print(f"[WARN] Partial exit resolver: missing columns {required - set(d.columns)}")
        return d

    for c in ["entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")

    _cache: Dict[str, pd.DataFrame] = {}

    def _load(ticker: str) -> pd.DataFrame:
        if ticker in _cache:
            return _cache[ticker]
        for pat in [
            f"{ticker}.parquet",
            f"{ticker}_1min.parquet",
            f"{ticker}_5min.parquet",
            f"{ticker}_eq_1min.parquet",
            f"{ticker}_eq_5min.parquet",
            f"{ticker}_15min.parquet",
        ]:
            fp = dir_5m / pat
            if fp.exists():
                try:
                    df5 = pd.read_parquet(fp, engine=engine)
                    # Normalise datetime column
                    for col in ("datetime", "date", "DateTime", "timestamp"):
                        if col in df5.columns:
                            df5 = df5.rename(columns={col: "datetime"}) if col != "datetime" else df5
                            break
                    if df5.index.name == "datetime" or isinstance(df5.index, pd.DatetimeIndex):
                        df5 = df5.reset_index()
                    if "datetime" in df5.columns:
                        df5["datetime"] = pd.to_datetime(df5["datetime"], errors="coerce")
                    _cache[ticker] = df5
                    return df5
                except Exception:
                    pass
        _cache[ticker] = pd.DataFrame()
        return pd.DataFrame()

    updated = 0
    for idx in d.index:
        ticker     = str(d.at[idx, "ticker"])
        side       = str(d.at[idx, "side"]).upper()
        ep         = float(d.at[idx, "entry_price"])
        sp         = float(d.at[idx, sl_col]) if pd.notna(d.at[idx, sl_col]) else ep
        t1         = float(d.at[idx, "t1_price"]) if pd.notna(d.at[idx, "t1_price"]) else ep
        t2         = float(d.at[idx, "t2_price"]) if "t2_price" in d.columns and pd.notna(d.at[idx, "t2_price"]) else t1
        t3         = float(d.at[idx, "t3_price"]) if "t3_price" in d.columns and pd.notna(d.at[idx, "t3_price"]) else t1
        entry_time = d.at[idx, "entry_time_ist"]
        if pd.isna(entry_time):
            continue
        trade_date = pd.Timestamp(entry_time).normalize()
        bars_5m    = _load(ticker)

        pnl_pct, exit_px, outcome, exit_t = _resolve_one_trade_partial(
            entry_price=ep, stop_price=sp, side=side,
            entry_time=entry_time, trade_date=trade_date,
            bars_5m_all=bars_5m,
            t1=t1, t2=t2, t3=t3,
            frac_t1=cfg.frac_t1, frac_t2=cfg.frac_t2, frac_t3=cfg.frac_t3,
            use_partial=cfg.use_partial_exits,
        )

        d.at[idx, "exit_price"]     = exit_px
        d.at[idx, "exit_time_ist"]  = exit_t
        d.at[idx, "outcome"]        = outcome
        d.at[idx, "pnl_pct_gross"]  = pnl_pct

        slippage_pct   = float(d.at[idx, "slippage_pct"])   if "slippage_pct"   in d.columns and pd.notna(d.at[idx, "slippage_pct"])   else 0.0005
        commission_pct = float(d.at[idx, "commission_pct"]) if "commission_pct" in d.columns and pd.notna(d.at[idx, "commission_pct"]) else 0.0003
        cost           = (slippage_pct + commission_pct) * 100.0 * 2
        d.at[idx, "pnl_pct"] = pnl_pct - cost
        updated += 1

    print(f"  [PARTIAL-EXIT] Resolved {updated}/{len(d)} trades.")
    return d


# ===========================================================================
# NOTIONAL P&L  (reused from V7 logic)
# ===========================================================================
def _add_notional_pnl(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    if "pnl_pct_price" not in d.columns:
        d["pnl_pct_price"] = pd.to_numeric(d.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)
    side_u = d["side"].astype(str).str.upper() if "side" in d.columns else pd.Series([""] * len(d))
    if "position_size_rs" not in d.columns:
        d["position_size_rs"] = np.nan
    d.loc[side_u.eq("SHORT") & d["position_size_rs"].isna(), "position_size_rs"] = float(POSITION_SIZE_RS_SHORT)
    d.loc[~side_u.eq("SHORT") & d["position_size_rs"].isna(), "position_size_rs"] = float(POSITION_SIZE_RS_LONG)
    d["position_size_rs"] = pd.to_numeric(d["position_size_rs"], errors="coerce").fillna(0.0)
    if "leverage" not in d.columns:
        d["leverage"] = np.nan
    d.loc[side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_SHORT)
    d.loc[~side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_LONG)
    d["leverage"] = pd.to_numeric(d["leverage"], errors="coerce").fillna(1.0)
    d["notional_exposure_rs"] = d["position_size_rs"] * d["leverage"]
    d["pnl_pct"]       = d["pnl_pct_price"] * d["leverage"]
    d["pnl_pct_gross"] = pd.to_numeric(d.get("pnl_pct_gross", d["pnl_pct_price"]), errors="coerce").fillna(0.0) * d["leverage"]
    d["pnl_rs"]        = (d["pnl_pct_price"] / 100.0) * d["notional_exposure_rs"]
    d["pnl_rs_gross"]  = (pd.to_numeric(d.get("pnl_pct_gross", d["pnl_pct_price"]), errors="coerce").fillna(0.0) / 100.0) * d["notional_exposure_rs"]
    return d


# ===========================================================================
# QUALITY SCORE FILTER  (post-scan, pre-cap)
# ===========================================================================
def _apply_quality_filter(df: pd.DataFrame, min_score: float) -> Tuple[pd.DataFrame, int]:
    """Drop trades with quality_score < min_score. Returns (filtered_df, n_dropped)."""
    if df.empty or min_score <= 0:
        return df, 0
    if "quality_score" not in df.columns:
        return df, 0
    qs = pd.to_numeric(df["quality_score"], errors="coerce").fillna(0.0)
    mask = qs >= min_score
    return df[mask].copy(), int((~mask).sum())


# ===========================================================================
# DAILY TRADE COUNT ENFORCEMENT  (5-15 entries / day target)
# ===========================================================================
def _apply_daily_trade_limits(
    df: pd.DataFrame,
    max_trades_per_day:   int,
    max_per_side_per_day: int,
    min_warn:             int,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Enforce per-day trade-count limits so the strategy generates
    5-15 entries per day (long + short combined).

    Logic per day:
      1. Rank all signals by quality_score desc (higher = better).
         Tie-break by entry_time_ist asc (earlier = preferred).
      2. Apply side sub-cap: keep at most max_per_side_per_day per side.
      3. Apply global cap: keep at most max_trades_per_day total.

    Also warns if average trades/day < min_warn.
    """
    if df.empty:
        stats = {"days": 0, "avg_per_day": 0.0, "capped_days": 0, "dropped": 0, "days_below_min": 0}
        return df, stats

    d = df.copy()
    d["trade_date"]     = pd.to_datetime(d.get("trade_date"), errors="coerce").dt.date
    d["entry_time_ist"] = pd.to_datetime(d.get("entry_time_ist"), errors="coerce")
    d["side"]           = d["side"].astype(str).str.upper()
    if "quality_score" not in d.columns:
        d["quality_score"] = 0.0
    d["quality_score"] = pd.to_numeric(d["quality_score"], errors="coerce").fillna(0.0)

    keep_indices: List[int] = []
    capped_days  = 0
    total_input  = len(d)

    for _, day_df in d.groupby("trade_date", sort=True):
        # Sort: quality_score desc, entry_time asc
        sort_cols = [c for c in ["entry_time_ist", "quality_score"] if c in day_df.columns]
        day_sorted = day_df.sort_values(
            ["quality_score", "entry_time_ist"],
            ascending=[False, True],
        )

        short_count = 0
        long_count  = 0
        day_keep    = []

        for idx, row in day_sorted.iterrows():
            if len(day_keep) >= max_trades_per_day:
                break
            side = str(row.get("side", "")).upper()
            if side == "SHORT":
                if short_count >= max_per_side_per_day:
                    continue
                short_count += 1
            else:
                if long_count >= max_per_side_per_day:
                    continue
                long_count += 1
            day_keep.append(idx)

        if len(day_keep) < len(day_sorted):
            capped_days += 1
        keep_indices.extend(day_keep)

    out = d.loc[keep_indices].copy()
    dropped = total_input - len(out)

    # Stats
    if not out.empty:
        trades_per_day = out.groupby("trade_date").size()
        avg_per_day    = float(trades_per_day.mean())
        n_days         = int(len(trades_per_day))
        days_below_min = int((trades_per_day < min_warn).sum())
    else:
        avg_per_day = 0.0; n_days = 0; days_below_min = 0

    if avg_per_day < min_warn and n_days > 0:
        print(f"  [WARN] avg {avg_per_day:.1f} trades/day < target min {min_warn} "
              f"({days_below_min}/{n_days} days below threshold)")

    stats = {
        "days":          n_days,
        "avg_per_day":   round(avg_per_day, 1),
        "capped_days":   capped_days,
        "dropped":       dropped,
        "days_below_min": days_below_min,
    }
    return out, stats


# ===========================================================================
# DAY-LOSS GUARD
# ===========================================================================
def _apply_day_loss_guard(df: pd.DataFrame, threshold_pct: float) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if df.empty or threshold_pct >= 0:
        return df, {"blocked_trades": 0, "blocked_days": 0}
    d = df.copy()
    d["trade_date"]    = pd.to_datetime(d.get("trade_date"), errors="coerce").dt.date
    d["entry_time_ist"] = pd.to_datetime(d.get("entry_time_ist"), errors="coerce")
    d["pnl_pct"]       = pd.to_numeric(d.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)
    keep_idx, blocked_trades, blocked_days = [], 0, 0
    sort_cols = [c for c in ["entry_time_ist", "ticker", "side"] if c in d.columns]
    for _, day_df in d.groupby("trade_date", sort=True):
        day_df    = day_df.sort_values(sort_cols)
        day_cum   = 0.0
        day_block = False
        for idx, row in day_df.iterrows():
            if day_cum <= threshold_pct:
                blocked_trades += 1
                day_block = True
                continue
            keep_idx.append(idx)
            day_cum += float(row["pnl_pct"])
        if day_block:
            blocked_days += 1
    out = d.loc[keep_idx].copy()
    return out, {"blocked_trades": blocked_trades, "blocked_days": blocked_days}


# ===========================================================================
# SORT HELPER
# ===========================================================================
def _sort_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    if "trade_date" in d.columns:
        d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce")
    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")
    sc = [c for c in ["trade_date", "entry_time_ist", "signal_time_ist", "ticker", "side"] if c in d.columns]
    if sc:
        d = d.sort_values(sc).reset_index(drop=True)
    return d


# ===========================================================================
# PARALLEL SCANNING  (same as V7)
# ===========================================================================
def _scan_one_short(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    ticker, path, cfg_ = args
    df_ = read_15m_parquet(path, cfg_.parquet_engine)
    if df_.empty:
        return []
    return [asdict(t) for t in scan_short(ticker, df_, cfg_)]


def _scan_one_long(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    ticker, path, cfg_ = args
    df_ = read_15m_parquet(path, cfg_.parquet_engine)
    if df_.empty:
        return []
    return [asdict(t) for t in scan_long(ticker, df_, cfg_)]


def _run_side_parallel(side: str, cfg_: StrategyConfig) -> pd.DataFrame:
    tickers = list_tickers_15m(cfg_.dir_15m, cfg_.end_15m)
    print(f"  [{side}] Tickers: {len(tickers)}")
    worker = _scan_one_short if side == "SHORT" else _scan_one_long
    task_args = [(t, os.path.join(cfg_.dir_15m, f"{t}{cfg_.end_15m}"), cfg_) for t in tickers]
    all_dicts: List[dict] = []
    errors: List[str] = []
    if MAX_WORKERS <= 1:
        for k, a in enumerate(task_args, 1):
            try:
                all_dicts.extend(worker(a))
            except Exception as e:
                errors.append(f"{a[0]}:{e}")
            if k % 100 == 0:
                print(f"    [{side}] {k}/{len(tickers)}")
    else:
        done = 0
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(worker, a): a[0] for a in task_args}
            for fut in as_completed(futs):
                done += 1
                try:
                    all_dicts.extend(fut.result())
                except Exception as e:
                    errors.append(f"{futs[fut]}:{e}")
                if done % 200 == 0:
                    print(f"    [{side}] {done}/{len(tickers)}")
    if errors:
        print(f"  [{side}] {len(errors)} errors: {', '.join(errors[:5])}")
    if not all_dicts:
        return pd.DataFrame()
    out = pd.DataFrame(all_dicts)
    if getattr(cfg_, "enable_topn_per_day", True):
        out = apply_topn_per_day(out, cfg_)
    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return _sort_trades(out)


# ===========================================================================
# 5-MIN DIR RESOLVER
# ===========================================================================
def _resolve_5min_dir() -> Path:
    candidates = [
        _project_root / "stocks_indicators_1min_eq",
        _project_root / "stocks_indicators_5min_eq",
        _project_root / "data" / "stocks_indicators_1min_eq",
        _project_root.parent / "stocks_indicators_1min_eq",
    ]
    for c in candidates:
        if c.is_dir():
            return c
    return candidates[0]


# ===========================================================================
# COMPOSITE SCORE  (multi-objective)
# ===========================================================================
def _composite_score(m: dict) -> float:
    """
    Multi-objective ranking score targeting:
      - Max profit factor
      - Max Sharpe
      - Max win rate
      - Min drawdown
      - Min SL %

    Score = PF * (Sharpe + 1) * win_rate_frac / (max_dd_pct / 100 + 0.05)
    """
    pf     = float(m.get("profit_factor",    1.0))
    sharpe = float(m.get("sharpe_ratio",     0.0))
    win_r  = float(m.get("win_rate_pct",    50.0)) / 100.0
    dd     = float(m.get("max_drawdown_pct", 50.0)) / 100.0
    if pf <= 0:
        return 0.0
    return pf * max(sharpe + 1.0, 0.1) * win_r / (dd + 0.05)


# ===========================================================================
# PER-CONFIG RUNNER
# ===========================================================================
def run_sweep_config(
    sw:          V8SweepConfig,
    short_raw:   pd.DataFrame,
    long_raw:    pd.DataFrame,
    dir_5m:      Path,
    outputs_dir: Path,
    short_cfg:   StrategyConfig,
    long_cfg:    StrategyConfig,
    _vix_map:    dict,
    regime_map:  dict,
    ts:          str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Apply V8 post-processing for one sweep config.
    Returns (combined, short_df, long_df, metrics_dict).
    """
    print(f"\n{'='*60}")
    print(f"  CONFIG: {sw.name}")
    print(f"  {sw.description}")
    print(f"{'='*60}")

    # Deep-copy raw scanned trades
    sd = short_raw.copy() if not short_raw.empty else pd.DataFrame()
    ld = long_raw.copy()  if not long_raw.empty  else pd.DataFrame()

    # 1. Compute ATR-based targets + SL overrides
    if not sd.empty:
        sd = _apply_atr_targets(sd, sw)
    if not ld.empty:
        ld = _apply_atr_targets(ld, sw)

    # 2. Quality score gate (drop low-quality candidates before further processing)
    # Short QS is in [0, 1]; long QS is in [0, ~12]. Apply separate thresholds.
    s_qs_min = (
        sw.short_quality_score_min
        if sw.short_quality_score_min is not None
        else sw.quality_score_min / 12.0
    )
    if s_qs_min > 0 or sw.quality_score_min > 0:
        sd, n_qs_s = _apply_quality_filter(sd, s_qs_min)
        ld, n_qs_l = _apply_quality_filter(ld, sw.quality_score_min)
        print(f"  [QS-GATE]  Dropped {n_qs_s} short (QS<{s_qs_min:.3f}) / "
              f"{n_qs_l} long (QS<{sw.quality_score_min})")

    # 3. Min R:R gate
    if sw.min_rr > 0:
        sd, n_drop_s = _apply_min_rr_filter(sd, sw.min_rr)
        ld, n_drop_l = _apply_min_rr_filter(ld, sw.min_rr)
        print(f"  [RR-GATE]  Dropped {n_drop_s} short / {n_drop_l} long (min_rr={sw.min_rr})")

    # 4. Short anti-chase
    sd, n_exp = _apply_short_anticache(sd, sw)
    if n_exp:
        print(f"  [ANTICACHE] {n_exp} short signals expired (anti-chase)")

    # 5. Re-resolve exits with partial exit logic
    if not sd.empty:
        sd = _resolve_exits_partial(sd, dir_5m, sw, engine=short_cfg.parquet_engine)
        sd = _add_notional_pnl(sd)
        sd = _sort_trades(sd)
    if not ld.empty:
        ld = _resolve_exits_partial(ld, dir_5m, sw, engine=long_cfg.parquet_engine)
        ld = _add_notional_pnl(ld)
        ld = _sort_trades(ld)

    combined = pd.concat([sd, ld], ignore_index=True)
    combined = _add_notional_pnl(combined)
    combined = _sort_trades(combined)

    # 6. Daily trade count limits (target: 5–15 per day, long + short combined)
    combined, count_stats = _apply_daily_trade_limits(
        combined,
        max_trades_per_day=sw.max_trades_per_day,
        max_per_side_per_day=sw.max_per_side_per_day,
        min_warn=sw.min_trades_per_day_warn,
    )
    print(f"  [COUNT] avg={count_stats['avg_per_day']:.1f}/day  "
          f"capped_days={count_stats['capped_days']}  "
          f"dropped={count_stats['dropped']}  "
          f"days_below_min={count_stats['days_below_min']}")

    # 7. Day-loss guard
    combined, guard_info = _apply_day_loss_guard(combined, sw.day_loss_guard_pct)
    print(f"  [GUARD] blocked_trades={guard_info['blocked_trades']} blocked_days={guard_info['blocked_days']}")

    # Re-split after guard + count limits
    sd = _sort_trades(combined[combined["side"].astype(str).str.upper().eq("SHORT")].copy())
    ld = _sort_trades(combined[combined["side"].astype(str).str.upper().eq("LONG")].copy())

    # 8. Metrics  (keep BacktestMetrics objects for print_metrics; also convert to dict)
    def _to_dict(m) -> dict:
        try:
            return asdict(m)
        except Exception:
            return vars(m) if hasattr(m, "__dict__") else {}

    bm_short    = compute_backtest_metrics(sd)
    bm_long     = compute_backtest_metrics(ld)
    bm_combined = compute_backtest_metrics(combined)

    m_short    = _to_dict(bm_short)
    m_long     = _to_dict(bm_long)
    m_combined = _to_dict(bm_combined)

    # Normalise field names: BacktestMetrics uses total_trades / hit_rate_pct.
    # Add aliases so downstream .get("num_trades") / .get("win_rate_pct") work.
    def _add_aliases(m: dict, df_side: pd.DataFrame) -> dict:
        m["num_trades"]   = m.get("total_trades", 0)
        m["win_rate_pct"] = m.get("hit_rate_pct",  0.0)
        m["net_pnl_rs"]   = (
            float(df_side["pnl_rs"].sum())
            if not df_side.empty and "pnl_rs" in df_side.columns
            else 0.0
        )
        return m

    m_short    = _add_aliases(m_short,    sd)
    m_long     = _add_aliases(m_long,     ld)
    m_combined = _add_aliases(m_combined, combined)

    # 9. Print detailed metrics (full BacktestMetrics format)
    print_metrics("SHORT (net of slippage+comm, partial exits)",    bm_short)
    print_metrics("LONG (net of slippage+comm, partial exits)",     bm_long)
    print_metrics("COMBINED (net of slippage+comm, partial exits)", bm_combined)

    # Notional P&L summary (Rs.)
    short_rs = m_short["net_pnl_rs"]
    long_rs  = m_long["net_pnl_rs"]
    total_rs = m_combined["net_pnl_rs"]
    w        = 55
    print(f"\n{'=' * w}")
    print(f"  NOTIONAL P&L SUMMARY — {sw.name}")
    print(f"{'=' * w}")
    print(f"  SHORT  notional P&L : Rs.{short_rs:>+14,.2f}")
    print(f"  LONG   notional P&L : Rs.{long_rs:>+14,.2f}")
    print(f"  TOTAL  notional P&L : Rs.{total_rs:>+14,.2f}")
    print(f"{'=' * w}")

    score = _composite_score(m_combined)
    print(f"  COMPOSITE SCORE: {score:.4f}")

    metrics = {
        "short":       m_short,
        "long":        m_long,
        "combined":    m_combined,
        "score":       score,
        "guard":       guard_info,
        "count_stats": count_stats,
    }
    return combined, sd, ld, metrics


# ===========================================================================
# SWEEP COMPARISON TABLE
# ===========================================================================
def print_sweep_table(results: List[Tuple[V8SweepConfig, dict]]) -> None:
    ranked = sorted(results, key=lambda x: x[1]["score"], reverse=True)

    hdr = (
        f"{'Rank':<5} {'Config':<28} {'Trades':>7} {'PF':>7} {'Win%':>6} "
        f"{'Sharpe':>7} {'Calmar':>7} {'DD%':>6} {'Rs.PnL':>12} {'Score':>8}"
    )
    sep = "-" * len(hdr)
    print(f"\n{'='*len(hdr)}")
    print("  V8 SWEEP RESULTS  —  ranked by composite score (PF*Sharpe*WinRate/DD)")
    print(f"{'='*len(hdr)}")
    print(hdr)
    print(sep)

    for rank, (sw, m_all) in enumerate(ranked, 1):
        mc  = m_all["combined"]
        pf  = mc.get("profit_factor",    0.0)
        wr  = mc.get("win_rate_pct",     0.0)
        sh  = mc.get("sharpe_ratio",     0.0)
        ca  = mc.get("calmar_ratio",     0.0)
        dd  = mc.get("max_drawdown_pct", 0.0)
        n   = mc.get("num_trades",       0)
        pnl = mc.get("net_pnl_rs",       0.0)
        sc  = m_all["score"]
        print(
            f"  {rank:<4} {sw.name:<28} {n:>7} {pf:>7.3f} {wr:>5.1f}% "
            f"{sh:>7.2f} {ca:>7.2f} {dd:>5.1f}% {pnl:>+12,.0f} {sc:>8.4f}"
        )

    print(sep)
    print(f"\n  WINNER: {ranked[0][0].name}")
    print(f"  {ranked[0][0].description}")

    # Short vs Long breakdown for top-3
    print(f"\n{'='*len(hdr)}")
    print("  TOP-3 SIDE BREAKDOWN")
    print(f"{'='*len(hdr)}")
    for rank, (sw, m_all) in enumerate(ranked[:3], 1):
        ms = m_all["short"];  ml = m_all["long"]
        print(f"\n  #{rank}  {sw.name}")
        print(f"    SHORT  trades={ms.get('num_trades',0):>4}  PF={ms.get('profit_factor',0):.3f}  "
              f"Win%={ms.get('win_rate_pct',0):.1f}  Sharpe={ms.get('sharpe_ratio',0):.2f}  "
              f"DD={ms.get('max_drawdown_pct',0):.1f}%  Rs={ms.get('net_pnl_rs',0):+,.0f}")
        print(f"    LONG   trades={ml.get('num_trades',0):>4}  PF={ml.get('profit_factor',0):.3f}  "
              f"Win%={ml.get('win_rate_pct',0):.1f}  Sharpe={ml.get('sharpe_ratio',0):.2f}  "
              f"DD={ml.get('max_drawdown_pct',0):.1f}%  Rs={ml.get('net_pnl_rs',0):+,.0f}")
    print()


# ===========================================================================
# VIX LOADER
# ===========================================================================
def _load_india_vix(proj: Path) -> dict:
    vix_path = proj / "india_vix.parquet"
    if not vix_path.exists():
        return {}
    try:
        df = pd.read_parquet(vix_path)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return dict(zip(df["date"], df["india_vix"].astype(float)))
    except Exception:
        return {}


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    _outputs_dir = _project_root / SWEEP_OUTPUTS_SUBDIR
    _outputs_dir.mkdir(parents=True, exist_ok=True)

    ts       = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"v8_sweep_{ts}.txt"

    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)
        try:
            print("=" * 70)
            print("AVWAP V8 SWEEP RUNNER  —  20 configs × full ATR partial-exit sim")
            print(f"  Output dir : {_outputs_dir}")
            print(f"  Configs    : {len(SWEEP_CONFIGS)}")
            print(f"  Workers    : {MAX_WORKERS}")
            print("=" * 70)

            dir_5m = _resolve_5min_dir()
            print(f"[INFO] 5-min dir: {dir_5m}  exists={dir_5m.is_dir()}")

            _vix_map = _load_india_vix(_project_root)
            print(f"[VIX] {len(_vix_map)} daily VIX values loaded.")

            # ---- Build base StrategyConfig objects (scan phase) ----
            # SCAN PHILOSOPHY: be INCLUSIVE here — generate enough candidates.
            # V8 post-processing (quality_score gate → min_rr gate → day cap)
            # does the selective filtering, not the scan.
            #
            # Key relaxations vs V7 defaults:
            #   - Liquidity sweep: disabled (hard gate, kills ~70% of signals)
            #   - AVWAP no-trade zone: disabled (V8 handles this via quality score)
            #   - ADX slope: 0.40 (from 1.25) — still requires rising ADX
            #   - ADX min: 20 (from 25) — catch more trending stocks early
            #   - Volume ratio: 1.0 (from 1.2) — V8 quality score penalises low-vol
            #   - AVWAP distance: 0.10 ATR (from 0.25) — less aggressive
            #   - AVWAP consec closes: 1 (from 2)
            #   - RSI short max: 60 (from 55); RSI long min: 40 (from 45)
            #   - Stoch max short: 80 (from 75); Stoch min long: 20 (from 25)
            #   - Impulse ATR min: 0.30 (from 0.45)
            #   - Close near extreme max: 0.35 (from 0.25)
            #   - Risk guardrails: disabled at scan level (V8 guard handles it)
            #   - max_trades_per_ticker_per_day: 3 (from 1) — allow more candidates
            #   - topn_per_day: 100 (effectively unlimited at scan; V8 caps at 15)

            SCAN_RELAXATIONS = dict(
                # Trigger relaxations
                mod_impulse_min_atr=0.30,
                mod_impulse_max_atr=1.20,
                close_near_extreme_max=0.35,
                # Trend indicator relaxations
                adx_min=20.0,
                adx_slope_min=0.40,
                rsi_max_short=60.0,
                rsi_min_long=40.0,
                stochk_max=80.0,
                stochk_min=20.0,
                # Volume / ATR relaxations
                volume_min_ratio=1.0,
                atr_pct_min=0.0015,
                # AVWAP structure relaxations
                avwap_dist_atr_mult=0.10,
                avwap_min_consec_closes=1,
                # Disable hard-gate filters (V8 replaces with quality_score gate)
                enable_liquidity_sweep_filter=False,
                enable_avwap_no_trade_zone=False,
                enable_risk_guardrails=False,
                # Allow more candidates per ticker/day
                max_trades_per_ticker_per_day=3,
                enable_topn_per_day=False,
                topn_per_day=999,
                min_bars_left_after_entry=0,
            )

            short_cfg = default_short_config(reports_dir=_outputs_dir, **SCAN_RELAXATIONS)
            long_cfg  = default_long_config(reports_dir=_outputs_dir,  **SCAN_RELAXATIONS)

            short_cfg.lag_bars_short_a_mod_break_c1_low        = SHORT_LAG_BARS_A_MOD
            short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = SHORT_LAG_BARS_A_PULLBACK
            short_cfg.lag_bars_short_b_huge_failed_bounce        = SHORT_LAG_BARS_B_HUGE
            short_cfg.enable_setup_b_huge_failed_bounce          = PACK2_ENABLE_B_HUGE_SHORT
            short_cfg.max_vix_for_entries                        = float(PACK2_SHORT_MAX_VIX)
            long_cfg.lag_bars_long_a_mod_break_c1_high           = LONG_LAG_BARS_A_MOD
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high   = LONG_LAG_BARS_A_PULLBACK
            long_cfg.lag_bars_long_b_huge_pullback_hold_break     = LONG_LAG_BARS_B_HUGE
            long_cfg.enable_setup_a_pullback_c2_break             = True
            long_cfg.enable_setup_a_close_continuation_break      = True
            long_cfg.enable_setup_b_huge_c1_close_reclaim_break   = True

            # Signal windows: widest possible at scan phase (each sweep config narrows it)
            short_cfg.use_time_windows = True
            long_cfg.use_time_windows  = True
            short_cfg.signal_windows   = [(dtime(9, 15), dtime(14, 30))]
            long_cfg.signal_windows    = [(dtime(9, 15), dtime(14, 30))]

            # Market regime — REQUIRED in V8
            regime_map, regime_src = build_market_regime_map(short_cfg)
            for cfg_ in [short_cfg, long_cfg]:
                cfg_.enable_market_regime_filter = bool(regime_map)
                cfg_.market_regime_map = regime_map
            if regime_map:
                print(f"[REGIME] Required filter enabled via {regime_src}: {len(regime_map)} bars")
            else:
                print("[REGIME] WARNING: no index parquet found — regime filter inactive")

            # ---- PHASE 1: Scan all tickers ONCE (shared across all configs) ----
            print("\n[PHASE 1] Scanning 15-min entry signals (once, shared across all configs)...")
            t0 = time.time()
            short_raw = _run_side_parallel("SHORT", short_cfg)
            long_raw  = _run_side_parallel("LONG",  long_cfg)
            print(f"[PHASE 1] Done in {time.time()-t0:.1f}s  |  "
                  f"SHORT={len(short_raw)}  LONG={len(long_raw)}")

            if short_raw.empty and long_raw.empty:
                print("[DONE] No trades found after scanning.")
                return

            # ---- PHASE 2: Sweep configs ----
            print(f"\n[PHASE 2] Running {len(SWEEP_CONFIGS)} sweep configs with V8 post-processing...")
            all_results: List[Tuple[V8SweepConfig, dict]] = []
            best_combined = pd.DataFrame()
            best_score    = -1.0
            best_sw       = None

            for sw in SWEEP_CONFIGS:
                # Per-config VIX setup (if enabled in this config)
                if sw.vix_scale_enabled and _vix_map:
                    for cfg_ in [short_cfg, long_cfg]:
                        cfg_.vix_scale_enabled = True
                        cfg_.vix_daily         = _vix_map
                        cfg_.vix_baseline      = sw.vix_baseline
                        cfg_.vix_scale_min     = sw.vix_scale_min
                        cfg_.vix_scale_max     = sw.vix_scale_max
                        cfg_.vix_scale_target  = sw.vix_scale_target
                        cfg_.vix_scale_sl      = sw.vix_scale_sl
                else:
                    for cfg_ in [short_cfg, long_cfg]:
                        cfg_.vix_scale_enabled = False

                # Per-config signal window
                short_cfg.signal_windows = [(sw.signal_start, sw.signal_end)]
                long_cfg.signal_windows  = [(sw.signal_start, sw.signal_end)]

                # Per-config fixed SL/TGT for scan (ATR targets are applied post-scan)
                short_cfg.stop_pct   = sw.short_sl_pct
                long_cfg.stop_pct    = sw.long_sl_pct
                short_cfg.target_pct = sw.short_target_pct if not sw.use_atr_targets else 0.009
                long_cfg.target_pct  = sw.long_target_pct  if not sw.use_atr_targets else 0.011

                combined, sd, ld, metrics = run_sweep_config(
                    sw=sw, short_raw=short_raw, long_raw=long_raw,
                    dir_5m=dir_5m, outputs_dir=_outputs_dir,
                    short_cfg=short_cfg, long_cfg=long_cfg,
                    _vix_map=_vix_map, regime_map=regime_map, ts=ts,
                )
                all_results.append((sw, metrics))

                score = metrics["score"]
                if score > best_score:
                    best_score    = score
                    best_combined = combined
                    best_sw       = sw

                # Save per-config CSV
                if not combined.empty:
                    out_csv = _outputs_dir / f"{sw.name}_{ts}.csv"
                    combined.to_csv(out_csv, index=False)

            # ---- PHASE 3: Print sweep comparison table ----
            print_sweep_table(all_results)

            # ---- PHASE 4: Charts for best config ----
            if not best_combined.empty and best_sw is not None:
                print(f"\n[CHARTS] Generating charts for best config: {best_sw.name}")
                best_sd = best_combined[best_combined["side"].astype(str).str.upper().eq("SHORT")].copy()
                best_ld = best_combined[best_combined["side"].astype(str).str.upper().eq("LONG")].copy()
                chart_dir = _outputs_dir / "charts" / best_sw.name
                try:
                    generate_backtest_charts(best_combined, best_sd, best_ld, save_dir=chart_dir, ts_label=ts)
                    print(f"[CHARTS] Saved to {chart_dir}/")
                except Exception as e:
                    print(f"[CHARTS] Failed: {e}")

            # ---- Save master sweep summary CSV ----
            summary_rows = []
            for sw, m_all in all_results:
                mc = m_all["combined"]
                ms = m_all["short"]
                ml = m_all["long"]
                summary_rows.append({
                    "config":           sw.name,
                    "description":      sw.description,
                    "score":            round(m_all["score"], 4),
                    "trades":           mc.get("num_trades", 0),
                    "profit_factor":    round(mc.get("profit_factor", 0), 3),
                    "win_rate_pct":     round(mc.get("win_rate_pct", 0), 1),
                    "sharpe":           round(mc.get("sharpe_ratio", 0), 3),
                    "calmar":           round(mc.get("calmar_ratio", 0), 3),
                    "max_dd_pct":       round(mc.get("max_drawdown_pct", 0), 2),
                    "net_pnl_rs":       round(mc.get("net_pnl_rs", 0), 0),
                    "short_pf":         round(ms.get("profit_factor", 0), 3),
                    "long_pf":          round(ml.get("profit_factor", 0), 3),
                    "short_win_pct":    round(ms.get("win_rate_pct", 0), 1),
                    "long_win_pct":     round(ml.get("win_rate_pct", 0), 1),
                    "short_sl_pct":     sw.short_sl_pct,
                    "long_sl_pct":      sw.long_sl_pct,
                    "use_atr_targets":  sw.use_atr_targets,
                    "t1_mult":          sw.t1_atr_mult,
                    "t2_mult":          sw.t2_atr_mult,
                    "t3_mult":          sw.t3_atr_mult,
                    "partial_exits":    sw.use_partial_exits,
                    "min_rr":           sw.min_rr,
                    "quality_score_min": sw.quality_score_min,
                    "day_guard_pct":    sw.day_loss_guard_pct,
                    "window":           f"{sw.signal_start.strftime('%H:%M')}-{sw.signal_end.strftime('%H:%M')}",
                    "vix_scale":        sw.vix_scale_enabled,
                    "blocked_trades":   m_all["guard"]["blocked_trades"],
                    "avg_trades_per_day": m_all.get("count_stats", {}).get("avg_per_day", 0),
                    "days_below_min5":    m_all.get("count_stats", {}).get("days_below_min", 0),
                })

            summary_df = pd.DataFrame(summary_rows).sort_values("score", ascending=False)
            summary_csv = _outputs_dir / f"v8_sweep_summary_{ts}.csv"
            summary_df.to_csv(summary_csv, index=False)
            print(f"\n[FILE] Sweep summary saved: {summary_csv}")
            print(f"[FILE] Log saved          : {log_path}")
            print("[DONE]")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr

    print(f"[LOG] {log_path}")


if __name__ == "__main__":
    main()
