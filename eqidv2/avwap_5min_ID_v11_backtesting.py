"""
AVWAP ID 5-min v11 live-strategy backtester.

v11 keeps the v8 backtesting output format, but the candidate source and
filters mirror the current v7 live scanner. Each historical slot is scanned,
ranked, passed through the live gate and research filters, then resolved with
the next available 1-minute open after the 5-minute signal time and
setup-specific 1-minute exits.

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v11_5min

Default historical 5-minute source:
  C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import time as dtime
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

# Backtest default: both LONG and SHORT setups allowed.
# The live scanner defaults SHORT_FOCUS=1 (SHORT-only) via env var, but the
# backtest should replay all sides unless the caller explicitly overrides.
os.environ.setdefault("EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS", "0")

import eqidv2_signal_discovery_v7_5min_id_persistent as live_discovery
import eqidv2_v11_live_overlay as v11_overlay
import avwap_5min_ID_v7_candidate_scan as candidate_scan
import avwap_5min_ID_v5_backtesting as v5
import avwap_5min_ID_v6_backtesting as v6
import eqidv2_setup_conf_loader as setup_conf_loader
import eqidv2_pre_momentum as shared_pre_momentum
from eqidv2_runtime_manifest import freeze_runtime_manifest
from eqidv2_v7_position_sizing import (
    RiskSizingConfig,
    nifty_regime_short_multiplier,
    risk_based_quantity,
)
import v11_exit_policy_resolver as er


OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_5min")
LIVE_CANDIDATE_JSON_DIR = Path(r"C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json")
LIVE_PAPER_DIR = Path(r"C:\TradingData\eqidv2\live_signals")
HISTORICAL_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_CANDIDATE_5M_DIR = HISTORICAL_5M_DIR
EXCLUDED_SETUPS = candidate_scan.EXCLUDED_SETUPS
FINAL_SETUP_CONF_MODULE_ENV = setup_conf_loader.LEGACY_V11_FINAL_SETUP_CONF_MODULE_ENV
SHARED_FINAL_SETUP_CONF_MODULE_ENV = setup_conf_loader.FINAL_SETUP_CONF_MODULE_ENV

# --- 09:15 opening-snapshot policy -------------------------------------------
# 5m hybrid convention: 09:15 is the session-open SNAPSHOT row (no prior in-day
# bar), 09:20..15:30 are completed end-stamped 5m candles. Default keeps the
# 09:15 row so the backtest stays in parity with live. --exclude_opening_snapshot
# turns on strict research mode: skip the 09:15 slot for signal scanning AND drop
# it from the market-context regime. (Indicator anchoring still uses the stored
# bars; a fully strict indicator rebuild would need a separate data build.)
_OPENING_SNAPSHOT_HHMM = (9, 15)
_EXCLUDE_OPENING_SNAPSHOT = False


def _drop_opening_snapshot_rows(df: pd.DataFrame) -> pd.DataFrame:
    if not _EXCLUDE_OPENING_SNAPSHOT or df is None or df.empty or "date" not in df.columns:
        return df
    ts = pd.to_datetime(df["date"], errors="coerce")
    try:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    except (TypeError, AttributeError):
        pass
    keep = ~((ts.dt.hour == _OPENING_SNAPSHOT_HHMM[0]) & (ts.dt.minute == _OPENING_SNAPSHOT_HHMM[1]))
    return df.loc[keep].reset_index(drop=True)

# Mirrors the current v7 live chain:
#   signal discovery -> 1-minute entry engine -> PAPER_TRADE_TRUE executor.
V7_ENTRY_SEARCH_MAX_DELAY_MIN = 3
V7_SIGNAL_MARGIN_RS = 20_000.0
V7_INTRADAY_LEVERAGE = 5.0
V7_SIGNAL_NOTIONAL_RS = V7_SIGNAL_MARGIN_RS * V7_INTRADAY_LEVERAGE
V7_PAPER_SLIPPAGE_PCT = 0.0005
# V7 PAPER_TRUE parity: risk-based sizing in the entry engine and statutory
# costs in the executor. Set only by main(), so imports do not mutate live state.
_V11_COST_MODEL = "statutory"         # "flat_bps" (legacy) | "statutory" (paper parity)
_V11_SLIPPAGE_BPS = 0.0               # adverse per-leg slippage (bps), statutory mode only
RISK_SIZING_ENABLED = str(os.getenv("EQIDV2_RISK_SIZING_ENABLED", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
RISK_PCT_PER_TRADE = float(os.getenv("EQIDV2_RISK_PCT_PER_TRADE", "0.25"))
RISK_EQUITY_RS = float(os.getenv("EQIDV2_RISK_EQUITY_RS", "200000.0"))
RISK_MAX_NOTIONAL_RS = float(os.getenv("EQIDV2_RISK_MAX_NOTIONAL_RS", "150000.0"))
RISK_MIN_NOTIONAL_RS = float(os.getenv("EQIDV2_RISK_MIN_NOTIONAL_RS", "50000.0"))
NIFTY_REGIME_GATE_ENABLED = str(os.getenv("EQIDV2_NIFTY_REGIME_GATE_ENABLED", "1")).strip().lower() not in {
    "0", "false", "no", "off"
}
NIFTY_5MIN_PARQUET = os.getenv(
    "EQIDV2_NIFTY_5MIN_PARQUET",
    r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live\NIFTY_stocks_indicators_5min.parquet",
)
NIFTY_MA_DAYS = int(os.getenv("EQIDV2_NIFTY_MA_DAYS", "20"))
NIFTY_REGIME_SHORT_SIZE_MULT = float(os.getenv("EQIDV2_NIFTY_REGIME_SHORT_SIZE_MULT", "0.5"))
V7_LIVE_RAW_1M_DIR = Path(os.getenv(
    "EQIDV2_V7_LIVE_RAW_1M_DIR",
    r"C:\TradingData\eqidv2\stocks_raw_1min_entry_v5_id_live",
))
V7_LIVE_INDICATORS_5M_DIR = Path(os.getenv(
    "EQIDV2_V7_LIVE_INDICATORS_5M_DIR",
    r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live",
))
V7_HIST_INDICATORS_5M_DIR = Path(os.getenv(
    "EQIDV2_V7_HIST_INDICATORS_5M_DIR",
    r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2",
))
V7_SLOT_RAW_1M_DIR = Path(os.getenv(
    "EQIDV2_V7_SLOT_RAW_1M_DIR",
    r"C:\TradingData\eqidv2\entry_engine_1min_v5_ID\slot_raw_1min",
))
_V11_EXACT_LIVE_PARITY = False

AB_PROBATION_SETUPS = tuple(
    setup for setup in sorted(v6.SETUP_EXIT_RULES)
    if str(setup).startswith(("A_", "B_"))
)
AB_GOOD_RESULT_SETUPS = (
    "A_MOD_BREAK_C1_LOW",
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
)
AB_FILTERED_RELAXED_SETUPS = (
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
)
AB_MAX_PNL_LOW_VALID_SETUPS = (
    "A_MOD_BREAK_C1_HIGH",
    "A_MOD_BREAK_C1_LOW",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_AVWAP_RECLAIM_REVERSAL",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
)

# ---------------------------------------------------------------------------
# Gate-blocked setups — honest out-of-sample verdict (2026-06-11).
# v11_setup_rescue_search.py walk-forward fit a single indicator/non-indicator
# threshold on TRAIN ONLY for each of these setups and scored every candidate
# OUT-OF-SAMPLE, net-of-cost (nse_intraday_costs), with a day-clustered bootstrap.
# None could be rescued: every cut was an in-sample mirage (best OOS net PF < 1.20
# and/or day-block p > 0.10), and the full-sample day-clustered bootstrap shows
# each is reliably NEGATIVE net-of-cost (p(net>0) ~ 0.87-0.98). Over Jan-Jun 2026,
# blocking these six vs the full book moved the costed result:
#   net Rs -77,588 -> +39,307 ; net PF 0.87 -> 1.39 ; day-win 40% -> 58% ;
#   max DD Rs -126,309 -> -9,311 ; net-positive in every one of the 6 months.
# Removed at the selected-strategy chokepoint, so it applies to every profile
# (except `none`, the raw research passthrough) and to the Tier123 add-on.
# Reversible: drop a name to restore it. Do NOT hand-tune one back in without a
# fresh OOS rescue (v11_setup_rescue_search.py) that clears the gate.
# ---------------------------------------------------------------------------
# DISABLED 2026-06-11 per user request: all setups unblocked (empty set below).
# The six that FAILED the OOS rescue and were previously blocked here were:
#   E_VWAP_LOSE_EARLY_SHORT, A_MOD_BREAK_C1_LOW, E_ORB_BREAKOUT_SHORT,
#   D_EMA20_BOUNCE, L_BB_SQUEEZE_LONG, A_MOD_CLOSE_CONTINUATION_BREAK.
# Re-add those names to restore the costed clean book (net +39,307, PF 1.39,
# max DD -9.3k, net-positive every month) documented above.
GATE_BLOCKED_SETUPS: frozenset[str] = frozenset()

AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT = 10.7
AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT = 0.84
MAX_PNL_SBB_MIN_MARKET_RET_PCT = 0.54
MAX_PNL_SBB_MIN_NOTIONAL_RS = 100_000.0
MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR = 0.60
MAX_PNL_A_MOD_CLOSE_MIN_SIGNAL_RANGE_PCT = 2.2
MAX_PNL_A_MOD_CLOSE_MAX_NOTIONAL_RS = 100_000.0
# A_MOD_BREAK_C1_HIGH gate — replaced 2026-06-09. Old: market_abs<=0.26 AND vol_ratio<=2.0
# rejected genuine winners (SUVEN, BANKINDIA, CARYSIL, MAYURUNIQ).
# New: rs_pct>=2.0 AND atr_pct<=0.006 AND signal_minute<=670 (11:10 IST).
# Validated across 10 sessions: 32 trades, PF 3.25. Mirrors eqidv2_v11_live_overlay.py.
A_MOD_C1_HIGH_MIN_RS_PCT     = 2.0
A_MOD_C1_HIGH_MAX_ATR_PCT    = 0.006
A_MOD_C1_HIGH_MAX_SIGNAL_MIN = 670.0
MAX_PNL_A_MOD_C1_LOW_MIN_RS_ABS_PCT = 9.2
MAX_PNL_A_MOD_C1_LOW_MIN_VOL_RATIO = 1.80
MAX_PNL_D_EMA20_REJECTION_MIN_BODY_PCT = 0.89
MAX_PNL_D_EMA20_REJECTION_MIN_RANKER_SCORE = 0.39
MAX_PNL_E_VWAP_BAND_MIN_ATR_PCT = 0.0059
MAX_PNL_E_VWAP_BAND_MAX_SIGNAL_MINUTE = 690
MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR = -1.25
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_SIGNAL_MINUTE = 780.0
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_SIGNAL_MINUTE = 825.0
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_BODY_PCT = 0.93
RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_WICK_SKEW_PCT = -0.065
RESIDUAL_OVERLAY_SBB_MAX_SIGNAL_MINUTE = 705
TIER123_BALANCED_PROFILE = "production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced"
TIER123_BALANCED_SETUPS = (
    "T_TREND_DAY_EMA_STAIR_SHORT",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT",
)
TIER123_BALANCED_EXIT_RULES = {
    "T_TREND_DAY_EMA_STAIR_SHORT": (0.70, 1.00),
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": (0.70, 0.80),
    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": (0.70, 0.80),
}
TIER123_T_STAIR_SHORT_MAX_MARKET_RET_PCT = -0.39
TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE = 780.0
TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE = 840.0
TIER123_MR_FADE_LONG_MIN_VOL_RATIO = 2.47
TIER123_MR_FADE_SHORT_MAX_VOL_RATIO = 1.70
TIER123_MR_FADE_SHORT_MIN_QUALITY_SCORE = 60.7
TIER123_MAX_RAW_PER_SETUP = 2500
v6.SETUP_EXIT_RULES.update(TIER123_BALANCED_EXIT_RULES)
AB_SELECTED_STRATEGY_PROFILES = {
    "production_core_ab_probe",
    "ab_only_probe",
    "production_core_ab_good_probe",
    "ab_good_only_probe",
    "production_core_ab_filtered_relaxed",
    "ab_filtered_relaxed_only",
    "production_core_ab_max_pnl_low_valid",
    "production_core_ab_max_pnl_low_valid_residual_overlay",
    TIER123_BALANCED_PROFILE,
    "final_setup_conf",  # conf book contains A/B probation setups -> admit via quality_top_slot
}
AB_GATE_PROFILE_CHOICES = ("off", "quality_top_slot")

# Opt-in profile: run ONLY the approved final_setup_conf.py book (the 9 passed setups),
# each with its own tuned exit / pre-momentum gate / mask / entry guard. Non-breaking —
# default behaviour is unchanged; activated only by --selected_strategy_profile final_setup_conf.
FINAL_CONF_PROFILE = "final_setup_conf"
# Conf setups whose candidates do NOT survive (or are not produced by) the standard v11 scan/gate:
#   - RAW_PRE_GATE_POOL  : generated by candidate_scan but the v8 gate drops 100% (L_DOUBLE/L_PRESSURE)
#   - TIER123_OVERLAY_PROBE / NEW_SETUPS_SCAN : NOT emitted by candidate_scan at all (T / S_UPTHRUST);
#                          their candidates are merged in from external scan CSVs (see _FINAL_CONF_EXT_*)
# All of these are re-admitted from `ranked` past v8+research in conf mode (the conf mask + premom +
# exit then handle them). Populated by _activate_final_setup_conf(); empty otherwise.
_FINAL_CONF_READMIT_SETUPS: frozenset[str] = frozenset()
_FINAL_CONF_ACTIVE = False
_FINAL_CONF_SETUP_KEYS: frozenset[str] = frozenset()
_FINAL_CONF_EXIT_POLICIES: dict[str, dict] = {}
_FINAL_CONF_ENTRY_POLICIES: dict[str, dict] = {}
# External scan-source candidates (T_TREND_DAY_EMA_STAIR_SHORT, S_UPTHRUST_TRAP_FADE) loaded once in
# conf mode and merged into the per-day raw_candidates before the pipeline. Empty otherwise.
_FINAL_CONF_EXT_CANDIDATES = None
_FINAL_CONF_EXT_SRC = {
    "TIER123_OVERLAY_PROBE": os.getenv(
        "EQIDV2_FINAL_CONF_TIER123_SOURCE_CSV",
        r"C:\TradingData\eqidv2\outputs_ID_v11_conf_tier_c_current\tier123\tier123_standalone_trades.csv",
    ),
    "NEW_SETUPS_SCAN": os.getenv(
        "EQIDV2_FINAL_CONF_NEW_SETUPS_SOURCE_CSV",
        r"C:\TradingData\eqidv2\outputs_ID_v11_conf_tier_c_current\new_setups\new_setups_standalone_trades.csv",
    ),
}

SELECTED_STRATEGY_PROFILE_CHOICES = (
    "none",
    "production_core",
    "production_core_tiny",
    "production_core_ab_probe",
    "ab_only_probe",
    "production_core_ab_good_probe",
    "ab_good_only_probe",
    "production_core_ab_filtered_relaxed",
    "ab_filtered_relaxed_only",
    "production_core_ab_max_pnl_low_valid",
    "production_core_ab_max_pnl_low_valid_residual_overlay",
    TIER123_BALANCED_PROFILE,
    FINAL_CONF_PROFILE,
)
SELECTED_STRATEGY_DEFAULT_PROFILE = TIER123_BALANCED_PROFILE
SELECTED_STRATEGY_RULE_LABELS = {
    "C_OR_BREAKOUT": "production_core: broad C_OR_BREAKOUT kept after honest holdout test",
    "D_EMA20_BOUNCE": "production_core: vol_ratio <= 1.5975512 OR vwap_dist_atr >= -0.38557115, and signal_minute <= 705",
    "E_ORB_BREAKOUT_LONG": "production_core: v7_signal_notional_rs >= 99937.32",
    "E_ORB_BREAKOUT_SHORT": "production_core: market_ret_pct >= -0.63438346, quality_score >= 97.873364, upper_wick_pct <= 0.014647435; exit 0.80/1.50",
    "L_BB_SQUEEZE_LONG": "production_core: market_abs_ret_pct <= 0.74284715 OR vol_ratio <= 3.0227043, and ranker_score >= 0.7332456",
    "E_VWAP_LOSE_EARLY_SHORT": (
        "profile-specific: production_core_tiny uses regime == BEAR; "
        f"production_core_ab_max_pnl_low_valid uses vwap_dist_atr >= {MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR:.7f}"
    ),
}
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        setup: (
            "ab_probe: explicit A/B probation gate; admitted by top-slot quality only, "
            "requires separate train/holdout PF validation"
        )
        for setup in AB_PROBATION_SETUPS
    }
)
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        setup: (
            "ab_good_probe: selected from 2026-06-01 A/B diagnostic as a positive-PnL "
            "A/B watchlist setup; still probation, not production"
        )
        for setup in AB_GOOD_RESULT_SETUPS
    }
)
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        "B_HUGE_C1_CLOSE_RECLAIM_BREAK": (
            "ab_filtered_relaxed: B_HUGE_C1_CLOSE_RECLAIM_BREAK with "
            f"rs_pct <= {AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT:.4f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_PULLBACK_C2_THEN_BREAK_C2_LOW": (
            "ab_filtered_relaxed: A_PULLBACK_C2_THEN_BREAK_C2_LOW with "
            f"market_abs_ret_pct <= {AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT:.4f}; "
            "A/B quality_top_slot probation gate still required"
        ),
    }
)
SELECTED_STRATEGY_RULE_LABELS.update(
    {
        "S_BB_SQUEEZE_SHORT": (
            "max_pnl_low_valid: relaxed S_BB_SQUEEZE_SHORT with "
            f"market_ret_pct >= {MAX_PNL_SBB_MIN_MARKET_RET_PCT:.8f} OR "
            f"v7_signal_notional_rs >= {MAX_PNL_SBB_MIN_NOTIONAL_RS:.2f}; "
            "residual_overlay profile additionally admits residual morning S_BB with "
            f"signal_minute <= {RESIDUAL_OVERLAY_SBB_MAX_SIGNAL_MINUTE:.1f}"
        ),
        "B_AVWAP_RECLAIM_REVERSAL": (
            "max_pnl_low_valid: B_AVWAP_RECLAIM_REVERSAL with "
            f"vwap_dist_atr >= {MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR:.8f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_MOD_CLOSE_CONTINUATION_BREAK": (
            "max_pnl_low_valid: A_MOD_CLOSE_CONTINUATION_BREAK with "
            f"signal_range_pct >= {MAX_PNL_A_MOD_CLOSE_MIN_SIGNAL_RANGE_PCT:.7f} OR "
            f"v7_signal_notional_rs <= {MAX_PNL_A_MOD_CLOSE_MAX_NOTIONAL_RS:.0f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_MOD_BREAK_C1_LOW": (
            "max_pnl_low_valid micro-probe: A_MOD_BREAK_C1_LOW with "
            f"abs(rs_pct) >= {MAX_PNL_A_MOD_C1_LOW_MIN_RS_ABS_PCT:.7f} AND "
            f"vol_ratio >= {MAX_PNL_A_MOD_C1_LOW_MIN_VOL_RATIO:.7f}; "
            "A/B quality_top_slot probation gate still required"
        ),
        "A_MOD_BREAK_C1_HIGH": (
            f"max_pnl_low_valid add-on (gate updated 2026-06-09): A_MOD_BREAK_C1_HIGH with "
            f"rs_pct >= {A_MOD_C1_HIGH_MIN_RS_PCT} AND atr_pct <= {A_MOD_C1_HIGH_MAX_ATR_PCT} AND "
            f"signal_minute <= {A_MOD_C1_HIGH_MAX_SIGNAL_MIN:.0f} (11:10 IST); "
            "A/B quality_top_slot probation gate still required; "
            "entry engine additionally caps to top-2 per slot by vwap_dist_atr with 11:10 signal-time ceiling"
        ),
        "D_EMA20_REJECTION": (
            "max_pnl_low_valid add-on: D_EMA20_REJECTION with "
            f"body_pct >= {MAX_PNL_D_EMA20_REJECTION_MIN_BODY_PCT} AND "
            f"ranker_score >= {MAX_PNL_D_EMA20_REJECTION_MIN_RANKER_SCORE}; "
            "residual_overlay profile additionally admits late-session residual D_EMA20_REJECTION "
            f"when {RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_SIGNAL_MINUTE:.0f} < signal_minute <= "
            f"{RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_SIGNAL_MINUTE:.0f} and "
            f"(body_pct >= {RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_BODY_PCT} OR "
            f"wick_skew_pct <= {RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_WICK_SKEW_PCT})"
        ),
        "E_VWAP_BAND_FADE": (
            "max_pnl_low_valid: filtered E_VWAP_BAND_FADE count add with "
            f"atr_pct >= {MAX_PNL_E_VWAP_BAND_MIN_ATR_PCT} AND "
            f"signal_minute <= {MAX_PNL_E_VWAP_BAND_MAX_SIGNAL_MINUTE}"
        ),
        "T_TREND_DAY_EMA_STAIR_SHORT": (
            "tier123_balanced: late bearish trend-day EMA stair short with "
            f"market_ret_pct <= {TIER123_T_STAIR_SHORT_MAX_MARKET_RET_PCT} AND "
            f"{TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE:.0f} <= signal_minute <= "
            f"{TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE:.0f}; "
            "resolved as non-overlap add-on after the protected residual-overlay book"
        ),
        "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG": (
            "tier123_balanced: controlled VWAP extreme fade long with "
            f"vol_ratio >= {TIER123_MR_FADE_LONG_MIN_VOL_RATIO}; "
            "resolved as non-overlap add-on after the protected residual-overlay book"
        ),
        "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT": (
            "tier123_balanced: controlled VWAP extreme fade short with "
            f"vol_ratio <= {TIER123_MR_FADE_SHORT_MAX_VOL_RATIO} AND "
            f"quality_score >= {TIER123_MR_FADE_SHORT_MIN_QUALITY_SCORE}; "
            "resolved as non-overlap add-on after the protected residual-overlay book"
        ),
    }
)
SELECTED_STRATEGY_EXIT_OVERRIDES = {
    "production_core": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_tiny": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_probe": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_good_probe": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_filtered_relaxed": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_max_pnl_low_valid": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    "production_core_ab_max_pnl_low_valid_residual_overlay": {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
    TIER123_BALANCED_PROFILE: {"E_ORB_BREAKOUT_SHORT": (0.80, 1.50)},
}


# ---------------------------------------------------------------------------
# Entry-engine time gate constants — mirrors eqidv2_entry_engine_1min_v5_id.py
# ---------------------------------------------------------------------------
# E_VWAP_LOSE_EARLY_SHORT must not fire before 09:45 IST (first two 5-min
# bars lack a meaningful intraday VWAP).
ENTRY_E_VWAP_EARLY_SHORT_MIN_SLOT  = dtime(9, 45)
# A_MOD_BREAK_C1_HIGH: signal-time ceiling mirrors V11 overlay (670 min = 11:10).
ENTRY_A_MOD_C1_HIGH_MAX_SLOT       = dtime(11, 10)
# Top-N per slot for A_MOD_C1_HIGH, ranked by vwap_dist_atr desc.
ENTRY_A_MOD_C1_HIGH_TOP_N          = 2
# C_OR_BREAKOUT VWAP-separation floor / ATR cap / time window.
# VWAP separation >= 2.0 → PF 2.46; ATR >= 0.010 → PF 0.93 (bucket analysis).
# Morning window bars 7-18 (09:55-10:40): WR 71.2%, PF 1.71.
ENTRY_C_OR_BREAKOUT_MIN_VWAP_DIST_ATR = 2.0
ENTRY_C_OR_BREAKOUT_MAX_ATR_PCT       = 0.010
ENTRY_C_OR_BREAKOUT_MIN_SIGNAL_TIME   = dtime(9, 55)
ENTRY_C_OR_BREAKOUT_MAX_SIGNAL_TIME   = dtime(10, 40)
# Setups that are permanently blocked (shadow setups — never executable in live).
ENTRY_SHADOW_SETUPS: frozenset[str] = frozenset({"C_OR_BREAKDOWN"})

# Pre-entry momentum gate — mirrors PRE_ENTRY_MOMENTUM_SETUP_GATES in the
# live entry engine (v7_pre_entry_momentum_2026_06_04_t_probation).
# Each entry is a tuple of (feature, op, threshold) triples.
PRE_ENTRY_MOMENTUM_GATE_VERSION = "v7_pre_entry_momentum_2026_06_04_t_probation"
PRE_ENTRY_MOMENTUM_MISSING_ACTION = "block"  # block if features are missing
PRE_ENTRY_MOMENTUM_SETUP_GATES: dict[str, tuple[tuple[str, str, float], ...]] = {
    "B_AVWAP_RECLAIM_REVERSAL": (
        ("pre_entry_momentum_score", "<=", 64.7678),
    ),
    "C_OR_BREAKOUT": (
        ("sig5_adx_calc", ">=", 25.0),
        ("sig5_rsi_dir", ">=", 60.0),
        ("sig5_vol_ratio20", ">=", 1.5),
        ("pre2_mom_r", ">=", -0.050),
    ),
    "D_EMA20_BOUNCE": (
        ("pre3_range_r", ">=", 0.292349),
        ("pre_entry_momentum_score", "<=", 78.3448),
    ),
    "D_EMA20_REJECTION": (
        ("pre10_mom_r", "<=", 0.156614),
        ("pre5_mom_r", ">=", 0.12493),
        ("sig5_adx_calc", ">=", 20.0),
    ),
    "E_ORB_BREAKOUT_LONG": (
        ("pre15_vol_ratio20", "<=", 1.08301),
        ("pre1_adx", ">=", 42.3138),
    ),
    "E_ORB_BREAKOUT_SHORT": (
        ("pre10_dir_count", ">=", 5.0),
        ("pre5_vol_ratio20", ">=", 1.65561),
    ),
    "E_VWAP_LOSE_EARLY_SHORT": (
        ("sig5_vol_ratio20", ">=", 1.5643),
        ("pre3_body_sum_r", "<=", 0.797498),
    ),
    "G_HIGHER_HIGH_BREAK": (
        ("pre3_close_pos", "<=", 0.985417),
        ("sig5_rsi_dir", "<=", 67.878),
    ),
    "L_TREND_PULLBACK": (
        ("pre_entry_momentum_score", ">=", 73.021),
        ("pre2_mom_r", ">=", 0.233909),
    ),
    "T_TREND_DAY_EMA_STAIR_SHORT": (
        ("pre3_close_pos", ">=", 0.662492),
        ("pre5_dir_count", "<=", 3.0),
        ("pre1_adx", "<=", 31.0),
        ("pre5_range_r", ">=", 0.35),
    ),
}


def _normalise_bars_date_index(df: pd.DataFrame, *, naive_tz: str) -> pd.DataFrame | None:
    if df is None or df.empty or "date" not in df.columns:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize(naive_tz).dt.tz_convert("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date")
    if df.empty:
        return None
    return df.set_index("date")


@lru_cache(maxsize=None)
def _load_live_raw_1m_with_open(ticker: str) -> pd.DataFrame | None:
    path = V7_LIVE_RAW_1M_DIR / f"{str(ticker).upper()}_stocks_raw_1min.parquet"
    if not path.exists():
        return None
    want = ["date", "open", "high", "low", "close", "volume", "ADX", "RSI"]
    try:
        df = pd.read_parquet(path, columns=want)
    except Exception:
        try:
            df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume"])
        except Exception:
            df = pd.read_parquet(path)
    return _normalise_bars_date_index(df, naive_tz="Asia/Kolkata")


@lru_cache(maxsize=None)
def _load_1m_with_open(ticker: str) -> pd.DataFrame | None:
    path = v6.DATA_1M_DIR / f"{str(ticker).upper()}_stocks_indicators_1min.parquet"
    hist = None
    # Load OHLCV + ADX + RSI (needed for pre-entry momentum gate)
    if path.exists():
        want = ["date", "open", "high", "low", "close", "volume", "ADX", "RSI"]
        try:
            df = pd.read_parquet(path, columns=want)
        except Exception:
            df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
        hist = _normalise_bars_date_index(df, naive_tz="UTC")

    live_raw = _load_live_raw_1m_with_open(ticker)
    if hist is None or hist.empty:
        return live_raw
    if live_raw is None or live_raw.empty:
        return hist
    # For overlapping timestamps the fresh Kite raw row is authoritative.
    # This also avoids the historical store's legacy one-minute label shift.
    hist_extra = hist[~hist.index.isin(live_raw.index)]
    return pd.concat([hist_extra, live_raw], axis=0, sort=False).sort_index()


def _immutable_slot_raw_1m(
    ticker: str,
    signal_ts: pd.Timestamp,
) -> pd.DataFrame | None:
    sig = _normalise_ts(signal_ts)
    if pd.isna(sig):
        return None
    slot_key = sig.strftime("%Y%m%d_%H%M")
    path = V7_SLOT_RAW_1M_DIR / slot_key / f"{str(ticker).upper()}_raw_1min.parquet"
    if not path.exists():
        return None
    try:
        frame = pd.read_parquet(path)
    except Exception:
        return None
    return _normalise_bars_date_index(frame, naive_tz="Asia/Kolkata")


def _entry_bars_for_signal(
    ticker: str,
    signal_ts: pd.Timestamp,
) -> tuple[pd.DataFrame | None, str]:
    immutable = _immutable_slot_raw_1m(ticker, signal_ts)
    if immutable is not None and not immutable.empty:
        return immutable, "immutable_slot_raw_1min"
    if _V11_EXACT_LIVE_PARITY:
        return None, "missing_immutable_slot_raw_1min"
    return _load_1m_with_open(ticker), "historical_1min"


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        out = float(value)
        if not np.isfinite(out):
            return default
        return out
    except Exception:
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _signal_time(row: pd.Series) -> pd.Timestamp:
    for col in ("signal_time_ist", "signal_ts", "bar_time_ist"):
        if col in row.index and pd.notna(row.get(col)):
            ts = _normalise_ts(row.get(col))
            if pd.notna(ts):
                return ts
    entry = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    ts = _normalise_ts(entry)
    if pd.notna(ts):
        return ts - pd.Timedelta(minutes=5)
    return pd.NaT


def _first_1m_entry(
    bars: pd.DataFrame,
    signal_ts: pd.Timestamp,
    *,
    max_delay_minutes: int = 1,
    decision_ready_at: pd.Timestamp | None = None,
) -> tuple[pd.Timestamp, float] | None:
    sig = _normalise_ts(signal_ts)
    if pd.isna(sig):
        return None
    intended_entry = (sig + pd.Timedelta(minutes=1)).floor("min")
    not_before = intended_entry
    if decision_ready_at is not None:
        ready = _normalise_ts(decision_ready_at)
        if pd.notna(ready):
            not_before = max(not_before, ready.ceil("min"))
    latest_allowed = intended_entry + pd.Timedelta(minutes=max_delay_minutes)
    if not_before > latest_allowed:
        return None
    sub = bars[(bars.index >= not_before) & (bars.index <= latest_allowed)]
    if sub.empty:
        return None
    entry_ts = pd.Timestamp(sub.index[0])
    entry_px = float(sub.iloc[0]["open"])
    if not np.isfinite(entry_px) or entry_px <= 0:
        return None
    return entry_ts, entry_px


def _resolve_trade_1m_entry(row: pd.Series, cost_bps: float) -> dict | None:
    setup = str(row["setup"])
    if setup not in v6.SETUP_EXIT_RULES:
        raise ValueError(f"missing v11 exit rule for setup: {setup}")
    sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup]
    if sl_pct < v6.MIN_SL_PCT and not (_FINAL_CONF_ACTIVE and setup in _FINAL_CONF_SETUP_KEYS):
        raise ValueError(f"SL for {setup} is below {v6.MIN_SL_PCT:.2f}%: {sl_pct}")

    bars = _load_1m_with_open(str(row["ticker"]))
    if bars is None or bars.empty:
        return None

    sig_ts = _signal_time(row)
    entry_policy = _FINAL_CONF_ENTRY_POLICIES.get(setup, {})
    if entry_policy.get("model") == "high_break_trigger":
        entry = candidate_scan.late_bb10.resolve_entry_1m(
            bars,
            sig_ts,
            trigger=float(row.get("entry_trigger_price", np.nan)),
            cancel=float(row.get("entry_cancel_price", np.nan)),
            valid_minutes=int(entry_policy.get("valid_minutes", 3)),
            max_gap_pct=float(entry_policy.get("max_gap_pct", 0.20)),
        )
    else:
        entry = _first_1m_entry(bars, sig_ts)
    if entry is None:
        return None
    entry_ts, entry_px = entry

    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=entry_px,
        entry_time_ist=entry_ts,
        sl_pct=sl_pct,
        tgt_pct=target_pct,
        exit_policy=_FINAL_CONF_EXIT_POLICIES.get(setup),
    )
    if res is None:
        return None

    if _V11_COST_MODEL == "statutory":
        # G4: resolve P&L like the tuner + live — per-trade NSE statutory costs on the
        # V7 risk-sized live notional, with adverse per-leg slippage on entry and exit.
        import nse_intraday_costs as _nse
        _side = str(row["side"]).upper()
        if _side == "LONG":
            _stop_ref = entry_px * (1.0 - sl_pct / 100.0)
        else:
            _stop_ref = entry_px * (1.0 + sl_pct / 100.0)
        _qty = _risk_based_qty(entry_px, _stop_ref)
        if _side == "SHORT":
            _short_mult = _historical_nifty_short_mult(str(entry_ts.date()))
            if _short_mult < 1.0:
                _qty = max(1, int(_qty * _short_mult))
        _s = _V11_SLIPPAGE_BPS / 1e4
        _fill = entry_px * (1 + _s) if _side == "LONG" else entry_px * (1 - _s)
        _exit = res.exit_price * (1 - _s) if _side == "LONG" else res.exit_price * (1 + _s)
        gross = (_exit - _fill) * _qty if _side == "LONG" else (_fill - _exit) * _qty
        net = _nse.net_pnl(_fill, _exit, _qty, _side)
        cost = gross - net
    else:                                   # flat_bps (legacy default — byte-identical)
        net, gross, cost = v6._net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
    old_entry_ts = row.get("entry_time_v6", row.get("entry_time_ist", row.get("entry_ts", pd.NaT)))
    old_entry_px = row.get("entry_price_v6", row.get("entry_price", row.get("entry_px", np.nan)))
    rec = row.to_dict()
    rec.update({
        "signal_time_v8": sig_ts,
        "old_entry_time_v7": old_entry_ts,
        "old_entry_price_v7": old_entry_px,
        "entry_time_v6": entry_ts,
        "entry_price_v6": float(entry_px),
        "trade_date": str(entry_ts.date()),
        "v8_entry_model": "NEXT_1MIN_OPEN_AFTER_5MIN_SIGNAL",
        "v8_entry_delay_minutes": float((entry_ts - sig_ts).total_seconds() / 60.0) if pd.notna(sig_ts) else np.nan,
        "v6_sl_pct": sl_pct,
        "v6_target_pct": target_pct,
        "v6_outcome": res.outcome,
        "v6_exit_price": float(res.exit_price),
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(gross),
        "v6_cost_rs": float(cost),
        "v6_net_pnl_rs": float(net),
        "capital_per_trade_rs": float(
            (_fill * _qty / V7_INTRADAY_LEVERAGE)
            if _V11_COST_MODEL == "statutory"
            else v6.CAPITAL_PER_TRADE
        ),
        "leverage": V7_INTRADAY_LEVERAGE,
        "notional_exposure_rs": float(
            (_fill * _qty)
            if _V11_COST_MODEL == "statutory"
            else v6.EFFECTIVE_NOTIONAL
        ),
    })
    return rec


def _price_pnl_rs(side: str, entry_price: float, exit_price: float, quantity: int) -> float:
    if str(side).upper() == "SHORT":
        return float((entry_price - exit_price) * quantity)
    return float((exit_price - entry_price) * quantity)


def _risk_based_qty(entry_price: float, stop_price: float) -> int:
    """Use the exact sizing primitive called by the V7 live entry engine."""
    return risk_based_quantity(
        entry_price,
        stop_price,
        RiskSizingConfig(
            enabled=RISK_SIZING_ENABLED,
            fallback_notional_rs=V7_SIGNAL_NOTIONAL_RS,
            equity_rs=RISK_EQUITY_RS,
            risk_pct_per_trade=RISK_PCT_PER_TRADE,
            min_notional_rs=RISK_MIN_NOTIONAL_RS,
            max_notional_rs=RISK_MAX_NOTIONAL_RS,
        ),
    )


@lru_cache(maxsize=64)
def _historical_nifty_short_mult(trade_day: str) -> float:
    return nifty_regime_short_multiplier(
        NIFTY_5MIN_PARQUET,
        trade_day=trade_day,
        enabled=NIFTY_REGIME_GATE_ENABLED,
        ma_days=NIFTY_MA_DAYS,
        bullish_multiplier=NIFTY_REGIME_SHORT_SIZE_MULT,
    )


def _pnl_model_description(*, actual_live_quantity: bool = False) -> str:
    quantity_model = "actual live quantity" if actual_live_quantity else "V7 risk-based signal quantity"
    if _V11_COST_MODEL == "statutory":
        return f"{quantity_model}, PAPER_TRUE NSE statutory costs, net P&L"
    return f"{quantity_model}, legacy price-only P&L, no backtest costs"


def _side_exit_pcts(side: str, entry_price: float, stop_price: float, target_price: float) -> tuple[float, float]:
    side = str(side).upper()
    if entry_price <= 0 or stop_price <= 0 or target_price <= 0:
        return np.nan, np.nan
    if side == "LONG":
        return (
            float((entry_price - stop_price) / entry_price * 100.0),
            float((target_price - entry_price) / entry_price * 100.0),
        )
    return (
        float((stop_price - entry_price) / entry_price * 100.0),
        float((entry_price - target_price) / entry_price * 100.0),
    )


def _selected_strategy_numeric(frame: pd.DataFrame, col: str) -> pd.Series:
    if col in frame.columns:
        return pd.to_numeric(frame[col], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _selected_strategy_features(signals: pd.DataFrame) -> pd.DataFrame:
    work = signals.copy()
    for col in [
        "vol_ratio",
        "vwap_dist_atr",
        "v7_signal_notional_rs",
        "market_ret_pct",
        "quality_score",
        "ranker_score",
        "body_pct",
        "signal_open",
        "signal_high",
        "signal_low",
        "signal_close",
    ]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    signal_minutes: list[float] = []
    for _, row in work.iterrows():
        ts = pd.NaT
        for col in ("signal_time_ist", "signal_datetime", "signal_ts", "bar_time_ist", "scan_slot_ist"):
            if col in row.index and pd.notna(row.get(col)):
                ts = _normalise_ts(row.get(col))
                if pd.notna(ts):
                    break
        signal_minutes.append(float(ts.hour * 60 + ts.minute) if pd.notna(ts) else np.nan)
    work["signal_minute"] = signal_minutes

    open_px = _selected_strategy_numeric(work, "signal_open")
    high_px = _selected_strategy_numeric(work, "signal_high")
    low_px = _selected_strategy_numeric(work, "signal_low")
    close_px = _selected_strategy_numeric(work, "signal_close")
    close_safe = close_px.replace(0, np.nan)
    body_top = pd.concat([open_px, close_px], axis=1).max(axis=1)
    body_bottom = pd.concat([open_px, close_px], axis=1).min(axis=1)
    work["upper_wick_pct"] = (high_px - body_top) / close_safe * 100.0
    work["lower_wick_pct"] = (body_bottom - low_px) / close_safe * 100.0
    work["wick_skew_pct"] = work["upper_wick_pct"] - work["lower_wick_pct"]
    work["signal_range_pct"] = (high_px - low_px) / close_safe * 100.0
    work["market_abs_ret_pct"] = _selected_strategy_numeric(work, "market_ret_pct").abs()
    return work


def _normalise_selected_strategy_profile(profile: str | None) -> str:
    out = str(profile or SELECTED_STRATEGY_DEFAULT_PROFILE).strip().lower()
    if out not in SELECTED_STRATEGY_PROFILE_CHOICES:
        raise SystemExit(
            f"[v11 selected_strategy] unknown profile {profile!r}; "
            f"choose one of {', '.join(SELECTED_STRATEGY_PROFILE_CHOICES)}"
        )
    return out


@lru_cache(maxsize=1)
def _load_final_setup_conf_module():
    """Load the same configurable setup book used by the V7 live path."""
    target = setup_conf_loader.configured_target()
    try:
        module = setup_conf_loader.load_setup_conf_module(
            base_dir=Path(__file__).resolve().parent
        )
    except Exception as exc:
        raise SystemExit(
            "[v11 final_setup_conf] failed to load "
            f"{SHARED_FINAL_SETUP_CONF_MODULE_ENV}/"
            f"{FINAL_SETUP_CONF_MODULE_ENV}={target!r}: {exc}"
        ) from exc

    print(
        f"[v11 final_setup_conf] config module={target} file={getattr(module, '__file__', 'unknown')}",
        flush=True,
    )
    return module


def _final_setup_conf_mask(signals: pd.DataFrame) -> pd.Series:
    """Selected-strategy mask for the final_setup_conf book: keep ONLY the approved setups,
    each filtered by its own mask_terms + entry-guard min_slot. Pre-momentum gates and exits
    are applied downstream via the overridden PRE_ENTRY_MOMENTUM_SETUP_GATES / SETUP_EXIT_RULES
    (see _activate_final_setup_conf). Production masks/overlays are intentionally bypassed."""
    _fc = _load_final_setup_conf_module()
    work = _selected_strategy_features(signals)
    setup = work.get("setup", pd.Series("", index=work.index)).astype(str)
    regime = work.get("regime", pd.Series("", index=work.index)).astype(str).str.upper()
    sig_min = _selected_strategy_numeric(work, "signal_minute")
    mask = pd.Series(False, index=work.index)
    for name, cfg in _fc.FINAL_SETUP_CONF.items():
        m = setup.eq(name)
        for term in cfg.get("mask_terms", []):
            f, op, v = term[0], term[1], term[2]
            if isinstance(v, str):
                col = regime if f == "regime" else work.get(f, pd.Series("", index=work.index)).astype(str).str.upper()
                vv = v.upper()
                m = m & (col.ne(vv) if op == "!=" else col.eq(vv))
            else:
                col = _selected_strategy_numeric(work, f)
                if op == ">=":
                    m = m & (col >= v)
                elif op == "<=":
                    m = m & (col <= v)
                elif op == "!=":
                    m = m & (col != v)
                else:
                    m = m & (col == v)
        guard = cfg.get("entry_guards", {})

        def _slot_minute(value: object) -> int:
            hh, mm = str(value).split(":")
            return int(hh) * 60 + int(mm)

        if guard.get("min_slot"):
            m = m & (sig_min >= _slot_minute(guard["min_slot"]))
        if guard.get("max_slot"):
            m = m & (sig_min <= _slot_minute(guard["max_slot"]))
        for start, end in guard.get("exclude_windows", []):
            start_min = _slot_minute(start)
            end_min = _slot_minute(end)
            m = m & ~sig_min.between(start_min, end_min, inclusive="both")
        mask = mask | m.fillna(False)
    return mask.fillna(False)


def _selected_strategy_mask(signals: pd.DataFrame, profile: str) -> pd.Series:
    profile = _normalise_selected_strategy_profile(profile)
    if signals is None or signals.empty:
        return pd.Series(False, index=pd.RangeIndex(0))
    if profile == "none":
        return pd.Series(True, index=signals.index)
    if profile == FINAL_CONF_PROFILE:
        return _final_setup_conf_mask(signals)

    work = _selected_strategy_features(signals)
    setup = work.get("setup", pd.Series("", index=work.index)).astype(str)
    ab_mask = setup.isin(AB_PROBATION_SETUPS)
    ab_good_mask = setup.isin(AB_GOOD_RESULT_SETUPS)
    rs_pct = _selected_strategy_numeric(work, "rs_pct")
    market_abs = _selected_strategy_numeric(work, "market_abs_ret_pct")
    ab_filtered_relaxed_mask = (
        setup.eq("B_HUGE_C1_CLOSE_RECLAIM_BREAK")
        & (rs_pct <= AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT)
    ) | (
        setup.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW")
        & (market_abs <= AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT)
    )
    if profile == "ab_only_probe":
        return ab_mask.fillna(False)
    if profile == "ab_good_only_probe":
        return ab_good_mask.fillna(False)
    if profile == "ab_filtered_relaxed_only":
        return ab_filtered_relaxed_mask.fillna(False)

    vol_ratio = _selected_strategy_numeric(work, "vol_ratio")
    vwap_dist_atr = _selected_strategy_numeric(work, "vwap_dist_atr")
    notional = _selected_strategy_numeric(work, "v7_signal_notional_rs")
    market_ret = _selected_strategy_numeric(work, "market_ret_pct")
    quality_score = _selected_strategy_numeric(work, "quality_score")
    ranker_score = _selected_strategy_numeric(work, "ranker_score")
    body_pct = _selected_strategy_numeric(work, "body_pct")
    atr_pct = _selected_strategy_numeric(work, "atr_pct")
    signal_minute = _selected_strategy_numeric(work, "signal_minute")
    upper_wick_pct = _selected_strategy_numeric(work, "upper_wick_pct")
    wick_skew_pct = _selected_strategy_numeric(work, "wick_skew_pct")
    signal_range_pct = _selected_strategy_numeric(work, "signal_range_pct")
    regime = work.get("regime", pd.Series("", index=work.index)).astype(str).str.upper()

    max_pnl_low_valid_mask = ab_filtered_relaxed_mask.copy()
    max_pnl_low_valid_mask |= setup.eq("S_BB_SQUEEZE_SHORT") & (
        (market_ret >= MAX_PNL_SBB_MIN_MARKET_RET_PCT)
        | (notional >= MAX_PNL_SBB_MIN_NOTIONAL_RS)
    )
    max_pnl_low_valid_mask |= setup.eq("B_AVWAP_RECLAIM_REVERSAL") & (
        vwap_dist_atr >= MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR
    )
    max_pnl_low_valid_mask |= setup.eq("A_MOD_CLOSE_CONTINUATION_BREAK") & (
        (signal_range_pct >= MAX_PNL_A_MOD_CLOSE_MIN_SIGNAL_RANGE_PCT)
        | (notional <= MAX_PNL_A_MOD_CLOSE_MAX_NOTIONAL_RS)
    )
    max_pnl_low_valid_mask |= setup.eq("A_MOD_BREAK_C1_LOW") & (
        (rs_pct.abs() >= MAX_PNL_A_MOD_C1_LOW_MIN_RS_ABS_PCT)
        & (vol_ratio >= MAX_PNL_A_MOD_C1_LOW_MIN_VOL_RATIO)
    )
    max_pnl_low_valid_mask |= setup.eq("A_MOD_BREAK_C1_HIGH") & (
        (rs_pct >= A_MOD_C1_HIGH_MIN_RS_PCT)
        & (atr_pct <= A_MOD_C1_HIGH_MAX_ATR_PCT)
        & (signal_minute <= A_MOD_C1_HIGH_MAX_SIGNAL_MIN)
    )
    max_pnl_low_valid_mask |= setup.eq("D_EMA20_REJECTION") & (
        (body_pct >= MAX_PNL_D_EMA20_REJECTION_MIN_BODY_PCT)
        & (ranker_score >= MAX_PNL_D_EMA20_REJECTION_MIN_RANKER_SCORE)
    )
    max_pnl_low_valid_mask |= setup.eq("E_VWAP_BAND_FADE") & (
        (atr_pct >= MAX_PNL_E_VWAP_BAND_MIN_ATR_PCT)
        & (signal_minute <= MAX_PNL_E_VWAP_BAND_MAX_SIGNAL_MINUTE)
    )
    max_pnl_low_valid_mask |= setup.eq("E_VWAP_LOSE_EARLY_SHORT") & (
        vwap_dist_atr >= MAX_PNL_E_VWAP_LOSE_MIN_VWAP_DIST_ATR
    )
    residual_late_d_mask = setup.eq("D_EMA20_REJECTION") & (
        (signal_minute > RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_SIGNAL_MINUTE)
        & (signal_minute <= RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_SIGNAL_MINUTE)
        & (
            (body_pct >= RESIDUAL_OVERLAY_D_EMA20_REJECTION_MIN_BODY_PCT)
            | (wick_skew_pct <= RESIDUAL_OVERLAY_D_EMA20_REJECTION_MAX_WICK_SKEW_PCT)
        )
    )
    residual_morning_sbb_mask = setup.eq("S_BB_SQUEEZE_SHORT") & (
        signal_minute <= RESIDUAL_OVERLAY_SBB_MAX_SIGNAL_MINUTE
    )
    max_pnl_residual_overlay_mask = (
        max_pnl_low_valid_mask
        | residual_late_d_mask
        | residual_morning_sbb_mask
    )
    tier123_balanced_mask = (
        setup.eq("T_TREND_DAY_EMA_STAIR_SHORT")
        & (market_ret <= TIER123_T_STAIR_SHORT_MAX_MARKET_RET_PCT)
        & (signal_minute >= TIER123_T_STAIR_SHORT_MIN_SIGNAL_MINUTE)
        & (signal_minute <= TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE)
    )
    tier123_balanced_mask |= setup.eq("MR_CONTROLLED_VWAP_EXTREME_FADE_LONG") & (
        vol_ratio >= TIER123_MR_FADE_LONG_MIN_VOL_RATIO
    )
    tier123_balanced_mask |= setup.eq("MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT") & (
        (vol_ratio <= TIER123_MR_FADE_SHORT_MAX_VOL_RATIO)
        & (quality_score >= TIER123_MR_FADE_SHORT_MIN_QUALITY_SCORE)
    )

    mask = setup.eq("C_OR_BREAKOUT")
    mask |= setup.eq("D_EMA20_BOUNCE") & (
        ((vol_ratio <= 1.60) | (vwap_dist_atr >= -0.39))
        & (signal_minute <= 705)
    )
    mask |= setup.eq("E_ORB_BREAKOUT_LONG") & (notional >= 100_000.0)
    mask |= setup.eq("E_ORB_BREAKOUT_SHORT") & (
        (market_ret >= -0.63)
        & (quality_score >= 97.9)
        & (upper_wick_pct <= 0.015)
    )
    mask |= setup.eq("L_BB_SQUEEZE_LONG") & (
        ((market_abs <= 0.74) | (vol_ratio <= 3.0))
        & (ranker_score >= 0.73)
    )
    if profile == "production_core_tiny":
        mask |= setup.eq("E_VWAP_LOSE_EARLY_SHORT") & regime.eq("BEAR")
    if profile == "production_core_ab_probe":
        mask |= ab_mask
    if profile == "production_core_ab_good_probe":
        mask |= ab_good_mask
    if profile == "production_core_ab_filtered_relaxed":
        mask |= ab_filtered_relaxed_mask
    if profile == "production_core_ab_max_pnl_low_valid":
        mask |= max_pnl_low_valid_mask
    if profile == "production_core_ab_max_pnl_low_valid_residual_overlay":
        mask |= max_pnl_residual_overlay_mask
    if profile == TIER123_BALANCED_PROFILE:
        mask |= max_pnl_residual_overlay_mask
        mask |= tier123_balanced_mask

    # Honest OOS verdict: drop setups that no train-only threshold could rescue
    # out-of-sample, net-of-cost (see GATE_BLOCKED_SETUPS). Applies to every
    # profile except `none` (which returned True above as the raw passthrough).
    mask &= ~setup.isin(GATE_BLOCKED_SETUPS)
    return mask.fillna(False)


def _apply_selected_strategy_profile(
    signals: pd.DataFrame,
    profile: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    profile = _normalise_selected_strategy_profile(profile)
    if signals is None:
        signals = pd.DataFrame()
    if signals.empty:
        stats = {
            "selected_strategy_profile": profile,
            "selected_strategy_input_signals": 0,
            "selected_strategy_signals": 0,
            "selected_strategy_rejects": 0,
        }
        return signals.copy(), pd.DataFrame(), stats

    work = _selected_strategy_features(signals)
    mask = _selected_strategy_mask(work, profile)
    accepted = work.loc[mask].copy().reset_index(drop=True)
    rejected = work.loc[~mask].copy().reset_index(drop=True)
    if not accepted.empty:
        accepted["v11_selected_strategy_profile"] = profile
        accepted["v11_selected_strategy_rule"] = accepted["setup"].astype(str).map(SELECTED_STRATEGY_RULE_LABELS).fillna("")
    if not rejected.empty:
        rejected["v11_selected_strategy_profile"] = profile
        rejected["v11_selected_strategy_reject_reason"] = "not_in_selected_strategy_profile_or_rule_failed"

    stats = {
        "selected_strategy_profile": profile,
        "selected_strategy_input_signals": int(len(work)),
        "selected_strategy_signals": int(len(accepted)),
        "selected_strategy_rejects": int(len(rejected)),
    }
    return accepted, rejected, stats


def _selected_exit_override(setup: str, profile: str) -> tuple[float, float] | None:
    profile = _normalise_selected_strategy_profile(profile)
    # The final-conf profile installs its per-setup exits dynamically in
    # ``v6.SETUP_EXIT_RULES``.  Cached entry-engine rows still carry the stop and
    # target from whichever book created the cache, so replaying those row
    # values would silently test the wrong exit geometry.
    setup_name = str(setup)
    if (
        profile == FINAL_CONF_PROFILE
        and _FINAL_CONF_ACTIVE
        and setup_name in _FINAL_CONF_SETUP_KEYS
        and setup_name in v6.SETUP_EXIT_RULES
    ):
        sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup_name]
        return float(sl_pct), float(target_pct)
    return SELECTED_STRATEGY_EXIT_OVERRIDES.get(profile, {}).get(setup_name, None)


def _normalise_ab_gate_profile(profile: str | None) -> str:
    out = str(profile or "off").strip().lower()
    if out not in AB_GATE_PROFILE_CHOICES:
        raise SystemExit(
            f"[v11 ab_gate] unknown profile {profile!r}; "
            f"choose one of {', '.join(AB_GATE_PROFILE_CHOICES)}"
        )
    return out


def _ab_gate_enabled(profile: str | None) -> bool:
    return _normalise_ab_gate_profile(profile) != "off"


def _ab_gate_setups_for_selected_profile(profile: str | None) -> tuple[str, ...]:
    profile = _normalise_selected_strategy_profile(profile)
    if profile in {
        "production_core_ab_max_pnl_low_valid",
        "production_core_ab_max_pnl_low_valid_residual_overlay",
        TIER123_BALANCED_PROFILE,
    }:
        return AB_MAX_PNL_LOW_VALID_SETUPS
    if profile in {"production_core_ab_filtered_relaxed", "ab_filtered_relaxed_only"}:
        return AB_FILTERED_RELAXED_SETUPS
    if profile in {"production_core_ab_good_probe", "ab_good_only_probe"}:
        return AB_GOOD_RESULT_SETUPS
    return AB_PROBATION_SETUPS


def _ab_gate_base_stats(
    *,
    profile: str,
    min_quality: float,
    max_per_side: int,
    max_per_slot: int,
    allowed_setups: tuple[str, ...] | None = None,
) -> dict:
    profile = _normalise_ab_gate_profile(profile)
    allowed = tuple(allowed_setups or AB_PROBATION_SETUPS)
    return {
        "ab_gate_profile": profile,
        "ab_gate_enabled": bool(profile != "off"),
        "ab_gate_allowed_setups": int(len(allowed)),
        "ab_gate_min_quality": float(min_quality),
        "ab_gate_max_per_side": int(max(1, max_per_side)),
        "ab_gate_max_per_slot": int(max(1, max_per_slot)),
        "ab_gate_candidates": 0,
        "ab_gate_quality_passed": 0,
        "ab_gate_quality_rejected": 0,
        "ab_gate_accepted": 0,
    }


def _apply_ab_probation_gate(
    ranked_slot: pd.DataFrame,
    *,
    profile: str,
    min_quality: float,
    max_per_side: int,
    max_per_slot: int,
    allowed_setups: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, dict]:
    profile = _normalise_ab_gate_profile(profile)
    allowed = tuple(allowed_setups or AB_PROBATION_SETUPS)
    stats = _ab_gate_base_stats(
        profile=profile,
        min_quality=min_quality,
        max_per_side=max_per_side,
        max_per_slot=max_per_slot,
        allowed_setups=allowed,
    )
    if profile == "off" or ranked_slot is None or ranked_slot.empty:
        return pd.DataFrame() if ranked_slot is None else ranked_slot.iloc[0:0].copy(), stats

    work = _ensure_candidate_id(ranked_slot)
    setup = work.get("setup", pd.Series("", index=work.index)).astype(str)
    ab = work.loc[setup.isin(allowed)].copy()
    stats["ab_gate_candidates"] = int(len(ab))
    if ab.empty:
        return ab, stats

    ab["_ab_quality_num"] = pd.to_numeric(ab.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    ab["_ab_ranker_num"] = pd.to_numeric(ab.get("ranker_score", 0.0), errors="coerce").fillna(0.0)
    ab["side"] = ab.get("side", pd.Series("", index=ab.index)).astype(str).str.upper().str.strip()
    ab = ab.loc[ab["_ab_quality_num"] >= float(min_quality)].copy()
    stats["ab_gate_quality_passed"] = int(len(ab))
    stats["ab_gate_quality_rejected"] = int(stats["ab_gate_candidates"] - len(ab))
    if ab.empty:
        return ab.drop(columns=["_ab_quality_num", "_ab_ranker_num"], errors="ignore"), stats

    side_cap = int(max(1, max_per_side))
    slot_cap = int(max(1, max_per_slot))
    selected = (
        ab.sort_values(
            ["side", "_ab_quality_num", "_ab_ranker_num", "ticker", "setup"],
            ascending=[True, False, False, True, True],
        )
        .groupby("side", group_keys=False)
        .head(side_cap)
        .sort_values(["_ab_quality_num", "_ab_ranker_num", "ticker", "setup"], ascending=[False, False, True, True])
        .head(slot_cap)
        .drop(columns=["_ab_quality_num", "_ab_ranker_num"], errors="ignore")
        .reset_index(drop=True)
    )
    stats["ab_gate_accepted"] = int(len(selected))
    if not selected.empty:
        rule = (
            f"ab_quality_top_slot|min_quality>={float(min_quality):.6g}|"
            f"max_per_side={side_cap}|max_per_slot={slot_cap}"
        )
        selected["v8_live_gate_status"] = "AB_PROBATION_PASSED"
        selected["v8_live_gate_rule"] = rule
        selected["v8_live_gate_version"] = "v11_ab_probe_v1"
        selected["research_live_filter_status"] = "AB_PROBATION_BYPASS"
        selected["research_live_filter_reason"] = "explicit_ab_gate_profile"
        selected["research_live_filter_version"] = "v11_ab_probe_v1"
        selected["ab_gate_profile"] = profile
        selected["ab_gate_rule"] = rule
    return selected, stats


def _v7_signal_id(ticker: str, side: str, bar_time: str, setup: str) -> str:
    raw = f"{str(ticker).upper()}|{str(side).upper()}|{str(bar_time)}|{str(setup)}"
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Pre-entry momentum feature engineering (mirrors _pre_entry_momentum_features
# in eqidv2_entry_engine_1min_v5_id.py, adapted for historical parquet data).
# ---------------------------------------------------------------------------

@lru_cache(maxsize=None)
def _load_5m_ind_bars(ticker: str) -> pd.DataFrame | None:
    name = f"{str(ticker).upper()}_stocks_indicators_5min.parquet"
    paths = [
        V7_HIST_INDICATORS_5M_DIR / name,
        V7_LIVE_INDICATORS_5M_DIR / name,
    ]
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            continue
        want = ["date", "open", "high", "low", "close", "volume", "ADX", "RSI"]
        try:
            df = pd.read_parquet(path, columns=want)
        except Exception:
            try:
                df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume"])
            except Exception:
                continue
        norm = _normalise_bars_date_index(df, naive_tz="UTC")
        if norm is not None and not norm.empty:
            frames.append(norm.reset_index())
    if not frames:
        return None
    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out.dropna(subset=["date"]).sort_values("date")
    out = out.drop_duplicates(subset=["date"], keep="last")
    return out.reset_index(drop=True)


def _last_finite(series: pd.Series) -> float:
    vals = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(vals.iloc[-1]) if len(vals) else float("nan")


def _calc_rsi_last(bars: pd.DataFrame, period: int = 14) -> float:
    if "close" not in bars.columns or len(bars) < period + 1:
        return float("nan")
    close = pd.to_numeric(bars["close"], errors="coerce")
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.mask((avg_loss == 0.0) & (avg_gain > 0.0), 100.0)
    rsi = rsi.mask((avg_loss == 0.0) & (avg_gain <= 0.0), 50.0)
    return _last_finite(rsi)


def _calc_adx_last(bars: pd.DataFrame, period: int = 14) -> float:
    needed = {"high", "low", "close"}
    if not needed.issubset(set(bars.columns)) or len(bars) < period + 2:
        return float("nan")
    high = pd.to_numeric(bars["high"], errors="coerce")
    low = pd.to_numeric(bars["low"], errors="coerce")
    close = pd.to_numeric(bars["close"], errors="coerce")
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=bars.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=bars.index)
    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean().replace(0.0, np.nan)
    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean() / atr
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    adx = dx.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    return _last_finite(adx)


def _pre_entry_momentum_features_v11(
    ticker: str,
    side: str,
    entry_price: float,
    stop_price: float,
    entry_ts: pd.Timestamp,
    signal_ts: pd.Timestamp,
    candidate: pd.Series | dict | None = None,
) -> tuple[dict[str, float], str]:
    candidate_payload = dict(candidate) if candidate is not None else {}
    candidate_payload.setdefault("ticker", ticker)
    candidate_payload.setdefault("side", side)
    bars_full, source = _entry_bars_for_signal(ticker, signal_ts)
    if bars_full is None or bars_full.empty:
        return {}, source

    # Legacy historical candidates predate frozen signal ADX/RSI columns.
    # Historical research may enrich them from the completed 5m row, but exact
    # live parity never does: missing frozen inputs must fail closed.
    required_signal_fields = ("signal_adx", "signal_rsi", "signal_vol_ratio20")
    if not _V11_EXACT_LIVE_PARITY and any(
        not np.isfinite(_safe_float(candidate_payload.get(field), np.nan))
        for field in required_signal_fields
    ):
        ind5 = _load_5m_ind_bars(ticker)
        if ind5 is not None and not ind5.empty:
            sig_rows = ind5[
                (ind5["date"].dt.date == signal_ts.date())
                & (ind5["date"] <= signal_ts)
            ].tail(1)
            if len(sig_rows):
                sig = sig_rows.iloc[-1]
                candidate_payload.setdefault("signal_open", sig.get("open"))
                candidate_payload.setdefault("signal_high", sig.get("high"))
                candidate_payload.setdefault("signal_low", sig.get("low"))
                candidate_payload.setdefault("signal_close", sig.get("close"))
                candidate_payload["signal_adx"] = sig.get("ADX")
                candidate_payload["signal_rsi"] = sig.get("RSI")
                candidate_payload.setdefault("signal_vol_ratio20", candidate_payload.get("vol_ratio"))
    features, missing_reason = shared_pre_momentum.calculate_features(
        raw_1m=bars_full,
        candidate=candidate_payload,
        entry_price=entry_price,
        stop_price=stop_price,
        cutoff_ist=entry_ts,
    )
    return features, missing_reason

    # Legacy implementation retained below temporarily for reference; the
    # shared immutable implementation above is the only reachable path.
    risk = abs(entry_price - stop_price)
    if not risk or not np.isfinite(risk):
        return {}, "zero risk"
    d = 1.0 if side == "LONG" else -1.0

    bars_full = _load_1m_with_open(ticker)
    if bars_full is None or bars_full.empty:
        return {}, "missing 1m file"
    entry_date = entry_ts.date()
    cutoff = entry_ts.floor("min")
    bars = bars_full[(bars_full.index.date == entry_date) & (bars_full.index < cutoff)].copy()
    if len(bars) < 16:
        return {"pre_bars": float(len(bars))}, f"insufficient pre-entry 1m bars ({len(bars)})"

    for col in ("open", "high", "low", "close"):
        bars[col] = pd.to_numeric(bars[col], errors="coerce")
    closes = bars["close"].values.astype(float)
    opens  = bars["open"].values.astype(float)
    highs  = bars["high"].values.astype(float)
    lows   = bars["low"].values.astype(float)
    volumes = pd.to_numeric(bars.get("volume", pd.Series(dtype=float)), errors="coerce").values.astype(float)

    out: dict[str, float] = {"pre_bars": float(len(bars))}
    last_close = closes[-1]
    last_open  = opens[-1]
    last_high  = highs[-1]
    last_low   = lows[-1]
    rng = max(last_high - last_low, 1e-9)

    for n in (1, 2, 3, 5, 10, 15):
        if len(closes) > n:
            old_close = closes[-(n + 1)]
            out[f"pre{n}_mom_r"] = float(d * (last_close - old_close) / risk) if np.isfinite(old_close) else float("nan")
        else:
            out[f"pre{n}_mom_r"] = float("nan")

    # Base vol: mean of 20 bars before the last bar — mirrors live engine's
    # `prior = bars.iloc[:-1].tail(20)` in _add_window_features.
    _prior_vols = volumes[-21:-1] if len(volumes) >= 21 else volumes[:-1]
    _base_vol = float(np.nanmean(_prior_vols)) if len(_prior_vols) > 0 else float("nan")

    def _window_features(n: int) -> None:
        if len(closes) < n:
            return
        wc = closes[-n:]
        wo = opens[-n:]
        wh = highs[-n:]
        wl = lows[-n:]
        wv = volumes[-n:]
        # Use window high/low (not close min/max) — mirrors live _add_window_features.
        w_high = float(wh.max())
        w_low  = float(wl.min())
        w_rng  = max(w_high - w_low, 1e-9)
        out[f"pre{n}_close_pos"] = float((last_close - w_low) / w_rng if side == "LONG" else (w_high - last_close) / w_rng)
        out[f"pre{n}_dir_count"] = float(np.sum(d * (wc - wo) > 0))
        out[f"pre{n}_body_sum_r"] = float(d * np.sum(wc - wo) / risk)
        # Overall window range (max_high - min_low), not sum of bar heights.
        out[f"pre{n}_range_r"]    = float(w_rng / risk)
        # Numerator: mean of window volume (not just last bar); base: 20 bars before
        # last bar. Mirrors live engine's _add_window_features exactly.
        out[f"pre{n}_vol_ratio20"] = float(float(np.nanmean(wv)) / _base_vol) if _base_vol and np.isfinite(_base_vol) else float("nan")

    for n in (3, 5, 10, 15):
        _window_features(n)

    out["pre1_body_r"]    = float(d * (last_close - last_open) / risk)
    out["pre1_close_pos"] = float((last_close - last_low) / rng if side == "LONG" else (last_high - last_close) / rng)
    out["pre1_range_r"]   = float(rng / risk)
    out["pre1_dir"]       = 1.0 if d * (last_close - last_open) > 0 else 0.0

    adx_col = bars.get("ADX") if isinstance(bars, pd.DataFrame) else None
    adx = _last_finite(adx_col) if adx_col is not None else float("nan")
    if not np.isfinite(adx):
        adx = _calc_adx_last(bars)
    out["pre1_adx"] = adx
    rsi_col = bars.get("RSI") if isinstance(bars, pd.DataFrame) else None
    rsi_last = _last_finite(rsi_col) if rsi_col is not None else float("nan")
    if not np.isfinite(rsi_last):
        rsi_last = _calc_rsi_last(bars)
    out["pre1_rsi_dir"] = float(rsi_last if side == "LONG" else 100.0 - rsi_last) if np.isfinite(rsi_last) else float("nan")

    mom_vals = [out.get("pre1_body_r"), out.get("pre3_mom_r"), out.get("pre5_mom_r")]
    finite_mom = [v for v in mom_vals if v is not None and np.isfinite(v)]
    mom = float(np.mean(finite_mom)) if finite_mom else 0.0
    pos_vals = [out.get("pre3_close_pos"), out.get("pre5_close_pos")]
    finite_pos = [v for v in pos_vals if v is not None and np.isfinite(v)]
    pos = float(np.mean(finite_pos)) if finite_pos else 0.5
    vol = out.get("pre3_vol_ratio20", float("nan"))
    vol_component = min(float(vol), 3.0) / 3.0 if np.isfinite(vol) else 0.33
    out["pre_entry_momentum_score"] = float(50 + 25 * np.tanh(2 * mom) + 15 * (pos - 0.5) + 10 * (vol_component - 0.33))

    ind5 = _load_5m_ind_bars(ticker)
    if ind5 is not None and not ind5.empty:
        sig_date = signal_ts.date()
        sig_bars = ind5[(ind5["date"].dt.date == sig_date) & (ind5["date"] <= signal_ts)]
        if len(sig_bars):
            sig = sig_bars.iloc[-1]
            sig_o  = float(pd.to_numeric(sig.get("open",  float("nan")), errors="coerce"))
            sig_h  = float(pd.to_numeric(sig.get("high",  float("nan")), errors="coerce"))
            sig_l  = float(pd.to_numeric(sig.get("low",   float("nan")), errors="coerce"))
            sig_c  = float(pd.to_numeric(sig.get("close", float("nan")), errors="coerce"))
            sig_rng = max(sig_h - sig_l, 1e-9)
            out["sig5_body_r"]    = float(d * (sig_c - sig_o) / risk)
            out["sig5_range_r"]   = float(sig_rng / risk)
            out["sig5_close_pos"] = float((sig_c - sig_l) / sig_rng if side == "LONG" else (sig_h - sig_c) / sig_rng)
            adx5 = float(pd.to_numeric(sig.get("ADX", float("nan")), errors="coerce"))
            out["sig5_adx_calc"] = adx5
            rsi5 = float(pd.to_numeric(sig.get("RSI", float("nan")), errors="coerce"))
            out["sig5_rsi_dir"] = float(rsi5 if side == "LONG" else 100.0 - rsi5) if np.isfinite(rsi5) else float("nan")
            prior5 = ind5[(ind5["date"].dt.date == sig_date) & (ind5["date"] < sig["date"])].tail(20)
            vol_base5 = pd.to_numeric(prior5["volume"], errors="coerce").mean() if len(prior5) else float("nan")
            sig_vol5 = float(pd.to_numeric(sig.get("volume", float("nan")), errors="coerce"))
            out["sig5_vol_ratio20"] = float(sig_vol5 / vol_base5) if vol_base5 and np.isfinite(vol_base5) else float("nan")
    return out, ""


def _eval_pre_momentum_terms(
    features: dict[str, float],
    terms: tuple[tuple[str, str, float], ...],
) -> tuple[bool, str]:
    failed: list[str] = []
    for feature, op, threshold in terms:
        value = features.get(feature, float("nan"))
        if not isinstance(value, (int, float)) or not np.isfinite(value):
            failed.append(f"{feature}=nan {op} {threshold:.6g}")
            continue
        ok = value >= threshold if op == ">=" else value <= threshold
        if not ok:
            failed.append(f"{feature}={value:.4f} {op} {threshold:.6g}")
    return len(failed) == 0, "; ".join(failed)


def _v7_entry_engine_candidate_time(row: pd.Series) -> pd.Timestamp:
    return _candidate_time(row, "signal_time_ist", "signal_ts", "bar_time_ist", "scan_slot_ist")


def _v7_entry_engine_raw_rows(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build rows the v7 1-minute entry engine would hand to the signal CSV writer.

    Mirrors eqidv2_entry_engine_1min_v5_id.py (V7 live) without fetching from Kite:
    uses historical 1-minute OHLC, next available 1-minute open after the 5-minute
    signal, v6.SETUP_EXIT_RULES, and v7's Rs 20k margin / 5x sizing.

    Entry-engine guards applied (parity with live engine, 2026-06-10):
    - C_OR_BREAKDOWN: permanently blocked (shadow setup)
    - E_VWAP_LOSE_EARLY_SHORT: blocked when signal time < 09:45 IST
    - A_MOD_BREAK_C1_HIGH: blocked when signal time >= 11:10 IST; top-2 per
      (day, slot) ranked by vwap_dist_atr desc
    - C_OR_BREAKOUT: requires vwap_dist_atr >= 2.0, atr_pct <= 0.010,
      signal time in 09:55–10:40 IST window
    - Pre-entry momentum gate (v7_pre_entry_momentum_2026_06_04_t_probation):
      applied per-setup after entry price is known; missing features → block
    """
    if candidates is None or candidates.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows: list[dict] = []
    rejects: list[dict] = []
    work = _ensure_candidate_id(candidates)
    work = _candidate_sort_frame(work)
    work["_v11_entry_signal_ts"] = [_v7_entry_engine_candidate_time(row) for _, row in work.iterrows()]
    work["_v11_entry_day"] = [
        ts.strftime("%Y-%m-%d") if pd.notna(ts) else ""
        for ts in work["_v11_entry_signal_ts"]
    ]
    work["_v11_entry_slot"] = [
        _fmt_ist(ts) if pd.notna(ts) else ""
        for ts in work["_v11_entry_signal_ts"]
    ]
    if "quality_score" in work.columns:
        score_source = work["quality_score"]
    elif "live_rank_score" in work.columns:
        score_source = work["live_rank_score"]
    else:
        score_source = pd.Series(0.0, index=work.index)
    work["_v11_score"] = pd.to_numeric(score_source, errors="coerce").fillna(0.0)
    work["_v11_vwap_dist_atr"] = pd.to_numeric(work.get("vwap_dist_atr", 0.0), errors="coerce").fillna(0.0)
    work = work.sort_values(["_v11_entry_day", "_v11_entry_signal_ts", "_v11_input_order"]).reset_index(drop=True)

    # Pre-pass: A_MOD_BREAK_C1_HIGH top-N per (day, slot) by vwap_dist_atr desc.
    # Mark rows that exceed the per-slot cap so they can be rejected with a clear reason.
    c1high_blocked: set[int] = set()
    c1high_mask = work["setup"].eq("A_MOD_BREAK_C1_HIGH")
    if c1high_mask.any():
        for (day_key, slot_key), grp in work[c1high_mask].groupby(
            ["_v11_entry_day", "_v11_entry_slot"], sort=True, dropna=False
        ):
            if len(grp) > ENTRY_A_MOD_C1_HIGH_TOP_N:
                sorted_idx = grp.sort_values("_v11_vwap_dist_atr", ascending=False).index
                for idx in sorted_idx[ENTRY_A_MOD_C1_HIGH_TOP_N:]:
                    c1high_blocked.add(idx)

    for idx, row in work.iterrows():
        ticker = str(row.get("ticker", "")).upper().strip()
        side = str(row.get("side", "")).upper().strip()
        setup = str(row.get("setup", "")).strip()
        signal_ts = row.get("_v11_entry_signal_ts", pd.NaT)
        reject_base = {
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "signal_time_ist": _fmt_ist(signal_ts) if pd.notna(signal_ts) else "",
            "candidate_id": str(row.get("candidate_id", "")),
        }
        if not ticker or side not in {"LONG", "SHORT"} or pd.isna(signal_ts):
            rejects.append({**reject_base, "reject_reason": "invalid_required_fields"})
            continue
        if setup not in v6.SETUP_EXIT_RULES:
            rejects.append({**reject_base, "reject_reason": "missing_v8_setup_exit_rule"})
            continue

        # --- Entry-engine time / setup guards ---
        signal_time = signal_ts.time() if pd.notna(signal_ts) else None

        if setup in ENTRY_SHADOW_SETUPS:
            rejects.append({**reject_base, "reject_reason": "entry_engine_shadow_setup"})
            continue

        if setup == "E_VWAP_LOSE_EARLY_SHORT" and signal_time is not None and signal_time < ENTRY_E_VWAP_EARLY_SHORT_MIN_SLOT:
            rejects.append({**reject_base, "reject_reason": f"entry_engine_e_vwap_early_short_too_early_{signal_time}"})
            continue

        if setup == "A_MOD_BREAK_C1_HIGH":
            if signal_time is not None and signal_time >= ENTRY_A_MOD_C1_HIGH_MAX_SLOT:
                rejects.append({**reject_base, "reject_reason": f"entry_engine_a_mod_c1_high_too_late_{signal_time}"})
                continue
            if idx in c1high_blocked:
                rejects.append({**reject_base, "reject_reason": f"entry_engine_a_mod_c1_high_top{ENTRY_A_MOD_C1_HIGH_TOP_N}_slot_cap"})
                continue

        if setup == "C_OR_BREAKOUT":
            vwap_dist = float(pd.to_numeric(row.get("vwap_dist_atr", float("nan")), errors="coerce"))
            atr_pct_val = float(pd.to_numeric(row.get("atr_pct", float("nan")), errors="coerce"))
            if np.isfinite(vwap_dist) and abs(vwap_dist) < ENTRY_C_OR_BREAKOUT_MIN_VWAP_DIST_ATR:
                rejects.append({**reject_base, "reject_reason": f"entry_engine_c_or_breakout_low_vwap_dist_atr_{vwap_dist:.3f}"})
                continue
            if np.isfinite(atr_pct_val) and atr_pct_val > ENTRY_C_OR_BREAKOUT_MAX_ATR_PCT:
                rejects.append({**reject_base, "reject_reason": f"entry_engine_c_or_breakout_high_atr_pct_{atr_pct_val:.5f}"})
                continue
            if signal_time is not None and not (ENTRY_C_OR_BREAKOUT_MIN_SIGNAL_TIME <= signal_time <= ENTRY_C_OR_BREAKOUT_MAX_SIGNAL_TIME):
                rejects.append({**reject_base, "reject_reason": f"entry_engine_c_or_breakout_out_of_window_{signal_time}"})
                continue

        # --- Causal 1-min entry bar lookup ---
        immutable_bars = _immutable_slot_raw_1m(ticker, signal_ts)
        if _V11_EXACT_LIVE_PARITY and (
            immutable_bars is None or immutable_bars.empty
        ):
            rejects.append({
                **reject_base,
                "reject_reason": "missing_immutable_slot_raw_1min",
                "entry_1m_source": "missing_immutable_slot_raw_1min",
            })
            continue
        bars = _load_1m_with_open(ticker)
        entry_1m_source = (
            "kite_live_raw_1min_cumulative"
            if _V11_EXACT_LIVE_PARITY
            else "historical_1min"
        )
        if bars is None or bars.empty:
            rejects.append({
                **reject_base,
                "reject_reason": "missing_1min_file",
                "entry_1m_source": entry_1m_source,
            })
            continue
        decision_ready_at = _normalise_ts(row.get("decision_ready_at_ist"))
        if _V11_EXACT_LIVE_PARITY and pd.isna(decision_ready_at):
            rejects.append({
                **reject_base,
                "reject_reason": "missing_decision_ready_at",
                "decision_ready_source": str(row.get("decision_ready_source", "")),
            })
            continue
        entry_policy = _FINAL_CONF_ENTRY_POLICIES.get(setup, {})
        if entry_policy.get("model") == "high_break_trigger":
            entry = candidate_scan.late_bb10.resolve_entry_1m(
                bars,
                signal_ts,
                trigger=float(row.get("entry_trigger_price", np.nan)),
                cancel=float(row.get("entry_cancel_price", np.nan)),
                valid_minutes=int(entry_policy.get("valid_minutes", 3)),
                max_gap_pct=float(entry_policy.get("max_gap_pct", 0.20)),
            )
        else:
            entry = _first_1m_entry(
                bars,
                signal_ts,
                max_delay_minutes=V7_ENTRY_SEARCH_MAX_DELAY_MIN,
                decision_ready_at=(
                    decision_ready_at if pd.notna(decision_ready_at) else signal_ts
                ),
            )
        if entry is None:
            rejects.append({
                **reject_base,
                "reject_reason": "late_unexecutable_or_missing_1min_entry_bar",
                "decision_ready_at_ist": (
                    _fmt_ist(decision_ready_at)
                    if pd.notna(decision_ready_at)
                    else ""
                ),
            })
            continue

        entry_ts, signal_entry_price_raw = entry
        signal_entry_price = round(float(signal_entry_price_raw), 2)
        if signal_entry_price <= 0:
            rejects.append({**reject_base, "reject_reason": "invalid_entry_price"})
            continue

        sl_pct, target_pct = v6.SETUP_EXIT_RULES[setup]
        if side == "LONG":
            stop_price = signal_entry_price * (1.0 - sl_pct / 100.0)
            target_price = signal_entry_price * (1.0 + target_pct / 100.0)
        else:
            stop_price = signal_entry_price * (1.0 + sl_pct / 100.0)
            target_price = signal_entry_price * (1.0 - target_pct / 100.0)
        stop_price = round(float(stop_price), 2)
        target_price = round(float(target_price), 2)

        # --- Pre-entry momentum gate ---
        momentum_terms = PRE_ENTRY_MOMENTUM_SETUP_GATES.get(setup)
        if momentum_terms:
            features, missing_reason = _pre_entry_momentum_features_v11(
                ticker,
                side,
                signal_entry_price,
                stop_price,
                (signal_ts + pd.Timedelta(minutes=1)).floor("min"),
                signal_ts,
                candidate=row,
            )
            if missing_reason:
                rejects.append({**reject_base, "reject_reason": f"pre_entry_momentum_missing_{missing_reason[:40]}"})
                continue
            gate_ok, gate_fail = _eval_pre_momentum_terms(features, momentum_terms)
            if not gate_ok:
                rejects.append({**reject_base, "reject_reason": f"pre_entry_momentum_fail: {gate_fail[:80]}"})
                continue

        quantity = _risk_based_qty(signal_entry_price, stop_price)
        nifty_short_mult = 1.0
        if side == "SHORT":
            nifty_short_mult = _historical_nifty_short_mult(str(entry_ts.date()))
            if nifty_short_mult < 1.0:
                quantity = max(1, int(quantity * nifty_short_mult))
        bar_time = _fmt_ist(signal_ts)
        score = float(row.get("_v11_score", 0.0))
        out = row.drop(labels=[c for c in row.index if str(c).startswith("_v11_")], errors="ignore").to_dict()
        out.update({
            "signal_id": _v7_signal_id(ticker, side, bar_time, setup),
            "signal_datetime": bar_time,
            "signal_time_ist": bar_time,
            "bar_time_ist": bar_time,
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "score": score,
            "quality_score": score,
            "v7_signal_entry_time_ist": _fmt_ist(entry_ts),
            "v7_signal_entry_price": signal_entry_price,
            "v7_signal_stop_price": stop_price,
            "v7_signal_target_price": target_price,
            "v7_signal_sl_pct": float(sl_pct),
            "v7_signal_target_pct": float(target_pct),
            "quantity": int(quantity),
            "signal_entry_datetime_ist": bar_time,
            "signal_bar_time_ist": bar_time,
            "v7_entry_engine_model": (
                "high_break_trigger_next_3x1min"
                if entry_policy.get("model") == "high_break_trigger"
                else "next_1min_open_after_5min_signal"
            ),
            "v7_entry_1m_source": entry_1m_source,
            "pre_momentum_feature_version": shared_pre_momentum.FEATURE_VERSION,
            "decision_ready_at_ist": (
                _fmt_ist(decision_ready_at)
                if pd.notna(decision_ready_at)
                else _fmt_ist(signal_ts)
            ),
            "decision_ready_source": str(
                row.get(
                    "decision_ready_source",
                    "synthetic_signal_close" if not _V11_EXACT_LIVE_PARITY else "",
                )
            ),
            "causal_entry_not_before_ist": _fmt_ist(entry_ts),
            "v7_signal_notional_rs": float(signal_entry_price * quantity),
            "v7_risk_sizing_enabled": bool(RISK_SIZING_ENABLED),
            "v7_risk_budget_rs": float(RISK_EQUITY_RS * RISK_PCT_PER_TRADE / 100.0),
            "v7_nifty_short_size_mult": float(nifty_short_mult),
        })
        rows.append(out)

    return pd.DataFrame(rows), pd.DataFrame(rejects)


def _select_v7_entry_engine_signals(raw_entries: pd.DataFrame) -> pd.DataFrame:
    if raw_entries is None or raw_entries.empty:
        return pd.DataFrame()

    df = raw_entries.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["bar_time_ist"] = df["bar_time_ist"].astype(str)
    df["_score_num"] = pd.to_numeric(df.get("score", df.get("quality_score", 0.0)), errors="coerce").fillna(0.0)
    df["_signal_ts"] = pd.to_datetime(df["bar_time_ist"], errors="coerce")
    if getattr(df["_signal_ts"].dt, "tz", None) is None:
        df["_signal_ts"] = df["_signal_ts"].dt.tz_localize("Asia/Kolkata")
    else:
        df["_signal_ts"] = df["_signal_ts"].dt.tz_convert("Asia/Kolkata")
    df["_day"] = df["_signal_ts"].dt.strftime("%Y-%m-%d")
    df["_slot"] = df["_signal_ts"].map(_fmt_ist)

    selected_frames: list[pd.DataFrame] = []
    for _, slot_df in df.sort_values(["_day", "_signal_ts"]).groupby(["_day", "_slot"], sort=True, dropna=False):
        selected = (
            slot_df.sort_values(["_score_num", "setup"], ascending=[False, True])
            .drop_duplicates(subset=["bar_time_ist", "ticker"], keep="first")
            .copy()
        )
        selected_frames.append(selected)
    if not selected_frames:
        return pd.DataFrame()

    selected_all = pd.concat(selected_frames, ignore_index=True)
    keep: list[dict] = []
    for _, day_df in selected_all.sort_values(["_day", "_signal_ts"]).groupby("_day", sort=True, dropna=False):
        used_tickers: set[str] = set()
        for _, slot_df in day_df.groupby("_slot", sort=True, dropna=False):
            batch: set[str] = set()
            slot_sorted = slot_df.sort_values(["_score_num", "ticker"], ascending=[False, True])
            for _, row in slot_sorted.iterrows():
                ticker = str(row.get("ticker", "")).upper().strip()
                if not ticker or ticker in used_tickers or ticker in batch:
                    continue
                keep.append(row.to_dict())
                used_tickers.add(ticker)
                batch.add(ticker)

    out = pd.DataFrame(keep)
    return out.drop(columns=["_score_num", "_signal_ts", "_day", "_slot"], errors="ignore").reset_index(drop=True)


def _build_v7_entry_engine_signals(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_entries, rejects = _v7_entry_engine_raw_rows(candidates)
    selected = _select_v7_entry_engine_signals(raw_entries)
    return selected, raw_entries, rejects


def _build_v7_entry_engine_signals_for_profile(
    candidates: pd.DataFrame,
    selected_strategy_profile: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Build entry-engine rows in the same order v7 live uses for conf mode.

    In final-conf live mode, the conf mask is applied before executable-entry
    selection/daily ticker de-dupe. Applying it after selection can let an early
    rejected candidate consume the ticker and hide a later valid live entry.
    """
    profile = _normalise_selected_strategy_profile(selected_strategy_profile)
    raw_entries, rejects = _v7_entry_engine_raw_rows(candidates)
    if profile == FINAL_CONF_PROFILE:
        preselect, profile_rejects, profile_stats = _apply_selected_strategy_profile(raw_entries, profile)
        selected = _select_v7_entry_engine_signals(preselect)
        if not selected.empty:
            selected["v11_selected_strategy_profile"] = profile
            selected["v11_selected_strategy_rule"] = selected["setup"].astype(str).map(SELECTED_STRATEGY_RULE_LABELS).fillna("")
        stats = {
            **profile_stats,
            "selected_strategy_preselect_signals": int(len(preselect)),
            "selected_strategy_signals": int(len(selected)),
        }
        return selected, raw_entries, rejects, selected, profile_rejects, stats

    entry_engine_signals = _select_v7_entry_engine_signals(raw_entries)
    selected, profile_rejects, profile_stats = _apply_selected_strategy_profile(entry_engine_signals, profile)
    return selected, raw_entries, rejects, entry_engine_signals, profile_rejects, profile_stats


def _resolve_v7_entry_engine_signal(
    row: pd.Series,
    *,
    label: str,
    entry_fill_model: str,
    selected_strategy_profile: str = "none",
) -> dict | None:
    ticker = str(row.get("ticker", "")).upper().strip()
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", "")).strip()
    bars = _load_1m_with_open(ticker)
    if bars is None or bars.empty:
        return None

    signal_ts = _normalise_ts(row.get("signal_time_ist", row.get("signal_datetime", pd.NaT)))
    entry_ts = _normalise_ts(row.get("v7_signal_entry_time_ist", pd.NaT))
    signal_entry = _safe_float(row.get("v7_signal_entry_price"), 0.0)
    stop_price = _safe_float(row.get("v7_signal_stop_price"), 0.0)
    target_price = _safe_float(row.get("v7_signal_target_price"), 0.0)
    quantity = _safe_int(row.get("quantity"), 0)
    if pd.isna(entry_ts) or side not in {"LONG", "SHORT"} or signal_entry <= 0 or stop_price <= 0 or target_price <= 0 or quantity <= 0:
        return None

    fill_model = str(entry_fill_model or "ltp_on_signal_1m_open").strip().lower()
    if fill_model == "signal_bar":
        entry_price = signal_entry
        entry_source_used = "signal_bar"
    else:
        if side == "LONG":
            entry_price = round(signal_entry * (1.0 + V7_PAPER_SLIPPAGE_PCT), 2)
        else:
            entry_price = round(signal_entry * (1.0 - V7_PAPER_SLIPPAGE_PCT), 2)
        stop_mult = stop_price / signal_entry
        target_mult = target_price / signal_entry
        stop_price = round(entry_price * stop_mult, 2)
        target_price = round(entry_price * target_mult, 2)
        entry_source_used = "ltp_on_signal_1m_open"

    exit_override = _selected_exit_override(setup, selected_strategy_profile)
    exit_override_applied = exit_override is not None
    if exit_override_applied:
        sl_pct, target_pct = exit_override
        if side == "LONG":
            stop_price = round(entry_price * (1.0 - (sl_pct / 100.0)), 2)
            target_price = round(entry_price * (1.0 + (target_pct / 100.0)), 2)
        else:
            stop_price = round(entry_price * (1.0 + (sl_pct / 100.0)), 2)
            target_price = round(entry_price * (1.0 - (target_pct / 100.0)), 2)
        exit_rule_source = "selected_strategy_exit_override"
    else:
        sl_pct, target_pct = _side_exit_pcts(side, entry_price, stop_price, target_price)
        exit_rule_source = "v7_signal_stop_target"
    if not np.isfinite(sl_pct) or not np.isfinite(target_pct) or sl_pct <= 0 or target_pct <= 0:
        return None

    res = er.resolve(
        bars=bars,
        side=side,
        entry_price=entry_price,
        entry_time_ist=entry_ts,
        sl_pct=sl_pct,
        tgt_pct=target_pct,
        exit_policy=_FINAL_CONF_EXIT_POLICIES.get(setup),
    )
    if res is None:
        return None

    gross_pnl_rs = _price_pnl_rs(side, entry_price, float(res.exit_price), quantity)
    if _V11_COST_MODEL == "statutory":
        import nse_intraday_costs as _nse

        cost_breakdown = _nse.intraday_equity_costs(
            entry_price,
            float(res.exit_price),
            quantity,
            side,
        )
        cost_rs = float(cost_breakdown.total_cost)
        net_pnl_rs = float(cost_breakdown.net_pnl)
        cost_rates_as_of = str(_nse.CostConfig().rates_as_of)
    else:
        cost_rs = 0.0
        net_pnl_rs = float(gross_pnl_rs)
        cost_rates_as_of = ""
    rec = row.to_dict()
    rec.update({
        "signal_time_v8": signal_ts,
        "old_entry_time_v7": entry_ts,
        "old_entry_price_v7": signal_entry,
        "entry_time_v6": entry_ts,
        "entry_price_v6": float(entry_price),
        "trade_date": str(entry_ts.date()),
        "v8_entry_model": f"V7_ENTRY_ENGINE_NEXT_1MIN_OPEN__PAPER_{entry_source_used.upper()}",
        "v8_entry_delay_minutes": float((entry_ts - signal_ts).total_seconds() / 60.0) if pd.notna(signal_ts) else np.nan,
        "v6_sl_pct": float(sl_pct),
        "v6_target_pct": float(target_pct),
        "v6_outcome": res.outcome,
        "v6_exit_price": float(res.exit_price),
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(gross_pnl_rs),
        "v6_cost_rs": float(cost_rs),
        "v6_net_pnl_rs": float(net_pnl_rs),
        "v6_net_pnl_pct": float(
            net_pnl_rs / (entry_price * quantity) * 100.0
            if entry_price > 0 and quantity > 0
            else np.nan
        ),
        "capital_per_trade_rs": float(entry_price * quantity / V7_INTRADAY_LEVERAGE),
        "leverage": V7_INTRADAY_LEVERAGE,
        "notional_exposure_rs": float(entry_price * quantity),
        "v7_resolution_source": label,
        "v8_resolution_source": label,
        "v7_executor_entry_price_source": entry_source_used,
        "v7_live_stop_price": float(stop_price),
        "v7_live_target_price": float(target_price),
        "v11_selected_strategy_profile": _normalise_selected_strategy_profile(selected_strategy_profile),
        "v11_exit_override_applied": bool(exit_override_applied),
        "v11_exit_rule_source": exit_rule_source,
        "v11_cost_model": _V11_COST_MODEL,
        "v11_cost_rates_as_of": cost_rates_as_of,
    })
    return rec


def _resolve_v7_entry_engine_signals(
    signals: pd.DataFrame,
    *,
    label: str,
    entry_fill_model: str,
    selected_strategy_profile: str = "none",
) -> pd.DataFrame:
    if signals is None or signals.empty:
        raise SystemExit(f"[v11 {label}] no v7 entry-engine signals to resolve")
    selected_strategy_profile = _normalise_selected_strategy_profile(selected_strategy_profile)
    print(
        f"[v11 {label}] resolving {len(signals):,} v7 entry-engine signals with 1-minute exits "
        f"profile={selected_strategy_profile}"
    )
    rows: list[dict] = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(signals.iterrows(), 1):
        rec = _resolve_v7_entry_engine_signal(
            row,
            label=label,
            entry_fill_model=entry_fill_model,
            selected_strategy_profile=selected_strategy_profile,
        )
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 250 == 0 or i == len(signals):
            print(f"  [v11 {label} {i:5d}/{len(signals)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")
    if not rows:
        raise SystemExit(f"[v11 {label}] no entry-engine signals resolved")
    out = pd.DataFrame(rows)
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["_entry_sort"] = pd.to_datetime(out["entry_time_v6"], errors="coerce")
    return (
        out.sort_values(["trade_date", "_entry_sort", "ticker"], ascending=[True, True, True])
        .drop(columns=["_entry_sort"], errors="ignore")
        .reset_index(drop=True)
    )


def _resolve_trades(trades: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = v6._normalise_trades(trades)
    normalised = normalised.loc[~normalised["setup"].astype(str).isin(EXCLUDED_SETUPS)].copy()
    missing = sorted(set(normalised["setup"].astype(str)) - set(v6.SETUP_EXIT_RULES))
    if missing:
        print(f"[v11] skipping {label} setups without setup-specific exits: {missing}")
        normalised = normalised.loc[normalised["setup"].astype(str).isin(v6.SETUP_EXIT_RULES)].copy()

    print(f"[v11] resolving {len(normalised):,} {label} trades with next-1min entry + 1-min exits")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 500 == 0 or i == len(normalised):
            print(f"  [v11 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")

    if not rows:
        raise SystemExit(f"[v11] no {label} trades resolved")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    return out


def _load_live_candidate_snapshots(json_dir: Path, day: str = "") -> pd.DataFrame:
    day_key = "".join(str(day or "").split("-"))
    raw_patterns = []
    fallback_patterns = []
    if day_key:
        raw_patterns.append(f"raw_candidate_tickers_{day_key}_*.json")
        fallback_patterns.append(f"candidate_tickers_{day_key}_*.json")
    else:
        raw_patterns.append("raw_candidate_tickers_*.json")
        fallback_patterns.append("candidate_tickers_*.json")

    raw_files = [path for pattern in raw_patterns for path in sorted(json_dir.glob(pattern))]
    fallback_files = [path for pattern in fallback_patterns for path in sorted(json_dir.glob(pattern))]
    files = raw_files or fallback_files

    slot_metadata: dict[str, dict[str, Any]] = {}
    for path in fallback_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            slot_ts = _normalise_ts(payload.get("slot_ist"))
            if pd.isna(slot_ts):
                continue
            key = slot_ts.floor("min").strftime("%Y%m%d_%H%M")
            slot_metadata[key] = {
                "decision_ready_at_ist": str(
                    payload.get("decision_ready_at_ist")
                    or payload.get("created_at_ist")
                    or ""
                ),
                "candidate_snapshot_path": str(path),
                "candidate_snapshot_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "decision_ready_source": "final_candidate_snapshot",
            }
        except Exception:
            continue
    marker_pattern = f"slot_complete_{day_key}_*.json" if day_key else "slot_complete_*.json"
    for path in sorted(json_dir.glob(marker_pattern)):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not payload.get("complete"):
                continue
            slot_ts = _normalise_ts(payload.get("slot_ist"))
            if pd.isna(slot_ts):
                continue
            key = slot_ts.floor("min").strftime("%Y%m%d_%H%M")
            slot_metadata[key] = {
                "decision_ready_at_ist": str(payload.get("decision_ready_at_ist", "")),
                "candidate_snapshot_path": str(payload.get("candidate_json_path", "")),
                "candidate_snapshot_sha256": str(payload.get("candidate_json_sha256", "")),
                "scanner_runtime_manifest_path": str(payload.get("runtime_manifest_path", "")),
                "decision_ready_source": "atomic_slot_complete_marker",
            }
        except Exception:
            continue

    rows: list[dict] = []
    seen_files: set[Path] = set()
    for path in files:
        if path in seen_files:
            continue
        seen_files.add(path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in payload.get("candidates", []) or []:
            if isinstance(row, dict):
                item = dict(row)
                sig = _normalise_ts(item.get("signal_time_ist", payload.get("slot_ist")))
                key = sig.floor("min").strftime("%Y%m%d_%H%M") if pd.notna(sig) else ""
                item.update(slot_metadata.get(key, {}))
                if not item.get("decision_ready_at_ist"):
                    item["decision_ready_at_ist"] = str(
                        payload.get("decision_ready_at_ist")
                        or payload.get("created_at_ist")
                        or item.get("created_at_ist", "")
                    )
                    item["decision_ready_source"] = "raw_snapshot_legacy_fallback"
                rows.append(item)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if day:
        sig = pd.to_datetime(df.get("signal_time_ist"), errors="coerce")
        df = df.loc[sig.dt.strftime("%Y-%m-%d").eq(str(day))].copy()
    return df.reset_index(drop=True)


def _parse_hhmm(text: str) -> int:
    try:
        hh_text, mm_text = str(text).strip().split(":", 1)
        hh = int(hh_text)
        mm = int(mm_text)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"[v11 historical_full_day] invalid time {text!r}; expected HH:MM") from exc
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise SystemExit(f"[v11 historical_full_day] invalid time {text!r}; expected HH:MM")
    return hh * 60 + mm


def _slot_range_for_day(day: str, start_time: str, end_time: str) -> list[pd.Timestamp]:
    if not str(day or "").strip():
        raise SystemExit("[v11 historical_full_day] --historical_date is required")
    day_date = pd.to_datetime(day, errors="coerce")
    if pd.isna(day_date):
        raise SystemExit(f"[v11 historical_full_day] invalid --historical_date={day!r}")
    start_min = _parse_hhmm(start_time)
    end_min = _parse_hhmm(end_time)
    if _EXCLUDE_OPENING_SNAPSHOT and start_min == _OPENING_SNAPSHOT_HHMM[0] * 60 + _OPENING_SNAPSHOT_HHMM[1]:
        start_min += 5  # strict mode: skip the 09:15 opening-snapshot slot
    if end_min < start_min:
        raise SystemExit("[v11 historical_full_day] --end_time must be >= --start_time")

    base = pd.Timestamp(
        year=int(day_date.year),
        month=int(day_date.month),
        day=int(day_date.day),
        tz="Asia/Kolkata",
    )
    return [base + pd.Timedelta(minutes=minute) for minute in range(start_min, end_min + 1, 5)]


def _fmt_ist(ts: pd.Timestamp) -> str:
    t = _normalise_ts(ts)
    if pd.isna(t):
        return ""
    offset = t.strftime("%z")
    return f"{t.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _write_candidate_snapshot(
    df: pd.DataFrame,
    slot: pd.Timestamp,
    path: Path,
    *,
    output_label: str,
    payload_extra: dict | None = None,
) -> None:
    rows = [] if df is None or df.empty else df.to_dict("records")
    side_counts = {"LONG": 0, "SHORT": 0}
    setup_counts: dict[str, int] = {}
    for row in rows:
        side = str(row.get("side", "")).upper()
        setup = str(row.get("setup", ""))
        side_counts[side] = side_counts.get(side, 0) + 1
        setup_counts[setup] = setup_counts.get(setup, 0) + 1

    payload = {
        "session": "V11 exact v7 live-strategy historical regeneration",
        "output_label": output_label,
        "slot_ist": _fmt_ist(slot),
        "created_at_ist": _fmt_ist(pd.Timestamp.now(tz="Asia/Kolkata")),
        "total_candidates": int(len(rows)),
        "long_candidates": int(side_counts.get("LONG", 0)),
        "short_candidates": int(side_counts.get("SHORT", 0)),
        "setup_counts": setup_counts,
        "candidates": rows,
        **(payload_extra or {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


_V11_WORKER_MARKET_CTX: dict | None = None


def _set_candidate_5m_dir(data_root: str | Path) -> Path:
    root = Path(data_root)
    candidate_scan.LIVE_5M_DIR = root
    candidate_scan.v2.DATA_ROOT_5M = root
    try:
        candidate_scan._MARKET_CTX_CACHE = {}
    except Exception:
        pass
    return root


def _v11_worker_init(data_root: str = "") -> None:
    global _V11_WORKER_MARKET_CTX
    if data_root:
        _set_candidate_5m_dir(data_root)
    if _V11_WORKER_MARKET_CTX is None:
        candidate_scan.v2._init_worker({
            "ENABLE_NOISY_ADVANCED_SHORTS": True,
            "ENABLE_NATIVE_V2_MINED_FILTER": False,
        })
        try:
            _V11_WORKER_MARKET_CTX = candidate_scan.v2._load_market_context()
        except Exception:
            _V11_WORKER_MARKET_CTX = {}


def _normalise_date_series(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize("Asia/Kolkata")
    return ts.dt.tz_convert("Asia/Kolkata")


def _scan_one_ticker_day_candidates(payload: tuple) -> list[dict]:
    ticker, day, start_min, end_min = payload[:4]
    include_ab_excluded = bool(payload[4]) if len(payload) >= 5 else False
    global _V11_WORKER_MARKET_CTX
    if _V11_WORKER_MARKET_CTX is None:
        _v11_worker_init()
    market_ctx = _V11_WORKER_MARKET_CTX or {}

    try:
        df = candidate_scan._load_live_5m(ticker)
        if df is None or df.empty:
            return []
        prepared = candidate_scan.v2._prepare_5m(df)
    except Exception:
        return []

    day_date = pd.to_datetime(day, errors="coerce")
    if pd.isna(day_date):
        return []
    if "date_only" not in prepared.columns:
        prepared["date_only"] = _normalise_date_series(prepared["date"]).dt.date

    day_df = prepared[prepared["date_only"] == day_date.date()].copy().reset_index(drop=True)
    if day_df.empty:
        return []
    try:
        day_df["date"] = _normalise_date_series(day_df["date"])
    except Exception:
        return []
    day_df = day_df.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if day_df.empty:
        return []

    minutes = day_df["date"].dt.hour * 60 + day_df["date"].dt.minute
    slot_df = day_df.loc[(minutes >= int(start_min)) & (minutes <= int(end_min))].copy()
    if slot_df.empty:
        return []
    slot_times = sorted({pd.Timestamp(ts).floor("min") for ts in slot_df["date"]})
    slot_set = set(slot_times)
    signal_map = {
        pd.Timestamp(row["date"]).floor("min"): row.to_dict()
        for _, row in slot_df.iterrows()
    }

    scan_df = candidate_scan._append_synthetic_successor(day_df, slot_times[-1])
    candidates: list = []
    try:
        candidates.extend(candidate_scan.v2._scan_day(scan_df, str(ticker).upper(), market_ctx) or [])
    except Exception:
        pass
    if candidate_scan.EARLY_MODE_ENABLE:
        for slot in slot_times:
            try:
                candidates.extend(candidate_scan._scan_early_slot_candidates(scan_df, str(ticker).upper(), slot, market_ctx) or [])
            except Exception:
                pass

    by_slot: dict[pd.Timestamp, list[tuple]] = {}
    for c in candidates:
        try:
            c_ts = _normalise_ts(c.signal_ts).floor("min")
        except Exception:
            continue
        if c_ts not in slot_set:
            continue
        setup = str(c.setup)
        if setup in candidate_scan.EXCLUDED_SETUPS:
            if not (include_ab_excluded and setup in AB_PROBATION_SETUPS):
                continue
        if candidate_scan.FILTER_TO_V8_EXIT_SETUPS and setup not in candidate_scan.ALLOWED_SETUPS:
            continue
        signal_row = signal_map.get(c_ts)
        if signal_row is None:
            continue
        by_slot.setdefault(c_ts, []).append((c, signal_row))

    if candidate_scan.late_bb10.SETUP in candidate_scan.ALLOWED_SETUPS:
        for slot in slot_times:
            custom = candidate_scan._scan_late_bb10_signal(df, str(ticker).upper(), slot)
            if custom is not None:
                by_slot.setdefault(slot, []).append(custom)

    frames: list[pd.DataFrame] = []
    for slot, rows in sorted(by_slot.items()):
        frame = candidate_scan.candidates_to_dataframe(rows, slot)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return []
    return pd.concat(frames, ignore_index=True).to_dict("records")


def _scan_historical_full_day_candidates(
    *,
    day: str,
    start_time: str,
    end_time: str,
    workers: int,
    snapshot_dir: Path,
    data_root: Path,
    include_ab_excluded: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    data_root = _set_candidate_5m_dir(data_root)
    slots = _slot_range_for_day(day, start_time, end_time)
    try:
        universe = candidate_scan.v2._load_universe()
    except Exception as exc:
        raise SystemExit(f"[v11 historical_full_day] universe load failed: {type(exc).__name__}: {exc}") from exc

    tickers = sorted({str(t).strip().upper() for t in universe if str(t).strip()})
    if not tickers:
        raise SystemExit("[v11 historical_full_day] universe is empty")

    worker_count = max(1, int(workers))
    print(
        f"[v11 historical_full_day] scanning {len(slots)} slots from {start_time} to {end_time} "
        f"for {day} across {len(tickers)} tickers with day-wide ticker workers={worker_count} "
        f"data_root={data_root} include_ab_excluded={bool(include_ab_excluded)}",
        flush=True,
    )
    t0 = time.time()
    start_min = _parse_hhmm(start_time)
    end_min = _parse_hhmm(end_time)
    payloads = [(ticker, day, start_min, end_min, bool(include_ab_excluded)) for ticker in tickers]
    rows: list[dict] = []
    if worker_count <= 1:
        _v11_worker_init(str(data_root))
        for i, payload in enumerate(payloads, 1):
            rows.extend(_scan_one_ticker_day_candidates(payload))
            if i % 250 == 0 or i == len(payloads):
                print(
                    f"  [v11 historical_full_day] tickers={i:,}/{len(payloads):,} raw_rows={len(rows):,} "
                    f"elapsed={time.time() - t0:.1f}s",
                    flush=True,
                )
    else:
        with ProcessPoolExecutor(max_workers=worker_count, initializer=_v11_worker_init, initargs=(str(data_root),)) as ex:
            for i, result in enumerate(ex.map(_scan_one_ticker_day_candidates, payloads, chunksize=8), 1):
                if result:
                    rows.extend(result)
                if i % 250 == 0 or i == len(payloads):
                    print(
                        f"  [v11 historical_full_day] tickers={i:,}/{len(payloads):,} raw_rows={len(rows):,} "
                        f"elapsed={time.time() - t0:.1f}s",
                        flush=True,
                    )

    raw = pd.DataFrame(rows)
    if not raw.empty and "setup" in raw.columns:
        custom_mask = raw["setup"].astype(str).eq(candidate_scan.late_bb10.SETUP)
        if custom_mask.any():
            custom_slots = [
                _normalise_ts(value).floor("min")
                for value in raw.loc[custom_mask, "scan_slot_ist"].dropna().unique()
            ]
            alignment = candidate_scan.late_bb10.market_alignment_for_slots(
                tickers,
                custom_slots,
                candidate_scan._load_live_5m,
            )
            for idx in raw.index[custom_mask]:
                slot = _normalise_ts(raw.at[idx, "scan_slot_ist"]).floor("min")
                values = alignment.get(slot.isoformat(), {})
                raw.at[idx, "market_breadth"] = values.get("market_breadth", np.nan)
                raw.at[idx, "nifty_ema_up"] = values.get("nifty_ema_up", np.nan)
    if not raw.empty:
        raw = candidate_scan._dedupe_candidate_frame(raw)
    day_key = "".join(str(day).split("-"))
    slot_rows: list[dict] = []
    for slot in slots:
        if raw.empty or "scan_slot_ist" not in raw.columns:
            df = pd.DataFrame()
        else:
            df = raw.loc[raw["scan_slot_ist"].astype(str).eq(_fmt_ist(slot))].copy()
        raw_path = snapshot_dir / f"raw_candidate_tickers_{day_key}_{slot.strftime('%H%M')}.json"
        _write_candidate_snapshot(
            df,
            slot,
            raw_path,
            output_label="raw_pre_gate",
            payload_extra={"candidate_source": "day_wide_candidate_scan_v11_live_parity", "candidate_5m_dir": str(data_root)},
        )
        long_count = int(0 if df.empty else (df["side"].astype(str).str.upper() == "LONG").sum())
        short_count = int(0 if df.empty else (df["side"].astype(str).str.upper() == "SHORT").sum())
        slot_rows.append({
            "slot_ist": _fmt_ist(slot),
            "raw_candidate_count": int(len(df)),
            "long_candidates": long_count,
            "short_candidates": short_count,
            "elapsed_sec": round(time.time() - t0, 3),
            "snapshot_path": str(raw_path),
        })

    print(
        f"[v11 historical_full_day] finished {day}: raw={len(raw)} elapsed={time.time() - t0:.1f}s",
        flush=True,
    )
    return raw.reset_index(drop=True), pd.DataFrame(slot_rows)


def _available_historical_dates(data_root: Path, start_date: str = "", end_date: str = "") -> list[str]:
    root = Path(data_root)
    files = sorted(root.glob("*_stocks_indicators_5min.parquet"))
    if not files:
        raise SystemExit(f"[v11 all_available] no 5-minute parquet files found under {root}")

    end_text = str(end_date or "").strip() or pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    start_text = str(start_date or "").strip()
    end_day = pd.to_datetime(end_text, errors="coerce")
    start_day = pd.to_datetime(start_text, errors="coerce") if start_text else pd.NaT
    if pd.isna(end_day):
        raise SystemExit(f"[v11 all_available] invalid --end_date={end_date!r}")
    if start_text and pd.isna(start_day):
        raise SystemExit(f"[v11 all_available] invalid --start_date={start_date!r}")

    seen: set[str] = set()
    for i, fp in enumerate(files, 1):
        try:
            dates = pd.read_parquet(fp, columns=["date"])["date"]
        except Exception:
            continue
        ts = pd.to_datetime(dates, errors="coerce")
        if ts.empty:
            continue
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize("Asia/Kolkata")
        else:
            ts = ts.dt.tz_convert("Asia/Kolkata")
        days = ts.dropna().dt.strftime("%Y-%m-%d").unique().tolist()
        seen.update(days)
        if i % 250 == 0:
            print(f"[v11 all_available] indexed dates from {i:,}/{len(files):,} files", flush=True)

    out = []
    end_key = end_day.strftime("%Y-%m-%d")
    start_key = start_day.strftime("%Y-%m-%d") if pd.notna(start_day) else ""
    for day in sorted(seen):
        if start_key and day < start_key:
            continue
        if day > end_key:
            continue
        out.append(day)
    return out


def _candidate_roots(primary: Path, fallback: Path | None = None) -> list[Path]:
    roots: list[Path] = []
    for root in (primary, fallback):
        if root is None:
            continue
        path = Path(root)
        if not path.exists():
            continue
        if any(path == existing for existing in roots):
            continue
        roots.append(path)
    return roots


def _available_historical_dates_with_roots(
    primary: Path,
    fallback: Path | None,
    start_date: str = "",
    end_date: str = "",
) -> tuple[list[str], dict[str, Path]]:
    date_to_root: dict[str, Path] = {}
    for root in _candidate_roots(primary, fallback):
        try:
            root_dates = _available_historical_dates(root, start_date, end_date)
        except SystemExit as exc:
            print(f"[v11 all_available] skipping candidate root {root}: {exc}", flush=True)
            continue
        for day in root_dates:
            date_to_root.setdefault(day, root)
    dates = sorted(date_to_root)
    return dates, date_to_root


_TIER123_WORKER_DATA_ROOT: Path | None = None
_TIER123_WORKER_MARKET_CTX: dict[str, dict[pd.Timestamp, dict]] = {}
_TIER123_WORKER_DATES: set[str] = set()


def _tier123_profile_enabled(profile: str | None) -> bool:
    return _normalise_selected_strategy_profile(profile) == TIER123_BALANCED_PROFILE


def _tier123_prepare_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ATR" not in out.columns or out["ATR"].isna().all() or "VWAP" not in out.columns or out["VWAP"].isna().all():
        return candidate_scan.v2._prepare_5m(out)
    out["Volume_SMA20"] = out.groupby("date_only")["volume"].transform(
        lambda s: s.shift(1).rolling(candidate_scan.v2.VWAP_LOOKBACK, min_periods=8).mean()
    )
    out["traded_value_rs"] = out["close"] * out["volume"]
    out["day_value_so_far_rs"] = out.groupby("date_only")["traded_value_rs"].cumsum()
    out["range"] = out["high"] - out["low"]
    range_nonzero = out["range"].replace(0, np.nan)
    out["body_pct"] = (out["close"] - out["open"]).abs() / range_nonzero
    out["close_loc"] = (out["close"] - out["low"]) / range_nonzero
    out["upper_wick_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / range_nonzero
    out["lower_wick_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / range_nonzero
    out["vol_ratio"] = out["volume"] / out["Volume_SMA20"].replace(0, np.nan)
    out["atr_pct"] = out["ATR"] / out["close"].replace(0, np.nan)
    # --- VWAP FIX (2026-06-13, mirrors research_v11_tier123_new_setups._prepare_probe_5m) ----------
    # The 5-min source parquet "VWAP" column is stale/anchored (near-constant, far from price), which
    # corrupts vwap_dist_atr + VWAP-based detections + regime. Recompute a proper intraday SESSION VWAP
    # (typical-price x volume, reset each day) so the tier123 overlay / probe use a correct VWAP.
    out["VWAP_parquet"] = out["VWAP"]
    _tp = (out["high"] + out["low"] + out["close"]) / 3.0
    _cum_pv = (_tp * out["volume"]).groupby(out["date_only"]).cumsum()
    _cum_v = out["volume"].groupby(out["date_only"]).cumsum().replace(0, np.nan)
    _sess_vwap = _cum_pv / _cum_v
    out["VWAP"] = _sess_vwap.where(_sess_vwap.notna(), out["VWAP_parquet"])
    _atr_floor = out["ATR"].where(out["ATR"] >= 0.0003 * out["close"], 0.0003 * out["close"])
    out["vwap_dist_atr"] = ((out["close"] - out["VWAP"]) / _atr_floor.replace(0, np.nan)).clip(-15.0, 15.0)
    return out


def _tier123_prev_day_levels(prepared: pd.DataFrame) -> pd.DataFrame:
    out = prepared.drop(columns=["prev_day_high", "prev_day_low", "prev_day_close_calc"], errors="ignore")
    daily = out.groupby("date_only", sort=True).agg(
        day_high=("high", "max"),
        day_low=("low", "min"),
        day_close=("close", "last"),
    )
    daily["prev_day_high"] = daily["day_high"].shift(1)
    daily["prev_day_low"] = daily["day_low"].shift(1)
    daily["prev_day_close_calc"] = daily["day_close"].shift(1)
    return out.join(daily[["prev_day_high", "prev_day_low", "prev_day_close_calc"]], on="date_only")


def _tier123_read_5m(ticker: str, data_root: Path, date_keys: set[str] | None = None) -> pd.DataFrame | None:
    path = Path(data_root) / f"{str(ticker).upper()}_stocks_indicators_5min.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df.empty or "date" not in df.columns:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = (
        df.dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")  # defensive: _eq_live2 dup-ts
        .reset_index(drop=True)
    )
    df = _drop_opening_snapshot_rows(df)  # strict mode only; no-op by default
    df["date_only"] = df["date"].dt.strftime("%Y-%m-%d")
    if date_keys:
        df = df.loc[df["date_only"].isin(date_keys)].copy()
    if df.empty:
        return None
    return _tier123_prev_day_levels(_tier123_prepare_5m(df))


def _tier123_market_context(data_root: Path, date_keys: set[str]) -> dict[str, dict[pd.Timestamp, dict]]:
    # NIFTYBEES first: keeps the backtest regime in parity with live
    # (v2._load_market_context is NIFTYBEES-first) and guarantees a
    # volume-bearing ETF series so session VWAP (BULL/BEAR classification)
    # works. The true NIFTY 50 index has zero traded volume -> NaN VWAP ->
    # regime would collapse to TREND/NEUTRAL, so it must never lead here.
    for ticker in ["NIFTYBEES", "NIFTY", "NIFTY50", "NIFTY_50"]:
        df = _tier123_read_5m(ticker, data_root, date_keys)
        if df is None or df.empty:
            continue
        ctx: dict[str, dict[pd.Timestamp, dict]] = {}
        for day, g in df.groupby("date_only", sort=True):
            g = g.reset_index(drop=True)
            day_open = float(g["open"].iloc[0])
            by_ts: dict[pd.Timestamp, dict] = {}
            for _, row in g.iterrows():
                ts = _normalise_ts(row["date"])
                close = float(row["close"])
                ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
                vwap = float(row.get("VWAP", np.nan))
                adx = float(row.get("ADX", np.nan))
                if np.isfinite(vwap) and close > vwap and ret >= 0.20:
                    regime = "BULL"
                elif np.isfinite(vwap) and close < vwap and ret <= -0.20:
                    regime = "BEAR"
                elif np.isfinite(adx) and adx >= 25:
                    regime = "TREND"
                else:
                    regime = "NEUTRAL"
                by_ts[ts] = {"market_ret_pct": float(ret), "regime": regime}
            ctx[str(day)] = by_ts
        return ctx
    return {}


def _tier123_bar_context(ctx: dict[str, dict[pd.Timestamp, dict]], day: str, ts: pd.Timestamp) -> tuple[float, str]:
    by_ts = ctx.get(str(day), {})
    if not by_ts:
        return 0.0, "UNKNOWN"
    if ts in by_ts:
        rec = by_ts[ts]
        return float(rec["market_ret_pct"]), str(rec["regime"])
    keys = [key for key in by_ts if key <= ts]
    if not keys:
        return 0.0, "UNKNOWN"
    rec = by_ts[max(keys)]
    return float(rec["market_ret_pct"]), str(rec["regime"])


def _tier123_quality(side: str, setup: str, row: pd.Series, rs_pct: float, market_ret: float, regime: str) -> float:
    close_loc = float(row.get("close_loc", 0.5))
    vol_ratio = float(row.get("vol_ratio", 1.0))
    vwap_dist = float(row.get("vwap_dist_atr", 0.0))
    body_pct = float(row.get("body_pct", 0.0))
    adx = float(row.get("ADX", 0.0))
    score = 20.0
    if side == "LONG":
        score += 18.0 * max(rs_pct, 0.0)
        score += 15.0 * max(close_loc - 0.45, 0.0)
        score += 5.0 if market_ret >= -0.20 else -8.0
        score += 5.0 if regime != "BEAR" else -10.0
    else:
        score += 18.0 * max(-rs_pct, 0.0)
        score += 15.0 * max(0.55 - close_loc, 0.0)
        score += 5.0 if market_ret <= 0.20 else -8.0
        score += 5.0 if regime != "BULL" else -10.0
    score += 8.0 * min(max(vol_ratio - 1.0, 0.0), 3.0)
    score += 8.0 * body_pct
    score += 0.35 * max(adx - 15.0, 0.0)
    if "FADE" in setup:
        score += 4.0 * min(abs(vwap_dist), 3.5)
    elif side == "LONG":
        score += 5.0 if vwap_dist >= -0.25 else -5.0
    else:
        score += 5.0 if vwap_dist <= 0.25 else -5.0
    return float(score)


def _tier123_candidate(
    ticker: str,
    setup: str,
    side: str,
    row: pd.Series,
    rs_pct: float,
    market_ret: float,
    regime: str,
    reason: str,
) -> dict:
    ts = _normalise_ts(row["date"])
    score = _tier123_quality(side, setup, row, rs_pct, market_ret, regime)
    return {
        "candidate_id": f"{ticker}|{side}|{setup}|{ts.isoformat()}",
        "scan_session": "v11_tier123_balanced_addon",
        "selection_mode": "tier123_balanced_addon_gate",
        "candidate_family": "Tier123 balanced add-on",
        "scan_slot_ist": _fmt_ist(ts),
        "signal_time_ist": _fmt_ist(ts),
        "bar_time_ist": _fmt_ist(ts),
        "signal_timestamp_convention": "end_labeled_completed_5m",
        "ticker": ticker,
        "side": side,
        "setup": setup,
        "signal_open": float(row["open"]),
        "signal_high": float(row["high"]),
        "signal_low": float(row["low"]),
        "signal_close": float(row["close"]),
        "signal_volume": float(row.get("volume", 0.0)),
        "signal_adx": float(row.get("ADX", np.nan)),
        "signal_rsi": float(row.get("RSI", np.nan)),
        "signal_vol_ratio20": float(row.get("vol_ratio", np.nan)),
        "quality_score": score,
        "ranker_score": score,
        "score": score,
        "rs_pct": float(rs_pct),
        "market_ret_pct": float(market_ret),
        "regime": regime,
        "vol_ratio": float(row.get("vol_ratio", np.nan)),
        "atr_pct": float(row.get("atr_pct", np.nan)),
        "body_pct": float(row.get("body_pct", np.nan)),
        "close_loc": float(row.get("close_loc", np.nan)),
        "vwap_dist_atr": float(row.get("vwap_dist_atr", np.nan)),
        "reason": reason,
        "status": "TIER123_BALANCED_RAW",
        "created_at_ist": _fmt_ist(pd.Timestamp.now(tz="Asia/Kolkata")),
    }


def _tier123_scan_ticker_dates(
    ticker: str,
    data_root: Path,
    date_keys: set[str],
    market_ctx: dict[str, dict[pd.Timestamp, dict]],
) -> list[dict]:
    df = _tier123_read_5m(ticker, data_root, date_keys)
    if df is None or df.empty:
        return []
    rows: list[dict] = []
    for day, group in df.groupby("date_only", sort=True):
        if str(day) not in date_keys:
            continue
        g = group.reset_index(drop=True)
        if len(g) < 15:
            continue
        day_open = float(g["open"].iloc[0])
        for i in range(4, len(g) - 1):
            row = g.iloc[i]
            prev = g.iloc[i - 1]
            ts = _normalise_ts(row["date"])
            minute = ts.hour * 60 + ts.minute
            if minute < 570 or minute > 900:
                continue
            close = float(row["close"])
            open_px = float(row["open"])
            high = float(row["high"])
            atr = float(row.get("ATR", np.nan))
            vwap = float(row.get("VWAP", np.nan))
            ema20 = float(row.get("EMA_20", np.nan))
            ema50 = float(row.get("EMA_50", np.nan))
            adx = float(row.get("ADX", np.nan))
            rsi = float(row.get("RSI", np.nan))
            stoch = float(row.get("Stoch_%K", np.nan))
            vol_ratio = float(row.get("vol_ratio", np.nan))
            close_loc = float(row.get("close_loc", np.nan))
            lower_wick = float(row.get("lower_wick_pct", np.nan))
            upper_wick = float(row.get("upper_wick_pct", np.nan))
            vwap_dist = float(row.get("vwap_dist_atr", np.nan))
            rng = float(row.get("range", np.nan))
            if not np.isfinite(atr) or atr <= 0 or not np.isfinite(vol_ratio) or vol_ratio < 1.0:
                continue
            if close < 25 or float(row.get("day_value_so_far_rs", 0.0)) < 20_000_000:
                continue
            if np.isfinite(rng) and rng > 3.5 * atr:
                continue

            market_ret, regime = _tier123_bar_context(market_ctx, str(day), ts)
            stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
            rs_pct = stock_ret - market_ret
            below_vwap = np.isfinite(vwap) and close < vwap
            above_vwap = np.isfinite(vwap) and close > vwap
            short_struct = close < open_px and close_loc <= 0.45

            ema20_slope = float(g["EMA_20"].iloc[i] - g["EMA_20"].iloc[max(0, i - 3)]) if np.isfinite(ema20) else np.nan
            if (
                np.isfinite(ema20)
                and np.isfinite(ema50)
                and np.isfinite(ema20_slope)
                and minute <= TIER123_T_STAIR_SHORT_MAX_SIGNAL_MINUTE
                and regime in {"BEAR", "TREND"}
                and ema20_slope < 0
                and close < ema20 <= ema50
                and high >= ema20 - 0.25 * atr
                and short_struct
                and below_vwap
                and rs_pct <= -0.05
            ):
                rows.append(_tier123_candidate(
                    ticker,
                    "T_TREND_DAY_EMA_STAIR_SHORT",
                    "SHORT",
                    row,
                    rs_pct,
                    market_ret,
                    regime,
                    "tier123_late_bearish_trend_day_ema_stair_short",
                ))

            controlled = regime in {"NEUTRAL", "UNKNOWN"} or (np.isfinite(adx) and adx <= 22)
            if (
                minute >= 660
                and controlled
                and vwap_dist <= -2.0
                and lower_wick >= 0.35
                and close > float(prev["high"])
                and (not np.isfinite(rsi) or rsi <= 38)
                and (not np.isfinite(stoch) or stoch <= 35)
            ):
                rows.append(_tier123_candidate(
                    ticker,
                    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG",
                    "LONG",
                    row,
                    rs_pct,
                    market_ret,
                    regime,
                    "tier123_high_volume_controlled_vwap_extreme_fade_long",
                ))

            if (
                minute >= 660
                and controlled
                and vwap_dist >= 2.0
                and upper_wick >= 0.35
                and close < float(prev["low"])
                and (not np.isfinite(rsi) or rsi >= 62)
                and (not np.isfinite(stoch) or stoch >= 65)
            ):
                candidate = _tier123_candidate(
                    ticker,
                    "MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT",
                    "SHORT",
                    row,
                    rs_pct,
                    market_ret,
                    regime,
                    "tier123_quiet_volume_quality_controlled_vwap_extreme_fade_short",
                )
                rows.append(candidate)
    return rows


def _tier123_probe_gate(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw
    work = raw.copy()
    work["_day"] = pd.to_datetime(work["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
    work["_score"] = pd.to_numeric(work["quality_score"], errors="coerce").fillna(0.0)
    work = (
        work.sort_values(["setup", "ticker", "_day", "_score"], ascending=[True, True, True, False])
        .drop_duplicates(subset=["setup", "ticker", "_day"], keep="first")
    )
    frames = []
    for _, group in work.groupby("setup", sort=True):
        frames.append(group.sort_values("_score", ascending=False).head(TIER123_MAX_RAW_PER_SETUP))
    out = pd.concat(frames, ignore_index=True) if frames else work.iloc[0:0].copy()
    return out.drop(columns=["_day", "_score"], errors="ignore").reset_index(drop=True)


def _tier123_load_universe(data_root: Path) -> list[str]:
    try:
        from filtered_stocks_NSE_futures_only import selected_stocks as futures_stocks

        universe = sorted({str(x).upper().replace(".NS", "").strip() for x in futures_stocks if str(x).strip()})
    except Exception:
        try:
            universe = sorted({str(x).upper().replace(".NS", "").strip() for x in candidate_scan.v2._load_universe() if str(x).strip()})
        except Exception:
            universe = []
    return [
        ticker
        for ticker in universe
        if ticker
        and not ticker.startswith("NIFTY")
        and not ticker.endswith("BEES")
        and (Path(data_root) / f"{ticker}_stocks_indicators_5min.parquet").exists()
    ]


def _tier123_worker_init(data_root: str, dates: tuple[str, ...], market_ctx: dict[str, dict[pd.Timestamp, dict]]) -> None:
    global _TIER123_WORKER_DATA_ROOT, _TIER123_WORKER_DATES, _TIER123_WORKER_MARKET_CTX
    _TIER123_WORKER_DATA_ROOT = Path(data_root)
    _TIER123_WORKER_DATES = set(dates)
    _TIER123_WORKER_MARKET_CTX = market_ctx


def _tier123_scan_ticker_job(ticker: str) -> tuple[str, list[dict], str | None]:
    try:
        if _TIER123_WORKER_DATA_ROOT is None:
            return ticker, [], "tier123_worker_data_root_not_initialised"
        rows = _tier123_scan_ticker_dates(
            str(ticker).upper(),
            _TIER123_WORKER_DATA_ROOT,
            _TIER123_WORKER_DATES,
            _TIER123_WORKER_MARKET_CTX,
        )
        return ticker, rows, None
    except Exception as exc:
        return ticker, [], repr(exc)


def _scan_tier123_balanced_candidates_for_dates(
    dates: list[str],
    date_to_root: dict[str, Path],
    *,
    workers: int,
    out_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not dates:
        return pd.DataFrame(), pd.DataFrame()
    frames: list[pd.DataFrame] = []
    stats_rows: list[dict] = []
    worker_count = max(1, int(workers))
    by_root: dict[Path, list[str]] = {}
    for day in dates:
        root = Path(date_to_root.get(day, Path(DEFAULT_CANDIDATE_5M_DIR)))
        by_root.setdefault(root, []).append(day)

    for data_root, root_dates in sorted(by_root.items(), key=lambda item: str(item[0])):
        date_keys = set(root_dates)
        market_ctx = _tier123_market_context(data_root, date_keys)
        universe = _tier123_load_universe(data_root)
        rows: list[dict] = []
        errors: list[dict] = []
        t0 = time.time()
        print(
            f"[v11 tier123] scanning balanced add-on dates={len(root_dates):,} "
            f"tickers={len(universe):,} workers={worker_count} data_root={data_root}",
            flush=True,
        )
        if worker_count <= 1:
            _tier123_worker_init(str(data_root), tuple(sorted(date_keys)), market_ctx)
            for i, ticker in enumerate(universe, 1):
                _, ticker_rows, err = _tier123_scan_ticker_job(ticker)
                if err:
                    errors.append({"ticker": ticker, "error": err})
                rows.extend(ticker_rows)
                if i % 25 == 0 or i == len(universe):
                    print(
                        f"  [v11 tier123] tickers={i:,}/{len(universe):,} raw_rows={len(rows):,} "
                        f"elapsed={time.time() - t0:.1f}s",
                        flush=True,
                    )
        else:
            with ProcessPoolExecutor(
                max_workers=worker_count,
                initializer=_tier123_worker_init,
                initargs=(str(data_root), tuple(sorted(date_keys)), market_ctx),
            ) as ex:
                for i, (ticker, ticker_rows, err) in enumerate(ex.map(_tier123_scan_ticker_job, universe, chunksize=4), 1):
                    if err:
                        errors.append({"ticker": ticker, "error": err})
                    rows.extend(ticker_rows)
                    if i % 25 == 0 or i == len(universe):
                        print(
                            f"  [v11 tier123] tickers={i:,}/{len(universe):,} raw_rows={len(rows):,} "
                            f"elapsed={time.time() - t0:.1f}s",
                            flush=True,
                        )
        raw = pd.DataFrame(rows)
        gated = _tier123_probe_gate(raw) if not raw.empty else raw
        if not gated.empty:
            gated["v11_tier123_data_root"] = str(data_root)
            frames.append(gated)
        stats_rows.append({
            "stage": "tier123_balanced_candidate_scan",
            "candidate_5m_dir": str(data_root),
            "dates": len(root_dates),
            "first_date": min(root_dates),
            "last_date": max(root_dates),
            "tickers": len(universe),
            "raw_candidates": int(len(raw)),
            "gated_candidates": int(len(gated)),
            "scan_errors": int(len(errors)),
            "elapsed_sec": round(time.time() - t0, 3),
        })
        if errors:
            pd.DataFrame(errors).to_csv(out_dir / f"tier123_balanced_scan_errors_{abs(hash(str(data_root))) % 100000}.csv", index=False)

    candidates = _concat_frames(frames)
    if not candidates.empty:
        candidates = _tier123_probe_gate(candidates)
    return candidates.reset_index(drop=True), pd.DataFrame(stats_rows)


def _trade_key_frame(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype="object")
    return df["ticker"].astype(str).str.upper().str.strip() + "|" + df["trade_date"].astype(str).str[:10]


def _sort_resolved_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    out["_entry_sort"] = pd.to_datetime(out.get("entry_time_v6", out.get("old_entry_time_v7")), errors="coerce")
    if "trade_date" in out.columns:
        out["_trade_date_sort"] = out["trade_date"].astype(str)
    else:
        out["_trade_date_sort"] = ""
    return (
        out.sort_values(["_trade_date_sort", "_entry_sort", "ticker", "setup"], ascending=[True, True, True, True])
        .drop(columns=["_entry_sort", "_trade_date_sort"], errors="ignore")
        .reset_index(drop=True)
    )


def _apply_tier123_balanced_addon(
    *,
    base_final: pd.DataFrame,
    dates: list[str],
    date_to_root: dict[str, Path],
    args: argparse.Namespace,
    out_dir: Path,
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not _tier123_profile_enabled(str(args.selected_strategy_profile)):
        return base_final, pd.DataFrame()

    candidates, scan_stats = _scan_tier123_balanced_candidates_for_dates(
        dates,
        date_to_root,
        workers=int(args.workers),
        out_dir=out_dir,
    )
    candidates.to_csv(out_dir / f"{label}_tier123_balanced_raw_candidates.csv", index=False)
    scan_stats.to_csv(out_dir / f"{label}_tier123_balanced_scan_stats.csv", index=False)
    if candidates.empty:
        stats = pd.DataFrame([{
            "stage": f"{label}_tier123_balanced_addon",
            "tier123_balanced_raw_candidates": 0,
            "tier123_balanced_entry_signals": 0,
            "tier123_balanced_selected_signals": 0,
            "tier123_balanced_resolved_trades": 0,
            "tier123_balanced_non_overlap_trades": 0,
            "tier123_balanced_overlap_trades": 0,
        }])
        return base_final, stats

    selected, selected_rejects, selected_stats = _apply_selected_strategy_profile(
        candidates,
        str(args.selected_strategy_profile),
    )
    selected.to_csv(out_dir / f"{label}_tier123_balanced_selected_strategy_signals.csv", index=False)
    selected_rejects.to_csv(out_dir / f"{label}_tier123_balanced_selected_strategy_rejects.csv", index=False)

    entry_signals, entry_raw, entry_rejects = _build_v7_entry_engine_signals(selected)
    entry_raw.to_csv(out_dir / f"{label}_tier123_balanced_entry_engine_raw_entries.csv", index=False)
    entry_rejects.to_csv(out_dir / f"{label}_tier123_balanced_entry_engine_rejects.csv", index=False)
    entry_signals.to_csv(out_dir / f"{label}_tier123_balanced_entry_engine_signals.csv", index=False)

    addon_final = pd.DataFrame()
    if not entry_signals.empty:
        addon_final = _resolve_v7_entry_engine_signals(
            entry_signals,
            label=f"{label}_tier123_balanced_addon",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )
    addon_final.to_csv(out_dir / f"{label}_tier123_balanced_resolved_trades.csv", index=False)

    if addon_final.empty:
        stats = pd.DataFrame([{
            "stage": f"{label}_tier123_balanced_addon",
            "tier123_balanced_raw_candidates": int(len(candidates)),
            "tier123_balanced_entry_signals": int(len(entry_signals)),
            **selected_stats,
            "tier123_balanced_resolved_trades": 0,
            "tier123_balanced_non_overlap_trades": 0,
            "tier123_balanced_overlap_trades": 0,
        }])
        return base_final, stats

    base_keys = set(_trade_key_frame(base_final))
    addon_keys = _trade_key_frame(addon_final)
    non_overlap = addon_final.loc[~addon_keys.isin(base_keys)].copy()
    overlap = addon_final.loc[addon_keys.isin(base_keys)].copy()
    non_overlap.to_csv(out_dir / f"{label}_tier123_balanced_non_overlap_trades.csv", index=False)
    overlap.to_csv(out_dir / f"{label}_tier123_balanced_overlap_rejected_trades.csv", index=False)
    combined = _sort_resolved_trades(_concat_frames([base_final, non_overlap]))
    stats = pd.DataFrame([{
        "stage": f"{label}_tier123_balanced_addon",
        "tier123_balanced_raw_candidates": int(len(candidates)),
        "tier123_balanced_entry_raw_entries": int(len(entry_raw)),
        "tier123_balanced_entry_rejects": int(len(entry_rejects)),
        "tier123_balanced_entry_signals": int(len(entry_signals)),
        **selected_stats,
        "tier123_balanced_resolved_trades": int(len(addon_final)),
        "tier123_balanced_non_overlap_trades": int(len(non_overlap)),
        "tier123_balanced_overlap_trades": int(len(overlap)),
        "tier123_balanced_combined_trades": int(len(combined)),
    }])
    return combined, stats


def _candidate_time(row: pd.Series, *columns: str) -> pd.Timestamp:
    for col in columns:
        if col in row.index and pd.notna(row.get(col)):
            ts = _normalise_ts(row.get(col))
            if pd.notna(ts):
                return ts
    return pd.NaT


def _candidate_sort_frame(candidates: pd.DataFrame, date_hint: str = "") -> pd.DataFrame:
    out = candidates.copy()
    if out.empty:
        return out
    out["_v11_input_order"] = np.arange(len(out))
    out["_v11_slot_ts"] = [
        _candidate_time(row, "scan_slot_ist", "signal_time_ist", "bar_time_ist")
        for _, row in out.iterrows()
    ]
    out["_v11_signal_ts"] = [
        _candidate_time(row, "signal_time_ist", "signal_ts", "bar_time_ist", "scan_slot_ist")
        for _, row in out.iterrows()
    ]
    hint_day = str(date_hint or "").strip()
    out["_v11_day"] = [
        (ts.strftime("%Y-%m-%d") if pd.notna(ts) else hint_day)
        for ts in out["_v11_signal_ts"]
    ]
    out["_v11_slot_key"] = [
        _fmt_ist(ts) if pd.notna(ts) else ""
        for ts in out["_v11_slot_ts"]
    ]
    return out


def _drop_v11_temp_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    return df.drop(columns=[c for c in df.columns if str(c).startswith("_v11_")], errors="ignore")


def _ensure_candidate_id(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty or "candidate_id" in out.columns:
        return out
    for col in ("ticker", "side", "setup", "signal_time_ist"):
        if col not in out.columns:
            out[col] = ""
    out["candidate_id"] = (
        out["ticker"].astype(str).str.upper().str.strip()
        + "|"
        + out["side"].astype(str).str.upper().str.strip()
        + "|"
        + out["setup"].astype(str).str.strip()
        + "|"
        + out["signal_time_ist"].astype(str)
    )
    return out


def _tag_pipeline_frame(df: pd.DataFrame, *, day: str, slot_key: str, stage: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    out["v11_pipeline_day"] = day
    out["v11_pipeline_slot_ist"] = slot_key
    out["v11_pipeline_stage"] = stage
    return out


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _live_like_daily_dedupe(candidates: pd.DataFrame, date_hint: str = "") -> tuple[pd.DataFrame, dict]:
    if candidates is None or candidates.empty:
        return pd.DataFrame() if candidates is None else candidates.copy(), {"live_like_written": 0, "live_like_duplicates": 0}

    work = _ensure_candidate_id(candidates)
    if "_v11_day" not in work.columns or "_v11_live_order" not in work.columns:
        work = _candidate_sort_frame(work, date_hint)
        work["_v11_live_order"] = np.arange(len(work))

    rows: list[dict] = []
    duplicates = 0
    for _, day_df in work.sort_values(["_v11_day", "_v11_live_order"]).groupby("_v11_day", sort=True, dropna=False):
        existing_ids: set[str] = set()
        existing_tickers: set[str] = set()
        for _, row in day_df.iterrows():
            cid = str(row.get("candidate_id", ""))
            ticker = str(row.get("ticker", "")).upper().strip()
            if not cid or not ticker or cid in existing_ids or ticker in existing_tickers:
                duplicates += 1
                continue
            item = row.to_dict()
            rows.append(item)
            existing_ids.add(cid)
            existing_tickers.add(ticker)

    out = pd.DataFrame(rows)
    return _drop_v11_temp_cols(out), {
        "live_like_written": int(len(out)),
        "live_like_duplicates": int(duplicates),
    }


def _apply_v7_live_strategy(
    raw_candidates: pd.DataFrame,
    date_hint: str = "",
    *,
    ab_gate_profile: str = "off",
    ab_gate_min_quality: float = 250.0,
    ab_gate_max_per_side: int = 1,
    ab_gate_max_per_slot: int = 2,
    ab_gate_setups: tuple[str, ...] | None = None,
    selected_strategy_profile: str = "none",
) -> dict:
    ab_gate_profile = _normalise_ab_gate_profile(ab_gate_profile)
    ab_gate_setups = tuple(ab_gate_setups or AB_PROBATION_SETUPS)
    if raw_candidates is None or raw_candidates.empty:
        empty_stats = {
            "slots_processed": 0,
            "raw_candidates": 0,
            "ranked_raw_candidates": 0,
            "v8_gated_candidates": 0,
            "regular_v8_gated_candidates": 0,
            "research_rejected_candidates": 0,
            "pre_dedupe_live_candidates": 0,
            "live_like_candidates": 0,
            "live_like_duplicates": 0,
            **_ab_gate_base_stats(
                profile=ab_gate_profile,
                min_quality=ab_gate_min_quality,
                max_per_side=ab_gate_max_per_side,
                max_per_slot=ab_gate_max_per_slot,
                allowed_setups=ab_gate_setups,
            ),
        }
        return {
            "ranked_raw_candidates": pd.DataFrame(),
            "v8_gated_candidates": pd.DataFrame(),
            "research_rejected_candidates": pd.DataFrame(),
            "pre_dedupe_live_candidates": pd.DataFrame(),
            "live_like_candidates": pd.DataFrame(),
            "slot_audit": pd.DataFrame(),
            "stats": empty_stats,
        }

    work = _candidate_sort_frame(raw_candidates, date_hint)
    work = work.sort_values(["_v11_day", "_v11_slot_ts", "_v11_input_order"]).reset_index(drop=True)

    ranked_frames: list[pd.DataFrame] = []
    gated_frames: list[pd.DataFrame] = []
    rejected_frames: list[pd.DataFrame] = []
    accepted_frames: list[pd.DataFrame] = []
    slot_rows: list[dict] = []
    live_order = 0

    for day, day_df in work.groupby("_v11_day", sort=True, dropna=False):
        day_text = str(day or date_hint or "").strip()
        for slot_key, slot_df in day_df.groupby("_v11_slot_key", sort=True, dropna=False):
            slot_key_text = str(slot_key or "")
            slot_t0 = time.time()
            raw_slot = _drop_v11_temp_cols(slot_df)

            ranked = live_discovery.add_live_ranker_scores(raw_slot, day_text)
            gated, gate_stats = live_discovery.apply_v8_live_gate(ranked)
            ab_accepted, ab_stats = _apply_ab_probation_gate(
                ranked,
                profile=ab_gate_profile,
                min_quality=ab_gate_min_quality,
                max_per_side=ab_gate_max_per_side,
                max_per_slot=ab_gate_max_per_slot,
                allowed_setups=ab_gate_setups,
            )
            # v11 live overlay — mirrors eqidv2_v11_live_overlay.apply_live_candidate_overlay
            # which runs in the live pipeline alongside the standard v8 gate.  It admits
            # candidates from V11_PROFILE_SETUP_UNIVERSE (including E_VWAP_LOSE_EARLY_SHORT)
            # that failed the early quality gate but pass the selected-strategy profile rules.
            # conf mode: the live overlay module doesn't know "final_setup_conf"; feed it the
            # BROADEST production profile it understands (tier123_balanced) so it still admits the
            # early-quality-gate failers we need (E_VWAP_LOSE_EARLY_SHORT + the tier123 T setup).
            # The final_setup_conf selected-strategy mask downstream does the real 9-setup selection.
            _overlay_profile = (
                TIER123_BALANCED_PROFILE
                if str(selected_strategy_profile) == FINAL_CONF_PROFILE
                else str(selected_strategy_profile)
            )
            overlay_accepted, _overlay_rejected, _overlay_stats = v11_overlay.apply_live_candidate_overlay(
                ranked,
                profile=_overlay_profile,
                ab_gate_profile=str(ab_gate_profile),
            )
            # Exact live semantics: the V11 overlay REPLACES ordinary V7 rows
            # for setups in its override universe.  A simple union here created
            # backtest-only trades when the overlay had explicitly rejected the
            # same setup (INFOBEAN/HINDUNILVR/DMART on 2026-07-28).
            pre_research = v11_overlay.merge_v7_and_v11_candidates(
                gated,
                overlay_accepted,
                profile=_overlay_profile,
            )
            accepted, rejected, research_stats = live_discovery.apply_research_live_filters(pre_research, day_text)
            # conf-mode raw-pool re-admit: setups validated on the RAW pre-gate pool (L_DOUBLE_BOTTOM_VWAP,
            # L_PRESSURE_BURST_VWAP) are dropped 100% by the v8 gate. Re-admit them straight from `ranked`
            # (bypassing v8 + research, consistent with their validation); the entry-window filter, the
            # final_setup_conf selected-strategy mask, the conf pre-momentum gate and conf exit still apply.
            if (str(selected_strategy_profile) == FINAL_CONF_PROFILE and _FINAL_CONF_READMIT_SETUPS
                    and not ranked.empty and "setup" in ranked.columns):
                _readmit = ranked[ranked["setup"].astype(str).isin(_FINAL_CONF_READMIT_SETUPS)]
                if not _readmit.empty:
                    accepted = _concat_frames([accepted, _readmit])
                    if not accepted.empty:
                        accepted = (
                            _ensure_candidate_id(accepted)
                            .drop_duplicates(subset=["candidate_id"], keep="first")
                            .reset_index(drop=True)
                        )
            # Live parity fix: apply entry window filter (09:30-14:30) after research filters.
            # The live scanner calls _filter_entry_window after apply_research_live_filters
            # at scanner line 1539; the original backtester was missing this step entirely.
            accepted, _ew_rejected = live_discovery._filter_entry_window(accepted)
            gated_output = _concat_frames([gated, ab_accepted])
            if not gated_output.empty:
                gated_output = _ensure_candidate_id(gated_output).drop_duplicates(subset=["candidate_id"], keep="first").reset_index(drop=True)
            # overlay_accepted is now consumed by pre_research -> accepted (via research + entry-window filters)
            accepted_combined = _concat_frames([accepted, ab_accepted])
            if not accepted_combined.empty:
                accepted_combined = (
                    _ensure_candidate_id(accepted_combined)
                    .drop_duplicates(subset=["candidate_id"], keep="first")
                    .reset_index(drop=True)
                )

            if not ranked.empty:
                ranked_frames.append(_tag_pipeline_frame(ranked, day=day_text, slot_key=slot_key_text, stage="ranked_raw"))
            if not gated_output.empty:
                gated_frames.append(_tag_pipeline_frame(gated_output, day=day_text, slot_key=slot_key_text, stage="v8_or_ab_live_gate"))
            if not rejected.empty:
                rejected_frames.append(_tag_pipeline_frame(rejected, day=day_text, slot_key=slot_key_text, stage="research_live_filter_rejected"))
            if not accepted_combined.empty:
                accepted_combined = accepted_combined.copy()
                accepted_combined["_v11_day"] = day_text
                accepted_combined["_v11_live_order"] = np.arange(live_order, live_order + len(accepted_combined))
                live_order += len(accepted_combined)
                accepted_frames.append(_tag_pipeline_frame(accepted_combined, day=day_text, slot_key=slot_key_text, stage="live_accepted_pre_dedupe"))

            slot_rows.append({
                "day": day_text,
                "slot_ist": slot_key_text,
                "raw_candidates": int(len(raw_slot)),
                "ranked_raw_candidates": int(len(ranked)),
                "v8_gated_candidates": int(len(gated_output)),
                "regular_v8_gated_candidates": int(len(gated)),
                "research_rejected_candidates": int(len(rejected)),
                "pre_dedupe_live_candidates": int(len(accepted_combined)),
                "elapsed_sec": round(time.time() - slot_t0, 3),
                **gate_stats,
                **ab_stats,
                **research_stats,
            })

    ranked_raw = _concat_frames(ranked_frames)
    v8_gated = _concat_frames(gated_frames)
    research_rejected = _concat_frames(rejected_frames)
    pre_dedupe = _concat_frames(accepted_frames)
    live_like, dedupe_stats = _live_like_daily_dedupe(pre_dedupe, date_hint)
    slot_audit = pd.DataFrame(slot_rows)
    stats = {
        "slots_processed": int(len(slot_audit)),
        "raw_candidates": int(len(raw_candidates)),
        "ranked_raw_candidates": int(len(ranked_raw)),
        "v8_gated_candidates": int(len(v8_gated)),
        "regular_v8_gated_candidates": int(slot_audit["regular_v8_gated_candidates"].sum()) if "regular_v8_gated_candidates" in slot_audit.columns else 0,
        "research_rejected_candidates": int(len(research_rejected)),
        "pre_dedupe_live_candidates": int(len(pre_dedupe)),
        "live_like_candidates": int(len(live_like)),
        "live_like_duplicates": int(dedupe_stats.get("live_like_duplicates", 0)),
        **_ab_gate_base_stats(
            profile=ab_gate_profile,
            min_quality=ab_gate_min_quality,
            max_per_side=ab_gate_max_per_side,
            max_per_slot=ab_gate_max_per_slot,
            allowed_setups=ab_gate_setups,
        ),
    }
    for col in ("ab_gate_candidates", "ab_gate_quality_passed", "ab_gate_quality_rejected", "ab_gate_accepted"):
        if col in slot_audit.columns:
            stats[col] = int(pd.to_numeric(slot_audit[col], errors="coerce").fillna(0).sum())
    return {
        "ranked_raw_candidates": _drop_v11_temp_cols(ranked_raw),
        "v8_gated_candidates": _drop_v11_temp_cols(v8_gated),
        "research_rejected_candidates": _drop_v11_temp_cols(research_rejected),
        "pre_dedupe_live_candidates": _drop_v11_temp_cols(pre_dedupe),
        "live_like_candidates": live_like,
        "slot_audit": slot_audit,
        "stats": stats,
    }


def _normalise_live_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return pd.DataFrame()
    out = candidates.copy()
    required = {"ticker", "side", "setup", "signal_time_ist"}
    if not required.issubset(out.columns):
        missing = sorted(required - set(out.columns))
        raise SystemExit(f"[v11 live_parity] missing candidate columns: {missing}")
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    out["setup"] = out["setup"].astype(str).str.strip()
    out["signal_time_ist"] = pd.to_datetime(out["signal_time_ist"], errors="coerce")
    if getattr(out["signal_time_ist"].dt, "tz", None) is None:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_localize("Asia/Kolkata")
    else:
        out["signal_time_ist"] = out["signal_time_ist"].dt.tz_convert("Asia/Kolkata")
    out = out.dropna(subset=["ticker", "side", "setup", "signal_time_ist"]).copy()
    out = out.loc[~out["setup"].isin(EXCLUDED_SETUPS)].copy()
    out = out.loc[out["setup"].isin(v6.SETUP_EXIT_RULES)].copy()
    out["signal_time_ist"] = out["signal_time_ist"].map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S%z"))
    out["signal_time_ist"] = out["signal_time_ist"].str.replace(r"(\+\d{2})(\d{2})$", r"\1:\2", regex=True)
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    if "candidate_id" not in out.columns:
        out["candidate_id"] = (
            out["ticker"].astype(str)
            + "|"
            + out["side"].astype(str)
            + "|"
            + out["setup"].astype(str)
            + "|"
            + out["signal_time_ist"].astype(str)
        )
    out = (
        out.sort_values(["signal_time_ist", "quality_score", "ticker", "setup"], ascending=[True, False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )
    return out


def _resolve_live_parity_candidates(candidates: pd.DataFrame, cost_bps: float, label: str) -> pd.DataFrame:
    normalised = _normalise_live_candidates(candidates)
    print(f"[v11 {label}] resolving {len(normalised):,} live-like candidates")
    rows = []
    misses = 0
    t0 = time.time()
    for i, (_, row) in enumerate(normalised.iterrows(), 1):
        rec = _resolve_trade_1m_entry(row, cost_bps=cost_bps)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 250 == 0 or i == len(normalised):
            print(f"  [v11 {label} {i:5d}/{len(normalised)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")
    if not rows:
        detail = _format_1m_miss_diagnostics(normalised)
        raise SystemExit(f"[v11 {label}] no trades resolved; {detail}")
    out = pd.DataFrame(rows)
    out["v7_resolution_source"] = label
    out["v8_resolution_source"] = label
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["_entry_sort"] = pd.to_datetime(out["entry_time_v6"], errors="coerce")
    out["_score_sort"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    out = (
        out.sort_values(["trade_date", "_entry_sort", "_score_sort", "ticker"], ascending=[True, True, False, True])
        .drop_duplicates(subset=["trade_date", "ticker"], keep="first")
        .drop(columns=["_entry_sort", "_score_sort"], errors="ignore")
        .reset_index(drop=True)
    )
    return out


def _load_v7_live_paper_trades(live_paper_dir: Path, start_date: str, end_date: str) -> pd.DataFrame:
    start = pd.to_datetime(start_date, errors="coerce")
    end = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        raise SystemExit("[v11 v7_live_paper_replay] --start_date and --end_date are required as YYYY-MM-DD")
    rows: list[dict] = []
    for path in sorted(live_paper_dir.glob("paper_trades_*_id_5min_v7.csv")):
        name = path.name
        if not name.startswith("paper_trades_") or not name.endswith("_id_5min_v7.csv"):
            continue
        day_text = name.replace("paper_trades_", "").replace("_id_5min_v7.csv", "")
        day = pd.to_datetime(day_text, errors="coerce")
        if pd.isna(day) or day < start or day > end:
            continue
        frame = pd.read_csv(path)
        if frame.empty or "ticker" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            ticker = str(row.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            side = str(row.get("side", "")).strip().upper()
            setup = str(row.get("setup", "")).strip()
            entry_ts = _normalise_ts(row.get("entry_time"))
            signal_ts = _normalise_ts(
                row.get("signal_datetime", row.get("signal_entry_datetime_ist", pd.NaT))
            )
            if pd.isna(entry_ts) or side not in {"LONG", "SHORT"} or not setup:
                continue
            entry_price = _safe_float(row.get("entry_price"))
            stop_price = _safe_float(row.get("stop_price"))
            target_price = _safe_float(row.get("target_price"))
            quantity = _safe_int(row.get("quantity"), 0)
            if entry_price <= 0 or stop_price <= 0 or target_price <= 0 or quantity <= 0:
                continue
            if side == "LONG":
                sl_pct = (entry_price - stop_price) / entry_price * 100.0
                target_pct = (target_price - entry_price) / entry_price * 100.0
            else:
                sl_pct = (stop_price - entry_price) / entry_price * 100.0
                target_pct = (entry_price - target_price) / entry_price * 100.0
            rows.append({
                "candidate_id": (
                    f"{ticker}|{side}|{setup}|"
                    f"{signal_ts if pd.notna(signal_ts) else entry_ts}|{row.get('signal_id', '')}"
                ),
                "scan_session": "v7_live_paper_replay",
                "selection_mode": "actual_v7_live_paper_trade",
                "candidate_family": "V7_LIVE_PAPER_EXECUTED",
                "signal_time_ist": signal_ts,
                "signal_time_v8": signal_ts,
                "signal_datetime": row.get("signal_datetime", ""),
                "signal_entry_datetime_ist": row.get("signal_entry_datetime_ist", row.get("signal_datetime", "")),
                "old_entry_time_v7": entry_ts,
                "old_entry_price_v7": entry_price,
                "entry_time_v6": entry_ts,
                "entry_price_v6": entry_price,
                "trade_date": day.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "side": side,
                "setup": setup,
                "quantity": quantity,
                "live_trade_id": row.get("trade_id", ""),
                "live_signal_id": row.get("signal_id", ""),
                "live_exit_time": _normalise_ts(row.get("exit_time")),
                "live_exit_price": _safe_float(row.get("exit_price")),
                "live_outcome": str(row.get("outcome", "")).strip().upper(),
                "live_pnl_rs": _safe_float(row.get("pnl_rs")),
                "quality_score": _safe_float(row.get("quality_score"), 0.0),
                "v7_live_stop_price": stop_price,
                "v7_live_target_price": target_price,
                "v7_live_sl_pct": sl_pct,
                "v7_live_target_pct": target_pct,
                "v8_entry_model": "ACTUAL_V7_LIVE_PAPER_ENTRY",
                "v8_entry_delay_minutes": (
                    float((entry_ts - signal_ts).total_seconds() / 60.0) if pd.notna(signal_ts) else np.nan
                ),
            })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    return out.sort_values(["trade_date", "entry_time_v6", "ticker"]).reset_index(drop=True)


def _resolve_v7_live_paper_trade(row: pd.Series) -> dict | None:
    bars = _load_1m_with_open(str(row["ticker"]))
    if bars is None or bars.empty:
        return None
    sl_pct = float(row["v7_live_sl_pct"])
    target_pct = float(row["v7_live_target_pct"])
    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=float(row["entry_price_v6"]),
        entry_time_ist=row["entry_time_v6"],
        sl_pct=sl_pct,
        tgt_pct=target_pct,
    )
    if res is None:
        return None

    import nse_intraday_costs as _nse

    quantity = int(row["quantity"])
    entry_price = float(row["entry_price_v6"])
    exit_price = float(res.exit_price)
    side = str(row["side"]).upper()
    gross_pnl_rs = _price_pnl_rs(side, entry_price, exit_price, quantity)
    if _V11_COST_MODEL == "statutory":
        cost_breakdown = _nse.intraday_equity_costs(
            entry_price,
            exit_price,
            quantity,
            side,
        )
        cost_rs = float(cost_breakdown.total_cost)
        net_pnl_rs = float(cost_breakdown.net_pnl)
    else:
        cost_rs = 0.0
        net_pnl_rs = float(gross_pnl_rs)

    rec = row.to_dict()
    rec.update({
        "v6_sl_pct": float(sl_pct),
        "v6_target_pct": float(target_pct),
        "v6_outcome": res.outcome,
        "v6_exit_price": exit_price,
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(gross_pnl_rs),
        "v6_cost_rs": float(cost_rs),
        "v6_net_pnl_rs": float(net_pnl_rs),
        "v6_net_pnl_pct": float(net_pnl_rs / (entry_price * quantity) * 100.0),
        "capital_per_trade_rs": float(entry_price * quantity / V7_INTRADAY_LEVERAGE),
        "leverage": V7_INTRADAY_LEVERAGE,
        "notional_exposure_rs": float(entry_price * quantity),
        "v7_resolution_source": "v7_live_paper_replay",
        "v8_resolution_source": "v7_live_paper_replay",
        "v11_cost_model": _V11_COST_MODEL,
    })
    return rec


def _run_v7_live_paper_replay(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    live_paper_dir = Path(args.live_paper_dir)
    start_date = str(args.start_date or args.live_date or "").strip()
    end_date = str(args.end_date or args.live_date or start_date).strip()
    if not start_date or not end_date:
        raise SystemExit("[v11 v7_live_paper_replay] provide --start_date and --end_date")

    source_trades = _load_v7_live_paper_trades(live_paper_dir, start_date, end_date)
    source_trades.to_csv(out_dir / "v7_live_paper_replay_source_trades.csv", index=False)
    accepted = pd.DataFrame([{
        "stage": "v7_live_paper_replay",
        "source": str(live_paper_dir),
        "start_date": start_date,
        "end_date": end_date,
        "source_trades": int(len(source_trades)),
    }])

    if source_trades.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(live_paper_dir),
            accepted_filename="v7_live_paper_replay_rules_used.csv",
            reason="[v11 v7_live_paper_replay] no v7 live paper trades found for requested range",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=v7_live_paper_replay\nlive_paper_dir={live_paper_dir}\n"
            f"start_date={start_date}\nend_date={end_date}\n",
            encoding="utf-8",
        )
        return 0

    rows = []
    misses = 0
    print(f"[v11 v7_live_paper_replay] resolving {len(source_trades):,} actual v7 live paper trades")
    for i, (_, row) in enumerate(source_trades.iterrows(), 1):
        rec = _resolve_v7_live_paper_trade(row)
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 100 == 0 or i == len(source_trades):
            print(f"  [v11 v7_live_paper_replay {i:4d}/{len(source_trades)}] resolved={len(rows)} misses={misses}")
    if not rows:
        raise SystemExit("[v11 v7_live_paper_replay] no trades resolved from 1-minute data")

    final = pd.DataFrame(rows).sort_values(["trade_date", "entry_time_v6", "ticker"]).reset_index(drop=True)
    accepted.loc[0, "resolved_trades"] = int(len(final))
    accepted.loc[0, "misses"] = int(misses)
    _write_outputs(
        out_dir,
        final,
        accepted,
        str(live_paper_dir),
        accepted_filename="v7_live_paper_replay_rules_used.csv",
        strategy_description="actual v7 live paper trades re-resolved with 1-minute exits",
    )
    (out_dir / "inputs.txt").write_text(
        f"mode=v7_live_paper_replay\nlive_paper_dir={live_paper_dir}\n"
        f"start_date={start_date}\nend_date={end_date}\n"
        f"data_1m_dir={v6.DATA_1M_DIR}\n"
        "entry_model=ACTUAL_V7_LIVE_PAPER_ENTRY\n"
        "exit_model=v7 live stop/target from paper trade rows, resolved on 1-minute OHLC to 15:20\n"
        f"pnl_model={_pnl_model_description(actual_live_quantity=True)}\n",
        encoding="utf-8",
    )
    print(f"[v11 v7_live_paper_replay] wrote {out_dir}")
    return 0


def _format_1m_miss_diagnostics(candidates: pd.DataFrame) -> str:
    if candidates is None or candidates.empty:
        return "no normalised candidates remained after v11 live-strategy filtering"

    signal_times = pd.to_datetime(candidates.get("signal_time_ist"), errors="coerce")
    if getattr(signal_times.dt, "tz", None) is None:
        signal_times = signal_times.dt.tz_localize("Asia/Kolkata")
    else:
        signal_times = signal_times.dt.tz_convert("Asia/Kolkata")
    valid_signals = signal_times.dropna()
    min_signal = valid_signals.min() if not valid_signals.empty else pd.NaT
    max_signal = valid_signals.max() if not valid_signals.empty else pd.NaT

    missing_files = 0
    stale_before_signal = 0
    latest_seen = pd.NaT
    examples: list[str] = []
    for _, row in candidates.iterrows():
        ticker = str(row.get("ticker", "")).upper().strip()
        sig = _normalise_ts(row.get("signal_time_ist"))
        bars = _load_1m_with_open(ticker)
        if bars is None or bars.empty:
            missing_files += 1
            if len(examples) < 3:
                examples.append(f"{ticker}: no 1m file")
            continue
        latest = pd.Timestamp(bars.index.max())
        latest_seen = latest if pd.isna(latest_seen) else max(latest_seen, latest)
        if pd.notna(sig) and latest < sig:
            stale_before_signal += 1
            if len(examples) < 3:
                examples.append(f"{ticker}: latest 1m {_fmt_ist(latest)} before signal {_fmt_ist(sig)}")

    parts = [
        f"candidates={len(candidates)}",
        f"missing_1m_files={missing_files}",
        f"stale_before_signal={stale_before_signal}",
    ]
    if pd.notna(min_signal) and pd.notna(max_signal):
        parts.append(f"signal_window={_fmt_ist(min_signal)}..{_fmt_ist(max_signal)}")
    if pd.notna(latest_seen):
        parts.append(f"latest_1m_seen={_fmt_ist(latest_seen)}")
    parts.append(f"data_1m_dir={v6.DATA_1M_DIR}")
    if examples:
        parts.append("examples=[" + "; ".join(examples) + "]")
    return ", ".join(parts)


def _run_live_parity(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.live_candidate_json_dir)
    raw_candidates = _load_live_candidate_snapshots(source_dir, str(args.live_date or ""))
    if raw_candidates.empty:
        raise SystemExit(f"[v11 live_parity] no live candidate snapshots found in {source_dir} for date={args.live_date!r}")

    pipeline = _apply_v7_live_strategy(
        raw_candidates,
        str(args.live_date or ""),
        ab_gate_profile=str(args.ab_gate_profile),
        ab_gate_min_quality=float(args.ab_gate_min_quality),
        ab_gate_max_per_side=int(args.ab_gate_max_per_side),
        ab_gate_max_per_slot=int(args.ab_gate_max_per_slot),
        ab_gate_setups=_ab_gate_setups_for_selected_profile(str(args.selected_strategy_profile)),
        selected_strategy_profile=str(args.selected_strategy_profile),
    )
    live_like_candidates = pipeline["live_like_candidates"]
    (
        selected_strategy_signals,
        entry_engine_raw,
        entry_engine_rejects,
        entry_engine_signals,
        selected_strategy_rejects,
        selected_strategy_stats,
    ) = _build_v7_entry_engine_signals_for_profile(
        pipeline["pre_dedupe_live_candidates"],
        str(args.selected_strategy_profile),
    )
    raw_candidates.to_csv(out_dir / "live_parity_raw_candidates.csv", index=False)
    pipeline["ranked_raw_candidates"].to_csv(out_dir / "live_parity_ranked_raw_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "live_parity_v8_gated_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "live_parity_gated_candidates.csv", index=False)
    pipeline["research_rejected_candidates"].to_csv(out_dir / "live_parity_research_rejected_candidates.csv", index=False)
    pipeline["pre_dedupe_live_candidates"].to_csv(out_dir / "live_parity_pre_dedupe_live_candidates.csv", index=False)
    live_like_candidates.to_csv(out_dir / "live_parity_live_like_candidates.csv", index=False)
    entry_engine_raw.to_csv(out_dir / "live_parity_entry_engine_raw_entries.csv", index=False)
    entry_engine_rejects.to_csv(out_dir / "live_parity_entry_engine_rejects.csv", index=False)
    entry_engine_signals.to_csv(out_dir / "live_parity_entry_engine_signals.csv", index=False)
    selected_strategy_signals.to_csv(out_dir / "live_parity_selected_strategy_signals.csv", index=False)
    selected_strategy_rejects.to_csv(out_dir / "live_parity_selected_strategy_rejects.csv", index=False)
    pipeline["slot_audit"].to_csv(out_dir / "live_parity_live_pipeline_slot_audit.csv", index=False)
    live_parity_stats = {
        **pipeline["stats"],
        "entry_engine_raw_entries": int(len(entry_engine_raw)),
        "entry_engine_rejects": int(len(entry_engine_rejects)),
        "entry_engine_signals": int(len(entry_engine_signals)),
        **selected_strategy_stats,
    }
    pd.DataFrame([live_parity_stats]).to_csv(out_dir / "live_parity_pipeline_stats.csv", index=False)

    accepted = pd.DataFrame([{
        "stage": "live_parity",
        "source": str(source_dir),
        "date": str(args.live_date or ""),
        **live_parity_stats,
    }])
    if selected_strategy_signals.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(source_dir),
            accepted_filename="live_parity_rules_used.csv",
            reason="[v11 live_parity] no v7 entry-engine signals survived the selected strategy profile",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=live_parity\nlive_candidate_json_dir={source_dir}\nlive_date={args.live_date}\n"
            f"ab_gate_profile={args.ab_gate_profile}\n"
            f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
            f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
            f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
            f"entry_fill_model={args.entry_fill_model}\n"
            f"selected_strategy_profile={args.selected_strategy_profile}\n"
            "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
            "candidate_source=signal_discovery_v7_5mins_ID snapshots\n"
            "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
            "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
            "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
            f"pnl_model={_pnl_model_description()}\n",
            encoding="utf-8",
        )
        print(f"[v11 live_parity] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = _resolve_v7_entry_engine_signals(
        selected_strategy_signals,
        label="live_parity",
        entry_fill_model=str(args.entry_fill_model),
        selected_strategy_profile=str(args.selected_strategy_profile),
    )
    _write_outputs(out_dir, final, accepted, str(source_dir), accepted_filename="live_parity_rules_used.csv")
    (out_dir / "inputs.txt").write_text(
        f"mode=live_parity\nlive_candidate_json_dir={source_dir}\nlive_date={args.live_date}\n"
        f"ab_gate_profile={args.ab_gate_profile}\n"
        f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
        f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
        f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
        f"entry_fill_model={args.entry_fill_model}\n"
        f"selected_strategy_profile={args.selected_strategy_profile}\n"
        "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
        "candidate_source=signal_discovery_v7_5mins_ID snapshots\n"
        "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
        "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
        "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
        f"pnl_model={_pnl_model_description()}\n",
        encoding="utf-8",
    )
    print(f"[v11 live_parity] wrote {out_dir}")
    return 0


def _run_historical_full_day(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    historical_date = str(args.historical_date or args.live_date or "").strip()
    snapshot_dir = out_dir / "generated_candidate_snapshots"
    primary_root = Path(args.candidate_5m_dir)
    fallback_root = Path(args.fallback_candidate_5m_dir) if str(args.fallback_candidate_5m_dir or "").strip() else None
    _, one_day_roots = _available_historical_dates_with_roots(
        primary_root,
        fallback_root,
        historical_date,
        historical_date,
    )
    data_root = one_day_roots.get(historical_date, primary_root)

    raw_candidates, slot_summary = _scan_historical_full_day_candidates(
        day=historical_date,
        start_time=str(args.start_time),
        end_time=str(args.end_time),
        workers=int(args.workers),
        snapshot_dir=snapshot_dir,
        data_root=data_root,
        include_ab_excluded=_ab_gate_enabled(str(args.ab_gate_profile)),
    )
    slot_summary.to_csv(out_dir / "historical_full_day_slot_summary.csv", index=False)
    raw_candidates.to_csv(out_dir / "historical_full_day_raw_candidates.csv", index=False)

    pipeline = _apply_v7_live_strategy(
        raw_candidates,
        historical_date,
        ab_gate_profile=str(args.ab_gate_profile),
        ab_gate_min_quality=float(args.ab_gate_min_quality),
        ab_gate_max_per_side=int(args.ab_gate_max_per_side),
        ab_gate_max_per_slot=int(args.ab_gate_max_per_slot),
        ab_gate_setups=_ab_gate_setups_for_selected_profile(str(args.selected_strategy_profile)),
        selected_strategy_profile=str(args.selected_strategy_profile),
    )
    live_like_candidates = pipeline["live_like_candidates"]
    (
        selected_strategy_signals,
        entry_engine_raw,
        entry_engine_rejects,
        entry_engine_signals,
        selected_strategy_rejects,
        selected_strategy_stats,
    ) = _build_v7_entry_engine_signals_for_profile(
        pipeline["pre_dedupe_live_candidates"],
        str(args.selected_strategy_profile),
    )
    pipeline["ranked_raw_candidates"].to_csv(out_dir / "historical_full_day_ranked_raw_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "historical_full_day_v8_gated_candidates.csv", index=False)
    pipeline["v8_gated_candidates"].to_csv(out_dir / "historical_full_day_gated_candidates.csv", index=False)
    pipeline["research_rejected_candidates"].to_csv(out_dir / "historical_full_day_research_rejected_candidates.csv", index=False)
    pipeline["pre_dedupe_live_candidates"].to_csv(out_dir / "historical_full_day_pre_dedupe_live_candidates.csv", index=False)
    live_like_candidates.to_csv(out_dir / "historical_full_day_live_like_candidates.csv", index=False)
    entry_engine_raw.to_csv(out_dir / "historical_full_day_entry_engine_raw_entries.csv", index=False)
    entry_engine_rejects.to_csv(out_dir / "historical_full_day_entry_engine_rejects.csv", index=False)
    entry_engine_signals.to_csv(out_dir / "historical_full_day_entry_engine_signals.csv", index=False)
    selected_strategy_signals.to_csv(out_dir / "historical_full_day_selected_strategy_signals.csv", index=False)
    selected_strategy_rejects.to_csv(out_dir / "historical_full_day_selected_strategy_rejects.csv", index=False)
    pipeline["slot_audit"].to_csv(out_dir / "historical_full_day_live_pipeline_slot_audit.csv", index=False)
    if getattr(args, "parity_debug", False):
        _write_parity_debug_csv(out_dir, pipeline, entry_engine_rejects, label="historical_full_day")
    full_day_stats = {
        **pipeline["stats"],
        "entry_engine_raw_entries": int(len(entry_engine_raw)),
        "entry_engine_rejects": int(len(entry_engine_rejects)),
        "entry_engine_signals": int(len(entry_engine_signals)),
        **selected_strategy_stats,
    }
    pd.DataFrame([full_day_stats]).to_csv(out_dir / "historical_full_day_pipeline_stats.csv", index=False)

    if not live_like_candidates.empty and "signal_time_ist" in live_like_candidates.columns:
        slot_keys = pd.to_datetime(live_like_candidates["signal_time_ist"], errors="coerce").dt.strftime("%H%M")
        for slot_key, group in live_like_candidates.groupby(slot_keys):
            if not str(slot_key) or str(slot_key) == "NaT":
                continue
            slot = _normalise_ts(group["signal_time_ist"].iloc[0])
            _write_candidate_snapshot(
                group,
                slot,
                snapshot_dir / f"gated_candidate_tickers_{''.join(historical_date.split('-'))}_{slot_key}.json",
                output_label="live_like_for_entry_engine",
                payload_extra={"candidate_source": "historical_full_day_raw_candidates", **pipeline["stats"]},
            )

    accepted = pd.DataFrame([{
        "stage": "historical_full_day",
        "source": "candidate_scan.scan_slot_candidates",
        "date": historical_date,
        "start_time": str(args.start_time),
        "end_time": str(args.end_time),
        "slots_scanned": int(len(slot_summary)),
        "raw_candidates": int(len(raw_candidates)),
        **full_day_stats,
    }])
    tier123_enabled = _tier123_profile_enabled(str(args.selected_strategy_profile))
    if selected_strategy_signals.empty and not tier123_enabled:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(out_dir / "historical_full_day_raw_candidates.csv"),
            accepted_filename="historical_full_day_rules_used.csv",
            reason="[v11 historical_full_day] no v7 entry-engine signals survived the selected strategy profile",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=historical_full_day\nhistorical_date={historical_date}\n"
            f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
            f"candidate_snapshot_dir={snapshot_dir}\n"
            f"candidate_5m_dir={data_root}\n"
            f"entry_fill_model={args.entry_fill_model}\n"
            f"selected_strategy_profile={args.selected_strategy_profile}\n"
            f"ab_gate_profile={args.ab_gate_profile}\n"
            f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
            f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
            f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
            "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
            "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
            "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
            "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
            "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
            f"pnl_model={_pnl_model_description()}\n",
            encoding="utf-8",
        )
        print(f"[v11 historical_full_day] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = pd.DataFrame()
    if not selected_strategy_signals.empty:
        final = _resolve_v7_entry_engine_signals(
            selected_strategy_signals,
            label="historical_full_day",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )
    final, tier123_stats = _apply_tier123_balanced_addon(
        base_final=final,
        dates=[historical_date],
        date_to_root={historical_date: data_root},
        args=args,
        out_dir=out_dir,
        label="historical_full_day",
    )
    tier123_stats.to_csv(out_dir / "historical_full_day_tier123_balanced_addon_stats.csv", index=False)
    accepted_out = _concat_frames([accepted, tier123_stats])
    if final.empty:
        _write_empty_outputs(
            out_dir,
            accepted_out,
            str(out_dir / "historical_full_day_raw_candidates.csv"),
            accepted_filename="historical_full_day_rules_used.csv",
            reason="[v11 historical_full_day] no base or Tier123 balanced add-on trades resolved",
        )
        (out_dir / "inputs.txt").write_text(
            f"mode=historical_full_day\nhistorical_date={historical_date}\n"
            f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
            f"candidate_snapshot_dir={snapshot_dir}\n"
            f"candidate_5m_dir={data_root}\n"
            f"entry_fill_model={args.entry_fill_model}\n"
            f"selected_strategy_profile={args.selected_strategy_profile}\n"
            f"ab_gate_profile={args.ab_gate_profile}\n"
            f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
            f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
            f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
            f"tier123_balanced_addon_enabled={tier123_enabled}\n",
            encoding="utf-8",
        )
        print(f"[v11 historical_full_day] wrote empty no-trade outputs to {out_dir}")
        return 0
    _write_outputs(
        out_dir,
        final,
        accepted_out,
        str(out_dir / "historical_full_day_raw_candidates.csv"),
        accepted_filename="historical_full_day_rules_used.csv",
        strategy_description=(
            "current v7 live scanner + v7 1-minute entry engine + PAPER_TRUE-style "
            "historical fill + selected honest production profile + optional Tier123 balanced non-overlap add-on "
            "+ setup-specific 1-minute exits"
        ),
    )
    (out_dir / "inputs.txt").write_text(
        f"mode=historical_full_day\nhistorical_date={historical_date}\n"
        f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
        f"candidate_snapshot_dir={snapshot_dir}\n"
        f"candidate_5m_dir={data_root}\n"
        f"entry_fill_model={args.entry_fill_model}\n"
        f"selected_strategy_profile={args.selected_strategy_profile}\n"
        f"ab_gate_profile={args.ab_gate_profile}\n"
        f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
        f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
        f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
        f"tier123_balanced_addon_enabled={tier123_enabled}\n"
        "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
        "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
        "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
        "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
        "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
        f"pnl_model={_pnl_model_description()}\n",
        encoding="utf-8",
    )
    print(f"[v11 historical_full_day] wrote {out_dir}")
    return 0


def _run_historical_all_available(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    data_root = Path(args.candidate_5m_dir)
    fallback_root = Path(args.fallback_candidate_5m_dir) if str(args.fallback_candidate_5m_dir or "").strip() else None
    dates, date_to_root = _available_historical_dates_with_roots(
        data_root,
        fallback_root,
        str(args.start_date or ""),
        str(args.end_date or ""),
    )
    if not dates:
        raise SystemExit("[v11 all_available] no available historical dates matched the requested range")

    day_root = out_dir / "historical_all_available_days"
    raw_frames: list[pd.DataFrame] = []
    slot_summary_frames: list[pd.DataFrame] = []
    ranked_frames: list[pd.DataFrame] = []
    gated_frames: list[pd.DataFrame] = []
    rejected_frames: list[pd.DataFrame] = []
    pre_dedupe_frames: list[pd.DataFrame] = []
    live_like_frames: list[pd.DataFrame] = []
    entry_signal_frames: list[pd.DataFrame] = []
    selected_signal_frames: list[pd.DataFrame] = []
    selected_reject_frames: list[pd.DataFrame] = []
    entry_raw_frames: list[pd.DataFrame] = []
    entry_reject_frames: list[pd.DataFrame] = []
    slot_audit_frames: list[pd.DataFrame] = []
    accepted_rows: list[dict] = []

    print(
        f"[v11 all_available] running {len(dates)} dates from {dates[0]} to {dates[-1]} "
        f"with workers={int(args.workers)} primary_data_root={data_root} fallback_data_root={fallback_root or ''}",
        flush=True,
    )
    started = time.time()
    for i, day in enumerate(dates, 1):
        day_dir = day_root / day
        day_dir.mkdir(parents=True, exist_ok=True)
        snapshot_dir = day_dir / "generated_candidate_snapshots"
        day_data_root = date_to_root.get(day, data_root)
        print(f"[v11 all_available {i:02d}/{len(dates):02d}] date={day} data_root={day_data_root}", flush=True)

        raw_candidates, slot_summary = _scan_historical_full_day_candidates(
            day=day,
            start_time=str(args.start_time),
            end_time=str(args.end_time),
            workers=int(args.workers),
            snapshot_dir=snapshot_dir,
            data_root=day_data_root,
            include_ab_excluded=_ab_gate_enabled(str(args.ab_gate_profile)),
        )
        if str(args.selected_strategy_profile) == FINAL_CONF_PROFILE:
            raw_candidates = _merge_external_conf_candidates(raw_candidates, day)
        if not raw_candidates.empty:
            raw_candidates = raw_candidates.copy()
            raw_candidates["v11_source_day"] = day
        if not slot_summary.empty:
            slot_summary = slot_summary.copy()
            slot_summary.insert(0, "date", day)

        raw_candidates.to_csv(day_dir / "raw_candidates.csv", index=False)
        slot_summary.to_csv(day_dir / "slot_summary.csv", index=False)
        raw_frames.append(raw_candidates)
        slot_summary_frames.append(slot_summary)

        pipeline = _apply_v7_live_strategy(
            raw_candidates,
            day,
            ab_gate_profile=str(args.ab_gate_profile),
            ab_gate_min_quality=float(args.ab_gate_min_quality),
            ab_gate_max_per_side=int(args.ab_gate_max_per_side),
            ab_gate_max_per_slot=int(args.ab_gate_max_per_slot),
            ab_gate_setups=_ab_gate_setups_for_selected_profile(str(args.selected_strategy_profile)),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )
        (
            selected_strategy_signals,
            entry_engine_raw,
            entry_engine_rejects,
            entry_engine_signals,
            selected_strategy_rejects,
            selected_strategy_stats,
        ) = _build_v7_entry_engine_signals_for_profile(
            pipeline["pre_dedupe_live_candidates"],
            str(args.selected_strategy_profile),
        )
        for key, filename, bucket in [
            ("ranked_raw_candidates", "ranked_raw_candidates.csv", ranked_frames),
            ("v8_gated_candidates", "v8_gated_candidates.csv", gated_frames),
            ("research_rejected_candidates", "research_rejected_candidates.csv", rejected_frames),
            ("pre_dedupe_live_candidates", "pre_dedupe_live_candidates.csv", pre_dedupe_frames),
            ("live_like_candidates", "live_like_candidates.csv", live_like_frames),
            ("slot_audit", "live_pipeline_slot_audit.csv", slot_audit_frames),
        ]:
            frame = pipeline[key]
            if frame is not None and not frame.empty:
                frame = frame.copy()
                if "v11_source_day" not in frame.columns:
                    frame["v11_source_day"] = day
            frame.to_csv(day_dir / filename, index=False)
            bucket.append(frame)

        for frame, filename, bucket in [
            (entry_engine_raw, "entry_engine_raw_entries.csv", entry_raw_frames),
            (entry_engine_rejects, "entry_engine_rejects.csv", entry_reject_frames),
            (entry_engine_signals, "entry_engine_signals.csv", entry_signal_frames),
            (selected_strategy_signals, "selected_strategy_signals.csv", selected_signal_frames),
            (selected_strategy_rejects, "selected_strategy_rejects.csv", selected_reject_frames),
        ]:
            if frame is not None and not frame.empty:
                frame = frame.copy()
                frame["v11_source_day"] = day
            frame.to_csv(day_dir / filename, index=False)
            bucket.append(frame)

        stats = {
            "stage": "historical_all_available_day",
            "date": day,
            "candidate_5m_dir": str(day_data_root),
            "start_time": str(args.start_time),
            "end_time": str(args.end_time),
            "slots_scanned": int(len(slot_summary)),
            **pipeline["stats"],
            "entry_engine_raw_entries": int(len(entry_engine_raw)),
            "entry_engine_rejects": int(len(entry_engine_rejects)),
            "entry_engine_signals": int(len(entry_engine_signals)),
            **selected_strategy_stats,
        }
        accepted_rows.append(stats)
        pd.DataFrame([stats]).to_csv(day_dir / "pipeline_stats.csv", index=False)
        print(
            f"[v11 all_available {i:02d}/{len(dates):02d}] "
            f"raw={stats['raw_candidates']} live_like={stats['live_like_candidates']} "
            f"entry_signals={stats['entry_engine_signals']} selected={stats['selected_strategy_signals']} "
            f"dupes={stats['live_like_duplicates']} elapsed_total={time.time() - started:.1f}s",
            flush=True,
        )

    raw_all = _concat_frames(raw_frames)
    slot_summary_all = _concat_frames(slot_summary_frames)
    ranked_all = _concat_frames(ranked_frames)
    gated_all = _concat_frames(gated_frames)
    rejected_all = _concat_frames(rejected_frames)
    pre_dedupe_all = _concat_frames(pre_dedupe_frames)
    live_like_all = _concat_frames(live_like_frames)
    entry_raw_all = _concat_frames(entry_raw_frames)
    entry_reject_all = _concat_frames(entry_reject_frames)
    entry_signals_all = _concat_frames(entry_signal_frames)
    selected_signals_all = _concat_frames(selected_signal_frames)
    selected_rejects_all = _concat_frames(selected_reject_frames)
    slot_audit_all = _concat_frames(slot_audit_frames)
    accepted = pd.DataFrame(accepted_rows)

    raw_all.to_csv(out_dir / "historical_all_available_raw_candidates.csv", index=False)
    slot_summary_all.to_csv(out_dir / "historical_all_available_slot_summary.csv", index=False)
    ranked_all.to_csv(out_dir / "historical_all_available_ranked_raw_candidates.csv", index=False)
    gated_all.to_csv(out_dir / "historical_all_available_v8_gated_candidates.csv", index=False)
    gated_all.to_csv(out_dir / "historical_all_available_gated_candidates.csv", index=False)
    rejected_all.to_csv(out_dir / "historical_all_available_research_rejected_candidates.csv", index=False)
    pre_dedupe_all.to_csv(out_dir / "historical_all_available_pre_dedupe_live_candidates.csv", index=False)
    live_like_all.to_csv(out_dir / "historical_all_available_live_like_candidates.csv", index=False)
    entry_raw_all.to_csv(out_dir / "historical_all_available_entry_engine_raw_entries.csv", index=False)
    entry_reject_all.to_csv(out_dir / "historical_all_available_entry_engine_rejects.csv", index=False)
    entry_signals_all.to_csv(out_dir / "historical_all_available_entry_engine_signals.csv", index=False)
    selected_signals_all.to_csv(out_dir / "historical_all_available_selected_strategy_signals.csv", index=False)
    selected_rejects_all.to_csv(out_dir / "historical_all_available_selected_strategy_rejects.csv", index=False)
    slot_audit_all.to_csv(out_dir / "historical_all_available_live_pipeline_slot_audit.csv", index=False)
    accepted.to_csv(out_dir / "historical_all_available_pipeline_stats_by_day.csv", index=False)
    pd.DataFrame({"date": dates}).to_csv(out_dir / "historical_all_available_dates.csv", index=False)

    inputs_text = (
        "mode=historical_all_available\n"
        f"dates={len(dates)}\nfirst_date={dates[0]}\nlast_date={dates[-1]}\n"
        f"start_time={args.start_time}\nend_time={args.end_time}\nworkers={args.workers}\n"
        f"candidate_5m_dir={data_root}\n"
        f"fallback_candidate_5m_dir={fallback_root or ''}\n"
        "candidate_5m_dir_selection=primary root when date exists there, else fallback root\n"
        f"data_1m_dir={v6.DATA_1M_DIR}\n"
        f"entry_fill_model={args.entry_fill_model}\n"
        f"selected_strategy_profile={args.selected_strategy_profile}\n"
        f"ab_gate_profile={args.ab_gate_profile}\n"
        f"ab_gate_min_quality={args.ab_gate_min_quality}\n"
        f"ab_gate_max_per_side={args.ab_gate_max_per_side}\n"
        f"ab_gate_max_per_slot={args.ab_gate_max_per_slot}\n"
        "entry_model=v7 1-minute entry engine, next 1-minute open after 5-minute signal\n"
        "candidate_source=candidate_scan.scan_slot_candidates regenerated from historical 5-minute bars\n"
        "live_strategy_pipeline=add_live_ranker_scores -> apply_v8_live_gate -> apply_research_live_filters\n"
        "selected_strategy=post-v7-entry honest production profile filters; pass --selected_strategy_profile none for raw v11\n"
        "entry_engine_dedupe=best setup per ticker/slot, then one ticker per day matching v7 entry-engine guard\n"
        f"pnl_model={_pnl_model_description()}\n"
    )

    tier123_enabled = _tier123_profile_enabled(str(args.selected_strategy_profile))
    if selected_signals_all.empty and not tier123_enabled:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(out_dir / "historical_all_available_raw_candidates.csv"),
            accepted_filename="historical_all_available_rules_used.csv",
            reason="[v11 all_available] no v7 entry-engine signals survived the selected strategy profile",
        )
        (out_dir / "inputs.txt").write_text(inputs_text, encoding="utf-8")
        print(f"[v11 all_available] wrote empty no-trade outputs to {out_dir}")
        return 0

    final = pd.DataFrame()
    if not selected_signals_all.empty:
        final = _resolve_v7_entry_engine_signals(
            selected_signals_all,
            label="historical_all_available",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )
    final, tier123_stats = _apply_tier123_balanced_addon(
        base_final=final,
        dates=dates,
        date_to_root=date_to_root,
        args=args,
        out_dir=out_dir,
        label="historical_all_available",
    )
    tier123_stats.to_csv(out_dir / "historical_all_available_tier123_balanced_addon_stats.csv", index=False)
    accepted_out = _concat_frames([accepted, tier123_stats])
    if final.empty:
        _write_empty_outputs(
            out_dir,
            accepted_out,
            str(out_dir / "historical_all_available_raw_candidates.csv"),
            accepted_filename="historical_all_available_rules_used.csv",
            reason="[v11 all_available] no base or Tier123 balanced add-on trades resolved",
        )
        (out_dir / "inputs.txt").write_text(inputs_text + f"tier123_balanced_addon_enabled={tier123_enabled}\n", encoding="utf-8")
        print(f"[v11 all_available] wrote empty no-trade outputs to {out_dir}")
        return 0
    _write_outputs(
        out_dir,
        final,
        accepted_out,
        str(out_dir / "historical_all_available_raw_candidates.csv"),
        accepted_filename="historical_all_available_rules_used.csv",
        strategy_description=(
            "current v7 live scanner + v7 1-minute entry engine + PAPER_TRUE-style "
            "historical fill + selected honest production profile + optional Tier123 balanced non-overlap add-on "
            "+ setup-specific 1-minute exits"
        ),
    )
    (out_dir / "inputs.txt").write_text(inputs_text + f"tier123_balanced_addon_enabled={tier123_enabled}\n", encoding="utf-8")
    print(f"[v11 all_available] wrote {out_dir}")
    return 0


def _cached_entry_signal_path(source_dir: Path) -> Path:
    candidates = [
        source_dir / "all_setups_entry_engine_signals.csv",
        source_dir / "historical_all_available_entry_engine_signals.csv",
        source_dir / "entry_engine_signals.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise SystemExit(
        f"[v11 cached_all_setups] no cached entry-engine signals found under {source_dir}; "
        "expected all_setups_entry_engine_signals.csv or historical_all_available_entry_engine_signals.csv"
    )


def _dates_for_cached_replay(args: argparse.Namespace, cached_signals: pd.DataFrame) -> tuple[list[str], dict[str, Path]]:
    primary_root = Path(args.candidate_5m_dir)
    fallback_root = Path(args.fallback_candidate_5m_dir) if str(args.fallback_candidate_5m_dir or "").strip() else None
    if str(args.start_date or "").strip() or str(args.end_date or "").strip():
        dates, date_to_root = _available_historical_dates_with_roots(
            primary_root,
            fallback_root,
            str(args.start_date or ""),
            str(args.end_date or ""),
        )
        if dates:
            return dates, date_to_root

    signal_col = "signal_time_ist" if "signal_time_ist" in cached_signals.columns else "bar_time_ist"
    ts = pd.to_datetime(cached_signals.get(signal_col), errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("Asia/Kolkata")
    else:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    dates = sorted(ts.dropna().dt.strftime("%Y-%m-%d").unique().tolist())
    return dates, {day: primary_root for day in dates}


def _run_historical_cached_all_setups(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.cached_all_setups_dir)
    signal_path = _cached_entry_signal_path(source_dir)
    print(f"[v11 cached_all_setups] loading cached entry-engine signals from {signal_path}", flush=True)
    cached_signals = pd.read_csv(signal_path)
    dates, date_to_root = _dates_for_cached_replay(args, cached_signals)
    if not dates:
        raise SystemExit("[v11 cached_all_setups] no dates available for cached replay")
    print(
        f"[v11 cached_all_setups] dates={len(dates):,} first={dates[0]} last={dates[-1]} "
        f"profile={args.selected_strategy_profile}",
        flush=True,
    )

    selected_signals, selected_rejects, selected_stats = _apply_selected_strategy_profile(
        cached_signals,
        str(args.selected_strategy_profile),
    )
    selected_signals.to_csv(out_dir / "cached_all_setups_selected_strategy_signals.csv", index=False)
    selected_rejects.to_csv(out_dir / "cached_all_setups_selected_strategy_rejects.csv", index=False)
    pd.DataFrame([{
        "stage": "historical_cached_all_setups",
        "source_entry_signal_path": str(signal_path),
        "cached_entry_signals": int(len(cached_signals)),
        **selected_stats,
    }]).to_csv(out_dir / "cached_all_setups_pipeline_stats.csv", index=False)

    final = pd.DataFrame()
    if not selected_signals.empty:
        final = _resolve_v7_entry_engine_signals(
            selected_signals,
            label="historical_cached_all_setups",
            entry_fill_model=str(args.entry_fill_model),
            selected_strategy_profile=str(args.selected_strategy_profile),
        )

    tier123_enabled = _tier123_profile_enabled(str(args.selected_strategy_profile))
    final, tier123_stats = _apply_tier123_balanced_addon(
        base_final=final,
        dates=dates,
        date_to_root=date_to_root,
        args=args,
        out_dir=out_dir,
        label="historical_cached_all_setups",
    )
    tier123_stats.to_csv(out_dir / "historical_cached_all_setups_tier123_balanced_addon_stats.csv", index=False)
    accepted = _concat_frames([
        pd.DataFrame([{
            "stage": "historical_cached_all_setups",
            "source_entry_signal_path": str(signal_path),
            "dates": int(len(dates)),
            "first_date": dates[0],
            "last_date": dates[-1],
            "cached_entry_signals": int(len(cached_signals)),
            **selected_stats,
        }]),
        tier123_stats,
    ])

    inputs_text = (
        "mode=historical_cached_all_setups\n"
        f"cached_all_setups_dir={source_dir}\n"
        f"source_entry_signal_path={signal_path}\n"
        f"dates={len(dates)}\nfirst_date={dates[0]}\nlast_date={dates[-1]}\n"
        f"candidate_5m_dir={args.candidate_5m_dir}\n"
        f"fallback_candidate_5m_dir={args.fallback_candidate_5m_dir}\n"
        f"data_1m_dir={v6.DATA_1M_DIR}\n"
        f"entry_fill_model={args.entry_fill_model}\n"
        f"selected_strategy_profile={args.selected_strategy_profile}\n"
        f"tier123_balanced_addon_enabled={tier123_enabled}\n"
        "candidate_source=cached full v11 entry-engine signals plus current Tier123 balanced add-on scanner\n"
        f"pnl_model={_pnl_model_description()}\n"
    )

    if final.empty:
        _write_empty_outputs(
            out_dir,
            accepted,
            str(signal_path),
            accepted_filename="historical_cached_all_setups_rules_used.csv",
            reason="[v11 cached_all_setups] no base or Tier123 balanced add-on trades resolved",
        )
        (out_dir / "inputs.txt").write_text(inputs_text, encoding="utf-8")
        print(f"[v11 cached_all_setups] wrote empty no-trade outputs to {out_dir}")
        return 0

    _write_outputs(
        out_dir,
        final,
        accepted,
        str(signal_path),
        accepted_filename="historical_cached_all_setups_rules_used.csv",
        strategy_description=(
            "cached full v11 entry-engine replay + selected honest production profile + "
            "optional Tier123 balanced non-overlap add-on + setup-specific 1-minute exits"
        ),
    )
    (out_dir / "inputs.txt").write_text(inputs_text, encoding="utf-8")
    print(f"[v11 cached_all_setups] wrote {out_dir}")
    return 0


def _write_empty_outputs(
    out_dir: Path,
    accepted: pd.DataFrame,
    source: str,
    *,
    accepted_filename: str,
    reason: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trade_cols = [
        "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
        "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
        "trade_date", "v8_entry_model", "v8_entry_delay_minutes",
        "v6_sl_pct", "v6_target_pct", "v6_outcome", "v6_exit_price",
        "v6_exit_time_ist", "v6_bars_held", "v6_pnl_pct_price",
        "v6_gross_pnl_rs", "v6_cost_rs", "v6_net_pnl_rs",
        "capital_per_trade_rs", "leverage", "notional_exposure_rs",
        "v7_resolution_source", "v8_resolution_source",
    ]
    pd.DataFrame(columns=trade_cols).to_csv(out_dir / "trades.csv", index=False)
    pd.DataFrame(columns=["trade_date", "net_pnl_rs", "cum_pnl_rs", "drawdown_rs"]).to_csv(
        out_dir / "daily.csv",
        index=False,
    )
    pd.DataFrame(
        columns=[
            "side", "setup", "trades", "win_rate_pct", "target_rate_pct",
            "sl_rate_pct", "eod_rate_pct", "pnl_rs", "sl_pct", "target_pct",
        ]
    ).to_csv(out_dir / "by_setup.csv", index=False)
    accepted.to_csv(out_dir / accepted_filename, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)
    pd.DataFrame(
        columns=[
            "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
            "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
            "v8_entry_delay_minutes", "v6_outcome", "v6_net_pnl_rs",
        ]
    ).to_csv(out_dir / "entry_timing_audit.csv", index=False)

    text = "\n".join([
        "=" * 100,
        "AVWAP ID 5-min v11 backtest",
        "current v7 live signal flow + next 1-minute open entry + setup-specific 1-minute exits",
        f"Input: {source}",
        "=" * 100,
        "Trades              : 0",
        "Trading days        : 0",
        "Avg trades/day      : 0.00",
        "Win rate            : 0.00%",
        "Target / SL / EOD   : 0.00% / 0.00% / 0.00%",
        "Profit factor       : NA",
        "Net PnL             : Rs 0.00",
        "Day win rate        : 0.00%",
        "Max drawdown        : Rs 0.00",
        "LONG trades/PnL     : 0 / Rs 0.00",
        "SHORT trades/PnL    : 0 / Rs 0.00",
        "",
        f"No-trade reason     : {reason}",
        "=" * 100,
    ])
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def _write_parity_debug_csv(
    out_dir: Path,
    pipeline: dict,
    entry_rejects: pd.DataFrame | None = None,
    label: str = "",
) -> None:
    """Write v11_ID_parity_debug.csv — per-candidate audit trail across all pipeline stages.

    Columns: ticker, side, setup, signal_time_ist, pipeline_stage, fate, reason,
             quality_score, ranker_score, vwap_dist_atr, rs_pct, atr_pct,
             signal_minute, v11_pipeline_day, v11_pipeline_slot_ist
    """
    frames: list[pd.DataFrame] = []

    def _tag(df: pd.DataFrame, fate: str, reason_col: str | None = None) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        t = df.copy()
        t["_parity_fate"] = fate
        t["_parity_reason"] = (
            t[reason_col].astype(str) if reason_col and reason_col in t.columns
            else t.get("v8_live_gate_rule", t.get("research_reject_reason", pd.Series("", index=t.index)))
        )
        return t

    ranked = pipeline.get("ranked_raw_candidates", pd.DataFrame())
    v8_gated = pipeline.get("v8_gated_candidates", pd.DataFrame())
    research_rejected = pipeline.get("research_rejected_candidates", pd.DataFrame())
    pre_dedupe = pipeline.get("pre_dedupe_live_candidates", pd.DataFrame())
    live_like = pipeline.get("live_like_candidates", pd.DataFrame())

    if not research_rejected.empty:
        frames.append(_tag(research_rejected, "rejected_research_filter", "research_reject_reason"))
    if not v8_gated.empty:
        frames.append(_tag(v8_gated, "v8_gate_passed"))
    if not pre_dedupe.empty:
        frames.append(_tag(pre_dedupe, "live_accepted_pre_dedupe"))
    if not live_like.empty:
        frames.append(_tag(live_like, "live_final_accepted"))

    if entry_rejects is not None and not entry_rejects.empty:
        er = entry_rejects.copy()
        er["_parity_fate"] = "entry_engine_rejected"
        er["_parity_reason"] = er.get("reject_reason", pd.Series("", index=er.index))
        er["v11_pipeline_stage"] = "entry_engine"
        frames.append(er)

    if not frames:
        return

    combined = pd.concat(frames, ignore_index=True)
    keep_cols = [c for c in [
        "ticker", "side", "setup", "signal_time_ist", "v11_pipeline_stage",
        "_parity_fate", "_parity_reason",
        "quality_score", "ranker_score", "vwap_dist_atr", "rs_pct", "atr_pct",
        "signal_minute", "v8_live_gate_status", "v8_live_gate_rule",
        "v11_pipeline_day", "v11_pipeline_slot_ist",
    ] if c in combined.columns]
    combined = combined[keep_cols].rename(columns={
        "_parity_fate": "fate",
        "_parity_reason": "reason",
    })
    combined = combined.sort_values(
        [c for c in ["v11_pipeline_day", "signal_time_ist", "ticker"] if c in combined.columns]
    )
    fname = f"v11_ID_parity_debug{'_' + label if label else ''}.csv"
    combined.to_csv(out_dir / fname, index=False)
    print(f"[v11 parity_debug{' ' + label if label else ''}] wrote {len(combined)} rows → {out_dir / fname}")


def _build_v11_id_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Map the internal trades DataFrame to the canonical v11_ID output schema."""
    if trades is None or trades.empty:
        return pd.DataFrame(columns=[
            "date", "symbol", "side", "setup_name", "entry_time", "entry_price",
            "entry_source_timeframe", "exit_time", "exit_price", "exit_reason",
            "gross_pnl_rs", "total_cost_rs", "net_pnl_rs", "pnl", "pnl_pct",
            "quantity", "notional_exposure_rs", "reason", "filters_passed", "filters_failed",
            "source_5min_file", "source_1min_file",
        ])
    t = trades.copy()
    col = lambda *names: next((t[n] for n in names if n in t.columns), pd.Series(dtype=object, index=t.index))
    out = pd.DataFrame({
        "date":                 col("trade_date"),
        "symbol":               col("ticker"),
        "side":                 col("side"),
        "setup_name":           col("setup"),
        "entry_time":           col("entry_time_v6", "entry_time_ist", "entry_ts"),
        "entry_price":          col("entry_price_v6", "entry_price", "entry_px"),
        "entry_source_timeframe": "5min_signal_1min_open",
        "exit_time":            col("v6_exit_time_ist", "exit_time_ist"),
        "exit_price":           col("v6_exit_price", "exit_price"),
        "exit_reason":          col("v6_outcome", "outcome"),
        "gross_pnl_rs":         col("v6_gross_pnl_rs", "gross_pnl_rs"),
        "total_cost_rs":        col("v6_cost_rs", "total_cost_rs"),
        "net_pnl_rs":           col("v6_net_pnl_rs", "net_pnl_rs"),
        "pnl":                  col("v6_net_pnl_rs", "net_pnl_rs"),
        "pnl_pct":              col("v6_net_pnl_pct", "v6_pnl_pct_price", "pnl_pct"),
        "quantity":             col("quantity"),
        "notional_exposure_rs": col("notional_exposure_rs", "v7_signal_notional_rs"),
        "reason":               col("v8_live_gate_rule", "v8_live_gate_status"),
        "filters_passed":       col("research_filter_version", "v8_live_gate_status"),
        "filters_failed":       col("research_filter_reject_reason", "research_reject_reason"),
        "source_5min_file":     col("v11_source_day", "source_5min_file"),
        "source_1min_file":     str(v6.DATA_1M_DIR),
    })
    return out


def _write_outputs(
    out_dir: Path,
    trades: pd.DataFrame,
    accepted: pd.DataFrame,
    source: str,
    accepted_filename: str = "accepted_rules.csv",
    strategy_description: str = "current v7 live signal flow + next 1-minute open entry + setup-specific 1-minute exits",
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_dir / "trades.csv", index=False)
    summary, daily, by_setup = v6._metrics(trades)
    daily_out = daily.rename(columns={"_pnl": "net_pnl_rs"})
    daily_out.to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    accepted.to_csv(out_dir / accepted_filename, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)

    entry_audit = trades[[
        c for c in [
            "ticker", "side", "setup", "signal_time_v8", "old_entry_time_v7",
            "entry_time_v6", "old_entry_price_v7", "entry_price_v6",
            "v8_entry_delay_minutes", "v6_outcome", "v6_net_pnl_rs",
        ] if c in trades.columns
    ]].copy()
    entry_audit.to_csv(out_dir / "entry_timing_audit.csv", index=False)

    # v11_ID canonical output files
    _build_v11_id_trades(trades).to_csv(out_dir / "v11_ID_trades.csv", index=False)
    daily_out.to_csv(out_dir / "v11_ID_daily_summary.csv", index=False)
    by_setup.to_csv(out_dir / "v11_ID_setup_summary.csv", index=False)

    text = v6._summary_text(summary, by_setup, Path(source))
    text = text.replace("AVWAP ID 5-min v6 backtest", "AVWAP ID 5-min v11 backtest")
    text = text.replace(
        "v5 trades re-resolved on 1-minute bars with setup-specific SL/target exits",
        strategy_description,
    )
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")
    (out_dir / "v11_ID_summary.csv").write_text(text + "\n", encoding="utf-8")


def _merge_external_conf_candidates(raw_candidates: "pd.DataFrame", day: str) -> "pd.DataFrame":
    """conf mode: merge external scan-source candidates (T_TREND_DAY_EMA_STAIR_SHORT, S_UPTHRUST_TRAP_FADE)
    for `day` into raw_candidates so they enter the pipeline. They are re-admitted past v8+research
    downstream (they are in _FINAL_CONF_READMIT_SETUPS); the conf mask + premom gate + exit then apply."""
    ext = _FINAL_CONF_EXT_CANDIDATES
    if not isinstance(ext, pd.DataFrame) or ext.empty:
        return raw_candidates
    day_ext = ext[ext["_ext_day"] == str(day)]
    if day_ext.empty:
        return raw_candidates
    day_ext = day_ext.drop(columns=["_ext_day"], errors="ignore").copy()
    # de-dupe external candidates (the scan CSVs can carry repeats) before injecting
    _key = [c for c in ("ticker", "setup", "signal_time_ist") if c in day_ext.columns]
    if _key:
        day_ext = day_ext.drop_duplicates(subset=_key, keep="first")
    if raw_candidates is None or raw_candidates.empty:
        return day_ext.reset_index(drop=True)
    return pd.concat([raw_candidates, day_ext], ignore_index=True, sort=False)


def _activate_final_setup_conf() -> dict:
    """Opt-in (--selected_strategy_profile final_setup_conf): run ONLY the approved book in
    final_setup_conf.FINAL_SETUP_CONF. Overrides exits + pre-momentum gates with each setup's
    tuned values (dropping the production gates for these setups), and restricts the candidate
    scan to only these setups. The selected-strategy mask (_final_setup_conf_mask) applies each
    setup's mask_terms + entry guards. Non-breaking: only runs when the conf profile is chosen.

    COVERAGE NOTE: A/B/D/E/G were validated on the clean (post-v8/research) pool, so they replay
    faithfully here. L_DOUBLE_BOTTOM_VWAP / L_PRESSURE_BURST_VWAP (raw pre-gate pool) and
    T_TREND_DAY_EMA_STAIR_SHORT (tier123 overlay) were validated on non-standard pools — the
    standard v11 v8/research/overlay stages may admit fewer of them; verify per-setup counts on
    the first run and reconcile gating (see provenance.gating_caveat in the conf)."""
    global PRE_ENTRY_MOMENTUM_SETUP_GATES, _FINAL_CONF_READMIT_SETUPS, _FINAL_CONF_EXT_CANDIDATES, ENTRY_SHADOW_SETUPS
    global _FINAL_CONF_ACTIVE, _FINAL_CONF_SETUP_KEYS, _FINAL_CONF_EXIT_POLICIES, _FINAL_CONF_ENTRY_POLICIES
    _fc = _load_final_setup_conf_module()
    conf = _fc.FINAL_SETUP_CONF
    keys = frozenset(conf)
    _FINAL_CONF_ACTIVE = True
    _FINAL_CONF_SETUP_KEYS = keys
    _FINAL_CONF_EXIT_POLICIES = {
        name: dict(cfg["exit_policy"])
        for name, cfg in conf.items()
        if isinstance(cfg.get("exit_policy"), dict) and cfg["exit_policy"]
    }
    _FINAL_CONF_ENTRY_POLICIES = {
        name: dict(cfg["entry_policy"])
        for name, cfg in conf.items()
        if isinstance(cfg.get("entry_policy"), dict) and cfg["entry_policy"]
    }
    for name, cfg in conf.items():
        ex = cfg.get("exit", {})
        if "sl_pct" in ex and "tgt_pct" in ex:
            v6.SETUP_EXIT_RULES[name] = (float(ex["sl_pct"]), float(ex["tgt_pct"]))
    PRE_ENTRY_MOMENTUM_SETUP_GATES = {
        name: tuple((t[0], t[1], float(t[2])) for t in cfg.get("pre_momentum_terms", []))
        for name, cfg in conf.items() if cfg.get("pre_momentum_terms")
    }
    # setups that must be re-admitted past v8+research: raw-pool (v8 drops them) + scan-source (not emitted)
    _readmit_evals = ("RAW_PRE_GATE_POOL", "TIER123_OVERLAY_PROBE", "NEW_SETUPS_SCAN")
    _FINAL_CONF_READMIT_SETUPS = frozenset(
        name for name, cfg in conf.items()
        if str(cfg.get("provenance", {}).get("evaluated_on", "")).upper() in _readmit_evals
    )
    # scan-source setups (not emitted by candidate_scan) -> load their external candidate CSVs once
    _ext = []
    for name, cfg in conf.items():
        ev = str(cfg.get("provenance", {}).get("evaluated_on", "")).upper()
        if ev not in ("TIER123_OVERLAY_PROBE", "NEW_SETUPS_SCAN"):
            continue
        csv = cfg.get("provenance", {}).get("scan_source_csv") or _FINAL_CONF_EXT_SRC.get(ev)
        if csv and os.path.exists(csv):
            try:
                d = pd.read_csv(csv, low_memory=False)
                d = d[d["setup"].astype(str) == name].copy()
                if not d.empty:
                    d["_ext_day"] = pd.to_datetime(d["signal_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d")
                    _ext.append(d)
                    print(f"[v11 final_setup_conf]   loaded {len(d)} external candidates for {name} from {os.path.basename(csv)}", flush=True)
            except Exception as exc:
                print(f"[v11 final_setup_conf]   !! failed to load external candidates for {name}: {exc!r}", flush=True)
    _FINAL_CONF_EXT_CANDIDATES = pd.concat(_ext, ignore_index=True) if _ext else pd.DataFrame()
    # conf book is the gate of record: un-shadow any approved setup (its conf gate is the quality filter).
    _unshadow = frozenset(ENTRY_SHADOW_SETUPS) & keys
    if _unshadow:
        ENTRY_SHADOW_SETUPS = frozenset(ENTRY_SHADOW_SETUPS) - keys
        print(f"[v11 final_setup_conf]   un-shadowed conf setups (now tradeable): {sorted(_unshadow)}", flush=True)
    candidate_scan.ALLOWED_SETUPS = keys
    candidate_scan.FILTER_TO_V8_EXIT_SETUPS = True
    # USER_DIRECTED setups whose detector is emitted only under the conf book (default-off in
    # production). Enable them now that the conf is active and they are in ALLOWED_SETUPS.
    if hasattr(candidate_scan, "v2") and "S9_MIDDAY_LOSE" in keys:
        candidate_scan.v2.ENABLE_S9_MIDDAY_LOSE = True
    if hasattr(candidate_scan, "v2") and "DOC5D_AVWAP_RECLAIM_LONG" in keys:
        candidate_scan.v2.ENABLE_DOC5D_AVWAP_RECLAIM = True
    if _FINAL_CONF_READMIT_SETUPS:
        print(f"[v11 final_setup_conf]   re-admit (bypass v8+research): {sorted(_FINAL_CONF_READMIT_SETUPS)}", flush=True)
    print(f"[v11 final_setup_conf] ACTIVE — {len(keys)} approved setups: {sorted(keys)}", flush=True)
    print(f"[v11 final_setup_conf]   exits overridden for all; pre-momentum gated: "
          f"{sorted(PRE_ENTRY_MOMENTUM_SETUP_GATES)}", flush=True)
    print("[v11 final_setup_conf]   masks+guards via _final_setup_conf_mask; production masks/overlays bypassed.",
          flush=True)
    return conf


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v11 exact current-v7 live strategy backtest")
    ap.add_argument(
        "--mode",
        choices=[
            "live_parity",
            "v7_live_paper_replay",
            "historical_full_day",
            "historical_all_available",
            "historical_cached_all_setups",
        ],
        default="historical_full_day",
    )
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument(
        "--cached_all_setups_dir",
        type=str,
        default=r"C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250",
        help=(
            "For --mode historical_cached_all_setups: directory containing a completed full v11 "
            "all_setups_entry_engine_signals.csv or historical_all_available_entry_engine_signals.csv."
        ),
    )
    ap.add_argument("--live_candidate_json_dir", type=str, default=str(LIVE_CANDIDATE_JSON_DIR))
    ap.add_argument("--live_paper_dir", type=str, default=str(LIVE_PAPER_DIR))
    ap.add_argument("--candidate_5m_dir", type=str, default=str(DEFAULT_CANDIDATE_5M_DIR))
    ap.add_argument("--fallback_candidate_5m_dir", type=str, default=str(HISTORICAL_5M_DIR))
    ap.add_argument("--live_date", type=str, default="")
    ap.add_argument("--historical_date", type=str, default="")
    ap.add_argument("--start_date", type=str, default="")
    ap.add_argument("--end_date", type=str, default="")
    ap.add_argument("--start_time", type=str, default="09:15")
    ap.add_argument("--end_time", type=str, default="15:00")
    ap.add_argument(
        "--exclude_opening_snapshot",
        action="store_true",
        help=(
            "Strict research mode: skip the 09:15 opening-snapshot slot for signal "
            "scanning and drop it from the market-context regime. Default OFF keeps "
            "the hybrid 76-bar convention (09:15..15:30) for live parity."
        ),
    )
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--cost_bps", type=float, default=v6.DEFAULT_COST_BPS)
    ap.add_argument(
        "--cost_model", choices=["flat_bps", "statutory"], default="statutory",
        help=(
            "statutory = V7 PAPER_TRUE NSE costs on the risk-sized live notional (default); "
            "flat_bps = legacy price-only behavior for V7 entry-engine modes"
        ),
    )
    ap.add_argument(
        "--slippage_bps", type=float, default=0.0,
        help="adverse per-leg slippage/half-spread (bps) on entry+exit; statutory cost_model only (set 15 to match the tuner)",
    )
    ap.add_argument(
        "--entry_fill_model",
        choices=["ltp_on_signal_1m_open", "signal_bar"],
        default="ltp_on_signal_1m_open",
        help=(
            "Historical approximation of PAPER_TRADE_TRUE fills. "
            "ltp_on_signal_1m_open mirrors the live bat's ltp_on_signal mode using "
            "the next 1-minute open plus v7 slippage/rebased SL-target."
        ),
    )
    ap.add_argument(
        "--selected_strategy_profile",
        choices=list(SELECTED_STRATEGY_PROFILE_CHOICES),
        default=SELECTED_STRATEGY_DEFAULT_PROFILE,
        help=(
            "Post-v7-entry strategy profile for historical modes. "
            "production_core is the honest holdout-improved profile; "
            "production_core_tiny adds the tiny BEAR-only E_VWAP_LOSE_EARLY_SHORT rule; "
            "production_core_ab_probe adds all explicit A/B probation setups; "
            "production_core_ab_good_probe adds only the positive A/B watchlist setups "
            f"{', '.join(AB_GOOD_RESULT_SETUPS)}; "
            "production_core_ab_filtered_relaxed adds the current best filtered A/B probation "
            f"rules: B_HUGE rs_pct <= {AB_FILTERED_RELAXED_B_HUGE_MAX_RS_PCT:.4f} and "
            "A_PULLBACK market_abs_ret_pct <= "
            f"{AB_FILTERED_RELAXED_A_PULLBACK_MAX_MARKET_ABS_RET_PCT:.4f}; "
            "production_core_ab_max_pnl_low_valid adds the 2026-06-02 max-PnL profile "
            "with relaxed S_BB, filtered B_AVWAP, filtered A_MOD_CLOSE, "
            "A_MOD_BREAK_C1_LOW micro-probe, A_MOD_BREAK_C1_HIGH add-on, "
            "D_EMA20_REJECTION add-on, filtered E_VWAP_BAND, and low-validation E_VWAP_LOSE; "
            "production_core_ab_max_pnl_low_valid_residual_overlay keeps that profile unchanged "
            "and additionally admits the residual late-session D_EMA20_REJECTION and residual "
            "morning S_BB_SQUEEZE_SHORT probation overlays; "
            f"{TIER123_BALANCED_PROFILE} keeps the residual-overlay profile protected and appends "
            "the Tier123 balanced non-overlap add-on: late bearish EMA stair short plus controlled "
            "VWAP extreme fade long/short; "
            "ab_only_probe tests all explicit A/B probation setups; "
            "ab_good_only_probe tests only the positive A/B watchlist setups; "
            "ab_filtered_relaxed_only tests only the current best filtered A/B probation rules; "
            "none keeps raw v11/v7 entry-engine signals."
        ),
    )
    ap.add_argument(
        "--ab_gate_profile",
        choices=list(AB_GATE_PROFILE_CHOICES),
        default="off",
        help=(
            "Explicit A/B setup admission gate before the v7 entry engine. "
            "off keeps exact production live-gate behavior; quality_top_slot admits only "
            "top quality A_/B_ candidates per slot for probation research."
        ),
    )
    ap.add_argument(
        "--ab_gate_min_quality",
        type=float,
        default=250.0,
        help="Minimum quality_score for --ab_gate_profile quality_top_slot.",
    )
    ap.add_argument(
        "--ab_gate_max_per_side",
        type=int,
        default=1,
        help="Maximum A/B probation candidates per side per 5-minute slot.",
    )
    ap.add_argument(
        "--ab_gate_max_per_slot",
        type=int,
        default=2,
        help="Maximum A/B probation candidates across both sides per 5-minute slot.",
    )
    ap.add_argument(
        "--parity-debug",
        action="store_true",
        default=False,
        dest="parity_debug",
        help=(
            "Write v11_ID_parity_debug.csv: per-candidate audit trail across all pipeline stages "
            "(ranked → v8_gate → research_filter → entry_engine → accepted/rejected with reasons). "
            "Use to cross-check v11 backtest entries against V7 live session logs."
        ),
    )
    args = ap.parse_args()
    global _V11_EXACT_LIVE_PARITY
    _V11_EXACT_LIVE_PARITY = str(args.mode) == "live_parity"
    manifest_path, _ = freeze_runtime_manifest(
        "avwap_5min_ID_v11_backtesting",
        runtime_root=os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"),
        source_files=(
            Path(__file__),
            Path(live_discovery.__file__),
            Path(v11_overlay.__file__),
            Path(candidate_scan.__file__),
            Path(setup_conf_loader.__file__),
            Path(er.__file__),
        ),
        resolved_config={
            "mode": str(args.mode),
            "out": str(args.out),
            "live_date": str(args.live_date),
            "historical_date": str(args.historical_date),
            "selected_strategy_profile": str(args.selected_strategy_profile),
            "entry_fill_model": str(args.entry_fill_model),
            "cost_model": str(args.cost_model),
            "slippage_bps": float(args.slippage_bps),
            "candidate_5m_dir": str(args.candidate_5m_dir),
            "live_candidate_json_dir": str(args.live_candidate_json_dir),
            "exact_live_parity": bool(_V11_EXACT_LIVE_PARITY),
            "immutable_slot_raw_1m_dir": str(V7_SLOT_RAW_1M_DIR),
        },
    )
    print(f"[v11] frozen_runtime_manifest={manifest_path}", flush=True)
    global _EXCLUDE_OPENING_SNAPSHOT
    _EXCLUDE_OPENING_SNAPSHOT = bool(getattr(args, "exclude_opening_snapshot", False))
    if _EXCLUDE_OPENING_SNAPSHOT:
        print("[v11] --exclude_opening_snapshot ON: skipping 09:15 slot + dropping it from regime (strict research mode; NOT live-parity)")
    global _V11_COST_MODEL, _V11_SLIPPAGE_BPS
    _V11_COST_MODEL = str(getattr(args, "cost_model", "statutory"))
    _V11_SLIPPAGE_BPS = float(getattr(args, "slippage_bps", 0.0))
    if _V11_COST_MODEL == "statutory":
        print(
            "[v11] cost_model=statutory: V7 risk-sized quantity "
            f"(risk=Rs {RISK_EQUITY_RS * RISK_PCT_PER_TRADE / 100.0:,.0f}, "
            f"notional bounds=Rs {RISK_MIN_NOTIONAL_RS:,.0f}..{RISK_MAX_NOTIONAL_RS:,.0f}) "
            f"+ NSE PAPER_TRUE costs + {_V11_SLIPPAGE_BPS:.1f} bps/leg extra slippage"
        )
    args.selected_strategy_profile = _normalise_selected_strategy_profile(args.selected_strategy_profile)
    args.ab_gate_profile = _normalise_ab_gate_profile(args.ab_gate_profile)
    if args.selected_strategy_profile in AB_SELECTED_STRATEGY_PROFILES and args.ab_gate_profile == "off":
        args.ab_gate_profile = "quality_top_slot"
    if args.selected_strategy_profile == FINAL_CONF_PROFILE:
        _activate_final_setup_conf()
        print(
            "[v11 ab_gate] auto-enabled quality_top_slot because selected_strategy_profile "
            f"is {args.selected_strategy_profile}",
            flush=True,
        )

    if args.mode == "live_parity":
        return _run_live_parity(args)
    if args.mode == "v7_live_paper_replay":
        return _run_v7_live_paper_replay(args)
    if args.mode == "historical_full_day":
        return _run_historical_full_day(args)
    if args.mode == "historical_all_available":
        return _run_historical_all_available(args)
    if args.mode == "historical_cached_all_setups":
        return _run_historical_cached_all_setups(args)
    raise SystemExit(f"[v11] unsupported mode: {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
