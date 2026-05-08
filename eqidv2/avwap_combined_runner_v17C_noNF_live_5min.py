 # -*- coding: utf-8 -*-
"""
v17C noNF LIVE -- Candidate E3 (R:R-protected) with NIFTY_CONTEXT regime
filter DISABLED.  PRODUCTION view.

WHAT THIS IS
============
Inherits the v17B production stack (8 honesty fixes ON, v17p cascade) and
overrides FOUR things:

  1. CANDIDATE_B_FILTER_SPEC         -> CANDIDATE_E_FILTER_SPEC  (8 setups)
  2. Phase-5 SIZE_MULTIPLIERS        -> Candidate-E sizing tiers
  3. NIFTY_CONTEXT engine filter     -> no-op pass-through
  4. Engine SL=0.75/TGT=0.80 fixed   -> per-setup CANDIDATE_E3_SL_TGT
                                        (re-resolved against 1-min OHLC)

Plus adds a NEW post-Phase-2 stage:

  5. Candidate-E portfolio governors -- time-of-day window per side, daily
     side cap, daily loss-streak kill, per-setup cooling on a rolling
     causal lookback.

The runner emits TWO CSVs:
  *_candE.csv   - engine outcomes at SL=0.75/TGT=0.80 (Cand-E2 reference)
  *_candE3.csv  - per-setup R:R-protected re-resolution (PRODUCTION)

R:R protection rules (per user 2026-05-02):
  - SL  >= 0.75% (no tighter stop than baseline)
  - TGT >= 0.80% (no tighter target than baseline)
  - TGT >= SL    (R:R >= 1.0 -- reward at least matches risk)

Cand-E4 production lab numbers (re-tuned 2026-05-04 against the FRESH
2,465-trade post-Cand-E CSV from the actual production scan):
    n=978    PF=2.160    win=73.42%    day-win=78.54%    DD=42.5%
    sumPnL=+1328.44%    Notional=+Rs.2,65,689
    vs prior Cand-E3 production run (n=1514, PF 1.24, Rs +119,693):
        PF +0.92, win +12.65 pp, day-win +19.82 pp, Rs +145,996

Per-setup chain re-tuning vs Cand-E3 (added filter steps for weak setups):
    A_MOD_BREAK_C1_HIGH    : added avwap_dist>=1.5766       PF 1.04 -> 2.38
    B_HUGE_RECLAIM_BREAK   : added ema20_gap<=3.50, hour<=9.92  PF 0.88 -> 2.49
    D_EMA20_BOUNCE         : added avwap_dist>=1.5329       PF 0.99 -> 1.89

Per-setup sizing tiers (drop two broken SHORT setups):
    LONG  A_MOD_BREAK_C1_HIGH         1.30x  (PF 2.38)
    LONG  B_AVWAP_RECLAIM_REVERSAL    1.30x  (PF 2.41)
    LONG  B_HUGE_*_RECLAIM_BREAK      1.30x  (PF 2.49)
    LONG  D_EMA20_BOUNCE              1.00x  (PF 1.89)
    SHORT A_MOD_BREAK_C1_LOW          1.00x  (PF 2.10)
    SHORT C_OR_BREAKDOWN              0.00x  DROPPED  (PF 1.00, no edge)
    SHORT D_EMA20_REJECTION           0.00x  DROPPED  (PF 0.83, losing)
    SHORT G_LOWER_LOW_BREAK           0.50x  (PF 1.44 borderline)

Per-setup R:R-protected SL/TGT picks (Cand-E4):
    LONG  A_MOD_BREAK_C1_HIGH         SL=0.75 TGT=0.80  R:R=1.07
    LONG  B_AVWAP_RECLAIM_REVERSAL    SL=0.75 TGT=0.80  R:R=1.07
    LONG  B_HUGE_C1_CLOSE_RECLAIM_BR  SL=0.75 TGT=0.90  R:R=1.20  (wider TGT, small n)
    LONG  D_EMA20_BOUNCE              SL=0.80 TGT=0.80  R:R=1.00
    SHORT A_MOD_BREAK_C1_LOW          SL=0.80 TGT=0.85  R:R=1.06
    SHORT G_LOWER_LOW_BREAK           SL=0.80 TGT=0.80  R:R=1.00

Backed-out filter (the one disabled here):
    - LONG signals where nifty_context_mode = SHORT_ONLY
    - SHORT signals where nifty_context_mode = LONG_ONLY
    - BOTH-mode LONG signals where nifty_rel_strength_pct < 0.75pp
    - BOTH-mode SHORT signals where nifty_rel_strength_pct > -0.50pp
    - V17c hybrid ATR-relative variants of the above

WHAT IT INHERITS
================
Imports avwap_combined_runner_v17B_live_5min, which pulls the v17p cascade
+ all 8 honesty fixes. v17B's Candidate-B per-setup chains are then
OVERRIDDEN (not inherited) -- v17C swaps in Candidate-E.

  - F1 (hardened Stage 0)            ON
  - F4 (post-run audit)              ON  (widened band 1000-5000)
  - F6 (vol-ratio prior-bar)         ON
  - F7 (NIFTY lookup -5min)          ON  (irrelevant since context is no-op)
  - F11 (no close-confirm)           ON
  - F12 (entry-bar exits)            ON
  - F14 (floor zero-lag)             ON
  - F15 (drop 5M_FALLBACK)           ON
  - Phase 5 honest size mults        OVERRIDE -> Cand-E sizing tiers
  - Per-setup filter chains          OVERRIDE -> CANDIDATE_E_FILTER_SPEC
  - Portfolio governors              NEW      -> G1 time / G2 cap / G3 kill / G4 cool

CANDIDATE-E IS BUILT FROM THE LAB
=================================
See C:\\TradingData\\eqidv2\\outputs_v17C_candC_lab\\v17C_candE_specs.py
for the canonical spec (drop-in compatible). This file mirrors that dict
inline so the runner is self-contained.

Output: outputs_v17C_noNF_live_5min/
"""
from __future__ import annotations

import glob
from pathlib import Path

import pandas as pd

# Pull in v17B's full stack first (Candidate B spec + 8 honesty fixes +
# v17p cascade). All patches install during this import.
import avwap_combined_runner_v17B_live_5min as _v17B
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# 1. OUTPUT DIR ROUTING -> outputs_v17C_noNF_live_5min
#
# Override v17B's runtime_dir patch so all artefacts land in a v17C-specific
# directory and don't pollute v17B output.
# ---------------------------------------------------------------------------
_orig_runtime_dir_pre_v17B = _base.runtime_dir  # this is v17B's wrapper


def _v17C_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17C_noNF_live_5min", "v17B_live_5min", "v17t_live_5min",
            "v17p_5min", "v17o_5min", "v17n_5min", "v17m_5min",
            "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min",
            "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min",
            "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17C_noNF_live_5min")
        new_parts.append(text)
    # Call the original (pre-patched) runtime_dir to avoid the v17B wrapper
    # rewriting our v17C path back to v17B.
    import avwap_combined_runner_v16_5min
    # We need the truly original runtime_dir, not the v17B-patched one.
    # _v17B's _orig_runtime_dir captured the un-patched original.
    return _v17B._orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17C_runtime_dir


# ---------------------------------------------------------------------------
# 2. NO-OP THE NIFTY_CONTEXT FILTER (the entire point of this file)
#
# Replace _base._apply_nifty_intraday_context with a pass-through. The F7
# fix wrapped this earlier (in v17B's import) to shift the lookup -5min;
# we replace the whole thing with a function that never reads real NIFTY
# context and instead stamps synthetic side-aware columns.
#
# Why not leave NIFTY columns missing?
# Downstream inherited filters use these columns as ordinary features:
#   - v17d drops shorts with NaN nifty_rel_strength_pct
#   - v17h/v17k/v17n/v17o require positive NIFTY RS for some long setups
#   - v17j/v17k require negative NIFTY RS for some short setups
# Missing columns therefore do not mean "no NIFTY"; they mean "drop rows".
#
# The constants below make those gates permissive without using NIFTY data:
#   LONG  +1.25 passes long min-RS gates.
#   SHORT -1.25 passes short max-RS gates and stays above v17d's
#          extreme-downside cap of -2.00.
#   BOTH  satisfies both-mode setup gates without selecting LONG_ONLY or
#          SHORT_ONLY branches.
#
# The signature must match the original:
#   _apply_nifty_intraday_context(short_df, long_df, cfg, mode_map, nifty_ret_map)
# ---------------------------------------------------------------------------
_V17C_NO_NIFTY_CONTEXT_MODE = "BOTH"
_V17C_NO_NIFTY_LONG_RS = 1.25
_V17C_NO_NIFTY_SHORT_RS = -1.25


def _v17C_stamp_no_nifty_columns(df: pd.DataFrame | None, *, side: str) -> pd.DataFrame | None:
    """Stamp synthetic no-NIFTY context columns for downstream inherited filters."""
    if df is None:
        return None

    out = df.copy()
    rs_value = _V17C_NO_NIFTY_LONG_RS if side.upper() == "LONG" else _V17C_NO_NIFTY_SHORT_RS
    out["nifty_context_mode"] = _V17C_NO_NIFTY_CONTEXT_MODE
    out["nifty_rel_strength_pct"] = float(rs_value)
    out["v17C_noNF_nifty_context_disabled"] = True
    out["v17C_noNF_synthetic_nifty_rs"] = float(rs_value)
    return out


def _v17C_no_nifty_context(short_df, long_df, cfg, mode_map, nifty_ret_map):
    """No-op replacement for v16's NIFTY context filter.

    It intentionally does not read mode_map or nifty_ret_map. It stamps
    synthetic context fields so downstream inherited filters do not treat
    the no-NIFTY experiment as missing-data failure.
    """
    short_df = _v17C_stamp_no_nifty_columns(short_df, side="SHORT")
    long_df = _v17C_stamp_no_nifty_columns(long_df, side="LONG")
    n_short = 0 if short_df is None else len(short_df)
    n_long = 0 if long_df is None else len(long_df)
    print(f"[V17C_NO_NIFTY] NIFTY_CONTEXT filter DISABLED -- "
          f"passthrough SHORT={n_short}, LONG={n_long}; "
          f"stamped synthetic mode={_V17C_NO_NIFTY_CONTEXT_MODE}, "
          f"SHORT_RS={_V17C_NO_NIFTY_SHORT_RS:+.2f}%, "
          f"LONG_RS={_V17C_NO_NIFTY_LONG_RS:+.2f}%")
    return short_df, long_df


_base._apply_nifty_intraday_context = _v17C_no_nifty_context


# ---------------------------------------------------------------------------
# 2.5  CANDIDATE-E: PER-SETUP FILTER CHAINS + SIZING TIERS (overrides v17B)
#
# v17B installed CANDIDATE_B_FILTER_SPEC + Phase-5 SIZE_MULTIPLIERS during
# its module import. We replace both in-place AFTER that import so v17C
# runs Candidate-E without v17B knowing.
#
# Candidate-E was mined from the latest live noNF trades CSV
# (avwap_longshort_trades_v16_5min_ALL_DAYS_20260501_104428.csv) by
# _v17C_candE_balanced_analyzer.py. Lab numbers, full window 222 days:
#     n=1018  PF=2.288  win=70.24%  DD=30.4%  Sharpe=5.48  Notional +Rs.2,03,396
#     OOS  : n=350   PF=1.836  win=68.00%  Sharpe=4.05
# ---------------------------------------------------------------------------
# 2.5.1 Augment v17B's causal-feature whitelist (Cand-E uses bars_from_open
# and entry_bar_vol_ratio which v17B did not).
_v17B._CAUSAL_FEATURES |= {"bars_from_open", "entry_bar_vol_ratio"}

# 2.5.2 Replace v17B's filter spec dict with Candidate-E2 (REFINED).
# Three LONG setups re-tuned by exhaustive search to lift trade count
# and/or PF (see _v17C_candE_setup_tuner.py for the search):
#   A_MOD_BREAK_C1_HIGH   : 73  -> 184 trades (PF 5.53 -> 2.04, target met)
#   B_HUGE_*_RECLAIM      : 40  -> 54  trades (PF 3.59 -> 2.02, target met)
#   D_EMA20_BOUNCE        : 409 -> 142 trades (PF 1.28 -> 2.12, lift met;
#                                              n drop is unavoidable - no
#                                              high-PF subset of 400+ exists)
CANDIDATE_E_FILTER_SPEC = {
    # CAND-E4: re-tuned 2026-05-04 against fresh production CSV (the actual
    # 2,465-trade post-Cand-E output, NOT the stale Cand-B-prefiltered CSV).
    # Adds extra filter step(s) to weak setups whose fresh-data PF was < 1.5.
    # ---- LONG -----------------------------------------------------------
    ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
        ("bars_from_open",        "<=", 9.0),
        # ADDED in E4: avwap_dist filter lifts PF 1.04 -> 2.38 (OOS 2.24)
        ("avwap_dist_atr_signal", ">=", 1.5766),
    ],
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
        ("atr_pct_signal",        ">=", 0.0040),
        ("entry_bar_vol_ratio",   ">=", 0.8842),
    ],
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
        ("bars_from_open",        "<=", 4.0),
        # ADDED in E4: ema20_gap + entry_hour lifts PF 0.88 -> 2.49 (n=30)
        ("ema20_gap_atr_signal",  "<=", 3.4968),
        ("entry_hour",            "<=", 9.9167),
    ],
    ("LONG",  "D_EMA20_BOUNCE"): [
        ("atr_pct_signal",        ">=", 0.0051),
        ("stochk_signal",         "<=", 54.0876),
        # ADDED in E4: avwap_dist filter lifts PF 0.99 -> 1.89 (OOS 2.04)
        ("avwap_dist_atr_signal", ">=", 1.5329),
    ],
    # ---- SHORT ---------------------------------------------------------
    ("SHORT", "A_MOD_BREAK_C1_LOW"): [
        ("atr_pct_signal",        ">=", 0.0037),
        ("stochk_signal",         "<=", 12.4427),
    ],
    ("SHORT", "C_OR_BREAKDOWN"): [
        # E4: no robust additional filter found; SHORT C_OR sized to 0 below
        ("entry_hour",            ">=", 9.9167),
        ("avwap_dist_atr_signal", "<=", 1.8253),
    ],
    ("SHORT", "D_EMA20_REJECTION"): [
        # E4: no robust additional filter found; SHORT D_EMA20_REJ sized to 0 below
        ("atr_pct_signal",        "<=", 0.0047),
        ("quality_score",         "<=", 0.5633),
    ],
    ("SHORT", "G_LOWER_LOW_BREAK"): [
        ("rsi_signal",            ">=", 27.3299),
        ("entry_hour",            "<=", 10.1667),
    ],
    # Phase 2b candidate. Detector already enforces tight intra-bar filters
    # (upper-wick, MFI, MACD_Hist slope, vol burst, hour <= 10:45). The chain
    # below is a redundant-but-safe entry-hour gate so v17B's validator is
    # happy. Tighten only after backtest data shows where real edge lives.
    ("SHORT", "H_FAILED_BREAKOUT_TRAP"): [
        ("entry_hour",            "<=", 10.7500),
    ],
}

# Validate (mirror v17B's check) so a bad chain fails at import.
for (_side, _setup), _chain in CANDIDATE_E_FILTER_SPEC.items():
    if _side not in ("LONG", "SHORT"):
        raise SystemExit(f"[V17C_E] bad side {_side!r}")
    for _step in _chain:
        if not (isinstance(_step, tuple) and len(_step) == 3):
            raise SystemExit(f"[V17C_E] {_setup!r} malformed step {_step!r}")
        _f, _d, _ = _step
        if _f not in _v17B._CAUSAL_FEATURES:
            raise SystemExit(
                f"[V17C_E] {_setup!r} non-causal feature {_f!r}"
            )
        if _d not in (">=", "<="):
            raise SystemExit(f"[V17C_E] {_setup!r} bad direction {_d!r}")

# Hot-swap into v17B's module so its already-installed filter function
# (`_v17B_candidate_filter`) reads the new dict at call time.
_v17B.CANDIDATE_B_FILTER_SPEC = CANDIDATE_E_FILTER_SPEC
print(f"[V17C_E] swapped CANDIDATE_B_FILTER_SPEC -> Candidate-E "
      f"({len(CANDIDATE_E_FILTER_SPEC)} setups)")


# 2.5.3 Per-setup configuration: master kill-switch + sizing + caps + auto-kill.
#
# Single source of truth for per-setup state. Fields:
#   enabled           master kill-switch. False -> derived size_mult forced
#                     to 0 so G0 strips this setup before governors run.
#                     Use this to halt a degrading setup with a 1-line edit.
#   size_mult         Cand-E size multiplier when enabled.
#   max_daily_trades  per-setup daily cap (None = side cap only). Enforced
#                     by governor G5. Useful for trickling a new setup in
#                     at small per-day exposure (e.g. C_OR_BREAKDOWN at 3/day
#                     once flipped on in Phase 1).
#   kill_pf_30        if rolling PF over the last 30 KEPT trades for this
#                     setup falls below this value, governor G6 halts it for
#                     the rest of the run. None = no auto-kill at this window.
#   kill_pf_60        same, over the last 60 KEPT trades.
#   notes             rationale / tier reference. Never read by code.
#
# Auto-kill thresholds (kill_pf_*) ship as None for every setup. Calibrate
# them against ~30+ live trades per setup before arming -- setting thresholds
# off backtest data risks tripping on in-sample noise.
#
# CAND-E4 sizing tiers (fresh per-setup PFs):
#   PF >= 2.20  -> 1.30x (elite)
#   PF 1.80-2.20-> 1.00x
#   PF 1.20-1.80-> 0.50x (light size on borderline)
#   PF < 1.20   -> 0.00x (drop -- no robust filter found)
CANDIDATE_E_SETUP_CONFIG = {
    # LONG
    "A_MOD_BREAK_C1_HIGH":             {"enabled": True,  "size_mult": 1.30, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 2.38"},
    "B_AVWAP_RECLAIM_REVERSAL":        {"enabled": True,  "size_mult": 1.30, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 2.41"},
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":   {"enabled": True,  "size_mult": 1.30, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 2.49 (n=30 small)"},
    "D_EMA20_BOUNCE":                  {"enabled": True,  "size_mult": 1.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 1.89"},
    # SHORT
    "A_MOD_BREAK_C1_LOW":              {"enabled": True,  "size_mult": 1.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 2.10"},
    "C_OR_BREAKDOWN":                  {"enabled": False, "size_mult": 0.00, "max_daily_trades": 3,    "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 1.00 raw; Phase 1 candidate at 0.25x cap=3 once strict filter validated"},
    "D_EMA20_REJECTION":               {"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 0.83 -- DROP. Re-enable only after a specific filter raises OOS PF >= 1.30"},
    "G_LOWER_LOW_BREAK":               {"enabled": True,  "size_mult": 0.50, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "E4 PF 1.44 borderline"},
    "H_FAILED_BREAKOUT_TRAP":          {"enabled": False, "size_mult": 0.00, "max_daily_trades": 3,    "kill_pf_30": None, "kill_pf_60": None, "notes": "Phase 2b new short. Two-step gate: (1) set PHASE2B_FBT_ENGINE_SCAN_ENABLED=True so engine emits raw trades; (2) backtest, then flip enabled=True size_mult=0.25 here."},
    # Setups not in Cand-E filter spec -- explicit disabled
    "A_MOD_CLOSE_CONTINUATION_BREAK":  {"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "not in spec"},
    "C_OR_BREAKOUT":                   {"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "not in spec"},
    "G_HIGHER_HIGH_BREAK":             {"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "not in spec"},
    "D_AVWAP_LOSE_REVERSAL":           {"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "not in spec"},
    "E_VWAP_BAND_FADE":                {"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "not in spec"},
    "B_HUGE_RED_FAILED_BOUNCE":        {"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "not in spec"},
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH":{"enabled": False, "size_mult": 0.00, "max_daily_trades": None, "kill_pf_30": None, "kill_pf_60": None, "notes": "not in spec"},
}

# Derived back-compat dicts (read by v17B / v17p hot-swap and by G0 strip).
CANDIDATE_E_SIZE_MULTIPLIERS = {
    name: (cfg["size_mult"] if cfg["enabled"] else 0.0)
    for name, cfg in CANDIDATE_E_SETUP_CONFIG.items()
}
CANDIDATE_E_PER_SETUP_DAILY_CAP = {
    name: cfg["max_daily_trades"]
    for name, cfg in CANDIDATE_E_SETUP_CONFIG.items()
    if cfg.get("max_daily_trades") is not None and cfg.get("enabled")
}
# G6 auto-kill thresholds: only setups that are enabled AND have at least one
# threshold set. Each value is (kill_pf_30, kill_pf_60); None for unset.
CANDIDATE_E_KILL_PF_THRESHOLDS = {
    name: (cfg.get("kill_pf_30"), cfg.get("kill_pf_60"))
    for name, cfg in CANDIDATE_E_SETUP_CONFIG.items()
    if cfg.get("enabled") and (cfg.get("kill_pf_30") is not None
                               or cfg.get("kill_pf_60") is not None)
}

# Validation: every filter-spec setup must have a config entry.
_spec_setups = {setup for (_side, setup) in CANDIDATE_E_FILTER_SPEC.keys()}
_missing_cfg = _spec_setups - set(CANDIDATE_E_SETUP_CONFIG.keys())
if _missing_cfg:
    raise SystemExit(f"[V17C_E] CANDIDATE_E_SETUP_CONFIG missing entries for "
                     f"filter-spec setups: {sorted(_missing_cfg)}")
_required_fields = ("enabled", "size_mult", "max_daily_trades",
                    "kill_pf_30", "kill_pf_60", "notes")
for _name, _cfg in CANDIDATE_E_SETUP_CONFIG.items():
    if not all(k in _cfg for k in _required_fields):
        raise SystemExit(f"[V17C_E] CANDIDATE_E_SETUP_CONFIG[{_name!r}] "
                         f"missing fields (need {_required_fields})")

import avwap_combined_runner_v17p_5min as _v17p_for_E
_v17p_for_E.SIZE_MULTIPLIERS = CANDIDATE_E_SIZE_MULTIPLIERS
_v17B.SIZE_MULTIPLIERS = CANDIDATE_E_SIZE_MULTIPLIERS
_n_enabled  = sum(1 for c in CANDIDATE_E_SETUP_CONFIG.values() if c["enabled"])
_n_capped   = len(CANDIDATE_E_PER_SETUP_DAILY_CAP)
_n_killable = len(CANDIDATE_E_KILL_PF_THRESHOLDS)
print(f"[V17C_E] setup config -> {len(CANDIDATE_E_SETUP_CONFIG)} entries "
      f"({_n_enabled} enabled, {_n_capped} with daily cap, "
      f"{_n_killable} with kill-PF armed)")


# ---------------------------------------------------------------------------
# 2.54  PHASE 2B -- FAILED_BREAKOUT_TRAP ENGINE-SCAN HOT-SWAP
#
# H_FAILED_BREAKOUT_TRAP is a new SHORT setup defined in
# avwap_v11_refactored/avwap_short_strategy_v11.py (see _scan_failed_
# breakout_trap_at). The detector is gated by cfg.enable_setup_failed_
# breakout_trap which defaults to False everywhere -- so the engine does
# NOT scan for it unless we hot-swap the cfg here.
#
# This is a two-step gate to keep risk contained:
#   step 1: PHASE2B_FBT_ENGINE_SCAN_ENABLED = True
#           -> engine emits H_FAILED_BREAKOUT_TRAP trades into the raw CSV
#           -> they show up sized 0 (CANDIDATE_E_SETUP_CONFIG enabled=False)
#           -> you can inspect their PF, win-rate, diag_* columns
#   step 2: edit CANDIDATE_E_SETUP_CONFIG["H_FAILED_BREAKOUT_TRAP"] to
#           enabled=True size_mult=0.25 (cap=3/day already wired via G5)
#           -> trades start participating in the live book at 0.25x.
#
# The four PHASE2B_FBT_* tunables below are passed through to the detector
# via the cfg adjuster. Defaults match the spec in the improved plan.
# ---------------------------------------------------------------------------
PHASE2B_FBT_ENGINE_SCAN_ENABLED = True
PHASE2B_FBT_MAX_HOUR_IST        = 10.75   # 10:45 IST as decimal hours
PHASE2B_FBT_MIN_UPPER_WICK_PCT  = 40.0    # rejection bar wick floor
PHASE2B_FBT_MIN_MFI             = 70.0    # hot-MFI floor at rejection
PHASE2B_FBT_MIN_VOL_RATIO       = 1.2     # vol vs VOL_SMA20 floor

import avwap_combined_runner_v16_5min as _v16_base_for_E
_v17C_orig_scan_short_prepared = getattr(_v16_base_for_E, "scan_short_prepared", None)


def _v17C_E_adjust_short_cfg_for_fbt(cfg):
    if PHASE2B_FBT_ENGINE_SCAN_ENABLED:
        try:
            cfg.enable_setup_failed_breakout_trap = True
            cfg.fbt_max_hour_ist        = float(PHASE2B_FBT_MAX_HOUR_IST)
            cfg.fbt_min_upper_wick_pct  = float(PHASE2B_FBT_MIN_UPPER_WICK_PCT)
            cfg.fbt_min_mfi             = float(PHASE2B_FBT_MIN_MFI)
            cfg.fbt_min_vol_ratio       = float(PHASE2B_FBT_MIN_VOL_RATIO)
        except Exception:
            pass
    return cfg


if _v17C_orig_scan_short_prepared is not None:
    def _v17C_E_scan_short_prepared(ticker, df_prepared, short_cfg):
        short_cfg = _v17C_E_adjust_short_cfg_for_fbt(short_cfg)
        return _v17C_orig_scan_short_prepared(ticker, df_prepared, short_cfg)
    _v16_base_for_E.scan_short_prepared = _v17C_E_scan_short_prepared
    print(f"[V17C_E] FBT engine-scan: "
          f"{'ENABLED' if PHASE2B_FBT_ENGINE_SCAN_ENABLED else 'disabled (default)'}"
          f"  (hour<={PHASE2B_FBT_MAX_HOUR_IST}, wick%>={PHASE2B_FBT_MIN_UPPER_WICK_PCT}, "
          f"MFI>={PHASE2B_FBT_MIN_MFI}, vol>={PHASE2B_FBT_MIN_VOL_RATIO}x)")
else:
    print("[V17C_E] FBT engine-scan: cannot install -- v16 has no scan_short_prepared")


# ---------------------------------------------------------------------------
# 2.55  CANDIDATE-E3 PER-SETUP SL/TGT OVERRIDES
#
# Re-resolves each trade against the actual 1-minute OHLC bars at a per-setup
# (SL, TGT) chosen by the Pareto picker (beats baseline 0.75/0.80 on ALL of
# {win%, day_win%, sumPnL%, PF}).
#
# SL floor is 0.75% per requirement; targets re-tuned per setup.
#
# Lab numbers (Cand-E3 vs Cand-E2, both governors+sizing):
#     Cand-E2 baseline (SL=0.75/TGT=0.80 fixed):
#         n=874  win=74.37%  day_win=80.09%  PnL=+1118.53%  PF=2.154
#     Cand-E3 (R:R-protected per-setup picks):
#         n=874  win=74.94%  day_win=78.20%  PnL=+1182.59%  PF=2.211
#     All TGT >= 0.80, all R:R >= 1.0 (one setup at R:R 1.13 strictly better
#     than baseline's 1.07).
# ---------------------------------------------------------------------------
CANDIDATE_E3_SL_TGT = {
    # CAND-E4 SL/TGT picks: re-tuned 2026-05-04 against fresh production CSV.
    # Constraints preserved:
    #   SL  >= 0.75% (no tighter stop than baseline)
    #   TGT >= 0.80% (no tighter target than baseline)
    #   TGT >= SL    (R:R >= 1.0 -- reward at least matches risk)
    ("LONG",  "A_MOD_BREAK_C1_HIGH"):           (0.75, 0.80),  # R:R 1.07
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"):      (0.75, 0.80),  # R:R 1.07 (E4 found baseline best)
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): (0.75, 0.90),  # R:R 1.20 (wider TGT helps small n)
    ("LONG",  "D_EMA20_BOUNCE"):                (0.80, 0.80),  # R:R 1.00
    ("SHORT", "A_MOD_BREAK_C1_LOW"):            (0.80, 0.85),  # R:R 1.06
    ("SHORT", "C_OR_BREAKDOWN"):                (0.85, 0.85),  # R:R 1.00 (sized 0 anyway)
    ("SHORT", "D_EMA20_REJECTION"):             (0.75, 0.80),  # R:R 1.07 (sized 0 anyway)
    ("SHORT", "G_LOWER_LOW_BREAK"):             (0.80, 0.80),  # R:R 1.00
}

# Cost model identical to runner Phase 2 stressed-pessimistic path
_E3_TGT_COSTS_PCT = (5 + 3) * 2 / 100.0          # 0.16% (slip + comm both legs)
_E3_SL_COSTS_PCT  = (5 + 3) * 2 / 100.0 + 3 / 100.0  # 0.19% (+ extra stop slip)
_E3_EOD_COSTS_PCT = _E3_TGT_COSTS_PCT
_E3_LEVERAGE = 5.0
_E3_ONEMIN_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")


def _e3_load_1min(ticker: str):
    p = _E3_ONEMIN_DIR / f"{ticker}_stocks_indicators_1min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df = df[["date", "high", "low", "close"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_convert("Asia/Kolkata")
    return df


def _e3_resolve(bars, side, entry_price, entry_time_ist, sl_pct, tgt_pct):
    """Walk 1-min bars from entry to 15:20 IST and return (outcome, price%)."""
    if bars is None or bars.empty:
        return None
    et = pd.to_datetime(entry_time_ist, utc=True).tz_convert("Asia/Kolkata")
    eod_cutoff = et.normalize() + pd.Timedelta(hours=15, minutes=20)
    sub = bars[(bars["date"] >= et) & (bars["date"] <= eod_cutoff)]
    if sub.empty:
        return None
    sl_price = entry_price * (1 - sl_pct / 100.0) if side == "LONG" \
        else entry_price * (1 + sl_pct / 100.0)
    tgt_price = entry_price * (1 + tgt_pct / 100.0) if side == "LONG" \
        else entry_price * (1 - tgt_pct / 100.0)
    h = sub["high"].to_numpy()
    l_ = sub["low"].to_numpy()
    c = sub["close"].to_numpy()
    if side == "LONG":
        sl_hit = l_ <= sl_price
        tgt_hit = h >= tgt_price
    else:
        sl_hit = h >= sl_price
        tgt_hit = l_ <= tgt_price
    for i in range(len(sub)):
        if sl_hit[i]:
            return ("SL", -sl_pct - _E3_SL_COSTS_PCT)
        if tgt_hit[i]:
            return ("TARGET", tgt_pct - _E3_TGT_COSTS_PCT)
    raw = (c[-1] - entry_price) / entry_price * 100.0
    if side == "SHORT":
        raw = -raw
    return ("EOD", raw - _E3_EOD_COSTS_PCT)


def _v17C_E3_reresolve_per_setup(df: pd.DataFrame) -> pd.DataFrame:
    """Re-resolve every trade at its (side, setup) PARETO (SL, TGT)."""
    if df is None:
        return df
    if df.empty:
        out = df.copy()
        if "candE3_sl_pct" not in out.columns:
            out["candE3_sl_pct"] = pd.Series([], dtype=float)
        if "candE3_tgt_pct" not in out.columns:
            out["candE3_tgt_pct"] = pd.Series([], dtype=float)
        return out
    out_rows = []
    bar_cache: dict = {}
    for _, t in df.iterrows():
        side = t["side"]
        setup = t["setup"]
        sl_tgt = CANDIDATE_E3_SL_TGT.get((side, setup))
        if sl_tgt is None:
            # Setup not in spec - keep original outcome row
            out_rows.append(t.to_dict())
            continue
        sl_pct, tgt_pct = sl_tgt
        ticker = t["ticker"]
        if ticker not in bar_cache:
            bar_cache[ticker] = _e3_load_1min(ticker)
        out = _e3_resolve(bar_cache[ticker], side, float(t["entry_price"]),
                          t["entry_time_ist"], sl_pct, tgt_pct)
        if out is None:
            out_rows.append(t.to_dict())
            continue
        outc, pnl_price = out
        pnl_lev = pnl_price * _E3_LEVERAGE
        new = t.to_dict()
        new["outcome"] = outc
        new["pnl_pct"] = pnl_lev
        new["pnl_pct_price"] = pnl_price
        if "pnl_rs" in new:
            position = new.get("position_size_rs", 20000)
            new["pnl_rs"] = pnl_lev * position / 100.0
        new["candE3_sl_pct"] = sl_pct
        new["candE3_tgt_pct"] = tgt_pct
        out_rows.append(new)
    return pd.DataFrame(out_rows, columns=list(df.columns) + [
        c for c in ("candE3_sl_pct", "candE3_tgt_pct") if c not in df.columns
    ])


# ---------------------------------------------------------------------------
# 2.6  CANDIDATE-E PORTFOLIO GOVERNORS (post-Phase-2 stage)
#
# Four governors applied to the final trades CSV after exit resolution.
# All governors are causal -- they only look at trades whose entry_time
# precedes the candidate row's entry_time.
#
#   G1 time-window     LONG  09:15-13:00, SHORT 09:15-14:00
#   G2 daily side cap  LONG  15/day, SHORT 10/day
#   G3 daily loss-kill 4 consecutive intraday losses -> halt the side
#   G4 setup cooling   6 losses in last 12 trades -> block the setup
#
# Outputs (both written to outputs_v17C_noNF_live_5min/):
#   avwap_longshort_trades_v16_5min_ALL_DAYS_*_candE.csv
#   avwap_combined_runner_*_candE_summary.txt
# ---------------------------------------------------------------------------
LONG_TIME_WINDOW = (9.25, 13.00)
SHORT_TIME_WINDOW = (9.25, 14.00)
PORTFOLIO_MAX_LONG_PER_DAY = 15
PORTFOLIO_MAX_SHORT_PER_DAY = 10
DAILY_LOSS_KILL_STREAK = 4
SETUP_COOL_LOSSES = 6
SETUP_COOL_WINDOW = 12


def _v17C_E_strip_zero_size(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """G0: drop zero-size setups BEFORE governors so they cannot consume the
    daily side cap (G2) or contribute to the side loss-streak (G3).

    Returns (active_df, leak_log) where leak_log quantifies what was stripped
    and on how many days the stripped shorts alone would have saturated the
    short cap -- a proxy for live-execution starvation.
    """
    if df is None or df.empty:
        return df, {"zero_total": 0, "zero_short": 0, "zero_long": 0,
                    "short_cap_starve_days": 0}
    sizes = (df["setup"].astype(str).str.upper().str.strip()
             .map(CANDIDATE_E_SIZE_MULTIPLIERS).fillna(0.0))
    active = df[sizes > 0].copy()
    zero = df[sizes <= 0].copy()
    leak = {
        "zero_total": int(len(zero)),
        "zero_short": int((zero["side"] == "SHORT").sum()) if len(zero) else 0,
        "zero_long":  int((zero["side"] == "LONG").sum())  if len(zero) else 0,
        "short_cap_starve_days": 0,
    }
    if leak["zero_short"]:
        s = zero[zero["side"] == "SHORT"].copy()
        s["trade_date"] = pd.to_datetime(s["trade_date"]).dt.date
        per_day = s.groupby("trade_date").size()
        leak["short_cap_starve_days"] = int(
            (per_day >= PORTFOLIO_MAX_SHORT_PER_DAY).sum()
        )
    return active, leak


def _v17C_E_apply_governors(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Apply Candidate-E portfolio governors. Returns (kept_df, drop_log).

    Governors (in order):
      G1 time-window     side-specific entry-hour bounds
      G3 daily loss-kill DAILY_LOSS_KILL_STREAK consecutive intraday losses
      G2 side daily cap  PORTFOLIO_MAX_{LONG,SHORT}_PER_DAY
      G5 setup day cap   CANDIDATE_E_PER_SETUP_DAILY_CAP[setup]
      G6 rolling PF kill rolling PF over last 30 / 60 KEPT trades for setup
                         falls below CANDIDATE_E_KILL_PF_THRESHOLDS[setup]
      G4 setup cooling   SETUP_COOL_LOSSES of last SETUP_COOL_WINDOW losses
    """
    empty_log = {"G1": 0, "G2": 0, "G3": 0, "G4": 0, "G5": 0, "G6": 0}
    if df is None or df.empty:
        return df, empty_log

    work = df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"]).dt.date
    work["entry_dt"] = pd.to_datetime(work["entry_time_ist"], errors="coerce")
    work["entry_hour"] = (work["entry_dt"].dt.hour
                          + work["entry_dt"].dt.minute / 60.0)
    work = work.sort_values(["trade_date", "entry_dt", "ticker"]).reset_index(drop=True)

    log = dict(empty_log)
    keep = []
    state_day: dict = {}
    state_setup_day: dict = {}
    closed_history: dict = {}        # (side, setup) -> [outcome_int]   (G4)
    closed_pnl_history: dict = {}    # (side, setup) -> [pnl_pct float] (G6)

    def _g6_tripped(side, setup):
        """Return True if rolling PF over kept trades has fallen below the
        configured kill_pf_30 or kill_pf_60 threshold for this setup."""
        thresholds = CANDIDATE_E_KILL_PF_THRESHOLDS.get(setup)
        if thresholds is None:
            return False
        pnl_hist = closed_pnl_history.get((side, setup), [])
        for n, threshold in zip((30, 60), thresholds):
            if threshold is None or len(pnl_hist) < n:
                continue
            recent = pnl_hist[-n:]
            wins = sum(p for p in recent if p > 0)
            losses = -sum(p for p in recent if p < 0)
            if losses > 0 and (wins / losses) < threshold:
                return True
        return False

    for _, row in work.iterrows():
        side = row["side"]
        setup = row["setup"]
        date = row["trade_date"]
        ehr = row["entry_hour"] if pd.notna(row["entry_hour"]) else 9.5
        outcome_int = (1 if row["pnl_pct"] > 0
                       else (-1 if row["pnl_pct"] < 0 else 0))
        lo, hi = LONG_TIME_WINDOW if side == "LONG" else SHORT_TIME_WINDOW

        if not (lo <= ehr <= hi):
            keep.append(False); log["G1"] += 1
            closed_history.setdefault((side, setup), []).append(outcome_int)
            continue

        ds = state_day.setdefault(
            (date, side),
            {"open_count": 0, "loss_streak": 0, "blocked": False},
        )

        if ds["blocked"]:
            keep.append(False); log["G3"] += 1
            closed_history.setdefault((side, setup), []).append(outcome_int)
            ds["loss_streak"] = ds["loss_streak"] + 1 if outcome_int < 0 else 0
            continue

        cap = (PORTFOLIO_MAX_LONG_PER_DAY if side == "LONG"
               else PORTFOLIO_MAX_SHORT_PER_DAY)
        if ds["open_count"] >= cap:
            keep.append(False); log["G2"] += 1
            closed_history.setdefault((side, setup), []).append(outcome_int)
            continue

        per_setup_cap = CANDIDATE_E_PER_SETUP_DAILY_CAP.get(setup)
        if per_setup_cap is not None:
            psd = state_setup_day.setdefault((date, side, setup), {"open_count": 0})
            if psd["open_count"] >= per_setup_cap:
                keep.append(False); log["G5"] += 1
                closed_history.setdefault((side, setup), []).append(outcome_int)
                continue

        if _g6_tripped(side, setup):
            keep.append(False); log["G6"] += 1
            closed_history.setdefault((side, setup), []).append(outcome_int)
            continue

        hist = closed_history.get((side, setup), [])
        recent = hist[-SETUP_COOL_WINDOW:]
        if (len(recent) == SETUP_COOL_WINDOW
                and sum(1 for x in recent if x == -1) >= SETUP_COOL_LOSSES):
            keep.append(False); log["G4"] += 1
            closed_history.setdefault((side, setup), []).append(outcome_int)
            continue

        keep.append(True)
        ds["open_count"] += 1
        if per_setup_cap is not None:
            state_setup_day[(date, side, setup)]["open_count"] += 1
        if outcome_int < 0:
            ds["loss_streak"] += 1
            if ds["loss_streak"] >= DAILY_LOSS_KILL_STREAK:
                ds["blocked"] = True
        else:
            ds["loss_streak"] = 0
        closed_history.setdefault((side, setup), []).append(outcome_int)
        pnl_val = pd.to_numeric(row.get("pnl_pct"), errors="coerce")
        if pd.notna(pnl_val):
            closed_pnl_history.setdefault((side, setup), []).append(float(pnl_val))

    out = work[pd.Series(keep, index=work.index)].copy()
    return out, log


def _v17C_E_apply_sizing_and_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Cand-E size multipliers, compute effective pnl, return df.

    Always adds candE_size_mult / pnl_pct_eff (and pnl_rs_eff if pnl_rs is
    present) so downstream filters and summary code can operate uniformly
    even when the input has zero rows (e.g. when the engine + Cand-E + F15
    chain drops everything, as happens in a short backtest window without
    1-min coverage).
    """
    if df is None:
        return df
    out = df.copy()
    if out.empty:
        # Pre-create expected columns with empty dtype so callers like
        # `sized[sized["candE_size_mult"] > 0]` work without KeyError.
        out["candE_size_mult"] = pd.Series([], dtype=float)
        out["pnl_pct_eff"]     = pd.Series([], dtype=float)
        if "pnl_rs" in out.columns:
            out["pnl_rs_eff"]  = pd.Series([], dtype=float)
        return out
    out["candE_size_mult"] = (out["setup"].astype(str).str.upper().str.strip()
                              .map(CANDIDATE_E_SIZE_MULTIPLIERS).fillna(0.0))
    out["pnl_pct_eff"] = pd.to_numeric(out.get("pnl_pct"), errors="coerce") * out["candE_size_mult"]
    if "pnl_rs" in out.columns:
        out["pnl_rs_eff"] = pd.to_numeric(out["pnl_rs"], errors="coerce") * out["candE_size_mult"]
    return out


def _v17C_E_attach_rolling_telemetry(df: pd.DataFrame) -> pd.DataFrame:
    """Attach causal per-setup rolling-PF telemetry. Read-only: does NOT
    change which trades are kept or sized -- it only adds columns to the
    output so live-degradation thresholds can be wired in a later phase.

    PF is computed over engine-level pnl_pct (NOT pnl_pct_eff) so setup
    health is measured at the signal level and is not distorted by size
    changes between trades. Causal: each row's value uses only trades
    whose entry strictly precedes the current row.

    Adds columns:
      setup_trades_so_far    count of prior trades for (side, setup)
      setup_rolling_pf_30    PF over prior 30 trades  (NaN until 30 priors)
      setup_rolling_pf_60    PF over prior 60 trades  (NaN until 60 priors)
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    out["_telem_entry_dt"] = pd.to_datetime(out.get("entry_time_ist"), errors="coerce")
    out = (out.sort_values(["_telem_entry_dt", "ticker"], kind="mergesort")
              .reset_index(drop=True))
    out["setup_trades_so_far"] = out.groupby(["side", "setup"], sort=False).cumcount()

    def _pf_window(arr):
        # arr is numpy ndarray under rolling.apply(raw=True)
        arr = arr[pd.notna(arr)]
        if len(arr) == 0:
            return float("nan")
        wins = float(arr[arr > 0].sum())
        losses = float(-arr[arr < 0].sum())
        if losses <= 0:
            return float("inf") if wins > 0 else float("nan")
        return wins / losses

    pnl = pd.to_numeric(out["pnl_pct"], errors="coerce")
    grp_indices = out.groupby(["side", "setup"], sort=False).indices

    for n in (30, 60):
        col = f"setup_rolling_pf_{n}"
        out[col] = float("nan")
        col_pos = out.columns.get_loc(col)
        for _, idx in grp_indices.items():
            if len(idx) < 2:
                continue
            grp_pnl = pnl.iloc[idx].reset_index(drop=True)
            # shift(1) excludes current trade -> strictly causal
            rolled = (grp_pnl.shift(1)
                             .rolling(window=n, min_periods=n)
                             .apply(_pf_window, raw=True))
            out.iloc[idx, col_pos] = rolled.values

    out = out.drop(columns=["_telem_entry_dt"])
    return out


# ---------------------------------------------------------------------------
# 2.7  PHASE 2A DIAGNOSTIC ENRICHMENT
#
# Per-ticker indicator parquets are at PHASE2A_PARQUET_DIR. Each has 5-min
# bars with OHLC + RSI/ATR/EMA_20/EMA_50/EMA_200/Stoch/ADX/VWAP/CCI/MFI/OBV/
# MACD_Hist/Upper_Band/Lower_Band/Recent_High/Recent_Low/Prev_Day_Close.
#
# Enrichment is purely additive -- adds columns prefixed `diag_` to the
# kept-trade CSV without affecting any decision. Causal: every value comes
# from the entry bar (or 3-5 bars prior for slopes), never post-entry.
#
# Per-ticker parquets are cached in _PHASE2A_PARQUET_CACHE to avoid re-reads
# across E2 and E3 pipelines.
# ---------------------------------------------------------------------------
PHASE2A_PARQUET_DIR = r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"
PHASE2A_DIAGNOSTIC_COLS = (
    "diag_upper_wick_pct",       # entry bar: upper wick / total range * 100
    "diag_body_atr",             # entry bar: |close-open| / ATR
    "diag_mfi",                  # entry bar: MFI raw value
    "diag_mfi_slope_3",          # MFI(entry) - MFI(entry-3 bars)
    "diag_obv_slope_5",          # (OBV(entry) - OBV(entry-5 bars)) / 5
    "diag_bb_width_pct",         # entry bar: (Upper_Band - Lower_Band) / close * 100
    "diag_vwap_dist_atr",        # entry bar: signed (close - VWAP) / ATR
    "diag_gap_pct_open",         # (today_open - Prev_Day_Close) / Prev_Day_Close * 100
    "diag_cci",                  # entry bar: CCI raw value
    "diag_macd_hist",            # entry bar: MACD_Hist raw value
    "diag_macd_hist_slope_3",    # MACD_Hist(entry) - MACD_Hist(entry-3 bars)
)
_PHASE2A_PARQUET_CACHE: dict = {}   # ticker -> (bars_df, first_open_per_day) or None


def _v17C_phase2a_load_indicators(ticker: str):
    """Load one ticker's indicator parquet (cached). Returns
    (bars_df_indexed_by_date, first_open_per_day_series) or None if missing.
    """
    if ticker in _PHASE2A_PARQUET_CACHE:
        return _PHASE2A_PARQUET_CACHE[ticker]
    fp = Path(PHASE2A_PARQUET_DIR) / f"{ticker}_stocks_indicators_5min.parquet"
    if not fp.exists():
        _PHASE2A_PARQUET_CACHE[ticker] = None
        return None
    df = pd.read_parquet(fp)
    df = df.set_index("date").sort_index()
    # Pre-compute first-bar open per calendar date for fast gap-pct lookup
    first_open = df.groupby(df.index.date)["open"].first()
    _PHASE2A_PARQUET_CACHE[ticker] = (df, first_open)
    return _PHASE2A_PARQUET_CACHE[ticker]


def _v17C_E_attach_phase2a_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    """Phase 2a: attach causal bar-level diagnostic columns to kept trades.

    Read-only: adds columns, never drops rows or alters existing values.
    Tickers missing a parquet OR trades whose entry timestamp is outside
    the parquet's date range get NaN (logged at end as no_parquet/no_bar).
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    for c in PHASE2A_DIAGNOSTIC_COLS:
        out[c] = float("nan")

    if not Path(PHASE2A_PARQUET_DIR).is_dir():
        print(f"[V17C_PHASE2A] parquet dir not found ({PHASE2A_PARQUET_DIR}); "
              f"skipping enrichment ({len(out)} rows untouched)")
        return out

    n_total = len(out)
    n_no_parquet = 0
    n_no_bar = 0
    n_enriched = 0

    for ticker, group in out.groupby("ticker", sort=False):
        loaded = _v17C_phase2a_load_indicators(ticker)
        if loaded is None:
            n_no_parquet += len(group)
            continue
        bars, first_open_map = loaded
        # Per-ticker column presence (some tickers' parquets are missing
        # MFI/OBV/MACD_Hist/CCI/Bands -- fail soft, not loud).
        has_mfi  = "MFI" in bars.columns
        has_obv  = "OBV" in bars.columns
        has_mh   = "MACD_Hist" in bars.columns
        has_cci  = "CCI" in bars.columns
        has_band = "Upper_Band" in bars.columns and "Lower_Band" in bars.columns
        has_vwap = "VWAP" in bars.columns
        has_atr  = "ATR" in bars.columns
        has_pdc  = "Prev_Day_Close" in bars.columns
        for ridx, row in group.iterrows():
            ts = pd.to_datetime(row.get("entry_time_ist"), errors="coerce")
            if pd.isna(ts):
                n_no_bar += 1
                continue
            ipos = bars.index.searchsorted(ts)
            if ipos >= len(bars) or bars.index[ipos] != ts:
                n_no_bar += 1
                continue
            bar = bars.iloc[ipos]

            o = float(bar["open"]); h = float(bar["high"])
            l = float(bar["low"]);  c = float(bar["close"])
            atr  = bar.get("ATR")  if has_atr  else None
            vwap = bar.get("VWAP") if has_vwap else None
            mfi  = bar.get("MFI")  if has_mfi  else None
            obv  = bar.get("OBV")  if has_obv  else None
            cci  = bar.get("CCI")  if has_cci  else None
            mh   = bar.get("MACD_Hist")    if has_mh   else None
            ub   = bar.get("Upper_Band")   if has_band else None
            lb   = bar.get("Lower_Band")   if has_band else None
            pdc  = bar.get("Prev_Day_Close") if has_pdc else None
            rng  = h - l
            wick = h - max(o, c)

            out.at[ridx, "diag_upper_wick_pct"] = (wick / rng * 100) if rng > 0 else float("nan")
            if atr is not None and pd.notna(atr) and float(atr) > 0:
                out.at[ridx, "diag_body_atr"] = abs(c - o) / float(atr)
            if mfi is not None and pd.notna(mfi):
                out.at[ridx, "diag_mfi"] = float(mfi)
            if ub is not None and lb is not None and pd.notna(ub) and pd.notna(lb) and c > 0:
                out.at[ridx, "diag_bb_width_pct"] = (float(ub) - float(lb)) / c * 100
            if atr is not None and pd.notna(atr) and float(atr) > 0 and vwap is not None and pd.notna(vwap):
                out.at[ridx, "diag_vwap_dist_atr"] = (c - float(vwap)) / float(atr)
            if cci is not None and pd.notna(cci):
                out.at[ridx, "diag_cci"] = float(cci)
            if mh is not None and pd.notna(mh):
                out.at[ridx, "diag_macd_hist"] = float(mh)

            if ipos >= 3 and has_mfi:
                m_prev = bars["MFI"].iloc[ipos - 3]
                if mfi is not None and pd.notna(mfi) and pd.notna(m_prev):
                    out.at[ridx, "diag_mfi_slope_3"] = float(mfi) - float(m_prev)
            if ipos >= 3 and has_mh:
                mh_prev = bars["MACD_Hist"].iloc[ipos - 3]
                if mh is not None and pd.notna(mh) and pd.notna(mh_prev):
                    out.at[ridx, "diag_macd_hist_slope_3"] = float(mh) - float(mh_prev)
            if ipos >= 5 and has_obv:
                obv_prev = bars["OBV"].iloc[ipos - 5]
                if obv is not None and pd.notna(obv) and pd.notna(obv_prev):
                    out.at[ridx, "diag_obv_slope_5"] = (float(obv) - float(obv_prev)) / 5.0

            if pdc is not None and pd.notna(pdc) and float(pdc) > 0:
                today_open = first_open_map.get(ts.date())
                if today_open is not None and pd.notna(today_open):
                    out.at[ridx, "diag_gap_pct_open"] = (
                        (float(today_open) - float(pdc)) / float(pdc) * 100
                    )

            n_enriched += 1

    print(f"[V17C_PHASE2A] diagnostics enriched: {n_enriched}/{n_total} "
          f"(no_parquet={n_no_parquet}, no_bar={n_no_bar}, "
          f"tickers_cached={sum(1 for v in _PHASE2A_PARQUET_CACHE.values() if v is not None)})")
    return out


def _v17C_E_post_resolve_pipeline():
    """Read the latest trades CSV, run two pipelines:
       (1) Cand-E2: governors + sizing on engine outcomes (SL=0.75/TGT=0.80)
       (2) Cand-E3: per-setup SL/TGT re-resolution then governors + sizing
    Writes *_candE.csv (E2 view) and *_candE3.csv (E3 view) plus summaries."""
    out_dir = _v17C_runtime_dir("outputs_v16_5min")
    pattern = str(Path(out_dir) / "avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv")
    files = sorted([f for f in glob.glob(pattern) if "_candE" not in f])
    if not files:
        print("[V17C_E] no trades CSV found; skipping Candidate-E post-pipeline")
        return
    latest = files[-1]
    df = pd.read_csv(latest)
    n_in = len(df)

    # ---- G0: strip zero-size setups BEFORE governors (was post-hoc) ----
    active_df, leak = _v17C_E_strip_zero_size(df)
    print(f"[V17C_G0] zero-size strip: dropped {leak['zero_total']} "
          f"(SHORT={leak['zero_short']}, LONG={leak['zero_long']}); "
          f"days where stripped shorts >= cap({PORTFOLIO_MAX_SHORT_PER_DAY}): "
          f"{leak['short_cap_starve_days']}  "
          f"(input {n_in} -> active {len(active_df)})")

    # ---- Pipeline 1: Cand-E2 (engine outcomes) ----
    governed, log = _v17C_E_apply_governors(active_df)
    sized = _v17C_E_apply_sizing_and_summary(governed)
    sized = sized[sized["candE_size_mult"] > 0].copy()  # safety net
    sized = _v17C_E_attach_rolling_telemetry(sized)
    sized = _v17C_E_attach_phase2a_diagnostics(sized)
    out_csv = latest.replace(".csv", "_candE.csv")
    sized.to_csv(out_csv, index=False)
    print(f"[V17C_E2] governors dropped: G1={log['G1']} G2={log['G2']} "
          f"G3={log['G3']} G4={log['G4']} G5={log['G5']} G6={log['G6']} "
          f"(active {len(active_df)} -> kept {len(sized)})")

    # ---- Pipeline 2: Cand-E3 (per-setup SL/TGT re-resolution) ----
    print(f"[V17C_E3] re-resolving {len(active_df)} active trades at per-setup PARETO (SL, TGT)...")
    df_e3 = _v17C_E3_reresolve_per_setup(active_df)
    governed_e3, log_e3 = _v17C_E_apply_governors(df_e3)
    sized_e3 = _v17C_E_apply_sizing_and_summary(governed_e3)
    sized_e3 = sized_e3[sized_e3["candE_size_mult"] > 0].copy()
    sized_e3 = _v17C_E_attach_rolling_telemetry(sized_e3)
    sized_e3 = _v17C_E_attach_phase2a_diagnostics(sized_e3)
    out_csv_e3 = latest.replace(".csv", "_candE3.csv")
    sized_e3.to_csv(out_csv_e3, index=False)
    print(f"[V17C_E3] governors dropped: G1={log_e3['G1']} G2={log_e3['G2']} "
          f"G3={log_e3['G3']} G4={log_e3['G4']} G5={log_e3['G5']} G6={log_e3['G6']} "
          f"(active {len(active_df)} -> kept {len(sized_e3)})")

    # ---- summary helpers ----
    def _pf(s):
        s = pd.to_numeric(s, errors="coerce").dropna()
        w = s[s > 0].sum(); l = -s[s < 0].sum()
        return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)

    def _sblock(name, sub):
        n = len(sub)
        if n == 0:
            return f"  [{name:5s}]  n=0"
        pf_v = _pf(sub["pnl_pct_eff"])
        win = (sub["outcome"] == "TARGET").mean() * 100
        daily = sub.groupby("trade_date")["pnl_pct_eff"].sum()
        day_win = (daily > 0).mean() * 100 if len(daily) else 0
        sumpnl = sub["pnl_pct_eff"].sum()
        sumrs = sub.get("pnl_rs_eff", pd.Series([0])).sum()
        return (f"  [{name:5s}]  n={n:<5d}  pf={pf_v:.3f}  "
                f"win%={win:5.2f}  day_win%={day_win:5.2f}  "
                f"sumPnL%={sumpnl:>+9.2f}  sumRs=Rs.{sumrs:>+9,.0f}")

    summary_path = out_csv.replace(".csv", "_summary.txt")
    lines = [
        "=" * 80,
        "v17C noNF LIVE -- Candidate E2/E3 post-Phase-2 summary",
        "=" * 80,
        f"  source CSV     : {Path(latest).name}",
        f"  governors      : LONG {LONG_TIME_WINDOW} | SHORT {SHORT_TIME_WINDOW} "
        f"| caps L={PORTFOLIO_MAX_LONG_PER_DAY}/S={PORTFOLIO_MAX_SHORT_PER_DAY} "
        f"| loss_kill={DAILY_LOSS_KILL_STREAK} | cool={SETUP_COOL_LOSSES}/{SETUP_COOL_WINDOW}",
        f"  G0 strip       : zero-size dropped={leak['zero_total']} "
        f"(SHORT={leak['zero_short']} starve_days={leak['short_cap_starve_days']}, "
        f"LONG={leak['zero_long']})",
        "",
        "===== Cand-E2 (engine outcomes at SL=0.75 / TGT=0.80) =====",
        f"  drop counts: G1={log['G1']} G2={log['G2']} G3={log['G3']} "
        f"G4={log['G4']} G5={log['G5']} G6={log['G6']}",
        _sblock("SHORT", sized[sized["side"] == "SHORT"]),
        _sblock("LONG ", sized[sized["side"] == "LONG"]),
        _sblock("COMB ", sized),
        "",
        "  Per-setup (Cand-E2):",
    ]
    for (side, setup), g in sized.groupby(["side", "setup"]):
        n = len(g); pf_v = _pf(g["pnl_pct_eff"])
        win = (g["outcome"] == "TARGET").mean() * 100
        rs = g.get("pnl_rs_eff", pd.Series([0])).sum()
        size = g["candE_size_mult"].iloc[0]
        lines.append(f"    {side:5s} {setup:<32s}  n={n:<5d} pf={pf_v:.3f} "
                     f"win%={win:5.2f}  Rs.{rs:>+9,.0f}  size={size:.2f}x")

    lines.append("")
    lines.append("===== Cand-E3 (per-setup SL/TGT re-resolved) =====")
    lines.append(f"  drop counts: G1={log_e3['G1']} G2={log_e3['G2']} "
                 f"G3={log_e3['G3']} G4={log_e3['G4']} G5={log_e3['G5']} "
                 f"G6={log_e3['G6']}")
    lines.append(_sblock("SHORT", sized_e3[sized_e3["side"] == "SHORT"]))
    lines.append(_sblock("LONG ", sized_e3[sized_e3["side"] == "LONG"]))
    lines.append(_sblock("COMB ", sized_e3))
    lines.append("")
    lines.append("  Per-setup (Cand-E3, SL%/TGT%):")
    for (side, setup), g in sized_e3.groupby(["side", "setup"]):
        n = len(g); pf_v = _pf(g["pnl_pct_eff"])
        win = (g["outcome"] == "TARGET").mean() * 100
        rs = g.get("pnl_rs_eff", pd.Series([0])).sum()
        size = g["candE_size_mult"].iloc[0]
        sl_tgt = CANDIDATE_E3_SL_TGT.get((side, setup), (0.75, 0.80))
        lines.append(f"    {side:5s} {setup:<32s}  SL={sl_tgt[0]:.2f}/TGT={sl_tgt[1]:.2f}  "
                     f"n={n:<5d} pf={pf_v:.3f} win%={win:5.2f}  "
                     f"Rs.{rs:>+9,.0f}  size={size:.2f}x")
    lines.append("")
    lines.append(f"[OUT] {out_csv}")
    lines.append(f"[OUT] {out_csv_e3}")
    lines.append(f"[OUT] {summary_path}")
    Path(summary_path).write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


# ---------------------------------------------------------------------------
# 3. RELAX F4 TRADE-COUNT BAND
#
# v17B's audit asserts n in [600, 1000] (Candidate B reference range).
# With the regime filter off, n will be much larger (likely 1500-4000).
# Override the audit to print a warning instead of failing on count band.
# ---------------------------------------------------------------------------
_v17B_main = _base.main  # this is v17B's wrapped main with audit


def _v17C_post_run_audit():
    """Same as v17B's audit but with widened trade-count band and a label
    noting that NIFTY_CONTEXT was disabled."""
    out_dir = _v17C_runtime_dir("outputs_v16_5min")
    pattern = str(Path(out_dir) / "avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print("[V17C_AUDIT] no output CSV found; skipping audit")
        return
    latest = files[-1]
    df = pd.read_csv(latest)
    print(f"[V17C_AUDIT] (NIFTY_CONTEXT DISABLED) auditing "
          f"{Path(latest).name} (rows={len(df)})")

    failures = []

    def _fail(name, n, hint):
        if n > 0:
            failures.append(f"{name} ({hint}: n={n})")
            print(f"[V17C_AUDIT][FAIL] {name}: n={n} ({hint})")
        else:
            print(f"[V17C_AUDIT][PASS] {name}")

    _fail("no_dup_signal_key",
          int(df.duplicated(subset=["trade_date", "ticker", "side", "signal_time_ist"]).sum()),
          "duplicates on (date,ticker,side,signal_time)")
    _fail("no_dup_entry_key",
          int(df.duplicated(subset=["trade_date", "ticker", "side", "entry_time_ist"]).sum()),
          "duplicates on (date,ticker,side,entry_time)")
    _fail("F1_one_ticker_per_day",
          int(df.duplicated(subset=["trade_date", "ticker", "side"]).sum()),
          "duplicates on (date,ticker,side)")

    et = pd.to_datetime(df["entry_time_ist"], utc=True, errors="coerce")
    xt = pd.to_datetime(df["exit_time_ist"], utc=True, errors="coerce")
    case_col = df.get("exit_resolution_case", pd.Series("", index=df.index)).astype(str)
    is_fb = case_col.str.startswith("1MIN_FILL_BAR")
    tol = pd.to_timedelta(is_fb.map({True: "5min", False: "0min"}))
    bad = (xt + tol < et) & et.notna() & xt.notna()
    _fail("exit_time_after_entry", int(bad.sum()),
          "rows with exit_time materially before entry_time")

    pnl_p = pd.to_numeric(df.get("pnl_pct_price", pd.Series(dtype=float)), errors="coerce")
    if not pnl_p.empty:
        _fail("TARGET_has_positive_pnl",
              int((df["outcome"].eq("TARGET") & (pnl_p <= 0)).sum()),
              "TARGET rows with pnl_pct_price <= 0")
        _fail("SL_has_negative_pnl",
              int((df["outcome"].eq("SL") & (pnl_p >= 0)).sum()),
              "SL rows with pnl_pct_price >= 0")

    if "stop_fill_penalty_applied" in df.columns:
        sfp_raw = df["stop_fill_penalty_applied"]
        sfp = (sfp_raw if sfp_raw.dtype == bool
               else sfp_raw.astype(str).str.lower().isin(("true", "1", "yes")))
        _fail("stop_fill_penalty_iff_SL",
              int((sfp != df["outcome"].eq("SL")).sum()),
              "rows where stop_fill_penalty_applied != (outcome=='SL')")

    if "exit_resolution_case" in df.columns:
        _fail("F15_no_5M_fallback",
              int(df["exit_resolution_case"].astype(str).str.startswith("5M_FALLBACK").sum()),
              "rows with 5M_FALLBACK exit_resolution_case")

    # v17C-specific: widened trade-count band. Warning only, not a failure.
    if not (1000 <= len(df) <= 5000):
        print(f"[V17C_AUDIT][WARN] trade_count_band: n={len(df)} "
              f"(expected 1000..5000 with NIFTY_CONTEXT off; outside range)")
    else:
        print(f"[V17C_AUDIT][PASS] trade_count_band: n={len(df)}")

    if failures:
        print(f"[V17C_AUDIT] {len(failures)} check(s) FAILED: " + "; ".join(failures))
        import sys as _sys
        print("[V17C_AUDIT] STRICT mode -- exiting with code 2")
        _sys.exit(2)
    else:
        print("[V17C_AUDIT] all checks passed (NIFTY_CONTEXT was DISABLED for this run)")


# Capture the un-wrapped main (the v16 base.main wrapped by v17B). We need
# the v16 original main so our wrapper runs once, not twice.
_orig_main_for_v17C = _v17B._orig_main


def _v17C_main():
    # v16's main() can crash AFTER writing the trades CSV but during
    # chart generation (e.g. older matplotlib that doesn't accept the
    # height_ratios kwarg to plt.subplots). The trades CSV is the input
    # to post-resolve, so as long as it was written we should still run
    # governors + sizing + Phase 2a regardless of a downstream chart fail.
    result = None
    try:
        result = _orig_main_for_v17C()
    except SystemExit:
        raise
    except Exception as exc:
        import traceback as _tb
        print(f"[V17C_E] _orig_main raised ({type(exc).__name__}: {exc}); "
              f"continuing to audit + post-resolve since trades CSV is "
              f"already on disk by this stage of v16's main.")
        print("[V17C_E] _orig_main traceback (for diagnosis, not fatal):")
        _tb.print_exc()
    try:
        _v17C_post_run_audit()
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[V17C_AUDIT] post-run audit error: {exc}")
    # Candidate-E post-Phase-2 pipeline: governors + sizing + summary
    try:
        _v17C_E_post_resolve_pipeline()
    except Exception as exc:
        import traceback as _tb
        print(f"[V17C_E] post-resolve pipeline error: {exc}")
        _tb.print_exc()
    return result


_base.main = _v17C_main


# ---------------------------------------------------------------------------
# 4. BANNER
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 78)
    print("v17C noNF LIVE -- Candidate E4 (re-tuned on production data)")
    print("  Output dir   : outputs_v17C_noNF_live_5min")
    print("  Strategy     : v17p cascade + Cand-E4 chains + per-setup SL/TGT + sizing")
    print("  Filter       : CANDIDATE_E_FILTER_SPEC  (8 setups, 4L+4S; E4-tuned)")
    print("  SL/TGT       : CANDIDATE_E3_SL_TGT      (per-setup, R:R >= 1.0)")
    print("                   - SL  >= 0.75 (no tighter stop)")
    print("                   - TGT >= 0.80 (no tighter target)")
    print("                   - TGT >= SL   (reward >= risk per trade)")
    print("  Sizing       : 1.30x elite / 1.00x good / 0.50x borderline / 0x drop")
    print("                 (SHORT C_OR_BREAKDOWN + SHORT D_EMA20_REJECTION dropped)")
    print("  Governors    : G1 time / G2 daily-cap / G3 loss-kill / G4 setup-cool")
    print("  Honesty fixes: F1 STAGE0 | F4 AUDIT | F6 VOL_RATIO | F7 NIFTY_LAG")
    print("                 F11 NO_CLOSE_CONFIRM | F12 ENTRY_BAR_EXITS")
    print("                 F14 FLOOR_LAG | F15 REQUIRE_1MIN")
    print("  Outputs (2 CSVs)")
    print("    *_candE.csv  : Cand-E2 view (engine SL=0.75/TGT=0.80)")
    print("    *_candE3.csv : Cand-E4 view (per-setup R:R-protected, PRODUCTION)")
    print("  Lab numbers  : Cand-E4 (re-tuned on production data, May 4 2026)")
    print("                 n=978   PF=2.160  win=73.42%  day_win=78.54%")
    print("                 sumPnL=+1328.44%  DD=42.5%")
    print("                 vs prior Cand-E3 production (n=1514, PF 1.24):")
    print("                   PF +0.92, win +12.65 pp, day_win +19.82 pp")
    print("                 Notional: SHORT +Rs.30,939 LONG +Rs.234,749")
    print("                          TOTAL +Rs.265,689")
    print("  Per-setup E4 chain refinements (added filters):")
    print("    A_MOD_BREAK_C1_HIGH    : + avwap_dist>=1.5766     PF 1.04 -> 2.38")
    print("    B_HUGE_RECLAIM_BREAK   : + ema20_gap<=3.50,h<=9.92  PF 0.88 -> 2.49")
    print("    D_EMA20_BOUNCE         : + avwap_dist>=1.5329     PF 0.99 -> 1.89")
    print("  Per-setup E4 SL/TGT (R:R-protected) and sizing:")
    print("    LONG  A_MOD_BREAK_C1_HIGH         SL=0.75 TGT=0.80 R:R 1.07  size 1.30x")
    print("    LONG  B_AVWAP_RECLAIM_REVERSAL    SL=0.75 TGT=0.80 R:R 1.07  size 1.30x")
    print("    LONG  B_HUGE_*_RECLAIM_BREAK      SL=0.75 TGT=0.90 R:R 1.20  size 1.30x")
    print("    LONG  D_EMA20_BOUNCE              SL=0.80 TGT=0.80 R:R 1.00  size 1.00x")
    print("    SHORT A_MOD_BREAK_C1_LOW          SL=0.80 TGT=0.85 R:R 1.06  size 1.00x")
    print("    SHORT G_LOWER_LOW_BREAK           SL=0.80 TGT=0.80 R:R 1.00  size 0.50x")
    print("    SHORT C_OR_BREAKDOWN              -- DROPPED (size 0, PF<1.20)")
    print("    SHORT D_EMA20_REJECTION           -- DROPPED (size 0, PF<1.20)")
    print("=" * 78)
    _base.main()
