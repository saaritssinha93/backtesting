# -*- coding: utf-8 -*-
"""
v17r SETUP LAB RUNTIME -- v17p strategy + v17q honest-backtest fixes +
v17r per-setup candidate filter dispatch.

WHY THIS FILE EXISTS
====================
This is the runtime counterpart to _v17r_setup_lab_analyzer.py. It re-uses
v17t_live's complete cascade (v17p Stage 0/1/2 + 8 honesty fixes F1, F4,
F6, F7, F11, F12, F14, F15) and replaces v17t_live's Phase 5d AGGRESSIVE
filter chain with one of five v17r candidates selected via env var:

    EQIDV17R_CANDIDATE in {baseline, A, B, C, D, E}

`baseline` = no v17r filters (i.e., the unfiltered honest output).

Mutually exclusive with v17t_live's P5b/P5c/P5d toggles AND with v17q's
RUN5_OPTIMIZED/RUN5_PRO/RUN5_MAX. Module import will SystemExit on
collision.

Output: outputs_v17r_setup_lab_5min/
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Tuple, Dict, List

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Mutual-exclusion guard. v17r OWNS the post-scan filter slot. v17t's
# P5b/P5c/P5d default ON; we forcibly disable them BEFORE importing v17t_live
# so its deep filter never installs in the first place. If the user
# EXPLICITLY enabled any v17t/v17q filter via env, refuse to run.
# ---------------------------------------------------------------------------
def _env_str(name: str) -> str:
    raw = os.environ.get(name)
    return "" if raw is None else str(raw).strip().lower()


def _env_explicitly_on(name: str) -> bool:
    return _env_str(name) in ("1", "true", "yes", "on")


# v17q RUN5 family default OFF; only collide if user explicitly enabled.
# v17t P5 family default ON; we override to 0 before import. User-explicit
# True still collides.
_USER_FORBIDDEN_ON = (
    "EQIDV17T_DROP_LOSING_SETUPS",
    "EQIDV17T_PER_SETUP_FILTERS",
    "EQIDV17T_DEEP_FILTERS",
    "EQIDV17_RUN5_OPTIMIZED",
    "EQIDV17_RUN5_PRO",
    "EQIDV17_RUN5_MAX",
)
_collisions = [k for k in _USER_FORBIDDEN_ON if _env_explicitly_on(k)]
if _collisions:
    raise SystemExit(
        f"[V17R_SETUP_LAB] refusing to run: v17r owns the post-scan filter "
        f"slot, but the user explicitly enabled: {sorted(_collisions)}. "
        f"Unset or set them to 0 before running v17r."
    )

# Forcibly disable v17t_live's filters before its import sees them.
for _k in ("EQIDV17T_DROP_LOSING_SETUPS",
           "EQIDV17T_PER_SETUP_FILTERS",
           "EQIDV17T_DEEP_FILTERS"):
    os.environ[_k] = "0"

# Pull the full v17t_live stack (which itself imports v17p -> ... -> v16).
# With the three flags above forced to 0, the v17t chain installs only F1
# stage-0 hardening; P5b/P5c/P5d are skipped.
import avwap_combined_runner_v17t_live_5min as _v17t  # noqa: F401, E402
import avwap_combined_runner_v16_5min as _base  # noqa: E402


# ---------------------------------------------------------------------------
# Honesty-fix audit. All 8 must be ON.
# ---------------------------------------------------------------------------
_REQUIRED_HONESTY = (
    ("V17T_STAGE0_HARDEN", "F1"),
    ("V17T_AUDIT_STRICT",  "F4"),
    ("V17T_VOL_RATIO_NO_LOOKAHEAD",     "F6"),
    ("V17T_NIFTY_LOOKUP_PREV_BAR",      "F7"),
    ("V17T_NO_CLOSE_CONFIRM_LOOKAHEAD", "F11"),
    ("V17T_ENTRY_BAR_AWARE_EXITS",      "F12"),
    ("V17T_FLOOR_ZERO_LAG",             "F14"),
    ("V17T_REQUIRE_1MIN_EXITS",         "F15"),
)
_missing = []
for attr, code in _REQUIRED_HONESTY:
    if not getattr(_v17t, attr, False):
        _missing.append(f"{code}({attr})")
if _missing:
    raise SystemExit(
        f"[V17R_SETUP_LAB] honesty contract violated; the following fixes are OFF: "
        f"{_missing}. v17r refuses to run with any honesty fix disabled."
    )


# ---------------------------------------------------------------------------
# Output dir routing -> outputs_v17r_setup_lab_5min
# ---------------------------------------------------------------------------
_orig_runtime_dir = _base.runtime_dir


def _v17r_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17r_setup_lab_5min", "v17t_live_5min", "v17p_5min", "v17o_5min",
            "v17n_5min", "v17m_5min", "v17l_5min", "v17k_5min", "v17j_5min",
            "v17i_5min", "v17h_5min", "v17g_5min", "v17f_5min", "v17d_5min",
            "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17r_setup_lab_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17r_runtime_dir


# ---------------------------------------------------------------------------
# v17r CANDIDATE SPECS
#
# Each candidate is a dict mapping (side, setup) -> chain. A chain is a list
# of (feature, direction, threshold) constraints. Setups not in the dict
# are DROPPED (zero rows kept). The categorical literal
# `('nifty_context_mode', '==', 'LONG_OR_BOTH')` and `'SHORT_OR_BOTH'`
# is interpreted by _v17r_apply_chain to mean
# nifty_context_mode IN {LONG_ONLY, BOTH} (or {SHORT_ONLY, BOTH}).
#
# Auto-generated by _v17r_setup_lab_analyzer.py and pasted in verbatim so
# this file remains runnable without re-running the analyzer.
# ---------------------------------------------------------------------------
V17R_CANDIDATE_SPECS: Dict[str, Dict[Tuple[str, str], List]] = {
    "A": {
        # Minimal cleanup -- drop weak LONG (4) and weak SHORT (2) setups,
        # no per-setup filters.
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [],
        ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"): [],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [],
        ("SHORT", "C_OR_BREAKDOWN"): [],
        ("SHORT", "D_AVWAP_LOSE_REVERSAL"): [],
        ("SHORT", "D_EMA20_REJECTION"): [],
        ("SHORT", "E_VWAP_BAND_FADE"): [],
    },
    "B": {
        # Setup-wise practical filters -- best Stage-3 chain per setup.
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
            ("avwap_dist_atr_signal", ">=", 1.5260),
            ("entry_hour", "<=", 9.6667),
        ],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
            ("adx_signal", ">=", 34.1655),
        ],
        ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 1.5133),
            ("entry_hour", "<=", 9.9167),
        ],
        ("LONG",  "D_EMA20_BOUNCE"): [
            ("quality_score", ">=", 1.3833),
            ("ema20_gap_atr_signal", ">=", -2.1524),
            ("adx_signal", "<=", 37.6647),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("entry_hour", "<=", 10.0833),
            ("quality_score", ">=", 0.4577),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
    },
    "C": {
        # Regime-aware -- B + lagged NIFTY context guard. (Note: in the
        # 2025-06 -> 2026-04 sample BOTH dominates 95% of bars, so this
        # gate is mostly a no-op vs B; kept for symmetry.)
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
            ("avwap_dist_atr_signal", ">=", 1.5260),
            ("entry_hour", "<=", 9.6667),
            ("nifty_context_mode", "==", "LONG_OR_BOTH"),
        ],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
            ("adx_signal", ">=", 34.1655),
            ("nifty_context_mode", "==", "LONG_OR_BOTH"),
        ],
        ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 1.5133),
            ("entry_hour", "<=", 9.9167),
            ("nifty_context_mode", "==", "LONG_OR_BOTH"),
        ],
        ("LONG",  "D_EMA20_BOUNCE"): [
            ("quality_score", ">=", 1.3833),
            ("ema20_gap_atr_signal", ">=", -2.1524),
            ("adx_signal", "<=", 37.6647),
            ("nifty_context_mode", "==", "LONG_OR_BOTH"),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
            ("nifty_context_mode", "==", "SHORT_OR_BOTH"),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
            ("nifty_context_mode", "==", "SHORT_OR_BOTH"),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("entry_hour", "<=", 10.0833),
            ("quality_score", ">=", 0.4577),
            ("nifty_context_mode", "==", "SHORT_OR_BOTH"),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
            ("nifty_context_mode", "==", "SHORT_OR_BOTH"),
        ],
    },
    "D": {
        # High-quality / lower count. Keep only setups whose Stage-3 chain
        # achieves PF >= 1.55 with n >= 25 in train.
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
            ("avwap_dist_atr_signal", ">=", 1.5260),
            ("entry_hour", "<=", 9.6667),
        ],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
            ("adx_signal", ">=", 34.1655),
        ],
        ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 1.5133),
            ("entry_hour", "<=", 9.9167),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("entry_hour", "<=", 10.0833),
            ("quality_score", ">=", 0.4577),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
    },
    "E": {
        # Count-preserving (kept for completeness; in this sample the
        # setups that pass the looser filter are roughly the same as D
        # plus B_AVWAP unfiltered).
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [],
        ("LONG",  "D_EMA20_BOUNCE"): [
            ("quality_score", ">=", 1.3833),
            ("ema20_gap_atr_signal", ">=", -2.1524),
            ("adx_signal", "<=", 37.6647),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
    },
    # -----------------------------------------------------------------
    # F variants: Candidate B + best-effort rescue of the three weak
    # LONG setups (A_MOD_CLOSE_CONTINUATION_BREAK, C_OR_BREAKOUT,
    # G_HIGHER_HIGH_BREAK). The rescue chains were derived by
    # _v17r_setup_rescue.py / _v17r_setup_rescue_v2.py with
    # MAX_LEN=5, range filters and trailing-90d ticker eligibility
    # tested. None of the three setups individually clears the
    # standard OOS gates (train PF >= 1.20 AND OOS PF >= 1.10
    # AND decay >= 0.55) -- they are kept as a deliberate
    # volume-vs-edge trade-off, not because they have an OOS edge.
    #
    # Aggregate trade-offs (Stage-8 metrics on existing CSV):
    #   F1 = B + G_HH only           : 1163 trades, PF 1.32, OOS PF 1.35
    #   F2 = B + G_HH + C_OR         : 1642 trades, PF 1.18, OOS PF 1.14
    #   F  = B + all 3 rescued       : 1667 trades, PF 1.18, OOS PF 1.15
    # -----------------------------------------------------------------
    "F1": {
        # ---- Candidate B core (8 setups) --------------------------------
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
            ("avwap_dist_atr_signal", ">=", 1.5260),
            ("entry_hour", "<=", 9.6667),
        ],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
            ("adx_signal", ">=", 34.1655),
        ],
        ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 1.5133),
            ("entry_hour", "<=", 9.9167),
        ],
        ("LONG",  "D_EMA20_BOUNCE"): [
            ("quality_score", ">=", 1.3833),
            ("ema20_gap_atr_signal", ">=", -2.1524),
            ("adx_signal", "<=", 37.6647),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("entry_hour", "<=", 10.0833),
            ("quality_score", ">=", 0.4577),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
        # ---- Rescue: G_HIGHER_HIGH_BREAK only ---------------------------
        # Best 5-feature greedy: train PF 0.98 -> rescued PF 0.98, OOS PF 0.99,
        # decay 1.00. Adding it to B doubles trade count to 1163 with
        # aggregate PF 1.32 (B's PF dilutes from 1.89 to 1.32).
        ("LONG",  "G_HIGHER_HIGH_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 0.8826),
            ("entry_hour", "between", 9.8333, 10.6667),
            ("quality_score", ">=", 1.7248),
            ("stochk_signal", "<=", 97.176),
            ("atr_pct_signal", ">=", 0.0045),
        ],
    },
    "F": {
        # ---- Candidate B core (8 setups) --------------------------------
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
            ("avwap_dist_atr_signal", ">=", 1.5260),
            ("entry_hour", "<=", 9.6667),
        ],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
            ("adx_signal", ">=", 34.1655),
        ],
        ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 1.5133),
            ("entry_hour", "<=", 9.9167),
        ],
        ("LONG",  "D_EMA20_BOUNCE"): [
            ("quality_score", ">=", 1.3833),
            ("ema20_gap_atr_signal", ">=", -2.1524),
            ("adx_signal", "<=", 37.6647),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("entry_hour", "<=", 10.0833),
            ("quality_score", ">=", 0.4577),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
        # ---- Rescue: all three weak LONG setups -------------------------
        # A_MOD_CLOSE_CONTINUATION_BREAK -- best chain: avwap_dist range +
        # rsi cap. n=25, train PF 0.80, OOS PF 2.04 (n_oos=8). Aggregate
        # impact in F: minimal (25 trades).
        ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"): [
            ("avwap_dist_atr_signal", "between", 0.8928, 2.0578),
            ("rsi_signal", "<=", 81.086),
        ],
        # C_OR_BREAKOUT -- best 5-feature chain. train PF 1.00, OOS PF 0.64,
        # decay 0.64. Loss-making OOS in isolation; included for volume.
        ("LONG",  "C_OR_BREAKOUT"): [
            ("quality_score", ">=", 1.4789),
            ("entry_hour", "<=", 10.5),
            ("ema20_gap_atr_signal", ">=", -3.94),
            ("atr_pct_signal", "<=", 0.00934),
            ("adx_signal", ">=", 23.234),
        ],
        # G_HIGHER_HIGH_BREAK -- best 5-feature chain. train PF 0.98,
        # OOS PF 0.99, decay 1.00 -- break-even but stable.
        ("LONG",  "G_HIGHER_HIGH_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 0.8826),
            ("entry_hour", "between", 9.8333, 10.6667),
            ("quality_score", ">=", 1.7248),
            ("stochk_signal", "<=", 97.176),
            ("atr_pct_signal", ">=", 0.0045),
        ],
    },
}


# Causal feature whitelist. Any threshold targeting a feature outside this
# set raises at install time -- prevents accidentally importing a non-causal
# column into a future spec edit.
V17R_CAUSAL_FEATURES = {
    "rsi_signal", "adx_signal", "atr_pct_signal", "avwap_dist_atr_signal",
    "ema20_gap_atr_signal", "stochk_signal", "quality_score",
    "nifty_rel_strength_pct", "nifty_context_mode",
    "entry_hour", "gap_pct_open", "opening_range_width_pct", "india_vix",
}


def _validate_spec(name: str, spec: Dict[Tuple[str, str], List]) -> None:
    for (side, setup), chain in spec.items():
        if side not in ("LONG", "SHORT"):
            raise SystemExit(
                f"[V17R_SETUP_LAB] candidate {name!r} bad side {side!r} in "
                f"setup {setup!r}"
            )
        for step in chain:
            if not (isinstance(step, tuple) and len(step) in (3, 4)):
                raise SystemExit(
                    f"[V17R_SETUP_LAB] candidate {name!r} setup {setup!r} "
                    f"chain step malformed: {step!r}"
                )
            feat, direction = step[0], step[1]
            if feat not in V17R_CAUSAL_FEATURES:
                raise SystemExit(
                    f"[V17R_SETUP_LAB] candidate {name!r} setup {setup!r} "
                    f"uses non-causal feature {feat!r}; refuse to run"
                )
            if direction not in (">=", "<=", "==", "between"):
                raise SystemExit(
                    f"[V17R_SETUP_LAB] candidate {name!r} setup {setup!r} "
                    f"chain bad direction {direction!r}"
                )
            if direction == "between" and len(step) != 4:
                raise SystemExit(
                    f"[V17R_SETUP_LAB] candidate {name!r} setup {setup!r} "
                    f"between op needs 4-tuple (feat,'between',lo,hi); got {step!r}"
                )


for cname, cspec in V17R_CANDIDATE_SPECS.items():
    _validate_spec(cname, cspec)


# ---------------------------------------------------------------------------
# Candidate selection.
# ---------------------------------------------------------------------------
EQIDV17R_CANDIDATE = os.environ.get("EQIDV17R_CANDIDATE", "baseline").strip()
if EQIDV17R_CANDIDATE not in {"baseline", "A", "B", "C", "D", "E", "F", "F1"}:
    raise SystemExit(
        f"[V17R_SETUP_LAB] EQIDV17R_CANDIDATE must be in "
        f"{{baseline,A,B,C,D,E,F,F1}}; got {EQIDV17R_CANDIDATE!r}"
    )

V17R_RECOMMENDED_CANDIDATE = "B"


# ---------------------------------------------------------------------------
# Per-row chain application -- supports the LONG_OR_BOTH / SHORT_OR_BOTH
# regime literal.
# ---------------------------------------------------------------------------
def _v17r_apply_chain(
    df: pd.DataFrame,
    chain: List[Tuple[str, str, float]],
) -> pd.DataFrame:
    if df is None or len(df) == 0 or not chain:
        return df
    et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
    try:
        entry_hour = (et.dt.tz_convert("Asia/Kolkata").dt.hour
                      + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0)
    except Exception:
        entry_hour = pd.Series(np.nan, index=df.index)

    keep = pd.Series(True, index=df.index)
    for step in chain:
        feat = step[0]
        direction = step[1]
        if feat == "nifty_context_mode" and direction == "==" \
                and isinstance(step[2], str) and step[2] in ("LONG_OR_BOTH", "SHORT_OR_BOTH"):
            allowed = (("LONG_ONLY", "BOTH") if step[2] == "LONG_OR_BOTH"
                       else ("SHORT_ONLY", "BOTH"))
            ctx = df.get("nifty_context_mode",
                         pd.Series("", index=df.index)).astype(str)
            keep &= ctx.isin(allowed)
            continue
        col = (entry_hour if feat == "entry_hour"
               else pd.to_numeric(df.get(feat, pd.Series(np.nan, index=df.index)),
                                  errors="coerce"))
        if direction == ">=":
            keep &= (col >= step[2]).fillna(False)
        elif direction == "<=":
            keep &= (col <= step[2]).fillna(False)
        elif direction == "between":
            keep &= col.between(step[2], step[3]).fillna(False)
        elif direction == "==":
            sval = df.get(feat, pd.Series("", index=df.index)).astype(str)
            keep &= sval.eq(str(step[2]))
    return df.loc[keep].copy()


def _v17r_setup_filter(
    df: pd.DataFrame, side_label: str,
    spec: Dict[Tuple[str, str], List],
) -> pd.DataFrame:
    if df is None or df.empty or "setup" not in df.columns:
        return df
    n_in = len(df)
    setup_norm = df["setup"].astype(str).str.upper().str.strip()
    keep_mask = pd.Series(False, index=df.index)
    seen_setups = []
    for (k_side, k_setup), chain in spec.items():
        if k_side != side_label:
            continue
        in_setup = setup_norm.eq(k_setup)
        if not in_setup.any():
            print(f"[V17R] {side_label} {k_setup}: 0 rows present -> 0 kept")
            seen_setups.append(k_setup)
            continue
        sub = df[in_setup]
        n_setup_in = int(in_setup.sum())
        sub_kept = _v17r_apply_chain(sub, chain) if chain else sub
        keep_mask.loc[sub_kept.index] = True
        print(f"[V17R] {side_label} {k_setup}: {n_setup_in} -> {len(sub_kept)}")
        seen_setups.append(k_setup)
    # Setups not in spec are dropped.
    dropped_setups = (set(setup_norm.unique()) - set(seen_setups))
    if dropped_setups:
        for s in sorted(dropped_setups):
            n_dropped = int(setup_norm.eq(s).sum())
            print(f"[V17R] {side_label} {s}: {n_dropped} -> 0 (DROPPED)")

    out = df.loc[keep_mask].copy()
    print(f"[V17R] {side_label} candidate={EQIDV17R_CANDIDATE} "
          f"{n_in}->{len(out)} ({len(spec)} setups in spec)")
    return out


# ---------------------------------------------------------------------------
# Patch the post-scan pipeline. We bypass v17t_live's P5b/P5c/P5d entry
# points entirely (we already asserted those are OFF). Instead we wrap the
# v17p chain (which performs Stage 0/1/2) and apply the v17r filter after.
# ---------------------------------------------------------------------------
import avwap_combined_runner_v17p_5min as _v17p  # noqa: E402

# Capture the chain that the v17p import installed -- this includes Stage 0,
# Stage 1, and Stage 2 from v17p plus everything beneath (v17o ... v16).
_v17p_post_scan_chain = _base._apply_v16_post_scan_filters


def _v17r_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17p_post_scan_chain(short_df, long_df)
    if EQIDV17R_CANDIDATE == "baseline":
        print("[V17R] EQIDV17R_CANDIDATE=baseline -- no v17r filters applied")
        return short_df, long_df

    spec = V17R_CANDIDATE_SPECS[EQIDV17R_CANDIDATE]
    long_df = _v17r_setup_filter(long_df, "LONG", spec)
    short_df = _v17r_setup_filter(short_df, "SHORT", spec)

    # F1 hardening still applies as the last step (v17t_live also does this
    # explicitly). We rely on v17t_live's own _v17t_apply_stage0 having
    # already run inside the v17p->v17t chain since V17T_STAGE0_HARDEN is
    # required ON.
    return short_df, long_df


_base._apply_v16_post_scan_filters = _v17r_apply_post_scan_filters


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
def _banner() -> None:
    print("=" * 72)
    print("v17r SETUP LAB -- v17p strategy logic + v17q honesty fixes + "
          "v17r candidate filters")
    print(f"  candidate          = {EQIDV17R_CANDIDATE}")
    print(f"  recommended        = {V17R_RECOMMENDED_CANDIDATE}")
    print(f"  honesty fixes      = ALL ON ({len(_REQUIRED_HONESTY)} verified)")
    print(f"  output dir suffix  = v17r_setup_lab_5min")
    print("=" * 72)


_banner()


# ---------------------------------------------------------------------------
# CLI passthrough
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # The actual scan is owned by avwap_combined_runner_v16_5min.main()
    # (re-exported through v17t_live -> v17p -> v16). Simply invoke it.
    if hasattr(_base, "main"):
        _base.main()
    else:
        raise SystemExit(
            "[V17R_SETUP_LAB] avwap_combined_runner_v16_5min.main() not found"
        )
