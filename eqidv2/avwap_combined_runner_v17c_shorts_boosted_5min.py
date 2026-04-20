# -*- coding: utf-8 -*-
"""
V17c SHORTS BOOSTED 5-min combined runner.
==============================================================================

Purpose
-------
DOES NOT MODIFY v17c. This runner imports v17c as the base, keeps LONG logic
untouched, and replaces the SHORT side with a data-driven configuration tuned
from deep analytics of the v17c SHORT trade set:
  * n=503, PF=1.340, DayWin=58.82%, MaxDD=64.76% (baseline).

Analytics (slice PFs):
  Setups  : A_MOD=1.39 (bulk)  | A_PULLBACK=0.85 LOSER | B_HUGE_FB=5.51 WIN (rare)
  Context : BOTH=1.54 (bulk)   | SHORT_ONLY=1.05 (weak)
  RSI     : 40-45=2.08 (best)  | 20-25=1.02 weak | dead-zone 35-40 already cut
  ADX     : 25-30=2.27         | >=50=0.96 weak  | 20-25=1.04 weak
  AVWAP   : 1.75-2.0=1.87      | 0.75-1.0=1.00 dead
  Nifty RS: -0.5..-0.25=2.44   | <-2.0=0.68 (short-cover risk)
  ATR%    : 0.3-0.5=1.60-1.62  | 0.7-1.0=0.92 weak | >=1.5%=0.51 very weak
  Time    : 13:00-13:30=2.89   | 12:30-13:00=0.78 pocket | 11:30-12:30=1.86
  Day     : Mon=1.74 | Thu=1.00 weak | Tue=1.18 weak

Design
------
1. RELAX upstream to generate more shorts:
   - Signal windows: keep 09:15-12:00, extend session-2 12:00-14:00 (was 13:30)
   - SHORT entry_time_cutoff 13:30 -> 14:00
   - NIFTY BOTH-mode RS threshold -0.75% -> configurable (default -0.35%) to let
     in weak-RS shorts which empirically carry the edge (PF 2.44 in -0.5..-0.25)
   - AVWAP dist cap 2.10 -> 2.50 ATR (1.75-2.0 already PF 1.87)
   - adx_min 30.0 -> 25.0 (ADX 25-30 is PF 2.27; below 25 only reversal mode,
     which is less common)
   - mod_impulse_min_atr 0.30 -> 0.25 (more C1 classifications qualify)
   - volume_min_ratio 0.90 -> 0.75

2. APPLY downstream quality filters (new BOOSTED filter bundle):
   - Drop A_PULLBACK_C2_THEN_BREAK_C2_LOW (matches v17c intent; PF 0.85 loser)
   - Keep A_MOD_BREAK_C1_LOW as the core
   - Keep B_HUGE_RED_FAILED_BOUNCE (rare but high PF)
   - Block entries 12:30-13:00 (explicit dead pocket, PF 0.78)
   - Block ATR% > 0.70% (PF collapses 1.21 -> 0.92 -> 0.51 with rising ATR)
   - Block RS <= -2.0% (short-cover risk, PF 0.68)
   - In SHORT_ONLY context, require RS <= -1.0% (SHORT_ONLY overall PF=1.05
     driven by A_MOD PF=0.98; gated by strong RS it becomes usable)
   - Keep v16 RSI 35-40 dead zone (documented: 38.5% win)
   - Drop v17c SHORT_ONLY RSI 21-28 block (our data: 20-25 PF 1.02, 25-30 PF 1.29
     — narrow but not the biggest issue; SHORT_ONLY gating via RS handles it)
   - Drop v17c SHORT_ONLY ADX>=44 (replaced by ADX>=50 block across all shorts,
     PF 0.96 universally weak)

All thresholds are env-var overridable for sweep tuning without editing code.

Outputs -> outputs_v17c_shorts_boosted_5min/
"""
from __future__ import annotations

import os
from datetime import time as dtime
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17c_5min as _v17c  # DO NOT MODIFY v17c
import avwap_combined_runner_v17b_5min as _v17b
import avwap_combined_runner_v16_5min as _base

from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker_prepared as _orig_scan_short_prepared,
)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    s = str(raw).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw)


# ---------------------------------------------------------------------------
# UPSTREAM KNOBS
# ---------------------------------------------------------------------------
BOOST_SHORT_WINDOW_SESSION1_START = _env_str("EQIDV17C_BOOST_SH_W1_START", "09:15")
BOOST_SHORT_WINDOW_SESSION1_END = _env_str("EQIDV17C_BOOST_SH_W1_END", "12:00")
BOOST_SHORT_WINDOW_SESSION2_START = _env_str("EQIDV17C_BOOST_SH_W2_START", "12:00")
BOOST_SHORT_WINDOW_SESSION2_END = _env_str("EQIDV17C_BOOST_SH_W2_END", "14:00")
BOOST_SHORT_ENTRY_CUTOFF_HHMM = _env_str("EQIDV17C_BOOST_SH_ENTRY_CUTOFF", "14:00")

BOOST_SHORT_RS_BOTH_PCT = _env_float("EQIDV17C_BOOST_SH_RS_BOTH_PCT", 0.35)
BOOST_SHORT_AVWAP_DIST_CAP = _env_float("EQIDV17C_BOOST_SH_AVWAP_DIST_CAP", 2.50)
BOOST_SHORT_ADX_MIN = _env_float("EQIDV17C_BOOST_SH_ADX_MIN", 25.0)
BOOST_SHORT_MOD_IMPULSE_MIN_ATR = _env_float("EQIDV17C_BOOST_SH_MOD_IMP_MIN_ATR", 0.25)
BOOST_SHORT_VOL_MIN_RATIO = _env_float("EQIDV17C_BOOST_SH_VOL_MIN_RATIO", 0.75)

# ---------------------------------------------------------------------------
# DOWNSTREAM BOOSTED QUALITY FILTERS
# ---------------------------------------------------------------------------
BOOST_DROP_PULLBACK_SETUP = _env_bool("EQIDV17C_BOOST_DROP_PULLBACK", True)

BOOST_BLOCK_TIME_DEAD_ENABLED = _env_bool("EQIDV17C_BOOST_BLOCK_DEAD_TIME", True)
BOOST_BLOCK_TIME_DEAD_START = _env_str("EQIDV17C_BOOST_DEAD_TIME_START", "12:30")
BOOST_BLOCK_TIME_DEAD_END = _env_str("EQIDV17C_BOOST_DEAD_TIME_END", "13:00")

BOOST_BLOCK_HIGH_ATR_ENABLED = _env_bool("EQIDV17C_BOOST_BLOCK_HIGH_ATR", True)
BOOST_BLOCK_HIGH_ATR_PCT = _env_float("EQIDV17C_BOOST_HIGH_ATR_PCT", 0.007)

BOOST_BLOCK_HIGH_ADX_ENABLED = _env_bool("EQIDV17C_BOOST_BLOCK_HIGH_ADX", True)
BOOST_BLOCK_HIGH_ADX_MIN = _env_float("EQIDV17C_BOOST_HIGH_ADX_MIN", 50.0)

BOOST_BLOCK_EXTREME_RS_ENABLED = _env_bool("EQIDV17C_BOOST_BLOCK_EXTREME_RS", True)
BOOST_BLOCK_EXTREME_RS_PCT = _env_float("EQIDV17C_BOOST_EXTREME_RS_PCT", -2.0)

BOOST_SHORTONLY_REQUIRE_STRONG_RS = _env_bool(
    "EQIDV17C_BOOST_SHORTONLY_REQ_STRONG_RS", True
)
BOOST_SHORTONLY_MIN_RS_NEG_PCT = _env_float(
    "EQIDV17C_BOOST_SHORTONLY_MIN_RS_NEG_PCT", 1.0
)

# Keep v16 RSI 35-40 dead zone (good filter). Disable v17c's SHORT_ONLY RSI 21-28
# because our data says SHORT_ONLY gating via RS is a better blanket rule.
BOOST_DROP_V17C_SHORTONLY_RSI = _env_bool("EQIDV17C_BOOST_DROP_V17C_SHORTONLY_RSI", True)
BOOST_DROP_V17C_SHORTONLY_HIGH_ADX = _env_bool(
    "EQIDV17C_BOOST_DROP_V17C_SHORTONLY_HIGH_ADX", True
)


def _parse_hhmm(s: str) -> dtime:
    parts = s.strip().split(":")
    hh = int(parts[0])
    mm = int(parts[1]) if len(parts) > 1 else 0
    return dtime(hh, mm, 0)


# ===========================================================================
# PATCH 1: output dir redirection
# ===========================================================================
_BOOSTED_OUT_NAME = "outputs_v17c_shorts_boosted_5min"
_orig_runtime_dir = _base.runtime_dir  # captured before v17c patched it


def _boost_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
            "v17c_shorts_boosted_5min",
        ):
            text = text.replace(old, "v17c_shorts_boosted_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _boost_runtime_dir


# ===========================================================================
# PATCH 2: SHORT signal windows + entry cutoff + nifty RS + AVWAP cap
# ===========================================================================
_new_short_windows = [
    (_parse_hhmm(BOOST_SHORT_WINDOW_SESSION1_START), _parse_hhmm(BOOST_SHORT_WINDOW_SESSION1_END)),
    (_parse_hhmm(BOOST_SHORT_WINDOW_SESSION2_START), _parse_hhmm(BOOST_SHORT_WINDOW_SESSION2_END)),
]
_base.FINAL_SHORT_SIGNAL_WINDOWS = _new_short_windows

_new_entry_cutoff = _parse_hhmm(BOOST_SHORT_ENTRY_CUTOFF_HHMM)
_base.V15_SHORT_ENTRY_CUTOFF = _new_entry_cutoff

_base.NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT = float(BOOST_SHORT_RS_BOTH_PCT)
_base.V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = float(BOOST_SHORT_AVWAP_DIST_CAP)


# ===========================================================================
# PATCH 3: wrap scan_short_prepared to tweak short_cfg before scanning
# ===========================================================================
def _boost_adjust_short_cfg(cfg):
    # These attributes live on the StrategyConfig dataclass.
    try:
        cfg.adx_min = float(BOOST_SHORT_ADX_MIN)
    except Exception:
        pass
    try:
        cfg.mod_impulse_min_atr = float(BOOST_SHORT_MOD_IMPULSE_MIN_ATR)
    except Exception:
        pass
    try:
        cfg.volume_min_ratio = float(BOOST_SHORT_VOL_MIN_RATIO)
    except Exception:
        pass
    try:
        cfg.signal_avwap_dist_atr_max = float(BOOST_SHORT_AVWAP_DIST_CAP)
    except Exception:
        pass
    try:
        cfg.entry_time_cutoff = _new_entry_cutoff
    except Exception:
        pass
    return cfg


def _boost_scan_short_prepared(ticker, df_prepared, short_cfg):
    short_cfg = _boost_adjust_short_cfg(short_cfg)
    return _orig_scan_short_prepared(ticker, df_prepared, short_cfg)


# Install monkey-patch where v16 imports scan_short_prepared from.
_base.scan_short_prepared = _boost_scan_short_prepared


# ===========================================================================
# PATCH 4: boosted post-scan filter — replaces v17c's filter bundle
# ===========================================================================
# Preserve the v17b reference because it contains the v16 filters we still want.
_v16_apply_post_scan_filters = _v17b._v16_apply_post_scan_filters
_v16_get_filter_reason = _v17b._v16_get_filter_reason


def _time_minutes(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    return ts.dt.hour.astype("Int64") * 60 + ts.dt.minute.astype("Int64")


_DEAD_START_MIN = _parse_hhmm(BOOST_BLOCK_TIME_DEAD_START).hour * 60 + _parse_hhmm(BOOST_BLOCK_TIME_DEAD_START).minute
_DEAD_END_MIN = _parse_hhmm(BOOST_BLOCK_TIME_DEAD_END).hour * 60 + _parse_hhmm(BOOST_BLOCK_TIME_DEAD_END).minute


def _apply_v17c_long_late_antichase(long_df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Inline copy of v17c's LONG anti-chase filter (no SHORT interaction)."""
    if long_df.empty or not _v17c.V17C_LONG_LATE_ANTICHASE_ENABLED:
        return long_df, 0
    needed = {"setup", "bars_from_open", "entry_bar_vol_ratio"}
    if not needed.issubset(long_df.columns):
        return long_df, 0
    w = long_df.copy()
    setup = w["setup"].astype(str).str.upper().str.strip()
    bfo = pd.to_numeric(w["bars_from_open"], errors="coerce")
    vr = pd.to_numeric(w["entry_bar_vol_ratio"], errors="coerce")
    mask = (
        setup.eq(_v17c.V17C_LONG_LATE_SETUP_NAME)
        & bfo.ge(_v17c.V17C_LONG_LATE_MIN_BARS_FROM_OPEN)
        & vr.ge(_v17c.V17C_LONG_LATE_MIN_VOL_RATIO)
    )
    removed = int(mask.sum())
    return w.loc[~mask].copy(), removed


def _boost_apply_post_scan_filters(
    short_df: pd.DataFrame, long_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # First apply the original v16 filter bundle (RSI 35-40 dead zone, LONG gates).
    short_df, long_df = _v16_apply_post_scan_filters(short_df, long_df)

    # Apply v17c's LONG anti-chase once (does not touch SHORT).
    long_df, long_late_removed = _apply_v17c_long_late_antichase(long_df)

    short_before = len(short_df)
    pullback_removed = time_removed = atr_removed = adx_removed = 0
    extreme_rs_removed = short_only_rs_removed = 0

    if not short_df.empty:
        w = short_df.copy()

        # 1) Drop the pullback setup entirely.
        if BOOST_DROP_PULLBACK_SETUP and "setup" in w.columns:
            setup = w["setup"].astype(str).str.upper().str.strip()
            mask = setup.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW")
            pullback_removed = int(mask.sum())
            w = w.loc[~mask].copy()

        # 2) Block dead-time entry pocket (default 12:30-13:00).
        if BOOST_BLOCK_TIME_DEAD_ENABLED and "entry_time_ist" in w.columns:
            mins = _time_minutes(w["entry_time_ist"])
            mask = mins.between(_DEAD_START_MIN, _DEAD_END_MIN - 1)
            mask = mask.fillna(False).astype(bool)
            time_removed = int(mask.sum())
            w = w.loc[~mask].copy()

        # 3) Block stretched-volatility shorts.
        if BOOST_BLOCK_HIGH_ATR_ENABLED and "atr_pct_signal" in w.columns:
            atr = pd.to_numeric(w["atr_pct_signal"], errors="coerce")
            mask = atr.ge(BOOST_BLOCK_HIGH_ATR_PCT).fillna(False)
            atr_removed = int(mask.sum())
            w = w.loc[~mask].copy()

        # 4) Block saturated trend shorts (ADX>=50 across ALL modes).
        if BOOST_BLOCK_HIGH_ADX_ENABLED and "adx_signal" in w.columns:
            adx = pd.to_numeric(w["adx_signal"], errors="coerce")
            mask = adx.ge(BOOST_BLOCK_HIGH_ADX_MIN).fillna(False)
            adx_removed = int(mask.sum())
            w = w.loc[~mask].copy()

        # 5) Block extreme-negative RS (short-cover risk).
        if BOOST_BLOCK_EXTREME_RS_ENABLED and "nifty_rel_strength_pct" in w.columns:
            rs = pd.to_numeric(w["nifty_rel_strength_pct"], errors="coerce")
            mask = rs.le(BOOST_BLOCK_EXTREME_RS_PCT).fillna(False)
            extreme_rs_removed = int(mask.sum())
            w = w.loc[~mask].copy()

        # 6) SHORT_ONLY mode: require strong negative RS to keep.
        if (
            BOOST_SHORTONLY_REQUIRE_STRONG_RS
            and "nifty_context_mode" in w.columns
            and "nifty_rel_strength_pct" in w.columns
        ):
            mode = w["nifty_context_mode"].astype(str).str.upper().str.strip()
            rs = pd.to_numeric(w["nifty_rel_strength_pct"], errors="coerce")
            mask = mode.eq("SHORT_ONLY") & (rs.gt(-BOOST_SHORTONLY_MIN_RS_NEG_PCT) | rs.isna())
            mask = mask.fillna(False).astype(bool)
            short_only_rs_removed = int(mask.sum())
            w = w.loc[~mask].copy()

        short_df = w

    removed_short_total = short_before - len(short_df)
    print(
        "[V17C_BOOST] LONG late-chase removed={lr}".format(lr=long_late_removed)
    )
    print(
        "[BOOST_SHORTS] SHORT: {before}->{after} "
        "(-{pb} pullback | -{tm} dead_time {ts}-{te} | "
        "-{ap} atr%>={apct:.2f}% | -{ax} adx>={axm:.0f} | "
        "-{xr} rs<={xrp:.2f}% | -{so} SHORT_ONLY rs>-{somn:.2f}%) "
        "| total_removed={tot}".format(
            before=short_before,
            after=len(short_df),
            pb=pullback_removed,
            tm=time_removed,
            ts=BOOST_BLOCK_TIME_DEAD_START,
            te=BOOST_BLOCK_TIME_DEAD_END,
            ap=atr_removed,
            apct=BOOST_BLOCK_HIGH_ATR_PCT * 100.0,
            ax=adx_removed,
            axm=BOOST_BLOCK_HIGH_ADX_MIN,
            xr=extreme_rs_removed,
            xrp=BOOST_BLOCK_EXTREME_RS_PCT,
            so=short_only_rs_removed,
            somn=BOOST_SHORTONLY_MIN_RS_NEG_PCT,
            tot=removed_short_total,
        )
    )
    return short_df, long_df


def _boost_get_filter_reason(row: dict, side: str):
    reason = _v16_get_filter_reason(row, side)
    if reason is not None:
        return reason

    side_u = str(side).upper().strip()
    if side_u != "SHORT":
        # LONG: defer to v17c's anti-chase logic.
        return _v17c._v17c_get_filter_reason(row, side)

    setup = str(row.get("setup", "")).upper().strip()
    if BOOST_DROP_PULLBACK_SETUP and setup == "A_PULLBACK_C2_THEN_BREAK_C2_LOW":
        return "boost_shorts: A_PULLBACK setup disabled"

    if BOOST_BLOCK_TIME_DEAD_ENABLED and "entry_time_ist" in row:
        ts = pd.to_datetime(row.get("entry_time_ist"), errors="coerce")
        if pd.notna(ts):
            mins = ts.hour * 60 + ts.minute
            if _DEAD_START_MIN <= mins < _DEAD_END_MIN:
                return (
                    f"boost_shorts: dead time {BOOST_BLOCK_TIME_DEAD_START}-"
                    f"{BOOST_BLOCK_TIME_DEAD_END}"
                )

    if BOOST_BLOCK_HIGH_ATR_ENABLED:
        try:
            atr = float(row.get("atr_pct_signal", float("nan")))
        except (TypeError, ValueError):
            atr = float("nan")
        if np.isfinite(atr) and atr >= BOOST_BLOCK_HIGH_ATR_PCT:
            return f"boost_shorts: atr_pct={atr * 100:.2f}% >= {BOOST_BLOCK_HIGH_ATR_PCT * 100:.2f}%"

    if BOOST_BLOCK_HIGH_ADX_ENABLED:
        try:
            adx = float(row.get("adx_signal", float("nan")))
        except (TypeError, ValueError):
            adx = float("nan")
        if np.isfinite(adx) and adx >= BOOST_BLOCK_HIGH_ADX_MIN:
            return f"boost_shorts: adx={adx:.1f} >= {BOOST_BLOCK_HIGH_ADX_MIN:.0f}"

    if BOOST_BLOCK_EXTREME_RS_ENABLED:
        try:
            rs = float(row.get("nifty_rel_strength_pct", float("nan")))
        except (TypeError, ValueError):
            rs = float("nan")
        if np.isfinite(rs) and rs <= BOOST_BLOCK_EXTREME_RS_PCT:
            return f"boost_shorts: extreme RS {rs:.2f}% <= {BOOST_BLOCK_EXTREME_RS_PCT:.2f}%"

    if BOOST_SHORTONLY_REQUIRE_STRONG_RS:
        mode = str(row.get("nifty_context_mode", "")).upper().strip()
        try:
            rs = float(row.get("nifty_rel_strength_pct", float("nan")))
        except (TypeError, ValueError):
            rs = float("nan")
        if mode == "SHORT_ONLY" and (not np.isfinite(rs) or rs > -BOOST_SHORTONLY_MIN_RS_NEG_PCT):
            return (
                f"boost_shorts: SHORT_ONLY requires rs<=-{BOOST_SHORTONLY_MIN_RS_NEG_PCT:.2f}%"
                f" (got {rs:.2f}%)"
            )

    return None


_base._apply_v16_post_scan_filters = _boost_apply_post_scan_filters
_base.get_v16_filter_reason = _boost_get_filter_reason


# ===========================================================================
# MAIN
# ===========================================================================
if __name__ == "__main__":
    print("=" * 74)
    print("AVWAP v17c SHORTS BOOSTED 5-min runner")
    print("  Inherits v17c for LONG. Replaces SHORT side with tuned config.")
    print(
        "  SHORT windows: "
        f"{BOOST_SHORT_WINDOW_SESSION1_START}-{BOOST_SHORT_WINDOW_SESSION1_END} | "
        f"{BOOST_SHORT_WINDOW_SESSION2_START}-{BOOST_SHORT_WINDOW_SESSION2_END} "
        f"(cutoff {BOOST_SHORT_ENTRY_CUTOFF_HHMM})"
    )
    print(
        f"  BOTH-mode RS<=-{BOOST_SHORT_RS_BOTH_PCT:.2f}% | "
        f"AVWAP cap={BOOST_SHORT_AVWAP_DIST_CAP:.2f} ATR | "
        f"adx_min={BOOST_SHORT_ADX_MIN:.0f} | "
        f"mod_imp_min_atr={BOOST_SHORT_MOD_IMPULSE_MIN_ATR:.2f} | "
        f"vol_min_ratio={BOOST_SHORT_VOL_MIN_RATIO:.2f}"
    )
    print(
        "  Boost filters: "
        f"drop_pullback={BOOST_DROP_PULLBACK_SETUP} | "
        f"dead_time={BOOST_BLOCK_TIME_DEAD_START}-{BOOST_BLOCK_TIME_DEAD_END} | "
        f"block_atr_pct>={BOOST_BLOCK_HIGH_ATR_PCT * 100:.2f}% | "
        f"block_adx>={BOOST_BLOCK_HIGH_ADX_MIN:.0f} | "
        f"block_rs<={BOOST_BLOCK_EXTREME_RS_PCT:.2f}% | "
        f"short_only_min_rs<=-{BOOST_SHORTONLY_MIN_RS_NEG_PCT:.2f}%"
    )
    print(f"  Output dir: outputs_v17c_shorts_boosted_5min")
    print("=" * 74)
    _base.main()
