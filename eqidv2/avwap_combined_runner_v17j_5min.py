# -*- coding: utf-8 -*-
"""
V17j 5-min combined runner — adds D_AVWAP_LOSE_REVERSAL (SHORT mirror of
B_AVWAP_RECLAIM_REVERSAL) on top of v17i.

Motivation
----------
v17g's LONG reversal setup proved its alpha (9,671 trades, 95% of LONG volume).
v17j introduces the symmetric SHORT setup: price was above AVWAP in the last
1-2 bars, current bar closes below AVWAP with decisive red body, falling RSI,
K<D, ADX>=threshold, above-avg volume. Entry on the next bar via breakdown
below the lose-bar's low.

Design inherits all v17i improvements (LONG target 0.80%, F7 dead-zone drop,
max_trades/ticker/day=2, reversal_max_hour_ist=13). The LONG side is UNTOUCHED;
v17j only activates + configures the new SHORT setup.

Five changes (all env-toggleable)
---------------------------------
  Change 1 (ENABLE)     EQIDV17J_ENABLE_SHORT_REVERSAL=True
  Change 2 (SCAN-TIME)  short_reversal_volume_max_ratio (cap; default 3.0)
  Change 3 (SCAN-TIME)  short_reversal_avwap_dist_atr_min (floor; default 1.0)
  Change 4 (POST-SCAN)  require nifty_context_mode in {BOTH, SHORT_ONLY}
  Change 5 (POST-SCAN)  require nifty_rel_strength_pct <= negative threshold

Env knobs (defaults mirror Patch B from LONG side)
--------------------------------------------------
  EQIDV17J_ENABLE_SHORT_REVERSAL            (default True)
  EQIDV17J_SHORT_REVERSAL_MAX_HOUR          (default 13)
  EQIDV17J_SHORT_REVERSAL_VOL_MAX_ON        (default True)
  EQIDV17J_SHORT_REVERSAL_VOL_MAX_RATIO     (default 3.0)
  EQIDV17J_SHORT_REVERSAL_DIST_MIN_ON       (default True)
  EQIDV17J_SHORT_REVERSAL_DIST_MIN_ATR      (default 1.0)
  EQIDV17J_SHORT_REVERSAL_MODE_FILTER       (default True — drop LONG_ONLY)
  EQIDV17J_SHORT_REVERSAL_MAX_NIFTY_RS_ON   (default True)
  EQIDV17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT  (default -1.0)
  EQIDV17J_SHORT_MAX_TRADES_PER_TICKER      (default 2)

Outputs go to outputs_v17j_5min/.

Note on asymmetry
-----------------
Indian equities have a structural upward drift, so the SHORT reversal edge is
expected to be smaller than LONG reversal's. The Patch-B-mirrored gates are
deliberately conservative (nifty_rs <= -1% is tighter than LONG's +1% because
the short side has less absolute edge to absorb filter softness).
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17i_5min as _v17i  # cascade import
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# Env helpers.
# ---------------------------------------------------------------------------
def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return int(default)
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return int(default)


# ---------------------------------------------------------------------------
# V17j env toggles.
# ---------------------------------------------------------------------------
V17J_ENABLE_SHORT_REVERSAL           = _env_bool("EQIDV17J_ENABLE_SHORT_REVERSAL", True)
V17J_SHORT_REVERSAL_MAX_HOUR         = _env_int("EQIDV17J_SHORT_REVERSAL_MAX_HOUR", 13)
V17J_SHORT_REVERSAL_VOL_MAX_ON       = _env_bool("EQIDV17J_SHORT_REVERSAL_VOL_MAX_ON", True)
V17J_SHORT_REVERSAL_VOL_MAX_RATIO    = _env_float("EQIDV17J_SHORT_REVERSAL_VOL_MAX_RATIO", 3.0)
V17J_SHORT_REVERSAL_DIST_MIN_ON      = _env_bool("EQIDV17J_SHORT_REVERSAL_DIST_MIN_ON", True)
V17J_SHORT_REVERSAL_DIST_MIN_ATR     = _env_float("EQIDV17J_SHORT_REVERSAL_DIST_MIN_ATR", 1.0)
V17J_SHORT_REVERSAL_MODE_FILTER      = _env_bool("EQIDV17J_SHORT_REVERSAL_MODE_FILTER", True)
V17J_SHORT_REVERSAL_MAX_NIFTY_RS_ON  = _env_bool("EQIDV17J_SHORT_REVERSAL_MAX_NIFTY_RS_ON", True)
V17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT = _env_float("EQIDV17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT", -1.0)
V17J_SHORT_MAX_TRADES_PER_TICKER     = _env_int("EQIDV17J_SHORT_MAX_TRADES_PER_TICKER", 2)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17j_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17j = _base.runtime_dir  # already v17i-patched


def _v17j_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17j_5min",
            "v17i_5min",
            "v17h_5min",
            "v17g_5min",
            "v17f_5min",
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17j_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17j(*tuple(new_parts))


_base.runtime_dir = _v17j_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: SHORT-side cfg adjust — enable setup + set Patch-B-mirrored gates.
# v17i touches the LONG scanner wrapper; we add a parallel wrapper for SHORT.
# ---------------------------------------------------------------------------
_v17j_orig_scan_short_prepared = getattr(_base, "scan_short_prepared", None)


def _v17j_adjust_short_cfg(cfg):
    if V17J_ENABLE_SHORT_REVERSAL:
        try:
            cfg.enable_setup_d_avwap_lose_reversal = True
            cfg.short_reversal_max_hour_ist = int(V17J_SHORT_REVERSAL_MAX_HOUR)
        except Exception:
            pass
    if V17J_SHORT_REVERSAL_VOL_MAX_ON:
        try:
            cfg.short_reversal_volume_max_ratio = float(V17J_SHORT_REVERSAL_VOL_MAX_RATIO)
        except Exception:
            pass
    if V17J_SHORT_REVERSAL_DIST_MIN_ON:
        try:
            cfg.short_reversal_avwap_dist_atr_min = float(V17J_SHORT_REVERSAL_DIST_MIN_ATR)
        except Exception:
            pass
    try:
        cfg.max_trades_per_ticker_per_day = int(V17J_SHORT_MAX_TRADES_PER_TICKER)
    except Exception:
        pass
    return cfg


if _v17j_orig_scan_short_prepared is not None:
    def _v17j_scan_short_prepared(ticker, df_prepared, short_cfg):
        short_cfg = _v17j_adjust_short_cfg(short_cfg)
        return _v17j_orig_scan_short_prepared(ticker, df_prepared, short_cfg)

    _base.scan_short_prepared = _v17j_scan_short_prepared


# ---------------------------------------------------------------------------
# PATCH 3: post-scan mode + nifty_rs gates for D_AVWAP_LOSE_REVERSAL.
# Runs AFTER v17i's LONG post-scan stack (which itself chains v17h/g/f/d/v16).
# ---------------------------------------------------------------------------
_v17i_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17i_get_filter_reason       = _base.get_v16_filter_reason


def _v17j_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17i_apply_post_scan_filters(short_df, long_df)

    if short_df is None or short_df.empty:
        return short_df, long_df
    if "setup" not in short_df.columns:
        return short_df, long_df

    work = short_df.copy()
    is_rev = work["setup"].astype(str).str.strip().eq("D_AVWAP_LOSE_REVERSAL")
    if not is_rev.any():
        return short_df, long_df

    drop_mask = pd.Series(False, index=work.index)
    mode_dropped = 0
    rs_dropped = 0

    if V17J_SHORT_REVERSAL_MODE_FILTER and "nifty_context_mode" in work.columns:
        mode = work["nifty_context_mode"].astype(str).str.upper().str.strip()
        # Drop LONG_ONLY days — SHORT reversal in an up-drift regime is noise.
        m_drop = is_rev & mode.eq("LONG_ONLY")
        mode_dropped = int(m_drop.sum())
        drop_mask = drop_mask | m_drop

    if V17J_SHORT_REVERSAL_MAX_NIFTY_RS_ON and "nifty_rel_strength_pct" in work.columns:
        rs = pd.to_numeric(work["nifty_rel_strength_pct"], errors="coerce")
        # SHORT wants WEAK stocks: nifty_rs must be <= threshold (e.g. <= -1.0%).
        # NaN-RS rows fail the gate (consistent with v17h LONG behavior).
        r_drop = is_rev & ~(rs <= float(V17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT))
        r_drop = r_drop & ~drop_mask
        rs_dropped = int(r_drop.sum())
        drop_mask = drop_mask | r_drop

    before = len(work)
    work = work.loc[~drop_mask].copy()
    after = len(work)
    print(
        "[V17J_FILTER] SHORT D_AVWAP_LOSE_REVERSAL post-scan: "
        f"{before}->{after} (-{mode_dropped} mode==LONG_ONLY "
        f"| -{rs_dropped} nifty_rs>{V17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT})"
    )
    return work, long_df


def _v17j_get_filter_reason(row: dict, side: str):
    reason = _v17i_get_filter_reason(row, side)
    if reason is not None:
        return reason

    if str(side).upper().strip() != "SHORT":
        return None
    if str(row.get("setup", "")).strip() != "D_AVWAP_LOSE_REVERSAL":
        return None

    if V17J_SHORT_REVERSAL_MODE_FILTER:
        mode = str(row.get("nifty_context_mode", "")).upper().strip()
        if mode == "LONG_ONLY":
            return (
                "v17j short reversal cleanup: nifty_context_mode=LONG_ONLY "
                "(drop — up-drift regime)"
            )

    if V17J_SHORT_REVERSAL_MAX_NIFTY_RS_ON:
        try:
            rs = float(row.get("nifty_rel_strength_pct", float("nan")))
        except (TypeError, ValueError):
            rs = float("nan")
        if not (np.isfinite(rs) and rs <= float(V17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT)):
            return (
                f"v17j short reversal cleanup: nifty_rs={rs:.2f}% "
                f"(require <= {V17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT:.2f}%)"
            )

    return None


_base._apply_v16_post_scan_filters = _v17j_apply_post_scan_filters
_base.get_v16_filter_reason = _v17j_get_filter_reason


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 78)
    print("V17j 5-min runner: adds SHORT D_AVWAP_LOSE_REVERSAL on top of v17i")
    print("  Output dir: outputs_v17j_5min")
    print("--- Toggles ---")
    print(f"  Change 1  enable_short_reversal       = {V17J_ENABLE_SHORT_REVERSAL}"
          f"  (max_hour_ist={V17J_SHORT_REVERSAL_MAX_HOUR}:00)")
    print(f"  Change 2  short_reversal_vol_max_on   = {V17J_SHORT_REVERSAL_VOL_MAX_ON}"
          f"  (cap {V17J_SHORT_REVERSAL_VOL_MAX_RATIO:.2f}x VOL_SMA20)")
    print(f"  Change 3  short_reversal_dist_min_on  = {V17J_SHORT_REVERSAL_DIST_MIN_ON}"
          f"  (min {V17J_SHORT_REVERSAL_DIST_MIN_ATR:.2f} ATR below AVWAP)")
    print(f"  Change 4  short_reversal_mode_filter  = {V17J_SHORT_REVERSAL_MODE_FILTER}"
          "  (drop LONG_ONLY days)")
    print(f"  Change 5  short_reversal_max_nifty_rs = {V17J_SHORT_REVERSAL_MAX_NIFTY_RS_ON}"
          f"  (threshold {V17J_SHORT_REVERSAL_MAX_NIFTY_RS_PCT:.2f}%)")
    print(f"  Aux       short_max_trades/ticker/day = {V17J_SHORT_MAX_TRADES_PER_TICKER}")
    print("--- Inherits all v17i / v17h / v17g / v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
