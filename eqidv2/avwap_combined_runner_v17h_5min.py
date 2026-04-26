# -*- coding: utf-8 -*-
"""
V17h 5-min combined runner — Patch B from B_AVWAP_RECLAIM_REVERSAL frontier
analysis (project_v17g_results_and_reversal_tuning.md).

Background
----------
v17g introduced B_AVWAP_RECLAIM_REVERSAL (9,671 trades, 95% of LONG volume,
58% win, PF 1.29, MaxDD -248). Sub-bucket analysis on the v17g full-run trade
log identified four causally-named gates that, applied together, project to:
    n=2,077  win%=73.0  PF=2.54  sumPnL=3,796%  DD=-46.6  dwin%=84.8
That projection is "Patch B".

Patch B gates (each independently env-toggleable for A/B)
---------------------------------------------------------
  Gate 1 (SCAN-TIME)  reversal_volume_max_ratio   — drop bars where vol > N x VOL_SMA20
  Gate 2 (SCAN-TIME)  reversal_avwap_dist_atr_min — require AVWAP distance >= N ATR at entry
  Gate 3 (POST-SCAN)  drop reversal trades when nifty_context_mode != BOTH
  Gate 4 (POST-SCAN)  drop reversal trades when nifty_rel_strength_pct < threshold

Env knobs (defaults match Patch B as proposed)
----------------------------------------------
  EQIDV17H_REVERSAL_VOL_MAX_ON          (default True)
  EQIDV17H_REVERSAL_VOL_MAX_RATIO       (default 3.0)
  EQIDV17H_REVERSAL_DIST_MIN_ON         (default True)
  EQIDV17H_REVERSAL_DIST_MIN_ATR        (default 1.0)
  EQIDV17H_REVERSAL_REQUIRE_BOTH_MODE   (default True)
  EQIDV17H_REVERSAL_MIN_NIFTY_RS_ON     (default True)
  EQIDV17H_REVERSAL_MIN_NIFTY_RS_PCT    (default 1.0)

Outputs go to outputs_v17h_5min/.

Notes
-----
- This file does NOT modify v17g. It imports v17g (which already cascades
  v17f -> v17d -> v17c -> v17b -> v16) and patches on top.
- Gates 1 and 2 require the new cfg attrs ``reversal_volume_max_ratio`` and
  ``reversal_avwap_dist_atr_min`` (added to StrategyConfig in
  avwap_common_v7_sweep.py). The reads live in
  ``_scan_reversal_at`` (avwap_long_strategy_v9_sweep.py); both default to 0
  (disabled) so v17g behavior is unchanged unless v17h sets them.
- Gates 3 and 4 are post-scan because nifty_context_mode and
  nifty_rel_strength_pct are populated by v16's nifty_context filter
  (between scan and final emit).
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17g_5min as _v17g  # noqa: F401 — cascade import
import avwap_combined_runner_v16_5min as _base


# ---------------------------------------------------------------------------
# Local env helpers (mirror the v17g style).
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


# ---------------------------------------------------------------------------
# V17h env toggles (defaults = Patch B as proposed).
# ---------------------------------------------------------------------------
V17H_REVERSAL_VOL_MAX_ON          = _env_bool("EQIDV17H_REVERSAL_VOL_MAX_ON", True)
V17H_REVERSAL_VOL_MAX_RATIO       = _env_float("EQIDV17H_REVERSAL_VOL_MAX_RATIO", 3.0)
V17H_REVERSAL_DIST_MIN_ON         = _env_bool("EQIDV17H_REVERSAL_DIST_MIN_ON", True)
V17H_REVERSAL_DIST_MIN_ATR        = _env_float("EQIDV17H_REVERSAL_DIST_MIN_ATR", 1.0)
V17H_REVERSAL_REQUIRE_BOTH_MODE   = _env_bool("EQIDV17H_REVERSAL_REQUIRE_BOTH_MODE", True)
V17H_REVERSAL_MIN_NIFTY_RS_ON     = _env_bool("EQIDV17H_REVERSAL_MIN_NIFTY_RS_ON", True)
V17H_REVERSAL_MIN_NIFTY_RS_PCT    = _env_float("EQIDV17H_REVERSAL_MIN_NIFTY_RS_PCT", 1.0)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17h_5min/.
# v17g already redirects v17f/v17d/v17c/v17b/v16 -> v17g_5min; we add one
# more redirect layer to land files in v17h_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17h = _base.runtime_dir  # already v17g-patched


def _v17h_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17h_5min",
            "v17g_5min",
            "v17f_5min",
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17h_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17h(*tuple(new_parts))


_base.runtime_dir = _v17h_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: scan-time gates 1 & 2 — wired via cfg attrs read inside
# _scan_reversal_at. Compose on top of v17g's own _v17g_adjust_long_cfg by
# wrapping the long-side scan entry point.
# ---------------------------------------------------------------------------
_v17h_orig_scan_long_prepared = getattr(_base, "scan_long_prepared", None)


def _v17h_adjust_long_cfg(cfg):
    if V17H_REVERSAL_VOL_MAX_ON:
        try:
            cfg.reversal_volume_max_ratio = float(V17H_REVERSAL_VOL_MAX_RATIO)
        except Exception:
            pass
    if V17H_REVERSAL_DIST_MIN_ON:
        try:
            cfg.reversal_avwap_dist_atr_min = float(V17H_REVERSAL_DIST_MIN_ATR)
        except Exception:
            pass
    return cfg


if _v17h_orig_scan_long_prepared is not None:
    def _v17h_scan_long_prepared(ticker, df_prepared, long_cfg):
        long_cfg = _v17h_adjust_long_cfg(long_cfg)
        return _v17h_orig_scan_long_prepared(ticker, df_prepared, long_cfg)

    _base.scan_long_prepared = _v17h_scan_long_prepared


# ---------------------------------------------------------------------------
# PATCH 3: post-scan gates 3 & 4 — drop B_AVWAP_RECLAIM_REVERSAL trades whose
# nifty_context_mode != BOTH or whose nifty_rel_strength_pct is below the
# threshold. Runs AFTER v17g's post-scan stack (which itself chains v17f,
# v17d, v16) so we operate on the final pre-emit long_df.
# ---------------------------------------------------------------------------
_v17g_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17g_get_filter_reason       = _base.get_v16_filter_reason


def _v17h_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17g_apply_post_scan_filters(short_df, long_df)

    if long_df is None or long_df.empty:
        return short_df, long_df
    if "setup" not in long_df.columns:
        return short_df, long_df

    work = long_df.copy()
    is_rev = work["setup"].astype(str).str.strip().eq("B_AVWAP_RECLAIM_REVERSAL")
    if not is_rev.any():
        return short_df, long_df

    drop_mask = pd.Series(False, index=work.index)
    mode_dropped = 0
    rs_dropped = 0

    if V17H_REVERSAL_REQUIRE_BOTH_MODE and "nifty_context_mode" in work.columns:
        mode = work["nifty_context_mode"].astype(str).str.upper().str.strip()
        m_drop = is_rev & mode.ne("BOTH")
        mode_dropped = int(m_drop.sum())
        drop_mask = drop_mask | m_drop

    if V17H_REVERSAL_MIN_NIFTY_RS_ON and "nifty_rel_strength_pct" in work.columns:
        rs = pd.to_numeric(work["nifty_rel_strength_pct"], errors="coerce")
        # NaN-RS rows fail the gate (consistent with v16 behavior on missing context).
        r_drop = is_rev & ~(rs >= float(V17H_REVERSAL_MIN_NIFTY_RS_PCT))
        # Don't double-count rows already dropped by mode.
        r_drop = r_drop & ~drop_mask
        rs_dropped = int(r_drop.sum())
        drop_mask = drop_mask | r_drop

    before = len(work)
    work = work.loc[~drop_mask].copy()
    after = len(work)
    print(
        "[V17H_FILTER] LONG B_AVWAP_RECLAIM_REVERSAL post-scan: "
        f"{before}->{after} (-{mode_dropped} mode!=BOTH "
        f"| -{rs_dropped} nifty_rs<{V17H_REVERSAL_MIN_NIFTY_RS_PCT})"
    )
    return short_df, work


def _v17h_get_filter_reason(row: dict, side: str):
    reason = _v17g_get_filter_reason(row, side)
    if reason is not None:
        return reason

    if str(side).upper().strip() != "LONG":
        return None
    setup = str(row.get("setup", "")).strip()
    if setup != "B_AVWAP_RECLAIM_REVERSAL":
        return None

    if V17H_REVERSAL_REQUIRE_BOTH_MODE:
        mode = str(row.get("nifty_context_mode", "")).upper().strip()
        if mode and mode != "BOTH":
            return (
                f"v17h reversal cleanup: nifty_context_mode={mode} "
                "(require BOTH)"
            )

    if V17H_REVERSAL_MIN_NIFTY_RS_ON:
        try:
            rs = float(row.get("nifty_rel_strength_pct", float("nan")))
        except (TypeError, ValueError):
            rs = float("nan")
        if not (np.isfinite(rs) and rs >= float(V17H_REVERSAL_MIN_NIFTY_RS_PCT)):
            return (
                f"v17h reversal cleanup: nifty_rs={rs:.2f}% "
                f"(require >= {V17H_REVERSAL_MIN_NIFTY_RS_PCT:.2f}%)"
            )

    return None


_base._apply_v16_post_scan_filters = _v17h_apply_post_scan_filters
_base.get_v16_filter_reason = _v17h_get_filter_reason


# ---------------------------------------------------------------------------
# Entry point — print active configuration, then defer to v16 main().
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 78)
    print("V17h 5-min runner: Patch B reversal-setup tightening on top of v17g")
    print("  Output dir: outputs_v17h_5min")
    print("--- Toggles ---")
    print(f"  Gate 1  reversal_vol_max_on        = {V17H_REVERSAL_VOL_MAX_ON} "
          f"(cap = {V17H_REVERSAL_VOL_MAX_RATIO:.2f}x VOL_SMA20)")
    print(f"  Gate 2  reversal_dist_min_on       = {V17H_REVERSAL_DIST_MIN_ON} "
          f"(min = {V17H_REVERSAL_DIST_MIN_ATR:.2f} ATR)")
    print(f"  Gate 3  reversal_require_both_mode = {V17H_REVERSAL_REQUIRE_BOTH_MODE}")
    print(f"  Gate 4  reversal_min_nifty_rs_on   = {V17H_REVERSAL_MIN_NIFTY_RS_ON} "
          f"(min = {V17H_REVERSAL_MIN_NIFTY_RS_PCT:.2f}%)")
    print("--- Inherits all v17g / v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
