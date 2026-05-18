# -*- coding: utf-8 -*-
"""
V17k 5-min combined runner — adds 4 new setups (2 LONG + 2 SHORT) + enables
LONG A_PULLBACK_C2_BREAK on top of v17j.

New setups
----------
LONG:
  - A_PULLBACK_C2_THEN_BREAK_C2_HIGH (already coded; gated on by v17k)
  - C_OR_BREAKOUT (new)            -- opening-range break above OR high
  - D_EMA20_BOUNCE (new)           -- pullback to EMA20, bullish reject

SHORT:
  - C_OR_BREAKDOWN (new)           -- opening-range break below OR low
  - D_EMA20_REJECTION (new)        -- pullback to EMA20 from below, bearish reject

(SHORT mirrors of A_MOD_CCB and B_HUGE_C1_RECLAIM_BREAK plus E_VWAP_BAND_FADE
deferred to v17l.)

Each new setup ships with Patch-B-style post-scan filters: nifty_context_mode
must be aligned with the side, and nifty_rs must clear a directional threshold.

Env knobs (all default ON; flip to 0 to A/B-test each setup independently)
-------------------------------------------------------------------------
  EQIDV17K_LONG_ENABLE_A_PULLBACK             (default True)
  EQIDV17K_LONG_ENABLE_C_OR_BREAKOUT          (default True)
  EQIDV17K_LONG_OR_BREAKOUT_MAX_HOUR          (default 11)
  EQIDV17K_LONG_OR_BREAKOUT_VOL_MIN_RATIO     (default 1.50)
  EQIDV17K_LONG_OR_BREAKOUT_MIN_WIDTH_PCT     (default 0.0050)
  EQIDV17K_LONG_OR_BREAKOUT_MAX_WIDTH_PCT     (default 0.0250)
  EQIDV17K_LONG_ENABLE_D_EMA20_BOUNCE         (default True)
  EQIDV17K_LONG_EMA20_BOUNCE_MAX_HOUR         (default 14)
  EQIDV17K_SHORT_ENABLE_C_OR_BREAKDOWN        (default True)
  EQIDV17K_SHORT_OR_BREAKDOWN_MAX_HOUR        (default 11)
  EQIDV17K_SHORT_ENABLE_D_EMA20_REJECTION     (default True)
  EQIDV17K_SHORT_EMA20_REJECTION_MAX_HOUR     (default 14)
  EQIDV17K_NEW_SETUPS_REQUIRE_BOTH_MODE       (default True)
  EQIDV17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG   (default 1.0)
  EQIDV17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT  (default -1.0)

Outputs go to outputs_v17k_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

from . import v17j_5min as _v17j  # cascade
from . import v16_5min as _base


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
# V17k env toggles.
# ---------------------------------------------------------------------------
V17K_LONG_ENABLE_A_PULLBACK              = _env_bool("EQIDV17K_LONG_ENABLE_A_PULLBACK", True)

V17K_LONG_ENABLE_C_OR_BREAKOUT           = _env_bool("EQIDV17K_LONG_ENABLE_C_OR_BREAKOUT", True)
V17K_LONG_OR_BREAKOUT_MAX_HOUR           = _env_int("EQIDV17K_LONG_OR_BREAKOUT_MAX_HOUR", 11)
V17K_LONG_OR_BREAKOUT_VOL_MIN_RATIO      = _env_float("EQIDV17K_LONG_OR_BREAKOUT_VOL_MIN_RATIO", 1.50)
V17K_LONG_OR_BREAKOUT_MIN_WIDTH_PCT      = _env_float("EQIDV17K_LONG_OR_BREAKOUT_MIN_WIDTH_PCT", 0.0050)
V17K_LONG_OR_BREAKOUT_MAX_WIDTH_PCT      = _env_float("EQIDV17K_LONG_OR_BREAKOUT_MAX_WIDTH_PCT", 0.0250)

V17K_LONG_ENABLE_D_EMA20_BOUNCE          = _env_bool("EQIDV17K_LONG_ENABLE_D_EMA20_BOUNCE", True)
V17K_LONG_EMA20_BOUNCE_MAX_HOUR          = _env_int("EQIDV17K_LONG_EMA20_BOUNCE_MAX_HOUR", 14)

V17K_SHORT_ENABLE_C_OR_BREAKDOWN         = _env_bool("EQIDV17K_SHORT_ENABLE_C_OR_BREAKDOWN", True)
V17K_SHORT_OR_BREAKDOWN_MAX_HOUR         = _env_int("EQIDV17K_SHORT_OR_BREAKDOWN_MAX_HOUR", 11)

V17K_SHORT_ENABLE_D_EMA20_REJECTION      = _env_bool("EQIDV17K_SHORT_ENABLE_D_EMA20_REJECTION", True)
V17K_SHORT_EMA20_REJECTION_MAX_HOUR      = _env_int("EQIDV17K_SHORT_EMA20_REJECTION_MAX_HOUR", 14)

V17K_LONG_ENABLE_E_VWAP_BAND_FADE        = _env_bool("EQIDV17K_LONG_ENABLE_E_VWAP_BAND_FADE", True)
V17K_LONG_VWAP_BAND_FADE_MAX_HOUR        = _env_int("EQIDV17K_LONG_VWAP_BAND_FADE_MAX_HOUR", 14)
V17K_SHORT_ENABLE_E_VWAP_BAND_FADE       = _env_bool("EQIDV17K_SHORT_ENABLE_E_VWAP_BAND_FADE", True)
V17K_SHORT_VWAP_BAND_FADE_MAX_HOUR       = _env_int("EQIDV17K_SHORT_VWAP_BAND_FADE_MAX_HOUR", 14)

V17K_LONG_ENABLE_G_HIGHER_HIGH_BREAK     = _env_bool("EQIDV17K_LONG_ENABLE_G_HIGHER_HIGH_BREAK", True)
V17K_LONG_G_HH_MAX_HOUR                  = _env_int("EQIDV17K_LONG_G_HH_MAX_HOUR", 14)
V17K_LONG_G_HH_LOOKBACK                  = _env_int("EQIDV17K_LONG_G_HH_LOOKBACK", 5)
V17K_SHORT_ENABLE_G_LOWER_LOW_BREAK      = _env_bool("EQIDV17K_SHORT_ENABLE_G_LOWER_LOW_BREAK", True)
V17K_SHORT_G_LL_MAX_HOUR                 = _env_int("EQIDV17K_SHORT_G_LL_MAX_HOUR", 14)
V17K_SHORT_G_LL_LOOKBACK                 = _env_int("EQIDV17K_SHORT_G_LL_LOOKBACK", 5)

V17K_NEW_SETUPS_REQUIRE_BOTH_MODE        = _env_bool("EQIDV17K_NEW_SETUPS_REQUIRE_BOTH_MODE", True)
V17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG    = _env_float("EQIDV17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG", 1.0)
V17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT   = _env_float("EQIDV17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT", -1.0)

NEW_LONG_SETUPS = {"A_PULLBACK_C2_THEN_BREAK_C2_HIGH", "C_OR_BREAKOUT", "D_EMA20_BOUNCE", "E_VWAP_BAND_FADE", "G_HIGHER_HIGH_BREAK"}
NEW_SHORT_SETUPS = {"C_OR_BREAKDOWN", "D_EMA20_REJECTION", "E_VWAP_BAND_FADE", "G_LOWER_LOW_BREAK"}


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17k_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17k = _base.runtime_dir  # already v17j-patched


def _v17k_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17k_5min", "v17j_5min", "v17i_5min", "v17h_5min",
            "v17g_5min", "v17f_5min", "v17d_5min", "v17c_5min",
            "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17k_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17k(*tuple(new_parts))


_base.runtime_dir = _v17k_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: LONG-side cfg adjust — enable new setups + pass tunables.
# ---------------------------------------------------------------------------
_v17k_orig_scan_long_prepared = getattr(_base, "scan_long_prepared", None)


def _v17k_adjust_long_cfg(cfg):
    if V17K_LONG_ENABLE_A_PULLBACK:
        try:
            cfg.enable_setup_a_pullback_c2_break = True
        except Exception:
            pass
    if V17K_LONG_ENABLE_C_OR_BREAKOUT:
        try:
            cfg.enable_setup_c_or_breakout = True
            cfg.or_breakout_max_hour_ist = int(V17K_LONG_OR_BREAKOUT_MAX_HOUR)
            cfg.or_breakout_volume_min_ratio = float(V17K_LONG_OR_BREAKOUT_VOL_MIN_RATIO)
            cfg.or_breakout_min_width_pct = float(V17K_LONG_OR_BREAKOUT_MIN_WIDTH_PCT)
            cfg.or_breakout_max_width_pct = float(V17K_LONG_OR_BREAKOUT_MAX_WIDTH_PCT)
        except Exception:
            pass
    if V17K_LONG_ENABLE_D_EMA20_BOUNCE:
        try:
            cfg.enable_setup_d_ema20_bounce = True
            cfg.ema20_bounce_max_hour_ist = int(V17K_LONG_EMA20_BOUNCE_MAX_HOUR)
        except Exception:
            pass
    if V17K_LONG_ENABLE_E_VWAP_BAND_FADE:
        try:
            cfg.enable_setup_e_vwap_band_fade = True
            cfg.vwap_band_fade_max_hour_ist = int(V17K_LONG_VWAP_BAND_FADE_MAX_HOUR)
        except Exception:
            pass
    if V17K_LONG_ENABLE_G_HIGHER_HIGH_BREAK:
        try:
            cfg.enable_setup_g_higher_high_break = True
            cfg.g_hh_max_hour_ist = int(V17K_LONG_G_HH_MAX_HOUR)
            cfg.g_hh_lookback_bars = int(V17K_LONG_G_HH_LOOKBACK)
        except Exception:
            pass
    return cfg


if _v17k_orig_scan_long_prepared is not None:
    def _v17k_scan_long_prepared(ticker, df_prepared, long_cfg):
        long_cfg = _v17k_adjust_long_cfg(long_cfg)
        return _v17k_orig_scan_long_prepared(ticker, df_prepared, long_cfg)
    _base.scan_long_prepared = _v17k_scan_long_prepared


# ---------------------------------------------------------------------------
# PATCH 3: SHORT-side cfg adjust.
# ---------------------------------------------------------------------------
_v17k_orig_scan_short_prepared = getattr(_base, "scan_short_prepared", None)


def _v17k_adjust_short_cfg(cfg):
    if V17K_SHORT_ENABLE_C_OR_BREAKDOWN:
        try:
            cfg.enable_setup_c_or_breakdown = True
            cfg.or_breakdown_max_hour_ist = int(V17K_SHORT_OR_BREAKDOWN_MAX_HOUR)
        except Exception:
            pass
    if V17K_SHORT_ENABLE_D_EMA20_REJECTION:
        try:
            cfg.enable_setup_d_ema20_rejection = True
            cfg.ema20_rejection_max_hour_ist = int(V17K_SHORT_EMA20_REJECTION_MAX_HOUR)
        except Exception:
            pass
    if V17K_SHORT_ENABLE_E_VWAP_BAND_FADE:
        try:
            cfg.enable_setup_e_vwap_band_fade = True
            cfg.vwap_band_fade_max_hour_ist = int(V17K_SHORT_VWAP_BAND_FADE_MAX_HOUR)
        except Exception:
            pass
    if V17K_SHORT_ENABLE_G_LOWER_LOW_BREAK:
        try:
            cfg.enable_setup_g_lower_low_break = True
            cfg.g_ll_max_hour_ist = int(V17K_SHORT_G_LL_MAX_HOUR)
            cfg.g_ll_lookback_bars = int(V17K_SHORT_G_LL_LOOKBACK)
        except Exception:
            pass
    return cfg


if _v17k_orig_scan_short_prepared is not None:
    def _v17k_scan_short_prepared(ticker, df_prepared, short_cfg):
        short_cfg = _v17k_adjust_short_cfg(short_cfg)
        return _v17k_orig_scan_short_prepared(ticker, df_prepared, short_cfg)
    _base.scan_short_prepared = _v17k_scan_short_prepared


# ---------------------------------------------------------------------------
# PATCH 4: post-scan Patch-B-style gates for new setups.
# Drops new-setup trades whose nifty_context_mode != BOTH or whose
# nifty_rs is on the wrong side of the threshold.
# ---------------------------------------------------------------------------
_v17j_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17j_get_filter_reason       = _base.get_v16_filter_reason


def _v17k_apply_post_scan_filters(
    short_df: pd.DataFrame, long_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17j_apply_post_scan_filters(short_df, long_df)

    # LONG side filter on new setups
    if long_df is not None and not long_df.empty and "setup" in long_df.columns:
        work = long_df.copy()
        is_new = work["setup"].astype(str).str.strip().isin(NEW_LONG_SETUPS)
        if is_new.any():
            drop_mask = pd.Series(False, index=work.index)
            mode_dropped = 0; rs_dropped = 0
            if V17K_NEW_SETUPS_REQUIRE_BOTH_MODE and "nifty_context_mode" in work.columns:
                mode = work["nifty_context_mode"].astype(str).str.upper().str.strip()
                m_drop = is_new & mode.ne("BOTH")
                mode_dropped = int(m_drop.sum())
                drop_mask = drop_mask | m_drop
            if "nifty_rel_strength_pct" in work.columns:
                rs = pd.to_numeric(work["nifty_rel_strength_pct"], errors="coerce")
                r_drop = is_new & ~(rs >= float(V17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG))
                r_drop = r_drop & ~drop_mask
                rs_dropped = int(r_drop.sum())
                drop_mask = drop_mask | r_drop
            before = len(work)
            work = work.loc[~drop_mask].copy()
            print(
                "[V17K_FILTER] LONG new-setups post-scan: "
                f"{before}->{len(work)} (-{mode_dropped} mode!=BOTH "
                f"| -{rs_dropped} nifty_rs<{V17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG})"
            )
            long_df = work

    # SHORT side filter on new setups
    if short_df is not None and not short_df.empty and "setup" in short_df.columns:
        work = short_df.copy()
        is_new = work["setup"].astype(str).str.strip().isin(NEW_SHORT_SETUPS)
        if is_new.any():
            drop_mask = pd.Series(False, index=work.index)
            mode_dropped = 0; rs_dropped = 0
            if V17K_NEW_SETUPS_REQUIRE_BOTH_MODE and "nifty_context_mode" in work.columns:
                mode = work["nifty_context_mode"].astype(str).str.upper().str.strip()
                # SHORT: drop LONG_ONLY mode
                m_drop = is_new & mode.eq("LONG_ONLY")
                mode_dropped = int(m_drop.sum())
                drop_mask = drop_mask | m_drop
            if "nifty_rel_strength_pct" in work.columns:
                rs = pd.to_numeric(work["nifty_rel_strength_pct"], errors="coerce")
                r_drop = is_new & ~(rs <= float(V17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT))
                r_drop = r_drop & ~drop_mask
                rs_dropped = int(r_drop.sum())
                drop_mask = drop_mask | r_drop
            before = len(work)
            work = work.loc[~drop_mask].copy()
            print(
                "[V17K_FILTER] SHORT new-setups post-scan: "
                f"{before}->{len(work)} (-{mode_dropped} mode==LONG_ONLY "
                f"| -{rs_dropped} nifty_rs>{V17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT})"
            )
            short_df = work

    return short_df, long_df


def _v17k_get_filter_reason(row: dict, side: str):
    reason = _v17j_get_filter_reason(row, side)
    if reason is not None:
        return reason
    setup = str(row.get("setup", "")).strip()
    side_u = str(side).upper().strip()
    if side_u == "LONG" and setup in NEW_LONG_SETUPS:
        if V17K_NEW_SETUPS_REQUIRE_BOTH_MODE:
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            if mode and mode != "BOTH":
                return f"v17k new-setup cleanup: nifty_context_mode={mode} (require BOTH)"
        try:
            rs = float(row.get("nifty_rel_strength_pct", float("nan")))
        except (TypeError, ValueError):
            rs = float("nan")
        if not (np.isfinite(rs) and rs >= float(V17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG)):
            return f"v17k new-setup cleanup: nifty_rs={rs:.2f}% (require >= {V17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG:.2f}%)"
    elif side_u == "SHORT" and setup in NEW_SHORT_SETUPS:
        if V17K_NEW_SETUPS_REQUIRE_BOTH_MODE:
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            if mode == "LONG_ONLY":
                return "v17k new-setup cleanup: nifty_context_mode=LONG_ONLY (drop)"
        try:
            rs = float(row.get("nifty_rel_strength_pct", float("nan")))
        except (TypeError, ValueError):
            rs = float("nan")
        if not (np.isfinite(rs) and rs <= float(V17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT)):
            return f"v17k new-setup cleanup: nifty_rs={rs:.2f}% (require <= {V17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT:.2f}%)"
    return None


_base._apply_v16_post_scan_filters = _v17k_apply_post_scan_filters
_base.get_v16_filter_reason = _v17k_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17k 5-min runner: 5 new setups (3 LONG + 2 SHORT) on top of v17j")
    print("  Output dir: outputs_v17k_5min")
    print("--- LONG new setups ---")
    print(f"  A_PULLBACK_C2_BREAK     enabled={V17K_LONG_ENABLE_A_PULLBACK}")
    print(f"  C_OR_BREAKOUT           enabled={V17K_LONG_ENABLE_C_OR_BREAKOUT}"
          f"  (max_hour={V17K_LONG_OR_BREAKOUT_MAX_HOUR}, vol>={V17K_LONG_OR_BREAKOUT_VOL_MIN_RATIO}x,"
          f" width=[{V17K_LONG_OR_BREAKOUT_MIN_WIDTH_PCT*100:.2f}%, {V17K_LONG_OR_BREAKOUT_MAX_WIDTH_PCT*100:.2f}%])")
    print(f"  D_EMA20_BOUNCE          enabled={V17K_LONG_ENABLE_D_EMA20_BOUNCE}"
          f"  (max_hour={V17K_LONG_EMA20_BOUNCE_MAX_HOUR})")
    print("--- SHORT new setups ---")
    print(f"  C_OR_BREAKDOWN          enabled={V17K_SHORT_ENABLE_C_OR_BREAKDOWN}"
          f"  (max_hour={V17K_SHORT_OR_BREAKDOWN_MAX_HOUR})")
    print(f"  D_EMA20_REJECTION       enabled={V17K_SHORT_ENABLE_D_EMA20_REJECTION}"
          f"  (max_hour={V17K_SHORT_EMA20_REJECTION_MAX_HOUR})")
    print(f"  E_VWAP_BAND_FADE        LONG={V17K_LONG_ENABLE_E_VWAP_BAND_FADE} "
          f"(max_hour={V17K_LONG_VWAP_BAND_FADE_MAX_HOUR}) | "
          f"SHORT={V17K_SHORT_ENABLE_E_VWAP_BAND_FADE} "
          f"(max_hour={V17K_SHORT_VWAP_BAND_FADE_MAX_HOUR})")
    print(f"  G_HIGHER_HIGH_BREAK     enabled={V17K_LONG_ENABLE_G_HIGHER_HIGH_BREAK} "
          f"(max_hour={V17K_LONG_G_HH_MAX_HOUR}, lookback={V17K_LONG_G_HH_LOOKBACK})")
    print(f"  G_LOWER_LOW_BREAK       enabled={V17K_SHORT_ENABLE_G_LOWER_LOW_BREAK} "
          f"(max_hour={V17K_SHORT_G_LL_MAX_HOUR}, lookback={V17K_SHORT_G_LL_LOOKBACK})")
    print("--- Patch-B-style new-setup gates ---")
    print(f"  require_both_mode={V17K_NEW_SETUPS_REQUIRE_BOTH_MODE}, "
          f"LONG_min_rs={V17K_NEW_SETUPS_MIN_NIFTY_RS_PCT_LONG}, "
          f"SHORT_max_rs={V17K_NEW_SETUPS_MAX_NIFTY_RS_PCT_SHORT}")
    print("--- Inherits all v17j / v17i / v17h / v17g / v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
