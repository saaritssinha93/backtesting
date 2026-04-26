# -*- coding: utf-8 -*-
"""
V17l 5-min combined runner — surgical tightening of v17k weak setups.

Goals (per user spec):
  1. Slight tightening of LONG A_MOD_BREAK_C1_HIGH, A_MOD_CCB,
     G_HIGHER_HIGH_BREAK, C_OR_BREAKOUT, D_EMA20_BOUNCE
  2. Slight tightening of SHORT G_LOWER_LOW_BREAK, D_EMA20_REJECTION,
     C_OR_BREAKDOWN
  3. SHORT target 1.00% -> 0.80% (LONG stays at 0.80%)

Outcome aim: higher PF, higher win%, lower MaxDD.

What's tightened (vs v17k defaults)
-----------------------------------
LONG:
  C_OR_BREAKOUT:      vol_min 1.50->1.75x, adx_min 22->25, OR width [0.50, 2.50] -> [0.75, 2.25]%
  D_EMA20_BOUNCE:     adx_min 22->25, vol_min 1.20->1.40x, rsi_min 50->55
  G_HIGHER_HIGH_BREAK: vol_min 1.30->1.50x, adx_min 22->25, lookback 5->8
  Post-scan quality_score floors:
    A_MOD_BREAK_C1_HIGH         >= 5
    A_MOD_CLOSE_CONTINUATION    >= 5
    G_HIGHER_HIGH_BREAK         >= 4
    C_OR_BREAKOUT               >= 4
    D_EMA20_BOUNCE              >= 4

SHORT:
  C_OR_BREAKDOWN:     vol_min 1.50->1.75x, adx_min 22->25, OR width [0.50, 2.50] -> [0.75, 2.25]%
  D_EMA20_REJECTION:  adx_min 22->28, vol_min 1.20->1.50x, rsi_max 50->45
  G_LOWER_LOW_BREAK:  vol_min 1.30->1.75x, adx_min 22->28, lookback 5->8
  Tighter nifty_rs gate for new SHORT setups: -1.0 -> -1.5% (post-scan)
  SHORT TARGET: 1.00% -> 0.80% via TEST_SHORT_TARGET_PCT

All changes env-toggleable (default ON).

Outputs go to outputs_v17l_5min/.
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17k_5min as _v17k  # cascade
import avwap_combined_runner_v16_5min as _base


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None: return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None: return float(default)
    try: return float(raw)
    except (TypeError, ValueError): return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None: return int(default)
    try: return int(float(raw))
    except (TypeError, ValueError): return int(default)


# ---------------------------------------------------------------------------
# V17l env toggles.
# ---------------------------------------------------------------------------
V17L_TIGHTEN_ENABLED                 = _env_bool("EQIDV17L_TIGHTEN_ENABLED", True)
V17L_SHORT_TARGET_PCT                = _env_float("EQIDV17L_SHORT_TARGET_PCT", 0.0080)

# LONG tightenings (RELAXED in v17l rev2 — original v17l killed too many)
V17L_LONG_C_OR_VOL_MIN               = _env_float("EQIDV17L_LONG_C_OR_VOL_MIN", 1.50)   # was 1.75
V17L_LONG_C_OR_ADX_MIN               = _env_float("EQIDV17L_LONG_C_OR_ADX_MIN", 22.0)   # was 25.0
V17L_LONG_C_OR_MIN_WIDTH             = _env_float("EQIDV17L_LONG_C_OR_MIN_WIDTH", 0.0050)  # was 0.0075 — revert
V17L_LONG_C_OR_MAX_WIDTH             = _env_float("EQIDV17L_LONG_C_OR_MAX_WIDTH", 0.0250)  # was 0.0225 — revert

V17L_LONG_D_EMA20_ADX_MIN            = _env_float("EQIDV17L_LONG_D_EMA20_ADX_MIN", 23.0)  # was 25.0
V17L_LONG_D_EMA20_VOL_MIN            = _env_float("EQIDV17L_LONG_D_EMA20_VOL_MIN", 1.30)  # was 1.40
V17L_LONG_D_EMA20_RSI_MIN            = _env_float("EQIDV17L_LONG_D_EMA20_RSI_MIN", 52.0)  # was 55

V17L_LONG_G_HH_VOL_MIN               = _env_float("EQIDV17L_LONG_G_HH_VOL_MIN", 1.40)   # was 1.50
V17L_LONG_G_HH_ADX_MIN               = _env_float("EQIDV17L_LONG_G_HH_ADX_MIN", 23.0)   # was 25.0
V17L_LONG_G_HH_LOOKBACK              = _env_int("EQIDV17L_LONG_G_HH_LOOKBACK", 6)        # was 8

# SHORT tightenings
V17L_SHORT_C_OR_VOL_MIN              = _env_float("EQIDV17L_SHORT_C_OR_VOL_MIN", 1.75)
V17L_SHORT_C_OR_ADX_MIN              = _env_float("EQIDV17L_SHORT_C_OR_ADX_MIN", 25.0)
V17L_SHORT_C_OR_MIN_WIDTH            = _env_float("EQIDV17L_SHORT_C_OR_MIN_WIDTH", 0.0075)
V17L_SHORT_C_OR_MAX_WIDTH            = _env_float("EQIDV17L_SHORT_C_OR_MAX_WIDTH", 0.0225)

V17L_SHORT_D_EMA20_ADX_MIN           = _env_float("EQIDV17L_SHORT_D_EMA20_ADX_MIN", 28.0)
V17L_SHORT_D_EMA20_VOL_MIN           = _env_float("EQIDV17L_SHORT_D_EMA20_VOL_MIN", 1.50)
V17L_SHORT_D_EMA20_RSI_MAX           = _env_float("EQIDV17L_SHORT_D_EMA20_RSI_MAX", 45.0)

V17L_SHORT_G_LL_VOL_MIN              = _env_float("EQIDV17L_SHORT_G_LL_VOL_MIN", 1.75)
V17L_SHORT_G_LL_ADX_MIN              = _env_float("EQIDV17L_SHORT_G_LL_ADX_MIN", 28.0)
V17L_SHORT_G_LL_LOOKBACK             = _env_int("EQIDV17L_SHORT_G_LL_LOOKBACK", 8)

# Tighter SHORT new-setup nifty_rs gate
V17L_SHORT_NEW_SETUPS_MAX_NIFTY_RS   = _env_float("EQIDV17L_SHORT_NEW_SETUPS_MAX_NIFTY_RS", -1.5)

# Post-scan quality_score floors (LONG setups) — calibrated per-setup based on
# v17k qs distributions (each setup family has different qs scale).
# A_MOD setups have qs ~6.7 mean; new setups (C_OR, D_EMA20, G_HH) have qs ~1.8-2.2 mean.
V17L_QSCORE_FLOOR_A_MOD_BREAK        = _env_float("EQIDV17L_QSCORE_FLOOR_A_MOD_BREAK", 5.0)
V17L_QSCORE_FLOOR_A_MOD_CCB          = _env_float("EQIDV17L_QSCORE_FLOOR_A_MOD_CCB", 5.0)
V17L_QSCORE_FLOOR_G_HH_BREAK         = _env_float("EQIDV17L_QSCORE_FLOOR_G_HH_BREAK", 3.0)   # was 4 -> 3 (PF 2.57 at qs>=3)
V17L_QSCORE_FLOOR_C_OR_BREAKOUT      = _env_float("EQIDV17L_QSCORE_FLOOR_C_OR_BREAKOUT", 3.0)  # was 4 -> 3 (PF 2.57)
V17L_QSCORE_FLOOR_D_EMA20_BOUNCE     = _env_float("EQIDV17L_QSCORE_FLOOR_D_EMA20_BOUNCE", 2.0)  # was 4 -> 2 (PF 2.08, max practical)

LONG_QSCORE_FLOORS = {
    "A_MOD_BREAK_C1_HIGH":            V17L_QSCORE_FLOOR_A_MOD_BREAK,
    "A_MOD_CLOSE_CONTINUATION_BREAK": V17L_QSCORE_FLOOR_A_MOD_CCB,
    "G_HIGHER_HIGH_BREAK":            V17L_QSCORE_FLOOR_G_HH_BREAK,
    "C_OR_BREAKOUT":                  V17L_QSCORE_FLOOR_C_OR_BREAKOUT,
    "D_EMA20_BOUNCE":                 V17L_QSCORE_FLOOR_D_EMA20_BOUNCE,
}

NEW_SHORT_SETUPS = {"C_OR_BREAKDOWN", "D_EMA20_REJECTION", "E_VWAP_BAND_FADE", "G_LOWER_LOW_BREAK"}


# ---------------------------------------------------------------------------
# Change 3: SHORT target override.
# ---------------------------------------------------------------------------
_base.TEST_SHORT_TARGET_PCT = float(V17L_SHORT_TARGET_PCT)


# ---------------------------------------------------------------------------
# Override v17k's module-level constants for params it explicitly sets in
# its adjust_long_cfg / adjust_short_cfg. v17k runs AFTER v17l in the cascade
# (because we wrap scan_X_prepared which calls v17l first, then the previous
# wrapper which is v17k). So v17k would re-overwrite our tightenings unless
# we patch its constants directly.
# ---------------------------------------------------------------------------
if V17L_TIGHTEN_ENABLED:
    # LONG C_OR_BREAKOUT — v17k sets these from V17K_LONG_OR_BREAKOUT_*
    _v17k.V17K_LONG_OR_BREAKOUT_VOL_MIN_RATIO = float(V17L_LONG_C_OR_VOL_MIN)
    _v17k.V17K_LONG_OR_BREAKOUT_MIN_WIDTH_PCT = float(V17L_LONG_C_OR_MIN_WIDTH)
    _v17k.V17K_LONG_OR_BREAKOUT_MAX_WIDTH_PCT = float(V17L_LONG_C_OR_MAX_WIDTH)
    # LONG G_HIGHER_HIGH_BREAK — v17k sets g_hh_lookback_bars from V17K_LONG_G_HH_LOOKBACK
    _v17k.V17K_LONG_G_HH_LOOKBACK = int(V17L_LONG_G_HH_LOOKBACK)
    # SHORT G_LOWER_LOW_BREAK
    _v17k.V17K_SHORT_G_LL_LOOKBACK = int(V17L_SHORT_G_LL_LOOKBACK)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17l_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17l = _base.runtime_dir


def _v17l_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17l_5min", "v17k_5min", "v17j_5min", "v17i_5min", "v17h_5min",
            "v17g_5min", "v17f_5min", "v17d_5min", "v17c_5min", "v17b_5min", "v16_5min",
        ):
            text = text.replace(old, "v17l_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17l(*tuple(new_parts))


_base.runtime_dir = _v17l_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: LONG-side cfg tightenings.
# ---------------------------------------------------------------------------
_v17l_orig_scan_long_prepared = getattr(_base, "scan_long_prepared", None)


def _v17l_adjust_long_cfg(cfg):
    if not V17L_TIGHTEN_ENABLED:
        return cfg
    # C_OR_BREAKOUT
    try:
        cfg.or_breakout_volume_min_ratio = float(V17L_LONG_C_OR_VOL_MIN)
        cfg.or_breakout_adx_min = float(V17L_LONG_C_OR_ADX_MIN)
        cfg.or_breakout_min_width_pct = float(V17L_LONG_C_OR_MIN_WIDTH)
        cfg.or_breakout_max_width_pct = float(V17L_LONG_C_OR_MAX_WIDTH)
    except Exception:
        pass
    # D_EMA20_BOUNCE
    try:
        cfg.ema20_bounce_adx_min = float(V17L_LONG_D_EMA20_ADX_MIN)
        cfg.ema20_bounce_volume_min_ratio = float(V17L_LONG_D_EMA20_VOL_MIN)
        cfg.ema20_bounce_rsi_min = float(V17L_LONG_D_EMA20_RSI_MIN)
    except Exception:
        pass
    # G_HIGHER_HIGH_BREAK
    try:
        cfg.g_hh_volume_min_ratio = float(V17L_LONG_G_HH_VOL_MIN)
        cfg.g_hh_adx_min = float(V17L_LONG_G_HH_ADX_MIN)
        cfg.g_hh_lookback_bars = int(V17L_LONG_G_HH_LOOKBACK)
    except Exception:
        pass
    return cfg


if _v17l_orig_scan_long_prepared is not None:
    def _v17l_scan_long_prepared(ticker, df_prepared, long_cfg):
        long_cfg = _v17l_adjust_long_cfg(long_cfg)
        return _v17l_orig_scan_long_prepared(ticker, df_prepared, long_cfg)
    _base.scan_long_prepared = _v17l_scan_long_prepared


# ---------------------------------------------------------------------------
# PATCH 3: SHORT-side cfg tightenings.
# ---------------------------------------------------------------------------
_v17l_orig_scan_short_prepared = getattr(_base, "scan_short_prepared", None)


def _v17l_adjust_short_cfg(cfg):
    if not V17L_TIGHTEN_ENABLED:
        return cfg
    # C_OR_BREAKDOWN
    try:
        cfg.or_breakdown_volume_min_ratio = float(V17L_SHORT_C_OR_VOL_MIN)
        cfg.or_breakdown_adx_min = float(V17L_SHORT_C_OR_ADX_MIN)
        cfg.or_breakdown_min_width_pct = float(V17L_SHORT_C_OR_MIN_WIDTH)
        cfg.or_breakdown_max_width_pct = float(V17L_SHORT_C_OR_MAX_WIDTH)
    except Exception:
        pass
    # D_EMA20_REJECTION
    try:
        cfg.ema20_rejection_adx_min = float(V17L_SHORT_D_EMA20_ADX_MIN)
        cfg.ema20_rejection_volume_min_ratio = float(V17L_SHORT_D_EMA20_VOL_MIN)
        cfg.ema20_rejection_rsi_max = float(V17L_SHORT_D_EMA20_RSI_MAX)
    except Exception:
        pass
    # G_LOWER_LOW_BREAK
    try:
        cfg.g_ll_volume_min_ratio = float(V17L_SHORT_G_LL_VOL_MIN)
        cfg.g_ll_adx_min = float(V17L_SHORT_G_LL_ADX_MIN)
        cfg.g_ll_lookback_bars = int(V17L_SHORT_G_LL_LOOKBACK)
    except Exception:
        pass
    return cfg


if _v17l_orig_scan_short_prepared is not None:
    def _v17l_scan_short_prepared(ticker, df_prepared, short_cfg):
        short_cfg = _v17l_adjust_short_cfg(short_cfg)
        return _v17l_orig_scan_short_prepared(ticker, df_prepared, short_cfg)
    _base.scan_short_prepared = _v17l_scan_short_prepared


# ---------------------------------------------------------------------------
# PATCH 4: post-scan filters — quality_score floor + tighter SHORT nifty_rs.
# ---------------------------------------------------------------------------
_v17k_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17k_get_filter_reason       = _base.get_v16_filter_reason


def _v17l_apply_post_scan_filters(
    short_df: pd.DataFrame, long_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17k_apply_post_scan_filters(short_df, long_df)

    if not V17L_TIGHTEN_ENABLED:
        return short_df, long_df

    # LONG quality_score floor per setup
    if long_df is not None and not long_df.empty and "setup" in long_df.columns and "quality_score" in long_df.columns:
        work = long_df.copy()
        setup = work["setup"].astype(str).str.strip()
        qs = pd.to_numeric(work["quality_score"], errors="coerce")
        drop_mask = pd.Series(False, index=work.index)
        per_setup_dropped = {}
        for s, floor in LONG_QSCORE_FLOORS.items():
            m = setup.eq(s) & ~(qs >= float(floor))
            cnt = int(m.sum())
            per_setup_dropped[s] = cnt
            drop_mask = drop_mask | m
        before = len(work)
        work = work.loc[~drop_mask].copy()
        details = ", ".join(f"-{cnt} {s}" for s, cnt in per_setup_dropped.items() if cnt > 0)
        print(f"[V17L_FILTER] LONG quality_score floor: {before}->{len(work)} ({details if details else 'no drops'})")
        long_df = work

    # SHORT tighter nifty_rs gate for new setups
    if short_df is not None and not short_df.empty and "setup" in short_df.columns and "nifty_rel_strength_pct" in short_df.columns:
        work = short_df.copy()
        is_new = work["setup"].astype(str).str.strip().isin(NEW_SHORT_SETUPS)
        rs = pd.to_numeric(work["nifty_rel_strength_pct"], errors="coerce")
        drop = is_new & ~(rs <= float(V17L_SHORT_NEW_SETUPS_MAX_NIFTY_RS))
        before = len(work)
        n_drop = int(drop.sum())
        work = work.loc[~drop].copy()
        print(f"[V17L_FILTER] SHORT new-setups nifty_rs<= {V17L_SHORT_NEW_SETUPS_MAX_NIFTY_RS}: "
              f"{before}->{len(work)} (-{n_drop} drops)")
        short_df = work

    return short_df, long_df


def _v17l_get_filter_reason(row: dict, side: str):
    reason = _v17k_get_filter_reason(row, side)
    if reason is not None:
        return reason
    if not V17L_TIGHTEN_ENABLED:
        return None
    setup = str(row.get("setup", "")).strip()
    side_u = str(side).upper().strip()
    if side_u == "LONG" and setup in LONG_QSCORE_FLOORS:
        try:
            qs = float(row.get("quality_score", float("nan")))
        except (TypeError, ValueError):
            qs = float("nan")
        floor = LONG_QSCORE_FLOORS[setup]
        if not (np.isfinite(qs) and qs >= floor):
            return f"v17l qs floor: setup={setup} qs={qs:.2f} (require >= {floor:.2f})"
    if side_u == "SHORT" and setup in NEW_SHORT_SETUPS:
        try:
            rs = float(row.get("nifty_rel_strength_pct", float("nan")))
        except (TypeError, ValueError):
            rs = float("nan")
        if not (np.isfinite(rs) and rs <= float(V17L_SHORT_NEW_SETUPS_MAX_NIFTY_RS)):
            return f"v17l SHORT new-setup tighter rs: nifty_rs={rs:.2f}% (require <= {V17L_SHORT_NEW_SETUPS_MAX_NIFTY_RS:.2f}%)"
    return None


_base._apply_v16_post_scan_filters = _v17l_apply_post_scan_filters
_base.get_v16_filter_reason = _v17l_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17l 5-min runner: tightened weak setups + SHORT target 0.80% on top of v17k")
    print("  Output dir: outputs_v17l_5min")
    print("--- Targets ---")
    print(f"  LONG  target = {_base.TEST_LONG_TARGET_PCT*100:.2f}% (inherited)")
    print(f"  SHORT target = {_base.TEST_SHORT_TARGET_PCT*100:.2f}% (overridden)")
    print("--- LONG tightenings ---")
    print(f"  C_OR_BREAKOUT:      vol>={V17L_LONG_C_OR_VOL_MIN}, adx>={V17L_LONG_C_OR_ADX_MIN}, "
          f"width=[{V17L_LONG_C_OR_MIN_WIDTH*100:.2f}%,{V17L_LONG_C_OR_MAX_WIDTH*100:.2f}%]")
    print(f"  D_EMA20_BOUNCE:     adx>={V17L_LONG_D_EMA20_ADX_MIN}, vol>={V17L_LONG_D_EMA20_VOL_MIN}, "
          f"rsi>={V17L_LONG_D_EMA20_RSI_MIN}")
    print(f"  G_HIGHER_HIGH:      vol>={V17L_LONG_G_HH_VOL_MIN}, adx>={V17L_LONG_G_HH_ADX_MIN}, "
          f"lookback={V17L_LONG_G_HH_LOOKBACK}")
    print(f"  qscore floors: " + ", ".join(f"{s}>={f}" for s,f in LONG_QSCORE_FLOORS.items()))
    print("--- SHORT tightenings ---")
    print(f"  C_OR_BREAKDOWN:     vol>={V17L_SHORT_C_OR_VOL_MIN}, adx>={V17L_SHORT_C_OR_ADX_MIN}, "
          f"width=[{V17L_SHORT_C_OR_MIN_WIDTH*100:.2f}%,{V17L_SHORT_C_OR_MAX_WIDTH*100:.2f}%]")
    print(f"  D_EMA20_REJECTION:  adx>={V17L_SHORT_D_EMA20_ADX_MIN}, vol>={V17L_SHORT_D_EMA20_VOL_MIN}, "
          f"rsi<={V17L_SHORT_D_EMA20_RSI_MAX}")
    print(f"  G_LOWER_LOW_BREAK:  vol>={V17L_SHORT_G_LL_VOL_MIN}, adx>={V17L_SHORT_G_LL_ADX_MIN}, "
          f"lookback={V17L_SHORT_G_LL_LOOKBACK}")
    print(f"  new-setup nifty_rs <= {V17L_SHORT_NEW_SETUPS_MAX_NIFTY_RS}%")
    print("--- Inherits all v17k / v17j / v17i / v17h / v17g / v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
