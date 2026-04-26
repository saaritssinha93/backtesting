"""
eqidv2_early_emit_v16_5min.py

Early-emit CANDIDATE rows for the V16 5min setup family. SEE calls
`evaluate_slot_candidates` at every 5-min slot; this module returns the set
of candidate rows that become visible at that slot close.

Coverage (all currently enabled V16 5min setups):
  C1-anchored (slot_ts = C1 close):
    - A_MOD_BREAK_C1_HIGH             (LONG, lag=1)      Phase 2a
    - A_MOD_BREAK_C1_LOW              (SHORT, lag=1)     Phase 2a
    - A_MOD_CLOSE_CONTINUATION_BREAK  (LONG, lag=2)      Phase 2b
    - B_HUGE_C1_CLOSE_RECLAIM_BREAK   (LONG, lag=2)      Phase 2b
  C2-anchored (slot_ts = C2 close, C1 at slot-1):
    - A_PULLBACK_C2_THEN_BREAK_C2_HIGH (LONG, lag=2)     Phase 2c
    - A_PULLBACK_C2_THEN_BREAK_C2_LOW  (SHORT, lag=2)    Phase 2c
  Bounce-anchored (slot_ts = bounce_end close, C1 at slot-3):
    - B_HUGE_RED_FAILED_BOUNCE        (SHORT, dynamic)   Phase 2d

Skipped (dead in V16): B_HUGE_PULLBACK_HOLD_BREAK (lag=999).

The candidate dict carries both `c1_iso` (scanner-canonical signal_time_ist =
C1 close stamp) and `entry_iso` (expected entry-bar OPEN stamp). The runner
uses c1_iso as signal_time_ist so downstream dedup/F3 remains in parity with
scanner output.

Design decisions: docs_early_emit/phase0_design_decisions.md
Setup map:        docs_early_emit/setup_early_emit_map.md
Phase 2 plan:     docs_early_emit/phase2_plan.md
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pytz

from avwap_v11_refactored.avwap_common_v11 import (
    in_signal_window,
    entry_buffer,
    volume_filter_pass,
    has_recent_liquidity_sweep,
    avwap_no_trade_zone_block,
    market_regime_pass,
    select_day_mode,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    classify_red_impulse,
    _trend_filter_short,
    _reversal_filter_short,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    classify_green_impulse,
    _trend_filter_long,
)

IST = pytz.timezone("Asia/Kolkata")
POOL_VERSION = "v16_5min_early_emit_v2"
POOL_DIR_DEFAULT = Path("C:/TradingData/eqidv2/live_signals")
SLOT_MINUTES = 5


def _compute_signal_id(ticker: str, side: str, c1_iso: str, setup: str) -> str:
    # Key on C1 close (scanner-canonical signal_time), so C2- and bounce-anchored
    # candidates dedup against themselves across re-evaluation, not across C1/C2.
    key = f"{ticker}|{side}|{c1_iso}|{setup}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _find_bar_index_at_slot(df_prepared: pd.DataFrame, slot_ts: pd.Timestamp) -> Optional[int]:
    if df_prepared.empty:
        return None
    dates = pd.to_datetime(df_prepared["date"])
    if dates.dt.tz is None:
        dates = dates.dt.tz_localize(IST)
    else:
        dates = dates.dt.tz_convert(IST)
    mask = dates == slot_ts
    if not mask.any():
        return None
    return int(np.where(mask)[0][-1])


def _bar_iso(df_prepared: pd.DataFrame, idx: int) -> str:
    ts = pd.Timestamp(df_prepared.iloc[idx]["date"])
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts.isoformat()


def _compute_day_mode_live(df_prepared: pd.DataFrame, cfg) -> str:
    # Live-mode parity: df_prepared is filtered to today; prev_close unavailable → pass None.
    try:
        mode, _gap, _or_w = select_day_mode(df_prepared, cfg, prev_close=None)
        return str(mode)
    except Exception:
        return "trend"


def _short_c1_preconds_pass(
    df_prepared: pd.DataFrame,
    i: int,
    cfg,
    day_mode: str,
    target_impulse: str = "MODERATE",
) -> Optional[Dict[str, Any]]:
    """Mirror of short scan_one_day lines 403-477 (preconds only, no break-level check)."""
    if i < 2 or i >= len(df_prepared):
        return None
    c1 = df_prepared.iloc[i]

    if not in_signal_window(c1["date"], cfg):
        return None
    impulse = classify_red_impulse(c1, cfg)
    if impulse != target_impulse:
        return None
    if not market_regime_pass(c1["date"], "SHORT", cfg):
        return None

    try:
        atr1 = float(c1["ATR15"])
        close1 = float(c1["close"])
    except Exception:
        return None
    if not (np.isfinite(atr1) and atr1 > 0 and np.isfinite(close1) and close1 > 0):
        return None
    if getattr(cfg, "use_atr_pct_filter", False) and (atr1 / close1) < float(cfg.atr_pct_min):
        return None
    if not volume_filter_pass(c1, cfg):
        return None

    if day_mode == "trend":
        if not _trend_filter_short(df_prepared, i, c1, cfg):
            return None
    else:
        if not _reversal_filter_short(df_prepared, i, c1, cfg):
            return None

    require_sweep = bool(
        getattr(cfg, "enable_liquidity_sweep_filter", False)
        or (day_mode == "reversal" and getattr(cfg, "reversal_requires_sweep", False))
    )
    has_sweep_ctx = False
    if require_sweep:
        sweep_cfg = cfg if cfg.enable_liquidity_sweep_filter else dataclasses.replace(
            cfg, enable_liquidity_sweep_filter=True
        )
        has_sweep_ctx = has_recent_liquidity_sweep(df_prepared, i, "SHORT", sweep_cfg)
        if not has_sweep_ctx:
            return None

    if avwap_no_trade_zone_block(c1, impulse, has_sweep_ctx, cfg):
        return None

    avwap_raw = c1.get("AVWAP", np.nan)
    avwap1 = float(avwap_raw) if np.isfinite(avwap_raw) else np.nan
    avwap_dist_atr = (avwap1 - close1) / atr1 if np.isfinite(avwap1) else 0.0
    max_dist = float(getattr(cfg, "signal_avwap_dist_atr_max", 0.0))
    if max_dist > 0 and np.isfinite(avwap_dist_atr) and avwap_dist_atr > max_dist:
        return None

    return {
        "impulse": impulse,
        "day_mode": day_mode,
        "sweep_ctx": bool(has_sweep_ctx),
        "avwap_dist_atr": float(avwap_dist_atr),
        "c1_close": float(close1),
        "c1_atr": float(atr1),
    }


def _long_c1_preconds_pass(
    df_prepared: pd.DataFrame,
    i: int,
    cfg,
    target_impulse: str = "MODERATE",
) -> Optional[Dict[str, Any]]:
    """Mirror of long scan_one_day lines 362-403 (preconds only, no break-level check)."""
    if i < 2 or i >= len(df_prepared):
        return None
    c1 = df_prepared.iloc[i]

    if not in_signal_window(c1["date"], cfg):
        return None
    impulse = classify_green_impulse(c1, cfg)
    if impulse != target_impulse:
        return None
    if not market_regime_pass(c1["date"], "LONG", cfg):
        return None

    try:
        atr1 = float(c1["ATR15"])
        close1 = float(c1["close"])
    except Exception:
        return None
    if not (np.isfinite(atr1) and atr1 > 0 and np.isfinite(close1) and close1 > 0):
        return None
    if getattr(cfg, "use_atr_pct_filter", False) and (atr1 / close1) < float(cfg.atr_pct_min):
        return None
    if not volume_filter_pass(c1, cfg):
        return None
    if not _trend_filter_long(df_prepared, i, c1, cfg):
        return None

    has_sweep_ctx = has_recent_liquidity_sweep(df_prepared, i, "LONG", cfg)
    if not has_sweep_ctx:
        return None
    if avwap_no_trade_zone_block(c1, impulse, has_sweep_ctx, cfg):
        return None

    avwap_raw = c1.get("AVWAP", np.nan)
    avwap1 = float(avwap_raw) if np.isfinite(avwap_raw) else np.nan
    avwap_dist_atr = (close1 - avwap1) / atr1 if np.isfinite(avwap1) else 0.0

    return {
        "impulse": impulse,
        "sweep_ctx": bool(has_sweep_ctx),
        "avwap_dist_atr": float(avwap_dist_atr),
        "c1_close": float(close1),
        "c1_atr": float(atr1),
    }


def _long_pullback_c2_shape_ok(df_prepared: pd.DataFrame, i_c1: int, i_c2: int, cfg) -> bool:
    """Mirror of long scanner lines 600-610: small-red C2 body AND c2.close > c2.AVWAP."""
    if i_c2 >= len(df_prepared):
        return False
    c1 = df_prepared.iloc[i_c1]
    c2 = df_prepared.iloc[i_c2]
    try:
        c2o = float(c2["open"])
        c2c = float(c2["close"])
    except Exception:
        return False
    c2_body = abs(c2c - c2o)
    c2_atr = float(c2["ATR15"]) if np.isfinite(c2.get("ATR15", np.nan)) else float(c1.get("ATR15", np.nan))
    c2_avwap = float(c2["AVWAP"]) if np.isfinite(c2.get("AVWAP", np.nan)) else np.nan
    small_red = (c2c < c2o) and np.isfinite(c2_atr) and (c2_body <= float(cfg.small_counter_max_atr) * c2_atr)
    above_avwap = np.isfinite(c2_avwap) and (c2c > c2_avwap)
    return bool(small_red and above_avwap)


def _short_pullback_c2_shape_ok(df_prepared: pd.DataFrame, i_c1: int, i_c2: int, cfg) -> bool:
    """Mirror of short scanner lines 586-592: small-green C2 body AND c2.close < c2.AVWAP."""
    if i_c2 >= len(df_prepared):
        return False
    c1 = df_prepared.iloc[i_c1]
    c2 = df_prepared.iloc[i_c2]
    try:
        c2o = float(c2["open"])
        c2c = float(c2["close"])
    except Exception:
        return False
    c2_body = abs(c2c - c2o)
    c2_atr_raw = c2.get("ATR15", np.nan)
    c2_atr = float(c2_atr_raw) if np.isfinite(c2_atr_raw) else float(c1.get("ATR15", np.nan))
    c2_avwap_raw = c2.get("AVWAP", np.nan)
    c2_avwap = float(c2_avwap_raw) if np.isfinite(c2_avwap_raw) else np.nan
    small_green = (c2c > c2o) and np.isfinite(c2_atr) and (c2_body <= float(cfg.small_counter_max_atr) * c2_atr)
    below_avwap = np.isfinite(c2_avwap) and (c2c < c2_avwap)
    return bool(small_green and below_avwap)


def _short_bounce_check(
    df_prepared: pd.DataFrame,
    i_c1: int,
    bounce_end_idx: int,
    atr1: float,
    cfg,
) -> Optional[Dict[str, Any]]:
    """Mirror of short scanner lines 626-657: bounce shape + optional touch-fail + bounce_low."""
    if bounce_end_idx >= len(df_prepared) or bounce_end_idx < i_c1 + 3:
        return None
    # Scanner: bounce = df_day.iloc[i+1 : bounce_end+1]  (bars i+1, i+2, i+3)
    bounce = df_prepared.iloc[i_c1 + 1 : bounce_end_idx + 1].copy()
    if bounce.empty:
        return None

    closes = pd.to_numeric(bounce["close"], errors="coerce")
    opens = pd.to_numeric(bounce["open"], errors="coerce")
    bounce_atr = pd.to_numeric(bounce.get("ATR15", atr1), errors="coerce").fillna(atr1)
    bounce_body = (closes - opens).abs()
    bounce_green = closes > opens
    bounce_small = bounce_body <= (float(cfg.small_counter_max_atr) * bounce_atr)

    if not bool((bounce_green & bounce_small).any()):
        return None

    if bool(getattr(cfg, "require_avwap_rule", False)) and bool(getattr(cfg, "avwap_touch", False)):
        avwaps = pd.to_numeric(bounce["AVWAP"], errors="coerce")
        highs = pd.to_numeric(bounce["high"], errors="coerce")
        touch_fail = bool(((highs >= avwaps) & (closes < avwaps)).fillna(False).any())
        if not touch_fail:
            return None

    bounce_low = float(pd.to_numeric(bounce["low"], errors="coerce").min())
    if not np.isfinite(bounce_low):
        return None
    return {"bounce_low": bounce_low}


def evaluate_slot_candidates(
    ticker: str,
    df_prepared: pd.DataFrame,
    slot_ts: pd.Timestamp,
    short_cfg,
    long_cfg,
) -> List[Dict[str, Any]]:
    """At slot_ts, emit CANDIDATE rows for every setup whose anchor bar is the
    current slot. Three anchor types are considered:
      * C1-anchored: slot bar itself is C1 (A_MOD_*, A_MOD_CLOSE_CONTINUATION, B_HUGE_C1_CLOSE_RECLAIM)
      * C2-anchored: slot bar is C2 (A_PULLBACK_C2_*), C1 at slot-1
      * Bounce-anchored: slot bar is bounce_end=i+3 (B_HUGE_RED_FAILED_BOUNCE), C1 at slot-3
    """
    if df_prepared is None or df_prepared.empty:
        return []
    slot_ts = pd.Timestamp(slot_ts)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)

    df_prepared = df_prepared.reset_index(drop=True)

    i = _find_bar_index_at_slot(df_prepared, slot_ts)
    if i is None:
        return []

    trigger_iso = slot_ts.isoformat()
    emitted_at_iso = datetime.now(IST).isoformat()

    day_mode_short = _compute_day_mode_live(df_prepared, short_cfg)

    out: List[Dict[str, Any]] = []

    # ---------------- C1-anchored (i_c1 = i) ----------------
    if i >= 2:
        c1 = df_prepared.iloc[i]
        c1_iso = _bar_iso(df_prepared, i)

        # SHORT A_MOD_BREAK_C1_LOW (MODERATE, lag=1)
        short_mod_snap = _short_c1_preconds_pass(
            df_prepared, i, short_cfg, day_mode_short, target_impulse="MODERATE"
        )
        if short_mod_snap is not None:
            low1 = float(c1["low"])
            break_level = low1 - entry_buffer(low1, short_cfg)
            entry_iso = (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 1)).isoformat()
            setup = "A_MOD_BREAK_C1_LOW"
            out.append({
                "signal_id": _compute_signal_id(ticker, "SHORT", c1_iso, setup),
                "ticker": ticker,
                "side": "SHORT",
                "setup": setup,
                "trigger_iso": trigger_iso,
                "c1_iso": c1_iso,
                "entry_iso": entry_iso,
                "break_level": round(float(break_level), 4),
                "entry_window_start_iso": entry_iso,
                "entry_window_end_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 2)).isoformat(),
                "state": "CANDIDATE_WRITTEN",
                "emitted_at_iso": emitted_at_iso,
                "c1_close": short_mod_snap["c1_close"],
                "c1_atr": short_mod_snap["c1_atr"],
                "preconds_snapshot": short_mod_snap,
            })

        # LONG A_MOD_BREAK_C1_HIGH (MODERATE, lag=1)
        long_mod_snap = _long_c1_preconds_pass(
            df_prepared, i, long_cfg, target_impulse="MODERATE"
        )
        if long_mod_snap is not None:
            high1 = float(c1["high"])
            break_level = high1 + entry_buffer(high1, long_cfg)
            entry_iso = (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 1)).isoformat()
            setup = "A_MOD_BREAK_C1_HIGH"
            out.append({
                "signal_id": _compute_signal_id(ticker, "LONG", c1_iso, setup),
                "ticker": ticker,
                "side": "LONG",
                "setup": setup,
                "trigger_iso": trigger_iso,
                "c1_iso": c1_iso,
                "entry_iso": entry_iso,
                "break_level": round(float(break_level), 4),
                "entry_window_start_iso": entry_iso,
                "entry_window_end_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 2)).isoformat(),
                "state": "CANDIDATE_WRITTEN",
                "emitted_at_iso": emitted_at_iso,
                "c1_close": long_mod_snap["c1_close"],
                "c1_atr": long_mod_snap["c1_atr"],
                "preconds_snapshot": long_mod_snap,
            })

            # LONG A_MOD_CLOSE_CONTINUATION_BREAK (MODERATE, lag=2)
            if bool(getattr(long_cfg, "enable_setup_a_close_continuation_break", False)):
                close1 = float(c1["close"])
                close_trigger = close1 + entry_buffer(close1, long_cfg)
                lag_close = int(getattr(long_cfg, "lag_bars_long_a_close_continuation_break", 2))
                if lag_close < 0:
                    lag_close = 2
                entry_iso = (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * lag_close)).isoformat()
                setup = "A_MOD_CLOSE_CONTINUATION_BREAK"
                out.append({
                    "signal_id": _compute_signal_id(ticker, "LONG", c1_iso, setup),
                    "ticker": ticker,
                    "side": "LONG",
                    "setup": setup,
                    "trigger_iso": trigger_iso,
                    "c1_iso": c1_iso,
                    "entry_iso": entry_iso,
                    "break_level": round(float(close_trigger), 4),
                    "entry_window_start_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES)).isoformat(),
                    "entry_window_end_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * (lag_close + 1))).isoformat(),
                    "state": "CANDIDATE_WRITTEN",
                    "emitted_at_iso": emitted_at_iso,
                    "c1_close": long_mod_snap["c1_close"],
                    "c1_atr": long_mod_snap["c1_atr"],
                    "preconds_snapshot": long_mod_snap,
                })

        # LONG B_HUGE_C1_CLOSE_RECLAIM_BREAK (HUGE, lag=2) — independent of MODERATE pass
        if bool(getattr(long_cfg, "enable_setup_b_huge_c1_close_reclaim_break", False)):
            long_huge_snap = _long_c1_preconds_pass(
                df_prepared, i, long_cfg, target_impulse="HUGE"
            )
            if long_huge_snap is not None:
                close1 = float(c1["close"])
                reclaim_trigger = close1 + entry_buffer(close1, long_cfg)
                lag_reclaim = int(getattr(long_cfg, "lag_bars_long_b_huge_c1_close_reclaim_break", 2))
                if lag_reclaim < 0:
                    lag_reclaim = 2
                entry_iso = (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * lag_reclaim)).isoformat()
                setup = "B_HUGE_C1_CLOSE_RECLAIM_BREAK"
                out.append({
                    "signal_id": _compute_signal_id(ticker, "LONG", c1_iso, setup),
                    "ticker": ticker,
                    "side": "LONG",
                    "setup": setup,
                    "trigger_iso": trigger_iso,
                    "c1_iso": c1_iso,
                    "entry_iso": entry_iso,
                    "break_level": round(float(reclaim_trigger), 4),
                    "entry_window_start_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES)).isoformat(),
                    "entry_window_end_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * (lag_reclaim + 1))).isoformat(),
                    "state": "CANDIDATE_WRITTEN",
                    "emitted_at_iso": emitted_at_iso,
                    "c1_close": long_huge_snap["c1_close"],
                    "c1_atr": long_huge_snap["c1_atr"],
                    "preconds_snapshot": long_huge_snap,
                })

    # ---------------- C2-anchored (i_c1 = i-1, i_c2 = i; needs i>=3) ----------------
    if i >= 3:
        i_c1 = i - 1
        i_c2 = i
        c1_iso = _bar_iso(df_prepared, i_c1)
        c2 = df_prepared.iloc[i_c2]
        entry_iso = (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 1)).isoformat()

        # LONG A_PULLBACK_C2_THEN_BREAK_C2_HIGH (MODERATE C1, C2 small-red above AVWAP)
        if bool(getattr(long_cfg, "enable_setup_a_pullback_c2_break", False)):
            long_pb_snap = _long_c1_preconds_pass(
                df_prepared, i_c1, long_cfg, target_impulse="MODERATE"
            )
            if long_pb_snap is not None and _long_pullback_c2_shape_ok(df_prepared, i_c1, i_c2, long_cfg):
                high2 = float(c2["high"])
                trigger2 = high2 + entry_buffer(high2, long_cfg)
                setup = "A_PULLBACK_C2_THEN_BREAK_C2_HIGH"
                out.append({
                    "signal_id": _compute_signal_id(ticker, "LONG", c1_iso, setup),
                    "ticker": ticker,
                    "side": "LONG",
                    "setup": setup,
                    "trigger_iso": trigger_iso,
                    "c1_iso": c1_iso,
                    "entry_iso": entry_iso,
                    "break_level": round(float(trigger2), 4),
                    "entry_window_start_iso": entry_iso,
                    "entry_window_end_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 2)).isoformat(),
                    "state": "CANDIDATE_WRITTEN",
                    "emitted_at_iso": emitted_at_iso,
                    "c1_close": long_pb_snap["c1_close"],
                    "c1_atr": long_pb_snap["c1_atr"],
                    "preconds_snapshot": long_pb_snap,
                })

        # SHORT A_PULLBACK_C2_THEN_BREAK_C2_LOW (MODERATE C1, C2 small-green below AVWAP)
        # Scanner has no explicit cfg gate here; matches short_strategy_v11 lines 585-618.
        short_pb_snap = _short_c1_preconds_pass(
            df_prepared, i_c1, short_cfg, day_mode_short, target_impulse="MODERATE"
        )
        if short_pb_snap is not None and _short_pullback_c2_shape_ok(df_prepared, i_c1, i_c2, short_cfg):
            low2 = float(c2["low"])
            trigger2 = low2 - entry_buffer(low2, short_cfg)
            setup = "A_PULLBACK_C2_THEN_BREAK_C2_LOW"
            out.append({
                "signal_id": _compute_signal_id(ticker, "SHORT", c1_iso, setup),
                "ticker": ticker,
                "side": "SHORT",
                "setup": setup,
                "trigger_iso": trigger_iso,
                "c1_iso": c1_iso,
                "entry_iso": entry_iso,
                "break_level": round(float(trigger2), 4),
                "entry_window_start_iso": entry_iso,
                "entry_window_end_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 2)).isoformat(),
                "state": "CANDIDATE_WRITTEN",
                "emitted_at_iso": emitted_at_iso,
                "c1_close": short_pb_snap["c1_close"],
                "c1_atr": short_pb_snap["c1_atr"],
                "preconds_snapshot": short_pb_snap,
            })

    # ---------------- Bounce-anchored (i_c1 = i-3, bounce_end = i; needs i>=5) ----------------
    if i >= 5 and bool(getattr(short_cfg, "enable_setup_b_huge_failed_bounce", False)):
        i_c1 = i - 3
        c1_iso = _bar_iso(df_prepared, i_c1)
        short_huge_snap = _short_c1_preconds_pass(
            df_prepared, i_c1, short_cfg, day_mode_short, target_impulse="HUGE"
        )
        if short_huge_snap is not None:
            bounce_info = _short_bounce_check(
                df_prepared, i_c1, i, float(short_huge_snap["c1_atr"]), short_cfg
            )
            if bounce_info is not None:
                bounce_low = float(bounce_info["bounce_low"])
                trigger_b = bounce_low - entry_buffer(bounce_low, short_cfg)
                entry_iso = (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 1)).isoformat()
                setup = "B_HUGE_RED_FAILED_BOUNCE"
                out.append({
                    "signal_id": _compute_signal_id(ticker, "SHORT", c1_iso, setup),
                    "ticker": ticker,
                    "side": "SHORT",
                    "setup": setup,
                    "trigger_iso": trigger_iso,
                    "c1_iso": c1_iso,
                    "entry_iso": entry_iso,
                    "break_level": round(float(trigger_b), 4),
                    "entry_window_start_iso": entry_iso,
                    "entry_window_end_iso": (slot_ts + pd.Timedelta(minutes=SLOT_MINUTES * 8)).isoformat(),
                    "state": "CANDIDATE_WRITTEN",
                    "emitted_at_iso": emitted_at_iso,
                    "c1_close": short_huge_snap["c1_close"],
                    "c1_atr": short_huge_snap["c1_atr"],
                    "bounce_low": bounce_low,
                    "preconds_snapshot": short_huge_snap,
                })

    return out


def write_candidate_pool(
    candidates: List[Dict[str, Any]],
    date_str: str,
    pool_dir: Optional[Path] = None,
) -> Path:
    """Merge candidates into today's candidate pool file with idempotent dedup on signal_id.
    Existing rows keep their original emitted_at_iso + state."""
    pool_dir = Path(pool_dir) if pool_dir is not None else POOL_DIR_DEFAULT
    pool_dir.mkdir(parents=True, exist_ok=True)
    path = pool_dir / f"candidate_signals_{date_str}_v16_5min.json"

    existing_by_id: Dict[str, Dict[str, Any]] = {}
    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            for row in payload.get("candidates", []):
                sid = row.get("signal_id")
                if sid:
                    existing_by_id[sid] = row
        except Exception:
            existing_by_id = {}

    for row in candidates:
        sid = row.get("signal_id")
        if not sid:
            continue
        if sid in existing_by_id:
            continue
        existing_by_id[sid] = row

    merged = sorted(
        existing_by_id.values(),
        key=lambda r: (r.get("trigger_iso", ""), r.get("ticker", ""), r.get("side", ""), r.get("setup", "")),
    )

    payload = {
        "generated_at_ist": datetime.now(IST).isoformat(),
        "date": date_str,
        "version": POOL_VERSION,
        "candidates": merged,
    }

    fd, tmp_path = tempfile.mkstemp(prefix="candidate_signals_", suffix=".json", dir=str(pool_dir))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise

    return path
