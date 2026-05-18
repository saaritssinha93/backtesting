# -*- coding: utf-8 -*-
"""
V17i 5-min combined runner — target+filter rebalance on top of v17h.

Objective (user-defined): LONG 10-15 trades/day, day-win > 80%, PF better
than v17h's 1.844, target in 0.75-1.00% range.

Research leading to v17i
------------------------
Friction math (5x leverage, slippage 8bps/side, stop_extra_slip 3bps):
  net TARGET = 5*target% - 0.80%    net SL = -(5*stop% + 0.95%)
At SL 0.75%, loser is fixed at -4.70%. For PF>=1.85 the required hit-rate
is 67-75% depending on target. The required EDGE over random-walk is
~24.7pp across all target choices in 0.75-1.00%.

v17h actual edge = 64.11% hit - 42.86% random-walk baseline = 21.25pp.
So we need ~3.5pp more edge AND a lower target to lift dwin above 80%.

The v17g trade-log bucket analysis identified the "RSI 55-65 dead zone"
as a ~3pp edge leak on reversal trades (win% 52-54 vs cohort 58). Dropping
that bucket is the targeted edge addition.

Five changes
------------
Change 1 (TARGET)     EQIDV17I_LONG_TARGET_PCT=0.0080 (LONG only; SHORT unchanged)
Change 2 (FILTER)     EQIDV17I_REVERSAL_DROP_RSI_DEADZONE=True (drop reversal
                      trades where 55 <= rsi_signal < 65; post-scan)
Change 3 (CAP)        EQIDV17I_MAX_TRADES_PER_TICKER=2 (raised from 1 for LONG
                      only — allows reversal + later A_MOD on strong tickers)
Change 4 (WINDOW)     EQIDV17I_REVERSAL_MAX_HOUR=13 (extend reversal emission
                      from pre-12:00 IST to pre-13:00 IST)
Change 5 (PASSTHRU)   inherits Patch B gates from v17h (vol<=3x, dist>=1.0,
                      BOTH-only, nifty_rs>=1.0%)

Projected vs actual for v17h → v17i (order-of-magnitude):
  trade count: 1627 * ~0.85 (F7) * ~1.15 (cap) * ~1.20 (max_hour) = ~1900 LONG
              → ~9.5-10/day on active days
  dwin:        73.4% + ~4pp (target drop) + ~3pp (F7) - ~1pp (later trades weaker)
              = ~79-80% (borderline; will iterate)
  PF:          expected hold near 1.85 (target drop hurts, F7 edge compensates)

Outputs go to outputs_v17i_5min/.

Notes
-----
- Does NOT modify v17h; imports and patches via composition.
- Change 1 overrides the base-module TEST_LONG_TARGET_PCT constant, which is
  applied in two places in v16 main() (apply_live_parity_profile and the
  in-line block around line 3670). SHORT stays at 1.00%.
- Change 3 is LONG-scoped by setting the cfg attr inside _v17i_adjust_long_cfg
  (only the LONG scanner reads this cfg).
"""
from __future__ import annotations

import os
from typing import Tuple

import numpy as np
import pandas as pd

from . import v17h_5min as _v17h  # cascade: v17h -> v17g -> v17f -> v17d -> v17b -> v16
from . import v17g_5min as _v17g  # needed to override v17g's reversal_max_hour_ist constant
from . import v16_5min as _base


# ---------------------------------------------------------------------------
# Env helpers (mirror v17h/v17g style).
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
# V17i env toggles.
# ---------------------------------------------------------------------------
V17I_LONG_TARGET_PCT             = _env_float("EQIDV17I_LONG_TARGET_PCT", 0.0080)
V17I_REVERSAL_DROP_RSI_DEADZONE  = _env_bool("EQIDV17I_REVERSAL_DROP_RSI_DEADZONE", True)
V17I_REVERSAL_RSI_DEADZONE_LO    = _env_float("EQIDV17I_REVERSAL_RSI_DEADZONE_LO", 55.0)
V17I_REVERSAL_RSI_DEADZONE_HI    = _env_float("EQIDV17I_REVERSAL_RSI_DEADZONE_HI", 65.0)
V17I_MAX_TRADES_PER_TICKER       = _env_int("EQIDV17I_MAX_TRADES_PER_TICKER", 2)
V17I_REVERSAL_MAX_HOUR           = _env_int("EQIDV17I_REVERSAL_MAX_HOUR", 13)


# ---------------------------------------------------------------------------
# Change 1: LONG target override.
# ---------------------------------------------------------------------------
_base.TEST_LONG_TARGET_PCT = float(V17I_LONG_TARGET_PCT)


# ---------------------------------------------------------------------------
# Change 4: override v17g's module-level reversal_max_hour_ist so the
# downstream _v17g_adjust_long_cfg doesn't reset our extended window back
# to 12. v17g looks this up from module globals at call time, so overwriting
# the attribute post-import propagates to the next scan.
# ---------------------------------------------------------------------------
_v17g.V17G_REVERSAL_MAX_HOUR_IST = int(V17I_REVERSAL_MAX_HOUR)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to outputs_v17i_5min/.
# ---------------------------------------------------------------------------
_orig_runtime_dir_v17i = _base.runtime_dir  # already v17h-patched


def _v17i_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17i_5min",
            "v17h_5min",
            "v17g_5min",
            "v17f_5min",
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17i_5min")
        new_parts.append(text)
    return _orig_runtime_dir_v17i(*tuple(new_parts))


_base.runtime_dir = _v17i_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: LONG-side cfg adjust (Change 3 + Change 4). Wrap the v17h scanner
# wrapper so our adjustments compose cleanly with v17h's and v17g's.
# ---------------------------------------------------------------------------
_v17i_orig_scan_long_prepared = getattr(_base, "scan_long_prepared", None)


def _v17i_adjust_long_cfg(cfg):
    try:
        cfg.max_trades_per_ticker_per_day = int(V17I_MAX_TRADES_PER_TICKER)
    except Exception:
        pass
    try:
        cfg.reversal_max_hour_ist = int(V17I_REVERSAL_MAX_HOUR)
    except Exception:
        pass
    return cfg


if _v17i_orig_scan_long_prepared is not None:
    def _v17i_scan_long_prepared(ticker, df_prepared, long_cfg):
        long_cfg = _v17i_adjust_long_cfg(long_cfg)
        return _v17i_orig_scan_long_prepared(ticker, df_prepared, long_cfg)

    _base.scan_long_prepared = _v17i_scan_long_prepared


# ---------------------------------------------------------------------------
# PATCH 3: post-scan F7 filter (Change 2) — drop B_AVWAP_RECLAIM_REVERSAL
# trades whose rsi_signal falls in the dead-zone bucket. Runs AFTER v17h's
# post-scan stack so we operate on the already-Patch-B'd long_df.
# ---------------------------------------------------------------------------
_v17h_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v17h_get_filter_reason       = _base.get_v16_filter_reason


def _v17i_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17h_apply_post_scan_filters(short_df, long_df)

    if (
        not V17I_REVERSAL_DROP_RSI_DEADZONE
        or long_df is None
        or long_df.empty
        or "setup" not in long_df.columns
        or "rsi_signal" not in long_df.columns
    ):
        return short_df, long_df

    work = long_df.copy()
    setup = work["setup"].astype(str).str.strip()
    rsi = pd.to_numeric(work["rsi_signal"], errors="coerce")
    drop = (
        setup.eq("B_AVWAP_RECLAIM_REVERSAL")
        & rsi.ge(V17I_REVERSAL_RSI_DEADZONE_LO)
        & rsi.lt(V17I_REVERSAL_RSI_DEADZONE_HI)
    ).fillna(False)
    before = len(work)
    n_drop = int(drop.sum())
    work = work.loc[~drop].copy()
    print(
        "[V17I_FILTER] LONG B_AVWAP_RECLAIM_REVERSAL RSI dead-zone drop: "
        f"{before}->{len(work)} (-{n_drop} rsi in "
        f"[{V17I_REVERSAL_RSI_DEADZONE_LO:.0f}, {V17I_REVERSAL_RSI_DEADZONE_HI:.0f}))"
    )
    return short_df, work


def _v17i_get_filter_reason(row: dict, side: str):
    reason = _v17h_get_filter_reason(row, side)
    if reason is not None:
        return reason

    if not V17I_REVERSAL_DROP_RSI_DEADZONE:
        return None
    if str(side).upper().strip() != "LONG":
        return None
    if str(row.get("setup", "")).strip() != "B_AVWAP_RECLAIM_REVERSAL":
        return None

    try:
        rsi = float(row.get("rsi_signal", float("nan")))
    except (TypeError, ValueError):
        rsi = float("nan")
    if (
        np.isfinite(rsi)
        and V17I_REVERSAL_RSI_DEADZONE_LO <= rsi < V17I_REVERSAL_RSI_DEADZONE_HI
    ):
        return (
            f"v17i reversal cleanup: rsi={rsi:.1f} in dead-zone "
            f"[{V17I_REVERSAL_RSI_DEADZONE_LO:.0f}, "
            f"{V17I_REVERSAL_RSI_DEADZONE_HI:.0f})"
        )
    return None


_base._apply_v16_post_scan_filters = _v17i_apply_post_scan_filters
_base.get_v16_filter_reason = _v17i_get_filter_reason


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 78)
    print("V17i 5-min runner: target+filter rebalance on top of v17h")
    print("  Output dir: outputs_v17i_5min")
    print("--- Changes ---")
    print(f"  Change 1  LONG target_pct           = {V17I_LONG_TARGET_PCT*100:.2f}%"
          f"  (SHORT unchanged at base override)")
    print(f"  Change 2  reversal RSI dead-zone drop = {V17I_REVERSAL_DROP_RSI_DEADZONE}"
          f"  (drop [{V17I_REVERSAL_RSI_DEADZONE_LO:.0f}, "
          f"{V17I_REVERSAL_RSI_DEADZONE_HI:.0f}))")
    print(f"  Change 3  max_trades_per_ticker/day  = {V17I_MAX_TRADES_PER_TICKER}  "
          "(LONG only)")
    print(f"  Change 4  reversal_max_hour_ist      = {V17I_REVERSAL_MAX_HOUR}:00 IST")
    print("--- Inherits all v17h / v17g / v17f / v17d / v17b / v16 behavior ---")
    print("=" * 78)
    _base.main()
