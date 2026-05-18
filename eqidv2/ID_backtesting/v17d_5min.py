# -*- coding: utf-8 -*-
"""
V17d 5-min combined runner - V17c long core + selective short expansion.

Design goals
------------
1. Do NOT modify v17c. This file is a new variant.
2. Keep the LONG side identical to v17c's latest anti-chase logic.
3. Expand SHORT participation carefully instead of blindly:
   - relax BOTH-mode short RS from 0.75% to a milder default
   - extend the short window / cutoff into 14:00
   - relax short scan trend/impulse gates a bit to admit more continuation trades
4. Offset that expansion with data-driven short cleanup filters derived from the
   latest v17c short trade distribution (2026-04-18):
   - drop RS-NaN shorts
   - drop BOTH-mode AVWAP dead-zone shorts in [0.5, 1.0) ATR
   - drop BOTH-mode weak intraday time pockets
   - drop ADX chop [20,25) and exhausted ADX>=50 shorts
   - drop extreme-negative RS exhaustion shorts
   - allow late shorts only when downside control is clearly established

Outputs go to outputs_v17d_5min/.
"""
from __future__ import annotations

import os
from datetime import time as dtime
from typing import Tuple

import numpy as np
import pandas as pd

from . import v17c_5min as _v17c  # Keep as the base reference.
from . import v17b_5min as _v17b
from . import v16_5min as _base

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
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw)


def _parse_hhmm(text: str) -> dtime:
    hh, mm = str(text).strip().split(":")
    return dtime(int(hh), int(mm), 0)


def _hhmm_to_minutes(text: str) -> int:
    t = _parse_hhmm(text)
    return t.hour * 60 + t.minute


# ---------------------------------------------------------------------------
# SHORT expansion knobs
# ---------------------------------------------------------------------------
V17D_SHORT_WINDOW_SESSION1_START = _env_str("EQIDV17D_SH_W1_START", "09:15")
V17D_SHORT_WINDOW_SESSION1_END = _env_str("EQIDV17D_SH_W1_END", "12:00")
V17D_SHORT_WINDOW_SESSION2_START = _env_str("EQIDV17D_SH_W2_START", "12:00")
V17D_SHORT_WINDOW_SESSION2_END = _env_str("EQIDV17D_SH_W2_END", "14:00")
V17D_SHORT_ENTRY_CUTOFF_HHMM = _env_str("EQIDV17D_SH_ENTRY_CUTOFF", "14:00")

V17D_SHORT_BOTH_RS_PCT = _env_float("EQIDV17D_SH_RS_BOTH_PCT", 0.60)
V17D_SHORT_ADX_MIN = _env_float("EQIDV17D_SH_ADX_MIN", 25.0)
V17D_SHORT_MOD_IMPULSE_MIN_ATR = _env_float("EQIDV17D_SH_MOD_IMP_MIN_ATR", 0.25)
V17D_SHORT_VOL_MIN_RATIO = _env_float("EQIDV17D_SH_VOL_MIN_RATIO", 0.90)
V17D_SHORT_AVWAP_DIST_CAP = _env_float("EQIDV17D_SH_AVWAP_DIST_CAP", 2.10)


# ---------------------------------------------------------------------------
# SHORT cleanup bundle
# ---------------------------------------------------------------------------
V17D_DISABLE_SHORT_PULLBACK_SETUP = _env_bool(
    "EQIDV17D_DISABLE_SHORT_PULLBACK_SETUP", True
)

V17D_DROP_RS_NAN_ENABLED = _env_bool("EQIDV17D_DROP_RS_NAN", True)
V17D_DROP_EXTREME_RS_ENABLED = _env_bool("EQIDV17D_DROP_EXTREME_RS", True)
V17D_EXTREME_RS_CAP = _env_float("EQIDV17D_EXTREME_RS_CAP", -2.0)

V17D_DROP_ADX_CHOP_ENABLED = _env_bool("EQIDV17D_DROP_ADX_CHOP", True)
V17D_ADX_CHOP_LO = _env_float("EQIDV17D_ADX_CHOP_LO", 20.0)
V17D_ADX_CHOP_HI = _env_float("EQIDV17D_ADX_CHOP_HI", 25.0)
V17D_ADX_CHOP_SHORTONLY_ONLY = _env_bool(
    "EQIDV17D_ADX_CHOP_SHORTONLY_ONLY", False
)

V17D_DROP_HIGH_ADX_ENABLED = _env_bool("EQIDV17D_DROP_HIGH_ADX", True)
V17D_HIGH_ADX_MIN = _env_float("EQIDV17D_HIGH_ADX_MIN", 50.0)

V17D_DROP_BOTH_AVWAP_DEAD_ENABLED = _env_bool(
    "EQIDV17D_DROP_BOTH_AVWAP_DEAD", True
)
V17D_BOTH_AVWAP_DEAD_LO = _env_float("EQIDV17D_BOTH_AVWAP_DEAD_LO", 0.50)
V17D_BOTH_AVWAP_DEAD_HI = _env_float("EQIDV17D_BOTH_AVWAP_DEAD_HI", 1.00)

V17D_DROP_BOTH_TIME_POCKETS_ENABLED = _env_bool(
    "EQIDV17D_DROP_BOTH_TIME_POCKETS", True
)
V17D_BOTH_BAD_TIME1_START = _env_str("EQIDV17D_BOTH_BAD_TIME1_START", "10:30")
V17D_BOTH_BAD_TIME1_END = _env_str("EQIDV17D_BOTH_BAD_TIME1_END", "11:00")
V17D_BOTH_BAD_TIME2_START = _env_str("EQIDV17D_BOTH_BAD_TIME2_START", "11:30")
V17D_BOTH_BAD_TIME2_END = _env_str("EQIDV17D_BOTH_BAD_TIME2_END", "12:00")

V17D_LATE_GATE_ENABLED = _env_bool("EQIDV17D_LATE_GATE_ENABLED", True)
V17D_LATE_GATE_START = _env_str("EQIDV17D_LATE_GATE_START", "13:30")
V17D_LATE_GATE_MAX_RS = _env_float("EQIDV17D_LATE_GATE_MAX_RS", -1.0)


_SHORT_WINDOW_OVERRIDE = [
    (_parse_hhmm(V17D_SHORT_WINDOW_SESSION1_START), _parse_hhmm(V17D_SHORT_WINDOW_SESSION1_END)),
    (_parse_hhmm(V17D_SHORT_WINDOW_SESSION2_START), _parse_hhmm(V17D_SHORT_WINDOW_SESSION2_END)),
]
_SHORT_ENTRY_CUTOFF = _parse_hhmm(V17D_SHORT_ENTRY_CUTOFF_HHMM)

_BOTH_BAD_TIME1_START_MIN = _hhmm_to_minutes(V17D_BOTH_BAD_TIME1_START)
_BOTH_BAD_TIME1_END_MIN = _hhmm_to_minutes(V17D_BOTH_BAD_TIME1_END)
_BOTH_BAD_TIME2_START_MIN = _hhmm_to_minutes(V17D_BOTH_BAD_TIME2_START)
_BOTH_BAD_TIME2_END_MIN = _hhmm_to_minutes(V17D_BOTH_BAD_TIME2_END)
_LATE_GATE_START_MIN = _hhmm_to_minutes(V17D_LATE_GATE_START)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to the new v17d folder
# ---------------------------------------------------------------------------
_orig_runtime_dir = _v17c._orig_runtime_dir


def _v17d_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17d_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17d_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: controlled short expansion, long side left untouched
# ---------------------------------------------------------------------------
_base.FINAL_SHORT_SIGNAL_WINDOWS = list(_SHORT_WINDOW_OVERRIDE)
_base.V15_SHORT_ENTRY_CUTOFF = _SHORT_ENTRY_CUTOFF
_base.NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT = float(V17D_SHORT_BOTH_RS_PCT)
_base.V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = float(V17D_SHORT_AVWAP_DIST_CAP)
_base.MAX_WORKERS = 16


def _v17d_adjust_short_cfg(cfg):
    try:
        cfg.adx_min = float(V17D_SHORT_ADX_MIN)
    except Exception:
        pass
    try:
        cfg.mod_impulse_min_atr = float(V17D_SHORT_MOD_IMPULSE_MIN_ATR)
    except Exception:
        pass
    try:
        cfg.volume_min_ratio = float(V17D_SHORT_VOL_MIN_RATIO)
    except Exception:
        pass
    try:
        cfg.signal_avwap_dist_atr_max = float(V17D_SHORT_AVWAP_DIST_CAP)
    except Exception:
        pass
    try:
        cfg.entry_time_cutoff = _SHORT_ENTRY_CUTOFF
    except Exception:
        pass
    return cfg


def _v17d_scan_short_prepared(ticker, df_prepared, short_cfg):
    short_cfg = _v17d_adjust_short_cfg(short_cfg)
    return _orig_scan_short_prepared(ticker, df_prepared, short_cfg)


_base.scan_short_prepared = _v17d_scan_short_prepared


# ---------------------------------------------------------------------------
# PATCH 3: post-scan filters
# ---------------------------------------------------------------------------
_v16_apply_post_scan_filters = _v17b._v16_apply_post_scan_filters
_v16_get_filter_reason = _v17b._v16_get_filter_reason


def _time_minutes(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    return ts.dt.hour.astype("Int64") * 60 + ts.dt.minute.astype("Int64")


def _apply_v17d_long_filter(long_df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if long_df.empty or not _v17c.V17C_LONG_LATE_ANTICHASE_ENABLED:
        return long_df, 0
    work = long_df.copy()
    removed = 0
    if {"setup", "bars_from_open", "entry_bar_vol_ratio"}.issubset(work.columns):
        setup = work["setup"].astype(str).str.upper().str.strip()
        bfo = pd.to_numeric(work["bars_from_open"], errors="coerce")
        vr = pd.to_numeric(work["entry_bar_vol_ratio"], errors="coerce")
        mask = (
            setup.eq(_v17c.V17C_LONG_LATE_SETUP_NAME)
            & bfo.ge(_v17c.V17C_LONG_LATE_MIN_BARS_FROM_OPEN)
            & vr.ge(_v17c.V17C_LONG_LATE_MIN_VOL_RATIO)
        )
        removed = int(mask.sum())
        work = work.loc[~mask].copy()
    return work, removed


def _v17d_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v16_apply_post_scan_filters(short_df, long_df)

    short_before = len(short_df)
    long_before = len(long_df)
    pullback_removed = 0
    rs_nan_removed = 0
    extreme_rs_removed = 0
    adx_chop_removed = 0
    high_adx_removed = 0
    both_avwap_dead_removed = 0
    both_time_removed = 0
    late_gate_removed = 0

    if not short_df.empty:
        work = short_df.copy()

        mode = (
            work["nifty_context_mode"].astype(str).str.upper().str.strip()
            if "nifty_context_mode" in work.columns
            else pd.Series("", index=work.index)
        )
        rs = (
            pd.to_numeric(work["nifty_rel_strength_pct"], errors="coerce")
            if "nifty_rel_strength_pct" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        adx = (
            pd.to_numeric(work["adx_signal"], errors="coerce")
            if "adx_signal" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        avwap = (
            pd.to_numeric(work["avwap_dist_atr_signal"], errors="coerce")
            if "avwap_dist_atr_signal" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        setup = (
            work["setup"].astype(str).str.upper().str.strip()
            if "setup" in work.columns
            else pd.Series("", index=work.index)
        )
        mins = (
            _time_minutes(work["entry_time_ist"])
            if "entry_time_ist" in work.columns
            else _time_minutes(work["signal_time_ist"])
            if "signal_time_ist" in work.columns
            else pd.Series(pd.NA, index=work.index, dtype="Int64")
        )

        if V17D_DISABLE_SHORT_PULLBACK_SETUP:
            mask = setup.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW")
            pullback_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            mins = mins.loc[work.index]

        if V17D_DROP_RS_NAN_ENABLED:
            mask = rs.isna()
            rs_nan_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            mins = mins.loc[work.index]

        if V17D_DROP_EXTREME_RS_ENABLED:
            mask = rs.le(V17D_EXTREME_RS_CAP).fillna(False)
            extreme_rs_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            mins = mins.loc[work.index]

        if V17D_DROP_ADX_CHOP_ENABLED:
            mask = adx.ge(V17D_ADX_CHOP_LO) & adx.lt(V17D_ADX_CHOP_HI)
            if V17D_ADX_CHOP_SHORTONLY_ONLY:
                mask = mask & mode.eq("SHORT_ONLY")
            mask = mask.fillna(False)
            adx_chop_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            mins = mins.loc[work.index]

        if V17D_DROP_HIGH_ADX_ENABLED:
            mask = adx.ge(V17D_HIGH_ADX_MIN).fillna(False)
            high_adx_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            mins = mins.loc[work.index]

        if V17D_DROP_BOTH_AVWAP_DEAD_ENABLED:
            mask = (
                mode.eq("BOTH")
                & avwap.ge(V17D_BOTH_AVWAP_DEAD_LO)
                & avwap.lt(V17D_BOTH_AVWAP_DEAD_HI)
            )
            mask = mask.fillna(False)
            both_avwap_dead_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            mins = mins.loc[work.index]

        if V17D_DROP_BOTH_TIME_POCKETS_ENABLED:
            mask = mode.eq("BOTH") & (
                mins.between(_BOTH_BAD_TIME1_START_MIN, _BOTH_BAD_TIME1_END_MIN - 1)
                | mins.between(_BOTH_BAD_TIME2_START_MIN, _BOTH_BAD_TIME2_END_MIN - 1)
            )
            mask = mask.fillna(False)
            both_time_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            adx = adx.loc[work.index]
            avwap = avwap.loc[work.index]
            mins = mins.loc[work.index]

        if V17D_LATE_GATE_ENABLED:
            strong_late = mode.eq("SHORT_ONLY") | rs.le(V17D_LATE_GATE_MAX_RS)
            mask = mins.ge(_LATE_GATE_START_MIN) & ~strong_late
            mask = mask.fillna(False)
            late_gate_removed = int(mask.sum())
            work = work.loc[~mask].copy()

        short_df = work

    long_df, long_late_removed = _apply_v17d_long_filter(long_df)

    print(
        "[V17D_FILTER] SHORT: {before}->{after} "
        "(-{pb} pullback | -{rn} rs_nan | -{xr} rs<={xcap:.2f}% | "
        "-{ac} adx[{alo:.1f},{ahi:.1f}) | -{ah} adx>={hmin:.1f} | "
        "-{av} BOTH avwap[{vlo:.2f},{vhi:.2f}) | -{tm} BOTH time pockets | "
        "-{lg} late gate after {late}) | LONG: {lbefore}->{lafter} "
        "(-{ll} late {lname} bars>={lbars}, vol>={lvol:.1f}x)".format(
            before=short_before,
            after=len(short_df),
            pb=pullback_removed,
            rn=rs_nan_removed,
            xr=extreme_rs_removed,
            xcap=V17D_EXTREME_RS_CAP,
            ac=adx_chop_removed,
            alo=V17D_ADX_CHOP_LO,
            ahi=V17D_ADX_CHOP_HI,
            ah=high_adx_removed,
            hmin=V17D_HIGH_ADX_MIN,
            av=both_avwap_dead_removed,
            vlo=V17D_BOTH_AVWAP_DEAD_LO,
            vhi=V17D_BOTH_AVWAP_DEAD_HI,
            tm=both_time_removed,
            lg=late_gate_removed,
            late=V17D_LATE_GATE_START,
            lbefore=long_before,
            lafter=len(long_df),
            ll=long_late_removed,
            lname=_v17c.V17C_LONG_LATE_SETUP_NAME,
            lbars=_v17c.V17C_LONG_LATE_MIN_BARS_FROM_OPEN,
            lvol=_v17c.V17C_LONG_LATE_MIN_VOL_RATIO,
        )
    )
    return short_df, long_df


def _v17d_get_filter_reason(row: dict, side: str):
    reason = _v16_get_filter_reason(row, side)
    if reason is not None:
        return reason

    side_u = str(side).upper().strip()
    if side_u != "SHORT":
        return _v17c._v17c_get_filter_reason(row, side)

    setup = str(row.get("setup", "")).upper().strip()
    if V17D_DISABLE_SHORT_PULLBACK_SETUP and setup == "A_PULLBACK_C2_THEN_BREAK_C2_LOW":
        return "v17d short cleanup: pullback setup disabled"

    try:
        rs = float(row.get("nifty_rel_strength_pct", float("nan")))
    except (TypeError, ValueError):
        rs = float("nan")
    try:
        adx = float(row.get("adx_signal", float("nan")))
    except (TypeError, ValueError):
        adx = float("nan")
    try:
        avwap = float(row.get("avwap_dist_atr_signal", float("nan")))
    except (TypeError, ValueError):
        avwap = float("nan")

    mode = str(row.get("nifty_context_mode", "")).upper().strip()
    ts_raw = row.get("entry_time_ist", row.get("signal_time_ist"))
    ts_val = pd.to_datetime(ts_raw, errors="coerce")
    mins = (int(ts_val.hour) * 60 + int(ts_val.minute)) if not pd.isna(ts_val) else None

    if V17D_DROP_RS_NAN_ENABLED and not np.isfinite(rs):
        return "v17d short cleanup: missing nifty_rel_strength_pct"

    if V17D_DROP_EXTREME_RS_ENABLED and np.isfinite(rs) and rs <= V17D_EXTREME_RS_CAP:
        return f"v17d short cleanup: rs={rs:.2f}% <= {V17D_EXTREME_RS_CAP:.2f}%"

    if (
        V17D_DROP_ADX_CHOP_ENABLED
        and np.isfinite(adx)
        and V17D_ADX_CHOP_LO <= adx < V17D_ADX_CHOP_HI
    ):
        if V17D_ADX_CHOP_SHORTONLY_ONLY and mode != "SHORT_ONLY":
            pass
        else:
            return (
                f"v17d short cleanup: adx={adx:.1f} in "
                f"[{V17D_ADX_CHOP_LO:.1f},{V17D_ADX_CHOP_HI:.1f})"
            )

    if V17D_DROP_HIGH_ADX_ENABLED and np.isfinite(adx) and adx >= V17D_HIGH_ADX_MIN:
        return f"v17d short cleanup: adx={adx:.1f} >= {V17D_HIGH_ADX_MIN:.1f}"

    if (
        V17D_DROP_BOTH_AVWAP_DEAD_ENABLED
        and mode == "BOTH"
        and np.isfinite(avwap)
        and V17D_BOTH_AVWAP_DEAD_LO <= avwap < V17D_BOTH_AVWAP_DEAD_HI
    ):
        return (
            f"v17d short cleanup: BOTH avwap_dist_atr={avwap:.2f} in "
            f"[{V17D_BOTH_AVWAP_DEAD_LO:.2f},{V17D_BOTH_AVWAP_DEAD_HI:.2f})"
        )

    if V17D_DROP_BOTH_TIME_POCKETS_ENABLED and mode == "BOTH" and mins is not None:
        if _BOTH_BAD_TIME1_START_MIN <= mins < _BOTH_BAD_TIME1_END_MIN:
            return (
                "v17d short cleanup: BOTH weak time pocket "
                f"{V17D_BOTH_BAD_TIME1_START}-{V17D_BOTH_BAD_TIME1_END}"
            )
        if _BOTH_BAD_TIME2_START_MIN <= mins < _BOTH_BAD_TIME2_END_MIN:
            return (
                "v17d short cleanup: BOTH weak time pocket "
                f"{V17D_BOTH_BAD_TIME2_START}-{V17D_BOTH_BAD_TIME2_END}"
            )

    if V17D_LATE_GATE_ENABLED and mins is not None and mins >= _LATE_GATE_START_MIN:
        strong_late = mode == "SHORT_ONLY" or (np.isfinite(rs) and rs <= V17D_LATE_GATE_MAX_RS)
        if not strong_late:
            return (
                "v17d short cleanup: late short requires SHORT_ONLY or "
                f"rs<={V17D_LATE_GATE_MAX_RS:.2f}% after {V17D_LATE_GATE_START}"
            )

    return None


_base._apply_v16_post_scan_filters = _v17d_apply_post_scan_filters
_base.get_v16_filter_reason = _v17d_get_filter_reason


if __name__ == "__main__":
    print("=" * 76)
    print("V17d 5-min runner: v17c long core + selective short expansion")
    print(
        "  SHORT windows: "
        f"{V17D_SHORT_WINDOW_SESSION1_START}-{V17D_SHORT_WINDOW_SESSION1_END} | "
        f"{V17D_SHORT_WINDOW_SESSION2_START}-{V17D_SHORT_WINDOW_SESSION2_END} "
        f"(cutoff {V17D_SHORT_ENTRY_CUTOFF_HHMM})"
    )
    print(
        f"  SHORT BOTH-mode RS<=-{V17D_SHORT_BOTH_RS_PCT:.2f}% | "
        f"adx_min={V17D_SHORT_ADX_MIN:.0f} | "
        f"mod_impulse_min_atr={V17D_SHORT_MOD_IMPULSE_MIN_ATR:.2f} | "
        f"vol_min_ratio={V17D_SHORT_VOL_MIN_RATIO:.2f} | "
        f"avwap_cap={V17D_SHORT_AVWAP_DIST_CAP:.2f}"
    )
    print(
        "  SHORT cleanup: "
        f"drop_pullback={V17D_DISABLE_SHORT_PULLBACK_SETUP} | "
        f"drop_rs_nan={V17D_DROP_RS_NAN_ENABLED} | "
        f"drop_rs<={V17D_EXTREME_RS_CAP:.2f}%={V17D_DROP_EXTREME_RS_ENABLED} | "
        f"drop_adx[{V17D_ADX_CHOP_LO:.1f},{V17D_ADX_CHOP_HI:.1f})={V17D_DROP_ADX_CHOP_ENABLED} | "
        f"drop_adx>={V17D_HIGH_ADX_MIN:.1f}={V17D_DROP_HIGH_ADX_ENABLED} | "
        f"drop_BOTH_avwap[{V17D_BOTH_AVWAP_DEAD_LO:.2f},{V17D_BOTH_AVWAP_DEAD_HI:.2f})="
        f"{V17D_DROP_BOTH_AVWAP_DEAD_ENABLED} | "
        f"late_gate_after={V17D_LATE_GATE_START}"
    )
    print("  LONG logic: unchanged from v17c")
    print("  Output dir: outputs_v17d_5min")
    print("=" * 76)
    _base.main()
