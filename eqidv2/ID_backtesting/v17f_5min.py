# -*- coding: utf-8 -*-
"""
V17f 5-min combined runner - v17d expansion plus targeted short cleanup.

Design goals
------------
1. Do NOT modify v17c or v17d. This file is a new variant.
2. Keep the LONG side identical to v17c/v17d.
3. Start from the stronger v17d short-expansion shape:
   - same extended short windows through 14:00
   - same relaxed short scan gates
   - more permissive BOTH-mode short RS threshold of 0.50%
4. Add only the extra short filters that survived trade-level testing on the
   latest aggressive v17d run (2026-04-18 22:54:30):
   - drop SHORT_ONLY shorts with ultra-weak RS (> -0.25%)
   - drop SHORT_ONLY high-ATR exhaustion shorts (ATR% >= 0.70%)
   - drop BOTH-mode shorts in the weak 12:15-12:45 pocket

Outputs go to outputs_v17f_5min/.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

from . import v17d_5min as _v17d
from . import v16_5min as _base


# ---------------------------------------------------------------------------
# Reuse v17d helpers and defaults, but expose v17f-specific env knobs.
# ---------------------------------------------------------------------------
V17F_SHORT_WINDOW_SESSION1_START = _v17d._env_str(
    "EQIDV17F_SH_W1_START", _v17d.V17D_SHORT_WINDOW_SESSION1_START
)
V17F_SHORT_WINDOW_SESSION1_END = _v17d._env_str(
    "EQIDV17F_SH_W1_END", _v17d.V17D_SHORT_WINDOW_SESSION1_END
)
V17F_SHORT_WINDOW_SESSION2_START = _v17d._env_str(
    "EQIDV17F_SH_W2_START", _v17d.V17D_SHORT_WINDOW_SESSION2_START
)
V17F_SHORT_WINDOW_SESSION2_END = _v17d._env_str(
    "EQIDV17F_SH_W2_END", _v17d.V17D_SHORT_WINDOW_SESSION2_END
)
V17F_SHORT_ENTRY_CUTOFF_HHMM = _v17d._env_str(
    "EQIDV17F_SH_ENTRY_CUTOFF", _v17d.V17D_SHORT_ENTRY_CUTOFF_HHMM
)

V17F_SHORT_BOTH_RS_PCT = _v17d._env_float("EQIDV17F_SH_RS_BOTH_PCT", 0.50)
V17F_SHORT_ADX_MIN = _v17d._env_float(
    "EQIDV17F_SH_ADX_MIN", _v17d.V17D_SHORT_ADX_MIN
)
V17F_SHORT_MOD_IMPULSE_MIN_ATR = _v17d._env_float(
    "EQIDV17F_SH_MOD_IMP_MIN_ATR", _v17d.V17D_SHORT_MOD_IMPULSE_MIN_ATR
)
V17F_SHORT_VOL_MIN_RATIO = _v17d._env_float(
    "EQIDV17F_SH_VOL_MIN_RATIO", _v17d.V17D_SHORT_VOL_MIN_RATIO
)
V17F_SHORT_AVWAP_DIST_CAP = _v17d._env_float(
    "EQIDV17F_SH_AVWAP_DIST_CAP", _v17d.V17D_SHORT_AVWAP_DIST_CAP
)
V17F_ADX_CHOP_SHORTONLY_ONLY = _v17d._env_bool(
    "EQIDV17F_ADX_CHOP_SHORTONLY_ONLY", True
)

V17F_DROP_SHORTONLY_WEAK_RS_ENABLED = _v17d._env_bool(
    "EQIDV17F_DROP_SHORTONLY_WEAK_RS", True
)
V17F_SHORTONLY_WEAK_RS_MAX = _v17d._env_float(
    "EQIDV17F_SHORTONLY_WEAK_RS_MAX", -0.25
)

V17F_DROP_SHORTONLY_HIGH_ATR_ENABLED = _v17d._env_bool(
    "EQIDV17F_DROP_SHORTONLY_HIGH_ATR", True
)
V17F_SHORTONLY_HIGH_ATR_MIN = _v17d._env_float(
    "EQIDV17F_SHORTONLY_HIGH_ATR_MIN", 0.007
)

V17F_DROP_BOTH_MIDDAY_POCKET_ENABLED = _v17d._env_bool(
    "EQIDV17F_DROP_BOTH_MIDDAY_POCKET", True
)
V17F_BOTH_MIDDAY_START = _v17d._env_str("EQIDV17F_BOTH_MIDDAY_START", "12:15")
V17F_BOTH_MIDDAY_END = _v17d._env_str("EQIDV17F_BOTH_MIDDAY_END", "12:45")


_SHORT_WINDOW_OVERRIDE = [
    (
        _v17d._parse_hhmm(V17F_SHORT_WINDOW_SESSION1_START),
        _v17d._parse_hhmm(V17F_SHORT_WINDOW_SESSION1_END),
    ),
    (
        _v17d._parse_hhmm(V17F_SHORT_WINDOW_SESSION2_START),
        _v17d._parse_hhmm(V17F_SHORT_WINDOW_SESSION2_END),
    ),
]
_SHORT_ENTRY_CUTOFF = _v17d._parse_hhmm(V17F_SHORT_ENTRY_CUTOFF_HHMM)
_BOTH_MIDDAY_START_MIN = _v17d._hhmm_to_minutes(V17F_BOTH_MIDDAY_START)
_BOTH_MIDDAY_END_MIN = _v17d._hhmm_to_minutes(V17F_BOTH_MIDDAY_END)


# ---------------------------------------------------------------------------
# PATCH 1: route outputs to the new v17f folder.
# ---------------------------------------------------------------------------
_orig_runtime_dir = _v17d._orig_runtime_dir


def _v17f_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        for old in (
            "v17f_5min",
            "v17d_5min",
            "v17c_5min",
            "v17b_5min",
            "v16_5min",
        ):
            text = text.replace(old, "v17f_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17f_runtime_dir


# ---------------------------------------------------------------------------
# PATCH 2: keep the v17d expansion profile, but lock BOTH-RS at 0.50%.
# ---------------------------------------------------------------------------
_base.FINAL_SHORT_SIGNAL_WINDOWS = list(_SHORT_WINDOW_OVERRIDE)
_base.V15_SHORT_ENTRY_CUTOFF = _SHORT_ENTRY_CUTOFF
_base.NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT = float(V17F_SHORT_BOTH_RS_PCT)
_base.V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = float(V17F_SHORT_AVWAP_DIST_CAP)
_base.MAX_WORKERS = 16
_v17d.V17D_ADX_CHOP_SHORTONLY_ONLY = bool(V17F_ADX_CHOP_SHORTONLY_ONLY)


def _v17f_adjust_short_cfg(cfg):
    try:
        cfg.adx_min = float(V17F_SHORT_ADX_MIN)
    except Exception:
        pass
    try:
        cfg.mod_impulse_min_atr = float(V17F_SHORT_MOD_IMPULSE_MIN_ATR)
    except Exception:
        pass
    try:
        cfg.volume_min_ratio = float(V17F_SHORT_VOL_MIN_RATIO)
    except Exception:
        pass
    try:
        cfg.signal_avwap_dist_atr_max = float(V17F_SHORT_AVWAP_DIST_CAP)
    except Exception:
        pass
    try:
        cfg.entry_time_cutoff = _SHORT_ENTRY_CUTOFF
    except Exception:
        pass
    return cfg


def _v17f_scan_short_prepared(ticker, df_prepared, short_cfg):
    short_cfg = _v17f_adjust_short_cfg(short_cfg)
    return _v17d._orig_scan_short_prepared(ticker, df_prepared, short_cfg)


_base.scan_short_prepared = _v17f_scan_short_prepared


# Apply v17f short-cfg adjustments at profile-build time. Without this, only
# callers routed through scan_short_prepared see the adjusted cfg; SEE's
# evaluate_slot_candidates path uses the raw parity-profile cfg (adx_min=30,
# mod_impulse_min_atr=0.30) and under-emits shorts vs v17f's 25 / 0.25.
_orig_apply_live_parity_profile = _base.apply_live_parity_profile


def _v17f_apply_live_parity_profile(short_cfg, long_cfg):
    short_cfg, long_cfg = _orig_apply_live_parity_profile(short_cfg, long_cfg)
    _v17f_adjust_short_cfg(short_cfg)
    return short_cfg, long_cfg


_base.apply_live_parity_profile = _v17f_apply_live_parity_profile


# ---------------------------------------------------------------------------
# PATCH 3: add only the new v17f filters on top of the existing v17d bundle.
# ---------------------------------------------------------------------------
def _time_minutes(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    return ts.dt.hour.astype("Int64") * 60 + ts.dt.minute.astype("Int64")


def _v17f_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v17d._v17d_apply_post_scan_filters(short_df, long_df)

    short_before = len(short_df)
    shortonly_weak_rs_removed = 0
    shortonly_high_atr_removed = 0
    both_midday_removed = 0

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
        atr = (
            pd.to_numeric(work["atr_pct_signal"], errors="coerce")
            if "atr_pct_signal" in work.columns
            else pd.Series(np.nan, index=work.index)
        )
        mins = (
            _time_minutes(work["entry_time_ist"])
            if "entry_time_ist" in work.columns
            else _time_minutes(work["signal_time_ist"])
            if "signal_time_ist" in work.columns
            else pd.Series(pd.NA, index=work.index, dtype="Int64")
        )

        if V17F_DROP_SHORTONLY_WEAK_RS_ENABLED:
            mask = mode.eq("SHORT_ONLY") & rs.gt(V17F_SHORTONLY_WEAK_RS_MAX)
            mask = mask.fillna(False)
            shortonly_weak_rs_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            atr = atr.loc[work.index]
            mins = mins.loc[work.index]

        if V17F_DROP_SHORTONLY_HIGH_ATR_ENABLED:
            mask = mode.eq("SHORT_ONLY") & atr.ge(V17F_SHORTONLY_HIGH_ATR_MIN)
            mask = mask.fillna(False)
            shortonly_high_atr_removed = int(mask.sum())
            work = work.loc[~mask].copy()
            mode = mode.loc[work.index]
            rs = rs.loc[work.index]
            atr = atr.loc[work.index]
            mins = mins.loc[work.index]

        if V17F_DROP_BOTH_MIDDAY_POCKET_ENABLED:
            mask = (
                mode.eq("BOTH")
                & mins.ge(_BOTH_MIDDAY_START_MIN)
                & mins.lt(_BOTH_MIDDAY_END_MIN)
            )
            mask = mask.fillna(False)
            both_midday_removed = int(mask.sum())
            work = work.loc[~mask].copy()

        short_df = work

    print(
        "[V17F_FILTER] SHORT extra: {before}->{after} "
        "(-{rsw} SHORT_ONLY rs>{rsmax:.2f}% | "
        "-{atrh} SHORT_ONLY atr_pct>={atrmin:.2f}% | "
        "-{mid} BOTH {midstart}-{midend})".format(
            before=short_before,
            after=len(short_df),
            rsw=shortonly_weak_rs_removed,
            rsmax=V17F_SHORTONLY_WEAK_RS_MAX,
            atrh=shortonly_high_atr_removed,
            atrmin=V17F_SHORTONLY_HIGH_ATR_MIN * 100.0,
            mid=both_midday_removed,
            midstart=V17F_BOTH_MIDDAY_START,
            midend=V17F_BOTH_MIDDAY_END,
        )
    )
    return short_df, long_df


def _v17f_get_filter_reason(row: dict, side: str):
    reason = _v17d._v17d_get_filter_reason(row, side)
    if reason is not None:
        return reason

    side_u = str(side).upper().strip()
    if side_u != "SHORT":
        return None

    mode = str(row.get("nifty_context_mode", "")).upper().strip()
    ts_raw = row.get("entry_time_ist", row.get("signal_time_ist"))
    ts_val = pd.to_datetime(ts_raw, errors="coerce")
    mins = (int(ts_val.hour) * 60 + int(ts_val.minute)) if not pd.isna(ts_val) else None

    try:
        rs = float(row.get("nifty_rel_strength_pct", float("nan")))
    except (TypeError, ValueError):
        rs = float("nan")
    try:
        atr = float(row.get("atr_pct_signal", float("nan")))
    except (TypeError, ValueError):
        atr = float("nan")

    if (
        V17F_DROP_SHORTONLY_WEAK_RS_ENABLED
        and mode == "SHORT_ONLY"
        and np.isfinite(rs)
        and rs > V17F_SHORTONLY_WEAK_RS_MAX
    ):
        return (
            "v17f short cleanup: SHORT_ONLY requires "
            f"rs<={V17F_SHORTONLY_WEAK_RS_MAX:.2f}%"
        )

    if (
        V17F_DROP_SHORTONLY_HIGH_ATR_ENABLED
        and mode == "SHORT_ONLY"
        and np.isfinite(atr)
        and atr >= V17F_SHORTONLY_HIGH_ATR_MIN
    ):
        return (
            "v17f short cleanup: SHORT_ONLY atr_pct="
            f"{atr * 100.0:.2f}% >= {V17F_SHORTONLY_HIGH_ATR_MIN * 100.0:.2f}%"
        )

    if (
        V17F_DROP_BOTH_MIDDAY_POCKET_ENABLED
        and mode == "BOTH"
        and mins is not None
        and _BOTH_MIDDAY_START_MIN <= mins < _BOTH_MIDDAY_END_MIN
    ):
        return (
            "v17f short cleanup: BOTH weak pocket "
            f"{V17F_BOTH_MIDDAY_START}-{V17F_BOTH_MIDDAY_END}"
        )

    return None


_base._apply_v16_post_scan_filters = _v17f_apply_post_scan_filters
_base.get_v16_filter_reason = _v17f_get_filter_reason


if __name__ == "__main__":
    print("=" * 78)
    print("V17f 5-min runner: v17d expansion plus targeted short cleanup")
    print(
        "  SHORT windows: "
        f"{V17F_SHORT_WINDOW_SESSION1_START}-{V17F_SHORT_WINDOW_SESSION1_END} | "
        f"{V17F_SHORT_WINDOW_SESSION2_START}-{V17F_SHORT_WINDOW_SESSION2_END} "
        f"(cutoff {V17F_SHORT_ENTRY_CUTOFF_HHMM})"
    )
    print(
        f"  SHORT BOTH-mode RS<=-{V17F_SHORT_BOTH_RS_PCT:.2f}% | "
        f"adx_min={V17F_SHORT_ADX_MIN:.0f} | "
        f"mod_impulse_min_atr={V17F_SHORT_MOD_IMPULSE_MIN_ATR:.2f} | "
        f"vol_min_ratio={V17F_SHORT_VOL_MIN_RATIO:.2f} | "
        f"avwap_cap={V17F_SHORT_AVWAP_DIST_CAP:.2f} | "
        f"adx_chop_shortonly_only={V17F_ADX_CHOP_SHORTONLY_ONLY}"
    )
    print(
        "  V17f extra cleanup: "
        f"drop_SHORT_ONLY_rs>{V17F_SHORTONLY_WEAK_RS_MAX:.2f}%="
        f"{V17F_DROP_SHORTONLY_WEAK_RS_ENABLED} | "
        f"drop_SHORT_ONLY_atr_pct>={V17F_SHORTONLY_HIGH_ATR_MIN * 100.0:.2f}%="
        f"{V17F_DROP_SHORTONLY_HIGH_ATR_ENABLED} | "
        f"drop_BOTH_{V17F_BOTH_MIDDAY_START}-{V17F_BOTH_MIDDAY_END}="
        f"{V17F_DROP_BOTH_MIDDAY_POCKET_ENABLED}"
    )
    print("  LONG logic: unchanged from v17c/v17d")
    print("  Output dir: outputs_v17f_5min")
    print("=" * 78)
    _base.main()
