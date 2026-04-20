# -*- coding: utf-8 -*-
"""
V17c 5-min combined runner - V16 live parity core + best-short/best-long hybrid.

Base patches applied on top of v16 Run14 to match the V16 5-min live stack:
  - SHORT context: raw RS <= -0.75% (same as V16 live; ATR-norm disabled)
  - LONG context:  raw RS >= +0.75% (unchanged from V16)
  - Breakeven and trailing stop DISABLED (live executor: fixed LIMIT + SL-M only)
  - Nifty confirm time: 09:20 (matches V16 live detection engine)
  - Data dir: stocks_indicators_5min_eq_live2 (separate historical copy)

V17c merges the strongest v17b research slices:
  - Keep the CURRENT best-short cleanup bundle:
      * block SHORT_ONLY shorts in the RSI 21-28 pocket
      * block SHORT_ONLY shorts with ADX >= 44
      * block the A_PULLBACK_C2_THEN_BREAK_C2_LOW short setup, except for
        strong-trend pullbacks that already show established downside control
  - Upgrade the long anti-chase filter to the STRICT-BETTER setting:
      * block late/high-volume A_MOD_BREAK_C1_HIGH longs at bars >= 12
        and vol_ratio >= 3.5x

Outputs go to outputs_v17c_5min/ (separate from v16/v17/v17b/v18).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v17b_5min as _v17b
import avwap_combined_runner_v16_5min as _base


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
    raw_s = str(raw).strip().lower()
    if raw_s in {"1", "true", "yes", "y", "on"}:
        return True
    if raw_s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


_v16_apply_nifty_intraday_context = _v17b._v16_apply_nifty_intraday_context
_v16_apply_post_scan_filters = _v17b._v16_apply_post_scan_filters
_v16_get_filter_reason = _v17b._v16_get_filter_reason


# ===========================================================================
# V17c CONFIG - same live-parity short context as v17b
# ===========================================================================
V17C_RS_ATR_NORM_ENABLED = _env_bool("EQIDV17C_RS_ATR_NORM_ENABLED", False)
V17C_RS_ATR_NORM_THRESH_SHORT_BOTH = _env_float(
    "EQIDV17C_RS_ATR_NORM_THRESH_SHORT_BOTH", 0.50
)
V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT = _env_float(
    "EQIDV17C_RS_ATR_DIRECTIONAL_THRESH_SHORT", 0.35
)
V17C_RS_ATR_FALLBACK_PCT_SHORT = _env_float(
    "EQIDV17C_RS_ATR_FALLBACK_PCT_SHORT", 0.40
)
V17C_DATA_5M_DIR = Path(
    os.getenv(
        "EQIDV17C_DATA_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2",
    )
)

V17C_FILTER_BUNDLE_ENABLED = _env_bool("EQIDV17C_FILTER_BUNDLE_ENABLED", True)

V17C_SHORT_BLOCK_SHORTONLY_RSI_ENABLED = _env_bool(
    "EQIDV17C_SHORT_BLOCK_SHORTONLY_RSI_ENABLED", True
)
V17C_SHORT_SHORTONLY_RSI_LO = _env_float(
    "EQIDV17C_SHORT_SHORTONLY_RSI_LO", 21.0
)
V17C_SHORT_SHORTONLY_RSI_HI = _env_float(
    "EQIDV17C_SHORT_SHORTONLY_RSI_HI", 28.0
)
V17C_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED = _env_bool(
    "EQIDV17C_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED", True
)
V17C_SHORT_SHORTONLY_HIGH_ADX_MIN = _env_float(
    "EQIDV17C_SHORT_SHORTONLY_HIGH_ADX_MIN", 44.0
)
V17C_DISABLE_SHORT_PULLBACK_SETUP = _env_bool(
    "EQIDV17C_DISABLE_SHORT_PULLBACK_SETUP", True
)
V17C_SHORT_PULLBACK_EXCEPTION_ENABLED = _env_bool(
    "EQIDV17C_SHORT_PULLBACK_EXCEPTION_ENABLED", True
)
V17C_SHORT_PULLBACK_EXCEPTION_MIN_ADX = _env_float(
    "EQIDV17C_SHORT_PULLBACK_EXCEPTION_MIN_ADX", 44.0
)
V17C_SHORT_PULLBACK_EXCEPTION_MIN_HOUR = int(
    _env_float("EQIDV17C_SHORT_PULLBACK_EXCEPTION_MIN_HOUR", 10)
)
V17C_SHORT_PULLBACK_EXCEPTION_ALLOW_SHORTONLY_ANY_HOUR = _env_bool(
    "EQIDV17C_SHORT_PULLBACK_EXCEPTION_ALLOW_SHORTONLY_ANY_HOUR", True
)

V17C_LONG_LATE_ANTICHASE_ENABLED = _env_bool(
    "EQIDV17C_LONG_LATE_ANTICHASE_ENABLED", True
)
V17C_LONG_LATE_MIN_BARS_FROM_OPEN = int(
    _env_float("EQIDV17C_LONG_LATE_MIN_BARS_FROM_OPEN", 12)
)
V17C_LONG_LATE_MIN_VOL_RATIO = _env_float(
    "EQIDV17C_LONG_LATE_MIN_VOL_RATIO", 3.5
)
V17C_LONG_LATE_SETUP_NAME = os.getenv(
    "EQIDV17C_LONG_LATE_SETUP_NAME",
    "A_MOD_BREAK_C1_HIGH",
).strip().upper()


# ===========================================================================
# PATCH 1: redirect all outputs from v16_5min / v17b_5min -> v17c_5min
# ===========================================================================
_orig_runtime_dir = _v17b._orig_runtime_dir


def _v17c_runtime_dir(*parts):
    new_parts = []
    for part in parts:
        text = str(part)
        text = text.replace("v16_5min", "v17c_5min")
        text = text.replace("v17b_5min", "v17c_5min")
        new_parts.append(text)
    return _orig_runtime_dir(*tuple(new_parts))


_base.runtime_dir = _v17c_runtime_dir


# ===========================================================================
# PATCH 2: keep the same live 5-minute parquet universe
# ===========================================================================
_base.RUNTIME_DATA_5M_DIR = V17C_DATA_5M_DIR
_base.MAX_WORKERS = 16


# ===========================================================================
# ATR MAP BUILDER
# ===========================================================================
_v17c_atr_cache: Dict[str, Dict[str, float]] = {}


def _build_stock_atr_map(ticker: str, cfg, cache: Dict) -> Dict[str, float]:
    return _v17b._build_stock_atr_map(ticker, cfg, cache)


def _apply_short_v17c_context(
    short_df: pd.DataFrame,
    cfg,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, int, int]:
    if short_df.empty:
        return short_df, 0, 0

    d = short_df.copy()
    ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
    ts_series = pd.to_datetime(d[ts_col], errors="coerce")
    if getattr(ts_series.dt, "tz", None) is None:
        ts_series = ts_series.dt.tz_localize(_base.IST)
    else:
        ts_series = ts_series.dt.tz_convert(_base.IST)
    d["ts_key_local"] = ts_series.map(_base._ts_to_key_local)
    d["nifty_context_mode"] = d["ts_key_local"].map(mode_map).fillna("BOTH")

    before = len(d)
    d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
    mode_removed = before - len(d)

    if not _base.NIFTY_RS_FILTER_ENABLED or d.empty:
        if "ts_key_local" in d.columns:
            d = d.drop(columns=["ts_key_local"])
        return d, mode_removed, 0

    stock_ret_cache: Dict = {}
    atr_cache: Dict = {}
    keep_mask = []
    rel_vals = []
    rs_removed = 0

    for row in d.itertuples(index=False):
        ts_key = getattr(row, "ts_key_local")
        mode = getattr(row, "nifty_context_mode", "BOTH")
        rel_val = np.nan
        keep = True
        apply_rs = (mode != "BOTH") or _base.NIFTY_RS_BOTH_MODE_ENABLED

        if apply_rs:
            stock_ret_map = _base._build_stock_return_map(
                getattr(row, "ticker"), cfg, stock_ret_cache
            )
            stock_ret = stock_ret_map.get(ts_key, np.nan)
            nifty_ret = nifty_ret_map.get(ts_key, np.nan)

            if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                raw_rs = float(stock_ret - nifty_ret)
                rel_val = raw_rs
                if V17C_RS_ATR_NORM_ENABLED:
                    atr_map = _build_stock_atr_map(getattr(row, "ticker"), cfg, atr_cache)
                    atr_pct = atr_map.get(ts_key, np.nan)
                    if np.isfinite(atr_pct) and atr_pct > 0:
                        rs_norm = raw_rs / atr_pct
                        thresh = (
                            V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT
                            if mode != "BOTH"
                            else V17C_RS_ATR_NORM_THRESH_SHORT_BOTH
                        )
                        keep = rs_norm <= -thresh
                    else:
                        keep = raw_rs <= -V17C_RS_ATR_FALLBACK_PCT_SHORT
                else:
                    thresh = (
                        float(_base.NIFTY_RS_THRESHOLD_PCT)
                        if mode != "BOTH"
                        else float(_base.NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT)
                    )
                    keep = raw_rs <= -thresh

        keep_mask.append(bool(keep))
        rel_vals.append(rel_val)
        if not keep:
            rs_removed += 1

    d["nifty_rel_strength_pct"] = rel_vals
    d = d[pd.Series(keep_mask, index=d.index)].copy()
    if "ts_key_local" in d.columns:
        d = d.drop(columns=["ts_key_local"])
    return d, mode_removed, rs_removed


def _v17c_apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not mode_map:
        return short_df, long_df

    short_out, s_mode_removed, s_rs_removed = _apply_short_v17c_context(
        short_df, cfg, mode_map, nifty_ret_map
    )
    _, long_out = _v16_apply_nifty_intraday_context(
        pd.DataFrame(), long_df, cfg, mode_map, nifty_ret_map
    )
    l_mode_removed = max(0, len(long_df) - len(long_out))

    print(
        "[NIFTY_CONTEXT] Applied intraday filter (V17c hybrid): "
        f"SHORT {len(short_df)}->{len(short_out)} "
        f"(mode_removed={s_mode_removed}, rs_removed={s_rs_removed}) "
        f"[ATR both/directional={V17C_RS_ATR_NORM_THRESH_SHORT_BOTH:.2f}/{V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT:.2f}"
        f" | fallback={V17C_RS_ATR_FALLBACK_PCT_SHORT:.2f}%] | "
        f"LONG {len(long_df)}->{len(long_out)} "
        f"(v16 long filter retained, total_removed={l_mode_removed})"
    )
    return short_out, long_out


_base._apply_nifty_intraday_context = _v17c_apply_nifty_intraday_context


# ===========================================================================
# PATCH 2c: v17c hybrid cleanup bundle
# ===========================================================================
def _v17c_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v16_apply_post_scan_filters(short_df, long_df)

    if not V17C_FILTER_BUNDLE_ENABLED:
        return short_df, long_df

    short_before = len(short_df)
    long_before = len(long_df)
    short_rsi_removed = 0
    short_high_adx_removed = 0
    short_pullback_removed = 0
    short_pullback_exception_kept = 0
    long_late_removed = 0

    if not short_df.empty:
        short_work = short_df.copy()
        if (
            V17C_SHORT_BLOCK_SHORTONLY_RSI_ENABLED
            and "rsi_signal" in short_work.columns
            and "nifty_context_mode" in short_work.columns
        ):
            short_rsi = pd.to_numeric(short_work["rsi_signal"], errors="coerce")
            short_mode = (
                short_work["nifty_context_mode"]
                .astype(str)
                .str.upper()
                .str.strip()
            )
            mask_short_rsi = (
                short_mode.eq("SHORT_ONLY")
                & short_rsi.ge(V17C_SHORT_SHORTONLY_RSI_LO)
                & short_rsi.lt(V17C_SHORT_SHORTONLY_RSI_HI)
            )
            short_rsi_removed = int(mask_short_rsi.sum())
            short_work = short_work.loc[~mask_short_rsi].copy()

        if (
            V17C_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED
            and "adx_signal" in short_work.columns
            and "nifty_context_mode" in short_work.columns
        ):
            short_adx = pd.to_numeric(short_work["adx_signal"], errors="coerce")
            short_mode2 = (
                short_work["nifty_context_mode"]
                .astype(str)
                .str.upper()
                .str.strip()
            )
            mask_short_high_adx = (
                short_mode2.eq("SHORT_ONLY")
                & short_adx.ge(V17C_SHORT_SHORTONLY_HIGH_ADX_MIN)
            )
            short_high_adx_removed = int(mask_short_high_adx.sum())
            short_work = short_work.loc[~mask_short_high_adx].copy()

        if V17C_DISABLE_SHORT_PULLBACK_SETUP and "setup" in short_work.columns:
            short_setup = short_work["setup"].astype(str).str.upper().str.strip()
            mask_pullback = short_setup.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW")
            mask_pullback_keep = pd.Series(False, index=short_work.index)

            if (
                V17C_SHORT_PULLBACK_EXCEPTION_ENABLED
                and mask_pullback.any()
                and {"adx_signal", "nifty_context_mode"}.issubset(short_work.columns)
            ):
                pullback_adx = pd.to_numeric(short_work["adx_signal"], errors="coerce")
                pullback_mode = (
                    short_work["nifty_context_mode"]
                    .astype(str)
                    .str.upper()
                    .str.strip()
                )
                ts_col = (
                    "entry_time_ist"
                    if "entry_time_ist" in short_work.columns
                    else "signal_time_ist"
                    if "signal_time_ist" in short_work.columns
                    else None
                )
                if ts_col is not None:
                    pullback_ts = pd.to_datetime(short_work[ts_col], errors="coerce")
                    pullback_hour = pullback_ts.dt.hour
                else:
                    pullback_hour = pd.Series(np.nan, index=short_work.index)

                mask_pullback_keep = (
                    mask_pullback
                    & pullback_adx.ge(V17C_SHORT_PULLBACK_EXCEPTION_MIN_ADX)
                    & (
                        pullback_hour.ge(V17C_SHORT_PULLBACK_EXCEPTION_MIN_HOUR)
                        | (
                            V17C_SHORT_PULLBACK_EXCEPTION_ALLOW_SHORTONLY_ANY_HOUR
                            & pullback_mode.eq("SHORT_ONLY")
                        )
                    )
                )

            short_pullback_exception_kept = int(mask_pullback_keep.sum())
            mask_pullback_remove = mask_pullback & ~mask_pullback_keep
            short_pullback_removed = int(mask_pullback_remove.sum())
            short_work = short_work.loc[~mask_pullback_remove].copy()

        short_df = short_work

    if not long_df.empty and V17C_LONG_LATE_ANTICHASE_ENABLED:
        long_work = long_df.copy()
        if {
            "setup",
            "bars_from_open",
            "entry_bar_vol_ratio",
        }.issubset(long_work.columns):
            long_setup = long_work["setup"].astype(str).str.upper().str.strip()
            long_bfo = pd.to_numeric(long_work["bars_from_open"], errors="coerce")
            long_vr = pd.to_numeric(long_work["entry_bar_vol_ratio"], errors="coerce")
            mask_late_chase = (
                long_setup.eq(V17C_LONG_LATE_SETUP_NAME)
                & long_bfo.ge(V17C_LONG_LATE_MIN_BARS_FROM_OPEN)
                & long_vr.ge(V17C_LONG_LATE_MIN_VOL_RATIO)
            )
            long_late_removed = int(mask_late_chase.sum())
            long_work = long_work.loc[~mask_late_chase].copy()
        long_df = long_work

    print(
        f"[V17C_FILTER] SHORT: {short_before}->{len(short_df)} "
        f"(-{short_rsi_removed} SHORT_ONLY RSI "
        f"[{V17C_SHORT_SHORTONLY_RSI_LO:.1f},{V17C_SHORT_SHORTONLY_RSI_HI:.1f}), "
        f"-{short_high_adx_removed} SHORT_ONLY ADX>={V17C_SHORT_SHORTONLY_HIGH_ADX_MIN:.1f}, "
        f"-{short_pullback_removed} pullback setup"
        f" (+{short_pullback_exception_kept} strong-trend pullback exceptions)) | "
        f"LONG: {long_before}->{len(long_df)} "
        f"(-{long_late_removed} late {V17C_LONG_LATE_SETUP_NAME} "
        f"bars>={V17C_LONG_LATE_MIN_BARS_FROM_OPEN}, "
        f"vol>={V17C_LONG_LATE_MIN_VOL_RATIO:.1f}x)"
    )
    return short_df, long_df


def _v17c_get_filter_reason(row: dict, side: str):
    reason = _v16_get_filter_reason(row, side)
    if reason is not None or not V17C_FILTER_BUNDLE_ENABLED:
        return reason

    side_u = str(side).upper().strip()
    if side_u == "SHORT":
        setup = str(row.get("setup", "")).upper().strip()
        if V17C_DISABLE_SHORT_PULLBACK_SETUP and setup == "A_PULLBACK_C2_THEN_BREAK_C2_LOW":
            try:
                adx = float(row.get("adx_signal", float("nan")))
            except (TypeError, ValueError):
                adx = float("nan")
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            ts_raw = row.get("entry_time_ist", row.get("signal_time_ist"))
            ts_val = pd.to_datetime(ts_raw, errors="coerce")
            hour = int(ts_val.hour) if not pd.isna(ts_val) else None
            allow_pullback = (
                V17C_SHORT_PULLBACK_EXCEPTION_ENABLED
                and np.isfinite(adx)
                and adx >= V17C_SHORT_PULLBACK_EXCEPTION_MIN_ADX
                and (
                    (hour is not None and hour >= V17C_SHORT_PULLBACK_EXCEPTION_MIN_HOUR)
                    or (
                        V17C_SHORT_PULLBACK_EXCEPTION_ALLOW_SHORTONLY_ANY_HOUR
                        and mode == "SHORT_ONLY"
                    )
                )
            )
            if not allow_pullback:
                return "v17c short cleanup: pullback setup disabled"

        if V17C_SHORT_BLOCK_SHORTONLY_RSI_ENABLED:
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            try:
                rsi = float(row.get("rsi_signal", float("nan")))
            except (TypeError, ValueError):
                rsi = float("nan")
            if (
                mode == "SHORT_ONLY"
                and np.isfinite(rsi)
                and V17C_SHORT_SHORTONLY_RSI_LO <= rsi < V17C_SHORT_SHORTONLY_RSI_HI
            ):
                return (
                    f"v17c short cleanup: SHORT_ONLY RSI={rsi:.1f} in "
                    f"[{V17C_SHORT_SHORTONLY_RSI_LO:.1f},{V17C_SHORT_SHORTONLY_RSI_HI:.1f})"
                )

        if V17C_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED:
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            try:
                adx = float(row.get("adx_signal", float("nan")))
            except (TypeError, ValueError):
                adx = float("nan")
            if (
                mode == "SHORT_ONLY"
                and np.isfinite(adx)
                and adx >= V17C_SHORT_SHORTONLY_HIGH_ADX_MIN
            ):
                return (
                    f"v17c short cleanup: SHORT_ONLY ADX={adx:.1f} >= "
                    f"{V17C_SHORT_SHORTONLY_HIGH_ADX_MIN:.1f}"
                )
        return None

    if V17C_LONG_LATE_ANTICHASE_ENABLED:
        setup = str(row.get("setup", "")).upper().strip()
        try:
            bfo = float(row.get("bars_from_open", float("nan")))
            vr = float(row.get("entry_bar_vol_ratio", float("nan")))
        except (TypeError, ValueError):
            bfo = float("nan")
            vr = float("nan")
        if (
            setup == V17C_LONG_LATE_SETUP_NAME
            and np.isfinite(bfo)
            and np.isfinite(vr)
            and bfo >= V17C_LONG_LATE_MIN_BARS_FROM_OPEN
            and vr >= V17C_LONG_LATE_MIN_VOL_RATIO
        ):
            return (
                f"v17c long anti-chase: setup={setup}, bars_from_open={int(bfo)}, "
                f"vol_ratio={vr:.2f}x"
            )

    return None


_base._apply_v16_post_scan_filters = _v17c_apply_post_scan_filters
_base.get_v16_filter_reason = _v17c_get_filter_reason


if __name__ == "__main__":
    print("=" * 70)
    print("V17c 5-min runner: best-short / best-long hybrid")
    print("  SHORT context: raw RS <= -0.75% (same as V16 live)")
    print("  LONG context:  raw RS >= +0.75% (same as V16 live)")
    print("  Exits: fixed LIMIT target + SL-M stop only (no BE, no trail)")
    print("  Nifty confirm time: 09:20 (matches V16 live)")
    print(
        "  Short cleanup: block SHORT_ONLY RSI "
        f"[{V17C_SHORT_SHORTONLY_RSI_LO:.1f},{V17C_SHORT_SHORTONLY_RSI_HI:.1f})"
        f" + SHORT_ONLY ADX>={V17C_SHORT_SHORTONLY_HIGH_ADX_MIN:.1f}"
    )
    if V17C_DISABLE_SHORT_PULLBACK_SETUP:
        print(
            "  Short pullback rule: disable pullback setup except when "
            f"ADX>={V17C_SHORT_PULLBACK_EXCEPTION_MIN_ADX:.1f} and "
            f"(hour>={V17C_SHORT_PULLBACK_EXCEPTION_MIN_HOUR}"
            + (
                " or mode=SHORT_ONLY)"
                if V17C_SHORT_PULLBACK_EXCEPTION_ALLOW_SHORTONLY_ANY_HOUR
                else ")"
            )
        )
    print(
        "  Long cleanup: block "
        f"{V17C_LONG_LATE_SETUP_NAME} when bars_from_open>="
        f"{V17C_LONG_LATE_MIN_BARS_FROM_OPEN} and vol_ratio>="
        f"{V17C_LONG_LATE_MIN_VOL_RATIO:.1f}x"
    )
    print(f"  Data dir: {V17C_DATA_5M_DIR}")
    print("=" * 70)
    _base.main()
