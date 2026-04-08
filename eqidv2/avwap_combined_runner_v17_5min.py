# -*- coding: utf-8 -*-
"""
V17 5-min combined runner — ATR-normalized Relative Strength filter.

V16 baseline : stock_ret - nifty_ret  >= 0.75%  (fixed pct threshold, BOTH mode)
V17 change   : (stock_ret - nifty_ret) / stock_atr_pct  >= 0.50 ATR units

Why ATR-normalised is better:
  • Flat-NIFTY day: a stock up 0.5% vs NIFTY +0.1% = +0.4% raw RS
    → v16 rejects (< 0.75%), v17 passes if ATR ~ 0.6% (= 0.67 ATR units)
  • Noisy high-vol stock: raw RS 0.9% but ATR 2.5% → only 0.36 ATR units
    → v16 accepts (> 0.75%), v17 rejects (< 0.50) — correctly filters noise
  • Adapts to both cross-sectional volatility AND intraday regime automatically

All other logic (signal windows, QS filters, vol exhaust, etc.) identical to v16 Run14.
Outputs go to outputs_v17_5min/ (separate from v16).
"""
from __future__ import annotations

import os
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple

# Import the full v16 runner as the base — we monkey-patch what we need
import avwap_combined_runner_v16_5min as _base

# ===========================================================================
# V17 CONFIG — ATR-normalised RS
# ===========================================================================
def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


V17_RS_ATR_NORM_ENABLED     = True
V17_RS_ATR_NORM_THRESH_LONG_BOTH = _env_float(
    "EQIDV17_RS_ATR_NORM_THRESH_LONG_BOTH", 0.60
)
V17_RS_ATR_NORM_THRESH_SHORT_BOTH = _env_float(
    "EQIDV17_RS_ATR_NORM_THRESH_SHORT_BOTH", 0.50
)
V17_RS_ATR_DIRECTIONAL_THRESH_LONG = _env_float(
    "EQIDV17_RS_ATR_DIRECTIONAL_THRESH_LONG", 0.45
)
V17_RS_ATR_DIRECTIONAL_THRESH_SHORT = _env_float(
    "EQIDV17_RS_ATR_DIRECTIONAL_THRESH_SHORT", 0.35
)
V17_RS_ATR_FALLBACK_PCT_LONG = _env_float(
    "EQIDV17_RS_ATR_FALLBACK_PCT_LONG", 0.40
)
V17_RS_ATR_FALLBACK_PCT_SHORT = _env_float(
    "EQIDV17_RS_ATR_FALLBACK_PCT_SHORT", 0.40
)
# Research-only extra long tightening. Defaults preserve the original v17 behaviour.
V17_RS_RAW_FLOOR_LONG_BOTH = _env_float(
    "EQIDV17_RS_RAW_FLOOR_LONG_BOTH", 0.45
)
V17_RS_RAW_FLOOR_LONG_DIRECTIONAL = _env_float(
    "EQIDV17_RS_RAW_FLOOR_LONG_DIRECTIONAL", 0.30
)

# ===========================================================================
# PATCH 1: redirect all outputs from v16_5min -> v17_5min
# ===========================================================================
_orig_runtime_dir = _base.runtime_dir

def _v17_runtime_dir(*parts):
    new_parts = tuple(str(p).replace("v16_5min", "v17_5min") for p in parts)
    return _orig_runtime_dir(*new_parts)

_base.runtime_dir = _v17_runtime_dir

# ===========================================================================
# ATR MAP BUILDER
# Loads 5-min parquet for a ticker and returns  ts_key -> atr_pct
# where atr_pct = ATR / close * 100  (same time-key format as return map)
# ===========================================================================
_v17_atr_cache: Dict[str, Dict[str, float]] = {}


def _build_stock_atr_map(ticker: str, cfg, cache: Dict) -> Dict[str, float]:
    """Return {ts_key: atr_pct} for every 5-min bar of this ticker."""
    ticker_u = str(ticker).strip().upper()
    if ticker_u in cache:
        return cache[ticker_u]
    out: Dict[str, float] = {}
    p = Path(cfg.dir_15m) / f"{ticker_u}{cfg.end_15m}"
    if not p.exists():
        cache[ticker_u] = out
        return out
    try:
        df = _base.read_15m_parquet(str(p), cfg.parquet_engine)
        if df.empty:
            cache[ticker_u] = out
            return out
        # ATR column may be capitalised differently
        atr_col = next((c for c in df.columns if c.lower() == "atr"), None)
        if atr_col is None:
            cache[ticker_u] = out
            return out
        df = df.sort_values("date").reset_index(drop=True)
        dt = pd.to_datetime(df["date"], errors="coerce")
        df = df.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert(_base.IST)
        df["date"] = dt.dt.tz_convert(_base.IST)
        df["close_num"] = pd.to_numeric(df["close"], errors="coerce")
        df["atr_num"]   = pd.to_numeric(df[atr_col], errors="coerce")
        df["atr_pct"]   = df["atr_num"] / df["close_num"].replace(0, np.nan) * 100.0
        for ts, atr_pct in zip(df["date"], df["atr_pct"]):
            if np.isfinite(atr_pct) and atr_pct > 0:
                out[_base._ts_to_key_local(ts)] = float(atr_pct)
    except Exception:
        out = {}
    cache[ticker_u] = out
    return out


# ===========================================================================
# PATCH 2: replace the NIFTY context / RS filter with ATR-normalised version
# ===========================================================================
def _v17_apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    V17: ATR-normalised RS.
    RS_norm = (stock_ret_4bars - nifty_ret_4bars) / stock_atr_pct
    LONG  passes if RS_norm >= V17_RS_ATR_NORM_THRESH  (0.50)
    SHORT passes if RS_norm <= -V17_RS_ATR_NORM_THRESH (0.50)
    Falls back to raw pct threshold if ATR unavailable.
    """
    if not mode_map:
        return short_df, long_df

    stock_ret_cache: Dict = {}
    atr_cache: Dict = {}

    def _apply_side(df: pd.DataFrame, side: str):
        if df.empty:
            return df, 0, 0

        d = df.copy()
        ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
        ts_series = pd.to_datetime(d[ts_col], errors="coerce")
        if getattr(ts_series.dt, "tz", None) is None:
            ts_series = ts_series.dt.tz_localize(_base.IST)
        else:
            ts_series = ts_series.dt.tz_convert(_base.IST)
        d["ts_key_local"]       = ts_series.map(_base._ts_to_key_local)
        d["nifty_context_mode"] = d["ts_key_local"].map(mode_map).fillna("BOTH")

        before = len(d)
        if side.upper() == "SHORT":
            d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
        else:
            d = d[d["nifty_context_mode"].ne("SHORT_ONLY")].copy()
        mode_removed = before - len(d)

        if not _base.NIFTY_RS_FILTER_ENABLED or d.empty:
            if "ts_key_local" in d.columns:
                d = d.drop(columns=["ts_key_local"])
            return d, mode_removed, 0

        keep_mask = []
        rel_vals  = []
        rs_removed = 0
        side_u = side.upper()

        for row in d.itertuples(index=False):
            ts_key = getattr(row, "ts_key_local")
            mode   = getattr(row, "nifty_context_mode", "BOTH")
            rel_val = np.nan
            keep    = True
            raw_floor = (
                V17_RS_RAW_FLOOR_LONG_DIRECTIONAL
                if (side_u == "LONG" and mode != "BOTH")
                else V17_RS_RAW_FLOOR_LONG_BOTH
                if side_u == "LONG"
                else 0.0
            )

            apply_rs = (mode != "BOTH") or _base.NIFTY_RS_BOTH_MODE_ENABLED

            if apply_rs:
                stock_ret_map = _base._build_stock_return_map(
                    getattr(row, "ticker"), cfg, stock_ret_cache
                )
                stock_ret = stock_ret_map.get(ts_key, np.nan)
                nifty_ret = nifty_ret_map.get(ts_key, np.nan)

                if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                    raw_rs  = float(stock_ret - nifty_ret)
                    rel_val = raw_rs

                    if V17_RS_ATR_NORM_ENABLED:
                        atr_map = _build_stock_atr_map(
                            getattr(row, "ticker"), cfg, atr_cache
                        )
                        atr_pct = atr_map.get(ts_key, np.nan)
                        if np.isfinite(atr_pct) and atr_pct > 0:
                            rs_norm = raw_rs / atr_pct
                            if side_u == "LONG":
                                thresh = (
                                    V17_RS_ATR_DIRECTIONAL_THRESH_LONG
                                    if mode != "BOTH"
                                    else V17_RS_ATR_NORM_THRESH_LONG_BOTH
                                )
                            else:
                                thresh = (
                                    V17_RS_ATR_DIRECTIONAL_THRESH_SHORT
                                    if mode != "BOTH"
                                    else V17_RS_ATR_NORM_THRESH_SHORT_BOTH
                                )
                            keep = rs_norm >= thresh if side_u == "LONG" else rs_norm <= -thresh
                            if side_u == "LONG" and raw_floor > 0:
                                keep = keep and raw_rs >= raw_floor
                        else:
                            # No ATR — fall back to raw pct fallback
                            fallback_thresh = (
                                V17_RS_ATR_FALLBACK_PCT_LONG
                                if side_u == "LONG"
                                else V17_RS_ATR_FALLBACK_PCT_SHORT
                            )
                            keep = (
                                raw_rs >= fallback_thresh
                                if side_u == "LONG"
                                else raw_rs <= -fallback_thresh
                            )
                            if side_u == "LONG" and raw_floor > 0:
                                keep = keep and raw_rs >= raw_floor
                    else:
                        # V17_RS_ATR_NORM disabled → exact v16 behaviour
                        if mode != "BOTH":
                            thresh = float(_base.NIFTY_RS_THRESHOLD_PCT)
                        else:
                            thresh = float(
                                _base.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT
                                if side_u == "LONG"
                                else _base.NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT
                            )
                        keep = raw_rs >= thresh if side_u == "LONG" else raw_rs <= -thresh

            keep_mask.append(bool(keep))
            rel_vals.append(rel_val)
            if not keep:
                rs_removed += 1

        d["nifty_rel_strength_pct"] = rel_vals
        d = d[pd.Series(keep_mask, index=d.index)].copy()
        if "ts_key_local" in d.columns:
            d = d.drop(columns=["ts_key_local"])
        return d, mode_removed, rs_removed

    s, s_mode_removed, s_rs_removed = _apply_side(short_df, "SHORT")
    l, l_mode_removed, l_rs_removed = _apply_side(long_df, "LONG")

    print(
        "[NIFTY_CONTEXT] Applied intraday filter (V17 ATR-norm RS): "
        f"SHORT {len(short_df)}->{len(s)} "
        f"(mode_removed={s_mode_removed}, rs_removed={s_rs_removed}) | "
        f"LONG {len(long_df)}->{len(l)} "
        f"(mode_removed={l_mode_removed}, rs_removed={l_rs_removed}) "
        "[LONG both/directional="
        f"{V17_RS_ATR_NORM_THRESH_LONG_BOTH:.2f}/{V17_RS_ATR_DIRECTIONAL_THRESH_LONG:.2f} ATR"
        f" | SHORT both/directional={V17_RS_ATR_NORM_THRESH_SHORT_BOTH:.2f}/{V17_RS_ATR_DIRECTIONAL_THRESH_SHORT:.2f} ATR"
        f" | LONG raw floor both/directional={V17_RS_RAW_FLOOR_LONG_BOTH:.2f}/{V17_RS_RAW_FLOOR_LONG_DIRECTIONAL:.2f}%"
        f" | fallback long/short={V17_RS_ATR_FALLBACK_PCT_LONG:.2f}/{V17_RS_ATR_FALLBACK_PCT_SHORT:.2f}%]"
    )
    return s, l


# Apply patch — must happen before main() is called
_base._apply_nifty_intraday_context = _v17_apply_nifty_intraday_context


if __name__ == "__main__":
    print("=" * 70)
    print("V17 5-min runner: ATR-normalised RS filter")
    print(
        "  LONG RS_ATR thresholds: "
        f"BOTH>={V17_RS_ATR_NORM_THRESH_LONG_BOTH:.2f}, "
        f"directional>={V17_RS_ATR_DIRECTIONAL_THRESH_LONG:.2f}"
    )
    print(
        "  SHORT RS_ATR thresholds: "
        f"BOTH>={V17_RS_ATR_NORM_THRESH_SHORT_BOTH:.2f}, "
        f"directional>={V17_RS_ATR_DIRECTIONAL_THRESH_SHORT:.2f}"
    )
    print(
        "  LONG raw RS floor: "
        f"BOTH>={V17_RS_RAW_FLOOR_LONG_BOTH:.2f}%, "
        f"directional>={V17_RS_RAW_FLOOR_LONG_DIRECTIONAL:.2f}%"
    )
    print(
        "  Fallback (no ATR): "
        f"LONG>={V17_RS_ATR_FALLBACK_PCT_LONG:.2f}% raw RS, "
        f"SHORT<=-{V17_RS_ATR_FALLBACK_PCT_SHORT:.2f}% raw RS"
    )
    print("  All other settings identical to v16 Run14")
    print("=" * 70)
    _base.main()
