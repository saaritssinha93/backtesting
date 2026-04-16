# -*- coding: utf-8 -*-
"""
V17b 5-min combined runner - hybrid context filter.

Idea:
  - SHORT side uses v17 ATR-normalized RS context filtering
  - LONG side uses the original v16 raw-RS context filtering

All other logic remains identical to v16 Run14.
Outputs go to outputs_v17b_5min/ (separate from v16/v17/v18).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v16_5min as _base


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


# Keep a handle to the original v16 long/short context filter before patching.
_v16_apply_nifty_intraday_context = _base._apply_nifty_intraday_context


# ===========================================================================
# V17b CONFIG - v17 short-side ATR-normalized RS only
# ===========================================================================
V17B_RS_ATR_NORM_ENABLED = True
V17B_RS_ATR_NORM_THRESH_SHORT_BOTH = _env_float(
    "EQIDV17B_RS_ATR_NORM_THRESH_SHORT_BOTH", 0.50
)
V17B_RS_ATR_DIRECTIONAL_THRESH_SHORT = _env_float(
    "EQIDV17B_RS_ATR_DIRECTIONAL_THRESH_SHORT", 0.35
)
V17B_RS_ATR_FALLBACK_PCT_SHORT = _env_float(
    "EQIDV17B_RS_ATR_FALLBACK_PCT_SHORT", 0.40
)
V17B_DATA_5M_DIR = Path(
    os.getenv(
        "EQIDV17B_DATA_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live",
    )
)


# ===========================================================================
# PATCH 1: redirect all outputs from v16_5min -> v17b_5min
# ===========================================================================
_orig_runtime_dir = _base.runtime_dir


def _v17b_runtime_dir(*parts):
    new_parts = tuple(str(p).replace("v16_5min", "v17b_5min") for p in parts)
    return _orig_runtime_dir(*new_parts)


_base.runtime_dir = _v17b_runtime_dir


# ===========================================================================
# PATCH 2: use live 5-minute parquet universe for v17b
# ===========================================================================
_base.RUNTIME_DATA_5M_DIR = V17B_DATA_5M_DIR


# ===========================================================================
# ATR MAP BUILDER
# ===========================================================================
_v17b_atr_cache: Dict[str, Dict[str, float]] = {}


def _build_stock_atr_map(ticker: str, cfg, cache: Dict) -> Dict[str, float]:
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
        df["atr_num"] = pd.to_numeric(df[atr_col], errors="coerce")
        df["atr_pct"] = df["atr_num"] / df["close_num"].replace(0, np.nan) * 100.0
        for ts, atr_pct in zip(df["date"], df["atr_pct"]):
            if np.isfinite(atr_pct) and atr_pct > 0:
                out[_base._ts_to_key_local(ts)] = float(atr_pct)
    except Exception:
        out = {}
    cache[ticker_u] = out
    return out


def _apply_short_v17b_context(
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
                if V17B_RS_ATR_NORM_ENABLED:
                    atr_map = _build_stock_atr_map(getattr(row, "ticker"), cfg, atr_cache)
                    atr_pct = atr_map.get(ts_key, np.nan)
                    if np.isfinite(atr_pct) and atr_pct > 0:
                        rs_norm = raw_rs / atr_pct
                        thresh = (
                            V17B_RS_ATR_DIRECTIONAL_THRESH_SHORT
                            if mode != "BOTH"
                            else V17B_RS_ATR_NORM_THRESH_SHORT_BOTH
                        )
                        keep = rs_norm <= -thresh
                    else:
                        keep = raw_rs <= -V17B_RS_ATR_FALLBACK_PCT_SHORT
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


def _v17b_apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not mode_map:
        return short_df, long_df

    short_out, s_mode_removed, s_rs_removed = _apply_short_v17b_context(
        short_df, cfg, mode_map, nifty_ret_map
    )
    _, long_out = _v16_apply_nifty_intraday_context(
        pd.DataFrame(), long_df, cfg, mode_map, nifty_ret_map
    )
    l_mode_removed = max(0, len(long_df) - len(long_out))

    print(
        "[NIFTY_CONTEXT] Applied intraday filter (V17b hybrid): "
        f"SHORT {len(short_df)}->{len(short_out)} "
        f"(mode_removed={s_mode_removed}, rs_removed={s_rs_removed}) "
        f"[ATR both/directional={V17B_RS_ATR_NORM_THRESH_SHORT_BOTH:.2f}/{V17B_RS_ATR_DIRECTIONAL_THRESH_SHORT:.2f}"
        f" | fallback={V17B_RS_ATR_FALLBACK_PCT_SHORT:.2f}%] | "
        f"LONG {len(long_df)}->{len(long_out)} "
        f"(v16 long filter retained, total_removed={l_mode_removed})"
    )
    return short_out, long_out


_base._apply_nifty_intraday_context = _v17b_apply_nifty_intraday_context


if __name__ == "__main__":
    print("=" * 70)
    print("V17b 5-min runner: v17 shorts + v16 longs")
    print(
        "  SHORT ATR-normalized RS: "
        f"BOTH>={V17B_RS_ATR_NORM_THRESH_SHORT_BOTH:.2f}, "
        f"directional>={V17B_RS_ATR_DIRECTIONAL_THRESH_SHORT:.2f}"
    )
    print(
        "  SHORT fallback raw RS: "
        f"<=-{V17B_RS_ATR_FALLBACK_PCT_SHORT:.2f}%"
    )
    print("  LONG context filter: original v16 raw-RS path")
    print("=" * 70)
    _base.main()
