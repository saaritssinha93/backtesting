# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v18c6_5min.py - AVWAP v18c6 hybrid wrapper on 5-minute data
=================================================================================

V18c6 is a selective mix built on top of v18c5:

1. Keeps the v18c5 shortlist-v2 / quality-filter backbone.
2. Keeps v18c5 for the main LONG breakout setup: A_MOD_BREAK_C1_HIGH.
3. Keeps v18c5 for the main SHORT breakdown setup: A_MOD_BREAK_C1_LOW.
4. Uses a faster 4-bar RS context only for SHORT continuation setup:
   C_SHORT_CONTINUATION_BREAK.
5. Uses a faster / looser reclaim-specific LONG RS context plus a dedicated
   reclaim exit profile for B_HUGE_C1_CLOSE_RECLAIM_BREAK.

This stays intentionally thin so the hybrid can be iterated without forking the
full runner.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v18c5_5min as _base


V18C6_DATA_5M_DIR = Path(
    os.getenv(
        "EQIDV18C6_DATA_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2",
    )
)

V18C6_SHORT_CONT_RS_LOOKBACK_BARS = 4
V18C6_LONG_RECLAIM_RS_LOOKBACK_BARS = 4
V18C6_LONG_RECLAIM_RS_BOTH_PCT = 0.50
V18C6_LONG_RECLAIM_RS_DIRECTIONAL_PCT = float(_base.NIFTY_RS_THRESHOLD_PCT)
V18C6_LONG_RECLAIM_BE_TRIGGER_PCT = 0.0060
V18C6_LONG_RECLAIM_TRAIL_PCT = 0.0050

# Sweep-optimised exit parameters (param sweep run, 2026-04-15)
V18C6_SHORT_STOP_PCT   = 0.0085   # SHORT SL:  0.75% → 0.85%
V18C6_SHORT_TARGET_PCT = 0.0140   # SHORT TGT: 0.95% → 1.40%
V18C6_LONG_STOP_PCT    = 0.0070   # LONG  SL:  0.75% → 0.70%
V18C6_LONG_TARGET_PCT  = 0.0130   # LONG  TGT: 1.00% → 1.30%

_ORIG_RUNTIME_DIR = _base.runtime_dir
_ORIG_EXIT_PROFILE = _base._v17c_setup_exit_profile

_RET_CACHE: Dict[Tuple[str, str, int], Dict[str, float]] = {}
_NIFTY_RET_CACHE: Dict[Tuple[str, int], Dict[str, float]] = {}


def _v18c6_resolve_15m_dir() -> Path:
    """
    Force v18c6 to use stocks_indicators_5min_eq_live2 as the single source of truth.

    This bypasses v18c5's timestamp-based auto-resolver so the live directory
    (which has full history + latest data) is always used regardless of what
    other candidate directories exist.
    """
    if V18C6_DATA_5M_DIR.is_dir():
        return V18C6_DATA_5M_DIR
    return _base._resolve_15m_dir()


def _v18c6_runtime_dir(_name: str) -> Path:
    return _ORIG_RUNTIME_DIR("outputs_v18c6_5min")


def _build_return_map_with_lookback(
    ticker: str,
    cfg: Any,
    lookback_bars: int,
) -> Dict[str, float]:
    ticker_u = str(ticker).strip().upper()
    cache_key = (str(Path(cfg.dir_15m)), ticker_u, int(lookback_bars))
    cached = _RET_CACHE.get(cache_key)
    if cached is not None:
        return cached

    out: Dict[str, float] = {}
    p = Path(cfg.dir_15m) / f"{ticker_u}{cfg.end_15m}"
    if not p.exists():
        _RET_CACHE[cache_key] = out
        return out

    try:
        df = _base.read_15m_parquet(str(p), cfg.parquet_engine)
        if df.empty:
            _RET_CACHE[cache_key] = out
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
        df["day"] = df["date"].dt.strftime("%Y-%m-%d")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["ret_lookback_pct"] = (
            df.groupby("day")["close"].pct_change(int(lookback_bars)) * 100.0
        )
        for ts, ret in zip(df["date"], df["ret_lookback_pct"]):
            if np.isfinite(ret):
                out[_base._ts_to_key_local(ts)] = float(ret)
    except Exception:
        out = {}

    _RET_CACHE[cache_key] = out
    return out


def _build_nifty_return_map_with_lookback(
    cfg: Any,
    lookback_bars: int,
) -> Dict[str, float]:
    cache_key = (str(Path(cfg.dir_15m)), int(lookback_bars))
    cached = _NIFTY_RET_CACHE.get(cache_key)
    if cached is not None:
        return cached

    out: Dict[str, float] = {}
    idx, _ = _base._load_first_nifty_source(cfg, _base.NIFTY_CONTEXT_TICKERS)
    if idx.empty:
        _NIFTY_RET_CACHE[cache_key] = out
        return out

    try:
        dt = pd.to_datetime(idx["date"], errors="coerce")
        idx = idx.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert(_base.IST)
        idx["date"] = dt.dt.tz_convert(_base.IST)
        idx = idx.sort_values("date").reset_index(drop=True)
        idx["day"] = idx["date"].dt.strftime("%Y-%m-%d")
        idx["close"] = pd.to_numeric(idx["close"], errors="coerce")
        idx["ret_lookback_pct"] = (
            idx.groupby("day")["close"].pct_change(int(lookback_bars)) * 100.0
        )
        for ts, ret in zip(idx["date"], idx["ret_lookback_pct"]):
            if np.isfinite(ret):
                out[_base._ts_to_key_local(ts)] = float(ret)
    except Exception:
        out = {}

    _NIFTY_RET_CACHE[cache_key] = out
    return out


def _apply_short_v18c6_context(
    short_df: pd.DataFrame,
    cfg: Any,
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
        return d.drop(columns=["ts_key_local"], errors="ignore"), mode_removed, 0

    cont_nifty_ret_map = _build_nifty_return_map_with_lookback(
        cfg, V18C6_SHORT_CONT_RS_LOOKBACK_BARS
    )
    atr_cache: Dict[str, Dict[str, float]] = {}
    keep_mask: List[bool] = []
    rel_vals: List[float] = []
    rs_removed = 0

    for row in d.itertuples(index=False):
        ts_key = getattr(row, "ts_key_local")
        mode = getattr(row, "nifty_context_mode", "BOTH")
        setup = str(getattr(row, "setup", "")).upper()
        rel_val = np.nan
        keep = True
        apply_rs = (mode != "BOTH") or _base.NIFTY_RS_BOTH_MODE_ENABLED

        if apply_rs:
            if setup == "C_SHORT_CONTINUATION_BREAK":
                stock_ret_map = _build_return_map_with_lookback(
                    getattr(row, "ticker"), cfg, V18C6_SHORT_CONT_RS_LOOKBACK_BARS
                )
                stock_ret = stock_ret_map.get(ts_key, np.nan)
                nifty_ret = cont_nifty_ret_map.get(ts_key, np.nan)
            else:
                stock_ret_map = _base._build_stock_return_map(
                    getattr(row, "ticker"), cfg, {}
                )
                stock_ret = stock_ret_map.get(ts_key, np.nan)
                nifty_ret = nifty_ret_map.get(ts_key, np.nan)

            if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                raw_rs = float(stock_ret - nifty_ret)
                rel_val = raw_rs
                if _base.V17C_RS_ATR_NORM_ENABLED:
                    atr_map = _base._build_stock_atr_map(getattr(row, "ticker"), cfg, atr_cache)
                    atr_pct = atr_map.get(ts_key, np.nan)
                    if np.isfinite(atr_pct) and atr_pct > 0:
                        rs_norm = raw_rs / atr_pct
                        thresh = (
                            _base.V17C_RS_ATR_DIRECTIONAL_THRESH_SHORT
                            if mode != "BOTH"
                            else _base.V17C_RS_ATR_NORM_THRESH_SHORT_BOTH
                        )
                        keep = rs_norm <= -thresh
                    else:
                        keep = raw_rs <= -_base.V17C_RS_ATR_FALLBACK_PCT_SHORT
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
    d.drop(columns=["ts_key_local"], inplace=True, errors="ignore")
    return d, mode_removed, rs_removed


def _apply_nifty_intraday_context_v18c6(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg: Any,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not mode_map:
        return short_df, long_df

    def _prepare(df: pd.DataFrame, side: str) -> Tuple[pd.DataFrame, int]:
        if df.empty:
            return df, 0
        d = df.copy()
        ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
        ts = pd.to_datetime(d[ts_col], errors="coerce")
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(_base.IST)
        else:
            ts = ts.dt.tz_convert(_base.IST)
        d["ts_key_local"] = ts.map(_base._ts_to_key_local)
        d["nifty_context_mode"] = d["ts_key_local"].map(mode_map).fillna("BOTH")
        before = len(d)
        if side == "SHORT":
            d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
        else:
            d = d[d["nifty_context_mode"].ne("SHORT_ONLY")].copy()
        return d, before - len(d)

    long_d, l_mode_removed = _prepare(long_df, "LONG")
    short_d, s_mode_removed, s_rs_removed = _apply_short_v18c6_context(
        short_df, cfg, mode_map, nifty_ret_map
    )

    l_rs_removed = 0
    reclaim_nifty_ret_map = _build_nifty_return_map_with_lookback(
        cfg, V18C6_LONG_RECLAIM_RS_LOOKBACK_BARS
    )
    keep_mask: List[bool] = []
    rel_vals: List[float] = []
    long_stock_ret_cache: Dict[str, Dict[str, float]] = {}  # shared across rows for non-reclaim LONGs

    if not long_d.empty and _base.NIFTY_RS_FILTER_ENABLED:
        for row in long_d.itertuples(index=False):
            ts_key = getattr(row, "ts_key_local")
            mode = getattr(row, "nifty_context_mode", "BOTH")
            setup = str(getattr(row, "setup", "")).upper()

            if setup == "B_HUGE_C1_CLOSE_RECLAIM_BREAK":
                stock_ret_map = _build_return_map_with_lookback(
                    getattr(row, "ticker"), cfg, V18C6_LONG_RECLAIM_RS_LOOKBACK_BARS
                )
                stock_ret = stock_ret_map.get(ts_key, np.nan)
                nifty_ret = reclaim_nifty_ret_map.get(ts_key, np.nan)
            else:
                stock_ret_map = _base._build_stock_return_map(
                    getattr(row, "ticker"), cfg, long_stock_ret_cache
                )
                stock_ret = stock_ret_map.get(ts_key, np.nan)
                nifty_ret = nifty_ret_map.get(ts_key, np.nan)

            raw_rs = np.nan
            if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                raw_rs = float(stock_ret - nifty_ret)
            rel_vals.append(raw_rs)

            keep = True
            if np.isfinite(raw_rs):
                if setup == "B_HUGE_C1_CLOSE_RECLAIM_BREAK":
                    rs_thresh = (
                        float(V18C6_LONG_RECLAIM_RS_DIRECTIONAL_PCT)
                        if mode != "BOTH"
                        else float(V18C6_LONG_RECLAIM_RS_BOTH_PCT)
                    )
                else:
                    if mode != "BOTH":
                        rs_thresh = max(float(_base.NIFTY_RS_THRESHOLD_PCT), 0.35)
                    elif _base.NIFTY_RS_BOTH_MODE_ENABLED:
                        rs_thresh = float(_base.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT)
                    else:
                        rs_thresh = 0.0
                    if setup == "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK":
                        rs_thresh = (
                            float(_base.V17C_LONG_HUGE_PULLBACK_RS_DIRECTIONAL_PCT)
                            if mode != "BOTH"
                            else float(_base.V17C_LONG_HUGE_PULLBACK_RS_BOTH_PCT)
                        )
                keep = raw_rs >= rs_thresh

            keep_mask.append(bool(keep))
            if not keep:
                l_rs_removed += 1

        long_d["nifty_rel_strength_pct"] = rel_vals
        long_d = long_d[pd.Series(keep_mask, index=long_d.index)].copy()

    for frame in (short_d, long_d):
        if not frame.empty:
            frame.drop(columns=["ts_key_local"], inplace=True, errors="ignore")

    print(
        "[NIFTY_CONTEXT] Applied intraday filter (v18c6 hybrid): "
        f"SHORT {len(short_df)}->{len(short_d)} "
        f"(mode_removed={s_mode_removed}, rs_removed={s_rs_removed}, "
        f"short_cont_lookback={V18C6_SHORT_CONT_RS_LOOKBACK_BARS}) | "
        f"LONG {len(long_df)}->{len(long_d)} "
        f"(mode_removed={l_mode_removed}, rs_removed={l_rs_removed}, "
        f"reclaim_rs_lookback={V18C6_LONG_RECLAIM_RS_LOOKBACK_BARS}, "
        f"reclaim_both_thresh={V18C6_LONG_RECLAIM_RS_BOTH_PCT:.2f}%)"
    )
    return short_d, long_d


def _v18c6_setup_exit_profile(
    side_val: str,
    setup_val: str,
    entry_time_ist=None,
) -> Dict[str, float]:
    profile = dict(
        _ORIG_EXIT_PROFILE(side_val, setup_val, entry_time_ist=entry_time_ist)
    )
    side_u = str(side_val or "").upper()
    setup_u = str(setup_val or "").upper()

    if side_u == "SHORT":
        # All SHORT setups: SL=0.85%, TGT=1.40% (sweep-optimised)
        profile.update(
            stop_pct=V18C6_SHORT_STOP_PCT,
            target_pct=V18C6_SHORT_TARGET_PCT,
            enable_breakeven=False,
            enable_trailing_stop=False,
        )
        return profile

    # LONG side
    if setup_u == "B_HUGE_C1_CLOSE_RECLAIM_BREAK":
        # RECLAIM: flat SL=0.85%, TGT=1.20% — no BE/trailing
        profile.update(
            stop_pct=0.0085,
            target_pct=0.0120,
            be_trigger_pct=0.0,
            trail_pct=0.0,
            be_pad_pct=0.0001,
            enable_breakeven=False,
            enable_trailing_stop=False,
        )
    else:
        # All other LONG setups: SL=0.70%, TGT=1.30% (sweep-optimised)
        profile.update(
            stop_pct=V18C6_LONG_STOP_PCT,
            target_pct=V18C6_LONG_TARGET_PCT,
            enable_breakeven=False,
            enable_trailing_stop=False,
        )
    return profile


_base._resolve_15m_dir = _v18c6_resolve_15m_dir
_base.runtime_dir = _v18c6_runtime_dir
_base._apply_nifty_intraday_context = _apply_nifty_intraday_context_v18c6
_base._v17c_setup_exit_profile = _v18c6_setup_exit_profile


def _rename_new_outputs(output_dir: Path, before: set[str]) -> List[Path]:
    renamed: List[Path] = []
    for path in output_dir.iterdir():
        if not path.is_file() or path.name in before:
            continue
        new_name = (
            path.name.replace("v18c5_5min", "v18c6_5min").replace(
                "avwap_combined_runner_v18c3_", "avwap_combined_runner_v18c6_"
            )
        )
        if new_name != path.name:
            new_path = path.with_name(new_name)
            path.rename(new_path)
            renamed.append(new_path)
        else:
            renamed.append(path)
    return sorted(renamed)


def main() -> None:
    output_dir = _v18c6_runtime_dir("outputs_v18c6_5min")
    output_dir.mkdir(parents=True, exist_ok=True)
    before = {p.name for p in output_dir.iterdir() if p.is_file()}
    _base.main()
    renamed = _rename_new_outputs(output_dir, before)
    if renamed:
        print("[V18C6] New output files:")
        for path in renamed:
            print(f"[V18C6] {path}")


if __name__ == "__main__":
    main()
