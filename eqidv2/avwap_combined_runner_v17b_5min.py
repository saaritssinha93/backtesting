# -*- coding: utf-8 -*-
"""
V17b 5-min combined runner - V16 live parity core + data-driven quality bundle.

Base patches (live parity):
  - SHORT context: raw RS <= -0.75% BOTH / -0.20% directional (V16 live)
  - LONG context:  raw RS >= +0.75% (unchanged from V16)
  - Breakeven and trailing stop DISABLED (live executor: fixed LIMIT + SL-M only)
  - Nifty confirm time: 09:20 (matches V16 live detection engine)
  - Data dir: stocks_indicators_5min_eq_live2 (separate historical copy)

Research cleanup bundle (Run1, empirical):
  - Block SHORT_ONLY shorts in RSI [21,28) pocket (empirical dead zone)
  - Block SHORT_ONLY shorts with ADX >= 44 (exhausted trend)
  - Block A_PULLBACK_C2_THEN_BREAK_C2_LOW short setup
  - Block late/high-vol A_MOD_BREAK_C1_HIGH longs (anti-chase)

Data-driven quality bundle (Run2, from trade-level analysis of Run1 results):
  SHORT improvements (removes confirmed net-negative clusters):
    - Expand RSI dead zone lower bound: 35→30 (RSI[30,35) = pf=0.971, net loser)
    - Block entry 09:15-09:45 (opening-bar shorts = pf=0.786, worst time bucket)
    - Block AVWAP dist [0.5,1.0) shorts (mid-range dead zone = pf=0.986, net loser)
    - Block BOTH-mode shorts with ADX [25,30) (chop zone = pf=0.858, net loser)
    - Block LONG RSI [60,65) entries (breakout-zone = pf=0.957, net loser)
  LONG improvements (relaxes over-aggressive base filters):
    - Raise QS abs-max: 10→12 (QS 9-10 has pf=6.36; QS 10-12 expected even higher)
    - Raise vol-exhaust threshold: 4.0x→5.0x (vol[4-5x] = pf=1.70, valid entries)
    - Narrow QS dead zone: [7.5,8.0)→[7.6,7.9) (recover boundary-zone quality trades)

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


# Keep a handle to the original v16 long/short context filter before patching.
_v16_apply_nifty_intraday_context = _base._apply_nifty_intraday_context
_v16_apply_post_scan_filters = _base._apply_v16_post_scan_filters
_v16_get_filter_reason = _base.get_v16_filter_reason


# ===========================================================================
# V17b CONFIG - v17 short-side ATR-normalized RS only
# ===========================================================================
V17B_RS_ATR_NORM_ENABLED = False
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
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2",
    )
)
V17B_FILTER_BUNDLE_ENABLED = _env_bool("EQIDV17B_FILTER_BUNDLE_ENABLED", True)
V17B_SHORT_BLOCK_SHORTONLY_RSI_ENABLED = _env_bool(
    "EQIDV17B_SHORT_BLOCK_SHORTONLY_RSI_ENABLED", True
)
V17B_SHORT_SHORTONLY_RSI_LO = _env_float(
    "EQIDV17B_SHORT_SHORTONLY_RSI_LO", 21.0
)
V17B_SHORT_SHORTONLY_RSI_HI = _env_float(
    "EQIDV17B_SHORT_SHORTONLY_RSI_HI", 28.0
)
V17B_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED = _env_bool(
    "EQIDV17B_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED", True
)
V17B_SHORT_SHORTONLY_HIGH_ADX_MIN = _env_float(
    "EQIDV17B_SHORT_SHORTONLY_HIGH_ADX_MIN", 44.0
)
V17B_DISABLE_SHORT_PULLBACK_SETUP = _env_bool(
    "EQIDV17B_DISABLE_SHORT_PULLBACK_SETUP", True
)
V17B_LONG_LATE_ANTICHASE_ENABLED = _env_bool(
    "EQIDV17B_LONG_LATE_ANTICHASE_ENABLED", True
)
V17B_LONG_LATE_MIN_BARS_FROM_OPEN = int(
    _env_float("EQIDV17B_LONG_LATE_MIN_BARS_FROM_OPEN", 13)
)
V17B_LONG_LATE_MIN_VOL_RATIO = _env_float(
    "EQIDV17B_LONG_LATE_MIN_VOL_RATIO", 3.7
)
V17B_LONG_LATE_SETUP_NAME = os.getenv(
    "EQIDV17B_LONG_LATE_SETUP_NAME",
    "A_MOD_BREAK_C1_HIGH",
).strip().upper()

# ---------------------------------------------------------------------------
# Data-driven quality bundle (Run2) — new filter parameters
# ---------------------------------------------------------------------------
# SHORT: opening-bar dead window (09:15–09:45 = pf=0.786, net loser)
V17B_SHORT_BLOCK_OPEN_WINDOW = _env_bool("EQIDV17B_SHORT_BLOCK_OPEN_WINDOW", True)
V17B_SHORT_OPEN_CUTOFF_MIN = int(_env_float("EQIDV17B_SHORT_OPEN_CUTOFF_MIN", 9 * 60 + 45))

# SHORT: AVWAP mid-range dead zone (dist [0.5,1.0) ATR = pf=0.986, net loser)
V17B_SHORT_AVWAP_DEAD_ENABLED = _env_bool("EQIDV17B_SHORT_AVWAP_DEAD_ENABLED", True)
V17B_SHORT_AVWAP_DEAD_LO = _env_float("EQIDV17B_SHORT_AVWAP_DEAD_LO", 0.5)
V17B_SHORT_AVWAP_DEAD_HI = _env_float("EQIDV17B_SHORT_AVWAP_DEAD_HI", 1.0)

# SHORT: BOTH-mode ADX chop zone ([25,30) = pf=0.858, net loser in BOTH mode)
V17B_SHORT_BOTH_ADX_DEAD_ENABLED = _env_bool("EQIDV17B_SHORT_BOTH_ADX_DEAD_ENABLED", True)
V17B_SHORT_BOTH_ADX_DEAD_LO = _env_float("EQIDV17B_SHORT_BOTH_ADX_DEAD_LO", 25.0)
V17B_SHORT_BOTH_ADX_DEAD_HI = _env_float("EQIDV17B_SHORT_BOTH_ADX_DEAD_HI", 30.0)

# LONG: RSI breakout-entry dead zone ([60,65) = pf=0.957, net loser)
V17B_LONG_RSI_DEAD_ENABLED = _env_bool("EQIDV17B_LONG_RSI_DEAD_ENABLED", True)
V17B_LONG_RSI_DEAD_LO = _env_float("EQIDV17B_LONG_RSI_DEAD_LO", 60.0)
V17B_LONG_RSI_DEAD_HI = _env_float("EQIDV17B_LONG_RSI_DEAD_HI", 65.0)


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
# PATCH 2b: increase parallel workers for faster backtesting
# ===========================================================================
_base.MAX_WORKERS = 16

# ===========================================================================
# PATCH 5: Data-driven base-constant overrides (Run2 quality bundle)
# All changes are backed by trade-level analysis of the Run1 (20260418_144831)
# result CSV.  Each override is env-var overridable for future sweep runs.
# ===========================================================================

# SHORT — expand RSI dead-zone lower bound: RSI [30,35) has pf=0.971 (net loser)
_base.V16_SHORT_RSI_DEAD_ZONE_LO = _env_float("EQIDV17B_BASE_SHORT_RSI_DEAD_LO", 30.0)

# LONG — unlock exceptional high-QS trades: QS[9,10)=pf=6.36; QS 10-12 expected higher
_base.V16_LONG_QS_ABS_MAX = _env_float("EQIDV17B_BASE_LONG_QS_ABS_MAX", 12.0)

# LONG — narrow QS dead zone: recover boundary trades at [7.5,7.6) and [7.9,8.0)
_base.V16_LONG_QS_DEAD_LO = _env_float("EQIDV17B_BASE_LONG_QS_DEAD_LO", 7.6)
_base.V16_LONG_QS_DEAD_HI = _env_float("EQIDV17B_BASE_LONG_QS_DEAD_HI", 7.9)

# LONG — raise vol-exhaust threshold: vol[4-5x] entries have pf=1.70 (valid signal)
_base.V16_LONG_ENTRY_VOL_EXHAUST_MULT = _env_float("EQIDV17B_BASE_LONG_VOL_EXHAUST", 5.0)


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


# ===========================================================================
# PATCH 2c: research cleanup bundle for v17b
# ===========================================================================
def _v17b_apply_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    short_df, long_df = _v16_apply_post_scan_filters(short_df, long_df)

    if not V17B_FILTER_BUNDLE_ENABLED:
        return short_df, long_df

    short_before = len(short_df)
    long_before = len(long_df)
    short_rsi_removed = 0
    short_high_adx_removed = 0
    short_pullback_removed = 0
    short_open_removed = 0
    short_avwap_dead_removed = 0
    short_both_adx_dead_removed = 0
    long_late_removed = 0
    long_rsi_dead_removed = 0

    if not short_df.empty:
        short_work = short_df.copy()

        # --- Run1 cleanup: SHORT_ONLY RSI dead zone ---
        if (
            V17B_SHORT_BLOCK_SHORTONLY_RSI_ENABLED
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
                & short_rsi.ge(V17B_SHORT_SHORTONLY_RSI_LO)
                & short_rsi.lt(V17B_SHORT_SHORTONLY_RSI_HI)
            )
            short_rsi_removed = int(mask_short_rsi.sum())
            short_work = short_work.loc[~mask_short_rsi].copy()

        # --- Run1 cleanup: SHORT_ONLY high-ADX exhausted trend ---
        if (
            V17B_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED
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
                & short_adx.ge(V17B_SHORT_SHORTONLY_HIGH_ADX_MIN)
            )
            short_high_adx_removed = int(mask_short_high_adx.sum())
            short_work = short_work.loc[~mask_short_high_adx].copy()

        # --- Run1 cleanup: A_PULLBACK setup ---
        if V17B_DISABLE_SHORT_PULLBACK_SETUP and "setup" in short_work.columns:
            short_setup = short_work["setup"].astype(str).str.upper().str.strip()
            mask_pullback = short_setup.eq("A_PULLBACK_C2_THEN_BREAK_C2_LOW")
            short_pullback_removed = int(mask_pullback.sum())
            short_work = short_work.loc[~mask_pullback].copy()

        # --- Run2 (data-driven): block opening-bar shorts 09:15–09:45 ---
        # Analysis: opening window has pf=0.786 — net loser cluster.
        if (
            V17B_SHORT_BLOCK_OPEN_WINDOW
            and "entry_time_ist" in short_work.columns
        ):
            _st = pd.to_datetime(short_work["entry_time_ist"], errors="coerce")
            if getattr(_st.dt, "tz", None) is None:
                _st = _st.dt.tz_localize(_base.IST)
            else:
                _st = _st.dt.tz_convert(_base.IST)
            _st_min = _st.dt.hour * 60 + _st.dt.minute
            mask_open = _st_min.lt(V17B_SHORT_OPEN_CUTOFF_MIN)
            short_open_removed = int(mask_open.sum())
            short_work = short_work.loc[~mask_open].copy()

        # --- Run2 (data-driven): AVWAP dist [0.5,1.0) dead zone for shorts ---
        # Analysis: dist [0.5,1.0) ATR has pf=0.986 — mid-range no-man's land.
        if (
            V17B_SHORT_AVWAP_DEAD_ENABLED
            and "avwap_dist_atr_signal" in short_work.columns
        ):
            _dist = pd.to_numeric(short_work["avwap_dist_atr_signal"], errors="coerce")
            mask_avwap_dead = (
                _dist.ge(V17B_SHORT_AVWAP_DEAD_LO)
                & _dist.lt(V17B_SHORT_AVWAP_DEAD_HI)
            )
            short_avwap_dead_removed = int(mask_avwap_dead.sum())
            short_work = short_work.loc[~mask_avwap_dead].copy()

        # --- Run2 (data-driven): BOTH-mode ADX [25,30) chop zone ---
        # Analysis: BOTH-mode ADX[25,30) has pf=0.858 — trend too weak for short.
        if (
            V17B_SHORT_BOTH_ADX_DEAD_ENABLED
            and "adx_signal" in short_work.columns
            and "nifty_context_mode" in short_work.columns
        ):
            _adx2 = pd.to_numeric(short_work["adx_signal"], errors="coerce")
            _mode3 = (
                short_work["nifty_context_mode"]
                .astype(str)
                .str.upper()
                .str.strip()
            )
            mask_both_adx = (
                _mode3.eq("BOTH")
                & _adx2.ge(V17B_SHORT_BOTH_ADX_DEAD_LO)
                & _adx2.lt(V17B_SHORT_BOTH_ADX_DEAD_HI)
            )
            short_both_adx_dead_removed = int(mask_both_adx.sum())
            short_work = short_work.loc[~mask_both_adx].copy()

        short_df = short_work

    if not long_df.empty:
        long_work = long_df.copy()

        # --- Run1 cleanup: late A_MOD anti-chase ---
        if V17B_LONG_LATE_ANTICHASE_ENABLED and {
            "setup",
            "bars_from_open",
            "entry_bar_vol_ratio",
        }.issubset(long_work.columns):
            long_setup = long_work["setup"].astype(str).str.upper().str.strip()
            long_bfo = pd.to_numeric(long_work["bars_from_open"], errors="coerce")
            long_vr = pd.to_numeric(long_work["entry_bar_vol_ratio"], errors="coerce")
            mask_late_chase = (
                long_setup.eq(V17B_LONG_LATE_SETUP_NAME)
                & long_bfo.ge(V17B_LONG_LATE_MIN_BARS_FROM_OPEN)
                & long_vr.ge(V17B_LONG_LATE_MIN_VOL_RATIO)
            )
            long_late_removed = int(mask_late_chase.sum())
            long_work = long_work.loc[~mask_late_chase].copy()

        # --- Run2 (data-driven): LONG RSI [60,65) dead zone ---
        # Analysis: RSI[60,65) for longs has pf=0.957 — just-entered-overbought trap.
        if (
            V17B_LONG_RSI_DEAD_ENABLED
            and "rsi_signal" in long_work.columns
        ):
            _lrsi = pd.to_numeric(long_work["rsi_signal"], errors="coerce")
            mask_long_rsi = (
                _lrsi.ge(V17B_LONG_RSI_DEAD_LO)
                & _lrsi.lt(V17B_LONG_RSI_DEAD_HI)
            )
            long_rsi_dead_removed = int(mask_long_rsi.sum())
            long_work = long_work.loc[~mask_long_rsi].copy()

        long_df = long_work

    print(
        f"[V17B_FILTER] SHORT: {short_before}->{len(short_df)} "
        f"(-{short_rsi_removed} SO-RSI[{V17B_SHORT_SHORTONLY_RSI_LO:.0f},{V17B_SHORT_SHORTONLY_RSI_HI:.0f}) "
        f"-{short_high_adx_removed} SO-ADX>={V17B_SHORT_SHORTONLY_HIGH_ADX_MIN:.0f} "
        f"-{short_pullback_removed} pullback "
        f"-{short_open_removed} open<{V17B_SHORT_OPEN_CUTOFF_MIN//60:02d}:{V17B_SHORT_OPEN_CUTOFF_MIN%60:02d} "
        f"-{short_avwap_dead_removed} AVWAP[{V17B_SHORT_AVWAP_DEAD_LO:.1f},{V17B_SHORT_AVWAP_DEAD_HI:.1f}) "
        f"-{short_both_adx_dead_removed} BOTH-ADX[{V17B_SHORT_BOTH_ADX_DEAD_LO:.0f},{V17B_SHORT_BOTH_ADX_DEAD_HI:.0f})) | "
        f"LONG: {long_before}->{len(long_df)} "
        f"(-{long_late_removed} late-{V17B_LONG_LATE_SETUP_NAME[:8]} "
        f"bars>={V17B_LONG_LATE_MIN_BARS_FROM_OPEN} vol>={V17B_LONG_LATE_MIN_VOL_RATIO:.1f}x "
        f"-{long_rsi_dead_removed} RSI[{V17B_LONG_RSI_DEAD_LO:.0f},{V17B_LONG_RSI_DEAD_HI:.0f}))"
    )
    return short_df, long_df


def _v17b_get_filter_reason(row: dict, side: str):
    reason = _v16_get_filter_reason(row, side)
    if reason is not None or not V17B_FILTER_BUNDLE_ENABLED:
        return reason

    side_u = str(side).upper().strip()
    if side_u == "SHORT":
        setup = str(row.get("setup", "")).upper().strip()
        if V17B_DISABLE_SHORT_PULLBACK_SETUP and setup == "A_PULLBACK_C2_THEN_BREAK_C2_LOW":
            return "v17b short cleanup: pullback setup disabled"

        if V17B_SHORT_BLOCK_SHORTONLY_RSI_ENABLED:
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            try:
                rsi = float(row.get("rsi_signal", float("nan")))
            except (TypeError, ValueError):
                rsi = float("nan")
            if (
                mode == "SHORT_ONLY"
                and np.isfinite(rsi)
                and V17B_SHORT_SHORTONLY_RSI_LO <= rsi < V17B_SHORT_SHORTONLY_RSI_HI
            ):
                return (
                    f"v17b short cleanup: SHORT_ONLY RSI={rsi:.1f} in "
                    f"[{V17B_SHORT_SHORTONLY_RSI_LO:.1f},{V17B_SHORT_SHORTONLY_RSI_HI:.1f})"
                )

        if V17B_SHORT_BLOCK_SHORTONLY_HIGH_ADX_ENABLED:
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            try:
                adx = float(row.get("adx_signal", float("nan")))
            except (TypeError, ValueError):
                adx = float("nan")
            if (
                mode == "SHORT_ONLY"
                and np.isfinite(adx)
                and adx >= V17B_SHORT_SHORTONLY_HIGH_ADX_MIN
            ):
                return (
                    f"v17b short cleanup: SHORT_ONLY ADX={adx:.1f} >= "
                    f"{V17B_SHORT_SHORTONLY_HIGH_ADX_MIN:.1f}"
                )

        # Run2: opening-bar dead window
        if V17B_SHORT_BLOCK_OPEN_WINDOW:
            try:
                et = pd.to_datetime(row.get("entry_time_ist", None))
                if et is not None:
                    et_min = et.hour * 60 + et.minute
                    if et_min < V17B_SHORT_OPEN_CUTOFF_MIN:
                        return (
                            f"v17b Run2: short open-window block "
                            f"({et.strftime('%H:%M')} < "
                            f"{V17B_SHORT_OPEN_CUTOFF_MIN//60:02d}:{V17B_SHORT_OPEN_CUTOFF_MIN%60:02d})"
                        )
            except Exception:
                pass

        # Run2: SHORT AVWAP dist dead zone
        if V17B_SHORT_AVWAP_DEAD_ENABLED:
            try:
                dist = float(row.get("avwap_dist_atr_signal", float("nan")))
            except (TypeError, ValueError):
                dist = float("nan")
            if (
                np.isfinite(dist)
                and V17B_SHORT_AVWAP_DEAD_LO <= dist < V17B_SHORT_AVWAP_DEAD_HI
            ):
                return (
                    f"v17b Run2: short AVWAP dist={dist:.2f} in "
                    f"[{V17B_SHORT_AVWAP_DEAD_LO:.1f},{V17B_SHORT_AVWAP_DEAD_HI:.1f}) dead zone"
                )

        # Run2: BOTH-mode ADX chop zone
        if V17B_SHORT_BOTH_ADX_DEAD_ENABLED:
            mode = str(row.get("nifty_context_mode", "")).upper().strip()
            try:
                adx = float(row.get("adx_signal", float("nan")))
            except (TypeError, ValueError):
                adx = float("nan")
            if (
                mode == "BOTH"
                and np.isfinite(adx)
                and V17B_SHORT_BOTH_ADX_DEAD_LO <= adx < V17B_SHORT_BOTH_ADX_DEAD_HI
            ):
                return (
                    f"v17b Run2: BOTH-mode ADX={adx:.1f} in chop zone "
                    f"[{V17B_SHORT_BOTH_ADX_DEAD_LO:.0f},{V17B_SHORT_BOTH_ADX_DEAD_HI:.0f})"
                )

        return None

    # LONG side filter reasons
    if V17B_LONG_RSI_DEAD_ENABLED:
        try:
            rsi = float(row.get("rsi_signal", float("nan")))
        except (TypeError, ValueError):
            rsi = float("nan")
        if np.isfinite(rsi) and V17B_LONG_RSI_DEAD_LO <= rsi < V17B_LONG_RSI_DEAD_HI:
            return (
                f"v17b Run2: LONG RSI={rsi:.1f} in breakout-trap zone "
                f"[{V17B_LONG_RSI_DEAD_LO:.0f},{V17B_LONG_RSI_DEAD_HI:.0f})"
            )

    if V17B_LONG_LATE_ANTICHASE_ENABLED:
        setup = str(row.get("setup", "")).upper().strip()
        try:
            bfo = float(row.get("bars_from_open", float("nan")))
            vr = float(row.get("entry_bar_vol_ratio", float("nan")))
        except (TypeError, ValueError):
            bfo = float("nan")
            vr = float("nan")
        if (
            setup == V17B_LONG_LATE_SETUP_NAME
            and np.isfinite(bfo)
            and np.isfinite(vr)
            and bfo >= V17B_LONG_LATE_MIN_BARS_FROM_OPEN
            and vr >= V17B_LONG_LATE_MIN_VOL_RATIO
        ):
            return (
                f"v17b long anti-chase: setup={setup}, bars_from_open={int(bfo)}, "
                f"vol_ratio={vr:.2f}x"
            )

    return None


_base._apply_v16_post_scan_filters = _v17b_apply_post_scan_filters
_base.get_v16_filter_reason = _v17b_get_filter_reason


# ===========================================================================
# PATCH 3: disable breakeven and trailing stop to match live executor.
# The live executor places a fixed LIMIT target + SL-M stop only — no
# breakeven trigger and no trailing stop. Both StrategyConfig classes used
# by the V16 base runner are patched so every assignment of enable_breakeven
# or enable_trailing_stop (including from __init__) is forced to False.
# ===========================================================================
import avwap_v11_refactored.avwap_common_v11 as _common_v11
import avwap_v11_refactored.avwap_common_v7_sweep as _common_v7

_BE_TRAIL_FIELDS = frozenset({"enable_breakeven", "enable_trailing_stop"})


def _v17b_no_be_trail_setattr(self, name: str, value) -> None:
    object.__setattr__(self, name, False if name in _BE_TRAIL_FIELDS else value)


_common_v11.StrategyConfig.__setattr__ = _v17b_no_be_trail_setattr
_common_v7.StrategyConfig.__setattr__ = _v17b_no_be_trail_setattr

# Also cover apply_live_parity_profile (called from live detection wrappers).
_orig_apply_live_parity_profile = _base.apply_live_parity_profile


def _v17b_apply_live_parity_profile(short_cfg, long_cfg):
    short_cfg, long_cfg = _orig_apply_live_parity_profile(short_cfg, long_cfg)
    short_cfg.enable_breakeven = False
    short_cfg.enable_trailing_stop = False
    long_cfg.enable_breakeven = False
    long_cfg.enable_trailing_stop = False
    return short_cfg, long_cfg


_base.apply_live_parity_profile = _v17b_apply_live_parity_profile


# ===========================================================================
# PATCH 4: match V16 live Nifty confirm time (09:20, not 09:30 backtest default).
# Live detection engine sets NIFTY_CONTEXT_OR_END_TIME = NIFTY_CONTEXT_CONFIRM_TIME
# = 09:20 so context activates from the first tradeable slots.
# ===========================================================================
_base.NIFTY_CONTEXT_OR_END_TIME  = _base.dtime(9, 20)
_base.NIFTY_CONTEXT_CONFIRM_TIME = _base.dtime(9, 20)


if __name__ == "__main__":
    print("=" * 70)
    print("V17b 5-min runner: live-parity core + Run1 cleanup + Run2 data-driven bundle")
    print("  SHORT context : raw RS <= -0.75% BOTH / -0.20% directional (V16 live)")
    print("  LONG  context : raw RS >= +0.75% (V16 live)")
    print("  Exits         : LIMIT target + SL-M only (no BE, no trail)")
    print("  Nifty confirm : 09:20 (V16 live)")
    print("  Workers       : 16 (process executor)")
    print("--- Run1 cleanup filters ---")
    print(f"  SHORT: block SO-RSI[{V17B_SHORT_SHORTONLY_RSI_LO:.0f},{V17B_SHORT_SHORTONLY_RSI_HI:.0f})"
          f" SO-ADX>={V17B_SHORT_SHORTONLY_HIGH_ADX_MIN:.0f} A_PULLBACK setup")
    print(f"  LONG : block late {V17B_LONG_LATE_SETUP_NAME} "
          f"bars>={V17B_LONG_LATE_MIN_BARS_FROM_OPEN} vol>={V17B_LONG_LATE_MIN_VOL_RATIO:.1f}x")
    print("--- Run2 data-driven filters ---")
    print(f"  SHORT: block opening<{V17B_SHORT_OPEN_CUTOFF_MIN//60:02d}:{V17B_SHORT_OPEN_CUTOFF_MIN%60:02d}"
          f" | AVWAP dist [{V17B_SHORT_AVWAP_DEAD_LO:.1f},{V17B_SHORT_AVWAP_DEAD_HI:.1f}) dead"
          f" | BOTH ADX [{V17B_SHORT_BOTH_ADX_DEAD_LO:.0f},{V17B_SHORT_BOTH_ADX_DEAD_HI:.0f}) dead")
    print(f"  LONG : block RSI [{V17B_LONG_RSI_DEAD_LO:.0f},{V17B_LONG_RSI_DEAD_HI:.0f}) dead")
    print("--- Run2 base-constant overrides ---")
    print(f"  SHORT RSI dead zone LO : {_base.V16_SHORT_RSI_DEAD_ZONE_LO:.0f} (was 35)")
    print(f"  LONG QS abs-max        : {_base.V16_LONG_QS_ABS_MAX:.0f} (was 10)")
    print(f"  LONG QS dead zone      : [{_base.V16_LONG_QS_DEAD_LO:.2f},{_base.V16_LONG_QS_DEAD_HI:.2f}) (was [7.50,8.00))")
    print(f"  LONG vol-exhaust mult  : {_base.V16_LONG_ENTRY_VOL_EXHAUST_MULT:.1f}x (was 4.0x)")
    print(f"  Data dir: {V17B_DATA_5M_DIR}")
    print("=" * 70)
    _base.main()
