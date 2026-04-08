# -*- coding: utf-8 -*-
"""
V18 5-min combined runner — Composite Breadth Score filter.

V16 baseline : stock_ret - nifty_ret >= 0.75%  (single metric, fixed threshold)
V18 change   : 3 independent signals, keep if score >= 2

  Signal 1 — Price RS        : (stock_ret - nifty_ret) >= 0.30%   (lower bar, one of 3)
  Signal 2 — Volume confirm  : entry_bar_vol / day_avg_vol >= 1.20x
  Signal 3 — OR quality      : OR close_pos >= 0.60 (LONG) / <= 0.40 (SHORT)
                                = bar closed in top/bottom 40% of opening range

Why composite is more robust:
  • A stock with weak RS but huge volume breakout + strong OR close is worth taking
  • A stock with RS but low volume + weak OR bar is likely false breakout — rejected
  • On flat-NIFTY days, any 2 of 3 signals fire → participates in genuine movers
  • No single metric can be gamed; must show confluence across price/volume/structure

All other logic (signal windows, QS filters, vol exhaust, etc.) identical to v16 Run14.
Outputs go to outputs_v18_5min/ (separate from v16/v17).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple

import avwap_combined_runner_v16_5min as _base

# ===========================================================================
# V18 CONFIG — Composite breadth score
# ===========================================================================
V18_COMPOSITE_ENABLED      = True
V18_COMPOSITE_MIN_SCORE    = 2      # need at least 2 of 3 signals

# Signal 1: price RS (lower bar than v16's 0.75% since it's 1-of-3)
V18_RS_THRESH_BOTH         = 0.30   # raw RS >= 0.30% in BOTH mode
V18_RS_THRESH_DIRECTIONAL  = 0.20   # raw RS >= 0.20% in directional mode (already strong trend)

# Signal 2: entry bar volume confirmation
V18_VOL_CONFIRM_RATIO      = 1.20   # entry bar vol >= 1.2x day average

# Signal 3: opening range bar quality
V18_OR_CLOSE_POS_LONG      = 0.55   # OR closed in top 45% of its range = bullish structure (LONG)
V18_OR_CLOSE_POS_SHORT     = 0.45   # OR closed in bottom 45% of its range = bearish structure (SHORT)

# ===========================================================================
# PATCH 1: redirect all outputs from v16_5min -> v18_5min
# ===========================================================================
_orig_runtime_dir = _base.runtime_dir

def _v18_runtime_dir(*parts):
    new_parts = tuple(str(p).replace("v16_5min", "v18_5min") for p in parts)
    return _orig_runtime_dir(*new_parts)

_base.runtime_dir = _v18_runtime_dir

# ===========================================================================
# 5-MIN DAY DATA CACHE
# Loads full day parquet data for a (ticker, date) pair.
# Used to compute OR close position + entry bar volume inline.
# ===========================================================================
_v18_day_cache: Dict[Tuple[str, str], pd.DataFrame] = {}


def _get_day_bars(ticker: str, date_str: str, cfg) -> pd.DataFrame:
    """Return all 5-min bars for (ticker, date_str). Cached."""
    key = (ticker, date_str)
    if key in _v18_day_cache:
        return _v18_day_cache[key]

    p = Path(cfg.dir_15m) / f"{ticker}{cfg.end_15m}"
    if not p.exists():
        _v18_day_cache[key] = pd.DataFrame()
        return _v18_day_cache[key]
    try:
        df = _base.read_15m_parquet(str(p), cfg.parquet_engine)
        if df.empty:
            _v18_day_cache[key] = pd.DataFrame()
            return _v18_day_cache[key]
        df = df.sort_values("date").reset_index(drop=True)
        dt = pd.to_datetime(df["date"], errors="coerce")
        df = df.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert(_base.IST)
        df["date"] = dt.dt.tz_convert(_base.IST)
        df["_date_str"] = df["date"].dt.strftime("%Y-%m-%d")
        day_df = df[df["_date_str"] == date_str].reset_index(drop=True)
        _v18_day_cache[key] = day_df
        return day_df
    except Exception:
        _v18_day_cache[key] = pd.DataFrame()
        return _v18_day_cache[key]


def _compute_or_close_pos(day_df: pd.DataFrame) -> float:
    """Return OR close position = (close - low) / (high - low) of first bar."""
    if day_df.empty:
        return np.nan
    bar = day_df.iloc[0]
    try:
        o, h, l, c = float(bar["open"]), float(bar["high"]), float(bar["low"]), float(bar["close"])
        rng = h - l
        return (c - l) / rng if rng > 0 else 0.5
    except Exception:
        return np.nan


def _compute_entry_vol_ratio(day_df: pd.DataFrame, entry_px: float) -> float:
    """Return entry_bar_vol / day_avg_vol. Entry bar = first bar where high >= entry_px*0.999."""
    if day_df.empty or entry_px <= 0:
        return np.nan
    try:
        avg_vol = float(day_df["volume"].mean())
        if avg_vol <= 0:
            return np.nan
        hits = day_df[day_df["high"] >= entry_px * 0.999]
        if hits.empty:
            return np.nan
        entry_vol = float(hits.iloc[0]["volume"])
        return entry_vol / avg_vol
    except Exception:
        return np.nan


# ===========================================================================
# PATCH 2: replace RS filter with composite breadth scoring
# ===========================================================================
def _v18_apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    V18: Composite Breadth Score filter.
    Keep trade if score >= V18_COMPOSITE_MIN_SCORE (2 of 3):
      +1 if price RS >= threshold
      +1 if entry bar vol >= 1.2x day avg
      +1 if OR close position is bullish/bearish (above/below midpoint)
    """
    if not mode_map:
        return short_df, long_df

    stock_ret_cache: Dict = {}

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

        # Hard directional mode filter (same as v16)
        before = len(d)
        if side.upper() == "SHORT":
            d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
        else:
            d = d[d["nifty_context_mode"].ne("SHORT_ONLY")].copy()
        mode_removed = before - len(d)

        if not _base.NIFTY_RS_FILTER_ENABLED or d.empty or not V18_COMPOSITE_ENABLED:
            if "ts_key_local" in d.columns:
                d = d.drop(columns=["ts_key_local"])
            return d, mode_removed, 0

        keep_mask  = []
        rel_vals   = []
        score_log  = []
        rs_removed = 0
        side_u = side.upper()

        for row in d.itertuples(index=False):
            ts_key     = getattr(row, "ts_key_local")
            mode       = getattr(row, "nifty_context_mode", "BOTH")
            ticker     = str(getattr(row, "ticker", ""))
            rel_val    = np.nan
            score      = 0

            # Determine RS threshold based on mode
            rs_thresh = (
                V18_RS_THRESH_DIRECTIONAL if mode != "BOTH" else V18_RS_THRESH_BOTH
            )

            # ── Signal 1: Price RS ───────────────────────────────────────
            stock_ret_map = _base._build_stock_return_map(ticker, cfg, stock_ret_cache)
            stock_ret = stock_ret_map.get(ts_key, np.nan)
            nifty_ret = nifty_ret_map.get(ts_key, np.nan)
            rs_signal = False
            if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                raw_rs  = float(stock_ret - nifty_ret)
                rel_val = raw_rs
                if side_u == "LONG":
                    rs_signal = raw_rs >= rs_thresh
                else:
                    rs_signal = raw_rs <= -rs_thresh
            if rs_signal:
                score += 1

            # ── Signal 2 + 3: need day bars ─────────────────────────────
            try:
                date_str = str(getattr(row, "trade_date", ""))[:10]
                entry_px = float(getattr(row, "entry_price", 0))
            except Exception:
                date_str = ""
                entry_px = 0.0

            day_df = _get_day_bars(ticker, date_str, cfg)

            # Signal 2: Volume confirmation
            vol_ratio  = _compute_entry_vol_ratio(day_df, entry_px)
            vol_signal = np.isfinite(vol_ratio) and vol_ratio >= V18_VOL_CONFIRM_RATIO
            if vol_signal:
                score += 1

            # Signal 3: OR close position
            or_pos     = _compute_or_close_pos(day_df)
            or_signal  = False
            if np.isfinite(or_pos):
                if side_u == "LONG":
                    or_signal = or_pos >= V18_OR_CLOSE_POS_LONG
                else:
                    or_signal = or_pos <= V18_OR_CLOSE_POS_SHORT
            if or_signal:
                score += 1

            keep = score >= V18_COMPOSITE_MIN_SCORE
            keep_mask.append(bool(keep))
            rel_vals.append(rel_val)
            score_log.append(score)
            if not keep:
                rs_removed += 1

        d["nifty_rel_strength_pct"] = rel_vals
        d["v18_breadth_score"]      = score_log
        d = d[pd.Series(keep_mask, index=d.index)].copy()
        if "ts_key_local" in d.columns:
            d = d.drop(columns=["ts_key_local"])
        return d, mode_removed, rs_removed

    s, s_mode_removed, s_rs_removed = _apply_side(short_df, "SHORT")
    l, l_mode_removed, l_rs_removed = _apply_side(long_df, "LONG")

    print(
        "[NIFTY_CONTEXT] Applied intraday filter (V18 Composite Breadth): "
        f"SHORT {len(short_df)}->{len(s)} "
        f"(mode_removed={s_mode_removed}, rs_removed={s_rs_removed}) | "
        f"LONG {len(long_df)}->{len(l)} "
        f"(mode_removed={l_mode_removed}, rs_removed={l_rs_removed}) "
        f"[min_score={V18_COMPOSITE_MIN_SCORE}/3 | "
        f"RS>={V18_RS_THRESH_BOTH:.2f}% | vol>={V18_VOL_CONFIRM_RATIO:.1f}x | "
        f"OR_pos LONG>={V18_OR_CLOSE_POS_LONG:.2f}/SHORT<={V18_OR_CLOSE_POS_SHORT:.2f}]"
    )
    return s, l


# Apply patches
_base._apply_nifty_intraday_context = _v18_apply_nifty_intraday_context


if __name__ == "__main__":
    print("=" * 70)
    print("V18 5-min runner: Composite Breadth Score filter (2-of-3)")
    print(f"  Signal 1 — Price RS    : stock_rs >= {V18_RS_THRESH_BOTH:.2f}% (BOTH mode)")
    print(f"  Signal 2 — Volume      : entry_bar_vol >= {V18_VOL_CONFIRM_RATIO:.1f}x day avg")
    print(f"  Signal 3 — OR quality  : OR close_pos >= {V18_OR_CLOSE_POS_LONG:.2f} (LONG) / <= {V18_OR_CLOSE_POS_SHORT:.2f} (SHORT)")
    print(f"  Keep if score >= {V18_COMPOSITE_MIN_SCORE}/3")
    print("  All other settings identical to v16 Run14")
    print("=" * 70)
    _base.main()
