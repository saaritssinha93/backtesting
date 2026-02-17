# -*- coding: utf-8 -*-
"""
eqidv3_meta_label_triple_barrier.py  (Strategy v1)
====================================================

Build a meta-label dataset from candidate trades using a triple-barrier label
with R_net based labeling and expanded feature extraction.

Strategy v1 changes:
- Label = 1 if R_net >= +0.05R (net-positive after costs), else 0
  where R_net = pnl_net / SL_distance
- Expanded 30-feature set from candle data (no leakage)
- Slippage/fees baked into label computation
- Default horizon N=6 bars (30 min on 5-min TF)
- TP = 0.9*ATR, SL = 0.6*ATR from signal bar

Usage
-----
python eqidv3_meta_label_triple_barrier.py \\
  --trades-csv outputs/avwap_longshort_trades_ALL_DAYS_20260216_115227.csv \\
  --candles-dir stocks_indicators_5min_eq \\
  --out-csv meta_dataset.csv \\
  --horizon-bars 6 \\
  --bar-minutes 5

Notes
-----
- Conservative: if TP and SL both hit in same bar, label = 0 (SL first).
- Features use ONLY completed candles up to signal bar (no future leakage).
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import numpy as np

from ml_meta_filter import ALL_FEATURES, build_features_from_candles

IST_TZ = "Asia/Kolkata"

# Default slippage + commission (bps per side)
DEFAULT_SLIPPAGE_BPS = 3.0
DEFAULT_COMMISSION_BPS = 2.0
DEFAULT_R_NET_THRESHOLD = 0.05  # label y=1 if R_net >= this


def _to_dt(x) -> pd.Timestamp:
    """Robust timestamp parsing."""
    try:
        ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return pd.NaT
        if ts.tzinfo is None:
            ts = ts.tz_localize(IST_TZ)
        else:
            ts = ts.tz_convert(IST_TZ)
        return ts
    except Exception:
        return pd.NaT


def _find_ticker_parquet(candles_dir: Path, ticker: str) -> Optional[Path]:
    """Find a parquet file for a ticker in a directory."""
    candidates = [
        candles_dir / f"{ticker}.parquet",
        candles_dir / f"{ticker}_stocks_indicators_5min.parquet",
        candles_dir / f"{ticker}_stocks_indicators_5min_eq.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    hits = list(candles_dir.glob(f"{ticker}*.parquet"))
    if hits:
        return hits[0]
    return None


def _read_candles_cached(cache: Dict[str, pd.DataFrame], candles_dir: Path, ticker: str) -> pd.DataFrame:
    if ticker in cache:
        return cache[ticker]
    p = _find_ticker_parquet(candles_dir, ticker)
    if p is None:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]
    try:
        df = pd.read_parquet(p, engine="pyarrow")
    except Exception:
        df = pd.read_parquet(p)
    if df is None or df.empty:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    # normalize datetime column
    cols_lower = {c.lower(): c for c in df.columns}
    if "datetime" in cols_lower:
        dtc = cols_lower["datetime"]
        df["datetime"] = pd.to_datetime(df[dtc], errors="coerce")
        if dtc != "datetime":
            df = df.drop(columns=[dtc], errors="ignore")
    elif "date" in cols_lower:
        dc = cols_lower["date"]
        df["datetime"] = pd.to_datetime(df[dc], errors="coerce")
        df = df.drop(columns=[dc], errors="ignore")
    elif isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index().rename(columns={"index": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    else:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    # localize to IST
    try:
        if df["datetime"].dt.tz is None:
            df["datetime"] = df["datetime"].dt.tz_localize(IST_TZ)
        else:
            df["datetime"] = df["datetime"].dt.tz_convert(IST_TZ)
    except Exception:
        df["datetime"] = df["datetime"].apply(_to_dt)

    # Rename 'datetime' -> 'date' for feature builder compatibility
    df["date"] = df["datetime"]

    # standardize OHLCV columns
    cols_lower = {c.lower(): c for c in df.columns}
    for want in ["open", "high", "low", "close", "volume"]:
        if want in df.columns:
            continue
        if want in cols_lower and cols_lower[want] != want:
            df.rename(columns={cols_lower[want]: want}, inplace=True)

    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["datetime"]).sort_values("datetime")

    cache[ticker] = df
    return df


@dataclass
class LabelResult:
    label: int
    hit: str                 # "TP", "SL", "TIMEOUT", "BOTH_SAME_BAR"
    hit_time: Optional[pd.Timestamp]
    t1: pd.Timestamp         # event end time (entry + horizon)
    tp: float
    sl: float
    pnl_gross: float         # gross P&L (price move)
    pnl_net: float           # net P&L after slippage+fees
    r_net: float             # R_net = pnl_net / SL_distance
    exit_price: float


def triple_barrier_label(
    candles: pd.DataFrame,
    entry_time: pd.Timestamp,
    side: str,
    entry_price: float,
    sl_price: float,
    target_price: float,
    horizon_bars: int,
    bar_minutes: int = 5,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    commission_bps: float = DEFAULT_COMMISSION_BPS,
    r_net_threshold: float = DEFAULT_R_NET_THRESHOLD,
) -> LabelResult:
    """
    Triple barrier with R_net labeling (Strategy v1 Section 5+8).

    Label = 1 if R_net >= r_net_threshold, else 0.
    R_net = pnl_net / SL_distance (in R units).

    Conservative: if both TP and SL hit in same bar -> label=0.
    """
    side_u = str(side).upper()
    if side_u not in ("LONG", "SHORT"):
        side_u = "SHORT"

    tp = float(target_price) if target_price and not np.isnan(target_price) else np.nan
    sl = float(sl_price) if sl_price and not np.isnan(sl_price) else np.nan

    t1 = entry_time + timedelta(minutes=horizon_bars * bar_minutes)

    # Compute SL distance
    if np.isfinite(sl) and np.isfinite(entry_price):
        sl_distance = abs(entry_price - sl)
    else:
        sl_distance = 0.0

    # Default result for missing data
    def _make_result(label: int, hit: str, hit_time, exit_price: float) -> LabelResult:
        # Gross P&L
        if side_u == "SHORT":
            pnl_gross = (entry_price - exit_price) / entry_price * 100.0 if entry_price > 0 else 0.0
        else:
            pnl_gross = (exit_price - entry_price) / entry_price * 100.0 if entry_price > 0 else 0.0

        # Net P&L after costs (round-trip)
        cost_pct = (slippage_bps + commission_bps) * 2.0 / 10000.0 * 100.0
        pnl_net = pnl_gross - cost_pct

        # R_net
        if sl_distance > 0:
            if side_u == "SHORT":
                pnl_abs = entry_price - exit_price
            else:
                pnl_abs = exit_price - entry_price
            cost_abs = entry_price * (slippage_bps + commission_bps) * 2.0 / 10000.0
            r_net = (pnl_abs - cost_abs) / sl_distance
        else:
            r_net = 0.0

        return LabelResult(
            label=label, hit=hit, hit_time=hit_time, t1=t1,
            tp=tp if np.isfinite(tp) else 0.0,
            sl=sl if np.isfinite(sl) else 0.0,
            pnl_gross=pnl_gross, pnl_net=pnl_net, r_net=r_net,
            exit_price=exit_price,
        )

    if not np.isfinite(tp) or not np.isfinite(sl) or not np.isfinite(entry_price):
        return _make_result(0, "TIMEOUT", None, entry_price)

    # Forward window candles strictly after entry_time, up to t1
    fw = candles[(candles["datetime"] > entry_time) & (candles["datetime"] <= t1)]
    if fw.empty or ("high" not in fw.columns) or ("low" not in fw.columns):
        return _make_result(0, "TIMEOUT", None, entry_price)

    for _, row in fw.iterrows():
        hi = row.get("high", np.nan)
        lo = row.get("low", np.nan)
        cl = row.get("close", np.nan)
        dt = row.get("datetime", pd.NaT)

        if not np.isfinite(hi) or not np.isfinite(lo):
            continue

        if side_u == "LONG":
            hit_tp = hi >= tp
            hit_sl = lo <= sl
        else:
            hit_tp = lo <= tp
            hit_sl = hi >= sl

        if hit_tp and hit_sl:
            return _make_result(0, "BOTH_SAME_BAR", dt, sl)  # conservative: SL
        if hit_sl:
            return _make_result(0, "SL", dt, sl)
        if hit_tp:
            result = _make_result(1, "TP", dt, tp)
            # Re-check: label depends on R_net threshold
            result.label = 1 if result.r_net >= r_net_threshold else 0
            return result

    # Timeout: exit at last bar close
    last_close = float(fw.iloc[-1].get("close", entry_price))
    if not np.isfinite(last_close):
        last_close = entry_price
    result = _make_result(0, "TIMEOUT", None, last_close)
    # Even timeout can be labeled 1 if R_net is positive enough
    result.label = 1 if result.r_net >= r_net_threshold else 0
    return result


def _pick_col(df: pd.DataFrame, *names: str) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    return None


def _find_signal_bar_idx(candles: pd.DataFrame, entry_time: pd.Timestamp) -> int:
    """Find the candle index at or just before entry_time (signal bar)."""
    if candles.empty:
        return -1
    mask = candles["datetime"] <= entry_time
    if not mask.any():
        return -1
    return int(mask.values.nonzero()[0][-1])


def build_dataset(
    trades_csv: Path,
    candles_dir: Path,
    out_csv: Path,
    horizon_bars: int,
    bar_minutes: int,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    commission_bps: float = DEFAULT_COMMISSION_BPS,
    r_net_threshold: float = DEFAULT_R_NET_THRESHOLD,
    max_rows: Optional[int] = None,
) -> dict:
    df = pd.read_csv(trades_csv)
    if df.empty:
        raise ValueError("No rows in trades CSV")

    # map essential columns
    ticker_col = _pick_col(df, "ticker", "symbol")
    side_col = _pick_col(df, "side")
    entry_time_col = _pick_col(df, "entry_time_ist", "entry_time", "signal_datetime", "signal_time_ist")
    entry_price_col = _pick_col(df, "entry_price")
    sl_col = _pick_col(df, "sl_price", "stop_price")
    tp_col = _pick_col(df, "target_price")

    missing = [("ticker", ticker_col), ("side", side_col), ("entry_time", entry_time_col),
               ("entry_price", entry_price_col), ("sl_price", sl_col), ("target_price", tp_col)]
    miss_names = [n for n, c in missing if c is None]
    if miss_names:
        raise ValueError(f"Trades CSV missing required columns: {miss_names}. Found columns: {list(df.columns)}")

    # optional legacy diagnostics (kept for backward compat)
    q_col = _pick_col(df, "quality_score")
    atrp_col = _pick_col(df, "atr_pct_signal", "atr_pct")
    rsi_col = _pick_col(df, "rsi_signal", "rsi")
    adx_col = _pick_col(df, "adx_signal", "adx")

    # parse times
    df["_entry_time"] = df[entry_time_col].apply(_to_dt)
    df = df.dropna(subset=["_entry_time"])
    df = df.sort_values("_entry_time")
    if max_rows:
        df = df.head(max_rows)

    cache: Dict[str, pd.DataFrame] = {}
    out_rows = []
    dropped_no_candles = 0

    for _, r in df.iterrows():
        ticker = str(r[ticker_col]).strip()
        candles = _read_candles_cached(cache, candles_dir, ticker)
        if candles.empty:
            dropped_no_candles += 1
            continue

        entry_time = r["_entry_time"]
        side = str(r[side_col]).upper().strip()
        entry_price = float(r[entry_price_col]) if pd.notna(r[entry_price_col]) else np.nan
        sl_price = float(r[sl_col]) if pd.notna(r[sl_col]) else np.nan
        tp_price = float(r[tp_col]) if pd.notna(r[tp_col]) else np.nan

        lab = triple_barrier_label(
            candles=candles,
            entry_time=entry_time,
            side=side,
            entry_price=entry_price,
            sl_price=sl_price,
            target_price=tp_price,
            horizon_bars=horizon_bars,
            bar_minutes=bar_minutes,
            slippage_bps=slippage_bps,
            commission_bps=commission_bps,
            r_net_threshold=r_net_threshold,
        )

        # --- Build v1 expanded features from candle data ---
        signal_idx = _find_signal_bar_idx(candles, entry_time)
        if signal_idx >= 3:
            feats = build_features_from_candles(candles, signal_idx, side)
        else:
            feats = {f: 0.0 for f in ALL_FEATURES}

        # Legacy features (backward compat)
        quality_score = float(r[q_col]) if q_col and pd.notna(r[q_col]) else 0.0
        atr_pct = float(r[atrp_col]) if atrp_col and pd.notna(r[atrp_col]) else 0.0
        rsi = float(r[rsi_col]) if rsi_col and pd.notna(r[rsi_col]) else 50.0
        adx = float(r[adx_col]) if adx_col and pd.notna(r[adx_col]) else 20.0

        row_out = {
            "ticker": ticker,
            "side": side,
            "entry_time": entry_time.isoformat(),
            "t1": lab.t1.isoformat(),
            "entry_price": float(entry_price) if np.isfinite(entry_price) else 0.0,
            "sl_price": float(sl_price) if np.isfinite(sl_price) else 0.0,
            "target_price": float(tp_price) if np.isfinite(tp_price) else 0.0,
            "exit_price": lab.exit_price,
            "label": int(lab.label),
            "hit": lab.hit,
            "hit_time": lab.hit_time.isoformat() if isinstance(lab.hit_time, pd.Timestamp) and not pd.isna(lab.hit_time) else "",
            "pnl_gross": lab.pnl_gross,
            "pnl_net": lab.pnl_net,
            "r_net": lab.r_net,
            # Legacy features (backward compat)
            "quality_score": quality_score,
            "atr_pct": atr_pct,
            "rsi": rsi,
            "adx": adx,
        }
        # Add all v1 expanded features
        row_out.update(feats)

        out_rows.append(row_out)

    out_df = pd.DataFrame(out_rows)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False)

    report = {
        "trades_csv": str(trades_csv),
        "candles_dir": str(candles_dir),
        "out_csv": str(out_csv),
        "rows_in": int(len(df)),
        "rows_out": int(len(out_df)),
        "dropped_no_candles": int(dropped_no_candles),
        "label_pos_rate": float(out_df["label"].mean()) if not out_df.empty else 0.0,
        "horizon_bars": int(horizon_bars),
        "bar_minutes": int(bar_minutes),
        "slippage_bps": slippage_bps,
        "commission_bps": commission_bps,
        "r_net_threshold": r_net_threshold,
        "hits": out_df["hit"].value_counts(dropna=False).to_dict() if not out_df.empty else {},
        "avg_r_net": float(out_df["r_net"].mean()) if not out_df.empty else 0.0,
        "features_count": len(ALL_FEATURES),
    }
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades-csv", required=True, help="CSV produced by avwap_combined_runner")
    ap.add_argument("--candles-dir", required=True, help="Directory containing per-ticker 5m parquet files")
    ap.add_argument("--out-csv", default="eqidv3/datasets/meta_dataset.csv", help="Output dataset CSV")
    ap.add_argument("--horizon-bars", type=int, default=6, help="N bars forward window (default 6 = 30min on 5m)")
    ap.add_argument("--bar-minutes", type=int, default=5, help="Bar size in minutes")
    ap.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS, help="Slippage per side in bps")
    ap.add_argument("--commission-bps", type=float, default=DEFAULT_COMMISSION_BPS, help="Commission per side in bps")
    ap.add_argument("--r-net-threshold", type=float, default=DEFAULT_R_NET_THRESHOLD, help="R_net threshold for label=1")
    ap.add_argument("--max-rows", type=int, default=0, help="Optional cap rows for quick tests")
    args = ap.parse_args()

    trades_csv = Path(args.trades_csv)
    candles_dir = Path(args.candles_dir)
    out_csv = Path(args.out_csv)

    report = build_dataset(
        trades_csv=trades_csv,
        candles_dir=candles_dir,
        out_csv=out_csv,
        horizon_bars=int(args.horizon_bars),
        bar_minutes=int(args.bar_minutes),
        slippage_bps=float(args.slippage_bps),
        commission_bps=float(args.commission_bps),
        r_net_threshold=float(args.r_net_threshold),
        max_rows=int(args.max_rows) if int(args.max_rows) > 0 else None,
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
