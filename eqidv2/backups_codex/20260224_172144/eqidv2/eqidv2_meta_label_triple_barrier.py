# -*- coding: utf-8 -*-
"""
eqidv2_meta_label_triple_barrier.py
==================================

Build a meta-label dataset from candidate trades using a triple-barrier label:

Label = 1  if TP hit before SL within N bars
Label = 0  otherwise (SL first or timeout)

This is designed to work with your AVWAP combined runner output (trades CSV),
but it can also work with any "candidate trades" CSV that has:
  - ticker
  - side ("LONG"/"SHORT")
  - entry_time_ist (or signal_datetime)
  - entry_price
  - sl_price (or stop_price)
  - target_price
and (optionally) signal diagnostics like:
  - quality_score
  - atr_pct_signal
  - rsi_signal
  - adx_signal

It looks up future 5-minute candles (high/low) from per-ticker parquet files
to decide which barrier was hit first.

Outputs a dataset CSV that you can use to train the meta-model.

Usage example
-------------
python eqidv2_meta_label_triple_barrier.py ^
  --trades-csv outputs/avwap_longshort_trades_ALL_DAYS_20260213_101530.csv ^
  --candles-dir stocks_indicators_5min_eq ^
  --out-csv eqidv2/datasets/meta_dataset.csv ^
  --horizon-bars 12

Notes
-----
- If TP and SL are both hit in the same bar, this script marks it as SL-first
  (conservative, avoids optimistic bias).
- Timezone: the script tries to parse timestamps robustly and treats them as IST
  if timezone info is missing.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import numpy as np

IST_TZ = "Asia/Kolkata"


def _to_dt(x) -> pd.Timestamp:
    """Robust timestamp parsing."""
    try:
        ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return pd.NaT
        # Localize naive timestamps to IST for consistency
        if ts.tzinfo is None:
            ts = ts.tz_localize(IST_TZ)
        else:
            ts = ts.tz_convert(IST_TZ)
        return ts
    except Exception:
        return pd.NaT


def _find_ticker_parquet(candles_dir: Path, ticker: str) -> Optional[Path]:
    """Find a parquet file for a ticker in a directory."""
    # common exact names
    candidates = [
        candles_dir / f"{ticker}.parquet",
        candles_dir / f"{ticker}_stocks_indicators_5min.parquet",
        candles_dir / f"{ticker}_stocks_indicators_5min_eq.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    # fallback: any parquet starting with ticker
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
        # fallback to default engine if needed
        df = pd.read_parquet(p)
    if df is None or df.empty:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    # normalize datetime column (case-insensitive)
    cols_lower = {c.lower(): c for c in df.columns}
    if "datetime" in cols_lower:
        dtc = cols_lower["datetime"]
        df["datetime"] = pd.to_datetime(df[dtc], errors="coerce")
        if dtc != "datetime":
            # avoid duplicate column if original was different case
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

    # localize/convert to IST
    try:
        if df["datetime"].dt.tz is None:
            df["datetime"] = df["datetime"].dt.tz_localize(IST_TZ)
        else:
            df["datetime"] = df["datetime"].dt.tz_convert(IST_TZ)
    except Exception:
        # if .dt fails due to NaT mix
        df["datetime"] = df["datetime"].apply(_to_dt)

    # standardize OHLC columns (case-insensitive -> lowercase)
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


def triple_barrier_label(
    candles: pd.DataFrame,
    entry_time: pd.Timestamp,
    side: str,
    entry_price: float,
    sl_price: float,
    target_price: float,
    horizon_bars: int,
    bar_minutes: int = 5,
) -> LabelResult:
    """
    Determine which barrier hit first in forward window.

    Conservative rule if both hit in same bar:
        -> treat as SL first (label=0), hit="BOTH_SAME_BAR"
    """
    side_u = str(side).upper()
    if side_u not in ("LONG", "SHORT"):
        side_u = "LONG"

    # derive TP/SL from provided prices
    tp = float(target_price) if target_price and not np.isnan(target_price) else np.nan
    sl = float(sl_price) if sl_price and not np.isnan(sl_price) else np.nan

    # if missing, can't label
    if not np.isfinite(tp) or not np.isfinite(sl) or not np.isfinite(entry_price):
        t1 = entry_time + timedelta(minutes=horizon_bars * bar_minutes)
        return LabelResult(label=0, hit="TIMEOUT", hit_time=None, t1=t1, tp=float(tp) if np.isfinite(tp) else 0.0, sl=float(sl) if np.isfinite(sl) else 0.0)

    t1 = entry_time + timedelta(minutes=horizon_bars * bar_minutes)

    # forward window candles strictly after entry_time, up to t1 inclusive
    fw = candles[(candles["datetime"] > entry_time) & (candles["datetime"] <= t1)]
    if fw.empty or ("high" not in fw.columns) or ("low" not in fw.columns):
        return LabelResult(label=0, hit="TIMEOUT", hit_time=None, t1=t1, tp=tp, sl=sl)

    # iterate in time order
    for _, row in fw.iterrows():
        hi = row.get("high", np.nan)
        lo = row.get("low", np.nan)
        dt = row.get("datetime", pd.NaT)

        if not np.isfinite(hi) or not np.isfinite(lo):
            continue

        if side_u == "LONG":
            hit_tp = hi >= tp
            hit_sl = lo <= sl
        else:  # SHORT
            hit_tp = lo <= tp
            hit_sl = hi >= sl

        if hit_tp and hit_sl:
            # conservative: SL first
            return LabelResult(label=0, hit="BOTH_SAME_BAR", hit_time=dt, t1=t1, tp=tp, sl=sl)
        if hit_sl:
            return LabelResult(label=0, hit="SL", hit_time=dt, t1=t1, tp=tp, sl=sl)
        if hit_tp:
            return LabelResult(label=1, hit="TP", hit_time=dt, t1=t1, tp=tp, sl=sl)

    return LabelResult(label=0, hit="TIMEOUT", hit_time=None, t1=t1, tp=tp, sl=sl)


def _pick_col(df: pd.DataFrame, *names: str) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    return None


def build_dataset(
    trades_csv: Path,
    candles_dir: Path,
    out_csv: Path,
    horizon_bars: int,
    bar_minutes: int,
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

    # optional diagnostics
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
        )

        # unify basic features to match ml_meta_filter expectations
        quality_score = float(r[q_col]) if q_col and pd.notna(r[q_col]) else 0.0
        atr_pct = float(r[atrp_col]) if atrp_col and pd.notna(r[atrp_col]) else 0.0
        rsi = float(r[rsi_col]) if rsi_col and pd.notna(r[rsi_col]) else 50.0
        adx = float(r[adx_col]) if adx_col and pd.notna(r[adx_col]) else 20.0

        out_rows.append({
            "ticker": ticker,
            "side": side,
            "entry_time": entry_time.isoformat(),
            "t1": lab.t1.isoformat(),
            "entry_price": float(entry_price) if np.isfinite(entry_price) else 0.0,
            "sl_price": float(sl_price) if np.isfinite(sl_price) else 0.0,
            "target_price": float(tp_price) if np.isfinite(tp_price) else 0.0,
            "label": int(lab.label),
            "hit": lab.hit,
            "hit_time": lab.hit_time.isoformat() if isinstance(lab.hit_time, pd.Timestamp) and not pd.isna(lab.hit_time) else "",
            "quality_score": quality_score,
            "atr_pct": atr_pct,
            "rsi": rsi,
            "adx": adx,
        })

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
        "hits": out_df["hit"].value_counts(dropna=False).to_dict() if not out_df.empty else {},
    }
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades-csv", required=True, help="CSV produced by avwap_combined_runner or other candidate-trade generator")
    ap.add_argument("--candles-dir", required=True, help="Directory containing per-ticker 5m parquet files")
    ap.add_argument("--out-csv", default="eqidv2/datasets/meta_dataset.csv", help="Output dataset CSV")
    ap.add_argument("--horizon-bars", type=int, default=12, help="N bars forward window (5m bars by default)")
    ap.add_argument("--bar-minutes", type=int, default=5, help="Bar size in minutes (5 for 5m candles)")
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
        max_rows=int(args.max_rows) if int(args.max_rows) > 0 else None,
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
