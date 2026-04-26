# -*- coding: utf-8 -*-
"""Quick target-sweep re-resolver for v17j.

For each trade in the v17j trade log, re-walks the actual 1-min bars from
entry through EOD and re-resolves the exit for a range of candidate targets,
keeping SL=0.75% fixed. Falls back to 5-min bars when 1-min is missing.

Output: a per-side, per-target metrics table
(trades / win% / target_hr% / PF / sumPnL% / avg/trade% / MaxDD% / dwin%).

Friction model (matches v16_5min canonical):
  net TARGET = 5*T - 0.80
  net SL     = -(5*SL + 0.80 + 0.15)  = -4.70 at SL=0.75%
  net EOD    = (exit_close/entry - 1)*100*5 - 0.80   (LONG)
              -(exit_close/entry - 1)*100*5 - 0.80  (SHORT)

EOD cutoff = 15:20 IST.
"""
from __future__ import annotations

import sys
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

TRADES_CSV = Path(r"C:/TradingData/eqidv2/outputs_v17j_5min/avwap_longshort_trades_v16_5min_ALL_DAYS_20260425_115645.csv")
DIR_1MIN = Path(r"C:/TradingData/eqidv2/stocks_indicators_1min_eq")
DIR_5MIN = Path(r"C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2")

SL_PCT = 0.75 / 100.0  # 0.75%
COST_CAPITAL = 0.80    # 80bps round-trip on capital at 5x leverage
STOP_EXTRA = 0.15      # extra stop slip
LEVERAGE = 5.0
EOD_HOUR = 15
EOD_MIN = 20  # 15:20 IST

# Target sweep ranges
LONG_TARGETS = [0.0050, 0.0060, 0.0065, 0.0070, 0.0075, 0.0080, 0.0085, 0.0090, 0.0095, 0.0100, 0.0105, 0.0110, 0.0120, 0.0130, 0.0150]
SHORT_TARGETS = [0.0050, 0.0060, 0.0065, 0.0070, 0.0075, 0.0080, 0.0085, 0.0090, 0.0095, 0.0100, 0.0105, 0.0110, 0.0120, 0.0130, 0.0150]


@lru_cache(maxsize=None)
def _load_bars(ticker: str, freq: str) -> pd.DataFrame:
    """Load bar parquet for a ticker. freq='1min' or '5min'."""
    if freq == "1min":
        path = DIR_1MIN / f"{ticker}_stocks_indicators_1min.parquet"
    else:
        path = DIR_5MIN / f"{ticker}_stocks_indicators_5min.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    return df


def get_intraday_slice(ticker: str, trade_date: str, entry_time_ist) -> pd.DataFrame:
    """Get bar slice from entry_time to EOD for this ticker on trade_date.

    Tries 1-min first, falls back to 5-min if 1-min is missing or empty.
    """
    entry_ts = pd.Timestamp(entry_time_ist)
    if entry_ts.tz is None:
        entry_ts = entry_ts.tz_localize("Asia/Kolkata")
    else:
        entry_ts = entry_ts.tz_convert("Asia/Kolkata")

    eod_ts = pd.Timestamp.combine(entry_ts.date(), pd.Timestamp(f"{EOD_HOUR:02d}:{EOD_MIN:02d}:00").time())
    eod_ts = eod_ts.tz_localize("Asia/Kolkata")

    # Try 1-min
    df1 = _load_bars(ticker, "1min")
    if not df1.empty:
        mask = (df1["date"] >= entry_ts) & (df1["date"] <= eod_ts)
        sl = df1.loc[mask]
        if not sl.empty:
            return sl

    # Fallback to 5-min
    df5 = _load_bars(ticker, "5min")
    if not df5.empty:
        mask = (df5["date"] >= entry_ts) & (df5["date"] <= eod_ts)
        return df5.loc[mask]

    return pd.DataFrame()


def resolve_exit(side: str, entry_price: float, target_pct: float, bars: pd.DataFrame) -> Tuple[str, float]:
    """Walk bars and resolve exit. Returns (outcome, exit_price).

    Pessimistic resolution: when both target and SL hit in same bar, SL wins.
    """
    if bars.empty:
        return ("EOD", entry_price)

    if side == "LONG":
        tgt_price = entry_price * (1.0 + target_pct)
        sl_price = entry_price * (1.0 - SL_PCT)
    else:  # SHORT
        tgt_price = entry_price * (1.0 - target_pct)
        sl_price = entry_price * (1.0 + SL_PCT)

    highs = bars["high"].to_numpy()
    lows = bars["low"].to_numpy()

    if side == "LONG":
        target_hit = highs >= tgt_price
        sl_hit = lows <= sl_price
    else:
        target_hit = lows <= tgt_price
        sl_hit = highs >= sl_price

    target_idx = np.argmax(target_hit) if target_hit.any() else len(bars)
    sl_idx = np.argmax(sl_hit) if sl_hit.any() else len(bars)

    if target_idx >= len(bars) and sl_idx >= len(bars):
        # No exit, EOD close
        return ("EOD", float(bars["close"].iloc[-1]))

    if sl_idx < target_idx:
        return ("SL", float(sl_price))
    elif target_idx < sl_idx:
        return ("TARGET", float(tgt_price))
    else:
        # Same bar — pessimistic: SL
        return ("SL", float(sl_price))


def pnl_pct_from_outcome(side: str, entry: float, exit: float, outcome: str, target_pct: float) -> float:
    """Compute net pnl_pct on capital matching v16 friction model."""
    if outcome == "TARGET":
        return LEVERAGE * (target_pct * 100) - COST_CAPITAL  # e.g. 5*0.80 - 0.80 = 3.20
    elif outcome == "SL":
        return -(LEVERAGE * (SL_PCT * 100) + COST_CAPITAL + STOP_EXTRA)  # -4.70
    else:  # EOD
        if side == "LONG":
            ret_price = (exit / entry - 1.0) * 100  # %
        else:
            ret_price = (entry / exit - 1.0) * 100
        return LEVERAGE * ret_price - COST_CAPITAL


def aggregate_metrics(pnls: pd.Series, dates: pd.Series) -> Dict[str, float]:
    n = len(pnls)
    if n == 0:
        return dict(n=0, win=0, pf=0, sumPnL=0, avg=0, dd=0, dwin=0)
    win = (pnls > 0).mean() * 100
    spnl = pnls.sum()
    apnl = pnls.mean()
    gw = pnls[pnls > 0].sum()
    gl = -pnls[pnls < 0].sum()
    pf = gw / gl if gl > 0 else float("inf")
    cum = pnls.cumsum()
    dd = (cum - cum.cummax()).min()
    dwins = pd.Series(pnls.values).groupby(dates.values).sum()
    dwin = (dwins > 0).mean() * 100
    return dict(n=n, win=win, pf=pf, sumPnL=spnl, avg=apnl, dd=dd, dwin=dwin)


def main():
    print(f"Loading trades: {TRADES_CSV.name}")
    df = pd.read_csv(TRADES_CSV, parse_dates=["entry_time_ist", "exit_time_ist"])
    print(f"  {len(df)} trades total ({(df.side=='LONG').sum()} LONG / {(df.side=='SHORT').sum()} SHORT)")

    # Pre-fetch bar slices to avoid recomputing per-target
    print("Pre-fetching intraday bar slices for all trades...")
    t0 = time.time()
    slices: Dict[int, pd.DataFrame] = {}
    miss_count = 0
    for idx, row in df.iterrows():
        bars = get_intraday_slice(row["ticker"], row["trade_date"], row["entry_time_ist"])
        if bars.empty:
            miss_count += 1
        slices[idx] = bars
        if (idx + 1) % 200 == 0:
            print(f"  fetched {idx+1}/{len(df)} (miss={miss_count}), elapsed={time.time()-t0:.0f}s")
    print(f"Done in {time.time()-t0:.0f}s. Bar-slice misses: {miss_count}/{len(df)}")

    # Sweep targets per side
    for side, target_grid in [("LONG", LONG_TARGETS), ("SHORT", SHORT_TARGETS)]:
        side_df = df[df["side"] == side]
        side_idx = side_df.index
        print(f"\n========== {side} target sweep (n={len(side_df)} trades) ==========")
        print(f"{'target':>7} {'win%':>6} {'tgt_hr%':>7} {'pf':>6} {'avg/tr':>7} {'sumPnL':>9} {'maxDD':>8} {'dwin%':>6}  {'TGT':>4} {'SL':>4} {'EOD':>4}")

        for T in target_grid:
            outcomes: List[str] = []
            pnls: List[float] = []
            for idx in side_idx:
                row = df.loc[idx]
                bars = slices[idx]
                outc, exit_p = resolve_exit(side, float(row["entry_price"]), T, bars)
                pnl = pnl_pct_from_outcome(side, float(row["entry_price"]), exit_p, outc, T)
                outcomes.append(outc)
                pnls.append(pnl)
            pnls_s = pd.Series(pnls)
            outc_s = pd.Series(outcomes)
            dates = side_df["trade_date"].reset_index(drop=True)
            m = aggregate_metrics(pnls_s, dates)
            tgt_hr = (outc_s == "TARGET").mean() * 100
            tgt_n = (outc_s == "TARGET").sum()
            sl_n = (outc_s == "SL").sum()
            eod_n = (outc_s == "EOD").sum()
            print(f"  {T*100:5.2f}% {m['win']:5.2f} {tgt_hr:6.2f} {m['pf']:5.3f} {m['avg']:6.3f}% {m['sumPnL']:8.1f}% {m['dd']:7.2f}% {m['dwin']:5.2f}%  {tgt_n:4d} {sl_n:4d} {eod_n:4d}")


if __name__ == "__main__":
    main()
