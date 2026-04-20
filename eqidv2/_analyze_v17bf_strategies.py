"""Deep strategy analysis for v17b and v17f: per-setup, per-mode, per-hour edge.

Reports PF, win%, sum PnL, avg PnL for every meaningful slice.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 80)
pd.set_option("display.max_rows", 200)


CSV_PATHS = {
    "v17b": Path(r"C:\TradingData\eqidv2\outputs_v17b_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_202805.csv"),
    "v17f": Path(r"C:\TradingData\eqidv2\outputs_v17f_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260419_125208.csv"),
}


def _pf(p):
    g = float(p[p > 0].sum())
    l = float(-p[p < 0].sum())
    return g / l if l > 0 else float("inf")


def _summ(df, label):
    n = len(df)
    if n == 0:
        return None
    wins = int((df["outcome"].str.upper() == "TARGET").sum())
    sls = int((df["outcome"].str.upper() == "SL").sum())
    eods = int((df["outcome"].str.upper() == "EOD").sum())
    return dict(
        label=label, n=n, win=wins, sl=sls, eod=eods,
        win_pct=100.0 * wins / n,
        pf=_pf(df["pnl_pct"]),
        sum_pnl=float(df["pnl_pct"].sum()),
        avg_pnl=float(df["pnl_pct"].mean()),
        avg_win=float(df.loc[df.pnl_pct > 0, "pnl_pct"].mean()) if wins else 0.0,
        avg_loss=float(df.loc[df.pnl_pct < 0, "pnl_pct"].mean()) if sls else 0.0,
    )


def _sliced(df, series, title):
    rows = []
    for v, sub in df.groupby(series, dropna=False, observed=True):
        s = _summ(sub, str(v))
        if s:
            rows.append(s)
    out = pd.DataFrame(rows)
    if out.empty:
        return
    out = out.sort_values(["n"], ascending=False)
    print(f"\n-- {title} --")
    print(out.to_string(index=False, float_format=lambda x: f"{x:.2f}"))


def analyze(label: str, csv: Path):
    print("\n" + "=" * 90)
    print(f"   STRATEGY ANALYSIS — {label.upper()}   ({csv.name})")
    print("=" * 90)
    df = pd.read_csv(csv)
    print(f"Total rows: {len(df)} | columns: {len(df.columns)}")

    for side in ("LONG", "SHORT"):
        sub = df[df["side"].str.upper() == side].copy()
        if sub.empty:
            continue
        print(f"\n---- {side} overview ({len(sub)} trades) ----")
        s = _summ(sub, f"{label}_{side}_ALL")
        print(pd.Series(s).to_string())

        # Per-setup
        if "setup" in sub.columns:
            _sliced(sub, sub["setup"].astype(str), f"{side} by setup")

        # Per impulse
        if "impulse_type" in sub.columns:
            _sliced(sub, sub["impulse_type"].astype(str), f"{side} by impulse_type")

        # Per mode
        if "nifty_context_mode" in sub.columns:
            _sliced(sub, sub["nifty_context_mode"].astype(str), f"{side} by nifty_context_mode")

        # Setup x mode
        if {"setup", "nifty_context_mode"}.issubset(sub.columns):
            combo = sub["nifty_context_mode"].astype(str) + "|" + sub["setup"].astype(str)
            _sliced(sub, combo, f"{side} by context|setup")

        # Quality score
        if "quality_score" in sub.columns:
            qs = pd.cut(
                pd.to_numeric(sub["quality_score"], errors="coerce"),
                bins=[-np.inf, 2, 3, 4, 5, 6, 7, 8, 9, np.inf],
                labels=["<2", "2-3", "3-4", "4-5", "5-6", "6-7", "7-8", "8-9", ">=9"],
                include_lowest=True,
            )
            _sliced(sub, qs, f"{side} by quality_score")

        # Hour bucket
        if "entry_time_ist" in sub.columns:
            ts = pd.to_datetime(sub["entry_time_ist"], errors="coerce")
            mins = ts.dt.hour * 60 + ts.dt.minute
            hb = pd.cut(
                mins,
                bins=[0, 9 * 60 + 30, 10 * 60, 10 * 60 + 30, 11 * 60, 11 * 60 + 30, 12 * 60, 12 * 60 + 30, 13 * 60, 13 * 60 + 30, 24 * 60],
                labels=["0915-30", "0930-1000", "1000-30", "1030-1100", "1100-30", "1130-1200", "1200-30", "1230-1300", "1300-30", "post1330"],
                include_lowest=True,
            )
            _sliced(sub, hb, f"{side} by entry hour")

        # ATR% bucket
        if "atr_pct_signal" in sub.columns:
            at = pd.cut(
                pd.to_numeric(sub["atr_pct_signal"], errors="coerce"),
                bins=[-np.inf, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.010, np.inf],
                labels=["<0.3%", "0.3-0.4", "0.4-0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-1.0", ">=1.0%"],
                include_lowest=True,
            )
            _sliced(sub, at, f"{side} by atr_pct_signal")

        # AVWAP dist
        for col in ("avwap_dist_atr_signal", "avwap_dist_atr"):
            if col in sub.columns:
                av = pd.cut(
                    pd.to_numeric(sub[col], errors="coerce"),
                    bins=[-np.inf, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, np.inf],
                    labels=["<0.25", "0.25-0.5", "0.5-0.75", "0.75-1.0", "1.0-1.25", "1.25-1.5", "1.5-1.75", "1.75-2.0", ">=2.0"],
                    include_lowest=True,
                )
                _sliced(sub, av, f"{side} by {col}")
                break

        # Outcome distribution
        if "outcome" in sub.columns:
            _sliced(sub, sub["outcome"].astype(str), f"{side} outcome mix")

        # P&L magnitude
        if "pnl_pct" in sub.columns:
            r = pd.cut(
                pd.to_numeric(sub["pnl_pct"], errors="coerce"),
                bins=[-np.inf, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, np.inf],
                labels=["<-6", "-6to-5", "-5to-4", "-4to-3", "-3to-2", "-2to-1", "-1to0", "0to1", "1to2", "2to3", "3to4", "4to5", "5to6", ">=6"],
                include_lowest=True,
            )
            _sliced(sub, r, f"{side} pnl_pct histogram")


def main():
    for label, p in CSV_PATHS.items():
        if not p.exists():
            print(f"[MISS] {label}: {p}")
            continue
        analyze(label, p)


if __name__ == "__main__":
    main()
