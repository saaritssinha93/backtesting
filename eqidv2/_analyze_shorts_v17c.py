"""
Deep analytics on v17c SHORT trades:
  - Winners vs losers by feature
  - Slices that are being filtered out but could be re-admitted
  - Time-of-day / day-of-week / day-mode / impulse-type slices
  - Setup-wise edge
  - Quality score, RSI, ADX, AVWAP dist, ATR pct bucketing
  - RS / Nifty context effects
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

INPUT = Path(
    r"C:\TradingData\eqidv2\outputs_v17c_5min\avwap_longshort_trades_v17c_5min_ALL_DAYS_20260409_145937.csv"
)

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 80)
pd.set_option("display.max_rows", 60)


def _pf(pnls: pd.Series) -> float:
    gains = float(pnls[pnls > 0].sum())
    losses = float(-pnls[pnls < 0].sum())
    return gains / losses if losses > 0 else float("inf")


def _summarise(df: pd.DataFrame, label: str) -> dict:
    n = len(df)
    wins = int((df["outcome"].str.upper() == "TARGET").sum())
    losses = int((df["outcome"].str.upper() == "SL").sum())
    eod = int((df["outcome"].str.upper() == "EOD").sum())
    avg = float(df["pnl_pct"].mean()) if n else 0.0
    total = float(df["pnl_pct"].sum()) if n else 0.0
    pf = _pf(df["pnl_pct"]) if n else 0.0
    win_rate = 100.0 * wins / n if n else 0.0
    return dict(
        label=label,
        n=n,
        win=wins,
        sl=losses,
        eod=eod,
        win_pct=win_rate,
        avg_pnl_pct=avg,
        sum_pnl_pct=total,
        pf=pf,
    )


def _print_slice(df: pd.DataFrame, col: str, bins=None, labels=None, title: str = ""):
    if df.empty:
        return
    if bins is not None:
        series = pd.cut(pd.to_numeric(df[col], errors="coerce"), bins=bins, labels=labels, include_lowest=True)
    else:
        series = df[col]
    rows = []
    for val, sub in df.groupby(series, dropna=False, observed=True):
        s = _summarise(sub, str(val))
        rows.append(s)
    out = pd.DataFrame(rows)
    if out.empty:
        return
    out = out.sort_values("label")
    print(f"\n-- {title or col} --")
    print(out.to_string(index=False, float_format=lambda x: f"{x:.2f}"))


def main():
    df = pd.read_csv(INPUT)
    print(f"Loaded {len(df)} rows; columns: {list(df.columns)[:40]}...")

    df_short = df[df["side"].str.upper() == "SHORT"].copy()
    df_long = df[df["side"].str.upper() == "LONG"].copy()
    print(f"\nSHORT rows: {len(df_short)}  LONG rows: {len(df_long)}")

    # Overall
    print("\n==== SHORT overall ====")
    s = _summarise(df_short, "SHORT_all")
    print(pd.Series(s).to_string())

    # Setup-wise
    _print_slice(df_short, "setup", title="SHORT setup edge")

    # Impulse-type
    _print_slice(df_short, "impulse_type", title="SHORT impulse_type edge")

    # Day-mode
    if "day_mode" in df_short.columns:
        _print_slice(df_short, "day_mode", title="SHORT day_mode edge")

    # Hour of day from entry_time_ist
    for ts_col in ("entry_time_ist", "signal_time_ist"):
        if ts_col in df_short.columns:
            ts = pd.to_datetime(df_short[ts_col], errors="coerce")
            df_short["_hhmm"] = ts.dt.strftime("%H:%M")
            df_short["_hour_bucket"] = pd.cut(
                ts.dt.hour * 60 + ts.dt.minute,
                bins=[0, 9 * 60 + 30, 10 * 60, 10 * 60 + 30, 11 * 60, 11 * 60 + 30, 12 * 60 + 30, 13 * 60, 13 * 60 + 30, 14 * 60, 24 * 60],
                labels=[
                    "0915-0930",
                    "0930-1000",
                    "1000-1030",
                    "1030-1100",
                    "1100-1130",
                    "1130-1230",
                    "1230-1300",
                    "1300-1330",
                    "1330-1400",
                    "post1400",
                ],
                include_lowest=True,
            )
            _print_slice(df_short, "_hour_bucket", title=f"SHORT edge by entry time bucket ({ts_col})")
            break

    # Quality score
    if "quality_score" in df_short.columns:
        _print_slice(
            df_short,
            "quality_score",
            bins=[-np.inf, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, np.inf],
            labels=["<2", "2-3", "3-4", "4-5", "5-6", "6-7", "7-8", "8-9", "9-10", "10-12", ">=12"],
            title="SHORT quality_score edge",
        )

    # RSI at signal
    for col in ("rsi_signal", "rsi", "rsi_entry"):
        if col in df_short.columns:
            _print_slice(
                df_short,
                col,
                bins=[-np.inf, 20, 25, 30, 35, 40, 45, 50, 55, 60, np.inf],
                labels=["<20", "20-25", "25-30", "30-35", "35-40", "40-45", "45-50", "50-55", "55-60", ">=60"],
                title=f"SHORT {col} edge",
            )
            break

    # ADX at signal
    for col in ("adx_signal", "adx"):
        if col in df_short.columns:
            _print_slice(
                df_short,
                col,
                bins=[-np.inf, 20, 25, 30, 35, 40, 45, 50, np.inf],
                labels=["<20", "20-25", "25-30", "30-35", "35-40", "40-45", "45-50", ">=50"],
                title=f"SHORT {col} edge",
            )
            break

    # AVWAP distance
    for col in ("avwap_dist_atr_signal", "avwap_dist_atr"):
        if col in df_short.columns:
            _print_slice(
                df_short,
                col,
                bins=[-np.inf, 0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, np.inf],
                labels=["<0", "0-0.25", "0.25-0.5", "0.5-0.75", "0.75-1.0", "1.0-1.25", "1.25-1.5", "1.5-1.75", "1.75-2.0", "2.0-2.5", ">=2.5"],
                title=f"SHORT {col} edge",
            )
            break

    # ATR%
    if "atr_pct_signal" in df_short.columns:
        _print_slice(
            df_short,
            "atr_pct_signal",
            bins=[-np.inf, 0.002, 0.003, 0.004, 0.005, 0.007, 0.010, 0.015, np.inf],
            labels=["<0.2%", "0.2-0.3", "0.3-0.4", "0.4-0.5", "0.5-0.7", "0.7-1.0", "1.0-1.5", ">=1.5%"],
            title="SHORT atr_pct_signal edge",
        )

    # Nifty RS
    if "nifty_rel_strength_pct" in df_short.columns:
        _print_slice(
            df_short,
            "nifty_rel_strength_pct",
            bins=[-np.inf, -2.0, -1.5, -1.0, -0.75, -0.5, -0.25, 0, 0.5, np.inf],
            labels=["<-2", "-2 to -1.5", "-1.5 to -1", "-1 to -0.75", "-0.75 to -0.5", "-0.5 to -0.25", "-0.25 to 0", "0 to 0.5", ">=0.5"],
            title="SHORT rel_strength edge",
        )

    # Context mode
    if "nifty_context_mode" in df_short.columns:
        _print_slice(df_short, "nifty_context_mode", title="SHORT nifty_context_mode edge")

    # Month
    if "trade_date" in df_short.columns:
        df_short["_ym"] = pd.to_datetime(df_short["trade_date"], errors="coerce").dt.strftime("%Y-%m")
        _print_slice(df_short, "_ym", title="SHORT month edge")

    # Day-of-week
    if "trade_date" in df_short.columns:
        df_short["_dow"] = pd.to_datetime(df_short["trade_date"], errors="coerce").dt.day_name()
        _print_slice(df_short, "_dow", title="SHORT day-of-week edge")

    # Setup x impulse
    if {"setup", "impulse_type"}.issubset(df_short.columns):
        df_short["_setup_imp"] = df_short["setup"].astype(str) + "|" + df_short["impulse_type"].astype(str)
        _print_slice(df_short, "_setup_imp", title="SHORT setup|impulse edge")

    # Context x setup
    if {"setup", "nifty_context_mode"}.issubset(df_short.columns):
        df_short["_ctx_setup"] = df_short["nifty_context_mode"].astype(str) + "|" + df_short["setup"].astype(str)
        _print_slice(df_short, "_ctx_setup", title="SHORT context|setup edge")

    # Outcome breakdown
    if "outcome" in df_short.columns:
        _print_slice(df_short, "outcome", title="SHORT outcome distribution")


if __name__ == "__main__":
    main()
