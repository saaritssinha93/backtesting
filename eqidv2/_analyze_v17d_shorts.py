"""Find the filter leverage points between v17d SHORT trades (294, PF 1.648)
and the desired v17e target (better PF without losing too many trades).
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

INPUT = Path(
    r"C:\TradingData\eqidv2\outputs_v17d_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_225430.csv"
)


def _pf(pnls: pd.Series) -> float:
    g = float(pnls[pnls > 0].sum())
    l = float(-pnls[pnls < 0].sum())
    return g / l if l > 0 else float("inf")


def _summ(df: pd.DataFrame, label: str) -> dict:
    n = len(df)
    if n == 0:
        return dict(label=label, n=0, pf=0.0, win_pct=0.0, sum_pnl=0.0, avg_pnl=0.0)
    wins = int((df["outcome"].str.upper() == "TARGET").sum())
    return dict(
        label=label,
        n=n,
        pf=_pf(df["pnl_pct"]),
        win_pct=100.0 * wins / n,
        sum_pnl=float(df["pnl_pct"].sum()),
        avg_pnl=float(df["pnl_pct"].mean()),
    )


def _slice(df: pd.DataFrame, series, title: str):
    rows = []
    for val, sub in df.groupby(series, dropna=False, observed=True):
        rows.append(_summ(sub, str(val)))
    out = pd.DataFrame(rows).sort_values("label")
    print(f"\n-- {title} --")
    print(out.to_string(index=False, float_format=lambda x: f"{x:.2f}"))


def main():
    df = pd.read_csv(INPUT)
    sh = df[df["side"].str.upper() == "SHORT"].copy()
    print(f"V17D SHORT rows: {len(sh)}")
    print(pd.Series(_summ(sh, "V17D_SHORT_all")).to_string())

    for col in ("setup", "impulse_type", "nifty_context_mode"):
        if col in sh.columns:
            _slice(sh, sh[col], f"by {col}")

    if "quality_score" in sh.columns:
        qs = pd.cut(
            pd.to_numeric(sh["quality_score"], errors="coerce"),
            bins=[-np.inf, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, np.inf],
            labels=["<1.5", "1.5-2.5", "2.5-3.5", "3.5-4.5", "4.5-5.5", "5.5-6.5", "6.5-7.5", ">=7.5"],
            include_lowest=True,
        )
        _slice(sh, qs, "by quality_score")

    for col in ("avwap_dist_atr_signal", "avwap_dist_atr"):
        if col in sh.columns:
            av = pd.cut(
                pd.to_numeric(sh[col], errors="coerce"),
                bins=[-np.inf, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, np.inf],
                labels=["<0.25", "0.25-0.5", "0.5-0.75", "0.75-1.0", "1.0-1.25", "1.25-1.5", "1.5-1.75", "1.75-2.0", ">=2.0"],
                include_lowest=True,
            )
            _slice(sh, av, f"by {col}")
            break

    if "atr_pct_signal" in sh.columns:
        at = pd.cut(
            pd.to_numeric(sh["atr_pct_signal"], errors="coerce"),
            bins=[-np.inf, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.010, np.inf],
            labels=["<0.3%", "0.3-0.4", "0.4-0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-1.0", ">=1.0%"],
            include_lowest=True,
        )
        _slice(sh, at, "by atr_pct_signal")

    for col in ("adx_signal", "adx"):
        if col in sh.columns:
            ad = pd.cut(
                pd.to_numeric(sh[col], errors="coerce"),
                bins=[-np.inf, 22, 25, 28, 31, 35, 40, 45, 50, np.inf],
                labels=["<22", "22-25", "25-28", "28-31", "31-35", "35-40", "40-45", "45-50", ">=50"],
                include_lowest=True,
            )
            _slice(sh, ad, f"by {col}")
            break

    for col in ("rsi_signal", "rsi"):
        if col in sh.columns:
            rs = pd.cut(
                pd.to_numeric(sh[col], errors="coerce"),
                bins=[-np.inf, 20, 25, 30, 35, 40, 45, 50, 55, np.inf],
                labels=["<20", "20-25", "25-30", "30-35", "35-40", "40-45", "45-50", "50-55", ">=55"],
                include_lowest=True,
            )
            _slice(sh, rs, f"by {col}")
            break

    if "nifty_rel_strength_pct" in sh.columns:
        rs2 = pd.cut(
            pd.to_numeric(sh["nifty_rel_strength_pct"], errors="coerce"),
            bins=[-np.inf, -1.5, -1.0, -0.75, -0.5, -0.25, 0, 0.25, np.inf],
            labels=["<-1.5", "-1.5..-1", "-1..-0.75", "-0.75..-0.5", "-0.5..-0.25", "-0.25..0", "0..0.25", ">=0.25"],
            include_lowest=True,
        )
        _slice(sh, rs2, "by nifty_rel_strength_pct")

    for ts_col in ("entry_time_ist", "signal_time_ist"):
        if ts_col in sh.columns:
            ts = pd.to_datetime(sh[ts_col], errors="coerce")
            mins = ts.dt.hour * 60 + ts.dt.minute
            hb = pd.cut(
                mins,
                bins=[0, 9 * 60 + 30, 10 * 60, 10 * 60 + 30, 11 * 60, 11 * 60 + 30, 12 * 60, 12 * 60 + 30, 13 * 60, 13 * 60 + 30, 14 * 60, 24 * 60],
                labels=["0915-0930", "0930-1000", "1000-1030", "1030-1100", "1100-1130", "1130-1200", "1200-1230", "1230-1300", "1300-1330", "1330-1400", "post1400"],
                include_lowest=True,
            )
            _slice(sh, hb, f"by {ts_col} bucket")
            break

    if {"setup", "nifty_context_mode"}.issubset(sh.columns):
        combo = sh["nifty_context_mode"].astype(str) + "|" + sh["setup"].astype(str)
        _slice(sh, combo, "by context|setup")


if __name__ == "__main__":
    main()
