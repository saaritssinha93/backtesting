"""
Diagnostic views that describe *where V17's edge lives* — broken down by
side, setup, time-of-day, quality score, RSI bucket, avwap distance bucket,
and nifty context mode.  Produces CSVs and a terse printed summary.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .core import load_all_v17, _pf

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def _group_metrics(df: pd.DataFrame, groupcol: str, pnl_col: str = "pnl_pct") -> pd.DataFrame:
    d = df.copy()
    d[groupcol] = d[groupcol].astype("object")
    rows = []
    for g, sub in d.groupby(groupcol, dropna=False):
        pnls = sub[pnl_col]
        n = len(sub)
        target = (sub["outcome"].astype(str).str.upper() == "TARGET").sum()
        sl = (sub["outcome"].astype(str).str.upper() == "SL").sum()
        eod = (sub["outcome"].astype(str).str.upper() == "EOD").sum()
        rows.append({
            groupcol: g,
            "n_trades": n,
            "target_hits": int(target),
            "sl_hits": int(sl),
            "eod_hits": int(eod),
            "win_rate": 100.0 * target / n if n else 0.0,
            "avg_pnl": float(pnls.mean()) if n else 0.0,
            "sum_pnl": float(pnls.sum()),
            "pf": _pf(pnls),
        })
    return pd.DataFrame(rows).sort_values("sum_pnl", ascending=False).reset_index(drop=True)


def main():
    bundles = load_all_v17()
    df = bundles["v17f"].copy()

    # time bucket
    bins = [9*60, 9*60+45, 10*60+30, 11*60+30, 12*60+30, 13*60+30, 14*60+30, 16*60]
    labels = ["0915_0945", "0945_1030", "1030_1130", "1130_1230", "1230_1330", "1330_1430", "1430_1600"]
    df["time_bucket"] = pd.cut(df["entry_minute"], bins=bins, right=False, labels=labels).astype(str)

    df["qs_bucket"] = pd.cut(
        pd.to_numeric(df["quality_score"], errors="coerce"),
        bins=[-np.inf, 4, 5, 6, 7, 8, 9, 10, np.inf],
        right=False,
        labels=["<4", "[4,5)", "[5,6)", "[6,7)", "[7,8)", "[8,9)", "[9,10)", "10+"],
    ).astype(str)

    df["rsi_bucket"] = pd.cut(
        pd.to_numeric(df["rsi_signal"], errors="coerce"),
        bins=[0, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 100],
        labels=["<20","20-25","25-30","30-35","35-40","40-45","45-50","50-55","55-60","60-65","65-70","70-75","75+"],
    ).astype(str)

    df["avwap_bucket"] = pd.cut(
        pd.to_numeric(df["avwap_dist_atr_signal"], errors="coerce"),
        bins=[-np.inf, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, np.inf],
        right=False,
        labels=["<.25", "[.25,.5)", "[.5,1)", "[1,1.5)", "[1.5,2)", "[2,3)", "3+"],
    ).astype(str)

    # Write breakouts by (side, groupcol)
    for side in ["LONG", "SHORT"]:
        sub = df[df["side"] == side]
        for col in ["setup", "time_bucket", "qs_bucket", "rsi_bucket", "avwap_bucket", "nifty_context_mode"]:
            table = _group_metrics(sub, col)
            out = RESULTS_DIR / f"diag_{side}_{col}.csv"
            table.to_csv(out, index=False)

    # Print highlight bars — things with PF < 1.2 (leaks) and > 2.5 (edge concentrations)
    print("=== V17F LEAKS (PF < 1.2, n>=10) and STRONG EDGE (PF > 2.5, n>=10) ===")
    for side in ["LONG", "SHORT"]:
        sub = df[df["side"] == side]
        for col in ["setup", "time_bucket", "qs_bucket", "rsi_bucket", "avwap_bucket"]:
            table = _group_metrics(sub, col)
            weak = table[(table["pf"] < 1.2) & (table["n_trades"] >= 10)]
            strong = table[(table["pf"] > 2.5) & (table["n_trades"] >= 10)]
            if not weak.empty:
                print(f"\n[{side}] LEAK in {col}:")
                print(weak.to_string(index=False))
            if not strong.empty:
                print(f"\n[{side}] STRONG edge in {col}:")
                print(strong.to_string(index=False))

    print("\n[OK] diagnostics written to results/diag_*.csv")


if __name__ == "__main__":
    main()
