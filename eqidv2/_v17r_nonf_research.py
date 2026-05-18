"""READ-ONLY research scratch — sub-bucket analysis for v17r_nonf.

Does NOT modify any strategy / pipeline / config file. Loads the trades CSV,
applies realistic per-row costs, then for each setup splits each signal-time
feature into quantile buckets and reports PF/n/Rs per bucket so we can see
where edge concentrates. Then greedily builds a candidate filter chain.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm

TRADES = r"C:\TradingData\eqidv2\outputs_v17r_nonf_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260513_193130.csv"
UNIVERSE = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\configs\universe.csv"
LEVERAGE = 5.0

# signal-time features only (available at entry — no look-ahead)
FEATURES = [
    "adx_signal", "rsi_signal", "stochk_signal", "avwap_dist_atr_signal",
    "ema20_gap_atr_signal", "atr_pct_signal", "quality_score",
    "entry_bar_vol_ratio", "bars_from_open",
]


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def apply_costs(df, universe):
    adv = dict(zip(universe["ticker"], universe["adv_rs_cr"]))
    df = df.copy()
    df["adv_rs_cr"] = df["ticker"].map(adv).fillna(0.0)
    df["adv_bucket"] = df["adv_rs_cr"].apply(cm.adv_bucket_for)
    costs = []
    for _, r in df.iterrows():
        kind = r["outcome"] if r["outcome"] in ("TARGET", "SL") else "TARGET"
        costs.append(cm.costs_pct_for_v17C(r["adv_bucket"], kind))
    df["cost_pct"] = costs
    gross = pd.to_numeric(df["pnl_pct_gross_price"], errors="coerce")
    sm = pd.to_numeric(df.get("size_multiplier", 1.0), errors="coerce").fillna(1.0)
    df["net_eff"] = (gross - df["cost_pct"]) * LEVERAGE * sm
    df["net_rs"] = df["net_eff"] / 100.0 * pd.to_numeric(df["position_size_rs"], errors="coerce")
    return df


def hour_of(ts):
    t = pd.to_datetime(ts)
    return t.dt.hour + t.dt.minute / 60.0


def main():
    df = pd.read_csv(TRADES)
    uni = pd.read_csv(UNIVERSE)
    df = apply_costs(df, uni)
    df["entry_hour"] = hour_of(df["entry_time_ist"])

    print("=" * 90)
    print(f"v17r_nonf — {len(df)} trades, realistic per-row costs, sizing-aware")
    print(f"  overall PF={pf(df['net_eff']):.3f}  sumRs={df['net_rs'].sum():+,.0f}")
    print("=" * 90)

    for setup in sorted(df["setup"].unique()):
        sub = df[df["setup"] == setup].copy()
        print(f"\n{'#'*90}\n# {setup}   n={len(sub)}  PF={pf(sub['net_eff']):.3f}  "
              f"sumRs={sub['net_rs'].sum():+,.0f}  win%={(sub['outcome']=='TARGET').mean()*100:.1f}")
        print(f"{'#'*90}")

        # ADV bucket split
        print("  -- by adv_bucket --")
        for b, g in sub.groupby("adv_bucket"):
            print(f"     {b:<10s} n={len(g):<4d} PF={pf(g['net_eff']):.3f} "
                  f"Rs={g['net_rs'].sum():+,.0f}")

        # entry_hour quartiles
        for feat in FEATURES + ["entry_hour"]:
            if feat not in sub.columns or sub[feat].notna().sum() < 20:
                continue
            try:
                sub["_q"] = pd.qcut(sub[feat], 4, duplicates="drop")
            except Exception:
                continue
            rows = []
            for q, g in sub.groupby("_q", observed=True):
                rows.append((str(q), len(g), pf(g["net_eff"]),
                             g["net_rs"].sum(),
                             (g["outcome"] == "TARGET").mean() * 100))
            # only print if there's meaningful PF spread across buckets
            pfs = [r[2] for r in rows if r[2] != float("inf")]
            if not pfs or (max(pfs) - min(pfs)) < 0.4:
                continue
            print(f"  -- {feat} quartiles (PF spread {max(pfs)-min(pfs):.2f}) --")
            for label, n, p, rs, w in rows:
                flag = "  <== strong" if p >= 1.6 else ("  <-- weak" if p < 0.9 else "")
                print(f"     {label:<28s} n={n:<4d} PF={p:6.3f} win%={w:5.1f} Rs={rs:+,.0f}{flag}")


if __name__ == "__main__":
    main()
