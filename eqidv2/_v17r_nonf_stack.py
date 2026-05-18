"""READ-ONLY — test candidate stacked filter chains on v17r_nonf.

Does NOT modify any strategy/pipeline/config file. Tests honest PF (realistic
per-row costs) for filter combos so suggestions are data-grounded, including
an IS/OOS split to check the chain isn't overfit.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm

TRADES = r"C:\TradingData\eqidv2\outputs_v17r_nonf_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260514_071744.csv"
UNIVERSE = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\configs\universe.csv"
LEVERAGE = 5.0
IS_END = "2026-02-15"


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def load():
    df = pd.read_csv(TRADES)
    uni = pd.read_csv(UNIVERSE)
    adv = dict(zip(uni["ticker"], uni["adv_rs_cr"]))
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
    t = pd.to_datetime(df["entry_time_ist"])
    df["entry_hour"] = t.dt.hour + t.dt.minute / 60.0
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def report(df, label):
    n = len(df)
    if n == 0:
        print(f"  {label:<52s} n=0")
        return
    is_d = df[df["trade_date"] <= pd.to_datetime(IS_END).date()]
    oos = df[df["trade_date"] > pd.to_datetime(IS_END).date()]
    ndays = df["trade_date"].nunique()
    print(f"  {label:<52s} n={n:<4d} /day={n/max(ndays,1):.2f} "
          f"PF={pf(df['net_eff']):.3f} win%={(df['outcome']=='TARGET').mean()*100:.1f} "
          f"Rs={df['net_rs'].sum():+,.0f} | IS PF={pf(is_d['net_eff']):.2f}(n{len(is_d)}) "
          f"OOS PF={pf(oos['net_eff']):.2f}(n{len(oos)})")


def main():
    df = load()
    a = df[df["setup"] == "A_MOD_BREAK_C1_HIGH"].copy()

    print("=" * 100)
    print("A_MOD_BREAK_C1_HIGH — greedy stacked-filter test (honest costs, IS<=2026-02-15)")
    print("=" * 100)
    report(a, "baseline (all)")

    # layer the filters one at a time
    f1 = a[a["adv_bucket"].isin(["mid", "top100"])]
    report(f1, "F1: ADV >= Rs50cr")

    f2 = f1[f1["entry_bar_vol_ratio"] < 1.45]
    report(f2, "F2: + entry_bar_vol_ratio < 1.45")

    f3 = f2[f2["rsi_signal"] <= 73]
    report(f3, "F3: + rsi_signal <= 73")

    f4 = f3[f3["bars_from_open"] <= 5]
    report(f4, "F4: + bars_from_open <= 5")

    # alternative branches from F1
    print("\n  -- alternative single adds on F1 (ADV>=50cr) --")
    for label, mask in [
        ("adx_signal >= 46", f1["adx_signal"] >= 46),
        ("entry_bar_vol_ratio < 1.45", f1["entry_bar_vol_ratio"] < 1.45),
        ("rsi_signal 60-69", (f1["rsi_signal"] >= 60) & (f1["rsi_signal"] <= 69)),
        ("atr_pct_signal >= 0.0072", f1["atr_pct_signal"] >= 0.0072),
        ("entry_hour < 9.83", f1["entry_hour"] < 9.83),
        ("bars_from_open <= 2", f1["bars_from_open"] <= 2),
    ]:
        report(f1[mask], label)

    # two-filter combos on F1
    print("\n  -- two-filter combos on F1 --")
    combos = [
        ("volratio<1.45 & adx>=46",
         (f1["entry_bar_vol_ratio"] < 1.45) & (f1["adx_signal"] >= 46)),
        ("volratio<1.45 & rsi 60-69",
         (f1["entry_bar_vol_ratio"] < 1.45) & (f1["rsi_signal"] >= 60) & (f1["rsi_signal"] <= 69)),
        ("volratio<1.45 & bars<=2",
         (f1["entry_bar_vol_ratio"] < 1.45) & (f1["bars_from_open"] <= 2)),
        ("volratio<1.45 & entry_hour<9.83",
         (f1["entry_bar_vol_ratio"] < 1.45) & (f1["entry_hour"] < 9.83)),
        ("adx>=46 & rsi<=73",
         (f1["adx_signal"] >= 46) & (f1["rsi_signal"] <= 73)),
        ("adx>=37 & volratio<1.8 & bars<=5",
         (f1["adx_signal"] >= 37) & (f1["entry_bar_vol_ratio"] < 1.8) & (f1["bars_from_open"] <= 5)),
    ]
    for label, mask in combos:
        report(f1[mask], label)

    # what does the loosest "PF>=2" still-meaningful-count combo look like?
    print("\n  -- best-effort PF>=2 chains --")
    for label, mask in [
        ("top100 only",
         a["adv_bucket"] == "top100"),
        ("ADV>=50 & volratio<1.45 & entry_hour<9.83",
         (f1["entry_bar_vol_ratio"] < 1.45) & (f1["entry_hour"] < 9.83)),
        ("ADV>=50 & volratio<1.2 & adx>=37",
         (f1["entry_bar_vol_ratio"] < 1.2) & (f1["adx_signal"] >= 37)),
    ]:
        src = a if "top100 only" in label else f1
        report(src[mask] if "top100 only" not in label else a[mask], label)


if __name__ == "__main__":
    main()
