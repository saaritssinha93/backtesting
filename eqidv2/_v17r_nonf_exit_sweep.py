"""READ-ONLY exit-geometry sweep for v17r_nonf.

Does NOT modify any strategy/pipeline/config file. Takes the v17r_nonf
3-month trades CSV (entries are fixed — they do not depend on exit
geometry), re-resolves every trade against 1-min bars under a grid of
TGT/SL percentages, applies honest v17D per-row costs, and reports PF.

The point: find the exit geometry that lifts honest PF toward 2.0 before
committing a full 50-min backtest run.
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm
from eqidv2 import v17D_exit_resolver as er

TRADES_GLOB = r"C:\TradingData\eqidv2\outputs_v17r_nonf_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv"
UNIVERSE = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\configs\universe.csv"
PARQUET_1MIN = r"C:\TradingData\eqidv2\stocks_indicators_1min_eq"
LEVERAGE = 5.0

# (tgt_pct, sl_pct) grid -- percentages, e.g. 1.2 = 1.2%
# Round 1 showed: tightening SL hurts (noise stop-outs); widening TGT wins.
# Round 2 refines around the TGT 1.5 / SL 0.75 sweet spot.
GRID = [
    (0.8, 0.75),   # current baseline
    (1.3, 0.75),
    (1.4, 0.75),
    (1.5, 0.75),
    (1.6, 0.75),
    (1.75, 0.75),
    (1.4, 0.85),
    (1.5, 0.85),
    (1.6, 0.85),
    (1.5, 1.0),
    (1.6, 1.0),
    (1.75, 1.0),
]


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def _f(x):
    return "{:+,.0f}".format(x)


def pick_latest_3mo_csv():
    # newest CSV whose trade_date span looks like the ~3-month run
    paths = sorted(glob.glob(TRADES_GLOB))
    for p in reversed(paths):
        df = pd.read_csv(p, usecols=["trade_date"])
        ndays = pd.to_datetime(df["trade_date"]).dt.date.nunique()
        if ndays <= 70:  # 3-month run, not the full 11-month
            return p
    return paths[-1]


def main():
    path = pick_latest_3mo_csv()
    print(f"[exit-sweep] trades CSV: {path}")
    trades = pd.read_csv(path)
    uni = pd.read_csv(UNIVERSE)
    adv = dict(zip(uni["ticker"], uni["adv_rs_cr"]))
    trades["adv_rs_cr"] = trades["ticker"].map(adv).fillna(0.0)
    trades["adv_bucket"] = trades["adv_rs_cr"].apply(cm.adv_bucket_for)
    print(f"[exit-sweep] {len(trades)} trades | "
          f"{pd.to_datetime(trades['trade_date']).min().date()} -> "
          f"{pd.to_datetime(trades['trade_date']).max().date()}")

    # cache 1-min bars per ticker
    bar_cache: dict = {}

    def bars_for(tk):
        if tk not in bar_cache:
            bar_cache[tk] = er.load_1min(PARQUET_1MIN, tk)
        return bar_cache[tk]

    print(f"\n{'TGT/SL':<12} {'n':>4} {'win%':>6} {'PF':>7} {'sumRs':>12} "
          f"{'tgt/sl/eod':>14}  {'SHORT_PF':>9} {'LONG_PF':>8}")
    print("-" * 86)

    results = []
    for tgt, sl in GRID:
        rows = []
        n_missing = 0
        for _, t in trades.iterrows():
            tk = str(t["ticker"])
            side = str(t["side"]).upper()
            bars = bars_for(tk)
            res = None
            if bars is not None:
                res = er.resolve(bars, side, float(t["entry_price"]),
                                 t["entry_time_ist"], sl, tgt)
            if res is None:
                n_missing += 1
                continue
            cost = cm.costs_pct_for_v17C(
                t["adv_bucket"],
                res.outcome if res.outcome in ("TARGET", "SL") else "TARGET",
            )
            sm = float(t.get("size_multiplier", 1.0) or 1.0)
            net_eff = (res.pnl_pct_price - cost) * LEVERAGE * sm
            net_rs = net_eff / 100.0 * float(t.get("position_size_rs", 0) or 0)
            rows.append({
                "side": side, "setup": t["setup"], "outcome": res.outcome,
                "net_eff": net_eff, "net_rs": net_rs,
            })
        r = pd.DataFrame(rows)
        if r.empty:
            continue
        win = (r["outcome"] == "TARGET").mean() * 100
        mix = r["outcome"].value_counts()
        mixs = f"{mix.get('TARGET',0)}/{mix.get('SL',0)}/{mix.get('EOD',0)}"
        sp = r[r.side == "SHORT"]
        lp = r[r.side == "LONG"]
        label = f"{tgt:.1f}/{sl:.2f}"
        if (tgt, sl) == (0.8, 0.75):
            label += "*"
        print(f"{label:<12} {len(r):>4} {win:>5.1f}% {pf(r['net_eff']):>7.3f} "
              f"{_f(r['net_rs'].sum()):>12} {mixs:>14}  "
              f"{pf(sp['net_eff']):>9.3f} {pf(lp['net_eff']):>8.3f}")
        results.append((tgt, sl, pf(r["net_eff"]), len(r), win, r))

    # best by PF with n>=40
    valid = [x for x in results if x[3] >= 40]
    if valid:
        best = max(valid, key=lambda x: x[2])
        print(f"\n[exit-sweep] best (n>=40): TGT {best[0]:.1f}% / SL {best[1]:.2f}% "
              f"-> PF {best[2]:.3f}, n={best[3]}, win={best[4]:.1f}%")
        bdf = best[5]
        print("  per-setup at best geometry:")
        for (sd, st), g in bdf.groupby(["side", "setup"]):
            print(f"    {sd:<6} {st:<28} n={len(g):<4d} PF={pf(g['net_eff']):.3f} "
                  f"win%={(g['outcome']=='TARGET').mean()*100:.1f} Rs={_f(g['net_rs'].sum())}")
    print("\n* = current production geometry")


if __name__ == "__main__":
    main()
