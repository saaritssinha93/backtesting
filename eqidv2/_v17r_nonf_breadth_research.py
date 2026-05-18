"""READ-ONLY research — does a causal universe-breadth regime gate help v17r_nonf?

Does NOT modify any strategy/pipeline/config file.

Idea: removing the NIFTY filter destroyed the short-side regime context.
Rebuild it WITHOUT an index lookup by measuring breadth across the stock
universe itself: at each 5-min timestamp, what fraction of stocks are
above VWAP / above EMA20 / above prior-day close, and the mean intraday
change. All of these are known-at-bar-close -> causal.

Step 1: build a per-5min-timestamp breadth series from the universe
        parquets (cached to a local parquet so re-runs are instant).
Step 2: merge breadth onto the v17r_nonf 3-month trades CSV at each
        trade's signal_time.
Step 3: report honest PF by breadth bucket, per side, to see whether a
        regime gate concentrates the edge.
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm

DATA_5M = r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"
TRADES_GLOB = r"C:\TradingData\eqidv2\outputs_v17r_nonf_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv"
UNIVERSE = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\configs\universe.csv"
BREADTH_CACHE = Path(__file__).resolve().parent / "_v17r_breadth_cache.parquet"
LEVERAGE = 5.0
# Use the full data window by default; matches the runner's no-env-var behaviour.
# Can be overridden via env for fast last-N-month rebuilds.
import os as _os
DATE_FROM = _os.environ.get("EQIDV_DATE_FROM", "2025-06-02")


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def _f(x):
    return "{:+,.0f}".format(x)


def pick_3mo_csv():
    for p in reversed(sorted(glob.glob(TRADES_GLOB))):
        nd = pd.to_datetime(pd.read_csv(p, usecols=["trade_date"])["trade_date"]).dt.date.nunique()
        if nd <= 70:
            return p
    return sorted(glob.glob(TRADES_GLOB))[-1]


def build_breadth(universe_tickers, force=False):
    if BREADTH_CACHE.exists() and not force:
        print(f"[breadth] using cache {BREADTH_CACHE.name}")
        return pd.read_parquet(BREADTH_CACHE)

    print(f"[breadth] building from {len(universe_tickers)} universe parquets "
          f"(date >= {DATE_FROM}) ...")
    cut = pd.Timestamp(DATE_FROM).tz_localize("Asia/Kolkata")
    cols = ["date", "close", "VWAP", "EMA_20", "Prev_Day_Close", "RSI", "Intra_Change"]
    frames = []
    n_ok = 0
    for tk in universe_tickers:
        f = Path(DATA_5M) / f"{tk}_stocks_indicators_5min.parquet"
        if not f.exists():
            continue
        try:
            d = pd.read_parquet(f, columns=cols)
        except Exception:
            continue
        dt = pd.to_datetime(d["date"], errors="coerce")
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        d["date"] = dt.dt.tz_convert("Asia/Kolkata")
        d = d[d["date"] >= cut]
        if d.empty:
            continue
        d["above_vwap"] = (pd.to_numeric(d["close"], errors="coerce")
                           > pd.to_numeric(d["VWAP"], errors="coerce")).astype(float)
        d["above_ema20"] = (pd.to_numeric(d["close"], errors="coerce")
                            > pd.to_numeric(d["EMA_20"], errors="coerce")).astype(float)
        d["above_pdc"] = (pd.to_numeric(d["close"], errors="coerce")
                          > pd.to_numeric(d["Prev_Day_Close"], errors="coerce")).astype(float)
        d["intra"] = pd.to_numeric(d["Intra_Change"], errors="coerce")
        d["rsi"] = pd.to_numeric(d["RSI"], errors="coerce")
        frames.append(d[["date", "above_vwap", "above_ema20", "above_pdc", "intra", "rsi"]])
        n_ok += 1

    allbars = pd.concat(frames, ignore_index=True)
    breadth = allbars.groupby("date").agg(
        pct_above_vwap=("above_vwap", "mean"),
        pct_above_ema20=("above_ema20", "mean"),
        pct_above_pdc=("above_pdc", "mean"),
        mean_intra=("intra", "mean"),
        mean_rsi=("rsi", "mean"),
        n_stocks=("above_vwap", "size"),
    ).reset_index()
    breadth.to_parquet(BREADTH_CACHE)
    print(f"[breadth] built from {n_ok} tickers -> {len(breadth)} timestamps, cached")
    return breadth


def load_trades():
    path = pick_3mo_csv()
    print(f"[breadth] trades CSV: {path}")
    df = pd.read_csv(path)
    uni = pd.read_csv(UNIVERSE)
    adv = dict(zip(uni["ticker"], uni["adv_rs_cr"]))
    df["adv_rs_cr"] = df["ticker"].map(adv).fillna(0.0)
    df["adv_bucket"] = df["adv_rs_cr"].apply(cm.adv_bucket_for)
    c = [cm.costs_pct_for_v17C(r["adv_bucket"],
         r["outcome"] if r["outcome"] in ("TARGET", "SL") else "TARGET")
         for _, r in df.iterrows()]
    df["cost_pct"] = c
    g = pd.to_numeric(df["pnl_pct_gross_price"], errors="coerce")
    sm = pd.to_numeric(df.get("size_multiplier", 1.0), errors="coerce").fillna(1.0)
    df["net_eff"] = (g - df["cost_pct"]) * LEVERAGE * sm
    df["net_rs"] = df["net_eff"] / 100.0 * pd.to_numeric(df["position_size_rs"], errors="coerce")
    return df


def main():
    uni = pd.read_csv(UNIVERSE)
    breadth = build_breadth(uni["ticker"].astype(str).tolist())
    trades = load_trades()

    # merge breadth onto each trade at signal time (asof: last breadth bar <= signal)
    sig = pd.to_datetime(trades["signal_time_ist"])
    if getattr(sig.dt, "tz", None) is None:
        sig = sig.dt.tz_localize("Asia/Kolkata")
    else:
        sig = sig.dt.tz_convert("Asia/Kolkata")
    trades = trades.assign(_sig=sig).sort_values("_sig")
    breadth = breadth.sort_values("date")
    merged = pd.merge_asof(trades, breadth, left_on="_sig", right_on="date",
                           direction="backward")

    feats = ["pct_above_vwap", "pct_above_ema20", "pct_above_pdc", "mean_intra", "mean_rsi"]
    print(f"\n{'='*92}")
    print(f"v17r_nonf — breadth-at-signal vs honest PF  (n={len(merged)} trades)")
    print(f"  overall PF={pf(merged['net_eff']):.3f}  "
          f"SHORT PF={pf(merged[merged.side=='SHORT']['net_eff']):.3f}  "
          f"LONG PF={pf(merged[merged.side=='LONG']['net_eff']):.3f}")
    print(f"{'='*92}")

    for side in ["SHORT", "LONG"]:
        sub = merged[merged["side"] == side].copy()
        if len(sub) < 8:
            continue
        print(f"\n### {side}  (n={len(sub)}, baseline PF={pf(sub['net_eff']):.3f})")
        for feat in feats:
            v = sub[feat]
            if v.notna().sum() < 8:
                continue
            try:
                sub["_q"] = pd.qcut(v, 3, duplicates="drop")
            except Exception:
                continue
            rows = []
            for q, g in sub.groupby("_q", observed=True):
                rows.append((str(q), len(g), pf(g["net_eff"]),
                             (g["outcome"] == "TARGET").mean() * 100, g["net_rs"].sum()))
            pfs = [r[2] for r in rows if r[2] != float("inf")]
            if not pfs or (max(pfs) - min(pfs)) < 0.5:
                continue
            print(f"  -- {feat} terciles (PF spread {max(pfs)-min(pfs):.2f}) --")
            for lbl, n, p, w, rs in rows:
                flag = "  <== strong" if p >= 2.0 else ("  <-- weak" if p < 1.0 else "")
                print(f"     {lbl:<26s} n={n:<3d} PF={p:6.3f} win%={w:5.1f} Rs={_f(rs)}{flag}")

    # quick gate test: SHORT only when bearish breadth, LONG only when bullish
    print(f"\n{'='*92}\nGATE TEST — directional breadth filter\n{'='*92}")
    for vwap_lo in [0.40, 0.45, 0.50, 0.55]:
        s = merged[(merged.side == "SHORT") & (merged.pct_above_vwap <= vwap_lo)]
        l = merged[(merged.side == "LONG") & (merged.pct_above_vwap >= (1 - vwap_lo))]
        comb = pd.concat([s, l])
        if len(comb) < 10:
            continue
        print(f"  SHORT@pct_above_vwap<={vwap_lo:.2f} + LONG@>={1-vwap_lo:.2f}: "
              f"n={len(comb):<3d} PF={pf(comb['net_eff']):.3f} "
              f"(SHORT n={len(s)} PF={pf(s['net_eff']):.2f} | LONG n={len(l)} PF={pf(l['net_eff']):.2f}) "
              f"Rs={_f(comb['net_rs'].sum())}")


if __name__ == "__main__":
    main()
