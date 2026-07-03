r"""long_edge_study.py — Stage 2 winner/loser LONG follow-through study (research-only).

Studies what conditions precede profitable LONG follow-through, using the pool's pre-computed v6 backtest
outcome (`v6_net_pnl_rs` / `v6_outcome`) as a consistent, causal (no-lookahead) reference label across ALL
LONG candidates. Restricted to sessions BEFORE the TEST window (no TEST peeking). For each feature it reports
winner-vs-loser stats + win-rate by quantile bucket (to spot monotonic edges), plus win-rate by regime,
time-of-day, source detector (structural family), and symbol. Writes RAW_DATA_LONG_EDGE_STUDY.md.

Run:
  py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/long_edge_study.py [--pre_test_end 2026-06-11]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import setup_train_test as tt   # noqa: E402

POOL_DIR = Path(r"C:/TradingData/eqidv2/outputs_ID_v11_unified_pool")
NUM_FEATS = ["vwap_dist_atr", "vol_ratio", "atr_pct", "body_pct", "close_loc", "upper_wick_pct",
             "lower_wick_pct", "wick_skew_pct", "rs_pct", "stock_ret", "quality_score", "signal_range_pct",
             "rsi", "rsi3max", "adx", "macd_hist", "macd_hist_delta", "ema20_slope", "signal_minute",
             "market_ret_pct"]


def _wr(mask):
    n = int(mask.sum())
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pre_test_end", default="2026-06-11", help="study sessions on/before this date (exclude TEST)")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

    tt.POOL_DIRS = [POOL_DIR]; tt.POOL_DIR = POOL_DIR
    pool = tt.load_pool()
    pool = pool[pool["side"].astype(str).str.upper() == "LONG"].copy()
    cutoff = pd.Timestamp(args.pre_test_end)
    pool = pool[pool["_day"] <= cutoff].copy()
    if "v6_net_pnl_rs" not in pool.columns:
        print("[edge] no v6_net_pnl_rs in pool"); return 1
    net = pd.to_numeric(pool["v6_net_pnl_rs"], errors="coerce")
    pool = pool[net.notna()].copy()
    net = net[net.notna()]
    pool["_win"] = (net > 0).astype(int)
    base_wr = pool["_win"].mean()
    print(f"[edge] LONG candidates (<= {cutoff.date()}): {len(pool):,} | base win-rate {base_wr*100:.1f}% "
          f"| avg net Rs {net.mean():.0f}")

    L = ["# RAW_DATA_LONG_EDGE_STUDY — what precedes profitable LONG follow-through", "",
         f"Universe = **{len(pool):,}** LONG raw-candidates on sessions ≤ {cutoff.date()} (TEST window excluded). "
         "Label = pool's pre-computed v6 backtest outcome `v6_net_pnl_rs > 0` (a consistent, causal reference exit "
         "across all candidates — no lookahead). 'lift' = bucket win-rate − base win-rate.", "",
         f"- **Base LONG win-rate: {base_wr*100:.1f}%** · avg net Rs {net.mean():.0f} · "
         f"v6 exit basis: SL {pd.to_numeric(pool.get('v6_sl_pct'),errors='coerce').median():.2f} / "
         f"Tgt {pd.to_numeric(pool.get('v6_target_pct'),errors='coerce').median():.2f} (median)", ""]
    # outcome split
    if "v6_outcome" in pool.columns:
        oc = pool["v6_outcome"].astype(str).value_counts()
        L.append("- v6 outcome split: " + ", ".join(f"{k}={v:,}" for k, v in oc.items()))
        L.append("")

    # ---- numeric features: win-rate by quintile ----
    L += ["## Numeric features — win-rate by quintile (find monotonic LONG edges)",
          "Each row: feature, [Q1..Q5] win-rate%, populated count. A rising/falling sequence = a usable threshold.", ""]
    edges = []
    for f in NUM_FEATS:
        if f not in pool.columns:
            continue
        x = pd.to_numeric(pool[f], errors="coerce")
        m = x.notna()
        if m.sum() < 500 or x[m].nunique() < 5:
            L.append(f"- `{f}`: sparse/constant (pop {int(m.sum()):,}) — skipped")
            continue
        try:
            q = pd.qcut(x[m], 5, labels=False, duplicates="drop")
        except Exception:
            continue
        wr = pool.loc[m, "_win"].groupby(q).mean() * 100
        ed = pool.loc[m].groupby(q).apply(lambda g: pd.to_numeric(g[f], errors="coerce").median())
        cells = " ".join(f"Q{int(i)+1}:{wr.loc[i]:.0f}%(≈{ed.loc[i]:.3g})" for i in wr.index)
        spread = wr.max() - wr.min()
        edges.append((f, spread, wr))
        L.append(f"- `{f}` (pop {int(m.sum()):,}, spread {spread:.0f}pp): {cells}")
    L.append("")
    # rank features by quintile win-rate spread (strength of monotone-ish edge)
    edges.sort(key=lambda e: -e[1])
    L += ["### Strongest single-feature LONG edges (by quintile win-rate spread)"]
    for f, spread, wr in edges[:8]:
        hi = wr.idxmax(); lo = wr.idxmin()
        L.append(f"- **{f}**: spread {spread:.0f}pp — best quintile Q{int(hi)+1} ({wr.loc[hi]:.0f}%), "
                 f"worst Q{int(lo)+1} ({wr.loc[lo]:.0f}%)")
    L.append("")

    # ---- regime ----
    if "regime" in pool.columns:
        L += ["## Regime", ""]
        g = pool.groupby(pool["regime"].astype(str)).agg(n=("_win", "size"), wr=("_win", "mean"))
        for r, row in g.sort_values("wr", ascending=False).iterrows():
            L.append(f"- {r}: n={int(row['n']):,} win {row['wr']*100:.1f}%")
        L.append("")

    # ---- time of day ----
    if "signal_minute" in pool.columns:
        sm = pd.to_numeric(pool["signal_minute"], errors="coerce")
        hr = (sm // 60)
        L += ["## Time of day (entry hour, IST) — LONG follow-through", ""]
        g = pool.groupby(hr).agg(n=("_win", "size"), wr=("_win", "mean"))
        for h, row in g.iterrows():
            if pd.isna(h):
                continue
            L.append(f"- {int(h):02d}:xx  n={int(row['n']):,} win {row['wr']*100:.1f}%")
        L.append("")

    # ---- source detector (structural family) ----
    L += ["## Source detector (structural family) — LONG win-rate", ""]
    g = pool.groupby(pool["setup"].astype(str)).agg(n=("_win", "size"), wr=("_win", "mean"),
                                                    avgnet=("v6_net_pnl_rs", lambda s: pd.to_numeric(s, errors="coerce").mean()))
    for s, row in g.sort_values("wr", ascending=False).iterrows():
        L.append(f"- {s}: n={int(row['n']):,} win {row['wr']*100:.1f}% avgNet Rs{row['avgnet']:.0f}")
    L.append("")

    # ---- symbol concentration (top/bottom by win-rate, min n) ----
    g = pool.groupby("ticker").agg(n=("_win", "size"), wr=("_win", "mean"))
    g = g[g["n"] >= 40]
    L += ["## Symbols with best / worst LONG follow-through (min 40 candidates)", "",
          "Best:"]
    for t, row in g.sort_values("wr", ascending=False).head(10).iterrows():
        L.append(f"- {t}: n={int(row['n'])} win {row['wr']*100:.1f}%")
    L += ["", "Worst:"]
    for t, row in g.sort_values("wr").head(10).iterrows():
        L.append(f"- {t}: n={int(row['n'])} win {row['wr']*100:.1f}%")
    L.append("")

    out = TT_DIR / "long_setup_discovery_from_raw_data"
    (out / "RAW_DATA_LONG_EDGE_STUDY.md").write_text("\n".join(L), encoding="utf-8")
    print(f"[edge] wrote {out/'RAW_DATA_LONG_EDGE_STUDY.md'}")
    # console: top edges
    for f, spread, wr in edges[:6]:
        print(f"  edge {f}: spread {spread:.0f}pp")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
