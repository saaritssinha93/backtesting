"""READ-ONLY analyzer — mine per-setup causal filter chains for v17r_nonf.

Does NOT modify any strategy / pipeline / config file. Loads the latest
v17r_nonf trades CSV, applies realistic per-row costs (sizing + leverage
aware), then for each setup runs a greedy 3-step causal-filter chain search
with an IS/OOS split. Finally assembles an aggregate candidate config and
reports honest PF so the runner-side filter chains can be data-grounded.

All features used are known-at-entry. `entry_bar_vol_ratio` is treated as a
SEPARATE "volume" profile because the full entry bar must close before the
ratio is known — it is reported but kept out of the default causal chains.
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm

TRADES_GLOB = r"C:\TradingData\eqidv2\outputs_v17r_nonf_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv"
UNIVERSE = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\configs\universe.csv"
LEVERAGE = 5.0
IS_END = "2026-02-15"

# Known-at-entry features. entry_bar_vol_ratio kept separate (see module docstring).
CAUSAL_FEATURES = [
    "adx_signal", "rsi_signal", "stochk_signal", "avwap_dist_atr_signal",
    "ema20_gap_atr_signal", "atr_pct_signal", "quality_score", "india_vix",
    "gap_pct_open", "opening_range_width_pct", "bars_from_open", "entry_hour",
]
VOLUME_FEATURE = "entry_bar_vol_ratio"

N_FLOOR = 30          # min kept rows for a chain step to be considered
PF_TARGET = 2.0       # cascade target tiers
PF_TIERS = [2.0, 1.7, 1.5, 1.3, 1.2]
KEEP_PF_MIN = 1.20    # setups whose best chain stays below this are dropped


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def _f(x):
    return "{:+,.0f}".format(x)


def load():
    path = sorted(glob.glob(TRADES_GLOB))[-1]
    print(f"[analyzer] trades CSV: {path}")
    df = pd.read_csv(path)
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
    return df, path


def is_oos(df):
    cut = pd.to_datetime(IS_END).date()
    return df[df["trade_date"] <= cut], df[df["trade_date"] > cut]


def score(sub):
    """PF * sqrt(n) — rewards PF without rewarding tiny overfit pools."""
    n = len(sub)
    if n < N_FLOOR:
        return -1.0
    p = pf(sub["net_eff"])
    if p == float("inf"):
        p = 5.0
    return p * np.sqrt(n)


def candidate_rules(sub, features):
    """Yield (feature, op, threshold) candidate cuts from quantiles."""
    out = []
    for feat in features:
        if feat not in sub.columns:
            continue
        vals = pd.to_numeric(sub[feat], errors="coerce").dropna()
        if len(vals) < N_FLOOR * 2:
            continue
        qs = vals.quantile([0.15, 0.25, 0.35, 0.5, 0.65, 0.75, 0.85]).unique()
        for thr in qs:
            out.append((feat, ">=", float(thr)))
            out.append((feat, "<=", float(thr)))
    return out


def apply_rule(df, feat, op, thr):
    vals = pd.to_numeric(df[feat], errors="coerce")
    if op == ">=":
        return df[vals >= thr]
    return df[vals <= thr]


def greedy_chain(sub, features, max_steps=3):
    """Greedy chain: each step maximizes PF*sqrt(n), n>=N_FLOOR."""
    chain = []
    cur = sub.copy()
    base_score = score(cur)
    for _ in range(max_steps):
        best = None
        best_score = score(cur)
        for feat, op, thr in candidate_rules(cur, features):
            cand = apply_rule(cur, feat, op, thr)
            sc = score(cand)
            if sc > best_score + 1e-9:
                best_score = sc
                best = (feat, op, thr, cand)
        if best is None:
            break
        feat, op, thr, cand = best
        # stop if PF already strong and adding step barely helps n-adjusted
        chain.append((feat, op, thr))
        cur = cand
        if pf(cur["net_eff"]) >= PF_TARGET:
            break
    return chain, cur


def fmt_chain(chain):
    return " AND ".join(f"{f} {o} {t:.4g}" for f, o, t in chain) if chain else "(none)"


def analyze_setup(df, setup, side, features):
    sub = df[(df["setup"] == setup) & (df["side"] == side)].copy()
    if len(sub) < N_FLOOR:
        return None
    chain, kept = greedy_chain(sub, features)
    is_k, oos_k = is_oos(kept)
    return {
        "setup": setup, "side": side,
        "n_base": len(sub), "pf_base": pf(sub["net_eff"]),
        "n_kept": len(kept), "pf_kept": pf(kept["net_eff"]),
        "win_kept": (kept["outcome"] == "TARGET").mean() * 100 if len(kept) else 0,
        "rs_kept": kept["net_rs"].sum(),
        "is_n": len(is_k), "is_pf": pf(is_k["net_eff"]),
        "oos_n": len(oos_k), "oos_pf": pf(oos_k["net_eff"]),
        "chain": chain, "kept_df": kept,
    }


def main():
    df, path = load()
    print("=" * 100)
    print(f"v17r_nonf analyzer — {len(df)} trades | honest per-row costs, {LEVERAGE}x, sizing-aware")
    print(f"  overall PF={pf(df['net_eff']):.3f}  sumRs={_f(df['net_rs'].sum())}  "
          f"days={df['trade_date'].nunique()}  IS<= {IS_END}")
    # MANDATORY universe gate: long_tail round-trip cost ~0.51% kills the 0.8/0.75
    # geometry outright (break-even win rate ~82%). Drop it before mining.
    n0 = len(df)
    df = df[df["adv_bucket"].isin(["mid", "top100"])].copy()
    print(f"  ADV gate (mid+top100 only): {n0} -> {len(df)}  "
          f"PF={pf(df['net_eff']):.3f}  sumRs={_f(df['net_rs'].sum())}")
    print("=" * 100)

    pairs = df.groupby(["side", "setup"]).size().reset_index()[["side", "setup"]].values.tolist()

    for profile, feats in [("CAUSAL", CAUSAL_FEATURES),
                           ("VOLUME", CAUSAL_FEATURES + [VOLUME_FEATURE])]:
        print(f"\n{'#'*100}\n# PROFILE = {profile}\n{'#'*100}")
        results = []
        for side, setup in pairs:
            r = analyze_setup(df, setup, side, feats)
            if r:
                results.append(r)

        print(f"\n{'side':<6} {'setup':<34} {'n_base':>7} {'pf_b':>6} "
              f"{'n_kept':>7} {'pf_k':>6} {'win%':>6} {'IS_pf':>6} {'OOS_pf':>7} {'OOS_n':>6}  chain")
        kept_setups = []
        for r in sorted(results, key=lambda x: -x["pf_kept"]):
            keep = r["pf_kept"] >= KEEP_PF_MIN and r["oos_n"] >= 10
            mark = "KEEP" if keep else "drop"
            print(f"{r['side']:<6} {r['setup']:<34} {r['n_base']:>7} {r['pf_base']:>6.2f} "
                  f"{r['n_kept']:>7} {r['pf_kept']:>6.2f} {r['win_kept']:>6.1f} "
                  f"{r['is_pf']:>6.2f} {r['oos_pf']:>7.2f} {r['oos_n']:>6}  [{mark}] {fmt_chain(r['chain'])}")
            if keep:
                kept_setups.append(r)

        if kept_setups:
            agg = pd.concat([r["kept_df"] for r in kept_setups], ignore_index=True)
            is_a, oos_a = is_oos(agg)
            print(f"\n  >>> {profile} AGGREGATE CANDIDATE ({len(kept_setups)} setups kept) <<<")
            print(f"      n={len(agg)}  PF={pf(agg['net_eff']):.3f}  "
                  f"win%={(agg['outcome']=='TARGET').mean()*100:.1f}  "
                  f"sumRs={_f(agg['net_rs'].sum())}")
            print(f"      IS  PF={pf(is_a['net_eff']):.3f} (n={len(is_a)})   "
                  f"OOS PF={pf(oos_a['net_eff']):.3f} (n={len(oos_a)})   "
                  f"decay={pf(oos_a['net_eff'])/max(pf(is_a['net_eff']),1e-9):.2f}")
            # monthly stability
            agg["_m"] = pd.to_datetime(agg["trade_date"]).dt.to_period("M")
            pos = sum(1 for _, g in agg.groupby("_m") if pf(g["net_eff"]) >= 1.0)
            tot = agg["_m"].nunique()
            print(f"      months PF>=1.0: {pos}/{tot}")
            # emit the runner rule spec (drop-rules: the COMPLEMENT of the keep chain)
            print(f"\n      --- runner KEEP-chain spec ({profile}) ---")
            for r in kept_setups:
                print(f"      {r['side']:<6} {r['setup']:<34} {fmt_chain(r['chain'])}")


if __name__ == "__main__":
    main()
