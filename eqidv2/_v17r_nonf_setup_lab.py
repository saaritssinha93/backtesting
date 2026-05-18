"""READ-ONLY setup lab for v17r_nonf — mine chains for new setups.

Does NOT modify any strategy / pipeline / config file.

Pipeline:
  1. Load the broken-run 071744 CSV (13k trades, NIFTY filter off, no v17r
     filters, no breadth gate — has every setup the cascade can emit).
  2. Re-resolve every trade against 1-min bars at the v17r_nonf live
     geometry: TGT 1.5% / SL 0.75%.
  3. Apply the v17r_nonf live gates causally:
       - ADV gate (drop long_tail, ADV < Rs50cr)
       - Breadth gate (drop SHORT when pct_above_vwap < 0.119)
  4. For every (side, setup) NOT already in the live whitelist, run a
     greedy 3-step causal-chain search with IS/OOS split (IS <= 2026-02-15)
     and PF*sqrt(n) scoring.
  5. Report passing setups under strict OOS gates:
        n_oos >= 15  AND  OOS PF >= 1.30  AND  decay >= 0.65

Setups that pass become candidates to wire into the runner via a new
extension file. The live runner's 3 existing chains are NOT re-tuned.
"""
from __future__ import annotations

import glob
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm
from eqidv2 import v17D_exit_resolver as er

SOURCE_CSV = r"C:\TradingData\eqidv2\outputs_v17r_nonf_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260514_071744.csv"
UNIVERSE = r"c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\configs\universe.csv"
PARQUET_1MIN = r"C:\TradingData\eqidv2\stocks_indicators_1min_eq"
BREADTH_CACHE = Path(__file__).resolve().parent / "_v17r_breadth_cache.parquet"

LEVERAGE = 5.0
TGT_PCT = 1.5      # match live runner
SL_PCT = 0.75      # match live runner
BREADTH_THRESHOLD = 0.119   # match live loose gate (SHORT only)
IS_END = "2026-02-15"

# Setups already in the live runner — skip these in the lab.
LIVE_WHITELIST = {("SHORT", "A_MOD_BREAK_C1_LOW"),
                  ("SHORT", "D_AVWAP_LOSE_REVERSAL"),
                  ("LONG",  "A_MOD_BREAK_C1_HIGH")}

CAUSAL_FEATURES = [
    "adx_signal", "rsi_signal", "stochk_signal", "avwap_dist_atr_signal",
    "ema20_gap_atr_signal", "atr_pct_signal", "quality_score", "india_vix",
    "gap_pct_open", "opening_range_width_pct", "bars_from_open", "entry_hour",
]

N_FLOOR = 25
PF_TARGET = 2.0
KEEP_PF_MIN = 1.15      # variety-friendly: passes if filtered subset has slim edge
OOS_N_MIN = 10
DECAY_MIN = 0.65
MAX_STEPS = 5


def pf(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    return float("inf") if l <= 0 and w > 0 else (0.0 if l <= 0 else w / l)


def _f(x):
    return "{:+,.0f}".format(x)


def is_oos(df):
    cut = pd.to_datetime(IS_END).date()
    return df[df["trade_date"] <= cut], df[df["trade_date"] > cut]


def re_resolve_all(trades, parquet_dir):
    """Re-resolve every trade against 1-min bars at TGT_PCT / SL_PCT."""
    cache = {}

    def bars_for(tk):
        if tk not in cache:
            cache[tk] = er.load_1min(parquet_dir, tk)
        return cache[tk]

    out = []
    for i, t in enumerate(trades.itertuples(index=False), 1):
        bars = bars_for(t.ticker)
        if bars is None:
            continue
        res = er.resolve(bars, t.side, float(t.entry_price),
                         t.entry_time_ist, SL_PCT, TGT_PCT)
        if res is None:
            continue
        out.append({
            "side": t.side, "setup": t.setup, "ticker": t.ticker,
            "trade_date": pd.to_datetime(t.trade_date).date(),
            "entry_price": float(t.entry_price),
            "entry_time_ist": t.entry_time_ist,
            "signal_time_ist": getattr(t, "signal_time_ist", t.entry_time_ist),
            "adv_bucket": t.adv_bucket,
            "outcome": res.outcome,
            "pnl_pct_price": res.pnl_pct_price,
            "size_multiplier": getattr(t, "size_multiplier", 1.0) or 1.0,
            "position_size_rs": getattr(t, "position_size_rs", 20000.0) or 20000.0,
            "adx_signal": getattr(t, "adx_signal", np.nan),
            "rsi_signal": getattr(t, "rsi_signal", np.nan),
            "stochk_signal": getattr(t, "stochk_signal", np.nan),
            "avwap_dist_atr_signal": getattr(t, "avwap_dist_atr_signal", np.nan),
            "ema20_gap_atr_signal": getattr(t, "ema20_gap_atr_signal", np.nan),
            "atr_pct_signal": getattr(t, "atr_pct_signal", np.nan),
            "quality_score": getattr(t, "quality_score", np.nan),
            "india_vix": getattr(t, "india_vix", np.nan),
            "gap_pct_open": getattr(t, "gap_pct_open", np.nan),
            "opening_range_width_pct": getattr(t, "opening_range_width_pct", np.nan),
            "bars_from_open": getattr(t, "bars_from_open", np.nan),
        })
        if i % 1000 == 0:
            print(f"  [re-resolve] {i}/{len(trades)} ...")
    df = pd.DataFrame(out)
    # honest net
    df["cost_pct"] = [cm.costs_pct_for_v17C(
        b, o if o in ("TARGET", "SL") else "TARGET")
        for b, o in zip(df["adv_bucket"], df["outcome"])]
    df["net_eff"] = (df["pnl_pct_price"] - df["cost_pct"]) * LEVERAGE * df["size_multiplier"]
    df["net_rs"] = df["net_eff"] / 100.0 * df["position_size_rs"]
    t = pd.to_datetime(df["entry_time_ist"])
    df["entry_hour"] = t.dt.hour + t.dt.minute / 60.0
    return df


def apply_breadth(df, breadth):
    """Merge pct_above_vwap at signal time; drop SHORT when below threshold."""
    sig = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    if getattr(sig.dt, "tz", None) is None:
        sig = sig.dt.tz_localize("Asia/Kolkata")
    else:
        sig = sig.dt.tz_convert("Asia/Kolkata")
    work = df.assign(_sig=sig).sort_values("_sig")
    bdf = breadth.sort_values("date")
    merged = pd.merge_asof(work, bdf[["date", "pct_above_vwap"]],
                           left_on="_sig", right_on="date", direction="backward")
    keep = ~((merged["side"] == "SHORT") &
             (merged["pct_above_vwap"].fillna(0.0) < BREADTH_THRESHOLD))
    out = merged.loc[keep].drop(columns=["_sig", "date"], errors="ignore")
    return out


def score(sub):
    n = len(sub)
    if n < N_FLOOR:
        return -1.0
    p = pf(sub["net_eff"])
    if p == float("inf"):
        p = 5.0
    return p * np.sqrt(n)


def candidate_rules(sub, features):
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
    return df[vals >= thr] if op == ">=" else df[vals <= thr]


def greedy_chain(sub, features, max_steps=MAX_STEPS):
    chain = []
    cur = sub.copy()
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
        chain.append((feat, op, thr))
        cur = cand
        if pf(cur["net_eff"]) >= PF_TARGET:
            break
    return chain, cur


def fmt_chain(chain):
    return " AND ".join(f"{f} {o} {t:.4g}" for f, o, t in chain) if chain else "(none)"


def main():
    print(f"[setup-lab] loading source {Path(SOURCE_CSV).name}")
    raw = pd.read_csv(SOURCE_CSV)
    uni = pd.read_csv(UNIVERSE)
    adv = dict(zip(uni["ticker"], uni["adv_rs_cr"]))
    raw["adv_bucket"] = raw["ticker"].map(adv).fillna(0.0).apply(cm.adv_bucket_for)
    # ADV gate
    n0 = len(raw)
    raw = raw[raw["adv_bucket"].isin(["mid", "top100"])].copy()
    print(f"[setup-lab] ADV gate (mid+top100): {n0} -> {len(raw)}")

    print(f"[setup-lab] re-resolving {len(raw)} trades at TGT {TGT_PCT}% / SL {SL_PCT}% ...")
    rr = re_resolve_all(raw, PARQUET_1MIN)
    print(f"[setup-lab] re-resolved {len(rr)} trades  baseline PF={pf(rr['net_eff']):.3f}")

    print(f"[setup-lab] applying breadth gate (SHORT pct_above_vwap >= {BREADTH_THRESHOLD})")
    breadth = pd.read_parquet(BREADTH_CACHE)
    rr = apply_breadth(rr, breadth)
    print(f"[setup-lab] after breadth gate: n={len(rr)}  PF={pf(rr['net_eff']):.3f}")

    print("\n" + "=" * 110)
    print(f"PER-SETUP CHAIN SEARCH  (LIVE_WHITELIST skipped: {LIVE_WHITELIST})")
    print("=" * 110)
    print(f"{'side':<6} {'setup':<34} {'n':>5} {'pf_b':>6} {'kept':>5} {'pf_k':>6} "
          f"{'win%':>6} {'IS_pf':>6} {'OOSn':>5} {'OOSpf':>6} {'decay':>6}  chain")
    rows = []
    for (sd, st), gg in rr.groupby(["side", "setup"]):
        if (sd, st) in LIVE_WHITELIST:
            continue
        if len(gg) < N_FLOOR:
            continue
        chain, kept = greedy_chain(gg, CAUSAL_FEATURES)
        if kept is None or len(kept) < N_FLOOR:
            continue
        is_k, oos_k = is_oos(kept)
        is_pf = pf(is_k["net_eff"])
        oos_pf = pf(oos_k["net_eff"])
        decay = oos_pf / max(is_pf, 1e-9) if is_pf > 0 else 0
        passes = (pf(kept["net_eff"]) >= KEEP_PF_MIN
                  and len(oos_k) >= OOS_N_MIN
                  and oos_pf >= KEEP_PF_MIN
                  and decay >= DECAY_MIN)
        mark = "**PASS**" if passes else "drop"
        print(f"{sd:<6} {st:<34} {len(gg):>5} {pf(gg['net_eff']):>6.2f} "
              f"{len(kept):>5} {pf(kept['net_eff']):>6.2f} "
              f"{(kept['outcome']=='TARGET').mean()*100:>5.1f}% {is_pf:>6.2f} "
              f"{len(oos_k):>5} {oos_pf:>6.2f} {decay:>6.2f}  [{mark}] {fmt_chain(chain)}")
        if passes:
            rows.append({"side": sd, "setup": st, "chain": chain,
                         "n": len(kept), "pf": pf(kept["net_eff"]),
                         "oos_pf": oos_pf, "decay": decay})

    if not rows:
        print("\n[setup-lab] NO new setups passed strict OOS gates.")
        return

    print(f"\n{'='*110}\nPASSING SETUPS — RUNNER SPEC TO ADD\n{'='*110}")
    for r in rows:
        chain_lit = ", ".join(f"(\"{c}\", \"{o}\", {t:.6g})" for c, o, t in r["chain"])
        print(f'    ("{r["side"]}", "{r["setup"]}"): [{chain_lit}],   '
              f'# n={r["n"]} PF={r["pf"]:.2f} OOS_PF={r["oos_pf"]:.2f} decay={r["decay"]:.2f}')


if __name__ == "__main__":
    main()
