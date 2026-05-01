# -*- coding: utf-8 -*-
"""
v17r AGGRESSIVE SETUP RESCUE v2 -- pushes harder on the three dropped
LONG setups, with a STRICT causality contract:

    Ticker eligibility is computed CAUSALLY -- only past trades on
    (setup, ticker) up to D-1 are used. No future leak.

Strategies tried:
    1. Greedy extended (length up to 5, range filters allowed)
    2. Greedy + causal-trailing-window ticker eligibility (require
       (setup, ticker) trailing 90-day cumulative PnL > 0 with n_prior >= 2)
    3. Greedy + entry-hour fixed buckets
    4. Brute 2-feature AND of best buckets

The trailing-window ticker filter is causal: at each (date, ticker), it
asks "did this (setup, ticker) generate positive PnL in the previous
N days, with at least M prior trades?" -- which is information available
at signal-time-1.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import List, Tuple, Dict
from itertools import combinations

import numpy as np
import pandas as pd

import _v17r_setup_lab_analyzer as L


TARGET_SETUPS = [
    ("LONG", "A_MOD_CLOSE_CONTINUATION_BREAK"),
    ("LONG", "C_OR_BREAKOUT"),
    ("LONG", "G_HIGHER_HIGH_BREAK"),
]

OUT_DIR = Path("C:/TradingData/eqidv2/outputs_v17r_setup_lab_5min")


# ---------------------------------------------------------------------------
# CAUSAL trailing-window ticker eligibility.
#
# For each row at (trade_date D, ticker T, setup S), compute the cumulative
# PnL of trades with the SAME (S, T) where trade_date < D and trade_date
# >= D - WINDOW_DAYS, requiring at least MIN_PRIOR trades. Row is eligible
# iff that cumulative PnL > 0.
#
# This is the ONLY ticker-level filter that's causal -- it uses past trades
# only, never the trade itself or future trades.
# ---------------------------------------------------------------------------
def add_trailing_ticker_eligibility(
    df: pd.DataFrame,
    side: str,
    setup: str,
    window_days: int = 90,
    min_prior: int = 2,
) -> pd.DataFrame:
    """Adds a boolean column `_ticker_eligible` based on trailing-window
    (setup, ticker) PnL. Only rows of (side, setup) are evaluated; others
    are left untouched."""
    df = df.copy()
    df["_ticker_eligible"] = False
    if "_ticker_eligible" in df.columns and "trade_date_dt" not in df.columns:
        df["trade_date_dt"] = pd.to_datetime(df["trade_date"])

    sub = df[(df["side"] == side) & (df["setup"] == setup)].sort_values("trade_date_dt").copy()
    pnl = pd.to_numeric(sub["pnl_pct_price"], errors="coerce").fillna(0.0)
    dates = sub["trade_date_dt"].values
    tickers = sub["ticker"].astype(str).values

    elig = np.zeros(len(sub), dtype=bool)
    # Per-ticker rolling window: pre-build (date, pnl) lists per ticker.
    from collections import defaultdict
    history: Dict[str, List[Tuple[np.datetime64, float]]] = defaultdict(list)

    for i in range(len(sub)):
        t = tickers[i]
        d = dates[i]
        cutoff = d - np.timedelta64(window_days, "D")
        # Drop entries older than cutoff.
        h = [x for x in history[t] if x[0] >= cutoff]
        history[t] = h
        if len(h) >= min_prior and sum(p for _, p in h) > 0:
            elig[i] = True
        # Append THIS trade for FUTURE rows (after eligibility decision).
        history[t].append((d, float(pnl.iloc[i])))

    df.loc[sub.index, "_ticker_eligible"] = elig
    return df


# ---------------------------------------------------------------------------
# Score helpers
# ---------------------------------------------------------------------------
def score(pf: float, n: int) -> float:
    if not math.isfinite(pf) or n < 1:
        return -1.0
    return float(pf) * math.sqrt(n)


def apply_chain(df, chain):
    if df is None or len(df) == 0 or not chain:
        return df
    keep = pd.Series(True, index=df.index)
    for step in chain:
        feat, op = step[0], step[1]
        if op == "ticker_eligible":
            keep &= df.get("_ticker_eligible", pd.Series(False, index=df.index)).fillna(False)
            continue
        col = (df["entry_hour"] if feat == "entry_hour"
               else pd.to_numeric(df.get(feat), errors="coerce"))
        if op == ">=":
            keep &= (col >= step[2]).fillna(False)
        elif op == "<=":
            keep &= (col <= step[2]).fillna(False)
        elif op == "between":
            keep &= col.between(step[2], step[3]).fillna(False)
        elif op == "in":
            sval = df.get(feat, pd.Series("", index=df.index)).astype(str)
            keep &= sval.isin(set(step[2]))
    return df.loc[keep].copy()


# ---------------------------------------------------------------------------
# Brute 2-feature AND on best per-bucket pairs
# ---------------------------------------------------------------------------
def brute_pair(sub: pd.DataFrame, log_rows: List[Dict], label: str):
    """Find the best 2-feature AND-intersection of (feature, bucket) pairs
    where each bucket has PF >= 1.3, n >= 15."""
    bucketers = L.FEATURE_BUCKETERS
    pos_buckets: List[Tuple[str, str, pd.Index]] = []
    for feat, b in bucketers.items():
        if feat not in sub.columns and feat != "entry_hour":
            continue
        series = (sub["entry_hour"] if feat == "entry_hour"
                  else pd.to_numeric(sub.get(feat), errors="coerce"))
        bset = series.map(b)
        for bk, bs in sub.groupby(bset):
            m = L.metrics(bs)
            if m["n"] >= 15 and math.isfinite(m["pf"]) and m["pf"] >= 1.3:
                pos_buckets.append((feat, bk, bs.index))
    if len(pos_buckets) < 2:
        return None, sub
    # Pair-wise AND.
    best = None
    for (f1, b1, idx1), (f2, b2, idx2) in combinations(pos_buckets, 2):
        if f1 == f2:
            continue
        joint = idx1.intersection(idx2)
        if len(joint) < 20:
            continue
        sub_j = sub.loc[joint]
        m = L.metrics(sub_j)
        log_rows.append({"label": label, "kind": "brute_pair",
                         "f1": f1, "b1": b1, "f2": f2, "b2": b2,
                         "n": m["n"], "pf": m["pf"], "win": m["win_pct"]})
        if math.isfinite(m["pf"]) and m["pf"] >= 1.3:
            cand = {"chain": [(f1, "bucket", b1), (f2, "bucket", b2)],
                    "result": sub_j, "metrics": m}
            if best is None or score(m["pf"], m["n"]) > score(best["metrics"]["pf"], best["metrics"]["n"]):
                best = cand
    if best is None:
        return None, sub
    return best, best["result"]


# ---------------------------------------------------------------------------
# Greedy with ticker-eligibility added as a candidate filter
# ---------------------------------------------------------------------------
QUANTILES = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
             0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
N_FLOOR = 25
MAX_LEN = 5


def greedy_with_ticker(
    df: pd.DataFrame, log_rows: List[Dict], label: str,
) -> Tuple[List, pd.DataFrame]:
    chain = []
    cur = df.copy()
    while len(chain) < MAX_LEN:
        m_now = L.metrics(cur)
        s_now = score(m_now["pf"], m_now["n"])
        used = {step[0] for step in chain}
        best_gain = 0.0
        best_step = None
        best_after = None
        # Add ticker_eligible as a candidate step once.
        if "ticker_eligible" not in used and "_ticker_eligible" in cur.columns:
            after = apply_chain(cur, [("ticker_eligible", "ticker_eligible")])
            mm = L.metrics(after)
            log_rows.append({"label": label, "step": len(chain) + 1,
                             "kind": "ticker_eligible", "feature": "_te",
                             "op": "te", "param": "trailing90d",
                             "n_in": m_now["n"], "n_out": mm["n"],
                             "pf_in": m_now["pf"], "pf_out": mm["pf"]})
            if mm["n"] >= N_FLOOR:
                gain = score(mm["pf"], mm["n"]) - s_now
                if gain > best_gain:
                    best_gain = gain
                    best_step = ("ticker_eligible", "ticker_eligible")
                    best_after = after
        # Continuous features.
        for feat in ["rsi_signal", "adx_signal", "atr_pct_signal",
                     "avwap_dist_atr_signal", "ema20_gap_atr_signal",
                     "stochk_signal", "quality_score",
                     "nifty_rel_strength_pct", "entry_hour", "gap_pct_open",
                     "opening_range_width_pct", "india_vix"]:
            if feat in used or (feat not in cur.columns and feat != "entry_hour"):
                continue
            series = (cur["entry_hour"] if feat == "entry_hour"
                      else pd.to_numeric(cur.get(feat), errors="coerce"))
            if series.dropna().empty:
                continue
            qvals = series.quantile(QUANTILES).dropna().unique()
            for thr in qvals:
                for op in (">=", "<="):
                    after = apply_chain(cur, [(feat, op, float(thr))])
                    mm = L.metrics(after)
                    log_rows.append({"label": label, "step": len(chain) + 1,
                                     "kind": "thr", "feature": feat, "op": op,
                                     "param": float(thr),
                                     "n_in": m_now["n"], "n_out": mm["n"],
                                     "pf_in": m_now["pf"], "pf_out": mm["pf"]})
                    if mm["n"] < N_FLOOR:
                        continue
                    gain = score(mm["pf"], mm["n"]) - s_now
                    if gain > best_gain:
                        best_gain = gain
                        best_step = (feat, op, float(thr))
                        best_after = after
            for i, lo in enumerate(qvals[:-1]):
                for hi in qvals[i + 1:]:
                    after = apply_chain(cur, [(feat, "between", float(lo), float(hi))])
                    mm = L.metrics(after)
                    if mm["n"] < N_FLOOR:
                        continue
                    gain = score(mm["pf"], mm["n"]) - s_now
                    if gain > best_gain:
                        best_gain = gain
                        best_step = (feat, "between", float(lo), float(hi))
                        best_after = after
        if best_step is None or best_gain < 0.05:
            break
        chain.append(best_step)
        cur = best_after
    return chain, cur


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"[rescue v2] reading {L.V17T_LIVE_CSV}")
    df = L.load_csv(L.V17T_LIVE_CSV)
    df["trade_date_dt"] = pd.to_datetime(df["trade_date"])

    log_rows: List[Dict] = []
    sum_rows: List[Dict] = []

    for side, setup in TARGET_SETUPS:
        sub_raw = df[(df["side"] == side) & (df["setup"] == setup)].copy()
        baseline = L.metrics(sub_raw)
        print(f"\n{'='*70}")
        print(f"  {side}.{setup}  baseline n={baseline['n']} PF={baseline['pf']:.3f} "
              f"win={baseline['win_pct']:.1f}%")
        print('='*70)

        # Add trailing-window ticker eligibility.
        df_with_te = add_trailing_ticker_eligibility(df, side, setup,
                                                     window_days=90, min_prior=2)
        sub = df_with_te[(df_with_te["side"] == side)
                          & (df_with_te["setup"] == setup)].copy()
        n_te = int(sub["_ticker_eligible"].sum())
        print(f"  trailing-90d ticker-eligible rows: {n_te}/{len(sub)}")

        # Strategy A: ticker-eligibility ONLY.
        sub_te = sub[sub["_ticker_eligible"]]
        m_te = L.metrics(sub_te)
        tr_te = L.metrics(sub_te[sub_te["split"] == "TRAIN"])
        oo_te = L.metrics(sub_te[sub_te["split"] == "OOS"])
        decay_te = (oo_te["pf"] / tr_te["pf"]
                    if math.isfinite(oo_te["pf"]) and math.isfinite(tr_te["pf"])
                    and tr_te["pf"] > 0 else float("nan"))
        print(f"  [A] te_only           n={m_te['n']} PF={m_te['pf']:.2f} "
              f"win={m_te['win_pct']:.1f}% | train n={tr_te['n']} PF={tr_te['pf']:.2f} "
              f"| oos n={oo_te['n']} PF={oo_te['pf']:.2f} decay={decay_te:.2f}")

        # Strategy B: greedy on full data (no TE).
        cb, kb = greedy_with_ticker(sub, log_rows, f"{setup}__greedy")
        m_b = L.metrics(kb)
        tr_b = L.metrics(kb[kb["split"] == "TRAIN"])
        oo_b = L.metrics(kb[kb["split"] == "OOS"])
        decay_b = (oo_b["pf"] / tr_b["pf"]
                   if math.isfinite(oo_b["pf"]) and math.isfinite(tr_b["pf"])
                   and tr_b["pf"] > 0 else float("nan"))
        print(f"  [B] greedy            n={m_b['n']} PF={m_b['pf']:.2f} "
              f"win={m_b['win_pct']:.1f}% | train n={tr_b['n']} PF={tr_b['pf']:.2f} "
              f"| oos n={oo_b['n']} PF={oo_b['pf']:.2f} decay={decay_b:.2f}")
        print(f"      chain: {cb}")

        # Strategy C: greedy seeded with ticker_eligible.
        sub_seeded = sub.copy()
        cc_pre = [("ticker_eligible", "ticker_eligible")]
        sub_after_te = apply_chain(sub_seeded, cc_pre)
        cc, kc_full = greedy_with_ticker(sub_after_te, log_rows, f"{setup}__te_then_greedy")
        kc = kc_full
        m_c = L.metrics(kc)
        tr_c = L.metrics(kc[kc["split"] == "TRAIN"])
        oo_c = L.metrics(kc[kc["split"] == "OOS"])
        decay_c = (oo_c["pf"] / tr_c["pf"]
                   if math.isfinite(oo_c["pf"]) and math.isfinite(tr_c["pf"])
                   and tr_c["pf"] > 0 else float("nan"))
        print(f"  [C] te+greedy         n={m_c['n']} PF={m_c['pf']:.2f} "
              f"win={m_c['win_pct']:.1f}% | train n={tr_c['n']} PF={tr_c['pf']:.2f} "
              f"| oos n={oo_c['n']} PF={oo_c['pf']:.2f} decay={decay_c:.2f}")
        print(f"      chain: te + {cc}")

        # Strategy D: brute pair on positive buckets.
        bp, kbp = brute_pair(sub, log_rows, f"{setup}__pair")
        if bp is not None:
            kbp_v = kbp
            m_d = L.metrics(kbp_v)
            tr_d = L.metrics(kbp_v[kbp_v["split"] == "TRAIN"])
            oo_d = L.metrics(kbp_v[kbp_v["split"] == "OOS"])
            decay_d = (oo_d["pf"] / tr_d["pf"]
                       if math.isfinite(oo_d["pf"]) and math.isfinite(tr_d["pf"])
                       and tr_d["pf"] > 0 else float("nan"))
            print(f"  [D] brute pair        n={m_d['n']} PF={m_d['pf']:.2f} "
                  f"win={m_d['win_pct']:.1f}% | train n={tr_d['n']} PF={tr_d['pf']:.2f} "
                  f"| oos n={oo_d['n']} PF={oo_d['pf']:.2f} decay={decay_d:.2f}")
            print(f"      buckets: {bp['chain']}")
        else:
            m_d = tr_d = oo_d = {"n": 0, "pf": float("nan")}
            decay_d = float("nan")
            bp = None
            print(f"  [D] brute pair        no qualifying intersection")

        # Pick winner: best train PF*sqrt(n), with n_train >= 25, n_oos >= 8,
        # decay >= 0.55.
        cands = [
            ("te_only", m_te, tr_te, oo_te, decay_te, "trailing-90d ticker_eligible"),
            ("greedy", m_b, tr_b, oo_b, decay_b, str(cb)),
            ("te+greedy", m_c, tr_c, oo_c, decay_c, "te + " + str(cc)),
        ]
        if bp is not None:
            cands.append(("brute_pair", m_d, tr_d, oo_d, decay_d, str(bp["chain"])))
        viable = [c for c in cands
                  if c[2]["n"] >= 25 and c[3]["n"] >= 8
                  and math.isfinite(c[2]["pf"]) and math.isfinite(c[3]["pf"])
                  and c[2]["pf"] >= 1.20 and c[3]["pf"] >= 1.10
                  and (math.isfinite(c[4]) and c[4] >= 0.55)]
        winner = (max(viable, key=lambda c: score(c[1]["pf"], c[1]["n"]))
                  if viable else None)

        if winner is None:
            print(f"\n  *** NO RESCUE PASSES STRICT GATES (train PF >= 1.20, "
                  f"OOS PF >= 1.10, OOS n >= 8, decay >= 0.55) ***")
            # Show best by train PF*sqrt(n) for transparency, but mark it.
            scored = [c for c in cands if c[2]["n"] >= 25
                      and math.isfinite(c[1]["pf"])]
            if scored:
                best_unsafe = max(scored, key=lambda c: score(c[1]["pf"], c[1]["n"]))
                print(f"  best by train score (UNSAFE OOS): {best_unsafe[0]} "
                      f"n={best_unsafe[1]['n']} PF={best_unsafe[1]['pf']:.2f} "
                      f"oos PF={best_unsafe[3]['pf']:.2f} decay={best_unsafe[4]:.2f}")
                sum_rows.append({
                    "side": side, "setup": setup, "verdict": "NO_VIABLE_RESCUE",
                    "best_strategy": best_unsafe[0],
                    "n_baseline": baseline["n"], "pf_baseline": baseline["pf"],
                    "n_filt": best_unsafe[1]["n"], "pf_filt": best_unsafe[1]["pf"],
                    "n_train": best_unsafe[2]["n"], "pf_train": best_unsafe[2]["pf"],
                    "n_oos": best_unsafe[3]["n"], "pf_oos": best_unsafe[3]["pf"],
                    "decay": best_unsafe[4], "chain": best_unsafe[5],
                })
            continue

        print(f"\n  WINNER: {winner[0]}")
        print(f"    overall n={winner[1]['n']} PF={winner[1]['pf']:.2f} "
              f"win={winner[1]['win_pct']:.1f}% DD={winner[1]['max_dd_pct']:.2f}%")
        print(f"    train   n={winner[2]['n']} PF={winner[2]['pf']:.2f} "
              f"DD={winner[2]['max_dd_pct']:.2f}%")
        print(f"    oos     n={winner[3]['n']} PF={winner[3]['pf']:.2f} "
              f"DD={winner[3]['max_dd_pct']:.2f}% decay={winner[4]:.2f}")
        print(f"    chain   {winner[5]}")

        sum_rows.append({
            "side": side, "setup": setup, "verdict": "RESCUE_OK",
            "best_strategy": winner[0],
            "n_baseline": baseline["n"], "pf_baseline": baseline["pf"],
            "n_filt": winner[1]["n"], "pf_filt": winner[1]["pf"],
            "n_train": winner[2]["n"], "pf_train": winner[2]["pf"],
            "n_oos": winner[3]["n"], "pf_oos": winner[3]["pf"],
            "decay": winner[4], "chain": winner[5],
        })

    pd.DataFrame(sum_rows).to_csv(OUT_DIR / "v17r_rescue_v2_chains.csv", index=False)
    pd.DataFrame(log_rows).to_csv(OUT_DIR / "v17r_rescue_v2_lever_log.csv", index=False)
    print(f"\n[rescue v2] wrote v17r_rescue_v2_chains.csv "
          f"({len(sum_rows)} rows) and v17r_rescue_v2_lever_log.csv "
          f"({len(log_rows)} rows)")


if __name__ == "__main__":
    main()
