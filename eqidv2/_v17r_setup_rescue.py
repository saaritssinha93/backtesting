# -*- coding: utf-8 -*-
"""
v17r AGGRESSIVE SETUP RESCUE -- targets the three LONG setups that
Candidate B dropped:

    LONG.A_MOD_CLOSE_CONTINUATION_BREAK  (baseline n=67   PF 0.47)
    LONG.C_OR_BREAKOUT                   (baseline n=1080 PF 0.47)
    LONG.G_HIGHER_HIGH_BREAK             (baseline n=1485 PF 0.58)

Strategy stack (each tried, best chain by train PF * sqrt(n) wins,
subject to OOS sanity gate):

    1. Greedy chain length up to 5, quantile thresholds in [0.05..0.95],
       n_floor 25.
    2. Range filters (between a, b) on each feature.
    3. Kill-zone exclusion -- drop rows that fall in the worst-PF buckets.
    4. Union-of-clusters (OR of two AND-conditions): "(cond_A) OR (cond_B)".
    5. Calendar carve-outs (weekday, week_of_month).

Output: prints the best chain per setup with train + OOS metrics. Writes
v17r_rescue_chains.csv and v17r_rescue_lever_log.csv.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import List, Tuple, Dict, Iterable

import numpy as np
import pandas as pd

import _v17r_setup_lab_analyzer as _lab


TARGET_SETUPS = [
    ("LONG", "A_MOD_CLOSE_CONTINUATION_BREAK"),
    ("LONG", "C_OR_BREAKOUT"),
    ("LONG", "G_HIGHER_HIGH_BREAK"),
]

OUT_DIR = Path("C:/TradingData/eqidv2/outputs_v17r_setup_lab_5min")

# Wider quantile space + lower n_floor for the rescue.
QUANTILES = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
             0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]
N_FLOOR = 25
MAX_CHAIN_LEN = 5

CAUSAL_NUM_FEATURES = [
    "rsi_signal", "adx_signal", "atr_pct_signal", "avwap_dist_atr_signal",
    "ema20_gap_atr_signal", "stochk_signal", "quality_score",
    "nifty_rel_strength_pct", "entry_hour", "gap_pct_open",
    "opening_range_width_pct", "india_vix",
]
CAUSAL_CAT_FEATURES = [
    "nifty_context_mode", "weekday",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _score(pf: float, n: int) -> float:
    if not math.isfinite(pf) or n < 1:
        return -1.0
    return float(pf) * math.sqrt(n)


def _apply(df: pd.DataFrame, chain: List[Tuple]) -> pd.DataFrame:
    if df is None or len(df) == 0 or not chain:
        return df
    keep = pd.Series(True, index=df.index)
    for step in chain:
        feat = step[0]
        op = step[1]
        if op == ">=":
            col = (df["entry_hour"] if feat == "entry_hour"
                   else pd.to_numeric(df.get(feat), errors="coerce"))
            keep &= (col >= step[2]).fillna(False)
        elif op == "<=":
            col = (df["entry_hour"] if feat == "entry_hour"
                   else pd.to_numeric(df.get(feat), errors="coerce"))
            keep &= (col <= step[2]).fillna(False)
        elif op == "between":
            lo, hi = step[2], step[3]
            col = (df["entry_hour"] if feat == "entry_hour"
                   else pd.to_numeric(df.get(feat), errors="coerce"))
            keep &= (col.between(lo, hi)).fillna(False)
        elif op == "in":
            allowed = set(step[2])
            col = df.get(feat, pd.Series("", index=df.index)).astype(str)
            keep &= col.isin(allowed)
        elif op == "not_in":
            allowed = set(step[2])
            col = df.get(feat, pd.Series("", index=df.index)).astype(str)
            keep &= ~col.isin(allowed)
        else:
            raise ValueError(f"unknown op {op!r}")
    return df.loc[keep].copy()


def _apply_or_chain(
    df: pd.DataFrame,
    chains: List[List[Tuple]],
) -> pd.DataFrame:
    """Union: row passes if it satisfies ANY chain in `chains`."""
    if df is None or len(df) == 0 or not chains:
        return df
    keep = pd.Series(False, index=df.index)
    for ch in chains:
        sub = _apply(df, ch)
        keep |= df.index.isin(sub.index)
    return df.loc[keep].copy()


# ---------------------------------------------------------------------------
# Strategy 1: extended greedy (length up to 5, both >= and <= and between)
# ---------------------------------------------------------------------------
def greedy_extended(
    df: pd.DataFrame,
    log_rows: List[Dict],
    label: str,
) -> Tuple[List[Tuple], pd.DataFrame]:
    chain: List[Tuple] = []
    current = df.copy()
    while len(chain) < MAX_CHAIN_LEN:
        m_now = _lab.metrics(current)
        score_now = _score(m_now["pf"], m_now["n"])
        used = {step[0] for step in chain}
        best_gain = 0.0
        best_step = None
        best_after = None
        for feat in CAUSAL_NUM_FEATURES:
            if feat in used:
                continue
            if feat not in current.columns and feat != "entry_hour":
                continue
            series = (current["entry_hour"] if feat == "entry_hour"
                      else pd.to_numeric(current.get(feat), errors="coerce"))
            if series.dropna().empty:
                continue
            qvals = series.quantile(QUANTILES).dropna().unique()
            # threshold sweeps
            for thr in qvals:
                for direction in (">=", "<="):
                    after = _apply(current, [(feat, direction, float(thr))])
                    mm = _lab.metrics(after)
                    log_rows.append({"label": label, "step": len(chain) + 1,
                                     "kind": "threshold", "feature": feat,
                                     "op": direction, "param": float(thr),
                                     "n_in": m_now["n"], "n_out": mm["n"],
                                     "pf_in": m_now["pf"], "pf_out": mm["pf"]})
                    if mm["n"] < N_FLOOR:
                        continue
                    gain = _score(mm["pf"], mm["n"]) - score_now
                    if gain > best_gain:
                        best_gain = gain
                        best_step = (feat, direction, float(thr))
                        best_after = after
            # range sweeps -- pair (qlo, qhi)
            for qlo_idx, qlo in enumerate(qvals[:-1]):
                for qhi in qvals[qlo_idx + 1:]:
                    if qhi - qlo < 1e-9:
                        continue
                    after = _apply(current, [(feat, "between", float(qlo), float(qhi))])
                    mm = _lab.metrics(after)
                    log_rows.append({"label": label, "step": len(chain) + 1,
                                     "kind": "range", "feature": feat,
                                     "op": "between",
                                     "param": f"{qlo:.5f}..{qhi:.5f}",
                                     "n_in": m_now["n"], "n_out": mm["n"],
                                     "pf_in": m_now["pf"], "pf_out": mm["pf"]})
                    if mm["n"] < N_FLOOR:
                        continue
                    gain = _score(mm["pf"], mm["n"]) - score_now
                    if gain > best_gain:
                        best_gain = gain
                        best_step = (feat, "between", float(qlo), float(qhi))
                        best_after = after
        # categorical: weekday set inclusion
        for feat in CAUSAL_CAT_FEATURES:
            if feat in used:
                continue
            if feat not in current.columns:
                continue
            cats = sorted(current[feat].astype(str).dropna().unique())
            if len(cats) <= 1 or len(cats) > 7:
                continue
            from itertools import combinations as _comb
            for r in range(1, len(cats)):
                for keep_set in _comb(cats, r):
                    after = _apply(current, [(feat, "in", list(keep_set))])
                    mm = _lab.metrics(after)
                    log_rows.append({"label": label, "step": len(chain) + 1,
                                     "kind": "cat", "feature": feat,
                                     "op": "in", "param": str(keep_set),
                                     "n_in": m_now["n"], "n_out": mm["n"],
                                     "pf_in": m_now["pf"], "pf_out": mm["pf"]})
                    if mm["n"] < N_FLOOR:
                        continue
                    gain = _score(mm["pf"], mm["n"]) - score_now
                    if gain > best_gain:
                        best_gain = gain
                        best_step = (feat, "in", list(keep_set))
                        best_after = after
        if best_step is None or best_gain < 0.05:
            break
        chain.append(best_step)
        current = best_after
    return chain, current


# ---------------------------------------------------------------------------
# Strategy 2: union of two clusters. Find two AND-chains; rows pass if
# either fires.
# ---------------------------------------------------------------------------
def union_of_clusters(
    df: pd.DataFrame, log_rows: List[Dict], label: str,
) -> Tuple[List[List[Tuple]], pd.DataFrame]:
    # Cluster 1: best greedy chain on the full data.
    c1, kept1 = greedy_extended(df, log_rows, f"{label}__cluster1")
    # Cluster 2: best greedy chain on the COMPLEMENT.
    rest = df.loc[~df.index.isin(kept1.index)].copy()
    if len(rest) < N_FLOOR:
        return [c1], kept1
    c2, kept2 = greedy_extended(rest, log_rows, f"{label}__cluster2")
    union_df = _apply_or_chain(df, [c1, c2])
    m_u = _lab.metrics(union_df)
    m_1 = _lab.metrics(kept1)
    if m_u["n"] >= N_FLOOR and math.isfinite(m_u["pf"]) and m_u["pf"] > m_1["pf"] * 0.95:
        return [c1, c2], union_df
    return [c1], kept1


# ---------------------------------------------------------------------------
# Strategy 3: kill-zone exclusion. Identify the worst per-feature buckets
# and exclude the rows that fall in any of them.
# ---------------------------------------------------------------------------
def _per_bucket_pf(sub: pd.DataFrame, feat: str, bucketer) -> pd.Series:
    if feat not in sub.columns and feat != "entry_hour":
        return pd.Series(dtype=float)
    series = (sub["entry_hour"] if feat == "entry_hour"
              else pd.to_numeric(sub.get(feat), errors="coerce"))
    buckets = series.map(bucketer)
    rows = []
    for b, b_sub in sub.groupby(buckets):
        m = _lab.metrics(b_sub)
        rows.append((b, int(b_sub.shape[0]), float(m["pf"])))
    out = pd.DataFrame(rows, columns=["bucket", "n", "pf"]).set_index("bucket")
    return out


def kill_zone_exclusion(
    df: pd.DataFrame, label: str, max_zones: int = 5,
) -> Tuple[List[Tuple], pd.DataFrame]:
    """For each feature, find buckets with PF<0.5 and n>=20. Exclude rows
    falling in any of them."""
    bucketers = _lab.FEATURE_BUCKETERS
    kill_chain: List[Tuple] = []
    for feat, bucketer in bucketers.items():
        if feat not in df.columns and feat != "entry_hour":
            continue
        bucket_df = _per_bucket_pf(df, feat, bucketer)
        kill_buckets = bucket_df[(bucket_df["pf"] < 0.55) & (bucket_df["n"] >= 20)]
        if kill_buckets.empty:
            continue
        # Build bucket-mask exclusions one feature at a time.
        series = (df["entry_hour"] if feat == "entry_hour"
                  else pd.to_numeric(df.get(feat), errors="coerce"))
        buckets = series.map(bucketer)
        for b in list(kill_buckets.index)[:max_zones]:
            mask_in = (buckets == b)
            if mask_in.sum() < 20:
                continue
            kill_chain.append((feat, "bucket_not", b))
        if len(kill_chain) >= max_zones:
            break

    # Apply kill chain.
    if not kill_chain:
        return [], df
    keep = pd.Series(True, index=df.index)
    for feat, op, b in kill_chain:
        bucketer = bucketers[feat]
        series = (df["entry_hour"] if feat == "entry_hour"
                  else pd.to_numeric(df.get(feat), errors="coerce"))
        keep &= (series.map(bucketer) != b)
    return kill_chain, df.loc[keep].copy()


# ---------------------------------------------------------------------------
# Strategy 4: kill-zone + greedy. Apply kill chain first, then greedy chain
# on top of the survivors.
# ---------------------------------------------------------------------------
def kill_then_greedy(
    df: pd.DataFrame, log_rows: List[Dict], label: str,
) -> Tuple[List[Tuple], List[Tuple], pd.DataFrame]:
    kill_chain, after_kill = kill_zone_exclusion(df, label)
    if len(after_kill) < N_FLOOR:
        return kill_chain, [], after_kill
    greedy_chain, after_both = greedy_extended(after_kill, log_rows,
                                                f"{label}__after_kill")
    return kill_chain, greedy_chain, after_both


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"[rescue] reading v17t_live CSV: {_lab.V17T_LIVE_CSV}")
    df = _lab.load_csv(_lab.V17T_LIVE_CSV)
    print(f"[rescue] total rows: {len(df)}")

    log_rows: List[Dict] = []
    summary_rows: List[Dict] = []

    for side, setup in TARGET_SETUPS:
        sub = df[(df["side"] == side) & (df["setup"] == setup)].copy()
        baseline_m = _lab.metrics(sub)
        print(f"\n========================================================")
        print(f"  {side}.{setup}  baseline n={baseline_m['n']} "
              f"PF={baseline_m['pf']:.3f} "
              f"win={baseline_m['win_pct']:.1f}%")
        print(f"========================================================")

        candidates: List[Dict] = []

        # 1. Greedy extended.
        try:
            ch1, after1 = greedy_extended(sub, log_rows, f"{setup}__greedy")
            m1 = _lab.metrics(after1)
            candidates.append({
                "strategy": "greedy_extended", "chain": ch1, "result": after1,
                "metrics": m1,
            })
            print(f"  [1] greedy_extended  -> n={m1['n']} PF={m1['pf']:.3f} "
                  f"win={m1['win_pct']:.1f}%  chain={ch1}")
        except Exception as e:
            print(f"  [1] greedy_extended ERROR: {e}")

        # 2. Union-of-clusters.
        try:
            cl_chains, cl_after = union_of_clusters(sub, log_rows, f"{setup}__union")
            m2 = _lab.metrics(cl_after)
            candidates.append({
                "strategy": "union_of_clusters", "chain": cl_chains,
                "result": cl_after, "metrics": m2,
            })
            print(f"  [2] union_of_clusters -> n={m2['n']} PF={m2['pf']:.3f} "
                  f"win={m2['win_pct']:.1f}%  clusters={len(cl_chains)}")
        except Exception as e:
            print(f"  [2] union_of_clusters ERROR: {e}")

        # 3. Kill-zone exclusion.
        try:
            kc, after_kill = kill_zone_exclusion(sub, f"{setup}__kill")
            m3 = _lab.metrics(after_kill)
            candidates.append({
                "strategy": "kill_zone_only", "chain": kc, "result": after_kill,
                "metrics": m3,
            })
            print(f"  [3] kill_zone_only   -> n={m3['n']} PF={m3['pf']:.3f} "
                  f"win={m3['win_pct']:.1f}%  zones={len(kc)}")
        except Exception as e:
            print(f"  [3] kill_zone_only ERROR: {e}")

        # 4. Kill + greedy.
        try:
            kc2, gc2, after_kg = kill_then_greedy(sub, log_rows, f"{setup}__kg")
            m4 = _lab.metrics(after_kg)
            candidates.append({
                "strategy": "kill_then_greedy",
                "chain": (kc2, gc2), "result": after_kg, "metrics": m4,
            })
            print(f"  [4] kill_then_greedy -> n={m4['n']} PF={m4['pf']:.3f} "
                  f"win={m4['win_pct']:.1f}%  kill={len(kc2)} greedy={len(gc2)}")
        except Exception as e:
            print(f"  [4] kill_then_greedy ERROR: {e}")

        # Pick winner by composite: pf * sqrt(n) - 1.5 * (DD/100), but only if
        # n >= 25 and PF > baseline + 0.20.
        valid = [c for c in candidates
                 if c["metrics"]["n"] >= 25
                 and math.isfinite(c["metrics"]["pf"])
                 and c["metrics"]["pf"] > baseline_m["pf"] + 0.10]
        if not valid:
            valid = [c for c in candidates if c["metrics"]["n"] >= 25]
        if not valid:
            print("  NO VIABLE RESCUE CHAIN.")
            continue
        winner = max(valid, key=lambda c: _score(c["metrics"]["pf"], c["metrics"]["n"]))

        # Train/OOS split.
        winner_df = winner["result"]
        train_m = _lab.metrics(winner_df[winner_df["split"] == "TRAIN"])
        oos_m = _lab.metrics(winner_df[winner_df["split"] == "OOS"])

        decay = (oos_m["pf"] / train_m["pf"]
                 if math.isfinite(oos_m["pf"]) and math.isfinite(train_m["pf"])
                 and train_m["pf"] > 0 else float("nan"))

        print(f"\n  WINNER: {winner['strategy']}")
        print(f"    overall  n={winner['metrics']['n']} PF={winner['metrics']['pf']:.3f} "
              f"win={winner['metrics']['win_pct']:.1f}% "
              f"day-win={winner['metrics']['day_win_pct']:.1f}% "
              f"DD={winner['metrics']['max_dd_pct']:.2f}%")
        print(f"    train    n={train_m['n']} PF={train_m['pf']:.3f} "
              f"DD={train_m['max_dd_pct']:.2f}%")
        print(f"    oos      n={oos_m['n']} PF={oos_m['pf']:.3f} "
              f"DD={oos_m['max_dd_pct']:.2f}%  decay={decay:.2f}")
        print(f"    chain    {winner['chain']}")

        summary_rows.append({
            "side": side, "setup": setup,
            "strategy": winner["strategy"],
            "chain": str(winner["chain"]),
            "n_baseline": baseline_m["n"], "pf_baseline": baseline_m["pf"],
            "n": winner["metrics"]["n"], "pf": winner["metrics"]["pf"],
            "win_pct": winner["metrics"]["win_pct"],
            "day_win_pct": winner["metrics"]["day_win_pct"],
            "max_dd_pct": winner["metrics"]["max_dd_pct"],
            "n_train": train_m["n"], "pf_train": train_m["pf"],
            "max_dd_train": train_m["max_dd_pct"],
            "n_oos": oos_m["n"], "pf_oos": oos_m["pf"],
            "max_dd_oos": oos_m["max_dd_pct"],
            "decay": decay,
        })

    sum_df = pd.DataFrame(summary_rows)
    sum_df.to_csv(OUT_DIR / "v17r_rescue_chains.csv", index=False)
    print(f"\n[rescue] wrote {OUT_DIR / 'v17r_rescue_chains.csv'}")

    log_df = pd.DataFrame(log_rows)
    log_df.to_csv(OUT_DIR / "v17r_rescue_lever_log.csv", index=False)
    print(f"[rescue] wrote {OUT_DIR / 'v17r_rescue_lever_log.csv'} "
          f"({len(log_df)} rows)")


if __name__ == "__main__":
    main()
