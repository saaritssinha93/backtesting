# -*- coding: utf-8 -*-
"""
v17t_live HIGH-PF RELAXATION optimizer.

Targets the deep-filter setups currently sitting at PF > 2.5 with low trade
counts. For each, search a relaxation grid over the existing filter chain
to find the LOOSEST combination of thresholds that still produces PF >= 2.0.

For each setup's current threshold list, every threshold can be:
  - kept as-is                    (factor 0.00 = strict)
  - relaxed 25%  toward "no constraint"
  - relaxed 50%
  - relaxed 75%
  - removed entirely               (factor 1.00 = no constraint)

Cartesian product over thresholds. Pick the combination with maximum n
subject to PF >= TARGET_PF on the kept rows. PF target cascade:
[2.0, 1.85, 1.70] (so we never accept worse than 1.70 even when relaxing).

Output:
  run5_relaxed_high_pf_filters.csv  (one row per setup with relaxed chain)
  run5_relaxed_selected_trades.csv  (aggregate post-relaxation)
  Plus a Python literal dict for v17t_live Phase 5d.
"""
from __future__ import annotations

import sys
import itertools
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path(r"C:/TradingData/eqidv2/outputs_v17q_5min")
RUN5_CSV = OUT_DIR / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_143701.csv"

# PF cascade -- accept the LOOSEST combo whose PF >= these targets in order
PF_TARGETS = [2.0, 1.85, 1.70]
N_FLOOR = 12

# Current Phase 5d filter chains (from _v17t_live_deep_optimizer output)
CURRENT_CHAINS = {
    ("LONG",  "A_MOD_BREAK_C1_HIGH"):
        [("avwap_dist_atr_signal", ">=", 2.5473)],
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"):
        [("stochk_signal", ">=", 93.7635), ("entry_hour", "<=", 11.6417)],
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"):
        [("avwap_dist_atr_signal", "<=", 1.7149)],
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"):
        [("quality_score", ">=", 9.6570)],
    ("LONG",  "C_OR_BREAKOUT"):
        [("quality_score", ">=", 2.3487),
         ("atr_pct_signal", "<=", 0.0055),
         ("avwap_dist_atr_signal", ">=", 1.9958)],
    ("LONG",  "D_EMA20_BOUNCE"):
        [("quality_score", ">=", 2.1436), ("adx_signal", "<=", 29.3609)],
    ("LONG",  "G_HIGHER_HIGH_BREAK"):
        [("quality_score", ">=", 2.1540),
         ("entry_hour", "<=", 10.4167),
         ("avwap_dist_atr_signal", ">=", 1.9995)],
    ("SHORT", "A_MOD_BREAK_C1_LOW"):
        [("quality_score", "<=", 0.4121)],
    ("SHORT", "C_OR_BREAKDOWN"):
        [("avwap_dist_atr_signal", ">=", 1.7857),
         ("atr_pct_signal", ">=", 0.0066)],
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):
        [("avwap_dist_atr_signal", ">=", 1.4375)],
    ("SHORT", "D_EMA20_REJECTION"):
        [("quality_score", ">=", 0.5764), ("entry_hour", "<=", 10.0)],
    ("SHORT", "G_LOWER_LOW_BREAK"):
        [("atr_pct_signal", ">=", 0.0070)],
}

# Setups whose Phase 5d PF was > 2.5 -- target for relaxation
HIGH_PF_SETUPS = {
    ("LONG",  "A_MOD_BREAK_C1_HIGH"),            # PF 3.29
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"),       # PF 4.77
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"),  # PF 3.72
    ("LONG",  "C_OR_BREAKOUT"),                  # PF 2.76
    ("SHORT", "A_MOD_BREAK_C1_LOW"),             # PF 2.88
    ("SHORT", "C_OR_BREAKDOWN"),                 # PF 8.17 (outlier; relax for stability)
    ("SHORT", "D_EMA20_REJECTION"),              # PF 29.5 (outlier)
}


def metrics(df):
    n = len(df)
    if n == 0:
        return dict(n=0, win_rate=0.0, pf=0.0, sum_pnl_p=0.0, max_dd=0.0,
                    day_count=0, day_win=0.0)
    p = pd.to_numeric(df["pnl_pct_price"], errors="coerce").fillna(0.0)
    wins = p[p > 0].sum()
    losses = abs(p[p < 0].sum())
    pf = (wins / losses) if losses > 0 else float("inf")
    d2 = df.copy()
    d2["trade_date"] = pd.to_datetime(d2["trade_date"], errors="coerce").dt.date
    daily = d2.groupby("trade_date")["pnl_pct_price"].sum()
    cum = daily.cumsum()
    return dict(
        n=n, win_rate=float((p > 0).mean() * 100), pf=pf, sum_pnl_p=float(p.sum()),
        max_dd=float((cum.cummax() - cum).max()) if len(cum) else 0.0,
        day_count=int(len(daily)),
        day_win=float((daily > 0).sum() / len(daily) * 100) if len(daily) else 0.0,
    )


def feature_col(df, feat):
    if feat == "entry_hour":
        et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
        return et.dt.tz_convert("Asia/Kolkata").dt.hour + \
               et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0
    return pd.to_numeric(df.get(feat, np.nan), errors="coerce")


def apply_chain(df, chain):
    mask = pd.Series(True, index=df.index)
    for feat, direction, threshold in chain:
        col = feature_col(df, feat)
        if direction == ">=":
            mask &= (col >= threshold).fillna(False)
        elif direction == "<=":
            mask &= (col <= threshold).fillna(False)
    return mask


def relax_threshold(constraint, factor, sub_unfiltered):
    """factor 0 = strict (original), 1 = remove constraint, intermediate values
    move threshold linearly toward the population's permissive end.

    For ">=" thresholds, "permissive" = quantile near 0.0 of the unfiltered
    population (lowest values).
    For "<=" thresholds, "permissive" = quantile near 1.0 (highest values).
    """
    if factor == "remove":
        return None  # represents removed constraint
    feat, direction, original_thr = constraint
    col = feature_col(sub_unfiltered, feat).dropna()
    if len(col) < 5:
        return constraint  # cannot relax safely
    if direction == ">=":
        permissive_end = float(col.quantile(0.05))
        new_thr = original_thr + factor * (permissive_end - original_thr)
    elif direction == "<=":
        permissive_end = float(col.quantile(0.95))
        new_thr = original_thr + factor * (permissive_end - original_thr)
    else:
        return constraint
    return (feat, direction, round(new_thr, 4))


def build_relaxed_chain(original_chain, factors, sub_unfiltered):
    """Apply per-constraint relaxation factors to produce a new chain."""
    new_chain = []
    for ci, c in enumerate(original_chain):
        f = factors[ci]
        if f == "remove":
            continue  # constraint removed
        relaxed = relax_threshold(c, f, sub_unfiltered)
        if relaxed is None:
            continue
        new_chain.append(relaxed)
    return new_chain


def search_setup_relaxation(df, side, setup, original_chain, target_pfs):
    sub = df[(df["side"] == side) & (df["setup"] == setup)].reset_index(drop=True)
    if len(sub) < N_FLOOR:
        return None

    # Build factor grid -- 5 levels per constraint
    factor_levels = [0.00, 0.25, 0.50, 0.75, "remove"]
    n_constraints = len(original_chain)
    if n_constraints == 0:
        return dict(n=len(sub), pf=metrics(sub)["pf"], chain=[],
                    factors=[], side=side, setup=setup)

    grid = list(itertools.product(*[factor_levels] * n_constraints))

    # For each PF target, find the relaxation combo with max n
    best_per_target = {}
    for tgt in target_pfs:
        best = None
        for combo in grid:
            new_chain = build_relaxed_chain(original_chain, combo, sub)
            mask = apply_chain(sub, new_chain)
            kept = sub.loc[mask]
            m = metrics(kept)
            if m["n"] < N_FLOOR:
                continue
            if m["pf"] < tgt:
                continue
            if best is None or m["n"] > best["n"]:
                best = dict(side=side, setup=setup, target_pf=tgt,
                            combo=combo, chain=new_chain, **m)
        if best is not None:
            best_per_target[tgt] = best

    # Prefer the highest target with a feasible solution; among ties pick max n
    if not best_per_target:
        return None
    # Pick the BEST combo: prioritize meeting the highest PF target,
    # then maximizing n.
    sorted_targets = sorted(best_per_target.keys(), reverse=True)
    chosen = best_per_target[sorted_targets[0]]
    return chosen


def main():
    df = pd.read_csv(RUN5_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)
    print(f"Loaded {len(df)} trades. Relaxing PF>2.5 setups -- target cascade {PF_TARGETS}")
    print()

    new_chains = {}
    rows = []
    for (side, setup), original_chain in CURRENT_CHAINS.items():
        if (side, setup) not in HIGH_PF_SETUPS:
            # Keep as-is
            new_chains[(side, setup)] = original_chain
            sub = df[(df["side"] == side) & (df["setup"] == setup)]
            mask = apply_chain(sub, original_chain)
            m = metrics(sub.loc[mask])
            rows.append(dict(side=side, setup=setup, mode="UNCHANGED",
                             n=m["n"], pf=m["pf"], win=m["win_rate"],
                             chain=" | ".join([f"{f} {d} {t:.4f}" for f, d, t in original_chain])))
            print(f"  KEEP    {side:6s} {setup:35s} n={m['n']:>3d} PF={m['pf']:.2f} (unchanged)")
            continue

        result = search_setup_relaxation(df, side, setup, original_chain, PF_TARGETS)
        if result is None:
            new_chains[(side, setup)] = original_chain
            print(f"  NO-RELAX {side:6s} {setup:35s} (no relaxation kept PF >= {PF_TARGETS[-1]})")
            continue

        new_chains[(side, setup)] = result["chain"]
        # Compare baseline vs relaxed
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        base_mask = apply_chain(sub, original_chain)
        base_m = metrics(sub.loc[base_mask])
        new_mask = apply_chain(sub, result["chain"])
        new_m = metrics(sub.loc[new_mask])
        delta_n = new_m["n"] - base_m["n"]
        chain_str = " | ".join([f"{f} {d} {t:.4f}" for f, d, t in result["chain"]])
        print(f"  RELAX   {side:6s} {setup:35s}  n {base_m['n']:>3d}->{new_m['n']:>3d} "
              f"({delta_n:+d}) PF {base_m['pf']:.2f}->{new_m['pf']:.2f} "
              f"win={new_m['win_rate']:.1f}% (target {result['target_pf']})")
        if chain_str:
            print(f"          chain: {chain_str}")
        rows.append(dict(side=side, setup=setup, mode=f"RELAXED@PF>={result['target_pf']:.2f}",
                         n=new_m["n"], pf=new_m["pf"], win=new_m["win_rate"], chain=chain_str))

    # Aggregate metrics with new chains
    print("\n=== AGGREGATE on relaxed selection ===")
    agg_keep = pd.Series(False, index=df.index)
    for (side, setup), chain in new_chains.items():
        sub_idx = df.index[(df["side"] == side) & (df["setup"] == setup)]
        sub = df.loc[sub_idx]
        mask = apply_chain(sub, chain)
        agg_keep.loc[sub.index[mask]] = True
    selected = df.loc[agg_keep].copy()
    m = metrics(selected)
    print(f"  trades       : {m['n']}")
    print(f"  win rate     : {m['win_rate']:.2f}%")
    print(f"  PF           : {m['pf']:.3f}")
    print(f"  sum PnL %    : {m['sum_pnl_p']:+.2f}")
    print(f"  max DD %     : {m['max_dd']:.2f}")
    print(f"  day-win rate : {m['day_win']:.2f}%")

    # Save
    pd.DataFrame(rows).to_csv(OUT_DIR / "run5_relaxed_high_pf_filters.csv", index=False)
    selected.to_csv(OUT_DIR / "run5_relaxed_selected_trades.csv", index=False)
    print()
    print(f"Wrote run5_relaxed_high_pf_filters.csv ({len(rows)} setups)")
    print(f"Wrote run5_relaxed_selected_trades.csv ({len(selected)} trades)")

    # Emit Python literal for v17t_live Phase 5d
    print()
    print("=== Phase 5d filter dict (relaxed; paste into v17t_live) ===")
    print("V17T_DEEP_FILTER_SPEC = {")
    for (side, setup), chain in new_chains.items():
        chain_d = [(f, d, round(t, 4)) for f, d, t in chain]
        print(f"    ({side!r:8s}, {setup!r:42s}): {chain_d!r},")
    print("}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
