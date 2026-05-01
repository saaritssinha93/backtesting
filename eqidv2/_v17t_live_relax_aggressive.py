# -*- coding: utf-8 -*-
"""
v17t_live AGGRESSIVE relaxation -- target n=1000+ across all setups.

Lowers the PF floor to 1.5 (with 1.4 / 1.3 fallbacks) so high-PF setups
can be relaxed enough to admit substantially more trades. Uses 7 relaxation
levels per constraint (0, 0.25, 0.50, 0.65, 0.80, 0.95, "remove").

Loops PF floor: tries [1.7, 1.5, 1.4, 1.3]. For each setup, picks the
loosest combo whose PF is still above the target floor.

Outputs:
  run5_aggressive_relaxed_filters.csv
  run5_aggressive_selected_trades.csv
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

# Per-setup PF target cascade -- aim for 1.4 floor to maximize n while
# aggregate stays > 1.6.
PER_SETUP_TARGETS = [1.45]
# Aggregate target: at least this PF when all setups combined.
AGGREGATE_PF_FLOOR = 1.55
N_FLOOR = 12

CURRENT_CHAINS = {
    ("LONG",  "A_MOD_BREAK_C1_HIGH"):
        [("avwap_dist_atr_signal", ">=", 2.5473)],
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"):
        [("stochk_signal", ">=", 93.7635), ("entry_hour", "<=", 11.6417)],
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"):
        [("avwap_dist_atr_signal", "<=", 1.9609)],
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"):
        [("quality_score", ">=", 9.6570)],
    ("LONG",  "C_OR_BREAKOUT"):
        [("quality_score", ">=", 2.3487),
         ("atr_pct_signal", "<=", 0.0064),
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
         ("atr_pct_signal", ">=", 0.0053)],
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):
        [("avwap_dist_atr_signal", ">=", 1.4375)],
    ("SHORT", "D_EMA20_REJECTION"):
        [("quality_score", ">=", 0.4856), ("entry_hour", "<=", 10.0)],
    ("SHORT", "G_LOWER_LOW_BREAK"):
        [("atr_pct_signal", ">=", 0.0070)],
}

FACTOR_LEVELS = [0.00, 0.25, 0.50, 0.65, 0.80, 0.95, "remove"]


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
        n=n, win_rate=float((p > 0).mean() * 100), pf=pf,
        sum_pnl_p=float(p.sum()),
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
    if factor == "remove":
        return None
    feat, direction, original_thr = constraint
    col = feature_col(sub_unfiltered, feat).dropna()
    if len(col) < 5:
        return constraint
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
    new_chain = []
    for ci, c in enumerate(original_chain):
        f = factors[ci]
        if f == "remove":
            continue
        relaxed = relax_threshold(c, f, sub_unfiltered)
        if relaxed is None:
            continue
        new_chain.append(relaxed)
    return new_chain


def search_setup(df, side, setup, original_chain, target_pfs):
    sub = df[(df["side"] == side) & (df["setup"] == setup)].reset_index(drop=True)
    if len(sub) < N_FLOOR:
        return None
    n_constraints = len(original_chain)
    if n_constraints == 0:
        m = metrics(sub)
        return dict(side=side, setup=setup, target_pf=0.0, chain=[], **m)
    grid = list(itertools.product(*[FACTOR_LEVELS] * n_constraints))

    # Try each PF target in cascade order. For each, find combo with max n.
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
                            chain=new_chain, **m)
        if best is not None:
            return best
    return None


def map_curve(df):
    """Run the search at each per-setup PF floor and report aggregate volume."""
    floors = [1.70, 1.60, 1.55, 1.50, 1.45, 1.40]
    rows = []
    for floor in floors:
        new_chains = {}
        for (side, setup), original_chain in CURRENT_CHAINS.items():
            r = search_setup(df, side, setup, original_chain, [floor])
            if r is None:
                new_chains[(side, setup)] = original_chain
            else:
                new_chains[(side, setup)] = r["chain"]
        agg_keep = pd.Series(False, index=df.index)
        for (side, setup), chain in new_chains.items():
            sub_idx = df.index[(df["side"] == side) & (df["setup"] == setup)]
            sub = df.loc[sub_idx]
            mask = apply_chain(sub, chain)
            agg_keep.loc[sub.index[mask]] = True
        sel = df.loc[agg_keep]
        m = metrics(sel)
        rows.append(dict(per_setup_floor=floor, **m))
        print(f"  per-setup floor={floor:.2f}  agg n={m['n']:>5d}  PF={m['pf']:.3f}  "
              f"win={m['win_rate']:>5.1f}%  day-win={m['day_win']:>5.1f}%  DD={m['max_dd']:.2f}%")
    return rows


def main():
    df = pd.read_csv(RUN5_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)
    print(f"Loaded {len(df)} trades. Aggressive relaxation -- target cascade {PER_SETUP_TARGETS}")
    print()

    print("=== Volume vs quality curve (per-setup PF floor sweep) ===")
    map_curve(df)
    print()

    new_chains = {}
    rows = []
    for (side, setup), original_chain in CURRENT_CHAINS.items():
        result = search_setup(df, side, setup, original_chain, PER_SETUP_TARGETS)
        if result is None:
            new_chains[(side, setup)] = original_chain
            sub = df[(df["side"] == side) & (df["setup"] == setup)]
            mask = apply_chain(sub, original_chain)
            m = metrics(sub.loc[mask])
            print(f"  KEEP    {side:6s} {setup:35s} (no relax kept PF >= {PER_SETUP_TARGETS[-1]})  "
                  f"n={m['n']:>4d} PF={m['pf']:.2f}")
            rows.append(dict(side=side, setup=setup, mode="UNCHANGED",
                             n=m["n"], pf=m["pf"], win=m["win_rate"]))
            continue

        new_chains[(side, setup)] = result["chain"]
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        base_mask = apply_chain(sub, original_chain)
        base_m = metrics(sub.loc[base_mask])
        chain_str = " | ".join([f"{f} {d} {t:.4f}" for f, d, t in result["chain"]]) or "(no constraints)"
        print(f"  RELAX   {side:6s} {setup:35s} n {base_m['n']:>4d}->{result['n']:>4d} "
              f"({result['n']-base_m['n']:+d}) PF {base_m['pf']:.2f}->{result['pf']:.2f} "
              f"win={result['win_rate']:.1f}% (target {result['target_pf']})")
        if chain_str:
            print(f"          {chain_str}")
        rows.append(dict(side=side, setup=setup, mode=f"RELAXED@PF>={result['target_pf']}",
                         n=result["n"], pf=result["pf"], win=result["win_rate"], chain=chain_str))

    # Aggregate
    agg_keep = pd.Series(False, index=df.index)
    for (side, setup), chain in new_chains.items():
        sub_idx = df.index[(df["side"] == side) & (df["setup"] == setup)]
        sub = df.loc[sub_idx]
        mask = apply_chain(sub, chain)
        agg_keep.loc[sub.index[mask]] = True
    selected = df.loc[agg_keep].copy()
    m = metrics(selected)
    print()
    print("=== AGGREGATE on aggressive relaxation ===")
    print(f"  trades       : {m['n']}")
    print(f"  long/short   : {(selected['side']=='LONG').sum()} / {(selected['side']=='SHORT').sum()}")
    print(f"  win rate     : {m['win_rate']:.2f}%")
    print(f"  PF           : {m['pf']:.3f}")
    print(f"  sum PnL %    : {m['sum_pnl_p']:+.2f}")
    print(f"  max DD %     : {m['max_dd']:.2f}")
    print(f"  day count    : {m['day_count']}")
    print(f"  day-win rate : {m['day_win']:.2f}%")
    if m["pf"] < AGGREGATE_PF_FLOOR:
        print(f"  WARN aggregate PF below {AGGREGATE_PF_FLOOR} floor.")

    pd.DataFrame(rows).to_csv(OUT_DIR / "run5_aggressive_relaxed_filters.csv", index=False)
    selected.to_csv(OUT_DIR / "run5_aggressive_selected_trades.csv", index=False)
    print()
    print(f"Wrote run5_aggressive_relaxed_filters.csv ({len(rows)} setups)")
    print(f"Wrote run5_aggressive_selected_trades.csv ({len(selected)} trades)")
    print()
    print("=== Phase 5d filter dict (aggressive; paste into v17t_live) ===")
    print("V17T_DEEP_FILTER_SPEC = {")
    for (side, setup), chain in new_chains.items():
        chain_d = [(f, d, round(t, 4)) for f, d, t in chain]
        print(f"    ({side!r:8s}, {setup!r:42s}): {chain_d!r},")
    print("}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
