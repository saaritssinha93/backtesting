# -*- coding: utf-8 -*-
"""
v17t_live DEEP per-setup optimizer.

Goal: for EVERY setup in the Run 5 honest universe, find the multi-feature
filter that produces the maximum trade count subject to PF >= TARGET_PF.
Targets PF 1.70 first; falls back to 1.50 if none found; falls back to 1.30
if still none found; otherwise drops the setup.

Approach:
1. For each (side, setup), pull all candidate trades (Run 5 honest CSV).
2. For each numeric feature, sweep over quantile thresholds (lo, hi).
3. Greedy: start with no filter; iteratively add the (feature, threshold)
   that improves PF the most while keeping n >= n_floor.
4. Stop when PF >= TARGET_PF (locked in) OR no further improvement.

Features considered:
    rsi_signal, adx_signal, stochk_signal, quality_score,
    atr_pct_signal, avwap_dist_atr_signal, ema20_gap_atr_signal,
    nifty_rel_strength_pct, entry_hour, entry_bar_vol_ratio, bars_from_open

Output:
    run5_deep_per_setup_filters.csv  (one row per kept setup with chosen filter)
    run5_deep_selected_trades.csv    (aggregate trade set after filters)
    Plus a printed Python literal that we paste into v17t_live as Phase 5d.
"""
from __future__ import annotations

import sys
import itertools
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path(r"C:/TradingData/eqidv2/outputs_v17q_5min")
RUN5_CSV = OUT_DIR / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_143701.csv"

PF_TARGETS = [1.70, 1.55, 1.40, 1.25]   # cascade of acceptance thresholds
N_FLOOR = 12                              # absolute minimum trades to keep a setup
ABSOLUTE_MIN_FOR_SEARCH = 8               # if unfiltered n is below this, skip entirely


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
        n=n,
        win_rate=float((p > 0).mean() * 100),
        pf=pf,
        sum_pnl_p=float(p.sum()),
        max_dd=float((cum.cummax() - cum).max()) if len(cum) else 0.0,
        day_count=int(len(daily)),
        day_win=float((daily > 0).sum() / len(daily) * 100) if len(daily) else 0.0,
    )


FEATURES = [
    "rsi_signal", "adx_signal", "stochk_signal", "quality_score",
    "atr_pct_signal", "avwap_dist_atr_signal", "ema20_gap_atr_signal",
    "nifty_rel_strength_pct", "entry_hour", "entry_bar_vol_ratio",
    "bars_from_open",
]
QUANTILES = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]


def feature_threshold_search(sub: pd.DataFrame, current_mask: pd.Series,
                             used_features: set, n_floor: int):
    """For the current filtered slice, find the single (feature, threshold,
    direction) that improves PF most while keeping n >= n_floor."""
    base_metric = metrics(sub.loc[current_mask])
    base_pf = base_metric["pf"]
    base_n = base_metric["n"]
    if base_n < n_floor:
        return None  # already too thin

    best_gain = 0.0
    best_choice = None
    for feat in FEATURES:
        if feat in used_features:
            continue
        if feat not in sub.columns:
            continue
        col = pd.to_numeric(sub[feat], errors="coerce")
        # Pull only rows in current_mask for quantile reference
        ref = col.loc[current_mask].dropna()
        if len(ref) < n_floor:
            continue

        # For each quantile threshold and each direction (>= q, <= q),
        # compute filtered metrics
        for q in QUANTILES:
            try:
                thr = float(ref.quantile(q))
            except Exception:
                continue
            for direction in (">=", "<="):
                if direction == ">=":
                    cond = (col >= thr)
                else:
                    cond = (col <= thr)
                new_mask = current_mask & cond.fillna(False)
                m = metrics(sub.loc[new_mask])
                if m["n"] < n_floor:
                    continue
                # Score: PF improvement, weighted toward keeping n
                gain = (m["pf"] - base_pf) * np.sqrt(m["n"] / max(base_n, 1))
                if gain > best_gain:
                    best_gain = gain
                    best_choice = dict(
                        feature=feat, direction=direction, threshold=thr,
                        new_pf=m["pf"], new_n=m["n"], new_win=m["win_rate"],
                    )
    return best_choice


def search_setup(df: pd.DataFrame, side: str, setup: str,
                 target_pf: float, n_floor: int):
    """For a given (side, setup), run greedy feature selection until
    PF >= target_pf or no improvement possible."""
    sub = df[(df["side"] == side) & (df["setup"] == setup)].copy()
    if len(sub) < ABSOLUTE_MIN_FOR_SEARCH:
        return None
    sub = sub.reset_index(drop=True)

    # Add helper columns
    et = pd.to_datetime(sub.get("entry_time_ist"), errors="coerce", utc=True)
    sub["entry_hour"] = et.dt.tz_convert("Asia/Kolkata").dt.hour + \
                       et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0

    current_mask = pd.Series(True, index=sub.index)
    used = set()
    chain = []
    base_m = metrics(sub.loc[current_mask])

    for _ in range(4):  # at most 4 features stacked
        cur_m = metrics(sub.loc[current_mask])
        if cur_m["pf"] >= target_pf and cur_m["n"] >= n_floor:
            break
        best = feature_threshold_search(sub, current_mask, used, n_floor)
        if best is None or (best["new_pf"] - cur_m["pf"]) < 0.02:
            break
        # Apply
        col = pd.to_numeric(sub[best["feature"]], errors="coerce")
        if best["direction"] == ">=":
            current_mask &= (col >= best["threshold"]).fillna(False)
        else:
            current_mask &= (col <= best["threshold"]).fillna(False)
        used.add(best["feature"])
        chain.append(best)
        if metrics(sub.loc[current_mask])["pf"] >= target_pf:
            break

    final_m = metrics(sub.loc[current_mask])
    return dict(side=side, setup=setup, baseline_n=base_m["n"], baseline_pf=base_m["pf"],
                final_n=final_m["n"], final_pf=final_m["pf"], final_win=final_m["win_rate"],
                final_dd=final_m["max_dd"], final_day_win=final_m["day_win"],
                chain=chain, kept_idx=set(sub.loc[current_mask].index.tolist()))


def main():
    df = pd.read_csv(RUN5_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)
    print(f"Loaded {len(df)} trades. Searching ALL setups with PF cascade {PF_TARGETS}...")

    setups = sorted(df.groupby(["side", "setup"]).size().index.tolist())
    print(f"Setups to evaluate: {len(setups)}")
    print()

    all_results = []
    keep_idx_global = set()
    chosen_filters = []

    for side, setup in setups:
        # Try each PF target cascade
        chosen = None
        for tgt in PF_TARGETS:
            r = search_setup(df, side, setup, tgt, N_FLOOR)
            if r is None:
                break
            if r["final_pf"] >= tgt and r["final_n"] >= N_FLOOR:
                chosen = (tgt, r)
                break
        if chosen is None:
            print(f"  DROP  {side:6s} {setup:35s}  no filter combo achieved PF>=1.25 with n>={N_FLOOR}")
            continue
        tgt, r = chosen
        keep_idx_global |= r["kept_idx"]
        chosen_filters.append((side, setup, tgt, r))
        chain_str = "  ".join([f"{c['feature']} {c['direction']} {c['threshold']:.3f}" for c in r["chain"]])
        print(f"  KEEP  {side:6s} {setup:35s}  baseline n={r['baseline_n']:>4d} PF={r['baseline_pf']:.2f}  "
              f"-> n={r['final_n']:>3d} PF={r['final_pf']:.2f} win={r['final_win']:.1f}% (target {tgt})")
        if chain_str:
            print(f"          chain: {chain_str}")
        all_results.append(dict(
            side=side, setup=setup, target_pf=tgt,
            baseline_n=r["baseline_n"], baseline_pf=r["baseline_pf"],
            final_n=r["final_n"], final_pf=r["final_pf"], final_win=r["final_win"],
            final_dd=r["final_dd"], final_day_win=r["final_day_win"],
            filter_chain=" | ".join([f"{c['feature']} {c['direction']} {c['threshold']:.4f}" for c in r["chain"]]),
        ))

    # Aggregate via reset-index search; rebuild masks against full df
    print("\n=== AGGREGATE on selected trades ===")
    # We need to map each chosen subset back to the full-df indices
    # Search returned kept_idx within the per-setup sub.
    agg_keep_global = pd.Series(False, index=df.index)
    for side, setup, tgt, r in chosen_filters:
        # Re-apply chain to the full-df subset
        sub_idx = df.index[(df["side"] == side) & (df["setup"] == setup)]
        sub = df.loc[sub_idx].copy()
        et = pd.to_datetime(sub.get("entry_time_ist"), errors="coerce", utc=True)
        sub["entry_hour"] = et.dt.tz_convert("Asia/Kolkata").dt.hour + \
                           et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0
        mask = pd.Series(True, index=sub.index)
        for c in r["chain"]:
            col = pd.to_numeric(sub[c["feature"]], errors="coerce")
            if c["direction"] == ">=":
                mask &= (col >= c["threshold"]).fillna(False)
            else:
                mask &= (col <= c["threshold"]).fillna(False)
        agg_keep_global.loc[sub.index[mask]] = True

    selected = df.loc[agg_keep_global].copy()
    m = metrics(selected)
    print(f"  trades       : {m['n']}")
    print(f"  win rate     : {m['win_rate']:.2f}%")
    print(f"  PF           : {m['pf']:.3f}")
    print(f"  sum PnL %    : {m['sum_pnl_p']:+.2f}")
    print(f"  max DD %     : {m['max_dd']:.2f}")
    print(f"  day count    : {m['day_count']}")
    print(f"  day-win rate : {m['day_win']:.2f}%")

    # Save artifacts
    pd.DataFrame(all_results).to_csv(OUT_DIR / "run5_deep_per_setup_filters.csv", index=False)
    selected.to_csv(OUT_DIR / "run5_deep_selected_trades.csv", index=False)
    print(f"\nWrote run5_deep_per_setup_filters.csv ({len(all_results)} kept setups)")
    print(f"Wrote run5_deep_selected_trades.csv ({len(selected)} trades)")

    # Emit Python literal for v17t_live Phase 5d
    print("\n=== Phase 5d filter dict (paste into v17t_live) ===")
    print("V17T_DEEP_FILTERS = {")
    for side, setup, tgt, r in chosen_filters:
        chain_d = []
        for c in r["chain"]:
            chain_d.append((c["feature"], c["direction"], round(c["threshold"], 4)))
        print(f"    ({side!r:8s}, {setup!r:42s}): {chain_d!r},")
    print("}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
