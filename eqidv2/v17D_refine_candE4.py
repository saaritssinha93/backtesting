"""v17D Cand-E4 refinement — apply 4 production decisions to a trades CSV.

  1. Drop SHORT G_LOWER_LOW_BREAK (failed Phase 0.2 robustness)
  2. Trades CSV input is already re-resolved with MAE/MFE SL/TGT (Phase 3.1)
  3. Filter to top-100 ADV tickers (≥ Rs 500 cr; cost model "top100" bucket)
  4. Output a refined trades CSV ready for stress test

Universe CSV provides ticker → adv_rs_cr.

Usage:
    python -m eqidv2.v17D_refine_candE4 \
        --trades   outputs_v17D_phase0/trades_reresolved_mae_mfe.csv \
        --universe configs/universe.csv \
        --output   outputs_v17D_phase0/trades_candE4_refined.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm

DROP_SETUPS = {("SHORT", "G_LOWER_LOW_BREAK")}


def refine(trades: pd.DataFrame, universe: pd.DataFrame, args_bucket_filter=None) -> tuple:
    n_in = len(trades)
    df = trades.copy()
    df["side"] = df["side"].astype(str).str.upper()
    df["setup"] = df["setup"].astype(str)

    drop_mask = df.apply(
        lambda r: (r["side"], r["setup"]) in DROP_SETUPS, axis=1
    )
    n_dropped_setup = int(drop_mask.sum())
    df = df[~drop_mask]

    adv_lookup = dict(zip(universe["ticker"], universe["adv_rs_cr"]))
    df["adv_rs_cr"] = df["ticker"].map(adv_lookup)
    df["adv_bucket"] = df["adv_rs_cr"].fillna(0.0).apply(cm.adv_bucket_for)

    bucket_counts = df["adv_bucket"].value_counts().to_dict()

    keep_buckets = set(args_bucket_filter or {"top100"})
    bucket_mask = df["adv_bucket"].isin(keep_buckets)
    n_dropped_universe = int((~bucket_mask).sum())
    df = df[bucket_mask]

    info = {
        "n_in": n_in,
        "n_dropped_setup": n_dropped_setup,
        "n_dropped_universe": n_dropped_universe,
        "n_out": len(df),
        "bucket_counts_pre_universe": bucket_counts,
    }
    return df, info


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True)
    p.add_argument("--universe", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--keep-buckets", default="top100",
                   help="comma list of ADV buckets to keep (top100,mid,long_tail)")
    args = p.parse_args()

    trades = pd.read_csv(args.trades)
    universe = pd.read_csv(args.universe)
    keep = set(s.strip() for s in args.keep_buckets.split(",") if s.strip())
    out, info = refine(trades, universe, args_bucket_filter=keep)
    out.to_csv(args.output, index=False)

    print(f"v17D Cand-E4 refinement — {info['n_in']} input trades")
    print(f"  drop SHORT G_LOWER_LOW_BREAK: {info['n_dropped_setup']} trades removed")
    print(f"  ADV bucket distribution (post-setup-drop): "
          f"{info['bucket_counts_pre_universe']}")
    print(f"  drop non-top100 ADV tickers: {info['n_dropped_universe']} trades removed")
    print(f"  remaining: {info['n_out']} trades")
    print(f"Wrote: {args.output}")
    print("\nPer-(side, setup) breakdown of survivors:")
    print(out.groupby(["side", "setup"]).size().to_string())


if __name__ == "__main__":
    main()
