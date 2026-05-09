"""v17D Step 0.5 — setup correlation audit (pairwise Jaccard).

For every pair of setups, compute Jaccard similarity over (date, ticker, bar)
signal sets. High Jaccard means the two setups are firing on essentially the
same trades — they should be merged or one dropped to avoid double-counting.

Gate: max pair-wise Jaccard < 0.5. Pairs >= 0.5 are flagged.

Usage:
    python -m eqidv2.v17D_correlation_audit \
        --trades outputs_v17C_noNF_live_5min/avwap_longshort_trades_*_candE3.csv \
        --output v17D_correlation.csv
"""
from __future__ import annotations

import argparse
from itertools import combinations

import pandas as pd


def _signal_sets(df: pd.DataFrame) -> dict:
    """Return {setup_id: set of (date, ticker, bar_minute) tuples}."""
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["entry_dt"] = pd.to_datetime(df["entry_time_ist"], errors="coerce")
    df["bar_minute"] = df["entry_dt"].dt.floor("5min").astype(str)
    sets: dict = {}
    for setup, g in df.groupby("setup"):
        sets[setup] = set(zip(g["trade_date"], g["ticker"], g["bar_minute"]))
    return sets


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


def audit(df: pd.DataFrame, threshold: float = 0.5) -> tuple:
    sets = _signal_sets(df)
    rows = []
    flagged = []
    for s1, s2 in combinations(sorted(sets.keys()), 2):
        j = jaccard(sets[s1], sets[s2])
        rows.append({
            "setup_a": s1, "setup_b": s2,
            "n_a": len(sets[s1]), "n_b": len(sets[s2]),
            "intersection": len(sets[s1] & sets[s2]),
            "union": len(sets[s1] | sets[s2]),
            "jaccard": j,
            "flagged": j >= threshold,
        })
        if j >= threshold:
            flagged.append((s1, s2, j))
    return pd.DataFrame(rows).sort_values("jaccard", ascending=False), flagged


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--threshold", type=float, default=0.5)
    args = p.parse_args()

    df = pd.read_csv(args.trades)
    table, flagged = audit(df, args.threshold)
    table.to_csv(args.output, index=False)

    print(f"\nv17D correlation audit — {len(df)} trades, "
          f"{table.shape[0]} setup pairs")
    print("=" * 78)
    if flagged:
        print(f"  FLAGGED: {len(flagged)} pair(s) at Jaccard >= {args.threshold}")
        for a, b, j in flagged:
            print(f"    {a} + {b}: J={j:.3f}")
        print("  ACTION: merge or drop one setup from each flagged pair.")
    else:
        print(f"  PASS: max Jaccard < {args.threshold} across all pairs.")
    print()
    print("Top 10 by Jaccard:")
    print(table.head(10).to_string(index=False))
    print(f"\nWrote: {args.output}")


if __name__ == "__main__":
    main()
