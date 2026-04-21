r"""
A/B comparison: v17g (baseline C10) vs v17j (v17g + relaxed short indicators).

Usage
-----
  python -m eqidv2.research.compare_v17g_v17j \
      --v17g C:\TradingData\eqidv2\outputs_v17g_5min\<...>.csv \
      --v17j C:\TradingData\eqidv2\outputs_v17j_5min\<...>.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .core import compute_metrics, load_trades


ENTRY_KEYS = ["signal_time_ist", "ticker", "side", "setup"]


def _report(label: str, df: pd.DataFrame) -> dict:
    m = compute_metrics(df).as_row()
    print(
        f"  {label:25s}  n={m['n_trades']:4d}  L={m['n_long']:3d}  S={m['n_short']:3d}  "
        f"PF={m['profit_factor']:.3f}  DD={m['max_drawdown_pct']:5.2f}%  "
        f"Win={m['win_rate']:5.2f}%  Sharpe={m['sharpe_ann']:5.2f}  "
        f"PnL={m['sum_pnl_pct']:7.2f}%  Calmar={m['calmar']:5.2f}"
    )
    return m


def _delta(a: dict, b: dict, key: str) -> str:
    va, vb = a[key], b[key]
    d = vb - va
    rel = (d / va * 100.0) if va else float("inf")
    unit = "pp" if "rate" in key or "pct" in key else ""
    arrow = ">" if d >= 0 else "<"
    return f"{va:8.3f} {arrow} {vb:8.3f}  (d {d:+7.3f}{unit}, {rel:+5.1f}%)"


def _print_added_shorts(df: pd.DataFrame) -> None:
    if df.empty:
        print("  added SHORT entries       : none")
        return

    print(f"  added SHORT entries       : {len(df)}")
    top_setups = (
        df["setup"].astype(str).value_counts().head(5)
        if "setup" in df.columns
        else pd.Series(dtype="int64")
    )
    if not top_setups.empty:
        print("  top added SHORT setups    :")
        for setup, count in top_setups.items():
            print(f"    {setup}: {count}")

    if "entry_time_ist" in df.columns:
        ts = pd.to_datetime(df["entry_time_ist"], errors="coerce")
        bucket = ts.dt.strftime("%H:%M").fillna("NaT")
        top_times = bucket.value_counts().head(5)
        if not top_times.empty:
            print("  top added SHORT times     :")
            for hhmm, count in top_times.items():
                print(f"    {hhmm}: {count}")

    short_metrics = compute_metrics(df).as_row()
    print(
        "  added SHORT metrics       : "
        f"PF={short_metrics['profit_factor']:.3f}  "
        f"Win={short_metrics['win_rate']:.2f}%  "
        f"PnL={short_metrics['sum_pnl_pct']:.2f}%"
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--v17g", type=Path, required=True)
    p.add_argument("--v17j", type=Path, required=True)
    args = p.parse_args()

    g = load_trades(args.v17g)
    j = load_trades(args.v17j)

    print(f"v17g: {args.v17g}  ({len(g)} rows)")
    print(f"v17j: {args.v17j}  ({len(j)} rows)")

    print("\n=== METRICS ===")
    mg = _report("v17g (baseline)", g)
    mj = _report("v17j (relaxed short)", j)

    print("\n=== DELTA v17g -> v17j ===")
    for key in [
        "n_trades",
        "n_short",
        "profit_factor",
        "max_drawdown_pct",
        "win_rate",
        "sum_pnl_pct",
        "sharpe_ann",
        "calmar",
        "short_pf",
        "long_pf",
    ]:
        print(f"  {key:20s}  {_delta(mg, mj, key)}")

    g_k = g.set_index(ENTRY_KEYS).sort_index()
    j_k = j.set_index(ENTRY_KEYS).sort_index()
    both = g_k.index.intersection(j_k.index)
    only_g = g_k.index.difference(j_k.index)
    only_j = j_k.index.difference(g_k.index)

    print("\n=== ENTRY DIFFERENCE ===")
    print(f"  entries in both      : {len(both)}")
    print(f"  only in v17g         : {len(only_g)}")
    print(f"  only in v17j         : {len(only_j)}")

    j_only = j_k.loc[only_j].reset_index() if len(only_j) else j.iloc[0:0].copy()
    j_only_short = (
        j_only[j_only["side"].astype(str).str.upper().eq("SHORT")].copy()
        if not j_only.empty and "side" in j_only.columns
        else j.iloc[0:0].copy()
    )

    print("\n=== ADDED SHORTS IN v17j ===")
    _print_added_shorts(j_only_short)

    print("\n=== VERDICT ===")
    checks = [
        ("SHORT count is higher", mj["n_short"] > mg["n_short"]),
        ("PF stays healthy", mj["profit_factor"] >= 1.75),
        ("DD not >20% worse", mj["max_drawdown_pct"] <= mg["max_drawdown_pct"] * 1.20),
        ("SHORT PF not badly degraded", mj["short_pf"] >= mg["short_pf"] - 0.20),
    ]
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}]  {name}")

    if all(ok for _, ok in checks):
        print("\n  >>> v17j looks viable for paper-trade A/B against v17g.")
    else:
        print("\n  >>> v17j needs more tuning before replacing v17g.")


if __name__ == "__main__":
    main()
