"""
A/B comparison: v17g (TGT 1.00%) vs v17h (TGT 1.50%, SL unchanged at 0.75%).

Same filter stack, same entries — only the target differs.  Verifies the
research exit-sweep P03 finding holds on real 5-min execution rather than
re-resolved bars.

Usage
-----
  python -m eqidv2.research.compare_v17g_v17h \
      --v17g C:\TradingData\eqidv2\outputs_v17g_5min\<...>.csv \
      --v17h C:\TradingData\eqidv2\outputs_v17h_5min\<...>.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .core import compute_metrics, load_trades


def _report(label: str, df: pd.DataFrame) -> dict:
    m = compute_metrics(df).as_row()
    print(
        f"  {label:25s}  n={m['n_trades']:4d}  L={m['n_long']:3d}  S={m['n_short']:3d}  "
        f"PF={m['profit_factor']:.3f}  DD={m['max_drawdown_pct']:5.2f}%  "
        f"Win={m['win_rate']:5.2f}%  Sharpe={m['sharpe_ann']:5.2f}  "
        f"PnL={m['sum_pnl_pct']:7.2f}%  Calmar={m['calmar']:5.2f}"
    )
    return m


def _delta(a: dict, b: dict, key: str, pct: bool = False) -> str:
    va, vb = a[key], b[key]
    d = vb - va
    rel = (d / va * 100.0) if va else float("inf")
    unit = "pp" if "rate" in key or "pct" in key else ""
    arrow = ">" if d >= 0 else "<"
    return f"{va:8.3f} {arrow} {vb:8.3f}  (d {d:+7.3f}{unit}, {rel:+5.1f}%)"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--v17g", type=Path, required=True)
    p.add_argument("--v17h", type=Path, required=True)
    args = p.parse_args()

    g = load_trades(args.v17g)
    h = load_trades(args.v17h)

    print(f"v17g: {args.v17g}  ({len(g)} rows)")
    print(f"v17h: {args.v17h}  ({len(h)} rows)")

    print("\n=== METRICS ===")
    mg = _report("v17g (TGT 1.00%)", g)
    mh = _report("v17h (TGT 1.50%)", h)

    print("\n=== DELTA v17g -> v17h ===")
    for key in [
        "n_trades", "profit_factor", "max_drawdown_pct", "win_rate",
        "sum_pnl_pct", "sharpe_ann", "calmar", "sortino_ann",
        "long_pf", "short_pf",
    ]:
        print(f"  {key:20s}  {_delta(mg, mh, key)}")

    # Structural check: v17g and v17h should have SAME entries (same filter stack).
    keys = ["signal_time_ist", "ticker", "side", "setup"]
    g_k = g.set_index(keys).sort_index()
    h_k = h.set_index(keys).sort_index()
    both = g_k.index.intersection(h_k.index)
    only_g = g_k.index.difference(h_k.index)
    only_h = h_k.index.difference(g_k.index)

    print("\n=== ENTRY IDENTITY CHECK ===")
    print(f"  entries in both      : {len(both)}")
    print(f"  only in v17g         : {len(only_g)}")
    print(f"  only in v17h         : {len(only_h)}")

    # Outcome transitions — TGT widening should convert some TARGET->EOD and
    # a few SL->? (actually SL count should be similar since SL is unchanged).
    g_j = g_k.loc[both]
    h_j = h_k.loc[both]
    trans = pd.crosstab(
        g_j["outcome"].astype(str).str.upper(),
        h_j["outcome"].astype(str).str.upper(),
        margins=True, margins_name="TOTAL",
    )
    print("\n=== OUTCOME TRANSITIONS (rows=v17g, cols=v17h) ===")
    print(trans.to_string())

    # How many 1.00% winners now go to EOD (target moved out of reach)?
    g_tgt = (g_j["outcome"].astype(str).str.upper() == "TARGET")
    h_tgt = (h_j["outcome"].astype(str).str.upper() == "TARGET")
    kept_target = (g_tgt & h_tgt).sum()
    lost_target = (g_tgt & ~h_tgt).sum()
    new_target_from_eod = (~g_tgt & h_tgt).sum()
    print(f"\n  TARGET kept (still 1.50%+ move)  : {kept_target}")
    print(f"  TARGET -> non-TARGET (fell short): {lost_target}")
    print(f"  non-TARGET -> TARGET             : {new_target_from_eod}")

    # Per-trade PnL impact
    g_pnl = g_j["pnl_pct"]
    h_pnl = h_j["pnl_pct"]
    delta = h_pnl - g_pnl
    print(f"\n  avg per-trade PnL delta (h-g) : {delta.mean():+.4f}%  (median {delta.median():+.4f}%)")
    print(f"  trades improved              : {(delta > 0.001).sum()}")
    print(f"  trades worsened              : {(delta < -0.001).sum()}")
    print(f"  trades unchanged             : {(delta.abs() <= 0.001).sum()}")

    # Verdict
    print("\n=== VERDICT ===")
    checks = [
        ("PF not worse",       mh["profit_factor"] >= mg["profit_factor"] - 0.02),
        ("DD within 15% of v17g", mh["max_drawdown_pct"] <= mg["max_drawdown_pct"] * 1.15),
        ("PnL materially up",  mh["sum_pnl_pct"] > mg["sum_pnl_pct"] * 1.10),
        ("Sharpe not worse",   mh["sharpe_ann"] >= mg["sharpe_ann"] - 0.5),
    ]
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}]  {name}")

    all_pass = all(ok for _, ok in checks)
    if all_pass:
        print("\n  >>> P03 exit widening is a CANDIDATE for paper-trading alongside v17g.")
    else:
        print("\n  >>> P03 did NOT pass all criteria on live execution — stick with v17g's 1.00% target.")


if __name__ == "__main__":
    main()
