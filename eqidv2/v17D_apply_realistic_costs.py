"""v17D Phase 1.2 — apply realistic ADV-bucketed cost model to a trades CSV.

Replaces v17C's lab-cost-baked-in columns with realistic per-row costs:
    cost_pct_realistic       = realistic round-trip cost % for this row
                               (based on ticker's ADV bucket + outcome)
    pnl_pct_price_net        = pnl_pct_gross_price - cost_pct_realistic
    pnl_pct_net              = pnl_pct_price_net * leverage
    pnl_pct_eff_net          = pnl_pct_net * candE_size_mult
    pnl_rs_eff_net           = pnl_pct_eff_net / 100 * position_size_rs

The lab columns (pnl_pct_price, pnl_pct, pnl_pct_eff, pnl_rs_eff) are
preserved so the existing stress-test logic still works. Downstream code
that wants production numbers should use the *_net variants.

Apply the B_HUGE Pareto filter (atr_pct_rank_60d >= 0.30) if --apply-pareto
is set (recommended for production builds).

Usage:
    python -m eqidv2.v17D_apply_realistic_costs \
        --trades   outputs_v17D_phase0/trades_candE4_refined_enriched.csv \
        --universe configs/universe.csv \
        --output   outputs_v17D_phase0/trades_candE4_production.csv \
        --apply-pareto
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm

PARETO_FILTERS = {
    ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
        ("atr_pct_rank_60d", ">=", 0.30),
    ],
}


def _apply_pareto(df: pd.DataFrame) -> tuple:
    n_in = len(df)
    drop_count = 0
    keep_mask = pd.Series(True, index=df.index)
    for (side, setup), rules in PARETO_FILTERS.items():
        mask = (df["side"] == side) & (df["setup"] == setup)
        sub = df[mask]
        for feat, op, thr in rules:
            if feat not in sub.columns:
                continue
            col = pd.to_numeric(sub[feat], errors="coerce")
            if op == ">=":
                rule_keep = col >= thr
            elif op == "<=":
                rule_keep = col <= thr
            else:
                raise ValueError(op)
            # NaN rows: keep them (don't drop on missing data)
            rule_keep = rule_keep.fillna(True)
            drop_count += int((~rule_keep).sum())
            keep_mask.loc[sub.index] &= rule_keep
    out = df[keep_mask].copy()
    print(f"  Pareto filters: dropped {drop_count} trades "
          f"({n_in} -> {len(out)})")
    return out, drop_count


def apply_costs(
    df: pd.DataFrame, universe: pd.DataFrame, leverage: float = 5.0,
) -> pd.DataFrame:
    out = df.copy()
    adv_lookup = dict(zip(universe["ticker"], universe["adv_rs_cr"]))
    out["adv_rs_cr_universe"] = out["ticker"].map(adv_lookup).fillna(0.0)
    out["adv_bucket"] = out["adv_rs_cr_universe"].apply(cm.adv_bucket_for)

    cost_per_row = []
    for _, r in out.iterrows():
        bucket = r["adv_bucket"]
        kind = r["outcome"] if r["outcome"] in ("TARGET", "SL") else "TARGET"
        cost_per_row.append(cm.costs_pct_for_v17C(bucket, kind))
    out["cost_pct_realistic"] = cost_per_row

    gross_price = pd.to_numeric(
        out.get("pnl_pct_gross_price", out.get("pnl_pct_price")),
        errors="coerce",
    )
    out["pnl_pct_price_net"] = gross_price - out["cost_pct_realistic"]
    out["pnl_pct_net"] = out["pnl_pct_price_net"] * leverage
    if "candE_size_mult" in out.columns:
        size_mult = pd.to_numeric(out["candE_size_mult"], errors="coerce").fillna(1.0)
    else:
        size_mult = pd.Series(1.0, index=out.index)
    out["pnl_pct_eff_net"] = out["pnl_pct_net"] * size_mult
    if "position_size_rs" in out.columns:
        pos_rs = pd.to_numeric(out["position_size_rs"], errors="coerce").fillna(20000.0)
    else:
        pos_rs = pd.Series(20000.0, index=out.index)
    out["pnl_rs_eff_net"] = out["pnl_pct_eff_net"] / 100.0 * pos_rs
    return out


def _pf(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce").dropna()
    w, l = float(s[s > 0].sum()), float(-s[s < 0].sum())
    if l <= 0:
        return float("inf") if w > 0 else 0.0
    return w / l


def _print_summary(df: pd.DataFrame):
    pf_eff = _pf(df["pnl_pct_eff_net"])
    win = (df["outcome"] == "TARGET").mean() * 100
    daily = pd.DataFrame({
        "d": pd.to_datetime(df["trade_date"]).dt.date,
        "p": df["pnl_pct_eff_net"],
    }).groupby("d")["p"].sum()
    n_days = max(daily.index.nunique(), 1)
    day_win = (daily > 0).mean() * 100
    sum_rs = float(pd.to_numeric(df["pnl_rs_eff_net"], errors="coerce").sum())

    print()
    print("=" * 78)
    print(f"v17D production trades (REALISTIC costs, sizing-aware):")
    print(f"  n_trades        = {len(df)}")
    print(f"  n_days          = {n_days}  (per_day = {len(df)/n_days:.2f})")
    print(f"  PF              = {pf_eff:.3f}")
    print(f"  win%            = {win:.2f}")
    print(f"  day_win%        = {day_win:.2f}")
    print(f"  sum_pnl_rs_eff  = Rs {sum_rs:>+12,.0f}")
    print()

    print("Per-setup:")
    for (side, setup), g in df.groupby(["side", "setup"]):
        pf_g = _pf(g["pnl_pct_eff_net"])
        win_g = (g["outcome"] == "TARGET").mean() * 100
        rs_g = float(pd.to_numeric(g["pnl_rs_eff_net"], errors="coerce").sum())
        print(f"  {side:5s} {setup:<32s}  n={len(g):<4d}  "
              f"PF={pf_g:.3f}  win%={win_g:5.2f}  Rs={rs_g:>+10,.0f}")

    print()
    print("Per-ADV-bucket:")
    for bucket, g in df.groupby("adv_bucket"):
        pf_b = _pf(g["pnl_pct_eff_net"])
        rs_b = float(pd.to_numeric(g["pnl_rs_eff_net"], errors="coerce").sum())
        print(f"  {bucket:<10s}  n={len(g):<4d}  PF={pf_b:.3f}  "
              f"Rs={rs_b:>+10,.0f}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True)
    p.add_argument("--universe", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--leverage", type=float, default=5.0)
    p.add_argument("--apply-pareto", action="store_true",
                   help="apply Pareto-found filters (B_HUGE atr_rank>=0.30)")
    args = p.parse_args()

    df = pd.read_csv(args.trades)
    u = pd.read_csv(args.universe)

    print(f"v17D apply realistic costs — {len(df)} input trades")
    if args.apply_pareto:
        df, _ = _apply_pareto(df)

    out = apply_costs(df, u, leverage=args.leverage)
    out.to_csv(args.output, index=False)
    _print_summary(out)
    print(f"\nWrote: {args.output}")


if __name__ == "__main__":
    main()
