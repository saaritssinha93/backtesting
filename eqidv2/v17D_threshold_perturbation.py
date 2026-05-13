"""v17D Step 0.2 — threshold perturbation sensitivity.

Re-applies the v17C Cand-E filter chains with each numeric threshold
shifted by +/-5%, +/-10%, +/-15% and reports per-setup PF at each level.

Robustness gate: a setup passes if PF stays >= 1.50 under +/-10% perturbation.

This script consumes a "raw" trades CSV (the engine output BEFORE
Cand-E filtering) and the v17C filter spec, simulates the filter at each
threshold perturbation, and computes PF on the surviving trades.

Usage:
    python -m eqidv2.v17D_threshold_perturbation \
        --raw-trades outputs_v17C_noNF_live_5min/avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv \
        --output v17D_perturbation.csv

If your output CSVs have already been Cand-E filtered, this script will
under-count surviving trades. Use the unfiltered v17C engine output (the
ALL_DAYS file *without* the _candE / _candE3 suffix).
"""
from __future__ import annotations

import argparse

import pandas as pd

# Default Cand-E4 spec mirrored from v17C (numeric thresholds only).
# Each entry: (feature_column, direction, threshold_value)
DEFAULT_FILTER_SPEC = {
    ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
        ("bars_from_open",        "<=", 9.0),
        ("avwap_dist_atr_signal", ">=", 1.5766),
    ],
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
        ("atr_pct_signal",        ">=", 0.0040),
        ("entry_bar_vol_ratio",   ">=", 0.8842),
    ],
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
        ("bars_from_open",        "<=", 4.0),
        ("ema20_gap_atr_signal",  "<=", 3.4968),
        ("entry_hour",            "<=", 9.9167),
    ],
    ("LONG",  "D_EMA20_BOUNCE"): [
        ("atr_pct_signal",        ">=", 0.0051),
        ("stochk_signal",         "<=", 54.0876),
        ("avwap_dist_atr_signal", ">=", 1.5329),
    ],
    ("SHORT", "A_MOD_BREAK_C1_LOW"): [
        ("atr_pct_signal",        ">=", 0.0037),
        ("stochk_signal",         "<=", 12.4427),
    ],
    ("SHORT", "G_LOWER_LOW_BREAK"): [
        ("rsi_signal",            ">=", 27.3299),
        ("entry_hour",            "<=", 10.1667),
    ],
}


def _pf(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce").dropna()
    w = float(s[s > 0].sum())
    l = float(-s[s < 0].sum())
    if l <= 0:
        return float("inf") if w > 0 else 0.0
    return w / l


def _apply_chain(df: pd.DataFrame, chain: list, perturb: float) -> pd.DataFrame:
    """Apply chain with each threshold scaled by (1 + perturb).

    For ">=" rules we want "tighter" to mean higher threshold; perturb
    positive raises the threshold and rejects more trades.
    For "<=" rules, the same perturb sign should also tighten — i.e.
    LOWER the threshold. So we apply +perturb to ">=" and -perturb to "<=".
    """
    out = df.copy()
    for feature, direction, threshold in chain:
        if feature not in out.columns:
            continue
        col = pd.to_numeric(out[feature], errors="coerce")
        if direction == ">=":
            t = threshold * (1.0 + perturb)
            out = out[col >= t]
        elif direction == "<=":
            t = threshold * (1.0 - perturb)
            out = out[col <= t]
        else:
            raise ValueError(f"unknown direction {direction!r}")
    return out


def perturbation_sweep(
    df: pd.DataFrame, spec: dict, deltas=(-0.15, -0.10, -0.05, 0.0, 0.05, 0.10, 0.15),
) -> pd.DataFrame:
    df = df.copy()
    if "side" not in df.columns or "setup" not in df.columns:
        raise ValueError("trades CSV missing side/setup columns")
    if "pnl_pct" not in df.columns:
        raise ValueError("trades CSV missing pnl_pct column")

    rows = []
    for (side, setup), chain in spec.items():
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        if sub.empty:
            continue
        for delta in deltas:
            kept = _apply_chain(sub, chain, delta)
            n = len(kept)
            pf_v = _pf(kept["pnl_pct"]) if n else 0.0
            rows.append({
                "side": side, "setup": setup,
                "delta_pct": int(delta * 100),
                "n_kept": n, "n_input": len(sub),
                "pf": pf_v,
                "win_pct": (
                    (kept["outcome"] == "TARGET").mean() * 100
                    if "outcome" in kept.columns and n else 0.0
                ),
            })
    return pd.DataFrame(rows)


def robustness_verdict(sweep: pd.DataFrame, pf_floor: float = 1.5) -> pd.DataFrame:
    """For each setup, robust if PF >= pf_floor at all of -10%, 0, +10%."""
    out = []
    for (side, setup), g in sweep.groupby(["side", "setup"]):
        critical = g[g["delta_pct"].isin([-10, 0, 10])]
        all_pass = bool((critical["pf"] >= pf_floor).all()) and len(critical) == 3
        out.append({
            "side": side, "setup": setup,
            "pf_at_minus10": float(g[g["delta_pct"] == -10]["pf"].iloc[0]) if -10 in g["delta_pct"].values else float("nan"),
            "pf_at_0": float(g[g["delta_pct"] == 0]["pf"].iloc[0]) if 0 in g["delta_pct"].values else float("nan"),
            "pf_at_plus10": float(g[g["delta_pct"] == 10]["pf"].iloc[0]) if 10 in g["delta_pct"].values else float("nan"),
            "robust": all_pass,
        })
    return pd.DataFrame(out)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-trades", required=True,
                   help="trades CSV BEFORE Cand-E filtering (no _candE suffix)")
    p.add_argument("--output", required=True)
    args = p.parse_args()

    df = pd.read_csv(args.raw_trades)
    sweep = perturbation_sweep(df, DEFAULT_FILTER_SPEC)
    verdict = robustness_verdict(sweep)

    sweep.to_csv(args.output, index=False)
    verdict_path = args.output.replace(".csv", "_verdict.csv")
    verdict.to_csv(verdict_path, index=False)

    print(f"\nv17D threshold perturbation — {len(df)} trades")
    print("=" * 78)
    print("Per-setup robustness (PF at -10% / 0 / +10% perturbation):")
    print(verdict.to_string(index=False))
    n_robust = int(verdict["robust"].sum())
    print(f"\nGate: at least 4 of {len(verdict)} setups robust at PF>=1.50.")
    print(f"Result: {n_robust} of {len(verdict)} setups pass.")
    if n_robust >= 4:
        print("PASS — proceed.")
    else:
        print("FAIL — drop non-robust setups before proceeding.")
    print(f"\nWrote: {args.output}")
    print(f"Wrote verdict: {verdict_path}")


if __name__ == "__main__":
    main()
