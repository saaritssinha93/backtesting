# -*- coding: utf-8 -*-
"""
v17r POST-FILTER -- apply a v17r candidate filter to an existing baseline
trade CSV (output of avwap_combined_runner_v17r_setup_lab_5min.py with
EQIDV17R_CANDIDATE=baseline) and write a candidate-tagged CSV alongside.

Saves ~1h45m vs re-running the full backtest. Produces:
    avwap_longshort_trades_v16_5min_ALL_DAYS_<TS>_v17r_<CAND>.csv
    avwap_daywise_breakdown_v16_5min_ALL_DAYS_<TS>_v17r_<CAND>.csv
    avwap_combined_runner_<TS>_v17r_<CAND>.txt   (one-line log)

Usage:
    python _v17r_post_filter.py [--candidate B] [--input <path>]

Defaults:
    --candidate = B
    --input     = latest avwap_longshort_trades_*.csv in
                  C:/TradingData/eqidv2/outputs_v17r_setup_lab_5min/
"""
from __future__ import annotations

import argparse
import math
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

import _v17r_setup_lab_analyzer as L


OUT_DIR = Path("C:/TradingData/eqidv2/outputs_v17r_setup_lab_5min")


# Mirrors V17R_CANDIDATE_SPECS from avwap_combined_runner_v17r_setup_lab_5min.py.
CANDIDATE_SPECS: Dict[str, Dict[Tuple[str, str], List]] = {
    "A": {
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [],
        ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"): [],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [],
        ("SHORT", "C_OR_BREAKDOWN"): [],
        ("SHORT", "D_AVWAP_LOSE_REVERSAL"): [],
        ("SHORT", "D_EMA20_REJECTION"): [],
        ("SHORT", "E_VWAP_BAND_FADE"): [],
    },
    "B": {
        # Loosened on 2026-04-29 (hour caps) and 2026-04-30 (B_AVWAP +
        # A_MOD_BREAK_C1_LOW filters swapped/removed because original
        # were overfit). Aggregate: 562 -> 802.
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
            ("avwap_dist_atr_signal", ">=", 1.5260),
            ("entry_hour", "<=", 12.0),
        ],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
            ("avwap_dist_atr_signal", "<=", 2.0),
        ],
        ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 1.5133),
            ("entry_hour", "<=", 10.5),
        ],
        ("LONG",  "D_EMA20_BOUNCE"): [
            ("quality_score", ">=", 1.3833),
            ("ema20_gap_atr_signal", ">=", -2.1524),
            ("adx_signal", "<=", 37.6647),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            # No filter (original rsi>=25.22 was overfit; OOS unchanged).
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("entry_hour", "<=", 10.25),
            ("quality_score", ">=", 0.4577),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
    },
    "D": {  # high quality
        ("LONG",  "A_MOD_BREAK_C1_HIGH"): [
            ("avwap_dist_atr_signal", ">=", 1.5260),
            ("entry_hour", "<=", 9.6667),
        ],
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [
            ("avwap_dist_atr_signal", "<=", 2.0),
        ],
        ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): [
            ("avwap_dist_atr_signal", ">=", 1.5133),
            ("entry_hour", "<=", 9.9167),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "D_EMA20_REJECTION"): [
            ("entry_hour", "<=", 10.0833),
            ("quality_score", ">=", 0.4577),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
    },
    "E": {  # count-preserving
        ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"): [],
        ("LONG",  "D_EMA20_BOUNCE"): [
            ("quality_score", ">=", 1.3833),
            ("ema20_gap_atr_signal", ">=", -2.1524),
            ("adx_signal", "<=", 37.6647),
        ],
        ("SHORT", "A_MOD_BREAK_C1_LOW"): [
            ("rsi_signal", ">=", 25.2176),
        ],
        ("SHORT", "C_OR_BREAKDOWN"): [
            ("avwap_dist_atr_signal", ">=", 1.5731),
            ("rsi_signal", "<=", 28.9934),
        ],
        ("SHORT", "G_LOWER_LOW_BREAK"): [
            ("atr_pct_signal", ">=", 0.0070),
        ],
    },
}

# F1 = B + G_HH rescue
CANDIDATE_SPECS["F1"] = dict(CANDIDATE_SPECS["B"])
CANDIDATE_SPECS["F1"][("LONG", "G_HIGHER_HIGH_BREAK")] = [
    ("avwap_dist_atr_signal", ">=", 0.8826),
    ("entry_hour", "between", 9.8333, 10.6667),
    ("quality_score", ">=", 1.7248),
    ("stochk_signal", "<=", 97.176),
    ("atr_pct_signal", ">=", 0.0045),
]

# F = B + all 3 rescued LONGs
CANDIDATE_SPECS["F"] = dict(CANDIDATE_SPECS["F1"])
CANDIDATE_SPECS["F"][("LONG", "A_MOD_CLOSE_CONTINUATION_BREAK")] = [
    ("avwap_dist_atr_signal", "between", 0.8928, 2.0578),
    ("rsi_signal", "<=", 81.086),
]
CANDIDATE_SPECS["F"][("LONG", "C_OR_BREAKOUT")] = [
    ("quality_score", ">=", 1.4789),
    ("entry_hour", "<=", 10.5),
    ("ema20_gap_atr_signal", ">=", -3.94),
    ("atr_pct_signal", "<=", 0.00934),
    ("adx_signal", ">=", 23.234),
]


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------
def apply_chain(sub: pd.DataFrame, chain: List) -> pd.DataFrame:
    if sub is None or len(sub) == 0 or not chain:
        return sub
    keep = pd.Series(True, index=sub.index)
    for step in chain:
        feat = step[0]
        op = step[1]
        col = (sub["entry_hour"] if feat == "entry_hour"
               else pd.to_numeric(sub.get(feat), errors="coerce"))
        if op == ">=":
            keep &= (col >= step[2]).fillna(False)
        elif op == "<=":
            keep &= (col <= step[2]).fillna(False)
        elif op == "between":
            keep &= col.between(step[2], step[3]).fillna(False)
        elif op == "==":
            sval = sub.get(feat, pd.Series("", index=sub.index)).astype(str)
            keep &= sval.eq(str(step[2]))
    return sub.loc[keep]


def apply_spec(df: pd.DataFrame, spec: Dict[Tuple[str, str], List]) -> pd.DataFrame:
    keep_idx = []
    for (side, setup), chain in spec.items():
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        if chain:
            sub = apply_chain(sub, chain)
        keep_idx.extend(list(sub.index))
    return df.loc[df.index.isin(keep_idx)].copy()


# ---------------------------------------------------------------------------
# Daywise breakdown rebuild
# ---------------------------------------------------------------------------
def rebuild_daywise(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame()
    pnl = pd.to_numeric(df["pnl_pct_price"], errors="coerce").fillna(0.0)
    df = df.copy()
    df["_pnl"] = pnl
    out = df.groupby("trade_date").agg(
        n_trades=("ticker", "size"),
        n_wins=("_pnl", lambda s: int((s > 0).sum())),
        n_losses=("_pnl", lambda s: int((s < 0).sum())),
        n_target=("outcome", lambda s: int((s.astype(str) == "TARGET").sum())),
        n_sl=("outcome", lambda s: int((s.astype(str) == "SL").sum())),
        n_eod=("outcome", lambda s: int((s.astype(str) == "EOD").sum())),
        sum_pnl_pct_price=("_pnl", "sum"),
        avg_pnl_pct_price=("_pnl", "mean"),
    ).reset_index()
    out["win_pct"] = (out["n_wins"] / out["n_trades"] * 100.0).round(2)
    out["day_outcome"] = (out["sum_pnl_pct_price"] > 0).map(
        {True: "WIN", False: "LOSS"}
    )
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def find_latest_baseline_csv() -> Path:
    cands = sorted(OUT_DIR.glob("avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv"))
    cands = [c for c in cands if "_v17r_" not in c.name]
    if not cands:
        raise SystemExit("[v17r-post] no baseline trade CSV found; "
                         "run avwap_combined_runner_v17r_setup_lab_5min.py "
                         "with EQIDV17R_CANDIDATE=baseline first")
    cands.sort(key=lambda p: p.stat().st_mtime)
    return cands[-1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidate", default="B",
                    choices=sorted(CANDIDATE_SPECS.keys()))
    ap.add_argument("--input", default=None,
                    help="Override input CSV path")
    args = ap.parse_args()

    in_path = Path(args.input) if args.input else find_latest_baseline_csv()
    if not in_path.exists():
        raise SystemExit(f"[v17r-post] input not found: {in_path}")

    spec = CANDIDATE_SPECS[args.candidate]
    print(f"[v17r-post] input:      {in_path}")
    print(f"[v17r-post] candidate:  {args.candidate}  ({len(spec)} setups in spec)")

    df = L.load_csv(in_path)
    df_in = df.copy()
    n_in = len(df_in)

    # Per-setup verbose.
    print()
    for (side, setup), chain in spec.items():
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        n_setup_in = len(sub)
        if chain:
            sub_kept = apply_chain(sub, chain)
        else:
            sub_kept = sub
        print(f"  {side:5s} {setup:35s}: {n_setup_in:5d} -> {len(sub_kept):5d}")

    # Apply spec.
    df_out = apply_spec(df, spec)

    # Aggregate metrics.
    m = L.metrics(df_out)
    tr = L.metrics(df_out[df_out["split"] == "TRAIN"])
    oo = L.metrics(df_out[df_out["split"] == "OOS"])
    decay = (oo["pf"] / tr["pf"]
             if tr["pf"] > 0 and math.isfinite(oo["pf"]) and math.isfinite(tr["pf"])
             else float("nan"))

    print()
    print(f"[v17r-post] AGGREGATE candidate={args.candidate}")
    print(f"  total       n={m['n']:4d} -> {m['n']:4d}  PF={m['pf']:.3f} "
          f"win={m['win_pct']:.1f}% day-win={m['day_win_pct']:.1f}% "
          f"DD={m['max_dd_pct']:.2f}%")
    print(f"  train       n={tr['n']:4d}  PF={tr['pf']:.3f} "
          f"DD={tr['max_dd_pct']:.2f}%")
    print(f"  oos         n={oo['n']:4d}  PF={oo['pf']:.3f} "
          f"DD={oo['max_dd_pct']:.2f}% decay={decay:.2f}")
    print(f"  filter kept {len(df_out)}/{n_in} rows")

    # Drop helper columns we added in load_csv() so the CSV matches the
    # canonical schema downstream tools expect.
    helper_cols = ["trade_date_dt", "entry_hour", "entry_minute_bin",
                   "weekday", "month", "week_of_month", "split"]
    for c in helper_cols:
        if c in df_out.columns:
            df_out = df_out.drop(columns=c)

    # Build output filenames.
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_v17r_{args.candidate}"
    trades_out = OUT_DIR / f"avwap_longshort_trades_v16_5min_ALL_DAYS_{ts}{suffix}.csv"
    daywise_out = OUT_DIR / f"avwap_daywise_breakdown_v16_5min_ALL_DAYS_{ts}{suffix}.csv"
    log_out = OUT_DIR / f"avwap_combined_runner_{ts}{suffix}.txt"

    df_out.to_csv(trades_out, index=False)
    print(f"\n[v17r-post] wrote trades CSV: {trades_out.name} "
          f"({len(df_out)} rows)")

    daywise = rebuild_daywise(df_out)
    daywise.to_csv(daywise_out, index=False)
    print(f"[v17r-post] wrote daywise:    {daywise_out.name} "
          f"({len(daywise)} rows)")

    # One-line log mirroring v16's style.
    with log_out.open("w", encoding="utf-8") as fh:
        fh.write(f"[v17r-post] post-filter from baseline CSV -- candidate={args.candidate}\n")
        fh.write(f"  source: {in_path.name}\n")
        fh.write(f"  total   n_in={n_in} n_out={m['n']} kept={m['n']/n_in*100:.2f}%\n")
        fh.write(f"  PF={m['pf']:.3f} win={m['win_pct']:.2f}% "
                 f"day-win={m['day_win_pct']:.2f}% DD={m['max_dd_pct']:.2f}%\n")
        fh.write(f"  train n={tr['n']} PF={tr['pf']:.3f} DD={tr['max_dd_pct']:.2f}%\n")
        fh.write(f"  oos   n={oo['n']} PF={oo['pf']:.3f} DD={oo['max_dd_pct']:.2f}% "
                 f"decay={decay:.3f}\n")
        for (side, setup), chain in spec.items():
            sub = df_in[(df_in["side"] == side) & (df_in["setup"] == setup)]
            sub_kept = apply_chain(sub, chain) if chain else sub
            fh.write(f"  {side:5s} {setup:35s}: {len(sub):5d} -> {len(sub_kept):5d}\n")
    print(f"[v17r-post] wrote log:        {log_out.name}")
    print(f"\n[v17r-post] done.")


if __name__ == "__main__":
    main()
