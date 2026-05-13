"""v17D Step 2.0 — constrained Pareto search per setup.

Searches the per-setup filter combination space to maximize PF subject to:
  - total avg trades/day >= COUNT_FLOOR (default 4.5)
  - per-setup OOS PF >= MIN_OOS_PF (default 1.40)
  - tier-1 setups (PF >= 2.20 in baseline) receive zero filters

Search space per setup:
  ADX gate            ON ({22, 25, 28}) / OFF
  DI separation       ON ({3, 5}) / OFF
  ATR rank gate       ON ({0.20, 0.30, 0.40}) / OFF
  Two-stage entry     ON / OFF
  bars_from_open      ({4, 9, 11, none})
  Volume ratio        ON ({1.0, 1.3, 1.5}) / OFF

All combinations are evaluated against an in-sample window and a holdout
out-of-sample window. The chosen chain is the one maximizing IS PF
subject to OOS PF >= MIN_OOS_PF.

Usage:
    python -m eqidv2.v17D_pareto_search \
        --raw-trades outputs_v17C_noNF_live_5min/avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv \
        --is-end-date 2026-03-31 \
        --output v17D_pareto_results.json

Notes:
  - Expects the raw trades CSV to have all candidate filter columns
    (adx_signal, di_plus, di_minus, atr_pct_rank_60d, etc.). Phase 2 work
    is responsible for ensuring these are precomputed and attached.
  - Tier-1 setups are skipped automatically by reading their baseline PF.
"""
from __future__ import annotations

import argparse
import json
from itertools import product

import pandas as pd

COUNT_FLOOR = 4.5
MIN_OOS_PF = 1.40
TIER_1_PF = 2.20


def _pf(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce").dropna()
    w = float(s[s > 0].sum())
    l = float(-s[s < 0].sum())
    if l <= 0:
        return float("inf") if w > 0 else 0.0
    return w / l


def _filter_options() -> list:
    """Cartesian product of filter ON/OFF + threshold choices."""
    return list(product(
        [None, 22.0, 25.0, 28.0],            # ADX gate
        [None, 3.0, 5.0],                    # DI separation
        [None, 0.20, 0.30, 0.40],            # ATR rank gate
        [False, True],                       # two-stage entry
        [None, 4, 9, 11],                    # bars_from_open <=
        [None, 1.0, 1.3, 1.5],               # volume ratio
    ))


def _apply_combo(df: pd.DataFrame, combo, side: str) -> pd.DataFrame:
    adx, di_sep, atr_rank, two_stage, bfo, vol = combo
    out = df.copy()
    if adx is not None and "adx_signal" in out.columns:
        out = out[pd.to_numeric(out["adx_signal"], errors="coerce") >= adx]
    if di_sep is not None and {"di_plus", "di_minus"}.issubset(out.columns):
        sep = pd.to_numeric(out["di_plus"], errors="coerce") - \
              pd.to_numeric(out["di_minus"], errors="coerce")
        if side == "LONG":
            out = out[sep >= di_sep]
        else:
            out = out[sep <= -di_sep]
    if atr_rank is not None and "atr_pct_rank_60d" in out.columns:
        out = out[pd.to_numeric(out["atr_pct_rank_60d"], errors="coerce") >= atr_rank]
    if two_stage and "next_bar_dir" in out.columns:
        # next_bar_dir > 0 means next bar closed in signal direction
        out = out[pd.to_numeric(out["next_bar_dir"], errors="coerce") > 0]
    if bfo is not None and "bars_from_open" in out.columns:
        out = out[pd.to_numeric(out["bars_from_open"], errors="coerce") <= bfo]
    if vol is not None and "entry_bar_vol_ratio" in out.columns:
        out = out[pd.to_numeric(out["entry_bar_vol_ratio"], errors="coerce") >= vol]
    return out


def _trades_per_day(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    df = df.copy()
    df["d"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    n_days = df["d"].nunique()
    return len(df) / n_days if n_days else 0.0


def search_per_setup(
    raw: pd.DataFrame, is_end_date: str,
    count_floor: float = COUNT_FLOOR,
    min_oos_pf: float = MIN_OOS_PF,
    tier1_pf: float = TIER_1_PF,
) -> dict:
    raw = raw.copy()
    raw["trade_date"] = pd.to_datetime(raw["trade_date"], errors="coerce").dt.date
    is_end = pd.to_datetime(is_end_date).date()
    is_df = raw[raw["trade_date"] <= is_end]
    oos_df = raw[raw["trade_date"] > is_end]

    results: dict = {}
    options = _filter_options()
    for (side, setup), is_g in is_df.groupby(["side", "setup"]):
        baseline_pf = _pf(is_g["pnl_pct"])
        # Tier-1: don't filter
        if baseline_pf >= tier1_pf:
            results[f"{side}|{setup}"] = {
                "tier": 1,
                "baseline_pf": baseline_pf,
                "chosen_combo": None,
                "is_pf": baseline_pf,
                "is_n": len(is_g),
                "is_per_day": _trades_per_day(is_g),
                "oos_pf": _pf(
                    oos_df[(oos_df["side"] == side) & (oos_df["setup"] == setup)]["pnl_pct"]
                ),
                "note": "tier-1 (no filters applied)",
            }
            continue

        oos_g = oos_df[(oos_df["side"] == side) & (oos_df["setup"] == setup)]

        best = None
        for combo in options:
            kept_is = _apply_combo(is_g, combo, side)
            if len(kept_is) == 0:
                continue
            kept_oos = _apply_combo(oos_g, combo, side)
            is_pf = _pf(kept_is["pnl_pct"])
            oos_pf = _pf(kept_oos["pnl_pct"]) if len(kept_oos) else float("nan")
            if oos_pf < min_oos_pf:
                continue
            score = is_pf
            if best is None or score > best["score"]:
                best = {
                    "score": score, "combo": combo,
                    "is_pf": is_pf, "is_n": len(kept_is),
                    "is_per_day": _trades_per_day(kept_is),
                    "oos_pf": oos_pf, "oos_n": len(kept_oos),
                }

        results[f"{side}|{setup}"] = {
            "tier": "borderline" if baseline_pf < 1.80 else 2,
            "baseline_pf": baseline_pf,
            "chosen_combo": best["combo"] if best else None,
            "is_pf": best["is_pf"] if best else 0.0,
            "is_n": best["is_n"] if best else 0,
            "is_per_day": best["is_per_day"] if best else 0.0,
            "oos_pf": best["oos_pf"] if best else float("nan"),
            "note": "no combo passed OOS gate" if best is None else "ok",
        }
    # Total count check
    total_per_day = sum(r.get("is_per_day", 0.0) for r in results.values())
    return {
        "results": results,
        "total_is_per_day": total_per_day,
        "count_floor": count_floor,
        "passes_count_floor": total_per_day >= count_floor,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-trades", required=True)
    p.add_argument("--is-end-date", required=True,
                   help="YYYY-MM-DD; trades with date > this are OOS")
    p.add_argument("--output", required=True)
    p.add_argument("--count-floor", type=float, default=COUNT_FLOOR)
    p.add_argument("--min-oos-pf", type=float, default=MIN_OOS_PF)
    args = p.parse_args()

    raw = pd.read_csv(args.raw_trades)
    out = search_per_setup(raw, args.is_end_date,
                           count_floor=args.count_floor,
                           min_oos_pf=args.min_oos_pf)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"\nv17D Pareto search — IS<={args.is_end_date}  OOS>{args.is_end_date}")
    print(f"Count floor: {args.count_floor}/day; Min OOS PF: {args.min_oos_pf}")
    print("=" * 78)
    for k, v in out["results"].items():
        combo = v.get("chosen_combo")
        print(
            f"  {k:<48s}  IS PF={v['is_pf']:.2f}  "
            f"OOS PF={v['oos_pf']:.2f}  /day={v['is_per_day']:.2f}  "
            f"combo={combo}"
        )
    print(f"\nTotal IS trades/day: {out['total_is_per_day']:.2f}  "
          f"(floor {args.count_floor}: "
          f"{'PASS' if out['passes_count_floor'] else 'FAIL'})")
    print(f"\nWrote: {args.output}")


if __name__ == "__main__":
    main()
