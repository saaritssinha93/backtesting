# -*- coding: utf-8 -*-
"""eqidv2 ML meta-label backtest filter over AVWAP trades."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from ml_meta_filter import MetaFilterConfig, MetaLabelFilter


def run(input_csv: str, output_csv: str, threshold: float) -> None:
    df = pd.read_csv(input_csv)
    if df.empty:
        raise ValueError("No trades found in input CSV")

    req = {"quality_score", "atr_pct_signal", "rsi_signal", "adx_signal", "side", "pnl_rs"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns: {sorted(miss)}")

    filt = MetaLabelFilter(MetaFilterConfig(pwin_threshold=threshold))

    def _score(r):
        p = filt.predict_pwin({
            "quality_score": r["quality_score"],
            "atr_pct": r["atr_pct_signal"],
            "rsi": r["rsi_signal"],
            "adx": r["adx_signal"],
            "side": r["side"],
        })
        m = filt.confidence_multiplier(p)
        return pd.Series({"p_win": p, "confidence_multiplier": m})

    scored = df.join(df.apply(_score, axis=1))
    kept = scored[scored["confidence_multiplier"] > 0].copy()
    kept["pnl_rs_ml"] = kept["pnl_rs"] * kept["confidence_multiplier"]

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    kept.to_csv(out, index=False)

    base_pnl = float(df["pnl_rs"].sum())
    ml_pnl = float(kept["pnl_rs_ml"].sum())
    print(f"rows={len(df)} kept={len(kept)}")
    print(f"base_pnl={base_pnl:.2f} ml_pnl={ml_pnl:.2f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--output-csv", default="eqidv2/outputs/ml_filtered_trades.csv")
    ap.add_argument("--threshold", type=float, default=0.60)
    args = ap.parse_args()
    run(args.input_csv, args.output_csv, args.threshold)


if __name__ == "__main__":
    main()
