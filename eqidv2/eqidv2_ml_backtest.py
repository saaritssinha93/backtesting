# -*- coding: utf-8 -*-
"""eqidv2 ML meta-label backtest filter over AVWAP trades."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from ml_meta_filter import MetaFilterConfig, MetaLabelFilter


def _safe_win_rate(df: pd.DataFrame, pnl_col: str) -> float:
    if df.empty:
        return 0.0
    return float((df[pnl_col] > 0).mean())


def _summarize(df: pd.DataFrame, kept: pd.DataFrame) -> dict:
    base_pnl = float(df["pnl_rs"].sum())
    ml_pnl = float(kept["pnl_rs_ml"].sum()) if not kept.empty else 0.0
    summary = {
        "rows": int(len(df)),
        "kept": int(len(kept)),
        "kept_pct": float((len(kept) / len(df)) * 100.0) if len(df) else 0.0,
        "base_pnl": base_pnl,
        "ml_pnl": ml_pnl,
        "pnl_delta": ml_pnl - base_pnl,
        "base_avg_trade": float(df["pnl_rs"].mean()),
        "ml_avg_trade": float(kept["pnl_rs_ml"].mean()) if not kept.empty else 0.0,
        "base_win_rate": _safe_win_rate(df, "pnl_rs"),
        "ml_win_rate": _safe_win_rate(kept, "pnl_rs_ml"),
    }
    return summary


def _stock_breakdown(kept: pd.DataFrame, top_n: int) -> pd.DataFrame:
    ticker_col = "ticker" if "ticker" in kept.columns else "symbol" if "symbol" in kept.columns else None
    if ticker_col is None or kept.empty:
        return pd.DataFrame()

    by_stock = (
        kept.groupby(ticker_col, dropna=False)
        .agg(
            trades=(ticker_col, "size"),
            avg_p_win=("p_win", "mean"),
            avg_confidence=("confidence_multiplier", "mean"),
            ml_pnl=("pnl_rs_ml", "sum"),
            base_pnl=("pnl_rs", "sum"),
        )
        .sort_values(["avg_p_win", "ml_pnl"], ascending=[False, False])
        .head(top_n)
        .reset_index()
    )
    by_stock.rename(columns={ticker_col: "ticker"}, inplace=True)
    return by_stock


def run(input_csv: str, output_csv: str, threshold: float, summary_json: str | None, top_stocks_csv: str | None, top_n: int) -> None:
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

    summary = _summarize(df, kept)
    print(f"rows={summary['rows']} kept={summary['kept']} kept_pct={summary['kept_pct']:.2f}%")
    print(f"base_pnl={summary['base_pnl']:.2f} ml_pnl={summary['ml_pnl']:.2f} delta={summary['pnl_delta']:.2f}")
    print(f"base_win_rate={summary['base_win_rate']:.3f} ml_win_rate={summary['ml_win_rate']:.3f}")

    if summary_json:
        p = Path(summary_json)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if top_stocks_csv:
        tops = _stock_breakdown(kept, top_n=top_n)
        p = Path(top_stocks_csv)
        p.parent.mkdir(parents=True, exist_ok=True)
        tops.to_csv(p, index=False)
        if not tops.empty:
            print("Top scanned stocks (ML-filtered):")
            print(tops.to_string(index=False))
        else:
            print("Top stock breakdown not generated (ticker/symbol column missing or no kept trades).")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--output-csv", default="eqidv2/outputs/ml_filtered_trades.csv")
    ap.add_argument("--threshold", type=float, default=0.60)
    ap.add_argument("--summary-json", default="eqidv2/outputs/ml_backtest_summary.json")
    ap.add_argument("--top-stocks-csv", default="eqidv2/outputs/ml_top_scanned_stocks.csv")
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()
    run(args.input_csv, args.output_csv, args.threshold, args.summary_json, args.top_stocks_csv, args.top_n)


if __name__ == "__main__":
    main()
