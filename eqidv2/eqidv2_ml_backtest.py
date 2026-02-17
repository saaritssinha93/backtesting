# -*- coding: utf-8 -*-
"""
eqidv2 ML meta-label backtest filter over AVWAP trades.

Strategy v1: supports expanded feature set (30 features) and
confidence-based sizing with ATR volatility cap.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ml_meta_filter import MetaFilterConfig, MetaLabelFilter, build_feature_vector


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
    # R_net stats if available
    if "r_net" in kept.columns and not kept.empty:
        summary["avg_r_net_kept"] = float(kept["r_net"].mean())
    if "p_win" in kept.columns and not kept.empty:
        summary["avg_p_win_kept"] = float(kept["p_win"].mean())
        summary["median_p_win_kept"] = float(kept["p_win"].median())
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


def _build_signal_for_row(r: pd.Series) -> dict:
    """
    Build a signal dict for MetaLabelFilter.predict_pwin().
    Passes all available v1 feature columns through; the filter will
    use whichever features match its loaded model.
    """
    sig = {}
    # Pass through all columns as potential features
    for col in r.index:
        try:
            sig[col] = float(r[col]) if isinstance(r[col], (int, float, np.floating, np.integer)) else r[col]
        except (TypeError, ValueError):
            sig[col] = r[col]

    # Ensure legacy aliases are present
    sig.setdefault("quality_score", 0.0)
    sig.setdefault("atr_pct", sig.get("atr_pct_signal", 0.0))
    sig.setdefault("rsi", sig.get("rsi_signal", 50.0))
    sig.setdefault("adx", sig.get("adx_signal", 20.0))
    sig.setdefault("side", "SHORT")
    return sig


def run(input_csv: str, output_csv: str, threshold: float, summary_json: str | None, top_stocks_csv: str | None, top_n: int) -> None:
    df = pd.read_csv(input_csv)
    if df.empty:
        raise ValueError("No trades found in input CSV")

    if "pnl_rs" not in df.columns:
        raise ValueError("Missing required column: pnl_rs")

    filt = MetaLabelFilter(MetaFilterConfig(pwin_threshold=threshold))

    def _score(r):
        sig = _build_signal_for_row(r)
        p = filt.predict_pwin(sig)
        # Use vol-capped multiplier if ATR percentile available
        atr_pctile = float(sig.get("atr_pctile_50", 50.0))
        m = filt.confidence_multiplier_with_vol_cap(p, atr_pctile)
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
    if "avg_p_win_kept" in summary:
        print(f"avg_p_win={summary['avg_p_win_kept']:.4f} median_p_win={summary['median_p_win_kept']:.4f}")

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
    ap.add_argument("--threshold", type=float, default=0.62)
    ap.add_argument("--summary-json", default="eqidv2/outputs/ml_backtest_summary.json")
    ap.add_argument("--top-stocks-csv", default="eqidv2/outputs/ml_top_scanned_stocks.csv")
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()
    run(args.input_csv, args.output_csv, args.threshold, args.summary_json, args.top_stocks_csv, args.top_n)


if __name__ == "__main__":
    main()
