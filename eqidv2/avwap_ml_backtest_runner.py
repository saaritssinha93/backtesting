# -*- coding: utf-8 -*-
"""
avwap_ml_backtest_runner.py
===========================

ML backtest runner (RAW + ML):
- Runs standard AVWAP combined backtest (LONG + SHORT)
- Produces RAW metrics/output
- Applies ML meta filter + confidence sizing
- Produces ML metrics/output

Output files:
- avwap_short_trades_ALL_DAYS_<ts>.csv
- avwap_longshort_trades_ALL_DAYS_<ts>.csv
- avwap_longshort_trades_ALL_DAYS_ML_<ts>.csv

Note:
- avwap_long_trades_ALL_DAYS_*.csv is intentionally not generated.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

import avwap_combined_runner as base_runner
from ml_meta_filter import MetaFilterConfig, MetaLabelFilter


def _detect_5m_suffix(dir_5m: Path) -> str:
    if not dir_5m.is_dir():
        return ".parquet"
    for sf in dir_5m.iterdir():
        if sf.is_file() and sf.suffix:
            return sf.suffix
    return ".parquet"


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, str) and x.strip() == "":
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _build_signal_dict_from_trade_row(r: pd.Series) -> Dict[str, Any]:
    return {
        "side": str(r.get("side", "LONG")).upper().strip(),
        "quality_score": _safe_float(r.get("quality_score", 0.0), 0.0),
        "atr_pct_signal": _safe_float(r.get("atr_pct_signal", r.get("atr_pct", 0.0)), 0.0),
        "rsi_signal": _safe_float(r.get("rsi_signal", r.get("rsi", 50.0)), 50.0),
        "adx_signal": _safe_float(r.get("adx_signal", r.get("adx", 20.0)), 20.0),
    }


def _apply_ml_filter_and_size(
    trades: pd.DataFrame,
    meta_filter: MetaLabelFilter,
    ml_threshold: float,
) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()

    meta_filter.cfg.pwin_threshold = float(ml_threshold)
    df = trades.copy()

    using_model = meta_filter.model is not None and bool(meta_filter.features)
    p_list: List[float] = []
    m_list: List[float] = []
    mode_list: List[str] = []

    for _, r in df.iterrows():
        sig = _build_signal_dict_from_trade_row(r)
        p = float(meta_filter.predict_pwin(sig))
        m = float(meta_filter.confidence_multiplier(p))
        p_list.append(p)
        m_list.append(m)
        mode_list.append("model" if using_model else "heur")

    df["p_win"] = p_list
    df["confidence_multiplier"] = m_list
    df["ml_threshold"] = float(ml_threshold)
    df["ml_mode"] = mode_list

    # ML gate
    df = df[df["confidence_multiplier"] > 0.0].copy()
    if df.empty:
        return df

    # Confidence-based sizing effect
    scale_cols = []
    for c in [
        "pnl_pct",
        "pnl_pct_gross",
        "pnl_rs",
        "pnl_rs_gross",
        "pnl_pct_price",
        "pnl_pct_gross_price",
    ]:
        if c in df.columns:
            scale_cols.append(c)

    for c in scale_cols:
        df[c + "_raw"] = df[c]
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) * df["confidence_multiplier"].astype(float)

    return df


def _save_trade_csvs(
    short_raw: pd.DataFrame,
    combined_raw: pd.DataFrame,
    combined_ml: pd.DataFrame,
    outputs_dir: Path,
    ts: str,
) -> Dict[str, Path]:
    paths = {
        "short_raw": outputs_dir / f"avwap_short_trades_ALL_DAYS_{ts}.csv",
        "combined_raw": outputs_dir / f"avwap_longshort_trades_ALL_DAYS_{ts}.csv",
        "combined_ml": outputs_dir / f"avwap_longshort_trades_ALL_DAYS_ML_{ts}.csv",
    }
    short_raw.to_csv(paths["short_raw"], index=False)
    combined_raw.to_csv(paths["combined_raw"], index=False)
    combined_ml.to_csv(paths["combined_ml"], index=False)
    return paths


def _print_output_paths(csv_paths: Dict[str, Path], outputs_dir: Path, log_path: Path) -> None:
    print(f"\n[FILE SAVED][SHORT RAW]   {csv_paths['short_raw']}")
    print(f"[FILE SAVED][COMBINED]    {csv_paths['combined_raw']}")
    print(f"[FILE SAVED][COMBINED ML] {csv_paths['combined_ml']}")
    print(f"[OUTPUTS DIR] {outputs_dir}")
    print(f"[CONSOLE LOG] {log_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outputs-dir", type=str, default="", help="Optional override outputs directory")
    ap.add_argument("--max-workers", type=int, default=base_runner.MAX_WORKERS, help="Parallel worker count")
    ap.add_argument("--no-charts", action="store_true", help="Skip chart generation")
    ap.add_argument("--ml-threshold", type=float, default=0.60, help="p_win threshold for taking trades")
    ap.add_argument("--model-path", type=str, default="models/meta_model.pkl", help="Path to meta model")
    ap.add_argument("--features-path", type=str, default="models/meta_features.json", help="Path to feature json")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    outputs_dir = Path(args.outputs_dir) if args.outputs_dir else (script_dir / "outputs")
    outputs_dir.mkdir(parents=True, exist_ok=True)

    ts = base_runner.now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = outputs_dir / f"avwap_ml_backtest_runner_{ts}.txt"

    orig_stdout, orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as log_fh:
        sys.stdout = base_runner._Tee(orig_stdout, log_fh)
        sys.stderr = base_runner._Tee(orig_stderr, log_fh)
        try:
            print("=" * 70)
            print("AVWAP ML backtest runner (RAW + ML)")
            print("=" * 70)

            dir_5m = base_runner._resolve_5min_dir()
            print(f"[INFO] 5-min data directory: {dir_5m}")

            short_cfg = base_runner.default_short_config(reports_dir=outputs_dir)
            long_cfg = base_runner.default_long_config(reports_dir=outputs_dir)

            if base_runner.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
                short_cfg.min_bars_left_after_entry = 0
                long_cfg.min_bars_left_after_entry = 0
            if base_runner.FORCE_LIVE_PARITY_DISABLE_TOPN:
                short_cfg.enable_topn_per_day = False
                long_cfg.enable_topn_per_day = False

            ml_cfg = MetaFilterConfig(
                model_path=args.model_path,
                feature_path=args.features_path,
                pwin_threshold=float(args.ml_threshold),
            )
            meta_filter = MetaLabelFilter(ml_cfg)
            print(f"[INFO] ML mode: {'MODEL' if meta_filter.model is not None else 'HEURISTIC'}")
            print(f"[INFO] ML threshold: {args.ml_threshold:.3f}")
            print("-" * 70)

            print("\n[PHASE 1] Scanning entry signals...")
            short_df = base_runner._run_side_parallel("SHORT", short_cfg, int(args.max_workers))
            long_df = base_runner._run_side_parallel("LONG", long_cfg, int(args.max_workers))

            if short_df.empty and long_df.empty:
                print("[DONE] No trades found.")
                combined_raw = pd.concat([short_df, long_df], ignore_index=True)
                combined_ml = combined_raw.copy()
                csv_paths = _save_trade_csvs(short_df, combined_raw, combined_ml, outputs_dir, ts)
                _print_output_paths(csv_paths, outputs_dir, log_path)
                return

            print("\n[PHASE 2] Re-resolving exits on 5-min...")
            suffix_5m = _detect_5m_suffix(dir_5m)
            if not short_df.empty:
                short_df = base_runner._resolve_exits_5min(short_df, dir_5m, suffix_5m, short_cfg.parquet_engine)
            if not long_df.empty:
                long_df = base_runner._resolve_exits_5min(long_df, dir_5m, suffix_5m, long_cfg.parquet_engine)

            print("\n[PHASE 3] Computing RAW P&L columns...")
            if not short_df.empty:
                short_df = base_runner._add_notional_pnl(short_df)
                short_df = base_runner._sort_trades_for_output(short_df)
            if not long_df.empty:
                long_df = base_runner._add_notional_pnl(long_df)
                long_df = base_runner._sort_trades_for_output(long_df)

            combined_raw = pd.concat([short_df, long_df], ignore_index=True)
            combined_raw = base_runner._add_notional_pnl(combined_raw)
            combined_raw = base_runner._sort_trades_for_output(combined_raw)

            base_runner.print_metrics("SHORT RAW", base_runner.compute_backtest_metrics(short_df))
            base_runner.print_metrics("LONG RAW", base_runner.compute_backtest_metrics(long_df))
            base_runner.print_metrics("COMBINED RAW", base_runner.compute_backtest_metrics(combined_raw))
            print("\n[RAW] NOTIONAL P&L SUMMARY")
            base_runner._print_notional_pnl(combined_raw)

            print("\n[PHASE 4] Applying ML filter + sizing...")
            combined_ml = _apply_ml_filter_and_size(combined_raw, meta_filter, float(args.ml_threshold))
            if combined_ml.empty:
                print("[ML] No trades passed ML filter.")
                short_ml = combined_ml.copy()
                long_ml = combined_ml.copy()
            else:
                combined_ml = base_runner._sort_trades_for_output(combined_ml)
                short_ml = combined_ml[combined_ml["side"].astype(str).str.upper().eq("SHORT")].copy()
                long_ml = combined_ml[combined_ml["side"].astype(str).str.upper().eq("LONG")].copy()
                base_runner.print_metrics("SHORT ML", base_runner.compute_backtest_metrics(short_ml))
                base_runner.print_metrics("LONG ML", base_runner.compute_backtest_metrics(long_ml))
                base_runner.print_metrics("COMBINED ML", base_runner.compute_backtest_metrics(combined_ml))
                print(f"[ML] trades_raw={len(combined_raw)} trades_ml={len(combined_ml)}")

            print("\n[ML] NOTIONAL P&L SUMMARY")
            base_runner._print_notional_pnl(combined_ml)

            csv_paths = _save_trade_csvs(short_df, combined_raw, combined_ml, outputs_dir, ts)

            if not args.no_charts and not combined_raw.empty:
                base_runner.generate_enhanced_charts(
                    combined_raw, short_df, long_df,
                    save_dir=outputs_dir / "charts" / "RAW",
                    ts_label=f"RAW_{ts}",
                )
            if not args.no_charts and not combined_ml.empty:
                base_runner.generate_enhanced_charts(
                    combined_ml, short_ml, long_ml,
                    save_dir=outputs_dir / "charts" / "ML",
                    ts_label=f"ML_{ts}",
                )

            _print_output_paths(csv_paths, outputs_dir, log_path)
            print("[DONE] ML backtest complete.")

        finally:
            sys.stdout = orig_stdout
            sys.stderr = orig_stderr

    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()
