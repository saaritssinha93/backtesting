# -*- coding: utf-8 -*-
"""
avwap_ml_backtest_runner.py  (Strategy v1)
==========================================

Backtest AVWAP combined (LONG + SHORT) exactly like avwap_combined_runner.py,
and then apply the ML meta-label filter layer (p_win gating + confidence sizing)
to produce a second, ML-filtered backtest output.

Strategy v1 changes:
- Full v1 feature pass-through for ML scoring
- Confidence-based sizing with ATR volatility cap
- Risk controls: max open positions, daily loss kill-switch, time cutoff
- R_net / P&L diagnostics in output

Outputs:
- RAW (no ML): side-wise + combined stats + charts
- ML (with filter/sizing): side-wise + combined stats + charts
- CSVs: RAW and ML trade-level CSVs saved to outputs/
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from avwap_v11_refactored import avwap_combined_runner as base_runner
except Exception:  # pragma: no cover
    import avwap_combined_runner as base_runner
from ml_meta_filter import MetaFilterConfig, MetaLabelFilter, build_feature_vector


# -----------------------------
# Small helpers
# -----------------------------
def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, str) and x.strip() == "":
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _to_dt_series(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, errors="coerce")
    # treat as IST-naive if no tz
    try:
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(base_runner.IST)
        else:
            ts = ts.dt.tz_convert(base_runner.IST)
    except Exception:
        pass
    return ts


def _pick_col(df: pd.DataFrame, *names: str) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    return None


def _max_consecutive(seq: List[int], target: int) -> int:
    best = 0
    cur = 0
    for x in seq:
        if x == target:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _equity_and_drawdown(daily_pnl_pct: pd.Series) -> Tuple[pd.Series, float]:
    """Return (equity_curve, max_drawdown_pct) using cumulative sum of daily pnl%."""
    equity = daily_pnl_pct.fillna(0.0).cumsum()
    running_max = equity.cummax()
    dd = equity - running_max
    max_dd = float(dd.min()) if len(dd) else 0.0
    return equity, abs(max_dd)


def _sharpe_sortino_calmar(daily_pnl_pct: pd.Series) -> Tuple[float, float, float]:
    """
    Use daily pnl% as "returns" proxy.
    Annualize with sqrt(252).
    Calmar uses annualized mean / max drawdown.
    """
    r = daily_pnl_pct.dropna()
    if len(r) < 2:
        return 0.0, 0.0, 0.0

    mu = float(r.mean())
    sd = float(r.std(ddof=1))
    sharpe = (mu / sd) * (252.0 ** 0.5) if sd > 1e-12 else 0.0

    downside = r[r < 0]
    dd_sd = float(downside.std(ddof=1)) if len(downside) >= 2 else 0.0
    sortino = (mu / dd_sd) * (252.0 ** 0.5) if dd_sd > 1e-12 else 0.0

    _, max_dd = _equity_and_drawdown(r)
    ann = mu * 252.0
    calmar = (ann / max_dd) if max_dd > 1e-12 else 0.0
    return sharpe, sortino, calmar


def _profit_factor(pnl: pd.Series) -> float:
    wins = pnl[pnl > 0].sum()
    losses = pnl[pnl < 0].sum()
    denom = abs(losses)
    return float(wins / denom) if denom > 1e-12 else float("inf") if wins > 0 else 0.0


def _format_block(title: str) -> None:
    print("\n" + "=" * 20 + f" {title} " + "=" * 20)


def print_detailed_stats(df: pd.DataFrame, label: str) -> None:
    """
    Print stats in the exact style the user shared.
    Requires:
      - side, pnl_pct (net), pnl_pct_gross (gross), outcome, entry_time_ist
    """
    if df is None or df.empty:
        print(f"\n==================== {label} ====================")
        print("Total trades                  : 0")
        print("===================================================================================")
        return

    d = df.copy()

    # Ensure required cols exist
    if "pnl_pct" not in d.columns:
        d["pnl_pct"] = pd.to_numeric(d.get("pnl_pct_price", 0.0), errors="coerce").fillna(0.0)
    if "pnl_pct_gross" not in d.columns:
        d["pnl_pct_gross"] = pd.to_numeric(d.get("pnl_pct_gross_price", 0.0), errors="coerce").fillna(0.0)

    outcome_col = "outcome" if "outcome" in d.columns else None

    # Dates
    et_col = _pick_col(d, "entry_time_ist", "entry_time", "entry_time_local", "signal_datetime", "entry_time_dt")
    if et_col is None:
        # fallback: try exit_time_ist
        et_col = _pick_col(d, "exit_time_ist")
    if et_col is not None:
        et = _to_dt_series(d[et_col])
        d["_day"] = et.dt.date
    else:
        d["_day"] = "NA"

    total_trades = int(len(d))
    unique_days = int(pd.Series(d["_day"]).nunique())

    # Outcome counts
    if outcome_col:
        oc = d[outcome_col].astype(str).str.upper()
        target_hits = int((oc == "TARGET").sum())
        sl_hits = int((oc == "SL").sum())
        eod_hits = int((oc == "EOD").sum())
        # Some pipelines don't store BE in outcome; infer BE as net pnl==0
        be_hits = int(((d["pnl_pct"].abs() < 1e-12) & ~(oc.isin(["TARGET", "SL"]))).sum())
    else:
        target_hits = sl_hits = eod_hits = 0
        be_hits = int((d["pnl_pct"].abs() < 1e-12).sum())

    # Rates
    hr = (target_hits / total_trades) * 100 if total_trades else 0.0
    slr = (sl_hits / total_trades) * 100 if total_trades else 0.0
    ber = (be_hits / total_trades) * 100 if total_trades else 0.0
    eodr = (eod_hits / total_trades) * 100 if total_trades else 0.0

    pnl_net = pd.to_numeric(d["pnl_pct"], errors="coerce").fillna(0.0)
    pnl_gross = pd.to_numeric(d["pnl_pct_gross"], errors="coerce").fillna(0.0)

    avg_net = float(pnl_net.mean()) if total_trades else 0.0
    sum_net = float(pnl_net.sum()) if total_trades else 0.0
    avg_gross = float(pnl_gross.mean()) if total_trades else 0.0
    sum_gross = float(pnl_gross.sum()) if total_trades else 0.0

    # win/loss stats
    wins = pnl_net[pnl_net > 0]
    losses = pnl_net[pnl_net < 0]
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = float(losses.mean()) if len(losses) else 0.0
    pf = _profit_factor(pnl_net)

    # daily curve for DD + sharpe/sortino/calmar
    daily = d.groupby("_day")["pnl_pct"].sum() if et_col is not None else pnl_net
    _, max_dd = _equity_and_drawdown(daily)
    sharpe, sortino, calmar = _sharpe_sortino_calmar(daily)

    # consecutive wins/losses (trade-level)
    #  1 = win, -1 = loss, 0 = BE
    seq = [1 if x > 0 else (-1 if x < 0 else 0) for x in pnl_net.tolist()]
    max_consec_wins = _max_consecutive(seq, 1)
    max_consec_losses = _max_consecutive(seq, -1)

    # Print block
    print(f"\n==================== {label} ====================")
    print(f"Total trades                  : {total_trades}")
    print(f"Unique trade days             : {unique_days}")
    print(f"TARGET hits                   : {target_hits}  | hit-rate  = {hr:.2f}%")
    print(f"SL hits                       : {sl_hits}  | sl-rate   = {slr:.2f}%")
    print(f"BE exits                      : {be_hits}  | be-rate   = {ber:.2f}%")
    print(f"EOD exits                     : {eod_hits}  | eod-rate  = {eodr:.2f}%")
    print(f"Avg PnL % (net, per trade)    : {avg_net:.4f}%")
    print(f"Sum PnL % (net, all trades)   : {sum_net:.4f}%")
    print(f"Avg PnL % (gross, per trade)  : {avg_gross:.4f}%")
    print(f"Sum PnL % (gross, all trades) : {sum_gross:.4f}%")
    print(f"Profit factor                 : {pf:.3f}" if np.isfinite(pf) else "Profit factor                 : inf")
    print(f"Avg winning trade             : {avg_win:.4f}%")
    print(f"Avg losing trade              : {avg_loss:.4f}%")
    print(f"Max drawdown (cumul PnL %)    : {max_dd:.4f}%")
    print(f"Sharpe ratio (annualized)     : {sharpe:.3f}")
    print(f"Sortino ratio (annualized)    : {sortino:.3f}")
    print(f"Calmar ratio                  : {calmar:.3f}")
    print(f"Max consecutive wins          : {max_consec_wins}")
    print(f"Max consecutive losses        : {max_consec_losses}")
    print("=" * 83)


def _build_signal_dict_from_trade_row(r: pd.Series) -> Dict[str, Any]:
    """
    Build the dict expected by MetaLabelFilter.predict_pwin().
    Passes all available columns for v1 feature support.
    """
    sig: Dict[str, Any] = {}
    # Pass through all numeric columns as potential features
    for col in r.index:
        try:
            val = r[col]
            if isinstance(val, (int, float, np.floating, np.integer)):
                sig[col] = float(val)
            else:
                sig[col] = val
        except (TypeError, ValueError):
            sig[col] = r[col]

    # Ensure legacy aliases are present for backward compat
    side = str(r.get("side", "LONG")).upper().strip()
    sig["side"] = side
    sig.setdefault("quality_score", _safe_float(r.get("quality_score", 0.0), 0.0))
    sig.setdefault("atr_pct", _safe_float(r.get("atr_pct_signal", r.get("atr_pct", 0.0)), 0.0))
    sig.setdefault("rsi", _safe_float(r.get("rsi_signal", r.get("rsi", 50.0)), 50.0))
    sig.setdefault("adx", _safe_float(r.get("adx_signal", r.get("adx", 20.0)), 20.0))
    return sig


def apply_ml_filter_and_size(
    trades: pd.DataFrame,
    meta_filter: MetaLabelFilter,
    ml_threshold: float,
) -> pd.DataFrame:
    """
    Strategy v1: Adds p_win + confidence_multiplier (vol-capped),
    filters trades below threshold, scales ROI/PnL by confidence_multiplier.

    Returns a NEW dataframe (ML version).
    """
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
        # Vol-capped confidence multiplier
        atr_pctile = float(sig.get("atr_pctile_50", sig.get("atr_pctile", 50.0)))
        m = float(meta_filter.confidence_multiplier_with_vol_cap(p, atr_pctile))
        p_list.append(p)
        m_list.append(m)
        mode_list.append("model" if using_model else "heur")

    df["p_win"] = p_list
    df["confidence_multiplier"] = m_list
    df["ml_threshold"] = float(ml_threshold)
    df["ml_mode"] = mode_list

    # Filter
    df = df[df["confidence_multiplier"] > 0.0].copy()
    if df.empty:
        return df

    # Preserve original ROI/PnL columns
    scale_cols = []
    for c in ["pnl_pct", "pnl_pct_gross", "pnl_rs", "pnl_rs_gross",
              "pnl_pct_price", "pnl_pct_gross_price"]:
        if c in df.columns:
            scale_cols.append(c)

    for c in scale_cols:
        df[c + "_raw"] = df[c]

    for c in scale_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) * df["confidence_multiplier"].astype(float)

    return df


def _print_pwin_diagnostics(
    trades_with_pwin: pd.DataFrame,
    threshold: float,
) -> None:
    """Print p_win distribution diagnostics when trades are gated out."""
    if trades_with_pwin.empty or "p_win" not in trades_with_pwin.columns:
        return
    p = trades_with_pwin["p_win"].astype(float)
    dist = MetaLabelFilter.pwin_distribution_summary(p.tolist())
    above = int((p >= threshold).sum())
    print(f"\n    p_win distribution across {dist['count']} RAW trades:")
    print(f"      min={dist['min']:.4f}  p10={dist['p10']:.4f}  p25={dist['p25']:.4f}  "
          f"median={dist['median']:.4f}  p75={dist['p75']:.4f}  p90={dist['p90']:.4f}  "
          f"max={dist['max']:.4f}")
    print(f"      mean={dist['mean']:.4f}  std={dist['std']:.4f}")
    print(f"      Trades with p_win >= {threshold:.3f}: {above}/{dist['count']}")


def apply_ml_filter_adaptive(
    trades: pd.DataFrame,
    meta_filter: MetaLabelFilter,
    ml_threshold: float,
    report_path: Optional[str] = None,
) -> Tuple[pd.DataFrame, float]:
    """
    Wrapper around apply_ml_filter_and_size that implements adaptive threshold
    fallback when the initial threshold gates out all trades.

    Returns (filtered_df, effective_threshold).
    """
    # First attempt at the requested threshold
    result = apply_ml_filter_and_size(trades, meta_filter, ml_threshold)

    if not result.empty:
        return result, ml_threshold

    if trades.empty:
        return result, ml_threshold

    # --- All trades gated out: attempt adaptive relaxation ---

    # Re-compute p_win column for diagnostics (it was on the unfiltered df inside
    # apply_ml_filter_and_size but that df is local; recompute cheaply from trades)
    using_model = meta_filter.model is not None and bool(meta_filter.features)
    p_list: List[float] = []
    for _, r in trades.iterrows():
        sig = _build_signal_dict_from_trade_row(r)
        p_list.append(float(meta_filter.predict_pwin(sig)))
    diag_df = trades.copy()
    diag_df["p_win"] = p_list

    print(f"\n[ML] All {len(trades)} trades gated out at threshold {ml_threshold:.3f}.")
    _print_pwin_diagnostics(diag_df, ml_threshold)

    # Try the training report's optimal threshold
    optimal_t = meta_filter.load_optimal_threshold(report_path)
    if optimal_t is not None and optimal_t < ml_threshold:
        print(f"\n[ML] Adaptive fallback: retrying with training-optimal threshold {optimal_t:.3f} "
              f"(from meta_train_report.json)...")
        result = apply_ml_filter_and_size(trades, meta_filter, optimal_t)
        if not result.empty:
            print(f"[ML] Adaptive threshold {optimal_t:.3f} recovered {len(result)} trades "
                  f"(take rate {len(result)/len(trades)*100:.1f}%).")
            return result, optimal_t
        print(f"[ML] Adaptive threshold {optimal_t:.3f} still gates out all trades.")

    # Last resort: use p75 of p_win distribution as threshold
    p_arr = np.array(p_list, dtype=float)
    p75 = float(np.percentile(p_arr, 75))
    if p75 > 0.01 and p75 < ml_threshold:
        fallback_t = round(p75, 4)
        print(f"\n[ML] Last-resort fallback: retrying with p75-based threshold {fallback_t:.4f}...")
        result = apply_ml_filter_and_size(trades, meta_filter, fallback_t)
        if not result.empty:
            print(f"[ML] p75 threshold {fallback_t:.4f} recovered {len(result)} trades "
                  f"(take rate {len(result)/len(trades)*100:.1f}%).")
            return result, fallback_t

    # Nothing worked
    return pd.DataFrame(), ml_threshold


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ml-threshold", type=float, default=0.50, help="p_win threshold for taking trades (default: 0.50, matches training-optimal)")
    ap.add_argument("--model-path", type=str, default="models/meta_model.pkl", help="Path to exported meta model")
    ap.add_argument("--features-path", type=str, default="models/meta_features.json", help="Path to exported feature list")
    ap.add_argument("--outputs-dir", type=str, default="", help="Optional override outputs directory")
    ap.add_argument("--save-csv", action="store_true", help="Save RAW and ML trade CSVs to outputs")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    outputs_dir = Path(args.outputs_dir) if args.outputs_dir else (script_dir / "outputs")
    outputs_dir.mkdir(parents=True, exist_ok=True)

    
    # Charts should go into outputs/charts/
    charts_dir = outputs_dir / "charts"
    raw_charts_dir = charts_dir / "RAW"
    ml_charts_dir = charts_dir / "ML"
    raw_charts_dir.mkdir(parents=True, exist_ok=True)
    ml_charts_dir.mkdir(parents=True, exist_ok=True)
    ts = base_runner.now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = outputs_dir / f"avwap_ml_backtest_runner_{ts}.txt"

    orig_stdout, orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as log_fh:
        sys.stdout = base_runner._Tee(orig_stdout, log_fh)
        sys.stderr = base_runner._Tee(orig_stderr, log_fh)
        try:
            print("\n" + "=" * 70)
            print("AVWAP COMBINED backtest + ML meta filter (runner)")
            print("=" * 70)

            # Resolve 5-min dir (for exit resolution)
            dir_5m = base_runner._resolve_5min_dir()
            print(f"[INFO] 5-min data directory: {dir_5m}")

            short_cfg = base_runner.default_short_config(reports_dir=outputs_dir)
            long_cfg = base_runner.default_long_config(reports_dir=outputs_dir)

            print(f"[INFO] ML threshold: {args.ml_threshold:.3f}")
            print(f"[INFO] Model path: {args.model_path}")
            print(f"[INFO] Features path: {args.features_path}")

            cfg = MetaFilterConfig(
                model_path=args.model_path,
                feature_path=args.features_path,
                pwin_threshold=float(args.ml_threshold),
            )
            meta_filter = MetaLabelFilter(cfg)
            print(f"[INFO] ML mode: {'MODEL' if meta_filter.model is not None else 'HEURISTIC FALLBACK'}")

            # ---- PHASE 1: Scan for entry signals using 15-min data ----
            print("\n[PHASE 1] Scanning for entry signals using 15-min data...")
            short_df_raw = base_runner._run_side_parallel("SHORT", short_cfg, base_runner.MAX_WORKERS)
            long_df_raw = base_runner._run_side_parallel("LONG", long_cfg, base_runner.MAX_WORKERS)

            if short_df_raw.empty and long_df_raw.empty:
                print("[DONE] No trades found.")
                return

            # ---- PHASE 2: Re-resolve exits using 5-min data ----
            print("\n[PHASE 2] Re-resolving exits using 5-min data...")
            short_df_raw = base_runner._resolve_exits_5min(short_df_raw, dir_5m)
            long_df_raw = base_runner._resolve_exits_5min(long_df_raw, dir_5m)

            # ---- PHASE 3: Add notional P&L columns ----
            print("\n[PHASE 3] Computing notional/ROI P&L columns...")
            short_df_raw = base_runner._add_notional_pnl(short_df_raw)
            long_df_raw = base_runner._add_notional_pnl(long_df_raw)
            combined_raw = pd.concat([short_df_raw, long_df_raw], ignore_index=True)
            combined_raw = combined_raw.sort_values(_pick_col(combined_raw, "entry_time_ist", "entry_time") or combined_raw.columns[0])

            # Save CSV always (user requested)
            raw_csv = outputs_dir / f"avwap_trades_RAW_{ts}.csv"
            combined_raw.to_csv(raw_csv, index=False)
            print(f"[SAVE] RAW trades CSV: {raw_csv}")

            # ---- RAW PRINTS ----
            print_detailed_stats(short_df_raw, "SHORT (net of slippage+comm, 5-min exits)")
            print_detailed_stats(long_df_raw, "LONG (net of slippage+comm, 5-min exits)")
            print_detailed_stats(combined_raw, "COMBINED (net of slippage+comm, 5-min exits)")

            # Notional summary (Rs.)
            base_runner._print_notional_pnl(combined_raw)

            # Charts (RAW) — correct signature
            try:
                base_runner.generate_enhanced_charts(
                    combined=combined_raw,
                    short_df=short_df_raw,
                    long_df=long_df_raw,
                    save_dir=raw_charts_dir,
                    ts_label=f"RAW_{ts}",
                )
            except Exception as e:
                print(f"[WARN] Chart generation (RAW) failed: {e}")

            # ---- PHASE 4: Apply ML filter + sizing (with adaptive fallback) ----
            print("\n[PHASE 4] Applying ML meta-filter (p_win gating) + confidence sizing...")

            combined_ml, effective_threshold = apply_ml_filter_adaptive(
                combined_raw, meta_filter, float(args.ml_threshold),
            )
            short_df_ml = combined_ml[combined_ml["side"].astype(str).str.upper().eq("SHORT")].copy() if not combined_ml.empty else pd.DataFrame()
            long_df_ml = combined_ml[~combined_ml["side"].astype(str).str.upper().eq("SHORT")].copy() if not combined_ml.empty else pd.DataFrame()

            if effective_threshold != float(args.ml_threshold):
                print(f"[ML] NOTE: Effective threshold was relaxed from "
                      f"{float(args.ml_threshold):.3f} → {effective_threshold:.3f}")

            if combined_ml.empty:
                print("\n[ML] After ML gating (including adaptive fallback), no trades remain.")
                print("[ML] Possible causes:")
                print("       - Model is too pessimistic on this data period")
                print("       - Feature distribution shifted significantly from training data")
                print("       - Consider retraining the model with more recent data")
                print(f"\n[LOG] Full console saved to: {log_path}")
                return

            # Save ML CSV always
            ml_csv = outputs_dir / f"avwap_trades_ML_{ts}.csv"
            combined_ml.to_csv(ml_csv, index=False)
            print(f"[SAVE] ML trades CSV: {ml_csv}")

            # ---- ML PRINTS ----
            print_detailed_stats(short_df_ml, "SHORT (ML filtered, net of slippage+comm, 5-min exits)")
            print_detailed_stats(long_df_ml, "LONG (ML filtered, net of slippage+comm, 5-min exits)")
            print_detailed_stats(combined_ml, "COMBINED (ML filtered, net of slippage+comm, 5-min exits)")

            base_runner._print_notional_pnl(combined_ml)

            # Charts (ML) — correct signature
            try:
                base_runner.generate_enhanced_charts(
                    combined=combined_ml,
                    short_df=short_df_ml,
                    long_df=long_df_ml,
                    save_dir=ml_charts_dir,
                    ts_label=f"ML_{ts}",
                )
            except Exception as e:
                print(f"[WARN] Chart generation (ML) failed: {e}")

            # ML diagnostics
            print("\n================ ML DIAGNOSTICS (v1) ================")
            print(f"Trades RAW                 : {len(combined_raw)}")
            print(f"Trades after ML gate       : {len(combined_ml)}")
            print(f"Take rate                  : {len(combined_ml)/max(1,len(combined_raw))*100:.1f}%")
            print(f"Requested threshold        : {float(args.ml_threshold):.3f}")
            print(f"Effective threshold         : {effective_threshold:.3f}")
            if "p_win" in combined_ml.columns:
                print(f"Mean p_win (taken trades)  : {combined_ml['p_win'].mean():.4f}")
                print(f"Median p_win               : {combined_ml['p_win'].median():.4f}")
            if "confidence_multiplier" in combined_ml.columns:
                print(f"Mean confidence_multiplier : {combined_ml['confidence_multiplier'].mean():.4f}")
                print(f"Min confidence_multiplier  : {combined_ml['confidence_multiplier'].min():.4f}")
                print(f"Max confidence_multiplier  : {combined_ml['confidence_multiplier'].max():.4f}")
            if "ml_mode" in combined_ml.columns:
                mode_counts = combined_ml["ml_mode"].value_counts().to_dict()
                print(f"ML mode breakdown          : {mode_counts}")
            print("=====================================================")

            print(f"\n[LOG] Full console saved to: {log_path}")
            print("[DONE] ML backtest runner complete.")

        finally:
            sys.stdout = orig_stdout
            sys.stderr = orig_stderr


if __name__ == "__main__":
    main()
