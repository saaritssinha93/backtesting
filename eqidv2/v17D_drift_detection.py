"""v17D Step 4.5 — feature distribution drift detection (KS test).

Daily job: compare today's feature distribution (RSI, ATR%, volume_ratio,
adx, etc. across all signals fired) against the in-sample baseline.

If KS test p-value < 0.01 for any feature on 3 consecutive days, the
in-sample assumption is breaking — alert before P&L tells you.

Public API:
    capture_baseline(trades_csv, feature_cols, output_path)
        Computes baseline distributions from in-sample trades; saves as JSON.
    drift_check(trades_today, baseline_path) -> dict
        Per-feature KS p-value + alert flags.
    update_drift_history(date, drift_result, history_path)
        Append today's result; track 3-consecutive-day rule.

Usage:
    # One-time, after Phase 0:
    python -m eqidv2.v17D_drift_detection capture \
        --trades v17D_baseline_trades.csv \
        --output baseline_features.json

    # Daily (cron at EOD):
    python -m eqidv2.v17D_drift_detection check \
        --trades today_trades.csv \
        --baseline baseline_features.json \
        --history drift_history.json
"""
from __future__ import annotations

import argparse
import json
from datetime import date as date_type
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_FEATURES = [
    "rsi_signal", "atr_pct_signal", "adx_signal", "stochk_signal",
    "ema20_gap_atr_signal", "avwap_dist_atr_signal",
    "entry_bar_vol_ratio",
]


def _ks_pvalue(sample_a: np.ndarray, sample_b: np.ndarray) -> float:
    """Two-sample Kolmogorov-Smirnov p-value (no scipy dependency)."""
    a = np.sort(np.asarray(sample_a, dtype=float))
    b = np.sort(np.asarray(sample_b, dtype=float))
    a = a[~np.isnan(a)]
    b = b[~np.isnan(b)]
    if len(a) < 2 or len(b) < 2:
        return float("nan")
    n_a, n_b = len(a), len(b)
    all_vals = np.sort(np.concatenate([a, b]))
    cdf_a = np.searchsorted(a, all_vals, side="right") / n_a
    cdf_b = np.searchsorted(b, all_vals, side="right") / n_b
    d_stat = np.max(np.abs(cdf_a - cdf_b))
    en = np.sqrt(n_a * n_b / (n_a + n_b))
    # Approximate KS p-value using the asymptotic formula.
    arg = (en + 0.12 + 0.11 / en) * d_stat
    if arg < 0.001:
        return 1.0
    p = 0.0
    for k in range(1, 100):
        p += 2 * ((-1) ** (k - 1)) * np.exp(-2 * (k * arg) ** 2)
        if abs(2 * ((-1) ** (k - 1)) * np.exp(-2 * (k * arg) ** 2)) < 1e-10:
            break
    return float(max(0.0, min(1.0, p)))


def capture_baseline(
    trades_csv: str, feature_cols: list, output_path: str,
) -> dict:
    df = pd.read_csv(trades_csv)
    baseline = {"feature_cols": feature_cols, "n_trades": len(df), "samples": {}}
    for col in feature_cols:
        if col not in df.columns:
            continue
        vals = pd.to_numeric(df[col], errors="coerce").dropna().tolist()
        baseline["samples"][col] = vals
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(baseline, f)
    return baseline


def drift_check(
    trades_today: pd.DataFrame, baseline: dict,
    pvalue_threshold: float = 0.01,
) -> dict:
    out = {"flagged_features": [], "per_feature": {}}
    for col, baseline_vals in baseline["samples"].items():
        if col not in trades_today.columns:
            continue
        today_vals = pd.to_numeric(trades_today[col], errors="coerce").dropna().to_numpy()
        p = _ks_pvalue(np.array(baseline_vals), today_vals)
        out["per_feature"][col] = {
            "ks_p": p,
            "n_today": int(len(today_vals)),
            "flagged": bool(p < pvalue_threshold),
        }
        if p < pvalue_threshold:
            out["flagged_features"].append(col)
    return out


def update_drift_history(
    today: date_type, drift_result: dict, history_path: str,
    consecutive_days_for_alert: int = 3,
) -> dict:
    """Append today's drift to history, return alert verdict."""
    history = []
    p = Path(history_path)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            history = json.load(f)
    history.append({"date": str(today), "flagged": drift_result["flagged_features"]})
    history = history[-30:]   # keep last 30 days

    # 3-consecutive-day rule per feature
    feature_streak: dict = {}
    for entry in history:
        for f in entry["flagged"]:
            feature_streak.setdefault(f, 0)
        # Reset features not flagged today
        for f in list(feature_streak.keys()):
            if f in entry["flagged"]:
                feature_streak[f] += 1
            else:
                feature_streak[f] = 0

    alerts = [f for f, s in feature_streak.items() if s >= consecutive_days_for_alert]
    with open(p, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)
    return {"history_len": len(history), "alerts": alerts}


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)
    pc = sub.add_parser("capture")
    pc.add_argument("--trades", required=True)
    pc.add_argument("--output", required=True)
    pc.add_argument("--features", default=",".join(DEFAULT_FEATURES))

    pk = sub.add_parser("check")
    pk.add_argument("--trades", required=True)
    pk.add_argument("--baseline", required=True)
    pk.add_argument("--history", default="drift_history.json")
    pk.add_argument("--pvalue", type=float, default=0.01)

    args = p.parse_args()

    if args.command == "capture":
        cols = [c.strip() for c in args.features.split(",")]
        baseline = capture_baseline(args.trades, cols, args.output)
        print(f"baseline captured: {baseline['n_trades']} trades, "
              f"features: {list(baseline['samples'].keys())}")
        print(f"wrote: {args.output}")

    elif args.command == "check":
        with open(args.baseline, "r", encoding="utf-8") as f:
            baseline = json.load(f)
        df = pd.read_csv(args.trades)
        result = drift_check(df, baseline, args.pvalue)
        from datetime import date
        verdict = update_drift_history(date.today(), result, args.history)
        print(f"\nv17D drift check — {len(df)} trades today")
        print("=" * 60)
        for col, det in result["per_feature"].items():
            tag = "FLAG" if det["flagged"] else "ok"
            print(f"  {col:<28s}  p={det['ks_p']:.4f}  n={det['n_today']:<4d}  {tag}")
        if verdict["alerts"]:
            print(f"\n!! ALERT: features flagged 3+ days consecutively: "
                  f"{verdict['alerts']}")
        else:
            print(f"\nno consecutive-day alerts (history len={verdict['history_len']})")


if __name__ == "__main__":
    main()
