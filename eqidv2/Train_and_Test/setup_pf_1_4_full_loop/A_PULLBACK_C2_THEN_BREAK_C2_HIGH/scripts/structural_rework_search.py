"""Structural rework search for A_PULLBACK_C2_THEN_BREAK_C2_HIGH.

Builds a relaxed C1/C2/break research pool from full 5-minute bars, then reuses
the path-based recovery evaluator. Research-only; no final config edits.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
SCRIPT_DIR = HERE.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import full_loop_a_pullback_c2_high as fl  # noqa: E402
import path_rework_recovery as pr  # noqa: E402

SETUP = fl.SETUP
WORK = fl.WORK
DATA_5M = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
STRUCT_DIR = WORK / "structural_rework"
STRUCT_POOL = STRUCT_DIR / "structural_relaxed_pool.csv"
STRUCT_ITER = WORK / "structural_rework_iterations.csv"
STRUCT_REPORT = WORK / "STRUCTURAL_REWORK_RESULT.md"


def _safe_div(a: float, b: float) -> float:
    return float(a / b) if np.isfinite(a) and np.isfinite(b) and b != 0 else np.nan


def build_structural_pool(force: bool = False) -> pd.DataFrame:
    if STRUCT_POOL.exists() and not force:
        return pd.read_csv(STRUCT_POOL, low_memory=False, parse_dates=["sig_ts"])
    STRUCT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(DATA_5M.glob("*_stocks_indicators_5min.parquet"))
    rows: list[dict[str, Any]] = []
    t0 = time.time()
    start, end = fl.TRAIN_REQ[0], fl.TEST_REQ[1]
    needed = [
        "date", "open", "high", "low", "close", "volume", "ATR", "RSI", "ADX",
        "EMA_20", "EMA_50", "EMA_200", "VWAP", "MACD_Hist", "Stoch_%K", "Stoch_%D",
        "Daily_Change",
    ]
    for n, path in enumerate(files, start=1):
        ticker = path.name.replace("_stocks_indicators_5min.parquet", "").upper()
        try:
            df = pd.read_parquet(path)
            df = df[[c for c in needed if c in df.columns]]
        except Exception:
            continue
        if "date" not in df.columns:
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[(df["date"] >= start) & (df["date"] <= f"{end} 23:59:59")].dropna(subset=["date"]).copy()
        if len(df) < 10:
            continue
        df = df.sort_values("date").reset_index(drop=True)
        df["day"] = df["date"].dt.strftime("%Y-%m-%d")
        vol_sma = pd.to_numeric(df.get("volume"), errors="coerce").rolling(20, min_periods=5).mean()
        df["vol_ratio_calc"] = pd.to_numeric(df.get("volume"), errors="coerce") / vol_sma
        for col in ("open", "high", "low", "close", "volume", "ATR", "RSI", "ADX", "EMA_20", "EMA_50", "EMA_200", "VWAP", "MACD_Hist", "Stoch_%K", "Stoch_%D", "Daily_Change", "vol_ratio_calc"):
            if col not in df.columns:
                df[col] = np.nan
        o = df["open"].to_numpy(float)
        h = df["high"].to_numpy(float)
        l = df["low"].to_numpy(float)
        c = df["close"].to_numpy(float)
        v = df["volume"].to_numpy(float)
        atr = df["ATR"].to_numpy(float)
        rsi = df["RSI"].to_numpy(float)
        adx = df["ADX"].to_numpy(float)
        ema20 = df["EMA_20"].to_numpy(float)
        ema50 = df["EMA_50"].to_numpy(float)
        ema200 = df["EMA_200"].to_numpy(float)
        vwap = df["VWAP"].to_numpy(float)
        macdh = df["MACD_Hist"].to_numpy(float)
        st_k = df["Stoch_%K"].to_numpy(float)
        st_d = df["Stoch_%D"].to_numpy(float)
        daychg = df["Daily_Change"].to_numpy(float)
        volr = df["vol_ratio_calc"].to_numpy(float)
        day = df["day"].to_numpy(str)
        dates = df["date"].to_numpy()
        for i in range(2, len(df) - 3):
            if day[i] != day[i + 1] or day[i] != day[i + 2]:
                continue
            if not (np.isfinite(atr[i]) and atr[i] > 0 and c[i] > o[i]):
                continue
            body_atr = (c[i] - o[i]) / atr[i]
            rng = h[i] - l[i]
            close_near_high = _safe_div(h[i] - c[i], rng)
            # Superset around the original "moderate green C1" rule.
            if not (0.20 <= body_atr <= 1.60 and np.isfinite(close_near_high) and close_near_high <= 0.50):
                continue
            if not (c[i] > ema20[i] and ema20[i] >= ema50[i] and c[i] > vwap[i]):
                continue
            if not (rsi[i] >= 42 and adx[i] >= 15):
                continue
            c2 = i + 1
            if not (np.isfinite(atr[c2]) and atr[c2] > 0):
                continue
            c2_body_atr = abs(c[c2] - o[c2]) / atr[c2]
            c2_pullback_atr = (c[i] - c[c2]) / atr[i]
            c2_red = c[c2] < o[c2]
            c2_pull = c2_red or c[c2] <= c[i]
            if not (c2_pull and c2_body_atr <= 0.85 and c[c2] > vwap[c2]):
                continue
            for lag in (2, 3):
                e = i + lag
                if e >= len(df) or day[e] != day[i]:
                    continue
                trigger = h[c2] * 1.0002 + 0.05
                if not (h[e] > trigger and c[e] > trigger):
                    continue
                sig_range = h[e] - l[e]
                if not (np.isfinite(sig_range) and sig_range > 0):
                    continue
                body_pct = abs(c[e] - o[e]) / sig_range
                close_loc = (c[e] - l[e]) / sig_range
                upper_wick = (h[e] - max(o[e], c[e])) / sig_range
                lower_wick = (min(o[e], c[e]) - l[e]) / sig_range
                break_margin_atr = (c[e] - trigger) / atr[e] if np.isfinite(atr[e]) and atr[e] > 0 else np.nan
                q = (
                    50.0
                    + min(30.0, max(0.0, (adx[i] - 15.0) * 1.2 if np.isfinite(adx[i]) else 0.0))
                    + min(20.0, max(0.0, (rsi[i] - 42.0) * 0.8 if np.isfinite(rsi[i]) else 0.0))
                    + min(20.0, max(0.0, break_margin_atr * 15.0 if np.isfinite(break_margin_atr) else 0.0))
                    + min(15.0, max(0.0, (volr[i] - 1.0) * 8.0 if np.isfinite(volr[i]) else 0.0))
                )
                rows.append({
                    "ticker": ticker,
                    "side": "LONG",
                    "setup": SETUP,
                    "signal_time_ist": pd.Timestamp(dates[e]).isoformat(),
                    "sig_ts": pd.Timestamp(dates[e]),
                    "signal_open": o[e],
                    "signal_high": h[e],
                    "signal_low": l[e],
                    "signal_close": c[e],
                    "signal_volume": v[e],
                    "quality_score": round(q, 6),
                    "ranker_score": round(q, 6),
                    "reason": f"struct_relaxed_lag{lag}",
                    "status": "STRUCTURAL_REWORK",
                    "lag": lag,
                    "signal_minute": int(pd.Timestamp(dates[e]).hour * 60 + pd.Timestamp(dates[e]).minute),
                    "vol_ratio": volr[i],
                    "atr_pct": _safe_div(atr[e], c[e]),
                    "body_pct": body_pct,
                    "close_loc": close_loc,
                    "upper_wick_pct": upper_wick,
                    "lower_wick_pct": lower_wick,
                    "wick_skew_pct": upper_wick - lower_wick,
                    "signal_range_pct": _safe_div(sig_range, c[e]),
                    "vwap_dist_atr": _safe_div(c[e] - vwap[e], atr[e]),
                    "rs_pct": np.nan,
                    "market_ret_pct": np.nan,
                    "market_abs_ret_pct": np.nan,
                    "notional": 100000.0,
                    "regime": "BULL" if np.isfinite(daychg[e]) and daychg[e] > 0 else ("BEAR" if np.isfinite(daychg[e]) and daychg[e] < 0 else "NEUTRAL"),
                    "c1_body_atr": body_atr,
                    "c1_close_near_high": close_near_high,
                    "c1_rsi": rsi[i],
                    "c1_adx": adx[i],
                    "c1_vol_ratio": volr[i],
                    "c1_ema20_gt_ema50": float(ema20[i] >= ema50[i]),
                    "c1_close_gt_ema200": float(c[i] > ema200[i]) if np.isfinite(ema200[i]) else np.nan,
                    "c1_macd_hist": macdh[i],
                    "c1_stoch_k": st_k[i],
                    "c1_stoch_d": st_d[i],
                    "c2_red": float(c2_red),
                    "c2_body_atr": c2_body_atr,
                    "c2_pullback_atr": c2_pullback_atr,
                    "c2_close_gt_vwap": float(c[c2] > vwap[c2]),
                    "break_margin_atr": break_margin_atr,
                })
                break
        if n % 150 == 0 or n == len(files):
            print(f"[structural] scanned {n}/{len(files)} files rows={len(rows)} elapsed={time.time()-t0:.0f}s", flush=True)
    pool = pd.DataFrame(rows)
    if pool.empty:
        raise SystemExit("[structural] no structural rows generated")
    pool = pool.drop_duplicates(subset=["ticker", "side", "setup", "signal_time_ist", "lag"]).sort_values("signal_time_ist").reset_index(drop=True)
    pool.index.name = "sid"
    pool = pool.reset_index()
    pool.to_csv(STRUCT_POOL, index=False)
    print(f"[structural] wrote {STRUCT_POOL} rows={len(pool)}", flush=True)
    return pool


def write_structural_report(rows: pd.DataFrame, passing: list[dict[str, Any]], validation: dict[str, Any], pool: pd.DataFrame) -> None:
    for c in ["train_pf", "test_pf", "train_net", "test_net", "train_n", "test_n", "score"]:
        if c in rows.columns:
            rows[c] = pd.to_numeric(rows[c], errors="coerce")
    tested = rows[rows["test_pf"].notna()].sort_values(["test_pf", "test_net"], ascending=False)
    controlled = rows[(rows["train_pf"] >= 1.30) & (rows["train_pf"] <= 1.80) & (rows["train_net"] > 0)]
    lines = [
        f"# Structural Rework Result - {SETUP}",
        "",
        "Research-only. Generated from full `stocks_indicators_5min_eq_live2` bars through 2026-07-02.",
        "",
        "## Status",
        f"- Structural pool rows: {len(pool)}",
        f"- Path validation: {validation}",
        f"- Iterations: {len(rows)}",
        f"- Passing approval-required candidates: {len(passing)}",
        "",
        "## Best TEST Rows",
    ]
    if tested.empty:
        lines.append("No candidate reached TEST.")
    else:
        for _, r in tested.head(20).iterrows():
            lines.append(
                f"- {r['name']}: TRAIN n={r.get('train_n')} PF={r.get('train_pf')} net=Rs {float(r.get('train_net') or 0):,.0f}; "
                f"TEST n={r.get('test_n')} PF={r.get('test_pf')} net=Rs {float(r.get('test_net') or 0):,.0f}; verdict={r.get('verdict')} reason={r.get('reason')}"
            )
    lines += ["", "## Controlled TRAIN Rows"]
    if controlled.empty:
        lines.append("none")
    else:
        tmp = controlled.copy().sort_values(["test_pf", "test_net"], ascending=False)
        for _, r in tmp.head(25).iterrows():
            lines.append(
                f"- {r['name']}: TRAIN n={r.get('train_n')} PF={r.get('train_pf')} net=Rs {float(r.get('train_net') or 0):,.0f}; "
                f"TEST n={r.get('test_n')} PF={r.get('test_pf')} net=Rs {float(r.get('test_net') or 0):,.0f}; reason={r.get('reason')}"
            )
    lines += ["", "## Passing Candidates"]
    if not passing:
        lines.append("No structural candidate passed TRAIN PF > 1.30 and TEST PF > 1.40 with positive PnL/stability gates.")
    for i, p in enumerate(passing, start=1):
        lines += [
            f"### {SETUP}_structural_candidate_{i:03d}",
            f"- config: `{json.dumps(pr._json_safe(asdict(p['variant'])), sort_keys=True)}`",
            f"- TRAIN: {p['train']}",
            f"- TEST: {p['test']}",
            "- approval status: APPROVAL REQUIRED before live/paper.",
            "",
        ]
    STRUCT_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    rec = [
        f"# Approval Required Final Recommendation - {SETUP}",
        "",
        "## Current Rework Result",
        "- Adaptive all-knob filter search: 0 passing candidates.",
        "- Path rework search on original pool: 0 passing candidates.",
        f"- Structural rework search: {len(passing)} passing candidates.",
        f"- Structural report: `{STRUCT_REPORT}`",
        "",
    ]
    if passing:
        best = passing[0]
        rec += [
            "## Best Candidate",
            f"- config: `{json.dumps(pr._json_safe(asdict(best['variant'])), sort_keys=True)}`",
            f"- TRAIN: {best['train']}",
            f"- TEST: {best['test']}",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    else:
        rec += [
            "No candidate is approved for final config or live/paper watch.",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(rec) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force_pool", action="store_true")
    ap.add_argument("--force_paths", action="store_true")
    ap.add_argument("--max_iter", type=int, default=520)
    args = ap.parse_args()
    # Redirect the imported path recovery module to structural artifacts.
    pr.PATH_DIR = STRUCT_DIR / "paths"
    pr.PATHS_PARQUET = pr.PATH_DIR / "paths.parquet"
    pr.SUMMARY_CSV = pr.PATH_DIR / "summary.csv"
    pr.VALIDATION_JSON = pr.PATH_DIR / "validation.json"
    pr.ITER_CSV = STRUCT_ITER
    pr.REPORT_MD = STRUCT_REPORT
    pool = build_structural_pool(force=args.force_pool)
    manifest = fl.build_pool() if not (fl.POOL_DIR / "_manifest.json").exists() else json.loads((fl.POOL_DIR / "_manifest.json").read_text(encoding="utf-8"))
    pr.build_paths(pool, force=args.force_paths)
    validation = pr.validate_paths(pool)
    print(f"[structural] validation={validation}", flush=True)
    engine = pr.PathEngine(pool, manifest)
    rows, passing = pr.run_rework(engine, max_iter=int(args.max_iter))
    write_structural_report(rows, passing, validation, pool)
    print(f"[structural] wrote {STRUCT_REPORT}", flush=True)
    print(f"[structural] passing candidates={len(passing)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
