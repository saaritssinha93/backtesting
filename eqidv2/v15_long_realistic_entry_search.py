#!/usr/bin/env python3
"""
v15_long_realistic_entry_search.py
====================================
Backtest parameter search using REALISTIC entry prices for the LONG side.

Problem being solved
---------------------
The existing backtest fills LONG entries at the model "trigger" price (e.g.
close[huge_bar] + buffer).  In live trading the signal fires ~35 seconds after
the bar closes, so the actual fill is at the bar's CLOSE price — which is
typically 0.5–0.8% above the trigger for B_HUGE_C1_CLOSE_RECLAIM_BREAK setups.
With a 0.75% SL measured from trigger, that gap leaves almost no room.

This script re-runs the V15 LONG backtest with entry_at_bar_close=True
(and optionally entry_at_next_open=True) across a SL/target grid to find
the parameters that actually work under live conditions.

Usage
-----
    python v15_long_realistic_entry_search.py
    python v15_long_realistic_entry_search.py --entry-mode next_open
    python v15_long_realistic_entry_search.py --entry-mode bar_close --slip-cap 0.005

Output
------
    outputs_v15_realistic_entry/
        summary.csv          — full grid results
        best_trades.csv      — trades from the top config
        best_daywise.csv     — daily breakdown for the top config
        exact_validation.json — top-N configs re-run with full runner
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import avwap_combined_runner_v15 as runner
from avwap_v11_refactored.avwap_common_v7_sweep import StrategyConfig
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker_prepared,
)
from eqidv2_runtime_paths import DATA_15M_DIR
import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly as core

OUTPUT_DIR = ROOT / "outputs_v15_realistic_entry"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAX_WORKERS = int(os.getenv("EQIDV2_REALISTIC_WORKERS", "4"))


# ---------------------------------------------------------------------------
# Grid
# ---------------------------------------------------------------------------
# With bar-close entry the effective entry is ~0.5–0.8% above trigger, so SL
# needs to be wider.  Target grid centres around the 1% profit goal.
SL_VALUES   = [0.0050, 0.0060, 0.0070, 0.0075, 0.0080, 0.0090, 0.0100, 0.0110, 0.0120]
TGT_VALUES  = [0.0090, 0.0100, 0.0110, 0.0120, 0.0130, 0.0140, 0.0150]
BE_VALUES   = [0.0050, 0.0060, 0.0070, 0.0080]  # breakeven trigger
TRAIL_VALUES = [0.0025, 0.0030, 0.0035]          # trailing stop after BE

# Score weights (mirrors existing search scripts)
SCORE_WEIGHTS: List[Tuple[str, bool, float]] = [
    ("sum_pnl_pct",      False, 0.25),
    ("profit_factor",    False, 0.30),
    ("day_win_pct",      False, 0.20),
    ("trade_win_pct",    False, 0.10),
    ("max_drawdown_pct", True,  0.15),
]

TOP_N_EXACT = 4   # number of configs to re-validate with full runner


# ---------------------------------------------------------------------------
# Build base LONG config (with entry-mode overrides applied)
# ---------------------------------------------------------------------------
def _build_long_cfg(
    stop_pct: float,
    target_pct: float,
    be_trigger_pct: float,
    trail_pct: float,
    entry_at_bar_close: bool,
    entry_at_next_open: bool,
    max_entry_slip_pct: float,
) -> StrategyConfig:
    short_cfg, long_cfg = runner.apply_live_parity_profile(
        runner.v15_short_cfg(), runner.v15_long_cfg()
    )
    long_cfg.stop_pct = float(stop_pct)
    long_cfg.target_pct = float(target_pct)
    long_cfg.be_trigger_pct = float(be_trigger_pct)
    long_cfg.trail_pct = float(trail_pct)
    long_cfg.entry_at_bar_close = bool(entry_at_bar_close)
    long_cfg.entry_at_next_open = bool(entry_at_next_open)
    long_cfg.max_entry_slip_pct = float(max_entry_slip_pct)
    return long_cfg


# ---------------------------------------------------------------------------
# Load tickers + data (once, shared across workers via initializer)
# ---------------------------------------------------------------------------
_TICKER_DATA: Dict[str, pd.DataFrame] = {}


def _init_worker(tickers_and_data: Dict[str, pd.DataFrame]) -> None:
    global _TICKER_DATA
    _TICKER_DATA = tickers_and_data


def _scan_ticker(args: Tuple[str, StrategyConfig]) -> List[Any]:
    ticker, cfg = args
    df = _TICKER_DATA.get(ticker)
    if df is None or df.empty:
        return []
    from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
        scan_all_days_for_ticker_prepared,
        _prepare_session_bars_for_scan,
    )
    df_prep = _prepare_session_bars_for_scan(df, cfg)
    return scan_all_days_for_ticker_prepared(ticker, df_prep, cfg)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def _compute_metrics(trades: List[Any]) -> Dict[str, float]:
    if not trades:
        return {
            "trades": 0, "trade_days": 0, "trades_per_day": 0.0,
            "sum_pnl_pct": 0.0, "avg_pnl_pct": 0.0,
            "trade_win_pct": 0.0, "day_win_pct": 0.0,
            "profit_factor": 0.0, "max_drawdown_pct": 0.0,
            "sl_rate_pct": 0.0, "target_rate_pct": 0.0, "eod_rate_pct": 0.0,
        }

    pnls = [t.pnl_pct for t in trades]
    days = sorted({t.trade_date for t in trades})
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_wins = sum(wins) if wins else 0.0
    gross_losses = abs(sum(losses)) if losses else 0.0
    pf = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    day_pnls = {}
    for t in trades:
        day_pnls.setdefault(t.trade_date, []).append(t.pnl_pct)
    day_totals = [sum(v) for v in day_pnls.values()]
    day_wins = sum(1 for d in day_totals if d > 0)

    # Max drawdown (cumulative)
    cum = np.cumsum(pnls)
    peak = np.maximum.accumulate(cum)
    dd_series = peak - cum
    max_dd = float(np.max(dd_series)) if len(dd_series) else 0.0

    outcomes = [str(t.outcome).upper() for t in trades]
    n = len(trades)
    return {
        "trades": n,
        "trade_days": len(days),
        "trades_per_day": n / max(len(days), 1),
        "sum_pnl_pct": sum(pnls),
        "avg_pnl_pct": sum(pnls) / n,
        "trade_win_pct": len(wins) / n * 100,
        "day_win_pct": day_wins / max(len(day_totals), 1) * 100,
        "profit_factor": pf,
        "max_drawdown_pct": max_dd,
        "sl_rate_pct": outcomes.count("SL") / n * 100,
        "target_rate_pct": outcomes.count("TARGET") / n * 100,
        "eod_rate_pct": outcomes.count("EOD") / n * 100,
    }


def _score(metrics: Dict[str, float]) -> float:
    if metrics["trades"] < 20:
        return -999.0
    vals: Dict[str, float] = {}
    for key, _, _ in SCORE_WEIGHTS:
        vals[key] = metrics.get(key, 0.0)
    # Normalise across this single row (relative scoring done externally)
    return sum(w * ((-1 if inv else 1) * vals[key]) for key, inv, w in SCORE_WEIGHTS)


# ---------------------------------------------------------------------------
# Main grid search
# ---------------------------------------------------------------------------
def run_grid(
    entry_at_bar_close: bool,
    entry_at_next_open: bool,
    max_entry_slip_pct: float,
    workers: int = MAX_WORKERS,
) -> List[Dict]:
    print(f"[INFO] Loading 15-min data from {DATA_15M_DIR} ...", flush=True)
    tickers = core.get_ticker_list(str(DATA_15M_DIR))
    print(f"[INFO] {len(tickers)} tickers found", flush=True)

    ticker_data: Dict[str, pd.DataFrame] = {}
    for t in tickers:
        path = Path(DATA_15M_DIR) / f"{t}_stocks_indicators_15min.parquet"
        if path.exists():
            try:
                df = pd.read_parquet(path)
                if not df.empty:
                    ticker_data[t] = df
            except Exception:
                pass
    print(f"[INFO] Loaded data for {len(ticker_data)} tickers", flush=True)

    # Build all (sl, tgt, be, trail) combos
    combos: List[Tuple[float, float, float, float]] = [
        (sl, tgt, be, trail)
        for sl in SL_VALUES
        for tgt in TGT_VALUES
        for be in BE_VALUES
        for trail in TRAIL_VALUES
        if be < tgt and be > sl * 0.5  # sanity: BE fires before target, after partial move
    ]
    print(f"[INFO] Grid: {len(combos)} SL/TGT/BE/trail combos", flush=True)

    results = []
    for idx, (sl, tgt, be, trail) in enumerate(combos, 1):
        print(
            f"[{idx:>4}/{len(combos)}] SL={sl*100:.2f}% TGT={tgt*100:.2f}% "
            f"BE={be*100:.2f}% trail={trail*100:.2f}% ...",
            end=" ", flush=True,
        )
        cfg = _build_long_cfg(sl, tgt, be, trail, entry_at_bar_close, entry_at_next_open, max_entry_slip_pct)

        all_trades = []
        if workers > 1:
            with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(ticker_data,)) as ex:
                futs = {ex.submit(_scan_ticker, (t, cfg)): t for t in ticker_data}
                for fut in as_completed(futs):
                    try:
                        all_trades.extend(fut.result() or [])
                    except Exception as e:
                        pass
        else:
            from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
                _prepare_session_bars_for_scan,
            )
            for t, df in ticker_data.items():
                df_prep = _prepare_session_bars_for_scan(df, cfg)
                all_trades.extend(scan_all_days_for_ticker_prepared(t, df_prep, cfg))

        m = _compute_metrics(all_trades)
        m.update({
            "stop_pct": sl, "target_pct": tgt,
            "be_trigger_pct": be, "trail_pct": trail,
            "rr": tgt / sl if sl > 0 else 0.0,
            "entry_mode": "next_open" if entry_at_next_open else ("bar_close" if entry_at_bar_close else "trigger"),
            "max_slip_pct": max_entry_slip_pct,
        })
        print(f"PF={m['profit_factor']:.3f} win={m['trade_win_pct']:.1f}% trades={m['trades']}", flush=True)
        results.append(m)

    # Normalise scores (0–100 per metric across all combos)
    if results:
        df_res = pd.DataFrame(results)
        for key, inv, _ in SCORE_WEIGHTS:
            col = df_res[key]
            mn, mx = col.min(), col.max()
            rng = mx - mn if mx != mn else 1.0
            normed = (col - mn) / rng * 100.0
            df_res[f"norm_{key}"] = normed if not inv else (100.0 - normed)
        df_res["score"] = sum(
            df_res[f"norm_{key}"] * w for key, _, w in SCORE_WEIGHTS
        )
        results = df_res.sort_values("score", ascending=False).to_dict("records")

    return results


# ---------------------------------------------------------------------------
# Save + exact-validate top configs
# ---------------------------------------------------------------------------
def _save_results(results: List[Dict], output_dir: Path) -> None:
    if not results:
        print("[WARN] No results to save.", flush=True)
        return

    summary_path = output_dir / "summary.csv"
    fieldnames = list(results[0].keys())
    with open(summary_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)
    print(f"[INFO] Summary saved → {summary_path}", flush=True)

    # Top-N exact validation with full runner
    top = [r for r in results if r.get("trades", 0) >= 20][:TOP_N_EXACT]
    validation_records = []
    for rank, cfg_row in enumerate(top, 1):
        sl = float(cfg_row["stop_pct"])
        tgt = float(cfg_row["target_pct"])
        be = float(cfg_row.get("be_trigger_pct", 0.0055))
        trail = float(cfg_row.get("trail_pct", 0.003))
        em = str(cfg_row.get("entry_mode", "bar_close"))
        slip = float(cfg_row.get("max_slip_pct", 0.0))

        label = (
            f"rank{rank}_lSL{int(sl*10000):04d}_lTGT{int(tgt*10000):04d}"
            f"_BE{int(be*10000):04d}_{em}"
        )
        print(f"\n[EXACT] Validating {label} ...", flush=True)

        sub_dir = output_dir / label
        sub_dir.mkdir(parents=True, exist_ok=True)

        # Set runner flags
        runner.LONG_ENTRY_AT_BAR_CLOSE = (em == "bar_close")
        runner.LONG_ENTRY_AT_NEXT_OPEN = (em == "next_open")
        runner.LONG_MAX_ENTRY_SLIP_PCT = slip
        runner.TEST_LONG_TARGET_PCT = tgt
        runner.TEST_TARGET_OVERRIDE = True

        try:
            result = runner.run_backtest(
                output_dir=str(sub_dir),
                long_stop_pct=sl,
                long_target_pct=tgt,
                long_be_trigger_pct=be,
                long_trail_pct=trail,
            )
            if result:
                validation_records.append({"label": label, **result})
                print(f"  → PF={result.get('long',{}).get('profit_factor','?')} "
                      f"trades={result.get('long',{}).get('trades','?')}", flush=True)
        except Exception as e:
            print(f"  [ERROR] {e}", flush=True)

    if validation_records:
        with open(output_dir / "exact_validation.json", "w") as f:
            json.dump(validation_records, f, indent=2, default=str)
        print(f"[INFO] Exact validation saved → {output_dir / 'exact_validation.json'}", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="V15 LONG realistic-entry parameter search")
    parser.add_argument(
        "--entry-mode",
        choices=["trigger", "bar_close", "next_open"],
        default="bar_close",
        help="Entry price mode (default: bar_close)",
    )
    parser.add_argument(
        "--slip-cap",
        type=float,
        default=0.003,
        help="Max allowed slip from trigger to actual entry, 0=disabled (default: 0.003)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Parallel worker count (default: {MAX_WORKERS})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help="Output directory",
    )
    args = parser.parse_args()

    entry_at_bar_close = args.entry_mode == "bar_close"
    entry_at_next_open = args.entry_mode == "next_open"
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("=" * 65, flush=True)
    print(f"V15 LONG Realistic Entry Search", flush=True)
    print(f"  Entry mode  : {args.entry_mode}", flush=True)
    print(f"  Slip cap    : {args.slip_cap*100:.2f}%", flush=True)
    print(f"  Workers     : {args.workers}", flush=True)
    print(f"  Output dir  : {out}", flush=True)
    print(f"  Grid size   : SL×{len(SL_VALUES)} × TGT×{len(TGT_VALUES)} × BE×{len(BE_VALUES)} × trail×{len(TRAIL_VALUES)}", flush=True)
    print("=" * 65, flush=True)

    results = run_grid(
        entry_at_bar_close=entry_at_bar_close,
        entry_at_next_open=entry_at_next_open,
        max_entry_slip_pct=args.slip_cap,
        workers=args.workers,
    )

    _save_results(results, out)

    if results:
        top3 = [r for r in results if r.get("trades", 0) >= 20][:3]
        print("\n" + "=" * 65, flush=True)
        print("TOP 3 CONFIGS (by composite score):", flush=True)
        print(f"{'SL%':>6} {'TGT%':>6} {'BE%':>5} {'PF':>6} {'Win%':>6} {'SumPnL':>8} {'DD%':>7} {'Trades':>7}", flush=True)
        for r in top3:
            print(
                f"{float(r['stop_pct'])*100:>6.2f} "
                f"{float(r['target_pct'])*100:>6.2f} "
                f"{float(r.get('be_trigger_pct',0))*100:>5.2f} "
                f"{float(r['profit_factor']):>6.3f} "
                f"{float(r['trade_win_pct']):>6.1f} "
                f"{float(r['sum_pnl_pct']):>8.1f} "
                f"{float(r['max_drawdown_pct']):>7.2f} "
                f"{int(r['trades']):>7}",
                flush=True,
            )
        print("=" * 65, flush=True)


if __name__ == "__main__":
    main()
