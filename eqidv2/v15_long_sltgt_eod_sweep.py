# -*- coding: utf-8 -*-
"""
v15_long_sltgt_eod_sweep.py  — V15 LONG  |  SL × Target × EOD window sweep
============================================================================
Pure SL / Target / EOD strategy  — breakeven and trailing stop DISABLED.
Exit logic: SL hit  |  Target hit  |  End-of-day close.

Phases:
  Phase 1 – SL × Target grid  (10 SL levels × 10 TGT levels = ~80 valid combos)
  Phase 2 – Signal window end time  (10 variants, best SL/TGT fixed)
  Phase 3 – Combined best SL × TGT × window  (20 hand-picked combos)

All other params locked at v15 optimals:
  ADX=22, RSI=50, AVWAP_dist=0.5, QS=5, RS_thr=0.30,
  lag_b_huge=2, lag_a_mod=1, both setups enabled.

Output:
  outputs_v15_sltgt_eod_sweep/
    sweep_results.csv
    sweep_log.txt

Usage:
    python -u v15_long_sltgt_eod_sweep.py
"""

from __future__ import annotations

import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import time as dtime, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
sys.path.insert(0, str(ROOT))

# ===========================================================================
# CONSTANTS
# ===========================================================================
MAX_WORKERS      = 6
POSITION_SIZE_RS = 50_000
LEVERAGE         = 5.0
MIN_TRADES       = 15

DATA_END_SUFFIX  = "_stocks_indicators_15min.parquet"


# ===========================================================================
# WORKER
# ===========================================================================
def _worker_scan_one_ticker(args: Tuple[str, str, Any]) -> List[dict]:
    ticker, path, cfg = args
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return []
        from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
            scan_all_days_for_ticker as _scan_long,
        )
        trades = _scan_long(ticker, df, cfg)
        return [asdict(t) for t in trades]
    except Exception:
        return []


# ===========================================================================
# METRICS
# ===========================================================================
def _compute_metrics(trades: List[dict]) -> Dict[str, Any]:
    if not trades:
        return {
            "n_trades": 0, "n_target": 0, "n_sl": 0, "n_eod": 0, "n_be": 0,
            "win_pct": 0.0, "pf": 0.0, "net_rs": 0.0, "net_pct": 0.0,
            "max_dd_rs": 0.0, "sharpe": 0.0, "sortino": 0.0, "score": 0.0,
        }

    df        = pd.DataFrame(trades)
    pnl_gross = pd.to_numeric(df.get("pnl_pct_gross", pd.Series([])), errors="coerce").fillna(0.0)
    pnl_rs    = pnl_gross * POSITION_SIZE_RS * LEVERAGE / 100.0

    wins, losses = pnl_rs[pnl_rs > 0], pnl_rs[pnl_rs < 0]
    gross_win    = float(wins.sum())
    gross_loss   = float(abs(losses.sum()))
    n            = len(df)

    pf      = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
    win_pct = float((pnl_rs > 0).sum()) / n * 100.0 if n > 0 else 0.0
    net_rs  = float(pnl_rs.sum())
    net_pct = float(pnl_gross.sum())

    equity  = pnl_rs.cumsum()
    max_dd  = float(abs((equity - equity.cummax()).min())) if len(equity) > 0 else 0.0

    if "trade_date" in df.columns:
        daily = pnl_rs.groupby(pd.to_datetime(df["trade_date"], errors="coerce")).sum()
    else:
        daily = pnl_rs

    std_d   = float(daily.std()) if len(daily) > 1 else 0.0
    sharpe  = (float(daily.mean()) / std_d * 252 ** 0.5) if std_d > 0 else 0.0
    neg     = daily[daily < 0]
    neg_std = float(neg.std()) if len(neg) > 1 else (std_d or 1.0)
    sortino = (float(daily.mean()) / neg_std * 252 ** 0.5) if neg_std > 0 else 0.0

    outcomes = df.get("outcome", pd.Series(["?"] * n))
    n_t = int((outcomes == "TARGET").sum())
    n_s = int((outcomes == "SL").sum())
    n_e = int((outcomes == "EOD").sum())
    n_b = int((outcomes == "BE").sum())

    score = (
        float(np.log1p(max(pf - 1.0, 0.0)) * (win_pct / 100.0) * max(sharpe, 0.0))
        if n >= MIN_TRADES and pf > 0 else 0.0
    )

    return {
        "n_trades": n, "n_target": n_t, "n_sl": n_s, "n_eod": n_e, "n_be": n_b,
        "win_pct":   round(win_pct, 2),
        "pf":        round(pf, 4),
        "net_rs":    round(net_rs, 0),
        "net_pct":   round(net_pct, 4),
        "max_dd_rs": round(max_dd, 0),
        "sharpe":    round(sharpe, 4),
        "sortino":   round(sortino, 4),
        "score":     round(score, 6),
    }


# ===========================================================================
# CONFIG FACTORY  — BE and trailing DISABLED
# ===========================================================================
def _make_long_config(p: Dict[str, Any]):
    from avwap_v11_refactored.avwap_common_v7_sweep_v15 import default_long_config as _dlc
    cfg = _dlc(reports_dir=str(ROOT / "outputs_v15_sltgt_eod_sweep"))

    # Fixed v15 profile
    cfg.require_entry_close_confirm   = True
    cfg.enable_liquidity_sweep_filter = False
    cfg.enable_avwap_no_trade_zone    = False
    cfg.adx_min                       = 22.0
    cfg.adx_slope_min                 = 0.50
    cfg.rsi_min_long                  = 50.0
    cfg.volume_min_ratio              = 0.95
    cfg.stochk_min                    = 15.0
    cfg.stochk_max                    = 95.0
    cfg.atr_pct_min                   = 0.0025
    cfg.min_bars_left_after_entry     = 0
    cfg.enable_topn_per_day           = False
    cfg.topn_per_day                  = 0
    cfg.max_trades_per_ticker_per_day = 4
    cfg.max_vix_for_entries           = 0.0
    cfg.enable_market_regime_filter   = False
    cfg.signal_avwap_dist_atr_min     = 0.5
    cfg.quality_score_min             = 5.0
    cfg.ema_gap_atr_min               = 0.0
    cfg.lag_bars_long_b_huge_c1_close_reclaim_break = 2
    cfg.lag_bars_long_a_mod_break_c1_high           = 1
    cfg.lag_bars_long_a_close_continuation_break    = 2
    cfg.lag_bars_long_a_pullback_c2_break_c2_high   = 1
    cfg.lag_bars_long_b_huge_pullback_hold_break     = 999
    cfg.entry_at_bar_close = False
    cfg.entry_at_next_open = False
    cfg.max_entry_slip_pct = 0.0
    cfg.enable_setup_a_pullback_c2_break           = True
    cfg.enable_setup_a_close_continuation_break    = True
    cfg.enable_setup_b_huge_c1_close_reclaim_break = True

    # ── Pure SL / Target — NO breakeven, NO trailing ──────────────────────
    cfg.enable_breakeven     = False
    cfg.enable_trailing_stop = False
    cfg.stop_pct             = float(p["stop_pct"])
    cfg.target_pct           = float(p["target_pct"])

    # Signal window
    we_h = int(p.get("win_end_h", 13))
    we_m = int(p.get("win_end_m", 0))
    cfg.use_time_windows = True
    cfg.signal_windows   = [(dtime(9, 15, 0), dtime(we_h, we_m, 0))]

    return cfg


# ===========================================================================
# PARALLEL SCAN
# ===========================================================================
def _run_config_parallel(
    p: Dict[str, Any],
    task_args: List[Tuple],
    max_workers: int,
    rs_threshold: float = 0.30,
) -> Dict[str, Any]:
    cfg       = _make_long_config(p)
    all_trades: List[dict] = []

    cfg_tasks = [(t[0], t[1], cfg) for t in task_args]

    if max_workers <= 1:
        for args in cfg_tasks:
            all_trades.extend(_worker_scan_one_ticker(args))
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_worker_scan_one_ticker, a): a[0] for a in cfg_tasks}
            for fut in as_completed(futures):
                try:
                    all_trades.extend(fut.result())
                except Exception:
                    pass

    # RS post-filter (fixed at 0.30)
    if rs_threshold > 0.0 and all_trades:
        filtered = []
        for t in all_trades:
            rs = t.get("nifty_rel_strength_pct", None)
            if rs is None or (isinstance(rs, float) and not np.isfinite(rs)):
                filtered.append(t)
            elif float(rs) >= rs_threshold:
                filtered.append(t)
        all_trades = filtered

    return _compute_metrics(all_trades)


# ===========================================================================
# CONFIG LIST
# ===========================================================================
def _build_configs() -> List[Dict[str, Any]]:
    configs: List[Dict[str, Any]] = []
    _id = [0]

    def C(name: str, **kw) -> Dict[str, Any]:
        _id[0] += 1
        base = dict(stop_pct=0.0077, target_pct=0.0110, win_end_h=13, win_end_m=0)
        base.update(kw)
        return {"id": _id[0], "name": name, **base}

    # ── BASELINE (SL=0.77%, TGT=1.10%, window 9:15-13:00) ────────────────
    configs.append(C("BASELINE"))

    # ── PHASE 1: SL × TGT grid ────────────────────────────────────────────
    # SL: 0.40% to 1.50%   TGT: 0.80% to 3.00%
    sl_levels  = [0.0040, 0.0050, 0.0060, 0.0070, 0.0077, 0.0085,
                  0.0100, 0.0120, 0.0150, 0.0180]
    tgt_levels = [0.0080, 0.0100, 0.0110, 0.0120, 0.0130, 0.0150,
                  0.0180, 0.0200, 0.0250, 0.0300]

    for sl in sl_levels:
        for tgt in tgt_levels:
            if tgt < sl * 1.20:          # minimum 1.2 R:R
                continue
            configs.append(C(
                f"SL{int(sl*10000):04d}_TGT{int(tgt*10000):04d}",
                stop_pct=sl, target_pct=tgt,
            ))

    # ── PHASE 2: Signal window end time  (baseline SL/TGT) ────────────────
    for weh, wem, lbl in [
        (10, 30, "WIN1030"), (11,  0, "WIN1100"), (11, 30, "WIN1130"),
        (12,  0, "WIN1200"), (12, 30, "WIN1230"), (13,  0, "WIN1300"),
        (13, 30, "WIN1330"), (14,  0, "WIN1400"), (14, 30, "WIN1430"),
        (15,  0, "WIN1500"),
    ]:
        configs.append(C(lbl, win_end_h=weh, win_end_m=wem))

    # ── PHASE 3: Best SL/TGT × best window combos ─────────────────────────
    # Hand-picked based on expected output from phases 1 & 2
    combos = [
        # (sl,    tgt,   weh, wem)
        (0.0050, 0.0100, 10, 30),
        (0.0050, 0.0100, 11,  0),
        (0.0050, 0.0110, 10, 30),
        (0.0050, 0.0110, 11,  0),
        (0.0050, 0.0120, 11,  0),
        (0.0060, 0.0100, 10, 30),
        (0.0060, 0.0100, 11,  0),
        (0.0060, 0.0120, 11,  0),
        (0.0070, 0.0110, 10, 30),
        (0.0070, 0.0110, 11,  0),
        (0.0070, 0.0120, 11,  0),
        (0.0070, 0.0130, 11,  0),
        (0.0077, 0.0110, 10, 30),
        (0.0077, 0.0110, 11,  0),
        (0.0077, 0.0120, 11,  0),
        (0.0077, 0.0130, 11,  0),
        (0.0085, 0.0130, 11,  0),
        (0.0085, 0.0150, 11,  0),
        (0.0050, 0.0150, 11,  0),
        (0.0060, 0.0150, 11,  0),
    ]
    for i, (sl, tgt, weh, wem) in enumerate(combos, 1):
        configs.append(C(
            f"COMBO_{i:02d}_SL{int(sl*10000)}_TGT{int(tgt*10000)}_W{weh:02d}{wem:02d}",
            stop_pct=sl, target_pct=tgt, win_end_h=weh, win_end_m=wem,
        ))

    return configs


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    from eqidv2_runtime_paths import DATA_15M_DIR
    data_dir = Path(DATA_15M_DIR)
    out_dir  = ROOT / "outputs_v15_sltgt_eod_sweep"
    out_dir.mkdir(exist_ok=True)
    csv_path = out_dir / "sweep_results.csv"
    log_path = out_dir / "sweep_log.txt"

    configs = _build_configs()
    n_total = len(configs)

    print(f"\n{'='*70}")
    print(f"V15 LONG SL/TGT/EOD SWEEP  —  {n_total} configs  |  workers={MAX_WORKERS}")
    print(f"Breakeven: DISABLED   Trailing stop: DISABLED")
    print(f"Data dir : {data_dir}")
    print(f"Output   : {out_dir}")
    print(f"{'='*70}\n")

    parquet_files = sorted(data_dir.glob(f"*{DATA_END_SUFFIX}"))
    task_args = [
        (fpath.name.replace(DATA_END_SUFFIX, ""), str(fpath))
        for fpath in parquet_files
    ]
    if not task_args:
        print(f"[ERROR] No parquet files found in {data_dir}")
        return

    print(f"[INFO] Tickers: {len(task_args)}")
    print(f"[INFO] Est. time: ~{n_total * 45 // 60} min (45s/config × {n_total})\n")

    results: List[Dict[str, Any]] = []
    t_total = time.perf_counter()

    with open(log_path, "w", encoding="utf-8", buffering=1) as log_fh:
        log_fh.write(
            f"V15 LONG SL/TGT/EOD SWEEP — {n_total} configs — "
            f"BE=OFF, Trail=OFF — started {datetime.now().isoformat()}\n\n"
        )

        for idx, p in enumerate(configs, 1):
            t_cfg = time.perf_counter()
            print(f"[{idx:3d}/{n_total}] {p['name']:<58}", end=" ", flush=True)

            try:
                metrics = _run_config_parallel(p, task_args, MAX_WORKERS)
                elapsed = time.perf_counter() - t_cfg

                n, pf, wp, sc, nr = (
                    metrics["n_trades"], metrics["pf"],
                    metrics["win_pct"],  metrics["score"], metrics["net_rs"],
                )
                nt, ns, ne = metrics["n_target"], metrics["n_sl"], metrics["n_eod"]

                print(
                    f"n={n:4d}  win={wp:5.1f}%  PF={pf:6.3f}  "
                    f"T={nt} SL={ns} EOD={ne}  "
                    f"net=Rs.{nr:+10.0f}  score={sc:.4f}  [{elapsed:.0f}s]"
                )
                log_fh.write(
                    f"[{idx:3d}] {p['name']:<58} "
                    f"n={n}  win={wp:.1f}%  PF={pf:.4f}  "
                    f"T={nt} SL={ns} EOD={ne}  net={nr:.0f}  "
                    f"score={sc:.6f}  [{elapsed:.0f}s]\n"
                )

                row = {
                    "id":         p["id"],
                    "name":       p["name"],
                    "stop_pct":   p["stop_pct"],
                    "target_pct": p["target_pct"],
                    "win_end":    f"{p['win_end_h']:02d}:{p['win_end_m']:02d}",
                    **metrics,
                }
                results.append(row)

            except Exception as exc:
                print(f"  ERROR: {exc}")
                log_fh.write(f"[{idx}] {p['name']} ERROR: {traceback.format_exc()}\n")

            # Incremental save every 5 configs
            if idx % 5 == 0 or idx == n_total:
                df_tmp = pd.DataFrame(results).sort_values("score", ascending=False)
                df_tmp.to_csv(csv_path, index=False)

    total_elapsed = time.perf_counter() - t_total
    print(f"\n{'='*70}")
    print(f"Done in {total_elapsed/60:.1f} min  |  Results → {csv_path}")

    # Final sorted output
    df_final = pd.DataFrame(results).sort_values("score", ascending=False)
    df_final.to_csv(csv_path, index=False)

    print(f"\n=== TOP 15 (SL/TGT/EOD only, no BE/trail) ===")
    cols = ["name", "stop_pct", "target_pct", "win_end",
            "n_trades", "n_target", "n_sl", "n_eod",
            "win_pct", "pf", "net_pct", "max_dd_rs", "sharpe", "score"]
    print(df_final[cols].head(15).to_string(index=False))


if __name__ == "__main__":
    main()
