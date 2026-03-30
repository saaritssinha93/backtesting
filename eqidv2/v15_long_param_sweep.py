# -*- coding: utf-8 -*-
"""
v15_long_param_sweep.py  — V15 LONG comprehensive parameter sweep
==================================================================
Sweeps 281 config combinations across:
  Phase 1  – SL / Target grid
  Phase 2  – Entry mode (trigger / bar-close / next-open)
  Phase 3  – ADX × RSI grid (8 × 8 = 64)
  Phase 4  – AVWAP dist min (12 levels)
  Phase 5  – EMA gap min   (10 levels)
  Phase 6  – Quality score min (12 levels)
  Phase 7  – Lag bars B_HUGE × A_MOD (3 × 2 = 6)
  Phase 8  – Signal window end time (12 variants)
  Phase 9  – Nifty RS threshold post-filter (11 levels)
  Phase 10 – BE trigger × trail stop (8 × 5 = 40)
  Phase 11 – Setup enable/disable (4 combos)
  Phase 12 – Hand-crafted best-guess combos (30)

Parallelism: ProcessPoolExecutor with MAX_WORKERS workers per config.
Each worker reads the parquet from disk (OS cache kicks in after run 1).

Output:
  outputs_v15_long_sweep/
    sweep_results.csv   — all configs, ranked by composite score
    sweep_log.txt       — per-config detail log

Usage:
    python -u v15_long_param_sweep.py
  or via bat:
    run_v15_long_sweep.bat
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
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Path setup — must come BEFORE local imports
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
sys.path.insert(0, str(ROOT))

# ===========================================================================
# CONSTANTS (edit here to tune the sweep)
# ===========================================================================
MAX_WORKERS      = 6       # parallel workers per config (raise if CPUs allow)
POSITION_SIZE_RS = 50_000  # Rs. margin per trade
LEVERAGE         = 5.0     # intraday leverage
MIN_TRADES       = 15      # minimum trades to compute meaningful score

DATA_END_SUFFIX  = "_stocks_indicators_15min.parquet"


# ===========================================================================
# TOP-LEVEL WORKER FUNCTION (must be importable for pickling)
# ===========================================================================
def _worker_scan_one_ticker(args: Tuple[str, str, Any]) -> List[dict]:
    """
    Scan one ticker. Runs in a subprocess.
    args = (ticker, parquet_path, long_cfg)
    Returns list of Trade dicts.
    """
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
# NIFTY RS POST-FILTER
# ===========================================================================
def _apply_rs_filter(trades: List[dict], rs_threshold: float) -> List[dict]:
    if rs_threshold <= 0.0 or not trades:
        return trades
    out = []
    for t in trades:
        rs = t.get("nifty_rel_strength_pct", None)
        if rs is None or (isinstance(rs, float) and not np.isfinite(rs)):
            out.append(t)   # no RS data → keep
        elif float(rs) >= rs_threshold:
            out.append(t)
    return out


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

    # Sharpe / Sortino on daily returns
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

    # Composite score: needs minimum trades; rewards high PF, win%, Sharpe
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
# CONFIG FACTORY
# ===========================================================================
def _make_long_config(p: Dict[str, Any]):
    """Build a StrategyConfig from a param dict."""
    from avwap_v11_refactored.avwap_common_v7_sweep_v15 import default_long_config as _dlc
    cfg = _dlc(reports_dir=str(ROOT / "outputs_v15_long_sweep"))

    # Fixed V15 profile
    cfg.require_entry_close_confirm   = True
    cfg.enable_liquidity_sweep_filter = False
    cfg.enable_avwap_no_trade_zone    = False
    cfg.adx_slope_min                 = 0.50
    cfg.volume_min_ratio              = 0.95
    cfg.stochk_min                    = 15.0
    cfg.stochk_max                    = 95.0
    cfg.atr_pct_min                   = 0.0025
    cfg.min_bars_left_after_entry     = 0
    cfg.enable_topn_per_day           = False
    cfg.topn_per_day                  = 0
    cfg.max_trades_per_ticker_per_day = 4
    cfg.max_vix_for_entries           = 0.0
    cfg.enable_market_regime_filter   = False   # applied post-scan via rs_threshold

    # Swept parameters
    cfg.stop_pct                  = float(p["stop_pct"])
    cfg.target_pct                = float(p["target_pct"])
    cfg.be_trigger_pct            = float(p["be_trigger_pct"])
    cfg.trail_pct                 = float(p["trail_pct"])
    cfg.adx_min                   = float(p["adx_min"])
    cfg.rsi_min_long              = float(p["rsi_min_long"])
    cfg.signal_avwap_dist_atr_min = float(p["avwap_dist_min"])
    cfg.ema_gap_atr_min           = float(p["ema_gap_min"])
    cfg.quality_score_min         = float(p["qs_min"])

    cfg.lag_bars_long_b_huge_c1_close_reclaim_break = int(p["lag_b_huge"])
    cfg.lag_bars_long_a_mod_break_c1_high           = int(p["lag_a_mod"])
    cfg.lag_bars_long_a_close_continuation_break    = 2
    cfg.lag_bars_long_a_pullback_c2_break_c2_high   = 1
    cfg.lag_bars_long_b_huge_pullback_hold_break     = 999

    cfg.entry_at_bar_close = bool(p.get("entry_at_bar_close", False))
    cfg.entry_at_next_open = bool(p.get("entry_at_next_open", False))
    cfg.max_entry_slip_pct = 0.0

    we_h = int(p.get("win_end_h", 13))
    we_m = int(p.get("win_end_m", 0))
    cfg.use_time_windows = True
    cfg.signal_windows   = [(dtime(9, 15, 0), dtime(we_h, we_m, 0))]

    cfg.enable_setup_a_pullback_c2_break           = True
    cfg.enable_setup_a_close_continuation_break    = bool(p.get("enable_a_cont", True))
    cfg.enable_setup_b_huge_c1_close_reclaim_break = bool(p.get("enable_b_huge", True))

    return cfg


# ===========================================================================
# PARALLEL SCAN (one config, all tickers)
# ===========================================================================
def _run_config_parallel(
    p: Dict[str, Any],
    task_args: List[Tuple],
    max_workers: int,
) -> Dict[str, Any]:
    """Scan all tickers in parallel for one config. Returns metrics dict."""
    cfg          = _make_long_config(p)
    all_trades: List[dict] = []

    # Inject cfg into every task arg
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

    # Post-scan RS threshold filter
    rs_thr = float(p.get("rs_threshold", 0.0))
    if rs_thr > 0.0:
        all_trades = _apply_rs_filter(all_trades, rs_thr)

    return _compute_metrics(all_trades)


# ===========================================================================
# CONFIG LIST BUILDER
# ===========================================================================
def _build_configs() -> List[Dict[str, Any]]:
    configs: List[Dict[str, Any]] = []
    _id = [0]

    def C(name: str, **kw) -> Dict[str, Any]:
        _id[0] += 1
        base = dict(
            stop_pct=0.0077, target_pct=0.0110,
            be_trigger_pct=0.0055, trail_pct=0.0028,
            adx_min=22.0, rsi_min_long=50.0,
            avwap_dist_min=0.5, ema_gap_min=0.0, qs_min=5.0,
            lag_b_huge=2, lag_a_mod=1,
            win_end_h=13, win_end_m=0,
            entry_at_bar_close=False, entry_at_next_open=False,
            rs_threshold=0.30,
            enable_a_cont=True, enable_b_huge=True,
        )
        base.update(kw)
        return {"id": _id[0], "name": name, **base}

    # ── BASELINE ──────────────────────────────────────────────────────────
    configs.append(C("BASELINE"))

    # ── PHASE 1: SL / TGT grid ────────────────────────────────────────────
    for sl in [0.0050, 0.0055, 0.006, 0.0065, 0.007, 0.0077,
               0.008,  0.0084, 0.009, 0.010,  0.011]:
        for tgt in [0.009, 0.010, 0.011, 0.012, 0.013, 0.015, 0.018]:
            if tgt < sl * 1.12:
                continue
            configs.append(C(
                f"SL{int(sl*10000)}_TGT{int(tgt*10000)}",
                stop_pct=sl, target_pct=tgt,
                be_trigger_pct=round(sl * 0.68, 4),
            ))

    # ── PHASE 2: Entry mode × 4 SL/TGT pairs ─────────────────────────────
    for sl, tgt in [(0.007, 0.011), (0.0077, 0.011), (0.008, 0.013), (0.009, 0.013)]:
        be = round(sl * 0.68, 4)
        for mode_name, bc, no in [
            ("trigger",   False, False),
            ("bar_close", True,  False),
            ("next_open", False, True),
        ]:
            configs.append(C(
                f"SL{int(sl*10000)}_TGT{int(tgt*10000)}_{mode_name}",
                stop_pct=sl, target_pct=tgt, be_trigger_pct=be,
                entry_at_bar_close=bc, entry_at_next_open=no,
            ))

    # ── PHASE 3: ADX × RSI grid (8 × 8 = 64) ─────────────────────────────
    for adx in [14.0, 17.0, 20.0, 22.0, 24.0, 25.0, 27.0, 30.0]:
        for rsi in [42.0, 45.0, 48.0, 50.0, 52.0, 55.0, 57.0, 60.0]:
            configs.append(C(f"ADX{int(adx)}_RSI{int(rsi)}", adx_min=adx, rsi_min_long=rsi))

    # ── PHASE 4: AVWAP dist min ────────────────────────────────────────────
    for v in [0.0, 0.2, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0, 1.2, 1.5, 1.8, 2.0]:
        configs.append(C(f"AVWAP_MIN_{int(v*10):02d}", avwap_dist_min=v))

    # ── PHASE 5: EMA gap min ──────────────────────────────────────────────
    for v in [0.0, 0.3, 0.5, 0.6, 0.7, 0.8, 1.0, 1.2, 1.5, 1.8]:
        configs.append(C(f"EMA_MIN_{int(v*10):02d}", ema_gap_min=v))

    # ── PHASE 6: Quality score min ────────────────────────────────────────
    for v in [0.0, 2.0, 3.0, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 9.0]:
        configs.append(C(f"QS_MIN_{int(v*10):03d}", qs_min=v))

    # ── PHASE 7: Lag bars (3 × 2 = 6) ────────────────────────────────────
    for lb in [1, 2, 3]:
        for la in [1, 2]:
            configs.append(C(f"LAG_B{lb}_A{la}", lag_b_huge=lb, lag_a_mod=la))

    # ── PHASE 8: Signal window end time ──────────────────────────────────
    for weh, wem in [
        (9, 45), (10, 0), (10, 30), (11, 0), (11, 30),
        (12, 0), (12, 30), (13, 0), (13, 30), (14, 0), (14, 30), (15, 0)
    ]:
        configs.append(C(f"WIN_END_{weh:02d}{wem:02d}", win_end_h=weh, win_end_m=wem))

    # ── PHASE 9: Nifty RS threshold ───────────────────────────────────────
    for rs in [0.0, 0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50]:
        configs.append(C(f"RS_THR_{int(rs*100):03d}", rs_threshold=rs))

    # ── PHASE 10: BE trigger × trail (8 × 5 = 40) ────────────────────────
    for be in [0.003, 0.004, 0.0045, 0.005, 0.0055, 0.006, 0.007, 0.008]:
        for trail in [0.002, 0.0025, 0.003, 0.0035, 0.004]:
            configs.append(C(
                f"BE{int(be*10000)}_TRAIL{int(trail*10000)}",
                be_trigger_pct=be, trail_pct=trail,
            ))

    # ── PHASE 11: Setup toggles ───────────────────────────────────────────
    for a_cont, b_huge, lbl in [
        (True,  True,  "SETUP_BOTH"),
        (False, True,  "SETUP_BHUGE_ONLY"),
        (True,  False, "SETUP_AMOD_ONLY"),
        (False, False, "SETUP_NONE"),
    ]:
        configs.append(C(lbl, enable_a_cont=a_cont, enable_b_huge=b_huge))

    # ── PHASE 12: Combined high-conviction combos (30) ────────────────────
    combos = [
        # (sl,    tgt,   adx,  rsi,  avwap, ema,  qs,  lb, la, rs,   weh, wem)
        (0.006,  0.010, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 12, 0),
        (0.006,  0.010, 22.0, 50.0, 0.5, 0.0, 5.0,  1,  1, 0.30, 12, 0),
        (0.006,  0.009, 20.0, 48.0, 0.3, 0.0, 4.0,  1,  1, 0.20, 11, 0),
        (0.007,  0.011, 22.0, 50.0, 0.5, 0.0, 5.0,  1,  1, 0.30, 12, 0),
        (0.007,  0.011, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 12, 0),
        (0.007,  0.011, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.20, 12, 0),
        (0.007,  0.012, 22.0, 50.0, 0.5, 0.0, 5.0,  1,  1, 0.30, 12, 0),
        (0.007,  0.013, 22.0, 50.0, 0.5, 0.0, 5.0,  1,  1, 0.30, 12, 0),
        (0.007,  0.011, 25.0, 52.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.007,  0.011, 25.0, 52.0, 0.8, 0.0, 6.0,  2,  1, 0.30, 13, 0),
        (0.007,  0.011, 25.0, 52.0, 1.0, 0.0, 6.0,  2,  1, 0.30, 13, 0),
        (0.007,  0.011, 22.0, 50.0, 0.5, 0.5, 5.0,  2,  1, 0.30, 13, 0),
        (0.007,  0.011, 22.0, 50.0, 0.5, 1.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.007,  0.011, 22.0, 50.0, 0.5, 0.0, 6.0,  2,  1, 0.40, 12, 0),
        (0.007,  0.011, 22.0, 50.0, 0.5, 0.0, 6.0,  2,  1, 0.50, 12, 0),
        (0.0077, 0.011, 22.0, 50.0, 0.8, 0.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.0077, 0.011, 22.0, 50.0, 1.0, 0.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.0077, 0.011, 25.0, 52.0, 0.5, 0.5, 5.0,  2,  1, 0.30, 13, 0),
        (0.0077, 0.013, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.008,  0.012, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 12, 0),
        (0.008,  0.012, 25.0, 52.0, 0.8, 0.5, 6.0,  2,  1, 0.30, 13, 0),
        (0.008,  0.013, 25.0, 52.0, 0.8, 0.0, 6.0,  2,  1, 0.30, 12, 0),
        (0.009,  0.013, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.009,  0.013, 25.0, 52.0, 0.8, 0.5, 6.0,  2,  1, 0.35, 13, 0),
        (0.009,  0.015, 25.0, 52.0, 0.5, 0.0, 6.0,  2,  1, 0.30, 13, 0),
        (0.010,  0.015, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.010,  0.015, 25.0, 52.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 13, 0),
        (0.0055, 0.010, 22.0, 50.0, 0.5, 0.0, 5.0,  2,  1, 0.30, 12, 0),
        (0.0055, 0.009, 20.0, 48.0, 0.5, 0.0, 4.0,  1,  1, 0.20, 12, 0),
        (0.006,  0.010, 22.0, 50.0, 0.5, 0.0, 5.0,  1,  1, 0.20, 11, 30),
    ]
    for i, (sl, tgt, adx, rsi, avwap, ema, qs, lb, la, rs, weh, wem) in enumerate(combos):
        configs.append(C(
            f"COMBO_{i+1:02d}",
            stop_pct=sl, target_pct=tgt, be_trigger_pct=round(sl * 0.68, 4),
            adx_min=adx, rsi_min_long=rsi,
            avwap_dist_min=avwap, ema_gap_min=ema, qs_min=qs,
            lag_b_huge=lb, lag_a_mod=la,
            rs_threshold=rs, win_end_h=weh, win_end_m=wem,
        ))

    return configs


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    from eqidv2_runtime_paths import DATA_15M_DIR
    data_dir = Path(DATA_15M_DIR)
    out_dir  = ROOT / "outputs_v15_long_sweep"
    out_dir.mkdir(exist_ok=True)
    csv_path = out_dir / "sweep_results.csv"
    log_path = out_dir / "sweep_log.txt"

    configs   = _build_configs()
    n_total   = len(configs)

    print(f"\n{'='*70}")
    print(f"V15 LONG PARAMETER SWEEP  —  {n_total} configs  |  workers={MAX_WORKERS}")
    print(f"Data dir : {data_dir}")
    print(f"Output   : {out_dir}")
    print(f"{'='*70}\n")

    # Build task arg list (ticker, path) — shared across all configs
    parquet_files = sorted(data_dir.glob(f"*{DATA_END_SUFFIX}"))
    task_args = [
        (fpath.name.replace(DATA_END_SUFFIX, ""), str(fpath))
        for fpath in parquet_files
    ]
    print(f"[INFO] Tickers found: {len(task_args)}")
    print(f"[INFO] Estimated time: ~{n_total * 45 // 60} min (45s/config × {n_total})\n")

    results: List[Dict[str, Any]] = []
    t_total = time.perf_counter()

    with open(log_path, "w", encoding="utf-8", buffering=1) as log_fh:
        log_fh.write(
            f"V15 LONG SWEEP — {n_total} configs — workers={MAX_WORKERS} "
            f"— started {datetime.now().isoformat()}\n\n"
        )

        for idx, p in enumerate(configs, 1):
            t_cfg = time.perf_counter()
            print(f"[{idx:3d}/{n_total}] {p['name']:<55}", end=" ", flush=True)

            try:
                metrics = _run_config_parallel(p, task_args, MAX_WORKERS)

                elapsed = time.perf_counter() - t_cfg
                n  = metrics["n_trades"]
                pf = metrics["pf"]
                wp = metrics["win_pct"]
                sc = metrics["score"]
                nr = metrics["net_rs"]

                print(
                    f"n={n:4d}  win={wp:5.1f}%  PF={pf:5.3f}  "
                    f"net=Rs.{nr:+10.0f}  score={sc:.4f}  [{elapsed:.0f}s]"
                )
                log_fh.write(
                    f"[{idx:3d}] {p['name']:<55} "
                    f"n={n:4d}  win={wp:.1f}%  PF={pf:.4f}  "
                    f"net={nr:.0f}  score={sc:.6f}  [{elapsed:.0f}s]\n"
                )

                row = {
                    "id": p["id"], "name": p["name"],
                    "stop_pct": p["stop_pct"],       "target_pct": p["target_pct"],
                    "be_trigger": p["be_trigger_pct"], "trail_pct": p["trail_pct"],
                    "adx_min": p["adx_min"],           "rsi_min": p["rsi_min_long"],
                    "avwap_min": p["avwap_dist_min"],  "ema_min": p["ema_gap_min"],
                    "qs_min": p["qs_min"],             "lag_b_huge": p["lag_b_huge"],
                    "lag_a_mod": p["lag_a_mod"],
                    "win_end": f"{p['win_end_h']:02d}:{p['win_end_m']:02d}",
                    "rs_thr": p.get("rs_threshold", 0.0),
                    "entry_mode": (
                        "bar_close" if p.get("entry_at_bar_close") else
                        "next_open" if p.get("entry_at_next_open") else "trigger"
                    ),
                    "a_cont": p.get("enable_a_cont", True),
                    "b_huge": p.get("enable_b_huge", True),
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

    # ── Final ranking ──────────────────────────────────────────────────────
    df_final = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
    df_final.to_csv(csv_path, index=False)

    total_min = (time.perf_counter() - t_total) / 60.0
    print(f"\n{'='*70}")
    print(f"SWEEP COMPLETE — {n_total} configs in {total_min:.1f} min")
    print(f"Results → {csv_path}")
    print(f"{'='*70}\n")

    display_cols = [
        "name", "n_trades", "win_pct", "pf", "net_rs", "max_dd_rs",
        "sharpe", "score", "stop_pct", "target_pct", "adx_min", "rsi_min",
        "avwap_min", "ema_min", "qs_min", "lag_b_huge", "win_end",
        "entry_mode", "rs_thr",
    ]
    dc = [c for c in display_cols if c in df_final.columns]

    print("── TOP 20 by composite score ──")
    print(df_final.head(20)[dc].to_string(index=False))

    min30 = df_final[df_final["n_trades"] >= 30]
    print("\n── TOP 10 by Profit Factor (min 30 trades) ──")
    print(min30.sort_values("pf", ascending=False).head(10)[dc].to_string(index=False))

    print("\n── TOP 10 by Net Rs. PnL (min 30 trades) ──")
    print(min30.sort_values("net_rs", ascending=False).head(10)[dc].to_string(index=False))

    print("\n── TOP 5 by Sharpe (min 30 trades) ──")
    print(min30.sort_values("sharpe", ascending=False).head(5)[dc].to_string(index=False))


if __name__ == "__main__":
    main()
