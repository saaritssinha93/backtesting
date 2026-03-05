# -*- coding: utf-8 -*-
"""
Scenario sweep for PDF-transformed eqidv5 (3TF: Daily + 15m + 5m confirmation).
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from eqidv5_combined_runner import _add_notional_pnl, _run_side_parallel
from eqidv5_strategy_common import compute_backtest_metrics, default_long_config, default_short_config


def _derive_5m_dir(dir15m: str) -> str:
    p15 = Path(dir15m)
    cands = []
    if "15min" in p15.name:
        cands.append(p15.with_name(p15.name.replace("15min", "5min")))
    cands.append(p15.with_name("stocks_indicators_5min_eq"))
    cands.append(Path("eqidv2/stocks_indicators_5min_eq"))
    cands.append(Path("stocks_indicators_5min_eq"))
    for c in cands:
        if c.exists():
            return str(c)
    return str(cands[0])


def _scenarios() -> List[Tuple[str, Dict]]:
    return [
        ("pdf_default", {}),
        ("pdf_loose", {
            "min_quality_score": 7, "sweep_vol_ratio": 1.8, "adx_min": 22.0, "min_rr": 1.6,
            "require_vp_confluence": False, "require_rsi_filter": True, "require_ema_alignment": False,
            "confirm_window_min": 45, "delta_spike_mult": 1.1, "confirm_min_score": 1,
            "scalp_time_stop_minutes": 120,
        }),
        ("pdf_medium", {
            "min_quality_score": 7, "sweep_vol_ratio": 2.0, "adx_min": 23.0, "min_rr": 1.8,
            "require_vp_confluence": True, "require_rsi_filter": True, "require_ema_alignment": True,
            "confirm_window_min": 30, "delta_spike_mult": 1.3, "confirm_min_score": 1,
            "scalp_time_stop_minutes": 90,
        }),
        ("pdf_strict", {
            "min_quality_score": 8, "sweep_vol_ratio": 2.2, "adx_min": 25.0, "min_rr": 1.8,
            "require_vp_confluence": True, "require_rsi_filter": True, "require_ema_alignment": True,
            "confirm_window_min": 30, "delta_spike_mult": 1.3, "confirm_min_score": 2,
            "scalp_time_stop_minutes": 90,
        }),
        ("pdf_time_60", {"scalp_time_stop_minutes": 60}),
        ("pdf_time_120", {"scalp_time_stop_minutes": 120}),
        ("pdf_rr_16", {"min_rr": 1.6}),
        ("pdf_rr_22", {"min_rr": 2.2}),
        ("pdf_be_fast", {"be_trigger_pct": 0.0035, "be_pad_pct": 0.0010, "trail_pct": 0.0030}),
        ("pdf_be_slow", {"be_trigger_pct": 0.0055, "be_pad_pct": 0.0025, "trail_pct": 0.0050}),
        ("pdf_no_trail", {"enable_trailing_stop": False, "be_trigger_pct": 0.0040, "be_pad_pct": 0.0015}),
        ("hybrid_5m_optional", {"use_5m_confirmation": True, "require_5m_confirmation": False, "confirm_min_score": 1}),
        ("hybrid_5m_off", {"use_5m_confirmation": False, "require_5m_confirmation": False}),
        ("hybrid_5m_off_loose", {
            "use_5m_confirmation": False, "require_5m_confirmation": False,
            "min_quality_score": 7, "sweep_vol_ratio": 1.9, "adx_min": 22.0, "min_rr": 1.7,
            "require_vp_confluence": False, "require_rsi_filter": True, "require_ema_alignment": False,
            "be_trigger_pct": 0.0040, "be_pad_pct": 0.0015, "trail_pct": 0.0035,
            "scalp_time_stop_minutes": 120,
        }),
        ("hybrid_5m_off_strict_rr", {
            "use_5m_confirmation": False, "require_5m_confirmation": False,
            "min_rr": 2.2, "min_quality_score": 8, "sweep_vol_ratio": 2.2, "adx_min": 25.0,
            "require_vp_confluence": True, "require_rsi_filter": True, "require_ema_alignment": True,
        }),
    ]


def _objective(sum_pnl_pct: float, pf: float, max_dd_pct: float, trades_per_day: float) -> float:
    # Penalize DD and overtrading while rewarding PF and net returns.
    return sum_pnl_pct + 140.0 * (pf - 1.0) - 0.35 * max_dd_pct - 2.0 * max(0.0, trades_per_day - 10.0)


def run_sweep(dir15m: str, workers: int, max_tickers: int) -> pd.DataFrame:
    rows = []
    dir5m = _derive_5m_dir(dir15m)

    for name, overrides in _scenarios():
        t0 = time.time()
        long_cfg = default_long_config(dir_15m=dir15m, dir_5m=dir5m, **overrides)
        short_cfg = default_short_config(dir_15m=dir15m, dir_5m=dir5m, **overrides)

        max_t = max_tickers if max_tickers > 0 else None
        dl = _run_side_parallel("LONG", long_cfg, max_workers=workers, max_tickers=max_t)
        ds = _run_side_parallel("SHORT", short_cfg, max_workers=workers, max_tickers=max_t)
        parts = [p for p in [dl, ds] if not p.empty]
        dc = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if not dc.empty:
            dc = _add_notional_pnl(dc)

        m = compute_backtest_metrics(dc)
        gross_sum = float(pd.to_numeric(dc.get("pnl_pct_gross", 0), errors="coerce").fillna(0).sum()) if not dc.empty else 0.0
        pnl_rs = float(pd.to_numeric(dc.get("pnl_rs", 0), errors="coerce").fillna(0).sum()) if not dc.empty else 0.0
        tpd = m.total_trades / max(m.unique_days, 1)
        score = _objective(m.sum_pnl_pct, m.profit_factor, m.max_drawdown_pct, tpd)

        rows.append({
            "scenario": name,
            "trades": m.total_trades,
            "days": m.unique_days,
            "trades_per_day": round(tpd, 2),
            "sum_pnl_pct": round(m.sum_pnl_pct, 2),
            "sum_pnl_pct_gross": round(gross_sum, 2),
            "profit_factor": round(m.profit_factor, 3),
            "max_dd_pct": round(m.max_drawdown_pct, 2),
            "pnl_rs": round(pnl_rs, 0),
            "target_hits": m.target_count,
            "sl_hits": m.sl_count,
            "be_hits": m.be_count,
            "eod_hits": m.eod_count,
            "elapsed_s": round(time.time() - t0, 1),
            "objective": round(score, 2),
        })

    out = pd.DataFrame(rows).sort_values(["objective", "sum_pnl_pct", "profit_factor"], ascending=False)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="PDF strategy sweep for eqidv5")
    ap.add_argument("--dir15m", default="eqidv2/stocks_indicators_15min_eq")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--max_tickers", type=int, default=300)
    ap.add_argument("--out", default="eqidv2/outputs_eqidv5/pdf_sweep_subset.csv")
    args = ap.parse_args()

    out = run_sweep(args.dir15m, args.workers, args.max_tickers)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(out.to_string(index=False))
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()

