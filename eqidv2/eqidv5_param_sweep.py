# -*- coding: utf-8 -*-
"""
eqidv5_param_sweep.py
=====================

Batch-evaluate parameter scenarios for eqidv5 long+short strategy and rank
them by a risk-adjusted objective.
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
        ("baseline", {}),
        (
            "be_fast_tight",
            {
                "be_trigger_pct": 0.0035,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0035,
                "min_rr": 2.10,
            },
        ),
        (
            "be_fast_no_trail",
            {
                "be_trigger_pct": 0.0035,
                "be_pad_pct": 0.0015,
                "enable_trailing_stop": False,
                "min_rr": 2.10,
            },
        ),
        (
            "be_late_wide_trail",
            {
                "be_trigger_pct": 0.0065,
                "be_pad_pct": 0.0010,
                "trail_pct": 0.0060,
                "min_rr": 2.10,
            },
        ),
        (
            "risk_tighter_sl",
            {
                "sl_buffer_pct": 0.0005,
                "be_trigger_pct": 0.0040,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0035,
                "min_rr": 2.00,
            },
        ),
        (
            "quality_strict",
            {
                "min_quality_score": 8,
                "sweep_vol_ratio": 2.20,
                "adx_min": 25.0,
                "require_rsi_filter": True,
                "require_ema_alignment": True,
                "be_trigger_pct": 0.0040,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0035,
                "min_rr": 2.00,
            },
        ),
        (
            "quality_mid",
            {
                "min_quality_score": 7,
                "sweep_vol_ratio": 2.00,
                "adx_min": 23.0,
                "require_rsi_filter": True,
                "require_ema_alignment": False,
                "be_trigger_pct": 0.0040,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0035,
                "min_rr": 2.00,
            },
        ),
        (
            "target_easier",
            {
                "t1_pct": 0.55,
                "t2_pct": 0.30,
                "t3_pct": 0.15,
                "min_rr": 1.70,
                "be_trigger_pct": 0.0040,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0040,
            },
        ),
        (
            "quality_strict_vp",
            {
                "min_quality_score": 8,
                "sweep_vol_ratio": 2.20,
                "adx_min": 25.0,
                "require_vp_confluence": True,
                "require_rsi_filter": True,
                "require_ema_alignment": True,
                "be_trigger_pct": 0.0040,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0035,
                "min_rr": 2.00,
            },
        ),
        (
            "quality_ultra",
            {
                "min_quality_score": 9,
                "sweep_vol_ratio": 2.40,
                "adx_min": 27.0,
                "require_vp_confluence": True,
                "require_rsi_filter": True,
                "require_ema_alignment": True,
                "be_trigger_pct": 0.0040,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0035,
                "min_rr": 2.00,
            },
        ),
        (
            "quality_strict_rr24",
            {
                "min_quality_score": 8,
                "sweep_vol_ratio": 2.20,
                "adx_min": 25.0,
                "require_vp_confluence": True,
                "require_rsi_filter": True,
                "require_ema_alignment": True,
                "be_trigger_pct": 0.0040,
                "be_pad_pct": 0.0015,
                "trail_pct": 0.0035,
                "min_rr": 2.40,
            },
        ),
        (
            "quality_strict_be20",
            {
                "min_quality_score": 8,
                "sweep_vol_ratio": 2.20,
                "adx_min": 25.0,
                "require_vp_confluence": True,
                "require_rsi_filter": True,
                "require_ema_alignment": True,
                "be_trigger_pct": 0.0035,
                "be_pad_pct": 0.0020,
                "trail_pct": 0.0030,
                "min_rr": 2.00,
            },
        ),
    ]


def _objective(sum_pnl_pct: float, profit_factor: float, max_dd_pct: float, trades_per_day: float) -> float:
    # Bias toward better net pnl/pf and lower DD; soft penalty for overtrading.
    return (
        sum_pnl_pct
        + 120.0 * (profit_factor - 1.0)
        - 0.35 * max_dd_pct
        - 3.0 * max(0.0, trades_per_day - 12.0)
    )


def run_sweep(dir15m: str, workers: int, max_tickers: int, out_csv: str) -> pd.DataFrame:
    rows = []
    dir5m = _derive_5m_dir(dir15m)
    for name, overrides in _scenarios():
        t0 = time.time()
        long_cfg = default_long_config(dir_15m=dir15m, dir_5m=dir5m, **overrides)
        short_cfg = default_short_config(dir_15m=dir15m, dir_5m=dir5m, **overrides)

        df_long = _run_side_parallel(
            side="LONG",
            cfg=long_cfg,
            max_workers=workers,
            max_tickers=max_tickers if max_tickers > 0 else None,
        )
        df_short = _run_side_parallel(
            side="SHORT",
            cfg=short_cfg,
            max_workers=workers,
            max_tickers=max_tickers if max_tickers > 0 else None,
        )

        parts = [p for p in [df_long, df_short] if not p.empty]
        df_combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if not df_combined.empty:
            df_combined = _add_notional_pnl(df_combined)

        m = compute_backtest_metrics(df_combined)
        gross_sum = (
            float(pd.to_numeric(df_combined.get("pnl_pct_gross", 0), errors="coerce").fillna(0).sum())
            if not df_combined.empty
            else 0.0
        )
        days = max(m.unique_days, 1)
        tpd = m.total_trades / days
        score = _objective(m.sum_pnl_pct, m.profit_factor, m.max_drawdown_pct, tpd)

        rows.append(
            {
                "scenario": name,
                "trades": m.total_trades,
                "days": m.unique_days,
                "trades_per_day": round(tpd, 2),
                "sum_pnl_pct": round(m.sum_pnl_pct, 2),
                "sum_pnl_pct_gross": round(gross_sum, 2),
                "profit_factor": round(m.profit_factor, 3),
                "max_dd_pct": round(m.max_drawdown_pct, 2),
                "target_hits": m.target_count,
                "sl_hits": m.sl_count,
                "be_hits": m.be_count,
                "eod_hits": m.eod_count,
                "elapsed_s": round(time.time() - t0, 1),
                "objective": round(score, 2),
            }
        )

    out = pd.DataFrame(rows).sort_values(
        ["objective", "sum_pnl_pct", "profit_factor"],
        ascending=False,
    )
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Parameter sweep for eqidv5 strategy")
    parser.add_argument("--dir15m", default="eqidv2/stocks_indicators_15min_eq")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--max_tickers", type=int, default=400)
    parser.add_argument("--out", default="eqidv2/outputs_eqidv5/tuning_subset_400.csv")
    args = parser.parse_args()

    out = run_sweep(
        dir15m=args.dir15m,
        workers=args.workers,
        max_tickers=args.max_tickers,
        out_csv=args.out,
    )
    print("\n=== SCENARIO RANKING ===")
    print(out.to_string(index=False))
    print(f"\nSaved: {args.out}")


if __name__ == "__main__":
    main()
