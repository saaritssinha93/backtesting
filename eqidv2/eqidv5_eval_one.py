# -*- coding: utf-8 -*-
"""
Evaluate one eqidv5 configuration on LONG+SHORT and print summary metrics.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir15m", default="eqidv2/stocks_indicators_15min_eq")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--max_tickers", type=int, default=0)
    ap.add_argument(
        "--preset",
        choices=[
            "baseline",
            "quality_strict",
            "quality_strict_be25",
            "quality_strict_be30",
            "quality_strict_notrail",
            "quality_strict_rr24",
            "quality_strict_rr18",
            "quality_mid_rr18",
        ],
        default=None,
        help="Use a named override preset instead of JSON",
    )
    ap.add_argument(
        "--overrides_json",
        default="{}",
        help="JSON dict of config overrides applied to both long and short configs",
    )
    args = ap.parse_args()

    strict_base = {
        "min_quality_score": 8,
        "sweep_vol_ratio": 2.2,
        "adx_min": 25.0,
        "require_vp_confluence": True,
        "require_rsi_filter": True,
        "require_ema_alignment": True,
        "min_rr": 2.0,
    }

    if args.preset == "quality_strict":
        overrides = {
            **strict_base,
            "be_trigger_pct": 0.004,
            "be_pad_pct": 0.0015,
            "trail_pct": 0.0035,
        }
    elif args.preset == "quality_strict_be25":
        overrides = {
            **strict_base,
            "be_trigger_pct": 0.0045,
            "be_pad_pct": 0.0025,
            "trail_pct": 0.0040,
        }
    elif args.preset == "quality_strict_be30":
        overrides = {
            **strict_base,
            "be_trigger_pct": 0.0050,
            "be_pad_pct": 0.0030,
            "trail_pct": 0.0045,
        }
    elif args.preset == "quality_strict_notrail":
        overrides = {
            **strict_base,
            "be_trigger_pct": 0.0045,
            "be_pad_pct": 0.0025,
            "enable_trailing_stop": False,
        }
    elif args.preset == "quality_strict_rr24":
        overrides = {
            **strict_base,
            "min_rr": 2.4,
            "be_trigger_pct": 0.004,
            "be_pad_pct": 0.0015,
            "trail_pct": 0.0035,
        }
    elif args.preset == "quality_strict_rr18":
        overrides = {
            **strict_base,
            "min_rr": 1.8,
            "be_trigger_pct": 0.004,
            "be_pad_pct": 0.0015,
            "trail_pct": 0.0035,
        }
    elif args.preset == "quality_mid_rr18":
        overrides = {
            "min_quality_score": 7,
            "sweep_vol_ratio": 2.0,
            "adx_min": 23.0,
            "require_vp_confluence": False,
            "require_rsi_filter": True,
            "require_ema_alignment": False,
            "min_rr": 1.8,
            "be_trigger_pct": 0.004,
            "be_pad_pct": 0.0015,
            "trail_pct": 0.0035,
        }
    else:
        overrides: Dict[str, Any] = json.loads(args.overrides_json)
    dir5m = _derive_5m_dir(args.dir15m)
    long_cfg = default_long_config(dir_15m=args.dir15m, dir_5m=dir5m, **overrides)
    short_cfg = default_short_config(dir_15m=args.dir15m, dir_5m=dir5m, **overrides)

    max_t = args.max_tickers if args.max_tickers > 0 else None
    dl = _run_side_parallel("LONG", long_cfg, max_workers=args.workers, max_tickers=max_t)
    ds = _run_side_parallel("SHORT", short_cfg, max_workers=args.workers, max_tickers=max_t)
    parts = [p for p in [dl, ds] if not p.empty]
    dc = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if not dc.empty:
        dc = _add_notional_pnl(dc)

    m = compute_backtest_metrics(dc)
    pnl_rs = float(dc["pnl_rs"].sum()) if (not dc.empty and "pnl_rs" in dc.columns) else 0.0
    gross_sum = float(pd.to_numeric(dc.get("pnl_pct_gross", 0), errors="coerce").fillna(0).sum()) if not dc.empty else 0.0
    tpd = m.total_trades / max(m.unique_days, 1)

    print("\n=== EVAL SUMMARY ===")
    print("overrides:", overrides)
    print(
        f"trades={m.total_trades} days={m.unique_days} trades/day={tpd:.2f} "
        f"sum_pnl_pct={m.sum_pnl_pct:.2f} gross_sum_pct={gross_sum:.2f} "
        f"pf={m.profit_factor:.3f} max_dd_pct={m.max_drawdown_pct:.2f} pnl_rs={pnl_rs:,.0f}"
    )
    if not dc.empty and "outcome" in dc.columns:
        print("outcomes:", dc["outcome"].value_counts().to_dict())


if __name__ == "__main__":
    main()
