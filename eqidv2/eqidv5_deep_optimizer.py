# -*- coding: utf-8 -*-
"""
eqidv5_deep_optimizer.py
========================

Exhaustive scenario sweep for eqidv5 VP + AVWAP + Liquidity Sweep strategy.

Tests 30+ parameter combinations across:
  A) Filter relaxation  (5m confirm, ADX, EMA, RSI gates)
  B) Signal quality     (AVWAP slope, close quality, swing lookback)
  C) Volume thresholds  (sweep_vol_ratio)
  D) Risk management    (min_rr, SL buffer, BE, trail)
  E) Entry windows      (morning vs all day)
  F) Combined best attempts

Run:
    python eqidv5_deep_optimizer.py
    python eqidv5_deep_optimizer.py --workers 8 --max_tickers 100
    python eqidv5_deep_optimizer.py --workers 8   # all tickers
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

_this_dir = Path(__file__).resolve().parent
if str(_this_dir) not in sys.path:
    sys.path.insert(0, str(_this_dir))

from eqidv5_combined_runner import _add_notional_pnl, _run_side_parallel
from eqidv5_strategy_common import (
    BacktestMetrics,
    compute_backtest_metrics,
    default_long_config,
    default_short_config,
)


# ===========================================================================
# SCENARIO DEFINITIONS
# ===========================================================================
def _scenarios() -> List[Tuple[str, Dict]]:
    """
    Return (name, overrides_dict) pairs.
    Overrides are applied to BOTH long and short default configs.
    """

    # -----------------------------------------------------------------------
    # GROUP A: Filter relaxation — find the trade count floor
    # -----------------------------------------------------------------------
    group_a = [
        # Baseline (all defaults): vol=2.2, adx>=25, ema on, rsi on, 5m required
        ("A1_baseline", {}),

        # Disable only 5m confirmation requirement
        ("A2_no5m",  {"require_5m_confirmation": False}),

        # Disable ADX hard gate only
        ("A3_no_adx", {"require_adx_filter": False}),

        # Disable EMA alignment gate only
        ("A4_no_ema", {"require_ema_alignment": False}),

        # Disable RSI gate only
        ("A5_no_rsi", {"require_rsi_filter": False}),

        # Disable all secondary gates (keep sweep + avwap + vol + absorption as core)
        ("A6_core_only", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
        }),

        # Core only + lower vol threshold
        ("A7_core_vol15", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
            "sweep_vol_ratio":         1.50,
        }),

        ("A8_core_vol18", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
            "sweep_vol_ratio":         1.80,
        }),
    ]

    # -----------------------------------------------------------------------
    # GROUP B: Signal quality improvements
    # -----------------------------------------------------------------------
    group_b = [
        # AVWAP slope filter (rising for LONG, falling for SHORT)
        ("B1_avwap_slope", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "require_avwap_slope":     True,
        }),

        # Close quality filter (close in top/bottom 55% of bar)
        ("B2_close_quality", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.55,
        }),

        # Both quality filters
        ("B3_slope_and_close", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "require_avwap_slope":     True,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.55,
        }),

        # Tight close quality (top/bottom 65%)
        ("B4_close_quality_tight", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "require_avwap_slope":     True,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.65,
        }),

        # Shorter swing lookback = more recent, tighter swings
        ("B5_swing3", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "swing_lookback":          3,
        }),

        # Longer swing lookback = bigger structural sweeps
        ("B6_swing7", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "swing_lookback":          7,
        }),

        # Morning session only (9:50 - 11:30)
        ("B7_morning_only", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "entry_start":             dtime(9, 50, 0),
            "entry_end":               dtime(11, 30, 0),
        }),

        # Afternoon session only (13:00 - 14:45)
        ("B8_afternoon_only", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "sweep_vol_ratio":         1.80,
            "entry_start":             dtime(13, 0, 0),
            "entry_end":               dtime(14, 45, 0),
        }),
    ]

    # -----------------------------------------------------------------------
    # GROUP C: Risk management tuning
    # -----------------------------------------------------------------------
    base_c = {
        "require_5m_confirmation": False,
        "require_adx_filter":      False,
        "require_ema_alignment":   False,
        "sweep_vol_ratio":         1.80,
    }

    group_c = [
        # Tighter SL buffer (less giveaway)
        ("C1_tight_sl", {**base_c,
            "sl_buffer_pct":  0.0005,
            "min_rr":         1.80,
        }),

        # Lower min R:R = more trades that qualify
        ("C2_rr15", {**base_c, "min_rr": 1.50}),
        ("C3_rr18", {**base_c, "min_rr": 1.80}),
        ("C4_rr20", {**base_c, "min_rr": 2.00}),

        # Fast breakeven
        ("C5_fast_be", {**base_c,
            "be_trigger_pct": 0.0030,
            "be_pad_pct":     0.0010,
            "trail_pct":      0.0030,
            "min_rr":         1.80,
        }),

        # Slow BE + wide trail (let winners run more)
        ("C6_slow_be_wide_trail", {**base_c,
            "be_trigger_pct":       0.0065,
            "be_pad_pct":           0.0010,
            "trail_pct":            0.0060,
            "min_rr":               1.80,
        }),

        # No trailing stop (take full target)
        ("C7_no_trail", {**base_c,
            "enable_trailing_stop": False,
            "be_trigger_pct":       0.0040,
            "min_rr":               1.80,
        }),

        # Heavier weight on T1 (AVWAP = closest, most reliably hit)
        ("C8_t1_heavy", {**base_c,
            "t1_pct": 0.60,
            "t2_pct": 0.30,
            "t3_pct": 0.10,
            "min_rr": 1.60,
        }),
    ]

    # -----------------------------------------------------------------------
    # GROUP D: Quality score + VP tuning
    # -----------------------------------------------------------------------
    base_d = {
        "require_5m_confirmation": False,
        "require_adx_filter":      False,
        "require_ema_alignment":   False,
        "sweep_vol_ratio":         1.80,
        "min_rr":                  1.80,
    }

    group_d = [
        ("D1_score4",  {**base_d, "min_quality_score": 4}),
        ("D2_score5",  {**base_d, "min_quality_score": 5}),
        ("D3_score6",  {**base_d, "min_quality_score": 6}),
        ("D4_score7",  {**base_d, "min_quality_score": 7}),
        ("D5_score8",  {**base_d, "min_quality_score": 8}),

        # Wider VP proximity tolerance
        ("D6_vp_wide", {**base_d, "near_level_atr_mult": 1.80}),
        # Tighter VP proximity
        ("D7_vp_tight", {**base_d, "near_level_atr_mult": 0.80}),
        # No VP required as hard gate
        ("D8_no_vp_gate", {**base_d, "require_vp_confluence": False}),
    ]

    # -----------------------------------------------------------------------
    # GROUP E: Combined best attempts
    # -----------------------------------------------------------------------
    group_e = [
        # Best v1: core only + avwap slope + close quality + fast BE + rr18
        ("E1_best_v1", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
            "sweep_vol_ratio":         1.80,
            "min_rr":                  1.80,
            "min_quality_score":       6,
            "require_avwap_slope":     True,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.55,
            "be_trigger_pct":          0.0035,
            "be_pad_pct":              0.0010,
            "trail_pct":               0.0040,
        }),

        # Best v2: core + avwap slope + morning window + score6 + rr18
        ("E2_best_v2", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
            "sweep_vol_ratio":         1.80,
            "min_rr":                  1.80,
            "min_quality_score":       6,
            "require_avwap_slope":     True,
            "entry_start":             dtime(9, 50, 0),
            "entry_end":               dtime(11, 30, 0),
        }),

        # Best v3: ADX=20 (soft) + close quality + tight sl + score6
        ("E3_best_v3", {
            "require_5m_confirmation": False,
            "require_adx_filter":      True,
            "adx_min":                 20.0,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
            "sweep_vol_ratio":         1.80,
            "min_rr":                  1.80,
            "min_quality_score":       6,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.55,
            "sl_buffer_pct":           0.0005,
        }),

        # Best v4: swing3 + close quality + avwap slope + score5 + rr15
        ("E4_best_v4", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
            "sweep_vol_ratio":         1.80,
            "swing_lookback":          3,
            "min_rr":                  1.50,
            "min_quality_score":       5,
            "require_avwap_slope":     True,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.55,
            "t1_pct":                  0.55,
            "t2_pct":                  0.30,
            "t3_pct":                  0.15,
        }),

        # Best v5: ultra strict (vol=2.5, score=9, avwap slope, close quality)
        ("E5_ultra_strict", {
            "require_5m_confirmation": False,
            "require_adx_filter":      True,
            "adx_min":                 25.0,
            "require_ema_alignment":   True,
            "require_rsi_filter":      True,
            "sweep_vol_ratio":         2.50,
            "min_rr":                  2.00,
            "min_quality_score":       9,
            "require_avwap_slope":     True,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.65,
        }),

        # Best v6: balanced (no 5m, adx=22, ema off, vol=1.8, slope+quality, score=6, rr=1.8)
        ("E6_balanced", {
            "require_5m_confirmation": False,
            "require_adx_filter":      True,
            "adx_min":                 22.0,
            "require_ema_alignment":   False,
            "require_rsi_filter":      True,
            "sweep_vol_ratio":         1.80,
            "min_rr":                  1.80,
            "min_quality_score":       6,
            "require_avwap_slope":     True,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.60,
            "sl_buffer_pct":           0.0008,
            "be_trigger_pct":          0.0040,
            "trail_pct":               0.0040,
        }),

        # Best v7: swing7 + close quality + avwap slope + t1 heavy exit
        ("E7_swing7_quality", {
            "require_5m_confirmation": False,
            "require_adx_filter":      False,
            "require_ema_alignment":   False,
            "require_rsi_filter":      False,
            "sweep_vol_ratio":         1.80,
            "swing_lookback":          7,
            "min_rr":                  1.80,
            "min_quality_score":       6,
            "require_avwap_slope":     True,
            "require_close_quality":   True,
            "close_quality_min_pct":   0.55,
            "t1_pct":                  0.60,
            "t2_pct":                  0.30,
            "t3_pct":                  0.10,
            "be_trigger_pct":          0.0035,
            "trail_pct":               0.0040,
        }),
    ]

    return group_a + group_b + group_c + group_d + group_e


# ===========================================================================
# SCORING OBJECTIVE
# ===========================================================================
def _objective(m: BacktestMetrics, total_notional_rs: float) -> float:
    """
    Composite score balancing:
      - Net PnL % (most important)
      - Profit factor (quality of edge)
      - Sharpe ratio (risk-adjusted)
      - Max drawdown (penalty)
      - Trade count (soft penalty for too few / too many)
    """
    if m.total_trades < 5:
        return -9999.0  # not enough trades to be meaningful

    score = (
          0.35 * m.sum_pnl_pct                           # net % return
        + 80.0  * max(0.0, m.profit_factor - 1.0)        # edge above 1.0
        + 25.0  * m.sharpe_ratio                         # risk-adjusted return
        - 0.40  * m.max_drawdown_pct                     # drawdown penalty
        + 0.10  * m.hit_rate_pct                         # directional accuracy
        - 2.00  * max(0.0, (m.total_trades / max(m.unique_days, 1)) - 15.0)  # overtrading penalty
    )
    return score


# ===========================================================================
# MAIN SWEEP
# ===========================================================================
def run_deep_sweep(
    dir15m: str,
    workers: int,
    max_tickers: int,
    out_csv: str,
) -> pd.DataFrame:
    dir5m_cands = [
        Path(dir15m).with_name(Path(dir15m).name.replace("15min", "5min")),
        Path("stocks_indicators_5min_eq"),
    ]
    dir5m = str(next((c for c in dir5m_cands if c.exists()), dir5m_cands[-1]))

    # Notional constants (match combined runner)
    LONG_NOTIONAL  = 100_000  # Rs.
    SHORT_NOTIONAL = 50_000
    LEVERAGE       = 5

    rows = []
    scenarios = _scenarios()
    n_total = len(scenarios)

    print(f"\n{'='*70}")
    print(f"  eqidv5 Deep Optimizer  |  {n_total} scenarios  |  workers={workers}")
    print(f"  15m: {dir15m}")
    print(f"  5m : {dir5m}")
    print(f"  max_tickers: {'ALL' if max_tickers <= 0 else max_tickers}")
    print(f"{'='*70}\n")

    for idx, (name, overrides) in enumerate(scenarios, 1):
        t0 = time.time()
        mt = max_tickers if max_tickers > 0 else None

        long_cfg  = default_long_config(dir_15m=dir15m,  dir_5m=dir5m,  **overrides)
        short_cfg = default_short_config(dir_15m=dir15m, dir_5m=dir5m, **overrides)

        df_long  = _run_side_parallel("LONG",  long_cfg,  workers, mt)
        df_short = _run_side_parallel("SHORT", short_cfg, workers, mt)

        parts = [p for p in [df_long, df_short] if not p.empty]
        df_combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

        if not df_combined.empty:
            df_combined = _add_notional_pnl(df_combined)

        m = compute_backtest_metrics(df_combined)

        # Notional PnL in Rs.  (column added by _add_notional_pnl is "pnl_rs")
        notional_long_rs  = 0.0
        notional_short_rs = 0.0
        if not df_long.empty:
            df_long = _add_notional_pnl(df_long)
            if "pnl_rs" in df_long.columns:
                notional_long_rs = float(
                    pd.to_numeric(df_long["pnl_rs"], errors="coerce").fillna(0).sum()
                )
        if not df_short.empty:
            df_short = _add_notional_pnl(df_short)
            if "pnl_rs" in df_short.columns:
                notional_short_rs = float(
                    pd.to_numeric(df_short["pnl_rs"], errors="coerce").fillna(0).sum()
                )
        total_notional_rs = notional_long_rs + notional_short_rs

        obj = _objective(m, total_notional_rs)
        elapsed = round(time.time() - t0, 1)
        days = max(m.unique_days, 1)

        row = {
            "rank":            0,
            "scenario":        name,
            "trades":          m.total_trades,
            "days":            m.unique_days,
            "tpd":             round(m.total_trades / days, 2),
            "sum_pnl_pct":     round(m.sum_pnl_pct, 2),
            "avg_pnl_pct":     round(m.avg_pnl_pct, 4),
            "profit_factor":   round(m.profit_factor, 3),
            "hit_rate_pct":    round(m.hit_rate_pct, 1),
            "sl_rate_pct":     round(m.sl_rate_pct, 1),
            "be_rate_pct":     round(m.be_rate_pct, 1),
            "eod_rate_pct":    round(m.eod_rate_pct, 1),
            "max_dd_pct":      round(m.max_drawdown_pct, 2),
            "sharpe":          round(m.sharpe_ratio, 3),
            "sortino":         round(m.sortino_ratio, 3),
            "calmar":          round(m.calmar_ratio, 3),
            "avg_quality":     round(m.avg_quality_score, 2),
            "avg_vol_ratio":   round(m.avg_volume_ratio, 2),
            "notional_rs":     int(round(total_notional_rs)),
            "notional_long":   int(round(notional_long_rs)),
            "notional_short":  int(round(notional_short_rs)),
            "objective":       round(obj, 2),
            "elapsed_s":       elapsed,
        }
        rows.append(row)

        print(
            f"  [{idx:02d}/{n_total}] {name:<28} | "
            f"T={m.total_trades:4d} PnL={m.sum_pnl_pct:+7.2f}% "
            f"PF={m.profit_factor:.2f} DD={m.max_drawdown_pct:.2f}% "
            f"Sharpe={m.sharpe_ratio:.2f} "
            f"Rs={int(total_notional_rs):+,} "
            f"Obj={obj:.1f} [{elapsed}s]"
        )

    # ---- Rank and save ----
    df_out = pd.DataFrame(rows).sort_values("objective", ascending=False).reset_index(drop=True)
    df_out["rank"] = range(1, len(df_out) + 1)
    cols = ["rank"] + [c for c in df_out.columns if c != "rank"]
    df_out = df_out[cols]

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv, index=False)

    print(f"\n{'='*70}")
    print("  TOP 10 SCENARIOS BY OBJECTIVE SCORE")
    print(f"{'='*70}")
    top10 = df_out.head(10)
    display_cols = [
        "rank", "scenario", "trades", "sum_pnl_pct", "profit_factor",
        "hit_rate_pct", "max_dd_pct", "sharpe", "notional_rs", "objective",
    ]
    print(top10[display_cols].to_string(index=False))
    print(f"\n  Full results saved: {out_csv}")

    return df_out


# ===========================================================================
# ENTRY POINT
# ===========================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="eqidv5 Deep Optimizer -- 30+ scenario sweep")
    parser.add_argument(
        "--dir15m", default="stocks_indicators_15min_eq",
        help="15-min data directory"
    )
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4)
    parser.add_argument("--max_tickers", type=int, default=0, help="0 = all tickers")
    parser.add_argument(
        "--out", default="outputs_eqidv5/deep_optimizer_results.csv",
        help="Output CSV path"
    )
    args = parser.parse_args()

    df = run_deep_sweep(
        dir15m=args.dir15m,
        workers=args.workers,
        max_tickers=args.max_tickers,
        out_csv=args.out,
    )

    # Print best config details
    best = df.iloc[0]
    print(f"\n{'='*70}")
    print(f"  WINNER: {best['scenario']}")
    print(f"  Trades={best['trades']}  PnL={best['sum_pnl_pct']:+.2f}%  "
          f"PF={best['profit_factor']:.3f}  DD={best['max_dd_pct']:.2f}%  "
          f"Sharpe={best['sharpe']:.3f}  Notional=Rs.{best['notional_rs']:,}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
