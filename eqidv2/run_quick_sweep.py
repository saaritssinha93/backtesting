"""
Quick 6-config sweep on ALL tickers.
MUST run from the strategy directory.
Windows multiprocessing requires if __name__ == "__main__" guard.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import time as dtime
import pandas as pd

from eqidv5_combined_runner import _add_notional_pnl, _run_side_parallel
from eqidv5_strategy_common import (
    compute_backtest_metrics, default_long_config, default_short_config, print_metrics,
)

WORKERS = 2
DIR15M  = "stocks_indicators_15min_eq"
DIR5M   = "stocks_indicators_5min_eq"

CONFIGS = [
    ("C1_strict_baseline", {}),
    ("C2_core_relaxed", {
        "require_5m_confirmation": False, "require_adx_filter": False,
        "require_ema_alignment": False, "require_rsi_filter": False,
        "sweep_vol_ratio": 1.80, "min_quality_score": 5,
    }),
    ("C3_avwap_slope", {
        "require_5m_confirmation": False, "require_adx_filter": False,
        "require_ema_alignment": False, "require_rsi_filter": False,
        "sweep_vol_ratio": 1.80, "min_quality_score": 5,
        "require_avwap_slope": True,
    }),
    ("C4_close_quality", {
        "require_5m_confirmation": False, "require_adx_filter": False,
        "require_ema_alignment": False, "require_rsi_filter": False,
        "sweep_vol_ratio": 1.80, "min_quality_score": 5,
        "require_close_quality": True, "close_quality_min_pct": 0.55,
    }),
    ("C5_best_combo", {
        "require_5m_confirmation": False, "require_adx_filter": True, "adx_min": 20.0,
        "require_ema_alignment": False, "require_rsi_filter": False,
        "sweep_vol_ratio": 1.80, "min_quality_score": 6,
        "require_avwap_slope": True, "require_close_quality": True, "close_quality_min_pct": 0.55,
        "be_trigger_pct": 0.0035, "be_pad_pct": 0.0010, "trail_pct": 0.0040,
        "min_rr": 1.80, "t1_pct": 0.55, "t2_pct": 0.30, "t3_pct": 0.15,
    }),
    ("C6_morning_slope_quality", {
        "require_5m_confirmation": False, "require_adx_filter": False,
        "require_ema_alignment": False, "require_rsi_filter": False,
        "sweep_vol_ratio": 1.80, "min_quality_score": 5,
        "require_avwap_slope": True, "require_close_quality": True, "close_quality_min_pct": 0.55,
        "entry_start": dtime(9, 50, 0), "entry_end": dtime(11, 30, 0),
    }),
]


def _pnl_rs(df):
    if df.empty or "pnl_rs" not in df.columns:
        return 0.0
    return float(pd.to_numeric(df["pnl_rs"], errors="coerce").fillna(0).sum())


def run_sweep():
    rows = []
    for name, overrides in CONFIGS:
        t0 = time.time()
        print(f"\n{'='*60}\n  Config: {name}\n{'='*60}")
        long_cfg  = default_long_config(dir_15m=DIR15M,  dir_5m=DIR5M,  **overrides)
        short_cfg = default_short_config(dir_15m=DIR15M, dir_5m=DIR5M, **overrides)
        df_long   = _run_side_parallel("LONG",  long_cfg,  WORKERS, None)
        df_short  = _run_side_parallel("SHORT", short_cfg, WORKERS, None)

        parts   = [p for p in [df_long, df_short] if not p.empty]
        df_comb = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

        if not df_long.empty:  df_long  = _add_notional_pnl(df_long)
        if not df_short.empty: df_short = _add_notional_pnl(df_short)
        if not df_comb.empty:  df_comb  = _add_notional_pnl(df_comb)

        m    = compute_backtest_metrics(df_comb)
        days = max(m.unique_days, 1)
        pnl_l, pnl_s = _pnl_rs(df_long), _pnl_rs(df_short)
        pnl_t = pnl_l + pnl_s
        elapsed = round(time.time() - t0, 1)

        print_metrics(name, m)
        print(f"  Notional Rs: LONG=Rs.{pnl_l:+,.0f}  SHORT=Rs.{pnl_s:+,.0f}  TOTAL=Rs.{pnl_t:+,.0f}")
        print(f"  Elapsed: {elapsed}s")

        rows.append({
            "config": name, "trades": m.total_trades,
            "long_trades": len(df_long) if not df_long.empty else 0,
            "short_trades": len(df_short) if not df_short.empty else 0,
            "days": m.unique_days, "tpd": round(m.total_trades/days,2),
            "sum_pnl_pct": round(m.sum_pnl_pct,2), "avg_pnl_pct": round(m.avg_pnl_pct,4),
            "profit_factor": round(m.profit_factor,3), "hit_rate_pct": round(m.hit_rate_pct,1),
            "sl_rate_pct": round(m.sl_rate_pct,1), "be_rate_pct": round(m.be_rate_pct,1),
            "eod_rate_pct": round(m.eod_rate_pct,1), "max_dd_pct": round(m.max_drawdown_pct,2),
            "sharpe": round(m.sharpe_ratio,3), "sortino": round(m.sortino_ratio,3),
            "calmar": round(m.calmar_ratio,3), "avg_quality": round(m.avg_quality_score,2),
            "pnl_long_rs": int(pnl_l), "pnl_short_rs": int(pnl_s), "pnl_total_rs": int(pnl_t),
            "elapsed_s": elapsed,
        })

    df_r = pd.DataFrame(rows).sort_values("sum_pnl_pct", ascending=False)
    out  = "outputs_eqidv5/quick_sweep_results.csv"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df_r.to_csv(out, index=False)

    print("\n" + "="*70)
    print("  FINAL RANKING (sorted by sum PnL %)")
    print("="*70)
    cols = ["config","trades","sum_pnl_pct","profit_factor","hit_rate_pct","max_dd_pct","sharpe","pnl_total_rs"]
    print(df_r[cols].to_string(index=False))
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    run_sweep()
