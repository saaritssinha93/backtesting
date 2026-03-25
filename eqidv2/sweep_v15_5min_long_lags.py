from __future__ import annotations

import itertools
import json
import multiprocessing
import os
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

import avwap_combined_runner_v15_5min as r


OUTPUT_ROOT = Path(r"C:\TradingData\eqidv2_v15_5min_long_lag_sweep_20260324")
SUMMARY_CSV = OUTPUT_ROOT / "summary.csv"
SUMMARY_JSON = OUTPUT_ROOT / "summary.json"
BEST_TRADES_CSV = OUTPUT_ROOT / "best_long_trades.csv"
BEST_DAYWISE_CSV = OUTPUT_ROOT / "best_long_daywise.csv"

LAG_KEYS = [
    "A_MOD_BREAK_C1_HIGH",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
]
LAG_VALUES = [1, 2]
MAX_WORKERS = int(os.getenv("EQIDV2_LONG_LAG_SWEEP_WORKERS", "4"))


def _project_root() -> Path:
    script_dir = Path(r.__file__).resolve().parent
    return script_dir.parent if script_dir.name == "avwap_v11_refactored" else script_dir


def _detect_5m_suffix(dir_5m: Path) -> str:
    suffix = ".parquet"
    if dir_5m.is_dir():
        sample_files = list(dir_5m.glob("*"))[:5]
        for sample in sample_files:
            if sample.suffix:
                suffix = sample.suffix
                break
    return suffix


def _build_long_cfg(reports_dir: Path, lag_map: Dict[str, int]):
    dir_15m = r._resolve_15m_dir()
    long_cfg = r.default_long_config_v9(reports_dir=reports_dir)
    long_cfg.dir_15m = str(dir_15m)
    long_cfg.market_regime_tickers = tuple(r.NIFTY_CONTEXT_TICKERS)

    if r.ENABLE_PLAYBOOK_V11_PROFILE:
        long_cfg.require_entry_close_confirm = True
        long_cfg.enable_liquidity_sweep_filter = False
        long_cfg.enable_avwap_no_trade_zone = False
        long_cfg.adx_min = 17.0
        long_cfg.adx_slope_min = 0.50
        long_cfg.volume_min_ratio = 0.95
        long_cfg.rsi_min_long = 38.0
        long_cfg.stochk_min = 15.0
        long_cfg.stochk_max = 95.0
        long_cfg.atr_pct_min = 0.0025
        long_cfg.enable_setup_a_pullback_c2_break = True
        long_cfg.enable_setup_a_close_continuation_break = True
        long_cfg.enable_setup_b_huge_c1_close_reclaim_break = True
        long_cfg.stop_pct = 0.0075
        long_cfg.target_pct = 0.0110
        long_cfg.be_trigger_pct = 0.0055
        long_cfg.trail_pct = 0.0028
        long_cfg.min_bars_left_after_entry = 0
        long_cfg.max_vix_for_entries = 13.0
        long_cfg.max_trades_per_ticker_per_day = 4
        long_cfg.enable_topn_per_day = False
        long_cfg.topn_per_day = 0

    long_cfg.max_vix_for_entries = float(r.PACK2_LONG_MAX_VIX_FOR_ENTRIES)
    long_cfg.lag_bars_long_a_mod_break_c1_high = int(lag_map["A_MOD_BREAK_C1_HIGH"])
    long_cfg.lag_bars_long_a_close_continuation_break = int(
        lag_map["A_MOD_CLOSE_CONTINUATION_BREAK"]
    )
    long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(
        lag_map["A_PULLBACK_C2_THEN_BREAK_C2_HIGH"]
    )
    long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(
        r.LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK
    )
    long_cfg.lag_bars_long_b_huge_c1_close_reclaim_break = int(
        lag_map["B_HUGE_C1_CLOSE_RECLAIM_BREAK"]
    )

    if r.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        long_cfg.min_bars_left_after_entry = 0
    if r.FORCE_LIVE_PARITY_DISABLE_TOPN:
        long_cfg.enable_topn_per_day = False
    if r.FINAL_SIGNAL_WINDOW_OVERRIDE:
        long_cfg.use_time_windows = bool(r.FINAL_LONG_USE_TIME_WINDOWS)
        long_cfg.signal_windows = list(r.FINAL_LONG_SIGNAL_WINDOWS)
    if r.TEST_TARGET_OVERRIDE:
        long_cfg.target_pct = r.TEST_LONG_TARGET_PCT

    vix_map = r._load_india_vix(_project_root())
    long_cfg.vix_scale_enabled = r.VIX_SCALE_ENABLED
    if r.VIX_SCALE_ENABLED and vix_map:
        long_cfg.vix_daily = vix_map
        long_cfg.vix_baseline = r.VIX_BASELINE
        long_cfg.vix_scale_min = r.VIX_SCALE_MIN
        long_cfg.vix_scale_max = r.VIX_SCALE_MAX
        long_cfg.vix_scale_target = r.VIX_SCALE_TARGET
        long_cfg.vix_scale_sl = r.VIX_SCALE_SL

    regime_map, regime_source = r.build_market_regime_map(long_cfg)
    if regime_map:
        long_cfg.market_regime_map = regime_map
        long_cfg.enable_market_regime_filter = True
    else:
        long_cfg.enable_market_regime_filter = False
        regime_source = ""

    return long_cfg, Path(dir_15m), regime_source


def _apply_nifty_context_to_long(long_df: pd.DataFrame, long_cfg) -> pd.DataFrame:
    if not r.NIFTY_CONTEXT_ENABLED:
        return long_df
    mode_map, nifty_ret_map, _context_src, _counts = r._build_nifty_intraday_context(long_cfg)
    if not mode_map:
        return long_df
    _short, long_filtered = r._apply_nifty_intraday_context(
        pd.DataFrame(),
        long_df,
        long_cfg,
        mode_map,
        nifty_ret_map,
    )
    return long_filtered


def _combo_name(lag_map: Dict[str, int]) -> str:
    return (
        f"amod{lag_map['A_MOD_BREAK_C1_HIGH']}_"
        f"acont{lag_map['A_MOD_CLOSE_CONTINUATION_BREAK']}_"
        f"apull{lag_map['A_PULLBACK_C2_THEN_BREAK_C2_HIGH']}_"
        f"breclaim{lag_map['B_HUGE_C1_CLOSE_RECLAIM_BREAK']}"
    )


def _score_tuple(row: Dict[str, float]) -> Tuple[int, int, float, float, float, float]:
    avg_ok = 1 if 5.0 <= float(row["avg_trades_per_active_day"]) <= 10.0 else 0
    max_ok = 1 if 5 <= int(row["max_trades_in_day"]) <= 10 else 0
    return (
        avg_ok,
        max_ok,
        float(row["sum_pnl_pct"]),
        float(row["profit_factor"]),
        float(row["day_win_pct"]),
        -float(row["max_drawdown_pct"]),
    )


def _run_one_combo(lag_map: Dict[str, int]) -> Tuple[Dict[str, float], pd.DataFrame, pd.DataFrame]:
    reports_dir = OUTPUT_ROOT / "reports" / _combo_name(lag_map)
    reports_dir.mkdir(parents=True, exist_ok=True)
    dir_5m = r._resolve_5min_dir()
    suffix_5m = _detect_5m_suffix(dir_5m)

    long_cfg, dir_15m, regime_source = _build_long_cfg(reports_dir, lag_map)

    long_df = r._run_side_parallel("LONG", long_cfg, max_workers=MAX_WORKERS)
    long_df = _apply_nifty_context_to_long(long_df, long_cfg)

    if not long_df.empty:
        long_df = r._resolve_exits_5min(
            long_df,
            dir_5m,
            suffix_5m,
            long_cfg.parquet_engine,
            eod_exit_time=r.V15_EOD_EXIT_TIME,
        )
        long_df = r._add_notional_pnl(long_df)
        long_df = r._sort_trades_for_output(long_df)

    metrics = r.compute_backtest_metrics(long_df)
    daywise_df = r._build_daily_breakdown_df(long_df, include_total=False)

    active_days = int(len(daywise_df))
    positive_days = int((pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0) > 0).sum()) if active_days else 0
    negative_days = int((pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0) < 0).sum()) if active_days else 0
    best_day_idx = pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0).idxmax() if active_days else None
    worst_day_idx = pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0).idxmin() if active_days else None

    summary = {
        "combo": _combo_name(lag_map),
        **{f"lag_{k.lower()}": int(v) for k, v in lag_map.items()},
        "dir_15m": str(dir_15m),
        "regime_source": regime_source,
        "total_trades": int(metrics.total_trades),
        "active_trade_days": active_days,
        "avg_trades_per_active_day": round(float(metrics.total_trades) / active_days, 4) if active_days else 0.0,
        "max_trades_in_day": int(pd.to_numeric(daywise_df.get("Tot", 0), errors="coerce").fillna(0).max()) if active_days else 0,
        "positive_days": positive_days,
        "negative_days": negative_days,
        "day_win_pct": round((positive_days / active_days) * 100.0, 4) if active_days else 0.0,
        "hit_rate_pct": round(float(metrics.hit_rate_pct), 4),
        "sum_pnl_pct": round(float(metrics.sum_pnl_pct), 4),
        "avg_pnl_pct": round(float(metrics.avg_pnl_pct), 4),
        "profit_factor": round(float(metrics.profit_factor), 6) if metrics.profit_factor != float("inf") else float("inf"),
        "avg_win_pct": round(float(metrics.avg_win_pct), 4),
        "avg_loss_pct": round(float(metrics.avg_loss_pct), 4),
        "max_drawdown_pct": round(float(metrics.max_drawdown_pct), 4),
        "sharpe_ratio": round(float(metrics.sharpe_ratio), 4),
        "sortino_ratio": round(float(metrics.sortino_ratio), 4),
        "calmar_ratio": round(float(metrics.calmar_ratio), 4),
        "target_count": int(metrics.target_count),
        "sl_count": int(metrics.sl_count),
        "be_count": int(metrics.be_count),
        "eod_count": int(metrics.eod_count),
        "best_day": str(daywise_df.loc[best_day_idx, "Date"]) if best_day_idx is not None else "",
        "best_day_sum_pnl_pct": round(float(daywise_df.loc[best_day_idx, "SumPnL%"]), 4) if best_day_idx is not None else 0.0,
        "worst_day": str(daywise_df.loc[worst_day_idx, "Date"]) if worst_day_idx is not None else "",
        "worst_day_sum_pnl_pct": round(float(daywise_df.loc[worst_day_idx, "SumPnL%"]), 4) if worst_day_idx is not None else 0.0,
    }
    return summary, long_df, daywise_df


def main() -> None:
    multiprocessing.freeze_support()
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    combos = []
    for vals in itertools.product(LAG_VALUES, repeat=len(LAG_KEYS)):
        combos.append(dict(zip(LAG_KEYS, vals)))

    results: List[Dict[str, float]] = []
    best_summary = None
    best_long_df = pd.DataFrame()
    best_daywise_df = pd.DataFrame()

    for idx, lag_map in enumerate(combos, start=1):
        print(f"[{idx}/{len(combos)}] Running {_combo_name(lag_map)}", flush=True)
        summary, long_df, daywise_df = _run_one_combo(lag_map)
        results.append(summary)
        if best_summary is None or _score_tuple(summary) > _score_tuple(best_summary):
            best_summary = summary
            best_long_df = long_df.copy()
            best_daywise_df = daywise_df.copy()
        print(
            "[DONE] "
            f"trades={summary['total_trades']} | "
            f"sum_pnl={summary['sum_pnl_pct']:.2f}% | "
            f"pf={summary['profit_factor']} | "
            f"win={summary['hit_rate_pct']:.2f}% | "
            f"avg_trades/active_day={summary['avg_trades_per_active_day']:.2f}",
            flush=True,
        )

    summary_df = pd.DataFrame(results)
    sort_cols = [
        "sum_pnl_pct",
        "profit_factor",
        "day_win_pct",
        "hit_rate_pct",
    ]
    summary_df = summary_df.sort_values(sort_cols, ascending=[False, False, False, False]).reset_index(drop=True)
    summary_df.to_csv(SUMMARY_CSV, index=False)
    SUMMARY_JSON.write_text(
        json.dumps(
            {
                "max_workers": MAX_WORKERS,
                "tested_combos": len(combos),
                "best_by_score": best_summary,
                "top10": summary_df.head(10).to_dict(orient="records"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    if not best_long_df.empty:
        best_long_df.to_csv(BEST_TRADES_CSV, index=False)
    if not best_daywise_df.empty:
        best_daywise_df.to_csv(BEST_DAYWISE_CSV, index=False)

    print(f"[RESULT] Summary CSV: {SUMMARY_CSV}")
    print(f"[RESULT] Summary JSON: {SUMMARY_JSON}")
    print(f"[RESULT] Best combo: {best_summary['combo'] if best_summary else 'NA'}")


if __name__ == "__main__":
    main()
