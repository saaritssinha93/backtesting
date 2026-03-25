from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd

import avwap_combined_runner_v15_5min as r


OUTPUT_ROOT = Path(r"C:\TradingData\eqidv2_v15_5min_long_quality_sweep_20260325")
SUMMARY_CSV = OUTPUT_ROOT / "summary.csv"
SUMMARY_JSON = OUTPUT_ROOT / "summary.json"
BEST_TRADES_CSV = OUTPUT_ROOT / "best_long_trades.csv"
BEST_DAYWISE_CSV = OUTPUT_ROOT / "best_long_daywise.csv"

MAX_WORKERS = int(os.getenv("EQIDV2_LONG_QUALITY_SWEEP_WORKERS", "4"))
MIN_TOTAL_TRADES = int(os.getenv("EQIDV2_LONG_QUALITY_MIN_TRADES", "80"))

STRICT_NIFTY = {
    "daymove_pct": 0.50,
    "rs_threshold_pct": 0.30,
    "both_mode_long_pct": 0.15,
}

PROFILES: List[Dict[str, object]] = [
    {
        "profile": "amod_only",
        "enable_a_cont": False,
        "enable_a_pull": False,
        "enable_b_reclaim": False,
        "lag_a_mod": 1,
        "lag_a_cont": 1,
        "lag_a_pull": 1,
        "lag_b_reclaim": 1,
    },
    {
        "profile": "amod_reclaim",
        "enable_a_cont": False,
        "enable_a_pull": False,
        "enable_b_reclaim": True,
        "lag_a_mod": 1,
        "lag_a_cont": 1,
        "lag_a_pull": 1,
        "lag_b_reclaim": 1,
    },
    {
        "profile": "amod_acont",
        "enable_a_cont": True,
        "enable_a_pull": False,
        "enable_b_reclaim": False,
        "lag_a_mod": 1,
        "lag_a_cont": 1,
        "lag_a_pull": 1,
        "lag_b_reclaim": 1,
    },
    {
        "profile": "balanced_strict",
        "enable_a_cont": True,
        "enable_a_pull": True,
        "enable_b_reclaim": True,
        "lag_a_mod": 1,
        "lag_a_cont": 2,
        "lag_a_pull": 1,
        "lag_b_reclaim": 2,
    },
]

ADX_VALUES = [17.0, 20.0, 22.0]
VOLUME_VALUES = [0.95, 1.05]
RSI_VALUES = [38.0, 42.0, 45.0]


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


def _combo_name(profile: Dict[str, object], adx: float, vol: float, rsi: float) -> str:
    return (
        f"{profile['profile']}"
        f"_adx{int(round(adx * 10)):03d}"
        f"_vol{int(round(vol * 100)):03d}"
        f"_rsi{int(round(rsi)):02d}"
    )


def _build_long_cfg(reports_dir: Path, profile: Dict[str, object], adx: float, vol: float, rsi: float):
    dir_15m = r._resolve_15m_dir()
    long_cfg = r.default_long_config_v9(reports_dir=reports_dir)
    long_cfg.dir_15m = str(dir_15m)
    long_cfg.market_regime_tickers = tuple(r.NIFTY_CONTEXT_TICKERS)

    long_cfg.require_entry_close_confirm = True
    long_cfg.enable_liquidity_sweep_filter = False
    long_cfg.enable_avwap_no_trade_zone = False
    long_cfg.adx_min = float(adx)
    long_cfg.adx_slope_min = 0.50
    long_cfg.volume_min_ratio = float(vol)
    long_cfg.rsi_min_long = float(rsi)
    long_cfg.stochk_min = 15.0
    long_cfg.stochk_max = 95.0
    long_cfg.atr_pct_min = 0.0025
    long_cfg.enable_setup_a_pullback_c2_break = bool(profile["enable_a_pull"])
    long_cfg.enable_setup_a_close_continuation_break = bool(profile["enable_a_cont"])
    long_cfg.enable_setup_b_huge_c1_close_reclaim_break = bool(profile["enable_b_reclaim"])
    long_cfg.stop_pct = 0.0075
    long_cfg.target_pct = 0.0110
    long_cfg.be_trigger_pct = 0.0055
    long_cfg.trail_pct = 0.0028
    long_cfg.min_bars_left_after_entry = 0
    long_cfg.max_vix_for_entries = float(r.PACK2_LONG_MAX_VIX_FOR_ENTRIES)
    long_cfg.max_trades_per_ticker_per_day = 4
    long_cfg.enable_topn_per_day = False
    long_cfg.topn_per_day = 0

    long_cfg.lag_bars_long_a_mod_break_c1_high = int(profile["lag_a_mod"])
    long_cfg.lag_bars_long_a_close_continuation_break = int(profile["lag_a_cont"])
    long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(profile["lag_a_pull"])
    long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(r.LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)
    long_cfg.lag_bars_long_b_huge_c1_close_reclaim_break = int(profile["lag_b_reclaim"])

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


def _apply_strict_nifty_context_to_long(long_df: pd.DataFrame, long_cfg) -> pd.DataFrame:
    prev_daymove = r.NIFTY_CONTEXT_MIN_DAYMOVE_PCT
    prev_rs = r.NIFTY_RS_THRESHOLD_PCT
    prev_both_long = r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT
    prev_both_shared = r.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT
    try:
        r.NIFTY_CONTEXT_MIN_DAYMOVE_PCT = STRICT_NIFTY["daymove_pct"]
        r.NIFTY_RS_THRESHOLD_PCT = STRICT_NIFTY["rs_threshold_pct"]
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT = STRICT_NIFTY["both_mode_long_pct"]
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = STRICT_NIFTY["both_mode_long_pct"]
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
    finally:
        r.NIFTY_CONTEXT_MIN_DAYMOVE_PCT = prev_daymove
        r.NIFTY_RS_THRESHOLD_PCT = prev_rs
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT = prev_both_long
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = prev_both_shared


def _score_tuple(row: Dict[str, float]) -> tuple:
    enough_trades = 1 if int(row["total_trades"]) >= MIN_TOTAL_TRADES else 0
    return (
        enough_trades,
        float(row["profit_factor"]),
        float(row["hit_rate_pct"]),
        float(row["sum_pnl_pct"]),
        -float(row["max_drawdown_pct"]),
    )


def _run_one_combo(profile: Dict[str, object], adx: float, vol: float, rsi: float):
    combo = _combo_name(profile, adx, vol, rsi)
    reports_dir = OUTPUT_ROOT / "reports" / combo
    reports_dir.mkdir(parents=True, exist_ok=True)

    dir_5m = r._resolve_5min_dir()
    suffix_5m = _detect_5m_suffix(dir_5m)

    long_cfg, dir_15m, regime_source = _build_long_cfg(reports_dir, profile, adx, vol, rsi)
    long_df = r._run_side_parallel("LONG", long_cfg, max_workers=MAX_WORKERS)
    long_df = _apply_strict_nifty_context_to_long(long_df, long_cfg)

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
    setup_counts = (
        long_df.groupby("setup").size().to_dict()
        if not long_df.empty and "setup" in long_df.columns else {}
    )

    active_days = int(len(daywise_df))
    positive_days = int((pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0) > 0).sum()) if active_days else 0
    best_day_idx = pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0).idxmax() if active_days else None
    worst_day_idx = pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0).idxmin() if active_days else None

    summary = {
        "combo": combo,
        "profile": str(profile["profile"]),
        "adx_min": float(adx),
        "volume_min_ratio": float(vol),
        "rsi_min_long": float(rsi),
        "strict_nifty_daymove_pct": STRICT_NIFTY["daymove_pct"],
        "strict_nifty_rs_pct": STRICT_NIFTY["rs_threshold_pct"],
        "strict_nifty_both_long_pct": STRICT_NIFTY["both_mode_long_pct"],
        "total_trades": int(metrics.total_trades),
        "active_trade_days": active_days,
        "avg_trades_per_active_day": round(float(metrics.total_trades) / active_days, 4) if active_days else 0.0,
        "day_win_pct": round((positive_days / active_days) * 100.0, 4) if active_days else 0.0,
        "hit_rate_pct": round(float(metrics.hit_rate_pct), 4),
        "sum_pnl_pct": round(float(metrics.sum_pnl_pct), 4),
        "avg_pnl_pct": round(float(metrics.avg_pnl_pct), 4),
        "profit_factor": round(float(metrics.profit_factor), 6) if metrics.profit_factor != float("inf") else float("inf"),
        "max_drawdown_pct": round(float(metrics.max_drawdown_pct), 4),
        "target_count": int(metrics.target_count),
        "sl_count": int(metrics.sl_count),
        "eod_count": int(metrics.eod_count),
        "regime_source": regime_source,
        "dir_15m": str(dir_15m),
        "setup_counts": json.dumps(setup_counts, sort_keys=True),
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
    for profile in PROFILES:
        for adx in ADX_VALUES:
            for vol in VOLUME_VALUES:
                for rsi in RSI_VALUES:
                    combos.append((profile, adx, vol, rsi))

    results = []
    best_summary = None
    best_trades = pd.DataFrame()
    best_daywise = pd.DataFrame()

    for idx, (profile, adx, vol, rsi) in enumerate(combos, start=1):
        combo = _combo_name(profile, adx, vol, rsi)
        print(f"[{idx}/{len(combos)}] Running {combo}", flush=True)
        summary, trades_df, daywise_df = _run_one_combo(profile, adx, vol, rsi)
        results.append(summary)
        if best_summary is None or _score_tuple(summary) > _score_tuple(best_summary):
            best_summary = summary
            best_trades = trades_df.copy()
            best_daywise = daywise_df.copy()
        print(
            "[DONE] "
            f"profile={summary['profile']} | "
            f"trades={summary['total_trades']} | "
            f"pf={summary['profit_factor']} | "
            f"win={summary['hit_rate_pct']:.2f}% | "
            f"sum_pnl={summary['sum_pnl_pct']:.2f}% | "
            f"dd={summary['max_drawdown_pct']:.2f}%",
            flush=True,
        )

    summary_df = pd.DataFrame(results).sort_values(
        ["profit_factor", "hit_rate_pct", "sum_pnl_pct", "max_drawdown_pct"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    summary_df.to_csv(SUMMARY_CSV, index=False)
    SUMMARY_JSON.write_text(
        json.dumps(
            {
                "strict_nifty": STRICT_NIFTY,
                "min_total_trades_for_score": MIN_TOTAL_TRADES,
                "tested_combos": len(combos),
                "best_by_score": best_summary,
                "top15": summary_df.head(15).to_dict(orient="records"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    if not best_trades.empty:
        best_trades.to_csv(BEST_TRADES_CSV, index=False)
    if not best_daywise.empty:
        best_daywise.to_csv(BEST_DAYWISE_CSV, index=False)

    print(f"[RESULT] Summary CSV: {SUMMARY_CSV}")
    print(f"[RESULT] Summary JSON: {SUMMARY_JSON}")
    print(f"[RESULT] Best combo: {best_summary['combo'] if best_summary else 'NA'}")


if __name__ == "__main__":
    main()
