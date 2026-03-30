from __future__ import annotations

from pathlib import Path

import pandas as pd

import avwap_combined_runner_v15 as r
import sweep_v15_long_quality as sq


OUTPUT_ROOT = Path(r"C:\TradingData\eqidv2_v15_long_recovery_sweep_20260326")
SUMMARY_CSV = OUTPUT_ROOT / "summary.csv"

PROFILE_AMOD = {
    "profile": "amod_only",
    "enable_a_cont": False,
    "enable_a_pull": False,
    "enable_b_reclaim": False,
    "lag_a_mod": 1,
    "lag_a_cont": 1,
    "lag_a_pull": 1,
    "lag_b_reclaim": 1,
}

WINDOWS_CURRENT = [
    (r.dtime(9, 15, 0), r.dtime(10, 15, 0)),
    (r.dtime(11, 0, 0), r.dtime(11, 59, 0)),
    (r.dtime(13, 0, 0), r.dtime(14, 15, 0)),
]
WINDOWS_FULL = [
    (r.dtime(9, 15, 0), r.dtime(14, 30, 0)),
]

CASES = [
    {
        "name": "baseline_elite",
        "adx": 22.0,
        "vol": 0.95,
        "rsi": 50.0,
        "qmin": 4.5,
        "avmin": 0.4,
        "rs_both_long": 2.0,
        "windows": WINDOWS_CURRENT,
    },
    {
        "name": "open_full_rs2_0_q4_0_av0_5_r55_a20",
        "adx": 20.0,
        "vol": 0.95,
        "rsi": 55.0,
        "qmin": 4.0,
        "avmin": 0.5,
        "rs_both_long": 2.0,
        "windows": WINDOWS_FULL,
    },
    {
        "name": "open_full_rs1_5_q4_0_av0_5_r55_a20",
        "adx": 20.0,
        "vol": 0.95,
        "rsi": 55.0,
        "qmin": 4.0,
        "avmin": 0.5,
        "rs_both_long": 1.5,
        "windows": WINDOWS_FULL,
    },
    {
        "name": "open_full_rs1_2_q4_0_av0_5_r55_a20",
        "adx": 20.0,
        "vol": 0.95,
        "rsi": 55.0,
        "qmin": 4.0,
        "avmin": 0.5,
        "rs_both_long": 1.2,
        "windows": WINDOWS_FULL,
    },
    {
        "name": "open_current_rs1_2_q4_0_av0_5_r55_a20",
        "adx": 20.0,
        "vol": 0.95,
        "rsi": 55.0,
        "qmin": 4.0,
        "avmin": 0.5,
        "rs_both_long": 1.2,
        "windows": WINDOWS_CURRENT,
    },
    {
        "name": "open_full_rs1_2_q4_0_av0_3_r55_a20",
        "adx": 20.0,
        "vol": 0.95,
        "rsi": 55.0,
        "qmin": 4.0,
        "avmin": 0.3,
        "rs_both_long": 1.2,
        "windows": WINDOWS_FULL,
    },
    {
        "name": "tight_full_rs1_2_q4_5_av0_5_r55_a22",
        "adx": 22.0,
        "vol": 0.95,
        "rsi": 55.0,
        "qmin": 4.5,
        "avmin": 0.5,
        "rs_both_long": 1.2,
        "windows": WINDOWS_FULL,
    },
]


def _detect_1m_suffix(dir_1m: Path) -> str:
    suffix = ".parquet"
    if dir_1m.is_dir():
        for sample in list(dir_1m.glob("*"))[:5]:
            if sample.suffix:
                return sample.suffix
    return suffix


def _apply_custom_nifty_context(
    long_df: pd.DataFrame,
    long_cfg,
    rs_both_long: float,
) -> pd.DataFrame:
    prev_both_long = r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT
    prev_both_shared = r.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT
    try:
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT = float(rs_both_long)
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = float(rs_both_long)
        mode_map, nifty_ret_map, _context_src, _counts = r._build_nifty_intraday_context(
            long_cfg
        )
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
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT = prev_both_long
        r.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT = prev_both_shared


def _build_case_cfg(case: dict, reports_dir: Path):
    long_cfg, dir_15m, regime_source = sq._build_long_cfg(
        reports_dir,
        PROFILE_AMOD,
        case["adx"],
        case["vol"],
        case["rsi"],
    )
    long_cfg.enable_setup_a_pullback_c2_break = False
    long_cfg.enable_setup_a_close_continuation_break = False
    long_cfg.enable_setup_b_huge_c1_close_reclaim_break = False
    long_cfg.quality_score_min = float(case["qmin"])
    long_cfg.signal_avwap_dist_atr_min = float(case["avmin"])
    long_cfg.use_time_windows = True
    long_cfg.signal_windows = list(case["windows"])
    long_cfg.max_vix_for_entries = 0.0
    long_cfg.max_entry_slip_pct = float(r.LONG_MAX_ENTRY_SLIP_PCT)
    long_cfg.entry_at_bar_close = bool(r.LONG_ENTRY_AT_BAR_CLOSE)
    long_cfg.entry_at_next_open = bool(r.LONG_ENTRY_AT_NEXT_OPEN)
    return long_cfg, dir_15m, regime_source


def _run_case(case: dict, dir_1m: Path, suffix_1m: str) -> dict:
    reports_dir = OUTPUT_ROOT / "reports" / str(case["name"])
    reports_dir.mkdir(parents=True, exist_ok=True)

    long_cfg, _dir_15m, regime_source = _build_case_cfg(case, reports_dir)
    long_df = r._run_side_parallel("LONG", long_cfg, max_workers=4)
    pre_context_trades = int(len(long_df))
    long_df = _apply_custom_nifty_context(long_df, long_cfg, float(case["rs_both_long"]))

    if not long_df.empty:
        long_df = r._resolve_exits_5min(
            long_df,
            dir_1m,
            suffix_1m,
            long_cfg.parquet_engine,
            eod_exit_time=r.V15_EOD_EXIT_TIME,
        )
        long_df = r._add_notional_pnl(long_df)
        long_df = r._sort_trades_for_output(long_df)

    metrics = r.compute_backtest_metrics(long_df)
    daywise_df = r._build_daily_breakdown_df(long_df, include_total=False)
    active_days = int(len(daywise_df))
    positive_days = (
        int(
            (
                pd.to_numeric(daywise_df.get("SumPnL%", 0), errors="coerce").fillna(0.0)
                > 0
            ).sum()
        )
        if active_days
        else 0
    )

    return {
        "case": case["name"],
        "trades_pre_context": pre_context_trades,
        "trades": int(metrics.total_trades),
        "active_days": active_days,
        "avg_trades_per_active_day": round(
            float(metrics.total_trades) / active_days, 4
        )
        if active_days
        else 0.0,
        "hit_rate_pct": round(float(metrics.hit_rate_pct), 4),
        "sl_rate_pct": round(float(metrics.sl_rate_pct), 4),
        "avg_pnl_pct": round(float(metrics.avg_pnl_pct), 4),
        "sum_pnl_pct": round(float(metrics.sum_pnl_pct), 4),
        "profit_factor": round(float(metrics.profit_factor), 6)
        if metrics.profit_factor != float("inf")
        else float("inf"),
        "day_win_pct": round((positive_days / active_days) * 100.0, 4)
        if active_days
        else 0.0,
        "target_count": int(metrics.target_count),
        "sl_count": int(metrics.sl_count),
        "eod_count": int(metrics.eod_count),
        "adx_min": float(case["adx"]),
        "volume_min_ratio": float(case["vol"]),
        "rsi_min_long": float(case["rsi"]),
        "quality_score_min": float(case["qmin"]),
        "signal_avwap_dist_atr_min": float(case["avmin"]),
        "rs_both_long_pct": float(case["rs_both_long"]),
        "window_desc": "; ".join(
            f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}" for a, b in case["windows"]
        ),
        "regime_source": regime_source,
        "setup_counts": str(
            long_df["setup"].value_counts().to_dict()
            if not long_df.empty and "setup" in long_df.columns
            else {}
        ),
    }


def main() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    dir_1m = r._resolve_5min_dir()
    suffix_1m = _detect_1m_suffix(dir_1m)

    rows = []
    for idx, case in enumerate(CASES, start=1):
        print(f"[{idx}/{len(CASES)}] {case['name']}", flush=True)
        row = _run_case(case, dir_1m, suffix_1m)
        rows.append(row)
        pd.DataFrame(rows).to_csv(SUMMARY_CSV, index=False)
        print(row, flush=True)

    res = pd.DataFrame(rows)
    print("\n=== SORTED BY trades desc, pf desc, sum_pnl desc ===")
    print(
        res.sort_values(
            ["trades", "profit_factor", "sum_pnl_pct"],
            ascending=[False, False, False],
        ).to_string(index=False)
    )
    print(f"\nSaved: {SUMMARY_CSV}")


if __name__ == "__main__":
    main()
