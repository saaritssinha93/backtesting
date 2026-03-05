import os
import sys
import time
import json
from dataclasses import asdict
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

ROOT = Path(r"C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2")
sys.path.insert(0, str(ROOT))

import avwap_combined_runner_v6_sweep as r
from avwap_v11_refactored.avwap_common_v6_sweep import (
    default_short_config,
    default_long_config,
    compute_backtest_metrics,
)

OUTDIR = ROOT / "outputs_v6_sweep_analysis"
OUTDIR.mkdir(parents=True, exist_ok=True)

VIX_MAP = r._load_india_vix(ROOT)


def apply_runner_baseline(short_cfg, long_cfg):
    short_cfg.lag_bars_short_a_mod_break_c1_low = int(r.SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW)
    short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = int(r.SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW)
    short_cfg.lag_bars_short_b_huge_failed_bounce = int(r.SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE)
    long_cfg.lag_bars_long_a_mod_break_c1_high = int(r.LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH)
    long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(r.LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH)
    long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(r.LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)

    if r.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        short_cfg.min_bars_left_after_entry = 0
        long_cfg.min_bars_left_after_entry = 0

    if r.FORCE_LIVE_PARITY_DISABLE_TOPN:
        short_cfg.enable_topn_per_day = False
        long_cfg.enable_topn_per_day = False

    if r.FINAL_SIGNAL_WINDOW_OVERRIDE:
        short_cfg.use_time_windows = bool(r.FINAL_SHORT_USE_TIME_WINDOWS)
        long_cfg.use_time_windows = bool(r.FINAL_LONG_USE_TIME_WINDOWS)
        short_cfg.signal_windows = list(r.FINAL_SHORT_SIGNAL_WINDOWS)
        long_cfg.signal_windows = list(r.FINAL_LONG_SIGNAL_WINDOWS)


def apply_vix(short_cfg, long_cfg, *, enabled, scale_target=True, scale_sl=True, baseline=18.0, vmin=0.70, vmax=1.80):
    short_cfg.vix_scale_enabled = bool(enabled)
    long_cfg.vix_scale_enabled = bool(enabled)
    if enabled and VIX_MAP:
        for cfg in (short_cfg, long_cfg):
            cfg.vix_daily = VIX_MAP
            cfg.vix_baseline = float(baseline)
            cfg.vix_scale_min = float(vmin)
            cfg.vix_scale_max = float(vmax)
            cfg.vix_scale_target = bool(scale_target)
            cfg.vix_scale_sl = bool(scale_sl)


def eval_one(name, p, max_workers=4):
    t0 = time.time()

    short_cfg = default_short_config(reports_dir=OUTDIR)
    long_cfg = default_long_config(reports_dir=OUTDIR)
    apply_runner_baseline(short_cfg, long_cfg)

    # core overrides
    short_cfg.stop_pct = float(p.get("short_stop", short_cfg.stop_pct))
    short_cfg.target_pct = float(p.get("short_target", short_cfg.target_pct))
    long_cfg.stop_pct = float(p.get("long_stop", long_cfg.stop_pct))
    long_cfg.target_pct = float(p.get("long_target", long_cfg.target_pct))

    short_cfg.be_trigger_pct = float(p.get("short_be", short_cfg.be_trigger_pct))
    short_cfg.trail_pct = float(p.get("short_trail", short_cfg.trail_pct))
    long_cfg.be_trigger_pct = float(p.get("long_be", long_cfg.be_trigger_pct))
    long_cfg.trail_pct = float(p.get("long_trail", long_cfg.trail_pct))

    short_cfg.adx_min = float(p.get("short_adx_min", short_cfg.adx_min))
    long_cfg.adx_min = float(p.get("long_adx_min", long_cfg.adx_min))
    short_cfg.adx_slope_min = float(p.get("short_adx_slope_min", short_cfg.adx_slope_min))
    long_cfg.adx_slope_min = float(p.get("long_adx_slope_min", long_cfg.adx_slope_min))

    short_cfg.volume_min_ratio = float(p.get("volume_min_ratio", short_cfg.volume_min_ratio))
    long_cfg.volume_min_ratio = float(p.get("volume_min_ratio", long_cfg.volume_min_ratio))
    short_cfg.atr_pct_min = float(p.get("atr_pct_min", short_cfg.atr_pct_min))
    long_cfg.atr_pct_min = float(p.get("atr_pct_min", long_cfg.atr_pct_min))

    if "window" in p:
        short_cfg.signal_windows = [p["window"]]
        long_cfg.signal_windows = [p["window"]]
        short_cfg.use_time_windows = True
        long_cfg.use_time_windows = True

    # optional setup toggle
    if "long_enable_pullback" in p:
        long_cfg.enable_setup_a_pullback_c2_break = bool(p["long_enable_pullback"])

    # optional topn enable
    if "enable_topn" in p:
        short_cfg.enable_topn_per_day = bool(p["enable_topn"])
        long_cfg.enable_topn_per_day = bool(p["enable_topn"])
    if "topn" in p:
        short_cfg.topn_per_day = int(p["topn"])
        long_cfg.topn_per_day = int(p["topn"])

    apply_vix(
        short_cfg,
        long_cfg,
        enabled=bool(p.get("vix_enabled", True)),
        scale_target=bool(p.get("vix_scale_target", True)),
        scale_sl=bool(p.get("vix_scale_sl", True)),
        baseline=float(p.get("vix_baseline", 18.0)),
        vmin=float(p.get("vix_min", 0.70)),
        vmax=float(p.get("vix_max", 1.80)),
    )

    short_df = r._run_side_parallel("SHORT", short_cfg, max_workers=max_workers)
    long_df = r._run_side_parallel("LONG", long_cfg, max_workers=max_workers)

    dir_1m = r._resolve_5min_dir()
    suffix = ".parquet"
    if dir_1m.is_dir():
        sample = list(dir_1m.glob("*"))[:5]
        for sf in sample:
            if sf.suffix:
                suffix = sf.suffix
                break

    if not short_df.empty:
        short_df = r._resolve_exits_5min(short_df, dir_1m, suffix, short_cfg.parquet_engine)
        short_df = r._add_notional_pnl(short_df)
        short_df = r._sort_trades_for_output(short_df)
    if not long_df.empty:
        long_df = r._resolve_exits_5min(long_df, dir_1m, suffix, long_cfg.parquet_engine)
        long_df = r._add_notional_pnl(long_df)
        long_df = r._sort_trades_for_output(long_df)

    combined = pd.concat([short_df, long_df], ignore_index=True)
    if not combined.empty:
        combined = r._add_notional_pnl(combined)
        combined = r._sort_trades_for_output(combined)

    m = compute_backtest_metrics(combined)

    daily_win_pct = 0.0
    daily_win_pct_2w = 0.0
    total_pnl_rs = 0.0
    trades_per_day = 0.0
    days = 0
    pnl_2w = 0.0
    if not combined.empty and "trade_date" in combined.columns:
        dsum = combined.groupby("trade_date", as_index=False)["pnl_rs"].sum()
        days = int(dsum["trade_date"].nunique())
        daily_win_pct = float((dsum["pnl_rs"] > 0).mean() * 100.0)
        total_pnl_rs = float(combined["pnl_rs"].sum())
        trades_per_day = float(len(combined) / max(1, days))

        dsum["trade_date"] = pd.to_datetime(dsum["trade_date"], errors="coerce")
        if dsum["trade_date"].notna().any():
            cutoff = dsum["trade_date"].max() - pd.Timedelta(days=14)
            d2 = dsum[dsum["trade_date"] >= cutoff]
            if len(d2):
                daily_win_pct_2w = float((d2["pnl_rs"] > 0).mean() * 100.0)
                pnl_2w = float(d2["pnl_rs"].sum())

    runtime = time.time() - t0
    row = {
        "name": name,
        "trades": int(len(combined)),
        "days": days,
        "trades_per_day": trades_per_day,
        "pf": float(m.profit_factor),
        "sum_pnl_pct": float(m.sum_pnl_pct),
        "avg_pnl_pct": float(m.avg_pnl_pct),
        "max_dd_pct": float(m.max_drawdown_pct),
        "target_hit_pct": float(m.hit_rate_pct),
        "daily_win_pct": daily_win_pct,
        "daily_win_pct_2w": daily_win_pct_2w,
        "total_pnl_rs": total_pnl_rs,
        "pnl_rs_2w": pnl_2w,
        "runtime_sec": runtime,
        "params": json.dumps(p, sort_keys=True),
    }
    return row


def score_row(rw):
    # Higher better for PF, pnl, win; lower better for DD.
    pf = min(float(rw["pf"]), 5.0)
    pnl = float(rw["sum_pnl_pct"])
    dd = float(rw["max_dd_pct"])
    win = float(rw["daily_win_pct"])
    tpd = float(rw["trades_per_day"])

    # soft target around 8-12 trades/day
    tpd_pen = abs(tpd - 10.0)
    return 2.2 * pf + 0.9 * pnl + 0.5 * (win / 10.0) - 0.7 * dd - 0.15 * tpd_pen


def main():
    all_tickers = r.list_tickers_15m('stocks_indicators_15min_eq', '_stocks_indicators_15min.parquet')
    subset = all_tickers[:300]

    base_list_tickers = r.list_tickers_15m

    configs = [
        ("baseline_v6", {}),
        ("v5_targets", {"short_target": 0.009, "long_target": 0.011}),
        ("pf_focus_1", {"short_stop": 0.0065, "short_target": 0.0100, "long_stop": 0.0065, "long_target": 0.0120}),
        ("pf_focus_2", {"short_stop": 0.0070, "short_target": 0.0105, "long_stop": 0.0065, "long_target": 0.0125}),
        ("strict_filters", {"short_target":0.0095, "long_target":0.0120, "short_adx_min":28, "long_adx_min":28, "volume_min_ratio":1.35, "atr_pct_min":0.0025}),
        ("tight_window", {"short_target":0.0095, "long_target":0.0115, "window": (datetime.strptime('09:30','%H:%M').time(), datetime.strptime('13:30','%H:%M').time())}),
        ("no_vix", {"vix_enabled": False, "short_target":0.0095, "long_target":0.0115}),
        ("vix_target_only", {"vix_enabled": True, "vix_scale_target": True, "vix_scale_sl": False, "short_target":0.0095, "long_target":0.0115}),
        ("vix_sl_only", {"vix_enabled": True, "vix_scale_target": False, "vix_scale_sl": True, "short_target":0.0095, "long_target":0.0115}),
        ("be_trail_fast", {"short_target":0.0095, "long_target":0.0115, "short_be":0.0040, "long_be":0.0045, "short_trail":0.0025, "long_trail":0.0025}),
    ]

    rows_coarse = []
    print("[PHASE A] Coarse scan on 300 tickers...")
    r.list_tickers_15m = lambda d, e: subset
    try:
        for i, (name, params) in enumerate(configs, 1):
            print(f"\n[{i}/{len(configs)}] {name}")
            row = eval_one(name, params, max_workers=4)
            row["phase"] = "coarse"
            row["score"] = score_row(row)
            rows_coarse.append(row)
            print({k: row[k] for k in ["name","trades","trades_per_day","pf","sum_pnl_pct","max_dd_pct","daily_win_pct","total_pnl_rs","score","runtime_sec"]})
    finally:
        r.list_tickers_15m = base_list_tickers

    coarse_df = pd.DataFrame(rows_coarse).sort_values("score", ascending=False)
    coarse_csv = OUTDIR / f"v6_sweep_coarse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    coarse_df.to_csv(coarse_csv, index=False)
    print(f"[SAVE] {coarse_csv}")

    top_names = coarse_df.head(4)["name"].tolist()
    top_params = {n: p for n, p in configs if n in top_names}

    print("\n[PHASE B] Full-universe validation on top configs:", top_names)
    rows_full = []
    for i, name in enumerate(top_names, 1):
        print(f"\n[full {i}/{len(top_names)}] {name}")
        row = eval_one(name, top_params[name], max_workers=4)
        row["phase"] = "full"
        row["score"] = score_row(row)
        rows_full.append(row)
        print({k: row[k] for k in ["name","trades","trades_per_day","pf","sum_pnl_pct","max_dd_pct","daily_win_pct","daily_win_pct_2w","total_pnl_rs","pnl_rs_2w","score","runtime_sec"]})

    full_df = pd.DataFrame(rows_full).sort_values("score", ascending=False)
    full_csv = OUTDIR / f"v6_sweep_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    full_df.to_csv(full_csv, index=False)
    print(f"[SAVE] {full_csv}")

    print("\n[TOP FULL RESULTS]")
    print(full_df[["name","trades","trades_per_day","pf","sum_pnl_pct","max_dd_pct","daily_win_pct","daily_win_pct_2w","total_pnl_rs","pnl_rs_2w","score"]].to_string(index=False))


if __name__ == "__main__":
    main()
