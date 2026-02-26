import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner as r2
import avwap_combined_runner_v3 as r3
from avwap_v11_refactored.avwap_common import (
    compute_backtest_metrics,
    default_long_config,
    default_short_config,
)

OUT_DIR = Path("outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _safe_pf(v: Any):
    try:
        x = float(v)
    except Exception:
        return None
    if x == float("inf"):
        return "inf"
    return x


def _build_cfg_runner_v2(min_bars: int):
    short_cfg = default_short_config(reports_dir=OUT_DIR)
    long_cfg = default_long_config(reports_dir=OUT_DIR)

    # Mirror avwap_combined_runner.main config wiring
    short_cfg.lag_bars_short_a_mod_break_c1_low = int(r2.SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW)
    short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = int(r2.SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW)
    short_cfg.lag_bars_short_b_huge_failed_bounce = int(r2.SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE)
    long_cfg.lag_bars_long_a_mod_break_c1_high = int(r2.LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH)
    long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(r2.LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH)
    long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(r2.LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)

    if r2.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        short_cfg.min_bars_left_after_entry = 0
        long_cfg.min_bars_left_after_entry = 0

    if r2.FORCE_LIVE_PARITY_DISABLE_TOPN:
        short_cfg.enable_topn_per_day = False
        long_cfg.enable_topn_per_day = False

    if r2.FINAL_SIGNAL_WINDOW_OVERRIDE:
        short_cfg.use_time_windows = bool(r2.FINAL_SHORT_USE_TIME_WINDOWS)
        long_cfg.use_time_windows = bool(r2.FINAL_LONG_USE_TIME_WINDOWS)
        short_cfg.signal_windows = list(r2.FINAL_SHORT_SIGNAL_WINDOWS)
        long_cfg.signal_windows = list(r2.FINAL_LONG_SIGNAL_WINDOWS)

    short_cfg.min_bars_for_scan = int(min_bars)
    long_cfg.min_bars_for_scan = int(min_bars)
    return short_cfg, long_cfg


def _build_cfg_runner_v3(min_bars: int):
    # Mirror avwap_combined_runner_v3.main config wiring
    short_cfg = default_short_config(reports_dir=OUT_DIR)
    long_cfg = default_long_config(
        reports_dir=OUT_DIR,
        stop_pct=float(r3.LONG_STOP_PCT_V3),
        target_pct=float(r3.LONG_TARGET_PCT_V3),
    )

    short_cfg.enable_topn_per_day = False
    long_cfg.enable_topn_per_day = False

    short_cfg.min_bars_for_scan = int(min_bars)
    long_cfg.min_bars_for_scan = int(min_bars)
    return short_cfg, long_cfg


def _case_stats(
    combined: pd.DataFrame,
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    start_capital: float,
    elapsed_sec: float,
) -> Dict[str, Any]:
    m_short = compute_backtest_metrics(short_df)
    m_long = compute_backtest_metrics(long_df)
    m_comb = compute_backtest_metrics(combined)

    net_pnl_rs = float(combined["pnl_rs"].sum()) if (not combined.empty and "pnl_rs" in combined.columns) else 0.0
    roi_pct = (net_pnl_rs / float(start_capital) * 100.0) if start_capital else 0.0

    return {
        "elapsed_sec": round(float(elapsed_sec), 2),
        "entries": {
            "short": int(len(short_df)),
            "long": int(len(long_df)),
            "combined": int(len(combined)),
        },
        "metrics": {
            "net_pnl_rs": round(float(net_pnl_rs), 2),
            "profit_pct_roi_on_start_capital": round(float(roi_pct), 6),
            "profit_factor": _safe_pf(m_comb.profit_factor),
            "sum_pnl_pct": round(float(m_comb.sum_pnl_pct), 6),
            "avg_pnl_pct": round(float(m_comb.avg_pnl_pct), 6),
            "hit_rate_pct": round(float(m_comb.hit_rate_pct), 6),
            "total_trades": int(m_comb.total_trades),
        },
        "side_metrics": {
            "short": {
                "total_trades": int(m_short.total_trades),
                "profit_factor": _safe_pf(m_short.profit_factor),
                "sum_pnl_pct": round(float(m_short.sum_pnl_pct), 6),
            },
            "long": {
                "total_trades": int(m_long.total_trades),
                "profit_factor": _safe_pf(m_long.profit_factor),
                "sum_pnl_pct": round(float(m_long.sum_pnl_pct), 6),
            },
        },
    }


def _run_one_runner_case(runner_name: str, min_bars: int) -> Dict[str, Any]:
    t0 = time.time()

    if runner_name == "v2":
        mod = r2
        short_cfg, long_cfg = _build_cfg_runner_v2(min_bars)
        run_short = True
        run_long = True
    elif runner_name == "v3":
        mod = r3
        short_cfg, long_cfg = _build_cfg_runner_v3(min_bars)
        # Enable both sides for comparable combined metrics (v3 keeps anti-chase on LONG)
        run_short = True
        run_long = True
    else:
        raise ValueError(f"Unknown runner_name={runner_name}")

    short_df = pd.DataFrame()
    long_df = pd.DataFrame()

    max_workers = int(getattr(mod, "MAX_WORKERS", 4))

    if run_short:
        short_df = mod._run_side_parallel("SHORT", short_cfg, max_workers=max_workers)
    if run_long:
        long_df = mod._run_side_parallel("LONG", long_cfg, max_workers=max_workers)

    dir_5m = mod._resolve_5min_dir()
    suffix_5m = ".parquet"

    # v3-specific LONG anti-chase model
    if runner_name == "v3" and (not long_df.empty):
        long_df, _skip_df = mod._apply_long_entry_model_v3(
            long_df=long_df,
            dir_5m=dir_5m,
            suffix_5m=suffix_5m,
            engine=long_cfg.parquet_engine,
        )

    if not short_df.empty:
        short_df = mod._resolve_exits_5min(short_df, dir_5m, suffix_5m=suffix_5m, engine=short_cfg.parquet_engine)
    if not long_df.empty:
        long_df = mod._resolve_exits_5min(long_df, dir_5m, suffix_5m=suffix_5m, engine=long_cfg.parquet_engine)

    if not short_df.empty:
        short_df = mod._add_notional_pnl(short_df)
        short_df = mod._sort_trades_for_output(short_df)
    if not long_df.empty:
        long_df = mod._add_notional_pnl(long_df)
        long_df = mod._sort_trades_for_output(long_df)

    if short_df.empty and long_df.empty:
        combined = pd.DataFrame()
    elif short_df.empty:
        combined = long_df.copy()
    elif long_df.empty:
        combined = short_df.copy()
    else:
        combined = pd.concat([short_df, long_df], ignore_index=True)

    if not combined.empty:
        combined = mod._add_notional_pnl(combined)
        combined = mod._sort_trades_for_output(combined)

    elapsed = time.time() - t0
    return _case_stats(
        combined=combined,
        short_df=short_df,
        long_df=long_df,
        start_capital=float(getattr(mod, "PORTFOLIO_START_CAPITAL_RS", r2.PORTFOLIO_START_CAPITAL_RS)),
        elapsed_sec=elapsed,
    )


def _delta(base: Dict[str, Any], alt: Dict[str, Any]) -> Dict[str, Any]:
    pf_base = base["metrics"]["profit_factor"]
    pf_alt = alt["metrics"]["profit_factor"]
    pf_delta = None
    if not isinstance(pf_base, str) and not isinstance(pf_alt, str):
        pf_delta = round(float(pf_alt) - float(pf_base), 6)

    return {
        "entries_delta": int(alt["entries"]["combined"] - base["entries"]["combined"]),
        "net_pnl_rs_delta": round(float(alt["metrics"]["net_pnl_rs"]) - float(base["metrics"]["net_pnl_rs"]), 2),
        "profit_pct_delta": round(
            float(alt["metrics"]["profit_pct_roi_on_start_capital"]) - float(base["metrics"]["profit_pct_roi_on_start_capital"]),
            6,
        ),
        "profit_factor_delta": pf_delta,
        "sum_pnl_pct_delta": round(float(alt["metrics"]["sum_pnl_pct"]) - float(base["metrics"]["sum_pnl_pct"]), 6),
    }


def run_runner_ab(runner_name: str) -> Dict[str, Any]:
    print(f"[ABTEST:{runner_name}] Running min_bars=5", flush=True)
    res5 = _run_one_runner_case(runner_name, 5)

    print(f"[ABTEST:{runner_name}] Running min_bars=3", flush=True)
    res3 = _run_one_runner_case(runner_name, 3)

    return {
        "runner": runner_name,
        "min_bars_5": res5,
        "min_bars_3": res3,
        "delta_3_minus_5": _delta(res5, res3),
    }


def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    out = {
        "generated_at": ts,
        "scope": "all tickers, all days in local 15m parquet",
        "notes": [
            "Only parameter changed per runner: min_bars_for_scan (5 vs 3)",
            "5-min exit resolution enabled",
            "v3 run here enables both SHORT and LONG for comparable combined metrics",
            "v3 LONG anti-chase model is applied",
        ],
        "results": {
            "avwap_combined_runner.py": run_runner_ab("v2"),
            "avwap_combined_runner_v3.py": run_runner_ab("v3"),
        },
    }

    out_json = OUT_DIR / f"min_bars_scan_abtest_v2_v3_{ts}.json"
    out_json.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"[ABTEST] Saved: {out_json}", flush=True)
    print(json.dumps(out, indent=2), flush=True)


if __name__ == "__main__":
    main()
