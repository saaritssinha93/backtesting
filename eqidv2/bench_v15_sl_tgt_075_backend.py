from __future__ import annotations

import importlib
import json
import math
from dataclasses import dataclass
from datetime import date, time as dtime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from eqidv2_runtime_paths import report_subdir, runtime_dir


END_DATE = date(2026, 3, 27)
LOOKBACK_DAYS = 20
OUTPUT_DIR = report_subdir("v15_sl_tgt_075_backend")


@dataclass(frozen=True)
class Variant:
    name: str
    description: str
    force_equal_sl_tgt_075: bool = False


VARIANTS = [
    Variant(
        name="baseline",
        description="Current v15 backtesting profile.",
        force_equal_sl_tgt_075=False,
    ),
    Variant(
        name="equal_075",
        description="Backend-only v15 profile with SHORT/LONG stop and target both forced to 0.75%.",
        force_equal_sl_tgt_075=True,
    ),
]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return float(default)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isinf(value):
            return "inf"
        return None
    return value


def _profit_factor(pnl: pd.Series) -> float:
    gross_profit = pnl[pnl > 0].sum()
    gross_loss = -pnl[pnl < 0].sum()
    if gross_profit <= 0 and gross_loss <= 0:
        return 0.0
    if gross_loss <= 0:
        return float("inf")
    return float(gross_profit / gross_loss)


def _max_drawdown_from_daily_pnl(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = pnl.cumsum()
    running_peak = equity.cummax()
    drawdown = equity - running_peak
    return float(drawdown.min())


def _compute_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "trades": 0,
            "active_days": 0,
            "avg_trades_per_active_day": 0.0,
            "max_trades_in_day": 0,
            "win_rate_pct": 0.0,
            "day_win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "sum_pnl_rs": 0.0,
            "sum_pnl_pct": 0.0,
            "avg_pnl_pct": 0.0,
            "max_drawdown_rs": 0.0,
        }

    work = df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce").dt.date
    pnl_rs = pd.to_numeric(work["pnl_rs"], errors="coerce").fillna(0.0)
    pnl_pct = pd.to_numeric(work["pnl_pct"], errors="coerce").fillna(0.0)
    day_pnl = work.groupby("trade_date", dropna=True)["pnl_rs"].sum()
    day_counts = work.groupby("trade_date", dropna=True).size()

    trades = int(len(work))
    active_days = int(len(day_pnl))
    wins = int((pnl_rs > 0).sum())
    return {
        "trades": trades,
        "active_days": active_days,
        "avg_trades_per_active_day": round(trades / active_days, 2) if active_days else 0.0,
        "max_trades_in_day": int(day_counts.max()) if not day_counts.empty else 0,
        "win_rate_pct": round((wins / trades) * 100.0, 2) if trades else 0.0,
        "day_win_rate_pct": round(((day_pnl > 0).sum() / active_days) * 100.0, 2) if active_days else 0.0,
        "profit_factor": round(_profit_factor(pnl_rs), 4),
        "sum_pnl_rs": round(_safe_float(pnl_rs.sum()), 2),
        "sum_pnl_pct": round(_safe_float(pnl_pct.sum()), 4),
        "avg_pnl_pct": round(_safe_float(pnl_pct.mean()), 4),
        "max_drawdown_rs": round(_max_drawdown_from_daily_pnl(day_pnl), 2),
    }


def _extract_scope_metrics(trades: pd.DataFrame, analysis_dates: List[date]) -> Dict[str, Dict[str, Any]]:
    work = trades.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce").dt.date
    recent_dates = set(analysis_dates)

    short_all = work[work["side"].astype(str).str.upper() == "SHORT"].copy()
    long_all = work[work["side"].astype(str).str.upper() == "LONG"].copy()
    combined_recent = work[work["trade_date"].isin(recent_dates)].copy()
    short_recent = short_all[short_all["trade_date"].isin(recent_dates)].copy()
    long_recent = long_all[long_all["trade_date"].isin(recent_dates)].copy()

    return {
        "short_all_days": _compute_metrics(short_all),
        "short_recent_window": _compute_metrics(short_recent),
        "long_all_days": _compute_metrics(long_all),
        "long_recent_window": _compute_metrics(long_recent),
        "combined_all_days": _compute_metrics(work),
        "combined_recent_window": _compute_metrics(combined_recent),
    }


def _latest_new_file(root: Path, pattern: str, before: set[Path]) -> Path:
    candidates = [p for p in root.glob(pattern) if p not in before]
    if not candidates:
        raise FileNotFoundError(f"No new file created for pattern {pattern}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _apply_current_v15_short_profile(cfg) -> None:
    cfg.enable_liquidity_sweep_filter = False
    cfg.reversal_requires_sweep = True
    cfg.enable_avwap_no_trade_zone = False
    cfg.enable_mode_selector = True
    cfg.use_prev_close_for_day_mode = False
    cfg.use_time_windows = False
    cfg.min_bars_left_after_entry = 0
    cfg.enable_ema200_filter = False
    cfg.require_vwap_side_persistence = False
    cfg.vwap_side_lookback_bars = 5
    cfg.vwap_side_min_count = 3
    cfg.require_structure_filter = False
    cfg.structure_lookback_bars = 30
    cfg.adx_min = 17.0
    cfg.adx_slope_min = 0.40
    cfg.volume_min_ratio = 0.95
    cfg.rsi_max_short = 62.0
    cfg.stochk_max = 90.0
    cfg.stop_pct = 0.0084
    cfg.target_pct = 0.0110
    cfg.be_trigger_pct = 0.0042
    cfg.trail_pct = 0.0023
    cfg.enable_partial_exit = True
    cfg.partial_exit_fraction = 0.50
    cfg.partial_target_fraction = 0.50
    cfg.enable_risk_based_position_sizing = False
    cfg.risk_per_trade_pct_of_capital = 0.0035
    cfg.max_trades_per_ticker_per_day = 5
    cfg.enable_topn_per_day = False
    cfg.topn_per_day = 0
    cfg.entry_time_cutoff = dtime(13, 15, 0)
    cfg.min_opening_range_width_pct = 1.00
    cfg.signal_avwap_dist_atr_max = 2.10


def _apply_current_v15_long_profile(cfg) -> None:
    cfg.require_entry_close_confirm = True
    cfg.enable_liquidity_sweep_filter = False
    cfg.enable_avwap_no_trade_zone = False
    cfg.adx_min = 20.0
    cfg.adx_slope_min = 0.50
    cfg.volume_min_ratio = 0.95
    cfg.rsi_min_long = 55.0
    cfg.stochk_min = 15.0
    cfg.stochk_max = 95.0
    cfg.atr_pct_min = 0.0025
    cfg.stop_pct = 0.0077
    cfg.target_pct = 0.0110
    cfg.be_trigger_pct = 0.0055
    cfg.trail_pct = 0.0028
    cfg.min_bars_left_after_entry = 0
    cfg.max_vix_for_entries = 13.0
    cfg.max_trades_per_ticker_per_day = 4
    cfg.enable_topn_per_day = False
    cfg.topn_per_day = 0
    cfg.signal_avwap_dist_atr_min = 0.5
    cfg.quality_score_min = 4.0


def _run_variant(variant: Variant, analysis_dates: List[date]) -> Dict[str, Any]:
    import codex_post_trade_advisor_v15_new as advisor
    import avwap_combined_runner_v15 as runner

    advisor = importlib.reload(advisor)
    runner = importlib.reload(runner)

    outputs_root = runtime_dir("outputs_v15")
    before_trades = set(outputs_root.glob("avwap_longshort_trades_v15_ALL_DAYS_*.csv"))
    before_daywise = set(outputs_root.glob("avwap_daywise_breakdown_v15_ALL_DAYS_*.csv"))

    if variant.force_equal_sl_tgt_075:
        base_default_short_config = runner.default_short_config
        base_default_long_config = runner.default_long_config_v9

        def _patched_default_short_config(*args, **kwargs):
            cfg = base_default_short_config(*args, **kwargs)
            _apply_current_v15_short_profile(cfg)
            cfg.stop_pct = 0.0075
            cfg.target_pct = 0.0075
            return cfg

        def _patched_default_long_config(*args, **kwargs):
            cfg = base_default_long_config(*args, **kwargs)
            _apply_current_v15_long_profile(cfg)
            cfg.stop_pct = 0.0075
            cfg.target_pct = 0.0075
            return cfg

        runner.default_short_config = _patched_default_short_config
        runner.default_long_config_v9 = _patched_default_long_config
        runner.ENABLE_PLAYBOOK_V11_PROFILE = False
        runner.TEST_TARGET_OVERRIDE = True
        runner.TEST_SHORT_TARGET_PCT = 0.0075
        runner.TEST_LONG_TARGET_PCT = 0.0075

    runner.main()

    trades_csv = _latest_new_file(outputs_root, "avwap_longshort_trades_v15_ALL_DAYS_*.csv", before_trades)
    daywise_csv = _latest_new_file(outputs_root, "avwap_daywise_breakdown_v15_ALL_DAYS_*.csv", before_daywise)
    trades = pd.read_csv(trades_csv)
    metrics = _extract_scope_metrics(trades, analysis_dates)
    return {
        "variant": variant.name,
        "description": variant.description,
        "changes": {
            "short_stop_pct": 0.0075 if variant.force_equal_sl_tgt_075 else 0.0084,
            "short_target_pct": 0.0075 if variant.force_equal_sl_tgt_075 else 0.0090,
            "long_stop_pct": 0.0075 if variant.force_equal_sl_tgt_075 else 0.0077,
            "long_target_pct": 0.0075 if variant.force_equal_sl_tgt_075 else 0.0090,
        },
        "trades_csv": trades_csv,
        "daywise_csv": daywise_csv,
        "metrics": metrics,
    }


def _delta_metrics(base: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in current.items():
        if isinstance(value, (int, float)) and isinstance(base.get(key), (int, float)):
            out[f"{key}_delta"] = round(float(value) - float(base[key]), 4)
    return out


def main() -> None:
    import codex_post_trade_advisor_v15_new as advisor

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    analysis_dates = advisor._choose_analysis_dates(END_DATE, LOOKBACK_DAYS)
    results = [_run_variant(variant, analysis_dates) for variant in VARIANTS]

    baseline = next(result for result in results if result["variant"] == "baseline")
    baseline_metrics = baseline["metrics"]

    summary_rows: List[Dict[str, Any]] = []
    for result in results:
        row: Dict[str, Any] = {
            "variant": result["variant"],
            "description": result["description"],
            "trades_csv": result["trades_csv"],
        }
        for scope in [
            "short_recent_window",
            "short_all_days",
            "long_recent_window",
            "long_all_days",
            "combined_recent_window",
            "combined_all_days",
        ]:
            metrics = result["metrics"][scope]
            for key, value in metrics.items():
                row[f"{scope}__{key}"] = value
            delta = _delta_metrics(baseline_metrics[scope], metrics)
            for key, value in delta.items():
                row[f"{scope}__{key}"] = value
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = OUTPUT_DIR / f"v15_equal_075_summary_{END_DATE.isoformat()}.csv"
    summary_json = OUTPUT_DIR / f"v15_equal_075_summary_{END_DATE.isoformat()}.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_json.write_text(
        json.dumps(
            _json_ready(
                {
                    "end_date": END_DATE,
                    "analysis_dates": analysis_dates,
                    "variants": results,
                }
            ),
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"[V15-075] analysis_dates={','.join(d.isoformat() for d in analysis_dates)}", flush=True)
    print(f"[V15-075] summary_csv={summary_csv}", flush=True)
    print(f"[V15-075] summary_json={summary_json}", flush=True)
    print(summary_df.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
