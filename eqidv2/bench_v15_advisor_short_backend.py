from __future__ import annotations

import importlib
import json
import math
from dataclasses import dataclass
from datetime import date, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from eqidv2_runtime_paths import report_subdir, runtime_dir


END_DATE = date(2026, 3, 26)
LOOKBACK_DAYS = 20
OUTPUT_DIR = report_subdir("codex_post_trade_advisor_v15_new_experiments")
SHORT_SETUPS = [
    "A_MOD_BREAK_C1_LOW",
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
]


@dataclass(frozen=True)
class Variant:
    name: str
    description: str
    pullback_lag: Optional[int] = None
    small_counter_max_atr: Optional[float] = None
    signal_avwap_dist_atr_max: Optional[float] = None
    entry_cutoff: Optional[dtime] = None
    min_opening_range_width_pct: Optional[float] = None
    adx_min: Optional[float] = None


VARIANTS: List[Variant] = [
    Variant(
        name="baseline",
        description="Current v15 backtest short profile with no advisor tweaks.",
    ),
    Variant(
        name="advisor_core",
        description="Core weak-pullback cleanup: tighter C2 size, tighter AVWAP distance, faster lag.",
        pullback_lag=1,
        small_counter_max_atr=0.15,
        signal_avwap_dist_atr_max=1.60,
    ),
    Variant(
        name="advisor_full",
        description="Full backtestable advisor package for short side, excluding live-only slip cap.",
        pullback_lag=1,
        small_counter_max_atr=0.15,
        signal_avwap_dist_atr_max=1.60,
        entry_cutoff=dtime(12, 30, 0),
        min_opening_range_width_pct=1.25,
    ),
    Variant(
        name="advisor_full_plus_adx",
        description="Full advisor package plus slightly stricter ADX floor on shorts.",
        pullback_lag=1,
        small_counter_max_atr=0.15,
        signal_avwap_dist_atr_max=1.60,
        entry_cutoff=dtime(12, 30, 0),
        min_opening_range_width_pct=1.25,
        adx_min=18.5,
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
    short_all = work[work["side"].astype(str).str.upper() == "SHORT"].copy()
    recent_mask = work["trade_date"].isin(set(analysis_dates))
    short_recent = short_all[short_all["trade_date"].isin(set(analysis_dates))].copy()

    result = {
        "short_all_days": _compute_metrics(short_all),
        "short_recent_window": _compute_metrics(short_recent),
        "combined_all_days": _compute_metrics(work),
        "combined_recent_window": _compute_metrics(work[recent_mask].copy()),
    }
    for setup in SHORT_SETUPS:
        result[f"setup_all::{setup}"] = _compute_metrics(short_all[short_all["setup"] == setup].copy())
        result[f"setup_recent::{setup}"] = _compute_metrics(short_recent[short_recent["setup"] == setup].copy())
    return result


def _latest_new_file(root: Path, pattern: str, before: set[Path]) -> Path:
    candidates = [p for p in root.glob(pattern) if p not in before]
    if not candidates:
        raise FileNotFoundError(f"No new file created for pattern {pattern}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _run_variant(variant: Variant, analysis_dates: List[date]) -> Dict[str, Any]:
    import codex_post_trade_advisor_v15_new as advisor
    import avwap_combined_runner_v15 as runner

    advisor = importlib.reload(advisor)
    runner = importlib.reload(runner)

    outputs_root = runtime_dir("outputs_v15")
    before_trades = set(outputs_root.glob("avwap_longshort_trades_v15_ALL_DAYS_*.csv"))
    before_daywise = set(outputs_root.glob("avwap_daywise_breakdown_v15_ALL_DAYS_*.csv"))

    base_default_short_config = runner.default_short_config

    def _patched_default_short_config(*args, **kwargs):
        cfg = base_default_short_config(*args, **kwargs)
        if variant.small_counter_max_atr is not None:
            cfg.small_counter_max_atr = float(variant.small_counter_max_atr)
        if variant.adx_min is not None:
            cfg.adx_min = float(variant.adx_min)
        return cfg

    runner.default_short_config = _patched_default_short_config
    if variant.pullback_lag is not None:
        runner.SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW = int(variant.pullback_lag)
    if variant.signal_avwap_dist_atr_max is not None:
        runner.V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = float(variant.signal_avwap_dist_atr_max)
    if variant.entry_cutoff is not None:
        runner.V15_SHORT_ENTRY_CUTOFF = variant.entry_cutoff
    if variant.min_opening_range_width_pct is not None:
        runner.V15_SHORT_MIN_OPENING_RANGE_WIDTH_PCT = float(variant.min_opening_range_width_pct)

    runner.main()

    trades_csv = _latest_new_file(outputs_root, "avwap_longshort_trades_v15_ALL_DAYS_*.csv", before_trades)
    daywise_csv = _latest_new_file(outputs_root, "avwap_daywise_breakdown_v15_ALL_DAYS_*.csv", before_daywise)
    trades = pd.read_csv(trades_csv)
    metrics = _extract_scope_metrics(trades, analysis_dates)
    return {
        "variant": variant.name,
        "description": variant.description,
        "changes": {
            "pullback_lag": variant.pullback_lag,
            "small_counter_max_atr": variant.small_counter_max_atr,
            "signal_avwap_dist_atr_max": variant.signal_avwap_dist_atr_max,
            "entry_cutoff": variant.entry_cutoff.strftime("%H:%M") if variant.entry_cutoff else None,
            "min_opening_range_width_pct": variant.min_opening_range_width_pct,
            "adx_min": variant.adx_min,
            "live_only_not_tested": ["SHORT_MAX_ENTRY_SLIP_PCT"],
        },
        "trades_csv": trades_csv,
        "daywise_csv": daywise_csv,
        "metrics": metrics,
    }


def main() -> None:
    import codex_post_trade_advisor_v15_new as advisor

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    analysis_dates = advisor._choose_analysis_dates(END_DATE, LOOKBACK_DAYS)
    results = [_run_variant(variant, analysis_dates) for variant in VARIANTS]

    summary_rows: List[Dict[str, Any]] = []
    for result in results:
        recent = result["metrics"]["short_recent_window"]
        overall = result["metrics"]["short_all_days"]
        summary_rows.append(
            {
                "variant": result["variant"],
                "description": result["description"],
                "recent_trades": recent["trades"],
                "recent_win_rate_pct": recent["win_rate_pct"],
                "recent_day_win_rate_pct": recent["day_win_rate_pct"],
                "recent_profit_factor": recent["profit_factor"],
                "recent_sum_pnl_rs": recent["sum_pnl_rs"],
                "recent_sum_pnl_pct": recent["sum_pnl_pct"],
                "recent_avg_trades_per_active_day": recent["avg_trades_per_active_day"],
                "overall_trades": overall["trades"],
                "overall_win_rate_pct": overall["win_rate_pct"],
                "overall_day_win_rate_pct": overall["day_win_rate_pct"],
                "overall_profit_factor": overall["profit_factor"],
                "overall_sum_pnl_rs": overall["sum_pnl_rs"],
                "overall_sum_pnl_pct": overall["sum_pnl_pct"],
                "overall_avg_trades_per_active_day": overall["avg_trades_per_active_day"],
                "trades_csv": result["trades_csv"],
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = OUTPUT_DIR / f"advisor_short_backend_summary_{END_DATE.isoformat()}.csv"
    summary_json = OUTPUT_DIR / f"advisor_short_backend_summary_{END_DATE.isoformat()}.json"
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

    print(f"[ADVISOR-BENCH] analysis_dates={','.join(d.isoformat() for d in analysis_dates)}", flush=True)
    print(f"[ADVISOR-BENCH] summary_csv={summary_csv}", flush=True)
    print(f"[ADVISOR-BENCH] summary_json={summary_json}", flush=True)
    print(summary_df.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
