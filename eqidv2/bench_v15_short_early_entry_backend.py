from __future__ import annotations

import importlib
import json
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from eqidv2_runtime_paths import report_subdir, runtime_dir


END_DATE = date(2026, 3, 27)
LOOKBACK_DAYS = 20
OUTPUT_DIR = report_subdir("v15_short_early_entry_backend")
WATCH_TICKERS = ("HAL", "MAZDOCK")


@dataclass(frozen=True)
class Variant:
    name: str
    description: str
    mod_impulse_min_atr: Optional[float] = None
    close_near_extreme_max: Optional[float] = None
    small_counter_max_atr: Optional[float] = None
    buffer_abs: Optional[float] = None
    buffer_pct: Optional[float] = None
    require_entry_close_confirm: Optional[bool] = None
    avwap_dist_atr_mult: Optional[float] = None
    signal_avwap_dist_atr_max: Optional[float] = None
    require_avwap_rule: Optional[bool] = None
    volume_min_ratio: Optional[float] = None


VARIANTS: List[Variant] = [
    Variant(
        name="baseline",
        description="Current v15 short profile with no earlier-entry overrides.",
    ),
    Variant(
        name="early_mild",
        description="Smaller trigger buffer + no close-confirm + lighter AVWAP distance gate.",
        buffer_abs=0.02,
        buffer_pct=0.00010,
        require_entry_close_confirm=False,
        avwap_dist_atr_mult=0.15,
    ),
    Variant(
        name="early_structural",
        description="Earlier A_MOD recognition via looser impulse shape and slightly lighter volume gate.",
        mod_impulse_min_atr=0.35,
        close_near_extreme_max=0.35,
        small_counter_max_atr=0.25,
        volume_min_ratio=0.90,
        avwap_dist_atr_mult=0.15,
    ),
    Variant(
        name="early_combo",
        description="Mild + structural package, still keeping AVWAP rule on.",
        mod_impulse_min_atr=0.35,
        close_near_extreme_max=0.35,
        small_counter_max_atr=0.25,
        buffer_abs=0.02,
        buffer_pct=0.00010,
        require_entry_close_confirm=False,
        avwap_dist_atr_mult=0.15,
        signal_avwap_dist_atr_max=2.50,
        volume_min_ratio=0.90,
    ),
    Variant(
        name="early_upper_bound_no_avwap",
        description="Upper-bound aggressive short timing test with AVWAP rule disabled.",
        mod_impulse_min_atr=0.35,
        close_near_extreme_max=0.35,
        small_counter_max_atr=0.25,
        buffer_abs=0.02,
        buffer_pct=0.00010,
        require_entry_close_confirm=False,
        avwap_dist_atr_mult=0.10,
        signal_avwap_dist_atr_max=2.80,
        require_avwap_rule=False,
        volume_min_ratio=0.90,
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


def _latest_new_file(root: Path, pattern: str, before: set[Path]) -> Path:
    candidates = [p for p in root.glob(pattern) if p not in before]
    if not candidates:
        raise FileNotFoundError(f"No new file created for pattern {pattern}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _extract_scope_metrics(trades: pd.DataFrame, analysis_dates: List[date]) -> Dict[str, Dict[str, Any]]:
    work = trades.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce").dt.date
    short_all = work[work["side"].astype(str).str.upper() == "SHORT"].copy()
    recent_dates = set(analysis_dates)
    short_recent = short_all[short_all["trade_date"].isin(recent_dates)].copy()
    amod_all = short_all[short_all["setup"] == "A_MOD_BREAK_C1_LOW"].copy()
    amod_recent = amod_all[amod_all["trade_date"].isin(recent_dates)].copy()

    return {
        "short_all_days": _compute_metrics(short_all),
        "short_recent_window": _compute_metrics(short_recent),
        "a_mod_all_days": _compute_metrics(amod_all),
        "a_mod_recent_window": _compute_metrics(amod_recent),
    }


def _today_watch_summary(trades: pd.DataFrame) -> Dict[str, Any]:
    if trades.empty:
        return {}
    work = trades.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce").dt.date
    work["entry_time_ist"] = pd.to_datetime(work["entry_time_ist"], errors="coerce")
    today = END_DATE
    out: Dict[str, Any] = {}
    for ticker in WATCH_TICKERS:
        subset = work[
            (work["side"].astype(str).str.upper() == "SHORT")
            & (work["trade_date"] == today)
            & (work["ticker"].astype(str).str.upper() == ticker)
        ].copy()
        if subset.empty:
            out[ticker] = {"entries": 0}
            continue
        subset = subset.sort_values("entry_time_ist")
        first_row = subset.iloc[0]
        out[ticker] = {
            "entries": int(len(subset)),
            "first_entry_time": pd.Timestamp(first_row["entry_time_ist"]).strftime("%H:%M"),
            "first_setup": str(first_row["setup"]),
            "sum_pnl_pct": round(_safe_float(pd.to_numeric(subset["pnl_pct"], errors="coerce").sum()), 4),
            "sum_pnl_rs": round(_safe_float(pd.to_numeric(subset["pnl_rs"], errors="coerce").sum()), 2),
        }
    return out


def _delta_metrics(base: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in current.items():
        if isinstance(value, (int, float)) and isinstance(base.get(key), (int, float)):
            out[f"{key}_delta"] = round(float(value) - float(base[key]), 4)
    return out


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
        if variant.mod_impulse_min_atr is not None:
            cfg.mod_impulse_min_atr = float(variant.mod_impulse_min_atr)
        if variant.close_near_extreme_max is not None:
            cfg.close_near_extreme_max = float(variant.close_near_extreme_max)
        if variant.small_counter_max_atr is not None:
            cfg.small_counter_max_atr = float(variant.small_counter_max_atr)
        if variant.buffer_abs is not None:
            cfg.buffer_abs = float(variant.buffer_abs)
        if variant.buffer_pct is not None:
            cfg.buffer_pct = float(variant.buffer_pct)
        if variant.require_entry_close_confirm is not None:
            cfg.require_entry_close_confirm = bool(variant.require_entry_close_confirm)
        if variant.avwap_dist_atr_mult is not None:
            cfg.avwap_dist_atr_mult = float(variant.avwap_dist_atr_mult)
        if variant.require_avwap_rule is not None:
            cfg.require_avwap_rule = bool(variant.require_avwap_rule)
        if variant.volume_min_ratio is not None:
            cfg.volume_min_ratio = float(variant.volume_min_ratio)
        return cfg

    runner.default_short_config = _patched_default_short_config
    if variant.signal_avwap_dist_atr_max is not None:
        runner.V15_SHORT_SIGNAL_AVWAP_DIST_ATR_MAX = float(variant.signal_avwap_dist_atr_max)

    runner.main()

    trades_csv = _latest_new_file(outputs_root, "avwap_longshort_trades_v15_ALL_DAYS_*.csv", before_trades)
    daywise_csv = _latest_new_file(outputs_root, "avwap_daywise_breakdown_v15_ALL_DAYS_*.csv", before_daywise)
    trades = pd.read_csv(trades_csv)
    metrics = _extract_scope_metrics(trades, analysis_dates)
    watch = _today_watch_summary(trades)
    return {
        "variant": variant.name,
        "description": variant.description,
        "changes": {
            "mod_impulse_min_atr": variant.mod_impulse_min_atr,
            "close_near_extreme_max": variant.close_near_extreme_max,
            "small_counter_max_atr": variant.small_counter_max_atr,
            "buffer_abs": variant.buffer_abs,
            "buffer_pct": variant.buffer_pct,
            "require_entry_close_confirm": variant.require_entry_close_confirm,
            "avwap_dist_atr_mult": variant.avwap_dist_atr_mult,
            "signal_avwap_dist_atr_max": variant.signal_avwap_dist_atr_max,
            "require_avwap_rule": variant.require_avwap_rule,
            "volume_min_ratio": variant.volume_min_ratio,
        },
        "trades_csv": trades_csv,
        "daywise_csv": daywise_csv,
        "metrics": metrics,
        "today_watch": watch,
    }


def main() -> None:
    import codex_post_trade_advisor_v15_new as advisor

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    analysis_dates = advisor._choose_analysis_dates(END_DATE, LOOKBACK_DAYS)
    results = [_run_variant(variant, analysis_dates) for variant in VARIANTS]

    baseline = next(result for result in results if result["variant"] == "baseline")
    baseline_recent = baseline["metrics"]["short_recent_window"]
    baseline_all = baseline["metrics"]["short_all_days"]

    summary_rows: List[Dict[str, Any]] = []
    for result in results:
        recent = result["metrics"]["short_recent_window"]
        overall = result["metrics"]["short_all_days"]
        amod_recent = result["metrics"]["a_mod_recent_window"]
        hal = result["today_watch"].get("HAL", {})
        mazdock = result["today_watch"].get("MAZDOCK", {})
        row = {
            "variant": result["variant"],
            "description": result["description"],
            "recent_trades": recent["trades"],
            "recent_win_rate_pct": recent["win_rate_pct"],
            "recent_day_win_rate_pct": recent["day_win_rate_pct"],
            "recent_profit_factor": recent["profit_factor"],
            "recent_sum_pnl_rs": recent["sum_pnl_rs"],
            "recent_sum_pnl_pct": recent["sum_pnl_pct"],
            "recent_avg_trades_per_active_day": recent["avg_trades_per_active_day"],
            "recent_max_drawdown_rs": recent["max_drawdown_rs"],
            "overall_trades": overall["trades"],
            "overall_win_rate_pct": overall["win_rate_pct"],
            "overall_day_win_rate_pct": overall["day_win_rate_pct"],
            "overall_profit_factor": overall["profit_factor"],
            "overall_sum_pnl_rs": overall["sum_pnl_rs"],
            "overall_sum_pnl_pct": overall["sum_pnl_pct"],
            "overall_avg_trades_per_active_day": overall["avg_trades_per_active_day"],
            "overall_max_drawdown_rs": overall["max_drawdown_rs"],
            "a_mod_recent_trades": amod_recent["trades"],
            "a_mod_recent_win_rate_pct": amod_recent["win_rate_pct"],
            "a_mod_recent_profit_factor": amod_recent["profit_factor"],
            "today_hal_first_entry": hal.get("first_entry_time", ""),
            "today_hal_entries": hal.get("entries", 0),
            "today_mazdock_first_entry": mazdock.get("first_entry_time", ""),
            "today_mazdock_entries": mazdock.get("entries", 0),
            "trades_csv": result["trades_csv"],
        }
        row.update({f"recent_{k}": v for k, v in _delta_metrics(baseline_recent, recent).items()})
        row.update({f"overall_{k}": v for k, v in _delta_metrics(baseline_all, overall).items()})
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = OUTPUT_DIR / f"short_early_entry_summary_{END_DATE.isoformat()}.csv"
    summary_json = OUTPUT_DIR / f"short_early_entry_summary_{END_DATE.isoformat()}.json"
    summary_df.to_csv(summary_csv, index=False)
    summary_json.write_text(
        json.dumps(
            _json_ready(
                {
                    "end_date": END_DATE,
                    "analysis_dates": analysis_dates,
                    "watch_tickers": list(WATCH_TICKERS),
                    "variants": results,
                }
            ),
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"[SHORT-EARLY] analysis_dates={','.join(d.isoformat() for d in analysis_dates)}", flush=True)
    print(f"[SHORT-EARLY] summary_csv={summary_csv}", flush=True)
    print(f"[SHORT-EARLY] summary_json={summary_json}", flush=True)
    print(summary_df.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
