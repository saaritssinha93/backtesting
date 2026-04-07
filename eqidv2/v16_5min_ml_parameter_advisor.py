# -*- coding: utf-8 -*-
"""
Suggestion-only ML/AI research advisor for AVWAP v16 5-minute strategy.

What it does
------------
1. Reuses the real `avwap_combined_runner_v16_5min.py` scan/context/filter/exit flow.
2. Evaluates curated entry + filter + context scenarios over historical data.
3. Runs walk-forward scoring on each scenario for robustness.
4. Applies a second-stage risk overlay search on the top entry scenarios.
5. Optionally fits a lightweight surrogate model to estimate which parameters
   matter most for robust out-of-sample performance.
6. Produces suggestion files only. It never edits live/backtest strategy code.

Typical usage
-------------
Quick smoke:
    python eqidv2/v16_5min_ml_parameter_advisor.py --breadth quick --scenario-limit 3 --skip-risk-stage

Standard research run:
    python eqidv2/v16_5min_ml_parameter_advisor.py --breadth standard --max-workers 4

Intensive run:
    python eqidv2/v16_5min_ml_parameter_advisor.py --breadth intensive --max-workers 4
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass, field
from datetime import time as dtime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Sequence, Tuple

import numpy as np
import pandas as pd

import avwap_combined_runner_v16_5min as base

try:
    from sklearn.ensemble import RandomForestRegressor
except Exception:  # pragma: no cover - optional dependency
    RandomForestRegressor = None


ROOT = Path(__file__).resolve().parent
OUT_ROOT = ROOT / "outputs_v16_5min_ml_advisor"
DEFAULT_ADVISOR_WORKERS = max(1, min(8, (os.cpu_count() or 8)))


@dataclass(frozen=True)
class ParameterSpec:
    name: str
    owner: str
    kind: str
    baseline: Any
    candidates: List[Any]
    description: str


@dataclass
class ScenarioSpec:
    name: str
    stage: str
    notes: str
    shared_globals: Dict[str, Any] = field(default_factory=dict)
    filter_globals: Dict[str, Any] = field(default_factory=dict)
    short_overrides: Dict[str, Any] = field(default_factory=dict)
    long_overrides: Dict[str, Any] = field(default_factory=dict)
    parent: str = ""


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Suggestion-only ML advisor for AVWAP v16 5-minute strategy")
    p.add_argument("--breadth", choices=["quick", "standard", "intensive"], default="standard")
    p.add_argument(
        "--max-workers",
        type=int,
        default=max(1, int(os.getenv("EQIDV16_5MIN_MAX_WORKERS", str(DEFAULT_ADVISOR_WORKERS)))),
        help="Workers passed to the underlying v16 scanner",
    )
    p.add_argument("--train-days", type=int, default=30, help="Walk-forward train window in market days")
    p.add_argument("--test-days", type=int, default=10, help="Walk-forward test window in market days")
    p.add_argument("--step-days", type=int, default=5, help="Walk-forward step size in market days")
    p.add_argument(
        "--top-entry-k",
        type=int,
        default=4,
        help="Top entry-stage scenarios passed into risk-stage refinement",
    )
    p.add_argument("--scenario-limit", type=int, default=0, help="Optional hard limit on entry scenarios")
    p.add_argument("--scenario-names", default="", help="Comma-separated scenario names to run")
    p.add_argument("--skip-risk-stage", action="store_true", help="Skip the second-stage SL/target search")
    p.add_argument("--skip-surrogate", action="store_true", help="Skip surrogate model fit")
    p.add_argument("--save-trades", action="store_true", help="Save per-scenario trades.csv outputs")
    p.add_argument("--dry-run", action="store_true", help="Write catalogs and planned scenarios only")
    return p.parse_args()


def _windows_to_str(windows: Sequence[Tuple[dtime, dtime]]) -> str:
    return " | ".join(f"{a.strftime('%H:%M')}-{b.strftime('%H:%M')}" for a, b in windows)


def _windows_total_minutes(windows: Sequence[Tuple[dtime, dtime]]) -> int:
    total = 0
    for start, end in windows:
        total += ((end.hour * 60 + end.minute) - (start.hour * 60 + start.minute))
    return int(total)


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


def _profit_factor(pnl: pd.Series) -> float:
    pos = float(pnl[pnl > 0].sum())
    neg = float(-pnl[pnl < 0].sum())
    if neg <= 0:
        return math.inf if pos > 0 else 0.0
    return pos / neg


def _max_drawdown(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = pnl.cumsum()
    dd = equity.cummax() - equity
    return float(dd.max())


def _calc_metrics(df: pd.DataFrame, market_days: Sequence[pd.Timestamp]) -> Dict[str, float]:
    market_days_count = max(1, len(market_days))
    if df.empty:
        return {
            "trades": 0,
            "trade_days": 0,
            "market_days": market_days_count,
            "trades_per_market_day": 0.0,
            "sum_pnl_pct": 0.0,
            "pnl_pct_per_market_day": 0.0,
            "avg_pnl_pct": 0.0,
            "trade_win_pct": 0.0,
            "day_win_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "expectancy_pct_per_trade": 0.0,
            "sum_pnl_rs": 0.0,
        }

    work = df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce").dt.normalize()
    work = work.dropna(subset=["trade_date"]).copy()
    if work.empty:
        return _calc_metrics(pd.DataFrame(), market_days)

    work = work.sort_values(["trade_date", "entry_time_ist", "ticker"], kind="mergesort")
    pnl = pd.to_numeric(work["pnl_pct"], errors="coerce").fillna(0.0)
    day_pnl = work.groupby("trade_date", sort=True)["pnl_pct"].sum()
    sum_pnl_pct = float(pnl.sum())

    return {
        "trades": int(len(work)),
        "trade_days": int(work["trade_date"].nunique()),
        "market_days": market_days_count,
        "trades_per_market_day": round(float(len(work) / market_days_count), 4),
        "sum_pnl_pct": round(sum_pnl_pct, 4),
        "pnl_pct_per_market_day": round(sum_pnl_pct / market_days_count, 4),
        "avg_pnl_pct": round(float(pnl.mean()), 4),
        "trade_win_pct": round(float((pnl > 0).mean() * 100.0), 2),
        "day_win_pct": round(float((day_pnl > 0).mean() * 100.0), 2),
        "profit_factor": round(float(_profit_factor(pnl)), 4),
        "max_drawdown_pct": round(float(_max_drawdown(pnl)), 4),
        "expectancy_pct_per_trade": round(float(pnl.mean()), 4),
        "sum_pnl_rs": round(
            float(pd.to_numeric(work.get("pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()),
            2,
        ),
    }


def _score_metrics(metrics: Dict[str, float], target_tpd: float) -> float:
    # Guard: folds with fewer than 5 trades have no statistical meaning — return neutral 0
    if int(metrics.get("trades", 0)) < 5:
        return 0.0
    pf = float(metrics.get("profit_factor", 0.0))
    pf = min(max(pf, 0.0), 5.0)
    pnl_per_day = float(metrics.get("pnl_pct_per_market_day", 0.0))
    day_win = float(metrics.get("day_win_pct", 0.0))
    trade_win = float(metrics.get("trade_win_pct", 0.0))
    max_dd = max(0.0, float(metrics.get("max_drawdown_pct", 0.0)))
    tpd = float(metrics.get("trades_per_market_day", 0.0))
    # Only penalise over-trading (noise/overfit signal) — not under-trading (may be higher quality)
    overtrading_penalty = max(0.0, tpd - max(target_tpd * 1.5, 1.0))
    calmar_like = pnl_per_day / max(max_dd / max(float(metrics.get("market_days", 1)), 1.0), 1e-6)
    score = (
        (8.0 * pnl_per_day)
        + (2.4 * np.log1p(pf))
        + (0.05 * day_win)
        + (0.025 * trade_win)
        + (0.35 * calmar_like)
        - (0.50 * overtrading_penalty)
    )
    return round(float(score), 6)


@contextmanager
def _temporary_globals(overrides: Dict[str, Any]) -> Iterator[None]:
    original: Dict[str, Any] = {}
    try:
        for key, value in overrides.items():
            original[key] = getattr(base, key)
            setattr(base, key, value)
        yield
    finally:
        for key, value in original.items():
            setattr(base, key, value)


def _configure_cfgs(run_dir: Path) -> Tuple[base.StrategyConfig, base.StrategyConfig]:
    short_cfg = base.default_short_config(reports_dir=run_dir)
    long_cfg = base.default_long_config_v9(reports_dir=run_dir)
    dir_5m = base._resolve_15m_dir()
    for cfg in (short_cfg, long_cfg):
        cfg.dir_15m = str(dir_5m)
        cfg.end_15m = "_stocks_indicators_5min.parquet"
        cfg.market_regime_tickers = tuple(base.NIFTY_CONTEXT_TICKERS)

    short_cfg, long_cfg = base.apply_live_parity_profile(short_cfg, long_cfg)

    if base.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        short_cfg.min_bars_left_after_entry = 0
        long_cfg.min_bars_left_after_entry = 0
    if base.FORCE_LIVE_PARITY_DISABLE_TOPN:
        for cfg in (short_cfg, long_cfg):
            cfg.enable_topn_per_day = False
            cfg.topn_per_day = 0
    if base.FINAL_SIGNAL_WINDOW_OVERRIDE:
        short_cfg.use_time_windows = bool(base.FINAL_SHORT_USE_TIME_WINDOWS)
        short_cfg.signal_windows = list(base.FINAL_SHORT_SIGNAL_WINDOWS)
        long_cfg.use_time_windows = bool(base.FINAL_LONG_USE_TIME_WINDOWS)
        long_cfg.signal_windows = list(base.FINAL_LONG_SIGNAL_WINDOWS)
    if base.TEST_TARGET_OVERRIDE:
        short_cfg.target_pct = float(base.TEST_SHORT_TARGET_PCT)
        long_cfg.target_pct = float(base.TEST_LONG_TARGET_PCT)

    vix_map = base._load_india_vix(ROOT)
    for cfg in (short_cfg, long_cfg):
        cfg.vix_scale_enabled = base.VIX_SCALE_ENABLED
        if base.VIX_SCALE_ENABLED and vix_map:
            cfg.vix_daily = vix_map
            cfg.vix_baseline = base.VIX_BASELINE
            cfg.vix_scale_min = base.VIX_SCALE_MIN
            cfg.vix_scale_max = base.VIX_SCALE_MAX
            cfg.vix_scale_target = base.VIX_SCALE_TARGET
            cfg.vix_scale_sl = base.VIX_SCALE_SL

    short_regime_map, short_regime_source = base.build_market_regime_map(short_cfg)
    if short_regime_map:
        short_cfg.market_regime_map = short_regime_map
        short_cfg.enable_market_regime_filter = True
    else:
        short_cfg.enable_market_regime_filter = False
    setattr(short_cfg, "_regime_source", short_regime_source or "")

    long_regime_map, long_regime_source = base.build_market_regime_map(long_cfg)
    if long_regime_map:
        long_cfg.market_regime_map = long_regime_map
        long_cfg.enable_market_regime_filter = True
    else:
        long_cfg.enable_market_regime_filter = False
    setattr(long_cfg, "_regime_source", long_regime_source or "")
    return short_cfg, long_cfg


def _apply_cfg_overrides(cfg: base.StrategyConfig, overrides: Dict[str, Any]) -> None:
    for key, value in (overrides or {}).items():
        if not hasattr(cfg, key):
            raise ValueError(f"Unknown StrategyConfig override: {key}")
        setattr(cfg, key, value)


def _resolve_market_days(cfg: base.StrategyConfig) -> List[pd.Timestamp]:
    mode_map, _, _, _ = base._build_nifty_intraday_context(cfg)
    days = sorted(
        {
            pd.Timestamp(key[:10]).normalize()
            for key in mode_map.keys()
            if isinstance(key, str) and len(key) >= 10
        }
    )
    return days


def _walkforward_splits(
    market_days: Sequence[pd.Timestamp],
    train_days: int,
    test_days: int,
    step_days: int,
) -> List[Tuple[int, List[pd.Timestamp], List[pd.Timestamp]]]:
    days = list(sorted(pd.Timestamp(d).normalize() for d in market_days))
    if len(days) < (train_days + test_days):
        return []
    step = max(1, int(step_days))
    out: List[Tuple[int, List[pd.Timestamp], List[pd.Timestamp]]] = []
    fold = 0
    for start in range(0, len(days) - (train_days + test_days) + 1, step):
        fold += 1
        train_slice = days[start : start + train_days]
        test_slice = days[start + train_days : start + train_days + test_days]
        if train_slice and test_slice:
            out.append((fold, train_slice, test_slice))
    return out


def _slice_days(df: pd.DataFrame, days: Sequence[pd.Timestamp]) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    day_set = {pd.Timestamp(d).normalize() for d in days}
    work = df.copy()
    trade_dates = pd.to_datetime(work["trade_date"], errors="coerce").dt.normalize()
    return work.loc[trade_dates.isin(day_set)].copy().reset_index(drop=True)


def _inspect_intrabar_suffix(dir_1m: Path) -> str:
    suffix = ".parquet"
    if dir_1m.is_dir():
        sample_files = list(dir_1m.glob("*"))[:5]
        for sample_file in sample_files:
            if sample_file.suffix:
                suffix = sample_file.suffix
                break
    return suffix


def _collect_actual_parameters(short_cfg: base.StrategyConfig, long_cfg: base.StrategyConfig) -> Dict[str, Any]:
    return {
        "shared.nifty_min_daymove_pct": float(base.NIFTY_CONTEXT_MIN_DAYMOVE_PCT),
        "shared.nifty_rs_lookback_bars": int(base.NIFTY_RS_LOOKBACK_BARS),
        "shared.nifty_both_rs_long_pct": float(base.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT),
        "shared.nifty_both_rs_short_pct": float(base.NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT),
        "shared.v16_or_gate_enabled": bool(base.V16_OR_GATE_ENABLED),
        "short.adx_min": float(short_cfg.adx_min),
        "short.volume_min_ratio": float(short_cfg.volume_min_ratio),
        "short.rsi_max_short": float(short_cfg.rsi_max_short),
        "short.stop_pct": float(short_cfg.stop_pct),
        "short.target_pct": float(short_cfg.target_pct),
        "short.lag_mod_break": int(short_cfg.lag_bars_short_a_mod_break_c1_low),
        "short.lag_pullback_break": int(short_cfg.lag_bars_short_a_pullback_c2_break_c2_low),
        "short.lag_huge_failed_bounce": int(short_cfg.lag_bars_short_b_huge_failed_bounce),
        "short.signal_windows": _windows_to_str(short_cfg.signal_windows),
        "short.signal_windows_total_min": _windows_total_minutes(short_cfg.signal_windows),
        "short.signal_window_segments": int(len(short_cfg.signal_windows)),
        "short.dead_rsi_lo": float(base.V16_SHORT_RSI_DEAD_ZONE_LO),
        "short.dead_rsi_hi": float(base.V16_SHORT_RSI_DEAD_ZONE_HI),
        "long.adx_min": float(long_cfg.adx_min),
        "long.rsi_min_long": float(long_cfg.rsi_min_long),
        "long.volume_min_ratio": float(long_cfg.volume_min_ratio),
        "long.quality_score_min": float(long_cfg.quality_score_min),
        "long.stop_pct": float(long_cfg.stop_pct),
        "long.target_pct": float(long_cfg.target_pct),
        "long.lag_mod_break": int(long_cfg.lag_bars_long_a_mod_break_c1_high),
        "long.lag_pullback_break": int(long_cfg.lag_bars_long_a_pullback_c2_break_c2_high),
        "long.lag_huge_hold_break": int(long_cfg.lag_bars_long_b_huge_pullback_hold_break),
        "long.lag_huge_reclaim_break": int(long_cfg.lag_bars_long_b_huge_c1_close_reclaim_break),
        "long.signal_windows": _windows_to_str(long_cfg.signal_windows),
        "long.signal_windows_total_min": _windows_total_minutes(long_cfg.signal_windows),
        "long.signal_window_segments": int(len(long_cfg.signal_windows)),
        "long.qs_dead_lo": float(base.V16_LONG_QS_DEAD_LO),
        "long.qs_dead_hi": float(base.V16_LONG_QS_DEAD_HI),
        "long.qs_abs_max": float(base.V16_LONG_QS_ABS_MAX),
        "long.avwap_dist_max": float(base.V16_LONG_AVWAP_DIST_ATR_MAX),
        "long.avwap_dead_lo": float(base.V16_LONG_AVWAP_DIST_DEAD_LO),
        "long.avwap_dead_hi": float(base.V16_LONG_AVWAP_DIST_DEAD_HI),
        "shared.v16_long_vol_exhaust_enabled": bool(base.V16_LONG_ENTRY_VOL_EXHAUST_ENABLED),
        "shared.v16_long_vol_exhaust_mult": float(base.V16_LONG_ENTRY_VOL_EXHAUST_MULT),
        "shared.v16_long_vol_exhaust_min_bars": int(base.V16_LONG_ENTRY_VOL_EXHAUST_MIN_BARS),
    }


def _numeric_param_frame(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    out = pd.DataFrame(index=df.index)
    for col in df.columns:
        if col in {"scenario_name", "stage", "notes"}:
            continue
        series = df[col]
        if series.dtype == bool:
            out[col] = series.astype(int)
        else:
            numeric = pd.to_numeric(series, errors="coerce")
            if numeric.notna().any():
                fill = float(numeric.median()) if numeric.notna().any() else 0.0
                out[col] = numeric.fillna(fill)
    return out


def _parameter_catalog() -> List[ParameterSpec]:
    return [
        ParameterSpec("shared.nifty_min_daymove_pct", "shared", "float", float(base.NIFTY_CONTEXT_MIN_DAYMOVE_PCT), [0.20, 0.35, 0.50], "Minimum NIFTY day move for directional context."),
        ParameterSpec("shared.nifty_rs_lookback_bars", "shared", "int", int(base.NIFTY_RS_LOOKBACK_BARS), [3, 4, 5], "Lookback bars for stock-vs-NIFTY relative strength."),
        ParameterSpec("shared.nifty_both_rs_long_pct", "shared", "float", float(base.NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT), [0.50, 0.75, 1.00], "LONG-side RS threshold in BOTH mode."),
        ParameterSpec("shared.nifty_both_rs_short_pct", "shared", "float", float(base.NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT), [0.50, 0.75, 1.00], "SHORT-side RS threshold in BOTH mode."),
        ParameterSpec("shared.v16_or_gate_enabled", "shared", "bool", bool(base.V16_OR_GATE_ENABLED), [False, True], "Opening-range gate toggle."),
        ParameterSpec("short.signal_windows", "short", "window", _windows_to_str(base.FINAL_SHORT_SIGNAL_WINDOWS), ["09:15-11:00 | 12:00-13:30", "09:15-11:30 | 12:00-13:30", "09:15-11:00 | 12:00-14:00"], "Short-side entry windows."),
        ParameterSpec("short.adx_min", "short", "float", 28.0, [26.0, 28.0, 30.0], "Minimum ADX for short entry."),
        ParameterSpec("short.volume_min_ratio", "short", "float", 0.80, [0.70, 0.80, 0.90], "Impulse volume ratio for short entry."),
        ParameterSpec("short.rsi_max_short", "short", "float", 62.0, [58.0, 62.0, 66.0], "Maximum RSI allowed for short entries."),
        ParameterSpec("short.dead_rsi_band", "short", "band", [35.0, 40.0], [[34.0, 39.0], [35.0, 40.0], [36.0, 41.0]], "RSI dead-zone band for short anti-exhaustion."),
        ParameterSpec("long.signal_windows", "long", "window", _windows_to_str(base.FINAL_LONG_SIGNAL_WINDOWS), ["09:15-11:00 | 12:00-13:00", "09:15-11:30", "09:15-11:00 | 12:00-13:30"], "Long-side entry windows."),
        ParameterSpec("long.adx_min", "long", "float", 22.0, [20.0, 22.0, 24.0], "Minimum ADX for long entry."),
        ParameterSpec("long.rsi_min_long", "long", "float", 50.0, [48.0, 50.0, 52.0], "Minimum RSI for long entry."),
        ParameterSpec("long.volume_min_ratio", "long", "float", 0.80, [0.70, 0.80, 0.90], "Impulse volume ratio for long entry."),
        ParameterSpec("long.quality_score_min", "long", "float", 4.0, [3.5, 4.0, 4.5], "Minimum long quality score."),
        ParameterSpec("long.qs_dead_band", "long", "band", [7.5, 8.0], [[7.0, 7.5], [7.5, 8.0], [8.0, 8.5]], "Long quality-score dead-zone band."),
        ParameterSpec("long.avwap_dead_band", "long", "band", [1.0, 1.5], [[0.8, 1.2], [1.0, 1.5], [1.2, 1.6]], "Long AVWAP dead-zone band in ATR."),
        ParameterSpec("long.avwap_dist_max", "long", "float", 3.0, [2.5, 3.0, 3.5], "Maximum long AVWAP distance ATR."),
        ParameterSpec("short.stop_pct", "risk", "float", 0.0078, [0.0072, 0.0078, 0.0084], "Short stop-loss percent."),
        ParameterSpec("short.target_pct", "risk", "float", 0.0070, [0.0065, 0.0070, 0.0075], "Short target percent."),
        ParameterSpec("long.stop_pct", "risk", "float", 0.0075, [0.0065, 0.0075, 0.0080], "Long stop-loss percent."),
        ParameterSpec("long.target_pct", "risk", "float", 0.0080, [0.0075, 0.0080, 0.0085], "Long target percent."),
        ParameterSpec("shared.v16_long_vol_exhaust_enabled", "shared", "bool", bool(base.V16_LONG_ENTRY_VOL_EXHAUST_ENABLED), [False, True], "LONG volume exhaustion filter toggle."),
        ParameterSpec("shared.v16_long_vol_exhaust_mult", "shared", "float", float(base.V16_LONG_ENTRY_VOL_EXHAUST_MULT), [3.0, 4.0, 5.0], "Entry bar volume multiplier threshold for exhaustion filter."),
        ParameterSpec("shared.v16_long_vol_exhaust_min_bars", "shared", "int", int(base.V16_LONG_ENTRY_VOL_EXHAUST_MIN_BARS), [2, 3, 4], "Minimum bars from open for exhaustion filter to apply."),
    ]


def _entry_scenarios(breadth: str) -> List[ScenarioSpec]:
    scenarios = [
        ScenarioSpec(name="baseline", stage="entry", notes="Current v16 5-minute live/backtest parity profile."),
        ScenarioSpec(name="context_looser", stage="entry", notes="Allow more participation via softer NIFTY context thresholds.", shared_globals={"NIFTY_CONTEXT_MIN_DAYMOVE_PCT": 0.20, "NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT": 0.50, "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT": 0.50, "NIFTY_RS_BOTH_MODE_THRESHOLD_PCT": 0.50}),
        ScenarioSpec(name="context_stricter", stage="entry", notes="Demand stronger NIFTY trend and stronger relative strength.", shared_globals={"NIFTY_CONTEXT_MIN_DAYMOVE_PCT": 0.50, "NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT": 1.00, "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT": 1.00, "NIFTY_RS_BOTH_MODE_THRESHOLD_PCT": 1.00}),
        ScenarioSpec(name="short_participation_plus", stage="entry", notes="Open short entries moderately with looser ADX and volume rules.", short_overrides={"adx_min": 26.0, "volume_min_ratio": 0.70, "rsi_max_short": 64.0}),
        ScenarioSpec(name="short_quality_plus", stage="entry", notes="Higher-quality short entries only.", short_overrides={"adx_min": 30.0, "volume_min_ratio": 0.90, "rsi_max_short": 58.0}),
        ScenarioSpec(name="long_participation_plus", stage="entry", notes="Open long participation through slightly looser trend and quality filters.", long_overrides={"adx_min": 20.0, "rsi_min_long": 48.0, "volume_min_ratio": 0.70, "quality_score_min": 3.5}),
        ScenarioSpec(name="long_quality_plus", stage="entry", notes="Higher-quality long entries only.", long_overrides={"adx_min": 24.0, "rsi_min_long": 52.0, "volume_min_ratio": 0.90, "quality_score_min": 4.5}),
        ScenarioSpec(name="balanced_plus", stage="entry", notes="Moderate participation increase on both sides without removing core guards.", shared_globals={"NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT": 0.60, "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT": 0.60, "NIFTY_RS_BOTH_MODE_THRESHOLD_PCT": 0.60}, short_overrides={"adx_min": 27.0, "volume_min_ratio": 0.75}, long_overrides={"adx_min": 21.0, "rsi_min_long": 49.0, "volume_min_ratio": 0.75, "quality_score_min": 3.75}),
        ScenarioSpec(name="momentum_strict", stage="entry", notes="Higher-conviction momentum filter on both sides.", shared_globals={"NIFTY_CONTEXT_MIN_DAYMOVE_PCT": 0.40}, short_overrides={"adx_min": 30.0, "volume_min_ratio": 0.90}, long_overrides={"adx_min": 24.0, "rsi_min_long": 52.0, "volume_min_ratio": 0.90, "quality_score_min": 4.5}),
        ScenarioSpec(name="long_window_ext_1130", stage="entry", notes="Extend long morning window to 11:30.", long_overrides={"use_time_windows": True, "signal_windows": [(dtime(9, 15), dtime(11, 30))]}),
        ScenarioSpec(name="long_window_pm_1330", stage="entry", notes="Extend long afternoon participation to 13:30.", long_overrides={"use_time_windows": True, "signal_windows": [(dtime(9, 15), dtime(11, 0)), (dtime(12, 0), dtime(13, 30))]}),
        ScenarioSpec(name="short_window_ext_1130", stage="entry", notes="Extend short morning window to 11:30.", short_overrides={"use_time_windows": True, "signal_windows": [(dtime(9, 15), dtime(11, 30)), (dtime(12, 0), dtime(13, 30))]}),
        ScenarioSpec(name="both_window_ext_1130", stage="entry", notes="Extend both sides through 11:30 for the morning leg.", short_overrides={"use_time_windows": True, "signal_windows": [(dtime(9, 15), dtime(11, 30)), (dtime(12, 0), dtime(13, 30))]}, long_overrides={"use_time_windows": True, "signal_windows": [(dtime(9, 15), dtime(11, 30))]}),
        ScenarioSpec(name="long_deadzone_narrow", stage="entry", notes="Narrow long AVWAP and quality dead zones without removing exhaustion caps.", filter_globals={"V16_LONG_QS_DEAD_LO": 7.25, "V16_LONG_QS_DEAD_HI": 7.75, "V16_LONG_AVWAP_DIST_DEAD_LO": 1.10, "V16_LONG_AVWAP_DIST_DEAD_HI": 1.35}),
        ScenarioSpec(name="long_deadzone_removed", stage="entry", notes="Remove long dead zones while keeping the absolute exhaustion caps.", filter_globals={"V16_LONG_QS_DEAD_LO": 99.0, "V16_LONG_QS_DEAD_HI": 100.0, "V16_LONG_AVWAP_DIST_DEAD_LO": 99.0, "V16_LONG_AVWAP_DIST_DEAD_HI": 100.0}),
        ScenarioSpec(name="short_deadzone_shifted", stage="entry", notes="Shift short RSI dead zone slightly lower.", filter_globals={"V16_SHORT_RSI_DEAD_ZONE_LO": 34.0, "V16_SHORT_RSI_DEAD_ZONE_HI": 39.0}),
        ScenarioSpec(name="or_gate_on_probe", stage="entry", notes="Probe whether restoring OR gate improves robustness.", filter_globals={"V16_OR_GATE_ENABLED": True}),
        ScenarioSpec(name="reactive_lags", stage="entry", notes="Faster signal-to-entry response for both sides.", short_overrides={"lag_bars_short_a_mod_break_c1_low": 0, "lag_bars_short_a_pullback_c2_break_c2_low": 1}, long_overrides={"lag_bars_long_a_mod_break_c1_high": 0, "lag_bars_long_a_pullback_c2_break_c2_high": 1, "lag_bars_long_b_huge_c1_close_reclaim_break": 1}),
        ScenarioSpec(name="max_participation_safe", stage="entry", notes="Most permissive entry profile while retaining broad anti-exhaustion caps.", shared_globals={"NIFTY_CONTEXT_MIN_DAYMOVE_PCT": 0.20, "NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT": 0.50, "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT": 0.50, "NIFTY_RS_BOTH_MODE_THRESHOLD_PCT": 0.50}, short_overrides={"adx_min": 26.0, "volume_min_ratio": 0.70, "rsi_max_short": 64.0}, long_overrides={"adx_min": 20.0, "rsi_min_long": 48.0, "volume_min_ratio": 0.70, "quality_score_min": 3.5}, filter_globals={"V16_LONG_AVWAP_DIST_ATR_MAX": 3.2, "V16_LONG_QS_ABS_MAX": 10.5}),
    ]

    if breadth == "quick":
        keep = {"baseline", "context_looser", "short_participation_plus", "long_participation_plus", "balanced_plus", "max_participation_safe"}
        return [scenario for scenario in scenarios if scenario.name in keep]
    if breadth == "standard":
        return scenarios

    intensive = list(scenarios)
    intensive.extend(
        [
            ScenarioSpec(name="context_looser_lookback3", stage="entry", notes="Looser NIFTY thresholds with shorter RS lookback.", shared_globals={"NIFTY_CONTEXT_MIN_DAYMOVE_PCT": 0.20, "NIFTY_RS_LOOKBACK_BARS": 3, "NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT": 0.50, "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT": 0.50, "NIFTY_RS_BOTH_MODE_THRESHOLD_PCT": 0.50}),
            ScenarioSpec(name="context_strict_lookback5", stage="entry", notes="Stricter context with longer RS lookback.", shared_globals={"NIFTY_CONTEXT_MIN_DAYMOVE_PCT": 0.50, "NIFTY_RS_LOOKBACK_BARS": 5, "NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT": 1.00, "NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT": 1.00, "NIFTY_RS_BOTH_MODE_THRESHOLD_PCT": 1.00}),
            ScenarioSpec(name="both_window_pm", stage="entry", notes="Add more afternoon participation on both sides.", short_overrides={"use_time_windows": True, "signal_windows": [(dtime(9, 15), dtime(11, 0)), (dtime(12, 0), dtime(14, 0))]}, long_overrides={"use_time_windows": True, "signal_windows": [(dtime(9, 15), dtime(11, 0)), (dtime(12, 0), dtime(13, 30))]}),
            ScenarioSpec(name="long_avwap_cap_tighter", stage="entry", notes="Reduce long AVWAP chase tolerance.", filter_globals={"V16_LONG_AVWAP_DIST_ATR_MAX": 2.5}),
            ScenarioSpec(name="vol_exhaust_off", stage="entry", notes="Disable LONG volume exhaustion filter entirely to measure its marginal contribution.", filter_globals={"V16_LONG_ENTRY_VOL_EXHAUST_ENABLED": False}),
            ScenarioSpec(name="vol_exhaust_strict_5x", stage="entry", notes="Tighten LONG volume exhaustion threshold to 5x avg — fewer trades filtered.", filter_globals={"V16_LONG_ENTRY_VOL_EXHAUST_MULT": 5.0}),
            ScenarioSpec(name="vol_exhaust_loose_3x", stage="entry", notes="Loosen LONG volume exhaustion threshold to 3x avg — more aggressive filtering.", filter_globals={"V16_LONG_ENTRY_VOL_EXHAUST_MULT": 3.0}),
        ]
    )
    return intensive


def _risk_overlays() -> List[ScenarioSpec]:
    return [
        ScenarioSpec(name="risk_baseline", stage="risk", notes="Baseline SL/target settings."),
        ScenarioSpec(name="risk_tighter", stage="risk", notes="Tighter stops and slightly smaller targets.", short_overrides={"stop_pct": 0.0072, "target_pct": 0.0065}, long_overrides={"stop_pct": 0.0065, "target_pct": 0.0075}),
        ScenarioSpec(name="risk_balanced", stage="risk", notes="Moderately tighter stops with baseline targets.", short_overrides={"stop_pct": 0.0074, "target_pct": 0.0070}, long_overrides={"stop_pct": 0.0070, "target_pct": 0.0080}),
        ScenarioSpec(name="risk_asymmetric_push", stage="risk", notes="Let winners run a bit further on both sides.", short_overrides={"stop_pct": 0.0082, "target_pct": 0.0075}, long_overrides={"stop_pct": 0.0078, "target_pct": 0.0085}),
        ScenarioSpec(name="risk_short_tighter_long_looser", stage="risk", notes="Prioritize cleaner shorts while giving longs more room.", short_overrides={"stop_pct": 0.0072, "target_pct": 0.0068}, long_overrides={"stop_pct": 0.0080, "target_pct": 0.0085}),
        ScenarioSpec(name="risk_short_looser_long_tighter", stage="risk", notes="Give shorts more room while tightening long risk.", short_overrides={"stop_pct": 0.0084, "target_pct": 0.0072}, long_overrides={"stop_pct": 0.0065, "target_pct": 0.0078}),
    ]


def _scenario_row(spec: ScenarioSpec) -> Dict[str, Any]:
    return {"name": spec.name, "stage": spec.stage, "notes": spec.notes, "parent": spec.parent, "shared_globals": _safe_json(spec.shared_globals), "filter_globals": _safe_json(spec.filter_globals), "short_overrides": _safe_json(spec.short_overrides), "long_overrides": _safe_json(spec.long_overrides)}


def _run_scenario_backtest(
    short_cfg: base.StrategyConfig,
    long_cfg: base.StrategyConfig,
    max_workers: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    short_df, long_df = base._run_both_parallel(short_cfg, long_cfg, max_workers)

    if base.NIFTY_CONTEXT_ENABLED:
        mode_map, nifty_ret_map, _, _ = base._build_nifty_intraday_context(short_cfg)
        if mode_map:
            short_df, long_df = base._apply_nifty_intraday_context(
                short_df,
                long_df,
                short_cfg,
                mode_map,
                nifty_ret_map,
            )

    if base.V16_OR_GATE_ENABLED:
        short_df, long_df = base._enrich_with_or_levels(
            short_df,
            long_df,
            dir_15m=short_cfg.dir_15m,
            parquet_suffix=short_cfg.end_15m,
        )

    if base.V16_LONG_ENTRY_VOL_EXHAUST_ENABLED and not long_df.empty:
        long_df = base._enrich_with_entry_vol_ratio(
            long_df,
            dir_15m=short_cfg.dir_15m,
            parquet_suffix=short_cfg.end_15m,
        )

    short_df, long_df = base._apply_v16_post_scan_filters(short_df, long_df)

    if short_df.empty and long_df.empty:
        return short_df, long_df, pd.DataFrame()

    dir_1m = base._resolve_5min_dir()
    suffix = _inspect_intrabar_suffix(dir_1m)

    if not short_df.empty:
        short_df = base._resolve_exits_5min(
            short_df,
            dir_1m,
            suffix,
            short_cfg.parquet_engine,
            eod_exit_time=base.V15_EOD_EXIT_TIME,
        )
        short_df = base._add_notional_pnl(base._sort_trades_for_output(short_df))

    if not long_df.empty:
        long_df = base._resolve_exits_5min(
            long_df,
            dir_1m,
            suffix,
            long_cfg.parquet_engine,
            eod_exit_time=base.V15_EOD_EXIT_TIME,
        )
        long_df = base._add_notional_pnl(base._sort_trades_for_output(long_df))

    combined = pd.concat([short_df, long_df], ignore_index=True)
    if not combined.empty:
        combined = base._add_notional_pnl(base._sort_trades_for_output(combined))
        short_df = combined[combined["side"].astype(str).str.upper().eq("SHORT")].copy().reset_index(drop=True)
        long_df = combined[combined["side"].astype(str).str.upper().eq("LONG")].copy().reset_index(drop=True)
    return short_df, long_df, combined.reset_index(drop=True)


def _evaluate_scenario(
    spec: ScenarioSpec,
    out_dir: Path,
    max_workers: int,
    train_days: int,
    test_days: int,
    step_days: int,
    save_trades: bool,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    scenario_dir = out_dir / "scenarios" / spec.name
    scenario_dir.mkdir(parents=True, exist_ok=True)
    log_path = scenario_dir / "scenario.log"

    temp_globals = {}
    temp_globals.update(spec.shared_globals)
    temp_globals.update(spec.filter_globals)

    started = time.perf_counter()
    with open(log_path, "w", encoding="utf-8") as log_fh, redirect_stdout(log_fh), redirect_stderr(log_fh):
        with _temporary_globals(temp_globals):
            short_cfg, long_cfg = _configure_cfgs(scenario_dir)
            _apply_cfg_overrides(short_cfg, spec.short_overrides)
            _apply_cfg_overrides(long_cfg, spec.long_overrides)
            market_days = _resolve_market_days(short_cfg)
            short_df, long_df, combined = _run_scenario_backtest(short_cfg, long_cfg, max_workers)
            actual_params = _collect_actual_parameters(short_cfg, long_cfg)

    runtime_sec = round(float(time.perf_counter() - started), 2)
    if not market_days:
        market_days = sorted(
            {
                pd.Timestamp(d).normalize()
                for d in pd.to_datetime(combined.get("trade_date", pd.Series(dtype="object")), errors="coerce").dropna()
            }
        )

    combined_metrics = _calc_metrics(combined, market_days)
    short_metrics = _calc_metrics(short_df, market_days)
    long_metrics = _calc_metrics(long_df, market_days)
    splits = _walkforward_splits(market_days, train_days=train_days, test_days=test_days, step_days=step_days)

    fold_rows: List[Dict[str, Any]] = []
    for fold_id, train_slice, test_slice in splits:
        train_combined = _slice_days(combined, train_slice)
        train_short = _slice_days(short_df, train_slice)
        train_long = _slice_days(long_df, train_slice)
        test_combined = _slice_days(combined, test_slice)
        test_short = _slice_days(short_df, test_slice)
        test_long = _slice_days(long_df, test_slice)

        row: Dict[str, Any] = {
            "scenario_name": spec.name,
            "stage": spec.stage,
            "fold": fold_id,
            "train_start": str(train_slice[0].date()),
            "train_end": str(train_slice[-1].date()),
            "test_start": str(test_slice[0].date()),
            "test_end": str(test_slice[-1].date()),
        }
        for prefix, metrics in [
            ("train_combined", _calc_metrics(train_combined, train_slice)),
            ("test_combined", _calc_metrics(test_combined, test_slice)),
            ("train_short", _calc_metrics(train_short, train_slice)),
            ("test_short", _calc_metrics(test_short, test_slice)),
            ("train_long", _calc_metrics(train_long, train_slice)),
            ("test_long", _calc_metrics(test_long, test_slice)),
        ]:
            for key, value in metrics.items():
                row[f"{prefix}_{key}"] = value
        fold_rows.append(row)

    summary_row: Dict[str, Any] = {
        "scenario_name": spec.name,
        "stage": spec.stage,
        "notes": spec.notes,
        "parent": spec.parent,
        "runtime_sec": runtime_sec,
        "fold_count": int(len(splits)),
        "market_days": int(len(market_days)),
        "log_path": str(log_path),
        "shared_globals": _safe_json(spec.shared_globals),
        "filter_globals": _safe_json(spec.filter_globals),
        "short_overrides": _safe_json(spec.short_overrides),
        "long_overrides": _safe_json(spec.long_overrides),
    }
    for prefix, metrics in [("combined", combined_metrics), ("short", short_metrics), ("long", long_metrics)]:
        for key, value in metrics.items():
            summary_row[f"{prefix}_{key}"] = value
    summary_row.update(actual_params)
    summary_row["short_windows_str"] = actual_params["short.signal_windows"]
    summary_row["long_windows_str"] = actual_params["long.signal_windows"]

    payload = {"spec": _scenario_row(spec), "summary": summary_row, "folds": fold_rows}
    (scenario_dir / "summary.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    if save_trades and not combined.empty:
        combined.to_csv(scenario_dir / "trades.csv", index=False)
    return summary_row, fold_rows


def _build_leaderboard(summary_rows: List[Dict[str, Any]], fold_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not summary_rows:
        return pd.DataFrame()

    summary_df = pd.DataFrame(summary_rows).copy()
    fold_df = pd.DataFrame(fold_rows).copy()
    baseline = summary_df.loc[summary_df["scenario_name"] == "baseline"]
    if baseline.empty:
        baseline = summary_df.head(1)
    base_combined_tpd = float(baseline.iloc[0].get("combined_trades_per_market_day", 3.0))
    base_short_tpd = float(baseline.iloc[0].get("short_trades_per_market_day", 1.5))
    base_long_tpd = float(baseline.iloc[0].get("long_trades_per_market_day", 1.5))

    if not fold_df.empty:
        for prefix, target in [
            ("train_combined", base_combined_tpd),
            ("test_combined", base_combined_tpd),
            ("train_short", base_short_tpd),
            ("test_short", base_short_tpd),
            ("train_long", base_long_tpd),
            ("test_long", base_long_tpd),
        ]:
            score_col = f"{prefix}_score"
            fold_df[score_col] = fold_df.apply(
                lambda row, pref=prefix, tpd=target: _score_metrics(
                    {
                        "trades": row.get(f"{pref}_trades", 0),
                        "profit_factor": row.get(f"{pref}_profit_factor", 0.0),
                        "pnl_pct_per_market_day": row.get(f"{pref}_pnl_pct_per_market_day", 0.0),
                        "day_win_pct": row.get(f"{pref}_day_win_pct", 0.0),
                        "trade_win_pct": row.get(f"{pref}_trade_win_pct", 0.0),
                        "max_drawdown_pct": row.get(f"{pref}_max_drawdown_pct", 0.0),
                        "trades_per_market_day": row.get(f"{pref}_trades_per_market_day", 0.0),
                        "market_days": row.get(f"{pref}_market_days", 1),
                    },
                    tpd,
                ),
                axis=1,
            )

        agg = fold_df.groupby(["scenario_name", "stage"], as_index=False).agg(
            avg_train_combined_score=("train_combined_score", "mean"),
            avg_test_combined_score=("test_combined_score", "mean"),
            std_test_combined_score=("test_combined_score", "std"),
            avg_train_long_score=("train_long_score", "mean"),
            avg_test_long_score=("test_long_score", "mean"),
            std_test_long_score=("test_long_score", "std"),
            avg_train_short_score=("train_short_score", "mean"),
            avg_test_short_score=("test_short_score", "mean"),
            std_test_short_score=("test_short_score", "std"),
        )
    else:
        agg = pd.DataFrame(
            {
                "scenario_name": summary_df["scenario_name"],
                "stage": summary_df["stage"],
                "avg_train_combined_score": 0.0,
                "avg_test_combined_score": 0.0,
                "std_test_combined_score": 0.0,
                "avg_train_long_score": 0.0,
                "avg_test_long_score": 0.0,
                "std_test_long_score": 0.0,
                "avg_train_short_score": 0.0,
                "avg_test_short_score": 0.0,
                "std_test_short_score": 0.0,
            }
        )

    for col in agg.columns:
        if col.startswith("std_"):
            agg[col] = agg[col].fillna(0.0)

    leaderboard = summary_df.merge(agg, on=["scenario_name", "stage"], how="left")
    leaderboard["combined_overfit_gap"] = leaderboard["avg_train_combined_score"] - leaderboard["avg_test_combined_score"]
    leaderboard["long_overfit_gap"] = leaderboard["avg_train_long_score"] - leaderboard["avg_test_long_score"]
    leaderboard["short_overfit_gap"] = leaderboard["avg_train_short_score"] - leaderboard["avg_test_short_score"]
    leaderboard["combined_robust_score"] = leaderboard["avg_test_combined_score"] - (0.50 * leaderboard["std_test_combined_score"]) - (0.25 * leaderboard["combined_overfit_gap"].clip(lower=0.0))
    leaderboard["long_robust_score"] = leaderboard["avg_test_long_score"] - (0.50 * leaderboard["std_test_long_score"]) - (0.25 * leaderboard["long_overfit_gap"].clip(lower=0.0))
    leaderboard["short_robust_score"] = leaderboard["avg_test_short_score"] - (0.50 * leaderboard["std_test_short_score"]) - (0.25 * leaderboard["short_overfit_gap"].clip(lower=0.0))
    return leaderboard.sort_values(["combined_robust_score", "combined_sum_pnl_pct", "combined_profit_factor"], ascending=[False, False, False]).reset_index(drop=True)


def _fit_surrogate(leaderboard: pd.DataFrame, out_dir: Path, skip_surrogate: bool) -> pd.DataFrame:
    if skip_surrogate or RandomForestRegressor is None or leaderboard.empty:
        return pd.DataFrame()

    feature_cols = [col for col in leaderboard.columns if col.startswith("shared.") or col.startswith("short.") or col.startswith("long.")]
    param_df = _numeric_param_frame(leaderboard[["scenario_name", "stage", "notes"] + feature_cols].to_dict("records"))
    if param_df.empty or param_df.shape[1] == 0:
        return pd.DataFrame()

    # Require at least 3x samples-per-feature (minimum 20) for the RF to be meaningful.
    # With fewer rows the importance scores are dominated by sampling noise, not signal.
    min_rows_needed = max(20, param_df.shape[1] * 3)
    if len(leaderboard) < min_rows_needed:
        print(
            f"[SURROGATE] Skipped: need >= {min_rows_needed} scenarios for {param_df.shape[1]} features, "
            f"got {len(leaderboard)}. Run --breadth intensive with risk-stage enabled to accumulate more scenarios."
        )
        return pd.DataFrame()

    y = pd.to_numeric(leaderboard["combined_robust_score"], errors="coerce").fillna(0.0).to_numpy()
    model = RandomForestRegressor(n_estimators=500, random_state=42, min_samples_leaf=2, n_jobs=-1)
    model.fit(param_df, y)
    importance = pd.DataFrame({"feature": param_df.columns, "importance": model.feature_importances_}).sort_values("importance", ascending=False).reset_index(drop=True)

    top_n = max(3, min(6, len(leaderboard) // 3))
    top_df = leaderboard.head(top_n)
    bottom_df = leaderboard.tail(top_n)
    direction_rows: List[Dict[str, Any]] = []
    for col in param_df.columns:
        top_vals = pd.to_numeric(top_df[col], errors="coerce")
        bottom_vals = pd.to_numeric(bottom_df[col], errors="coerce")
        if top_vals.notna().sum() == 0 or bottom_vals.notna().sum() == 0:
            continue
        delta = float(top_vals.mean() - bottom_vals.mean())
        pref = "mixed" if abs(delta) < 1e-9 else ("higher" if delta > 0 else "lower")
        direction_rows.append(
            {
                "feature": col,
                "top_mean": round(float(top_vals.mean()), 6),
                "bottom_mean": round(float(bottom_vals.mean()), 6),
                "delta_top_minus_bottom": round(delta, 6),
                "preferred_direction": pref,
            }
        )
    direction_df = pd.DataFrame(direction_rows)
    if not direction_df.empty:
        importance = importance.merge(direction_df, on="feature", how="left")
    importance.to_csv(out_dir / "surrogate_feature_importance.csv", index=False)
    return importance


def _report_changed_values(best_row: pd.Series, baseline_row: pd.Series, prefixes: Sequence[str]) -> List[str]:
    out: List[str] = []
    for prefix in prefixes:
        keys = [
            col
            for col in best_row.index
            if col.startswith(prefix)
            and not col.endswith("_total_min")
            and not col.endswith("_segments")
            and col not in {"short.signal_windows", "long.signal_windows"}
        ]
        for key in sorted(keys):
            best_val = best_row.get(key)
            base_val = baseline_row.get(key)
            if str(best_val) != str(base_val):
                out.append(f"- `{key}`: `{base_val}` -> `{best_val}`")
    return out


def _write_markdown_report(out_dir: Path, leaderboard: pd.DataFrame, importance: pd.DataFrame) -> None:
    if leaderboard.empty:
        (out_dir / "suggestion_report.md").write_text("# v16_5min ML Advisor\n\nNo scenarios were evaluated.\n", encoding="utf-8")
        return

    baseline = leaderboard.loc[leaderboard["scenario_name"] == "baseline"]
    if baseline.empty:
        baseline = leaderboard.head(1)
    baseline_row = baseline.iloc[0]
    best_combined = leaderboard.sort_values("combined_robust_score", ascending=False).iloc[0]
    best_long = leaderboard.sort_values("long_robust_score", ascending=False).iloc[0]
    best_short = leaderboard.sort_values("short_robust_score", ascending=False).iloc[0]

    lines = [
        "# v16_5min ML Advisor Report",
        "",
        "This report is suggestion-only. No strategy files were changed by this advisor.",
        "",
        "## Best Combined Scenario",
        f"- Scenario: `{best_combined['scenario_name']}`",
        f"- Stage: `{best_combined['stage']}`",
        f"- Notes: {best_combined['notes']}",
        f"- Combined robust score: `{round(float(best_combined['combined_robust_score']), 4)}`",
        f"- Combined avg test score: `{round(float(best_combined['avg_test_combined_score']), 4)}`",
        f"- Combined pnl pct: `{best_combined['combined_sum_pnl_pct']}`",
        f"- Combined PF: `{best_combined['combined_profit_factor']}`",
        f"- Combined max DD pct: `{best_combined['combined_max_drawdown_pct']}`",
        f"- Long windows: `{best_combined['long_windows_str']}`",
        f"- Short windows: `{best_combined['short_windows_str']}`",
        "",
        "## Recommended Parameter Changes vs Baseline",
    ]
    changes = _report_changed_values(best_combined, baseline_row, ["shared.", "short.", "long."])
    lines.extend(changes or ["- No value changes from baseline."])

    lines.extend(
        [
            "",
            "## Best Long Scenario",
            f"- Scenario: `{best_long['scenario_name']}`",
            f"- Stage: `{best_long['stage']}`",
            f"- Notes: {best_long['notes']}",
            f"- Long robust score: `{round(float(best_long['long_robust_score']), 4)}`",
            f"- Long pnl pct: `{best_long['long_sum_pnl_pct']}`",
            f"- Long PF: `{best_long['long_profit_factor']}`",
            f"- Long windows: `{best_long['long_windows_str']}`",
            "",
            "## Best Short Scenario",
            f"- Scenario: `{best_short['scenario_name']}`",
            f"- Stage: `{best_short['stage']}`",
            f"- Notes: {best_short['notes']}",
            f"- Short robust score: `{round(float(best_short['short_robust_score']), 4)}`",
            f"- Short pnl pct: `{best_short['short_sum_pnl_pct']}`",
            f"- Short PF: `{best_short['short_profit_factor']}`",
            f"- Short windows: `{best_short['short_windows_str']}`",
            "",
            "## Top 5 Combined Scenarios",
        ]
    )
    for _, row in leaderboard.head(5).iterrows():
        lines.append(f"- `{row['scenario_name']}` | stage=`{row['stage']}` | robust=`{round(float(row['combined_robust_score']), 4)}` | pnl=`{row['combined_sum_pnl_pct']}` | pf=`{row['combined_profit_factor']}` | dd=`{row['combined_max_drawdown_pct']}`")

    if not importance.empty:
        lines.extend(["", "## Surrogate Feature Importance"])
        for _, row in importance.head(10).iterrows():
            direction = row.get("preferred_direction", "")
            direction_txt = f" | preferred=`{direction}`" if isinstance(direction, str) and direction else ""
            lines.append(f"- `{row['feature']}` | importance=`{round(float(row['importance']), 6)}`{direction_txt}")

    (out_dir / "suggestion_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_summary(out_dir: Path, leaderboard: pd.DataFrame) -> None:
    if leaderboard.empty:
        payload = {"status": "empty"}
    else:
        payload = {
            "best_combined": leaderboard.sort_values("combined_robust_score", ascending=False).head(1).to_dict("records")[0],
            "best_long": leaderboard.sort_values("long_robust_score", ascending=False).head(1).to_dict("records")[0],
            "best_short": leaderboard.sort_values("short_robust_score", ascending=False).head(1).to_dict("records")[0],
            "top5_combined": leaderboard.head(5).to_dict("records"),
        }
    (out_dir / "advisor_summary.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def main() -> None:
    args = _parse_args()
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = OUT_ROOT / f"run_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    base.MAX_WORKERS = max(1, int(args.max_workers))
    base.ENABLE_LEGACY_CHARTS = False
    base.ENABLE_ENHANCED_CHARTS = False

    catalog = _parameter_catalog()
    (out_dir / "parameter_catalog.json").write_text(json.dumps([asdict(spec) for spec in catalog], indent=2, default=str), encoding="utf-8")

    entry_scenarios = _entry_scenarios(args.breadth)
    if args.scenario_names:
        allowed = {name.strip() for name in args.scenario_names.split(",") if name.strip()}
        entry_scenarios = [scenario for scenario in entry_scenarios if scenario.name in allowed]
    if args.scenario_limit and args.scenario_limit > 0:
        entry_scenarios = entry_scenarios[: int(args.scenario_limit)]

    pd.DataFrame([_scenario_row(scenario) for scenario in entry_scenarios]).to_csv(out_dir / "entry_scenario_catalog.csv", index=False)

    if args.dry_run:
        print(f"[DRY RUN] Wrote catalogs to {out_dir}")
        print(f"[DRY RUN] Entry scenarios planned: {len(entry_scenarios)}")
        return

    entry_summary_rows: List[Dict[str, Any]] = []
    entry_fold_rows: List[Dict[str, Any]] = []

    print(f"Running v16_5min ML advisor with {len(entry_scenarios)} entry scenarios")
    print(f"Outputs: {out_dir}")
    for idx, scenario in enumerate(entry_scenarios, start=1):
        print(f"[ENTRY {idx}/{len(entry_scenarios)}] {scenario.name} - {scenario.notes}")
        summary_row, fold_rows = _evaluate_scenario(
            scenario,
            out_dir=out_dir,
            max_workers=base.MAX_WORKERS,
            train_days=args.train_days,
            test_days=args.test_days,
            step_days=args.step_days,
            save_trades=bool(args.save_trades),
        )
        entry_summary_rows.append(summary_row)
        entry_fold_rows.extend(fold_rows)

    pd.DataFrame(entry_summary_rows).to_csv(out_dir / "entry_stage_summary.csv", index=False)
    pd.DataFrame(entry_fold_rows).to_csv(out_dir / "entry_stage_folds.csv", index=False)

    leaderboard = _build_leaderboard(entry_summary_rows, entry_fold_rows)
    if not leaderboard.empty:
        leaderboard.to_csv(out_dir / "entry_stage_leaderboard.csv", index=False)

    all_summary_rows = list(entry_summary_rows)
    all_fold_rows = list(entry_fold_rows)

    if not args.skip_risk_stage and not leaderboard.empty:
        risk_catalog = _risk_overlays()
        pd.DataFrame([_scenario_row(spec) for spec in risk_catalog]).to_csv(out_dir / "risk_overlay_catalog.csv", index=False)
        top_entry_names = leaderboard.head(max(1, int(args.top_entry_k)))["scenario_name"].tolist()
        name_to_spec = {spec.name: spec for spec in entry_scenarios}
        risk_summary_rows: List[Dict[str, Any]] = []
        risk_fold_rows: List[Dict[str, Any]] = []
        combos: List[ScenarioSpec] = []
        for parent_name in top_entry_names:
            parent = name_to_spec.get(parent_name)
            if parent is None:
                continue
            for overlay in risk_catalog:
                combos.append(
                    ScenarioSpec(
                        name=f"{parent.name}__{overlay.name}",
                        stage="risk",
                        notes=f"{parent.notes} + {overlay.notes}",
                        shared_globals=dict(parent.shared_globals),
                        filter_globals=dict(parent.filter_globals),
                        short_overrides={**parent.short_overrides, **overlay.short_overrides},
                        long_overrides={**parent.long_overrides, **overlay.long_overrides},
                        parent=parent.name,
                    )
                )

        print(f"Running risk-stage refinement on {len(combos)} scenarios")
        for idx, scenario in enumerate(combos, start=1):
            print(f"[RISK {idx}/{len(combos)}] {scenario.name}")
            summary_row, fold_rows = _evaluate_scenario(
                scenario,
                out_dir=out_dir,
                max_workers=base.MAX_WORKERS,
                train_days=args.train_days,
                test_days=args.test_days,
                step_days=args.step_days,
                save_trades=bool(args.save_trades),
            )
            risk_summary_rows.append(summary_row)
            risk_fold_rows.extend(fold_rows)

        pd.DataFrame(risk_summary_rows).to_csv(out_dir / "risk_stage_summary.csv", index=False)
        pd.DataFrame(risk_fold_rows).to_csv(out_dir / "risk_stage_folds.csv", index=False)
        all_summary_rows.extend(risk_summary_rows)
        all_fold_rows.extend(risk_fold_rows)

    final_leaderboard = _build_leaderboard(all_summary_rows, all_fold_rows)
    final_leaderboard.to_csv(out_dir / "advisor_leaderboard.csv", index=False)
    pd.DataFrame(all_fold_rows).to_csv(out_dir / "advisor_walkforward_folds.csv", index=False)

    importance = _fit_surrogate(final_leaderboard, out_dir, skip_surrogate=args.skip_surrogate)
    _write_markdown_report(out_dir, final_leaderboard, importance)
    _write_json_summary(out_dir, final_leaderboard)

    print(f"Advisor run complete. Leaderboard: {out_dir / 'advisor_leaderboard.csv'}")
    print(f"Suggestion report: {out_dir / 'suggestion_report.md'}")


if __name__ == "__main__":
    main()
