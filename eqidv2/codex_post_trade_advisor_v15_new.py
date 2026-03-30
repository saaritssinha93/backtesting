from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, RUNTIME_ROOT, report_subdir


VERSION_TAG = "v15_new"
SIGNAL_PATTERNS = (
    "signals_{}_v15_new_short.csv",
    "signals_{}_v15_new_long.csv",
)
LIVE_TRADES_PATTERN = "live_trades_{}_v15_new.csv"
PAPER_TRADES_PATTERN = "paper_trades_{}_v15_new.csv"
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
DEFAULT_LOOKBACK_DAYS = 20
MIN_SETUP_SAMPLE = 2
MAX_REASONABLE_SIGNAL_LAG_SEC = 1800.0

CONFIG_HINTS: Dict[Tuple[str, str], List[Dict[str, str]]] = {
    (
        "SHORT",
        "A_MOD_BREAK_C1_LOW",
    ): [
        {
            "field": "SHORT_MAX_ENTRY_SLIP_PCT",
            "file": "avwap_trade_execution_PAPER_TRADE_FALSE_v15.py",
            "action": "consider_tightening",
            "note": "Paper is much stronger than live. A short-side slip cap can protect against weaker live fills.",
            "current_value": "0.0000",
            "trial_value": "0.0015",
            "priority_hint": 60,
        },
        {
            "field": "short_cfg.adx_min",
            "file": "avwap_combined_runner_v15.py",
            "action": "monitor_only",
            "note": "Core short A_MOD is working. Change only if you want even fewer but cleaner shorts.",
            "current_value": "17.0",
            "trial_value": "18.5",
            "priority_hint": 20,
        },
    ],
    (
        "SHORT",
        "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    ): [
        {
            "field": "SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW",
            "file": "avwap_combined_runner_v15.py",
            "action": "test_alternative_lag",
            "note": "This setup is weak recently. Test a different lag before keeping it unchanged.",
            "current_value": "2",
            "trial_value": "1",
            "priority_hint": 75,
        },
        {
            "field": "short_cfg.small_counter_max_atr",
            "file": "avwap_v11_refactored/avwap_common_v11.py",
            "action": "tighten",
            "note": "Require a cleaner, smaller C2 pullback before accepting the setup.",
            "current_value": "0.20",
            "trial_value": "0.15",
            "priority_hint": 80,
        },
        {
            "field": "short_cfg.signal_avwap_dist_atr_max",
            "file": "avwap_combined_runner_v15.py",
            "action": "tighten",
            "note": "Lower the allowed AVWAP distance so weak stretched pullbacks get filtered out.",
            "current_value": "2.10",
            "trial_value": "1.60",
            "priority_hint": 78,
        },
        {
            "field": "short_cfg.entry_time_cutoff",
            "file": "avwap_combined_runner_v15.py",
            "action": "make_earlier",
            "note": "If late-day pullback shorts are weaker, move the cutoff earlier.",
            "current_value": "13:15",
            "trial_value": "12:30",
            "priority_hint": 65,
        },
        {
            "field": "short_cfg.min_opening_range_width_pct",
            "file": "avwap_combined_runner_v15.py",
            "action": "raise",
            "note": "Require a stronger opening range before allowing this pullback-break short.",
            "current_value": "1.00",
            "trial_value": "1.25",
            "priority_hint": 70,
        },
    ],
    (
        "LONG",
        "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
    ): [
        {
            "field": "LONG_LAG_BARS_B_HUGE_C1_CLOSE_RECLAIM_BREAK",
            "file": "avwap_combined_runner_v15.py",
            "action": "test_higher_lag",
            "note": "This long setup is weak recently. Test a later confirmation bar to avoid low-quality reclaims.",
            "current_value": "2",
            "trial_value": "3",
            "priority_hint": 88,
        },
        {
            "field": "long_cfg.enable_setup_b_huge_c1_close_reclaim_break",
            "file": "avwap_v11_refactored/avwap_common_v7_sweep.py",
            "action": "consider_disable",
            "note": "If weakness persists, disable this setup entirely instead of letting it drag the long side.",
            "current_value": "True",
            "trial_value": "False",
            "priority_hint": 95,
        },
        {
            "field": "long_cfg.quality_score_min",
            "file": "avwap_combined_runner_v15.py",
            "action": "raise",
            "note": "Require better-quality reclaim setups before entry.",
            "current_value": "4.5",
            "trial_value": "5.5",
            "priority_hint": 92,
        },
        {
            "field": "long_cfg.signal_avwap_dist_atr_min",
            "file": "avwap_combined_runner_v15.py",
            "action": "raise",
            "note": "Demand stronger momentum away from AVWAP before allowing the reclaim long.",
            "current_value": "0.40",
            "trial_value": "0.70",
            "priority_hint": 90,
        },
        {
            "field": "long_cfg.adx_min",
            "file": "avwap_combined_runner_v15.py",
            "action": "raise",
            "note": "Filter out weaker trend environments for this long setup.",
            "current_value": "22.0",
            "trial_value": "25.0",
            "priority_hint": 84,
        },
        {
            "field": "long_cfg.rsi_min_long",
            "file": "avwap_combined_runner_v15.py",
            "action": "raise",
            "note": "Require firmer momentum before accepting a reclaim breakout long.",
            "current_value": "50.0",
            "trial_value": "55.0",
            "priority_hint": 82,
        },
        {
            "field": "LONG_MAX_ENTRY_SLIP_PCT",
            "file": "avwap_trade_execution_PAPER_TRADE_FALSE_v15.py",
            "action": "tighten",
            "note": "Live longs are underperforming. Tightening the live slip gate can block overstretched fills.",
            "current_value": "0.0030",
            "trial_value": "0.0015",
            "priority_hint": 86,
        },
    ],
    (
        "LONG",
        "A_MOD_CLOSE_CONTINUATION_BREAK",
    ): [
        {
            "field": "long_cfg.enable_setup_a_close_continuation_break",
            "file": "avwap_v11_refactored/avwap_common_v7_sweep.py",
            "action": "enable_only_if_backtest_supports",
            "note": "This setup should stay gated behind evidence because recent live sample is thin.",
            "current_value": "False",
            "trial_value": "True",
            "priority_hint": 35,
        },
        {
            "field": "long_cfg.lag_bars_long_a_close_continuation_break",
            "file": "avwap_v11_refactored/avwap_common_v7_sweep.py",
            "action": "test_lag",
            "note": "If you enable this branch, test lag carefully before promoting it live.",
            "current_value": "1",
            "trial_value": "1",
            "priority_hint": 30,
        },
    ],
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _date_str(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def _to_ist_ts(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    try:
        if getattr(ts.dt, "tz", None) is None:
            return ts.dt.tz_localize("Asia/Kolkata", nonexistent="NaT", ambiguous="NaT")
        return ts.dt.tz_convert("Asia/Kolkata")
    except Exception:
        return ts


def _profit_factor(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    gross_profit = pnl[pnl > 0].sum()
    gross_loss = -pnl[pnl < 0].sum()
    if gross_profit <= 0 and gross_loss <= 0:
        return 0.0
    if gross_loss <= 0:
        return float("inf")
    return float(gross_profit / gross_loss)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isinf(value):
            return "inf"
        return None
    return value


def _display_metric(value: Any, digits: int = 4) -> Any:
    try:
        if value is None:
            return None
        value = float(value)
        if math.isinf(value):
            return "inf"
        if math.isnan(value):
            return None
        return round(value, digits)
    except Exception:
        return value


def _find_latest_backtest_csv(pattern: str) -> Optional[Path]:
    candidates = [
        RUNTIME_ROOT / "outputs_v15",
        Path(__file__).resolve().parent.parent / "outputs_v15",
    ]
    best: Optional[Path] = None
    best_mtime = -1.0
    for root in candidates:
        if not root.exists():
            continue
        for path in root.glob(pattern):
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if mtime > best_mtime:
                best = path
                best_mtime = mtime
    return best


def _discover_backtest_dates() -> List[date]:
    trades_path = _find_latest_backtest_csv("avwap_longshort_trades_v15_ALL_DAYS_*.csv")
    if trades_path is None:
        return []
    df = _read_csv(trades_path)
    if df.empty or "trade_date" not in df.columns:
        return []
    dates = pd.to_datetime(df["trade_date"], errors="coerce").dt.date.dropna().tolist()
    return sorted(set(dates))


def _discover_live_dates() -> List[date]:
    dates: set[date] = set()
    patterns = [
        "signals_*_v15_new_short.csv",
        "signals_*_v15_new_long.csv",
        "live_trades_*_v15_new.csv",
        "paper_trades_*_v15_new.csv",
    ]
    for pattern in patterns:
        for path in LIVE_SIGNALS_DIR.glob(pattern):
            match = DATE_RE.search(path.name)
            if not match:
                continue
            try:
                dates.add(_parse_date(match.group(1)))
            except ValueError:
                continue
    return sorted(dates)


def _choose_analysis_dates(end_date: date, lookback_days: int) -> List[date]:
    discovered = [d for d in (_discover_live_dates() + _discover_backtest_dates()) if d <= end_date]
    selected = discovered[-max(1, int(lookback_days)) :]
    if end_date not in selected:
        selected.append(end_date)
    selected = sorted(set(selected))
    return selected[-max(1, int(lookback_days)) :]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _load_signals_for_date(day: date) -> pd.DataFrame:
    day_str = _date_str(day)
    frames: List[pd.DataFrame] = []
    for pattern in SIGNAL_PATTERNS:
        path = LIVE_SIGNALS_DIR / pattern.format(day_str)
        df = _read_csv(path)
        if df.empty:
            continue
        df["source_path"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["analysis_date"] = day_str
    out["signal_datetime"] = _to_ist_ts(out.get("signal_datetime", pd.Series(dtype="object")))
    out["received_time"] = _to_ist_ts(out.get("received_time", pd.Series(dtype="object")))
    out["signal_lag_sec_raw"] = (out["received_time"] - out["signal_datetime"]).dt.total_seconds()
    out["signal_lag_sec"] = out["signal_lag_sec_raw"].where(
        (out["signal_lag_sec_raw"] >= 0.0) & (out["signal_lag_sec_raw"] <= MAX_REASONABLE_SIGNAL_LAG_SEC),
        np.nan,
    )
    out["side"] = out.get("side", pd.Series(dtype="object")).astype(str).str.upper()
    out["setup"] = out.get("setup", pd.Series(dtype="object")).fillna("UNKNOWN")
    out["quality_score"] = pd.to_numeric(out.get("quality_score"), errors="coerce")
    out["quantity"] = pd.to_numeric(out.get("quantity"), errors="coerce")
    return out


def _load_trades_for_date(day: date, mode: str) -> pd.DataFrame:
    day_str = _date_str(day)
    pattern = LIVE_TRADES_PATTERN if mode.upper() == "LIVE" else PAPER_TRADES_PATTERN
    path = LIVE_SIGNALS_DIR / pattern.format(day_str)
    df = _read_csv(path)
    if df.empty:
        return pd.DataFrame()
    df["analysis_date"] = day_str
    df["source_mode"] = mode.upper()
    df["source_path"] = str(path)
    for col in ("signal_datetime", "signal_entry_datetime_ist", "entry_time", "exit_time"):
        if col in df.columns:
            df[col] = _to_ist_ts(df[col])
    for col in ("pnl_rs", "pnl_pct", "entry_price", "filled_price", "exit_price", "quality_score", "quantity"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "side" in df.columns:
        df["side"] = df["side"].astype(str).str.upper()
    if "setup" in df.columns:
        df["setup"] = df["setup"].fillna("UNKNOWN")
    if "signal_datetime" in df.columns and "entry_time" in df.columns:
        df["entry_lag_sec"] = (df["entry_time"] - df["signal_datetime"]).dt.total_seconds()
    if "entry_time" in df.columns and "exit_time" in df.columns:
        df["holding_sec"] = (df["exit_time"] - df["entry_time"]).dt.total_seconds()
    return df


def _load_backtest_recent(dates: Sequence[date]) -> Tuple[pd.DataFrame, Optional[Path], Optional[Path]]:
    trades_path = _find_latest_backtest_csv("avwap_longshort_trades_v15_ALL_DAYS_*.csv")
    daywise_path = _find_latest_backtest_csv("avwap_daywise_breakdown_v15_ALL_DAYS_*.csv")
    if trades_path is None:
        return pd.DataFrame(), None, daywise_path
    df = _read_csv(trades_path)
    if df.empty:
        return pd.DataFrame(), trades_path, daywise_path
    df["trade_date"] = pd.to_datetime(df.get("trade_date"), errors="coerce").dt.date
    df = df[df["trade_date"].isin(set(dates))].copy()
    if "side" in df.columns:
        df["side"] = df["side"].astype(str).str.upper()
    if "setup" in df.columns:
        df["setup"] = df["setup"].fillna("UNKNOWN")
    if "signal_time_ist" in df.columns:
        df["signal_time_ist"] = _to_ist_ts(df["signal_time_ist"])
    if "entry_time_ist" in df.columns:
        df["entry_time_ist"] = _to_ist_ts(df["entry_time_ist"])
    for col in ("pnl_rs", "pnl_pct", "quality_score"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df, trades_path, daywise_path


def _trade_stats(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "sum_pnl_rs": 0.0,
            "sum_pnl_pct": 0.0,
            "profit_factor": 0.0,
            "avg_entry_lag_sec": 0.0,
            "avg_holding_min": 0.0,
        }
    pnl = pd.to_numeric(df.get("pnl_rs"), errors="coerce").fillna(0.0)
    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    trades = int(len(df))
    entry_lag = pd.to_numeric(df.get("entry_lag_sec"), errors="coerce")
    holding_sec = pd.to_numeric(df.get("holding_sec"), errors="coerce")
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round((wins / trades) * 100.0, 2) if trades else 0.0,
        "sum_pnl_rs": round(_safe_float(pnl.sum()), 2),
        "sum_pnl_pct": round(_safe_float(pd.to_numeric(df.get("pnl_pct"), errors="coerce").sum()), 4),
        "profit_factor": round(_profit_factor(pnl), 4) if trades else 0.0,
        "avg_entry_lag_sec": round(_safe_float(entry_lag.mean()), 2) if entry_lag is not None else 0.0,
        "avg_holding_min": round(_safe_float(holding_sec.mean()) / 60.0, 2) if holding_sec is not None else 0.0,
    }


def _daily_trade_row(
    day: date,
    signals_df: pd.DataFrame,
    live_df: pd.DataFrame,
    paper_df: pd.DataFrame,
    backtest_df: pd.DataFrame,
) -> Dict[str, Any]:
    signals_total = int(len(signals_df)) if not signals_df.empty else 0
    live_stats = _trade_stats(live_df)
    paper_stats = _trade_stats(paper_df)
    backtest_stats = _trade_stats(backtest_df)
    avg_signal_lag = round(_safe_float(pd.to_numeric(signals_df.get("signal_lag_sec"), errors="coerce").mean()), 2) if not signals_df.empty else 0.0
    return {
        "date": _date_str(day),
        "signals_total": signals_total,
        "signals_long": int((signals_df["side"] == "LONG").sum()) if not signals_df.empty else 0,
        "signals_short": int((signals_df["side"] == "SHORT").sum()) if not signals_df.empty else 0,
        "live_trades": live_stats["trades"],
        "paper_trades": paper_stats["trades"],
        "live_coverage_pct": round((live_stats["trades"] / signals_total) * 100.0, 2) if signals_total else 0.0,
        "paper_coverage_pct": round((paper_stats["trades"] / signals_total) * 100.0, 2) if signals_total else 0.0,
        "live_win_rate_pct": live_stats["win_rate_pct"],
        "paper_win_rate_pct": paper_stats["win_rate_pct"],
        "live_profit_factor": live_stats["profit_factor"],
        "paper_profit_factor": paper_stats["profit_factor"],
        "live_pnl_rs": live_stats["sum_pnl_rs"],
        "paper_pnl_rs": paper_stats["sum_pnl_rs"],
        "backtest_trades": backtest_stats["trades"],
        "backtest_win_rate_pct": backtest_stats["win_rate_pct"],
        "backtest_profit_factor": backtest_stats["profit_factor"],
        "backtest_pnl_rs": backtest_stats["sum_pnl_rs"],
        "avg_signal_lag_sec": avg_signal_lag,
        "avg_live_entry_lag_sec": live_stats["avg_entry_lag_sec"],
        "avg_paper_entry_lag_sec": paper_stats["avg_entry_lag_sec"],
    }


def _format_slot_label(ts_series: pd.Series) -> pd.Series:
    return ts_series.dt.strftime("%H:%M")


def _build_slot_timing_rows(
    signals_all: pd.DataFrame,
    live_all: pd.DataFrame,
    paper_all: pd.DataFrame,
) -> pd.DataFrame:
    if signals_all.empty:
        return pd.DataFrame()

    sig = signals_all.copy()
    sig["slot"] = _format_slot_label(sig["signal_datetime"])
    sig_rows = (
        sig.groupby(["analysis_date", "side", "slot"], dropna=False)
        .agg(
            signals=("signal_id", "count"),
            avg_signal_lag_sec=("signal_lag_sec", "mean"),
            avg_quality_score=("quality_score", "mean"),
        )
        .reset_index()
    )

    def _trade_slot_stats(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["analysis_date", "side", "slot", f"{prefix}_trades", f"avg_{prefix}_entry_lag_sec"])
        work = df.copy()
        work["slot"] = _format_slot_label(work["signal_datetime"])
        return (
            work.groupby(["analysis_date", "side", "slot"], dropna=False)
            .agg(
                **{
                    f"{prefix}_trades": ("trade_id", "count"),
                    f"avg_{prefix}_entry_lag_sec": ("entry_lag_sec", "mean"),
                }
            )
            .reset_index()
        )

    live_rows = _trade_slot_stats(live_all, "live")
    paper_rows = _trade_slot_stats(paper_all, "paper")
    out = sig_rows.merge(live_rows, on=["analysis_date", "side", "slot"], how="left")
    out = out.merge(paper_rows, on=["analysis_date", "side", "slot"], how="left")
    for col in out.columns:
        if col.endswith("_sec") or col.endswith("_score"):
            out[col] = out[col].round(2)
    count_cols = [c for c in out.columns if c.endswith("_trades") or c == "signals"]
    for col in count_cols:
        out[col] = out[col].fillna(0).astype(int)
    return out.sort_values(["analysis_date", "slot", "side"]).reset_index(drop=True)


def _build_missed_opportunity_rows(signals_all: pd.DataFrame, backtest_all: pd.DataFrame) -> pd.DataFrame:
    if backtest_all.empty:
        return pd.DataFrame()

    bt = backtest_all.copy()
    bt["signal_time_ist"] = _to_ist_ts(bt.get("signal_time_ist", pd.Series(dtype="object")))
    bt["signal_slot"] = _format_slot_label(bt["signal_time_ist"])
    bt["match_key"] = (
        bt["trade_date"].astype(str)
        + "|"
        + bt.get("ticker", pd.Series(dtype="object")).astype(str)
        + "|"
        + bt.get("side", pd.Series(dtype="object")).astype(str).str.upper()
        + "|"
        + bt.get("setup", pd.Series(dtype="object")).astype(str)
        + "|"
        + bt["signal_slot"].astype(str)
    )

    if signals_all.empty:
        signal_keys: set[str] = set()
    else:
        sig = signals_all.copy()
        sig["signal_slot"] = _format_slot_label(sig["signal_datetime"])
        sig["match_key"] = (
            sig["analysis_date"].astype(str)
            + "|"
            + sig.get("ticker", pd.Series(dtype="object")).astype(str)
            + "|"
            + sig.get("side", pd.Series(dtype="object")).astype(str).str.upper()
            + "|"
            + sig.get("setup", pd.Series(dtype="object")).astype(str)
            + "|"
            + sig["signal_slot"].astype(str)
        )
        signal_keys = set(sig["match_key"].dropna().tolist())

    missed = bt[~bt["match_key"].isin(signal_keys)].copy()
    if missed.empty:
        return missed
    cols = [
        "trade_date",
        "ticker",
        "side",
        "setup",
        "signal_time_ist",
        "entry_time_ist",
        "outcome",
        "pnl_rs",
        "pnl_pct",
        "quality_score",
        "match_key",
    ]
    keep = [c for c in cols if c in missed.columns]
    missed = missed.loc[:, keep].copy()
    if "signal_time_ist" in missed.columns:
        missed["signal_time_ist"] = missed["signal_time_ist"].astype(str)
    if "entry_time_ist" in missed.columns:
        missed["entry_time_ist"] = missed["entry_time_ist"].astype(str)
    return missed.sort_values(["trade_date", "side", "setup", "ticker"]).reset_index(drop=True)


def _aggregate_setup_metrics(
    signals_all: pd.DataFrame,
    live_all: pd.DataFrame,
    paper_all: pd.DataFrame,
    backtest_all: pd.DataFrame,
) -> pd.DataFrame:
    keys: set[Tuple[str, str]] = set()
    for df in (signals_all, live_all, paper_all, backtest_all):
        if df.empty or "setup" not in df.columns or "side" not in df.columns:
            continue
        keys.update(
            (str(row["side"]).upper(), str(row["setup"]))
            for _, row in df[["side", "setup"]].dropna().drop_duplicates().iterrows()
        )

    rows: List[Dict[str, Any]] = []
    for side, setup in sorted(keys):
        sig = signals_all[(signals_all["side"] == side) & (signals_all["setup"] == setup)].copy()
        live = live_all[(live_all["side"] == side) & (live_all["setup"] == setup)].copy()
        paper = paper_all[(paper_all["side"] == side) & (paper_all["setup"] == setup)].copy()
        bt = backtest_all[(backtest_all["side"] == side) & (backtest_all["setup"] == setup)].copy()
        live_stats = _trade_stats(live)
        paper_stats = _trade_stats(paper)
        bt_stats = _trade_stats(bt)
        rows.append(
            {
                "side": side,
                "setup": setup,
                "signals": int(len(sig)),
                "avg_signal_lag_sec": round(_safe_float(pd.to_numeric(sig.get("signal_lag_sec"), errors="coerce").mean()), 2)
                if not sig.empty
                else 0.0,
                "avg_quality_score": round(_safe_float(pd.to_numeric(sig.get("quality_score"), errors="coerce").mean()), 4)
                if not sig.empty
                else 0.0,
                "live_trades": live_stats["trades"],
                "live_win_rate_pct": live_stats["win_rate_pct"],
                "live_profit_factor": live_stats["profit_factor"],
                "live_pnl_rs": live_stats["sum_pnl_rs"],
                "live_avg_entry_lag_sec": live_stats["avg_entry_lag_sec"],
                "paper_trades": paper_stats["trades"],
                "paper_win_rate_pct": paper_stats["win_rate_pct"],
                "paper_profit_factor": paper_stats["profit_factor"],
                "paper_pnl_rs": paper_stats["sum_pnl_rs"],
                "paper_avg_entry_lag_sec": paper_stats["avg_entry_lag_sec"],
                "backtest_trades": bt_stats["trades"],
                "backtest_win_rate_pct": bt_stats["win_rate_pct"],
                "backtest_profit_factor": bt_stats["profit_factor"],
                "backtest_pnl_rs": bt_stats["sum_pnl_rs"],
                "live_signal_coverage_pct": round((live_stats["trades"] / len(sig)) * 100.0, 2) if len(sig) else 0.0,
                "paper_signal_coverage_pct": round((paper_stats["trades"] / len(sig)) * 100.0, 2) if len(sig) else 0.0,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        by=["paper_profit_factor", "live_profit_factor", "signals", "paper_trades"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def _side_summary(df: pd.DataFrame, label: str) -> List[Dict[str, Any]]:
    if df.empty or "side" not in df.columns:
        return []
    rows: List[Dict[str, Any]] = []
    for side, group in df.groupby("side"):
        stats = _trade_stats(group)
        rows.append(
            {
                "mode": label,
                "side": str(side),
                "trades": stats["trades"],
                "win_rate_pct": stats["win_rate_pct"],
                "profit_factor": stats["profit_factor"],
                "sum_pnl_rs": stats["sum_pnl_rs"],
                "avg_entry_lag_sec": stats["avg_entry_lag_sec"],
            }
        )
    return rows


def _generate_recommendations(setup_metrics: pd.DataFrame, daily_metrics: pd.DataFrame, end_date: date) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    if not daily_metrics.empty:
        avg_signal_lag = _safe_float(daily_metrics["avg_signal_lag_sec"].mean())
        avg_live_entry_lag = _safe_float(daily_metrics["avg_live_entry_lag_sec"].mean())
        if avg_signal_lag >= 25.0:
            recs.append(
                {
                    "priority": "medium",
                    "category": "latency",
                    "title": "Scanner latency is still meaningful",
                    "suggestion": "Keep tracking scanner-to-entry lag. Recent signal latency is high enough to affect fills on faster setups.",
                    "evidence": {
                        "avg_signal_lag_sec": _display_metric(avg_signal_lag, 2),
                        "avg_live_entry_lag_sec": _display_metric(avg_live_entry_lag, 2),
                        "window_end_date": _date_str(end_date),
                    },
                }
            )

    if setup_metrics.empty:
        recs.append(
            {
                "priority": "medium",
                "category": "data",
                "title": "Recent live sample is thin",
                "suggestion": "Collect a few more trading days before changing logic from this report alone.",
                "evidence": {"setups": 0},
            }
        )
        return recs

    strong = setup_metrics[
        (setup_metrics["paper_trades"] >= MIN_SETUP_SAMPLE)
        & (setup_metrics["paper_profit_factor"] >= 1.35)
        & (setup_metrics["paper_win_rate_pct"] >= 55.0)
    ].copy()
    for _, row in strong.head(3).iterrows():
        recs.append(
            {
                "priority": "high",
                "category": "favor_setup",
                "title": f"Favor {row['side']} {row['setup']} if it appears again",
                "suggestion": "Recent paper and live evidence is supportive. Keep this setup high on the watchlist.",
                "evidence": {
                    "signals": _safe_int(row["signals"]),
                    "paper_trades": _safe_int(row["paper_trades"]),
                    "paper_profit_factor": _display_metric(row["paper_profit_factor"]),
                    "paper_win_rate_pct": _display_metric(row["paper_win_rate_pct"], 2),
                    "live_trades": _safe_int(row["live_trades"]),
                    "live_profit_factor": _display_metric(row["live_profit_factor"]),
                },
            }
        )

    weak = setup_metrics[
        (
            (setup_metrics["paper_trades"] >= MIN_SETUP_SAMPLE)
            & ((setup_metrics["paper_profit_factor"] < 0.9) | (setup_metrics["paper_win_rate_pct"] < 45.0))
        )
        | (
            (setup_metrics["live_trades"] >= MIN_SETUP_SAMPLE)
            & ((setup_metrics["live_profit_factor"] < 0.9) | (setup_metrics["live_win_rate_pct"] < 45.0))
        )
    ].copy()
    for _, row in weak.head(3).iterrows():
        recs.append(
            {
                "priority": "high",
                "category": "review_setup",
                "title": f"Review {row['side']} {row['setup']}",
                "suggestion": "This setup is underperforming in the recent window. Consider tighter filters or lower priority.",
                "evidence": {
                    "signals": _safe_int(row["signals"]),
                    "paper_trades": _safe_int(row["paper_trades"]),
                    "paper_profit_factor": _display_metric(row["paper_profit_factor"]),
                    "paper_win_rate_pct": _display_metric(row["paper_win_rate_pct"], 2),
                    "live_trades": _safe_int(row["live_trades"]),
                    "live_profit_factor": _display_metric(row["live_profit_factor"]),
                },
            }
        )

    gap = setup_metrics[
        (setup_metrics["paper_trades"] >= MIN_SETUP_SAMPLE)
        & (setup_metrics["live_trades"] >= MIN_SETUP_SAMPLE)
        & (setup_metrics["paper_profit_factor"] - setup_metrics["live_profit_factor"] >= 0.75)
    ].copy()
    for _, row in gap.head(2).iterrows():
        recs.append(
            {
                "priority": "medium",
                "category": "execution_gap",
                "title": f"Execution gap on {row['side']} {row['setup']}",
                "suggestion": "Paper performance is stronger than live. Review signal timing, entry slip, and fill quality.",
                "evidence": {
                    "paper_profit_factor": _display_metric(row["paper_profit_factor"]),
                    "live_profit_factor": _display_metric(row["live_profit_factor"]),
                    "paper_avg_entry_lag_sec": _display_metric(row["paper_avg_entry_lag_sec"], 2),
                    "live_avg_entry_lag_sec": _display_metric(row["live_avg_entry_lag_sec"], 2),
                },
            }
        )

    if not recs:
        recs.append(
            {
                "priority": "low",
                "category": "stable",
                "title": "No strong recommendation yet",
                "suggestion": "Recent evidence is mixed or too thin. Keep collecting post-close reports before changing live logic.",
                "evidence": {"setups_observed": int(len(setup_metrics)), "window_days": int(len(daily_metrics))},
            }
        )
    return recs


def _markdown_table(df: pd.DataFrame, columns: Sequence[str]) -> str:
    if df.empty:
        return "_No rows_"
    subset = df.loc[:, [c for c in columns if c in df.columns]].copy()
    headers = [str(c) for c in subset.columns]
    rows = [[("" if pd.isna(v) else str(v)) for v in row] for row in subset.itertuples(index=False, name=None)]
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))
    header_line = "| " + " | ".join(h.ljust(widths[idx]) for idx, h in enumerate(headers)) + " |"
    sep_line = "| " + " | ".join("-" * widths[idx] for idx in range(len(headers))) + " |"
    body = [
        "| " + " | ".join(row[idx].ljust(widths[idx]) for idx in range(len(headers))) + " |"
        for row in rows
    ]
    return "\n".join([header_line, sep_line, *body])


def _build_report_markdown(
    end_date: date,
    analysis_dates: Sequence[date],
    daily_metrics: pd.DataFrame,
    setup_metrics: pd.DataFrame,
    side_rows: pd.DataFrame,
    slot_timing_rows: pd.DataFrame,
    missed_rows: pd.DataFrame,
    knob_rows: pd.DataFrame,
    recommendations: Sequence[Dict[str, Any]],
    source_info: Dict[str, Any],
) -> str:
    lines: List[str] = []
    lines.append(f"# Codex Post-Trade Advisor - {VERSION_TAG}")
    lines.append("")
    lines.append(f"- End date: `{_date_str(end_date)}`")
    lines.append(f"- Window: `{_date_str(analysis_dates[0])}` to `{_date_str(analysis_dates[-1])}`")
    lines.append(f"- Runtime root: `{RUNTIME_ROOT}`")
    lines.append("")
    lines.append("## Daily Window")
    lines.append("")
    lines.append(_markdown_table(
        daily_metrics,
        [
            "date", "signals_total", "signals_long", "signals_short", "live_trades", "paper_trades",
            "live_coverage_pct", "paper_coverage_pct", "live_win_rate_pct", "paper_win_rate_pct",
            "live_profit_factor", "paper_profit_factor", "live_pnl_rs", "paper_pnl_rs",
            "backtest_trades", "backtest_profit_factor", "backtest_pnl_rs",
            "avg_signal_lag_sec", "avg_live_entry_lag_sec",
        ],
    ))
    lines.append("")
    lines.append("## Side Summary")
    lines.append("")
    lines.append(_markdown_table(side_rows, ["mode", "side", "trades", "win_rate_pct", "profit_factor", "sum_pnl_rs", "avg_entry_lag_sec"]))
    lines.append("")
    lines.append("## Slot Timing")
    lines.append("")
    lines.append(_markdown_table(
        slot_timing_rows.head(25),
        ["analysis_date", "slot", "side", "signals", "live_trades", "paper_trades", "avg_signal_lag_sec", "avg_live_entry_lag_sec"],
    ))
    lines.append("")
    lines.append("## Setup Scoreboard")
    lines.append("")
    lines.append(_markdown_table(
        setup_metrics.head(12),
        [
            "side", "setup", "signals", "paper_trades", "paper_win_rate_pct", "paper_profit_factor",
            "paper_pnl_rs", "live_trades", "live_win_rate_pct", "live_profit_factor", "live_pnl_rs",
            "avg_signal_lag_sec",
        ],
    ))
    lines.append("")
    lines.append("## Exact Config Knobs")
    lines.append("")
    lines.append(_markdown_table(
        knob_rows,
        ["priority_rank", "side", "setup", "field", "current_value", "trial_value", "action", "file", "note"],
    ))
    lines.append("")
    lines.append("## Missed Opportunities Vs Backtest")
    lines.append("")
    lines.append(_markdown_table(
        missed_rows.head(20),
        ["trade_date", "ticker", "side", "setup", "signal_time_ist", "outcome", "pnl_rs", "quality_score"],
    ))
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")
    for rec in recommendations:
        lines.append(f"- `{rec['priority'].upper()}` {rec['title']}: {rec['suggestion']}")
        evidence = rec.get("evidence", {})
        if evidence:
            lines.append(f"  Evidence: {', '.join(f'{k}={v}' for k, v in evidence.items())}")
    lines.append("")
    lines.append("## Sources")
    lines.append("")
    for key, value in source_info.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")
    return "\n".join(lines)


def _build_codex_brief(
    end_date: date,
    analysis_dates: Sequence[date],
    daily_metrics: pd.DataFrame,
    setup_metrics: pd.DataFrame,
    slot_timing_rows: pd.DataFrame,
    missed_rows: pd.DataFrame,
    knob_rows: pd.DataFrame,
    recommendations: Sequence[Dict[str, Any]],
    source_info: Dict[str, Any],
) -> str:
    lines: List[str] = []
    lines.append(f"# Codex Follow-up Brief - {VERSION_TAG} - {_date_str(end_date)}")
    lines.append("")
    lines.append(f"- Dates analyzed: `{_date_str(analysis_dates[0])}` to `{_date_str(analysis_dates[-1])}`")
    lines.append("")
    lines.append("## Top Evidence")
    lines.append("")
    if not daily_metrics.empty:
        latest = daily_metrics.tail(1).iloc[0].to_dict()
        lines.append(
            f"- Latest day `{latest['date']}`: signals={latest['signals_total']}, live_trades={latest['live_trades']}, paper_trades={latest['paper_trades']}, avg_signal_lag_sec={latest['avg_signal_lag_sec']}"
        )
    for _, row in setup_metrics.head(5).iterrows():
        lines.append(
            f"- {row['side']} {row['setup']}: signals={int(row['signals'])}, paper_pf={row['paper_profit_factor']}, live_pf={row['live_profit_factor']}, paper_trades={int(row['paper_trades'])}, live_trades={int(row['live_trades'])}"
        )
    if not knob_rows.empty:
        lines.append("")
        lines.append("## Exact Knobs To Consider")
        lines.append("")
        for _, row in knob_rows.head(8).iterrows():
            lines.append(
                f"- P{row['priority_rank']} {row['side']} {row['setup']}: `{row['field']}` {row['current_value']} -> {row['trial_value']} in `{row['file']}` ({row['action']}; {row['note']})"
            )
    if not missed_rows.empty:
        lines.append("")
        lines.append("## Missed Backtest Opportunities")
        lines.append("")
        for _, row in missed_rows.head(8).iterrows():
            lines.append(
                f"- {row['trade_date']} {row['side']} {row['ticker']} {row['setup']} at {row['signal_time_ist']} | backtest_outcome={row.get('outcome', '')} | pnl_rs={row.get('pnl_rs', '')}"
            )
    if not slot_timing_rows.empty:
        lines.append("")
        lines.append("## Slowest Recent Slots")
        lines.append("")
        slow = slot_timing_rows.sort_values("avg_signal_lag_sec", ascending=False).head(5)
        for _, row in slow.iterrows():
            lines.append(
                f"- {row['analysis_date']} {row['slot']} {row['side']}: signals={row['signals']} avg_signal_lag_sec={row['avg_signal_lag_sec']} avg_live_entry_lag_sec={row.get('avg_live_entry_lag_sec', '')}"
            )
    lines.append("")
    lines.append("## Suggested Questions For Codex")
    lines.append("")
    for rec in recommendations[:5]:
        lines.append(f"- {rec['title']}: {rec['suggestion']}")
    lines.append("")
    lines.append("## Source Files")
    lines.append("")
    for key, value in source_info.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")
    return "\n".join(lines)


def _build_config_knob_rows(setup_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if setup_metrics.empty:
        return pd.DataFrame()
    for _, row in setup_metrics.iterrows():
        key = (str(row["side"]), str(row["setup"]))
        hints = CONFIG_HINTS.get(key, [])
        if not hints:
            continue
        for hint in hints:
            rows.append(
                {
                    "side": row["side"],
                    "setup": row["setup"],
                    "field": hint["field"],
                    "file": hint["file"],
                    "action": hint["action"],
                    "note": hint["note"],
                    "current_value": hint.get("current_value", ""),
                    "trial_value": hint.get("trial_value", ""),
                    "priority_rank": int(hint.get("priority_hint", 50)),
                    "signals": _safe_int(row["signals"]),
                    "live_trades": _safe_int(row["live_trades"]),
                    "paper_trades": _safe_int(row["paper_trades"]),
                    "live_profit_factor": _display_metric(row["live_profit_factor"]),
                    "paper_profit_factor": _display_metric(row["paper_profit_factor"]),
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        by=["priority_rank", "paper_profit_factor", "live_profit_factor"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def build_advisor_report(end_date: date, lookback_days: int, output_dir: Path) -> Dict[str, Any]:
    analysis_dates = _choose_analysis_dates(end_date, lookback_days)
    signals_frames: List[pd.DataFrame] = []
    live_frames: List[pd.DataFrame] = []
    paper_frames: List[pd.DataFrame] = []

    for day in analysis_dates:
        signals_df = _load_signals_for_date(day)
        live_df = _load_trades_for_date(day, "LIVE")
        paper_df = _load_trades_for_date(day, "PAPER")
        if not signals_df.empty:
            signals_frames.append(signals_df)
        if not live_df.empty:
            live_frames.append(live_df)
        if not paper_df.empty:
            paper_frames.append(paper_df)

    signals_all = pd.concat(signals_frames, ignore_index=True) if signals_frames else pd.DataFrame()
    live_all = pd.concat(live_frames, ignore_index=True) if live_frames else pd.DataFrame()
    paper_all = pd.concat(paper_frames, ignore_index=True) if paper_frames else pd.DataFrame()
    backtest_all, backtest_trades_path, backtest_daywise_path = _load_backtest_recent(analysis_dates)

    daily_rows: List[Dict[str, Any]] = []
    for day in analysis_dates:
        signals_df = signals_all[signals_all["analysis_date"] == _date_str(day)].copy() if not signals_all.empty else pd.DataFrame()
        live_df = live_all[live_all["analysis_date"] == _date_str(day)].copy() if not live_all.empty else pd.DataFrame()
        paper_df = paper_all[paper_all["analysis_date"] == _date_str(day)].copy() if not paper_all.empty else pd.DataFrame()
        backtest_day_df = backtest_all[backtest_all["trade_date"] == day].copy() if not backtest_all.empty else pd.DataFrame()
        daily_rows.append(_daily_trade_row(day, signals_df, live_df, paper_df, backtest_day_df))

    daily_metrics = pd.DataFrame(daily_rows)
    setup_metrics = _aggregate_setup_metrics(signals_all, live_all, paper_all, backtest_all)
    side_rows = pd.DataFrame(_side_summary(live_all, "LIVE") + _side_summary(paper_all, "PAPER"))
    slot_timing_rows = _build_slot_timing_rows(signals_all, live_all, paper_all)
    missed_rows = _build_missed_opportunity_rows(signals_all, backtest_all)
    knob_rows = _build_config_knob_rows(setup_metrics)
    recommendations = _generate_recommendations(setup_metrics, daily_metrics, end_date)

    source_info = {
        "live_signals_dir": LIVE_SIGNALS_DIR,
        "latest_backtest_trades_csv": backtest_trades_path,
        "latest_backtest_daywise_csv": backtest_daywise_path,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    report_json_path = output_dir / f"advisor_report_{_date_str(end_date)}.json"
    report_md_path = output_dir / f"advisor_report_{_date_str(end_date)}.md"
    codex_brief_path = output_dir / f"codex_brief_{_date_str(end_date)}.md"
    daily_csv_path = output_dir / f"daily_metrics_{_date_str(end_date)}.csv"
    setup_csv_path = output_dir / f"setup_metrics_{_date_str(end_date)}.csv"
    knob_csv_path = output_dir / f"config_knobs_{_date_str(end_date)}.csv"
    slot_csv_path = output_dir / f"slot_timing_{_date_str(end_date)}.csv"
    missed_csv_path = output_dir / f"missed_opportunities_{_date_str(end_date)}.csv"

    payload = {
        "generated_at": datetime.now().isoformat(),
        "version_tag": VERSION_TAG,
        "end_date": _date_str(end_date),
        "analysis_dates": [_date_str(d) for d in analysis_dates],
        "source_info": source_info,
        "summary": {
            "signals_total": int(len(signals_all)),
            "live_trades_total": int(len(live_all)),
            "paper_trades_total": int(len(paper_all)),
            "setups_observed": int(len(setup_metrics)),
        },
        "daily_metrics": daily_metrics.to_dict(orient="records"),
        "side_summary": side_rows.to_dict(orient="records"),
        "setup_metrics": setup_metrics.to_dict(orient="records"),
        "slot_timing": slot_timing_rows.to_dict(orient="records"),
        "missed_opportunities": missed_rows.to_dict(orient="records"),
        "config_knobs": knob_rows.to_dict(orient="records"),
        "recommendations": recommendations,
    }

    report_json_path.write_text(json.dumps(_json_ready(payload), indent=2), encoding="utf-8")
    report_md_path.write_text(
        _build_report_markdown(end_date, analysis_dates, daily_metrics, setup_metrics, side_rows, slot_timing_rows, missed_rows, knob_rows, recommendations, source_info),
        encoding="utf-8",
    )
    codex_brief_path.write_text(
        _build_codex_brief(end_date, analysis_dates, daily_metrics, setup_metrics, slot_timing_rows, missed_rows, knob_rows, recommendations, source_info),
        encoding="utf-8",
    )
    daily_metrics.to_csv(daily_csv_path, index=False)
    setup_metrics.to_csv(setup_csv_path, index=False)
    knob_rows.to_csv(knob_csv_path, index=False)
    slot_timing_rows.to_csv(slot_csv_path, index=False)
    missed_rows.to_csv(missed_csv_path, index=False)

    payload["output_paths"] = {
        "json": report_json_path,
        "markdown": report_md_path,
        "codex_brief": codex_brief_path,
        "daily_csv": daily_csv_path,
        "setup_csv": setup_csv_path,
        "config_knobs_csv": knob_csv_path,
        "slot_timing_csv": slot_csv_path,
        "missed_opportunities_csv": missed_csv_path,
    }
    return payload


def main() -> None:
    default_end_date = date.today()
    default_output_root = report_subdir("codex_post_trade_advisor_v15_new")

    ap = argparse.ArgumentParser(description="Post-market Codex advisor for v15_new live, paper, and backtest outputs.")
    ap.add_argument("--end-date", default=_date_str(default_end_date))
    ap.add_argument("--days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    ap.add_argument("--output-dir", default=str(default_output_root))
    args = ap.parse_args()

    end_date = _parse_date(args.end_date)
    payload = build_advisor_report(
        end_date=end_date,
        lookback_days=max(1, int(args.days)),
        output_dir=Path(args.output_dir),
    )

    print("[ADVISOR] Codex post-trade advisor ready", flush=True)
    print(f"[ADVISOR] end_date={payload['end_date']} | analysis_days={len(payload['analysis_dates'])}", flush=True)
    print(
        f"[ADVISOR] signals={payload['summary']['signals_total']} | live_trades={payload['summary']['live_trades_total']} | paper_trades={payload['summary']['paper_trades_total']}",
        flush=True,
    )
    for key, path in payload["output_paths"].items():
        print(f"[ADVISOR] {key}={path}", flush=True)


if __name__ == "__main__":
    main()
