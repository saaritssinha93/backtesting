"""
V7 ID 5-min live research layer.

Builds a non-trading daily truth table from the existing live pipeline:

  signal_discovery raw/gated candidates
  entry_engine audit rows and reject rows
  live signal CSVs
  paper trade results

Outputs:
  C:\\TradingData\\eqidv2\\live_research_v7_research_layer\\truth_table\\truth_table_YYYY-MM-DD.csv
  C:\\TradingData\\eqidv2\\live_research_v7_research_layer\\reports\\reality_gap_YYYY-MM-DD.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from eqidv2_runtime_paths import (
    DATA_1MIN_DIR,
    DATA_5M_DIR,
    LIVE_SIGNALS_DIR,
    NIFTY_OPEN_SLOT_DIR,
    NIFTY_SLOT_FAIL_DIR,
    NIFTY_SLOT_READY_DIR,
    RUNTIME_STATUS_DIR,
    runtime_dir,
)
import avwap_5min_ID_v7_candidate_scan as candidate_scan
import eqidv2_v11_live_overlay as v11_live_overlay
from eqidv2_v7_light_ops import run_light_ops


RESEARCH_ROOT = runtime_dir("live_research_v7_research_layer")
TRUTH_DIR = RESEARCH_ROOT / "truth_table"
REPORT_DIR = RESEARCH_ROOT / "reports"
LATEST_DIR = RESEARCH_ROOT / "latest"
HEARTBEAT_DIR = RESEARCH_ROOT / "heartbeat"
RANKER_DIR = RESEARCH_ROOT / "ranker"
SUGGESTIONS_DIR = RESEARCH_ROOT / "suggestions"
EXIT_LAB_DIR = RESEARCH_ROOT / "exit_lab"
OPS_DIR = RESEARCH_ROOT / "ops_audit"
DEEP_ANALYSIS_DIR = RESEARCH_ROOT / "deep_analysis"

SIGNAL_DISCOVERY_CSV_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "csv")
ENTRY_AUDIT_DIR = runtime_dir("entry_engine_1min_v5_ID", "audit")
DATA_5M_LIVE_DIR = runtime_dir("stocks_indicators_5min_eq_live")
NIFTY_5M_PARQUET = DATA_5M_LIVE_DIR / "NIFTYBEES_stocks_indicators_5min.parquet"
NIFTY_STATUS_FILE = REPO_ROOT / "logs" / "nifty_guard_fetcher_v16_5min.status"
LIVE_5MIN_STATUS_JSON = REPO_ROOT / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"
LIVE_5MIN_SUPERVISOR_STATUS = REPO_ROOT / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.status"
LIVE_5MIN_SLOT_READY_DIR = runtime_dir("slot_ready_5m")
BACKTEST_RESULT_V11_ROOT = runtime_dir("backtesting_result_v11")


def _v7_live_setup_universe() -> list[str]:
    """Setups the current v7 live scanner can emit into the research layer."""
    blocked_early = {
        str(x).upper().strip()
        for x in getattr(candidate_scan, "EARLY_BLOCKED_SETUPS", set())
        if str(x).strip()
    }
    allowed = {
        str(x).upper().strip()
        for x in getattr(candidate_scan, "ALLOWED_SETUPS", set())
        if str(x).strip()
    }
    excluded = {
        str(x).upper().strip()
        for x in getattr(candidate_scan, "EXCLUDED_SETUPS", set())
        if str(x).strip()
    }
    v11_overlay = {
        str(x).upper().strip()
        for x in v11_live_overlay.v11_override_setup_universe(v11_live_overlay.DEFAULT_SELECTED_STRATEGY_PROFILE)
        if str(x).strip()
    }
    return sorted((allowed - excluded - blocked_early) | v11_overlay)


V7_LIVE_SETUP_UNIVERSE = _v7_live_setup_universe()


for _p in (RESEARCH_ROOT, TRUTH_DIR, REPORT_DIR, LATEST_DIR, HEARTBEAT_DIR, RANKER_DIR, SUGGESTIONS_DIR, EXIT_LAB_DIR, OPS_DIR, DEEP_ANALYSIS_DIR):
    _p.mkdir(parents=True, exist_ok=True)


DEEP_ANALYSIS_NUMERIC_FIELDS = [
    "signal_minute",
    "upper_wick_pct",
    "lower_wick_pct",
    "wick_skew_pct",
    "signal_range_pct",
    "market_abs_ret_pct",
    "pre_bars",
    "pre1_mom_r",
    "pre2_mom_r",
    "pre3_mom_r",
    "pre5_mom_r",
    "pre10_mom_r",
    "pre15_mom_r",
    "pre3_close_pos",
    "pre3_dir_count",
    "pre3_body_sum_r",
    "pre3_range_r",
    "pre3_vol_ratio20",
    "pre5_close_pos",
    "pre5_dir_count",
    "pre5_body_sum_r",
    "pre5_range_r",
    "pre5_vol_ratio20",
    "pre10_close_pos",
    "pre10_dir_count",
    "pre10_body_sum_r",
    "pre10_range_r",
    "pre10_vol_ratio20",
    "pre15_close_pos",
    "pre15_dir_count",
    "pre15_body_sum_r",
    "pre15_range_r",
    "pre15_vol_ratio20",
    "pre1_body_r",
    "pre1_close_pos",
    "pre1_range_r",
    "pre1_dir",
    "pre1_adx",
    "pre1_rsi_dir",
    "pre_entry_momentum_score",
    "sig5_body_r",
    "sig5_range_r",
    "sig5_close_pos",
    "sig5_adx_calc",
    "sig5_rsi_dir",
    "sig5_vol_ratio20",
]

DEEP_ANALYSIS_TEXT_FIELDS = [
    "pre_momentum_cutoff_ist",
    "pre_momentum_gate_version",
    "pre_momentum_gate_rule",
    "pre_momentum_gate_pass",
    "pre_momentum_gate_reason",
    "v11_live_entry_overlay_status",
    "v11_live_entry_overlay_version",
]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.DataFrame()


def _read_many(paths: list[Path]) -> pd.DataFrame:
    parts = [_read_csv(p) for p in paths]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True, sort=False)


def _normalise_ts(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _fmt_ts(value: Any) -> str:
    ts = _normalise_ts(value)
    if pd.isna(ts):
        return ""
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _candidate_key(ticker: Any, side: Any, setup: Any, signal_time: Any) -> str:
    return "|".join(
        [
            str(ticker or "").upper().strip(),
            str(side or "").upper().strip(),
            str(setup or "").strip(),
            _fmt_ts(signal_time),
        ]
    )


def _add_key(
    df: pd.DataFrame,
    *,
    time_col: str,
    key_col: str = "_research_key",
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ("ticker", "side", "setup"):
        if col not in out.columns:
            out[col] = ""
    if time_col not in out.columns:
        out[time_col] = ""
    out[key_col] = [
        _candidate_key(row.get("ticker"), row.get("side"), row.get("setup"), row.get(time_col))
        for _, row in out.iterrows()
    ]
    return out


def _first_nonblank(row: pd.Series, cols: list[str], default: Any = "") -> Any:
    for col in cols:
        if col in row.index:
            value = row.get(col)
            if pd.notna(value) and str(value) != "":
                return value
    return default


def _first_from_rows(rows: list[pd.Series], cols: list[str], default: Any = "") -> Any:
    for row in rows:
        if row is None or row.empty:
            continue
        value = _first_nonblank(row, cols, default=None)
        if value is not None and pd.notna(value) and str(value) != "":
            return value
    return default


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _bool_series(df: pd.DataFrame, col: str, default: bool = False) -> pd.Series:
    if df.empty or col not in df.columns:
        return pd.Series(default, index=df.index, dtype=bool)
    raw = df[col]
    if pd.api.types.is_bool_dtype(raw):
        return raw.fillna(default).astype(bool)
    text = raw.fillna("").astype(str).str.strip().str.lower()
    true_values = {"1", "true", "t", "yes", "y"}
    false_values = {"0", "false", "f", "no", "n", ""}
    out = text.map(lambda v: True if v in true_values else (False if v in false_values else default))
    return out.astype(bool)


def _load_raw_candidates(day: str) -> pd.DataFrame:
    path = SIGNAL_DISCOVERY_CSV_DIR / f"raw_candidate_tickers_{day}.csv"
    df = _read_csv(path)
    if df.empty:
        return df
    df = _add_key(df, time_col="signal_time_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_gated_candidates(day: str) -> pd.DataFrame:
    path = SIGNAL_DISCOVERY_CSV_DIR / f"candidate_tickers_{day}.csv"
    df = _read_csv(path)
    if df.empty:
        return df
    df = _add_key(df, time_col="signal_time_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_research_filter_rejections(day: str) -> pd.DataFrame:
    path = SIGNAL_DISCOVERY_CSV_DIR / f"research_filter_rejected_candidate_tickers_{day}.csv"
    df = _read_csv(path)
    if df.empty:
        return df
    df = _add_key(df, time_col="signal_time_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_v11_overlay_candidates(day: str) -> pd.DataFrame:
    path = SIGNAL_DISCOVERY_CSV_DIR / f"v11_overlay_candidate_tickers_{day}.csv"
    df = _read_csv(path)
    if df.empty:
        return df
    df = _add_key(df, time_col="signal_time_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_v11_overlay_rejections(day: str) -> pd.DataFrame:
    path = SIGNAL_DISCOVERY_CSV_DIR / f"v11_overlay_rejected_candidate_tickers_{day}.csv"
    df = _read_csv(path)
    if df.empty:
        return df
    df = _add_key(df, time_col="signal_time_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_entry_rows(day: str, raw: bool = False) -> pd.DataFrame:
    compact = day.replace("-", "")
    prefix = "entry_rows_raw_candidates" if raw else "entry_rows"
    paths = sorted(ENTRY_AUDIT_DIR.glob(f"{prefix}_{compact}_*.csv"))
    df = _read_many(paths)
    if df.empty:
        return df
    df = _add_key(df, time_col="bar_time_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_entry_rejects(day: str) -> pd.DataFrame:
    compact = day.replace("-", "")
    paths = sorted(ENTRY_AUDIT_DIR.glob(f"entry_rejected_candidates_{compact}_*.csv"))
    df = _read_many(paths)
    if df.empty:
        return df
    df = _add_key(df, time_col="signal_time_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_live_signals(day: str) -> pd.DataFrame:
    parts = []
    for side in ("short", "long"):
        path = LIVE_SIGNALS_DIR / f"signals_{day}_id_5min_v7_{side}.csv"
        df = _read_csv(path)
        if not df.empty:
            parts.append(df)
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True, sort=False)
    if "signal_entry_datetime_ist" not in df.columns:
        df["signal_entry_datetime_ist"] = df.get("signal_datetime", "")
    df = _add_key(df, time_col="signal_entry_datetime_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _load_paper_trades(day: str) -> pd.DataFrame:
    path = LIVE_SIGNALS_DIR / f"paper_trades_{day}_id_5min_v7.csv"
    df = _read_csv(path)
    if df.empty:
        return df
    if "signal_entry_datetime_ist" not in df.columns:
        df["signal_entry_datetime_ist"] = df.get("signal_datetime", "")
    df = _add_key(df, time_col="signal_entry_datetime_ist")
    return df.drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)


def _map_by_key(df: pd.DataFrame) -> pd.DataFrame:
    return df.set_index("_research_key", drop=False) if not df.empty and "_research_key" in df.columns else pd.DataFrame()


def _map_by_signal_id(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "signal_id" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["signal_id"] = out["signal_id"].astype(str)
    out = out.loc[out["signal_id"].str.len() > 0]
    if out.empty:
        return pd.DataFrame()
    return out.drop_duplicates(subset=["signal_id"], keep="last").set_index("signal_id", drop=False)


def _row_from_map(map_df: pd.DataFrame, key: str) -> pd.Series:
    if map_df.empty or key not in map_df.index:
        return pd.Series(dtype=object)
    row = map_df.loc[key]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return row


def _row_by_signal_id(map_df: pd.DataFrame, signal_id: Any) -> pd.Series:
    sid = str(signal_id or "")
    if map_df.empty or not sid or sid not in map_df.index:
        return pd.Series(dtype=object)
    row = map_df.loc[sid]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return row


def _all_research_keys(*frames: pd.DataFrame) -> list[str]:
    keys: set[str] = set()
    for frame in frames:
        if not frame.empty and "_research_key" in frame.columns:
            keys.update(str(k) for k in frame["_research_key"].dropna().tolist() if str(k))
    return sorted(keys)


def _signal_delay_seconds(signal_time: Any, created_at: Any) -> float:
    signal_ts = _normalise_ts(signal_time)
    created_ts = _normalise_ts(created_at)
    if pd.isna(signal_ts) or pd.isna(created_ts):
        return np.nan
    return float((created_ts - signal_ts).total_seconds())


def _entry_delay_seconds(signal_time: Any, entry_time: Any) -> float:
    signal_ts = _normalise_ts(signal_time)
    entry_ts = _normalise_ts(entry_time)
    if pd.isna(signal_ts) or pd.isna(entry_ts):
        return np.nan
    return float((entry_ts - signal_ts).total_seconds())


_PRICE_CACHE: dict[str, pd.DataFrame] = {}


def _price_path(ticker: Any) -> Path | None:
    symbol = str(ticker or "").upper().strip()
    if not symbol:
        return None
    name = f"{symbol}_stocks_indicators_5min.parquet"
    for base in (DATA_5M_LIVE_DIR, DATA_5M_DIR):
        path = base / name
        if path.exists():
            return path
    return None


def _price_bars(ticker: Any) -> pd.DataFrame:
    symbol = str(ticker or "").upper().strip()
    if symbol in _PRICE_CACHE:
        return _PRICE_CACHE[symbol]
    path = _price_path(symbol)
    if path is None:
        _PRICE_CACHE[symbol] = pd.DataFrame()
        return _PRICE_CACHE[symbol]
    try:
        df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
    except Exception:
        df = pd.DataFrame()
    if not df.empty:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if getattr(df["date"].dt, "tz", None) is None:
            df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
        else:
            df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    _PRICE_CACHE[symbol] = df
    return df


def _forward_outcome(ticker: Any, side: Any, signal_time: Any, entry_price: Any, bars: int = 3) -> dict[str, Any]:
    signal_ts = _normalise_ts(signal_time)
    ref = _safe_float(entry_price)
    if pd.isna(signal_ts) or not np.isfinite(ref) or ref <= 0:
        return {}
    prices = _price_bars(ticker)
    if prices.empty:
        return {}
    future = prices.loc[prices["date"] > signal_ts].head(max(1, int(bars))).copy()
    if future.empty:
        return {}
    side_norm = str(side or "").upper().strip()
    hi = pd.to_numeric(future["high"], errors="coerce").max()
    lo = pd.to_numeric(future["low"], errors="coerce").min()
    close_n = _safe_float(future.iloc[-1].get("close"))
    if side_norm == "SHORT":
        mfe = (ref - lo) / ref * 100.0 if np.isfinite(lo) else np.nan
        mae = (hi - ref) / ref * 100.0 if np.isfinite(hi) else np.nan
        close_ret = (ref - close_n) / ref * 100.0 if np.isfinite(close_n) else np.nan
    else:
        mfe = (hi - ref) / ref * 100.0 if np.isfinite(hi) else np.nan
        mae = (ref - lo) / ref * 100.0 if np.isfinite(lo) else np.nan
        close_ret = (close_n - ref) / ref * 100.0 if np.isfinite(close_n) else np.nan
    return {
        "forward_bars": int(len(future)),
        "forward_last_time_ist": _fmt_ts(future.iloc[-1].get("date")),
        "forward_mfe_pct": mfe,
        "forward_mae_pct": mae,
        "forward_close_ret_pct": close_ret,
    }


def _build_reason(
    *,
    passed_gate: bool,
    entry_raw: bool,
    entry_selected: bool,
    live_written: bool,
    paper_traded: bool,
    reject_reason: str,
    research_filter_reason: str = "",
) -> str:
    if paper_traded:
        return ""
    if not passed_gate:
        if research_filter_reason:
            return research_filter_reason
        return "rejected_v8_gate"
    if reject_reason:
        return reject_reason
    if not entry_raw:
        return "no_entry_row"
    if not entry_selected:
        return "entry_deduped_or_not_selected"
    if not live_written:
        return "not_written_to_live_signal_csv"
    return "live_signal_not_paper_executed"


def build_truth_table(day: str) -> pd.DataFrame:
    raw = _load_raw_candidates(day)
    gated = _load_gated_candidates(day)
    research_filter_rejects = _load_research_filter_rejections(day)
    v11_overlay = _load_v11_overlay_candidates(day)
    v11_overlay_rejects = _load_v11_overlay_rejections(day)
    entry_raw = _load_entry_rows(day, raw=True)
    entry_selected = _load_entry_rows(day, raw=False)
    rejects = _load_entry_rejects(day)
    live = _load_live_signals(day)
    paper = _load_paper_trades(day)

    if raw.empty:
        raw = _read_many([]) if gated.empty and v11_overlay.empty and v11_overlay_rejects.empty else pd.concat(
            [df for df in (gated, v11_overlay, v11_overlay_rejects) if not df.empty],
            ignore_index=True,
            sort=False,
        ).drop_duplicates(subset=["_research_key"], keep="last").reset_index(drop=True)
    if raw.empty:
        return pd.DataFrame()

    raw_map = _map_by_key(raw)
    gated_map = _map_by_key(gated)
    research_filter_map = _map_by_key(research_filter_rejects)
    v11_overlay_map = _map_by_key(v11_overlay)
    v11_overlay_reject_map = _map_by_key(v11_overlay_rejects)
    entry_raw_map = _map_by_key(entry_raw)
    entry_sel_map = _map_by_key(entry_selected)
    rejects_map = _map_by_key(rejects)
    live_map = _map_by_key(live)
    paper_map = _map_by_key(paper)
    paper_signal_map = _map_by_signal_id(paper)

    rows: list[dict[str, Any]] = []
    for key in _all_research_keys(
        raw,
        gated,
        research_filter_rejects,
        v11_overlay,
        v11_overlay_rejects,
        entry_raw,
        entry_selected,
        rejects,
        live,
        paper,
    ):
        cand = _row_from_map(raw_map, key)
        gated_row = _row_from_map(gated_map, key)
        research_filter_row = _row_from_map(research_filter_map, key)
        v11_overlay_row = _row_from_map(v11_overlay_map, key)
        v11_overlay_reject_row = _row_from_map(v11_overlay_reject_map, key)
        entry_raw_row = _row_from_map(entry_raw_map, key)
        entry_sel_row = _row_from_map(entry_sel_map, key)
        reject_row = _row_from_map(rejects_map, key)
        live_row = _row_from_map(live_map, key)
        paper_row = _row_from_map(paper_map, key)
        signal_id = live_row.get("signal_id", "") if not live_row.empty else ""
        paper_by_signal = _row_by_signal_id(paper_signal_map, signal_id)
        if not paper_by_signal.empty:
            paper_row = paper_by_signal

        base_row = cand
        for fallback in (v11_overlay_row, v11_overlay_reject_row, gated_row, entry_raw_row, entry_sel_row, live_row, paper_row):
            if base_row.empty and not fallback.empty:
                base_row = fallback
        candidate_row = cand
        if candidate_row.empty:
            candidate_row = v11_overlay_row if not v11_overlay_row.empty else v11_overlay_reject_row

        has_entry_raw = not entry_raw_row.empty
        audit_entry_selected = not entry_sel_row.empty
        live_written = not live_row.empty
        paper_traded = not paper_row.empty
        has_entry_selected = audit_entry_selected or live_written or paper_traded
        passed_gate = not gated_row.empty or not v11_overlay_row.empty or live_written or paper_traded
        if not v11_overlay_row.empty:
            gate_source = "v11_backtesting_overlay_passed"
        elif not v11_overlay_reject_row.empty:
            gate_source = "v11_backtesting_overlay_rejected"
        elif not gated_row.empty:
            gate_source = "accepted_rules_csv+research_live_filters"
        elif not research_filter_row.empty:
            gate_source = "research_live_filter_rejected"
        elif live_written:
            gate_source = "live_signal_reconciled"
        elif paper_traded:
            gate_source = "paper_trade_reconciled"
        else:
            gate_source = ""
        selection_source = "entry_audit" if audit_entry_selected else ("live_signal_reconciled" if live_written else ("paper_trade_reconciled" if paper_traded else ""))
        reject_reason = str(reject_row.get("reject_reason", "")) if not reject_row.empty else ""
        research_filter_reason = str(research_filter_row.get("research_live_filter_reason", "")) if not research_filter_row.empty else ""
        v11_reject_reason = str(v11_overlay_reject_row.get("v11_selected_strategy_reject_reason", "")) if not v11_overlay_reject_row.empty else ""
        if v11_reject_reason and not research_filter_reason:
            research_filter_reason = v11_reject_reason

        signal_time = _first_nonblank(
            base_row,
            ["signal_time_ist", "bar_time_ist", "signal_entry_datetime_ist", "signal_datetime"],
            "",
        )
        signal_close = _safe_float(_first_nonblank(candidate_row, ["signal_close", "signal_price", "entry_price"], np.nan))
        entry_price = _safe_float(_first_nonblank(entry_sel_row, ["entry_price"], np.nan))
        if not np.isfinite(entry_price):
            entry_price = _safe_float(_first_nonblank(entry_raw_row, ["entry_price"], np.nan))
        if not np.isfinite(entry_price):
            entry_price = _safe_float(_first_nonblank(live_row, ["entry_price"], np.nan))
        paper_entry = _safe_float(_first_nonblank(paper_row, ["entry_price"], np.nan))
        effective_entry = paper_entry if np.isfinite(paper_entry) else entry_price
        if not np.isfinite(signal_close):
            signal_close = effective_entry
        entry_distance_pct = (
            abs(effective_entry - signal_close) / signal_close * 100.0
            if np.isfinite(effective_entry) and np.isfinite(signal_close) and signal_close > 0
            else np.nan
        )
        fwd = _forward_outcome(
            _first_nonblank(base_row, ["ticker"], ""),
            _first_nonblank(base_row, ["side"], ""),
            signal_time,
            signal_close,
            bars=3,
        )
        indicator_sources = [entry_sel_row, reject_row, entry_raw_row, gated_row, research_filter_row, candidate_row]
        deep_numeric = {
            col: _safe_float(_first_from_rows(indicator_sources, [col], np.nan))
            for col in DEEP_ANALYSIS_NUMERIC_FIELDS
        }
        deep_text = {
            col: _first_from_rows(indicator_sources, [col], "")
            for col in DEEP_ANALYSIS_TEXT_FIELDS
        }

        rows.append(
            {
                "date": day,
                "research_key": key,
                "candidate_id": _first_nonblank(base_row, ["candidate_id"], key),
                "ticker": _first_nonblank(base_row, ["ticker"], ""),
                "side": _first_nonblank(base_row, ["side"], ""),
                "setup": _first_nonblank(base_row, ["setup"], ""),
                "signal_time_ist": _fmt_ts(signal_time),
                "scan_slot_ist": candidate_row.get("scan_slot_ist", "") if not candidate_row.empty else "",
                "selection_mode": _first_nonblank(base_row, ["selection_mode"], ""),
                "candidate_family": _first_nonblank(base_row, ["candidate_family"], ""),
                "scan_created_at_ist": candidate_row.get("created_at_ist", "") if not candidate_row.empty else "",
                "signal_delay_seconds": _signal_delay_seconds(signal_time, candidate_row.get("created_at_ist", "") if not candidate_row.empty else ""),
                "signal_close": signal_close,
                "quality_score": _safe_float(_first_nonblank(base_row, ["quality_score", "score"], np.nan)),
                "rs_pct": _safe_float(candidate_row.get("rs_pct")) if not candidate_row.empty else np.nan,
                "market_ret_pct": _safe_float(candidate_row.get("market_ret_pct")) if not candidate_row.empty else np.nan,
                "regime": candidate_row.get("regime", "") if not candidate_row.empty else "",
                "vol_ratio": _safe_float(candidate_row.get("vol_ratio")) if not candidate_row.empty else np.nan,
                "atr_pct": _safe_float(candidate_row.get("atr_pct")) if not candidate_row.empty else np.nan,
                "body_pct": _safe_float(candidate_row.get("body_pct")) if not candidate_row.empty else np.nan,
                "close_loc": _safe_float(candidate_row.get("close_loc")) if not candidate_row.empty else np.nan,
                "vwap_dist_atr": _safe_float(candidate_row.get("vwap_dist_atr")) if not candidate_row.empty else np.nan,
                "candidate_reason": candidate_row.get("reason", "") if not candidate_row.empty else "",
                "passed_v8_gate": passed_gate,
                "v8_gate_source": gate_source,
                "v8_live_gate_rule": gated_row.get("v8_live_gate_rule", "") if not gated_row.empty else "",
                "v8_live_gate_stage": gated_row.get("v8_live_gate_stage", "") if not gated_row.empty else "",
                "research_live_filter_status": _first_nonblank(
                    gated_row,
                    ["research_live_filter_status"],
                    _first_nonblank(
                        research_filter_row,
                        ["research_live_filter_status"],
                        "V11_BACKTESTING_REJECTED" if not v11_overlay_reject_row.empty else (candidate_row.get("research_live_filter_status", "") if not candidate_row.empty else ""),
                    ),
                ),
                "research_live_filter_reason": _first_nonblank(
                    gated_row,
                    ["research_live_filter_reason"],
                    _first_nonblank(
                        research_filter_row,
                        ["research_live_filter_reason"],
                        research_filter_reason if research_filter_reason else (candidate_row.get("research_live_filter_reason", "") if not candidate_row.empty else ""),
                    ),
                ),
                "v11_live_overlay_status": _first_nonblank(v11_overlay_row, ["v11_live_overlay_status"], _first_nonblank(v11_overlay_reject_row, ["v11_live_overlay_status"], "")),
                "v11_selected_strategy_profile": _first_nonblank(v11_overlay_row, ["v11_selected_strategy_profile"], _first_nonblank(v11_overlay_reject_row, ["v11_selected_strategy_profile"], "")),
                "v11_selected_strategy_rule": _first_from_rows([v11_overlay_row, entry_sel_row, reject_row, entry_raw_row], ["v11_selected_strategy_rule"], ""),
                "v11_selected_strategy_reject_reason": v11_reject_reason,
                "scanner_ranker_score": _safe_float(_first_nonblank(gated_row, ["ranker_score"], _first_nonblank(research_filter_row, ["ranker_score"], _first_nonblank(candidate_row, ["ranker_score"], np.nan)))),
                "entry_row_built": has_entry_raw,
                "entry_audit_selected": audit_entry_selected,
                "entry_selected": has_entry_selected,
                "entry_selection_source": selection_source,
                "entry_time_ist": _first_nonblank(entry_sel_row, ["entry_time_ist"], _first_nonblank(live_row, ["signal_entry_datetime_ist", "signal_datetime"], "")),
                "entry_delay_seconds": _entry_delay_seconds(signal_time, _first_nonblank(entry_sel_row, ["entry_time_ist"], "")),
                "entry_price_model": entry_price,
                "sl_price_model": _safe_float(_first_nonblank(entry_sel_row, ["sl_price"], _first_nonblank(live_row, ["stop_price"], np.nan))),
                "target_price_model": _safe_float(_first_nonblank(entry_sel_row, ["target_price"], _first_nonblank(live_row, ["target_price"], np.nan))),
                "sl_pct": _safe_float(_first_nonblank(entry_sel_row, ["sl_pct"], np.nan)),
                "target_pct": _safe_float(_first_nonblank(entry_sel_row, ["target_pct"], np.nan)),
                "entry_reject_reason": reject_reason,
                "live_signal_written": live_written,
                "live_signal_id": signal_id,
                "paper_traded": paper_traded,
                "paper_trade_id": paper_row.get("trade_id", "") if not paper_row.empty else "",
                "paper_signal_id": paper_row.get("signal_id", "") if not paper_row.empty else "",
                "paper_entry_time": paper_row.get("entry_time", "") if not paper_row.empty else "",
                "paper_exit_time": paper_row.get("exit_time", "") if not paper_row.empty else "",
                "paper_quantity": _safe_float(paper_row.get("quantity", np.nan)) if not paper_row.empty else np.nan,
                "paper_entry_price": paper_entry,
                "paper_exit_price": _safe_float(paper_row.get("exit_price", np.nan)) if not paper_row.empty else np.nan,
                "paper_outcome": paper_row.get("outcome", "") if not paper_row.empty else "",
                "paper_pnl_rs": _safe_float(paper_row.get("pnl_rs", np.nan)) if not paper_row.empty else np.nan,
                "paper_pnl_pct": _safe_float(paper_row.get("pnl_pct", np.nan)) if not paper_row.empty else np.nan,
                "entry_distance_pct": entry_distance_pct,
                "forward_bars": fwd.get("forward_bars", np.nan),
                "forward_last_time_ist": fwd.get("forward_last_time_ist", ""),
                "forward_mfe_pct": fwd.get("forward_mfe_pct", np.nan),
                "forward_mae_pct": fwd.get("forward_mae_pct", np.nan),
                "forward_close_ret_pct": fwd.get("forward_close_ret_pct", np.nan),
                **deep_numeric,
                **deep_text,
                "reason_not_taken": _build_reason(
                    passed_gate=passed_gate,
                    entry_raw=has_entry_raw,
                    entry_selected=has_entry_selected,
                    live_written=live_written,
                    paper_traded=paper_traded,
                    reject_reason=reject_reason,
                    research_filter_reason=research_filter_reason,
                ),
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(["signal_time_ist", "quality_score", "ticker"], ascending=[True, False, True])
    return out.reset_index(drop=True)


def _fmt_num(value: Any, digits: int = 2) -> str:
    val = _safe_float(value)
    if not np.isfinite(val):
        return "NA"
    return f"{val:,.{digits}f}"


def _summary_counts(truth: pd.DataFrame) -> dict[str, Any]:
    if truth.empty:
        return {}
    pnl = pd.to_numeric(truth.get("paper_pnl_rs", 0.0), errors="coerce").fillna(0.0)
    traded = truth["paper_traded"].astype(bool)
    audit_selected = truth.get("entry_audit_selected", truth["entry_selected"]).astype(bool)
    raw_mask = truth.get("scan_created_at_ist", pd.Series("", index=truth.index)).fillna("").astype(str).str.len() > 0
    v11_status = truth.get("v11_live_overlay_status", pd.Series("", index=truth.index)).fillna("").astype(str).str.upper()
    return {
        "research_rows_total": int(len(truth)),
        "raw_candidates": int(raw_mask.sum()),
        "v11_overlay_passed": int((v11_status == "PASSED").sum()),
        "v11_overlay_rejected": int((v11_status == "REJECTED").sum()),
        "passed_v8_gate": int(truth["passed_v8_gate"].sum()),
        "entry_rows": int(truth["entry_row_built"].sum()),
        "selected_entries": int(truth["entry_selected"].sum()),
        "audit_selected_entries": int(audit_selected.sum()),
        "live_signals": int(truth["live_signal_written"].sum()),
        "paper_trades": int(traded.sum()),
        "paper_pnl_rs": float(pnl[traded].sum()),
        "avg_entry_delay_sec": float(pd.to_numeric(truth.loc[truth["entry_selected"], "entry_delay_seconds"], errors="coerce").mean()),
        "avg_entry_distance_pct": float(pd.to_numeric(truth.loc[truth["paper_traded"], "entry_distance_pct"], errors="coerce").mean()),
    }


def _profit_factor(values: pd.Series) -> float:
    pnl = pd.to_numeric(values, errors="coerce").fillna(0.0)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(-pnl[pnl < 0].sum())
    if gross_loss <= 0:
        return np.inf if gross_profit > 0 else np.nan
    return gross_profit / gross_loss


def _setup_risk_label(side: Any, setup: Any, count: int, pnl: float, pf: float) -> str:
    if count <= 0:
        return "NO_TRADE"
    if count < 2:
        return "WATCH_SMALL_SAMPLE"
    if np.isfinite(pf) and pf < 0.8:
        return "PROBATION"
    if pnl < 0:
        return "WATCH"
    return "HEALTHY"


def _truth_bool_series(df: pd.DataFrame, col: str, default: bool = False) -> pd.Series:
    if df.empty or col not in df.columns:
        return pd.Series(default, index=df.index, dtype=bool)
    raw = df[col]
    if pd.api.types.is_bool_dtype(raw):
        return raw.fillna(default).astype(bool)
    text = raw.fillna("").astype(str).str.strip().str.lower()
    true_values = {"1", "true", "t", "yes", "y", "passed", "pass"}
    false_values = {"0", "false", "f", "no", "n", "rejected", "reject", ""}
    return text.map(lambda v: True if v in true_values else (False if v in false_values else default)).astype(bool)


def _num_col(df: pd.DataFrame, col: str, default: float = np.nan) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def _text_col(df: pd.DataFrame, col: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=str)
    if col not in df.columns:
        return pd.Series("", index=df.index, dtype=str)
    return df[col].fillna("").astype(str)


def _side_dir_for_frame(df: pd.DataFrame) -> pd.Series:
    side = _text_col(df, "side").str.upper().str.strip()
    return side.map(lambda x: -1.0 if x == "SHORT" else 1.0).astype(float)


def _side_close_loc(df: pd.DataFrame, col: str) -> pd.Series:
    value = _num_col(df, col)
    short_mask = _text_col(df, "side").str.upper().str.strip().eq("SHORT")
    return value.where(~short_mask, 1.0 - value)


def _deep_stage_counts(group: pd.DataFrame) -> dict[str, int]:
    pre_reject_reason = _text_col(group, "pre_momentum_gate_reason")
    reject_reason = _text_col(group, "entry_reject_reason")
    return {
        "potential_raw": int(len(group)),
        "passed_gate": int(_truth_bool_series(group, "passed_v8_gate").sum()),
        "entry_raw": int(_truth_bool_series(group, "entry_row_built").sum()),
        "pre_momentum_rejected": int(
            (
                reject_reason.eq("pre_entry_momentum_gate")
                | (
                    pre_reject_reason.str.len().gt(0)
                    & ~(_truth_bool_series(group, "entry_selected") | _truth_bool_series(group, "paper_traded"))
                )
            ).sum()
        ),
        "selected_entries": int(_truth_bool_series(group, "entry_selected").sum()),
        "live_signals": int(_truth_bool_series(group, "live_signal_written").sum()),
        "paper_trades": int(_truth_bool_series(group, "paper_traded").sum()),
    }


def _outcome_counts(traded: pd.DataFrame) -> dict[str, int]:
    if traded.empty:
        return {"target_count": 0, "sl_count": 0, "eod_count": 0, "time_stop_count": 0}
    outcome = _text_col(traded, "paper_outcome").str.upper()
    target = outcome.str.contains("TARGET", regex=False)
    sl = outcome.eq("SL") | outcome.str.contains("STOP", regex=False)
    time_stop = outcome.str.contains("TIME_STOP", regex=False)
    eod = outcome.str.contains("EOD", regex=False) | (~target & ~sl & ~time_stop)
    return {
        "target_count": int(target.sum()),
        "sl_count": int(sl.sum()),
        "eod_count": int(eod.sum()),
        "time_stop_count": int(time_stop.sum()),
    }


def _mean_value(df: pd.DataFrame, col: str) -> float:
    return float(_num_col(df, col).mean()) if not df.empty else np.nan


def _avg_for_mask(df: pd.DataFrame, values: pd.Series, mask: pd.Series) -> float:
    if df.empty:
        return np.nan
    aligned = pd.to_numeric(values.reindex(df.index), errors="coerce")
    return float(aligned.loc[mask.reindex(df.index).fillna(False)].mean())


def _top_count_text(series: pd.Series, limit: int = 2) -> str:
    if series.empty:
        return ""
    clean = series.fillna("").astype(str).str.strip()
    counts = clean.loc[clean.ne("")].value_counts().head(limit)
    if counts.empty:
        return ""
    return "; ".join(f"{idx} ({int(val)})" for idx, val in counts.items())


def _short_note(text: str, max_len: int = 135) -> str:
    clean = " ".join(str(text or "").replace("|", "/").split())
    if len(clean) <= max_len:
        return clean
    return clean[: max(0, max_len - 3)].rstrip() + "..."


def _indicator_separators(group: pd.DataFrame, good: pd.Series, bad: pd.Series) -> dict[str, float]:
    side_dir = _side_dir_for_frame(group)
    features = {
        "pre_score_gap": _num_col(group, "pre_entry_momentum_score"),
        "pre2_side_mom_gap": side_dir * _num_col(group, "pre2_mom_r"),
        "sig5_adx_gap": _num_col(group, "sig5_adx_calc"),
        "sig5_vol_gap": _num_col(group, "sig5_vol_ratio20").fillna(_num_col(group, "vol_ratio")),
        "side_close_loc_gap": _side_close_loc(group, "sig5_close_pos").fillna(_side_close_loc(group, "close_loc")),
        "rs_side_gap": side_dir * _num_col(group, "rs_pct"),
    }
    gaps: dict[str, float] = {}
    for name, values in features.items():
        gaps[name] = _avg_for_mask(group, values, good) - _avg_for_mask(group, values, bad)
    abs_vwap = _num_col(group, "vwap_dist_atr").abs()
    gaps["bad_extra_vwap_extension"] = _avg_for_mask(group, abs_vwap, bad) - _avg_for_mask(group, abs_vwap, good)
    return gaps


def _deep_common_mistake(
    *,
    setup: str,
    stages: dict[str, int],
    outcome: dict[str, int],
    net: float,
    eod_avg_pnl: float,
    gaps: dict[str, float],
    top_reasons: str,
    top_pre_reasons: str,
) -> str:
    if stages["paper_trades"] <= 0:
        if stages["potential_raw"] > 0 and stages["passed_gate"] <= 0:
            return f"No final entries; scanner/gate rejected this setup. Main reason: {top_reasons or top_pre_reasons or 'not enough reason detail'}."
        if stages["pre_momentum_rejected"] > 0:
            return f"No final entries after pre-momentum. Main reject: {top_pre_reasons or top_reasons or 'pre-momentum gate'}."
        return "No final entries yet; keep as funnel/coverage diagnostic."
    notes: list[str] = []
    if outcome["sl_count"] > 0:
        if gaps.get("bad_extra_vwap_extension", 0.0) > 0.50:
            notes.append("SL rows are more extended from VWAP than winners")
        if gaps.get("pre_score_gap", 0.0) > 8.0:
            notes.append("losers have weaker pre-entry momentum score")
        if gaps.get("pre2_side_mom_gap", 0.0) > 0.12:
            notes.append("losers have weaker 2-bar side momentum")
        if gaps.get("side_close_loc_gap", 0.0) > 0.12:
            notes.append("losers close worse for trade direction")
        if gaps.get("sig5_adx_gap", 0.0) > 4.0:
            notes.append("losers have weaker signal-bar ADX")
        if gaps.get("sig5_vol_gap", 0.0) > 0.60:
            notes.append("losers have weaker signal-bar volume participation")
    if outcome["eod_count"] > 0 and np.isfinite(eod_avg_pnl) and eod_avg_pnl < 0:
        notes.append("EOD bucket is negative drag")
    if not notes and net < 0:
        notes.append("negative expectancy but no single indicator explains it yet")
    if not notes:
        notes.append("no repeated mistake pattern yet")
    return "; ".join(notes[:3])


def _deep_correctness_pattern(group: pd.DataFrame, good: pd.Series, bad: pd.Series, outcome: dict[str, int]) -> str:
    if int(good.sum()) <= 0:
        return "No winning/target sample yet."
    gaps = _indicator_separators(group, good, bad)
    strengths: list[tuple[str, float, str]] = [
        ("pre-entry momentum", gaps.get("pre_score_gap", np.nan), "higher"),
        ("2-bar side momentum", gaps.get("pre2_side_mom_gap", np.nan), "higher"),
        ("signal ADX", gaps.get("sig5_adx_gap", np.nan), "higher"),
        ("signal volume", gaps.get("sig5_vol_gap", np.nan), "higher"),
        ("directional close location", gaps.get("side_close_loc_gap", np.nan), "better"),
        ("less VWAP extension", gaps.get("bad_extra_vwap_extension", np.nan), "cleaner"),
    ]
    strengths = [x for x in strengths if np.isfinite(x[1]) and x[1] > (0.10 if x[0] != "pre-entry momentum" else 5.0)]
    if strengths:
        names = ", ".join(f"{name} {label}" for name, _, label in strengths[:3])
        return f"Correct entries cluster when {names}."
    if outcome["target_count"] > 0:
        return "Targets exist, but winners and losers look similar on current indicators."
    return "Positive rows exist, but target-specific pattern is still thin."


def _deep_improvement_focus(stages: dict[str, int], outcome: dict[str, int], net: float, pf: float, mistake: str) -> str:
    if stages["paper_trades"] <= 0:
        return "Use as shadow coverage; do not loosen until missed-forward outcomes prove clean."
    sl_rate = outcome["sl_count"] / max(1, stages["paper_trades"])
    eod_rate = outcome["eod_count"] / max(1, stages["paper_trades"])
    if sl_rate >= 0.35:
        return "Add/tighten setup-specific pre-momentum and anti-chase checks before increasing size."
    if eod_rate >= 0.50 and net <= 0:
        return "Study time-stop or EOD handoff; avoid letting non-progress trades wait to close."
    if np.isfinite(pf) and pf < 1.0:
        return "Keep paper/probation and compare rejected winners before changing live rules."
    if "no repeated mistake" in mistake:
        return "Keep current logic; collect more rows before changing thresholds."
    return "Use the mistake pattern as a shadow rule candidate, then replay before promotion."


def build_deep_analysis(day: str, truth: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if truth is None or truth.empty:
        empty = pd.DataFrame()
        return empty, {"day": day, "rows": 0, "setup_rows": 0}
    work = truth.copy()
    work["side"] = _text_col(work, "side").replace({"nan": ""}).str.strip()
    work["setup"] = _text_col(work, "setup").replace({"nan": ""}).str.strip()
    work.loc[work["side"].eq(""), "side"] = "UNKNOWN_SIDE"
    work.loc[work["setup"].eq(""), "setup"] = "UNKNOWN_SETUP"
    for col in ("paper_traded", "passed_v8_gate", "entry_row_built", "entry_selected", "live_signal_written"):
        work[col] = _truth_bool_series(work, col)
    work["paper_pnl_rs"] = _num_col(work, "paper_pnl_rs").fillna(0.0)
    rows: list[dict[str, Any]] = []
    for (side, setup), group in work.groupby(["side", "setup"], dropna=False):
        stages = _deep_stage_counts(group)
        traded = group.loc[group["paper_traded"].astype(bool)].copy()
        outcome = _outcome_counts(traded)
        pnl = _num_col(traded, "paper_pnl_rs").fillna(0.0) if not traded.empty else pd.Series(dtype=float)
        net = float(pnl.sum()) if not pnl.empty else 0.0
        setup_pf = _profit_factor(pnl) if not pnl.empty else np.nan
        good = (group["paper_pnl_rs"] > 0) | _text_col(group, "paper_outcome").str.upper().str.contains("TARGET", regex=False)
        bad = (group["paper_pnl_rs"] < 0) | _text_col(group, "paper_outcome").str.upper().eq("SL")
        eod_mask = group["paper_traded"].astype(bool) & _text_col(group, "paper_outcome").str.upper().str.contains("EOD", regex=False)
        gaps = _indicator_separators(group, good, bad)
        top_reason = _top_count_text(_text_col(group.loc[~group["paper_traded"].astype(bool)], "reason_not_taken"))
        top_pre_reason = _top_count_text(_text_col(group, "pre_momentum_gate_reason"))
        eod_avg_pnl = float(group.loc[eod_mask, "paper_pnl_rs"].mean()) if bool(eod_mask.any()) else np.nan
        mistake = _deep_common_mistake(
            setup=str(setup),
            stages=stages,
            outcome=outcome,
            net=net,
            eod_avg_pnl=eod_avg_pnl,
            gaps=gaps,
            top_reasons=top_reason,
            top_pre_reasons=top_pre_reason,
        )
        correctness = _deep_correctness_pattern(group, good, bad, outcome)
        focus = _deep_improvement_focus(stages, outcome, net, setup_pf, mistake)
        rows.append(
            {
                "date": day,
                "side": side,
                "setup": setup,
                **stages,
                **outcome,
                "wins": int((pnl > 0).sum()) if not pnl.empty else 0,
                "losses": int((pnl < 0).sum()) if not pnl.empty else 0,
                "net_pnl_rs": net,
                "profit_factor": setup_pf,
                "avg_quality_score": _mean_value(group, "quality_score"),
                "avg_ranker_score": _mean_value(group, "ranker_score"),
                "avg_entry_distance_pct": _mean_value(traded, "entry_distance_pct"),
                "avg_forward_mfe_pct": _mean_value(group, "forward_mfe_pct"),
                "avg_forward_mae_pct": _mean_value(group, "forward_mae_pct"),
                "good_avg_pre_entry_momentum_score": _avg_for_mask(group, _num_col(group, "pre_entry_momentum_score"), good),
                "bad_avg_pre_entry_momentum_score": _avg_for_mask(group, _num_col(group, "pre_entry_momentum_score"), bad),
                "good_avg_pre2_side_mom_r": _avg_for_mask(group, _side_dir_for_frame(group) * _num_col(group, "pre2_mom_r"), good),
                "bad_avg_pre2_side_mom_r": _avg_for_mask(group, _side_dir_for_frame(group) * _num_col(group, "pre2_mom_r"), bad),
                "good_avg_sig5_adx_calc": _avg_for_mask(group, _num_col(group, "sig5_adx_calc"), good),
                "bad_avg_sig5_adx_calc": _avg_for_mask(group, _num_col(group, "sig5_adx_calc"), bad),
                "good_avg_side_close_loc": _avg_for_mask(group, _side_close_loc(group, "sig5_close_pos").fillna(_side_close_loc(group, "close_loc")), good),
                "bad_avg_side_close_loc": _avg_for_mask(group, _side_close_loc(group, "sig5_close_pos").fillna(_side_close_loc(group, "close_loc")), bad),
                "good_avg_abs_vwap_dist_atr": _avg_for_mask(group, _num_col(group, "vwap_dist_atr").abs(), good),
                "bad_avg_abs_vwap_dist_atr": _avg_for_mask(group, _num_col(group, "vwap_dist_atr").abs(), bad),
                "good_avg_sig5_vol_ratio20": _avg_for_mask(group, _num_col(group, "sig5_vol_ratio20").fillna(_num_col(group, "vol_ratio")), good),
                "bad_avg_sig5_vol_ratio20": _avg_for_mask(group, _num_col(group, "sig5_vol_ratio20").fillna(_num_col(group, "vol_ratio")), bad),
                "pre_score_good_bad_gap": gaps.get("pre_score_gap", np.nan),
                "pre2_side_mom_good_bad_gap": gaps.get("pre2_side_mom_gap", np.nan),
                "sig5_adx_good_bad_gap": gaps.get("sig5_adx_gap", np.nan),
                "sig5_vol_good_bad_gap": gaps.get("sig5_vol_gap", np.nan),
                "side_close_loc_good_bad_gap": gaps.get("side_close_loc_gap", np.nan),
                "bad_extra_vwap_extension": gaps.get("bad_extra_vwap_extension", np.nan),
                "top_not_taken_reason": top_reason,
                "top_pre_momentum_reject_reason": top_pre_reason,
                "common_mistake": mistake,
                "correctness_pattern": correctness,
                "improvement_focus": focus,
            }
        )
    deep = pd.DataFrame(rows)
    if not deep.empty:
        deep = deep.sort_values(["net_pnl_rs", "paper_trades", "potential_raw"], ascending=[True, False, False]).reset_index(drop=True)
    payload = {
        "day": day,
        "rows": int(len(truth)),
        "setup_rows": int(len(deep)),
        "paper_traded_setups": int((deep.get("paper_trades", pd.Series(dtype=float)) > 0).sum()) if not deep.empty else 0,
        "negative_setup_rows": int((deep.get("net_pnl_rs", pd.Series(dtype=float)) < 0).sum()) if not deep.empty else 0,
    }
    return deep, payload


def _deep_pf_text(value: Any) -> str:
    val = _safe_float(value)
    if np.isinf(val):
        return "inf"
    if not np.isfinite(val):
        return "NA"
    return _fmt_num(val, 2)


def deep_analysis_report(day: str, truth: pd.DataFrame, *, standalone: bool = True, limit: int = 25) -> str:
    deep, payload = build_deep_analysis(day, truth)
    lines = [f"# V7 Deep Analysis Block - {day}" if standalone else "## Deep Analysis Block", ""]
    lines.extend(
        [
            "This block follows each setup from potential raw candidate to final paper entry, then compares outcomes against the scanner, entry, pre-momentum, and exit result fields.",
            "",
        ]
    )
    if deep.empty:
        lines.append("No setup rows available for deep analysis.")
        return "\n".join(lines) + "\n"
    lines.extend(
        [
            f"- Truth rows analysed: {payload['rows']}",
            f"- Setup rows: {payload['setup_rows']}",
            f"- Paper-traded setups: {payload['paper_traded_setups']}",
            f"- Negative setup rows: {payload['negative_setup_rows']}",
            "",
            "| side | setup | funnel raw/gate/entry/paper | T/SL/EOD | pnl | PF | common mistake | correctness | improvement |",
            "|---|---|---:|---:|---:|---:|---|---|---|",
        ]
    )
    for _, row in deep.head(limit).iterrows():
        funnel = (
            f"{int(row.get('potential_raw', 0))}/"
            f"{int(row.get('passed_gate', 0))}/"
            f"{int(row.get('selected_entries', 0))}/"
            f"{int(row.get('paper_trades', 0))}"
        )
        outcome = f"{int(row.get('target_count', 0))}/{int(row.get('sl_count', 0))}/{int(row.get('eod_count', 0))}"
        lines.append(
            f"| {row.get('side', '')} | {row.get('setup', '')} | {funnel} | {outcome} | "
            f"Rs {_fmt_num(row.get('net_pnl_rs'))} | {_deep_pf_text(row.get('profit_factor'))} | "
            f"{_short_note(row.get('common_mistake'))} | {_short_note(row.get('correctness_pattern'))} | "
            f"{_short_note(row.get('improvement_focus'))} |"
        )
    lines.append("")
    lines.extend(
        [
            "Legend: funnel is raw candidates / passed scanner gate / selected entry rows / final paper trades. T/SL/EOD is target / stop-loss / EOD-like outcomes.",
            "Use this as an improvement map only. Promote a rule only after replay/shadow proof, especially when setup samples are small.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _clip01(value: Any) -> float:
    val = _safe_float(value, 0.0)
    if not np.isfinite(val):
        return 0.0
    return float(min(1.0, max(0.0, val)))


def _signed_rs_score(side: Any, rs_pct: Any) -> float:
    rs = _safe_float(rs_pct, 0.0)
    if not np.isfinite(rs):
        return 0.0
    signed = -rs if str(side or "").upper().strip() == "SHORT" else rs
    return _clip01((signed + 1.0) / 7.0)


def _close_location_score(side: Any, close_loc: Any) -> float:
    loc = _safe_float(close_loc, np.nan)
    if not np.isfinite(loc):
        return 0.35
    ideal = 0.25 if str(side or "").upper().strip() == "SHORT" else 0.75
    return _clip01(1.0 - abs(loc - ideal) / 0.75)


def _vwap_extension_score(vwap_dist_atr: Any) -> float:
    dist = abs(_safe_float(vwap_dist_atr, 0.0))
    if not np.isfinite(dist):
        return 0.35
    return _clip01(1.0 - max(0.0, dist - 0.5) / 5.0)


def _ranker_labels(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    mfe = pd.to_numeric(out.get("forward_mfe_pct", np.nan), errors="coerce")
    mae = pd.to_numeric(out.get("forward_mae_pct", np.nan), errors="coerce")
    pnl = pd.to_numeric(out.get("paper_pnl_rs", np.nan), errors="coerce")
    out["ranker_clean_move_label"] = ((mfe >= 0.8) & (mae <= 0.8)) | (pnl > 0)
    out["ranker_bad_move_label"] = ((mae >= 0.8) & (mfe < 0.8)) | (pnl < 0)
    return out


def _setup_memory(history: pd.DataFrame) -> dict[tuple[str, str], float]:
    if history.empty:
        return {}
    hist = _ranker_labels(history)
    memory: dict[tuple[str, str], float] = {}
    for (side, setup), group in hist.groupby(["side", "setup"], dropna=False):
        clean = group["ranker_clean_move_label"].astype(bool)
        bad = group["ranker_bad_move_label"].astype(bool)
        # Smoothed score in [0, 1]. A setup with no clear evidence stays neutral.
        score = (float(clean.sum()) + 1.0) / (float(clean.sum() + bad.sum()) + 2.0)
        memory[(str(side), str(setup))] = score
    return memory


def _heuristic_rank_scores(df: pd.DataFrame, history: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    memory = _setup_memory(history)
    scores: list[float] = []
    for _, row in df.iterrows():
        side = row.get("side", "")
        setup = row.get("setup", "")
        quality = _clip01(_safe_float(row.get("quality_score"), 0.0) / 250.0)
        rs_score = _signed_rs_score(side, row.get("rs_pct"))
        vol_score = _clip01(_safe_float(row.get("vol_ratio"), 1.0) / 6.0)
        atr_score = _clip01(_safe_float(row.get("atr_pct"), 0.0) / 0.006)
        close_score = _close_location_score(side, row.get("close_loc"))
        vwap_score = _vwap_extension_score(row.get("vwap_dist_atr"))
        market = _safe_float(row.get("market_ret_pct"), 0.0)
        market_score = _clip01((market + 0.20) / 0.40)
        setup_score = memory.get((str(side), str(setup)), 0.50)
        score = (
            0.24 * quality
            + 0.16 * rs_score
            + 0.14 * vol_score
            + 0.10 * atr_score
            + 0.14 * close_score
            + 0.12 * vwap_score
            + 0.04 * market_score
            + 0.06 * setup_score
        )
        scores.append(round(float(score), 6))
    return pd.Series(scores, index=df.index)


def _load_truth_history(before_day: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(TRUTH_DIR.glob("truth_table_*.csv")):
        day = path.stem.replace("truth_table_", "")
        if day >= before_day:
            continue
        df = _read_csv(path)
        if not df.empty:
            df["history_day"] = day
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def add_ranker_scores(day: str, truth: pd.DataFrame) -> pd.DataFrame:
    if truth.empty:
        return truth
    out = _ranker_labels(truth)
    history = _load_truth_history(day)
    out["ranker_score"] = _heuristic_rank_scores(out, history)
    out["ranker_model"] = "heuristic_v1_dependency_free"
    out["ranker_shadow_pick_top10"] = False
    out["ranker_shadow_pick_top20"] = False
    eligible = out.loc[out["ranker_score"].notna()].sort_values("ranker_score", ascending=False)
    out.loc[eligible.head(10).index, "ranker_shadow_pick_top10"] = True
    out.loc[eligible.head(20).index, "ranker_shadow_pick_top20"] = True
    return out


def _ranker_pick_stats(df: pd.DataFrame, mask: pd.Series) -> dict[str, Any]:
    picked = df.loc[mask].copy()
    if picked.empty:
        return {"count": 0, "clean": 0, "bad": 0, "clean_rate": np.nan, "avg_mfe": np.nan, "avg_mae": np.nan}
    clean = picked["ranker_clean_move_label"].astype(bool)
    bad = picked["ranker_bad_move_label"].astype(bool)
    return {
        "count": int(len(picked)),
        "clean": int(clean.sum()),
        "bad": int(bad.sum()),
        "clean_rate": float(clean.mean()),
        "avg_mfe": float(pd.to_numeric(picked.get("forward_mfe_pct"), errors="coerce").mean()),
        "avg_mae": float(pd.to_numeric(picked.get("forward_mae_pct"), errors="coerce").mean()),
    }


def write_ranker_report(day: str, truth: pd.DataFrame) -> str:
    lines = [
        f"# V7 Research Layer Candidate Ranker - {day}",
        "",
        "Model: heuristic_v1_dependency_free",
        "",
    ]
    if truth.empty:
        lines.append("No candidates found.")
        return "\n".join(lines) + "\n"
    top10 = _ranker_pick_stats(truth, truth["ranker_shadow_pick_top10"].astype(bool))
    top20 = _ranker_pick_stats(truth, truth["ranker_shadow_pick_top20"].astype(bool))
    v8 = _ranker_pick_stats(truth, truth["passed_v8_gate"].astype(bool))
    paper = truth.loc[truth["paper_traded"].astype(bool)].copy()
    lines.extend(
        [
            "## Shadow Proof",
            "",
            "| bucket | count | clean | bad | clean rate | avg MFE | avg MAE |",
            "|---|---:|---:|---:|---:|---:|---:|",
            f"| Ranker top 10 | {top10['count']} | {top10['clean']} | {top10['bad']} | {_fmt_num(top10['clean_rate'] * 100, 1)}% | {_fmt_num(top10['avg_mfe'], 3)}% | {_fmt_num(top10['avg_mae'], 3)}% |",
            f"| Ranker top 20 | {top20['count']} | {top20['clean']} | {top20['bad']} | {_fmt_num(top20['clean_rate'] * 100, 1)}% | {_fmt_num(top20['avg_mfe'], 3)}% | {_fmt_num(top20['avg_mae'], 3)}% |",
            f"| V8 accepted | {v8['count']} | {v8['clean']} | {v8['bad']} | {_fmt_num(v8['clean_rate'] * 100, 1)}% | {_fmt_num(v8['avg_mfe'], 3)}% | {_fmt_num(v8['avg_mae'], 3)}% |",
            "",
        ]
    )
    if not paper.empty:
        pnl = pd.to_numeric(paper["paper_pnl_rs"], errors="coerce").fillna(0.0)
        lines.extend(
            [
                "## Actual Paper Trades",
                "",
                f"- Trades: {len(paper)}",
                f"- Wins: {int((pnl > 0).sum())}",
                f"- Net PnL: Rs {_fmt_num(pnl.sum())}",
                "",
            ]
        )
    top = truth.sort_values("ranker_score", ascending=False).head(25)
    lines.extend(["## Top Ranked Candidates", ""])
    lines.append("| rank | time | ticker | side | setup | score | V8 gate | paper | MFE | MAE |")
    lines.append("|---:|---|---|---|---|---:|---|---|---:|---:|")
    for rank, (_, row) in enumerate(top.iterrows(), start=1):
        lines.append(
            f"| {rank} | {row.get('signal_time_ist', '')} | {row.get('ticker', '')} | {row.get('side', '')} | "
            f"{row.get('setup', '')} | {_fmt_num(row.get('ranker_score'), 4)} | {bool(row.get('passed_v8_gate'))} | "
            f"{bool(row.get('paper_traded'))} | {_fmt_num(row.get('forward_mfe_pct'), 3)}% | {_fmt_num(row.get('forward_mae_pct'), 3)}% |"
        )
    lines.append("")

    rejected_top = truth.loc[~truth["passed_v8_gate"].astype(bool)].sort_values("ranker_score", ascending=False).head(15)
    lines.extend(["## Top Rejected By Ranker", ""])
    if rejected_top.empty:
        lines.append("None.")
    else:
        lines.append("| time | ticker | side | setup | score | MFE | MAE |")
        lines.append("|---|---|---|---|---:|---:|---:|")
        for _, row in rejected_top.iterrows():
            lines.append(
                f"| {row.get('signal_time_ist', '')} | {row.get('ticker', '')} | {row.get('side', '')} | "
                f"{row.get('setup', '')} | {_fmt_num(row.get('ranker_score'), 4)} | "
                f"{_fmt_num(row.get('forward_mfe_pct'), 3)}% | {_fmt_num(row.get('forward_mae_pct'), 3)}% |"
            )
    lines.append("")
    return "\n".join(lines) + "\n"


def write_report(day: str, truth: pd.DataFrame) -> str:
    summary = _summary_counts(truth)
    lines = [
        f"# V7 Research Layer Reality Gap Report - {day}",
        "",
    ]
    if truth.empty:
        lines.append("No candidates found.")
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "## Summary",
            "",
            f"- Research rows total: {summary['research_rows_total']}",
            f"- Raw candidates: {summary['raw_candidates']}",
            f"- Passed v8 gate: {summary['passed_v8_gate']}",
            f"- Entry rows built: {summary['entry_rows']}",
            f"- Selected entries reconciled: {summary['selected_entries']}",
            f"- Selected entries from entry audit: {summary['audit_selected_entries']}",
            f"- Live signal rows written: {summary['live_signals']}",
            f"- Paper trades: {summary['paper_trades']}",
            f"- Paper PnL: Rs {_fmt_num(summary['paper_pnl_rs'])}",
            f"- Avg entry delay: {_fmt_num(summary['avg_entry_delay_sec'])} sec",
            f"- Avg paper entry distance: {_fmt_num(summary['avg_entry_distance_pct'], 4)}%",
            "",
        ]
    )

    reason_counts = (
        truth["reason_not_taken"].replace("", "taken").value_counts(dropna=False).rename_axis("reason").reset_index(name="count")
    )
    lines.extend(["## Not-Taken Reasons", ""])
    for _, row in reason_counts.iterrows():
        lines.append(f"- {row['reason']}: {int(row['count'])}")
    lines.append("")

    setup = (
        truth.groupby(["side", "setup"], dropna=False)
        .agg(
            raw_candidates=("research_key", "count"),
            passed_v8_gate=("passed_v8_gate", "sum"),
            paper_trades=("paper_traded", "sum"),
            paper_pnl_rs=("paper_pnl_rs", "sum"),
        )
        .reset_index()
        .sort_values(["paper_pnl_rs", "paper_trades", "passed_v8_gate"], ascending=[False, False, False])
    )
    traded = truth.loc[truth["paper_traded"].astype(bool)].copy()
    if not traded.empty:
        risk = (
            traded.groupby(["side", "setup"], dropna=False)["paper_pnl_rs"]
            .agg(["count", "sum"])
            .reset_index()
            .rename(columns={"count": "trade_count", "sum": "net_pnl"})
        )
        risk["pf"] = [
            _profit_factor(traded.loc[(traded["side"] == row["side"]) & (traded["setup"] == row["setup"]), "paper_pnl_rs"])
            for _, row in risk.iterrows()
        ]
        risk["risk_label"] = [
            _setup_risk_label(row["side"], row["setup"], int(row["trade_count"]), float(row["net_pnl"]), float(row["pf"]))
            for _, row in risk.iterrows()
        ]
    else:
        risk = pd.DataFrame(columns=["side", "setup", "trade_count", "net_pnl", "pf", "risk_label"])
    lines.extend(["## Setup Scorecard", ""])
    lines.append("| side | setup | raw | gated | paper trades | paper pnl | PF | risk |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---|")
    for _, row in setup.head(30).iterrows():
        risk_row = risk.loc[(risk["side"] == row["side"]) & (risk["setup"] == row["setup"])]
        pf_text = ""
        risk_text = ""
        if not risk_row.empty:
            pf_val = float(risk_row.iloc[0]["pf"])
            pf_text = "inf" if np.isinf(pf_val) else _fmt_num(pf_val, 2)
            risk_text = str(risk_row.iloc[0]["risk_label"])
        lines.append(
            f"| {row['side']} | {row['setup']} | {int(row['raw_candidates'])} | "
            f"{int(row['passed_v8_gate'])} | {int(row['paper_trades'])} | Rs {_fmt_num(row['paper_pnl_rs'])} | "
            f"{pf_text} | {risk_text} |"
        )
    lines.append("")
    lines.append(deep_analysis_report(day, truth, standalone=False, limit=20).rstrip())
    lines.append("")

    audit_gap = truth.loc[
        truth["paper_traded"].astype(bool)
        & ~truth.get("entry_audit_selected", truth["entry_selected"]).astype(bool)
    ].copy()
    lines.extend(["## Reconciliation Gaps", ""])
    if audit_gap.empty:
        lines.append("- No paper trades without an entry-audit selected row.")
    else:
        lines.append(f"- Paper trades without entry-audit selected row: {len(audit_gap)}")
        lines.append("")
        lines.append("| time | ticker | side | setup | signal_id | pnl | reconciled source |")
        lines.append("|---|---|---|---|---|---:|---|")
        for _, row in audit_gap.sort_values("signal_time_ist").iterrows():
            lines.append(
                f"| {row.get('signal_time_ist', '')} | {row.get('ticker', '')} | {row.get('side', '')} | "
                f"{row.get('setup', '')} | {row.get('paper_signal_id', '')} | Rs {_fmt_num(row.get('paper_pnl_rs'))} | "
                f"{row.get('entry_selection_source', '')} |"
            )
    lines.append("")

    passed_not_traded = truth.loc[truth["passed_v8_gate"].astype(bool) & ~truth["paper_traded"].astype(bool)].copy()
    lines.extend(["## Passed Gate But Not Traded", ""])
    if passed_not_traded.empty:
        lines.append("None.")
    else:
        lines.append("| time | ticker | side | setup | score | reason | live signal |")
        lines.append("|---|---|---|---|---:|---|---|")
        for _, row in passed_not_traded.sort_values("quality_score", ascending=False).head(20).iterrows():
            lines.append(
                f"| {row.get('signal_time_ist', '')} | {row.get('ticker', '')} | {row.get('side', '')} | "
                f"{row.get('setup', '')} | {_fmt_num(row.get('quality_score'), 3)} | {row.get('reason_not_taken', '')} | "
                f"{row.get('live_signal_id', '')} |"
            )
    lines.append("")

    rejected = truth.loc[~truth["passed_v8_gate"].astype(bool)].copy()
    rejected["quality_score"] = pd.to_numeric(rejected["quality_score"], errors="coerce").fillna(0.0)
    rejected["forward_mfe_pct"] = pd.to_numeric(rejected["forward_mfe_pct"], errors="coerce")
    rejected["forward_mae_pct"] = pd.to_numeric(rejected["forward_mae_pct"], errors="coerce")
    lines.extend(["## High-Score Rejected Forward Outcome", ""])
    if rejected.empty:
        lines.append("None.")
    else:
        lines.append("| time | ticker | side | setup | score | 3-bar MFE | 3-bar MAE | close ret |")
        lines.append("|---|---|---|---|---:|---:|---:|---:|")
        for _, row in rejected.sort_values("quality_score", ascending=False).head(20).iterrows():
            lines.append(
                f"| {row.get('signal_time_ist', '')} | {row.get('ticker', '')} | {row.get('side', '')} | "
                f"{row.get('setup', '')} | {_fmt_num(row.get('quality_score'), 3)} | "
                f"{_fmt_num(row.get('forward_mfe_pct'), 3)}% | {_fmt_num(row.get('forward_mae_pct'), 3)}% | "
                f"{_fmt_num(row.get('forward_close_ret_pct'), 3)}% |"
            )
    lines.append("")

    missed = truth.loc[~truth["paper_traded"].astype(bool)].copy()
    missed["quality_score"] = pd.to_numeric(missed["quality_score"], errors="coerce").fillna(0.0)
    lines.extend(["## Highest-Score Missed Candidates", ""])
    cols = ["signal_time_ist", "ticker", "side", "setup", "quality_score", "reason_not_taken"]
    if missed.empty:
        lines.append("None.")
    else:
        lines.append("| time | ticker | side | setup | score | reason |")
        lines.append("|---|---|---|---|---:|---|")
        for _, row in missed.sort_values("quality_score", ascending=False).head(20).iterrows():
            lines.append(
                f"| {row.get(cols[0], '')} | {row.get(cols[1], '')} | {row.get(cols[2], '')} | "
                f"{row.get(cols[3], '')} | {_fmt_num(row.get(cols[4]), 3)} | {row.get(cols[5], '')} |"
            )
    lines.append("")

    return "\n".join(lines) + "\n"


def write_action_plan(day: str, truth: pd.DataFrame) -> str:
    summary = _summary_counts(truth)
    lines = [
        f"# V7 Research Layer EOD Action Plan - {day}",
        "",
    ]
    if truth.empty:
        lines.append("No candidates found.")
        return "\n".join(lines) + "\n"

    traded = truth.loc[truth["paper_traded"].astype(bool)].copy()
    traded["paper_pnl_rs"] = pd.to_numeric(traded["paper_pnl_rs"], errors="coerce").fillna(0.0)
    pf = _profit_factor(traded["paper_pnl_rs"]) if not traded.empty else np.nan
    pf_text = "inf" if np.isinf(pf) else _fmt_num(pf, 2)
    lines.extend(
        [
            "## Snapshot",
            "",
            f"- Research rows total: {summary['research_rows_total']}",
            f"- Raw candidates: {summary['raw_candidates']}",
            f"- Passed gate: {summary['passed_v8_gate']}",
            f"- Live signals: {summary['live_signals']}",
            f"- Paper trades: {summary['paper_trades']}",
            f"- Net paper PnL: Rs {_fmt_num(summary['paper_pnl_rs'])}",
            f"- Paper PF: {pf_text}",
            "",
        ]
    )

    audit_gap = truth.loc[
        truth["paper_traded"].astype(bool)
        & ~truth.get("entry_audit_selected", truth["entry_selected"]).astype(bool)
    ].copy()
    lines.extend(["## Technical Fixes", ""])
    if audit_gap.empty:
        lines.append("- Reconciliation OK: no paper trades missing from entry-audit selection.")
    else:
        lines.append(
            f"- Monitor reconciliation: {len(audit_gap)} paper trades were reconciled through live/paper signal IDs, not entry-audit selected rows."
        )
    if summary["live_signals"] != summary["paper_trades"]:
        lines.append(
            f"- Investigate executor gap: {summary['live_signals']} live signals vs {summary['paper_trades']} paper trades."
        )
    else:
        lines.append("- Executor gap OK: live signal count matches paper trade count.")
    lines.append("")

    if not traded.empty:
        setup_rows = []
        for (side, setup), group in traded.groupby(["side", "setup"], dropna=False):
            net = float(group["paper_pnl_rs"].sum())
            setup_pf = _profit_factor(group["paper_pnl_rs"])
            setup_rows.append(
                {
                    "side": side,
                    "setup": setup,
                    "trades": int(len(group)),
                    "wins": int((group["paper_pnl_rs"] > 0).sum()),
                    "net": net,
                    "pf": setup_pf,
                    "risk": _setup_risk_label(side, setup, int(len(group)), net, setup_pf),
                }
            )
        setup_df = pd.DataFrame(setup_rows).sort_values(["risk", "net"], ascending=[False, True])
    else:
        setup_df = pd.DataFrame()

    lines.extend(["## Setup Watchlist", ""])
    if setup_df.empty:
        lines.append("No traded setups.")
    else:
        lines.append("| side | setup | trades | wins | net | PF | action |")
        lines.append("|---|---|---:|---:|---:|---:|---|")
        for _, row in setup_df.iterrows():
            pf_val = float(row["pf"])
            pf_str = "inf" if np.isinf(pf_val) else _fmt_num(pf_val, 2)
            action = "keep" if row["risk"] == "HEALTHY" else ("probation" if row["risk"] == "PROBATION" else "watch")
            lines.append(
                f"| {row['side']} | {row['setup']} | {int(row['trades'])} | {int(row['wins'])} | "
                f"Rs {_fmt_num(row['net'])} | {pf_str} | {action} |"
            )
    lines.append("")

    rejected = truth.loc[~truth["passed_v8_gate"].astype(bool)].copy()
    rejected["quality_score"] = pd.to_numeric(rejected["quality_score"], errors="coerce").fillna(0.0)
    rejected["forward_mfe_pct"] = pd.to_numeric(rejected["forward_mfe_pct"], errors="coerce")
    rejected["forward_mae_pct"] = pd.to_numeric(rejected["forward_mae_pct"], errors="coerce")
    opportunity = rejected.loc[
        (rejected["quality_score"] >= 100.0)
        & (rejected["forward_mfe_pct"] >= 0.8)
        & (rejected["forward_mae_pct"] <= 0.8)
    ].copy()
    lines.extend(["## Rejected-Candidate Opportunities", ""])
    if opportunity.empty:
        lines.append("- No high-score rejected candidates had clean 3-bar MFE >= 0.8% with MAE <= 0.8%.")
    else:
        lines.append("| time | ticker | side | setup | score | MFE | MAE |")
        lines.append("|---|---|---|---|---:|---:|---:|")
        for _, row in opportunity.sort_values(["forward_mfe_pct", "quality_score"], ascending=False).head(15).iterrows():
            lines.append(
                f"| {row.get('signal_time_ist', '')} | {row.get('ticker', '')} | {row.get('side', '')} | "
                f"{row.get('setup', '')} | {_fmt_num(row.get('quality_score'), 3)} | "
                f"{_fmt_num(row.get('forward_mfe_pct'), 3)}% | {_fmt_num(row.get('forward_mae_pct'), 3)}% |"
            )
    lines.append("")

    if "ranker_score" in truth.columns:
        top10 = _ranker_pick_stats(truth, truth["ranker_shadow_pick_top10"].astype(bool))
        v8 = _ranker_pick_stats(truth, truth["passed_v8_gate"].astype(bool))
        lines.extend(["## Ranker Shadow Check", ""])
        lines.append("| bucket | count | clean | bad | clean rate | avg MFE | avg MAE |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        lines.append(
            f"| Ranker top 10 | {top10['count']} | {top10['clean']} | {top10['bad']} | "
            f"{_fmt_num(top10['clean_rate'] * 100, 1)}% | {_fmt_num(top10['avg_mfe'], 3)}% | {_fmt_num(top10['avg_mae'], 3)}% |"
        )
        lines.append(
            f"| V8 accepted | {v8['count']} | {v8['clean']} | {v8['bad']} | "
            f"{_fmt_num(v8['clean_rate'] * 100, 1)}% | {_fmt_num(v8['avg_mfe'], 3)}% | {_fmt_num(v8['avg_mae'], 3)}% |"
        )
        lines.append("")
        lines.append("- Ranker is research-only. It does not change live scanner or executor decisions.")
        lines.append("")

    return "\n".join(lines) + "\n"


SUGGESTION_WINDOWS = (3, 5, 7, 11, 13, 15, 17, 20)


def _truth_day_from_path(path: Path) -> str:
    return path.stem.replace("truth_table_", "")


def _available_truth_days(up_to_day: str) -> list[str]:
    days: list[str] = []
    for path in sorted(TRUTH_DIR.glob("truth_table_*.csv")):
        day = _truth_day_from_path(path)
        if day <= up_to_day and path.stat().st_size > 2:
            days.append(day)
    return sorted(set(days))


def _load_truth_days(days: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for day in days:
        df = _read_csv(TRUTH_DIR / f"truth_table_{day}.csv")
        if df.empty:
            continue
        df["research_day"] = day
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    for col in ("paper_traded", "passed_v8_gate", "ranker_shadow_pick_top10", "ranker_shadow_pick_top20"):
        if col in out.columns:
            out[col] = out[col].fillna(False).astype(bool)
    return out


_ONE_MIN_CACHE: dict[str, pd.DataFrame] = {}
EXIT_LAB_PROFILES: tuple[dict[str, Any], ...] = (
    {"profile": "static_model", "kind": "static", "sl_pct": None, "target_pct": None},
    {"profile": "static_tight_0p50_0p80", "kind": "static", "sl_pct": 0.50, "target_pct": 0.80},
    {"profile": "static_balanced_0p70_1p00", "kind": "static", "sl_pct": 0.70, "target_pct": 1.00},
    {"profile": "static_wide_1p00_1p50", "kind": "static", "sl_pct": 1.00, "target_pct": 1.50},
    {"profile": "dynamic_be_0p60_model_target", "kind": "breakeven", "sl_pct": None, "target_pct": None, "be_trigger_pct": 0.60},
    {"profile": "dynamic_trail_0p80_0p35", "kind": "trail", "sl_pct": None, "target_pct": None, "trail_trigger_pct": 0.80, "trail_gap_pct": 0.35},
    {"profile": "time_stop_30m_model_sl", "kind": "time_stop", "sl_pct": None, "target_pct": None, "max_hold_min": 30},
)


def _load_1min_bars(ticker: Any) -> pd.DataFrame:
    symbol = str(ticker or "").upper().strip()
    if not symbol:
        return pd.DataFrame()
    if symbol in _ONE_MIN_CACHE:
        return _ONE_MIN_CACHE[symbol]
    path = DATA_1MIN_DIR / f"{symbol}_stocks_indicators_1min.parquet"
    if not path.exists():
        _ONE_MIN_CACHE[symbol] = pd.DataFrame()
        return _ONE_MIN_CACHE[symbol]
    try:
        df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume"])
    except Exception:
        df = pd.DataFrame()
    if not df.empty:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if getattr(df["date"].dt, "tz", None) is None:
            df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
        else:
            df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
        df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.reset_index(drop=True)
    _ONE_MIN_CACHE[symbol] = df
    return df


def _session_exit_cutoff(day_text: Any) -> pd.Timestamp:
    day = pd.to_datetime(str(day_text)[:10], errors="coerce")
    if pd.isna(day):
        day = _now_ist()
    return pd.Timestamp(
        year=int(day.year),
        month=int(day.month),
        day=int(day.day),
        hour=15,
        minute=20,
        tz="Asia/Kolkata",
    )


def _first_1m_entry_from_signal(bars: pd.DataFrame, signal_time: Any) -> tuple[pd.Timestamp, float, str]:
    signal_ts = _normalise_ts(signal_time)
    if bars.empty or pd.isna(signal_ts):
        return pd.NaT, np.nan, "missing_1min_entry"
    sub = bars.loc[
        (bars["date"] >= signal_ts)
        & (bars["date"] <= signal_ts + pd.Timedelta(minutes=5))
    ].sort_values("date")
    if sub.empty:
        return pd.NaT, np.nan, "missing_1min_entry"
    row = sub.iloc[0]
    return _normalise_ts(row.get("date")), _safe_float(row.get("open")), "first_1min_open"


def _exit_lab_entry(row: pd.Series, bars: pd.DataFrame) -> tuple[pd.Timestamp, float, str]:
    if bool(row.get("paper_traded", False)):
        entry_ts = _normalise_ts(row.get("paper_entry_time"))
        entry_px = _safe_float(row.get("paper_entry_price"))
        if pd.notna(entry_ts) and np.isfinite(entry_px) and entry_px > 0:
            return entry_ts, entry_px, "paper_trade_entry"
    entry_ts = _normalise_ts(row.get("entry_time_ist"))
    entry_px = _safe_float(row.get("entry_price_model"))
    if pd.notna(entry_ts) and np.isfinite(entry_px) and entry_px > 0:
        return entry_ts, entry_px, "entry_engine_model"
    return _first_1m_entry_from_signal(bars, row.get("signal_time_ist"))


def _row_sl_target_pct(row: pd.Series, entry_px: float) -> tuple[float, float]:
    sl_pct = _safe_float(row.get("sl_pct"))
    target_pct = _safe_float(row.get("target_pct"))
    if np.isfinite(sl_pct) and np.isfinite(target_pct) and sl_pct > 0 and target_pct > 0:
        return float(sl_pct), float(target_pct)
    stop_px = _safe_float(row.get("sl_price_model"))
    target_px = _safe_float(row.get("target_price_model"))
    side = str(row.get("side", "")).upper().strip()
    if np.isfinite(entry_px) and entry_px > 0 and np.isfinite(stop_px) and np.isfinite(target_px):
        if side == "SHORT":
            sl_pct = (stop_px - entry_px) / entry_px * 100.0
            target_pct = (entry_px - target_px) / entry_px * 100.0
        else:
            sl_pct = (entry_px - stop_px) / entry_px * 100.0
            target_pct = (target_px - entry_px) / entry_px * 100.0
    if not np.isfinite(sl_pct) or sl_pct <= 0:
        sl_pct = 0.70
    if not np.isfinite(target_pct) or target_pct <= 0:
        target_pct = 1.00
    return float(sl_pct), float(target_pct)


def _profile_sl_target(profile: dict[str, Any], row: pd.Series, entry_px: float) -> tuple[float, float]:
    model_sl, model_target = _row_sl_target_pct(row, entry_px)
    sl_pct = model_sl if profile.get("sl_pct") is None else float(profile["sl_pct"])
    target_pct = model_target if profile.get("target_pct") is None else float(profile["target_pct"])
    return float(sl_pct), float(target_pct)


def _ret_pct(side: str, entry_px: float, exit_px: float) -> float:
    if not np.isfinite(entry_px) or entry_px <= 0 or not np.isfinite(exit_px):
        return np.nan
    if side == "SHORT":
        return (entry_px - exit_px) / entry_px * 100.0
    return (exit_px - entry_px) / entry_px * 100.0


def _path_stats(side: str, path: pd.DataFrame, entry_px: float) -> tuple[float, float]:
    if path.empty or not np.isfinite(entry_px) or entry_px <= 0:
        return np.nan, np.nan
    hi = pd.to_numeric(path["high"], errors="coerce").max()
    lo = pd.to_numeric(path["low"], errors="coerce").min()
    if side == "SHORT":
        mfe = (entry_px - lo) / entry_px * 100.0 if np.isfinite(lo) else np.nan
        mae = (hi - entry_px) / entry_px * 100.0 if np.isfinite(hi) else np.nan
    else:
        mfe = (hi - entry_px) / entry_px * 100.0 if np.isfinite(hi) else np.nan
        mae = (entry_px - lo) / entry_px * 100.0 if np.isfinite(lo) else np.nan
    return float(mfe), float(mae)


def _coverage_level(path_rows: int, coverage_pct: float) -> str:
    if path_rows <= 0:
        return "NONE"
    if coverage_pct >= 80.0 and path_rows >= 5:
        return "HIGH"
    if coverage_pct >= 45.0 and path_rows >= 3:
        return "MEDIUM"
    return "LOW"


def _empty_exit_profile_result(outcome: str = "NO_1MIN_PATH", expected_rows: float = np.nan) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "exit_price": np.nan,
        "exit_time_ist": "",
        "ret_pct": np.nan,
        "pnl_rs_model": np.nan,
        "path_rows": 0,
        "expected_1min_rows": int(expected_rows) if np.isfinite(expected_rows) else np.nan,
        "path_coverage_pct": 0.0,
        "path_coverage_level": "NONE",
        "mfe_pct_1min": np.nan,
        "mae_pct_1min": np.nan,
        "bars_held_1min": np.nan,
        "hit_order": "",
        "effective_sl_pct": np.nan,
        "effective_target_pct": np.nan,
    }


def _simulate_exit_profile(
    row: pd.Series,
    profile: dict[str, Any],
    bars: pd.DataFrame,
    entry_ts: pd.Timestamp,
    entry_px: float,
) -> dict[str, Any]:
    side = str(row.get("side", "")).upper().strip()
    if side not in {"LONG", "SHORT"} or bars.empty or pd.isna(entry_ts) or not np.isfinite(entry_px) or entry_px <= 0:
        return _empty_exit_profile_result()

    sl_pct, target_pct = _profile_sl_target(profile, row, entry_px)
    cutoff = _session_exit_cutoff(row.get("date", row.get("research_day", "")))
    max_hold_min = profile.get("max_hold_min")
    horizon = min(cutoff, entry_ts + pd.Timedelta(minutes=int(max_hold_min))) if max_hold_min else cutoff
    expected = max(1, int(np.floor((horizon - entry_ts.floor("min")).total_seconds() / 60.0)) + 1)
    path = bars.loc[(bars["date"] >= entry_ts.floor("min")) & (bars["date"] <= horizon)].sort_values("date").copy()
    if path.empty:
        result = _empty_exit_profile_result(expected_rows=expected)
        result["effective_sl_pct"] = float(sl_pct)
        result["effective_target_pct"] = float(target_pct)
        return result

    coverage_pct = float(min(100.0, len(path) / expected * 100.0))
    path_mfe, path_mae = _path_stats(side, path, entry_px)
    stop_px = entry_px * (1.0 - sl_pct / 100.0) if side == "LONG" else entry_px * (1.0 + sl_pct / 100.0)
    target_px = entry_px * (1.0 + target_pct / 100.0) if side == "LONG" else entry_px * (1.0 - target_pct / 100.0)
    outcome = "EOD"
    exit_px = _safe_float(path.iloc[-1].get("close"), entry_px)
    exit_ts = _normalise_ts(path.iloc[-1].get("date"))
    hit_order = ""
    kind = str(profile.get("kind", "static"))
    best_favorable = 0.0

    for _, bar in path.iterrows():
        ts = _normalise_ts(bar.get("date"))
        high = _safe_float(bar.get("high"))
        low = _safe_float(bar.get("low"))
        close = _safe_float(bar.get("close"), entry_px)
        if side == "SHORT":
            favorable = (entry_px - low) / entry_px * 100.0 if np.isfinite(low) else best_favorable
            stop_hit = np.isfinite(high) and high >= stop_px
            target_hit = np.isfinite(low) and low <= target_px
        else:
            favorable = (high - entry_px) / entry_px * 100.0 if np.isfinite(high) else best_favorable
            stop_hit = np.isfinite(low) and low <= stop_px
            target_hit = np.isfinite(high) and high >= target_px
        best_favorable = max(best_favorable, float(favorable) if np.isfinite(favorable) else 0.0)

        if stop_hit and target_hit:
            outcome = "SL"
            exit_px = stop_px
            exit_ts = ts
            hit_order = "same_bar_sl_first"
            break
        if stop_hit:
            outcome = "SL"
            exit_px = stop_px
            exit_ts = ts
            hit_order = "stop"
            break
        if target_hit:
            outcome = "TARGET"
            exit_px = target_px
            exit_ts = ts
            hit_order = "target"
            break
        if kind == "time_stop" and ts >= horizon:
            outcome = "TIME"
            exit_px = close
            exit_ts = ts
            hit_order = "time_stop"
            break

        if kind == "breakeven" and best_favorable >= float(profile.get("be_trigger_pct", 0.60)):
            stop_px = max(stop_px, entry_px) if side == "LONG" else min(stop_px, entry_px)
        elif kind == "trail" and best_favorable >= float(profile.get("trail_trigger_pct", 0.80)):
            gap = float(profile.get("trail_gap_pct", 0.35)) / 100.0
            if side == "SHORT" and np.isfinite(low):
                stop_px = min(stop_px, low * (1.0 + gap))
            elif side == "LONG" and np.isfinite(high):
                stop_px = max(stop_px, high * (1.0 - gap))

    quantity = _safe_float(row.get("paper_quantity"))
    pnl_rs_model = np.nan
    if np.isfinite(quantity) and quantity > 0:
        pnl_rs_model = (entry_px - exit_px) * quantity if side == "SHORT" else (exit_px - entry_px) * quantity

    return {
        "outcome": outcome,
        "exit_price": float(exit_px) if np.isfinite(exit_px) else np.nan,
        "exit_time_ist": _fmt_ts(exit_ts),
        "ret_pct": _ret_pct(side, entry_px, exit_px),
        "pnl_rs_model": float(pnl_rs_model) if np.isfinite(pnl_rs_model) else np.nan,
        "path_rows": int(len(path)),
        "expected_1min_rows": int(expected),
        "path_coverage_pct": coverage_pct,
        "path_coverage_level": _coverage_level(int(len(path)), coverage_pct),
        "mfe_pct_1min": path_mfe,
        "mae_pct_1min": path_mae,
        "bars_held_1min": int(max(0, (exit_ts - entry_ts.floor("min")).total_seconds() // 60)) if pd.notna(exit_ts) else np.nan,
        "hit_order": hit_order,
        "effective_sl_pct": float(sl_pct),
        "effective_target_pct": float(target_pct),
    }


def build_exit_strategy_lab(day: str, truth: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str, dict[str, Any]]:
    if truth is None or truth.empty:
        empty = pd.DataFrame()
        return empty, empty, f"# V7 Live 1-Min Exit Strategy Lab - {day}\n\nNo truth rows found.\n", {"day": day, "rows": 0}

    rows: list[dict[str, Any]] = []
    work = truth.copy()
    if "paper_traded" not in work.columns:
        work["paper_traded"] = False
    if "passed_v8_gate" not in work.columns:
        work["passed_v8_gate"] = False
    work["paper_traded"] = work["paper_traded"].fillna(False).astype(bool)
    work["passed_v8_gate"] = work["passed_v8_gate"].fillna(False).astype(bool)
    for _, row in work.iterrows():
        bars = _load_1min_bars(row.get("ticker"))
        entry_ts, entry_px, entry_source = _exit_lab_entry(row, bars)
        base = {
            "date": str(row.get("date", day))[:10],
            "research_key": row.get("research_key", ""),
            "candidate_id": row.get("candidate_id", ""),
            "ticker": str(row.get("ticker", "")).upper().strip(),
            "side": str(row.get("side", "")).upper().strip(),
            "setup": row.get("setup", ""),
            "signal_time_ist": row.get("signal_time_ist", ""),
            "paper_traded": bool(row.get("paper_traded", False)),
            "passed_v8_gate": bool(row.get("passed_v8_gate", False)),
            "reason_not_taken": row.get("reason_not_taken", ""),
            "cohort": "ACTUAL_PAPER" if bool(row.get("paper_traded", False)) else ("PASSED_NOT_TRADED" if bool(row.get("passed_v8_gate", False)) else "REJECTED_MISSED"),
            "entry_time_ist": _fmt_ts(entry_ts),
            "entry_price": float(entry_px) if np.isfinite(entry_px) else np.nan,
            "entry_source": entry_source,
            "one_min_source": str(DATA_1MIN_DIR),
            "one_min_last_bar": _fmt_ts(bars["date"].max()) if not bars.empty else "",
            "paper_outcome": row.get("paper_outcome", ""),
            "paper_pnl_rs": row.get("paper_pnl_rs", np.nan),
            "paper_pnl_pct": row.get("paper_pnl_pct", np.nan),
        }
        for profile in EXIT_LAB_PROFILES:
            sim = _simulate_exit_profile(row, profile, bars, entry_ts, entry_px)
            rows.append({**base, "exit_profile": profile["profile"], **sim})

    lab = pd.DataFrame(rows)
    summary = _exit_lab_summary(lab)
    report = _exit_lab_report(day, lab, summary)
    payload = {
        "day": day,
        "rows": int(len(lab)),
        "truth_rows": int(len(truth)),
        "profiles": [p["profile"] for p in EXIT_LAB_PROFILES],
        "one_min_dir": str(DATA_1MIN_DIR),
        "covered_profile_rows": int(lab["path_coverage_level"].isin(["HIGH", "MEDIUM"]).sum()) if not lab.empty else 0,
    }
    return lab, summary, report, payload


def _exit_lab_summary(lab: pd.DataFrame) -> pd.DataFrame:
    if lab is None or lab.empty:
        return pd.DataFrame()
    work = lab.copy()
    if "path_coverage_level" not in work.columns:
        work["path_coverage_level"] = "NONE"
    work["path_coverage_level"] = work["path_coverage_level"].fillna("NONE").astype(str)
    work["ret_pct"] = pd.to_numeric(work.get("ret_pct"), errors="coerce")
    work["mae_pct_1min"] = pd.to_numeric(work.get("mae_pct_1min"), errors="coerce")
    work["mfe_pct_1min"] = pd.to_numeric(work.get("mfe_pct_1min"), errors="coerce")
    work["usable_1min_path"] = work["path_coverage_level"].isin(["HIGH", "MEDIUM"])
    rows: list[dict[str, Any]] = []
    for (cohort, profile), group in work.groupby(["cohort", "exit_profile"], dropna=False):
        usable = group.loc[group["usable_1min_path"]].copy()
        ret = pd.to_numeric(usable.get("ret_pct"), errors="coerce").dropna()
        rows.append(
            {
                "cohort": cohort,
                "exit_profile": profile,
                "rows": int(len(group)),
                "usable_rows": int(len(usable)),
                "unique_candidates": int(group["research_key"].nunique()),
                "avg_ret_pct": float(ret.mean()) if not ret.empty else np.nan,
                "median_ret_pct": float(ret.median()) if not ret.empty else np.nan,
                "win_rate_pct": float((ret > 0).mean() * 100.0) if not ret.empty else np.nan,
                "target_rate_pct": float((usable["outcome"].astype(str) == "TARGET").mean() * 100.0) if not usable.empty else np.nan,
                "sl_rate_pct": float((usable["outcome"].astype(str) == "SL").mean() * 100.0) if not usable.empty else np.nan,
                "avg_mfe_pct": float(pd.to_numeric(usable.get("mfe_pct_1min"), errors="coerce").mean()) if not usable.empty else np.nan,
                "avg_mae_pct": float(pd.to_numeric(usable.get("mae_pct_1min"), errors="coerce").mean()) if not usable.empty else np.nan,
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["cohort", "avg_ret_pct", "usable_rows"], ascending=[True, False, False])
    return out


def _exit_lab_report(day: str, lab: pd.DataFrame, summary: pd.DataFrame) -> str:
    lines = [
        f"# V7 Live 1-Min Exit Strategy Lab - {day}",
        "",
        "Scope: live paper trades plus live-generated missed candidates only. No v7/v9 backtesting comparison is used.",
        f"1-minute source: `{DATA_1MIN_DIR}`",
        "",
    ]
    if lab.empty:
        lines.append("No exit lab rows found.")
        return "\n".join(lines) + "\n"

    coverage = lab.drop_duplicates(subset=["research_key"]).copy()
    if "path_coverage_level" not in coverage.columns:
        coverage["path_coverage_level"] = "NONE"
    coverage["path_coverage_level"] = coverage["path_coverage_level"].fillna("NONE").astype(str)
    coverage_counts = coverage["path_coverage_level"].value_counts(dropna=False).to_dict()
    latest_1m = sorted(set(str(x) for x in coverage.get("one_min_last_bar", pd.Series(dtype=str)).dropna().astype(str) if x))[-5:]
    lines.extend(
        [
            "## Coverage",
            "",
            f"- Truth candidates analysed: {coverage['research_key'].nunique()}",
            f"- Actual paper candidates: {int(coverage['paper_traded'].astype(bool).sum())}",
            f"- Potentially missed candidates: {int((~coverage['paper_traded'].astype(bool)).sum())}",
            f"- Usable 1-minute paths: {int(coverage['path_coverage_level'].isin(['HIGH', 'MEDIUM']).sum())}",
            f"- Coverage levels: {coverage_counts}",
            f"- Latest observed 1-minute bars sample: {', '.join(latest_1m) if latest_1m else 'NA'}",
            "",
        ]
    )

    lines.extend(["## Exit Profile Leaderboard", ""])
    if summary.empty:
        lines.append("No usable summary rows.")
    else:
        lines.append("| cohort | profile | usable | avg ret | win | target | SL | avg MFE | avg MAE |")
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
        for _, row in summary.head(30).iterrows():
            lines.append(
                f"| {row.get('cohort', '')} | {row.get('exit_profile', '')} | {int(row.get('usable_rows', 0))} | "
                f"{_fmt_num(row.get('avg_ret_pct'), 3)}% | {_fmt_num(row.get('win_rate_pct'), 1)}% | "
                f"{_fmt_num(row.get('target_rate_pct'), 1)}% | {_fmt_num(row.get('sl_rate_pct'), 1)}% | "
                f"{_fmt_num(row.get('avg_mfe_pct'), 3)}% | {_fmt_num(row.get('avg_mae_pct'), 3)}% |"
            )
    lines.append("")

    usable = lab.loc[lab["path_coverage_level"].isin(["HIGH", "MEDIUM"])].copy()
    lines.extend(["## Setup-Level Exit Suggestions", ""])
    if usable.empty:
        lines.append("No setup-level suggestions because usable 1-minute coverage is insufficient.")
    else:
        setup_rows: list[dict[str, Any]] = []
        for (cohort, side, setup, profile), group in usable.groupby(["cohort", "side", "setup", "exit_profile"], dropna=False):
            ret = pd.to_numeric(group.get("ret_pct"), errors="coerce").dropna()
            if len(ret) < 3:
                continue
            setup_rows.append(
                {
                    "cohort": cohort,
                    "side": side,
                    "setup": setup,
                    "exit_profile": profile,
                    "samples": int(len(ret)),
                    "avg_ret_pct": float(ret.mean()),
                    "win_rate_pct": float((ret > 0).mean() * 100.0),
                    "avg_mae_pct": float(pd.to_numeric(group.get("mae_pct_1min"), errors="coerce").mean()),
                }
            )
        setup_df = pd.DataFrame(setup_rows)
        if setup_df.empty:
            lines.append("No setup has at least 3 usable 1-minute samples yet.")
        else:
            best = setup_df.sort_values(["cohort", "side", "setup", "avg_ret_pct"], ascending=[True, True, True, False])
            best = best.drop_duplicates(subset=["cohort", "side", "setup"], keep="first")
            lines.append("| cohort | side | setup | best profile | samples | avg ret | win | avg MAE | suggestion strength |")
            lines.append("|---|---|---|---|---:|---:|---:|---:|---|")
            for _, row in best.sort_values(["cohort", "avg_ret_pct"], ascending=[True, False]).head(40).iterrows():
                strength = "PAPER_EXPERIMENT" if int(row["samples"]) < 8 else "SHADOW_ONLY"
                lines.append(
                    f"| {row['cohort']} | {row['side']} | {row['setup']} | {row['exit_profile']} | {int(row['samples'])} | "
                    f"{_fmt_num(row['avg_ret_pct'], 3)}% | {_fmt_num(row['win_rate_pct'], 1)}% | "
                    f"{_fmt_num(row['avg_mae_pct'], 3)}% | {strength} |"
                )
    lines.append("")

    missed = usable.loc[~usable["paper_traded"].astype(bool)].copy()
    missed["ret_pct"] = pd.to_numeric(missed["ret_pct"], errors="coerce")
    lines.extend(["## Potentially Missed Trades With Clean 1-Minute Follow-Through", ""])
    if missed.empty:
        lines.append("No missed candidates have usable 1-minute paths yet.")
    else:
        static_model = missed.loc[missed["exit_profile"].eq("static_model")].copy()
        static_model = static_model.sort_values(["ret_pct", "mfe_pct_1min"], ascending=[False, False]).head(20)
        lines.append("| time | ticker | side | setup | reason | model ret | MFE | MAE | coverage |")
        lines.append("|---|---|---|---|---|---:|---:|---:|---|")
        for _, row in static_model.iterrows():
            lines.append(
                f"| {row.get('signal_time_ist', '')} | {row.get('ticker', '')} | {row.get('side', '')} | "
                f"{row.get('setup', '')} | {row.get('reason_not_taken', '')} | {_fmt_num(row.get('ret_pct'), 3)}% | "
                f"{_fmt_num(row.get('mfe_pct_1min'), 3)}% | {_fmt_num(row.get('mae_pct_1min'), 3)}% | "
                f"{row.get('path_coverage_level', '')} |"
            )
    lines.append("")

    lines.extend(
        [
            "## Interpretation Guardrails",
            "",
            "- Treat LOW/NONE coverage as diagnostic only; do not promote exit rules from incomplete 1-minute paths.",
            "- Same-bar SL/target conflicts are resolved conservatively as SL first.",
            "- Dynamic profiles are research candidates for paper/shadow testing, not automatic live changes.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def build_multi_window_exit_strategy_lab(day: str) -> tuple[pd.DataFrame, pd.DataFrame, str, dict[str, Any]]:
    days = _available_truth_days(day)[-max(SUGGESTION_WINDOWS):]
    truth = _load_truth_days(days)
    if truth.empty:
        empty = pd.DataFrame()
        return empty, empty, f"# V7 Live 1-Min Exit Strategy Lab - Multi-Window - {day}\n\nNo historical live truth rows found.\n", {"day": day, "days": days, "rows": 0}
    lab, summary, _, payload = build_exit_strategy_lab(day, truth)
    report = _exit_lab_report(f"multi-window through {day}", lab, summary)
    payload = {**payload, "day": day, "available_sessions": days, "mode": "multi_window"}
    return lab, summary, report, payload


def _read_kv_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            out[key.strip()] = value.strip()
    except Exception:
        return {}
    return out


def _expected_nifty_slot_count(day: str) -> int:
    try:
        d = dt.datetime.strptime(day, "%Y-%m-%d").date()
    except Exception:
        return 0
    start = pd.Timestamp(dt.datetime.combine(d, dt.time(9, 20)), tz="Asia/Kolkata")
    end = pd.Timestamp(dt.datetime.combine(d, dt.time(15, 30)), tz="Asia/Kolkata")
    return int(len(pd.date_range(start, end, freq="5min"))) + 1  # + 09:15 open neutral marker


def _nifty_parquet_last_bar() -> str:
    try:
        if not NIFTY_5M_PARQUET.exists():
            return ""
        df = pd.read_parquet(NIFTY_5M_PARQUET, columns=["date"])
        if df.empty:
            return ""
        return _fmt_ts(df["date"].iloc[-1])
    except Exception:
        return ""


def _nifty_day_context(day: str, last_bar_ist: str, status: dict[str, str]) -> dict[str, Any]:
    ymd = str(day).replace("-", "")
    ready = sorted(NIFTY_SLOT_READY_DIR.glob(f"nifty_ready_{ymd}_*.json"))
    fail = sorted(NIFTY_SLOT_FAIL_DIR.glob(f"nifty_slot_fail_{ymd}_*.json"))
    open_slots = sorted(NIFTY_OPEN_SLOT_DIR.glob(f"nifty_open_slot_{ymd}_*.json"))
    expected = _expected_nifty_slot_count(day)
    marker_count = len(ready) + len(open_slots)
    coverage = float(marker_count / expected) if expected else np.nan
    last_ready = ""
    last_ready_bar = ""
    if ready:
        try:
            payload = json.loads(ready[-1].read_text(encoding="utf-8", errors="replace"))
            last_ready = str(payload.get("slot_key", ""))
            last_ready_bar = str(payload.get("last_bar_ist", ""))
        except Exception:
            last_ready = ready[-1].stem

    stale_for_day = True
    if last_bar_ist:
        ts = _normalise_ts(last_bar_ist)
        stale_for_day = bool(pd.isna(ts) or ts.strftime("%Y-%m-%d") < day)
    health = "OK"
    notes: list[str] = []
    if fail:
        health = "WARN"
        notes.append(f"{len(fail)} fail marker(s)")
    if marker_count == 0:
        health = "STALE_OR_CLOSED"
        notes.append("no NIFTY slot-ready/open markers")
    elif np.isfinite(coverage) and coverage < 0.80:
        health = "WARN"
        notes.append(f"low marker coverage {coverage * 100:.1f}%")
    if stale_for_day:
        health = "STALE_OR_CLOSED"
        notes.append(f"NIFTY parquet last bar not on {day}")
    if status and status.get("status", "").upper() not in {"STOPPED", "RUNNING", "OK"}:
        health = "WARN"
        notes.append(f"status={status.get('status')}")

    return {
        "day": day,
        "expected_slots": expected,
        "ready_markers": len(ready),
        "open_markers": len(open_slots),
        "fail_markers": len(fail),
        "marker_coverage_pct": coverage * 100 if np.isfinite(coverage) else np.nan,
        "last_ready_slot": last_ready,
        "last_ready_bar": last_ready_bar,
        "nifty_parquet_last_bar": last_bar_ist,
        "nifty_status": status.get("status", ""),
        "health": health,
        "note": "; ".join(notes) if notes else "NIFTY context usable",
    }


def _nifty_context(days: list[str], include_day: str) -> pd.DataFrame:
    audit_days = sorted(set([d for d in days if d] + [include_day]))
    status = _read_kv_file(NIFTY_STATUS_FILE)
    last_bar = _nifty_parquet_last_bar()
    rows = [_nifty_day_context(d, last_bar, status if d == include_day else {}) for d in audit_days]
    return pd.DataFrame(rows)


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 2:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _slot_ready_payload(day: str) -> dict[str, Any]:
    ymd = str(day).replace("-", "")
    markers = sorted(LIVE_5MIN_SLOT_READY_DIR.glob(f"slot_{ymd}_*.json"))
    if not markers:
        return {}
    return _read_json_file(markers[-1])


def _live_5min_context(day: str) -> dict[str, Any]:
    status = _read_json_file(LIVE_5MIN_STATUS_JSON)
    supervisor = _read_kv_file(LIVE_5MIN_SUPERVISOR_STATUS)
    slot_payload = _slot_ready_payload(day)
    source = status or slot_payload

    slot_ist = str(source.get("slot_ist") or slot_payload.get("slot_ist") or "")
    complete = source.get("complete", slot_payload.get("complete", ""))
    overall_state = str(source.get("overall_state", "") or ("OK" if complete is True else ""))
    failed = int(_safe_float(source.get("verification_failed_count", slot_payload.get("verification_failed_count", 0)), 0))
    expected = int(_safe_float(source.get("tickers_expected", slot_payload.get("tickers_expected", 0)), 0))
    written = int(_safe_float(source.get("tickers_written", slot_payload.get("tickers_written", 0)), 0))
    duration = _safe_float(source.get("total_elapsed_sec", slot_payload.get("duration_ms", np.nan)), np.nan)
    if np.isfinite(duration) and duration > 1000:
        duration = duration / 1000.0
    sla_state = str(source.get("sla_state", ""))
    supervisor_status = str(supervisor.get("status", ""))
    sample = source.get("verification_failure_sample", [])
    if isinstance(sample, list):
        sample_text = "; ".join(str(x) for x in sample[:5])
    else:
        sample_text = str(sample or "")

    health = "OK"
    notes: list[str] = []
    if not source:
        health = "MISSING"
        notes.append("no 5min fetch status/slot marker found")
    if overall_state.upper() == "FAIL" or failed > 0 or complete is False:
        health = "FAIL"
        notes.append(f"verification_failed={failed}")
    if supervisor_status and supervisor_status.upper() not in {"RUNNING", "STOPPED", "OK"}:
        health = "WARN" if health == "OK" else health
        notes.append(f"supervisor={supervisor_status}")
    if sla_state.upper() == "WARN":
        health = "WARN" if health == "OK" else health
        notes.append("SLA warn")
    if slot_ist:
        ts = _normalise_ts(slot_ist)
        if pd.isna(ts) or ts.strftime("%Y-%m-%d") < day:
            health = "STALE"
            notes.append(f"latest slot not on {day}")

    return {
        "day": day,
        "health": health,
        "slot_ist": slot_ist,
        "complete": complete,
        "overall_state": overall_state,
        "tickers_expected": expected,
        "tickers_written": written,
        "verification_failed_count": failed,
        "duration_sec": duration,
        "sla_state": sla_state,
        "supervisor_status": supervisor_status,
        "note": "; ".join(notes) if notes else "5min data fetch usable",
        "failure_sample": sample_text,
    }


def _runtime_component_status(name: str) -> dict[str, Any]:
    status = _read_kv_file(RUNTIME_STATUS_DIR / f"{name}.status")
    heartbeat = _read_kv_file(RUNTIME_STATUS_DIR / f"{name}.heartbeat")
    out = {
        "component": name,
        "status": status.get("status", ""),
        "reason": status.get("reason", ""),
        "heartbeat_state": heartbeat.get("state", ""),
        "updated": status.get("ts", "") or status.get("updated_at_ist", ""),
        "heartbeat_updated": heartbeat.get("ts", "") or heartbeat.get("updated_at_ist", ""),
    }
    return out


def _pf_gate_skipped_count(day: str) -> int:
    path = LIVE_SIGNALS_DIR / f"pf_gate_skipped_{day}_id_5min_v7_PAPER.csv"
    df = _read_csv(path)
    return int(len(df)) if not df.empty else 0


def _ops_audit_rows(day: str, truth: pd.DataFrame, live_5min: dict[str, Any], nifty_df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if truth.empty:
        rows.append(
            {
                "area": "Scanner Flow Audit",
                "severity": "WARN",
                "finding": "No truth rows were available for today's report.",
                "evidence": "truth table empty",
                "report_only_action": "Check signal discovery and entry engine outputs before evaluating strategy.",
            }
        )
    else:
        summary = _summary_counts(truth)
        raw = int(summary.get("raw_candidates", 0))
        gated = int(summary.get("passed_v8_gate", 0))
        entries = int(summary.get("selected_entries", 0))
        live = int(summary.get("live_signals", 0))
        paper = int(summary.get("paper_trades", 0))
        pf_skipped = _pf_gate_skipped_count(day)

        rows.append(
            {
                "area": "Scanner Flow Audit",
                "severity": "INFO",
                "finding": "Candidate-to-paper funnel snapshot.",
                "evidence": f"raw={raw}, gated={gated}, selected_entries={entries}, live_signals={live}, paper_trades={paper}",
                "report_only_action": "Use this funnel to locate drop-off before changing strategy logic.",
            }
        )

        if live != paper + pf_skipped:
            rows.append(
                {
                    "area": "Trade Sync Audit",
                    "severity": "WARN",
                    "finding": "Live signal count does not reconcile cleanly with paper trades plus PF-gate skips.",
                    "evidence": f"live_signals={live}, paper_trades={paper}, pf_gate_skipped={pf_skipped}",
                    "report_only_action": "Inspect paper executor rejection logs, duplicate signal IDs, and reservation/risk reject reasons.",
                }
            )
        else:
            rows.append(
                {
                    "area": "Trade Sync Audit",
                    "severity": "OK",
                    "finding": "Live signals reconcile with paper trades plus known PF-gate skips.",
                    "evidence": f"live_signals={live}, paper_trades={paper}, pf_gate_skipped={pf_skipped}",
                    "report_only_action": "No sync action needed.",
                }
            )

        audit_selected = int(summary.get("audit_selected_entries", 0))
        if entries != audit_selected:
            rows.append(
                {
                    "area": "Scanner Flow Audit",
                    "severity": "WARN",
                    "finding": "Selected entry count differs from audit-selected entry count.",
                    "evidence": f"selected_entries={entries}, audit_selected_entries={audit_selected}",
                    "report_only_action": "Review entry audit reconciliation before blaming scanner logic.",
                }
            )

    fetch_health = str(live_5min.get("health", ""))
    if fetch_health not in {"", "OK"}:
        rows.append(
            {
                "area": "Data Completeness Audit",
                "severity": "FAIL",
                "finding": "Live Data Fetch 5mins is not clean.",
                "evidence": str(live_5min.get("note", "")),
                "report_only_action": "Do not use this day for scanner improvement decisions; fix data fetch first.",
            }
        )
    if str(live_5min.get("sla_state", "")).upper() == "WARN":
        rows.append(
            {
                "area": "Fetch Performance Audit",
                "severity": "WARN",
                "finding": "5min fetch breached soft SLA.",
                "evidence": f"duration={_fmt_num(live_5min.get('duration_sec'), 1)}s, failed={live_5min.get('verification_failed_count', 0)}",
                "report_only_action": "Consider partition rebalancing, failing-symbol quarantine, or worker-budget review if this repeats on a valid trading day.",
            }
        )

    current_nifty = pd.DataFrame()
    if not nifty_df.empty and "day" in nifty_df.columns:
        current_nifty = nifty_df.loc[nifty_df["day"] == day]
    if not current_nifty.empty and str(current_nifty.iloc[0].get("health", "")) != "OK":
        rows.append(
            {
                "area": "Data Completeness Audit",
                "severity": "FAIL",
                "finding": "NIFTY Fetch 5min context is not clean.",
                "evidence": str(current_nifty.iloc[0].get("note", "")),
                "report_only_action": "Do not promote regime/RS/scanner changes from this day.",
            }
        )

    rows.append(
        {
            "area": "Logic Drift Audit",
            "severity": "INFO",
            "finding": "Paper executor has research PF gate while scanner remains full-universe.",
            "evidence": "PAPER_TRADE_TRUE can skip PF-gate candidates; scanner still writes all eligible V7 entries.",
            "report_only_action": "Expected during paper experiment mode; promote to scanner shadow only after clean-session proof.",
        }
    )

    for component in (
        "signal_discovery_v7_5mins_ID",
        "entry_engine_1min_v5_ID",
        "live_research_v7_research_layer",
    ):
        comp = _runtime_component_status(component)
        status = str(comp.get("status", "")).upper()
        severity = "OK" if status in {"RUNNING", "OK"} else ("WARN" if status else "UNKNOWN")
        rows.append(
            {
                "area": "Infra Health Audit",
                "severity": severity,
                "finding": f"{component} runtime status.",
                "evidence": f"status={comp.get('status', '')}, heartbeat={comp.get('heartbeat_state', '')}, updated={comp.get('updated', '') or comp.get('heartbeat_updated', '')}",
                "report_only_action": "Investigate stale/blank status only if dashboard also shows the card stale or stopped.",
            }
        )

    return rows


def _regime_pf_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "paper_traded" not in df.columns:
        return pd.DataFrame()
    traded = df.loc[df["paper_traded"].astype(bool)].copy()
    if traded.empty:
        return pd.DataFrame()
    traded["paper_pnl_rs"] = pd.to_numeric(traded["paper_pnl_rs"], errors="coerce").fillna(0.0)
    if "regime" not in traded.columns:
        traded["regime"] = "UNKNOWN"
    rows: list[dict[str, Any]] = []
    traded["regime"] = traded["regime"].fillna("UNKNOWN").replace("", "UNKNOWN").astype(str)
    traded.loc[traded["regime"].str.lower().isin({"nan", "none", "nat"}), "regime"] = "UNKNOWN"
    for regime, group in traded.groupby("regime", dropna=False):
        pnl = group["paper_pnl_rs"]
        rows.append(
            {
                "regime": str(regime or "UNKNOWN"),
                "trades": int(len(group)),
                "wins": int((pnl > 0).sum()),
                "net_pnl_rs": float(pnl.sum()),
                "profit_factor": _profit_factor(pnl),
                "avg_market_ret_pct": float(pd.to_numeric(group.get("market_ret_pct"), errors="coerce").mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(["net_pnl_rs", "trades"], ascending=[False, False])


def _fmt_pf(value: Any) -> str:
    val = _safe_float(value, np.nan)
    if np.isinf(val):
        return "inf"
    if not np.isfinite(val):
        return "NA"
    return _fmt_num(val, 2)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size <= 2:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _safe_int(value: Any, default: int = 0) -> int:
    val = _safe_float(value, np.nan)
    if not np.isfinite(val):
        return default
    return int(val)


def _summary_metric(summary: dict[str, Any], key: str, default: float = 0.0) -> float:
    if not isinstance(summary, dict):
        return default
    return _safe_float(summary.get(key, default), default)


def _top_setup_gap_rows(df: pd.DataFrame, pnl_col: str, limit: int = 5) -> list[dict[str, Any]]:
    if df.empty or "setup" not in df.columns:
        return []
    work = df.copy()
    if "side" not in work.columns:
        work["side"] = ""
    work["_pnl_for_gap"] = pd.to_numeric(work.get(pnl_col, pd.Series(0.0, index=work.index)), errors="coerce").fillna(0.0)
    rows: list[dict[str, Any]] = []
    for (side, setup), group in work.groupby(["side", "setup"], dropna=False):
        pnl = group["_pnl_for_gap"]
        rows.append(
            {
                "side": str(side or ""),
                "setup": str(setup or ""),
                "trades": int(len(group)),
                "wins": int((pnl > 0).sum()),
                "losses": int((pnl < 0).sum()),
                "net_pnl_rs": float(pnl.sum()),
                "profit_factor": _profit_factor(pnl),
            }
        )
    return sorted(rows, key=lambda r: (abs(float(r.get("net_pnl_rs", 0.0))), int(r.get("trades", 0))), reverse=True)[:limit]


def _v11_comparison_suggestion_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("status") != "OK":
        return []
    rows: list[dict[str, Any]] = []
    matched = _safe_int(payload.get("matched"))
    paper_only = _safe_int(payload.get("paper_only"))
    v11_only = _safe_int(payload.get("v11_only"))
    paper_trades = _safe_int(payload.get("paper_trades"))
    v11_trades = _safe_int(payload.get("v11_trades"))
    match_vs_paper = _safe_float(payload.get("match_rate_vs_paper_pct"), 0.0)
    pnl_gap = _safe_float(payload.get("pnl_gap_v11_minus_paper_rs"), 0.0)

    if paper_trades > 0 and match_vs_paper < 80.0:
        rows.append(
            {
                "window_sessions": 0,
                "level": "SHADOW_ONLY",
                "side": "COMPARISON",
                "setup": "V11_VS_V7_SIGNAL_MATCH",
                "evidence": f"matched {matched}/{paper_trades} paper trades; v11 trades {v11_trades}, paper-only {paper_only}, v11-only {v11_only}",
                "suggestion_type": "v11_live_parity_match_gap",
                "proposed_change": "Keep v11 comparison report-only until signal-key, entry-time, and executor skip differences are audited.",
                "apply_to": "research report / parity audit",
                "approval_required": "NO",
            }
        )
    if paper_only > 0:
        rows.append(
            {
                "window_sessions": 0,
                "level": "PAPER_EXPERIMENT",
                "side": "COMPARISON",
                "setup": "PAPER_ONLY_VS_V11",
                "evidence": f"{paper_only} V7 paper trades were absent from v11 live_parity.",
                "suggestion_type": "audit_paper_only_vs_v11",
                "proposed_change": "Bucket paper-only trades by setup/reason before using v11 backtest misses as strategy evidence.",
                "apply_to": "comparison report",
                "approval_required": "NO",
            }
        )
    if v11_only > 0:
        rows.append(
            {
                "window_sessions": 0,
                "level": "VIRTUAL_BACKTEST_ONLY",
                "side": "COMPARISON",
                "setup": "V11_ONLY_VS_PAPER",
                "evidence": f"{v11_only} v11 live_parity trades were absent from V7 paper.",
                "suggestion_type": "audit_v11_only_vs_paper",
                "proposed_change": "Inspect whether these were real paper skips, dedupe/capacity effects, or v11 candidate-source differences.",
                "apply_to": "comparison report",
                "approval_required": "NO",
            }
        )
    if abs(pnl_gap) >= 1000.0:
        rows.append(
            {
                "window_sessions": 0,
                "level": "PAPER_EXPERIMENT",
                "side": "COMPARISON",
                "setup": "V11_PNL_MODEL_PARITY",
                "evidence": f"v11 minus paper net PnL gap Rs {_fmt_num(pnl_gap)}.",
                "suggestion_type": "audit_v11_pnl_model_gap",
                "proposed_change": "Compare matched-trade entry/exit prices before accepting v11 backtest PnL as live-paper equivalent.",
                "apply_to": "backtest parity audit",
                "approval_required": "NO",
            }
        )
    return rows


def _v11_backtesting_comparison(day: str) -> tuple[list[str], dict[str, Any], list[dict[str, Any]]]:
    latest_json = BACKTEST_RESULT_V11_ROOT / "latest" / "latest_backtesting_result_v11.json"
    summary = _read_json(latest_json)
    lines = ["## V11 Backtesting Vs V7 Live Papertrade", ""]
    if not summary:
        payload = {
            "status": "MISSING",
            "day": day,
            "latest_json": str(latest_json),
            "note": "Backtesting Result v11 output is not available yet.",
        }
        lines.append(f"- Status: {payload['note']}")
        lines.append(f"- Expected source: `{latest_json}`")
        lines.append("")
        return lines, payload, []

    summary_day = str(summary.get("day", ""))
    if summary_day != day:
        payload = {
            "status": "STALE",
            "day": day,
            "latest_day": summary_day,
            "latest_json": str(latest_json),
            "note": "Latest Backtesting Result v11 is for a different day.",
        }
        lines.append(f"- Status: stale. Latest v11 comparison is for `{summary_day}`, not `{day}`.")
        lines.append(f"- Source: `{latest_json}`")
        lines.append("")
        return lines, payload, []

    out_dir_text = str(summary.get("out_dir", "") or "")
    out_dir = Path(out_dir_text) if out_dir_text else BACKTEST_RESULT_V11_ROOT / day
    v11_summary = summary.get("v11_summary", {}) if isinstance(summary.get("v11_summary"), dict) else {}
    paper_summary = summary.get("paper_summary", {}) if isinstance(summary.get("paper_summary"), dict) else {}
    matched = _safe_int(summary.get("matched"))
    v11_only = _safe_int(summary.get("v11_only"))
    paper_only = _safe_int(summary.get("paper_only"))
    v11_trades = _safe_int(v11_summary.get("trades"))
    paper_trades = _safe_int(paper_summary.get("trades"))
    match_vs_paper = (matched / paper_trades * 100.0) if paper_trades > 0 else np.nan
    match_vs_v11 = (matched / v11_trades * 100.0) if v11_trades > 0 else np.nan
    near_paper_5m = _safe_int(summary.get("paper_only_nearest_v11_within_5min"))
    near_paper_15m = _safe_int(summary.get("paper_only_nearest_v11_within_15min"))
    near_v11_5m = _safe_int(summary.get("v11_only_nearest_paper_within_5min"))
    near_v11_15m = _safe_int(summary.get("v11_only_nearest_paper_within_15min"))
    matched_pnl_gap = _safe_float(summary.get("matched_pnl_gap_v11_minus_paper_rs"), 0.0)
    v11_net = _summary_metric(v11_summary, "net")
    paper_net = _summary_metric(paper_summary, "net")
    pnl_gap = v11_net - paper_net

    paper_only_df = _read_csv(out_dir / "paper_only_vs_v11.csv")
    v11_only_df = _read_csv(out_dir / "v11_only_vs_paper.csv")
    matched_df = _read_csv(out_dir / "matched_v11_vs_paper.csv")
    paper_only_top = _top_setup_gap_rows(paper_only_df, "pnl_rs")
    v11_only_top = _top_setup_gap_rows(v11_only_df, "v6_net_pnl_rs")

    payload: dict[str, Any] = {
        "status": "OK",
        "day": day,
        "latest_json": str(latest_json),
        "report": str(summary.get("report", "")),
        "out_dir": str(out_dir),
        "exit_code": _safe_int(summary.get("exit_code")),
        "v11_trades": v11_trades,
        "paper_trades": paper_trades,
        "matched": matched,
        "v11_only": v11_only,
        "paper_only": paper_only,
        "paper_only_nearest_v11_within_5min": near_paper_5m,
        "paper_only_nearest_v11_within_15min": near_paper_15m,
        "v11_only_nearest_paper_within_5min": near_v11_5m,
        "v11_only_nearest_paper_within_15min": near_v11_15m,
        "matched_pnl_gap_v11_minus_paper_rs": matched_pnl_gap,
        "match_rate_vs_paper_pct": float(match_vs_paper) if np.isfinite(match_vs_paper) else np.nan,
        "match_rate_vs_v11_pct": float(match_vs_v11) if np.isfinite(match_vs_v11) else np.nan,
        "v11_net_pnl_rs": v11_net,
        "paper_net_pnl_rs": paper_net,
        "pnl_gap_v11_minus_paper_rs": pnl_gap,
        "v11_profit_factor": _summary_metric(v11_summary, "pf", np.nan),
        "paper_profit_factor": _summary_metric(paper_summary, "pf", np.nan),
        "matched_rows_available": int(len(matched_df)),
        "paper_only_top_setups": paper_only_top,
        "v11_only_top_setups": v11_only_top,
    }

    lines.append(f"- Source: `{latest_json}`")
    lines.append(f"- v11 output dir: `{out_dir}`")
    lines.append(f"- v11 report: `{summary.get('report', '')}`")
    lines.append("")
    lines.append("| source | trades | wins | losses | net pnl | PF |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    lines.append(
        f"| Backtesting Result v11 | {v11_trades} | {_safe_int(v11_summary.get('wins'))} | "
        f"{_safe_int(v11_summary.get('losses'))} | Rs {_fmt_num(v11_net)} | {_fmt_pf(v11_summary.get('pf'))} |"
    )
    lines.append(
        f"| V7 live papertrade | {paper_trades} | {_safe_int(paper_summary.get('wins'))} | "
        f"{_safe_int(paper_summary.get('losses'))} | Rs {_fmt_num(paper_net)} | {_fmt_pf(paper_summary.get('pf'))} |"
    )
    lines.append("")
    lines.append("| reconciliation | count | rate |")
    lines.append("|---|---:|---:|")
    lines.append(f"| matched exact signal keys | {matched} | {_fmt_num(match_vs_paper, 1)}% of paper / {_fmt_num(match_vs_v11, 1)}% of v11 |")
    lines.append(f"| v11-only trades | {v11_only} | {_fmt_num((v11_only / max(v11_trades, 1)) * 100.0, 1)}% of v11 |")
    lines.append(f"| paper-only trades | {paper_only} | {_fmt_num((paper_only / max(paper_trades, 1)) * 100.0, 1)}% of paper |")
    lines.append(f"| paper-only nearest v11 within 5m / 15m | {near_paper_5m} / {near_paper_15m} | timestamp-drift audit |")
    lines.append(f"| v11-only nearest paper within 5m / 15m | {near_v11_5m} / {near_v11_15m} | timestamp-drift audit |")
    lines.append(f"| exact-match PnL gap, v11 minus paper | Rs {_fmt_num(matched_pnl_gap)} | matched-trade audit |")
    lines.append(f"| PnL gap, v11 minus paper | Rs {_fmt_num(pnl_gap)} | report-only |")
    lines.append("")
    if paper_only_top:
        lines.append("Top V7 paper-only setup buckets:")
        lines.append("")
        lines.append("| side | setup | trades | wins | losses | net pnl | PF |")
        lines.append("|---|---|---:|---:|---:|---:|---:|")
        for row in paper_only_top:
            lines.append(
                f"| {row.get('side', '')} | {row.get('setup', '')} | {int(row.get('trades', 0))} | "
                f"{int(row.get('wins', 0))} | {int(row.get('losses', 0))} | Rs {_fmt_num(row.get('net_pnl_rs'))} | "
                f"{_fmt_pf(row.get('profit_factor'))} |"
            )
        lines.append("")
    if v11_only_top:
        lines.append("Top v11-only setup buckets:")
        lines.append("")
        lines.append("| side | setup | trades | wins | losses | net pnl | PF |")
        lines.append("|---|---|---:|---:|---:|---:|---:|")
        for row in v11_only_top:
            lines.append(
                f"| {row.get('side', '')} | {row.get('setup', '')} | {int(row.get('trades', 0))} | "
                f"{int(row.get('wins', 0))} | {int(row.get('losses', 0))} | Rs {_fmt_num(row.get('net_pnl_rs'))} | "
                f"{_fmt_pf(row.get('profit_factor'))} |"
            )
        lines.append("")
    lines.append(
        "- Interpretation: this is an extra parity/comparison layer. Keep the existing v7 live research analysis separate, and use this section to decide whether v11 backtest behavior is close enough to live paper before applying strategy changes."
    )
    lines.append("")
    return lines, payload, _v11_comparison_suggestion_rows(payload)


def _setup_coverage_status(row: pd.Series) -> str:
    if int(row.get("paper_trades", 0)) > 0:
        return "PAPER_TRADED"
    if int(row.get("live_signals", 0)) > 0:
        return "LIVE_SIGNAL_NO_PAPER"
    if int(row.get("selected_entries", 0)) > 0:
        return "ENTRY_SELECTED_NO_SIGNAL"
    if int(row.get("entry_rows", 0)) > 0:
        return "ENTRY_ROW_ONLY"
    if int(row.get("passed_v8_gate", 0)) > 0:
        return "GATED_ONLY"
    if int(row.get("raw_candidates", 0)) > 0:
        return "RAW_ONLY"
    return "NO_ROWS_IN_WINDOW"


def _setup_coverage_table(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "setup",
        "observed_sides",
        "first_day",
        "last_day",
        "research_rows",
        "raw_candidates",
        "passed_v8_gate",
        "entry_rows",
        "selected_entries",
        "live_signals",
        "paper_trades",
        "paper_pnl_rs",
        "profit_factor",
        "coverage_status",
    ]
    setup_universe = set(V7_LIVE_SETUP_UNIVERSE)
    if not df.empty and "setup" in df.columns:
        setup_universe.update(
            str(x).upper().strip()
            for x in df["setup"].dropna().astype(str)
            if str(x).strip()
        )
    if not setup_universe:
        return pd.DataFrame(columns=columns)

    work = df.copy()
    if not work.empty:
        work["_setup_norm"] = work.get("setup", pd.Series("", index=work.index)).fillna("").astype(str).str.upper().str.strip()
        work["_side_norm"] = work.get("side", pd.Series("", index=work.index)).fillna("").astype(str).str.upper().str.strip()
        work["_day_norm"] = work.get("date", pd.Series("", index=work.index)).fillna("").astype(str).str[:10]

    rows: list[dict[str, Any]] = []
    for setup in sorted(setup_universe):
        if work.empty:
            sub = pd.DataFrame()
        else:
            sub = work.loc[work["_setup_norm"].eq(setup)].copy()
        if sub.empty:
            rec = {
                "setup": setup,
                "observed_sides": "",
                "first_day": "",
                "last_day": "",
                "research_rows": 0,
                "raw_candidates": 0,
                "passed_v8_gate": 0,
                "entry_rows": 0,
                "selected_entries": 0,
                "live_signals": 0,
                "paper_trades": 0,
                "paper_pnl_rs": 0.0,
                "profit_factor": np.nan,
            }
            rec["coverage_status"] = _setup_coverage_status(pd.Series(rec))
            rows.append(rec)
            continue

        raw_mask = sub.get("scan_created_at_ist", pd.Series("", index=sub.index)).fillna("").astype(str).str.len() > 0
        passed = _bool_series(sub, "passed_v8_gate")
        entry_rows = _bool_series(sub, "entry_row_built")
        selected = _bool_series(sub, "entry_selected")
        live = _bool_series(sub, "live_signal_written")
        paper = _bool_series(sub, "paper_traded")
        pnl = pd.to_numeric(sub.get("paper_pnl_rs", pd.Series(0.0, index=sub.index)), errors="coerce").fillna(0.0)
        traded_pnl = pnl.loc[paper]
        days = sorted(
            d for d in sub["_day_norm"].dropna().astype(str).unique().tolist()
            if d and d.lower() not in {"nan", "none", "nat"}
        )
        sides = sorted(
            s for s in sub["_side_norm"].dropna().astype(str).unique().tolist()
            if s and s.lower() not in {"nan", "none", "nat"}
        )
        rec = {
            "setup": setup,
            "observed_sides": ",".join(sides),
            "first_day": days[0] if days else "",
            "last_day": days[-1] if days else "",
            "research_rows": int(len(sub)),
            "raw_candidates": int(raw_mask.sum()),
            "passed_v8_gate": int(passed.sum()),
            "entry_rows": int(entry_rows.sum()),
            "selected_entries": int(selected.sum()),
            "live_signals": int(live.sum()),
            "paper_trades": int(paper.sum()),
            "paper_pnl_rs": float(traded_pnl.sum()) if not traded_pnl.empty else 0.0,
            "profit_factor": _profit_factor(traded_pnl) if not traded_pnl.empty else np.nan,
        }
        rec["coverage_status"] = _setup_coverage_status(pd.Series(rec))
        rows.append(rec)

    return pd.DataFrame(rows, columns=columns)


def _coverage_side_label(observed_sides: Any) -> str:
    sides = [s for s in str(observed_sides or "").split(",") if s]
    if len(sides) == 1:
        return sides[0]
    return "BOTH" if sides else "UNKNOWN"


def _window_summary(window: int, days: list[str], df: pd.DataFrame) -> dict[str, Any]:
    traded = df.loc[_bool_series(df, "paper_traded")].copy() if not df.empty else pd.DataFrame()
    pnl = pd.to_numeric(traded.get("paper_pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    coverage = _setup_coverage_table(df)
    return {
        "window_sessions": window,
        "actual_sessions": len(days),
        "start_day": days[0] if days else "",
        "end_day": days[-1] if days else "",
        "research_rows": int(len(df)),
        "paper_trades": int(len(traded)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "net_pnl_rs": float(pnl.sum()),
        "profit_factor": _profit_factor(pnl) if not pnl.empty else np.nan,
        "v7_live_setup_universe": int(len(V7_LIVE_SETUP_UNIVERSE)),
        "setups_seen": int((coverage["research_rows"] > 0).sum()) if not coverage.empty else 0,
        "setups_gated": int((coverage["passed_v8_gate"] > 0).sum()) if not coverage.empty else 0,
        "setups_paper_traded": int((coverage["paper_trades"] > 0).sum()) if not coverage.empty else 0,
    }


def _suggestion_level(window: int, trades: int, net: float, pf: float, kind: str) -> str:
    if trades < 3:
        return "VIRTUAL_BACKTEST_ONLY"
    if kind == "bad_setup":
        if window >= 5 and trades >= 5 and net < 0 and np.isfinite(pf) and pf < 0.75:
            return "SHADOW_ONLY"
        if window >= 7 and trades >= 8 and net < 0 and np.isfinite(pf) and pf < 0.70:
            return "REAL_CHANGE_READY"
        return "PAPER_EXPERIMENT"
    if kind == "good_setup":
        if trades >= 5 and (np.isinf(pf) or pf >= 1.30) and net > 0:
            return "REAL_CHANGE_READY" if window >= 7 else "SHADOW_ONLY"
        return "VIRTUAL_BACKTEST_ONLY"
    if kind == "filter":
        if window >= 5 and trades >= 5:
            return "PAPER_EXPERIMENT"
        return "VIRTUAL_BACKTEST_ONLY"
    return "VIRTUAL_BACKTEST_ONLY"


def _build_setup_suggestions(window: int, df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty or "paper_traded" not in df.columns:
        return []
    traded = df.loc[_bool_series(df, "paper_traded")].copy()
    if traded.empty:
        return []
    traded["paper_pnl_rs"] = pd.to_numeric(traded["paper_pnl_rs"], errors="coerce").fillna(0.0)
    rows: list[dict[str, Any]] = []
    for (side, setup), group in traded.groupby(["side", "setup"], dropna=False):
        pnl = group["paper_pnl_rs"]
        trades = int(len(group))
        wins = int((pnl > 0).sum())
        net = float(pnl.sum())
        pf = _profit_factor(pnl)
        win_rate = float(wins / trades) if trades else np.nan
        side_s = str(side).upper().strip()
        setup_s = str(setup).upper().strip()
        if trades >= 3 and net < 0 and np.isfinite(pf) and pf < 0.85:
            level = _suggestion_level(window, trades, net, pf, "bad_setup")
            if setup_s == "B_AVWAP_RECLAIM_REVERSAL":
                proposed = "Require ranker_score >= 0.65 before this setup can be accepted."
                target = "paper true now; scanner shadow after approval; scanner active only after sustained proof"
            else:
                proposed = "Move setup to probation: add scanner shadow reject flag and keep paper-only validation."
                target = "scanner shadow first"
            rows.append(
                {
                    "window_sessions": window,
                    "level": level,
                    "side": side_s,
                    "setup": setup_s,
                    "evidence": f"{trades} trades, PF {_fmt_pf(pf)}, net Rs {_fmt_num(net)}, win rate {_fmt_num(win_rate * 100, 1)}%",
                    "suggestion_type": "restrict_weak_setup",
                    "proposed_change": proposed,
                    "apply_to": target,
                    "approval_required": "YES",
                }
            )
        elif trades >= 3 and net > 0 and (np.isinf(pf) or pf >= 1.25):
            level = _suggestion_level(window, trades, net, pf, "good_setup")
            rows.append(
                {
                    "window_sessions": window,
                    "level": level,
                    "side": side_s,
                    "setup": setup_s,
                    "evidence": f"{trades} trades, PF {_fmt_pf(pf)}, net Rs {_fmt_num(net)}, win rate {_fmt_num(win_rate * 100, 1)}%",
                    "suggestion_type": "keep_or_promote_setup",
                    "proposed_change": "Keep unchanged; do not tighten unless a separate indicator filter proves better.",
                    "apply_to": "scanner/executor no change",
                    "approval_required": "NO",
                }
            )
    return rows


def _build_indicator_suggestions(window: int, df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty or "paper_traded" not in df.columns:
        return []
    traded = df.loc[_bool_series(df, "paper_traded")].copy()
    if traded.empty:
        return []
    traded["paper_pnl_rs"] = pd.to_numeric(traded["paper_pnl_rs"], errors="coerce").fillna(0.0)
    rows: list[dict[str, Any]] = []

    long_df = traded.loc[traded.get("side", "").astype(str).str.upper() == "LONG"].copy()
    if not long_df.empty and {"close_loc", "vwap_dist_atr"}.issubset(long_df.columns):
        long_df["close_loc"] = pd.to_numeric(long_df["close_loc"], errors="coerce")
        long_df["vwap_dist_atr"] = pd.to_numeric(long_df["vwap_dist_atr"], errors="coerce")
        stretched = long_df.loc[(long_df["close_loc"] > 0.88) & (long_df["vwap_dist_atr"] > 0.52)]
        normal = long_df.drop(stretched.index)
        if len(stretched) >= 2:
            st_pnl = pd.to_numeric(stretched["paper_pnl_rs"], errors="coerce").fillna(0.0)
            nm_pnl = pd.to_numeric(normal["paper_pnl_rs"], errors="coerce").fillna(0.0)
            st_pf = _profit_factor(st_pnl)
            nm_pf = _profit_factor(nm_pnl)
            if float(st_pnl.sum()) < 0 or (np.isfinite(st_pf) and st_pf < 0.85):
                rows.append(
                    {
                        "window_sessions": window,
                        "level": _suggestion_level(window, int(len(stretched)), float(st_pnl.sum()), st_pf, "filter"),
                        "side": "LONG",
                        "setup": "ALL_LONG_SETUPS",
                        "evidence": (
                            f"stretched {len(stretched)} trades PF {_fmt_pf(st_pf)} net Rs {_fmt_num(st_pnl.sum())}; "
                            f"non-stretched {len(normal)} trades PF {_fmt_pf(nm_pf)} net Rs {_fmt_num(nm_pnl.sum())}"
                        ),
                        "suggestion_type": "anti_chase_filter",
                        "proposed_change": "For LONG entries, reject or shadow-flag close_loc > 0.88 AND vwap_dist_atr > 0.52.",
                        "apply_to": "paper true first; scanner shadow next",
                        "approval_required": "YES",
                    }
                )

    if "ranker_score" in traded.columns:
        scored = traded.copy()
        scored["ranker_score"] = pd.to_numeric(scored["ranker_score"], errors="coerce")
        low = scored.loc[scored["ranker_score"].notna() & (scored["ranker_score"] < 0.65)]
        high = scored.loc[scored["ranker_score"].notna() & (scored["ranker_score"] >= 0.65)]
        if len(low) >= 3 and len(high) >= 3:
            low_pnl = pd.to_numeric(low["paper_pnl_rs"], errors="coerce").fillna(0.0)
            high_pnl = pd.to_numeric(high["paper_pnl_rs"], errors="coerce").fillna(0.0)
            low_pf = _profit_factor(low_pnl)
            high_pf = _profit_factor(high_pnl)
            if (np.isfinite(low_pf) and low_pf < 0.90) and (np.isinf(high_pf) or high_pf > low_pf):
                rows.append(
                    {
                        "window_sessions": window,
                        "level": _suggestion_level(window, int(len(low)), float(low_pnl.sum()), low_pf, "filter"),
                        "side": "BOTH",
                        "setup": "RANKER_FILTER",
                        "evidence": (
                            f"ranker<0.65 {len(low)} trades PF {_fmt_pf(low_pf)} net Rs {_fmt_num(low_pnl.sum())}; "
                            f"ranker>=0.65 {len(high)} trades PF {_fmt_pf(high_pf)} net Rs {_fmt_num(high_pnl.sum())}"
                        ),
                        "suggestion_type": "ranker_quality_gate",
                        "proposed_change": "Use ranker_score >= 0.65 as setup-specific shadow gate; do not make global until more proof.",
                        "apply_to": "scanner shadow by setup",
                        "approval_required": "YES",
                    }
                )
    return rows


def _build_setup_coverage_suggestions(window: int, df: pd.DataFrame) -> list[dict[str, Any]]:
    coverage = _setup_coverage_table(df)
    if coverage.empty:
        return []

    rows: list[dict[str, Any]] = []
    active = coverage.loc[
        (pd.to_numeric(coverage["research_rows"], errors="coerce").fillna(0) > 0)
        & (pd.to_numeric(coverage["paper_trades"], errors="coerce").fillna(0) == 0)
    ].copy()
    if active.empty:
        return rows

    active["raw_candidates"] = pd.to_numeric(active["raw_candidates"], errors="coerce").fillna(0).astype(int)
    active["passed_v8_gate"] = pd.to_numeric(active["passed_v8_gate"], errors="coerce").fillna(0).astype(int)
    active["selected_entries"] = pd.to_numeric(active["selected_entries"], errors="coerce").fillna(0).astype(int)
    active["live_signals"] = pd.to_numeric(active["live_signals"], errors="coerce").fillna(0).astype(int)
    active["research_rows"] = pd.to_numeric(active["research_rows"], errors="coerce").fillna(0).astype(int)

    for _, row in active.iterrows():
        raw = int(row["raw_candidates"])
        gated = int(row["passed_v8_gate"])
        selected = int(row["selected_entries"])
        live = int(row["live_signals"])
        if gated >= 3:
            suggestion_type = "audit_active_untraded_setup"
            proposed = "Keep research-only; inspect entry-engine, live-signal, and executor drop-off before setup-level promotion or restriction."
        elif raw >= 10 and gated == 0:
            suggestion_type = "audit_gate_filtered_setup"
            proposed = "Keep research-only; review rejected forward outcomes and gate reasons before relaxing this setup."
        else:
            continue
        rows.append(
            {
                "window_sessions": window,
                "level": "VIRTUAL_BACKTEST_ONLY",
                "side": _coverage_side_label(row.get("observed_sides", "")),
                "setup": str(row.get("setup", "")).upper().strip(),
                "evidence": (
                    f"{int(row['research_rows'])} rows, raw {raw}, gated {gated}, "
                    f"selected {selected}, live {live}, paper 0"
                ),
                "suggestion_type": suggestion_type,
                "proposed_change": proposed,
                "apply_to": "research audit only",
                "approval_required": "NO",
            }
        )
    return rows


def _dedupe_suggestions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    priority = {
        "REAL_CHANGE_READY": 5,
        "SHADOW_ONLY": 4,
        "PAPER_EXPERIMENT": 3,
        "VIRTUAL_BACKTEST_ONLY": 2,
        "REJECT_CHANGE": 1,
    }
    best: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("suggestion_type")), str(row.get("side")), str(row.get("setup")))
        existing = best.get(key)
        if existing is None:
            best[key] = row
            continue
        old_score = priority.get(str(existing.get("level")), 0) * 100 + int(existing.get("window_sessions", 0))
        new_score = priority.get(str(row.get("level")), 0) * 100 + int(row.get("window_sessions", 0))
        if new_score >= old_score:
            best[key] = row
    return sorted(
        best.values(),
        key=lambda r: (
            -priority.get(str(r.get("level")), 0),
            -int(r.get("window_sessions", 0)),
            str(r.get("suggestion_type")),
            str(r.get("side")),
            str(r.get("setup")),
        ),
    )


def build_multi_window_suggestions(day: str) -> tuple[str, pd.DataFrame, dict[str, Any]]:
    available_days = _available_truth_days(day)
    summary_rows: list[dict[str, Any]] = []
    suggestion_rows: list[dict[str, Any]] = []

    for window in SUGGESTION_WINDOWS:
        days = available_days[-window:]
        df = _load_truth_days(days)
        summary_rows.append(_window_summary(window, days, df))
        suggestion_rows.extend(_build_setup_suggestions(window, df))
        suggestion_rows.extend(_build_indicator_suggestions(window, df))
        suggestion_rows.extend(_build_setup_coverage_suggestions(window, df))

    largest_days = available_days[-max(SUGGESTION_WINDOWS):]
    largest_df = _load_truth_days(largest_days)
    setup_coverage = _setup_coverage_table(largest_df)
    nifty_df = _nifty_context(largest_days, day)
    live_5min = _live_5min_context(day)
    regime_df = _regime_pf_table(largest_df)
    current_truth = _read_csv(TRUTH_DIR / f"truth_table_{day}.csv")
    ops_rows = _ops_audit_rows(day, truth=current_truth, live_5min=live_5min, nifty_df=nifty_df)
    v11_comparison_lines, v11_comparison_payload, v11_comparison_suggestions = _v11_backtesting_comparison(day)
    suggestion_rows.extend(v11_comparison_suggestions)
    if not nifty_df.empty:
        current_nifty = nifty_df.loc[nifty_df["day"] == day]
        if not current_nifty.empty and str(current_nifty.iloc[0].get("health", "")) != "OK":
            suggestion_rows.append(
                {
                    "window_sessions": 0,
                    "level": "REJECT_CHANGE",
                    "side": "CONTEXT",
                    "setup": "NIFTY_FETCH_5MIN",
                    "evidence": str(current_nifty.iloc[0].get("note", "")),
                    "suggestion_type": "data_context_guard",
                    "proposed_change": "Do not promote scanner/live changes from a day where NIFTY context is stale, closed, or incomplete.",
                    "apply_to": "research approval gate",
                    "approval_required": "NO",
                }
            )
    if live_5min.get("health") not in {"", "OK"}:
        suggestion_rows.append(
            {
                "window_sessions": 0,
                "level": "REJECT_CHANGE",
                "side": "CONTEXT",
                "setup": "LIVE_DATA_FETCH_5MIN",
                "evidence": str(live_5min.get("note", "")),
                "suggestion_type": "data_context_guard",
                "proposed_change": "Do not promote scanner/live changes from a day where 5min stock data fetch is failed, stale, or incomplete.",
                "apply_to": "research approval gate",
                "approval_required": "NO",
            }
        )

    suggestion_rows = _dedupe_suggestions(suggestion_rows)
    suggestions = pd.DataFrame(suggestion_rows)
    if suggestions.empty:
        suggestions = pd.DataFrame(
            columns=[
                "window_sessions",
                "level",
                "side",
                "setup",
                "suggestion_type",
                "evidence",
                "proposed_change",
                "apply_to",
                "approval_required",
            ]
        )
    summary_df = pd.DataFrame(summary_rows)

    lines = [
        f"# Suggestions v7 live research - Multi-Window Advisor - {day}",
        "",
        "This report is advisory. It does not change scanner/live code by itself.",
        "",
        "## Decision Levels",
        "",
        "| level | meaning |",
        "|---|---|",
        "| REAL_CHANGE_READY | Evidence is strong enough to propose a real scanner/live change after approval. |",
        "| SHADOW_ONLY | Add scanner marking/logging first; do not block signals yet. |",
        "| PAPER_EXPERIMENT | Test only in PAPER_TRADE_TRUE before scanner promotion. |",
        "| VIRTUAL_BACKTEST_ONLY | Interesting, but sample is too small or proof is incomplete. |",
        "| REJECT_CHANGE | Evidence argues against making this change. |",
        "",
        "## Window Health",
        "",
        "| window | actual sessions | period | rows | paper trades | wins | losses | net pnl | PF |",
        "|---:|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in summary_df.iterrows():
        period = f"{row.get('start_day', '')} to {row.get('end_day', '')}" if row.get("start_day") else "NA"
        lines.append(
            f"| {int(row['window_sessions'])} | {int(row['actual_sessions'])} | {period} | "
            f"{int(row['research_rows'])} | {int(row['paper_trades'])} | {int(row['wins'])} | {int(row['losses'])} | "
            f"Rs {_fmt_num(row['net_pnl_rs'])} | {_fmt_pf(row['profit_factor'])} |"
        )
    lines.append("")

    lines.extend(["## All V7 Live Setup Coverage", ""])
    lines.append(
        "- Universe source: `(avwap_5min_ID_v7_candidate_scan.ALLOWED_SETUPS - EXCLUDED_SETUPS - EARLY_BLOCKED_SETUPS) + eqidv2_v11_live_overlay.V11_PROFILE_SETUP_UNIVERSE`."
    )
    if setup_coverage.empty:
        lines.append("- No setup universe or truth rows available yet.")
    else:
        seen_count = int((setup_coverage["research_rows"] > 0).sum())
        gated_count = int((setup_coverage["passed_v8_gate"] > 0).sum())
        traded_count = int((setup_coverage["paper_trades"] > 0).sum())
        lines.append(
            f"- Coverage over largest available window: {seen_count}/{len(setup_coverage)} setups seen, "
            f"{gated_count} gated, {traded_count} paper-traded."
        )
        lines.append("")
        lines.append("| setup | sides | first | last | rows | raw | gated | entries | live | paper | pnl | PF | status |")
        lines.append("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        coverage_view = setup_coverage.sort_values(
            ["paper_trades", "live_signals", "selected_entries", "passed_v8_gate", "raw_candidates", "setup"],
            ascending=[False, False, False, False, False, True],
        )
        for _, row in coverage_view.iterrows():
            lines.append(
                f"| {row.get('setup', '')} | {row.get('observed_sides', '')} | {row.get('first_day', '')} | "
                f"{row.get('last_day', '')} | {int(row.get('research_rows', 0))} | "
                f"{int(row.get('raw_candidates', 0))} | {int(row.get('passed_v8_gate', 0))} | "
                f"{int(row.get('selected_entries', 0))} | {int(row.get('live_signals', 0))} | "
                f"{int(row.get('paper_trades', 0))} | Rs {_fmt_num(row.get('paper_pnl_rs'))} | "
                f"{_fmt_pf(row.get('profit_factor'))} | {row.get('coverage_status', '')} |"
            )
    lines.append("")

    lines.extend(v11_comparison_lines)

    lines.extend(["## Concrete Suggestions", ""])
    if suggestions.empty:
        lines.append("No concrete suggestions generated yet. Continue collecting sessions.")
    else:
        lines.append("| level | window | side | setup | type | evidence | proposed change | apply to | approval |")
        lines.append("|---|---:|---|---|---|---|---|---|---|")
        for _, row in suggestions.iterrows():
            lines.append(
                f"| {row.get('level', '')} | {int(row.get('window_sessions', 0))} | {row.get('side', '')} | "
                f"{row.get('setup', '')} | {row.get('suggestion_type', '')} | {row.get('evidence', '')} | "
                f"{row.get('proposed_change', '')} | {row.get('apply_to', '')} | {row.get('approval_required', '')} |"
            )
    lines.append("")

    exit_summary = _read_csv(EXIT_LAB_DIR / f"exit_strategy_lab_1min_multi_window_summary_{day}.csv")
    lines.extend(["## 1-Min Exit Strategy Lab Snapshot", ""])
    lines.append(f"- Source: `{DATA_1MIN_DIR}`")
    lines.append("- Scope: v7 live raw/gated candidates, v11 overlay pass/reject rows, entry-audit rows, live paper trades, live-generated missed candidates, v11 backtesting comparison output when available, and the full current v7 live setup universe.")
    if exit_summary.empty:
        lines.append("- Exit lab summary not available yet for this run.")
    elif not {"cohort", "exit_profile", "usable_rows", "avg_ret_pct"}.issubset(set(exit_summary.columns)):
        lines.append("- Exit lab summary exists but is missing expected columns; see exit_lab CSV for diagnostics.")
    else:
        actual = exit_summary.loc[exit_summary["cohort"].astype(str).eq("ACTUAL_PAPER")].copy()
        missed = exit_summary.loc[~exit_summary["cohort"].astype(str).eq("ACTUAL_PAPER")].copy()
        for label, frame in (("actual paper", actual), ("potentially missed", missed)):
            frame["usable_rows"] = pd.to_numeric(frame.get("usable_rows"), errors="coerce").fillna(0)
            frame["avg_ret_pct"] = pd.to_numeric(frame.get("avg_ret_pct"), errors="coerce")
            best = frame.loc[frame["usable_rows"] >= 3].sort_values("avg_ret_pct", ascending=False).head(3)
            if best.empty:
                lines.append(f"- {label}: no profile has at least 3 usable 1-minute samples yet.")
                continue
            bits = [
                f"{row.get('exit_profile')} ({int(row.get('usable_rows', 0))} samples, avg {_fmt_num(row.get('avg_ret_pct'), 3)}%)"
                for _, row in best.iterrows()
            ]
            lines.append(f"- {label}: " + "; ".join(bits))
    lines.append("")

    lines.extend(["## Operations And Flow Audit", ""])
    if not ops_rows:
        lines.append("No operations audit rows generated.")
    else:
        lines.append("| area | severity | finding | evidence | report-only action |")
        lines.append("|---|---|---|---|---|")
        for row in ops_rows:
            lines.append(
                f"| {row.get('area', '')} | {row.get('severity', '')} | {row.get('finding', '')} | "
                f"{row.get('evidence', '')} | {row.get('report_only_action', '')} |"
            )
    lines.append("")

    lines.extend(["## Live Data Fetch 5mins Context", ""])
    lines.append("| health | slot | complete | state | expected | written | failed | duration | SLA | supervisor | note |")
    lines.append("|---|---|---|---|---:|---:|---:|---:|---|---|---|")
    lines.append(
        f"| {live_5min.get('health', '')} | {live_5min.get('slot_ist', '')} | {live_5min.get('complete', '')} | "
        f"{live_5min.get('overall_state', '')} | {int(live_5min.get('tickers_expected', 0))} | "
        f"{int(live_5min.get('tickers_written', 0))} | {int(live_5min.get('verification_failed_count', 0))} | "
        f"{_fmt_num(live_5min.get('duration_sec'), 1)}s | {live_5min.get('sla_state', '')} | "
        f"{live_5min.get('supervisor_status', '')} | {live_5min.get('note', '')} |"
    )
    if live_5min.get("failure_sample"):
        lines.append("")
        lines.append(f"Failure sample: {live_5min.get('failure_sample')}")
    lines.append("")

    lines.extend(["## NIFTY Fetch 5min Context", ""])
    if nifty_df.empty:
        lines.append("No NIFTY context files found.")
    else:
        lines.append("| day | health | ready | open | fail | coverage | last ready | NIFTY last bar | note |")
        lines.append("|---|---|---:|---:|---:|---:|---|---|---|")
        for _, row in nifty_df.tail(20).iterrows():
            lines.append(
                f"| {row.get('day', '')} | {row.get('health', '')} | {int(row.get('ready_markers', 0))} | "
                f"{int(row.get('open_markers', 0))} | {int(row.get('fail_markers', 0))} | "
                f"{_fmt_num(row.get('marker_coverage_pct'), 1)}% | {row.get('last_ready_slot', '')} | "
                f"{row.get('nifty_parquet_last_bar', '')} | {row.get('note', '')} |"
            )
    lines.append("")

    lines.extend(["## PF By NIFTY/Market Regime", ""])
    if regime_df.empty:
        lines.append("No traded regime data found.")
    else:
        lines.append("| regime | trades | wins | net pnl | PF | avg market ret |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for _, row in regime_df.iterrows():
            lines.append(
                f"| {row.get('regime', '')} | {int(row.get('trades', 0))} | {int(row.get('wins', 0))} | "
                f"Rs {_fmt_num(row.get('net_pnl_rs'))} | {_fmt_pf(row.get('profit_factor'))} | "
                f"{_fmt_num(row.get('avg_market_ret_pct'), 3)}% |"
            )
    lines.append("")

    lines.extend(
        [
            "## Daily Evaluation Checklist",
            "",
            "- Check whether any REAL_CHANGE_READY item has at least 5 actual sessions behind it.",
            "- Promote weak setup filters in this order: virtual -> paper -> scanner shadow -> scanner active -> live false.",
            "- Reject any change that improves PF only by removing too many winners or leaving fewer than 3 trades in the window.",
            "- Keep live false protected until scanner shadow and paper evidence agree.",
            "",
        ]
    )

    payload = {
        "day": day,
        "available_sessions": available_days,
        "windows": summary_rows,
        "suggestion_count": int(len(suggestions)),
        "real_change_ready_count": int((suggestions.get("level", pd.Series(dtype=str)) == "REAL_CHANGE_READY").sum()) if not suggestions.empty else 0,
        "shadow_only_count": int((suggestions.get("level", pd.Series(dtype=str)) == "SHADOW_ONLY").sum()) if not suggestions.empty else 0,
        "paper_experiment_count": int((suggestions.get("level", pd.Series(dtype=str)) == "PAPER_EXPERIMENT").sum()) if not suggestions.empty else 0,
        "operations_audit": ops_rows,
        "v11_backtesting_comparison": v11_comparison_payload,
        "live_5min_context": live_5min,
        "nifty_context": nifty_df.to_dict("records") if not nifty_df.empty else [],
        "regime_pf": regime_df.to_dict("records") if not regime_df.empty else [],
        "v7_live_setup_universe": V7_LIVE_SETUP_UNIVERSE,
        "setup_coverage": setup_coverage.to_dict("records") if not setup_coverage.empty else [],
        "setup_coverage_summary": {
            "universe_setups": int(len(setup_coverage)) if not setup_coverage.empty else int(len(V7_LIVE_SETUP_UNIVERSE)),
            "seen_setups": int((setup_coverage["research_rows"] > 0).sum()) if not setup_coverage.empty else 0,
            "gated_setups": int((setup_coverage["passed_v8_gate"] > 0).sum()) if not setup_coverage.empty else 0,
            "paper_traded_setups": int((setup_coverage["paper_trades"] > 0).sum()) if not setup_coverage.empty else 0,
        },
    }
    return "\n".join(lines) + "\n", suggestions, payload


def run(day: str) -> tuple[Path, Path]:
    truth = add_ranker_scores(day, build_truth_table(day))
    truth_path = TRUTH_DIR / f"truth_table_{day}.csv"
    report_path = REPORT_DIR / f"reality_gap_{day}.md"
    action_path = REPORT_DIR / f"eod_action_plan_{day}.md"
    ranker_path = RANKER_DIR / f"candidate_ranker_{day}.csv"
    ranker_report_path = REPORT_DIR / f"candidate_ranker_{day}.md"
    suggestions_path = SUGGESTIONS_DIR / f"multi_window_suggestions_{day}.md"
    suggestions_csv_path = SUGGESTIONS_DIR / f"multi_window_suggestions_{day}.csv"
    suggestions_json_path = SUGGESTIONS_DIR / f"multi_window_suggestions_{day}.json"
    exit_lab_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_{day}.csv"
    exit_lab_summary_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_summary_{day}.csv"
    exit_lab_report_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_{day}.md"
    exit_lab_json_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_{day}.json"
    exit_lab_multi_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_multi_window_{day}.csv"
    exit_lab_multi_summary_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_multi_window_summary_{day}.csv"
    exit_lab_multi_report_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_multi_window_{day}.md"
    exit_lab_multi_json_path = EXIT_LAB_DIR / f"exit_strategy_lab_1min_multi_window_{day}.json"
    deep_analysis_path = DEEP_ANALYSIS_DIR / f"deep_analysis_block_{day}.csv"
    deep_analysis_report_path = DEEP_ANALYSIS_DIR / f"deep_analysis_block_{day}.md"
    deep_analysis_json_path = DEEP_ANALYSIS_DIR / f"deep_analysis_block_{day}.json"
    truth.to_csv(truth_path, index=False)
    truth.to_csv(ranker_path, index=False)
    report_text = write_report(day, truth)
    action_text = write_action_plan(day, truth)
    ranker_text = write_ranker_report(day, truth)
    deep_df, deep_payload = build_deep_analysis(day, truth)
    deep_text = deep_analysis_report(day, truth, standalone=True, limit=50)
    exit_lab, exit_lab_summary, exit_lab_text, exit_lab_payload = build_exit_strategy_lab(day, truth)
    exit_lab.to_csv(exit_lab_path, index=False)
    exit_lab_summary.to_csv(exit_lab_summary_path, index=False)
    exit_lab_report_path.write_text(exit_lab_text, encoding="utf-8")
    exit_lab_json_path.write_text(json.dumps(exit_lab_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    exit_lab_multi, exit_lab_multi_summary, exit_lab_multi_text, exit_lab_multi_payload = build_multi_window_exit_strategy_lab(day)
    exit_lab_multi.to_csv(exit_lab_multi_path, index=False)
    exit_lab_multi_summary.to_csv(exit_lab_multi_summary_path, index=False)
    exit_lab_multi_report_path.write_text(exit_lab_multi_text, encoding="utf-8")
    exit_lab_multi_json_path.write_text(json.dumps(exit_lab_multi_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    suggestions_text, suggestions_df, suggestions_payload = build_multi_window_suggestions(day)
    deep_df.to_csv(deep_analysis_path, index=False)
    deep_analysis_report_path.write_text(deep_text, encoding="utf-8")
    deep_analysis_json_path.write_text(json.dumps(deep_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    report_path.write_text(report_text, encoding="utf-8")
    action_path.write_text(action_text, encoding="utf-8")
    ranker_report_path.write_text(ranker_text, encoding="utf-8")
    suggestions_path.write_text(suggestions_text, encoding="utf-8")
    suggestions_df.to_csv(suggestions_csv_path, index=False)
    suggestions_json_path.write_text(json.dumps(suggestions_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    (LATEST_DIR / "latest_truth_table.csv").write_text(truth.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_candidate_ranker.csv").write_text(truth.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_reality_gap.md").write_text(report_text, encoding="utf-8")
    (LATEST_DIR / "latest_eod_action_plan.md").write_text(action_text, encoding="utf-8")
    (LATEST_DIR / "latest_candidate_ranker.md").write_text(ranker_text, encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min.csv").write_text(exit_lab.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min_summary.csv").write_text(exit_lab_summary.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min.md").write_text(exit_lab_text, encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min.json").write_text(json.dumps(exit_lab_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min_multi_window.csv").write_text(exit_lab_multi.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min_multi_window_summary.csv").write_text(exit_lab_multi_summary.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min_multi_window.md").write_text(exit_lab_multi_text, encoding="utf-8")
    (LATEST_DIR / "latest_exit_strategy_lab_1min_multi_window.json").write_text(json.dumps(exit_lab_multi_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    (LATEST_DIR / "latest_deep_analysis_block.csv").write_text(deep_df.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_deep_analysis_block.md").write_text(deep_text, encoding="utf-8")
    (LATEST_DIR / "latest_deep_analysis_block.json").write_text(json.dumps(deep_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    (LATEST_DIR / "latest_multi_window_suggestions.md").write_text(suggestions_text, encoding="utf-8")
    (LATEST_DIR / "latest_multi_window_suggestions.csv").write_text(suggestions_df.to_csv(index=False), encoding="utf-8")
    (LATEST_DIR / "latest_multi_window_suggestions.json").write_text(json.dumps(suggestions_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    summary = {
        "day": day,
        "truth_table": str(truth_path),
        "report": str(report_path),
        "action_plan": str(action_path),
        "ranker_csv": str(ranker_path),
        "ranker_report": str(ranker_report_path),
        "exit_strategy_lab_1min": str(exit_lab_path),
        "exit_strategy_lab_1min_summary": str(exit_lab_summary_path),
        "exit_strategy_lab_1min_report": str(exit_lab_report_path),
        "exit_strategy_lab_1min_json": str(exit_lab_json_path),
        "exit_strategy_lab_1min_multi_window": str(exit_lab_multi_path),
        "exit_strategy_lab_1min_multi_window_summary": str(exit_lab_multi_summary_path),
        "exit_strategy_lab_1min_multi_window_report": str(exit_lab_multi_report_path),
        "exit_strategy_lab_1min_multi_window_json": str(exit_lab_multi_json_path),
        "deep_analysis_block": str(deep_analysis_path),
        "deep_analysis_block_report": str(deep_analysis_report_path),
        "deep_analysis_block_json": str(deep_analysis_json_path),
        "deep_analysis_setup_rows": int(deep_payload.get("setup_rows", 0)),
        "deep_analysis_paper_traded_setups": int(deep_payload.get("paper_traded_setups", 0)),
        "deep_analysis_negative_setup_rows": int(deep_payload.get("negative_setup_rows", 0)),
        "exit_strategy_lab_1min_rows": int(len(exit_lab)),
        "exit_strategy_lab_1min_usable_rows": int(exit_lab["path_coverage_level"].isin(["HIGH", "MEDIUM"]).sum()) if not exit_lab.empty else 0,
        "exit_strategy_lab_1min_multi_window_rows": int(len(exit_lab_multi)),
        "exit_strategy_lab_1min_multi_window_usable_rows": int(exit_lab_multi["path_coverage_level"].isin(["HIGH", "MEDIUM"]).sum()) if not exit_lab_multi.empty else 0,
        "exit_strategy_lab_1min_source": str(DATA_1MIN_DIR),
        "multi_window_suggestions": str(suggestions_path),
        "multi_window_suggestions_csv": str(suggestions_csv_path),
        "multi_window_suggestions_json": str(suggestions_json_path),
        "multi_window_suggestion_count": int(suggestions_payload.get("suggestion_count", 0)),
        "multi_window_real_change_ready_count": int(suggestions_payload.get("real_change_ready_count", 0)),
        "v7_live_setup_universe_count": int(
            suggestions_payload.get("setup_coverage_summary", {}).get("universe_setups", len(V7_LIVE_SETUP_UNIVERSE))
        ),
        "v7_live_setups_seen_count": int(
            suggestions_payload.get("setup_coverage_summary", {}).get("seen_setups", 0)
        ),
        "v7_live_setups_gated_count": int(
            suggestions_payload.get("setup_coverage_summary", {}).get("gated_setups", 0)
        ),
        "v7_live_setups_paper_traded_count": int(
            suggestions_payload.get("setup_coverage_summary", {}).get("paper_traded_setups", 0)
        ),
        **_summary_counts(truth),
    }
    (LATEST_DIR / "latest_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return truth_path, report_path


def _now_ist() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Kolkata")


def _write_status(status: str, **extra: Any) -> None:
    payload = {
        "status": status,
        "session": "Suggestions v7 live research",
        "updated_at_ist": _fmt_ts(_now_ist()),
        **extra,
    }
    text = json.dumps(payload, indent=2, sort_keys=True, default=str)
    kv_text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    (HEARTBEAT_DIR / "v7_research_layer.status.json").write_text(text, encoding="utf-8")
    (HEARTBEAT_DIR / "v7_research_layer.heartbeat.json").write_text(text, encoding="utf-8")
    (RUNTIME_STATUS_DIR / "live_research_v7_research_layer.status").write_text(kv_text, encoding="utf-8")
    (RUNTIME_STATUS_DIR / "live_research_v7_research_layer.heartbeat").write_text(kv_text, encoding="utf-8")


def _light_ops_console_lines(summary: dict[str, Any]) -> list[str]:
    fetch = summary.get("fetch", {}) if isinstance(summary.get("fetch"), dict) else {}
    signal = summary.get("signal", {}) if isinstance(summary.get("signal"), dict) else {}
    entry = summary.get("entry", {}) if isinstance(summary.get("entry"), dict) else {}
    pre = summary.get("pre_momentum", {}) if isinstance(summary.get("pre_momentum"), dict) else {}
    paper = summary.get("paper", {}) if isinstance(summary.get("paper"), dict) else {}
    recs = summary.get("recommendations", [])
    if not isinstance(recs, list):
        recs = []

    lines = [
        (
            "[v7_research_layer ops] "
            f"5m_fetch={fetch.get('duration_sec', '')}s "
            f"state={fetch.get('overall_state', '')}/{fetch.get('sla_state', '')} "
            f"written={fetch.get('tickers_written', 0)}/{fetch.get('tickers_expected', 0)}"
        ),
        (
            "[v7_research_layer ops] "
            f"scanner_slot={signal.get('slot_ist', '')} "
            f"publish_delay={signal.get('publish_delay_sec', '')}s "
            f"raw={signal.get('raw_candidates', 0)} "
            f"v11_in={signal.get('v11_input', 0)} "
            f"selected={signal.get('v11_selected', 0)} "
            f"tier123={signal.get('tier123_scan_elapsed_sec', '')}s/{signal.get('tier123_workers', 0)}w"
        ),
        (
            "[v7_research_layer ops] "
            f"entry_slot={entry.get('slot_ist', '')} "
            f"raw_fetch={entry.get('raw_fetch_elapsed_sec', '')}s "
            f"entries={entry.get('entry_rows', 0)} "
            f"pre_pass={entry.get('pre_momentum_output_rows', 0)}/{entry.get('pre_momentum_input_rows', 0)} "
            f"latest_nan_rejects={pre.get('latest_nan_rejects', 0)}"
        ),
        (
            "[v7_research_layer ops] "
            f"paper_traded={paper.get('paper_traded_rows', 0)} "
            f"target={paper.get('targets', 0)} "
            f"sl={paper.get('sl', 0)} "
            f"open={paper.get('open_trades', 0)} "
            f"slow_open={paper.get('slow_open_trades', 0)} "
            f"anti_chase_skips={paper.get('anti_chase_skips', 0)}"
        ),
    ]
    for rec in recs[:6]:
        if isinstance(rec, dict):
            lines.append(
                "[v7_research_layer suggestion] "
                f"{rec.get('severity', '')} {rec.get('area', '')}: "
                f"{rec.get('finding', '')} -> {rec.get('suggestion', '')}"
            )
    return lines


def _parse_today_time(day: dt.date, value: str) -> pd.Timestamp:
    parsed = dt.datetime.strptime(str(value), "%H:%M:%S").time()
    return pd.Timestamp(dt.datetime.combine(day, parsed), tz="Asia/Kolkata")


def _next_run_time(now: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp, interval_min: int) -> pd.Timestamp | None:
    if now < start:
        return start
    end_grace = pd.Timedelta(seconds=60)
    if now > end + end_grace:
        return None
    if now > end:
        return end
    interval = pd.Timedelta(minutes=max(1, int(interval_min)))
    elapsed = now - start
    steps = int(np.floor(elapsed / interval))
    due_slot = start + steps * interval
    if pd.Timedelta(0) <= now - due_slot <= pd.Timedelta(seconds=60):
        return due_slot
    candidate = due_slot + interval
    if candidate > end:
        return end if now <= end else None
    return candidate


def run_loop(*, start_time: str, end_time: str, interval_min: int, light_ops: bool = False) -> int:
    today = _now_ist().date()
    start = _parse_today_time(today, start_time)
    end = _parse_today_time(today, end_time)
    last_run_key = ""
    _write_status(
        "RUNNING",
        phase="LOOP_START",
        mode="light_ops" if light_ops else "full_research",
        start_time=start_time,
        end_time=end_time,
        interval_min=int(interval_min),
    )
    while True:
        now = _now_ist()
        if now.date() != today:
            _write_status("STOPPED", phase="DATE_ROLLED", reason="date_changed")
            return 0
        next_run = _next_run_time(now, start, end, interval_min)
        if next_run is None:
            _write_status("STOPPED", phase="DONE", reason="after_end_time", end_time=end_time)
            return 0
        wait_sec = max(0.0, (next_run - now).total_seconds())
        _write_status(
            "RUNNING",
            phase="WAIT",
            mode="light_ops" if light_ops else "full_research",
            next_run_ist=_fmt_ts(next_run),
            wait_sec=round(wait_sec, 1),
            start_time=start_time,
            end_time=end_time,
            interval_min=int(interval_min),
        )
        if wait_sec > 0:
            time.sleep(min(wait_sec, 60.0))
            continue

        run_key = _fmt_ts(next_run)
        if run_key == last_run_key:
            interval = pd.Timedelta(minutes=max(1, int(interval_min)))
            sleep_sec = max(1.0, min(60.0, ((next_run + interval) - now).total_seconds()))
            time.sleep(sleep_sec)
            continue
        last_run_key = run_key
        day = next_run.strftime("%Y-%m-%d")
        _write_status("RUNNING", phase="BUILD_LIGHT_OPS" if light_ops else "BUILD_REPORT", mode="light_ops" if light_ops else "full_research", run_time_ist=run_key, day=day)
        try:
            if light_ops:
                report_path, json_path = run_light_ops(day)
                summary = json.loads((LATEST_DIR / "latest_live_ops_snapshot.json").read_text(encoding="utf-8"))
                _write_status(
                    "RUNNING",
                    phase="LIGHT_OPS_DONE",
                    mode="light_ops",
                    run_time_ist=run_key,
                    day=day,
                    report=str(report_path),
                    json=str(json_path),
                    latest_signal_slot=summary.get("signal", {}).get("slot_ist", ""),
                    latest_entry_slot=summary.get("entry", {}).get("slot_ist", ""),
                    paper_open_trades=summary.get("paper", {}).get("open_trades", 0),
                    paper_slow_open_trades=summary.get("paper", {}).get("slow_open_trades", 0),
                    pre_momentum_nan_rejects=summary.get("pre_momentum", {}).get("nan_rejects", 0),
                )
                print(f"[v7_research_layer loop] {run_key} wrote light ops {report_path}", flush=True)
                for line in _light_ops_console_lines(summary):
                    print(line, flush=True)
            else:
                truth_path, report_path = run(day)
                summary = json.loads((LATEST_DIR / "latest_summary.json").read_text(encoding="utf-8"))
                status_summary = {k: v for k, v in summary.items() if k not in {"day", "truth_table", "report"}}
                _write_status(
                    "RUNNING",
                    phase="REPORT_DONE",
                    mode="full_research",
                    run_time_ist=run_key,
                    day=day,
                    truth_table=str(truth_path),
                    report=str(report_path),
                    **status_summary,
                )
                print(f"[v7_research_layer loop] {run_key} wrote {truth_path}", flush=True)
        except Exception as exc:
            _write_status("ERROR", phase="REPORT_FAILED", mode="light_ops" if light_ops else "full_research", run_time_ist=run_key, error=f"{type(exc).__name__}: {exc}")
            print(f"[v7_research_layer loop] ERROR {type(exc).__name__}: {exc}", flush=True)
        time.sleep(1.0)


def _default_day() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build V7 ID 5-min research-layer truth table and report")
    ap.add_argument("--date", default=_default_day(), help="Trading date YYYY-MM-DD; default today IST")
    ap.add_argument("--loop", action="store_true", help="Run repeatedly during the market session")
    ap.add_argument("--light-ops", action="store_true", help="In loop mode, run lightweight operations diagnostics only")
    ap.add_argument("--start-time", default="09:17:30", help="Loop start time HH:MM:SS IST")
    ap.add_argument("--end-time", default="16:00:00", help="Loop end time HH:MM:SS IST")
    ap.add_argument("--interval-min", type=int, default=15, help="Loop interval in minutes")
    args = ap.parse_args()
    if args.loop:
        return run_loop(start_time=str(args.start_time), end_time=str(args.end_time), interval_min=int(args.interval_min), light_ops=bool(args.light_ops))
    if args.light_ops:
        report_path, json_path = run_light_ops(str(args.date))
        summary = json.loads((LATEST_DIR / "latest_live_ops_snapshot.json").read_text(encoding="utf-8"))
        print(f"[v7_research_layer] wrote light ops {report_path}")
        print(f"[v7_research_layer] wrote light ops {json_path}")
        for line in _light_ops_console_lines(summary):
            print(line, flush=True)
        return 0
    truth_path, report_path = run(str(args.date))
    print(f"[v7_research_layer] wrote {truth_path}")
    print(f"[v7_research_layer] wrote {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
