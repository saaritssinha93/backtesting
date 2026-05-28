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
    DATA_5M_DIR,
    LIVE_SIGNALS_DIR,
    NIFTY_OPEN_SLOT_DIR,
    NIFTY_SLOT_FAIL_DIR,
    NIFTY_SLOT_READY_DIR,
    RUNTIME_STATUS_DIR,
    runtime_dir,
)


RESEARCH_ROOT = runtime_dir("live_research_v7_research_layer")
TRUTH_DIR = RESEARCH_ROOT / "truth_table"
REPORT_DIR = RESEARCH_ROOT / "reports"
LATEST_DIR = RESEARCH_ROOT / "latest"
HEARTBEAT_DIR = RESEARCH_ROOT / "heartbeat"
RANKER_DIR = RESEARCH_ROOT / "ranker"
SUGGESTIONS_DIR = RESEARCH_ROOT / "suggestions"

SIGNAL_DISCOVERY_CSV_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "csv")
ENTRY_AUDIT_DIR = runtime_dir("entry_engine_1min_v5_ID", "audit")
DATA_5M_LIVE_DIR = runtime_dir("stocks_indicators_5min_eq_live")
NIFTY_5M_PARQUET = DATA_5M_LIVE_DIR / "NIFTYBEES_stocks_indicators_5min.parquet"
NIFTY_STATUS_FILE = REPO_ROOT / "logs" / "nifty_guard_fetcher_v16_5min.status"
LIVE_5MIN_STATUS_JSON = REPO_ROOT / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json"
LIVE_5MIN_SUPERVISOR_STATUS = REPO_ROOT / "logs" / "eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.status"
LIVE_5MIN_SLOT_READY_DIR = runtime_dir("slot_ready_5m")


for _p in (RESEARCH_ROOT, TRUTH_DIR, REPORT_DIR, LATEST_DIR, HEARTBEAT_DIR, RANKER_DIR, SUGGESTIONS_DIR):
    _p.mkdir(parents=True, exist_ok=True)


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


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


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
) -> str:
    if paper_traded:
        return ""
    if not passed_gate:
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
    entry_raw = _load_entry_rows(day, raw=True)
    entry_selected = _load_entry_rows(day, raw=False)
    rejects = _load_entry_rejects(day)
    live = _load_live_signals(day)
    paper = _load_paper_trades(day)

    if raw.empty:
        raw = gated.copy()
    if raw.empty:
        return pd.DataFrame()

    raw_map = _map_by_key(raw)
    gated_map = _map_by_key(gated)
    entry_raw_map = _map_by_key(entry_raw)
    entry_sel_map = _map_by_key(entry_selected)
    rejects_map = _map_by_key(rejects)
    live_map = _map_by_key(live)
    paper_map = _map_by_key(paper)
    paper_signal_map = _map_by_signal_id(paper)

    rows: list[dict[str, Any]] = []
    for key in _all_research_keys(raw, gated, entry_raw, entry_selected, rejects, live, paper):
        cand = _row_from_map(raw_map, key)
        gated_row = _row_from_map(gated_map, key)
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
        for fallback in (gated_row, entry_raw_row, entry_sel_row, live_row, paper_row):
            if base_row.empty and not fallback.empty:
                base_row = fallback

        has_entry_raw = not entry_raw_row.empty
        audit_entry_selected = not entry_sel_row.empty
        live_written = not live_row.empty
        paper_traded = not paper_row.empty
        has_entry_selected = audit_entry_selected or live_written or paper_traded
        passed_gate = not gated_row.empty or live_written or paper_traded
        gate_source = "accepted_rules_csv" if not gated_row.empty else ("live_signal_reconciled" if live_written else ("paper_trade_reconciled" if paper_traded else ""))
        selection_source = "entry_audit" if audit_entry_selected else ("live_signal_reconciled" if live_written else ("paper_trade_reconciled" if paper_traded else ""))
        reject_reason = str(reject_row.get("reject_reason", "")) if not reject_row.empty else ""

        signal_time = _first_nonblank(
            base_row,
            ["signal_time_ist", "bar_time_ist", "signal_entry_datetime_ist", "signal_datetime"],
            "",
        )
        signal_close = _safe_float(_first_nonblank(cand, ["signal_close", "signal_price", "entry_price"], np.nan))
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

        rows.append(
            {
                "date": day,
                "research_key": key,
                "candidate_id": _first_nonblank(base_row, ["candidate_id"], key),
                "ticker": _first_nonblank(base_row, ["ticker"], ""),
                "side": _first_nonblank(base_row, ["side"], ""),
                "setup": _first_nonblank(base_row, ["setup"], ""),
                "signal_time_ist": _fmt_ts(signal_time),
                "scan_slot_ist": cand.get("scan_slot_ist", "") if not cand.empty else "",
                "scan_created_at_ist": cand.get("created_at_ist", "") if not cand.empty else "",
                "signal_delay_seconds": _signal_delay_seconds(signal_time, cand.get("created_at_ist", "") if not cand.empty else ""),
                "signal_close": signal_close,
                "quality_score": _safe_float(_first_nonblank(base_row, ["quality_score", "score"], np.nan)),
                "rs_pct": _safe_float(cand.get("rs_pct")) if not cand.empty else np.nan,
                "market_ret_pct": _safe_float(cand.get("market_ret_pct")) if not cand.empty else np.nan,
                "regime": cand.get("regime", "") if not cand.empty else "",
                "vol_ratio": _safe_float(cand.get("vol_ratio")) if not cand.empty else np.nan,
                "atr_pct": _safe_float(cand.get("atr_pct")) if not cand.empty else np.nan,
                "body_pct": _safe_float(cand.get("body_pct")) if not cand.empty else np.nan,
                "close_loc": _safe_float(cand.get("close_loc")) if not cand.empty else np.nan,
                "vwap_dist_atr": _safe_float(cand.get("vwap_dist_atr")) if not cand.empty else np.nan,
                "candidate_reason": cand.get("reason", "") if not cand.empty else "",
                "passed_v8_gate": passed_gate,
                "v8_gate_source": gate_source,
                "v8_live_gate_rule": gated_row.get("v8_live_gate_rule", "") if not gated_row.empty else "",
                "v8_live_gate_stage": gated_row.get("v8_live_gate_stage", "") if not gated_row.empty else "",
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
                "reason_not_taken": _build_reason(
                    passed_gate=passed_gate,
                    entry_raw=has_entry_raw,
                    entry_selected=has_entry_selected,
                    live_written=live_written,
                    paper_traded=paper_traded,
                    reject_reason=reject_reason,
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
    return {
        "research_rows_total": int(len(truth)),
        "raw_candidates": int(raw_mask.sum()),
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


def _window_summary(window: int, days: list[str], df: pd.DataFrame) -> dict[str, Any]:
    traded = df.loc[df.get("paper_traded", False).astype(bool)].copy() if not df.empty else pd.DataFrame()
    pnl = pd.to_numeric(traded.get("paper_pnl_rs", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
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
    traded = df.loc[df["paper_traded"].astype(bool)].copy()
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
    traded = df.loc[df["paper_traded"].astype(bool)].copy()
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

    largest_days = available_days[-max(SUGGESTION_WINDOWS):]
    largest_df = _load_truth_days(largest_days)
    nifty_df = _nifty_context(largest_days, day)
    live_5min = _live_5min_context(day)
    regime_df = _regime_pf_table(largest_df)
    current_truth = _read_csv(TRUTH_DIR / f"truth_table_{day}.csv")
    ops_rows = _ops_audit_rows(day, truth=current_truth, live_5min=live_5min, nifty_df=nifty_df)
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
        "live_5min_context": live_5min,
        "nifty_context": nifty_df.to_dict("records") if not nifty_df.empty else [],
        "regime_pf": regime_df.to_dict("records") if not regime_df.empty else [],
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
    truth.to_csv(truth_path, index=False)
    truth.to_csv(ranker_path, index=False)
    report_text = write_report(day, truth)
    action_text = write_action_plan(day, truth)
    ranker_text = write_ranker_report(day, truth)
    suggestions_text, suggestions_df, suggestions_payload = build_multi_window_suggestions(day)
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
        "multi_window_suggestions": str(suggestions_path),
        "multi_window_suggestions_csv": str(suggestions_csv_path),
        "multi_window_suggestions_json": str(suggestions_json_path),
        "multi_window_suggestion_count": int(suggestions_payload.get("suggestion_count", 0)),
        "multi_window_real_change_ready_count": int(suggestions_payload.get("real_change_ready_count", 0)),
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


def run_loop(*, start_time: str, end_time: str, interval_min: int) -> int:
    today = _now_ist().date()
    start = _parse_today_time(today, start_time)
    end = _parse_today_time(today, end_time)
    last_run_key = ""
    _write_status(
        "RUNNING",
        phase="LOOP_START",
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
        _write_status("RUNNING", phase="BUILD_REPORT", run_time_ist=run_key, day=day)
        try:
            truth_path, report_path = run(day)
            summary = json.loads((LATEST_DIR / "latest_summary.json").read_text(encoding="utf-8"))
            status_summary = {k: v for k, v in summary.items() if k != "day"}
            _write_status(
                "RUNNING",
                phase="REPORT_DONE",
                run_time_ist=run_key,
                day=day,
                truth_table=str(truth_path),
                report=str(report_path),
                **status_summary,
            )
            print(f"[v7_research_layer loop] {run_key} wrote {truth_path}", flush=True)
        except Exception as exc:
            _write_status("ERROR", phase="REPORT_FAILED", run_time_ist=run_key, error=f"{type(exc).__name__}: {exc}")
            print(f"[v7_research_layer loop] ERROR {type(exc).__name__}: {exc}", flush=True)
        time.sleep(1.0)


def _default_day() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build V7 ID 5-min research-layer truth table and report")
    ap.add_argument("--date", default=_default_day(), help="Trading date YYYY-MM-DD; default today IST")
    ap.add_argument("--loop", action="store_true", help="Run repeatedly during the market session")
    ap.add_argument("--start-time", default="09:17:30", help="Loop start time HH:MM:SS IST")
    ap.add_argument("--end-time", default="16:00:00", help="Loop end time HH:MM:SS IST")
    ap.add_argument("--interval-min", type=int, default=15, help="Loop interval in minutes")
    args = ap.parse_args()
    if args.loop:
        return run_loop(start_time=str(args.start_time), end_time=str(args.end_time), interval_min=int(args.interval_min))
    truth_path, report_path = run(str(args.date))
    print(f"[v7_research_layer] wrote {truth_path}")
    print(f"[v7_research_layer] wrote {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
