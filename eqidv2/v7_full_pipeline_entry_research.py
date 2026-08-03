#!/usr/bin/env python3
"""
Read-only V7 5-minute full-pipeline entry research.

This job consumes the existing live research truth tables and 5-minute live
bar store. It does not replay scanner/filter/setup logic and does not write
anything to live trading paths.
"""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir
# House cost model (flat 16 bps + 3 bps on stops, on EFFECTIVE_NOTIONAL=50k).
# Reused so this research uses the exact same net-of-cost basis as the v11
# backtester / live conf, instead of an invented cost-free notional.
import avwap_5min_ID_v6_backtesting as v6


IST = ZoneInfo("Asia/Kolkata")
SESSION_SLUG = "v7_full_pipeline_entry_research"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
LATEST_DIR = SESSION_ROOT / "latest"
REPORT_DIR = SESSION_ROOT / "reports"
CSV_DIR = SESSION_ROOT / "csv"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"

TRUTH_DIR = runtime_dir("live_research_v7_research_layer", "truth_table")
DATA_5M_LIVE_DIR = runtime_dir("stocks_indicators_5min_eq_live")
# 1-minute indicator store: full multi-month history (the 5-min *_live store only
# holds ~8 rolling days, which silently NO_DATA'd older truth dates). Also carries
# VWAP / EMA_20/50/200 / MACD(+Hist) that the 5-min store lacks, so it is the single
# source for entry, the multi-day forward walk, and EMA/MACD/VWAP feature ranges.
DATA_1MIN_DIR = runtime_dir("stocks_indicators_1min_eq")
# Round-trip cost in bps applied via v6._net_pnl_rs (house flat_bps default).
DEFAULT_COST_BPS = float(getattr(v6, "DEFAULT_COST_BPS", 16.0))
NOTIONAL_RS = float(getattr(v6, "EFFECTIVE_NOTIONAL", 50_000.0))

PIPELINE_LEVELS: tuple[tuple[str, str], ...] = (
    ("RAW_SCANNER", "all live research truth-table candidates"),
    ("POST_SCANNER_GATE", "passed_v8_gate == true"),
    ("POST_RESEARCH_FILTER", "research_live_filter_status == PASSED"),
    ("POST_V11_OVERLAY", "v11_live_overlay_status == PASSED"),
    ("POST_PRE_MOMENTUM", "pre_momentum_gate_pass == true"),
    ("FINAL_ENTRY_LEVEL", "entry_selected/live_signal_written/paper_traded == true"),
)

FEATURE_CATEGORIES: dict[str, tuple[str, ...]] = {
    "ranker_non_indicator": (
        "quality_score",
        "ranker_score",
        "scanner_ranker_score",
        "signal_delay_seconds",
        "signal_minute",
    ),
    "market_context": (
        "rs_pct",
        "market_ret_pct",
        "market_abs_ret_pct",
        "regime",
    ),
    "indicator": (
        "atr_pct",
        "vwap_dist_atr",
        "sig5_adx_calc",
        "sig5_rsi_dir",
        "pre1_adx",
        "pre1_rsi_dir",
    ),
    # Derived at the signal bar from the 1-min indicator store (not present in the
    # truth table): EMA / MACD / VWAP ranges the task explicitly asked for.
    "indicator_1m_derived": (
        "m_ema20_dist_atr",
        "m_ema50_dist_atr",
        "m_ema200_dist_atr",
        "m_vwap_dist_atr",
        "m_macd_hist_bp",
        "m_rsi",
        "m_adx",
        "m_atr_pct",
    ),
    "candle_price_action": (
        "body_pct",
        "close_loc",
        "signal_range_pct",
        "upper_wick_pct",
        "lower_wick_pct",
        "wick_skew_pct",
        "sig5_body_r",
        "sig5_range_r",
        "sig5_close_pos",
        "pre1_body_r",
        "pre1_close_pos",
        "pre1_range_r",
    ),
    "volume_liquidity": (
        "vol_ratio",
        "signal_volume",
        "sig5_vol_ratio20",
        "pre3_vol_ratio20",
        "pre5_vol_ratio20",
        "pre10_vol_ratio20",
        "pre15_vol_ratio20",
    ),
    "pre_momentum": (
        "pre_entry_momentum_score",
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
        "pre5_close_pos",
        "pre5_dir_count",
        "pre5_body_sum_r",
        "pre5_range_r",
        "pre10_close_pos",
        "pre10_dir_count",
        "pre10_body_sum_r",
        "pre10_range_r",
        "pre15_close_pos",
        "pre15_dir_count",
        "pre15_body_sum_r",
        "pre15_range_r",
    ),
}

ALL_FEATURES: tuple[str, ...] = tuple(
    dict.fromkeys(feature for features in FEATURE_CATEGORIES.values() for feature in features)
)


for _path in (SESSION_ROOT, LATEST_DIR, REPORT_DIR, CSV_DIR, HEARTBEAT_DIR, RUNTIME_STATUS_DIR):
    _path.mkdir(parents=True, exist_ok=True)


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if not math.isfinite(float(value)):
            return None
        return float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return str(value)
    if pd.isna(value) if not isinstance(value, (str, bytes, bytearray, list, tuple, dict)) else False:
        return None
    return value


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", errors="replace")
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _write_text_atomic(path, json.dumps(_json_sanitize(payload), indent=2, sort_keys=True))


def _write_status(state: str, **extra: Any) -> None:
    now = dt.datetime.now(IST)
    payload = {
        "status": state,
        "state": state,
        "ts_ist": now.isoformat(),
        "session": SESSION_SLUG,
        **extra,
    }
    text = "\n".join(f"{k}={v}" for k, v in payload.items()) + "\n"
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status", text)
    _write_text_atomic(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat", text)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.status.json", payload)
    _write_json_atomic(HEARTBEAT_DIR / f"{SESSION_SLUG}.heartbeat.json", payload)


def _today_ist() -> dt.date:
    return dt.datetime.now(IST).date()


def _to_date(value: Any) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _to_ist_ts(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if getattr(ts, "tzinfo", None) is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _clean_ticker(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value or "").upper().strip()
    return "" if text in {"", "NAN", "NONE", "NULL"} else text


def _truth_file_date(path: Path) -> dt.date | None:
    stem = path.stem
    prefix = "truth_table_"
    if not stem.startswith(prefix):
        return None
    return _to_date(stem[len(prefix):])


def _latest_truth_files(day: dt.date, lookback_sessions: int) -> list[Path]:
    dated: list[tuple[dt.date, Path]] = []
    for path in TRUTH_DIR.glob("truth_table_*.csv"):
        file_day = _truth_file_date(path)
        if file_day is not None and file_day <= day and path.stat().st_size > 2:
            dated.append((file_day, path))
    dated.sort(key=lambda item: item[0])
    return [path for _, path in dated[-lookback_sessions:]]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size <= 2:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def _candidate_key(row: pd.Series) -> str:
    if "research_key" in row and str(row.get("research_key", "")).strip():
        return str(row.get("research_key")).strip()
    signal_ts = _to_ist_ts(row.get("signal_time_ist"))
    signal_text = "" if pd.isna(signal_ts) else signal_ts.isoformat()
    return "|".join(
        [
            _clean_ticker(row.get("ticker", "")),
            str(row.get("side", "")).upper().strip(),
            str(row.get("setup", "")).strip(),
            signal_text,
        ]
    )


def _load_truth(files: list[Path]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for path in files:
        df = _read_csv(path)
        if df.empty:
            continue
        df["source_truth_file"] = str(path)
        df["truth_date"] = str(_truth_file_date(path) or "")
        parts.append(df)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True, sort=False)
    if "signal_time_ist" not in out.columns:
        out["signal_time_ist"] = ""
    out["_signal_ts"] = out["signal_time_ist"].map(_to_ist_ts)
    out["_candidate_key"] = out.apply(_candidate_key, axis=1)
    out = out.drop_duplicates("_candidate_key", keep="last").reset_index(drop=True)
    return out


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "passed", "pass"}


def _pipeline_mask(df: pd.DataFrame, level: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    if level == "RAW_SCANNER":
        return pd.Series(True, index=df.index)
    if level == "POST_SCANNER_GATE":
        return df.get("passed_v8_gate", False).map(_boolish)
    if level == "POST_RESEARCH_FILTER":
        return df.get("research_live_filter_status", "").astype(str).str.upper().eq("PASSED")
    if level == "POST_V11_OVERLAY":
        return df.get("v11_live_overlay_status", "").astype(str).str.upper().eq("PASSED")
    if level == "POST_PRE_MOMENTUM":
        return df.get("pre_momentum_gate_pass", False).map(_boolish)
    if level == "FINAL_ENTRY_LEVEL":
        selected = df.get("entry_selected", False).map(_boolish)
        written = df.get("live_signal_written", False).map(_boolish)
        traded = df.get("paper_traded", False).map(_boolish)
        return selected | written | traded
    raise KeyError(level)


_IST_OFFSET_NS = 19_800_000_000_000  # +05:30 in ns (India, no DST)


def _ns_to_ist_iso(ns: int) -> str:
    return pd.Timestamp(int(ns) + _IST_OFFSET_NS).isoformat() + "+05:30"


def _sf(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


class BarCache:
    """1-minute OHLC + indicator cache.

    Provides numpy arrays for a vectorized multi-day forward walk, and a
    signal-bar feature snapshot (EMA/MACD/VWAP) that the truth table lacks.
    """

    _FEATURE_COLS = ("close", "EMA_20", "EMA_50", "EMA_200", "VWAP", "ATR", "MACD_Hist", "RSI", "ADX")

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._cache: dict[str, dict[str, Any] | None] = {}
        self.missing: set[str] = set()

    def _load(self, key: str) -> dict[str, Any] | None:
        path = self.data_dir / f"{key}_stocks_indicators_1min.parquet"
        if not path.exists():
            self.missing.add(key)
            return None
        try:
            df = pd.read_parquet(path)
        except Exception:
            self.missing.add(key)
            return None
        if "date" not in df.columns:
            self.missing.add(key)
            return None
        df = df.copy()
        # Keep tz-aware UTC only (this pandas build's ZoneInfo DST path is broken);
        # all IST work is done from int64 ns via a fixed +5:30 offset.
        df["_bt"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
        df = df.dropna(subset=["_bt", "open", "high", "low"]).sort_values("_bt").reset_index(drop=True)
        if df.empty:
            self.missing.add(key)
            return None
        feat = {c: (df[c].to_numpy(dtype=float, na_value=np.nan) if c in df.columns else None) for c in self._FEATURE_COLS}
        ts = df["_bt"].to_numpy(dtype="datetime64[ns]").view("int64")  # ns since epoch (UTC)
        # IST day bucket via fixed +5:30 offset (India has no DST) — avoids the
        # tz .dt.date accessor which is broken for ZoneInfo on this pandas build.
        day = ((ts + 19_800_000_000_000) // 86_400_000_000_000).astype("int64")
        return {
            "ts": ts,
            "open": df["open"].to_numpy(dtype=float, na_value=np.nan),
            "high": df["high"].to_numpy(dtype=float, na_value=np.nan),
            "low": df["low"].to_numpy(dtype=float, na_value=np.nan),
            "day": day,
            "feat": feat,
        }

    def get(self, ticker: str) -> dict[str, Any] | None:
        key = str(ticker or "").upper().strip()
        if not key:
            return None
        if key not in self._cache:
            self._cache[key] = self._load(key)
        return self._cache[key]

    def features(self, ticker: str, signal_ts: pd.Timestamp) -> dict[str, Any]:
        obj = self.get(ticker)
        if obj is None or pd.isna(signal_ts):
            return {}
        sig_ns = pd.Timestamp(signal_ts).value  # ns UTC
        idx = int(np.searchsorted(obj["ts"], sig_ns, side="right")) - 1
        if idx < 0:
            return {}
        feat = obj["feat"]

        def _v(col: str) -> float:
            arr = feat.get(col)
            return _sf(arr[idx]) if arr is not None else np.nan

        close = _v("close")
        atr = _v("ATR")
        atr = atr if (math.isfinite(atr) and atr > 0) else np.nan
        macd_hist = _v("MACD_Hist")
        return {
            "m_ema20_dist_atr": (close - _v("EMA_20")) / atr if math.isfinite(atr) else np.nan,
            "m_ema50_dist_atr": (close - _v("EMA_50")) / atr if math.isfinite(atr) else np.nan,
            "m_ema200_dist_atr": (close - _v("EMA_200")) / atr if math.isfinite(atr) else np.nan,
            "m_vwap_dist_atr": (close - _v("VWAP")) / atr if math.isfinite(atr) else np.nan,
            "m_macd_hist_bp": (macd_hist / close * 1e4) if (math.isfinite(macd_hist) and close) else np.nan,
            "m_rsi": _v("RSI"),
            "m_adx": _v("ADX"),
            "m_atr_pct": (atr / close * 100.0) if (math.isfinite(atr) and close) else np.nan,
        }


def _no_data(reason: str) -> dict[str, Any]:
    return {
        "outcome": "NO_DATA",
        "entry_time_ist": "",
        "entry_price": np.nan,
        "target_price": np.nan,
        "sl_price": np.nan,
        "exit_time_ist": "",
        "exit_price": np.nan,
        "bars_to_exit": np.nan,
        "minutes_to_exit": np.nan,
        "hold_days": np.nan,
        "mfe_pct": np.nan,
        "mae_pct": np.nan,
        "gross_pnl_rs": 0.0,
        "cost_rs": 0.0,
        "pnl_rs": 0.0,
        "failure_pattern": reason,
    }


def _simulate_path(
    obj: dict[str, Any] | None,
    signal_ts: pd.Timestamp,
    direction: str,
    target_pct: float,
    stop_pct: float,
    cost_bps: float,
) -> dict[str, Any]:
    """Enter at the first 1-min bar open after ``signal_ts`` (== the next 5-min
    bar open) and walk 1-min bars forward ACROSS DAYS until target or stop.

    No EOD exit: a position that never resolves within the available history is
    ``NO_HIT`` (open / unrealized, zero modeled P&L). Same-bar target+stop -> SL.
    P&L is net of the house v6 flat cost (16 bps + 3 bps on stops, 50k notional).
    """
    if obj is None or pd.isna(signal_ts):
        return _no_data("missing_bars_or_signal_time")

    ts = obj["ts"]
    sig_ns = pd.Timestamp(signal_ts).value
    start = int(np.searchsorted(ts, sig_ns, side="right"))
    if start >= len(ts):
        return _no_data("no_future_1min_bars")

    entry_price = float(obj["open"][start])
    if not math.isfinite(entry_price) or entry_price <= 0:
        return _no_data("bad_entry_open")

    highs = obj["high"][start:]
    lows = obj["low"][start:]
    is_long = direction == "LONG"
    if is_long:
        target_price = entry_price * (1.0 + target_pct / 100.0)
        sl_price = entry_price * (1.0 - stop_pct / 100.0)
        hit_t = highs >= target_price
        hit_s = lows <= sl_price
    else:
        target_price = entry_price * (1.0 - target_pct / 100.0)
        sl_price = entry_price * (1.0 + stop_pct / 100.0)
        hit_t = lows <= target_price
        hit_s = highs >= sl_price

    have_t = bool(hit_t.any())
    have_s = bool(hit_s.any())
    ti = int(np.argmax(hit_t)) if have_t else -1
    si = int(np.argmax(hit_s)) if have_s else -1

    if have_t and have_s:
        # Same-bar or earlier stop -> SL first (conservative).
        if si <= ti:
            outcome, exit_i, exit_price = "SL", si, sl_price
        else:
            outcome, exit_i, exit_price = "TARGET", ti, target_price
    elif have_t:
        outcome, exit_i, exit_price = "TARGET", ti, target_price
    elif have_s:
        outcome, exit_i, exit_price = "SL", si, sl_price
    else:
        outcome, exit_i, exit_price = "NO_HIT", len(highs) - 1, np.nan

    seg_hi = highs[: exit_i + 1]
    seg_lo = lows[: exit_i + 1]
    if is_long:
        mfe_pct = ((np.nanmax(seg_hi) / entry_price) - 1.0) * 100.0 if seg_hi.size else np.nan
        mae_pct = (1.0 - (np.nanmin(seg_lo) / entry_price)) * 100.0 if seg_lo.size else np.nan
    else:
        mfe_pct = (1.0 - (np.nanmin(seg_lo) / entry_price)) * 100.0 if seg_lo.size else np.nan
        mae_pct = ((np.nanmax(seg_hi) / entry_price) - 1.0) * 100.0 if seg_hi.size else np.nan

    bars_to_exit = exit_i + 1
    exit_time = _ns_to_ist_iso(ts[start + exit_i])
    hold_days = int(len(set(obj["day"][start : start + exit_i + 1].tolist())))

    if outcome == "TARGET":
        price_pnl_pct = target_pct
        failure = "target_before_sl"
    elif outcome == "SL":
        price_pnl_pct = -stop_pct
        if bars_to_exit <= 5:
            failure = "early_stop_1_to_5_min"
        elif math.isfinite(mfe_pct) and mfe_pct >= target_pct * 0.5:
            failure = "failed_after_partial_followthrough"
        else:
            failure = "stop_without_followthrough"
    else:
        price_pnl_pct = 0.0
        if math.isfinite(mfe_pct) and mfe_pct >= target_pct * 0.8:
            failure = "near_target_no_fill"
        elif math.isfinite(mae_pct) and mae_pct >= stop_pct * 0.8:
            failure = "near_stop_survived_no_exit"
        else:
            failure = "stalled_no_hit"

    if outcome in ("TARGET", "SL"):
        net, gross, cost = v6._net_pnl_rs(price_pnl_pct, outcome, cost_bps)
    else:
        # Open position at end of available data: no round trip realized.
        net = gross = cost = 0.0

    return {
        "outcome": outcome,
        "entry_time_ist": _ns_to_ist_iso(ts[start]),
        "entry_price": entry_price,
        "target_price": target_price,
        "sl_price": sl_price,
        "exit_time_ist": exit_time if outcome != "NO_HIT" else "",
        "exit_price": exit_price,
        "bars_to_exit": bars_to_exit,
        "minutes_to_exit": bars_to_exit,  # 1-min bars -> in-market minutes
        "hold_days": hold_days,
        "mfe_pct": float(mfe_pct) if mfe_pct == mfe_pct else np.nan,
        "mae_pct": float(mae_pct) if mae_pct == mae_pct else np.nan,
        "gross_pnl_rs": float(gross),
        "cost_rs": float(cost),
        "pnl_rs": float(net),
        "failure_pattern": failure,
    }


def _simulate_candidates(
    truth: pd.DataFrame,
    target_pct: float,
    stop_pct: float,
    cost_bps: float,
) -> tuple[pd.DataFrame, list[str]]:
    cache = BarCache(DATA_1MIN_DIR)
    rows: list[dict[str, Any]] = []
    feature_cols = [c for c in ALL_FEATURES if c in truth.columns]
    feat_memo: dict[tuple[str, Any], dict[str, Any]] = {}

    for level, _desc in PIPELINE_LEVELS:
        selected = truth[_pipeline_mask(truth, level)]
        for _, row in selected.iterrows():
            ticker = _clean_ticker(row.get("ticker", ""))
            obj = cache.get(ticker)
            signal_ts = row.get("_signal_ts", pd.NaT)
            memo_key = (ticker, None if pd.isna(signal_ts) else pd.Timestamp(signal_ts).value)
            if memo_key not in feat_memo:
                feat_memo[memo_key] = cache.features(ticker, signal_ts)
            mfeat = feat_memo[memo_key]
            for direction in ("LONG", "SHORT"):
                sim = _simulate_path(obj, signal_ts, direction, target_pct, stop_pct, cost_bps)
                out = {
                    "truth_date": row.get("truth_date", ""),
                    "pipeline_level": level,
                    "ticker": ticker,
                    "source_side": str(row.get("side", "")).upper().strip(),
                    "direction": direction,
                    "setup": row.get("setup", ""),
                    "signal_time_ist": "" if pd.isna(signal_ts) else pd.Timestamp(signal_ts).isoformat(),
                    "scan_slot_ist": row.get("scan_slot_ist", ""),
                    "candidate_id": row.get("candidate_id", ""),
                    "research_key": row.get("_candidate_key", ""),
                    "passed_v8_gate": row.get("passed_v8_gate", ""),
                    "research_live_filter_status": row.get("research_live_filter_status", ""),
                    "research_live_filter_reason": row.get("research_live_filter_reason", ""),
                    "v11_live_overlay_status": row.get("v11_live_overlay_status", ""),
                    "v11_selected_strategy_reject_reason": row.get("v11_selected_strategy_reject_reason", ""),
                    "entry_row_built": row.get("entry_row_built", ""),
                    "entry_selected": row.get("entry_selected", ""),
                    "live_signal_written": row.get("live_signal_written", ""),
                    "paper_traded": row.get("paper_traded", ""),
                    "reason_not_taken": row.get("reason_not_taken", ""),
                    "entry_reject_reason": row.get("entry_reject_reason", ""),
                }
                for col in feature_cols:
                    out[col] = row.get(col, np.nan)
                out.update(mfeat)
                out.update(sim)
                rows.append(out)

    return pd.DataFrame(rows), sorted(cache.missing)


def _profit_factor(gains: float, losses_abs: float) -> float:
    if losses_abs > 0:
        return gains / losses_abs
    if gains > 0:
        return float("inf")
    return 0.0


def _summarize(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=group_cols + [
            "trades", "target_hits", "sl_hits", "no_hits", "no_data", "win_rate_all",
            "resolved_win_rate", "avg_win_rs", "avg_loss_rs", "gross_win_rs",
            "gross_loss_rs", "pnl_rs", "profit_factor",
        ])

    working = df.copy()
    working["_target"] = working["outcome"].eq("TARGET").astype(int)
    working["_sl"] = working["outcome"].eq("SL").astype(int)
    working["_no_hit"] = working["outcome"].eq("NO_HIT").astype(int)
    working["_no_data"] = working["outcome"].eq("NO_DATA").astype(int)
    working["_win_rs"] = working["pnl_rs"].where(working["pnl_rs"] > 0, 0.0)
    working["_loss_rs"] = working["pnl_rs"].where(working["pnl_rs"] < 0, 0.0)

    grouped = working.groupby(group_cols, dropna=False) if group_cols else [((), working)]
    rows: list[dict[str, Any]] = []
    for key, part in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        row = {col: val for col, val in zip(group_cols, key)}
        trades = int(len(part))
        target_hits = int(part["_target"].sum())
        sl_hits = int(part["_sl"].sum())
        no_hits = int(part["_no_hit"].sum())
        no_data = int(part["_no_data"].sum())
        gains = float(part["_win_rs"].sum())
        losses = float(-part["_loss_rs"].sum())
        resolved = target_hits + sl_hits
        win_values = part.loc[part["pnl_rs"] > 0, "pnl_rs"]
        loss_values = part.loc[part["pnl_rs"] < 0, "pnl_rs"]
        row.update({
            "trades": trades,
            "target_hits": target_hits,
            "sl_hits": sl_hits,
            "no_hits": no_hits,
            "no_data": no_data,
            "win_rate_all": target_hits / trades if trades else 0.0,
            "resolved_win_rate": target_hits / resolved if resolved else 0.0,
            "avg_win_rs": float(win_values.mean()) if len(win_values) else 0.0,
            "avg_loss_rs": float(loss_values.mean()) if len(loss_values) else 0.0,
            "gross_win_rs": gains,
            "gross_loss_rs": -losses,
            "pnl_rs": float(part["pnl_rs"].sum()),
            "profit_factor": _profit_factor(gains, losses),
        })
        rows.append(row)
    out = pd.DataFrame(rows)
    if "pnl_rs" in out.columns:
        out = out.sort_values(["pnl_rs", "profit_factor"], ascending=[False, False], na_position="last")
    return out.reset_index(drop=True)


def _category_for_feature(feature: str) -> str:
    for category, features in FEATURE_CATEGORIES.items():
        if feature in features:
            return category
    return "other"


def _range_mask(series: pd.Series, left: float, right: float) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.gt(left) & values.le(right)


def _metric_row(part: pd.DataFrame) -> dict[str, Any]:
    if part.empty:
        return {
            "trades": 0,
            "target_hits": 0,
            "sl_hits": 0,
            "no_hits": 0,
            "pnl_rs": 0.0,
            "win_rate_all": 0.0,
            "resolved_win_rate": 0.0,
            "profit_factor": 0.0,
            "days": 0,
        }
    summ = _summarize(part, []).iloc[0].to_dict()
    summ["days"] = int(pd.Series(part["truth_date"]).nunique())
    return summ


def _mine_feature_ranges(
    sim: pd.DataFrame,
    *,
    min_rows: int,
    min_days: int,
    qbins: int = 8,
) -> pd.DataFrame:
    base = sim[(sim["pipeline_level"] == "RAW_SCANNER") & sim["outcome"].ne("NO_DATA")].copy()
    rows: list[dict[str, Any]] = []
    for direction in ("LONG", "SHORT"):
        ddf = base[base["direction"].eq(direction)]
        for feature in ALL_FEATURES:
            if feature not in ddf.columns:
                continue
            values = pd.to_numeric(ddf[feature], errors="coerce")
            valid = ddf[values.notna()].copy()
            if len(valid) < max(min_rows, qbins * 3) or values.nunique(dropna=True) < 4:
                continue
            try:
                bins = pd.qcut(pd.to_numeric(valid[feature], errors="coerce"), q=qbins, duplicates="drop")
            except ValueError:
                continue
            valid["_bin"] = bins
            for interval, part in valid.groupby("_bin", observed=True):
                metric = _metric_row(part)
                if metric["trades"] < min_rows or metric["days"] < min_days:
                    continue
                left = float(interval.left)
                right = float(interval.right)
                rows.append({
                    "direction": direction,
                    "category": _category_for_feature(feature),
                    "feature": feature,
                    "range": f"({left:.6g}, {right:.6g}]",
                    "left": left,
                    "right": right,
                    **metric,
                })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["direction", "pnl_rs", "profit_factor"], ascending=[True, False, False]).reset_index(drop=True)


def _apply_condition(df: pd.DataFrame, cond: dict[str, Any]) -> pd.Series:
    if "conditions" in cond:
        mask = pd.Series(True, index=df.index)
        for child in cond["conditions"]:
            mask &= _apply_condition(df, child)
        return mask
    feature = cond["feature"]
    if feature not in df.columns:
        return pd.Series(False, index=df.index)
    return _range_mask(df[feature], float(cond["left"]), float(cond["right"]))


def _condition_text(cond: dict[str, Any]) -> str:
    if "conditions" in cond:
        return " AND ".join(_condition_text(child) for child in cond["conditions"])
    return f"{cond['feature']} {cond['range']}"


def _mine_patterns(
    sim: pd.DataFrame,
    ranges: pd.DataFrame,
    *,
    min_val_rows: int,
    min_val_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = sim[(sim["pipeline_level"] == "RAW_SCANNER") & sim["outcome"].ne("NO_DATA")].copy()
    dates = sorted(str(x) for x in base["truth_date"].dropna().unique() if str(x))
    if len(dates) < 4 or ranges.empty:
        return pd.DataFrame(), pd.DataFrame()
    split = max(1, int(len(dates) * 0.6))
    train_dates = set(dates[:split])
    val_dates = set(dates[split:])

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for direction in ("LONG", "SHORT"):
        ddf = base[base["direction"].eq(direction)].copy()
        train = ddf[ddf["truth_date"].astype(str).isin(train_dates)]
        val = ddf[ddf["truth_date"].astype(str).isin(val_dates)]
        candidate_ranges = ranges[
            (ranges["direction"].eq(direction))
            & (ranges["pnl_rs"] > 0)
            & (ranges["profit_factor"] >= 1.2)
        ].head(30)
        conds = [
            {
                "feature": r["feature"],
                "range": r["range"],
                "left": float(r["left"]),
                "right": float(r["right"]),
                "category": r["category"],
            }
            for _, r in candidate_ranges.iterrows()
        ]
        tests: list[dict[str, Any]] = [{"conditions": [cond], "kind": "single"} for cond in conds]
        for a, b in itertools.combinations(conds[:20], 2):
            if a["feature"] == b["feature"]:
                continue
            tests.append({"conditions": [a, b], "kind": "pair"})

        seen: set[str] = set()
        for test in tests:
            text = _condition_text(test)
            if text in seen:
                continue
            seen.add(text)
            train_part = train[_apply_condition(train, test)]
            val_part = val[_apply_condition(val, test)]
            train_metric = _metric_row(train_part)
            val_metric = _metric_row(val_part)
            record = {
                "direction": direction,
                "kind": test["kind"],
                "condition": text,
                "train_dates": ",".join(sorted(train_dates)),
                "validation_dates": ",".join(sorted(val_dates)),
                "train_trades": train_metric["trades"],
                "train_days": train_metric["days"],
                "train_pnl_rs": train_metric["pnl_rs"],
                "train_win_rate_all": train_metric["win_rate_all"],
                "train_profit_factor": train_metric["profit_factor"],
                "val_trades": val_metric["trades"],
                "val_days": val_metric["days"],
                "val_pnl_rs": val_metric["pnl_rs"],
                "val_win_rate_all": val_metric["win_rate_all"],
                "val_profit_factor": val_metric["profit_factor"],
            }
            good = (
                train_metric["pnl_rs"] > 0
                and train_metric["profit_factor"] >= 1.2
                and val_metric["trades"] >= min_val_rows
                and val_metric["days"] >= min_val_days
                and val_metric["pnl_rs"] > 0
                and val_metric["profit_factor"] >= 1.2
            )
            if good:
                score = val_metric["pnl_rs"] * min(float(val_metric["profit_factor"]), 3.0) * math.log1p(val_metric["trades"])
                record["honesty_status"] = "PASS_VALIDATION_RESEARCH_ONLY"
                record["score"] = score
                accepted.append(record)
            else:
                if train_metric["pnl_rs"] > 0 and train_metric["trades"] >= min_val_rows:
                    reason = []
                    if val_metric["trades"] < min_val_rows:
                        reason.append("validation_support_too_low")
                    if val_metric["days"] < min_val_days:
                        reason.append("validation_days_too_low")
                    if val_metric["pnl_rs"] <= 0:
                        reason.append("validation_pnl_failed")
                    if val_metric["profit_factor"] < 1.2:
                        reason.append("validation_pf_failed")
                    record["honesty_status"] = "REJECT"
                    record["reject_reason"] = ";".join(reason) or "failed_validation"
                    rejected.append(record)

    accepted_df = pd.DataFrame(accepted)
    rejected_df = pd.DataFrame(rejected)
    if not accepted_df.empty:
        accepted_df = accepted_df.sort_values(["score", "val_pnl_rs"], ascending=[False, False]).reset_index(drop=True)
    if not rejected_df.empty:
        rejected_df = rejected_df.sort_values(["train_pnl_rs", "val_pnl_rs"], ascending=[False, True]).reset_index(drop=True)
    return accepted_df, rejected_df


def _rejection_summary(truth: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "reason_not_taken",
        "entry_reject_reason",
        "research_live_filter_reason",
        "v11_selected_strategy_reject_reason",
    ]
    rows: list[dict[str, Any]] = []
    for col in cols:
        if col not in truth.columns:
            continue
        vc = truth[col].fillna("").astype(str).str.strip()
        vc = vc[vc.ne("")]
        for reason, count in vc.value_counts().head(30).items():
            rows.append({"source_column": col, "reason": reason, "count": int(count)})
    return pd.DataFrame(rows)


def _examples(sim: pd.DataFrame, n: int = 24) -> pd.DataFrame:
    preferred = sim[
        (sim["pipeline_level"].eq("RAW_SCANNER"))
        & (sim["outcome"].isin(["TARGET", "SL"]))
    ].copy()
    if preferred.empty:
        return pd.DataFrame()
    winners = preferred[preferred["outcome"].eq("TARGET")].sort_values(["direction", "minutes_to_exit"], na_position="last").head(n // 2)
    losers = preferred[preferred["outcome"].eq("SL")].sort_values(["direction", "minutes_to_exit"], na_position="last").head(n // 2)
    cols = [
        "truth_date", "ticker", "setup", "direction", "signal_time_ist", "entry_time_ist",
        "entry_price", "target_price", "sl_price", "outcome", "pnl_rs", "minutes_to_exit",
        "mfe_pct", "mae_pct", "failure_pattern",
    ]
    return pd.concat([winners, losers], ignore_index=True, sort=False)[cols]


def _fmt_num(value: Any, digits: int = 2) -> str:
    try:
        f = float(value)
    except Exception:
        return str(value)
    if math.isinf(f):
        return "inf"
    if math.isnan(f):
        return ""
    return f"{f:,.{digits}f}"


def _md_table(df: pd.DataFrame, columns: list[str], max_rows: int = 20) -> list[str]:
    if df.empty:
        return ["_No rows._"]
    rows = df.head(max_rows).copy()
    out = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in rows.iterrows():
        vals = []
        for col in columns:
            val = row.get(col, "")
            if isinstance(val, float):
                if col.endswith("rate") or col.endswith("rate_all") or "win_rate" in col:
                    vals.append(_fmt_num(val * 100.0, 2) + "%")
                elif "factor" in col:
                    vals.append(_fmt_num(val, 2))
                else:
                    vals.append(_fmt_num(val, 2))
            else:
                vals.append(str(val).replace("|", "/"))
        out.append("| " + " | ".join(vals) + " |")
    return out


def _build_report(
    *,
    day: dt.date,
    files: list[Path],
    truth: pd.DataFrame,
    sim: pd.DataFrame,
    missing_bars: list[str],
    pipeline_summary: pd.DataFrame,
    direction_summary: pd.DataFrame,
    setup_summary: pd.DataFrame,
    ranges: pd.DataFrame,
    patterns: pd.DataFrame,
    rejected: pd.DataFrame,
    examples: pd.DataFrame,
    target_pct: float,
    stop_pct: float,
    notional: float,
    cost_bps: float,
    elapsed_sec: float,
) -> str:
    lines: list[str] = []
    lines.append(f"# V7 5-min Full-Pipeline Entry Research - {day}")
    lines.append("")
    lines.append("Read-only EOD analyzer. No live scanner/filter/setup/backtesting logic is replayed or modified.")
    lines.append("")
    lines.append("## Exact Analysis Parameters")
    lines.extend([
        f"- Sessions used: {', '.join(str(_truth_file_date(p)) for p in files)}",
        f"- Truth source: `{TRUTH_DIR}`",
        f"- Bar source: `{DATA_1MIN_DIR}` (1-min OHLC + indicators, full history)",
        "- Entry model: open of the first 1-min bar after `signal_time_ist` "
        "(== next 5-min bar open); house `next_1min_open_after_5min_signal`",
        f"- LONG target/SL: `+{target_pct:.2f}%` / `-{stop_pct:.2f}%`",
        f"- SHORT target/SL: `-{target_pct:.2f}%` / `+{stop_pct:.2f}%`",
        "- Forward walk: 1-min bars, **across days, NO EOD exit**; a path that never "
        "resolves in available history is `NO_HIT` (open/unrealized, zero P&L)",
        "- Same-bar target+SL conflict: SL first",
        f"- Cost basis: **net** via v6 flat `{cost_bps:.0f}`bps (+{getattr(v6, 'STOP_EXTRA_BPS', 3.0):.0f}bps on stops), "
        f"notional Rs `{notional:,.0f}` (= v6 EFFECTIVE_NOTIONAL); `pnl_rs` is NET, `gross_pnl_rs`/`cost_rs` also reported",
        f"- Runtime: `{elapsed_sec:.1f}s`",
    ])
    lines.append("")
    lines.append("## Data Health")
    lines.extend([
        f"- Truth-table candidate rows after dedupe: `{len(truth)}`",
        f"- Simulated candidate-level-direction rows: `{len(sim)}`",
        f"- Missing/failed ticker bar files: `{len(missing_bars)}`",
    ])
    if missing_bars:
        lines.append(f"- Missing sample: `{', '.join(missing_bars[:25])}`")
    lines.append("")
    lines.append("## Pipeline by Direction")
    lines.extend(_md_table(
        pipeline_summary,
        ["pipeline_level", "direction", "trades", "target_hits", "sl_hits", "no_hits", "no_data", "win_rate_all", "pnl_rs", "profit_factor"],
        30,
    ))
    lines.append("")
    lines.append("## Direction Totals")
    lines.extend(_md_table(
        direction_summary,
        ["direction", "trades", "target_hits", "sl_hits", "no_hits", "no_data", "win_rate_all", "resolved_win_rate", "pnl_rs", "profit_factor"],
        10,
    ))
    lines.append("")
    lines.append("## Setup Context")
    lines.extend(_md_table(
        setup_summary,
        ["setup", "direction", "trades", "target_hits", "sl_hits", "win_rate_all", "pnl_rs", "profit_factor"],
        20,
    ))
    lines.append("")
    lines.append("## Best Indicator and Non-Indicator Ranges")
    best_ranges = ranges[ranges["pnl_rs"] > 0].sort_values(["direction", "pnl_rs"], ascending=[True, False])
    lines.extend(_md_table(
        best_ranges,
        ["direction", "category", "feature", "range", "trades", "days", "target_hits", "sl_hits", "win_rate_all", "pnl_rs", "profit_factor"],
        30,
    ))
    lines.append("")
    lines.append("## Honest Pattern Candidates")
    lines.extend(_md_table(
        patterns,
        ["direction", "kind", "condition", "train_trades", "train_pnl_rs", "train_profit_factor", "val_trades", "val_days", "val_pnl_rs", "val_profit_factor", "honesty_status"],
        20,
    ))
    lines.append("")
    lines.append("## Rejected Patterns")
    lines.extend(_md_table(
        rejected,
        ["direction", "kind", "condition", "train_trades", "train_pnl_rs", "val_trades", "val_pnl_rs", "val_profit_factor", "reject_reason"],
        20,
    ))
    lines.append("")
    lines.append("## Entry Examples")
    lines.extend(_md_table(
        examples,
        ["truth_date", "ticker", "setup", "direction", "signal_time_ist", "entry_price", "target_price", "sl_price", "outcome", "pnl_rs", "minutes_to_exit", "failure_pattern"],
        24,
    ))
    lines.append("")
    lines.append("## Proposal Only")
    lines.append("- Keep this research-only until a candidate survives several more EOD windows.")
    lines.append("- LONG edge should be tested first: mid-morning, moderate ranker strength, positive RS, healthy ATR, above-VWAP but not exhausted.")
    lines.append("- SHORT edge is narrower and should stay shadow-only unless future validation improves.")
    lines.append("- Do not promote any condition directly from this report without a separate walk-forward and live-parity check.")
    lines.append("")
    lines.append("## Honest Conclusion")
    if not patterns.empty:
        long_ok = patterns[patterns["direction"].eq("LONG")]
        short_ok = patterns[patterns["direction"].eq("SHORT")]
        if not long_ok.empty and short_ok.empty:
            conclusion = "LONG has research-grade candidates; SHORT is not live-ready yet."
        elif not long_ok.empty and not short_ok.empty:
            conclusion = "Both sides have candidates, but only the highest-support patterns should advance to shadow."
        else:
            conclusion = "SHORT has narrow research-grade candidates; validate harder before considering live use."
    else:
        conclusion = "No honest pattern survived validation today. Keep collecting data."
    lines.append(conclusion)
    lines.append("")
    return "\n".join(lines)


def run(day: dt.date, lookback_sessions: int, target_pct: float, stop_pct: float, cost_bps: float) -> dict[str, Any]:
    started = time.perf_counter()
    _write_status("RUNNING", phase="START", day=str(day), lookback_sessions=lookback_sessions)
    files = _latest_truth_files(day, lookback_sessions)
    truth = _load_truth(files)
    if truth.empty:
        md = (
            f"# V7 5-min Full-Pipeline Entry Research - {day}\n\n"
            "No truth-table input files were available for the requested window.\n"
        )
        latest_md = LATEST_DIR / "latest_v7_full_pipeline_entry_research.md"
        latest_json = LATEST_DIR / "latest_v7_full_pipeline_entry_research.json"
        _write_text_atomic(latest_md, md)
        _write_json_atomic(latest_json, {"status": "NO_INPUT", "day": str(day), "files": [str(p) for p in files]})
        _write_status("DONE", phase="NO_INPUT", day=str(day), report=str(latest_md))
        return {"status": "NO_INPUT", "report": str(latest_md)}

    sim, missing_bars = _simulate_candidates(truth, target_pct, stop_pct, cost_bps)
    pipeline_summary = _summarize(sim, ["pipeline_level", "direction"])
    direction_summary = _summarize(sim, ["direction"])
    setup_summary = _summarize(sim[sim["pipeline_level"].eq("RAW_SCANNER")], ["setup", "direction"])
    ranges = _mine_feature_ranges(sim, min_rows=25, min_days=3)
    patterns, rejected = _mine_patterns(sim, ranges, min_val_rows=10, min_val_days=2)
    examples = _examples(sim)
    rejection_summary = _rejection_summary(truth)

    run_key = day.isoformat()
    latest_md = LATEST_DIR / "latest_v7_full_pipeline_entry_research.md"
    latest_json = LATEST_DIR / "latest_v7_full_pipeline_entry_research.json"
    dated_md = REPORT_DIR / f"v7_full_pipeline_entry_research_{run_key}.md"
    dated_json = REPORT_DIR / f"v7_full_pipeline_entry_research_{run_key}.json"

    outputs = {
        "simulated_candidates_csv": CSV_DIR / f"simulated_candidates_{run_key}.csv",
        "pipeline_summary_csv": CSV_DIR / f"pipeline_summary_{run_key}.csv",
        "direction_summary_csv": CSV_DIR / f"direction_summary_{run_key}.csv",
        "setup_summary_csv": CSV_DIR / f"setup_summary_{run_key}.csv",
        "feature_ranges_csv": CSV_DIR / f"feature_ranges_{run_key}.csv",
        "pattern_candidates_csv": CSV_DIR / f"pattern_candidates_{run_key}.csv",
        "rejected_patterns_csv": CSV_DIR / f"rejected_patterns_{run_key}.csv",
        "entry_examples_csv": CSV_DIR / f"entry_examples_{run_key}.csv",
        "rejection_summary_csv": CSV_DIR / f"rejection_summary_{run_key}.csv",
    }
    latest_outputs = {
        "simulated_candidates_csv": LATEST_DIR / "latest_simulated_candidates.csv",
        "pipeline_summary_csv": LATEST_DIR / "latest_pipeline_summary.csv",
        "direction_summary_csv": LATEST_DIR / "latest_direction_summary.csv",
        "setup_summary_csv": LATEST_DIR / "latest_setup_summary.csv",
        "feature_ranges_csv": LATEST_DIR / "latest_feature_ranges.csv",
        "pattern_candidates_csv": LATEST_DIR / "latest_pattern_candidates.csv",
        "rejected_patterns_csv": LATEST_DIR / "latest_rejected_patterns.csv",
        "entry_examples_csv": LATEST_DIR / "latest_entry_examples.csv",
        "rejection_summary_csv": LATEST_DIR / "latest_rejection_summary.csv",
    }
    frames = {
        "simulated_candidates_csv": sim,
        "pipeline_summary_csv": pipeline_summary,
        "direction_summary_csv": direction_summary,
        "setup_summary_csv": setup_summary,
        "feature_ranges_csv": ranges,
        "pattern_candidates_csv": patterns,
        "rejected_patterns_csv": rejected,
        "entry_examples_csv": examples,
        "rejection_summary_csv": rejection_summary,
    }
    for key, frame in frames.items():
        clean = frame.replace([np.inf, -np.inf], np.nan)
        clean.to_csv(outputs[key], index=False)
        clean.to_csv(latest_outputs[key], index=False)

    elapsed = time.perf_counter() - started
    md = _build_report(
        day=day,
        files=files,
        truth=truth,
        sim=sim,
        missing_bars=missing_bars,
        pipeline_summary=pipeline_summary,
        direction_summary=direction_summary,
        setup_summary=setup_summary,
        ranges=ranges,
        patterns=patterns,
        rejected=rejected,
        examples=examples,
        target_pct=target_pct,
        stop_pct=stop_pct,
        notional=NOTIONAL_RS,
        cost_bps=cost_bps,
        elapsed_sec=elapsed,
    )
    _write_text_atomic(latest_md, md)
    _write_text_atomic(dated_md, md)

    summary = {
        "status": "DONE",
        "day": str(day),
        "lookback_sessions": lookback_sessions,
        "truth_files": [str(p) for p in files],
        "truth_rows": int(len(truth)),
        "simulated_rows": int(len(sim)),
        "missing_bar_tickers": missing_bars,
        "target_pct": target_pct,
        "stop_pct": stop_pct,
        "cost_bps": cost_bps,
        "notional": NOTIONAL_RS,
        "latest_report": str(latest_md),
        "dated_report": str(dated_md),
        "latest_outputs": {k: str(v) for k, v in latest_outputs.items()},
        "dated_outputs": {k: str(v) for k, v in outputs.items()},
        "pipeline_summary": pipeline_summary.head(50).replace([np.inf, -np.inf], np.nan).to_dict(orient="records"),
        "direction_summary": direction_summary.replace([np.inf, -np.inf], np.nan).to_dict(orient="records"),
        "top_patterns": patterns.head(20).replace([np.inf, -np.inf], np.nan).to_dict(orient="records"),
        "elapsed_sec": elapsed,
    }
    _write_json_atomic(latest_json, summary)
    _write_json_atomic(dated_json, summary)
    _write_status(
        "DONE",
        phase="COMPLETE",
        day=str(day),
        report=str(latest_md),
        truth_rows=len(truth),
        simulated_rows=len(sim),
        patterns=len(patterns),
        elapsed_sec=round(elapsed, 2),
    )
    return summary


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=str(_today_ist()), help="EOD date in YYYY-MM-DD, IST")
    ap.add_argument("--lookback-sessions", type=int, default=7, help="Number of truth-table sessions to analyze")
    ap.add_argument("--target-pct", type=float, default=1.10, help="Target percentage")
    ap.add_argument("--stop-pct", type=float, default=0.85, help="Stop-loss percentage")
    ap.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS,
                    help="Round-trip cost in bps (house v6 flat_bps; +3bps auto on stops)")
    args = ap.parse_args(list(argv) if argv is not None else None)

    day = _to_date(args.date)
    if day is None:
        raise SystemExit(f"Bad --date: {args.date!r}")
    try:
        summary = run(day, args.lookback_sessions, args.target_pct, args.stop_pct, args.cost_bps)
        print(f"[{SESSION_SLUG}] {summary.get('status')} report={summary.get('latest_report') or summary.get('report')}", flush=True)
        return 0
    except Exception as exc:
        _write_status("ERROR", phase="FAILED", day=str(day), error=f"{type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
