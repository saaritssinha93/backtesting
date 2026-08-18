"""
Signal-only candidate scanner for "Signal discovery v7 5mins ID".

This module scans the completed 5-minute signal candle and returns candidate
tickers only. It deliberately does not emit entry_ts, entry_price, SL, target,
or trade signal CSV rows. Entry is a separate 1-minute module.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_backtesting as v7
import eqidv2_late_bb10_compression as late_bb10
import hilega_milega_setups as hm


IST_TZ = "Asia/Kolkata"
LIVE_5M_DIR = Path(
    os.getenv(
        "EQIDV2_ID5MIN_V7_LIVE_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live",
    )
)

# v8 backtesting filters candidates through v7 exclusions and v6
# setup-specific exits before resolving trades. Keep live discovery on the
# same setup universe by default while still writing signal-only candidates.
EXCLUDED_SETUPS = set(v7.EXCLUDED_SETUPS)
ALLOWED_SETUPS = set(v6.SETUP_EXIT_RULES)
SELECTION_MODE = os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SELECTION_MODE", "v8_setup_compatible")
FILTER_TO_V8_EXIT_SETUPS = SELECTION_MODE.strip().lower() in {
    "v8",
    "v8_setup_compatible",
    "v8_compatible",
}

DEFAULT_SCAN_WORKERS = max(1, int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_WORKERS", "8")))
DEFAULT_SCAN_CHUNKSIZE = max(
    1,
    int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_CHUNKSIZE", "24")),
)
SCAN_CACHE_ENABLED = os.getenv(
    "EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_CACHE_ENABLED",
    "1",
).strip().lower() not in {"0", "false", "no", "off"}
# Per process: enough for a normal 24-worker shard while bounding memory if
# ProcessPool scheduling moves shards between workers over later slots.
SCAN_CACHE_MAX_TICKERS = max(
    1,
    int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_SCAN_CACHE_MAX_TICKERS", "128")),
)


@dataclass
class _TickerFrameCache:
    """Process-local cache, reusable only for an exactly verified input."""

    fingerprint: Tuple[int, int, int]
    frame: pd.DataFrame
    prepared_key: Optional[Tuple[Any, ...]] = None
    prepared: Optional[pd.DataFrame] = None


_LOCAL_FRAME_CACHE: Dict[str, _TickerFrameCache] = {}
_LAST_SCAN_TELEMETRY: Dict[str, Any] = {}
_HM_FNO_UNIVERSE_CACHE: Dict[str, Any] = {}

HM_FNO_SETUP = hm.SHORT_RSI50_REVERSAL
HM_FNO_SIGNAL_TF_MIN = 60
HM_FNO_ENTRY_LAG_MIN = int(os.getenv("EQIDV2_HM_FNO_ENTRY_LAG_MIN", "1"))
HM_FNO_SIGNAL_WARMUP_DAYS = int(os.getenv("EQIDV2_HM_FNO_WARMUP_DAYS", "45"))
HM_FNO_SIGNAL_START_MIN = 12 * 60 + 15
HM_FNO_SIGNAL_END_MIN = 14 * 60 + 15
HM_FNO_MIN_LINE_DISTANCE = float(os.getenv("EQIDV2_HM_FNO_MIN_LINE_DISTANCE", "6.0"))
HM_FNO_SHORT_MAX_RSI = float(os.getenv("EQIDV2_HM_FNO_SHORT_MAX_RSI", "47.0"))
HM_FNO_RISK_REWARD = float(os.getenv("EQIDV2_HM_FNO_RISK_REWARD", "1.35"))
HM_FNO_MIN_RISK_PCT = float(os.getenv("EQIDV2_HM_FNO_MIN_RISK_PCT", "1.0"))
HM_FNO_MAX_RISK_PCT = float(os.getenv("EQIDV2_HM_FNO_MAX_RISK_PCT", "1.25"))
HM_FNO_UNIVERSE_PATH = os.getenv(
    "EQIDV2_HILEGA_MILEGA_FNO_UNIVERSE",
    r"C:\TradingData\eqidv2\fno_oi\universe\latest_near_month.parquet",
)


def _new_ticker_telemetry() -> Dict[str, Any]:
    return {
        "raw_cache_hits": 0,
        "raw_cache_misses": 0,
        "prepared_cache_hits": 0,
        "prepared_cache_misses": 0,
        "unchanged_frame_hits": 0,
        "file_read_seconds": 0.0,
        "prepare_seconds": 0.0,
        "strategy_seconds": 0.0,
        "unstable_file_reads": 0,
        "ticker_errors": 0,
        "ticker_elapsed_seconds": 0.0,
    }


def _bump_telemetry(
    telemetry: Optional[Dict[str, Any]],
    key: str,
    amount: float | int = 1,
) -> None:
    if telemetry is not None:
        telemetry[key] = telemetry.get(key, 0) + amount


def _file_fingerprint(fp: Path) -> Optional[Tuple[int, int, int]]:
    try:
        stat = fp.stat()
    except OSError:
        return None
    return (int(stat.st_mtime_ns), int(stat.st_ctime_ns), int(stat.st_size))


def _remember_frame(
    ticker: str,
    fingerprint: Tuple[int, int, int],
    frame: pd.DataFrame,
) -> _TickerFrameCache:
    if ticker not in _LOCAL_FRAME_CACHE and len(_LOCAL_FRAME_CACHE) >= SCAN_CACHE_MAX_TICKERS:
        _LOCAL_FRAME_CACHE.pop(next(iter(_LOCAL_FRAME_CACHE)))
    entry = _TickerFrameCache(fingerprint=fingerprint, frame=frame)
    _LOCAL_FRAME_CACHE[ticker] = entry
    return entry

EARLY_MODE_ENABLE = str(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MODE", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
    "disabled",
}
EARLY_START = pd.Timestamp("09:30").time()
EARLY_END = pd.Timestamp("11:00").time()
EARLY_OR_MINUTES = int(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_OR_MINUTES", "15"))
EARLY_MIN_5M_TRADED_VALUE_RS = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_5M_TRADED_VALUE_RS", "1000000"))
EARLY_MAX_VWAP_DIST_ATR = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MAX_VWAP_DIST_ATR", "2.80"))
EARLY_MAX_CANDLE_RANGE_ATR = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MAX_CANDLE_RANGE_ATR", "3.80"))
EARLY_MIN_BODY_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_BODY_PCT", "0.42"))
EARLY_MIN_VOL_RATIO = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_VOL_RATIO", "1.10"))
EARLY_SELECTION_MODE = "early_v1"
EARLY_TIGHT_FILTERS_ENABLE = str(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_TIGHT_FILTERS", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
    "disabled",
}
EARLY_BLOCKED_SETUPS_DEFAULT = ",".join(
    [
        "E_RS_FIRST_HOUR_BREAK_LONG",
        "E_RS_FIRST_HOUR_BREAK_SHORT",
        "E_VWAP_RECLAIM_EARLY_LONG",
        "E_FAILED_OR_BREAKOUT_TRAP_SHORT",
        "E_ORB_RETEST_HOLD_SHORT",
        "E_ORB_RETEST_HOLD_LONG",
        "E_FAILED_OR_BREAKDOWN_TRAP_LONG",
        "E_GAP_HOLD_CONTINUATION_LONG",
        "E_GAP_HOLD_CONTINUATION_SHORT",
        "E_OPENING_DRIVE_CONTINUATION_LONG",
        "E_OPENING_DRIVE_CONTINUATION_SHORT",
    ]
)
EARLY_BLOCKED_SETUPS = {
    x.strip().upper()
    for x in os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_BLOCKED_SETUPS", EARLY_BLOCKED_SETUPS_DEFAULT).split(",")
    if x.strip()
}
EARLY_ORB_LONG_MAX_VOL_RATIO = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VOL_RATIO", "2.00"))
EARLY_ORB_LONG_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MIN_RS_PCT", "4.00"))
EARLY_ORB_LONG_MAX_VWAP_DIST_ATR = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VWAP_DIST_ATR", "1.80"))
EARLY_GAP_LONG_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_RS_PCT", "3.00"))
EARLY_GAP_LONG_MIN_QUALITY = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_QUALITY", "160.00"))
EARLY_ORB_SHORT_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_RS_PCT", "-1.50"))
EARLY_ORB_SHORT_MAX_ATR_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MAX_ATR_PCT", "0.0065"))
EARLY_ORB_SHORT_MIN_BODY_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_BODY_PCT", "0.82"))
EARLY_VWAP_SHORT_MIN_RS_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_RS_PCT", "-1.20"))
EARLY_VWAP_SHORT_MIN_CLOSE_LOC = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_CLOSE_LOC", "0.08"))
EARLY_VWAP_SHORT_MAX_ATR_PCT = float(os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MAX_ATR_PCT", "0.008"))

RESEARCH_SHADOW_VERSION = "v7_research_2026_06_03"
RESEARCH_PROBATION_SETUPS = {
    x.strip().upper()
    for x in os.getenv(
        "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_PROBATION_SETUPS",
        "T_TREND_DAY_EMA_STAIR_SHORT,C_OR_BREAKDOWN,L_TREND_PULLBACK",
    ).split(",")
    if x.strip()
}
RESEARCH_ANTI_CHASE_LONG_CLOSE_LOC_MIN = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_CLOSE_LOC_MIN", "0.97")
)
RESEARCH_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN = float(
    os.getenv("EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN", "3.50")
)


def _ensure_ist_ts(ts: Any) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if out.tz is None:
        out = out.tz_localize(IST_TZ)
    else:
        out = out.tz_convert(IST_TZ)
    return out


def _read_one(fp: Path) -> Optional[pd.DataFrame]:
    if not fp.exists():
        return None
    try:
        df = v2._read_ohlcv(fp)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df


def _load_live_5m_cached(
    ticker: str,
    telemetry: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[_TickerFrameCache]]:
    ticker = str(ticker).upper()
    fp = LIVE_5M_DIR / f"{ticker}_stocks_indicators_5min.parquet"
    fingerprint_before = _file_fingerprint(fp)
    cached = _LOCAL_FRAME_CACHE.get(ticker) if SCAN_CACHE_ENABLED else None
    if (
        fingerprint_before is not None
        and cached is not None
        and cached.fingerprint == fingerprint_before
    ):
        _bump_telemetry(telemetry, "raw_cache_hits")
        return cached.frame, cached

    _bump_telemetry(telemetry, "raw_cache_misses")
    read_started = time.perf_counter()
    df = _read_one(fp)
    _bump_telemetry(
        telemetry,
        "file_read_seconds",
        time.perf_counter() - read_started,
    )
    if df is None or "date" not in df.columns:
        return None, None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None, None
    normalized = (
        df.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    fingerprint_after = _file_fingerprint(fp)
    if (
        SCAN_CACHE_ENABLED
        and fingerprint_before is not None
        and fingerprint_before == fingerprint_after
    ):
        # A feed may rewrite an unchanged illiquid symbol.  Retain prepared
        # state only after exact equality, never from timestamp/shape alone.
        if cached is not None and cached.frame.equals(normalized):
            cached.fingerprint = fingerprint_after
            _bump_telemetry(telemetry, "unchanged_frame_hits")
            return cached.frame, cached
        return normalized, _remember_frame(ticker, fingerprint_after, normalized)
    if fingerprint_before != fingerprint_after:
        _bump_telemetry(telemetry, "unstable_file_reads")
    _LOCAL_FRAME_CACHE.pop(ticker, None)
    return normalized, None


def _load_live_5m(ticker: str) -> Optional[pd.DataFrame]:
    """Compatibility loader used by market-alignment and research helpers."""
    frame, _ = _load_live_5m_cached(ticker)
    return frame


def _append_synthetic_successor(prepared_day: pd.DataFrame, slot_ts: pd.Timestamp) -> pd.DataFrame:
    """Let v2._scan_day evaluate the latest signal candle without using entry.

    v2._scan_day loops to len(df)-1 because its normal trade candidate needs
    next_row for entry. For signal discovery, entry is intentionally absent.
    We append one synthetic successor after the completed signal candle only so
    v2 can evaluate the candle at slot_ts. All successor/entry fields are
    discarded from output.
    """
    if prepared_day.empty:
        return prepared_day
    last = prepared_day.iloc[-1].copy()
    next_ts = slot_ts + pd.Timedelta(minutes=5)
    last["date"] = next_ts
    if "date_only" in last.index:
        last["date_only"] = next_ts.date()
    close = float(last.get("close", np.nan))
    if np.isfinite(close):
        for col in ("open", "high", "low", "close"):
            if col in last.index:
                last[col] = close
    if "volume" in last.index:
        last["volume"] = 0
    return pd.concat([prepared_day, pd.DataFrame([last])], ignore_index=True)


def _scan_late_bb10_signal(
    raw: pd.DataFrame,
    ticker: str,
    slot_ts: Any,
) -> Optional[Tuple["v2.Candidate", Dict[str, Any]]]:
    """Build the shared late-BB10 candidate without using future candles."""
    slot = _ensure_ist_ts(slot_ts).floor("min")
    try:
        custom = late_bb10.signal_for_slot(raw, slot)
    except Exception:
        return None
    if custom is None:
        return None
    close = float(custom["close"])
    candle_range = float(custom["high"]) - float(custom["low"])
    body_pct = abs(float(custom["close"]) - float(custom["open"])) / candle_range if candle_range > 0 else 0.0
    vwap_dist_atr = (
        (close - float(custom["avwap"])) / float(custom["atr"])
        if float(custom["atr"]) > 0 else np.nan
    )
    candidate = v2.Candidate(
        ticker=str(ticker).upper(),
        date=str(slot.date()),
        setup=late_bb10.SETUP,
        side="LONG",
        signal_ts=slot,
        signal_close=close,
        entry_ts=slot + pd.Timedelta(minutes=1),
        entry_px=float(custom["entry_trigger_price"]),
        target_px=0.0,
        sl_px=0.0,
        quality_score=float(custom["quality_score"]),
        rs_pct=0.0,
        market_ret_pct=0.0,
        regime="NEUTRAL",
        vol_ratio=float(custom["rel_volume"]),
        atr_pct=float(custom["atr_pct"]),
        close_loc=float(custom["close_loc"]),
        body_pct=float(body_pct),
        vwap_dist_atr=float(vwap_dist_atr),
        day_value_so_far_rs=float(custom["traded_value"]),
        reason="late_bb10_causal_compression_breakout",
    )
    return candidate, custom


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def _hm_norm_symbol(value: Any) -> str:
    return str(value or "").strip().upper().replace(".NS", "")


def _hm_fno_enabled() -> bool:
    if str(os.getenv("EQIDV2_HILEGA_MILEGA_FNO_PAPER", "1")).strip().lower() in {
        "0",
        "false",
        "no",
        "off",
        "disabled",
    }:
        return False
    return HM_FNO_SETUP in ALLOWED_SETUPS


def _hm_load_fno_universe() -> frozenset[str]:
    path = Path(HM_FNO_UNIVERSE_PATH)
    try:
        stat = path.stat()
        key = (str(path), int(stat.st_mtime_ns), int(stat.st_size))
    except OSError:
        key = (str(path), 0, 0)
    if _HM_FNO_UNIVERSE_CACHE.get("key") == key:
        return _HM_FNO_UNIVERSE_CACHE.get("symbols", frozenset())

    symbols: set[str] = set()
    if path.exists():
        try:
            universe = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
            if "is_index_future" in universe.columns:
                universe = universe.loc[~universe["is_index_future"].fillna(False).astype(bool)]
            symbol_column = next(
                (
                    col
                    for col in ("underlying", "ticker", "symbol", "tradingsymbol")
                    if col in universe.columns
                ),
                None,
            )
            if symbol_column is not None:
                symbols = {
                    _hm_norm_symbol(x)
                    for x in universe[symbol_column].dropna().tolist()
                    if _hm_norm_symbol(x)
                }
        except Exception:
            symbols = set()
    out = frozenset(symbols)
    _HM_FNO_UNIVERSE_CACHE["key"] = key
    _HM_FNO_UNIVERSE_CACHE["symbols"] = out
    return out


def _hm_normalise_5m(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty or "date" not in frame.columns:
        return pd.DataFrame()
    required = [c for c in ("date", "open", "high", "low", "close", "volume") if c in frame.columns]
    out = frame[required].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    if getattr(out["date"].dt, "tz", None) is None:
        out["date"] = out["date"].dt.tz_localize(IST_TZ)
    else:
        out["date"] = out["date"].dt.tz_convert(IST_TZ)
    out = out.dropna(subset=["date", "open", "high", "low", "close"]).copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    opening = out["date"].dt.hour.eq(9) & out["date"].dt.minute.eq(15)
    out = out.loc[~opening.fillna(False)].copy()
    return (
        out.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )


def _hm_aggregate_signal_bars(frame: pd.DataFrame, minutes: int = HM_FNO_SIGNAL_TF_MIN) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
    expected_rows = int(minutes) // 5
    pieces: List[pd.DataFrame] = []
    working = frame.copy()
    working["trade_day"] = working["date"].dt.date
    agg_spec: Dict[str, Tuple[str, str]] = {
        "open": ("open", "first"),
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close": ("close", "last"),
        "source_rows": ("close", "count"),
    }
    if "volume" in working.columns:
        agg_spec["volume"] = ("volume", "sum")
    for _, day in working.groupby("trade_day", sort=True):
        indexed = day.set_index("date").sort_index()
        aggregate = indexed.resample(
            f"{int(minutes)}min",
            origin="start_day",
            offset="15min",
            closed="right",
            label="right",
        ).agg(**agg_spec)
        aggregate = aggregate.loc[aggregate["source_rows"].eq(expected_rows)]
        if not aggregate.empty:
            pieces.append(aggregate.reset_index())
    if not pieces:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close"])
    return pd.concat(pieces, ignore_index=True).sort_values("date").reset_index(drop=True)


def _hm_minute(ts: pd.Timestamp) -> int:
    return int(ts.hour) * 60 + int(ts.minute)


def _scan_hm_fno_short_signal(
    raw: pd.DataFrame,
    ticker: str,
    slot_ts: Any,
) -> Optional[Tuple["v2.Candidate", Dict[str, Any]]]:
    """Emit the approved FnO Hilega short signal on completed 60-minute bars."""
    if not _hm_fno_enabled():
        return None
    symbol = _hm_norm_symbol(ticker)
    if symbol not in _hm_load_fno_universe():
        return None
    slot = _ensure_ist_ts(slot_ts).floor("min")
    signal_minute = _hm_minute(slot)
    if signal_minute < HM_FNO_SIGNAL_START_MIN or signal_minute > HM_FNO_SIGNAL_END_MIN:
        return None
    if slot.minute != 15:
        return None

    frame = _hm_normalise_5m(raw)
    if frame.empty:
        return None
    start = (slot - pd.Timedelta(days=max(1, HM_FNO_SIGNAL_WARMUP_DAYS))).normalize()
    frame = frame.loc[(frame["date"] >= start) & (frame["date"] <= slot)].copy()
    if frame.empty:
        return None
    bars = _hm_aggregate_signal_bars(frame, HM_FNO_SIGNAL_TF_MIN)
    if bars.empty:
        return None
    try:
        features = hm.add_hilega_milega_features(bars)
    except Exception:
        return None
    signal_rows = features[features["date"].dt.floor("min").eq(slot)]
    if signal_rows.empty:
        return None
    signal = signal_rows.iloc[-1]
    if not bool(signal.get(hm.SETUP_FLAG_COLUMNS[HM_FNO_SETUP], False)):
        return None
    rsi = _safe_float(signal.get(hm.RSI_COLUMN))
    line_distance = _safe_float(signal.get("HM_LINE_DISTANCE"))
    if not (np.isfinite(rsi) and rsi <= HM_FNO_SHORT_MAX_RSI):
        return None
    if not (np.isfinite(line_distance) and line_distance >= HM_FNO_MIN_LINE_DISTANCE):
        return None

    close = _safe_float(signal.get("close"))
    high = _safe_float(signal.get("high"))
    low = _safe_float(signal.get("low"))
    open_price = _safe_float(signal.get("open"))
    if not all(np.isfinite(x) and x > 0 for x in (close, high, low, open_price)):
        return None
    candle_range = max(high - low, 0.0)
    close_loc = (close - low) / candle_range if candle_range > 0 else np.nan
    body_pct = abs(close - open_price) / candle_range if candle_range > 0 else 0.0
    atr_pct = candle_range / close if close > 0 else np.nan
    quality = 100.0 + max(0.0, line_distance) + max(0.0, HM_FNO_SHORT_MAX_RSI - rsi)
    signal_row = signal.to_dict()
    signal_row.update(
        {
            "candidate_family": "HILEGA_MILEGA_FNO",
            "selection_mode": "hilega_milega_fno_60m",
            "signal_timestamp_convention": "end_labeled_completed_60m",
            "signal_timeframe_min": HM_FNO_SIGNAL_TF_MIN,
            "entry_lag_min": HM_FNO_ENTRY_LAG_MIN,
            "hm_signal_bar_time_ist": _fmt_ist(slot),
            "hm_signal_stop_price": high,
            "hm_risk_reward": HM_FNO_RISK_REWARD,
            "hm_min_risk_pct": HM_FNO_MIN_RISK_PCT,
            "hm_max_risk_pct": HM_FNO_MAX_RISK_PCT,
            "hm_rsi_9": rsi,
            "hm_rsi_ema_3": _safe_float(signal.get(hm.EMA_COLUMN)),
            "hm_rsi_wma_21": _safe_float(signal.get(hm.WMA_COLUMN)),
            "hm_line_distance": line_distance,
            "hm_exit_model": "signal_candle_high_stop_1_35R",
            "RSI": rsi,
            "ADX": np.nan,
            "vol_ratio": np.nan,
        }
    )
    candidate = v2.Candidate(
        ticker=symbol,
        date=str(slot.date()),
        setup=HM_FNO_SETUP,
        side="SHORT",
        signal_ts=slot,
        signal_close=close,
        entry_ts=slot + pd.Timedelta(minutes=HM_FNO_ENTRY_LAG_MIN),
        entry_px=close,
        target_px=0.0,
        sl_px=high,
        quality_score=float(quality),
        rs_pct=0.0,
        market_ret_pct=0.0,
        regime="NEUTRAL",
        vol_ratio=0.0,
        atr_pct=float(atr_pct),
        close_loc=float(close_loc),
        body_pct=float(body_pct),
        vwap_dist_atr=0.0,
        day_value_so_far_rs=0.0,
        reason="hm_rsi50_reversal_60m_fno",
    )
    return candidate, signal_row


def _early_signal_window(ts: pd.Timestamp) -> bool:
    t = _ensure_ist_ts(ts).time()
    return EARLY_START <= t <= EARLY_END


def _early_atr(day_df: pd.DataFrame, idx: int) -> float:
    row_atr = _safe_float(day_df.iloc[idx].get("ATR"))
    if np.isfinite(row_atr) and row_atr > 0:
        return row_atr
    work = day_df.iloc[: idx + 1].copy()
    prev_close = pd.to_numeric(work["close"], errors="coerce").shift(1)
    high = pd.to_numeric(work["high"], errors="coerce")
    low = pd.to_numeric(work["low"], errors="coerce")
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.tail(6).mean()
    return float(atr) if np.isfinite(atr) and atr > 0 else float("nan")


def _early_vol_ratio(day_df: pd.DataFrame, idx: int) -> float:
    row_ratio = _safe_float(day_df.iloc[idx].get("vol_ratio"))
    if np.isfinite(row_ratio) and row_ratio > 0:
        return row_ratio
    if idx <= 0:
        return float("nan")
    prev = pd.to_numeric(day_df["volume"].iloc[max(0, idx - 4):idx], errors="coerce").dropna()
    base = float(prev.mean()) if not prev.empty else float("nan")
    vol = _safe_float(day_df.iloc[idx].get("volume"))
    if np.isfinite(base) and base > 0 and np.isfinite(vol):
        return float(vol / base)
    return float("nan")


def _early_opening_range(day_df: pd.DataFrame) -> tuple[float, float, float, float]:
    start = _ensure_ist_ts(day_df["date"].iloc[0])
    cutoff = start + pd.Timedelta(minutes=EARLY_OR_MINUTES)
    dates = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(dates.dt, "tz", None) is None:
        dates = dates.dt.tz_localize(IST_TZ)
    else:
        dates = dates.dt.tz_convert(IST_TZ)
    opening = day_df.loc[dates < cutoff]
    if opening.empty:
        return float("nan"), float("nan"), float("nan"), float("nan")
    high = float(pd.to_numeric(opening["high"], errors="coerce").max())
    low = float(pd.to_numeric(opening["low"], errors="coerce").min())
    open_px = _safe_float(opening.iloc[0].get("open"))
    close_px = _safe_float(opening.iloc[-1].get("close"))
    return high, low, open_px, close_px


def _bar_market_context(market_ctx: Dict[str, Dict[str, Any]], day: str, ts: pd.Timestamp) -> tuple[float, str]:
    try:
        return v2._bar_context(market_ctx, day, ts)
    except Exception:
        return 0.0, "NEUTRAL"


def _early_candidate(
    ticker: str,
    day: str,
    setup: str,
    side: str,
    row: pd.Series,
    next_row: pd.Series,
    *,
    rs_pct: float,
    market_ret: float,
    regime: str,
    reason: str,
    early_vol_ratio: float,
    atr: float,
    vwap_dist_atr: float,
    score_boost: float = 0.0,
) -> "v2.Candidate":
    close = _safe_float(row.get("close"))
    body_pct = _safe_float(row.get("body_pct"), 0.0)
    close_loc = _safe_float(row.get("close_loc"), 0.5)
    signal_volume = _safe_float(row.get("volume"), 0.0)
    day_value = _safe_float(row.get("day_value_so_far_rs"), close * signal_volume if np.isfinite(close) else 0.0)
    side_u = str(side).upper()
    loc_score = close_loc if side_u == "LONG" else 1.0 - close_loc
    score = (
        30.0
        + 16.0 * max(abs(rs_pct), 0.0)
        + 8.0 * max(early_vol_ratio, 0.0)
        + 18.0 * max(body_pct, 0.0)
        + 16.0 * max(loc_score, 0.0)
        - 5.0 * max(0.0, abs(vwap_dist_atr) - 1.5)
        + score_boost
    )
    entry_px = _safe_float(next_row.get("open"), close)
    return v2.Candidate(
        ticker=str(ticker).upper(),
        date=day,
        setup=setup,
        side=side_u,
        signal_ts=pd.Timestamp(row["date"]),
        signal_close=close,
        entry_ts=pd.Timestamp(next_row.get("date", row["date"])),
        entry_px=entry_px,
        target_px=entry_px,
        sl_px=entry_px,
        quality_score=float(score),
        rs_pct=float(rs_pct),
        market_ret_pct=float(market_ret),
        regime=regime,
        vol_ratio=float(early_vol_ratio),
        atr_pct=float(atr / close) if np.isfinite(atr) and np.isfinite(close) and close > 0 else float("nan"),
        close_loc=float(close_loc),
        body_pct=float(body_pct),
        vwap_dist_atr=float(vwap_dist_atr),
        day_value_so_far_rs=float(day_value),
        reason=reason,
    )


def _early_tight_filter(
    candidate: "v2.Candidate",
    *,
    setup: str,
    rs_pct: float,
    early_vol_ratio: float,
    atr: float,
    close: float,
    body_pct: float,
    close_loc: float,
    vwap_dist_atr: float,
) -> bool:
    if not EARLY_TIGHT_FILTERS_ENABLE:
        return True

    setup_u = str(setup).upper().strip()
    if setup_u in EARLY_BLOCKED_SETUPS:
        return False

    atr_pct = atr / close if np.isfinite(atr) and np.isfinite(close) and close > 0 else float("nan")
    quality = _safe_float(getattr(candidate, "quality_score", np.nan))

    if setup_u == "E_ORB_BREAKOUT_LONG":
        return (
            early_vol_ratio <= EARLY_ORB_LONG_MAX_VOL_RATIO
            and rs_pct >= EARLY_ORB_LONG_MIN_RS_PCT
            and vwap_dist_atr <= EARLY_ORB_LONG_MAX_VWAP_DIST_ATR
        )

    if setup_u == "E_GAP_HOLD_CONTINUATION_LONG":
        return rs_pct >= EARLY_GAP_LONG_MIN_RS_PCT and quality >= EARLY_GAP_LONG_MIN_QUALITY

    if setup_u == "E_ORB_BREAKOUT_SHORT":
        return (
            rs_pct >= EARLY_ORB_SHORT_MIN_RS_PCT
            and np.isfinite(atr_pct)
            and atr_pct <= EARLY_ORB_SHORT_MAX_ATR_PCT
            and body_pct >= EARLY_ORB_SHORT_MIN_BODY_PCT
        )

    if setup_u == "E_VWAP_LOSE_EARLY_SHORT":
        return (
            rs_pct >= EARLY_VWAP_SHORT_MIN_RS_PCT
            and close_loc >= EARLY_VWAP_SHORT_MIN_CLOSE_LOC
            and np.isfinite(atr_pct)
            and atr_pct <= EARLY_VWAP_SHORT_MAX_ATR_PCT
        )

    return True


def _scan_early_slot_candidates(
    day_df: pd.DataFrame,
    ticker: str,
    slot_ts: pd.Timestamp,
    market_ctx: Dict[str, Dict[str, Any]],
) -> List["v2.Candidate"]:
    if not EARLY_MODE_ENABLE or day_df.empty or not _early_signal_window(slot_ts):
        return []

    dates = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(dates.dt, "tz", None) is None:
        dates = dates.dt.tz_localize(IST_TZ)
    else:
        dates = dates.dt.tz_convert(IST_TZ)
    idxs = np.where(dates.dt.floor("min").eq(slot_ts.floor("min")))[0]
    if len(idxs) == 0:
        return []
    idx = int(idxs[-1])
    if idx < 3:
        return []

    row = day_df.iloc[idx]
    next_row = day_df.iloc[idx + 1] if idx + 1 < len(day_df) else row
    day = str(row.get("date_only", slot_ts.date()))
    close = _safe_float(row.get("close"))
    open_px = _safe_float(row.get("open"))
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    volume = _safe_float(row.get("volume"), 0.0)
    traded_value = close * volume if np.isfinite(close) else 0.0
    if not np.isfinite(close) or close < v2.MIN_PRICE or traded_value < EARLY_MIN_5M_TRADED_VALUE_RS:
        return []

    atr = _early_atr(day_df, idx)
    rng = high - low if np.isfinite(high) and np.isfinite(low) else float("nan")
    if np.isfinite(atr) and atr > 0 and np.isfinite(rng) and rng > EARLY_MAX_CANDLE_RANGE_ATR * atr:
        return []

    vwap = _safe_float(row.get("VWAP"))
    if not np.isfinite(vwap) or not np.isfinite(atr) or atr <= 0:
        return []
    vwap_dist_atr = (close - vwap) / atr
    close_loc = _safe_float(row.get("close_loc"), 0.5)
    body_pct = _safe_float(row.get("body_pct"), 0.0)
    early_vol = _early_vol_ratio(day_df, idx)
    if not np.isfinite(early_vol):
        return []

    day_open = _safe_float(day_df.iloc[0].get("open"))
    stock_ret = (close / day_open - 1.0) * 100.0 if np.isfinite(day_open) and day_open > 0 else 0.0
    market_ret, regime = _bar_market_context(market_ctx, day, slot_ts)
    rs_pct = stock_ret - market_ret
    or_high, or_low, or_open, or_close = _early_opening_range(day_df)
    if not np.isfinite(or_high) or not np.isfinite(or_low):
        return []

    prev = day_df.iloc[idx - 1]
    prev_close = _safe_float(prev.get("close"))
    prev_high = _safe_float(prev.get("high"))
    prev_low = _safe_float(prev.get("low"))
    prev_vwap = _safe_float(prev.get("VWAP"), vwap)
    prior = day_df.iloc[3:idx]
    prior_broke_high = (not prior.empty) and bool((pd.to_numeric(prior["high"], errors="coerce") > or_high).any())
    prior_broke_low = (not prior.empty) and bool((pd.to_numeric(prior["low"], errors="coerce") < or_low).any())
    first15_ret = (or_close / or_open - 1.0) * 100.0 if np.isfinite(or_open) and or_open > 0 and np.isfinite(or_close) else 0.0
    prev_day_close = _safe_float(row.get("Prev_Day_Close"))
    gap_pct = (day_open / prev_day_close - 1.0) * 100.0 if np.isfinite(prev_day_close) and prev_day_close > 0 else float("nan")
    opening_low = float(pd.to_numeric(day_df["low"].iloc[:3], errors="coerce").min())
    opening_high = float(pd.to_numeric(day_df["high"].iloc[:3], errors="coerce").max())
    upper_wick = _safe_float(row.get("upper_wick_pct"), 0.0)
    lower_wick = _safe_float(row.get("lower_wick_pct"), 0.0)

    above_vwap = close > vwap
    below_vwap = close < vwap
    out: List["v2.Candidate"] = []

    def add(setup: str, side: str, condition: bool, reason: str, boost: float = 0.0) -> None:
        if condition:
            candidate = _early_candidate(
                ticker,
                day,
                setup,
                side,
                row,
                next_row,
                rs_pct=rs_pct,
                market_ret=market_ret,
                regime=regime,
                reason=reason,
                early_vol_ratio=early_vol,
                atr=atr,
                vwap_dist_atr=vwap_dist_atr,
                score_boost=boost,
            )
            if _early_tight_filter(
                candidate,
                setup=setup,
                rs_pct=rs_pct,
                early_vol_ratio=early_vol,
                atr=atr,
                close=close,
                body_pct=body_pct,
                close_loc=close_loc,
                vwap_dist_atr=vwap_dist_atr,
            ):
                out.append(candidate)

    common_long = (
        body_pct >= EARLY_MIN_BODY_PCT
        and early_vol >= EARLY_MIN_VOL_RATIO
        and regime != "BEAR"
        and 0.0 <= vwap_dist_atr <= EARLY_MAX_VWAP_DIST_ATR
    )
    common_short = (
        body_pct >= EARLY_MIN_BODY_PCT
        and early_vol >= EARLY_MIN_VOL_RATIO
        and regime != "BULL"
        and -EARLY_MAX_VWAP_DIST_ATR <= vwap_dist_atr <= 0.0
    )

    add(
        "E_ORB_BREAKOUT_LONG",
        "LONG",
        common_long and close > or_high and above_vwap and close_loc >= 0.70 and rs_pct >= 0.20,
        "early_opening_range_breakout_long",
        10.0,
    )
    add(
        "E_ORB_BREAKOUT_SHORT",
        "SHORT",
        common_short and close < or_low and below_vwap and close_loc <= 0.30 and rs_pct <= -0.20,
        "early_opening_range_breakout_short",
        10.0,
    )
    add(
        "E_ORB_RETEST_HOLD_LONG",
        "LONG",
        common_long and prior_broke_high and low <= or_high + 0.35 * atr and close > or_high and above_vwap and close_loc >= 0.55 and rs_pct >= 0.0,
        "early_orb_retest_hold_long",
        7.0,
    )
    add(
        "E_ORB_RETEST_HOLD_SHORT",
        "SHORT",
        common_short and prior_broke_low and high >= or_low - 0.35 * atr and close < or_low and below_vwap and close_loc <= 0.45 and rs_pct <= 0.0,
        "early_orb_retest_hold_short",
        7.0,
    )
    add(
        "E_VWAP_RECLAIM_EARLY_LONG",
        "LONG",
        common_long and prev_close <= prev_vwap and close > vwap and close > prev_high and close_loc >= 0.65 and rs_pct >= 0.10 and vwap_dist_atr <= 1.80,
        "early_vwap_reclaim_break_prev_high",
        6.0,
    )
    add(
        "E_VWAP_LOSE_EARLY_SHORT",
        "SHORT",
        common_short and prev_close >= prev_vwap and close < vwap and close < prev_low and close_loc <= 0.35 and rs_pct <= -0.10 and vwap_dist_atr >= -1.80,
        "early_vwap_lose_break_prev_low",
        6.0,
    )
    # S9_MIDDAY_LOSE (SHORT) — USER_DIRECTED promotion 2026-06-30 (research verdict break-even/REJECT;
    # see final_setup_conf.py provenance + Train_and_Test/long_setup_discovery_from_raw_data/claude_engine/).
    # Late-morning (10:20-11:00 IST) failed bounce that loses VWAP: prior bar at/above VWAP, this bar
    # red and below VWAP, breaks prior bar low, after a positive 3-bar push, with ATR room (>=0.30%).
    # Emitted only when the conf book is active (v2.ENABLE_S9_MIDDAY_LOSE, default OFF). Exit 1.25/2.50
    # is set in final_setup_conf. Placed in the early-slot layer because its edge window (10:20-11:00)
    # is before v2._scan_day's catalog (which only scans from ~11:00 onward).
    _s9_c3 = _safe_float(day_df["close"].iloc[idx - 3]) if idx >= 3 else float("nan")
    _s9_mom3 = (close / _s9_c3 - 1.0) * 100.0 if np.isfinite(_s9_c3) and _s9_c3 > 0 else 0.0
    add(
        "S9_MIDDAY_LOSE",
        "SHORT",
        bool(getattr(v2, "ENABLE_S9_MIDDAY_LOSE", False))
        and _ensure_ist_ts(slot_ts).time() >= pd.Timestamp("10:20").time()
        and below_vwap and close < open_px and close < prev_low
        and prev_close >= 0.999 * vwap and _s9_mom3 >= 0.10
        and np.isfinite(atr) and atr > 0 and (atr / close) >= 0.0030
        and regime != "BULL",
        "midday_vwap_lose_failed_bounce_short",
        6.0,
    )
    add(
        "E_GAP_HOLD_CONTINUATION_LONG",
        "LONG",
        common_long and np.isfinite(gap_pct) and gap_pct >= 0.50 and opening_low >= prev_day_close * 0.997 and close > or_high and above_vwap and rs_pct >= 0.20,
        "early_gap_up_hold_continuation",
        8.0,
    )
    add(
        "E_GAP_HOLD_CONTINUATION_SHORT",
        "SHORT",
        common_short and np.isfinite(gap_pct) and gap_pct <= -0.50 and opening_high <= prev_day_close * 1.003 and close < or_low and below_vwap and rs_pct <= -0.20,
        "early_gap_down_hold_continuation",
        8.0,
    )
    add(
        "E_RS_FIRST_HOUR_BREAK_LONG",
        "LONG",
        common_long and stock_ret >= 1.0 and rs_pct >= 0.50 and close > max(or_high, prev_high) and above_vwap,
        "early_relative_strength_first_hour_break",
        9.0,
    )
    add(
        "E_RS_FIRST_HOUR_BREAK_SHORT",
        "SHORT",
        common_short and stock_ret <= -1.0 and rs_pct <= -0.50 and close < min(or_low, prev_low) and below_vwap,
        "early_relative_weakness_first_hour_break",
        9.0,
    )
    add(
        "E_OPENING_DRIVE_CONTINUATION_LONG",
        "LONG",
        common_long and first15_ret >= 0.50 and close > prev_high and close > or_high and rs_pct >= 0.30,
        "early_opening_drive_continuation_long",
        5.0,
    )
    add(
        "E_OPENING_DRIVE_CONTINUATION_SHORT",
        "SHORT",
        common_short and first15_ret <= -0.50 and close < prev_low and close < or_low and rs_pct <= -0.30,
        "early_opening_drive_continuation_short",
        5.0,
    )
    add(
        "E_FAILED_OR_BREAKOUT_TRAP_SHORT",
        "SHORT",
        early_vol >= 1.0 and regime != "BULL" and high > or_high and close < or_high and (below_vwap or close_loc <= 0.35) and upper_wick >= 0.30 and rs_pct <= 0.10,
        "early_failed_or_breakout_trap_short",
        4.0,
    )
    add(
        "E_FAILED_OR_BREAKDOWN_TRAP_LONG",
        "LONG",
        early_vol >= 1.0 and regime != "BEAR" and low < or_low and close > or_low and (above_vwap or close_loc >= 0.65) and lower_wick >= 0.30 and rs_pct >= -0.10,
        "early_failed_or_breakdown_trap_long",
        4.0,
    )
    return out


def scan_ticker_signal_candle(
    ticker: str,
    slot_ist: Any,
    market_ctx: Dict[str, Dict[str, Any]],
    *,
    telemetry: Optional[Dict[str, Any]] = None,
) -> List[Tuple["v2.Candidate", Dict[str, Any]]]:
    ticker_started = time.perf_counter()

    def finish(
        result: List[Tuple["v2.Candidate", Dict[str, Any]]],
    ) -> List[Tuple["v2.Candidate", Dict[str, Any]]]:
        _bump_telemetry(
            telemetry,
            "ticker_elapsed_seconds",
            time.perf_counter() - ticker_started,
        )
        return result

    df, cache_entry = _load_live_5m_cached(ticker, telemetry)
    if df is None or df.empty:
        return finish([])

    slot_ts = _ensure_ist_ts(slot_ist).floor("min")
    df = df[df["date"] <= slot_ts].copy()
    if df.empty:
        return finish([])

    # Keep a separate causal history for the BB10 detector. The native v2 scan
    # must retain its fast three-day preparation window.
    late_bb10_df = None
    if late_bb10.SETUP in ALLOWED_SETUPS:
        late_bb10_start = (slot_ts - pd.Timedelta(days=45)).normalize()
        late_bb10_df = df[df["date"] >= late_bb10_start].copy()
    hm_fno_df = None
    if HM_FNO_SETUP in ALLOWED_SETUPS:
        hm_fno_start = (slot_ts - pd.Timedelta(days=max(1, HM_FNO_SIGNAL_WARMUP_DAYS))).normalize()
        hm_fno_df = df[df["date"] >= hm_fno_start].copy()
    _trim_start = (slot_ts - pd.Timedelta(days=3)).normalize()
    df = df[df["date"] >= _trim_start].copy()
    if df.empty:
        return finish([])

    prepared_key: Optional[Tuple[Any, ...]] = None
    if cache_entry is not None:
        prepared_key = (
            int(_trim_start.value),
            int(pd.Timestamp(df["date"].iloc[-1]).value),
            int(len(df)),
            bool(getattr(v2, "ENABLE_HILEGA_MILEGA_RESEARCH", False)),
        )
    if (
        prepared_key is not None
        and cache_entry is not None
        and cache_entry.prepared_key == prepared_key
        and cache_entry.prepared is not None
    ):
        prepared = cache_entry.prepared
        _bump_telemetry(telemetry, "prepared_cache_hits")
    else:
        _bump_telemetry(telemetry, "prepared_cache_misses")
        prepare_started = time.perf_counter()
        try:
            prepared = v2._prepare_5m(df)
        except Exception:
            _bump_telemetry(telemetry, "ticker_errors")
            _bump_telemetry(
                telemetry,
                "prepare_seconds",
                time.perf_counter() - prepare_started,
            )
            return finish([])
        _bump_telemetry(
            telemetry,
            "prepare_seconds",
            time.perf_counter() - prepare_started,
        )
        if cache_entry is not None and prepared_key is not None:
            cache_entry.prepared_key = prepared_key
            cache_entry.prepared = prepared
    if "date_only" not in prepared.columns:
        prepared["date_only"] = prepared["date"].dt.tz_convert(IST_TZ).dt.date

    day_df = prepared[prepared["date_only"] == slot_ts.date()].copy().reset_index(drop=True)
    if day_df.empty:
        return finish([])
    day_df["date"] = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(day_df["date"].dt, "tz", None) is None:
        day_df["date"] = day_df["date"].dt.tz_localize(IST_TZ)
    else:
        day_df["date"] = day_df["date"].dt.tz_convert(IST_TZ)

    signal_rows = day_df[day_df["date"].dt.floor("min") == slot_ts]
    if signal_rows.empty:
        return finish([])

    scan_df = _append_synthetic_successor(day_df, slot_ts)
    strategy_started = time.perf_counter()
    try:
        candidates = v2._scan_day(scan_df, str(ticker).upper(), market_ctx)
    except Exception:
        _bump_telemetry(telemetry, "ticker_errors")
        candidates = []
    candidates = list(candidates or [])
    try:
        candidates.extend(_scan_early_slot_candidates(scan_df, str(ticker).upper(), slot_ts, market_ctx))
    except Exception:
        _bump_telemetry(telemetry, "ticker_errors")

    signal_row = signal_rows.iloc[-1].to_dict()
    out: List[Tuple["v2.Candidate", Dict[str, Any]]] = []
    for c in candidates:
        c_ts = _ensure_ist_ts(c.signal_ts).floor("min")
        if c_ts != slot_ts:
            continue
        if str(c.setup) in EXCLUDED_SETUPS:
            continue
        if FILTER_TO_V8_EXIT_SETUPS and str(c.setup) not in ALLOWED_SETUPS:
            continue
        out.append((c, signal_row))
    if late_bb10.SETUP in ALLOWED_SETUPS:
        custom_row = _scan_late_bb10_signal(
            late_bb10_df if late_bb10_df is not None else df,
            str(ticker).upper(),
            slot_ts,
        )
        if custom_row is not None:
            out.append(custom_row)
    if HM_FNO_SETUP in ALLOWED_SETUPS:
        custom_row = _scan_hm_fno_short_signal(
            hm_fno_df if hm_fno_df is not None else df,
            str(ticker).upper(),
            slot_ts,
        )
        if custom_row is not None:
            out.append(custom_row)
    _bump_telemetry(
        telemetry,
        "strategy_seconds",
        time.perf_counter() - strategy_started,
    )
    return finish(out)


def _fmt_ist(ts: Any) -> str:
    t = _ensure_ist_ts(ts)
    offset = t.strftime("%z")
    return f"{t.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _finite_or_blank(x: Any) -> Any:
    try:
        v = float(x)
    except Exception:
        return ""
    return v if np.isfinite(v) else ""


def _research_shadow_metadata(side: str, setup: str, close_loc: Any, vwap_dist_atr: Any) -> Dict[str, str]:
    side_u = str(side).upper().strip()
    setup_u = str(setup).upper().strip()
    reasons: List[str] = []
    actions: List[str] = []
    status = ""
    try:
        close_loc_f = float(close_loc)
    except Exception:
        close_loc_f = np.nan
    try:
        vwap_dist_f = float(vwap_dist_atr)
    except Exception:
        vwap_dist_f = np.nan

    if setup_u in RESEARCH_PROBATION_SETUPS:
        status = "PROBATION"
        reasons.append("weak_setup_from_v7_live_research")
        actions.append("scanner_shadow_only_no_block")

    if (
        side_u == "LONG"
        and np.isfinite(close_loc_f)
        and np.isfinite(vwap_dist_f)
        and close_loc_f > RESEARCH_ANTI_CHASE_LONG_CLOSE_LOC_MIN
        and vwap_dist_f > RESEARCH_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN
    ):
        status = status or "PAPER_EXPERIMENT"
        reasons.append(
            f"anti_chase_long close_loc>{RESEARCH_ANTI_CHASE_LONG_CLOSE_LOC_MIN:.2f} "
            f"vwap_dist_atr>{RESEARCH_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN:.2f}"
        )
        actions.append("paper_gate_active_scanner_shadow_only")

    return {
        "research_shadow_status": status,
        "research_shadow_reason": ";".join(reasons),
        "research_shadow_action": ";".join(actions),
        "research_shadow_version": RESEARCH_SHADOW_VERSION if reasons else "",
    }


def candidates_to_dataframe(
    rows_in: Iterable[Tuple["v2.Candidate", Dict[str, Any]]],
    scan_slot_ist: Any,
    *,
    dedupe: bool = True,
) -> pd.DataFrame:
    scan_slot = _ensure_ist_ts(scan_slot_ist)
    created_at = pd.Timestamp.now(tz=IST_TZ)
    rows: List[Dict[str, Any]] = []
    for c, signal_row in rows_in:
        signal_ts = _ensure_ist_ts(c.signal_ts)
        ticker = str(c.ticker).upper().strip()
        side = str(c.side).upper().strip()
        setup = str(c.setup)
        selection_mode = str(
            signal_row.get(
                "selection_mode",
                EARLY_SELECTION_MODE if setup.startswith("E_") else SELECTION_MODE,
            )
        )
        candidate_family = str(
            signal_row.get(
                "candidate_family",
                "EARLY" if setup.startswith("E_") else "V7_STANDARD",
            )
        )
        signal_time = _fmt_ist(signal_ts)
        candidate_id = f"{ticker}|{side}|{setup}|{signal_time}"
        avwap_value = _finite_or_blank(signal_row.get("AVWAP", getattr(c, "avwap", np.nan)))
        avwap_dist_atr = _finite_or_blank(
            signal_row.get("avwap_dist_atr", getattr(c, "avwap_dist_atr", np.nan))
        )
        setup_distance = avwap_dist_atr if "AVWAP" in setup.upper() else c.vwap_dist_atr
        diag = {
            "reason": str(c.reason),
            "day_value_so_far_rs": _finite_or_blank(c.day_value_so_far_rs),
            "market_ret_pct": _finite_or_blank(c.market_ret_pct),
            "rs_pct": _finite_or_blank(c.rs_pct),
            "regime": str(c.regime),
            "avwap": avwap_value,
            "avwap_dist_atr": avwap_dist_atr,
            "signal_timeframe_min": _finite_or_blank(signal_row.get("signal_timeframe_min")),
            "entry_lag_min": _finite_or_blank(signal_row.get("entry_lag_min")),
            "hm_signal_bar_time_ist": str(signal_row.get("hm_signal_bar_time_ist", "")),
            "hm_signal_stop_price": _finite_or_blank(signal_row.get("hm_signal_stop_price")),
            "hm_risk_reward": _finite_or_blank(signal_row.get("hm_risk_reward")),
            "hm_min_risk_pct": _finite_or_blank(signal_row.get("hm_min_risk_pct")),
            "hm_max_risk_pct": _finite_or_blank(signal_row.get("hm_max_risk_pct")),
            "hm_rsi_9": _finite_or_blank(signal_row.get("hm_rsi_9")),
            "hm_rsi_ema_3": _finite_or_blank(signal_row.get("hm_rsi_ema_3")),
            "hm_rsi_wma_21": _finite_or_blank(signal_row.get("hm_rsi_wma_21")),
            "hm_line_distance": _finite_or_blank(signal_row.get("hm_line_distance")),
            "hm_exit_model": str(signal_row.get("hm_exit_model", "")),
        }
        rows.append({
            "candidate_id": candidate_id,
            "scan_session": "Signal discovery v7 5mins ID",
            "selection_mode": selection_mode,
            "candidate_family": candidate_family,
            "scan_slot_ist": _fmt_ist(scan_slot),
            "signal_time_ist": signal_time,
            "signal_timestamp_convention": str(
                signal_row.get("signal_timestamp_convention", "end_labeled_completed_5m")
            ),
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "signal_open": _finite_or_blank(signal_row.get("open")),
            "signal_high": _finite_or_blank(signal_row.get("high")),
            "signal_low": _finite_or_blank(signal_row.get("low")),
            "signal_close": _finite_or_blank(c.signal_close),
            "signal_volume": _finite_or_blank(signal_row.get("volume")),
            "signal_adx": _finite_or_blank(signal_row.get("ADX")),
            "signal_rsi": _finite_or_blank(signal_row.get("RSI")),
            "signal_vol_ratio20": _finite_or_blank(
                signal_row.get("vol_ratio", c.vol_ratio)
            ),
            "quality_score": _finite_or_blank(c.quality_score),
            "rs_pct": _finite_or_blank(c.rs_pct),
            "market_ret_pct": _finite_or_blank(c.market_ret_pct),
            "regime": str(c.regime),
            "vol_ratio": _finite_or_blank(c.vol_ratio),
            "atr_pct": _finite_or_blank(c.atr_pct),
            "body_pct": _finite_or_blank(c.body_pct),
            "close_loc": _finite_or_blank(c.close_loc),
            "vwap_dist_atr": _finite_or_blank(c.vwap_dist_atr),
            "avwap": avwap_value,
            "avwap_dist_atr": avwap_dist_atr,
            "reason": str(c.reason),
            "status": "CANDIDATE",
            "created_at_ist": _fmt_ist(created_at),
            "diagnostics_json": json.dumps(diag, default=str),
            "confirmation_score": _finite_or_blank(signal_row.get("confirmation_score")),
            "breakout_level": _finite_or_blank(signal_row.get("breakout_level")),
            "entry_trigger_price": _finite_or_blank(signal_row.get("entry_trigger_price")),
            "entry_cancel_price": _finite_or_blank(signal_row.get("entry_cancel_price")),
            "entry_valid_minutes": _finite_or_blank(signal_row.get("entry_valid_minutes")),
            "entry_max_gap_pct": _finite_or_blank(signal_row.get("entry_max_gap_pct")),
            "market_breadth": _finite_or_blank(signal_row.get("market_breadth")),
            "nifty_ema_up": _finite_or_blank(signal_row.get("nifty_ema_up")),
            "signal_timeframe_min": _finite_or_blank(signal_row.get("signal_timeframe_min")),
            "entry_lag_min": _finite_or_blank(signal_row.get("entry_lag_min")),
            "hm_signal_bar_time_ist": str(signal_row.get("hm_signal_bar_time_ist", "")),
            "hm_signal_stop_price": _finite_or_blank(signal_row.get("hm_signal_stop_price")),
            "hm_risk_reward": _finite_or_blank(signal_row.get("hm_risk_reward")),
            "hm_min_risk_pct": _finite_or_blank(signal_row.get("hm_min_risk_pct")),
            "hm_max_risk_pct": _finite_or_blank(signal_row.get("hm_max_risk_pct")),
            "hm_rsi_9": _finite_or_blank(signal_row.get("hm_rsi_9")),
            "hm_rsi_ema_3": _finite_or_blank(signal_row.get("hm_rsi_ema_3")),
            "hm_rsi_wma_21": _finite_or_blank(signal_row.get("hm_rsi_wma_21")),
            "hm_line_distance": _finite_or_blank(signal_row.get("hm_line_distance")),
            "hm_exit_model": str(signal_row.get("hm_exit_model", "")),
            **_research_shadow_metadata(side, setup, c.close_loc, setup_distance),
        })
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    if dedupe:
        return _dedupe_candidate_frame(out)
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    return (
        out.sort_values(["signal_time_ist", "ticker", "quality_score", "setup"], ascending=[True, True, False, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .reset_index(drop=True)
    )


def _dedupe_candidate_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the single best signal candidate per ticker per signal candle.

    Multiple setup labels can fire on the same 5-minute candle for the same
    ticker. Candidate discovery is signal-only, so downstream entry should see
    only the strongest ticker candidate, not one row per setup label.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["quality_score"] = pd.to_numeric(out.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["signal_time_ist"] = out["signal_time_ist"].astype(str)
    out = (
        out.sort_values(["quality_score", "ticker", "setup"], ascending=[False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )
    return out


_MARKET_CTX_CACHE: Dict[str, Dict[str, Any]] = {}


def build_market_context_once() -> Dict[str, Dict[str, Any]]:  # noqa: F811
    global _MARKET_CTX_CACHE
    if not _MARKET_CTX_CACHE:
        v2.DATA_ROOT_5M = LIVE_5M_DIR
        v2._init_worker({
            "ENABLE_NOISY_ADVANCED_SHORTS": True,
            "ENABLE_NATIVE_V2_MINED_FILTER": False,
        })
        _MARKET_CTX_CACHE = v2._load_market_context()
    return _MARKET_CTX_CACHE


_WORKER_MARKET_CTX: Optional[Dict[str, Dict[str, Any]]] = None
_WORKER_LAST_SLOT_ISO: Optional[str] = None

# Persistent ProcessPool — reused across slots to avoid Windows spawn overhead (~5-15s/slot).
# Recreated at day change so workers get fresh market context for the new trading day.
_SCAN_POOL: Optional[ProcessPoolExecutor] = None
_SCAN_POOL_WORKERS: int = 0
_SCAN_POOL_DAY: Optional[str] = None


def shutdown_scan_pool(*, wait: bool = True) -> None:
    global _SCAN_POOL, _SCAN_POOL_WORKERS, _SCAN_POOL_DAY
    pool = _SCAN_POOL
    _SCAN_POOL = None
    _SCAN_POOL_WORKERS = 0
    _SCAN_POOL_DAY = None
    if pool is not None:
        try:
            pool.shutdown(wait=wait, cancel_futures=True)
        except Exception:
            pass


def reset_scan_caches(*, shutdown_pool_workers: bool = True) -> None:
    """Clear parent and (by default) process-local scanner caches."""
    global _LAST_SCAN_TELEMETRY, _MARKET_CTX_CACHE
    _LOCAL_FRAME_CACHE.clear()
    _MARKET_CTX_CACHE = {}
    _LAST_SCAN_TELEMETRY = {}
    if shutdown_pool_workers:
        shutdown_scan_pool(wait=True)


def get_last_scan_telemetry() -> Dict[str, Any]:
    return dict(_LAST_SCAN_TELEMETRY)


def _replace_scan_pool(workers: int, today_str: str) -> None:
    global _SCAN_POOL, _SCAN_POOL_WORKERS, _SCAN_POOL_DAY
    shutdown_scan_pool(wait=False)
    _SCAN_POOL = ProcessPoolExecutor(max_workers=workers, initializer=_worker_init)
    _SCAN_POOL_WORKERS = workers
    _SCAN_POOL_DAY = today_str


def _get_scan_pool(workers: int, today_str: str) -> ProcessPoolExecutor:
    global _SCAN_POOL, _SCAN_POOL_WORKERS, _SCAN_POOL_DAY
    if _SCAN_POOL is None or _SCAN_POOL_WORKERS != workers or _SCAN_POOL_DAY != today_str:
        _replace_scan_pool(workers, today_str)
    return _SCAN_POOL


def _worker_init() -> None:
    global _WORKER_MARKET_CTX, _WORKER_LAST_SLOT_ISO
    v2.DATA_ROOT_5M = LIVE_5M_DIR
    v2._init_worker({
        "ENABLE_NOISY_ADVANCED_SHORTS": True,
        "ENABLE_NATIVE_V2_MINED_FILTER": False,
    })
    # First task (or explicit prewarm) performs one slot-aware context read,
    # avoiding the previous initializer + first-task duplicate.
    _WORKER_MARKET_CTX = {}
    _WORKER_LAST_SLOT_ISO = None
    _LOCAL_FRAME_CACHE.clear()


def _refresh_worker_market_context(slot_iso: str) -> None:
    global _WORKER_MARKET_CTX, _WORKER_LAST_SLOT_ISO
    if _WORKER_MARKET_CTX is None:
        _worker_init()
    if _WORKER_LAST_SLOT_ISO == slot_iso:
        return
    try:
        _WORKER_MARKET_CTX = v2._load_market_context()
    except Exception:
        if _WORKER_MARKET_CTX is None:
            _WORKER_MARKET_CTX = {}
    _WORKER_LAST_SLOT_ISO = slot_iso


def _worker_prewarm(slot_iso: str) -> int:
    _refresh_worker_market_context(slot_iso)
    return os.getpid()


def prewarm_scan_pool(
    slot_ist: Any,
    max_workers: Optional[int] = None,
) -> Dict[str, Any]:
    """Eagerly start scanner processes outside the decision-critical slot."""
    slot_ts = _ensure_ist_ts(slot_ist).floor("min")
    workers = int(max_workers if max_workers is not None else DEFAULT_SCAN_WORKERS)
    started = time.perf_counter()
    if workers <= 1:
        build_market_context_once()
        return {
            "workers_requested": workers,
            "worker_pids_seen": 1,
            "seconds": time.perf_counter() - started,
        }
    pool = _get_scan_pool(workers, slot_ts.date().isoformat())
    pids = list(
        pool.map(
            _worker_prewarm,
            [slot_ts.isoformat()] * (workers * 2),
            chunksize=1,
        )
    )
    result = {
        "workers_requested": workers,
        "worker_pids_seen": len(set(pids)),
        "seconds": time.perf_counter() - started,
    }
    print(
        "[candidate_scan] prewarm "
        f"requested={workers} pids_seen={result['worker_pids_seen']} "
        f"total={result['seconds']:.3f}s",
        flush=True,
    )
    return result


def _worker_scan(
    payload: Tuple[str, str] | Tuple[str, str, bool],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if len(payload) >= 3:
        ticker, slot_iso, dedupe = payload
    else:
        ticker, slot_iso = payload
        dedupe = True
    telemetry = _new_ticker_telemetry()
    _refresh_worker_market_context(slot_iso)
    try:
        out = scan_ticker_signal_candle(
            ticker,
            pd.Timestamp(slot_iso),
            _WORKER_MARKET_CTX or {},
            telemetry=telemetry,
        )
    except Exception:
        telemetry["ticker_errors"] += 1
        return [], telemetry
    if not out:
        return [], telemetry
    rows = candidates_to_dataframe(
        out,
        pd.Timestamp(slot_iso),
        dedupe=bool(dedupe),
    ).to_dict("records")
    return rows, telemetry


_SUM_TELEMETRY_KEYS = (
    "raw_cache_hits",
    "raw_cache_misses",
    "prepared_cache_hits",
    "prepared_cache_misses",
    "unchanged_frame_hits",
    "file_read_seconds",
    "prepare_seconds",
    "strategy_seconds",
    "unstable_file_reads",
    "ticker_errors",
)


def _summarize_slot_telemetry(
    ticker_telemetry: List[Dict[str, Any]],
    *,
    slot_ts: pd.Timestamp,
    ticker_count: int,
    workers: int,
    chunksize: int,
    pool_get_seconds: float,
    scan_wall_seconds: float,
    total_wall_seconds: float,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "slot_ist": slot_ts.isoformat(),
        "ticker_count": ticker_count,
        "workers": workers,
        "chunksize": chunksize,
        "pool_get_seconds": pool_get_seconds,
        "scan_wall_seconds": scan_wall_seconds,
        "total_wall_seconds": total_wall_seconds,
    }
    for key in _SUM_TELEMETRY_KEYS:
        summary[key] = sum(float(item.get(key, 0)) for item in ticker_telemetry)
        if key.endswith(("hits", "misses", "reads", "errors")):
            summary[key] = int(summary[key])
    elapsed = [
        float(item.get("ticker_elapsed_seconds", 0.0))
        for item in ticker_telemetry
        if float(item.get("ticker_elapsed_seconds", 0.0)) >= 0.0
    ]
    summary["ticker_p50_seconds"] = float(np.percentile(elapsed, 50)) if elapsed else 0.0
    summary["ticker_p95_seconds"] = float(np.percentile(elapsed, 95)) if elapsed else 0.0
    summary["ticker_max_seconds"] = max(elapsed, default=0.0)
    raw_lookups = summary["raw_cache_hits"] + summary["raw_cache_misses"]
    prepared_lookups = summary["prepared_cache_hits"] + summary["prepared_cache_misses"]
    summary["raw_cache_hit_rate"] = (
        summary["raw_cache_hits"] / raw_lookups if raw_lookups else 0.0
    )
    summary["prepared_cache_hit_rate"] = (
        summary["prepared_cache_hits"] / prepared_lookups if prepared_lookups else 0.0
    )
    return summary


def scan_slot_candidates(
    slot_ist: Any,
    tickers: Iterable[str],
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
    max_workers: Optional[int] = None,
    *,
    dedupe: bool = True,
    chunksize: Optional[int] = None,
) -> pd.DataFrame:
    global _LAST_SCAN_TELEMETRY
    overall_started = time.perf_counter()
    slot_ts = _ensure_ist_ts(slot_ist)
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]
    workers = int(max_workers if max_workers is not None else DEFAULT_SCAN_WORKERS)
    effective_chunksize = max(
        1,
        int(chunksize if chunksize is not None else DEFAULT_SCAN_CHUNKSIZE),
    )
    rows: List[Dict[str, Any]] = []
    ticker_telemetry: List[Dict[str, Any]] = []
    pool_get_seconds = 0.0
    scan_started = time.perf_counter()

    if workers <= 1:
        if market_ctx is None:
            market_ctx = build_market_context_once()
        for ticker in tickers:
            one_telemetry = _new_ticker_telemetry()
            try:
                found = scan_ticker_signal_candle(
                    ticker,
                    slot_ts,
                    market_ctx,
                    telemetry=one_telemetry,
                )
            except Exception:
                one_telemetry["ticker_errors"] += 1
                found = []
            ticker_telemetry.append(one_telemetry)
            if found:
                rows.extend(candidates_to_dataframe(found, slot_ts, dedupe=dedupe).to_dict("records"))
    else:
        slot_iso = slot_ts.isoformat()
        payloads = [(ticker, slot_iso, bool(dedupe)) for ticker in tickers]
        today_str = slot_ts.date().isoformat()
        _t0 = time.perf_counter()
        pool = _get_scan_pool(workers, today_str)
        _t1 = time.perf_counter()
        pool_get_seconds = _t1 - _t0
        scan_started = _t1
        try:
            for result, one_telemetry in pool.map(
                _worker_scan,
                payloads,
                chunksize=effective_chunksize,
            ):
                ticker_telemetry.append(one_telemetry)
                if result:
                    rows.extend(result)
        except Exception as exc:
            print(
                f"[candidate_scan] pool error ({type(exc).__name__}); sequential fallback this slot",
                flush=True,
            )
            _replace_scan_pool(workers, today_str)
            rows = []
            ticker_telemetry = []
            if market_ctx is None:
                market_ctx = build_market_context_once()
            for ticker in tickers:
                one_telemetry = _new_ticker_telemetry()
                try:
                    found = scan_ticker_signal_candle(
                        ticker,
                        slot_ts,
                        market_ctx,
                        telemetry=one_telemetry,
                    )
                except Exception:
                    one_telemetry["ticker_errors"] += 1
                    found = []
                ticker_telemetry.append(one_telemetry)
                if found:
                    rows.extend(candidates_to_dataframe(found, slot_ts, dedupe=dedupe).to_dict("records"))

    finished = time.perf_counter()
    _LAST_SCAN_TELEMETRY = _summarize_slot_telemetry(
        ticker_telemetry,
        slot_ts=slot_ts,
        ticker_count=len(tickers),
        workers=workers,
        chunksize=effective_chunksize,
        pool_get_seconds=pool_get_seconds,
        scan_wall_seconds=finished - scan_started,
        total_wall_seconds=finished - overall_started,
    )
    print(
        "[candidate_scan] "
        f"n={len(tickers)} workers={workers} chunksize={effective_chunksize} "
        f"pool_get={_LAST_SCAN_TELEMETRY['pool_get_seconds']:.3f}s "
        f"scan={_LAST_SCAN_TELEMETRY['scan_wall_seconds']:.3f}s "
        f"total={_LAST_SCAN_TELEMETRY['total_wall_seconds']:.3f}s "
        f"raw_cache={_LAST_SCAN_TELEMETRY['raw_cache_hits']}/"
        f"{_LAST_SCAN_TELEMETRY['raw_cache_misses']} "
        f"unchanged_frames={_LAST_SCAN_TELEMETRY['unchanged_frame_hits']} "
        f"prepared_cache={_LAST_SCAN_TELEMETRY['prepared_cache_hits']}/"
        f"{_LAST_SCAN_TELEMETRY['prepared_cache_misses']} "
        f"io_cpu={_LAST_SCAN_TELEMETRY['file_read_seconds']:.3f}s "
        f"prepare_cpu={_LAST_SCAN_TELEMETRY['prepare_seconds']:.3f}s "
        f"strategy_cpu={_LAST_SCAN_TELEMETRY['strategy_seconds']:.3f}s "
        f"ticker_p95={_LAST_SCAN_TELEMETRY['ticker_p95_seconds']:.3f}s "
        f"ticker_max={_LAST_SCAN_TELEMETRY['ticker_max_seconds']:.3f}s "
        f"errors={_LAST_SCAN_TELEMETRY['ticker_errors']}",
        flush=True,
    )

    if not rows:
        return pd.DataFrame()
    raw_frame = pd.DataFrame(rows)
    custom_mask = raw_frame.get("setup", pd.Series("", index=raw_frame.index)).astype(str).eq(late_bb10.SETUP)
    if custom_mask.any():
        alignment = late_bb10.market_alignment_for_slots(tickers, [slot_ts], _load_live_5m)
        values = alignment.get(slot_ts.floor("min").isoformat(), {})
        for key in ("market_breadth", "nifty_ema_up"):
            raw_frame.loc[custom_mask, key] = values.get(key, np.nan)
    df = _dedupe_candidate_frame(raw_frame) if dedupe else raw_frame.drop_duplicates(subset=["candidate_id"], keep="first")
    if df.empty:
        return pd.DataFrame()
    return df.sort_values(["quality_score", "ticker"], ascending=[False, True]).reset_index(drop=True)
