"""
Signal-only candidate scanner for "Signal discovery v7 5mins ID".

This module scans the completed 5-minute signal candle and returns candidate
tickers only. It deliberately does not emit entry_ts, entry_price, SL, target,
or trade signal CSV rows. Entry is a separate 1-minute module.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v6_backtesting as v6
import avwap_5min_ID_v7_backtesting as v7


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


def _load_live_5m(ticker: str) -> Optional[pd.DataFrame]:
    fp = LIVE_5M_DIR / f"{str(ticker).upper()}_stocks_indicators_5min.parquet"
    df = _read_one(fp)
    if df is None or "date" not in df.columns:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None
    return (
        df.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )


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


def scan_ticker_signal_candle(
    ticker: str,
    slot_ist: Any,
    market_ctx: Dict[str, Dict[str, Any]],
) -> List[Tuple["v2.Candidate", Dict[str, Any]]]:
    df = _load_live_5m(ticker)
    if df is None or df.empty:
        return []

    slot_ts = _ensure_ist_ts(slot_ist).floor("min")
    df = df[df["date"] <= slot_ts].copy()
    if df.empty:
        return []

    try:
        prepared = v2._prepare_5m(df)
    except Exception:
        return []
    if "date_only" not in prepared.columns:
        prepared["date_only"] = prepared["date"].dt.tz_convert(IST_TZ).dt.date

    day_df = prepared[prepared["date_only"] == slot_ts.date()].copy().reset_index(drop=True)
    if day_df.empty:
        return []
    day_df["date"] = pd.to_datetime(day_df["date"], errors="coerce")
    if getattr(day_df["date"].dt, "tz", None) is None:
        day_df["date"] = day_df["date"].dt.tz_localize(IST_TZ)
    else:
        day_df["date"] = day_df["date"].dt.tz_convert(IST_TZ)

    signal_rows = day_df[day_df["date"].dt.floor("min") == slot_ts]
    if signal_rows.empty:
        return []

    scan_df = _append_synthetic_successor(day_df, slot_ts)
    try:
        candidates = v2._scan_day(scan_df, str(ticker).upper(), market_ctx)
    except Exception:
        return []

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
    return out


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


def candidates_to_dataframe(rows_in: Iterable[Tuple["v2.Candidate", Dict[str, Any]]], scan_slot_ist: Any) -> pd.DataFrame:
    scan_slot = _ensure_ist_ts(scan_slot_ist)
    created_at = pd.Timestamp.now(tz=IST_TZ)
    rows: List[Dict[str, Any]] = []
    for c, signal_row in rows_in:
        signal_ts = _ensure_ist_ts(c.signal_ts)
        ticker = str(c.ticker).upper().strip()
        side = str(c.side).upper().strip()
        setup = str(c.setup)
        signal_time = _fmt_ist(signal_ts)
        candidate_id = f"{ticker}|{side}|{setup}|{signal_time}"
        diag = {
            "reason": str(c.reason),
            "day_value_so_far_rs": _finite_or_blank(c.day_value_so_far_rs),
            "market_ret_pct": _finite_or_blank(c.market_ret_pct),
            "rs_pct": _finite_or_blank(c.rs_pct),
            "regime": str(c.regime),
        }
        rows.append({
            "candidate_id": candidate_id,
            "scan_session": "Signal discovery v7 5mins ID",
            "selection_mode": SELECTION_MODE,
            "scan_slot_ist": _fmt_ist(scan_slot),
            "signal_time_ist": signal_time,
            "ticker": ticker,
            "side": side,
            "setup": setup,
            "signal_open": _finite_or_blank(signal_row.get("open")),
            "signal_high": _finite_or_blank(signal_row.get("high")),
            "signal_low": _finite_or_blank(signal_row.get("low")),
            "signal_close": _finite_or_blank(c.signal_close),
            "signal_volume": _finite_or_blank(signal_row.get("volume")),
            "quality_score": _finite_or_blank(c.quality_score),
            "rs_pct": _finite_or_blank(c.rs_pct),
            "market_ret_pct": _finite_or_blank(c.market_ret_pct),
            "regime": str(c.regime),
            "vol_ratio": _finite_or_blank(c.vol_ratio),
            "atr_pct": _finite_or_blank(c.atr_pct),
            "body_pct": _finite_or_blank(c.body_pct),
            "close_loc": _finite_or_blank(c.close_loc),
            "vwap_dist_atr": _finite_or_blank(c.vwap_dist_atr),
            "reason": str(c.reason),
            "status": "CANDIDATE",
            "created_at_ist": _fmt_ist(created_at),
            "diagnostics_json": json.dumps(diag, default=str),
        })
    if not rows:
        return pd.DataFrame()
    return _dedupe_candidate_frame(pd.DataFrame(rows))


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


def build_market_context_once() -> Dict[str, Dict[str, Any]]:
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


def _worker_init() -> None:
    global _WORKER_MARKET_CTX
    v2.DATA_ROOT_5M = LIVE_5M_DIR
    v2._init_worker({
        "ENABLE_NOISY_ADVANCED_SHORTS": True,
        "ENABLE_NATIVE_V2_MINED_FILTER": False,
    })
    try:
        _WORKER_MARKET_CTX = v2._load_market_context()
    except Exception:
        _WORKER_MARKET_CTX = {}


def _worker_scan(payload: Tuple[str, str]) -> List[Dict[str, Any]]:
    ticker, slot_iso = payload
    global _WORKER_MARKET_CTX
    if _WORKER_MARKET_CTX is None:
        _worker_init()
    try:
        out = scan_ticker_signal_candle(ticker, pd.Timestamp(slot_iso), _WORKER_MARKET_CTX or {})
    except Exception:
        return []
    if not out:
        return []
    return candidates_to_dataframe(out, pd.Timestamp(slot_iso)).to_dict("records")


def scan_slot_candidates(
    slot_ist: Any,
    tickers: Iterable[str],
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
    max_workers: Optional[int] = None,
) -> pd.DataFrame:
    slot_ts = _ensure_ist_ts(slot_ist)
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]
    workers = int(max_workers if max_workers is not None else DEFAULT_SCAN_WORKERS)
    rows: List[Dict[str, Any]] = []

    if workers <= 1:
        if market_ctx is None:
            market_ctx = build_market_context_once()
        for ticker in tickers:
            try:
                found = scan_ticker_signal_candle(ticker, slot_ts, market_ctx)
            except Exception:
                found = []
            if found:
                rows.extend(candidates_to_dataframe(found, slot_ts).to_dict("records"))
    else:
        slot_iso = slot_ts.isoformat()
        payloads = [(ticker, slot_iso) for ticker in tickers]
        with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init) as ex:
            for result in ex.map(_worker_scan, payloads, chunksize=24):
                if result:
                    rows.extend(result)

    if not rows:
        return pd.DataFrame()
    df = _dedupe_candidate_frame(pd.DataFrame(rows))
    if df.empty:
        return pd.DataFrame()
    return df.sort_values(["quality_score", "ticker"], ascending=[False, True]).reset_index(drop=True)
