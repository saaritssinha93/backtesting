"""Read-only, timestamp-exact cash-price / near-month futures-OI reporting.

This module intentionally does not import the strategy runtime or any broker
client. It reconstructs a market view, never signals or orders. All timestamps
are candle ends in Asia/Kolkata; a reconstructed 09:15 open is labelled as such.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
import hashlib
import math
import re
import threading
import time as clock
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

IST = ZoneInfo("Asia/Kolkata")
INDEX_UNDERLYINGS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYFPI"}
SLOTS = tuple((datetime(2000, 1, 1, 9, 15) + timedelta(minutes=5 * n)).strftime("%H:%M") for n in range(76))
_CATALOG: dict[str, tuple[float, list[str]]] = {}
_CATALOG_LOCK = threading.Lock()


def _session_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
        raise ValueError("Session date must use YYYY-MM-DD")
    return date.fromisoformat(value)


def _number(value: Any, *, positive: bool = False) -> float | None:
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) and (not positive or number > 0) else None


def _pct(value: Any, baseline: Any) -> float | None:
    a, b = _number(value), _number(baseline, positive=True)
    return (a / b - 1) * 100 if a is not None and b is not None else None


def _contract_stem(contract: str) -> str:
    # Same persisted filename convention as fno_oi_common.safe_contract_stem,
    # without importing the writer/runtime module and creating directories.
    stem = re.sub(r"[^A-Z0-9._-]+", "_", contract).strip("._") or "CONTRACT"
    return f"{stem}_{hashlib.sha1(contract.encode('utf-8')).hexdigest()[:8]}" if stem != contract else stem


def _stamp(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    return parsed.dt.tz_localize(IST) if parsed.dt.tz is None else parsed.dt.tz_convert(IST)


def _flag(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").fillna(0).ne(0) | values.astype(str).str.lower().isin({"true", "yes", "on"})


@lru_cache(maxsize=1536)
def _read_cached(path: str, modified: int, size: int, start: str, end: str) -> pd.DataFrame:
    """Cache by file identity and requested date range, including atomic rewrites."""
    schema = pq.ParquetFile(path).schema_arrow
    names = set(schema.names)
    timestamp = "timestamp" if "timestamp" in names else "date"
    requested = {timestamp, "candle_start", "open", "high", "low", "close", "volume", "oi", "gap_filled", "opening_snapshot", "provisional_stale", "source_1m_count", "quality_state"}
    columns = sorted(requested & names)
    filters = None
    if timestamp in names and start:
        dtype = schema.field(timestamp).type
        if hasattr(dtype, "tz"):
            lower, upper = pd.Timestamp(start), pd.Timestamp(end)
            if dtype.tz:
                lower, upper = lower.tz_localize(IST), upper.tz_localize(IST)
            filters = [(timestamp, ">=", lower), (timestamp, "<=", upper)]
    return pd.read_parquet(path, columns=columns, filters=filters)


def _read_bars(path: Path, start: date, end: date) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    stat = path.stat()
    frame = _read_cached(str(path), stat.st_mtime_ns, stat.st_size, f"{start} 00:00:00", f"{end} 23:59:59").copy()
    key = "timestamp" if "timestamp" in frame else "date"
    if key not in frame:
        return pd.DataFrame()
    frame["ts"] = _stamp(frame[key])
    frame = frame.loc[frame.ts.notna() & frame.ts.dt.date.between(start, end)]
    return frame.drop_duplicates("ts", keep="last").sort_values("ts")


def list_available_dates(root: Path | str) -> list[str]:
    """Cheap cached calendar of locally evidenced dates; no recursive data scan."""
    root = Path(root)
    key = str(root.resolve())
    with _CATALOG_LOCK:
        cached = _CATALOG.get(key)
        if cached and clock.monotonic() - cached[0] < 30:
            return list(cached[1])
    days: set[str] = set()
    for p in (root / "fno_oi/universe").glob("near_month_*.parquet"):
        value = p.stem.removeprefix("near_month_")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            days.add(value)
    for directory in (root / "fno_oi/raw_equity_1m", root / "fno_oi/v13_v10_g_3_live/scanner_5m"):
        if directory.is_dir():
            days.update(p.name for p in directory.iterdir() if p.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name))
    # Old backtests precede the first saved daily universe. A small representative
    # contract sample discovers those sessions without reading 200 full histories.
    contracts = sorted((root / "fno_oi/raw_contracts_5m").glob("*_5minute.parquet"))
    for p in contracts[:6]:
        try:
            frame = pd.read_parquet(p, columns=["timestamp"])
            days.update(_stamp(frame.timestamp).dropna().dt.strftime("%Y-%m-%d"))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    result = sorted(days, reverse=True)
    with _CATALOG_LOCK:
        _CATALOG[key] = (clock.monotonic(), result)
    return list(result)


def _universe(root: Path, day: date) -> tuple[list[dict[str, str]], str]:
    path = root / "fno_oi/universe" / f"near_month_{day}.parquet"
    kind = "DATED_UNIVERSE"
    if path.is_file():
        frame = pd.read_parquet(path)
        if "master_date" in frame and set(pd.to_datetime(frame.master_date).dt.date.dropna()) != {day}:
            raise ValueError("Saved universe date does not match requested session")
    else:
        path = root / "fno_oi/universe/contract_registry.parquet"
        if not path.is_file():
            return [], "MISSING_UNIVERSE"
        frame = pd.read_parquet(path)
        frame["expiry"] = pd.to_datetime(frame["expiry"], errors="coerce")
        frame = frame.loc[frame.expiry.dt.date.ge(day)].sort_values("expiry").drop_duplicates("underlying", keep="first")
        kind = "RECONSTRUCTED_NEAR_MONTH_REGISTRY"
    result = []
    seen = set()
    for row in frame.to_dict("records"):
        underlying = str(row.get("underlying", "")).strip().upper()
        if underlying in INDEX_UNDERLYINGS or _flag(pd.Series([row.get("is_index_future", False)])).iloc[0]:
            continue
        symbol_value = row.get("equity_symbol")
        symbol = str(symbol_value).strip().upper() if pd.notna(symbol_value) and symbol_value else {"LTM": "LTIM"}.get(underlying, underlying)
        contract_value = row.get("futures_tradingsymbol")
        contract = str(contract_value if pd.notna(contract_value) and contract_value else row.get("tradingsymbol", "")).strip().upper()
        if not re.fullmatch(r"[A-Z0-9&_.-]+", symbol) or not re.fullmatch(r"[A-Z0-9&_.-]+", contract) or symbol in seen:
            continue
        seen.add(symbol)
        result.append({"symbol": symbol, "contract": contract})
    return sorted(result, key=lambda row: row["symbol"]), kind


def _cash_valid(frame: pd.DataFrame, *, minute: bool = False) -> pd.DataFrame:
    if frame.empty or not {"open", "close", "ts"} <= set(frame):
        return pd.DataFrame()
    valid = pd.to_numeric(frame.close, errors="coerce").gt(0) & pd.to_numeric(frame.open, errors="coerce").gt(0)
    for name in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if name in frame:
            valid &= ~_flag(frame[name])
    if not minute and "source_1m_count" in frame:
        count = pd.to_numeric(frame.source_1m_count, errors="coerce")
        valid &= count.isna() | count.eq(5)
    offset = (frame.ts - frame.ts.dt.normalize()).dt.total_seconds() / 60
    valid &= offset.between(556 if minute else 560, 930)
    if not minute:
        valid &= offset.mod(5).eq(0)
        if {"high", "low", "volume"} <= set(frame):
            completed = frame.loc[valid]
            previous = completed.shift(1)
            copied = completed.ts.sub(previous.ts).eq(pd.Timedelta(minutes=5))
            for column in ("open", "high", "low", "close", "volume"):
                copied &= pd.to_numeric(completed[column], errors="coerce").eq(pd.to_numeric(previous[column], errors="coerce"))
            if "source_1m_count" in completed:
                counts = pd.to_numeric(completed.source_1m_count, errors="coerce")
                copied &= ~(counts.eq(5) & counts.shift(1).eq(5))
            valid.loc[copied.index] &= ~copied
    return frame.loc[valid].copy()


def _aggregate_minutes(frame: pd.DataFrame) -> pd.DataFrame:
    minute = _cash_valid(frame, minute=True)
    if minute.empty:
        return pd.DataFrame()
    minute["end"] = minute.ts.dt.ceil("5min")
    aggregated = minute.groupby("end").agg(open=("open", "first"), close=("close", "last"), count=("ts", "size"), first=("ts", "first"), last=("ts", "last"))
    good = aggregated["count"].eq(5) & aggregated["last"].eq(aggregated.index) & aggregated["first"].eq(aggregated.index - pd.Timedelta(minutes=4))
    result = aggregated.loc[good, ["open", "close"]].copy()
    result["ts"] = result.index
    return result.reset_index(drop=True)


def _symbol_rows(root: Path, day: date, prior: date | None, now: pd.Timestamp, item: dict[str, str]) -> tuple[dict[str, dict[str, Any]], list[str], dict[str, int]]:
    symbol, contract = item["symbol"], item["contract"]
    paths = (
        root / "stocks_indicators_5min_eq_live" / f"{symbol}_stocks_indicators_5min.parquet",
        root / "stocks_indicators_1min_eq" / f"{symbol}_stocks_indicators_1min.parquet",
        root / "stocks_indicators_5min_eq_live2" / f"{symbol}_stocks_indicators_5min.parquet",
        root / "stocks_indicators_daily_eq" / f"{symbol}_stocks_indicators_daily.parquet",
        root / "fno_oi/raw_contracts_5m" / f"{_contract_stem(contract)}_5minute.parquet",
    )
    identities = []
    for path in paths:
        try:
            stat = path.stat()
            identities.append((stat.st_mtime_ns, stat.st_size))
        except FileNotFoundError:
            identities.append((0, 0))
    # Boundary changes and atomic source updates both invalidate the result.
    # Historical dates use a fixed cutoff, so scrolling between them is cheap.
    cutoff = min(now.floor("5min"), pd.Timestamp(f"{day} 23:55", tz=IST))
    return _symbol_rows_cached(str(root), day, prior, cutoff, day < now.date(), symbol, contract, tuple(identities))


@lru_cache(maxsize=1024)
def _symbol_rows_cached(root_string: str, day: date, prior: date | None, now: pd.Timestamp, historical: bool, symbol: str, contract: str, identities: tuple) -> tuple[dict[str, dict[str, Any]], list[str], dict[str, int]]:
    root = Path(root_string)
    start = prior or day - timedelta(days=10)
    errors: list[str] = []
    paths = [
        (root / "stocks_indicators_5min_eq_live" / f"{symbol}_stocks_indicators_5min.parquet", "LIVE_CASH_5M", False),
        (root / "stocks_indicators_1min_eq" / f"{symbol}_stocks_indicators_1min.parquet", "HISTORICAL_CASH_1M_AGGREGATED", True),
        (root / "stocks_indicators_5min_eq_live2" / f"{symbol}_stocks_indicators_5min.parquet", "HISTORICAL_CASH_5M", False),
    ]
    if historical:
        paths[0], paths[1] = paths[1], paths[0]
    cash: dict[pd.Timestamp, dict[str, Any]] = {}
    used: dict[str, int] = {}
    for path, label, minute in paths:
        try:
            bars = _read_bars(path, start, day)
            bars = _aggregate_minutes(bars) if minute else _cash_valid(bars)
            for row in bars.to_dict("records"):
                ts = row["ts"]
                if ts <= now and ts not in cash:
                    cash[ts] = {"price": _number(row["close"], positive=True), "open": _number(row["open"], positive=True), "source": label}
                    used[label] = used.get(label, 0) + 1
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"{symbol}: {label} {type(exc).__name__}")
    prior_stamps = sorted(ts for ts in cash if ts.date() < day and (prior is None or ts.date() == prior))
    proxy_ts = prior_stamps[-1] if prior_stamps else None
    proxy = cash[proxy_ts]["price"] if proxy_ts is not None else None
    close_ts = pd.Timestamp(datetime.combine(prior, time(15, 30)), tz=IST) if prior else None
    previous_close = cash.get(close_ts, {}).get("price")
    previous_close_source = cash.get(close_ts, {}).get("source") if previous_close is not None else None
    # Exact dated daily candles are valid baselines; stale daily caches are not.
    if prior is not None and previous_close is None:
        daily_path = root / "stocks_indicators_daily_eq" / f"{symbol}_stocks_indicators_daily.parquet"
        try:
            daily = _read_bars(daily_path, prior, prior)
            if not daily.empty:
                previous_close = _number(daily.iloc[-1].get("close"), positive=True)
                if previous_close is not None:
                    previous_close_source = "DAILY_CASH_CLOSE"
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"{symbol}: DAILY_CASH_CLOSE {type(exc).__name__}")
    oi_values: dict[pd.Timestamp, float] = {}
    try:
        futures = _read_bars(root / "fno_oi/raw_contracts_5m" / f"{_contract_stem(contract)}_5minute.parquet", start, day)
        if not futures.empty:
            if "quality_state" in futures:
                futures = futures.loc[futures.quality_state.astype(str).str.upper().eq("VALID")]
            if "candle_start" in futures:
                futures = futures.loc[futures.ts.sub(_stamp(futures.candle_start)).eq(pd.Timedelta(minutes=5))]
            for row in futures.to_dict("records"):
                number = _number(row.get("oi"))
                if row["ts"] <= now and number is not None and number >= 0:
                    oi_values[row["ts"]] = number
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(f"{symbol}: FUTURES_OI {type(exc).__name__}")
    previous_oi = oi_values.get(close_ts)
    result = {}
    for slot in SLOTS:
        stamp = pd.Timestamp(f"{day} {slot}", tz=IST)
        if stamp > now:
            continue
        entry = cash.get(stamp, {})
        price, price_source = entry.get("price"), entry.get("source")
        kind = "COMPLETED_5M_BAR"
        if slot == "09:15":
            first = cash.get(stamp + pd.Timedelta(minutes=5), {})
            price, price_source = first.get("open"), first.get("source")
            kind = "RECONSTRUCTED_SESSION_OPEN"
        oi = oi_values.get(stamp) if slot != "09:15" else None
        if price is None and oi is None:
            continue
        previous_price = cash.get(stamp - pd.Timedelta(minutes=5), {}).get("price")
        if slot == "09:20":
            previous_price = entry.get("open")
        oi_change_5m = _pct(oi, oi_values.get(stamp - pd.Timedelta(minutes=5))) if slot != "09:15" else None
        oi_day = _pct(oi, previous_oi)
        result[slot] = {
            "symbol": symbol, "contract": contract, "timestamp": stamp.isoformat(),
            "price": price, "previous_close": previous_close, "previous_close_source": previous_close_source,
            "previous_close_date": prior.isoformat() if previous_close is not None and prior else None,
            "price_pct": _pct(price, previous_close), "change5m_pct": _pct(price, previous_price) if slot != "09:15" else None,
            "previous_close_proxy": proxy, "previous_close_proxy_timestamp": proxy_ts.isoformat() if proxy_ts is not None else None,
            "previous_close_proxy_time": proxy_ts.isoformat() if proxy_ts is not None else None,
            "price_proxy_pct": _pct(price, proxy), "oi": oi, "previous_oi": previous_oi,
            "oi_change_pct": oi_day, "oi_day_change_pct": oi_day, "oi_change5m_pct": oi_change_5m,
            "price_source": price_source, "oi_source": "NEAR_MONTH_FUTURES_5M" if oi is not None else None,
            "bar_kind": kind,
        }
    return result, errors, used


def load_session_market(root: Path | str, session_date: str | date, now: datetime | pd.Timestamp | None = None) -> dict[str, Any]:
    """Return all 76 reporting boundaries with explicit missing/future coverage.

    ``price_pct`` only uses an exact previous-session close or dated daily close.
    An incomplete day's last real candle is exposed separately as a proxy. OI
    daily and adjacent-five-minute deltas always use the same futures contract.
    """
    root, day = Path(root), _session_date(session_date)
    current = pd.Timestamp(now or datetime.now(IST))
    current = current.tz_localize(IST) if current.tzinfo is None else current.tz_convert(IST)
    dates = list_available_dates(root)
    prior_dates = [date.fromisoformat(value) for value in dates if value < day.isoformat()]
    prior = max(prior_dates) if prior_dates else None
    universe, universe_kind = _universe(root, day)
    gathered: dict[str, list[dict[str, Any]]] = {slot: [] for slot in SLOTS}
    errors: list[str] = []
    source_counts: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="flow-market-read") as workers:
        for rows, issues, used in workers.map(lambda item: _symbol_rows(root, day, prior, current, item), universe):
            for slot, row in rows.items():
                gathered[slot].append(row)
            errors.extend(issues)
            for label, count in used.items():
                source_counts[label] = source_counts.get(label, 0) + count
    slots = []
    for slot in SLOTS:
        rows = gathered[slot]
        coverage = {"universe": len(universe), "prices": sum(row["price"] is not None for row in rows), "previous_close": sum(row["previous_close"] is not None and row["price"] is not None for row in rows), "oi": sum(row["oi"] is not None for row in rows), "oi_baseline": sum(row["oi_change_pct"] is not None for row in rows), "change5m": sum(row["change5m_pct"] is not None for row in rows), "oi_change5m": sum(row["oi_change5m_pct"] is not None for row in rows)}
        future = pd.Timestamp(f"{day} {slot}", tz=IST) > current
        state = "FUTURE" if future else "MISSING" if not rows else "OPENING_REFERENCE" if slot == "09:15" else "COMPLETE" if coverage["prices"] == len(universe) and coverage["oi"] == len(universe) else "PARTIAL"
        slots.append({"time": slot, "state": state, "rows": rows, "coverage": coverage})
    warnings = []
    if universe_kind == "RECONSTRUCTED_NEAR_MONTH_REGISTRY":
        warnings.append("No dated universe was saved. Near-month membership is reconstructed from the local contract registry; historical membership may differ.")
    if universe_kind == "MISSING_UNIVERSE":
        warnings.append("No saved universe or contract registry is available for this session.")
    if any(slot["coverage"]["prices"] > slot["coverage"]["previous_close"] for slot in slots):
        warnings.append("Some previous-session closing bars are missing or gap-filled. Verified previous-close changes are unavailable for those stocks; a last-real-bar proxy is supplied separately.")
    return {
        "schema_version": "dashboard_flow_live_market_v1", "session_date": day.isoformat(), "as_of": current.isoformat(),
        "previous_session": prior.isoformat() if prior else None, "universe_state": universe_kind, "universe_count": len(universe),
        "slots": slots, "warnings": warnings, "errors": errors[:30], "error_count": len(errors),
        "sources": [
            {"label": "Live equity candles", "path": str(root / "stocks_indicators_5min_eq_live"), "detail": "NSE cash price; exact completed bars, never gap-filled or provisional."},
            {"label": "Historical equity candles", "path": str(root / "stocks_indicators_1min_eq"), "detail": "Five exact completed one-minute candles per historical five-minute bar; saved live/historical 5m fallback."},
            {"label": "Futures open interest", "path": str(root / "fno_oi/raw_contracts_5m"), "detail": "Exact same-contract OI; daily change versus the previous session's 15:30 bar, 5m change only for adjacent bars."},
            {"label": "Contract membership", "path": str(root / "fno_oi/universe"), "detail": universe_kind},
        ],
        "source_counts": source_counts,
        "semantics": {"timezone": "Asia/Kolkata", "price_pct": "cash price versus verified previous-session close", "oi_change_pct": "same-contract OI versus previous-session 15:30 OI", "oi_change5m_pct": "same-contract OI versus the exact previous five-minute boundary", "opening": "09:15 is the reconstructed opening price from the completed 09:20 candle; no 09:15 OI or five-minute change."},
    }
