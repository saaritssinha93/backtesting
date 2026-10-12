"""Reporting-only, completed daily equity OHLC backfill for Dashboard Flow.

Dry run is the default. ``--fetch`` reads Kite historical DAY candles using one
saved app credential; this module has no broker order, position or trade APIs.
Existing canonical daily rows are validated and retained. Missing candles stay
missing and are listed in the audit instead of being filled or invented.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, time, timedelta
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time as clock
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

from nse_market_calendar import market_closed_reason

IST = ZoneInfo("Asia/Kolkata")
SOURCE = "KITE_HISTORICAL_DAY"
SCHEMA = "dashboard_flow_daily_backfill_v1"
COLUMNS = ("date", "open", "high", "low", "close", "volume", "source", "quality_state", "fetched_at")
SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9&.\-]{0,39}\Z")
FINALIZED_AFTER = time(16, 0)


class BackfillError(ValueError):
    """A fixed, non-sensitive failure code suitable for audit output."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _now(value: datetime | None = None) -> datetime:
    value = value or datetime.now(IST)
    return value.replace(tzinfo=IST) if value.tzinfo is None else value.astimezone(IST)


def _day(value: date | str) -> date:
    try:
        if isinstance(value, datetime):
            raise ValueError
        return value if isinstance(value, date) else date.fromisoformat(value)
    except (TypeError, ValueError):
        raise BackfillError("INVALID_SESSION_DATE") from None


def _completed(day: date, now: datetime) -> bool:
    return day < now.date() or day == now.date() and now.time().replace(tzinfo=None) >= FINALIZED_AFTER


def _timestamp(value: Any) -> pd.Timestamp:
    try:
        stamp = pd.Timestamp(value)
        if pd.isna(stamp):
            raise ValueError
        return stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)
    except (TypeError, ValueError, OverflowError):
        raise BackfillError("INVALID_TIMESTAMP") from None


def _flagged(value: Any) -> bool:
    if value is None or pd.isna(value):
        return False
    return str(value).strip().lower() not in {"", "false", "0", "0.0", "no", "none"}


def validate_daily_frame(frame: pd.DataFrame, *, now: datetime, provenance: bool = True,
                         start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
    """Reject the entire response/file on malformed, duplicate or incomplete rows."""
    now = _now(now)
    if frame.empty:
        return pd.DataFrame(columns=COLUMNS)
    if not set(COLUMNS[:6]).issubset(frame.columns):
        raise BackfillError("MISSING_OHLC_COLUMNS")
    if provenance and not set(COLUMNS).issubset(frame.columns):
        raise BackfillError("MISSING_DAILY_PROVENANCE")
    for flag in ("is_gap_filled", "gap_filled", "forward_filled", "is_synthetic", "synthetic", "is_filled"):
        if flag in frame and frame[flag].map(_flagged).any():
            raise BackfillError("SYNTHETIC_DAILY_ROW")
    if "quality_state" in frame and not frame["quality_state"].eq("VALID").all():
        raise BackfillError("INVALID_DAILY_QUALITY")
    if "source" in frame and not frame["source"].eq(SOURCE).all():
        raise BackfillError("UNVERIFIED_DAILY_SOURCE")
    output = frame.copy()
    output["date"] = pd.DatetimeIndex([_timestamp(value).normalize() for value in output["date"]])
    if output["date"].duplicated().any():
        raise BackfillError("DUPLICATE_SESSION_DATES")
    for stamp in output["date"]:
        day = stamp.date()
        if not _completed(day, now):
            raise BackfillError("UNFINISHED_OR_FUTURE_SESSION")
        if start_date is not None and day < start_date or end_date is not None and day > end_date:
            raise BackfillError("OUT_OF_REQUEST_RANGE")
    for name in COLUMNS[1:6]:
        if output[name].map(lambda value: isinstance(value, bool)).any():
            raise BackfillError("INVALID_OHLC_VALUE")
        output[name] = pd.to_numeric(output[name], errors="coerce")
        if not output[name].map(lambda value: pd.notna(value) and math.isfinite(float(value))).all():
            raise BackfillError("INVALID_OHLC_VALUE")
    valid = ((output[["open", "high", "low", "close"]] > 0).all(axis=1)
             & (output["volume"] >= 0) & (output["volume"] % 1 == 0)
             & (output["low"] <= output[["open", "close"]].min(axis=1))
             & (output["high"] >= output[["open", "close"]].max(axis=1)))
    if not valid.all():
        raise BackfillError("INVALID_OHLC_BOUNDS")
    if provenance:
        fetched = pd.DatetimeIndex([_timestamp(value) for value in output["fetched_at"]])
        if any(value > now or value.date() < stamp.date() for value, stamp in zip(fetched, output["date"])):
            raise BackfillError("INVALID_FETCH_PROVENANCE")
        output["fetched_at"] = fetched
    else:
        output["fetched_at"] = pd.Timestamp(now)
    output["source"], output["quality_state"] = SOURCE, "VALID"
    return output[list(COLUMNS)].sort_values("date").reset_index(drop=True)


def load_equity_tokens(data_root: Path, *, now: datetime, symbols: list[str] | None = None,
                       universe_path: Path | None = None) -> tuple[dict[str, int], Path]:
    directory = Path(data_root).resolve() / "fno_oi" / "universe"
    dated = []
    for path in directory.glob("near_month_*.parquet"):
        match = re.fullmatch(r"near_month_(\d{4}-\d{2}-\d{2})\.parquet", path.name)
        if match and _day(match[1]) <= now.date():
            dated.append(path)
    if universe_path is not None:
        path = Path(universe_path).resolve()
        match = re.fullmatch(r"near_month_(\d{4}-\d{2}-\d{2})\.parquet", path.name)
        if path.parent != directory.resolve() or not match or _day(match[1]) > now.date():
            raise BackfillError("INVALID_RETAINED_UNIVERSE_PATH")
    else:
        path = sorted(dated)[-1] if dated else directory / "latest_near_month.parquet"
    try:
        universe = pd.read_parquet(path)
    except Exception:
        raise BackfillError("UNIVERSE_UNAVAILABLE") from None
    if not {"equity_symbol", "equity_instrument_token"}.issubset(universe.columns):
        raise BackfillError("UNIVERSE_MAPPING_MISSING")
    if universe_path is not None and not {"master_date", "equity_exchange"}.issubset(universe.columns):
        raise BackfillError("RETAINED_UNIVERSE_PROVENANCE_MISSING")
    if "master_date" in universe:
        try:
            master_dates = pd.to_datetime(universe["master_date"], errors="raise")
            observed = set(master_dates.dt.date)
            match = re.fullmatch(r"near_month_(\d{4}-\d{2}-\d{2})\.parquet", path.name)
            if master_dates.isna().any() or len(observed) != 1 or any(day > now.date() for day in observed):
                raise ValueError
            if match and observed != {_day(match[1])}:
                raise ValueError
        except (TypeError, ValueError, AttributeError):
            raise BackfillError("INVALID_UNIVERSE_MASTER_DATE") from None
    if "is_index_future" in universe:
        universe = universe.loc[~universe["is_index_future"].map(_flagged)]
    if "equity_exchange" in universe:
        universe = universe.loc[universe["equity_exchange"].eq("NSE")]
    mapping: dict[str, int] = {}
    for row in universe.itertuples(index=False):
        if pd.isna(row.equity_symbol) or pd.isna(row.equity_instrument_token):
            continue
        symbol = str(row.equity_symbol).strip().upper()
        try:
            token = float(row.equity_instrument_token)
        except (TypeError, ValueError):
            raise BackfillError("INVALID_EQUITY_TOKEN") from None
        if not SYMBOL.fullmatch(symbol) or not math.isfinite(token) or token <= 0 or token % 1:
            raise BackfillError("INVALID_EQUITY_MAPPING")
        if symbol in mapping and mapping[symbol] != int(token):
            raise BackfillError("CONFLICTING_EQUITY_TOKENS")
        mapping[symbol] = int(token)
    if len(set(mapping.values())) != len(mapping):
        raise BackfillError("DUPLICATED_EQUITY_TOKEN")
    if symbols:
        wanted = {str(symbol).strip().upper() for symbol in symbols}
        if any(not SYMBOL.fullmatch(symbol) for symbol in wanted):
            raise BackfillError("INVALID_SYMBOL_SELECTION")
        if wanted.difference(mapping):
            raise BackfillError("SYMBOL_NOT_IN_SAVED_UNIVERSE")
        mapping = {symbol: mapping[symbol] for symbol in wanted}
    if not mapping:
        raise BackfillError("EMPTY_EQUITY_UNIVERSE")
    return dict(sorted(mapping.items())), path


def requested_sessions(start: date, end: date) -> list[date]:
    return [day for count in range((end - start).days + 1)
            if not market_closed_reason(day := start + timedelta(days=count))]


def latest_completed_session(now: datetime | None = None) -> date:
    now = _now(now)
    day = now.date() if now.time().replace(tzinfo=None) >= FINALIZED_AFTER else now.date() - timedelta(days=1)
    while market_closed_reason(day):
        day -= timedelta(days=1)
    return day


def missing_windows(sessions: list[date], existing: set[date], *, chunk_days: int = 365) -> list[tuple[date, date]]:
    """Group missing sessions, ending at known rows or bounded request windows."""
    result = []
    first = last = None
    for day in sessions:
        if day in existing:
            if first is not None:
                result.append((first, last))
            first = last = None
            continue
        if first is None:
            first = day
        elif (day - first).days >= chunk_days:
            result.append((first, last))
            first = day
        last = day
    if first is not None:
        result.append((first, last))
    return result


def _atomic(path: Path, writer: Callable[[Path], None]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix="." + path.stem + ".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    temp = Path(name)
    try:
        writer(temp)
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _write_json(path: Path, data: dict) -> None:
    _atomic(path, lambda temp: temp.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8"))


def _error_code(error: Exception) -> str:
    if isinstance(error, BackfillError):
        return error.code
    name = type(error).__name__
    code = getattr(error, "code", None)
    if name in {"TokenException", "PermissionException"} or code in {401, 403}:
        return "BROKER_AUTHENTICATION_FAILED"
    if code == 429 or name in {"RateLimitException", "TooManyRequests"}:
        return "BROKER_RATE_LIMITED"
    if name in {"InputException", "DataException"}:
        return "BROKER_INVALID_REQUEST_OR_RESPONSE"
    return "BROKER_READ_FAILED"


class _Reader:
    def __init__(self, client: Any, *, interval: float, retries: int, sleep: Callable, monotonic: Callable):
        self.client, self.interval, self.retries = client, max(0.4, interval), retries
        self.sleep, self.monotonic, self.last_start, self.calls = sleep, monotonic, None, 0

    def read(self, token: int, start: date, end: date) -> Any:
        for attempt in range(self.retries):
            if self.last_start is not None:
                remaining = self.interval - (self.monotonic() - self.last_start)
                if remaining > 0:
                    self.sleep(remaining)
            self.last_start = self.monotonic()
            self.calls += 1
            try:
                return self.client.historical_data(instrument_token=token,
                    from_date=datetime.combine(start, time.min, IST),
                    to_date=datetime.combine(end, time(23, 59, 59), IST),
                    interval="day", continuous=False, oi=False)
            except Exception as error:
                code = _error_code(error)
                if code in {"BROKER_AUTHENTICATION_FAILED", "BROKER_INVALID_REQUEST_OR_RESPONSE"} or attempt + 1 == self.retries:
                    raise BackfillError(code) from None
                self.sleep(min(3.0, 0.8 * (attempt + 1)))
        raise BackfillError("BROKER_READ_FAILED")


def _saved_client(app_name: str | None):
    from fno_oi_common import discover_kite_credentials, make_kite_client
    try:
        credentials = discover_kite_credentials()
        credential = next((value for value in credentials if not app_name or value.app_name == app_name), None)
        if credential is None:
            raise BackfillError("SAVED_APP_UNAVAILABLE")
        return make_kite_client(credential, timeout_sec=12.0)
    except BackfillError:
        raise
    except Exception:
        raise BackfillError("SAVED_CREDENTIAL_CLIENT_UNAVAILABLE") from None


def backfill(data_root: Path, start_date: date | str = "2026-05-26", end_date: date | str | None = None, *,
             symbols: list[str] | None = None, fetch: bool = False, client: Any = None,
             app_name: str | None = None, universe_path: Path | None = None,
             now: datetime | None = None, interval: float = 0.45,
             retries: int = 3, chunk_days: int = 365, sleep: Callable = clock.sleep,
             monotonic: Callable = clock.monotonic) -> dict:
    live_clock = now is None
    now, data_root = _now(now), Path(data_root).resolve()
    start, end = _day(start_date), _day(end_date) if end_date is not None else latest_completed_session(now)
    if start > end:
        raise BackfillError("INVALID_DATE_RANGE")
    if not _completed(end, now):
        raise BackfillError("UNFINISHED_OR_FUTURE_REQUEST")
    if not 1 <= chunk_days <= 365 or not 1 <= retries <= 3 or not math.isfinite(interval) or interval < 0.4:
        raise BackfillError("INVALID_RATE_OR_RETRY_LIMIT")
    mapping, selected_universe = load_equity_tokens(data_root, now=now, symbols=symbols, universe_path=universe_path)
    sessions = requested_sessions(start, end)
    output_root = data_root / "fno_oi" / "raw_equity_daily"
    audit_path = output_root / "audits" / (now.strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8] + ".json")
    audit = dict(schema_version=SCHEMA, generated_at=now.isoformat(), mode="FETCH" if fetch else "DRY_RUN",
        source=SOURCE, reporting_only=True, execution_authority=False,
        start_date=start.isoformat(), end_date=end.isoformat(), expected_sessions=len(sessions),
        calendar="nse_market_calendar: regular reviewed sessions", finalization="16:00 Asia/Kolkata",
        universe=str(selected_universe.relative_to(data_root)), minimum_request_interval_sec=interval,
        max_attempts_per_request=retries, audit_path=str(audit_path), symbols=[], calls=0)
    reader = None
    initialization_failed = False
    for symbol, token in mapping.items():
        path = output_root / (symbol + "_day.parquet")
        item = dict(symbol=symbol, instrument_token=token, status="PLANNED", existing_rows=0,
            fetched_rows=0, written_rows=0, covered_sessions=0, missing_sessions=[], requests=[])
        audit["symbols"].append(item)
        try:
            try:
                existing = validate_daily_frame(pd.read_parquet(path), now=now) if path.exists() else pd.DataFrame(columns=COLUMNS)
            except BackfillError:
                raise
            except Exception:
                raise BackfillError("EXISTING_DAILY_FILE_UNREADABLE") from None
            known = {stamp.date() for stamp in existing["date"]}
            item["existing_rows"] = len(existing)
            windows = missing_windows(sessions, known, chunk_days=chunk_days)
            item["requests"] = [dict(start=a.isoformat(), end=b.isoformat()) for a, b in windows]
            item["missing_sessions"] = [day.isoformat() for day in sessions if day not in known]
            item["covered_sessions"] = len(sessions) - len(item["missing_sessions"])
            if not windows:
                item["status"] = "UP_TO_DATE"
                continue
            if not fetch:
                continue
            if initialization_failed:
                raise BackfillError("SKIPPED_AFTER_AUTHENTICATION_OR_CLIENT_FAILURE")
            if reader is None:
                try:
                    reader = _Reader(client if client is not None else _saved_client(app_name),
                        interval=interval, retries=retries, sleep=sleep, monotonic=monotonic)
                except Exception:
                    initialization_failed = True
                    raise BackfillError("SAVED_CREDENTIAL_CLIENT_UNAVAILABLE") from None
            new_frames = []
            for first, last in windows:
                records = reader.read(token, first, last)
                if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
                    raise BackfillError("INVALID_BROKER_CANDLE_RESPONSE")
                current = validate_daily_frame(pd.DataFrame(records), now=_now() if live_clock else now, provenance=False,
                    start_date=first, end_date=last)
                new_frames.append(current)
            fetched = pd.concat(new_frames, ignore_index=True) if new_frames else pd.DataFrame(columns=COLUMNS)
            item["fetched_rows"] = len(fetched)
            if not fetched.empty:
                merged = pd.concat([existing, fetched], ignore_index=True) if not existing.empty else fetched
                merged = validate_daily_frame(merged, now=_now() if live_clock else now)
                try:
                    _atomic(path, lambda temp: merged.to_parquet(temp, index=False))
                except Exception:
                    raise BackfillError("ATOMIC_DAILY_WRITE_FAILED") from None
                item["written_rows"] = len(merged)
                known = {stamp.date() for stamp in merged["date"]}
            item["missing_sessions"] = [day.isoformat() for day in sessions if day not in known]
            item["covered_sessions"] = len(sessions) - len(item["missing_sessions"])
            item["status"] = "PARTIAL" if item["missing_sessions"] else "COMPLETE"
        except KeyboardInterrupt:
            item.update(status="INTERRUPTED", error_code="INTERRUPTED")
            audit["interrupted"] = True
            break
        except Exception as error:
            code = error.code if isinstance(error, BackfillError) else "LOCAL_VALIDATION_OR_IO_FAILED"
            item.update(status="FAILED", error_code=code)
            if code == "BROKER_AUTHENTICATION_FAILED":
                initialization_failed = True
    audit["calls"] = reader.calls if reader is not None else 0
    audit["summary"] = dict(Counter(item["status"] for item in audit["symbols"]))
    audit["state"] = ("INTERRUPTED" if audit.get("interrupted") else "DRY_RUN" if not fetch
        else "PARTIAL" if any(item["status"] in {"FAILED", "PARTIAL"} for item in audit["symbols"]) else "COMPLETE")
    _write_json(audit_path, audit)
    return audit


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")))
    parser.add_argument("--start-date", default="2026-05-26", type=str)
    end = parser.add_mutually_exclusive_group()
    end.add_argument("--end-date", type=str, help="Completed session cutoff; default latest completed NSE session")
    end.add_argument("--latest-completed", action="store_true", help="Use the most recent NSE session finalized before 16:00 IST (the default)")
    parser.add_argument("--symbols", help="Comma-separated saved-universe equity symbols; default all mapped NSE stocks")
    parser.add_argument("--universe-path", type=Path, help="Exact retained near_month_YYYY-MM-DD.parquet inside this data root's fno_oi/universe directory; default newest saved universe")
    parser.add_argument("--fetch", action="store_true", help="Read completed daily candles from Kite and atomically save them; otherwise plan only")
    parser.add_argument("--app-name", choices=["app" + str(number) for number in range(1, 9)])
    args = parser.parse_args(argv)
    try:
        result = backfill(args.data_root, args.start_date, args.end_date,
            symbols=args.symbols.split(",") if args.symbols else None, fetch=args.fetch,
            app_name=args.app_name, universe_path=args.universe_path)
        print(json.dumps({key: result[key] for key in ("state", "mode", "calls", "summary", "audit_path")}, indent=2))
        return 130 if result["state"] == "INTERRUPTED" else 2 if result["state"] == "PARTIAL" else 0
    except Exception as error:
        code = error.code if isinstance(error, BackfillError) else "BACKFILL_FAILED"
        print(json.dumps({"state": "FAILED", "error_code": code}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
