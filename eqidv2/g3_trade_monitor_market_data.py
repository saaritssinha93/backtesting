"""Isolated market-data reads for already entered G3 positions.

Only full quotes and historical one-minute candles are requested. This module
has no order, position mutation, token refresh or production-data write path.
"""
from __future__ import annotations

from datetime import datetime, time as wall_time, timedelta
import json
import math
import os
from pathlib import Path
import re
import time
from zoneinfo import ZoneInfo

from fno_v13_v10_g_identity import _safe_contract_stem
from nse_market_calendar import market_closed_reason

IST = ZoneInfo("Asia/Kolkata")
SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9.&_-]{0,39}\Z")


def _stamp(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.replace(tzinfo=IST) if result.tzinfo is None else result.astimezone(IST)
    except (TypeError, ValueError, OverflowError):
        return None


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, allow_nan=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def quote_rows(payload, symbols, received):
    """Use exchange and last-trade timestamps, never receipt as price time."""
    result = []
    for symbol in sorted(symbols):
        raw = payload.get("NSE:" + symbol, {})
        exchanged, traded = _stamp(raw.get("timestamp")), _stamp(raw.get("last_trade_time"))
        price, token = _number(raw.get("last_price")), _number(raw.get("instrument_token"))
        if (not exchanged or not traded or traded.date() != received.date()
                or not traded <= exchanged <= received or price is None or price <= 0
                or token is None or token <= 0 or not token.is_integer()):
            continue
        result.append(dict(symbol=symbol, price=price, instrument_token=int(token),
            exchange_timestamp=exchanged.isoformat(), last_trade_time=traded.isoformat(),
            received_at=received.isoformat(), source="KITE_FULL_QUOTE"))
    return result


def completed_bars(records, day, now):
    """Kite historical dates label START; normalize to completed candle END."""
    result = {}
    for raw in records:
        start = _stamp(raw.get("date"))
        if not start or start.date() != day or start.second or start.microsecond:
            continue
        end = start + timedelta(minutes=1)
        if end > now:
            continue
        values = {key: _number(raw.get(key)) for key in ("open", "high", "low", "close", "volume")}
        if (any(values[k] is None or values[k] <= 0 for k in ("open", "high", "low", "close"))
                or values["volume"] is None or values["volume"] < 0
                or not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]):
            raise ValueError("Invalid historical OHLCV")
        bar = dict(timestamp=end.isoformat(), **values)
        if end in result and result[end] != bar:
            raise ValueError("Conflicting historical candles")
        result[end] = bar
    return [result[key] for key in sorted(result)]


class MarketDataFeed:
    def __init__(self, output_root, *, credential_dir=None, app="app1", client=None,
                 sleeper=time.sleep, clock=None):
        if not re.fullmatch(r"app[1-8]", app):
            raise ValueError("Market-data app must be app1 through app8")
        self.output_root = Path(output_root).resolve()
        self.credential_dir = Path(credential_dir or Path(__file__).resolve().parent)
        self.app, self.client = app, client
        self.sleeper, self.clock = sleeper, clock or (lambda: datetime.now(IST))
        self.last_history, self.retry_at = {}, None

    def refresh(self, snapshot, now=None):
        started = _stamp(now or self.clock())
        if started is None:
            raise ValueError("A valid current time is required")
        status = dict(schema_version="g3_trade_monitor_market_data_status_v1",
            generated_at_ist=started.isoformat(), source="KITE_FULL_QUOTE_AND_1M_HISTORY",
            app=self.app, read_only=True, execution_authority=False, symbols=0,
            quotes=0, minute_symbols=0, warming_up_symbols=[], errors=[])
        if (snapshot.get("session_date") != started.date().isoformat()
                or market_closed_reason(started.date())
                or not wall_time(9,15) <= started.time().replace(tzinfo=None) <= wall_time(15,35)):
            status["state"] = "OUTSIDE_CURRENT_MARKET_SESSION"
            return status
        entries = {}
        for row in snapshot.get("trades", []):
            symbol, entered = row.get("symbol", ""), _stamp(row.get("entry_at_ist"))
            if (row.get("status") not in {"OPEN", "SQUARE_OFF_PENDING"} or row.get("mode") not in {"PAPER", "LIVE"}
                    or not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol)
                    or entered is None or entered.date() != started.date() or entered > started):
                continue
            entries[symbol] = min(entered, entries.get(symbol, entered))
        if len(entries) > 100:
            status.update(state="SYMBOL_LIMIT_EXCEEDED", symbols=len(entries))
            return status
        status["symbols"] = len(entries)
        if not entries:
            status["state"] = "NO_OPEN_POSITIONS"
            return status
        if self.retry_at and started < self.retry_at:
            status["state"] = "WAITING_MARKET_DATA_RETRY"
            return status
        try:
            if self.client is None:
                from dashboard_flow_observer import make_client
                self.client = make_client(self.app, self.credential_dir, timeout=6)
            payload = self.client.quote(["NSE:" + symbol for symbol in sorted(entries)])
            received = _stamp(self.clock())
            quotes = quote_rows(payload, entries, received)
            _write(self.output_root / "quotes/latest.json", dict(schema_version="g3_trade_monitor_quotes_v1",
                session_date=started.date().isoformat(), received_at=received.isoformat(), quotes=quotes))
            status["quotes"] = len(quotes)
        except Exception as exc:
            # Exception text may contain authenticated transport details.
            status.update(state="MARKET_DATA_UNAVAILABLE", errors=[type(exc).__name__])
            self.client = None
            self.retry_at = started + timedelta(seconds=60)
            _write(self.output_root / "quotes/status.json", status)
            return status
        for quote in quotes:
            symbol = quote["symbol"]
            if symbol in self.last_history and (started - self.last_history[symbol]).total_seconds() < 60:
                continue
            self.last_history[symbol] = started
            try:
                self.sleeper(.4)
                records = self.client.historical_data(quote["instrument_token"],
                    entries[symbol].replace(second=0, microsecond=0), started, "minute", continuous=False, oi=False)
                observed = _stamp(self.clock())
                bars = completed_bars(records, started.date(), observed)
                if not bars and (observed - entries[symbol]).total_seconds() < 120:
                    status['warming_up_symbols'].append(symbol)
                    continue
                if not bars:
                    raise ValueError("No completed post-entry minute data")
                destination = self.output_root / "minutes" / started.date().isoformat() / (_safe_contract_stem(symbol) + ".json")
                _write(destination, dict(schema_version="g3_trade_monitor_minutes_v1", symbol=symbol,
                    session_date=started.date().isoformat(), received_at=observed.isoformat(),
                    source="KITE_HISTORICAL_1M", timestamp_convention="CANDLE_END", bars=bars))
                status["minute_symbols"] += 1
            except Exception as exc:
                status["errors"].append(symbol + ":" + type(exc).__name__)
        status["state"] = "READY" if not status["errors"] and len(quotes) == len(entries) else "PARTIAL_MARKET_DATA"
        _write(self.output_root / "quotes/status.json", status)
        return status
