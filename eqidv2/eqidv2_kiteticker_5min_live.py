# -*- coding: utf-8 -*-
"""
Persistent eight-app KiteTicker 5-minute feed.

This is a shadow feed by default. It writes to its own Parquet directory and
publishes its own status, universe manifest, and slot markers so it can run
beside the existing REST feed without creating a dual-writer race.

Data contract:
- all eight configured Kite apps are required;
- the canonical fetchable universe is split round-robin across the apps;
- MODE_FULL ticks are aggregated with exchange/last-trade timestamps;
- 09:15 is an opening snapshot;
- 09:20..15:30 are completed, end-stamped five-minute candles;
- cumulative day volume is converted to per-candle volume;
- genuine no-trade intervals receive the same zero-volume carry-forward
  treatment as the existing minimal feed;
- an app that did not cover a complete interval is repaired through its REST
  historical endpoint before the slot can be marked complete;
- indicators and atomic Parquet persistence reuse the existing minimal core.

The scheduled task for this module is intentionally created disabled. Do not
point the production scanner at this feed until shadow parity is proven.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import pytz
from kiteconnect import KiteConnect, KiteTicker

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal as core
from eqidv2_runtime_paths import DATA_5M_DIR as PRIMARY_DATA_5M_DIR
from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir


SCRIPT_DIR = Path(__file__).resolve().parent
IST = pytz.timezone("Asia/Kolkata")
APP_NAMES = tuple(f"app{idx}" for idx in range(1, 9))
INTERVAL_MINUTES = 5
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
PROCESS_EXIT_TIME = dtime(15, 31)
END_5M = "_stocks_indicators_5min.parquet"

OUTPUT_DIR = Path(
    os.getenv(
        "EQIDV2_KITETICKER_5M_DATA_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live_kiteticker",
    )
)
SEED_DATA_DIR = Path(
    os.getenv(
        "EQIDV2_KITETICKER_5M_SEED_DATA_DIR",
        str(PRIMARY_DATA_5M_DIR),
    )
)
STATUS_PATH = SCRIPT_DIR / "logs" / "eqidv2_kiteticker_5min_live.status.json"
UNIVERSE_MANIFEST_PATH = Path(
    os.getenv(
        "EQIDV2_KITETICKER_5M_UNIVERSE_MANIFEST",
        str(RUNTIME_STATUS_DIR / "feed_universe_kiteticker_5m.json"),
    )
)
RUNTIME_MANIFEST_DIR = runtime_dir("runtime_manifests", "kiteticker_5min_feed")
SLOT_MARKER_DIR = runtime_dir("slot_ready_kiteticker_5m")
TOKEN_CACHE_PATH = Path(getattr(core, "TOKENS_CACHE_FILE", SCRIPT_DIR / "stocks_tokens_cache.json"))

DEFAULT_BOUNDARY_GRACE_SEC = float(os.getenv("EQIDV2_KITETICKER_5M_BOUNDARY_GRACE_SEC", "2"))
DEFAULT_WRITE_WORKERS = max(1, int(os.getenv("EQIDV2_KITETICKER_5M_WRITE_WORKERS", "32")))
DEFAULT_REST_REPAIR_ENABLED = str(
    os.getenv("EQIDV2_KITETICKER_5M_REST_REPAIR", "1")
).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_REST_REPAIR_WORKERS_PER_APP = max(
    1, int(os.getenv("EQIDV2_KITETICKER_5M_REST_REPAIR_WORKERS_PER_APP", "10"))
)
DEFAULT_STATUS_HEARTBEAT_SEC = max(
    1.0, float(os.getenv("EQIDV2_KITETICKER_5M_STATUS_HEARTBEAT_SEC", "5"))
)


def now_ist() -> datetime:
    return datetime.now(IST)


def _canonical_symbols(symbols: Iterable[str]) -> list[str]:
    return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})


def _universe_sha256(symbols: Iterable[str]) -> str:
    canonical = _canonical_symbols(symbols)
    return hashlib.sha256("\n".join(canonical).encode("utf-8")).hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp_path, path)


def _coerce_ist(value: Any, fallback: Optional[datetime] = None) -> Optional[datetime]:
    if value is None:
        return fallback
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return fallback
    if pd.isna(ts):
        return fallback
    if ts.tzinfo is None:
        return IST.localize(ts.to_pydatetime())
    return ts.tz_convert(IST).to_pydatetime()


def _slot_end_for_trade(trade_ts: datetime) -> datetime:
    """Map a trade to its completed end-stamped five-minute candle."""
    ts = trade_ts if trade_ts.tzinfo is not None else IST.localize(trade_ts)
    ts = ts.astimezone(IST)
    floored_minute = (ts.minute // INTERVAL_MINUTES) * INTERVAL_MINUTES
    floor = ts.replace(minute=floored_minute, second=0, microsecond=0)
    return floor + timedelta(minutes=INTERVAL_MINUTES)


def _next_slot_end(current: datetime) -> datetime:
    ts = current if current.tzinfo is not None else IST.localize(current)
    ts = ts.astimezone(IST)
    open_dt = IST.localize(datetime.combine(ts.date(), MARKET_OPEN))
    if ts < open_dt:
        return open_dt
    return _slot_end_for_trade(ts)


def split_tickers_evenly(tickers: Iterable[str], app_count: int = 8) -> list[list[str]]:
    ordered = _canonical_symbols(tickers)
    count = max(1, int(app_count))
    partitions: list[list[str]] = [[] for _ in range(count)]
    for idx, ticker in enumerate(ordered):
        partitions[idx % count].append(ticker)
    return partitions


def _read_first_token(path: Path) -> str:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise RuntimeError(f"Credential file is empty: {path.name}")
    return raw.split()[0].strip()


@dataclass(frozen=True)
class AppCredential:
    app_name: str
    app_index: int
    api_key: str
    access_token: str


def load_app_credentials(base_dir: Path = SCRIPT_DIR) -> dict[str, AppCredential]:
    credentials: dict[str, AppCredential] = {}
    for idx, app_name in enumerate(APP_NAMES, start=1):
        suffix = "" if idx == 1 else str(idx)
        api_path = base_dir / f"api_key{suffix}.txt"
        access_path = base_dir / f"access_token{suffix}.txt"
        if not api_path.exists() or not access_path.exists():
            missing = [
                path.name for path in (api_path, access_path) if not path.exists()
            ]
            raise FileNotFoundError(
                f"{app_name} is missing required credential file(s): {', '.join(missing)}"
            )
        credentials[app_name] = AppCredential(
            app_name=app_name,
            app_index=idx,
            api_key=_read_first_token(api_path),
            access_token=_read_first_token(access_path),
        )
    return credentials


def _cached_token_map() -> dict[str, int]:
    try:
        raw = json.loads(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for symbol, token in raw.items():
        try:
            token_i = int(token)
        except (TypeError, ValueError):
            continue
        symbol_u = str(symbol).strip().upper()
        if symbol_u and token_i > 0:
            out[symbol_u] = token_i
    return out


def _rest_client(credential: AppCredential) -> KiteConnect:
    client = KiteConnect(
        api_key=credential.api_key,
        timeout=float(getattr(core, "DEFAULT_KITE_REQUEST_TIMEOUT_SEC", 8.0)),
    )
    client.set_access_token(credential.access_token)
    return client


def load_fetchable_universe(
    logger: logging.Logger,
    credentials: dict[str, AppCredential],
    *,
    allow_instrument_refresh: bool,
) -> tuple[list[str], dict[str, int]]:
    tickers, embedded_tokens = core.load_stocks_universe(logger)
    universe = _canonical_symbols(tickers)
    token_map = {
        str(symbol).strip().upper(): int(token)
        for symbol, token in dict(embedded_tokens or {}).items()
        if str(symbol).strip()
        and isinstance(token, (int, float))
        and int(token) > 0
    }
    token_map.update(_cached_token_map())

    missing = [ticker for ticker in universe if ticker not in token_map]
    if missing and allow_instrument_refresh:
        logger.warning(
            "Token cache is missing %d symbol(s); refreshing the NSE instrument map with app1.",
            len(missing),
        )
        refreshed = core.load_or_fetch_tokens(
            _rest_client(credentials["app1"]),
            universe,
            logger,
            refresh=True,
        )
        token_map.update({str(k).strip().upper(): int(v) for k, v in refreshed.items()})
        missing = [ticker for ticker in universe if ticker not in token_map]

    if missing:
        raise RuntimeError(
            "Canonical universe contains symbols without an instrument token: "
            + ", ".join(missing[:25])
        )

    return universe, {ticker: int(token_map[ticker]) for ticker in universe}


@dataclass
class MutableCandle:
    slot_end: datetime
    open: float
    high: float
    low: float
    close: float
    start_cumulative_volume: int
    last_cumulative_volume: int
    volume_valid: bool
    tick_count: int = 1

    def update(self, price: float, cumulative_volume: int) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.last_cumulative_volume = max(
            self.last_cumulative_volume, cumulative_volume
        )
        self.tick_count += 1

    def to_row(self) -> dict[str, Any]:
        volume = max(0, self.last_cumulative_volume - self.start_cumulative_volume)
        return {
            "date": self.slot_end,
            "open": float(self.open),
            "high": float(self.high),
            "low": float(self.low),
            "close": float(self.close),
            "volume": float(volume),
            "gap_filled": 0,
            "_volume_valid": bool(self.volume_valid),
            "_tick_count": int(self.tick_count),
            "_source": "kiteticker",
        }


class TickAggregator:
    """Thread-safe tick-to-candle accumulator shared by all eight sockets."""

    def __init__(self, token_to_symbol: dict[int, str]):
        self.token_to_symbol = dict(token_to_symbol)
        self._lock = threading.RLock()
        self._candles: dict[tuple[int, datetime], MutableCandle] = {}
        self._latest_price: dict[int, float] = {}
        self._latest_tick_at: dict[int, datetime] = {}
        self._last_cumulative_volume: dict[int, int] = {}
        self._last_trade_key: dict[int, tuple[str, int, float]] = {}
        self._accepted_ticks = 0
        self._ignored_ticks = 0

    def ingest(
        self,
        ticks: Iterable[dict[str, Any]],
        *,
        received_at: Optional[datetime] = None,
    ) -> None:
        recv = received_at or now_ist()
        with self._lock:
            for tick in ticks:
                try:
                    token = int(tick.get("instrument_token"))
                    price = float(tick.get("last_price"))
                except (TypeError, ValueError):
                    self._ignored_ticks += 1
                    continue
                if token not in self.token_to_symbol or not math.isfinite(price) or price <= 0:
                    self._ignored_ticks += 1
                    continue

                trade_ts = _coerce_ist(
                    tick.get("last_trade_time")
                    or tick.get("exchange_timestamp"),
                    recv,
                )
                if trade_ts is None:
                    self._ignored_ticks += 1
                    continue

                try:
                    cumulative_volume = max(0, int(tick.get("volume_traded", 0) or 0))
                except (TypeError, ValueError):
                    cumulative_volume = 0

                self._latest_price[token] = price
                self._latest_tick_at[token] = trade_ts
                previous_volume = self._last_cumulative_volume.get(token)
                if previous_volume is not None and cumulative_volume < previous_volume:
                    previous_volume = 0
                trade_key = (trade_ts.isoformat(), cumulative_volume, price)
                if self._last_trade_key.get(token) == trade_key:
                    self._ignored_ticks += 1
                    continue
                self._last_trade_key[token] = trade_key

                market_open_dt = IST.localize(datetime.combine(trade_ts.date(), MARKET_OPEN))
                market_close_dt = IST.localize(datetime.combine(trade_ts.date(), MARKET_CLOSE))
                if trade_ts < market_open_dt or trade_ts >= market_close_dt:
                    self._last_cumulative_volume[token] = cumulative_volume
                    self._ignored_ticks += 1
                    continue

                slot_end = _slot_end_for_trade(trade_ts)
                if slot_end > market_close_dt:
                    self._last_cumulative_volume[token] = cumulative_volume
                    self._ignored_ticks += 1
                    continue

                # A pre-open subscription normally establishes the zero-volume
                # baseline. If the first observed trade is in the first regular
                # candle, zero remains a valid session baseline. Mid-session
                # starts are flagged for REST repair.
                volume_valid = previous_volume is not None or slot_end.time() == dtime(9, 20)
                start_volume = previous_volume if previous_volume is not None else 0
                key = (token, slot_end)
                candle = self._candles.get(key)
                if candle is None:
                    self._candles[key] = MutableCandle(
                        slot_end=slot_end,
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        start_cumulative_volume=int(start_volume),
                        last_cumulative_volume=cumulative_volume,
                        volume_valid=volume_valid,
                    )
                else:
                    candle.update(price, cumulative_volume)
                    candle.volume_valid = candle.volume_valid and volume_valid

                self._last_cumulative_volume[token] = cumulative_volume
                self._accepted_ticks += 1

    def rows_for_slot(self, slot_end: datetime) -> dict[int, dict[str, Any]]:
        target = slot_end.astimezone(IST)
        with self._lock:
            return {
                token: candle.to_row()
                for (token, candle_slot), candle in self._candles.items()
                if candle_slot == target
            }

    def latest_price(self, token: int) -> Optional[float]:
        with self._lock:
            return self._latest_price.get(int(token))

    def latest_tick_at(self, token: int) -> Optional[datetime]:
        with self._lock:
            return self._latest_tick_at.get(int(token))

    def discard_through(self, slot_end: datetime) -> None:
        target = slot_end.astimezone(IST)
        with self._lock:
            stale = [key for key in self._candles if key[1] <= target]
            for key in stale:
                self._candles.pop(key, None)

    def telemetry(self) -> dict[str, int]:
        with self._lock:
            return {
                "accepted_ticks": int(self._accepted_ticks),
                "ignored_ticks": int(self._ignored_ticks),
                "latest_price_count": int(len(self._latest_price)),
                "open_candle_count": int(len(self._candles)),
            }


@dataclass
class AppStreamState:
    app_name: str
    assigned_symbols: int
    assigned_tokens: int
    connected: bool = False
    subscribed_tokens: int = 0
    connected_since: Optional[datetime] = None
    last_disconnect_at: Optional[datetime] = None
    last_tick_at: Optional[datetime] = None
    reconnect_count: int = 0
    last_error: str = ""
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def mark_connected(self, when: Optional[datetime] = None) -> None:
        with self.lock:
            self.connected = True
            self.connected_since = when or now_ist()
            self.last_error = ""

    def mark_disconnected(self, reason: str, when: Optional[datetime] = None) -> None:
        with self.lock:
            self.connected = False
            self.last_disconnect_at = when or now_ist()
            self.last_error = str(reason)

    def mark_tick(self, when: Optional[datetime] = None) -> None:
        with self.lock:
            self.last_tick_at = when or now_ist()

    def covers(self, window_start: datetime) -> bool:
        with self.lock:
            if not self.connected or self.connected_since is None:
                return False
            return self.connected_since <= window_start

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "connected": bool(self.connected),
                "assigned_symbols": int(self.assigned_symbols),
                "assigned_tokens": int(self.assigned_tokens),
                "subscribed_tokens": int(self.subscribed_tokens),
                "connected_since_ist": (
                    self.connected_since.strftime("%Y-%m-%d %H:%M:%S%z")
                    if self.connected_since
                    else ""
                ),
                "last_disconnect_at_ist": (
                    self.last_disconnect_at.strftime("%Y-%m-%d %H:%M:%S%z")
                    if self.last_disconnect_at
                    else ""
                ),
                "last_tick_at_ist": (
                    self.last_tick_at.strftime("%Y-%m-%d %H:%M:%S%z")
                    if self.last_tick_at
                    else ""
                ),
                "reconnect_count": int(self.reconnect_count),
                "last_error": str(self.last_error),
            }


class KiteTickerApp:
    def __init__(
        self,
        credential: AppCredential,
        symbols: list[str],
        token_map: dict[str, int],
        aggregator: TickAggregator,
        logger: logging.Logger,
    ):
        self.credential = credential
        self.symbols = list(symbols)
        self.tokens = [int(token_map[symbol]) for symbol in self.symbols]
        self.aggregator = aggregator
        self.logger = logger
        self.state = AppStreamState(
            app_name=credential.app_name,
            assigned_symbols=len(self.symbols),
            assigned_tokens=len(self.tokens),
        )
        self.ticker: Optional[KiteTicker] = None
        self.rest_client: Optional[KiteConnect] = None

    def start(self) -> None:
        kws = KiteTicker(
            self.credential.api_key,
            self.credential.access_token,
            reconnect=True,
            reconnect_max_tries=50,
            reconnect_max_delay=60,
        )

        def on_connect(ws: KiteTicker, _response: Any) -> None:
            self.state.mark_connected()
            ws.subscribe(self.tokens)
            ws.set_mode(ws.MODE_FULL, self.tokens)
            with self.state.lock:
                self.state.subscribed_tokens = len(self.tokens)
            self.logger.info(
                "[APP] %s connected and subscribed in MODE_FULL | symbols=%d",
                self.credential.app_name,
                len(self.tokens),
            )

        def on_ticks(_ws: KiteTicker, ticks: list[dict[str, Any]]) -> None:
            received = now_ist()
            self.aggregator.ingest(ticks, received_at=received)
            self.state.mark_tick(received)

        def on_close(_ws: KiteTicker, code: Any, reason: Any) -> None:
            self.state.mark_disconnected(f"close code={code} reason={reason}")
            self.logger.warning(
                "[APP] %s disconnected | code=%s reason=%s",
                self.credential.app_name,
                code,
                reason,
            )

        def on_error(_ws: KiteTicker, code: Any, reason: Any) -> None:
            with self.state.lock:
                self.state.last_error = f"error code={code} reason={reason}"
            self.logger.error(
                "[APP] %s websocket error | code=%s reason=%s",
                self.credential.app_name,
                code,
                reason,
            )

        def on_reconnect(_ws: KiteTicker, attempts_count: int) -> None:
            with self.state.lock:
                self.state.reconnect_count = int(attempts_count)
            self.logger.warning(
                "[APP] %s reconnect attempt=%s",
                self.credential.app_name,
                attempts_count,
            )

        kws.on_connect = on_connect
        kws.on_ticks = on_ticks
        kws.on_close = on_close
        kws.on_error = on_error
        kws.on_reconnect = on_reconnect
        self.ticker = kws
        kws.connect(threaded=True)

    def get_rest_client(self) -> KiteConnect:
        if self.rest_client is None:
            self.rest_client = _rest_client(self.credential)
        return self.rest_client

    def close(self) -> None:
        if self.ticker is None:
            return
        try:
            self.ticker.close(code=1000, reason="eqidv2 shutdown")
        except Exception:
            pass


def _read_previous_close(ticker: str, output_dir: Path, seed_dir: Path) -> Optional[float]:
    for base in (output_dir, seed_dir):
        path = base / f"{ticker}{END_5M}"
        existing = core._load_existing_ohlc(str(path), "end", "5min")
        if existing is None or existing.empty or "close" not in existing.columns:
            continue
        try:
            value = float(pd.to_numeric(existing["close"], errors="coerce").dropna().iloc[-1])
        except Exception:
            continue
        if math.isfinite(value) and value > 0:
            return value
    return None


def _load_symbol_history(ticker: str, output_dir: Path, seed_dir: Path) -> pd.DataFrame:
    output_path = output_dir / f"{ticker}{END_5M}"
    existing = core._load_existing_ohlc(str(output_path), "end", "5min")
    if existing is not None and not existing.empty:
        return existing
    seed_path = seed_dir / f"{ticker}{END_5M}"
    return core._load_existing_ohlc(str(seed_path), "end", "5min")


def _persist_symbol_candle(
    ticker: str,
    slot_end: datetime,
    row: Optional[dict[str, Any]],
    *,
    fallback_price: Optional[float],
    output_dir: Path,
    seed_dir: Path,
    logger: logging.Logger,
) -> dict[str, Any]:
    started = time.perf_counter()
    source = str((row or {}).get("_source", "synthetic_no_trade"))
    history = _load_symbol_history(ticker, output_dir, seed_dir)

    if row is None:
        price = fallback_price
        if price is None or not math.isfinite(float(price)) or float(price) <= 0:
            price = _read_previous_close(ticker, output_dir, seed_dir)
        if price is None:
            return {
                "ticker": ticker,
                "ok": False,
                "source": source,
                "error": "no_stream_row_or_previous_close",
                "elapsed_sec": time.perf_counter() - started,
            }
        row = {
            "date": slot_end,
            "open": float(price),
            "high": float(price),
            "low": float(price),
            "close": float(price),
            "volume": 0.0,
            "gap_filled": 1,
            "_source": "synthetic_no_trade",
        }
        source = "synthetic_no_trade"

    clean_row = {
        "date": pd.Timestamp(row["date"]),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row.get("volume", 0.0) or 0.0),
        "gap_filled": int(row.get("gap_filled", 0) or 0),
    }
    incoming = pd.DataFrame([clean_row])
    incoming["date"] = core._to_ist(incoming["date"])

    merged = incoming
    if history is not None and not history.empty:
        merged = pd.concat([history, incoming], ignore_index=True)
    merged = (
        merged.drop_duplicates(subset="date", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    merged, _ = core._apply_synthetic_5min_gap_fill(
        merged,
        slot_end,
        ticker,
        logger,
    )
    merged = core._trim_live_5min_history(merged)
    merged = core._compute_common_features(merged, "5min")
    if getattr(core, "DEFAULT_DOWNCAST_NUMERIC", False):
        merged = core._downcast_numeric_columns(merged)

    output_path = output_dir / f"{ticker}{END_5M}"
    core._finalize_and_save(merged, str(output_path))
    last_ts = core._coerce_ist_datetime(merged["date"].iloc[-1])
    missing_session = core._missing_5min_session_stamps_from_df(merged, slot_end)
    ok = bool(last_ts is not None and last_ts >= pd.Timestamp(slot_end) and not missing_session)
    return {
        "ticker": ticker,
        "ok": ok,
        "source": source,
        "error": "" if ok else "post_write_exact_slot_verification_failed",
        "elapsed_sec": time.perf_counter() - started,
    }


def _fetch_rest_repair_row(
    app: KiteTickerApp,
    ticker: str,
    token: int,
    slot_start: datetime,
    slot_end: datetime,
    logger: logging.Logger,
) -> tuple[str, Optional[dict[str, Any]], str]:
    try:
        frame = core.fetch_historical_5min_df(
            app.get_rest_client(),
            int(token),
            slot_start,
            slot_end,
            logger,
            "end",
        )
        if frame is None or frame.empty:
            return ticker, None, "rest_returned_no_rows"
        frame = frame.copy()
        frame["date"] = core._to_ist(frame["date"])
        target = pd.Timestamp(slot_end)
        selected = frame.loc[frame["date"] == target]
        if selected.empty:
            return ticker, None, "rest_exact_slot_missing"
        last = selected.iloc[-1]
        return ticker, {
            "date": target,
            "open": float(last["open"]),
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
            "volume": float(last.get("volume", 0.0) or 0.0),
            "gap_filled": 0,
            "_source": "rest_repair",
            "_volume_valid": True,
        }, ""
    except Exception as exc:
        return ticker, None, f"{type(exc).__name__}: {exc}"


class KiteTickerFiveMinuteFeed:
    def __init__(
        self,
        universe: list[str],
        token_map: dict[str, int],
        credentials: dict[str, AppCredential],
        *,
        output_dir: Path,
        seed_dir: Path,
        boundary_grace_sec: float,
        write_workers: int,
        rest_repair_enabled: bool,
        rest_repair_workers_per_app: int,
        logger: logging.Logger,
    ):
        self.universe = _canonical_symbols(universe)
        self.token_map = {symbol: int(token_map[symbol]) for symbol in self.universe}
        self.token_to_symbol = {token: symbol for symbol, token in self.token_map.items()}
        if len(self.token_to_symbol) != len(self.token_map):
            raise RuntimeError("Instrument token map is not one-to-one.")
        self.credentials = dict(credentials)
        self.output_dir = Path(output_dir)
        self.seed_dir = Path(seed_dir)
        self.boundary_grace_sec = max(0.0, float(boundary_grace_sec))
        self.write_workers = max(1, int(write_workers))
        self.rest_repair_enabled = bool(rest_repair_enabled)
        self.rest_repair_workers_per_app = max(1, int(rest_repair_workers_per_app))
        self.logger = logger
        self.aggregator = TickAggregator(self.token_to_symbol)
        self.stop_event = threading.Event()
        self.partitions = split_tickers_evenly(self.universe, len(APP_NAMES))
        self.apps: dict[str, KiteTickerApp] = {}
        for idx, app_name in enumerate(APP_NAMES):
            self.apps[app_name] = KiteTickerApp(
                credentials[app_name],
                self.partitions[idx],
                self.token_map,
                self.aggregator,
                logger,
            )
        self.last_slot_summary: dict[str, Any] = {}
        self.next_slot_end: Optional[datetime] = None
        self.last_status_write_monotonic = 0.0
        self.started_at = now_ist()
        self.universe_hash = _universe_sha256(self.universe)

    def configuration_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "eqidv2_kiteticker_5m_runtime_v1",
            "component": "kiteticker_5min_feed",
            "mode": "shadow",
            "created_at_ist": now_ist().isoformat(),
            "pid": int(os.getpid()),
            "python_executable": sys.executable,
            "source_file": str(Path(__file__).resolve()),
            "output_dir": str(self.output_dir),
            "seed_data_dir": str(self.seed_dir),
            "interval_min": INTERVAL_MINUTES,
            "boundary_grace_sec": float(self.boundary_grace_sec),
            "write_workers": int(self.write_workers),
            "rest_repair_enabled": bool(self.rest_repair_enabled),
            "rest_repair_workers_per_app": int(self.rest_repair_workers_per_app),
            "universe_count": int(len(self.universe)),
            "universe_sha256": self.universe_hash,
            "app_assignments": {
                app_name: {
                    "symbol_count": len(self.apps[app_name].symbols),
                    "symbols_sha256": _universe_sha256(self.apps[app_name].symbols),
                }
                for app_name in APP_NAMES
            },
        }

    def publish_manifests(self) -> Path:
        payload = self.configuration_payload()
        stamp = now_ist().strftime("%Y%m%d_%H%M%S_%f")
        runtime_manifest = RUNTIME_MANIFEST_DIR / f"{stamp}_pid{os.getpid()}.json"
        _atomic_write_json(runtime_manifest, payload)
        universe_payload = {
            "schema_version": "eqidv2_5m_feed_universe_v1",
            "feed_kind": "kiteticker_shadow",
            "slot_ist": "",
            "published_at_ist": now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
            "universe_count": len(self.universe),
            "universe_sha256": self.universe_hash,
            "symbols": self.universe,
            "app_assignment_counts": {
                app_name: len(self.apps[app_name].symbols) for app_name in APP_NAMES
            },
            "runtime_manifest_path": str(runtime_manifest),
        }
        _atomic_write_json(UNIVERSE_MANIFEST_PATH, universe_payload)
        return runtime_manifest

    def start_streams(self) -> None:
        for app_name in APP_NAMES:
            self.apps[app_name].start()
            time.sleep(0.10)

    def close_streams(self) -> None:
        for app_name in APP_NAMES:
            self.apps[app_name].close()

    def _status_payload(self, state: str) -> dict[str, Any]:
        now = now_ist()
        app_states = {name: app.state.snapshot() for name, app in self.apps.items()}
        connected = sum(1 for value in app_states.values() if value["connected"])
        next_slot = self.next_slot_end
        seconds_to_next = (
            max(0.0, (next_slot - now).total_seconds()) if next_slot is not None else None
        )
        payload: dict[str, Any] = {
            "schema_version": "eqidv2_kiteticker_5m_status_v1",
            "state": state,
            "overall_state": (
                self.last_slot_summary.get("overall_state", state)
                if self.last_slot_summary
                else state
            ),
            "updated_at_ist": now.strftime("%Y-%m-%d %H:%M:%S%z"),
            "started_at_ist": self.started_at.strftime("%Y-%m-%d %H:%M:%S%z"),
            "mode": "shadow",
            "first_slot": "09:15",
            "end_cutoff": "15:30",
            "interval_min": INTERVAL_MINUTES,
            "next_slot_ist": (
                next_slot.strftime("%Y-%m-%d %H:%M:%S%z") if next_slot else ""
            ),
            "seconds_to_next_slot": seconds_to_next,
            "connected_apps": connected,
            "configured_apps": len(APP_NAMES),
            "universe_count": len(self.universe),
            "universe_sha256": self.universe_hash,
            "write_workers": self.write_workers,
            "total_worker_budget": self.write_workers,
            "per_app_cap": self.rest_repair_workers_per_app,
            "effective_per_app": max(1, math.ceil(self.write_workers / len(APP_NAMES))),
            "intraday_ts": "end",
            "completion_policy": "exact_current_slot_only",
            "rest_repair_enabled": self.rest_repair_enabled,
            "output_dir": str(self.output_dir),
            "app_states": app_states,
            "partition_symbol_counts": {
                name: len(app.symbols) for name, app in self.apps.items()
            },
            "aggregator": self.aggregator.telemetry(),
            "last_slot_summary": self.last_slot_summary,
        }
        if self.last_slot_summary:
            payload.update(self.last_slot_summary)
            # The slot summary has its own completion timestamp. Keep that
            # value nested, but never let it replace the top-level heartbeat
            # consumed by the freshness supervisor.
            payload["updated_at_ist"] = now.strftime("%Y-%m-%d %H:%M:%S%z")
        return payload

    def write_status(self, state: str, *, force: bool = False) -> None:
        now_mono = time.monotonic()
        if (
            not force
            and now_mono - self.last_status_write_monotonic < DEFAULT_STATUS_HEARTBEAT_SEC
        ):
            return
        _atomic_write_json(STATUS_PATH, self._status_payload(state))
        self.last_status_write_monotonic = now_mono

    def _repair_uncovered_apps(
        self,
        uncovered_apps: list[str],
        slot_start: datetime,
        slot_end: datetime,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
        repaired: dict[str, dict[str, Any]] = {}
        failures: dict[str, str] = {}
        if not uncovered_apps or not self.rest_repair_enabled:
            return repaired, failures

        def repair_one_app(
            app_name: str,
        ) -> tuple[str, dict[str, dict[str, Any]], dict[str, str]]:
            app = self.apps[app_name]
            workers = min(self.rest_repair_workers_per_app, max(1, len(app.symbols)))
            app_repaired: dict[str, dict[str, Any]] = {}
            app_failures: dict[str, str] = {}
            with ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix=f"{app_name}-rest-repair",
            ) as pool:
                futures = {
                    pool.submit(
                        _fetch_rest_repair_row,
                        app,
                        ticker,
                        self.token_map[ticker],
                        slot_start,
                        slot_end,
                        self.logger,
                    ): ticker
                    for ticker in app.symbols
                }
                for future in as_completed(futures):
                    ticker, row, error = future.result()
                    if row is not None:
                        app_repaired[ticker] = row
                    else:
                        app_failures[ticker] = error
            return app_name, app_repaired, app_failures

        # Each app has its own credentials, HTTP session, and rate limit. Repair
        # the eight partitions concurrently just like the production REST feed,
        # while preserving the per-app worker ceiling.
        with ThreadPoolExecutor(
            max_workers=len(uncovered_apps),
            thread_name_prefix="kiteticker-rest-repair-app",
        ) as app_pool:
            app_futures = {
                app_pool.submit(repair_one_app, app_name): app_name
                for app_name in uncovered_apps
            }
            for future in as_completed(app_futures):
                app_name, app_repaired, app_failures = future.result()
                repaired.update(app_repaired)
                failures.update(app_failures)
                self.logger.info(
                    "[REPAIR] %s complete | repaired=%d failed=%d",
                    app_name,
                    len(app_repaired),
                    len(app_failures),
                )
                self.write_status("REPAIRING", force=True)
        return repaired, failures

    def seal_slot(self, slot_end: datetime) -> dict[str, Any]:
        started = time.perf_counter()
        slot_end = slot_end.astimezone(IST)
        opening_snapshot = slot_end.time() == MARKET_OPEN
        slot_start = (
            slot_end if opening_snapshot else slot_end - timedelta(minutes=INTERVAL_MINUTES)
        )
        rows_by_token = self.aggregator.rows_for_slot(slot_end)
        uncovered_apps = [
            app_name
            for app_name, app in self.apps.items()
            if not app.state.covers(slot_start)
        ]

        repaired_rows: dict[str, dict[str, Any]] = {}
        repair_failures: dict[str, str] = {}
        if uncovered_apps and not opening_snapshot:
            self.logger.warning(
                "[SLOT] %s stream coverage gap in %s; invoking REST repair.",
                slot_end.strftime("%H:%M"),
                ",".join(uncovered_apps),
            )
            repaired_rows, repair_failures = self._repair_uncovered_apps(
                uncovered_apps,
                slot_start,
                slot_end,
            )

        rows_by_symbol: dict[str, Optional[dict[str, Any]]] = {}
        source_by_symbol: dict[str, str] = {}
        unresolved: dict[str, str] = {}
        for app_name, app in self.apps.items():
            app_covered = app_name not in uncovered_apps
            for ticker in app.symbols:
                token = self.token_map[ticker]
                if ticker in repaired_rows:
                    rows_by_symbol[ticker] = repaired_rows[ticker]
                    source_by_symbol[ticker] = "rest_repair"
                    continue

                stream_row = rows_by_token.get(token)
                if opening_snapshot:
                    if not app_covered:
                        unresolved[ticker] = "opening_snapshot_stream_not_covered"
                        continue
                    price = self.aggregator.latest_price(token)
                    if price is None:
                        rows_by_symbol[ticker] = None
                        source_by_symbol[ticker] = "synthetic_no_trade"
                    else:
                        rows_by_symbol[ticker] = {
                            "date": slot_end,
                            "open": price,
                            "high": price,
                            "low": price,
                            "close": price,
                            "volume": 0.0,
                            "gap_filled": 0,
                            "_source": "kiteticker_opening_snapshot",
                        }
                        source_by_symbol[ticker] = "kiteticker_opening_snapshot"
                    continue

                if not app_covered:
                    unresolved[ticker] = repair_failures.get(
                        ticker,
                        "stream_gap_and_rest_repair_unavailable",
                    )
                    continue
                if stream_row is not None and not bool(stream_row.get("_volume_valid", False)):
                    unresolved[ticker] = "stream_volume_baseline_invalid"
                    continue
                rows_by_symbol[ticker] = stream_row
                source_by_symbol[ticker] = (
                    str(stream_row.get("_source")) if stream_row is not None else "synthetic_no_trade"
                )

        results: list[dict[str, Any]] = []
        eligible = sorted(rows_by_symbol)
        with ThreadPoolExecutor(
            max_workers=min(self.write_workers, max(1, len(eligible))),
            thread_name_prefix="kiteticker-5m-writer",
        ) as pool:
            futures = {}
            for ticker in eligible:
                token = self.token_map[ticker]
                futures[
                    pool.submit(
                        _persist_symbol_candle,
                        ticker,
                        slot_end,
                        rows_by_symbol[ticker],
                        fallback_price=self.aggregator.latest_price(token),
                        output_dir=self.output_dir,
                        seed_dir=self.seed_dir,
                        logger=self.logger,
                    )
                ] = ticker
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(
                        {
                            "ticker": ticker,
                            "ok": False,
                            "source": source_by_symbol.get(ticker, ""),
                            "error": f"{type(exc).__name__}: {exc}",
                            "elapsed_sec": 0.0,
                        }
                    )

        successful = {str(result["ticker"]) for result in results if result.get("ok")}
        failed = {
            str(result["ticker"]): str(result.get("error", "write_failed"))
            for result in results
            if not result.get("ok")
        }
        unresolved.update(failed)
        current_count = len(successful)
        accounting_exact = (
            current_count == len(self.universe)
            and not unresolved
            and successful == set(self.universe)
        )
        elapsed = time.perf_counter() - started
        source_counts: dict[str, int] = {}
        for result in results:
            if not result.get("ok"):
                continue
            source = str(result.get("source") or source_by_symbol.get(str(result["ticker"]), ""))
            source_counts[source] = source_counts.get(source, 0) + 1

        partition_elapsed: dict[str, float] = {}
        for app_name, app in self.apps.items():
            app_results = [
                result for result in results if str(result.get("ticker")) in set(app.symbols)
            ]
            partition_elapsed[app_name] = max(
                [float(result.get("elapsed_sec", 0.0)) for result in app_results] or [0.0]
            )

        overall_state = "OK" if accounting_exact else "FAIL"
        summary: dict[str, Any] = {
            "slot_ist": slot_end.strftime("%Y-%m-%d %H:%M:%S%z"),
            "updated_at_ist": now_ist().strftime("%Y-%m-%d %H:%M:%S%z"),
            "total_elapsed_sec": float(elapsed),
            "partition_elapsed_sec": partition_elapsed,
            "max_partition_elapsed_sec": max(partition_elapsed.values(), default=0.0),
            "avg_partition_elapsed_sec": (
                sum(partition_elapsed.values()) / len(partition_elapsed)
                if partition_elapsed
                else 0.0
            ),
            "min_partition_elapsed_sec": min(partition_elapsed.values(), default=0.0),
            "universe_count": len(self.universe),
            "universe_sha256": self.universe_hash,
            "current_symbol_count": current_count,
            "previous_slot_symbol_count": 0,
            "complete_symbol_count": current_count,
            "written_symbol_count": current_count,
            "noop_symbol_count": 0,
            "unresolved_symbol_count": len(unresolved),
            "failed_symbol_count": len(failed),
            "token_missing_symbol_count": 0,
            "accounting_exact": accounting_exact,
            "overall_state": overall_state,
            "stream_uncovered_apps": uncovered_apps,
            "source_counts": source_counts,
            "rest_repair_failure_count": len(repair_failures),
            "verification_failed_count": len(unresolved),
            "verification_failure_sample": [
                f"{ticker}:{reason}" for ticker, reason in sorted(unresolved.items())[:25]
            ],
            "failures": [
                f"{ticker}:{reason}" for ticker, reason in sorted(unresolved.items())[:25]
            ],
        }
        self.last_slot_summary = summary

        marker = {
            "schema_version": "eqidv2_kiteticker_5m_slot_complete_v1",
            "feed_kind": "kiteticker_shadow",
            "slot_ist": summary["slot_ist"],
            "published_at_ist": summary["updated_at_ist"],
            "complete": accounting_exact,
            "universe_count": len(self.universe),
            "universe_sha256": self.universe_hash,
            "current_symbol_count": current_count,
            "unresolved_symbol_count": len(unresolved),
            "duration_ms": elapsed * 1000.0,
            "source_counts": source_counts,
            "output_dir": str(self.output_dir),
        }
        marker_path = SLOT_MARKER_DIR / f"slot_{slot_end.strftime('%Y%m%d_%H%M')}.json"
        _atomic_write_json(marker_path, marker)

        universe_payload = json.loads(UNIVERSE_MANIFEST_PATH.read_text(encoding="utf-8"))
        universe_payload["slot_ist"] = summary["slot_ist"]
        universe_payload["published_at_ist"] = summary["updated_at_ist"]
        _atomic_write_json(UNIVERSE_MANIFEST_PATH, universe_payload)

        self.write_status(overall_state, force=True)
        self.aggregator.discard_through(slot_end)
        self.logger.info(
            "[READY] KiteTicker shadow slot=%s complete=%s current=%d/%d "
            "unresolved=%d elapsed=%.3fs sources=%s",
            slot_end.strftime("%H:%M"),
            accounting_exact,
            current_count,
            len(self.universe),
            len(unresolved),
            elapsed,
            json.dumps(source_counts, sort_keys=True),
        )
        return summary

    def _wait_until(self, target: datetime, state: str) -> bool:
        while not self.stop_event.is_set():
            remaining = (target - now_ist()).total_seconds()
            if remaining <= 0:
                return True
            self.write_status(state)
            self.stop_event.wait(min(1.0, remaining))
        return False

    def run(self, holidays: set[date]) -> int:
        runtime_manifest = self.publish_manifests()
        self.logger.info(
            "[START] Live Data kiteticker Fetch (5mins) | mode=shadow apps=8 "
            "universe=%d sha256=%s output=%s manifest=%s",
            len(self.universe),
            self.universe_hash[:12],
            self.output_dir,
            runtime_manifest,
        )
        self.write_status("STARTING", force=True)
        self.start_streams()
        try:
            today = now_ist().date()
            if today.weekday() >= 5 or today in holidays:
                self.logger.info("[IDLE] Non-trading day; keeping supervisor heartbeat alive.")
                cutoff = IST.localize(datetime.combine(today, PROCESS_EXIT_TIME))
                self.next_slot_end = None
                self._wait_until(cutoff, "IDLE_NON_TRADING_DAY")
                return 0

            self.next_slot_end = _next_slot_end(now_ist())
            close_dt = IST.localize(datetime.combine(today, MARKET_CLOSE))
            while not self.stop_event.is_set() and self.next_slot_end <= close_dt:
                seal_at = self.next_slot_end + timedelta(seconds=self.boundary_grace_sec)
                self.logger.info(
                    "[WAIT] next KiteTicker slot=%s seal_at=%s",
                    self.next_slot_end.strftime("%H:%M:%S"),
                    seal_at.strftime("%H:%M:%S"),
                )
                if not self._wait_until(seal_at, "RUNNING"):
                    break
                self.seal_slot(self.next_slot_end)
                self.next_slot_end += timedelta(minutes=INTERVAL_MINUTES)

            if not self.stop_event.is_set():
                cutoff = IST.localize(datetime.combine(today, PROCESS_EXIT_TIME))
                self.next_slot_end = None
                self.logger.info("[DONE] Final 15:30 slot sealed; waiting for 15:31 cutoff.")
                self._wait_until(cutoff, "COMPLETED")
            return 0
        finally:
            self.close_streams()
            self.write_status("STOPPED", force=True)


def _configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        force=True,
    )
    return logging.getLogger("eqidv2_kiteticker_5min")


def _read_holidays() -> set[date]:
    holiday_path = str(getattr(core, "HOLIDAYS_FILE_DEFAULT", "holidays.csv"))
    try:
        return set(core._read_holidays(holiday_path))
    except Exception:
        return set()


def validate_configuration(
    logger: Optional[logging.Logger] = None,
) -> dict[str, Any]:
    log = logger or logging.getLogger("eqidv2_kiteticker_5min.validate")
    credentials = load_app_credentials()
    universe, token_map = load_fetchable_universe(
        log,
        credentials,
        allow_instrument_refresh=False,
    )
    partitions = split_tickers_evenly(universe, len(APP_NAMES))
    assigned = _canonical_symbols(symbol for partition in partitions for symbol in partition)
    if assigned != universe:
        raise RuntimeError("Eight-app partitioning does not preserve the canonical universe.")
    counts = [len(partition) for partition in partitions]
    if max(counts, default=0) - min(counts, default=0) > 1:
        raise RuntimeError(f"Eight-app partitioning is imbalanced: {counts}")
    return {
        "ok": True,
        "apps": len(credentials),
        "universe_count": len(universe),
        "token_count": len(token_map),
        "universe_sha256": _universe_sha256(universe),
        "partition_counts": {
            app_name: len(partitions[idx]) for idx, app_name in enumerate(APP_NAMES)
        },
        "output_dir": str(OUTPUT_DIR),
        "seed_data_dir": str(SEED_DATA_DIR),
        "mode": "shadow",
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Persistent eight-app KiteTicker five-minute shadow feed."
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate credentials, tokens, and equal partitioning without opening WebSockets.",
    )
    parser.add_argument(
        "--boundary-grace-sec",
        type=float,
        default=DEFAULT_BOUNDARY_GRACE_SEC,
    )
    parser.add_argument("--write-workers", type=int, default=DEFAULT_WRITE_WORKERS)
    parser.add_argument(
        "--rest-repair-workers-per-app",
        type=int,
        default=DEFAULT_REST_REPAIR_WORKERS_PER_APP,
    )
    parser.add_argument(
        "--disable-rest-repair",
        action="store_true",
        help="Mark stream gaps unresolved instead of repairing them through historical REST.",
    )
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logger = _configure_logging()
    if args.validate_config:
        print(json.dumps(validate_configuration(logger), indent=2, sort_keys=True))
        return 0

    credentials = load_app_credentials()
    universe, token_map = load_fetchable_universe(
        logger,
        credentials,
        allow_instrument_refresh=True,
    )
    feed = KiteTickerFiveMinuteFeed(
        universe,
        token_map,
        credentials,
        output_dir=args.output_dir,
        seed_dir=SEED_DATA_DIR,
        boundary_grace_sec=args.boundary_grace_sec,
        write_workers=args.write_workers,
        rest_repair_enabled=(
            DEFAULT_REST_REPAIR_ENABLED and not bool(args.disable_rest_repair)
        ),
        rest_repair_workers_per_app=args.rest_repair_workers_per_app,
        logger=logger,
    )

    def request_stop(_signum: int, _frame: Any) -> None:
        logger.info("[STOP] Shutdown signal received.")
        feed.stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    return feed.run(_read_holidays())


if __name__ == "__main__":
    raise SystemExit(main())
