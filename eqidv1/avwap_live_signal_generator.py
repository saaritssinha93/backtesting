# -*- coding: utf-8 -*-
"""
avwap_live_signal_generator.py — Live AVWAP Signal Scanner & CSV Writer
========================================================================

Runs continuously during market hours (9:15 AM – 3:30 PM IST).
Every 15 minutes (aligned to candle closes), scans the latest parquet data
for AVWAP entry signals and appends them to a daily CSV file.

Output CSV: live_signals/signals_YYYY-MM-DD.csv
Each row contains:
  - signal_id          : unique identifier for deduplication
  - signal_datetime    : candle datetime from the strategy
  - received_time      : current IST wall-clock time when signal was detected
  - ticker, side, setup, impulse_type, entry_price, stop_price, target_price
  - quality_score, atr_pct, rsi, adx
  - quantity           : computed from position size / entry price

The CSV is appended atomically — trade executors can watch it via watchdog.

Usage:
    python avwap_live_signal_generator.py
    python avwap_live_signal_generator.py --once      # single scan, then exit
    python avwap_live_signal_generator.py --position-size 50000
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, time as dt_time, date
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import pytz

# ============================================================================
# CONSTANTS & CONFIG
# ============================================================================
IST = pytz.timezone("Asia/Kolkata")

MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
SCAN_INTERVAL_MINUTES = 15

# Default position sizing
DEFAULT_POSITION_SIZE_RS = 50_000
DEFAULT_INTRADAY_LEVERAGE = 5.0

# Signal output
SIGNAL_DIR = "live_signals"
SIGNAL_CSV_PATTERN = "signals_{}.csv"
SEEN_SIGNALS_FILE = "live_signals/.seen_signals.json"

# Parquet data directories (relative to script dir)
DIR_15MIN = "stocks_indicators_15min_eq"
DIR_5MIN = "stocks_indicators_5min_eq"

# CSV columns for the signal file
SIGNAL_COLUMNS = [
    "signal_id",
    "signal_datetime",
    "received_time",
    "ticker",
    "side",
    "setup",
    "impulse_type",
    "entry_price",
    "stop_price",
    "target_price",
    "quality_score",
    "atr_pct",
    "rsi",
    "adx",
    "quantity",
]

# ============================================================================
# LOGGING
# ============================================================================
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("live_signal_gen")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(SIGNAL_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(SIGNAL_DIR, "signal_generator.log"),
        mode="a",
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ============================================================================
# SIGNAL DATA CLASS
# ============================================================================
@dataclass
class LiveSignal:
    signal_id: str = ""
    signal_datetime: str = ""
    received_time: str = ""
    ticker: str = ""
    side: str = ""
    setup: str = ""
    impulse_type: str = ""
    entry_price: float = 0.0
    stop_price: float = 0.0
    target_price: float = 0.0
    quality_score: float = 0.0
    atr_pct: float = 0.0
    rsi: float = 0.0
    adx: float = 0.0
    quantity: int = 1


# ============================================================================
# SEEN SIGNALS PERSISTENCE (deduplication across restarts)
# ============================================================================
def load_seen_signals() -> Set[str]:
    if os.path.exists(SEEN_SIGNALS_FILE):
        try:
            with open(SEEN_SIGNALS_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("signals", []))
        except (json.JSONDecodeError, KeyError):
            log.warning("Seen signals file corrupted, starting fresh.")
    return set()


def save_seen_signals(seen: Set[str]) -> None:
    os.makedirs(os.path.dirname(SEEN_SIGNALS_FILE), exist_ok=True)
    # Only keep today's signals to prevent unbounded growth
    with open(SEEN_SIGNALS_FILE, "w") as f:
        json.dump({"date": datetime.now(IST).strftime("%Y-%m-%d"),
                    "signals": list(seen)}, f)


def generate_signal_id(ticker: str, side: str, signal_dt: str) -> str:
    """Deterministic signal ID from ticker + side + signal_datetime."""
    raw = f"{ticker}|{side}|{signal_dt}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ============================================================================
# PARQUET READER
# ============================================================================
def read_parquet_safe(path: str) -> pd.DataFrame:
    try:
        p = Path(path)
        if not p.exists():
            return pd.DataFrame()
        df = pd.read_parquet(p, engine="pyarrow")
        if df.empty:
            return df
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        elif df.index.name == "datetime" or isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        return df
    except Exception as e:
        log.debug(f"Failed to read {path}: {e}")
        return pd.DataFrame()


def list_tickers(data_dir: str, suffix: str = ".parquet") -> List[str]:
    d = Path(data_dir)
    if not d.is_dir():
        return []
    tickers = []
    for f in d.iterdir():
        if f.name.endswith(suffix):
            name = f.stem
            # Strip common suffixes
            for tag in ["_stocks_indicators_15min", "_stocks_indicators_5min"]:
                if name.endswith(tag):
                    name = name[: -len(tag)]
            tickers.append(name)
    return sorted(set(tickers))


# ============================================================================
# AVWAP SIGNAL DETECTION (self-contained fallback)
# ============================================================================
# This implements the core AVWAP mean-reversion signal logic when the
# avwap_v11_refactored package is not available. It detects:
#   SHORT: price breaks below anchored VWAP support
#   LONG:  price bounces above anchored VWAP resistance
#
# The indicator columns (RSI, ATR, ADX, VWAP, EMA, etc.) are expected to
# already exist in the parquet files produced by the data fetcher.

def _col(df: pd.DataFrame, *names: str) -> Optional[pd.Series]:
    """Return the first matching column (case-insensitive)."""
    cols_lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols_lower:
            return df[cols_lower[n.lower()]]
    return None


def _last_n_rows(df: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    """Get the last n rows of a datetime-sorted DataFrame."""
    if "datetime" in df.columns:
        df = df.sort_values("datetime")
    return df.tail(n)


def detect_signals_for_ticker(
    ticker: str,
    df_15m: pd.DataFrame,
    today: date,
    position_size_rs: float = DEFAULT_POSITION_SIZE_RS,
) -> List[LiveSignal]:
    """
    Detect AVWAP entry signals from the latest 15-min candles for a ticker.
    Returns a list of LiveSignal objects for signals generated today.
    """
    if df_15m.empty or "datetime" not in df_15m.columns:
        return []

    # Filter to today's candles only
    df_15m["datetime"] = pd.to_datetime(df_15m["datetime"], errors="coerce")
    df_15m = df_15m.dropna(subset=["datetime"])

    if df_15m["datetime"].dt.tz is None:
        df_15m["datetime"] = df_15m["datetime"].dt.tz_localize(IST)
    else:
        df_15m["datetime"] = df_15m["datetime"].dt.tz_convert(IST)

    today_mask = df_15m["datetime"].dt.date == today
    df_today = df_15m[today_mask].copy()
    if df_today.empty:
        return []

    # Need at least 2 candles for comparison
    if len(df_today) < 2:
        return []

    # Get indicator columns
    close = _col(df_today, "close", "Close")
    high = _col(df_today, "high", "High")
    low = _col(df_today, "low", "Low")
    vwap = _col(df_today, "vwap", "VWAP", "vwap_cumulative")
    rsi_col = _col(df_today, "rsi", "RSI", "rsi_14")
    atr_col = _col(df_today, "atr", "ATR", "atr_14")
    adx_col = _col(df_today, "adx", "ADX", "adx_14")
    ema20 = _col(df_today, "ema_20", "ema20", "EMA_20")
    volume = _col(df_today, "volume", "Volume")

    if close is None or vwap is None:
        return []

    signals: List[LiveSignal] = []
    now_ist = datetime.now(IST)

    # Only check the latest completed candle
    latest_idx = df_today.index[-1]
    prev_idx = df_today.index[-2]

    c = float(close.iloc[-1])
    c_prev = float(close.iloc[-2])
    v = float(vwap.iloc[-1]) if vwap is not None else c
    h = float(high.iloc[-1]) if high is not None else c
    l = float(low.iloc[-1]) if low is not None else c
    candle_dt = df_today["datetime"].iloc[-1]

    rsi_val = float(rsi_col.iloc[-1]) if rsi_col is not None else 50.0
    atr_val = float(atr_col.iloc[-1]) if atr_col is not None else c * 0.02
    adx_val = float(adx_col.iloc[-1]) if adx_col is not None else 25.0
    ema20_val = float(ema20.iloc[-1]) if ema20 is not None else c
    atr_pct = atr_val / c if c > 0 else 0.0

    # Quality score: higher ADX, appropriate RSI, reasonable ATR% → better quality
    quality = 0.0
    if adx_val > 25:
        quality += 0.3
    if adx_val > 40:
        quality += 0.2
    if 0.002 < atr_pct < 0.02:
        quality += 0.2
    if abs(c - ema20_val) / c < 0.03:
        quality += 0.15
    quality = round(min(quality + 0.15, 1.0), 4)

    # VWAP distance in ATR units
    vwap_dist_atr = abs(c - v) / atr_val if atr_val > 0 else 0

    # Determine impulse type from candle size
    candle_body_pct = abs(c - float(close.iloc[-2])) / c_prev if c_prev > 0 else 0
    if candle_body_pct > 0.03:
        impulse = "HUGE"
    else:
        impulse = "MODERATE"

    # ---- SHORT SIGNAL DETECTION ----
    # Conditions: price breaks below VWAP, bearish candle, RSI not oversold
    short_conditions = (
        c < v                           # close below VWAP
        and c < c_prev                  # bearish candle
        and rsi_val < 45                # not overbought, momentum downward
        and rsi_val > 15                # not extremely oversold
        and adx_val > 20                # some trend strength
        and vwap_dist_atr > 0.3         # meaningful distance from VWAP
    )

    if short_conditions:
        # Determine setup type
        if impulse == "HUGE":
            setup = "B_HUGE_RED_FAILED_BOUNCE"
        elif vwap_dist_atr > 1.0:
            setup = "A_MOD_BREAK_C1_LOW"
        else:
            setup = "A_PULLBACK_C2_THEN_BREAK_C2_LOW"

        # Stop loss: entry + ATR * factor (above entry for short)
        sl_factor = 0.75
        stop = round(c + atr_val * sl_factor, 2)
        # Target: entry - ATR * factor (below entry for short)
        tgt_factor = 1.2
        target = round(c - atr_val * tgt_factor, 2)
        # Quantity from position size
        qty = max(1, int(position_size_rs / c)) if c > 0 else 1

        sig_dt_str = candle_dt.strftime("%Y-%m-%d %H:%M:%S%z")
        sig_id = generate_signal_id(ticker, "SHORT", sig_dt_str)

        signals.append(LiveSignal(
            signal_id=sig_id,
            signal_datetime=sig_dt_str,
            received_time=now_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
            ticker=ticker,
            side="SHORT",
            setup=setup,
            impulse_type=impulse,
            entry_price=round(c, 2),
            stop_price=stop,
            target_price=target,
            quality_score=quality,
            atr_pct=round(atr_pct, 6),
            rsi=round(rsi_val, 2),
            adx=round(adx_val, 2),
            quantity=qty,
        ))

    # ---- LONG SIGNAL DETECTION ----
    # Conditions: price bounces above VWAP, bullish candle, RSI not overbought
    long_conditions = (
        c > v                           # close above VWAP
        and c > c_prev                  # bullish candle
        and rsi_val > 55                # momentum upward
        and rsi_val < 85                # not extremely overbought
        and adx_val > 20                # some trend strength
        and vwap_dist_atr > 0.3         # meaningful distance from VWAP
    )

    if long_conditions:
        if impulse == "HUGE":
            setup = "B_HUGE_GREEN_BREAKOUT"
        elif vwap_dist_atr > 1.0:
            setup = "A_MOD_BREAK_C1_HIGH"
        else:
            setup = "A_PULLBACK_C2_THEN_BREAK_C2_HIGH"

        sl_factor = 0.75
        stop = round(c - atr_val * sl_factor, 2)
        tgt_factor = 1.2
        target = round(c + atr_val * tgt_factor, 2)
        qty = max(1, int(position_size_rs / c)) if c > 0 else 1

        sig_dt_str = candle_dt.strftime("%Y-%m-%d %H:%M:%S%z")
        sig_id = generate_signal_id(ticker, "LONG", sig_dt_str)

        signals.append(LiveSignal(
            signal_id=sig_id,
            signal_datetime=sig_dt_str,
            received_time=now_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
            ticker=ticker,
            side="LONG",
            setup=setup,
            impulse_type=impulse,
            entry_price=round(c, 2),
            stop_price=stop,
            target_price=target,
            quality_score=quality,
            atr_pct=round(atr_pct, 6),
            rsi=round(rsi_val, 2),
            adx=round(adx_val, 2),
            quantity=qty,
        ))

    return signals


# ============================================================================
# STRATEGY SCANNER (tries avwap_v11_refactored first, falls back to built-in)
# ============================================================================
def _try_import_strategy():
    """Attempt to import the full AVWAP strategy modules."""
    try:
        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parent
        for p in [str(script_dir), str(project_root)]:
            if p not in sys.path:
                sys.path.insert(0, p)

        from avwap_v11_refactored.avwap_common import (
            StrategyConfig,
            Trade,
            default_short_config,
            default_long_config,
            read_15m_parquet,
        )
        from avwap_v11_refactored.avwap_short_strategy import (
            scan_all_days_for_ticker as scan_short,
        )
        from avwap_v11_refactored.avwap_long_strategy import (
            scan_all_days_for_ticker as scan_long,
        )
        log.info("Loaded avwap_v11_refactored strategy modules.")
        return {
            "scan_short": scan_short,
            "scan_long": scan_long,
            "default_short_config": default_short_config,
            "default_long_config": default_long_config,
            "read_15m_parquet": read_15m_parquet,
            "Trade": Trade,
            "StrategyConfig": StrategyConfig,
        }
    except ImportError:
        log.info("avwap_v11_refactored not available; using built-in signal detection.")
        return None


def scan_all_tickers(
    data_dir: str,
    today: date,
    position_size_rs: float,
    strategy_modules: Optional[dict] = None,
) -> List[LiveSignal]:
    """
    Scan all tickers in data_dir for today's signals.
    Uses avwap_v11_refactored if available, otherwise built-in detection.
    """
    tickers = list_tickers(data_dir)
    if not tickers:
        log.warning(f"No tickers found in {data_dir}")
        return []

    log.info(f"Scanning {len(tickers)} tickers for signals...")
    all_signals: List[LiveSignal] = []
    now_ist = datetime.now(IST)

    if strategy_modules is not None:
        # Use full strategy via avwap_v11_refactored
        scan_short = strategy_modules["scan_short"]
        scan_long = strategy_modules["scan_long"]
        read_15m = strategy_modules["read_15m_parquet"]
        short_cfg = strategy_modules["default_short_config"]()
        long_cfg = strategy_modules["default_long_config"]()

        for ticker in tickers:
            parquet_path = os.path.join(data_dir, f"{ticker}.parquet")
            if not os.path.exists(parquet_path):
                # Try alternative naming
                for pattern in [
                    f"{ticker}_stocks_indicators_15min.parquet",
                    f"{ticker}_stocks_indicators_5min.parquet",
                ]:
                    alt = os.path.join(data_dir, pattern)
                    if os.path.exists(alt):
                        parquet_path = alt
                        break

            df = read_15m(parquet_path, short_cfg.parquet_engine)
            if df.empty:
                continue

            # Scan for short and long trades
            for scan_fn, side, cfg in [
                (scan_short, "SHORT", short_cfg),
                (scan_long, "LONG", long_cfg),
            ]:
                try:
                    trades = scan_fn(ticker, df, cfg)
                    for t in trades:
                        td = asdict(t)
                        sig_dt = str(td.get("signal_time_ist", td.get("entry_time_ist", "")))
                        # Only keep today's signals
                        try:
                            sig_date = pd.to_datetime(sig_dt).date()
                            if sig_date != today:
                                continue
                        except Exception:
                            continue

                        sig_id = generate_signal_id(ticker, side, sig_dt)
                        entry_price = float(td.get("entry_price", 0))
                        qty = max(1, int(position_size_rs / entry_price)) if entry_price > 0 else 1

                        all_signals.append(LiveSignal(
                            signal_id=sig_id,
                            signal_datetime=sig_dt,
                            received_time=now_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
                            ticker=ticker,
                            side=side,
                            setup=str(td.get("setup", "")),
                            impulse_type=str(td.get("impulse_type", "")),
                            entry_price=round(entry_price, 2),
                            stop_price=round(float(td.get("stop_price", td.get("sl_price", 0))), 2),
                            target_price=round(float(td.get("target_price", 0)), 2),
                            quality_score=round(float(td.get("quality_score", 0)), 4),
                            atr_pct=round(float(td.get("atr_pct_signal", 0)), 6),
                            rsi=round(float(td.get("rsi_signal", 0)), 2),
                            adx=round(float(td.get("adx_signal", 0)), 2),
                            quantity=qty,
                        ))
                except Exception as e:
                    log.debug(f"Strategy scan error for {ticker}/{side}: {e}")

    else:
        # Use built-in signal detection
        for ticker in tickers:
            parquet_path = None
            for pattern in [
                f"{ticker}.parquet",
                f"{ticker}_stocks_indicators_15min.parquet",
            ]:
                candidate = os.path.join(data_dir, pattern)
                if os.path.exists(candidate):
                    parquet_path = candidate
                    break

            if parquet_path is None:
                continue

            df = read_parquet_safe(parquet_path)
            if df.empty:
                continue

            try:
                sigs = detect_signals_for_ticker(ticker, df, today, position_size_rs)
                all_signals.extend(sigs)
            except Exception as e:
                log.debug(f"Signal detection error for {ticker}: {e}")

    log.info(f"Found {len(all_signals)} raw signals.")
    return all_signals


# ============================================================================
# CSV WRITER (atomic append)
# ============================================================================
def append_signals_to_csv(signals: List[LiveSignal], csv_path: str) -> int:
    """
    Append new signals to the daily CSV. Creates file with header if it
    doesn't exist. Returns number of signals written.
    """
    if not signals:
        return 0

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    written = 0
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=SIGNAL_COLUMNS,
            quoting=csv.QUOTE_ALL,
        )
        if not file_exists:
            writer.writeheader()

        for sig in signals:
            writer.writerow({
                "signal_id": sig.signal_id,
                "signal_datetime": sig.signal_datetime,
                "received_time": sig.received_time,
                "ticker": sig.ticker,
                "side": sig.side,
                "setup": sig.setup,
                "impulse_type": sig.impulse_type,
                "entry_price": sig.entry_price,
                "stop_price": sig.stop_price,
                "target_price": sig.target_price,
                "quality_score": sig.quality_score,
                "atr_pct": sig.atr_pct,
                "rsi": sig.rsi,
                "adx": sig.adx,
                "quantity": sig.quantity,
            })
            written += 1

    return written


# ============================================================================
# SCHEDULING HELPERS
# ============================================================================
def next_scan_time(now: datetime) -> datetime:
    """
    Compute the next 15-minute aligned scan time after `now`.
    Candles close at: 9:30, 9:45, 10:00, ..., 15:15, 15:30
    We scan 75 seconds after candle close to allow the data fetcher
    (~1 min per cycle) to finish writing updated parquets.
    """
    # Market candle anchored at 9:15
    anchor = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now.tzinfo is None:
        anchor = IST.localize(anchor)
    else:
        anchor = anchor.astimezone(IST)

    minutes_since_anchor = (now - anchor).total_seconds() / 60.0
    if minutes_since_anchor < 0:
        return anchor + timedelta(minutes=SCAN_INTERVAL_MINUTES, seconds=75)

    intervals_elapsed = int(minutes_since_anchor // SCAN_INTERVAL_MINUTES)
    next_interval = anchor + timedelta(
        minutes=(intervals_elapsed + 1) * SCAN_INTERVAL_MINUTES,
        seconds=75,  # 75s buffer for data fetcher to complete (~1 min)
    )
    return next_interval


def is_market_hours(now: datetime) -> bool:
    t = now.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


# ============================================================================
# MAIN SCAN LOOP
# ============================================================================
def run_single_scan(
    data_dir: str,
    position_size_rs: float,
    strategy_modules: Optional[dict],
    seen_signals: Set[str],
) -> Tuple[int, Set[str]]:
    """
    Run a single scan cycle. Returns (num_new_signals, updated_seen_signals).
    """
    now = datetime.now(IST)
    today = now.date()
    today_str = today.strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, SIGNAL_CSV_PATTERN.format(today_str))

    log.info(f"=== Scan cycle at {now.strftime('%H:%M:%S')} IST ===")

    # Scan for signals
    all_signals = scan_all_tickers(data_dir, today, position_size_rs, strategy_modules)

    # Filter out already-seen signals
    new_signals = [s for s in all_signals if s.signal_id not in seen_signals]

    if new_signals:
        written = append_signals_to_csv(new_signals, csv_path)
        for s in new_signals:
            seen_signals.add(s.signal_id)
            log.info(
                f"  NEW SIGNAL: {s.side} {s.ticker} @ {s.entry_price} "
                f"| SL={s.stop_price} TGT={s.target_price} "
                f"| Setup={s.setup} Q={s.quality_score}"
            )
        save_seen_signals(seen_signals)
        log.info(f"Wrote {written} new signals to {csv_path}")
    else:
        log.info("No new signals this cycle.")

    return len(new_signals), seen_signals


def main():
    parser = argparse.ArgumentParser(description="Live AVWAP Signal Generator")
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single scan and exit (don't loop)",
    )
    parser.add_argument(
        "--data-dir", type=str, default=None,
        help=f"Path to 15-min parquet directory (default: ./{DIR_15MIN})",
    )
    parser.add_argument(
        "--position-size", type=float, default=DEFAULT_POSITION_SIZE_RS,
        help=f"Position size in Rs per trade (default: {DEFAULT_POSITION_SIZE_RS})",
    )
    args = parser.parse_args()

    # Resolve data directory
    script_dir = Path(__file__).resolve().parent
    if args.data_dir:
        data_dir = args.data_dir
    else:
        data_dir = str(script_dir / DIR_15MIN)
        if not os.path.isdir(data_dir):
            # Try parent directory
            alt = str(script_dir.parent / DIR_15MIN)
            if os.path.isdir(alt):
                data_dir = alt

    log.info("=" * 60)
    log.info("AVWAP Live Signal Generator — Starting")
    log.info(f"  Data directory : {data_dir}")
    log.info(f"  Position size  : Rs.{args.position_size:,.0f}")
    log.info(f"  Signal output  : {os.path.abspath(SIGNAL_DIR)}/")
    log.info(f"  Scan interval  : {SCAN_INTERVAL_MINUTES} minutes")
    log.info("=" * 60)

    # Try to load strategy modules
    strategy_modules = _try_import_strategy()

    # Load previously seen signals (reset if from a different day)
    seen_signals = load_seen_signals()
    try:
        with open(SEEN_SIGNALS_FILE, "r") as f:
            saved_date = json.load(f).get("date", "")
        if saved_date != datetime.now(IST).strftime("%Y-%m-%d"):
            log.info("New trading day — clearing seen signals cache.")
            seen_signals = set()
    except Exception:
        pass

    if args.once:
        # Single scan mode
        count, seen_signals = run_single_scan(
            data_dir, args.position_size, strategy_modules, seen_signals,
        )
        log.info(f"Single scan complete. {count} new signals found.")
        return

    # Continuous loop
    log.info("Entering continuous scan loop. Press Ctrl+C to stop.")

    try:
        while True:
            now = datetime.now(IST)

            if is_market_hours(now):
                count, seen_signals = run_single_scan(
                    data_dir, args.position_size, strategy_modules, seen_signals,
                )

                # Wait until next scan time
                next_t = next_scan_time(now)
                wait_secs = max(0, (next_t - datetime.now(IST)).total_seconds())
                if wait_secs > 0:
                    log.info(
                        f"Next scan at {next_t.strftime('%H:%M:%S')} IST "
                        f"(waiting {wait_secs:.0f}s)"
                    )
                    time.sleep(wait_secs)
            else:
                # Outside market hours
                if now.time() < MARKET_OPEN:
                    # Before market open — wait until 9:15
                    market_open_dt = now.replace(
                        hour=9, minute=15, second=0, microsecond=0
                    )
                    wait_secs = max(0, (market_open_dt - now).total_seconds())
                    log.info(
                        f"Before market hours. Waiting until {MARKET_OPEN} IST "
                        f"({wait_secs:.0f}s)"
                    )
                    time.sleep(min(wait_secs, 60))  # Check every minute
                else:
                    # After market close
                    log.info("Market closed for today. Shutting down.")
                    break

    except KeyboardInterrupt:
        log.info("Received interrupt. Shutting down gracefully.")
    finally:
        save_seen_signals(seen_signals)
        log.info("Signal generator stopped.")


if __name__ == "__main__":
    main()
