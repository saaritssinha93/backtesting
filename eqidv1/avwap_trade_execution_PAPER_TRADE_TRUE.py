# -*- coding: utf-8 -*-
"""
avwap_trade_execution_PAPER_TRADE_TRUE.py — Paper Trade Executor (Simulation)
==============================================================================

Watches the daily signal CSV produced by avwap_live_signal_generator.py and
simulates trade execution locally. No real orders are placed.

For each new signal:
  1. Records the simulated entry at the signal's entry_price
  2. Tracks P&L against target and stop-loss using 5-min LTP polling
  3. Forces simulated close at 15:15 IST if neither target nor SL is hit
  4. Appends results to a daily paper trade log CSV

Output:
  - live_signals/paper_trades_YYYY-MM-DD.csv  (detailed trade log)
  - live_signals/paper_trade_summary.json       (running P&L summary)

Features:
  - Watchdog-based CSV monitoring for instant reaction
  - Concurrent trade simulation threads (one per active trade)
  - Signal deduplication via signal_id tracking
  - Graceful shutdown on Ctrl+C
  - Optional Kite LTP polling for realistic price simulation

Usage:
    python avwap_trade_execution_PAPER_TRADE_TRUE.py
    python avwap_trade_execution_PAPER_TRADE_TRUE.py --no-ltp    # skip Kite LTP
    python avwap_trade_execution_PAPER_TRADE_TRUE.py --capital 500000
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import pytz

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    print("ERROR: watchdog package required. Install with: pip install watchdog")
    sys.exit(1)

# ============================================================================
# CONSTANTS
# ============================================================================
IST = pytz.timezone("Asia/Kolkata")

SIGNAL_DIR = "live_signals"
SIGNAL_CSV_PATTERN = "signals_{}.csv"
PAPER_TRADE_LOG_PATTERN = "paper_trades_{}.csv"
EXECUTED_SIGNALS_FILE = os.path.join(SIGNAL_DIR, "executed_signals_paper.json")
SUMMARY_FILE = os.path.join(SIGNAL_DIR, "paper_trade_summary.json")

# Trading hours
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
FORCED_CLOSE_TIME = dt_time(15, 15)

# Simulation
POLL_INTERVAL_SEC = 5
MAX_CONCURRENT_TRADES = 30

# Default capital
DEFAULT_START_CAPITAL = 1_000_000
DEFAULT_POSITION_SIZE = 50_000

# Paper trade log columns
TRADE_LOG_COLUMNS = [
    "trade_id",
    "signal_id",
    "signal_datetime",
    "entry_time",
    "exit_time",
    "ticker",
    "side",
    "setup",
    "impulse_type",
    "quantity",
    "entry_price",
    "exit_price",
    "stop_price",
    "target_price",
    "outcome",
    "pnl_rs",
    "pnl_pct",
    "quality_score",
]

# Signal CSV columns (must match signal generator output)
SIGNAL_COLUMNS = [
    "signal_id", "signal_datetime", "received_time", "ticker", "side",
    "setup", "impulse_type", "entry_price", "stop_price", "target_price",
    "quality_score", "atr_pct", "rsi", "adx", "quantity",
]

# ============================================================================
# LOGGING
# ============================================================================
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("paper_trade")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(SIGNAL_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(SIGNAL_DIR, "paper_trade_execution.log"),
        mode="a",
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ============================================================================
# KITE SESSION (optional — for LTP simulation)
# ============================================================================
kite = None


def setup_kite_session():
    """Set up Kite session for LTP polling. Non-fatal if it fails."""
    global kite
    try:
        from kiteconnect import KiteConnect

        with open("access_token.txt", "r") as f:
            access_token = f.read().strip()
        with open("api_key.txt", "r") as f:
            api_key = f.read().split()[0]

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        log.info("Kite session established (LTP polling enabled).")
    except Exception as e:
        log.warning(f"Kite session not available: {e}")
        log.warning("Paper trades will use signal entry price for simulation.")
        kite = None


def get_ltp(ticker: str) -> Optional[float]:
    """Get last traded price from Kite. Returns None on failure."""
    if kite is None:
        return None
    try:
        data = kite.ltp(f"NSE:{ticker}")
        return float(data[f"NSE:{ticker}"]["last_price"])
    except Exception:
        return None


# ============================================================================
# EXECUTED SIGNALS TRACKING
# ============================================================================
def load_executed_signals() -> Set[str]:
    if os.path.exists(EXECUTED_SIGNALS_FILE):
        try:
            with open(EXECUTED_SIGNALS_FILE, "r") as f:
                data = json.load(f)
                # Reset if from a different day
                if data.get("date") != datetime.now(IST).strftime("%Y-%m-%d"):
                    return set()
                return set(data.get("signals", []))
        except (json.JSONDecodeError, KeyError):
            return set()
    return set()


def save_executed_signals(executed: Set[str]) -> None:
    os.makedirs(SIGNAL_DIR, exist_ok=True)
    with open(EXECUTED_SIGNALS_FILE, "w") as f:
        json.dump({
            "date": datetime.now(IST).strftime("%Y-%m-%d"),
            "signals": list(executed),
        }, f)


# ============================================================================
# TRADE SIMULATION
# ============================================================================
@dataclass
class PaperTrade:
    trade_id: str = ""
    signal_id: str = ""
    signal_datetime: str = ""
    entry_time: str = ""
    exit_time: str = ""
    ticker: str = ""
    side: str = ""
    setup: str = ""
    impulse_type: str = ""
    quantity: int = 1
    entry_price: float = 0.0
    exit_price: float = 0.0
    stop_price: float = 0.0
    target_price: float = 0.0
    outcome: str = ""
    pnl_rs: float = 0.0
    pnl_pct: float = 0.0
    quality_score: float = 0.0


# Shared state
active_trades: Dict[str, threading.Thread] = {}
active_trades_lock = threading.Lock()
daily_pnl: Dict[str, float] = {"total": 0.0, "wins": 0, "losses": 0, "trades": 0}
daily_pnl_lock = threading.Lock()


def simulate_trade(signal: dict, use_ltp: bool = True) -> None:
    """
    Simulate a single trade in a background thread.
    Monitors LTP (if available) against target and stop-loss.
    Forces close at 15:15 IST.
    """
    ticker = signal["ticker"]
    side = signal["side"].upper()
    entry_price = float(signal["entry_price"])
    stop_price = float(signal["stop_price"])
    target_price = float(signal["target_price"])
    quantity = int(signal.get("quantity", 1))
    signal_id = signal["signal_id"]

    now_ist = datetime.now(IST)
    trade_id = f"PT-{signal_id[:8]}-{now_ist.strftime('%H%M%S')}"
    today = now_ist.date()
    forced_close_dt = IST.localize(
        datetime.combine(today, FORCED_CLOSE_TIME)
    )

    log.info(
        f"[SIM] ENTRY {side} {ticker} @ {entry_price} | "
        f"SL={stop_price} TGT={target_price} qty={quantity} | ID={trade_id}"
    )

    # Monitor loop
    exit_price = entry_price
    outcome = "MONITORING"

    while True:
        now_ist = datetime.now(IST)

        # Forced close at 15:15
        if now_ist >= forced_close_dt:
            ltp = get_ltp(ticker) if use_ltp else None
            exit_price = ltp if ltp else entry_price
            outcome = "EOD_CLOSE"
            log.info(
                f"[SIM] FORCED CLOSE {side} {ticker} @ {exit_price} "
                f"(15:15 IST) | ID={trade_id}"
            )
            break

        # Check LTP
        if use_ltp:
            ltp = get_ltp(ticker)
        else:
            ltp = None

        if ltp is not None:
            if side == "SHORT":
                if ltp >= stop_price:
                    exit_price = stop_price
                    outcome = "SL"
                    log.info(
                        f"[SIM] SL HIT {side} {ticker} @ {exit_price} "
                        f"(LTP={ltp}) | ID={trade_id}"
                    )
                    break
                elif ltp <= target_price:
                    exit_price = target_price
                    outcome = "TARGET"
                    log.info(
                        f"[SIM] TARGET HIT {side} {ticker} @ {exit_price} "
                        f"(LTP={ltp}) | ID={trade_id}"
                    )
                    break
            else:  # LONG
                if ltp <= stop_price:
                    exit_price = stop_price
                    outcome = "SL"
                    log.info(
                        f"[SIM] SL HIT {side} {ticker} @ {exit_price} "
                        f"(LTP={ltp}) | ID={trade_id}"
                    )
                    break
                elif ltp >= target_price:
                    exit_price = target_price
                    outcome = "TARGET"
                    log.info(
                        f"[SIM] TARGET HIT {side} {ticker} @ {exit_price} "
                        f"(LTP={ltp}) | ID={trade_id}"
                    )
                    break

        # If no LTP available (no Kite session), just record entry and exit at signal prices
        if not use_ltp or ltp is None:
            # Without LTP we can't monitor real-time; record as pending
            exit_price = entry_price
            outcome = "NO_LTP_SIMULATED"
            log.info(
                f"[SIM] NO LTP — recording {side} {ticker} @ {entry_price} "
                f"as simulated entry | ID={trade_id}"
            )
            break

        time.sleep(POLL_INTERVAL_SEC)

    # Calculate P&L
    exit_time_ist = datetime.now(IST)
    if side == "SHORT":
        pnl_rs = (entry_price - exit_price) * quantity
        pnl_pct = (entry_price - exit_price) / entry_price * 100 if entry_price > 0 else 0
    else:
        pnl_rs = (exit_price - entry_price) * quantity
        pnl_pct = (exit_price - entry_price) / entry_price * 100 if entry_price > 0 else 0

    trade = PaperTrade(
        trade_id=trade_id,
        signal_id=signal_id,
        signal_datetime=signal.get("signal_datetime", ""),
        entry_time=now_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        exit_time=exit_time_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        ticker=ticker,
        side=side,
        setup=signal.get("setup", ""),
        impulse_type=signal.get("impulse_type", ""),
        quantity=quantity,
        entry_price=entry_price,
        exit_price=round(exit_price, 2),
        stop_price=stop_price,
        target_price=target_price,
        outcome=outcome,
        pnl_rs=round(pnl_rs, 2),
        pnl_pct=round(pnl_pct, 4),
        quality_score=float(signal.get("quality_score", 0)),
    )

    # Log to daily CSV
    _log_trade(trade)

    # Update daily P&L
    with daily_pnl_lock:
        daily_pnl["total"] += pnl_rs
        daily_pnl["trades"] += 1
        if pnl_rs > 0:
            daily_pnl["wins"] += 1
        elif pnl_rs < 0:
            daily_pnl["losses"] += 1
        _save_summary()

    log.info(
        f"[SIM] RESULT {side} {ticker} | {outcome} | "
        f"P&L: Rs.{pnl_rs:+,.2f} ({pnl_pct:+.2f}%) | "
        f"Day total: Rs.{daily_pnl['total']:+,.2f} "
        f"({daily_pnl['wins']}W/{daily_pnl['losses']}L)"
    )

    # Remove from active trades
    with active_trades_lock:
        active_trades.pop(signal_id, None)


def _log_trade(trade: PaperTrade) -> None:
    """Append trade result to daily CSV."""
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, PAPER_TRADE_LOG_PATTERN.format(today_str))

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_LOG_COLUMNS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "trade_id": trade.trade_id,
            "signal_id": trade.signal_id,
            "signal_datetime": trade.signal_datetime,
            "entry_time": trade.entry_time,
            "exit_time": trade.exit_time,
            "ticker": trade.ticker,
            "side": trade.side,
            "setup": trade.setup,
            "impulse_type": trade.impulse_type,
            "quantity": trade.quantity,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "stop_price": trade.stop_price,
            "target_price": trade.target_price,
            "outcome": trade.outcome,
            "pnl_rs": trade.pnl_rs,
            "pnl_pct": trade.pnl_pct,
            "quality_score": trade.quality_score,
        })


def _save_summary() -> None:
    """Save running P&L summary to JSON."""
    try:
        wr = daily_pnl["wins"] / daily_pnl["trades"] * 100 if daily_pnl["trades"] > 0 else 0
        summary = {
            "date": datetime.now(IST).strftime("%Y-%m-%d"),
            "total_pnl_rs": round(daily_pnl["total"], 2),
            "total_trades": daily_pnl["trades"],
            "wins": daily_pnl["wins"],
            "losses": daily_pnl["losses"],
            "win_rate_pct": round(wr, 2),
            "last_updated": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(SUMMARY_FILE, "w") as f:
            json.dump(summary, f, indent=2)
    except Exception:
        pass


# ============================================================================
# CSV SIGNAL READER
# ============================================================================
def read_signals_csv(csv_path: str) -> List[dict]:
    """Read signals from the daily CSV file."""
    if not os.path.exists(csv_path):
        return []
    try:
        df = pd.read_csv(
            csv_path,
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
            engine="python",
        )
        if df.empty:
            return []
        return df.to_dict("records")
    except Exception as e:
        log.error(f"Error reading signals CSV: {e}")
        return []


# ============================================================================
# SIGNAL PROCESSOR
# ============================================================================
def process_new_signals(
    csv_path: str,
    executed: Set[str],
    use_ltp: bool,
    trade_semaphore: threading.Semaphore,
) -> Set[str]:
    """
    Read signals CSV, find unprocessed signals, launch simulation threads.
    Returns updated executed signals set.
    """
    signals = read_signals_csv(csv_path)
    if not signals:
        return executed

    new_count = 0
    for signal in signals:
        signal_id = str(signal.get("signal_id", ""))
        if not signal_id or signal_id in executed:
            continue

        # Mark as executed immediately to prevent duplicates
        executed.add(signal_id)
        new_count += 1

        # Launch simulation thread
        def _run_sim(sig=signal, ultp=use_ltp):
            trade_semaphore.acquire()
            try:
                simulate_trade(sig, use_ltp=ultp)
            except Exception as e:
                log.error(f"Simulation error for {sig.get('ticker', '?')}: {e}")
                log.error(traceback.format_exc())
            finally:
                trade_semaphore.release()

        t = threading.Thread(target=_run_sim, daemon=True)
        with active_trades_lock:
            active_trades[signal_id] = t
        t.start()

        log.info(
            f"[DISPATCH] Launched simulation for "
            f"{signal.get('side', '?')} {signal.get('ticker', '?')} "
            f"@ {signal.get('entry_price', '?')} | ID={signal_id[:12]}"
        )

    if new_count > 0:
        save_executed_signals(executed)
        log.info(f"Processed {new_count} new signal(s). Active sims: {len(active_trades)}")

    return executed


# ============================================================================
# WATCHDOG FILE MONITOR
# ============================================================================
class SignalCSVHandler(FileSystemEventHandler):
    """Watches the signal CSV for modifications and triggers processing."""

    def __init__(self, csv_path: str, callback, debounce_sec: float = 3.0):
        super().__init__()
        self.csv_path = csv_path
        self.csv_filename = os.path.basename(csv_path)
        self.callback = callback
        self.debounce_sec = debounce_sec
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def on_modified(self, event):
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == self.csv_filename:
            self._debounce()

    def on_created(self, event):
        if event.is_directory:
            return
        if os.path.basename(event.src_path) == self.csv_filename:
            self._debounce()

    def _debounce(self):
        with self._lock:
            if self._timer and self._timer.is_alive():
                self._timer.cancel()
            self._timer = threading.Timer(self.debounce_sec, self.callback)
            self._timer.start()


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="AVWAP Paper Trade Executor")
    parser.add_argument(
        "--no-ltp", action="store_true",
        help="Disable Kite LTP polling (record trades at signal prices)",
    )
    parser.add_argument(
        "--capital", type=float, default=DEFAULT_START_CAPITAL,
        help=f"Starting capital in Rs (default: {DEFAULT_START_CAPITAL})",
    )
    args = parser.parse_args()

    use_ltp = not args.no_ltp

    log.info("=" * 65)
    log.info("AVWAP Paper Trade Executor — PAPER_TRADE = TRUE")
    log.info(f"  Mode            : SIMULATION (no real orders)")
    log.info(f"  LTP polling     : {'Enabled' if use_ltp else 'Disabled'}")
    log.info(f"  Starting capital: Rs.{args.capital:,.0f}")
    log.info(f"  Signal dir      : {os.path.abspath(SIGNAL_DIR)}/")
    log.info(f"  Forced close at : {FORCED_CLOSE_TIME} IST")
    log.info("=" * 65)

    # Set up Kite for LTP if requested
    if use_ltp:
        setup_kite_session()
        if kite is None:
            log.warning("LTP polling disabled — Kite session unavailable.")
            use_ltp = False

    # Load executed signals
    executed = load_executed_signals()
    log.info(f"Loaded {len(executed)} previously executed signals.")

    # Resolve today's signal CSV
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, SIGNAL_CSV_PATTERN.format(today_str))

    # Semaphore for concurrent trade limit
    trade_semaphore = threading.Semaphore(MAX_CONCURRENT_TRADES)

    # Callback for watchdog
    def on_csv_change():
        nonlocal executed
        log.info("Signal CSV changed — processing new signals...")
        executed = process_new_signals(csv_path, executed, use_ltp, trade_semaphore)

    # Set up watchdog
    os.makedirs(SIGNAL_DIR, exist_ok=True)
    handler = SignalCSVHandler(csv_path, on_csv_change, debounce_sec=2.0)
    observer = Observer()
    observer.schedule(handler, path=SIGNAL_DIR, recursive=False)
    observer.start()
    log.info(f"Watchdog started — monitoring {csv_path}")

    # Initial check for existing signals
    if os.path.exists(csv_path):
        log.info("Checking for existing unprocessed signals...")
        executed = process_new_signals(csv_path, executed, use_ltp, trade_semaphore)

    # Main loop
    try:
        while True:
            now = datetime.now(IST)

            # Check if market is still open
            if now.time() > MARKET_CLOSE:
                # Wait for all active trades to complete
                with active_trades_lock:
                    remaining = len(active_trades)
                if remaining > 0:
                    log.info(f"Market closed. Waiting for {remaining} active sim(s)...")
                    time.sleep(10)
                else:
                    log.info("Market closed. All simulations complete.")
                    break

            time.sleep(1)

    except KeyboardInterrupt:
        log.info("Received interrupt. Shutting down...")
    finally:
        observer.stop()
        observer.join()
        save_executed_signals(executed)

        # Print daily summary
        with daily_pnl_lock:
            wr = daily_pnl["wins"] / daily_pnl["trades"] * 100 if daily_pnl["trades"] > 0 else 0
            log.info("=" * 55)
            log.info("DAILY PAPER TRADE SUMMARY")
            log.info(f"  Total trades : {daily_pnl['trades']}")
            log.info(f"  Wins         : {daily_pnl['wins']}")
            log.info(f"  Losses       : {daily_pnl['losses']}")
            log.info(f"  Win rate     : {wr:.1f}%")
            log.info(f"  Total P&L    : Rs.{daily_pnl['total']:+,.2f}")
            log.info("=" * 55)

        log.info("Paper trade executor stopped.")


if __name__ == "__main__":
    main()
