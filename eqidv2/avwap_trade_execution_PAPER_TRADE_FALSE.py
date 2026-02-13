# -*- coding: utf-8 -*-
"""
avwap_trade_execution_PAPER_TRADE_FALSE.py — Live Trade Executor (Zerodha Real)
================================================================================

Watches the daily signal CSV produced by avwap_live_signal_generator.py and
places REAL orders on Zerodha via KiteConnect.

For each new signal:
  1. Places a MARKET entry order (MIS product for intraday)
  2. Waits for fill confirmation
  3. Places a LIMIT target order + SL-M stop-loss order
  4. Monitors target/SL in a background thread
  5. If either fills → cancels the other (with retry)
  6. Forces close at 15:15 IST if neither is hit

Concurrent trades run in parallel threads (up to MAX_CONCURRENT_TRADES).

Output:
  - live_signals/live_trades_YYYY-MM-DD.csv   (detailed trade log)
  - live_signals/live_trade_summary.json       (running P&L summary)

Safety features:
  - Signal deduplication via signal_id tracking
  - Order cancellation with retries on failure
  - Forced position closure at 15:15 IST
  - Thread-safe state management
  - Comprehensive logging
  - Graceful shutdown on Ctrl+C

Usage:
    python avwap_trade_execution_PAPER_TRADE_FALSE.py
    python avwap_trade_execution_PAPER_TRADE_FALSE.py --max-trades 10
    python avwap_trade_execution_PAPER_TRADE_FALSE.py --dry-run  # validate without trading
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
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

import pandas as pd
import pytz

try:
    from kiteconnect import KiteConnect
except ImportError:
    print("ERROR: kiteconnect package required. Install with: pip install kiteconnect")
    sys.exit(1)

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
TRADE_LOG_PATTERN = "live_trades_{}.csv"
EXECUTED_SIGNALS_FILE = os.path.join(SIGNAL_DIR, "executed_signals_live.json")
SUMMARY_FILE = os.path.join(SIGNAL_DIR, "live_trade_summary.json")

# Trading hours
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
FORCED_CLOSE_TIME = dt_time(15, 20)  # aligned closer to backtest EOD; safe before broker auto-square-off

# Order monitoring
ORDER_POLL_SEC = 3
FILL_WAIT_TIMEOUT_SEC = 60
MAX_CANCEL_RETRIES = 3
CANCEL_RETRY_WAIT_SEC = 2

# Risk limits
MAX_DAILY_LOSS_RS = 5_000           # stop taking new trades if daily loss exceeds this
MAX_OPEN_POSITIONS = 10             # max simultaneous open positions
MAX_CAPITAL_DEPLOYED_RS = 500_000   # max total margin that can be deployed
INTRADAY_LEVERAGE = 5.0             # MIS leverage on Zerodha

# Concurrency
MAX_CONCURRENT_TRADES = 20

# Trade log columns
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
    "filled_price",
    "exit_price",
    "stop_price",
    "target_price",
    "outcome",
    "pnl_rs",
    "pnl_pct",
    "entry_order_id",
    "target_order_id",
    "sl_order_id",
    "close_order_id",
    "quality_score",
]

# Signal CSV columns
SIGNAL_COLUMNS = [
    "signal_id", "signal_datetime", "received_time", "ticker", "side",
    "setup", "impulse_type", "entry_price", "stop_price", "target_price",
    "quality_score", "atr_pct", "rsi", "adx", "quantity",
]

# ============================================================================
# LOGGING
# ============================================================================
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("live_trade")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(SIGNAL_DIR, exist_ok=True)
    fh = logging.FileHandler(
        os.path.join(SIGNAL_DIR, "live_trade_execution.log"),
        mode="a",
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ============================================================================
# KITE SESSION
# ============================================================================
kite: Optional[KiteConnect] = None


def setup_kite_session() -> KiteConnect:
    """Set up and return a KiteConnect session. Fatal if it fails."""
    global kite
    try:
        with open("access_token.txt", "r") as f:
            access_token = f.read().strip()
        with open("api_key.txt", "r") as f:
            key_data = f.read().strip().split()

        if len(key_data) < 2:
            raise ValueError(
                "api_key.txt must contain 'API_KEY API_SECRET' separated by space."
            )

        api_key = key_data[0]
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        # Validate session with a profile call
        profile = kite.profile()
        log.info(f"Kite session established. User: {profile.get('user_name', 'N/A')}")
        return kite

    except FileNotFoundError as e:
        log.error(f"Credential file not found: {e}")
        log.error("Ensure access_token.txt and api_key.txt exist in the working directory.")
        raise
    except Exception as e:
        log.error(f"Kite session setup failed: {e}")
        raise


def get_ltp(ticker: str) -> float:
    """Get last traded price. Raises on failure."""
    data = kite.ltp(f"NSE:{ticker}")
    return float(data[f"NSE:{ticker}"]["last_price"])


def round_to_tick(price: float, tick: float = 0.05) -> float:
    """Round price to nearest tick size."""
    return round(round(price / tick) * tick, 2)


# ============================================================================
# ORDER HELPERS
# ============================================================================
def cancel_order_safe(variety: str, order_id: str) -> bool:
    """Cancel an order with retries. Returns True on success."""
    for attempt in range(1, MAX_CANCEL_RETRIES + 1):
        try:
            kite.cancel_order(variety, order_id)
            log.info(f"Cancelled order {order_id} (attempt {attempt})")
            return True
        except Exception as e:
            log.warning(f"Cancel attempt {attempt}/{MAX_CANCEL_RETRIES} for {order_id}: {e}")
            if attempt < MAX_CANCEL_RETRIES:
                time.sleep(CANCEL_RETRY_WAIT_SEC)
    log.error(f"FAILED to cancel order {order_id} after {MAX_CANCEL_RETRIES} attempts")
    return False


def wait_for_fill(order_id: str, timeout_sec: int = FILL_WAIT_TIMEOUT_SEC) -> Optional[float]:
    """
    Wait for an order to fill. Returns average fill price or None on timeout.
    """
    start = time.monotonic()
    while (time.monotonic() - start) < timeout_sec:
        try:
            orders = kite.orders()
            for o in orders:
                if o["order_id"] == order_id:
                    if o["status"] == "COMPLETE":
                        return float(o.get("average_price", 0))
                    elif o["status"] in ("REJECTED", "CANCELLED"):
                        log.warning(f"Order {order_id} status: {o['status']}")
                        return None
        except Exception as e:
            log.warning(f"Error polling order {order_id}: {e}")
        time.sleep(ORDER_POLL_SEC)

    log.warning(f"Order {order_id} did not fill within {timeout_sec}s")
    return None


# ============================================================================
# EXECUTED SIGNALS TRACKING
# ============================================================================
def load_executed_signals() -> Set[str]:
    if os.path.exists(EXECUTED_SIGNALS_FILE):
        try:
            with open(EXECUTED_SIGNALS_FILE, "r") as f:
                data = json.load(f)
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
# LIVE TRADE MANAGEMENT
# ============================================================================
@dataclass
class LiveTradeResult:
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
    filled_price: float = 0.0
    exit_price: float = 0.0
    stop_price: float = 0.0
    target_price: float = 0.0
    outcome: str = ""
    pnl_rs: float = 0.0
    pnl_pct: float = 0.0
    entry_order_id: str = ""
    target_order_id: str = ""
    sl_order_id: str = ""
    close_order_id: str = ""
    quality_score: float = 0.0


# Shared state
active_trades: Dict[str, threading.Thread] = {}
active_trades_lock = threading.Lock()
daily_pnl: Dict[str, Any] = {"total": 0.0, "wins": 0, "losses": 0, "trades": 0}
daily_pnl_lock = threading.Lock()

# Capital / position tracking (margin, not notional — accounts for MIS leverage)
capital_deployed: Dict[str, float] = {}   # signal_id → margin blocked
capital_lock = threading.Lock()


def _check_risk_limits(signal: dict) -> Optional[str]:
    """
    Check daily loss limit, open positions, and capital deployed.
    Returns a rejection reason string, or None if the trade is allowed.
    """
    with daily_pnl_lock:
        if daily_pnl["total"] <= -MAX_DAILY_LOSS_RS:
            return f"daily loss limit hit (Rs.{daily_pnl['total']:+,.2f} <= -{MAX_DAILY_LOSS_RS:,})"

    with capital_lock:
        open_count = len(capital_deployed)
        total_deployed = sum(capital_deployed.values())

    if open_count >= MAX_OPEN_POSITIONS:
        return f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})"

    entry_price = float(signal.get("entry_price", 0))
    quantity = int(signal.get("quantity", 1))
    margin = (entry_price * quantity) / INTRADAY_LEVERAGE
    if (total_deployed + margin) > MAX_CAPITAL_DEPLOYED_RS:
        return (
            f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
            f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})"
        )

    return None


def execute_live_trade(signal: dict) -> None:
    """
    Execute a complete live MIS trade on Zerodha:
      1. Market entry order
      2. Wait for fill
      3. Place target (LIMIT) + stop-loss (SL-M)
      4. Monitor until target/SL fills or 15:15 forced close
    """
    ticker = str(signal["ticker"]).upper()
    side = str(signal["side"]).upper()
    signal_id = signal["signal_id"]
    signal_entry_price = float(signal["entry_price"])
    signal_stop = float(signal["stop_price"])
    signal_target = float(signal["target_price"])
    quantity = int(signal.get("quantity", 1))

    now_ist = datetime.now(IST)
    trade_id = f"LT-{signal_id[:8]}-{now_ist.strftime('%H%M%S')}"
    today = now_ist.date()
    forced_close_dt = IST.localize(datetime.combine(today, FORCED_CLOSE_TIME))

    if side == "SHORT":
        entry_txn = "SELL"
        exit_txn = "BUY"
    else:
        entry_txn = "BUY"
        exit_txn = "SELL"

    log.info(
        f"[LIVE] Starting trade {trade_id}: {entry_txn} {ticker} qty={quantity} "
        f"| SL={signal_stop} TGT={signal_target}"
    )

    result = LiveTradeResult(
        trade_id=trade_id,
        signal_id=signal_id,
        signal_datetime=signal.get("signal_datetime", ""),
        ticker=ticker,
        side=side,
        setup=signal.get("setup", ""),
        impulse_type=signal.get("impulse_type", ""),
        quantity=quantity,
        entry_price=signal_entry_price,
        stop_price=signal_stop,
        target_price=signal_target,
        quality_score=float(signal.get("quality_score", 0)),
    )

    try:
        # ---- STEP 1: Place market entry ----
        entry_order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker,
            transaction_type=entry_txn,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            validity=kite.VALIDITY_DAY,
            tag="AVWAPEntry",
        )
        result.entry_order_id = str(entry_order_id)
        result.entry_time = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
        log.info(f"[LIVE] Entry order placed: {entry_txn} {ticker} ID={entry_order_id}")

        # ---- STEP 2: Wait for fill ----
        filled_price = wait_for_fill(entry_order_id)
        if filled_price is None or filled_price == 0:
            log.error(f"[LIVE] Entry order {entry_order_id} not filled. Aborting trade.")
            result.outcome = "ENTRY_FAILED"
            _log_trade_result(result)
            return

        result.filled_price = filled_price
        log.info(f"[LIVE] Entry filled: {ticker} @ {filled_price}")

        # Register margin deployment (notional / leverage)
        margin = (filled_price * quantity) / INTRADAY_LEVERAGE
        with capital_lock:
            capital_deployed[signal_id] = margin

        # Recalculate SL/target based on actual fill price
        if side == "SHORT":
            # SL above entry, target below
            sl_offset = abs(signal_stop - signal_entry_price)
            tgt_offset = abs(signal_entry_price - signal_target)
            sl_price = round_to_tick(filled_price + sl_offset)
            tgt_price = round_to_tick(filled_price - tgt_offset)
        else:
            # SL below entry, target above
            sl_offset = abs(signal_entry_price - signal_stop)
            tgt_offset = abs(signal_target - signal_entry_price)
            sl_price = round_to_tick(filled_price - sl_offset)
            tgt_price = round_to_tick(filled_price + tgt_offset)

        result.stop_price = sl_price
        result.target_price = tgt_price

        # ---- STEP 3: Place target order (LIMIT) ----
        target_order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker,
            transaction_type=exit_txn,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=tgt_price,
            validity=kite.VALIDITY_DAY,
            tag="AVWAPTarget",
        )
        result.target_order_id = str(target_order_id)
        log.info(f"[LIVE] Target order: {exit_txn} LIMIT @ {tgt_price} ID={target_order_id}")

        # ---- STEP 4: Place stop-loss order (SL-M) ----
        sl_order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker,
            transaction_type=exit_txn,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=sl_price,
            validity=kite.VALIDITY_DAY,
            tag="AVWAPStopLoss",
        )
        result.sl_order_id = str(sl_order_id)
        log.info(f"[LIVE] SL order: {exit_txn} SL-M trigger={sl_price} ID={sl_order_id}")

        # ---- STEP 5: Monitor until target/SL/forced close ----
        while True:
            now_ist = datetime.now(IST)

            # Force close at 15:15
            if now_ist >= forced_close_dt:
                log.info(f"[LIVE] EOD forced close for {ticker}")

                cancel_order_safe(kite.VARIETY_REGULAR, target_order_id)
                cancel_order_safe(kite.VARIETY_REGULAR, sl_order_id)

                # Place forced close market order
                try:
                    close_order_id = kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange=kite.EXCHANGE_NSE,
                        tradingsymbol=ticker,
                        transaction_type=exit_txn,
                        quantity=quantity,
                        product=kite.PRODUCT_MIS,
                        order_type=kite.ORDER_TYPE_MARKET,
                        validity=kite.VALIDITY_DAY,
                        tag="AVWAPForceClose",
                    )
                    result.close_order_id = str(close_order_id)

                    close_price = wait_for_fill(close_order_id, timeout_sec=30)
                    result.exit_price = close_price if close_price else filled_price
                    result.outcome = "EOD_CLOSE"
                    log.info(
                        f"[LIVE] Forced close: {ticker} @ {result.exit_price} "
                        f"ID={close_order_id}"
                    )
                except Exception as e:
                    log.error(f"[LIVE] Forced close failed for {ticker}: {e}")
                    result.exit_price = filled_price
                    result.outcome = "EOD_CLOSE_FAILED"
                break

            # Check order statuses
            try:
                orders = kite.orders()
            except Exception as e:
                log.warning(f"[LIVE] Order poll error: {e}")
                time.sleep(ORDER_POLL_SEC)
                continue

            target_filled = False
            sl_filled = False
            target_fill_price = tgt_price
            sl_fill_price = sl_price

            for o in orders:
                if o["order_id"] == target_order_id and o["status"] == "COMPLETE":
                    target_filled = True
                    target_fill_price = float(o.get("average_price", tgt_price))
                if o["order_id"] == sl_order_id and o["status"] == "COMPLETE":
                    sl_filled = True
                    sl_fill_price = float(o.get("average_price", sl_price))

            if target_filled:
                log.info(
                    f"[LIVE] TARGET HIT: {ticker} @ {target_fill_price} "
                    f"→ cancelling SL {sl_order_id}"
                )
                cancel_order_safe(kite.VARIETY_REGULAR, sl_order_id)
                result.exit_price = target_fill_price
                result.outcome = "TARGET"
                break

            elif sl_filled:
                log.info(
                    f"[LIVE] SL TRIGGERED: {ticker} @ {sl_fill_price} "
                    f"→ cancelling Target {target_order_id}"
                )
                cancel_order_safe(kite.VARIETY_REGULAR, target_order_id)
                result.exit_price = sl_fill_price
                result.outcome = "SL"
                break

            time.sleep(ORDER_POLL_SEC)

    except Exception as e:
        log.error(f"[LIVE] Trade execution error for {ticker}: {e}")
        log.error(traceback.format_exc())
        result.outcome = f"ERROR: {str(e)[:80]}"
        result.exit_price = result.filled_price or signal_entry_price

    # Calculate P&L
    result.exit_time = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
    ep = result.filled_price if result.filled_price > 0 else signal_entry_price
    xp = result.exit_price if result.exit_price > 0 else ep

    if side == "SHORT":
        result.pnl_rs = round((ep - xp) * quantity, 2)
        result.pnl_pct = round((ep - xp) / ep * 100, 4) if ep > 0 else 0
    else:
        result.pnl_rs = round((xp - ep) * quantity, 2)
        result.pnl_pct = round((xp - ep) / ep * 100, 4) if ep > 0 else 0

    # Log the trade
    _log_trade_result(result)

    # Update daily P&L
    with daily_pnl_lock:
        daily_pnl["total"] += result.pnl_rs
        daily_pnl["trades"] += 1
        if result.pnl_rs > 0:
            daily_pnl["wins"] += 1
        elif result.pnl_rs < 0:
            daily_pnl["losses"] += 1
        _save_summary()

    log.info(
        f"[LIVE] RESULT {side} {ticker} | {result.outcome} | "
        f"Entry={ep} Exit={xp} | "
        f"P&L: Rs.{result.pnl_rs:+,.2f} ({result.pnl_pct:+.2f}%) | "
        f"Day total: Rs.{daily_pnl['total']:+,.2f}"
    )

    # Release capital
    with capital_lock:
        capital_deployed.pop(signal_id, None)

    # Remove from active trades
    with active_trades_lock:
        active_trades.pop(signal_id, None)


def _log_trade_result(result: LiveTradeResult) -> None:
    """Append trade result to daily CSV."""
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, TRADE_LOG_PATTERN.format(today_str))

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_LOG_COLUMNS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "trade_id": result.trade_id,
            "signal_id": result.signal_id,
            "signal_datetime": result.signal_datetime,
            "entry_time": result.entry_time,
            "exit_time": result.exit_time,
            "ticker": result.ticker,
            "side": result.side,
            "setup": result.setup,
            "impulse_type": result.impulse_type,
            "quantity": result.quantity,
            "entry_price": result.entry_price,
            "filled_price": result.filled_price,
            "exit_price": result.exit_price,
            "stop_price": result.stop_price,
            "target_price": result.target_price,
            "outcome": result.outcome,
            "pnl_rs": result.pnl_rs,
            "pnl_pct": result.pnl_pct,
            "entry_order_id": result.entry_order_id,
            "target_order_id": result.target_order_id,
            "sl_order_id": result.sl_order_id,
            "close_order_id": result.close_order_id,
            "quality_score": result.quality_score,
        })


def _save_summary() -> None:
    """Save running P&L summary to JSON."""
    try:
        wr = daily_pnl["wins"] / daily_pnl["trades"] * 100 if daily_pnl["trades"] > 0 else 0
        summary = {
            "date": datetime.now(IST).strftime("%Y-%m-%d"),
            "mode": "LIVE",
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
    trade_semaphore: threading.Semaphore,
    dry_run: bool = False,
) -> Set[str]:
    """
    Read signals CSV, find unprocessed signals, launch trade threads.
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

        # Validate required fields
        required = ["ticker", "side", "entry_price", "stop_price", "target_price"]
        missing = [k for k in required if not signal.get(k)]
        if missing:
            log.warning(f"Signal {signal_id[:12]} missing fields: {missing}. Skipping.")
            continue

        # Risk checks: daily loss, open positions, capital deployed
        reject_reason = _check_risk_limits(signal)
        if reject_reason:
            log.warning(
                f"[RISK] Rejecting {signal.get('side', '?')} "
                f"{signal.get('ticker', '?')}: {reject_reason}"
            )
            executed.add(signal_id)  # don't re-process
            continue

        # Mark as executed immediately
        executed.add(signal_id)
        new_count += 1

        ticker = signal.get("ticker", "?")
        side = signal.get("side", "?")
        entry_p = signal.get("entry_price", "?")

        if dry_run:
            log.info(
                f"[DRY-RUN] Would execute: {side} {ticker} @ {entry_p} | "
                f"SL={signal.get('stop_price')} TGT={signal.get('target_price')}"
            )
            continue

        # Launch trade thread
        def _run_trade(sig=signal):
            trade_semaphore.acquire()
            try:
                execute_live_trade(sig)
            except Exception as e:
                log.error(f"Trade thread error for {sig.get('ticker', '?')}: {e}")
                log.error(traceback.format_exc())
            finally:
                trade_semaphore.release()

        t = threading.Thread(target=_run_trade, daemon=True)
        with active_trades_lock:
            active_trades[signal_id] = t
        t.start()

        log.info(
            f"[DISPATCH] Launched LIVE trade: {side} {ticker} @ {entry_p} "
            f"| ID={signal_id[:12]}"
        )

    if new_count > 0:
        save_executed_signals(executed)
        log.info(
            f"Dispatched {new_count} new trade(s). "
            f"Active trades: {len(active_trades)}"
        )

    return executed


# ============================================================================
# WATCHDOG FILE MONITOR
# ============================================================================
class SignalCSVHandler(FileSystemEventHandler):
    """Watches the signal CSV for modifications and triggers processing."""

    def __init__(self, csv_path: str, callback, debounce_sec: float = 2.0):
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
    parser = argparse.ArgumentParser(
        description="AVWAP Live Trade Executor (Zerodha Real Orders)"
    )
    parser.add_argument(
        "--max-trades", type=int, default=MAX_CONCURRENT_TRADES,
        help=f"Max concurrent trades (default: {MAX_CONCURRENT_TRADES})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate signals without placing real orders",
    )
    args = parser.parse_args()

    log.info("=" * 65)
    log.info("AVWAP Live Trade Executor — PAPER_TRADE = FALSE")
    log.info("  ***  REAL ORDERS WILL BE PLACED ON ZERODHA  ***")
    log.info(f"  Mode              : {'DRY-RUN' if args.dry_run else 'LIVE TRADING'}")
    log.info(f"  Max concurrent    : {args.max_trades}")
    log.info(f"  Signal dir        : {os.path.abspath(SIGNAL_DIR)}/")
    log.info(f"  Forced close at   : {FORCED_CLOSE_TIME} IST")
    log.info("=" * 65)

    if not args.dry_run:
        log.info("Establishing Kite session...")
        setup_kite_session()

    # Load executed signals
    executed = load_executed_signals()
    log.info(f"Loaded {len(executed)} previously executed signals.")

    # Resolve today's signal CSV
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, SIGNAL_CSV_PATTERN.format(today_str))

    # Trade concurrency semaphore
    trade_semaphore = threading.Semaphore(args.max_trades)

    # Callback for watchdog
    def on_csv_change():
        nonlocal executed
        log.info("Signal CSV changed — processing new signals...")
        executed = process_new_signals(
            csv_path, executed, trade_semaphore, dry_run=args.dry_run,
        )

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
        executed = process_new_signals(
            csv_path, executed, trade_semaphore, dry_run=args.dry_run,
        )

    # Main loop
    try:
        while True:
            now = datetime.now(IST)

            if now.time() > MARKET_CLOSE:
                with active_trades_lock:
                    remaining = len(active_trades)
                if remaining > 0:
                    log.info(
                        f"Market closed. Waiting for {remaining} active trade(s)..."
                    )
                    time.sleep(10)
                else:
                    log.info("Market closed. All trades complete.")
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
            wr = (
                daily_pnl["wins"] / daily_pnl["trades"] * 100
                if daily_pnl["trades"] > 0
                else 0
            )
            log.info("=" * 55)
            log.info("DAILY LIVE TRADE SUMMARY")
            log.info(f"  Total trades : {daily_pnl['trades']}")
            log.info(f"  Wins         : {daily_pnl['wins']}")
            log.info(f"  Losses       : {daily_pnl['losses']}")
            log.info(f"  Win rate     : {wr:.1f}%")
            log.info(f"  Total P&L    : Rs.{daily_pnl['total']:+,.2f}")
            log.info("=" * 55)

        log.info("Live trade executor stopped.")


if __name__ == "__main__":
    main()
