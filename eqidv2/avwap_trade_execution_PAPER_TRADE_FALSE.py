# -*- coding: utf-8 -*-
"""
avwap_trade_execution_PAPER_TRADE_FALSE.py - Live Trade Executor (Zerodha Real)
================================================================================

Watches the daily signal CSV produced by avwap_live_signal_generator.py and
places REAL orders on Zerodha via KiteConnect.

For each new signal:
  1. Places a MARKET entry order (MIS product for intraday)
  2. Waits for fill confirmation
  3. Places a LIMIT target order + SL-M stop-loss order
  4. Monitors target/SL in a background thread
  5. If either fills -> cancels the other (with retry)
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
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

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
OPEN_TRADES_STATE_PATTERN = "open_live_trades_state_{}.json"

# Trading hours
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
FORCED_CLOSE_TIME = dt_time(15, 20)  # aligned closer to backtest EOD; safe before broker auto-square-off

# Order monitoring
ORDER_POLL_SEC = 3
FILL_WAIT_TIMEOUT_SEC = 60
MAX_CANCEL_RETRIES = 3
CANCEL_RETRY_WAIT_SEC = 2

# Exposure limits
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
    "signal_entry_datetime_ist",
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
    "signal_entry_datetime_ist", "signal_bar_time_ist",
]

# Column name mapping: signal generator CSV name -> executor expected name
_SIGNAL_COL_MAP = {
    "entry":          "entry_price",
    "sl":             "stop_price",
    "target":         "target_price",
    "impulse":        "impulse_type",
    "created_ts_ist": "signal_datetime",
    "bar_time_ist":   "signal_entry_datetime_ist",
    "signal_bar_time_ist": "signal_entry_datetime_ist",
    "conf_mult":      "confidence_multiplier",
}

# Default position size for quantity calculation
DEFAULT_POSITION_SIZE = 50_000

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


def _place_exit_legs(
    ticker: str,
    exit_txn: str,
    quantity: int,
    target_price: float,
    stop_price: float,
) -> Tuple[str, str]:
    """
    Place target LIMIT + stop-loss SLM exit legs and return their order IDs.
    """
    target_order_id = str(
        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker,
            transaction_type=exit_txn,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=target_price,
            validity=kite.VALIDITY_DAY,
            tag="AVWAPTarget",
        )
    )
    sl_order_id = str(
        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker,
            transaction_type=exit_txn,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=stop_price,
            validity=kite.VALIDITY_DAY,
            tag="AVWAPStopLoss",
        )
    )
    return target_order_id, sl_order_id


# ============================================================================
# EXECUTED SIGNALS TRACKING
# ============================================================================
def load_executed_signals() -> Set[str]:
    if os.path.exists(EXECUTED_SIGNALS_FILE):
        try:
            with open(EXECUTED_SIGNALS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("date") != datetime.now(IST).strftime("%Y-%m-%d"):
                    return set()
                return set(data.get("signals", []))
        except (json.JSONDecodeError, KeyError):
            try:
                bad = EXECUTED_SIGNALS_FILE + ".corrupt"
                os.replace(EXECUTED_SIGNALS_FILE, bad)
            except Exception:
                pass
            return set()
    return set()


def save_executed_signals(executed: Set[str]) -> None:
    payload = {
        "date": datetime.now(IST).strftime("%Y-%m-%d"),
        "signals": sorted(set(executed)),
    }
    _atomic_write_json(EXECUTED_SIGNALS_FILE, payload)


# ============================================================================
# LIVE TRADE MANAGEMENT
# ============================================================================
@dataclass
class LiveTradeResult:
    trade_id: str = ""
    signal_id: str = ""
    signal_datetime: str = ""
    signal_entry_datetime_ist: str = ""
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
active_positions: Dict[str, Dict[str, Any]] = {}
active_positions_lock = threading.Lock()
inflight_signals: Set[str] = set()
inflight_signals_lock = threading.Lock()
executed_lock = threading.Lock()
daily_pnl: Dict[str, Any] = {"total": 0.0, "wins": 0, "losses": 0, "trades": 0}
daily_pnl_lock = threading.Lock()
dispatch_lockdown_reason: Optional[str] = None
dispatch_lockdown_lock = threading.Lock()

# Capital / position tracking (margin, not notional - accounts for MIS leverage)
capital_deployed: Dict[str, float] = {}   # signal_id -> margin blocked
capital_lock = threading.Lock()


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: object, default: int = 1) -> int:
    try:
        parsed = int(float(value))
        return parsed if parsed > 0 else int(default)
    except Exception:
        return int(default)


def _calc_pnl(side: str, entry_price: float, exit_price: float, qty: int) -> Tuple[float, float]:
    if side.upper() == "SHORT":
        pnl_rs = (entry_price - exit_price) * qty
    else:
        pnl_rs = (exit_price - entry_price) * qty
    pnl_pct = (pnl_rs / (entry_price * qty) * 100) if (entry_price > 0 and qty > 0) else 0.0
    return float(pnl_rs), float(pnl_pct)


def _atomic_write_json(path: str, payload: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _open_trades_state_path(for_date: Optional[str] = None) -> str:
    day = for_date or datetime.now(IST).strftime("%Y-%m-%d")
    return os.path.join(SIGNAL_DIR, OPEN_TRADES_STATE_PATTERN.format(day))


def _persist_open_trades_state() -> None:
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    with active_positions_lock:
        open_rows = [dict(v) for v in active_positions.values()]
    payload = {
        "date": today_str,
        "open_trades": open_rows,
        "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
    }
    _atomic_write_json(_open_trades_state_path(today_str), payload)


def _load_open_trades_state(today_str: str) -> Dict[str, Dict[str, Any]]:
    state_path = _open_trades_state_path(today_str)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        log.warning(f"[RESTORE] Could not parse open-trades state file: {e}")
        return {}

    if str(payload.get("date", "")).strip() != today_str:
        return {}

    rows = payload.get("open_trades", [])
    if isinstance(rows, dict):
        rows = list(rows.values())
    if not isinstance(rows, list):
        return {}

    restored: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("signal_id", "")).strip()
        if not sid:
            continue
        restored[sid] = dict(row)
    return restored


def _set_dispatch_lockdown(reason: Optional[str]) -> None:
    global dispatch_lockdown_reason
    with dispatch_lockdown_lock:
        dispatch_lockdown_reason = reason.strip() if reason else None


def _get_dispatch_lockdown() -> Optional[str]:
    with dispatch_lockdown_lock:
        return dispatch_lockdown_reason


def _release_capacity(signal_id: str) -> None:
    with capital_lock:
        capital_deployed.pop(signal_id, None)


def _is_market_open_now(now_ist: Optional[datetime] = None) -> bool:
    ts = now_ist or datetime.now(IST)
    t = ts.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def _check_risk_limits(signal: dict) -> Optional[str]:
    """
    Check open positions and capital deployed.
    Returns a rejection reason string, or None if the trade is allowed.
    """
    with capital_lock:
        open_count = len(capital_deployed)
        total_deployed = sum(capital_deployed.values())

    if open_count >= MAX_OPEN_POSITIONS:
        return f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})"

    entry_price = _safe_float(signal.get("entry_price", 0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)
    margin = (entry_price * quantity) / INTRADAY_LEVERAGE
    if (total_deployed + margin) > MAX_CAPITAL_DEPLOYED_RS:
        return (
            f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
            f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})"
        )

    return None


def _reserve_capacity_for_signal(signal_id: str, signal: dict) -> Tuple[bool, str, float]:
    """
    Atomically validate risk and reserve margin for a signal to avoid races
    between concurrent CSV callbacks/threads.
    """
    entry_price = _safe_float(signal.get("entry_price", 0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)
    margin = (entry_price * quantity) / INTRADAY_LEVERAGE if INTRADAY_LEVERAGE > 0 else 0.0
    margin = float(max(0.0, margin))

    with capital_lock:
        open_count = len(capital_deployed)
        total_deployed = float(sum(capital_deployed.values()))

        if signal_id in capital_deployed:
            return False, "already reserved", float(capital_deployed.get(signal_id, 0.0))

        if open_count >= MAX_OPEN_POSITIONS:
            return False, f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})", 0.0

        if (total_deployed + margin) > MAX_CAPITAL_DEPLOYED_RS:
            return (
                False,
                f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
                f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})",
                0.0,
            )

        capital_deployed[signal_id] = margin
        return True, "ok", margin


def execute_live_trade(signal: dict, resume_mode: bool = False) -> None:
    """
    Execute or resume a complete live MIS trade on Zerodha:
      1. Market entry order (new trades only)
      2. Wait for fill (new trades only)
      3. Place target (LIMIT) + stop-loss (SL-M), or restore existing legs
      4. Monitor until target/SL fills or 15:20 forced close
    """
    ticker = str(signal["ticker"]).upper()
    side = str(signal["side"]).upper()
    signal_id = str(signal.get("signal_id", ""))
    signal_entry_price = _safe_float(signal.get("entry_price", 0), 0.0)
    signal_stop = _safe_float(signal.get("stop_price", 0), 0.0)
    signal_target = _safe_float(signal.get("target_price", 0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)

    trade_start_ist = datetime.now(IST)
    default_trade_id = f"LT-{signal_id[:8]}-{trade_start_ist.strftime('%H%M%S')}"
    trade_id = str(signal.get("trade_id", "")).strip() or default_trade_id
    today = trade_start_ist.date()
    forced_close_dt = IST.localize(datetime.combine(today, FORCED_CLOSE_TIME))

    if side == "SHORT":
        entry_txn = "SELL"
        exit_txn = "BUY"
    else:
        entry_txn = "BUY"
        exit_txn = "SELL"

    result = LiveTradeResult(
        trade_id=trade_id,
        signal_id=signal_id,
        signal_datetime=signal.get("signal_datetime", ""),
        signal_entry_datetime_ist=signal.get("signal_entry_datetime_ist", ""),
        ticker=ticker,
        side=side,
        setup=signal.get("setup", ""),
        impulse_type=signal.get("impulse_type", ""),
        quantity=quantity,
        entry_price=signal_entry_price,
        stop_price=signal_stop,
        target_price=signal_target,
        quality_score=_safe_float(signal.get("quality_score", 0), 0.0),
    )
    if resume_mode:
        log.info(
            f"[RESUME] Restoring trade {trade_id}: {entry_txn} {ticker} qty={quantity} "
            f"| SL={signal_stop} TGT={signal_target}"
        )
    else:
        log.info(
            f"[LIVE] Starting trade {trade_id}: {entry_txn} {ticker} qty={quantity} "
            f"| SL={signal_stop} TGT={signal_target}"
        )

    target_order_id = str(signal.get("target_order_id", "")).strip() if resume_mode else ""
    sl_order_id = str(signal.get("sl_order_id", "")).strip() if resume_mode else ""
    trade_closed = False

    def _sync_active_position(stage: str) -> None:
        with active_positions_lock:
            active_positions[signal_id] = {
                "signal_id": signal_id,
                "trade_id": result.trade_id,
                "ticker": ticker,
                "side": side,
                "quantity": int(quantity),
                "entry_price": float(result.entry_price),
                "filled_price": float(result.filled_price if result.filled_price > 0 else result.entry_price),
                "stop_price": float(result.stop_price),
                "target_price": float(result.target_price),
                "entry_order_id": str(result.entry_order_id),
                "target_order_id": str(result.target_order_id),
                "sl_order_id": str(result.sl_order_id),
                "entry_time": str(result.entry_time),
                "signal_datetime": str(result.signal_datetime),
                "signal_entry_datetime_ist": str(result.signal_entry_datetime_ist),
                "setup": str(result.setup),
                "impulse_type": str(result.impulse_type),
                "quality_score": float(result.quality_score),
                "stage": stage,
                "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            }
        _persist_open_trades_state()

    try:
        if resume_mode:
            result.entry_order_id = str(signal.get("entry_order_id", "")).strip()
            result.target_order_id = target_order_id
            result.sl_order_id = sl_order_id
            result.entry_time = str(signal.get("entry_time", "")).strip()
            result.filled_price = _safe_float(signal.get("filled_price", signal_entry_price), signal_entry_price)
            if result.filled_price <= 0:
                log.error(
                    f"[RESUME] Missing filled entry price for {ticker} (signal_id={signal_id[:12]}). "
                    "Keeping position state for manual reconciliation."
                )
                _sync_active_position("resume_missing_filled_price")
                return

            margin = (result.filled_price * quantity) / INTRADAY_LEVERAGE
            with capital_lock:
                capital_deployed[signal_id] = margin
            _sync_active_position("restored")
        else:
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
                cancel_order_safe(kite.VARIETY_REGULAR, str(entry_order_id))
                result.outcome = "ENTRY_FAILED"
                result.exit_price = signal_entry_price
                trade_closed = True
            else:
                result.filled_price = filled_price
                log.info(f"[LIVE] Entry filled: {ticker} @ {filled_price}")

                # Register margin deployment (notional / leverage)
                margin = (filled_price * quantity) / INTRADAY_LEVERAGE
                with capital_lock:
                    capital_deployed[signal_id] = margin

                # Recalculate SL/target based on actual fill price
                if side == "SHORT":
                    sl_offset = abs(signal_stop - signal_entry_price)
                    tgt_offset = abs(signal_entry_price - signal_target)
                    sl_price = round_to_tick(filled_price + sl_offset)
                    tgt_price = round_to_tick(filled_price - tgt_offset)
                else:
                    sl_offset = abs(signal_entry_price - signal_stop)
                    tgt_offset = abs(signal_target - signal_entry_price)
                    sl_price = round_to_tick(filled_price - sl_offset)
                    tgt_price = round_to_tick(filled_price + tgt_offset)

                result.stop_price = sl_price
                result.target_price = tgt_price
                _sync_active_position("entry_filled")

        # No open position to monitor
        if trade_closed:
            pass
        else:
            # Ensure exit leg orders exist (for new trades and resumed trades alike)
            if not target_order_id or not sl_order_id:
                try:
                    target_order_id, sl_order_id = _place_exit_legs(
                        ticker=ticker,
                        exit_txn=exit_txn,
                        quantity=quantity,
                        target_price=result.target_price,
                        stop_price=result.stop_price,
                    )
                    result.target_order_id = target_order_id
                    result.sl_order_id = sl_order_id
                    _sync_active_position("exit_legs_placed")
                    if resume_mode:
                        log.info(
                            f"[RESUME] Rebuilt exit legs for {ticker}: "
                            f"target_id={target_order_id} sl_id={sl_order_id}"
                        )
                    else:
                        log.info(
                            f"[LIVE] Exit legs placed for {ticker}: "
                            f"target_id={target_order_id} sl_id={sl_order_id}"
                        )
                except Exception as e:
                    log.error(f"[LIVE] Exit leg placement failed for {ticker}: {e}")
                    if target_order_id:
                        cancel_order_safe(kite.VARIETY_REGULAR, target_order_id)
                    if sl_order_id:
                        cancel_order_safe(kite.VARIETY_REGULAR, sl_order_id)
                    if resume_mode:
                        _sync_active_position("resume_pending_exit_legs")
                        return

                    # New trade fallback: attempt immediate market close.
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
                            tag="AVWAPExitSetupFailClose",
                        )
                        result.close_order_id = str(close_order_id)
                        close_price = wait_for_fill(close_order_id, timeout_sec=30)
                        if close_price and close_price > 0:
                            result.exit_price = close_price
                            result.outcome = "EXIT_SETUP_FAILED_FORCE_CLOSED"
                            trade_closed = True
                            log.warning(
                                f"[LIVE] Exit setup fallback close for {ticker}: "
                                f"close_order_id={close_order_id} exit={result.exit_price}"
                            )
                        else:
                            log.error(
                                f"[LIVE] Exit setup fallback close unconfirmed for {ticker}; "
                                "keeping state for restart reconciliation."
                            )
                            _sync_active_position("exit_setup_failed_needs_reconcile")
                            return
                    except Exception as close_err:
                        log.error(f"[LIVE] Exit setup fallback close failed for {ticker}: {close_err}")
                        _sync_active_position("exit_setup_failed_needs_reconcile")
                        return
            else:
                result.target_order_id = target_order_id
                result.sl_order_id = sl_order_id
                _sync_active_position("monitoring")

            # ---- STEP 5: Monitor until target/SL/forced close ----
            while (not trade_closed) and result.target_order_id and result.sl_order_id:
                now_ist = datetime.now(IST)

                if now_ist >= forced_close_dt:
                    log.info(f"[LIVE] EOD forced close for {ticker}")
                    cancel_order_safe(kite.VARIETY_REGULAR, result.target_order_id)
                    cancel_order_safe(kite.VARIETY_REGULAR, result.sl_order_id)
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
                        if close_price and close_price > 0:
                            result.exit_price = close_price
                            result.outcome = "EOD_CLOSE"
                            trade_closed = True
                            log.info(
                                f"[LIVE] Forced close: {ticker} @ {result.exit_price} "
                                f"ID={close_order_id}"
                            )
                        else:
                            log.error(
                                f"[LIVE] Forced close unconfirmed for {ticker}; "
                                "keeping state for restart reconciliation."
                            )
                            _sync_active_position("forced_close_unconfirmed")
                            return
                    except Exception as e:
                        log.error(f"[LIVE] Forced close failed for {ticker}: {e}")
                        _sync_active_position("forced_close_failed")
                        return
                    break

                try:
                    orders = kite.orders()
                except Exception as e:
                    log.warning(f"[LIVE] Order poll error: {e}")
                    time.sleep(ORDER_POLL_SEC)
                    continue

                target_filled = False
                sl_filled = False
                target_fill_price = result.target_price
                sl_fill_price = result.stop_price

                for o in orders:
                    oid = str(o.get("order_id", ""))
                    status = str(o.get("status", "")).upper()
                    if oid == result.target_order_id and status == "COMPLETE":
                        target_filled = True
                        target_fill_price = float(o.get("average_price", result.target_price))
                    if oid == result.sl_order_id and status == "COMPLETE":
                        sl_filled = True
                        sl_fill_price = float(o.get("average_price", result.stop_price))

                if target_filled:
                    log.info(
                        f"[LIVE] TARGET HIT: {ticker} @ {target_fill_price} "
                        f"-> cancelling SL {result.sl_order_id}"
                    )
                    cancel_order_safe(kite.VARIETY_REGULAR, result.sl_order_id)
                    result.exit_price = target_fill_price
                    result.outcome = "TARGET"
                    trade_closed = True
                    break

                if sl_filled:
                    log.info(
                        f"[LIVE] SL TRIGGERED: {ticker} @ {sl_fill_price} "
                        f"-> cancelling Target {result.target_order_id}"
                    )
                    cancel_order_safe(kite.VARIETY_REGULAR, result.target_order_id)
                    result.exit_price = sl_fill_price
                    result.outcome = "SL"
                    trade_closed = True
                    break

                time.sleep(ORDER_POLL_SEC)

    except Exception as e:
        if resume_mode:
            log.error(f"[RESUME] Monitor error for {ticker}: {e}")
            log.error(traceback.format_exc())
            _sync_active_position("monitor_error")
            return

        log.error(f"[LIVE] Trade execution error for {ticker}: {e}")
        log.error(traceback.format_exc())
        result.outcome = f"ERROR: {str(e)[:80]}"
        result.exit_price = result.filled_price or signal_entry_price
        # If a real position may still be open, keep it in state for restart reconciliation.
        if result.filled_price > 0:
            _sync_active_position("error_needs_reconcile")
            return
        trade_closed = True

    # If trade has not actually closed, preserve state and exit without booking P&L.
    if not trade_closed and result.outcome not in {"ENTRY_FAILED"}:
        if resume_mode:
            _sync_active_position("resume_incomplete")
        return

    result.exit_time = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
    ep = result.filled_price if result.filled_price > 0 else signal_entry_price
    xp = result.exit_price if result.exit_price > 0 else ep
    pnl_rs, pnl_pct = _calc_pnl(side, ep, xp, quantity)
    result.pnl_rs = round(pnl_rs, 2)
    result.pnl_pct = round(pnl_pct, 4)

    _log_trade_result(result)

    with daily_pnl_lock:
        daily_pnl["total"] += result.pnl_rs
        daily_pnl["trades"] += 1
        if result.pnl_rs > 0:
            daily_pnl["wins"] += 1
        elif result.pnl_rs < 0:
            daily_pnl["losses"] += 1
        _save_summary()
        day_total = float(daily_pnl["total"])

    log.info(
        f"[LIVE] RESULT {side} {ticker} | {result.outcome} | "
        f"Entry={ep} Exit={xp} | "
        f"P&L: Rs.{result.pnl_rs:+,.2f} ({result.pnl_pct:+.2f}%) | "
        f"Day total: Rs.{day_total:+,.2f}"
    )

    _release_capacity(signal_id)
    with active_positions_lock:
        active_positions.pop(signal_id, None)
    _persist_open_trades_state()


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
            "signal_entry_datetime_ist": result.signal_entry_datetime_ist,
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
        _atomic_write_json(SUMMARY_FILE, summary)
    except Exception:
        pass


def _load_closed_ids_and_realized_summary(
    trade_csv_path: str,
    today_date: date,
) -> Tuple[Set[str], Dict[str, float]]:
    closed_ids: Set[str] = set()
    realized_total = 0.0
    realized_trades = 0
    realized_wins = 0
    realized_losses = 0

    if os.path.exists(trade_csv_path) and os.path.getsize(trade_csv_path) > 0:
        try:
            df = pd.read_csv(
                trade_csv_path,
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                on_bad_lines="warn",
                engine="python",
            )
            if not df.empty:
                for row in df.to_dict("records"):
                    row_date = _signal_ist_date(row)
                    if row_date is not None and row_date != today_date:
                        continue
                    sid = str(row.get("signal_id", "")).strip()
                    if sid:
                        closed_ids.add(sid)
                    pnl_rs = _safe_float(row.get("pnl_rs", 0.0), 0.0)
                    realized_total += pnl_rs
                    realized_trades += 1
                    if pnl_rs > 0:
                        realized_wins += 1
                    elif pnl_rs < 0:
                        realized_losses += 1
        except Exception as e:
            log.warning(f"[RESTORE] Could not parse live trade CSV: {e}")

    return closed_ids, {
        "realized_total": float(realized_total),
        "realized_trades": float(realized_trades),
        "realized_wins": float(realized_wins),
        "realized_losses": float(realized_losses),
    }


def _restore_intraday_runtime_state(
    signal_csv_path: str,
    trade_csv_path: str,
    executed: Set[str],
) -> Tuple[Dict[str, float], List[dict]]:
    """
    Restore runtime state after mid-session restart using persisted open-trade state.
    """
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    today_date = datetime.now(IST).date()

    signal_rows = read_signals_csv(signal_csv_path)
    signals_by_id: Dict[str, dict] = {}
    for sig in signal_rows:
        sid = str(sig.get("signal_id", "")).strip()
        if sid:
            signals_by_id[sid] = sig

    closed_ids, realized = _load_closed_ids_and_realized_summary(
        trade_csv_path=trade_csv_path,
        today_date=today_date,
    )

    with daily_pnl_lock:
        daily_pnl["total"] = float(realized["realized_total"])
        daily_pnl["trades"] = int(realized["realized_trades"])
        daily_pnl["wins"] = int(realized["realized_wins"])
        daily_pnl["losses"] = int(realized["realized_losses"])
        _save_summary()

    raw_open_state = _load_open_trades_state(today_str)
    restored_positions: Dict[str, Dict[str, Any]] = {}
    resume_signals: List[dict] = []

    for sid, pos in raw_open_state.items():
        if sid in closed_ids:
            continue

        base = signals_by_id.get(sid, {})
        ticker = str(pos.get("ticker") or base.get("ticker") or "").upper()
        side = str(pos.get("side") or base.get("side") or "").upper()
        qty = _safe_int(pos.get("quantity", base.get("quantity", 1)), 1)
        entry_price = _safe_float(pos.get("entry_price", base.get("entry_price", 0.0)), 0.0)
        filled_price = _safe_float(pos.get("filled_price", entry_price), entry_price)
        stop_price = _safe_float(pos.get("stop_price", base.get("stop_price", 0.0)), 0.0)
        target_price = _safe_float(pos.get("target_price", base.get("target_price", 0.0)), 0.0)
        entry_order_id = str(pos.get("entry_order_id", "")).strip()
        target_order_id = str(pos.get("target_order_id", "")).strip()
        sl_order_id = str(pos.get("sl_order_id", "")).strip()
        trade_id = str(pos.get("trade_id", "")).strip() or f"RESTORE-{sid[:8]}"
        entry_time = str(pos.get("entry_time", "")).strip()

        if not ticker or side not in {"LONG", "SHORT"}:
            continue
        if qty <= 0 or filled_price <= 0 or stop_price <= 0 or target_price <= 0:
            continue

        restored_positions[sid] = {
            "signal_id": sid,
            "trade_id": trade_id,
            "ticker": ticker,
            "side": side,
            "quantity": int(qty),
            "entry_price": float(entry_price if entry_price > 0 else filled_price),
            "filled_price": float(filled_price),
            "stop_price": float(stop_price),
            "target_price": float(target_price),
            "entry_order_id": entry_order_id,
            "target_order_id": target_order_id,
            "sl_order_id": sl_order_id,
            "entry_time": entry_time,
            "signal_datetime": str(base.get("signal_datetime", pos.get("signal_datetime", ""))),
            "signal_entry_datetime_ist": str(
                base.get("signal_entry_datetime_ist", pos.get("signal_entry_datetime_ist", ""))
            ),
            "setup": str(base.get("setup", pos.get("setup", ""))),
            "impulse_type": str(base.get("impulse_type", pos.get("impulse_type", ""))),
            "quality_score": _safe_float(base.get("quality_score", pos.get("quality_score", 0.0)), 0.0),
            "stage": str(pos.get("stage", "restored")),
            "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }
        resume_signals.append(
            {
                "signal_id": sid,
                "trade_id": trade_id,
                "ticker": ticker,
                "side": side,
                "quantity": int(qty),
                "entry_price": float(entry_price if entry_price > 0 else filled_price),
                "filled_price": float(filled_price),
                "stop_price": float(stop_price),
                "target_price": float(target_price),
                "entry_order_id": entry_order_id,
                "target_order_id": target_order_id,
                "sl_order_id": sl_order_id,
                "entry_time": entry_time,
                "signal_datetime": str(base.get("signal_datetime", pos.get("signal_datetime", ""))),
                "signal_entry_datetime_ist": str(
                    base.get("signal_entry_datetime_ist", pos.get("signal_entry_datetime_ist", ""))
                ),
                "setup": str(base.get("setup", pos.get("setup", ""))),
                "impulse_type": str(base.get("impulse_type", pos.get("impulse_type", ""))),
                "quality_score": _safe_float(base.get("quality_score", pos.get("quality_score", 0.0)), 0.0),
            }
        )

    with active_positions_lock:
        active_positions.clear()
        active_positions.update(restored_positions)

    with capital_lock:
        capital_deployed.clear()
        for sid, pos in restored_positions.items():
            margin = float(pos["filled_price"] * pos["quantity"] / INTRADAY_LEVERAGE)
            capital_deployed[sid] = margin

    with executed_lock:
        executed.difference_update(closed_ids)
        executed.update(restored_positions.keys())

    _persist_open_trades_state()
    with capital_lock:
        deployed_margin = float(sum(capital_deployed.values()))
    stats = {
        "signals_today": float(len(signals_by_id)),
        "executed_loaded": float(len(executed)),
        "closed_today": float(len(closed_ids)),
        "open_restored": float(len(restored_positions)),
        "realized_trades": float(realized["realized_trades"]),
        "realized_total": float(realized["realized_total"]),
        "deployed_margin": float(deployed_margin),
    }
    return stats, resume_signals


def _detect_orphan_broker_positions() -> List[dict]:
    """
    Detect open MIS positions present at broker but not tracked by executor state.
    """
    if kite is None:
        return []
    try:
        payload = kite.positions()
    except Exception as e:
        log.warning(f"[RESTORE] Could not fetch broker positions for safety check: {e}")
        return []

    net_rows = payload.get("net", []) if isinstance(payload, dict) else []
    if not isinstance(net_rows, list):
        return []

    with active_positions_lock:
        tracked = {
            str(pos.get("ticker", "")).upper(): int(_safe_int(pos.get("quantity", 0), 0))
            for pos in active_positions.values()
        }

    orphans: List[dict] = []
    for row in net_rows:
        ticker = str(row.get("tradingsymbol", "")).upper()
        product = str(row.get("product", "")).upper()
        qty = int(_safe_int(row.get("quantity", 0), 0))
        if qty == 0:
            continue
        if product and product != "MIS":
            continue
        tracked_qty = tracked.get(ticker)
        if tracked_qty is None or abs(tracked_qty) != abs(qty):
            orphans.append(
                {
                    "ticker": ticker,
                    "quantity": qty,
                    "product": product or "?",
                    "average_price": _safe_float(row.get("average_price", 0.0), 0.0),
                }
            )
    return orphans


# ============================================================================
# SIGNAL NORMALISATION
# ============================================================================
def _normalize_signal(raw: dict) -> dict:
    """
    Map signal-generator CSV column names to the names the executor expects.
    Also computes a quantity from DEFAULT_POSITION_SIZE when the CSV has none.
    """
    sig = {}
    for k, v in raw.items():
        mapped = _SIGNAL_COL_MAP.get(k, k)
        sig[mapped] = v

    if not sig.get("signal_entry_datetime_ist"):
        sig["signal_entry_datetime_ist"] = (
            sig.get("signal_bar_time_ist")
            or sig.get("bar_time_ist")
            or sig.get("signal_datetime")
            or ""
        )

    if not sig.get("signal_datetime"):
        sig["signal_datetime"] = sig.get("signal_entry_datetime_ist", "")

    # Compute quantity if missing (generator does not emit one)
    if "quantity" not in sig or pd.isna(sig.get("quantity")):
        entry = _safe_float(sig.get("entry_price", 0), 0.0)
        if entry > 0:
            sig["quantity"] = max(1, int(DEFAULT_POSITION_SIZE / entry))
        else:
            sig["quantity"] = 1
    else:
        sig["quantity"] = _safe_int(sig.get("quantity", 1), 1)

    return sig


def _parse_ist_signal_ts(value: object) -> Optional[pd.Timestamp]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize(IST)
        else:
            ts = ts.tz_convert(IST)
        return ts
    except Exception:
        return None


def _signal_ist_date(sig: dict) -> Optional[date]:
    for key in ("signal_entry_datetime_ist", "signal_bar_time_ist", "bar_time_ist", "signal_datetime"):
        ts = _parse_ist_signal_ts(sig.get(key, ""))
        if ts is not None:
            return ts.date()
    return None


def _filter_today_signals(signals: List[dict]) -> Tuple[List[dict], int]:
    today = datetime.now(IST).date()
    filtered: List[dict] = []
    dropped = 0
    for sig in signals:
        sig_date = _signal_ist_date(sig)
        if sig_date == today:
            filtered.append(sig)
        else:
            dropped += 1
    return filtered, dropped


def _sanitize_today_trade_csv() -> Tuple[int, int]:
    """
    Ensure today's live trade CSV only has rows whose signal time is from today (IST).
    Returns (rows_before, rows_removed).
    """
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    today_date = datetime.now(IST).date()
    csv_path = os.path.join(SIGNAL_DIR, TRADE_LOG_PATTERN.format(today_str))
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return 0, 0

    try:
        df = pd.read_csv(
            csv_path,
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
            engine="python",
        )
    except Exception:
        return 0, 0

    if df.empty:
        return 0, 0

    rows_before = int(len(df))
    keep_mask = []
    for _, row in df.iterrows():
        row_date = _signal_ist_date(row)
        keep_mask.append(row_date == today_date if row_date is not None else False)

    df_today = df[pd.Series(keep_mask, index=df.index)].copy()
    rows_removed = rows_before - int(len(df_today))
    if rows_removed <= 0:
        return rows_before, 0

    for c in TRADE_LOG_COLUMNS:
        if c not in df_today.columns:
            df_today[c] = ""
    df_today = df_today[TRADE_LOG_COLUMNS]
    df_today.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
    return rows_before, rows_removed


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
        signals = [_normalize_signal(row) for row in df.to_dict("records")]
        signals, dropped = _filter_today_signals(signals)
        if dropped > 0:
            log.warning(
                f"[CSV] Ignored {dropped} stale signal row(s) not matching today's IST date."
            )
        return signals
    except Exception as e:
        log.error(f"Error reading signals CSV: {e}")
        return []


# ============================================================================
# SIGNAL PROCESSOR
# ============================================================================
def _launch_trade_thread(
    signal: dict,
    signal_id: str,
    trade_semaphore: threading.Semaphore,
    resume_mode: bool = False,
) -> bool:
    def _run_trade(sig=signal, sid=signal_id, is_resume=resume_mode):
        with inflight_signals_lock:
            if sid in inflight_signals:
                return
            inflight_signals.add(sid)
        trade_semaphore.acquire()
        try:
            execute_live_trade(sig, resume_mode=is_resume)
        except Exception as e:
            mode = "RESUME" if is_resume else "LIVE"
            log.error(f"{mode} trade thread error for {sig.get('ticker', '?')}: {e}")
            log.error(traceback.format_exc())
        finally:
            trade_semaphore.release()
            with inflight_signals_lock:
                inflight_signals.discard(sid)
            with active_trades_lock:
                active_trades.pop(sid, None)

    try:
        t = threading.Thread(
            target=_run_trade,
            daemon=True,
            name=f"{'resume' if resume_mode else 'live'}-trade-{signal_id[:8]}",
        )
        with active_trades_lock:
            active_trades[signal_id] = t
        t.start()
        return True
    except Exception:
        with active_trades_lock:
            active_trades.pop(signal_id, None)
        with inflight_signals_lock:
            inflight_signals.discard(signal_id)
        return False


def start_resumed_trade_monitors(
    resumed_signals: List[dict],
    trade_semaphore: threading.Semaphore,
    dry_run: bool,
) -> int:
    if dry_run or not resumed_signals:
        return 0

    started = 0
    for signal in resumed_signals:
        sid = str(signal.get("signal_id", "")).strip()
        if not sid:
            continue
        ok = _launch_trade_thread(
            signal=signal,
            signal_id=sid,
            trade_semaphore=trade_semaphore,
            resume_mode=True,
        )
        if ok:
            started += 1
        else:
            log.error(f"[RESUME] Failed to launch monitor thread for signal_id={sid[:12]}")
    return started


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
    lockdown = _get_dispatch_lockdown()
    if lockdown:
        log.error(f"[SAFETY] New entries blocked: {lockdown}")
        return executed

    if not _is_market_open_now():
        return executed

    signals = read_signals_csv(csv_path)
    if not signals:
        return executed

    new_count = 0
    executed_changed = False
    for signal in signals:
        signal_id = str(signal.get("signal_id", "")).strip()
        if not signal_id:
            continue

        with executed_lock:
            if signal_id in executed:
                continue

        with inflight_signals_lock:
            if signal_id in inflight_signals:
                continue

        # Validate required fields
        required = ["ticker", "side", "entry_price", "stop_price", "target_price"]
        missing = [k for k in required if not signal.get(k)]
        if missing:
            log.warning(f"Signal {signal_id[:12]} missing fields: {missing}. Skipping.")
            continue

        # Risk + margin reservation (atomic)
        allowed, reason, reserved_margin = _reserve_capacity_for_signal(signal_id, signal)
        if not allowed:
            log.warning(
                f"[RISK] Rejecting {signal.get('side', '?')} "
                f"{signal.get('ticker', '?')}: {reason}"
            )
            continue

        # Mark executed only after reservation succeeds.
        with executed_lock:
            executed.add(signal_id)
            executed_changed = True

        ticker = signal.get("ticker", "?")
        side = signal.get("side", "?")
        entry_p = signal.get("entry_price", "?")

        if dry_run:
            log.info(
                f"[DRY-RUN] Would execute: {side} {ticker} @ {entry_p} | "
                f"SL={signal.get('stop_price')} TGT={signal.get('target_price')}"
            )
            _release_capacity(signal_id)
            continue

        sid = signal_id
        started = _launch_trade_thread(
            signal=signal,
            signal_id=sid,
            trade_semaphore=trade_semaphore,
            resume_mode=False,
        )
        if not started:
            _release_capacity(sid)
            with executed_lock:
                executed.discard(sid)
                executed_changed = True
            log.error(f"[DISPATCH] Failed to launch trade thread for signal_id={sid[:12]}")
            continue

        log.info(
            f"[DISPATCH] Launched LIVE trade: {side} {ticker} @ {entry_p} "
            f"| reserved_margin=Rs.{reserved_margin:,.2f} | ID={signal_id[:12]}"
        )
        new_count += 1

    if executed_changed:
        with executed_lock:
            executed_snapshot = set(executed)
        save_executed_signals(executed_snapshot)

    if new_count > 0:
        with active_trades_lock:
            active_count = len(active_trades)
        log.info(
            f"Dispatched {new_count} new trade(s). "
            f"Active trades: {active_count}"
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
    _set_dispatch_lockdown(None)

    log.info("=" * 65)
    log.info("AVWAP Live Trade Executor - PAPER_TRADE = FALSE")
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

    rows_before, rows_removed = _sanitize_today_trade_csv()
    if rows_removed > 0:
        log.warning(
            f"[CSV] Removed {rows_removed}/{rows_before} stale row(s) "
            "from today's live trade CSV."
        )

    # Resolve today's signal CSV
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, SIGNAL_CSV_PATTERN.format(today_str))
    trade_csv_path = os.path.join(SIGNAL_DIR, TRADE_LOG_PATTERN.format(today_str))

    # Trade concurrency semaphore
    trade_semaphore = threading.Semaphore(args.max_trades)

    # Restore runtime state for mid-session restarts.
    if not args.dry_run:
        restore_stats, resume_signals = _restore_intraday_runtime_state(
            signal_csv_path=csv_path,
            trade_csv_path=trade_csv_path,
            executed=executed,
        )
        save_executed_signals(executed)
        log.info(
            "[RESTORE] "
            f"signals_today={int(restore_stats['signals_today'])} "
            f"executed={int(restore_stats['executed_loaded'])} "
            f"closed_today={int(restore_stats['closed_today'])} "
            f"open_restored={int(restore_stats['open_restored'])} "
            f"realized_trades={int(restore_stats['realized_trades'])} "
            f"realized_total=Rs.{restore_stats['realized_total']:+,.2f} "
            f"deployed_margin=Rs.{restore_stats['deployed_margin']:,.2f}"
        )

        orphan_positions = _detect_orphan_broker_positions()
        if orphan_positions:
            orphan_summary = ", ".join(
                f"{x['ticker']}:{x['quantity']}" for x in orphan_positions
            )
            reason = (
                "orphan MIS broker positions detected; "
                f"manual reconciliation required ({orphan_summary})"
            )
            _set_dispatch_lockdown(reason)
            log.error(f"[SAFETY] {reason}")
            for orphan in orphan_positions:
                log.error(
                    "[SAFETY] ORPHAN "
                    f"{orphan.get('ticker', '?')} qty={orphan.get('quantity', 0)} "
                    f"product={orphan.get('product', '?')} "
                    f"avg={_safe_float(orphan.get('average_price', 0.0), 0.0):.2f}"
                )
        else:
            log.info("[SAFETY] Broker MIS positions match restored runtime state.")

        resumed_count = start_resumed_trade_monitors(
            resumed_signals=resume_signals,
            trade_semaphore=trade_semaphore,
            dry_run=args.dry_run,
        )
        if resumed_count > 0:
            log.info(f"[RESTORE] Resumed {resumed_count} open trade monitor(s).")

    # Callback for watchdog
    def on_csv_change():
        nonlocal executed
        log.info("Signal CSV changed - processing new signals...")
        executed = process_new_signals(
            csv_path, executed, trade_semaphore, dry_run=args.dry_run,
        )

    # Set up watchdog
    os.makedirs(SIGNAL_DIR, exist_ok=True)
    handler = SignalCSVHandler(csv_path, on_csv_change, debounce_sec=2.0)
    observer = Observer()
    observer.schedule(handler, path=SIGNAL_DIR, recursive=False)
    observer.start()
    log.info(f"Watchdog started - monitoring {csv_path}")

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
