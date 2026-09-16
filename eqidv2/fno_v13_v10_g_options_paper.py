"""One-lot ATM option paper execution driven by actual V13-V10-G equity fills.

The two entry workers watch the canonical G equity order books.  A filled LONG
equity order maps to one monthly ATM CE lot; a filled SHORT order maps to one
monthly ATM PE lot.  This module is deliberately paper-only: it imports no
broker order API and uses Kite only for NFO quotes through up to eight
credential lanes.  Historical replay uses only locally persisted candles.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_options_atm_fetch_5min as atm_fetch
import fno_v13_v10_g_live_config as equity_config
import fno_v13_v10_g_options_data as option_data
from fno_v13_v10_g_options_one_lot_execution import (
    _tick,
    option_order_costs,
    simulate_one_lot_trade,
)
from fno_v13_v10_g_paper import paper_portfolio_lock


SCHEMA_VERSION = "fno_v13_v10_g_options_paper_state_v1"
from fno_v13_v10_g_options_config import PROFILE_LABEL, STOP_PCT, TARGET_PCT

OPTIONS_STRATEGY_VERSION = f"FNO_V13_V10_G_OPTIONS_ONE_LOT_ATM_{PROFILE_LABEL}_20260915"
OPTIONS_ENGINE = "one_lot_atm_quote_and_exact_5m_replay_v1"
ROLE_SESSIONS = {
    "long-entry": "fno_v13_v10_g_options_live_long",
    "short-entry": "fno_v13_v10_g_options_live_short",
    "trade-logger": "fno_v13_v10_g_options_trade_logger",
    "net-result": "fno_v13_v10_g_options_net_result",
}
ROLE_REPORTS = {
    role: f"latest_{session}.md" for role, session in ROLE_SESSIONS.items()
}
ROLE_TITLES = {
    "long-entry": "Options V13-V10-G LONG ATM CE Buy Paper Entry Session",
    "short-entry": "Options V13-V10-G SHORT ATM PE Buy Paper Entry Session",
    "trade-logger": "Options V13-V10-G Continuous Paper Trade Log",
    "net-result": "Options V13-V10-G Paper Net Result",
}
OPTIONS_ROOT = common.FNO_ROOT / "v13_v10_g_options_paper"
ORDER_ROOT = OPTIONS_ROOT / "orders"
CONSOLIDATED_ROOT = OPTIONS_ROOT / "consolidated"
AUDIT_ROOT = OPTIONS_ROOT / "bar_audit"
EQUITY_ORDER_ROOT = common.FNO_ROOT / equity_config.LIVE_ROOT_NAME / "orders"
SLIPPAGE_BPS = 10.0
PREVIOUS_BAR_PARTICIPATION = 0.10
PORTFOLIO_CAPITAL_RS = 1_500_000.0
SQUARE_OFF = dtime(15, 15)
SESSION_END = dtime(15, 32)
DEFAULT_MAX_ENTRY_LAG_SEC = 180.0
DEFAULT_MAX_QUOTE_AGE_SEC = 30.0
DEFAULT_MAX_SPREAD_PCT = 10.0
TERMINAL_STATES = frozenset({"CLOSED", "SKIPPED", "BLOCKED", "UNRESOLVED"})


for _path in (OPTIONS_ROOT, ORDER_ROOT, CONSOLIDATED_ROOT, AUDIT_ROOT):
    _path.mkdir(parents=True, exist_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Timestamp is missing")
    return stamp.tz_localize(common.IST) if stamp.tzinfo is None else stamp.tz_convert(common.IST)


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return "" if pd.isna(value) else value.isoformat()
    if isinstance(value, np.generic):
        return _json_value(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value) if not isinstance(value, (str, bytes)) else False:
        return None
    return value


def order_day_dir(session_date: date) -> Path:
    return ORDER_ROOT / session_date.isoformat()


def order_path(session_date: date, signal_id: str) -> Path:
    return order_day_dir(session_date) / f"{signal_id}.json"


def report_path(role: str) -> Path:
    return common.LATEST_DIR / ROLE_REPORTS[role]


def consolidated_csv_path(session_date: date) -> Path:
    return CONSOLIDATED_ROOT / f"options_v13_v10_g_trades_{session_date.isoformat()}.csv"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = common.read_json(path)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _equity_roots(session_date: date, source: str) -> list[tuple[str, Path]]:
    day = session_date.isoformat()
    roots = {
        "PAPER": EQUITY_ORDER_ROOT / "PAPER" / day,
        "LIVE": EQUITY_ORDER_ROOT / "LIVE" / "live_kite_qty1" / day,
    }
    return [(name, roots[name]) for name in (("LIVE", "PAPER") if source == "AUTO" else (source,))]


def load_equity_entries(session_date: date, side: str = "", source: str = "AUTO") -> list[dict[str, Any]]:
    """Return validated, actually filled equity states, with LIVE preferred per signal."""
    source = source.upper()
    if source not in {"AUTO", "PAPER", "LIVE"}:
        raise ValueError("equity source must be AUTO, PAPER, or LIVE")
    selected: dict[str, dict[str, Any]] = {}
    for source_mode, root in _equity_roots(session_date, source):
        if not root.exists():
            continue
        for path in sorted(root.glob("*.json")):
            row = _read_json(path)
            signal_id = str(row.get("signal_id", "")).strip()
            row_side = str(row.get("side", "")).upper()
            if not signal_id or signal_id in selected or row_side not in {"LONG", "SHORT"}:
                continue
            if side and row_side != side.upper():
                continue
            if row.get("strategy_version") != equity_config.STRATEGY_VERSION:
                continue
            if row.get("strategy_fingerprint") != equity_config.strategy_fingerprint():
                continue
            if str(row.get("session_date", "")) != session_date.isoformat():
                continue
            if str(row.get("mode", "")).upper() != source_mode:
                continue
            if str(row.get("status", "")).upper() not in {"OPEN", "CLOSED"}:
                continue
            try:
                entry_price = float(row["entry_price"])
                entry_at = _ist(row["entry_at_ist"])
            except (KeyError, TypeError, ValueError):
                continue
            if not math.isfinite(entry_price) or entry_price <= 0 or entry_at.date() != session_date:
                continue
            symbol = str(row.get("tradingsymbol", "")).strip().upper()
            if not symbol:
                continue
            row.update(
                source_equity_mode=source_mode,
                source_equity_path=str(path),
                source_equity_sha256=_sha256(path),
                _equity_entry_price=entry_price,
                _equity_entry_ts=entry_at,
                _equity_symbol=symbol,
            )
            selected[signal_id] = row
    return sorted(selected.values(), key=lambda row: (_ist(row["_equity_entry_ts"]), str(row["signal_id"])))


def _master_candidates(session_date: date) -> list[Path]:
    day = session_date.isoformat()
    candidates = [
        common.MASTER_DIR / f"options_instrument_master_{day}.parquet",
        common.MASTER_DIR / f"instrument_master_{day}.parquet",
    ]
    research = common.FNO_ROOT / "strategy_research"
    candidates.extend(sorted(research.glob(f"**/nfo_master_{day}.parquet")))
    return candidates


def load_exact_options_master(session_date: date) -> tuple[pd.DataFrame, Path, str]:
    required = {"underlying", "expiry", "strike", "lot_size", "tick_size", "instrument_token", "tradingsymbol", "instrument_type"}
    failures: list[str] = []
    for path in _master_candidates(session_date):
        if not path.is_file():
            continue
        try:
            frame = pd.read_parquet(path)
            if "underlying" not in frame and "name" in frame:
                frame["underlying"] = frame["name"]
            missing = required - set(frame.columns)
            if missing:
                failures.append(f"{path.name}:missing={sorted(missing)}")
                continue
            if "master_date" in frame:
                dates = pd.to_datetime(frame["master_date"], errors="coerce").dropna().dt.date.unique()
                if len(dates) and set(dates) != {session_date}:
                    failures.append(f"{path.name}:master_date_mismatch")
                    continue
            frame = frame.loc[frame["instrument_type"].isin(["CE", "PE"])].copy()
            if frame.empty:
                continue
            frame["underlying"] = frame["underlying"].astype(str).str.upper().str.strip()
            frame["expiry"] = pd.to_datetime(frame["expiry"], errors="coerce").dt.normalize()
            for column in ("strike", "lot_size", "tick_size", "instrument_token"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            frame = frame.dropna(subset=list(required - {"underlying", "tradingsymbol", "instrument_type"}))
            if not frame.empty:
                return frame, path, _sha256(path)
        except (OSError, ValueError, TypeError) as exc:
            failures.append(f"{path.name}:{type(exc).__name__}")
    detail = "; ".join(failures) if failures else "no dated option master file"
    raise FileNotFoundError(f"Exact {session_date} NFO option master unavailable: {detail}")


def map_equity_entry(equity: dict[str, Any], master: pd.DataFrame, master_path: Path, master_sha256: str) -> dict[str, Any]:
    side = str(equity["side"]).upper()
    option_type = "CE" if side == "LONG" else "PE"
    symbol = str(equity["_equity_symbol"])
    entry_price = float(equity["_equity_entry_price"])
    session_date = date.fromisoformat(str(equity["session_date"]))
    expiry = atm_fetch.intended_expiry(master, symbol, session_date, "monthly")
    candidates = master.loc[
        master["underlying"].eq(symbol)
        & master["instrument_type"].eq(option_type)
        & master["expiry"].eq(expiry)
    ].copy()
    if candidates.empty:
        raise ValueError(f"No exact monthly {option_type} contracts for {symbol} expiry {expiry.date()}")
    candidates["atm_distance"] = (candidates["strike"] - entry_price).abs()
    chosen = candidates.sort_values(["atm_distance", "strike", "tradingsymbol"], kind="stable").iloc[0]
    lot_size = int(chosen["lot_size"])
    token = int(chosen["instrument_token"])
    tick_size = float(chosen["tick_size"])
    if lot_size <= 0 or token <= 0 or not math.isfinite(tick_size) or tick_size <= 0:
        raise ValueError("Mapped option contract has invalid execution metadata")
    return {
        "option_type": option_type,
        "position_side": "BUY",
        "option_symbol": str(chosen["tradingsymbol"]),
        "option_instrument_token": token,
        "option_strike": float(chosen["strike"]),
        "option_expiry": expiry.date().isoformat(),
        "atm_distance": float(chosen["atm_distance"]),
        "lot_size": lot_size,
        "lots": 1,
        "quantity": lot_size,
        "tick_size": tick_size,
        "mapping_status": "MAPPED_ATM",
        "mapping_spot_source": "ACTUAL_EQUITY_ENTRY_PRICE",
        "mapping_spot_price": entry_price,
        "master_path": str(master_path),
        "master_sha256": master_sha256,
    }


def create_state(equity: dict[str, Any], mapping: dict[str, Any], now: datetime, execution_source: str) -> dict[str, Any]:
    signal_id = str(equity["signal_id"])
    return {
        "schema_version": SCHEMA_VERSION,
        "strategy_version": OPTIONS_STRATEGY_VERSION,
        "strategy_fingerprint": equity_config.strategy_fingerprint(),
        "equity_strategy_version": equity_config.STRATEGY_VERSION,
        "trade_id": f"OPT_{signal_id}",
        "signal_id": signal_id,
        "session_date": str(equity["session_date"]),
        "side": str(equity["side"]).upper(),
        "equity_symbol": str(equity["_equity_symbol"]),
        "equity_entry_price": float(equity["_equity_entry_price"]),
        "equity_entry_at_ist": _ist(equity["_equity_entry_ts"]).isoformat(),
        "source_equity_mode": equity["source_equity_mode"],
        "source_equity_path": equity["source_equity_path"],
        "source_equity_sha256": equity["source_equity_sha256"],
        **mapping,
        "mode": "PAPER",
        "execution_source": execution_source,
        "status": "WAITING_ENTRY_QUOTE",
        "status_reason": "ATM_CONTRACT_MAPPED_WAITING_GUARDED_QUOTE",
        "stop_pct": STOP_PCT,
        "target_pct": TARGET_PCT,
        "slippage_bps": SLIPPAGE_BPS,
        "portfolio_capital_rs": PORTFOLIO_CAPITAL_RS,
        "entry_raw_price": 0.0,
        "entry_price": 0.0,
        "entry_at_ist": "",
        "stop_price": 0.0,
        "target_price": 0.0,
        "last_price": 0.0,
        "exit_raw_price": 0.0,
        "exit_price": 0.0,
        "exit_at_ist": "",
        "exit_reason": "",
        "entry_costs_rs": 0.0,
        "exit_costs_rs": 0.0,
        "estimated_cost_rs": 0.0,
        "entry_premium_outlay_rs": 0.0,
        "gross_pnl_rs": 0.0,
        "net_pnl_rs": 0.0,
        "net_cash_flow_rs": 0.0,
        "created_at_ist": now.isoformat(timespec="seconds"),
        "updated_at_ist": now.isoformat(timespec="seconds"),
        "engine": OPTIONS_ENGINE,
    }


def load_states(session_date: date, side: str = "") -> list[dict[str, Any]]:
    root = order_day_dir(session_date)
    rows = [_read_json(path) for path in sorted(root.glob("*.json"))] if root.exists() else []
    rows = [row for row in rows if row and (not side or str(row.get("side", "")).upper() == side.upper())]
    return rows


def _validate_state(state: dict[str, Any], session_date: date) -> None:
    if state.get("schema_version") != SCHEMA_VERSION or state.get("strategy_version") != OPTIONS_STRATEGY_VERSION:
        raise ValueError("Foreign option state in isolated one-lot paper book")
    if state.get("strategy_fingerprint") != equity_config.strategy_fingerprint():
        raise ValueError("Option state equity strategy fingerprint mismatch")
    if state.get("mode") != "PAPER" or state.get("session_date") != session_date.isoformat():
        raise ValueError("Option state mode/date mismatch")
    lot_size = int(state.get("lot_size", 0))
    if int(state.get("lots", 0)) != 1 or int(state.get("quantity", 0)) != lot_size or lot_size <= 0:
        raise ValueError("Option paper quantity is not exactly one exchange lot")
    expected_type = "CE" if state.get("side") == "LONG" else "PE" if state.get("side") == "SHORT" else ""
    if state.get("option_type") != expected_type or state.get("position_side") != "BUY":
        raise ValueError("Option direction mapping mismatch")


class KiteQuotePool:
    """Read-only NFO quote pool with bounded pacing and credential failover."""

    def __init__(self, max_apps: int, timeout_sec: float, request_interval_sec: float) -> None:
        self.credentials = common.discover_kite_credentials(max_apps=max_apps)
        self.clients = [common.make_kite_client(item, timeout_sec=timeout_sec) for item in self.credentials]
        if len(self.clients) != 8:
            raise RuntimeError(
                f"Options paper quote pool requires all eight Kite app credentials; found {len(self.clients)}"
            )
        self.request_interval_sec = max(0.34, float(request_interval_sec))
        self._last_call = [0.0 for _ in self.clients]
        self._next = 0
        self.apps_used: set[str] = set()

    @property
    def app_count(self) -> int:
        return len(self.clients)

    def quotes(self, symbols: Iterable[str]) -> dict[str, dict[str, Any]]:
        unique = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
        if not unique:
            return {}
        keys = [f"NFO:{symbol}" for symbol in unique]
        failures = []
        for offset in range(len(self.clients)):
            index = (self._next + offset) % len(self.clients)
            wait = self.request_interval_sec - (time.monotonic() - self._last_call[index])
            if wait > 0:
                time.sleep(wait)
            try:
                self._last_call[index] = time.monotonic()
                payload = self.clients[index].quote(keys)
                self._next = (index + 1) % len(self.clients)
                self.apps_used.add(self.credentials[index].app_name)
                return {str(key).split(":", 1)[-1]: dict(value) for key, value in payload.items()}
            except Exception as exc:
                failures.append(f"{self.credentials[index].app_name}:{type(exc).__name__}")
        raise RuntimeError("All option quote lanes failed: " + ", ".join(failures))


def _depth_price(quote: dict[str, Any], side: str) -> float:
    rows = ((quote.get("depth") or {}).get(side) or [])
    try:
        price = float(rows[0]["price"])
    except (IndexError, KeyError, TypeError, ValueError):
        return math.nan
    return price if math.isfinite(price) and price > 0 else math.nan


def validate_quote(quote: dict[str, Any], state: dict[str, Any], now: datetime, max_age_sec: float, max_spread_pct: float) -> dict[str, float]:
    try:
        last = float(quote.get("last_price"))
        volume = float(quote.get("volume"))
    except (TypeError, ValueError):
        raise ValueError("Option quote has invalid price or volume")
    bid, ask = _depth_price(quote, "buy"), _depth_price(quote, "sell")
    if not all(math.isfinite(value) and value > 0 for value in (last, bid, ask)) or ask < bid:
        raise ValueError("Option quote has invalid top-of-book depth")
    spread_pct = (ask - bid) / ((ask + bid) / 2.0) * 100.0
    if spread_pct > max_spread_pct:
        raise ValueError(f"Option quote spread {spread_pct:.2f}% exceeds {max_spread_pct:.2f}%")
    if not math.isfinite(volume) or volume < int(state["quantity"]):
        raise ValueError("Option quote cumulative volume is below one requested lot")
    stamp_value = quote.get("timestamp") or quote.get("last_trade_time")
    if stamp_value:
        stamp = _ist(stamp_value).to_pydatetime()
        age = (now - stamp).total_seconds()
        if age < -5 or age > max_age_sec:
            raise ValueError(f"Option quote is stale by {age:.1f}s")
    return {"last": last, "bid": bid, "ask": ask, "volume": volume, "spread_pct": spread_pct}


def _book_cash(states: list[dict[str, Any]], session_date: date) -> float:
    cash = PORTFOLIO_CAPITAL_RS
    seen: set[str] = set()
    for state in states:
        _validate_state(state, session_date)
        trade_id = str(state.get("trade_id", ""))
        if not trade_id or trade_id in seen:
            raise ValueError("Duplicate or missing option trade ID")
        seen.add(trade_id)
        cash += float(state.get("net_cash_flow_rs", 0.0) or 0.0)
    return cash


def _enter_state(state: dict[str, Any], quote: dict[str, float], now: datetime, session_date: date) -> None:
    raw = quote["ask"]
    price = _tick(raw * (1.0 + SLIPPAGE_BPS / 10_000.0), float(state["tick_size"]), up=True)
    quantity = int(state["quantity"])
    fees = option_order_costs(price, quantity, "BUY")
    debit = price * quantity + fees["total"]
    others = [row for row in load_states(session_date) if row.get("signal_id") != state.get("signal_id")]
    free_cash = _book_cash(others, session_date)
    if debit > free_cash + 1e-9:
        state.update(status="BLOCKED", status_reason="PAPER_FULL_PREMIUM_CAPITAL_LIMIT", available_cash_rs=free_cash,
                     required_cash_rs=debit, updated_at_ist=now.isoformat(timespec="seconds"))
        return
    stop = _tick(price * (1.0 - STOP_PCT), float(state["tick_size"]), up=True)
    target = _tick(price * (1.0 + TARGET_PCT), float(state["tick_size"]), up=True)
    state.update(
        status="OPEN", status_reason="PAPER_ONE_LOT_ATM_BUY_FILLED_AT_GUARDED_ASK",
        entry_raw_price=raw, entry_price=price, entry_at_ist=now.isoformat(timespec="seconds"),
        stop_price=stop, target_price=target, last_price=quote["last"],
        entry_costs_rs=fees["total"], estimated_cost_rs=fees["total"],
        entry_premium_outlay_rs=price * quantity, net_cash_flow_rs=-debit,
        quote_spread_pct=quote["spread_pct"], quote_volume=quote["volume"],
        available_cash_before_entry_rs=free_cash, updated_at_ist=now.isoformat(timespec="seconds"),
    )


def _close_state(state: dict[str, Any], raw: float, reason: str, now: datetime) -> None:
    price = _tick(raw * (1.0 - SLIPPAGE_BPS / 10_000.0), float(state["tick_size"]), up=False)
    quantity = int(state["quantity"])
    entry = float(state["entry_price"])
    exit_fees = option_order_costs(price, quantity, "SELL")
    gross = (price - entry) * quantity
    total_cost = float(state["entry_costs_rs"]) + exit_fees["total"]
    state.update(
        status="CLOSED", status_reason=reason, exit_reason=reason,
        exit_raw_price=raw, exit_price=price, exit_at_ist=now.isoformat(timespec="seconds"),
        last_price=raw, exit_costs_rs=exit_fees["total"], estimated_cost_rs=total_cost,
        gross_pnl_rs=gross, net_pnl_rs=gross - total_cost,
        net_cash_flow_rs=(price - entry) * quantity - total_cost,
        updated_at_ist=now.isoformat(timespec="seconds"),
    )


def advance_quote_state(state: dict[str, Any], quote: dict[str, Any], now: datetime, args: argparse.Namespace) -> None:
    session_date = date.fromisoformat(str(state["session_date"]))
    _validate_state(state, session_date)
    if state["status"] in TERMINAL_STATES:
        return
    checked = validate_quote(quote, state, now, args.max_quote_age_sec, args.max_spread_pct)
    if state["status"] == "WAITING_ENTRY_QUOTE":
        equity_entry = _ist(state["equity_entry_at_ist"]).to_pydatetime()
        lag = (now - equity_entry).total_seconds()
        if lag < -5:
            state.update(status_reason="WAITING_FOR_EQUITY_ENTRY_TIME", updated_at_ist=now.isoformat(timespec="seconds"))
        elif lag > args.max_entry_lag_sec or now.time() >= SQUARE_OFF:
            state.update(status="SKIPPED", status_reason="OPTION_ENTRY_WINDOW_EXPIRED_NO_RETROACTIVE_FILL",
                         entry_lag_sec=lag, updated_at_ist=now.isoformat(timespec="seconds"))
        else:
            _enter_state(state, checked, now, session_date)
        return
    if state["status"] != "OPEN":
        raise ValueError(f"Unknown active option state {state['status']}")
    state.update(last_price=checked["last"], quote_spread_pct=checked["spread_pct"], quote_volume=checked["volume"])
    reason = ""
    if checked["last"] <= float(state["stop_price"]):
        reason = "STOP"
    elif checked["bid"] >= float(state["target_price"]):
        reason = "TARGET"
    elif now.time() >= SQUARE_OFF:
        reason = "SQUARE_OFF_1515"
    if reason:
        _close_state(state, checked["bid"], reason, now)
    else:
        quantity = int(state["quantity"])
        mark_fees = option_order_costs(checked["bid"], quantity, "SELL")["total"]
        gross = (checked["bid"] - float(state["entry_price"])) * quantity
        state.update(gross_pnl_rs=gross, estimated_cost_rs=float(state["entry_costs_rs"]) + mark_fees,
                     net_pnl_rs=gross - float(state["entry_costs_rs"]) - mark_fees,
                     updated_at_ist=now.isoformat(timespec="seconds"))


def expire_without_quote(state: dict[str, Any], now: datetime, args: argparse.Namespace) -> bool:
    """Advance time-only terminal guards even when every quote lane is down."""
    session_date = date.fromisoformat(str(state["session_date"]))
    _validate_state(state, session_date)
    if state["status"] == "WAITING_ENTRY_QUOTE":
        lag = (now - _ist(state["equity_entry_at_ist"]).to_pydatetime()).total_seconds()
        if lag > args.max_entry_lag_sec or now.date() != session_date or now.time() >= SQUARE_OFF:
            state.update(
                status="SKIPPED", status_reason="OPTION_ENTRY_WINDOW_EXPIRED_NO_RETROACTIVE_FILL",
                entry_lag_sec=lag, updated_at_ist=now.isoformat(timespec="seconds"),
            )
            return True
    elif state["status"] == "OPEN" and (now.date() != session_date or now.time() >= SESSION_END):
        state.update(
            status="UNRESOLVED", status_reason="SQUARE_OFF_QUOTE_UNAVAILABLE_BY_SESSION_END",
            exit_reason="UNRESOLVED", updated_at_ist=now.isoformat(timespec="seconds"),
        )
        return True
    return False


def replay_state(equity: dict[str, Any], master: pd.DataFrame, master_path: Path, master_sha: str, data_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    mapping = map_equity_entry(equity, master, master_path, master_sha)
    now = common.now_ist()
    state = create_state(equity, mapping, now, "HISTORICAL_EXACT_5M_REPLAY")
    entry_ts = _ist(equity["_equity_entry_ts"]).ceil("5min")
    roots = [(data_root / "raw_options_1m", 1), (data_root / "raw_options_5m", 5)]
    candles, sources = option_data.load_contract_five_minutes(mapping["option_symbol"], roots)
    candles = candles.loc[common._to_ist(candles["timestamp"]).dt.date.eq(date.fromisoformat(state["session_date"]))].copy() if not candles.empty else candles
    row = {
        "trade_id": state["trade_id"], "day": state["session_date"], "entry_ts": entry_ts,
        "lot_size": mapping["lot_size"], "tick_size": mapping["tick_size"],
        "check_previous_volume": True,
    }
    result, audit = simulate_one_lot_trade(
        row, candles, STOP_PCT, TARGET_PCT, slippage_bps=SLIPPAGE_BPS,
        participation=PREVIOUS_BAR_PARTICIPATION,
    )
    state.update(
        status=result["status"], status_reason=result.get("reason", ""),
        entry_raw_price=result.get("entry_raw_price", 0.0), entry_price=result.get("entry_price", 0.0),
        entry_at_ist=_json_value(result.get("entry_ts")) if result.get("entered") else "",
        stop_price=result.get("stop_price", 0.0), target_price=result.get("target_price", 0.0),
        exit_raw_price=result.get("exit_raw_price", 0.0), exit_price=result.get("exit_price", 0.0),
        exit_at_ist=_json_value(result.get("exit_ts")), exit_reason=result.get("exit_reason", ""),
        entry_costs_rs=result.get("entry_costs", 0.0), exit_costs_rs=result.get("exit_costs", 0.0),
        estimated_cost_rs=result.get("total_costs", 0.0), entry_premium_outlay_rs=result.get("entry_premium_outlay", 0.0),
        gross_pnl_rs=result.get("gross_pnl", 0.0), net_pnl_rs=result.get("net_pnl", 0.0),
        net_cash_flow_rs=result.get("net_cash_flow", 0.0), replay_entry_candle_start=entry_ts.isoformat(),
        candle_sources=sources, updated_at_ist=now.isoformat(timespec="seconds"),
    )
    return _json_value(state), [_json_value(item) for item in audit]


def _table(states: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Equity | Side | Option | Qty | Equity fill time | Option fill time | Entry | Stop | Target | Exit | Status | Net P&L |",
        "|---|---|---|---:|---|---|---:|---:|---:|---:|---|---:|",
    ]
    for row in states:
        lines.append(
            f"| {row.get('equity_symbol','')} | {row.get('side','')} | {row.get('option_symbol','')} | {row.get('quantity',0)} | "
            f"{row.get('equity_entry_at_ist','')} | {row.get('entry_at_ist','')} | {float(row.get('entry_price',0) or 0):.2f} | "
            f"{float(row.get('stop_price',0) or 0):.2f} | {float(row.get('target_price',0) or 0):.2f} | "
            f"{float(row.get('exit_price',0) or 0):.2f} | {row.get('status','')} | {float(row.get('net_pnl_rs',0) or 0):.2f} |"
        )
    return lines


def render_report(role: str, session_date: date) -> str:
    states = load_states(session_date)
    if role == "long-entry":
        states = [row for row in states if row.get("side") == "LONG"]
    elif role == "short-entry":
        states = [row for row in states if row.get("side") == "SHORT"]
    summary = net_summary(states)
    lines = [
        f"# {ROLE_TITLES[role]}", "",
        f"- Session date (IST): `{session_date}`", "- Mode: **PAPER**",
        f"- Mapping: equity LONG -> ATM CE BUY; equity SHORT -> ATM PE BUY",
        f"- Size: **exactly one exchange lot per filled equity entry**",
        f"- Premium stop / target: `{STOP_PCT:.1%}` / `{TARGET_PCT:.1%}`; square-off `{SQUARE_OFF.strftime('%H:%M')}`",
        f"- Full-premium account: `Rs {PORTFOLIO_CAPITAL_RS:,.2f}`; adverse fill slippage: `{SLIPPAGE_BPS:g} bps`",
        f"- Trades: `{summary['trades']}`; open: `{summary['open']}`; closed: `{summary['closed']}`; net: `Rs {summary['net_pnl_rs']:,.2f}`",
        "", *_table(states), "",
        "Paper safeguards require the exact G strategy fingerprint, an actual filled equity state, a same-day NFO master, deterministic monthly ATM mapping, one-lot quantity, fresh two-sided NFO depth, bounded spread, traded volume, full-premium cash, atomic state writes, and credential failover. No broker order method is called.", "",
    ]
    return "\n".join(lines)


def net_summary(states: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {name.lower(): sum(str(row.get("status", "")) == name for row in states) for name in ("OPEN", "CLOSED", "SKIPPED", "BLOCKED", "UNRESOLVED")}
    realized = sum(float(row.get("net_pnl_rs", 0.0) or 0.0) for row in states if row.get("status") == "CLOSED")
    mark = sum(float(row.get("net_pnl_rs", 0.0) or 0.0) for row in states if row.get("status") == "OPEN")
    cash = PORTFOLIO_CAPITAL_RS + sum(float(row.get("net_cash_flow_rs", 0.0) or 0.0) for row in states)
    return {"trades": len(states), **counts, "realized_net_pnl_rs": realized, "open_mark_net_pnl_rs": mark,
            "net_pnl_rs": realized + mark, "free_cash_rs": cash}


def publish_consolidated(session_date: date) -> Path:
    states = load_states(session_date)
    frame = pd.DataFrame(states)
    common.atomic_write_csv(frame, consolidated_csv_path(session_date))
    return consolidated_csv_path(session_date)


def _publish(role: str, state: str, **extra: Any) -> None:
    common.publish_status(
        ROLE_SESSIONS[role], state, role=role, execution_mode="PAPER",
        strategy_version=equity_config.STRATEGY_VERSION,
        options_strategy_version=OPTIONS_STRATEGY_VERSION,
        stop_pct=round(STOP_PCT * 100, 4),
        target_pct=round(TARGET_PCT * 100, 4),
        worker_pid=os.getpid(), **extra,
    )


def run_replay(args: argparse.Namespace, session_date: date, side: str) -> int:
    master, master_path, master_sha = load_exact_options_master(session_date)
    entries = load_equity_entries(session_date, side, args.equity_source)
    audits: list[dict[str, Any]] = []
    for equity in entries:
        state, audit = replay_state(equity, master, master_path, master_sha, args.data_root)
        common.atomic_write_json(order_path(session_date, str(equity["signal_id"])), state)
        audits.extend(audit)
    if audits:
        common.atomic_write_parquet(pd.DataFrame(audits), AUDIT_ROOT / f"options_v13_v10_g_bars_{session_date.isoformat()}.parquet")
    publish_consolidated(session_date)
    role = "long-entry" if side == "LONG" else "short-entry"
    common.atomic_write_text(report_path(role), render_report(role, session_date))
    summary = net_summary(load_states(session_date, side))
    _publish(role, "DONE", phase="HISTORICAL_REPLAY", session_date_ist=session_date, replay=True, **summary)
    return 0


def run_entry(args: argparse.Namespace, session_date: date, side: str) -> int:
    role = "long-entry" if side == "LONG" else "short-entry"
    if args.replay:
        return run_replay(args, session_date, side)
    quote_pool: KiteQuotePool | None = None
    while True:
        now = common.now_ist()
        entries = load_equity_entries(session_date, side, args.equity_source)
        master_info: tuple[pd.DataFrame, Path, str] | None = None
        master_error = ""
        if entries:
            try:
                master_info = load_exact_options_master(session_date)
            except Exception as exc:
                master_error = f"{type(exc).__name__}: {exc}"
        for equity in entries:
            path = order_path(session_date, str(equity["signal_id"]))
            with paper_portfolio_lock(order_day_dir(session_date)):
                if path.exists():
                    continue
                if master_info is None:
                    continue
                try:
                    mapping = map_equity_entry(equity, *master_info)
                    state = create_state(equity, mapping, now, "LIVE_GUARDED_NFO_QUOTE")
                    common.atomic_write_json(path, state)
                except Exception as exc:
                    _publish(role, "DEGRADED", phase="MAP_FAILED", signal_id=equity["signal_id"], reason=f"{type(exc).__name__}: {exc}")
        active = [row for row in load_states(session_date, side) if row.get("status") not in TERMINAL_STATES]
        for row in active:
            path = order_path(session_date, str(row["signal_id"]))
            with paper_portfolio_lock(order_day_dir(session_date)):
                state = _read_json(path)
                if expire_without_quote(state, now, args):
                    common.atomic_write_json(path, _json_value(state))
        active = [row for row in load_states(session_date, side) if row.get("status") not in TERMINAL_STATES]
        apps_available = 0
        apps_used = 0
        if active:
            try:
                if quote_pool is None:
                    quote_pool = KiteQuotePool(args.max_apps, args.timeout_sec, args.request_interval_sec)
                apps_available = quote_pool.app_count
                quotes = quote_pool.quotes(row["option_symbol"] for row in active)
                apps_used = len(quote_pool.apps_used)
                for row in active:
                    path = order_path(session_date, str(row["signal_id"]))
                    with paper_portfolio_lock(order_day_dir(session_date)):
                        state = _read_json(path)
                        quote = quotes.get(str(state.get("option_symbol", "")))
                        if not quote:
                            state.update(status_reason="WAITING_FOR_OPTION_QUOTE", updated_at_ist=now.isoformat(timespec="seconds"))
                        else:
                            try:
                                advance_quote_state(state, quote, now, args)
                            except Exception as exc:
                                state.update(status_reason=f"QUOTE_GUARD:{type(exc).__name__}:{exc}", updated_at_ist=now.isoformat(timespec="seconds"))
                        common.atomic_write_json(path, _json_value(state))
            except Exception as exc:
                master_error = f"{type(exc).__name__}: {exc}"
        common.atomic_write_text(report_path(role), render_report(role, session_date))
        counts = net_summary(load_states(session_date, side))
        state_name = "DONE" if args.once or now.date() != session_date or now.time() >= SESSION_END else "RUNNING"
        _publish(role, state_name, session_date_ist=session_date, equity_entries=len(entries), apps_configured=args.max_apps,
                 apps_available=apps_available, apps_used=apps_used, reason=master_error, **counts)
        if state_name == "DONE":
            return 0
        time.sleep(args.poll_sec)


def run_reporting(args: argparse.Namespace, session_date: date, role: str) -> int:
    while True:
        now = common.now_ist()
        states = load_states(session_date)
        output = publish_consolidated(session_date)
        common.atomic_write_text(report_path(role), render_report(role, session_date))
        summary = net_summary(states)
        state_name = "DONE" if args.once or now.date() != session_date or now.time() >= SESSION_END else "RUNNING"
        _publish(role, state_name, session_date_ist=session_date, output=output, **summary)
        if state_name == "DONE":
            return 0
        time.sleep(args.reporting_poll_sec)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=tuple(ROLE_SESSIONS))
    parser.add_argument("--session-date", default="")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--equity-source", choices=("AUTO", "PAPER", "LIVE"), default="AUTO")
    parser.add_argument("--data-root", type=Path, default=common.FNO_ROOT)
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--reporting-poll-sec", type=float, default=5.0)
    parser.add_argument("--max-entry-lag-sec", type=float, default=DEFAULT_MAX_ENTRY_LAG_SEC)
    parser.add_argument("--max-quote-age-sec", type=float, default=DEFAULT_MAX_QUOTE_AGE_SEC)
    parser.add_argument("--max-spread-pct", type=float, default=DEFAULT_MAX_SPREAD_PCT)
    return parser


def run(args: argparse.Namespace) -> int:
    if args.max_apps != 8:
        raise ValueError("Options V13-V10-G is pinned to all eight configured Kite app lanes")
    if args.max_entry_lag_sec <= 0 or args.max_quote_age_sec <= 0 or not 0 < args.max_spread_pct <= 100:
        raise ValueError("Quote and entry guards must be positive")
    session_date = date.fromisoformat(args.session_date) if args.session_date else common.now_ist().date()
    if args.replay and not args.once:
        raise ValueError("Historical replay requires --once")
    if args.role == "long-entry":
        return run_entry(args, session_date, "LONG")
    if args.role == "short-entry":
        return run_entry(args, session_date, "SHORT")
    if args.replay:
        raise ValueError("--replay is valid only for long-entry or short-entry")
    return run_reporting(args, session_date, args.role)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        _publish(args.role, "STOPPED", phase="INTERRUPTED")
        return 130
    except Exception as exc:
        _publish(args.role, "FAILED", phase="EXCEPTION", reason=f"{type(exc).__name__}: {exc}")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
