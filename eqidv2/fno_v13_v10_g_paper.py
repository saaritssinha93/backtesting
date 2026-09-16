"""Shared G PAPER capital admission and quote-independent pending expiry.

The caller locks the PAPER day directory, re-reads state, advances a copy,
checks new fills, and persists before releasing the lock. LONG and SHORT use
the same day directory and lock. No broker API is imported or called here.
"""
from __future__ import annotations

import copy
import errno
import json
import math
import os
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

import fno_v13_v10_g_live_config as config


TERMINAL_STATES = frozenset({"CLOSED", "NO_FILL", "ENTRY_REJECTED", "BLOCKED_SIZING",
                             "CANCELLED", "BLOCKED_PORTFOLIO"})


class PortfolioStateError(RuntimeError):
    """The paper book cannot safely be valued for a new fill."""


@contextmanager
def paper_portfolio_lock(day_root: Path | str, *, timeout_sec: float = 5.) -> Iterator[None]:
    """Cross-process, kernel-released lock; keep read/advance/write inside it."""
    if not math.isfinite(timeout_sec) or timeout_sec < 0:
        raise ValueError("Lock timeout must be finite and nonnegative")
    root = Path(day_root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".paper_portfolio.lock").open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        deadline = time.monotonic() + timeout_sec
        acquired = False
        while not acquired:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("G PAPER portfolio lock timed out") from exc
                time.sleep(min(.025, remaining))
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _positive_number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioStateError(f"Invalid active paper {field}") from exc
    if not math.isfinite(number) or number <= 0:
        raise PortfolioStateError(f"Invalid active paper {field}")
    return number


def capacity_from_states(states: Iterable[dict[str, Any]], current: dict[str, Any]) -> dict[str, Any]:
    """Pure capital admission, counting only this day's filled OPEN positions.

    Pending orders do not consume capital. Other strategy OPEN states inside
    this isolated paper book block admission; they cannot be silently ignored.
    """
    if current.get("mode") != "PAPER" or current.get("strategy_version") != config.STRATEGY_VERSION:
        raise PortfolioStateError("G capacity admission requires a current G PAPER order")
    fingerprint = config.strategy_fingerprint()
    if current.get("strategy_fingerprint") != fingerprint:
        raise PortfolioStateError("Current G paper fingerprint mismatch")
    signal_id = str(current.get("signal_id", ""))
    session = str(current.get("session_date", ""))
    try:
        date.fromisoformat(session)
    except ValueError as exc:
        raise PortfolioStateError("Invalid G paper session date") from exc
    if not signal_id:
        raise PortfolioStateError("Missing G paper signal ID")
    required = _positive_number(current.get("capital_rs"), "capital")
    if abs(required - config.CAPITAL_PER_ENTRY_RS) > 1e-9:
        raise PortfolioStateError("G paper allocation must be one lakh per filled entry")
    reserved = 0.
    seen = set()
    for state in states:
        if not isinstance(state, dict):
            raise PortfolioStateError("Invalid paper order object")
        mode = state.get("mode")
        if mode == "LIVE":
            continue
        if mode != "PAPER":
            raise PortfolioStateError("Unknown order mode in G PAPER book")
        other_session = str(state.get("session_date", ""))
        try:
            date.fromisoformat(other_session)
        except ValueError as exc:
            raise PortfolioStateError("Paper order has an invalid session date") from exc
        if other_session != session:
            # day_root limits the normal inventory; explicit other-day files
            # never reserve today's budget.
            continue
        identity = str(state.get("signal_id", ""))
        if not identity:
            raise PortfolioStateError("Paper order has no signal ID")
        if identity == signal_id:
            continue
        status = state.get("status")
        if status in TERMINAL_STATES or status == "PENDING_ENTRY":
            continue
        if status != "OPEN":
            raise PortfolioStateError(f"Unknown active paper status: {status}")
        if identity in seen:
            raise PortfolioStateError("Duplicate OPEN paper signal ID")
        seen.add(identity)
        if state.get("strategy_version") != config.STRATEGY_VERSION or state.get("strategy_fingerprint") != fingerprint:
            raise PortfolioStateError("Foreign or stale strategy OPEN in G PAPER book")
        if state.get("side") not in {"LONG", "SHORT"}:
            raise PortfolioStateError("Unknown side in OPEN paper order")
        capital = _positive_number(state.get("capital_rs"), "capital")
        if abs(capital - config.CAPITAL_PER_ENTRY_RS) > 1e-9:
            raise PortfolioStateError("OPEN paper capital differs from frozen allocation")
        quantity = _positive_number(state.get("quantity"), "quantity")
        if not quantity.is_integer():
            raise PortfolioStateError("OPEN paper quantity is not whole equity shares")
        _positive_number(state.get("entry_price"), "entry price")
        reserved += capital
    available = max(0., config.PORTFOLIO_CAPITAL_RS - reserved)
    allowed = reserved + required <= config.PORTFOLIO_CAPITAL_RS + 1e-9
    return dict(allowed=allowed, reason="PAPER_CAPITAL_AVAILABLE" if allowed else "PORTFOLIO_CAPITAL_LIMIT",
                portfolio_capital_rs=config.PORTFOLIO_CAPITAL_RS, reserved_capital_rs=reserved,
                available_capital_rs=available, required_capital_rs=required, open_positions=len(seen))


def capacity_for_fill(current: dict[str, Any], day_root: Path | str) -> dict[str, Any]:
    """Read the locked day book strictly; malformed JSON is never an empty book."""
    states = []
    try:
        for path in sorted(Path(day_root).rglob("*.json")):
            states.append(json.loads(path.read_text(encoding="utf-8-sig")))
    except (OSError, ValueError, TypeError) as exc:
        raise PortfolioStateError("Unreadable order state in G PAPER book") from exc
    return capacity_from_states(states, current)


def enforce_fill_capacity(previous: dict[str, Any], proposed: dict[str, Any],
                          day_root: Path | str) -> dict[str, Any]:
    """Admit a new simulated fill or return its unfilled terminal rejection.

    This catches accounting/read failures before any proposed OPEN state can
    escape to disk. Existing OPEN positions may always progress toward exits.
    """
    if previous.get("status") != "PENDING_ENTRY" or proposed.get("status") != "OPEN":
        return proposed
    try:
        admission = capacity_for_fill(proposed, day_root)
    except PortfolioStateError as exc:
        admission = dict(allowed=False, reason="PAPER_PORTFOLIO_STATE_INVALID", error=str(exc))
    if admission["allowed"]:
        proposed["paper_portfolio_admission"] = admission
        return proposed
    rejected = copy.deepcopy(previous)
    rejected.update(status="BLOCKED_PORTFOLIO", status_reason=admission["reason"],
                    updated_at_ist=proposed.get("updated_at_ist", previous.get("updated_at_ist", "")),
                    last_price=proposed.get("last_price", previous.get("last_price", 0.)),
                    paper_portfolio_admission=admission)
    return rejected


def expire_pending(state: dict[str, Any], now: datetime) -> bool:
    """Expire untouched entries even if every quote request has failed."""
    if state.get("status") != "PENDING_ENTRY":
        return False
    if state.get("mode") != "PAPER" or state.get("strategy_version") != config.STRATEGY_VERSION:
        raise PortfolioStateError("G pending expiry requires a current G PAPER order")
    try:
        session = date.fromisoformat(str(state["session_date"]))
        expected = config.entry_expiry(session, str(state["confirmation_end"]))
        recorded = datetime.fromisoformat(str(state["entry_activation_deadline_ist"]))
        if recorded != expected or now.tzinfo is None:
            raise ValueError("Expiry or current clock invalid")
    except (KeyError, TypeError, ValueError) as exc:
        raise PortfolioStateError("Invalid G paper entry expiry") from exc
    if now > expected:
        state.update(status="CANCELLED", status_reason="ENTRY_TRIGGER_WINDOW_EXPIRED",
                     updated_at_ist=now.isoformat(timespec="seconds"))
        return True
    return False
