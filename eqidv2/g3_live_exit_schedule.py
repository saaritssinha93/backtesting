"""Protective exit timing for G3 LIVE on verified regular NSE sessions.

The operator's existing calendar/authorization gate establishes the regular
session. This pure module neither verifies nor authorizes an exchange session
and must not be used to infer special-session hours. Muhurat PAPER/replay has
its own date-bound schedule. Decision/PAPER exits and sizing fingerprints are
unchanged by this separate execution safeguard.
"""
from __future__ import annotations

from datetime import date, datetime, time
import re
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
SCHEMA_VERSION = "g3_live_exit_schedule_v1"
FLATTEN_START = "15:05"
VERIFY_FLAT = "15:10"
BROKER_MIS_CUTOFF = "15:12"


def _session_date(value: date) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError("Exit schedule requires a session date")
    return value


def _at(session_date: date, clock: str) -> datetime:
    if not isinstance(clock, str) or not re.fullmatch(r"\d{2}:\d{2}", clock):
        raise ValueError("Exit schedule clock must use HH:MM")
    try:
        parsed = time.fromisoformat(clock)
    except ValueError as exc:
        raise ValueError("Exit schedule clock must use a valid HH:MM") from exc
    return datetime.combine(_session_date(session_date), parsed, IST)


def _now(now: datetime, session_date: date | None) -> tuple[datetime, date]:
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Exit schedule requires a timezone-aware clock")
    current = now.astimezone(IST)
    day = current.date() if session_date is None else _session_date(session_date)
    if current.date() != day:
        raise ValueError("Exit schedule clock does not match the session date in IST")
    return current, day


def build_schedule(session_date: date, strategy_square_off: str) -> dict[str, str]:
    """Describe the LIVE safeguard after the caller verifies a regular session.

    An earlier strategy exit always wins. No date or holiday transformation is
    performed here and this metadata does not grant execution authority.
    """
    day = _session_date(session_date)
    flatten = min(_at(day, strategy_square_off), _at(day, FLATTEN_START))
    return dict(
        schema_version=SCHEMA_VERSION,
        scope="G3_LIVE_REGULAR_NSE",
        session_date=day.isoformat(),
        strategy_square_off=strategy_square_off,
        flatten_at_ist=flatten.isoformat(),
        verify_flat_at_ist=_at(day, VERIFY_FLAT).isoformat(),
        broker_mis_cutoff_at_ist=_at(day, BROKER_MIS_CUTOFF).isoformat(),
    )


def effective_square_off(
    now: datetime, strategy_square_off: str, *, session_date: date | None = None
) -> datetime:
    """Return the earlier strategy/protective exit for the same IST session."""
    _, day = _now(now, session_date)
    return datetime.fromisoformat(build_schedule(day, strategy_square_off)["flatten_at_ist"])


def is_verification_due(
    now: datetime, strategy_square_off: str, *, session_date: date | None = None
) -> bool:
    """Whether this regular session has reached the broker-flat check deadline."""
    current, day = _now(now, session_date)
    schedule = build_schedule(day, strategy_square_off)
    return current >= datetime.fromisoformat(schedule["verify_flat_at_ist"])
