"""G3 always uses strict selection and the accepted single stop tightening."""
from __future__ import annotations

from datetime import date, datetime

REVISION = "V13_V10_G_3_STRICT_W1_LONG110_STAGED125_100_120M_V1"
INITIAL_STOP_PCT = 1.25
TIGHTENED_STOP_PCT = 1.0
TIGHTEN_AFTER_MINUTES = 120


def enabled_for_session(day: date | str | None) -> bool:
    if day is not None and not isinstance(day, (date, datetime, str)):
        raise TypeError("Session date must be a date or ISO date")
    if isinstance(day, str):
        date.fromisoformat(day)
    return True


def policy_for_day(day: date | str | None) -> dict:
    enabled_for_session(day)
    return dict(revision=REVISION, effective_from="PROFILE_ACTIVATION",
                relaxed_0925_long=False, staged_stop=True, initial_stop_pct=INITIAL_STOP_PCT,
                tightened_stop_pct=TIGHTENED_STOP_PCT, tighten_after_minutes=TIGHTEN_AFTER_MINUTES)
