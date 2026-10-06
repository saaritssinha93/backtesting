"""Dated production promotion of the G-2 rules into V13-v10-G.

Session dates, never import time, determine activation. Historical frozen G
artifacts remain the baseline and are not rewritten by this policy.
"""
from __future__ import annotations

from datetime import date, datetime

EFFECTIVE_DATE = date(2026, 10, 6)
REVISION = "V13_V10_G_20261006_RELAXED0925_STAGED125_100_120M"
INITIAL_STOP_PCT = 1.25
TIGHTENED_STOP_PCT = 1.0
TIGHTEN_AFTER_MINUTES = 120
RELAXED_0925_LONG = dict(
    oi_max_pct=1.20,
    minimum_volume_ratio=1.75,
    minimum_body_ratio=.54,
    ignore_ema_alignment=True,
)


def enabled_for_session(day: date | str | None) -> bool:
    if day is None:
        return False
    if isinstance(day, datetime):
        day = day.date()
    if isinstance(day, str):
        day = date.fromisoformat(day)
    if not isinstance(day, date):
        raise TypeError("An explicit session date is required")
    return day >= EFFECTIVE_DATE


def policy_for_day(day: date | str | None) -> dict:
    enabled = enabled_for_session(day)
    return dict(
        revision=REVISION if enabled else "V13_V10_G_RETAINED_20260914",
        effective_from=EFFECTIVE_DATE.isoformat(),
        relaxed_0925_long=enabled,
        staged_stop=enabled,
        initial_stop_pct=INITIAL_STOP_PCT if enabled else None,
        tightened_stop_pct=TIGHTENED_STOP_PCT if enabled else None,
        tighten_after_minutes=TIGHTEN_AFTER_MINUTES if enabled else None,
    )
