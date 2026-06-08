"""
Signal timing contract for the v7 live pipeline.

Centralises timestamp parsing, timing validation, and signal ID generation
so every pipeline stage uses identical semantics.

Key rule: a signal is valid if and only if
  - all timestamps are parseable and non-null
  - detection_lag_sec >= 0 and <= MAX_LAG_SEC (30 s)
  - signal_bar_time_ist and intended_entry_ist are on the same trading date
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

IST_TZ = "Asia/Kolkata"
MAX_LAG_SEC: float = 30.0
# T+1 is the executable wall-clock instant one minute after the completed
# signal candle. The executor uses live LTP rather than a historical bar open.
ENTRY_LAG_MIN: int = 1


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def parse_ist_timestamp(value: object) -> Optional[pd.Timestamp]:
    """Return an IST-aware Timestamp or None if unparseable / null."""
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize(IST_TZ)
    return ts.tz_convert(IST_TZ)


# ---------------------------------------------------------------------------
# Contract dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SignalTiming:
    signal_bar_time_ist: pd.Timestamp          # candle close = slot_ts
    intended_entry_ist: pd.Timestamp           # signal_bar_time_ist + ENTRY_LAG_MIN
    detected_time_ist: pd.Timestamp            # wall-clock when written to CSV
    deadline_ist: pd.Timestamp                 # intended_entry_ist + MAX_LAG_SEC
    detection_lag_sec: float                   # (detected - intended_entry).total_seconds()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_signal_timing(
    signal_bar_time: object,
    *,
    detected_time: object,
    intended_entry_time: object = None,
    entry_lag_min: int = ENTRY_LAG_MIN,
    max_lag_sec: float = MAX_LAG_SEC,
) -> Optional[SignalTiming]:
    """
    Build a SignalTiming from raw values.

    Returns None if any timestamp is unparseable — callers treat None as
    malformed and should reject the signal.
    """
    bar_ts = parse_ist_timestamp(signal_bar_time)
    det_ts = parse_ist_timestamp(detected_time)
    if bar_ts is None or det_ts is None:
        return None

    intended = parse_ist_timestamp(intended_entry_time)
    if intended is None:
        intended = bar_ts + pd.Timedelta(minutes=entry_lag_min)
    deadline = intended + pd.Timedelta(seconds=max_lag_sec)
    lag = (det_ts - intended).total_seconds()

    return SignalTiming(
        signal_bar_time_ist=bar_ts,
        intended_entry_ist=intended,
        detected_time_ist=det_ts,
        deadline_ist=deadline,
        detection_lag_sec=float(lag),
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_signal_timing(
    timing: Optional[SignalTiming],
    max_lag_sec: float = MAX_LAG_SEC,
) -> Tuple[bool, str]:
    """
    Return (ok, reason).

    ok=True  → signal passes timing contract.
    ok=False → reason explains the exact failure.
    """
    if timing is None:
        return False, "MALFORMED_TIMING: could not build SignalTiming (unparseable timestamp)"

    if pd.isna(timing.signal_bar_time_ist):
        return False, "MALFORMED_TIMING: signal_bar_time_ist is NaT"
    if pd.isna(timing.intended_entry_ist):
        return False, "MALFORMED_TIMING: intended_entry_ist is NaT"
    if pd.isna(timing.detected_time_ist):
        return False, "MALFORMED_TIMING: detected_time_ist is NaT"

    bar_date = timing.signal_bar_time_ist.date()
    entry_date = timing.intended_entry_ist.date()
    det_date = timing.detected_time_ist.date()
    if bar_date != entry_date or bar_date != det_date:
        return False, (
            f"CROSS_DAY: bar={bar_date} entry={entry_date} detected={det_date}"
        )

    if timing.detection_lag_sec < 0:
        return False, (
            f"NEGATIVE_LAG: detected {timing.detection_lag_sec:.1f}s before intended entry "
            f"(signal appears future-dated)"
        )
    if timing.detection_lag_sec > max_lag_sec:
        return False, (
            f"STALE_DETECTION: lag={timing.detection_lag_sec:.0f}s > threshold={max_lag_sec:.0f}s"
        )

    return True, ""


def validate_signal_timing_row(
    row: dict,
    max_lag_sec: float = MAX_LAG_SEC,
    *,
    detected_time: object = None,
) -> Tuple[bool, str]:
    """Convenience wrapper that builds SignalTiming from a CSV row dict."""
    detected = detected_time
    if detected is None:
        detected = row.get("detected_time_ist") or row.get("received_time")
    timing = build_signal_timing(
        row.get("signal_bar_time_ist") or row.get("bar_time_ist") or row.get("signal_datetime"),
        detected_time=detected,
        intended_entry_time=row.get("intended_entry_ist") or row.get("entry_time_ist"),
    )
    return validate_signal_timing(timing, max_lag_sec=max_lag_sec)


# ---------------------------------------------------------------------------
# Signal ID
# ---------------------------------------------------------------------------

def canonical_signal_id(ticker: str, side: str, setup: str, signal_bar_time_ist: object) -> str:
    """
    Deterministic 16-char signal ID.

    Matches the existing _generate_signal_id() logic in base_v15 but uses the
    explicit signal_bar_time_ist rather than an ambiguous signal_datetime.
    """
    ts_str = ""
    parsed = parse_ist_timestamp(signal_bar_time_ist)
    if parsed is not None:
        ts_str = parsed.strftime("%Y-%m-%d %H:%M:%S%z")
    raw = f"EQIDV2|{str(ticker).upper()}|{str(side).upper()}|{ts_str}|{str(setup).upper()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
