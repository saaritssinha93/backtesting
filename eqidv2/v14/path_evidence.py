"""Prove a selected order's outcome from a real uninterrupted minute prefix.

The unchanged frozen entry and staged-exit functions determine the outcome.
This module changes only the amount of observed data needed to *prove* it:
bars after an observed terminal event or complete unfilled entry window cannot
alter that order.  Missing bars before resolution remain unknown.  No candles
are generated, filled forward, reordered, or inferred from later prices.

Selection must happen before this assessment.  An unresolved order must never
be replaced with a lower-ranked candidate.  The caller remains responsible
for the original portfolio constraints across every resolved order.
"""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd

from . import baseline
from ._baseline import execution

EVIDENCE_VERSION = "V14_OBSERVED_TERMINAL_PREFIX_V1"
FIELDS = ("timestamp_ns", "open", "high", "low", "close")
TERMINAL_REASONS = {"STOP", "TIGHTENED_STOP", "TARGET"}


def _value(row: Any, name: str, default=None):
    return row.get(name, default) if isinstance(row, (Mapping, pd.Series)) else getattr(row, name, default)


def _stamp(value: int) -> str:
    return str(pd.Timestamp(int(value), tz="UTC").tz_convert(baseline.IST))


def _hash_prefix(path: dict[str, np.ndarray]) -> str:
    digest = hashlib.sha256()
    for name in FIELDS:
        digest.update(name.encode("ascii"))
        digest.update(np.asarray(path[name], dtype="<i8" if name == "timestamp_ns" else "<f8").tobytes())
    return digest.hexdigest()


def assess(row: Any, path: dict[str, np.ndarray]) -> dict:
    """Return ``resolved``, ``reason``, real ``path`` prefix and JSON evidence.

    On success the returned prefix is sufficient for the unchanged
    ``execution.simulate_staged`` function.  The original full-session
    ``baseline.simulate`` intentionally still rejects short paths; its caller
    needs an explicit evidence-aware validation boundary before using this
    proof.  Unknown outcomes are never represented as unfilled trades.
    """
    empty = {name: np.array([], dtype=np.int64 if name == "timestamp_ns" else float) for name in FIELDS}
    evidence = dict(version=EVIDENCE_VERSION, selected_before_assessment=True,
                    synthetic_bars=False, input_minutes=0, continuous_valid_minutes=0,
                    required_minutes=0, full_session_observed=False,
                    first_interruption=None, outcome=None,
                    entry_helper="v14._baseline.execution._entry",
                    exit_helper="v14._baseline.execution.staged_exit")

    def result(resolved: bool, reason: str, prefix=empty):
        # Independent copies prevent a consumer from accidentally altering the
        # observed source arrays while retaining the evidence hash.
        copied = {name: np.array(prefix[name], copy=True) for name in FIELDS}
        details = {**evidence, "required_minutes": len(copied["timestamp_ns"]),
                   "prefix_sha256": _hash_prefix(copied)}
        if len(copied["timestamp_ns"]):
            details.update(prefix_start=_stamp(copied["timestamp_ns"][0]),
                           prefix_end=_stamp(copied["timestamp_ns"][-1]))
            if resolved:
                details["proof_available_ts"] = _stamp(copied["timestamp_ns"][-1])
        return dict(resolved=bool(resolved), reason=reason, path=copied, evidence=details)

    try:
        confirmation = execution._to_ist_timestamp(_value(row, "confirmation_ts"))
        trigger = float(_value(row, "trigger"))
    except (TypeError, ValueError, OverflowError):
        return result(False, "INVALID_SELECTED_ORDER")
    side, setup_id = _value(row, "side"), _value(row, "setup_id")
    if (pd.isna(confirmation) or side not in {"LONG", "SHORT"}
            or not isinstance(setup_id, str) or not np.isfinite(trigger) or trigger <= 0
            or confirmation.value % execution.MINUTE_NS):
        return result(False, "INVALID_SELECTED_ORDER")
    cutoff = confirmation.normalize() + pd.Timedelta(hours=15, minutes=15)
    expected = pd.date_range(confirmation + pd.Timedelta(minutes=1), cutoff, freq="min").as_unit("ns").asi8
    if not len(expected):
        return result(False, "NO_EXECUTION_WINDOW")
    if not isinstance(path, Mapping) or set(FIELDS).difference(path):
        return result(False, "MISSING_PATH_FIELDS")
    try:
        arrays = {name: np.asarray(path[name]) for name in FIELDS}
        if any(value.ndim != 1 for value in arrays.values()):
            return result(False, "PATH_ARRAYS_NOT_ONE_DIMENSIONAL")
        stamps = arrays["timestamp_ns"]
        if not np.issubdtype(stamps.dtype, np.signedinteger):
            return result(False, "TIMESTAMPS_MUST_BE_INTEGER_NANOSECONDS")
        count = len(stamps)
        evidence["input_minutes"] = count
        if any(len(value) != count for value in arrays.values()):
            return result(False, "MISMATCHED_PATH_ARRAY_LENGTHS")
        arrays = {name: value.astype(np.int64 if name == "timestamp_ns" else float, copy=False)
                  for name, value in arrays.items()}
    except (TypeError, ValueError, OverflowError):
        return result(False, "INVALID_PATH_ARRAYS")
    if not count:
        evidence["first_interruption"] = dict(reason="MISSING_FIRST_MINUTE", expected_ts=_stamp(expected[0]))
        return result(False, "EMPTY_EXECUTION_PATH")

    # A matching timestamp alone is insufficient: all consumed OHLC bars must
    # be finite, positive and internally consistent.  Stop at the first defect.
    prices = np.column_stack([arrays[name] for name in FIELDS[1:]])
    valid = np.isfinite(prices).all(axis=1) & (prices > 0).all(axis=1)
    opening, high, low, close = prices.T
    valid &= (high >= np.maximum(opening, close)) & (low <= np.minimum(opening, close)) & (high >= low)
    length = 0
    for i in range(min(count, len(expected))):
        if arrays["timestamp_ns"][i] != expected[i]:
            evidence["first_interruption"] = dict(reason="MISSING_DUPLICATE_OR_MISTIMED_MINUTE", expected_ts=_stamp(expected[i]), observed_ts=_stamp(arrays["timestamp_ns"][i]))
            break
        if not valid[i]:
            evidence["first_interruption"] = dict(reason="INVALID_OHLC", expected_ts=_stamp(expected[i]))
            break
        length += 1
    if evidence["first_interruption"] is None and length < len(expected):
        evidence["first_interruption"] = dict(reason="MISSING_TAIL", expected_ts=_stamp(expected[length]))
    evidence.update(continuous_valid_minutes=length, full_session_observed=length == len(expected),
                    confirmation_ts=str(confirmation), square_off_ts=str(cutoff))
    prefix = {name: value[:length] for name, value in arrays.items()}
    if not length:
        return result(False, "NO_UNINTERRUPTED_EXECUTION_PREFIX", prefix)

    config = baseline.frozen_config()
    expiry = int(config["entry_expiry_minutes"])
    target_pct = float(config["exit"]["setups"].get(setup_id, config["exit"]["default"])["target_pct"])
    evidence.update(baseline_version=config["version"], entry_expiry_minutes=expiry,
                    target_pct=target_pct, target_authority="FROZEN_SETUP_CONFIGURATION")
    entry = execution._entry(SimpleNamespace(side=side, trigger=trigger), prefix,
                             delay_bars=0, trigger_buffer_pct=0., worse_fill_bps=0.,
                             max_entry_delay_minutes=expiry)
    if entry is None:
        needed = min(expiry, len(expected))
        if length < needed:
            return result(False, "ENTRY_WINDOW_INCOMPLETE", prefix)
        proof_path = {name: value[:needed] for name, value in prefix.items()}
        outcome = dict(filled=False, exit_reason="UNFILLED", resolution_ts=_stamp(expected[needed - 1]))
        reason = "RESOLVED_UNFILLED_ENTRY_WINDOW"
    else:
        entry_index, entry_price, effective_trigger, gap, overshoot = entry
        exit_index, price, exit_reason, active_stop, event = execution.staged_exit(
            prefix, entry_index, entry_price, side == "LONG", target_pct)
        evidence["entry"] = dict(index=int(entry_index), price=float(entry_price),
                                  trigger=float(effective_trigger), gap_through=bool(gap),
                                  overshoot_bps=float(overshoot), ts=_stamp(prefix["timestamp_ns"][entry_index]))
        if exit_reason not in TERMINAL_REASONS and not evidence["full_session_observed"]:
            # staged_exit's fallback closes at its final array element.  It is
            # a real TIME_EXIT_1515 only when exact square-off is observed.
            return result(False, "POSITION_OPEN_AT_FIRST_INTERRUPTION", prefix)
        if exit_reason == "TIME_EXIT_1515" and prefix["timestamp_ns"][exit_index] != expected[-1]:
            return result(False, "SQUARE_OFF_NOT_OBSERVED", prefix)
        needed = int(exit_index) + 1
        proof_path = {name: value[:needed] for name, value in prefix.items()}
        end = int(prefix["timestamp_ns"][exit_index])
        execution_time = end - execution.MINUTE_NS if event == "OPEN" else end
        outcome = dict(filled=True, exit_reason=exit_reason, entry_index=int(entry_index),
                       entry_price=float(entry_price), exit_index=int(exit_index), exit_price=float(price),
                       active_stop_pct=float(active_stop), event=event,
                       exit_bar_end_ts=_stamp(end), resolution_ts=_stamp(execution_time))
        reason = f"RESOLVED_{exit_reason}"

    # A duplicate observed later in an unsorted array may conflict with an
    # already consumed bar.  Reject even a nominally terminal first version.
    required_end = proof_path["timestamp_ns"][-1]
    relevant = arrays["timestamp_ns"][(arrays["timestamp_ns"] >= expected[0]) & (arrays["timestamp_ns"] <= required_end)]
    if len(relevant) != needed or len(np.unique(relevant)) != needed:
        return result(False, "AMBIGUOUS_REQUIRED_TIMESTAMP", prefix)
    evidence["outcome"] = outcome
    return result(True, reason, proof_path)
