"""Frozen, causal OI exit hypotheses for isolated G3 historical research.

OI expansion/contraction describe participation, not buyer/seller identity.
Every OI arm requires adverse cash price AND adverse same-contract futures
price over its OI interval. This module never alters a native stop or target.
"""
from __future__ import annotations

import hashlib
import json
import math

import pandas as pd


VERSION = "G3-TRADE-MONITOR-OI-RULES-V1"
GUARD_CONSTANTS = dict(minimum_bars=6, adverse_closes=3,
                       trend_momentum_5m_pct=-.15, trend_entry_move_pct=-.30)
OI_INTERVAL_MINUTES = 10
OI_MAX_AGE_SECONDS = 360


def candidate_registry():
    """Eight nonbaseline arms fixed before outcome scoring; no refinement."""
    definitions = [("price_guard_only", "price_guard_only", None),
                   ("futures_price_guard_only", "futures_price_guard_only", None)]
    for family in ("oi_expansion", "oi_contraction", "oi_activity"):
        for threshold in (.5, 1.):
            definitions.append((f"{family}_10m_{int(threshold * 100):03d}", family, threshold))
    candidates = []
    for order, (identifier, family, threshold) in enumerate(definitions):
        candidate = dict(candidate_id=identifier, family=family,
            oi_threshold_pct=threshold, guard_constants=dict(GUARD_CONSTANTS),
            grid_order=order, rules_version=VERSION,
            oi_interval_minutes=OI_INTERVAL_MINUTES,
            default_oi_max_age_seconds=OI_MAX_AGE_SECONDS,
            oi_age_policy="MODELED_INGESTION_LAG_PLUS_FIVE_MINUTE_SAMPLING",
            futures_price_confirmation="STRICTLY_ADVERSE_OVER_SAME_OI_INTERVAL",
            execution_authority=False)
        digest = hashlib.sha256(json.dumps(candidate, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        candidate.update(sha256=digest, candidate_sha256=digest)
        candidates.append(candidate)
    return candidates


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _stamp(value):
    try:
        stamp = pd.Timestamp(value)
        return stamp.tz_convert("Asia/Kolkata") if not pd.isna(stamp) and stamp.tzinfo is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def _true(value):
    # CSV bools may be strings or numpy.bool_; missing/unknown is never true.
    return str(value).strip().lower() == "true"


def _base_valid(row):
    numeric = {key: _number(row.get(key)) for key in (
        "sid", "completed_bar_count", "consecutive_adverse_closes",
        "momentum_5m_pct", "signed_move_pct", "execution_path_index", "next_bar_open_price")}
    if any(value is None for value in numeric.values()):
        return None
    for key in ("sid", "completed_bar_count", "consecutive_adverse_closes", "execution_path_index"):
        if numeric[key] < 0 or numeric[key] != int(numeric[key]):
            return None
    if (row.get("side") not in {"LONG", "SHORT"}
            or numeric["next_bar_open_price"] <= 0
            or numeric["completed_bar_count"] < GUARD_CONSTANTS["minimum_bars"]
            or numeric["consecutive_adverse_closes"] < GUARD_CONSTANTS["adverse_closes"]
            or numeric["momentum_5m_pct"] > GUARD_CONSTANTS["trend_momentum_5m_pct"]
            or numeric["signed_move_pct"] > GUARD_CONSTANTS["trend_entry_move_pct"]):
        return None
    times = {key: _stamp(row.get(key)) for key in (
        "decision_at_ist", "next_bar_open_at_ist", "modeled_execution_at_ist", "original_exit_at_ist")}
    if any(value is None for value in times.values()):
        return None
    decision, opened, execution, original = (times[key] for key in (
        "decision_at_ist", "next_bar_open_at_ist", "modeled_execution_at_ist", "original_exit_at_ist"))
    if (str(row.get("day")) != decision.date().isoformat()
            or any(stamp.date() != decision.date() for stamp in (opened, execution, original))
            or opened < decision or execution - opened != pd.Timedelta(minutes=1)
            or original <= opened):
        return None
    return numeric, times


def _oi_valid(row, candidate, decision):
    if not _true(row.get("oi_valid")):
        return False
    change = _number(row.get("oi_change_10m_pct"))
    futures_return = _number(row.get("futures_return_10m_pct"))
    current = _stamp(row.get("oi_current_bar_end"))
    reference = _stamp(row.get("oi_reference_bar_end"))
    available = _stamp(row.get("oi_available_at"))
    contract = row.get("oi_contract")
    max_age_minutes = _number(row.get("oi_max_age_minutes", OI_MAX_AGE_SECONDS / 60))
    if (change is None or futures_return is None
            or any(stamp is None for stamp in (current, reference, available))
            or not isinstance(contract, str) or not contract.strip()
            or max_age_minutes is None or max_age_minutes <= 0):
        return False
    if (current - reference != pd.Timedelta(minutes=OI_INTERVAL_MINUTES)
            or current.date() != reference.date() or current.date() != decision.date()
            or not current <= available <= decision
            or not 0 <= (decision - current).total_seconds() <= max_age_minutes * 60):
        return False
    sign = 1 if row["side"] == "LONG" else -1
    if sign * futures_return >= 0:
        return False
    if candidate["family"] == "futures_price_guard_only":
        return True
    threshold = candidate["oi_threshold_pct"]
    if candidate["family"] == "oi_expansion":
        return change >= threshold
    if candidate["family"] == "oi_contraction":
        return change <= -threshold
    return abs(change) >= threshold


def candidate_events(features, candidate, *, days=None):
    """Return each sid's earliest eligible event, independent of row ordering.

    The upstream immutable decision cache supplies contiguous completed cash
    bars. Its OI adapter must verify same dated contract, real valid candles,
    positive finite OI, and exact observations including the 5-minute midpoint.
    Receipt/availability and chronology are checked again here. A native exit
    at/before the next open takes priority; a later intrabar exit does not.
    """
    registered = {item["candidate_id"]: item for item in candidate_registry()}
    if candidate != registered.get(candidate.get("candidate_id")):
        raise ValueError("OI candidate must match the frozen registry")
    frame = features.copy() if isinstance(features, pd.DataFrame) else pd.DataFrame(features)
    if frame.empty:
        return []
    allowed_days = None if days is None else {str(day) for day in days}
    events = []
    for row in frame.to_dict("records"):
        if allowed_days is not None and str(row.get("day")) not in allowed_days:
            continue
        validated = _base_valid(row)
        if validated is None:
            continue
        numeric, times = validated
        oi_arm = candidate["family"] != "price_guard_only"
        if oi_arm and not _oi_valid(row, candidate, times["decision_at_ist"]):
            continue
        event = dict(
            sid=int(numeric["sid"]), source_signal_id=row["source_signal_id"], day=str(row["day"]),
            setup_id=row["setup_id"], tradingsymbol=row["tradingsymbol"], side=row["side"],
            decision_at_ist=times["decision_at_ist"].isoformat(),
            completed_bar_count=int(numeric["completed_bar_count"]),
            flags=json.dumps(["TREND_FAILURE", candidate["family"].upper()] if oi_arm else ["TREND_FAILURE"]),
            next_bar_open_at_ist=times["next_bar_open_at_ist"].isoformat(),
            modeled_execution_at_ist=times["modeled_execution_at_ist"].isoformat(),
            execution_path_index=int(numeric["execution_path_index"]),
            next_bar_open_price=numeric["next_bar_open_price"],
            original_exit_at_ist=times["original_exit_at_ist"].isoformat(),
            eligible_for_monitor_exit=True, decision="MONITOR_CANDIDATE",
            candidate_id=candidate["candidate_id"], candidate_sha256=candidate["sha256"])
        if oi_arm:
            event.update({key: row[key] for key in (
                "oi_change_10m_pct", "futures_return_10m_pct", "oi_current_bar_end",
                "oi_reference_bar_end", "oi_contract", "oi_available_at")})
        events.append(event)
    events.sort(key=lambda item: (item["sid"], item["decision_at_ist"], item["execution_path_index"]))
    chosen, seen = [], {}
    for event in events:
        key = (event["sid"], event["decision_at_ist"])
        if key in seen and seen[key] != event:
            raise ValueError("Conflicting candidate observations at the same sid and decision time")
        seen[key] = event
        if not chosen or chosen[-1]["sid"] != event["sid"]:
            chosen.append(event)
    return chosen
