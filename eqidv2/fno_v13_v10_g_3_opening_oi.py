"""Pure 09:16-to-09:20 same-contract futures OI comparison.

The result is a candidate *feature*, not an entry rule. ``available_by_0920``
only verifies that the first-minute observation was acquired by that boundary;
callers must independently require a completed, received 09:20 bar before
using it in a live setup. Later archive recovery remains useful for research
but cannot be represented as a live-as-of-09:20 observation.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
import math
import re


def _stamp(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result if result.tzinfo is not None and result.utcoffset() == timedelta(hours=5, minutes=30) else None
    except (ValueError, TypeError):
        return None


def _number(value, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value) if value >= 0 and (not positive or value > 0) else None


def _quality(record):
    for key in ("gap_filled", "is_gap_filled", "provisional_stale", "is_synthetic", "synthetic", "forward_filled", "is_filled", "interpolated", "synthetic_repair"):
        if str(record.get(key, "")).strip().lower() not in {"", "0", "0.0", "false", "no", "none"}:
            return False
    if str(record.get("quality_state", "VALID")).upper() != "VALID":
        return False
    return not re.search(r"SYNTHETIC|GAP.?FILL|FORWARD.?FILL|INTERPOLAT|PROXY|CARRY.?FORWARD", str(record.get("source", "")).upper())


def compare_first_minute_oi_to_0920(first_minute, completed_row, *, boundary):
    """Return a four-minute OI comparison, or ``None`` for invalid evidence.

    ``first_minute`` must describe a real 09:15–09:16 futures minute candle;
    ``completed_row`` must carry the same dated contract's real 09:20 five-
    minute futures OI. Neither the 09:15 snapshot nor the regular five-minute
    OI-change field is inferred or modified by this calculation.
    """
    if not isinstance(first_minute, dict) or not isinstance(completed_row, dict):
        return None
    contract = first_minute.get("contract")
    if not isinstance(contract, str) or not contract or completed_row.get("contract") != contract:
        return None
    if first_minute.get("instrument") != "FUTURES" or first_minute.get("price_basis") != "DATED_NEAR_MONTH_FUTURES":
        return None
    if first_minute.get("source") not in {"KITE_HISTORICAL_MINUTE", "KITE_WEBSOCKET_FULL"} or not _quality(first_minute):
        return None
    if completed_row.get("oi_source") != "NEAR_MONTH_FUTURES_5M" or not _quality(completed_row):
        return None
    start, end, completed = _stamp(first_minute.get("candle_start")), _stamp(first_minute.get("candle_end")), _stamp(boundary)
    if start is None or end is None or completed is None or start.date() != completed.date() or start.time() != time(9, 15) or end - start != timedelta(minutes=1) or completed.time() != time(9, 20) or completed - end != timedelta(minutes=4):
        return None
    opening_oi = _number(first_minute.get("oi"), positive=True)
    completed_oi = _number(completed_row.get("oi"), positive=True)
    if opening_oi is None or completed_oi is None:
        return None
    acquired = _stamp(first_minute.get("acquired_at"))
    change_pct = (completed_oi / opening_oi - 1) * 100
    return {
        "oi_change_since_first_minute_pct": change_pct,
        "oi_change_since_first_minute_from": end.isoformat(),
        "oi_change_since_first_minute_to": completed.isoformat(),
        "oi_change_since_first_minute_interval_minutes": 4,
        "oi_change_since_first_minute_source": "SAME_CONTRACT_FIRST_MINUTE_TO_COMPLETED_5M",
        "available_by_0920": acquired is not None and end <= acquired <= completed,
    }


def bootstrap_0920_oi_change(first_minute, completed_row, *, boundary, as_of=None):
    """Return an opt-in 09:20 OI feature for a future G-3 setup.

    Historical research may use the recovered 09:16 minute, but is explicitly
    labelled retrospective. For a live-as-of feature, both the minute and the
    completed 09:20 bar must have actually been received by ``as_of``. This
    function changes no active strategy's ``oi_change_pct`` or setup clock.
    """
    comparison = compare_first_minute_oi_to_0920(first_minute, completed_row, boundary=boundary)
    if comparison is None:
        return None
    if as_of is not None:
        observed = _stamp(as_of)
        completed = _stamp(boundary)
        minute_end = _stamp(first_minute.get("candle_end"))
        minute_received = _stamp(first_minute.get("acquired_at"))
        bar_received = _stamp(completed_row.get("received_at"))
        if (observed is None or minute_received is None or bar_received is None
                or not minute_end <= minute_received <= observed
                or not completed <= bar_received <= observed):
            return None
    return {
        "oi_change_pct": comparison["oi_change_since_first_minute_pct"],
        "oi": float(completed_row["oi"]),
        "prev_oi": float(first_minute["oi"]),
        "oi_change_source": "FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M",
        "oi_change_interval_minutes": comparison["oi_change_since_first_minute_interval_minutes"],
        "oi_change_from": comparison["oi_change_since_first_minute_from"],
        "oi_change_to": comparison["oi_change_since_first_minute_to"],
        "availability": "LIVE_AS_OF_VERIFIED" if as_of is not None else "RETROSPECTIVE_RESEARCH",
        "minute_received_by_0920": comparison["available_by_0920"],
    }


def bootstrap_0920_oi_change_from_quote(quote, completed_row, *, boundary, as_of):
    """Live-only 09:20 candidate from a timely provisional futures full quote.

    This is not a first-minute candle. The quote's actual exchange time sets
    the comparison interval, and both observations must have been received by
    the requested as-of time. A recovered candle takes precedence in the batch
    adapter below when both are valid.
    """
    if not isinstance(quote, dict) or not isinstance(completed_row, dict):
        return None
    if (quote.get("source") != "KITE_FULL_QUOTE"
            or quote.get("baseline_kind") != "PROVISIONAL_FULL_QUOTE"
            or quote.get("status") != "QUOTE_NEAR_0916_NOT_CANDLE"
            or quote.get("exact_first_minute_candle") is not False
            or completed_row.get("oi_source") != "NEAR_MONTH_FUTURES_5M"
            or not _quality(quote) or not _quality(completed_row)):
        return None
    contract = quote.get("contract")
    if not isinstance(contract, str) or not contract or completed_row.get("contract") != contract:
        return None
    completed, observed = _stamp(boundary), _stamp(as_of)
    exchanged, received = _stamp(quote.get("exchange_timestamp")), _stamp(quote.get("received_at"))
    bar_received = _stamp(completed_row.get("received_at"))
    if any(stamp is None for stamp in (completed, observed, exchanged, received, bar_received)):
        return None
    quote_window = completed - timedelta(minutes=4)
    if (completed.time() != time(9, 20) or exchanged.date() != completed.date()
            or not quote_window <= exchanged <= quote_window + timedelta(seconds=15)
            or not exchanged <= received < completed
            or (received - exchanged).total_seconds() > 15
            or not completed <= bar_received <= observed):
        return None
    quote_oi = _number(quote.get("oi"), positive=True)
    completed_oi = _number(completed_row.get("oi"), positive=True)
    if quote_oi is None or completed_oi is None:
        return None
    return {
        "oi_change_pct": (completed_oi / quote_oi - 1) * 100,
        "oi": completed_oi,
        "prev_oi": quote_oi,
        "oi_change_source": "PROVISIONAL_FULL_QUOTE",
        "oi_change_interval_seconds": int((completed - exchanged).total_seconds()),
        "oi_change_from": exchanged.isoformat(),
        "oi_change_to": completed.isoformat(),
        "availability": "LIVE_AS_OF_VERIFIED",
        "exact_first_minute_candle": False,
    }


def bootstrap_universe_0920_oi_changes(universe, first_minutes, completed_rows, *, boundary, as_of=None,
                                        provisional_quotes=None):
    """Build optional first-slot OI features for every dated stock contract.

    ``first_minutes`` and ``completed_rows`` are contract-keyed evidence maps.
    A stock with incomplete evidence remains in the output with a null feature;
    a different expiry's OI is never used to fill it. Callers can use the same
    contract over historical or live inputs, passing ``as_of`` for live use.
    A provisional quote can fill a live gap only when a validated, timely
    first-minute candle is unavailable.
    """
    if not isinstance(first_minutes, dict) or not isinstance(completed_rows, dict):
        raise TypeError("Opening-minute and completed-bar evidence must be contract-keyed maps")
    if provisional_quotes is not None and not isinstance(provisional_quotes, dict):
        raise TypeError("Provisional quotes must be a contract-keyed map")
    result = []
    seen_symbols, seen_contracts = set(), set()
    for item in universe:
        if not isinstance(item, dict):
            raise TypeError("Dated stock universe rows must be dictionaries")
        symbol, contract = item.get("symbol"), item.get("contract")
        if not isinstance(symbol, str) or not symbol or not isinstance(contract, str) or not contract:
            raise ValueError("Every dated stock universe row needs a symbol and contract")
        if symbol in seen_symbols or contract in seen_contracts:
            raise ValueError("Dated stock universe contains duplicate symbols or contracts")
        seen_symbols.add(symbol)
        seen_contracts.add(contract)
        first, bar = first_minutes.get(contract), completed_rows.get(contract)
        feature = bootstrap_0920_oi_change(first, bar, boundary=boundary, as_of=as_of)
        if feature is None and as_of is not None and provisional_quotes is not None:
            feature = bootstrap_0920_oi_change_from_quote(
                provisional_quotes.get(contract), bar, boundary=boundary, as_of=as_of,
            )
        result.append({
            "symbol": symbol,
            "contract": contract,
            "status": "VALID_0920_BOOTSTRAP" if feature is not None else "MISSING_OR_INVALID_EVIDENCE",
            **(feature if feature is not None else {"oi_change_pct": None}),
        })
    return result
