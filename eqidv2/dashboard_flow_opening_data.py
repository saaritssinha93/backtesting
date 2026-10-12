"""Read-only opening evidence overlay for G3 reporting, never strategy inputs.

``apply_opening_observations(root, market, universe, now=...)`` returns detached
rows. Disabled-by-default config.json gates all consumption. Only observations
both exchanged and received by a boundary, at most 60 seconds old, qualify.
Minute-candle OI is context available after its end, never exact 09:15 OI.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time, timedelta
import hashlib
import math
from pathlib import Path
import re

from dashboard_flow_observer_store import IST, observer_root, read_events_with_audit, read_json, session_key
from fno_v13_v10_g_3_opening_oi import compare_first_minute_oi_to_0920


def _number(value, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value) if value >= 0 and (not positive or value > 0) else None


def _pct(current, baseline):
    a, b = _number(current), _number(baseline, positive=True)
    return (a / b - 1) * 100 if a is not None and b is not None else None


def _stamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.astimezone(IST) if result.tzinfo is not None else None
    except (ValueError, TypeError):
        return None


def opening_config(data_root):
    """Read without creating anything; malformed config always disables usage."""
    try:
        config = read_json(data_root, "config.json")
        return config if isinstance(config, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def opening_enabled(data_root):
    return opening_config(data_root).get("opening_enabled") is True


def _read_status(data_root, path, day, flags):
    try:
        value = read_json(data_root, path)
    except (OSError, ValueError, TypeError):
        if "OBSERVER_STATUS_READ_FAILED" not in flags:
            flags.append("OBSERVER_STATUS_READ_FAILED")
        return None
    return value if isinstance(value, dict) and value.get("session_date") == day else None


def _latest(events, boundary, field):
    qualified = []
    for record in events:
        if not _quality(record) or any(word in record.get("source", "").upper() for word in ("HISTORICAL", "CANDLE", "PROXY")):
            continue
        exchange, received = _stamp(record.get("exchange_timestamp")), _stamp(record.get("received_at"))
        if exchange is None or received is None or _number(record.get(field)) is None:
            continue
        if exchange.date() == boundary.date() and exchange <= received <= boundary and 0 <= (boundary - exchange).total_seconds() <= 60:
            qualified.append((exchange, received, record))
    return max(qualified, key=lambda item: item[:2])[2] if qualified else None


def _quality(record):
    for flag in ("gap_filled", "is_gap_filled", "provisional_stale", "is_synthetic", "synthetic", "forward_filled", "is_filled", "interpolated", "synthetic_repair"):
        if str(record.get(flag, "")).strip().lower() not in {"", "0", "0.0", "false", "no", "none"}:
            return False
    if str(record.get("quality_state", "VALID")).upper() != "VALID":
        return False
    return not re.search(r"SYNTHETIC|GAP.?FILL|FORWARD.?FILL|INTERPOLAT|PROXY|CARRY.?FORWARD", str(record.get("source", "")).upper())


def _safe_contract(contract):
    stem = re.sub(r"[^A-Z0-9._-]+", "_", contract).strip("._") or "CONTRACT"
    return f"{stem}_{hashlib.sha1(contract.encode()).hexdigest()[:8]}" if stem != contract else stem


def _first_minute(root, day, symbol, contract):
    raw = read_json(root, Path("historical") / day / f"{_safe_contract(contract)}.json")
    # The saved dated universe maps the LTM futures underlying to the LTIM
    # cash symbol. Preserve that one explicit mapping while requiring the
    # exact dated futures contract; never accept a generic symbol mismatch.
    identity_matches = isinstance(raw, dict) and (raw.get("symbol") == symbol or
        (raw.get("symbol") == "LTM" and symbol == "LTIM" and re.fullmatch(r"LTM\d{2}[A-Z]{3}FUT", contract) is not None))
    if not identity_matches or raw.get("session_date") != day or raw.get("contract") != contract or raw.get("source") != "KITE_HISTORICAL_MINUTE":
        return None
    for candle in raw.get("candles", []):
        if not isinstance(candle, dict) or not _quality(candle):
            continue
        start, end = _stamp(candle.get("candle_start")), _stamp(candle.get("candle_end"))
        if start is None or end is None or start.date().isoformat() != day or start.time() != time(9, 15) or end - start != timedelta(minutes=1):
            continue
        values = {key: _number(candle.get(key), positive=key in {"open", "high", "low", "close"}) for key in ("open", "high", "low", "close", "volume", "oi")}
        if any(value is None for value in values.values()) or values["high"] < max(values["open"], values["close"]) or values["low"] > min(values["open"], values["close"]):
            continue
        return {
            "candle_start": start.isoformat(), "candle_end": end.isoformat(),
            "instrument": "FUTURES", "price_basis": "DATED_NEAR_MONTH_FUTURES", "contract": contract,
            **values, "source": raw["source"], "acquired_at": raw.get("acquired_at"),
            "retrospective": True,
        }
    return None


def _live_quote_baselines(root, day, current, audit_flags):
    """Read the observer's dated-universe near-09:16 quote inventory.

    Each accepted quote is a provisional baseline at its own exchange time,
    never a one-minute candle or a synthetic 09:16:00 observation.
    """
    try:
        archive = read_json(root, Path("coverage") / f"live_opening_oi_{day}.json")
    except (OSError, ValueError, TypeError):
        audit_flags.append("LIVE_OPENING_QUOTE_READ_FAILED")
        return {}
    if not isinstance(archive, dict) or archive.get("schema") != "dashboard_flow_live_opening_oi_v1" or archive.get("session_date") != day or archive.get("universe_source") != "DATED_UNIVERSE":
        return {}
    target = datetime.fromisoformat(f"{day}T09:16:00").replace(tzinfo=IST)
    cutoff = target + timedelta(minutes=4)
    qualified = {}
    duplicates = set()
    rows = archive.get("rows")
    if not isinstance(rows, list):
        audit_flags.append("LIVE_OPENING_QUOTE_SCHEMA_INVALID")
        return {}
    for item in rows:
        if not isinstance(item, dict) or item.get("status") != "QUOTE_NEAR_0916_NOT_CANDLE" or item.get("baseline_kind") != "PROVISIONAL_FULL_QUOTE" or item.get("source") != "KITE_FULL_QUOTE" or item.get("exact_first_minute_candle") is not False or item.get("quote_baseline_available_by_0920") is not True or not _quality(item):
            continue
        symbol, contract = item.get("symbol"), item.get("contract")
        exchange, received = _stamp(item.get("exchange_timestamp")), _stamp(item.get("received_at"))
        oi = _number(item.get("oi"), positive=True)
        if not isinstance(symbol, str) or not isinstance(contract, str) or not symbol or not contract or exchange is None or received is None or oi is None:
            continue
        if not (target <= exchange <= target + timedelta(seconds=15) and exchange <= received < cutoff and received <= current and 0 <= (received - exchange).total_seconds() <= 15):
            continue
        key = symbol, contract
        if key in qualified:
            duplicates.add(key)
        qualified[key] = {"oi": oi, "exchange_timestamp": exchange.isoformat(), "received_at": received.isoformat(), "contract": contract,
                          "source": "KITE_FULL_QUOTE", "baseline_kind": "PROVISIONAL_FULL_QUOTE", "seconds_to_0920": (cutoff - exchange).total_seconds()}
    for key in duplicates:
        qualified.pop(key, None)
    if duplicates:
        audit_flags.append("DUPLICATE_LIVE_OPENING_QUOTES_IGNORED")
    return qualified


def apply_opening_observations(data_root, market, universe=None, *, now=None):
    """Add independently sourced reporting fields while preserving base inputs.

    At 09:20, a validated 09:16 minute or timely provisional quote may
    bootstrap the displayed OI change over the actual interval. Neither is
    represented as an exact adjacent five-minute change. Production-derived
    values at 09:25 and later are never replaced.
    """
    day = session_key(market["session_date"])
    current = now or datetime.now(IST)
    current = current.replace(tzinfo=IST) if current.tzinfo is None else current.astimezone(IST)
    opening = datetime.combine(datetime.fromisoformat(day).date(), time(9, 15), IST)
    earlier = opening - timedelta(minutes=5)
    result = dict(market)
    result["slots"] = [{**slot, "rows": [dict(row) for row in slot.get("rows", [])], "coverage": dict(slot.get("coverage", {}))} for slot in market.get("slots", [])]
    result["opening_first_minutes"] = []
    result["warnings"] = list(market.get("warnings", []))
    result["sources"] = list(market.get("sources", []))
    enabled = opening_enabled(data_root)
    summary = {"enabled": enabled, "state": "DISABLED", "message": "Opening observations are disabled; production reporting is unchanged.", "session_date": day,
               "observed_symbols": 0, "verified_opening_oi": 0, "first_minute_symbols": 0,
               "source_root": str(observer_root(data_root)), "read_only": True, "strategy_inputs_changed": False,
               "audit_flags": [], "max_age_seconds": 60}
    result["opening_observer"] = summary
    if not enabled:
        return result
    try:
        records, audit = read_events_with_audit(data_root, day)
        summary["record_audit"] = audit
        if audit["invalid_records"] or audit["partial_records"]:
            summary["audit_flags"].append("INVALID_OR_PARTIAL_OBSERVATIONS_IGNORED")
    except (OSError, ValueError, TypeError):
        records = []
        summary["audit_flags"].append("OBSERVER_SOURCE_READ_FAILED")
    status = _read_status(data_root, "status/opening_observer.json", day, summary["audit_flags"])
    if status is None:
        status = _read_status(data_root, Path("status") / f"{day}.json", day, summary["audit_flags"])
    history_status = _read_status(data_root, "status/opening_history.json", day, summary["audit_flags"])
    # Status is deliberately a small allowlist, never arbitrary config/credentials.
    if isinstance(status, dict):
        summary["collector"] = {key: status.get(key) for key in ("state", "session_date", "updated_at", "started_at", "stopped_at", "event_count", "message") if key in status}
    if isinstance(history_status, dict):
        summary["history_collector"] = {key: history_status.get(key) for key in ("state", "session_date", "updated_at", "started_at", "stopped_at", "message") if key in history_status}
    live_quotes = _live_quote_baselines(data_root, day, current, summary["audit_flags"])
    summary["provisional_quote_symbols"] = len(live_quotes)
    by_key = defaultdict(list)
    for record in records:
        received, exchange = _stamp(record.get("received_at")), _stamp(record.get("exchange_timestamp"))
        if received and exchange and received <= current and exchange <= current:
            by_key[(record["symbol"], record["contract"] if record["kind"] == "future" else "", record["kind"])].append(record)
    summary["observed_symbols"] = len({key[0] for key in by_key})
    membership = {(row["symbol"], row["contract"]) for row in (universe or [])}
    membership.update((row["symbol"], row["contract"]) for slot in result["slots"] for row in slot["rows"] if row.get("contract"))
    previous_oi = {}
    for slot in result["slots"]:
        for row in slot["rows"]:
            key = (row.get("symbol"), row.get("contract"))
            if key in membership and key not in previous_oi and _number(row.get("previous_oi")) is not None:
                previous_oi[key] = row["previous_oi"]
    contexts = {}
    for symbol, contract in sorted(membership):
        future = by_key[(symbol, contract, "future")]
        cash = by_key[(symbol, "", "cash")]
        start_oi = _latest(future, opening, "oi") if opening <= current else None
        prior_oi = _latest(future, earlier, "oi") if earlier <= current else None
        start_price = _latest(cash, opening, "price") if opening <= current else None
        prior_price = _latest(cash, earlier, "price") if earlier <= current else None
        context = {"quality": "VERIFIED_OPENING_SNAPSHOT" if start_oi else "UNAVAILABLE", "reason": "Timestamped same-contract OI observed by 09:15." if start_oi else "No same-contract OI observed and received by 09:15 within the 60-second freshness limit.",
                   "opening_oi": start_oi["oi"] if start_oi else None, "opening_oi_time": start_oi["exchange_timestamp"] if start_oi else None,
                   "opening_oi_received_at": start_oi["received_at"] if start_oi else None,
                   "opening_oi_age_seconds": (opening - _stamp(start_oi["exchange_timestamp"])).total_seconds() if start_oi else None,
                   "opening_oi_source": start_oi["source"] if start_oi else None,
                   "price_preopen_change_pct": _pct(start_price["price"], prior_price["price"]) if start_price and prior_price else None,
                   "oi_preopen_change_pct": _pct(start_oi["oi"], prior_oi["oi"]) if start_oi and prior_oi else None,
                   "preopen_price_from": prior_price["exchange_timestamp"] if prior_price else None, "preopen_price_to": start_price["exchange_timestamp"] if start_price else None,
                   "preopen_oi_from": prior_oi["exchange_timestamp"] if prior_oi else None, "preopen_oi_to": start_oi["exchange_timestamp"] if start_oi else None,
                   "first_minute_oi": None, "first_minute_end": None, "contract": contract}
        try:
            first = _first_minute(data_root, day, symbol, contract)
        except (OSError, ValueError, TypeError):
            first = None
            if "HISTORICAL_CONTEXT_READ_FAILED" not in summary["audit_flags"]:
                summary["audit_flags"].append("HISTORICAL_CONTEXT_READ_FAILED")
        contexts[(symbol, contract)] = (context, first, start_price, live_quotes.get((symbol, contract)))
        summary["verified_opening_oi"] += start_oi is not None
        summary["first_minute_symbols"] += first is not None and _stamp(first["candle_end"]) <= current
        if first and _stamp(first["candle_end"]) <= current:
            baseline = previous_oi.get((symbol, contract))
            result["opening_first_minutes"].append({
                "symbol": symbol, "contract": contract,
                "first_minute": {**first, "previous_oi": baseline, "oi_day_change_pct": _pct(first["oi"], baseline)},
            })
    for slot in result["slots"]:
        boundary = datetime.fromisoformat(f"{day}T{slot['time']}:00").replace(tzinfo=IST)
        if boundary > current:
            continue
        kept = []
        for row in slot["rows"]:
            entry = contexts.get((row.get("symbol"), row.get("contract")))
            if entry is None:
                kept.append(row)
                continue
            template, first, start_price, live_quote = entry
            context = dict(template)
            if first and _stamp(first["candle_end"]) <= boundary:
                context.update(first_minute_oi=first["oi"], first_minute_end=first["candle_end"], first_minute_source=first["source"], first_minute_acquired_at=first["acquired_at"], first_minute_retrospective=True)
                if slot["time"] == "09:20":
                    # Include full minute OHLC once on the first completed 5m
                    # slot. The top-level collection preserves the evidence
                    # when that regular slot is absent, without repeating the
                    # payload across every later five-minute row.
                    context["first_minute"] = {
                        **first, "previous_oi": row.get("previous_oi"),
                        "oi_day_change_pct": _pct(first["oi"], row.get("previous_oi")),
                    }
                if context["opening_oi"] is None:
                    context["quality"] = "FIRST_MINUTE_OBSERVATION"
                    context["reason"] = "First-minute OI is historical context after 09:16; exact opening OI remains unavailable."
            row["opening_context"] = context
            if slot["time"] == "09:15":
                if context["opening_oi"] is not None:
                    row["oi"] = context["opening_oi"]
                    row["oi_source"] = "OBSERVER_VERIFIED_OPENING_SNAPSHOT"
                    row["oi_day_change_pct"] = _pct(row["oi"], row.get("previous_oi"))
                    row["oi_change_pct"] = row["oi_day_change_pct"]
                # Cash LTP may be pre-open. Do not turn it into an official open
                # or overwrite the existing completed-candle opening reference.
                row["change5m_pct"] = None
                row["oi_change5m_pct"] = None
                if row.get("price") is None and row.get("oi") is None:
                    continue
            elif slot["time"] == "09:20" and context["opening_oi"] is not None:
                context["oi_opening_change_pct"] = _pct(row.get("oi"), context["opening_oi"])
                context["comparison_seconds"] = (boundary - _stamp(context["opening_oi_time"])).total_seconds()
                context["comparison_from"] = context["opening_oi_time"]
                context["comparison_to"] = boundary.isoformat()
                if _stamp(context["opening_oi_time"]) == opening and row.get("oi_change5m_pct") is None:
                    row["oi_change5m_pct"] = context["oi_opening_change_pct"]
                    row["oi_change5m_source"] = "EXACT_OPENING_OBSERVER_BASELINE"
            if slot["time"] == "09:20" and first:
                comparison = compare_first_minute_oi_to_0920(first, row, boundary=boundary)
                if comparison is not None:
                    context.update(comparison)
                    context["oi_change_since_0916_pct"] = comparison["oi_change_since_first_minute_pct"]
                    context["oi_change_since_0916_source"] = "VALIDATED_FIRST_MINUTE_CANDLE"
                    context["oi_change_since_0916_from"] = comparison["oi_change_since_first_minute_from"]
                    context["oi_change_since_0916_to"] = comparison["oi_change_since_first_minute_to"]
                    context["oi_change_since_0916_interval_seconds"] = 240
                    # User-selected first-bar bootstrap: the 09:20 display
                    # metric uses the 09:16 minute OI when it exists. This is
                    # four elapsed minutes, not an exact adjacent 5m change.
                    # Preserve the distinction and live availability in the
                    # source fields; strategy evidence is never rewritten.
                    row["oi_change5m_pct"] = comparison["oi_change_since_first_minute_pct"]
                    row["oi_change_pct"] = comparison["oi_change_since_first_minute_pct"]
                    row["oi_change5m_source"] = "FIRST_MINUTE_0916_BOOTSTRAP_4M"
                    row["oi_change5m_from"] = comparison["oi_change_since_first_minute_from"]
                    row["oi_change5m_to"] = comparison["oi_change_since_first_minute_to"]
                    row["oi_change5m_interval_minutes"] = comparison["oi_change_since_first_minute_interval_minutes"]
                    row["oi_change5m_available_by_0920"] = comparison["available_by_0920"]
                    row["oi_change5m_baseline_available_by_0920"] = comparison["available_by_0920"]
                    row["oi_change5m_completed_bar_received_by_0920"] = None
            if slot["time"] == "09:20" and live_quote:
                context["provisional_quote_baseline"] = live_quote
                if context["quality"] == "UNAVAILABLE":
                    context["quality"] = "PROVISIONAL_QUOTE_BASELINE"
                    context["reason"] = "A fresh same-contract full quote near 09:16 is available; this is not an exact first-minute candle."
                if row.get("oi_change5m_source") != "FIRST_MINUTE_0916_BOOTSTRAP_4M" and row.get("oi_source") == "NEAR_MONTH_FUTURES_5M" and _quality(row) and _number(row.get("oi"), positive=True) is not None:
                    comparison_pct = _pct(row.get("oi"), live_quote["oi"])
                    if comparison_pct is not None:
                        context["oi_change_since_0916_pct"] = comparison_pct
                        context["oi_change_since_0916_source"] = "PROVISIONAL_FULL_QUOTE"
                        context["oi_change_since_0916_from"] = live_quote["exchange_timestamp"]
                        context["oi_change_since_0916_to"] = boundary.isoformat()
                        context["oi_change_since_0916_interval_seconds"] = live_quote["seconds_to_0920"]
                        context["quote_baseline_available_by_0920"] = True
                        row["oi_change5m_pct"] = comparison_pct
                        row["oi_change_pct"] = comparison_pct
                        row["oi_change5m_source"] = "LIVE_QUOTE_NEAR_0916_BOOTSTRAP_PROVISIONAL"
                        row["oi_change5m_from"] = live_quote["exchange_timestamp"]
                        row["oi_change5m_to"] = boundary.isoformat()
                        row["oi_change5m_interval_minutes"] = live_quote["seconds_to_0920"] / 60
                        row["oi_change5m_available_by_0920"] = True
                        row["oi_change5m_baseline_available_by_0920"] = True
                        row["oi_change5m_completed_bar_received_by_0920"] = None
            kept.append(row)
        slot["rows"] = kept
        slot["coverage"].update(oi=sum(row.get("oi") is not None for row in kept), oi_baseline=sum(row.get("oi_change_pct") is not None for row in kept), oi_change5m=sum(row.get("oi_change5m_pct") is not None for row in kept))
        if kept and slot.get("state") == "MISSING":
            slot["state"] = "OPENING_REFERENCE" if slot["time"] == "09:15" else "PARTIAL"
        elif not kept and slot.get("state") != "FUTURE":
            slot["state"] = "MISSING"
    summary["state"] = "AVAILABLE" if summary["verified_opening_oi"] else "HISTORICAL_CONTEXT_ONLY" if summary["first_minute_symbols"] else "PROVISIONAL_QUOTE_AVAILABLE" if summary["provisional_quote_symbols"] else "UNAVAILABLE"
    summary["message"] = f"{summary['verified_opening_oi']} verified opening OI observations; {summary['first_minute_symbols']} first-minute historical contexts; {summary['provisional_quote_symbols']} timely provisional 09:16 quotes. Valid 09:20 comparisons bootstrap the displayed OI change over their actual interval; strategy decisions are unchanged."
    result.setdefault("semantics", {}).update(
        oi_change5m_pct="Regular slots compare the same contract at adjacent five-minute boundaries; 09:20 may instead use a validated 09:16 first-minute OI over four minutes or a timely quote near 09:16 over its actual interval. Source and interval fields identify the exception.",
        oi_change_pct="Dashboard ranking OI change follows oi_change5m_pct, including the explicitly sourced 09:20 first-bar bootstrap.",
    )
    if not summary["verified_opening_oi"]:
        summary["audit_flags"].append("EXACT_OPENING_EVIDENCE_UNAVAILABLE")
    result["sources"].append({"label": "Isolated opening observer", "path": summary["source_root"], "detail": "Timestamped snapshots require exchange and receipt before the boundary, same contract and age at most 60 seconds. Historical minute OI is separate retrospective context."})
    return result
