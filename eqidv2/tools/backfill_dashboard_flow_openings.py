"""Recover a date range of opening-minute evidence into isolated observer storage.

The default command is a local plan. --import-local reads each contract archive
once; --fetch queries one designated app, grouping missing dates into at most
30-calendar-day requests. Expired/recycled contract tokens are never guessed.
This does not reconstruct exact 09:15 snapshots or rerun strategy backtests.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, time as wall_time, timedelta
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dashboard_flow_observer_history as history
from dashboard_flow_observer import (AlreadyRunning, DEFAULT_ROOT, IST, load_universe,
    make_client, now_ist, publish_status, single_instance, stamp)
from dashboard_flow_observer_store import read_json, write_json
from nse_market_calendar import market_closed_reason

AUTH_ERRORS = {"TokenException", "PermissionException"}
SEMANTICS = "Minute OI is interval-end evidence, not an exact 09:15 opening snapshot"


def build_plan(data_root, start, end, *, universe_loader=load_universe, limit=0):
    sessions = []
    grouped = defaultdict(list)
    day = start
    while day <= end:
        closed = market_closed_reason(day)
        if closed:
            sessions.append({"session_date": str(day), "state": "CLOSED", "reason": closed,
                             "universe_count": 0})
        else:
            try:
                rows, source = universe_loader(data_root, day)
                if limit:
                    rows = rows[:limit]
                reason = None if rows else source or "MISSING_UNIVERSE"
            except (OSError, ValueError, KeyError, TypeError) as exc:
                rows, source, reason = [], "UNAVAILABLE", f"Universe{type(exc).__name__}"
            sessions.append({"session_date": str(day), "state": "PLANNED" if rows else "UNAVAILABLE",
                             "reason": reason, "universe_source": source, "universe_count": len(rows)})
            for item in rows:
                grouped[(item["contract"], item["symbol"], item["instrument_token"])].append((day, item))
        day += timedelta(days=1)
    identities = defaultdict(set)
    for contract, symbol, token in grouped:
        identities[contract].add((symbol, token))
    for identity, entries in grouped.items():
        if len(identities[identity[0]]) > 1:
            for _, item in entries:
                item["identity_conflict"] = True
    return sessions, dict(sorted(grouped.items()))


def bounded_ranges(days, max_calendar_days=30):
    """Each request covers at most 30 calendar dates, including both endpoints."""
    if not 1 <= max_calendar_days <= 30:
        raise ValueError("Calendar range must be between 1 and 30 days")
    result = []
    pending = sorted(set(days))
    while pending:
        first = pending[0]
        last_allowed = first + timedelta(days=max_calendar_days - 1)
        batch = [day for day in pending if day <= last_allowed]
        result.append(batch)
        pending = pending[len(batch):]
    return result


def index_opening_records(records, wanted_days):
    wanted_days = set(wanted_days)
    grouped = defaultdict(list)
    for row in records:
        moment = stamp(row.get("candle_start", row.get("date", row.get("ts"))))
        if moment is not None and moment.date() in wanted_days and wall_time(9, 15) <= moment.time() <= wall_time(9, 20):
            grouped[moment.date()].append(row)
    return {day: history.valid_candles(rows, day) for day, rows in grouped.items()}


def load_local_index(data_root, contract, days):
    """Read each known archive once and reject conflicts across archive sources."""
    from dashboard_flow_observer_archives import load_contract_archives
    archives = load_contract_archives(data_root, contract)
    records = (row for archive in archives for row in archive["records"])
    return index_opening_records(records, days), [str(archive["path"]) for archive in archives]


def merge_preserving(existing, incoming):
    """Keep valid saved evidence; additions cannot silently revise a saved minute."""
    merged = {row["candle_start"]: row for row in existing}
    conflicts, added = [], []
    for row in incoming:
        key = row["candle_start"]
        if key in merged and merged[key] != row:
            conflicts.append(key)
        elif key not in merged:
            merged[key] = row
            added.append(key)
    return [merged[key] for key in sorted(merged)], sorted(set(conflicts)), added


def recover_contract(data_root, entries, *, client=None, import_local=False,
                     clock=now_ist, sleep=time.sleep, pace=1.2, retries=2,
                     local_loader=load_local_index, heartbeat=None):
    """Recover one stable contract identity; persist valid additions per session."""
    item = entries[0][1]
    if any((row["contract"], row["symbol"], row["instrument_token"]) !=
           (item["contract"], item["symbol"], item["instrument_token"]) for _, row in entries):
        raise ValueError("Conflicting contract identity in group")
    states = {}
    for day, row in entries:
        saved = read_json(data_root, history.history_path(day, row["contract"])) or {}
        mismatch = bool(saved) and (saved.get("session_date") != str(day) or
            saved.get("contract") != row["contract"] or saved.get("symbol") != row["symbol"])
        identity_conflict = row.get("identity_conflict", False)
        states[day] = {"item": row, "saved": saved, "candles": [] if mismatch or identity_conflict else history.valid_candles(saved.get("candles", []), day),
                       "reason": "StoredHistoricalIdentityMismatch" if mismatch else "UniverseContractIdentityConflict" if identity_conflict else None,
                       "locked": mismatch or identity_conflict, "conflicts": [], "added": [],
                       "imported_from": saved.get("imported_from"), "requests": 0}
    local_error = None
    if import_local and any(not state["locked"] and history.missing_minutes(state["candles"], day) for day, state in states.items()):
        try:
            local, archive = local_loader(data_root, item["contract"], states)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            local, archive, local_error = {}, None, f"LocalArchive{type(exc).__name__}"
        for day, state in states.items():
            if state["locked"]:
                continue
            merged, conflicts, added = merge_preserving(state["candles"], local.get(day, []))
            state["candles"], state["conflicts"] = merged, conflicts
            state["added"].extend(added)
            if added:
                state["imported_from"] = archive
            if local_error:
                state["reason"] = local_error
    pending = [day for day, state in states.items() if not state["locked"] and history.missing_minutes(state["candles"], day)]
    requests, stop_reason = 0, None
    if client is not None and pending:
        identity_reason = history.broker_identity_reason(data_root, item, clock())
        if identity_reason:
            for day in pending:
                states[day]["reason"] = identity_reason
        else:
            for batch in bounded_ranges(pending):
                if stop_reason:
                    break
                error = None
                for attempt in range(max(1, min(retries, 3))):
                    error = "ProductionWindowGuard" if history.broker_guard(clock()) else history.broker_identity_reason(data_root, item, clock()) or None
                    if error:
                        stop_reason = error if error == "ProductionWindowGuard" else None
                        break
                    sleep(max(1.2, pace))
                    if history.broker_guard(clock()):
                        error = stop_reason = "ProductionWindowGuard"
                        break
                    if heartbeat:
                        heartbeat(item["contract"], requests)
                    try:
                        requests += 1
                        for day in batch:
                            states[day]["requests"] += 1
                        records = client.historical_data(item["instrument_token"],
                            datetime.combine(batch[0], wall_time(9, 15), IST),
                            datetime.combine(batch[-1], wall_time(9, 21), IST),
                            "minute", continuous=False, oi=True)
                        fetched = index_opening_records(records, batch)
                        for day in batch:
                            state = states[day]
                            merged, conflicts, added = merge_preserving(state["candles"], fetched.get(day, []))
                            state["candles"] = merged
                            state["conflicts"].extend(conflicts)
                            state["added"].extend(added)
                            state["reason"] = "BrokerOpeningMinutesUnavailable" if history.missing_minutes(merged, day) else None
                        error = None
                        break
                    except Exception as exc:
                        error = type(exc).__name__
                        if error in AUTH_ERRORS:
                            stop_reason = error
                            break
                        if attempt + 1 < max(1, min(retries, 3)):
                            sleep(5)
                if error:
                    for day in batch:
                        states[day]["reason"] = error
            if stop_reason:
                for day in pending:
                    if history.missing_minutes(states[day]["candles"], day) and not states[day]["reason"]:
                        states[day]["reason"] = stop_reason
    outcomes = []
    for day, state in sorted(states.items()):
        missing = history.missing_minutes(state["candles"], day)
        reason = state["reason"] if missing else None
        if missing and not reason:
            reason = "LocalOpeningMinutesUnavailable" if import_local else "NotFetched"
        payload = {"session_date": str(day), "symbol": item["symbol"], "contract": item["contract"],
            "instrument_token": item["instrument_token"], "universe_source": state["item"]["universe_source"],
            "source": "KITE_HISTORICAL_MINUTE", "acquired_at": clock().isoformat(),
            "candles": state["candles"], "imported_from": state["imported_from"],
            "local_archive_paths": state["imported_from"] if isinstance(state["imported_from"], list) else [state["imported_from"]] if state["imported_from"] else [],
            "exact_opening_snapshot": False, "semantics": SEMANTICS,
            "missing_minutes": [value.strftime("%H:%M") for value in missing],
            "requests": state["requests"], "error_type": reason,
            "conflicting_minutes_preserved": sorted(set(state["conflicts"])),
            "state": "COMPLETE" if not missing else "PARTIAL" if state["candles"] else "UNAVAILABLE"}
        # Do not rewrite an existing valid document when no new minutes arrived.
        # An identity mismatch is immutable and only reported in coverage.
        if (import_local or client is not None) and not state["locked"] and (state["added"] or not state["saved"]):
            if state["saved"].get("acquired_at"):
                payload["previous_acquired_at"] = state["saved"]["acquired_at"]
            write_json(data_root, history.history_path(day, item["contract"]), payload)
        outcomes.append({key: value for key, value in payload.items() if key != "candles"} |
                        {"valid_minutes": len(state["candles"]), "added_minutes": len(set(state["added"]))})
    return {"outcomes": outcomes, "requests": requests, "stop_reason": stop_reason}


def coverage_payload(start, end, sessions, outcomes, requests, *, planned, stop_reason=None, plan_only=False):
    per_day = defaultdict(list)
    for row in outcomes:
        per_day[row["session_date"]].append(row)
    dates = []
    for session in sessions:
        rows = per_day.get(session["session_date"], [])
        result = dict(session)
        if session["universe_count"]:
            result.update(complete_contract_sessions=sum(row["state"] == "COMPLETE" for row in rows),
                first_minute_contract_sessions=sum("09:15" not in row["missing_minutes"] for row in rows),
                processed_contract_sessions=len(rows), valid_minutes=sum(row["valid_minutes"] for row in rows),
                added_minutes=sum(row["added_minutes"] for row in rows),
                reasons=dict(Counter(row["error_type"] for row in rows if row["error_type"])))
            result["state"] = "COMPLETE" if result["complete_contract_sessions"] == session["universe_count"] else "PARTIAL"
            if len(rows) < session["universe_count"]:
                result["reasons"][stop_reason or "NotProcessed"] = session["universe_count"] - len(rows)
        dates.append(result)
    complete = sum(row["state"] == "COMPLETE" for row in outcomes)
    full = planned > 0 and complete == planned and not any(row["state"] == "UNAVAILABLE" for row in dates)
    return {"schema": "dashboard_flow_opening_range_v1", "state": "PLAN_ONLY" if plan_only else "COMPLETE" if full else "PARTIAL",
        "session_date": str(end), "from_date": str(start), "to_date": str(end), "mode": "REPORTING_ONLY",
        "completed": len(outcomes), "planned": planned, "complete_contract_sessions": complete,
        "valid_minutes": sum(row["valid_minutes"] for row in outcomes),
        "added_minutes": sum(row["added_minutes"] for row in outcomes),
        "requests": requests, "production_writes": 0, "stop_reason": stop_reason,
        "exact_opening_snapshot": False, "semantics": SEMANTICS, "sessions": dates, "outcomes": outcomes}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--from-date", type=date.fromisoformat, required=True)
    parser.add_argument("--to-date", type=date.fromisoformat)
    parser.add_argument("--import-local", action="store_true")
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--app", default="app1", choices=[f"app{i}" for i in range(1, 9)])
    parser.add_argument("--credential-dir", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--limit", type=int, default=0, help="Contracts per session for a pilot")
    args = parser.parse_args(argv)
    start, end = args.from_date, args.to_date or history.latest_completed(now_ist())
    if start > end or end > history.latest_completed(now_ist()) or args.limit < 0:
        parser.error("Use completed session dates in ascending order and a nonnegative limit")
    sessions, grouped = build_plan(args.data_root, start, end, limit=args.limit)
    planned = sum(len(entries) for entries in grouped.values())
    mutating = args.import_local or args.fetch
    if not mutating:
        outcomes = []
        broker_ranges = 0
        for entries in grouped.values():
            result = recover_contract(args.data_root, entries)
            outcomes.extend(result["outcomes"])
            broker_ranges += len(bounded_ranges([date.fromisoformat(row["session_date"]) for row in result["outcomes"] if row["missing_minutes"]]))
        payload = coverage_payload(start, end, sessions, outcomes, 0, planned=planned, plan_only=True)
        print(json.dumps({key: value for key, value in payload.items() if key not in {"sessions", "outcomes"}} |
                         {"contracts": len(grouped), "maximum_broker_ranges_before_local_import": broker_ranges,
                          "universe_unavailable_dates": [row["session_date"] for row in sessions if row["state"] == "UNAVAILABLE"]}))
        return 0
    if args.fetch and history.broker_guard(now_ist()):
        print(json.dumps({"state": "SKIPPED", "reason": "PRODUCTION_COLLECTION_AND_EOD_WINDOW"}))
        return 0
    try:
        with single_instance(args.data_root, "opening_history"):
            client = make_client(args.app, args.credential_dir) if args.fetch else None
            outcomes, requests, stop_reason = [], 0, None
            for index, entries in enumerate(grouped.values()):
                base = {"state": "RUNNING", "session_date": str(end), "from_date": str(start), "to_date": str(end),
                    "mode": "REPORTING_ONLY", "planned": planned, "completed": len(outcomes),
                    "contracts": len(grouped), "completed_contracts": index, "requests": requests}
                def heartbeat(contract, group_requests):
                    publish_status(args.data_root, history.STATUS, base | {"current_contract": contract, "requests": requests + group_requests})
                heartbeat(entries[0][1]["contract"], 0)
                result = recover_contract(args.data_root, entries, client=client,
                                          import_local=args.import_local, heartbeat=heartbeat)
                outcomes.extend(result["outcomes"])
                requests += result["requests"]
                if result["stop_reason"]:
                    stop_reason = result["stop_reason"]
                    client = None
            payload = coverage_payload(start, end, sessions, outcomes, requests, planned=planned, stop_reason=stop_reason)
            write_json(args.data_root, f"coverage/history_{start}_{end}.json", payload)
            summary = {key: value for key, value in payload.items() if key not in {"sessions", "outcomes"}}
            publish_status(args.data_root, history.STATUS, summary)
            print(json.dumps(summary))
            return 0
    except AlreadyRunning:
        print(json.dumps({"state": "SKIPPED", "reason": "ALREADY_RUNNING"}))
        return 0
    except Exception as exc:
        summary = {"state": "FAILED", "session_date": str(end), "from_date": str(start),
                   "to_date": str(end), "mode": "REPORTING_ONLY", "error_type": type(exc).__name__}
        publish_status(args.data_root, history.STATUS, summary)
        print(json.dumps(summary))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
