"""Recover opening minute evidence in isolated observer storage, never candles used by trading.

Local planning is the default. --import-local copies qualified evidence from
existing archives. --fetch explicitly enables the selected broker app and is
blocked throughout production collection and EOD catch-up (08:30-17:00 IST).
Minute candle OI is interval-end evidence, never an exact 09:15 opening snapshot.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, time as wall_time, timedelta
import json
import hashlib
from functools import lru_cache
from pathlib import Path
import re
import time

from dashboard_flow_observer import (AlreadyRunning, DEFAULT_ROOT, IST, load_universe, make_client,
    now_ist, number, publish_status, safe_name, single_instance, stamp)
from dashboard_flow_observer_store import read_json, write_json
from nse_market_calendar import market_closed_reason

STATUS = "status/opening_history.json"


def broker_guard(moment):
    moment = moment.astimezone(IST)
    if not market_closed_reason(moment.date()) and wall_time(8, 30) <= moment.time() < wall_time(17):
        return "PRODUCTION_COLLECTION_AND_EOD_WINDOW"
    return ""


def latest_completed(moment):
    moment = moment.astimezone(IST)
    day = moment.date()
    if moment.time() <= wall_time(15, 30):
        day -= timedelta(days=1)
    while market_closed_reason(day):
        day -= timedelta(days=1)
    return day


def opening_minutes(day):
    first = datetime.combine(day, wall_time(9, 15), IST)
    return [first + timedelta(minutes=i) for i in range(6)]


def history_path(day, contract):
    contract = safe_name(contract)
    stem = re.sub(r"[^A-Z0-9._-]+", "_", contract).strip("._") or "CONTRACT"
    stem = f"{stem}_{hashlib.sha1(contract.encode()).hexdigest()[:8]}" if stem != contract else stem
    return f"historical/{day}/{stem}.json"


def valid_candles(records, day):
    """Validate real minute intervals. Duplicated conflicting intervals are excluded."""
    allowed = set(opening_minutes(day))
    by_start = {}
    conflicts = set()
    for row in records:
        start = stamp(row.get("candle_start", row.get("date", row.get("ts"))))
        if start not in allowed:
            continue
        # An explicit interval boundary is provenance, not a formatting hint.
        # Never turn an existing five-minute candle into first-minute evidence.
        if "candle_end" in row and stamp(row["candle_end"]) != start + timedelta(minutes=1):
            continue
        source = str(row.get("source", "")).upper()
        if any(x in source for x in ("SYNTHETIC", "GAP_FILL", "GAPFIL", "FORWARD", "INTERPOLAT", "PROXY")):
            continue
        if any(str(row.get(flag, "")).lower() not in {"", "0", "0.0", "false", "no", "none", "nan"}
               for flag in ("gap_filled", "is_gap_filled", "synthetic", "is_synthetic", "is_forward_filled",
                            "provisional_stale", "forward_filled", "is_filled", "interpolated", "synthetic_repair")):
            continue
        if str(row.get("quality_state", "VALID")).upper() != "VALID":
            continue
        values = {key: number(row.get(key), positive=key in {"open", "high", "low", "close"})
                  for key in ("open", "high", "low", "close", "volume", "oi")}
        if any(value is None for value in values.values()):
            continue
        if values["high"] < max(values["open"], values["close"], values["low"]) or values["low"] > min(values["open"], values["close"]):
            continue
        candle = {"candle_start": start.isoformat(), "candle_end": (start + timedelta(minutes=1)).isoformat(),
                  **values}
        if start in by_start and by_start[start] != candle:
            conflicts.add(start)
        by_start[start] = candle
    return [by_start[key] for key in sorted(by_start) if key not in conflicts]


def missing_minutes(candles, day):
    available = {stamp(row["candle_start"]) for row in valid_candles(candles, day)}
    return [value for value in opening_minutes(day) if value not in available]


def local_records(data_root, contract):
    """Read-only archive access; never update the confirmation engine's parquet."""
    import pandas as pd
    # Match the production producer's collision-resistant naming, read only.
    stem = Path(history_path(date(2000, 1, 1), contract)).stem
    path = Path(data_root) / "fno_oi" / "raw_contracts_1m_hist" / f"{stem}_1minute.parquet"
    if not path.exists():
        return [], None
    return pd.read_parquet(path).to_dict("records"), str(path)


@lru_cache(maxsize=4)
def _current_master(path, mtime_ns, size):
    import pandas as pd
    frame = pd.read_parquet(path)
    dates = set(pd.to_datetime(frame.get("master_date", pd.Series(dtype=str)), errors="coerce").dropna().dt.date)
    identities = {(int(row["instrument_token"]), str(row["tradingsymbol"])) for row in frame.to_dict("records")
                  if number(row.get("instrument_token"), positive=True) is not None}
    return dates, identities


def broker_identity_reason(data_root, item, current):
    """Expired tokens may be recycled: never query them as if identity were stable."""
    master = Path(data_root) / "fno_oi/instrument_master/latest_instrument_master.parquet"
    try:
        metadata = master.stat()
        dates, identities = _current_master(str(master), metadata.st_mtime_ns, metadata.st_size)
    except (OSError, ValueError, KeyError, TypeError):
        return "CurrentInstrumentMasterUnavailable"
    if dates != {latest_completed(current)}:
        return "CurrentInstrumentMasterStale"
    if (item["instrument_token"], item["contract"]) not in identities:
        return "HistoricalContractTokenNotCurrentlyVerified"
    return ""


def recover_one(data_root, day, item, *, client=None, import_local=False,
                clock=now_ist, sleep=time.sleep, pace=1.2, retries=2):
    path = history_path(day, item["contract"])
    saved = read_json(data_root, path) or {}
    if saved and (saved.get("session_date") != str(day) or saved.get("contract") != item["contract"]
                  or saved.get("symbol") != item["symbol"]):
        raise ValueError("Stored historical identity mismatch")
    prior = valid_candles(saved.get("candles", []), day)
    candles = list(prior)
    imported_from = saved.get("imported_from")
    error_type = None
    requests = 0
    if import_local and missing_minutes(candles, day):
        records, archive = local_records(data_root, item["contract"])
        local = valid_candles(records, day)
        existing = {row["candle_start"]: row for row in local}
        existing.update({row["candle_start"]: row for row in candles})
        candles = [existing[key] for key in sorted(existing)]
        if local:
            imported_from = archive
    missing = missing_minutes(candles, day)
    if client is not None and missing:
        for attempt in range(retries):
            if broker_guard(clock()):
                error_type = "ProductionWindowGuard"
                break
            identity_reason = broker_identity_reason(data_root, item, clock())
            if identity_reason:
                error_type = identity_reason
                break
            sleep(max(1.2, pace))
            if broker_guard(clock()):
                error_type = "ProductionWindowGuard"
                break
            try:
                requests += 1
                # Full six-minute opening interval repairs INTERNAL missing minutes.
                records = client.historical_data(item["instrument_token"], min(missing), max(missing) + timedelta(minutes=1),
                                                 "minute", continuous=False, oi=True)
                fetched = valid_candles(records, day)
                error_type = None
                existing = {row["candle_start"]: row for row in candles}
                existing.update({row["candle_start"]: row for row in fetched})
                candles = [existing[key] for key in sorted(existing)]
                break
            except Exception as exc:
                error_type = type(exc).__name__
                if error_type in {"TokenException", "PermissionException"}:
                    break
                if attempt + 1 < retries:
                    sleep(5)
    missing = missing_minutes(candles, day)
    result = {"session_date": str(day), "symbol": item["symbol"], "contract": item["contract"],
              "source": "KITE_HISTORICAL_MINUTE", "acquired_at": clock().isoformat(),
              "universe_source": item["universe_source"], "candles": candles,
              "exact_opening_snapshot": False, "semantics": "Minute OI is interval-end evidence, not exact opening OI",
              "missing_minutes": [value.strftime("%H:%M") for value in missing],
              "requests": requests, "error_type": error_type, "imported_from": imported_from,
              "state": "COMPLETE" if not missing else "PARTIAL" if candles else "UNAVAILABLE"}
    if import_local or client is not None:
        write_json(data_root, path, result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--from-date", type=date.fromisoformat)
    parser.add_argument("--to-date", type=date.fromisoformat)
    parser.add_argument("--latest-completed", action="store_true")
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--import-local", action="store_true")
    parser.add_argument("--scheduled", action="store_true")
    parser.add_argument("--app", default="app1")
    parser.add_argument("--credential-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--limit", type=int, default=0, help="Maximum contracts per date for a pilot")
    args = parser.parse_args(argv)
    now = now_ist()
    end = args.to_date or latest_completed(now)
    start = args.from_date or end
    if args.latest_completed:
        start = end = latest_completed(now)
    if start > end or end > latest_completed(now):
        parser.error("Dates must be completed sessions with from-date <= to-date")
    mutating = args.fetch or args.import_local
    if args.fetch and broker_guard(now):
        publish_status(args.data_root, STATUS, {"state": "SKIPPED", "reason": broker_guard(now),
            "session_date": str(end), "mode": "REPORTING_ONLY"})
        print(json.dumps({"state": "SKIPPED", "reason": broker_guard(now)}))
        return 0
    plans = []
    day = start
    while day <= end:
        if not market_closed_reason(day):
            rows, source = load_universe(args.data_root, day)
            if args.limit > 0:
                rows = rows[:args.limit]
            plans.extend((day, row) for row in rows)
        day += timedelta(days=1)
    if not mutating:
        missing = sum(bool(missing_minutes((read_json(args.data_root, history_path(day, row["contract"])) or {}).get("candles", []), day))
                      for day, row in plans)
        print(json.dumps({"state": "PLAN_ONLY", "from_date": str(start), "to_date": str(end),
                          "contract_sessions": len(plans), "sessions_needing_opening_minutes": missing,
                          "broker_calls": 0, "production_writes": 0}))
        return 0
    try:
        with single_instance(args.data_root, "opening_history"):
            client = make_client(args.app, args.credential_dir) if args.fetch else None
            outcomes = []
            for day, row in plans:
                if args.fetch and broker_guard(now_ist()):
                    break
                outcome = recover_one(args.data_root, day, row, client=client, import_local=args.import_local)
                outcomes.append({key: value for key, value in outcome.items() if key != "candles"})
                publish_status(args.data_root, STATUS, {"state": "RUNNING", "session_date": str(end),
                    "mode": "REPORTING_ONLY", "completed": len(outcomes), "planned": len(plans)})
                if outcome["error_type"] in {"TokenException", "PermissionException"}:
                    break
            complete = sum(row["state"] == "COMPLETE" for row in outcomes)
            payload = {"state": "COMPLETE" if complete == len(plans) and plans else "PARTIAL",
                       "session_date": str(end), "from_date": str(start), "mode": "REPORTING_ONLY",
                       "completed": len(outcomes), "planned": len(plans), "complete_contract_sessions": complete,
                       "requests": sum(row["requests"] for row in outcomes), "outcomes": outcomes}
            write_json(args.data_root, f"coverage/history_{start}_{end}.json", payload)
            publish_status(args.data_root, STATUS, {key: value for key, value in payload.items() if key != "outcomes"})
            print(json.dumps({key: value for key, value in payload.items() if key != "outcomes"}))
            return 0
    except AlreadyRunning:
        print(json.dumps({"state": "SKIPPED", "reason": "ALREADY_RUNNING"}))
        return 0
    except Exception as exc:
        publish_status(args.data_root, STATUS, {"state": "FAILED", "session_date": str(end),
                       "mode": "REPORTING_ONLY", "error_type": type(exc).__name__})
        print(json.dumps({"state": "FAILED", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
