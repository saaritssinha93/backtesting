"""Isolated, read-only broker observer for opening dashboard evidence.

Default is a local plan. --run explicitly enables ONE designated app's full
quote endpoint; this module never creates orders, subscribes sockets, refreshes
credentials, or writes production data. All writes use observer storage.
"""
from __future__ import annotations

import argparse
import calendar
from contextlib import contextmanager
from datetime import date, datetime, time as wall_time, timedelta
import json
import math
import os
from pathlib import Path
import re
import time
from zoneinfo import ZoneInfo

from nse_market_calendar import market_closed_reason
from dashboard_flow_observer_store import append_events, observer_root, write_json

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))
START = wall_time(8, 55)
LATEST_START = wall_time(9, 15)
DEADLINE = wall_time(9, 21)
STATUS = "status/opening_observer.json"
INDICES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYFPI"}
LIVE_OI_TARGET = wall_time(9, 16)
LIVE_OI_CUTOFF = wall_time(9, 20)
LIVE_OI_EXCHANGE_WINDOW_SECONDS = 15
LIVE_OI_MAX_QUOTE_AGE_SECONDS = 15


class AlreadyRunning(RuntimeError):
    pass


def now_ist():
    return datetime.now(IST)


def publish_status(data_root, path, payload):
    current = now_ist().isoformat()
    write_json(data_root, path, {"pid": os.getpid(), "phase": payload.get("state", "UNKNOWN"),
               "status": payload.get("state", "UNKNOWN"), "updated_at_ist": payload.get("updated_at", current),
               "updated_at": current, **payload})


def stamp(value):
    """Kite's naive exchange timestamps are IST; never substitute receipt time."""
    if value is None or value == "":
        return None
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        if result.tzinfo is None:
            result = result.replace(tzinfo=IST)
        return result.astimezone(IST)
    except (TypeError, ValueError):
        return None


def number(value, *, positive=False):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) and (value > 0 if positive else value >= 0) else None
    except (ValueError, TypeError):
        return None


def safe_name(value):
    value = str(value).strip().upper()
    if not re.fullmatch(r"[A-Z0-9&_.-]+", value) or value in {".", ".."}:
        raise ValueError("Invalid instrument identity")
    return value


def expected_near_month_expiry(day):
    """Conservative 2026 stock-futures fallback; dated universes stay authoritative.

    NSE stock futures expire on the last Tuesday, or the preceding trading day:
    https://www.nseindia.com/static/products-services/equity-derivatives-individual-securities
    Do not infer another year's calendar from the reviewed 2026 holiday set.
    """
    if day.year != 2026:
        return None
    def expiry(year, month):
        last = date(year, month, calendar.monthrange(year, month)[1])
        last -= timedelta(days=(last.weekday() - 1) % 7)
        while market_closed_reason(last):
            last -= timedelta(days=1)
        return last
    current = expiry(day.year, day.month)
    if day <= current:
        return current
    if day.month == 12:
        return None
    return expiry(day.year, day.month + 1)


def load_universe(data_root, day):
    """Prefer exact dated membership; registry fallback is explicitly approximate."""
    import pandas as pd
    directory = Path(data_root) / "fno_oi" / "universe"
    path = directory / f"near_month_{day}.parquet"
    source = "DATED_UNIVERSE"
    if path.exists():
        frame = pd.read_parquet(path)
        if "master_date" in frame and set(pd.to_datetime(frame.master_date).dt.date.dropna()) != {day}:
            raise ValueError("Universe session mismatch")
    else:
        path = directory / "contract_registry.parquet"
        if not path.exists():
            return [], "MISSING_UNIVERSE"
        frame = pd.read_parquet(path)
        frame["expiry"] = pd.to_datetime(frame["expiry"], errors="coerce")
        # A surviving next-month contract cannot stand in for a missing expired
        # near-month contract, including the expiry day itself. One expected
        # expiry also prevents future listings leaking into past membership.
        expected = expected_near_month_expiry(day)
        if expected is None:
            return [], "MISSING_DATED_UNIVERSE_UNVERIFIED_EXPIRY_CALENDAR"
        frame = frame.loc[frame.expiry.dt.date.eq(expected)].drop_duplicates("underlying")
        if frame.empty:
            return [], "MISSING_NEAR_MONTH_CONTRACT_IDENTITY"
        source = "RECONSTRUCTED_NEAR_MONTH_REGISTRY"
    result = []
    seen = set()
    def present(row, primary, fallback):
        value = row.get(primary)
        return value if pd.notna(value) and str(value).strip() else row.get(fallback)
    for row in frame.to_dict("records"):
        underlying = str(row.get("underlying", "")).strip().upper()
        if underlying in INDICES or str(row.get("is_index_future", "")).lower() in {"true", "1", "1.0"}:
            continue
        try:
            symbol = safe_name(present(row, "equity_symbol", "underlying"))
            contract = safe_name(present(row, "futures_tradingsymbol", "tradingsymbol"))
            token = int(present(row, "futures_instrument_token", "instrument_token"))
        except (TypeError, ValueError, OverflowError):
            continue
        if symbol in seen or not symbol or token <= 0:
            continue
        seen.add(symbol)
        result.append({"symbol": symbol, "contract": contract, "instrument_token": token,
                       "universe_source": source})
    return sorted(result, key=lambda row: row["symbol"]), source


def make_client(app, credential_dir, *, timeout=6):
    """Read the explicitly selected pair, without credential discovery or logging."""
    if not re.fullmatch(r"app[1-8]", app):
        raise ValueError("App must be app1 through app8")
    suffix = app[3:] if app != "app1" else ""
    values = []
    for stem in ("api_key", "access_token"):
        raw = (Path(credential_dir) / f"{stem}{suffix}.txt").read_text(encoding="utf-8").strip()
        if not raw:
            raise ValueError("Selected credential pair is empty")
        values.append(raw.split()[0])
    from kiteconnect import KiteConnect
    client = KiteConnect(api_key=values[0], timeout=timeout)
    client.set_access_token(values[1])
    return client


@contextmanager
def single_instance(data_root, name):
    """OS-held lock; crashes release it, and no production PID is ever stopped."""
    if not re.fullmatch(r"[a-z_]+", name):
        raise ValueError("Invalid lock name")
    root = observer_root(data_root)
    lock_path = (root / "state" / f"{name}.lock").resolve()
    if not lock_path.is_relative_to(root):
        raise ValueError("Observer lock escapes isolated storage")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if not lock_path.resolve().is_relative_to(root):
        raise ValueError("Observer lock escapes isolated storage")
    with lock_path.open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise AlreadyRunning("Observer job already running") from exc
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise AlreadyRunning("Observer job already running") from exc
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)


def startup_reason(moment):
    moment = moment.astimezone(IST)
    return market_closed_reason(moment.date()) or (
        "BEFORE_OBSERVER_WINDOW" if moment.time() < START else
        "LATE_START_NO_OPENING_BACKDATE" if moment.time() >= LATEST_START else "")


def quote_events(quotes, universe, received_at):
    """Retain actual timestamps; ignore missing, future, or prior-day snapshots."""
    received_at = received_at.astimezone(IST)
    events = []
    for item in universe:
        for kind, key in (("cash", f"NSE:{item['symbol']}"), ("future", f"NFO:{item['contract']}")):
            quote = quotes.get(key) or {}
            exchange_time = stamp(quote.get("timestamp"))
            if not exchange_time or exchange_time.date() != received_at.date() or exchange_time > received_at:
                continue
            price = number(quote.get("last_price"), positive=True)
            last_trade = stamp(quote.get("last_trade_time"))
            # A fresh quote packet may carry yesterday's last traded cash price.
            # Such a price cannot establish a same-session pre-open comparison.
            if kind == "cash" and (last_trade is None or last_trade.date() != received_at.date()
                                   or last_trade > exchange_time):
                price = None
            oi = number(quote.get("oi")) if kind == "future" else None
            if price is None and oi is None:
                continue
            events.append({"session_date": received_at.date().isoformat(), "symbol": item["symbol"],
                           "contract": item["contract"], "kind": kind,
                           "exchange_timestamp": exchange_time.isoformat(), "received_at": received_at.isoformat(),
                           "price": price, "session_open": number((quote.get("ohlc") or {}).get("open"), positive=True),
                           "oi": oi, "source": "KITE_FULL_QUOTE", "last_trade_time": last_trade.isoformat() if last_trade else None,
                           "universe_source": item["universe_source"]})
    return events


def _live_0916_quote(event, item, day):
    """Qualify a near-09:16 *quote*, never a fabricated minute-end candle.

    A quote only establishes its own exchange and receipt times. The exact
    09:15-09:16 candle and a completed 09:20 bar need separate evidence.
    """
    if (event.get("kind") != "future" or event.get("source") != "KITE_FULL_QUOTE"
            or event.get("symbol") != item["symbol"] or event.get("contract") != item["contract"]
            or event.get("session_date") != day):
        return None
    if any(not isinstance(event.get(key), str) or stamp(event[key]) is None
           or datetime.fromisoformat(event[key]).tzinfo is None
           for key in ("exchange_timestamp", "received_at")):
        return None
    oi = number(event.get("oi"), positive=True)
    exchange, received = stamp(event.get("exchange_timestamp")), stamp(event.get("received_at"))
    target = datetime.combine(date.fromisoformat(day), LIVE_OI_TARGET, IST)
    cutoff = datetime.combine(date.fromisoformat(day), LIVE_OI_CUTOFF, IST)
    if (oi is None or exchange is None or received is None or exchange.date().isoformat() != day
            or received.date().isoformat() != day or exchange > received or received >= cutoff):
        return None
    offset = (exchange - target).total_seconds()
    age = (received - exchange).total_seconds()
    if not (0 <= offset <= LIVE_OI_EXCHANGE_WINDOW_SECONDS and 0 <= age <= LIVE_OI_MAX_QUOTE_AGE_SECONDS):
        return None
    return {"oi": oi, "source": "KITE_FULL_QUOTE", "exchange_timestamp": exchange.isoformat(),
            "received_at": received.isoformat(), "seconds_after_0916": offset,
            "seconds_to_0920": (cutoff - exchange).total_seconds(),
            "quote_age_seconds": age, "exact_first_minute_candle": False}


def live_opening_oi_coverage(universe, candidates, moment, *, session_date=None, collection_outcome=None):
    """Expose a complete dated-universe quality inventory for the live quote baseline."""
    day = session_date or moment.astimezone(IST).date().isoformat()
    target = datetime.combine(date.fromisoformat(day), LIVE_OI_TARGET, IST)
    cutoff = datetime.combine(date.fromisoformat(day), LIVE_OI_CUTOFF, IST)
    state = ("FAILED" if collection_outcome == "FAILED" else "FINAL") if collection_outcome else (
        "AWAITING_0916" if moment < target else "COLLECTING" if moment < cutoff else "FINAL")
    missing_status = ("AWAITING_0916" if state == "AWAITING_0916" else
                      "NO_QUALIFIED_QUOTE_YET" if state == "COLLECTING" else
                      "COLLECTOR_FAILED" if state == "FAILED" else "NO_QUALIFIED_QUOTE")
    rows = []
    for item in universe:
        evidence = candidates.get(item["symbol"])
        rows.append({"symbol": item["symbol"], "contract": item["contract"],
                     "instrument_token": item["instrument_token"],
                     "status": "QUOTE_NEAR_0916_NOT_CANDLE" if evidence else missing_status,
                     "baseline_kind": "PROVISIONAL_FULL_QUOTE" if evidence else None,
                     "oi": evidence["oi"] if evidence else None,
                     "source": evidence["source"] if evidence else None,
                     "exchange_timestamp": evidence["exchange_timestamp"] if evidence else None,
                     "received_at": evidence["received_at"] if evidence else None,
                     "seconds_after_0916": evidence["seconds_after_0916"] if evidence else None,
                     "seconds_to_0920": evidence["seconds_to_0920"] if evidence else None,
                     "quote_age_seconds": evidence["quote_age_seconds"] if evidence else None,
                     "exact_first_minute_candle": False,
                     "quote_baseline_available_by_0920": evidence is not None,
                     "missing_reason": None if evidence else (
                         "No fresh same-contract full quote within 15 seconds of 09:16 and received before 09:20."
                         if state == "FINAL" else missing_status)})
    return {"schema": "dashboard_flow_live_opening_oi_v1", "session_date": day,
            "universe_source": "DATED_UNIVERSE", "generated_at": moment.astimezone(IST).isoformat(),
            "state": state, "collection_outcome": collection_outcome,
            "target_ist": "09:16:00", "cutoff_ist": "09:20:00",
            "exchange_window_seconds": LIVE_OI_EXCHANGE_WINDOW_SECONDS,
            "max_quote_age_seconds": LIVE_OI_MAX_QUOTE_AGE_SECONDS,
            "meaning": "Fresh full-quote OI near 09:16, not the exact 09:15-09:16 minute candle; the 09:20 bar must be verified separately.",
            "universe_count": len(rows), "qualified_count": sum(row["quote_baseline_available_by_0920"] for row in rows),
            "missing_count": sum(not row["quote_baseline_available_by_0920"] for row in rows), "rows": rows}


def run_collection(data_root, universe, client, *, app="app1", poll_seconds=5,
                   clock=now_ist, sleep=time.sleep, max_failures=3, reload_client=None):
    started = clock()
    reason = startup_reason(started)
    common = {"session": "dashboard_flow_observer", "session_date": started.date().isoformat(),
              "mode": "REPORTING_ONLY", "broker_mode": "FULL_QUOTE_POLL", "app": app,
              "start": "08:55", "deadline": "09:21", "poll_seconds": max(5, poll_seconds)}
    if reason:
        publish_status(data_root, STATUS, {**common, "state": "SKIPPED", "reason": reason, "updated_at": started.isoformat()})
        return {"state": "SKIPPED", "reason": reason, "events": 0}
    if not universe or any(item["universe_source"] != "DATED_UNIVERSE" for item in universe):
        raise ValueError("Live observer requires the exact dated universe")
    keys = [key for row in universe for key in (f"NSE:{row['symbol']}", f"NFO:{row['contract']}")]
    if len(keys) > 500:
        raise ValueError("Universe exceeds the single conservative full-quote request limit")
    failures = total = polls = 0
    last_error = None
    live_candidates = {}
    universe_by_symbol = {item["symbol"]: item for item in universe}
    session_date = started.date().isoformat()
    coverage_path = f"coverage/live_opening_oi_{started.date()}.json"
    coverage_state = None
    write_json(data_root, coverage_path, live_opening_oi_coverage(
        universe, live_candidates, started, session_date=session_date))
    coverage_state = "AWAITING_0916" if started.time() < LIVE_OI_TARGET else "COLLECTING"
    while True:
        moment = clock()
        if moment.date() != started.date() or moment.time() >= DEADLINE:
            state = "COMPLETE"
            break
        current_coverage_state = "AWAITING_0916" if moment.time() < LIVE_OI_TARGET else (
            "COLLECTING" if moment.time() < LIVE_OI_CUTOFF else "FINAL")
        if current_coverage_state != coverage_state:
            write_json(data_root, coverage_path, live_opening_oi_coverage(
                universe, live_candidates, moment, session_date=session_date))
            coverage_state = current_coverage_state
        try:
            quotes = client.quote(keys)
            received = clock()
            if received.date() != started.date() or received.time() > DEADLINE:
                state = "COMPLETE"
                break
            events = quote_events(quotes, universe, received)
            append_events(data_root, started.date().isoformat(), events)
            improved = False
            if received.time() < LIVE_OI_CUTOFF:
                for event in events:
                    item = universe_by_symbol.get(event["symbol"])
                    if item is None:
                        continue
                    evidence = _live_0916_quote(event, item, started.date().isoformat())
                    if evidence is None:
                        continue
                    previous = live_candidates.get(item["symbol"])
                    if previous is None or (evidence["seconds_after_0916"], evidence["received_at"]) < (
                            previous["seconds_after_0916"], previous["received_at"]):
                        live_candidates[item["symbol"]] = evidence
                        improved = True
            if improved:
                write_json(data_root, coverage_path, live_opening_oi_coverage(
                    universe, live_candidates, received, session_date=session_date))
            total += len(events)
            polls += 1
            failures = 0
            last_error = None
        except Exception as exc:
            failures += 1
            # Exception messages may contain request URLs or broker account data.
            last_error = type(exc).__name__
            if last_error in {"TokenException", "PermissionException"} and reload_client and clock().time() < wall_time(9, 14):
                publish_status(data_root, STATUS, {**common, "state": "WAITING_AUTH", "events": total,
                    "polls": polls, "error_type": last_error, "retry_seconds": 30,
                    "updated_at": clock().isoformat()})
                sleep(30)
                try:
                    client = reload_client()
                except (OSError, ValueError):
                    pass
                failures = 0
                continue
            if failures >= max_failures:
                state = "FAILED"
                break
        publish_status(data_root, STATUS, {**common, "state": "RUNNING", "events": total,
                                     "polls": polls, "failures": failures, "error_type": last_error,
                                     "updated_at": clock().isoformat()})
        remaining = (datetime.combine(started.date(), DEADLINE, IST) - clock()).total_seconds()
        if remaining > 0:
            sleep(min(max(5, poll_seconds) * (failures + 1), remaining))
    outcome = {**common, "state": state, "events": total, "polls": polls,
               "error_type": last_error, "updated_at": clock().isoformat()}
    write_json(data_root, coverage_path, live_opening_oi_coverage(
        universe, live_candidates, clock(), session_date=session_date, collection_outcome=state))
    publish_status(data_root, STATUS, outcome)
    return outcome


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--app", default="app1")
    parser.add_argument("--credential-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--poll-seconds", type=float, default=5)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--scheduled", action="store_true")
    args = parser.parse_args(argv)
    moment = now_ist()
    if not args.run:
        print(json.dumps({"mode": "PLAN_ONLY", "app": args.app, "broker_mode": "FULL_QUOTE_POLL",
                          "window": "08:55-09:21 IST", "startup_cutoff": "09:15 IST",
                          "output": str(observer_root(args.data_root)), "current_guard": startup_reason(moment)}))
        return 0
    try:
        with single_instance(args.data_root, "opening_observer"):
            reason = startup_reason(moment)
            if reason:
                publish_status(args.data_root, STATUS, {"state": "SKIPPED", "reason": reason,
                           "session_date": str(moment.date()), "updated_at": moment.isoformat(), "mode": "REPORTING_ONLY"})
                print(json.dumps({"state": "SKIPPED", "reason": reason}))
                return 0
            while True:
                try:
                    universe, source = load_universe(args.data_root, moment.date())
                    if source != "DATED_UNIVERSE" or not universe:
                        raise ValueError("Exact dated universe unavailable")
                    client = make_client(args.app, args.credential_dir)
                    break
                except (OSError, ValueError):
                    if not args.scheduled or now_ist().time() >= wall_time(9, 14):
                        raise
                    publish_status(args.data_root, STATUS, {"state": "WAITING_INPUTS", "session_date": str(moment.date()),
                        "mode": "REPORTING_ONLY", "reason": "Awaiting dated universe and existing authentication task",
                        "retry_seconds": 30})
                    time.sleep(30)
            write_json(args.data_root, f"universe/{moment.date()}.json", {
                "session_date": str(moment.date()), "source": source, "rows": universe})
            result = run_collection(args.data_root, universe, client, app=args.app, poll_seconds=args.poll_seconds,
                reload_client=(lambda: make_client(args.app, args.credential_dir)) if args.scheduled else None)
            print(json.dumps(result))
            return 1 if result["state"] == "FAILED" else 0
    except AlreadyRunning:
        print(json.dumps({"state": "SKIPPED", "reason": "ALREADY_RUNNING"}))
        return 0
    except Exception as exc:
        publish_status(args.data_root, STATUS, {"state": "FAILED", "error_type": type(exc).__name__,
                   "session_date": str(moment.date()), "updated_at": now_ist().isoformat(), "mode": "REPORTING_ONLY"})
        print(json.dumps({"state": "FAILED", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
