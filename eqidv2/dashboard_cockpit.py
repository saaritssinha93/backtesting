"""Read-only active-strategy cockpit projections from bounded local evidence.

No trading runtime is imported. Snapshots are recovery observations, events are
history, and sparse completed bars are never interpolated into market activity.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from contextvars import ContextVar
from datetime import date, datetime, timedelta
from itertools import islice
from pathlib import Path
from typing import Any

from fno_eq_id_monitor_detail import ACTIVE_PROFILE, IST, STRATEGY_VERSION, build_monitor_detail
from fno_strategy_registry import DisplayIdentity, display_identity, expected_fingerprint
from fno_v13_v10_g_3_live_execution_policy import (
    execution_metadata as g3_execution_metadata,
    validate_existing_order_sizing as validate_g3_order_sizing,
    validate_existing_execution_metadata as validate_g3_execution_metadata,
)

MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_EVENT_BYTES = 16 * 1024 * 1024
MAX_ORDERS = 500
MAX_EVENTS = 2000
MAX_EVENT_SCAN = 10000
MAX_CANDLES = 1000
FRESH_SECONDS = 120
SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9.&_-]{0,39}\Z")
OPEN_STATES = {"OPEN", "SQUARE_OFF_PENDING"}
KNOWN_STATES = OPEN_STATES | {"PENDING_ENTRY", "CLOSED", "NO_FILL", "ENTRY_REJECTED", "BLOCKED_SIZING", "BLOCKED_PORTFOLIO", "CANCELLED", "FAILED"}
_REQUEST_FINGERPRINT: ContextVar[DisplayIdentity | None] = ContextVar("cockpit_request_fingerprint", default=None)


def _request_version() -> str:
    identity = _REQUEST_FINGERPRINT.get()
    return identity.version if identity is not None and identity.profile == ACTIVE_PROFILE.name else STRATEGY_VERSION


def _active_fingerprint_valid(record: dict) -> bool:
    value = record.get("strategy_fingerprint")
    if not isinstance(value, str) or not 0 < len(value) <= 128:
        return False
    if ACTIVE_PROFILE.name != "V13_V10_G_3":
        return True
    cached = _REQUEST_FINGERPRINT.get()
    expected = cached.fingerprint if cached is not None and cached.profile == ACTIVE_PROFILE.name else expected_fingerprint(ACTIVE_PROFILE.name)
    return value == expected


def _text(value: Any, limit: int = 250) -> str:
    return str(value)[:limit] if isinstance(value, (str, int, float)) else ""


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _stamp(value: Any, *, allow_naive: bool = False) -> datetime | None:
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if result.tzinfo is None:
            if not allow_naive:
                return None
            result = result.replace(tzinfo=IST)
        return result.astimezone(IST)
    except (ValueError, TypeError, OverflowError):
        return None


def _contained(path: Path, root: Path) -> bool:
    try:
        return path.resolve().is_relative_to(root.resolve())
    except (OSError, RuntimeError):
        return False


def _read(path: Path, root: Path, warnings: list[str]) -> dict:
    if not path.is_file():
        return {}
    try:
        if not _contained(path, root) or path.stat().st_size > MAX_JSON_BYTES:
            raise ValueError("outside root or oversized")
        with path.open("rb") as handle:
            raw = handle.read(MAX_JSON_BYTES + 1)
        if len(raw) > MAX_JSON_BYTES:
            raise ValueError("oversized")
        value = json.loads(raw.decode("utf-8-sig"))
        if not isinstance(value, dict):
            raise ValueError("not an object")
        return value
    except (OSError, UnicodeError, ValueError, RecursionError):
        warnings.append(f"{path.name}: unreadable or invalid evidence")
        return {}


def _files(directory: Path, suffix: str, maximum: int, warnings: list[str]) -> list[Path]:
    try:
        paths = list(islice(directory.glob(f"*{suffix}"), maximum + 1))
    except OSError:
        warnings.append(f"{directory.name}: evidence directory unreadable")
        return []
    if len(paths) > maximum:
        warnings.append(f"{directory.name}: evidence truncated at {maximum} files")
    return sorted(paths[:maximum])


def _order_rows(root: Path, day: str, mode: str, now: datetime, warnings: list[str]) -> tuple[list[dict], bool]:
    directories = [root / "orders" / mode / day]
    if mode == "LIVE":
        directories.append(root / "orders" / mode / "live_kite_qty1" / day)
    rows: dict[str, dict] = {}
    incomplete = False
    for directory in directories:
        before = len(warnings)
        paths = _files(directory, ".json", MAX_ORDERS, warnings)
        incomplete |= len(warnings) > before
        for path in paths:
            raw = _read(path, root, warnings)
            stamp = _stamp(raw.get("updated_at_ist"))
            symbol = _text(raw.get("tradingsymbol"), 40)
            signal_id = _text(raw.get("signal_id"), 160)
            valid = (raw.get("mode") == mode and raw.get("session_date") == day
                     and raw.get("strategy_version") == _request_version()
                     and _active_fingerprint_valid(raw)
                     and bool(signal_id) and bool(SYMBOL.fullmatch(symbol))
                     and stamp is not None and stamp.date().isoformat() == day
                     and stamp <= now + timedelta(seconds=5)
                     and isinstance(raw.get("status"), str) and raw["status"] in KNOWN_STATES
                     and isinstance(raw.get("side"), str) and raw["side"] in {"LONG", "SHORT"})
            quantity = _number(raw.get("quantity"))
            if mode == "LIVE" and ACTIVE_PROFILE.name == "V13_V10_G_3":
                try:
                    validate_g3_order_sizing(raw)
                except (ValueError, TypeError):
                    warnings.append(f"{path.name}: G3 LIVE execution policy or sizing mismatch; omitted")
                    valid = False
            if isinstance(raw.get("status"), str) and raw["status"] in OPEN_STATES | {"CLOSED"} and (quantity is None or quantity <= 0 or not quantity.is_integer()):
                valid = False
            if not valid:
                warnings.append(f"{path.name}: invalid mode, identity, state or timestamp; omitted")
                incomplete = True
                continue
            row = {"raw": raw, "source": path.relative_to(root).as_posix(), "stamp": stamp, "conflict": False}
            if signal_id in rows:
                existing = rows[signal_id]
                if existing["raw"] != raw:
                    warnings.append(f"{symbol}: conflicting snapshots for one signal; P&L unavailable")
                    incomplete = True
                    row["conflict"] = True
                    if existing["stamp"] > stamp:
                        existing["conflict"] = True
                        continue
                else:
                    row["conflict"] = existing["conflict"]
            rows[signal_id] = row
    return list(rows.values()), incomplete


def _project_order(item: dict, day: str, mode: str, now: datetime) -> dict:
    raw, stamp = item["raw"], item["stamp"]
    state = ("CONFLICT" if item["conflict"] else "HISTORICAL" if day != now.date().isoformat()
             else "RECENT_SNAPSHOT" if (now - stamp).total_seconds() <= FRESH_SECONDS else "STALE")
    row = {name: _number(raw.get(name)) for name in ("quantity", "trigger_price", "entry_price", "last_price", "exit_price", "stop_price", "target_price", "stop_pct", "active_stop_pct", "target_pct", "capital_rs", "estimated_cost_rs")}
    row.update({name: _text(raw.get(name)) for name in ("signal_id", "side", "status", "entry_at_ist", "exit_at_ist", "stop_tighten_due_at_ist", "stop_tighten_status", "entry_activation_deadline_ist", "setup_id", "quantity_policy", "execution_profile", "strategy_fingerprint")})
    if mode == "LIVE" and ACTIVE_PROFILE.name == "V13_V10_G_3":
        row["live_execution_policy"] = {key: raw.get(key) for key in g3_execution_metadata()}
        row.update({key: _number(raw.get(key)) for key in ("sizing_reference_price", "strategy_sized_quantity", "requested_quantity", "execution_quantity_override")})
    row.update(id=f"{mode}:{raw['signal_id']}", symbol=raw["tradingsymbol"], mode=mode,
               reason=_text(raw.get("status_reason")), source=item["source"], evidence_state=state,
               updated_at_ist=stamp.isoformat(), recorded_net_pnl_rs=_number(raw.get("net_pnl_rs")),
               realized_pnl_rs=_number(raw.get("net_pnl_rs")) if raw["status"] == "CLOSED" else None,
               unrealized_pnl_rs=None, quote_at_ist=None, quote_state="NO_TIMESTAMPED_QUOTE",
               first_blocker=_text(raw.get("first_entry_blocker_reason")),
               last_blocker=_text(raw.get("last_entry_blocker_reason")),
               terminal_cause=_text(raw.get("entry_terminal_cause")),
               execution_error_count=_number(raw.get("execution_error_count")),
               paper_portfolio_admission={})
    admission = raw.get("paper_portfolio_admission")
    if isinstance(admission, dict):
        row["paper_portfolio_admission"] = {name: _number(admission.get(name)) for name in ("available_capital_rs", "reserved_capital_rs", "required_capital_rs", "portfolio_capital_rs")}
        row["paper_portfolio_admission"].update(allowed=admission.get("allowed") if isinstance(admission.get("allowed"), bool) else None, reason=_text(admission.get("reason")))
    partial_exit = mode == "LIVE" and bool(raw.get("g3_exit_fills_by_order"))
    if partial_exit:
        ledger = raw["g3_exit_fills_by_order"]
        quantities = [_number(fill.get("quantity")) if isinstance(fill, dict) else None for fill in ledger.values()] if isinstance(ledger, dict) else [None]
        closed_quantity = sum(quantities) if all(q is not None and q >= 0 and q.is_integer() for q in quantities) else None
        row["remaining_quantity"] = (row["quantity"] - closed_quantity if closed_quantity is not None and row["quantity"] is not None and closed_quantity <= row["quantity"] else None)
        row["partial_exit_pnl_note"] = "Partial exit fills recorded; combined realized/unrealized P&L is not inferred by this view."
    # Legacy updated_at_ist can advance after a failed quote request. Never use
    # it as proof that last_price is current, especially for LIVE exit managers.
    quote = _stamp(raw.get("last_price_at_ist") or raw.get("quote_at_ist") or raw.get("quote_timestamp_ist"))
    if quote is not None and quote.date().isoformat() == day and quote <= now + timedelta(seconds=5):
        row["quote_at_ist"] = quote.isoformat()
        row["quote_state"] = "RECENT" if day == now.date().isoformat() and (now - quote).total_seconds() <= FRESH_SECONDS else "STALE"
        prices = [row["entry_price"], row["last_price"], row["quantity"]]
        if raw["status"] in OPEN_STATES and row["quote_state"] == "RECENT" and not item["conflict"] and not partial_exit and all(v is not None and v > 0 for v in prices):
            gross = (prices[1] - prices[0]) * prices[2] * (1 if raw["side"] == "LONG" else -1)
            costs = row["estimated_cost_rs"]
            row["unrealized_pnl_rs"] = round(gross - costs, 2) if costs is not None and costs >= 0 else None
    return row


def _events(root: Path, day: str, mode: str, now: datetime, warnings: list[str]) -> list[dict]:
    path = root / "order_events" / mode / f"{day}.jsonl"
    if not path.is_file():
        return []
    result, seen = [], set()
    try:
        if not _contained(path, root):
            raise ValueError("event archive outside configured root")
        with path.open("rb") as handle:
            handle.seek(0, 2)
            offset = max(0, handle.tell() - MAX_EVENT_BYTES)
            handle.seek(offset)
            raw_file = handle.read(MAX_EVENT_BYTES)
        if offset:
            # The tail may begin inside a UTF-8 codepoint or JSON record. Drop
            # that partial line before decoding; never read the whole archive.
            raw_file = raw_file.partition(b"\n")[2]
            warnings.append("Event stream uses a bounded archive tail; earlier events are omitted and journal-chain integrity is not verified here")
        lines = raw_file.decode("utf-8-sig").splitlines()
        if len(lines) > MAX_EVENT_SCAN:
            warnings.append("Event archive scan uses the latest recorded lines; earlier events are omitted")
        for index, line in enumerate(lines[-MAX_EVENT_SCAN:]):
            try:
                event = json.loads(line)
                context, data = event.get("context", {}), event.get("data", {})
                stamp = _stamp(event.get("timestamp_utc"))
                if not isinstance(context, dict) or not isinstance(data, dict):
                    raise ValueError("event object")
                if (context.get("mode", "").upper() != mode or context.get("session_date") != day
                        or context.get("strategy_version") != _request_version()
                        or not _active_fingerprint_valid(context)
                        or stamp is None or stamp.date().isoformat() != day or stamp > now + timedelta(seconds=5)):
                    raise ValueError("event identity/time")
                if mode == "LIVE" and ACTIVE_PROFILE.name == "V13_V10_G_3":
                    try:
                        validate_g3_execution_metadata(data)
                    except (ValueError, TypeError):
                        warnings.append(f"LIVE event {index + 1}: unverified G3 execution policy; omitted")
                        continue
                symbol = _text(data.get("tradingsymbol"), 40)
                if not SYMBOL.fullmatch(symbol):
                    raise ValueError("event symbol")
                digest = hashlib.sha256(line.encode()).hexdigest()[:20]
                if digest in seen:
                    continue
                seen.add(digest)
                result.append(dict(id=digest, timestamp_ist=stamp.isoformat(), mode=mode, symbol=symbol,
                                   signal_id=_text(context.get("signal_id")), side=_text(data.get("side")),
                                   event_type=_text(event.get("event_type")), state_before=_text(data.get("state_before")),
                                   state_after=_text(data.get("state_after")), reason=_text(data.get("reason")),
                                   quantity=_number(data.get("quantity")), net_pnl_rs=_number(data.get("net_pnl_rs")),
                                   source=path.relative_to(root).as_posix()))
            except (ValueError, TypeError, AttributeError, RecursionError):
                warnings.append(f"{mode} event {index + 1}: invalid evidence omitted")
    except (OSError, UnicodeError, ValueError):
        warnings.append("Order event archive unavailable or too large to read safely")
    result.sort(key=lambda row: row["timestamp_ist"])
    if len(result) > MAX_EVENTS:
        warnings.append(f"Only the latest {MAX_EVENTS} recorded events are displayed")
    return result[-MAX_EVENTS:]


def _chart(fno_root: Path, day: str, symbol: str | None, orders: list[dict], now: datetime, warnings: list[str]) -> dict:
    chart = dict(symbol=symbol, kind="observations", state="NO_EVIDENCE", candles=[], observations=[],
                 interval="1m", source=None, note="Recorded observations only; no price interpolation.")
    if symbol is None:
        return chart
    observations = {}
    for row in orders:
        if row["symbol"] != symbol:
            continue
        for time_key, price_key, label in (("entry_at_ist", "entry_price", "Entry fill"), ("exit_at_ist", "exit_price", "Exit fill"), ("quote_at_ist", "last_price", "Timestamped quote")):
            stamp, price = _stamp(row.get(time_key)), _number(row.get(price_key))
            if stamp and stamp.date().isoformat() == day and stamp <= now + timedelta(seconds=5) and price is not None and price > 0:
                observations[(stamp.isoformat(), price, label)] = dict(timestamp_ist=stamp.isoformat(), price=price, label=label)
    chart["observations"] = sorted(observations.values(), key=lambda row: row["timestamp_ist"])
    if observations:
        chart["state"] = "RECORDED_OBSERVATIONS"
    stem = re.sub(r"[^A-Z0-9._-]+", "_", symbol).strip("._") or "CONTRACT"
    if stem != symbol:
        stem += "_" + hashlib.sha1(symbol.encode()).hexdigest()[:8]
    path = fno_root / "raw_equity_1m" / day / f"{stem}_1minute.parquet"
    if not path.is_file():
        return chart
    try:
        if not _contained(path, fno_root) or path.stat().st_size > MAX_EVENT_BYTES:
            raise ValueError("candle source exceeds safe read limit")
        import pyarrow.parquet as pq

        source = pq.ParquetFile(path)
        if source.metadata.num_rows > 5000:
            raise ValueError("candle row limit")
        required = {"timestamp", "open", "high", "low", "close"}
        if not required.issubset(source.schema.names):
            raise ValueError("candle schema")
        columns = sorted((required | {"volume", "tradingsymbol", "exchange"}) & set(source.schema.names))
        rows = source.read(columns=columns).to_pylist()
        candles = {}
        invalid = 0
        for row in rows:
            stamp = _stamp(row.get("timestamp"), allow_naive=True)
            values = {key: _number(row.get(key)) for key in ("open", "high", "low", "close", "volume")}
            prices = [values[key] for key in ("open", "high", "low", "close")]
            if (stamp is None or stamp.date().isoformat() != day or stamp > now
                    or row.get("tradingsymbol", symbol) != symbol or row.get("exchange", "NSE") != "NSE"
                    or not all(value is not None and value > 0 for value in prices)
                    or not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]
                    or (values["volume"] is not None and values["volume"] < 0)):
                invalid += 1
                continue
            key = stamp.isoformat()
            candidate = dict(timestamp_ist=key, **values)
            if key in candles and candles[key] != candidate:
                raise ValueError("conflicting duplicate candle")
            candles[key] = candidate
        if invalid:
            warnings.append(f"{symbol}: {invalid} invalid/wrong-session/future candle rows omitted")
        if len(candles) > MAX_CANDLES:
            raise ValueError("candle display limit")
        chart.update(candles=[candles[key] for key in sorted(candles)], source=path.relative_to(fno_root).as_posix())
        if candles:
            chart.update(kind="candles", state="RECORDED_CANDLES",
                         note="Actual locally persisted completed 1m candles. Candidate-scoped coverage can have gaps; gaps are not filled.")
    except (ImportError, OSError, ValueError, TypeError, KeyError):
        warnings.append(f"{symbol}: local candle history unavailable or invalid; showing only recorded price observations")
    return chart


def _available_dates(root: Path, mode: str, day: str) -> list[str]:
    result = {day}
    directories = [root / "scanner_5m", root / "confirmation_1m", root / "orders" / mode]
    if mode == "LIVE":
        directories.append(root / "orders" / mode / "live_kite_qty1")
    for directory in directories:
        try:
            for path in islice(directory.iterdir(), 1500):
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.name) and path.is_dir():
                    date.fromisoformat(path.name)
                    result.add(path.name)
        except (OSError, ValueError):
            continue
    return sorted(result, reverse=True)[:366]


def _comparison(items: list[dict], other_items: list[dict], mode: str, warnings: list[str]) -> list[dict]:
    """Join immutable signal identities without equating quantities or fill times."""
    books = {mode: {item["raw"]["signal_id"]: item for item in items},
             "LIVE" if mode == "PAPER" else "PAPER": {item["raw"]["signal_id"]: item for item in other_items}}
    identities = sorted(books["PAPER"].keys() | books["LIVE"].keys())
    if len(identities) > MAX_ORDERS:
        warnings.append(f"PAPER/LIVE comparison truncated at {MAX_ORDERS} signal identities")

    def leg(item: dict | None) -> dict:
        if item is None:
            return dict(status=None, entry=None, exit=None, qty=None, return_pct=None)
        raw = item["raw"]
        result = dict(status=_text(raw.get("status")), entry=None, exit=None,
                      qty=_number(raw.get("quantity")), return_pct=None)
        if item["conflict"]:
            return result
        entry, exit_price = _number(raw.get("entry_price")), _number(raw.get("exit_price"))
        entered, exited = _stamp(raw.get("entry_at_ist")), _stamp(raw.get("exit_at_ist"))
        if (raw["status"] in OPEN_STATES | {"CLOSED"} and entry is not None and entry > 0
                and entered is not None and entered.date().isoformat() == raw["session_date"]
                and entered <= item["stamp"] + timedelta(seconds=5)):
            result["entry"] = entry
            if (raw["status"] == "CLOSED" and exit_price is not None and exit_price > 0
                    and exited is not None and exited.date().isoformat() == raw["session_date"]
                    and entered <= exited <= item["stamp"] + timedelta(seconds=5)):
                result["exit"] = exit_price
                result["return_pct"] = round((exit_price / entry - 1) * (1 if raw["side"] == "LONG" else -1) * 100, 6)
        return result

    result = []
    for signal_id in identities[:MAX_ORDERS]:
        paper_item, live_item = books["PAPER"].get(signal_id), books["LIVE"].get(signal_id)
        paper, live = leg(paper_item), leg(live_item)
        reference = (paper_item or live_item)["raw"]
        conflict = bool((paper_item and paper_item["conflict"]) or (live_item and live_item["conflict"]))
        mismatch = bool(paper_item and live_item and any(paper_item["raw"].get(key) != live_item["raw"].get(key)
                        for key in ("tradingsymbol", "side", "strategy_fingerprint")))
        pair_state = ("CONFLICT" if conflict else "IDENTITY_MISMATCH" if mismatch
                      else "MISSING_PAPER" if paper_item is None else "MISSING_LIVE" if live_item is None
                      else "PAIRED" if paper["return_pct"] is not None and live["return_pct"] is not None
                      else "INCOMPLETE_FILLS")
        if mismatch or conflict:
            for value in (paper, live):
                value.update(entry=None, exit=None, return_pct=None)
        difference = round((live["entry"] - paper["entry"]) / paper["entry"] * 10000, 4) if paper["entry"] is not None and live["entry"] is not None else None
        row = dict(signal_id=signal_id, symbol=reference["tradingsymbol"], side=reference["side"],
                   state=pair_state, entry_difference_bps=difference,
                   note="Directional gross price returns exclude quantity and costs. Entry difference is raw LIVE minus PAPER relative to PAPER entry; differing fill times prevent treating it as slippage.")
        row.update({f"paper_{key}": value for key, value in paper.items()})
        row.update({f"live_{key}": value for key, value in live.items()})
        result.append(row)
    return result


def load_cockpit_data(fno_root: Path | str, session_date: str, mode: str = "PAPER", symbol: str | None = None, *, now_ist: datetime | None = None) -> dict[str, Any]:
    """Return one mode's real observations. Invalid query arguments raise ValueError."""
    if not isinstance(session_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", session_date):
        raise ValueError("session_date must be YYYY-MM-DD")
    session_day = date.fromisoformat(session_date)
    if mode not in {"PAPER", "LIVE"}:
        raise ValueError("mode must be PAPER or LIVE")
    if symbol is not None and (not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol)):
        raise ValueError("symbol must be a valid uppercase NSE symbol")
    now = now_ist or datetime.now(IST)
    if now.tzinfo is None:
        raise ValueError("now_ist must be timezone-aware")
    now = now.astimezone(IST)
    # Each HTTP request gets its own strategy identity. Event archives can hold
    # thousands of records; hashing the contract for every record stalls the UI.
    expected = expected_fingerprint(ACTIVE_PROFILE.name) if ACTIVE_PROFILE.name == "V13_V10_G_3" else ""
    selected_identity = display_identity(session_date, ACTIVE_PROFILE.name, current_fingerprint=expected)
    token = _REQUEST_FINGERPRINT.set(selected_identity)
    try:
        return _load_cockpit_data(fno_root, session_date, session_day, mode, symbol, now)
    finally:
        _REQUEST_FINGERPRINT.reset(token)


def _load_cockpit_data(fno_root: Path | str, session_date: str, session_day: date, mode: str, symbol: str | None, now: datetime) -> dict[str, Any]:
    clock_phase = ("WEEKEND" if session_day.weekday() >= 5 else "HISTORICAL_SESSION" if session_day < now.date()
                   else "FUTURE_SESSION" if session_day > now.date() else "PREOPEN" if now.strftime("%H:%M") < "09:15"
                   else "AFTER_SESSION" if now.strftime("%H:%M") >= "15:30" else "IN_SESSION_WINDOW")
    fno_root = Path(fno_root)
    root = fno_root / ACTIVE_PROFILE.live_dir
    warnings: list[str] = []
    # Capture moving worker snapshots against the request clock before the
    # slower full-universe detail scan; later writes are not future evidence.
    items, incomplete = _order_rows(root, session_date, mode, now, warnings)
    other_mode = "LIVE" if mode == "PAPER" else "PAPER"
    other_items, comparison_incomplete = _order_rows(root, session_date, other_mode, now, warnings)
    comparison_incomplete |= len({item["raw"]["signal_id"] for item in [*items, *other_items]}) > MAX_ORDERS
    comparison = _comparison(items, other_items, mode, warnings)
    orders = sorted((_project_order(item, session_date, mode, now) for item in items), key=lambda row: (row["updated_at_ist"], row["id"]))
    events = _events(root, session_date, mode, now, warnings)
    detail = build_monitor_detail(fno_root, session_date, now_ist=now)
    # The detail reader also inspects order events for its own screen. Our
    # selected-mode bounded tail reader is the authority for this stream.
    warnings.extend(w for w in detail["warnings"] if "order events exceed safe read limit" not in w)
    manifest = _read(root / "strategy_manifest.json", root, warnings)
    if (manifest.get("strategy_version") != _request_version()
            or not _active_fingerprint_valid(manifest)):
        manifest = {}
    fingerprints = {item["raw"]["strategy_fingerprint"] for item in items}
    current_mismatch = bool(session_date == now.date().isoformat() and fingerprints and manifest.get("strategy_fingerprint") and fingerprints != {manifest["strategy_fingerprint"]})
    incompatible = len(fingerprints) > 1 or current_mismatch
    if incompatible:
        warnings.append("Order fingerprints disagree with each other or the current manifest; aggregate P&L withheld")
        for row in comparison:
            row.update(state="IDENTITY_MISMATCH", paper_return_pct=None, live_return_pct=None, entry_difference_bps=None)
    policy = manifest.get("scheduled_promotion")
    policy = policy if isinstance(policy, dict) and _text(policy.get("effective_from")) <= session_date else {}
    policy_fields = {name: policy.get(name) for name in ("revision", "effective_from", "initial_stop_pct", "tightened_stop_pct", "tighten_after_minutes", "relaxed_0925_long") if isinstance(policy.get(name), (str, int, float, bool))}
    identity = dict(profile=ACTIVE_PROFILE.name, label=ACTIVE_PROFILE.label, version=_request_version(),
                    fingerprint=next(iter(fingerprints)) if len(fingerprints) == 1 else manifest.get("strategy_fingerprint"),
                    state="MISMATCH" if incompatible else "RECORDED" if fingerprints else "CONFIGURED" if manifest else "NO_EVIDENCE", policy=policy_fields)
    if ACTIVE_PROFILE.name == "V13_V10_G_3":
        identity["configured_live_execution_policy"] = g3_execution_metadata()
        selected_identity = _REQUEST_FINGERPRINT.get()
        if selected_identity is not None:
            identity["display_reference"] = selected_identity.metadata()
    phase_5m = detail["rows_5m"]
    phase_1m = [row for row in detail["rows_1m"] if not row["stage"].endswith(("ORDER_SNAPSHOT", "ORDER_EVENT"))]
    symbols = sorted({row["symbol"] for row in [*orders, *phase_5m, *phase_1m] if SYMBOL.fullmatch(row.get("symbol", ""))})
    selected = symbol or (orders[0]["symbol"] if orders else symbols[0] if symbols else None)
    positions = [row for row in orders if row["status"] in OPEN_STATES]
    closed = [row for row in orders if row["status"] == "CLOSED"]
    aggregate_valid = bool(orders) and not incomplete and not incompatible
    def total(rows: list[dict], key: str) -> float | None:
        return round(sum(row[key] for row in rows), 2) if aggregate_valid and all(row[key] is not None for row in rows) else None
    realized, unrealized = total(closed, "realized_pnl_rs"), total(positions, "unrealized_pnl_rs")
    selected_rows = [row for row in phase_1m if row.get("decision") == "SELECTED" and row.get("evidence_state") == "RECORDED"]
    recorded_pending = sum(row["status"] == "PENDING_ENTRY" for row in orders)
    summary = dict(signals=len(selected_rows), orders=len(orders), pending=recorded_pending if aggregate_valid else None,
                   open=len(positions) if aggregate_valid else None, closed=len(closed) if aggregate_valid else None,
                   recorded_counts=dict(orders=len(orders), pending=recorded_pending, open=len(positions), closed=len(closed)),
                   realized_pnl_rs=realized, unrealized_pnl_rs=unrealized,
                   total_pnl_rs=round(realized + unrealized, 2) if realized is not None and unrealized is not None else None,
                   win_rate_pct=round(100 * sum(row["realized_pnl_rs"] > 0 for row in closed) / len(closed), 2) if closed and realized is not None else None,
                   pnl_state="RECORDED" if realized is not None and unrealized is not None else "INCOMPLETE",
                   scope="Selected mode's available order snapshots; not a broker account balance or a completeness attestation.",
                   costs="Recorded modeled costs; not independently verified broker charges.")
    coverage = detail["coverage"]
    blocked = [row for row in orders if row["status"].startswith("BLOCKED") or row["last_blocker"]]
    stages = [
        dict(key="data", label="Market data", status=detail["state"], count=len(phase_5m), detail="Recorded equity prices/volume and futures OI; inspect coverage for missing inputs."),
        dict(key="scanner", label="5m scanner", status="RECORDED" if phase_5m else "NO_EVIDENCE", count=sum(row.get("decision") == "BASE_PASS" for row in phase_5m), detail="Recorded base-gate candidates; no live rules are re-executed here."),
        dict(key="confirmation", label="1m confirmation", status="RECORDED" if phase_1m else "NO_EVIDENCE", count=len(selected_rows), detail="Selected signals after recorded confirmation and setup gates."),
        dict(key="risk", label="Entry checks", status="BLOCKED" if blocked else "RECORDED" if orders else "NO_EVIDENCE", count=len(blocked), detail="Recorded sizing/capital/blocker evidence. A missing blocker does not prove all entry guards passed."),
        dict(key="execution", label=f"{mode} execution", status="MISMATCH" if incompatible else "RECORDED" if orders else "NO_EVIDENCE", count=len(positions), detail="Open order snapshots, recorded fills and execution errors; not broker-truth attestation."),
        dict(key="exits", label="Stop / target / exit", status="RECORDED" if orders else "NO_EVIDENCE", count=len(closed), detail="Recorded stop levels, tightening deadlines and closed outcomes."),
    ]
    observed_times = [_stamp(row["updated_at_ist"]) for row in orders]
    latest = max((stamp for stamp in observed_times if stamp is not None), default=None)
    has_evidence = bool(orders or phase_5m or phase_1m or events)
    evidence_state = ("NO_EVIDENCE" if not has_evidence else "MISMATCH" if incompatible else "HISTORICAL" if session_date != now.date().isoformat()
                      else "RECENT_SNAPSHOTS" if latest and (now - latest).total_seconds() <= FRESH_SECONDS else "STALE_OR_PARTIAL")
    if positions and unrealized is None:
        warnings.append("Open-position P&L is unknown without a recent timestamped quote and modeled costs. Recorded snapshot P&L is shown separately.")
    if any(row.get("partial_exit_pnl_note") for row in positions):
        warnings.append("Partial LIVE exits are being reconciled. Remaining shares are shown; combined P&L remains unavailable until the filled position closes.")
    chart = _chart(fno_root, session_date, selected, orders, now, warnings)
    evidence_rows_5m = [row for row in phase_5m if selected is None or row["symbol"] == selected]
    evidence_rows_1m = [row for row in phase_1m if selected is None or row["symbol"] == selected]
    if len(evidence_rows_5m) > 250 or len(evidence_rows_1m) > 250:
        warnings.append("Selected-stock evidence rows truncated at 250 per phase")
    sizing = dict(label="One-share LIVE execution" if mode == "LIVE" else "Strategy-sized PAPER simulation",
                  details="Actual snapshot quantities are authoritative. PAPER and LIVE rupee P&L are not directly comparable because sizing differs.",
                  capital_per_entry_rs=_number(manifest.get("capital_per_entry_rs")),
                  portfolio_capital_rs=_number(manifest.get("portfolio_capital_rs")), leverage=_number(manifest.get("leverage")))
    if mode == "LIVE" and ACTIVE_PROFILE.name == "V13_V10_G_3":
        live_policy = g3_execution_metadata()
        sizing.update(label="₹10,000 capital × 5 · ₹50,000 target exposure",
                      details="Configured whole shares = floor(₹50,000 / signal trigger price); quantities can be reduced. Local LIVE portfolio-admission caps are disabled: reserved/open entries, daily admissions, and aggregate reserved gross exposure are unlimited. The ₹10,000 per-trade capital, ₹50,000 target exposure, strategy setup selection, stop/target protections, and broker funds/margin/exchange checks still apply. Filled exposure can differ with execution price. PAPER sizing is independent.",
                      capital_per_entry_rs=live_policy["capital_per_trade_rs"], leverage=live_policy["leverage"],
                      target_exposure_rs=live_policy["target_exposure_rs"],
                      max_reserved_open_positions=live_policy["max_reserved_open_positions"],
                      max_daily_entries=live_policy["max_daily_entries"],
                      max_reserved_gross_exposure_rs=live_policy["max_reserved_gross_exposure_rs"],
                      portfolio_capital_rs=None, configured_execution_policy=live_policy)
    return dict(schema_version="fno_strategy_cockpit_v1", session_date=session_date, mode=mode,
                generated_at_ist=now.isoformat(), identity=identity, policy=policy_fields,
                session_clock=dict(phase=clock_phase, timezone="Asia/Kolkata", note="IST clock context; exchange holidays are not evaluated by this read-only view."),
                available_dates=_available_dates(root, mode, session_date), symbols=symbols, selected_symbol=selected,
                evidence=dict(state=evidence_state, updated_at_ist=latest.isoformat() if latest else None,
                              warnings=list(dict.fromkeys(warnings))[:100], rows_5m=evidence_rows_5m[:250],
                              rows_1m=evidence_rows_1m[:250], coverage=coverage, orders_incomplete=incomplete),
                summary=summary, stages=stages, positions=positions, orders=orders, events=events,
                chart=chart, sizing=sizing, comparison=comparison,
                comparison_incomplete=incomplete or comparison_incomplete, read_only=True)
