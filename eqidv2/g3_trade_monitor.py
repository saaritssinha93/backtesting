"""Read-only G3 entered-trade monitoring from bounded local evidence.

The producer refreshes observations every 15 seconds. Prices retain their real
timestamps and granularity; no 15-second candles, broker calls, or orders exist
in this module. Strategy execution and this advisory store are independent.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
import hashlib
import html
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time as clock_time
from typing import Any
from types import SimpleNamespace

import dashboard_cockpit as cockpit
import fno_v13_v10_g_3_contract as contract
from fno_v13_v10_g_3_live_execution_policy import validate_existing_order_sizing
from fno_v13_v10_g_identity import canonical_signal_id, _safe_contract_stem
from fno_strategy_registry import display_identity
from g3_trade_monitor_rules import assess_trade, MONITOR_CONFIG, RULES_VERSION

SCHEMA_VERSION = "g3_trade_monitor_snapshot_v1"
IST = contract.IST
RUNTIME_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))
DEFAULT_OUTPUT_ROOT = RUNTIME_ROOT / "v13_v10_g_3_trade_monitor"
DEFAULT_LIVE_ROOT = RUNTIME_ROOT / "fno_oi/v13_v10_g_3_live"
DEFAULT_MINUTE_ROOT = RUNTIME_ROOT / "fno_oi/raw_equity_1m"
REFRESH_SECONDS = 15
MAX_SYMBOLS = 100
MAX_QUOTE_BYTES = 2 * 1024 * 1024
MAX_BAR_BYTES = 16 * 1024 * 1024
MAX_BAR_ROWS = 5000
ATOMIC_REPLACE_ATTEMPTS = 20
ATOMIC_REPLACE_DELAY_SECONDS = 0.05
ATOMIC_REPLACE_MAX_DELAY_SECONDS = 0.25
EXECUTED_STATES = {"OPEN", "SQUARE_OFF_PENDING", "CLOSED"}


def _clean(value: Any) -> Any:
    if isinstance(value, str):
        # JSON is rendered with textContent by the dashboard. Escaping here
        # would alter canonical symbols such as M&M and break quote matching.
        return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", value)[:1200]
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _day(value: date | str | None, now: datetime) -> date:
    if value is None:
        return now.date()
    if type(value) is date:
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Session date must be a date or YYYY-MM-DD")
    return date.fromisoformat(value)


def _now(value=None):
    result = datetime.now(IST) if value is None else cockpit._stamp(value)
    if result is None:
        raise ValueError("now must be timezone-aware")
    return result


def _base(day, now):
    identity = display_identity(day.isoformat(), "V13_V10_G_3")
    return dict(schema_version=SCHEMA_VERSION, session_date=day.isoformat(),
        rules_version=RULES_VERSION, monitor_config=dict(MONITOR_CONFIG),
        generated_at_ist=now.isoformat(), updated_at_ist=now.isoformat(),
        strategy_version=identity.version, strategy_fingerprint=identity.fingerprint,
        identity=identity.metadata(),
        read_only=True, advisory_only=True, execution_authority=False, refresh_seconds=REFRESH_SECONDS,
        price_granularity="AVAILABLE_TIMESTAMPED_QUOTES_OR_COMPLETED_1M", historical=day < now.date(),
        summary=dict(executed=0, open=0, closed=0, pending=0, invalid=0, conflicts=0),
        trades=[], pending=[], modes={}, warnings=[],
        note="Advisory observations only. A 15-second refresh does not create 15-second market candles.")


def _load_orders(root, day, mode, now, warnings):
    """Use cockpit bounded readers, then enforce the canonical G3 fill identity."""
    identity = display_identity(day, "V13_V10_G_3")
    archived_setups = {(row["signal_end"], row["side"]): SimpleNamespace(
        **row, setup_id=row["confirmation_end"].replace(":", "") + "_" + row["side"])
        for row in identity.configuration.get("active_setups", [])} if identity.previous_session else {}
    directories = [root / "orders" / mode / day]
    if mode == "LIVE":
        directories.append(root / "orders" / mode / "live_kite_qty1" / day)
    by_id, invalid = {}, 0
    for directory in directories:
        for path in cockpit._files(directory, ".json", cockpit.MAX_ORDERS, warnings):
            raw = cockpit._read(path, root, warnings)
            try:
                stamp = cockpit._stamp(raw.get("updated_at_ist"))
                symbol, side = raw.get("tradingsymbol"), raw.get("side")
                if (not isinstance(symbol, str) or not cockpit.SYMBOL.fullmatch(symbol)
                        or side not in {"LONG", "SHORT"} or raw.get("mode") != mode
                        or raw.get("session_date") != day or stamp is None
                        or stamp.date().isoformat() != day or stamp > now + timedelta(seconds=5)
                        or raw.get("strategy_version") != identity.version
                        or raw.get("strategy_fingerprint") != identity.fingerprint
                        or raw.get("status") not in cockpit.KNOWN_STATES):
                    raise ValueError("identity, state or timestamp mismatch")
                setup = (archived_setups.get((raw.get("signal_end"), side)) if identity.previous_session
                         else contract.setup_for(raw.get("signal_end"), side))
                if setup is None or raw.get("setup_id") != setup.setup_id or raw.get("confirmation_end") != setup.confirmation_end:
                    raise ValueError("setup mismatch")
                expected_id = canonical_signal_id(identity.version, date.fromisoformat(day),
                    setup.signal_end, setup.confirmation_end, side, symbol)
                if raw.get("signal_id") != expected_id:
                    raise ValueError("canonical signal identity mismatch")
                if raw.get("target_pct") != setup.target_pct or raw.get("stop_pct") != setup.stop_pct:
                    raise ValueError("configured target or initial stop mismatch")
                quantity = cockpit._number(raw.get("quantity"))
                if mode == "LIVE":
                    validate_existing_order_sizing(raw)
                elif raw.get("status") in EXECUTED_STATES | {"PENDING_ENTRY"}:
                    trigger = cockpit._number(raw.get("trigger_price"))
                    if (not trigger or trigger <= 0 or raw.get("capital_rs") != contract.CAPITAL_PER_ENTRY_RS
                            or raw.get("leverage") != contract.LEVERAGE
                            or raw.get("target_exposure_rs") != contract.TARGET_EXPOSURE_RS
                            or raw.get("lot_size") != 1
                            or quantity != int(contract.TARGET_EXPOSURE_RS / trigger)):
                        raise ValueError("PAPER sizing mismatch")
                if raw.get("status") in EXECUTED_STATES:
                    entered = cockpit._stamp(raw.get("entry_at_ist"))
                    entry = cockpit._number(raw.get("entry_price"))
                    confirmation = datetime.combine(date.fromisoformat(day), time.fromisoformat(setup.confirmation_end), IST)
                    if (quantity is None or quantity <= 0 or not quantity.is_integer()
                            or entry is None or entry <= 0 or entered is None
                            or entered.date().isoformat() != day or not confirmation <= entered <= now):
                        raise ValueError("no valid executed-entry evidence")
                    if raw["status"] == "CLOSED":
                        exited = cockpit._stamp(raw.get("exit_at_ist"))
                        exit_price = cockpit._number(raw.get("exit_price"))
                        if exited is None or not entered <= exited <= now or exited.date().isoformat() != day or not exit_price or exit_price <= 0:
                            raise ValueError("closed trade lacks valid exit evidence")
                item = dict(raw=raw, stamp=stamp, conflict=False)
                previous = by_id.get(expected_id)
                if previous:
                    if previous["raw"] != raw:
                        item["conflict"] = True
                        previous["conflict"] = True
                        warnings.append(f"{symbol}: conflicting {mode} snapshots; advisory flags withheld")
                    if previous["stamp"] > stamp:
                        continue
                    item["conflict"] |= previous["conflict"]
                by_id[expected_id] = item
            except (ValueError, TypeError, KeyError, OverflowError) as exc:
                invalid += 1
                warnings.append(f"Order snapshot omitted: {str(exc)[:160]}")
    return list(by_id.values()), invalid


def _validated_bars(rows, day, symbol, now, warnings):
    result, rejected = {}, 0
    flags = {"gap_filled", "is_synthetic", "synthetic", "provisional_stale", "opening_snapshot"}
    for row in rows:
        if not isinstance(row, dict):
            rejected += 1
            continue
        stamp = cockpit._stamp(row.get("timestamp"), allow_naive=True)
        values = {name: cockpit._number(row.get(name)) for name in ("open", "high", "low", "close", "volume")}
        bad_flag = any(str(row.get(flag, "")).lower() not in {"", "0", "0.0", "false", "none", "nan"} for flag in flags)
        start = cockpit._stamp(row.get("candle_start"), allow_naive=True)
        if (stamp is None or stamp.date().isoformat() != day or stamp > now
                or stamp.second or stamp.microsecond or not time(9, 16) <= stamp.time() <= time(15, 30)
                or bad_flag or str(row.get("quality_state", "VALID")).upper() != "VALID"
                or row.get("tradingsymbol", symbol) != symbol or row.get("exchange", "NSE") != "NSE"
                or any(values[k] is None or values[k] <= 0 for k in ("open", "high", "low", "close"))
                or values["volume"] is None or values["volume"] < 0
                or not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]
                or start is not None and stamp - start != timedelta(minutes=1)):
            rejected += 1
            continue
        key = stamp.isoformat()
        normalized = dict(timestamp=key, **values)
        if key in result and result[key] != normalized:
            raise ValueError("conflicting duplicate completed bars")
        result[key] = normalized
    if rejected:
        warnings.append(f"{symbol}: {rejected} invalid, flagged or future minute bars omitted")
    return [result[key] for key in sorted(result)]


def _bars(root, day, symbol, now, warnings, cache_root=None):
    """Read only the candidate feed's real completed minute bars, never interpolate."""
    path = root / day / f"{_safe_contract_stem(symbol)}_1minute.parquet"
    candidates = []
    if cache_root is not None:
        private = cache_root / day / f"{_safe_contract_stem(symbol)}.json"
        cached = cockpit._read(private, cache_root, warnings)
        if cached:
            try:
                if (cached.get("schema_version") != "g3_trade_monitor_minutes_v1"
                        or cached.get("symbol") != symbol or cached.get("session_date") != day
                        or not isinstance(cached.get("bars"), list) or len(cached["bars"]) > MAX_BAR_ROWS):
                    raise ValueError("private completed minute schema mismatch")
                candidates.append(_validated_bars(cached["bars"], day, symbol, now, warnings))
            except (ValueError, TypeError, KeyError):
                warnings.append(f"{symbol}: private minute cache invalid or conflicting")
    if not path.is_file():
        return candidates[0] if candidates else []
    try:
        if not cockpit._contained(path, root) or path.stat().st_size > MAX_BAR_BYTES:
            raise ValueError("source path or size invalid")
        import pyarrow.parquet as pq
        reader = pq.ParquetFile(path)
        required = {"timestamp", "open", "high", "low", "close", "volume"}
        names = set(reader.schema.names)
        if reader.metadata.num_rows > MAX_BAR_ROWS or not required <= names:
            raise ValueError("minute source row limit or schema invalid")
        flags = {"gap_filled", "is_synthetic", "synthetic", "provisional_stale", "opening_snapshot", "quality_state"}
        fields = sorted((required | flags | {"tradingsymbol", "exchange", "candle_start"}) & names)
        candidates.append(_validated_bars(reader.read(columns=fields).to_pylist(), day, symbol, now, warnings))
    except (ImportError, OSError, ValueError, TypeError, KeyError) as exc:
        warnings.append(f"{symbol}: completed minute data unavailable ({type(exc).__name__})")
    return max(candidates, key=lambda rows: rows[-1]["timestamp"] if rows else "", default=[])


def _quote(row, day, now):
    """Accept real quote timestamps, never file or order-update timestamps."""
    price = cockpit._number(row.get("price"))
    exchange = cockpit._stamp(row.get("exchange_timestamp"))
    received = cockpit._stamp(row.get("received_at"))
    last_trade = cockpit._stamp(row.get("last_trade_time"))
    if (not price or price <= 0 or exchange is None or received is None or last_trade is None
            or any(stamp.date().isoformat() != day for stamp in (exchange, received, last_trade))
            or not last_trade <= exchange <= received <= now):
        return None
    return dict(price=price, timestamp=last_trade, source="TIMESTAMPED_CASH_QUOTE", granularity="QUOTE",
        exchange_timestamp=exchange.isoformat(), received_at=received.isoformat())


def _quotes(day, now, symbols, quote_root, observer_root, warnings):
    result = {}
    path = quote_root / "latest.json"
    cached = cockpit._read(path, quote_root, warnings)
    if cached.get("schema_version") == "g3_trade_monitor_quotes_v1" and cached.get("session_date") == day:
        rows = cached.get("quotes")
        if isinstance(rows, list):
            for row in rows[:MAX_SYMBOLS]:
                if isinstance(row, dict) and isinstance(row.get("symbol"), str) and row["symbol"] in symbols:
                    item = _quote(row, day, now)
                    if item:
                        result[row["symbol"]] = item
    journal = observer_root / "raw" / day / "events.jsonl"
    if journal.is_file():
        try:
            if not cockpit._contained(journal, observer_root):
                raise ValueError("observer quote path escaped its root")
            with journal.open("rb") as stream:
                stream.seek(0, 2)
                offset = max(0, stream.tell() - MAX_QUOTE_BYTES)
                stream.seek(offset)
                raw = stream.read(MAX_QUOTE_BYTES)
            if offset:
                raw = raw.partition(b"\n")[2]
            # A producer may currently be appending the last line.
            lines = raw.splitlines(keepends=True)
            from dashboard_flow_observer_store import validate_event
            for line in lines:
                if not line.endswith(b"\n"):
                    continue
                try:
                    row = validate_event(json.loads(line), day)
                    if row.get("kind") != "cash" or row.get("symbol") not in symbols:
                        continue
                    item = _quote(row, day, now)
                    previous = result.get(row["symbol"])
                    if item and (not previous or item["timestamp"] > previous["timestamp"]):
                        result[row["symbol"]] = item
                except (ValueError, TypeError, UnicodeError):
                    continue
        except (OSError, ValueError) as exc:
            warnings.append(f"Local observer quote journal unavailable: {str(exc)[:100]}")
    return result


def _summarize_modes(snapshot, *, stale=False):
    """Describe each open book without presenting partial evidence as healthy."""
    directions = ("FAVORABLE", "ADVERSE", "MIXED", "WARMING_UP", "STALE", "NO_DATA")
    for mode in ("PAPER", "LIVE"):
        summary = snapshot.setdefault("modes", {}).setdefault(mode, {})
        rows = [row for row in snapshot.get("trades", []) if isinstance(row, dict)
                and row.get("mode") == mode and row.get("status") in {"OPEN", "SQUARE_OFF_PENDING"}]
        counts = dict.fromkeys(directions, 0)
        eligible, attention, marked = 0, 0, []
        for row in rows:
            direction = row.get("direction")
            counts[direction if direction in counts else "NO_DATA"] += 1
            reconciled = not any(row.get(key) for key in (
                "conflict", "partial_fill", "partial_exit", "entry_submission_uncertain"))
            fresh = (not stale and not snapshot.get("historical")
                     and row.get("mark_state") == "FRESH"
                     and row.get("freshness", {}).get("price_state") == "FRESH")
            valid = reconciled and fresh
            assessable = valid and direction in {"FAVORABLE", "ADVERSE", "MIXED"}
            eligible += int(assessable)
            attention += int(assessable and bool(row.get("advisory_flags")))
            pnl = cockpit._number(row.get("metrics", {}).get("gross_unrealized_pnl_rs"))
            if valid and pnl is not None:
                marked.append(pnl)
        open_count = len(rows)
        invalid = summary.get("invalid", 0)
        if not open_count and not invalid:
            overall = "NO_OPEN_POSITIONS"
            reason = "No verified open positions; closed trades and pending entries are excluded."
        elif invalid or eligible != open_count:
            overall = "INSUFFICIENT_DATA"
            reason = (f"{eligible}/{open_count} open positions have complete fresh directional evidence. "
                      "Stale, warming, partial, conflicting or uncertain evidence cannot establish overall direction.")
            if invalid:
                reason += f" {invalid} invalid order snapshots were omitted; book completeness is unverified."
        elif counts["FAVORABLE"] > open_count / 2:
            overall = "FAVORABLE"
            reason = "Every open position is assessable and a strict majority has favorable direction."
        elif counts["ADVERSE"] > open_count / 2:
            overall = "ADVERSE"
            reason = "Every open position is assessable and a strict majority has adverse direction."
        else:
            overall = "MIXED"
            reason = "Every open position is assessable, with no strict favorable or adverse majority."
        summary.update(open_direction_counts=counts, open_count=open_count, eligible_count=eligible,
            attention_count=attention, overall_direction=overall, overall_reason=reason,
            gross_marked_pnl_coverage_count=len(marked),
            gross_marked_pnl_rs=sum(marked) if open_count and len(marked) == open_count and not invalid else None)


def build_snapshot(day=None, *, now=None, live_root=None, minute_root=None,
                   quote_root=None, observer_root=None, minute_cache_root=None):
    now = _now(now)
    requested = _day(day, now)
    result = _base(requested, now)
    if requested > now.date():
        result["state"] = "FUTURE_SESSION"
        return result
    day = requested.isoformat()
    live_root = Path(live_root) if live_root is not None else DEFAULT_LIVE_ROOT
    minute_root = Path(minute_root) if minute_root is not None else DEFAULT_MINUTE_ROOT
    quote_root = Path(quote_root) if quote_root is not None else DEFAULT_OUTPUT_ROOT / "quotes"
    minute_cache_root = Path(minute_cache_root) if minute_cache_root is not None else DEFAULT_OUTPUT_ROOT / "minutes"
    observer_root = Path(observer_root) if observer_root is not None else RUNTIME_ROOT / "dashboard_flow_observer"
    warnings, items = result["warnings"], []
    for mode in ("PAPER", "LIVE"):
        rows, invalid = _load_orders(live_root, day, mode, now, warnings)
        counts = dict(executed=0, open=0, closed=0, pending=0, invalid=invalid, conflicts=0)
        for item in rows:
            raw = item["raw"]
            if raw["status"] == "PENDING_ENTRY":
                counts["pending"] += 1
                result["pending"].append(dict(id=f"{mode}:{raw['signal_id']}", signal_id=raw["signal_id"],
                    symbol=raw["tradingsymbol"], mode=mode, status="PENDING_ENTRY", side=raw["side"],
                    reason="Entry has not executed; excluded from trade assessments"))
            elif raw["status"] in EXECUTED_STATES:
                items.append(item)
                counts["executed"] += 1
                counts["closed" if raw["status"] == "CLOSED" else "open"] += 1
                counts["conflicts"] += int(item["conflict"])
        for key in result["summary"]:
            result["summary"][key] += counts[key]
        counts["no_trades_reason"] = ("" if counts["executed"] else
            "No verified executed trades; entry remains pending" if counts["pending"] else
            "No verified executed trades; invalid snapshots were omitted" if invalid else "No recorded executed trades for this mode and date")
        result["modes"][mode] = counts
    symbols = sorted({item["raw"]["tradingsymbol"] for item in items})
    if len(symbols) > MAX_SYMBOLS:
        warnings.append(f"Minute/quote evidence limited to {MAX_SYMBOLS} symbols")
    quotes = _quotes(day, now, set(symbols[:MAX_SYMBOLS]), quote_root, observer_root, warnings)
    bars_by_symbol = {symbol: _bars(minute_root, day, symbol, now, warnings, minute_cache_root) for symbol in symbols[:MAX_SYMBOLS]}
    for item in items:
        raw, mode = item["raw"], item["raw"]["mode"]
        bars = bars_by_symbol.get(raw["tradingsymbol"], [])
        row = dict(id=f"{mode}:{raw['signal_id']}", signal_id=raw["signal_id"], symbol=raw["tradingsymbol"],
            mode=mode, side=raw["side"], status=raw["status"], setup_id=raw["setup_id"],
            entry_at_ist=raw["entry_at_ist"], exit_at_ist=raw.get("exit_at_ist"),
            updated_at_ist=item["stamp"].isoformat(), conflict=item["conflict"],
            evidence_state="CONFLICT" if item["conflict"] else "HISTORICAL" if requested < now.date() else "VERIFIED_LOCAL_STATE")
        for key in ("entry_price", "quantity", "stop_price", "target_price", "exit_price"):
            row[key] = cockpit._number(raw.get(key))
        row["recorded_net_pnl_rs"] = cockpit._number(raw.get("net_pnl_rs"))
        requested_quantity = cockpit._number(raw.get("requested_quantity"))
        row["partial_fill"] = bool(mode == "LIVE" and requested_quantity and row["quantity"] < requested_quantity)
        row["partial_exit"] = bool(raw.get("g3_exit_fills_by_order"))
        row["entry_submission_uncertain"] = bool(raw.get("entry_submission_uncertain"))
        mark = quotes.get(raw["tradingsymbol"])
        observed = cockpit._stamp(raw.get("last_price_at_ist") or raw.get("quote_at_ist") or raw.get("quote_timestamp_ist"))
        last_price = cockpit._number(raw.get("last_price"))
        if observed and observed.date().isoformat() == day and observed <= now and last_price and last_price > 0:
            if not mark or observed > mark["timestamp"]:
                mark = dict(price=last_price, timestamp=observed, source="TIMESTAMPED_ORDER_QUOTE", granularity="QUOTE")
        if bars:
            last_bar = bars[-1]
            stamp = cockpit._stamp(last_bar["timestamp"])
            if not mark or stamp > mark["timestamp"]:
                mark = dict(price=last_bar["close"], timestamp=stamp, source="COMPLETED_1M_CLOSE", granularity="1m")
        if raw["status"] == "CLOSED":
            mark = dict(price=row["exit_price"], timestamp=cockpit._stamp(row["exit_at_ist"]),
                source="RECORDED_EXIT", granularity="FILL")
            bars = [bar for bar in bars if cockpit._stamp(bar["timestamp"]) <= mark["timestamp"]]
        row.update(mark_price=mark["price"] if mark else None,
            mark_timestamp=mark["timestamp"].isoformat() if mark else None,
            mark_source=mark["source"] if mark else None, mark_granularity=mark["granularity"] if mark else None,
            mark_age_seconds=max(0., (now - mark["timestamp"]).total_seconds()) if mark else None,
            mark_state="NO_DATA" if not mark else "HISTORICAL" if requested < now.date() else
                "FRESH" if (now - mark["timestamp"]).total_seconds() <= 120 else "STALE",
            completed_bars=len(bars), bars_granularity="1m")
        assessment = assess_trade(row, bars, now=now, mark_price=row["mark_price"],
            mark_timestamp=row["mark_timestamp"], config=MONITOR_CONFIG)
        suppressed = []
        if raw["status"] == "CLOSED": suppressed.append("Closed trade: no current action flags")
        if requested < now.date(): suppressed.append("Historical session: no current action flags")
        if item["conflict"]: suppressed.append("Conflicting order snapshots: no action flags")
        if row["partial_fill"] or row["partial_exit"]: suppressed.append("Partial fills/exits: quantity and P&L require execution reconciliation")
        if row["entry_submission_uncertain"]: suppressed.append("Entry submission uncertainty: no action flags")
        if suppressed:
            assessment = {**assessment, "advisory_flags": [], "reasons": [*assessment.get("reasons", []), *suppressed]}
        if row["partial_exit"] or item["conflict"] or raw["status"] == "CLOSED":
            assessment["metrics"] = {**assessment.get("metrics", {}), "gross_unrealized_pnl_rs": None}
        if item["conflict"]:
            assessment["direction"] = "NO_DATA"
        row["assessment"] = assessment
        row.update({key: assessment.get(key) for key in ("direction", "metrics", "reasons", "advisory_flags", "freshness", "rules_version")})
        result["trades"].append(row)
    result["trades"].sort(key=lambda row: (row["mode"], row["entry_at_ist"], row["id"]))
    result["state"] = ("HISTORICAL" if requested < now.date() else "PARTIAL" if warnings or result["summary"]["invalid"]
                       or result["summary"]["conflicts"] else "READY" if result["trades"] else "NO_EXECUTED_TRADES")
    open_rows = [row for row in result["trades"] if row["status"] != "CLOSED"]
    if open_rows and all(row["mark_state"] in {"STALE", "NO_DATA", "HISTORICAL"} for row in open_rows):
        warnings.append("No fresh timestamped price evidence is available; order update times are not quote timestamps")
    _summarize_modes(result)
    return _clean(result)


def _atomic(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".monitor-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt + 1 >= ATOMIC_REPLACE_ATTEMPTS:
                    raise
                clock_time.sleep(min(
                    ATOMIC_REPLACE_DELAY_SECONDS * (attempt + 1),
                    ATOMIC_REPLACE_MAX_DELAY_SECONDS,
                ))
    finally:
        Path(temporary).unlink(missing_ok=True)


def render_markdown(snapshot):
    lines = ["# G3 entered-trade monitor", "", f"Session: {snapshot['session_date']}",
        f"Observed: {snapshot['generated_at_ist']}", f"State: {snapshot['state']}",
        "Advisory only; no order authority. Refresh: 15 seconds; prices retain quote or completed 1m granularity.", "",
        "| Mode | Symbol | Side | Status | Mark | Price time | Direction |", "|---|---|---|---|---:|---|---|"]
    for row in snapshot["trades"]:
        values = [row["mode"], row["symbol"], row["side"], row["status"], row.get("mark_price"),
                  row.get("mark_timestamp"), row.get("direction")]
        lines.append("| " + " | ".join(html.escape(str(value if value is not None else "—")).replace("|", "\\|") for value in values) + " |")
    lines += ["", f"Pending entries (not entered trades): {snapshot['summary']['pending']}"]
    lines += ["", *["- " + html.escape(str(value)) for value in snapshot.get("warnings", [])]]
    return "\n".join(lines) + "\n"


def write_snapshot(snapshot, output_root=None):
    root = Path(output_root) if output_root is not None else DEFAULT_OUTPUT_ROOT
    if snapshot.get("schema_version") != SCHEMA_VERSION or snapshot.get("execution_authority") is not False:
        raise ValueError("Invalid advisory snapshot")
    day = _day(snapshot.get("session_date"), _now())
    stamp = _now(snapshot.get("generated_at_ist"))
    encoded = json.dumps(snapshot, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    relative = Path("snapshots") / day.isoformat() / (stamp.strftime("%Y%m%dT%H%M%S%f") + "_" + digest[:12] + ".json")
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(encoded)
    except FileExistsError:
        if target.read_text(encoding="utf-8") != encoded:
            raise ValueError("Immutable snapshot collision")
    else:
        log = root / "logs" / f"{day}.jsonl"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(dict(generated_at_ist=stamp.isoformat(), session_date=str(day),
                snapshot=relative.as_posix(), sha256=digest, state=snapshot["state"], summary=snapshot["summary"]), allow_nan=False) + "\n")
    _atomic(root / "latest/latest.md", render_markdown(snapshot))
    _atomic(root / "latest/latest.json", encoded)
    return dict(snapshot=relative.as_posix(), sha256=digest, latest="latest/latest.json", report="latest/latest.md")


def load_latest(output_root=None, *, day=None, now=None):
    root = Path(output_root) if output_root is not None else DEFAULT_OUTPUT_ROOT
    current = _now(now)
    requested = _day(day, current)
    identity = display_identity(str(requested), "V13_V10_G_3")
    warnings = []
    snapshot = cockpit._read(root / "latest/latest.json", root, warnings)
    if (snapshot.get("schema_version") != SCHEMA_VERSION or snapshot.get("session_date") != str(requested)
            or snapshot.get("rules_version") != RULES_VERSION
            or snapshot.get("execution_authority") is not False
            or snapshot.get("strategy_version") != identity.version
            or snapshot.get("strategy_fingerprint") != identity.fingerprint):
        empty = _base(requested, current)
        empty.update(state="NO_SNAPSHOT", generated_at_ist=None, updated_at_ist=None,
            snapshot_age_seconds=None, snapshot_stale=True, warnings=warnings)
        return empty
    snapshot["identity"] = identity.metadata()
    generated = cockpit._stamp(snapshot.get("generated_at_ist"))
    if generated is None or generated > current + timedelta(seconds=5):
        empty = _base(requested, current)
        empty.update(state="NO_SNAPSHOT", generated_at_ist=None, updated_at_ist=None,
            snapshot_age_seconds=None, snapshot_stale=True, warnings=["Snapshot time is invalid"])
        return empty
    age = max(0., (current-generated).total_seconds())
    stale = age > 3 * REFRESH_SECONDS
    if stale:
        for row in snapshot.get("trades", []):
            if not isinstance(row, dict):
                continue
            row["advisory_flags"] = []
            if row.get("status") != "CLOSED":
                row["direction"] = "STALE"
            reasons = row.get("reasons", [])
            row["reasons"] = [*(reasons if isinstance(reasons, list) else []), "Monitor snapshot is stale; cached action flags are withheld"]
            assessment = row.get("assessment")
            if isinstance(assessment, dict):
                assessment.update(advisory_flags=[], direction=row.get("direction"), reasons=row["reasons"])
    _summarize_modes(snapshot, stale=stale)
    return {**snapshot, "snapshot_age_seconds": age, "snapshot_stale": stale}


class AlreadyRunning(RuntimeError):
    pass


@contextmanager
def worker_lock(output_root=None):
    """An OS-released exclusive lock makes duplicate worker starts harmless."""
    root = Path(output_root) if output_root is not None else DEFAULT_OUTPUT_ROOT
    root.mkdir(parents=True, exist_ok=True)
    handle = (root / ".worker.lock").open("a+b")
    try:
        if handle.seek(0, 2) == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise AlreadyRunning("G3 advisory monitor already running") from exc
        yield
    finally:
        handle.close()
