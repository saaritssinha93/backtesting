"""Read-only, recorded G3 decision evidence for the five-minute market flow.

The market ranking is not a strategy decision. This module never evaluates a
rule, repairs an old packet, imports a live engine, or talks to a broker.
Confirmation and eventual order outcomes are explicitly subsequent evidence,
not facts that were available at the selected five-minute close.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
from datetime import date, datetime, timedelta
from functools import lru_cache
from itertools import islice
from pathlib import Path
from typing import Any

from fno_v13_v10_g_3_contract import (IST, SIGNAL_TO_CONFIRMATION, STRATEGY_VERSION,
    strategy_fingerprint, supported_identity)
from fno_v13_v10_g_identity import canonical_signal_id

MAX_JSON = 4 * 1024 * 1024
MAX_CSV = 64 * 1024 * 1024
MAX_ROWS = 100_000
MAX_FILES = 500
SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9.&_-]{0,39}\Z")
SLOTS = tuple(f"{minute // 60:02d}:{minute % 60:02d}" for minute in range(555, 931, 5))
FINGERPRINT = strategy_fingerprint()


def _text(value: Any, maximum: int = 240) -> str:
    return value[:maximum] if isinstance(value, str) else ""


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if not isinstance(value, bool) and math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _stamp(value: Any) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.astimezone(IST) if result.tzinfo is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def _truth(value: Any) -> bool | None:
    if type(value) is bool:
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    return None


def _bytes(path: Path, root: Path, maximum: int) -> bytes:
    if not path.resolve().is_relative_to(root.resolve()) or path.stat().st_size > maximum:
        raise ValueError("Evidence leaves root or exceeds read limit")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("Evidence exceeds read limit")
    return raw


@lru_cache(maxsize=48)
def _cached_json(path: str, root: str, modified_ns: int, size: int) -> dict:
    value = json.loads(_bytes(Path(path), Path(root), MAX_JSON).decode("utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError("Expected evidence object")
    return value


def _json(path: Path, root: Path) -> dict:
    # Atomic producer replacement invalidates this bounded cache immediately.
    info = path.stat()
    return _cached_json(str(path.resolve()), str(root.resolve()), info.st_mtime_ns, info.st_size)


def _identity(raw: dict, day: str, *, allow_legacy: bool = False) -> bool:
    return (raw.get("session_date") == day and supported_identity(
        raw.get("strategy_version"), raw.get("strategy_fingerprint"), allow_legacy=allow_legacy))


def _source(path: Path, root: Path, kind: str) -> dict:
    return {"path": path.relative_to(root).as_posix(), "label": kind}


def _row(slot: dict, symbol: str) -> dict:
    return slot["rows"].setdefault(symbol, dict(symbol=symbol, side=None, decision="NO_RECORDED_DECISION",
        reasons=[], base_pass=None, published=None, confirmed=None, selected=None,
        scanner_gates={}, confirmation_gates={}, orders=[], backtest=[], signal_ids=[]))


def _gates(raw: dict) -> dict:
    return {key: value for key, value in raw.items()
            if key.startswith("gate_") and len(key) <= 80 and type(value) is bool}


def _failed(raw: dict) -> list[str]:
    value = raw.get("failed_gates", [])
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            value = []
    result = [_text(item, 80) for item in value[:40] if isinstance(item, str)] if isinstance(value, list) else []
    first = _text(raw.get("first_failed_gate"), 80)
    return result or ([first] if first else [])


def _packet(path: Path, root: Path, day: str, clock: str, now: datetime, warnings: list[str]) -> dict | None:
    if not path.is_file():
        return None
    try:
        raw = _json(path, root)
        stamp = _stamp(raw.get("published_at_ist"))
        if (not _identity(raw, day, allow_legacy=True) or raw.get("signal_end") != clock or stamp is None
                or stamp.date().isoformat() != day or stamp > now + timedelta(seconds=5)
                or stamp < datetime.fromisoformat(f"{day}T{clock}:00").replace(tzinfo=IST)
                or raw.get("confirmation_end") != SIGNAL_TO_CONFIRMATION[clock]):
            raise ValueError("Packet identity or clock mismatch")
        for key in ("feature_evaluations", "candidates", "selected_signal_ids"):
            if key in raw and (not isinstance(raw[key], list) or len(raw[key]) > 2000):
                raise ValueError("Packet list invalid")
        return raw
    except (OSError, UnicodeError, ValueError, TypeError, KeyError, RecursionError):
        warnings.append(f"{path.relative_to(root).as_posix()}: invalid or future G3 evidence omitted")
        return None


def _features(raw: dict, day: str, clock: str, warnings: list[str]) -> list[dict]:
    rows, symbols = [], set()
    for value in raw.get("feature_evaluations", []):
        symbol = value.get("tradingsymbol") if isinstance(value, dict) else None
        if (not isinstance(value, dict) or not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol)
                or not _identity(value, day, allow_legacy=True) or value.get("signal_end") != clock
                or value.get("strategy_version") != raw.get("strategy_version")
                or value.get("strategy_fingerprint") != raw.get("strategy_fingerprint")
                or symbol in symbols):
            warnings.append(f"{clock}: invalid or duplicate G3 feature row omitted")
            continue
        symbols.add(symbol)
        rows.append(value)
    return rows


def _load_slot(slot: dict, root: Path, data_root: Path, day: str, clock: str, now: datetime,
               warnings: list[str], sources: list[dict]) -> None:
    scanner_path = root / "scanner_5m" / day / f"slot_{clock.replace(':', '')}.json"
    confirmation_clock = SIGNAL_TO_CONFIRMATION[clock]
    confirmation_path = root / "confirmation_1m" / day / f"slot_{confirmation_clock.replace(':', '')}.json"
    scanner = _packet(scanner_path, data_root, day, clock, now, warnings)
    confirmation = _packet(confirmation_path, data_root, day, clock, now, warnings)
    if (scanner is not None and confirmation is not None
            and (scanner["strategy_version"], scanner["strategy_fingerprint"])
            != (confirmation["strategy_version"], confirmation["strategy_fingerprint"])):
        warnings.append(f"{clock}: scanner and confirmation strategy identities differ; confirmation omitted")
        confirmation = None
    if scanner is not None:
        sources.append(_source(scanner_path, data_root, "Recorded G3 scanner"))
        features = _features(scanner, day, clock, warnings)
        slot["scanner"] = dict(state=_text(scanner.get("state")), published_at_ist=scanner["published_at_ist"],
            strategy_version=scanner["strategy_version"],
            source=sources[-1]["path"], contracts_expected=_number(scanner.get("contracts_expected")),
            contracts_missing=_number(scanner.get("contracts_missing_slot")))
        for value in features:
            row = _row(slot, value["tradingsymbol"])
            long, short = value.get("base_long_pass") is True, value.get("base_short_pass") is True
            row.update(side="LONG" if long else "SHORT" if short else None, base_pass=long or short,
                       recorded_strategy_version=scanner["strategy_version"],
                       scanner_gates=_gates(value), decision="BASE_PASSED" if long or short else "BASE_REJECTED",
                       reasons=_failed(value), signal_at_ist=_text(value.get("signal_ts")))
        candidates = []
        for value in scanner.get("candidates", []):
            symbol = value.get("tradingsymbol") if isinstance(value, dict) else None
            if (not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol)
                    or value.get("side") not in {"LONG", "SHORT"} or value.get("signal_end") != clock):
                warnings.append(f"{clock}: invalid scanner candidate omitted")
                continue
            candidates.append(symbol)
            row = _row(slot, symbol)
            row.update(side=value["side"], published=True, decision="PUBLISHED_CANDIDATE",
                       recorded_strategy_version=scanner["strategy_version"])
        for row in slot["rows"].values():
            if row["published"] is None:
                row["published"] = False if "candidates" in scanner else None
            if row["base_pass"] is True and row["published"] is False:
                row.update(decision="BASE_PASSED_NOT_PUBLISHED", reasons=["Base gates passed in the recorded ledger; no scanner candidate was published."])
        slot["counts"].update(evaluated=len(features) if "feature_evaluations" in scanner else None,
            base_pass=sum(row["base_pass"] is True for row in slot["rows"].values()) if "feature_evaluations" in scanner else None,
            published=len(set(candidates)) if "candidates" in scanner else None,
            base_not_published=sum(row["decision"] == "BASE_PASSED_NOT_PUBLISHED" for row in slot["rows"].values()))
        if slot["counts"]["base_not_published"]:
            slot["notes"].append("Base-passed / not-published rows identify a recorded pipeline gap; these are not proven missed trades.")
    if confirmation is not None:
        sources.append(_source(confirmation_path, data_root, "Subsequent G3 confirmation outcome"))
        selected_ids = {value for value in confirmation.get("selected_signal_ids", []) if isinstance(value, str)}
        rejections = confirmation.get("candidate_rejections", {})
        rejections = rejections if isinstance(rejections, dict) else {}
        slot["confirmation"] = dict(state=_text(confirmation.get("state")),
            confirmation_end=confirmation_clock, published_at_ist=confirmation["published_at_ist"],
            strategy_version=confirmation["strategy_version"],
            source=sources[-1]["path"], selected_signal_ids=sorted(selected_ids),
            timing="SUBSEQUENT_OUTCOME", scanner_complete=confirmation.get("scanner_complete") is True)
        for value in _features(confirmation, day, clock, warnings):
            row = _row(slot, value["tradingsymbol"])
            side = value.get("base_side")
            if side in {"LONG", "SHORT"}:
                row["side"] = side
            row.update(recorded_strategy_version=confirmation["strategy_version"],
                confirmation_gates=_gates(value), confirmation_at_ist=_text(value.get("confirmation_ts")),
                confirmation_volume_ratio=_number(value.get("v9_1m_volume_ratio")),
                minimum_confirmation_volume_ratio=_number(value.get("minimum_confirmation_volume_ratio")),
                setup_id=_text(value.get("setup_id")), setup_filter_pass=_truth(value.get("setup_filter_pass")))
            # This is the recorded directional/volume confirmation outcome;
            # merely passing its direction gate does not pass volume gates.
            if value["tradingsymbol"] in rejections:
                row.update(confirmed=False, decision="CONFIRMATION_REJECTED",
                           reasons=[_text(rejections[value["tradingsymbol"]])] + _failed(value))
            elif value.get("gate_confirmation_present") is True:
                confirmation_gates = ("gate_confirmation_direction", "gate_confirmation_volume")
                row["confirmed"] = all(value.get(key) is True for key in confirmation_gates)
                row.update(decision="SETUP_PASSED" if value.get("setup_filter_pass") is True else "SETUP_REJECTED", reasons=_failed(value))
        for symbol, reason in rejections.items():
            if isinstance(symbol, str) and SYMBOL.fullmatch(symbol):
                row = _row(slot, symbol)
                row.update(recorded_strategy_version=confirmation["strategy_version"],
                           confirmed=False, decision="CONFIRMATION_REJECTED", reasons=[_text(reason)] + row["reasons"])
        mapped_ids = set()
        for row in slot["rows"].values():
            if row["side"] not in {"LONG", "SHORT"}:
                continue
            signal_id = canonical_signal_id(confirmation["strategy_version"], date.fromisoformat(day),
                clock, confirmation_clock, row["side"], row["symbol"])
            row["selected"] = signal_id in selected_ids if "selected_signal_ids" in confirmation else None
            if row["selected"]:
                row.update(decision="SELECTED_SIGNAL", reasons=[])
                row["signal_ids"].append(signal_id)
                mapped_ids.add(signal_id)
        if selected_ids - mapped_ids:
            warnings.append(f"{clock}: selected signal IDs lack matching symbol evidence")
        slot["counts"].update(confirmed=_number(confirmation.get("directional_confirmed")),
            selected=len(selected_ids) if "selected_signal_ids" in confirmation else None)
        slot["notes"].append(f"Confirmation at {confirmation_clock} and eventual order outcomes occurred after the {clock} market snapshot.")
    if scanner is not None or confirmation is not None:
        slot["state"] = "RECORDED" if scanner and scanner.get("state") == "SUCCESS" else "PARTIAL_EVIDENCE"
        if (scanner or confirmation)["strategy_version"] != STRATEGY_VERSION:
            slot["notes"].append("Recorded G-3 V1 decisions are historical; V2 retrospective backtest evidence is separate.")


def _load_orders(slots: dict, root: Path, data_root: Path, day: str, now: datetime,
                 warnings: list[str], sources: list[dict]) -> None:
    for mode in ("PAPER", "LIVE"):
        directories = [root / "orders" / mode / day]
        if mode == "LIVE":
            directories.append(root / "orders" / mode / "live_kite_qty1" / day)
        known = any(directory.is_dir() for directory in directories)
        seen, valid, bad = {}, [], False
        for directory in directories:
            paths = sorted(islice(directory.glob("*.json"), MAX_FILES + 1))
            if len(paths) > MAX_FILES:
                bad = True
                warnings.append(f"{mode}: order snapshots truncated at {MAX_FILES}")
            for path in paths[:MAX_FILES]:
                try:
                    raw = _json(path, data_root)
                    symbol, clock, signal_id = raw.get("tradingsymbol"), raw.get("signal_end"), raw.get("signal_id")
                    stamp = _stamp(raw.get("updated_at_ist"))
                    if (not _identity(raw, day, allow_legacy=True) or raw.get("mode") != mode or clock not in SIGNAL_TO_CONFIRMATION
                            or not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol) or not isinstance(signal_id, str)
                            or raw.get("side") not in {"LONG", "SHORT"} or stamp is None or stamp > now + timedelta(seconds=5)
                            or stamp.date().isoformat() != day
                            or stamp < datetime.fromisoformat(f"{day}T{clock}:00").replace(tzinfo=IST)):
                        raise ValueError("Order identity mismatch")
                    expected_id = canonical_signal_id(raw["strategy_version"], date.fromisoformat(day),
                        clock, SIGNAL_TO_CONFIRMATION[clock], raw["side"], symbol)
                    if signal_id != expected_id:
                        raise ValueError("Signal identity mismatch")
                    packet = slots[clock].get("scanner") or slots[clock].get("confirmation")
                    if packet and packet["strategy_version"] != raw["strategy_version"]:
                        raise ValueError("Order and recorded packet strategy identities differ")
                    if signal_id in seen:
                        if seen[signal_id] != raw:
                            bad = True
                            warnings.append(f"{mode} {symbol}: conflicting order snapshots; omitted")
                            valid = [item for item in valid if item[0].get("signal_id") != signal_id]
                        continue
                    seen[signal_id] = raw
                    valid.append((raw, path))
                except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
                    bad = True
                    warnings.append(f"{mode} {path.name}: invalid G3 order snapshot omitted")
        for slot in slots.values():
            slot["orders_state"][mode] = "INCOMPLETE" if bad else "RECORDED_SNAPSHOTS" if known else "UNAVAILABLE"
            if known:
                slot["counts"][mode.lower() + "_orders"] = 0
        for raw, path in valid:
            slot = slots[raw["signal_end"]]
            row = _row(slot, raw["tradingsymbol"])
            if (row.get("recorded_strategy_version") is not None
                    and row["recorded_strategy_version"] != raw["strategy_version"]):
                slot["orders_state"][mode] = "INCOMPLETE"
                warnings.append(f"{mode} {raw['tradingsymbol']}: mixed strategy versions; order omitted")
                continue
            row["side"] = raw["side"]
            row["recorded_strategy_version"] = raw["strategy_version"]
            row["orders"].append(dict(mode=mode, status=_text(raw.get("status")), reason=_text(raw.get("status_reason")),
                strategy_version=raw["strategy_version"],
                signal_id=raw["signal_id"], quantity=_number(raw.get("quantity")),
                entry_at_ist=_text(raw.get("entry_at_ist")), exit_at_ist=_text(raw.get("exit_at_ist")),
                updated_at_ist=raw["updated_at_ist"], timing="LATEST_RECORDED_OUTCOME",
                filled=bool(_stamp(raw.get("entry_at_ist"))
                    and _stamp(raw["entry_at_ist"]).date().isoformat() == day
                    and _stamp(raw["entry_at_ist"]) <= _stamp(raw["updated_at_ist"])
                    and (_number(raw.get("entry_price")) or 0) > 0 and (_number(raw.get("quantity")) or 0) > 0),
                source=path.relative_to(data_root).as_posix()))
            slot["counts"][mode.lower() + "_orders"] += 1
            sources.append(_source(path, data_root, f"Recorded {mode} order snapshot"))


def _history(data_root: Path, day: str, warnings: list[str], sources: list[dict]) -> tuple[dict, list[dict], list[dict]]:
    root = data_root / "backtesting_result_v13_v10_g_3"
    pointer_path = root / "latest" / "current_history.json"
    if not pointer_path.is_file():
        return {"state": "UNAVAILABLE"}, [], []
    try:
        pointer = _json(pointer_path, root)
        relative = Path(pointer["run_dir"])
        run = root / relative
        if relative.is_absolute() or run.resolve().relative_to(root.resolve()).parts[:2] != ("history", "runs"):
            raise ValueError("Invalid publication tree")
        manifest_raw = _bytes(run / "run_manifest.json", root, MAX_JSON)
        if hashlib.sha256(manifest_raw).hexdigest() != pointer["manifest_sha256"]:
            raise ValueError("Manifest hash mismatch")
        manifest = json.loads(manifest_raw)
        for record in (pointer, manifest):
            if (record.get("role") != "cumulative_history" or record.get("strategy_version") != STRATEGY_VERSION
                    or record.get("strategy_fingerprint") != FINGERPRINT):
                raise ValueError("Historical identity mismatch")
        if (manifest.get("schema_version") != "fno_g3_production_run_v1" or manifest.get("complete") is not True
                or manifest.get("execution_authority") is not False
                or manifest.get("state") not in {"SUCCESS", "SUCCESS_WITH_EXCLUDED_SESSIONS"}
                or pointer.get("session_date") != manifest.get("session_date")):
            raise ValueError("Incomplete history")
        coverage = manifest.get("coverage", {})
        excluded = [row for row in coverage.get("excluded_sessions", []) if row.get("day") == day]
        if excluded:
            return {"state": "EXCLUDED_SESSION", "reason": _text(excluded[0].get("state"))}, [], []
        if day not in manifest.get("days", []):
            return {"state": "SESSION_NOT_COVERED"}, [], []
        result, artifact_sources = [], []
        for name in ("selection_audit", "portfolio_trades"):
            artifact = manifest["artifacts"][name]
            relative = Path(artifact["path"])
            if relative.is_absolute():
                raise ValueError("Absolute artifact path")
            path = run / relative
            raw = _bytes(path, run, MAX_CSV)
            if hashlib.sha256(raw).hexdigest() != artifact["sha256"]:
                raise ValueError("Artifact hash mismatch")
            records = list(islice(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))), MAX_ROWS + 1))
            if len(records) > MAX_ROWS:
                raise ValueError("CSV row limit")
            rows = [row for row in records if row.get("day") == day]
            if any(row.get("strategy_version") != STRATEGY_VERSION or row.get("strategy_fingerprint") != FINGERPRINT for row in rows):
                raise ValueError("Historical row identity mismatch")
            result.append(rows)
            artifact_sources.append(_source(path, data_root, f"Hash-verified G3 backtest {name}"))
        sources.extend(artifact_sources)
        return {"state": "VERIFIED_BACKTEST", "run": run.relative_to(data_root).as_posix(),
                "strategy_version": STRATEGY_VERSION,
                "manifest_sha256": pointer["manifest_sha256"], "timing": "RETROSPECTIVE_BACKTEST"}, result[0], result[1]
    except (OSError, ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
        warnings.append("G3 historical package failed identity/integrity checks; no backtest decisions accepted")
        return {"state": "INVALID_BACKTEST_EVIDENCE"}, [], []


def _load_history(slots: dict, history: dict, audits: list[dict], trades: list[dict], day: str) -> None:
    if history["state"] != "VERIFIED_BACKTEST":
        return
    trade_index = {row.get("signal_id"): row for row in trades if row.get("signal_id")}
    for clock, slot in slots.items():
        if clock in SIGNAL_TO_CONFIRMATION and slot["state"] != "FUTURE":
            slot["counts"].update(backtest_candidates=0, backtest_selected=0, backtest_fills=0)
            if slot["state"] == "MISSING_EVIDENCE":
                slot["state"] = "BACKTEST_ONLY"
    for raw in audits:
        stamp = _stamp(raw.get("signal_timestamp") or raw.get("signal_ts"))
        symbol, side = raw.get("tradingsymbol"), raw.get("side")
        clock = stamp.strftime("%H:%M") if stamp else None
        if (clock not in SIGNAL_TO_CONFIRMATION or stamp.date().isoformat() != day
                or not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol) or side not in {"LONG", "SHORT"}):
            continue
        slot, selected = slots[clock], _truth(raw.get("v9_selected"))
        if slot["state"] == "FUTURE":
            continue
        row = _row(slot, symbol)
        if row["side"] is None and row.get("recorded_strategy_version") is None:
            row["side"] = side
        trade = trade_index.get(raw.get("signal_id"), {})
        filled = _truth(trade.get("portfolio_executed")) if trade else None
        row["backtest"].append(dict(side=side, selected=selected, filled=filled,
            strategy_version=history["strategy_version"],
            decision=_text(raw.get("v9_decision")), setup_id=_text(raw.get("setup_id")),
            filter_pass=_truth(raw.get("v9_filter_pass")), rank=_number(raw.get("v9_rank_in_setup_day")),
            signal_id=_text(raw.get("signal_id")), signal_at_ist=stamp.isoformat(),
            confirmation_at_ist=_text(raw.get("confirmation_timestamp") or raw.get("confirmation_ts")),
            entry_at_ist=_text(trade.get("entry_ts")), exit_at_ist=_text(trade.get("exit_ts")),
            status=_text(trade.get("portfolio_status")), reason=_text(trade.get("portfolio_reject_reason")),
            net_pnl_rs=_number(trade.get("portfolio_net_profit_rupees")), timing="RETROSPECTIVE_BACKTEST"))
        if row["decision"] == "NO_RECORDED_DECISION":
            row["decision"] = "BACKTEST_SELECTED" if selected else "BACKTEST_REJECTED"
        slot["counts"]["backtest_candidates"] += 1
        slot["counts"]["backtest_selected"] += selected is True
        slot["counts"]["backtest_fills"] += filled is True


def build_live_evidence(data_root: Path, session_date: str, *, now: datetime | None = None) -> dict:
    """Return one session of immutable observations, keyed by signal slot/symbol.

    Counters are ``None`` where evidence is absent, zero only for an available
    recorded source. Backtest and recorded runtime counters remain separate.
    ``now`` bounds future-dated files, not subsequent outcomes of an old slot.
    """
    if not isinstance(session_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", session_date):
        raise ValueError("Use a YYYY-MM-DD session date")
    date.fromisoformat(session_date)
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        raise ValueError("now must have a timezone")
    now = now.astimezone(IST)
    data_root = Path(data_root)
    root = data_root / "fno_oi" / "v13_v10_g_3_live"
    warnings, sources, slots = [], [], {}
    count_names = ("evaluated", "base_pass", "published", "confirmed", "selected", "base_not_published",
                   "paper_orders", "live_orders", "backtest_candidates", "backtest_selected", "backtest_fills")
    for clock in SLOTS:
        expected = clock in SIGNAL_TO_CONFIRMATION
        future = datetime.fromisoformat(f"{session_date}T{clock}:00").replace(tzinfo=IST) > now
        slots[clock] = dict(state="FUTURE" if future else "MISSING_EVIDENCE" if expected else "NOT_STRATEGY_SLOT",
            strategy_slot=expected, scanner=None, confirmation=None, counts=dict.fromkeys(count_names),
            orders_state={}, rows={}, notes=[])
        if expected and not future:
            _load_slot(slots[clock], root, data_root, session_date, clock, now, warnings, sources)
    _load_orders(slots, root, data_root, session_date, now, warnings, sources)
    history, audits, trades = _history(data_root, session_date, warnings, sources)
    _load_history(slots, history, audits, trades, session_date)
    return dict(schema_version="dashboard_flow_live_evidence_v1", session_date=session_date,
        strategy_version=STRATEGY_VERSION, strategy_fingerprint=FINGERPRINT, read_only=True,
        state="AVAILABLE" if any(slot["rows"] for slot in slots.values()) else "NO_RECORDED_EVIDENCE",
        history=history, slots=slots, warnings=sorted(set(warnings))[:100], sources=sources,
        note="Recorded scanner decisions, subsequent confirmation/order outcomes, and retrospective backtests are separate evidence; no rules are re-evaluated.")
