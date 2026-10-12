"""Resolve portfolio days without requiring exits for provably rejected fills.

Selection is immutable input. A missing exit may be irrelevant when the native
portfolio must reject that exact, already observed entry for lack of capital.
An admissible unresolved fill blocks its entire session immediately. We never
invent an exit, reserve capital for an unknown duration, refill a selection,
or publish partial P&L for a blocked day.

The caller must verify source hashes and obtain evidence from the execution
proof helpers. This module consumes their evidence; it does not independently
authenticate candle files. ``known_ledger`` contains every resolved order's
native trade fields, including unfilled orders. Existing portfolio fields in
that ledger are ignored and recomputed with the unchanged frozen helper.
"""
from __future__ import annotations

from collections.abc import Mapping
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd

from . import baseline
from ._baseline import execution

EVIDENCE_VERSION = "V14_EXACT_PORTFOLIO_REJECTION_PROOF_V1"
_STATE = (
    "portfolio_open_positions_before", "portfolio_reserved_capital_before_rupees",
    "portfolio_gross_exposure_before_rupees", "portfolio_open_risk_before_rupees",
    "portfolio_trade_capital_rupees", "portfolio_trade_exposure_rupees",
    "portfolio_trade_initial_risk_rupees",
)
_PROOF_COLUMNS = ("sid", "day", "entry_ts", "reason", "active_sids", "active_exit_timestamps",
                  "reserved_capital_rupees", "required_capital_rupees", "portfolio_capital_rupees",
                  "source_prefix_sha256", "source_evidence_version")
_BLOCK_COLUMNS = ("day", "reason", "sid", "source_reason", "entry_ts", "active_sids")


def _truth(value) -> bool:
    if value is True or isinstance(value, np.bool_) and bool(value):
        return True
    if value is False or isinstance(value, np.bool_) and not bool(value):
        return False
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise ValueError(f"Expected explicit Boolean, received {value!r}")


def _stamp(value) -> pd.Timestamp:
    result = execution._to_ist_timestamp(value)
    if pd.isna(result) or result.value % execution.MINUTE_NS:
        raise ValueError("Portfolio evidence requires exact minute timestamps")
    return result


def _close(left, right) -> bool:
    return bool(np.isfinite(float(left)) and np.isfinite(float(right))
                and np.isclose(float(left), float(right), rtol=1e-10, atol=1e-8))


def _evidence(raw: Mapping, row: dict) -> tuple[bool, str, dict]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"Missing execution evidence for sid={row['sid']}")
    resolved = _truth(raw.get("resolved"))
    details = dict(raw.get("evidence", raw))
    for field, expected in (("sid", int(row["sid"])), ("day", str(row["day"])),
                            ("symbol", str(row["tradingsymbol"]))):
        if field in details and str(details[field]) != str(expected):
            raise ValueError(f"Execution evidence {field} differs for sid={row['sid']}")
    return resolved, str(raw.get("reason", "")), details


def _entry(details: dict, row: dict, *, require_contiguous: bool, expiry: int) -> dict:
    entry = details.get("entry")
    if not isinstance(entry, Mapping):
        raise ValueError(f"Missing proven entry for sid={row['sid']}")
    timestamp = _stamp(entry["ts"])
    confirmation = _stamp(row["confirmation_ts"])
    elapsed = int((timestamp - confirmation) / pd.Timedelta(minutes=1))
    if not 1 <= elapsed <= expiry:
        raise ValueError(f"Proven entry lies outside native expiry for sid={row['sid']}")
    if not np.isfinite(float(entry["price"])) or float(entry["price"]) <= 0:
        raise ValueError(f"Invalid proven entry price for sid={row['sid']}")
    if "trigger" in entry and not _close(entry["trigger"], row["trigger"]):
        raise ValueError(f"Proven entry trigger differs for sid={row['sid']}")
    if require_contiguous:
        index = int(entry["index"])
        if index != elapsed - 1 or index < 0 or int(details.get("continuous_valid_minutes", 0)) <= index:
            raise ValueError(f"Unresolved entry lacks a contiguous proof for sid={row['sid']}")
        if _stamp(details["prefix_end"]) < timestamp:
            raise ValueError(f"Proven entry follows observed prefix for sid={row['sid']}")
        if "confirmation_ts" in details and _stamp(details["confirmation_ts"]) != confirmation:
            raise ValueError(f"Execution evidence confirmation differs for sid={row['sid']}")
    return {**dict(entry), "ts": timestamp, "price": float(entry["price"])}


def _block(day, reason, sid=None, source_reason=None, entry=None, active=()):
    return dict(day=day, reason=reason, sid=sid, source_reason=source_reason,
                entry_ts=str(entry) if entry is not None else None,
                active_sids=json.dumps([int(x["sid"]) for x in active]))


def resolve_portfolio(orders: pd.DataFrame, evidence_by_sid: Mapping,
                      known_ledger: pd.DataFrame, *, days: list[str],
                      unavailable_days=(), cost_bps: float | None = None) -> dict:
    """Return exact resolved dates, their native ledger, and exclusion proofs.

    ``ledger`` excludes entire blocked/unavailable days. A rejected unresolved
    order remains in the successful-day ledger: observed entry, unknown source
    exit/return, false ``portfolio_executed``, and zero realized portfolio P&L.
    ``rejected_unknown`` contains its capital-state proof. ``blocked_days``
    explains every excluded date. No original object or baseline file changes.
    """
    frozen = baseline.frozen_config()
    costs = float(frozen["cost_bps"] if cost_bps is None else cost_bps)
    if not np.isfinite(costs) or costs < 0:
        raise ValueError("cost_bps must be finite and nonnegative")
    requested = [str(pd.Timestamp(day).date()) for day in days]
    if requested != sorted(set(requested)):
        raise ValueError("Requested days must be unique and chronological")
    unavailable = {str(pd.Timestamp(day).date()) for day in unavailable_days}
    config = execution.PortfolioConfig(portfolio_capital_rupees=frozen["portfolio_capital_rupees"],
                                       max_positions=frozen["max_positions"])
    config.validate()
    work = orders.copy(deep=True).reset_index(drop=True)
    if work.empty:
        allowed = [day for day in requested if day not in unavailable]
        empty = baseline.simulate(work, {}, cost_bps=costs)
        empty["source_outcome_known"] = pd.Series(dtype=bool)
        empty["portfolio_evidence_policy"] = pd.Series(dtype=str)
        return dict(version=EVIDENCE_VERSION, resolved_days=allowed, ledger=empty,
                    rejected_unknown=pd.DataFrame(columns=_PROOF_COLUMNS),
                    blocked_days=pd.DataFrame([_block(d, "UNAVAILABLE_SESSION") for d in requested
                                               if d in unavailable], columns=_BLOCK_COLUMNS),
                    audit=dict(policy=EVIDENCE_VERSION, requested_sessions=len(requested),
                               resolved_sessions=len(allowed), selected_orders=0, rejected_unknown_orders=0))
    required = {"sid", "day", "tradingsymbol", "confirmation_ts", "setup_id", "trigger", "side"}
    if required.difference(work):
        raise ValueError(f"Missing selected-order fields: {sorted(required.difference(work))}")
    if work.sid.duplicated().any() or not work.sid.map(lambda x: int(x) == x).all():
        raise ValueError("Selected signal IDs must be unique integers")
    work["sid"] = work.sid.astype(int)
    work["day"] = work.day.map(lambda x: str(pd.Timestamp(x).date()))
    if not set(work.day).issubset(requested):
        raise ValueError("Selected orders contain dates outside the requested calendar")
    work["confirmation_ts"] = work.confirmation_ts.map(_stamp)
    if not work.confirmation_ts.map(lambda t: str(t.date())).eq(work.day).all():
        raise ValueError("Selected-order confirmation date differs from its day")
    if not work.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Selected-order side must be LONG or SHORT")
    work["_priority"] = execution._picker_priority(work)
    if not np.isfinite(work._priority).all():
        raise ValueError("Nonfinite native portfolio priority")
    known = known_ledger.copy(deep=True)
    if len(known) and ("sid" not in known or known.sid.duplicated().any()):
        raise ValueError("Known-ledger signal IDs must be unique")
    known = known.set_index("sid", drop=False) if "sid" in known else pd.DataFrame()
    if len(known) and not set(known.index).issubset(set(work.sid)):
        raise ValueError("Known ledger contains unselected signal IDs")
    known_priorities = execution._picker_priority(known) if len(known) else pd.Series(dtype=float)
    capital = float(frozen["capital_per_entry_rupees"])
    exposure = capital * float(frozen["leverage_factor"])
    risk = exposure * execution.INITIAL_STOP_PCT / 100 + exposure * costs / 10000
    events, rows_by_sid, status = {}, {}, {}
    for row in work.to_dict("records"):
        sid = row["sid"]
        raw = evidence_by_sid.get(sid, evidence_by_sid.get(str(sid)))
        resolved, reason, proof = _evidence(raw, row)
        rows_by_sid[sid] = row
        status[sid] = dict(resolved=resolved, reason=reason, proof=proof)
        if not resolved:
            if sid in known.index:
                raise ValueError(f"Unresolved signal has a claimed known ledger: sid={sid}")
            if reason != "POSITION_OPEN_AT_FIRST_INTERRUPTION" or not isinstance(proof.get("entry"), Mapping):
                events[sid] = None
                continue
            entry = _entry(proof, row, require_contiguous=True, expiry=frozen["entry_expiry_minutes"])
            events[sid] = dict(row=row, entry=entry, exit_ts=None, capital=capital, exposure=exposure, risk=risk)
            continue
        if sid not in known.index:
            raise ValueError(f"Resolved signal missing from native known ledger: sid={sid}")
        record = known.loc[sid]
        for key in ("day", "tradingsymbol", "setup_id", "side"):
            if key not in record or str(record[key]) != str(row[key]):
                raise ValueError(f"Known ledger {key} differs for sid={sid}")
        if (_stamp(record["confirmation_ts"]) != row["confirmation_ts"]
                or not _close(record["trigger"], row["trigger"])):
            raise ValueError(f"Known ledger confirmation or trigger differs for sid={sid}")
        known_priority = known_priorities.loc[sid]
        if not _close(known_priority, row["_priority"]):
            raise ValueError(f"Known ledger native priority differs for sid={sid}")
        outcome = proof.get("outcome")
        if not isinstance(outcome, Mapping) or _truth(record["filled"]) != _truth(outcome.get("filled")):
            raise ValueError(f"Known ledger fill differs from execution proof for sid={sid}")
        if not _truth(record["filled"]):
            events[sid] = False
            continue
        entry = _entry(proof, row, require_contiguous=False, expiry=frozen["entry_expiry_minutes"])
        exit_ts = _stamp(record["exit_ts"])
        if (_stamp(record["entry_ts"]) != entry["ts"] or not _close(record["entry_price"], entry["price"])
                or exit_ts != _stamp(outcome["resolution_ts"]) or exit_ts < entry["ts"]
                or not _close(record["exit_price"], outcome["exit_price"])):
            raise ValueError(f"Known ledger execution differs from its proof for sid={sid}")
        expected = {"capital_per_entry_rupees": capital, "exposure_per_entry_rupees": exposure,
                    "cost_rupees": exposure * costs / 10000, "initial_stop_pct": execution.INITIAL_STOP_PCT}
        if any(key not in record or not _close(record[key], value) for key, value in expected.items()):
            raise ValueError(f"Known ledger capital, stop, or cost differs for sid={sid}")
        amounts = execution._trade_amounts(SimpleNamespace(**record.to_dict()))
        for field in ("net_profit_rupees", "pre_cost_profit_rupees"):
            if not np.isfinite(float(record[field])):
                raise ValueError(f"Known ledger has nonfinite P&L for sid={sid}")
        if not _close(record.net_profit_rupees, record.pre_cost_profit_rupees - record.cost_rupees):
            raise ValueError(f"Known ledger P&L arithmetic differs for sid={sid}")
        gross = (1 if row["side"] == "LONG" else -1) * (float(record.exit_price) / entry["price"] - 1) * exposure
        if not _close(record.pre_cost_profit_rupees, gross):
            raise ValueError(f"Known ledger P&L differs from proven entry/exit prices for sid={sid}")
        events[sid] = dict(row=row, entry=entry, exit_ts=exit_ts,
                           capital=amounts[0], exposure=amounts[1], risk=amounts[2])

    resolved_days, blocked, rejected, states = [], [], [], {}
    for day in requested:
        if day in unavailable:
            blocked.append(_block(day, "UNAVAILABLE_SESSION"))
            continue
        ids = work.loc[work.day.eq(day), "sid"].tolist()
        uncertain = [sid for sid in ids if events[sid] is None]
        if uncertain:
            sid = uncertain[0]
            blocked.append(_block(day, "UNKNOWN_ENTRY_OR_UNTRUSTWORTHY_PROOF", sid, status[sid]["reason"]))
            continue
        candidates = [events[sid] for sid in ids if events[sid] is not False]
        candidates.sort(key=lambda e: (e["entry"]["ts"], e["row"]["confirmation_ts"],
            str(e["row"]["setup_id"]), -e["row"]["_priority"], str(e["row"]["tradingsymbol"]), e["row"]["sid"]))
        active, day_rejected, day_states = [], [], {}
        for event in candidates:
            sid, entry = event["row"]["sid"], event["entry"]["ts"]
            active = [position for position in active if position["exit_ts"] > entry]
            reserved = float(sum(x["capital"] for x in active))
            gross = float(sum(x["exposure"] for x in active))
            open_risk = float(sum(x["risk"] for x in active))
            before = [len(active), reserved, gross, open_risk, event["capital"], event["exposure"], event["risk"]]
            rejection = ""
            if reserved + event["capital"] > config.portfolio_capital_rupees + 1e-9:
                rejection = "INSUFFICIENT_PORTFOLIO_CAPITAL"
            elif config.max_positions is not None and len(active) >= config.max_positions:
                rejection = "MAX_POSITIONS_REACHED"
            day_states[sid] = dict(executed=not bool(rejection), reason=rejection, state=before)
            if rejection:
                if not status[sid]["resolved"]:
                    proof = status[sid]["proof"]
                    day_rejected.append(dict(sid=sid, day=day, entry_ts=str(entry), reason=rejection,
                        active_sids=json.dumps([int(x["sid"]) for x in active]),
                        active_exit_timestamps=json.dumps([str(x["exit_ts"]) for x in active]),
                        reserved_capital_rupees=reserved, required_capital_rupees=event["capital"],
                        portfolio_capital_rupees=config.portfolio_capital_rupees,
                        source_prefix_sha256=proof.get("prefix_sha256"), source_evidence_version=proof.get("version")))
                continue
            if not status[sid]["resolved"]:
                blocked.append(_block(day, "ADMISSIBLE_ORDER_HAS_UNKNOWN_EXIT", sid,
                                      status[sid]["reason"], entry, active))
                break
            active.append(dict(sid=sid, exit_ts=event["exit_ts"], capital=event["capital"],
                               exposure=event["exposure"], risk=event["risk"]))
        else:
            resolved_days.append(day)
            rejected.extend(day_rejected)
            states.update(day_states)

    final_orders = work.loc[work.day.isin(resolved_days)].drop(columns="_priority").reset_index(drop=True)
    resolved_ids = [sid for sid in final_orders.sid if status[sid]["resolved"]]
    if resolved_ids:
        source = known.loc[resolved_ids].reset_index(drop=True).copy()
        # Exact frozen implementation provides every reported native state/P&L.
        native, _ = execution.apply_portfolio_constraints(source, config)
        native = native.set_index("sid", drop=False)
    else:
        native = pd.DataFrame()
    assembled = []
    for row in final_orders.to_dict("records"):
        sid = row["sid"]
        if status[sid]["resolved"]:
            record = native.loc[sid].to_dict()
            if _truth(record["filled"]):
                check = states[sid]
                if _truth(record["portfolio_executed"]) != check["executed"] or any(
                        not _close(record[key], value) for key, value in zip(_STATE, check["state"])):
                    raise RuntimeError(f"Portfolio proof differs from frozen execution for sid={sid}")
        else:
            entry = events[sid]["entry"]
            record = dict(row, filled=True, entry_ts=entry["ts"], entry_price=entry["price"],
                entry_path_index=entry["index"], entry_gap_through=bool(entry.get("gap_through", False)),
                entry_overshoot_bps=entry.get("overshoot_bps", np.nan), trigger_used=entry.get("trigger", row["trigger"]),
                exit_ts=pd.NaT, exit_price=np.nan, exit_reason="UNKNOWN_NOT_EXECUTED",
                net_return_pct=np.nan, gross_return_pct=np.nan, net_profit_rupees=np.nan,
                pre_cost_profit_rupees=np.nan, capital_per_entry_rupees=capital,
                leverage_factor=frozen["leverage_factor"], exposure_per_entry_rupees=exposure,
                initial_stop_pct=execution.INITIAL_STOP_PCT, cost_pct=costs / 100,
                cost_rupees=exposure * costs / 10000, portfolio_executed=False,
                portfolio_status="REJECTED", portfolio_reject_reason=states[sid]["reason"],
                portfolio_net_profit_rupees=0., portfolio_gross_profit_rupees=0., portfolio_cost_rupees=0.,
                portfolio_priority_value=rows_by_sid[sid]["_priority"])
            record.update(zip(_STATE, states[sid]["state"]))
        record.update(source_outcome_known=status[sid]["resolved"], portfolio_evidence_policy=EVIDENCE_VERSION,
                      portfolio_source_row=len(assembled))
        assembled.append(record)
    ledger = pd.DataFrame(assembled) if assembled else baseline.simulate(final_orders, {}, cost_bps=costs)
    if not assembled:
        ledger["source_outcome_known"] = pd.Series(dtype=bool)
        ledger["portfolio_evidence_policy"] = pd.Series(dtype=str)
    return dict(version=EVIDENCE_VERSION, resolved_days=resolved_days, ledger=ledger,
        rejected_unknown=pd.DataFrame(rejected, columns=_PROOF_COLUMNS),
        blocked_days=pd.DataFrame(blocked, columns=_BLOCK_COLUMNS),
        audit=dict(policy=EVIDENCE_VERSION, requested_sessions=len(requested), resolved_sessions=len(resolved_days),
            blocked_sessions=len(blocked), selected_orders=len(work), published_selected_orders=len(ledger),
            rejected_unknown_orders=len(rejected), cost_bps=costs, selection_unchanged=True,
            synthetic_exits=False, unknown_accepted_blocks_whole_day=True, source_hash_verification_required=True))
