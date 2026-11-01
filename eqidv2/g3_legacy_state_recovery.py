"""Validate saved prior G3 positions for protection, never for new entry.

By default only the exact V12/V13/V14 contracts preceding the all-slot selection
are recognized. The protective worker can also opt into the exact current
contract when recovering a position independently of the entry signal feed.
Records are never relabeled and recovery cannot create an entry.
"""
from __future__ import annotations

from datetime import date
import math


def archived_contract(state):
    import fno_v13_v10_g_3_contract as v12
    from research import archived_g3_production_v13 as v13
    from research import archived_g3_production_v14 as v14
    for contract in (v14, v13, v12):
        if (state.get("strategy_version"), state.get("strategy_fingerprint")) == (
                contract.STRATEGY_VERSION, contract.strategy_fingerprint()):
            return contract
    return None


def validate_protective_state(state, session_date, mode, *, allow_current=False):
    contract = archived_contract(state)
    if contract is None and allow_current:
        import fno_v13_v10_g_3_cutoff_production as current
        if (state.get("strategy_version"), state.get("strategy_fingerprint")) == (
                current.STRATEGY_VERSION, current.strategy_fingerprint()):
            contract = current
    if contract is None:
        raise ValueError("Unrecognized archived G3 position identity")
    if state.get("mode") != mode or state.get("session_date") != str(session_date):
        raise ValueError("Archived G3 position mode/date mismatch")
    status = state.get("status")
    active = status in {"OPEN", "SQUARE_OFF_PENDING"}
    pending_broker = mode == "LIVE" and status == "PENDING_ENTRY" and (
        bool(state.get("entry_order_id")) or bool(state.get("entry_submission_uncertain"))
        or int(state.get("entry_submission_attempt_count", 0)) > 0)
    if not active and not pending_broker:
        raise ValueError("Archived G3 recovery cannot create a new entry")
    setup = contract.setup_for(str(state.get("signal_end", "")), str(state.get("side", "")))
    if setup is None or state.get("setup_id") != setup.setup_id:
        raise ValueError("Archived G3 setup is not in its original contract")
    expected = contract.canonical_signal_id(contract.STRATEGY_VERSION,
        date.fromisoformat(str(session_date)), setup.signal_end, setup.confirmation_end,
        setup.side, str(state.get("tradingsymbol", "")))
    if state.get("signal_id") != expected or state.get("confirmation_end") != setup.confirmation_end:
        raise ValueError("Archived G3 position deterministic identity mismatch")
    for key, expected_value in (("stop_pct", setup.stop_pct), ("target_pct", setup.target_pct)):
        if not math.isclose(float(state.get(key, math.nan)), expected_value, abs_tol=1e-9):
            raise ValueError("Archived G3 position changed its original " + key)
    if state.get("exchange") != "NSE" or int(state.get("lot_size", 0)) != 1:
        raise ValueError("Archived G3 position instrument mismatch")
    quantity = float(state.get("quantity", math.nan))
    if not math.isfinite(quantity) or not quantity.is_integer() or quantity < 0:
        raise ValueError("Archived G3 position quantity is invalid")
    if mode == "LIVE":
        from fno_v13_v10_g_3_live_execution_policy import validate_existing_order_sizing
        validate_existing_order_sizing(state)
        if active and not state.get("entry_order_id"):
            raise ValueError("Archived LIVE position lacks broker entry evidence")
    elif mode == "PAPER":
        trigger = float(state.get("trigger_price", math.nan))
        if not math.isfinite(trigger) or trigger <= 0:
            raise ValueError("Archived PAPER position trigger is invalid")
        if state.get("capital_rs") != 100000. or state.get("leverage") != 5.:
            raise ValueError("Archived PAPER position sizing mismatch")
        if quantity != math.floor(500000. / trigger):
            raise ValueError("Archived PAPER position quantity mismatch")
    else:
        raise ValueError("Unsupported archived position mode")
    if active:
        if not math.isfinite(float(state.get("entry_price", math.nan))) or float(state["entry_price"]) <= 0:
            raise ValueError("Archived filled position has no valid entry price")
        from datetime import datetime
        entered = datetime.fromisoformat(state["entry_at_ist"])
        if entered.tzinfo is None or entered.date() != session_date:
            raise ValueError("Archived filled position entry clock mismatch")
    return state
