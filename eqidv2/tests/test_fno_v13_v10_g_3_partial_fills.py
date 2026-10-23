"""Multi-share G3 recovery with an in-memory broker; real networking is forbidden."""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import json
import socket
import urllib.request
from types import SimpleNamespace

import pytest

import fno_live_entry_guard as entry_guard
from tests.test_fno_v13_v10_g_3_runtime import (
    DAY, candidate, config, runtime as base_runtime,
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Partial-fill tests must never access a real broker or network")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)


@pytest.fixture
def runtime(base_runtime, monkeypatch):
    monkeypatch.setattr(base_runtime, "_live_arm_state", lambda day: (True, "FAKE_ARM"))
    monkeypatch.setattr(base_runtime, "_kill_switch_enabled", lambda: False)
    monkeypatch.setattr(base_runtime.common, "runtime_dir",
                        lambda *parts: base_runtime.common.FNO_ROOT.parent.joinpath(*parts))
    return base_runtime


class MemoryBroker:
    def __init__(self):
        self.rows = {}
        self.placed = []
        self.cancelled = []
        self.cancel_pending = set()
        self.fill_on_cancel = {}
        self.lower_circuit_limit = 50.
        self.upper_circuit_limit = 150.

    def orders(self):
        return deepcopy(list(self.rows.values()))

    def positions(self):
        quantity = sum(row.get("filled_quantity", 0) * (1 if row.get("transaction_type") == "BUY" else -1)
                       for row in self.rows.values())
        return {"net": [dict(tradingsymbol="TEST", exchange="NSE", product="MIS", quantity=quantity)]}

    def order_history(self, order_id):
        return [deepcopy(self.rows[order_id])] if order_id in self.rows else []

    def quote(self, keys):
        return {key: dict(last_price=102.,
                          lower_circuit_limit=self.lower_circuit_limit,
                          upper_circuit_limit=self.upper_circuit_limit)
                for key in keys}

    def place_order(self, **kwargs):
        order_id = f"MEMORY{len(self.rows) + 1}"
        row = dict(kwargs, order_id=order_id,
                   status="TRIGGER PENDING" if kwargs["order_type"] == "SL-M" else "OPEN",
                   filled_quantity=0, pending_quantity=kwargs["quantity"], average_price=0.)
        self.rows[order_id] = row
        self.placed.append(deepcopy(row))
        return order_id

    def cancel_order(self, *, variety, order_id):
        self.cancelled.append(order_id)
        row = self.rows[order_id]
        if row["status"] in {"COMPLETE", "CANCELLED", "REJECTED"}:
            return order_id
        if order_id in self.fill_on_cancel:
            quantity, price = self.fill_on_cancel.pop(order_id)
            row.update(filled_quantity=quantity, average_price=price)
        if order_id not in self.cancel_pending:
            row.update(status="COMPLETE" if row["filled_quantity"] == row["quantity"] else "CANCELLED",
                       pending_quantity=0)
        return order_id

    def modify_order(self, **kwargs):
        self.rows[kwargs["order_id"]].update(kwargs)
        return kwargs["order_id"]


def pending_entry(runtime, *, status="CANCELLED", filled=100, side="LONG"):
    signal = runtime.select_entry_signals([candidate(side=side, ratio=1.3)], DAY, "09:30", capital_rs=100000., leverage=5.)[0]
    state = runtime.create_order_state(signal, "LIVE")
    state.update(entry_order_id="ENTRY", entry_order_type="MARKET")
    broker = MemoryBroker()
    broker.rows["ENTRY"] = dict(order_id="ENTRY", status=status, quantity=state["quantity"],
        filled_quantity=filled, average_price=(102. if side == "LONG" else 98.) if filled else 0.,
        pending_quantity=state["quantity"]-filled if status == "OPEN" else 0,
        order_type="MARKET", transaction_type="BUY" if side == "LONG" else "SELL",
        exchange="NSE", product="MIS",
        tradingsymbol=state["tradingsymbol"], tag=runtime._live_order_tag(state["signal_id"], "entry"))
    return signal, state, broker, config.slot_datetime(DAY, "09:32")


def step(runtime, state, broker, at, count=3):
    for index in range(count):
        runtime.advance_live_order(state, broker, at + timedelta(seconds=index), last_price=102.)


@pytest.mark.parametrize("status", ["CANCELLED", "REJECTED"])
def test_terminal_partial_entry_keeps_filled_shares_protected(runtime, status):
    signal, state, broker, at = pending_entry(runtime, status=status)
    original = state["requested_quantity"]
    step(runtime, state, broker, at)
    assert state["status"] == "OPEN"
    assert state["quantity"] == 100
    assert state["requested_quantity"] == original == 490
    assert state["sizing_reference_price"] == 102.
    assert broker.rows[state["stop_order_id"]]["quantity"] == 100
    assert broker.rows[state["target_order_id"]]["quantity"] == 100
    runtime._validate_order_state(state, signal, "LIVE")


def test_working_partial_entry_cancels_remainder_then_uses_final_fill(runtime):
    _, state, broker, at = pending_entry(runtime, status="OPEN")
    broker.fill_on_cancel["ENTRY"] = (150, 102.1)
    step(runtime, state, broker, at)
    assert "ENTRY" in broker.cancelled
    assert state["status"] == "OPEN"
    assert (state["quantity"], state["entry_price"]) == (150, 102.1)
    assert state["requested_quantity"] == 490
    assert broker.rows[state["stop_order_id"]]["quantity"] == 150
    assert broker.rows[state["target_order_id"]]["quantity"] == 150


def test_working_partial_entry_awaits_cancel_confirmation(runtime):
    _, state, broker, at = pending_entry(runtime, status="OPEN")
    broker.cancel_pending.add("ENTRY")
    step(runtime, state, broker, at)
    assert "ENTRY" in broker.cancelled
    assert state["status"] == "PENDING_ENTRY"
    assert not broker.placed
    broker.cancel_pending.clear()
    step(runtime, state, broker, at + timedelta(seconds=4))
    assert state["status"] == "OPEN"
    assert state["quantity"] == 100


def opened(runtime):
    signal, state, broker, at = pending_entry(runtime, status="COMPLETE", filled=490)
    step(runtime, state, broker, at, count=1)
    assert state["status"] == "OPEN"
    return signal, state, broker, at


def fresh_entry(runtime, side="LONG"):
    signal = runtime.select_entry_signals(
        [candidate(side=side, ratio=1.3)], DAY, "09:30",
        capital_rs=100000., leverage=5.,
    )[0]
    state = runtime.create_order_state(signal, "LIVE")
    return signal, state, MemoryBroker(), config.slot_datetime(DAY, "09:32")


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("price_role", ["stop", "target"])
def test_g3_preentry_blocks_exit_outside_current_circuit_band(runtime, side, price_role):
    _, state, broker, at = fresh_entry(runtime, side)
    price = float(state[f"{price_role}_price"])
    if (side, price_role) in {("LONG", "target"), ("SHORT", "stop")}:
        broker.upper_circuit_limit = price - .05
    else:
        broker.lower_circuit_limit = price + .05

    runtime.advance_live_order(
        state, broker, at, last_price=float(state["trigger_price"])
    )

    assert state["status"] == "CANCELLED"
    assert state["entry_terminal_cause"] == "EXIT_PRICE_BAND_BLOCKED"
    assert not state.get("entry_order_id")
    assert broker.placed == []


def test_g3_preentry_waits_when_quote_has_no_circuit_band(runtime, monkeypatch):
    _, state, broker, at = fresh_entry(runtime)
    monkeypatch.setattr(
        broker, "quote", lambda keys: {key: {"last_price": 102.} for key in keys}
    )

    runtime.advance_live_order(state, broker, at, last_price=102.)

    assert state["status"] == "PENDING_ENTRY"
    assert state["status_reason"] == "WAITING_FOR_EXIT_PRICE_BAND_QUOTE"
    assert not state.get("entry_order_id")
    assert broker.placed == []


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_g3_valid_circuit_band_allows_entry_and_both_exit_orders(runtime, monkeypatch, side):
    monkeypatch.setattr(entry_guard, "entry_authority", lambda profile: (True, "OFFLINE_TEST"))
    _, state, broker, at = fresh_entry(runtime, side)
    runtime.advance_live_order(
        state, broker, at, last_price=float(state["trigger_price"])
    )
    assert state["entry_order_id"]
    entry = broker.rows[state["entry_order_id"]]
    entry.update(
        status="COMPLETE", filled_quantity=int(state["quantity"]),
        pending_quantity=0, average_price=float(state["trigger_price"]),
    )

    runtime.advance_live_order(
        state, broker, at + timedelta(seconds=1),
        last_price=float(state["trigger_price"]),
    )

    assert state["status"] == "OPEN"
    assert state["stop_order_id"] and state["target_order_id"]
    assert state["bracket_confirmed"] is True
    assert broker.rows[state["stop_order_id"]]["quantity"] == state["quantity"]
    assert broker.rows[state["target_order_id"]]["quantity"] == state["quantity"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_definite_stop_placement_rejection_squares_off_without_target(
    runtime, monkeypatch, side
):
    """A filled G3 entry cannot continue after its protective stop is rejected."""

    class InputException(Exception):
        code = 400

    _, state, broker, at = pending_entry(
        runtime, status="COMPLETE", filled=490, side=side
    )
    original_place = broker.place_order
    stop_attempts = []

    def reject_stop(**kwargs):
        if kwargs["order_type"] == "SL-M":
            stop_attempts.append(deepcopy(kwargs))
            raise InputException("Stop trigger is outside the circuit limit")
        return original_place(**kwargs)

    monkeypatch.setattr(broker, "place_order", reject_stop)
    runtime.advance_live_order(state, broker, at, last_price=102.)

    assert len(stop_attempts) == 1
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert state["bracket_confirmed"] is False
    assert not state.get("stop_order_id")
    assert not state.get("target_order_id")
    assert state["exit_bracket_failure"] == "STOP_ORDER_PLACE_REJECTED"
    assert not [row for row in broker.placed if row["order_type"] == "LIMIT"]
    squareoff_id = state["squareoff_order_id"]
    assert broker.rows[squareoff_id]["quantity"] == 490
    assert broker.rows[squareoff_id]["transaction_type"] == (
        "SELL" if side == "LONG" else "BUY"
    )

    runtime.advance_live_order(state, broker, at + timedelta(seconds=1))
    assert state["squareoff_order_id"] == squareoff_id
    assert len(stop_attempts) == 1
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 1


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_ambiguous_stop_placement_recovers_by_tag_without_duplicate(
    runtime, monkeypatch, side
):
    _, state, broker, at = pending_entry(
        runtime, status="COMPLETE", filled=490, side=side
    )
    original_place = broker.place_order
    stop_attempts = []

    def place_stop_but_lose_response(**kwargs):
        if kwargs["order_type"] == "SL-M":
            stop_attempts.append(deepcopy(kwargs))
            original_place(**kwargs)
            raise TimeoutError("Broker accepted stop, response lost")
        return original_place(**kwargs)

    monkeypatch.setattr(broker, "place_order", place_stop_but_lose_response)
    with pytest.raises(TimeoutError, match="response lost"):
        runtime.advance_live_order(state, broker, at, last_price=102.)

    stop_id = next(
        order_id for order_id, row in broker.rows.items()
        if row["order_type"] == "SL-M"
    )
    persisted = json.loads(
        runtime._order_path(DAY, "LIVE", state["signal_id"]).read_text(encoding="utf-8")
    )
    assert persisted["stop_submission_pending"] is True
    assert not persisted.get("stop_order_id")
    assert state["stop_submission_pending"] is True
    assert not [row for row in broker.placed if row["order_type"] == "LIMIT"]

    runtime.advance_live_order(state, broker, at + timedelta(seconds=1), last_price=102.)

    assert state["status"] == "OPEN"
    assert state["stop_order_id"] == stop_id
    assert state["stop_submission_pending"] is False
    assert state["target_order_id"]
    assert state["bracket_confirmed"] is True
    assert len(stop_attempts) == 1
    assert len([row for row in broker.placed if row["order_type"] == "SL-M"]) == 1


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_definite_target_limit_rejection_squares_off_once(runtime, monkeypatch, side):
    """A filled G3 entry cannot remain OPEN with only its stop after target rejection."""

    class InputException(Exception):
        code = 400

    _, state, broker, at = pending_entry(
        runtime, status="COMPLETE", filled=490, side=side
    )
    original_place = broker.place_order
    target_attempts = []

    def reject_target(**kwargs):
        if kwargs["order_type"] == "LIMIT":
            target_attempts.append(deepcopy(kwargs))
            raise InputException("Price is outside the current circuit limit")
        return original_place(**kwargs)

    monkeypatch.setattr(broker, "place_order", reject_target)
    runtime.advance_live_order(state, broker, at, last_price=102.)

    assert len(target_attempts) == 1
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert state["bracket_confirmed"] is False
    assert state["stop_order_id"] in broker.cancelled
    assert not state.get("target_order_id")
    squareoff_id = state["squareoff_order_id"]
    assert squareoff_id
    assert broker.rows[squareoff_id]["order_type"] == "MARKET"
    assert broker.rows[squareoff_id]["quantity"] == 490
    assert broker.rows[squareoff_id]["transaction_type"] == (
        "SELL" if side == "LONG" else "BUY"
    )

    runtime.advance_live_order(state, broker, at + timedelta(seconds=1), last_price=102.)

    assert state["status"] == "SQUARE_OFF_PENDING"
    assert state["bracket_confirmed"] is False
    assert state["squareoff_order_id"] == squareoff_id
    assert len(target_attempts) == 1
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 1


def test_target_rejected_after_broker_ack_squares_off_once(runtime):
    _, state, broker, at = opened(runtime)
    target_id = state["target_order_id"]
    broker.rows[target_id].update(status="REJECTED", pending_quantity=0)

    runtime.advance_live_order(state, broker, at + timedelta(seconds=1))

    assert state["status"] == "SQUARE_OFF_PENDING"
    assert state["bracket_confirmed"] is False
    assert state["stop_order_id"] in broker.cancelled
    squareoff_id = state["squareoff_order_id"]
    assert squareoff_id
    runtime.advance_live_order(state, broker, at + timedelta(seconds=2))
    assert state["squareoff_order_id"] == squareoff_id
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 1


@pytest.mark.parametrize("target_visible_on_restart", [True, False])
def test_ambiguous_target_placement_recovers_without_duplicate(
    runtime, monkeypatch, target_visible_on_restart
):
    _, state, broker, at = pending_entry(runtime, status="COMPLETE", filled=490)
    original_place = broker.place_order
    target_attempts = []

    def place_but_lose_response(**kwargs):
        if kwargs["order_type"] == "LIMIT":
            target_attempts.append(deepcopy(kwargs))
            original_place(**kwargs)
            raise TimeoutError("Broker accepted target, response lost")
        return original_place(**kwargs)

    monkeypatch.setattr(broker, "place_order", place_but_lose_response)
    with pytest.raises(TimeoutError, match="response lost"):
        runtime.advance_live_order(state, broker, at, last_price=102.)

    target_id = next(
        order_id for order_id, row in broker.rows.items()
        if row["order_type"] == "LIMIT"
    )
    persisted = json.loads(
        runtime._order_path(DAY, "LIVE", state["signal_id"]).read_text(encoding="utf-8")
    )
    assert persisted["target_submission_pending"] is True
    assert not persisted.get("target_order_id")
    assert state["target_submission_pending"] is True
    assert state["stop_order_id"]
    assert len(target_attempts) == 1

    if not target_visible_on_restart:
        original_orders = broker.orders
        monkeypatch.setattr(
            broker, "orders",
            lambda: [row for row in original_orders() if row["order_id"] != target_id],
        )

    runtime.advance_live_order(state, broker, at + timedelta(seconds=1), last_price=102.)

    assert len(target_attempts) == 1
    assert len([row for row in broker.placed if row["order_type"] == "LIMIT"]) == 1
    if target_visible_on_restart:
        assert state["target_order_id"] == target_id
        assert state["target_submission_pending"] is False
        assert state["bracket_confirmed"] is True
        assert state["status"] == "OPEN"
    else:
        assert state["status"] == "OPEN"
        assert state["bracket_confirmed"] is False
        assert state["target_submission_pending"] is True
        assert not state.get("target_order_id")
        assert state["status_reason"] == "TARGET_SUBMISSION_UNCERTAIN_AWAITING_RECONCILIATION"
        assert state["exit_bracket_failure"] == "TARGET_SUBMISSION_UNCERTAIN"


def test_g3_full_quote_fails_over_to_second_kite_app(runtime, monkeypatch):
    first = MemoryBroker()
    second = MemoryBroker()
    monkeypatch.setattr(first, "quote", lambda keys: (_ for _ in ()).throw(TimeoutError("app1 quote timeout")))
    credentials = [
        SimpleNamespace(app_name=name, api_key=f"key-{name}", access_token=f"token-{name}")
        for name in ("app1", "app2")
    ]
    clients = {"app1": first, "app2": second}
    pool = runtime.KitePool(
        2, 1,
        credential_loader=lambda **_kwargs: credentials,
        client_factory=lambda item, **_kwargs: clients[item.app_name],
    )

    quote = pool.quote(["NSE:TEST"])

    assert quote["NSE:TEST"]["lower_circuit_limit"] == 50.
    assert quote["NSE:TEST"]["upper_circuit_limit"] == 150.
    assert pool.last_operation == "quote"
    assert pool.last_operation_app == "app2"
    assert len(pool.last_operation_failures) == 1
    assert pool.last_operation_failures[0]["app"] == "app1"


@pytest.mark.parametrize("role,fill_price", [("target", 103.), ("stop", 100.)])
@pytest.mark.parametrize("broker_status", ["OPEN", "CANCELLED"])
def test_ordinary_partial_exit_squareoff_uses_only_residual(runtime, monkeypatch, role, fill_price, broker_status):
    _, state, broker, at = opened(runtime)
    broker.rows[state[f"{role}_order_id"]].update(status=broker_status, filled_quantity=100, average_price=fill_price)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    step(runtime, state, broker, at + timedelta(seconds=4))
    squareoff = broker.rows[state["squareoff_order_id"]]
    assert squareoff["quantity"] == 390
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 1
    config.live_execution_policy.validate_order_sizing(state)


def test_two_exit_partials_and_cancel_race_are_counted_once(runtime, monkeypatch):
    _, state, broker, at = opened(runtime)
    broker.rows[state["target_order_id"]].update(status="OPEN", filled_quantity=100, average_price=103.)
    broker.rows[state["stop_order_id"]].update(status="OPEN", filled_quantity=80, average_price=100.)
    broker.fill_on_cancel[state["target_order_id"]] = (150, 103.)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    step(runtime, state, broker, at + timedelta(seconds=4))
    squareoff = broker.rows[state["squareoff_order_id"]]
    assert squareoff["quantity"] == 260
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 1
    squareoff.update(status="COMPLETE", filled_quantity=260, average_price=101.)
    step(runtime, state, broker, at + timedelta(seconds=8))
    assert state["status"] == "CLOSED"
    expected_exit = (150*103. + 80*100. + 260*101.) / 490
    assert state["exit_price"] == pytest.approx(expected_exit)
    assert state["gross_pnl_rs"] == pytest.approx((expected_exit-102.) * 490)


def test_squareoff_waits_for_both_exit_cancellations(runtime, monkeypatch):
    _, state, broker, at = opened(runtime)
    target_id = state["target_order_id"]
    broker.rows[target_id].update(status="OPEN", filled_quantity=100, average_price=103.)
    broker.cancel_pending.add(target_id)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    step(runtime, state, broker, at + timedelta(seconds=4))
    assert not state.get("squareoff_order_id")
    assert not [row for row in broker.placed if row["order_type"] == "MARKET"]
    broker.cancel_pending.clear()
    step(runtime, state, broker, at + timedelta(seconds=8))
    assert broker.rows[state["squareoff_order_id"]]["quantity"] == 390


def test_target_fills_completely_during_cancel_does_not_reverse_position(runtime, monkeypatch):
    _, state, broker, at = opened(runtime)
    target_id = state["target_order_id"]
    broker.fill_on_cancel[target_id] = (490, 103.)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    step(runtime, state, broker, at + timedelta(seconds=4))
    assert state["status"] == "CLOSED"
    assert not [row for row in broker.placed if row["order_type"] == "MARKET"]
    assert state["gross_pnl_rs"] == pytest.approx(490.)


def test_reconciliation_uses_unclosed_residual_and_original_active_order_quantity(runtime, monkeypatch):
    _, state, broker, at = opened(runtime)
    broker.rows[state["target_order_id"]].update(status="OPEN", filled_quantity=100, average_price=103.)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    step(runtime, state, broker, at + timedelta(seconds=4))
    squareoff = broker.rows[state["squareoff_order_id"]]
    squareoff.update(filled_quantity=50, average_price=101., pending_quantity=340)
    step(runtime, state, broker, at + timedelta(seconds=8))
    report = runtime._publish_broker_position_reconciliation([state], broker, DAY)
    assert report["mismatch_count"] == 0
    assert report["active_order_parity_complete"] is True, report["active_order_mismatches"]
    assert report["local_expected_active_order_count"] == 1
    assert runtime._g3_exit_residual(state) == 340
    assert squareoff["quantity"] == 390


def test_reconciliation_accepts_broker_protected_sl_normalization(runtime):
    _, state, broker, _ = opened(runtime)
    stop = broker.rows[state["stop_order_id"]]
    assert stop["order_type"] == "SL-M"
    # Kite exposes a market-protected SL-M as an SL carrying a calculated
    # limit price. The ID, tag, trigger, side and quantity remain authoritative.
    stop.update(order_type="SL", price=104.0, status="TRIGGER PENDING")

    report = runtime._publish_broker_position_reconciliation(
        [state], broker, DAY
    )

    assert report["mismatch_count"] == 0
    assert report["active_order_parity_complete"] is True, report[
        "active_order_mismatches"
    ]


def test_cancelled_partial_market_exit_retries_only_confirmed_residual(runtime, monkeypatch):
    _, state, broker, at = opened(runtime)
    broker.rows[state["target_order_id"]].update(status="OPEN", filled_quantity=100, average_price=103.)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    step(runtime, state, broker, at + timedelta(seconds=4))
    first = broker.rows[state["squareoff_order_id"]]
    first.update(status="CANCELLED", filled_quantity=50, average_price=101., pending_quantity=0)
    step(runtime, state, broker, at + timedelta(seconds=8))
    second = broker.rows[state["squareoff_order_id"]]
    assert second["order_id"] != first["order_id"]
    assert second["tag"] != first["tag"]
    assert second["quantity"] == 340
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 2
    report = runtime._publish_broker_position_reconciliation([state], broker, DAY)
    assert report["mismatch_count"] == 0
    assert report["active_order_parity_complete"] is True, report["active_order_mismatches"]
    second.update(status="COMPLETE", filled_quantity=340, average_price=100., pending_quantity=0)
    step(runtime, state, broker, at + timedelta(seconds=12))
    assert state["status"] == "CLOSED"
    assert state["exit_price"] == pytest.approx((100*103. + 50*101. + 340*100.) / 490)


def test_confirmed_rejected_market_exit_retries_are_bounded(runtime, monkeypatch):
    _, state, broker, at = opened(runtime)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    step(runtime, state, broker, at + timedelta(seconds=4))
    for attempt in range(3):
        broker.rows[state["squareoff_order_id"]].update(status="REJECTED", pending_quantity=0)
        step(runtime, state, broker, at + timedelta(seconds=8 + attempt * 4))
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert "MANUAL_RECONCILIATION_REQUIRED" in state["status_reason"]
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 3


def test_unknown_market_acceptance_never_resubmits_when_broker_list_is_delayed(runtime, monkeypatch):
    _, state, broker, at = opened(runtime)
    attempts = []
    def ambiguous(**kwargs):
        attempts.append(kwargs)
        raise runtime.BrokerMutationUncertain("Fake response lost after broker may have accepted")
    monkeypatch.setattr(broker, "place_order", ambiguous)
    monkeypatch.setattr(runtime, "_kill_switch_enabled", lambda: True)
    for index in range(4):
        try:
            runtime.advance_live_order(state, broker, at + timedelta(seconds=4+index), last_price=102.)
        except runtime.BrokerMutationUncertain:
            pass
    assert len(attempts) == 1
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert not state.get("squareoff_order_id")


@pytest.mark.parametrize("stale_state_exists", [False, True])
def test_worker_blocks_stale_unsubmitted_row_without_stranding_legacy_position(runtime, monkeypatch, stale_state_exists):
    signal, state, broker, at = pending_entry(runtime, status="COMPLETE", filled=1)
    state.update(status="OPEN", quantity=1, strategy_sized_quantity=1,
        execution_quantity_override=1, entry_price=102., entry_at_ist=at.isoformat(),
        capital_rs=100000., leverage=5., target_exposure_rs=500000.)
    for key in config.live_execution_policy.execution_metadata():
        state.pop(key, None)
    state.update(quantity_policy="FIXED_ONE_SHARE", leverage=5., target_exposure_rs=500000.)
    broker.rows["ENTRY"]["quantity"] = 1
    for key in config.live_execution_policy.signal_metadata():
        signal.pop(key, None)
    signal["live_sizing"] = {**signal["paper_sizing"], "quantity": 1,
                             "estimated_exposure_rs": 102., "state": "LIVE_QUANTITY_ONE"}
    stale_signal = {**deepcopy(signal), "signal_id": signal["signal_id"] + "_stale"}
    stale_state = {**deepcopy(state), "signal_id": stale_signal["signal_id"],
                   "status": "PENDING_ENTRY", "entry_order_id": "", "entry_price": 0.}
    snapshots = {state["signal_id"]: state}
    if stale_state_exists:
        snapshots[stale_state["signal_id"]] = stale_state
    monkeypatch.setattr(runtime, "load_signals", lambda *_: [stale_signal, signal])
    monkeypatch.setattr(runtime, "_blocking_pipeline_issue", lambda *_: None)
    monkeypatch.setattr(runtime, "_read_json", lambda path: deepcopy(snapshots.get(path.stem, {})))
    monkeypatch.setattr(runtime.common, "now_ist", lambda: at)
    monkeypatch.setattr(runtime, "_observability_runtime", lambda: None)
    monkeypatch.setattr(runtime, "_render_worker_report", lambda *_: "offline fixture")
    broker.last_operation = broker.last_operation_app = ""
    broker.last_operation_failures = []
    broker.credential_reload_count = 0
    monkeypatch.setattr(runtime, "KitePool", lambda *_: broker)
    published = []
    monkeypatch.setattr(runtime, "_publish", lambda *args, **kwargs: published.append((args, kwargs)))
    args = SimpleNamespace(execution_mode="LIVE", live_quantity=None, max_apps=1, timeout_sec=1., once=True)
    assert runtime.run_worker(args, DAY, "LONG") == 2
    assert published[-1][0][1] == "DEGRADED"
    assert published[-1][1]["error_count"] >= 1
    assert len(broker.placed) == 2
    assert all(row["quantity"] == 1 and row["transaction_type"] == "SELL" for row in broker.placed)
