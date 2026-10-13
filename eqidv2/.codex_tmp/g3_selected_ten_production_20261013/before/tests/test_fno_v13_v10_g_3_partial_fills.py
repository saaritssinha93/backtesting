"""Multi-share G3 recovery with an in-memory broker; real networking is forbidden."""
from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import socket
import urllib.request
from types import SimpleNamespace

import pytest

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

    def orders(self):
        return deepcopy(list(self.rows.values()))

    def positions(self):
        quantity = sum(row.get("filled_quantity", 0) * (1 if row.get("transaction_type") == "BUY" else -1)
                       for row in self.rows.values())
        return {"net": [dict(tradingsymbol="TEST", exchange="NSE", product="MIS", quantity=quantity)]}

    def order_history(self, order_id):
        return [deepcopy(self.rows[order_id])] if order_id in self.rows else []

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


def pending_entry(runtime, *, status="CANCELLED", filled=100):
    signal = runtime.select_entry_signals([candidate()], DAY, "09:30", capital_rs=100000., leverage=5.)[0]
    state = runtime.create_order_state(signal, "LIVE")
    state.update(entry_order_id="ENTRY", entry_order_type="MARKET")
    broker = MemoryBroker()
    broker.rows["ENTRY"] = dict(order_id="ENTRY", status=status, quantity=state["quantity"],
        filled_quantity=filled, average_price=102. if filled else 0.,
        pending_quantity=state["quantity"]-filled if status == "OPEN" else 0,
        order_type="MARKET", transaction_type="BUY", exchange="NSE", product="MIS",
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
