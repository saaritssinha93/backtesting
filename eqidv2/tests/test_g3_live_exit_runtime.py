"""G3 end-of-day execution checks using only an in-memory broker."""
from __future__ import annotations

from datetime import timedelta
import json
import socket
from types import SimpleNamespace
import urllib.request

import pytest

from tests.test_fno_v13_v10_g_3_partial_fills import (
    DAY,
    MemoryBroker,
    base_runtime,
    config,
    fresh_entry,
    opened,
    pending_entry,
    runtime,
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("G3 end-of-day tests must not access a real broker or network")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)


@pytest.fixture
def clock_only(runtime, monkeypatch):
    # Isolate the end-of-day trigger from the separately tested staged stops.
    monkeypatch.setattr(runtime, "_staged_stop_target", lambda *_args: None)
    return runtime


def market_exits(broker):
    return [row for row in broker.placed if row["order_type"] == "MARKET"]


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_live_cutoff_exits_at_1505_and_never_duplicates_after_restart(clock_only, side):
    runtime = clock_only
    _, state, broker, _ = opened(runtime, side=side)
    cutoff = config.slot_datetime(DAY, "15:05")

    runtime.advance_live_order(state, broker, cutoff - timedelta(seconds=1))
    assert state["status"] == "OPEN"
    assert not market_exits(broker)

    runtime.advance_live_order(state, broker, cutoff)
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert broker.rows[state["stop_order_id"]]["status"] == "CANCELLED"
    assert broker.rows[state["target_order_id"]]["status"] == "CANCELLED"
    exit_id = state["squareoff_order_id"]
    exit_order = broker.rows[exit_id]
    assert exit_order["quantity"] == state["quantity"]
    assert exit_order["transaction_type"] == ("SELL" if side == "LONG" else "BUY")
    assert len(market_exits(broker)) == 1
    assert isinstance(state["g3_live_exit_schedule"], dict)
    assert state["g3_live_exit_schedule"]

    state = json.loads(runtime._order_path(DAY, "LIVE", state["signal_id"]).read_text())
    for seconds in (1, 3, 10):
        runtime.advance_live_order(state, broker, cutoff + timedelta(seconds=seconds))
    assert state["squareoff_order_id"] == exit_id
    assert len(market_exits(broker)) == 1

    exit_order.update(status="COMPLETE", filled_quantity=state["quantity"],
                      pending_quantity=0, average_price=state["entry_price"])
    runtime.advance_live_order(state, broker, cutoff + timedelta(seconds=11))
    assert state["status"] == "CLOSED"
    assert state["remaining_quantity"] == 0
    assert broker.positions()["net"][0]["quantity"] == 0
    assert len(market_exits(broker)) == 1


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_cutoff_entry_cancel_race_exits_only_final_acquired_quantity(clock_only, side):
    runtime = clock_only
    _, state, broker, _ = pending_entry(runtime, status="OPEN", filled=0, side=side)
    requested = state["quantity"]
    price = 102. if side == "LONG" else 98.
    broker.fill_on_cancel["ENTRY"] = (150, price)

    runtime.advance_live_order(state, broker, config.slot_datetime(DAY, "15:05"))

    assert state["status"] == "SQUARE_OFF_PENDING"
    assert state["quantity"] == 150 < requested
    assert state["requested_quantity"] == requested
    assert broker.rows["ENTRY"]["status"] == "CANCELLED"
    assert broker.rows[state["squareoff_order_id"]]["quantity"] == 150
    assert len(market_exits(broker)) == 1


def test_live_cutoff_honours_an_earlier_strategy_exit(clock_only, monkeypatch):
    runtime = clock_only
    _, state, broker, _ = opened(runtime)
    monkeypatch.setattr(runtime.config, "SQUARE_OFF", "14:55")
    runtime.advance_live_order(state, broker, config.slot_datetime(DAY, "14:55"))
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert len(market_exits(broker)) == 1


@pytest.mark.parametrize("clock", ["15:05", "15:06"])
def test_low_level_entry_submission_is_blocked_at_protective_cutoff(runtime, monkeypatch, clock):
    _, state, broker, _ = fresh_entry(runtime)
    monkeypatch.setattr(runtime, "admit_live_entry", lambda *_args, **_kwargs:
                        pytest.fail("Late entry reached portfolio admission"))

    with pytest.raises(ValueError, match="protective square-off"):
        runtime._place_live_entry_order(
            state, broker, config.slot_datetime(DAY, clock), transaction_type="BUY",
            tag=runtime._live_order_tag(state["signal_id"], "entry"),
            order_type="MARKET", reason="TEST_LATE_ENTRY",
        )
    assert not broker.placed
    assert state["entry_submission_attempt_count"] == 0


def test_paper_strategy_keeps_1515_exit(clock_only):
    runtime = clock_only
    signal, _, _, _ = opened(runtime)
    state = runtime.create_order_state(signal, "PAPER")
    price = float(state["trigger_price"])
    runtime.advance_paper_order(state, price, config.slot_datetime(DAY, "09:27"))
    assert state["status"] == "OPEN"

    for clock in ("15:05", "15:10"):
        runtime.advance_paper_order(state, price, config.slot_datetime(DAY, clock))
        assert state["status"] == "OPEN"
    runtime.advance_paper_order(state, price, config.slot_datetime(DAY, "15:15"))
    assert config.SQUARE_OFF == "15:15"
    assert state["status"] == "CLOSED"
    assert state["exit_reason"] == "SQUARE_OFF"


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_end_of_day_reconciliation_requires_flat_not_just_matching_positions(runtime, monkeypatch, side):
    _, state, broker, _ = opened(runtime, side=side)
    monkeypatch.setattr(runtime.common, "now_ist", lambda: config.slot_datetime(DAY, "15:10"))
    result = runtime._publish_broker_position_reconciliation([state], broker, DAY)
    assert result["mismatch_count"] == 0
    assert result["active_order_parity_complete"] is True
    assert result["snapshot_fresh"] is True
    assert result["verification_due"] is True
    assert result["all_flat"] is False
    assert result["broker_nonzero_positions"]
    assert result["local_nonzero_positions"]
    signed_quantity = state["quantity"] * (1 if side == "LONG" else -1)
    assert result["broker_nonzero_positions"][0]["quantity"] == signed_quantity
    assert result["local_nonzero_positions"][0]["quantity"] == signed_quantity
    assert result["live_exit_schedule"]


def test_end_of_day_reconciliation_accepts_confirmed_closed_position(clock_only, monkeypatch):
    runtime = clock_only
    _, state, broker, _ = opened(runtime)
    at = config.slot_datetime(DAY, "15:05")
    runtime.advance_live_order(state, broker, at)
    broker.rows[state["squareoff_order_id"]].update(
        status="COMPLETE", filled_quantity=state["quantity"],
        pending_quantity=0, average_price=state["entry_price"],
    )
    runtime.advance_live_order(state, broker, at + timedelta(seconds=1))
    monkeypatch.setattr(runtime.common, "now_ist", lambda: config.slot_datetime(DAY, "15:10"))

    result = runtime._publish_broker_position_reconciliation([state], broker, DAY)
    assert result["all_flat"] is True
    assert result["broker_nonzero_positions"] == []
    assert result["local_nonzero_positions"] == []
    assert result["broker_active_tagged_order_count"] == 0


@pytest.mark.parametrize("case", ["broker_residual", "local_residual", "working_tagged_order"])
def test_end_of_day_reconciliation_rejects_incomplete_flatness(runtime, monkeypatch, case):
    _, state, broker, _ = opened(runtime)
    states = [state]
    if case == "broker_residual":
        states = []
        monkeypatch.setattr(broker, "orders", lambda: [])
    elif case == "local_residual":
        monkeypatch.setattr(broker, "positions", lambda: {"net": []})
    else:
        states = []
        monkeypatch.setattr(broker, "positions", lambda: {"net": []})
    monkeypatch.setattr(runtime.common, "now_ist", lambda: config.slot_datetime(DAY, "15:10"))

    result = runtime._publish_broker_position_reconciliation(states, broker, DAY)
    assert result["all_flat"] is False
    assert result["verification_due"] is True


def test_reconciliation_worker_degrades_open_positions_after_verification_deadline(runtime, monkeypatch):
    _, state, broker, _ = opened(runtime)
    published = []
    monkeypatch.setattr(runtime.common, "now_ist", lambda: config.slot_datetime(DAY, "15:10"))
    monkeypatch.setattr(runtime, "load_order_states", lambda *_args, **_kwargs: [state])
    monkeypatch.setattr(runtime, "KitePool", lambda *_args, **_kwargs: broker)
    monkeypatch.setattr(runtime, "_publish", lambda *args, **kwargs: published.append((args, kwargs)))
    args = SimpleNamespace(execution_mode="LIVE", max_apps=1, timeout_sec=1,
                           once=True, broker_reconcile_sec=30)

    assert runtime.run_broker_reconciliation(args, DAY) == 2
    call_args, payload = published[-1]
    assert call_args == ("broker-reconciliation", "DEGRADED")
    result = payload["broker_position_reconciliation"]
    assert result["mismatch_count"] == 0
    assert result["active_order_parity_complete"] is True
    assert result["all_flat"] is False


@pytest.mark.parametrize("failure", ["timeout", "missing_quantity", "invalid_quantity", "fractional_quantity",
                                     "missing_exchange", "missing_product", "missing_symbol"])
def test_reconciliation_error_never_publishes_flat_success(runtime, monkeypatch, failure):
    broker = MemoryBroker()
    if failure == "timeout":
        monkeypatch.setattr(broker, "positions", lambda: (_ for _ in ()).throw(TimeoutError("offline")))
    else:
        row = dict(tradingsymbol="TEST", exchange="NSE", product="MIS")
        if failure != "missing_quantity":
            row["quantity"] = "bad-data" if failure == "invalid_quantity" else 0.5
        missing_identity = {"missing_exchange": "exchange", "missing_product": "product",
                            "missing_symbol": "tradingsymbol"}.get(failure)
        if missing_identity:
            row.pop(missing_identity)
            row["quantity"] = 1
        monkeypatch.setattr(broker, "positions", lambda: {"net": [row]})
    monkeypatch.setattr(runtime.common, "now_ist", lambda: config.slot_datetime(DAY, "15:10"))
    monkeypatch.setattr(runtime, "load_order_states", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(runtime, "KitePool", lambda *_args, **_kwargs: broker)
    published = []
    monkeypatch.setattr(runtime, "_publish", lambda *args, **kwargs: published.append((args, kwargs)))
    args = SimpleNamespace(execution_mode="LIVE", max_apps=1, timeout_sec=1,
                           once=True, broker_reconcile_sec=30)

    assert runtime.run_broker_reconciliation(args, DAY) == 2
    call_args, payload = published[-1]
    assert call_args == ("broker-reconciliation", "DEGRADED")
    assert payload["broker_position_reconciliation"]["all_flat"] is False


@pytest.mark.parametrize("offset", [61, -1])
def test_flat_snapshot_must_finish_within_valid_freshness_window(runtime, monkeypatch, offset):
    broker = MemoryBroker()
    started = config.slot_datetime(DAY, "15:10")
    calls = iter((started, started + timedelta(seconds=offset)))
    monkeypatch.setattr(runtime.common, "now_ist",
                        lambda: next(calls, started + timedelta(seconds=offset)))

    result = runtime._publish_broker_position_reconciliation([], broker, DAY)
    assert result["mismatch_count"] == 0
    assert result["active_order_parity_complete"] is True
    assert result["broker_nonzero_positions"] == []
    assert result["local_nonzero_positions"] == []
    assert result["snapshot_fresh"] is False
    assert result["all_flat"] is False


def test_flat_snapshot_for_wrong_session_is_not_verified(runtime, monkeypatch):
    broker = MemoryBroker()
    monkeypatch.setattr(runtime.common, "now_ist",
                        lambda: config.slot_datetime(DAY + timedelta(days=1), "15:10"))

    result = runtime._publish_broker_position_reconciliation([], broker, DAY)
    assert result["snapshot_fresh"] is False
    assert result["all_flat"] is False
