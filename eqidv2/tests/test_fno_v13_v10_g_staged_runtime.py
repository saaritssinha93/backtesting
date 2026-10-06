"""Dated production exits using an in-memory broker only."""
from datetime import date, timedelta

import pytest

import fno_v13_v10_g_live_config as config
from tests.test_fno_v13_v10_g_live_execution import (
    FakeBroker, InputException, kite_pool, runtime, signal_for,
)


DAY = date(2026, 10, 6)


class StagedBroker(FakeBroker):
    def __init__(self):
        super().__init__()
        self.modified = []
        self.modify_error = None
        self.cancel_pending = False
        self.complete_target_on_cancel = False

    def place_order(self, **kwargs):
        order_id = super().place_order(**kwargs)
        if kwargs.get("order_type") == "SL-M":
            self.rows[-1]["status"] = "TRIGGER PENDING"
        return order_id

    def modify_order(self, **kwargs):
        self.modified.append(dict(kwargs))
        row = next(row for row in self.rows if row["order_id"] == kwargs["order_id"])
        if self.modify_error == "timeout_accepted":
            row.update(kwargs)
            self.modify_error = None
            raise TimeoutError("Accepted, response lost")
        if self.modify_error == "rejected":
            self.modify_error = None
            raise InputException("Trigger price for stoploss must be below last traded price")
        if self.modify_error == "pending":
            self.modify_error = None
            row["status"] = "MODIFY PENDING"
            return row["order_id"]
        row.update(kwargs)
        return row["order_id"]

    def cancel_order(self, *, variety, order_id):
        if self.cancel_pending:
            self.cancelled.append(order_id)
            return
        if self.complete_target_on_cancel:
            row = next(row for row in self.rows if row["order_id"] == order_id)
            if row["order_type"] == "LIMIT":
                self.complete(order_id, row["price"])
                return
        return super().cancel_order(variety=variety, order_id=order_id)


def promoted_signal(runtime, side="LONG", day=DAY):
    setup = config.setup_for("09:25", side, session_date=day)
    signal = signal_for(runtime, setup)
    signal.update(session_date=day.isoformat(),
                  signal_id=runtime._signal_id(day, setup, "EXAMPLE"),
                  entry_activation_deadline_ist=config.activation_deadline(day, setup.confirmation_end).isoformat(),
                  **runtime._g_stop_policy_metadata(day))
    return signal


def open_live(runtime, side="LONG", quantity=1):
    signal = promoted_signal(runtime, side)
    state = runtime.create_order_state(signal, "LIVE", live_quantity=quantity)
    broker = StagedBroker()
    at = config.slot_datetime(DAY, "09:27")
    runtime.advance_live_order(state, broker, at, last_price=99 if side == "LONG" else 101)
    broker.complete(state["entry_order_id"], 100.)
    runtime.advance_live_order(state, broker, at)
    return state, broker, at


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_paper_stop_changes_exactly_120_minutes_after_actual_fill(runtime, side):
    signal = promoted_signal(runtime, side)
    runtime._validate_signal(signal, DAY)
    state = runtime.create_order_state(signal, "PAPER")
    at = config.slot_datetime(DAY, "09:31")
    runtime.advance_paper_order(state, 100., at)
    assert state["stop_price"] == (98.75 if side == "LONG" else 101.25)
    target = state["target_price"]
    runtime.advance_paper_order(state, 100., at + timedelta(minutes=120, seconds=-1))
    assert state.get("stop_tightened") is None
    runtime.advance_paper_order(state, 100., at + timedelta(minutes=120))
    assert state["stop_price"] == (99. if side == "LONG" else 101.)
    assert state["target_price"] == target
    runtime._validate_order_state(state, signal, "PAPER")


def test_paper_tightening_uses_available_gap_price(runtime):
    state = runtime.create_order_state(promoted_signal(runtime), "PAPER")
    at = config.slot_datetime(DAY, "09:27")
    runtime.advance_paper_order(state, 100., at)
    runtime.advance_paper_order(state, 98.9, at + timedelta(minutes=120))
    assert (state["status"], state["exit_price"], state["exit_reason"]) == ("CLOSED", 98.9, "STOP")


def test_prior_session_preserves_original_fixed_stop(runtime):
    day = date(2026, 10, 5)
    state = runtime.create_order_state(promoted_signal(runtime, day=day), "PAPER")
    at = config.slot_datetime(day, "09:27")
    runtime.advance_paper_order(state, 100., at)
    original = state["stop_price"]
    runtime.advance_paper_order(state, 100., at + timedelta(minutes=121))
    assert state["stop_price"] == original
    assert "stop_policy" not in state


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_live_modify_requires_due_clock_and_reconciles_idempotently(runtime, side):
    state, broker, at = open_live(runtime, side)
    runtime.advance_live_order(state, broker, at + timedelta(minutes=119), last_price=100.)
    assert not broker.modified
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=100.)
    assert len(broker.modified) == 1
    assert state["stop_price"] == (99. if side == "LONG" else 101.)
    runtime.advance_live_order(state, broker, at + timedelta(minutes=121), last_price=100.)
    assert len(broker.modified) == 1
    assert len(broker.placed) == 3


def test_accepted_timeout_reconciles_from_broker_without_second_mutation(runtime):
    state, broker, at = open_live(runtime)
    pool = kite_pool(runtime, app1=broker)
    broker.modify_error = "timeout_accepted"
    with pytest.raises(runtime.BrokerMutationUncertain):
        runtime.advance_live_order(state, pool, at + timedelta(minutes=120), last_price=100.)
    assert state["stop_price"] == 98.75
    assert state["stop_modification_uncertain"] is True
    runtime.advance_live_order(state, pool, at + timedelta(minutes=120, seconds=1), last_price=100.)
    assert state["stop_price"] == 99.
    assert state["stop_modification_uncertain"] is False
    assert len(broker.modified) == 1


def test_crossed_threshold_waits_for_cancel_ack_then_converts_same_stop(runtime):
    state, broker, at = open_live(runtime)
    due = at + timedelta(minutes=120)
    broker.cancel_pending = True
    runtime.advance_live_order(state, broker, due, last_price=98.9)
    assert not broker.modified
    assert state["stop_tighten_status"] == "WAITING_FOR_TARGET_CANCEL_ACK"
    broker.cancel_pending = False
    runtime.advance_live_order(state, broker, due, last_price=98.9)
    assert broker.modified[-1]["order_id"] == state["stop_order_id"]
    assert broker.modified[-1]["order_type"] == "MARKET"
    assert broker.modified[-1]["market_protection"] == -1
    runtime.advance_live_order(state, broker, due + timedelta(seconds=1), last_price=98.9)
    assert len(broker.modified) == 1
    assert len(broker.placed) == 3
    broker.complete(state["stop_order_id"], 98.9)
    runtime.advance_live_order(state, broker, due + timedelta(seconds=2))
    assert state["status"] == "CLOSED"
    assert state["exit_reason"] == "STOP"


def test_target_completing_during_cancel_prevents_market_conversion(runtime):
    state, broker, at = open_live(runtime)
    broker.complete_target_on_cancel = True
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=98.9)
    assert state["status"] == "CLOSED"
    assert state["exit_reason"] == "TARGET"
    assert not broker.modified


def test_partial_target_fill_reduces_converted_stop_and_weights_exit(runtime):
    state, broker, at = open_live(runtime, quantity=10)
    target = next(row for row in broker.rows if row["order_id"] == state["target_order_id"])
    target.update(filled_quantity=3, average_price=101.)
    due = at + timedelta(minutes=120)
    runtime.advance_live_order(state, broker, due, last_price=98.9)
    assert broker.modified[-1]["quantity"] == 7
    broker.complete(state["stop_order_id"], 98.9)
    runtime.advance_live_order(state, broker, due)
    assert state["quantity"] == 10
    assert state["exit_price"] == pytest.approx((3 * 101 + 7 * 98.9) / 10)
    assert state["gross_pnl_rs"] == pytest.approx(3 - 7.7)


@pytest.mark.parametrize("role", ["stop", "target"])
def test_complete_exit_at_tightening_does_not_modify(runtime, role):
    state, broker, at = open_live(runtime)
    broker.complete(state[f"{role}_order_id"], state[f"{role}_price"])
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=100.)
    assert state["status"] == "CLOSED"
    assert not broker.modified


def test_broker_exchange_fill_timestamp_drives_due_time_on_recovery(runtime):
    state = runtime.create_order_state(promoted_signal(runtime), "LIVE", live_quantity=1)
    actual = config.slot_datetime(DAY, "09:27")
    runtime._apply_live_entry_fill(state, {"average_price": 100., "filled_quantity": 1,
                                        "exchange_update_timestamp": actual.isoformat()},
                                   actual + timedelta(minutes=5))
    assert state["entry_at_ist"] == actual.isoformat(timespec="seconds")
    assert runtime._staged_stop_target(state, actual + timedelta(minutes=120)) == 99.


def test_installed_sdk_modify_compat_preserves_market_protection(runtime):
    class OldSDK:
        def _put(self, route, *, url_args, params):
            assert route == "order.modify"
            assert params["market_protection"] == -1
            assert url_args == {"variety": "regular", "order_id": "STOP1"}
            return {"order_id": "STOP1"}

    assert runtime._kite_modify_order_compat(OldSDK(), dict(variety="regular", order_id="STOP1",
                                            order_type="MARKET", market_protection=-1)) == "STOP1"


@pytest.mark.parametrize("partial", [False, True])
def test_triggered_or_partially_filled_stop_is_never_modified(runtime, partial):
    state, broker, at = open_live(runtime, quantity=10)
    stop = next(row for row in broker.rows if row["order_id"] == state["stop_order_id"])
    stop.update(status="OPEN", filled_quantity=3 if partial else 0)
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=98.9)
    assert not broker.modified
    assert not broker.cancelled
    assert state["stop_tighten_status"] == "TRIGGERED_STOP_AWAITING_FILL"


def test_broker_tighter_stop_is_adopted_without_loosening(runtime):
    state, broker, at = open_live(runtime)
    stop = next(row for row in broker.rows if row["order_id"] == state["stop_order_id"])
    stop["trigger_price"] = 99.5
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=100.)
    assert state["stop_price"] == 99.5
    assert not broker.modified


def test_cancelled_protective_stop_uses_existing_emergency_exit(runtime):
    state, broker, at = open_live(runtime)
    broker.cancel_order(variety="regular", order_id=state["stop_order_id"])
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=100.)
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert len(broker.placed) == 4
    assert not broker.modified


def test_partial_target_remaining_quantity_matches_broker_reconciliation(runtime):
    state, broker, at = open_live(runtime, quantity=10)
    target = next(row for row in broker.rows if row["order_id"] == state["target_order_id"])
    target.update(filled_quantity=3, average_price=101.)
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=98.9)
    broker.positions = lambda: {"net": [{"tradingsymbol": "EXAMPLE", "exchange": "NSE",
                                        "product": "MIS", "quantity": 7}]}
    result = runtime._publish_broker_position_reconciliation([state], broker, DAY)
    assert result["mismatch_count"] == 0
    assert result["active_order_mismatch_count"] == 0


def test_unconfirmed_broker_response_does_not_change_local_stop(runtime):
    state, broker, at = open_live(runtime)
    broker.modify_error = "pending"
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=100.)
    assert state["stop_price"] == 98.75
    assert state["stop_tighten_status"] == "AWAITING_BROKER_CONFIRMATION"
    runtime.advance_live_order(state, broker, at + timedelta(minutes=121), last_price=100.)
    assert len(broker.modified) == 1
    assert state["stop_price"] == 98.75


def test_crossed_trigger_rejection_uses_cancel_ack_and_same_order_exit(runtime):
    state, broker, at = open_live(runtime)
    broker.modify_error = "rejected"
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=100.)
    assert len(broker.modified) == 2
    assert broker.modified[0]["trigger_price"] == 99.
    assert broker.modified[1]["order_type"] == "MARKET"
    assert len({row["order_id"] for row in broker.modified}) == 1
    assert len(broker.placed) == 3


def test_market_conversion_timeout_is_reconciled_without_second_exit(runtime):
    state, broker, at = open_live(runtime)
    broker.modify_error = "timeout_accepted"
    pool = kite_pool(runtime, app1=broker)
    with pytest.raises(runtime.BrokerMutationUncertain):
        runtime.advance_live_order(state, pool, at + timedelta(minutes=120), last_price=98.9)
    runtime.advance_live_order(state, pool, at + timedelta(minutes=120, seconds=1), last_price=98.9)
    assert len(broker.modified) == 1
    assert len(broker.placed) == 3
    assert state["stop_tighten_status"] == "PROTECTIVE_STOP_MARKET_EXIT_PENDING"


def test_cancelled_partially_filled_converted_stop_only_exits_residual(runtime):
    state, broker, at = open_live(runtime, quantity=10)
    target = next(row for row in broker.rows if row["order_id"] == state["target_order_id"])
    target.update(filled_quantity=3, average_price=101.)
    due = at + timedelta(minutes=120)
    runtime.advance_live_order(state, broker, due, last_price=98.9)
    stop = next(row for row in broker.rows if row["order_id"] == state["stop_order_id"])
    stop.update(status="CANCELLED", filled_quantity=2, average_price=98.9)
    runtime.advance_live_order(state, broker, due + timedelta(seconds=1))
    assert state["status"] == "SQUARE_OFF_PENDING"
    assert broker.placed[-1]["quantity"] == 5
    broker.complete(state["squareoff_order_id"], 98.8)
    runtime.advance_live_order(state, broker, due + timedelta(seconds=2))
    assert state["status"] == "CLOSED"
    assert state["exit_price"] == pytest.approx((3 * 101 + 2 * 98.9 + 5 * 98.8) / 10)


def test_stop_triggering_during_target_cancel_is_not_modified(runtime):
    state, broker, at = open_live(runtime, quantity=10)
    normal_cancel = broker.cancel_order

    def racing_cancel(**kwargs):
        normal_cancel(**kwargs)
        stop = next(row for row in broker.rows if row["order_id"] == state["stop_order_id"])
        stop.update(status="OPEN", filled_quantity=2, average_price=98.7)

    broker.cancel_order = racing_cancel
    runtime.advance_live_order(state, broker, at + timedelta(minutes=120), last_price=98.9)
    assert not broker.modified
    assert state["stop_tighten_status"] == "TRIGGERED_STOP_AWAITING_FILL"
