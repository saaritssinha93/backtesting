from __future__ import annotations

import importlib.util
import sys
from copy import deepcopy
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import fno_oi_common as common
from fno_v13_v10_g_identity import canonical_signal_id
import fno_v13_v10_g_live_config as config
import fno_v6_live_kite_session as coordinator


DAY = date(2026, 9, 15)
ROOT = Path(__file__).resolve().parents[1]


class FakeBroker:
    """In-memory broker: these tests cannot connect or submit real orders."""

    def __init__(self):
        self.rows = []
        self.placed = []
        self.cancelled = []

    def orders(self):
        return deepcopy(self.rows)

    def ltp(self, keys):
        return {key: {"last_price": 99.95} for key in keys}

    def place_order(self, **kwargs):
        order_id = f"FAKE{len(self.rows) + 1}"
        row = {**kwargs, "order_id": order_id, "status": "OPEN"}
        self.rows.append(row)
        self.placed.append(deepcopy(row))
        return order_id

    def order_history(self, order_id):
        return deepcopy([row for row in self.rows if row["order_id"] == order_id])

    def cancel_order(self, *, variety, order_id):
        self.cancelled.append(order_id)
        for row in self.rows:
            if row["order_id"] == order_id:
                row["status"] = "CANCELLED"

    def complete(self, order_id, price):
        for row in self.rows:
            if row["order_id"] == order_id:
                row.update(status="COMPLETE", average_price=price, filled_quantity=row["quantity"])


class TokenException(Exception):
    """Test double matching the explicit Kite authentication exception name."""


class InputException(Exception):
    """Test double for a deterministic broker-side HTTP 400 rejection."""

    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


class ScriptedBroker(FakeBroker):
    """Broker lane with deterministic quotes and placement outcomes."""

    def __init__(self, *, quotes=(), placement_errors=(), rows=()):
        super().__init__()
        self.rows = [deepcopy(row) for row in rows]
        self.quotes = list(quotes)
        self.placement_errors = list(placement_errors)
        self.place_attempts = []
        self.ltp_calls = 0
        self.orders_calls = 0

    def orders(self):
        self.orders_calls += 1
        return super().orders()

    def ltp(self, keys):
        self.ltp_calls += 1
        if not self.quotes:
            raise AssertionError("No scripted LTP remains.")
        price = self.quotes.pop(0) if len(self.quotes) > 1 else self.quotes[0]
        return {key: {"last_price": price} for key in keys}

    def place_order(self, **kwargs):
        self.place_attempts.append(deepcopy(kwargs))
        if self.placement_errors:
            raise self.placement_errors.pop(0)
        return super().place_order(**kwargs)


def credential(app_name, access_token):
    return SimpleNamespace(
        app_name=app_name,
        api_key=f"key-{app_name}",
        access_token=access_token,
    )


def kite_pool(runtime, **lanes):
    credentials = [credential(app_name, f"token-{app_name}") for app_name in lanes]
    return runtime.KitePool(
        len(credentials),
        1,
        credential_loader=lambda **_kwargs: credentials,
        client_factory=lambda item, **_kwargs: lanes[item.app_name],
    )


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("FNO_LIVE_GENERATION", "v6")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "V13_V10_G")
    monkeypatch.setenv("FNO_V6_EXECUTION_SESSION_NAMESPACE", "live_kite_qty1")
    monkeypatch.setattr(common, "FNO_ROOT", tmp_path / "fno_oi")
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path / "latest")
    spec = importlib.util.spec_from_file_location("g_execution_test_runtime", ROOT / "fno_v5_live.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.config.STRATEGY_VERSION == config.STRATEGY_VERSION
    module.unmocked_live_arm_state = module._live_arm_state
    monkeypatch.setattr(module, "_live_arm_state", lambda day: (True, "LIVE_ARMED"))
    monkeypatch.setattr(module, "_read_json", lambda path: {})
    return module


@pytest.fixture
def isolated_coordinator(tmp_path, monkeypatch):
    runtime_root = tmp_path / "fno_oi" / "v13_v10_g_live"
    controls = tmp_path / "fno_oi" / "v6_live"
    export = runtime_root / "live_kite"
    monkeypatch.setattr(coordinator, "config", config)
    monkeypatch.setattr(common, "RUNTIME_STATUS_DIR", tmp_path / "runtime_status")
    for name, value in {
        "SESSION_ID": "fno_v13_v10_g_live_kite_qty1",
        "LIVE_ROOT": runtime_root,
        "CONTROL_ROOT": controls,
        "CONFIRMATION_ROOT": runtime_root / "confirmation_1m",
        "SIGNAL_ROOT": runtime_root / "signals",
        "PROFILE_ORDER_ROOT": runtime_root / "orders" / "LIVE" / "live_kite_qty1",
        "EXPORT_ROOT": export,
        "STATUS_PATH": export / "status.json",
        "HEARTBEAT_PATH": export / "heartbeat.json",
    }.items():
        monkeypatch.setattr(coordinator, name, value)
    return runtime_root


def test_coordinator_surfaces_same_run_reconciliation_degradation(
    isolated_coordinator,
) -> None:
    class Process:
        pid = 123

        @staticmethod
        def poll():
            return None

    session_id = coordinator.worker_session_id("BROKER_RECONCILIATION")
    common.atomic_write_kv(
        common.session_status_path(session_id),
        {
            "status": "DEGRADED",
            "session": session_id,
            "ts": f"{DAY.isoformat()}T09:20:00+05:30",
            "run_id": coordinator.RUN_ID,
            "broker_position_reconciliation": {
                "broker_truth_available": False,
                "error_type": "TokenException",
            },
        },
    )

    status = coordinator._child_process_status("BROKER_RECONCILIATION", Process())

    assert status["state"] == "DEGRADED"
    assert status["broker_truth_available"] is False
    assert status["scope_complete"] is False
    assert status["active_order_parity_complete"] is False
    assert status["error_type"] == "TokenException"


def test_coordinator_surfaces_active_order_reconciliation_mismatch(
    isolated_coordinator,
) -> None:
    class Process:
        pid = 124

        @staticmethod
        def poll():
            return None

    session_id = coordinator.worker_session_id("BROKER_RECONCILIATION")
    common.atomic_write_kv(
        common.session_status_path(session_id),
        {
            "status": "DEGRADED",
            "session": session_id,
            "ts": f"{DAY.isoformat()}T09:21:00+05:30",
            "run_id": coordinator.RUN_ID,
            "broker_position_reconciliation": {
                "broker_truth_available": True,
                "scope_complete": True,
                "mismatch_count": 0,
                "active_order_parity_complete": False,
                "active_order_mismatch_count": 1,
            },
        },
    )

    status = coordinator._child_process_status("BROKER_RECONCILIATION", Process())

    assert status["state"] == "DEGRADED"
    assert status["broker_truth_available"] is True
    assert status["scope_complete"] is True
    assert status["mismatch_count"] == 0
    assert status["active_order_parity_complete"] is False
    assert status["active_order_mismatch_count"] == 1


def signal_for(runtime, setup):
    stop, target = config.bracket_levels(100.0, setup.side, setup.stop_pct, setup.target_pct, .05)
    return {
        "signal_id": runtime._signal_id(DAY, setup, "EXAMPLE"),
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "session_date": DAY.isoformat(),
        "signal_end": setup.signal_end,
        "confirmation_end": setup.confirmation_end,
        "signal_timestamp": config.slot_datetime(DAY, setup.signal_end).isoformat(),
        "confirmation_timestamp": config.slot_datetime(DAY, setup.confirmation_end).isoformat(),
        "entry_activation_deadline_ist": config.activation_deadline(DAY, setup.confirmation_end).isoformat(),
        "side": setup.side,
        "setup_id": setup.setup_id,
        "setup_source": setup.source_version,
        "setup_mode": setup.mode,
        "max_entries": setup.max_entries,
        "picker": setup.picker,
        "rank_within_scan": 1,
        "tradingsymbol": "EXAMPLE",
        "exchange": "NSE",
        "instrument_token": 111,
        "futures_tradingsymbol": "EXAMPLE26SEPFUT",
        "futures_instrument_token": 222,
        "data_contract": runtime.hybrid.DATA_CONTRACT_VERSION,
        "tick_size": .05,
        "lot_size": 1,
        "paper_sizing": asdict(config.size_position(100.0, 1, live=False)),
        "live_sizing": asdict(config.size_position(100.0, 1, live=True)),
        "capital_rs": config.CAPITAL_PER_ENTRY_RS,
        "leverage": config.LEVERAGE,
        "target_exposure_rs": config.TARGET_EXPOSURE_RS,
        "trigger_price": 100.0,
        "stop_pct": setup.stop_pct,
        "target_pct": setup.target_pct,
        "stop_price": stop,
        "target_price": target,
        "round_trip_cost_bps": config.ROUND_TRIP_COST_BPS,
    }


def test_live_signal_id_delegates_to_canonical_legacy_identity(runtime):
    session_date = date(2026, 9, 25)
    setup = next(
        setup
        for setup in config.ACTIVE_SETUPS
        if setup.confirmation_end == "09:31" and setup.side == "SHORT"
    )

    expected = "20260925_0931_SHORT_OFSS_6ac0b887eee8"
    assert runtime._signal_id(session_date, setup, "OFSS") == expected
    assert canonical_signal_id(
        config.STRATEGY_VERSION,
        session_date,
        setup.signal_end,
        setup.confirmation_end,
        setup.side,
        "OFSS",
    ) == expected


def write_authority(signal):
    common.atomic_write_json(
        coordinator.SIGNAL_ROOT / DAY.isoformat() / f"{signal['signal_id']}.json", signal
    )
    common.atomic_write_json(
        coordinator._confirmation_path(DAY, signal["signal_end"]),
        {
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "session_date": DAY.isoformat(),
            "state": "SUCCESS",
            "selected_signal_ids": [signal["signal_id"]],
        },
    )


@pytest.mark.parametrize("setup", config.ACTIVE_SETUPS, ids=lambda setup: setup.setup_id)
def test_each_g_setup_uses_fill_based_full_exit_without_partial_or_breakeven(runtime, setup):
    signal = signal_for(runtime, setup)
    state = runtime.create_order_state(signal, "PAPER")
    quantity = state["quantity"]
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)
    fill = 100.25 if setup.side == "LONG" else 99.75
    state = runtime.advance_paper_order(state, fill, at)
    assert state["status"] == "OPEN"
    stop, target = config.bracket_levels(fill, setup.side, setup.stop_pct, setup.target_pct, .05)
    assert state["stop_price"] == stop
    assert state["target_price"] == target
    state = runtime.advance_paper_order(state, (fill + target) / 2, at + timedelta(seconds=5))
    assert state["status"] == "OPEN"
    assert state["quantity"] == quantity
    assert state["stop_price"] == stop
    state = runtime.advance_paper_order(state, target, at + timedelta(seconds=10))
    assert state["status"] == "CLOSED"
    assert state["exit_reason"] == "TARGET"
    assert state["quantity"] == quantity
    assert state["net_pnl_rs"] > 0


def test_g_paper_entry_expires_at_ten_minutes(runtime):
    setup = config.setup_for("09:25", "LONG")
    signal = signal_for(runtime, setup)
    deadline = config.activation_deadline(DAY, setup.confirmation_end)
    assert (deadline - config.slot_datetime(DAY, setup.confirmation_end)).total_seconds() == 600
    state = runtime.advance_paper_order(runtime.create_order_state(signal, "PAPER"), 101, deadline)
    assert state["status"] == "OPEN"
    state = runtime.advance_paper_order(
        runtime.create_order_state(signal, "PAPER"), 101, deadline + timedelta(seconds=1)
    )
    assert state["status"] == "CANCELLED"
    assert state["entry_price"] == 0


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_g_live_qty_one_recovers_without_duplicate_orders_and_exits_full_position(runtime, side):
    setup = config.setup_for("09:25", side)
    signal = signal_for(runtime, setup)
    runtime._validate_signal(signal, DAY)
    state = runtime.create_order_state(signal, "LIVE", live_quantity=1)
    runtime._validate_order_state(state, signal, "LIVE", live_quantity=1)
    broker = FakeBroker()
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)
    state = runtime.advance_live_order(state, broker, at)
    assert len(broker.placed) == 1
    entry_id = state["entry_order_id"]
    # A restart between the broker response and state persistence recovers by tag.
    state["entry_order_id"] = ""
    state = runtime.advance_live_order(state, broker, at)
    assert state["entry_order_id"] == entry_id
    assert len(broker.placed) == 1
    fill = 100.25 if side == "LONG" else 99.75
    broker.complete(entry_id, fill)
    state = runtime.advance_live_order(state, broker, at + timedelta(seconds=5))
    assert state["status"] == "OPEN"
    assert len(broker.placed) == 3
    assert {row["quantity"] for row in broker.placed} == {1}
    assert {row["tag"] for row in broker.placed} == {
        runtime._live_order_tag(signal["signal_id"], "entry"),
        runtime._live_order_tag(signal["signal_id"], "stop"),
        runtime._live_order_tag(signal["signal_id"], "target"),
    }
    assert broker.placed[0]["market_protection"] == -1
    assert broker.placed[1]["market_protection"] == -1
    assert "market_protection" not in broker.placed[2]
    assert (state["stop_price"], state["target_price"]) == config.bracket_levels(
        fill, side, setup.stop_pct, setup.target_pct, .05
    )
    state = runtime.advance_live_order(state, broker, at + timedelta(seconds=6))
    assert len(broker.placed) == 3
    broker.complete(state["target_order_id"], state["target_price"])
    state = runtime.advance_live_order(state, broker, at + timedelta(seconds=7))
    assert state["status"] == "CLOSED"
    assert state["exit_reason"] == "TARGET"
    assert state["quantity"] == 1
    assert state["stop_order_id"] in broker.cancelled
    assert state["net_pnl_rs"] > 0


@pytest.mark.parametrize(
    ("side", "last_price"),
    (("LONG", 100.05), ("SHORT", 99.95)),
)
def test_crossed_live_entry_uses_protected_market(runtime, side, last_price):
    setup = config.setup_for("09:25", side)
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    lane = ScriptedBroker(quotes=(last_price,))
    pool = kite_pool(runtime, app1=lane)
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)

    state = runtime.advance_live_order(state, pool, at)

    assert state["entry_order_id"]
    assert len(lane.place_attempts) == 1
    submitted = lane.place_attempts[0]
    assert submitted["order_type"] == "MARKET"
    assert submitted["market_protection"] == runtime.AUTO_MARKET_PROTECTION
    assert "trigger_price" not in submitted
    assert submitted["tag"] == runtime._live_order_tag(state["signal_id"], "entry")


@pytest.mark.parametrize(
    ("side", "last_price"),
    (("LONG", 99.95), ("SHORT", 100.05)),
)
def test_uncrossed_live_entry_uses_protected_stop_market(runtime, side, last_price):
    setup = config.setup_for("09:25", side)
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    lane = ScriptedBroker(quotes=(last_price,))
    pool = kite_pool(runtime, app1=lane)
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)

    state = runtime.advance_live_order(state, pool, at)

    assert state["entry_order_id"]
    assert len(lane.place_attempts) == 1
    submitted = lane.place_attempts[0]
    assert submitted["order_type"] == "SL-M"
    assert submitted["trigger_price"] == state["trigger_price"]
    assert submitted["market_protection"] == runtime.AUTO_MARKET_PROTECTION


def test_live_pool_does_not_wrap_or_fail_over_deterministic_input_rejection(runtime):
    rejection = InputException("trigger price has already crossed the LTP")
    primary = ScriptedBroker(placement_errors=(rejection,))
    secondary = ScriptedBroker()
    pool = kite_pool(runtime, app1=primary, app2=secondary)

    with pytest.raises(InputException, match="already crossed") as observed:
        pool.place_order(
            variety="regular",
            exchange="NSE",
            tradingsymbol="EXAMPLE",
            transaction_type="BUY",
            quantity=1,
            product="MIS",
            order_type="SL-M",
            trigger_price=100.0,
            validity="DAY",
            tag="deterministic-E",
        )

    assert observed.value is rejection
    assert len(primary.place_attempts) == 1
    assert secondary.place_attempts == []
    assert pool.last_operation == "place_order"
    assert pool.last_operation_app == "app1"
    assert pool.last_operation_failures[0]["error_type"] == "InputException"


def test_non_cross_deterministic_entry_rejection_is_terminal(runtime):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    lane = ScriptedBroker(
        quotes=(99.95,),
        placement_errors=(InputException("invalid order parameter"),),
    )
    pool = kite_pool(runtime, app1=lane)
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)

    state = runtime.advance_live_order(state, pool, at)

    assert state["status"] == "ENTRY_REJECTED"
    assert "invalid order parameter" in state["status_reason"]
    assert len(lane.place_attempts) == 1

    state = runtime.advance_live_order(state, pool, at + timedelta(seconds=1))
    assert state["status"] == "ENTRY_REJECTED"
    assert len(lane.place_attempts) == 1


def test_stop_entry_cross_race_requotes_and_uses_protected_market(runtime):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    lane = ScriptedBroker(
        quotes=(99.95, 100.05),
        placement_errors=(
            InputException(
                "Trigger price for stoploss buy orders should be higher than "
                "the last traded price."
            ),
        ),
    )
    pool = kite_pool(runtime, app1=lane)
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)

    state = runtime.advance_live_order(state, pool, at)

    assert state["entry_order_id"]
    assert lane.ltp_calls == 2
    assert [row["order_type"] for row in lane.place_attempts] == ["SL-M", "MARKET"]
    assert lane.place_attempts[1]["market_protection"] == runtime.AUTO_MARKET_PROTECTION
    assert "trigger_price" not in lane.place_attempts[1]
    assert {
        row["tag"] for row in lane.place_attempts
    } == {runtime._live_order_tag(state["signal_id"], "entry")}


def test_ambiguous_entry_submission_latches_reconciliation_without_retry(runtime):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    lane = ScriptedBroker(
        quotes=(99.95,),
        placement_errors=(TimeoutError("response lost after submission"),),
    )
    pool = kite_pool(runtime, app1=lane)
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)

    for observed_at in (at, at + timedelta(seconds=1)):
        try:
            state = runtime.advance_live_order(state, pool, observed_at)
        except runtime.BrokerMutationUncertain:
            pass

    assert state["status"] == "PENDING_ENTRY"
    assert len(lane.place_attempts) == 1
    assert lane.orders_calls >= 2


@pytest.mark.parametrize("broker_order_type", ("MARKET", "LIMIT"))
def test_live_entry_recovers_market_or_protected_limit_by_role_tag(
    runtime, broker_order_type
):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    order_id = f"RECOVERED-{broker_order_type}"
    lane = ScriptedBroker(
        quotes=(100.05,),
        rows=(
            {
                "order_id": order_id,
                "tag": runtime._live_order_tag(state["signal_id"], "entry"),
                "tradingsymbol": state["tradingsymbol"],
                "transaction_type": "BUY",
                "order_type": broker_order_type,
                "quantity": 1,
                "status": "OPEN",
            },
        ),
    )
    pool = kite_pool(runtime, app1=lane)
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)

    state = runtime.advance_live_order(state, pool, at)

    assert state["entry_order_id"] == order_id
    assert state["status_reason"] == (
        "LIVE_MARKET_ENTRY_WORKING"
        if broker_order_type == "MARKET"
        else "LIVE_STOP_ENTRY_WORKING"
    )
    assert lane.place_attempts == []


def test_live_pool_fails_over_reads_and_explicit_auth_rejected_submission(runtime):
    calls = []

    class Lane:
        def __init__(self, app_name, reject_auth=False):
            self.app_name = app_name
            self.reject_auth = reject_auth

        def orders(self):
            calls.append((self.app_name, "orders"))
            if self.app_name == "app1":
                raise RuntimeError("temporary read failure")
            return [{"order_id": "READ-OK"}]

        def place_order(self, **kwargs):
            calls.append((self.app_name, "place_order"))
            if self.reject_auth:
                raise TokenException("Incorrect `api_key` or `access_token`.")
            return "PLACED-ON-APP2"

    lanes = {
        "app1": Lane("app1", reject_auth=True),
        "app2": Lane("app2"),
    }
    pool = runtime.KitePool(
        2,
        1,
        credential_loader=lambda **_kwargs: [
            credential("app1", "old"),
            credential("app2", "valid"),
        ],
        client_factory=lambda item, **_kwargs: lanes[item.app_name],
    )

    assert pool.orders() == [{"order_id": "READ-OK"}]
    assert pool.last_operation_app == "app2"
    assert pool.place_order(
        tag="safe-tag",
        tradingsymbol="OFSS",
        transaction_type="SELL",
        order_type="SL-M",
        quantity=1,
    ) == "PLACED-ON-APP2"
    assert pool.last_operation_app == "app2"
    assert ("app1", "place_order") in calls
    assert ("app2", "place_order") in calls


def test_live_pool_reconciles_ambiguous_submission_without_duplicate(runtime):
    broker_rows = []
    secondary_place_calls = []

    class AmbiguousLane:
        def orders(self):
            return list(broker_rows)

        def place_order(self, **kwargs):
            broker_rows.append({**kwargs, "order_id": "RECOVERED-1"})
            raise TimeoutError("response lost after broker acceptance")

    class SecondaryLane:
        def orders(self):
            return list(broker_rows)

        def place_order(self, **kwargs):
            secondary_place_calls.append(kwargs)
            return "DUPLICATE"

    lanes = {"app1": AmbiguousLane(), "app2": SecondaryLane()}
    pool = runtime.KitePool(
        2,
        1,
        credential_loader=lambda **_kwargs: [
            credential("app1", "valid-1"),
            credential("app2", "valid-2"),
        ],
        client_factory=lambda item, **_kwargs: lanes[item.app_name],
    )
    payload = dict(
        tag="safe-tag",
        tradingsymbol="FORTIS",
        transaction_type="SELL",
        order_type="SL-M",
        quantity=1,
    )

    assert pool.place_order(**payload) == "RECOVERED-1"
    assert pool.last_operation == "place_order_reconciled"
    assert secondary_place_calls == []


def test_live_pool_posts_required_market_protection_through_old_sdk(runtime):
    post_calls = []

    class OldSdkLane:
        def _post(self, route, *, url_args, params):
            post_calls.append((route, deepcopy(url_args), deepcopy(params)))
            return {"order_id": "PROTECTED-1"}

    pool = runtime.KitePool(
        1,
        1,
        credential_loader=lambda **_kwargs: [credential("app1", "valid")],
        client_factory=lambda _item, **_kwargs: OldSdkLane(),
    )

    order_id = runtime._broker_place(
        pool,
        variety="regular",
        exchange="NSE",
        tradingsymbol="FORTIS",
        transaction_type="SELL",
        quantity=1,
        product="MIS",
        order_type="SL-M",
        trigger_price=100.0,
        validity="DAY",
        tag="safe-tag-E",
    )

    assert order_id == "PROTECTED-1"
    assert post_calls == [
        (
            "order.place",
            {"variety": "regular"},
            {
                "variety": "regular",
                "exchange": "NSE",
                "tradingsymbol": "FORTIS",
                "transaction_type": "SELL",
                "quantity": 1,
                "product": "MIS",
                "order_type": "SL-M",
                "trigger_price": 100.0,
                "validity": "DAY",
                "tag": "safe-tag-E",
                "market_protection": -1,
            },
        )
    ]


def test_live_pool_recovers_protected_market_normalized_to_limit(runtime):
    broker_rows = []

    class ResponseLostLane:
        def _post(self, _route, *, url_args, params):
            assert url_args == {"variety": "regular"}
            broker_rows.append(
                {
                    **params,
                    "order_type": "LIMIT",
                    "order_id": "RECOVERED-PROTECTED-1",
                    "status": "COMPLETE",
                }
            )
            raise TimeoutError("response lost after protected order acceptance")

        def orders(self):
            return deepcopy(broker_rows)

    pool = runtime.KitePool(
        1,
        1,
        credential_loader=lambda **_kwargs: [credential("app1", "valid")],
        client_factory=lambda _item, **_kwargs: ResponseLostLane(),
    )

    assert pool.place_order(
        variety="regular",
        exchange="NSE",
        tradingsymbol="FORTIS",
        transaction_type="SELL",
        quantity=1,
        product="MIS",
        order_type="MARKET",
        validity="DAY",
        tag="safe-tag-X",
        market_protection=-1,
    ) == "RECOVERED-PROTECTED-1"
    assert pool.last_operation == "place_order_reconciled"


def test_live_pool_never_retries_ambiguous_submission_without_broker_evidence(runtime):
    secondary_place_calls = []

    class AmbiguousLane:
        def orders(self):
            return []

        def place_order(self, **_kwargs):
            raise TimeoutError("unknown submission outcome")

    class SecondaryLane:
        def place_order(self, **kwargs):
            secondary_place_calls.append(kwargs)
            return "UNSAFE-DUPLICATE"

    lanes = {"app1": AmbiguousLane(), "app2": SecondaryLane()}
    pool = runtime.KitePool(
        2,
        1,
        credential_loader=lambda **_kwargs: [
            credential("app1", "valid-1"),
            credential("app2", "valid-2"),
        ],
        client_factory=lambda item, **_kwargs: lanes[item.app_name],
    )

    with pytest.raises(runtime.BrokerMutationUncertain):
        pool.place_order(
            tag="safe-tag",
            tradingsymbol="FORTIS",
            transaction_type="SELL",
            order_type="SL-M",
            quantity=1,
        )
    assert secondary_place_calls == []


def test_live_pool_hot_reloads_changed_access_token(runtime):
    tokens = {"app1": "old-token"}

    class Lane:
        def __init__(self, token):
            self.token = token

        def orders(self):
            return [{"token": self.token}]

    def load_credentials(**_kwargs):
        return [credential("app1", tokens["app1"])]

    pool = runtime.KitePool(
        1,
        1,
        credential_loader=load_credentials,
        client_factory=lambda item, **_kwargs: Lane(item.access_token),
    )
    assert pool.orders() == [{"token": "old-token"}]
    tokens["app1"] = "new-token"
    assert pool.orders() == [{"token": "new-token"}]
    assert pool.credential_reload_count == 1


def test_g_live_disarmed_does_not_place_and_working_entry_expires(runtime, monkeypatch):
    setup = config.setup_for("09:25", "LONG")
    signal = signal_for(runtime, setup)
    state = runtime.create_order_state(signal, "LIVE", live_quantity=1)
    broker = FakeBroker()
    at = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)
    monkeypatch.setattr(runtime, "_live_arm_state", lambda day: (False, "LIVE_ARM_STRATEGY_MISMATCH"))
    state = runtime.advance_live_order(state, broker, at)
    assert broker.placed == []
    assert state["status_reason"] == "LIVE_ARM_STRATEGY_MISMATCH"
    monkeypatch.setattr(runtime, "_live_arm_state", lambda day: (True, "LIVE_ARMED"))
    state = runtime.advance_live_order(state, broker, at)
    entry_id = state["entry_order_id"]
    state = runtime.advance_live_order(
        state, broker, config.activation_deadline(DAY, setup.confirmation_end) + timedelta(seconds=1)
    )
    assert entry_id in broker.cancelled
    assert state["status"] == "CANCELLED"
    assert state["entry_price"] == 0


def test_in_window_live_execution_error_survives_deadline(runtime):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    observed = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)
    state["created_at_ist"] = observed.isoformat(timespec="seconds")

    runtime._record_live_execution_error(
        state,
        TokenException("Incorrect api_key or access_token."),
        observed,
    )
    state = runtime.advance_live_order(
        state,
        FakeBroker(),
        config.activation_deadline(DAY, setup.confirmation_end)
        + timedelta(seconds=1),
    )

    assert state["status"] == "CANCELLED"
    assert state["status_reason"] == "ENTRY_ACTIVATION_DEADLINE_EXPIRED"
    assert state["entry_terminal_cause"] == "EXECUTION_ERROR:TokenException"
    assert state["execution_error_count"] == 1
    assert state["first_execution_error_type"] == "TokenException"
    assert state["last_execution_error_type"] == "TokenException"
    assert state["first_execution_error_at_ist"] == observed.isoformat(
        timespec="seconds"
    )
    assert state["entry_order_id"] == ""


def test_in_window_disarm_reason_survives_deadline(runtime, monkeypatch):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    observed = config.slot_datetime(DAY, setup.confirmation_end) + timedelta(seconds=15)
    state["created_at_ist"] = observed.isoformat(timespec="seconds")
    broker = FakeBroker()
    monkeypatch.setattr(
        runtime,
        "_live_arm_state",
        lambda _day: (False, "LIVE_ARM_FILE_DISABLED"),
    )

    state = runtime.advance_live_order(state, broker, observed)
    state = runtime.advance_live_order(
        state,
        broker,
        config.activation_deadline(DAY, setup.confirmation_end)
        + timedelta(seconds=1),
    )

    assert state["status"] == "CANCELLED"
    assert state["status_reason"] == "ENTRY_ACTIVATION_DEADLINE_EXPIRED"
    assert state["entry_terminal_cause"] == (
        "ENTRY_BLOCKER:LIVE_ARM_FILE_DISABLED"
    )
    assert state["first_entry_blocker_reason"] == "LIVE_ARM_FILE_DISABLED"
    assert state["last_entry_blocker_reason"] == "LIVE_ARM_FILE_DISABLED"
    assert broker.placed == []


def test_terminal_live_state_does_not_require_broker_client(runtime):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(
        signal_for(runtime, setup), "LIVE", live_quantity=1
    )
    state.update(
        status="CANCELLED",
        status_reason="ENTRY_ACTIVATION_DEADLINE_EXPIRED",
        entry_terminal_cause="EXECUTION_ERROR:TokenException",
    )
    before = deepcopy(state)

    result = runtime.advance_live_order(
        state,
        None,
        config.activation_deadline(DAY, setup.confirmation_end)
        + timedelta(minutes=30),
    )

    assert result == before


def test_g_paper_squareoff_is_1515(runtime):
    assert config.SQUARE_OFF == "15:15"
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(signal_for(runtime, setup), "PAPER")
    state = runtime.advance_paper_order(state, 100.05, config.slot_datetime(DAY, "09:27"))
    state = runtime.advance_paper_order(state, 100.1, config.slot_datetime(DAY, "15:15"))
    assert state["status"] == "CLOSED"
    assert state["exit_reason"] == "SQUARE_OFF"


@pytest.mark.parametrize("trigger", ["squareoff", "kill"])
def test_g_live_full_squareoff_releases_brackets_once(runtime, monkeypatch, trigger):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(signal_for(runtime, setup), "LIVE", live_quantity=1)
    broker = FakeBroker()
    at = config.slot_datetime(DAY, "09:27")
    state = runtime.advance_live_order(state, broker, at)
    broker.complete(state["entry_order_id"], 100.05)
    state = runtime.advance_live_order(state, broker, at)
    if trigger == "kill":
        monkeypatch.setattr(runtime, "_read_json", lambda path: {"enabled": True})
    else:
        at = config.slot_datetime(DAY, "15:15")
    state = runtime.advance_live_order(state, broker, at)
    assert state["status"] == "SQUARE_OFF_PENDING"
    squareoff_id = state["squareoff_order_id"]
    assert state["stop_order_id"] in broker.cancelled
    assert state["target_order_id"] in broker.cancelled
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 1
    state = runtime.advance_live_order(state, broker, at + timedelta(seconds=2))
    assert state["squareoff_order_id"] == squareoff_id
    assert len([row for row in broker.placed if row["order_type"] == "MARKET"]) == 1
    broker.complete(squareoff_id, 100.1)
    state = runtime.advance_live_order(state, broker, at + timedelta(seconds=3))
    assert state["status"] == "CLOSED"
    assert state["quantity"] == 1


def test_g_live_fill_during_entry_cancellation_still_gets_protection(runtime, monkeypatch):
    setup = config.setup_for("09:25", "LONG")
    state = runtime.create_order_state(signal_for(runtime, setup), "LIVE", live_quantity=1)
    broker = FakeBroker()
    state = runtime.advance_live_order(state, broker, config.slot_datetime(DAY, "09:27"))
    monkeypatch.setattr(broker, "cancel_order", lambda **kw: broker.complete(kw["order_id"], 100.1))
    state = runtime.advance_live_order(
        state, broker, config.activation_deadline(DAY, setup.confirmation_end) + timedelta(seconds=1)
    )
    assert state["status"] == "OPEN"
    assert state["stop_order_id"]
    assert state["target_order_id"]
    assert len(broker.placed) == 3
    assert {row["quantity"] for row in broker.placed} == {1}


def test_g_worker_uses_legacy_control_root_and_requires_new_identity(runtime, monkeypatch):
    monkeypatch.setenv(config.LIVE_ACK_ENV, config.LIVE_ACK)
    monkeypatch.setattr(runtime, "_read_json", lambda path: common.read_json(path) if path.exists() else {})
    assert runtime.LIVE_ARM_PATH.parent.name == "v6_live"
    assert runtime.LIVE_ROOT.name == "v13_v10_g_live"
    arm = {"enabled": True, "session_date": DAY.isoformat()}
    common.atomic_write_json(runtime.LIVE_ARM_PATH, arm)
    assert runtime.unmocked_live_arm_state(DAY) == (False, "LIVE_ARM_STRATEGY_MISMATCH")
    arm["strategy_fingerprint"] = config.strategy_fingerprint()
    common.atomic_write_json(runtime.LIVE_ARM_PATH, arm)
    assert runtime.unmocked_live_arm_state(DAY) == (True, "LIVE_ARMED")


@pytest.mark.parametrize("field,bad", [
    ("strategy_version", "FNO_V6_BEST_NET_CASH_EQUITY_20260811"),
    ("strategy_fingerprint", "legacy-fingerprint"),
    ("stop_pct", .2),
    ("target_pct", 10.),
    ("setup_id", "WRONG_SETUP"),
    ("entry_activation_deadline_ist", "2026-09-15T09:27:30+05:30"),
])
def test_g_export_rejects_changed_signal_terms(runtime, isolated_coordinator, field, bad):
    signal = signal_for(runtime, config.setup_for("09:25", "LONG"))
    signal[field] = bad
    write_authority(signal)
    with pytest.raises(RuntimeError):
        coordinator.load_authoritative_signals(DAY)


def test_g_export_rejects_wrong_slot_commit(runtime, isolated_coordinator):
    signal = signal_for(runtime, config.setup_for("09:30", "LONG"))
    write_authority(signal)
    proper = coordinator._confirmation_path(DAY, "09:30")
    snapshot = common.read_json(proper)
    proper.unlink()
    common.atomic_write_json(coordinator._confirmation_path(DAY, "09:25"), snapshot)
    with pytest.raises(RuntimeError, match="identity checks"):
        coordinator.load_authoritative_signals(DAY)


def test_g_export_publishes_strategy_terms_and_rejects_legacy_state(runtime, isolated_coordinator):
    setup = config.setup_for("09:25", "LONG")
    signal = signal_for(runtime, setup)
    write_authority(signal)
    state = runtime.create_order_state(signal, "LIVE", live_quantity=1)
    state.update(status="OPEN", entry_price=100.05)
    state_path = coordinator.profile_order_day_dir(DAY) / f"{signal['signal_id']}.json"
    common.atomic_write_json(state_path, state)
    summary = coordinator.export_snapshot(DAY)
    assert summary["strategy_version"] == config.STRATEGY_VERSION
    assert summary["strategy_fingerprint"] == config.strategy_fingerprint()
    frame = pd.read_csv(coordinator.trades_csv_path(DAY))
    assert frame.loc[0, "setup_id"] == setup.setup_id
    assert frame.loc[0, "quantity"] == 1
    assert frame.loc[0, "stop_pct"] == setup.stop_pct
    assert frame.loc[0, "target_pct"] == setup.target_pct
    assert frame.loc[0, "strategy_version"] == config.STRATEGY_VERSION
    state["strategy_fingerprint"] = "legacy-fingerprint"
    common.atomic_write_json(state_path, state)
    before = state_path.read_bytes()
    with pytest.raises(RuntimeError, match="strategy_fingerprint"):
        coordinator.load_profile_order_states(DAY, {signal["signal_id"]})
    assert state_path.read_bytes() == before


def test_g_live_arm_requires_fingerprint_without_rewriting_controls(isolated_coordinator, monkeypatch):
    monkeypatch.setenv(config.LIVE_ACK_ENV, config.LIVE_ACK)
    arm_path = coordinator.CONTROL_ROOT / "live_arm.json"
    arm = {"enabled": True, "session_date": DAY.isoformat()}
    common.atomic_write_json(arm_path, arm)
    before = arm_path.read_bytes()
    assert coordinator._arm_status(DAY)["arm_reason"] == "LIVE_ARM_STRATEGY_MISMATCH"
    assert arm_path.read_bytes() == before
    arm["strategy_fingerprint"] = config.strategy_fingerprint()
    common.atomic_write_json(arm_path, arm)
    assert coordinator._arm_status(DAY)["armed"] is True
    common.atomic_write_json(coordinator.CONTROL_ROOT / "kill_switch.json", {"enabled": True})
    assert coordinator._arm_status(DAY)["arm_reason"] == "KILL_SWITCH_ENABLED"
    assert not (coordinator.LIVE_ROOT / "live_arm.json").exists()


def test_g_auto_arm_writes_current_date_and_strategy_identity(isolated_coordinator, monkeypatch):
    monkeypatch.setenv(config.LIVE_ACK_ENV, config.LIVE_ACK)
    path = coordinator._auto_arm_session(DAY)
    arm = common.read_json(path)
    assert path == coordinator.CONTROL_ROOT / "live_arm.json"
    assert arm["enabled"] is True
    assert arm["session_date"] == DAY.isoformat()
    assert arm["strategy_version"] == config.STRATEGY_VERSION
    assert arm["strategy_fingerprint"] == config.strategy_fingerprint()
    assert arm["source"] == "V13_V10_G_QTY1_AUTO_ARM"
    assert coordinator._arm_status(DAY)["arm_reason"] == "LIVE_ARMED"


def test_g_workers_keep_one_share_and_do_not_supply_acknowledgement(isolated_coordinator, monkeypatch):
    monkeypatch.delenv(config.LIVE_ACK_ENV, raising=False)
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "stale")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", raising=False)
    monkeypatch.delenv("OTEL_SDK_DISABLED", raising=False)
    environment = coordinator.worker_environment()
    assert environment["FNO_V6_STRATEGY_PROFILE"] == "V13_V10_G"
    assert environment["FNO_V6_EXECUTION_SESSION_NAMESPACE"] == "live_kite_qty1"
    assert environment["EQIDV2_OBSERVABILITY_ENABLED"] == "1"
    assert environment["EQIDV2_OBS_RUN_ID"] == coordinator.RUN_ID
    assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in environment
    assert "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL" not in environment
    assert "OTEL_SDK_DISABLED" not in environment
    assert config.LIVE_ACK_ENV not in environment
    for side in ("LONG", "SHORT"):
        command = coordinator.worker_command(DAY, side)
        assert Path(command[2]).name == "fno_v13_v10_g_live.py"
        assert command[command.index("--live-quantity") + 1] == "1"
        assert coordinator.worker_session_id(side) == f"fno_v13_v10_g_live_kite_qty1_{side.lower()}"
    reconciliation = coordinator.broker_reconciliation_command(DAY)
    assert Path(reconciliation[2]).name == "fno_v13_v10_g_live.py"
    assert reconciliation[reconciliation.index("--role") + 1] == "broker-reconciliation"
    assert "--live-quantity" not in reconciliation
    assert (
        coordinator.worker_session_id("BROKER_RECONCILIATION")
        == "fno_v13_v10_g_live_kite_qty1_broker_reconciliation"
    )


def test_g_exports_use_renamed_session_and_csvs(isolated_coordinator):
    assert coordinator.SESSION_ID == "fno_v13_v10_g_live_kite_qty1"
    assert coordinator.entry_csv_path(DAY, "LONG").name == "signals_2026-09-15_fno_id_v13_v10_g_long.csv"
    assert coordinator.entry_csv_path(DAY, "SHORT").name == "signals_2026-09-15_fno_id_v13_v10_g_short.csv"
    assert coordinator.trades_csv_path(DAY).name == "live_trades_2026-09-15_fno_id_v13_v10_g.csv"
    payload = coordinator.export_snapshot(DAY)
    assert payload["session_id"] == "fno_v13_v10_g_live_kite_qty1"
    assert coordinator.EXECUTION_PROFILE == "live_kite_qty1"
    assert coordinator._display_label() == "V13-V10-G"


def test_named_g_wrapper_pins_profile_without_changing_execution_identity(monkeypatch):
    before_fingerprint = config.strategy_fingerprint()
    monkeypatch.setenv("FNO_LIVE_GENERATION", "wrong")
    monkeypatch.setenv("FNO_V6_STRATEGY_PROFILE", "wrong")
    monkeypatch.delenv(config.LIVE_ACK_ENV, raising=False)
    monkeypatch.delitem(sys.modules, "fno_v6_live_kite_session", raising=False)
    spec = importlib.util.spec_from_file_location(
        "g_named_coordinator_test", ROOT / "fno_v13_v10_g_live_kite_session.py"
    )
    wrapper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wrapper)
    named = wrapper.coordinator
    assert named.SESSION_ID == "fno_v13_v10_g_live_kite_qty1"
    assert named.config.strategy_fingerprint() == before_fingerprint
    assert named.config.ORDER_TAG_PREFIX == "FVG"
    assert named.EXECUTION_PROFILE == "live_kite_qty1"
    assert named.QUANTITY_POLICY == "FIXED_ONE_SHARE"
    assert named.CONTROL_ROOT.name == "v6_live"
    assert named.LIVE_ROOT.name == "v13_v10_g_live"
    environment = named.worker_environment()
    assert environment["FNO_LIVE_GENERATION"] == "v6"
    assert environment["FNO_V6_STRATEGY_PROFILE"] == "V13_V10_G"
    assert config.LIVE_ACK_ENV not in environment
    called = []
    monkeypatch.setattr(named, "main", lambda argv: called.append(argv) or 17)
    assert wrapper.main(["--readiness-only"]) == 17
    assert called == [["--readiness-only"]]


def test_g_readiness_only_prepares_outputs_without_worker_or_control_changes(
    isolated_coordinator, monkeypatch
):
    def unexpected(*args, **kwargs):
        raise AssertionError("Readiness-only mode attempted to start workers or enter the run loop.")

    monkeypatch.setattr(coordinator.subprocess, "Popen", unexpected)
    monkeypatch.setattr(common, "is_trading_day", unexpected)
    monkeypatch.delenv(config.LIVE_ACK_ENV, raising=False)
    arm_path = coordinator.CONTROL_ROOT / "live_arm.json"
    common.atomic_write_json(arm_path, {"enabled": False, "session_date": DAY.isoformat()})
    before = arm_path.read_bytes()
    args = coordinator.build_parser().parse_args(["--readiness-only", "--session-date", DAY.isoformat()])
    assert coordinator.run(args) == 0
    status = common.read_json(coordinator.STATUS_PATH)
    assert status["state"] == "READY_DISARMED"
    assert status["armed"] is False
    assert status["readiness_only"] is True
    assert status["workers_started"] is False
    assert status["execution_enabled"] is False
    assert status["strategy_version"] == config.STRATEGY_VERSION
    assert arm_path.read_bytes() == before
    assert not (coordinator.CONTROL_ROOT / "kill_switch.json").exists()
    for side in ("LONG", "SHORT"):
        frame = pd.read_csv(coordinator.entry_csv_path(DAY, side))
        assert frame.empty
        assert list(frame.columns) == coordinator.ENTRY_COLUMNS
    assert pd.read_csv(coordinator.trades_csv_path(DAY)).empty


@pytest.mark.parametrize("failure", ["export", "long_worker", "reconciliation"])
def test_reporting_or_other_side_failure_keeps_healthy_live_manager_alive(
    isolated_coordinator, monkeypatch, failure
):
    processes = []
    snapshots = []
    sleeps = []

    class FakeProcess:
        def __init__(self, command, **kwargs):
            self.pid = 100 + len(processes)
            self.return_code = 2 if failure == "long_worker" and not processes else None
            self.command = command
            processes.append(self)

        def poll(self):
            return self.return_code

        def terminate(self):
            raise AssertionError("A healthy LIVE trade manager was terminated.")

    def snapshot(day, **kwargs):
        snapshots.append(kwargs)
        if failure == "export" and len(snapshots) == 1:
            raise RuntimeError("Simulated CSV validation failure")
        return dict(signals=0, order_states=0, filled_trades=0, open=0, arm_reason="LIVE_ARM_FILE_DISABLED")

    def sleep(seconds):
        sleeps.append(seconds)
        assert processes[1].poll() is None
        for process in processes:
            if process.poll() is None:
                process.return_code = 0

    monkeypatch.setattr(coordinator.subprocess, "Popen", FakeProcess)
    monkeypatch.setattr(coordinator, "export_snapshot", snapshot)
    monkeypatch.setattr(
        coordinator,
        "_child_process_status",
        lambda role, process: {
            "session_id": coordinator.worker_session_id(role),
            "pid": process.pid,
            "return_code": process.poll(),
            **(
                {"state": "DEGRADED", "broker_truth_available": False}
                if failure == "reconciliation"
                and role == "BROKER_RECONCILIATION"
                else {}
            ),
        },
    )
    monkeypatch.setattr(coordinator.time, "sleep", sleep)
    monkeypatch.setattr(config, "validate_strategy", lambda: None)
    monkeypatch.setattr(config, "attest_selected_backtest", lambda: None)
    args = SimpleNamespace(session_date=DAY.isoformat(), allow_non_trading_day=True, once=False, poll_sec=.01)
    assert coordinator.run(args) == (2 if failure == "long_worker" else 0)
    assert sleeps == [.01]
    assert len(processes) == 3
    assert processes[2].command[processes[2].command.index("--role") + 1] == "broker-reconciliation"
    if failure in {"long_worker", "reconciliation"}:
        assert snapshots[0]["state"] == "DEGRADED"
    else:
        saved = common.read_json(coordinator.STATUS_PATH)
        assert saved["export_unavailable"] is True
        assert saved["state"] == "DEGRADED"
