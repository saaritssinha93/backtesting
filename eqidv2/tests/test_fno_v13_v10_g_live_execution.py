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
    environment = coordinator.worker_environment()
    assert environment["FNO_V6_STRATEGY_PROFILE"] == "V13_V10_G"
    assert environment["FNO_V6_EXECUTION_SESSION_NAMESPACE"] == "live_kite_qty1"
    assert config.LIVE_ACK_ENV not in environment
    for side in ("LONG", "SHORT"):
        command = coordinator.worker_command(DAY, side)
        assert Path(command[2]).name == "fno_v13_v10_g_live.py"
        assert command[command.index("--live-quantity") + 1] == "1"
        assert coordinator.worker_session_id(side) == f"fno_v13_v10_g_live_kite_qty1_{side.lower()}"


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


@pytest.mark.parametrize("failure", ["export", "long_worker"])
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
    monkeypatch.setattr(coordinator.time, "sleep", sleep)
    monkeypatch.setattr(config, "validate_strategy", lambda: None)
    monkeypatch.setattr(config, "attest_selected_backtest", lambda: None)
    args = SimpleNamespace(session_date=DAY.isoformat(), allow_non_trading_day=True, once=False, poll_sec=.01)
    assert coordinator.run(args) == (2 if failure == "long_worker" else 0)
    assert sleeps == [.01]
    if failure == "long_worker":
        assert snapshots[0]["state"] == "DEGRADED"
    else:
        saved = common.read_json(coordinator.STATUS_PATH)
        assert saved["export_unavailable"] is True
        assert saved["state"] == "DEGRADED"
