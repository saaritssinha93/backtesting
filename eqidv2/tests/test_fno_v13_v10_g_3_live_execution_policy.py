"""Pure budget/identity tests plus isolated state/coordinator integration."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import json
import socket
import urllib.request

import pytest

import fno_oi_common as common
import fno_v13_v10_g_3_live_execution_policy as policy
from tests.test_fno_v13_v10_g_3_runtime import DAY, candidate, config, isolated, runtime


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Sizing tests cannot access a network or broker")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)


@pytest.mark.parametrize("price,quantity", [(102.,490), (250.,200), (50000.,1), (50000.01,0),
                                          (2000.,25), (.05,1000000), (333.33,150)])
def test_floor_budget(price, quantity):
    sized = policy.size_position(price)
    assert sized["quantity"] == quantity
    assert sized["estimated_exposure_rs"] <= 50000.
    assert (quantity + 1) * price > 50000.
    assert (sized["capital_rs"], sized["leverage"], sized["target_exposure_rs"]) == (10000.,5.,50000.)


@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf"), -float("inf"), None, True, "bad"])
def test_invalid_prices_fail(price):
    with pytest.raises(ValueError):
        policy.size_position(price)


@pytest.mark.parametrize("lot", [0, -1, 1.5, True, float("nan")])
def test_invalid_lots_fail(lot):
    with pytest.raises(ValueError):
        policy.size_position(100., lot)


def test_lot_constraint_overrides_and_separate_paper_contract():
    assert policy.size_position(100.,25)["quantity"] == 0
    assert policy.requested_quantity(102.,1,10000) == 490
    assert policy.requested_quantity(102.,1,100) == 100
    for override in (0, -1, 1.5, True, float("inf")):
        with pytest.raises(ValueError):
            policy.requested_quantity(102.,1,override)
    assert config.size_position(250.,1,live=True,capital_rs=1e9,leverage=100).quantity == 200
    assert config.size_position(250.,1,live=False).quantity == 2000
    assert config.strategy_fingerprint() == "9761be9583ef63f4627bad43d3b67f6c5b12480e09a5d853325fe2b994e82275"
    assert policy.policy_fingerprint() != config.strategy_fingerprint()
    policy.validate_policy()


def production_signal(runtime):
    row = candidate()
    row["data_contract"] = runtime.hybrid.DATA_CONTRACT_VERSION
    return runtime.select_entry_signals([row],DAY,"09:30",capital_rs=100000.,leverage=5.)[0]


def test_live_state_budget_roundtrip_and_reduction_override(runtime):
    signal = production_signal(runtime)
    runtime._validate_signal(signal,DAY)
    for override,quantity in ((None,490),(100,100),(10000,490)):
        state = runtime.create_order_state(signal,"LIVE",live_quantity=override)
        runtime._validate_order_state(json.loads(json.dumps(state)),signal,"LIVE",live_quantity=override)
        policy.validate_order_sizing(state)
        assert state["quantity"] == state["requested_quantity"] == quantity
        assert state["capital_rs"] == 10000.
        assert state["execution_profile"] == "live_kite_qty1"
        assert state["live_execution_profile"] == "G3_LIVE_10K_5X"
        # Fill price may slip and a broker may fill fewer shares. Neither may
        # rewrite the immutable requested budget at a restart.
        state.update(status="OPEN", entry_price=103., quantity=20)
        runtime._validate_order_state(state,signal,"LIVE",live_quantity=override)


@pytest.mark.parametrize("field,value", [("quantity",491), ("requested_quantity",491),
    ("strategy_sized_quantity",491), ("capital_rs",100000.), ("target_exposure_rs",500000.),
    ("sizing_reference_price",100.), ("live_execution_policy_fingerprint","old"),
    ("quantity_policy","FIXED_ONE_SHARE"), ("execution_quantity_override",1)])
def test_saved_live_tampering_rejected(runtime,field,value):
    signal = production_signal(runtime)
    state = runtime.create_order_state(signal,"LIVE")
    state[field] = value
    with pytest.raises((ValueError,RuntimeError)):
        runtime._validate_order_state(state,signal,"LIVE")


def test_old_paper_signal_valid_but_cannot_create_new_live_state(runtime):
    signal = production_signal(runtime)
    signal["live_sizing"] = {**asdict(config.size_position(102.,1,live=False)),
                             "quantity":1,"estimated_exposure_rs":102.,"state":"LIVE_QUANTITY_ONE"}
    for key in policy.signal_metadata():
        signal.pop(key)
    runtime._validate_signal(signal,DAY)
    state = runtime.create_order_state(signal,"PAPER")
    runtime._validate_order_state(state,signal,"PAPER")
    with pytest.raises(ValueError,match="execution policy"):
        runtime.create_order_state(signal,"LIVE")


def test_coordinator_exports_current_policy_and_real_quantity(runtime):
    coordinator = isolated("g3_budget_coordinator","fno_v6_live_kite_session.py")
    signal = production_signal(runtime)
    state = runtime.create_order_state(signal,"LIVE")
    common.atomic_write_json(coordinator._confirmation_path(DAY,"09:30"), {
        "strategy_version":config.STRATEGY_VERSION,"strategy_fingerprint":config.strategy_fingerprint(),
        "session_date":DAY.isoformat(),"state":"SUCCESS","selected_signal_ids":[signal["signal_id"]]})
    common.atomic_write_json(coordinator.SIGNAL_ROOT/DAY.isoformat()/(signal["signal_id"]+".json"),signal)
    path = coordinator.profile_order_day_dir(DAY)/(signal["signal_id"]+".json")
    common.atomic_write_json(path,state)
    payload = coordinator.export_snapshot(DAY)
    policy.validate_execution_metadata(payload)
    frame = coordinator._entry_rows([signal],{signal["signal_id"]:state},"LONG")
    assert frame.iloc[0].quantity == 490
    state["quantity"] = 491
    common.atomic_write_json(path,state)
    with pytest.raises(ValueError,match="quantity"):
        coordinator.export_snapshot(DAY)


def test_legacy_open_state_can_only_manage_existing_position(runtime,monkeypatch):
    from tests.test_fno_v13_v10_g_3_partial_fills import MemoryBroker
    signal = production_signal(runtime)
    state = runtime.create_order_state(signal,"LIVE")
    for key in policy.execution_metadata():
        state.pop(key,None)
    state.update(status="OPEN",entry_order_id="OLD_ENTRY",entry_price=102.,entry_at_ist=config.slot_datetime(DAY,"09:32").isoformat(),
                 quantity=1,strategy_sized_quantity=1,execution_quantity_override=1,quantity_policy="FIXED_ONE_SHARE",
                 capital_rs=100000.,leverage=5.,target_exposure_rs=500000.)
    runtime._validate_order_state(state,signal,"LIVE")
    assert state["live_execution_policy_mismatch"] == "LEGACY_QUANTITY_ONE_PROTECTIVE_MANAGEMENT_ONLY"
    broker = MemoryBroker()
    runtime.advance_live_order(state,broker,config.slot_datetime(DAY,"09:33"))
    assert len(broker.placed) == 2
    assert all(row["transaction_type"] == "SELL" and row["quantity"] == 1 for row in broker.placed)
    with pytest.raises(ValueError,match="execution policy"):
        runtime._place_live_entry_order(state,broker,config.slot_datetime(DAY,"09:33"),
            transaction_type="BUY",tag="FAKE",order_type="MARKET",reason="TEST")


def test_partial_exit_pnl_unavailable_in_reports_and_csv(runtime,monkeypatch):
    state = runtime.create_order_state(production_signal(runtime),"LIVE")
    state.update(status="SQUARE_OFF_PENDING",entry_price=102.,entry_at_ist=config.slot_datetime(DAY,"09:32").isoformat(),
                 remaining_quantity=390,live_mtm_available=False,
                 live_mtm_unavailable_reason="PARTIAL_EXIT_RECONCILIATION_PENDING")
    monkeypatch.setattr(runtime,"load_order_states",lambda *a,**k:[state])
    monkeypatch.setattr(runtime,"_pipeline_notice_lines",lambda *a:([],None))
    assert runtime.net_summary([state])["unrealized_complete"] is False
    assert "UNAVAILABLE (partial exit)" in runtime.render_net_result(DAY)
    assert "UNAVAILABLE (partial exit)" in runtime.render_trade_log(DAY)
    report = runtime._render_worker_report(DAY,"LONG","LIVE")
    assert "UNAVAILABLE (partial exit)" in report and "Rs 10,000" in report and "Rs 50,000" in report
    coordinator = isolated("g3_partial_pnl_coordinator","fno_v6_live_kite_session.py")
    frame = coordinator._trade_rows([state])
    assert frame.iloc[0].pnl_rs is None
    assert frame.iloc[0].remaining_quantity == 390
