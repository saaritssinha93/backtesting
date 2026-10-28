"""Offline coverage of G3's cross-setup, cross-side symbol admission guard."""
from copy import deepcopy
import json
import subprocess
import sys

import pytest

import fno_v13_v10_g_3_live_config as g3_config
import fno_v13_v10_g_paper as paper
from g3_live_risk import admit_live_entry, LiveRiskLimitError, LiveRiskIntegrityError
from tests.test_fno_v13_v10_g_3_runtime import runtime, candidate, DAY
from tests.test_g3_live_risk import (
    order, admit, write, path_for, policy, NOW, TERMINAL,
    runtime_state, approve_entry_owner,
)


def paper_order(identity, *, symbol="HINDUNILVR", side="SHORT", status="OPEN"):
    return dict(signal_id=identity, tradingsymbol=symbol, side=side, status=status,
        mode="PAPER", session_date="2026-10-27", strategy_version=g3_config.STRATEGY_VERSION,
        strategy_fingerprint=g3_config.strategy_fingerprint(), capital_rs=100000.,
        quantity=280, entry_price=1781.5 if status == "OPEN" else 0.,
        entry_at_ist="2026-10-27T09:26:32+05:30" if status == "OPEN" else "",
        net_pnl_rs=0., updated_at_ist="2026-10-27T09:41:25+05:30")


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_paper_blocks_another_setup_in_either_direction(side):
    first = paper_order("0926_SHORT_HINDUNILVR")
    second = paper_order("0941_"+side+"_HINDUNILVR", symbol=" hindunilvr ", side=side)
    decision = paper.capacity_from_states([first], second, strategy_config=g3_config)
    assert not decision["allowed"]
    assert decision["reason"] == "SYMBOL_ALREADY_ACTIVE"
    assert decision["blocking_signal_ids"] == [first["signal_id"]]
    assert decision["available_capital_rs"] == 900000.


@pytest.mark.parametrize("change", [dict(status="CLOSED"), dict(status="CANCELLED"),
    dict(status="PENDING_ENTRY"), dict(tradingsymbol="OTHER"), dict(mode="LIVE"),
    dict(session_date="2026-10-26")])
def test_paper_releases_closed_symbols_and_does_not_reserve_unfilled_triggers(change):
    first = {**paper_order("FIRST"), **change}
    assert paper.capacity_from_states([first], paper_order("NEXT"), strategy_config=g3_config)["allowed"]


@pytest.mark.parametrize("bad", [None, "", "  ", 123, "HIND UNILVR"])
def test_paper_unidentified_active_position_fails_closed(bad):
    with pytest.raises(paper.PortfolioStateError, match="symbol"):
        paper.capacity_from_states([paper_order("FIRST", symbol=bad)],
                                   paper_order("SECOND"), strategy_config=g3_config)


def test_paper_rejection_preserves_unfilled_state_and_existing_exits(tmp_path):
    first = paper_order("FIRST")
    write(tmp_path / "FIRST.json", first)
    pending = paper_order("SECOND", status="PENDING_ENTRY")
    proposed = {**pending, "status": "OPEN", "entry_price": 1770.2,
                "entry_at_ist": pending["updated_at_ist"]}
    blocked = paper.enforce_fill_capacity(pending, proposed, tmp_path, strategy_config=g3_config)
    assert blocked["status"] == "BLOCKED_PORTFOLIO"
    assert blocked["status_reason"] == "SYMBOL_ALREADY_ACTIVE"
    assert blocked["entry_price"] == 0 and blocked["entry_at_ist"] == ""
    assert pending["status"] == "PENDING_ENTRY"
    closed = {**first, "status": "CLOSED"}
    assert paper.enforce_fill_capacity(first, closed, tmp_path, strategy_config=g3_config) == closed


def test_paper_runtime_persists_block_and_allows_later_setup_after_exit(runtime):
    signal = runtime.select_entry_signals(
        [{**candidate(), "data_contract": runtime.hybrid.DATA_CONTRACT_VERSION}],
        DAY, "09:30", capital_rs=100000., leverage=5.)[0]
    first = runtime.create_order_state(signal, "PAPER")
    second = {**deepcopy(first), "signal_id": first["signal_id"]+"_SECOND_SLOT"}
    now = runtime.config.slot_datetime(DAY, "09:32")
    filled = runtime._advance_g_paper_state(first, 102., now)
    assert filled["status"] == "OPEN"
    blocked = runtime._advance_g_paper_state(second, 102., now)
    stored = json.loads(runtime._order_path(DAY, "PAPER", second["signal_id"]).read_text())
    assert stored == blocked
    assert blocked["status_reason"] == "SYMBOL_ALREADY_ACTIVE"
    assert blocked["entry_price"] == 0
    closed = runtime._advance_g_paper_state(filled, float(filled["target_price"])+1., now)
    assert closed["status"] == "CLOSED"
    # An earlier terminal rejection stays blocked; a later distinct signal can enter.
    assert runtime._advance_g_paper_state(second, 102., now)["status"] == "BLOCKED_PORTFOLIO"
    third = {**deepcopy(first), "signal_id": first["signal_id"]+"_THIRD_SLOT"}
    assert runtime._advance_g_paper_state(third, 102., now)["status"] == "OPEN"


def test_two_paper_processes_cannot_fill_the_same_symbol(tmp_path):
    script = """import json,sys
from pathlib import Path
from types import SimpleNamespace
import fno_v13_v10_g_paper as p
root=Path(sys.argv[1]); previous=json.loads(sys.argv[2])
c=SimpleNamespace(STRATEGY_PROFILE='V13_V10_G_3', STRATEGY_VERSION=previous['strategy_version'],
    strategy_fingerprint=lambda:previous['strategy_fingerprint'], CAPITAL_PER_ENTRY_RS=100000., PORTFOLIO_CAPITAL_RS=1000000.)
with p.paper_portfolio_lock(root):
    proposed={**previous,'status':'OPEN','entry_price':1781.5,'entry_at_ist':previous['updated_at_ist']}
    result=p.enforce_fill_capacity(previous,proposed,root,strategy_config=c)
    (root/(result['signal_id']+'.json')).write_text(json.dumps(result))
    print(result['status'],flush=True)
"""
    children = [subprocess.Popen([sys.executable, "-B", "-c", script, str(tmp_path),
        json.dumps(paper_order(name, side=side, status="PENDING_ENTRY"))],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for name, side in (("SLOT_ONE", "LONG"), ("SLOT_TWO", "SHORT"))]
    try:
        results = [p.communicate(timeout=15) for p in children]
        assert [p.returncode for p in children] == [0, 0], results
        assert sorted(stdout.strip() for stdout, _ in results) == ["BLOCKED_PORTFOLIO", "OPEN"]
    finally:
        for p in children:
            if p.poll() is None:
                p.kill()
                p.communicate(timeout=10)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
@pytest.mark.parametrize("status,uncertain", [
    ("PENDING_ENTRY", False), ("OPEN", False), ("SQUARE_OFF_PENDING", False), ("CANCELLED", True)])
def test_live_reserves_symbol_across_sides_slots_namespaces_and_uncertain_submissions(tmp_path, side, status, uncertain):
    first = {**order("FIRST", side="SHORT"), "tradingsymbol": "HINDUNILVR"}
    path = admit(tmp_path, first, namespace="worker_one")
    first.update(status=status, entry_submission_uncertain=uncertain)
    write(path, first)
    second = {**order("SECOND", side=side), "tradingsymbol": " hindunilvr "}
    with pytest.raises(LiveRiskLimitError, match="SYMBOL_ALREADY_ACTIVE"):
        admit_live_entry(second, book_root=tmp_path, state_path=path_for(tmp_path, "SECOND", "worker_two"),
            now=NOW, policy=policy, terminal_states=TERMINAL)
    assert second["status"] == "BLOCKED_PORTFOLIO"
    assert second["live_risk_blocking_signal_ids"] == ["FIRST"]
    assert second["entry_submission_attempt_count"] == 0


@pytest.mark.parametrize("status", ["CLOSED", "CANCELLED", "ENTRY_REJECTED"])
def test_live_reentry_after_definite_terminal_state(tmp_path, status):
    first = {**order("FIRST"), "tradingsymbol": "HINDUNILVR"}
    path = admit(tmp_path, first)
    write(path, {**first, "status": status})
    second = {**order("SECOND"), "tradingsymbol": "HINDUNILVR"}
    admit(tmp_path, second)
    assert second["live_risk_decision"] == "ADMITTED"


def test_live_missing_active_symbol_fails_closed_and_cannot_change_durable_symbol(tmp_path):
    first = order("FIRST")
    path = admit(tmp_path, first)
    changed = {**first, "tradingsymbol": "OTHER"}
    with pytest.raises(LiveRiskIntegrityError, match="durable symbol"):
        admit_live_entry(changed, book_root=tmp_path, state_path=path, now=NOW,
                         policy=policy, terminal_states=TERMINAL)
    malformed = deepcopy(first)
    malformed.pop("tradingsymbol")
    write(path, malformed)
    with pytest.raises(LiveRiskIntegrityError, match="symbol"):
        admit(tmp_path, order("NEXT"))


def test_live_runtime_blocks_second_symbol_before_any_broker_call(runtime, monkeypatch):
    approve_entry_owner(monkeypatch)
    calls = []

    class Broker:
        def place_order(self, **payload):
            calls.append(payload)
            return "FIRST_ORDER"

    first, second = runtime_state(runtime, "_ONE"), runtime_state(runtime, "_TWO")
    second["tradingsymbol"] = first["tradingsymbol"]
    now = runtime.config.slot_datetime(DAY, "09:33")
    runtime._place_live_entry_order(first, Broker(), now, transaction_type="BUY",
        tag="FIRST", order_type="MARKET", reason="TEST")
    with pytest.raises(LiveRiskLimitError, match="SYMBOL_ALREADY_ACTIVE"):
        runtime._place_live_entry_order(second, Broker(), now, transaction_type="SELL",
            tag="SECOND", order_type="MARKET", reason="TEST")
    stored = json.loads(runtime._order_path(DAY, "LIVE", second["signal_id"]).read_text())
    assert len(calls) == 1
    assert stored["status"] == "BLOCKED_PORTFOLIO"
    assert stored["entry_submission_attempt_count"] == 0
    assert stored["live_risk_blocking_signal_ids"] == [first["signal_id"]]
