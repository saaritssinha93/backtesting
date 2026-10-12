"""Old LIVE journal policy must not borrow current G3 dashboard identity."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

import dashboard_cockpit as cockpit
import fno_eq_id_monitor_detail as monitor
from fno_strategy_registry import PROFILES, expected_fingerprint
from fno_v13_v10_g_3_live_execution_policy import execution_metadata

DAY = "2026-10-12"
NOW = datetime.fromisoformat(DAY + "T10:00:00+05:30")


@pytest.fixture
def g3(monkeypatch):
    profile = PROFILES["V13_V10_G_3"]
    for module in (cockpit, monitor):
        monkeypatch.setattr(module, "ACTIVE_PROFILE", profile)
        monkeypatch.setattr(module, "STRATEGY_VERSION", profile.version)
    return profile


def event(profile, mode="LIVE"):
    return dict(timestamp_utc=DAY + "T09:59:30+05:30", event_type="order_state_transition",
        context=dict(mode=mode, session_date=DAY, signal_id="event-check",
            strategy_version=profile.version, strategy_fingerprint=expected_fingerprint(profile.name)),
        data=dict(tradingsymbol="TEST", side="LONG", state_before="PENDING_ENTRY", state_after="OPEN",
            quantity=50, reason="FILLED", **execution_metadata()))


def write_event(root, row):
    path = root / "order_events" / row["context"]["mode"] / (DAY + ".jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")


@pytest.mark.parametrize("mode,corruption,accepted", [
    ("LIVE", None, True), ("LIVE", "missing", False), ("LIVE", "old", False),
    ("LIVE", "capital", False), ("PAPER", "missing", True),
])
def test_both_event_readers_bind_live_policy_only(tmp_path, g3, mode, corruption, accepted):
    row = event(g3, mode)
    if corruption == "missing":
        for key in execution_metadata():
            row["data"].pop(key)
    elif corruption == "old":
        row["data"]["live_execution_policy_fingerprint"] = "old-qty1-policy"
        row["data"]["quantity"] = 1
    elif corruption == "capital":
        row["data"]["capital_per_trade_rs"] = 100000
    write_event(tmp_path, row)
    cockpit_warnings, monitor_warnings = [], []
    rows = cockpit._events(tmp_path, DAY, mode, NOW, cockpit_warnings)
    observed = monitor._event_rows(tmp_path, DAY, NOW, monitor_warnings)
    assert bool(rows) is accepted and bool(observed) is accepted
    if not accepted:
        assert any("unverified G3 execution policy" in text for text in cockpit_warnings)
        assert any("unverified G3 execution policy" in text for text in monitor_warnings)


def test_legacy_g_live_events_keep_their_existing_read_only_contract(tmp_path, monkeypatch):
    profile = PROFILES["V13_V10_G"]
    for module in (cockpit, monitor):
        monkeypatch.setattr(module, "ACTIVE_PROFILE", profile)
        monkeypatch.setattr(module, "STRATEGY_VERSION", profile.version)
    row = event(profile)
    row["context"]["strategy_fingerprint"] = "historical-g-fingerprint"
    row["data"] = dict(tradingsymbol="TEST", quantity=1)
    write_event(tmp_path, row)
    assert len(cockpit._events(tmp_path, DAY, "LIVE", NOW, [])) == 1
    assert len(monitor._event_rows(tmp_path, DAY, NOW, [])) == 1


@pytest.mark.parametrize("status", ["OPEN", "CLOSED", "SQUARE_OFF_PENDING"])
def test_monitor_rejects_zero_quantity_filled_live_snapshots(tmp_path, g3, status):
    row = dict(mode="LIVE", session_date=DAY, strategy_version=g3.version,
        strategy_fingerprint=expected_fingerprint(g3.name), signal_id="zero-check", tradingsymbol="TEST",
        side="LONG", status=status, quantity=0, requested_quantity=50, strategy_sized_quantity=50,
        sizing_reference_price=1000., trigger_price=1000., lot_size=1, capital_rs=10000.,
        updated_at_ist=DAY + "T09:59:30+05:30", **execution_metadata())
    path = tmp_path / "orders/LIVE/live_kite_qty1" / DAY / "zero.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(row), encoding="utf-8")
    warnings = []
    assert monitor._order_rows(tmp_path, DAY, NOW, warnings) == []
    assert warnings
