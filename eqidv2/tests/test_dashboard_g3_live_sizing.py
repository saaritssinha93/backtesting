"""Current G3 sizing must be explicit, separate from historical/PAPER evidence."""
import json
import csv
from datetime import datetime

import pytest

import dashboard_cockpit as cockpit
import fno_eq_id_monitor_detail as monitor
import fno_strategy_registry as registry
import log_dashboard_server as dashboard
from fno_v13_v10_g_3_live_execution_policy import (
    execution_metadata,
    policy_fingerprint,
    previous_capped_execution_metadata,
)

DAY = "2026-10-12"
NOW = datetime.fromisoformat(f"{DAY}T10:00:00+05:30")


@pytest.fixture
def g3(monkeypatch):
    profile = registry.PROFILES["V13_V10_G_3"]
    for module in (cockpit, monitor):
        monkeypatch.setattr(module, "ACTIVE_PROFILE", profile)
        monkeypatch.setattr(module, "STRATEGY_VERSION", profile.version)
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", profile)
    monkeypatch.setattr(dashboard, "FNO_V13_V10_G_STRATEGY_VERSION", profile.version)
    return profile


def order(profile, mode="LIVE"):
    return dict(mode=mode, session_date=DAY, strategy_version=profile.version,
                strategy_fingerprint=registry.expected_fingerprint(profile.name),
                signal_id="g3-sizing-check", tradingsymbol="TEST", side="LONG",
                status="OPEN", quantity=50, requested_quantity=50, strategy_sized_quantity=50,
                sizing_reference_price=1000, trigger_price=1000, lot_size=1, capital_rs=10000,
                updated_at_ist=f"{DAY}T09:59:30+05:30", **execution_metadata())


def test_deployment_metadata_reflects_explicit_live_approval(g3):
    state = registry.deployment_state()
    assert state["live_execution_policy"] == execution_metadata()
    assert state["deployment_stage"] == "LIVE_APPROVED"
    assert state["live_activation_authorized"] is True


def test_display_updates_labels_preserves_transport_and_legacy(monkeypatch, g3):
    text = "V13-V10-G Live Kite - Quantity 1 | v13_v10_g_live_kite_qty1"
    updated = dashboard._active_strategy_display(text)
    assert "₹10,000 × 5" in updated and "Quantity 1" not in updated
    assert "v13_v10_g_live_kite_qty1" in updated
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", registry.PROFILES["V13_V10_G"])
    assert dashboard._active_strategy_display(text) == text


@pytest.mark.parametrize("metadata,expected", [(True, "MATCH"), (False, "AWAITING_RUNTIME")])
def test_status_requires_current_live_policy_even_when_strategy_matches(g3, metadata, expected):
    status = dict(strategy_version=g3.version, strategy_fingerprint=registry.expected_fingerprint(g3.name), status="SUCCESS")
    if metadata:
        status.update({key: str(value) for key, value in execution_metadata().items()})
    result = dashboard._apply_fno_v13_v10_g_identity(dashboard.FNO_V13_V10_G_CARD_IDS[-1], status)
    assert result["live_execution_policy_state"] == expected
    assert result["status"] == ("SUCCESS" if metadata else "PARTIAL")


@pytest.mark.parametrize("corruption", [None, "fingerprint", "oversized", "fractional", "missing_basis"])
def test_both_readers_reject_wrong_live_sizing(tmp_path, g3, corruption):
    row = order(g3)
    if corruption == "fingerprint":
        row["live_execution_policy_fingerprint"] = "old-one-share-policy"
    elif corruption == "oversized":
        row["quantity"] = 51
    elif corruption == "fractional":
        row["quantity"] = 1.5
    elif corruption == "missing_basis":
        row.pop("sizing_reference_price")
    root = tmp_path / g3.live_dir
    path = root / "orders/LIVE/live_kite_qty1" / DAY / "test.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(row), encoding="utf-8")
    warnings = []
    rows, incomplete = cockpit._order_rows(root, DAY, "LIVE", NOW, warnings)
    monitor_rows = monitor._order_rows(root, DAY, NOW, [])
    assert bool(rows) == bool(monitor_rows) == (corruption is None)
    assert incomplete == (corruption is not None)


def test_both_readers_keep_exact_previous_policy_rows_visible(tmp_path, g3):
    row = order(g3)
    row.update(previous_capped_execution_metadata())
    root = tmp_path / g3.live_dir
    path = root / "orders/LIVE/live_kite_qty1" / DAY / "test.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(row), encoding="utf-8")
    warnings = []
    rows, incomplete = cockpit._order_rows(root, DAY, "LIVE", NOW, warnings)
    monitor_rows = monitor._order_rows(root, DAY, NOW, [])
    assert len(rows) == len(monitor_rows) == 1
    assert incomplete is False


def test_paper_history_does_not_require_new_live_policy(tmp_path, g3):
    row = order(g3, mode="PAPER")
    for key in execution_metadata():
        row.pop(key)
    row.update(quantity=500, capital_rs=100000)
    root = tmp_path / g3.live_dir
    path = root / "orders/PAPER" / DAY / "paper.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(row), encoding="utf-8")
    rows, incomplete = cockpit._order_rows(root, DAY, "PAPER", NOW, [])
    assert len(rows) == 1 and not incomplete
    assert len(monitor._order_rows(root, DAY, NOW, [])) == 1


def test_empty_live_cockpit_reports_configured_budget_without_inventing_orders(tmp_path, g3):
    data = cockpit.load_cockpit_data(tmp_path, DAY, mode="LIVE", now_ist=NOW)
    assert data["sizing"]["capital_per_entry_rs"] == 10000
    assert data["sizing"]["target_exposure_rs"] == 50000
    assert data["sizing"]["max_reserved_open_positions"] is None
    assert data["sizing"]["max_daily_entries"] is None
    assert data["sizing"]["max_reserved_gross_exposure_rs"] is None
    assert "portfolio-admission caps are disabled" in data["sizing"]["details"]
    assert "unlimited" in data["sizing"]["details"]
    assert data["identity"]["configured_live_execution_policy"]["live_execution_policy_fingerprint"] == policy_fingerprint()
    assert data["summary"]["orders"] == 0 and data["summary"]["total_pnl_rs"] is None


def test_partial_exit_reports_remaining_shares_without_inventing_unrealized_pnl(g3):
    raw = order(g3)
    raw.update(status="SQUARE_OFF_PENDING", entry_price=1000, last_price=1010,
               quote_at_ist=f"{DAY}T09:59:30+05:30", estimated_cost_rs=20,
               g3_exit_fills_by_order={"fake-exit": {"quantity": 10, "notional": 10100}})
    row = cockpit._project_order(dict(raw=raw, stamp=NOW, conflict=False, source="fake.json"), DAY, "LIVE", NOW)
    assert row["quantity"] == 50 and row["remaining_quantity"] == 40
    assert row["unrealized_pnl_rs"] is None
    assert row["partial_exit_pnl_note"]


@pytest.mark.parametrize("corruption", [None, "status_policy", "trade_policy", "quantity"])
def test_main_timeline_rejects_old_or_oversized_live_ledger(tmp_path, monkeypatch, g3, corruption):
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", tmp_path / "fno")
    monkeypatch.setattr(dashboard, "FNO_MULTI_PAPER_ROOT", tmp_path / "multi")
    monkeypatch.setattr(dashboard, "FNO_MULTI_PAPER_STATUS_PATH", tmp_path / "multi/status.json")
    monkeypatch.setattr(dashboard, "SLOT_READY_5M_DIR", tmp_path / "slots")
    root = dashboard.FNO_OI_ROOT / g3.live_dir / "live_kite"
    root.mkdir(parents=True)
    status = dict(session_date=DAY, strategy_version=g3.version,
                  strategy_fingerprint=registry.expected_fingerprint(g3.name), armed=True,
                  state="RUNNING", **execution_metadata())
    trade = dict(status, execution_mode="LIVE", ticker="TEST", signal_end="09:30", quantity=50,
                 requested_quantity=50, sizing_reference_price=1000,
                 entry_time=f"{DAY}T09:31:00+05:30", exit_time="", pnl_rs=0)
    if corruption == "status_policy":
        status.pop("live_execution_policy_fingerprint")
    elif corruption == "trade_policy":
        trade["quantity_policy"] = "FIXED_ONE_SHARE"
    elif corruption == "quantity":
        trade["quantity"] = 51
    (root / "status.json").write_text(json.dumps(status), encoding="utf-8")
    with (root / f"live_trades_{DAY}_fno_id_{g3.slug}.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(trade))
        writer.writeheader()
        writer.writerow(trade)
    data = dashboard._build_fno_eq_id_strategy_timelines([], now_ist=NOW)
    five = next(row["v6_live"] for row in data["five_minute_rows"] if row["slot"] == "09:30")
    minute = next(row["v6_live"] for row in data["one_minute_rows"] if row["minute"] == "09:31")
    if corruption:
        assert "MISMATCH" in five and "MISMATCH" in minute
        assert "fills unavailable" in five and "0 live fill" not in five
    else:
        assert "LIVE 1" in five and "ENTRY 1" in minute
