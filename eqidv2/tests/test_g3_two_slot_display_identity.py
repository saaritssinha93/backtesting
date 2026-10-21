"""The V9 session remains readable without permitting V9 execution or future data."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

import dashboard_cockpit as cockpit
import fno_eq_id_monitor_detail as detail
import fno_strategy_registry as registry
import fno_v13_v10_g_3_contract as contract
import log_dashboard_server as dashboard


DAY = "2026-10-15"
PREVIOUS_VERSION = "V13-V10-G-3-PRODUCTION-V9"
PREVIOUS_FINGERPRINT = "d65237ffc1e5caf932258fc91b15c487951d1b6b75d32614e4fd0cbe88fa61b5"


@pytest.fixture
def migration(monkeypatch):
    state = dict(profile="V13_V10_G_3", previous_session_identity=dict(through_session=DAY,
        strategy_version=PREVIOUS_VERSION, strategy_fingerprint=PREVIOUS_FINGERPRINT))
    monkeypatch.setattr(registry, "deployment_state", lambda: state)
    profile = registry.PROFILES["V13_V10_G_3"]
    for module in (detail, cockpit):
        monkeypatch.setattr(module, "ACTIVE_PROFILE", profile)
        monkeypatch.setattr(module, "STRATEGY_VERSION", contract.STRATEGY_VERSION)
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", profile)
    monkeypatch.setattr(dashboard, "FNO_V13_V10_G_STRATEGY_VERSION", contract.STRATEGY_VERSION)
    return state


def test_display_exception_is_only_exact_explicit_previous_session(migration):
    previous = registry.display_identity(DAY)
    assert previous.previous_session is True
    assert (previous.version, previous.fingerprint) == (PREVIOUS_VERSION, PREVIOUS_FINGERPRINT)
    assert len(previous.configuration["active_setups"]) == 59
    for day in ("2026-10-14", "2026-10-16"):
        current = registry.display_identity(day)
        assert current.previous_session is False
        assert current.version == contract.STRATEGY_VERSION
        assert current.fingerprint == contract.strategy_fingerprint()
        assert len(current.configuration["active_setups"]) == 61
    # Runtime authority continues to see only the current production identity.
    assert registry.expected_fingerprint() == contract.strategy_fingerprint() != PREVIOUS_FINGERPRINT
    migration.pop("previous_session_identity")
    assert registry.display_identity(DAY).previous_session is False


@pytest.mark.parametrize("field,value", [("strategy_version", "V13-V10-G-3-PRODUCTION-V8"),
    ("strategy_fingerprint", "bad"), ("through_session", "2026-10-16")])
def test_deployment_record_cannot_authorize_other_archive_or_date(migration, field, value):
    migration["previous_session_identity"][field] = value
    with pytest.raises(ValueError, match="pinned migration session"):
        registry.display_identity(value if field == "through_session" else DAY)


def test_modified_archived_configuration_fails_closed(migration, monkeypatch, tmp_path):
    path = registry.WORKSPACE / "configs/strategies/v13_v10_g_3_production_v9.json"
    target = tmp_path / "configs/strategies" / path.name
    target.parent.mkdir(parents=True)
    target.write_bytes(path.read_bytes() + b" ")
    monkeypatch.setattr(registry, "WORKSPACE", tmp_path)
    with pytest.raises(ValueError, match="configuration file changed"):
        registry.display_identity(DAY)


def _write_evidence(tmp_path, day, version, fingerprint):
    root = tmp_path / "v13_v10_g_3_live"
    orders = root / "orders/PAPER" / day
    orders.mkdir(parents=True)
    row = dict(mode="PAPER", session_date=day, strategy_version=version, strategy_fingerprint=fingerprint,
        signal_id="recorded-order", tradingsymbol="TEST", side="LONG", status="CLOSED", quantity=10,
        updated_at_ist=f"{day}T10:00:00+05:30", net_pnl_rs=99.)
    (orders / "valid.json").write_text(json.dumps(row), encoding="utf-8")
    (orders / "invalid.json").write_text(json.dumps({**row, "signal_id": "invalid-order",
        "strategy_fingerprint": "invalid-fingerprint"}), encoding="utf-8")
    scanner = root / "scanner_5m" / day
    scanner.mkdir(parents=True)
    snapshot = dict(session_date=day, strategy_version=version, strategy_fingerprint=fingerprint,
        signal_end="09:20", confirmation_end="09:21", published_at_ist=f"{day}T09:20:10+05:30",
        state="SUCCESS", feature_evaluations=[], feature_evaluation_count=0, scanner_complete=True)
    (scanner / "slot_0920.json").write_text(json.dumps(snapshot), encoding="utf-8")
    snapshot.update(signal_end="09:25", confirmation_end="09:26", published_at_ist=f"{day}T09:25:10+05:30",
                    strategy_fingerprint="invalid-fingerprint")
    (scanner / "slot_0925.json").write_text(json.dumps(snapshot), encoding="utf-8")
    return root


@pytest.mark.parametrize("day,version,fingerprint", [(DAY, PREVIOUS_VERSION, PREVIOUS_FINGERPRINT),
    ("2026-10-16", contract.STRATEGY_VERSION, contract.strategy_fingerprint())])
def test_readers_select_one_session_identity_and_reject_other_fingerprints(migration, tmp_path, day, version, fingerprint):
    _write_evidence(tmp_path, day, version, fingerprint)
    now = datetime.fromisoformat(f"{day}T14:50:00+05:30")
    monitor = detail.build_monitor_detail(tmp_path, day, now_ist=now)
    assert monitor["identity"]["strategy_version"] == version
    assert monitor["identity"]["strategy_fingerprint"] == fingerprint
    coverage = {entry["slot"]: entry for entry in monitor["coverage"]}
    assert coverage["09:20"]["scanner_state"] == "RECORDED"
    assert coverage["09:25"]["scanner_state"] == "INVALID_STRATEGY"
    result = cockpit.load_cockpit_data(tmp_path, day, now_ist=now)
    assert result["identity"]["version"] == version
    assert result["identity"]["fingerprint"] == fingerprint
    assert [row["signal_id"] for row in result["orders"]] == ["recorded-order"]
    assert result["evidence"]["orders_incomplete"] is True
    assert detail._BUILD_IDENTITY.get() is None
    assert cockpit._REQUEST_FINGERPRINT.get() is None


def test_old_confirmation_and_opening_defaults_use_archived_rules(migration):
    previous = registry.display_identity(DAY)
    token = detail._BUILD_IDENTITY.set(previous)
    try:
        row = dict(base_side="LONG", signal_end="10:10", setup_id="")
        assert detail._confirmation_threshold(row) == 1.1
        assert detail._display_nifty_slots() == ("09:25",)
        assert "G3_ARCHIVED_CONTRACT" in detail._source_reference("confirmation_volume")
        row.update(minimum_confirmation_volume_ratio=.9)
        assert detail._confirmation_threshold(row) == .9
        row = dict(base_side="SHORT", signal_end="09:20", gate_nifty_0925_short=True)
        assert detail._gate(row, "gate_nifty_0925_short", "5m", pinned=True)["status"] == "NOT_APPLICABLE"
    finally:
        detail._BUILD_IDENTITY.reset(token)
    assert detail._confirmation_threshold(dict(base_side="LONG", signal_end="10:10", setup_id="1011_LONG")) == .6


@pytest.mark.parametrize("options", [False, True])
def test_dashboard_cards_bind_prior_status_to_its_explicit_session(migration, options):
    status = dict(status="RUNNING", session_date=DAY, strategy_version=PREVIOUS_VERSION,
                  strategy_fingerprint=PREVIOUS_FINGERPRINT)
    if options:
        card = dashboard.FNO_V13_V10_G_OPTIONS_PAPER_CARD_IDS[0]
        status.update(options_strategy_version="FNO_V13_V10_G_3_OPTIONS_ONE_LOT_ATM_SL30_T40P4_20261011",
                      execution_mode="PAPER", role="long-entry", stop_pct="30.0", target_pct="40.4")
    else:
        card = dashboard.FNO_V13_V10_G_CARD_IDS[0]
    result = dashboard._apply_fno_v13_v10_g_identity(card, status)
    assert result["strategy_identity_state"] == "MATCH"
    assert result["status"] == "RUNNING"
    assert result["expected_strategy_version"] == PREVIOUS_VERSION
    assert result["configured_strategy_version"] == contract.STRATEGY_VERSION
    assert result["display_identity"]["scope"] == "PREVIOUS_SESSION_READ_ONLY"
    for changed in ({"session_date": "2026-10-16"}, {"session_date": None}, {"strategy_fingerprint": "bad"}):
        result = dashboard._apply_fno_v13_v10_g_identity(card, {**status, **changed})
        assert result["strategy_identity_state"] == "MISMATCH"


@pytest.mark.parametrize("date_fields", [dict(ts=f"{DAY}T14:54:00+05:30"),
    dict(session_date_ist=DAY, ts=f"{DAY}T14:54:00+05:30")])
def test_actual_worker_status_date_formats_preserve_prior_session(migration, date_fields):
    row = dict(status="RUNNING", strategy_version=PREVIOUS_VERSION,
               strategy_fingerprint=PREVIOUS_FINGERPRINT, **date_fields)
    assert dashboard._apply_fno_v13_v10_g_identity(dashboard.FNO_V13_V10_G_CARD_IDS[0], row)["strategy_identity_state"] == "MATCH"
    assert dashboard._fno_g_status_session_date({**row, "session_date": "2026-10-16"}) is None
    assert dashboard._fno_g_status_session_date({**row, "ts": f"{DAY}T14:54:00"}) is None


def test_dashboard_confirmation_accepts_old_only_on_pinned_session(migration):
    payload = dict(session_date=DAY, strategy_version=PREVIOUS_VERSION, strategy_fingerprint=PREVIOUS_FINGERPRINT,
        signal_end="10:10", confirmation_end="10:11", state="SUCCESS", scanner_complete=True,
        candidate_count=7, confirmation_bars=7, ineligible_no_candle_count=0, error_count=0,
        selected_long=0, selected_short=1)
    def valid(row, day=DAY):
        return dashboard._fno_eq_id_timeline_v6_confirmation_valid(row,
            session_date=day, signal_slot="10:10", confirmation_slot="10:11")
    assert valid(payload)
    assert not valid({**payload, "strategy_fingerprint": "bad"})
    assert not valid({**payload, "session_date": "2026-10-16"}, "2026-10-16")


def test_dashboard_timeline_preserves_old_scanner_and_labels_its_identity(migration, monkeypatch, tmp_path):
    root = tmp_path / "v13_v10_g_3_live"
    snapshot = dict(session_date=DAY, strategy_version=PREVIOUS_VERSION, strategy_fingerprint=PREVIOUS_FINGERPRINT,
        signal_end="09:20", confirmation_end="09:21", state="SUCCESS", long_candidates=1, short_candidates=0)
    confirmation = dict(snapshot, scanner_complete=True, candidate_count=1, confirmation_bars=1,
        ineligible_no_candle_count=0, error_count=0, selected_long=1, selected_short=0)
    evidence = {root / "scanner_5m" / DAY / "slot_0920.json": snapshot,
                root / "confirmation_1m" / DAY / "slot_0921.json": confirmation}
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", tmp_path)
    monkeypatch.setattr(dashboard, "market_closed_reason", lambda _day: None)
    monkeypatch.setattr(dashboard, "_read_json_dict", lambda path: evidence.get(path, {}))
    monkeypatch.setattr(dashboard, "_read_csv_tail_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(dashboard, "_fno_eq_id_timeline_jsonl", lambda *args, **kwargs: [])
    timeline = dashboard._build_fno_eq_id_strategy_timelines([], now_ist=datetime.fromisoformat(f"{DAY}T10:12:00+05:30"))
    opening = next(row for row in timeline["five_minute_rows"] if row["slot"] == "09:20")
    assert "EVIDENCE MISMATCH" not in opening["v6"]
    assert "SEL L1/S0" in opening["v6"]
    assert opening["purpose"] == "G-3 LONG SELECTION (09:21 CONFIRM)"
    assert timeline["display_identity"]["strategy_version"] == PREVIOUS_VERSION


def test_cockpit_captures_worker_orders_before_slow_detail_scan(migration, monkeypatch, tmp_path):
    root = _write_evidence(tmp_path, DAY, PREVIOUS_VERSION, PREVIOUS_FINGERPRINT)
    path = root / "orders/PAPER" / DAY / "valid.json"
    def slow_detail(*args, **kwargs):
        # A live worker can advance this file while the full-universe scan runs.
        row = json.loads(path.read_text())
        row["updated_at_ist"] = f"{DAY}T10:00:30+05:30"
        path.write_text(json.dumps(row), encoding="utf-8")
        return dict(warnings=[], coverage=[], rows_5m=[], rows_1m=[], state="NO_EVIDENCE")
    monkeypatch.setattr(cockpit, "build_monitor_detail", slow_detail)
    result = cockpit.load_cockpit_data(tmp_path, DAY, now_ist=datetime.fromisoformat(f"{DAY}T10:00:00+05:30"))
    assert [row["signal_id"] for row in result["orders"]] == ["recorded-order"]
    assert result["orders"][0]["updated_at_ist"] == f"{DAY}T10:00:00+05:30"
