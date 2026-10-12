"""Options status identity differs from the equity LIVE sizing contract."""
from __future__ import annotations

import pytest

import log_dashboard_server as dashboard
from fno_strategy_registry import PROFILES, expected_fingerprint


@pytest.fixture
def g3(monkeypatch):
    profile = PROFILES["V13_V10_G_3"]
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", profile)
    monkeypatch.setattr(dashboard, "FNO_V13_V10_G_STRATEGY_VERSION", profile.version)
    return profile


def status(profile, role="long-entry", **changes):
    return dict(status="RUNNING", strategy_version=profile.version,
        options_strategy_version="FNO_V13_V10_G_3_OPTIONS_ONE_LOT_ATM_SL30_T40P4_20261011",
        execution_mode="PAPER", role=role, stop_pct="30.0", target_pct="40.4") | changes


@pytest.mark.parametrize("index,role", list(enumerate(("long-entry", "short-entry", "trade-logger", "net-result"))))
def test_existing_options_status_without_checksum_is_declared_unverified_not_mismatch(g3, index, role):
    result = dashboard._apply_fno_v13_v10_g_identity(
        dashboard.FNO_V13_V10_G_OPTIONS_PAPER_CARD_IDS[index], status(g3, role))
    assert result["status"] == "RUNNING"
    assert result["strategy_identity_state"] == "DECLARED_MATCH"
    assert result["fingerprint_evidence"] == "NOT_PUBLISHED"
    assert "unverified" in result["strategy_identity_detail"]
    assert "unverified" in result["derived_status"]
    activity = dashboard._fno_eq_id_monitor_activity(
        dashboard.FNO_V13_V10_G_OPTIONS_PAPER_CARD_IDS[index], result, "")
    assert "fingerprint not published (unverified)" in activity
    assert "live_execution_policy_state" not in result


def test_options_status_with_current_equity_checksum_is_verified_match(g3):
    result = dashboard._apply_fno_v13_v10_g_identity(
        dashboard.FNO_V13_V10_G_OPTIONS_PAPER_CARD_IDS[0],
        status(g3, strategy_fingerprint=expected_fingerprint(g3.name)))
    assert result["strategy_identity_state"] == result["fingerprint_evidence"] == "MATCH"
    assert result["status"] == "RUNNING"


@pytest.mark.parametrize("changes,field", [
    (dict(strategy_fingerprint="legacy-g-checksum"), "strategy_fingerprint"),
    (dict(strategy_version="FNO_V13_V10_G_RETAINED_20260914"), "strategy_version"),
    (dict(options_strategy_version="FNO_V13_V10_G_OPTIONS_ONE_LOT_ATM_SL30_T40P4_20260915"), "options_strategy_version"),
    (dict(execution_mode="LIVE"), "execution_mode"),
    (dict(role="short-entry"), "role"),
    (dict(stop_pct="1.25"), "stop_pct"), (dict(target_pct="40"), "target_pct"),
    (dict(stop_pct=float("nan")), "stop_pct"),
])
def test_options_present_wrong_identity_or_declared_policy_still_fails(g3, changes, field):
    result = dashboard._apply_fno_v13_v10_g_identity(
        dashboard.FNO_V13_V10_G_OPTIONS_PAPER_CARD_IDS[0], status(g3, **changes))
    assert result["strategy_identity_state"] == "MISMATCH"
    assert result["status"] == "PARTIAL"
    assert field in result["strategy_identity_detail"]


def test_options_correction_does_not_relax_equity_live_identity(g3):
    row = status(g3)
    equity_card = dashboard.FNO_V13_V10_G_CARD_IDS[-1]
    result = dashboard._apply_fno_v13_v10_g_identity(equity_card, row)
    assert result["strategy_identity_state"] == "MISMATCH"
    assert result["status"] == "PARTIAL"
    result = dashboard._apply_fno_v13_v10_g_identity(
        equity_card, row | dict(strategy_fingerprint=expected_fingerprint(g3.name)))
    assert result["live_execution_policy_state"] == "AWAITING_RUNTIME"
    assert result["status"] == "PARTIAL"


def test_legacy_g_options_status_behavior_stays_unchanged(monkeypatch):
    profile = PROFILES["V13_V10_G"]
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", profile)
    monkeypatch.setattr(dashboard, "FNO_V13_V10_G_STRATEGY_VERSION", profile.version)
    result = dashboard._apply_fno_v13_v10_g_identity(
        dashboard.FNO_V13_V10_G_OPTIONS_PAPER_CARD_IDS[0],
        dict(status="RUNNING", strategy_version=profile.version))
    assert result["status"] == "RUNNING" and result["strategy_identity_state"] == "MATCH"
