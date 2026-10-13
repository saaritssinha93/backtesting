"""Production provenance, active routing, and historical isolation boundaries."""
import csv
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

import dashboard_flow as flow
import fno_strategy_registry as registry
from fno_v13_v10_g_3_contract import (
    STRATEGY_VERSION, LEGACY_STRATEGY_VERSION, LEGACY_STRATEGY_FINGERPRINT,
    strategy_fingerprint,
)
from fno_v13_v10_g_3_backtest import publish_current_backtest
from research.g3_0950_long_backtest import PREFERRED_CONFIG
from ai_platform.observability.g3_analysis import generate_g3_analysis, verified_package


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def package(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    monkeypatch.setenv("FNO_ACTIVE_STRATEGY_PROFILE", "V13_V10_G_3")
    root = tmp_path / "backtesting_result_v13_v10_g_3"
    run = root / "history" / "runs" / "2026-10-09" / "new-production"
    daily = run / "data" / "daily_results.csv"
    ledger = run / "canonical" / "portfolio_trades.csv"
    write_csv(daily, [dict(day="2026-10-09", trades=1, wins=1, losses=0, net_pnl=100),
                      dict(day="2026-10-08", trades=0, wins=0, losses=0, net_pnl=0)])
    write_csv(ledger, [dict(day="2026-10-09", tradingsymbol="AAA", side="LONG", setup_id="0925_LONG",
                           filled=True, portfolio_executed=True, portfolio_net_profit_rupees=100,
                           strategy_version=STRATEGY_VERSION, strategy_fingerprint=strategy_fingerprint())])
    (run / "source_manifest.json").write_text('{}', encoding="utf-8")
    manifest = dict(schema_version="fno_g3_production_run_v1", execution_authority=False,
                    role="cumulative_history", complete=True, state="SUCCESS", strategy_version=STRATEGY_VERSION,
                    strategy_fingerprint=strategy_fingerprint(), session_date="2026-10-09", coverage={},
                    metrics=dict(net_pnl=100, trades=1), artifacts={name: dict(path=str(path.relative_to(run)), sha256=sha(path))
                    for name, path in (("daily_results", daily), ("portfolio_trades", ledger), ("source_manifest", run / "source_manifest.json"))})
    pointer = dict(role="cumulative_history", run_dir=str(run.relative_to(root)), session_date="2026-10-09", strategy_version=STRATEGY_VERSION,
                   strategy_fingerprint=strategy_fingerprint())
    def promote():
        (run / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        pointer["manifest_sha256"] = sha(run / "run_manifest.json")
        (root / "latest").mkdir(exist_ok=True)
        (root / "latest" / "current_history.json").write_text(json.dumps(pointer), encoding="utf-8")
    promote()
    return root, run, manifest, pointer, promote


def test_canonical_nested_artifacts_and_zero_day_survive(package):
    payload = flow.load_flow_data()
    assert payload["selected_run"]["verified"] is True
    assert payload["summary"]["net_pnl"] == 100
    assert len(payload["trades"]) == 1 and len(payload["daily"]) == 2
    assert not any("reconcile" in item for item in payload["warnings"])
    assert str(package[0]) not in json.dumps(payload)


def test_deployed_identity_tracks_the_shared_contract():
    state = registry.deployment_state()
    assert registry.PROFILES["V13_V10_G_3"].version == STRATEGY_VERSION
    assert state["strategy_version"] == STRATEGY_VERSION
    assert state["strategy_fingerprint"] == strategy_fingerprint()
    assert state["deployment_stage"] == "PAPER_VALIDATION"
    assert state["live_activation_authorized"] is False


def test_archived_v1_history_keeps_its_identity_and_results(package):
    root, run, manifest, pointer, promote = package
    for record in (manifest, pointer):
        record.update(strategy_version=LEGACY_STRATEGY_VERSION,
                      strategy_fingerprint=LEGACY_STRATEGY_FINGERPRINT)
    ledger = run / "canonical" / "portfolio_trades.csv"
    with ledger.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    for row in rows:
        row.update(strategy_version=LEGACY_STRATEGY_VERSION,
                   strategy_fingerprint=LEGACY_STRATEGY_FINGERPRINT)
    write_csv(ledger, rows)
    manifest["artifacts"]["portfolio_trades"]["sha256"] = sha(ledger)
    promote()
    archive = root / "latest" / "archived" / LEGACY_STRATEGY_VERSION / "current_history.json"
    archive.parent.mkdir(parents=True)
    archive.write_bytes((root / "latest/current_history.json").read_bytes())
    payload = flow.load_flow_data()
    assert len(payload["runs"]) == 1  # Current and archive pointers are the same immutable run.
    selected = payload["selected_run"]
    assert selected["strategy_version"] == LEGACY_STRATEGY_VERSION
    assert selected["strategy_matches_active"] is False
    assert selected["source_label"].startswith("Archived ")
    assert payload["summary"]["net_pnl"] == 100
    assert any(STRATEGY_VERSION in warning for warning in payload["warnings"])
    (root / "latest/current_history.json").unlink()
    assert flow.load_flow_data(selected["id"])["summary"]["net_pnl"] == 100
    manifest["strategy_fingerprint"] = strategy_fingerprint()
    (run / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    pointer["manifest_sha256"] = sha(run / "run_manifest.json")
    archive.write_text(json.dumps(pointer), encoding="utf-8")
    assert flow.load_flow_data()["state"] == "empty"


def test_production_history_outranks_future_research(package):
    root, *_ = package
    write_csv(root / "runs" / "2099-01-01" / "future" / "daily_results.csv", [dict(day="2099-01-01", net_pnl=999)])
    payload = flow.load_flow_data()
    assert payload["selected_run"]["role"] == "cumulative_history"
    research = next(item for item in payload["runs"] if item["run_name"] == "future")
    assert flow.load_flow_data(research["id"])["summary"]["net_pnl"] == 999


def test_verified_current_0950_backtest_becomes_latest_without_replacing_production(package, monkeypatch):
    root, *_ = package
    production_pointer = (root / "latest/current_history.json").read_bytes()
    run = root / "backtests" / "runs" / "2026-10-12" / "g3_0950_t075"
    days = [(date(2026, 10, 12) - timedelta(days=48 - index)).isoformat()
            for index in range(49)]
    write_csv(run / "daily_results.csv", [dict(day=day, variant_fills=int(index == 48),
        variant_wins=int(index == 48), variant_net_pnl_rs=100 if index == 48 else 0)
        for index, day in enumerate(days)])
    write_csv(run / "variant_trades.csv", [dict(day=days[-1], tradingsymbol="AAA", side="LONG",
        setup_id="0951_LONG", filled=True, portfolio_executed=True,
        portfolio_net_profit_rupees=100)])
    (run / "config.json").write_bytes(PREFERRED_CONFIG.read_bytes())
    (run / "summary.json").write_text(json.dumps(dict(
        variant_id="V13-V10-G-3-BT-0950-LONG-T075-V1", scope="BACKTEST_ONLY",
        execution_authority=False, sessions=49, first_date=days[0], last_date=days[-1],
        created_at_ist="2026-10-12T17:40:00+05:30", frozen_baseline_parity=True,
        existing_g3_trades_unchanged=True,
        with_0950_long=dict(fills=1, wins=1, net_pnl_rs=100))), encoding="utf-8")
    code = Path(__file__).resolve().parents[1] / "research/g3_0950_long_backtest.py"
    files = {name: sha(run / name) for name in
        ("daily_results.csv", "variant_trades.csv", "config.json", "summary.json")}
    (run / "manifest.json").write_text(json.dumps(dict(
        variant_id="V13-V10-G-3-BT-0950-LONG-T075-V1", execution_authority=False,
        code=str(code), code_sha256=sha(code), files=files)), encoding="utf-8")
    publish_current_backtest(root, run)
    selected = flow.load_flow_data()
    assert selected["selected_run"]["role"] == "backtest_history"
    assert selected["selected_run"]["target_pct"] == .75
    assert selected["selected_run"]["promoted_rules"] is True
    assert any("historical simulation" in warning and "09:50 LONG / 0.75%" in warning
               for warning in selected["warnings"])
    assert any("modeled fills are not PAPER or broker orders" in warning
               and "LIVE order execution is currently disabled" in warning
               for warning in selected["warnings"])
    monkeypatch.setattr(flow, "g3_live_activation_authorized", lambda: True)
    authorized = flow.load_flow_data()
    assert any("LIVE execution authorization is active" in warning
               for warning in authorized["warnings"])
    assert selected["summary"]["net_pnl"] == 100
    assert (root / "latest/current_history.json").read_bytes() == production_pointer
    current_pointer = (root / "latest/current_backtest.json").read_bytes()
    (run / "daily_results.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="manifest or artifact"):
        publish_current_backtest(root, run)
    assert (root / "latest/current_backtest.json").read_bytes() == current_pointer
    assert flow.load_flow_data()["selected_run"]["role"] == "cumulative_history"


@pytest.mark.parametrize("corruption", ["manifest", "ledger", "fingerprint", "role", "path", "incomplete", "undocumented_gap", "schema", "authority", "date"])
def test_invalid_package_cannot_fall_back_or_generate_analysis(package, tmp_path, corruption):
    root, run, manifest, pointer, promote = package
    if corruption == "manifest":
        (run / "run_manifest.json").write_text('{}', encoding="utf-8")
    elif corruption == "ledger":
        (run / "canonical" / "portfolio_trades.csv").write_text('tampered', encoding="utf-8")
    else:
        if corruption == "fingerprint": manifest["strategy_fingerprint"] = "old-G"
        if corruption == "role": manifest["role"] = "research"
        if corruption == "path": manifest["artifacts"]["portfolio_trades"]["path"] = "../../../../escape.csv"
        if corruption == "incomplete": manifest["complete"] = False
        if corruption == "undocumented_gap": manifest["state"] = "SUCCESS_WITH_EXCLUDED_SESSIONS"
        if corruption == "schema": manifest["schema_version"] = "research_v1"
        if corruption == "authority": manifest["execution_authority"] = True
        if corruption == "date": manifest["session_date"] = "2026-10-10"
        promote()
    write_csv(root / "runs" / "2026-10-09" / "legacy" / "daily_results.csv", [dict(day="2026-10-09", net_pnl=999)])
    assert flow.load_flow_data()["state"] == "empty"
    output = tmp_path / "analysis"
    with pytest.raises((ValueError, KeyError, OSError)):
        generate_g3_analysis(source_root=root, output_root=output)
    assert not output.exists()


def test_excluded_sessions_visible_not_zero_days(package):
    _, _, manifest, _, promote = package
    manifest.update(state="SUCCESS_WITH_EXCLUDED_SESSIONS", coverage={"excluded_sessions": [{"day": "2026-10-01", "state": "BLOCKED_INCOMPLETE_DATA"}]})
    promote()
    payload = flow.load_flow_data()
    assert payload["state"] == "ready"
    assert any("1 source sessions" in value for value in payload["warnings"])
    assert "2026-10-01" not in {row["date"] for row in payload["daily"]}


def test_failed_new_attempt_keeps_prior_verified_history_with_warning(package):
    root, *_ = package
    (root / "status.json").write_text(json.dumps(dict(session_date="2026-10-12", state="BLOCKED_INCOMPLETE_DATA")))
    payload = flow.load_flow_data()
    assert payload["summary"]["net_pnl"] == 100
    assert any("2026-10-12: BLOCKED_INCOMPLETE_DATA" in item for item in payload["warnings"])


def test_g3_cockpit_rejects_wrong_consistent_fingerprint(monkeypatch):
    import dashboard_cockpit as cockpit
    monkeypatch.setattr(cockpit, "ACTIVE_PROFILE", registry.PROFILES["V13_V10_G_3"])
    assert not cockpit._active_fingerprint_valid({"strategy_fingerprint": "consistent-but-old"})
    assert cockpit._active_fingerprint_valid({"strategy_fingerprint": strategy_fingerprint()})


@pytest.mark.parametrize("flag", [True, "true", 1])
def test_monitor_rejects_relaxed_policy_even_with_matching_identity(monkeypatch, flag):
    import fno_eq_id_monitor_detail as detail
    monkeypatch.setattr(detail, "ACTIVE_PROFILE", registry.PROFILES["V13_V10_G_3"])
    row = dict(session_date="2026-10-09", strategy_version=STRATEGY_VERSION,
               strategy_fingerprint=strategy_fingerprint(), relaxed_0925_long_base_branch=flag)
    assert detail._row_state(row, row, "2026-10-09", "09:25", "5m") == "INVALID_STRATEGY_POLICY"
    assert detail._confirmation_threshold({"side": "LONG"}) == 1.1
    assert detail._confirmation_threshold({"side": "SHORT"}) == 1.2


def test_analysis_reconciles_canonical_ledger_and_preserves_old_output(package, tmp_path):
    root, *_ = package
    old = tmp_path / "v13_v10_g_strategy_research" / "latest" / "sentinel.md"
    old.parent.mkdir(parents=True)
    old.write_text("preserve G", encoding="utf-8")
    result = generate_g3_analysis(source_root=root, output_root=tmp_path / "v13_v10_g_3_strategy_research")
    assert result["baseline"]["net_profit_rupees"] == 100
    assert result["execution_authority"] is False
    assert len(result["reports"]) == 15
    assert old.read_text(encoding="utf-8") == "preserve G"


def test_registry_routes_values_without_corrupting_historical_versions():
    profile = registry.PROFILES["V13_V10_G_3"]
    assert profile.reference("run_fno_v13_v10_g_live_long.bat") == "run_fno_v13_v10_g_3_live_long.bat"
    assert profile.reference("EQIDV2_backtesting_result_v13_v10_g_1620") == "EQIDV2_backtesting_result_v13_v10_g_3_1620"
    assert profile.reference("v13_v10_g_2 v13_v10_g_3") == "v13_v10_g_2 v13_v10_g_3"
    assert profile.display("V13-V10-G V13-V10-G-2 V13-V10-G-3") == "V13-V10-G-3 V13-V10-G-2 V13-V10-G-3"


def test_environment_profile_cannot_grant_live_authority(monkeypatch, tmp_path):
    path = tmp_path / "deployment.json"
    path.write_text(json.dumps(dict(profile="V13_V10_G_3", deployment_stage="PAPER_VALIDATION", live_activation_authorized=False)))
    monkeypatch.setattr(registry, "ACTIVE_PROFILE_PATH", path)
    monkeypatch.setenv("FNO_ACTIVE_STRATEGY_PROFILE", "V13_V10_G_3")
    assert not registry.g3_live_activation_authorized()


def test_report_status_honors_pending_state_hash_and_source(package, tmp_path, monkeypatch):
    import log_dashboard_server as dashboard
    root, _, _, pointer, _ = package
    output = tmp_path / "v13_v10_g_3_strategy_research"
    generate_g3_analysis(source_root=root, output_root=output)
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", registry.PROFILES["V13_V10_G_3"])
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_ROOT", root)
    monkeypatch.setattr(dashboard, "V13_V10_G_STRATEGY_RESEARCH_LATEST_DIR", output / "latest")
    report = output / "latest" / "latest_fno_v13_v10_g_3_research_predictions.md"
    assert dashboard._research_artifact_status(report)["status"] == "AWAITING_G3_PROSPECTIVE_EVIDENCE"
    pointer["manifest_sha256"] = "new-current-generation"
    (root / "latest/current_history.json").write_text(json.dumps(pointer))
    assert dashboard._research_artifact_status(report)["status"] == "STALE_SOURCE"
    report.write_text("tampered")
    assert dashboard._research_artifact_status(report)["status"] == "INVALID_EVIDENCE"


def test_main_daily_report_rejects_unverified_saved_markdown(package, monkeypatch):
    import log_dashboard_server as dashboard
    root, *_ = package  # Contains a verified HISTORY, not a daily production pointer.
    old_report = root / "latest/latest_backtesting_result_v13_v10_g_3.md"
    old_report.write_text("# Misleading saved result\nNet profit: 999999")
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", registry.PROFILES["V13_V10_G_3"])
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_ROOT", root)
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_REPORT", old_report)
    _, _, body = dashboard._backtesting_v13_v10_g_view({"status": "SUCCESS"}, today_ist="2026-10-09")
    assert "AWAITING_VERIFIED_G3_REPORT" in body and "999999" not in body


def test_main_daily_report_labels_verified_v1_as_archived(package, monkeypatch):
    import log_dashboard_server as dashboard
    root, *_ = package
    run = root / "production/runs/2026-10-09/prior-v1"
    daily = run / "daily_results.csv"
    ledger = run / "portfolio_trades.csv"
    write_csv(daily, [dict(day="2026-10-09", trades=1, net_pnl=100)])
    write_csv(ledger, [dict(day="2026-10-09", tradingsymbol="AAA", portfolio_net_profit_rupees=100,
                           strategy_version=LEGACY_STRATEGY_VERSION,
                           strategy_fingerprint=LEGACY_STRATEGY_FINGERPRINT)])
    source = run / "source_manifest.json"
    source.write_text("{}", encoding="utf-8")
    identity = dict(strategy_version=LEGACY_STRATEGY_VERSION,
                    strategy_fingerprint=LEGACY_STRATEGY_FINGERPRINT)
    manifest = dict(schema_version="fno_g3_production_run_v1", execution_authority=False,
        role="production_daily", complete=True, state="SUCCESS", session_date="2026-10-09", **identity,
        metrics=dict(trades=1, net_pnl_rupees=100), artifacts={name:dict(path=path.name, sha256=sha(path))
            for name,path in (("daily_results",daily),("portfolio_trades",ledger),("source_manifest",source))})
    manifest_path = run / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    pointer_path = root / "latest/current_run.json"
    pointer = dict(role="production_daily", session_date="2026-10-09", run_dir=run.relative_to(root).as_posix(),
                   manifest_sha256=sha(manifest_path), **identity)
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    original = pointer_path.read_bytes()
    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", registry.PROFILES["V13_V10_G_3"])
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_ROOT", root)
    _, _, body = dashboard._backtesting_v13_v10_g_view({"status":"SUCCESS"}, today_ist="2026-10-12")
    assert "VERIFIED_ARCHIVED_PRODUCTION_DAILY" in body
    assert f"Strategy version: {LEGACY_STRATEGY_VERSION}" in body
    assert f"Current strategy: {STRATEGY_VERSION}" in body
    assert "Net P&L (rupees): 100" in body
    assert pointer_path.read_bytes() == original
    pointer["strategy_fingerprint"] = strategy_fingerprint()
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    _, _, body = dashboard._backtesting_v13_v10_g_view({"status":"SUCCESS"}, today_ist="2026-10-12")
    assert "AWAITING_VERIFIED_G3_REPORT" in body
