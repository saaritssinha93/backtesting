from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ai_platform.adapters.common import ArtifactError, file_sha256
from ai_platform.api import ApiSettings, create_app
from ai_platform.api.registry import SourceRegistry
from ai_platform.observability.journal import AppendOnlyEventJournal
from ai_platform.observability.reconciliation import canonical_sha256


FIXTURES = Path(__file__).parent / "fixtures" / "ai_platform"
WORKSPACE = Path(__file__).resolve().parents[1]
TOKEN = "fixture-token-with-at-least-32-characters"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
DAY = "2026-09-17"
SIGNAL_ID = "20260917_0951_SHORT_DEMO_fixture"


def _copy(name: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(FIXTURES / name, destination)


def _json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture()
def api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AI_PLATFORM_OBS_LOG_PATH", raising=False)
    monkeypatch.delenv("AI_PLATFORM_OBS_JOURNAL_PATH", raising=False)
    runtime = tmp_path / "runtime"
    registries = tmp_path / "registries"
    registries.mkdir()

    source_registry = json.loads(
        (WORKSPACE / "docs/ai_platform/source_registry.json").read_text(encoding="utf-8")
    )
    source_registry_path = registries / "sources.json"
    _json(source_registry_path, source_registry)

    frozen = runtime / source_registry["sources"]["frozen_strategy_config"]["relative_path"]
    _json(frozen, {"morning_slots": False, "two_bar_continuation": False})

    profile_registry = json.loads(
        (WORKSPACE / "docs/ai_platform/profile_registry.json").read_text(encoding="utf-8")
    )
    for profile in profile_registry["profiles"].values():
        if "strategy_fingerprint" in profile:
            profile["strategy_fingerprint"] = "fixture-fingerprint"
    profile_registry["profiles"]["V13_V10_G"]["frozen_config_sha256"] = file_sha256(frozen)
    profile_registry_path = registries / "profiles.json"
    _json(profile_registry_path, profile_registry)

    sources = source_registry["sources"]
    _copy(
        "daily_success.json",
        runtime / sources["daily_replay_latest_json"]["relative_path"],
    )
    _json(
        runtime / sources["strategy_manifest"]["relative_path"],
        {"strategy_version": "FNO_V13_V10_G_RETAINED_20260914", "api_key": "must-redact"},
    )
    status_common = {
        "session_date": DAY,
        "strategy_version": "FNO_V13_V10_G_RETAINED_20260914",
        "strategy_fingerprint": "fixture-fingerprint",
        "state": "DONE",
    }
    _json(
        runtime / sources["equity_live_status"]["relative_path"],
        {
            **status_common,
            "schema_version": "fno_v6_live_kite_qty1_status_v1",
            "execution_mode": "LIVE",
            "updated_at_ist": "2026-09-17T15:32:00+05:30",
        },
    )
    _json(
        runtime / sources["equity_live_heartbeat"]["relative_path"],
        {
            **status_common,
            "schema_version": "fno_v6_live_kite_qty1_heartbeat_v1",
            "heartbeat_ist": "2026-09-17T15:32:00+05:30",
        },
    )
    _copy(
        "equity_paper_closed.json",
        runtime / sources["equity_paper_orders"]["relative_path"] / DAY / "paper.json",
    )
    _copy(
        "equity_live_cancelled.json",
        runtime / sources["equity_live_orders"]["relative_path"] / DAY / "live.json",
    )
    _copy(
        "option_open.json",
        runtime / sources["option_paper_orders"]["relative_path"] / DAY / "open.json",
    )
    _copy(
        "evidence_observed.json",
        runtime
        / sources["immutable_evidence"]["relative_path"]
        / DAY
        / "slot_1120"
        / "confirmation_snapshot"
        / "observed.json",
    )

    settings = ApiSettings(
        runtime_root=runtime,
        source_registry_path=source_registry_path,
        profile_registry_path=profile_registry_path,
        api_token=TOKEN,
        cache_ttl_seconds=0,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        yield client, app, runtime, source_registry_path


def test_liveness_is_public_but_data_routes_require_bearer(api) -> None:
    client, _, _, _ = api
    live = client.get("/api/v1/health/live")
    assert live.status_code == 200
    assert live.headers["x-request-id"]
    assert live.headers["x-trace-id"]
    assert live.json()["meta"]["trace_id"] == live.headers["x-trace-id"]
    denied = client.get("/api/v1/health/ready")
    assert denied.status_code == 401
    assert denied.json()["error"]["code"] == "AUTH_REQUIRED"
    assert denied.headers["www-authenticate"] == "Bearer"
    assert client.get("/api/v1/health/ready", headers=AUTH).status_code == 200
    assert client.get("/docs", headers=AUTH).status_code == 404
    assert client.get(
        "/api/v1/health/live", headers={"Host": "untrusted.example"}
    ).status_code == 400


def test_authenticated_prometheus_metrics_are_exposed(api) -> None:
    client, app, _, _ = api
    client.get("/api/v1/health/live")
    denied = client.get("/api/v1/observability/metrics")
    assert denied.status_code == 401
    response = client.get("/api/v1/observability/metrics", headers=AUTH)
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "trading_http_requests_total" in response.text
    assert "trading_http_request_duration_seconds" in response.text
    assert any(
        line in {"trading_market_open 0", "trading_market_open 1"}
        for line in response.text.splitlines()
    )
    assert app.state.observability.service == "ai_platform_api"
    assert (
        app.state.runtime_metrics_collector.runtime_root.resolve()
        == app.state.settings.runtime_root.resolve()
    )
    docker_host = client.get(
        "/api/v1/observability/metrics",
        headers={**AUTH, "Host": "host.docker.internal"},
    )
    assert docker_host.status_code == 200


def test_observability_status_and_reconciliation_reports_are_read_only(api) -> None:
    client, app, _, _ = api
    assert client.get("/api/v1/observability/status").status_code == 401
    assert client.get(
        f"/api/v1/observability/reconciliation/{DAY}"
    ).status_code == 401

    status = client.get("/api/v1/observability/status", headers=AUTH)
    assert status.status_code == 200
    details = status.json()["data"]
    root = Path(app.state.settings.observability_root).resolve()
    assert details["root_directory"] == str(root)
    assert details["metrics"]["endpoint"] == "/api/v1/observability/metrics"
    assert details["logs"]["file"] == str(root / "logs" / "ai_platform_api.jsonl")
    assert details["journal"]["file"] == str(
        root / "journals" / "ai_platform_api_events.jsonl"
    )
    journal_path = root / "journals" / "ai_platform_api_events.jsonl"
    assert details["journal"]["exists"] is True
    assert details["journal"]["size_bytes"] > 0
    assert details["journal"]["dropped_events"] == 0
    assert details["journal"]["last_error"] is None
    verification = AppendOnlyEventJournal(
        journal_path, service="ai_platform_api", strict=True
    ).verify()
    assert verification.valid is True
    assert verification.entries >= 1
    first_event = json.loads(journal_path.read_text(encoding="utf-8").splitlines()[0])
    assert first_event["event_type"] == "api.runtime.configured"
    assert first_event["data"]["read_only"] is True
    assert first_event["data"]["execution_authority"] is False
    assert isinstance(details["traces"]["otlp_enabled"], bool)
    assert isinstance(details["traces"]["dropped_spans"], int)

    missing = client.get(
        f"/api/v1/observability/reconciliation/{DAY}", headers=AUTH
    )
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "RECONCILIATION_NOT_FOUND"

    report_path = root / "reconciliation" / f"{DAY}.json"
    report_payload = {
        "session_date": DAY,
        "state": "MISMATCH",
        "first_divergence_stage": "feature",
        "api_key": "must-redact",
    }
    report_payload["report_sha256"] = canonical_sha256(report_payload)
    _json(report_path, report_payload)
    report = client.get(
        f"/api/v1/observability/reconciliation/{DAY}", headers=AUTH
    )
    assert report.status_code == 200
    assert report.json()["data"]["first_divergence_stage"] == "feature"
    assert report.json()["data"]["api_key"] == "[REDACTED]"

    report_payload["first_divergence_stage"] = "selection"
    _json(report_path, report_payload)
    tampered = client.get(
        f"/api/v1/observability/reconciliation/{DAY}", headers=AUTH
    )
    assert tampered.status_code == 503
    assert tampered.json()["error"]["code"] == "RECONCILIATION_DIGEST_MISMATCH"

    invalid = client.get(
        "/api/v1/observability/reconciliation/not-a-date", headers=AUTH
    )
    assert invalid.status_code == 422
    traversal = client.get(
        "/api/v1/observability/reconciliation/..%2Fsecret", headers=AUTH
    )
    assert traversal.status_code in {404, 422}


def test_observability_file_environment_overrides_take_precedence(
    api, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _, app, _, _ = api
    override_log = (tmp_path / "override" / "api.jsonl").resolve()
    override_journal = (tmp_path / "override" / "events.jsonl").resolve()
    monkeypatch.setenv("AI_PLATFORM_OBS_LOG_PATH", str(override_log))
    monkeypatch.setenv("AI_PLATFORM_OBS_JOURNAL_PATH", str(override_journal))
    overridden = create_app(app.state.settings)
    try:
        assert overridden.state.observability_paths["log"] == override_log
        assert overridden.state.observability_paths["journal"] == override_journal
    finally:
        for handler in list(overridden.state.observability.logger.logger.handlers):
            handler.close()


def test_api_journal_attestation_failure_is_fail_open(
    api, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _, app, _, _ = api
    invalid_journal = tmp_path / "journal-is-a-directory"
    invalid_journal.mkdir()
    monkeypatch.setenv("AI_PLATFORM_OBS_JOURNAL_PATH", str(invalid_journal))

    created = create_app(app.state.settings)
    try:
        journal = created.state.observability.event_journal
        assert journal is not None
        assert journal.dropped_events == 1
        assert journal.last_error in {"IsADirectoryError", "PermissionError"}
        assert created.state.observability_paths["journal"] == invalid_journal.resolve()
    finally:
        for handler in list(created.state.observability.logger.logger.handlers):
            handler.close()


def test_strategy_and_readiness_are_validated(api) -> None:
    client, app, _, _ = api
    strategy = client.get("/api/v1/strategies/v13-v10-g", headers=AUTH)
    assert strategy.status_code == 200
    assert strategy.json()["data"]["identity"]["strategy_fingerprint"] == "fixture-fingerprint"
    readiness = client.get("/api/v1/readiness", headers=AUTH)
    assert readiness.status_code == 200
    assert readiness.json()["data"]["coherent_snapshot_id"]
    assert "\\runtime\\" not in readiness.text.lower()
    assert not app.router.on_startup


def test_signals_trades_trace_and_summary_preserve_mode_and_pnl(api) -> None:
    client, _, _, _ = api
    signals = client.get(
        "/api/v1/signals",
        params={"date": DAY, "preferred_only": "true"},
        headers=AUTH,
    ).json()["data"]
    assert signals["total"] == 1
    assert signals["items"][0]["mode"] == "PAPER"

    trades = client.get(
        "/api/v1/trades",
        params={"date": DAY, "asset": "ALL"},
        headers=AUTH,
    ).json()["data"]
    assert trades["total"] == 3
    open_option = next(row for row in trades["items"] if row["asset"] == "OPTIONS")
    assert open_option["realized_net_pnl_rs"] is None
    assert open_option["open_mark_net_pnl_rs"] == "-12.5"

    trace = client.get(
        f"/api/v1/signals/{SIGNAL_ID}/trace",
        params={"date": DAY},
        headers=AUTH,
    )
    assert trace.status_code == 200
    assert len(trace.json()["data"]["equity_states"]) == 2

    summary = client.get(
        "/api/v1/results/summary", params={"date": DAY}, headers=AUTH
    )
    assert summary.status_code == 200
    assert summary.json()["data"]["replay"]["complete"] is True
    assert summary.json()["data"]["options_by_run_kind"]["PAPER_QUOTE_MONITOR"][
        "realized_net_pnl_rs"
    ] == "0.00"


def test_registered_evidence_only_and_redaction(api) -> None:
    client, _, _, _ = api
    evidence = client.get(
        "/api/v1/evidence/immutable_evidence",
        params={"date": DAY, "slot": "1120", "artifact_kind": "confirmation_snapshot"},
        headers=AUTH,
    )
    assert evidence.status_code == 200
    assert evidence.json()["data"]["available_revisions"] == 1

    manifest = client.get("/api/v1/evidence/strategy_manifest", headers=AUTH)
    assert manifest.status_code == 200
    assert manifest.json()["data"]["payload"]["api_key"] == "[REDACTED]"

    unknown = client.get("/api/v1/evidence/not_registered", headers=AUTH)
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "SOURCE_NOT_REGISTERED"
    assert client.get("/api/v1/evidence/..%2Fsecret", headers=AUTH).status_code in {404, 422}


def test_invalid_inputs_and_errors_are_bounded_and_redacted(api) -> None:
    client, _, runtime, _ = api
    invalid = client.get(
        "/api/v1/signals", params={"date": DAY, "symbol": "../../secret"}, headers=AUTH
    )
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "INVALID_REQUEST"

    daily = runtime / "backtesting_result_v13_v10_g/latest/latest_backtesting_result_v13_v10_g.json"
    daily.write_text("{broken", encoding="utf-8")
    failed = client.get("/api/v1/results/summary", headers=AUTH)
    assert failed.status_code == 503
    assert failed.json()["error"]["code"] == "SOURCE_INVALID"
    assert str(runtime) not in failed.text


def test_deterministic_assistant_session_lifecycle(api) -> None:
    client, _, _, _ = api
    created = client.post(
        "/api/v1/assistant/sessions",
        json={"session_date": DAY, "asset": "OPTIONS", "mode": "PAPER"},
        headers=AUTH,
    )
    assert created.status_code == 201
    record = created.json()["data"]
    assert record["state"] == "DETERMINISTIC_READY"
    assert record["ai_enabled"] is False
    fetched = client.get(
        f"/api/v1/assistant/sessions/{record['session_id']}", headers=AUTH
    )
    assert fetched.status_code == 200


def test_registry_rejects_paths_outside_runtime_root(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.json"
    _json(
        registry_path,
        {"sources": {"escape": {"relative_path": "../outside.json", "access": "read_only"}}},
    )
    registry = SourceRegistry(registry_path, tmp_path / "runtime")
    with pytest.raises(ArtifactError, match="leaves runtime root"):
        registry.path("escape")
