from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tools.observability_experiment_registry import (
    ExperimentRegistry,
    RegistryError,
    validate_spec,
)
from ai_platform.observability.catalog import register_standard_metrics
from ai_platform.observability.metrics import MetricsRegistry


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "observability"
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def valid_spec(experiment_id: str = "g-one-factor-001") -> dict:
    return {
        "schema_version": "eqidv2.observability.experiment.v1",
        "experiment_id": experiment_id,
        "created_at": "2026-09-24T10:00:00Z",
        "proposed_by": "researcher-a",
        "mode": "research_only",
        "hypothesis": "A causal one-factor filter improves net expectancy after costs.",
        "baseline": {
            "strategy_id": "v13-v10-g",
            "code_sha256": HASH_A,
            "config_sha256": HASH_B,
            "source_manifest_sha256": HASH_C,
        },
        "challenger": {
            "name": "one-factor-filter",
            "code_sha256": HASH_B,
            "config_sha256": HASH_C,
            "one_factor_change": "Increase exactly one confirmation threshold.",
        },
        "windows": {
            "development": {"start": "2026-01-01", "end": "2026-03-31"},
            "validation": {"start": "2026-04-01", "end": "2026-05-31"},
            "final_test": {"start": "2026-06-01", "end": "2026-06-30"},
        },
        "execution_assumptions": {
            "fees_bps": 5.0,
            "slippage_bps": 5.0,
            "liquidity_model": "observed executable volume",
            "capital_rupees": 1_500_000,
        },
        "trial_budget": {
            "max_variants": 1,
            "max_runs": 3,
            "max_compute_minutes": 240,
        },
        "decision_rules": {
            "primary_metric": "net_profit_rupees_after_costs",
            "minimum_improvement": 1000,
            "maximum_drawdown_degradation_rupees": 0,
            "minimum_forward_shadow_sessions": 2,
        },
        "data_policy": {
            "point_in_time_only": True,
            "include_failed_variants": True,
            "final_test_locked": True,
        },
    }


def valid_result(
    experiment_id: str = "g-one-factor-001",
    *,
    evidence_quality: str = "UNTOUCHED_HOLDOUT",
    sessions: int = 0,
) -> dict:
    return {
        "schema_version": "eqidv2.observability.experiment_result.v1",
        "experiment_id": experiment_id,
        "completed_at": "2026-09-24T11:00:00Z",
        "status": "COMPLETED",
        "evidence_quality": evidence_quality,
        "trials_attempted": 2,
        "checks": {
            "point_in_time": True,
            "costs_included": True,
            "outputs_hash_verified": True,
            "baseline_reproduced": True,
        },
        "metrics": {
            "baseline": {
                "net_profit_rupees_after_costs": 1000,
                "maximum_drawdown_rs": -500,
            },
            "challenger": {
                "net_profit_rupees_after_costs": 2200,
                "maximum_drawdown_rs": -400,
            },
            "difference": {"net_profit_rupees_after_costs": 1200},
        },
        "forward_shadow_sessions": sessions,
    }


def write_json(path: Path, value: dict) -> Path:
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def test_dashboards_are_valid_json_and_have_stable_uids() -> None:
    dashboards = sorted((CONFIG / "grafana" / "dashboards").glob("*.json"))
    assert len(dashboards) >= 2
    values = [json.loads(path.read_text(encoding="utf-8")) for path in dashboards]
    assert len({value["uid"] for value in values}) == len(values)
    assert all(value["panels"] for value in values)


def test_compose_binds_user_interfaces_to_loopback() -> None:
    compose = (CONFIG / "compose.yaml").read_text(encoding="utf-8")
    for port in ("3000", "9090", "9093", "3100", "3200", "12345", "4317", "4318"):
        assert f"127.0.0.1:{port}:" in compose
    assert "GF_SECURITY_ADMIN_PASSWORD__FILE" in compose
    assert "latest" not in compose


def test_metric_contract_prohibits_high_cardinality_identifiers() -> None:
    contract = (CONFIG / "metric-contract.yml").read_text(encoding="utf-8")
    for name in ("signal_id", "order_id", "run_id", "trace_id", "symbol"):
        assert f"- {name}" in contract
    assert "cardinal_identifiers_belong_in: [structured_logs, traces, immutable_evidence]" in contract


def test_log_retention_is_scoped_to_runtime_telemetry_and_dry_run_by_default() -> None:
    script = (
        ROOT / "bat" / "observability" / "Invoke-LogRetention.ps1"
    ).read_text(encoding="utf-8")
    assert "$env:EQIDV2_RUNTIME_ROOT" in script
    assert 'Join-Path $RuntimeRoot "observability\\logs"' in script
    assert "[switch]$Execute" in script
    assert 'if (-not $Execute)' in script
    assert "ReparsePoint" in script
    assert "Get-ChildItem -LiteralPath $current" in script
    assert "Get-ChildItem -LiteralPath $resolvedTarget -Recurse" not in script
    policy = (CONFIG / "retention-policy.yml").read_text(encoding="utf-8")
    assert "${EQIDV2_RUNTIME_ROOT:-C:/TradingData/eqidv2}/observability/logs" in policy


def test_windows_startup_supports_per_user_docker_and_powershell_51() -> None:
    scripts = {
        name: (ROOT / "bat" / "observability" / name).read_text(encoding="utf-8")
        for name in (
            "Start-Observability.ps1",
            "Test-Observability.ps1",
            "Stop-Observability.ps1",
            "Invoke-FailureDrill.ps1",
        )
    }
    for script in scripts.values():
        assert 'Programs\\DockerDesktop\\resources\\bin' in script

    start = scripts["Start-Observability.ps1"]
    assert "RandomNumberGenerator]::Create()" in start
    assert ".GetBytes($randomBytes)" in start
    assert "RandomNumberGenerator]::Fill" not in start

    api_start = (
        ROOT / "bat" / "observability" / "Start-AiPlatformApi.ps1"
    ).read_text(encoding="utf-8")
    assert "ai_platform_api_token.txt" in api_start
    assert "-WindowStyle Hidden" in api_start
    assert 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "http://127.0.0.1:4318/v1/traces"' in api_start

    retention = (
        ROOT / "bat" / "observability" / "Invoke-LogRetention.ps1"
    ).read_text(encoding="utf-8")
    assert "$measurement = $candidates | Measure-Object" in retention
    assert "$measurement.Sum" in retention


def test_dashboard_launcher_keeps_authentication_secrets_out_of_process_argv() -> None:
    launcher = (ROOT / "bat" / "run_log_dashboard_server.bat").read_text(
        encoding="utf-8"
    )
    invocation = next(
        line
        for line in launcher.splitlines()
        if '"%PYTHON_EXE%" -u' in line and "%SCRIPT_NAME%" in line
    )

    assert "--username" not in invocation
    assert "--password" not in invocation
    assert "--api-token" not in invocation
    assert "AUTH_ARGS" not in launcher
    assert 'set "LOG_DASH_TOKEN=%LOG_DASH_PASS%"' in launcher
    assert 'set "LOG_DASH_USER="' in launcher
    assert 'set "LOG_DASH_PASS="' in launcher


def test_alloy_preserves_metric_service_labels_and_alerts_on_absent_data() -> None:
    alloy = (CONFIG / "alloy" / "config.alloy").read_text(encoding="utf-8")
    assert re.search(r'(?m)^\s*scrape_service\s*=\s*"ai-platform-api"\s*,?$', alloy)
    assert not re.search(r'(?m)^\s*service\s*=\s*"ai-platform-api"\s*,?$', alloy)
    assert 'job              = "trading-supervised-worker"' in alloy
    assert '__path_exclude__ = "/var/log/trading/repository/*.log.supervisor.log"' in alloy
    assert 'ignore_older_than = "48h"' in alloy
    assert 'event = "broker_authentication_failure"' in alloy
    assert 'event = "alert_delivery_authentication_failure"' in alloy
    assert "bearer_secret" in alloy
    assert "credential_secret" in alloy
    assert 'values = ["filename"]' in alloy

    alerts = (
        CONFIG / "prometheus" / "rules" / "trading-alerts.yml"
    ).read_text(encoding="utf-8")
    assert "TradingFuturesOiMetricsMissing" in alerts
    assert "TradingFuturesOiCoverageMissing" in alerts
    assert "TradingEquityMetricsMissing" in alerts
    assert "TradingEquityCoverageMissing" in alerts
    assert "TradingEquityFinalSlotIncomplete" in alerts
    assert 'max_over_time(trading_data_slot_incomplete{source="equity",timeframe="5m"}[10m]) > 0' in alerts
    assert "TradingMarketCalendarMetricMissing" in alerts
    assert "TradingBrokerReconciliationMissing" in alerts
    assert "TradingActiveOrderReconciliationMismatch" in alerts
    assert "trading_active_order_reconciliation_mismatch" in alerts
    assert "TradingPipelineScheduleOverdue" in alerts
    assert "trading_pipeline_schedule_overdue" in alerts
    assert "TradingStrategyFingerprintEvidenceMissing" in alerts
    assert "TradingBrokerAuthenticationFailure" in alerts
    assert "TradingAlertDeliveryAuthenticationFailure" in alerts
    assert 'service=~".*(5min|5m).*"} > 420' in alerts
    assert 'service!~".*(5min|5m).*"} > 120' in alerts
    assert 'trading_data_age_seconds{timeframe="5m"} > 420' in alerts
    assert 'trading_data_age_seconds{timeframe!="5m",source!="equity_confirmation"} > 90' in alerts
    assert alerts.count('service=~"fno_v13_v10_g.*"') >= 2
    assert "expr: max(trading_replay_due) == 1" in alerts
    assert 'expr: absent(up{job="ai-platform"})' in alerts
    availability = alerts.split("- name: trading-availability-p1", 1)[1].split(
        "- name: trading-quality-p2", 1
    )[0]
    assert availability.count("trading_maintenance_window") >= 7
    safety = alerts.split("- name: trading-safety-p0", 1)[1].split(
        "- name: trading-availability-p1", 1
    )[0]
    assert "trading_maintenance_window" not in safety

    recording = (
        CONFIG / "prometheus" / "rules" / "recording-rules.yml"
    ).read_text(encoding="utf-8")
    assert recording.count("trading_maintenance_window") >= 3
    assert recording.count("trading_market_open") >= 3
    assert 'service=~".*(5min|5m).*"} <= bool 420' in recording
    assert 'service!~".*(5min|5m).*"} <= bool 120' in recording
    assert 'trading_data_age_seconds{timeframe="5m"} <= bool 420' in recording
    assert 'trading_data_age_seconds{timeframe!="5m",source!="equity_confirmation"} <= bool 90' in recording
    assert "trading_pipeline_schedule_overdue" in recording


def test_rules_and_dashboards_match_registered_metric_labels_and_types() -> None:
    registry = MetricsRegistry(namespace="trading", strict=True)
    register_standard_metrics(registry)
    observed = {
        name: (metric.metric_type, metric.label_names)
        for name, metric in registry._metrics.items()
    }
    assert observed["trading_data_age_seconds"] == ("gauge", ("source", "timeframe"))
    assert observed["trading_slot_deadline_lag_seconds"] == (
        "histogram",
        ("pipeline", "mode"),
    )
    assert observed["trading_live_eod_mismatch_total"] == (
        "counter",
        ("stage", "mismatch_type"),
    )

    prometheus_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (CONFIG / "prometheus" / "rules").glob("*.yml")
    )
    dashboards_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (CONFIG / "grafana" / "dashboards").glob("*.json")
    )
    combined = prometheus_text + dashboards_text
    assert 'trading_data_age_seconds{critical=' not in combined
    assert 'trading_data_age_seconds{mode=' not in combined
    assert 'trading_live_eod_mismatch_total{reason' not in combined
    assert "trading_slot_deadline_lag_seconds_bucket" in prometheus_text
    assert "trading_slot_deadline_lag_seconds)" not in prometheus_text
    assert 'trading_data_age_seconds{timeframe=\\"5m\\"} / 420' in dashboards_text
    assert 'trading_data_age_seconds{timeframe!=\\"5m\\",source!=\\"equity_confirmation\\"} / 90' in dashboards_text
    assert 'service=~\\".*(5min|5m).*\\"} / 420' in dashboards_text
    assert 'service!~\\".*(5min|5m).*\\"} / 120' in dashboards_text
    assert "trading-supervised-worker" in dashboards_text


def test_experiment_windows_must_be_chronological() -> None:
    spec = valid_spec()
    spec["windows"]["validation"]["start"] = "2026-03-01"
    with pytest.raises(RegistryError, match="development must end"):
        validate_spec(spec)


def test_registry_hash_chain_and_shadow_governance(tmp_path: Path) -> None:
    registry = ExperimentRegistry(tmp_path / "registry")
    spec_path = write_json(tmp_path / "spec.json", valid_spec())
    registry.register(spec_path)

    result_path = write_json(tmp_path / "result.json", valid_result())
    registry.record_result("g-one-factor-001", result_path, "runner")
    registry.decide(
        "g-one-factor-001",
        "ELIGIBLE_FOR_SHADOW",
        "researcher-a",
        "Holdout passed the preregistered checks.",
    )
    with pytest.raises(RegistryError, match="reviewer different"):
        registry.decide(
            "g-one-factor-001",
            "APPROVED_FOR_SHADOW",
            "researcher-a",
            "Self approval must fail.",
        )
    registry.decide(
        "g-one-factor-001",
        "APPROVED_FOR_SHADOW",
        "reviewer-b",
        "Independent review approved prospective shadow only.",
    )

    shadow_path = write_json(
        tmp_path / "shadow.json",
        valid_result(evidence_quality="PROSPECTIVE_SHADOW", sessions=2),
    )
    registry.record_result("g-one-factor-001", shadow_path, "shadow-runner")
    registry.decide(
        "g-one-factor-001",
        "CANDIDATE_FOR_MANUAL_LIVE_REVIEW",
        "reviewer-b",
        "Minimum prospective shadow evidence exists; no live edit was made.",
    )

    state = registry.status("g-one-factor-001")
    assert state["state"] == "CANDIDATE_FOR_MANUAL_LIVE_REVIEW"
    assert state["live_configuration_changed"] is False
    verification = registry.verify()
    assert verification["ok"] is True
    assert verification["verified_artifacts"] == 3

    with pytest.raises(RegistryError, match="cannot promote"):
        registry.decide(
            "g-one-factor-001",
            "PROMOTED_TO_LIVE",
            "reviewer-b",
            "This must never be automated.",
        )


def test_registry_detects_journal_and_artifact_tampering(tmp_path: Path) -> None:
    registry = ExperimentRegistry(tmp_path / "registry")
    registry.register(write_json(tmp_path / "spec.json", valid_spec()))
    stored_spec = registry.root / "experiments" / "g-one-factor-001" / "spec.json"
    stored_spec.write_text("{}", encoding="utf-8")
    with pytest.raises(RegistryError, match="artifact hash mismatch"):
        registry.verify()

    stored_spec.write_text(json.dumps(valid_spec()), encoding="utf-8")
    journal_lines = registry.journal.read_text(encoding="utf-8").splitlines()
    event = json.loads(journal_lines[0])
    event["actor"] = "tampered"
    registry.journal.write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(RegistryError, match="journal hash mismatch"):
        registry.events()


def test_result_cannot_exceed_preregistered_trial_budget(tmp_path: Path) -> None:
    registry = ExperimentRegistry(tmp_path / "registry")
    registry.register(write_json(tmp_path / "spec.json", valid_spec()))
    result = valid_result()
    result["trials_attempted"] = 4
    with pytest.raises(RegistryError, match="exceeds"):
        registry.record_result(
            "g-one-factor-001",
            write_json(tmp_path / "result.json", result),
            "runner",
        )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda result: result["metrics"]["difference"].update(
                net_profit_rupees_after_costs=999
            ),
            "does not equal challenger minus baseline",
        ),
        (
            lambda result: result["metrics"]["challenger"].update(
                net_profit_rupees_after_costs=1500
            )
            or result["metrics"]["difference"].update(
                net_profit_rupees_after_costs=500
            ),
            "below the preregistered minimum",
        ),
        (
            lambda result: result["metrics"]["challenger"].update(
                maximum_drawdown_rs=-501
            ),
            "exceeds the preregistered limit",
        ),
    ],
)
def test_positive_decision_enforces_preregistered_performance_rules(
    tmp_path: Path, mutate, message: str
) -> None:
    registry = ExperimentRegistry(tmp_path / "registry")
    registry.register(write_json(tmp_path / "spec.json", valid_spec()))
    result = valid_result()
    mutate(result)
    registry.record_result(
        "g-one-factor-001", write_json(tmp_path / "result.json", result), "runner"
    )
    with pytest.raises(RegistryError, match=message):
        registry.decide(
            "g-one-factor-001",
            "ELIGIBLE_FOR_SHADOW",
            "reviewer-b",
            "Should fail the preregistered rule.",
        )


def test_shadow_eligibility_rejects_reused_development_evidence(
    tmp_path: Path,
) -> None:
    registry = ExperimentRegistry(tmp_path / "registry")
    registry.register(write_json(tmp_path / "spec.json", valid_spec()))
    result = valid_result(evidence_quality="REUSED_DEVELOPMENT")
    registry.record_result(
        "g-one-factor-001", write_json(tmp_path / "result.json", result), "runner"
    )
    with pytest.raises(RegistryError, match="UNTOUCHED_HOLDOUT"):
        registry.decide(
            "g-one-factor-001",
            "ELIGIBLE_FOR_SHADOW",
            "reviewer-b",
            "Reused development evidence is not promotion evidence.",
        )
