from __future__ import annotations

import io
import importlib.util
import json
from pathlib import Path

import pytest

from ai_platform.observability import (
    AppendOnlyEventJournal,
    MetricsRegistry,
    Tracer,
    bind_context,
    configure_json_logger,
    context_from_env,
    context_to_env,
    create_observability,
    current_context,
    current_traceparent,
    parse_traceparent,
    register_standard_metrics,
)


def test_context_is_nested_resettable_and_process_propagatable() -> None:
    assert current_context().run_id is None
    with bind_context(run_id="run-1", session_date="2026-09-24"):
        assert current_context().run_id == "run-1"
        with bind_context(signal_id="sig-1", run_id=None):
            assert current_context().run_id is None
            assert current_context().signal_id == "sig-1"
        assert current_context().run_id == "run-1"
        environment = context_to_env()
    restored = context_from_env(environment)
    assert restored.run_id == "run-1"
    assert restored.session_date == "2026-09-24"
    assert current_context().run_id is None


def test_json_logger_redacts_structured_and_embedded_secrets() -> None:
    output = io.StringIO()
    logger = configure_json_logger(
        "test.observability.redaction", service="unit", stream=output
    )
    with bind_context(run_id="run-redact"):
        assert logger.info(
            "broker.request",
            api_key="key-must-not-appear",
            nested={"access_token": "token-must-not-appear"},
            url="https://broker.invalid/orders?token=query-must-not-appear",
            broker={"enctoken": "broker-token-must-not-appear"},
            error="request failed: enctoken=embedded-token-must-not-appear",
            harmless_token_count=3,
        )
    payload = json.loads(output.getvalue())
    assert payload["context"]["run_id"] == "run-redact"
    assert payload["fields"]["api_key"] == "[REDACTED]"
    assert payload["fields"]["nested"]["access_token"] == "[REDACTED]"
    assert payload["fields"]["broker"]["enctoken"] == "[REDACTED]"
    assert "embedded-token-must-not-appear" not in output.getvalue()
    assert "query-must-not-appear" not in output.getvalue()
    assert payload["fields"]["harmless_token_count"] == 3


def test_metrics_are_bounded_and_render_canonical_trading_names() -> None:
    registry = MetricsRegistry(max_series_per_metric=1)
    standard = register_standard_metrics(registry)
    assert standard.heartbeat_age_seconds.set(5, service="scanner", mode="LIVE")
    assert not standard.heartbeat_age_seconds.set(6, service="executor", mode="LIVE")
    standard.slot_deadline_lag_seconds.observe(
        -0.25, pipeline="g", mode="PAPER"
    )
    rendered = registry.render_prometheus()
    assert "trading_heartbeat_age_seconds" in rendered
    assert 'service="scanner"' in rendered
    assert "trading_slot_deadline_lag_seconds_bucket" in rendered
    assert 'trading_telemetry_dropped_total{component="metrics",reason="OverflowError"} 1' in rendered
    with pytest.raises(ValueError, match="High-cardinality"):
        registry.counter("bad_total", "bad", label_names=("signal_id",))


def test_tracer_creates_nested_ids_and_accepts_w3c_parent() -> None:
    completed = []
    tracer = Tracer("unit", on_end=completed.append, enable_opentelemetry=False)
    with tracer.start_span("parent") as parent:
        assert current_context().trace_id == parent.trace_id
        traceparent = current_traceparent()
        assert traceparent is not None
        assert parse_traceparent(traceparent) == (parent.trace_id, parent.span_id)
        with tracer.start_span("child") as child:
            assert child.trace_id == parent.trace_id
            assert child.parent_span_id == parent.span_id
    assert current_context().trace_id is None
    assert [span.name for span in completed] == ["child", "parent"]

    incoming_trace = "1" * 32
    incoming_parent = "2" * 16
    with tracer.start_span(
        "server",
        carrier={"traceparent": f"00-{incoming_trace}-{incoming_parent}-01"},
        kind="server",
    ) as server:
        assert server.trace_id == incoming_trace
        assert server.parent_span_id == incoming_parent


def test_hash_chained_journal_redacts_and_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    journal = AppendOnlyEventJournal(path, service="unit", strict=True)
    with bind_context(run_id="run-journal"):
        first = journal.append("order.submitted", {"api_key": "secret", "quantity": 1})
        second = journal.append("order.acknowledged", {"broker_order_id": "abc"})
    assert first is not None and second is not None
    assert second["previous_hash"] == first["event_hash"]
    assert journal.verify().valid
    assert "secret" not in path.read_text(encoding="utf-8")

    records = path.read_text(encoding="utf-8").splitlines()
    records[0] = records[0].replace("order.submitted", "order.changed")
    path.write_text("\n".join(records) + "\n", encoding="utf-8")
    verification = journal.verify()
    assert not verification.valid
    assert verification.error_line == 1


def test_observability_sinks_fail_open(tmp_path: Path) -> None:
    # A directory cannot be opened as a journal file.  The application event
    # still returns normally and exposes the loss as a metric.
    bad_path = tmp_path / "directory"
    bad_path.mkdir()
    runtime = create_observability("unit", journal_path=bad_path)
    assert not runtime.event("test.event", durable=True, password="secret")
    rendered = runtime.metrics.render_prometheus()
    assert 'component="journal"' in rendered


def test_otlp_http_export_is_explicit_and_fail_open(monkeypatch) -> None:
    for name in (
        "AI_PLATFORM_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_SDK_DISABLED",
    ):
        monkeypatch.delenv(name, raising=False)
    local = Tracer("local-only")
    assert not local.otlp_enabled
    assert local.force_flush()

    try:
        exporter_available = (
            importlib.util.find_spec("opentelemetry.exporter.otlp.proto.http")
            is not None
        )
    except ModuleNotFoundError:
        exporter_available = False
    if not exporter_available:
        pytest.skip("OTLP HTTP exporter is optional in this Python environment")
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://127.0.0.1:4318/v1/traces",
    )
    exported = Tracer("unit-export")
    try:
        assert exported.otlp_enabled
        assert exported.otel_endpoint == "http://127.0.0.1:4318/v1/traces"
    finally:
        exported.shutdown()
