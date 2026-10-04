from __future__ import annotations

import re
from dataclasses import fields
from pathlib import Path

from ai_platform.observability.catalog import StandardMetrics, register_standard_metrics
from ai_platform.observability.metrics import Counter, Gauge, Histogram, MetricsRegistry
from ai_platform.observability.runtime_collector import (
    _TEXTFILE_HISTOGRAM_BUCKETS,
    _TEXTFILE_SAMPLE_SCHEMAS,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "configs" / "observability" / "metric-contract.yml"


def _required_metric_names() -> set[str]:
    """Read inline ``name`` values without adding a YAML dependency."""

    text = CONTRACT.read_text(encoding="utf-8")
    return set(re.findall(r"\bname:\s*(trading_[a-z0-9_]+)\b", text))


def _sample_labels(label_names: tuple[str, ...]) -> dict[str, str]:
    values = {
        "service": "unit",
        "strategy": "v13_v10_g",
        "mode": "paper",
        "stage": "feature",
        "source": "equity_1m",
        "timeframe": "1m",
        "slot": "09:55",
        "operation": "orders",
        "outcome": "success",
        "event": "selected",
        "asset": "equity",
        "pipeline": "g",
        "profile": "v13-v10-g",
        "replay_kind": "observed",
        "mismatch_type": "numeric",
        "component": "metrics",
        "reason": "overflow",
        "anomaly_type": "gap",
        "feature": "ema9",
        "volume": "runtime",
        "method": "GET",
        "route": "/api/v1/test",
        "status_code": "200",
        "status_class": "2xx",
    }
    return {name: values[name] for name in label_names}


def test_standard_catalog_registers_and_renders_every_required_metric() -> None:
    registry = MetricsRegistry(strict=True)
    standard = register_standard_metrics(registry)
    catalog_metrics = {
        getattr(standard, field.name).name for field in fields(StandardMetrics)
    }

    assert catalog_metrics == _required_metric_names()

    for field in fields(StandardMetrics):
        metric = getattr(standard, field.name)
        labels = _sample_labels(metric.label_names)
        if isinstance(metric, Counter):
            metric.inc(labels=labels)
        elif isinstance(metric, Gauge):
            metric.set(1, labels=labels)
        elif isinstance(metric, Histogram):
            metric.observe(0.1, labels=labels)
        else:  # pragma: no cover - protects future metric primitive additions
            raise AssertionError(f"Unsupported metric primitive: {type(metric)!r}")

    rendered = registry.render_prometheus()
    for name in _required_metric_names():
        assert f"# HELP {name} " in rendered
        assert f"# TYPE {name} " in rendered


def test_rule_facing_metric_labels_are_bounded_and_present() -> None:
    standard = register_standard_metrics(MetricsRegistry(strict=True))
    expected = {
        "heartbeat_age_seconds": ("service", "mode"),
        "pipeline_schedule_overdue": ("pipeline", "mode"),
        "data_age_seconds": ("source", "timeframe"),
        "data_coverage_ratio": ("source", "timeframe"),
        "data_slot_incomplete": ("source", "timeframe", "slot"),
        "raw_data_anomaly_total": ("source", "anomaly_type"),
        "slot_deadline_lag_seconds": ("pipeline", "mode"),
        "signal_events_total": ("strategy", "mode", "event", "outcome"),
        "order_events_total": ("strategy", "mode", "asset", "event", "outcome"),
        "active_order_reconciliation_mismatch": ("mode", "asset"),
        "strategy_fingerprint_mismatch": ("service", "strategy", "mode"),
        "process_restarts_total": ("service", "strategy", "mode"),
        "clock_offset_seconds": ("service",),
        "feature_parity_mismatch_total": ("strategy", "feature"),
        "telemetry_dropped_total": ("component", "reason"),
        "disk_free_bytes": ("service", "volume"),
        "http_requests_total": ("method", "route", "status_code"),
        "http_request_duration_seconds": ("method", "route", "status_class"),
    }
    prohibited = {
        "symbol",
        "tradingsymbol",
        "signal_id",
        "order_id",
        "broker_order_id",
        "run_id",
        "trace_id",
        "span_id",
    }

    for attribute, labels in expected.items():
        actual = getattr(standard, attribute).label_names
        assert actual == labels
        assert prohibited.isdisjoint(actual)


def test_deadline_lag_remains_histogram_for_existing_rules() -> None:
    standard = register_standard_metrics(MetricsRegistry(strict=True))
    assert isinstance(standard.slot_deadline_lag_seconds, Histogram)


def test_runtime_textfile_allowlist_matches_catalog_sample_schemas() -> None:
    standard = register_standard_metrics(MetricsRegistry(strict=True))
    for field in fields(StandardMetrics):
        metric = getattr(standard, field.name)
        labels = frozenset(metric.label_names)
        if isinstance(metric, Histogram):
            assert _TEXTFILE_SAMPLE_SCHEMAS[f"{metric.name}_bucket"] == labels | {"le"}
            assert _TEXTFILE_SAMPLE_SCHEMAS[f"{metric.name}_count"] == labels
            assert _TEXTFILE_SAMPLE_SCHEMAS[f"{metric.name}_sum"] == labels
            expected_buckets = frozenset(
                {f'"{format(value, ".15g")}"' for value in metric.buckets}
                | {'"+Inf"'}
            )
            assert _TEXTFILE_HISTOGRAM_BUCKETS[f"{metric.name}_bucket"] == expected_buckets
        else:
            assert _TEXTFILE_SAMPLE_SCHEMAS[metric.name] == labels
