"""Canonical low-cardinality metric catalog for live and replay workflows."""

from __future__ import annotations

from dataclasses import dataclass

from .metrics import Counter, Gauge, Histogram, MetricsRegistry


@dataclass(frozen=True, slots=True)
class StandardMetrics:
    market_open: Gauge
    maintenance_window: Gauge
    heartbeat_age_seconds: Gauge
    pipeline_schedule_overdue: Gauge
    data_age_seconds: Gauge
    data_coverage_ratio: Gauge
    data_slot_incomplete: Gauge
    raw_data_anomaly_total: Counter
    slot_deadline_lag_seconds: Histogram
    stage_duration_seconds: Histogram
    signal_events_total: Counter
    broker_requests_total: Counter
    unprotected_position_seconds: Gauge
    position_reconciliation_mismatch: Gauge
    active_order_reconciliation_mismatch: Gauge
    duplicate_order_total: Counter
    strategy_fingerprint_mismatch: Gauge
    process_restarts_total: Counter
    clock_offset_seconds: Gauge
    replay_due: Gauge
    replay_success_timestamp_seconds: Gauge
    live_eod_mismatch_total: Counter
    feature_parity_mismatch_total: Counter
    telemetry_dropped_total: Counter
    order_events_total: Counter
    broker_request_duration_seconds: Histogram
    disk_free_bytes: Gauge
    http_requests_total: Counter
    http_request_duration_seconds: Histogram


def register_standard_metrics(registry: MetricsRegistry) -> StandardMetrics:
    """Register the stable ``trading_*`` operational metric contract.

    The supplied registry should use ``namespace="trading"`` (the default) to
    expose the exact names consumed by the supplied Prometheus/Grafana assets.
    Labels are deliberately bounded and never include symbol/order/signal IDs.
    """

    return StandardMetrics(
        market_open=registry.gauge(
            "market_open",
            "Whether the configured exchange is currently open (1) or closed (0).",
        ),
        maintenance_window=registry.gauge(
            "maintenance_window",
            "Whether a declared telemetry maintenance window is active (1) or inactive (0).",
        ),
        heartbeat_age_seconds=registry.gauge(
            "heartbeat_age_seconds",
            "Age of the latest worker heartbeat in seconds.",
            label_names=("service", "mode"),
        ),
        pipeline_schedule_overdue=registry.gauge(
            "pipeline_schedule_overdue",
            "Whether the latest due frozen pipeline slot is incomplete after grace.",
            label_names=("pipeline", "mode"),
        ),
        data_age_seconds=registry.gauge(
            "data_age_seconds",
            "Age of the newest source observation at decision time in seconds.",
            label_names=("source", "timeframe"),
        ),
        data_coverage_ratio=registry.gauge(
            "data_coverage_ratio",
            "Ratio of expected source observations present in the evaluated window.",
            label_names=("source", "timeframe"),
        ),
        data_slot_incomplete=registry.gauge(
            "data_slot_incomplete",
            "Whether the latest finalized, completed-candle data slot is explicitly incomplete (1) or complete (0).",
            label_names=("source", "timeframe", "slot"),
        ),
        raw_data_anomaly_total=registry.counter(
            "raw_data_anomaly_total",
            "Raw market-data anomalies detected by source and bounded anomaly class.",
            label_names=("source", "anomaly_type"),
        ),
        slot_deadline_lag_seconds=registry.histogram(
            "slot_deadline_lag_seconds",
            "Seconds a slot completed after its decision deadline; negative values are early.",
            label_names=("pipeline", "mode"),
            buckets=(-120, -60, -30, -10, -5, -2, -1, -0.5, -0.1, 0, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120),
        ),
        stage_duration_seconds=registry.histogram(
            "stage_duration_seconds",
            "End-to-end duration of a bounded strategy or execution stage in seconds.",
            label_names=("strategy", "mode", "stage"),
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
        ),
        signal_events_total=registry.counter(
            "signal_events_total",
            "Signal-funnel events by bounded strategy event and outcome.",
            label_names=("strategy", "mode", "event", "outcome"),
        ),
        broker_requests_total=registry.counter(
            "broker_requests_total",
            "Broker API requests by bounded operation and outcome.",
            label_names=("operation", "outcome"),
        ),
        unprotected_position_seconds=registry.gauge(
            "unprotected_position_seconds",
            "Age in seconds of the oldest position without confirmed protection.",
            label_names=("mode", "asset"),
        ),
        position_reconciliation_mismatch=registry.gauge(
            "position_reconciliation_mismatch",
            "Number of current broker/local position mismatches.",
            label_names=("mode", "asset"),
        ),
        active_order_reconciliation_mismatch=registry.gauge(
            "active_order_reconciliation_mismatch",
            "Number of current broker/local active-order mismatches.",
            label_names=("mode", "asset"),
        ),
        duplicate_order_total=registry.counter(
            "duplicate_order_total",
            "Duplicate order attempts prevented or detected.",
            label_names=("mode", "asset"),
        ),
        strategy_fingerprint_mismatch=registry.gauge(
            "strategy_fingerprint_mismatch",
            "Whether the loaded strategy fingerprint differs from the approved value.",
            label_names=("service", "strategy", "mode"),
        ),
        process_restarts_total=registry.counter(
            "process_restarts_total",
            "Process restarts by bounded service, strategy, and execution mode.",
            label_names=("service", "strategy", "mode"),
        ),
        clock_offset_seconds=registry.gauge(
            "clock_offset_seconds",
            "Signed host clock offset from the configured time authority in seconds.",
            label_names=("service",),
        ),
        replay_due=registry.gauge(
            "replay_due",
            "Whether a replay is currently required (1) or not due (0).",
            label_names=("profile", "replay_kind"),
        ),
        replay_success_timestamp_seconds=registry.gauge(
            "replay_success_timestamp_seconds",
            "Unix timestamp of the latest successful replay.",
            label_names=("profile", "replay_kind"),
        ),
        live_eod_mismatch_total=registry.counter(
            "live_eod_mismatch_total",
            "Live versus end-of-day mismatches by first divergence stage.",
            label_names=("stage", "mismatch_type"),
        ),
        feature_parity_mismatch_total=registry.counter(
            "feature_parity_mismatch_total",
            "Feature values outside their declared live-versus-replay tolerance.",
            label_names=("strategy", "feature"),
        ),
        telemetry_dropped_total=registry.counter(
            "telemetry_dropped_total",
            "Telemetry events or updates dropped before export.",
            label_names=("component", "reason"),
        ),
        order_events_total=registry.counter(
            "order_events_total",
            "Order lifecycle events by bounded state and outcome.",
            label_names=("strategy", "mode", "asset", "event", "outcome"),
        ),
        broker_request_duration_seconds=registry.histogram(
            "broker_request_duration_seconds",
            "Broker API request duration in seconds.",
            label_names=("operation", "outcome"),
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
        ),
        disk_free_bytes=registry.gauge(
            "disk_free_bytes",
            "Free bytes available on an observability or trading-data volume.",
            label_names=("service", "volume"),
        ),
        # These schemas and help strings intentionally match the API's direct
        # registration so both call sites receive the same registry objects.
        http_requests_total=registry.counter(
            "http_requests_total",
            "HTTP requests completed by the read-only API.",
            label_names=("method", "route", "status_code"),
        ),
        http_request_duration_seconds=registry.histogram(
            "http_request_duration_seconds",
            "HTTP request duration in seconds.",
            label_names=("method", "route", "status_class"),
        ),
    )


__all__ = ["StandardMetrics", "register_standard_metrics"]
