"""Fail-open observability primitives for live trading and backtesting.

Nothing in this package starts a worker, network listener or exporter on
import.  Callers opt into sinks explicitly and can continue operating when a
telemetry sink is unavailable.
"""

from .catalog import StandardMetrics, register_standard_metrics
from .context import (
    CorrelationContext,
    bind_context,
    context_from_env,
    context_to_env,
    current_context,
    new_run_id,
)
from .journal import AppendOnlyEventJournal, JournalVerification
from .metrics import (
    DEFAULT_BUCKETS,
    PROMETHEUS_CONTENT_TYPE,
    Counter,
    Gauge,
    Histogram,
    MetricsRegistry,
)
from .redaction import REDACTED, is_sensitive_key, redact, redact_text, safe_json_dumps
from .runtime import Observability, create_observability, create_observability_from_env
from .runtime_collector import RuntimeMetricsCollector, merge_prometheus_samples
from .structured_logging import EventLogger, JsonFormatter, configure_json_logger
from .tracing import (
    Span,
    SpanSnapshot,
    Tracer,
    current_trace_ids,
    current_traceparent,
    inject_trace_headers,
    parse_traceparent,
)

__all__ = [
    "AppendOnlyEventJournal",
    "CorrelationContext",
    "Counter",
    "DEFAULT_BUCKETS",
    "EventLogger",
    "Gauge",
    "Histogram",
    "JournalVerification",
    "JsonFormatter",
    "MetricsRegistry",
    "Observability",
    "PROMETHEUS_CONTENT_TYPE",
    "REDACTED",
    "RuntimeMetricsCollector",
    "Span",
    "SpanSnapshot",
    "StandardMetrics",
    "Tracer",
    "bind_context",
    "configure_json_logger",
    "context_from_env",
    "context_to_env",
    "create_observability",
    "create_observability_from_env",
    "current_context",
    "current_trace_ids",
    "current_traceparent",
    "inject_trace_headers",
    "is_sensitive_key",
    "merge_prometheus_samples",
    "new_run_id",
    "parse_traceparent",
    "redact",
    "redact_text",
    "register_standard_metrics",
    "safe_json_dumps",
]
