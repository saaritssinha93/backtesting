"""Composable facade used by API, live workers and offline replays."""

from __future__ import annotations

import atexit
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Mapping

from .catalog import StandardMetrics, register_standard_metrics
from .context import CorrelationContext, bind_context
from .journal import AppendOnlyEventJournal
from .metrics import Counter, Histogram, MetricsRegistry
from .structured_logging import EventLogger, configure_json_logger
from .tracing import Span, SpanSnapshot, Tracer


class _AsyncSpanSink:
    """Bounded, non-blocking local span persistence for latency-sensitive code."""

    def __init__(
        self,
        callback,
        *,
        on_drop,
        capacity: int,
    ) -> None:
        self._callback = callback
        self._on_drop = on_drop
        self._queue: queue.Queue[SpanSnapshot | None] = queue.Queue(
            maxsize=max(1, int(capacity))
        )
        self._condition = threading.Condition()
        self._pending = 0
        self._closed = False
        self._stop_when_drained = False
        self._thread = threading.Thread(
            target=self._run,
            name="eqidv2-observability-span-sink",
            daemon=True,
        )
        self._thread.start()

    def submit(self, snapshot: SpanSnapshot) -> None:
        try:
            with self._condition:
                if self._closed:
                    raise RuntimeError("span sink is closed")
                self._queue.put_nowait(snapshot)
                self._pending += 1
        except Exception as exc:
            try:
                self._on_drop(type(exc).__name__)
            except Exception:
                pass

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            stop_when_drained = False
            try:
                if item is None:
                    return
                self._callback(item)
            except Exception as exc:
                try:
                    self._on_drop(type(exc).__name__)
                except Exception:
                    pass
            finally:
                if item is not None:
                    with self._condition:
                        self._pending = max(0, self._pending - 1)
                        stop_when_drained = (
                            self._closed
                            and self._stop_when_drained
                            and self._pending == 0
                        )
                        self._condition.notify_all()
                self._queue.task_done()
            if stop_when_drained:
                return

    def flush(self, timeout_seconds: float = 1.0) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        with self._condition:
            while self._pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            return True

    def shutdown(self, timeout_seconds: float = 1.0) -> None:
        self.flush(timeout_seconds)
        with self._condition:
            if self._closed:
                return
            self._closed = True
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                # A timed-out flush can leave the bounded queue full.  The
                # worker must then exit itself after preserving every queued
                # span; otherwise it drains the queue and blocks forever with
                # no sentinel available.
                self._stop_when_drained = True
        self._thread.join(timeout=max(0.0, float(timeout_seconds)))


@dataclass(slots=True)
class Observability:
    service: str
    logger: EventLogger
    metrics: MetricsRegistry
    tracer: Tracer
    standard_metrics: StandardMetrics
    event_journal: AppendOnlyEventJournal | None
    event_count: Counter
    operation_duration: Histogram
    _async_span_sink: _AsyncSpanSink | None = field(default=None, repr=False)

    def bind(
        self,
        context: CorrelationContext | Mapping[str, object] | None = None,
        **fields: object,
    ):
        return bind_context(context, service=self.service, **fields)

    def span(
        self,
        name: str,
        *,
        attributes: Mapping[str, object] | None = None,
        carrier: Mapping[str, str] | None = None,
        kind: str = "internal",
    ) -> Span:
        return self.tracer.start_span(
            name, attributes=attributes, carrier=carrier, kind=kind
        )

    def event(
        self,
        event_type: str,
        *,
        severity: str = "INFO",
        message: str | None = None,
        durable: bool = False,
        **data: object,
    ) -> bool:
        """Emit an event without ever raising into the business operation."""

        try:
            logged = self.logger.emit(
                severity, event_type, message=message, **data
            )
            journaled = not durable
            if durable:
                if self.event_journal is None:
                    self.standard_metrics.telemetry_dropped_total.inc(
                        component="journal", reason="not_configured"
                    )
                else:
                    journaled = self.event_journal.append(
                        event_type, data, severity=severity
                    ) is not None
            outcome = "accepted" if logged and journaled else "dropped"
            self.event_count.inc(severity=severity.upper(), outcome=outcome)
            if outcome == "dropped":
                self.standard_metrics.telemetry_dropped_total.inc(
                    component="event", reason="sink_failure"
                )
            return outcome == "accepted"
        except Exception:
            # A second guard protects callers even if a custom sink violates
            # the interfaces above.
            try:
                self.standard_metrics.telemetry_dropped_total.inc(
                    component="event", reason="unexpected_error"
                )
            except Exception:
                pass
            return False

    def flush_spans(self, timeout_seconds: float = 1.0) -> bool:
        """Flush local queued spans; intended for shutdown/tests, not order paths."""

        sink_ok = (
            self._async_span_sink.flush(timeout_seconds)
            if self._async_span_sink is not None
            else True
        )
        return sink_ok

    def shutdown(self, timeout_seconds: float = 1.0) -> None:
        if self._async_span_sink is not None:
            self._async_span_sink.shutdown(timeout_seconds)
        self.tracer.shutdown()


def create_observability(
    service: str,
    *,
    namespace: str = "trading",
    logger: EventLogger | None = None,
    metrics: MetricsRegistry | None = None,
    tracer: Tracer | None = None,
    log_path: Path | str | None = None,
    journal_path: Path | str | None = None,
    stream: IO[str] | None = None,
    enable_opentelemetry: bool = True,
    journal_fsync: bool = False,
    async_span_logging: bool = False,
    span_queue_size: int = 2_048,
) -> Observability:
    """Build an isolated, fail-open telemetry runtime for one service."""

    registry = metrics or MetricsRegistry(namespace=namespace)
    standard = register_standard_metrics(registry)
    event_count = registry.counter(
        "observability_events_total",
        "Structured observability events submitted to local sinks.",
        label_names=("severity", "outcome"),
    )
    operation_duration = registry.histogram(
        "operation_duration_seconds",
        "Instrumented operation duration in seconds.",
        label_names=("operation", "outcome"),
    )
    if logger is not None:
        event_logger = logger
    else:
        logger_name = f"ai_platform.observability.{service}.{uuid.uuid4().hex}"
        try:
            event_logger = configure_json_logger(
                logger_name,
                service=service,
                stream=stream,
                path=log_path,
            )
        except Exception as exc:
            # Invalid or temporarily unavailable telemetry storage must not
            # prevent a live/replay service from starting.
            event_logger = configure_json_logger(logger_name, service=service)
            standard.telemetry_dropped_total.inc(
                component="logging", reason=type(exc).__name__
            )

    journal = None
    if journal_path is not None:
        journal = AppendOnlyEventJournal(
            journal_path,
            service=service,
            fsync=journal_fsync,
            on_drop=lambda reason: standard.telemetry_dropped_total.inc(
                component="journal", reason=reason
            ),
        )

    def persist_span(snapshot: SpanSnapshot) -> None:
        operation_duration.observe(
            snapshot.duration_seconds,
            operation=snapshot.name,
            outcome=snapshot.outcome,
        )
        fields = {
            "span_name": snapshot.name,
            "trace_id": snapshot.trace_id,
            "span_id": snapshot.span_id,
            "parent_span_id": snapshot.parent_span_id,
            "duration_seconds": snapshot.duration_seconds,
            "outcome": snapshot.outcome,
            "attributes": snapshot.attributes,
            "events": snapshot.events,
        }
        if not event_logger.info("trace.span.completed", **fields):
            standard.telemetry_dropped_total.inc(
                component="tracing", reason="log_sink_failure"
            )
        if journal is not None and snapshot.outcome == "error":
            journal.append("trace.span.failed", fields, severity="ERROR")

    async_span_sink = (
        _AsyncSpanSink(
            persist_span,
            capacity=span_queue_size,
            on_drop=lambda reason: standard.telemetry_dropped_total.inc(
                component="tracing", reason=reason
            ),
        )
        if async_span_logging
        else None
    )
    on_span_end = async_span_sink.submit if async_span_sink is not None else persist_span

    runtime_tracer = tracer or Tracer(
        service,
        on_end=on_span_end,
        enable_opentelemetry=enable_opentelemetry,
    )
    runtime = Observability(
        service=service,
        logger=event_logger,
        metrics=registry,
        tracer=runtime_tracer,
        standard_metrics=standard,
        event_journal=journal,
        event_count=event_count,
        operation_duration=operation_duration,
        _async_span_sink=async_span_sink,
    )
    # Live workers deliberately enqueue local span persistence off their
    # latency-sensitive paths, and OTLP uses its own batch processor.  A
    # bounded process-exit hook gives both queues a final chance to flush when
    # a caller does not own an explicit application-lifespan callback.  The
    # shutdown methods are idempotent, so applications may still call them.
    if async_span_sink is not None or runtime.tracer.otlp_enabled:
        atexit.register(runtime.shutdown, 1.0)
    return runtime


def create_observability_from_env(
    service: str,
    *,
    env_prefix: str = "AI_PLATFORM_OBS_",
    stream: IO[str] | None = None,
) -> Observability:
    """Create a runtime using optional file sinks configured by environment."""

    log_path = os.environ.get(f"{env_prefix}LOG_PATH") or None
    journal_path = os.environ.get(f"{env_prefix}JOURNAL_PATH") or None
    return create_observability(
        service,
        log_path=log_path,
        journal_path=journal_path,
        stream=stream,
    )


__all__ = ["Observability", "create_observability", "create_observability_from_env"]
