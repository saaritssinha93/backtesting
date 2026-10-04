"""OpenTelemetry-compatible tracing with a dependency-free local fallback."""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Mapping, MutableMapping

from .context import bind_context, current_context
from .redaction import redact


_TRACEPARENT = re.compile(
    r"^(?P<version>[0-9a-f]{2})-(?P<trace>[0-9a-f]{32})-(?P<span>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})$",
    re.IGNORECASE,
)


def _otlp_http_trace_endpoint() -> str | None:
    """Resolve an explicitly enabled OTLP/HTTP trace endpoint.

    No endpoint means local tracing only.  Requiring an explicit setting keeps
    test/developer processes from creating an exporter thread or repeatedly
    dialing a collector that was never started.
    """

    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() in {
        "1", "true", "yes", "on"
    }:
        return None
    protocol = os.environ.get(
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
    ).strip().lower()
    if protocol not in {"http/protobuf", "http"}:
        return None
    explicit = (
        os.environ.get("AI_PLATFORM_OTLP_TRACES_ENDPOINT", "").strip()
        or os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "").strip()
    )
    if explicit:
        return explicit
    base = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    return f"{base.rstrip('/')}/v1/traces" if base else None


def _otel_value(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value if value is not None else "null"
    if isinstance(value, (list, tuple)) and all(
        isinstance(item, (str, bool, int, float)) for item in value
    ):
        return tuple(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _otel_attributes(values: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _otel_value(value) for key, value in values.items()}


def parse_traceparent(value: str | None) -> tuple[str, str] | None:
    """Return ``(trace_id, parent_span_id)`` for a valid W3C traceparent."""

    if not value:
        return None
    match = _TRACEPARENT.fullmatch(value.strip())
    if not match or match.group("version").lower() == "ff":
        return None
    trace_id = match.group("trace").lower()
    span_id = match.group("span").lower()
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None
    return trace_id, span_id


def current_trace_ids() -> tuple[str | None, str | None]:
    context = current_context()
    return context.trace_id, context.span_id


def current_traceparent(*, sampled: bool = True) -> str | None:
    trace_id, span_id = current_trace_ids()
    if not trace_id or not span_id:
        return None
    if not re.fullmatch(r"[0-9a-f]{32}", trace_id) or not re.fullmatch(
        r"[0-9a-f]{16}", span_id
    ):
        return None
    return f"00-{trace_id}-{span_id}-{'01' if sampled else '00'}"


def inject_trace_headers(
    carrier: MutableMapping[str, str] | None = None,
    *,
    sampled: bool = True,
) -> MutableMapping[str, str]:
    target: MutableMapping[str, str] = {} if carrier is None else carrier
    traceparent = current_traceparent(sampled=sampled)
    if traceparent:
        target["traceparent"] = traceparent
    return target


@dataclass(slots=True)
class SpanSnapshot:
    name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    start_ns: int
    end_ns: int
    outcome: str
    attributes: dict[str, object] = field(default_factory=dict)
    events: list[dict[str, object]] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        return max(0, self.end_ns - self.start_ns) / 1_000_000_000


class Span:
    """Context manager backed by OpenTelemetry when present, otherwise local."""

    def __init__(
        self,
        tracer: "Tracer",
        name: str,
        *,
        attributes: Mapping[str, object] | None = None,
        carrier: Mapping[str, str] | None = None,
        kind: str = "internal",
    ) -> None:
        self.tracer = tracer
        self.name = name
        self.attributes = dict(redact(dict(attributes or {})))
        self.carrier = carrier
        self.kind = kind
        self.events: list[dict[str, object]] = []
        self.trace_id = ""
        self.span_id = ""
        self.parent_span_id: str | None = None
        self.start_ns = 0
        self.end_ns = 0
        self.outcome = "ok"
        self._binding = None
        self._otel_manager = None
        self._otel_span = None

    def __enter__(self) -> "Span":
        self.start_ns = time.perf_counter_ns()
        active = current_context()
        incoming = parse_traceparent(
            self.carrier.get("traceparent") if self.carrier is not None else None
        )
        fallback_trace_id = (
            incoming[0] if incoming else active.trace_id or uuid.uuid4().hex
        )
        self.parent_span_id = incoming[1] if incoming else active.span_id

        if self.tracer._otel_tracer is not None:
            try:
                context = None
                if self.carrier is not None and self.tracer._otel_extract is not None:
                    context = self.tracer._otel_extract(self.carrier)
                kind = self.tracer._otel_kinds.get(self.kind, self.tracer._otel_kinds["internal"])
                self._otel_manager = self.tracer._otel_tracer.start_as_current_span(
                    self.name,
                    context=context,
                    kind=kind,
                    attributes=_otel_attributes(self.attributes),
                )
                self._otel_span = self._otel_manager.__enter__()
                span_context = self._otel_span.get_span_context()
                if getattr(span_context, "is_valid", False):
                    self.trace_id = f"{span_context.trace_id:032x}"
                    self.span_id = f"{span_context.span_id:016x}"
            except Exception:
                # A broken exporter/provider must not affect the trading path.
                self._otel_manager = None
                self._otel_span = None
                self.tracer.dropped_spans += 1

        self.trace_id = self.trace_id or fallback_trace_id
        self.span_id = self.span_id or uuid.uuid4().hex[:16]
        self._binding = bind_context(trace_id=self.trace_id, span_id=self.span_id)
        self._binding.__enter__()
        return self

    def set_attribute(self, key: str, value: object) -> None:
        cleaned = redact(value)
        self.attributes[str(key)] = cleaned
        if self._otel_span is not None:
            try:
                self._otel_span.set_attribute(str(key), _otel_value(cleaned))
            except Exception:
                self.tracer.dropped_spans += 1

    def add_event(self, name: str, **attributes: object) -> None:
        event = {"name": name, "attributes": redact(attributes), "offset_ns": time.perf_counter_ns() - self.start_ns}
        self.events.append(event)
        if self._otel_span is not None:
            try:
                self._otel_span.add_event(
                    name, attributes=_otel_attributes(event["attributes"])
                )
            except Exception:
                self.tracer.dropped_spans += 1

    def record_exception(self, error: BaseException) -> None:
        self.outcome = "error"
        self.add_event("exception", exception_type=type(error).__name__, exception_message=str(error))
        if self._otel_span is not None:
            try:
                self._otel_span.record_exception(error)
            except Exception:
                self.tracer.dropped_spans += 1

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if exc_value is not None:
            self.record_exception(exc_value)
        self.end_ns = time.perf_counter_ns()
        snapshot = SpanSnapshot(
            name=self.name,
            trace_id=self.trace_id,
            span_id=self.span_id,
            parent_span_id=self.parent_span_id,
            start_ns=self.start_ns,
            end_ns=self.end_ns,
            outcome=self.outcome,
            attributes=dict(self.attributes),
            events=list(self.events),
        )
        try:
            if self.tracer.on_end is not None:
                self.tracer.on_end(snapshot)
        except Exception:
            self.tracer.dropped_spans += 1
        finally:
            if self._binding is not None:
                self._binding.__exit__(exc_type, exc_value, traceback)
            if self._otel_manager is not None:
                try:
                    self._otel_manager.__exit__(exc_type, exc_value, traceback)
                except Exception:
                    self.tracer.dropped_spans += 1
        return False


class Tracer:
    """Small tracing API that opportunistically bridges to OpenTelemetry."""

    def __init__(
        self,
        service: str,
        *,
        on_end: Callable[[SpanSnapshot], None] | None = None,
        enable_opentelemetry: bool = True,
    ) -> None:
        self.service = service
        self.on_end = on_end
        self.dropped_spans = 0
        self._otel_tracer = None
        self._otel_provider = None
        self._otel_extract = None
        self.otel_endpoint: str | None = None
        self.otel_error: str | None = None
        self._otel_kinds: dict[str, object] = {}
        if enable_opentelemetry:
            try:
                from opentelemetry import trace  # type: ignore[import-not-found]
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (  # type: ignore[import-not-found]
                    OTLPSpanExporter,
                )
                from opentelemetry.propagate import extract  # type: ignore[import-not-found]
                from opentelemetry.sdk.resources import Resource  # type: ignore[import-not-found]
                from opentelemetry.sdk.trace import TracerProvider  # type: ignore[import-not-found]
                from opentelemetry.sdk.trace.export import BatchSpanProcessor  # type: ignore[import-not-found]
                from opentelemetry.trace import SpanKind  # type: ignore[import-not-found]

                endpoint = _otlp_http_trace_endpoint()
                if endpoint:
                    provider = TracerProvider(
                        resource=Resource.create({"service.name": service})
                    )
                    provider.add_span_processor(
                        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
                    )
                    self._otel_provider = provider
                    self._otel_tracer = provider.get_tracer(
                        "ai_platform.observability", "1"
                    )
                    self.otel_endpoint = endpoint
                self._otel_extract = extract
                self._otel_kinds = {
                    "internal": SpanKind.INTERNAL,
                    "server": SpanKind.SERVER,
                    "client": SpanKind.CLIENT,
                    "producer": SpanKind.PRODUCER,
                    "consumer": SpanKind.CONSUMER,
                }
            except Exception as exc:
                self._otel_tracer = None
                self._otel_provider = None
                self._otel_extract = None
                self.otel_error = type(exc).__name__
        if not self._otel_kinds:
            self._otel_kinds = {key: key for key in ("internal", "server", "client", "producer", "consumer")}

    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, object] | None = None,
        carrier: Mapping[str, str] | None = None,
        kind: str = "internal",
    ) -> Span:
        if kind not in self._otel_kinds:
            raise ValueError(f"Unsupported span kind: {kind}")
        return Span(self, name, attributes=attributes, carrier=carrier, kind=kind)

    @property
    def otlp_enabled(self) -> bool:
        return self._otel_provider is not None and self._otel_tracer is not None

    def force_flush(self, timeout_millis: int = 5_000) -> bool:
        provider = self._otel_provider
        if provider is None:
            return True
        try:
            return bool(provider.force_flush(timeout_millis=timeout_millis))
        except Exception:
            self.dropped_spans += 1
            return False

    def shutdown(self) -> None:
        provider = self._otel_provider
        self._otel_provider = None
        if provider is None:
            return
        try:
            provider.shutdown()
        except Exception:
            self.dropped_spans += 1


__all__ = [
    "Span",
    "SpanSnapshot",
    "Tracer",
    "current_trace_ids",
    "current_traceparent",
    "inject_trace_headers",
    "parse_traceparent",
]
