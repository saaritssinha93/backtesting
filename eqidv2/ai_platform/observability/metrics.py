"""Dependency-free, bounded Prometheus/OpenMetrics-style metric primitives.

The registry intentionally prevents unbounded series creation.  Identifiers
such as ``signal_id``, ``order_id`` and ``symbol`` belong in logs/traces and
must not be metric labels.
"""

from __future__ import annotations

import math
import re
import threading
from dataclasses import dataclass, field
from typing import Iterable, Mapping


PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

_METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_FORBIDDEN_LABELS = frozenset(
    {
        "signal",
        "signal_id",
        "order",
        "order_id",
        "request_id",
        "run_id",
        "trace_id",
        "span_id",
        "symbol",
        "tradingsymbol",
    }
)


def _escape_help(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n")


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _number(value: float) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if math.isnan(value):
        return "NaN"
    return format(value, ".15g")


@dataclass(slots=True)
class _HistogramValue:
    buckets: list[int]
    count: int = 0
    total: float = 0.0


class _Metric:
    metric_type = "untyped"

    def __init__(
        self,
        registry: "MetricsRegistry",
        name: str,
        documentation: str,
        label_names: tuple[str, ...],
        max_series: int,
    ) -> None:
        self.registry = registry
        self.name = name
        self.documentation = documentation
        self.label_names = label_names
        self.max_series = max_series
        self._values: dict[tuple[str, ...], object] = {}

    def _label_tuple(
        self,
        labels: Mapping[str, object] | None,
        label_values: Mapping[str, object],
    ) -> tuple[str, ...]:
        supplied = {**(labels or {}), **label_values}
        missing = set(self.label_names) - set(supplied)
        extra = set(supplied) - set(self.label_names)
        if missing or extra:
            raise ValueError(
                f"Metric {self.name} label mismatch; missing={sorted(missing)}, extra={sorted(extra)}"
            )
        values = tuple(str(supplied[name])[:128] for name in self.label_names)
        if values not in self._values and len(self._values) >= self.max_series:
            raise OverflowError(f"Metric {self.name} exceeded its {self.max_series}-series limit")
        return values

    def _labels_text(self, values: tuple[str, ...], extra: tuple[str, str] | None = None) -> str:
        pairs = [
            f'{name}="{_escape_label(value)}"'
            for name, value in zip(self.label_names, values, strict=True)
        ]
        if extra is not None:
            pairs.append(f'{extra[0]}="{_escape_label(extra[1])}"')
        return "{" + ",".join(pairs) + "}" if pairs else ""

    def _drop(self, reason: str) -> bool:
        self.registry._record_drop(reason)
        return False

    def exposition(self) -> list[str]:
        raise NotImplementedError


class Counter(_Metric):
    metric_type = "counter"

    def inc(
        self,
        amount: float = 1.0,
        labels: Mapping[str, object] | None = None,
        **label_values: object,
    ) -> bool:
        try:
            numeric = float(amount)
            if not math.isfinite(numeric) or numeric < 0:
                raise ValueError("Counters require a finite, non-negative increment")
            with self.registry._lock:
                key = self._label_tuple(labels, label_values)
                self._values[key] = float(self._values.get(key, 0.0)) + numeric
            return True
        except Exception as exc:
            if self.registry.strict:
                raise
            return self._drop(type(exc).__name__)

    def exposition(self) -> list[str]:
        return [
            f"{self.name}{self._labels_text(labels)} {_number(float(value))}"
            for labels, value in sorted(self._values.items())
        ]


class Gauge(_Metric):
    metric_type = "gauge"

    def set(
        self,
        value: float,
        labels: Mapping[str, object] | None = None,
        **label_values: object,
    ) -> bool:
        try:
            numeric = float(value)
            if not math.isfinite(numeric):
                raise ValueError("Gauges require a finite value")
            with self.registry._lock:
                key = self._label_tuple(labels, label_values)
                self._values[key] = numeric
            return True
        except Exception as exc:
            if self.registry.strict:
                raise
            return self._drop(type(exc).__name__)

    def inc(
        self,
        amount: float = 1.0,
        labels: Mapping[str, object] | None = None,
        **label_values: object,
    ) -> bool:
        try:
            numeric = float(amount)
            if not math.isfinite(numeric):
                raise ValueError("Gauges require a finite increment")
            with self.registry._lock:
                key = self._label_tuple(labels, label_values)
                self._values[key] = float(self._values.get(key, 0.0)) + numeric
            return True
        except Exception as exc:
            if self.registry.strict:
                raise
            return self._drop(type(exc).__name__)

    def dec(
        self,
        amount: float = 1.0,
        labels: Mapping[str, object] | None = None,
        **label_values: object,
    ) -> bool:
        return self.inc(-amount, labels, **label_values)

    def exposition(self) -> list[str]:
        return [
            f"{self.name}{self._labels_text(labels)} {_number(float(value))}"
            for labels, value in sorted(self._values.items())
        ]


class Histogram(_Metric):
    metric_type = "histogram"

    def __init__(
        self,
        registry: "MetricsRegistry",
        name: str,
        documentation: str,
        label_names: tuple[str, ...],
        max_series: int,
        buckets: Iterable[float],
    ) -> None:
        super().__init__(registry, name, documentation, label_names, max_series)
        normalized = sorted({float(value) for value in buckets})
        if not normalized or any(not math.isfinite(value) for value in normalized):
            raise ValueError("Histogram buckets must be finite numbers")
        self.buckets = tuple(normalized)

    def observe(
        self,
        value: float,
        labels: Mapping[str, object] | None = None,
        **label_values: object,
    ) -> bool:
        try:
            numeric = float(value)
            if not math.isfinite(numeric):
                raise ValueError("Histograms require a finite observation")
            with self.registry._lock:
                key = self._label_tuple(labels, label_values)
                state = self._values.get(key)
                if state is None:
                    state = _HistogramValue([0] * len(self.buckets))
                    self._values[key] = state
                assert isinstance(state, _HistogramValue)
                state.count += 1
                state.total += numeric
                for index, upper_bound in enumerate(self.buckets):
                    if numeric <= upper_bound:
                        state.buckets[index] += 1
            return True
        except Exception as exc:
            if self.registry.strict:
                raise
            return self._drop(type(exc).__name__)

    def exposition(self) -> list[str]:
        lines: list[str] = []
        for labels, raw_state in sorted(self._values.items()):
            assert isinstance(raw_state, _HistogramValue)
            for upper_bound, count in zip(self.buckets, raw_state.buckets, strict=True):
                lines.append(
                    f"{self.name}_bucket{self._labels_text(labels, ('le', _number(upper_bound)))} {count}"
                )
            lines.append(
                f"{self.name}_bucket{self._labels_text(labels, ('le', '+Inf'))} {raw_state.count}"
            )
            lines.append(f"{self.name}_sum{self._labels_text(labels)} {_number(raw_state.total)}")
            lines.append(f"{self.name}_count{self._labels_text(labels)} {raw_state.count}")
        return lines


class MetricsRegistry:
    """Thread-safe metric registry with per-metric cardinality bounds."""

    def __init__(
        self,
        *,
        namespace: str = "trading",
        max_series_per_metric: int = 1_000,
        strict: bool = False,
    ) -> None:
        if namespace and not _METRIC_NAME.fullmatch(namespace):
            raise ValueError(f"Invalid metric namespace: {namespace!r}")
        if max_series_per_metric < 1:
            raise ValueError("max_series_per_metric must be positive")
        self.namespace = namespace
        self.max_series_per_metric = max_series_per_metric
        self.strict = strict
        self._metrics: dict[str, _Metric] = {}
        self._dropped: dict[str, int] = {}
        self._lock = threading.RLock()

    def _full_name(self, name: str) -> str:
        full_name = f"{self.namespace}_{name}" if self.namespace else name
        if not _METRIC_NAME.fullmatch(full_name):
            raise ValueError(f"Invalid metric name: {full_name!r}")
        return full_name

    @staticmethod
    def _validate_labels(label_names: Iterable[str]) -> tuple[str, ...]:
        names = tuple(label_names)
        if len(names) != len(set(names)):
            raise ValueError("Metric label names must be unique")
        invalid = [name for name in names if not _LABEL_NAME.fullmatch(name)]
        if invalid:
            raise ValueError(f"Invalid metric label name(s): {invalid}")
        forbidden = _FORBIDDEN_LABELS.intersection(names)
        if forbidden:
            raise ValueError(
                "High-cardinality identifiers cannot be metric labels: "
                + ", ".join(sorted(forbidden))
            )
        return names

    def _register(self, metric: _Metric) -> _Metric:
        with self._lock:
            existing = self._metrics.get(metric.name)
            if existing is None:
                self._metrics[metric.name] = metric
                return metric
            if (
                type(existing) is not type(metric)
                or existing.label_names != metric.label_names
                or existing.documentation != metric.documentation
                or (
                    isinstance(existing, Histogram)
                    and isinstance(metric, Histogram)
                    and existing.buckets != metric.buckets
                )
            ):
                raise ValueError(f"Metric {metric.name!r} is already registered differently")
            return existing

    def counter(
        self,
        name: str,
        documentation: str,
        *,
        label_names: Iterable[str] = (),
        max_series: int | None = None,
    ) -> Counter:
        metric = Counter(
            self,
            self._full_name(name),
            documentation,
            self._validate_labels(label_names),
            max_series or self.max_series_per_metric,
        )
        return self._register(metric)  # type: ignore[return-value]

    def gauge(
        self,
        name: str,
        documentation: str,
        *,
        label_names: Iterable[str] = (),
        max_series: int | None = None,
    ) -> Gauge:
        metric = Gauge(
            self,
            self._full_name(name),
            documentation,
            self._validate_labels(label_names),
            max_series or self.max_series_per_metric,
        )
        return self._register(metric)  # type: ignore[return-value]

    def histogram(
        self,
        name: str,
        documentation: str,
        *,
        label_names: Iterable[str] = (),
        buckets: Iterable[float] = DEFAULT_BUCKETS,
        max_series: int | None = None,
    ) -> Histogram:
        metric = Histogram(
            self,
            self._full_name(name),
            documentation,
            self._validate_labels(label_names),
            max_series or self.max_series_per_metric,
            buckets,
        )
        return self._register(metric)  # type: ignore[return-value]

    def _record_drop(self, reason: str) -> None:
        with self._lock:
            self._dropped[reason] = self._dropped.get(reason, 0) + 1
            dropped = self._metrics.get(self._full_name("telemetry_dropped_total"))
            if isinstance(dropped, Counter) and dropped.label_names == ("component", "reason"):
                key = ("metrics", reason[:128])
                if key in dropped._values or len(dropped._values) < dropped.max_series:
                    dropped._values[key] = float(dropped._values.get(key, 0.0)) + 1.0

    @property
    def dropped_updates(self) -> int:
        with self._lock:
            return sum(self._dropped.values())

    def render_prometheus(self) -> str:
        """Render a consistent Prometheus text snapshot."""

        with self._lock:
            lines: list[str] = []
            for name, metric in sorted(self._metrics.items()):
                lines.append(f"# HELP {name} {_escape_help(metric.documentation)}")
                lines.append(f"# TYPE {name} {metric.metric_type}")
                lines.extend(metric.exposition())
            dropped_name = self._full_name("telemetry_dropped_total")
            if dropped_name not in self._metrics:
                lines.extend(
                    (
                        f"# HELP {dropped_name} Telemetry updates dropped before export.",
                        f"# TYPE {dropped_name} counter",
                    )
                )
                for reason, count in sorted(self._dropped.items()):
                    lines.append(f'{dropped_name}{{component="metrics",reason="{_escape_label(reason)}"}} {count}')
            return "\n".join(lines) + "\n"


__all__ = [
    "Counter",
    "DEFAULT_BUCKETS",
    "Gauge",
    "Histogram",
    "MetricsRegistry",
    "PROMETHEUS_CONTENT_TYPE",
]
