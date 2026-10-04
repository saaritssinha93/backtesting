"""Fail-open filesystem metrics for the local trading runtime.

This module deliberately uses only the Python standard library.  It reads the
same immutable/status artifacts that the trading processes already publish;
it never imports a strategy module, calls a broker, or changes trading state.

The registry in :mod:`ai_platform.observability.metrics` owns HELP/TYPE lines.
This collector therefore returns sample lines only.  ``merge_prometheus_samples``
can safely append them to an existing registry exposition without emitting a
duplicate label set.
"""

from __future__ import annotations

import csv
import hashlib
import heapq
import json
import math
import os
import re
import shutil
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, time as day_time, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")

_METRIC_NAME = r"[a-zA-Z_:][a-zA-Z0-9_:]*"
_LABEL_NAME = r"[a-zA-Z_][a-zA-Z0-9_]*"
_LABEL_VALUE = r'"(?:[^"\\\n]|\\["\\n])*"'
_LABEL_PAIR = rf"{_LABEL_NAME}\s*=\s*{_LABEL_VALUE}"
_LABELS = rf"(?:{_LABEL_PAIR})(?:\s*,\s*{_LABEL_PAIR})*"
_NUMBER = r"(?:[-+]?(?:(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|Inf)|NaN)"
_SAMPLE_RE = re.compile(
    rf"^(?P<name>{_METRIC_NAME})(?P<labels>\{{(?:{_LABELS})?\}})?"
    rf"\s+(?P<value>{_NUMBER})(?:\s+(?P<timestamp>-?\d+))?$"
)
_LABEL_RE = re.compile(rf"(?P<name>{_LABEL_NAME})\s*=\s*(?P<value>{_LABEL_VALUE})")
_SAFE_LABEL = re.compile(r"[^a-zA-Z0-9_.:/-]+")

_ADDITIVE_SUFFIXES = ("_total", "_count", "_sum", "_bucket")
_MAX_CONSERVATIVE_PARTS = (
    "_age_seconds",
    "_mismatch",
    "_schedule_overdue",
    "_slot_incomplete",
    "_unprotected_position_seconds",
    "_replay_due",
    "_fingerprint_mismatch",
)
_MIN_CONSERVATIVE_PARTS = ("_coverage_ratio", "_disk_free_bytes")

_MAX_TOTAL_SERIES = 4096
_MAX_SERIES_PER_METRIC = 1000
_MAX_RECONCILIATION_ARTIFACTS = 4096
_MAX_LABEL_VALUE_LENGTH = 128
_MAX_FUTURE_SKEW_SECONDS = 0.0

_V13_SCANNER_SESSION = "fno_v13_v10_g_scanner_5min"
_V13_SCANNER_SLOTS = (
    "09:25",
    "09:30",
    "09:35",
    "09:40",
    "09:45",
    "09:50",
    "09:55",
    "10:00",
    "11:20",
)
_V13_SCANNER_COMPLETION_GRACE_SECONDS = 180.0

# These executables are successive implementations of the same production OI
# pipeline.  Heartbeat alerting must describe the logical dependency, not every
# retired executable name: during a cutover, a fresh replacement heartbeat is
# sufficient even when the old producer leaves a same-day non-terminal marker.
_HEARTBEAT_SERVICE_EQUIVALENTS = {
    "fno_oi_fetch_5min": "fno_oi_5min_production",
    "fno_oi_fetch_5min.py": "fno_oi_5min_production",
    "fno_oi_fetch_5min_fast_production": "fno_oi_5min_production",
    "fno_oi_fetch_5min_fast_production.py": "fno_oi_5min_production",
}

# Textfiles are an untrusted process boundary.  Keep this allow-list in lockstep
# with ``catalog.register_standard_metrics`` plus the three runtime/API metrics
# that are intentionally registered outside the standard catalog.  Requiring
# the exact label set prevents an accidental run/order/symbol identifier from
# turning into unbounded Prometheus cardinality.
_TEXTFILE_SAMPLE_SCHEMAS: dict[str, frozenset[str]] = {
    "trading_market_open": frozenset(),
    "trading_maintenance_window": frozenset(),
    "trading_heartbeat_age_seconds": frozenset(("service", "mode")),
    "trading_pipeline_schedule_overdue": frozenset(("pipeline", "mode")),
    "trading_data_age_seconds": frozenset(("source", "timeframe")),
    "trading_data_coverage_ratio": frozenset(("source", "timeframe")),
    "trading_data_slot_incomplete": frozenset(("source", "timeframe", "slot")),
    "trading_raw_data_anomaly_total": frozenset(("source", "anomaly_type")),
    "trading_signal_events_total": frozenset(("strategy", "mode", "event", "outcome")),
    "trading_broker_requests_total": frozenset(("operation", "outcome")),
    "trading_unprotected_position_seconds": frozenset(("mode", "asset")),
    "trading_position_reconciliation_mismatch": frozenset(("mode", "asset")),
    "trading_active_order_reconciliation_mismatch": frozenset(("mode", "asset")),
    "trading_duplicate_order_total": frozenset(("mode", "asset")),
    "trading_strategy_fingerprint_mismatch": frozenset(("service", "strategy", "mode")),
    "trading_process_restarts_total": frozenset(("service", "strategy", "mode")),
    "trading_clock_offset_seconds": frozenset(("service",)),
    "trading_replay_due": frozenset(("profile", "replay_kind")),
    "trading_replay_success_timestamp_seconds": frozenset(("profile", "replay_kind")),
    "trading_live_eod_mismatch_total": frozenset(("stage", "mismatch_type")),
    "trading_feature_parity_mismatch_total": frozenset(("strategy", "feature")),
    "trading_telemetry_dropped_total": frozenset(("component", "reason")),
    "trading_order_events_total": frozenset(("strategy", "mode", "asset", "event", "outcome")),
    "trading_disk_free_bytes": frozenset(("service", "volume")),
    "trading_http_requests_total": frozenset(("method", "route", "status_code")),
    "trading_observability_events_total": frozenset(("severity", "outcome")),
    "trading_http_requests_in_flight": frozenset(("method",)),
}

_DEFAULT_HISTOGRAM_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
_HISTOGRAM_DEFINITIONS = {
    "trading_slot_deadline_lag_seconds": (
        ("pipeline", "mode"),
        (-120, -60, -30, -10, -5, -2, -1, -0.5, -0.1, 0, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120),
    ),
    "trading_stage_duration_seconds": (
        ("strategy", "mode", "stage"),
        (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120),
    ),
    "trading_broker_request_duration_seconds": (
        ("operation", "outcome"),
        (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
    ),
    "trading_http_request_duration_seconds": (
        ("method", "route", "status_class"),
        _DEFAULT_HISTOGRAM_BUCKETS,
    ),
    "trading_operation_duration_seconds": (
        ("operation", "outcome"),
        _DEFAULT_HISTOGRAM_BUCKETS,
    ),
}
_TEXTFILE_HISTOGRAM_BUCKETS: dict[str, frozenset[str]] = {}
for _histogram_name, (_histogram_labels, _histogram_buckets) in _HISTOGRAM_DEFINITIONS.items():
    _bucket_name = f"{_histogram_name}_bucket"
    _TEXTFILE_HISTOGRAM_BUCKETS[_bucket_name] = frozenset(
        {f'"{format(float(value), ".15g")}"' for value in _histogram_buckets}
        | {'"+Inf"'}
    )
    _base_labels = frozenset(_histogram_labels)
    _TEXTFILE_SAMPLE_SCHEMAS[_bucket_name] = _base_labels | {"le"}
    _TEXTFILE_SAMPLE_SCHEMAS[f"{_histogram_name}_count"] = _base_labels
    _TEXTFILE_SAMPLE_SCHEMAS[f"{_histogram_name}_sum"] = _base_labels


@dataclass(frozen=True, slots=True)
class _ParsedSample:
    name: str
    labels: tuple[tuple[str, str], ...]
    value: float
    timestamp_ms: int | None

    @property
    def key(self) -> tuple[str, tuple[tuple[str, str], ...]]:
        return self.name, self.labels


@dataclass(slots=True)
class _SampleValue:
    sample: _ParsedSample
    observed_at: float


def _parse_sample(line: str, *, trading_only: bool = False) -> _ParsedSample | None:
    """Parse one bounded Prometheus 0.0.4 sample (comments are rejected)."""

    if not line or len(line) > 4096 or line.startswith("#"):
        return None
    match = _SAMPLE_RE.fullmatch(line.strip())
    if match is None:
        return None
    name = match.group("name")
    if trading_only and not name.startswith("trading_"):
        return None
    try:
        value = float(match.group("value"))
        timestamp = int(match.group("timestamp")) if match.group("timestamp") else None
    except (TypeError, ValueError, OverflowError):
        return None
    if timestamp is not None and abs(timestamp) > 253_402_300_799_999:
        # Millisecond Unix timestamps outside Python's supported datetime
        # range are never legitimate snapshots and can overflow conversion.
        return None
    raw_labels = match.group("labels") or ""
    labels = tuple(
        sorted(
            (item.group("name"), item.group("value"))
            for item in _LABEL_RE.finditer(raw_labels)
        )
    )
    # The outer expression already validates the complete label block.  Reject
    # duplicate label names, which Prometheus itself considers invalid.
    if len({name for name, _ in labels}) != len(labels):
        return None
    return _ParsedSample(name, labels, value, timestamp)


def _quoted_label_length(value: str) -> int:
    """Return decoded label length without allocating an unescaped string."""

    length = 0
    index = 1
    while index < len(value) - 1:
        if value[index] == "\\":
            index += 2
        else:
            index += 1
        length += 1
    return length


def _matches_textfile_schema(
    sample: _ParsedSample, *, newest_allowed: float | None = None
) -> bool:
    expected = _TEXTFILE_SAMPLE_SCHEMAS.get(sample.name)
    if expected is None or not math.isfinite(sample.value):
        return False
    if sample.name.endswith(("_total", "_count", "_bucket")) and sample.value < 0:
        return False
    if (
        sample.name == "trading_replay_success_timestamp_seconds"
        and (
            sample.value < 0
            or (newest_allowed is not None and sample.value > newest_allowed)
        )
    ):
        return False
    if frozenset(name for name, _ in sample.labels) != expected:
        return False
    allowed_buckets = _TEXTFILE_HISTOGRAM_BUCKETS.get(sample.name)
    if allowed_buckets is not None:
        label_values = dict(sample.labels)
        if label_values.get("le") not in allowed_buckets:
            return False
    return all(
        _quoted_label_length(value) <= _MAX_LABEL_VALUE_LENGTH
        for _, value in sample.labels
    )


def _number(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    return format(value, ".15g")


def _sample_line(sample: _ParsedSample) -> str:
    labels = ""
    if sample.labels:
        labels = "{" + ",".join(f"{name}={value}" for name, value in sample.labels) + "}"
    stamp = f" {sample.timestamp_ms}" if sample.timestamp_ms is not None else ""
    return f"{sample.name}{labels} {_number(sample.value)}{stamp}"


def _merge_value(current: _SampleValue, incoming: _SampleValue) -> _SampleValue:
    """Merge duplicate series deterministically and conservatively."""

    name = current.sample.name
    left = current.sample.value
    right = incoming.sample.value
    # A distinct textfile (or the in-process API registry) represents an
    # independent producer.  Its cumulative counter/histogram snapshot is
    # summed once per scrape.  Duplicate rows within one producer are rejected
    # before reaching this merge function.
    if name.endswith(_ADDITIVE_SUFFIXES) and math.isfinite(left) and math.isfinite(right):
        merged = _ParsedSample(name, current.sample.labels, left + right, None)
        return _SampleValue(merged, max(current.observed_at, incoming.observed_at))
    if any(part in name for part in _MAX_CONSERVATIVE_PARTS):
        chosen = current if left >= right else incoming
        return chosen
    if any(part in name for part in _MIN_CONSERVATIVE_PARTS):
        chosen = current if left <= right else incoming
        return chosen
    # Gauges from textfile snapshots use an explicit sample timestamp when
    # present and the file mtime otherwise.  The newest observation wins.
    return incoming if incoming.observed_at > current.observed_at else current


def merge_prometheus_samples(base: str, additions: str) -> str:
    """Append samples without duplicate label sets or duplicate metadata.

    Existing HELP/TYPE/comment lines remain byte-for-byte intact.  Duplicate
    independent counter/histogram producer snapshots are summed; safety gauges
    choose the conservative value; other duplicate gauges deterministically
    keep the existing registry value because its collection time is newest.
    """

    lines = base.rstrip("\n").splitlines() if base else []
    positions: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
    values: dict[tuple[str, tuple[tuple[str, str], ...]], _SampleValue] = {}
    registry_observed = time.time()
    for index, line in enumerate(lines):
        parsed = _parse_sample(line)
        if parsed is None:
            continue
        positions[parsed.key] = index
        values[parsed.key] = _SampleValue(parsed, registry_observed)

    for line in additions.splitlines():
        parsed = _parse_sample(line, trading_only=True)
        if parsed is None or not _matches_textfile_schema(
            parsed, newest_allowed=registry_observed
        ):
            continue
        # Textfile timestamps are milliseconds since epoch.  Samples without
        # one are older than this just-rendered in-process registry snapshot.
        if parsed.timestamp_ms is not None:
            observed = parsed.timestamp_ms / 1000.0
            if observed > registry_observed + _MAX_FUTURE_SKEW_SECONDS:
                continue
        else:
            observed = registry_observed - 1e-6
        # The API endpoint is a new scrape target, not a transparent timestamp
        # proxy.  Preserve observation time only for deterministic selection;
        # forwarding explicit textfile timestamps can create future or
        # out-of-order samples in Prometheus.
        normalized = _ParsedSample(parsed.name, parsed.labels, parsed.value, None)
        incoming = _SampleValue(normalized, min(observed, registry_observed - 1e-6))
        current = values.get(parsed.key)
        if current is None:
            positions[parsed.key] = len(lines)
            values[parsed.key] = incoming
            lines.append(_sample_line(normalized))
            continue
        merged = _merge_value(current, incoming)
        values[parsed.key] = merged
        lines[positions[parsed.key]] = _sample_line(merged.sample)
    return "\n".join(lines) + "\n"


class _Samples:
    def __init__(
        self,
        *,
        max_series: int = _MAX_TOTAL_SERIES,
        max_series_per_metric: int = _MAX_SERIES_PER_METRIC,
    ) -> None:
        self.values: dict[tuple[str, tuple[tuple[str, str], ...]], _SampleValue] = {}
        self._metric_counts: dict[str, int] = defaultdict(int)
        self._max_series = max(1, int(max_series))
        self._max_series_per_metric = max(1, int(max_series_per_metric))

    @staticmethod
    def _quoted(value: object) -> str:
        # Truncate before escaping so the decoded label remains bounded and an
        # escape sequence can never be cut in half.
        text = str(value)[:_MAX_LABEL_VALUE_LENGTH]
        escaped = text.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
        return f'"{escaped}"'

    def _store(self, incoming: _SampleValue) -> bool:
        parsed = incoming.sample
        existing = self.values.get(parsed.key)
        if existing is not None:
            self.values[parsed.key] = _merge_value(existing, incoming)
            return True
        if (
            len(self.values) >= self._max_series
            or self._metric_counts[parsed.name] >= self._max_series_per_metric
        ):
            return False
        self.values[parsed.key] = incoming
        self._metric_counts[parsed.name] += 1
        return True

    def add(
        self,
        name: str,
        value: object,
        labels: Mapping[str, object] | None = None,
        *,
        observed_at: float | None = None,
    ) -> bool:
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(numeric):
            return False
        normalized = tuple(
            sorted((str(key), self._quoted(raw)) for key, raw in (labels or {}).items())
        )
        parsed = _ParsedSample(name, normalized, numeric, None)
        incoming = _SampleValue(parsed, observed_at if observed_at is not None else time.time())
        return self._store(incoming)

    def add_line(
        self,
        line: str,
        *,
        observed_at: float,
        oldest_allowed: float,
        newest_allowed: float,
        seen_in_producer: set[tuple[str, tuple[tuple[str, str], ...]]] | None = None,
    ) -> bool:
        parsed = _parse_sample(line, trading_only=True)
        if parsed is None or not _matches_textfile_schema(
            parsed, newest_allowed=newest_allowed
        ):
            return False
        if seen_in_producer is not None:
            if parsed.key in seen_in_producer:
                return False
        effective = (
            parsed.timestamp_ms / 1000.0
            if parsed.timestamp_ms is not None
            else observed_at
        )
        if effective < oldest_allowed or effective > newest_allowed:
            return False
        if seen_in_producer is not None:
            seen_in_producer.add(parsed.key)
        normalized = _ParsedSample(parsed.name, parsed.labels, parsed.value, None)
        incoming = _SampleValue(normalized, effective)
        return self._store(incoming)

    def render(self) -> str:
        return "\n".join(
            _sample_line(self.values[key].sample) for key in sorted(self.values)
        ) + ("\n" if self.values else "")


class RuntimeMetricsCollector:
    """Collect low-cardinality operational metrics from runtime artifacts."""

    def __init__(
        self,
        runtime_root: Path | str,
        *,
        observability_root: Path | str | None = None,
        profile_registry_path: Path | str | None = None,
        cache_ttl_seconds: float = 5.0,
        textfile_max_age_seconds: float = 300.0,
        now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.runtime_root = Path(runtime_root).expanduser().resolve()
        self.observability_root = Path(
            observability_root or self.runtime_root / "observability"
        ).expanduser().resolve()
        self.profile_registry_path = (
            Path(profile_registry_path).expanduser().resolve()
            if profile_registry_path is not None
            else None
        )
        self.cache_ttl_seconds = min(5.0, max(0.0, float(cache_ttl_seconds)))
        self.textfile_max_age_seconds = max(0.0, min(float(textfile_max_age_seconds), 86400.0))
        self._now_fn = now or (lambda: datetime.now(tz=IST))
        self._monotonic = monotonic or time.monotonic
        self._lock = threading.RLock()
        self._cached = ""
        self._cached_until = -1.0
        self._reconciliation_seen: set[str] = set()
        self._reconciliation_seen_order: deque[str] = deque()
        self._reconciliation_mismatch_totals: dict[tuple[str, str], int] = {}
        self._reconciliation_feature_totals: dict[str, int] = {}

    def render_prometheus_samples(self) -> str:
        """Return a cached sample-only snapshot; telemetry failures are empty."""

        observed = self._monotonic()
        with self._lock:
            if observed < self._cached_until:
                return self._cached
            try:
                value = self._collect()
            except Exception:
                # This endpoint must never make the read API or trading stack
                # unavailable because an observability artifact is corrupt.
                value = ""
            self._cached = value
            self._cached_until = observed + self.cache_ttl_seconds
            return value

    def _now(self) -> datetime:
        value = self._now_fn()
        if value.tzinfo is None:
            value = value.replace(tzinfo=IST)
        return value.astimezone(IST)

    def _collect(self) -> str:
        samples = _Samples()
        now = self._now()
        collectors = (
            self._collect_schedule,
            self._collect_heartbeats,
            self._collect_pipeline_schedule,
            self._collect_data_markers,
            self._collect_disk,
            self._collect_replay,
            self._collect_fingerprint,
            self._collect_supervisors,
            self._collect_local_protection,
            self._collect_reconciliation,
            self._collect_textfiles,
        )
        for collector in collectors:
            try:
                collector(samples, now)
            except Exception:
                # Each source is isolated so one malformed artifact cannot hide
                # the other metrics on the scrape.
                continue
        return samples.render()

    @staticmethod
    def _read_artifact(path: Path, *, max_bytes: int = 1024 * 1024) -> dict[str, object]:
        try:
            stat = path.stat()
            if not path.is_file() or stat.st_size < 1 or stat.st_size > max_bytes:
                return {}
            text = path.read_text(encoding="utf-8-sig", errors="strict")
        except (OSError, UnicodeError):
            return {}
        stripped = text.lstrip()
        if stripped.startswith("{"):
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return {}
            return dict(payload) if isinstance(payload, dict) else {}
        result: dict[str, object] = {}
        for line in text.splitlines()[:256]:
            if not line.strip() or line.lstrip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key and len(key) <= 128:
                result[key] = value.strip()
        return result

    @staticmethod
    def _parse_datetime(value: object, *, utc_hint: bool = False) -> datetime | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            try:
                numeric = float(value)
                if not math.isfinite(numeric):
                    return None
                return datetime.fromtimestamp(numeric, tz=timezone.utc)
            except (OSError, OverflowError, ValueError):
                return None
        text = str(value).strip()
        if not text:
            return None
        candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}(?:\.\d+)?", candidate):
            candidate = candidate.replace("_", "T", 1)
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc if utc_hint else IST)
        return parsed

    @classmethod
    def _semantic_timestamp(cls, payload: Mapping[str, object]) -> datetime | None:
        for key in (
            "heartbeat_ist",
            "heartbeat_at",
            "ts_utc",
            "updated_utc",
            "updated_at_ist",
            "updated_at",
            "published_at_ist",
            "observed_at_ist",
            "generated_at_ist",
            "ts",
        ):
            if key not in payload:
                continue
            parsed = cls._parse_datetime(payload.get(key), utc_hint=key.endswith("_utc"))
            if parsed is not None:
                return parsed
        return None

    @classmethod
    def _artifact_timestamp(cls, payload: Mapping[str, object], path: Path) -> datetime | None:
        semantic = cls._semantic_timestamp(payload)
        if semantic is not None:
            return semantic
        try:
            return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            return None

    @staticmethod
    def _age_seconds(now: datetime, stamp: datetime) -> float | None:
        delta = (
            now.astimezone(timezone.utc) - stamp.astimezone(timezone.utc)
        ).total_seconds()
        if delta < -_MAX_FUTURE_SKEW_SECONDS:
            return None
        return max(0.0, delta)

    @staticmethod
    def _label(value: object, default: str = "unknown") -> str:
        text = _SAFE_LABEL.sub("_", str(value).strip()).strip("_.-")
        return (text or default)[:96]

    @classmethod
    def _mode(cls, payload: Mapping[str, object], service: str) -> str:
        explicit = payload.get("execution_mode") or payload.get("mode")
        if explicit:
            return cls._label(explicit).lower()
        lowered = service.lower()
        if "backtest" in lowered or "replay" in lowered:
            return "replay"
        if "paper" in lowered:
            return "paper"
        if any(word in lowered for word in ("live", "fetch", "signal", "scheduler")):
            return "live"
        return "operational"

    @staticmethod
    def _is_terminal(payload: Mapping[str, object]) -> bool:
        state = str(payload.get("state") or payload.get("status") or "").upper()
        return state in {
            "CANCELLED",
            "COMPLETE",
            "COMPLETED",
            "DONE",
            "EXITED",
            "STOPPED",
            "SUCCESS",
            "SKIPPED",
            "SKIPPED_NON_TRADING_DAY",
        } or state.startswith("SKIPPED_")

    @staticmethod
    def _canonical_heartbeat_service(service: str) -> str:
        """Return a stable logical service for equivalent producer generations."""

        suffix = "-supervisor" if service.lower().endswith("-supervisor") else ""
        base = service[: -len(suffix)] if suffix else service
        canonical = _HEARTBEAT_SERVICE_EQUIVALENTS.get(base.lower(), base)
        return f"{canonical}{suffix}"[:96]

    def _calendar_payload(self) -> dict[str, object]:
        return self._read_artifact(self.observability_root / "market_calendar.json")

    def _is_trading_date(self, value: date) -> bool:
        calendar = self._calendar_payload()
        iso = value.isoformat()
        open_dates = (
            {str(item) for item in calendar.get("open_dates", [])}
            if isinstance(calendar.get("open_dates"), list)
            else set()
        )
        closed = calendar.get("closed_dates", calendar.get("holidays", []))
        closed_dates = {str(item) for item in closed} if isinstance(closed, list) else set()
        if iso in open_dates:
            return True
        if value.weekday() >= 5 or iso in closed_dates:
            return False
        holiday_paths = (
            self.runtime_root / "nse_holidays.csv",
            self.runtime_root / "holidays.csv",
            Path(__file__).resolve().parents[2] / "nse_holidays.csv",
        )
        for holidays_path in holiday_paths:
            try:
                if not holidays_path.is_file() or holidays_path.stat().st_size > 256 * 1024:
                    continue
                with holidays_path.open(encoding="utf-8-sig", newline="") as handle:
                    for row in csv.reader(handle):
                        if row and row[0].strip() == iso:
                            return False
            except (OSError, UnicodeError, csv.Error):
                continue
        return True

    def _maintenance_active(self, now: datetime) -> bool:
        if str(os.environ.get("EQIDV2_OBS_MAINTENANCE", "")).strip().lower() in {
            "1", "true", "yes", "on", "active"
        }:
            return True
        candidates = (
            self.observability_root / "maintenance.json",
            self.runtime_root / "runtime_status" / "maintenance.status",
        )
        for path in candidates:
            payload = self._read_artifact(path, max_bytes=64 * 1024)
            if not payload:
                continue
            raw = payload.get("active", payload.get("maintenance", payload.get("state")))
            active = raw is True or str(raw).strip().lower() in {"1", "true", "active", "on"}
            if not active:
                continue
            starts = self._parse_datetime(payload.get("starts_at") or payload.get("start_ist"))
            ends = self._parse_datetime(payload.get("ends_at") or payload.get("end_ist"))
            if starts is not None and now < starts.astimezone(IST):
                continue
            if ends is not None and now > ends.astimezone(IST):
                continue
            return True
        return False

    def _collect_schedule(self, samples: _Samples, now: datetime) -> None:
        calendar = self._calendar_payload()
        session = None
        sessions = calendar.get("sessions")
        if isinstance(sessions, dict):
            session = sessions.get(now.date().isoformat())
        start, end = day_time(9, 15), day_time(15, 30)
        if isinstance(session, dict):
            try:
                start = day_time.fromisoformat(str(session.get("open", "09:15")))
                end = day_time.fromisoformat(str(session.get("close", "15:30")))
            except ValueError:
                start, end = day_time(9, 15), day_time(15, 30)
        opened = (
            self._is_trading_date(now.date())
            and start <= now.time().replace(tzinfo=None) <= end
        )
        samples.add("trading_market_open", 1 if opened else 0)
        samples.add("trading_maintenance_window", 1 if self._maintenance_active(now) else 0)

    @staticmethod
    def _direct_files(root: Path, suffixes: Iterable[str], *, limit: int) -> list[Path]:
        if limit <= 0:
            return []
        try:
            suffix_set = tuple(suffixes)
            rows: list[tuple[float, str, Path]] = []
            for path in root.iterdir():
                try:
                    if path.is_file() and path.name.endswith(suffix_set):
                        rows.append((path.stat().st_mtime, str(path), path))
                except OSError:
                    continue
            rows.sort(reverse=True)
            return [row[2] for row in rows[:limit]]
        except OSError:
            return []

    @staticmethod
    def _bounded_recursive_files(
        root: Path, name: str, *, limit: int, max_depth: int = 8
    ) -> list[Path]:
        if limit <= 0 or not root.is_dir():
            return []
        # Keep memory bounded while still considering the whole bounded-depth
        # tree.  Returning the first directory-order matches can permanently
        # hide a newly written marker once an old directory fills the limit.
        newest: list[tuple[float, str, Path]] = []
        base_depth = len(root.parts)
        try:
            for current, directories, files in os.walk(root):
                if len(Path(current).parts) - base_depth >= max_depth:
                    directories[:] = []
                for filename in files:
                    if filename == name or (name.startswith("*") and filename.endswith(name[1:])):
                        path = Path(current) / filename
                        try:
                            candidate = (path.stat().st_mtime, str(path), path)
                        except OSError:
                            continue
                        if len(newest) < limit:
                            heapq.heappush(newest, candidate)
                        elif candidate[:2] > newest[0][:2]:
                            heapq.heapreplace(newest, candidate)
        except OSError:
            pass
        return [item[2] for item in sorted(newest, reverse=True)]

    def _collect_heartbeats(self, samples: _Samples, now: datetime) -> None:
        canonical = self.runtime_root / "fno_oi" / "v13_v10_g_live" / "live_kite" / "heartbeat.json"
        paths = [canonical] if canonical.is_file() else []
        paths.extend(
            self._direct_files(
                self.runtime_root / "runtime_status", (".heartbeat",), limit=128
            )
        )
        newest: dict[tuple[str, str], float] = {}
        for path in paths:
            payload = self._read_artifact(path, max_bytes=256 * 1024)
            if not payload:
                continue
            service = self._label(
                payload.get("service") or payload.get("session_id") or payload.get("session")
                or payload.get("name") or path.stem
            )
            if path.parent.name == "runtime_status" and "restart_count" in payload:
                service = f"{service}-supervisor"[:96]
            mode = self._mode(payload, service)
            service = self._canonical_heartbeat_service(service)
            stamp = self._artifact_timestamp(payload, path)
            if stamp is None:
                continue
            # Runtime-status files are durable history. Export only today's
            # non-terminal workers so yesterday's retired V5/V6/V7 services do
            # not become false stale-heartbeat alerts on the next open day.
            if stamp.astimezone(IST).date() != now.date():
                continue
            if self._is_terminal(payload):
                continue
            age = self._age_seconds(now, stamp)
            if age is None:
                continue
            key = (service, mode)
            newest[key] = min(age, newest.get(key, age))
        for (service, mode), age in newest.items():
            samples.add(
                "trading_heartbeat_age_seconds", age,
                {"service": service, "mode": mode},
            )

    def _collect_pipeline_schedule(self, samples: _Samples, now: datetime) -> None:
        """Expose missed frozen scanner slots, including a missing heartbeat.

        Heartbeat age alone cannot model this scanner: a completed 10:00 slot
        remains valid until the frozen 11:20 slot is due.  Conversely, generic
        heartbeat aggregation can hide a missing scanner behind another fresh
        V13 worker.  This binary gauge evaluates the latest due slot directly
        and therefore emits a failure even when scanner evidence is absent.
        """

        labels = {"pipeline": "v13_v10_g_scanner", "mode": "live"}
        slot_datetimes = [
            datetime.combine(
                now.date(), day_time.fromisoformat(value), tzinfo=IST
            )
            for value in _V13_SCANNER_SLOTS
        ]
        due = [slot for slot in slot_datetimes if slot <= now]
        if not self._is_trading_date(now.date()) or not due:
            samples.add("trading_pipeline_schedule_overdue", 0, labels)
            return

        latest_due = due[-1]
        grace_deadline = latest_due + timedelta(
            seconds=_V13_SCANNER_COMPLETION_GRACE_SECONDS
        )
        if now <= grace_deadline:
            samples.add("trading_pipeline_schedule_overdue", 0, labels)
            return

        heartbeat = self._read_artifact(
            self.runtime_root
            / "runtime_status"
            / f"{_V13_SCANNER_SESSION}.heartbeat",
            max_bytes=256 * 1024,
        )
        stamp = self._semantic_timestamp(heartbeat) if heartbeat else None
        stamp_is_current = (
            stamp is not None
            and stamp.astimezone(IST).date() == now.date()
            and self._age_seconds(now, stamp) is not None
        )
        state = str(heartbeat.get("state") or heartbeat.get("status") or "").upper()
        phase = str(heartbeat.get("phase", ""))
        latest_label = latest_due.strftime("%H:%M")
        completed_label = str(
            heartbeat.get("last_completed_slot") or heartbeat.get("slot") or ""
        )
        completed = (
            stamp_is_current
            and state in {"RUNNING", "WAITING", "SUCCESS"}
            and completed_label == latest_label
            and phase in {"SLOT_DONE", "WAIT_FETCH", "WAIT_NEXT_SLOT"}
        )
        if latest_label == _V13_SCANNER_SLOTS[-1] and stamp_is_current:
            try:
                processed_slots = int(str(heartbeat.get("processed_slots", "")))
            except ValueError:
                processed_slots = -1
            completed = completed or (
                state == "DONE"
                and phase == "ALL_V6_WINDOWS_DONE"
                and processed_slots == len(_V13_SCANNER_SLOTS)
            )
        samples.add(
            "trading_pipeline_schedule_overdue", 0 if completed else 1, labels
        )

    def _latest_marker(
        self,
        root: Path,
        *,
        recursive: bool = False,
        not_after: datetime | None = None,
    ) -> tuple[Path, dict[str, object]] | None:
        if recursive:
            paths = self._bounded_recursive_files(root, "*.json", limit=512)
        else:
            paths = self._direct_files(root, (".json",), limit=96)
        best: tuple[datetime, Path, dict[str, object]] | None = None
        for path in paths:
            payload = self._read_artifact(path)
            stamp = self._parse_datetime(payload.get("slot_ist")) if payload else None
            if stamp is None:
                continue
            if not_after is not None and self._age_seconds(not_after, stamp) is None:
                continue
            if best is None or stamp > best[0]:
                best = stamp, path, payload
        return (best[1], best[2]) if best else None

    @staticmethod
    def _ratio(
        payload: Mapping[str, object],
        fields: Iterable[str],
        numerator: str,
        denominator: str,
    ) -> float | None:
        for field in fields:
            try:
                value = float(payload[field])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(value) and 0.0 <= value <= 1.0:
                return value
        try:
            top, bottom = float(payload[numerator]), float(payload[denominator])
            if math.isfinite(top) and math.isfinite(bottom) and bottom > 0:
                return max(0.0, min(1.0, top / bottom))
        except (KeyError, TypeError, ValueError):
            pass
        return None

    def _marker_samples(
        self,
        samples: _Samples,
        now: datetime,
        marker: tuple[Path, dict[str, object]] | None,
        *,
        source: str,
        timeframe: str,
        ratio_fields: Iterable[str],
        numerator: str,
        denominator: str,
    ) -> None:
        if marker is None:
            return
        _, payload = marker
        stamp = self._parse_datetime(payload.get("slot_ist"))
        if stamp is None:
            return
        age = self._age_seconds(now, stamp)
        if age is None:
            return
        labels = {"source": source, "timeframe": timeframe}
        samples.add("trading_data_age_seconds", age, labels)
        coverage = self._ratio(payload, ratio_fields, numerator, denominator)
        if coverage is not None:
            samples.add("trading_data_coverage_ratio", coverage, labels)

    def _collect_final_equity_slot_marker(self, samples: _Samples, now: datetime) -> None:
        """Report only the newest *final* completed-candle cash marker today.

        The 09:15 opening snapshot is intentionally not a completed 5m bar.
        Sampled watcher markers contain no authoritative ``complete`` field,
        so neither may be interpreted as an incomplete trading slot.
        """

        latest: tuple[datetime, bool] | None = None
        marker_dir = self.runtime_root / "slot_ready_5m"
        for path in self._direct_files(marker_dir, (".json",), limit=96):
            if re.fullmatch(r"slot_\d{8}_\d{4}\.json", path.name) is None:
                continue
            payload = self._read_artifact(path)
            complete = payload.get("complete")
            if payload.get("source") != "final" or type(complete) is not bool:
                continue
            stamp = self._parse_datetime(payload.get("slot_ist"))
            published = self._parse_datetime(payload.get("published_at_ist"))
            if stamp is None or published is None:
                continue
            slot = stamp.astimezone(IST)
            published = published.astimezone(IST)
            if (
                slot.date() != now.date()
                or published.date() != now.date()
                or path.name != f"slot_{slot.strftime('%Y%m%d_%H%M')}.json"
                or not day_time(9, 20) <= slot.time().replace(tzinfo=None) <= day_time(15, 30)
                or slot.minute % 5 != 0
                or slot.second != 0
                or published < slot
                or self._age_seconds(now, published) is None
            ):
                continue
            if latest is None or slot > latest[0]:
                latest = slot, complete
        if latest is not None:
            slot, complete = latest
            samples.add(
                "trading_data_slot_incomplete",
                0 if complete else 1,
                {"source": "equity", "timeframe": "5m", "slot": slot.strftime("%H:%M")},
            )

    def _collect_data_markers(self, samples: _Samples, now: datetime) -> None:
        self._collect_final_equity_slot_marker(samples, now)
        self._marker_samples(
            samples,
            now,
            self._latest_marker(
                self.runtime_root / "fno_oi" / "slot_ready", not_after=now
            ),
            source="futures_oi",
            timeframe="5m",
            ratio_fields=("stock_coverage_ratio", "coverage_ratio"),
            numerator="stock_contracts_written",
            denominator="stock_contracts_expected",
        )
        self._marker_samples(
            samples,
            now,
            self._latest_marker(self.runtime_root / "slot_ready_5m", not_after=now),
            source="equity",
            timeframe="5m",
            # ``fresh_ratio`` in this legacy marker is a small verification
            # sample, not full-universe coverage.  Prefer the authoritative
            # completed/expected counts.
            ratio_fields=("coverage_ratio",),
            numerator="tickers_complete",
            denominator="tickers_expected",
        )
        self._marker_samples(
            samples,
            now,
            self._latest_marker(
                self.runtime_root / "fno_oi" / "equity_1m_slot_ready",
                recursive=True,
                not_after=now,
            ),
            source="equity_confirmation",
            timeframe="1m",
            ratio_fields=("coverage_ratio",),
            numerator="resolved_count",
            denominator="candidate_count",
        )

    def _collect_disk(self, samples: _Samples, now: datetime) -> None:
        del now
        volumes = [("runtime", self.runtime_root)]
        try:
            same_volume = self.runtime_root.anchor.lower() == self.observability_root.anchor.lower()
        except AttributeError:
            same_volume = True
        if not same_volume:
            volumes.append(("observability", self.observability_root))
        for label, path in volumes:
            try:
                free = shutil.disk_usage(path if path.exists() else path.parent).free
            except OSError:
                continue
            samples.add(
                "trading_disk_free_bytes", free,
                {"service": "ai_platform_api", "volume": label},
            )

    def _replay_candidates(self) -> list[Path]:
        root = self.runtime_root / "backtesting_result_v13_v10_g"
        latest = root / "latest" / "latest_backtesting_result_v13_v10_g.json"
        rows = [latest] if latest.is_file() else []
        rows.extend(self._bounded_recursive_files(root / "runs", "replay_result.json", limit=128))
        return rows

    @staticmethod
    def _success_payload(payload: Mapping[str, object]) -> Mapping[str, object] | None:
        nested = payload.get("result")
        result = nested if isinstance(nested, dict) else payload
        status = str(payload.get("status") or result.get("state") or "").upper()
        if status != "SUCCESS" or result.get("complete", True) is not True:
            return None
        return result

    def _required_replay_date(self, now: datetime) -> date:
        candidate = now.date()
        if now.time().replace(tzinfo=None) < day_time(16, 0):
            candidate -= timedelta(days=1)
        while not self._is_trading_date(candidate):
            candidate -= timedelta(days=1)
        return candidate

    def _collect_replay(self, samples: _Samples, now: datetime) -> None:
        newest: tuple[date, float] | None = None
        for path in self._replay_candidates():
            payload = self._read_artifact(path, max_bytes=2 * 1024 * 1024)
            result = self._success_payload(payload)
            if result is None:
                continue
            try:
                session_day = date.fromisoformat(
                    str(result.get("session_date") or payload.get("session_date"))[:10]
                )
            except ValueError:
                continue
            if session_day > now.date():
                continue
            stamp = None
            for candidate in (
                payload.get("updated_at_ist"),
                result.get("generated_at_ist"),
                result.get("completed_at_ist"),
            ):
                stamp = self._parse_datetime(candidate)
                if stamp is not None:
                    break
            if stamp is None:
                try:
                    epoch = path.stat().st_mtime
                except OSError:
                    continue
            else:
                epoch = stamp.timestamp()
            if epoch > now.timestamp() + _MAX_FUTURE_SKEW_SECONDS:
                continue
            if newest is None or (session_day, epoch) > newest:
                newest = session_day, epoch
        labels = {"profile": "v13-v10-g", "replay_kind": "daily"}
        required = self._required_replay_date(now)
        due = newest is None or newest[0] < required
        samples.add("trading_replay_due", 1 if due else 0, labels)
        if newest is not None:
            samples.add("trading_replay_success_timestamp_seconds", newest[1], labels)

    def _expected_fingerprint(self) -> str:
        if self.profile_registry_path is None:
            return ""
        payload = self._read_artifact(self.profile_registry_path, max_bytes=2 * 1024 * 1024)
        profiles = payload.get("profiles")
        if not isinstance(profiles, dict):
            return ""
        profile = profiles.get("V13_V10_G")
        return (
            str(profile.get("strategy_fingerprint", "")).strip()
            if isinstance(profile, dict)
            else ""
        )

    def _collect_fingerprint(self, samples: _Samples, now: datetime) -> None:
        expected = self._expected_fingerprint()
        if not expected:
            return
        root = self.runtime_root / "fno_oi" / "v13_v10_g_live"
        active_paths = (
            root / "live_kite" / "status.json",
            root / "live_kite" / "heartbeat.json",
        )
        default_service = "fno_v13_v10_g_live_kite_qty1"
        active: list[tuple[datetime, dict[str, object], str]] = []
        for path in active_paths:
            payload = self._read_artifact(path)
            if not payload:
                continue
            service = self._label(
                payload.get("session_id")
                or payload.get("service")
                or default_service
            )
            explicit_mode = payload.get("execution_mode") or payload.get("mode")
            if explicit_mode and self._label(explicit_mode).lower() != "live":
                continue
            state = str(payload.get("state") or payload.get("status") or "").upper()
            if (
                self._is_terminal(payload)
                or state in {"ERROR", "FAILED"}
                or state.startswith("BLOCKED")
            ):
                continue
            stamp = self._semantic_timestamp(payload)
            if stamp is None or stamp.astimezone(IST).date() != now.date():
                continue
            age = self._age_seconds(now, stamp)
            if age is None or age > 120:
                continue
            active.append((stamp, payload, service))
        if not active:
            return
        active.sort(key=lambda item: item[0], reverse=True)
        service = active[0][2]
        observed = [
            value
            for _, payload, _ in active
            if (value := str(payload.get("strategy_fingerprint", "")).strip())
        ]
        # A static deployment manifest can prove a conflict, but it cannot
        # prove what a running process actually loaded.  Healthy zero requires
        # at least one fresh runtime artifact carrying its own fingerprint.
        if not observed:
            return
        manifest = self._read_artifact(root / "strategy_manifest.json")
        manifest_fingerprint = str(manifest.get("strategy_fingerprint", "")).strip()
        if manifest_fingerprint:
            observed.append(manifest_fingerprint)
        samples.add(
            "trading_strategy_fingerprint_mismatch",
            1 if any(value != expected for value in observed) else 0,
            {"service": service, "strategy": "v13_v10_g", "mode": "live"},
        )

    @staticmethod
    def _finite_field(payload: Mapping[str, object], names: Iterable[str]) -> float | None:
        for name in names:
            try:
                value = float(payload[name])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(value):
                return value
        return None

    @staticmethod
    def _active_order_reconciliation_count(
        payload: Mapping[str, object],
    ) -> float | None:
        """Validate the v2 active-order contract before exporting a gauge."""

        count = payload.get("active_order_mismatch_count")
        rows = payload.get("active_order_mismatches")
        complete = payload.get("active_order_parity_complete")
        if (
            type(count) is not int
            or count < 0
            or not isinstance(rows, list)
            or len(rows) != count
            or complete is not (count == 0)
        ):
            return None
        if count > 0:
            return float(count)

        tagged_count = payload.get("tagged_order_count")
        local_count = payload.get("local_expected_active_order_count")
        broker_count = payload.get("broker_active_tagged_order_count")
        local_ids = payload.get("local_expected_active_order_ids")
        broker_ids = payload.get("broker_active_tagged_order_ids")
        if not (
            type(tagged_count) is int
            and type(local_count) is int
            and type(broker_count) is int
            and tagged_count >= broker_count >= 0
            and local_count >= 0
            and isinstance(local_ids, list)
            and isinstance(broker_ids, list)
            and all(isinstance(value, str) and value for value in local_ids)
            and all(isinstance(value, str) and value for value in broker_ids)
            and local_ids == sorted(set(local_ids))
            and broker_ids == sorted(set(broker_ids))
            and len(local_ids) == local_count
            and len(broker_ids) == broker_count
            and local_ids == broker_ids
        ):
            return None
        return 0.0

    def _collect_supervisors(self, samples: _Samples, now: datetime) -> None:
        del now
        paths = self._direct_files(
            self.runtime_root / "runtime_status", (".heartbeat", ".status", ".spawn"), limit=256
        )
        records: dict[str, tuple[float, dict[str, object]]] = {}
        offsets: dict[str, tuple[float, float]] = {}
        for path in paths:
            payload = self._read_artifact(path, max_bytes=256 * 1024)
            if not payload:
                continue
            service = self._label(payload.get("name") or payload.get("session") or path.stem)
            try:
                modified = path.stat().st_mtime
            except OSError:
                continue
            if "restart_count" in payload and (
                service not in records or modified > records[service][0]
            ):
                records[service] = modified, payload
            offset = self._finite_field(
                payload,
                ("clock_offset_seconds", "ntp_offset_seconds", "clock_offset", "ntp_offset"),
            )
            if offset is not None and (service not in offsets or modified > offsets[service][0]):
                offsets[service] = modified, offset
        for service, (_, payload) in records.items():
            restarts = self._finite_field(payload, ("restart_count",))
            mode = self._mode(payload, service)
            strategy = "v13_v10_g" if "v13_v10_g" in service.lower() else "unknown"
            if restarts is not None and restarts >= 0:
                samples.add(
                    "trading_process_restarts_total", restarts,
                    {"service": service, "strategy": strategy, "mode": mode},
                )
        for service, (_, offset) in offsets.items():
            samples.add("trading_clock_offset_seconds", offset, {"service": service})

    def _collect_local_protection(self, samples: _Samples, now: datetime) -> None:
        root = self.runtime_root / "fno_oi" / "v13_v10_g_live" / "orders" / "LIVE"
        paths = self._bounded_recursive_files(root, "*.json", limit=2048, max_depth=5)
        if not paths:
            return
        valid = 0
        invalid = 0
        unprotected_ages: list[float] = []
        for path in paths:
            payload = self._read_artifact(path)
            if not payload:
                invalid += 1
                continue
            if str(payload.get("mode", "LIVE")).upper() != "LIVE":
                continue
            try:
                session_day = date.fromisoformat(str(payload.get("session_date", ""))[:10])
            except ValueError:
                invalid += 1
                continue
            if session_day != now.date():
                continue
            valid += 1
            status = str(payload.get("status", "")).upper()
            if status not in {"OPEN", "SQUARE_OFF_PENDING"}:
                continue
            # A square-off-pending state has already cancelled its stop before
            # submitting the market exit, so it remains locally unprotected
            # until that exit fill is persisted.
            confirmed_at = self._parse_datetime(
                payload.get("stop_order_status_observed_at_ist")
                or payload.get("protection_confirmed_at_ist")
            )
            confirmation_age = (
                (now.astimezone(timezone.utc) - confirmed_at.astimezone(timezone.utc)).total_seconds()
                if confirmed_at is not None
                else float("inf")
            )
            locally_protected = (
                status == "OPEN"
                and payload.get("protection_confirmed") is True
                and 0 <= confirmation_age <= 120
            )
            if locally_protected:
                continue
            stamp = confirmed_at if confirmed_at is not None and confirmation_age > 120 else None
            stamp_fields = (
                ("updated_at_ist", "entry_at_ist", "entry_time", "created_at_ist")
                if status == "SQUARE_OFF_PENDING"
                else ("entry_at_ist", "entry_time", "updated_at_ist", "created_at_ist")
            )
            for key in stamp_fields:
                stamp = self._parse_datetime(payload.get(key))
                if stamp is not None:
                    break
            if stamp is None:
                invalid += 1
                continue
            age = self._age_seconds(now, stamp)
            if age is None:
                invalid += 1
                continue
            unprotected_ages.append(age)
        if unprotected_ages:
            samples.add(
                "trading_unprotected_position_seconds", max(unprotected_ages),
                {"mode": "live", "asset": "equity"},
            )
        elif valid and not invalid:
            # This is explicitly *local order-state* evidence only.  Broker
            # reconciliation is intentionally a separate, absent-by-default
            # metric below.
            samples.add(
                "trading_unprotected_position_seconds", 0,
                {"mode": "live", "asset": "equity"},
            )

    @staticmethod
    def _normalized_mismatch(value: object, default: str) -> str:
        if value is None or not str(value).strip():
            return default
        text = re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")
        return (text or default)[:96]

    @staticmethod
    def _verified_reconciliation_digest(payload: Mapping[str, object]) -> str | None:
        claimed = payload.get("report_sha256")
        if not isinstance(claimed, str) or not re.fullmatch(r"[0-9a-f]{64}", claimed):
            return None
        unsigned = {key: value for key, value in payload.items() if key != "report_sha256"}
        try:
            encoded = json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        except (TypeError, ValueError, OverflowError):
            return None
        observed = hashlib.sha256(encoded).hexdigest()
        return claimed if observed == claimed else None

    def _collect_reconciliation(self, samples: _Samples, now: datetime) -> None:
        root = self.observability_root / "reconciliation"
        paths = self._direct_files(root, (".json",), limit=400)
        latest_broker: tuple[float, float, float] | None = None
        for path in paths:
            payload = self._read_artifact(path, max_bytes=2 * 1024 * 1024)
            if not payload:
                continue
            mismatch_counts: dict[tuple[str, str], int] = defaultdict(int)
            feature_counts: dict[str, int] = defaultdict(int)
            report_digest = self._verified_reconciliation_digest(payload)
            if report_digest is not None:
                comparisons = payload.get("comparisons")
                is_parity_report = isinstance(comparisons, dict) or str(
                    payload.get("state", "")
                ).upper() == "MISMATCH"
                if isinstance(comparisons, dict):
                    for comparison_name, raw in comparisons.items():
                        if (
                            not isinstance(raw, dict)
                            or str(raw.get("state", "")).upper() != "MISMATCH"
                        ):
                            continue
                        stage = self._normalized_mismatch(
                            raw.get("first_divergence_stage"), "unknown"
                        )
                        mismatch_type = (
                            "expected_data_revision"
                            if str(comparison_name) == "observed_vs_finalized"
                            else self._normalized_mismatch(
                                raw.get("classification"), "live_system_mismatch"
                            )
                        )
                        mismatch_counts[(stage, mismatch_type)] += 1
                        stages = raw.get("stages")
                        if isinstance(stages, list):
                            for stage_row in stages:
                                if (
                                    not isinstance(stage_row, dict)
                                    or str(stage_row.get("stage")) != "feature"
                                ):
                                    continue
                                differences = stage_row.get("differences")
                                if isinstance(differences, list):
                                    for difference in differences[:256]:
                                        if isinstance(difference, dict):
                                            feature = self._normalized_mismatch(
                                                difference.get("field"), "unknown"
                                            )
                                            feature_counts[feature] += 1
                elif str(payload.get("state", "")).upper() == "MISMATCH":
                    stage = self._normalized_mismatch(
                        payload.get("first_divergence_stage"), "unknown"
                    )
                    kind = self._normalized_mismatch(
                        payload.get("mismatch_type"), "unclassified"
                    )
                    mismatch_counts[(stage, kind)] += 1

                if (
                    is_parity_report
                    and report_digest not in self._reconciliation_seen
                ):
                    if (
                        len(self._reconciliation_seen_order)
                        >= _MAX_RECONCILIATION_ARTIFACTS
                    ):
                        expired = self._reconciliation_seen_order.popleft()
                        self._reconciliation_seen.discard(expired)
                    self._reconciliation_seen.add(report_digest)
                    self._reconciliation_seen_order.append(report_digest)
                    for key, count in mismatch_counts.items():
                        if (
                            key in self._reconciliation_mismatch_totals
                            or len(self._reconciliation_mismatch_totals)
                            < _MAX_SERIES_PER_METRIC
                        ):
                            self._reconciliation_mismatch_totals[key] = (
                                self._reconciliation_mismatch_totals.get(key, 0)
                                + count
                            )
                    for feature, count in feature_counts.items():
                        if (
                            feature in self._reconciliation_feature_totals
                            or len(self._reconciliation_feature_totals)
                            < _MAX_SERIES_PER_METRIC
                        ):
                            self._reconciliation_feature_totals[feature] = (
                                self._reconciliation_feature_totals.get(feature, 0)
                                + count
                            )

            # Never infer broker/local agreement from stage parity.  Export
            # this gauge only when a report explicitly attests broker truth.
            broker = payload.get("position_reconciliation") or payload.get(
                "broker_position_reconciliation"
            )
            if (
                report_digest is not None
                and payload.get("schema_version")
                == "v13_v10_g_broker_position_reconciliation_v2"
                and isinstance(broker, dict)
                and broker.get("broker_truth_available") is True
            ):
                try:
                    broker_session = date.fromisoformat(
                        str(payload.get("session_date", ""))[:10]
                    )
                except ValueError:
                    continue
                if broker_session != now.date():
                    continue
                observed_at = None
                for candidate in (
                    broker.get("observed_at_ist"),
                    broker.get("generated_at_ist"),
                    payload.get("observed_at_ist"),
                    payload.get("generated_at_ist"),
                ):
                    observed_at = self._parse_datetime(candidate)
                    if observed_at is not None:
                        break
                if observed_at is None:
                    continue
                broker_age = self._age_seconds(now, observed_at)
                if broker_age is None or broker_age > 120:
                    continue
                count = self._finite_field(
                    broker, ("mismatch_count", "position_mismatch_count", "mismatches")
                )
                active_order_count = self._active_order_reconciliation_count(broker)
                scope_complete = broker.get("scope_complete") is True
                # A non-zero mismatch is actionable even for a scoped view.
                # Healthy zeroes are exported only when the producer proves
                # the corresponding broker scope/parity contract complete;
                # absence must never masquerade as agreement.
                position_valid = count is not None and count >= 0 and (
                    count > 0 or scope_complete
                )
                active_order_valid = active_order_count is not None
                if position_valid and active_order_valid:
                    observed_epoch = observed_at.timestamp()
                    if latest_broker is None or observed_epoch > latest_broker[0]:
                        latest_broker = (
                            observed_epoch,
                            count,
                            active_order_count,
                        )
        for (stage, kind), count in self._reconciliation_mismatch_totals.items():
            samples.add(
                "trading_live_eod_mismatch_total", count,
                {"stage": stage, "mismatch_type": kind},
            )
        for feature, count in self._reconciliation_feature_totals.items():
            samples.add(
                "trading_feature_parity_mismatch_total", count,
                {"strategy": "v13_v10_g", "feature": feature},
            )
        if latest_broker is not None:
            samples.add(
                "trading_position_reconciliation_mismatch", latest_broker[1],
                {"mode": "live", "asset": "equity"},
            )
            samples.add(
                "trading_active_order_reconciliation_mismatch", latest_broker[2],
                {"mode": "live", "asset": "equity"},
            )

    def _collect_textfiles(self, samples: _Samples, now: datetime) -> None:
        roots = [self.runtime_root / "observability" / "metrics"]
        configured = self.observability_root / "metrics"
        if configured.resolve() != roots[0].resolve():
            roots.append(configured)
        path_roots: list[tuple[Path, Path]] = []
        for root in roots:
            path_roots.extend(
                (path, root)
                for path in self._direct_files(root, (".prom",), limit=32)
            )
        def sort_mtime(item: tuple[Path, Path]) -> float:
            try:
                return item[0].stat().st_mtime
            except OSError:
                return float("-inf")

        path_roots.sort(key=sort_mtime, reverse=True)
        now_epoch = now.timestamp()
        for path, root in path_roots[:32]:
            try:
                resolved_root = root.resolve()
                resolved = path.resolve()
                resolved.relative_to(resolved_root)
                stat = resolved.stat()
                if stat.st_size < 1 or stat.st_size > 256 * 1024:
                    continue
                age = now_epoch - stat.st_mtime
                if age > self.textfile_max_age_seconds or age < -_MAX_FUTURE_SKEW_SECONDS:
                    continue
                text = resolved.read_text(encoding="utf-8", errors="strict")
            except (OSError, UnicodeError, ValueError):
                continue
            seen_in_producer: set[
                tuple[str, tuple[tuple[str, str], ...]]
            ] = set()
            for line in text.splitlines()[:4096]:
                # HELP/TYPE and malformed/non-trading samples are discarded.
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                samples.add_line(
                    stripped,
                    observed_at=stat.st_mtime,
                    oldest_allowed=now_epoch - self.textfile_max_age_seconds,
                    newest_allowed=now_epoch + _MAX_FUTURE_SKEW_SECONDS,
                    seen_in_producer=seen_in_producer,
                )


__all__ = ["RuntimeMetricsCollector", "merge_prometheus_samples"]
