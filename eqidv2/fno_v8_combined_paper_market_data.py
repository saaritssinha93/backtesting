"""Eight-app completed one-minute market-data seam for V8-Combined PAPER.

This module is deliberately strategy-light.  It authenticates the exact local
Kite app1..app8 credential roster, assigns an immutable candidate set to those
apps deterministically, and fetches only an already-completed NSE equity
one-minute candle.  It never polls LTP, invents a candle, forwards a previous
bar, or places an order.

The session orchestrator persists the returned bars and audit payload in the
V8-only evidence tree.  Network-facing functions accept injectable credential
and client factories so the full contract can be tested without using a live
broker session.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

import fno_oi_common as common


MARKET_DATA_POLICY_VERSION = "fno_v8_combined_exact_completed_1m_resilient_pool_v2"
MINUTE_SNAPSHOT_SCHEMA_VERSION = "fno_v8_combined_minute_snapshot_v2"
REQUIRED_APP_COUNT = 8
EXPECTED_APP_NAMES = tuple(f"app{index}" for index in range(1, 9))
DEFAULT_MINIMUM_HEALTHY_APPS = 7
DEFAULT_AUTHENTICATION_ATTEMPTS = 3
DEFAULT_AUTHENTICATION_RETRY_SPACING_SEC = 1.0
DEFAULT_BOUNDARY_BUFFER_SEC = 3.0
DEFAULT_REQUEST_INTERVAL_SEC = 0.36
DEFAULT_TIMEOUT_SEC = 8.0
DEFAULT_OBSERVATIONS = 3
DEFAULT_OBSERVATION_SPACING_SEC = 2.0
DEFAULT_CIRCUIT_BREAKER_FAILURES = 2
DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SEC = 2.0
MARKER_PAYLOAD_SHA256_FIELD = "marker_payload_sha256"


def _to_ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def _clean_error(exc: BaseException) -> str:
    return " ".join(f"{type(exc).__name__}: {exc}".split())[:500]


class MarketDataDeadlineExceeded(TimeoutError):
    """A broker call could not start before its prospective deadline."""


@dataclass(frozen=True)
class CandidateRequest:
    symbol: str
    instrument_token: int

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").strip().upper()
        try:
            token = int(self.instrument_token)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Candidate request requires symbol and positive token"
            ) from exc
        if not symbol or token <= 0:
            raise ValueError("Candidate request requires symbol and positive token")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "instrument_token", token)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CandidateRequest":
        symbol = str(
            value.get("symbol", value.get("tradingsymbol", ""))
        ).strip().upper()
        token = int(value.get("instrument_token", 0) or 0)
        if not symbol or token <= 0:
            raise ValueError("Candidate request requires symbol and positive token")
        return cls(symbol=symbol, instrument_token=token)


@dataclass
class AppRuntime:
    app_name: str
    client: Any
    pace_seconds: float = DEFAULT_REQUEST_INTERVAL_SEC
    auth_attempts: int = 1
    auth_observations: tuple[Mapping[str, Any], ...] = ()
    pool_authentication_audit: tuple[Mapping[str, Any], ...] = ()
    _last_call_at: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _historical_call_lock: threading.Lock = field(default_factory=threading.Lock)
    _health_lock: threading.Lock = field(default_factory=threading.Lock)
    _consecutive_market_data_failures: int = 0
    _circuit_open_until: float = 0.0
    _circuit_opened_total: int = 0

    def pace(self) -> None:
        with self._lock:
            delay = float(self.pace_seconds) - (
                time.monotonic() - float(self._last_call_at)
            )
            if delay > 0:
                time.sleep(delay)
            self._last_call_at = time.monotonic()

    @staticmethod
    def _resolve_monotonic_deadline(
        *,
        deadline_at: datetime | pd.Timestamp | None,
        monotonic_deadline: float | None,
    ) -> float | None:
        deadlines: list[float] = []
        if monotonic_deadline is not None:
            deadlines.append(float(monotonic_deadline))
        if deadline_at is not None:
            remaining = (
                _to_ist(deadline_at) - _to_ist(common.now_ist())
            ).total_seconds()
            deadlines.append(time.monotonic() + max(0.0, float(remaining)))
        return min(deadlines) if deadlines else None

    def call_historical_data(
        self,
        *args: Any,
        deadline_at: datetime | pd.Timestamp | None = None,
        monotonic_deadline: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Serialize pace+HTTP for this client and honor the start deadline.

        Python cannot forcibly cancel an in-flight provider request.  Holding
        this lock through the entire call ensures that a logically timed-out
        request can never overlap a later request on the same Kite client.
        """

        deadline = self._resolve_monotonic_deadline(
            deadline_at=deadline_at,
            monotonic_deadline=monotonic_deadline,
        )
        if deadline is None:
            acquired = self._historical_call_lock.acquire()
        else:
            remaining = deadline - time.monotonic()
            acquired = remaining > 0 and self._historical_call_lock.acquire(
                timeout=remaining
            )
        if not acquired:
            raise MarketDataDeadlineExceeded(
                f"{self.app_name} historical call lock was unavailable before deadline"
            )
        try:
            if deadline is not None and time.monotonic() >= deadline:
                raise MarketDataDeadlineExceeded(
                    f"{self.app_name} historical call reached its deadline before pacing"
                )
            self.pace()
            if deadline is not None and time.monotonic() >= deadline:
                raise MarketDataDeadlineExceeded(
                    f"{self.app_name} historical call reached its deadline before HTTP start"
                )
            return self.client.historical_data(*args, **kwargs)
        finally:
            self._historical_call_lock.release()

    def market_data_health(self) -> dict[str, Any]:
        """Return persistent transport health shared across minute fetches."""

        with self._health_lock:
            return {
                "consecutive_failures": int(
                    self._consecutive_market_data_failures
                ),
                "circuit_open_until": float(self._circuit_open_until),
                "circuit_opened_total": int(self._circuit_opened_total),
            }

    def record_market_data_result(
        self,
        state: str,
        *,
        failure_threshold: int,
        cooldown_sec: float,
    ) -> dict[str, Any]:
        """Update persistent transport health and return the new snapshot."""

        failure_states = {"API_FAILURE", "DEADLINE_EXCEEDED"}
        now_monotonic = time.monotonic()
        with self._health_lock:
            if str(state) in failure_states:
                self._consecutive_market_data_failures += 1
                if self._consecutive_market_data_failures >= int(
                    failure_threshold
                ):
                    self._circuit_opened_total += 1
                    self._circuit_open_until = max(
                        self._circuit_open_until,
                        now_monotonic + max(0.0, float(cooldown_sec)),
                    )
                    self._consecutive_market_data_failures = 0
            else:
                self._consecutive_market_data_failures = 0
                self._circuit_open_until = 0.0
            return {
                "consecutive_failures": int(
                    self._consecutive_market_data_failures
                ),
                "circuit_open_until": float(self._circuit_open_until),
                "circuit_opened_total": int(self._circuit_opened_total),
            }


@dataclass(frozen=True)
class BarOutcome:
    symbol: str
    instrument_token: int
    app_name: str
    state: str
    attempts: int
    bar: Mapping[str, Any] | None = None
    error: str = ""
    observations: tuple[Mapping[str, Any], ...] = ()

    def audit_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("bar", None)
        return payload


def app_roster_payload(runtimes: Sequence[AppRuntime]) -> list[dict[str, Any]]:
    return [
        {"app_name": runtime.app_name, "authenticated": True}
        for runtime in sorted(runtimes, key=lambda item: item.app_name)
    ]


def app_roster_sha256(runtimes: Sequence[AppRuntime]) -> str:
    return common.canonical_json_sha256(app_roster_payload(runtimes))


def marker_payload_sha256(payload: Mapping[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop(MARKER_PAYLOAD_SHA256_FIELD, None)
    return common.canonical_json_sha256(unsigned)


def bind_marker_payload_sha256(payload: Mapping[str, Any]) -> dict[str, Any]:
    bound = dict(payload)
    bound.pop(MARKER_PAYLOAD_SHA256_FIELD, None)
    bound[MARKER_PAYLOAD_SHA256_FIELD] = marker_payload_sha256(bound)
    return bound


def app_authentication_payload(
    runtimes: Sequence[AppRuntime],
) -> list[dict[str, Any]]:
    """Return non-identity authentication evidence for the healthy pool."""

    pool = validate_runtime_pool(runtimes)
    complete_audits = [
        tuple(runtime.pool_authentication_audit)
        for runtime in pool
        if runtime.pool_authentication_audit
    ]
    if complete_audits:
        reference_sha = common.canonical_json_sha256(complete_audits[0])
        if any(
            common.canonical_json_sha256(audit) != reference_sha
            for audit in complete_audits[1:]
        ):
            raise RuntimeError("Healthy app runtimes disagree on authentication audit")
        return [dict(item) for item in complete_audits[0]]
    return [
        {
            "app_name": runtime.app_name,
            "authenticated": True,
            "attempts": int(runtime.auth_attempts),
            "observations": [dict(item) for item in runtime.auth_observations],
        }
        for runtime in pool
    ]


def validate_runtime_pool(
    runtimes: Sequence[AppRuntime],
    *,
    minimum_healthy_apps: int = 1,
) -> tuple[AppRuntime, ...]:
    """Validate an ordered, unique healthy subset of the preferred app roster."""

    minimum = int(minimum_healthy_apps)
    if minimum < 1 or minimum > REQUIRED_APP_COUNT:
        raise ValueError(
            f"minimum_healthy_apps must be between 1 and {REQUIRED_APP_COUNT}"
        )
    pool = tuple(runtimes)
    names = tuple(str(runtime.app_name) for runtime in pool)
    expected_subset = tuple(name for name in EXPECTED_APP_NAMES if name in set(names))
    if (
        len(pool) < minimum
        or len(names) != len(set(names))
        or any(name not in EXPECTED_APP_NAMES for name in names)
        or names != expected_subset
    ):
        raise RuntimeError(
            "Kite runtime pool must be a unique EXPECTED-order subset of "
            f"app1..app8 with at least {minimum} healthy apps; observed={list(names)}"
        )
    return pool


def authenticate_required_apps(
    *,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    request_interval_sec: float = DEFAULT_REQUEST_INTERVAL_SEC,
    authentication_attempts: int = DEFAULT_AUTHENTICATION_ATTEMPTS,
    authentication_retry_spacing_sec: float = DEFAULT_AUTHENTICATION_RETRY_SPACING_SEC,
    minimum_healthy_apps: int = DEFAULT_MINIMUM_HEALTHY_APPS,
    credential_loader: Callable[..., Sequence[Any]] = common.discover_kite_credentials,
    client_factory: Callable[..., Any] = common.make_kite_client,
) -> list[AppRuntime]:
    """Prefer all eight apps, but return an ordered healthy pool when safe.

    All eight credential pairs must be configured.  Each app is authenticated
    independently with bounded retries; one unavailable app does not discard
    seven healthy sessions.  Callers may raise ``minimum_healthy_apps`` to eight
    when a workflow genuinely requires the full preferred roster.
    """

    attempts = int(authentication_attempts)
    if attempts < 1:
        raise ValueError("authentication_attempts must be positive")
    retry_spacing = max(0.0, float(authentication_retry_spacing_sec))
    minimum = int(minimum_healthy_apps)
    if minimum < 1 or minimum > REQUIRED_APP_COUNT:
        raise ValueError(
            f"minimum_healthy_apps must be between 1 and {REQUIRED_APP_COUNT}"
        )

    credentials = list(credential_loader(max_apps=REQUIRED_APP_COUNT))
    names = tuple(str(item.app_name) for item in credentials)
    if len(credentials) != REQUIRED_APP_COUNT or set(names) != set(EXPECTED_APP_NAMES):
        raise RuntimeError(
            "V8 paper requires all eight credential pairs app1..app8; "
            f"observed={sorted(names)}"
        )

    authenticated: dict[str, AppRuntime] = {}
    failures: dict[str, dict[str, Any]] = {}

    def authenticate(credential: Any) -> AppRuntime:
        app_name = str(credential.app_name)
        observations: list[dict[str, Any]] = []
        last_error = ""
        for attempt in range(1, attempts + 1):
            started = common.now_ist()
            try:
                client = client_factory(credential, timeout_sec=float(timeout_sec))
                client.profile()
                finished = common.now_ist()
                observations.append(
                    {
                        "attempt": attempt,
                        "state": "AUTHENTICATED",
                        "started_at_ist": started.isoformat(timespec="microseconds"),
                        "finished_at_ist": finished.isoformat(timespec="microseconds"),
                        "error": "",
                    }
                )
                return AppRuntime(
                    app_name=app_name,
                    client=client,
                    pace_seconds=max(0.34, float(request_interval_sec)),
                    auth_attempts=attempt,
                    auth_observations=tuple(observations),
                )
            except Exception as exc:  # broker/library exceptions are provider-specific
                last_error = _clean_error(exc)
                finished = common.now_ist()
                observations.append(
                    {
                        "attempt": attempt,
                        "state": "AUTH_FAILURE",
                        "started_at_ist": started.isoformat(timespec="microseconds"),
                        "finished_at_ist": finished.isoformat(timespec="microseconds"),
                        "error": last_error,
                    }
                )
                if attempt < attempts and retry_spacing > 0:
                    time.sleep(retry_spacing)
        raise RuntimeError(
            json.dumps(
                {
                    "app_name": app_name,
                    "attempts": attempts,
                    "error": last_error,
                    "observations": observations,
                },
                sort_keys=True,
            )
        )

    with ThreadPoolExecutor(
        max_workers=REQUIRED_APP_COUNT,
        thread_name_prefix="fno-v8-paper-auth",
    ) as executor:
        pending = {
            executor.submit(authenticate, credential): credential
            for credential in credentials
        }
        for future in as_completed(pending):
            credential = pending[future]
            try:
                runtime = future.result()
                authenticated[runtime.app_name] = runtime
            except Exception as exc:  # broker/library exceptions are provider-specific
                raw_error = str(exc)
                try:
                    failure = json.loads(raw_error)
                except (TypeError, ValueError, json.JSONDecodeError):
                    failure = {
                        "app_name": str(credential.app_name),
                        "attempts": attempts,
                        "error": _clean_error(exc),
                    }
                failures[str(credential.app_name)] = failure

    missing = sorted(set(EXPECTED_APP_NAMES) - set(authenticated))
    if len(authenticated) < minimum:
        raise RuntimeError(
            "Insufficient healthy Kite apps after bounded authentication retries; "
            f"required={minimum}; healthy={sorted(authenticated)}; "
            f"missing={missing}; failures={failures}"
        )
    authentication_audit: list[dict[str, Any]] = []
    for name in EXPECTED_APP_NAMES:
        runtime = authenticated.get(name)
        if runtime is not None:
            authentication_audit.append(
                {
                    "app_name": name,
                    "authenticated": True,
                    "attempts": int(runtime.auth_attempts),
                    "observations": [
                        dict(item) for item in runtime.auth_observations
                    ],
                    "error": "",
                }
            )
            continue
        failure = failures.get(name, {})
        authentication_audit.append(
            {
                "app_name": name,
                "authenticated": False,
                "attempts": int(failure.get("attempts", attempts)),
                "observations": [
                    dict(item) for item in failure.get("observations", [])
                ],
                "error": str(failure.get("error", "authentication failed")),
            }
        )
    bound_audit = tuple(authentication_audit)
    for runtime in authenticated.values():
        runtime.pool_authentication_audit = bound_audit
    return [authenticated[name] for name in EXPECTED_APP_NAMES if name in authenticated]


def _validate_completed_bar(
    bar: Mapping[str, Any], expected_end: pd.Timestamp
) -> str:
    try:
        observed = _to_ist(bar["timestamp"])
        open_ = float(bar["open"])
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        volume = float(bar.get("volume", 0))
    except (KeyError, TypeError, ValueError):
        return "INVALID_OR_MISSING_OHLCV"
    values = (open_, high, low, close, volume)
    if observed != expected_end:
        return "WRONG_CANDLE_END"
    if not all(math.isfinite(value) for value in values):
        return "NONFINITE_OHLCV"
    if min(open_, high, low, close) <= 0:
        return "NONPOSITIVE_OHLC"
    if high < max(open_, close) or low > min(open_, close) or high < low:
        return "INVALID_OHLC_GEOMETRY"
    if volume < 0:
        return "NEGATIVE_VOLUME"
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if bool(bar.get(flag, False)):
            return f"LINEAGE_FLAG_{flag.upper()}"
    return ""


def _extract_exact_record(
    records: Iterable[Mapping[str, Any]], expected_start: pd.Timestamp
) -> dict[str, Any] | None:
    for record in records:
        raw = record.get("date", record.get("timestamp"))
        if raw is None or _to_ist(raw) != expected_start:
            continue
        return {
            "timestamp": (expected_start + pd.Timedelta(minutes=1)).isoformat(),
            "candle_start": expected_start.isoformat(),
            "open": record.get("open"),
            "high": record.get("high"),
            "low": record.get("low"),
            "close": record.get("close"),
            "volume": record.get("volume", 0),
            "gap_filled": False,
            "opening_snapshot": False,
            "provisional_stale": False,
        }
    return None


def _observe_once(
    runtime: AppRuntime,
    candidate: CandidateRequest,
    expected_end: pd.Timestamp,
    *,
    monotonic_deadline: float | None = None,
) -> tuple[str, Mapping[str, Any] | None, str]:
    expected_start = expected_end - pd.Timedelta(minutes=1)
    try:
        records = runtime.call_historical_data(
            int(candidate.instrument_token),
            expected_start.to_pydatetime(),
            (expected_start + pd.Timedelta(minutes=2)).to_pydatetime(),
            "minute",
            continuous=False,
            oi=False,
            monotonic_deadline=monotonic_deadline,
        )
        bar = _extract_exact_record(records or (), expected_start)
        if bar is None:
            return "NO_CANDLE", None, ""
        error = _validate_completed_bar(bar, expected_end)
        if error:
            return "INVALID_DATA", bar, error
        return "WRITTEN", bar, ""
    except MarketDataDeadlineExceeded as exc:
        return "DEADLINE_EXCEEDED", None, _clean_error(exc)
    except Exception as exc:  # broker/library exceptions are provider-specific
        return "API_FAILURE", None, _clean_error(exc)


def _fetch_candidate(
    runtime: AppRuntime,
    candidate: CandidateRequest,
    expected_end: pd.Timestamp,
    *,
    observations: int,
    observation_spacing_sec: float,
) -> BarOutcome:
    history: list[dict[str, Any]] = []
    latest_bar: Mapping[str, Any] | None = None
    terminal_state = "API_FAILURE"
    terminal_error = ""
    for attempt in range(1, observations + 1):
        state, bar, error = _observe_once(runtime, candidate, expected_end)
        history.append(
            {
                "attempt": attempt,
                "state": state,
                "error": error,
                "observed_at_ist": common.now_ist().isoformat(
                    timespec="microseconds"
                ),
            }
        )
        terminal_state = state
        terminal_error = error
        if state == "WRITTEN":
            latest_bar = bar
            break
        if state == "INVALID_DATA":
            latest_bar = bar
            break
        if attempt < observations and observation_spacing_sec > 0:
            time.sleep(float(observation_spacing_sec))
    if terminal_state == "NO_CANDLE" and len(history) >= observations:
        terminal_state = "VERIFIED_NO_CANDLE"
    return BarOutcome(
        symbol=candidate.symbol,
        instrument_token=candidate.instrument_token,
        app_name=runtime.app_name,
        state=terminal_state,
        attempts=len(history),
        bar=latest_bar,
        error=terminal_error,
        observations=tuple(history),
    )


def fetch_completed_minute(
    candidates: Sequence[CandidateRequest | Mapping[str, Any]],
    runtimes: Sequence[AppRuntime],
    expected_end: Any,
    *,
    now: datetime | pd.Timestamp | None = None,
    boundary_buffer_sec: float = DEFAULT_BOUNDARY_BUFFER_SEC,
    observations: int = DEFAULT_OBSERVATIONS,
    observation_spacing_sec: float = DEFAULT_OBSERVATION_SPACING_SEC,
    deadline_at: datetime | pd.Timestamp | None = None,
    circuit_breaker_failures: int = DEFAULT_CIRCUIT_BREAKER_FAILURES,
    circuit_breaker_cooldown_sec: float = DEFAULT_CIRCUIT_BREAKER_COOLDOWN_SEC,
    minimum_healthy_apps: int = DEFAULT_MINIMUM_HEALTHY_APPS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Fetch an exact completed minute through a resilient shared app queue.

    A candidate is never complete until its exact bar has been returned and
    validated.  Retryable failures are requeued to another healthy app where
    possible.  Faster apps naturally consume more of the shared queue, while a
    repeatedly failing app is cooled down instead of retaining an immutable
    symbol partition.
    """

    pool = validate_runtime_pool(
        runtimes, minimum_healthy_apps=int(minimum_healthy_apps)
    )
    if observations < 1:
        raise ValueError("observations must be positive")
    breaker_failures = int(circuit_breaker_failures)
    if breaker_failures < 1:
        raise ValueError("circuit_breaker_failures must be positive")
    breaker_cooldown = max(0.0, float(circuit_breaker_cooldown_sec))
    end = _to_ist(expected_end)
    observed_now = _to_ist(now if now is not None else common.now_ist())
    due = end + pd.Timedelta(seconds=float(boundary_buffer_sec))
    if observed_now < due:
        raise RuntimeError(
            f"Minute {end.isoformat()} is not complete until {due.isoformat()}"
        )

    normalized = [
        value
        if isinstance(value, CandidateRequest)
        else CandidateRequest.from_mapping(value)
        for value in candidates
    ]
    normalized.sort(key=lambda item: (item.symbol, item.instrument_token))
    symbols = [item.symbol for item in normalized]
    if len(symbols) != len(set(symbols)):
        raise ValueError("Completed-minute candidate symbols must be unique")
    if len({item.instrument_token for item in normalized}) != len(normalized):
        raise ValueError("Completed-minute candidate tokens must be unique")

    fetch_deadline = _to_ist(
        deadline_at
        if deadline_at is not None
        else end
        + pd.Timedelta(minutes=1)
        + pd.Timedelta(seconds=float(boundary_buffer_sec))
    )
    deadline_budget_sec = max(
        0.0, (fetch_deadline - observed_now).total_seconds()
    )
    monotonic_deadline = time.monotonic() + deadline_budget_sec
    spacing = max(0.0, float(observation_spacing_sec))
    fetch_started_at = common.now_ist()

    by_symbol = {item.symbol: item for item in normalized}
    pending_symbols: deque[str] = deque(symbols)
    candidate_state: dict[str, dict[str, Any]] = {
        item.symbol: {
            "attempts": [],
            "attempted_apps": set(),
            "ready_at": 0.0,
            "final_state": "",
            "final_error": "",
            "final_app": "",
            "bar": None,
        }
        for item in normalized
    }
    app_state: dict[str, dict[str, Any]] = {}
    for runtime in pool:
        persistent = runtime.market_data_health()
        app_state[runtime.app_name] = {
            "consecutive_api_failures": int(
                persistent["consecutive_failures"]
            ),
            "circuit_open_until": float(persistent["circuit_open_until"]),
            "circuit_opened": 0,
            "circuit_opened_total": int(
                persistent["circuit_opened_total"]
            ),
        }
    runtime_by_name = {runtime.app_name: runtime for runtime in pool}
    available_apps = set(runtime_by_name)
    inflight: dict[Future[Any], tuple[str, str, datetime, float]] = {}

    def observe(
        runtime: AppRuntime,
        candidate: CandidateRequest,
    ) -> tuple[str, Mapping[str, Any] | None, str, datetime, float, float]:
        started_at = common.now_ist()
        started_monotonic = time.monotonic()
        state, bar, error = _observe_once(
            runtime,
            candidate,
            end,
            monotonic_deadline=monotonic_deadline,
        )
        finished_monotonic = time.monotonic()
        return (
            state,
            bar,
            error,
            common.now_ist(),
            finished_monotonic - started_monotonic,
            finished_monotonic,
        )

    def eligible_symbol(app_name: str, now_monotonic: float) -> str | None:
        """Pop one ready job, preferring an app not yet tried for that symbol."""

        if not pending_symbols:
            return None
        alternative_apps = {
            name
            for name in runtime_by_name
            if name != app_name
            and app_state[name]["circuit_open_until"] <= now_monotonic
        }
        fallback: str | None = None
        for _ in range(len(pending_symbols)):
            symbol = pending_symbols.popleft()
            state = candidate_state[symbol]
            if state["ready_at"] > now_monotonic:
                pending_symbols.append(symbol)
                continue
            already_tried = app_name in state["attempted_apps"]
            has_untried_alternative = any(
                name not in state["attempted_apps"] for name in alternative_apps
            )
            if already_tried and has_untried_alternative:
                pending_symbols.append(symbol)
                if fallback is None:
                    fallback = symbol
                continue
            return symbol
        # ``fallback`` remains queued for another healthy worker.  Returning
        # None here is what guarantees cross-app retry when an alternative is
        # available rather than immediately hammering the same failed app.
        return None

    def finalize_deadline(symbol: str, app_name: str = "") -> None:
        state = candidate_state[symbol]
        if state["final_state"]:
            return
        state["final_state"] = "DEADLINE_EXCEEDED"
        state["final_error"] = (
            f"Exact candle was not completed before {fetch_deadline.isoformat()}"
        )
        state["final_app"] = app_name or (
            state["attempts"][-1]["app_name"] if state["attempts"] else ""
        )

    def update_app_health(app_name: str, state_name: str) -> None:
        prior_total = int(app_state[app_name]["circuit_opened_total"])
        snapshot = runtime_by_name[app_name].record_market_data_result(
            state_name,
            failure_threshold=breaker_failures,
            cooldown_sec=breaker_cooldown,
        )
        new_total = int(snapshot["circuit_opened_total"])
        app_state[app_name].update(
            {
                "consecutive_api_failures": int(
                    snapshot["consecutive_failures"]
                ),
                "circuit_open_until": float(snapshot["circuit_open_until"]),
                "circuit_opened_total": new_total,
                "circuit_opened": int(app_state[app_name]["circuit_opened"])
                + max(0, new_total - prior_total),
            }
        )

    executor = ThreadPoolExecutor(
        max_workers=len(pool),
        thread_name_prefix="fno-v8-paper-minute",
    )
    deadline_exhausted = False
    try:
        while pending_symbols or inflight:
            now_monotonic = time.monotonic()
            completed = {future for future in inflight if future.done()}
            if now_monotonic >= monotonic_deadline and not completed:
                deadline_exhausted = bool(pending_symbols or inflight)
                while pending_symbols:
                    finalize_deadline(pending_symbols.popleft())
                for future, (app_name, symbol, started_at, _) in list(inflight.items()):
                    cancelled_before_start = future.cancel()
                    state = candidate_state[symbol]
                    state["attempts"].append(
                        {
                            "attempt": len(state["attempts"]) + 1,
                            "app_name": app_name,
                            "state": "DEADLINE_EXCEEDED",
                            "error": (
                                "Broker request did not finish before the exact-minute deadline"
                            ),
                            "started_at_ist": started_at.isoformat(
                                timespec="microseconds"
                            ),
                            "finished_at_ist": common.now_ist().isoformat(
                                timespec="microseconds"
                            ),
                            "cancelled_before_start": bool(cancelled_before_start),
                        }
                    )
                    if not cancelled_before_start:
                        update_app_health(app_name, "DEADLINE_EXCEEDED")
                    finalize_deadline(symbol, app_name)
                inflight.clear()
                break

            if now_monotonic < monotonic_deadline:
                for app_name in tuple(
                    name for name in EXPECTED_APP_NAMES if name in available_apps
                ):
                    if not pending_symbols:
                        break
                    health = app_state[app_name]
                    if health["circuit_open_until"] > now_monotonic:
                        continue
                    symbol = eligible_symbol(app_name, now_monotonic)
                    if symbol is None:
                        continue
                    available_apps.remove(app_name)
                    started_at = common.now_ist()
                    started_monotonic = time.monotonic()
                    future = executor.submit(
                        observe, runtime_by_name[app_name], by_symbol[symbol]
                    )
                    inflight[future] = (
                        app_name,
                        symbol,
                        started_at,
                        started_monotonic,
                    )

            if not inflight:
                if not pending_symbols:
                    break
                wake_times = [monotonic_deadline]
                wake_times.extend(
                    float(candidate_state[symbol]["ready_at"])
                    for symbol in pending_symbols
                    if float(candidate_state[symbol]["ready_at"]) > now_monotonic
                )
                wake_times.extend(
                    float(state["circuit_open_until"])
                    for state in app_state.values()
                    if float(state["circuit_open_until"]) > now_monotonic
                )
                time.sleep(max(0.0, min(0.05, min(wake_times) - now_monotonic)))
                continue

            if not completed:
                remaining = max(0.0, monotonic_deadline - time.monotonic())
                completed, _ = wait(
                    tuple(inflight),
                    timeout=min(0.05, remaining),
                    return_when=FIRST_COMPLETED,
                )
            for future in completed:
                app_name, symbol, started_at, started_monotonic = inflight.pop(future)
                available_apps.add(app_name)
                candidate = candidate_state[symbol]
                try:
                    (
                        state_name,
                        bar,
                        error,
                        finished_at,
                        duration_sec,
                        finished_monotonic,
                    ) = future.result()
                except Exception as exc:  # defensive: _observe_once already contains provider errors
                    state_name, bar, error = "API_FAILURE", None, _clean_error(exc)
                    finished_at = common.now_ist()
                    finished_monotonic = time.monotonic()
                    duration_sec = finished_monotonic - started_monotonic
                if finished_monotonic > monotonic_deadline:
                    state_name, bar, error = (
                        "DEADLINE_EXCEEDED",
                        None,
                        "Broker response arrived after the exact-minute deadline",
                    )
                    deadline_exhausted = True
                candidate["attempted_apps"].add(app_name)
                candidate["attempts"].append(
                    {
                        "attempt": len(candidate["attempts"]) + 1,
                        "app_name": app_name,
                        "state": state_name,
                        "error": error,
                        "started_at_ist": started_at.isoformat(
                            timespec="microseconds"
                        ),
                        "finished_at_ist": finished_at.isoformat(
                            timespec="microseconds"
                        ),
                        "duration_sec": round(float(duration_sec), 6),
                    }
                )
                update_app_health(app_name, state_name)

                if state_name == "WRITTEN":
                    candidate["final_state"] = state_name
                    candidate["final_app"] = app_name
                    candidate["bar"] = bar
                    continue
                if state_name == "INVALID_DATA" and bar is not None:
                    candidate["bar"] = bar
                if state_name == "DEADLINE_EXCEEDED":
                    candidate["final_state"] = state_name
                    candidate["final_error"] = error
                    candidate["final_app"] = app_name
                    continue
                if len(candidate["attempts"]) >= int(observations):
                    candidate["final_state"] = (
                        "VERIFIED_NO_CANDLE"
                        if state_name == "NO_CANDLE"
                        else state_name
                    )
                    candidate["final_error"] = error
                    candidate["final_app"] = app_name
                    continue
                candidate["ready_at"] = time.monotonic() + spacing
                pending_symbols.append(symbol)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    outcomes: list[BarOutcome] = []
    for item in normalized:
        state = candidate_state[item.symbol]
        if not state["final_state"]:
            finalize_deadline(item.symbol)
        outcomes.append(
            BarOutcome(
                symbol=item.symbol,
                instrument_token=item.instrument_token,
                app_name=str(state["final_app"]),
                state=str(state["final_state"]),
                attempts=len(state["attempts"]),
                bar=state["bar"],
                error=str(state["final_error"]),
                observations=tuple(state["attempts"]),
            )
        )
    outcomes.sort(key=lambda item: item.symbol)

    bars = []
    for outcome in outcomes:
        if outcome.state != "WRITTEN" or outcome.bar is None:
            continue
        bars.append(
            {
                "symbol": outcome.symbol,
                "instrument_token": outcome.instrument_token,
                "app_name": outcome.app_name,
                **dict(outcome.bar),
            }
        )
    frame = pd.DataFrame(
        bars,
        columns=[
            "symbol",
            "instrument_token",
            "app_name",
            "timestamp",
            "candle_start",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "gap_filled",
            "opening_snapshot",
            "provisional_stale",
        ],
    )

    app_audit: list[dict[str, Any]] = []
    for runtime in pool:
        attempts_for_app = [
            observation
            for item in outcomes
            for observation in item.observations
            if observation.get("app_name") == runtime.app_name
        ]
        first_assignments = [
            item
            for item in outcomes
            if item.observations
            and item.observations[0].get("app_name") == runtime.app_name
        ]
        app_audit.append(
            {
                "app_name": runtime.app_name,
                "authenticated": True,
                "assigned": len(first_assignments),
                "attempted": len(attempts_for_app),
                "unique_symbols_attempted": len(
                    {
                        item.symbol
                        for item in outcomes
                        if any(
                            observation.get("app_name") == runtime.app_name
                            for observation in item.observations
                        )
                    }
                ),
                "written": sum(
                    observation.get("state") == "WRITTEN"
                    for observation in attempts_for_app
                ),
                "verified_no_candle": sum(
                    observation.get("state") == "NO_CANDLE"
                    for observation in attempts_for_app
                ),
                "invalid": sum(
                    observation.get("state") == "INVALID_DATA"
                    for observation in attempts_for_app
                ),
                "api_failed": sum(
                    observation.get("state") == "API_FAILURE"
                    for observation in attempts_for_app
                ),
                "deadline_exceeded": sum(
                    observation.get("state") == "DEADLINE_EXCEEDED"
                    for observation in attempts_for_app
                ),
                "circuit_opened": int(
                    app_state[runtime.app_name]["circuit_opened"]
                ),
                "circuit_opened_total": int(
                    app_state[runtime.app_name]["circuit_opened_total"]
                ),
                "circuit_open_at_finish": bool(
                    app_state[runtime.app_name]["circuit_open_until"]
                    > time.monotonic()
                ),
            }
        )
    expected_symbols = set(symbols)
    written_symbols = {
        item.symbol
        for item in outcomes
        if item.state == "WRITTEN" and item.bar is not None
    }
    complete = (
        len(outcomes) == len(normalized)
        and written_symbols == expected_symbols
        and len(frame) == len(normalized)
    )
    fetch_finished_at = common.now_ist()
    marker = {
        "schema_version": MINUTE_SNAPSHOT_SCHEMA_VERSION,
        "policy_version": MARKET_DATA_POLICY_VERSION,
        "expected_end_ist": end.isoformat(),
        "completed_boundary_ist": due.isoformat(),
        "observed_at_ist": observed_now.isoformat(),
        "candidate_count": len(normalized),
        "candidate_contract_sha256": common.canonical_json_sha256(
            [asdict(item) for item in normalized]
        ),
        "preferred_app_roster": list(EXPECTED_APP_NAMES),
        "preferred_app_count": REQUIRED_APP_COUNT,
        "app_roster": app_roster_payload(pool),
        "app_roster_sha256": app_roster_sha256(pool),
        "app_authentication": app_authentication_payload(pool),
        "healthy_app_count": len(pool),
        "minimum_healthy_app_count": int(minimum_healthy_apps),
        "degraded_app_pool": len(pool) < REQUIRED_APP_COUNT,
        "app_usage": app_audit,
        "outcomes": [item.audit_payload() for item in outcomes],
        "written_count": len(frame),
        "exact_symbol_completeness": complete,
        "complete": complete,
        "state": "SUCCESS" if complete else "DATA_INCOMPLETE",
        "fetch_started_at_ist": fetch_started_at.isoformat(
            timespec="microseconds"
        ),
        "fetch_finished_at_ist": fetch_finished_at.isoformat(
            timespec="microseconds"
        ),
        "deadline_at_ist": fetch_deadline.isoformat(),
        "deadline_budget_sec": float(deadline_budget_sec),
        "deadline_exhausted": bool(deadline_exhausted),
        "retry_policy": {
            "max_attempts_per_symbol": int(observations),
            "observation_spacing_sec": spacing,
            "cross_app_requeue": True,
            "circuit_breaker_failures": breaker_failures,
            "circuit_breaker_cooldown_sec": breaker_cooldown,
        },
    }
    return frame, bind_marker_payload_sha256(marker)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _publish_file_once(target: Path, writer: Callable[[Path], None]) -> str:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
    )
    os.close(fd)
    temporary = Path(raw)
    try:
        writer(temporary)
        with temporary.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        incoming = _sha256_file(temporary)
        try:
            os.link(temporary, target)
        except FileExistsError:
            existing = _sha256_file(target)
            if existing != incoming:
                raise RuntimeError(f"Immutable evidence collision: {target}")
            return existing
        return incoming
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def publish_minute_snapshot_once(
    root: Path,
    frame: pd.DataFrame,
    marker: Mapping[str, Any],
    *,
    strategy_fingerprint: str,
) -> dict[str, Any]:
    """Publish an immutable parquet+JSON pair and bind both byte identities."""

    end = _to_ist(marker["expected_end_ist"])
    stem = end.strftime("minute_%H%M")
    data_path = Path(root) / f"{stem}.parquet"
    marker_path = Path(root) / f"{stem}.json"
    data_sha = _publish_file_once(
        data_path,
        lambda path: frame.to_parquet(path, index=False, engine="pyarrow"),
    )
    bound = bind_marker_payload_sha256({
        **dict(marker),
        "strategy_fingerprint": str(strategy_fingerprint),
        "data_path": str(data_path),
        "data_sha256": data_sha,
        "data_rows": int(len(frame)),
    })
    _publish_file_once(
        marker_path,
        lambda path: path.write_text(
            json.dumps(bound, indent=2, ensure_ascii=True, default=str) + "\n",
            encoding="utf-8",
        ),
    )
    observed = json.loads(marker_path.read_text(encoding="utf-8"))
    if observed != json.loads(json.dumps(bound, default=str)):
        raise RuntimeError(f"Immutable minute marker validation failed: {marker_path}")
    if _sha256_file(data_path) != str(observed["data_sha256"]):
        raise RuntimeError(f"Immutable minute data hash mismatch: {data_path}")
    return observed


def load_validated_minute_snapshot(
    marker_path: Path,
    *,
    strategy_fingerprint: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    marker = json.loads(Path(marker_path).read_text(encoding="utf-8"))
    claimed_payload_sha = marker.get(MARKER_PAYLOAD_SHA256_FIELD)
    if (
        not isinstance(claimed_payload_sha, str)
        or claimed_payload_sha != marker_payload_sha256(marker)
    ):
        raise RuntimeError("V8 minute snapshot marker payload changed")
    if marker.get("schema_version") != MINUTE_SNAPSHOT_SCHEMA_VERSION:
        raise RuntimeError("Unsupported V8 minute snapshot schema")
    if marker.get("policy_version") != MARKET_DATA_POLICY_VERSION:
        raise RuntimeError("V8 minute snapshot policy mismatch")
    if marker.get("strategy_fingerprint") != strategy_fingerprint:
        raise RuntimeError("V8 minute snapshot strategy mismatch")
    data_path = Path(str(marker.get("data_path", "")))
    if not data_path.is_file() or _sha256_file(data_path) != marker.get("data_sha256"):
        raise RuntimeError("V8 minute snapshot data is missing or changed")
    frame = pd.read_parquet(data_path)
    if len(frame) != int(marker.get("data_rows", -1)):
        raise RuntimeError("V8 minute snapshot row count changed")
    return frame, marker
