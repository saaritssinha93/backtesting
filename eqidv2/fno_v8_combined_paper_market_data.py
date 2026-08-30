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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

import fno_oi_common as common


MARKET_DATA_POLICY_VERSION = "fno_v8_combined_exact_completed_1m_eight_app_v1"
MINUTE_SNAPSHOT_SCHEMA_VERSION = "fno_v8_combined_minute_snapshot_v1"
REQUIRED_APP_COUNT = 8
EXPECTED_APP_NAMES = tuple(f"app{index}" for index in range(1, 9))
DEFAULT_BOUNDARY_BUFFER_SEC = 3.0
DEFAULT_REQUEST_INTERVAL_SEC = 0.36
DEFAULT_TIMEOUT_SEC = 8.0
DEFAULT_OBSERVATIONS = 3
DEFAULT_OBSERVATION_SPACING_SEC = 2.0


def _to_ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def _clean_error(exc: BaseException) -> str:
    return " ".join(f"{type(exc).__name__}: {exc}".split())[:500]


@dataclass(frozen=True)
class CandidateRequest:
    symbol: str
    instrument_token: int

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
    _last_call_at: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def pace(self) -> None:
        with self._lock:
            delay = float(self.pace_seconds) - (
                time.monotonic() - float(self._last_call_at)
            )
            if delay > 0:
                time.sleep(delay)
            self._last_call_at = time.monotonic()


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


def authenticate_required_apps(
    *,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    request_interval_sec: float = DEFAULT_REQUEST_INTERVAL_SEC,
    credential_loader: Callable[..., Sequence[Any]] = common.discover_kite_credentials,
    client_factory: Callable[..., Any] = common.make_kite_client,
) -> list[AppRuntime]:
    """Authenticate exactly app1..app8 or fail before any market-data fetch."""

    credentials = list(credential_loader(max_apps=REQUIRED_APP_COUNT))
    names = tuple(str(item.app_name) for item in credentials)
    if len(credentials) != REQUIRED_APP_COUNT or set(names) != set(EXPECTED_APP_NAMES):
        raise RuntimeError(
            "V8 paper requires all eight credential pairs app1..app8; "
            f"observed={sorted(names)}"
        )

    authenticated: dict[str, AppRuntime] = {}
    failures: dict[str, str] = {}

    def authenticate(credential: Any) -> AppRuntime:
        client = client_factory(credential, timeout_sec=float(timeout_sec))
        client.profile()
        return AppRuntime(
            app_name=str(credential.app_name),
            client=client,
            pace_seconds=max(0.34, float(request_interval_sec)),
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
                failures[str(credential.app_name)] = _clean_error(exc)

    missing = sorted(set(EXPECTED_APP_NAMES) - set(authenticated))
    if failures or missing:
        raise RuntimeError(
            "All eight Kite apps must authenticate for V8 paper; "
            f"missing={missing}; failures={failures}"
        )
    return [authenticated[name] for name in EXPECTED_APP_NAMES]


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
) -> tuple[str, Mapping[str, Any] | None, str]:
    expected_start = expected_end - pd.Timedelta(minutes=1)
    try:
        runtime.pace()
        records = runtime.client.historical_data(
            int(candidate.instrument_token),
            expected_start.to_pydatetime(),
            (expected_start + pd.Timedelta(minutes=2)).to_pydatetime(),
            "minute",
            continuous=False,
            oi=False,
        )
        bar = _extract_exact_record(records or (), expected_start)
        if bar is None:
            return "NO_CANDLE", None, ""
        error = _validate_completed_bar(bar, expected_end)
        if error:
            return "INVALID_DATA", bar, error
        return "WRITTEN", bar, ""
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
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Fetch one exact completed end-labelled minute with an eight-app audit."""

    if len(runtimes) != REQUIRED_APP_COUNT or tuple(
        runtime.app_name for runtime in runtimes
    ) != EXPECTED_APP_NAMES:
        raise RuntimeError("fetch_completed_minute requires ordered app1..app8")
    if observations < 1:
        raise ValueError("observations must be positive")
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

    partitions = [normalized[index::REQUIRED_APP_COUNT] for index in range(8)]
    outcomes: list[BarOutcome] = []

    def work(runtime: AppRuntime, rows: Sequence[CandidateRequest]) -> list[BarOutcome]:
        return [
            _fetch_candidate(
                runtime,
                candidate,
                end,
                observations=int(observations),
                observation_spacing_sec=float(observation_spacing_sec),
            )
            for candidate in rows
        ]

    with ThreadPoolExecutor(
        max_workers=REQUIRED_APP_COUNT,
        thread_name_prefix="fno-v8-paper-minute",
    ) as executor:
        pending = {
            executor.submit(work, runtime, rows): runtime.app_name
            for runtime, rows in zip(runtimes, partitions)
        }
        for future in as_completed(pending):
            outcomes.extend(future.result())
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
    for runtime, assigned in zip(runtimes, partitions):
        app_outcomes = [item for item in outcomes if item.app_name == runtime.app_name]
        app_audit.append(
            {
                "app_name": runtime.app_name,
                "authenticated": True,
                "assigned": len(assigned),
                "written": sum(item.state == "WRITTEN" for item in app_outcomes),
                "verified_no_candle": sum(
                    item.state == "VERIFIED_NO_CANDLE" for item in app_outcomes
                ),
                "invalid": sum(item.state == "INVALID_DATA" for item in app_outcomes),
                "api_failed": sum(item.state == "API_FAILURE" for item in app_outcomes),
            }
        )
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
        "app_roster": app_roster_payload(runtimes),
        "app_roster_sha256": app_roster_sha256(runtimes),
        "app_usage": app_audit,
        "outcomes": [item.audit_payload() for item in outcomes],
        "written_count": len(frame),
        "complete": all(item.state == "WRITTEN" for item in outcomes),
        "state": (
            "SUCCESS"
            if all(item.state == "WRITTEN" for item in outcomes)
            else "DATA_INCOMPLETE"
        ),
    }
    return frame, marker


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
    bound = {
        **dict(marker),
        "strategy_fingerprint": str(strategy_fingerprint),
        "data_path": str(data_path),
        "data_sha256": data_sha,
        "data_rows": int(len(frame)),
    }
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

