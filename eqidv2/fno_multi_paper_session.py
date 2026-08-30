"""Prospective one-process PAPER session for frozen V10, V11 and V12.

The scheduled ``run`` command is a real completed-candle session.  It uses the
shared full-universe five-minute and union one-minute adapter, then feeds the
same immutable source bytes into three policy-isolated ledgers.  There is no
broker order API or LIVE execution mode in this module.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import inspect
import io
import json
import math
import os
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as day_time, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import fno_multi_paper_engine as paper_engine
import fno_multi_paper_profiles as profiles
import fno_multi_paper_report as reporting
import fno_multi_paper_parity as parity
from fno_v8_combined_paper_market_data import DEFAULT_BOUNDARY_BUFFER_SEC
from eqidv2_runtime_paths import runtime_dir


IST = ZoneInfo("Asia/Kolkata")
SESSION_ID = "fno_v10_v11_v12_paper"
SESSION_TITLE = "FnO V10/V11/V12 Papertrade"
SESSION_SCHEMA_VERSION = "fno_multi_paper_session_v2"
CHECKPOINT_SCHEMA_VERSION = "fno_multi_paper_checkpoint_v2"
STATUS_SCHEMA_VERSION = "fno_multi_paper_status_v2"
HEARTBEAT_SCHEMA_VERSION = "fno_multi_paper_heartbeat_v2"
MODE = "PAPER"
PAPER_ONLY = True
SIGNAL_ENDS = ("09:25", "09:30", "09:35", "09:40", "09:45")
BOUNDARY_BUFFER_SECONDS = DEFAULT_BOUNDARY_BUFFER_SEC
SQUARE_OFF = "15:30"
DEFAULT_POLL_SECONDS = 1.0
PREFERRED_APP_NAMES = tuple(f"app{index}" for index in range(1, 9))
MIN_HEALTHY_APP_COUNT = 7
TERMINAL_CHECKPOINT_STATES = frozenset({"COMPLETE", "BLOCKED", "DEGRADED"})

ROOT = runtime_dir("fno_oi", "multi_strategy_paper_v1")
LATEST_ROOT = runtime_dir("fno_oi", "latest")
STATUS_PATH = ROOT / "status.json"
HEARTBEAT_PATH = ROOT / "heartbeat.json"
LOCK_PATH = ROOT / "fno_v10_v11_v12_paper.lock"
LATEST_COMBINED_REPORT = LATEST_ROOT / "latest_fno_v10_v11_v12_paper.md"
LATEST_PROFILE_REPORTS: Mapping[str, Path] = {
    "v10": LATEST_ROOT / "latest_fno_v10_paper.md",
    "v11": LATEST_ROOT / "latest_fno_v11_paper.md",
    "v12": LATEST_ROOT / "latest_fno_v12_paper.md",
}


class MultiPaperSessionError(RuntimeError):
    pass


class ProspectiveStartMissed(MultiPaperSessionError):
    pass


class SourceIncompleteError(MultiPaperSessionError):
    pass


@dataclass(frozen=True)
class SessionPaths:
    session_date: date
    root: Path = ROOT
    latest_root: Path = LATEST_ROOT
    status_path: Path = STATUS_PATH
    heartbeat_path: Path = HEARTBEAT_PATH
    lock_path: Path = LOCK_PATH

    @property
    def day_root(self) -> Path:
        return self.root / "sessions" / self.session_date.isoformat()

    @property
    def profile_root(self) -> Path:
        return self.day_root / "profiles"

    @property
    def combined_report_path(self) -> Path:
        return self.day_root / "combined_report.md"

    @property
    def combined_trades_path(self) -> Path:
        return self.day_root / "combined_trades.csv"

    @property
    def events_path(self) -> Path:
        return self.day_root / "events.jsonl"

    @property
    def manifest_path(self) -> Path:
        return self.day_root / "run_manifest.json"

    @property
    def checkpoint_path(self) -> Path:
        return self.day_root / "checkpoint.json"

    def profile_report_path(self, key: str) -> Path:
        return self.profile_root / key / "report.md"

    def profile_trades_path(self, key: str) -> Path:
        return self.profile_root / key / "trades.csv"

    def profile_selection_path(self, key: str) -> Path:
        return self.profile_root / key / "selection_audit.csv"

    def latest_profile_report(self, key: str) -> Path:
        return self.latest_root / f"latest_fno_{key}_paper.md"

    @property
    def latest_combined_report(self) -> Path:
        return self.latest_root / "latest_fno_v10_v11_v12_paper.md"


@dataclass
class RuntimeState:
    status: str = "NOT_RUN"
    phase: str = "BOOT"
    message: str = ""
    source_complete: bool = False
    data_incomplete: bool = False
    completed_minutes: int = 0
    last_processed_minute: str | None = None
    ingested_slots: tuple[str, ...] = ()
    skipped_slots: tuple[str, ...] = ()
    preferred_app_count: int = len(PREFERRED_APP_NAMES)
    healthy_app_count: int = 0
    healthy_apps: tuple[str, ...] = ()
    unhealthy_apps: tuple[str, ...] = PREFERRED_APP_NAMES
    app_pool_state: str = "NOT_CHECKED"
    last_app_event_minute: str | None = None
    last_app_usage: str = ""
    last_app_retry_count: int = 0
    last_app_failure_count: int = 0


def _set_runtime_app_pool(
    state: RuntimeState,
    runtimes: Sequence[Any],
) -> None:
    names = tuple(str(runtime.app_name).strip() for runtime in runtimes)
    if (
        not names
        or len(names) < MIN_HEALTHY_APP_COUNT
        or len(names) != len(set(names))
        or any(name not in PREFERRED_APP_NAMES for name in names)
        or names != tuple(name for name in PREFERRED_APP_NAMES if name in set(names))
    ):
        raise MultiPaperSessionError(
            "authenticated app pool must contain at least "
            f"{MIN_HEALTHY_APP_COUNT} apps in approved order: {list(names)}"
        )
    state.preferred_app_count = len(PREFERRED_APP_NAMES)
    state.healthy_app_count = len(names)
    state.healthy_apps = names
    state.unhealthy_apps = tuple(name for name in PREFERRED_APP_NAMES if name not in names)
    state.app_pool_state = (
        "HEALTHY" if len(names) == len(PREFERRED_APP_NAMES) else "DEGRADED_HEALTHY"
    )


def _record_app_usage(
    state: RuntimeState,
    marker: Mapping[str, Any],
    event_minute: datetime,
) -> None:
    usage = marker.get("app_usage")
    outcomes = marker.get("outcomes")
    if not isinstance(usage, list):
        usage = []
    if not isinstance(outcomes, list):
        outcomes = []
    parts: list[str] = []
    failures = 0
    for raw in usage:
        if not isinstance(raw, Mapping):
            continue
        name = str(raw.get("app_name", "")).strip()
        attempted = int(raw.get("attempted", raw.get("assigned", 0)) or 0)
        written = int(raw.get("written", 0) or 0)
        failed = sum(
            int(raw.get(key, 0) or 0)
            for key in ("api_failed", "invalid", "deadline_exceeded")
        )
        failures += failed
        parts.append(f"{name}:{written}/{attempted}/err{failed}")
    retries = 0
    for raw in outcomes:
        if not isinstance(raw, Mapping):
            continue
        retries += max(0, int(raw.get("attempts", 0) or 0) - 1)
    state.last_app_event_minute = event_minute.isoformat()
    state.last_app_usage = "; ".join(parts)
    state.last_app_retry_count = retries
    state.last_app_failure_count = failures


def now_ist() -> datetime:
    return datetime.now(IST)


def _normalize_now(value: datetime | None) -> datetime:
    observed = value or now_ist()
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ValueError("runtime clock must be timezone-aware")
    return observed.astimezone(IST)


def _at(session_date: date, hhmm: str) -> datetime:
    return datetime.combine(session_date, day_time.fromisoformat(hhmm), IST)


def _floor_due(observed: datetime) -> datetime:
    value = observed - timedelta(seconds=BOUNDARY_BUFFER_SECONDS)
    return value.replace(second=0, microsecond=0)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date, Path)):
        return str(value) if isinstance(value, Path) else value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            _jsonable(value),
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _replace_with_retry(source: Path, destination: Path) -> None:
    """Atomically replace a file despite a brief Windows reader share-lock."""

    for attempt in range(8):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if attempt == 7:
                raise
            time.sleep(0.02 * (2**attempt))


def _atomic_bytes(path: Path, content: bytes) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=str(destination.parent),
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retry(temporary, destination)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, value: Any) -> None:
    _atomic_bytes(path, _json_bytes(value))


def _atomic_text(path: Path, value: str) -> None:
    content = value if value.endswith("\n") else value + "\n"
    _atomic_bytes(path, content.encode("utf-8"))


def _source_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ProcessLock:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._handle: io.BufferedRandom | None = None

    def __enter__(self) -> "ProcessLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise MultiPaperSessionError(
                "another combined PAPER writer already owns the lock"
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(
            _json_bytes(
                {
                    "pid": os.getpid(),
                    "session_id": SESSION_ID,
                    "acquired_at_ist": now_ist(),
                }
            )
        )
        handle.flush()
        self._handle = handle
        return self

    def __exit__(self, *args: Any) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        with contextlib.suppress(OSError):
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:  # pragma: no cover
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _write_csv(path: Path, records: Sequence[Mapping[str, Any]]) -> None:
    fields = sorted({str(key) for record in records for key in record})
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=str(destination.parent),
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=fields or ["candidate_id"])
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        key: (
                            json.dumps(_jsonable(value), sort_keys=True)
                            if isinstance(value, (Mapping, list, tuple))
                            else _jsonable(value)
                        )
                        for key, value in record.items()
                    }
                )
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retry(temporary, destination)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _event_records(engine: paper_engine.MultiStrategyPaperEngine) -> list[dict[str, Any]]:
    return [event.to_dict() for event in engine.events()]


def publish_outputs(
    paths: SessionPaths,
    engine: paper_engine.MultiStrategyPaperEngine,
    state: RuntimeState,
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    observed = _normalize_now(generated_at)
    records_by_profile = engine.records_by_profile()
    selection_by_profile = engine.selection_records_by_profile()
    all_events = _event_records(engine)
    summaries: dict[str, dict[str, Any]] = {}
    for profile in profiles.PROFILES:
        events = [row for row in all_events if row["profile_key"] == profile.key]
        text, summary = reporting.render_profile_report(
            profile,
            records_by_profile[profile.key],
            selection_by_profile[profile.key],
            events,
            session_date=paths.session_date,
            runtime_status=state.status,
            source_complete=state.source_complete,
            message=state.message,
            generated_at=observed,
        )
        summaries[profile.key] = summary
        _atomic_text(paths.profile_report_path(profile.key), text)
        _atomic_text(paths.latest_profile_report(profile.key), text)
        _write_csv(paths.profile_trades_path(profile.key), records_by_profile[profile.key])
        _write_csv(paths.profile_selection_path(profile.key), selection_by_profile[profile.key])

    combined = reporting.render_combined_report(
        summaries,
        session_date=paths.session_date,
        runtime_status=state.status,
        source_complete=state.source_complete,
        message=state.message,
        generated_at=observed,
    )
    _atomic_text(paths.combined_report_path, combined)
    _atomic_text(paths.latest_combined_report, combined)
    _write_csv(paths.combined_trades_path, engine.records())
    event_lines = b"".join(
        json.dumps(_jsonable(row), sort_keys=True, separators=(",", ":")).encode("utf-8")
        + b"\n"
        for row in all_events
    )
    _atomic_bytes(paths.events_path, event_lines)

    overall_valid = bool(summaries) and all(
        bool(item["headline_valid"]) for item in summaries.values()
    )
    status_payload = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "session_id": SESSION_ID,
        "session_title": SESSION_TITLE,
        "mode": MODE,
        "paper_only": True,
        "status": state.status,
        "phase": state.phase,
        "message": state.message,
        "session_date": paths.session_date.isoformat(),
        "last_update_ist": observed.isoformat(),
        "last_processed_minute": state.last_processed_minute,
        "ingested_slots": list(state.ingested_slots),
        "skipped_slots": list(state.skipped_slots),
        "completed_minutes": state.completed_minutes,
        "preferred_app_count": state.preferred_app_count,
        "healthy_app_count": state.healthy_app_count,
        "healthy_apps": ",".join(state.healthy_apps),
        "unhealthy_apps": ",".join(state.unhealthy_apps),
        "app_pool_state": state.app_pool_state,
        "last_app_event_minute": state.last_app_event_minute,
        "last_app_usage": state.last_app_usage,
        "last_app_retry_count": state.last_app_retry_count,
        "last_app_failure_count": state.last_app_failure_count,
        "source_complete": state.source_complete,
        "data_incomplete": state.data_incomplete,
        "headline_valid": overall_valid,
        "parity_status": parity.PARITY_STATUS,
        "full_history_event_parity_certified": False,
        "combined_report_path": str(paths.latest_combined_report),
        "profiles": {
            key: {
                **summary,
                "parity_status": parity.PARITY_STATUS,
                "full_history_event_parity_certified": False,
                "report_path": str(paths.latest_profile_report(key)),
            }
            for key, summary in summaries.items()
        },
    }
    _atomic_json(paths.status_path, status_payload)
    publish_heartbeat(paths, state, observed_at=observed)
    return status_payload


def publish_heartbeat(
    paths: SessionPaths,
    state: RuntimeState,
    *,
    observed_at: datetime | None = None,
) -> None:
    observed = _normalize_now(observed_at)
    _atomic_json(
        paths.heartbeat_path,
        {
            "schema_version": HEARTBEAT_SCHEMA_VERSION,
            "session_id": SESSION_ID,
            "status": state.status,
            "phase": state.phase,
            "message": state.message,
            "session_date": paths.session_date.isoformat(),
            "heartbeat_ist": observed.isoformat(),
            "pid": os.getpid(),
            "last_processed_minute": state.last_processed_minute,
            "preferred_app_count": state.preferred_app_count,
            "healthy_app_count": state.healthy_app_count,
            "healthy_apps": ",".join(state.healthy_apps),
            "unhealthy_apps": ",".join(state.unhealthy_apps),
            "app_pool_state": state.app_pool_state,
            "last_app_event_minute": state.last_app_event_minute,
            "last_app_usage": state.last_app_usage,
            "last_app_retry_count": state.last_app_retry_count,
            "last_app_failure_count": state.last_app_failure_count,
        },
    )


def write_manifest(paths: SessionPaths, *, source_module: Any) -> None:
    payload = {
        "schema_version": SESSION_SCHEMA_VERSION,
        "session_id": SESSION_ID,
        "session_title": SESSION_TITLE,
        "session_date": paths.session_date.isoformat(),
        "mode": MODE,
        "paper_only": True,
        "profiles": [profile.payload() for profile in profiles.PROFILES],
        "source_policy": "SHARED_FULL_MAPPED_UNIVERSE_5M_AND_UNION_COMPLETED_1M",
        "source_module": str(Path(source_module.__file__).resolve()),
        "source_module_sha256": _source_hash(Path(source_module.__file__)),
        "runtime_sources": {
            name: _source_hash(Path(__file__).resolve().with_name(name))
            for name in (
                "fno_multi_paper_profiles.py",
                "fno_multi_paper_engine.py",
                "fno_multi_paper_report.py",
                "fno_multi_paper_session.py",
            )
        },
        "execution_contract": {
            "paper_only": True,
            "broker_order_api": False,
            "same_confirmation_bar_fill": False,
            "completed_real_one_minute_only": True,
            "shared_source_independent_ledgers": True,
        },
        "parity": parity.validate_canonical_profiles(),
    }
    if paths.manifest_path.is_file():
        observed = json.loads(paths.manifest_path.read_text(encoding="utf-8"))
        if observed != payload:
            raise MultiPaperSessionError("run manifest changed for an existing session")
        return
    _atomic_json(paths.manifest_path, payload)


def persist_checkpoint(
    paths: SessionPaths,
    engine: paper_engine.MultiStrategyPaperEngine,
    state: RuntimeState,
    *,
    symbol_tokens: Mapping[str, int],
) -> None:
    _atomic_json(
        paths.checkpoint_path,
        {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "session_date": paths.session_date.isoformat(),
            "profile_fingerprints": {
                profile.key: profile.fingerprint for profile in profiles.PROFILES
            },
            "runtime_state": asdict(state),
            "symbol_tokens": dict(sorted((str(k), int(v)) for k, v in symbol_tokens.items())),
            "engine": engine.checkpoint(),
        },
    )


def load_checkpoint(
    paths: SessionPaths,
) -> tuple[paper_engine.MultiStrategyPaperEngine, RuntimeState, dict[str, int]] | None:
    if not paths.checkpoint_path.is_file():
        return None
    payload = json.loads(paths.checkpoint_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        raise MultiPaperSessionError("unsupported combined checkpoint schema")
    if payload.get("session_date") != paths.session_date.isoformat():
        raise MultiPaperSessionError("checkpoint session date changed")
    expected = {profile.key: profile.fingerprint for profile in profiles.PROFILES}
    if dict(payload.get("profile_fingerprints") or {}) != expected:
        raise MultiPaperSessionError("checkpoint profile identities changed")
    state = RuntimeState(**dict(payload["runtime_state"]))
    engine = paper_engine.MultiStrategyPaperEngine.from_checkpoint(payload["engine"])
    if (
        engine.last_processed_minute is not None
        and engine.last_processed_minute.isoformat() != state.last_processed_minute
    ):
        raise MultiPaperSessionError("engine and session checkpoint clocks differ")
    tokens = {str(key): int(value) for key, value in dict(payload["symbol_tokens"]).items()}
    return engine, state, tokens


def _is_regular_session(day: date) -> bool:
    # Reuse the reviewed calendar contract used by the proven V8 source path.
    from fno_v8_combined_paper_session import is_regular_nse_session

    return bool(is_regular_nse_session(day))


def _construct_supported(cls: type[Any], payload: Mapping[str, Any]) -> Any:
    params = inspect.signature(cls).parameters
    kwargs = {name: payload[name] for name in params if name in payload}
    return cls(**kwargs)


def _source_paths(source_module: Any, paths: SessionPaths) -> Any:
    return _construct_supported(
        source_module.LiveSourcePaths,
        {"session_date": paths.session_date, "root": paths.root},
    )


def _group_slot_rows(rows: Sequence[Mapping[str, Any]], signal_end: str) -> dict[str, list[Any]]:
    grouped = {f"{signal_end}_LONG": [], f"{signal_end}_SHORT": []}
    signal_time = _at(date.fromisoformat(str(next(iter(rows), {}).get("session_date", "1970-01-01"))), signal_end) if rows else None
    for raw in rows:
        row = dict(raw)
        side = str(row.get("side", "")).strip().upper()
        setup_id = str(row.get("setup_id", f"{signal_end}_{side}")).strip().upper()
        if setup_id not in grouped:
            continue
        row.setdefault("signal_time", row.get("signal_ts", row.get("signal_timestamp", signal_time)))
        grouped[setup_id].append(paper_engine.PaperCandidate.from_object(row))
    return grouped


def _register_slot(
    engine: paper_engine.MultiStrategyPaperEngine,
    result: Any,
    session_date: date,
    signal_end: str,
) -> None:
    rows = [dict(value) for value in result.rows]
    grouped: dict[str, list[Any]] = {
        f"{signal_end}_LONG": [],
        f"{signal_end}_SHORT": [],
    }
    for row in rows:
        side = str(row.get("side", "")).strip().upper()
        setup_id = str(row.get("setup_id", f"{signal_end}_{side}")).strip().upper()
        if setup_id not in grouped:
            continue
        row.setdefault("signal_time", _at(session_date, signal_end))
        grouped[setup_id].append(paper_engine.PaperCandidate.from_object(row))
    for side in ("LONG", "SHORT"):
        setup_id = f"{signal_end}_{side}"
        engine.register_candidates(setup_id, _at(session_date, signal_end), grouped[setup_id])


def _minute_bars(snapshot: Any) -> Mapping[str, Any]:
    if hasattr(snapshot, "bars_by_symbol"):
        return dict(snapshot.bars_by_symbol)
    rows = snapshot.frame.to_dict("records")
    return {str(row["symbol"]).strip().upper(): row for row in rows}


def _required_symbols_for_completed_minute(
    engine: paper_engine.MultiStrategyPaperEngine,
    completed_minute: datetime,
) -> list[str]:
    """Exclude candidates whose five-minute signal ends at this same minute.

    A just-registered S candidate cannot confirm or fill on S.  An older
    occurrence (including an open position) in the same symbol still requires
    the S candle, so filtering is record/state aware rather than symbol-blind.
    """

    required = set(engine.required_symbols())
    if not required or completed_minute.strftime("%H:%M") not in SIGNAL_ENDS:
        return sorted(required)
    active_states = {
        "MONITORING",
        "CONFIRMED_WAITING_CAP",
        "PENDING_STOP",
        "FILLED_OPEN",
    }
    needed: set[str] = set()
    for record in engine.records():
        symbol = str(record.get("symbol", "")).strip().upper()
        state = str(
            record.get("unconstrained_status", record.get("status", ""))
        )
        if symbol not in required or state not in active_states:
            continue
        raw_signal = record.get("signal_time")
        try:
            signal_time = (
                raw_signal
                if isinstance(raw_signal, datetime)
                else datetime.fromisoformat(str(raw_signal))
            )
        except (TypeError, ValueError) as exc:
            raise SourceIncompleteError(
                f"active candidate has invalid signal time: {symbol}"
            ) from exc
        if signal_time.tzinfo is None or signal_time.utcoffset() is None:
            raise SourceIncompleteError(
                f"active candidate has timezone-naive signal time: {symbol}"
            )
        if signal_time.astimezone(IST) < completed_minute:
            needed.add(symbol)
    return sorted(needed)


def run_preflight(
    session_date: date,
    *,
    paths: SessionPaths | None = None,
    authenticate_apps: bool = False,
    observed_now: datetime | None = None,
    source_module: Any | None = None,
) -> tuple[int, dict[str, Any]]:
    runtime_paths = paths or SessionPaths(session_date)
    observed = _normalize_now(observed_now)
    engine = paper_engine.MultiStrategyPaperEngine()
    state = RuntimeState(
        status="PREFLIGHT_OK",
        phase="PREFLIGHT",
        message="Configuration validated; no historical trades were replayed or fabricated.",
        source_complete=False,
    )
    payload: dict[str, Any] = {
        "session_id": SESSION_ID,
        "session_date": session_date.isoformat(),
        "paper_only": True,
        "regular_session": _is_regular_session(session_date),
        "profiles": {profile.key: profile.fingerprint for profile in profiles.PROFILES},
        "apps_authenticated": False,
    }
    try:
        payload["parity"] = parity.validate_canonical_profiles()
        if source_module is None:
            import fno_multi_paper_live_source as source_module
        if authenticate_apps:
            runtimes = tuple(source_module.authenticate_all_apps())
            _set_runtime_app_pool(state, runtimes)
            payload["apps_authenticated"] = True
            payload["app_names"] = [str(item.app_name) for item in runtimes]
            payload["app_pool_state"] = state.app_pool_state
            payload["healthy_app_count"] = state.healthy_app_count
        payload.update(ok=True, reason="PREFLIGHT_OK", observed_at_ist=observed.isoformat())
    except Exception as exc:
        state.status = "BLOCKED"
        state.message = f"Preflight failed: {type(exc).__name__}: {exc}"
        payload.update(ok=False, reason=state.message)
    status = publish_outputs(runtime_paths, engine, state, generated_at=observed)
    payload["status_path"] = str(runtime_paths.status_path)
    payload["combined_report_path"] = status["combined_report_path"]
    return (0 if payload["ok"] else 2), payload


def run_paper_session(
    session_date: date,
    *,
    paths: SessionPaths | None = None,
    now_provider: Callable[[], datetime] = now_ist,
    sleep_fn: Callable[[float], None] = time.sleep,
    source_module: Any | None = None,
    authenticator: Callable[..., Sequence[Any]] | None = None,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    max_iterations: int | None = None,
) -> int:
    observed = _normalize_now(now_provider())
    runtime_paths = paths or SessionPaths(session_date)
    mode = os.getenv("FNO_MULTI_PAPER_EXECUTION_MODE", MODE).strip().upper()
    if mode != MODE or not PAPER_ONLY:
        raise MultiPaperSessionError("combined V10/V11/V12 runtime is PAPER-only")
    if source_module is None:
        import fno_multi_paper_live_source as source_module
    parity.validate_canonical_profiles()

    if observed.date() != session_date or not _is_regular_session(session_date):
        state = RuntimeState(
            status="NOT_RUN",
            phase="SESSION_GATE",
            message="Not the requested regular NSE session; no trades were replayed or fabricated.",
        )
        publish_outputs(runtime_paths, paper_engine.MultiStrategyPaperEngine(), state, generated_at=observed)
        return 0
    first_confirmation_deadline = _at(session_date, "09:26") + timedelta(
        seconds=BOUNDARY_BUFFER_SECONDS
    )
    if observed >= first_confirmation_deadline and not runtime_paths.checkpoint_path.is_file():
        state = RuntimeState(
            status="NOT_RUN",
            phase="PROSPECTIVE_START_GATE",
            message="Started after the first S+1 deadline; retrospective paper trades are forbidden.",
        )
        publish_outputs(runtime_paths, paper_engine.MultiStrategyPaperEngine(), state, generated_at=observed)
        return 0

    with ProcessLock(runtime_paths.lock_path):
        source_paths = _source_paths(source_module, runtime_paths)
        # A terminal checkpoint is immutable session evidence, not a retry
        # invitation.  In particular, Task Scheduler may launch us once after
        # a non-zero exit; acknowledge the terminal state with exit 0 without
        # authenticating, mutating it, or resuming the reducer.
        restored = load_checkpoint(runtime_paths)
        if restored is not None:
            terminal_engine, terminal_state, _terminal_tokens = restored
            if terminal_state.status in TERMINAL_CHECKPOINT_STATES:
                publish_outputs(runtime_paths, terminal_engine, terminal_state)
                return 0

        auth = authenticator or source_module.authenticate_all_apps
        try:
            runtimes = tuple(auth())
        except Exception as exc:
            state = RuntimeState(
                status="BLOCKED",
                phase="APP_AUTHENTICATION",
                message=f"App authentication failed: {type(exc).__name__}: {exc}",
            )
            publish_outputs(runtime_paths, paper_engine.MultiStrategyPaperEngine(), state)
            return 2
        runtime_app_state = RuntimeState()
        _set_runtime_app_pool(runtime_app_state, runtimes)
        write_manifest(runtime_paths, source_module=source_module)

        if restored is None:
            engine = paper_engine.MultiStrategyPaperEngine()
            symbol_tokens: dict[str, int] = {}
            processed_end = _floor_due(observed)
            state = RuntimeState(
                status="RUNNING",
                phase="INITIALIZED",
                message=(
                    "Prospective PAPER session started with "
                    f"{runtime_app_state.healthy_app_count}/"
                    f"{runtime_app_state.preferred_app_count} healthy Kite apps."
                ),
                last_processed_minute=processed_end.isoformat(),
            )
            _set_runtime_app_pool(state, runtimes)
            engine.process_completed_minute(processed_end, {})
            persist_checkpoint(runtime_paths, engine, state, symbol_tokens=symbol_tokens)
        else:
            engine, state, symbol_tokens = restored
            state.status = "RUNNING"
            state.phase = (
                "FORWARD_ONLY_RECOVERY"
                if state.skipped_slots
                else "RESTORED_CHECKPOINT"
            )
            _set_runtime_app_pool(state, runtimes)
            if state.skipped_slots:
                state.data_incomplete = True
                state.message = (
                    "Forward-only partial PAPER recovery; skipped selection slots "
                    f"{list(state.skipped_slots)} will never be replayed."
                )
            else:
                state.message = (
                    "Prospective PAPER session restored from checkpoint with "
                    f"{state.healthy_app_count}/{state.preferred_app_count} healthy Kite apps."
                )
            assert engine.last_processed_minute is not None
            processed_end = engine.last_processed_minute

        publish_outputs(runtime_paths, engine, state)
        last_reported_minute = state.last_processed_minute
        iterations = 0
        try:
            while True:
                iterations += 1
                if max_iterations is not None and iterations > max_iterations:
                    state.phase = "TEST_LIMIT"
                    state.message = "Stopped at the injected test iteration limit."
                    publish_outputs(runtime_paths, engine, state)
                    return 0
                observed = _normalize_now(now_provider())
                if observed.date() != session_date:
                    raise SourceIncompleteError("runtime clock crossed the authorized session date")

                ingested = set(state.ingested_slots)
                skipped = set(state.skipped_slots)
                for signal_end in SIGNAL_ENDS:
                    signal_at = _at(session_date, signal_end)
                    ready_at = signal_at + timedelta(seconds=BOUNDARY_BUFFER_SECONDS)
                    deadline = signal_at + timedelta(minutes=1, seconds=BOUNDARY_BUFFER_SECONDS)
                    if signal_end in ingested or signal_end in skipped or observed < ready_at:
                        continue
                    if observed >= deadline:
                        raise ProspectiveStartMissed(
                            f"{signal_end} source was not sealed before its S+1 deadline"
                        )
                    state.phase = f"FIVE_MINUTE_SOURCE_{signal_end.replace(':', '')}"
                    publish_heartbeat(runtime_paths, state, observed_at=observed)
                    try:
                        result = source_module.build_and_publish_five_minute_source(
                            source_paths,
                            signal_end,
                            runtimes,
                            observed_at=observed,
                            clock=now_provider,
                        )
                    except source_module.SourceNotReadyError as exc:
                        state.phase = "WAITING_FIVE_MINUTE_SOURCE"
                        state.message = f"{signal_end} source not final yet: {exc}"
                        publish_heartbeat(runtime_paths, state)
                        continue
                    except (
                        source_module.SourceIncompleteError,
                        source_module.SourceContractError,
                    ) as exc:
                        raise SourceIncompleteError(str(exc)) from exc
                    decision_at = _normalize_now(now_provider())
                    if decision_at >= deadline:
                        raise ProspectiveStartMissed(
                            f"{signal_end} source work crossed its S+1 deadline"
                        )
                    _register_slot(engine, result, session_date, signal_end)
                    for symbol, token in dict(result.symbol_tokens).items():
                        prior = symbol_tokens.setdefault(str(symbol).upper(), int(token))
                        if prior != int(token):
                            raise SourceIncompleteError(f"cash token changed for {symbol}")
                    ingested.add(signal_end)
                    state.ingested_slots = tuple(sorted(ingested))
                    state.message = (
                        f"Registered shared full-universe five-minute source for {signal_end}."
                    )
                    persist_checkpoint(runtime_paths, engine, state, symbol_tokens=symbol_tokens)
                    publish_heartbeat(runtime_paths, state, observed_at=decision_at)

                due_end = min(_floor_due(observed), _at(session_date, SQUARE_OFF))
                next_end = processed_end + timedelta(minutes=1)
                while next_end <= due_end:
                    required = _required_symbols_for_completed_minute(
                        engine, next_end
                    )
                    recovery_snapshot_required = bool(next_end < due_end and required)
                    if recovery_snapshot_required:
                        marker_path = (
                            source_paths.minute_root
                            / f"minute_{next_end.strftime('%H%M')}.json"
                        )
                        if not marker_path.is_file():
                            raise SourceIncompleteError(
                                "active completed minute was missed and has no sealed "
                                f"recovery snapshot: {next_end.isoformat()}"
                            )
                    bars: Mapping[str, Any] = {}
                    if required:
                        token_contract = {
                            symbol: int(symbol_tokens.get(symbol, 0)) for symbol in required
                        }
                        missing_tokens = [symbol for symbol, token in token_contract.items() if token <= 0]
                        if missing_tokens:
                            raise SourceIncompleteError(
                                f"active symbols have no frozen cash token: {missing_tokens}"
                            )
                        state.phase = "UNION_COMPLETED_ONE_MINUTE_FETCH"
                        publish_heartbeat(runtime_paths, state)
                        try:
                            snapshot = source_module.fetch_and_publish_union_minute(
                                source_paths,
                                token_contract,
                                runtimes,
                                next_end,
                                observed_at=_normalize_now(now_provider()),
                            )
                        except (
                            source_module.SourceNotReadyError,
                            source_module.SourceIncompleteError,
                            source_module.SourceContractError,
                        ) as exc:
                            raise SourceIncompleteError(str(exc)) from exc
                        bars = _minute_bars(snapshot)
                        _record_app_usage(
                            state,
                            dict(getattr(snapshot, "marker", {})),
                            next_end,
                        )
                        if recovery_snapshot_required and not bool(snapshot.reused):
                            raise SourceIncompleteError(
                                "crash recovery attempted to refetch a missed active minute"
                            )
                        missing = sorted(set(required) - set(bars))
                        engine.process_completed_minute(next_end, bars)
                        processed_end = next_end
                        state.last_processed_minute = next_end.isoformat()
                        state.completed_minutes += 1
                        if missing or dict(getattr(snapshot, "marker", {})).get("complete") is not True:
                            state.data_incomplete = True
                            persist_checkpoint(runtime_paths, engine, state, symbol_tokens=symbol_tokens)
                            raise SourceIncompleteError(
                                f"union one-minute evidence is incomplete at {next_end.isoformat()}: {missing}"
                            )
                    else:
                        engine.process_completed_minute(next_end, {})
                        processed_end = next_end
                        state.last_processed_minute = next_end.isoformat()
                        state.completed_minutes += 1

                    state.phase = "CHRONOLOGICAL_PAPER_REDUCER"
                    persist_checkpoint(runtime_paths, engine, state, symbol_tokens=symbol_tokens)
                    if next_end.time() == day_time.fromisoformat(SQUARE_OFF):
                        state.status = "COMPLETE"
                        state.phase = "EXACT_1530_COMPLETE"
                        state.source_complete = bool(
                            set(state.ingested_slots) == set(SIGNAL_ENDS)
                            and not state.skipped_slots
                            and not state.data_incomplete
                            and not engine.required_symbols()
                        )
                        state.message = (
                            "Exact 15:30 PAPER session completed."
                            if state.source_complete
                            else "15:30 reached but source/state completeness failed."
                        )
                        if not state.source_complete:
                            state.status = "DEGRADED"
                        persist_checkpoint(runtime_paths, engine, state, symbol_tokens=symbol_tokens)
                        publish_outputs(runtime_paths, engine, state)
                        return 0 if state.source_complete else 2
                    next_end += timedelta(minutes=1)

                if state.last_processed_minute != last_reported_minute:
                    publish_outputs(runtime_paths, engine, state)
                    last_reported_minute = state.last_processed_minute
                else:
                    publish_heartbeat(runtime_paths, state)
                sleep_fn(max(0.05, float(poll_seconds)))
        except (ProspectiveStartMissed, SourceIncompleteError, MultiPaperSessionError) as exc:
            state.status = "BLOCKED"
            state.phase = "FAIL_CLOSED"
            state.data_incomplete = True
            state.message = f"{type(exc).__name__}: {exc}"
            persist_checkpoint(runtime_paths, engine, state, symbol_tokens=symbol_tokens)
            publish_outputs(runtime_paths, engine, state)
            return 2
        except KeyboardInterrupt:
            state.status = "DEGRADED"
            state.phase = "OPERATOR_INTERRUPT"
            state.data_incomplete = True
            state.message = "Operator interrupted the prospective PAPER session."
            persist_checkpoint(runtime_paths, engine, state, symbol_tokens=symbol_tokens)
            publish_outputs(runtime_paths, engine, state)
            return 130
        except Exception as exc:
            # Provider/library and filesystem failures are not guaranteed to
            # inherit one of the reviewed source exceptions.  They must still
            # stop the PAPER reducer and replace a stale RUNNING dashboard
            # state with explicit fail-closed evidence.  Persistence is best
            # effort here because the unexpected error itself may be an I/O
            # failure; either way the task exits non-zero and never continues
            # with partial or invented market data.
            state.status = "BLOCKED"
            state.phase = "UNEXPECTED_FAIL_CLOSED"
            state.data_incomplete = True
            state.message = f"Unexpected {type(exc).__name__}: {exc}"
            print(
                f"[FNO-MULTI-PAPER][UNEXPECTED_FAIL_CLOSED] {type(exc).__name__}: {exc}",
                flush=True,
            )
            try:
                persist_checkpoint(
                    runtime_paths,
                    engine,
                    state,
                    symbol_tokens=symbol_tokens,
                )
            except Exception as persist_exc:
                print(
                    "[FNO-MULTI-PAPER][CHECKPOINT_FAILED] "
                    f"{type(persist_exc).__name__}: {persist_exc}",
                    flush=True,
                )
            try:
                publish_outputs(runtime_paths, engine, state)
            except Exception as publish_exc:
                print(
                    "[FNO-MULTI-PAPER][STATUS_PUBLISH_FAILED] "
                    f"{type(publish_exc).__name__}: {publish_exc}",
                    flush=True,
                )
            return 2


def _parse_day(value: str | None) -> date:
    return date.fromisoformat(value) if value else now_ist().date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=SESSION_TITLE)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the real prospective PAPER session")
    run.add_argument("--session-date")
    run.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    preflight = commands.add_parser(
        "preflight", help="publish honest no-trade readiness views"
    )
    preflight.add_argument("--session-date")
    preflight.add_argument("--authenticate-apps", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    session_date = _parse_day(args.session_date)
    if args.command == "preflight":
        code, payload = run_preflight(
            session_date, authenticate_apps=bool(args.authenticate_apps)
        )
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return code
    return run_paper_session(
        session_date, poll_seconds=max(0.05, float(args.poll_seconds))
    )


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "HEARTBEAT_PATH",
    "LATEST_COMBINED_REPORT",
    "LATEST_PROFILE_REPORTS",
    "LOCK_PATH",
    "ROOT",
    "RuntimeState",
    "SESSION_ID",
    "SESSION_TITLE",
    "STATUS_PATH",
    "SessionPaths",
    "load_checkpoint",
    "main",
    "persist_checkpoint",
    "publish_outputs",
    "run_paper_session",
    "run_preflight",
]
