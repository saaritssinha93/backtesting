"""Independent same-session orchestrator for FNO V8-Combined PAPER.

The scheduled entry point in this module has deliberately narrow authority:

* validate a one-session activation permit and kill switch before discovering
  any Kite credential;
* independently rebuild the full mapped-universe V8 gates/ranking from strict
  current sources, while retaining the current-day finalized V6 five-minute
  scanner only as a post-registration diagnostic dependency;
* fetch exact completed NSE-equity one-minute candles through all eight apps;
* reduce both sides and all five setup slots in one chronological PAPER book;
* write an isolated, replayable evidence/checkpoint/report trail.

There is no LIVE mode and no broker order-placement seam in this file.  An
absent approval is a normal staged state: ``run`` reports ``DISABLED`` and
returns success without touching credentials.  ``preflight`` is intended for
the separately approval-gated scheduler cutover and can additionally require
that all eight market-data apps authenticate.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import importlib
import io
import inspect
import json
import math
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, time as day_time, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v8_combined_paper_config as config
import fno_v8_combined_paper_control as control
import fno_v8_combined_paper_market_data as market_data
from eqidv2_runtime_paths import RUNTIME_STATUS_DIR


SESSION_ID = "fno_v8_combined_paper"
SESSION_SCHEMA_VERSION = "fno_v8_combined_paper_session_v1"
EVIDENCE_SCHEMA_VERSION = "fno_v8_combined_paper_evidence_v1"
CHECKPOINT_SCHEMA_VERSION = "fno_v8_combined_paper_checkpoint_envelope_v1"
SOURCE_POLICY_VERSION = (
    "independent_all_mapped_strict_cash_exact_oi_approved_kite_pool_5x1m_v5"
)
MIN_HEALTHY_KITE_APPS = 7

# The source contract is literal on purpose.  Importing V6 live code would
# create/shared-state coupling and could change source semantics underneath an
# already-approved V8 runtime bundle.
V6_SCANNER_SCHEMA_VERSION = "fno_v6_scanner_5m_hybrid_v3"
V6_SCANNER_STRATEGY_VERSION = "FNO_V6_BEST_NET_CASH_EQUITY_20260811"
V6_SCANNER_STRATEGY_FINGERPRINT = (
    "0c0380f1a505bd5b59af1b07460fef40a7548fcc90b4e541f741625f3aa30a67"
)
V6_SCANNER_DATA_CONTRACT = hybrid.DATA_CONTRACT_VERSION

STATUS_PATH = RUNTIME_STATUS_DIR / f"{SESSION_ID}.status"
HEARTBEAT_PATH = RUNTIME_STATUS_DIR / f"{SESSION_ID}.heartbeat"
DEFAULT_V6_SCANNER_ROOT = config.ROOT.parent / "v6_live" / "scanner_5m"
# Do not inherit the generic live2 default on direct/manual invocation.  This
# is the same current writer root explicitly used by the V6 scanner runner.
DEFAULT_FIVE_MINUTE_ROOT = common.FNO_ROOT.parent / "stocks_indicators_5min_eq_live"
DEFAULT_FUTURES_FIVE_MINUTE_ROOT = common.RAW_CONTRACT_DIR
DEFAULT_FUTURES_SLOT_ROOT = common.FETCH_SLOT_DIR
DEFAULT_NEAR_MONTH_UNIVERSE_PATH = common.UNIVERSE_DIR / "latest_near_month.parquet"
DEFAULT_CASH_SLOT_ROOT = common.CASH_SLOT_DIR

FIRST_SIGNAL_END = day_time.fromisoformat("09:25")
FIRST_CONFIRMATION_END = day_time.fromisoformat("09:26")
SQUARE_OFF_TIME = day_time.fromisoformat(config.SQUARE_OFF)
BOUNDARY_BUFFER_SECONDS = market_data.DEFAULT_BOUNDARY_BUFFER_SEC
DEFAULT_POLL_SECONDS = 1.0

# Frozen official regular-session calendar used by the V8 research contract.
# A new calendar year must be reviewed, added here, and therefore changes the
# runtime bundle hash before any permit can authorize it.
NSE_FO_TRADING_HOLIDAYS_2026 = frozenset(
    date.fromisoformat(value)
    for value in (
        "2026-01-15",
        "2026-01-26",
        "2026-03-03",
        "2026-03-26",
        "2026-03-31",
        "2026-04-03",
        "2026-04-14",
        "2026-05-01",
        "2026-05-28",
        "2026-06-26",
        "2026-09-14",
        "2026-10-02",
        "2026-10-20",
        "2026-11-10",
        "2026-11-24",
        "2026-12-25",
    )
)
NSE_REGULAR_SPECIAL_SESSIONS_2026 = frozenset({date(2026, 2, 1)})
NSE_NONSTANDARD_SESSIONS_2026 = frozenset({date(2026, 11, 8)})


class SessionContractError(RuntimeError):
    """A fail-closed session, source, or persistence contract violation."""


class SourceNotReadyError(SessionContractError):
    """The upstream scanner has not published the exact slot yet."""


class SourceIncompleteError(SessionContractError):
    """The upstream slot exists but is not safe to use as a complete source."""


class AlreadyRunningError(SessionContractError):
    """Another V8 paper writer owns the independent process lock."""


@dataclass(frozen=True)
class SessionPaths:
    session_date: date
    root: Path = config.ROOT
    scanner_root: Path = DEFAULT_V6_SCANNER_ROOT
    five_minute_root: Path = DEFAULT_FIVE_MINUTE_ROOT
    futures_five_minute_root: Path = DEFAULT_FUTURES_FIVE_MINUTE_ROOT
    futures_slot_root: Path = DEFAULT_FUTURES_SLOT_ROOT
    near_month_universe_path: Path = DEFAULT_NEAR_MONTH_UNIVERSE_PATH
    cash_slot_root: Path = DEFAULT_CASH_SLOT_ROOT
    status_path: Path = STATUS_PATH
    heartbeat_path: Path = HEARTBEAT_PATH
    latest_report_path: Path = config.LATEST_REPORT_PATH
    lock_path: Path = config.LOCK_PATH

    @property
    def day_session_root(self) -> Path:
        return self.root / "sessions" / self.session_date.isoformat()

    @property
    def day_evidence_root(self) -> Path:
        return self.root / "evidence" / self.session_date.isoformat()

    @property
    def day_checkpoint_root(self) -> Path:
        return self.root / "checkpoints" / self.session_date.isoformat()

    @property
    def source_root(self) -> Path:
        return self.day_evidence_root / "five_minute_source"

    @property
    def candidate_root(self) -> Path:
        return self.day_evidence_root / "candidate_books"

    @property
    def minute_root(self) -> Path:
        return self.day_evidence_root / "one_minute"

    @property
    def cash_signal_audit_root(self) -> Path:
        return self.day_evidence_root / "cash_signal_5x1m_audit"

    @property
    def oi_superset_audit_root(self) -> Path:
        return self.day_evidence_root / "oi_superset_grid_audit"

    @property
    def independent_candidate_source_root(self) -> Path:
        return self.day_evidence_root / "independent_v8_candidate_source"

    @property
    def strict_cash_source_root(self) -> Path:
        return self.day_evidence_root / "strict_cash_universe_source"

    @property
    def event_root(self) -> Path:
        return self.day_evidence_root / "events"

    @property
    def trades_csv_path(self) -> Path:
        return self.day_session_root / f"v8_combined_paper_trades_{self.session_date}.csv"

    @property
    def session_report_path(self) -> Path:
        return self.day_session_root / f"v8_combined_paper_report_{self.session_date}.md"

    @property
    def latest_checkpoint_path(self) -> Path:
        return self.day_checkpoint_root / "latest.json"


@dataclass
class SessionTelemetry:
    state: str = "STARTING"
    phase: str = "BOOT"
    activation_reason: str = ""
    runtime_bundle_sha256: str = ""
    permit_id: str = ""
    app_roster_sha256: str = ""
    slots: dict[str, dict[str, Any]] = field(default_factory=dict)
    completed_minutes: int = 0
    incomplete_minutes: int = 0
    last_completed_minute: str = ""
    data_incomplete: bool = False
    messages: list[str] = field(default_factory=list)

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)


def now_ist() -> datetime:
    return datetime.now(config.IST)


def is_regular_nse_session(session_date: date) -> bool:
    """Fail closed outside the frozen 2026 regular-session calendar."""

    if session_date.year != 2026:
        return False
    if session_date in NSE_NONSTANDARD_SESSIONS_2026:
        return False
    if session_date in NSE_REGULAR_SPECIAL_SESSIONS_2026:
        return True
    return session_date.weekday() < 5 and session_date not in NSE_FO_TRADING_HOLIDAYS_2026


def _normalize_now(value: datetime | None) -> datetime:
    observed = value or now_ist()
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise SessionContractError("session timestamps must be timezone-aware")
    return observed.astimezone(config.IST)


def _slot_datetime(session_date: date, hhmm: str) -> datetime:
    return datetime.combine(session_date, day_time.fromisoformat(hhmm), tzinfo=config.IST)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date, pd.Timestamp, Path)):
        return str(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _json_bytes(payload: Any, *, pretty: bool = True) -> bytes:
    return (
        json.dumps(
            _jsonable(payload),
            indent=2 if pretty else None,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
            separators=None if pretty else (",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    temporary = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _write_immutable_bytes(path: Path, payload: bytes) -> str:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    incoming = _sha256_bytes(payload)
    if destination.exists():
        if _sha256_file(destination) != incoming:
            raise SessionContractError(f"immutable evidence collision: {destination}")
        return incoming
    fd, raw = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent)
    )
    temporary = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if _sha256_file(destination) != incoming:
                raise SessionContractError(f"immutable evidence collision: {destination}")
        return incoming
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _write_immutable_json(path: Path, payload: Mapping[str, Any]) -> str:
    return _write_immutable_bytes(path, _json_bytes(payload))


def _write_kv(path: Path, payload: Mapping[str, Any]) -> None:
    def clean(value: Any) -> str:
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(_jsonable(value), ensure_ascii=True, separators=(",", ":"))
        return " ".join(str(value).replace("=", ":").split())

    text = "".join(f"{key}={clean(value)}\n" for key, value in payload.items())
    _atomic_write_bytes(path, text.encode("utf-8"))


def publish_runtime_state(
    paths: SessionPaths,
    telemetry: SessionTelemetry,
    *,
    heartbeat_only: bool = False,
) -> None:
    stamp = now_ist().isoformat(timespec="seconds")
    common_payload = {
        "session": SESSION_ID,
        "session_date_ist": paths.session_date.isoformat(),
        "state": telemetry.state,
        "phase": telemetry.phase,
        "mode": config.MODE,
        "paper_only": True,
        "ts": stamp,
        "activation_reason": telemetry.activation_reason,
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "runtime_bundle_sha256": telemetry.runtime_bundle_sha256,
        "last_completed_minute": telemetry.last_completed_minute,
        "completed_minutes": telemetry.completed_minutes,
        "incomplete_minutes": telemetry.incomplete_minutes,
        "data_incomplete": telemetry.data_incomplete,
    }
    _write_kv(paths.heartbeat_path, common_payload)
    if not heartbeat_only:
        _write_kv(paths.status_path, {"status": telemetry.state, **common_payload})


class ProcessLock:
    """One cross-process writer lock; the file itself may safely remain."""

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
            else:  # pragma: no cover - exercised on non-Windows CI only
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise AlreadyRunningError(f"V8 paper writer lock is held: {self.path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(
            _json_bytes(
                {
                    "pid": os.getpid(),
                    "session": SESSION_ID,
                    "acquired_at_ist": now_ist().isoformat(timespec="seconds"),
                },
                pretty=False,
            )
        )
        handle.flush()
        self._handle = handle
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
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


def _scanner_slot_path(paths: SessionPaths, signal_end: str) -> Path:
    return (
        paths.scanner_root
        / paths.session_date.isoformat()
        / f"slot_{signal_end.replace(':', '')}.json"
    )


def _parse_aware_ist(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise SourceIncompleteError(f"invalid {field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SourceIncompleteError(f"naive {field}")
    return parsed.astimezone(config.IST)


def load_finalized_v6_scanner_slot(
    paths: SessionPaths,
    signal_end: str,
    *,
    observed_at: datetime | None = None,
) -> tuple[dict[str, Any], bytes, str]:
    """Load and validate one exact current-date V6 diagnostic snapshot."""

    source_path = _scanner_slot_path(paths, signal_end)
    try:
        raw = source_path.read_bytes()
    except FileNotFoundError as exc:
        raise SourceNotReadyError(f"scanner slot not published: {source_path}") from exc
    except OSError as exc:
        raise SourceIncompleteError(f"scanner slot unreadable: {source_path}") from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceIncompleteError("scanner slot is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise SourceIncompleteError("scanner slot must be a JSON object")

    exact_checks = (
        (payload.get("schema_version") == V6_SCANNER_SCHEMA_VERSION, "schema"),
        (payload.get("strategy_version") == V6_SCANNER_STRATEGY_VERSION, "strategy version"),
        (
            payload.get("strategy_fingerprint") == V6_SCANNER_STRATEGY_FINGERPRINT,
            "strategy fingerprint",
        ),
        (payload.get("session_date") == paths.session_date.isoformat(), "session date"),
        (payload.get("signal_end") == signal_end, "signal end"),
        (payload.get("data_contract") == V6_SCANNER_DATA_CONTRACT, "data contract"),
        (payload.get("price_volume_indicator_source") == "NSE_EQUITY", "price source"),
        (payload.get("oi_source") == "NFO_FUTURE", "OI source"),
        (
            payload.get("equity_five_minute_quality")
            == "COMPLETED_REAL_END_LABELLED_ONLY",
            "completed five-minute quality",
        ),
        (payload.get("state") == "SUCCESS", "complete state"),
        (int(payload.get("contracts_expected", -1)) > 0, "contracts expected"),
        (
            int(payload.get("contracts_evaluated", -2))
            == int(payload.get("contracts_expected", -1)),
            "contracts evaluated",
        ),
        (int(payload.get("contracts_missing_slot", -1)) == 0, "contracts missing slot"),
        (not list(payload.get("missing_contracts") or []), "missing contracts"),
        (int(payload.get("contracts_unexpected_missing", -1)) == 0, "unexpected missing"),
        (int(payload.get("contracts_skipped_no_candle", -1)) == 0, "skipped no-candle contracts"),
        (
            not list(payload.get("skipped_no_candle_symbols") or []),
            "skipped no-candle symbols",
        ),
        (
            not list(payload.get("skipped_no_candle_contracts") or []),
            "skipped no-candle contracts",
        ),
        (int(payload.get("invalid_candidates", -1)) == 0, "invalid candidates"),
        (
            not list(payload.get("unknown_verified_no_candle_symbols") or []),
            "unknown no-candle symbols",
        ),
    )
    failures = [label for passed, label in exact_checks if not passed]
    if failures:
        raise SourceIncompleteError("scanner slot contract mismatch: " + ", ".join(failures))
    published_at = _parse_aware_ist(payload.get("published_at_ist"), "published_at_ist")
    signal_at = _slot_datetime(paths.session_date, signal_end)
    if published_at.date() != paths.session_date or published_at < signal_at:
        raise SourceIncompleteError("scanner publication timestamp is outside its slot")
    if observed_at is not None and published_at > _normalize_now(observed_at):
        raise SourceIncompleteError("scanner publication timestamp is in the future")
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        raise SourceIncompleteError("scanner candidates must be a list")
    declared = int(payload.get("long_candidates", -1)) + int(
        payload.get("short_candidates", -1)
    )
    if declared != len(candidates):
        raise SourceIncompleteError("scanner candidate counts do not reconcile")
    identities = [
        (
            str(row.get("tradingsymbol", "")).strip().upper(),
            str(row.get("side", "")).strip().upper(),
        )
        for row in candidates
        if isinstance(row, Mapping)
    ]
    if (
        len(identities) != len(candidates)
        or any(not symbol or side not in {"LONG", "SHORT"} for symbol, side in identities)
    ):
        raise SourceIncompleteError("scanner candidate identity is invalid")
    if len(identities) != len(set(identities)):
        raise SourceIncompleteError("scanner candidates contain duplicate cash symbol/side")
    if (
        sum(side == "LONG" for _, side in identities)
        != int(payload.get("long_candidates", -1))
        or sum(side == "SHORT" for _, side in identities)
        != int(payload.get("short_candidates", -1))
    ):
        raise SourceIncompleteError("scanner side counts do not reconcile")
    digest = _sha256_bytes(raw)
    return payload, raw, digest


def _safe_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise SourceIncompleteError(f"non-numeric candidate value: {value!r}") from exc
    if not math.isfinite(result):
        raise SourceIncompleteError(f"non-finite candidate value: {value!r}")
    return result


def _ist_series(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize(config.IST)
    return parsed.dt.tz_convert(config.IST)


def prove_v6_oi_shift_is_exact_for_stock_universe(
    paths: SessionPaths,
    signal_end: str,
    *,
    observed_at: datetime,
    max_workers: int = config.REQUIRED_KITE_APPS,
) -> dict[str, Any]:
    """Prove exact S/S-5 OI lineage for every mapped stock future.

    The final futures marker proves which complete universe was written; this
    audit checks that the immediate sorted predecessor of the exact S row is
    exactly S-5 in *every* mapped stock-future file.  Those rows are the
    independent V8 authority and also make the later V6 scalar comparison
    meaningful.  It is deliberately read-only; the caller seals its result
    before prospective registration.
    """

    signal_at = _slot_datetime(paths.session_date, signal_end)
    marker_path = (
        paths.futures_slot_root
        / f"slot_{signal_at.strftime('%Y%m%d_%H%M')}.json"
    )
    try:
        marker_raw = marker_path.read_bytes()
    except FileNotFoundError as exc:
        raise SourceNotReadyError(
            f"final futures slot marker not published: {marker_path}"
        ) from exc
    except OSError as exc:
        raise SourceIncompleteError(
            f"final futures slot marker unreadable: {marker_path}"
        ) from exc
    try:
        marker = json.loads(marker_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceIncompleteError("final futures slot marker is invalid JSON") from exc
    if not isinstance(marker, Mapping):
        raise SourceIncompleteError("final futures slot marker must be an object")
    slot_at = _parse_aware_ist(marker.get("slot_ist"), "futures_slot_ist")
    published_at = _parse_aware_ist(
        marker.get("published_at_ist"), "futures_published_at_ist"
    )
    observed = _normalize_now(observed_at)
    if published_at > observed:
        # A valid marker that the caller's clock has not reached is not yet an
        # observable source; retry without treating it as corrupt.
        raise SourceNotReadyError("final futures slot marker is not observable yet")

    marker_symbols = [
        str(value).strip().upper()
        for value in marker.get("stock_written_symbols") or []
        if str(value).strip()
    ]
    marker_checks = (
        (marker.get("schema_version") == common.FNO_FETCH_SLOT_SCHEMA_VERSION, "schema"),
        (marker.get("source") == "final", "source"),
        (marker.get("state") == "SUCCESS", "state"),
        (marker.get("complete") is True, "complete"),
        (marker.get("attempt_complete") is True, "attempt complete"),
        (marker.get("outcome_symbol_set_complete") is True, "outcome set"),
        (
            marker.get("stock_outcome_symbol_set_complete") is True,
            "stock outcome set",
        ),
        (marker.get("stock_complete") is True, "stock complete"),
        (marker.get("stock_state") == "SUCCESS", "stock state"),
        (slot_at == signal_at, "slot"),
        (published_at.date() == paths.session_date, "publication date"),
        (published_at >= signal_at, "publication chronology"),
        (marker.get("universe_date") == paths.session_date.isoformat(), "universe date"),
        (int(marker.get("stock_contracts_expected", -1)) > 0, "stock expected"),
        (
            int(marker.get("stock_contracts_written", -2))
            == int(marker.get("stock_contracts_expected", -1)),
            "stock written",
        ),
        (
            len(marker_symbols) == int(marker.get("stock_contracts_expected", -1)),
            "stock symbol count",
        ),
        (len(marker_symbols) == len(set(marker_symbols)), "stock symbol uniqueness"),
        (int(marker.get("stock_no_candle_count", -1)) == 0, "stock no-candle"),
        (not list(marker.get("stock_no_candle_symbols") or []), "stock no-candle list"),
        (
            int(marker.get("stock_verified_no_candle_count", -1)) == 0,
            "verified stock no-candle",
        ),
        (
            not list(marker.get("stock_verified_no_candle_symbols") or []),
            "verified stock no-candle list",
        ),
        (
            not list(marker.get("stock_unverified_no_candle_symbols") or []),
            "unverified stock no-candle list",
        ),
        (int(marker.get("stock_invalid_data_count", -1)) == 0, "invalid stock data"),
        (
            not list(marker.get("stock_invalid_data_symbols") or []),
            "invalid stock data list",
        ),
        (int(marker.get("stock_failed_count", -1)) == 0, "failed stocks"),
        (not list(marker.get("stock_failed_symbols") or []), "failed stock list"),
        (
            _approved_app_names(
                marker.get("apps_used"), minimum=1, require_order=False
            )
            is not None,
            "approved Kite provenance",
        ),
    )
    marker_failures = [label for passed, label in marker_checks if not passed]
    if marker_failures:
        raise SourceIncompleteError(
            "final futures stock-slot contract mismatch: "
            + ", ".join(marker_failures)
        )

    universe_path = Path(paths.near_month_universe_path)
    try:
        universe = pd.read_parquet(universe_path)
    except Exception as exc:
        raise SourceIncompleteError(
            f"near-month universe unreadable: {universe_path}"
        ) from exc
    required_universe = {
        "tradingsymbol",
        "instrument_token",
        "underlying",
        "is_index_future",
        "master_date",
        "expiry",
        "contract_month",
        "lot_size",
        "tick_size",
        "equity_symbol",
        "equity_instrument_token",
        "equity_tick_size",
        "data_contract",
    }
    if universe.empty or not required_universe.issubset(universe.columns):
        raise SourceIncompleteError("near-month universe lacks required mapping columns")
    master_dates = set(
        pd.to_datetime(universe["master_date"], errors="coerce").dropna().dt.date
    )
    if master_dates != {paths.session_date}:
        raise SourceIncompleteError("near-month universe is not exact current-date")

    def is_stock(value: Any) -> bool:
        if isinstance(value, str):
            return value.strip().lower() in {"false", "0", "no", "off"}
        return value is not None and not bool(value)

    stocks = universe.loc[universe["is_index_future"].map(is_stock)].copy()
    contracts: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    seen_tokens: set[int] = set()
    for raw in stocks.to_dict("records"):
        symbol = str(raw.get("tradingsymbol", "")).strip().upper()
        underlying = str(raw.get("underlying", "")).strip().upper()
        try:
            token = int(raw.get("instrument_token", 0) or 0)
            equity_token = int(raw.get("equity_instrument_token", 0) or 0)
        except (TypeError, ValueError) as exc:
            raise SourceIncompleteError("near-month stock token is invalid") from exc
        equity_symbol = str(raw.get("equity_symbol", "")).strip().upper()
        expiry = pd.to_datetime(raw.get("expiry"), errors="coerce")
        contract_month = str(raw.get("contract_month", "")).strip()
        futures_tick_size = _safe_float(raw.get("tick_size"))
        equity_tick_size = _safe_float(raw.get("equity_tick_size"))
        lot_size = int(_safe_float(raw.get("lot_size")))
        if (
            not symbol
            or not underlying
            or token <= 0
            or not equity_symbol
            or equity_token <= 0
            or pd.isna(expiry)
            or contract_month != pd.Timestamp(expiry).strftime("%Y-%m")
            or futures_tick_size <= 0
            or equity_tick_size <= 0
            or lot_size <= 0
        ):
            raise SourceIncompleteError("near-month stock mapping is incomplete")
        if symbol in seen_symbols or token in seen_tokens:
            raise SourceIncompleteError("near-month stock mapping is not one-to-one")
        seen_symbols.add(symbol)
        seen_tokens.add(token)
        contracts.append(
            {
                "tradingsymbol": symbol,
                "instrument_token": token,
                "underlying": underlying,
                "expiry": pd.Timestamp(expiry).date().isoformat(),
                "contract_month": contract_month,
                "lot_size": lot_size,
                "tick_size": futures_tick_size,
                "equity_symbol": equity_symbol,
                "equity_instrument_token": equity_token,
                "equity_tick_size": equity_tick_size,
                "data_contract": str(raw.get("data_contract", "")),
            }
        )
    expected_symbols = sorted(seen_symbols)
    if expected_symbols != sorted(marker_symbols):
        raise SourceIncompleteError(
            "final futures marker stock symbols do not equal current mapped universe"
        )
    if marker.get("stock_symbol_set_sha256") != common.symbol_set_sha256(expected_symbols):
        raise SourceIncompleteError("final futures marker stock symbol hash mismatch")

    expected_cash_symbols = sorted({str(item["equity_symbol"]) for item in contracts})
    if len(expected_cash_symbols) != len(contracts):
        raise SourceIncompleteError("near-month cash mapping is not one-to-one")
    cash_universe_sha256 = common.symbol_set_sha256(expected_cash_symbols)
    cash_marker = load_final_cash_slot_marker(
        signal_at,
        paths.cash_slot_root,
        observed_at=observed,
    )
    cash_marker_checks = (
        int(cash_marker.get("tickers_expected", -1)) >= len(contracts),
        int(cash_marker.get("current_symbol_count", -1))
        == int(cash_marker.get("tickers_expected", -2)),
        int(cash_marker.get("fno_equity_expected", -1)) == len(contracts),
        cash_marker.get("fno_equity_universe_sha256") == cash_universe_sha256,
    )
    if not all(cash_marker_checks):
        raise SourceIncompleteError(
            "cash final marker does not bind the mapped stock universe"
        )

    previous_at = signal_at - timedelta(minutes=5)
    required_raw = [
        "timestamp",
        "candle_start",
        "underlying",
        "tradingsymbol",
        "instrument_token",
        "is_index_future",
        "expiry",
        "contract_month",
        "lot_size",
        "tick_size",
        "oi",
        "quality_state",
        "source",
        "data_version",
    ]

    def prove_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
        symbol = str(contract["tradingsymbol"])
        token = int(contract["instrument_token"])
        underlying = str(contract["underlying"])
        expiry = str(contract["expiry"])
        contract_month = str(contract["contract_month"])
        lot_size = int(contract["lot_size"])
        futures_tick_size = float(contract["tick_size"])
        source_path = (
            Path(paths.futures_five_minute_root)
            / f"{common.safe_contract_stem(symbol)}_5minute.parquet"
        )
        try:
            # One physical read both binds every source byte and supplies the
            # parquet decoder.  Intake later verifies the sealed proof hash;
            # it does not reread 208 mutable files in the S+1 critical path.
            source_raw = source_path.read_bytes()
            frame = pd.read_parquet(io.BytesIO(source_raw), columns=required_raw)
        except Exception as exc:
            raise SourceIncompleteError(
                f"universe OI proof cannot read {symbol}: {source_path}"
            ) from exc
        if frame.empty:
            raise SourceIncompleteError(f"universe OI proof source is empty: {symbol}")
        frame = frame.copy()
        frame["ts"] = _ist_series(frame["timestamp"])
        frame["start"] = _ist_series(frame["candle_start"])
        ordered = frame.loc[frame["ts"].notna()].sort_values(
            "ts", kind="stable"
        ).reset_index(drop=True)
        exact_s = ordered.index[ordered["ts"].eq(pd.Timestamp(signal_at))].tolist()
        exact_prev = ordered.index[ordered["ts"].eq(pd.Timestamp(previous_at))].tolist()
        if len(exact_s) != 1 or len(exact_prev) != 1:
            raise SourceIncompleteError(
                f"universe OI proof requires unique exact S/S-5 rows: {symbol}"
            )
        current_index = exact_s[0]
        if current_index <= 0 or current_index - 1 != exact_prev[0]:
            predecessor = (
                "NONE"
                if current_index <= 0
                else pd.Timestamp(ordered.iloc[current_index - 1]["ts"]).isoformat()
            )
            raise SourceIncompleteError(
                f"V6 OI shift predecessor is not exact S-5 for {symbol}: {predecessor}"
            )
        pair = [ordered.iloc[exact_prev[0]], ordered.iloc[current_index]]
        row_evidence: list[dict[str, Any]] = []
        for row, wanted in zip(pair, (previous_at, signal_at)):
            row_symbol = str(row.get("tradingsymbol", "")).strip().upper()
            row_underlying = str(row.get("underlying", "")).strip().upper()
            try:
                row_token = int(row.get("instrument_token", 0) or 0)
            except (TypeError, ValueError) as exc:
                raise SourceIncompleteError(
                    f"universe OI row token invalid: {symbol}"
                ) from exc
            oi = _safe_float(row.get("oi"))
            row_is_index = row.get("is_index_future")
            row_expiry = pd.to_datetime(row.get("expiry"), errors="coerce")
            checks = (
                pd.Timestamp(row["ts"]) == pd.Timestamp(wanted),
                pd.Timestamp(row["start"]) == pd.Timestamp(wanted - timedelta(minutes=5)),
                row_symbol == symbol,
                row_underlying == underlying,
                row_token == token,
                is_stock(row_is_index),
                not pd.isna(row_expiry)
                and pd.Timestamp(row_expiry).date().isoformat() == expiry,
                str(row.get("contract_month", "")) == contract_month,
                int(_safe_float(row.get("lot_size"))) == lot_size,
                _safe_float(row.get("tick_size")) == futures_tick_size,
                str(row.get("quality_state")) == "VALID",
                str(row.get("source")) == "kite_historical",
                str(row.get("data_version")) == common.RAW_DATA_VERSION,
                oi > 0,
            )
            if not all(checks):
                raise SourceIncompleteError(
                    f"universe OI S/S-5 row contract mismatch: {symbol}"
                )
            row_evidence.append(
                {
                    "timestamp": wanted.isoformat(),
                    "candle_start": (wanted - timedelta(minutes=5)).isoformat(),
                    "tradingsymbol": symbol,
                    "instrument_token": token,
                    "underlying": underlying,
                    "oi": oi,
                    "quality_state": "VALID",
                }
            )
        return {
            "tradingsymbol": symbol,
            "instrument_token": token,
            "underlying": underlying,
            "expiry": expiry,
            "contract_month": contract_month,
            "lot_size": lot_size,
            "tick_size": futures_tick_size,
            "equity_symbol": contract["equity_symbol"],
            "equity_instrument_token": contract["equity_instrument_token"],
            "equity_tick_size": contract["equity_tick_size"],
            "data_contract": contract["data_contract"],
            "source_path": str(source_path),
            "source_size_bytes": len(source_raw),
            "source_file_sha256": _sha256_bytes(source_raw),
            "predecessor_is_exact_s_minus_5": True,
            "rows": row_evidence,
            "rows_sha256": common.canonical_json_sha256(row_evidence),
        }

    proofs: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=max(1, min(int(max_workers), len(contracts))),
        thread_name_prefix="fno-v8-paper-oi-grid-proof",
    ) as executor:
        futures = {executor.submit(prove_contract, contract): contract for contract in contracts}
        for future in as_completed(futures):
            proofs.append(future.result())
    proofs.sort(key=lambda item: item["tradingsymbol"])
    proof_rows_sha256 = common.canonical_json_sha256(proofs)
    upstream_apps = _approved_app_names(
        marker.get("apps_used"), minimum=1, require_order=False
    )
    if upstream_apps is None:  # Defensive: the marker contract above already checked it.
        raise SourceIncompleteError("final futures marker app provenance is invalid")
    payload: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "V6_OI_SHIFT_EXACT_GRID_UNIVERSE_PROOF",
        "session_date": paths.session_date.isoformat(),
        "signal_end": signal_end,
        "signal_timestamp": signal_at.isoformat(),
        "previous_timestamp": previous_at.isoformat(),
        "source_policy_version": SOURCE_POLICY_VERSION,
        "proof_contract": "ALL_MAPPED_STOCK_FUTURES_SORTED_PREDECESSOR_EXACT_S_MINUS_5_APPROVED_KITE_PROVENANCE_V2",
        "upstream_app_provenance_policy": "NONEMPTY_UNIQUE_SUBSET_OF_CONFIGURED_APP1_TO_APP8_V1",
        "upstream_apps_used": list(upstream_apps),
        "upstream_apps_used_sha256": common.canonical_json_sha256(list(upstream_apps)),
        "marker_path": str(marker_path),
        "marker_sha256": _sha256_bytes(marker_raw),
        "marker_published_at_ist": published_at.isoformat(),
        "near_month_universe_path": str(universe_path),
        "near_month_universe_sha256": _sha256_file(universe_path),
        "cash_final_marker_path": cash_marker["marker_path"],
        "cash_final_marker_sha256": cash_marker["marker_sha256"],
        "cash_symbol_set_sha256": cash_universe_sha256,
        "stock_contracts_expected": len(contracts),
        "stock_contracts_proven": len(proofs),
        "stock_symbol_set_sha256": common.symbol_set_sha256(expected_symbols),
        "all_predecessors_exact_s_minus_5": True,
        "contracts": proofs,
        "contracts_sha256": proof_rows_sha256,
    }
    payload["proof_sha256"] = common.canonical_json_sha256(payload)
    return payload


def _validate_universe_oi_proof_payload(
    paths: SessionPaths,
    signal_end: str,
    payload: Mapping[str, Any],
) -> None:
    expected_marker_path = (
        paths.futures_slot_root
        / f"slot_{_slot_datetime(paths.session_date, signal_end).strftime('%Y%m%d_%H%M')}.json"
    )
    expected_cash_marker_path = (
        paths.cash_slot_root
        / f"slot_{_slot_datetime(paths.session_date, signal_end).strftime('%Y%m%d_%H%M')}.json"
    )
    expected = int(payload.get("stock_contracts_expected", -1))
    proven = int(payload.get("stock_contracts_proven", -2))
    contracts = payload.get("contracts")
    if not isinstance(contracts, list):
        raise SessionContractError("universe OI proof contracts are invalid")
    proof_started = _parse_aware_ist(
        payload.get("proof_started_at_ist"), "proof_started_at_ist"
    )
    proof_finished = _parse_aware_ist(
        payload.get("proof_finished_at_ist"), "proof_finished_at_ist"
    )
    confirmation_due = _parse_aware_ist(
        payload.get("confirmation_due_ist"), "confirmation_due_ist"
    )
    contract_symbols: list[str] = []
    contract_rows_valid = True
    for contract in contracts:
        if not isinstance(contract, Mapping):
            contract_rows_valid = False
            continue
        symbol = str(contract.get("tradingsymbol", "")).strip().upper()
        rows = contract.get("rows")
        digest = str(contract.get("source_file_sha256", ""))
        contract_symbols.append(symbol)
        contract_rows_valid = bool(
            contract_rows_valid
            and symbol
            and contract.get("predecessor_is_exact_s_minus_5") is True
            and isinstance(rows, list)
            and len(rows) == 2
            and contract.get("rows_sha256") == common.canonical_json_sha256(rows)
            and len(digest) == 64
            and all(character in "0123456789abcdef" for character in digest)
            and int(contract.get("source_size_bytes", 0)) > 0
            and str(contract.get("equity_symbol", "")).strip()
            and int(contract.get("equity_instrument_token", 0)) > 0
            and float(contract.get("equity_tick_size", 0)) > 0
            and float(contract.get("tick_size", 0)) > 0
            and int(contract.get("lot_size", 0)) > 0
            and bool(str(contract.get("expiry", "")))
            and bool(str(contract.get("contract_month", "")))
        )
    claimed = str(payload.get("proof_sha256", ""))
    unsigned = dict(payload)
    unsigned.pop("proof_sha256", None)
    checks = (
        payload.get("schema_version") == EVIDENCE_SCHEMA_VERSION,
        payload.get("kind") == "V6_OI_SHIFT_EXACT_GRID_UNIVERSE_PROOF",
        payload.get("session_date") == paths.session_date.isoformat(),
        payload.get("signal_end") == signal_end,
        payload.get("source_policy_version") == SOURCE_POLICY_VERSION,
        payload.get("all_predecessors_exact_s_minus_5") is True,
        payload.get("upstream_app_provenance_policy")
        == "NONEMPTY_UNIQUE_SUBSET_OF_CONFIGURED_APP1_TO_APP8_V1",
        _approved_app_names(
            payload.get("upstream_apps_used"), minimum=1, require_order=False
        )
        is not None,
        payload.get("upstream_apps_used_sha256")
        == common.canonical_json_sha256(
            list(
                _approved_app_names(
                    payload.get("upstream_apps_used"),
                    minimum=1,
                    require_order=False,
                )
                or ()
            )
        ),
        payload.get("proof_completed_before_confirmation_due") is True,
        proof_started <= proof_finished < confirmation_due,
        confirmation_due
        == _slot_datetime(paths.session_date, signal_end)
        + timedelta(minutes=1, seconds=BOUNDARY_BUFFER_SECONDS),
        expected > 0,
        proven == expected == len(contracts),
        contract_rows_valid,
        len(contract_symbols) == len(set(contract_symbols)),
        payload.get("contracts_sha256") == common.canonical_json_sha256(contracts),
        claimed == common.canonical_json_sha256(unsigned),
        Path(str(payload.get("marker_path", ""))) == expected_marker_path,
        Path(str(payload.get("near_month_universe_path", "")))
        == Path(paths.near_month_universe_path),
        Path(str(payload.get("cash_final_marker_path", "")))
        == expected_cash_marker_path,
    )
    if not all(checks):
        raise SessionContractError("universe OI proof semantic/hash binding mismatch")
    if _sha256_file(expected_marker_path) != payload.get("marker_sha256"):
        raise SessionContractError("universe OI proof marker changed after proof")
    if _sha256_file(Path(paths.near_month_universe_path)) != payload.get(
        "near_month_universe_sha256"
    ):
        raise SessionContractError("universe OI proof universe changed after proof")
    if _sha256_file(expected_cash_marker_path) != payload.get(
        "cash_final_marker_sha256"
    ):
        raise SessionContractError("universe OI proof cash marker changed after proof")


def load_immutable_universe_oi_proof(
    paths: SessionPaths,
    signal_end: str,
) -> tuple[dict[str, Any], str] | None:
    path = paths.oi_superset_audit_root / f"slot_{signal_end.replace(':', '')}.json"
    if not path.is_file():
        return None
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SessionContractError("immutable universe OI proof is unreadable") from exc
    if not isinstance(payload, dict):
        raise SessionContractError("immutable universe OI proof must be an object")
    _validate_universe_oi_proof_payload(paths, signal_end, payload)
    return payload, _sha256_bytes(raw)


def load_exact_futures_oi_pair(
    futures_symbol: str,
    expected_end: datetime,
    root: Path = DEFAULT_FUTURES_FIVE_MINUTE_ROOT,
    *,
    expected_token: int | None = None,
) -> dict[str, Any]:
    """Load OI at exactly S and S-5; an off-clock physical row is ignored."""

    path = Path(root) / f"{common.safe_contract_stem(futures_symbol)}_5minute.parquet"
    if not path.is_file():
        raise SourceIncompleteError(f"missing futures 5m source: {path}")
    required = [
        "timestamp",
        "candle_start",
        "tradingsymbol",
        "instrument_token",
        "oi",
        "quality_state",
        "source",
        "data_version",
    ]
    try:
        source_raw = path.read_bytes()
        frame = pd.read_parquet(io.BytesIO(source_raw), columns=required)
    except Exception as exc:
        raise SourceIncompleteError(f"futures 5m source unreadable/invalid: {path}") from exc
    if frame.empty:
        raise SourceIncompleteError(f"futures 5m source is empty: {futures_symbol}")
    frame = frame.copy()
    frame["ts"] = _ist_series(frame["timestamp"])
    frame["start"] = _ist_series(frame["candle_start"])
    end = _normalize_now(expected_end)
    previous_end = end - timedelta(minutes=5)
    selected = frame.loc[frame["ts"].isin({pd.Timestamp(previous_end), pd.Timestamp(end)})]
    if len(selected) != 2 or selected["ts"].duplicated(keep=False).any():
        raise SourceIncompleteError(
            f"futures OI requires unique exact S and S-5 rows: {futures_symbol}"
        )
    by_end = {pd.Timestamp(row["ts"]): row for row in selected.to_dict("records")}
    rows = [by_end[pd.Timestamp(previous_end)], by_end[pd.Timestamp(end)]]
    for row, wanted_end in zip(rows, (previous_end, end)):
        if pd.Timestamp(row["start"]) != pd.Timestamp(wanted_end - timedelta(minutes=5)):
            raise SourceIncompleteError("futures OI candle_start is not an exact 5m grid")
        if str(row.get("quality_state")) != "VALID":
            raise SourceIncompleteError("futures OI row quality is not VALID")
        if str(row.get("source")) != "kite_historical":
            raise SourceIncompleteError("futures OI row source is not kite_historical")
        if str(row.get("data_version")) != common.RAW_DATA_VERSION:
            raise SourceIncompleteError("futures OI row data version mismatch")
        if str(row.get("tradingsymbol", "")).strip().upper() != str(
            futures_symbol
        ).strip().upper():
            raise SourceIncompleteError("futures OI symbol mismatch")
        token = int(row.get("instrument_token", 0) or 0)
        if expected_token is not None and token != int(expected_token):
            raise SourceIncompleteError("futures OI token mismatch")
        oi = _safe_float(row.get("oi"))
        if oi <= 0:
            raise SourceIncompleteError("futures OI must be finite and positive")
    previous_oi = _safe_float(rows[0]["oi"])
    current_oi = _safe_float(rows[1]["oi"])
    delta = (current_oi / previous_oi - 1.0) * 100.0
    evidence_rows = [
        {
            "timestamp": wanted.isoformat(),
            "oi": oi,
            "quality_state": "VALID",
        }
        for wanted, oi in ((previous_end, previous_oi), (end, current_oi))
    ]
    return {
        "timestamp": end.isoformat(),
        "previous_timestamp": previous_end.isoformat(),
        "oi": current_oi,
        "prev_oi": previous_oi,
        "oi_change_pct": delta,
        "rows": evidence_rows,
        "rows_sha256": common.canonical_json_sha256(evidence_rows),
        "source_path": str(path),
        "source_file_size_bytes": len(source_raw),
        "source_file_sha256": _sha256_bytes(source_raw),
        "source_contract": "EXACT_NFO_5M_OI_S_AND_S_MINUS_5_V1",
    }


def load_final_cash_slot_marker(
    expected_end: datetime,
    root: Path = DEFAULT_CASH_SLOT_ROOT,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    path = Path(root) / f"slot_{expected_end.strftime('%Y%m%d_%H%M')}.json"
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise SourceNotReadyError(f"cash final-slot marker not published: {path}") from exc
    except OSError as exc:
        raise SourceIncompleteError(f"cash final-slot marker unreadable: {path}") from exc
    try:
        marker = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceIncompleteError(f"cash final-slot marker missing/invalid: {path}") from exc
    if not isinstance(marker, dict):
        raise SourceIncompleteError("cash final-slot marker must be an object")
    slot = _parse_aware_ist(marker.get("slot_ist"), "cash_slot_ist")
    published = _parse_aware_ist(
        marker.get("published_at_ist"), "cash_published_at_ist"
    )
    observed = _normalize_now(observed_at)
    if slot != expected_end:
        raise SourceIncompleteError("cash final-slot marker has the wrong slot")
    if published > observed:
        raise SourceNotReadyError(
            f"cash slot marker is not observable yet: {path}"
        )

    # The upstream 5-minute writer deliberately publishes a lightweight
    # `source=watcher` marker first, then atomically replaces it with the
    # authoritative `source=final` marker.  Seeing that provisional marker
    # before S+1+buffer is normal source latency, not permanent corruption.
    # Once the deadline is reached, however, the absence of a final marker is
    # terminal so the paper session remains fail-closed and never backfills.
    source_deadline = expected_end + timedelta(
        minutes=1, seconds=BOUNDARY_BUFFER_SECONDS
    )
    if marker.get("source") != "final":
        if observed < source_deadline:
            raise SourceNotReadyError(
                "cash final-slot marker is still provisional "
                f"(source={marker.get('source')!r}): {path}"
            )
        raise SourceIncompleteError(
            "cash final-slot marker was not authoritative final by the source "
            "deadline"
        )

    checks = (
        marker.get("complete") is True,
        int(marker.get("tickers_expected", -1)) > 0,
        int(marker.get("tickers_written", -2))
        == int(marker.get("tickers_expected", -1)),
        int(marker.get("tickers_complete", -2))
        == int(marker.get("tickers_expected", -1)),
        int(marker.get("tickers_failed", -1)) == 0,
        int(marker.get("unresolved_symbol_count", -1)) == 0,
        int(marker.get("failed_symbol_count", -1)) == 0,
        int(marker.get("token_missing_symbol_count", -1)) == 0,
        marker.get("fno_equity_quality_complete") is True,
        int(marker.get("fno_equity_ready", -2))
        == int(marker.get("fno_equity_expected", -1)),
        int(marker.get("fno_equity_failed", -1)) == 0,
        not list(marker.get("partition_failures") or []),
        int(marker.get("verification_failed_count", -1)) == 0,
    )
    if not all(checks):
        raise SourceIncompleteError("cash final-slot marker is not fully complete")
    return {
        **marker,
        "marker_path": str(path),
        "marker_sha256": _sha256_bytes(raw),
    }


def load_current_cash_signal_features(
    symbol: str,
    expected_end: datetime,
    five_minute_root: Path = DEFAULT_FIVE_MINUTE_ROOT,
    cash_slot_root: Path = DEFAULT_CASH_SLOT_ROOT,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    """Recompute V8 cash features from the current completed-real 5m store."""

    marker = load_final_cash_slot_marker(
        expected_end, cash_slot_root, observed_at=observed_at
    )
    path = hybrid.equity_five_minute_path(symbol, Path(five_minute_root))
    if not path.is_file():
        raise SourceIncompleteError(f"current cash 5m source missing: {path}")
    required = {
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "gap_filled",
        "opening_snapshot",
        "provisional_stale",
        "source_1m_count",
    }
    try:
        source_raw = path.read_bytes()
        raw_frame = pd.read_parquet(io.BytesIO(source_raw))
    except Exception as exc:
        raise SourceIncompleteError(f"current cash 5m source unreadable: {path}") from exc
    if raw_frame.empty or not required.issubset(raw_frame.columns):
        raise SourceIncompleteError(
            f"current cash 5m source lacks strict lineage columns: {symbol}"
        )
    raw_frame = raw_frame.copy()
    raw_frame["ts"] = _ist_series(raw_frame["date"])
    if raw_frame["ts"].isna().any():
        raise SourceIncompleteError(f"current cash 5m source has invalid dates: {symbol}")
    prefix_raw = raw_frame.loc[
        raw_frame["ts"].le(pd.Timestamp(expected_end))
    ].copy()
    if prefix_raw.empty or prefix_raw["ts"].duplicated(keep=False).any():
        raise SourceIncompleteError(
            f"current cash causal prefix is empty/duplicated: {symbol}"
        )
    selected_raw = prefix_raw.loc[prefix_raw["ts"].eq(pd.Timestamp(expected_end))]
    if len(selected_raw) != 1:
        raise SourceIncompleteError(f"current cash 5m signal row is not unique: {symbol}")
    raw = selected_raw.iloc[0]
    if int(_safe_float(raw.get("source_1m_count"))) != 5:
        raise SourceIncompleteError("cash 5m signal does not prove five 1m sources")
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale"):
        value = raw.get(flag)
        if bool(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0]) or str(
            value
        ).strip().lower() in {"true", "yes", "on"}:
            raise SourceIncompleteError(f"cash 5m signal has forbidden lineage: {flag}")
    numeric = ["open", "high", "low", "close", "volume"]
    prefix_raw[numeric] = prefix_raw[numeric].apply(pd.to_numeric, errors="coerce")
    # Historical V8 accepts only candles with proved 5x1m lineage.  Unlike the
    # V6 compatibility loader, NaN source counts are not implicitly eligible.
    strict_prefix = prefix_raw.loc[
        pd.to_numeric(prefix_raw["source_1m_count"], errors="coerce").eq(5)
    ].copy()
    completed = hybrid.reject_exact_adjacent_ohlcv_copies(
        hybrid.completed_real_equity_five_minute_bars(strict_prefix)
    )
    if completed.empty:
        raise SourceIncompleteError(f"current cash causal prefix is empty: {symbol}")
    if any(not is_regular_nse_session(value) for value in set(completed["ts"].dt.date)):
        raise SourceIncompleteError(
            f"current cash prefix contains a non-regular/unfrozen date: {symbol}"
        )
    geometry = (
        completed[numeric].notna().all(axis=1)
        & completed["open"].gt(0)
        & completed["high"].gt(0)
        & completed["low"].gt(0)
        & completed["close"].gt(0)
        & completed["volume"].ge(0)
        & completed["high"].ge(completed[["open", "close"]].max(axis=1))
        & completed["low"].le(completed[["open", "close"]].min(axis=1))
        & completed["high"].ge(completed["low"])
        & pd.to_numeric(completed["source_1m_count"], errors="coerce").eq(5)
    )
    if not geometry.all():
        raise SourceIncompleteError(
            f"current cash causal prefix has invalid OHLCV/lineage: {symbol}"
        )
    current_day = completed.loc[completed["ts"].dt.date.eq(expected_end.date())]
    expected_current_grid = pd.date_range(
        datetime.combine(expected_end.date(), day_time(9, 20), tzinfo=config.IST),
        expected_end,
        freq="5min",
    )
    if list(current_day["ts"]) != list(expected_current_grid):
        raise SourceIncompleteError(
            f"current cash signal-day prefix is not an exact completed 5m grid: {symbol}"
        )
    featured = hybrid.add_equity_five_minute_features(completed)
    selected = featured.loc[featured["ts"].eq(pd.Timestamp(expected_end))]
    if len(selected) != 1:
        raise SourceIncompleteError(f"cash feature row is not exact/unique: {symbol}")
    row = selected.iloc[0]
    values = {
        name: _safe_float(row.get(name))
        for name in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ema9",
            "ema20",
            "ema50",
            "price_change_pct",
            "volume_ratio",
            "traded_value",
        )
    }
    prefix_evidence = [
        {
            "timestamp": pd.Timestamp(item["ts"]).isoformat(),
            "open": float(item["open"]),
            "high": float(item["high"]),
            "low": float(item["low"]),
            "close": float(item["close"]),
            "volume": float(item["volume"]),
            "source_1m_count": 5,
        }
        for item in completed.to_dict("records")
    ]
    return {
        "timestamp": expected_end.isoformat(),
        **values,
        "source_1m_count": 5,
        "source_path": str(path),
        "source_file_size_bytes": len(source_raw),
        "source_file_sha256": _sha256_bytes(source_raw),
        "causal_prefix_count": len(prefix_evidence),
        "causal_prefix_first_ist": prefix_evidence[0]["timestamp"],
        "causal_prefix_last_ist": prefix_evidence[-1]["timestamp"],
        "causal_prefix_sha256": common.canonical_json_sha256(prefix_evidence),
        "cash_slot_marker_path": marker["marker_path"],
        "cash_slot_marker_sha256": marker["marker_sha256"],
        "source_contract": "CURRENT_LIVE_5M_FINAL_MARKER_SOURCE_1M_COUNT_5_V1",
    }


def fetch_exact_cash_signal_constituents(
    snapshot: Mapping[str, Any],
    paths: SessionPaths,
    signal_end: str,
    runtimes: Sequence[market_data.AppRuntime],
    *,
    observed_at: datetime,
    observations: int = market_data.DEFAULT_OBSERVATIONS,
    observation_spacing_sec: float = market_data.DEFAULT_OBSERVATION_SPACING_SEC,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Fetch one exact five-row range per candidate through an approved app pool.

    Nothing is persisted here.  The caller must finish all other source checks,
    re-read the wall clock, and prove it is still before S+1 before sealing the
    returned audit or registering candidates.
    """

    pool = _validate_approved_runtime_pool(
        runtimes, minimum_healthy_apps=MIN_HEALTHY_KITE_APPS
    )
    if observations < 1:
        raise ValueError("cash constituent observations must be positive")

    by_symbol: dict[str, int] = {}
    for raw in snapshot.get("candidates") or []:
        symbol = str(raw.get("tradingsymbol", "")).strip().upper()
        token = int(raw.get("instrument_token", 0) or 0)
        if not symbol or token <= 0:
            raise SourceIncompleteError("scanner candidate lacks cash symbol/token")
        prior = by_symbol.setdefault(symbol, token)
        if prior != token:
            raise SourceIncompleteError(f"scanner cash token changed for {symbol}")
    requests = [
        market_data.CandidateRequest(symbol, token)
        for symbol, token in sorted(by_symbol.items())
    ]
    signal_at = _slot_datetime(paths.session_date, signal_end)
    normalized_observed = _normalize_now(observed_at)
    if normalized_observed < signal_at + timedelta(seconds=BOUNDARY_BUFFER_SECONDS):
        raise SourceIncompleteError("cash signal candle is not completed yet")
    audit_deadline = signal_at + timedelta(
        minutes=1, seconds=BOUNDARY_BUFFER_SECONDS
    )
    monotonic_deadline = time.monotonic() + max(
        0.0, (audit_deadline - normalized_observed).total_seconds()
    )
    expected_starts = [
        pd.Timestamp(signal_at - timedelta(minutes=offset))
        for offset in range(5, 0, -1)
    ]
    expected_start_set = set(expected_starts)
    def fetch_one(
        request_index: int,
        request: market_data.CandidateRequest,
    ) -> tuple[str, dict[str, Any] | None, list[dict[str, Any]]]:
        attempts: list[dict[str, Any]] = []
        for attempt in range(1, observations + 1):
            runtime = pool[(request_index + attempt - 1) % len(pool)]
            try:
                raw_records = runtime.call_historical_data(
                    int(request.instrument_token),
                    expected_starts[0].to_pydatetime(),
                    (pd.Timestamp(signal_at) + pd.Timedelta(minutes=1)).to_pydatetime(),
                    "minute",
                    continuous=False,
                    oi=False,
                    monotonic_deadline=monotonic_deadline,
                )
                by_start: dict[pd.Timestamp, Mapping[str, Any]] = {}
                duplicate = False
                for raw in raw_records or ():
                    raw_time = raw.get("date", raw.get("timestamp"))
                    if raw_time is None:
                        continue
                    start = pd.Timestamp(raw_time)
                    if start.tzinfo is None:
                        start = start.tz_localize(config.IST)
                    else:
                        start = start.tz_convert(config.IST)
                    if start not in expected_start_set:
                        continue
                    if start in by_start:
                        duplicate = True
                    by_start[start] = raw
                if duplicate or set(by_start) != expected_start_set:
                    raise SourceIncompleteError(
                        "range response lacks five unique exact candle starts"
                    )
                records: list[dict[str, Any]] = []
                for start in expected_starts:
                    raw = by_start[start]
                    record = {
                        "timestamp": (start + pd.Timedelta(minutes=1)).isoformat(),
                        "candle_start": start.isoformat(),
                        "open": raw.get("open"),
                        "high": raw.get("high"),
                        "low": raw.get("low"),
                        "close": raw.get("close"),
                        "volume": raw.get("volume", 0),
                        "gap_filled": False,
                        "opening_snapshot": False,
                        "provisional_stale": False,
                    }
                    error = market_data._validate_completed_bar(
                        record, start + pd.Timedelta(minutes=1)
                    )
                    if error:
                        raise SourceIncompleteError(error)
                    records.append(
                        {
                            "timestamp": record["timestamp"],
                            "candle_start": record["candle_start"],
                            "open": _safe_float(record["open"]),
                            "high": _safe_float(record["high"]),
                            "low": _safe_float(record["low"]),
                            "close": _safe_float(record["close"]),
                            "volume": _safe_float(record["volume"]),
                            "app_name": runtime.app_name,
                        }
                    )
                attempts.append(
                    {
                        "attempt": attempt,
                        "app_name": runtime.app_name,
                        "state": "SUCCESS",
                    }
                )
                return request.symbol, {
                    "open": records[0]["open"],
                    "high": max(record["high"] for record in records),
                    "low": min(record["low"] for record in records),
                    "close": records[-1]["close"],
                    "volume": sum(record["volume"] for record in records),
                    "constituents": records,
                    "constituents_sha256": common.canonical_json_sha256(records),
                    "app_name": runtime.app_name,
                    "source_contract": (
                        "DIRECT_KITE_EXACT_COMPLETED_CASH_S_MINUS_4_THROUGH_S_V1"
                    ),
                }, attempts
            except Exception as exc:
                attempts.append(
                    {
                        "attempt": attempt,
                        "app_name": runtime.app_name,
                        "state": "FAILED",
                        "error": " ".join(f"{type(exc).__name__}: {exc}".split())[:500],
                    }
                )
                if attempt < observations and observation_spacing_sec > 0:
                    time.sleep(float(observation_spacing_sec))
        return request.symbol, None, attempts

    fetched: dict[str, dict[str, Any]] = {}
    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=len(pool), thread_name_prefix="fno-v8-paper-cash-audit"
    ) as executor:
        pending = {
            executor.submit(fetch_one, index, request): request
            for index, request in enumerate(requests)
        }
        for future in as_completed(pending):
            symbol, value, attempts = future.result()
            outcomes.append(
                {
                    "symbol": symbol,
                    "app_name": None if value is None else value.get("app_name"),
                    "attempted_apps": [item.get("app_name") for item in attempts],
                    "state": "SUCCESS" if value is not None else "DATA_INCOMPLETE",
                    "attempts": attempts,
                }
            )
            if value is not None:
                fetched[symbol] = value
    missing = sorted(set(by_symbol) - set(fetched))
    if missing:
        raise SourceIncompleteError(
            f"direct 5x1m cash audit incomplete for symbols: {missing}"
        )
    audit = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT",
        "session_date": paths.session_date.isoformat(),
        "signal_end": signal_end,
        "signal_timestamp": signal_at.isoformat(),
        "source_contract": "ONE_RANGE_REQUEST_PER_CANDIDATE_APPROVED_HEALTHY_APP_POOL_WITH_CROSS_APP_RETRY_V2",
        "candidate_contract_sha256": common.canonical_json_sha256(
            [asdict(request) for request in requests]
        ),
        "app_roster": market_data.app_roster_payload(pool),
        "app_roster_sha256": market_data.app_roster_sha256(pool),
        "healthy_app_count": len(pool),
        "minimum_healthy_app_count": MIN_HEALTHY_KITE_APPS,
        "app_pool_degraded": len(pool) < len(market_data.EXPECTED_APP_NAMES),
        "app_provenance_policy": "ORDERED_UNIQUE_APPROVED_HEALTHY_SUBSET_V1",
        "candidate_count": len(requests),
        "outcomes": sorted(outcomes, key=lambda item: item["symbol"]),
        "symbols": fetched,
    }
    return fetched, audit


def _candidate_passes_setup(row: Mapping[str, Any], setup: config.PaperSetup) -> bool:
    if str(row.get("side", "")).upper() != setup.side:
        return False
    if str(row.get("signal_end", "")) != setup.signal_end:
        raise SourceIncompleteError("candidate signal_end does not match its scanner slot")
    price = _safe_float(row.get("price_change_pct"))
    oi = _safe_float(row.get("oi"))
    prev_oi = _safe_float(row.get("prev_oi"))
    oi_change = _safe_float(row.get("oi_change_pct"))
    volume_ratio = _safe_float(row.get("volume_ratio"))
    traded_value = _safe_float(row.get("traded_value"))
    ema9 = _safe_float(row.get("ema9"))
    ema20 = _safe_float(row.get("ema20"))
    ema50 = _safe_float(row.get("ema50"))
    if oi <= prev_oi or oi_change < setup.oi_change_pct:
        return False
    if volume_ratio < setup.volume_ratio:
        return False
    if traded_value < setup.min_traded_value:
        return False
    if setup.side == "LONG":
        return ema9 > ema20 > ema50 and price >= setup.price_change_pct
    return ema9 < ema20 < ema50 and price <= -setup.price_change_pct


def _picker_value(row: Mapping[str, Any], picker: str) -> float:
    values = {
        "max_move": abs(_safe_float(row.get("price_change_pct"))),
        "max_oi": _safe_float(row.get("oi_change_pct")),
        "max_volume": _safe_float(row.get("volume_ratio")),
        "max_liquidity": _safe_float(row.get("traded_value")),
    }
    try:
        return values[picker]
    except KeyError as exc:
        raise SessionContractError(f"unsupported V8 picker: {picker}") from exc


def precompute_strict_cash_universe_source(
    paths: SessionPaths,
    signal_end: str,
    *,
    observed_at: datetime,
    cash_feature_loader: Callable[..., Mapping[str, Any]] = (
        load_current_cash_signal_features
    ),
    max_workers: int = config.REQUIRED_KITE_APPS,
) -> dict[str, Any]:
    """Prewarm strict cash features at the early cash-final marker."""

    signal_at = _slot_datetime(paths.session_date, signal_end)
    marker = load_final_cash_slot_marker(
        signal_at,
        paths.cash_slot_root,
        observed_at=observed_at,
    )
    universe_path = Path(paths.near_month_universe_path)
    try:
        universe_raw = universe_path.read_bytes()
        universe = pd.read_parquet(io.BytesIO(universe_raw))
    except Exception as exc:
        raise SourceIncompleteError("strict cash prewarm universe is unreadable") from exc
    required = {
        "master_date",
        "is_index_future",
        "equity_symbol",
        "equity_instrument_token",
        "equity_tick_size",
    }
    if universe.empty or not required.issubset(universe.columns):
        raise SourceIncompleteError("strict cash prewarm universe mapping is incomplete")
    dates = set(pd.to_datetime(universe["master_date"], errors="coerce").dropna().dt.date)
    if dates != {paths.session_date}:
        raise SourceIncompleteError("strict cash prewarm universe is not current-date")

    def is_stock(value: Any) -> bool:
        if isinstance(value, str):
            return value.strip().lower() in {"false", "0", "no", "off"}
        return value is not None and not bool(value)

    identities: list[dict[str, Any]] = []
    for raw in universe.loc[universe["is_index_future"].map(is_stock)].to_dict("records"):
        symbol = str(raw.get("equity_symbol", "")).strip().upper()
        token = int(raw.get("equity_instrument_token", 0) or 0)
        tick = _safe_float(raw.get("equity_tick_size"))
        if not symbol or token <= 0 or tick <= 0:
            raise SourceIncompleteError("strict cash prewarm identity is invalid")
        identities.append(
            {"symbol": symbol, "instrument_token": token, "tick_size": tick}
        )
    identities.sort(key=lambda item: item["symbol"])
    if len(identities) == 0 or len({item["symbol"] for item in identities}) != len(
        identities
    ):
        raise SourceIncompleteError("strict cash prewarm identities are not unique")
    symbol_hash = common.symbol_set_sha256(item["symbol"] for item in identities)
    if not (
        int(marker.get("tickers_expected", -1)) >= len(identities)
        and int(marker.get("current_symbol_count", -1))
        == int(marker.get("tickers_expected", -2))
        and int(marker.get("fno_equity_expected", -1)) == len(identities)
        and marker.get("fno_equity_universe_sha256") == symbol_hash
    ):
        raise SourceIncompleteError("strict cash marker/universe binding mismatch")

    def load_one(identity: Mapping[str, Any]) -> dict[str, Any]:
        features = dict(
            cash_feature_loader(
                str(identity["symbol"]),
                signal_at,
                paths.five_minute_root,
                paths.cash_slot_root,
                observed_at=observed_at,
            )
        )
        return {**dict(identity), "features": features}

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=max(1, min(int(max_workers), len(identities))),
        thread_name_prefix="fno-v8-paper-strict-cash-prewarm",
    ) as executor:
        pending = [executor.submit(load_one, identity) for identity in identities]
        for future in as_completed(pending):
            rows.append(future.result())
    rows.sort(key=lambda item: item["symbol"])
    payload: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "STRICT_CASH_ALL_STOCK_UNIVERSE_SOURCE",
        "session_date": paths.session_date.isoformat(),
        "signal_end": signal_end,
        "signal_timestamp": signal_at.isoformat(),
        "source_policy_version": SOURCE_POLICY_VERSION,
        "near_month_universe_path": str(universe_path),
        "near_month_universe_sha256": _sha256_bytes(universe_raw),
        "cash_final_marker_path": marker["marker_path"],
        "cash_final_marker_sha256": marker["marker_sha256"],
        "universe_count": len(rows),
        "cash_symbol_set_sha256": symbol_hash,
        "rows": rows,
    }
    payload["strict_cash_source_sha256"] = common.canonical_json_sha256(payload)
    return payload


def _validate_strict_cash_universe_source(
    paths: SessionPaths,
    signal_end: str,
    payload: Mapping[str, Any],
) -> None:
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise SessionContractError("strict cash source rows are invalid")
    started = _parse_aware_ist(payload.get("source_started_at_ist"), "cash_source_started")
    finished = _parse_aware_ist(payload.get("source_finished_at_ist"), "cash_source_finished")
    due = _parse_aware_ist(payload.get("confirmation_due_ist"), "cash_confirmation_due")
    symbols = [str(row.get("symbol", "")).strip().upper() for row in rows]
    unsigned = dict(payload)
    claimed = str(unsigned.pop("strict_cash_source_sha256", ""))
    expected_marker = (
        paths.cash_slot_root
        / f"slot_{_slot_datetime(paths.session_date, signal_end).strftime('%Y%m%d_%H%M')}.json"
    )
    checks = (
        payload.get("schema_version") == EVIDENCE_SCHEMA_VERSION,
        payload.get("kind") == "STRICT_CASH_ALL_STOCK_UNIVERSE_SOURCE",
        payload.get("session_date") == paths.session_date.isoformat(),
        payload.get("signal_end") == signal_end,
        payload.get("source_policy_version") == SOURCE_POLICY_VERSION,
        payload.get("source_completed_before_confirmation_due") is True,
        started <= finished < due,
        due
        == _slot_datetime(paths.session_date, signal_end)
        + timedelta(minutes=1, seconds=BOUNDARY_BUFFER_SECONDS),
        int(payload.get("universe_count", -1)) == len(rows) > 0,
        len(symbols) == len(set(symbols)),
        payload.get("cash_symbol_set_sha256") == common.symbol_set_sha256(symbols),
        Path(str(payload.get("near_month_universe_path", "")))
        == Path(paths.near_month_universe_path),
        Path(str(payload.get("cash_final_marker_path", ""))) == expected_marker,
        claimed == common.canonical_json_sha256(unsigned),
    )
    if not all(checks):
        raise SessionContractError("strict cash source semantic/hash binding mismatch")
    if _sha256_file(Path(paths.near_month_universe_path)) != payload.get(
        "near_month_universe_sha256"
    ) or _sha256_file(expected_marker) != payload.get("cash_final_marker_sha256"):
        raise SessionContractError("strict cash source dependency changed after prewarm")


def load_immutable_strict_cash_universe_source(
    paths: SessionPaths,
    signal_end: str,
) -> tuple[dict[str, Any], str] | None:
    path = paths.strict_cash_source_root / f"slot_{signal_end.replace(':', '')}.json"
    if not path.is_file():
        return None
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SessionContractError("immutable strict cash source is unreadable") from exc
    if not isinstance(payload, dict):
        raise SessionContractError("immutable strict cash source must be an object")
    _validate_strict_cash_universe_source(paths, signal_end, payload)
    return payload, _sha256_bytes(raw)


def precompute_independent_v8_candidate_source(
    paths: SessionPaths,
    signal_end: str,
    universe_oi_proof: Mapping[str, Any],
    *,
    observed_at: datetime,
    cash_feature_loader: Callable[..., Mapping[str, Any]] = (
        load_current_cash_signal_features
    ),
    strict_cash_source: Mapping[str, Any] | None = None,
    max_workers: int = config.REQUIRED_KITE_APPS,
) -> dict[str, Any]:
    """Rebuild V8 setup eligibility for the complete mapped stock universe."""

    signal_at = _slot_datetime(paths.session_date, signal_end)
    proof_contracts = universe_oi_proof.get("contracts")
    if not isinstance(proof_contracts, list) or not proof_contracts:
        raise SourceIncompleteError("independent candidate source lacks universe proof")
    strict_cash_by_symbol = {
        str(item.get("symbol", "")).strip().upper(): item
        for item in (strict_cash_source or {}).get("rows", [])
        if isinstance(item, Mapping)
    }

    def build_one(mapping: Mapping[str, Any]) -> dict[str, Any]:
        futures_symbol = str(mapping.get("tradingsymbol", "")).strip().upper()
        symbol = str(mapping.get("equity_symbol", "")).strip().upper()
        token = int(mapping.get("equity_instrument_token", 0) or 0)
        futures_token = int(mapping.get("instrument_token", 0) or 0)
        if not futures_symbol or not symbol or token <= 0 or futures_token <= 0:
            raise SourceIncompleteError("independent universe mapping is incomplete")
        if strict_cash_source is not None:
            cash_row = strict_cash_by_symbol.get(symbol)
            if not isinstance(cash_row, Mapping):
                raise SourceIncompleteError(
                    f"strict cash prewarm lacks mapped symbol: {symbol}"
                )
            if (
                int(cash_row.get("instrument_token", 0)) != token
                or _safe_float(cash_row.get("tick_size"))
                != _safe_float(mapping.get("equity_tick_size"))
            ):
                raise SourceIncompleteError(
                    f"strict cash prewarm mapping changed: {symbol}"
                )
            five = dict(cash_row.get("features") or {})
        else:
            five = dict(
                cash_feature_loader(
                    symbol,
                    signal_at,
                    paths.five_minute_root,
                    paths.cash_slot_root,
                    observed_at=observed_at,
                )
            )
        proof_rows = [
            dict(item)
            for item in mapping.get("rows") or []
            if isinstance(item, Mapping)
        ]
        if len(proof_rows) != 2:
            raise SourceIncompleteError(f"independent OI pair is incomplete: {symbol}")
        previous_oi = _safe_float(proof_rows[0].get("oi"))
        current_oi = _safe_float(proof_rows[1].get("oi"))
        if previous_oi <= 0 or current_oi <= 0:
            raise SourceIncompleteError(f"independent OI pair is non-positive: {symbol}")
        oi_pair = {
            "timestamp": signal_at.isoformat(),
            "previous_timestamp": (signal_at - timedelta(minutes=5)).isoformat(),
            "oi": current_oi,
            "prev_oi": previous_oi,
            "oi_change_pct": (current_oi / previous_oi - 1.0) * 100.0,
            "rows": [
                {
                    "timestamp": str(item.get("timestamp")),
                    "oi": _safe_float(item.get("oi")),
                    "quality_state": str(item.get("quality_state")),
                }
                for item in proof_rows
            ],
            "source_path": mapping.get("source_path"),
            "source_file_size_bytes": mapping.get("source_size_bytes"),
            "source_file_sha256": mapping.get("source_file_sha256"),
            "source_contract": "SEALED_UNIVERSE_EXACT_S_AND_S_MINUS_5_OI_V1",
        }
        oi_pair["rows_sha256"] = common.canonical_json_sha256(oi_pair["rows"])
        base = {
            "tradingsymbol": symbol,
            "underlying": mapping.get("underlying"),
            "instrument_token": token,
            "futures_tradingsymbol": futures_symbol,
            "futures_instrument_token": futures_token,
            "tick_size": _safe_float(mapping.get("equity_tick_size")),
            "lot_size": 1,
            "data_contract": V6_SCANNER_DATA_CONTRACT,
            "signal_end": signal_end,
            "signal_timestamp": signal_at.isoformat(),
            "signal_close": _safe_float(five.get("close")),
            "price_change_pct": _safe_float(five.get("price_change_pct")),
            "oi": current_oi,
            "prev_oi": previous_oi,
            "oi_change_pct": oi_pair["oi_change_pct"],
            "volume_ratio": _safe_float(five.get("volume_ratio")),
            "traded_value": _safe_float(five.get("traded_value")),
            "ema9": _safe_float(five.get("ema9")),
            "ema20": _safe_float(five.get("ema20")),
            "ema50": _safe_float(five.get("ema50")),
            "_cash_features": five,
            "_oi_pair": oi_pair,
            "_proof_mapping": dict(mapping),
        }
        eligible_sides: list[str] = []
        for side in ("LONG", "SHORT"):
            setup = config.setup_for(signal_end, side)
            if setup is None:
                raise SessionContractError(f"missing frozen setup: {signal_end}_{side}")
            if _candidate_passes_setup({**base, "side": side}, setup):
                eligible_sides.append(side)
        base["eligible_sides"] = eligible_sides
        return base

    universe_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=max(1, min(int(max_workers), len(proof_contracts))),
        thread_name_prefix="fno-v8-paper-independent-candidate-source",
    ) as executor:
        pending = {
            executor.submit(build_one, mapping): mapping
            for mapping in proof_contracts
            if isinstance(mapping, Mapping)
        }
        if len(pending) != len(proof_contracts):
            raise SourceIncompleteError("universe proof contains a non-object contract")
        for future in as_completed(pending):
            universe_rows.append(future.result())
    universe_rows.sort(key=lambda item: str(item["tradingsymbol"]))
    eligible_rows: list[dict[str, Any]] = []
    for row in universe_rows:
        for side in row["eligible_sides"]:
            eligible_rows.append({**row, "side": side})
    eligible_rows.sort(key=lambda item: (str(item["side"]), str(item["tradingsymbol"])))

    # The private-prefixed nested values are still explicit evidence; the
    # prefix only distinguishes authoritative source bundles from engine fields.
    payload: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "INDEPENDENT_V8_ALL_STOCK_CANDIDATE_SOURCE",
        "session_date": paths.session_date.isoformat(),
        "signal_end": signal_end,
        "signal_timestamp": signal_at.isoformat(),
        "source_policy_version": SOURCE_POLICY_VERSION,
        "authority": "INDEPENDENT_ALL_MAPPED_STOCKS_NOT_V6_CANDIDATE_ROWS",
        "universe_oi_proof_sha256": universe_oi_proof.get("proof_sha256"),
        "strict_cash_source_sha256": (
            None
            if strict_cash_source is None
            else strict_cash_source.get("strict_cash_source_sha256")
        ),
        "universe_count": len(universe_rows),
        "universe_symbol_set_sha256": common.symbol_set_sha256(
            row["tradingsymbol"] for row in universe_rows
        ),
        "eligible_count": len(eligible_rows),
        "eligible_by_side": {
            side: sum(row["side"] == side for row in eligible_rows)
            for side in ("LONG", "SHORT")
        },
        "universe_rows": universe_rows,
        "eligible_rows": eligible_rows,
    }
    payload["candidate_source_sha256"] = common.canonical_json_sha256(payload)
    return payload


def _validate_independent_candidate_source(
    paths: SessionPaths,
    signal_end: str,
    payload: Mapping[str, Any],
    *,
    universe_proof_sha256: str,
    strict_cash_source_sha256: str,
) -> None:
    rows = payload.get("universe_rows")
    eligible = payload.get("eligible_rows")
    if not isinstance(rows, list) or not isinstance(eligible, list):
        raise SessionContractError("independent candidate source rows are invalid")
    source_started = _parse_aware_ist(
        payload.get("source_started_at_ist"), "source_started_at_ist"
    )
    source_finished = _parse_aware_ist(
        payload.get("source_finished_at_ist"), "source_finished_at_ist"
    )
    confirmation_due = _parse_aware_ist(
        payload.get("confirmation_due_ist"), "confirmation_due_ist"
    )
    symbols = [str(row.get("tradingsymbol", "")).strip().upper() for row in rows]
    unsigned = dict(payload)
    claimed = str(unsigned.pop("candidate_source_sha256", ""))
    checks = (
        payload.get("schema_version") == EVIDENCE_SCHEMA_VERSION,
        payload.get("kind") == "INDEPENDENT_V8_ALL_STOCK_CANDIDATE_SOURCE",
        payload.get("session_date") == paths.session_date.isoformat(),
        payload.get("signal_end") == signal_end,
        payload.get("source_policy_version") == SOURCE_POLICY_VERSION,
        payload.get("authority")
        == "INDEPENDENT_ALL_MAPPED_STOCKS_NOT_V6_CANDIDATE_ROWS",
        payload.get("source_completed_before_confirmation_due") is True,
        source_started <= source_finished < confirmation_due,
        confirmation_due
        == _slot_datetime(paths.session_date, signal_end)
        + timedelta(minutes=1, seconds=BOUNDARY_BUFFER_SECONDS),
        payload.get("universe_oi_proof_sha256") == universe_proof_sha256,
        payload.get("strict_cash_source_sha256") == strict_cash_source_sha256,
        int(payload.get("universe_count", -1)) == len(rows) > 0,
        len(symbols) == len(set(symbols)),
        payload.get("universe_symbol_set_sha256") == common.symbol_set_sha256(symbols),
        int(payload.get("eligible_count", -1)) == len(eligible),
        claimed == common.canonical_json_sha256(unsigned),
    )
    if not all(checks):
        raise SessionContractError("independent candidate source binding mismatch")


def load_immutable_independent_candidate_source(
    paths: SessionPaths,
    signal_end: str,
    *,
    universe_proof_sha256: str,
    strict_cash_source_sha256: str,
) -> tuple[dict[str, Any], str] | None:
    path = (
        paths.independent_candidate_source_root
        / f"slot_{signal_end.replace(':', '')}.json"
    )
    if not path.is_file():
        return None
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SessionContractError(
            "immutable independent candidate source is unreadable"
        ) from exc
    if not isinstance(payload, dict):
        raise SessionContractError("independent candidate source must be an object")
    _validate_independent_candidate_source(
        paths,
        signal_end,
        payload,
        universe_proof_sha256=universe_proof_sha256,
        strict_cash_source_sha256=strict_cash_source_sha256,
    )
    return payload, _sha256_bytes(raw)


def build_v8_candidate_book(
    snapshot: Mapping[str, Any],
    setup: config.PaperSetup,
    paths: SessionPaths,
    *,
    source_sha256: str,
    observed_at: datetime,
    cash_constituent_audit: Mapping[str, Mapping[str, Any]],
    universe_oi_proof: Mapping[str, Any],
    independent_candidate_source: Mapping[str, Any] | None = None,
    cash_feature_loader: Callable[..., Mapping[str, Any]] = (
        load_current_cash_signal_features
    ),
    futures_oi_loader: Callable[..., Mapping[str, Any]] = (
        load_exact_futures_oi_pair
    ),
) -> list[dict[str, Any]]:
    """Reapply the V8 leg gates, freeze its complete deterministic rank."""

    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    signal_at = _slot_datetime(paths.session_date, setup.signal_end)
    proof_contracts = universe_oi_proof.get("contracts")
    if not isinstance(proof_contracts, list):
        raise SourceIncompleteError("universe proof contracts are unavailable")
    proof_by_future = {
        str(item.get("tradingsymbol", "")).strip().upper(): item
        for item in proof_contracts
        if isinstance(item, Mapping)
    }
    if len(proof_by_future) != len(proof_contracts):
        raise SourceIncompleteError("universe proof mapping is not unique/complete")
    independent_authority = independent_candidate_source is not None
    if independent_authority:
        source_candidates = [
            row
            for row in independent_candidate_source.get("eligible_rows") or []
            if isinstance(row, Mapping)
            and str(row.get("side", "")).upper() == setup.side
        ]
    else:
        source_candidates = list(snapshot.get("candidates") or [])
    scanner_by_identity = {
        (
            str(row.get("tradingsymbol", "")).strip().upper(),
            str(row.get("side", "")).strip().upper(),
        ): row
        for row in snapshot.get("candidates") or []
        if isinstance(row, Mapping)
    }
    for raw in source_candidates:
        if not isinstance(raw, Mapping):
            raise SourceIncompleteError("scanner candidate must be an object")
        if str(raw.get("side", "")).upper() != setup.side:
            continue
        symbol = str(raw.get("tradingsymbol", "")).strip().upper()
        token = int(raw.get("instrument_token", 0) or 0)
        tick_size = _safe_float(raw.get("tick_size", 0.05))
        futures_symbol = str(raw.get("futures_tradingsymbol", "")).strip().upper()
        futures_token = int(raw.get("futures_instrument_token", 0) or 0)
        if (
            not symbol
            or token <= 0
            or tick_size <= 0
            or not futures_symbol
            or futures_token <= 0
        ):
            raise SourceIncompleteError("scanner candidate lacks frozen cash/futures mapping")
        proof_mapping = proof_by_future.get(futures_symbol)
        if not isinstance(proof_mapping, Mapping):
            raise SourceIncompleteError(
                f"scanner future is absent from frozen universe proof: {futures_symbol}"
            )
        try:
            signal_timestamp = _parse_aware_ist(
                raw.get("signal_timestamp"), "candidate_signal_timestamp"
            )
        except SourceIncompleteError:
            raise
        mapping_checks = (
            signal_timestamp == signal_at,
            str(raw.get("data_contract", V6_SCANNER_DATA_CONTRACT))
            == V6_SCANNER_DATA_CONTRACT,
            str(proof_mapping.get("equity_symbol", "")).strip().upper() == symbol,
            int(proof_mapping.get("equity_instrument_token", 0)) == token,
            int(proof_mapping.get("instrument_token", 0)) == futures_token,
            _safe_float(proof_mapping.get("equity_tick_size")) == tick_size,
            str(raw.get("underlying", symbol)).strip().upper()
            == str(proof_mapping.get("underlying", "")).strip().upper(),
        )
        if not all(mapping_checks):
            raise SourceIncompleteError(
                f"scanner candidate/current universe identity mismatch: {symbol}"
            )
        if symbol in seen:
            raise SourceIncompleteError(f"duplicate eligible scanner candidate: {symbol}")
        seen.add(symbol)
        direct = dict(cash_constituent_audit.get(symbol) or {})
        if not direct:
            raise SourceIncompleteError(f"cash 5x1m direct audit missing: {symbol}")
        if independent_authority:
            five = dict(raw.get("_cash_features") or {})
            oi_pair = dict(raw.get("_oi_pair") or {})
            if not five or not oi_pair:
                raise SourceIncompleteError(
                    f"independent candidate source bundle is incomplete: {symbol}"
                )
        else:
            five = dict(
                cash_feature_loader(
                    symbol,
                    signal_at,
                    paths.five_minute_root,
                    paths.cash_slot_root,
                    observed_at=observed_at,
                )
            )
            oi_pair = dict(
                futures_oi_loader(
                    futures_symbol,
                    signal_at,
                    paths.futures_five_minute_root,
                    expected_token=futures_token,
                )
            )
        proof_oi_projection = [
            {
                "timestamp": str(item.get("timestamp")),
                "oi": _safe_float(item.get("oi")),
                "quality_state": str(item.get("quality_state")),
            }
            for item in proof_mapping.get("rows") or []
            if isinstance(item, Mapping)
        ]
        observed_oi_projection = [
            {
                "timestamp": str(item.get("timestamp")),
                "oi": _safe_float(item.get("oi")),
                "quality_state": str(item.get("quality_state")),
            }
            for item in oi_pair.get("rows") or []
            if isinstance(item, Mapping)
        ]
        if (
            oi_pair.get("source_file_sha256")
            != proof_mapping.get("source_file_sha256")
            or int(oi_pair.get("source_file_size_bytes", -1))
            != int(proof_mapping.get("source_size_bytes", -2))
            or observed_oi_projection != proof_oi_projection
        ):
            raise SourceIncompleteError(
                f"candidate OI source changed after universe proof: {futures_symbol}"
            )
        o, h, low, close, volume = (
            _safe_float(five.get(name))
            for name in ("open", "high", "low", "close", "volume")
        )
        if min(o, h, low, close) <= 0 or volume < 0:
            raise SourceIncompleteError(f"invalid exact 5m OHLCV for {symbol}")
        if h < max(o, close) or low > min(o, close) or h < low:
            raise SourceIncompleteError(f"invalid exact 5m OHLC geometry for {symbol}")
        for name, observed_value in (
            ("open", o),
            ("high", h),
            ("low", low),
            ("close", close),
            ("volume", volume),
        ):
            direct_value = _safe_float(direct.get(name))
            if not math.isclose(
                observed_value, direct_value, rel_tol=0.0, abs_tol=1e-6
            ):
                raise SourceIncompleteError(
                    f"current cash 5m/direct 5x1m {name} mismatch for {symbol}: "
                    f"{observed_value}/{direct_value}"
                )
        signal_close = _safe_float(raw.get("signal_close"))
        if not independent_authority and not math.isclose(
            close, signal_close, rel_tol=0.0, abs_tol=1e-8
        ):
            raise SourceIncompleteError(
                f"V6 scanner/cash 5m close mismatch for {symbol}: {signal_close}/{close}"
            )
        authoritative = {
            "side": setup.side,
            "signal_end": setup.signal_end,
            "price_change_pct": _safe_float(five.get("price_change_pct")),
            "oi": _safe_float(oi_pair.get("oi")),
            "prev_oi": _safe_float(oi_pair.get("prev_oi")),
            "oi_change_pct": _safe_float(oi_pair.get("oi_change_pct")),
            "volume_ratio": _safe_float(five.get("volume_ratio")),
            "traded_value": _safe_float(five.get("traded_value")),
            "ema9": _safe_float(five.get("ema9")),
            "ema20": _safe_float(five.get("ema20")),
            "ema50": _safe_float(five.get("ema50")),
        }
        if not _candidate_passes_setup(authoritative, setup):
            continue
        picker_value = _picker_value(authoritative, setup.picker)
        candidate_id = f"{paths.session_date.isoformat()}|{setup.setup_id}|{symbol}"
        selected.append(
            {
                "schema_version": "fno_v8_combined_paper_candidate_v1",
                "candidate_id": candidate_id,
                "session_date": paths.session_date.isoformat(),
                "signal_time": signal_at.isoformat(),
                "signal_end": setup.signal_end,
                "setup_id": setup.setup_id,
                "side": setup.side,
                "symbol": symbol,
                "tradingsymbol": symbol,
                "futures_symbol": futures_symbol,
                "futures_tradingsymbol": futures_symbol,
                "equity_instrument_token": token,
                "instrument_token": token,
                "futures_instrument_token": futures_token,
                "tick_size": tick_size,
                "lot_size": 1,
                "five_min_open": o,
                "five_min_high": h,
                "five_min_low": low,
                "five_min_close": close,
                "five_min_volume": volume,
                **authoritative,
                "picker": setup.picker,
                "picker_value": picker_value,
                "candidate_authority_artifact_sha256": source_sha256,
                "source_policy_version": SOURCE_POLICY_VERSION,
                "candidate_authority": (
                    "INDEPENDENT_ALL_MAPPED_STOCKS"
                    if independent_authority
                    else "V6_SCANNER_CANDIDATE_ROW"
                ),
                "cash_feature_source_path": five.get("source_path"),
                "cash_feature_source_sha256": five.get("source_file_sha256"),
                "cash_feature_source_size_bytes": five.get("source_file_size_bytes"),
                "cash_causal_prefix_count": five.get("causal_prefix_count"),
                "cash_causal_prefix_first_ist": five.get("causal_prefix_first_ist"),
                "cash_causal_prefix_last_ist": five.get("causal_prefix_last_ist"),
                "cash_causal_prefix_sha256": five.get("causal_prefix_sha256"),
                "cash_slot_marker_path": five.get("cash_slot_marker_path"),
                "cash_slot_marker_sha256": five.get("cash_slot_marker_sha256"),
                "cash_direct_constituents": direct.get("constituents"),
                "cash_direct_constituents_sha256": direct.get("constituents_sha256"),
                "cash_direct_app_name": direct.get("app_name"),
                "futures_oi_source_path": oi_pair.get("source_path"),
                "futures_oi_source_sha256": oi_pair.get("source_file_sha256"),
                "futures_oi_rows": oi_pair.get("rows"),
                "futures_oi_rows_sha256": oi_pair.get("rows_sha256"),
                "universe_mapping": {
                    name: proof_mapping.get(name)
                    for name in (
                        "tradingsymbol",
                        "instrument_token",
                        "underlying",
                        "expiry",
                        "contract_month",
                        "lot_size",
                        "tick_size",
                        "equity_symbol",
                        "equity_instrument_token",
                        "equity_tick_size",
                        "data_contract",
                        "source_file_sha256",
                    )
                },
                "v6_scanner_scalar_diagnostic": (
                    {
                        name: (
                            scanner_by_identity.get((symbol, setup.side), {}).get(name)
                            if independent_authority
                            else raw.get(name)
                        )
                        for name in (
                            "price_change_pct",
                            "oi",
                            "prev_oi",
                            "oi_change_pct",
                            "volume_ratio",
                            "traded_value",
                            "ema9",
                            "ema20",
                            "ema50",
                        )
                    }
                    if snapshot.get("candidates") is not None
                    else None
                ),
                "present_in_v6_scanner_diagnostic": (
                    (symbol, setup.side) in scanner_by_identity
                    if snapshot.get("candidates") is not None
                    else None
                ),
                "v6_scanner_diagnostic_state": (
                    "OBSERVED"
                    if snapshot.get("candidates") is not None
                    else "PENDING_POST_REGISTRATION"
                ),
            }
        )
    selected.sort(
        key=lambda row: (
            -float(row["picker_value"]),
            -float(row["traded_value"]),
            str(row["symbol"]),
        )
    )
    for rank, row in enumerate(selected, start=1):
        row["frozen_rank"] = rank
    return selected


def archive_slot_inputs(
    paths: SessionPaths,
    setup: config.PaperSetup,
    raw_source: bytes,
    source_sha256: str,
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    source_path = (
        paths.source_root
        / f"slot_{setup.signal_end.replace(':', '')}_independent_authority.json"
    )
    observed_sha = _write_immutable_bytes(source_path, raw_source)
    if observed_sha != source_sha256:
        raise SessionContractError("source evidence digest changed during archive")
    candidate_payload = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "V8_CANDIDATE_BOOK",
        "session_date": paths.session_date.isoformat(),
        "setup": asdict(setup),
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "source_policy_version": SOURCE_POLICY_VERSION,
        "source_role": "INDEPENDENT_ALL_MAPPED_STOCKS_CANDIDATE_AUTHORITY",
        "source_path": str(source_path),
        "source_sha256": source_sha256,
        "v6_scanner_diagnostic_state_at_registration": "PENDING_POST_REGISTRATION",
        "candidate_count": len(candidates),
        "candidates": list(candidates),
    }
    candidate_path = (
        paths.candidate_root / f"candidate_book_{setup.setup_id.replace(':', '')}.json"
    )
    candidate_sha = _write_immutable_json(candidate_path, candidate_payload)
    return {
        "source_path": str(source_path),
        "source_sha256": source_sha256,
        "candidate_path": str(candidate_path),
        "candidate_sha256": candidate_sha,
        "candidate_count": len(candidates),
    }


def archive_pending_v6_scanner_diagnostics(
    paths: SessionPaths,
    telemetry: SessionTelemetry,
    ingested_slots: Iterable[str],
    *,
    observed_at: datetime,
) -> bool:
    """Archive/reconcile V6 diagnostics without gating V8 registration.

    The independent all-universe source is the only candidate authority.  This
    function is therefore called after the registered engine checkpoint is
    durable.  A finalized V6 snapshot remains a required diagnostic by S+1+3,
    but waiting for its historically late S+57..59 publication can never turn
    a completed confirmation candle into a retrospective registration.
    """

    observed = _normalize_now(observed_at)
    changed = False
    for signal_end in sorted(set(ingested_slots)):
        slot_state = telemetry.slots.setdefault(signal_end, {})
        diagnostic_path = (
            paths.source_root
            / f"slot_{signal_end.replace(':', '')}_v6_scanner_diagnostic.json"
        )
        reconciliation_path = (
            paths.source_root
            / f"slot_{signal_end.replace(':', '')}_v6_scanner_reconciliation.json"
        )
        authority_path = (
            paths.independent_candidate_source_root
            / f"slot_{signal_end.replace(':', '')}.json"
        )
        try:
            authority_raw = authority_path.read_bytes()
            authority_payload = json.loads(authority_raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SessionContractError(
                "registered independent candidate authority is unreadable"
            ) from exc
        if not isinstance(authority_payload, dict):
            raise SessionContractError(
                "registered independent candidate authority must be an object"
            )
        _validate_independent_candidate_source(
            paths,
            signal_end,
            authority_payload,
            universe_proof_sha256=str(
                authority_payload.get("universe_oi_proof_sha256", "")
            ),
            strict_cash_source_sha256=str(
                authority_payload.get("strict_cash_source_sha256", "")
            ),
        )
        authority_artifact_sha = _sha256_bytes(authority_raw)

        # Crash after diagnostic fsync but before the next checkpoint is
        # recovered by validating the existing immutable reconciliation rather
        # than constructing it with a different observed_at timestamp.
        if reconciliation_path.is_file():
            try:
                reconciliation_raw = reconciliation_path.read_bytes()
                reconciliation = json.loads(reconciliation_raw.decode("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SessionContractError(
                    "immutable V6 scanner reconciliation is unreadable"
                ) from exc
            if not isinstance(reconciliation, dict):
                raise SessionContractError("V6 scanner reconciliation must be an object")
            unsigned = dict(reconciliation)
            claimed = str(unsigned.pop("reconciliation_sha256", ""))
            checks = (
                reconciliation.get("schema_version") == EVIDENCE_SCHEMA_VERSION,
                reconciliation.get("kind")
                == "POST_REGISTRATION_V6_SCANNER_DIAGNOSTIC",
                reconciliation.get("session_date") == paths.session_date.isoformat(),
                reconciliation.get("signal_end") == signal_end,
                reconciliation.get("source_policy_version") == SOURCE_POLICY_VERSION,
                reconciliation.get("candidate_authority_artifact_sha256")
                == authority_artifact_sha,
                reconciliation.get("v6_scanner_source_path") == str(diagnostic_path),
                diagnostic_path.is_file(),
                reconciliation.get("v6_scanner_source_sha256")
                == (_sha256_file(diagnostic_path) if diagnostic_path.is_file() else None),
                claimed == common.canonical_json_sha256(unsigned),
            )
            if not all(checks):
                raise SessionContractError(
                    "immutable V6 scanner reconciliation binding mismatch"
                )
            slot_state.update(
                {
                    "v6_scanner_diagnostic_state": "ARCHIVED_POST_REGISTRATION",
                    "v6_scanner_source_path": str(diagnostic_path),
                    "v6_scanner_source_sha256": reconciliation[
                        "v6_scanner_source_sha256"
                    ],
                    "v6_scanner_reconciliation_path": str(reconciliation_path),
                    "v6_scanner_reconciliation_sha256": _sha256_bytes(
                        reconciliation_raw
                    ),
                    "v6_missing_independent_eligible_count": len(
                        reconciliation.get("v6_missing_independent_eligible") or []
                    ),
                }
            )
            continue

        confirmation_due = _slot_datetime(
            paths.session_date, signal_end
        ) + timedelta(minutes=1, seconds=BOUNDARY_BUFFER_SECONDS)
        try:
            snapshot, scanner_raw, scanner_sha = load_finalized_v6_scanner_slot(
                paths, signal_end, observed_at=observed
            )
        except SourceNotReadyError:
            slot_state["v6_scanner_diagnostic_state"] = "PENDING_POST_REGISTRATION"
            if observed >= confirmation_due:
                raise SourceIncompleteError(
                    f"V6 diagnostic {signal_end} was not finalized by S+1+3"
                )
            continue
        scanner_published_at = _parse_aware_ist(
            snapshot.get("published_at_ist"), "published_at_ist"
        )
        if scanner_published_at >= confirmation_due:
            raise SourceIncompleteError(
                f"V6 diagnostic {signal_end} was published after S+1+3"
            )
        observed_scanner_sha = _write_immutable_bytes(diagnostic_path, scanner_raw)
        if observed_scanner_sha != scanner_sha:
            raise SessionContractError("V6 diagnostic changed during immutable archive")

        scanner_identities = {
            (
                str(row.get("tradingsymbol", "")).strip().upper(),
                str(row.get("side", "")).strip().upper(),
            )
            for row in snapshot.get("candidates") or []
            if isinstance(row, Mapping)
        }
        independent_identities = {
            (
                str(row.get("tradingsymbol", "")).strip().upper(),
                str(row.get("side", "")).strip().upper(),
            )
            for row in authority_payload.get("eligible_rows") or []
            if isinstance(row, Mapping)
        }
        reconciliation: dict[str, Any] = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "kind": "POST_REGISTRATION_V6_SCANNER_DIAGNOSTIC",
            "session_date": paths.session_date.isoformat(),
            "signal_end": signal_end,
            "source_policy_version": SOURCE_POLICY_VERSION,
            "candidate_authority": "INDEPENDENT_ALL_MAPPED_STOCKS",
            "candidate_authority_artifact_path": str(authority_path),
            "candidate_authority_artifact_sha256": authority_artifact_sha,
            "candidate_authority_source_sha256": authority_payload.get(
                "candidate_source_sha256"
            ),
            "v6_scanner_role": "POST_REGISTRATION_DIAGNOSTIC_ONLY",
            "v6_scanner_source_path": str(diagnostic_path),
            "v6_scanner_source_sha256": scanner_sha,
            "v6_scanner_published_at_ist": scanner_published_at.isoformat(),
            "diagnostic_observed_at_ist": observed.isoformat(),
            "confirmation_due_ist": confirmation_due.isoformat(),
            "scanner_published_before_confirmation_due": True,
            "independent_eligible_count": len(independent_identities),
            "v6_diagnostic_candidate_count": len(scanner_identities),
            "v6_missing_independent_eligible": [
                {"tradingsymbol": symbol, "side": side}
                for symbol, side in sorted(independent_identities - scanner_identities)
            ],
            "v6_only_diagnostic_candidates": [
                {"tradingsymbol": symbol, "side": side}
                for symbol, side in sorted(scanner_identities - independent_identities)
            ],
        }
        reconciliation["reconciliation_sha256"] = common.canonical_json_sha256(
            reconciliation
        )
        reconciliation_artifact_sha = _write_immutable_json(
            reconciliation_path, reconciliation
        )
        slot_state.update(
            {
                "v6_scanner_diagnostic_state": "ARCHIVED_POST_REGISTRATION",
                "v6_scanner_source_path": str(diagnostic_path),
                "v6_scanner_source_sha256": scanner_sha,
                "v6_scanner_reconciliation_path": str(reconciliation_path),
                "v6_scanner_reconciliation_sha256": reconciliation_artifact_sha,
                "v6_missing_independent_eligible_count": len(
                    independent_identities - scanner_identities
                ),
            }
        )
        changed = True
    return changed


def archive_event(
    paths: SessionPaths,
    event: Mapping[str, Any],
    *,
    kind: str,
) -> Path:
    envelope = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": str(kind).upper(),
        "session_date": paths.session_date.isoformat(),
        "strategy_fingerprint": config.strategy_fingerprint(),
        "event": dict(event),
    }
    payload = _json_bytes(envelope)
    digest = _sha256_bytes(payload)
    timestamp = str(event.get("event_time", event.get("timestamp", "undated")))
    safe_time = "".join(char for char in timestamp if char.isdigit())[:14] or "undated"
    destination = paths.event_root / f"{safe_time}_{str(kind).lower()}_{digest}.json"
    _write_immutable_bytes(destination, payload)
    return destination


def _engine_module() -> Any:
    try:
        return importlib.import_module("fno_v8_combined_paper_engine")
    except ImportError as exc:
        raise SessionContractError("V8-Combined PAPER engine is not installed") from exc


def _construct_supported(cls: type[Any], payload: Mapping[str, Any]) -> Any:
    """Construct a frozen engine value without guessing private parameters."""

    parameters = inspect.signature(cls).parameters
    kwargs = {name: payload[name] for name in parameters if name in payload}
    missing = [
        name
        for name, parameter in parameters.items()
        if name not in kwargs
        and parameter.default is inspect.Parameter.empty
        and parameter.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    if missing:
        raise SessionContractError(
            f"engine {cls.__name__} contract requires unmapped fields: {missing}"
        )
    return cls(**kwargs)


def _paper_candidate(module: Any, row: Mapping[str, Any]) -> Any:
    payload = dict(row)
    payload["signal_time"] = pd.Timestamp(row["signal_time"]).to_pydatetime()
    # The engine uses the backtest names; aliases above remain in evidence for
    # operational readers and source-token construction.
    payload.setdefault("five_min_volume", row.get("five_min_volume", 0.0))
    return _construct_supported(module.PaperCandidate, payload)


def _completed_bar(module: Any, row: Mapping[str, Any], timestamp: pd.Timestamp) -> Any:
    payload = {
        "timestamp": timestamp,
        "ts": timestamp,
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row.get("volume", 0.0)),
        "gap_filled": bool(row.get("gap_filled", False)),
        "opening_snapshot": bool(row.get("opening_snapshot", False)),
        "provisional_stale": bool(row.get("provisional_stale", False)),
    }
    return _construct_supported(module.CompletedMinuteBar, payload)


def _event_mapping(event: Any) -> dict[str, Any]:
    if hasattr(event, "to_record") and callable(event.to_record):
        value = event.to_record()
    elif is_dataclass(event):
        value = asdict(event)
    elif isinstance(event, Mapping):
        value = dict(event)
    else:
        value = vars(event)
    if not isinstance(value, Mapping):
        raise SessionContractError("engine event is not serializable as an object")
    return dict(_jsonable(value))


def archive_engine_events(paths: SessionPaths, events: Iterable[Any]) -> None:
    for event in events:
        record = _event_mapping(event)
        kind = str(
            record.get("event_type", record.get("kind", record.get("state", "ENGINE")))
        )
        archive_event(paths, record, kind=kind)


def register_candidate_book(
    engine: Any,
    module: Any,
    setup: config.PaperSetup,
    candidates: Sequence[Mapping[str, Any]],
    paths: SessionPaths,
) -> list[Any]:
    values = [_paper_candidate(module, row) for row in candidates]
    events = list(
        engine.register_candidates(
            setup.setup_id,
            pd.Timestamp(_slot_datetime(paths.session_date, setup.signal_end)),
            values,
        )
    )
    archive_engine_events(paths, events)
    return events


def process_engine_minute(
    engine: Any,
    module: Any,
    expected_end: datetime,
    frame: pd.DataFrame,
    paths: SessionPaths,
) -> list[Any]:
    timestamp = pd.Timestamp(expected_end)
    bars: dict[str, Any] = {}
    for row in frame.to_dict("records"):
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol or symbol in bars:
            raise SessionContractError("minute snapshot has missing/duplicate symbols")
        bars[symbol] = _completed_bar(module, row, timestamp)
    events = list(engine.process_completed_minute(timestamp, bars))
    archive_engine_events(paths, events)
    return events


def engine_required_symbols(engine: Any) -> tuple[str, ...]:
    if hasattr(engine, "required_symbols"):
        return tuple(sorted({str(value).strip().upper() for value in engine.required_symbols()}))
    active_states = {
        "REGISTERED",
        "WAITING_CONFIRMATION",
        "PENDING",
        "CONFIRMED",
        "ENTRY_PENDING",
        "OPEN",
    }
    symbols: set[str] = set()
    for record in engine.records():
        state = str(record.get("state", record.get("status", ""))).upper()
        if state in active_states:
            symbol = str(record.get("symbol", "")).strip().upper()
            if symbol:
                symbols.add(symbol)
    return tuple(sorted(symbols))


def persist_checkpoint(
    paths: SessionPaths,
    engine: Any,
    telemetry: SessionTelemetry,
    *,
    processed_clock_end: datetime,
    ingested_slots: Iterable[str],
    symbol_tokens: Mapping[str, int],
) -> dict[str, Any]:
    envelope = {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "session_schema_version": SESSION_SCHEMA_VERSION,
        "session_date": paths.session_date.isoformat(),
        "mode": config.MODE,
        "paper_only": True,
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "runtime_bundle_sha256": telemetry.runtime_bundle_sha256,
        "processed_clock_end_ist": processed_clock_end.isoformat(),
        "ingested_slots": sorted(set(ingested_slots)),
        "symbol_tokens": dict(sorted(symbol_tokens.items())),
        "telemetry": telemetry.as_payload(),
        "engine_checkpoint": engine.checkpoint(),
    }
    payload = _json_bytes(envelope)
    digest = _sha256_bytes(payload)
    stamp = processed_clock_end.strftime("%H%M")
    archive_name = f"checkpoint_{stamp}_{digest}.json"
    archive_path = paths.day_checkpoint_root / archive_name
    _write_immutable_bytes(archive_path, payload)
    pointer = {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "session_date": paths.session_date.isoformat(),
        "archive_name": archive_name,
        "archive_sha256": digest,
    }
    _atomic_write_bytes(paths.latest_checkpoint_path, _json_bytes(pointer))
    return pointer


def load_checkpoint(
    paths: SessionPaths,
    module: Any,
    *,
    expected_runtime_bundle_sha256: str,
) -> tuple[Any, datetime, set[str], dict[str, int], SessionTelemetry] | None:
    pointer_path = paths.latest_checkpoint_path
    if not pointer_path.is_file():
        return None
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SessionContractError("latest checkpoint pointer is invalid") from exc
    archive_name = str(pointer.get("archive_name", ""))
    if Path(archive_name).name != archive_name or not archive_name:
        raise SessionContractError("checkpoint archive name is unsafe")
    archive_path = paths.day_checkpoint_root / archive_name
    try:
        payload = archive_path.read_bytes()
    except OSError as exc:
        raise SessionContractError("checkpoint archive is missing/unreadable") from exc
    digest = _sha256_bytes(payload)
    if digest != pointer.get("archive_sha256"):
        raise SessionContractError("checkpoint archive hash mismatch")
    try:
        envelope = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SessionContractError("checkpoint archive JSON is invalid") from exc
    checks = (
        envelope.get("schema_version") == CHECKPOINT_SCHEMA_VERSION,
        envelope.get("session_date") == paths.session_date.isoformat(),
        envelope.get("mode") == config.MODE,
        envelope.get("paper_only") is True,
        envelope.get("setup_book_sha256") == config.COMBINED_SETUP_BOOK_SHA256,
        envelope.get("strategy_fingerprint") == config.strategy_fingerprint(),
        envelope.get("runtime_bundle_sha256") == expected_runtime_bundle_sha256,
    )
    if not all(checks):
        raise SessionContractError("checkpoint semantic binding mismatch")
    processed = _parse_aware_ist(
        envelope.get("processed_clock_end_ist"), "processed_clock_end_ist"
    )
    engine = module.V8CombinedPaperEngine.from_checkpoint(
        envelope.get("engine_checkpoint")
    )
    telemetry_payload = dict(envelope.get("telemetry") or {})
    allowed_fields = {item.name for item in __import__("dataclasses").fields(SessionTelemetry)}
    telemetry = SessionTelemetry(
        **{key: value for key, value in telemetry_payload.items() if key in allowed_fields}
    )
    slots = {str(value) for value in envelope.get("ingested_slots") or []}
    tokens = {
        str(symbol).strip().upper(): int(token)
        for symbol, token in dict(envelope.get("symbol_tokens") or {}).items()
    }
    return engine, processed, slots, tokens, telemetry


def recover_terminal_checkpoint_before_credentials(
    paths: SessionPaths,
    module: Any,
    *,
    expected_runtime_bundle_sha256: str,
) -> int | None:
    """Finalize a crash-window terminal checkpoint without reopening control.

    A kill/revoke remains engaged after intervention, so requiring a fresh
    activation merely to write completion would make a safely terminated book
    unrecoverable.  This path performs no authentication or market-data call.
    It accepts only a hash-bound terminal state (or exact-15:30 checkpoint) and
    explicitly downgrades any retained active/unresolved economics.
    """

    if not paths.latest_checkpoint_path.is_file():
        return None
    restored = load_checkpoint(
        paths,
        module,
        expected_runtime_bundle_sha256=expected_runtime_bundle_sha256,
    )
    if restored is None:  # pragma: no cover - guarded by the path check
        return None
    engine, processed, _slots, _tokens, telemetry = restored
    if engine.last_processed_minute != processed:
        raise SessionContractError("terminal recovery engine/checkpoint minute mismatch")
    exact_square_off = _slot_datetime(paths.session_date, config.SQUARE_OFF)
    terminal_checkpoint_states = {
        "STOPPED_CONTROL_INTERVENTION",
        "DATA_INCOMPLETE",
        "STOPPED_CONTROL_UNRESOLVED",
        "STOPPED_OPERATOR_INTERRUPT",
    }
    exact_1530 = processed == exact_square_off
    if telemetry.state not in terminal_checkpoint_states and not exact_1530:
        return None

    telemetry.runtime_bundle_sha256 = expected_runtime_bundle_sha256
    active = engine_required_symbols(engine)
    unresolved = _unresolved_filled_records(engine.records())
    if exact_1530 and not telemetry.data_incomplete and not active and not unresolved:
        telemetry.state = "COMPLETED"
        telemetry.phase = "EXACT_1530_COMPLETE"
    elif telemetry.state == "STOPPED_CONTROL_INTERVENTION" and not active and not unresolved:
        # Preserve the original state/phase so a manifest written immediately
        # before a crash remains byte-for-byte idempotent.
        pass
    else:
        telemetry.state = "DATA_INCOMPLETE"
        telemetry.phase = "TERMINAL_CHECKPOINT_RECOVERY_INVALID_OR_UNRESOLVED"
        telemetry.data_incomplete = True
        telemetry.messages.append(
            "Recovered a terminal checkpoint without credentials; active or "
            "unresolved paper economics remain explicitly invalid."
        )
    _finalize_session(paths, telemetry, engine)
    return 0 if telemetry.state == "COMPLETED" else 2


def write_strategy_manifest(
    paths: SessionPaths,
    decision: control.ActivationDecision,
    runtimes: Sequence[market_data.AppRuntime],
    *,
    dashboard_runtime_identity: Mapping[str, Any],
) -> Path:
    runtime_sources = list(control.runtime_bundle_records())
    if config.canonical_json_sha256(runtime_sources) != decision.runtime_bundle_sha256:
        raise SessionContractError("runtime source records changed before manifest seal")
    manifest = {
        "schema_version": SESSION_SCHEMA_VERSION,
        "session_date": paths.session_date.isoformat(),
        "mode": config.MODE,
        "paper_only": True,
        "broker_orders_authorized": False,
        "strategy_version": config.STRATEGY_VERSION,
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "runtime_bundle_sha256": decision.runtime_bundle_sha256,
        "runtime_bundle_sources": runtime_sources,
        "permit_id": decision.permit_id,
        "permit_sha256": decision.permit_sha256,
        "market_data_policy": market_data.MARKET_DATA_POLICY_VERSION,
        "required_kite_apps": config.REQUIRED_KITE_APPS,
        "app_roster": market_data.app_roster_payload(runtimes),
        "app_roster_sha256": market_data.app_roster_sha256(runtimes),
        "dashboard_runtime_identity": dict(_jsonable(dashboard_runtime_identity)),
        "source_dependency": {
            "role": "CURRENT_DATE_FINALIZED_POST_REGISTRATION_DIAGNOSTIC_ONLY",
            "root": str(paths.scanner_root),
            "schema_version": V6_SCANNER_SCHEMA_VERSION,
            "strategy_version": V6_SCANNER_STRATEGY_VERSION,
            "strategy_fingerprint": V6_SCANNER_STRATEGY_FINGERPRINT,
            "data_contract": V6_SCANNER_DATA_CONTRACT,
            "v8_gates_and_rank_reapplied": True,
            "candidate_authority": "INDEPENDENT_ALL_MAPPED_STOCKS_STRICT_V8_SOURCE",
            "v6_candidate_rows_authoritative": False,
            "v6_scanner_blocks_prospective_registration": False,
            "v6_runtime_imported": False,
            "current_cash_five_minute_root": str(paths.five_minute_root),
            "current_cash_slot_marker_root": str(paths.cash_slot_root),
            "futures_five_minute_root": str(paths.futures_five_minute_root),
            "futures_final_slot_root": str(paths.futures_slot_root),
            "near_month_universe_path": str(paths.near_month_universe_path),
            "cash_feature_contract": (
                "CURRENT_LIVE_5M_FINAL_MARKER_SOURCE_1M_COUNT_5_V1"
            ),
            "cash_signal_audit_contract": (
                "DIRECT_KITE_EXACT_COMPLETED_CASH_S_MINUS_4_THROUGH_S_V1"
            ),
            "futures_oi_contract": "EXACT_NFO_5M_OI_S_AND_S_MINUS_5_V1",
            "v6_oi_superset_proof_contract": (
                "ALL_MAPPED_STOCK_FUTURES_SORTED_PREDECESSOR_EXACT_S_MINUS_5_V1"
            ),
            "universe_proof_full_source_sha256_bound": True,
        },
        "execution_contract": {
            "same_session_only": True,
            "completed_one_minute_ohlcv_only": True,
            "ltp_fallback": False,
            "retroactive_entries": False,
            "global_single_writer_book": True,
            "square_off": config.SQUARE_OFF,
        },
    }
    path = paths.day_session_root / "strategy_manifest.json"
    _write_immutable_json(path, manifest)
    return path


def write_completion_artifacts(
    paths: SessionPaths,
    telemetry: SessionTelemetry,
    *,
    records: Sequence[Mapping[str, Any]],
) -> tuple[Path, Path]:
    """Seal all already-written session artifacts, then seal completion."""

    artifacts: list[dict[str, Any]] = []
    roots = (
        paths.day_session_root,
        paths.day_evidence_root,
        paths.day_checkpoint_root,
    )
    excluded = {
        paths.day_session_root / "artifact_manifest.json",
        paths.day_session_root / "completion.json",
    }
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(value for value in root.rglob("*") if value.is_file()):
            if path in excluded or path.name == "latest.json":
                continue
            artifacts.append(
                {
                    "path": str(path),
                    "size_bytes": int(path.stat().st_size),
                    "sha256": _sha256_file(path),
                }
            )
    manifest_payload = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "ARTIFACT_MANIFEST",
        "session_date": paths.session_date.isoformat(),
        "mode": config.MODE,
        "paper_only": True,
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "runtime_bundle_sha256": telemetry.runtime_bundle_sha256,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }
    manifest_path = paths.day_session_root / "artifact_manifest.json"
    manifest_sha = _write_immutable_json(manifest_path, manifest_payload)
    closed_records = _terminal_trade_records(records)
    unresolved_records = _unresolved_filled_records(records)
    funnel = flow_funnel(records)
    completion_payload = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "SESSION_COMPLETION",
        "session_date": paths.session_date.isoformat(),
        "state": telemetry.state,
        "phase": telemetry.phase,
        "mode": config.MODE,
        "paper_only": True,
        "valid_economic_result": _valid_economic_result(telemetry, records),
        "data_incomplete": telemetry.data_incomplete,
        "completed_minutes": telemetry.completed_minutes,
        "incomplete_minutes": telemetry.incomplete_minutes,
        "last_completed_minute": telemetry.last_completed_minute,
        "engine_record_count": len(records),
        # Retain trade_count for the dashboard contract, but spell out each
        # economic stage so a fill can never be mistaken for a finite close.
        "trade_count": len(closed_records),
        "filled_count": funnel["filled"],
        "closed_trade_count": len(closed_records),
        "unresolved_filled_count": len(unresolved_records),
        "artifact_manifest_path": str(manifest_path),
        "artifact_manifest_sha256": manifest_sha,
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "runtime_bundle_sha256": telemetry.runtime_bundle_sha256,
    }
    completion_path = paths.day_session_root / "completion.json"
    _write_immutable_json(completion_path, completion_payload)
    return manifest_path, completion_path


def _record_value(record: Mapping[str, Any], *names: str, default: Any = "") -> Any:
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return default


def _terminal_trade_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    terminal_trade_states = {
        "STOPPED",
        "TARGETED",
        "SQUARE_OFF",
        "INTERVENTION_CLOSED",
    }
    result: list[dict[str, Any]] = []
    for value in records:
        record = dict(_jsonable(value))
        entry = _record_value(record, "entry_time", "entry_timestamp")
        state = str(_record_value(record, "status", "state", default="")).upper()
        exit_time = _record_value(record, "exit_time", "exit_timestamp")
        exit_price = _record_value(record, "exit_price", default=None)
        net_return = _record_value(record, "net_return_pct", default=None)
        net_pnl = _record_value(record, "net_pnl_rs", "net_pnl", default=None)
        try:
            finite_economics = all(
                math.isfinite(float(item)) for item in (exit_price, net_return, net_pnl)
            )
        except (TypeError, ValueError):
            finite_economics = False
        if entry and exit_time and state in terminal_trade_states and finite_economics:
            result.append(record)
    return result


def _unresolved_filled_records(
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    terminal_ids = {
        str(_record_value(record, "candidate_id"))
        for record in _terminal_trade_records(records)
    }
    return [
        dict(_jsonable(record))
        for record in records
        if _record_value(record, "entry_time", "entry_timestamp")
        and str(record.get("portfolio_decision", "")).upper() != "REJECTED"
        and str(_record_value(record, "candidate_id")) not in terminal_ids
    ]


def _valid_economic_result(
    telemetry: SessionTelemetry,
    records: Sequence[Mapping[str, Any]],
) -> bool:
    return bool(
        telemetry.state == "COMPLETED"
        and not telemetry.data_incomplete
        and not _unresolved_filled_records(records)
    )


def flow_funnel(records: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """Return cumulative flow counts plus mutually exclusive current buckets."""

    normalized = [dict(_jsonable(record)) for record in records]
    identities = [str(_record_value(record, "candidate_id")) for record in normalized]
    if any(not value for value in identities) or len(identities) != len(set(identities)):
        raise SessionContractError("engine records do not have unique candidate identities")
    closed_ids = {
        str(_record_value(record, "candidate_id"))
        for record in _terminal_trade_records(normalized)
    }
    unresolved_ids = {
        str(_record_value(record, "candidate_id"))
        for record in _unresolved_filled_records(normalized)
    }
    buckets = {
        "pending": 0,
        "expired": 0,
        "cancelled": 0,
        "rejected": 0,
        "closed": 0,
        "unresolved": 0,
        "other_terminal": 0,
    }
    pending_states = {"MONITORING", "CONFIRMED_WAITING_CAP", "PENDING_STOP"}
    expired_states = {"NO_CONFIRMATION", "WINDOW_EXPIRED", "PRECONF_INVALIDATED"}
    cancelled_states = {"POSTCONF_CANCELLED", "INTERVENTION_CANCELLED"}
    for record in normalized:
        candidate_id = str(_record_value(record, "candidate_id"))
        state = str(
            _record_value(record, "status", "unconstrained_status", default="")
        ).upper()
        if str(record.get("portfolio_decision", "")).upper() == "REJECTED":
            buckets["rejected"] += 1
        elif candidate_id in closed_ids:
            buckets["closed"] += 1
        elif candidate_id in unresolved_ids:
            buckets["unresolved"] += 1
        elif state in pending_states:
            buckets["pending"] += 1
        elif state in expired_states:
            buckets["expired"] += 1
        elif state in cancelled_states:
            buckets["cancelled"] += 1
        else:
            buckets["other_terminal"] += 1
    funnel = {
        "registered_5m_candidates": len(normalized),
        "confirmed_1m": sum(
            bool(_record_value(record, "confirmation_time", "confirmation_timestamp"))
            for record in normalized
        ),
        "filled": sum(
            bool(_record_value(record, "entry_time", "entry_timestamp"))
            and str(record.get("portfolio_decision", "")).upper() != "REJECTED"
            for record in normalized
        ),
        **buckets,
    }
    bucket_total = sum(funnel[name] for name in buckets)
    if bucket_total != funnel["registered_5m_candidates"]:
        raise SessionContractError("flow funnel current-state buckets do not reconcile")
    return funnel


def _net_pnl(record: Mapping[str, Any]) -> float:
    value = _record_value(
        record,
        "net_pnl_rs",
        "net_pnl",
        "pnl_rs",
        "paper_net_pnl_rs",
        default=0.0,
    )
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def _summary_rows(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    trades = _terminal_trade_records(records)
    groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for record in trades:
        setup = str(_record_value(record, "setup_id", default="UNKNOWN"))
        side = str(_record_value(record, "side", default="UNKNOWN"))
        groups.setdefault((setup, side), []).append(record)
    rows: list[dict[str, Any]] = []
    for (setup, side), values in sorted(groups.items()):
        pnls = [_net_pnl(record) for record in values]
        wins = sum(value > 0 for value in pnls)
        gross_profit = sum(value for value in pnls if value > 0)
        gross_loss = -sum(value for value in pnls if value < 0)
        rows.append(
            {
                "setup_id": setup,
                "side": side,
                "trades": len(values),
                "wins": wins,
                "win_pct": 100.0 * wins / len(values) if values else 0.0,
                "pf": gross_profit / gross_loss if gross_loss > 0 else None,
                "net_pnl_rs": sum(pnls),
            }
        )
    return rows


def render_report(
    paths: SessionPaths,
    telemetry: SessionTelemetry,
    records: Sequence[Mapping[str, Any]],
) -> str:
    trades = _terminal_trade_records(records)
    unresolved = _unresolved_filled_records(records)
    summary = _summary_rows(records)
    funnel = flow_funnel(records)
    validity = (
        "PENDING"
        if telemetry.state in {"STARTING", "RUNNING"}
        else "YES"
        if _valid_economic_result(telemetry, records)
        else "NO"
    )
    lines = [
        "# FnO V8-Combined Paper Shadow Session",
        "",
        f"- Status: **{telemetry.state}**",
        f"- Phase: `{telemetry.phase}`",
        f"- Session date (IST): `{paths.session_date.isoformat()}`",
        "- Execution: **PAPER ONLY - no broker orders**",
        f"- Activation: `{telemetry.activation_reason or 'NOT_EVALUATED'}`",
        f"- Setup book SHA-256: `{config.COMBINED_SETUP_BOOK_SHA256}`",
        f"- Strategy fingerprint: `{config.strategy_fingerprint()}`",
        f"- Runtime bundle SHA-256: `{telemetry.runtime_bundle_sha256 or 'NOT_BOUND'}`",
        f"- Healthy app-pool roster SHA-256: `{telemetry.app_roster_sha256 or 'NOT_AUTHENTICATED'}`",
        f"- Completed minute snapshots: {telemetry.completed_minutes}",
        f"- Incomplete minute snapshots: {telemetry.incomplete_minutes}",
        f"- Economic result valid: **{validity}**",
        f"- Unresolved filled PAPER positions: {len(unresolved)}",
        "",
        "The five-minute candidate authority is rebuilt independently across the "
        "full mapped stock universe from strict causal cash features and exact "
        "S/S-5 futures OI. The finalized current-date V6 scanner is archived only "
        "as a post-registration diagnostic and cannot add, omit, rank, or delay a "
        "candidate. It must still publish before S+1+3 for the session result to "
        "remain valid.",
        "",
        "## Slot intake",
        "",
        "| 5m slot | State | LONG candidates | SHORT candidates | V6 diagnostic | Candidate-authority SHA-256 |",
        "|---|---:|---:|---:|---|---|",
    ]
    for slot in ("09:25", "09:30", "09:35", "09:40", "09:45"):
        value = telemetry.slots.get(slot, {})
        lines.append(
            "| {slot} | {state} | {long} | {short} | {diagnostic} | `{digest}` |".format(
                slot=slot,
                state=value.get("state", "WAITING"),
                long=value.get("long_candidates", 0),
                short=value.get("short_candidates", 0),
                diagnostic=value.get("v6_scanner_diagnostic_state", "WAITING"),
                digest=value.get("source_sha256", ""),
            )
        )
    lines.extend(
        [
        "",
        "## 5m to 1m to PAPER-entry flow",
        "",
        "| Registered 5m | Confirmed 1m | Pending | Expired | Cancelled | Rejected | Filled | Closed | Unresolved | Other terminal |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        "| {registered_5m_candidates} | {confirmed_1m} | {pending} | {expired} | "
        "{cancelled} | {rejected} | {filled} | {closed} | {unresolved} | "
        "{other_terminal} |".format(**funnel),
        "",
        "## Paper result by 5-minute leg",
            "",
            "| Setup | Side | Trades | Wins | Win % | PF | Net P&L (Rs) |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    if summary:
        for row in summary:
            pf = "inf" if row["pf"] is None and row["net_pnl_rs"] > 0 else (
                "-" if row["pf"] is None else f"{row['pf']:.3f}"
            )
            lines.append(
                f"| {row['setup_id']} | {row['side']} | {row['trades']} | "
                f"{row['wins']} | {row['win_pct']:.2f} | {pf} | "
                f"{row['net_pnl_rs']:.2f} |"
            )
    else:
        lines.append("| - | - | 0 | 0 | - | - | 0.00 |")
    lines.extend(
        [
            "",
            "## Detailed paper trades",
            "",
            "| Date | Setup | Side | Symbol | Signal | Confirm | Entry | Entry Px | Exit | Exit Px | State | Net P&L (Rs) |",
            "|---|---|---|---|---|---|---|---:|---|---:|---|---:|",
        ]
    )
    if trades:
        for record in trades:
            lines.append(
                "| {date} | {setup} | {side} | {symbol} | {signal} | {confirm} | "
                "{entry} | {entry_px} | {exit} | {exit_px} | {state} | {pnl:.2f} |".format(
                    date=_record_value(record, "session_date", default=paths.session_date),
                    setup=_record_value(record, "setup_id"),
                    side=_record_value(record, "side"),
                    symbol=_record_value(record, "symbol"),
                    signal=_record_value(record, "signal_time", "signal_timestamp"),
                    confirm=_record_value(record, "confirmation_time", "confirmation_timestamp"),
                    entry=_record_value(record, "entry_time", "entry_timestamp"),
                    entry_px=_record_value(record, "entry_price", "fill_price", default=""),
                    exit=_record_value(record, "exit_time", "exit_timestamp"),
                    exit_px=_record_value(record, "exit_price", default=""),
                    state=_record_value(record, "state", "status", "outcome"),
                    pnl=_net_pnl(record),
                )
            )
    else:
        lines.append("| - | - | - | - | - | - | - | - | - | - | NO_TRADES | 0.00 |")
    if unresolved:
        lines.extend(
            [
                "",
                "## Unresolved filled PAPER positions (excluded from PF/win rate)",
                "",
                "| Setup | Side | Symbol | Entry | State | Reason |",
                "|---|---|---|---|---|---|",
            ]
        )
        for record in unresolved:
            lines.append(
                "| {setup} | {side} | {symbol} | {entry} | {state} | {reason} |".format(
                    setup=_record_value(record, "setup_id"),
                    side=_record_value(record, "side"),
                    symbol=_record_value(record, "symbol"),
                    entry=_record_value(record, "entry_time", "entry_timestamp"),
                    state=_record_value(record, "status", "state"),
                    reason=_record_value(record, "reason"),
                )
            )
    if telemetry.messages:
        lines.extend(["", "## Audit messages", ""])
        lines.extend(f"- {message}" for message in telemetry.messages)
    if telemetry.data_incomplete:
        lines.extend(
            [
                "",
                "> **DATA_INCOMPLETE:** economics are not a valid performance result. "
                "No candle or exit was synthesized, forward-filled, or replaced by LTP.",
            ]
        )
    return "\n".join(lines) + "\n"


def publish_report(
    paths: SessionPaths,
    telemetry: SessionTelemetry,
    records: Sequence[Mapping[str, Any]],
    *,
    final: bool,
) -> str:
    report = render_report(paths, telemetry, records)
    _atomic_write_bytes(paths.latest_report_path, report.encode("utf-8"))
    if final:
        _write_immutable_bytes(paths.session_report_path, report.encode("utf-8"))
    return report


def publish_final_trades(
    paths: SessionPaths,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized = [dict(_jsonable(record)) for record in records]
    fieldnames = sorted({str(key) for record in normalized for key in record})
    output = io.StringIO(newline="")
    if fieldnames:
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in normalized:
            writer.writerow(
                {
                    key: (
                        json.dumps(value, ensure_ascii=True, separators=(",", ":"))
                        if isinstance(value, (dict, list))
                        else value
                    )
                    for key, value in record.items()
                }
            )
    payload = output.getvalue().encode("utf-8")
    digest = _write_immutable_bytes(paths.trades_csv_path, payload)
    marker = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "kind": "FINAL_ENGINE_RECORDS",
        "session_date": paths.session_date.isoformat(),
        "path": str(paths.trades_csv_path),
        "sha256": digest,
        "row_count": len(normalized),
        "strategy_fingerprint": config.strategy_fingerprint(),
    }
    _write_immutable_json(paths.trades_csv_path.with_suffix(".json"), marker)
    return marker


def _enforce_paper_only_environment() -> None:
    observed = os.getenv("FNO_V8_COMBINED_EXECUTION_MODE", config.MODE).strip().upper()
    if observed != config.MODE or config.MODE != "PAPER" or config.PAPER_ONLY is not True:
        raise SessionContractError("FNO V8-Combined session is PAPER-only")


def _floor_due_minute(observed: datetime) -> datetime:
    adjusted = observed - timedelta(seconds=BOUNDARY_BUFFER_SECONDS)
    return adjusted.replace(second=0, microsecond=0)


def _activation_for_runtime(
    session_date: date,
    observed: datetime,
    *,
    control_paths: control.ControlPaths,
    bundle_provider: Callable[[], str],
) -> tuple[control.ActivationDecision, str]:
    digest = str(bundle_provider()).strip().lower()
    decision = control.evaluate_activation(
        session_date,
        now=observed,
        paths=control_paths,
        expected_runtime_bundle_sha256=digest,
    )
    return decision, digest


def _require_runtime_activation(
    session_date: date,
    observed: datetime,
    *,
    control_paths: control.ControlPaths,
    bundle_provider: Callable[[], str],
    expected_bundle_sha256: str,
) -> control.ActivationDecision:
    decision, current_digest = _activation_for_runtime(
        session_date,
        observed,
        control_paths=control_paths,
        bundle_provider=bundle_provider,
    )
    if current_digest != expected_bundle_sha256:
        raise control.ActivationBlockedError("RUNTIME_BUNDLE_CHANGED_DURING_SESSION")
    if not decision.allowed:
        raise control.ActivationBlockedError(decision.reason)
    return decision


def _approved_app_names(
    values: Any,
    *,
    minimum: int,
    require_order: bool = True,
) -> tuple[str, ...] | None:
    if not isinstance(values, (list, tuple)):
        return None
    names = tuple(str(value).strip() for value in values)
    if len(names) < max(1, int(minimum)) or len(names) != len(set(names)):
        return None
    expected_order = tuple(
        name for name in market_data.EXPECTED_APP_NAMES if name in set(names)
    )
    if set(names) != set(expected_order) or (require_order and names != expected_order):
        return None
    return expected_order


def _validate_approved_runtime_pool(
    runtimes: Sequence[market_data.AppRuntime],
    *,
    minimum_healthy_apps: int,
) -> tuple[market_data.AppRuntime, ...]:
    pool = tuple(runtimes)
    validator = getattr(market_data, "validate_runtime_pool", None)
    if callable(validator):
        try:
            pool = tuple(
                validator(pool, minimum_healthy_apps=int(minimum_healthy_apps))
            )
        except Exception as exc:
            raise SessionContractError(
                "V8 PAPER requires an approved ordered app1..app8 subset with "
                f"at least {int(minimum_healthy_apps)} healthy apps: {exc}"
            ) from exc
    names = _approved_app_names(
        [runtime.app_name for runtime in pool], minimum=int(minimum_healthy_apps)
    )
    if names is None:
        raise SessionContractError(
            "V8 PAPER requires an approved ordered app1..app8 subset with at "
            f"least {int(minimum_healthy_apps)} healthy apps; "
            f"observed={[str(runtime.app_name) for runtime in pool]}"
        )
    return pool


def _assert_exact_roster(runtimes: Sequence[market_data.AppRuntime]) -> None:
    # Kept under its historical name because activation/preflight callers use
    # it as the runtime availability gate.  Data completeness, not the identity
    # of a temporarily failed app, is the actual trading-integrity boundary.
    _validate_approved_runtime_pool(
        runtimes, minimum_healthy_apps=MIN_HEALTHY_KITE_APPS
    )


def _validate_minute_evidence_contract(
    frame: pd.DataFrame,
    marker: Mapping[str, Any],
    requests: Sequence[market_data.CandidateRequest],
    runtimes: Sequence[market_data.AppRuntime],
    expected_end: datetime,
) -> None:
    normalized = sorted(requests, key=lambda item: (item.symbol, item.instrument_token))
    expected_contract = common.canonical_json_sha256([asdict(item) for item in normalized])
    recorded_roster = marker.get("app_roster")
    recorded_names = (
        [str(item.get("app_name", "")) for item in recorded_roster]
        if isinstance(recorded_roster, list)
        and all(isinstance(item, Mapping) for item in recorded_roster)
        else []
    )
    approved_recorded_names = _approved_app_names(recorded_names, minimum=1)
    checks = (
        pd.Timestamp(marker.get("expected_end_ist")) == pd.Timestamp(expected_end),
        marker.get("candidate_contract_sha256") == expected_contract,
        int(marker.get("candidate_count", -1)) == len(normalized),
        approved_recorded_names is not None,
        all(bool(item.get("authenticated")) for item in (recorded_roster or [])),
        marker.get("app_roster_sha256")
        == common.canonical_json_sha256(recorded_roster),
        marker.get("policy_version") == market_data.MARKET_DATA_POLICY_VERSION,
    )
    if not all(checks):
        raise SessionContractError("completed-minute evidence contract mismatch")
    expected = {item.symbol: item.instrument_token for item in normalized}
    observed: dict[str, int] = {}
    for row in frame.to_dict("records"):
        symbol = str(row.get("symbol", "")).strip().upper()
        token = int(row.get("instrument_token", 0) or 0)
        if symbol not in expected or expected[symbol] != token or symbol in observed:
            raise SessionContractError("minute evidence contains an unexpected symbol/token")
        if pd.Timestamp(row.get("timestamp")) != pd.Timestamp(expected_end):
            raise SessionContractError("minute evidence contains a wrong candle end")
        if any(bool(row.get(flag, False)) for flag in (
            "gap_filled", "opening_snapshot", "provisional_stale"
        )):
            raise SessionContractError("minute evidence contains forbidden lineage")
        observed[symbol] = token
    if marker.get("complete") is True and set(observed) != set(expected):
        raise SessionContractError("complete minute evidence does not cover its contract")


def load_or_fetch_completed_minute(
    paths: SessionPaths,
    requests: Sequence[market_data.CandidateRequest],
    runtimes: Sequence[market_data.AppRuntime],
    expected_end: datetime,
    *,
    observed_now: datetime,
    minute_fetcher: Callable[..., tuple[pd.DataFrame, dict[str, Any]]],
    snapshot_root: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any], bool]:
    evidence_root = Path(snapshot_root) if snapshot_root is not None else paths.minute_root
    marker_path = evidence_root / f"minute_{expected_end.strftime('%H%M')}.json"
    reused = marker_path.is_file()
    if reused:
        frame, marker = market_data.load_validated_minute_snapshot(
            marker_path,
            strategy_fingerprint=config.strategy_fingerprint(),
        )
    else:
        frame, marker = minute_fetcher(
            requests,
            runtimes,
            expected_end,
            now=observed_now,
        )
        marker = market_data.publish_minute_snapshot_once(
            evidence_root,
            frame,
            marker,
            strategy_fingerprint=config.strategy_fingerprint(),
        )
    _validate_minute_evidence_contract(
        frame, marker, requests, runtimes, expected_end
    )
    return frame, dict(marker), reused


def run_preflight(
    session_date: date,
    *,
    require_activation: bool,
    authenticate_apps: bool,
    observed_now: datetime | None = None,
    control_paths: control.ControlPaths = control.DEFAULT_CONTROL_PATHS,
    bundle_provider: Callable[[], str] = control.runtime_bundle_sha256,
    authenticator: Callable[[], Sequence[market_data.AppRuntime]] = (
        market_data.authenticate_required_apps
    ),
    dashboard_identity_provider: Callable[..., Mapping[str, Any]] = (
        control.require_dashboard_runtime_identity
    ),
) -> tuple[int, dict[str, Any]]:
    """Read-only preflight; app profiles are touched only when explicitly asked."""

    _enforce_paper_only_environment()
    config.validate_configuration()
    observed = _normalize_now(observed_now)
    payload: dict[str, Any] = {
        "session": SESSION_ID,
        "session_date": session_date.isoformat(),
        "mode": config.MODE,
        "paper_only": True,
        "regular_nse_session": is_regular_nse_session(session_date),
        "setup_book_sha256": config.COMBINED_SETUP_BOOK_SHA256,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "activation_required": bool(require_activation or authenticate_apps),
        "apps_authenticated": False,
    }
    if observed.date() != session_date:
        payload.update(ok=False, reason="SESSION_DATE_IS_NOT_TODAY_IST")
        return 2, payload
    if not is_regular_nse_session(session_date):
        payload.update(ok=False, reason="NOT_A_FROZEN_REGULAR_NSE_SESSION")
        return 2, payload
    try:
        digest = bundle_provider()
        records = control.runtime_bundle_records()
        if config.canonical_json_sha256(records) != digest:
            raise SessionContractError("runtime bundle digest does not match source records")
        payload["runtime_bundle_sha256"] = digest
        payload["runtime_bundle_sources"] = list(records)
    except Exception as exc:
        payload.update(ok=False, reason=f"RUNTIME_BUNDLE_INVALID:{type(exc).__name__}")
        return 2, payload

    try:
        dashboard_identity = dict(dashboard_identity_provider(observed_now=observed))
    except Exception as exc:
        reason = getattr(exc, "reason", f"{type(exc).__name__}:{exc}")
        payload.update(ok=False, reason=str(reason))
        return 2, payload
    payload["dashboard_runtime_identity"] = dashboard_identity

    if require_activation or authenticate_apps:
        decision = control.evaluate_activation(
            session_date,
            now=observed,
            paths=control_paths,
            expected_runtime_bundle_sha256=str(digest),
        )
        payload.update(
            activation_allowed=decision.allowed,
            activation_reason=decision.reason,
            permit_id=decision.permit_id,
        )
        if not decision.allowed:
            payload.update(ok=False, reason=decision.reason)
            return 2, payload
        if authenticate_apps:
            # The valid activation decision above is intentionally before this
            # first call into credential discovery/profile authentication.
            try:
                runtimes = tuple(authenticator())
                _assert_exact_roster(runtimes)
            except Exception as exc:
                payload.update(ok=False, reason=f"APP_AUTH_FAILED:{type(exc).__name__}:{exc}")
                return 2, payload
            payload.update(
                apps_authenticated=True,
                app_roster=market_data.app_roster_payload(runtimes),
                app_roster_sha256=market_data.app_roster_sha256(runtimes),
            )
    payload.update(ok=True, reason="PREFLIGHT_OK")
    return 0, payload


def _try_ingest_slot(
    *,
    paths: SessionPaths,
    observed: datetime,
    process_started_at: datetime,
    engine: Any,
    module: Any,
    telemetry: SessionTelemetry,
    ingested_slots: set[str],
    symbol_tokens: dict[str, int],
    runtimes: Sequence[market_data.AppRuntime] = (),
    cash_constituent_fetcher: Callable[
        ..., tuple[dict[str, dict[str, Any]], dict[str, Any]]
    ] = (
        fetch_exact_cash_signal_constituents
    ),
    cash_feature_loader: Callable[..., Mapping[str, Any]] = (
        load_current_cash_signal_features
    ),
    futures_oi_loader: Callable[..., Mapping[str, Any]] = (
        load_exact_futures_oi_pair
    ),
    oi_superset_proof_loader: Callable[..., Mapping[str, Any]] = (
        prove_v6_oi_shift_is_exact_for_stock_universe
    ),
    strict_cash_source_loader: Callable[..., Mapping[str, Any]] = (
        precompute_strict_cash_universe_source
    ),
    strict_cash_sources: dict[str, dict[str, Any]] | None = None,
    oi_superset_proofs: dict[str, dict[str, Any]] | None = None,
    independent_source_loader: Callable[..., Mapping[str, Any]] = (
        precompute_independent_v8_candidate_source
    ),
    independent_candidate_sources: dict[str, dict[str, Any]] | None = None,
    minute_fetcher: Callable[..., tuple[pd.DataFrame, dict[str, Any]]] = (
        market_data.fetch_completed_minute
    ),
    activation_guard: Callable[[], Any] = lambda: None,
    clock: Callable[[], datetime],
) -> bool:
    """Register both sides of a slot before S+1 can become hindsight."""

    del minute_fetcher  # Candidate intake uses only the dedicated range audit.
    attempt_now = _normalize_now(clock())
    if attempt_now.date() != paths.session_date:
        raise SessionContractError("candidate intake clock crossed the session date")
    signal_end = next(
        (
            slot
            for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
            if slot not in ingested_slots
            and attempt_now >= _slot_datetime(paths.session_date, slot)
            + timedelta(seconds=BOUNDARY_BUFFER_SECONDS)
        ),
        None,
    )
    if signal_end is None:
        return False
    signal_at = _slot_datetime(paths.session_date, signal_end)
    confirmation_due = signal_at + timedelta(minutes=1, seconds=BOUNDARY_BUFFER_SECONDS)
    if process_started_at >= confirmation_due:
        telemetry.slots[signal_end] = {"state": "MISSED_AT_PROCESS_START"}
        raise SourceIncompleteError(
            f"no-retro-entry gate: process started after {signal_end} S+1 boundary"
        )
    if attempt_now >= confirmation_due:
        telemetry.slots[signal_end] = {"state": "MISSED_BEFORE_SOURCE_INTAKE"}
        raise SourceIncompleteError(
            f"no-retro-entry gate: {signal_end} intake began after its S+1 boundary"
        )

    # Cash finalizes around S+17, materially before the futures marker.  Prewarm
    # all strict causal features now, so the later S+46 OI proof is followed by
    # only an in-memory authority join rather than 208 serialized parquet reads.
    cash_cache = strict_cash_sources if strict_cash_sources is not None else {}
    strict_cash_entry = cash_cache.get(signal_end)
    if strict_cash_entry is None:
        loaded_cash = load_immutable_strict_cash_universe_source(paths, signal_end)
        if loaded_cash is not None:
            strict_cash_payload, strict_cash_artifact_sha = loaded_cash
        else:
            cash_started_at = _normalize_now(clock())
            try:
                strict_cash_payload = dict(
                    strict_cash_source_loader(
                        paths,
                        signal_end,
                        observed_at=cash_started_at,
                        cash_feature_loader=cash_feature_loader,
                    )
                )
            except SourceNotReadyError:
                if _normalize_now(clock()) >= confirmation_due:
                    raise SourceIncompleteError(
                        f"strict cash prewarm {signal_end} was not ready by S+1"
                    )
                return False
            cash_finished_at = _normalize_now(clock())
            activation_guard()
            if cash_finished_at >= confirmation_due:
                telemetry.slots[signal_end] = {
                    "state": "STRICT_CASH_PREWARM_CROSSED_S_PLUS_1"
                }
                raise SourceIncompleteError(
                    f"no-retro-entry gate: {signal_end} cash prewarm crossed S+1"
                )
            strict_cash_payload.update(
                {
                    "source_started_at_ist": cash_started_at.isoformat(),
                    "source_finished_at_ist": cash_finished_at.isoformat(),
                    "confirmation_due_ist": confirmation_due.isoformat(),
                    "source_completed_before_confirmation_due": True,
                }
            )
            strict_cash_payload.pop("strict_cash_source_sha256", None)
            strict_cash_payload["strict_cash_source_sha256"] = (
                common.canonical_json_sha256(strict_cash_payload)
            )
            _validate_strict_cash_universe_source(
                paths, signal_end, strict_cash_payload
            )
            strict_cash_path = (
                paths.strict_cash_source_root
                / f"slot_{signal_end.replace(':', '')}.json"
            )
            strict_cash_artifact_sha = _write_immutable_json(
                strict_cash_path, strict_cash_payload
            )
        strict_cash_entry = {
            "payload": strict_cash_payload,
            "path": str(
                paths.strict_cash_source_root
                / f"slot_{signal_end.replace(':', '')}.json"
            ),
            "artifact_sha256": strict_cash_artifact_sha,
        }
        cash_cache[signal_end] = strict_cash_entry
    else:
        strict_cash_payload = dict(strict_cash_entry.get("payload") or {})
        _validate_strict_cash_universe_source(paths, signal_end, strict_cash_payload)
        strict_cash_path = Path(str(strict_cash_entry.get("path", "")))
        if strict_cash_path != (
            paths.strict_cash_source_root
            / f"slot_{signal_end.replace(':', '')}.json"
        ) or _sha256_file(strict_cash_path) != strict_cash_entry.get(
            "artifact_sha256"
        ):
            raise SessionContractError("cached strict cash source artifact hash mismatch")

    # Prewarm the expensive all-universe predecessor proof as soon as the final
    # futures marker exists, normally well before the V6 scanner is published.
    # It is immutable and restart-reusable; the prospective registration path
    # subsequently verifies only its hashes and semantics.
    proof_cache = oi_superset_proofs if oi_superset_proofs is not None else {}
    proof_entry = proof_cache.get(signal_end)
    if proof_entry is None:
        loaded_proof = load_immutable_universe_oi_proof(paths, signal_end)
        if loaded_proof is not None:
            proof_payload, proof_artifact_sha = loaded_proof
        else:
            proof_started_at = _normalize_now(clock())
            try:
                proof_payload = dict(
                    oi_superset_proof_loader(
                        paths,
                        signal_end,
                        observed_at=proof_started_at,
                    )
                )
            except SourceNotReadyError:
                if _normalize_now(clock()) >= confirmation_due:
                    telemetry.slots[signal_end] = {
                        "state": "FUTURES_PROOF_NOT_READY_BY_S_PLUS_1"
                    }
                    raise SourceIncompleteError(
                        f"futures universe proof {signal_end} was not ready by S+1"
                    )
                return False
            proof_finished_at = _normalize_now(clock())
            activation_guard()
            if proof_finished_at >= confirmation_due:
                telemetry.slots[signal_end] = {
                    "state": "FUTURES_PROOF_CROSSED_S_PLUS_1"
                }
                raise SourceIncompleteError(
                    f"no-retro-entry gate: {signal_end} futures proof crossed S+1"
                )
            proof_payload.update(
                {
                    "proof_started_at_ist": proof_started_at.isoformat(),
                    "proof_finished_at_ist": proof_finished_at.isoformat(),
                    "confirmation_due_ist": confirmation_due.isoformat(),
                    "proof_completed_before_confirmation_due": True,
                }
            )
            proof_payload.pop("proof_sha256", None)
            proof_payload["proof_sha256"] = common.canonical_json_sha256(
                proof_payload
            )
            _validate_universe_oi_proof_payload(paths, signal_end, proof_payload)
            proof_path = (
                paths.oi_superset_audit_root
                / f"slot_{signal_end.replace(':', '')}.json"
            )
            proof_artifact_sha = _write_immutable_json(proof_path, proof_payload)
        proof_entry = {
            "payload": proof_payload,
            "path": str(
                paths.oi_superset_audit_root
                / f"slot_{signal_end.replace(':', '')}.json"
            ),
            "artifact_sha256": proof_artifact_sha,
        }
        proof_cache[signal_end] = proof_entry
    else:
        proof_payload = dict(proof_entry.get("payload") or {})
        _validate_universe_oi_proof_payload(paths, signal_end, proof_payload)
        proof_path = Path(str(proof_entry.get("path", "")))
        if proof_path != (
            paths.oi_superset_audit_root
            / f"slot_{signal_end.replace(':', '')}.json"
        ) or _sha256_file(proof_path) != proof_entry.get("artifact_sha256"):
            raise SessionContractError("cached universe OI proof artifact hash mismatch")

    authority_cache = (
        independent_candidate_sources
        if independent_candidate_sources is not None
        else {}
    )
    authority_entry = authority_cache.get(signal_end)
    if authority_entry is None:
        loaded_authority = load_immutable_independent_candidate_source(
            paths,
            signal_end,
            universe_proof_sha256=str(proof_payload["proof_sha256"]),
            strict_cash_source_sha256=str(
                strict_cash_payload["strict_cash_source_sha256"]
            ),
        )
        if loaded_authority is not None:
            authority_payload, authority_artifact_sha = loaded_authority
        else:
            authority_started_at = _normalize_now(clock())
            try:
                authority_payload = dict(
                    independent_source_loader(
                        paths,
                        signal_end,
                        proof_payload,
                        observed_at=authority_started_at,
                        cash_feature_loader=cash_feature_loader,
                        strict_cash_source=strict_cash_payload,
                    )
                )
            except SourceNotReadyError:
                if _normalize_now(clock()) >= confirmation_due:
                    raise SourceIncompleteError(
                        f"independent V8 source {signal_end} was not ready by S+1"
                    )
                return False
            authority_finished_at = _normalize_now(clock())
            activation_guard()
            if authority_finished_at >= confirmation_due:
                telemetry.slots[signal_end] = {
                    "state": "INDEPENDENT_SOURCE_CROSSED_S_PLUS_1"
                }
                raise SourceIncompleteError(
                    f"no-retro-entry gate: {signal_end} independent source crossed S+1"
                )
            authority_payload.update(
                {
                    "source_started_at_ist": authority_started_at.isoformat(),
                    "source_finished_at_ist": authority_finished_at.isoformat(),
                    "confirmation_due_ist": confirmation_due.isoformat(),
                    "source_completed_before_confirmation_due": True,
                }
            )
            authority_payload.pop("candidate_source_sha256", None)
            authority_payload["candidate_source_sha256"] = (
                common.canonical_json_sha256(authority_payload)
            )
            _validate_independent_candidate_source(
                paths,
                signal_end,
                authority_payload,
                universe_proof_sha256=str(proof_payload["proof_sha256"]),
                strict_cash_source_sha256=str(
                    strict_cash_payload["strict_cash_source_sha256"]
                ),
            )
            authority_path = (
                paths.independent_candidate_source_root
                / f"slot_{signal_end.replace(':', '')}.json"
            )
            authority_artifact_sha = _write_immutable_json(
                authority_path, authority_payload
            )
        authority_entry = {
            "payload": authority_payload,
            "path": str(
                paths.independent_candidate_source_root
                / f"slot_{signal_end.replace(':', '')}.json"
            ),
            "artifact_sha256": authority_artifact_sha,
        }
        authority_cache[signal_end] = authority_entry
    else:
        authority_payload = dict(authority_entry.get("payload") or {})
        _validate_independent_candidate_source(
            paths,
            signal_end,
            authority_payload,
            universe_proof_sha256=str(proof_payload["proof_sha256"]),
            strict_cash_source_sha256=str(
                strict_cash_payload["strict_cash_source_sha256"]
            ),
        )
        authority_path = Path(str(authority_entry.get("path", "")))
        if authority_path != (
            paths.independent_candidate_source_root
            / f"slot_{signal_end.replace(':', '')}.json"
        ) or _sha256_file(authority_path) != authority_entry.get("artifact_sha256"):
            raise SessionContractError(
                "cached independent candidate source artifact hash mismatch"
            )

    # The independent all-mapped-stock authority above is complete.  The V6
    # scanner is deliberately not read here: its observed publication at
    # S+57..59 would consume nearly all of the prospective S+1+3 budget even
    # though it cannot add/remove a V8 candidate.  It is validated, archived,
    # and reconciled by ``archive_pending_v6_scanner_diagnostics`` only after
    # reducer registration.  Candidate inputs bind the immutable independent
    # authority bytes instead of mislabelling V6 as their source.
    snapshot: dict[str, Any] = {}
    raw_source = _json_bytes(authority_payload)
    source_sha = _sha256_bytes(raw_source)
    if source_sha != str(authority_entry["artifact_sha256"]):
        raise SessionContractError(
            "independent candidate authority bytes/artifact hash mismatch"
        )
    independent_identities = {
        (
            str(row.get("tradingsymbol", "")).strip().upper(),
            str(row.get("side", "")).strip().upper(),
        )
        for row in authority_payload.get("eligible_rows") or []
        if isinstance(row, Mapping)
    }
    audit_symbols: dict[str, int] = {}
    for row in authority_payload.get("eligible_rows") or []:
        symbol = str(row.get("tradingsymbol", "")).strip().upper()
        token = int(row.get("instrument_token", 0) or 0)
        prior = audit_symbols.setdefault(symbol, token)
        if not symbol or token <= 0 or prior != token:
            raise SourceIncompleteError("independent direct-audit identity is invalid")
    direct_request_snapshot = {
        "candidates": [
            {"tradingsymbol": symbol, "instrument_token": token}
            for symbol, token in sorted(audit_symbols.items())
        ]
    }

    audit_started_at = _normalize_now(clock())
    if audit_started_at >= confirmation_due:
        telemetry.slots[signal_end] = {"state": "RANGE_AUDIT_STARTED_TOO_LATE"}
        raise SourceIncompleteError(
            f"no-retro-entry gate: {signal_end} range audit started after S+1"
        )
    cash_constituent_audit, direct_audit_payload = cash_constituent_fetcher(
        direct_request_snapshot,
        paths,
        signal_end,
        runtimes,
        observed_at=audit_started_at,
    )
    audit_finished_at = _normalize_now(clock())
    # Network audit completed; a revoke/kill must stop before engine mutation.
    activation_guard()
    if audit_finished_at >= confirmation_due:
        telemetry.slots[signal_end] = {"state": "RANGE_AUDIT_CROSSED_S_PLUS_1"}
        raise SourceIncompleteError(
            f"no-retro-entry gate: {signal_end} range audit crossed S+1"
        )
    candidate_books: dict[str, list[dict[str, Any]]] = {}
    setup_by_side: dict[str, config.PaperSetup] = {}
    for side in ("LONG", "SHORT"):
        setup = config.setup_for(signal_end, side)
        if setup is None:
            raise SessionContractError(f"missing frozen setup: {signal_end}_{side}")
        setup_by_side[side] = setup
        candidate_books[side] = build_v8_candidate_book(
            snapshot,
            setup,
            paths,
            source_sha256=source_sha,
            observed_at=audit_finished_at,
            cash_constituent_audit=cash_constituent_audit,
            universe_oi_proof=proof_payload,
            independent_candidate_source=authority_payload,
            cash_feature_loader=cash_feature_loader,
            futures_oi_loader=futures_oi_loader,
        )

    # This is the last operation before any audit/candidate artifact or engine
    # mutation.  If data work consumed the S+1 boundary, fail without turning
    # completed hindsight into a prospective registration.
    activation_guard()
    decision_now = _normalize_now(clock())
    if decision_now >= confirmation_due:
        telemetry.slots[signal_end] = {"state": "SOURCE_AUDIT_CROSSED_S_PLUS_1"}
        raise SourceIncompleteError(
            f"no-retro-entry gate: {signal_end} audit crossed its S+1 boundary"
        )
    direct_audit_payload = dict(direct_audit_payload)
    direct_audit_payload.update(
        {
            "source_policy_version": SOURCE_POLICY_VERSION,
            "audit_started_at_ist": audit_started_at.isoformat(),
            "observed_at_ist": audit_started_at.isoformat(),
            "audit_finished_at_ist": audit_finished_at.isoformat(),
            "decision_at_ist": decision_now.isoformat(),
            "confirmation_due_ist": confirmation_due.isoformat(),
            "decision_before_confirmation_due": True,
            "oi_universe_proof_path": proof_entry["path"],
            "oi_universe_proof_artifact_sha256": proof_entry["artifact_sha256"],
            "oi_universe_proof_sha256": proof_payload["proof_sha256"],
            "independent_candidate_source_path": authority_entry["path"],
            "independent_candidate_source_artifact_sha256": authority_entry[
                "artifact_sha256"
            ],
            "independent_candidate_source_sha256": authority_payload[
                "candidate_source_sha256"
            ],
            "strict_cash_source_path": strict_cash_entry["path"],
            "strict_cash_source_artifact_sha256": strict_cash_entry[
                "artifact_sha256"
            ],
            "strict_cash_source_sha256": strict_cash_payload[
                "strict_cash_source_sha256"
            ],
            "v6_scanner_role": "POST_REGISTRATION_DIAGNOSTIC_ONLY",
            "v6_scanner_diagnostic_state": "PENDING",
            "independent_eligible_identities": [
                {"tradingsymbol": symbol, "side": side}
                for symbol, side in sorted(independent_identities)
            ],
        }
    )
    direct_audit_payload["audit_payload_sha256"] = common.canonical_json_sha256(
        direct_audit_payload
    )
    audit_path = (
        paths.cash_signal_audit_root
        / f"slot_{signal_end.replace(':', '')}"
        / "direct_range_audit.json"
    )
    audit_sha = _write_immutable_json(audit_path, direct_audit_payload)
    for rows in candidate_books.values():
        for candidate in rows:
            candidate["cash_direct_audit_path"] = str(audit_path)
            candidate["cash_direct_audit_sha256"] = audit_sha
            candidate["cash_audit_started_at_ist"] = audit_started_at.isoformat()
            candidate["cash_audit_finished_at_ist"] = audit_finished_at.isoformat()
            candidate["decision_at_ist"] = decision_now.isoformat()
            candidate["confirmation_due_ist"] = confirmation_due.isoformat()
            candidate["decision_before_confirmation_due"] = True
            candidate["oi_universe_proof_path"] = proof_entry["path"]
            candidate["oi_universe_proof_artifact_sha256"] = proof_entry[
                "artifact_sha256"
            ]
            candidate["oi_universe_proof_sha256"] = proof_payload["proof_sha256"]
            candidate["independent_candidate_source_path"] = authority_entry["path"]
            candidate["independent_candidate_source_artifact_sha256"] = (
                authority_entry["artifact_sha256"]
            )
            candidate["independent_candidate_source_sha256"] = authority_payload[
                "candidate_source_sha256"
            ]
            candidate["strict_cash_source_path"] = strict_cash_entry["path"]
            candidate["strict_cash_source_artifact_sha256"] = strict_cash_entry[
                "artifact_sha256"
            ]
            candidate["strict_cash_source_sha256"] = strict_cash_payload[
                "strict_cash_source_sha256"
            ]

    # Persist both complete books before either can mutate the reducer.
    archive_details: dict[str, Any] = {}
    for side in ("LONG", "SHORT"):
        archive_details[side] = archive_slot_inputs(
            paths,
            setup_by_side[side],
            raw_source,
            source_sha,
            candidate_books[side],
        )

    # Archival fsyncs are outside our control.  Re-read both control and clock
    # after every immutable input is durable, immediately before the first
    # reducer mutation.  If this crosses S+1, evidence remains for diagnosis
    # but the engine, token map, and ingested-slot set stay untouched.
    activation_guard()
    registration_decision_at = _normalize_now(clock())
    if registration_decision_at >= confirmation_due:
        telemetry.slots[signal_end] = {
            "state": "ARCHIVES_SEALED_REGISTRATION_DEADLINE_CROSSED",
            "decision_at_ist": decision_now.isoformat(),
            "registration_recheck_at_ist": registration_decision_at.isoformat(),
            "confirmation_due_ist": confirmation_due.isoformat(),
            "cash_direct_audit_path": str(audit_path),
            "cash_direct_audit_sha256": audit_sha,
            "oi_universe_proof_path": proof_entry["path"],
            "oi_universe_proof_artifact_sha256": proof_entry["artifact_sha256"],
            "archives": archive_details,
        }
        raise SourceIncompleteError(
            f"no-retro-entry gate: {signal_end} archives crossed its S+1 boundary"
        )
    for side in ("LONG", "SHORT"):
        register_candidate_book(
            engine, module, setup_by_side[side], candidate_books[side], paths
        )
        for candidate in candidate_books[side]:
            symbol = str(candidate["symbol"])
            token = int(candidate["equity_instrument_token"])
            prior = symbol_tokens.setdefault(symbol, token)
            if prior != token:
                raise SessionContractError(f"cash token changed for {symbol}")
    ingested_slots.add(signal_end)
    telemetry.slots[signal_end] = {
        "state": "REGISTERED",
        "source_sha256": source_sha,
        "long_candidates": len(candidate_books["LONG"]),
        "short_candidates": len(candidate_books["SHORT"]),
        "decision_at_ist": decision_now.isoformat(),
        "registration_recheck_at_ist": registration_decision_at.isoformat(),
        "confirmation_due_ist": confirmation_due.isoformat(),
        "decision_before_confirmation_due": True,
        "oi_universe_proof_path": proof_entry["path"],
        "oi_universe_proof_artifact_sha256": proof_entry["artifact_sha256"],
        "independent_candidate_source_path": authority_entry["path"],
        "independent_candidate_source_artifact_sha256": authority_entry[
            "artifact_sha256"
        ],
        "strict_cash_source_path": strict_cash_entry["path"],
        "strict_cash_source_artifact_sha256": strict_cash_entry[
            "artifact_sha256"
        ],
        "v6_scanner_diagnostic_state": "PENDING",
        "archives": archive_details,
    }
    return True


def _finalize_session(
    paths: SessionPaths,
    telemetry: SessionTelemetry,
    engine: Any,
) -> None:
    records = list(engine.records())
    publish_final_trades(paths, records)
    publish_report(paths, telemetry, records, final=True)
    write_completion_artifacts(paths, telemetry, records=records)
    publish_runtime_state(paths, telemetry)


def _intervention_open_symbols(engine: Any) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                str(record.get("symbol", "")).strip().upper()
                for record in engine.records()
                if str(record.get("portfolio_decision", "")).upper() == "ACCEPTED"
                and str(
                    record.get("unconstrained_status", record.get("status", ""))
                ).upper()
                == "FILLED_OPEN"
                and str(record.get("symbol", "")).strip()
            }
        )
    )


def _resolve_control_intervention(
    *,
    reason: str,
    paths: SessionPaths,
    engine: Any,
    module: Any,
    telemetry: SessionTelemetry,
    runtimes: Sequence[market_data.AppRuntime],
    symbol_tokens: Mapping[str, int],
    ingested_slots: set[str],
    processed_clock_end: datetime,
    now_provider: Callable[[], datetime],
    sleep_fn: Callable[[float], None],
    minute_fetcher: Callable[..., tuple[pd.DataFrame, dict[str, Any]]],
    poll_seconds: float,
) -> tuple[bool, datetime]:
    """Cancel shadows and close modeled opens at one exact completed close."""

    open_symbols = _intervention_open_symbols(engine)
    event_end = processed_clock_end
    bars: dict[str, Any] = {}
    if open_symbols:
        while True:
            observed = _normalize_now(now_provider())
            if observed.date() != paths.session_date:
                raise SessionContractError("intervention crossed the authorized date")
            due = _floor_due_minute(observed)
            if due > processed_clock_end:
                event_end = min(due, _slot_datetime(paths.session_date, config.SQUARE_OFF))
                break
            telemetry.phase = "WAITING_EXACT_INTERVENTION_BAR"
            publish_runtime_state(paths, telemetry, heartbeat_only=True)
            sleep_fn(max(0.05, min(float(poll_seconds), 1.0)))

        requests = [
            market_data.CandidateRequest(symbol, int(symbol_tokens.get(symbol, 0)))
            for symbol in open_symbols
        ]
        if any(item.instrument_token <= 0 for item in requests):
            raise SessionContractError("intervention open symbol lacks a frozen NSE token")
        marker_path = paths.minute_root / f"minute_{event_end.strftime('%H%M')}.json"
        if marker_path.is_file():
            frame, marker = market_data.load_validated_minute_snapshot(
                marker_path,
                strategy_fingerprint=config.strategy_fingerprint(),
            )
            recorded_roster = marker.get("app_roster")
            recorded_names = (
                [str(item.get("app_name", "")) for item in recorded_roster]
                if isinstance(recorded_roster, list)
                and all(isinstance(item, Mapping) for item in recorded_roster)
                else []
            )
            if (
                pd.Timestamp(marker.get("expected_end_ist")) != pd.Timestamp(event_end)
                or _approved_app_names(
                    recorded_names, minimum=MIN_HEALTHY_KITE_APPS
                )
                is None
                or marker.get("app_roster_sha256")
                != common.canonical_json_sha256(recorded_roster)
            ):
                raise SessionContractError("intervention snapshot binding mismatch")
        else:
            frame, marker = minute_fetcher(
                requests,
                runtimes,
                event_end,
                now=_normalize_now(now_provider()),
            )
            marker = market_data.publish_minute_snapshot_once(
                paths.minute_root,
                frame,
                marker,
                strategy_fingerprint=config.strategy_fingerprint(),
            )
        rows = {
            str(row["symbol"]).strip().upper(): row
            for row in frame.to_dict("records")
            if str(row.get("symbol", "")).strip().upper() in set(open_symbols)
        }
        missing = sorted(set(open_symbols) - set(rows))
        if missing:
            raise SourceIncompleteError(
                f"exact intervention close missing for modeled opens: {missing}"
            )
        bars = {
            symbol: _completed_bar(module, rows[symbol], pd.Timestamp(event_end))
            for symbol in open_symbols
        }

    events = engine.terminate_for_intervention(event_end, bars, reason)
    archive_engine_events(paths, events)
    processed_clock_end = max(processed_clock_end, event_end)
    telemetry.last_completed_minute = processed_clock_end.isoformat()
    telemetry.state = "STOPPED_CONTROL_INTERVENTION"
    telemetry.phase = "INTERVENTION_RECONCILED"
    telemetry.messages.append(
        f"Control gate stopped the session; {len(open_symbols)} modeled open(s) "
        "were closed at an exact completed candle close."
    )
    persist_checkpoint(
        paths,
        engine,
        telemetry,
        processed_clock_end=processed_clock_end,
        ingested_slots=ingested_slots,
        symbol_tokens=symbol_tokens,
    )
    return True, processed_clock_end


def run_paper_session(
    session_date: date,
    *,
    paths: SessionPaths | None = None,
    control_paths: control.ControlPaths = control.DEFAULT_CONTROL_PATHS,
    now_provider: Callable[[], datetime] = now_ist,
    sleep_fn: Callable[[float], None] = time.sleep,
    bundle_provider: Callable[[], str] = control.runtime_bundle_sha256,
    authenticator: Callable[[], Sequence[market_data.AppRuntime]] = (
        market_data.authenticate_required_apps
    ),
    minute_fetcher: Callable[..., tuple[pd.DataFrame, dict[str, Any]]] = (
        market_data.fetch_completed_minute
    ),
    cash_constituent_fetcher: Callable[
        ..., tuple[dict[str, dict[str, Any]], dict[str, Any]]
    ] = (
        fetch_exact_cash_signal_constituents
    ),
    cash_feature_loader: Callable[..., Mapping[str, Any]] = (
        load_current_cash_signal_features
    ),
    futures_oi_loader: Callable[..., Mapping[str, Any]] = (
        load_exact_futures_oi_pair
    ),
    oi_superset_proof_loader: Callable[..., Mapping[str, Any]] = (
        prove_v6_oi_shift_is_exact_for_stock_universe
    ),
    strict_cash_source_loader: Callable[..., Mapping[str, Any]] = (
        precompute_strict_cash_universe_source
    ),
    independent_source_loader: Callable[..., Mapping[str, Any]] = (
        precompute_independent_v8_candidate_source
    ),
    scanner_diagnostic_archiver: Callable[..., bool] = (
        archive_pending_v6_scanner_diagnostics
    ),
    dashboard_identity_provider: Callable[..., Mapping[str, Any]] = (
        control.require_dashboard_runtime_identity
    ),
    engine_module: Any | None = None,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    max_iterations: int | None = None,
) -> int:
    """Run one exact-date prospective V8-Combined PAPER session."""

    _enforce_paper_only_environment()
    config.validate_configuration()
    runtime_paths = paths or SessionPaths(session_date=session_date)
    telemetry = SessionTelemetry()
    observed = _normalize_now(now_provider())
    if observed.date() != session_date:
        telemetry.state = "DISABLED_WRONG_SESSION_DATE"
        telemetry.phase = "PRE_GATE"
        telemetry.activation_reason = "SESSION_DATE_IS_NOT_TODAY_IST"
        publish_report(runtime_paths, telemetry, [], final=False)
        publish_runtime_state(runtime_paths, telemetry)
        return 0
    if not is_regular_nse_session(session_date):
        telemetry.state = "DISABLED_NON_TRADING_DAY"
        telemetry.phase = "PRE_GATE"
        telemetry.activation_reason = "NOT_A_FROZEN_REGULAR_NSE_SESSION"
        publish_report(runtime_paths, telemetry, [], final=False)
        publish_runtime_state(runtime_paths, telemetry)
        return 0

    decision, bundle_digest = _activation_for_runtime(
        session_date,
        observed,
        control_paths=control_paths,
        bundle_provider=bundle_provider,
    )
    telemetry.runtime_bundle_sha256 = bundle_digest
    telemetry.activation_reason = decision.reason

    completion_path = runtime_paths.day_session_root / "completion.json"
    if completion_path.is_file():
        telemetry.state = "ALREADY_COMPLETED"
        telemetry.phase = "DUPLICATE_START_BLOCKED"
        publish_runtime_state(runtime_paths, telemetry)
        return 0

    module = engine_module or _engine_module()
    # A terminal checkpoint may have been durably written immediately before
    # power loss.  Recover it under the independent writer lock *before* the
    # activation rejection path and without authenticating any app.
    if runtime_paths.latest_checkpoint_path.is_file():
        with ProcessLock(runtime_paths.lock_path):
            recovered_code = recover_terminal_checkpoint_before_credentials(
                runtime_paths,
                module,
                expected_runtime_bundle_sha256=bundle_digest,
            )
        if recovered_code is not None:
            return recovered_code

    if not decision.allowed:
        # Staged/disabled is not a scheduler failure and must not discover apps.
        telemetry.state = "DISABLED_APPROVAL_REQUIRED"
        telemetry.phase = "ACTIVATION_GATE"
        publish_report(runtime_paths, telemetry, [], final=False)
        publish_runtime_state(runtime_paths, telemetry)
        return 0

    with ProcessLock(runtime_paths.lock_path):
        # Recheck after gaining the writer lock and before the first credential
        # discovery.  The authenticator itself is the first credential seam.
        decision = _require_runtime_activation(
            session_date,
            _normalize_now(now_provider()),
            control_paths=control_paths,
            bundle_provider=bundle_provider,
            expected_bundle_sha256=bundle_digest,
        )
        try:
            dashboard_runtime_identity = dict(
                dashboard_identity_provider(observed_now=_normalize_now(now_provider()))
            )
        except Exception as exc:
            telemetry.state = "BLOCKED_DASHBOARD_RUNTIME"
            telemetry.phase = "DASHBOARD_LOADED_SOURCE_IDENTITY"
            telemetry.messages.append(f"{type(exc).__name__}: {exc}")
            publish_report(runtime_paths, telemetry, [], final=False)
            publish_runtime_state(runtime_paths, telemetry)
            return 2
        try:
            runtimes = tuple(authenticator())
            _assert_exact_roster(runtimes)
        except Exception as exc:
            telemetry.state = "BLOCKED_APP_AUTH"
            telemetry.phase = "EIGHT_APP_AUTHENTICATION"
            telemetry.messages.append(f"{type(exc).__name__}: {exc}")
            publish_report(runtime_paths, telemetry, [], final=False)
            publish_runtime_state(runtime_paths, telemetry)
            return 2
        telemetry.permit_id = decision.permit_id
        telemetry.app_roster_sha256 = market_data.app_roster_sha256(runtimes)
        telemetry.state = "RUNNING"
        telemetry.phase = "INITIALIZING_ENGINE"
        write_strategy_manifest(
            runtime_paths,
            decision,
            runtimes,
            dashboard_runtime_identity=dashboard_runtime_identity,
        )
        strict_cash_sources: dict[str, dict[str, Any]] = {}
        oi_superset_proofs: dict[str, dict[str, Any]] = {}
        independent_candidate_sources: dict[str, dict[str, Any]] = {}

        restored = load_checkpoint(
            runtime_paths,
            module,
            expected_runtime_bundle_sha256=bundle_digest,
        )
        process_started_at = _normalize_now(now_provider())
        if restored is None:
            engine = module.V8CombinedPaperEngine()
            processed_clock_end = _floor_due_minute(process_started_at)
            ingested_slots: set[str] = set()
            symbol_tokens: dict[str, int] = {}
            first_confirmation_boundary = _slot_datetime(
                session_date, "09:26"
            ) + timedelta(seconds=BOUNDARY_BUFFER_SECONDS)
            if process_started_at >= first_confirmation_boundary:
                telemetry.state = "DATA_INCOMPLETE"
                telemetry.phase = "MISSED_PROSPECTIVE_START"
                telemetry.data_incomplete = True
                telemetry.messages.append(
                    "Process began after the first S+1 boundary; retroactive entries are forbidden."
                )
                _finalize_session(runtime_paths, telemetry, engine)
                return 2
            # Establish a chronological, empty baseline at process start.  It
            # has no candidate economics and prevents a later checkpoint from
            # claiming that pre-start minutes were replayed.
            process_engine_minute(
                engine,
                module,
                processed_clock_end,
                pd.DataFrame(),
                runtime_paths,
            )
        else:
            engine, processed_clock_end, ingested_slots, symbol_tokens, restored_telemetry = restored
            restored_telemetry.runtime_bundle_sha256 = bundle_digest
            restored_telemetry.activation_reason = decision.reason
            restored_telemetry.permit_id = decision.permit_id
            restored_telemetry.app_roster_sha256 = telemetry.app_roster_sha256
            restored_telemetry.state = "RUNNING"
            restored_telemetry.phase = "RESTORED_CHECKPOINT"
            telemetry = restored_telemetry
            if engine.last_processed_minute != processed_clock_end:
                raise SessionContractError("engine/checkpoint processed minute mismatch")
            due_at_restore = _floor_due_minute(process_started_at)
            if due_at_restore > processed_clock_end:
                replay_end = min(
                    due_at_restore,
                    _slot_datetime(session_date, config.SQUARE_OFF),
                )
                cursor = processed_clock_end + timedelta(minutes=1)
                while cursor <= replay_end:
                    required = engine_required_symbols(engine)
                    frame = pd.DataFrame()
                    if required:
                        marker_path = (
                            runtime_paths.minute_root
                            / f"minute_{cursor.strftime('%H%M')}.json"
                        )
                        if not marker_path.is_file():
                            telemetry.state = "DATA_INCOMPLETE"
                            telemetry.phase = "RESTART_GAP_WITH_ACTIVE_STATE"
                            telemetry.data_incomplete = True
                            telemetry.messages.append(
                                "No immutable crash-recovery snapshot exists for active "
                                f"minute {cursor.isoformat()}; the API was not re-queried."
                            )
                            _finalize_session(runtime_paths, telemetry, engine)
                            return 2
                        requests = [
                            market_data.CandidateRequest(
                                symbol, int(symbol_tokens.get(symbol, 0))
                            )
                            for symbol in required
                        ]
                        if any(item.instrument_token <= 0 for item in requests):
                            raise SessionContractError(
                                "checkpoint active symbol lacks a frozen NSE token"
                            )

                        def forbid_refetch(*args: Any, **kwargs: Any) -> Any:
                            raise SessionContractError(
                                "crash recovery must not re-fetch a published minute"
                            )

                        frame, marker, reused = load_or_fetch_completed_minute(
                            runtime_paths,
                            requests,
                            runtimes,
                            cursor,
                            observed_now=process_started_at,
                            minute_fetcher=forbid_refetch,
                        )
                        if not reused:
                            raise SessionContractError(
                                "crash recovery unexpectedly created minute evidence"
                            )
                        process_engine_minute(
                            engine, module, cursor, frame, runtime_paths
                        )
                        if marker.get("complete") is not True or marker.get("state") != "SUCCESS":
                            telemetry.incomplete_minutes += 1
                            telemetry.data_incomplete = True
                            telemetry.state = "DATA_INCOMPLETE"
                            telemetry.phase = "CRASH_SNAPSHOT_DATA_INCOMPLETE"
                    else:
                        process_engine_minute(
                            engine, module, cursor, frame, runtime_paths
                        )
                    processed_clock_end = cursor
                    telemetry.completed_minutes += 1
                    telemetry.last_completed_minute = cursor.isoformat()
                    cursor += timedelta(minutes=1)
                persist_checkpoint(
                    runtime_paths,
                    engine,
                    telemetry,
                    processed_clock_end=processed_clock_end,
                    ingested_slots=ingested_slots,
                    symbol_tokens=symbol_tokens,
                )
                if telemetry.data_incomplete:
                    _finalize_session(runtime_paths, telemetry, engine)
                    return 2
            if processed_clock_end == _slot_datetime(session_date, config.SQUARE_OFF):
                unresolved_at_restore = _unresolved_filled_records(engine.records())
                if (
                    engine_required_symbols(engine)
                    or telemetry.data_incomplete
                    or unresolved_at_restore
                ):
                    telemetry.state = "DATA_INCOMPLETE"
                    telemetry.phase = "RESTORED_1530_INVALID_OR_UNRESOLVED"
                    telemetry.data_incomplete = True
                    telemetry.messages.append(
                        "The exact-15:30 checkpoint retained active/unresolved state "
                        "or prior data-incomplete evidence."
                    )
                    _finalize_session(runtime_paths, telemetry, engine)
                    return 2
                telemetry.state = "COMPLETED"
                telemetry.phase = "EXACT_1530_COMPLETE"
                _finalize_session(runtime_paths, telemetry, engine)
                return 0

        publish_runtime_state(runtime_paths, telemetry)
        iterations = 0
        try:
            while True:
                iterations += 1
                if max_iterations is not None and iterations > max_iterations:
                    telemetry.state = "STOPPED_TEST_LIMIT"
                    telemetry.phase = "TEST_LIMIT"
                    publish_report(runtime_paths, telemetry, engine.records(), final=False)
                    publish_runtime_state(runtime_paths, telemetry)
                    return 0
                observed = _normalize_now(now_provider())
                if observed.date() != session_date:
                    raise SessionContractError("session crossed its authorized date")
                _require_runtime_activation(
                    session_date,
                    observed,
                    control_paths=control_paths,
                    bundle_provider=bundle_provider,
                    expected_bundle_sha256=bundle_digest,
                )
                telemetry.phase = "FIVE_MINUTE_INTAKE"
                # Multiple zero-latency slots cannot occur normally; the loop
                # makes synthetic clocks and a no-candidate catch-up explicit.
                while _try_ingest_slot(
                    paths=runtime_paths,
                    observed=observed,
                    process_started_at=process_started_at,
                    engine=engine,
                    module=module,
                    telemetry=telemetry,
                    ingested_slots=ingested_slots,
                    symbol_tokens=symbol_tokens,
                    runtimes=runtimes,
                    cash_constituent_fetcher=cash_constituent_fetcher,
                    cash_feature_loader=cash_feature_loader,
                    futures_oi_loader=futures_oi_loader,
                    strict_cash_source_loader=strict_cash_source_loader,
                    strict_cash_sources=strict_cash_sources,
                    oi_superset_proof_loader=oi_superset_proof_loader,
                    oi_superset_proofs=oi_superset_proofs,
                    independent_source_loader=independent_source_loader,
                    independent_candidate_sources=independent_candidate_sources,
                    minute_fetcher=minute_fetcher,
                    activation_guard=lambda: _require_runtime_activation(
                        session_date,
                        _normalize_now(now_provider()),
                        control_paths=control_paths,
                        bundle_provider=bundle_provider,
                        expected_bundle_sha256=bundle_digest,
                    ),
                    clock=now_provider,
                ):
                    persist_checkpoint(
                        runtime_paths,
                        engine,
                        telemetry,
                        processed_clock_end=processed_clock_end,
                        ingested_slots=ingested_slots,
                        symbol_tokens=symbol_tokens,
                    )

                # Registration is checkpointed above before the historically
                # late V6 diagnostic is touched.  A crash in this diagnostic
                # seam therefore restores the exact prospective book and
                # reuses any already-sealed reconciliation idempotently.
                if scanner_diagnostic_archiver(
                    runtime_paths,
                    telemetry,
                    ingested_slots,
                    observed_at=_normalize_now(now_provider()),
                ):
                    persist_checkpoint(
                        runtime_paths,
                        engine,
                        telemetry,
                        processed_clock_end=processed_clock_end,
                        ingested_slots=ingested_slots,
                        symbol_tokens=symbol_tokens,
                    )

                due_end = min(
                    _floor_due_minute(observed),
                    _slot_datetime(session_date, config.SQUARE_OFF),
                )
                next_end = processed_clock_end + timedelta(minutes=1)
                while next_end <= due_end:
                    # A delayed loop may safely catch up empty clock minutes,
                    # but never fetch/replay an already-missed active minute.
                    if next_end < due_end and engine_required_symbols(engine):
                        raise SourceIncompleteError(
                            f"active completed minute was missed: {next_end.isoformat()}"
                        )
                    decision = _require_runtime_activation(
                        session_date,
                        _normalize_now(now_provider()),
                        control_paths=control_paths,
                        bundle_provider=bundle_provider,
                        expected_bundle_sha256=bundle_digest,
                    )
                    required = engine_required_symbols(engine)
                    frame = pd.DataFrame()
                    if required:
                        requests: list[market_data.CandidateRequest] = []
                        for symbol in required:
                            token = int(symbol_tokens.get(symbol, 0))
                            if token <= 0:
                                raise SessionContractError(
                                    f"active symbol has no frozen NSE token: {symbol}"
                                )
                            requests.append(
                                market_data.CandidateRequest(symbol, token)
                            )
                        telemetry.phase = "COMPLETED_ONE_MINUTE_FETCH"
                        frame, marker, reused_snapshot = load_or_fetch_completed_minute(
                            runtime_paths,
                            requests,
                            runtimes,
                            next_end,
                            observed_now=_normalize_now(now_provider()),
                            minute_fetcher=minute_fetcher,
                        )
                        _require_runtime_activation(
                            session_date,
                            _normalize_now(now_provider()),
                            control_paths=control_paths,
                            bundle_provider=bundle_provider,
                            expected_bundle_sha256=bundle_digest,
                        )
                        if marker.get("complete") is not True or marker.get("state") != "SUCCESS":
                            telemetry.incomplete_minutes += 1
                            telemetry.data_incomplete = True
                            archive_event(
                                runtime_paths,
                                {
                                    "event_time": next_end.isoformat(),
                                    "state": "DATA_INCOMPLETE",
                                    "marker": marker,
                                },
                                kind="DATA_INCOMPLETE",
                            )
                            # Reconcile the exact partial evidence once.  The
                            # reducer marks missing active candidates as
                            # DATA_INCOMPLETE and can still close a present
                            # candidate deterministically; no bar is invented.
                            process_engine_minute(
                                engine, module, next_end, frame, runtime_paths
                            )
                            processed_clock_end = next_end
                            telemetry.last_completed_minute = next_end.isoformat()
                            persist_checkpoint(
                                runtime_paths,
                                engine,
                                telemetry,
                                processed_clock_end=processed_clock_end,
                                ingested_slots=ingested_slots,
                                symbol_tokens=symbol_tokens,
                            )
                            raise SourceIncompleteError(
                                f"exact completed minute is incomplete: {next_end.isoformat()}"
                            )
                    telemetry.phase = "CHRONOLOGICAL_PAPER_REDUCER"
                    process_engine_minute(
                        engine, module, next_end, frame, runtime_paths
                    )
                    processed_clock_end = next_end
                    telemetry.completed_minutes += 1
                    telemetry.last_completed_minute = next_end.isoformat()
                    persist_checkpoint(
                        runtime_paths,
                        engine,
                        telemetry,
                        processed_clock_end=processed_clock_end,
                        ingested_slots=ingested_slots,
                        symbol_tokens=symbol_tokens,
                    )
                    publish_runtime_state(runtime_paths, telemetry)
                    if next_end.time() == SQUARE_OFF_TIME:
                        telemetry.state = "COMPLETED"
                        telemetry.phase = "EXACT_1530_COMPLETE"
                        _finalize_session(runtime_paths, telemetry, engine)
                        return 0
                    next_end += timedelta(minutes=1)

                publish_report(
                    runtime_paths, telemetry, engine.records(), final=False
                )
                publish_runtime_state(runtime_paths, telemetry, heartbeat_only=True)
                sleep_fn(max(0.05, float(poll_seconds)))
        except control.ActivationBlockedError as exc:
            telemetry.activation_reason = exc.reason
            telemetry.messages.append(str(exc))
            telemetry.phase = "KILL_OR_PERMIT_RECHECK"
            try:
                _, processed_clock_end = _resolve_control_intervention(
                    reason=exc.reason,
                    paths=runtime_paths,
                    engine=engine,
                    module=module,
                    telemetry=telemetry,
                    runtimes=runtimes,
                    symbol_tokens=symbol_tokens,
                    ingested_slots=ingested_slots,
                    processed_clock_end=processed_clock_end,
                    now_provider=now_provider,
                    sleep_fn=sleep_fn,
                    minute_fetcher=minute_fetcher,
                    poll_seconds=poll_seconds,
                )
            except Exception as intervention_exc:
                telemetry.state = "STOPPED_CONTROL_UNRESOLVED"
                telemetry.phase = "INTERVENTION_DATA_INCOMPLETE"
                telemetry.data_incomplete = True
                telemetry.messages.append(
                    "Could not obtain exact completed intervention economics: "
                    f"{type(intervention_exc).__name__}: {intervention_exc}"
                )
                persist_checkpoint(
                    runtime_paths,
                    engine,
                    telemetry,
                    processed_clock_end=processed_clock_end,
                    ingested_slots=ingested_slots,
                    symbol_tokens=symbol_tokens,
                )
            _finalize_session(runtime_paths, telemetry, engine)
            return 2
        except (SourceIncompleteError, SessionContractError) as exc:
            telemetry.state = "DATA_INCOMPLETE"
            telemetry.phase = "FAIL_CLOSED_DATA_OR_SESSION_CONTRACT"
            telemetry.data_incomplete = True
            telemetry.messages.append(f"{type(exc).__name__}: {exc}")
            persist_checkpoint(
                runtime_paths,
                engine,
                telemetry,
                processed_clock_end=processed_clock_end,
                ingested_slots=ingested_slots,
                symbol_tokens=symbol_tokens,
            )
            _finalize_session(runtime_paths, telemetry, engine)
            return 2
        except KeyboardInterrupt:
            telemetry.state = "STOPPED_OPERATOR_INTERRUPT"
            telemetry.phase = "INTERRUPTED"
            telemetry.data_incomplete = True
            telemetry.messages.append("Operator interrupted the PAPER session.")
            persist_checkpoint(
                runtime_paths,
                engine,
                telemetry,
                processed_clock_end=processed_clock_end,
                ingested_slots=ingested_slots,
                symbol_tokens=symbol_tokens,
            )
            _finalize_session(runtime_paths, telemetry, engine)
            return 130
        except Exception as exc:
            # Provider, filesystem, and library failures do not necessarily
            # inherit the reviewed contract exceptions.  Never leave a stale
            # RUNNING dashboard or continue from partial evidence.
            telemetry.state = "BLOCKED"
            telemetry.phase = "UNEXPECTED_FAIL_CLOSED"
            telemetry.data_incomplete = True
            telemetry.messages.append(f"Unexpected {type(exc).__name__}: {exc}")
            try:
                persist_checkpoint(
                    runtime_paths,
                    engine,
                    telemetry,
                    processed_clock_end=processed_clock_end,
                    ingested_slots=ingested_slots,
                    symbol_tokens=symbol_tokens,
                )
            except Exception as persist_exc:
                print(
                    "[FNO-V8-PAPER][CHECKPOINT_FAILED] "
                    f"{type(persist_exc).__name__}: {persist_exc}",
                    flush=True,
                )
            try:
                _finalize_session(runtime_paths, telemetry, engine)
            except Exception as publish_exc:
                print(
                    "[FNO-V8-PAPER][STATUS_PUBLISH_FAILED] "
                    f"{type(publish_exc).__name__}: {publish_exc}",
                    flush=True,
                )
            return 2


def _parse_session_date(value: str | None) -> date:
    if value is None:
        return now_ist().date()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO session date: {value}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Independent FNO V8-Combined PAPER same-session orchestrator"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="run one approved PAPER session")
    run_parser.add_argument("--session-date", help="exact IST session date (YYYY-MM-DD)")
    run_parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    preflight_parser = subparsers.add_parser(
        "preflight", help="validate the immutable PAPER runtime and optional controls"
    )
    preflight_parser.add_argument("--session-date", help="exact IST session date (YYYY-MM-DD)")
    preflight_parser.add_argument("--require-activation", action="store_true")
    preflight_parser.add_argument("--authenticate-apps", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    session_date = _parse_session_date(args.session_date)
    if args.command == "preflight":
        code, payload = run_preflight(
            session_date,
            require_activation=bool(args.require_activation),
            authenticate_apps=bool(args.authenticate_apps),
        )
        print(json.dumps(payload, indent=2, sort_keys=True, default=str), flush=True)
        return code
    return run_paper_session(
        session_date,
        poll_seconds=max(0.05, float(args.poll_seconds)),
    )


if __name__ == "__main__":
    raise SystemExit(main())
