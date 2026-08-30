"""Shared, fail-closed market-data source for V10/V11/V12 PAPER.

The adapter deliberately owns no strategy decisions.  It seals one complete
all-mapped-stock five-minute metric universe for a signal slot and one union
of exact completed one-minute candles for each minute.  V10, V11 and V12 must
apply their own gates, ranking, caps and portfolio rules downstream.

The implementation reuses the independently rebuilt source contracts from the
V8 PAPER runtime.  The V6 scanner is never read and is not an authority here.
"""

from __future__ import annotations

import json
import math
import inspect
import io
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

import fno_oi_common as common
import fno_multi_paper_profiles as profiles
import fno_v8_combined_paper_market_data as market_data
import fno_v8_combined_paper_session as v8_source


LIVE_SOURCE_SCHEMA_VERSION = "fno_multi_paper_live_source_v2"
FIVE_MINUTE_MANIFEST_SCHEMA_VERSION = "fno_multi_paper_5m_source_v2"
FIVE_MINUTE_ROW_SCHEMA_VERSION = "fno_multi_paper_5m_metric_row_v2"
RAW_DIRECT_AUDIT_SCHEMA_VERSION = "fno_multi_paper_raw_direct_5x1_v1"
BOUND_DIRECT_AUDIT_SCHEMA_VERSION = "fno_multi_paper_bound_direct_5x1_v1"
UNION_MINUTE_SCHEMA_VERSION = "fno_multi_paper_union_1m_v2"
RUNTIME_POOL_POLICY_VERSION = "fno_multi_paper_approved_healthy_pool_min7_v1"
MIN_HEALTHY_APP_COUNT = 7
SIGNAL_ENDS = ("09:25", "09:30", "09:35", "09:40", "09:45")

_RAW_AUDIT_EXECUTOR = ThreadPoolExecutor(
    max_workers=len(SIGNAL_ENDS),
    thread_name_prefix="fno-multi-paper-raw-audit-stage",
)
_RAW_AUDIT_FUTURES: dict[str, Future[tuple[dict[str, Any], str]]] = {}
_RAW_AUDIT_FUTURES_LOCK = threading.Lock()


def _profile_binding_payload() -> dict[str, Any]:
    return {
        "schema_version": LIVE_SOURCE_SCHEMA_VERSION,
        "source_policy_version": v8_source.SOURCE_POLICY_VERSION,
        "minute_policy_version": market_data.MARKET_DATA_POLICY_VERSION,
        "runtime_pool_policy_version": RUNTIME_POOL_POLICY_VERSION,
        "profiles": [
            {
                "key": profile.key,
                "profile_id": profile.profile_id,
                "fingerprint": profile.fingerprint,
            }
            for profile in profiles.PROFILES
        ],
    }


PROFILE_BINDING_PAYLOAD = _profile_binding_payload()
PROFILE_BUNDLE_FINGERPRINT = profiles.canonical_sha256(PROFILE_BINDING_PAYLOAD)


SourceNotReadyError = v8_source.SourceNotReadyError
SourceIncompleteError = v8_source.SourceIncompleteError
SourceContractError = v8_source.SessionContractError


@dataclass(frozen=True)
class LiveSourcePaths:
    """Physical source roots plus a caller-owned immutable evidence root."""

    session_date: date
    root: Path
    five_minute_root: Path = v8_source.DEFAULT_FIVE_MINUTE_ROOT
    futures_five_minute_root: Path = v8_source.DEFAULT_FUTURES_FIVE_MINUTE_ROOT
    futures_slot_root: Path = v8_source.DEFAULT_FUTURES_SLOT_ROOT
    near_month_universe_path: Path = v8_source.DEFAULT_NEAR_MONTH_UNIVERSE_PATH
    cash_slot_root: Path = v8_source.DEFAULT_CASH_SLOT_ROOT

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root))
        for name in (
            "five_minute_root",
            "futures_five_minute_root",
            "futures_slot_root",
            "near_month_universe_path",
            "cash_slot_root",
        ):
            object.__setattr__(self, name, Path(getattr(self, name)))

    @property
    def day_evidence_root(self) -> Path:
        return self.root / "evidence" / self.session_date.isoformat()

    @property
    def minute_root(self) -> Path:
        return self.day_evidence_root / "one_minute_union"

    def five_minute_slot_root(self, signal_end: str) -> Path:
        return self.day_evidence_root / "five_minute" / f"slot_{signal_end.replace(':', '')}"

    def raw_direct_audit_path(self, signal_end: str) -> Path:
        return self.five_minute_slot_root(signal_end) / "direct_5x1_raw_audit.json"

    def as_v8_paths(self) -> v8_source.SessionPaths:
        return v8_source.SessionPaths(
            session_date=self.session_date,
            root=self.root,
            five_minute_root=self.five_minute_root,
            futures_five_minute_root=self.futures_five_minute_root,
            futures_slot_root=self.futures_slot_root,
            near_month_universe_path=self.near_month_universe_path,
            cash_slot_root=self.cash_slot_root,
        )


@dataclass(frozen=True)
class FiveMinuteSourceResult:
    signal_end: str
    rows: tuple[Mapping[str, Any], ...]
    symbol_tokens: Mapping[str, int]
    manifest: Mapping[str, Any]
    manifest_path: Path
    manifest_artifact_sha256: str
    reused: bool


@dataclass(frozen=True)
class UnionMinuteSnapshot:
    expected_end: pd.Timestamp
    frame: pd.DataFrame
    bars_by_symbol: Mapping[str, Mapping[str, Any]]
    marker: Mapping[str, Any]
    marker_path: Path
    reused: bool


def _as_ist(value: Any) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(common.IST)
    else:
        stamp = stamp.tz_convert(common.IST)
    return stamp.to_pydatetime()


def _signal_at(session_date: date, signal_end: str) -> datetime:
    if signal_end not in SIGNAL_ENDS:
        raise ValueError(f"unsupported FnO signal slot: {signal_end!r}")
    return datetime.combine(
        session_date,
        datetime.strptime(signal_end, "%H:%M").time(),
        tzinfo=common.IST,
    )


def _sha256_file(path: Path) -> str:
    return v8_source._sha256_file(Path(path))


def _read_json_artifact(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = Path(path).read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceContractError(f"immutable source artifact is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise SourceContractError(f"immutable source artifact is not an object: {path}")
    return payload, v8_source._sha256_bytes(raw)


def _publish_json_once(path: Path, payload: Mapping[str, Any]) -> str:
    return v8_source._write_immutable_json(Path(path), dict(payload))


def _with_timing_and_hash(
    payload: Mapping[str, Any],
    *,
    start: datetime,
    finish: datetime,
    due: datetime,
    prefix: str,
    hash_field: str,
) -> dict[str, Any]:
    if finish >= due:
        raise SourceIncompleteError(
            f"{prefix} source crossed the prospective S+1 boundary"
        )
    result = dict(payload)
    result.pop(hash_field, None)
    result.update(
        {
            f"{prefix}_started_at_ist": start.isoformat(),
            f"{prefix}_finished_at_ist": finish.isoformat(),
            "confirmation_due_ist": due.isoformat(),
            f"{prefix}_completed_before_confirmation_due": True,
        }
    )
    result[hash_field] = common.canonical_json_sha256(result)
    return result


def authenticate_all_apps(
    **kwargs: Any,
) -> tuple[market_data.AppRuntime, ...]:
    """Return an approved healthy app pool, or fail before source work."""

    runtimes = tuple(market_data.authenticate_required_apps(**kwargs))
    return _require_runtime_pool(runtimes)


def _require_runtime_pool(
    runtimes: Sequence[market_data.AppRuntime],
) -> tuple[market_data.AppRuntime, ...]:
    pool = tuple(runtimes)
    validator = getattr(market_data, "validate_runtime_pool", None)
    if callable(validator):
        try:
            pool = tuple(
                validator(pool, minimum_healthy_apps=MIN_HEALTHY_APP_COUNT)
            )
        except Exception as exc:
            raise SourceContractError(str(exc)) from exc
    names = tuple(str(runtime.app_name) for runtime in pool)
    expected = tuple(
        name for name in market_data.EXPECTED_APP_NAMES if name in set(names)
    )
    if (
        len(names) < MIN_HEALTHY_APP_COUNT
        or len(names) != len(set(names))
        or names != expected
    ):
        raise SourceContractError(
            "multi-paper source requires at least seven unique approved apps in "
            f"app1..app8 order; observed={list(names)}"
        )
    return pool


def _recorded_roster_is_approved(payload: Any, *, minimum: int) -> bool:
    if not isinstance(payload, list) or not all(
        isinstance(item, Mapping) for item in payload
    ):
        return False
    names = tuple(str(item.get("app_name", "")) for item in payload)
    expected = tuple(
        name for name in market_data.EXPECTED_APP_NAMES if name in set(names)
    )
    return bool(
        len(names) >= int(minimum)
        and len(names) == len(set(names))
        and names == expected
        and all(item.get("authenticated") is True for item in payload)
    )


def _app_authentication_payload(
    runtimes: Sequence[market_data.AppRuntime],
) -> list[dict[str, Any]]:
    provider = getattr(market_data, "app_authentication_payload", None)
    if callable(provider):
        return [dict(item) for item in provider(runtimes)]
    return market_data.app_roster_payload(runtimes)


def _is_stock_future(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"false", "0", "no", "off"}
    return value is not None and not bool(value)


def _load_frozen_cash_audit_contract(
    paths: LiveSourcePaths,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read the morning near-month mapping without depending on slot outputs."""

    universe_path = Path(paths.near_month_universe_path)
    try:
        universe_raw = universe_path.read_bytes()
        universe = pd.read_parquet(io.BytesIO(universe_raw))
    except Exception as exc:
        raise SourceIncompleteError(
            f"morning near-month universe is unreadable: {universe_path}"
        ) from exc
    required = {
        "master_date",
        "is_index_future",
        "tradingsymbol",
        "instrument_token",
        "equity_symbol",
        "equity_instrument_token",
    }
    if universe.empty or not required.issubset(universe.columns):
        raise SourceIncompleteError(
            "morning near-month universe lacks the mapped cash identity contract"
        )
    observed_dates = set(
        pd.to_datetime(universe["master_date"], errors="coerce")
        .dropna()
        .dt.date
    )
    if observed_dates != {paths.session_date}:
        raise SourceIncompleteError(
            "morning near-month universe is not frozen for the session date"
        )

    stock_rows = universe.loc[
        universe["is_index_future"].map(_is_stock_future)
    ]
    cash_contract: list[dict[str, Any]] = []
    mapped_contract: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    seen_cash_tokens: set[int] = set()
    seen_futures_symbols: set[str] = set()
    seen_futures_tokens: set[int] = set()
    for raw in stock_rows.to_dict("records"):
        cash_symbol = str(raw.get("equity_symbol", "")).strip().upper()
        futures_symbol = str(raw.get("tradingsymbol", "")).strip().upper()
        try:
            cash_token = int(raw.get("equity_instrument_token", 0) or 0)
            futures_token = int(raw.get("instrument_token", 0) or 0)
        except (TypeError, ValueError) as exc:
            raise SourceIncompleteError(
                "morning near-month universe contains an invalid token"
            ) from exc
        if (
            not cash_symbol
            or cash_token <= 0
            or not futures_symbol
            or futures_token <= 0
            or cash_symbol in seen_symbols
            or cash_token in seen_cash_tokens
            or futures_symbol in seen_futures_symbols
            or futures_token in seen_futures_tokens
        ):
            raise SourceIncompleteError(
                "morning near-month stock/cash mapping is incomplete or non-unique"
            )
        seen_symbols.add(cash_symbol)
        seen_cash_tokens.add(cash_token)
        seen_futures_symbols.add(futures_symbol)
        seen_futures_tokens.add(futures_token)
        cash_contract.append(
            {"tradingsymbol": cash_symbol, "instrument_token": cash_token}
        )
        mapped_contract.append(
            {
                "cash_symbol": cash_symbol,
                "cash_instrument_token": cash_token,
                "futures_symbol": futures_symbol,
                "futures_instrument_token": futures_token,
            }
        )
    cash_contract.sort(key=lambda item: (item["tradingsymbol"], item["instrument_token"]))
    mapped_contract.sort(key=lambda item: item["cash_symbol"])
    if not cash_contract:
        raise SourceIncompleteError("morning near-month stock universe is empty")
    request_contract = [
        {"symbol": item["tradingsymbol"], "instrument_token": item["instrument_token"]}
        for item in cash_contract
    ]
    metadata = {
        "near_month_universe_path": str(universe_path),
        "near_month_universe_sha256": v8_source._sha256_bytes(universe_raw),
        "cash_symbol_count": len(cash_contract),
        "cash_symbol_set_sha256": common.symbol_set_sha256(
            item["tradingsymbol"] for item in cash_contract
        ),
        "cash_symbol_contract_sha256": common.canonical_json_sha256(
            request_contract
        ),
        "mapped_stock_cash_contract_sha256": common.canonical_json_sha256(
            mapped_contract
        ),
        "cash_symbol_tokens": {
            item["tradingsymbol"]: item["instrument_token"]
            for item in cash_contract
        },
    }
    return cash_contract, metadata


def _validate_direct_symbol_payload(
    symbol: str,
    payload: Mapping[str, Any],
    *,
    signal_at: datetime,
    approved_apps: set[str],
) -> None:
    constituents = payload.get("constituents")
    if not isinstance(constituents, list) or len(constituents) != 5:
        raise SourceContractError(
            f"raw direct 5x1 audit lacks five constituents for {symbol}"
        )
    expected_starts = [
        pd.Timestamp(signal_at - timedelta(minutes=offset))
        for offset in range(5, 0, -1)
    ]
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(constituents):
        if not isinstance(raw, Mapping):
            raise SourceContractError(
                f"raw direct 5x1 audit contains a non-object candle for {symbol}"
            )
        try:
            start = pd.Timestamp(raw.get("candle_start"))
            end = pd.Timestamp(raw.get("timestamp"))
            if start.tzinfo is None:
                start = start.tz_localize(common.IST)
            else:
                start = start.tz_convert(common.IST)
            if end.tzinfo is None:
                end = end.tz_localize(common.IST)
            else:
                end = end.tz_convert(common.IST)
            values = {
                name: float(raw.get(name))
                for name in ("open", "high", "low", "close", "volume")
            }
        except (TypeError, ValueError) as exc:
            raise SourceContractError(
                f"raw direct 5x1 candle is invalid for {symbol}"
            ) from exc
        if (
            start != expected_starts[index]
            or end != start + pd.Timedelta(minutes=1)
            or not all(math.isfinite(value) for value in values.values())
            or values["volume"] < 0
            or values["low"] > min(values["open"], values["close"])
            or values["high"] < max(values["open"], values["close"])
            or values["low"] > values["high"]
            or str(raw.get("app_name", "")) not in approved_apps
        ):
            raise SourceContractError(
                f"raw direct 5x1 candle contract mismatch for {symbol}"
            )
        normalized.append(dict(raw))
    aggregate = {
        "open": float(normalized[0]["open"]),
        "high": max(float(item["high"]) for item in normalized),
        "low": min(float(item["low"]) for item in normalized),
        "close": float(normalized[-1]["close"]),
        "volume": sum(float(item["volume"]) for item in normalized),
    }
    for name, expected in aggregate.items():
        try:
            observed = float(payload.get(name))
        except (TypeError, ValueError) as exc:
            raise SourceContractError(
                f"raw direct aggregate is invalid for {symbol}"
            ) from exc
        if not math.isclose(observed, expected, rel_tol=0.0, abs_tol=1e-9):
            raise SourceContractError(
                f"raw direct aggregate {name} mismatch for {symbol}"
            )
    if (
        payload.get("constituents_sha256")
        != common.canonical_json_sha256(normalized)
        or str(payload.get("app_name", "")) not in approved_apps
    ):
        raise SourceContractError(
            f"raw direct 5x1 provenance mismatch for {symbol}"
        )


def _validate_raw_direct_audit(
    *,
    paths: LiveSourcePaths,
    signal_end: str,
    payload: Mapping[str, Any],
    frozen_contract: Sequence[Mapping[str, Any]],
    frozen_metadata: Mapping[str, Any],
) -> None:
    unsigned = dict(payload)
    claimed = str(unsigned.pop("raw_audit_sha256", ""))
    signal_at = _signal_at(paths.session_date, signal_end)
    due = signal_at + timedelta(
        minutes=1, seconds=market_data.DEFAULT_BOUNDARY_BUFFER_SEC
    )
    try:
        started = _as_ist(payload.get("raw_audit_started_at_ist"))
        finished = _as_ist(payload.get("raw_audit_finished_at_ist"))
        recorded_due = _as_ist(payload.get("confirmation_due_ist"))
    except Exception as exc:
        raise SourceContractError("raw direct 5x1 timing evidence is invalid") from exc
    expected_symbols = {
        str(item["tradingsymbol"]).strip().upper(): int(item["instrument_token"])
        for item in frozen_contract
    }
    symbols = payload.get("symbols")
    recorded_tokens = payload.get("cash_symbol_tokens")
    roster = payload.get("app_roster")
    approved_apps = {
        str(item.get("app_name", ""))
        for item in roster or ()
        if isinstance(item, Mapping)
    }
    outcomes = payload.get("outcomes")
    outcome_symbols = sorted(
        str(item.get("symbol", "")).strip().upper()
        for item in outcomes or ()
        if isinstance(item, Mapping) and item.get("state") == "SUCCESS"
    )
    current_universe_path = Path(str(payload.get("near_month_universe_path", "")))
    checks = (
        payload.get("raw_schema_version") == RAW_DIRECT_AUDIT_SCHEMA_VERSION,
        payload.get("multi_source_schema_version") == LIVE_SOURCE_SCHEMA_VERSION,
        payload.get("kind") == "RAW_DIRECT_CASH_SIGNAL_5X1M_AUDIT",
        payload.get("authority") == "FROZEN_MORNING_NEAR_MONTH_MAPPED_CASH_UNIVERSE",
        payload.get("session_date") == paths.session_date.isoformat(),
        payload.get("signal_end") == signal_end,
        payload.get("signal_timestamp") == signal_at.isoformat(),
        payload.get("profile_bundle_fingerprint") == PROFILE_BUNDLE_FINGERPRINT,
        payload.get("runtime_pool_policy_version") == RUNTIME_POOL_POLICY_VERSION,
        _recorded_roster_is_approved(roster, minimum=MIN_HEALTHY_APP_COUNT),
        payload.get("app_roster_sha256") == common.canonical_json_sha256(roster),
        int(payload.get("healthy_app_count", -1)) == len(roster or ()),
        isinstance(symbols, Mapping),
        sorted(str(value).strip().upper() for value in symbols or {})
        == sorted(expected_symbols),
        isinstance(recorded_tokens, Mapping),
        {str(key).strip().upper(): int(value) for key, value in (recorded_tokens or {}).items()}
        == expected_symbols,
        int(payload.get("candidate_count", -1)) == len(expected_symbols),
        int(payload.get("cash_symbol_count", -1)) == len(expected_symbols),
        payload.get("candidate_contract_sha256")
        == frozen_metadata.get("cash_symbol_contract_sha256"),
        payload.get("cash_symbol_contract_sha256")
        == frozen_metadata.get("cash_symbol_contract_sha256"),
        payload.get("cash_symbol_set_sha256")
        == frozen_metadata.get("cash_symbol_set_sha256"),
        payload.get("mapped_stock_cash_contract_sha256")
        == frozen_metadata.get("mapped_stock_cash_contract_sha256"),
        current_universe_path == Path(paths.near_month_universe_path),
        payload.get("near_month_universe_sha256")
        == frozen_metadata.get("near_month_universe_sha256"),
        current_universe_path.is_file(),
        current_universe_path.is_file()
        and _sha256_file(current_universe_path)
        == payload.get("near_month_universe_sha256"),
        started >= signal_at
        + timedelta(seconds=market_data.DEFAULT_BOUNDARY_BUFFER_SEC),
        started <= finished < due,
        recorded_due == due,
        payload.get("raw_completed_before_confirmation_due") is True,
        outcome_symbols == sorted(expected_symbols),
        claimed == common.canonical_json_sha256(unsigned),
    )
    if not all(checks):
        raise SourceContractError("raw direct 5x1 audit contract mismatch")
    assert isinstance(symbols, Mapping)
    for symbol in sorted(expected_symbols):
        direct = symbols.get(symbol)
        if not isinstance(direct, Mapping):
            raise SourceContractError(
                f"raw direct 5x1 audit lacks symbol payload: {symbol}"
            )
        _validate_direct_symbol_payload(
            symbol,
            direct,
            signal_at=signal_at,
            approved_apps=approved_apps,
        )


def _clean_provider_error(exc: BaseException) -> str:
    return " ".join(f"{type(exc).__name__}: {exc}".split())[:500]


def _runtime_historical_data(
    runtime: market_data.AppRuntime,
    *args: Any,
    deadline_at: datetime,
    monotonic_deadline: float | None = None,
    **kwargs: Any,
) -> Any:
    """Use an AppRuntime-wide serialized/deadline-aware broker seam if present."""

    caller = getattr(runtime, "call_historical_data", None)
    if not callable(caller):
        caller = getattr(runtime, "historical_data_exact", None)
    if not callable(caller):
        # Compatibility path for a legacy runtime.  Modern AppRuntime owns
        # pacing inside its serialized call wrapper, so pacing must not happen
        # at the call site as well.
        runtime.pace()
        return runtime.client.historical_data(*args, **kwargs)
    try:
        signature = inspect.signature(caller)
    except (TypeError, ValueError):
        signature = None
    accepts_kwargs = signature is not None and any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    # Prefer the already-frozen monotonic deadline.  Passing both an injected
    # wall-clock deadline and a monotonic deadline makes deterministic replay
    # tests depend on today's clock and can incorrectly collapse the budget to
    # zero.  The wall-clock form remains the fallback for older wrappers.
    if monotonic_deadline is not None and signature is not None and (
        "monotonic_deadline" in signature.parameters or accepts_kwargs
    ):
        kwargs["monotonic_deadline"] = float(monotonic_deadline)
    elif signature is not None and (
        "deadline_at" in signature.parameters
        or accepts_kwargs
    ):
        kwargs["deadline_at"] = deadline_at
    return caller(*args, **kwargs)


def _fetch_exact_cash_signal_constituents_resilient(
    snapshot: Mapping[str, Any],
    paths: v8_source.SessionPaths,
    signal_end: str,
    runtimes: Sequence[market_data.AppRuntime],
    *,
    observed_at: datetime,
    deadline_at: datetime,
    observations: int = market_data.DEFAULT_OBSERVATIONS,
    observation_spacing_sec: float = market_data.DEFAULT_OBSERVATION_SPACING_SEC,
    circuit_breaker_failures: int = market_data.DEFAULT_CIRCUIT_BREAKER_FAILURES,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Fetch exact 5x1 ranges through one deadline-aware cross-app queue."""

    pool = _require_runtime_pool(runtimes)
    attempt_limit = int(observations)
    breaker_limit = int(circuit_breaker_failures)
    if attempt_limit < 1 or breaker_limit < 1:
        raise ValueError("direct-audit retry and circuit limits must be positive")
    signal_at = _signal_at(paths.session_date, signal_end)
    observed = _as_ist(observed_at)
    deadline = _as_ist(deadline_at)
    ready_at = signal_at + timedelta(
        seconds=market_data.DEFAULT_BOUNDARY_BUFFER_SEC
    )
    if observed < ready_at:
        raise SourceNotReadyError(
            f"direct 5x1 audit cannot start before {ready_at.isoformat()}"
        )
    if observed >= deadline:
        raise SourceIncompleteError(
            "direct 5x1 audit has no prospective deadline budget"
        )

    requests: list[market_data.CandidateRequest] = []
    by_symbol: dict[str, market_data.CandidateRequest] = {}
    token_to_symbol: dict[int, str] = {}
    for raw in snapshot.get("candidates") or ():
        request = market_data.CandidateRequest.from_mapping(raw)
        prior = by_symbol.setdefault(request.symbol, request)
        if prior.instrument_token != request.instrument_token:
            raise SourceIncompleteError(
                f"direct-audit cash token changed for {request.symbol}"
            )
        other = token_to_symbol.setdefault(request.instrument_token, request.symbol)
        if other != request.symbol:
            raise SourceIncompleteError(
                "direct-audit cash token maps to multiple symbols"
            )
    requests = sorted(by_symbol.values(), key=lambda item: item.symbol)
    if not requests:
        raise SourceIncompleteError("direct-audit frozen cash contract is empty")

    expected_starts = [
        pd.Timestamp(signal_at - timedelta(minutes=offset))
        for offset in range(5, 0, -1)
    ]
    expected_start_set = set(expected_starts)
    monotonic_deadline = time.monotonic() + max(
        0.0, (deadline - observed).total_seconds()
    )
    spacing = max(0.0, float(observation_spacing_sec))
    state_by_symbol: dict[str, dict[str, Any]] = {
        request.symbol: {
            "attempts": [],
            "attempted_apps": set(),
            "ready_at": 0.0,
            "value": None,
            "terminal_error": "",
        }
        for request in requests
    }
    app_state: dict[str, dict[str, Any]] = {
        runtime.app_name: {
            "consecutive_api_failures": 0,
            "circuit_opened": 0,
            "quarantined": False,
            "request_count": 0,
        }
        for runtime in pool
    }
    runtime_by_name = {runtime.app_name: runtime for runtime in pool}
    pending_symbols: deque[str] = deque(request.symbol for request in requests)
    available_apps = set(runtime_by_name)
    inflight: dict[Future[Any], tuple[str, str, datetime, float]] = {}

    def observe(
        runtime: market_data.AppRuntime,
        request: market_data.CandidateRequest,
    ) -> tuple[str, Mapping[str, Any] | None, str, datetime, float]:
        started_monotonic = time.monotonic()
        try:
            raw_records = _runtime_historical_data(
                runtime,
                int(request.instrument_token),
                expected_starts[0].to_pydatetime(),
                (pd.Timestamp(signal_at) + pd.Timedelta(minutes=1)).to_pydatetime(),
                "minute",
                continuous=False,
                oi=False,
                deadline_at=deadline,
                monotonic_deadline=monotonic_deadline,
            )
        except Exception as exc:
            return (
                "API_FAILURE",
                None,
                _clean_provider_error(exc),
                common.now_ist(),
                time.monotonic() - started_monotonic,
            )
        try:
            by_start: dict[pd.Timestamp, Mapping[str, Any]] = {}
            duplicate = False
            for raw in raw_records or ():
                raw_time = raw.get("date", raw.get("timestamp"))
                if raw_time is None:
                    continue
                start = pd.Timestamp(raw_time)
                if start.tzinfo is None:
                    start = start.tz_localize(common.IST)
                else:
                    start = start.tz_convert(common.IST)
                if start not in expected_start_set:
                    continue
                duplicate = duplicate or start in by_start
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
                        "open": float(record["open"]),
                        "high": float(record["high"]),
                        "low": float(record["low"]),
                        "close": float(record["close"]),
                        "volume": float(record["volume"]),
                        "app_name": runtime.app_name,
                    }
                )
            value = {
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
            }
            return (
                "SUCCESS",
                value,
                "",
                common.now_ist(),
                time.monotonic() - started_monotonic,
            )
        except Exception as exc:
            return (
                "DATA_INCOMPLETE",
                None,
                _clean_provider_error(exc),
                common.now_ist(),
                time.monotonic() - started_monotonic,
            )

    def eligible_symbol(app_name: str, now_monotonic: float) -> str | None:
        for _ in range(len(pending_symbols)):
            symbol = pending_symbols.popleft()
            state = state_by_symbol[symbol]
            if state["value"] is not None:
                continue
            if float(state["ready_at"]) > now_monotonic:
                pending_symbols.append(symbol)
                continue
            alternatives = {
                name
                for name, health in app_state.items()
                if name != app_name
                and not bool(health["quarantined"])
                and name not in state["attempted_apps"]
            }
            if app_name in state["attempted_apps"] and alternatives:
                pending_symbols.append(symbol)
                continue
            return symbol
        return None

    executor = ThreadPoolExecutor(
        max_workers=len(pool),
        thread_name_prefix="fno-multi-paper-direct-5x1",
    )
    deadline_exhausted = False
    try:
        while pending_symbols or inflight:
            now_monotonic = time.monotonic()
            completed = {future for future in inflight if future.done()}
            if now_monotonic >= monotonic_deadline and not completed:
                deadline_exhausted = bool(pending_symbols or inflight)
                break
            for app_name in market_data.EXPECTED_APP_NAMES:
                if (
                    app_name not in available_apps
                    or app_name not in runtime_by_name
                    or bool(app_state[app_name]["quarantined"])
                    or not pending_symbols
                    or time.monotonic() >= monotonic_deadline
                ):
                    continue
                symbol = eligible_symbol(app_name, time.monotonic())
                if symbol is None:
                    continue
                available_apps.remove(app_name)
                started_at = common.now_ist()
                started_monotonic = time.monotonic()
                future = executor.submit(
                    observe,
                    runtime_by_name[app_name],
                    by_symbol[symbol],
                )
                inflight[future] = (
                    app_name,
                    symbol,
                    started_at,
                    started_monotonic,
                )
                app_state[app_name]["request_count"] += 1

            if not inflight:
                if not pending_symbols:
                    break
                if all(bool(value["quarantined"]) for value in app_state.values()):
                    deadline_exhausted = True
                    break
                wake_at = min(
                    [monotonic_deadline]
                    + [
                        float(state_by_symbol[symbol]["ready_at"])
                        for symbol in pending_symbols
                        if float(state_by_symbol[symbol]["ready_at"]) > now_monotonic
                    ]
                )
                time.sleep(max(0.0, min(0.02, wake_at - now_monotonic)))
                continue
            if not completed:
                remaining = max(0.0, monotonic_deadline - time.monotonic())
                completed, _ = wait(
                    tuple(inflight),
                    timeout=min(0.02, remaining),
                    return_when=FIRST_COMPLETED,
                )
            for future in completed:
                app_name, symbol, started_at, started_monotonic = inflight.pop(future)
                available_apps.add(app_name)
                state = state_by_symbol[symbol]
                try:
                    status, value, error, finished_at, duration = future.result()
                except Exception as exc:
                    status, value, error = (
                        "API_FAILURE",
                        None,
                        _clean_provider_error(exc),
                    )
                    finished_at = common.now_ist()
                    duration = time.monotonic() - started_monotonic
                if time.monotonic() > monotonic_deadline:
                    status, value, error = (
                        "DEADLINE_EXCEEDED",
                        None,
                        "broker response arrived after the direct-audit deadline",
                    )
                    deadline_exhausted = True
                state["attempted_apps"].add(app_name)
                state["attempts"].append(
                    {
                        "attempt": len(state["attempts"]) + 1,
                        "app_name": app_name,
                        "state": status,
                        "error": error,
                        "started_at_ist": started_at.isoformat(timespec="microseconds"),
                        "finished_at_ist": finished_at.isoformat(timespec="microseconds"),
                        "duration_sec": round(float(duration), 6),
                    }
                )
                health = app_state[app_name]
                if status == "API_FAILURE":
                    health["consecutive_api_failures"] += 1
                    if int(health["consecutive_api_failures"]) >= breaker_limit:
                        health["circuit_opened"] += 1
                        health["quarantined"] = True
                else:
                    health["consecutive_api_failures"] = 0
                if status == "SUCCESS" and value is not None:
                    state["value"] = dict(value)
                    continue
                state["terminal_error"] = error
                if status == "DEADLINE_EXCEEDED" or len(state["attempts"]) >= attempt_limit:
                    continue
                state["ready_at"] = time.monotonic() + spacing
                pending_symbols.append(symbol)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    fetched = {
        symbol: dict(state["value"])
        for symbol, state in state_by_symbol.items()
        if isinstance(state["value"], Mapping)
    }
    missing = sorted(set(by_symbol) - set(fetched))
    if missing:
        label = "deadline exceeded" if deadline_exhausted else "incomplete"
        raise SourceIncompleteError(
            f"direct 5x1 cash audit {label} for symbols: {missing}"
        )
    outcomes = [
        {
            "symbol": symbol,
            "app_name": fetched[symbol].get("app_name"),
            "attempted_apps": [
                item.get("app_name") for item in state_by_symbol[symbol]["attempts"]
            ],
            "state": "SUCCESS",
            "attempts": list(state_by_symbol[symbol]["attempts"]),
        }
        for symbol in sorted(fetched)
    ]
    audit = {
        "schema_version": v8_source.EVIDENCE_SCHEMA_VERSION,
        "kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT",
        "session_date": paths.session_date.isoformat(),
        "signal_end": signal_end,
        "signal_timestamp": signal_at.isoformat(),
        "source_contract": (
            "ONE_RANGE_REQUEST_PER_CANDIDATE_DEADLINE_AWARE_SHARED_QUEUE_"
            "CROSS_APP_RETRY_CIRCUIT_V1"
        ),
        "candidate_contract_sha256": common.canonical_json_sha256(
            [asdict(request) for request in requests]
        ),
        "app_roster": market_data.app_roster_payload(pool),
        "app_roster_sha256": market_data.app_roster_sha256(pool),
        "healthy_app_count": len(pool),
        "minimum_healthy_app_count": MIN_HEALTHY_APP_COUNT,
        "app_pool_degraded": len(pool) < len(market_data.EXPECTED_APP_NAMES),
        "app_provenance_policy": "ORDERED_UNIQUE_APPROVED_HEALTHY_SUBSET_V1",
        "candidate_count": len(requests),
        "deadline_at_ist": deadline.isoformat(),
        "deadline_aware": True,
        "retry_policy": {
            "attempts_per_symbol": attempt_limit,
            "cross_app_requeue": True,
            "circuit_breaker_failures": breaker_limit,
            "circuit_quarantine": "REMAINDER_OF_SLOT",
        },
        "app_runtime_health": [
            {
                "app_name": runtime.app_name,
                "request_count": int(app_state[runtime.app_name]["request_count"]),
                "circuit_opened": int(app_state[runtime.app_name]["circuit_opened"]),
                "quarantined": bool(app_state[runtime.app_name]["quarantined"]),
            }
            for runtime in pool
        ],
        "outcomes": outcomes,
        "symbols": dict(sorted(fetched.items())),
    }
    return fetched, audit


def _call_direct_audit_fetcher(
    fetcher: Callable[..., tuple[dict[str, dict[str, Any]], dict[str, Any]]],
    *,
    frozen_contract: Sequence[Mapping[str, Any]],
    source_paths: v8_source.SessionPaths,
    signal_end: str,
    pool: Sequence[market_data.AppRuntime],
    observed_at: datetime,
    deadline_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    kwargs: dict[str, Any] = {"observed_at": observed_at}
    try:
        signature = inspect.signature(fetcher)
    except (TypeError, ValueError):
        signature = None
    if signature is not None and (
        "deadline_at" in signature.parameters
        or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
    ):
        kwargs["deadline_at"] = deadline_at
    return fetcher(
        {"candidates": [dict(item) for item in frozen_contract]},
        source_paths,
        signal_end,
        pool,
        **kwargs,
    )


def _fetch_and_publish_raw_direct_audit(
    *,
    paths: LiveSourcePaths,
    source_paths: v8_source.SessionPaths,
    signal_end: str,
    pool: Sequence[market_data.AppRuntime],
    frozen_contract: Sequence[Mapping[str, Any]],
    frozen_metadata: Mapping[str, Any],
    started_at: datetime,
    due: datetime,
    clock: Callable[[], datetime],
    direct_audit_fetcher: Callable[..., tuple[dict[str, dict[str, Any]], dict[str, Any]]],
) -> tuple[dict[str, Any], str]:
    fetched_symbols, fetched_audit = _call_direct_audit_fetcher(
        direct_audit_fetcher,
        frozen_contract=frozen_contract,
        source_paths=source_paths,
        signal_end=signal_end,
        pool=pool,
        observed_at=started_at,
        deadline_at=due,
    )
    if dict(fetched_symbols) != fetched_audit.get("symbols"):
        raise SourceContractError(
            "direct fetch result and raw audit symbol payloads differ"
        )
    finished_at = _as_ist(clock())
    if finished_at >= due:
        raise SourceIncompleteError(
            f"direct full-universe 5x1 raw audit crossed {signal_end} S+1"
        )
    raw_payload = dict(fetched_audit)
    raw_payload.update(
        {
            "raw_schema_version": RAW_DIRECT_AUDIT_SCHEMA_VERSION,
            "multi_source_schema_version": LIVE_SOURCE_SCHEMA_VERSION,
            "kind": "RAW_DIRECT_CASH_SIGNAL_5X1M_AUDIT",
            "authority": "FROZEN_MORNING_NEAR_MONTH_MAPPED_CASH_UNIVERSE",
            "profile_bundle_fingerprint": PROFILE_BUNDLE_FINGERPRINT,
            "runtime_pool_policy_version": RUNTIME_POOL_POLICY_VERSION,
            "raw_audit_started_at_ist": started_at.isoformat(),
            "raw_audit_finished_at_ist": finished_at.isoformat(),
            "confirmation_due_ist": due.isoformat(),
            "raw_completed_before_confirmation_due": True,
            **dict(frozen_metadata),
        }
    )
    raw_payload["raw_audit_sha256"] = common.canonical_json_sha256(raw_payload)
    _validate_raw_direct_audit(
        paths=paths,
        signal_end=signal_end,
        payload=raw_payload,
        frozen_contract=frozen_contract,
        frozen_metadata=frozen_metadata,
    )
    artifact_sha = _publish_json_once(
        paths.raw_direct_audit_path(signal_end), raw_payload
    )
    return raw_payload, artifact_sha


def _start_raw_direct_audit_once(
    *,
    paths: LiveSourcePaths,
    source_paths: v8_source.SessionPaths,
    signal_end: str,
    pool: Sequence[market_data.AppRuntime],
    frozen_contract: Sequence[Mapping[str, Any]],
    frozen_metadata: Mapping[str, Any],
    started_at: datetime,
    due: datetime,
    clock: Callable[[], datetime],
    direct_audit_fetcher: Callable[..., tuple[dict[str, dict[str, Any]], dict[str, Any]]],
) -> Future[tuple[dict[str, Any], str]]:
    """Return the one in-process raw-stage future for this immutable path."""

    key = str(paths.raw_direct_audit_path(signal_end).resolve())
    with _RAW_AUDIT_FUTURES_LOCK:
        existing = _RAW_AUDIT_FUTURES.get(key)
        if existing is not None:
            return existing
        future = _RAW_AUDIT_EXECUTOR.submit(
            _fetch_and_publish_raw_direct_audit,
            paths=paths,
            source_paths=source_paths,
            signal_end=signal_end,
            pool=tuple(pool),
            frozen_contract=tuple(dict(item) for item in frozen_contract),
            frozen_metadata=dict(frozen_metadata),
            started_at=started_at,
            due=due,
            clock=clock,
            direct_audit_fetcher=direct_audit_fetcher,
        )
        _RAW_AUDIT_FUTURES[key] = future
        return future


def _validate_cross_stage_symbol_binding(
    *,
    authority: Mapping[str, Any],
    cash: Mapping[str, Any],
    proof: Mapping[str, Any],
    frozen_metadata: Mapping[str, Any],
) -> None:
    expected_tokens = {
        str(key).strip().upper(): int(value)
        for key, value in (frozen_metadata.get("cash_symbol_tokens") or {}).items()
    }
    cash_tokens: dict[str, int] = {}
    for raw in cash.get("rows") or ():
        if not isinstance(raw, Mapping):
            raise SourceContractError("strict cash source has a non-object identity")
        symbol = str(raw.get("symbol", "")).strip().upper()
        token = int(raw.get("instrument_token", 0) or 0)
        if not symbol or token <= 0 or symbol in cash_tokens:
            raise SourceContractError("strict cash source identity contract is invalid")
        cash_tokens[symbol] = token
    authority_tokens: dict[str, int] = {}
    for raw in authority.get("universe_rows") or ():
        if not isinstance(raw, Mapping):
            raise SourceContractError("candidate authority has a non-object identity")
        symbol = str(raw.get("tradingsymbol", "")).strip().upper()
        token = int(raw.get("instrument_token", 0) or 0)
        if not symbol or token <= 0 or symbol in authority_tokens:
            raise SourceContractError("candidate authority identity contract is invalid")
        authority_tokens[symbol] = token
    proof_tokens: dict[str, int] = {}
    for raw in proof.get("contracts") or ():
        if not isinstance(raw, Mapping):
            raise SourceContractError("OI proof has a non-object identity")
        symbol = str(raw.get("equity_symbol", "")).strip().upper()
        token = int(raw.get("equity_instrument_token", 0) or 0)
        if not symbol or token <= 0 or symbol in proof_tokens:
            raise SourceContractError("OI proof cash identity contract is invalid")
        proof_tokens[symbol] = token
    universe_sha = frozen_metadata.get("near_month_universe_sha256")
    checks = (
        bool(expected_tokens),
        cash_tokens == expected_tokens,
        proof_tokens == expected_tokens,
        authority_tokens == expected_tokens,
        cash.get("near_month_universe_sha256") == universe_sha,
        proof.get("near_month_universe_sha256") == universe_sha,
        cash.get("cash_symbol_set_sha256")
        == frozen_metadata.get("cash_symbol_set_sha256"),
        proof.get("cash_symbol_set_sha256")
        == frozen_metadata.get("cash_symbol_set_sha256"),
        authority.get("universe_symbol_set_sha256")
        == frozen_metadata.get("cash_symbol_set_sha256"),
    )
    if not all(checks):
        raise SourceContractError(
            "raw direct audit and finalized cash/OI/authority symbol contracts differ"
        )


def _bind_raw_direct_audit(
    *,
    raw_audit: Mapping[str, Any],
    raw_path: Path,
    raw_artifact_sha: str,
    cash: Mapping[str, Any],
    proof: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    bound = dict(raw_audit)
    bound.update(
        {
            "bound_schema_version": BOUND_DIRECT_AUDIT_SCHEMA_VERSION,
            "kind": "BOUND_DIRECT_CASH_SIGNAL_5X1M_AUDIT",
            "authority": "INDEPENDENT_ALL_MAPPED_STOCKS_NOT_V6",
            "raw_audit_artifact": {
                "path": str(raw_path),
                "sha256": raw_artifact_sha,
            },
            "raw_audit_sha256": raw_audit.get("raw_audit_sha256"),
            "strict_cash_source_sha256": cash.get("strict_cash_source_sha256"),
            "oi_proof_sha256": proof.get("proof_sha256"),
            "candidate_source_sha256": authority.get("candidate_source_sha256"),
            "exact_symbol_contract_bound": True,
            "decision_before_confirmation_due": True,
        }
    )
    bound.pop("audit_payload_sha256", None)
    bound["audit_payload_sha256"] = common.canonical_json_sha256(bound)
    return bound


def _build_metric_rows(
    *,
    paths: LiveSourcePaths,
    signal_end: str,
    authority: Mapping[str, Any],
    direct_audit: Mapping[str, Any],
    artifact_bindings: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Project every mapped stock to both sides without applying any gate."""

    universe_rows = authority.get("universe_rows")
    direct_symbols = direct_audit.get("symbols")
    if not isinstance(universe_rows, list) or not universe_rows:
        raise SourceIncompleteError("all-mapped-universe metric rows are unavailable")
    if not isinstance(direct_symbols, Mapping):
        raise SourceIncompleteError("direct 5x1 cash audit symbols are unavailable")

    signal_time = _signal_at(paths.session_date, signal_end)
    projected: list[dict[str, Any]] = []
    symbol_tokens: dict[str, int] = {}
    seen_futures: set[int] = set()
    for raw in universe_rows:
        if not isinstance(raw, Mapping):
            raise SourceIncompleteError("all-universe source contains a non-object row")
        symbol = str(raw.get("tradingsymbol", "")).strip().upper()
        token = int(raw.get("instrument_token", 0) or 0)
        futures_symbol = str(raw.get("futures_tradingsymbol", "")).strip().upper()
        futures_token = int(raw.get("futures_instrument_token", 0) or 0)
        tick_size = float(raw.get("tick_size", 0) or 0)
        five = raw.get("_cash_features")
        oi_pair = raw.get("_oi_pair")
        direct = direct_symbols.get(symbol)
        if (
            not symbol
            or token <= 0
            or not futures_symbol
            or futures_token <= 0
            or tick_size <= 0
            or not isinstance(five, Mapping)
            or not isinstance(oi_pair, Mapping)
            or not isinstance(direct, Mapping)
        ):
            raise SourceIncompleteError(f"all-universe metric lineage is incomplete: {symbol}")
        prior = symbol_tokens.setdefault(symbol, token)
        if prior != token or futures_token in seen_futures:
            raise SourceIncompleteError("all-universe symbol/token mapping is not one-to-one")
        seen_futures.add(futures_token)

        numeric_names = (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "price_change_pct",
            "volume_ratio",
            "traded_value",
            "ema9",
            "ema20",
            "ema50",
        )
        values: dict[str, float] = {}
        for name in numeric_names:
            value = float(five.get(name))
            if not math.isfinite(value):
                raise SourceIncompleteError(f"non-finite {name} for {symbol}")
            values[name] = value
        for name in ("open", "high", "low", "close", "volume"):
            direct_value = float(direct.get(name))
            if not math.isclose(values[name], direct_value, rel_tol=0.0, abs_tol=1e-6):
                raise SourceIncompleteError(
                    f"stored cash/direct exact 5x1 {name} mismatch for {symbol}"
                )
        oi = float(oi_pair.get("oi"))
        prev_oi = float(oi_pair.get("prev_oi"))
        oi_change_pct = float(oi_pair.get("oi_change_pct"))
        if not all(math.isfinite(value) for value in (oi, prev_oi, oi_change_pct)):
            raise SourceIncompleteError(f"non-finite exact OI metrics for {symbol}")

        common_row = {
            "schema_version": FIVE_MINUTE_ROW_SCHEMA_VERSION,
            "session_date": paths.session_date.isoformat(),
            "signal_time": signal_time.isoformat(),
            "signal_timestamp": signal_time.isoformat(),
            "signal_end": signal_end,
            "symbol": symbol,
            "tradingsymbol": symbol,
            "instrument_token": token,
            "equity_instrument_token": token,
            "futures_symbol": futures_symbol,
            "futures_tradingsymbol": futures_symbol,
            "futures_instrument_token": futures_token,
            "tick_size": tick_size,
            "lot_size": 1,
            "five_min_open": values["open"],
            "five_min_high": values["high"],
            "five_min_low": values["low"],
            "five_min_close": values["close"],
            "five_min_volume": values["volume"],
            "signal_open": values["open"],
            "signal_high": values["high"],
            "signal_low": values["low"],
            "signal_close": values["close"],
            "price_change_pct": values["price_change_pct"],
            "volume_ratio": values["volume_ratio"],
            "traded_value": values["traded_value"],
            "ema9": values["ema9"],
            "ema20": values["ema20"],
            "ema50": values["ema50"],
            "oi": oi,
            "prev_oi": prev_oi,
            "oi_change_pct": oi_change_pct,
            "cash_constituents_sha256": direct.get("constituents_sha256"),
            "cash_constituents": direct.get("constituents"),
            "cash_feature_source_sha256": five.get("source_file_sha256"),
            "cash_causal_prefix_sha256": five.get("causal_prefix_sha256"),
            "futures_oi_source_sha256": oi_pair.get("source_file_sha256"),
            "futures_oi_rows_sha256": oi_pair.get("rows_sha256"),
            "source_policy_version": v8_source.SOURCE_POLICY_VERSION,
            "profile_bundle_fingerprint": PROFILE_BUNDLE_FINGERPRINT,
            **dict(artifact_bindings),
        }
        for side in ("LONG", "SHORT"):
            setup_id = f"{signal_end}_{side}"
            projected.append(
                {
                    **common_row,
                    "candidate_source_id": (
                        f"{paths.session_date.isoformat()}|{setup_id}|{symbol}"
                    ),
                    "setup_id": setup_id,
                    "side": side,
                }
            )

    projected.sort(key=lambda row: (str(row["setup_id"]), str(row["symbol"])))
    expected_count = 2 * len(symbol_tokens)
    if len(projected) != expected_count:
        raise SourceIncompleteError("all-universe side projection is incomplete")
    return projected, dict(sorted(symbol_tokens.items()))


def _validate_direct_audit(
    *,
    paths: LiveSourcePaths,
    signal_end: str,
    payload: Mapping[str, Any],
    authority: Mapping[str, Any],
    cash: Mapping[str, Any],
    proof: Mapping[str, Any],
    raw_audit: Mapping[str, Any],
    raw_path: Path,
    raw_artifact_sha: str,
    frozen_metadata: Mapping[str, Any],
) -> None:
    unsigned = dict(payload)
    claimed = str(unsigned.pop("audit_payload_sha256", ""))
    symbols = payload.get("symbols")
    universe_rows = authority.get("universe_rows")
    expected_symbols = sorted(
        str(row.get("tradingsymbol", "")).strip().upper()
        for row in (universe_rows if isinstance(universe_rows, list) else [])
        if isinstance(row, Mapping)
    )
    observed_symbols = sorted(str(value).strip().upper() for value in symbols) if isinstance(symbols, Mapping) else []
    recorded_roster = payload.get("app_roster")
    raw_binding = payload.get("raw_audit_artifact")
    checks = (
        payload.get("bound_schema_version") == BOUND_DIRECT_AUDIT_SCHEMA_VERSION,
        payload.get("multi_source_schema_version") == LIVE_SOURCE_SCHEMA_VERSION,
        payload.get("kind") == "BOUND_DIRECT_CASH_SIGNAL_5X1M_AUDIT",
        payload.get("authority") == "INDEPENDENT_ALL_MAPPED_STOCKS_NOT_V6",
        payload.get("session_date") == paths.session_date.isoformat(),
        payload.get("signal_end") == signal_end,
        payload.get("profile_bundle_fingerprint") == PROFILE_BUNDLE_FINGERPRINT,
        payload.get("runtime_pool_policy_version") == RUNTIME_POOL_POLICY_VERSION,
        _recorded_roster_is_approved(
            recorded_roster, minimum=MIN_HEALTHY_APP_COUNT
        ),
        payload.get("app_roster_sha256")
        == common.canonical_json_sha256(recorded_roster),
        int(payload.get("healthy_app_count", -1)) == len(recorded_roster or ()),
        payload.get("strict_cash_source_sha256") == cash.get("strict_cash_source_sha256"),
        payload.get("oi_proof_sha256") == proof.get("proof_sha256"),
        payload.get("candidate_source_sha256") == authority.get("candidate_source_sha256"),
        isinstance(raw_binding, Mapping),
        Path(str((raw_binding or {}).get("path", ""))) == raw_path,
        (raw_binding or {}).get("sha256") == raw_artifact_sha,
        payload.get("raw_audit_sha256") == raw_audit.get("raw_audit_sha256"),
        payload.get("symbols") == raw_audit.get("symbols"),
        payload.get("outcomes") == raw_audit.get("outcomes"),
        payload.get("candidate_contract_sha256")
        == raw_audit.get("candidate_contract_sha256"),
        payload.get("cash_symbol_tokens") == raw_audit.get("cash_symbol_tokens"),
        payload.get("near_month_universe_sha256")
        == raw_audit.get("near_month_universe_sha256"),
        payload.get("cash_symbol_contract_sha256")
        == frozen_metadata.get("cash_symbol_contract_sha256"),
        payload.get("mapped_stock_cash_contract_sha256")
        == frozen_metadata.get("mapped_stock_cash_contract_sha256"),
        payload.get("exact_symbol_contract_bound") is True,
        isinstance(symbols, Mapping),
        len(expected_symbols) == len(set(expected_symbols)) > 0,
        observed_symbols == expected_symbols,
        int(payload.get("candidate_count", -1)) == len(expected_symbols),
        payload.get("decision_before_confirmation_due") is True,
        claimed == common.canonical_json_sha256(unsigned),
    )
    if not all(checks):
        raise SourceContractError("full-universe direct 5x1 audit binding mismatch")


def _validate_manifest(
    paths: LiveSourcePaths,
    signal_end: str,
    payload: Mapping[str, Any],
) -> None:
    unsigned = dict(payload)
    claimed = str(unsigned.pop("manifest_sha256", ""))
    rows = payload.get("rows")
    tokens = payload.get("symbol_tokens")
    checks = (
        payload.get("schema_version") == FIVE_MINUTE_MANIFEST_SCHEMA_VERSION,
        payload.get("session_date") == paths.session_date.isoformat(),
        payload.get("signal_end") == signal_end,
        payload.get("authority") == "INDEPENDENT_ALL_MAPPED_STOCKS_NOT_V6",
        payload.get("profile_bundle_fingerprint") == PROFILE_BUNDLE_FINGERPRINT,
        payload.get("profile_binding") == PROFILE_BINDING_PAYLOAD,
        payload.get("runtime_pool_policy_version") == RUNTIME_POOL_POLICY_VERSION,
        payload.get("raw_direct_audit_sha256") is not None,
        payload.get("cash_symbol_contract_sha256") is not None,
        _recorded_roster_is_approved(
            payload.get("app_roster"), minimum=MIN_HEALTHY_APP_COUNT
        ),
        payload.get("app_roster_sha256")
        == common.canonical_json_sha256(payload.get("app_roster")),
        isinstance(rows, list),
        isinstance(tokens, Mapping),
        int(payload.get("universe_count", -1)) == len(tokens) > 0,
        int(payload.get("row_count", -1)) == len(rows) == 2 * len(tokens),
        claimed == common.canonical_json_sha256(unsigned),
    )
    if not all(checks):
        raise SourceContractError("multi-paper five-minute manifest binding mismatch")
    for name in (
        "strict_cash_artifact",
        "oi_proof_artifact",
        "candidate_authority_artifact",
        "raw_direct_audit_artifact",
        "direct_audit_artifact",
        "near_month_universe_artifact",
    ):
        binding = payload.get(name)
        if not isinstance(binding, Mapping):
            raise SourceContractError(f"five-minute manifest lacks {name}")
        artifact_path = Path(str(binding.get("path", "")))
        if not artifact_path.is_file() or _sha256_file(artifact_path) != binding.get("sha256"):
            raise SourceContractError(f"five-minute dependency changed: {name}")


def build_and_publish_five_minute_source(
    paths: LiveSourcePaths,
    signal_end: str,
    runtimes: Sequence[market_data.AppRuntime],
    *,
    observed_at: datetime | None = None,
    clock: Callable[[], datetime] = v8_source.now_ist,
    require_prospective: bool = True,
    strict_cash_loader: Callable[..., Mapping[str, Any]] = v8_source.precompute_strict_cash_universe_source,
    oi_proof_loader: Callable[..., Mapping[str, Any]] = v8_source.prove_v6_oi_shift_is_exact_for_stock_universe,
    authority_loader: Callable[..., Mapping[str, Any]] = v8_source.precompute_independent_v8_candidate_source,
    direct_audit_fetcher: Callable[..., tuple[dict[str, dict[str, Any]], dict[str, Any]]] = _fetch_exact_cash_signal_constituents_resilient,
) -> FiveMinuteSourceResult:
    """Seal and return a gate-free metric superset for one five-minute slot."""

    pool = _require_runtime_pool(runtimes)
    signal_at = _signal_at(paths.session_date, signal_end)
    due = signal_at + timedelta(minutes=1, seconds=market_data.DEFAULT_BOUNDARY_BUFFER_SEC)
    initial_now = _as_ist(observed_at if observed_at is not None else clock())
    if initial_now.date() != paths.session_date:
        raise SourceContractError("five-minute source clock crossed its session date")
    manifest_path = paths.five_minute_slot_root(signal_end) / "manifest.json"
    if manifest_path.is_file():
        manifest, artifact_sha = _read_json_artifact(manifest_path)
        _validate_manifest(paths, signal_end, manifest)
        if require_prospective and initial_now >= due:
            raise SourceIncompleteError(
                f"no-retro-entry gate: {signal_end} source requested after S+1"
            )
        return FiveMinuteSourceResult(
            signal_end=signal_end,
            rows=tuple(dict(row) for row in manifest["rows"]),
            symbol_tokens={str(k): int(v) for k, v in manifest["symbol_tokens"].items()},
            manifest=manifest,
            manifest_path=manifest_path,
            manifest_artifact_sha256=artifact_sha,
            reused=True,
        )
    if require_prospective and initial_now >= due:
        raise SourceIncompleteError(
            f"no-retro-entry gate: {signal_end} source began after S+1"
        )

    source_paths = paths.as_v8_paths()
    frozen_contract, frozen_metadata = _load_frozen_cash_audit_contract(paths)
    raw_path = paths.raw_direct_audit_path(signal_end)
    raw_future: Future[tuple[dict[str, Any], str]] | None = None
    if raw_path.is_file():
        raw_audit, raw_artifact_sha = _read_json_artifact(raw_path)
        _validate_raw_direct_audit(
            paths=paths,
            signal_end=signal_end,
            payload=raw_audit,
            frozen_contract=frozen_contract,
            frozen_metadata=frozen_metadata,
        )
    else:
        raw_future = _start_raw_direct_audit_once(
            paths=paths,
            source_paths=source_paths,
            signal_end=signal_end,
            pool=pool,
            frozen_contract=frozen_contract,
            frozen_metadata=frozen_metadata,
            started_at=initial_now,
            due=due,
            clock=clock,
            direct_audit_fetcher=direct_audit_fetcher,
        )

    loaded_cash = v8_source.load_immutable_strict_cash_universe_source(source_paths, signal_end)
    if loaded_cash is None:
        started = _as_ist(clock())
        cash = dict(
            strict_cash_loader(source_paths, signal_end, observed_at=started)
        )
        finished = _as_ist(clock())
        cash = _with_timing_and_hash(
            cash,
            start=started,
            finish=finished,
            due=due,
            prefix="source",
            hash_field="strict_cash_source_sha256",
        )
        v8_source._validate_strict_cash_universe_source(source_paths, signal_end, cash)
        cash_path = source_paths.strict_cash_source_root / f"slot_{signal_end.replace(':', '')}.json"
        cash_artifact_sha = _publish_json_once(cash_path, cash)
    else:
        cash, cash_artifact_sha = loaded_cash
        cash_path = source_paths.strict_cash_source_root / f"slot_{signal_end.replace(':', '')}.json"

    loaded_proof = v8_source.load_immutable_universe_oi_proof(source_paths, signal_end)
    if loaded_proof is None:
        started = _as_ist(clock())
        proof = dict(oi_proof_loader(source_paths, signal_end, observed_at=started))
        finished = _as_ist(clock())
        proof = _with_timing_and_hash(
            proof,
            start=started,
            finish=finished,
            due=due,
            prefix="proof",
            hash_field="proof_sha256",
        )
        v8_source._validate_universe_oi_proof_payload(source_paths, signal_end, proof)
        proof_path = source_paths.oi_superset_audit_root / f"slot_{signal_end.replace(':', '')}.json"
        proof_artifact_sha = _publish_json_once(proof_path, proof)
    else:
        proof, proof_artifact_sha = loaded_proof
        proof_path = source_paths.oi_superset_audit_root / f"slot_{signal_end.replace(':', '')}.json"

    loaded_authority = v8_source.load_immutable_independent_candidate_source(
        source_paths,
        signal_end,
        universe_proof_sha256=str(proof["proof_sha256"]),
        strict_cash_source_sha256=str(cash["strict_cash_source_sha256"]),
    )
    if loaded_authority is None:
        started = _as_ist(clock())
        authority = dict(
            authority_loader(
                source_paths,
                signal_end,
                proof,
                observed_at=started,
                strict_cash_source=cash,
            )
        )
        finished = _as_ist(clock())
        authority = _with_timing_and_hash(
            authority,
            start=started,
            finish=finished,
            due=due,
            prefix="source",
            hash_field="candidate_source_sha256",
        )
        v8_source._validate_independent_candidate_source(
            source_paths,
            signal_end,
            authority,
            universe_proof_sha256=str(proof["proof_sha256"]),
            strict_cash_source_sha256=str(cash["strict_cash_source_sha256"]),
        )
        authority_path = source_paths.independent_candidate_source_root / f"slot_{signal_end.replace(':', '')}.json"
        authority_artifact_sha = _publish_json_once(authority_path, authority)
    else:
        authority, authority_artifact_sha = loaded_authority
        authority_path = source_paths.independent_candidate_source_root / f"slot_{signal_end.replace(':', '')}.json"

    if raw_future is not None:
        remaining = max(0.0, (due - _as_ist(clock())).total_seconds())
        try:
            raw_audit, raw_artifact_sha = raw_future.result(timeout=remaining)
        except TimeoutError as exc:
            raise SourceIncompleteError(
                f"direct full-universe 5x1 raw audit did not finish before {signal_end} S+1"
            ) from exc
    _validate_raw_direct_audit(
        paths=paths,
        signal_end=signal_end,
        payload=raw_audit,
        frozen_contract=frozen_contract,
        frozen_metadata=frozen_metadata,
    )
    _validate_cross_stage_symbol_binding(
        authority=authority,
        cash=cash,
        proof=proof,
        frozen_metadata=frozen_metadata,
    )
    direct_path = paths.five_minute_slot_root(signal_end) / "direct_5x1_audit.json"
    if direct_path.is_file():
        direct_audit, direct_artifact_sha = _read_json_artifact(direct_path)
    else:
        direct_audit = _bind_raw_direct_audit(
            raw_audit=raw_audit,
            raw_path=raw_path,
            raw_artifact_sha=raw_artifact_sha,
            cash=cash,
            proof=proof,
            authority=authority,
        )
        direct_artifact_sha = _publish_json_once(direct_path, direct_audit)

    _validate_direct_audit(
        paths=paths,
        signal_end=signal_end,
        payload=direct_audit,
        authority=authority,
        cash=cash,
        proof=proof,
        raw_audit=raw_audit,
        raw_path=raw_path,
        raw_artifact_sha=raw_artifact_sha,
        frozen_metadata=frozen_metadata,
    )

    decision_at = _as_ist(clock())
    if require_prospective and decision_at >= due:
        raise SourceIncompleteError(
            f"no-retro-entry gate: {signal_end} evidence crossed S+1"
        )
    bindings = {
        "strict_cash_source_sha256": cash["strict_cash_source_sha256"],
        "oi_proof_sha256": proof["proof_sha256"],
        "candidate_authority_sha256": authority["candidate_source_sha256"],
        "direct_audit_sha256": direct_audit.get("audit_payload_sha256"),
    }
    rows, symbol_tokens = _build_metric_rows(
        paths=paths,
        signal_end=signal_end,
        authority=authority,
        direct_audit=direct_audit,
        artifact_bindings=bindings,
    )
    manifest: dict[str, Any] = {
        "schema_version": FIVE_MINUTE_MANIFEST_SCHEMA_VERSION,
        "session_date": paths.session_date.isoformat(),
        "signal_end": signal_end,
        "signal_timestamp": signal_at.isoformat(),
        "confirmation_due_ist": due.isoformat(),
        "decision_at_ist": decision_at.isoformat(),
        "decision_before_confirmation_due": decision_at < due,
        "authority": "INDEPENDENT_ALL_MAPPED_STOCKS_NOT_V6",
        "source_policy_version": v8_source.SOURCE_POLICY_VERSION,
        "runtime_pool_policy_version": RUNTIME_POOL_POLICY_VERSION,
        "profile_binding": PROFILE_BINDING_PAYLOAD,
        "profile_bundle_fingerprint": PROFILE_BUNDLE_FINGERPRINT,
        "raw_direct_audit_sha256": raw_audit.get("raw_audit_sha256"),
        "cash_symbol_contract_sha256": frozen_metadata.get(
            "cash_symbol_contract_sha256"
        ),
        "mapped_stock_cash_contract_sha256": frozen_metadata.get(
            "mapped_stock_cash_contract_sha256"
        ),
        "app_roster": market_data.app_roster_payload(pool),
        "app_roster_sha256": market_data.app_roster_sha256(pool),
        "app_authentication": _app_authentication_payload(pool),
        "healthy_app_count": len(pool),
        "minimum_healthy_app_count": MIN_HEALTHY_APP_COUNT,
        "app_pool_degraded": len(pool) < len(market_data.EXPECTED_APP_NAMES),
        "universe_count": len(symbol_tokens),
        "row_count": len(rows),
        "side_projection": "EVERY_MAPPED_STOCK_PROJECTED_TO_LONG_AND_SHORT",
        "strategy_prefilter_applied": False,
        "symbol_tokens": symbol_tokens,
        "rows": rows,
        "strict_cash_artifact": {"path": str(cash_path), "sha256": cash_artifact_sha},
        "oi_proof_artifact": {"path": str(proof_path), "sha256": proof_artifact_sha},
        "candidate_authority_artifact": {"path": str(authority_path), "sha256": authority_artifact_sha},
        "raw_direct_audit_artifact": {"path": str(raw_path), "sha256": raw_artifact_sha},
        "direct_audit_artifact": {"path": str(direct_path), "sha256": direct_artifact_sha},
        "near_month_universe_artifact": {
            "path": str(paths.near_month_universe_path),
            "sha256": frozen_metadata.get("near_month_universe_sha256"),
        },
    }
    manifest["manifest_sha256"] = common.canonical_json_sha256(manifest)
    _validate_manifest(paths, signal_end, manifest)
    manifest_artifact_sha = _publish_json_once(manifest_path, manifest)
    return FiveMinuteSourceResult(
        signal_end=signal_end,
        rows=tuple(rows),
        symbol_tokens=symbol_tokens,
        manifest=manifest,
        manifest_path=manifest_path,
        manifest_artifact_sha256=manifest_artifact_sha,
        reused=False,
    )


def _normalize_requests(
    candidates_or_symbol_tokens: Mapping[str, int] | Sequence[market_data.CandidateRequest | Mapping[str, Any]],
) -> tuple[market_data.CandidateRequest, ...]:
    if isinstance(candidates_or_symbol_tokens, Mapping):
        raw_values: Sequence[market_data.CandidateRequest | Mapping[str, Any]] = [
            {"symbol": symbol, "instrument_token": token}
            for symbol, token in candidates_or_symbol_tokens.items()
        ]
    else:
        raw_values = candidates_or_symbol_tokens
    by_symbol: dict[str, market_data.CandidateRequest] = {}
    token_to_symbol: dict[int, str] = {}
    for raw in raw_values:
        request = raw if isinstance(raw, market_data.CandidateRequest) else market_data.CandidateRequest.from_mapping(raw)
        prior = by_symbol.setdefault(request.symbol, request)
        if prior.instrument_token != request.instrument_token:
            raise ValueError(f"cash token changed in union for {request.symbol}")
        other = token_to_symbol.setdefault(request.instrument_token, request.symbol)
        if other != request.symbol:
            raise ValueError("one cash token maps to multiple union symbols")
    return tuple(sorted(by_symbol.values(), key=lambda item: (item.symbol, item.instrument_token)))


def _validate_union_marker(
    frame: pd.DataFrame,
    marker: Mapping[str, Any],
    requests: Sequence[market_data.CandidateRequest],
    runtimes: Sequence[market_data.AppRuntime],
    expected_end: datetime,
) -> None:
    if (
        marker.get("multi_source_schema_version") != UNION_MINUTE_SCHEMA_VERSION
        or marker.get("profile_bundle_fingerprint") != PROFILE_BUNDLE_FINGERPRINT
        or marker.get("profile_binding") != PROFILE_BINDING_PAYLOAD
        or marker.get("runtime_pool_policy_version")
        != RUNTIME_POOL_POLICY_VERSION
        or not _recorded_roster_is_approved(
            marker.get("app_roster"), minimum=MIN_HEALTHY_APP_COUNT
        )
    ):
        raise SourceContractError("union minute profile/source binding mismatch")
    v8_source._validate_minute_evidence_contract(
        frame, marker, requests, runtimes, expected_end
    )


def fetch_and_publish_union_minute(
    paths: LiveSourcePaths,
    candidates_or_symbol_tokens: Mapping[str, int] | Sequence[market_data.CandidateRequest | Mapping[str, Any]],
    runtimes: Sequence[market_data.AppRuntime],
    expected_end: datetime | pd.Timestamp,
    *,
    observed_at: datetime | pd.Timestamp | None = None,
    minute_fetcher: Callable[..., tuple[pd.DataFrame, dict[str, Any]]] = market_data.fetch_completed_minute,
) -> UnionMinuteSnapshot:
    """Fetch every required cash symbol once and immutably publish the union."""

    pool = _require_runtime_pool(runtimes)
    requests = _normalize_requests(candidates_or_symbol_tokens)
    end = _as_ist(expected_end)
    marker_path = paths.minute_root / f"minute_{end.strftime('%H%M')}.json"
    reused = marker_path.is_file()
    if reused:
        frame, marker = market_data.load_validated_minute_snapshot(
            marker_path,
            strategy_fingerprint=PROFILE_BUNDLE_FINGERPRINT,
        )
    else:
        fetch_kwargs: dict[str, Any] = {
            "now": observed_at if observed_at is not None else common.now_ist(),
        }
        signature = inspect.signature(minute_fetcher)
        if "deadline_at" in signature.parameters or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        ):
            fetch_kwargs["deadline_at"] = end + timedelta(
                minutes=1, seconds=market_data.DEFAULT_BOUNDARY_BUFFER_SEC
            )
        frame, fetched_marker = minute_fetcher(
            requests,
            pool,
            end,
            **fetch_kwargs,
        )
        bound_marker = {
            **dict(fetched_marker),
            "multi_source_schema_version": UNION_MINUTE_SCHEMA_VERSION,
            "runtime_pool_policy_version": RUNTIME_POOL_POLICY_VERSION,
            "profile_binding": PROFILE_BINDING_PAYLOAD,
            "profile_bundle_fingerprint": PROFILE_BUNDLE_FINGERPRINT,
            "union_symbol_count": len(requests),
            "union_fetch_once": True,
        }
        marker = market_data.publish_minute_snapshot_once(
            paths.minute_root,
            frame,
            bound_marker,
            strategy_fingerprint=PROFILE_BUNDLE_FINGERPRINT,
        )
    _validate_union_marker(frame, marker, requests, pool, end)
    bars: dict[str, dict[str, Any]] = {}
    for raw in frame.to_dict("records"):
        symbol = str(raw.get("symbol", "")).strip().upper()
        bars[symbol] = {
            "timestamp": raw.get("timestamp"),
            "open": float(raw.get("open")),
            "high": float(raw.get("high")),
            "low": float(raw.get("low")),
            "close": float(raw.get("close")),
            "volume": float(raw.get("volume", 0)),
            "gap_filled": bool(raw.get("gap_filled", False)),
            "opening_snapshot": bool(raw.get("opening_snapshot", False)),
            "provisional_stale": bool(raw.get("provisional_stale", False)),
        }
    return UnionMinuteSnapshot(
        expected_end=pd.Timestamp(end),
        frame=frame,
        bars_by_symbol=bars,
        marker=marker,
        marker_path=marker_path,
        reused=reused,
    )


__all__ = [
    "FIVE_MINUTE_MANIFEST_SCHEMA_VERSION",
    "FIVE_MINUTE_ROW_SCHEMA_VERSION",
    "FiveMinuteSourceResult",
    "LIVE_SOURCE_SCHEMA_VERSION",
    "LiveSourcePaths",
    "PROFILE_BINDING_PAYLOAD",
    "PROFILE_BUNDLE_FINGERPRINT",
    "SIGNAL_ENDS",
    "SourceContractError",
    "SourceIncompleteError",
    "SourceNotReadyError",
    "UNION_MINUTE_SCHEMA_VERSION",
    "UnionMinuteSnapshot",
    "authenticate_all_apps",
    "build_and_publish_five_minute_source",
    "fetch_and_publish_union_minute",
]
