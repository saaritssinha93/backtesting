"""Isolated fast-shadow fetch and parity check for FnO 5-minute futures OI.

The shadow deliberately never appends to the production per-contract archive.
It validates every mapped stock future for the six strategy-relevant morning
slots and a deterministic rotating stock canary afterward.  Each independent
Kite call includes the exact S and S-5 OI rows needed by the strategies.  Full
OHLCV parity remains a diagnostic, but only exact strategy-OI parity determines
the v2 shadow result.
"""

from __future__ import annotations

import argparse
import math
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

import fno_oi_common as common


SESSION = "fno_oi_fetch_5min_fast_shadow"
SHADOW_SCHEMA_VERSION = "fno_oi_fast_shadow_slot_v2"
OI_EVIDENCE_SCHEMA_VERSION = "fno_oi_fast_shadow_oi_evidence_v1"
SCOPE_POLICY_VERSION = "strategy_full_6_then_rotating_canary_v1"
DEFAULT_OUTPUT_ROOT = common.FNO_ROOT / "shadow_fast_fetch"
PRODUCTION_SESSION = "fno_oi_fetch_5min"
FIRST_SLOT = dtime(9, 20)
LAST_SLOT = dtime(15, 30)
FIRST_CANARY_SLOT = dtime(9, 50)
DEFAULT_CANARY_COUNT = 20
DEFAULT_OI_PAIR_LOOKBACK_MINUTES = 10
DEFAULT_STRATEGY_JOB_FENCE_SECONDS = 65.0
SCOPE_STRATEGY_FULL = "STRATEGY_FULL_STOCKS"
SCOPE_ROTATING_CANARY = "ROTATING_STOCK_CANARY"
STRATEGY_FULL_SLOT_TIMES = tuple(
    dtime(9, minute) for minute in (20, 25, 30, 35, 40, 45)
)
COMPARE_FIELDS = (
    "timestamp",
    "candle_start",
    "instrument_token",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "quality_state",
)
OI_IDENTITY_FIELDS = (
    "timestamp",
    "candle_start",
    "underlying",
    "instrument_token",
    "oi",
    "quality_state",
    "source",
    "data_version",
)
OI_EVIDENCE_COLUMNS = (
    "timestamp",
    "candle_start",
    "previous_timestamp",
    "previous_candle_start",
    "underlying",
    "tradingsymbol",
    "instrument_token",
    "oi",
    "prev_oi",
    "oi_change_pct",
    "oi_pair_state",
    "quality_state",
    "fetch_timestamp",
    "source",
    "data_version",
)
OI_EVIDENCE_COMPARE_FIELDS = tuple(
    column for column in OI_EVIDENCE_COLUMNS if column != "fetch_timestamp"
)
OI_PAIR_VALID_STATES = {"VALID", "BASELINE_VALID"}


@dataclass(frozen=True)
class ShadowScope:
    universe: pd.DataFrame
    mode: str
    strategy_slot: bool
    full_universe_contracts: int
    full_stock_contracts: int
    selected_contracts: int
    full_universe_sha256: str
    full_stock_universe_sha256: str
    full_stock_symbol_set_sha256: str
    selected_universe_sha256: str
    selected_symbol_set_sha256: str
    selected_symbols: tuple[str, ...]
    canary_count: int
    rotation_ordinal: int | None
    rotation_offset: int | None


@dataclass
class AppLane:
    app_name: str
    clients: list[Any]
    pace_seconds: float
    _last_call_at: float = 0.0
    _pace_lock: threading.Lock = field(default_factory=threading.Lock)
    _client_lock: threading.Lock = field(default_factory=threading.Lock)
    _next_client_index: int = 0
    _runtime_auth_failure: threading.Event = field(default_factory=threading.Event)

    def pace(self) -> None:
        """Rate-limit request starts across every worker for this API app."""
        with self._pace_lock:
            wait = self.pace_seconds - (time.monotonic() - self._last_call_at)
            if wait > 0:
                time.sleep(wait)
            self._last_call_at = time.monotonic()

    def next_client(self) -> Any:
        with self._client_lock:
            client = self.clients[self._next_client_index % len(self.clients)]
            self._next_client_index += 1
            return client


def _auth_failure_text(value: object) -> bool:
    text = str(value).strip().lower()
    return any(
        marker in text
        for marker in (
            "tokenexception",
            "incorrect `api_key` or `access_token`",
            "incorrect api_key or access_token",
            "invalid api_key",
            "invalid access_token",
            "session expired",
            "session invalid",
            "http 403",
            "status 403",
            "status_code=403",
        )
    )


def _credential_pool_signature(
    args: argparse.Namespace,
    credentials: Iterable[common.KiteCredential],
) -> str:
    payload = {
        "credentials": [
            {
                "app_name": item.app_name,
                "api_key": item.api_key,
                "access_token": item.access_token,
            }
            for item in credentials
        ],
        "max_apps": int(args.max_apps),
        "workers_per_app": max(1, int(args.workers_per_app)),
        "timeout_sec": float(args.timeout_sec),
        "pace_seconds": max(0.34, float(args.request_interval_sec)),
    }
    return common.canonical_json_sha256(payload)


@dataclass
class AppLaneSession:
    """Reuse authenticated clients across continuous slots without caching data."""

    _signature: str = ""
    _lanes: list[AppLane] = field(default_factory=list)
    _failures: list[str] = field(default_factory=list)
    _refresh_next: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def acquire(self, args: argparse.Namespace) -> tuple[list[AppLane], list[str], bool]:
        credentials = common.discover_kite_credentials(max_apps=args.max_apps)
        signature = _credential_pool_signature(args, credentials)
        with self._lock:
            reusable = bool(
                self._lanes
                and self._signature == signature
                and not self._refresh_next
                and not any(lane._runtime_auth_failure.is_set() for lane in self._lanes)
            )
            if reusable:
                return self._lanes, list(self._failures), True

            lanes, failures = build_app_lanes(args, credentials=credentials)
            self._signature = signature
            self._lanes = lanes
            self._failures = list(failures)
            # A profile timeout/network failure may recover without a credential
            # file change. A definite invalid token is cached until that file changes.
            self._refresh_next = any(
                not _auth_failure_text(failure) for failure in failures
            )
            return self._lanes, list(self._failures), False

    def invalidate_runtime_auth_failures(self) -> bool:
        with self._lock:
            failed = any(
                lane._runtime_auth_failure.is_set() for lane in self._lanes
            )
            if failed:
                self._refresh_next = True
            return failed


def _coerce_slot(value: str) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(common.IST)
    else:
        stamp = stamp.tz_convert(common.IST)
    return stamp.to_pydatetime()


def _read_kv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    payload: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return {}
    for line in lines:
        key, separator, value = line.partition("=")
        if separator and key.strip():
            payload[key.strip()] = value.strip()
    return payload


def _production_marker(slot_end: datetime) -> dict[str, Any]:
    path = common.fetch_slot_path(slot_end)
    if not path.exists():
        raise FileNotFoundError(f"Production fetch marker is missing: {path}")
    marker = common.read_json(path)
    if str(marker.get("source", "")).lower() != "final":
        raise ValueError(f"Production marker is not final: {path}")
    return marker


def _latest_complete_production_slot() -> datetime:
    paths = sorted(common.FETCH_SLOT_DIR.glob("slot_*.json"), reverse=True)
    for path in paths:
        try:
            marker = common.read_json(path)
            if str(marker.get("source", "")).lower() != "final":
                continue
            if not bool(marker.get("complete")):
                continue
            return _coerce_slot(str(marker["slot_ist"]))
        except (KeyError, OSError, TypeError, ValueError):
            continue
    raise RuntimeError("No complete production FnO fetch marker is available.")


def complete_production_slots(session_date: date) -> list[datetime]:
    """Return final, complete production slots for one trading session."""
    slots: set[datetime] = set()
    pattern = f"slot_{session_date.strftime('%Y%m%d')}_*.json"
    for path in common.FETCH_SLOT_DIR.glob(pattern):
        try:
            marker = common.read_json(path)
            if str(marker.get("source", "")).lower() != "final":
                continue
            if not bool(marker.get("complete")):
                continue
            slot = _coerce_slot(str(marker["slot_ist"]))
        except (KeyError, OSError, TypeError, ValueError):
            continue
        if slot.date() != session_date or not FIRST_SLOT <= slot.time() <= LAST_SLOT:
            continue
        slots.add(slot)
    return sorted(slots)


def shadow_slot_history(
    output_root: Path,
    session_date: date,
) -> tuple[set[datetime], set[datetime]]:
    """Return all observed and quality-passing shadow slots for restart recovery."""
    observed: set[datetime] = set()
    successful: set[datetime] = set()
    day_root = output_root / session_date.isoformat()
    for path in day_root.glob("*/shadow_marker.json"):
        try:
            marker = common.read_json(path)
            slot = _coerce_slot(str(marker["slot_ist"]))
        except (KeyError, OSError, TypeError, ValueError):
            continue
        if slot.date() != session_date:
            continue
        observed.add(slot)
        comparison = marker.get("comparison")
        comparison = comparison if isinstance(comparison, Mapping) else {}
        if str(marker.get("schema_version", "")) == SHADOW_SCHEMA_VERSION:
            admission_parity = bool(
                marker.get("parity_complete")
                and comparison.get("strategy_oi_parity")
            )
        else:
            # Retain v1 evidence as observed/successful during a mid-session
            # upgrade so the new lower-volume shadow never backfills it.
            admission_parity = bool(comparison.get("quality_parity"))
        if (
            str(marker.get("state", "")).upper() == "SUCCESS"
            and bool(marker.get("complete"))
            and admission_parity
        ):
            successful.add(slot)
    return observed, successful


def _seconds_until_next_boundary(moment: datetime) -> float:
    stamp = pd.Timestamp(moment).tz_convert(common.IST)
    next_boundary = stamp.floor("5min") + pd.Timedelta(minutes=5)
    return max(0.0, float((next_boundary - stamp).total_seconds()))


def wait_for_primary_idle(
    slot_end: datetime,
    *,
    minimum_seconds_before_boundary: float,
    timeout_sec: float,
) -> dict[str, Any]:
    """Wait until production is between slots so API-key limits cannot overlap."""
    started = time.monotonic()
    last_notice = 0.0
    marker = _production_marker(slot_end)
    heartbeat_path = common.session_heartbeat_path(PRODUCTION_SESSION)
    allowed_phases = {"WAIT_NEXT_SLOT", "END_TIME", "DONE"}
    while True:
        current = common.now_ist()
        heartbeat = _read_kv(heartbeat_path)
        phase = str(heartbeat.get("phase", "")).upper()
        seconds_to_boundary = _seconds_until_next_boundary(current)
        safe = phase in allowed_phases and (
            phase in {"END_TIME", "DONE"}
            or seconds_to_boundary >= max(0.0, minimum_seconds_before_boundary)
        )
        if safe:
            return {
                "primary_phase": phase,
                "primary_heartbeat_ts": heartbeat.get("ts", ""),
                "seconds_to_next_boundary_at_start": seconds_to_boundary,
                "waited_sec": time.monotonic() - started,
            }
        elapsed = time.monotonic() - started
        if elapsed >= max(0.0, timeout_sec):
            raise TimeoutError(
                "Production fetcher did not enter a safe idle window: "
                f"phase={phase or 'missing'} seconds_to_boundary={seconds_to_boundary:.1f}"
            )
        if elapsed - last_notice >= 5.0 or last_notice == 0.0:
            common.publish_heartbeat(
                SESSION,
                "WAITING",
                phase="WAIT_PRIMARY_IDLE",
                target_slot=slot_end.isoformat(),
                primary_phase=phase or "missing",
                seconds_to_boundary=f"{seconds_to_boundary:.1f}",
            )
            last_notice = elapsed
        time.sleep(1.0)


def wait_for_strategy_job_fence(
    slot_end: datetime,
    *,
    not_before_seconds: float,
) -> dict[str, Any]:
    """Keep the validation shadow behind the S+1 prospective paper deadline."""
    slot = _slot_timestamp(slot_end)
    minimum = max(DEFAULT_STRATEGY_JOB_FENCE_SECONDS, float(not_before_seconds))
    target_fenced = slot.time() in STRATEGY_FULL_SLOT_TIMES and slot.time() != FIRST_SLOT
    target_ready_at = slot + pd.Timedelta(seconds=minimum)
    started = time.monotonic()
    applied = False
    effective_ready_at: pd.Timestamp | None = None
    reasons: set[str] = set()
    while True:
        current = _slot_timestamp(common.now_ist())
        candidates: list[tuple[pd.Timestamp, str]] = []
        if target_fenced and current < target_ready_at:
            candidates.append((target_ready_at, "TARGET_STRATEGY_SLOT"))
        # A restart can be catching up an older target while a newer prospective
        # signal is live.  Fence the wall-clock window too, so backlog work can
        # never consume the same app quota before that signal's S+1 deadline.
        for signal_time in STRATEGY_FULL_SLOT_TIMES:
            if signal_time == FIRST_SLOT:
                continue
            live_slot = pd.Timestamp.combine(current.date(), signal_time).tz_localize(
                common.IST
            )
            live_ready_at = live_slot + pd.Timedelta(seconds=minimum)
            if live_slot <= current < live_ready_at:
                candidates.append((live_ready_at, "CURRENT_STRATEGY_WINDOW"))
        if not candidates:
            break
        ready_at, reason = max(candidates, key=lambda item: item[0])
        applied = True
        effective_ready_at = ready_at
        reasons.add(reason)
        remaining = float((ready_at - current).total_seconds())
        common.publish_heartbeat(
            SESSION,
            "WAITING",
            phase="WAIT_STRATEGY_JOB_FENCE",
            target_slot=slot.isoformat(),
            not_before_ist=ready_at.isoformat(),
            remaining_sec=f"{remaining:.1f}",
        )
        time.sleep(max(0.05, min(1.0, remaining)))
    return {
        "applied": applied,
        "not_before_ist": effective_ready_at.isoformat() if effective_ready_at else "",
        "minimum_offset_sec": minimum if applied else 0.0,
        "waited_sec": time.monotonic() - started,
        "reason": "+".join(sorted(reasons)) if applied else "NOT_REQUIRED",
    }


def build_app_lanes(
    args: argparse.Namespace,
    *,
    credentials: Iterable[common.KiteCredential] | None = None,
) -> tuple[list[AppLane], list[str]]:
    lanes: list[AppLane] = []
    failures: list[str] = []
    workers_per_app = max(1, int(args.workers_per_app))
    discovered = (
        list(credentials)
        if credentials is not None
        else common.discover_kite_credentials(max_apps=args.max_apps)
    )
    for credential in discovered:
        try:
            first = common.make_kite_client(credential, timeout_sec=args.timeout_sec)
            profile = first.profile()
            user_name = str(profile.get("user_name") or profile.get("user_id") or "validated")
            clients = [first]
            for _ in range(1, workers_per_app):
                clients.append(
                    common.make_kite_client(credential, timeout_sec=args.timeout_sec)
                )
            lanes.append(
                AppLane(
                    app_name=credential.app_name,
                    clients=clients,
                    pace_seconds=max(0.34, float(args.request_interval_sec)),
                )
            )
            print(
                f"[SHADOW][AUTH] {credential.app_name} validated for {user_name} "
                f"workers={workers_per_app}",
                flush=True,
            )
        except Exception as exc:
            detail = f"{credential.app_name}:{type(exc).__name__}:{exc}"
            failures.append(detail)
            print(f"[SHADOW][AUTH][WARN] {detail}", flush=True)
    if not lanes:
        raise RuntimeError("No authenticated Kite apps are usable: " + " | ".join(failures))
    return lanes, failures


def _historical_call(
    lane: AppLane,
    client: Any,
    contract: Mapping[str, Any],
    from_dt: datetime,
    to_dt: datetime,
    *,
    max_retries: int,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    attempts = max(1, int(max_retries))
    for attempt in range(1, attempts + 1):
        try:
            lane.pace()
            return client.historical_data(
                int(contract["instrument_token"]),
                from_dt,
                to_dt,
                "5minute",
                continuous=False,
                oi=True,
            )
        except Exception as exc:
            last_error = exc
            if _auth_failure_text(f"{type(exc).__name__}: {exc}"):
                lane._runtime_auth_failure.set()
                # Retrying a definitely invalid credential only wastes quota.
                # The existing outer slot retry selects an alternate app.
                break
            if attempt >= attempts:
                break
            message = str(exc).lower()
            if "429" in message or "too many requests" in message or "rate limit" in message:
                delay = max(2.0, 2.0**attempt)
            else:
                delay = min(8.0, 0.75 * (2 ** (attempt - 1)))
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def _expected_oi_pair_times(slot_end: datetime) -> tuple[pd.Timestamp, pd.Timestamp]:
    current = _slot_timestamp(slot_end)
    return current - pd.Timedelta(minutes=5), current


def _oi_row_error(
    row: Mapping[str, Any],
    contract: Mapping[str, Any],
    expected_end: pd.Timestamp,
) -> str:
    timestamp = _slot_timestamp(pd.Timestamp(row.get("timestamp")))
    candle_start = _slot_timestamp(pd.Timestamp(row.get("candle_start")))
    if timestamp != expected_end or candle_start != expected_end - pd.Timedelta(minutes=5):
        return "OFF_GRID"
    if str(row.get("tradingsymbol", "")).strip().upper() != str(
        contract.get("tradingsymbol", "")
    ).strip().upper():
        return "SYMBOL_MISMATCH"
    try:
        token = int(row.get("instrument_token", 0) or 0)
        expected_token = int(contract.get("instrument_token", 0) or 0)
    except (TypeError, ValueError):
        return "TOKEN_INVALID"
    if token <= 0 or token != expected_token:
        return "TOKEN_MISMATCH"
    if str(row.get("quality_state", "")) != "VALID":
        return "QUALITY_INVALID"
    if str(row.get("source", "")) != "kite_historical":
        return "SOURCE_INVALID"
    if str(row.get("data_version", "")) != common.RAW_DATA_VERSION:
        return "VERSION_INVALID"
    try:
        oi = float(row.get("oi"))
    except (TypeError, ValueError):
        return "OI_INVALID"
    if not math.isfinite(oi) or oi <= 0:
        return "OI_INVALID"
    return ""


def classify_oi_pair(
    pair_rows: pd.DataFrame,
    contract: Mapping[str, Any],
    slot_end: datetime,
) -> str:
    """Classify exact strategy OI evidence without interpolation or forward fill."""
    previous_end, current_end = _expected_oi_pair_times(slot_end)
    if pair_rows.empty or "timestamp" not in pair_rows.columns:
        return "MISSING_CURRENT"
    rows = pair_rows.copy()
    rows["timestamp"] = common._to_ist(rows["timestamp"])
    current = rows.loc[rows["timestamp"].eq(current_end)]
    if len(current) != 1:
        return "MISSING_CURRENT" if current.empty else "DUPLICATE_CURRENT"
    current_error = _oi_row_error(current.iloc[0], contract, current_end)
    if current_error:
        return f"INVALID_CURRENT_{current_error}"
    if current_end.time() == FIRST_SLOT:
        return "BASELINE_VALID"
    previous = rows.loc[rows["timestamp"].eq(previous_end)]
    if len(previous) != 1:
        return "MISSING_PREVIOUS" if previous.empty else "DUPLICATE_PREVIOUS"
    previous_error = _oi_row_error(previous.iloc[0], contract, previous_end)
    if previous_error:
        return f"INVALID_PREVIOUS_{previous_error}"
    return "VALID"


def fetch_one_contract(
    lane: AppLane,
    client: Any,
    contract: Mapping[str, Any],
    slot_end: datetime,
    *,
    max_retries: int,
    lookback_minutes: int = 5,
    require_oi_pair: bool = False,
) -> dict[str, Any]:
    symbol = str(contract["tradingsymbol"]).strip().upper()
    started = time.monotonic()
    try:
        records = _historical_call(
            lane,
            client,
            contract,
            slot_end - timedelta(minutes=max(5, int(lookback_minutes))),
            slot_end,
            max_retries=max_retries,
        )
        pair_rows = common.normalize_historical_candles(
            records,
            contract,
            fetch_timestamp=common.now_ist(),
        )
        target = _slot_timestamp(slot_end)
        previous = target - pd.Timedelta(minutes=5)
        pair_rows = pair_rows.loc[
            pair_rows["timestamp"].isin({previous, target})
        ].reset_index(drop=True)
        rows = pair_rows.loc[pair_rows["timestamp"].eq(target)].reset_index(drop=True)
        pair_state = classify_oi_pair(pair_rows, contract, slot_end)
        if rows.empty:
            return {
                "tradingsymbol": symbol,
                "underlying": str(contract.get("underlying", "")),
                "app": lane.app_name,
                "state": "NO_CANDLE",
                "rows": 0,
                "valid_rows": 0,
                "elapsed_sec": time.monotonic() - started,
                "error": "",
                "_frame": rows,
                "_oi_pair_frame": pair_rows,
                "oi_pair_state": pair_state,
            }
        valid_rows = int(rows["quality_state"].eq("VALID").sum())
        pair_admitted = pair_state in OI_PAIR_VALID_STATES
        state = "WRITTEN" if valid_rows == len(rows) else "INVALID_DATA"
        if require_oi_pair and not pair_admitted:
            state = "INVALID_DATA"
        return {
            "tradingsymbol": symbol,
            "underlying": str(contract.get("underlying", "")),
            "app": lane.app_name,
            "state": state,
            "rows": int(len(rows)),
            "valid_rows": valid_rows,
            "elapsed_sec": time.monotonic() - started,
            "error": "",
            "_frame": rows,
            "_oi_pair_frame": pair_rows,
            "oi_pair_state": pair_state,
        }
    except Exception as exc:
        return {
            "tradingsymbol": symbol,
            "underlying": str(contract.get("underlying", "")),
            "app": lane.app_name,
            "state": "FAILED",
            "rows": 0,
            "valid_rows": 0,
            "elapsed_sec": time.monotonic() - started,
            "error": f"{type(exc).__name__}: {exc}",
            "_frame": pd.DataFrame(columns=list(common.RAW_COLUMNS)),
            "_oi_pair_frame": pd.DataFrame(columns=list(common.RAW_COLUMNS)),
            "oi_pair_state": "FETCH_FAILED",
        }


def fetch_dynamic_batch(
    contracts: Iterable[Mapping[str, Any]],
    lanes: list[AppLane],
    slot_end: datetime,
    *,
    max_retries: int,
    lookback_minutes: int = 5,
    require_oi_pair: bool = False,
) -> list[dict[str, Any]]:
    work: queue.Queue[Mapping[str, Any]] = queue.Queue()
    for contract in contracts:
        work.put(contract)
    outcomes: list[dict[str, Any]] = []
    outcomes_lock = threading.Lock()

    def _worker(lane: AppLane, client: Any) -> None:
        while True:
            if lane._runtime_auth_failure.is_set():
                return
            try:
                contract = work.get_nowait()
            except queue.Empty:
                return
            try:
                outcome = fetch_one_contract(
                    lane,
                    client,
                    contract,
                    slot_end,
                    max_retries=max_retries,
                    lookback_minutes=lookback_minutes,
                    require_oi_pair=require_oi_pair,
                )
                with outcomes_lock:
                    outcomes.append(outcome)
            finally:
                work.task_done()
            if lane._runtime_auth_failure.is_set():
                return

    worker_count = sum(len(lane.clients) for lane in lanes)
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="fno-oi-fast-shadow",
    ) as pool:
        futures = [
            pool.submit(_worker, lane, client)
            for lane in lanes
            for client in lane.clients
        ]
        for future in as_completed(futures):
            future.result()
    return sorted(outcomes, key=lambda item: str(item["tradingsymbol"]))


def _choose_retry_lane(
    lanes: list[AppLane],
    attempted_apps: list[str],
) -> AppLane:
    usable = [lane for lane in lanes if not lane._runtime_auth_failure.is_set()]
    if not usable:
        raise RuntimeError("No authenticated Kite app remains for a quality retry.")
    by_name = {lane.app_name: index for index, lane in enumerate(usable)}
    last_index = by_name.get(attempted_apps[-1], -1) if attempted_apps else -1
    for offset in range(1, len(usable) + 1):
        candidate = usable[(last_index + offset) % len(usable)]
        if candidate.app_name not in attempted_apps:
            return candidate
    return usable[(last_index + 1) % len(usable)]


def fetch_assigned_batch(
    assignments: Iterable[tuple[Mapping[str, Any], AppLane]],
    slot_end: datetime,
    *,
    max_retries: int,
    lookback_minutes: int = 5,
    require_oi_pair: bool = False,
) -> list[dict[str, Any]]:
    assignment_list = list(assignments)
    lane_work: dict[str, queue.Queue[Mapping[str, Any]]] = {
        lane.app_name: queue.Queue() for _, lane in assignment_list
    }
    lane_map: dict[str, AppLane] = {}
    for contract, lane in assignment_list:
        lane_map[lane.app_name] = lane
        lane_work[lane.app_name].put(contract)
    outcomes: list[dict[str, Any]] = []
    outcomes_lock = threading.Lock()

    def _worker(lane: AppLane, client: Any) -> None:
        work = lane_work[lane.app_name]
        while True:
            if lane._runtime_auth_failure.is_set():
                return
            try:
                contract = work.get_nowait()
            except queue.Empty:
                return
            try:
                outcome = fetch_one_contract(
                    lane,
                    client,
                    contract,
                    slot_end,
                    max_retries=max_retries,
                    lookback_minutes=lookback_minutes,
                    require_oi_pair=require_oi_pair,
                )
                with outcomes_lock:
                    outcomes.append(outcome)
            finally:
                work.task_done()
            if lane._runtime_auth_failure.is_set():
                return

    workers = [
        (lane, client)
        for lane in lane_map.values()
        for client in lane.clients
    ]
    with ThreadPoolExecutor(
        max_workers=max(1, len(workers)),
        thread_name_prefix="fno-oi-fast-shadow-retry",
    ) as pool:
        futures = [pool.submit(_worker, lane, client) for lane, client in workers]
        for future in as_completed(futures):
            future.result()
    return sorted(outcomes, key=lambda item: str(item["tradingsymbol"]))


def fetch_with_quality_retries(
    universe: pd.DataFrame,
    lanes: list[AppLane],
    slot_end: datetime,
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int], int]:
    pair_lookback = int(
        getattr(args, "oi_pair_lookback_minutes", DEFAULT_OI_PAIR_LOOKBACK_MINUTES)
    )
    contracts = universe.to_dict("records")
    contract_by_symbol = {
        str(contract["tradingsymbol"]).strip().upper(): contract
        for contract in contracts
    }
    initial = fetch_dynamic_batch(
        contracts,
        lanes,
        slot_end,
        max_retries=args.max_retries,
        lookback_minutes=pair_lookback,
        require_oi_pair=True,
    )
    outcome_by_symbol = {
        str(item["tradingsymbol"]).strip().upper(): item for item in initial
    }
    attempts_by_symbol = {symbol: 1 for symbol in outcome_by_symbol}
    no_candle_observations = {
        symbol: int(item["state"] == "NO_CANDLE")
        for symbol, item in outcome_by_symbol.items()
    }
    attempted_apps = {
        symbol: [str(item["app"])] for symbol, item in outcome_by_symbol.items()
    }
    retries_used = 0
    expected_symbols = set(contract_by_symbol)
    stock_symbols = {
        symbol
        for symbol, contract in contract_by_symbol.items()
        if not _is_index_row(contract)
    }

    for retry_number in range(1, max(0, int(args.slot_retry_attempts)) + 1):
        unresolved = sorted(
            (expected_symbols - set(outcome_by_symbol))
            | {
                symbol
                for symbol, item in outcome_by_symbol.items()
                if item["state"] != "WRITTEN"
            }
        )
        if not unresolved:
            break
        # Match production readiness semantics: an optional index-only gap is
        # recorded explicitly, but it must not consume the two verification
        # retries reserved for stock futures.
        if not (set(unresolved) & stock_symbols):
            break
        retries_used = retry_number
        time.sleep(max(0.0, float(args.slot_retry_delay_sec)))
        assignments: list[tuple[Mapping[str, Any], AppLane]] = []
        for symbol in unresolved:
            history = attempted_apps.get(symbol, [])
            assignments.append(
                (contract_by_symbol[symbol], _choose_retry_lane(lanes, history))
            )
        print(
            f"[SHADOW][RETRY {retry_number}] contracts={len(assignments)}",
            flush=True,
        )
        retry_outcomes = fetch_assigned_batch(
            assignments,
            slot_end,
            max_retries=args.max_retries,
            lookback_minutes=pair_lookback,
            require_oi_pair=True,
        )
        for item in retry_outcomes:
            symbol = str(item["tradingsymbol"]).strip().upper()
            attempts_by_symbol[symbol] = attempts_by_symbol.get(symbol, 0) + 1
            attempted_apps.setdefault(symbol, []).append(str(item["app"]))
            if item["state"] == "NO_CANDLE":
                no_candle_observations[symbol] = (
                    no_candle_observations.get(symbol, 0) + 1
                )
            outcome_by_symbol[symbol] = item

    outcomes = sorted(
        outcome_by_symbol.values(), key=lambda item: str(item["tradingsymbol"])
    )
    return outcomes, attempts_by_symbol, no_candle_observations, retries_used


def _is_index_row(row: Mapping[str, Any]) -> bool:
    raw_flag = row.get("is_index_future", False)
    try:
        flagged = False if pd.isna(raw_flag) else bool(raw_flag)
    except (TypeError, ValueError):
        flagged = False
    return flagged or str(row.get("underlying", "")).strip().upper() in common.INDEX_UNDERLYINGS


def _slot_timestamp(slot_end: datetime | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(slot_end)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(common.IST)
    else:
        stamp = stamp.tz_convert(common.IST)
    return stamp


def _validate_scope_universe(universe: pd.DataFrame) -> pd.DataFrame:
    required = {
        "exchange",
        "tradingsymbol",
        "underlying",
        "instrument_token",
        "expiry",
    }
    missing = required - set(universe.columns)
    if missing:
        raise ValueError(f"Shadow universe is missing identity columns: {sorted(missing)}")
    if universe.empty:
        raise ValueError("Shadow universe is empty.")
    normalized = universe.copy()
    normalized["tradingsymbol"] = (
        normalized["tradingsymbol"].astype(str).str.strip().str.upper()
    )
    normalized["underlying"] = (
        normalized["underlying"].astype(str).str.strip().str.upper()
    )
    normalized["instrument_token"] = pd.to_numeric(
        normalized["instrument_token"], errors="coerce"
    )
    if (
        normalized["tradingsymbol"].eq("").any()
        or normalized["underlying"].eq("").any()
        or normalized["instrument_token"].isna().any()
        or normalized["instrument_token"].le(0).any()
    ):
        raise ValueError("Shadow universe contains an empty or invalid stock identity.")
    for column in ("tradingsymbol", "instrument_token", "underlying"):
        duplicates = normalized.loc[
            normalized[column].duplicated(keep=False), column
        ].astype(str)
        if not duplicates.empty:
            raise ValueError(
                f"Shadow universe {column} is not one-to-one: "
                + ", ".join(sorted(duplicates.unique())[:10])
            )
    normalized["instrument_token"] = normalized["instrument_token"].astype("int64")
    return normalized.sort_values(
        ["tradingsymbol", "instrument_token"], kind="stable"
    ).reset_index(drop=True)


def build_shadow_scope(
    full_universe: pd.DataFrame,
    slot_end: datetime,
    *,
    canary_count: int = DEFAULT_CANARY_COUNT,
) -> ShadowScope:
    """Select a restart-stable stock-only validation scope for one exact slot."""
    slot = _slot_timestamp(slot_end)
    if (
        slot.second
        or slot.microsecond
        or slot.minute % 5
        or not FIRST_SLOT <= slot.time() <= LAST_SLOT
    ):
        raise ValueError(f"Shadow slot is not an exact in-session 5m boundary: {slot}")
    normalized = _validate_scope_universe(full_universe)
    stocks = normalized.loc[
        ~normalized.apply(lambda row: _is_index_row(row), axis=1)
    ].reset_index(drop=True)
    if stocks.empty:
        raise ValueError("Shadow stock universe is empty.")
    count = int(canary_count)
    if count < 1 or count > len(stocks):
        raise ValueError(
            f"--canary-count must be in [1, {len(stocks)}], observed {count}."
        )

    strategy_slot = slot.time() in STRATEGY_FULL_SLOT_TIMES
    rotation_ordinal: int | None = None
    rotation_offset: int | None = None
    if strategy_slot:
        selected = stocks.copy()
        mode = SCOPE_STRATEGY_FULL
    else:
        canary_start = pd.Timestamp.combine(slot.date(), FIRST_CANARY_SLOT).tz_localize(
            common.IST
        )
        rotation_ordinal = int((slot - canary_start).total_seconds() // 300)
        if rotation_ordinal < 0:
            raise ValueError(f"Unexpected non-strategy slot before 09:50: {slot}")
        rotation_offset = (rotation_ordinal * count) % len(stocks)
        positions = [
            (rotation_offset + index) % len(stocks) for index in range(count)
        ]
        selected = stocks.iloc[positions].copy().reset_index(drop=True)
        mode = SCOPE_ROTATING_CANARY

    selected_symbols = tuple(selected["tradingsymbol"].astype(str))
    stock_symbols = tuple(stocks["tradingsymbol"].astype(str))
    return ShadowScope(
        universe=selected,
        mode=mode,
        strategy_slot=strategy_slot,
        full_universe_contracts=int(len(normalized)),
        full_stock_contracts=int(len(stocks)),
        selected_contracts=int(len(selected)),
        full_universe_sha256=common.universe_sha256(normalized),
        full_stock_universe_sha256=common.universe_sha256(stocks),
        full_stock_symbol_set_sha256=common.symbol_set_sha256(stock_symbols),
        selected_universe_sha256=common.universe_sha256(selected),
        selected_symbol_set_sha256=common.symbol_set_sha256(selected_symbols),
        selected_symbols=selected_symbols,
        canary_count=count,
        rotation_ordinal=rotation_ordinal,
        rotation_offset=rotation_offset,
    )


def validate_production_scope_contract(
    scope: ShadowScope,
    production_marker: Mapping[str, Any],
) -> None:
    """Fail closed unless the selected scope comes from the final stock mapping."""
    if str(production_marker.get("schema_version", "")) != common.FNO_FETCH_SLOT_SCHEMA_VERSION:
        raise ValueError("Production marker schema is not the required current version.")
    if not bool(production_marker.get("complete")) or not bool(
        production_marker.get("stock_complete")
    ):
        raise ValueError("Production stock marker is not complete.")
    checks = {
        "universe_sha256": scope.full_universe_sha256,
        "stock_universe_sha256": scope.full_stock_universe_sha256,
        "stock_symbol_set_sha256": scope.full_stock_symbol_set_sha256,
    }
    for key, expected in checks.items():
        observed = str(production_marker.get(key, ""))
        if not observed or observed != expected:
            raise ValueError(
                f"Production {key} differs from the validated local mapping: "
                f"{observed or 'missing'} != {expected}"
            )
    try:
        stock_count = int(production_marker.get("stock_contracts_expected"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Production stock contract count is missing.") from exc
    if stock_count != scope.full_stock_contracts:
        raise ValueError(
            "Production stock contract count differs from the validated local mapping: "
            f"{stock_count} != {scope.full_stock_contracts}"
        )


def build_quality_marker(
    slot_end: datetime,
    universe: pd.DataFrame,
    outcomes: list[dict[str, Any]],
    attempts_by_symbol: dict[str, int],
    no_candle_observations: dict[str, int],
    lanes: list[AppLane],
    args: argparse.Namespace,
    *,
    retries_used: int,
) -> dict[str, Any]:
    universe_rows = universe.to_dict("records")
    expected_symbols = {
        str(row.get("tradingsymbol", "")).strip().upper() for row in universe_rows
    }
    stock_symbols = {
        str(row.get("tradingsymbol", "")).strip().upper()
        for row in universe_rows
        if not _is_index_row(row)
    }
    index_symbols = expected_symbols - stock_symbols
    outcome_by_symbol = {
        str(item["tradingsymbol"]).strip().upper(): item for item in outcomes
    }
    observed_symbols = set(outcome_by_symbol)
    written_symbols = {
        symbol for symbol, item in outcome_by_symbol.items() if item["state"] == "WRITTEN"
    }
    no_candle_symbols = {
        symbol for symbol, item in outcome_by_symbol.items() if item["state"] == "NO_CANDLE"
    }
    invalid_symbols = {
        symbol for symbol, item in outcome_by_symbol.items() if item["state"] == "INVALID_DATA"
    }
    failed_symbols = {
        symbol for symbol, item in outcome_by_symbol.items() if item["state"] == "FAILED"
    }
    verified_no_candle = {
        symbol
        for symbol in no_candle_symbols
        if no_candle_observations.get(symbol, 0)
        >= common.MIN_NO_CANDLE_FETCH_ATTEMPTS
    }
    unverified_no_candle = no_candle_symbols - verified_no_candle
    stock_written = written_symbols & stock_symbols
    stock_no_candle = no_candle_symbols & stock_symbols
    stock_verified_no_candle = verified_no_candle & stock_symbols
    stock_failed = failed_symbols & stock_symbols
    stock_invalid = invalid_symbols & stock_symbols
    stock_unverified = unverified_no_candle & stock_symbols
    index_no_candle = no_candle_symbols & index_symbols
    unexpected_observed = observed_symbols - expected_symbols
    expected = len(expected_symbols)
    written = len(written_symbols)
    stock_expected = len(stock_symbols)
    coverage = float(written / expected) if expected else 0.0
    stock_coverage = float(len(stock_written) / stock_expected) if stock_expected else 0.0
    minimum_coverage = max(
        float(args.min_coverage), common.MIN_STOCK_FUTURES_COVERAGE
    )
    stock_complete = bool(
        (observed_symbols & stock_symbols) == stock_symbols
        and not unexpected_observed
        and stock_expected > 0
        and not stock_failed
        and not stock_invalid
        and not stock_unverified
        and len(stock_written) + len(stock_no_candle) == stock_expected
        and stock_coverage >= minimum_coverage
        and len(stock_verified_no_candle) <= common.MAX_VERIFIED_NO_CANDLE_STOCKS
    )
    global_complete = bool(
        observed_symbols == expected_symbols
        and not failed_symbols
        and not invalid_symbols
        and not unverified_no_candle
        and coverage >= minimum_coverage
    )
    complete = bool(
        stock_complete
        and observed_symbols == expected_symbols
        and not failed_symbols
        and not invalid_symbols
        and written + len(no_candle_symbols) == expected
    )
    return {
        "schema_version": SHADOW_SCHEMA_VERSION,
        "source": "final",
        "state": "SUCCESS" if complete else "PARTIAL",
        "complete": complete,
        "slot_ist": slot_end.isoformat(),
        "published_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "universe_sha256": common.universe_sha256(universe),
        "contracts_expected": expected,
        "contracts_written": written,
        "coverage_ratio": coverage,
        "stock_contracts_expected": stock_expected,
        "stock_contracts_written": len(stock_written),
        "stock_coverage_ratio": stock_coverage,
        "stock_complete": stock_complete,
        "stock_state": "SUCCESS" if stock_complete else "PARTIAL",
        "stock_verified_no_candle_symbols": sorted(stock_verified_no_candle),
        "stock_unverified_no_candle_symbols": sorted(stock_unverified),
        "global_complete": global_complete,
        "index_contracts_expected": len(index_symbols),
        "index_contracts_written": len(written_symbols & index_symbols),
        "index_no_candle_count": len(index_no_candle),
        "index_no_candle_symbols": sorted(index_no_candle),
        "no_candle_count": len(no_candle_symbols),
        "no_candle_symbols": sorted(no_candle_symbols),
        "verified_no_candle_symbols": sorted(verified_no_candle),
        "unverified_no_candle_symbols": sorted(unverified_no_candle),
        "no_candle_fetch_attempts": {
            symbol: int(attempts_by_symbol.get(symbol, 0))
            for symbol in sorted(no_candle_symbols)
        },
        "no_candle_observations": {
            symbol: int(no_candle_observations.get(symbol, 0))
            for symbol in sorted(no_candle_symbols)
        },
        "invalid_data_count": len(invalid_symbols),
        "failed_count": len(failed_symbols),
        "minimum_stock_coverage": minimum_coverage,
        "minimum_no_candle_fetch_attempts": common.MIN_NO_CANDLE_FETCH_ATTEMPTS,
        "maximum_verified_no_candle_stocks": common.MAX_VERIFIED_NO_CANDLE_STOCKS,
        "readiness_policy": common.VERIFIED_NO_CANDLE_POLICY_VERSION,
        "apps_used": [lane.app_name for lane in lanes],
        "workers_per_app": max(1, int(args.workers_per_app)),
        "slot_retry_attempts_used": retries_used,
        "slot_fetch_attempts_max": max(attempts_by_symbol.values(), default=0),
        "failure_sample": [
            {
                "tradingsymbol": item["tradingsymbol"],
                "state": item["state"],
                "error": item.get("error", ""),
            }
            for item in outcomes
            if item["state"] in {"FAILED", "INVALID_DATA"}
        ][:20],
    }


def rows_from_outcomes(outcomes: list[dict[str, Any]]) -> pd.DataFrame:
    frames = [
        item["_frame"]
        for item in outcomes
        if item["state"] == "WRITTEN" and not item["_frame"].empty
    ]
    if not frames:
        return pd.DataFrame(columns=list(common.RAW_COLUMNS))
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["timestamp"] = common._to_ist(combined["timestamp"])
    return (
        combined.drop_duplicates(["tradingsymbol", "timestamp"], keep="last")
        .sort_values(["tradingsymbol", "timestamp"], kind="stable")
        .reset_index(drop=True)
        .loc[:, list(common.RAW_COLUMNS)]
    )


def oi_pair_rows_from_outcomes(outcomes: list[dict[str, Any]]) -> pd.DataFrame:
    frames = [
        item.get("_oi_pair_frame")
        for item in outcomes
        if isinstance(item.get("_oi_pair_frame"), pd.DataFrame)
        and not item["_oi_pair_frame"].empty
    ]
    if not frames:
        return pd.DataFrame(columns=list(common.RAW_COLUMNS))
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["timestamp"] = common._to_ist(combined["timestamp"])
    combined["candle_start"] = common._to_ist(combined["candle_start"])
    return (
        combined.drop_duplicates(["tradingsymbol", "timestamp"], keep="last")
        .sort_values(["tradingsymbol", "timestamp"], kind="stable")
        .reset_index(drop=True)
        .loc[:, list(common.RAW_COLUMNS)]
    )


def build_oi_evidence(
    universe: pd.DataFrame,
    pair_rows: pd.DataFrame,
    slot_end: datetime,
) -> pd.DataFrame:
    """Project exact S/S-5 rows to the compact strategy OI contract."""
    target = _slot_timestamp(slot_end)
    previous = target - pd.Timedelta(minutes=5)
    source = pair_rows.copy()
    if not source.empty:
        source["timestamp"] = common._to_ist(source["timestamp"])
        source["candle_start"] = common._to_ist(source["candle_start"])
        source["tradingsymbol"] = (
            source["tradingsymbol"].astype(str).str.strip().str.upper()
        )
    evidence: list[dict[str, Any]] = []
    for contract in universe.sort_values("tradingsymbol", kind="stable").to_dict("records"):
        symbol = str(contract.get("tradingsymbol", "")).strip().upper()
        selected = (
            source.loc[source["tradingsymbol"].eq(symbol)].copy()
            if not source.empty
            else pd.DataFrame(columns=list(common.RAW_COLUMNS))
        )
        state = classify_oi_pair(selected, contract, slot_end)
        current = selected.loc[selected["timestamp"].eq(target)].tail(1)
        prior = selected.loc[selected["timestamp"].eq(previous)].tail(1)
        current_row: Mapping[str, Any] = (
            current.iloc[0].to_dict() if not current.empty else {}
        )
        prior_row: Mapping[str, Any] = prior.iloc[0].to_dict() if not prior.empty else {}
        try:
            oi = float(current_row.get("oi"))
        except (TypeError, ValueError):
            oi = math.nan
        try:
            prev_oi = float(prior_row.get("oi"))
        except (TypeError, ValueError):
            prev_oi = math.nan
        if state == "VALID":
            oi_change_pct = (oi / prev_oi - 1.0) * 100.0
        else:
            oi_change_pct = math.nan
        baseline = target.time() == FIRST_SLOT
        evidence.append(
            {
                "timestamp": target,
                "candle_start": target - pd.Timedelta(minutes=5),
                "previous_timestamp": pd.NaT if baseline else previous,
                "previous_candle_start": (
                    pd.NaT if baseline else previous - pd.Timedelta(minutes=5)
                ),
                "underlying": str(
                    current_row.get("underlying", contract.get("underlying", ""))
                ).strip().upper(),
                "tradingsymbol": symbol,
                "instrument_token": current_row.get(
                    "instrument_token", contract.get("instrument_token")
                ),
                "oi": oi,
                "prev_oi": math.nan if baseline else prev_oi,
                "oi_change_pct": oi_change_pct,
                "oi_pair_state": state,
                "quality_state": str(current_row.get("quality_state", "")),
                "fetch_timestamp": current_row.get("fetch_timestamp", pd.NaT),
                "source": str(current_row.get("source", "")),
                "data_version": str(current_row.get("data_version", "")),
            }
        )
    frame = pd.DataFrame(evidence, columns=list(OI_EVIDENCE_COLUMNS))
    if frame.empty:
        return frame
    for column in (
        "timestamp",
        "candle_start",
        "previous_timestamp",
        "previous_candle_start",
        "fetch_timestamp",
    ):
        frame[column] = common._to_ist(frame[column])
    frame["instrument_token"] = pd.to_numeric(
        frame["instrument_token"], errors="coerce"
    ).astype("Int64")
    for column in ("oi", "prev_oi", "oi_change_pct"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_values("tradingsymbol", kind="stable").reset_index(drop=True)


def oi_evidence_complete(evidence: pd.DataFrame, slot_end: datetime) -> bool:
    expected_state = "BASELINE_VALID" if _slot_timestamp(slot_end).time() == FIRST_SLOT else "VALID"
    return bool(
        not evidence.empty
        and evidence["tradingsymbol"].nunique() == len(evidence)
        and evidence["oi_pair_state"].eq(expected_state).all()
        and pd.to_numeric(evidence["oi"], errors="coerce").gt(0).all()
        and (
            expected_state == "BASELINE_VALID"
            or (
                pd.to_numeric(evidence["prev_oi"], errors="coerce").gt(0).all()
                and np.isfinite(
                    pd.to_numeric(evidence["oi_change_pct"], errors="coerce")
                ).all()
            )
        )
    )


def compare_oi_evidence(
    universe: pd.DataFrame,
    shadow_evidence: pd.DataFrame,
    production_evidence: pd.DataFrame,
) -> tuple[dict[str, Any], pd.DataFrame]:
    symbols = sorted(universe["tradingsymbol"].astype(str).str.upper().unique())
    shadow_index = shadow_evidence.set_index("tradingsymbol", drop=False)
    production_index = production_evidence.set_index("tradingsymbol", drop=False)
    mismatch_rows: list[dict[str, Any]] = []
    exact_symbols = 0
    for symbol in symbols:
        if symbol not in shadow_index.index or symbol not in production_index.index:
            mismatch_rows.append(
                {
                    "tradingsymbol": symbol,
                    "field": "__evidence_state__",
                    "shadow": "PRESENT" if symbol in shadow_index.index else "MISSING",
                    "production": (
                        "PRESENT" if symbol in production_index.index else "MISSING"
                    ),
                }
            )
            continue
        left = shadow_index.loc[symbol]
        right = production_index.loc[symbol]
        matches = True
        for field_name in OI_EVIDENCE_COMPARE_FIELDS:
            if field_name == "tradingsymbol":
                continue
            if not _values_equal(left.get(field_name), right.get(field_name)):
                matches = False
                mismatch_rows.append(
                    {
                        "tradingsymbol": symbol,
                        "field": field_name,
                        "shadow": left.get(field_name),
                        "production": right.get(field_name),
                    }
                )
        if matches:
            exact_symbols += 1
    mismatches = pd.DataFrame(
        mismatch_rows,
        columns=["tradingsymbol", "field", "shadow", "production"],
    )
    mismatch_symbols = sorted(
        set(mismatches["tradingsymbol"].astype(str)) if not mismatches.empty else set()
    )
    return (
        {
            "strategy_oi_parity": not mismatch_rows,
            "parity_contract": "EXACT_NFO_5M_OI_S_AND_S_MINUS_5_V1",
            "symbols_compared": len(symbols),
            "exact_match_symbols": exact_symbols,
            "mismatch_symbol_count": len(mismatch_symbols),
            "mismatch_symbols": mismatch_symbols,
            "field_mismatch_count": len(mismatch_rows),
        },
        mismatches,
    )


def _production_compare_columns() -> list[str]:
    columns = ["timestamp", "tradingsymbol", *COMPARE_FIELDS, *OI_IDENTITY_FIELDS]
    return list(dict.fromkeys(columns))


def _load_production_rows_sequential(
    universe: pd.DataFrame,
    slot_end: datetime,
) -> pd.DataFrame:
    """Original per-file reader retained as the exact compatibility fallback."""
    target = pd.Timestamp(slot_end)
    frames: list[pd.DataFrame] = []
    columns = _production_compare_columns()
    for symbol in universe["tradingsymbol"].astype(str):
        path = common.raw_contract_path(symbol)
        if not path.exists():
            continue
        try:
            raw = pd.read_parquet(path, columns=columns)
            raw["timestamp"] = common._to_ist(raw["timestamp"])
            current = raw.loc[raw["timestamp"].eq(target)].tail(1)
            if not current.empty:
                frames.append(current)
        except (KeyError, OSError, ValueError):
            continue
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True, sort=False)


def _normalized_path_key(value: object) -> str:
    return os.path.normcase(str(Path(str(value)).resolve()))


def _path_stat_signature(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def _load_production_rows_dataset(
    universe: pd.DataFrame,
    slot_end: datetime,
) -> pd.DataFrame:
    """Read the exact target row from all universe files in one Arrow scan."""
    import pyarrow as pa
    import pyarrow.dataset as arrow_dataset

    columns = _production_compare_columns()
    path_entries: list[tuple[int, str, Path]] = []
    for order, symbol in enumerate(universe["tradingsymbol"].astype(str)):
        path = common.raw_contract_path(symbol)
        if path.exists():
            path_entries.append((order, str(symbol).strip().upper(), path.resolve()))
    if not path_entries:
        return pd.DataFrame(columns=columns)

    path_keys = [_normalized_path_key(path) for _, _, path in path_entries]
    if len(set(path_keys)) != len(path_keys):
        raise ValueError("duplicate production contract paths require sequential loading")
    file_snapshot = {
        key: _path_stat_signature(path)
        for key, (_, _, path) in zip(path_keys, path_entries)
    }

    dataset = arrow_dataset.dataset(
        [str(path) for _, _, path in path_entries],
        format="parquet",
    )
    required = set(columns)
    if not required.issubset(dataset.schema.names):
        raise KeyError(
            f"production dataset is missing comparison columns: "
            f"{sorted(required - set(dataset.schema.names))}"
        )
    timestamp_type = dataset.schema.field("timestamp").type
    if not pa.types.is_timestamp(timestamp_type) or timestamp_type.tz is None:
        # Filtering an aware target against a naive Arrow timestamp changes the
        # legacy behavior, which localizes each file before comparing.
        raise ValueError("production timestamps require sequential timezone normalization")

    target = pd.Timestamp(slot_end)
    if target.tzinfo is None:
        target = target.tz_localize(common.IST)
    target = target.tz_convert(timestamp_type.tz)
    target_scalar = pa.scalar(target.to_pydatetime(), type=timestamp_type)
    table = dataset.to_table(
        columns=[*columns, "__filename"],
        filter=arrow_dataset.field("timestamp") == target_scalar,
    )

    # A dataset can null-fill a column that is physically absent from one
    # fragment. The legacy reader skips that file, so detect it and fall back.
    for fragment in dataset.get_fragments():
        physical = fragment.physical_schema
        if not required.issubset(physical.names):
            raise KeyError(f"production fragment is missing comparison columns: {fragment.path}")
        physical_timestamp = physical.field("timestamp").type
        if (
            not pa.types.is_timestamp(physical_timestamp)
            or physical_timestamp.tz != timestamp_type.tz
        ):
            raise ValueError(
                f"production fragment timestamp timezone differs: {fragment.path}"
            )
        for column in columns:
            dataset_type = dataset.schema.field(column).type
            physical_type = physical.field(column).type
            if physical_type == dataset_type:
                continue
            # Pandas concatenation promotes mixed integer/float columns to
            # float64. Arrow is equivalent only when the dataset's selected
            # type is already float64; an integer-first dataset can silently
            # narrow whole-valued float fragments.
            safe_numeric_promotion = bool(
                pa.types.is_floating(dataset_type)
                and dataset_type.bit_width == 64
                and (
                    pa.types.is_integer(physical_type)
                    or pa.types.is_floating(physical_type)
                )
            )
            if not safe_numeric_promotion:
                raise TypeError(
                    f"production fragment type requires sequential promotion: "
                    f"{fragment.path} {column}={physical_type} dataset={dataset_type}"
                )

    frame = table.to_pandas()
    after_snapshot = {
        key: _path_stat_signature(path)
        for key, (_, _, path) in zip(path_keys, path_entries)
    }
    if after_snapshot != file_snapshot:
        raise OSError("production contract files changed during the Arrow scan")
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame["timestamp"] = common._to_ist(frame["timestamp"])
    details_by_path = {
        _normalized_path_key(path): (order, symbol)
        for order, symbol, path in path_entries
    }
    frame["_source_path"] = frame["__filename"].map(_normalized_path_key)
    frame["_source_order"] = frame["_source_path"].map(
        lambda value: details_by_path.get(value, (None, ""))[0]
    )
    if frame["_source_order"].isna().any():
        raise ValueError("Arrow returned a production fragment outside the universe")
    expected_symbols = frame["_source_path"].map(
        lambda value: details_by_path[value][1]
    )
    if not frame["tradingsymbol"].astype(str).str.strip().str.upper().eq(
        expected_symbols
    ).all():
        raise ValueError("production row symbol differs from its universe contract path")
    frame["_source_row_order"] = np.arange(len(frame), dtype=np.int64)
    frame = frame.sort_values(
        ["_source_order", "_source_row_order"],
        kind="stable",
    )
    # Match each legacy per-file `.tail(1)` if a physical file contains a
    # duplicate target timestamp, then restore universe order.
    frame = frame.drop_duplicates("_source_path", keep="last").sort_values(
        "_source_order",
        kind="stable",
    )
    return frame.loc[:, columns].reset_index(drop=True)


def load_production_rows(universe: pd.DataFrame, slot_end: datetime) -> pd.DataFrame:
    try:
        return _load_production_rows_dataset(universe, slot_end)
    except MemoryError:
        raise
    except Exception as exc:
        print(
            f"[SHADOW][COMPARE][FALLBACK] {type(exc).__name__}: {exc}",
            flush=True,
        )
        return _load_production_rows_sequential(universe, slot_end)


def _values_equal(left: Any, right: Any) -> bool:
    left_missing = bool(pd.isna(left))
    right_missing = bool(pd.isna(right))
    if left_missing or right_missing:
        return left_missing and right_missing
    if isinstance(left, (int, float, np.integer, np.floating)) and isinstance(
        right, (int, float, np.integer, np.floating)
    ):
        return bool(np.isclose(float(left), float(right), rtol=0.0, atol=0.0))
    return str(left) == str(right)


def compare_with_production(
    universe: pd.DataFrame,
    shadow_rows: pd.DataFrame,
    production_rows: pd.DataFrame,
    shadow_marker: Mapping[str, Any],
    production_marker: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame]:
    symbols = sorted(universe["tradingsymbol"].astype(str).str.upper().unique())
    selected_symbols = set(symbols)
    shadow_index = (
        shadow_rows.drop_duplicates("tradingsymbol", keep="last")
        .assign(tradingsymbol=lambda frame: frame["tradingsymbol"].astype(str).str.upper())
        .set_index("tradingsymbol", drop=False)
    )
    production_index = (
        production_rows.drop_duplicates("tradingsymbol", keep="last")
        .assign(tradingsymbol=lambda frame: frame["tradingsymbol"].astype(str).str.upper())
        .set_index("tradingsymbol", drop=False)
    )
    shadow_no_candle = {
        str(value).upper() for value in shadow_marker.get("no_candle_symbols", [])
    } & selected_symbols
    production_no_candle = {
        str(value).upper() for value in production_marker.get("no_candle_symbols", [])
    } & selected_symbols
    mismatch_rows: list[dict[str, Any]] = []
    exact_symbols = 0
    oi_exact_symbols = 0
    exact_mismatch_count = 0
    oi_mismatch_count = 0
    comparison_fields = tuple(dict.fromkeys((*COMPARE_FIELDS, *OI_IDENTITY_FIELDS)))

    for symbol in symbols:
        shadow_state = (
            "WRITTEN"
            if symbol in shadow_index.index
            else "NO_CANDLE"
            if symbol in shadow_no_candle
            else "MISSING"
        )
        production_state = (
            "WRITTEN"
            if symbol in production_index.index
            else "NO_CANDLE"
            if symbol in production_no_candle
            else "MISSING"
        )
        if shadow_state != production_state or shadow_state == "MISSING":
            mismatch_rows.append({
                "tradingsymbol": symbol,
                "field": "__state__",
                "shadow": shadow_state,
                "production": production_state,
            })
            exact_mismatch_count += 1
            oi_mismatch_count += 1
            continue
        if shadow_state == "NO_CANDLE":
            exact_symbols += 1
            oi_exact_symbols += 1
            continue
        shadow_row = shadow_index.loc[symbol]
        production_row = production_index.loc[symbol]
        symbol_exact_matches = True
        symbol_oi_matches = True
        for field_name in comparison_fields:
            left = shadow_row.get(field_name)
            right = production_row.get(field_name)
            if not _values_equal(left, right):
                if field_name in COMPARE_FIELDS:
                    symbol_exact_matches = False
                    exact_mismatch_count += 1
                if field_name in OI_IDENTITY_FIELDS:
                    symbol_oi_matches = False
                    oi_mismatch_count += 1
                mismatch_rows.append(
                    {
                        "tradingsymbol": symbol,
                        "field": field_name,
                        "shadow": left,
                        "production": right,
                    }
                )
        if symbol_exact_matches:
            exact_symbols += 1
        if symbol_oi_matches:
            oi_exact_symbols += 1

    mismatches = pd.DataFrame(
        mismatch_rows,
        columns=["tradingsymbol", "field", "shadow", "production"],
    )
    mismatch_symbols = sorted(
        set(mismatches["tradingsymbol"].astype(str)) if not mismatches.empty else set()
    )
    comparison = {
        # Backward-compatible exact 10-field OHLCV+OI diagnostic.  Strategy
        # admission is reported separately and never hides these differences.
        "quality_parity": exact_mismatch_count == 0,
        "exact_candle_parity": exact_mismatch_count == 0,
        "oi_quality_parity": oi_mismatch_count == 0,
        "oi_identity_parity": oi_mismatch_count == 0,
        "universe_symbols": len(symbols),
        "exact_match_symbols": exact_symbols,
        "oi_exact_match_symbols": oi_exact_symbols,
        "mismatch_symbol_count": len(mismatch_symbols),
        "mismatch_symbols": mismatch_symbols,
        "field_mismatch_count": exact_mismatch_count,
        "oi_field_mismatch_count": oi_mismatch_count,
        "diagnostic_mismatch_count": len(mismatch_rows),
        "shadow_rows": int(len(shadow_rows)),
        "production_rows": int(len(production_rows)),
        "shadow_no_candle_symbols": sorted(shadow_no_candle),
        "production_no_candle_symbols": sorted(production_no_candle),
        "no_candle_set_equal": shadow_no_candle == production_no_candle,
        "comparison_scope": "SELECTED_SYMBOLS_ONLY",
        "exact_compare_fields": list(COMPARE_FIELDS),
        "oi_identity_fields": list(OI_IDENTITY_FIELDS),
    }
    return comparison, mismatches


def load_production_oi_pair_rows(
    universe: pd.DataFrame,
    slot_end: datetime,
) -> pd.DataFrame:
    frames = [load_production_rows(universe, slot_end)]
    if _slot_timestamp(slot_end).time() != FIRST_SLOT:
        frames.insert(0, load_production_rows(universe, slot_end - timedelta(minutes=5)))
    available = [frame for frame in frames if not frame.empty]
    if not available:
        return pd.DataFrame(columns=_production_compare_columns())
    combined = pd.concat(available, ignore_index=True, sort=False)
    combined["timestamp"] = common._to_ist(combined["timestamp"])
    return combined.sort_values(
        ["tradingsymbol", "timestamp"], kind="stable"
    ).reset_index(drop=True)


def apply_scope_metadata(
    marker: dict[str, Any],
    scope: ShadowScope,
    outcomes: list[dict[str, Any]],
    evidence: pd.DataFrame,
    slot_end: datetime,
) -> None:
    expected = set(scope.selected_symbols)
    by_symbol = {
        str(item.get("tradingsymbol", "")).strip().upper(): item for item in outcomes
    }
    exact_outcomes = set(by_symbol) == expected and len(outcomes) == len(expected)
    every_written = exact_outcomes and all(
        str(by_symbol[symbol].get("state", "")) == "WRITTEN" for symbol in expected
    )
    pair_complete = bool(
        len(evidence) == scope.selected_contracts
        and set(evidence["tradingsymbol"].astype(str).str.upper()) == expected
        and oi_evidence_complete(evidence, slot_end)
    )
    scope_complete = bool(every_written and pair_complete)
    marker.update(
        {
            "complete": scope_complete,
            "scope_complete": scope_complete,
            "stock_complete": scope_complete,
            "global_complete": False,
            "scope_policy": SCOPE_POLICY_VERSION,
            "scope_mode": scope.mode,
            "strategy_slot": scope.strategy_slot,
            "strategy_full_slot_times": [
                value.strftime("%H:%M") for value in STRATEGY_FULL_SLOT_TIMES
            ],
            "validation_only": True,
            "strategy_authority": False,
            "full_universe_parity": False,
            "full_stock_parity": False,
            "full_contracts_expected": scope.full_universe_contracts,
            "full_stock_contracts_expected": scope.full_stock_contracts,
            "selected_contracts_expected": scope.selected_contracts,
            "contracts_expected": scope.selected_contracts,
            "stock_contracts_expected": scope.selected_contracts,
            "full_universe_sha256": scope.full_universe_sha256,
            "universe_sha256": scope.selected_universe_sha256,
            "full_stock_universe_sha256": scope.full_stock_universe_sha256,
            "stock_universe_sha256": scope.selected_universe_sha256,
            "full_stock_symbol_set_sha256": scope.full_stock_symbol_set_sha256,
            "stock_symbol_set_sha256": scope.selected_symbol_set_sha256,
            "selected_universe_sha256": scope.selected_universe_sha256,
            "selected_symbol_set_sha256": scope.selected_symbol_set_sha256,
            "selected_symbols": list(scope.selected_symbols),
            "canary_count": scope.canary_count,
            "canary_rotation_ordinal": scope.rotation_ordinal,
            "canary_rotation_offset": scope.rotation_offset,
            "oi_pair_expected": scope.selected_contracts,
            "oi_pair_valid": int(
                evidence["oi_pair_state"].isin(OI_PAIR_VALID_STATES).sum()
            ),
            "oi_pair_complete": pair_complete,
            "oi_pair_contract": "EXACT_NFO_5M_OI_S_AND_S_MINUS_5_V1",
            "oi_evidence_schema_version": OI_EVIDENCE_SCHEMA_VERSION,
            "oi_evidence_columns": list(OI_EVIDENCE_COLUMNS),
            "comparison_fields": list(COMPARE_FIELDS),
            "oi_identity_fields": list(OI_IDENTITY_FIELDS),
        }
    )
    marker["state"] = "SUCCESS" if scope_complete else "PARTIAL"


def _make_run_dir(output_root: Path, slot_end: datetime) -> tuple[str, Path]:
    run_id = f"{slot_end.strftime('%Y%m%d_%H%M')}_{common.now_ist().strftime('%Y%m%dT%H%M%S%f%z')}"
    run_dir = output_root / slot_end.date().isoformat() / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_id, run_dir


def render_shadow_report(marker: Mapping[str, Any]) -> str:
    """Render scoped strategy-OI parity and the separate candle diagnostic."""
    comparison = marker.get("comparison")
    comparison = comparison if isinstance(comparison, Mapping) else {}
    speedup = marker.get("speedup_vs_production")
    speedup_text = f"{float(speedup):.2f}x" if speedup is not None else "n/a"
    oi_parity = bool(comparison.get("strategy_oi_parity"))
    exact_parity = bool(comparison.get("quality_parity"))
    lines = [
        "# FnO Fast Shadow 5-Minute Futures OI Fetch",
        "",
        "This validation-only session reads Kite independently and never appends to the production archive or feeds a strategy.",
        "",
        f"Slot: {marker.get('slot_ist', '')}",
        f"Published: {marker.get('published_at_ist', '')}",
        f"State: {marker.get('state', '')}",
        f"Scope: {marker.get('scope_mode', '')} ({int(marker.get('contracts_expected') or 0)} of {int(marker.get('full_stock_contracts_expected') or 0)} stock futures)",
        f"Validation result: {marker.get('validation_result', '')}",
        f"Exact strategy OI parity: {'PASS' if oi_parity else 'FAIL'}",
        f"Exact OHLCV + OI diagnostic: {'PASS' if exact_parity else 'DIFFERENT'}",
        f"Strategy-input validation: {'PASS' if marker.get('strategy_validation_ready') else 'NOT READY'}",
        "Strategy authority: NO (shadow evidence is never routed to a strategy)",
        f"Fast fetch + persist: {float(marker.get('fetch_persist_duration_sec') or 0.0):.3f}s",
        f"Production fetch: {float(marker.get('production_duration_sec') or 0.0):.3f}s",
        f"Speedup: {speedup_text}",
        "",
        "Metric | Fast shadow | Production / comparison",
        "---|---:|---:",
        (
            f"Contracts written | {int(marker.get('contracts_written') or 0)} | "
            f"{int(marker.get('contracts_expected') or 0)} expected"
        ),
        (
            f"Exact candle symbols | {int(comparison.get('exact_match_symbols') or 0)} | "
            f"{int(comparison.get('universe_symbols') or 0)} compared"
        ),
        (
            f"Exact OI symbols | {int((comparison.get('oi_evidence') or {}).get('exact_match_symbols') or 0)} | "
            f"{int((comparison.get('oi_evidence') or {}).get('symbols_compared') or 0)} compared"
        ),
        (
            f"Field mismatches | {int(comparison.get('field_mismatch_count') or 0)} | "
            f"{int(comparison.get('mismatch_symbol_count') or 0)} symbols"
        ),
        (
            f"No-candle set equal | {'YES' if comparison.get('no_candle_set_equal') else 'NO'} | "
            f"policy={marker.get('readiness_policy', '')}"
        ),
        (
            f"Apps used | {len(marker.get('apps_used') or [])} | "
            f"workers/app={int(marker.get('workers_per_app') or 0)}"
        ),
        "",
        f"Shadow marker: `{marker.get('marker_path', '')}`",
        f"Compact OI evidence: `{marker.get('oi_evidence_path', '')}`",
        f"OI mismatch evidence: `{marker.get('oi_mismatch_path', '')}`",
        f"Mismatch evidence: `{marker.get('mismatch_path', '')}`",
    ]
    mismatch_symbols = comparison.get("mismatch_symbols") or []
    if mismatch_symbols:
        lines.extend(
            [
                "",
                "Mismatch symbols: " + ", ".join(str(value) for value in mismatch_symbols[:20]),
            ]
        )
    return "\n".join(lines) + "\n"


def run_shadow(
    args: argparse.Namespace,
    *,
    lane_session: AppLaneSession | None = None,
) -> tuple[int, dict[str, Any]]:
    slot_end = _coerce_slot(args.slot) if args.slot else _latest_complete_production_slot()
    production_marker = _production_marker(slot_end)
    if not bool(production_marker.get("complete")):
        raise ValueError("The selected production slot is incomplete; parity would be ambiguous.")
    if not 0 < float(args.min_coverage) <= 1:
        raise ValueError("--min-coverage must be in (0, 1].")
    if int(args.slot_retry_attempts) < common.MIN_NO_CANDLE_FETCH_ATTEMPTS - 1:
        raise ValueError(
            "--slot-retry-attempts must preserve the three-observation no-candle policy."
        )
    pair_lookback = int(
        getattr(args, "oi_pair_lookback_minutes", DEFAULT_OI_PAIR_LOOKBACK_MINUTES)
    )
    if pair_lookback < 10 or pair_lookback % 5:
        raise ValueError("--oi-pair-lookback-minutes must be a multiple of 5 and at least 10.")

    common.publish_status(
        SESSION,
        "RUNNING",
        phase="WAIT_PRIMARY_IDLE",
        target_slot=slot_end.isoformat(),
        output_root=args.output_root,
    )
    if args.skip_primary_idle_check:
        idle_evidence = {
            "primary_phase": "CHECK_SKIPPED",
            "primary_heartbeat_ts": "",
            "seconds_to_next_boundary_at_start": _seconds_until_next_boundary(
                common.now_ist()
            ),
            "waited_sec": 0.0,
        }
    else:
        idle_evidence = wait_for_primary_idle(
            slot_end,
            minimum_seconds_before_boundary=args.minimum_seconds_before_boundary,
            timeout_sec=args.wait_timeout_sec,
        )
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="WAIT_STRATEGY_JOB_FENCE",
        target_slot=slot_end.isoformat(),
    )
    strategy_job_fence = wait_for_strategy_job_fence(
        slot_end,
        not_before_seconds=float(
            getattr(
                args,
                "strategy_job_fence_seconds",
                DEFAULT_STRATEGY_JOB_FENCE_SECONDS,
            )
        ),
    )

    full_universe = common.load_near_month_universe(expected_date=slot_end.date())
    scope = build_shadow_scope(
        full_universe,
        slot_end,
        canary_count=int(getattr(args, "canary_count", DEFAULT_CANARY_COUNT)),
    )
    validate_production_scope_contract(scope, production_marker)
    universe = scope.universe

    auth_started = time.monotonic()
    if lane_session is None:
        lanes, auth_failures = build_app_lanes(args)
        auth_reused = False
    else:
        lanes, auth_failures, auth_reused = lane_session.acquire(args)
    auth_duration = time.monotonic() - auth_started
    if auth_reused:
        print(
            f"[SHADOW][AUTH] reusing session pool apps={len(lanes)}",
            flush=True,
        )
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="FETCH",
        target_slot=slot_end.isoformat(),
        contracts_expected=len(universe),
        full_stock_contracts=scope.full_stock_contracts,
        scope_mode=scope.mode,
        apps=len(lanes),
    )

    slot_started = time.monotonic()
    cpu_started = time.process_time()
    outcomes, attempts, no_candle_observations, retries_used = fetch_with_quality_retries(
        universe,
        lanes,
        slot_end,
        args,
    )
    if lane_session is not None:
        lane_session.invalidate_runtime_auth_failures()
    fetch_duration = time.monotonic() - slot_started
    shadow_rows = rows_from_outcomes(outcomes)
    shadow_pair_rows = oi_pair_rows_from_outcomes(outcomes)
    shadow_evidence = build_oi_evidence(universe, shadow_pair_rows, slot_end)
    output_root = Path(args.output_root).resolve()
    run_id, run_dir = _make_run_dir(output_root, slot_end)
    data_path = run_dir / "shadow_slot.parquet"
    oi_evidence_path = run_dir / "shadow_oi_evidence.parquet"
    persist_started = time.monotonic()
    common.atomic_write_parquet(shadow_rows, data_path)
    common.atomic_write_parquet(shadow_evidence, oi_evidence_path)
    persist_duration = time.monotonic() - persist_started
    fetch_persist_duration = time.monotonic() - slot_started

    marker = build_quality_marker(
        slot_end,
        universe,
        outcomes,
        attempts,
        no_candle_observations,
        lanes,
        args,
        retries_used=retries_used,
    )
    apply_scope_metadata(marker, scope, outcomes, shadow_evidence, slot_end)
    compare_started = time.monotonic()
    production_pair_rows = load_production_oi_pair_rows(universe, slot_end)
    target = _slot_timestamp(slot_end)
    production_rows = production_pair_rows.loc[
        common._to_ist(production_pair_rows["timestamp"]).eq(target)
    ].reset_index(drop=True)
    comparison, mismatches = compare_with_production(
        universe,
        shadow_rows,
        production_rows,
        marker,
        production_marker,
    )
    production_evidence = build_oi_evidence(universe, production_pair_rows, slot_end)
    oi_comparison, oi_mismatches = compare_oi_evidence(
        universe,
        shadow_evidence,
        production_evidence,
    )
    strategy_oi_parity = bool(
        comparison.get("oi_identity_parity")
        and oi_comparison.get("strategy_oi_parity")
    )
    comparison["current_oi_identity_parity"] = bool(
        comparison.get("oi_identity_parity")
    )
    comparison["strategy_oi_parity"] = strategy_oi_parity
    comparison["oi_quality_parity"] = strategy_oi_parity
    comparison["oi_evidence"] = oi_comparison
    comparison_duration = time.monotonic() - compare_started
    mismatch_path = run_dir / "parity_mismatches.csv"
    oi_mismatch_path = run_dir / "oi_parity_mismatches.csv"
    common.atomic_write_csv(mismatches, mismatch_path)
    common.atomic_write_csv(oi_mismatches, oi_mismatch_path)

    primary_duration = float(production_marker.get("duration_sec") or 0.0)
    marker.update(
        {
            "run_id": run_id,
            "session": SESSION,
            "output_root": str(output_root),
            "data_path": str(data_path),
            "oi_evidence_path": str(oi_evidence_path),
            "mismatch_path": str(mismatch_path),
            "oi_mismatch_path": str(oi_mismatch_path),
            "production_marker_path": str(common.fetch_slot_path(slot_end)),
            "production_duration_sec": primary_duration,
            "auth_duration_sec": auth_duration,
            "fetch_duration_sec": fetch_duration,
            "persist_duration_sec": persist_duration,
            "fetch_persist_duration_sec": fetch_persist_duration,
            "comparison_duration_sec": comparison_duration,
            "total_duration_sec": time.monotonic() - slot_started,
            "cpu_duration_sec": time.process_time() - cpu_started,
            "speedup_vs_production": (
                float(primary_duration / fetch_persist_duration)
                if primary_duration > 0 and fetch_persist_duration > 0
                else None
            ),
            "primary_idle_evidence": idle_evidence,
            "strategy_job_fence": strategy_job_fence,
            "auth_failures": auth_failures,
            "comparison": comparison,
        }
    )
    admitted = bool(marker.get("complete")) and strategy_oi_parity
    marker["state"] = "SUCCESS" if admitted else "PARTIAL"
    marker["parity_complete"] = strategy_oi_parity
    marker["strategy_oi_parity"] = strategy_oi_parity
    marker["diagnostic_exact_candle_parity"] = bool(
        comparison.get("quality_parity")
    )
    marker["full_stock_parity"] = bool(scope.strategy_slot and admitted)
    marker["strategy_validation_ready"] = bool(
        scope.strategy_slot and slot_end.time() != FIRST_SLOT and admitted
    )
    marker["strategy_ready"] = False
    marker["strategy_baseline_ready"] = bool(
        scope.strategy_slot and slot_end.time() == FIRST_SLOT and admitted
    )
    if scope.mode == SCOPE_ROTATING_CANARY:
        marker["validation_result"] = "SAMPLE_OI_PASS" if admitted else "SAMPLE_OI_FAIL"
    elif slot_end.time() == FIRST_SLOT:
        marker["validation_result"] = (
            "FULL_STOCK_BASELINE_OI_PASS" if admitted else "FULL_STOCK_BASELINE_OI_FAIL"
        )
    else:
        marker["validation_result"] = (
            "FULL_STOCK_STRATEGY_OI_PASS" if admitted else "FULL_STOCK_STRATEGY_OI_FAIL"
        )
    marker_path = run_dir / "shadow_marker.json"
    marker["marker_path"] = str(marker_path)
    common.atomic_write_json(marker_path, marker)
    common.atomic_write_json(
        output_root / "latest_fast_shadow.json",
        {
            "schema_version": SHADOW_SCHEMA_VERSION,
            "run_id": run_id,
            "slot_ist": slot_end.isoformat(),
            "marker_path": str(marker_path),
            "data_path": str(data_path),
            "oi_evidence_path": str(oi_evidence_path),
            "state": marker["state"],
            "quality_parity": comparison["quality_parity"],
            "strategy_oi_parity": strategy_oi_parity,
            "scope_mode": scope.mode,
            "validation_result": marker["validation_result"],
            "strategy_ready": marker["strategy_ready"],
            "strategy_validation_ready": marker["strategy_validation_ready"],
            "validation_only": True,
            "strategy_authority": False,
            "published_at_ist": common.now_ist().isoformat(timespec="seconds"),
        },
    )
    common.atomic_write_text(
        common.LATEST_DIR / "latest_fno_oi_fast_shadow.md",
        render_shadow_report(marker),
    )
    common.publish_status(
        SESSION,
        marker["state"],
        heartbeat_state="DONE",
        phase="DONE",
        slot=slot_end.isoformat(),
        contracts_written=marker["contracts_written"],
        contracts_expected=marker["contracts_expected"],
        quality_parity=comparison["quality_parity"],
        strategy_oi_parity=strategy_oi_parity,
        scope_mode=scope.mode,
        validation_result=marker["validation_result"],
        strategy_ready=marker["strategy_ready"],
        strategy_validation_ready=marker["strategy_validation_ready"],
        fetch_persist_duration_sec=f"{fetch_persist_duration:.3f}",
        speedup=f"{marker['speedup_vs_production']:.3f}"
        if marker["speedup_vs_production"] is not None
        else "",
        output=marker_path,
    )
    print(
        f"[SHADOW][{marker['state']}] slot={slot_end.strftime('%H:%M')} "
        f"scope={scope.mode} "
        f"written={marker['contracts_written']}/{marker['contracts_expected']} "
        f"fetch+persist={fetch_persist_duration:.3f}s "
        f"production={primary_duration:.3f}s "
        f"speedup={float(marker['speedup_vs_production'] or 0.0):.2f}x "
        f"oi={oi_comparison['exact_match_symbols']}/{oi_comparison['symbols_compared']} "
        f"exact={comparison['exact_match_symbols']}/{comparison['universe_symbols']} "
        f"mismatches={comparison['field_mismatch_count']}",
        flush=True,
    )
    return (0 if admitted else 2), marker


def continuous_session_floor(
    production_slots: Iterable[datetime],
    observed_shadow_slots: Iterable[datetime],
) -> datetime | None:
    """Choose a restart-safe floor without backfilling slots before activation."""
    observed = list(observed_shadow_slots)
    if observed:
        return min(observed)
    production = list(production_slots)
    return max(production) if production else None


def run_continuous(args: argparse.Namespace) -> int:
    """Compare each new complete production slot once, then wait for the next one."""
    current = common.now_ist()
    session_date = (
        date.fromisoformat(args.session_date) if args.session_date else current.date()
    )
    holidays = common.load_holidays()
    if (
        not args.allow_non_trading_day
        and not common.is_trading_day(session_date, holidays)
    ):
        common.publish_status(
            SESSION,
            "SKIPPED_NON_TRADING_DAY",
            heartbeat_state="SKIPPED_NON_TRADING_DAY",
            phase="NON_TRADING_DAY",
            session_date_ist=session_date.isoformat(),
        )
        return 0

    output_root = Path(args.output_root).resolve()
    observed, successful = shadow_slot_history(output_root, session_date)
    production = complete_production_slots(session_date)
    floor = continuous_session_floor(production, observed)
    last_success = max(successful) if successful else None
    end_deadline = datetime.combine(session_date, LAST_SLOT, tzinfo=common.IST) + timedelta(
        minutes=max(1.0, float(args.end_grace_min))
    )
    common.publish_status(
        SESSION,
        "RUNNING",
        heartbeat_state="RUNNING",
        phase="WAIT_PRODUCTION_SLOT",
        session_date_ist=session_date.isoformat(),
        processed_slots=len(successful),
        last_slot=last_success.isoformat() if last_success else "",
        activation_floor=floor.isoformat() if floor else "",
        continuous=True,
    )
    print(
        f"[SHADOW][SESSION] date={session_date.isoformat()} continuous=1 "
        f"processed={len(successful)} floor={floor.isoformat() if floor else 'pending'}",
        flush=True,
    )
    lane_session = AppLaneSession()

    while True:
        current = common.now_ist()
        if current.date() != session_date or current >= end_deadline:
            common.publish_status(
                SESSION,
                "DONE",
                heartbeat_state="DONE",
                phase="END_TIME",
                session_date_ist=session_date.isoformat(),
                processed_slots=len(successful),
                last_slot=last_success.isoformat() if last_success else "",
            )
            print(
                f"[SHADOW][SESSION][DONE] processed={len(successful)} "
                f"last={last_success.isoformat() if last_success else 'none'}",
                flush=True,
            )
            return 0

        production = complete_production_slots(session_date)
        if floor is None and production:
            # On a late first activation, start at the latest available marker.
            # After that, the persisted shadow history makes restarts catch up.
            floor = max(production)
        pending = [
            slot
            for slot in production
            if floor is not None and slot >= floor and slot not in observed
        ]
        if not pending:
            latest_production = max(production) if production else None
            common.publish_heartbeat(
                SESSION,
                "RUNNING",
                phase="WAIT_PRODUCTION_SLOT",
                session_date_ist=session_date.isoformat(),
                processed_slots=len(successful),
                last_slot=last_success.isoformat() if last_success else "",
                production_slot=(
                    latest_production.isoformat() if latest_production else ""
                ),
                activation_floor=floor.isoformat() if floor else "",
            )
            time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
            continue

        target = pending[0]
        slot_args = argparse.Namespace(**vars(args))
        slot_args.slot = target.isoformat()
        exit_code, marker = run_shadow(slot_args, lane_session=lane_session)
        observed.add(target)
        if exit_code == 0:
            successful.add(target)
            last_success = target
            next_phase = "WAIT_PRODUCTION_SLOT"
            runtime_state = "RUNNING"
        else:
            # Preserve the immutable failure evidence, but do not refetch the
            # entire universe indefinitely. A parity failure is an audit result,
            # not missing production data, and validation traffic must not
            # compete with the next live boundary.
            next_phase = "PARTIAL_SLOT_RECORDED"
            runtime_state = "PARTIAL"

        comparison = marker.get("comparison")
        comparison = comparison if isinstance(comparison, Mapping) else {}
        common.publish_status(
            SESSION,
            runtime_state,
            heartbeat_state="RUNNING",
            phase=next_phase,
            session_date_ist=session_date.isoformat(),
            slot=target.isoformat(),
            last_slot=last_success.isoformat() if last_success else "",
            processed_slots=len(successful),
            contracts_written=marker.get("contracts_written", 0),
            contracts_expected=marker.get("contracts_expected", 0),
            quality_parity=bool(comparison.get("quality_parity")),
            strategy_oi_parity=bool(comparison.get("strategy_oi_parity")),
            scope_mode=marker.get("scope_mode", ""),
            validation_result=marker.get("validation_result", ""),
            fetch_persist_duration_sec=f"{float(marker.get('fetch_persist_duration_sec') or 0.0):.3f}",
            speedup=(
                f"{float(marker['speedup_vs_production']):.3f}"
                if marker.get("speedup_vs_production") is not None
                else ""
            ),
            output=marker.get("marker_path", ""),
        )
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an isolated fast FnO REST shadow and compare it with production."
    )
    parser.add_argument("--slot", default="")
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--session-date", default="")
    parser.add_argument("--allow-non-trading-day", action="store_true")
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--partial-retry-sec", type=float, default=30.0)
    parser.add_argument("--end-grace-min", type=float, default=4.0)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--workers-per-app", type=int, default=2)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--slot-retry-attempts", type=int, default=2)
    parser.add_argument("--slot-retry-delay-sec", type=float, default=2.0)
    parser.add_argument("--canary-count", type=int, default=DEFAULT_CANARY_COUNT)
    parser.add_argument(
        "--oi-pair-lookback-minutes",
        type=int,
        default=DEFAULT_OI_PAIR_LOOKBACK_MINUTES,
    )
    parser.add_argument(
        "--strategy-job-fence-seconds",
        type=float,
        default=DEFAULT_STRATEGY_JOB_FENCE_SECONDS,
    )
    parser.add_argument("--min-coverage", type=float, default=common.MIN_STOCK_FUTURES_COVERAGE)
    parser.add_argument("--minimum-seconds-before-boundary", type=float, default=90.0)
    parser.add_argument("--wait-timeout-sec", type=float, default=900.0)
    parser.add_argument("--skip-primary-idle-check", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.continuous:
            return run_continuous(args)
        exit_code, _ = run_shadow(args)
        return exit_code
    except KeyboardInterrupt:
        common.publish_status(
            SESSION,
            "STOPPED",
            heartbeat_state="STOPPED",
            phase="INTERRUPTED",
        )
        return 130
    except Exception as exc:
        common.publish_status(
            SESSION,
            "FAILED",
            heartbeat_state="CRASHED",
            phase="FAILED",
            error=f"{type(exc).__name__}: {exc}",
        )
        print(f"[SHADOW][FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
