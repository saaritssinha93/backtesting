"""Opt-in fast production writer for the canonical FnO 5-minute archive.

This entrypoint changes only the live slot execution engine.  It reuses the
validated per-app lane scheduler from the fast shadow, while delegating slot
readiness and marker construction to :mod:`fno_oi_fetch_5min`.  Consequently
the durable contract files, raw schema, reports, and final v2 marker semantics
remain identical to the legacy producer.

The legacy ``fno_oi_fetch_5min.py`` entrypoint remains an independent fallback.
Only one of the two production writers may run at a time.
"""

from __future__ import annotations

import argparse
import math
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Mapping

import pandas as pd

import fno_oi_common as common
import fno_oi_fetch_5min as legacy
import fno_oi_fetch_5min_fast_shadow as fast_core


SESSION = "fno_oi_fetch_5min_fast_production"
ENGINE_VERSION = "fno_oi_fast_production_engine_v1"
DEFAULT_WORKERS_PER_APP = 2
DEFAULT_WRITER_WORKERS = 8
ARCHIVE_VERIFY_FIELDS = (
    "timestamp",
    "candle_start",
    "underlying",
    "tradingsymbol",
    "instrument_token",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "quality_state",
    "source",
    "data_version",
)


def _slot_timestamp(value: datetime | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def _validate_live_window(
    from_dt: datetime,
    to_dt: datetime,
    slot_end: datetime | None,
    from_column: str,
) -> pd.Timestamp:
    """Reject bootstrap or broad-range use of the exact-slot fast path."""
    if slot_end is None:
        raise ValueError("Fast production fetch requires an exact live slot.")
    if from_column:
        raise ValueError("Fast production fetch cannot be used for bootstrap ranges.")
    target = _slot_timestamp(slot_end)
    observed_from = _slot_timestamp(from_dt)
    observed_to = _slot_timestamp(to_dt)
    if observed_from != target - pd.Timedelta(minutes=5) or observed_to != target:
        raise ValueError(
            "Fast production fetch requires the canonical [S-5m, S] window: "
            f"from={observed_from} to={observed_to} slot={target}"
        )
    return target


def _values_equal(left: Any, right: Any) -> bool:
    try:
        if pd.isna(left) and pd.isna(right):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(left, (datetime, pd.Timestamp)) or isinstance(
        right, (datetime, pd.Timestamp)
    ):
        try:
            return _slot_timestamp(pd.Timestamp(left)) == _slot_timestamp(
                pd.Timestamp(right)
            )
        except (TypeError, ValueError):
            return False
    if isinstance(left, float) or isinstance(right, float):
        try:
            return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=0.0)
        except (TypeError, ValueError):
            return False
    return left == right


def _verify_archived_row(
    combined: pd.DataFrame,
    incoming: pd.DataFrame,
    target: pd.Timestamp,
) -> None:
    if combined.empty or incoming.empty:
        raise ValueError("Archive append returned no exact-slot evidence.")
    archived_timestamps = common._to_ist(combined["timestamp"])
    archived = combined.loc[archived_timestamps.eq(target)]
    incoming_timestamps = common._to_ist(incoming["timestamp"])
    expected = incoming.loc[incoming_timestamps.eq(target)]
    if len(archived) != 1 or len(expected) != 1:
        raise ValueError(
            "Archive must contain exactly one row for the instrument and slot: "
            f"archived={len(archived)} incoming={len(expected)}"
        )
    archived_row = archived.iloc[0]
    expected_row = expected.iloc[0]
    missing = [
        field
        for field in ARCHIVE_VERIFY_FIELDS
        if field not in archived.columns or field not in expected.columns
    ]
    if missing:
        raise ValueError(f"Archive verification fields are missing: {missing}")
    mismatches = [
        field
        for field in ARCHIVE_VERIFY_FIELDS
        if not _values_equal(archived_row[field], expected_row[field])
    ]
    if mismatches:
        raise ValueError(f"Archive exact-slot readback differs: {mismatches}")


@dataclass
class CanonicalArchiveCache:
    """Session-local canonical histories, advanced only after atomic success."""

    frames: dict[str, pd.DataFrame]
    _locks: dict[str, threading.Lock] = field(default_factory=dict)
    _locks_guard: threading.Lock = field(default_factory=threading.Lock)

    @classmethod
    def preload(
        cls,
        universe: pd.DataFrame,
        *,
        workers: int = DEFAULT_WRITER_WORKERS,
        session: str = SESSION,
    ) -> "CanonicalArchiveCache":
        started = time.monotonic()
        symbols = sorted(
            {
                str(value).strip().upper()
                for value in universe["tradingsymbol"].dropna()
                if str(value).strip()
            }
        )
        common.publish_heartbeat(
            session,
            "RUNNING",
            phase="PRELOAD_ARCHIVE_CACHE",
            contracts_expected=len(symbols),
            cache_workers=max(1, int(workers)),
            engine=ENGINE_VERSION,
        )

        def _load(symbol: str) -> tuple[str, pd.DataFrame]:
            path = common.raw_contract_path(symbol)
            frame = (
                pd.read_parquet(path)
                if path.exists()
                else pd.DataFrame(columns=list(common.RAW_COLUMNS))
            )
            return symbol, frame

        frames: dict[str, pd.DataFrame] = {}
        worker_count = max(1, min(int(workers), max(1, len(symbols)), 32))
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="fno-oi-fast-production-cache",
        ) as pool:
            futures = [pool.submit(_load, symbol) for symbol in symbols]
            for future in as_completed(futures):
                symbol, frame = future.result()
                frames[symbol] = frame
        print(
            f"[FAST-PROD][CACHE] loaded={len(frames)} "
            f"duration={time.monotonic() - started:.3f}s workers={worker_count}",
            flush=True,
        )
        return cls(frames=frames)

    def _lock_for(self, symbol: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(symbol, threading.Lock())

    def append(
        self,
        symbol: str,
        incoming: pd.DataFrame,
        target: pd.Timestamp,
    ) -> pd.DataFrame:
        normalized_symbol = str(symbol).strip().upper()
        with self._lock_for(normalized_symbol):
            existing = self.frames.get(normalized_symbol)
            combined = common.merge_contract_rows(existing, incoming)
            _verify_archived_row(combined, incoming, target)
            common.atomic_write_parquet(
                combined, common.raw_contract_path(normalized_symbol)
            )
            # Do not advance memory before the atomic replace succeeds.
            self.frames[normalized_symbol] = combined
            return combined


def _persist_outcome(
    outcome: Mapping[str, Any],
    target: pd.Timestamp,
    archive_cache: CanonicalArchiveCache | None = None,
) -> dict[str, Any]:
    result = dict(outcome)
    state = str(result.get("state", ""))
    if state not in {"WRITTEN", "INVALID_DATA"}:
        return result
    frame = result.get("_frame")
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        result.update(
            {
                "state": "FAILED",
                "rows": 0,
                "valid_rows": 0,
                "error": "ArchivePersistenceError: fetched row frame is empty",
            }
        )
        return result
    symbol = str(result.get("tradingsymbol", "")).strip().upper()
    try:
        combined = (
            archive_cache.append(symbol, frame, target)
            if archive_cache is not None
            else common.append_contract_rows(common.raw_contract_path(symbol), frame)
        )
        _verify_archived_row(combined, frame, target)
    except Exception as exc:
        result.update(
            {
                "state": "FAILED",
                "rows": 0,
                "valid_rows": 0,
                "error": f"ArchivePersistenceError: {type(exc).__name__}: {exc}",
            }
        )
    return result


def _stream_fetch_and_persist(
    contracts: list[Mapping[str, Any]],
    runtimes: list[fast_core.AppLane],
    target: pd.Timestamp,
    *,
    max_retries: int,
    writer_workers: int,
    attempt_history: dict[str, list[str]],
    session: str,
    phase: str,
    assignments: list[tuple[Mapping[str, Any], fast_core.AppLane]] | None = None,
    archive_cache: CanonicalArchiveCache | None = None,
) -> list[dict[str, Any]]:
    """Pipeline API outcomes into bounded writers backed by the warm cache."""
    pipeline_started = time.monotonic()
    writer_count = max(1, min(int(writer_workers), max(1, len(contracts)), 32))
    writer_capacity = writer_count * 2
    writer_slots = threading.Semaphore(writer_capacity)
    result_lock = threading.Lock()
    history_lock = threading.Lock()
    immediate: list[dict[str, Any]] = []
    writer_jobs: list[tuple[Any, dict[str, Any]]] = []
    persist_announced = False
    pending_writes = 0
    max_pending_writes = 0

    def _writer_done(_future: Any) -> None:
        nonlocal pending_writes
        with result_lock:
            pending_writes -= 1
        writer_slots.release()

    with ThreadPoolExecutor(
        max_workers=writer_count,
        thread_name_prefix="fno-oi-fast-production-writer",
    ) as writer_pool:

        def _accept(outcome: Mapping[str, Any]) -> None:
            nonlocal persist_announced, pending_writes, max_pending_writes
            result = dict(outcome)
            symbol = str(result.get("tradingsymbol", "")).strip().upper()
            app = str(result.get("app", "")).strip()
            if symbol and app:
                with history_lock:
                    attempt_history.setdefault(symbol, []).append(app)
            if str(result.get("state", "")) not in {"WRITTEN", "INVALID_DATA"}:
                with result_lock:
                    immediate.append(result)
                return

            announce = False
            with result_lock:
                if not persist_announced:
                    persist_announced = True
                    announce = True
            if announce:
                common.publish_heartbeat(
                    session,
                    "RUNNING",
                    phase=f"{phase}_PIPELINE",
                    slot=target.isoformat(),
                    writer_mode="CACHED_BOUNDED_POOL",
                    writer_workers=writer_count,
                    writer_queue_capacity=writer_capacity,
                    engine=ENGINE_VERSION,
                )
            writer_slots.acquire()
            try:
                future = writer_pool.submit(
                    _persist_outcome, result, target, archive_cache
                )
            except BaseException:
                writer_slots.release()
                raise
            with result_lock:
                pending_writes += 1
                max_pending_writes = max(max_pending_writes, pending_writes)
                writer_jobs.append((future, result))
            future.add_done_callback(_writer_done)

        if assignments is None:
            work: queue.Queue[Mapping[str, Any]] = queue.Queue()
            for contract in contracts:
                work.put(contract)

            def _network_worker(lane: fast_core.AppLane, client: Any) -> None:
                while True:
                    if lane._runtime_auth_failure.is_set():
                        return
                    try:
                        contract = work.get_nowait()
                    except queue.Empty:
                        return
                    try:
                        _accept(
                            fast_core.fetch_one_contract(
                                lane,
                                client,
                                contract,
                                target.to_pydatetime(),
                                max_retries=max_retries,
                                lookback_minutes=5,
                                require_oi_pair=False,
                            )
                        )
                    finally:
                        work.task_done()
                    if lane._runtime_auth_failure.is_set():
                        return

            workers = [
                (lane, client)
                for lane in runtimes
                for client in lane.clients
            ]
        else:
            lane_work: dict[str, queue.Queue[Mapping[str, Any]]] = {}
            lane_map: dict[str, fast_core.AppLane] = {}
            for contract, lane in assignments:
                lane_map[lane.app_name] = lane
                lane_work.setdefault(lane.app_name, queue.Queue()).put(contract)

            def _network_worker(lane: fast_core.AppLane, client: Any) -> None:
                work = lane_work[lane.app_name]
                while True:
                    if lane._runtime_auth_failure.is_set():
                        return
                    try:
                        contract = work.get_nowait()
                    except queue.Empty:
                        return
                    try:
                        _accept(
                            fast_core.fetch_one_contract(
                                lane,
                                client,
                                contract,
                                target.to_pydatetime(),
                                max_retries=max_retries,
                                lookback_minutes=5,
                                require_oi_pair=False,
                            )
                        )
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
            thread_name_prefix="fno-oi-fast-production-fetch",
        ) as network_pool:
            network_futures = [
                network_pool.submit(_network_worker, lane, client)
                for lane, client in workers
            ]
            for future in as_completed(network_futures):
                future.result()
        network_done = time.monotonic()

    pipeline_done = time.monotonic()
    finalized = list(immediate)
    for future, original in writer_jobs:
        try:
            finalized.append(future.result())
        except Exception as exc:
            finalized.append(
                {
                    **original,
                    "state": "FAILED",
                    "rows": 0,
                    "valid_rows": 0,
                    "error": (
                        f"ArchivePersistenceError: {type(exc).__name__}: {exc}"
                    ),
                }
            )
    print(
        f"[FAST-PROD][PIPELINE] phase={phase} contracts={len(contracts)} "
        f"network={network_done - pipeline_started:.3f}s "
        f"writer_drain={pipeline_done - network_done:.3f}s "
        f"total={pipeline_done - pipeline_started:.3f}s "
        f"mode=CACHED_BOUNDED_POOL lane_workers={len(workers)} "
        f"writers={writer_count} max_pending={max_pending_writes}/{writer_capacity}",
        flush=True,
    )
    return sorted(finalized, key=lambda item: str(item.get("tradingsymbol", "")))


def fetch_contracts_fast(
    universe: pd.DataFrame,
    runtimes: list[fast_core.AppLane],
    from_dt: datetime,
    to_dt: datetime,
    *,
    slot_end: datetime | None,
    max_retries: int,
    phase: str,
    from_column: str = "",
    session: str = SESSION,
    writer_workers: int = DEFAULT_WRITER_WORKERS,
    attempted_apps_by_symbol: dict[str, list[str]] | None = None,
    archive_cache: CanonicalArchiveCache | None = None,
) -> list[dict[str, Any]]:
    """Fetch and durably append a full exact slot through a bounded pipeline."""
    target = _validate_live_window(from_dt, to_dt, slot_end, from_column)
    if not runtimes:
        raise RuntimeError("Fast production has no authenticated Kite app lanes.")
    ordered = universe.sort_values(
        ["tradingsymbol", "instrument_token"], kind="stable"
    )
    common.publish_heartbeat(
        session,
        "RUNNING",
        phase=phase,
        slot=target.isoformat(),
        contracts_expected=len(ordered),
        apps=len(runtimes),
        engine=ENGINE_VERSION,
    )
    contracts = ordered.to_dict("records")
    attempt_history = (
        attempted_apps_by_symbol
        if attempted_apps_by_symbol is not None
        else {}
    )
    assignments: list[tuple[Mapping[str, Any], fast_core.AppLane]] | None = None
    if str(phase).upper().startswith("FETCH_SLOT_RETRY_"):
        assignments = []
        for contract in contracts:
            symbol = str(contract.get("tradingsymbol", "")).strip().upper()
            assignments.append(
                (
                    contract,
                    fast_core._choose_retry_lane(
                        runtimes, attempt_history.get(symbol, [])
                    ),
                )
            )
    return _stream_fetch_and_persist(
        contracts,
        runtimes,
        target,
        max_retries=max_retries,
        writer_workers=writer_workers,
        attempt_history=attempt_history,
        session=session,
        phase=phase,
        assignments=assignments,
        archive_cache=archive_cache,
    )


@dataclass
class FastSlotFetcher:
    """Stateful adapter retaining per-contract app history across slot retries."""

    writer_workers: int = DEFAULT_WRITER_WORKERS
    archive_cache: CanonicalArchiveCache | None = None
    attempted_apps_by_symbol: dict[str, list[str]] = field(default_factory=dict)

    def __call__(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        kwargs["writer_workers"] = max(1, int(self.writer_workers))
        kwargs["attempted_apps_by_symbol"] = self.attempted_apps_by_symbol
        kwargs["archive_cache"] = self.archive_cache
        return fetch_contracts_fast(*args, **kwargs)


def _bootstrap_runtimes(
    lanes: list[fast_core.AppLane],
) -> list[legacy.AppRuntime]:
    """Use one already-authenticated client per app for legacy range bootstrap."""
    return [
        legacy.AppRuntime(
            app_name=lane.app_name,
            client=lane.clients[0],
            pace_seconds=lane.pace_seconds,
        )
        for lane in lanes
    ]


def run_fast_slot(
    slot_end: datetime,
    universe: pd.DataFrame,
    lanes: list[fast_core.AppLane],
    args: argparse.Namespace,
    *,
    archive_cache: CanonicalArchiveCache | None = None,
) -> dict[str, Any]:
    fetch_impl = FastSlotFetcher(
        writer_workers=max(1, int(args.writer_workers)),
        archive_cache=archive_cache,
    )
    marker = legacy.run_slot(
        slot_end,
        universe,
        lanes,  # AppLane intentionally satisfies the app_name contract.
        args,
        fetch_contracts_impl=fetch_impl,
        session=SESSION,
    )
    canonical_report = common.LATEST_DIR / "latest_fno_oi_fetch.md"
    if canonical_report.exists():
        common.atomic_write_text(
            common.LATEST_DIR / "latest_fno_oi_fast_production.md",
            canonical_report.read_text(encoding="utf-8"),
        )
    return marker


def build_parser() -> argparse.ArgumentParser:
    parser = legacy.build_parser()
    parser.description = (
        "Fetch the full near-month NFO-FUT universe with the fast lane engine "
        "and write the canonical production archive and v2 slot marker."
    )
    parser.add_argument(
        "--workers-per-app",
        type=int,
        default=DEFAULT_WORKERS_PER_APP,
        help="Concurrent clients per Kite app; request starts remain app-rate-limited.",
    )
    parser.add_argument(
        "--writer-workers",
        type=int,
        default=DEFAULT_WRITER_WORKERS,
        help="Concurrent canonical per-contract archive writers.",
    )
    return parser


def run_session(args: argparse.Namespace) -> int:
    holidays = common.load_holidays()
    current = common.now_ist()
    session_date = (
        date.fromisoformat(args.session_date) if args.session_date else current.date()
    )
    if (
        not args.allow_non_trading_day
        and not common.is_trading_day(session_date, holidays)
    ):
        common.publish_status(
            SESSION,
            "SKIPPED_NON_TRADING_DAY",
            session_date_ist=session_date.isoformat(),
        )
        return 0
    if not 0 < float(args.min_coverage) <= 1:
        raise ValueError("--min-coverage must be in (0, 1].")
    if float(args.min_coverage) < common.MIN_STOCK_FUTURES_COVERAGE:
        raise ValueError(
            "--min-coverage cannot be below the locked stock-futures floor "
            f"of {common.MIN_STOCK_FUTURES_COVERAGE:.2f}."
        )
    minimum_retries = common.MIN_NO_CANDLE_FETCH_ATTEMPTS - 1
    if int(args.slot_retry_attempts) < minimum_retries:
        raise ValueError(
            "--slot-retry-attempts must allow the locked number of clean "
            f"no-candle observations (minimum {minimum_retries})."
        )
    if int(args.workers_per_app) < 1:
        raise ValueError("--workers-per-app must be at least 1.")
    if int(args.writer_workers) < 1:
        raise ValueError("--writer-workers must be at least 1.")

    universe = legacy.ensure_universe(session_date, args)
    lane_session = fast_core.AppLaneSession()
    lanes, auth_failures, _ = lane_session.acquire(args)
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="START",
        session_date_ist=session_date.isoformat(),
        contracts_expected=len(universe),
        apps=len(lanes),
        workers_per_app=int(args.workers_per_app),
        writer_workers=int(args.writer_workers),
        auth_failures=len(auth_failures),
        engine=ENGINE_VERSION,
    )
    if not args.no_bootstrap:
        legacy.run_bootstrap(
            universe,
            _bootstrap_runtimes(lanes),
            args,
            holidays,
            session=SESSION,
        )
    archive_cache = CanonicalArchiveCache.preload(
        universe,
        workers=int(args.writer_workers),
        session=SESSION,
    )

    if args.once:
        slot = (
            legacy._coerce_slot(args.slot)
            if args.slot
            else legacy.latest_completed_slot(common.now_ist(), holidays)
        )
        if slot is None:
            raise RuntimeError("No completed five-minute slot is available.")
        if slot.date() != session_date and not args.allow_non_trading_day:
            raise ValueError(
                f"Requested slot {slot.isoformat()} is not on session date {session_date}."
            )
        marker = run_fast_slot(
            slot, universe, lanes, args, archive_cache=archive_cache
        )
        lane_session.invalidate_runtime_auth_failures()
        return 0 if bool(marker.get("complete")) else 2

    processed = legacy._today_processed_slots(session_date)
    end_deadline = datetime.combine(
        session_date, legacy.LAST_SLOT, tzinfo=common.IST
    ) + timedelta(minutes=3)
    while True:
        current = common.now_ist()
        if current.date() != session_date or current >= end_deadline:
            common.publish_status(
                SESSION,
                "DONE",
                phase="END_TIME",
                session_date_ist=session_date.isoformat(),
                processed_slots=len(processed),
                engine=ENGINE_VERSION,
            )
            return 0
        slot = legacy.latest_completed_slot(current, holidays)
        if slot is None or slot.date() != session_date or slot.time() < legacy.FIRST_SLOT:
            common.publish_heartbeat(
                SESSION,
                "SCHEDULED",
                phase="WAIT_FIRST_SLOT",
                session_date_ist=session_date.isoformat(),
                engine=ENGINE_VERSION,
            )
            time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
            continue
        if slot.time() > legacy.LAST_SLOT:
            slot = datetime.combine(
                session_date, legacy.LAST_SLOT, tzinfo=common.IST
            )
        slot_key = slot.strftime("%H%M")
        due_at = slot + timedelta(
            seconds=max(0.0, float(args.boundary_buffer_sec))
        )
        if slot_key in processed or current < due_at:
            common.publish_heartbeat(
                SESSION,
                "WAITING",
                phase="WAIT_NEXT_SLOT",
                slot=slot.isoformat(),
                processed_slots=len(processed),
                engine=ENGINE_VERSION,
            )
            time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
            continue

        lanes, _, _ = lane_session.acquire(args)
        marker = run_fast_slot(
            slot, universe, lanes, args, archive_cache=archive_cache
        )
        lane_session.invalidate_runtime_auth_failures()
        if bool(marker.get("complete")):
            processed.add(slot_key)
        else:
            time.sleep(max(1.0, float(args.partial_retry_sec)))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run_session(args)
    except KeyboardInterrupt:
        common.publish_status(
            SESSION,
            "STOPPED",
            heartbeat_state="STOPPED",
            phase="INTERRUPTED",
        )
        return 0
    except Exception as exc:
        common.publish_status(
            SESSION,
            "FAILED",
            heartbeat_state="CRASHED",
            phase="FAILED",
            error=f"{type(exc).__name__}: {exc}",
            engine=ENGINE_VERSION,
        )
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
