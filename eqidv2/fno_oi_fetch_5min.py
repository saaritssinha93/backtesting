from __future__ import annotations

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Iterable

import pandas as pd

import fno_oi_common as common
import fno_oi_universe


SESSION = "fno_oi_fetch_5min"
FIRST_SLOT = dtime(9, 20)
LAST_SLOT = dtime(15, 30)


@dataclass
class AppRuntime:
    app_name: str
    client: Any
    pace_seconds: float
    _last_call_at: float = 0.0
    _pace_lock: threading.Lock = field(default_factory=threading.Lock)

    def pace(self) -> None:
        with self._pace_lock:
            wait = self.pace_seconds - (time.monotonic() - self._last_call_at)
            if wait > 0:
                time.sleep(wait)
            self._last_call_at = time.monotonic()


def _coerce_slot(value: str) -> datetime:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(common.IST)
    else:
        stamp = stamp.tz_convert(common.IST)
    return stamp.to_pydatetime()


def _previous_trading_day(day: date, holidays: set[date]) -> date:
    candidate = day - timedelta(days=1)
    while not common.is_trading_day(candidate, holidays):
        candidate -= timedelta(days=1)
    return candidate


def latest_completed_slot(moment: datetime, holidays: set[date]) -> datetime | None:
    current = moment.astimezone(common.IST)
    if not common.is_trading_day(current.date(), holidays):
        previous = _previous_trading_day(current.date(), holidays)
        return datetime.combine(previous, LAST_SLOT, tzinfo=common.IST)
    if current.time() < FIRST_SLOT:
        previous = _previous_trading_day(current.date(), holidays)
        return datetime.combine(previous, LAST_SLOT, tzinfo=common.IST)
    if current.time() >= LAST_SLOT:
        return datetime.combine(current.date(), LAST_SLOT, tzinfo=common.IST)
    minute = current.minute - (current.minute % 5)
    return current.replace(minute=minute, second=0, microsecond=0)


def _build_app_runtimes(args: argparse.Namespace) -> list[AppRuntime]:
    runtimes: list[AppRuntime] = []
    failures: list[str] = []
    for credential in common.discover_kite_credentials(max_apps=args.max_apps):
        try:
            client = common.make_kite_client(credential, timeout_sec=args.timeout_sec)
            profile = client.profile()
            user_name = str(profile.get("user_name") or profile.get("user_id") or "validated")
            print(f"[AUTH] {credential.app_name} validated for {user_name}", flush=True)
            runtimes.append(
                AppRuntime(
                    app_name=credential.app_name,
                    client=client,
                    pace_seconds=max(0.34, float(args.request_interval_sec)),
                )
            )
        except Exception as exc:
            failures.append(f"{credential.app_name}:{type(exc).__name__}:{exc}")
            print(
                f"[AUTH][WARN] {credential.app_name} unavailable: {type(exc).__name__}: {exc}",
                flush=True,
            )
    if not runtimes:
        raise RuntimeError("No authenticated Kite apps are usable: " + " | ".join(failures))
    return runtimes


def ensure_universe(session_date: date, args: argparse.Namespace) -> pd.DataFrame:
    try:
        return common.load_near_month_universe(expected_date=session_date)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[UNIVERSE] Refresh required: {exc}", flush=True)
        try:
            _, universe, _ = fno_oi_universe.refresh_universe(
                session_date,
                timeout_sec=args.timeout_sec,
                max_apps=args.max_apps,
            )
        except Exception as refresh_exc:
            common.publish_status(
                fno_oi_universe.SESSION,
                "FAILED",
                heartbeat_state="CRASHED",
                phase="AUTO_REFRESH_FAILED",
                session_date_ist=session_date.isoformat(),
                error=f"{type(refresh_exc).__name__}: {refresh_exc}",
            )
            raise
        return universe


def _historical_call(
    runtime: AppRuntime,
    contract: pd.Series,
    from_dt: datetime,
    to_dt: datetime,
    *,
    max_retries: int,
) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, max(1, int(max_retries)) + 1):
        try:
            runtime.pace()
            return runtime.client.historical_data(
                int(contract["instrument_token"]),
                from_dt,
                to_dt,
                "5minute",
                continuous=False,
                oi=True,
            )
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            if attempt >= max_retries:
                break
            if "429" in message or "too many requests" in message or "rate limit" in message:
                delay = max(2.0, 2.0**attempt)
            else:
                delay = min(8.0, 0.75 * (2 ** (attempt - 1)))
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def fetch_one_contract(
    runtime: AppRuntime,
    contract: pd.Series,
    from_dt: datetime,
    to_dt: datetime,
    *,
    slot_end: datetime | None,
    max_retries: int,
) -> dict[str, Any]:
    symbol = str(contract["tradingsymbol"])
    started = time.monotonic()
    try:
        records = _historical_call(
            runtime,
            contract,
            from_dt,
            to_dt,
            max_retries=max_retries,
        )
        rows = common.normalize_historical_candles(
            records,
            contract,
            fetch_timestamp=common.now_ist(),
            slot_end=slot_end,
        )
        if rows.empty:
            return {
                "tradingsymbol": symbol,
                "underlying": str(contract["underlying"]),
                "app": runtime.app_name,
                "state": "NO_CANDLE",
                "rows": 0,
                "elapsed_sec": time.monotonic() - started,
                "error": "",
            }
        valid_rows = int(rows["quality_state"].eq("VALID").sum())
        common.append_contract_rows(common.raw_contract_path(symbol), rows)
        return {
            "tradingsymbol": symbol,
            "underlying": str(contract["underlying"]),
            "app": runtime.app_name,
            "state": "WRITTEN" if valid_rows == len(rows) else "INVALID_DATA",
            "rows": int(len(rows)),
            "valid_rows": valid_rows,
            "elapsed_sec": time.monotonic() - started,
            "error": "",
        }
    except Exception as exc:
        return {
            "tradingsymbol": symbol,
            "underlying": str(contract["underlying"]),
            "app": runtime.app_name,
            "state": "FAILED",
            "rows": 0,
            "elapsed_sec": time.monotonic() - started,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _partition_rows(universe: pd.DataFrame, count: int) -> list[list[pd.Series]]:
    partitions: list[list[pd.Series]] = [[] for _ in range(max(1, count))]
    for index, (_, row) in enumerate(universe.sort_values("tradingsymbol").iterrows()):
        partitions[index % len(partitions)].append(row)
    return partitions


def fetch_contracts(
    universe: pd.DataFrame,
    runtimes: list[AppRuntime],
    from_dt: datetime,
    to_dt: datetime,
    *,
    slot_end: datetime | None,
    max_retries: int,
    phase: str,
    from_column: str = "",
) -> list[dict[str, Any]]:
    partitions = _partition_rows(universe, len(runtimes))

    def _run_partition(runtime: AppRuntime, rows: Iterable[pd.Series]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for contract in rows:
            contract_from = from_dt
            if from_column and from_column in contract.index and pd.notna(contract[from_column]):
                from_stamp = pd.Timestamp(contract[from_column])
                if from_stamp.tzinfo is None:
                    from_stamp = from_stamp.tz_localize(common.IST)
                else:
                    from_stamp = from_stamp.tz_convert(common.IST)
                contract_from = from_stamp.to_pydatetime()
            common.publish_heartbeat(
                SESSION,
                "RUNNING",
                phase=phase,
                app=runtime.app_name,
                contract=contract["tradingsymbol"],
                slot=slot_end.isoformat() if slot_end else "bootstrap",
            )
            results.append(
                fetch_one_contract(
                    runtime,
                    contract,
                    contract_from,
                    to_dt,
                    slot_end=slot_end,
                    max_retries=max_retries,
                )
            )
        return results

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=len(runtimes), thread_name_prefix="fno-oi-app") as pool:
        futures = [
            pool.submit(_run_partition, runtime, partition)
            for runtime, partition in zip(runtimes, partitions)
            if partition
        ]
        for future in as_completed(futures):
            outcomes.extend(future.result())
    return sorted(outcomes, key=lambda item: str(item["tradingsymbol"]))


def _bootstrap_required(
    universe: pd.DataFrame,
    target: datetime,
    *,
    bootstrap_days: int = 60,
) -> pd.DataFrame:
    required: list[int] = []
    fetch_from: dict[int, datetime] = {}
    target_stamp = pd.Timestamp(target)
    initial_start = target - timedelta(days=max(1, int(bootstrap_days)))
    heartbeat_at = 0.0
    total = int(len(universe))
    for checked, (index, row) in enumerate(universe.iterrows(), start=1):
        now_monotonic = time.monotonic()
        if checked == 1 or checked == total or now_monotonic - heartbeat_at >= 15.0:
            common.publish_heartbeat(
                SESSION,
                "RUNNING",
                phase="BOOTSTRAP_AUDIT",
                contracts_checked=checked,
                contracts_expected=total,
                target=target.isoformat(),
            )
            heartbeat_at = now_monotonic
        path = common.raw_contract_path(str(row["tradingsymbol"]))
        if not path.exists():
            required.append(index)
            fetch_from[index] = initial_start
            continue
        try:
            dates = pd.read_parquet(path, columns=["timestamp"])
            observed = common._to_ist(dates["timestamp"]).dropna()
            if observed.empty:
                required.append(index)
                fetch_from[index] = initial_start
                continue
            last = observed.max()
            if last < target_stamp:
                required.append(index)
                fetch_from[index] = max(last.to_pydatetime(), initial_start)
        except Exception:
            required.append(index)
            fetch_from[index] = initial_start
    planned = universe.loc[required].copy()
    planned["_fetch_from"] = pd.Series(fetch_from)
    return planned.reset_index(drop=True)


def run_bootstrap(
    universe: pd.DataFrame,
    runtimes: list[AppRuntime],
    args: argparse.Namespace,
    holidays: set[date],
) -> None:
    target = latest_completed_slot(common.now_ist(), holidays)
    if target is None:
        return
    required = _bootstrap_required(
        universe,
        target,
        bootstrap_days=args.bootstrap_days,
    )
    if required.empty:
        print(f"[BOOTSTRAP] All {len(universe)} contracts are current through {target}.", flush=True)
        return
    start = pd.Timestamp(required["_fetch_from"].min()).to_pydatetime()
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="BOOTSTRAP",
        target=target.isoformat(),
        contracts_expected=len(required),
    )
    print(
        f"[BOOTSTRAP] Fetching {len(required)}/{len(universe)} contracts from "
        f"{start.isoformat()} through {target.isoformat()}.",
        flush=True,
    )
    outcomes = fetch_contracts(
        required,
        runtimes,
        start,
        target,
        slot_end=None,
        max_retries=args.max_retries,
        phase="BOOTSTRAP",
        from_column="_fetch_from",
    )
    failed = [item for item in outcomes if item["state"] == "FAILED"]
    print(
        f"[BOOTSTRAP] Complete written={sum(item['state'] == 'WRITTEN' for item in outcomes)} "
        f"failed={len(failed)} no_candle={sum(item['state'] == 'NO_CANDLE' for item in outcomes)}",
        flush=True,
    )
    if failed:
        print(
            "[BOOTSTRAP][WARN] "
            + " | ".join(f"{item['tradingsymbol']}={item['error']}" for item in failed[:10]),
            flush=True,
        )


def _cash_marker_state(slot_end: datetime) -> tuple[bool, str]:
    path = common.cash_slot_path(slot_end)
    if not path.exists():
        return False, "missing"
    try:
        payload = common.read_json(path)
    except Exception as exc:
        return False, f"unreadable:{type(exc).__name__}"
    if str(payload.get("source", "")).lower() != "final":
        return False, "not_final"
    complete = bool(payload.get("complete"))
    return complete, "complete" if complete else "final_incomplete"


def _render_fetch_report(marker: dict[str, Any], outcomes: list[dict[str, Any]]) -> str:
    lines = [
        "# FnO 5-Minute OI Fetch",
        "",
        f"Slot: {marker['slot_ist']}",
        f"Published: {marker['published_at_ist']}",
        f"State: {marker['state']}",
        f"Coverage: {marker['coverage_ratio']:.1%}",
        f"Contracts written: {marker['contracts_written']}/{marker['contracts_expected']}",
        f"No candle: {marker['no_candle_count']}",
        f"Stock-futures coverage: {marker['stock_coverage_ratio']:.1%}",
        f"Stock futures written: {marker['stock_contracts_written']}/{marker['stock_contracts_expected']}",
        f"Verified stock no-candle skips: {marker['stock_verified_no_candle_count']}",
        f"Invalid data: {marker['invalid_data_count']}",
        f"API failures: {marker['failed_count']}",
        f"Cash feed at FnO fetch start: {marker['cash_marker_state_at_start']}",
        f"Cash feed at FnO publication: {marker['cash_marker_state']}",
        f"Duration: {marker['duration_sec']:.1f}s",
    ]
    failures = [item for item in outcomes if item["state"] in {"FAILED", "INVALID_DATA"}]
    if failures:
        lines.extend(["", "Exceptions / invalid rows:", "", "Contract | State | Detail", "--- | --- | ---"])
        for item in failures[:25]:
            detail = str(item.get("error") or "quality validation failed").replace("|", "/")
            lines.append(f"{item['tradingsymbol']} | {item['state']} | {detail}")
    verified_no_candle_symbols = list(
        marker.get("verified_no_candle_symbols") or []
    )
    if verified_no_candle_symbols:
        lines.extend(
            [
                "",
                "Verified no-candle contracts (never synthesized or forward-filled):",
                "",
                ", ".join(
                    f"`{symbol}`" for symbol in verified_no_candle_symbols
                ),
            ]
        )
    stock_unverified_no_candle_symbols = list(
        marker.get("stock_unverified_no_candle_symbols") or []
    )
    if stock_unverified_no_candle_symbols:
        lines.extend(
            [
                "",
                "Unverified stock no-candle contracts (slot remains incomplete):",
                "",
                ", ".join(
                    f"`{symbol}`" for symbol in stock_unverified_no_candle_symbols
                ),
            ]
        )
    excluded_unverified_symbols = sorted(
        set(marker.get("unverified_no_candle_symbols") or [])
        - set(stock_unverified_no_candle_symbols)
    )
    if excluded_unverified_symbols:
        lines.extend(
            [
                "",
                "Unverified excluded-index no-candle contracts (stock readiness unaffected):",
                "",
                ", ".join(f"`{symbol}`" for symbol in excluded_unverified_symbols),
            ]
        )
    return "\n".join(lines) + "\n"


def _is_index_universe_row(row: dict[str, Any]) -> bool:
    raw_flag = row.get("is_index_future", False)
    if isinstance(raw_flag, str):
        flagged = raw_flag.strip().lower() in {"1", "true", "yes"}
    else:
        try:
            flagged = False if pd.isna(raw_flag) else bool(raw_flag)
        except (TypeError, ValueError):
            flagged = False
    return bool(
        flagged
        or str(row.get("underlying", "")).strip().upper()
        in common.INDEX_UNDERLYINGS
    )


def run_slot(
    slot_end: datetime,
    universe: pd.DataFrame,
    runtimes: list[AppRuntime],
    args: argparse.Namespace,
) -> dict[str, Any]:
    started = time.monotonic()
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="FETCH_SLOT",
        slot=slot_end.isoformat(),
        contracts_expected=len(universe),
        apps=len(runtimes),
    )
    _, cash_state_at_start = _cash_marker_state(slot_end)
    print(
        f"[SLOT] {slot_end.strftime('%H:%M')} cash_marker_at_start={cash_state_at_start} "
        f"contracts={len(universe)} apps={len(runtimes)}",
        flush=True,
    )
    expected_symbols = {
        str(value).strip().upper()
        for value in universe["tradingsymbol"].dropna()
        if str(value).strip()
    }
    outcomes = fetch_contracts(
        universe,
        runtimes,
        slot_end - timedelta(minutes=5),
        slot_end,
        slot_end=slot_end,
        max_retries=args.max_retries,
        phase="FETCH_SLOT",
    )
    outcome_by_symbol = {
        str(item["tradingsymbol"]).strip().upper(): item for item in outcomes
    }
    attempts_by_symbol = {
        str(item["tradingsymbol"]).strip().upper(): 1 for item in outcomes
    }
    no_candle_observations_by_symbol = {
        str(item["tradingsymbol"]).strip().upper(): int(
            item["state"] == "NO_CANDLE"
        )
        for item in outcomes
    }
    retries_used = 0
    for retry_number in range(1, max(0, int(args.slot_retry_attempts)) + 1):
        unresolved_symbols = (expected_symbols - set(outcome_by_symbol)) | {
            symbol
            for symbol, item in outcome_by_symbol.items()
            if item["state"] != "WRITTEN"
        }
        if not unresolved_symbols:
            break
        retries_used = retry_number
        time.sleep(max(0.0, float(args.slot_retry_delay_sec)))
        retry_universe = universe.loc[
            universe["tradingsymbol"].astype(str).str.strip().str.upper().isin(
                unresolved_symbols
            )
        ].copy()
        rotated_runtimes = runtimes[retry_number % len(runtimes) :] + runtimes[: retry_number % len(runtimes)]
        print(
            f"[SLOT][RETRY {retry_number}] {slot_end.strftime('%H:%M')} "
            f"contracts={len(retry_universe)}",
            flush=True,
        )
        retry_outcomes = fetch_contracts(
            retry_universe,
            rotated_runtimes,
            slot_end - timedelta(minutes=5),
            slot_end,
            slot_end=slot_end,
            max_retries=args.max_retries,
            phase=f"FETCH_SLOT_RETRY_{retry_number}",
        )
        for item in retry_outcomes:
            symbol = str(item["tradingsymbol"]).strip().upper()
            attempts_by_symbol[symbol] = attempts_by_symbol.get(symbol, 0) + 1
            if item["state"] == "NO_CANDLE":
                no_candle_observations_by_symbol[symbol] = (
                    no_candle_observations_by_symbol.get(symbol, 0) + 1
                )
        outcome_by_symbol.update(
            {
                str(item["tradingsymbol"]).strip().upper(): item
                for item in retry_outcomes
            }
        )
    outcomes = sorted(outcome_by_symbol.values(), key=lambda item: str(item["tradingsymbol"]))
    written = sum(item["state"] == "WRITTEN" for item in outcomes)
    no_candle = sum(item["state"] == "NO_CANDLE" for item in outcomes)
    invalid = sum(item["state"] == "INVALID_DATA" for item in outcomes)
    failed = sum(item["state"] == "FAILED" for item in outcomes)
    expected = int(len(universe))
    coverage = float(written / expected) if expected else 0.0

    universe_rows = universe.to_dict("records")
    stock_symbols = {
        str(row.get("tradingsymbol", "")).strip().upper()
        for row in universe_rows
        if not _is_index_universe_row(row)
    }
    index_symbols = {
        str(row.get("tradingsymbol", "")).strip().upper()
        for row in universe_rows
        if _is_index_universe_row(row)
    }
    written_symbols = {
        str(item["tradingsymbol"]).strip().upper()
        for item in outcomes
        if item["state"] == "WRITTEN"
    }
    no_candle_symbols = {
        str(item["tradingsymbol"]).strip().upper()
        for item in outcomes
        if item["state"] == "NO_CANDLE"
    }
    verified_no_candle_symbols = {
        symbol
        for symbol in no_candle_symbols
        if no_candle_observations_by_symbol.get(symbol, 0)
        >= common.MIN_NO_CANDLE_FETCH_ATTEMPTS
    }
    unverified_no_candle_symbols = no_candle_symbols - verified_no_candle_symbols
    stock_written_symbols = written_symbols & stock_symbols
    stock_no_candle_symbols = no_candle_symbols & stock_symbols
    stock_verified_no_candle_symbols = verified_no_candle_symbols & stock_symbols
    index_written_symbols = written_symbols & index_symbols
    index_no_candle_symbols = no_candle_symbols & index_symbols
    observed_symbols = {
        str(item["tradingsymbol"]).strip().upper() for item in outcomes
    }
    invalid_symbols = {
        str(item["tradingsymbol"]).strip().upper()
        for item in outcomes
        if item["state"] == "INVALID_DATA"
    }
    failed_symbols = {
        str(item["tradingsymbol"]).strip().upper()
        for item in outcomes
        if item["state"] == "FAILED"
    }
    stock_invalid_symbols = invalid_symbols & stock_symbols
    stock_failed_symbols = failed_symbols & stock_symbols
    stock_unverified_no_candle_symbols = (
        unverified_no_candle_symbols & stock_symbols
    )
    observed_stock_symbols = observed_symbols & stock_symbols
    unexpected_observed_symbols = observed_symbols - expected_symbols
    stock_expected = len(stock_symbols)
    stock_written = len(stock_written_symbols)
    stock_coverage = float(stock_written / stock_expected) if stock_expected else 0.0
    stock_universe_frame = universe.loc[
        universe["tradingsymbol"].astype(str).str.upper().isin(stock_symbols)
    ].copy()
    try:
        stock_universe_sha256 = common.universe_sha256(stock_universe_frame)
    except (AttributeError, KeyError, TypeError, ValueError):
        stock_universe_sha256 = common.symbol_set_sha256(stock_symbols)
    minimum_stock_coverage = max(
        float(args.min_coverage), common.MIN_STOCK_FUTURES_COVERAGE
    )
    stock_complete = bool(
        observed_stock_symbols == stock_symbols
        and not unexpected_observed_symbols
        and stock_expected > 0
        and not stock_failed_symbols
        and not stock_invalid_symbols
        and not stock_unverified_no_candle_symbols
        and stock_written + len(stock_no_candle_symbols) == stock_expected
        and stock_coverage >= minimum_stock_coverage
        and len(stock_verified_no_candle_symbols)
        <= common.MAX_VERIFIED_NO_CANDLE_STOCKS
    )
    global_complete = bool(
        expected_symbols == observed_symbols
        and failed == 0
        and invalid == 0
        and not unverified_no_candle_symbols
        and coverage >= minimum_stock_coverage
    )
    complete = bool(
        stock_complete
        and expected_symbols == observed_symbols
        and written + no_candle + invalid + failed == expected
        and failed == 0
        and invalid == 0
    )
    state = "SUCCESS" if complete else "PARTIAL"
    _, cash_state = _cash_marker_state(slot_end)
    marker: dict[str, Any] = {
        "schema_version": common.FNO_FETCH_SLOT_SCHEMA_VERSION,
        "source": "final",
        "state": state,
        "complete": complete,
        "attempt_complete": expected_symbols == observed_symbols,
        "outcome_symbol_set_complete": expected_symbols == observed_symbols,
        "stock_outcome_symbol_set_complete": observed_stock_symbols == stock_symbols,
        "unexpected_outcome_symbols": sorted(unexpected_observed_symbols),
        "slot_ist": slot_end.isoformat(),
        "published_at_ist": common.now_ist().isoformat(timespec="seconds"),
        "universe_date": slot_end.date().isoformat(),
        "universe_sha256": common.universe_sha256(universe),
        "contracts_expected": expected,
        "contracts_written": written,
        "no_candle_count": no_candle,
        "no_candle_symbols": sorted(no_candle_symbols),
        "verified_no_candle_symbols": sorted(verified_no_candle_symbols),
        "unverified_no_candle_symbols": sorted(unverified_no_candle_symbols),
        "no_candle_fetch_attempts": {
            symbol: int(attempts_by_symbol.get(symbol, 0))
            for symbol in sorted(no_candle_symbols)
        },
        "no_candle_observations": {
            symbol: int(no_candle_observations_by_symbol.get(symbol, 0))
            for symbol in sorted(no_candle_symbols)
        },
        "minimum_no_candle_fetch_attempts": common.MIN_NO_CANDLE_FETCH_ATTEMPTS,
        "stock_universe_sha256": stock_universe_sha256,
        "stock_symbol_set_sha256": common.symbol_set_sha256(stock_symbols),
        "stock_contracts_expected": stock_expected,
        "stock_contracts_written": stock_written,
        "stock_written_symbols": sorted(stock_written_symbols),
        "stock_no_candle_count": len(stock_no_candle_symbols),
        "stock_no_candle_symbols": sorted(stock_no_candle_symbols),
        "stock_verified_no_candle_count": len(stock_verified_no_candle_symbols),
        "stock_verified_no_candle_symbols": sorted(stock_verified_no_candle_symbols),
        "stock_unverified_no_candle_symbols": sorted(stock_unverified_no_candle_symbols),
        "stock_invalid_data_count": len(stock_invalid_symbols),
        "stock_invalid_data_symbols": sorted(stock_invalid_symbols),
        "stock_failed_count": len(stock_failed_symbols),
        "stock_failed_symbols": sorted(stock_failed_symbols),
        "stock_coverage_ratio": stock_coverage,
        "stock_complete": stock_complete,
        "stock_state": "SUCCESS" if stock_complete else "PARTIAL",
        "global_complete": global_complete,
        "index_contracts_expected": len(index_symbols),
        "index_contracts_written": len(index_written_symbols),
        "index_no_candle_count": len(index_no_candle_symbols),
        "index_no_candle_symbols": sorted(index_no_candle_symbols),
        "invalid_data_count": invalid,
        "failed_count": failed,
        "coverage_ratio": coverage,
        "minimum_coverage": minimum_stock_coverage,
        "minimum_stock_coverage": minimum_stock_coverage,
        "maximum_verified_no_candle_stocks": common.MAX_VERIFIED_NO_CANDLE_STOCKS,
        "readiness_policy": common.VERIFIED_NO_CANDLE_POLICY_VERSION,
        "cash_marker_state_at_start": cash_state_at_start,
        "cash_marker_state": cash_state,
        "apps_used": [runtime.app_name for runtime in runtimes],
        "duration_sec": time.monotonic() - started,
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
    common.atomic_write_json(common.fetch_slot_path(slot_end), marker)
    report = _render_fetch_report(marker, outcomes)
    common.atomic_write_text(
        common.LATEST_DIR / "latest_fno_oi_fetch.md",
        report,
    )
    common.publish_status(
        SESSION,
        state,
        heartbeat_state="RUNNING",
        phase="SLOT_DONE",
        slot=slot_end.isoformat(),
        contracts_expected=expected,
        contracts_written=written,
        coverage_ratio=f"{coverage:.4f}",
        no_candle_count=no_candle,
        stock_contracts_expected=stock_expected,
        stock_contracts_written=stock_written,
        stock_coverage_ratio=f"{stock_coverage:.4f}",
        stock_verified_no_candle_count=len(stock_verified_no_candle_symbols),
        failed_count=failed,
        output=common.fetch_slot_path(slot_end),
    )
    print(
        f"[SLOT][{state}] {slot_end.strftime('%H:%M')} written={written}/{expected} "
        f"coverage={coverage:.1%} no_candle={no_candle} invalid={invalid} failed={failed} "
        f"stock={stock_written}/{stock_expected} stock_no_candle={len(stock_no_candle_symbols)} "
        f"duration={marker['duration_sec']:.1f}s",
        flush=True,
    )
    return marker


def _today_processed_slots(day: date) -> set[str]:
    completed: set[str] = set()
    for path in common.FETCH_SLOT_DIR.glob(f"slot_{day.strftime('%Y%m%d')}_*.json"):
        try:
            marker = common.read_json(path)
        except Exception:
            continue
        if not bool(marker.get("complete")):
            continue
        if str(marker.get("source", "")).lower() != "final":
            continue
        schema_version = str(marker.get("schema_version", ""))
        if schema_version == common.FNO_FETCH_SLOT_SCHEMA_VERSION:
            if (
                str(marker.get("readiness_policy", ""))
                == common.VERIFIED_NO_CANDLE_POLICY_VERSION
                and bool(marker.get("stock_complete"))
            ):
                completed.add(path.stem.rsplit("_", 1)[-1])
            continue
        # Legacy markers remain usable only when they contain no omission that
        # would need the exact v2 verification evidence.
        try:
            legacy_no_candle_count = int(marker.get("no_candle_count", 0) or 0)
        except (TypeError, ValueError):
            continue
        if legacy_no_candle_count == 0:
            completed.add(path.stem.rsplit("_", 1)[-1])
    return completed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch completed near-month NFO-FUT 5-minute OHLCV+OI candles."
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--slot", default="")
    parser.add_argument("--session-date", default="")
    parser.add_argument("--boundary-buffer-sec", type=float, default=3.0)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--slot-retry-attempts", type=int, default=2)
    parser.add_argument("--slot-retry-delay-sec", type=float, default=2.0)
    parser.add_argument("--partial-retry-sec", type=float, default=30.0)
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=common.MIN_STOCK_FUTURES_COVERAGE,
        help="Minimum exact-slot coverage across mapped stock futures.",
    )
    parser.add_argument("--bootstrap-days", type=int, default=60)
    parser.add_argument("--no-bootstrap", action="store_true")
    parser.add_argument("--allow-non-trading-day", action="store_true")
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

    universe = ensure_universe(session_date, args)
    runtimes = _build_app_runtimes(args)
    common.publish_status(
        SESSION,
        "RUNNING",
        phase="START",
        session_date_ist=session_date.isoformat(),
        contracts_expected=len(universe),
        apps=len(runtimes),
    )
    if not args.no_bootstrap:
        run_bootstrap(universe, runtimes, args, holidays)

    if args.once:
        slot = _coerce_slot(args.slot) if args.slot else latest_completed_slot(common.now_ist(), holidays)
        if slot is None:
            raise RuntimeError("No completed five-minute slot is available.")
        if slot.date() != session_date and not args.allow_non_trading_day:
            raise ValueError(f"Requested slot {slot.isoformat()} is not on session date {session_date}.")
        marker = run_slot(slot, universe, runtimes, args)
        return 0 if bool(marker.get("complete")) else 2

    processed = _today_processed_slots(session_date)
    end_deadline = datetime.combine(session_date, LAST_SLOT, tzinfo=common.IST) + timedelta(minutes=3)
    while True:
        current = common.now_ist()
        if current.date() != session_date or current >= end_deadline:
            common.publish_status(
                SESSION,
                "DONE",
                phase="END_TIME",
                session_date_ist=session_date.isoformat(),
                processed_slots=len(processed),
            )
            return 0
        slot = latest_completed_slot(current, holidays)
        if slot is None or slot.date() != session_date or slot.time() < FIRST_SLOT:
            common.publish_heartbeat(
                SESSION,
                "SCHEDULED",
                phase="WAIT_FIRST_SLOT",
                session_date_ist=session_date.isoformat(),
            )
            time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
            continue
        if slot.time() > LAST_SLOT:
            slot = datetime.combine(session_date, LAST_SLOT, tzinfo=common.IST)
        slot_key = slot.strftime("%H%M")
        due_at = slot + timedelta(seconds=max(0.0, float(args.boundary_buffer_sec)))
        if slot_key in processed or current < due_at:
            common.publish_heartbeat(
                SESSION,
                "WAITING",
                phase="WAIT_NEXT_SLOT",
                slot=slot.isoformat(),
                processed_slots=len(processed),
            )
            time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
            continue
        marker = run_slot(slot, universe, runtimes, args)
        if bool(marker.get("complete")):
            processed.add(slot_key)
        else:
            time.sleep(max(1.0, float(args.partial_retry_sec)))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run_session(args)
    except KeyboardInterrupt:
        common.publish_status(SESSION, "STOPPED", heartbeat_state="STOPPED", phase="INTERRUPTED")
        return 0
    except Exception as exc:
        common.publish_status(
            SESSION,
            "FAILED",
            heartbeat_state="CRASHED",
            phase="FAILED",
            error=f"{type(exc).__name__}: {exc}",
        )
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
