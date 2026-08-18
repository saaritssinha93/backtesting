"""Durable candidate-scoped NSE equity 1-minute feed for FNO confirmation.

The producer is deliberately separate from strategy confirmation.  It watches
immutable scanner candidate sets, fetches only the exact completed confirmation
candle, persists and re-reads every bar, then publishes an immutable marker.
Strategy confirmation consumes that marker and never calls the broker API.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


FEED_POLICY_VERSION = "candidate_exact_completed_1m_verified_no_candle_v1"
NO_CANDLE_RESOLUTION_POLICY = "ALL_WRITTEN_OR_VERIFIED_NO_CANDLE"
MIN_NO_CANDLE_OBSERVATIONS = 3
MIN_NO_CANDLE_VERIFICATION_AGE_SEC = 15
DEFAULT_NO_CANDLE_OBSERVATION_SPACING_SEC = 2.0
DEFAULT_GENERATION = os.getenv("FNO_LIVE_GENERATION", "v6").strip().lower()


@dataclass
class AppRuntime:
    app_name: str
    client: Any
    pace_seconds: float
    _last_call_at: float = 0.0
    _lock: Lock = field(default_factory=Lock)

    def pace(self) -> None:
        with self._lock:
            wait = self.pace_seconds - (time.monotonic() - self._last_call_at)
            if wait > 0:
                time.sleep(wait)
            self._last_call_at = time.monotonic()


def _config(generation: str):
    normalized = str(generation).strip().lower()
    if normalized not in {"v5", "v6"}:
        raise ValueError(f"Unsupported FNO generation: {generation}")
    return importlib.import_module(f"fno_{normalized}_live_config")


def scanner_slot_path(generation: str, session_date: date, signal_end: str) -> Path:
    return (
        common.FNO_ROOT
        / f"{generation}_live"
        / "scanner_5m"
        / session_date.isoformat()
        / f"slot_{signal_end.replace(':', '')}.json"
    )


def scanner_snapshot_sha256(snapshot: dict[str, Any]) -> str:
    return common.canonical_json_sha256(snapshot)


def candidate_contract_payload(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for candidate in list(snapshot.get("candidates") or []):
        rows.append(
            {
                "tradingsymbol": str(candidate.get("tradingsymbol", "")).strip().upper(),
                "instrument_token": int(candidate.get("instrument_token", 0) or 0),
                "futures_tradingsymbol": str(
                    candidate.get("futures_tradingsymbol", "")
                ).strip().upper(),
                "signal_timestamp": str(candidate.get("signal_timestamp", "")),
                "side": str(candidate.get("side", "")).strip().upper(),
            }
        )
    return sorted(rows, key=lambda row: row["tradingsymbol"])


def candidate_contract_sha256(snapshot: dict[str, Any]) -> str:
    return common.canonical_json_sha256(candidate_contract_payload(snapshot))


def _to_ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def _extract_exact_bar(
    records: Iterable[dict[str, Any]], slot_start: datetime
) -> dict[str, Any] | None:
    expected_start = pd.Timestamp(slot_start)
    for record in records:
        raw = record.get("date") or record.get("timestamp")
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
        }
    return None


def _validate_bar(bar: dict[str, Any], expected_end: datetime) -> str:
    try:
        observed_end = _to_ist(bar["timestamp"])
        expected = pd.Timestamp(expected_end)
        values = [float(bar[key]) for key in ("open", "high", "low", "close")]
        volume = float(bar.get("volume", 0))
    except (KeyError, TypeError, ValueError):
        return "invalid_or_missing_ohlcv"
    if observed_end != expected:
        return "wrong_candle_end"
    if not all(np.isfinite(value) for value in values) or not np.isfinite(volume):
        return "non_finite_ohlcv"
    if any(value <= 0 for value in values):
        return "non_positive_ohlc"
    open_, high, low, close = values
    if high < max(open_, close) or low > min(open_, close) or high < low or volume < 0:
        return "invalid_ohlcv_geometry"
    return ""


def _coerce_ist_timestamps(values: pd.Series) -> pd.Series:
    """Normalize mixed naive/aware timestamp values without changing wall time.

    Historical candidate files can contain naive IST values alongside explicit
    ``+05:30`` values.  ``pd.to_datetime`` rejects that mixture; parsing each
    value through the data contract's IST rule first preserves naive values as
    IST and converts aware values to IST before building one homogeneous series.
    """

    normalized: list[pd.Timestamp] = []
    for value in values:
        try:
            if pd.isna(value):
                normalized.append(pd.NaT)
            else:
                normalized.append(pd.Timestamp(_to_ist(value)))
        except (TypeError, ValueError):
            normalized.append(pd.NaT)
    return pd.Series(
        pd.DatetimeIndex(normalized),
        index=values.index,
        name=values.name,
    )


def _load_persisted_bar(
    candidate: dict[str, Any], session_date: date, expected_end: datetime
) -> dict[str, Any] | None:
    symbol = str(candidate["tradingsymbol"]).strip().upper()
    path = common.equity_1m_path(session_date, symbol)
    if not path.exists():
        return None
    try:
        frame = pd.read_parquet(path)
        timestamps = _coerce_ist_timestamps(frame["timestamp"])
        selected = frame.loc[timestamps.eq(pd.Timestamp(expected_end))]
        if selected.empty:
            return None
        result = selected.iloc[-1].to_dict()
        result["timestamp"] = pd.Timestamp(expected_end).isoformat()
        if _validate_bar(result, expected_end):
            return None
        if int(result.get("instrument_token", 0) or 0) != int(
            candidate["instrument_token"]
        ):
            return None
        return result
    except (OSError, KeyError, TypeError, ValueError):
        return None


def _persist_bar(
    candidate: dict[str, Any],
    bar: dict[str, Any],
    session_date: date,
    expected_end: datetime,
) -> dict[str, Any]:
    error = _validate_bar(bar, expected_end)
    if error:
        raise ValueError(error)
    symbol = str(candidate["tradingsymbol"]).strip().upper()
    path = common.equity_1m_path(session_date, symbol)
    incoming = pd.DataFrame(
        [
            {
                **bar,
                "tradingsymbol": symbol,
                "instrument_token": int(candidate["instrument_token"]),
                "exchange": "NSE",
                "source": "KITE_HISTORICAL_COMPLETED_1M",
                "data_contract": hybrid.DATA_CONTRACT_VERSION,
                "fetched_at_ist": common.now_ist().isoformat(timespec="microseconds"),
            }
        ]
    )
    existing = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    combined = (
        incoming
        if existing.empty
        else pd.concat([existing, incoming], ignore_index=True, sort=False)
    )
    combined["timestamp"] = _coerce_ist_timestamps(combined["timestamp"])
    if "candle_start" in combined.columns:
        combined["candle_start"] = _coerce_ist_timestamps(combined["candle_start"])
    combined = (
        combined.dropna(subset=["timestamp"])
        .drop_duplicates(["tradingsymbol", "timestamp"], keep="last")
        .sort_values("timestamp", kind="stable")
        .reset_index(drop=True)
    )
    common.atomic_write_parquet(combined, path)
    reloaded = _load_persisted_bar(candidate, session_date, expected_end)
    if reloaded is None:
        raise RuntimeError(f"Persisted completed 1-minute bar cannot be re-read: {path}")
    return reloaded


def _fetch_one(
    runtime: AppRuntime,
    candidate: dict[str, Any],
    slot_start: datetime,
    expected_end: datetime,
) -> dict[str, Any]:
    symbol = str(candidate["tradingsymbol"]).strip().upper()
    try:
        runtime.pace()
        records = runtime.client.historical_data(
            int(candidate["instrument_token"]),
            slot_start,
            slot_start + timedelta(minutes=2),
            "minute",
            continuous=False,
            oi=False,
        )
        bar = _extract_exact_bar(records, slot_start)
        if bar is None:
            return {
                "tradingsymbol": symbol,
                "state": "NO_CANDLE",
                "error": "",
                "observed_at_ist": common.now_ist().isoformat(timespec="microseconds"),
            }
        error = _validate_bar(bar, expected_end)
        if error:
            return {
                "tradingsymbol": symbol,
                "state": "INVALID_DATA",
                "error": error,
                "observed_at_ist": common.now_ist().isoformat(timespec="microseconds"),
            }
        persisted = _persist_bar(candidate, bar, slot_start.date(), expected_end)
        return {
            "tradingsymbol": symbol,
            "state": "WRITTEN",
            "error": "",
            "bar": persisted,
            "observed_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        }
    except Exception as exc:
        return {
            "tradingsymbol": symbol,
            "state": "FAILED",
            "error": f"{type(exc).__name__}: {exc}",
            "observed_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        }


def fetch_candidates(
    candidates: list[dict[str, Any]],
    runtimes: list[AppRuntime],
    slot_start: datetime,
    expected_end: datetime,
) -> list[dict[str, Any]]:
    if not candidates:
        return []
    if not runtimes:
        raise RuntimeError("No authenticated Kite runtime is available")
    partitions = [candidates[index :: len(runtimes)] for index in range(len(runtimes))]

    def work(runtime: AppRuntime, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [_fetch_one(runtime, row, slot_start, expected_end) for row in rows]

    outcomes: list[dict[str, Any]] = []
    with ThreadPoolExecutor(
        max_workers=len(runtimes), thread_name_prefix="fno-equity-1m"
    ) as executor:
        futures = [
            executor.submit(work, runtime, rows)
            for runtime, rows in zip(runtimes, partitions)
            if rows
        ]
        for future in as_completed(futures):
            outcomes.extend(future.result())
    return sorted(outcomes, key=lambda row: row["tradingsymbol"])


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _publish_slot_data_once(path: Path, frame: pd.DataFrame) -> str:
    """Create an immutable parquet snapshot without replacing a rival writer."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_temp_path = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".publish.tmp", dir=str(path.parent)
    )
    os.close(fd)
    temp_path = Path(raw_temp_path)
    try:
        frame.to_parquet(temp_path, index=False, engine="pyarrow")
        # Parquet close flushes Python/Arrow buffers, while fsync makes the
        # completed bytes durable before the create-once hard-link is exposed.
        with temp_path.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        incoming_sha256 = _sha256_file(temp_path)
        try:
            os.link(temp_path, path)
        except FileExistsError:
            existing_sha256 = _sha256_file(path)
            if existing_sha256 != incoming_sha256:
                raise RuntimeError(f"Immutable confirmation data collision: {path}")
            return existing_sha256
        return incoming_sha256
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _marker_path(
    generation: str, confirmation_end: datetime, snapshot: dict[str, Any]
) -> Path:
    return common.equity_1m_slot_path(
        confirmation_end,
        generation=generation,
        scanner_sha256=scanner_snapshot_sha256(snapshot),
    )


def _publish_final_marker_once(
    path: Path, marker: dict[str, Any]
) -> dict[str, Any]:
    """Atomically create an immutable marker, never replace a rival decision."""

    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(marker, indent=2, ensure_ascii=True, default=str) + "\n"
    ).encode("utf-8")
    fd, raw_temp_path = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".publish.tmp", dir=str(path.parent)
    )
    temp_path = Path(raw_temp_path)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # A hard-link publish is create-if-absent and atomic on the same
            # filesystem.  Unlike os.replace, it cannot overwrite a marker
            # won by another producer process.
            os.link(temp_path, path)
        except FileExistsError:
            existing = common.read_json(path)
            if common.canonical_json_sha256(existing) != common.canonical_json_sha256(
                marker
            ):
                raise RuntimeError(
                    f"Immutable confirmation marker collision: {path}"
                )
            return existing
        return marker
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _validate_reusable_final_marker(
    marker: dict[str, Any],
    *,
    marker_path: Path,
    snapshot: dict[str, Any],
    generation: str,
    session_date: date,
    signal_end: str,
    confirmation_end: datetime,
    deadline: datetime,
    minimum_no_candle_time: datetime,
    expected_symbols: set[str],
    config: Any,
) -> None:
    """Reject altered/corrupt content at an already-published marker path."""

    expected_fields = {
        "schema_version": common.EQUITY_1M_SLOT_SCHEMA_VERSION,
        "feed_policy": FEED_POLICY_VERSION,
        "source": "final",
        "generation": generation,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[signal_end],
        "slot_ist": confirmation_end.isoformat(),
        "deadline_ist": deadline.isoformat(),
        "minimum_no_candle_verification_ist": minimum_no_candle_time.isoformat(),
        "scanner_snapshot_sha256": scanner_snapshot_sha256(snapshot),
        "candidate_contract_sha256": candidate_contract_sha256(snapshot),
        "candidate_symbol_set_sha256": common.symbol_set_sha256(expected_symbols),
        "candidate_resolution_policy": NO_CANDLE_RESOLUTION_POLICY,
    }
    mismatches = [
        field
        for field, expected in expected_fields.items()
        if marker.get(field) != expected
    ]
    if mismatches:
        raise RuntimeError(
            f"Existing immutable confirmation marker identity mismatch "
            f"({', '.join(mismatches)}): {marker_path}"
        )
    if common.canonical_json_sha256(marker.get("scanner_snapshot")) != (
        scanner_snapshot_sha256(snapshot)
    ):
        raise RuntimeError(
            f"Existing immutable confirmation marker scanner snapshot is invalid: "
            f"{marker_path}"
        )
    try:
        published_at = _to_ist(marker["published_at_ist"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Existing immutable confirmation marker time is invalid: {marker_path}"
        ) from exc
    complete = marker.get("complete")
    within_deadline = marker.get("within_deadline")
    if type(complete) is not bool or type(within_deadline) is not bool:
        raise RuntimeError(
            f"Existing immutable confirmation marker flags are invalid: {marker_path}"
        )
    if within_deadline != (published_at <= pd.Timestamp(deadline)):
        raise RuntimeError(
            f"Existing immutable confirmation marker deadline state is invalid: "
            f"{marker_path}"
        )
    expected_state = (
        "SUCCESS"
        if complete and within_deadline
        else "LATE_COMPLETE"
        if complete
        else "BLOCKED_INCOMPLETE_DATA"
    )
    if marker.get("state") != expected_state:
        raise RuntimeError(
            f"Existing immutable confirmation marker state is invalid: {marker_path}"
        )

    def symbol_set(field: str) -> set[str]:
        values = marker.get(field)
        if not isinstance(values, list):
            raise RuntimeError(
                f"Existing immutable confirmation marker {field} is invalid: "
                f"{marker_path}"
            )
        normalized = [str(value).strip().upper() for value in values]
        if any(not value for value in normalized) or len(normalized) != len(
            set(normalized)
        ):
            raise RuntimeError(
                f"Existing immutable confirmation marker {field} is invalid: "
                f"{marker_path}"
            )
        return set(normalized)

    candidates = symbol_set("candidate_symbols")
    written = symbol_set("written_symbols")
    verified_no_candle = symbol_set("verified_no_candle_symbols")
    if candidates != expected_symbols or int(marker.get("candidate_count", -1)) != len(
        expected_symbols
    ):
        raise RuntimeError(
            f"Existing immutable confirmation marker candidates are invalid: "
            f"{marker_path}"
        )
    if int(marker.get("written_count", -1)) != len(written) or int(
        marker.get("verified_no_candle_count", -1)
    ) != len(verified_no_candle):
        raise RuntimeError(
            f"Existing immutable confirmation marker counts are invalid: {marker_path}"
        )
    if complete:
        if (
            written & verified_no_candle
            or written | verified_no_candle != expected_symbols
            or symbol_set("no_candle_symbols") != verified_no_candle
            or symbol_set("unverified_no_candle_symbols")
            or symbol_set("resolved_symbols") != expected_symbols
            or symbol_set("invalid_symbols")
            or symbol_set("api_failed_symbols")
            or symbol_set("unexpected_missing_symbols")
            or marker.get("errors")
        ):
            raise RuntimeError(
                f"Existing immutable confirmation marker coverage is invalid: "
                f"{marker_path}"
            )
        expected_data_path = common.equity_1m_slot_data_path(
            confirmation_end,
            generation=generation,
            scanner_sha256=scanner_snapshot_sha256(snapshot),
        )
        if (
            str(marker.get("slot_data_path", "")) != str(expected_data_path)
            or not expected_data_path.exists()
            or _sha256_file(expected_data_path)
            != str(marker.get("slot_data_sha256", ""))
        ):
            raise RuntimeError(
                f"Existing immutable confirmation marker data binding is invalid: "
                f"{marker_path}"
            )
    elif marker.get("slot_data_path") or marker.get("slot_data_sha256"):
        raise RuntimeError(
            f"Existing blocked confirmation marker unexpectedly binds data: "
            f"{marker_path}"
        )


def validate_scanner_snapshot(
    snapshot: dict[str, Any], generation: str, session_date: date, signal_end: str
) -> None:
    config = _config(generation)
    if snapshot.get("strategy_version") != config.STRATEGY_VERSION:
        raise ValueError("scanner strategy version mismatch")
    if snapshot.get("strategy_fingerprint") != config.strategy_fingerprint():
        raise ValueError("scanner strategy fingerprint mismatch")
    if snapshot.get("session_date") != session_date.isoformat():
        raise ValueError("scanner session date mismatch")
    if snapshot.get("signal_end") != signal_end:
        raise ValueError("scanner signal slot mismatch")
    if snapshot.get("data_contract") != hybrid.DATA_CONTRACT_VERSION:
        raise ValueError("scanner data contract mismatch")
    contracts = candidate_contract_payload(snapshot)
    symbols = [row["tradingsymbol"] for row in contracts]
    if any(not symbol for symbol in symbols) or len(set(symbols)) != len(symbols):
        raise ValueError("scanner candidate symbols are blank or duplicated")
    if any(int(row["instrument_token"]) <= 0 for row in contracts):
        raise ValueError("scanner candidate instrument tokens must be positive")


def _clean_no_candle_history(
    history: list[dict[str, str]],
    *,
    required_observations: int,
    minimum_spacing_sec: float,
    not_before: datetime | pd.Timestamp | None = None,
    not_after: datetime | pd.Timestamp | None = None,
) -> bool:
    if len(history) < required_observations:
        return False
    if any(str(item.get("state", "")) != "NO_CANDLE" for item in history):
        return False
    try:
        observed = [_to_ist(item["observed_at_ist"]) for item in history]
    except (KeyError, TypeError, ValueError):
        return False
    lower = _to_ist(not_before) if not_before is not None else None
    upper = _to_ist(not_after) if not_after is not None else None
    return bool(
        (lower is None or all(stamp >= lower for stamp in observed))
        and (upper is None or all(stamp <= upper for stamp in observed))
        and all(
            (right - left).total_seconds() + 1e-9 >= minimum_spacing_sec
            for left, right in zip(observed, observed[1:])
        )
    )


def produce_slot(
    snapshot: dict[str, Any],
    generation: str,
    session_date: date,
    signal_end: str,
    runtimes: list[AppRuntime],
    *,
    observations: int = MIN_NO_CANDLE_OBSERVATIONS,
    retry_delay_sec: float = DEFAULT_NO_CANDLE_OBSERVATION_SPACING_SEC,
    finalize_incomplete: bool = False,
) -> dict[str, Any]:
    validate_scanner_snapshot(snapshot, generation, session_date, signal_end)
    config = _config(generation)
    confirmation_end = config.slot_datetime(
        session_date, config.SIGNAL_TO_CONFIRMATION[signal_end]
    )
    deadline = confirmation_end + timedelta(
        seconds=int(config.ENTRY_ACTIVATION_GRACE_SEC)
    )
    required_no_candle_observations = int(
        getattr(
            config,
            "CONFIRMATION_NO_CANDLE_OBSERVATIONS",
            MIN_NO_CANDLE_OBSERVATIONS,
        )
    )
    minimum_no_candle_age_sec = int(
        getattr(
            config,
            "CONFIRMATION_NO_CANDLE_MIN_AGE_SEC",
            MIN_NO_CANDLE_VERIFICATION_AGE_SEC,
        )
    )
    minimum_no_candle_time = confirmation_end + timedelta(
        seconds=minimum_no_candle_age_sec
    )
    minimum_observation_spacing_sec = float(
        getattr(
            config,
            "CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC",
            DEFAULT_NO_CANDLE_OBSERVATION_SPACING_SEC,
        )
    )
    observation_spacing_sec = max(
        minimum_observation_spacing_sec, float(retry_delay_sec)
    )
    candidates = list(snapshot.get("candidates") or [])
    expected_symbols = {
        str(candidate["tradingsymbol"]).strip().upper() for candidate in candidates
    }
    candidates_by_symbol = {
        str(candidate["tradingsymbol"]).strip().upper(): candidate
        for candidate in candidates
    }
    marker_path = _marker_path(generation, confirmation_end, snapshot)
    if marker_path.exists():
        marker = common.read_json(marker_path)
        _validate_reusable_final_marker(
            marker,
            marker_path=marker_path,
            snapshot=snapshot,
            generation=generation,
            session_date=session_date,
            signal_end=signal_end,
            confirmation_end=confirmation_end,
            deadline=deadline,
            minimum_no_candle_time=minimum_no_candle_time,
            expected_symbols=expected_symbols,
            config=config,
        )
        return marker
    bars = {
        symbol: bar
        for symbol, candidate in candidates_by_symbol.items()
        if (
            bar := _load_persisted_bar(
                candidate, session_date, confirmation_end
            )
        )
        is not None
    }
    outcomes: dict[str, dict[str, Any]] = {
        symbol: {
            "tradingsymbol": symbol,
            "state": "WRITTEN",
            "bar": bar,
            "error": "",
            "observed_at_ist": str(bar.get("fetched_at_ist", "")),
        }
        for symbol, bar in bars.items()
    }
    attempts = {symbol: 0 for symbol in expected_symbols}
    no_candle_observations = {symbol: 0 for symbol in expected_symbols}
    observation_history: dict[str, list[dict[str, str]]] = {
        symbol: [] for symbol in expected_symbols
    }
    scanner_complete = snapshot.get("state") == "SUCCESS"
    if scanner_complete and runtimes:
        now_before_fetch = common.now_ist()
        attempt_budget = (
            max(required_no_candle_observations, int(observations))
            if now_before_fetch >= minimum_no_candle_time
            else 1
        )
        for attempt in range(max(1, attempt_budget)):
            unresolved = sorted(expected_symbols - set(bars))
            if not unresolved:
                break
            fetched = fetch_candidates(
                [candidates_by_symbol[symbol] for symbol in unresolved],
                runtimes,
                config.slot_datetime(session_date, signal_end),
                confirmation_end,
            )
            for outcome in fetched:
                symbol = str(outcome["tradingsymbol"]).strip().upper()
                if symbol not in expected_symbols:
                    continue
                attempts[symbol] += 1
                if outcome["state"] == "NO_CANDLE":
                    no_candle_observations[symbol] += 1
                observation_history[symbol].append(
                    {
                        "state": str(outcome.get("state", "")),
                        "observed_at_ist": str(outcome.get("observed_at_ist", "")),
                        "error": str(outcome.get("error", "")),
                    }
                )
                outcomes[symbol] = outcome
                if outcome["state"] == "WRITTEN":
                    bars[symbol] = outcome["bar"]
            if attempt + 1 < max(1, attempt_budget) and expected_symbols - set(bars):
                time.sleep(observation_spacing_sec)

    written_symbols = set(bars)
    no_candle_symbols = {
        symbol
        for symbol, count in no_candle_observations.items()
        if symbol not in written_symbols and count > 0
    }
    evidence_cutoff = common.now_ist()
    verified_no_candle_symbols = {
        symbol
        for symbol in no_candle_symbols
        if no_candle_observations[symbol] >= required_no_candle_observations
        and attempts[symbol] == no_candle_observations[symbol]
        and _clean_no_candle_history(
            observation_history[symbol],
            required_observations=required_no_candle_observations,
            minimum_spacing_sec=minimum_observation_spacing_sec,
            not_before=minimum_no_candle_time,
            not_after=evidence_cutoff,
        )
        and evidence_cutoff >= minimum_no_candle_time
    }
    unverified_no_candle_symbols = no_candle_symbols - verified_no_candle_symbols
    invalid_symbols = {
        symbol
        for symbol, history in observation_history.items()
        if symbol not in written_symbols
        and any(item.get("state") == "INVALID_DATA" for item in history)
    }
    failed_symbols = {
        symbol
        for symbol, history in observation_history.items()
        if symbol not in written_symbols
        and any(item.get("state") == "FAILED" for item in history)
    }
    unexpected_missing = expected_symbols - (
        written_symbols | no_candle_symbols | invalid_symbols | failed_symbols
    )
    resolved_symbols = written_symbols | verified_no_candle_symbols
    preliminary_complete = bool(
        scanner_complete
        and resolved_symbols == expected_symbols
        and not unverified_no_candle_symbols
        and not invalid_symbols
        and not failed_symbols
        and not unexpected_missing
    )
    slot_data_path = common.equity_1m_slot_data_path(
        confirmation_end,
        generation=generation,
        scanner_sha256=scanner_snapshot_sha256(snapshot),
    )
    slot_data_sha256 = ""
    if preliminary_complete:
        slot_rows = [bars[symbol] for symbol in sorted(written_symbols)]
        slot_frame = pd.DataFrame(slot_rows)
        if slot_frame.empty:
            slot_frame = pd.DataFrame(
                columns=(
                    "timestamp",
                    "candle_start",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "tradingsymbol",
                    "instrument_token",
                    "exchange",
                    "source",
                    "data_contract",
                    "fetched_at_ist",
                )
            )
        slot_data_sha256 = _publish_slot_data_once(slot_data_path, slot_frame)

    # Publication time is sampled only after the immutable bar snapshot has
    # been persisted and hashed.  A marker cannot claim an on-time SUCCESS
    # based on a classification timestamp from before filesystem I/O.
    published_at = common.now_ist()
    future_observation_symbols: set[str] = set()
    for symbol, history in observation_history.items():
        try:
            if any(
                _to_ist(item["observed_at_ist"]) > pd.Timestamp(published_at)
                for item in history
            ):
                future_observation_symbols.add(symbol)
        except (KeyError, TypeError, ValueError):
            future_observation_symbols.add(symbol)
    if future_observation_symbols:
        invalid_symbols |= future_observation_symbols
    complete = bool(preliminary_complete and not future_observation_symbols)
    within_deadline = published_at <= deadline
    finalize_incomplete = bool(finalize_incomplete or published_at > deadline)
    state = (
        "SUCCESS"
        if complete and within_deadline
        else "LATE_COMPLETE"
        if complete
        else "BLOCKED_INCOMPLETE_DATA"
        if finalize_incomplete
        else "WAITING_INCOMPLETE_DATA"
    )
    source = "final" if complete or finalize_incomplete else "provisional"
    marker = {
        "schema_version": common.EQUITY_1M_SLOT_SCHEMA_VERSION,
        "feed_policy": FEED_POLICY_VERSION,
        "source": source,
        "state": state,
        "complete": complete,
        "within_deadline": within_deadline,
        "generation": generation,
        "strategy_version": config.STRATEGY_VERSION,
        "strategy_fingerprint": config.strategy_fingerprint(),
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "session_date": session_date.isoformat(),
        "signal_end": signal_end,
        "confirmation_end": config.SIGNAL_TO_CONFIRMATION[signal_end],
        "slot_ist": confirmation_end.isoformat(),
        "deadline_ist": deadline.isoformat(),
        "minimum_no_candle_verification_ist": minimum_no_candle_time.isoformat(),
        "published_at_ist": published_at.isoformat(timespec="microseconds"),
        "scanner_snapshot_sha256": scanner_snapshot_sha256(snapshot),
        "scanner_snapshot": snapshot,
        "candidate_contract_sha256": candidate_contract_sha256(snapshot),
        "candidate_symbol_set_sha256": common.symbol_set_sha256(expected_symbols),
        "candidate_count": len(expected_symbols),
        "candidate_symbols": sorted(expected_symbols),
        "written_count": len(written_symbols),
        "written_symbols": sorted(written_symbols),
        "no_candle_symbols": sorted(no_candle_symbols),
        "verified_no_candle_count": len(verified_no_candle_symbols),
        "verified_no_candle_symbols": sorted(verified_no_candle_symbols),
        "unverified_no_candle_symbols": sorted(unverified_no_candle_symbols),
        "minimum_no_candle_observations": required_no_candle_observations,
        "minimum_no_candle_verification_age_sec": minimum_no_candle_age_sec,
        "minimum_no_candle_observation_spacing_sec": minimum_observation_spacing_sec,
        "configured_no_candle_observation_spacing_sec": observation_spacing_sec,
        "candidate_resolution_policy": NO_CANDLE_RESOLUTION_POLICY,
        "verified_no_candle_cap": None,
        "written_bar_minimum_ratio": None,
        "resolved_count": len(resolved_symbols),
        "resolved_symbols": sorted(resolved_symbols),
        "invalid_symbols": sorted(invalid_symbols),
        "api_failed_symbols": sorted(failed_symbols),
        "unexpected_missing_symbols": sorted(unexpected_missing),
        "attempts_by_symbol": attempts,
        "no_candle_observations": no_candle_observations,
        "observation_history": observation_history,
        "errors": {
            symbol: str(outcome.get("error", ""))
            for symbol, outcome in sorted(outcomes.items())
            if outcome.get("error")
        },
        "slot_data_path": str(slot_data_path) if complete else "",
        "slot_data_sha256": slot_data_sha256 if complete else "",
    }
    if source == "final":
        return _publish_final_marker_once(marker_path, marker)
    return marker


def _build_runtimes(args: argparse.Namespace) -> list[AppRuntime]:
    credentials = common.discover_kite_credentials(max_apps=args.max_apps)

    def authenticate(credential: common.KiteCredential) -> AppRuntime:
        client = common.make_kite_client(credential, timeout_sec=args.timeout_sec)
        client.profile()
        return AppRuntime(
            app_name=credential.app_name,
            client=client,
            pace_seconds=max(0.34, float(args.request_interval_sec)),
        )

    authenticated: dict[str, AppRuntime] = {}
    failures: list[str] = []
    # Each credential belongs to a separate Kite app.  Validate them in
    # parallel so one dead app cannot consume most of the 90-second activation
    # window before the other permitted apps are even tried.
    with ThreadPoolExecutor(
        max_workers=len(credentials), thread_name_prefix="fno-equity-auth"
    ) as executor:
        futures = {
            executor.submit(authenticate, credential): credential
            for credential in credentials
        }
        for future in as_completed(futures):
            credential = futures[future]
            try:
                authenticated[credential.app_name] = future.result()
            except Exception as exc:
                failures.append(
                    f"{credential.app_name}:{type(exc).__name__}:{exc}"
                )

    runtimes = [
        authenticated[credential.app_name]
        for credential in credentials
        if credential.app_name in authenticated
    ]
    if not runtimes:
        raise RuntimeError("No authenticated Kite apps are usable: " + " | ".join(failures))
    return runtimes


def _prewarm_runtimes(
    args: argparse.Namespace,
    *,
    session: str,
    signal_end: str,
) -> list[AppRuntime] | None:
    """Try every configured app without terminating the scheduler on failure."""

    try:
        runtimes = _build_runtimes(args)
    except Exception as exc:
        common.publish_status(
            session,
            "DEGRADED",
            heartbeat_state="WAITING",
            phase="KITE_RUNTIME_PREWARM_FAILED",
            slot=signal_end,
            error=f"{type(exc).__name__}: {exc}",
        )
        return None
    common.publish_status(
        session,
        "RUNNING",
        phase="KITE_RUNTIMES_PREWARMED",
        slot=signal_end,
        usable_apps=len(runtimes),
    )
    return runtimes


def _resolve_signal_end(config: Any, value: str) -> str:
    normalized = str(value).replace(":", "")
    match = next(
        (
            signal_end
            for signal_end, confirmation_end in config.SIGNAL_TO_CONFIRMATION.items()
            if signal_end.replace(":", "") == normalized
            or confirmation_end.replace(":", "") == normalized
        ),
        None,
    )
    if match is None:
        raise ValueError(f"Unsupported FNO confirmation slot: {value}")
    return match


def _load_scanner(generation: str, session_date: date, signal_end: str) -> dict[str, Any]:
    path = scanner_slot_path(generation, session_date, signal_end)
    try:
        return common.read_json(path)
    except (OSError, ValueError, TypeError):
        return {}


def _render_report(generation: str, session_date: date, config: Any) -> str:
    rows = []
    for signal_end, confirmation_hhmm in config.SIGNAL_TO_CONFIRMATION.items():
        scanner = _load_scanner(generation, session_date, signal_end)
        marker = {}
        if scanner:
            confirmation_end = config.slot_datetime(session_date, confirmation_hhmm)
            path = _marker_path(generation, confirmation_end, scanner)
            if path.exists():
                marker = common.read_json(path)
        rows.append(
            f"{signal_end} | {confirmation_hhmm} | {marker.get('state', 'WAITING')} | "
            f"{marker.get('written_count', 0)}/{marker.get('candidate_count', 0)} | "
            f"{len(marker.get('verified_no_candle_symbols') or [])} | "
            f"{len(marker.get('unverified_no_candle_symbols') or [])} | "
            f"{len(marker.get('api_failed_symbols') or [])} | "
            f"{len(marker.get('invalid_symbols') or [])} | "
            f"{marker.get('published_at_ist', '')}"
        )
    return "\n".join(
        [
            f"# FnO {generation.upper()} durable equity 1-minute feed",
            "",
            f"Session: {session_date.isoformat()}",
            "Confirmation is a read-only consumer of these immutable markers and bar snapshots.",
            "",
            "Signal | Confirmation | State | Written | Ineligible no-candle | Unverified no-candle | API failed | Invalid | Published",
            "--- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---",
            *rows,
            "",
        ]
    )


def run(args: argparse.Namespace) -> int:
    generation = args.generation.strip().lower()
    config = _config(generation)
    if generation == "v6" and abs(
        float(args.boundary_buffer_sec)
        - float(config.CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC)
    ) > 1e-9:
        raise ValueError(
            "V6 completed-candle boundary buffer is fingerprint-locked to "
            f"{config.CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC} seconds."
        )
    config.validate_strategy()
    session_date = (
        date.fromisoformat(args.session_date)
        if args.session_date
        else common.now_ist().date()
    )
    if not args.allow_non_trading_day and not common.is_trading_day(
        session_date, common.load_holidays()
    ):
        return 0
    session = f"fno_{generation}_equity_1min_feed"
    report_path = common.LATEST_DIR / f"latest_fno_{generation}_equity_1min_feed.md"
    selected_slots = (
        [_resolve_signal_end(config, args.slot)]
        if args.slot
        else list(config.SIGNAL_TO_CONFIRMATION)
    )
    processed: set[str] = set()
    runtimes: list[AppRuntime] | None = None
    runtime_retry_not_before = 0.0

    def maybe_prewarm(signal_end: str) -> None:
        nonlocal runtimes, runtime_retry_not_before
        if runtimes is not None or time.monotonic() < runtime_retry_not_before:
            return
        # A failed parallel profile sweep is retried, but not on every
        # scheduler poll.  The daemon remains alive so a transient auth/API
        # outage can recover inside the activation window.
        runtimes = _prewarm_runtimes(
            args,
            session=session,
            signal_end=signal_end,
        )
        if runtimes is None:
            runtime_retry_not_before = time.monotonic() + max(
                2.0, min(15.0, float(args.timeout_sec))
            )

    while len(processed) < len(selected_slots):
        now = common.now_ist()
        made_progress = False
        for signal_end in selected_slots:
            if signal_end in processed:
                continue
            confirmation_hhmm = config.SIGNAL_TO_CONFIRMATION[signal_end]
            confirmation_end = config.slot_datetime(session_date, confirmation_hhmm)
            due = confirmation_end + timedelta(seconds=args.boundary_buffer_sec)
            if now < due:
                # Scheduled sessions can authenticate as soon as the scanner
                # publishes a usable candidate set.  They still cannot fetch a
                # candle until the completed-candle boundary below.  Keep
                # --once as a side-effect-free early readiness probe.
                if not args.once and runtimes is None:
                    prewarm_snapshot = _load_scanner(
                        generation, session_date, signal_end
                    )
                    if (
                        prewarm_snapshot.get("state") == "SUCCESS"
                        and bool(prewarm_snapshot.get("candidates"))
                    ):
                        try:
                            validate_scanner_snapshot(
                                prewarm_snapshot,
                                generation,
                                session_date,
                                signal_end,
                            )
                        except (KeyError, TypeError, ValueError) as exc:
                            common.publish_status(
                                session,
                                "DEGRADED",
                                heartbeat_state="WAITING",
                                phase="SCANNER_PREWARM_VALIDATION_FAILED",
                                slot=signal_end,
                                error=f"{type(exc).__name__}: {exc}",
                            )
                        else:
                            maybe_prewarm(signal_end)
                common.publish_heartbeat(
                    session,
                    "WAITING",
                    phase="WAIT_COMPLETED_CANDLE_BOUNDARY",
                    slot=signal_end,
                    runtimes_ready=len(runtimes or []),
                )
                if args.once:
                    return 2
                continue
            snapshot = _load_scanner(generation, session_date, signal_end)
            if not snapshot:
                common.publish_heartbeat(
                    session, "WAITING", phase="WAIT_SCANNER", slot=signal_end
                )
                if args.once:
                    return 2
                continue
            deadline = confirmation_end + timedelta(
                seconds=config.ENTRY_ACTIVATION_GRACE_SEC
            )
            has_candidates = bool(snapshot.get("candidates")) and snapshot.get("state") == "SUCCESS"
            if has_candidates and runtimes is None and now <= deadline:
                maybe_prewarm(signal_end)
                if runtimes is None:
                    common.publish_heartbeat(
                        session,
                        "WAITING",
                        phase="WAIT_KITE_RUNTIME_PREWARM",
                        slot=signal_end,
                    )
                    if args.once:
                        return 2
                    continue
            marker = produce_slot(
                snapshot,
                generation,
                session_date,
                signal_end,
                runtimes or [],
                observations=args.observations,
                retry_delay_sec=args.retry_delay_sec,
                finalize_incomplete=bool(common.now_ist() > deadline),
            )
            if str(marker.get("source")) != "final":
                common.publish_heartbeat(
                    session,
                    "WAITING",
                    phase="WAIT_COMPLETED_BAR",
                    slot=signal_end,
                    written=marker.get("written_count", 0),
                    expected=marker.get("candidate_count", 0),
                )
                if args.once:
                    return 2
                continue
            processed.add(signal_end)
            made_progress = True
            common.publish_status(
                session,
                marker["state"],
                phase="SLOT_DONE",
                slot=signal_end,
                written=marker["written_count"],
                expected=marker["candidate_count"],
            )
            common.atomic_write_text(report_path, _render_report(generation, session_date, config))
            if args.once:
                return 0 if marker.get("state") == "SUCCESS" else 2
        if not made_progress:
            if now.date() != session_date or now.time() >= datetime.strptime("09:50", "%H:%M").time():
                return 2
            time.sleep(max(0.2, float(args.poll_sec)))
    common.publish_status(session, "DONE", processed_slots=len(processed))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generation", choices=("v5", "v6"), default=DEFAULT_GENERATION)
    parser.add_argument("--session-date", default="")
    parser.add_argument("--slot", default="")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--boundary-buffer-sec", type=float, default=3.0)
    parser.add_argument(
        "--observations", type=int, default=MIN_NO_CANDLE_OBSERVATIONS
    )
    parser.add_argument(
        "--retry-delay-sec",
        type=float,
        default=DEFAULT_NO_CANDLE_OBSERVATION_SPACING_SEC,
    )
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--allow-non-trading-day", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        session = f"fno_{args.generation}_equity_1min_feed"
        common.publish_status(
            session,
            "FAILED",
            heartbeat_state="CRASHED",
            error=f"{type(exc).__name__}: {exc}",
        )
        print(f"[FATAL] {type(exc).__name__}: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
