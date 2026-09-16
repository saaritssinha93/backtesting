"""Fetch live/historical ATM equity-option OHLCV+OI at 5m and 1m resolution.

The cash-equity completed five-minute close is the sole ATM anchor.  This
module resolves CE and PE deterministically from an archived NFO master,
fails closed when the requested expiry is absent, and never places orders.
"""

from __future__ import annotations

import argparse
import json
import math
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_fetch_5min as legacy
from eqidv2_runtime_paths import DATA_5M_DIR
from fno_oi_fetch_5min_fast_shadow import (
    AppLane,
    AppLaneSession,
    _auth_failure_text,
    _choose_retry_lane,
    _historical_call,
)
from fno_v13_v5_derivative_data import (
    MASTER_COLUMNS,
    build_option_contract_map,
    build_option_fetch_plan,
    normalize_minute_candles,
    normalize_nfo_master,
)


SESSION = "fno_options_atm_fetch_5min"
SCHEMA_VERSION = "fno_options_atm_slot_v1"
OPTIONS_SLOT_SCHEMA_VERSION = SCHEMA_VERSION
MAP_SCHEMA_VERSION = "fno_options_atm_map_v1"
ENGINE_VERSION = "fno_options_atm_dual_interval_v1"
OPTIONS_RAW_DATA_VERSION = "fno_options_raw_v1"
OPTIONS_5M_RAW_DATA_VERSION = "fno_options_raw_5m_v1"
OPTIONS_1M_RAW_DATA_VERSION = "fno_options_raw_1m_v1"
RAW_OPTIONS_5M_DIR = common.FNO_ROOT / "raw_options_5m"
RAW_OPTIONS_1M_DIR = common.FNO_ROOT / "raw_options_1m"
OPTIONS_SLOT_DIR = common.FNO_ROOT / "options_slot_ready"
OPTIONS_MAP_DIR = common.FNO_ROOT / "options_atm_map"
FIRST_SLOT = dtime(9, 20)
LAST_SLOT = dtime(15, 30)
INTERVAL_MINUTES = {"5minute": 5, "minute": 1}
INTERVAL_LABEL = {"5minute": "5m", "minute": "1m"}

OPTION_EXTRA_COLUMNS = (
    "candle_interval",
    "instrument_type",
    "strike",
    "strike_offset",
    "spot_price",
    "spot_source",
    "spot_slot",
    "atm_distance",
    "expiry_policy",
    "mapping_status",
    "master_date",
    "master_sha256",
)
OPTIONS_RAW_COLUMNS = tuple(common.RAW_COLUMNS) + OPTION_EXTRA_COLUMNS
OPTIONS_NUMERIC_COLUMNS = (
    "instrument_token", "exchange_token", "days_to_expiry", "lot_size",
    "tick_size", "open", "high", "low", "close", "volume", "oi",
    "strike", "strike_offset", "spot_price", "atm_distance",
)
MAP_IDENTITY_COLUMNS = (
    "underlying",
    "instrument_type",
    "expiry",
    "strike",
    "tradingsymbol",
    "instrument_token",
    "mapping_status",
    "spot_slot",
    "spot_price",
    "expiry_policy",
    "strike_offset",
)


class ContractSetDriftError(RuntimeError):
    """Raised when a rerun resolves a different contract set for the same slot."""


class CashSlotNotReadyError(RuntimeError):
    """Raised while the authoritative cash slot exists but is not complete yet."""


def _slot(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def _json_hash(value: Any) -> str:
    return common.canonical_json_sha256(value)


def _frame_records(frame: pd.DataFrame, columns: Sequence[str]) -> list[dict[str, Any]]:
    work = frame.reindex(columns=list(columns)).copy()
    for column in work.columns:
        if pd.api.types.is_datetime64_any_dtype(work[column]):
            work[column] = work[column].map(
                lambda value: "" if pd.isna(value) else pd.Timestamp(value).isoformat()
            )
    work = work.fillna("").astype(str).sort_values(list(columns), kind="stable")
    return work.to_dict("records")


def _archive_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Canonicalize numeric values across Parquet dtype round-trips."""
    work = frame.reindex(columns=list(OPTIONS_RAW_COLUMNS)).copy()

    def canonical_number(value: Any) -> str:
        if pd.isna(value):
            return ""
        number = Decimal(str(value)).normalize()
        return "0" if number == 0 else format(number, "f")

    for column in OPTIONS_NUMERIC_COLUMNS:
        work[column] = work[column].map(canonical_number)
    return _frame_records(work, OPTIONS_RAW_COLUMNS)


def option_contract_path(interval: str, tradingsymbol: str) -> Path:
    root = RAW_OPTIONS_5M_DIR if interval == "5minute" else RAW_OPTIONS_1M_DIR
    return root / f"{common.safe_contract_stem(tradingsymbol)}_{interval}.parquet"


def option_map_path(slot_end: Any) -> Path:
    stamp = _slot(slot_end)
    return OPTIONS_MAP_DIR / stamp.date().isoformat() / f"slot_{stamp.strftime('%H%M')}.parquet"


def option_marker_path(slot_end: Any) -> Path:
    stamp = _slot(slot_end)
    return OPTIONS_SLOT_DIR / f"slot_{stamp.strftime('%Y%m%d_%H%M')}.json"


def parse_intervals(value: str) -> tuple[str, ...]:
    normalized = value.strip().lower()
    if normalized in {"both", "all", "5minute,minute", "minute,5minute"}:
        return ("5minute", "minute")
    aliases = {"5m": "5minute", "5minute": "5minute", "1m": "minute", "minute": "minute"}
    parts = [part.strip() for part in normalized.split(",") if part.strip()]
    try:
        result = tuple(dict.fromkeys(aliases[part] for part in parts))
    except KeyError as exc:
        raise argparse.ArgumentTypeError(f"Unsupported interval: {exc.args[0]}") from exc
    if not result:
        raise argparse.ArgumentTypeError("At least one interval is required.")
    return result


def load_cash_marker(slot_end: Any) -> dict[str, Any]:
    path = common.cash_slot_path(_slot(slot_end))
    if not path.exists():
        raise FileNotFoundError(f"Completed cash-equity marker is missing: {path}")
    marker = common.read_json(path)
    if str(marker.get("source", "")).lower() != "final" or not bool(marker.get("complete")):
        raise CashSlotNotReadyError(
            f"Cash-equity marker is not final and complete: {path}"
        )
    return marker


def _load_spot_5m(symbol: str, slot_end: pd.Timestamp) -> float:
    path = DATA_5M_DIR / f"{symbol}_stocks_indicators_5min.parquet"
    if not path.exists():
        raise FileNotFoundError(str(path))
    frame = pd.read_parquet(path, columns=["date", "close"])
    timestamps = common._to_ist(frame["date"])
    values = pd.to_numeric(frame.loc[timestamps.eq(slot_end), "close"], errors="coerce")
    values = values[np.isfinite(values) & values.gt(0)]
    if len(values) != 1:
        raise ValueError(f"Expected one exact 5m cash close for {symbol} at {slot_end.isoformat()}")
    return float(values.iloc[0])


def _load_spot_1m(symbol: str, slot_end: pd.Timestamp) -> float:
    path = common.equity_1m_path(slot_end.date(), symbol)
    if not path.exists():
        raise FileNotFoundError(str(path))
    frame = pd.read_parquet(path)
    time_column = "timestamp" if "timestamp" in frame.columns else "date"
    timestamps = common._to_ist(frame[time_column])
    expected = pd.date_range(slot_end - pd.Timedelta(minutes=4), slot_end, freq="1min")
    selected = frame.loc[timestamps.isin(expected)].copy()
    selected["_ts"] = pd.Series(
        timestamps[timestamps.isin(expected)].array, index=selected.index
    )
    selected = selected.drop_duplicates("_ts", keep="last").sort_values("_ts")
    if len(selected) != 5 or set(pd.DatetimeIndex(selected["_ts"])) != set(expected):
        raise ValueError(f"Five exact 1m cash bars are unavailable for {symbol} at {slot_end.isoformat()}")
    value = pd.to_numeric(selected["close"], errors="coerce").iloc[-1]
    if not np.isfinite(value) or float(value) <= 0:
        raise ValueError(f"Invalid 1m cash close for {symbol} at {slot_end.isoformat()}")
    return float(value)


def load_spot(symbol: str, slot_end: Any, source: str = "auto") -> tuple[float, str]:
    stamp = _slot(slot_end)
    failures: list[str] = []
    loaders = {
        "5m": (_load_spot_5m, "EQUITY_5M"),
        "1m": (_load_spot_1m, "EQUITY_1M"),
    }
    choices = ("5m", "1m") if source == "auto" else (source,)
    for choice in choices:
        loader, label = loaders[choice]
        try:
            return loader(symbol, stamp), label
        except (FileNotFoundError, KeyError, TypeError, ValueError) as exc:
            failures.append(f"{choice}:{type(exc).__name__}:{exc}")
    raise ValueError(" | ".join(failures))


def _last_tuesday(year: int, month: int) -> date:
    first_next = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    day = first_next - timedelta(days=1)
    return day - timedelta(days=(day.weekday() - 1) % 7)


def intended_expiry(
    options: pd.DataFrame, underlying: str, session_day: date, policy: str
) -> pd.Timestamp:
    available = sorted(
        pd.to_datetime(
            options.loc[options["underlying"].eq(underlying), "expiry"], errors="coerce"
        ).dropna().dt.normalize().unique()
    )
    if str(policy).upper().endswith("WEEKLY"):
        per_month: dict[tuple[int, int], int] = {}
        for item in available:
            stamp = pd.Timestamp(item)
            key = (stamp.year, stamp.month)
            per_month[key] = per_month.get(key, 0) + 1
        if not per_month or max(per_month.values()) < 2:
            raise ValueError("WEEKLY_SERIES_NOT_DEMONSTRATED")
        future = [pd.Timestamp(item) for item in available if pd.Timestamp(item).date() >= session_day]
        if future:
            return min(future)
        weekday = pd.Timestamp(available[-1]).weekday()
        return pd.Timestamp(session_day + timedelta(days=(weekday - session_day.weekday()) % 7))

    year, month = session_day.year, session_day.month
    theoretical = _last_tuesday(year, month)
    if session_day > theoretical:
        month = 1 if month == 12 else month + 1
        year = year + 1 if month == 1 else year
        theoretical = _last_tuesday(year, month)
    same_month = [
        pd.Timestamp(item)
        for item in available
        if pd.Timestamp(item).year == year and pd.Timestamp(item).month == month
    ]
    return max(same_month) if same_month else pd.Timestamp(theoretical)


def _unresolved(symbol: str, option_type: str, stamp: pd.Timestamp, status: str, policy: str) -> dict[str, Any]:
    return {
        "underlying": symbol,
        "instrument_type": option_type,
        "expiry": pd.NaT,
        "strike": np.nan,
        "tradingsymbol": "",
        "instrument_token": pd.NA,
        "exchange_token": pd.NA,
        "lot_size": pd.NA,
        "tick_size": np.nan,
        "strike_offset": 0,
        "spot_price": np.nan,
        "spot_source": "",
        "spot_slot": stamp,
        "atm_distance": np.nan,
        "expiry_policy": policy,
        "mapping_status": status,
    }


def resolve_atm_contracts(
    master: pd.DataFrame,
    underlyings: Sequence[str],
    slot_end: Any,
    *,
    expiry_policy: str = "monthly",
    strike_window: int = 0,
    spot_source: str = "auto",
) -> pd.DataFrame:
    stamp = _slot(slot_end)
    options = master.loc[master["instrument_type"].isin(["CE", "PE"])].copy()
    master_day = pd.to_datetime(master["master_date"], errors="coerce").dropna()
    acquisition_day = master_day.max().date() if not master_day.empty else common.now_ist().date()
    rows: list[dict[str, Any]] = []
    for symbol_value in underlyings:
        symbol = str(symbol_value).upper().strip()
        try:
            spot, source = load_spot(symbol, stamp, spot_source)
        except ValueError:
            rows.extend(_unresolved(symbol, leg, stamp, "NO_SPOT_PRICE_FOR_SLOT", expiry_policy) for leg in ("CE", "PE"))
            continue
        try:
            expiry = intended_expiry(options, symbol, stamp.date(), expiry_policy)
        except ValueError as exc:
            rows.extend(_unresolved(symbol, leg, stamp, str(exc), expiry_policy) for leg in ("CE", "PE"))
            continue

        trades = pd.DataFrame(
            [
                {
                    "_trade_id": f"{stamp.isoformat()}|{symbol}|{side}", "sid": f"ATM_{leg}",
                    "day": stamp.date().isoformat(), "profile": "ATM_FETCH", "strategy_version": ENGINE_VERSION,
                    "tradingsymbol": symbol, "side": side, "_required_expiry": expiry,
                    "_equity_entry_price": spot, "_entry_ts": stamp, "_exit_ts": stamp,
                    "contract_month": expiry.strftime("%Y-%m"),
                }
                for side, leg in (("LONG", "CE"), ("SHORT", "PE"))
            ]
        )
        mapped = build_option_contract_map(trades, master, master_date=acquisition_day)
        plan = build_option_fetch_plan(mapped, master, strike_window=max(0, int(strike_window)))
        if plan.empty:
            for record in mapped.to_dict("records"):
                rows.append(_unresolved(symbol, record["required_option_type"], stamp, record["mapping_status"], expiry_policy))
            continue
        primary_by_leg = {
            str(record["required_option_type"]): record for record in mapped.to_dict("records")
        }
        for record in plan.to_dict("records"):
            leg = str(record["instrument_type"])
            primary = primary_by_leg[leg]
            leg_strikes = sorted(
                pd.to_numeric(
                    options.loc[
                        options["underlying"].eq(symbol)
                        & options["expiry"].eq(expiry)
                        & options["instrument_type"].eq(leg),
                        "strike",
                    ], errors="coerce"
                ).dropna().unique()
            )
            primary_index = leg_strikes.index(float(primary["option_strike"]))
            strike_index = leg_strikes.index(float(record["strike"]))
            selected = options.loc[options["instrument_token"].eq(int(record["instrument_token"]))].iloc[0]
            rows.append(
                {
                    **record,
                    "exchange_token": selected["exchange_token"],
                    "strike_offset": strike_index - primary_index,
                    "spot_price": spot,
                    "spot_source": source,
                    "spot_slot": stamp,
                    "atm_distance": abs(float(record["strike"]) - spot),
                    "expiry_policy": expiry_policy,
                    "mapping_status": "MAPPED_ATM" if strike_index == primary_index else "MAPPED_WINDOW",
                }
            )
    if not rows:
        return pd.DataFrame(columns=list(MAP_IDENTITY_COLUMNS))
    return pd.DataFrame(rows).sort_values(
        ["underlying", "instrument_type", "expiry", "strike"], kind="stable", na_position="last"
    ).reset_index(drop=True)


def load_or_acquire_master(
    session_day: date, args: argparse.Namespace, lane_session: AppLaneSession
) -> tuple[pd.DataFrame, str, list[AppLane], list[str], bool]:
    dated = common.MASTER_DIR / f"options_instrument_master_{session_day.isoformat()}.parquet"
    fallback = common.MASTER_DIR / f"instrument_master_{session_day.isoformat()}.parquet"
    research = (
        common.FNO_ROOT / "strategy_research" / "v13_corrected_v5" / "derivative_market_data"
        / "instrument_master" / f"nfo_master_{session_day.isoformat()}.parquet"
    )
    for path in (dated, fallback, research):
        if path.exists():
            frame = pd.read_parquet(path)
            if "underlying" not in frame.columns or "master_date" not in frame.columns:
                frame = normalize_nfo_master(frame.to_dict("records"), master_date=session_day)
            if not frame["instrument_type"].isin(["CE", "PE"]).any():
                continue
            digest = _json_hash(_frame_records(frame, [c for c in MASTER_COLUMNS if c in frame.columns]))
            if not args.dry_run and path != dated:
                common.atomic_write_parquet(frame, dated)
                option_symbols = sorted(
                    frame.loc[frame["instrument_type"].isin(["CE", "PE"]), "tradingsymbol"]
                    .astype(str).unique().tolist()
                )
                common.atomic_write_json(dated.with_suffix(".json"), {
                    "master_date": pd.to_datetime(frame["master_date"], errors="coerce").max().date().isoformat(),
                    "master_sha256": digest, "symbol_set_sha256": _json_hash(option_symbols),
                    "rows": len(frame), "source_path": str(path),
                })
            return frame, digest, [], [], True

    lanes, failures, reused = lane_session.acquire(args, auth_tag="[OPT-ATM]")
    last_error: Exception | None = None
    records: list[dict[str, Any]] = []
    for lane in lanes:
        try:
            lane.pace()
            records = list(lane.next_client().instruments("NFO"))
            if records:
                break
        except Exception as exc:
            last_error = exc
            if _auth_failure_text(exc):
                lane._runtime_auth_failure.set()
    if not records:
        raise RuntimeError(f"Unable to acquire NFO master: {last_error}")
    frame = normalize_nfo_master(records, master_date=common.now_ist().date())
    digest = _json_hash(_frame_records(frame, list(MASTER_COLUMNS)))
    if not args.dry_run:
        common.atomic_write_parquet(frame, dated)
        option_symbols = sorted(
            frame.loc[frame["instrument_type"].isin(["CE", "PE"]), "tradingsymbol"]
            .astype(str).unique().tolist()
        )
        common.atomic_write_json(dated.with_suffix(".json"), {
            "master_date": common.now_ist().date().isoformat(), "master_sha256": digest,
            "symbol_set_sha256": _json_hash(option_symbols), "rows": len(frame),
            "source": "kite_instruments_nfo",
        })
    return frame, digest, lanes, failures, reused


def persist_contract_map(frame: pd.DataFrame, slot_end: Any, *, dry_run: bool) -> tuple[Path, str]:
    path = option_map_path(slot_end)
    digest = _json_hash(_frame_records(frame, MAP_IDENTITY_COLUMNS))
    if path.exists():
        existing = pd.read_parquet(path)
        old_digest = _json_hash(_frame_records(existing, MAP_IDENTITY_COLUMNS))
        if old_digest != digest:
            old_records = _frame_records(existing, MAP_IDENTITY_COLUMNS)
            new_records = _frame_records(frame, MAP_IDENTITY_COLUMNS)
            key_columns = ("underlying", "instrument_type", "expiry_policy", "strike_offset")
            old_by_key = {
                tuple(row[column] for column in key_columns): row for row in old_records
            }
            new_by_key = {
                tuple(row[column] for column in key_columns): row for row in new_records
            }
            old_mapped = {
                key: row
                for key, row in old_by_key.items()
                if str(row["mapping_status"]).startswith("MAPPED")
            }
            # A slot may first run before its cash-price source is ready.  Such
            # a run records unresolved placeholders.  Permit those placeholders
            # to become mapped later, but never alter or remove a contract that
            # was already mapped for the slot.
            unresolved_upgrade = (
                set(old_by_key) == set(new_by_key)
                and any(
                    not str(row["mapping_status"]).startswith("MAPPED")
                    and str(new_by_key[key]["mapping_status"]).startswith("MAPPED")
                    for key, row in old_by_key.items()
                )
                and all(new_by_key.get(key) == row for key, row in old_mapped.items())
            )
            if unresolved_upgrade and not dry_run:
                common.atomic_write_parquet(frame, path)
                verify = pd.read_parquet(path)
                if _json_hash(_frame_records(verify, MAP_IDENTITY_COLUMNS)) != digest:
                    raise IOError(f"ATM map readback verification failed: {path}")
                return path, digest
            old = set(tuple(row.values()) for row in old_records)
            new = set(tuple(row.values()) for row in new_records)
            old_symbols = {row["tradingsymbol"] or row["underlying"] for row in old_records}
            new_symbols = {row["tradingsymbol"] or row["underlying"] for row in new_records}
            raise ContractSetDriftError(
                f"Contract-set drift at {_slot(slot_end).isoformat()}: "
                f"added={sorted(new_symbols-old_symbols)} removed={sorted(old_symbols-new_symbols)} "
                f"changed_rows={len(new-old) + len(old-new)}"
            )
        return path, digest
    if not dry_run:
        common.atomic_write_parquet(frame, path)
        verify = pd.read_parquet(path)
        if _json_hash(_frame_records(verify, MAP_IDENTITY_COLUMNS)) != digest:
            raise IOError(f"ATM map readback verification failed: {path}")
    return path, digest


def project_requests(contract_count: int, interval_count: int, args: argparse.Namespace, lane_count: int) -> dict[str, Any]:
    initial = int(contract_count) * int(interval_count)
    attempt_count = max(
        common.MIN_NO_CANDLE_FETCH_ATTEMPTS,
        int(args.max_retries),
        1 + max(0, int(args.slot_retry_attempts)),
    )
    attempts = initial * attempt_count
    pace = max(0.34, float(args.request_interval_sec))
    projected = math.ceil(attempts / max(1, lane_count)) * pace + float(args.timeout_sec)
    budget = 300.0 - max(0.0, float(args.boundary_buffer_sec))
    return {"initial_requests": initial, "max_requests": attempts, "projected_worst_case_sec": projected, "budget_sec": budget, "accepted": projected <= budget}


def normalize_option_candles(
    records: Iterable[Mapping[str, Any]], contract: Mapping[str, Any], interval: str,
    slot_end: Any, *, fetched_at: datetime, master_date: date, master_sha256: str,
) -> pd.DataFrame:
    stamp = _slot(slot_end)
    base = normalize_minute_candles(records, contract, fetched_at=fetched_at)
    if base.empty:
        return pd.DataFrame(columns=list(OPTIONS_RAW_COLUMNS))
    starts = common._to_ist(base["timestamp"])
    minutes = INTERVAL_MINUTES[interval]
    ends = starts + pd.Timedelta(minutes=minutes)
    expected = {stamp} if interval == "5minute" else set(pd.date_range(stamp - pd.Timedelta(minutes=4), stamp, freq="1min"))
    base = base.loc[ends.isin(expected)].copy()
    starts = starts[ends.isin(expected)]
    ends = ends[ends.isin(expected)]
    if base.empty:
        return pd.DataFrame(columns=list(OPTIONS_RAW_COLUMNS))
    base["timestamp"] = pd.Series(ends.array, index=base.index)
    base["candle_start"] = pd.Series(starts.array, index=base.index)
    for column in ("open", "high", "low", "close", "volume", "oi"):
        base[column] = pd.to_numeric(base[column], errors="coerce")
    valid_ohlc = base[["open", "high", "low", "close"]].notna().all(axis=1)
    valid_ohlc &= base["high"].ge(base[["open", "close", "low"]].max(axis=1))
    valid_ohlc &= base["low"].le(base[["open", "close", "high"]].min(axis=1))
    base["quality_state"] = np.select(
        [~valid_ohlc, base["volume"].lt(0), base["oi"].isna(), base["oi"].lt(0)],
        ["INVALID_OHLC", "INVALID_VOLUME", "MISSING_OI", "NEGATIVE_OI"], default="VALID",
    )
    expiry = pd.Timestamp(contract["expiry"]).normalize()
    base["exchange_token"] = contract.get("exchange_token", pd.NA)
    base["contract_month"] = expiry.strftime("%Y-%m")
    base["days_to_expiry"] = (expiry.date() - stamp.date()).days
    base["is_index_future"] = False
    base["fetch_timestamp"] = pd.Timestamp(fetched_at)
    base["source"] = "kite_historical"
    base["data_version"] = OPTIONS_5M_RAW_DATA_VERSION if interval == "5minute" else OPTIONS_1M_RAW_DATA_VERSION
    base["candle_interval"] = interval
    for column in ("strike_offset", "spot_price", "spot_source", "spot_slot", "atm_distance", "expiry_policy", "mapping_status"):
        base[column] = contract.get(column)
    base["master_date"] = pd.Timestamp(master_date)
    base["master_sha256"] = master_sha256
    return base.reindex(columns=list(OPTIONS_RAW_COLUMNS)).sort_values("timestamp").reset_index(drop=True)


_WRITE_LOCKS: dict[str, threading.Lock] = {}
_WRITE_LOCKS_GUARD = threading.Lock()


def _persist_and_verify(
    path: Path, existing: pd.DataFrame | None, incoming: pd.DataFrame
) -> pd.DataFrame:
    combined = common.merge_contract_rows(existing, incoming).reindex(
        columns=list(OPTIONS_RAW_COLUMNS)
    )
    common.atomic_write_parquet(combined, path)
    check = pd.read_parquet(path)
    keys = set(
        zip(incoming["instrument_token"].astype(int), common._to_ist(incoming["timestamp"]))
    )
    check_times = common._to_ist(check["timestamp"])
    observed = set(zip(check["instrument_token"].astype(int), check_times))
    selected = check.loc[
        [
            (int(token), stamp) in keys
            for token, stamp in zip(check["instrument_token"], check_times)
        ]
    ].reindex(columns=list(OPTIONS_RAW_COLUMNS))
    if (
        not keys.issubset(observed)
        or _archive_records(selected) != _archive_records(incoming)
    ):
        raise IOError(f"Option archive readback failed: {path}")
    return combined


class CanonicalArchiveCache:
    """Warm, verified archive frames; memory advances only after atomic readback."""

    def __init__(self) -> None:
        self._frames: dict[str, pd.DataFrame | None] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def preload(self, plan: pd.DataFrame, intervals: Sequence[str]) -> int:
        paths = [
            option_contract_path(interval, str(row["tradingsymbol"]))
            for row in plan.to_dict("records")
            for interval in intervals
        ]
        loaded = 0
        for path in sorted(set(paths), key=str):
            key = str(path)
            with self._guard:
                if key in self._frames:
                    continue
            frame = pd.read_parquet(path) if path.exists() else None
            with self._guard:
                self._frames.setdefault(key, frame)
                self._locks.setdefault(key, threading.Lock())
            loaded += 1
        return loaded

    def persist(self, path: Path, incoming: pd.DataFrame) -> int:
        key = str(path)
        with self._guard:
            lock = self._locks.setdefault(key, threading.Lock())
            if key not in self._frames:
                self._frames[key] = pd.read_parquet(path) if path.exists() else None
        with lock:
            combined = _persist_and_verify(path, self._frames[key], incoming)
            self._frames[key] = combined
        return len(incoming)


def persist_option_rows(path: Path, incoming: pd.DataFrame) -> int:
    with _WRITE_LOCKS_GUARD:
        lock = _WRITE_LOCKS.setdefault(str(path), threading.Lock())
    with lock:
        existing = pd.read_parquet(path) if path.exists() else None
        _persist_and_verify(path, existing, incoming)
    return len(incoming)


def fetch_contracts(
    plan: pd.DataFrame, intervals: Sequence[str], slot_end: Any, lanes: list[AppLane],
    args: argparse.Namespace, *, master_date: date, master_sha256: str,
    archive_cache: CanonicalArchiveCache | None = None,
) -> list[dict[str, Any]]:
    work: queue.Queue[tuple[dict[str, Any], str]] = queue.Queue()
    for contract in plan.to_dict("records"):
        if str(contract.get("mapping_status", "")).startswith("MAPPED"):
            for interval in intervals:
                work.put((contract, interval))
    outcomes: list[dict[str, Any]] = []
    outcome_lock = threading.Lock()
    writer_count = int(args.writer_workers)
    writer_pool = ThreadPoolExecutor(
        max_workers=writer_count, thread_name_prefix="opt-atm-writer"
    )

    def worker(lane: AppLane, client: Any) -> None:
        while True:
            try:
                contract, interval = work.get_nowait()
            except queue.Empty:
                return
            apps: list[str] = []
            last_error = ""
            rows = pd.DataFrame(columns=list(OPTIONS_RAW_COLUMNS))
            use_lane = lane
            attempt_count = max(
                common.MIN_NO_CANDLE_FETCH_ATTEMPTS,
                int(args.max_retries),
                1 + max(0, int(args.slot_retry_attempts)),
            )
            for attempt in range(attempt_count):
                apps.append(use_lane.app_name)
                try:
                    stamp = _slot(slot_end)
                    records = _historical_call(
                        use_lane, client if use_lane is lane else use_lane.next_client(), contract,
                        (stamp - pd.Timedelta(minutes=5)).to_pydatetime(), stamp.to_pydatetime(),
                        max_retries=1, interval=interval,
                    )
                    rows = normalize_option_candles(
                        records, contract, interval, stamp, fetched_at=common.now_ist(),
                        master_date=master_date, master_sha256=master_sha256,
                    )
                    expected = 1 if interval == "5minute" else 5
                    if len(rows) == expected and rows["quality_state"].eq("VALID").all():
                        break
                    last_error = f"expected={expected} observed={len(rows)}"
                except Exception as exc:
                    last_error = f"{type(exc).__name__}:{exc}"
                    common.publish_heartbeat(
                        SESSION, "RUNNING", phase=f"FETCH_SLOT_RETRY_{attempt + 1}",
                        slot_ist=_slot(slot_end).isoformat(), tradingsymbol=contract["tradingsymbol"],
                        interval=interval,
                    )
                    if _auth_failure_text(last_error):
                        use_lane._runtime_auth_failure.set()
                        use_lane = _choose_retry_lane(lanes, apps)
                    elif any(text in str(exc).lower() for text in ("429", "too many requests", "rate limit")):
                        time.sleep(max(2.0, 2.0 ** (attempt + 1)))
                    else:
                        time.sleep(min(8.0, 0.75 * (2 ** attempt)))
            state = "FAILED"
            written = 0
            if not rows.empty and rows["quality_state"].eq("VALID").all():
                try:
                    writer = archive_cache.persist if archive_cache is not None else persist_option_rows
                    written = writer_pool.submit(
                        writer,
                        option_contract_path(interval, contract["tradingsymbol"]),
                        rows,
                    ).result()
                    state = "VERIFIED"
                except Exception as exc:
                    last_error = f"{type(exc).__name__}:{exc}"
            elif not rows.empty:
                state = "INVALID"
            with outcome_lock:
                outcomes.append({
                    "tradingsymbol": contract["tradingsymbol"], "underlying": contract["underlying"],
                    "interval": interval, "state": state, "rows_written": written,
                    "apps_attempted": "|".join(apps), "error": last_error,
                })
            work.task_done()

    try:
        with ThreadPoolExecutor(max_workers=sum(len(lane.clients) for lane in lanes), thread_name_prefix="opt-atm") as pool:
            futures = [pool.submit(worker, lane, client) for lane in lanes for client in lane.clients]
            for future in as_completed(futures):
                future.result()
    finally:
        writer_pool.shutdown(wait=True)
    return sorted(outcomes, key=lambda item: (item["underlying"], item["tradingsymbol"], item["interval"]))


def write_latest_report(marker: Mapping[str, Any]) -> Path:
    path = common.LATEST_DIR / "latest_fno_options_atm.md"
    counts = marker.get("interval_counts", {})
    lines = [
        "# FnO ATM Options Fetch",
        "",
        f"- Slot (IST): `{marker.get('slot_ist', '')}`",
        f"- State: **{marker.get('state', '')}**",
        f"- Complete: `{marker.get('complete', False)}`",
        f"- Underlyings: `{marker.get('underlyings', 0)}`",
        f"- Mapped contracts: `{marker.get('contracts_mapped', 0)}`",
        f"- Unresolved map rows: `{marker.get('contracts_unresolved', 0)}`",
        f"- Coverage: `{float(marker.get('coverage', 0.0)):.2%}`",
        f"- Expiry policy: `{marker.get('expiry_policy', '')}`",
        f"- Strike window: `±{marker.get('strike_window', 0)}`",
        "",
        "| Interval | Expected contracts | Verified contracts | Rows written |",
        "|---|---:|---:|---:|",
    ]
    for interval in marker.get("intervals", []):
        item = counts.get(interval, {})
        lines.append(
            f"| {INTERVAL_LABEL.get(interval, interval)} | {item.get('expected_contracts', 0)} | "
            f"{item.get('verified_contracts', 0)} | {item.get('rows_written', 0)} |"
        )
    lines.extend(["", f"Master SHA-256: `{marker.get('master_sha256', '')}`", f"Map SHA-256: `{marker.get('map_sha256', '')}`", ""])
    common.atomic_write_text(path, "\n".join(lines))
    return path


def _selected_underlyings(master: pd.DataFrame, args: argparse.Namespace) -> list[str]:
    options = master.loc[master["instrument_type"].isin(["CE", "PE"])]
    available = set(options["underlying"].astype(str).str.upper())
    if args.underlyings:
        requested = [value.strip().upper() for value in args.underlyings.split(",") if value.strip()]
        missing = sorted(set(requested) - available)
        if missing:
            raise ValueError("No option master rows for: " + ", ".join(missing))
        return sorted(dict.fromkeys(requested))
    stock = sorted(available - common.INDEX_UNDERLYINGS)
    if args.include_index_options:
        stock.extend(sorted(available & common.INDEX_UNDERLYINGS))
    return stock


def run_slot(
    slot_end: Any, master: pd.DataFrame, master_sha256: str, lanes: list[AppLane],
    args: argparse.Namespace, archive_cache: CanonicalArchiveCache | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    stamp = _slot(slot_end)
    if not args.dry_run:
        common.publish_heartbeat(SESSION, "RUNNING", phase="RESOLVE_ATM", slot_ist=stamp.isoformat())
    load_cash_marker(stamp)
    symbols = _selected_underlyings(master, args)
    plan = resolve_atm_contracts(
        master, symbols, stamp, expiry_policy=args.expiry_policy,
        strike_window=args.strike_window, spot_source=args.spot_source,
    )
    mapped = plan.loc[plan["mapping_status"].astype(str).str.startswith("MAPPED")].copy()
    map_path, map_sha256 = persist_contract_map(plan, stamp, dry_run=args.dry_run)
    projection = project_requests(len(mapped), len(args.intervals), args, max(1, len(lanes) or int(args.max_apps)))
    if not projection["accepted"]:
        raise RuntimeError(
            "Projected option fetch exceeds the five-minute budget: "
            f"{projection['projected_worst_case_sec']:.2f}s > {projection['budget_sec']:.2f}s"
        )
    print(
        f"[OPT-ATM][PLAN] slot={stamp.isoformat()} underlyings={len(symbols)} "
        f"contracts={len(mapped)} intervals={','.join(args.intervals)} "
        f"requests={projection['initial_requests']} worst_case_sec={projection['projected_worst_case_sec']:.2f}",
        flush=True,
    )
    if args.dry_run:
        marker = {
            "schema_version": SCHEMA_VERSION, "source": "dry_run", "state": "DRY_RUN",
            "complete": False, "slot_ist": stamp.isoformat(), "underlyings": len(symbols),
            "contracts_mapped": len(mapped), "contracts_unresolved": len(plan) - len(mapped),
            "intervals": list(args.intervals), "historical_calls": 0, "writes": 0,
            "expiry_policy": args.expiry_policy, "strike_window": args.strike_window,
            "master_sha256": master_sha256, "map_sha256": map_sha256,
            "map_path": str(map_path), "projection": projection,
        }
        print(json.dumps(marker, indent=2, default=str), flush=True)
        return marker

    common.publish_heartbeat(
        SESSION, "RUNNING", phase="PRELOAD_ARCHIVE_CACHE", slot_ist=stamp.isoformat()
    )
    cache = archive_cache or CanonicalArchiveCache()
    cache.preload(mapped, args.intervals)
    common.publish_heartbeat(SESSION, "RUNNING", phase="FETCH_SLOT", slot_ist=stamp.isoformat(), contracts=len(mapped))
    outcomes = fetch_contracts(
        mapped, args.intervals, stamp, lanes, args,
        master_date=pd.to_datetime(master["master_date"], errors="coerce").max().date(),
        master_sha256=master_sha256, archive_cache=cache,
    )
    expected = len(mapped) * len(args.intervals)
    verified = sum(item["state"] == "VERIFIED" for item in outcomes)
    rows_expected = len(mapped) * sum(1 if interval == "5minute" else 5 for interval in args.intervals)
    rows_written = sum(int(item["rows_written"]) for item in outcomes)
    coverage = verified / expected if expected else 0.0
    state_counts = {
        state: sum(item["state"] == state for item in outcomes)
        for state in ("VERIFIED", "INVALID", "FAILED")
    }
    interval_counts = {
        interval: {
            "expected_contracts": len(mapped),
            "verified_contracts": sum(item["interval"] == interval and item["state"] == "VERIFIED" for item in outcomes),
            "rows_written": sum(item["rows_written"] for item in outcomes if item["interval"] == interval),
        }
        for interval in args.intervals
    }
    complete = bool(expected and coverage >= float(args.min_coverage))
    marker = {
        "schema_version": SCHEMA_VERSION, "source": "final", "state": "SUCCESS" if complete else "PARTIAL",
        "complete": complete, "mode": args.mode,
        "slot_ist": stamp.isoformat(), "published_at_ist": common.now_ist().isoformat(),
        "engine_version": ENGINE_VERSION, "intervals": list(args.intervals),
        "expiry_policy": args.expiry_policy, "strike_window": args.strike_window,
        "spot_source_policy": args.spot_source, "underlyings": len(symbols),
        "contracts_mapped": len(mapped), "contracts_unresolved": len(plan) - len(mapped),
        "requests_expected": expected, "contracts_expected": expected, "contracts_written": verified,
        "contracts_verified": verified, "rows_expected": rows_expected, "rows_written": rows_written,
        "coverage": coverage, "minimum_coverage": float(args.min_coverage),
        "state_counts": state_counts, "mapping_state_counts": plan["mapping_status"].value_counts().sort_index().to_dict(),
        "interval_counts": interval_counts,
        "master_sha256": master_sha256, "map_sha256": map_sha256, "map_path": str(map_path),
        "projection": projection, "duration_sec": time.monotonic() - started,
        "failed": [item for item in outcomes if item["state"] != "VERIFIED"],
    }
    common.atomic_write_json(option_marker_path(stamp), marker)
    reread = common.read_json(option_marker_path(stamp))
    if reread.get("map_sha256") != map_sha256:
        raise IOError("Option slot marker readback failed")
    marker["latest_report"] = str(write_latest_report(marker))
    common.publish_status(
        SESSION, marker["state"], phase="SLOT_DONE", slot_ist=stamp.isoformat(),
        complete=marker["complete"], coverage=f"{marker['coverage']:.6f}",
        contracts_written=verified, contracts_expected=expected,
        rows_written=rows_written, expiry_policy=args.expiry_policy,
    )
    return marker


def session_slots(session_day: date, requested_slot: str = "") -> list[pd.Timestamp]:
    if requested_slot:
        text = str(requested_slot).strip()
        if len(text) == 5 and text[2] == ":":
            hour, minute = (int(part) for part in text.split(":"))
            stamp = pd.Timestamp(
                datetime.combine(session_day, dtime(hour, minute), tzinfo=common.IST)
            )
        else:
            stamp = _slot(text)
        if stamp.date() != session_day:
            raise ValueError("--slot must be on --session-date")
        return [stamp]
    return list(pd.date_range(
        datetime.combine(session_day, FIRST_SLOT, tzinfo=common.IST),
        datetime.combine(session_day, LAST_SLOT, tzinfo=common.IST), freq="5min",
    ))


def _session_days(args: argparse.Namespace) -> list[date]:
    if args.from_date or args.through_date:
        if not (args.from_date and args.through_date):
            raise ValueError("--from-date and --through-date must be supplied together")
        start, end = date.fromisoformat(args.from_date), date.fromisoformat(args.through_date)
    else:
        selected = date.fromisoformat(args.session_date) if args.session_date else common.now_ist().date()
        start = end = selected
    if end < start:
        raise ValueError("--through-date must not precede --from-date")
    holidays = common.load_holidays()
    return [stamp.date() for stamp in pd.date_range(start, end, freq="D") if args.allow_non_trading_day or common.is_trading_day(stamp.date(), holidays)]


def run(args: argparse.Namespace) -> int:
    lane_session = AppLaneSession()
    archive_cache = CanonicalArchiveCache()
    days = _session_days(args)
    if not days:
        print("[OPT-ATM] No trading sessions selected.", flush=True)
        return 0
    exit_code = 0
    if args.mode == "live" and len(days) != 1:
        raise ValueError("Live mode accepts exactly one session date")
    for session_day in days:
        if not args.dry_run:
            common.publish_status(
                SESSION, "RUNNING", phase="START", session_date_ist=session_day.isoformat()
            )
        master, digest, lanes, failures, reused = load_or_acquire_master(session_day, args, lane_session)
        if not args.dry_run and not lanes:
            lanes, failures, reused = lane_session.acquire(args, auth_tag="[OPT-ATM]")
        if args.mode == "live":
            processed: set[str] = set()
            while True:
                current = common.now_ist()
                if current.date() != session_day or current > datetime.combine(session_day, LAST_SLOT, tzinfo=common.IST) + timedelta(minutes=3):
                    common.publish_status(SESSION, "DONE", phase="END_TIME", session_date_ist=session_day.isoformat(), processed_slots=len(processed))
                    break
                latest_value = (
                    session_slots(session_day, args.slot)[0]
                    if args.slot
                    else legacy.latest_completed_slot(current, common.load_holidays())
                )
                # The legacy scheduler returns a native datetime while an
                # explicit slot is a pandas Timestamp.  Normalize both before
                # using Timestamp-only helpers such as ``to_pydatetime``.
                latest = None if latest_value is None else _slot(latest_value)
                if latest is None or latest.date() != session_day or latest.time() < FIRST_SLOT:
                    common.publish_heartbeat(SESSION, "WAITING", phase="WAIT_FIRST_SLOT", session_date_ist=session_day.isoformat())
                    time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
                    continue
                key = latest.strftime("%H%M")
                if key in processed or current < latest.to_pydatetime() + timedelta(seconds=float(args.boundary_buffer_sec)):
                    common.publish_heartbeat(SESSION, "WAITING", phase="WAIT_NEXT_SLOT", slot_ist=latest.isoformat())
                    time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
                    continue
                try:
                    marker = run_slot(latest, master, digest, lanes, args, archive_cache)
                    if not marker.get("complete"):
                        exit_code = 2
                except (FileNotFoundError, CashSlotNotReadyError) as exc:
                    # The cash-data producer publishes a few seconds after the
                    # candle boundary.  Do not consume the slot before that
                    # authoritative marker exists; retry it on the next poll.
                    common.publish_heartbeat(
                        SESSION, "WAITING", phase="WAIT_CASH_SLOT",
                        slot_ist=latest.isoformat(), error=str(exc),
                    )
                    time.sleep(max(0.2, min(float(args.poll_sec), 5.0)))
                    continue
                except Exception as exc:
                    print(f"[OPT-ATM][ERROR] slot={latest.isoformat()} {type(exc).__name__}:{exc}", flush=True)
                    common.publish_status(SESSION, "FAILED", phase="FAILED", slot_ist=latest.isoformat(), error=f"{type(exc).__name__}:{exc}")
                    exit_code = 1
                processed.add(key)
                lane_session.invalidate_runtime_auth_failures()
                if args.once or args.slot:
                    break
            continue

        slots = session_slots(session_day, args.slot)
        if args.once and not args.slot:
            slots = slots[-1:]
        for stamp in slots:
            if args.mode == "live" and stamp > _slot(common.now_ist()):
                continue
            try:
                marker = run_slot(stamp, master, digest, lanes, args, archive_cache)
                if not args.dry_run and not marker.get("complete"):
                    exit_code = 2
            except (FileNotFoundError, ValueError) as exc:
                print(f"[OPT-ATM][SKIP] slot={stamp.isoformat()} {type(exc).__name__}:{exc}", flush=True)
                if not args.dry_run:
                    exit_code = 2
            except Exception as exc:
                print(f"[OPT-ATM][ERROR] slot={stamp.isoformat()} {type(exc).__name__}:{exc}", flush=True)
                if not args.dry_run:
                    common.publish_status(SESSION, "FAILED", phase="ERROR", slot_ist=stamp.isoformat(), error=f"{type(exc).__name__}:{exc}")
                exit_code = 1
            lane_session.invalidate_runtime_auth_failures()
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = legacy.build_parser()
    parser.description = "Fetch ATM CE/PE option OHLCV+OI at 5m and 1m resolution."
    parser.add_argument("--mode", choices=("live", "historical"), default="live")
    parser.add_argument("--from-day", "--from-date", dest="from_date", default="")
    parser.add_argument("--through-day", "--through-date", dest="through_date", default="")
    parser.add_argument("--underlyings", default="")
    index = parser.add_mutually_exclusive_group()
    index.add_argument("--include-index", "--include-index-options", dest="include_index_options", action="store_true")
    index.add_argument("--no-include-index", dest="include_index_options", action="store_false")
    parser.set_defaults(include_index_options=False)
    parser.add_argument(
        "--expiry-policy",
        type=lambda value: value.upper(),
        choices=("NEAREST_UNEXPIRED_MONTHLY", "NEAREST_UNEXPIRED_WEEKLY"),
        default="NEAREST_UNEXPIRED_MONTHLY",
    )
    parser.add_argument("--strike-window", type=int, default=0)
    parser.add_argument(
        "--spot-source", type=lambda value: {"AUTO": "auto", "EQUITY_5M": "5m", "EQUITY_1M": "1m", "5M": "5m", "1M": "1m"}[value.upper()],
        choices=("auto", "5m", "1m"), default="auto",
    )
    parser.add_argument("--intervals", type=parse_intervals, default=("5minute", "minute"), metavar="both|5m|1m")
    parser.add_argument("--workers-per-app", type=int, default=2)
    parser.add_argument("--writer-workers", type=int, default=8)
    parser.add_argument("--allow-high-writer-count", action="store_true")
    parser.add_argument("--allow-high-workers", dest="allow_high_writer_count", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.set_defaults(request_interval_sec=0.34, max_apps=8, timeout_sec=8.0, slot_retry_attempts=2)
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.strike_window < 0:
        raise ValueError("--strike-window must be >= 0")
    if args.workers_per_app < 1:
        raise ValueError("--workers-per-app must be >= 1")
    if args.writer_workers < 1:
        raise ValueError("--writer-workers must be >= 1")
    if args.writer_workers > 8 and not args.allow_high_writer_count:
        raise ValueError("--writer-workers is capped at 8 unless --allow-high-writer-count is set")
    args.request_interval_sec = max(0.34, float(args.request_interval_sec))
    if int(args.slot_retry_attempts) < common.MIN_NO_CANDLE_FETCH_ATTEMPTS - 1:
        raise ValueError(
            f"--slot-retry-attempts must be >= {common.MIN_NO_CANDLE_FETCH_ATTEMPTS - 1}"
        )
    if not 0 < float(args.min_coverage) <= 1:
        raise ValueError("--min-coverage must be in (0, 1]")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate_args(args)
        return run(args)
    except Exception as exc:
        print(f"[OPT-ATM][FATAL] {type(exc).__name__}:{exc}", file=sys.stderr, flush=True)
        if not args.dry_run:
            common.publish_status(
                SESSION, "FAILED", phase="FAILED", error=f"{type(exc).__name__}:{exc}"
            )
        return 1


if __name__ == "__main__":
    sys.exit(main())
