"""Audit and non-destructively repair V9/V8 FnO historical source snapshots.

The live monolithic Parquets and every supplied physical snapshot are read-only
inputs.  A repair run has three explicit phases:

1. ``audit`` inventories exact IST equity 1-minute and futures 5-minute grids.
2. ``fetch`` records immutable broker-response evidence for repairable cells.
3. ``publish`` creates a new physical snapshot and publishes its manifest last.

An empty API response is not silently treated as data.  It becomes
``VERIFIED_NO_CANDLE`` only after the configured number of successful empty
observations.  Any exception remains ``API_FAILURE`` and blocks publication by
default.  Reconstructed history is labelled research data; it cannot recreate
the original live publication/as-of state.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time as daytime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


AUDIT_SCHEMA_VERSION = "fno_v9_honest_source_audit_v1"
EVIDENCE_SCHEMA_VERSION = "fno_v9_honest_repair_evidence_v1"
REPAIR_LINEAGE_SCHEMA_VERSION = "fno_v9_honest_repair_lineage_v1"
REPAIR_POLICY_VERSION = "exact_ist_grid_repeated_empty_evidence_v2"

ROLE_EQUITY = "NSE_EQUITY_1M"
ROLE_FUTURES = "NFO_FUTURES_5M"
SUPPORTED_ROLES = (ROLE_EQUITY, ROLE_FUTURES)

# Independent literal copy of the frozen 2026 regular-session dates used by
# V8.  It deliberately does not infer holidays from observed candle presence.
NSE_FO_TRADING_HOLIDAYS_2026 = (
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
NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED = ("2026-11-08",)
NSE_REGULAR_SPECIAL_SESSIONS_INCLUDED = ("2026-02-01",)

DEFAULT_ROOT = common.FNO_ROOT / "historical_repair" / "v9_honest_v1"
DEFAULT_AUDIT_ROOT = DEFAULT_ROOT / "audits"
DEFAULT_EVIDENCE_ROOT = DEFAULT_ROOT / "evidence"
DEFAULT_REPAIRED_SNAPSHOT_ROOT = DEFAULT_ROOT / "snapshots"

ISSUE_COLUMNS = (
    "role",
    "logical_symbol",
    "equity_symbol",
    "futures_symbol",
    "instrument_token",
    "session_date",
    "expected_timestamp",
    "observed_timestamp",
    "issue_type",
    "detail",
    "repairable",
)
SESSION_COLUMNS = (
    "role",
    "logical_symbol",
    "equity_symbol",
    "futures_symbol",
    "instrument_token",
    "session_date",
    "expected_rows",
    "valid_rows",
    "missing_rows",
    "invalid_rows",
    "duplicate_rows",
    "off_grid_rows",
    "suspect_synthetic_rows",
    "trailing_suspect_synthetic_rows",
    "complete",
)
TARGET_ISSUES = {
    "MISSING_TIMESTAMP",
    "DUPLICATE_TIMESTAMP",
    "INVALID_OHLC",
    "INVALID_VOLUME",
    "INVALID_OI",
    "INVALID_QUALITY_STATE",
    "LINEAGE_FLAGGED",
    "IDENTITY_MISMATCH",
    "SUSPECT_SYNTHETIC_FLAT_ZERO_VOLUME",
}


def _now() -> datetime:
    return common.now_ist()


def _parse_day(value: date | str | pd.Timestamp) -> date:
    return pd.Timestamp(value).date()


def calendar_payload() -> dict[str, Any]:
    return {
        "schema_version": "fno_v9_honest_frozen_calendar_2026_v1",
        "timezone": "Asia/Kolkata",
        "equity_first_end_label": "09:16",
        "futures_first_end_label": "09:20",
        "regular_session_close": "15:30",
        "trading_holidays": list(NSE_FO_TRADING_HOLIDAYS_2026),
        "nonstandard_sessions_excluded": list(
            NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED
        ),
        "regular_special_sessions_included": list(
            NSE_REGULAR_SPECIAL_SESSIONS_INCLUDED
        ),
        "rule": (
            "MONDAY_TO_FRIDAY_EXCLUDING_LITERAL_HOLIDAYS_PLUS_LITERAL_"
            "FULL_REGULAR_SPECIAL_SESSIONS"
        ),
    }


def expected_session_dates(
    from_day: date | str | pd.Timestamp,
    through_day: date | str | pd.Timestamp,
) -> list[date]:
    start = _parse_day(from_day)
    stop = _parse_day(through_day)
    if stop < start:
        raise ValueError("through_day cannot precede from_day")
    if start.year != 2026 or stop.year != 2026:
        raise ValueError("The frozen V9-Honest calendar covers 2026 only")
    holidays = {date.fromisoformat(value) for value in NSE_FO_TRADING_HOLIDAYS_2026}
    excluded = {
        date.fromisoformat(value) for value in NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED
    }
    special = {
        date.fromisoformat(value) for value in NSE_REGULAR_SPECIAL_SESSIONS_INCLUDED
    }
    sessions = [
        stamp.date()
        for stamp in pd.date_range(start, stop, freq="D")
        if (
            stamp.date() in special
            or (stamp.weekday() < 5 and stamp.date() not in holidays)
        )
        and stamp.date() not in excluded
    ]
    if not sessions:
        raise ValueError("Requested range has no expected regular NSE session")
    return sessions


def expected_grid(session_day: date, role: str) -> pd.DatetimeIndex:
    if role == ROLE_EQUITY:
        first, frequency = "09:16", "1min"
    elif role == ROLE_FUTURES:
        first, frequency = "09:20", "5min"
    else:
        raise ValueError(f"Unsupported source role: {role}")
    return pd.date_range(
        pd.Timestamp(f"{session_day.isoformat()} {first}", tz=common.IST),
        pd.Timestamp(f"{session_day.isoformat()} 15:30", tz=common.IST),
        freq=frequency,
    )


def _to_ist_one(value: Any) -> pd.Timestamp:
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return pd.NaT
    if pd.isna(stamp):
        return pd.NaT
    try:
        if stamp.tzinfo is None:
            return stamp.tz_localize(common.IST)
        return stamp.tz_convert(common.IST)
    except (TypeError, ValueError):
        return pd.NaT


def _to_ist_series(values: pd.Series) -> pd.Series:
    if isinstance(values.dtype, pd.DatetimeTZDtype):
        return values.dt.tz_convert(common.IST)
    if pd.api.types.is_datetime64_dtype(values.dtype):
        return values.dt.tz_localize(common.IST)
    normalized = [_to_ist_one(value) for value in values]
    return pd.Series(
        pd.DatetimeIndex(normalized),
        index=values.index,
        dtype="datetime64[ns, Asia/Kolkata]",
    )


def _timezone_kind(value: Any) -> str:
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return "INVALID"
    if pd.isna(stamp):
        return "INVALID"
    if stamp.tzinfo is None:
        return "NAIVE_ASSUMED_IST"
    try:
        return f"AWARE_{stamp.utcoffset()}"
    except Exception:
        return "AWARE_UNKNOWN"


def _truthy(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
    textual = (
        values.astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "yes", "on", "1"})
    )
    return numeric | textual


def _valid_ohlc_row(row: Mapping[str, Any]) -> tuple[bool, str]:
    try:
        open_, high, low, close = [
            float(row.get(column)) for column in ("open", "high", "low", "close")
        ]
    except (TypeError, ValueError):
        return False, "non_numeric_ohlc"
    values = (open_, high, low, close)
    if not all(math.isfinite(value) and value > 0 for value in values):
        return False, "non_finite_or_non_positive_ohlc"
    if high < max(open_, close) or low > min(open_, close) or high < low:
        return False, "invalid_ohlc_geometry"
    return True, ""


def _valid_volume(row: Mapping[str, Any]) -> bool:
    try:
        value = float(row.get("volume"))
    except (TypeError, ValueError):
        return False
    return math.isfinite(value) and value >= 0


def _suspect_synthetic_flat_zero_volume(row: Mapping[str, Any]) -> bool:
    try:
        open_, high, low, close = [
            float(row.get(column)) for column in ("open", "high", "low", "close")
        ]
        volume = float(row.get("volume"))
    except (TypeError, ValueError):
        return False
    return (
        all(math.isfinite(value) for value in (open_, high, low, close, volume))
        and volume == 0.0
        and open_ == high == low == close
    )


def _empty_issues() -> pd.DataFrame:
    return pd.DataFrame(columns=list(ISSUE_COLUMNS))


def _empty_sessions() -> pd.DataFrame:
    return pd.DataFrame(columns=list(SESSION_COLUMNS))


@dataclass(frozen=True)
class SnapshotContract:
    mapped_universe: pd.DataFrame
    universe_record: dict[str, Any]
    snapshot: dict[str, Any]
    inventory: dict[str, Any]
    source_lookup: dict[tuple[str, str], Path]


@dataclass
class AuditResult:
    source_snapshot_manifest: Path
    source_snapshot_fingerprint: str
    from_day: date
    through_day: date
    session_dates: list[date]
    issues: pd.DataFrame
    symbol_sessions: pd.DataFrame
    summary: dict[str, Any]
    contract: SnapshotContract = field(repr=False)
    audit_fingerprint: str = ""


def load_snapshot_contract(source_snapshot: Path | str) -> SnapshotContract:
    snapshot = provenance.load_source_snapshot(source_snapshot)
    declared = dict(snapshot.get("universe") or {})
    required_identity = (
        "master_date",
        "file_sha256",
        "universe_sha256",
        "mapped_universe_sha256",
        "mapped_symbol_set_sha256",
    )
    missing = [key for key in required_identity if not str(declared.get(key, ""))]
    if missing:
        raise ValueError(f"Snapshot universe identity is incomplete: {missing}")
    mapped, universe_record = provenance.load_backtest_universe(
        universe_path=snapshot["universe_path"],
        universe_date=declared["master_date"],
        contract_month_contains=str(declared.get("contract_month_filter", "")),
        require_persisted_mapping=True,
        expected_file_sha256=str(declared["file_sha256"]),
        expected_universe_sha256=str(declared["universe_sha256"]),
        expected_mapped_universe_sha256=str(declared["mapped_universe_sha256"]),
        expected_mapped_symbol_set_sha256=str(declared["mapped_symbol_set_sha256"]),
    )
    snapshot, inventory = provenance.validate_source_snapshot(
        snapshot,
        mapped,
        universe_record,
        require_complete_sources=True,
    )
    lookup: dict[tuple[str, str], Path] = {}
    for entry in inventory.get("entries", []):
        if not bool(entry.get("exists")):
            continue
        key = (
            str(entry.get("role", "")).upper().strip(),
            str(entry.get("logical_symbol", "")).upper().strip(),
        )
        if key in lookup:
            raise ValueError(f"Duplicate snapshot source identity: {key}")
        lookup[key] = Path(str(entry["resolved_path"])).resolve()
    return SnapshotContract(
        mapped_universe=mapped.reset_index(drop=True),
        universe_record=dict(universe_record),
        snapshot=dict(snapshot),
        inventory=dict(inventory),
        source_lookup=lookup,
    )


def _contract_symbols(
    row: Mapping[str, Any], contract: SnapshotContract
) -> tuple[str, str, str]:
    equity = str(row["equity_symbol"]).upper().strip()
    futures = str(row["futures_tradingsymbol"]).upper().strip()
    equity_logical = equity
    if (ROLE_EQUITY, equity_logical) not in contract.source_lookup:
        equity_logical = hybrid.resolve_backtest_equity_symbol(
            equity, root=contract.snapshot["equity_1m_root"]
        ).upper().strip()
    return equity, equity_logical, futures


def _read_source_frame(path: Path, role: str) -> tuple[pd.DataFrame, str]:
    if not path.exists():
        raise FileNotFoundError(f"Snapshot source disappeared: {path}")
    available = set(pq.read_schema(path).names)
    timestamp_column = (
        "date"
        if role == ROLE_EQUITY and "date" in available
        else "timestamp"
    )
    required = {timestamp_column, "open", "high", "low", "close", "volume"}
    if role == ROLE_FUTURES:
        required |= {
            "oi",
            "quality_state",
            "tradingsymbol",
            "instrument_token",
        }
    missing = required - available
    if missing:
        raise ValueError(f"Source is missing columns {sorted(missing)}: {path}")
    optional = {
        "gap_filled",
        "opening_snapshot",
        "provisional_stale",
        "expiry",
        "contract_month",
    }
    columns = sorted(required | (optional & available))
    frame = pd.read_parquet(path, columns=columns, engine="pyarrow")
    frame["_raw_timestamp"] = frame[timestamp_column]
    frame["_ts"] = _to_ist_series(frame[timestamp_column])
    timestamp_values = frame["_raw_timestamp"]
    if isinstance(timestamp_values.dtype, pd.DatetimeTZDtype):
        timezone_kind = f"AWARE_{timestamp_values.dt.tz}"
        frame["_timezone_kind"] = timezone_kind
    elif pd.api.types.is_datetime64_dtype(timestamp_values.dtype):
        frame["_timezone_kind"] = "NAIVE_ASSUMED_IST"
    else:
        frame["_timezone_kind"] = timestamp_values.map(_timezone_kind)
    return frame, timestamp_column


def _issue_record(
    *,
    role: str,
    logical_symbol: str,
    equity_symbol: str,
    futures_symbol: str,
    instrument_token: int,
    session_day: date | None,
    expected_timestamp: pd.Timestamp | None,
    observed_timestamp: pd.Timestamp | None,
    issue_type: str,
    detail: str,
    repairable: bool,
) -> dict[str, Any]:
    return {
        "role": role,
        "logical_symbol": logical_symbol,
        "equity_symbol": equity_symbol,
        "futures_symbol": futures_symbol,
        "instrument_token": int(instrument_token),
        "session_date": session_day.isoformat() if session_day else "",
        "expected_timestamp": (
            expected_timestamp.isoformat() if expected_timestamp is not None else ""
        ),
        "observed_timestamp": (
            observed_timestamp.isoformat() if observed_timestamp is not None else ""
        ),
        "issue_type": issue_type,
        "detail": str(detail),
        "repairable": bool(repairable),
    }


def _row_issue_codes(
    row: Mapping[str, Any],
    *,
    role: str,
    logical_symbol: str,
    instrument_token: int,
) -> list[tuple[str, str]]:
    issues: list[tuple[str, str]] = []
    valid_ohlc, ohlc_detail = _valid_ohlc_row(row)
    if not valid_ohlc:
        issues.append(("INVALID_OHLC", ohlc_detail))
    if not _valid_volume(row):
        issues.append(("INVALID_VOLUME", "volume must be finite and non-negative"))
    if role == ROLE_EQUITY:
        flagged = [
            column
            for column in ("gap_filled", "opening_snapshot", "provisional_stale")
            if column in row and bool(_truthy(pd.Series([row.get(column)])).iloc[0])
        ]
        if flagged:
            issues.append(("LINEAGE_FLAGGED", ",".join(flagged)))
        if _suspect_synthetic_flat_zero_volume(row):
            lineage = (
                "lineage_flags_present"
                if any(
                    column in row
                    for column in (
                        "gap_filled",
                        "opening_snapshot",
                        "provisional_stale",
                    )
                )
                else "lineage_unknown"
            )
            issues.append(
                (
                    "SUSPECT_SYNTHETIC_FLAT_ZERO_VOLUME",
                    f"flat_zero_volume_completed_bar;{lineage}",
                )
            )
    else:
        try:
            oi_value = float(row.get("oi"))
        except (TypeError, ValueError):
            oi_value = math.nan
        if not math.isfinite(oi_value) or oi_value <= 0:
            issues.append(("INVALID_OI", "oi must be finite and positive"))
        if str(row.get("quality_state", "")).upper().strip() != "VALID":
            issues.append(
                (
                    "INVALID_QUALITY_STATE",
                    f"quality_state={row.get('quality_state', '')}",
                )
            )
        observed_symbol = str(row.get("tradingsymbol", "")).upper().strip()
        try:
            observed_token = int(row.get("instrument_token"))
        except (TypeError, ValueError):
            observed_token = 0
        if observed_symbol != logical_symbol or observed_token != int(instrument_token):
            issues.append(
                (
                    "IDENTITY_MISMATCH",
                    f"symbol={observed_symbol},token={observed_token}",
                )
            )
    return issues


def _vectorized_problem_map(
    frame: pd.DataFrame,
    *,
    role: str,
    logical_symbol: str,
    instrument_token: int,
) -> tuple[dict[Any, list[tuple[str, str]]], set[Any]]:
    """Precompute problems only for invalid rows; valid rows allocate nothing."""

    problems: dict[Any, list[tuple[str, str]]] = {}

    def add(mask: pd.Series, issue_type: str, detail: str) -> None:
        for index in frame.index[mask.fillna(False)]:
            problems.setdefault(index, []).append((issue_type, detail))

    numeric = frame[["open", "high", "low", "close", "volume"]].apply(
        pd.to_numeric, errors="coerce"
    )
    prices = numeric[["open", "high", "low", "close"]]
    finite_prices = pd.Series(
        np.isfinite(prices.to_numpy(dtype=float)).all(axis=1), index=frame.index
    )
    positive_prices = prices.gt(0).all(axis=1)
    geometry = (
        prices["high"].ge(prices[["open", "close"]].max(axis=1))
        & prices["low"].le(prices[["open", "close"]].min(axis=1))
        & prices["high"].ge(prices["low"])
    )
    add(
        ~(finite_prices & positive_prices & geometry),
        "INVALID_OHLC",
        "non_finite_non_positive_or_invalid_geometry",
    )
    valid_volume = pd.Series(
        np.isfinite(numeric["volume"].to_numpy(dtype=float)), index=frame.index
    ) & numeric["volume"].ge(0)
    add(~valid_volume, "INVALID_VOLUME", "volume must be finite and non-negative")
    suspect_indices: set[Any] = set()
    if role == ROLE_EQUITY:
        lineage_mask = pd.Series(False, index=frame.index)
        flagged_columns: list[str] = []
        for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
            if column in frame.columns:
                column_mask = _truthy(frame[column])
                lineage_mask |= column_mask
                if bool(column_mask.any()):
                    flagged_columns.append(column)
        add(
            lineage_mask,
            "LINEAGE_FLAGGED",
            ",".join(flagged_columns) or "lineage_flagged",
        )
        suspect = (
            numeric["volume"].eq(0)
            & prices["open"].eq(prices["high"])
            & prices["open"].eq(prices["low"])
            & prices["open"].eq(prices["close"])
            & finite_prices
        )
        suspect_indices = set(frame.index[suspect])
        lineage = (
            "lineage_flags_present"
            if any(
                column in frame.columns
                for column in (
                    "gap_filled",
                    "opening_snapshot",
                    "provisional_stale",
                )
            )
            else "lineage_unknown"
        )
        add(
            suspect,
            "SUSPECT_SYNTHETIC_FLAT_ZERO_VOLUME",
            f"flat_zero_volume_completed_bar;{lineage}",
        )
    else:
        oi = pd.to_numeric(frame["oi"], errors="coerce")
        valid_oi = pd.Series(
            np.isfinite(oi.to_numpy(dtype=float)), index=frame.index
        ) & oi.gt(0)
        add(~valid_oi, "INVALID_OI", "oi must be finite and positive")
        quality = frame["quality_state"].astype(str).str.upper().str.strip()
        add(
            ~quality.eq("VALID"),
            "INVALID_QUALITY_STATE",
            "quality_state is not VALID",
        )
        observed_symbol = frame["tradingsymbol"].astype(str).str.upper().str.strip()
        observed_token = pd.to_numeric(frame["instrument_token"], errors="coerce")
        identity = observed_symbol.eq(logical_symbol) & observed_token.eq(
            int(instrument_token)
        )
        add(
            ~identity,
            "IDENTITY_MISMATCH",
            "tradingsymbol or instrument_token differs from dated universe",
        )
    return problems, suspect_indices


def _audit_one_source(
    *,
    frame: pd.DataFrame,
    role: str,
    logical_symbol: str,
    equity_symbol: str,
    futures_symbol: str,
    instrument_token: int,
    sessions: Sequence[date],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    issue_records: list[dict[str, Any]] = []
    session_records: list[dict[str, Any]] = []
    timezone_kinds = sorted(set(frame["_timezone_kind"].astype(str)))
    if len(timezone_kinds) > 1:
        issue_records.append(
            _issue_record(
                role=role,
                logical_symbol=logical_symbol,
                equity_symbol=equity_symbol,
                futures_symbol=futures_symbol,
                instrument_token=instrument_token,
                session_day=None,
                expected_timestamp=None,
                observed_timestamp=None,
                issue_type="MIXED_TIMEZONE_SOURCE",
                detail=json.dumps(timezone_kinds, separators=(",", ":")),
                repairable=False,
            )
        )
    invalid_timestamp_count = int(frame["_ts"].isna().sum())
    if invalid_timestamp_count:
        issue_records.append(
            _issue_record(
                role=role,
                logical_symbol=logical_symbol,
                equity_symbol=equity_symbol,
                futures_symbol=futures_symbol,
                instrument_token=instrument_token,
                session_day=None,
                expected_timestamp=None,
                observed_timestamp=None,
                issue_type="INVALID_TIMESTAMP",
                detail=f"rows={invalid_timestamp_count}",
                repairable=False,
            )
        )

    session_set = set(sessions)
    valid_timestamp_mask = frame["_ts"].notna()
    window_rows = frame.loc[
        valid_timestamp_mask
        & frame["_ts"].dt.date.isin(session_set)
    ].copy()
    problem_map, suspect_row_indices = _vectorized_problem_map(
        window_rows,
        role=role,
        logical_symbol=logical_symbol,
        instrument_token=instrument_token,
    )
    day_groups = {
        session_day: group
        for session_day, group in window_rows.groupby(
            window_rows["_ts"].dt.date, sort=False
        )
    }

    for session_day in sessions:
        grid = expected_grid(session_day, role)
        grid_set = set(grid)
        day_rows = day_groups.get(session_day)
        if day_rows is None:
            day_rows = frame.iloc[0:0].copy()
        off_grid = day_rows.loc[~day_rows["_ts"].isin(grid_set)]
        for observed in off_grid["_ts"].tolist():
            issue_records.append(
                _issue_record(
                    role=role,
                    logical_symbol=logical_symbol,
                    equity_symbol=equity_symbol,
                    futures_symbol=futures_symbol,
                    instrument_token=instrument_token,
                    session_day=session_day,
                    expected_timestamp=None,
                    observed_timestamp=observed,
                    issue_type="OFF_GRID_TIMESTAMP",
                    detail="row is outside the exact completed-bar grid",
                    repairable=False,
                )
            )

        on_grid = day_rows.loc[day_rows["_ts"].isin(grid_set)].copy()
        counts = on_grid["_ts"].value_counts(sort=False)
        observed_grid = pd.DatetimeIndex(counts.index) if len(counts) else pd.DatetimeIndex([])
        missing_timestamps = list(grid.difference(observed_grid))
        duplicate_counts = counts.loc[counts.gt(1)]
        duplicate_timestamps = set(duplicate_counts.index)
        unique_rows = on_grid.loc[~on_grid["_ts"].isin(duplicate_timestamps)]
        invalid_unique = unique_rows.loc[unique_rows.index.isin(problem_map)]
        suspect_unique = unique_rows.loc[unique_rows.index.isin(suspect_row_indices)]
        suspect_timestamps = set(suspect_unique["_ts"].tolist())
        missing_rows = int(len(missing_timestamps))
        duplicate_rows = int(duplicate_counts.sum()) if len(duplicate_counts) else 0
        invalid_rows = int(invalid_unique["_ts"].nunique())
        valid_rows = int(len(unique_rows) - invalid_rows)

        for expected in missing_timestamps:
            issue_records.append(
                _issue_record(
                    role=role,
                    logical_symbol=logical_symbol,
                    equity_symbol=equity_symbol,
                    futures_symbol=futures_symbol,
                    instrument_token=instrument_token,
                    session_day=session_day,
                    expected_timestamp=expected,
                    observed_timestamp=None,
                    issue_type="MISSING_TIMESTAMP",
                    detail="no exact completed candle",
                    repairable=True,
                )
            )
        for expected, count in duplicate_counts.items():
            issue_records.append(
                _issue_record(
                    role=role,
                    logical_symbol=logical_symbol,
                    equity_symbol=equity_symbol,
                    futures_symbol=futures_symbol,
                    instrument_token=instrument_token,
                    session_day=session_day,
                    expected_timestamp=expected,
                    observed_timestamp=expected,
                    issue_type="DUPLICATE_TIMESTAMP",
                    detail=f"rows={int(count)}",
                    repairable=True,
                )
            )
        for row_index, row in invalid_unique.iterrows():
            expected = row["_ts"]
            for issue_type, detail in problem_map.get(row_index, []):
                issue_records.append(
                    _issue_record(
                        role=role,
                        logical_symbol=logical_symbol,
                        equity_symbol=equity_symbol,
                        futures_symbol=futures_symbol,
                        instrument_token=instrument_token,
                        session_day=session_day,
                        expected_timestamp=expected,
                        observed_timestamp=expected,
                        issue_type=issue_type,
                        detail=detail,
                        repairable=True,
                    )
                )
        trailing_suspect = 0
        for expected in reversed(grid):
            if expected not in suspect_timestamps:
                break
            trailing_suspect += 1
        session_records.append(
            {
                "role": role,
                "logical_symbol": logical_symbol,
                "equity_symbol": equity_symbol,
                "futures_symbol": futures_symbol,
                "instrument_token": int(instrument_token),
                "session_date": session_day.isoformat(),
                "expected_rows": int(len(grid)),
                "valid_rows": int(valid_rows),
                "missing_rows": int(missing_rows),
                "invalid_rows": int(invalid_rows),
                "duplicate_rows": int(duplicate_rows),
                "off_grid_rows": int(len(off_grid)),
                "suspect_synthetic_rows": int(len(suspect_timestamps)),
                "trailing_suspect_synthetic_rows": int(trailing_suspect),
                "complete": bool(
                    valid_rows == len(grid)
                    and not missing_rows
                    and not invalid_rows
                    and not duplicate_rows
                    and off_grid.empty
                    and not suspect_timestamps
                ),
            }
        )
    return issue_records, session_records


def _audit_contract(
    contract: SnapshotContract,
    *,
    from_day: date,
    through_day: date,
    source_snapshot_manifest: Path,
) -> AuditResult:
    sessions = expected_session_dates(from_day, through_day)
    expected_role_files = int(len(contract.mapped_universe) * 2)
    observed_role_files = int(len(contract.source_lookup))
    if observed_role_files != expected_role_files:
        raise ValueError(
            "Snapshot role-file inventory must contain exactly one equity and one "
            f"futures source per mapped symbol: expected={expected_role_files}, "
            f"observed={observed_role_files}"
        )
    issue_records: list[dict[str, Any]] = []
    session_records: list[dict[str, Any]] = []
    for raw_contract in contract.mapped_universe.to_dict("records"):
        equity, equity_logical, futures = _contract_symbols(raw_contract, contract)
        role_specs = (
            (
                ROLE_EQUITY,
                equity_logical,
                int(raw_contract["equity_instrument_token"]),
            ),
            (
                ROLE_FUTURES,
                futures,
                int(raw_contract["futures_instrument_token"]),
            ),
        )
        for role, logical_symbol, token in role_specs:
            source_path = contract.source_lookup.get((role, logical_symbol))
            if source_path is None:
                raise FileNotFoundError(
                    f"Snapshot lookup is missing {role}:{logical_symbol}"
                )
            frame, _ = _read_source_frame(source_path, role)
            issues, rows = _audit_one_source(
                frame=frame,
                role=role,
                logical_symbol=logical_symbol,
                equity_symbol=equity,
                futures_symbol=futures,
                instrument_token=token,
                sessions=sessions,
            )
            issue_records.extend(issues)
            session_records.extend(rows)
    issues = (
        pd.DataFrame(issue_records, columns=list(ISSUE_COLUMNS))
        if issue_records
        else _empty_issues()
    )
    symbol_sessions = (
        pd.DataFrame(session_records, columns=list(SESSION_COLUMNS))
        if session_records
        else _empty_sessions()
    )
    if not issues.empty:
        issues = issues.sort_values(
            ["role", "logical_symbol", "session_date", "expected_timestamp", "issue_type"],
            kind="stable",
        ).reset_index(drop=True)
    if not symbol_sessions.empty:
        symbol_sessions = symbol_sessions.sort_values(
            ["session_date", "equity_symbol", "role"], kind="stable"
        ).reset_index(drop=True)

    combined = (
        symbol_sessions.groupby(["equity_symbol", "session_date"], sort=False)[
            "complete"
        ]
        .all()
        .reset_index()
        if not symbol_sessions.empty
        else pd.DataFrame(columns=["equity_symbol", "session_date", "complete"])
    )
    repair_targets = (
        issues.loc[
            issues["repairable"].eq(True)
            & issues["expected_timestamp"].astype(str).ne("")
            & issues["issue_type"].isin(TARGET_ISSUES),
            ["role", "logical_symbol", "expected_timestamp"],
        ].drop_duplicates()
        if not issues.empty
        else pd.DataFrame(columns=["role", "logical_symbol", "expected_timestamp"])
    )
    missing_cells = issues.loc[
        issues["issue_type"].eq("MISSING_TIMESTAMP")
        & issues["expected_timestamp"].astype(str).ne("")
    ].copy()
    systematic_missing: list[dict[str, Any]] = []
    if not missing_cells.empty:
        denominator = int(len(contract.mapped_universe))
        grouped_missing = (
            missing_cells.groupby(
                ["role", "session_date", "expected_timestamp"], sort=True
            )["logical_symbol"]
            .nunique()
            .reset_index(name="affected_symbols")
        )
        grouped_missing["affected_ratio"] = (
            grouped_missing["affected_symbols"] / max(1, denominator)
        )
        systematic_missing = [
            {
                "role": str(row.role),
                "session_date": str(row.session_date),
                "expected_timestamp": str(row.expected_timestamp),
                "affected_symbols": int(row.affected_symbols),
                "mapped_symbols": denominator,
                "affected_ratio": round(float(row.affected_ratio), 6),
                "classification": "PROVIDER_WIDE_MISSING_SENSITIVITY",
            }
            for row in grouped_missing.loc[
                grouped_missing["affected_ratio"].ge(0.90)
            ].itertuples(index=False)
        ]
    summary = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "from_day": from_day.isoformat(),
        "through_day": through_day.isoformat(),
        "calendar_sha256": common.canonical_json_sha256(calendar_payload()),
        "calendar_session_count": int(len(sessions)),
        "mapped_symbol_count": int(len(contract.mapped_universe)),
        "expected_source_role_file_count": expected_role_files,
        "observed_source_role_file_count": observed_role_files,
        "expected_symbol_sessions": int(len(combined)),
        "complete_symbol_sessions": int(combined["complete"].sum()),
        "source_incomplete_symbol_sessions": int((~combined["complete"]).sum()),
        "role_symbol_sessions": int(len(symbol_sessions)),
        "complete_role_symbol_sessions": int(symbol_sessions["complete"].sum()),
        "expected_bar_count": int(symbol_sessions["expected_rows"].sum()),
        "valid_bar_count": int(symbol_sessions["valid_rows"].sum()),
        "missing_bar_count": int(symbol_sessions["missing_rows"].sum()),
        "invalid_bar_count": int(symbol_sessions["invalid_rows"].sum()),
        "duplicate_row_count": int(symbol_sessions["duplicate_rows"].sum()),
        "off_grid_row_count": int(symbol_sessions["off_grid_rows"].sum()),
        "suspect_synthetic_row_count": int(
            symbol_sessions["suspect_synthetic_rows"].sum()
        ),
        "trailing_suspect_synthetic_row_count": int(
            symbol_sessions["trailing_suspect_synthetic_rows"].sum()
        ),
        "mixed_timezone_file_count": int(
            issues.loc[issues["issue_type"].eq("MIXED_TIMEZONE_SOURCE"), [
                "role",
                "logical_symbol",
            ]].drop_duplicates().shape[0]
        ),
        "invalid_timestamp_file_count": int(
            issues.loc[issues["issue_type"].eq("INVALID_TIMESTAMP"), [
                "role",
                "logical_symbol",
            ]].drop_duplicates().shape[0]
        ),
        "repair_target_count": int(len(repair_targets)),
        "systematic_missing_grid_cell_count": int(len(systematic_missing)),
        "systematic_missing_grid_cells": systematic_missing,
        "headline_source_complete": bool(
            len(combined) > 0
            and bool(combined["complete"].all())
            and not issues["issue_type"].isin(
                {"MIXED_TIMEZONE_SOURCE", "INVALID_TIMESTAMP"}
            ).any()
        ),
    }
    fingerprint_payload = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "source_snapshot_fingerprint": contract.snapshot.get(
            "snapshot_fingerprint", ""
        ),
        "source_inventory_sha256": contract.inventory.get("inventory_sha256", ""),
        "calendar": calendar_payload(),
        "session_dates": [value.isoformat() for value in sessions],
        "summary": summary,
        "issues_sha256": common.canonical_json_sha256(
            issues.fillna("").to_dict("records")
        ),
        "symbol_sessions_sha256": common.canonical_json_sha256(
            symbol_sessions.fillna("").to_dict("records")
        ),
    }
    audit_fingerprint = common.canonical_json_sha256(fingerprint_payload)
    return AuditResult(
        source_snapshot_manifest=source_snapshot_manifest.resolve(),
        source_snapshot_fingerprint=str(
            contract.snapshot.get("snapshot_fingerprint", "")
        ),
        from_day=from_day,
        through_day=through_day,
        session_dates=list(sessions),
        issues=issues,
        symbol_sessions=symbol_sessions,
        summary=summary,
        contract=contract,
        audit_fingerprint=audit_fingerprint,
    )


def audit_snapshot(
    source_snapshot: Path | str,
    *,
    from_day: date | str | pd.Timestamp,
    through_day: date | str | pd.Timestamp,
) -> AuditResult:
    manifest = Path(source_snapshot).resolve()
    if manifest.is_dir():
        manifest = manifest / "manifest.json"
    contract = load_snapshot_contract(manifest)
    return _audit_contract(
        contract,
        from_day=_parse_day(from_day),
        through_day=_parse_day(through_day),
        source_snapshot_manifest=manifest,
    )


def _immutable_parquet(frame: pd.DataFrame, target: Path) -> Path:
    target = target.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=str(target.parent),
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
        frame.to_parquet(temp_path, index=False, engine="pyarrow")
        with temp_path.open("r+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        provenance.publish_immutable_copy(temp_path, target)
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
    return target


def publish_audit(
    audit: AuditResult,
    *,
    audit_root: Path | str = DEFAULT_AUDIT_ROOT,
) -> Path:
    root = Path(audit_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_id = (
        f"audit_{_now().strftime('%Y%m%dT%H%M%S%f%z')}_"
        f"{audit.audit_fingerprint[:12]}_{uuid.uuid4().hex[:8]}"
    )
    run_dir = root / run_id
    run_dir.mkdir()
    issues_path = _immutable_parquet(audit.issues, run_dir / "issues.parquet")
    sessions_path = _immutable_parquet(
        audit.symbol_sessions, run_dir / "symbol_sessions.parquet"
    )
    summary_path = provenance.write_immutable_json(
        run_dir / "summary.json", audit.summary
    )
    artifacts = {
        "issues": provenance.artifact_record(issues_path),
        "symbol_sessions": provenance.artifact_record(sessions_path),
        "summary": provenance.artifact_record(summary_path),
    }
    payload = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": _now().isoformat(timespec="microseconds"),
        "audit_fingerprint": audit.audit_fingerprint,
        "source_snapshot_manifest": str(audit.source_snapshot_manifest),
        "source_snapshot_manifest_sha256": provenance.sha256_file(
            audit.source_snapshot_manifest
        ),
        "source_snapshot_fingerprint": audit.source_snapshot_fingerprint,
        "from_day": audit.from_day.isoformat(),
        "through_day": audit.through_day.isoformat(),
        "session_dates": [value.isoformat() for value in audit.session_dates],
        "calendar": calendar_payload(),
        "summary": audit.summary,
        "artifacts": artifacts,
    }
    manifest_path = provenance.write_immutable_json(run_dir / "manifest.json", payload)
    return manifest_path


@dataclass(frozen=True)
class ProviderResponse:
    records: tuple[Mapping[str, Any], ...]
    provider_id: str
    request_metadata: Mapping[str, Any] = field(default_factory=dict)


class HistoricalProvider(Protocol):
    """Broker/provider seam used by the repair evidence collector."""

    def fetch(
        self,
        *,
        role: str,
        instrument_token: int,
        from_day: date,
        through_day: date,
        attempt: int,
    ) -> ProviderResponse:
        ...


@dataclass
class _KiteRuntime:
    provider_id: str
    client: Any
    pace_seconds: float
    last_call_monotonic: float = 0.0

    def pace(self) -> None:
        wait = self.pace_seconds - (time.monotonic() - self.last_call_monotonic)
        if wait > 0:
            time.sleep(wait)
        self.last_call_monotonic = time.monotonic()


class KiteHistoricalProvider:
    """Read historical candles through existing Kite credentials.

    No provider response writes to the live stores.  The caller persists only
    evidence artifacts under a new evidence run directory.
    """

    def __init__(self, runtimes: Sequence[_KiteRuntime]) -> None:
        if not runtimes:
            raise ValueError("KiteHistoricalProvider needs at least one runtime")
        self._runtimes = list(runtimes)
        self._call_index = 0

    @classmethod
    def from_existing_credentials(
        cls,
        *,
        max_apps: int = 8,
        timeout_sec: float = 12.0,
        request_interval_sec: float = 0.34,
    ) -> "KiteHistoricalProvider":
        credentials = common.discover_kite_credentials(max_apps=max_apps)
        runtimes: list[_KiteRuntime] = []
        failures: list[str] = []
        for credential in credentials:
            try:
                client = common.make_kite_client(
                    credential, timeout_sec=float(timeout_sec)
                )
                runtimes.append(
                    _KiteRuntime(
                        provider_id=str(credential.app_name),
                        client=client,
                        pace_seconds=max(0.34, float(request_interval_sec)),
                    )
                )
            except Exception as exc:
                failures.append(f"{credential.app_name}:{type(exc).__name__}")
        if not runtimes:
            raise RuntimeError(f"No usable Kite runtime; failures={failures}")
        return cls(runtimes)

    @staticmethod
    def _windows(
        start: date, stop: date, *, max_calendar_days: int
    ) -> list[tuple[date, date]]:
        if max_calendar_days <= 0:
            raise ValueError("max_calendar_days must be positive")
        windows: list[tuple[date, date]] = []
        cursor = start
        while cursor <= stop:
            end = min(stop, cursor + timedelta(days=max_calendar_days - 1))
            windows.append((cursor, end))
            cursor = end + timedelta(days=1)
        return windows

    def fetch(
        self,
        *,
        role: str,
        instrument_token: int,
        from_day: date,
        through_day: date,
        attempt: int,
    ) -> ProviderResponse:
        if role not in SUPPORTED_ROLES:
            raise ValueError(f"Unsupported historical role: {role}")
        runtime = self._runtimes[self._call_index % len(self._runtimes)]
        self._call_index += 1
        interval = "minute" if role == ROLE_EQUITY else "5minute"
        oi = role == ROLE_FUTURES
        # Kite rejects cash-minute requests spanning more than 60 calendar
        # days. Futures 5-minute permits 100; keep that endpoint-specific
        # boundary literal and record every subrequest in evidence metadata.
        max_calendar_days = 60 if role == ROLE_EQUITY else 100
        records: list[Mapping[str, Any]] = []
        request_windows: list[dict[str, str]] = []
        for start_day, stop_day in self._windows(
            from_day, through_day, max_calendar_days=max_calendar_days
        ):
            start = datetime.combine(start_day, daytime(9, 15), tzinfo=common.IST)
            stop = datetime.combine(stop_day, daytime(15, 31), tzinfo=common.IST)
            runtime.pace()
            response = runtime.client.historical_data(
                int(instrument_token),
                start,
                stop,
                interval,
                continuous=False,
                oi=oi,
            )
            records.extend(list(response or []))
            request_windows.append(
                {"from": start.isoformat(), "through": stop.isoformat()}
            )
        return ProviderResponse(
            records=tuple(records),
            provider_id=runtime.provider_id,
            request_metadata={
                "attempt": int(attempt),
                "interval": interval,
                "oi": oi,
                "max_calendar_days": max_calendar_days,
                "windows": request_windows,
            },
        )


def _api_record_timestamp(record: Mapping[str, Any], role: str) -> pd.Timestamp:
    raw = record.get("date", record.get("timestamp"))
    start = _to_ist_one(raw)
    if pd.isna(start):
        return pd.NaT
    return start + pd.Timedelta(minutes=1 if role == ROLE_EQUITY else 5)


def _normalize_api_record(
    record: Mapping[str, Any],
    *,
    role: str,
    expected_timestamp: pd.Timestamp,
) -> tuple[dict[str, Any] | None, str]:
    observed = _api_record_timestamp(record, role)
    if pd.isna(observed):
        return None, "invalid_api_timestamp"
    if observed != expected_timestamp:
        return None, "wrong_api_timestamp"
    valid_ohlc, detail = _valid_ohlc_row(record)
    if not valid_ohlc:
        return None, detail
    if not _valid_volume(record):
        return None, "invalid_api_volume"
    # A broker-returned flat zero-volume row is still not evidence of an
    # exchange trade.  The source audit deliberately treats this shape as
    # suspected synthetic/no-trade padding, so accepting it here would make a
    # repair look resolved only to fail the identical post-publication audit.
    if _suspect_synthetic_flat_zero_volume(record):
        return None, "suspect_api_flat_zero_volume"
    try:
        values = {
            column: float(record.get(column))
            for column in ("open", "high", "low", "close", "volume")
        }
    except (TypeError, ValueError):
        return None, "non_numeric_api_ohlcv"
    oi_value: float | None = None
    if role == ROLE_FUTURES:
        try:
            oi_value = float(record.get("oi"))
        except (TypeError, ValueError):
            return None, "invalid_api_oi"
        if not math.isfinite(oi_value) or oi_value <= 0:
            return None, "invalid_api_oi"
    normalized = {
        "expected_timestamp": expected_timestamp.isoformat(),
        **values,
        "oi": oi_value,
    }
    normalized["semantic_sha256"] = common.canonical_json_sha256(normalized)
    return normalized, ""


def _repair_targets(audit: AuditResult) -> pd.DataFrame:
    if audit.issues.empty:
        return pd.DataFrame(
            columns=[
                "role",
                "logical_symbol",
                "equity_symbol",
                "futures_symbol",
                "instrument_token",
                "session_date",
                "expected_timestamp",
            ]
        )
    targets = audit.issues.loc[
        audit.issues["repairable"].eq(True)
        & audit.issues["issue_type"].isin(TARGET_ISSUES)
        & audit.issues["expected_timestamp"].astype(str).ne(""),
        [
            "role",
            "logical_symbol",
            "equity_symbol",
            "futures_symbol",
            "instrument_token",
            "session_date",
            "expected_timestamp",
        ],
    ].drop_duplicates()
    return targets.sort_values(
        ["role", "logical_symbol", "expected_timestamp"], kind="stable"
    ).reset_index(drop=True)


def _source_groups(audit: AuditResult) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for contract_row in audit.contract.mapped_universe.to_dict("records"):
        equity, equity_logical, futures = _contract_symbols(
            contract_row, audit.contract
        )
        rows.extend(
            [
                {
                    "role": ROLE_EQUITY,
                    "logical_symbol": equity_logical,
                    "equity_symbol": equity,
                    "futures_symbol": futures,
                    "instrument_token": int(
                        contract_row["equity_instrument_token"]
                    ),
                },
                {
                    "role": ROLE_FUTURES,
                    "logical_symbol": futures,
                    "equity_symbol": equity,
                    "futures_symbol": futures,
                    "instrument_token": int(
                        contract_row["futures_instrument_token"]
                    ),
                },
            ]
        )
    groups = pd.DataFrame(rows).sort_values(
        ["role", "logical_symbol"], kind="stable"
    ).reset_index(drop=True)
    if len(groups) != len(audit.contract.mapped_universe) * 2 or groups.duplicated(
        ["role", "logical_symbol"]
    ).any():
        raise ValueError("Audit source group inventory is not one-to-one")
    return groups


def build_fetch_plan(
    audit: AuditResult, *, verification_attempts: int = 3
) -> dict[str, Any]:
    attempts = int(verification_attempts)
    if attempts < 2:
        raise ValueError("At least two observations are required for no-candle proof")
    targets = _repair_targets(audit)
    source_groups = _source_groups(audit)
    target_counts = (
        targets.groupby(["role", "logical_symbol"]).size().to_dict()
        if not targets.empty
        else {}
    )
    group_rows: list[dict[str, Any]] = []
    for source_group in source_groups.to_dict("records"):
        role = str(source_group["role"])
        logical_symbol = str(source_group["logical_symbol"])
        max_days = 60 if role == ROLE_EQUITY else 100
        windows = KiteHistoricalProvider._windows(
            audit.from_day,
            audit.through_day,
            max_calendar_days=max_days,
        )
        group_rows.append(
            {
                "role": str(role),
                "logical_symbol": str(logical_symbol),
                "instrument_token": int(source_group["instrument_token"]),
                "repair_target_count": int(
                    target_counts.get((role, logical_symbol), 0)
                ),
                "subrequests_per_observation": int(len(windows)),
                "request_windows": [
                    {"from_day": start.isoformat(), "through_day": stop.isoformat()}
                    for start, stop in windows
                ],
            }
        )
    target_group_rows = [row for row in group_rows if row["repair_target_count"] > 0]
    equity_groups = sum(
        row["role"] == ROLE_EQUITY for row in target_group_rows
    )
    futures_groups = sum(
        row["role"] == ROLE_FUTURES for row in target_group_rows
    )
    first_pass_requests = sum(
        int(row["subrequests_per_observation"]) for row in group_rows
    )
    additional_target_requests = sum(
        int(row["subrequests_per_observation"]) * (attempts - 1)
        for row in target_group_rows
    )
    return {
        "schema_version": "fno_v9_honest_fetch_plan_v1",
        "source_snapshot_fingerprint": audit.source_snapshot_fingerprint,
        "audit_fingerprint": audit.audit_fingerprint,
        "from_day": audit.from_day.isoformat(),
        "through_day": audit.through_day.isoformat(),
        "mapped_symbol_count": int(len(audit.contract.mapped_universe)),
        "source_role_file_count": int(len(audit.contract.source_lookup)),
        "repair_target_count": int(len(targets)),
        "role_files_planned": int(len(group_rows)),
        "role_files_with_targets": int(len(target_group_rows)),
        "equity_role_files_with_targets": int(equity_groups),
        "futures_role_files_with_targets": int(futures_groups),
        "verification_attempts": attempts,
        "first_pass_api_request_count": int(first_pass_requests),
        "maximum_api_request_count": int(
            first_pass_requests + additional_target_requests
        ),
        "full_three_pass_api_request_count": int(first_pass_requests * attempts),
        "request_count_note": (
            "Cash minute history is chunked to <=60 inclusive calendar days; "
            "futures 5-minute history is chunked to <=100. Additional passes "
            "run only for targets not filled by an earlier successful response."
        ),
        "groups": group_rows,
    }


def _load_audit_manifest(path: Path | str) -> dict[str, Any]:
    manifest_path = Path(path).resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != AUDIT_SCHEMA_VERSION or not bool(
        payload.get("complete")
    ):
        raise ValueError("Not a completed V9-Honest source audit")
    for record in dict(payload.get("artifacts") or {}).values():
        if not provenance.artifact_matches(record.get("path", ""), record):
            raise AssertionError("V9-Honest audit artifact hash mismatch")
    return {**payload, "manifest_path": str(manifest_path)}


def audit_from_manifest(path: Path | str) -> AuditResult:
    payload = _load_audit_manifest(path)
    artifacts = dict(payload["artifacts"])
    source_manifest = Path(str(payload["source_snapshot_manifest"])).resolve()
    if provenance.sha256_file(source_manifest) != str(
        payload.get("source_snapshot_manifest_sha256", "")
    ):
        raise AssertionError("Audit source snapshot manifest bytes changed")
    contract = load_snapshot_contract(source_manifest)
    if str(contract.snapshot.get("snapshot_fingerprint", "")) != str(
        payload.get("source_snapshot_fingerprint", "")
    ):
        raise AssertionError("Audit source snapshot fingerprint changed")
    issues = pd.read_parquet(artifacts["issues"]["path"])
    symbol_sessions = pd.read_parquet(artifacts["symbol_sessions"]["path"])
    return AuditResult(
        source_snapshot_manifest=source_manifest,
        source_snapshot_fingerprint=str(payload["source_snapshot_fingerprint"]),
        from_day=date.fromisoformat(str(payload["from_day"])),
        through_day=date.fromisoformat(str(payload["through_day"])),
        session_dates=[date.fromisoformat(value) for value in payload["session_dates"]],
        issues=issues,
        symbol_sessions=symbol_sessions,
        summary=dict(payload["summary"]),
        contract=contract,
        audit_fingerprint=str(payload["audit_fingerprint"]),
    )


def collect_repair_evidence(
    audit: AuditResult,
    provider: HistoricalProvider,
    *,
    evidence_root: Path | str = DEFAULT_EVIDENCE_ROOT,
    verification_attempts: int = 3,
) -> Path:
    attempts_required = int(verification_attempts)
    if attempts_required < 2:
        raise ValueError("At least two observations are required for no-candle proof")
    targets = _repair_targets(audit)
    root = Path(evidence_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_id = (
        f"evidence_{_now().strftime('%Y%m%dT%H%M%S%f%z')}_"
        f"{audit.audit_fingerprint[:12]}_{uuid.uuid4().hex[:8]}"
    )
    run_dir = root / run_id
    run_dir.mkdir()

    attempt_records: list[dict[str, Any]] = []
    status_records: list[dict[str, Any]] = []
    candle_records: list[dict[str, Any]] = []
    source_groups = _source_groups(audit)
    for source_group in source_groups.to_dict("records"):
        role = str(source_group["role"])
        logical_symbol = str(source_group["logical_symbol"])
        group = targets.loc[
            targets["role"].astype(str).eq(role)
            & targets["logical_symbol"].astype(str).eq(logical_symbol)
        ].sort_values("expected_timestamp", kind="stable")
        token = int(source_group["instrument_token"])
        wanted = {
            pd.Timestamp(value): row
            for value, row in zip(
                group["expected_timestamp"], group.to_dict("records")
            )
        }
        valid_by_target: dict[pd.Timestamp, list[dict[str, Any]]] = {
            value: [] for value in wanted
        }
        invalid_by_target: dict[pd.Timestamp, list[str]] = {
            value: [] for value in wanted
        }
        successful_calls = 0
        failed_calls = 0
        for attempt in range(1, attempts_required + 1):
            observed_at = _now()
            try:
                response = provider.fetch(
                    role=str(role),
                    instrument_token=token,
                    from_day=audit.from_day,
                    through_day=audit.through_day,
                    attempt=attempt,
                )
                successful_calls += 1
                records = list(response.records)
                raw_by_end: dict[pd.Timestamp, list[Mapping[str, Any]]] = {}
                invalid_timestamp_rows = 0
                for raw in records:
                    end = _api_record_timestamp(raw, str(role))
                    if pd.isna(end):
                        invalid_timestamp_rows += 1
                        continue
                    if end in wanted:
                        raw_by_end.setdefault(end, []).append(raw)
                valid_seen = 0
                for target in wanted:
                    matches = raw_by_end.get(target, [])
                    if len(matches) > 1:
                        invalid_by_target[target].append(
                            f"attempt_{attempt}:duplicate_api_rows={len(matches)}"
                        )
                        continue
                    if not matches:
                        continue
                    normalized, error = _normalize_api_record(
                        matches[0], role=str(role), expected_timestamp=target
                    )
                    if normalized is None:
                        invalid_by_target[target].append(
                            f"attempt_{attempt}:{error}"
                        )
                        continue
                    valid_seen += 1
                    valid_by_target[target].append(
                        {
                            **normalized,
                            "observed_at_ist": observed_at.isoformat(
                                timespec="microseconds"
                            ),
                            "provider_id": str(response.provider_id),
                            "attempt": attempt,
                        }
                    )
                attempt_records.append(
                    {
                        "role": str(role),
                        "logical_symbol": str(logical_symbol),
                        "instrument_token": token,
                        "attempt": attempt,
                        "state": "SUCCESS",
                        "provider_id": str(response.provider_id),
                        "observed_at_ist": observed_at.isoformat(
                            timespec="microseconds"
                        ),
                        "response_record_count": int(len(records)),
                        "target_count": int(len(wanted)),
                        "valid_target_count": int(valid_seen),
                        "invalid_timestamp_rows": int(invalid_timestamp_rows),
                        "request_metadata_json": json.dumps(
                            dict(response.request_metadata),
                            sort_keys=True,
                            separators=(",", ":"),
                            default=str,
                        ),
                        "error": "",
                    }
                )
                if not wanted or all(valid_by_target[target] for target in wanted):
                    break
            except Exception as exc:
                failed_calls += 1
                attempt_records.append(
                    {
                        "role": str(role),
                        "logical_symbol": str(logical_symbol),
                        "instrument_token": token,
                        "attempt": attempt,
                        "state": "API_FAILURE",
                        "provider_id": "",
                        "observed_at_ist": observed_at.isoformat(
                            timespec="microseconds"
                        ),
                        "response_record_count": 0,
                        "target_count": int(len(wanted)),
                        "valid_target_count": 0,
                        "invalid_timestamp_rows": 0,
                        "request_metadata_json": "{}",
                        "error": f"{type(exc).__name__}: {exc}"[:1000],
                    }
                )
                if not wanted:
                    break

        for target, target_row in wanted.items():
            observations = valid_by_target[target]
            semantic_hashes = sorted(
                {str(value["semantic_sha256"]) for value in observations}
            )
            if len(semantic_hashes) > 1:
                state = "CONFLICTING_CANDLES"
                detail = json.dumps(semantic_hashes, separators=(",", ":"))
            elif observations:
                state = "CANDLE"
                detail = ""
                chosen = observations[-1]
                candle_records.append(
                    {
                        **{key: target_row[key] for key in (
                            "role",
                            "logical_symbol",
                            "equity_symbol",
                            "futures_symbol",
                            "instrument_token",
                            "session_date",
                        )},
                        "expected_timestamp": target.isoformat(),
                        "open": float(chosen["open"]),
                        "high": float(chosen["high"]),
                        "low": float(chosen["low"]),
                        "close": float(chosen["close"]),
                        "volume": float(chosen["volume"]),
                        "oi": chosen.get("oi"),
                        "semantic_sha256": str(chosen["semantic_sha256"]),
                        "observed_at_ist": str(chosen["observed_at_ist"]),
                        "provider_id": str(chosen["provider_id"]),
                    }
                )
            elif invalid_by_target[target]:
                state = "INVALID_API_DATA"
                detail = "|".join(invalid_by_target[target])[:2000]
            elif successful_calls == attempts_required and failed_calls == 0:
                state = "VERIFIED_NO_CANDLE"
                detail = ""
            else:
                state = "API_FAILURE"
                detail = (
                    f"successful_observations={successful_calls},"
                    f"api_failures={failed_calls},required={attempts_required}"
                )
            status_records.append(
                {
                    **{key: target_row[key] for key in (
                        "role",
                        "logical_symbol",
                        "equity_symbol",
                        "futures_symbol",
                        "instrument_token",
                        "session_date",
                    )},
                    "expected_timestamp": target.isoformat(),
                    "state": state,
                    "successful_observations": int(successful_calls),
                    "api_failures": int(failed_calls),
                    "valid_candle_observations": int(len(observations)),
                    "invalid_observations": int(len(invalid_by_target[target])),
                    "detail": detail,
                }
            )

    attempts = pd.DataFrame(attempt_records)
    statuses = pd.DataFrame(status_records)
    candles = pd.DataFrame(candle_records)
    if targets.empty:
        statuses = pd.DataFrame(
            columns=[
                *targets.columns,
                "state",
                "successful_observations",
                "api_failures",
                "valid_candle_observations",
                "invalid_observations",
                "detail",
            ]
        )
        attempts = pd.DataFrame(
            columns=[
                "role",
                "logical_symbol",
                "instrument_token",
                "attempt",
                "state",
                "provider_id",
                "observed_at_ist",
                "response_record_count",
                "target_count",
                "valid_target_count",
                "invalid_timestamp_rows",
                "request_metadata_json",
                "error",
            ]
        )
        candles = pd.DataFrame(
            columns=[
                *targets.columns,
                "open",
                "high",
                "low",
                "close",
                "volume",
                "oi",
                "semantic_sha256",
                "observed_at_ist",
                "provider_id",
            ]
        )
    status_path = _immutable_parquet(statuses, run_dir / "target_status.parquet")
    attempts_path = _immutable_parquet(attempts, run_dir / "attempts.parquet")
    candles_path = _immutable_parquet(candles, run_dir / "candles.parquet")
    artifacts = {
        "target_status": provenance.artifact_record(status_path),
        "attempts": provenance.artifact_record(attempts_path),
        "candles": provenance.artifact_record(candles_path),
    }
    state_counts = (
        {str(key): int(value) for key, value in statuses["state"].value_counts().items()}
        if not statuses.empty
        else {}
    )
    evidence_payload = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": _now().isoformat(timespec="microseconds"),
        "policy_version": REPAIR_POLICY_VERSION,
        "provenance_claim": (
            "RECONSTRUCTED_CURRENT_HISTORICAL_API_RESPONSE_NOT_ORIGINAL_LIVE_AS_OF"
        ),
        "source_snapshot_manifest": str(audit.source_snapshot_manifest),
        "source_snapshot_manifest_sha256": provenance.sha256_file(
            audit.source_snapshot_manifest
        ),
        "source_snapshot_fingerprint": audit.source_snapshot_fingerprint,
        "audit_fingerprint": audit.audit_fingerprint,
        "from_day": audit.from_day.isoformat(),
        "through_day": audit.through_day.isoformat(),
        "verification_attempts": attempts_required,
        "target_count": int(len(targets)),
        "state_counts": state_counts,
        "all_targets_evidenced": bool(
            statuses.empty
            or statuses["state"].isin({"CANDLE", "VERIFIED_NO_CANDLE"}).all()
        ),
        "all_targets_filled": bool(
            statuses.empty or statuses["state"].eq("CANDLE").all()
        ),
        "verified_no_candle_is_valid_exchange_coverage": False,
        "verified_no_candle_symbols": sorted(
            statuses.loc[
                statuses["state"].eq("VERIFIED_NO_CANDLE"), "logical_symbol"
            ].astype(str).unique().tolist()
        )
        if not statuses.empty
        else [],
        "api_failure_symbols": sorted(
            statuses.loc[
                statuses["state"].eq("API_FAILURE"), "logical_symbol"
            ].astype(str).unique().tolist()
        )
        if not statuses.empty
        else [],
        "artifacts": artifacts,
    }
    evidence_fingerprint = common.canonical_json_sha256(evidence_payload)
    evidence_payload["evidence_fingerprint"] = evidence_fingerprint
    return provenance.write_immutable_json(run_dir / "manifest.json", evidence_payload)


def load_repair_evidence(path: Path | str) -> dict[str, Any]:
    manifest_path = Path(path).resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != EVIDENCE_SCHEMA_VERSION or not bool(
        payload.get("complete")
    ):
        raise ValueError("Not completed V9-Honest repair evidence")
    declared_fingerprint = str(payload.get("evidence_fingerprint", ""))
    fingerprint_payload = dict(payload)
    fingerprint_payload.pop("evidence_fingerprint", None)
    if common.canonical_json_sha256(fingerprint_payload) != declared_fingerprint:
        raise AssertionError("Repair evidence fingerprint is invalid")
    for record in dict(payload.get("artifacts") or {}).values():
        if not provenance.artifact_matches(record.get("path", ""), record):
            raise AssertionError("Repair evidence artifact hash mismatch")
    return {**payload, "manifest_path": str(manifest_path)}


def _rewrite_reasons(audit: AuditResult) -> set[tuple[str, str]]:
    if audit.issues.empty:
        return set()
    relevant = audit.issues["issue_type"].isin(
        {
            "MIXED_TIMEZONE_SOURCE",
            "INVALID_TIMESTAMP",
            "OFF_GRID_TIMESTAMP",
            *TARGET_ISSUES,
        }
    )
    return {
        (str(row.role), str(row.logical_symbol))
        for row in audit.issues.loc[relevant, ["role", "logical_symbol"]].itertuples(
            index=False
        )
    }


def _canonicalize_timestamp_columns(frame: pd.DataFrame, role: str) -> pd.DataFrame:
    out = frame.copy()
    primary = "date" if role == ROLE_EQUITY and "date" in out.columns else "timestamp"
    normalized = _to_ist_series(out[primary])
    out = out.loc[normalized.notna()].copy()
    out[primary] = normalized.loc[normalized.notna()].array
    for column in ("candle_start", "fetch_timestamp"):
        if column in out.columns:
            converted = _to_ist_series(out[column])
            out[column] = converted
    return out


def _remove_off_grid_rows(
    frame: pd.DataFrame,
    *,
    role: str,
    sessions: Sequence[date],
) -> pd.DataFrame:
    primary = "date" if role == ROLE_EQUITY and "date" in frame.columns else "timestamp"
    timestamps = _to_ist_series(frame[primary])
    expected = {
        timestamp
        for session_day in sessions
        for timestamp in expected_grid(session_day, role)
    }
    session_set = set(sessions)
    in_window_session = timestamps.notna() & timestamps.dt.date.isin(session_set)
    keep = ~in_window_session | timestamps.isin(expected)
    return frame.loc[keep].copy()


def _source_contract_row(
    mapped_universe: pd.DataFrame, role: str, logical_symbol: str
) -> dict[str, Any]:
    if role == ROLE_FUTURES:
        selected = mapped_universe.loc[
            mapped_universe["futures_tradingsymbol"]
            .astype(str)
            .str.upper()
            .str.strip()
            .eq(logical_symbol)
        ]
    else:
        selected = mapped_universe.loc[
            mapped_universe["equity_symbol"]
            .astype(str)
            .str.upper()
            .str.strip()
            .eq(logical_symbol)
        ]
    if len(selected) != 1:
        raise ValueError(
            f"Cannot resolve exactly one universe row for {role}:{logical_symbol}"
        )
    return selected.iloc[0].to_dict()


def _incoming_equity_rows(candles: pd.DataFrame, evidence_hash: str) -> pd.DataFrame:
    if candles.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for candle in candles.to_dict("records"):
        timestamp = _to_ist_one(candle["expected_timestamp"])
        rows.append(
            {
                "date": timestamp,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle["volume"]),
                "gap_filled": False,
                "opening_snapshot": False,
                "provisional_stale": False,
                "repair_source": "KITE_HISTORICAL_RECONSTRUCTED",
                "repair_evidence_sha256": evidence_hash,
                "repair_observed_at_ist": str(candle["observed_at_ist"]),
            }
        )
    return pd.DataFrame(rows)


def _incoming_futures_rows(
    candles: pd.DataFrame,
    universe_row: Mapping[str, Any],
    evidence_hash: str,
) -> pd.DataFrame:
    if candles.empty:
        return pd.DataFrame()
    expiry = pd.Timestamp(universe_row["expiry"])
    symbol = str(universe_row["futures_tradingsymbol"]).upper().strip()
    token = int(universe_row["futures_instrument_token"])
    rows: list[dict[str, Any]] = []
    for candle in candles.to_dict("records"):
        timestamp = _to_ist_one(candle["expected_timestamp"])
        rows.append(
            {
                "timestamp": timestamp,
                "candle_start": timestamp - pd.Timedelta(minutes=5),
                "underlying": str(universe_row.get("underlying", "")).upper().strip(),
                "tradingsymbol": symbol,
                "instrument_token": token,
                "exchange_token": pd.to_numeric(
                    universe_row.get("exchange_token"), errors="coerce"
                ),
                "expiry": expiry,
                "contract_month": str(universe_row["contract_month"]),
                "days_to_expiry": int((expiry.date() - timestamp.date()).days),
                "lot_size": pd.to_numeric(
                    universe_row.get("futures_lot_size", universe_row.get("lot_size")),
                    errors="coerce",
                ),
                "tick_size": pd.to_numeric(
                    universe_row.get("futures_tick_size", universe_row.get("tick_size")),
                    errors="coerce",
                ),
                "is_index_future": bool(universe_row.get("is_index_future", False)),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle["volume"]),
                "oi": float(candle["oi"]),
                "quality_state": "VALID",
                "fetch_timestamp": _to_ist_one(candle["observed_at_ist"]),
                "source": "kite_historical_reconstructed",
                "data_version": "fno_v9_honest_repair_v1",
                "repair_evidence_sha256": evidence_hash,
            }
        )
    return pd.DataFrame(rows)


def _build_repaired_file(
    *,
    source_path: Path,
    target_path: Path,
    role: str,
    logical_symbol: str,
    sessions: Sequence[date],
    target_statuses: pd.DataFrame,
    candles: pd.DataFrame,
    universe_row: Mapping[str, Any],
    evidence_hash: str,
) -> dict[str, Any]:
    original = pd.read_parquet(source_path, engine="pyarrow")
    work = _canonicalize_timestamp_columns(original, role)
    work = _remove_off_grid_rows(work, role=role, sessions=sessions)
    primary = "date" if role == ROLE_EQUITY and "date" in work.columns else "timestamp"
    work[primary] = _to_ist_series(work[primary])
    target_timestamps = {
        _to_ist_one(value) for value in target_statuses["expected_timestamp"].tolist()
    }
    if target_timestamps:
        work = work.loc[~work[primary].isin(target_timestamps)].copy()
    incoming = (
        _incoming_equity_rows(candles, evidence_hash)
        if role == ROLE_EQUITY
        else _incoming_futures_rows(candles, universe_row, evidence_hash)
    )
    if not incoming.empty:
        combined = pd.concat([work, incoming], ignore_index=True, sort=False)
    else:
        combined = work
    combined[primary] = _to_ist_series(combined[primary])
    combined = (
        combined.dropna(subset=[primary])
        .drop_duplicates(primary, keep="last")
        .sort_values(primary, kind="stable")
        .reset_index(drop=True)
    )
    _immutable_parquet(combined, target_path)
    return {
        "role": role,
        "logical_symbol": logical_symbol,
        "source_path": str(source_path.resolve()),
        "snapshot_path": str(target_path.resolve()),
        "source_sha256": provenance.sha256_file(source_path),
        "sha256": provenance.sha256_file(target_path),
        "source_rows": int(len(original)),
        "snapshot_rows": int(len(combined)),
        "repair_target_rows": int(len(target_statuses)),
        "inserted_candle_rows": int(len(incoming)),
        "canonicalized": True,
        "physical_copy": True,
    }


def _repair_fingerprint_payload(
    *,
    base_snapshot_fingerprint: str,
    base_manifest_record: Mapping[str, Any],
    evidence_fingerprint: str,
    evidence_manifest_record: Mapping[str, Any],
    evidence_artifacts: Mapping[str, Any],
    repaired_inventory: Mapping[str, Any],
    post_audit_summary: Mapping[str, Any],
    state_counts: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": REPAIR_LINEAGE_SCHEMA_VERSION,
        "policy_version": REPAIR_POLICY_VERSION,
        "base_snapshot_fingerprint": base_snapshot_fingerprint,
        "base_manifest_sha256": base_manifest_record.get("sha256", ""),
        "evidence_fingerprint": evidence_fingerprint,
        "evidence_manifest_sha256": evidence_manifest_record.get("sha256", ""),
        "evidence_artifact_sha256": {
            key: dict(value).get("sha256", "")
            for key, value in sorted(evidence_artifacts.items())
        },
        "repaired_inventory_sha256": repaired_inventory.get("inventory_sha256", ""),
        "repaired_source_fingerprint": repaired_inventory.get("source_fingerprint", ""),
        "post_audit_summary": dict(post_audit_summary),
        "state_counts": dict(state_counts),
    }


def publish_repaired_snapshot(
    source_snapshot: Path | str,
    evidence_manifest: Path | str,
    *,
    snapshot_root: Path | str = DEFAULT_REPAIRED_SNAPSHOT_ROOT,
    allow_unresolved: bool = False,
    allow_evidenced_absence: bool = False,
) -> Path:
    base_manifest = Path(source_snapshot).resolve()
    if base_manifest.is_dir():
        base_manifest = base_manifest / "manifest.json"
    contract = load_snapshot_contract(base_manifest)
    evidence = load_repair_evidence(evidence_manifest)
    if str(evidence.get("policy_version", "")) != REPAIR_POLICY_VERSION:
        raise ValueError(
            "Repair evidence policy differs from the active publication policy"
        )
    if str(evidence.get("source_snapshot_fingerprint", "")) != str(
        contract.snapshot.get("snapshot_fingerprint", "")
    ):
        raise AssertionError("Repair evidence belongs to a different source snapshot")
    if provenance.sha256_file(base_manifest) != str(
        evidence.get("source_snapshot_manifest_sha256", "")
    ):
        raise AssertionError("Repair evidence base manifest bytes changed")
    artifacts = dict(evidence["artifacts"])
    statuses = pd.read_parquet(artifacts["target_status"]["path"])
    candles = pd.read_parquet(artifacts["candles"]["path"])
    resolved_states = {"CANDLE", "VERIFIED_NO_CANDLE"}
    unresolved = statuses.loc[~statuses["state"].isin(resolved_states)]
    if not allow_unresolved and not unresolved.empty:
        counts = unresolved["state"].value_counts().to_dict()
        raise RuntimeError(f"Unresolved repair evidence blocks publication: {counts}")
    evidenced_absence = statuses.loc[
        statuses["state"].eq("VERIFIED_NO_CANDLE")
    ]
    if not allow_evidenced_absence and not evidenced_absence.empty:
        raise RuntimeError(
            "Verified no-candle evidence is an evidenced provider absence, not "
            "valid exchange coverage; exact snapshot publication is blocked: "
            f"targets={len(evidenced_absence)}"
        )

    audit = audit_snapshot(
        base_manifest,
        from_day=evidence["from_day"],
        through_day=evidence["through_day"],
    )
    if audit.audit_fingerprint != str(evidence.get("audit_fingerprint", "")):
        raise AssertionError("Repair evidence no longer matches the exact base audit")
    invalid_timestamp_files = audit.issues.loc[
        audit.issues["issue_type"].eq("INVALID_TIMESTAMP"),
        ["role", "logical_symbol"],
    ].drop_duplicates()
    if not allow_unresolved and not invalid_timestamp_files.empty:
        raise RuntimeError(
            "Unattributable invalid timestamps block exact publication: "
            f"files={len(invalid_timestamp_files)}"
        )
    targets = _repair_targets(audit)
    declared_target_keys = {
        (str(row.role), str(row.logical_symbol), str(row.expected_timestamp))
        for row in statuses.itertuples(index=False)
    }
    actual_target_keys = {
        (str(row.role), str(row.logical_symbol), str(row.expected_timestamp))
        for row in targets.itertuples(index=False)
    }
    if declared_target_keys != actual_target_keys:
        raise AssertionError("Repair evidence target set no longer matches the base audit")
    if len(statuses) != len(declared_target_keys):
        raise ValueError("Repair evidence has duplicate target status rows")
    if not candles.empty:
        if candles.duplicated(
            ["role", "logical_symbol", "expected_timestamp"]
        ).any():
            raise ValueError("Repair candle artifact has duplicate target rows")
        candle_artifact_keys = {
            (str(row.role), str(row.logical_symbol), str(row.expected_timestamp))
            for row in candles.itertuples(index=False)
        }
        candle_status_keys = {
            (str(row.role), str(row.logical_symbol), str(row.expected_timestamp))
            for row in statuses.loc[statuses["state"].eq("CANDLE")].itertuples(
                index=False
            )
        }
        if candle_artifact_keys != candle_status_keys:
            raise ValueError(
                "Repair candle artifact is inconsistent with target statuses"
            )

    root = Path(snapshot_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    snapshot_dir = root / (
        f"snapshot_{_now().strftime('%Y%m%dT%H%M%S%f%z')}_"
        f"{str(evidence['evidence_fingerprint'])[:12]}_{uuid.uuid4().hex[:8]}"
    )
    snapshot_dir.mkdir()
    futures_root = snapshot_dir / "futures_5m"
    equity_root = snapshot_dir / "equity_1m"
    universe_root = snapshot_dir / "universe"
    lineage_root = snapshot_dir / "repair_lineage"
    for directory in (futures_root, equity_root, universe_root, lineage_root):
        directory.mkdir()

    rewrite = _rewrite_reasons(audit)
    evidence_hash = provenance.sha256_file(evidence["manifest_path"])
    captures: list[dict[str, Any]] = []
    for entry in contract.inventory.get("entries", []):
        role = str(entry["role"])
        logical_symbol = str(entry["logical_symbol"])
        source_path = Path(str(entry["resolved_path"])).resolve()
        target_root = futures_root if role == ROLE_FUTURES else equity_root
        target_path = target_root / source_path.name
        selected_status = statuses.loc[
            statuses["role"].astype(str).eq(role)
            & statuses["logical_symbol"].astype(str).eq(logical_symbol)
        ].copy()
        selected_candles = candles.loc[
            candles["role"].astype(str).eq(role)
            & candles["logical_symbol"].astype(str).eq(logical_symbol)
        ].copy() if not candles.empty else candles.copy()
        if (role, logical_symbol) in rewrite or not selected_status.empty:
            universe_row = _source_contract_row(
                contract.mapped_universe, role, logical_symbol
            )
            capture = _build_repaired_file(
                source_path=source_path,
                target_path=target_path,
                role=role,
                logical_symbol=logical_symbol,
                sessions=audit.session_dates,
                target_statuses=selected_status,
                candles=selected_candles,
                universe_row=universe_row,
                evidence_hash=evidence_hash,
            )
        else:
            provenance.publish_immutable_copy(
                source_path,
                target_path,
                expected_sha256=str(entry["sha256"]),
            )
            capture = {
                "role": role,
                "logical_symbol": logical_symbol,
                "source_path": str(source_path),
                "snapshot_path": str(target_path),
                "source_sha256": str(entry["sha256"]),
                "sha256": provenance.sha256_file(target_path),
                "source_rows": int(pq.read_metadata(source_path).num_rows),
                "snapshot_rows": int(pq.read_metadata(target_path).num_rows),
                "repair_target_rows": 0,
                "inserted_candle_rows": 0,
                "canonicalized": False,
                "physical_copy": True,
            }
        captures.append(capture)

    frozen_universe = universe_root / Path(contract.snapshot["universe_path"]).name
    provenance.publish_immutable_copy(
        contract.snapshot["universe_path"],
        frozen_universe,
        expected_sha256=str(contract.universe_record["file_sha256"]),
    )
    universe_capture = {
        **provenance.artifact_record(frozen_universe),
        "source_path": str(Path(contract.snapshot["universe_path"]).resolve()),
        "snapshot_path": str(frozen_universe.resolve()),
        "physical_copy": True,
    }
    repaired_inventory = provenance.build_source_inventory(
        contract.mapped_universe,
        contract.universe_record,
        futures_5m_root=futures_root,
        equity_1m_root=equity_root,
    )
    provenance.validate_source_inventory_readable(repaired_inventory)
    repaired_lookup = {
        (str(entry["role"]), str(entry["logical_symbol"])): Path(
            str(entry["resolved_path"])
        ).resolve()
        for entry in repaired_inventory["entries"]
        if bool(entry["exists"])
    }
    provisional_contract = SnapshotContract(
        mapped_universe=contract.mapped_universe,
        universe_record=contract.universe_record,
        snapshot={
            "snapshot_fingerprint": "PENDING",
            "futures_5m_root": str(futures_root),
            "equity_1m_root": str(equity_root),
        },
        inventory=repaired_inventory,
        source_lookup=repaired_lookup,
    )
    post_audit = _audit_contract(
        provisional_contract,
        from_day=audit.from_day,
        through_day=audit.through_day,
        source_snapshot_manifest=snapshot_dir / "manifest.json",
    )
    post_issue_keys = {
        (str(row.role), str(row.logical_symbol), str(row.expected_timestamp))
        for row in post_audit.issues.loc[
            post_audit.issues["expected_timestamp"].astype(str).ne("")
        ].itertuples(index=False)
    }
    candle_keys = {
        (str(row.role), str(row.logical_symbol), str(row.expected_timestamp))
        for row in statuses.loc[statuses["state"].eq("CANDLE")].itertuples(index=False)
    }
    if candle_keys & post_issue_keys:
        raise AssertionError("A published repair candle still fails the exact-grid audit")

    local_base_manifest = lineage_root / "base_snapshot_manifest.json"
    provenance.publish_immutable_copy(base_manifest, local_base_manifest)
    local_evidence_manifest = lineage_root / "evidence_manifest.json"
    provenance.publish_immutable_copy(evidence["manifest_path"], local_evidence_manifest)
    local_evidence_artifacts: dict[str, dict[str, Any]] = {}
    evidence_artifact_root = lineage_root / "evidence_artifacts"
    evidence_artifact_root.mkdir()
    for name, record in sorted(artifacts.items()):
        source = Path(str(record["path"])).resolve()
        target = evidence_artifact_root / source.name
        provenance.publish_immutable_copy(
            source, target, expected_sha256=str(record["sha256"])
        )
        local_evidence_artifacts[name] = provenance.artifact_record(target)
    base_record = provenance.artifact_record(local_base_manifest)
    evidence_record = provenance.artifact_record(local_evidence_manifest)
    repair_payload = _repair_fingerprint_payload(
        base_snapshot_fingerprint=str(contract.snapshot["snapshot_fingerprint"]),
        base_manifest_record=base_record,
        evidence_fingerprint=str(evidence["evidence_fingerprint"]),
        evidence_manifest_record=evidence_record,
        evidence_artifacts=local_evidence_artifacts,
        repaired_inventory=repaired_inventory,
        post_audit_summary=post_audit.summary,
        state_counts=dict(evidence.get("state_counts") or {}),
    )
    repair_fingerprint = common.canonical_json_sha256(repair_payload)
    lineage_payload = {
        **repair_payload,
        "repair_fingerprint": repair_fingerprint,
        "provenance_claim": (
            "ISOLATED_RECONSTRUCTED_RESEARCH_SNAPSHOT_NOT_ORIGINAL_LIVE_AS_OF"
        ),
        "base_snapshot_manifest": base_record,
        "evidence_manifest": evidence_record,
        "evidence_artifacts": local_evidence_artifacts,
        "verified_no_candle_symbols": list(
            evidence.get("verified_no_candle_symbols") or []
        ),
        "api_failure_symbols": list(evidence.get("api_failure_symbols") or []),
    }
    lineage_manifest = provenance.write_immutable_json(
        lineage_root / "manifest.json", lineage_payload
    )

    snapshot_fingerprint = common.canonical_json_sha256(
        {
            "schema_version": provenance.SOURCE_SNAPSHOT_SCHEMA_VERSION,
            "universe_file_sha256": contract.universe_record.get("file_sha256", ""),
            "mapped_universe_sha256": contract.universe_record.get(
                "mapped_universe_sha256", ""
            ),
            "source_inventory_sha256": repaired_inventory["inventory_sha256"],
            "source_fingerprint": repaired_inventory["source_fingerprint"],
        }
    )
    payload = {
        "schema_version": provenance.SOURCE_SNAPSHOT_SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": _now().isoformat(timespec="microseconds"),
        "capture_scope": (
            "ISOLATED_REPAIRED_PHYSICAL_COPY; BASE_SNAPSHOT_NEVER_MODIFIED; "
            "MANIFEST_PUBLISHED_AFTER_VALIDATION"
        ),
        "physical_copy": True,
        "snapshot_fingerprint": snapshot_fingerprint,
        "snapshot_dir": str(snapshot_dir),
        "futures_5m_root": str(futures_root),
        "equity_1m_root": str(equity_root),
        "universe_path": str(frozen_universe),
        "universe": dict(contract.universe_record),
        "universe_capture": universe_capture,
        "source_inventory": repaired_inventory,
        "captures": captures,
        "repair_provenance": {
            "schema_version": REPAIR_LINEAGE_SCHEMA_VERSION,
            "manifest_path": str(lineage_manifest.resolve()),
            "manifest_sha256": provenance.sha256_file(lineage_manifest),
            "repair_fingerprint": repair_fingerprint,
            "base_snapshot_fingerprint": str(
                contract.snapshot["snapshot_fingerprint"]
            ),
            "evidence_fingerprint": str(evidence["evidence_fingerprint"]),
            "post_audit_summary": post_audit.summary,
        },
    }
    manifest = provenance.write_immutable_json(snapshot_dir / "manifest.json", payload)
    validate_repaired_snapshot(manifest)
    return manifest


def validate_repaired_snapshot(path: Path | str) -> dict[str, Any]:
    manifest = Path(path).resolve()
    if manifest.is_dir():
        manifest = manifest / "manifest.json"
    contract = load_snapshot_contract(manifest)
    repair = dict(contract.snapshot.get("repair_provenance") or {})
    if repair.get("schema_version") != REPAIR_LINEAGE_SCHEMA_VERSION:
        raise ValueError("Snapshot has no supported V9-Honest repair provenance")
    lineage_path = Path(str(repair.get("manifest_path", ""))).resolve()
    snapshot_dir = Path(str(contract.snapshot["snapshot_dir"])).resolve()
    if snapshot_dir not in lineage_path.parents:
        raise ValueError("Repair lineage manifest escapes the repaired snapshot")
    if provenance.sha256_file(lineage_path) != str(repair.get("manifest_sha256", "")):
        raise AssertionError("Repair lineage manifest hash is invalid")
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    if lineage.get("schema_version") != REPAIR_LINEAGE_SCHEMA_VERSION:
        raise ValueError("Unsupported repair lineage schema")
    for record in (
        dict(lineage.get("base_snapshot_manifest") or {}),
        dict(lineage.get("evidence_manifest") or {}),
        *[
            dict(value)
            for value in dict(lineage.get("evidence_artifacts") or {}).values()
        ],
    ):
        record_path = Path(str(record.get("path", ""))).resolve()
        if snapshot_dir not in record_path.parents:
            raise ValueError("Repair lineage artifact escapes the repaired snapshot")
        if not provenance.artifact_matches(record_path, record):
            raise AssertionError("Repair lineage artifact hash mismatch")
    fingerprint_payload = _repair_fingerprint_payload(
        base_snapshot_fingerprint=str(lineage["base_snapshot_fingerprint"]),
        base_manifest_record=dict(lineage["base_snapshot_manifest"]),
        evidence_fingerprint=str(lineage["evidence_fingerprint"]),
        evidence_manifest_record=dict(lineage["evidence_manifest"]),
        evidence_artifacts=dict(lineage["evidence_artifacts"]),
        repaired_inventory=contract.inventory,
        post_audit_summary=dict(lineage["post_audit_summary"]),
        state_counts=dict(lineage["state_counts"]),
    )
    observed_fingerprint = common.canonical_json_sha256(fingerprint_payload)
    if observed_fingerprint != str(lineage.get("repair_fingerprint", "")) or (
        observed_fingerprint != str(repair.get("repair_fingerprint", ""))
    ):
        raise AssertionError("Repair lineage fingerprint is invalid")
    return {
        "manifest_path": str(manifest),
        "snapshot_fingerprint": str(contract.snapshot["snapshot_fingerprint"]),
        "repair_fingerprint": observed_fingerprint,
        "source_fingerprint": str(contract.inventory["source_fingerprint"]),
        "post_audit_summary": dict(lineage["post_audit_summary"]),
    }


def _print_json(payload: Mapping[str, Any]) -> None:
    print(
        json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True, default=str),
        flush=True,
    )


def _plan_summary(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if key != "groups"}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    audit_parser = commands.add_parser(
        "audit", help="Read-only exact-grid audit of a physical source snapshot"
    )
    audit_parser.add_argument("--source-snapshot", type=Path, required=True)
    audit_parser.add_argument("--from-day", default="2026-05-27")
    audit_parser.add_argument("--through-day", default="2026-07-31")
    audit_parser.add_argument("--audit-root", type=Path, default=DEFAULT_AUDIT_ROOT)
    audit_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and print the audit without publishing audit artifacts",
    )

    plan_parser = commands.add_parser(
        "plan", help="Print the exact repair target/API subrequest plan"
    )
    plan_parser.add_argument("--audit-manifest", type=Path, required=True)
    plan_parser.add_argument("--verification-attempts", type=int, default=3)
    plan_parser.add_argument("--include-groups", action="store_true")

    fetch_parser = commands.add_parser(
        "fetch", help="Collect immutable Kite response evidence; never edit sources"
    )
    fetch_parser.add_argument("--audit-manifest", type=Path, required=True)
    fetch_parser.add_argument("--evidence-root", type=Path, default=DEFAULT_EVIDENCE_ROOT)
    fetch_parser.add_argument("--verification-attempts", type=int, default=3)
    fetch_parser.add_argument("--max-apps", type=int, default=8)
    fetch_parser.add_argument("--timeout-sec", type=float, default=12.0)
    fetch_parser.add_argument("--request-interval-sec", type=float, default=0.34)
    fetch_parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually call Kite; without this flag fetch is a network-free dry run",
    )

    publish_parser = commands.add_parser(
        "publish", help="Create a new isolated repaired physical snapshot"
    )
    publish_parser.add_argument("--source-snapshot", type=Path, required=True)
    publish_parser.add_argument("--evidence-manifest", type=Path, required=True)
    publish_parser.add_argument(
        "--snapshot-root", type=Path, default=DEFAULT_REPAIRED_SNAPSHOT_ROOT
    )
    publish_parser.add_argument(
        "--allow-unresolved",
        action="store_true",
        help="Diagnostic only: permit API failures/invalid/conflicting evidence",
    )
    publish_parser.add_argument(
        "--allow-evidenced-absence",
        action="store_true",
        help=(
            "Diagnostic only: publish despite VERIFIED_NO_CANDLE cells; those "
            "sessions remain incomplete and are never treated as valid coverage"
        ),
    )

    validate_parser = commands.add_parser(
        "validate", help="Re-hash and validate a repaired snapshot and its lineage"
    )
    validate_parser.add_argument("--snapshot", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    if args.command == "audit":
        audit = audit_snapshot(
            args.source_snapshot,
            from_day=args.from_day,
            through_day=args.through_day,
        )
        plan = build_fetch_plan(audit)
        output: dict[str, Any] = {
            "audit": audit.summary,
            "fetch_plan": _plan_summary(plan),
            "dry_run": bool(args.dry_run),
        }
        if not args.dry_run:
            output["audit_manifest"] = str(
                publish_audit(audit, audit_root=args.audit_root)
            )
        _print_json(output)
        return 0
    if args.command == "plan":
        audit = audit_from_manifest(args.audit_manifest)
        plan = build_fetch_plan(
            audit, verification_attempts=args.verification_attempts
        )
        _print_json(plan if args.include_groups else _plan_summary(plan))
        return 0
    if args.command == "fetch":
        audit = audit_from_manifest(args.audit_manifest)
        plan = build_fetch_plan(
            audit, verification_attempts=args.verification_attempts
        )
        _print_json({"fetch_plan": _plan_summary(plan), "execute": bool(args.execute)})
        if not args.execute:
            return 0
        provider = KiteHistoricalProvider.from_existing_credentials(
            max_apps=args.max_apps,
            timeout_sec=args.timeout_sec,
            request_interval_sec=args.request_interval_sec,
        )
        evidence_path = collect_repair_evidence(
            audit,
            provider,
            evidence_root=args.evidence_root,
            verification_attempts=args.verification_attempts,
        )
        _print_json({"evidence_manifest": str(evidence_path)})
        return 0
    if args.command == "publish":
        manifest = publish_repaired_snapshot(
            args.source_snapshot,
            args.evidence_manifest,
            snapshot_root=args.snapshot_root,
            allow_unresolved=bool(args.allow_unresolved),
            allow_evidenced_absence=bool(args.allow_evidenced_absence),
        )
        _print_json(validate_repaired_snapshot(manifest))
        return 0
    if args.command == "validate":
        _print_json(validate_repaired_snapshot(args.snapshot))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
