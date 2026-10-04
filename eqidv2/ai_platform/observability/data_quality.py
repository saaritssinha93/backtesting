"""Deterministic data fingerprints and append-only observation evidence.

The helpers in this module deliberately have no dependency on a metrics or
tracing backend.  Market-data evidence must remain usable when the telemetry
collector is unavailable, and observability must never become part of the
trading decision path.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


FRAME_FINGERPRINT_SCHEMA = "ai_platform_frame_fingerprint_v1"
INPUT_SLICE_SCHEMA = "ai_platform_input_slice_fingerprint_v1"
DATA_QUALITY_SCHEMA = "ai_platform_data_quality_v1"
OBSERVATION_SCHEMA = "ai_platform_observation_v1"
_SAFE_KIND = re.compile(r"[^a-zA-Z0-9_.-]+")


def _canonical_value(value: Any) -> Any:
    """Return a JSON-safe representation without lossy string coercion."""

    if value is pd.NA or value is pd.NaT or value is None:
        return None
    if isinstance(value, np.generic):
        return _canonical_value(value.item())
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        encoded = [_canonical_value(item) for item in value]
        return sorted(encoded, key=lambda item: _canonical_json_bytes(item))
    if isinstance(value, (pd.Timestamp, datetime)):
        stamp = pd.Timestamp(value)
        return {"__timestamp__": stamp.isoformat()}
    if isinstance(value, date):
        return {"__date__": value.isoformat()}
    if isinstance(value, pd.Timedelta):
        return {"__timedelta_ns__": int(value.value)}
    if isinstance(value, Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, Path):
        return {"__path__": str(value)}
    if isinstance(value, bytes):
        return {"__bytes_b64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, float):
        if math.isnan(value):
            return {"__float__": "nan"}
        if math.isinf(value):
            return {"__float__": "inf" if value > 0 else "-inf"}
        if value == 0:
            return 0.0
        return value
    if isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"Unsupported canonical value type: {type(value).__name__}")


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_payload_sha256(payload: Any) -> str:
    """Hash a supported Python payload using the shared canonical encoding."""

    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def canonical_row_sha256(row: Mapping[str, Any]) -> str:
    """Hash one named row independently of mapping insertion order."""

    return canonical_payload_sha256(dict(row))


def canonical_frame_sha256(
    frame: pd.DataFrame,
    *,
    columns: Sequence[str] | None = None,
    sort_by: Sequence[str] | None = None,
) -> str:
    """Return a deterministic SHA-256 for a dataframe's named values.

    Index values and pandas dtypes are intentionally excluded.  Column order
    and row order are included unless the caller supplies ``columns`` or
    ``sort_by``.  This makes the contract explicit and avoids hashes changing
    merely because a RangeIndex or nullable dtype was reconstructed.
    """

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas.DataFrame")
    if frame.columns.duplicated().any():
        raise ValueError("frame fingerprint does not permit duplicate columns")
    selected_columns = list(frame.columns if columns is None else columns)
    missing = [name for name in selected_columns if name not in frame.columns]
    if missing:
        raise KeyError(f"fingerprint columns are missing: {missing}")
    order = list(sort_by or [])
    missing_sort = [name for name in order if name not in selected_columns]
    if missing_sort:
        raise KeyError(f"sort columns are not in the fingerprint slice: {missing_sort}")
    values = frame.loc[:, selected_columns].copy()
    if order:
        values = values.sort_values(order, kind="stable", na_position="last")
    rows = [
        [_canonical_value(value) for value in row]
        for row in values.itertuples(index=False, name=None)
    ]
    payload = {
        "schema_version": FRAME_FINGERPRINT_SCHEMA,
        "columns": [str(name) for name in selected_columns],
        "rows": rows,
    }
    return canonical_payload_sha256(payload)


@dataclass(frozen=True)
class InputSliceFingerprint:
    schema_version: str
    sha256: str
    row_count: int
    columns: tuple[str, ...]
    timestamp_column: str
    start: str | None
    end: str | None
    first_timestamp: str | None
    last_timestamp: str | None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["columns"] = list(self.columns)
        return result


def _parse_timestamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("timestamp is missing")
    return stamp


def hash_input_window(
    frame: pd.DataFrame,
    *,
    timestamp_column: str = "ts",
    start: Any | None = None,
    end: Any | None = None,
    columns: Sequence[str] | None = None,
) -> InputSliceFingerprint:
    """Fingerprint an inclusive, chronologically ordered source-data window."""

    if timestamp_column not in frame.columns:
        raise KeyError(f"timestamp column is missing: {timestamp_column}")
    selected_columns = list(columns or frame.columns)
    if timestamp_column not in selected_columns:
        selected_columns.insert(0, timestamp_column)
    working = frame.loc[:, selected_columns].copy()
    parsed = pd.to_datetime(working[timestamp_column], errors="coerce")
    if parsed.isna().any():
        raise ValueError("input slice contains invalid timestamps")
    start_stamp = _parse_timestamp(start) if start is not None else None
    end_stamp = _parse_timestamp(end) if end is not None else None
    if start_stamp is not None and end_stamp is not None and start_stamp > end_stamp:
        raise ValueError("input slice start must not be later than end")
    mask = pd.Series(True, index=working.index)
    if start_stamp is not None:
        mask &= parsed.ge(start_stamp)
    if end_stamp is not None:
        mask &= parsed.le(end_stamp)
    working = working.loc[mask].copy()
    parsed = parsed.loc[mask]
    working["__fingerprint_timestamp__"] = parsed
    working = working.sort_values("__fingerprint_timestamp__", kind="stable")
    normalized_columns = [
        "__fingerprint_timestamp__" if name == timestamp_column else name
        for name in selected_columns
    ]
    digest_frame = working.loc[:, normalized_columns].rename(
        columns={"__fingerprint_timestamp__": timestamp_column}
    )
    ordered = parsed.sort_values(kind="stable")
    return InputSliceFingerprint(
        schema_version=INPUT_SLICE_SCHEMA,
        sha256=canonical_frame_sha256(digest_frame),
        row_count=len(digest_frame),
        columns=tuple(str(name) for name in selected_columns),
        timestamp_column=timestamp_column,
        start=start_stamp.isoformat() if start_stamp is not None else None,
        end=end_stamp.isoformat() if end_stamp is not None else None,
        first_timestamp=ordered.iloc[0].isoformat() if len(ordered) else None,
        last_timestamp=ordered.iloc[-1].isoformat() if len(ordered) else None,
    )


@dataclass(frozen=True)
class DataQualityReport:
    schema_version: str
    source: str
    symbol: str
    status: str
    row_count: int
    expected_count: int | None
    timestamp_min: str | None
    timestamp_max: str | None
    missing_columns: tuple[str, ...]
    invalid_timestamp_count: int
    duplicate_timestamp_count: int
    out_of_order_count: int
    missing_timestamp_count: int
    missing_timestamps: tuple[str, ...]
    invalid_ohlcv_count: int
    invalid_oi_count: int
    zero_oi_count: int
    content_sha256: str | None
    issues: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def usable(self) -> bool:
        return self.status != "BLOCKED"

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        for name in ("missing_columns", "missing_timestamps", "issues"):
            result[name] = list(result[name])
        result["usable"] = self.usable
        return result


def _stamp_key(stamp: pd.Timestamp) -> tuple[str, int]:
    if stamp.tzinfo is None:
        return "NAIVE", int(stamp.value)
    return "UTC", int(stamp.tz_convert("UTC").value)


def evaluate_ohlcv(
    frame: pd.DataFrame,
    *,
    timestamp_column: str = "ts",
    expected_timestamps: Iterable[Any] | None = None,
    source: str = "",
    symbol: str = "",
) -> DataQualityReport:
    """Evaluate raw equity/futures OHLCV and optional OI data.

    Zero OI is reported separately because it can be valid provider data while
    remaining unusable for V13-V10-G's positive current/previous OI gates.
    """

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas.DataFrame")
    required = [timestamp_column, "open", "high", "low", "close", "volume"]
    missing_columns = tuple(name for name in required if name not in frame.columns)
    issues: list[dict[str, Any]] = []
    if missing_columns:
        issues.append({"code": "MISSING_COLUMNS", "severity": "BLOCKED", "columns": list(missing_columns)})
        return DataQualityReport(
            DATA_QUALITY_SCHEMA, source, symbol, "BLOCKED", len(frame), None,
            None, None, missing_columns, 0, 0, 0, 0, (), 0, 0, 0, None,
            tuple(issues),
        )

    stamps = pd.to_datetime(frame[timestamp_column], errors="coerce")
    invalid_timestamp_count = int(stamps.isna().sum())
    valid_stamps = stamps.dropna()
    duplicate_timestamp_count = int(valid_stamps.duplicated(keep=False).sum())
    deltas = valid_stamps.diff()
    out_of_order_count = int(deltas.lt(pd.Timedelta(0)).sum())

    expected = None if expected_timestamps is None else pd.DatetimeIndex(
        [_parse_timestamp(value) for value in expected_timestamps]
    )
    observed_keys = {_stamp_key(stamp) for stamp in valid_stamps}
    missing_stamps = (
        [stamp for stamp in expected if _stamp_key(stamp) not in observed_keys]
        if expected is not None else []
    )

    numeric = frame[["open", "high", "low", "close", "volume"]].apply(
        pd.to_numeric, errors="coerce"
    )
    finite = np.isfinite(numeric.to_numpy(dtype=float)).all(axis=1)
    invalid_ohlcv = (
        ~finite
        | numeric[["open", "high", "low", "close"]].le(0).any(axis=1)
        | numeric["volume"].lt(0)
        | numeric["high"].lt(numeric[["open", "close"]].max(axis=1))
        | numeric["low"].gt(numeric[["open", "close"]].min(axis=1))
        | numeric["high"].lt(numeric["low"])
    )
    invalid_ohlcv_count = int(invalid_ohlcv.sum())

    invalid_oi_count = zero_oi_count = 0
    if "oi" in frame.columns:
        oi = pd.to_numeric(frame["oi"], errors="coerce")
        invalid_oi_count = int((oi.isna() | ~np.isfinite(oi) | oi.lt(0)).sum())
        zero_oi_count = int(oi.eq(0).sum())

    counts = (
        ("INVALID_TIMESTAMPS", invalid_timestamp_count, "BLOCKED"),
        ("DUPLICATE_TIMESTAMPS", duplicate_timestamp_count, "BLOCKED"),
        ("OUT_OF_ORDER_TIMESTAMPS", out_of_order_count, "WARN"),
        ("MISSING_EXPECTED_TIMESTAMPS", len(missing_stamps), "BLOCKED"),
        ("INVALID_OHLCV", invalid_ohlcv_count, "BLOCKED"),
        ("INVALID_OI", invalid_oi_count, "BLOCKED"),
        ("ZERO_OI_UNUSABLE_FOR_STRATEGY", zero_oi_count, "WARN"),
    )
    for code, count, severity in counts:
        if count:
            issue: dict[str, Any] = {"code": code, "severity": severity, "count": count}
            if code == "MISSING_EXPECTED_TIMESTAMPS":
                issue["timestamps"] = [stamp.isoformat() for stamp in missing_stamps]
            issues.append(issue)
    status = "BLOCKED" if any(item["severity"] == "BLOCKED" for item in issues) else "WARN" if issues else "GOOD"
    content_sha256 = canonical_frame_sha256(frame)
    return DataQualityReport(
        schema_version=DATA_QUALITY_SCHEMA,
        source=source,
        symbol=symbol,
        status=status,
        row_count=len(frame),
        expected_count=len(expected) if expected is not None else None,
        timestamp_min=valid_stamps.min().isoformat() if len(valid_stamps) else None,
        timestamp_max=valid_stamps.max().isoformat() if len(valid_stamps) else None,
        missing_columns=missing_columns,
        invalid_timestamp_count=invalid_timestamp_count,
        duplicate_timestamp_count=duplicate_timestamp_count,
        out_of_order_count=out_of_order_count,
        missing_timestamp_count=len(missing_stamps),
        missing_timestamps=tuple(stamp.isoformat() for stamp in missing_stamps),
        invalid_ohlcv_count=invalid_ohlcv_count,
        invalid_oi_count=invalid_oi_count,
        zero_oi_count=zero_oi_count,
        content_sha256=content_sha256,
        issues=tuple(issues),
    )


@dataclass(frozen=True)
class LedgerVerification:
    valid: bool
    record_count: int
    invalid_count: int
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AppendOnlyObservationLedger:
    """Self-validating immutable JSON records for raw-data observations.

    Records are created with exclusive file creation.  Existing records are
    never rewritten.  The verifier detects modified/truncated records; normal
    filesystem backup or WORM storage is still required to detect deletion.
    """

    def __init__(self, root: Path | str):
        self.root = Path(root)

    def append(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        observed_at: datetime | str | None = None,
        identity: Mapping[str, Any] | None = None,
    ) -> Path:
        safe_kind = _SAFE_KIND.sub("_", str(kind).strip()).strip("._")
        if not safe_kind:
            raise ValueError("observation kind must contain a safe character")
        stamp = pd.Timestamp(observed_at or datetime.now(timezone.utc))
        if pd.isna(stamp):
            raise ValueError("observed_at is invalid")
        if stamp.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        stamp = stamp.tz_convert("UTC")
        event_id = uuid.uuid4().hex
        normalized_payload = _canonical_value(dict(payload))
        record: dict[str, Any] = {
            "schema_version": OBSERVATION_SCHEMA,
            "event_id": event_id,
            "kind": safe_kind,
            "observed_at_utc": stamp.isoformat(),
            "identity": _canonical_value(dict(identity or {})),
            "payload": normalized_payload,
            "payload_sha256": canonical_payload_sha256(normalized_payload),
        }
        record["record_sha256"] = canonical_payload_sha256(record)
        directory = self.root / safe_kind / stamp.strftime("%Y-%m-%d")
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{stamp.strftime('%Y%m%dT%H%M%S%fZ')}_{event_id}.json"
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
        with path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        return path

    def verify(self) -> LedgerVerification:
        errors: list[str] = []
        count = 0
        seen_ids: set[str] = set()
        if not self.root.exists():
            return LedgerVerification(True, 0, 0, ())
        for path in sorted(self.root.rglob("*.json")):
            count += 1
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
                if record.get("schema_version") != OBSERVATION_SCHEMA:
                    raise ValueError("unsupported schema")
                event_id = str(record.get("event_id", ""))
                if not event_id or event_id in seen_ids:
                    raise ValueError("missing or duplicate event_id")
                seen_ids.add(event_id)
                payload = record.get("payload")
                if record.get("payload_sha256") != canonical_payload_sha256(payload):
                    raise ValueError("payload digest mismatch")
                claimed = record.pop("record_sha256", None)
                if claimed != canonical_payload_sha256(record):
                    raise ValueError("record digest mismatch")
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                errors.append(f"{path}: {type(exc).__name__}: {exc}")
        return LedgerVerification(not errors, count, len(errors), tuple(errors))
