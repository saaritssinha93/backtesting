"""Isolated storage for Dashboard Flow observations; never writes production data.

Public writers accept a production data root, but always resolve beneath its
``dashboard_flow_observer`` child. Read helpers never create directories. A
single collector owns each raw session file; incomplete trailing JSONL writes
are ignored by readers and surfaced by the adapter's source audit.
"""
from __future__ import annotations

from datetime import date, datetime
import json
import math
import os
from pathlib import Path
import re
import tempfile
import threading
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
_WRITE_LOCK = threading.Lock()
_IDENTITY = re.compile(r"[A-Z0-9&_.-]{1,100}\Z")


def session_key(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Session date must use YYYY-MM-DD")
    return date.fromisoformat(value).isoformat()


def observer_root(data_root: Path | str) -> Path:
    root = Path(data_root).resolve()
    candidate = (root / "dashboard_flow_observer").resolve()
    if candidate.parent != root:
        raise ValueError("Observer directory must remain inside the data root")
    return candidate


def _path(data_root: Path | str, relative_path: Path | str) -> Path:
    root = observer_root(data_root)
    relative = Path(relative_path)
    if relative.is_absolute() or relative.drive or any(part in {"..", ""} for part in relative.parts):
        raise ValueError("Observer path must be a relative path without traversal")
    # Reject alternate data streams on Windows, even when checked on POSIX.
    if ":" in str(relative) or "\\" in str(relative).replace(os.sep, ""):
        raise ValueError("Invalid observer path")
    target = (root / relative).resolve()
    if target == root or not target.is_relative_to(root):
        raise ValueError("Observer path escapes its isolated directory")
    return target


def read_json(data_root: Path | str, relative_path: Path | str):
    path = _path(data_root, relative_path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None


def write_json(data_root: Path | str, relative_path: Path | str, payload) -> Path:
    """Atomically write JSON inside the isolated directory, rejecting NaN/Inf."""
    encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
    target = _path(data_root, relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Recheck after mkdir to reject existing junctions/symlinks in a parent.
    target = _path(data_root, relative_path)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return target


def _timestamp(value: str, day: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Observation timestamps must be timezone-aware ISO strings")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.astimezone(IST).date().isoformat() != day:
        raise ValueError("Observation timestamp must have a timezone and match its session")
    return stamp.astimezone(IST)


def validate_event(raw: dict, day: str) -> dict:
    day = session_key(day)
    if not isinstance(raw, dict) or raw.get("session_date") != day:
        raise ValueError("Observation session mismatch")
    row = dict(raw)
    for name in ("symbol", "contract"):
        if name == "contract" and row.get("kind") == "cash" and not row.get(name):
            row[name] = ""
            continue
        if not isinstance(row.get(name), str) or not _IDENTITY.fullmatch(row[name]):
            raise ValueError(f"Invalid observation {name}")
    if row.get("kind") not in {"cash", "future"}:
        raise ValueError("Observation kind must be cash or future")
    for name in ("exchange_timestamp", "received_at"):
        row[name] = _timestamp(row.get(name), day).isoformat()
    if datetime.fromisoformat(row["exchange_timestamp"]) > datetime.fromisoformat(row["received_at"]):
        raise ValueError("Observation exchange timestamp exceeds receipt time")
    for name in ("price", "session_open", "oi"):
        value = row.get(name)
        if value is None:
            row[name] = None
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (name != "oi" and value == 0):
            raise ValueError(f"Invalid observation {name}")
    if not isinstance(row.get("source"), str) or not row["source"].strip():
        raise ValueError("Observation source is required")
    return row


def append_events(data_root: Path | str, session_date: str, records) -> int:
    """Validate the entire batch before appending; only one collector may own it."""
    day = session_key(session_date)
    rows = [validate_event(record, day) for record in records]
    if not rows:
        return 0
    encoded = "".join(json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n" for row in rows)
    relative = Path("raw") / day / "events.jsonl"
    target = _path(data_root, relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    with _WRITE_LOCK:
        target = _path(data_root, relative)
        # End a partial line left by a crashed process before adding new records.
        with target.open("ab+") as handle:
            handle.seek(0, os.SEEK_END)
            if handle.tell():
                handle.seek(-1, os.SEEK_END)
                if handle.read(1) != b"\n":
                    handle.write(b"\n")
            handle.write(encoded.encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
    return len(rows)


def read_events_with_audit(data_root: Path | str, session_date: str) -> tuple[list[dict], dict]:
    """Read complete validated observations and count ignored source damage."""
    day = session_key(session_date)
    target = _path(data_root, Path("raw") / day / "events.jsonl")
    try:
        raw = target.read_bytes()
    except FileNotFoundError:
        return [], {"valid_records": 0, "invalid_records": 0, "partial_records": 0}
    rows = []
    audit = {"valid_records": 0, "invalid_records": 0, "partial_records": 0}
    for line in raw.splitlines(keepends=True):
        if not line.endswith(b"\n"):
            audit["partial_records"] += 1
            continue
        try:
            rows.append(validate_event(json.loads(line), day))
        except (UnicodeError, ValueError, TypeError):
            audit["invalid_records"] += 1
            continue
    audit["valid_records"] = len(rows)
    return rows, audit


def read_events(data_root: Path | str, session_date: str) -> list[dict]:
    """Return valid complete observations; never manufacture a partial record."""
    return read_events_with_audit(data_root, session_date)[0]
