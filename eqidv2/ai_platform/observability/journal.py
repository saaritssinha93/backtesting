"""Append-only, hash-chained event journal for durable operational evidence."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Mapping

from .context import CorrelationContext, current_context
from .redaction import redact, safe_json_dumps


_ZERO_HASH = "0" * 64


@dataclass(frozen=True, slots=True)
class JournalVerification:
    valid: bool
    entries: int
    last_hash: str
    error_line: int | None = None
    error: str | None = None


class AppendOnlyEventJournal:
    """Write one canonical JSON event per line with a SHA-256 hash chain.

    ``append`` is fail-open by default.  Set ``strict=True`` in tests or
    offline tooling when a write failure should be raised.  A sidecar advisory
    lock protects sequence/hash assignment across cooperating processes.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        service: str,
        fsync: bool = False,
        strict: bool = False,
        max_event_bytes: int = 256 * 1024,
        on_drop: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        self.service = service
        self.fsync = fsync
        self.strict = strict
        self.max_event_bytes = max_event_bytes
        self.on_drop = on_drop
        self._thread_lock = threading.RLock()
        self.dropped_events = 0
        self.last_error: str | None = None

    @contextmanager
    def _process_lock(self) -> Iterator[None]:
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as lock_file:
            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write(b"0")
                lock_file.flush()
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                # Never wait behind telemetry I/O on a trading thread.  The
                # caller records a dropped event if another writer owns it.
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                try:
                    yield
                finally:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _last_entry(self) -> tuple[int, str]:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return 0, _ZERO_HASH
        size = self.path.stat().st_size
        with self.path.open("rb") as handle:
            handle.seek(max(0, size - (self.max_event_bytes * 2)))
            chunk = handle.read()
        lines = [line for line in chunk.splitlines() if line.strip()]
        if not lines:
            return 0, _ZERO_HASH
        try:
            last = json.loads(lines[-1].decode("utf-8"))
            return int(last["sequence"]), str(last["event_hash"])
        except Exception as exc:
            raise ValueError("The event journal has an invalid final record") from exc

    @staticmethod
    def _hash(entry_without_hash: Mapping[str, object]) -> str:
        canonical = safe_json_dumps(entry_without_hash).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def append(
        self,
        event_type: str,
        data: Mapping[str, object] | None = None,
        *,
        severity: str = "INFO",
        context: CorrelationContext | Mapping[str, object] | None = None,
        timestamp: datetime | None = None,
    ) -> dict[str, object] | None:
        """Append an event and return the written record, or ``None`` on drop."""

        try:
            if not event_type or len(event_type) > 128:
                raise ValueError("event_type must contain 1 to 128 characters")
            if context is None:
                event_context = current_context().as_dict()
            elif isinstance(context, CorrelationContext):
                event_context = context.as_dict()
            else:
                event_context = CorrelationContext.from_mapping(context).as_dict()
            event_context.setdefault("service", self.service)
            occurred = timestamp or datetime.now(timezone.utc)
            if occurred.tzinfo is None:
                occurred = occurred.replace(tzinfo=timezone.utc)

            with self._thread_lock, self._process_lock():
                self.path.parent.mkdir(parents=True, exist_ok=True)
                previous_sequence, previous_hash = self._last_entry()
                entry: dict[str, object] = {
                    "schema_version": "eqidv2_observability_event_v1",
                    "sequence": previous_sequence + 1,
                    "event_id": uuid.uuid4().hex,
                    "timestamp_utc": occurred.astimezone(timezone.utc)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z"),
                    "event_type": event_type,
                    "severity": severity.upper()[:16],
                    "context": redact(event_context),
                    "data": redact(dict(data or {})),
                    "previous_hash": previous_hash,
                }
                entry["event_hash"] = self._hash(entry)
                encoded = (safe_json_dumps(entry) + "\n").encode("utf-8")
                if len(encoded) > self.max_event_bytes:
                    raise ValueError(
                        f"Journal event exceeds {self.max_event_bytes} encoded bytes"
                    )
                descriptor = os.open(
                    self.path,
                    os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0),
                    0o600,
                )
                try:
                    written = os.write(descriptor, encoded)
                    if written != len(encoded):
                        raise OSError(f"Short journal write: {written}/{len(encoded)} bytes")
                    if self.fsync:
                        os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            self.last_error = None
            return entry
        except Exception as exc:
            self.dropped_events += 1
            self.last_error = type(exc).__name__
            if self.on_drop is not None:
                try:
                    self.on_drop(type(exc).__name__)
                except Exception:
                    pass
            if self.strict:
                raise
            return None

    def verify(self) -> JournalVerification:
        """Validate sequence and hash-chain integrity without changing the file."""

        previous_hash = _ZERO_HASH
        expected_sequence = 1
        if not self.path.exists():
            return JournalVerification(True, 0, previous_hash)
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        event_hash = str(entry.pop("event_hash"))
                        if int(entry.get("sequence", -1)) != expected_sequence:
                            raise ValueError("non-contiguous sequence")
                        if entry.get("previous_hash") != previous_hash:
                            raise ValueError("previous hash mismatch")
                        if self._hash(entry) != event_hash:
                            raise ValueError("event hash mismatch")
                    except Exception as exc:
                        return JournalVerification(
                            False,
                            expected_sequence - 1,
                            previous_hash,
                            line_number,
                            str(exc),
                        )
                    previous_hash = event_hash
                    expected_sequence += 1
        except Exception as exc:
            return JournalVerification(
                False, expected_sequence - 1, previous_hash, None, type(exc).__name__
            )
        return JournalVerification(True, expected_sequence - 1, previous_hash)


__all__ = ["AppendOnlyEventJournal", "JournalVerification"]
