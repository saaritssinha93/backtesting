"""Small, short-lived response cache for the read-only LIVE flow endpoint.

The market adapter already caches source files. This layer prevents simultaneous
HTTP clients from repeating the full session reconstruction before that cache
has warmed up. Responses are shared read-only; callers must not mutate them.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date as calendar_date
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable


MAX_ENTRIES = 4
MAX_TTL_SECONDS = 20.0
_LOCK = threading.Lock()
_RESPONSES: OrderedDict[tuple[str, str | None], "_Response"] = OrderedDict()
_INFLIGHT: dict[tuple[str, str | None], "_Flight"] = {}


@dataclass
class _Response:
    value: Any
    completed: float
    ttl: float


@dataclass
class _Flight:
    completed: threading.Event = field(default_factory=threading.Event)
    value: Any = None
    error: BaseException | None = None


def _date_key(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Invalid session date")
    calendar_date.fromisoformat(value)
    return value


def get_cached_live_flow(
    root: Path | str,
    date: str | None,
    loader: Callable[[Path, str | None], Any],
    ttl: float = 20,
) -> Any:
    """Load once per root/session, coalescing concurrent callers until complete.

    Successful results expire at most 20 seconds after completion, including
    historical sessions: saved evidence and the current clock can both change.
    Failed loads propagate to every waiting caller and are never cached. A
    failed refresh cannot return an older payload disguised as fresh data.
    """
    session = _date_key(date)
    duration = float(ttl)
    if not math.isfinite(duration) or duration < 0:
        raise ValueError("Cache TTL must be a finite non-negative number")
    duration = min(duration, MAX_TTL_SECONDS)
    directory = Path(root)
    key = (os.path.normcase(str(directory.resolve())), session)
    with _LOCK:
        now = time.monotonic()
        expired = [item for item, response in _RESPONSES.items() if now - response.completed >= response.ttl]
        for item in expired:
            del _RESPONSES[item]
        response = _RESPONSES.get(key)
        if response is not None and now - response.completed < min(duration, response.ttl):
            _RESPONSES.move_to_end(key)
            return response.value
        flight = _INFLIGHT.get(key)
        leader = flight is None
        if leader:
            flight = _Flight()
            _INFLIGHT[key] = flight
    if not leader:
        flight.completed.wait()
        if flight.error is not None:
            raise flight.error
        return flight.value
    try:
        value = loader(directory, session)
    except BaseException as exc:
        with _LOCK:
            # Explicitly remove an expired/shorter-TTL response if a refresh
            # fails. It must not reappear for a later caller using a longer TTL.
            _RESPONSES.pop(key, None)
            flight.error = exc
            _INFLIGHT.pop(key, None)
            flight.completed.set()
        raise
    with _LOCK:
        if duration > 0:
            _RESPONSES[key] = _Response(value, time.monotonic(), duration)
            _RESPONSES.move_to_end(key)
            while len(_RESPONSES) > MAX_ENTRIES:
                _RESPONSES.popitem(last=False)
        else:
            _RESPONSES.pop(key, None)
        flight.value = value
        _INFLIGHT.pop(key, None)
        flight.completed.set()
    return value
