"""Small process-local TTL cache for read-only summaries."""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Hashable


class TTLCache:
    def __init__(self, ttl_seconds: float):
        self.ttl_seconds = ttl_seconds
        self._items: dict[Hashable, tuple[float, Any]] = {}
        self._lock = threading.RLock()

    def get_or_create(self, key: Hashable, factory: Callable[[], Any]) -> Any:
        now = time.monotonic()
        with self._lock:
            existing = self._items.get(key)
            if existing is not None and existing[0] >= now:
                return existing[1]
        value = factory()
        with self._lock:
            self._items[key] = (now + self.ttl_seconds, value)
        return value
