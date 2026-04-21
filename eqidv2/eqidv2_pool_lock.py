"""
strategy_v2 §C3 — cross-process pool-JSON lock.

SE, PF and DE all mutate the same `pending_signals_<date>_v16_5min.json`
from independent processes (different supervisors, different Python
interpreters). Each process writes atomically via os.replace, but between
`_load_pending_state` and `_write_pending_state_atomic` there is a TOCTOU
window where a concurrent writer's update is silently lost.

This module exposes:

- ``pool_lock(date_str)``: advisory cross-process lock, one lockfile per
  date, used by SE / PF / DE around every read-modify-write sequence.
  Windows → msvcrt.locking byte-range locks; POSIX → fcntl.flock.
- ``bump_pool_rev(state)``: monotonic revision counter embedded in the
  JSON. Primarily observability — lets dashboards / replay tools detect
  state they have already processed without re-parsing the full payload.

Every write path MUST happen inside ``with pool_lock(date_str):`` and MUST
call ``bump_pool_rev(state)`` before writing. Plain reads may take the
lock briefly (safe snapshot) or skip it when the caller is tolerant of
stale views.
"""
from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator

from eqidv2_runtime_paths import LIVE_SIGNALS_DIR

_IS_WINDOWS = sys.platform.startswith("win")

if _IS_WINDOWS:
    import msvcrt
else:  # pragma: no cover - production is Windows; POSIX path for replay/dev
    import fcntl  # type: ignore[import-not-found]

POOL_REV_FIELD = "pool_rev"
_LOCK_TIMEOUT_DEFAULT = float(os.getenv("EQIDV2_POOL_LOCK_TIMEOUT_SEC", "30"))
_LOCK_POLL_SEC = max(0.01, float(os.getenv("EQIDV2_POOL_LOCK_POLL_SEC", "0.05")))


def _lock_path(date_str: str) -> Path:
    return Path(LIVE_SIGNALS_DIR) / f"pending_signals_{date_str}_v16_5min.lock"


@contextmanager
def pool_lock(date_str: str, timeout: float = _LOCK_TIMEOUT_DEFAULT) -> Iterator[None]:
    """
    Exclusive advisory lock for the pool JSON of `date_str`.

    Blocks up to `timeout` seconds, then raises TimeoutError. Callers must
    treat a timeout as a hard failure (skip the slot, surface in logs) —
    swallowing it reintroduces the race this lock exists to prevent.
    """
    lock_path = _lock_path(date_str)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+b")
    start = time.monotonic()
    acquired = False
    try:
        while True:
            try:
                if _IS_WINDOWS:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except (BlockingIOError, OSError):
                if time.monotonic() - start > timeout:
                    raise TimeoutError(
                        f"pool_lock: could not acquire {lock_path} after {timeout:.1f}s"
                    )
                time.sleep(_LOCK_POLL_SEC)
        yield
    finally:
        if acquired:
            try:
                if _IS_WINDOWS:
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        try:
            fh.close()
        except Exception:
            pass


def load_pool_rev(state: Dict[str, Any]) -> int:
    try:
        return int(state.get(POOL_REV_FIELD, 0) or 0)
    except (TypeError, ValueError):
        return 0


def bump_pool_rev(state: Dict[str, Any]) -> int:
    new_rev = load_pool_rev(state) + 1
    state[POOL_REV_FIELD] = new_rev
    return new_rev
