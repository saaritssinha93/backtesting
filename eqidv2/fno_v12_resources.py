"""Host resource detection and fresh-spawn worker planning for FNO V12.

The V12 replay mutates process-local engine seams, so variants must run in
fresh spawned processes rather than threads or a reused process pool.  This
module selects the largest practical worker count while retaining two logical
CPUs for the host and two GiB of available RAM.  Each worker is budgeted at
768 MiB and native numerical-library thread pools are forced to one thread so
that process parallelism does not multiply into hidden BLAS/OpenMP threads.

``psutil`` is optional.  On Windows, physical and available memory are read
first through ``GlobalMemoryStatusEx``; psutil is used only if the native call
is unavailable or fails.
"""

from __future__ import annotations

import ctypes
import json
import math
import os
from typing import Any, Mapping


RESOURCE_PLAN_SCHEMA_VERSION = "fno_v12_fresh_spawn_resource_plan_v1"

MIB = 1024**2
GIB = 1024**3
DEFAULT_CPU_HEADROOM = 2
DEFAULT_RAM_RESERVE_BYTES = 2 * GIB
DEFAULT_PER_WORKER_BYTES = 768 * MIB

THREAD_ENVIRONMENT_VARIABLES = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = (
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    )


def _is_windows() -> bool:
    return os.name == "nt"


def _windows_memory_bytes() -> tuple[int, int] | None:
    """Return (total physical, available physical) via the Windows API."""

    if not _is_windows():
        return None
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        function = kernel32.GlobalMemoryStatusEx
        function.argtypes = [ctypes.POINTER(_MemoryStatusEx)]
        function.restype = ctypes.c_int
        status = _MemoryStatusEx()
        status.dwLength = ctypes.sizeof(_MemoryStatusEx)
        if not function(ctypes.byref(status)):
            return None
        total = int(status.ullTotalPhys)
        available = int(status.ullAvailPhys)
    except (AttributeError, ctypes.ArgumentError, OSError, TypeError, ValueError):
        return None
    if total <= 0 or available < 0 or available > total:
        return None
    return total, available


def _psutil_resources() -> tuple[int | None, int | None, int | None]:
    """Read optional psutil CPU/VM data without creating a hard dependency."""

    try:
        import psutil  # type: ignore[import-not-found]
    except (ImportError, OSError):
        return None, None, None
    try:
        logical = psutil.cpu_count(logical=True)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        logical = None
    try:
        memory = psutil.virtual_memory()
        total = int(memory.total)
        available = int(memory.available)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        total = None
        available = None
    return (
        int(logical) if logical is not None and int(logical) > 0 else None,
        total if total is not None and total > 0 else None,
        (
            available
            if available is not None
            and available >= 0
            and (total is None or available <= total)
            else None
        ),
    )


def _posix_memory_bytes() -> tuple[int, int] | None:
    """Last-resort physical-page fallback for non-Windows hosts."""

    if _is_windows() or not hasattr(os, "sysconf"):
        return None
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        total_pages = int(os.sysconf("SC_PHYS_PAGES"))
        available_pages = int(os.sysconf("SC_AVPHYS_PAGES"))
    except (OSError, TypeError, ValueError):
        return None
    total = page_size * total_pages
    available = page_size * available_pages
    if page_size <= 0 or total <= 0 or available < 0 or available > total:
        return None
    return total, available


def _positive_cpu_count(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def detect_host_resources() -> dict[str, Any]:
    """Return JSON-safe logical CPU and physical/available RAM measurements."""

    psutil_logical: int | None = None
    psutil_total: int | None = None
    psutil_available: int | None = None

    logical = _positive_cpu_count(os.cpu_count())
    cpu_source = "os.cpu_count"
    if logical is None:
        psutil_logical, psutil_total, psutil_available = _psutil_resources()
        logical = psutil_logical
        cpu_source = "psutil.cpu_count"
    if logical is None:
        logical = _positive_cpu_count(os.environ.get("NUMBER_OF_PROCESSORS"))
        cpu_source = "NUMBER_OF_PROCESSORS"
    if logical is None:
        logical = 1
        cpu_source = "conservative_fallback_1"

    memory = _windows_memory_bytes()
    memory_source = "windows_GlobalMemoryStatusEx"
    if memory is None:
        if psutil_total is None or psutil_available is None:
            _, psutil_total, psutil_available = _psutil_resources()
        if psutil_total is not None and psutil_available is not None:
            memory = (psutil_total, psutil_available)
            memory_source = "psutil.virtual_memory"
    if memory is None:
        memory = _posix_memory_bytes()
        memory_source = "os.sysconf_physical_pages"

    if memory is None:
        total_memory: int | None = None
        available_memory: int | None = None
        memory_source = "unavailable_conservative_worker_1"
    else:
        total_memory, available_memory = memory

    return {
        "logical_cpu_count": int(logical),
        "physical_memory_bytes": total_memory,
        "available_memory_bytes": available_memory,
        "physical_memory_gib": (
            None if total_memory is None else float(total_memory) / GIB
        ),
        "available_memory_gib": (
            None if available_memory is None else float(available_memory) / GIB
        ),
        "cpu_measurement_source": cpu_source,
        "memory_measurement_source": memory_source,
        "platform": os.name,
    }


def apply_single_thread_environment() -> dict[str, str]:
    """Force numerical libraries to one native thread per worker process."""

    applied = {name: "1" for name in THREAD_ENVIRONMENT_VARIABLES}
    for name, value in applied.items():
        os.environ[name] = value
    return applied


def _required_positive_integer(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


def _required_nonnegative_integer(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return int(value)


def _resolved_hardware(
    measured: Mapping[str, Any],
    *,
    logical_cpu_count: int | None,
    physical_memory_bytes: int | None,
    available_memory_bytes: int | None,
) -> dict[str, Any]:
    logical = (
        measured["logical_cpu_count"]
        if logical_cpu_count is None
        else logical_cpu_count
    )
    total = (
        measured["physical_memory_bytes"]
        if physical_memory_bytes is None
        else physical_memory_bytes
    )
    available = (
        measured["available_memory_bytes"]
        if available_memory_bytes is None
        else available_memory_bytes
    )

    logical = _required_positive_integer("logical_cpu_count", logical)
    if total is not None:
        total = _required_positive_integer("physical_memory_bytes", total)
    if available is not None:
        available = _required_nonnegative_integer(
            "available_memory_bytes", available
        )
    if total is not None and available is not None and available > total:
        raise ValueError("available_memory_bytes cannot exceed physical memory")

    return {
        "logical_cpu_count": logical,
        "physical_memory_bytes": total,
        "available_memory_bytes": available,
        "physical_memory_gib": None if total is None else total / GIB,
        "available_memory_gib": None if available is None else available / GIB,
        "cpu_measurement_source": (
            measured.get("cpu_measurement_source")
            if logical_cpu_count is None
            else "explicit_override"
        ),
        "memory_measurement_source": (
            measured.get("memory_measurement_source")
            if physical_memory_bytes is None and available_memory_bytes is None
            else "explicit_override"
        ),
        "platform": measured.get("platform", os.name),
    }


def plan_fresh_spawn_workers(
    *,
    task_count: int | None = None,
    logical_cpu_count: int | None = None,
    physical_memory_bytes: int | None = None,
    available_memory_bytes: int | None = None,
    cpu_headroom: int = DEFAULT_CPU_HEADROOM,
    ram_reserve_bytes: int = DEFAULT_RAM_RESERVE_BYTES,
    per_worker_bytes: int = DEFAULT_PER_WORKER_BYTES,
    apply_thread_limits: bool = True,
) -> dict[str, Any]:
    """Measure/configure the host and return the maximum practical plan."""

    cpu_headroom = _required_nonnegative_integer("cpu_headroom", cpu_headroom)
    ram_reserve_bytes = _required_nonnegative_integer(
        "ram_reserve_bytes", ram_reserve_bytes
    )
    per_worker_bytes = _required_positive_integer(
        "per_worker_bytes", per_worker_bytes
    )
    if task_count is not None:
        task_count = _required_nonnegative_integer("task_count", task_count)

    measured = detect_host_resources()
    hardware = _resolved_hardware(
        measured,
        logical_cpu_count=logical_cpu_count,
        physical_memory_bytes=physical_memory_bytes,
        available_memory_bytes=available_memory_bytes,
    )
    logical = int(hardware["logical_cpu_count"])
    available = hardware["available_memory_bytes"]

    cpu_limit_raw = logical - cpu_headroom
    cpu_worker_limit = max(1, cpu_limit_raw)
    if available is None:
        available_after_reserve = None
        ram_limit_raw = None
        ram_worker_limit = 1
        reason = (
            "RAM measurement unavailable; conservatively limited to one "
            "fresh spawned worker"
        )
    else:
        available_after_reserve = max(0, int(available) - ram_reserve_bytes)
        ram_limit_raw = math.floor(available_after_reserve / per_worker_bytes)
        ram_worker_limit = max(1, ram_limit_raw)
        if cpu_worker_limit < ram_worker_limit:
            reason = (
                "CPU-limited after retaining "
                f"{cpu_headroom} logical CPUs for host responsiveness"
            )
        elif ram_worker_limit < cpu_worker_limit:
            reason = (
                "RAM-limited after reserving "
                f"{ram_reserve_bytes} bytes and budgeting "
                f"{per_worker_bytes} bytes per worker"
            )
        else:
            reason = "CPU and available-RAM limits are equal"

    resource_worker_limit = max(1, min(cpu_worker_limit, ram_worker_limit))
    recommended_workers = (
        resource_worker_limit
        if task_count is None
        else min(resource_worker_limit, task_count)
    )
    if task_count is not None and task_count < resource_worker_limit:
        reason = f"Task-limited to {task_count} runnable variant(s)"
    thread_environment = (
        apply_single_thread_environment()
        if apply_thread_limits
        else {name: os.environ.get(name, "") for name in THREAD_ENVIRONMENT_VARIABLES}
    )

    plan: dict[str, Any] = {
        "schema_version": RESOURCE_PLAN_SCHEMA_VERSION,
        "recommended_workers": int(recommended_workers),
        "multiprocessing_start_method": "spawn",
        "fresh_process_per_task": True,
        "max_tasks_per_child": 1,
        "measured_hardware": hardware,
        "constants": {
            "cpu_headroom": cpu_headroom,
            "ram_reserve_bytes": ram_reserve_bytes,
            "ram_reserve_gib": ram_reserve_bytes / GIB,
            "per_worker_bytes": per_worker_bytes,
            "per_worker_mib": per_worker_bytes / MIB,
        },
        "limits": {
            "cpu_limit_raw": cpu_limit_raw,
            "cpu_worker_limit": cpu_worker_limit,
            "available_after_reserve_bytes": available_after_reserve,
            "ram_limit_raw": ram_limit_raw,
            "ram_worker_limit": ram_worker_limit,
            "task_count": task_count,
            "task_worker_limit": task_count,
            "resource_worker_limit": resource_worker_limit,
        },
        "formula": (
            "min(task_count_if_supplied, "
            "max(1, min(max(1, logical_cpu_count - cpu_headroom), "
            "max(1, floor(max(0, available_memory_bytes - "
            "ram_reserve_bytes) / per_worker_bytes)))))"
        ),
        "reason": reason,
        "thread_environment": thread_environment,
    }
    # Fail here rather than after a worker pool starts if a future edit adds a
    # non-serializable object to the orchestration contract.
    json.dumps(plan, sort_keys=True)
    return plan


def configure_maximum_practical_workers(**kwargs: Any) -> dict[str, Any]:
    """Named orchestration front door; delegates to the deterministic planner."""

    return plan_fresh_spawn_workers(**kwargs)


__all__ = [
    "DEFAULT_CPU_HEADROOM",
    "DEFAULT_PER_WORKER_BYTES",
    "DEFAULT_RAM_RESERVE_BYTES",
    "GIB",
    "MIB",
    "RESOURCE_PLAN_SCHEMA_VERSION",
    "THREAD_ENVIRONMENT_VARIABLES",
    "apply_single_thread_environment",
    "configure_maximum_practical_workers",
    "detect_host_resources",
    "plan_fresh_spawn_workers",
]
