from __future__ import annotations

import json
import os

import pytest

import fno_v12_resources as resources


def _measured(
    *,
    cpus: int = 8,
    total: int | None = 16 * resources.GIB,
    available: int | None = 10 * resources.GIB,
) -> dict[str, object]:
    return {
        "logical_cpu_count": cpus,
        "physical_memory_bytes": total,
        "available_memory_bytes": available,
        "physical_memory_gib": None if total is None else total / resources.GIB,
        "available_memory_gib": (
            None if available is None else available / resources.GIB
        ),
        "cpu_measurement_source": "test",
        "memory_measurement_source": "test",
        "platform": "test",
    }


def test_synthetic_16_cpu_13_6_gib_available_resolves_fourteen_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources, "detect_host_resources", lambda: _measured())
    plan = resources.plan_fresh_spawn_workers(
        logical_cpu_count=16,
        physical_memory_bytes=32 * resources.GIB,
        available_memory_bytes=int(13.6 * resources.GIB),
    )

    assert plan["recommended_workers"] == 14
    assert plan["limits"]["cpu_worker_limit"] == 14
    assert plan["limits"]["ram_worker_limit"] == 15
    assert plan["reason"].startswith("CPU-limited")
    assert plan["multiprocessing_start_method"] == "spawn"
    assert plan["fresh_process_per_task"] is True
    assert plan["max_tasks_per_child"] == 1
    json.dumps(plan)


def test_task_count_is_a_hard_worker_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources, "detect_host_resources", lambda: _measured())
    plan = resources.plan_fresh_spawn_workers(
        task_count=3,
        logical_cpu_count=16,
        physical_memory_bytes=32 * resources.GIB,
        available_memory_bytes=int(13.6 * resources.GIB),
        apply_thread_limits=False,
    )

    assert plan["recommended_workers"] == 3
    assert plan["limits"]["resource_worker_limit"] == 14
    assert plan["limits"]["task_worker_limit"] == 3
    assert plan["reason"] == "Task-limited to 3 runnable variant(s)"


def test_zero_tasks_requests_no_worker_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources, "detect_host_resources", lambda: _measured())
    plan = resources.plan_fresh_spawn_workers(
        task_count=0,
        apply_thread_limits=False,
    )

    assert plan["recommended_workers"] == 0


def test_ram_limit_and_low_resource_floor_are_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources, "detect_host_resources", lambda: _measured())
    ram_limited = resources.plan_fresh_spawn_workers(
        logical_cpu_count=32,
        physical_memory_bytes=8 * resources.GIB,
        available_memory_bytes=5 * resources.GIB,
        apply_thread_limits=False,
    )
    assert ram_limited["recommended_workers"] == 4
    assert ram_limited["limits"]["ram_limit_raw"] == 4
    assert ram_limited["reason"].startswith("RAM-limited")

    floor = resources.plan_fresh_spawn_workers(
        logical_cpu_count=1,
        physical_memory_bytes=resources.GIB,
        available_memory_bytes=resources.GIB,
        apply_thread_limits=False,
    )
    assert floor["recommended_workers"] == 1
    assert floor["limits"]["cpu_limit_raw"] == -1
    assert floor["limits"]["ram_limit_raw"] == 0


def test_missing_ram_measurement_fails_conservatively_to_one_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        resources,
        "detect_host_resources",
        lambda: _measured(cpus=24, total=None, available=None),
    )
    plan = resources.plan_fresh_spawn_workers(apply_thread_limits=False)

    assert plan["recommended_workers"] == 1
    assert plan["limits"]["ram_limit_raw"] is None
    assert "unavailable" in plan["reason"]


def test_single_thread_environment_is_forced_and_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources, "detect_host_resources", lambda: _measured())
    for index, name in enumerate(resources.THREAD_ENVIRONMENT_VARIABLES, start=2):
        monkeypatch.setenv(name, str(index))

    plan = resources.plan_fresh_spawn_workers()

    assert plan["thread_environment"] == {
        name: "1" for name in resources.THREAD_ENVIRONMENT_VARIABLES
    }
    for name in resources.THREAD_ENVIRONMENT_VARIABLES:
        assert os.environ[name] == "1"


def test_windows_native_memory_is_preferred_over_optional_psutil(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources.os, "cpu_count", lambda: 12)
    monkeypatch.setattr(
        resources,
        "_windows_memory_bytes",
        lambda: (32 * resources.GIB, 20 * resources.GIB),
    )

    def unexpected_psutil() -> tuple[None, None, None]:
        raise AssertionError("psutil fallback should not run")

    monkeypatch.setattr(resources, "_psutil_resources", unexpected_psutil)
    detected = resources.detect_host_resources()

    assert detected["logical_cpu_count"] == 12
    assert detected["physical_memory_bytes"] == 32 * resources.GIB
    assert detected["available_memory_bytes"] == 20 * resources.GIB
    assert detected["memory_measurement_source"] == (
        "windows_GlobalMemoryStatusEx"
    )


def test_psutil_fallback_supplies_cpu_and_memory_when_native_calls_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources.os, "cpu_count", lambda: None)
    monkeypatch.setattr(resources, "_windows_memory_bytes", lambda: None)
    monkeypatch.setattr(
        resources,
        "_psutil_resources",
        lambda: (10, 24 * resources.GIB, 11 * resources.GIB),
    )
    monkeypatch.setattr(resources, "_posix_memory_bytes", lambda: None)

    detected = resources.detect_host_resources()

    assert detected["logical_cpu_count"] == 10
    assert detected["physical_memory_bytes"] == 24 * resources.GIB
    assert detected["available_memory_bytes"] == 11 * resources.GIB
    assert detected["cpu_measurement_source"] == "psutil.cpu_count"
    assert detected["memory_measurement_source"] == "psutil.virtual_memory"


def test_cpu_and_memory_last_resorts_remain_json_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources.os, "cpu_count", lambda: None)
    monkeypatch.delenv("NUMBER_OF_PROCESSORS", raising=False)
    monkeypatch.setattr(resources, "_windows_memory_bytes", lambda: None)
    monkeypatch.setattr(
        resources, "_psutil_resources", lambda: (None, None, None)
    )
    monkeypatch.setattr(resources, "_posix_memory_bytes", lambda: None)

    detected = resources.detect_host_resources()

    assert detected["logical_cpu_count"] == 1
    assert detected["physical_memory_bytes"] is None
    assert detected["available_memory_bytes"] is None
    assert detected["cpu_measurement_source"] == "conservative_fallback_1"
    json.dumps(detected)


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"logical_cpu_count": 0}, "logical_cpu_count"),
        ({"physical_memory_bytes": 0}, "physical_memory_bytes"),
        ({"available_memory_bytes": -1}, "available_memory_bytes"),
        ({"cpu_headroom": -1}, "cpu_headroom"),
        ({"ram_reserve_bytes": -1}, "ram_reserve_bytes"),
        ({"per_worker_bytes": 0}, "per_worker_bytes"),
        (
            {
                "physical_memory_bytes": 4 * resources.GIB,
                "available_memory_bytes": 5 * resources.GIB,
            },
            "cannot exceed",
        ),
    ],
)
def test_invalid_resource_inputs_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, int],
    match: str,
) -> None:
    monkeypatch.setattr(resources, "detect_host_resources", lambda: _measured())
    with pytest.raises(ValueError, match=match):
        resources.plan_fresh_spawn_workers(
            apply_thread_limits=False,
            **kwargs,
        )


def test_named_configuration_front_door_delegates_to_the_same_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(resources, "detect_host_resources", lambda: _measured())
    direct = resources.plan_fresh_spawn_workers(apply_thread_limits=False)
    configured = resources.configure_maximum_practical_workers(
        apply_thread_limits=False
    )
    assert configured == direct
