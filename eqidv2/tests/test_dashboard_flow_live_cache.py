from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import threading
from unittest.mock import Mock

import pytest

import dashboard_flow_live_cache as cache


@pytest.fixture(autouse=True)
def empty_cache(monkeypatch):
    monkeypatch.setattr(cache, "_RESPONSES", OrderedDict())
    monkeypatch.setattr(cache, "_INFLIGHT", {})


def test_simultaneous_same_date_requests_share_one_heavy_load(tmp_path):
    started = threading.Event()
    release = threading.Event()
    count = 0
    lock = threading.Lock()

    def loader(root, day):
        nonlocal count
        with lock:
            count += 1
        started.set()
        assert release.wait(5)
        return {"date": day, "count": count}

    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = [pool.submit(cache.get_cached_live_flow, tmp_path, "2026-10-12", loader) for _ in range(8)]
        assert started.wait(5)
        release.set()
        values = [job.result(timeout=5) for job in jobs]
    assert count == 1
    assert all(value == {"date": "2026-10-12", "count": 1} for value in values)


def test_expiration_uses_completion_time_and_refreshes_once(tmp_path, monkeypatch):
    tick = [0.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: tick[0])
    calls = []

    def loader(root, day):
        calls.append(day)
        tick[0] += 40  # Slow initial load must still be reusable when it finishes.
        return {"generation": len(calls)}

    assert cache.get_cached_live_flow(tmp_path, None, loader) == {"generation": 1}
    tick[0] = 59.9
    assert cache.get_cached_live_flow(tmp_path, None, loader) == {"generation": 1}
    tick[0] = 60
    assert cache.get_cached_live_flow(tmp_path, None, loader) == {"generation": 2}


def test_ttl_cannot_leave_clock_or_evidence_stale_for_more_than_twenty_seconds(tmp_path, monkeypatch):
    tick = [100.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: tick[0])
    loader = Mock(side_effect=[{"generation": 1}, {"generation": 2}])
    assert cache.get_cached_live_flow(tmp_path, None, loader, ttl=900)["generation"] == 1
    tick[0] += 20
    assert cache.get_cached_live_flow(tmp_path, None, loader, ttl=900)["generation"] == 2


def test_failure_is_shared_and_not_cached_or_replaced_by_an_old_success(tmp_path, monkeypatch):
    tick = [1.0]
    monkeypatch.setattr(cache.time, "monotonic", lambda: tick[0])
    assert cache.get_cached_live_flow(tmp_path, None, lambda *_: {"old": True}) == {"old": True}
    tick[0] += 21
    entered = threading.Event()
    release = threading.Event()
    calls = 0

    def failing(*args):
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(5)
        raise ValueError("source read failed")

    # Make the waiter observable without relying on scheduler timing or sleeps.
    waiter_entered = threading.Event()
    original_wait = threading.Event.wait
    target_event = [None]

    def tracked_wait(event, timeout=None):
        if event is target_event[0]:
            waiter_entered.set()
        return original_wait(event, timeout)

    monkeypatch.setattr(threading.Event, "wait", tracked_wait)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(cache.get_cached_live_flow, tmp_path, None, failing)
        assert entered.wait(5)
        target_event[0] = next(iter(cache._INFLIGHT.values())).completed
        second = pool.submit(cache.get_cached_live_flow, tmp_path, None, failing)
        assert waiter_entered.wait(5)
        release.set()
        for future in (first, second):
            with pytest.raises(ValueError, match="source read failed"):
                future.result(timeout=5)
    assert calls == 1
    fresh = cache.get_cached_live_flow(tmp_path, None, lambda *_: {"fresh": True})
    assert fresh == {"fresh": True}


def test_cache_is_bounded_and_least_recently_used_date_is_reloaded(tmp_path):
    calls = []
    def loader(root, day):
        calls.append(day)
        return {"date": day}

    for number in range(1, 5):
        cache.get_cached_live_flow(tmp_path, f"2026-10-0{number}", loader)
    cache.get_cached_live_flow(tmp_path, "2026-10-01", loader)
    cache.get_cached_live_flow(tmp_path, "2026-10-05", loader)
    cache.get_cached_live_flow(tmp_path, "2026-10-01", loader)
    assert len(calls) == 5
    cache.get_cached_live_flow(tmp_path, "2026-10-02", loader)
    assert len(calls) == 6
    assert len(cache._RESPONSES) == 4


def test_distinct_roots_and_dates_do_not_share_data(tmp_path):
    loader = Mock(side_effect=[{"value": 1}, {"value": 2}, {"value": 3}])
    assert cache.get_cached_live_flow(tmp_path / "a", "2026-10-12", loader)["value"] == 1
    assert cache.get_cached_live_flow(tmp_path / "b", "2026-10-12", loader)["value"] == 2
    assert cache.get_cached_live_flow(tmp_path / "a", "2026-10-09", loader)["value"] == 3


def test_disabled_cache_does_not_resurrect_an_older_response(tmp_path):
    loader = Mock(side_effect=[{"generation": 1}, {"generation": 2}, {"generation": 3}])
    assert cache.get_cached_live_flow(tmp_path, None, loader)["generation"] == 1
    assert cache.get_cached_live_flow(tmp_path, None, loader, ttl=0)["generation"] == 2
    assert cache.get_cached_live_flow(tmp_path, None, loader)["generation"] == 3


@pytest.mark.parametrize("date", ["../2026-10-12", "2026-02-30", "2026-1-2", "2026-10-12x"])
def test_invalid_date_rejected_without_loading(tmp_path, date):
    loader = Mock()
    with pytest.raises(ValueError):
        cache.get_cached_live_flow(tmp_path, date, loader)
    loader.assert_not_called()


@pytest.mark.parametrize("ttl", [-1, float("nan"), float("inf")])
def test_invalid_ttl_rejected_without_loading(tmp_path, ttl):
    loader = Mock()
    with pytest.raises(ValueError):
        cache.get_cached_live_flow(tmp_path, None, loader, ttl=ttl)
    loader.assert_not_called()
