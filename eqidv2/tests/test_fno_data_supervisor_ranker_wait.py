from dataclasses import replace
import os
from pathlib import Path

import pytest

from tools import fno_data_supervisor as supervisor


@pytest.fixture
def waiting_ranker(tmp_path, monkeypatch):
    config = supervisor.Config("ranker", root=tmp_path, runtime=tmp_path / "runtime")
    config.log.parent.mkdir()
    config.heartbeat.parent.mkdir(parents=True)
    payload = {
        "run_id": "current-run", "session": config.name,
        "state": "WAITING", "phase": "WAIT_FETCH_MARKER",
    }
    monkeypatch.setattr(supervisor.time, "sleep", lambda _: None)
    return config, payload


def write_heartbeat(config, payload, stamp=1700000000.0):
    config.heartbeat.write_text("".join(f"{key}={value}\n" for key, value in payload.items()))
    os.utime(config.heartbeat, (stamp, stamp))


def test_fresh_current_ranker_wait_is_not_killed_after_ten_minutes_without_log(waiting_ranker):
    config, payload = waiting_ranker
    write_heartbeat(config, payload)
    assert supervisor.worker_failure(config, "current-run", 1699999399.0, 1700000001.0) == ""


@pytest.mark.parametrize("field,value,expected", [
    ("run_id", "previous-run", "worker_run_id_mismatch"),
    ("session", "another_worker", "hung_idle"),
    ("state", "RUNNING", "hung_idle"),
    ("phase", "FETCH_SLOT", "hung_idle"),
    ("phase", "", "hung_idle"),
])
def test_log_idle_exemption_requires_exact_waiting_worker_identity(waiting_ranker, field, value, expected):
    config, payload = waiting_ranker
    payload[field] = value
    write_heartbeat(config, payload)
    assert supervisor.worker_failure(config, "current-run", 1699999399.0, 1700000001.0) == expected


def test_stale_waiting_heartbeat_cannot_suppress_restart(waiting_ranker):
    config, payload = waiting_ranker
    write_heartbeat(config, payload, stamp=1699999800.0)
    assert supervisor.worker_failure(config, "current-run", 1699999399.0, 1700000001.0) == "freshness_stale"


def test_waiting_exemption_retries_transient_partial_write_before_accepting(waiting_ranker, monkeypatch):
    config, payload = waiting_ranker
    write_heartbeat(config, payload)
    original_read = Path.read_text
    calls = []

    def read(path, *args, **kwargs):
        if path == config.heartbeat:
            calls.append(path)
            if len(calls) == 1:
                return "run_id=current-run\nstate=WAIT"
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    assert supervisor.worker_failure(config, "current-run", 1699999399.0, 1700000001.0) == ""
    assert len(calls) == 2


def test_cash_log_idle_behavior_is_unchanged(waiting_ranker):
    config, payload = waiting_ranker
    config = replace(config, service="cash5m")
    write_heartbeat(config, payload)
    assert supervisor.worker_failure(config, "current-run", 1699999200.0, 1700000001.0) == "hung_idle"
