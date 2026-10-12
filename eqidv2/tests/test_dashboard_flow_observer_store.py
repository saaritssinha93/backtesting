from pathlib import Path
import json

import pytest

from dashboard_flow_observer_store import append_events, observer_root, read_events, read_json, write_json

DAY = "2026-10-12"


def event(**changes):
    return {"session_date": DAY, "symbol": "TEST", "contract": "TEST26OCTFUT", "kind": "future", "exchange_timestamp": f"{DAY}T09:15:00+05:30", "received_at": f"{DAY}T09:15:00+05:30", "price": 100, "session_open": 99, "oi": 1000, "source": "KITE_WEBSOCKET_FULL", **changes}


def test_reads_create_nothing_and_writes_are_isolated(tmp_path):
    assert read_json(tmp_path, "config.json") is None
    assert read_events(tmp_path, DAY) == []
    assert not observer_root(tmp_path).exists()
    assert append_events(tmp_path, DAY, [event()]) == 1
    write_json(tmp_path, "config.json", {"opening_enabled": True})
    assert read_json(tmp_path, "config.json") == {"opening_enabled": True}
    assert read_events(tmp_path, DAY) == [event()]
    assert [path.name for path in tmp_path.iterdir()] == ["dashboard_flow_observer"]


@pytest.mark.parametrize("relative", ["../production.json", "raw/../../production.json", "/production.json", "C:/production.json", "raw/file:stream", "."])
def test_paths_cannot_escape_observer_directory(tmp_path, relative):
    with pytest.raises(ValueError):
        write_json(tmp_path, relative, {})
    with pytest.raises(ValueError):
        read_json(tmp_path, relative)


@pytest.mark.parametrize("changes", [{"session_date": "2026-10-09"}, {"symbol": "../TEST"}, {"kind": "order"}, {"oi": -1}, {"price": 0}, {"oi": float("nan")}, {"oi": True}, {"exchange_timestamp": f"{DAY}T09:15:00"}, {"received_at": f"{DAY}T09:14:59+05:30"}, {"source": ""}])
def test_invalid_batch_writes_nothing(tmp_path, changes):
    with pytest.raises(ValueError):
        append_events(tmp_path, DAY, [event(), event(**changes)])
    assert not observer_root(tmp_path).exists()


def test_partial_or_invalid_records_are_not_observations(tmp_path):
    append_events(tmp_path, DAY, [event()])
    path = observer_root(tmp_path) / "raw" / DAY / "events.jsonl"
    with path.open("ab") as handle:
        handle.write(b'{"partial":')
    assert len(read_events(tmp_path, DAY)) == 1
    append_events(tmp_path, DAY, [event(oi=1100)])
    assert [row["oi"] for row in read_events(tmp_path, DAY)] == [1000, 1100]


def test_atomic_write_preserves_old_value_on_invalid_json(tmp_path):
    write_json(tmp_path, "status/session.json", {"ok": True})
    with pytest.raises(ValueError):
        write_json(tmp_path, "status/session.json", {"bad": float("inf")})
    assert read_json(tmp_path, "status/session.json") == {"ok": True}
    assert not list(observer_root(tmp_path).rglob("*.tmp"))


def test_symlink_directory_cannot_redirect_writes(tmp_path):
    outside = tmp_path / "production"
    outside.mkdir()
    root = observer_root(tmp_path)
    root.mkdir()
    try:
        (root / "raw").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Windows symlink privilege unavailable")
    with pytest.raises(ValueError):
        append_events(tmp_path, DAY, [event()])
    assert list(outside.iterdir()) == []
