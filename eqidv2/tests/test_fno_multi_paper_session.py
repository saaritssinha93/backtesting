from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import fno_multi_paper_live_source as real_source
import fno_multi_paper_session as session


IST = ZoneInfo("Asia/Kolkata")
DAY = date(2026, 9, 1)


def paths(tmp_path: Path) -> session.SessionPaths:
    return session.SessionPaths(
        DAY,
        root=tmp_path / "root",
        latest_root=tmp_path / "latest",
        status_path=tmp_path / "status.json",
        heartbeat_path=tmp_path / "heartbeat.json",
        lock_path=tmp_path / "writer.lock",
    )


def test_atomic_publish_retries_transient_windows_share_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "status.json"
    real_replace = session.os.replace
    attempts = 0

    def transient_lock(source: Path, target: Path) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise PermissionError(5, "Access is denied")
        real_replace(source, target)

    monkeypatch.setattr(session.os, "replace", transient_lock)
    monkeypatch.setattr(session.time, "sleep", lambda _seconds: None)

    session._atomic_json(destination, {"status": "RUNNING"})

    assert attempts == 3
    assert json.loads(destination.read_text(encoding="utf-8"))["status"] == "RUNNING"


def test_after_hours_preflight_publishes_honest_combined_and_profile_views(
    tmp_path: Path,
) -> None:
    runtime_paths = paths(tmp_path)
    code, payload = session.run_preflight(
        DAY,
        paths=runtime_paths,
        observed_now=datetime(2026, 8, 31, 18, 0, tzinfo=IST),
    )

    assert code == 0
    assert payload["reason"] == "PREFLIGHT_OK"
    status = json.loads(runtime_paths.status_path.read_text(encoding="utf-8"))
    assert status["status"] == "PREFLIGHT_OK"
    assert status["headline_valid"] is False
    assert status["full_history_event_parity_certified"] is False
    assert status["app_pool_state"] == "NOT_CHECKED"
    assert status["preferred_app_count"] == 8
    assert set(status["profiles"]) == {"v10", "v11", "v12"}
    for key in status["profiles"]:
        assert status["profiles"][key]["candidate_count"] == 0
        report = runtime_paths.latest_profile_report(key).read_text(encoding="utf-8")
        assert "## 5-Minute Selection" in report
        assert "## 1-Minute Entry Decisions" in report
        assert "no historical trades were replayed or fabricated" in report


class Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


def _source_row(side: str) -> dict[str, object]:
    long_side = side == "LONG"
    return {
        "setup_id": f"09:25_{side}",
        "side": side,
        "symbol": "TEST",
        "signal_time": "2026-09-01T09:25:00+05:30",
        "five_min_open": 99.0 if long_side else 101.0,
        "five_min_high": 102.0,
        "five_min_low": 98.0,
        "five_min_close": 100.0,
        "price_change_pct": 0.8 if long_side else -0.8,
        "oi_change_pct": 1.2,
        "volume_ratio": 3.5,
        "traded_value": 30_000_000.0,
        "ema9": 102.0 if long_side else 98.0,
        "ema20": 101.0 if long_side else 99.0,
        "ema50": 100.0,
        "oi": 101.2,
        "prev_oi": 100.0,
        "tick_size": 0.05,
        "equity_instrument_token": 100,
        "futures_instrument_token": 200,
        "futures_symbol": "TESTFUT",
    }


def test_real_run_retries_not_ready_five_minute_source_before_s1(
    tmp_path: Path,
) -> None:
    runtime_paths = paths(tmp_path)
    clock = Clock(datetime(2026, 9, 1, 9, 24, 58, tzinfo=IST))
    calls = {"five": 0}

    def authenticate_all_apps():
        return tuple(SimpleNamespace(app_name=f"app{index}") for index in range(1, 9))

    def build_source(*args, **kwargs):
        calls["five"] += 1
        if calls["five"] == 1:
            raise real_source.SourceNotReadyError("final futures marker pending")
        return SimpleNamespace(
            rows=(_source_row("LONG"), _source_row("SHORT")),
            symbol_tokens={"TEST": 100},
            reused=False,
        )

    fake_source = SimpleNamespace(
        __file__=real_source.__file__,
        LiveSourcePaths=real_source.LiveSourcePaths,
        SourceNotReadyError=real_source.SourceNotReadyError,
        SourceIncompleteError=real_source.SourceIncompleteError,
        SourceContractError=real_source.SourceContractError,
        authenticate_all_apps=authenticate_all_apps,
        build_and_publish_five_minute_source=build_source,
        fetch_and_publish_union_minute=lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("S+1 should not be reached in this test")
        ),
    )

    code = session.run_paper_session(
        DAY,
        paths=runtime_paths,
        source_module=fake_source,
        now_provider=clock.now,
        sleep_fn=clock.sleep,
        poll_seconds=1.0,
        max_iterations=12,
    )

    assert code == 0
    assert calls["five"] >= 2
    status = json.loads(runtime_paths.status_path.read_text(encoding="utf-8"))
    assert status["status"] == "RUNNING"
    assert status["data_incomplete"] is False
    assert status["app_pool_state"] == "HEALTHY"
    assert status["healthy_app_count"] == 8
    assert status["healthy_apps"] == "app1,app2,app3,app4,app5,app6,app7,app8"
    assert status["ingested_slots"] == ["09:25"]
    assert status["profiles"]["v10"]["candidate_count"] >= 1
    checkpoint = json.loads(runtime_paths.checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["runtime_state"]["ingested_slots"] == ["09:25"]


def test_seven_app_pool_is_visible_as_degraded_healthy_transport(
    tmp_path: Path,
) -> None:
    runtime_paths = paths(tmp_path)
    clock = Clock(datetime(2026, 9, 1, 9, 15, 0, tzinfo=IST))

    fake_source = SimpleNamespace(
        __file__=real_source.__file__,
        LiveSourcePaths=real_source.LiveSourcePaths,
        SourceNotReadyError=real_source.SourceNotReadyError,
        SourceIncompleteError=real_source.SourceIncompleteError,
        SourceContractError=real_source.SourceContractError,
        authenticate_all_apps=lambda: tuple(
            SimpleNamespace(app_name=f"app{index}") for index in range(1, 8)
        ),
        build_and_publish_five_minute_source=lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no five-minute slot is due")
        ),
        fetch_and_publish_union_minute=lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no one-minute source is required")
        ),
    )

    code = session.run_paper_session(
        DAY,
        paths=runtime_paths,
        source_module=fake_source,
        now_provider=clock.now,
        sleep_fn=clock.sleep,
        max_iterations=1,
    )

    assert code == 0
    status = json.loads(runtime_paths.status_path.read_text(encoding="utf-8"))
    assert status["status"] == "RUNNING"
    assert status["app_pool_state"] == "DEGRADED_HEALTHY"
    assert status["healthy_app_count"] == 7
    assert status["unhealthy_apps"] == "app8"


def test_session_independently_rejects_fewer_than_seven_apps() -> None:
    with pytest.raises(session.MultiPaperSessionError, match="at least 7"):
        session._set_runtime_app_pool(
            session.RuntimeState(),
            tuple(SimpleNamespace(app_name=f"app{index}") for index in range(1, 7)),
        )


@pytest.mark.parametrize(
    ("terminal_status", "terminal_phase", "last_minute"),
    (
        ("BLOCKED", "FAIL_CLOSED", datetime(2026, 9, 1, 10, 0, tzinfo=IST)),
        (
            "DEGRADED",
            "EXACT_1530_COMPLETE",
            datetime(2026, 9, 1, 15, 30, tzinfo=IST),
        ),
    ),
)
def test_terminal_checkpoint_never_resurrects_or_reauthenticates(
    tmp_path: Path,
    terminal_status: str,
    terminal_phase: str,
    last_minute: datetime,
) -> None:
    runtime_paths = paths(tmp_path)
    engine = session.paper_engine.MultiStrategyPaperEngine()
    engine.process_completed_minute(last_minute, {})
    state = session.RuntimeState(
        status=terminal_status,
        phase=terminal_phase,
        message="terminal evidence must remain terminal",
        data_incomplete=terminal_status != "COMPLETE",
        last_processed_minute=last_minute.isoformat(),
    )
    session.persist_checkpoint(runtime_paths, engine, state, symbol_tokens={})
    auth_calls = 0

    def forbidden_authentication():
        nonlocal auth_calls
        auth_calls += 1
        raise AssertionError("terminal restore must not authenticate")

    fake_source = SimpleNamespace(
        __file__=real_source.__file__,
        LiveSourcePaths=real_source.LiveSourcePaths,
        authenticate_all_apps=forbidden_authentication,
    )
    code = session.run_paper_session(
        DAY,
        paths=runtime_paths,
        source_module=fake_source,
        now_provider=lambda: datetime(2026, 9, 1, 15, 31, tzinfo=IST),
        sleep_fn=lambda _seconds: None,
    )

    assert code == 0
    assert auth_calls == 0
    status = json.loads(runtime_paths.status_path.read_text(encoding="utf-8"))
    assert status["status"] == terminal_status
    assert status["phase"] == terminal_phase
    checkpoint = json.loads(runtime_paths.checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["runtime_state"]["status"] == terminal_status


def test_unexpected_source_error_publishes_fail_closed_status(tmp_path: Path) -> None:
    runtime_paths = paths(tmp_path)
    clock = Clock(datetime(2026, 9, 1, 9, 24, 58, tzinfo=IST))

    def authenticate_all_apps():
        return tuple(SimpleNamespace(app_name=f"app{index}") for index in range(1, 9))

    def fail_source(*args, **kwargs):
        raise OSError("injected immutable evidence write failure")

    fake_source = SimpleNamespace(
        __file__=real_source.__file__,
        LiveSourcePaths=real_source.LiveSourcePaths,
        SourceNotReadyError=real_source.SourceNotReadyError,
        SourceIncompleteError=real_source.SourceIncompleteError,
        SourceContractError=real_source.SourceContractError,
        authenticate_all_apps=authenticate_all_apps,
        build_and_publish_five_minute_source=fail_source,
        fetch_and_publish_union_minute=lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("minute fetch must not follow a source failure")
        ),
    )

    code = session.run_paper_session(
        DAY,
        paths=runtime_paths,
        source_module=fake_source,
        now_provider=clock.now,
        sleep_fn=clock.sleep,
        poll_seconds=1.0,
        max_iterations=12,
    )

    assert code == 2
    status = json.loads(runtime_paths.status_path.read_text(encoding="utf-8"))
    assert status["status"] == "BLOCKED"
    assert status["phase"] == "UNEXPECTED_FAIL_CLOSED"
    assert status["data_incomplete"] is True
    assert "injected immutable evidence write failure" in status["message"]


def test_signal_minute_is_not_refetched_for_just_registered_candidates(
    tmp_path: Path,
) -> None:
    runtime_paths = paths(tmp_path)
    clock = Clock(datetime(2026, 9, 1, 9, 24, 58, tzinfo=IST))
    minute_calls: list[datetime] = []

    def authenticate_all_apps():
        return tuple(SimpleNamespace(app_name=f"app{index}") for index in range(1, 9))

    def build_source(*args, **kwargs):
        return SimpleNamespace(
            rows=(_source_row("LONG"), _source_row("SHORT")),
            symbol_tokens={"TEST": 100},
            reused=False,
        )

    def fetch_minute(*args, **kwargs):
        minute_calls.append(args[3])
        raise AssertionError("new S candidates must not fetch the S one-minute candle")

    fake_source = SimpleNamespace(
        __file__=real_source.__file__,
        LiveSourcePaths=real_source.LiveSourcePaths,
        SourceNotReadyError=real_source.SourceNotReadyError,
        SourceIncompleteError=real_source.SourceIncompleteError,
        SourceContractError=real_source.SourceContractError,
        authenticate_all_apps=authenticate_all_apps,
        build_and_publish_five_minute_source=build_source,
        fetch_and_publish_union_minute=fetch_minute,
    )

    code = session.run_paper_session(
        DAY,
        paths=runtime_paths,
        source_module=fake_source,
        now_provider=clock.now,
        sleep_fn=clock.sleep,
        poll_seconds=1.0,
        max_iterations=6,
    )

    assert code == 0
    assert minute_calls == []
    status = json.loads(runtime_paths.status_path.read_text(encoding="utf-8"))
    assert status["status"] == "RUNNING"
    assert status["ingested_slots"] == ["09:25"]
