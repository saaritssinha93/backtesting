"""Daily G orchestration boundaries; all artifacts and replay calls are isolated."""
from __future__ import annotations

from datetime import date, datetime
import json
from types import SimpleNamespace

import pytest

import backtesting_result_v13_v10_g_daily as daily


DAY = date(2026, 9, 15)


@pytest.fixture(autouse=True)
def isolate_shared_runtime_publications(monkeypatch):
    monkeypatch.setattr(daily.common, "publish_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily.common, "publish_heartbeat", lambda *args, **kwargs: None)


@pytest.fixture
def harness(tmp_path, monkeypatch):
    calls, publications = [], []
    monkeypatch.setattr(daily.common, "now_ist", lambda: datetime(2026, 9, 15, 16, 15, tzinfo=daily.common.IST))
    monkeypatch.setattr(daily.common, "load_holidays", lambda: set())
    monkeypatch.setattr(daily.common, "is_trading_day", lambda day, holidays: True)
    monkeypatch.setattr(daily.config, "validate_strategy", lambda: None)
    monkeypatch.setattr(daily.config, "attest_selected_backtest", lambda: None)

    def publish(day, root, state, reason="", result=None):
        publications.append(dict(day=day, root=root, state=state, reason=reason, result=result))
        return {}

    def wait(day, args):
        calls.append(("wait", day))
        return True

    def verify(day, root):
        calls.append(("verify", day))
        return dict(date=day.isoformat(), scope="fno", overall_status="PASS", overall_exit_code=0)

    def replay(day, output):
        calls.append(("replay", day))
        raise RuntimeError("ISOLATED_REPLAY_NOT_CONFIGURED")

    monkeypatch.setattr(daily, "_publish", publish)
    monkeypatch.setattr(daily, "wait_for_data", wait)
    monkeypatch.setattr(daily, "verify_data", verify)
    monkeypatch.setattr(daily, "run_replay", replay)

    def args(day=DAY, *, wait=False):
        argv = ["--date", day.isoformat(), "--output-root", str(tmp_path / "g_daily")]
        if wait:
            argv.append("--wait-for-data")
        return daily.build_parser().parse_args(argv)

    return SimpleNamespace(calls=calls, publications=publications, args=args, root=tmp_path / "g_daily")


def test_default_date_is_today_even_on_weekend(harness, monkeypatch):
    saturday = datetime(2026, 9, 19, 16, 0, tzinfo=daily.common.IST)
    monkeypatch.setattr(daily.common, "now_ist", lambda: saturday)
    monkeypatch.setattr(daily.common, "is_trading_day", lambda day, holidays: False)
    args = daily.build_parser().parse_args(["--output-root", str(harness.root)])
    assert daily.run(args) == 0
    assert harness.calls == []
    assert harness.publications[-1]["day"] == saturday.date()


def test_holiday_skips_before_wait_verification_or_replay(harness, monkeypatch):
    monkeypatch.setattr(daily.common, "is_trading_day", lambda day, holidays: False)
    assert daily.run(harness.args(wait=True)) == 0
    assert harness.calls == []
    assert harness.publications[-1]["state"] == "SKIPPED_NON_TRADING_DAY"
    assert harness.publications[-1]["day"] == DAY
    assert harness.publications[-1]["result"] is None


def test_today_before_full_close_cannot_publish_partial_zero_result(harness, monkeypatch):
    monkeypatch.setattr(daily.common, "now_ist", lambda: datetime(2026, 9, 15, 15, 29, 59, tzinfo=daily.common.IST))
    assert daily.run(harness.args(wait=True)) == 2
    assert harness.calls == []
    assert harness.publications[-1]["state"] == "WAITING_FOR_SESSION_CLOSE"
    assert harness.publications[-1]["result"] is None


def test_future_trading_day_is_blocked_before_wait(harness):
    assert daily.run(harness.args(date(2026, 9, 16), wait=True)) != 0
    assert harness.calls == []
    assert harness.publications[-1]["state"] != "SUCCESS"
    assert harness.publications[-1]["result"] is None


def test_failed_wait_never_verifies_or_replays(harness, monkeypatch):
    def blocked(day, args):
        harness.calls.append(("wait", day))
        daily._publish(day, args.output_root, "BLOCKED_DATA_NOT_READY")
        return False

    monkeypatch.setattr(daily, "wait_for_data", blocked)
    assert daily.run(harness.args(wait=True)) != 0
    assert harness.calls == [("wait", DAY)]
    assert harness.publications[-1]["state"] != "SUCCESS"
    assert harness.publications[-1]["result"] is None


def test_historical_explicit_date_does_not_wait_for_today_producer(harness, monkeypatch):
    historical_day = date(2026, 9, 11)

    def blocked(day, root):
        harness.calls.append(("verify", day))
        raise RuntimeError("ISOLATED_VERIFY_FAILURE")

    monkeypatch.setattr(daily, "verify_data", blocked)
    assert daily.run(harness.args(historical_day, wait=True)) != 0
    assert harness.calls == [("verify", historical_day)]
    assert harness.publications[-1]["day"] == historical_day
    assert harness.publications[-1]["result"] is None


def test_verifier_failure_stops_before_replay(harness, monkeypatch):
    def blocked(day, root):
        harness.calls.append(("verify", day))
        raise RuntimeError("MISSING_CURRENT_DAY_DATA")

    monkeypatch.setattr(daily, "verify_data", blocked)
    assert daily.run(harness.args(wait=True)) != 0
    assert harness.calls == [("wait", DAY), ("verify", DAY)]
    assert harness.publications[-1]["state"] != "SUCCESS"
    assert harness.publications[-1]["result"] is None


def test_replay_failure_is_not_a_zero_trade_success(harness):
    assert daily.run(harness.args(wait=True)) != 0
    assert harness.calls == [("wait", DAY), ("verify", DAY), ("replay", DAY)]
    assert harness.publications[-1]["state"] != "SUCCESS"
    assert harness.publications[-1]["result"] is None


def test_daily_cli_has_no_publish_only_previous_run_shortcut():
    with pytest.raises(SystemExit):
        daily.build_parser().parse_args(["--publish-only", "old_completed_run"])


def successful_result(day=DAY):
    return dict(schema_version="fno_v13_v10_g_daily_replay_v1", strategy="V13-V10-G",
                strategy_version=daily.config.STRATEGY_VERSION, session_date=day.isoformat(),
                days=[day.isoformat()], state="SUCCESS", complete=True,
                metrics=dict(sessions=1, orders=0, fills=0, net_profit_rupees=0),
                artifacts={}, coverage=dict(problems=[]), source_fingerprint="a" * 64)


def test_successful_result_is_only_requested_day_g(harness, monkeypatch):
    def replay(day, output):
        harness.calls.append(("replay", day))
        assert output.parent == harness.root / "runs" / DAY.isoformat()
        return successful_result(day)

    monkeypatch.setattr(daily, "run_replay", replay)
    assert daily.run(harness.args(wait=True)) == 0
    assert harness.calls == [("wait", DAY), ("verify", DAY), ("replay", DAY)]
    final = harness.publications[-1]
    assert final["state"] == "SUCCESS"
    assert final["day"] == DAY
    assert final["result"]["strategy"] == "V13-V10-G"
    assert final["result"]["data_verification"]["date"] == DAY.isoformat()


@pytest.mark.parametrize("field,value", [
    ("strategy", "V6_CONTROL"),
    ("strategy_version", "FNO_V6_BEST_NET_CASH_EQUITY_20260811"),
    ("session_date", "2026-09-11"),
    ("days", ["2026-09-11", "2026-09-15"]),
    ("complete", False),
    ("metrics", {"sessions": 31}),
])
def test_wrong_strategy_old_session_or_incomplete_result_cannot_publish_success(
    harness, monkeypatch, field, value
):
    result = successful_result()
    result[field] = value
    monkeypatch.setattr(daily, "run_replay", lambda day, output: result)
    assert daily.run(harness.args()) != 0
    assert harness.publications[-1]["state"] != "SUCCESS"
    assert harness.publications[-1]["result"] is None


def test_artifact_from_another_day_fails_even_with_today_manifest(tmp_path):
    artifact = tmp_path / "portfolio_trades.csv"
    daily.common.atomic_write_text(artifact, "day,tradingsymbol\n2026-09-11,OLD\n2026-09-15,CURRENT\n")
    result = successful_result()
    result["artifacts"] = {"portfolio_trades": str(artifact)}
    with pytest.raises(ValueError, match="Different session"):
        daily.validate_result(result, DAY)


@pytest.mark.parametrize("field,bad", [("date", "2026-09-11"), ("scope", "all"),
                                      ("overall_status", "WARN"), ("overall_exit_code", 1)])
def test_verification_is_dated_fno_scoped_and_restores_shared_directory(tmp_path, monkeypatch, field, bad):
    import data_for_backtesting_verify as verifier
    legacy = tmp_path / "legacy_verification"
    root = tmp_path / "g_daily"
    monkeypatch.setattr(verifier, "VERIFY_DIR", legacy)

    def verify(day, scope):
        assert day == DAY.isoformat() and scope == "fno"
        assert verifier.VERIFY_DIR.is_relative_to(root)
        payload = dict(date=day, scope=scope, overall_status="PASS", overall_exit_code=0)
        payload[field] = bad
        daily.common.atomic_write_json(verifier.VERIFY_DIR / f"data_verify_{day}.json", payload)
        return 0

    monkeypatch.setattr(verifier, "run_verify", verify)
    with pytest.raises(ValueError, match="verification did not pass"):
        daily.verify_data(DAY, root)
    assert verifier.VERIFY_DIR == legacy
    assert not legacy.exists()


def test_verification_exception_restores_shared_directory(tmp_path, monkeypatch):
    import data_for_backtesting_verify as verifier
    legacy = tmp_path / "legacy_verification"
    monkeypatch.setattr(verifier, "VERIFY_DIR", legacy)

    def fail(*args, **kwargs):
        raise RuntimeError("SIMULATED_VERIFICATION_CRASH")

    monkeypatch.setattr(verifier, "run_verify", fail)
    with pytest.raises(RuntimeError, match="SIMULATED_VERIFICATION_CRASH"):
        daily.verify_data(DAY, tmp_path / "g_daily")
    assert verifier.VERIFY_DIR == legacy


def test_old_verification_file_is_not_accepted_if_no_new_proof_is_written(tmp_path, monkeypatch):
    import data_for_backtesting_verify as verifier
    root = tmp_path / "g_daily"
    daily.common.atomic_write_json(root / "verification" / f"data_verify_{DAY}.json",
                                  dict(date=DAY.isoformat(), scope="fno", overall_status="PASS", overall_exit_code=0))
    monkeypatch.setattr(verifier, "run_verify", lambda *args, **kwargs: 0)
    with pytest.raises((FileNotFoundError, ValueError, RuntimeError)):
        daily.verify_data(DAY, root)


def test_non_success_publication_clears_previous_result_from_latest_only(tmp_path, monkeypatch):
    monkeypatch.setattr(daily.common, "publish_status", lambda *args, **kwargs: None)
    yesterday = date(2026, 9, 11)
    daily._publish(yesterday, tmp_path, "SUCCESS", result=successful_result(yesterday))
    daily._publish(DAY, tmp_path, "WAITING_FOR_DATA", "Today's producer is still running.")
    payload = json.loads((tmp_path / "latest/latest_backtesting_result_v13_v10_g.json").read_text())
    report = (tmp_path / "latest/latest_backtesting_result_v13_v10_g.md").read_text()
    assert payload["session_date"] == DAY.isoformat()
    assert payload["strategy"] == "V13-V10-G"
    assert payload["status"] == "WAITING_FOR_DATA"
    assert "result" not in payload
    assert yesterday.isoformat() not in report
    assert "No completed backtest result" in report
    assert (tmp_path / f"reports/{yesterday}/backtesting_result_v13_v10_g.md").exists()


def test_data_wait_uses_requested_day_and_latest_producer_attempt(tmp_path, monkeypatch):
    import wait_for_data_backtesting_ready as readiness
    monkeypatch.setattr(readiness, "LOG_DIR", tmp_path)
    day_log = tmp_path / f"data_for_backtesting_{DAY}.log"
    day_log.write_text("START Data for backtesting parallel session\n"
                       "END Data for backtesting parallel session (exit=0)\n"
                       "START Data for backtesting parallel session\n", encoding="utf-8")
    # A previous completed run must not satisfy a newer in-progress attempt.
    assert readiness._data_job_status(DAY.isoformat())[0] == "WAIT"
    publications, sleeps = [], []
    monkeypatch.setattr(daily, "_publish", lambda *args, **kwargs: publications.append(args))
    monotonic = [0.0]
    monkeypatch.setattr(daily.time, "monotonic", lambda: monotonic[0])

    def finish_current_run(delay):
        sleeps.append(delay)
        monotonic[0] += delay
        with day_log.open("a", encoding="utf-8") as handle:
            handle.write("END Data for backtesting parallel session (exit=0)\n")

    monkeypatch.setattr(daily.time, "sleep", finish_current_run)
    args = SimpleNamespace(output_root=tmp_path, timeout_sec=30, poll_sec=15.)
    assert daily.wait_for_data(DAY, args) is True
    assert sleeps == [15.]
    assert len(publications) == 1
    assert publications[0][0] == DAY
    assert publications[0][2] == "WAITING_FOR_DATA"


@pytest.mark.parametrize("state,timeout", [("FAIL", 30), ("WAIT", 0)])
def test_failed_or_timed_out_producer_blocks_without_sleep(tmp_path, monkeypatch, state, timeout):
    import wait_for_data_backtesting_ready as readiness
    observed, publications = [], []

    def status(day):
        observed.append(day)
        return state, "ISOLATED_PRODUCER_NOT_READY"

    monkeypatch.setattr(readiness, "_data_job_status", status)
    monkeypatch.setattr(daily, "_publish", lambda *args, **kwargs: publications.append(args))
    monkeypatch.setattr(daily.time, "monotonic", lambda: 0.)
    monkeypatch.setattr(daily.time, "sleep", lambda delay: pytest.fail("Must not sleep after failure or timeout"))
    args = SimpleNamespace(output_root=tmp_path, timeout_sec=timeout, poll_sec=15.)
    assert daily.wait_for_data(DAY, args) is False
    assert observed == [DAY.isoformat()]
    assert publications[-1][2] == "BLOCKED_DATA_NOT_READY"


def test_fresh_verification_is_accepted_and_never_writes_legacy_root(tmp_path, monkeypatch):
    import data_for_backtesting_verify as verifier
    legacy = tmp_path / "legacy_verification"
    root = tmp_path / "g_daily"
    monkeypatch.setattr(verifier, "VERIFY_DIR", legacy)
    payload = dict(date=DAY.isoformat(), scope="fno", overall_status="PASS", overall_exit_code=0)

    def verify(day, scope):
        assert verifier.VERIFY_DIR.is_relative_to(root)
        assert scope == "fno" and day == DAY.isoformat()
        daily.common.atomic_write_json(verifier.VERIFY_DIR / f"data_verify_{day}.json", payload)
        return 0

    monkeypatch.setattr(verifier, "run_verify", verify)
    assert daily.verify_data(DAY, root) == payload
    assert verifier.VERIFY_DIR == legacy
    assert not legacy.exists()


def test_incomplete_coverage_has_blocked_status_and_no_success_metrics(harness, monkeypatch):
    result = successful_result()
    result.update(state="BLOCKED_INCOMPLETE_DATA", complete=False, metrics=None,
                  coverage={"problems": ["MISSING_CURRENT_DAY_1MIN_EXIT_PATH"]})
    monkeypatch.setattr(daily, "run_replay", lambda day, output: result)
    assert daily.run(harness.args()) == 2
    final = harness.publications[-1]
    assert final["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert final["result"]["metrics"] is None
    assert final["result"]["coverage"]["problems"] == ["MISSING_CURRENT_DAY_1MIN_EXIT_PATH"]


def test_trade_details_report_only_executed_rows_and_portfolio_profit(tmp_path):
    trade_path = tmp_path / "portfolio_trades.csv"
    trade_path.write_text("day,tradingsymbol,portfolio_executed,portfolio_net_profit_rupees,net_profit_rupees\n"
                          "2026-09-15,FILLED_STOCK,True,1234,5678\n"
                          "2026-09-15,REJECTED_STOCK,False,0,-9000\n", encoding="utf-8")
    result = successful_result()
    result["artifacts"] = {"portfolio_trades": str(trade_path)}
    report = daily.render_report(result)
    assert "FILLED_STOCK" in report
    assert "REJECTED_STOCK" not in report
    assert "1234" in report or "1,234" in report


def test_blocked_report_displays_adapter_nested_coverage_reasons(tmp_path, monkeypatch):
    monkeypatch.setattr(daily.common, "publish_status", lambda *args, **kwargs: None)
    result = successful_result()
    result.update(state="BLOCKED_INCOMPLETE_DATA", complete=False, metrics=None,
                  coverage={"problems": [{"symbol": "MISSING_STOCK", "reason": "MISSING_REQUIRED_EQUITY_MINUTES"}]})
    daily._publish(DAY, tmp_path, "BLOCKED_INCOMPLETE_DATA", result=result)
    report = (tmp_path / "latest/latest_backtesting_result_v13_v10_g.md").read_text()
    assert "MISSING_STOCK" in report
    assert "MISSING_REQUIRED_EQUITY_MINUTES" in report
    assert "No completed backtest result" in report


@pytest.fixture
def heartbeat_fakes(monkeypatch):
    events = []

    class Event:
        stopped = False

        def __init__(self):
            self.wait_count = 0

        def wait(self, interval):
            events.append(("wait", interval))
            self.wait_count += 1
            return self.wait_count > 1

        def set(self):
            self.stopped = True
            events.append(("stop",))

    class Thread:
        def __init__(self, *, target, name, daemon):
            assert name == "g-daily-heartbeat" and daemon is True
            self.target = target

        def start(self):
            events.append(("start",))
            self.target()

        def join(self):
            assert events[-1] == ("stop",)
            events.append(("join",))

    monkeypatch.setattr(daily, "threading", SimpleNamespace(Event=Event, Thread=Thread))
    monkeypatch.setattr(daily.common, "publish_heartbeat",
                        lambda session, state, **payload: events.append(("pulse", session, state, payload)))
    return events


@pytest.mark.parametrize("raise_in_body", [False, True])
def test_heartbeat_always_stops_and_joins_before_leaving_context(heartbeat_fakes, raise_in_body):
    events = heartbeat_fakes

    def operation():
        with daily._running_heartbeat(DAY, interval=.01):
            events.append(("body",))
            if raise_in_body:
                raise RuntimeError("ISOLATED_REPLAY_CRASH")

    if raise_in_body:
        with pytest.raises(RuntimeError, match="ISOLATED_REPLAY_CRASH"):
            operation()
    else:
        operation()
    assert events[-2:] == [("stop",), ("join",)]
    pulse = next(event for event in events if event[0] == "pulse")
    assert pulse[1:3] == (daily.SESSION, "RUNNING")
    assert pulse[3]["session_date"] == DAY.isoformat()
    assert pulse[3]["phase"] == "VERIFYING_OR_REPLAYING_G"
    assert [event for event in events if event[0] == "wait"] == [("wait", .01), ("wait", .01)]


@pytest.mark.parametrize("verifier_fails", [False, True])
def test_run_final_status_is_published_after_heartbeat_shutdown(harness, heartbeat_fakes, monkeypatch, verifier_fails):
    events = heartbeat_fakes
    publish = daily._publish

    def ordered_publish(day, root, state, reason="", result=None):
        events.append(("publish", state))
        return publish(day, root, state, reason, result)

    monkeypatch.setattr(daily, "_publish", ordered_publish)
    monkeypatch.setattr(daily, "run_replay", lambda day, output: successful_result(day))
    if verifier_fails:
        def fail(day, root):
            raise RuntimeError("ISOLATED_SOURCE_FAILURE")
        monkeypatch.setattr(daily, "verify_data", fail)
    assert daily.run(harness.args()) == (2 if verifier_fails else 0)
    assert events[-3:] == [("stop",), ("join",),
                           ("publish", "BLOCKED_BACKTEST" if verifier_fails else "SUCCESS")]


def test_success_status_includes_scalar_metrics_and_pending_status_clears_them(tmp_path, monkeypatch):
    statuses = []
    monkeypatch.setattr(daily.common, "publish_status",
                        lambda session, state, **payload: statuses.append((session, state, payload)))
    result = successful_result()
    result["metrics"].update(fills=3, win_rate_pct=66.67, net_profit_rupees=3210., profit_factor=2.5)
    daily._publish(DAY, tmp_path, "SUCCESS", result=result)
    published = statuses[-1][2]
    assert {key: published[key] for key in ("fills", "win_rate_pct", "net_profit_rupees", "trade_pf")} == {
        "fills": 3, "win_rate_pct": 66.67, "net_profit_rupees": 3210., "trade_pf": 2.5}
    daily._publish(DAY, tmp_path, "WAITING_FOR_DATA")
    assert not {"fills", "win_rate_pct", "net_profit_rupees", "trade_pf"}.intersection(statuses[-1][2])
