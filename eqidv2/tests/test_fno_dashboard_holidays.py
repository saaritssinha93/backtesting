from datetime import date, datetime
from types import SimpleNamespace

import pytest

import fno_oi_common as common
import log_dashboard_server as dashboard
import nse_market_calendar as calendar
import preopen_session_healthcheck as preopen


HOLIDAY_NOW = datetime(2026, 9, 14, 11, 30, tzinfo=dashboard.IST)


def test_missing_default_csv_still_closes_known_holiday(tmp_path, monkeypatch):
    monkeypatch.setattr(calendar, "HOLIDAY_CSV", tmp_path / "absent.csv")
    assert not common.is_trading_day(date(2026, 9, 14), common.load_holidays())
    assert common.is_trading_day(date(2026, 9, 15), common.load_holidays())
    assert not common.is_trading_day(date(2026, 9, 13))
    assert common.is_trading_day(date(2026, 2, 1))
    assert common.load_holidays(tmp_path / "explicit-absent.csv") == set()


def test_calendar_matches_shared_paper_for_entire_2026():
    from datetime import timedelta
    from fno_v8_combined_paper_session import is_regular_nse_session

    start = date(2026, 1, 1)
    for offset in range(365):
        day = start + timedelta(days=offset)
        assert (not calendar.market_closed_reason(day)) == is_regular_nse_session(day), day


@pytest.mark.parametrize("state", ["FAILED", "BLOCKED", "PARTIAL", "CRASHED", "RUNNING", "DISABLED"])
def test_holiday_does_not_hide_runtime_failures_or_active_processes(state):
    raw = {"status": state, "error": "original evidence"}
    result = dashboard._apply_fno_market_calendar("fno_v13_v10_g_scanner_5min", raw, now_ist=HOLIDAY_NOW)
    assert result["status"] == state
    assert result["error"] == "original evidence"
    assert "Ganesh Chaturthi" in result["market_closed_reason"]
    assert raw == {"status": state, "error": "original evidence"}


def test_newer_closed_export_preserves_earlier_live_supervisor_failure():
    raw = {
        "status": "FAILED", "reason": "max_restarts_exceeded",
        "ts": "2026-09-14T10:08:53+05:30", "execution_state": "SKIPPED_NON_TRADING_DAY",
        "updated_at_ist": "2026-09-14T11:00:00+05:30", "session_date": "2026-09-14",
        "armed": "false", "order_states": "0",
    }
    result = dashboard._apply_fno_market_calendar("kite_trade_fno_id_v13_v10_g", raw, now_ist=HOLIDAY_NOW)
    assert result["status"] == "SKIPPED_NON_TRADING_DAY"
    assert result["previous_status"] == "FAILED"
    assert result["previous_reason"] == "max_restarts_exceeded"
    for changed in ({"updated_at_ist": "2026-09-14T09:00:00+05:30"}, {"order_states": "1"}, {"armed": "true"}, {"session_date": "2026-09-11"}):
        result = dashboard._apply_fno_market_calendar("kite_trade_fno_id_v13_v10_g", {**raw, **changed}, now_ist=HOLIDAY_NOW)
        assert result["status"] == "FAILED"


def test_options_unscheduled_is_not_a_failed_session():
    tasks = {"\\some_other_task": {"Status": "Ready"}}
    result = dashboard.apply_scheduler_status("fno_options_atm_fetch_5min", {}, tasks, now_ist=HOLIDAY_NOW)
    assert result["status"] == "NOT_SCHEDULED"
    assert dashboard._fno_eq_id_monitor_state(result, exists=True) == ("INACTIVE", "NOT_SCHEDULED", False)
    assert dashboard.apply_scheduler_status("fno_options_atm_fetch_5min", {"status": "FAILED"}, tasks)["status"] == "FAILED"
    assert dashboard.apply_scheduler_status("fno_options_atm_fetch_5min", {}, {}) == {}


def test_waiting_holiday_profile_is_skipped_but_disabled_stays_disabled():
    raw = {"status": "WAITING_OUTPUT", "runtime_status": "NOT_RUN"}
    result = dashboard._apply_fno_market_calendar("fno_v10_paper", raw, now_ist=HOLIDAY_NOW)
    assert result["status"] == "SKIPPED_NON_TRADING_DAY"
    assert result["observed_runtime_status"] == "WAITING_OUTPUT"
    assert dashboard._fno_eq_id_monitor_state(result, exists=False)[0] == "OK"
    disabled = {**raw, "scheduler_status": "DISABLED"}
    assert dashboard._apply_fno_market_calendar("fno_v10_paper", disabled, now_ist=HOLIDAY_NOW)["status"] == "WAITING_OUTPUT"
    next_day = HOLIDAY_NOW.replace(day=15)
    assert dashboard._apply_fno_market_calendar("fno_v10_paper", raw, now_ist=next_day) == raw
    assert dashboard._apply_fno_market_calendar("authentication_v2", raw, now_ist=HOLIDAY_NOW) == raw


def test_closed_day_timeline_never_checks_missing_market_files(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("market file read on closed session")
    monkeypatch.setattr(dashboard, "_read_csv_tail_rows", unexpected)
    result = dashboard._build_fno_eq_id_strategy_timelines([], now_ist=HOLIDAY_NOW)
    assert result["five_minute_rows"] == result["one_minute_rows"] == []
    assert result["hard_issue_count"] == result["watch_issue_count"] == 0
    assert "Ganesh Chaturthi" in "\n".join(dashboard._format_fno_eq_id_strategy_timelines(result))


def test_closed_day_preopen_checks_http_but_not_market_tasks(monkeypatch):
    monkeypatch.setattr(preopen, "now_ist", lambda: HOLIDAY_NOW)
    monkeypatch.setattr(preopen, "check_http", lambda *a, **kw: preopen.CheckResult("http", "FAIL", "unreachable"))
    monkeypatch.setattr(preopen, "_task_is_enabled", lambda *a: pytest.fail("market task queried"))
    checks = preopen.build_checks(35, False, False)
    assert [(c.name, c.status) for c in checks] == [("http", "FAIL"), ("market_calendar", "PASS")]


def test_holiday_feed_publishes_fresh_status_and_report_without_auth(tmp_path, monkeypatch):
    import fno_equity_fetch_1min as feed
    monkeypatch.setattr(feed, "_config", lambda generation: SimpleNamespace(validate_strategy=lambda: None))
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path)
    calls = []
    monkeypatch.setattr(common, "publish_status", lambda *a, **kw: calls.append((a, kw)))
    monkeypatch.setattr(feed, "_prewarm_runtimes", lambda *a, **kw: pytest.fail("authentication attempted"))
    args = feed.build_parser().parse_args(["--generation", "v5", "--session-date", "2026-09-14"])
    assert feed.run(args) == 0
    assert calls[0][0] == ("fno_v5_equity_1min_feed", "SKIPPED_NON_TRADING_DAY")
    assert "2026-09-14" in (tmp_path / "latest_fno_v5_equity_1min_feed.md").read_text()


def test_autofix_does_not_start_market_sessions_on_holiday(tmp_path, monkeypatch):
    import sys
    import preopen_session_autofix as autofix
    monkeypatch.setattr(sys, "argv", ["autofix"])
    monkeypatch.setattr(autofix, "LOG_DIR", tmp_path)
    monkeypatch.setattr(autofix, "_now_ist", lambda: HOLIDAY_NOW)
    monkeypatch.setattr(autofix, "_run_healthcheck", lambda **kw: (0, "closed", []))
    monkeypatch.setattr(autofix, "_sleep_until", lambda *a: pytest.fail("waited for market open"))
    monkeypatch.setattr(autofix, "_apply_action", lambda *a: pytest.fail("market session launched"))
    assert autofix.main() == 0


@pytest.mark.parametrize("module_name,report_name", [
    ("fno_oi_fetch_5min_fast_production", "latest_fno_oi_fast_production.md"),
    ("fno_oi_feature_ranker", "latest_fno_oi_leaderboard.md"),
])
def test_holiday_producers_exit_before_loading_universe(module_name, report_name, tmp_path, monkeypatch):
    import importlib
    producer = importlib.import_module(module_name)
    monkeypatch.setattr(common, "LATEST_DIR", tmp_path)
    calls = []
    monkeypatch.setattr(common, "publish_status", lambda *a, **kw: calls.append(a))
    monkeypatch.setattr(common, "load_near_month_universe", lambda *a, **kw: pytest.fail("universe loaded on holiday"))
    args = producer.build_parser().parse_args(["--session-date", "2026-09-14"])
    assert producer.run_session(args) == 0
    assert calls[0][1] == "SKIPPED_NON_TRADING_DAY"
    assert "SKIPPED_NON_TRADING_DAY" in (tmp_path / report_name).read_text()
