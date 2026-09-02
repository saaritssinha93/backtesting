from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from unittest.mock import patch

import preopen_session_autofix as autofix
import preopen_session_healthcheck as preopen


def _task_query(
    *,
    state: str,
    last_run: str,
    status: str = "Ready",
    last_result: str = "0",
    start_time: str = "09:05:00",
) -> str:
    return "\n".join(
        (
            f"Scheduled Task State: {state}",
            f"Status: {status}",
            f"Last Run Time: {last_run}",
            f"Last Result: {last_result}",
            "Next Run Time: N/A",
            "Start Date: 02-09-2026",
            f"Start Time: {start_time}",
        )
    )


def test_selected_canonical_producer_is_required_by_date() -> None:
    with patch.object(
        preopen,
        "_run_schtasks_query",
        return_value=_task_query(
            state="Disabled",
            last_run="30-11-1999 00:00:00",
        ),
    ):
        fast_trial = preopen.check_dashboard_session_task(
            preopen.FNO_FAST_PRODUCTION_TASK,
            dt.datetime(2026, 9, 2, 8, 55, tzinfo=preopen.IST),
            v8_positively_disabled=True,
        )
        legacy_trial = preopen.check_dashboard_session_task(
            preopen.FNO_LEGACY_PRODUCTION_TASK,
            dt.datetime(2026, 9, 2, 8, 55, tzinfo=preopen.IST),
            v8_positively_disabled=True,
        )
        fast_ordinary = preopen.check_dashboard_session_task(
            preopen.FNO_FAST_PRODUCTION_TASK,
            dt.datetime(2026, 9, 3, 8, 55, tzinfo=preopen.IST),
            v8_positively_disabled=True,
        )
        legacy_ordinary = preopen.check_dashboard_session_task(
            preopen.FNO_LEGACY_PRODUCTION_TASK,
            dt.datetime(2026, 9, 3, 8, 55, tzinfo=preopen.IST),
            v8_positively_disabled=True,
        )

    assert fast_trial.status == "FAIL"
    assert legacy_trial.status == "PASS"
    assert fast_ordinary.status == "PASS"
    assert legacy_ordinary.status == "FAIL"


def test_never_run_or_completed_nonzero_task_fails_after_trigger() -> None:
    observed = dt.datetime(2026, 9, 2, 9, 7, tzinfo=preopen.IST)
    cases = (
        _task_query(state="Enabled", last_run="30-11-1999 00:00:00", last_result="267011"),
        _task_query(state="Enabled", last_run="02-09-2026 09:05:00", last_result="2"),
    )
    for query in cases:
        with patch.object(preopen, "_run_schtasks_query", return_value=query), patch.object(
            preopen,
            "now_ist",
            return_value=observed,
        ):
            result = preopen.check_task_enabled_state(
                preopen.FNO_FAST_PRODUCTION_TASK,
                f"task_{preopen.FNO_FAST_PRODUCTION_TASK}",
                require_run_today=True,
                inactive_ok=False,
                inactive_detail="session not enabled",
            )
        assert result.status == "FAIL"


def test_running_same_day_task_is_accepted_before_first_slot() -> None:
    observed = dt.datetime(2026, 9, 2, 9, 7, tzinfo=preopen.IST)
    with patch.object(
        preopen,
        "_run_schtasks_query",
        return_value=_task_query(
            state="Enabled",
            status="Running",
            last_run="02-09-2026 09:05:00",
            last_result="267009",
        ),
    ), patch.object(preopen, "now_ist", return_value=observed):
        result = preopen.check_task_enabled_state(
            preopen.FNO_FAST_PRODUCTION_TASK,
            f"task_{preopen.FNO_FAST_PRODUCTION_TASK}",
            require_run_today=True,
            inactive_ok=False,
            inactive_detail="session not enabled",
        )
    assert result.status == "PASS"


def test_fast_runtime_requires_same_day_supervisor_evidence(tmp_path: Path) -> None:
    status_path = tmp_path / "status"
    heartbeat_path = tmp_path / "heartbeat"
    observed = dt.datetime(2026, 9, 2, 9, 7, tzinfo=preopen.IST)

    missing = preopen.check_fast_production_trial_runtime(
        observed,
        status_path=status_path,
        heartbeat_path=heartbeat_path,
    )
    assert missing.status == "FAIL"

    status_path.write_text(
        "status=RUNNING\nts=2026-09-02_09:05:12\n",
        encoding="utf-8",
    )
    running = preopen.check_fast_production_trial_runtime(
        observed,
        status_path=status_path,
        heartbeat_path=heartbeat_path,
    )
    assert running.status == "PASS"

    status_path.write_text(
        "status=FAILED\nts=2026-09-02_09:05:40\n",
        encoding="utf-8",
    )
    heartbeat_path.write_text(
        "state=RUNNING\nts=2026-09-02T09:05:20+05:30\n",
        encoding="utf-8",
    )
    failed = preopen.check_fast_production_trial_runtime(
        observed,
        status_path=status_path,
        heartbeat_path=heartbeat_path,
    )
    assert failed.status == "FAIL"


def test_first_slot_requires_current_stock_complete_marker_within_60_seconds(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "slot_20260902_0920.json"
    observed = dt.datetime(2026, 9, 2, 9, 21, tzinfo=preopen.IST)
    pending = preopen.check_fast_production_trial_first_slot(
        dt.datetime(2026, 9, 2, 9, 10, tzinfo=preopen.IST),
        marker_path=marker,
    )
    assert pending.status == "WARN"

    marker.write_text(
        json.dumps(
            {
                "schema_version": "fno_oi_fetch_slot_v2",
                "slot_ist": "2026-09-02T09:20:00+05:30",
                "universe_date": "2026-09-02",
                "published_at_ist": "2026-09-02T09:20:15+05:30",
                "stock_complete": True,
                "stock_state": "SUCCESS",
                "global_complete": False,
                "index_no_candle_symbols": ["NIFTYFPI26SEPFUT"],
            }
        ),
        encoding="utf-8",
    )
    complete = preopen.check_fast_production_trial_first_slot(
        observed,
        marker_path=marker,
    )
    assert complete.status == "PASS"

    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["published_at_ist"] = "2026-09-02T09:21:01+05:30"
    marker.write_text(json.dumps(payload), encoding="utf-8")
    late = preopen.check_fast_production_trial_first_slot(
        observed,
        marker_path=marker,
    )
    assert late.status == "FAIL"


def test_active_omissions_and_v8_live_exclusivity_are_covered() -> None:
    assert preopen.FNO_FAST_PRODUCTION_TASK in preopen.DASHBOARD_SESSION_TASKS
    assert "EQIDV2_fno_v10_v11_v12_paper_0915" in preopen.DASHBOARD_SESSION_TASKS
    assert "EQIDV2_fno_v6_live_kite_qty1_0915" in preopen.DASHBOARD_SESSION_TASKS
    assert "EQIDV2_fno_v6_live_kite_qty1_0915" in preopen.FNO_V6_CUTOVER_DOWNSTREAM_TASKS


def test_fast_autofix_uses_only_scheduler_ignore_new_path() -> None:
    task = preopen.FNO_FAST_PRODUCTION_TASK
    assert list(autofix._iter_actions_for_fail("fno_fast_production_trial_runtime")) == [
        ("task_run", f"task:{task}", task)
    ]
    assert list(autofix._iter_actions_for_fail(f"task_{task}")) == [
        ("task_run", f"task:{task}", task)
    ]
    assert task not in autofix.TASK_TO_BAT


def test_shared_paper_runtime_rejects_late_not_run_and_stale_status(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"
    observed = dt.datetime(2026, 9, 2, 9, 20, tzinfo=preopen.IST)

    status_path.write_text(
        json.dumps(
            {
                "session_date": "2026-09-01",
                "status": "RUNNING",
                "phase": "INITIALIZED",
                "healthy_app_count": 8,
            }
        ),
        encoding="utf-8",
    )
    stale = preopen.check_v10_v11_v12_shared_runtime(
        observed,
        enabled=True,
        status_path=status_path,
    )
    assert stale.status == "FAIL"

    status_path.write_text(
        json.dumps(
            {
                "session_date": "2026-09-02",
                "status": "NOT_RUN",
                "phase": "PROSPECTIVE_START_GATE",
                "healthy_app_count": 0,
            }
        ),
        encoding="utf-8",
    )
    not_run = preopen.check_v10_v11_v12_shared_runtime(
        observed,
        enabled=True,
        status_path=status_path,
    )
    assert not_run.status == "FAIL"

    status_path.write_text(
        json.dumps(
            {
                "session_date": "2026-09-02",
                "status": "RUNNING",
                "phase": "INITIALIZED",
                "healthy_app_count": 7,
            }
        ),
        encoding="utf-8",
    )
    running = preopen.check_v10_v11_v12_shared_runtime(
        observed,
        enabled=True,
        status_path=status_path,
    )
    assert running.status == "PASS"
    task = preopen.FNO_V10_V11_V12_PAPER_TASK
    assert list(autofix._iter_actions_for_fail("fno_v10_v11_v12_shared_runtime")) == [
        ("task_run", f"task:{task}", task)
    ]


def test_live_kite_failure_is_not_in_autofix_task_namespace() -> None:
    with patch.object(
        preopen,
        "check_task_enabled_state",
        return_value=preopen.CheckResult("task_live", "FAIL", "never ran"),
    ):
        result = preopen.check_dashboard_session_task(
            preopen.FNO_V6_LIVE_KITE_TASK,
            dt.datetime(2026, 9, 2, 9, 20, tzinfo=preopen.IST),
            v8_positively_disabled=True,
        )
    assert result.status == "FAIL"
    assert not result.name.startswith("task_")
    assert list(autofix._iter_actions_for_fail(result.name)) == []
