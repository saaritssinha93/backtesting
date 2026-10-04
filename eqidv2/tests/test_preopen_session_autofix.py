from __future__ import annotations

import json
from datetime import datetime

import preopen_session_autofix as autofix
import preopen_session_healthcheck as healthcheck


def test_failed_auth_status_retries_the_guarded_scheduled_task() -> None:
    expected = [
        (
            "task_run",
            "task:EQIDV2_authentication_v2_0900",
            "EQIDV2_authentication_v2_0900",
        )
    ]
    assert list(autofix._iter_actions_for_fail("authentication_v2")) == expected
    assert list(
        autofix._iter_actions_for_fail("task_EQIDV2_authentication_v2_0900")
    ) == expected
    assert "EQIDV2_authentication_v2_0900" not in autofix.TASK_TO_BAT


def test_unmapped_failure_remains_non_mutating() -> None:
    assert list(autofix._iter_actions_for_fail("unknown_failure")) == []


def test_authentication_ready_requires_complete_eight_app_roster(
    tmp_path, monkeypatch
) -> None:
    observed = datetime(2026, 9, 29, 9, 45, tzinfo=healthcheck.IST)
    monkeypatch.setattr(healthcheck, "now_ist", lambda: observed)
    status_path = tmp_path / "authentication_v2_runner.status"
    status_path.write_text(
        "status=SUCCESS\nts=2026-09-29_09:40:11\nexit_code=0\n",
        encoding="utf-8",
    )
    state_path = tmp_path / "auth_v2_state.json"
    state = {"session_date_ist": "2026-09-29"}
    state.update(
        {f"session_date_ist_app{index}": "2026-09-29" for index in range(2, 9)}
    )
    state_path.write_text(json.dumps(state), encoding="utf-8")
    for index in range(1, 9):
        suffix = "" if index == 1 else str(index)
        (tmp_path / f"access_token{suffix}.txt").write_text(
            "token", encoding="utf-8"
        )

    ready = healthcheck.check_authentication_ready(
        status_path,
        state_path=state_path,
        token_dir=tmp_path,
    )
    assert ready.status == "PASS"

    state["session_date_ist_app8"] = "2026-09-28"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    partial = healthcheck.check_authentication_ready(
        status_path,
        state_path=state_path,
        token_dir=tmp_path,
    )
    assert partial.status == "FAIL"
    assert "current_apps=7/8" in partial.detail


def test_first_slot_warning_keeps_autofix_polling(tmp_path, monkeypatch) -> None:
    payload = {
        "checks": [
            {
                "name": "fno_fast_production_first_slot",
                "status": "WARN",
                "detail": "acceptance pending",
            }
        ]
    }
    report_json = tmp_path / "preopen.json"
    report_json.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(autofix, "HEALTHCHECK_JSON", report_json)
    monkeypatch.setattr(autofix, "_run_cmd", lambda *_args, **_kwargs: (0, "WAIT"))

    code, _output, blockers = autofix._run_healthcheck(max_age_min=35)

    assert code == 0
    assert [item["name"] for item in blockers] == [
        "fno_fast_production_first_slot"
    ]
