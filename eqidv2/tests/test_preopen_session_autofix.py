from __future__ import annotations

import json

import preopen_session_autofix as autofix


def test_failed_auth_status_retries_the_guarded_scheduled_task() -> None:
    assert list(autofix._iter_actions_for_fail("authentication_v2")) == [
        (
            "task_run",
            "task:EQIDV2_authentication_v2_0900",
            "EQIDV2_authentication_v2_0900",
        )
    ]


def test_unmapped_failure_remains_non_mutating() -> None:
    assert list(autofix._iter_actions_for_fail("unknown_failure")) == []


def test_first_slot_warning_keeps_autofix_polling(tmp_path, monkeypatch) -> None:
    payload = {
        "checks": [
            {
                "name": "fno_fast_production_trial_first_slot",
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
        "fno_fast_production_trial_first_slot"
    ]
