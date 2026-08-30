from __future__ import annotations

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
