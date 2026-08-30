from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from selenium.common.exceptions import NoSuchElementException, TimeoutException

import authentication_v2 as auth


def _stub_args() -> SimpleNamespace:
    return SimpleNamespace(force_login=False, test_now=False, max_refresh=0)


def test_main_authenticates_primary_before_every_secondary(monkeypatch) -> None:
    events: list[str] = []

    monkeypatch.setattr(auth, "parse_args", _stub_args)
    monkeypatch.setattr(auth, "_read_key_secret", lambda: ["key", "secret", "user", "pass", "totp"])
    monkeypatch.setattr(
        auth,
        "run_slot_scheduler",
        lambda **kwargs: events.append("app1"),
    )
    monkeypatch.setattr(
        auth,
        "_seed_additional_session_for_today",
        lambda **kwargs: events.append(f"app{kwargs['app_idx']}"),
    )

    auth.main()

    assert events == [f"app{app_idx}" for app_idx in range(1, 9)]


def test_main_attempts_all_secondaries_then_reraises_primary_failure(
    monkeypatch, capsys
) -> None:
    events: list[str] = []

    monkeypatch.setattr(auth, "parse_args", _stub_args)
    monkeypatch.setattr(auth, "_read_key_secret", lambda: ["key", "secret", "user", "pass", "totp"])

    def fail_primary(**kwargs) -> None:
        events.append("app1")
        raise RuntimeError("primary failed")

    monkeypatch.setattr(auth, "run_slot_scheduler", fail_primary)
    monkeypatch.setattr(
        auth,
        "_seed_additional_session_for_today",
        lambda **kwargs: events.append(f"app{kwargs['app_idx']}"),
    )

    with pytest.raises(RuntimeError, match="primary failed"):
        auth.main()

    assert events == [f"app{app_idx}" for app_idx in range(1, 9)]
    assert "[ERROR] [AUTH1] Primary app token generation failed" in capsys.readouterr().out


def test_main_reports_secondary_failure_and_continues_with_success_exit(
    monkeypatch, capsys
) -> None:
    events: list[str] = []

    monkeypatch.setattr(auth, "parse_args", _stub_args)
    monkeypatch.setattr(auth, "_read_key_secret", lambda: ["key", "secret", "user", "pass", "totp"])
    monkeypatch.setattr(
        auth,
        "run_slot_scheduler",
        lambda **kwargs: events.append("app1"),
    )

    def seed_secondary(**kwargs) -> None:
        app_idx = kwargs["app_idx"]
        events.append(f"app{app_idx}")
        if app_idx == 4:
            raise RuntimeError("app4 failed")

    monkeypatch.setattr(auth, "_seed_additional_session_for_today", seed_secondary)

    auth.main()

    assert events == [f"app{app_idx}" for app_idx in range(1, 9)]
    output = capsys.readouterr().out
    assert "[WARN] [AUTH4] App4 token generation failed" in output
    assert "Continuing with remaining apps." in output
def test_auth_url_log_redaction_hides_query_values():
    raw = "https://kite.example/connect/login?api_key=private-key&request_token=private-token"

    safe = auth._redact_url_for_log(raw)

    assert safe == "https://kite.example/connect/login?<redacted>"
    assert "private-key" not in safe
    assert "private-token" not in safe


class _SingleDeadlineWait:
    def __init__(self, driver, ignored_exceptions=()) -> None:
        self._driver = driver
        self._ignored_exceptions = tuple(ignored_exceptions)
        self.until_calls = 0

    def until(self, predicate):
        self.until_calls += 1
        value = predicate(self._driver)
        if value:
            return value
        raise TimeoutException("single deadline expired")


def test_find_first_checks_fallbacks_under_one_wait_deadline() -> None:
    expected = object()
    evaluated = []
    wait = _SingleDeadlineWait(driver=object())
    locators = [("id", "missing"), ("name", "working"), ("css", "unused")]

    def condition(locator):
        def predicate(_driver):
            evaluated.append(locator)
            return expected if locator == ("name", "working") else False

        return predicate

    result = auth._find_first(wait, locators, condition)

    assert result is expected
    assert wait.until_calls == 1
    assert evaluated == locators[:2]


def test_find_first_continues_after_wait_ignored_exception() -> None:
    expected = object()
    wait = _SingleDeadlineWait(
        driver=object(),
        ignored_exceptions=(NoSuchElementException,),
    )
    locators = [("id", "missing"), ("id", "working")]

    def condition(locator):
        def predicate(_driver):
            if locator == ("id", "missing"):
                raise NoSuchElementException("not present")
            return expected

        return predicate

    assert auth._find_first(wait, locators, condition) is expected
    assert wait.until_calls == 1


def test_request_token_after_totp_skips_click_for_auto_submit(monkeypatch) -> None:
    waits = []
    click = Mock(side_effect=AssertionError("submit click must be skipped"))

    def wait_for_token(driver, timeout_seconds, poll_seconds):
        waits.append((driver, timeout_seconds, poll_seconds))
        return "auto-token"

    monkeypatch.setattr(auth, "_wait_for_request_token_in_url", wait_for_token)
    monkeypatch.setattr(auth, "_click_with_retry", click)

    driver = object()
    result = auth._request_token_after_totp(driver, object(), [("id", "submit")])

    assert result == "auto-token"
    assert waits == [(driver, 2.0, 0.1)]
    click.assert_not_called()


def test_request_token_after_totp_keeps_optional_click_fallback(monkeypatch) -> None:
    token_results = iter((None, "clicked-token"))
    waits = []
    click = Mock()

    def wait_for_token(driver, timeout_seconds, poll_seconds):
        waits.append((timeout_seconds, poll_seconds))
        return next(token_results)

    monkeypatch.setattr(auth, "_wait_for_request_token_in_url", wait_for_token)
    monkeypatch.setattr(auth, "_click_with_retry", click)

    driver = object()
    wait = object()
    locators = [("id", "submit")]
    result = auth._request_token_after_totp(driver, wait, locators)

    assert result == "clicked-token"
    assert waits == [(2.0, 0.1), (45.0, 0.5)]
    click.assert_called_once_with(driver, wait, locators, retries=3)


def test_request_token_after_totp_preserves_timeout_after_optional_click_failure(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        auth,
        "_wait_for_request_token_in_url",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        auth,
        "_click_with_retry",
        Mock(side_effect=TimeoutException("no submit control")),
    )

    with pytest.raises(TimeoutException, match="request_token not found in URL"):
        auth._request_token_after_totp(object(), object(), [("id", "submit")])
