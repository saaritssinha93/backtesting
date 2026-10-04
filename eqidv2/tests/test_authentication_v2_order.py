from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from selenium.common.exceptions import NoSuchElementException, TimeoutException

import authentication_v2 as auth


ROOT = Path(__file__).resolve().parents[1]


def _stub_args() -> SimpleNamespace:
    return SimpleNamespace(force_login=False, test_now=False, max_refresh=0)


def test_auth_task_installer_has_bounded_task_level_retries() -> None:
    installer = (ROOT / "bat" / "schedule_authentication_v2_weekday.bat").read_text(
        encoding="utf-8"
    )
    hardener = (ROOT / "bat" / "harden_scheduled_task.ps1").read_text(
        encoding="utf-8"
    )

    assert "-RestartCount 2 -RestartInterval PT10M" in installer
    assert "[int]$RestartCount = 0" in hardener
    assert '[string]$RestartInterval = ""' in hardener
    assert '$definition.Settings.RestartCount = $RestartCount' in hardener
    assert '$definition.Settings.RestartInterval = if ($RestartCount -gt 0)' in hardener


def test_auth_runner_does_not_overwrite_status_for_duplicate_lock_exit() -> None:
    runner = (ROOT / "bat" / "run_authentication_v2.bat").read_text(
        encoding="utf-8"
    )
    duplicate_guard = runner.index('if "%EXIT_CODE%"=="75"')
    status_write = runner.index('>"%STATUS_FILE%" echo status=SUCCESS')
    assert duplicate_guard < status_write
    assert "active run retained" in runner


def test_main_authenticates_primary_before_every_secondary(monkeypatch) -> None:
    events: list[str] = []

    monkeypatch.setattr(auth, "parse_args", _stub_args)
    monkeypatch.setattr(auth, "_read_key_secret", lambda: ["key", "secret", "user", "pass", "totp"])
    monkeypatch.setattr(auth.time, "sleep", lambda _seconds: None)
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
    monkeypatch.setattr(auth.time, "sleep", lambda _seconds: None)

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

    assert events == ["app1", "app1"] + [
        f"app{app_idx}" for app_idx in range(2, 9)
    ]
    assert "[ERROR] [AUTH1] Primary app token generation failed" in capsys.readouterr().out


def test_main_attempts_all_apps_then_fails_if_secondary_remains_unhealthy(
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
    monkeypatch.setattr(auth.time, "sleep", lambda _seconds: None)

    def seed_secondary(**kwargs) -> None:
        app_idx = kwargs["app_idx"]
        events.append(f"app{app_idx}")
        if app_idx == 4:
            raise RuntimeError("app4 failed")

    monkeypatch.setattr(auth, "_seed_additional_session_for_today", seed_secondary)

    with pytest.raises(RuntimeError, match="app4"):
        auth.main()

    assert events == [
        "app1", "app2", "app3", "app4", "app4", "app5", "app6", "app7", "app8"
    ]
    output = capsys.readouterr().out
    assert "[WARN] [AUTH4] App4 attempt 1/2 failed" in output
    assert "[WARN] [AUTH4] App4 token generation failed after 2 attempts" in output
    assert "Continuing with remaining apps." in output
    assert "[ERROR] [AUTH-SUMMARY] healthy=7/8" in output


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
    assert waits == [(driver, auth.AUTO_SUBMIT_GRACE_SEC, 0.1)]
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
    assert waits == [
        (auth.AUTO_SUBMIT_GRACE_SEC, 0.1),
        (auth.POST_SUBMIT_TIMEOUT_SEC, 0.5),
    ]
    click.assert_called_once_with(driver, wait, locators, retries=1)


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


def test_resolve_totp_does_not_reuse_same_secret_in_one_interval(monkeypatch) -> None:
    clock = [60.0]
    sleep_calls: list[float] = []

    class FakeTotp:
        interval = 30

        def __init__(self, secret: str) -> None:
            self.secret = secret

        def now(self) -> str:
            return f"{int(clock[0] // self.interval):06d}"

    def advance_clock(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds

    monkeypatch.delenv("KITE_TOTP_COMMAND", raising=False)
    monkeypatch.delenv("KITE_TOTP_FILE", raising=False)
    monkeypatch.setattr(auth, "TOTP", FakeTotp)
    monkeypatch.setattr(auth.time, "time", lambda: clock[0])
    monkeypatch.setattr(auth.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(auth.time, "sleep", advance_clock)

    first, first_source = auth._resolve_totp("same-login-secret-for-window-test")
    second, second_source = auth._resolve_totp("same-login-secret-for-window-test")

    assert first_source == second_source == "pyotp_secret"
    assert first != second
    assert sleep_calls
    assert clock[0] >= 90.0


class _FakeLoginErrorElement:
    def __init__(self, text: str, *, displayed: bool = True) -> None:
        self.text = text
        self._displayed = displayed

    def is_displayed(self) -> bool:
        return self._displayed

    def get_attribute(self, name: str) -> str:
        if name in {"innerText", "textContent"}:
            return self.text
        return ""


class _FakeLoginErrorDriver:
    current_url = (
        "https://kite.example/connect/login?api_key=url-secret&"
        "request_token=url-request-secret"
    )

    def __init__(self, elements: list[_FakeLoginErrorElement]) -> None:
        self._elements = elements

    def find_elements(self, *_args) -> list[_FakeLoginErrorElement]:
        return self._elements


def test_visible_login_error_captures_message_without_secrets() -> None:
    driver = _FakeLoginErrorDriver(
        [
            _FakeLoginErrorElement(
                "Invalid two-factor authentication code 123456; "
                "password=hunter2; request_token=request-secret"
            ),
            _FakeLoginErrorElement("hidden-secret", displayed=False),
        ]
    )

    diagnostic = auth._visible_login_error(driver)

    assert "Invalid two-factor authentication code" in diagnostic
    for secret in (
        "123456",
        "hunter2",
        "request-secret",
        "hidden-secret",
        "url-secret",
        "url-request-secret",
    ):
        assert secret not in diagnostic


def test_request_token_timeout_includes_visible_login_error(monkeypatch) -> None:
    monkeypatch.setattr(
        auth,
        "_wait_for_request_token_in_url",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(auth, "_click_with_retry", Mock())
    monkeypatch.setattr(
        auth,
        "_visible_login_error",
        lambda _driver: "Invalid two-factor authentication code",
    )

    with pytest.raises(
        TimeoutException,
        match="Invalid two-factor authentication code",
    ):
        auth._request_token_after_totp(object(), object(), [("id", "submit")])


def test_request_token_detects_cleared_totp_without_second_click(monkeypatch) -> None:
    click = Mock(side_effect=AssertionError("cleared TOTP must not be resubmitted"))
    monkeypatch.setattr(
        auth,
        "_wait_for_request_token_in_url",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(auth, "_visible_login_error", lambda _driver: "")
    monkeypatch.setattr(auth, "_totp_form_state", lambda _driver: "empty")
    monkeypatch.setattr(auth, "_click_with_retry", click)

    with pytest.raises(TimeoutException, match="reset the TOTP field"):
        auth._request_token_after_totp(object(), object(), [("id", "submit")])
    click.assert_not_called()


class _FakeTotpField:
    def __init__(self, maxlength: str) -> None:
        self.value = ""
        self.attrs = {
            "maxlength": maxlength,
            "type": "number",
            "id": "userid" if maxlength == "6" else "",
            "name": "",
            "autocomplete": "",
        }

    def is_displayed(self) -> bool:
        return True

    def get_attribute(self, name: str) -> str:
        if name == "value":
            return self.value
        return self.attrs.get(name, "")

    def clear(self) -> None:
        self.value = ""

    def send_keys(self, text: str) -> None:
        self.value += str(text)


class _FakeTotpDriver:
    def __init__(self, fields: list[_FakeTotpField]) -> None:
        self.fields = fields

    def find_elements(self, *_args):
        return self.fields


class _FakeTotpWait:
    def __init__(self, driver: _FakeTotpDriver) -> None:
        self._driver = driver

    def until(self, predicate):
        value = predicate(self._driver)
        if not value:
            raise TimeoutException("not found")
        return value


def test_fill_totp_verifies_single_six_digit_field() -> None:
    field = _FakeTotpField("6")
    auth._fill_totp(_FakeTotpWait(_FakeTotpDriver([field])), "123456")
    assert field.value == "123456"


def test_fill_totp_uses_all_six_one_character_boxes() -> None:
    fields = [_FakeTotpField("1") for _ in range(6)]
    auth._fill_totp(_FakeTotpWait(_FakeTotpDriver(fields)), "123456")
    assert [field.value for field in fields] == list("123456")
