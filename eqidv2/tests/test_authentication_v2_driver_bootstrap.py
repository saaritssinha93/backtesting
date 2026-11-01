import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import authentication_v2 as auth


def _cached_binary(root: Path, version: str) -> Path:
    binary = root / "drivers" / "chromedriver" / "win64" / version / "chromedriver-win32" / "chromedriver.exe"
    binary.parent.mkdir(parents=True)
    binary.write_bytes(b"test")
    return binary


def test_cache_requires_matching_actual_driver_build_and_uses_bounded_probe(tmp_path, monkeypatch):
    wrong = _cached_binary(tmp_path, "156.0.8078.90")
    valid = _cached_binary(tmp_path, "156.0.8078.73")
    _cached_binary(tmp_path, "155.0.8059.84")
    calls = []

    def probe(args, **kwargs):
        calls.append((args, kwargs))
        version = "155.0.8059.84" if args[0] == str(wrong) else "156.0.8078.73"
        return SimpleNamespace(stdout=f"ChromeDriver {version} (build)")

    monkeypatch.setattr(auth.subprocess, "run", probe)
    assert auth._compatible_cached_driver("156.0.8078.48", tmp_path) == valid
    assert [args[0] for args, _ in calls] == [str(wrong), str(valid)]
    assert all(args[1:] == ["--version"] and kwargs["timeout"] == 5 for args, kwargs in calls)


def test_cache_recovers_from_unresponsive_driver(tmp_path, monkeypatch):
    _cached_binary(tmp_path, "156.0.8078.90")
    valid = _cached_binary(tmp_path, "156.0.8078.73")
    results = iter((subprocess.TimeoutExpired("driver", 5), SimpleNamespace(stdout="ChromeDriver 156.0.8078.73 (build)")))

    def probe(*args, **kwargs):
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(auth.subprocess, "run", probe)
    assert auth._compatible_cached_driver("156.0.8078.48", tmp_path) == valid


def test_windows_cache_bootstrap_needs_neither_powershell_nor_network(tmp_path, monkeypatch):
    cached = _cached_binary(tmp_path, "156.0.8078.73")
    chrome = tmp_path / "chrome.exe"
    monkeypatch.setattr(auth, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(auth, "_installed_windows_chrome", lambda: (chrome, "156.0.8078.48"))
    monkeypatch.setattr(auth, "_compatible_cached_driver", lambda *_args: cached)
    monkeypatch.setattr(auth, "ChromeDriverManager", Mock(side_effect=AssertionError("must use cache")))

    assert auth._resolve_browser_driver() == {"driver_path": str(cached), "browser_path": str(chrome), "browser_version": "156.0.8078.48", "source": "compatible-cache"}


def test_download_fallback_injects_native_version_into_both_wdm_managers(monkeypatch):
    manager = Mock()
    manager.return_value.install.return_value = "downloaded-driver.exe"
    monkeypatch.setattr(auth, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(auth, "_installed_windows_chrome", lambda: (Path("chrome.exe"), "156.0.8078.48"))
    monkeypatch.setattr(auth, "_compatible_cached_driver", lambda *_args: None)
    monkeypatch.setattr(auth, "ChromeDriverManager", manager)

    assert auth._resolve_browser_driver()["driver_path"] == "downloaded-driver.exe"
    kwargs = manager.call_args.kwargs
    assert kwargs["os_system_manager"].get_browser_version_from_os() == "156.0.8078"
    assert kwargs["cache_manager"]._os_system_manager is kwargs["os_system_manager"]


def test_driver_metadata_and_download_requests_have_timeouts(monkeypatch):
    get = Mock(return_value="response")
    monkeypatch.setattr(auth.WDMHttpClient, "get", get)
    assert auth._BoundedDriverHttpClient().get("https://example.invalid/driver") == "response"
    assert get.call_args.kwargs["timeout"] == (5, 15)


def test_bootstrap_timeout_is_reported_without_running_login(monkeypatch):
    run = Mock(side_effect=subprocess.TimeoutExpired("worker", 60))
    monkeypatch.setattr(auth.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="preparation exceeded 60s"):
        auth._prepare_browser_driver()
    assert run.call_args.kwargs["timeout"] == 60
    assert run.call_args.args[0][-1] == "--prepare-browser-driver"


def test_browser_page_deadline_precedes_navigation_and_quits_after_failure(monkeypatch):
    events = []
    driver = Mock()
    driver.set_page_load_timeout.side_effect = lambda value: events.append(("deadline", value))
    driver.get.side_effect = lambda _url: (_ for _ in ()).throw(RuntimeError("navigation failed"))
    monkeypatch.setattr(auth, "_prepare_browser_driver", lambda: {"driver_path": "driver.exe", "browser_path": None})
    monkeypatch.setattr(auth.webdriver, "Chrome", Mock(return_value=driver))
    with pytest.raises(RuntimeError, match="navigation failed"):
        auth._do_browser_login_for_request_token(Mock(), "unused", "unused", "unused")
    assert events == [("deadline", 30)]
    driver.quit.assert_called_once()


def test_auth_runner_no_longer_depends_on_powershell_for_completion():
    runner = (Path(auth.__file__).parent / "bat" / "run_authentication_v2.bat").read_text()
    assert "powershell" not in runner.lower()
    assert "isoformat(timespec='seconds')" in runner
