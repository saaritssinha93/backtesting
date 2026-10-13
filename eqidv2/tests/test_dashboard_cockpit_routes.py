"""The monitoring cockpit reuses the existing authenticated read boundary."""
import base64
import io
import json
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import pytest
import log_dashboard_server as dashboard


@pytest.fixture(autouse=True)
def empty_read_cache(monkeypatch):
    monkeypatch.setattr(dashboard, "_DASH_READ_CACHE", OrderedDict())
    monkeypatch.setattr(dashboard, "_DASH_READ_INFLIGHT", {})


class Handler(dashboard.LogDashboardHandler):
    def __init__(self, path, authorized=True):
        self.path = path
        self.server = SimpleNamespace(username="cockpit", password="test", api_token="")
        self.headers = {}
        if authorized:
            self.headers["Authorization"] = "Basic " + base64.b64encode(b"cockpit:test").decode()
        self.wfile = io.BytesIO()
        self.response_headers = {}
        self.status = None

    def send_response(self, code, message=None):
        self.status = int(code)

    def send_header(self, name, value):
        self.response_headers[name] = value

    def end_headers(self):
        pass


def test_authentication_precedes_cockpit_evidence_read(monkeypatch):
    import dashboard_cockpit
    calls = []
    monkeypatch.setattr(dashboard_cockpit, "load_cockpit_data", lambda *args: calls.append(args))
    handler = Handler("/api/cockpit?date=2026-10-09", authorized=False)
    handler.do_GET()
    assert handler.status == 401
    assert calls == []


def test_route_passes_selected_mode_date_symbol_without_mutation(monkeypatch):
    import dashboard_cockpit
    calls = []

    def load(*args):
        calls.append(args)
        return {"session_date": args[1], "mode": args[2], "symbol": args[3]}

    monkeypatch.setattr(dashboard_cockpit, "load_cockpit_data", load)
    handler = Handler("/api/cockpit?date=2026-10-09&mode=LIVE&symbol=INFY")
    handler.do_GET()
    assert handler.status == 200
    assert calls == [(dashboard.FNO_OI_ROOT, "2026-10-09", "LIVE", "INFY")]
    assert json.loads(handler.wfile.getvalue())["mode"] == "LIVE"
    assert handler.response_headers["Cache-Control"] == "no-store"


def test_cockpit_route_shares_inflight_read_and_keeps_identity_keys_separate(monkeypatch):
    import dashboard_cockpit

    started, release = Event(), Event()
    calls = []

    def load(*args):
        calls.append(args)
        if len(calls) == 1:
            started.set()
            assert release.wait(3)
        return {"session_date": args[1], "mode": args[2], "symbol": args[3]}

    monkeypatch.setattr(dashboard_cockpit, "load_cockpit_data", load)
    path = "/api/cockpit?date=2026-10-09&mode=PAPER&symbol=INFY"
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(lambda: (handler := Handler(path), handler.do_GET(), handler)[2])
        assert started.wait(3)
        second = pool.submit(lambda: (handler := Handler(path), handler.do_GET(), handler)[2])
        release.set()
        first, second = first.result(timeout=3), second.result(timeout=3)
    assert first.status == second.status == 200
    assert first.wfile.getvalue() == second.wfile.getvalue()
    assert len(calls) == 1

    for path in ("/api/cockpit?date=2026-10-09&mode=LIVE&symbol=INFY",
                 "/api/cockpit?date=2026-10-09&mode=PAPER&symbol=TCS"):
        handler = Handler(path)
        handler.do_GET()
        assert handler.status == 200
    assert [(args[1], args[2], args[3]) for args in calls] == [
        ("2026-10-09", "PAPER", "INFY"), ("2026-10-09", "LIVE", "INFY"),
        ("2026-10-09", "PAPER", "TCS")]


def test_snapshot_route_reuses_result_then_refreshes_after_ttl(monkeypatch):
    calls = []

    def snapshot(self, *, lines):
        calls.append(lines)
        return {"items": [], "revision": len(calls)}

    monkeypatch.setattr(dashboard.LogDashboardHandler, "_snapshot", snapshot)
    for expected in (1, 1):
        handler = Handler("/api/snapshot?lines=80")
        handler.do_GET()
        assert handler.status == 200
        assert json.loads(handler.wfile.getvalue())["revision"] == expected
        assert handler.response_headers["Cache-Control"] == "no-store"
    assert calls == [80]

    with dashboard._DASH_READ_LOCK:
        for key, (completed, ttl, payload) in list(dashboard._DASH_READ_CACHE.items()):
            dashboard._DASH_READ_CACHE[key] = (completed - ttl - 1, ttl, payload)
    handler = Handler("/api/snapshot?lines=80")
    handler.do_GET()
    assert json.loads(handler.wfile.getvalue())["revision"] == 2
    handler = Handler("/api/snapshot?lines=40")
    handler.do_GET()
    assert json.loads(handler.wfile.getvalue())["revision"] == 3
    assert calls == [80, 80, 40]


def test_snapshot_route_shares_inflight_read(monkeypatch):
    started, release = Event(), Event()
    calls = []

    def snapshot(self, *, lines):
        calls.append(lines)
        started.set()
        assert release.wait(3)
        return {"items": [], "revision": len(calls)}

    def request():
        handler = Handler("/api/snapshot?lines=80")
        handler.do_GET()
        return handler

    monkeypatch.setattr(dashboard.LogDashboardHandler, "_snapshot", snapshot)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(request)
        assert started.wait(3)
        second = pool.submit(request)
        release.set()
        first, second = first.result(timeout=3), second.result(timeout=3)
    assert first.status == second.status == 200
    assert first.wfile.getvalue() == second.wfile.getvalue()
    assert calls == [80]


@pytest.mark.parametrize("query", ["date=2026-10-09&date=2026-10-08", "mode=PAPER&mode=LIVE", "symbol=INFY&symbol=TCS"])
def test_ambiguous_query_rejected_before_loading(monkeypatch, query):
    import dashboard_cockpit
    calls = []
    monkeypatch.setattr(dashboard_cockpit, "load_cockpit_data", lambda *args: calls.append(args))
    handler = Handler("/api/cockpit?" + query)
    handler.do_GET()
    assert handler.status == 400
    assert calls == []


@pytest.mark.parametrize("exc,status", [(ValueError("invalid"), 400), (OSError("private filesystem detail"), 503)])
def test_failed_reader_does_not_expose_internal_paths(monkeypatch, exc, status):
    import dashboard_cockpit

    def fail(*args):
        raise exc

    monkeypatch.setattr(dashboard_cockpit, "load_cockpit_data", fail)
    handler = Handler("/api/cockpit")
    handler.do_GET()
    assert handler.status == status
    assert b"private filesystem detail" not in handler.wfile.getvalue()


def test_terminal_keeps_operations_and_safely_embeds_assets():
    handler = Handler("/")
    handler.server.api_token = "</script>"
    handler.do_GET()
    html = handler.wfile.getvalue().decode()
    assert handler.status == 200
    assert 'id="liveCockpit"' in html
    assert 'id="terminalOperations"' in html
    assert 'id="restartAllBtn"' in html
    assert 'id="cards"' in html
    assert "DashboardCockpit.mount" in html
    assert "__COCKPIT_" not in html
    assert "__TERMINAL_" not in html
    assert '"</script>"' not in html
    assert "\\u003c/script>" in html
