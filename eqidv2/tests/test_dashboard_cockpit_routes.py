"""The monitoring cockpit reuses the existing authenticated read boundary."""
import base64
import io
import json
from types import SimpleNamespace

import pytest
import log_dashboard_server as dashboard


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
