"""Dashboard Flow remains inside the existing read-only authentication boundary."""
import base64
import io
import json
from types import SimpleNamespace

import log_dashboard_server as dashboard


class Handler(dashboard.LogDashboardHandler):
    def __init__(self, path, authorized=True):
        self.path = path
        self.server = SimpleNamespace(username="flow", password="test", api_token="")
        self.headers = {}
        if authorized:
            self.headers["Authorization"] = "Basic " + base64.b64encode(b"flow:test").decode()
        self.wfile = io.BytesIO()
        self.response_headers = {}
        self.status = None

    def send_response(self, code, message=None):
        self.status = int(code)

    def send_header(self, name, value):
        self.response_headers[name] = value

    def end_headers(self):
        pass


def test_flow_api_requires_auth_before_reading(monkeypatch):
    import dashboard_flow
    calls = []
    monkeypatch.setattr(dashboard_flow, "load_flow_data", lambda *args: calls.append(args))
    handler = Handler("/api/dashboard-flow", authorized=False)
    handler.do_GET()
    assert handler.status == 401
    assert calls == []


def test_flow_page_requires_auth():
    handler = Handler("/dashboard-flow", authorized=False)
    handler.do_GET()
    assert handler.status == 401


def test_flow_api_passes_only_selected_run(monkeypatch):
    import dashboard_flow
    monkeypatch.setattr(dashboard_flow, "load_flow_data", lambda run: {"selected": run})
    handler = Handler("/api/dashboard-flow?run=g3-example")
    handler.do_GET()
    assert handler.status == 200
    assert json.loads(handler.wfile.getvalue()) == {"selected": "g3-example"}
    assert handler.response_headers["Cache-Control"] == "no-store"


def test_flow_api_rejects_unknown_run(monkeypatch):
    import dashboard_flow
    def invalid(run):
        raise ValueError("Unknown dashboard run.")
    monkeypatch.setattr(dashboard_flow, "load_flow_data", invalid)
    handler = Handler("/api/dashboard-flow?run=../../private")
    handler.do_GET()
    assert handler.status == 400
    assert json.loads(handler.wfile.getvalue()) == {"error": "Unknown dashboard run."}


def test_flow_html_inlines_assets_and_safely_embeds_token():
    handler = Handler("/dashboard-flow")
    handler.server.api_token = "</script>"
    handler.do_GET()
    html = handler.wfile.getvalue().decode()
    assert handler.status == 200
    assert "Dashboard Flow" in html
    assert "__FLOW_" not in html
    assert "__API_TOKEN_JSON__" not in html
    assert '"</script>"' not in html
    assert "\\u003c/script>" in html
