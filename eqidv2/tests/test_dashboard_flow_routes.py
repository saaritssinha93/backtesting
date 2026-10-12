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


def test_live_flow_requires_auth_before_reading(monkeypatch):
    import dashboard_flow_live
    calls = []
    monkeypatch.setattr(dashboard_flow_live, "load_live_flow", lambda *args: calls.append(args))
    handler = Handler("/api/dashboard-flow/live", authorized=False)
    handler.do_GET()
    assert handler.status == 401
    assert calls == []


def test_live_flow_date_route_and_cache_policy(monkeypatch):
    import dashboard_flow_live
    monkeypatch.setattr(dashboard_flow_live, "load_live_flow", lambda root, day: {"session_date": day, "read_only": True})
    handler = Handler("/api/dashboard-flow/live?date=2026-10-09")
    handler.do_GET()
    assert handler.status == 200
    assert json.loads(handler.wfile.getvalue())["session_date"] == "2026-10-09"
    assert handler.response_headers["Cache-Control"] == "no-store"


def test_live_flow_rejects_duplicate_dates(monkeypatch):
    import dashboard_flow_live
    monkeypatch.setattr(dashboard_flow_live, "load_live_flow", lambda *args: (_ for _ in ()).throw(AssertionError("Must not load")))
    handler = Handler("/api/dashboard-flow/live?date=2026-10-09&date=2026-10-12")
    handler.do_GET()
    assert handler.status == 400


def test_live_flow_source_error_does_not_expose_paths(monkeypatch):
    import dashboard_flow_live
    monkeypatch.setattr(dashboard_flow_live, "load_live_flow", lambda *args: (_ for _ in ()).throw(OSError("private/path")))
    handler = Handler("/api/dashboard-flow/live")
    handler.do_GET()
    assert handler.status == 503
    assert b"private/path" not in handler.wfile.getvalue()


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


def test_flow_preserves_historical_families_when_active_profile_is_g3(monkeypatch):
    from fno_strategy_registry import PROFILES

    monkeypatch.setattr(dashboard, "FNO_ACTIVE_PROFILE", PROFILES["V13_V10_G_3"])
    handler = Handler("/dashboard-flow")
    handler.do_GET()
    html = handler.wfile.getvalue().decode()

    assert handler.status == 200
    # Shared CSS contains main-dashboard selectors, but must never trigger
    # active-profile substitutions in this historical comparison workspace.
    assert "body.terminal-main" in html
    assert "{strategy:'V13-V10-G',label:'G'," in html
    assert "{strategy:'V13-V10-G-2',label:'G-2'," in html
    assert "{strategy:'V13-V10-G-3',label:'G-3'," in html
    compare_asset = (dashboard.BASE_DIR / "dashboard_flow_compare.js").read_text(encoding="utf-8-sig")
    assert compare_asset in html
