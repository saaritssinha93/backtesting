"""Read-only stock-detail integration contract for the FnO monitoring card."""

from __future__ import annotations

import base64
import gzip
import io
import json
import re
import shutil
import subprocess
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

import log_dashboard_server as dashboard


DAY = "2026-10-07"
NOW = datetime(2026, 10, 7, 10, 1, tzinfo=dashboard.IST)


class _MemoryHandler(dashboard.LogDashboardHandler):
    """Run routing/authentication without opening a socket or logging secrets."""

    def __init__(self, path: str, *, authorized: bool = True) -> None:
        self.path = path
        self.server = SimpleNamespace(
            username="test-monitor-user", password="test-monitor-password", api_token=""
        )
        self.headers = {}
        if authorized:
            credentials = base64.b64encode(
                b"test-monitor-user:test-monitor-password"
            ).decode("ascii")
            self.headers["Authorization"] = f"Basic {credentials}"
        self.wfile = io.BytesIO()
        self.status = None
        self.response_headers = {}

    def send_response(self, code, message=None) -> None:
        self.status = int(code)

    def send_header(self, name, value) -> None:
        self.response_headers[name] = value

    def end_headers(self) -> None:
        pass

    def send_error(self, code, message=None, explain=None) -> None:
        self.status = int(code)
        self.wfile.write(str(message or "").encode("utf-8"))

    def json_body(self) -> dict:
        return json.loads(self.wfile.getvalue().decode("utf-8"))


def _payload(day: str = DAY) -> dict:
    return {
        "session_date": day,
        "generated_at_ist": NOW.isoformat(),
        "state": "READY",
        "warnings": [],
        "coverage": {},
        "rows_5m": [],
        "rows_1m": [],
    }


@pytest.fixture
def empty_cache(monkeypatch):
    cache = {}
    monkeypatch.setattr(dashboard, "_FNO_MONITOR_DETAIL_CACHE", cache)
    return cache


def test_monitor_endpoint_requires_authentication_before_loading_evidence(monkeypatch):
    calls = []
    monkeypatch.setattr(
        dashboard, "_fno_eq_id_stock_detail", lambda *args, **kwargs: calls.append(args)
    )
    handler = _MemoryHandler(f"/api/fno-monitor?date={DAY}", authorized=False)

    handler.do_GET()

    assert handler.status == HTTPStatus.UNAUTHORIZED
    assert calls == []
    assert "WWW-Authenticate" in handler.response_headers
    assert b"Authentication required" in handler.wfile.getvalue()


def test_monitor_endpoint_returns_requested_day_and_uncached_http_response(monkeypatch):
    calls = []
    expected = _payload()

    def build(day, **kwargs):
        calls.append(day)
        return expected

    monkeypatch.setattr(dashboard, "_fno_eq_id_stock_detail", build)
    handler = _MemoryHandler(f"/api/fno-monitor?date={DAY}")

    handler.do_GET()

    assert handler.status == HTTPStatus.OK
    assert calls == [DAY]
    assert handler.json_body() == expected
    assert handler.response_headers["Cache-Control"] == "no-store"
    assert handler.response_headers["Content-Type"].startswith("application/json")


@pytest.mark.parametrize("accept, compressed", [("gzip, deflate, br", True),
    ("gzip;q=0.5", True), ("gzip;q=0", False), ("br", False), ("gzip;q=bad", False)])
def test_large_monitor_response_compression_preserves_evidence_and_auth(monkeypatch, accept, compressed):
    expected = {**_payload(), "rows_5m": [{"evidence": "observed >= required " * 100}] * 10}
    monkeypatch.setattr(dashboard, "_fno_eq_id_stock_detail", lambda *args, **kwargs: expected)
    handler = _MemoryHandler(f"/api/fno-monitor?date={DAY}")
    handler.headers["Accept-Encoding"] = accept
    handler.do_GET()
    raw = handler.wfile.getvalue()
    assert (handler.response_headers.get("Content-Encoding") == "gzip") is compressed
    assert int(handler.response_headers["Content-Length"]) == len(raw)
    assert handler.response_headers["Cache-Control"] == "no-store"
    assert handler.response_headers["Vary"] == "Accept-Encoding"
    assert json.loads(gzip.decompress(raw) if compressed else raw) == expected


def test_monitor_endpoint_default_day_is_today_ist(monkeypatch):
    calls = []

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz == dashboard.IST
            return NOW

    monkeypatch.setattr(dashboard, "dt", SimpleNamespace(datetime=FixedDateTime))
    monkeypatch.setattr(
        dashboard, "_fno_eq_id_stock_detail",
        lambda day, **kwargs: calls.append(day) or _payload(day),
    )
    handler = _MemoryHandler("/api/fno-monitor")

    handler.do_GET()

    assert handler.status == HTTPStatus.OK
    assert calls == [DAY]


def test_monitor_endpoint_rejects_ambiguous_duplicate_dates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        dashboard, "_fno_eq_id_stock_detail",
        lambda *args, **kwargs: calls.append(args) or _payload(),
    )
    handler = _MemoryHandler(f"/api/fno-monitor?date={DAY}&date=2026-10-06")

    handler.do_GET()

    assert handler.status == HTTPStatus.BAD_REQUEST
    assert calls == []


@pytest.mark.parametrize(
    "invalid_day",
    [
        "2026-2-07", "2026-02-30", "07-10-2026", "20261007",
        "../2026-10-07", "../../auth", "C:\\private\\secret", "2026-10-07/..",
        "2026-10-07T09:30:00", " 2026-10-07", "2026-10-07 ",
    ],
)
def test_invalid_or_pathlike_dates_are_rejected_before_builder(
    invalid_day, monkeypatch, empty_cache,
):
    calls = []
    monkeypatch.setattr(
        dashboard, "_build_fno_eq_id_stock_detail",
        lambda *args, **kwargs: calls.append(args) or _payload(),
    )
    handler = _MemoryHandler("/api/fno-monitor?" + urlencode({"date": invalid_day}))

    handler.do_GET()

    assert handler.status == HTTPStatus.BAD_REQUEST
    assert calls == []
    assert "error" in handler.json_body()
    assert empty_cache == {}


def test_monitor_endpoint_errors_do_not_expose_credentials_or_tracebacks(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("access_token=DO_NOT_EXPOSE; C:\\private\\credentials.json")

    monkeypatch.setattr(dashboard, "_fno_eq_id_stock_detail", broken)
    handler = _MemoryHandler(f"/api/fno-monitor?date={DAY}")

    handler.do_GET()

    assert handler.status == HTTPStatus.SERVICE_UNAVAILABLE
    body = handler.wfile.getvalue().decode("utf-8")
    assert "temporarily unavailable" in body
    for forbidden in ("DO_NOT_EXPOSE", "access_token", "credentials.json", "Traceback"):
        assert forbidden not in body


def test_monitor_endpoint_has_no_post_or_trade_execution_route(monkeypatch):
    calls = []
    monkeypatch.setattr(
        dashboard, "_fno_eq_id_stock_detail", lambda *args, **kwargs: calls.append(args)
    )
    handler = _MemoryHandler(f"/api/fno-monitor?date={DAY}")

    handler.do_POST()

    assert handler.status == HTTPStatus.NOT_FOUND
    assert calls == []


def test_detail_helper_uses_configured_root_day_and_now(tmp_path, monkeypatch, empty_cache):
    calls = []
    root = tmp_path / "fno_oi"
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", root)

    def build(fno_root, session_date, *, now_ist=None):
        calls.append((fno_root, session_date, now_ist))
        return _payload(session_date)

    monkeypatch.setattr(dashboard, "_build_fno_eq_id_stock_detail", build)

    result = dashboard._fno_eq_id_stock_detail(DAY, now_ist=NOW)

    assert result["session_date"] == DAY
    assert calls == [(root, DAY, NOW)]


def test_detail_cache_reuses_current_payload_then_expires(tmp_path, monkeypatch, empty_cache):
    tick = [100.0]
    calls = []
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", tmp_path)
    monkeypatch.setattr(dashboard.time, "monotonic", lambda: tick[0])

    def build(root, day, **kwargs):
        calls.append((root, day))
        return {**_payload(day), "build_number": len(calls)}

    monkeypatch.setattr(dashboard, "_build_fno_eq_id_stock_detail", build)

    first = dashboard._fno_eq_id_stock_detail(DAY)
    tick[0] = 104.9
    second = dashboard._fno_eq_id_stock_detail(DAY)
    tick[0] = 105.1
    third = dashboard._fno_eq_id_stock_detail(DAY)

    assert first == second
    assert third["build_number"] == 2
    assert len(calls) == 2
    assert len(empty_cache) == 1


def test_detail_cache_is_root_scoped_and_capped_at_four_dates(
    tmp_path, monkeypatch, empty_cache,
):
    tick = [100.0]
    calls = []
    monkeypatch.setattr(dashboard.time, "monotonic", lambda: tick[0])
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", tmp_path / "one")

    def build(root, day, **kwargs):
        calls.append((root, day))
        return _payload(day)

    monkeypatch.setattr(dashboard, "_build_fno_eq_id_stock_detail", build)
    dashboard._fno_eq_id_stock_detail(DAY)
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", tmp_path / "two")
    dashboard._fno_eq_id_stock_detail(DAY)
    assert len(calls) == 2
    for date in ("2026-10-06", "2026-10-05", "2026-10-02", "2026-09-30"):
        tick[0] += 0.1
        dashboard._fno_eq_id_stock_detail(date)

    assert len(empty_cache) <= 4


def test_detail_builder_failure_is_not_cached(tmp_path, monkeypatch, empty_cache):
    calls = []
    monkeypatch.setattr(dashboard, "FNO_OI_ROOT", tmp_path)

    def build(root, day, **kwargs):
        calls.append(day)
        if len(calls) == 1:
            raise OSError("source temporarily incomplete")
        return _payload(day)

    monkeypatch.setattr(dashboard, "_build_fno_eq_id_stock_detail", build)
    with pytest.raises(OSError):
        dashboard._fno_eq_id_stock_detail(DAY)
    assert empty_cache == {}
    result = dashboard._fno_eq_id_stock_detail(DAY)

    assert result["session_date"] == DAY
    assert calls == [DAY, DAY]


def test_rendered_dashboard_embeds_monitor_assets_and_readonly_panel():
    handler = _MemoryHandler("/")

    handler._send_html()

    html = handler.wfile.getvalue().decode("utf-8")
    assert "__FNO_MONITOR_JS__" not in html
    assert "__FNO_MONITOR_CSS__" not in html
    assert "FnoMonitor.mount" in html
    assert "fno-stock-monitor" in html
    assert "data-read-only" in html
    assert "/api/fno-monitor" in html
    assert '"v7_live_5min_monitor": "FnO EQ ID monitoring"' in html
    for field in ("date", "stock", "slot", "side", "result", "stage"):
        assert f'data-filter="{field}"' in html


def test_detail_browser_script_has_no_trade_or_process_write_endpoints():
    script = (Path(dashboard.__file__).parent / "dashboard_fno_monitor.js").read_text(
        encoding="utf-8"
    )

    for endpoint in ("/api/kill", "/api/restart", "/orders", "/api/trade"):
        assert endpoint not in script
    assert "method: 'POST'" not in script
    assert 'method: "POST"' not in script


def test_rendered_dashboard_inline_javascript_has_valid_syntax():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to check rendered dashboard JavaScript")
    handler = _MemoryHandler("/")
    handler._send_html()
    html = handler.wfile.getvalue().decode("utf-8")
    scripts = re.findall(r"<script\b[^>]*>(.*?)</script>", html, flags=re.DOTALL | re.I)
    assert scripts

    result = subprocess.run(
        [node, "--check"], input="\n".join(scripts), encoding="utf-8",
        capture_output=True, timeout=30, check=False,
    )

    assert result.returncode == 0, result.stderr


def _run_monitor_browser_scenario(scenario: str) -> dict:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required to evaluate the read-only monitoring UI")
    source = (Path(dashboard.__file__).parent / "dashboard_fno_monitor.js").read_text(
        encoding="utf-8"
    )
    scaffold = """
      global.window = {location: {origin: 'http://127.0.0.1:8787'}};
      global.document = {activeElement: null};
      const flush = () => new Promise(resolve => setImmediate(resolve));
      function fakeHost() {
        const zones = Object.fromEntries(['controls', 'status', 'warnings', 'summary',
          'rows', 'pager', 'detail', 'coverage'].map(name => [name, {
            innerHTML: '', textContent: '', hidden: false, scrollIntoView() {}
          }]));
        return {zones, listeners: {}, isConnected: true, innerHTML: '',
          querySelector(selector) { return zones[selector.match(/data-zone=([^\\]]+)/)[1]]; },
          addEventListener(type, fn) { this.listeners[type] = fn; },
          contains() { return true; }, closest() { return null; }
        };
      }
      function click(host, dataset) {
        const button = {dataset};
        host.listeners.click({target: {closest() { return button; }}});
      }
    """
    script = scaffold + source + "\n(async () => {\n" + scenario + "\n})().catch(error => { console.error(error); process.exitCode = 1; });"
    result = subprocess.run(
        [node, "-"], input=script, encoding="utf-8", capture_output=True,
        timeout=30, check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_monitor_ui_filters_paginates_escapes_evidence_and_preserves_filter_on_remount():
    result = _run_monitor_browser_scenario("""
      const calls = [];
      const rows = Array.from({length: 102}, (_, index) => ({
        id: String(index), symbol: 'STOCK' + String(index).padStart(3, '0'),
        signal_time: '09:45', side: 'LONG', stage: '5m', setup_id: '0946_LONG',
        decision: '<script>unsafe()</script>', indicators: {price_change_pct: 0.7},
        checks: [{name: 'test_guard', status: 'PASS', actual: '<img src=x onerror=unsafe()>', rule: '> 0'}]
      }));
      global.fetch = async (url, options) => {
        calls.push({url: String(url), method: options.method});
        return {ok: true, json: async () => ({session_date: url.searchParams.get('date'),
          state: 'READY', warnings: [], coverage: [], rows_5m: rows, rows_1m: []})};
      };
      const host = fakeHost();
      window.FnoMonitor.mount(host, 'test-token');
      await flush(); await flush();
      const initialRows = host.zones.rows.innerHTML;
      click(host, {action: 'next'});
      const secondPage = host.zones.rows.innerHTML;
      host.listeners.input({target: {dataset: {filter: 'stock'}, value: 'STOCK099'}});
      const filtered = host.zones.rows.innerHTML;
      click(host, {rowIndex: '0'});
      const expanded = host.zones.detail.innerHTML;
      const secondHost = fakeHost();
      host.isConnected = false;
      window.FnoMonitor.mount(secondHost, 'test-token');
      await flush();
      process.stdout.write(JSON.stringify({initialRows, secondPage, filtered, expanded,
        remounted: secondHost.zones.rows.innerHTML, controls: secondHost.zones.controls.innerHTML,
        calls}));
    """)
    assert result["initialRows"].count("data-row-index=") == 50
    assert 'data-row-index="50"' in result["secondPage"]
    assert result["filtered"].count("data-row-index=") == 1
    assert "STOCK099" in result["filtered"]
    assert "STOCK099" in result["remounted"]
    assert 'value="STOCK099"' in result["controls"]
    assert "<script>unsafe()" not in result["initialRows"]
    assert "&lt;script&gt;unsafe()&lt;/script&gt;" in result["initialRows"]
    assert "<img src=x" not in result["expanded"]
    assert "&lt;img src=x" in result["expanded"]
    assert len(result["calls"]) == 1
    assert result["calls"][0]["method"] == "GET"
    assert result["calls"][0]["url"].startswith("http://127.0.0.1:8787/api/fno-monitor?")


def test_monitor_ui_can_be_removed_while_evidence_request_is_pending():
    result = _run_monitor_browser_scenario("""
      const unhandled = [];
      process.on('unhandledRejection', error => unhandled.push(String(error)));
      let resolveFetch, requestedDate;
      global.fetch = url => {
        requestedDate = url.searchParams.get('date');
        return new Promise(resolve => { resolveFetch = resolve; });
      };
      const host = fakeHost();
      window.FnoMonitor.mount(host, '');
      host.isConnected = false;
      window.FnoMonitor.mount(null, '');
      resolveFetch({ok: true, json: async () => ({session_date: requestedDate,
        state: 'READY', warnings: [], coverage: [], rows_5m: [], rows_1m: []})});
      await flush(); await flush();
      process.stdout.write(JSON.stringify({unhandled}));
    """)
    assert result["unhandled"] == []


def test_gate_filter_keeps_confirmation_stage_and_final_selection_separate():
    result = _run_monitor_browser_scenario("""
      const common = {signal_time: '09:45', side: 'LONG', setup_id: '0946_LONG', indicators: {}};
      const rows5m = [
        {...common, id: 'five-pass', symbol: 'FIVE_PASS', stage: '5m', decision: 'FILTER_PASS',
          checks: [{name: 'price_change', status: 'PASS'}, {name: 'ema_alignment', status: 'PASS'},
            {name: 'confirmation_stage', status: 'NOT_EVALUATED'}]},
        {...common, id: 'five-fail', symbol: 'FIVE_FAIL', stage: '5m', decision: 'FILTER_FAIL',
          checks: [{name: 'price_change', status: 'FAIL'},
            {name: 'confirmation_stage', status: 'NOT_EVALUATED'}]}
      ];
      const rows1m = [
        {...common, id: 'one-pass', symbol: 'ONE_PASS', stage: '1m', minute: '09:46',
          decision: 'FILTER_PASS_NOT_SELECTED',
          checks: [{name: 'body_ratio', status: 'PASS'}, {name: 'volume_ratio', status: 'PASS'},
            {name: 'final_selection', status: 'FAIL', reason: 'Lower rank; slot capacity filled'}]},
        {...common, id: 'one-fail', symbol: 'ONE_FAIL', stage: '1m', minute: '09:46',
          decision: 'FILTER_FAIL',
          checks: [{name: 'body_ratio', status: 'FAIL'}, {name: 'final_selection', status: 'FAIL'}]}
      ];
      global.fetch = async url => ({ok: true, json: async () => ({
        session_date: url.searchParams.get('date'), state: 'READY', warnings: [], coverage: [],
        rows_5m: rows5m, rows_1m: rows1m
      })});
      const host = fakeHost();
      window.FnoMonitor.mount(host, '');
      await flush(); await flush();
      const change = (filter, value) => host.listeners.change({target: {dataset: {filter}, value}});
      change('result', 'PASS');
      const fivePass = host.zones.rows.innerHTML;
      click(host, {rowIndex: '0'});
      const fiveDetails = host.zones.detail.innerHTML;
      change('stage', '1m');
      const onePass = host.zones.rows.innerHTML;
      click(host, {rowIndex: '0'});
      const oneDetails = host.zones.detail.innerHTML;
      change('result', 'FAIL');
      const oneFail = host.zones.rows.innerHTML;
      process.stdout.write(JSON.stringify({fivePass, fiveDetails, onePass, oneDetails, oneFail}));
    """)

    assert result["fivePass"].count("data-row-index=") == 1
    assert "FIVE_PASS" in result["fivePass"]
    assert "FIVE_FAIL" not in result["fivePass"]
    assert 'class="fno-detail-pill pass">PASS<' in result["fivePass"]
    assert "confirmation stage" in result["fiveDetails"]
    assert "NOT_EVALUATED" in result["fiveDetails"]
    assert result["onePass"].count("data-row-index=") == 1
    assert "ONE_PASS" in result["onePass"]
    assert "ONE_FAIL" not in result["onePass"]
    assert "FILTER_PASS_NOT_SELECTED" in result["onePass"]
    assert 'class="fno-detail-pill pass">PASS<' in result["onePass"]
    assert "final selection" in result["oneDetails"]
    assert 'class="fno-detail-pill fail">FAIL<' in result["oneDetails"]
    assert "Lower rank; slot capacity filled" in result["oneDetails"]
    assert result["oneFail"].count("data-row-index=") == 1
    assert "ONE_FAIL" in result["oneFail"]
    assert "ONE_PASS" not in result["oneFail"]


def test_failed_checks_show_values_thresholds_gaps_and_missing_checks_together():
    result = _run_monitor_browser_scenario("""
      const row = {id: 'volume-fail', symbol: 'TEST', signal_time: '09:25', minute: '09:26',
        side: 'LONG', stage: '1m', setup_id: '0926_LONG', decision: 'CONFIRMATION_OR_SETUP_REJECTED',
        indicators: {}, checks: [
          {name: 'gate_confirmation_volume', label: '1m volume ratio', status: 'FAIL',
            actual: 0.7, rule: '>= 1.20x', margin: -0.5, actual_text: '0.70x',
            required_text: '>= 1.20x', margin_text: 'Shortfall: 0.50x',
            threshold_source: 'Pinned G source', margin_source: 'Recorded ledger margin'},
          {name: 'gate_setup_oi', label: 'Setup OI change', status: 'UNKNOWN',
            actual: null, actual_text: 'Not recorded', required_text: '>= 0.10%',
            reason: 'OI observation unavailable'},
          {name: 'gate_exact_confirmation_clock', status: 'NOT_EVALUATED',
            reason: 'Stage has not run'},
          {name: 'final_selection', status: 'FAIL', reason: 'No selection recorded'}
        ]};
      global.fetch = async url => ({ok: true, json: async () => ({session_date: url.searchParams.get('date'),
        rows_5m: [row], rows_1m: [], warnings: [], coverage: []})});
      const host = fakeHost(); window.FnoMonitor.mount(host, ''); await flush(); await flush();
      const rendered = host.zones.rows.innerHTML;
      click(host, {rowIndex: '0'});
      process.stdout.write(JSON.stringify({rendered, details: host.zones.detail.innerHTML}));
    """)
    rendered = result["rendered"]
    assert "Failed (1)" in rendered  # Selection is not an indicator failure.
    assert "1m volume ratio</span>: 0.70x" in rendered
    assert "Required: &gt;= 1.20x" in rendered
    assert "Shortfall: 0.50x" in rendered
    assert "Missing / unverified evidence (1)" in rendered
    assert "Setup OI change</span>: Not recorded" in rendered
    assert "OI observation unavailable" in rendered
    assert "Not evaluated (1)" in rendered
    assert "Not selected (separate from filter checks)" in rendered
    assert "Shortfall / margin" in result["details"]
    assert "Threshold source: Pinned G source" in result["details"]
    assert "Margin source: Recorded ledger margin" in result["details"]


def test_formatted_check_fields_remain_escaped_and_preserve_exact_operator():
    result = _run_monitor_browser_scenario("""
      const row = {id: 'escape', symbol: 'TEST', signal_time: '09:25', side: 'LONG',
        stage: '5m', indicators: {}, checks: [{name: 'gate_ema_long', status: 'FAIL',
          label: '<img src=x onerror=bad()>', actual_text: 'EMA9 100; EMA20 100; EMA50 99',
          required_text: 'EMA9 > EMA20 > EMA50', margin_text: 'Equal to strict boundary',
          evidence_note: '<script>bad()</script>', threshold_source: '<svg onload=bad()>'}]};
      global.fetch = async url => ({ok: true, json: async () => ({session_date: url.searchParams.get('date'),
        rows_5m: [row], rows_1m: []})});
      const host = fakeHost(); window.FnoMonitor.mount(host, ''); await flush(); await flush();
      click(host, {rowIndex: '0'});
      process.stdout.write(JSON.stringify({rendered: host.zones.rows.innerHTML, details: host.zones.detail.innerHTML}));
    """)
    for text in result.values():
        assert "<img" not in text and "<script>" not in text and "<svg" not in text
        assert "EMA9 &gt; EMA20 &gt; EMA50" in text
        assert "Equal to strict boundary" in text
        assert "&lt;script&gt;bad()&lt;/script&gt;" in text
    assert "&gt;=" not in result["rendered"]


def test_legacy_and_absent_check_data_do_not_invent_numeric_thresholds():
    result = _run_monitor_browser_scenario("""
      const rows = [
        {id: 'legacy', symbol: 'LEGACY', checks: [{name: 'legacy_check', status: 'FAIL',
          actual: 0, rule: 'Recorded rule', margin: 0}]},
        {id: 'absent', symbol: 'ABSENT', checks: [{name: 'missing_check', status: 'UNKNOWN'}]},
        {id: 'none', symbol: 'NONE', checks: []}
      ];
      global.fetch = async url => ({ok: true, json: async () => ({session_date: url.searchParams.get('date'),
        rows_5m: rows, rows_1m: []})});
      const host = fakeHost(); window.FnoMonitor.mount(host, ''); await flush(); await flush();
      process.stdout.write(JSON.stringify({rendered: host.zones.rows.innerHTML}));
    """)
    assert "legacy check</span>: 0" in result["rendered"]
    assert "Recorded margin: 0" in result["rendered"]
    assert "missing check</span>: Not recorded" in result["rendered"]
    assert "Required: Not recorded" in result["rendered"]
    assert "No per-check evidence recorded." in result["rendered"]


def test_monitor_renders_actual_backend_comparison_payload():
    from fno_eq_id_monitor_detail import _gate

    evidence = _gate({"base_side": "LONG", "gate_setup_volume": False,
        "volume_ratio": .70, "required_volume_ratio": 1.20, "margin_volume_ratio": -.50},
        "gate_setup_volume", "1m")
    scenario = """
      const row = {id: 'backend', symbol: 'TEST', signal_time: '09:25', side: 'LONG',
        stage: '1m', checks: [__CHECK__]};
      global.fetch = async url => ({ok: true, json: async () => ({session_date: url.searchParams.get('date'),
        rows_5m: [row], rows_1m: []})});
      const host = fakeHost(); window.FnoMonitor.mount(host, ''); await flush(); await flush();
      click(host, {rowIndex: '0'});
      process.stdout.write(JSON.stringify({rendered: host.zones.rows.innerHTML, details: host.zones.detail.innerHTML}));
    """.replace("__CHECK__", json.dumps(evidence))
    result = _run_monitor_browser_scenario(scenario)
    for rendered in result.values():
        assert "0.70×" in rendered
        assert "&gt;= 1.20×" in rendered
        assert "0.50×" in rendered
        assert "shortfall" in rendered.lower()
