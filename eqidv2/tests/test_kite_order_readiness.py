"""Readiness checks use only fake HTTPS transports and disposable credentials."""
from __future__ import annotations

import importlib.util
import io
import json
from datetime import datetime, timezone
from http.client import HTTPException
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

import pytest

SOURCE = Path(__file__).resolve().parents[1] / "tools/test_kite_order_readiness.py"
SPEC = importlib.util.spec_from_file_location("readiness_under_test", SOURCE)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)

SAFE_ROUTES = {
    ("GET", "/user/profile"), ("GET", "/user/margins/equity"),
    ("GET", "/orders"), ("GET", "/portfolio/positions"),
    ("GET", "/quote/ltp"), ("POST", "/margins/orders"),
}
PRIVATE = ("PRIVATE_USER_ID", "PRIVATE_NAME", "PRIVATE_EMAIL", "PRIVATE_SYMBOL", "PRIVATE_ORDER_ID")


@pytest.fixture(autouse=True)
def prohibit_real_transport(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("A real network opener must never be built in readiness tests")
    monkeypatch.setattr(probe, "build_opener", refuse)


class Reply:
    def __init__(self, body):
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
    def __enter__(self):
        return self
    def __exit__(self, *_args):
        return False
    def read(self, count):
        return self.body[:count]


class FakeOpener:
    """Independent allowlist: a tool regression cannot reach mutation endpoints."""
    def __init__(self, routes=None):
        self.calls = []
        self.routes = {
            ("GET", "/user/profile"): {"exchanges": ["NSE"], "products": ["MIS", "CNC"],
                "order_types": ["LIMIT", "MARKET", "SL", "SL-M"],
                "user_id": PRIVATE[0], "user_name": PRIVATE[1], "email": PRIVATE[2]},
            ("GET", "/user/margins/equity"): {"enabled": True, "available": {"live_balance": 123456.78}},
            ("GET", "/orders"): [{"order_id": PRIVATE[4], "tradingsymbol": PRIVATE[3], "status": "COMPLETE"},
                                      {"order_id": "PRIVATE_PENDING_ID", "status": "OPEN"}],
            ("GET", "/portfolio/positions"): {"net": [{"quantity": 0, "tradingsymbol": PRIVATE[3]},
                                                           {"quantity": -2, "tradingsymbol": PRIVATE[3]}]},
            ("POST", "/margins/orders"): [{"total": 500}],
        }
        self.routes.update(routes or {})
    def open(self, request, timeout):
        url = urlparse(request.full_url)
        route = (request.get_method(), url.path)
        assert url.scheme == "https" and url.netloc == "api.kite.trade"
        assert route in SAFE_ROUTES, f"Mutation or unexpected endpoint attempted: {route}"
        self.calls.append((route, request, timeout))
        payload = self.routes[route]
        if isinstance(payload, Exception):
            raise payload
        if isinstance(payload, bytes):
            return Reply(payload)
        return Reply({"status": "success", "data": payload})


def credentials(root, app="app1"):
    suffix = "" if app == "app1" else app[3:]
    (root / f"api_key{suffix}.txt").write_text(f"key_{app} PRIVATE_API_SECRET PRIVATE_LOGIN PRIVATE_PASSWORD PRIVATE_TOTP")
    (root / f"access_token{suffix}.txt").write_text(f"token_{app}\n")


def factory(opener):
    return lambda key, token, **kwargs: probe.ReadOnlyKite(key, token, opener=opener, **kwargs)


def inspect(root, opener, *, order=None, app="app1"):
    return probe.inspect_app(app, root=root, proposed_order=order, client_factory=factory(opener), delay=0)


def preview(*flags):
    parser = probe.build_parser()
    return probe.proposed_order(parser.parse_args(list(flags)), parser)


def http_error(status, kind, message="PRIVATE_API_SECRET PRIVATE_USER_ID token_app1"):
    return HTTPError("https://api.kite.trade/user/profile", status, message, {},
                     io.BytesIO(json.dumps({"status": "error", "error_type": kind, "message": message}).encode()))


@pytest.mark.parametrize("method,path", [
    ("POST", "/orders/regular"), ("PUT", "/orders/regular/123"), ("DELETE", "/orders/regular/123"),
    ("DELETE", "/session/token"), ("POST", "/session/token"), ("POST", "/session/refresh_token"),
    ("GET", "https://other.invalid/user/profile"), ("GET", "/user/profile/../orders"),
])
def test_mutation_and_unlisted_routes_rejected_before_fake_transport(method, path):
    opener = FakeOpener()
    client = probe.ReadOnlyKite("key", "token", opener=opener)
    with pytest.raises(probe.ProbeError, match="ENDPOINT_NOT_ALLOWED"):
        client.request(method, path)
    assert opener.calls == []


def test_redirect_and_get_body_cannot_escape_read_transport():
    opener = FakeOpener()
    client = probe.ReadOnlyKite("key", "token", opener=opener)
    with pytest.raises(probe.ProbeError, match="REQUEST_NOT_ALLOWED"):
        client.request("GET", "/orders", payload={"quantity": 1})
    with pytest.raises(probe.ProbeError, match="REQUEST_NOT_ALLOWED"):
        client.request("GET", "/user/profile", query={"i": "NSE:TEST"})
    with pytest.raises(probe.ProbeError, match="REDIRECT_REFUSED"):
        probe.NoRedirect().redirect_request(None, None, 302, "PRIVATE_TOKEN", {}, "https://other.invalid/")
    assert opener.calls == []


@pytest.mark.parametrize("error,code", [
    (http_error(401, "TokenException"), "SESSION_INVALID_OR_EXPIRED"),
    (http_error(403, "PermissionException"), "ACCESS_DENIED"),
    (http_error(429, "RateException"), "RATE_LIMITED"),
    (http_error(500, "PRIVATE_API_SECRET"), "HTTP_ERROR_500"),
    (URLError("PRIVATE_API_SECRET token_app1"), "NETWORK_UNAVAILABLE"),
    (HTTPException("PRIVATE_API_SECRET token_app1"), "NETWORK_UNAVAILABLE"),
])
def test_transport_errors_only_return_sanitized_codes(error, code):
    opener = FakeOpener({("GET", "/user/profile"): error})
    client = probe.ReadOnlyKite("key", "token", opener=opener)
    with pytest.raises(probe.ProbeError) as caught:
        client.request("GET", "/user/profile")
    assert str(caught.value) == code


@pytest.mark.parametrize("raw,code", [
    (b"PRIVATE_API_SECRET not JSON", "INVALID_BROKER_RESPONSE"),
    (b"[]", "INVALID_BROKER_RESPONSE"),
    (b'{"status":"error","message":"PRIVATE_API_SECRET"}', "INVALID_BROKER_RESPONSE"),
    (b"x" * (8 * 1024 * 1024 + 1), "RESPONSE_TOO_LARGE"),
], ids=["invalid_json", "wrong_shape", "error_envelope", "oversized"])
def test_malformed_or_oversized_responses_do_not_escape(raw, code):
    client = probe.ReadOnlyKite("key", "token", opener=FakeOpener({("GET", "/user/profile"): raw}))
    with pytest.raises(probe.ProbeError) as caught:
        client.request("GET", "/user/profile")
    assert str(caught.value) == code


def test_expired_profile_stops_other_reads_and_optional_preview(tmp_path):
    credentials(tmp_path)
    opener = FakeOpener({("GET", "/user/profile"): http_error(401, "TokenException")})
    row = inspect(tmp_path, opener, order=preview("--symbol", "TEST", "--side", "BUY", "--order-type", "MARKET"))
    assert row["state"] == "SESSION_INVALID_OR_EXPIRED"
    assert [item[0] for item in opener.calls] == [("GET", "/user/profile")]
    assert row["orders_submitted"] == 0 and row["order_acceptance"] == "NOT_TESTED"


@pytest.mark.parametrize("profile", [{}, {"exchanges": None}, {"exchanges": "NSE", "products": [], "order_types": []},
                                       {"exchanges": [1], "products": [], "order_types": []}])
def test_malformed_profile_stops_without_uncaught_exception(tmp_path, profile):
    credentials(tmp_path)
    opener = FakeOpener({("GET", "/user/profile"): profile})
    row = inspect(tmp_path, opener)
    assert row["state"] == "INVALID_BROKER_RESPONSE" and len(opener.calls) == 1


@pytest.mark.parametrize("route,response,check", [
    ("/user/margins/equity", {}, "margins"),
    ("/user/margins/equity", {"enabled": "true", "available": {}}, "margins"),
    ("/orders", ["PRIVATE_BROKER_DATA"], "orders"),
    ("/portfolio/positions", {"net": [{"quantity": "NaN"}]}, "positions"),
    ("/portfolio/positions", {"net": [{"quantity": True}]}, "positions"),
    ("/portfolio/positions", {"net": [{"quantity": False}]}, "positions"),
    ("/portfolio/positions", {"net": [{}]}, "positions"),
])
def test_partial_read_access_never_becomes_full_success(tmp_path, route, response, check):
    credentials(tmp_path)
    opener = FakeOpener({("GET", route): response})
    row = inspect(tmp_path, opener)
    assert row["state"] == "PARTIAL_READ_ACCESS" and row["checks"][check] == "INVALID_BROKER_RESPONSE"
    assert len(opener.calls) == 4 and all(item[0][0] == "GET" for item in opener.calls)


def test_success_is_only_read_access_and_outputs_no_account_identity(tmp_path):
    credentials(tmp_path)
    opener = FakeOpener()
    row = inspect(tmp_path, opener)
    assert row["state"] == "READ_ACCESS_CONFIRMED"
    assert row["existing_orders_today"] == 2 and row["nonterminal_orders"] == 1 and row["open_net_positions"] == 1
    assert row["funds_positive"] is True and row["order_acceptance"] == "NOT_TESTED"
    serialized = json.dumps(row)
    assert all(secret not in serialized for secret in (*PRIVATE, "PRIVATE_PENDING_ID", "123456.78", "key_app1", "token_app1"))
    assert opener.calls[0][1].get_header("Authorization") == "token key_app1:token_app1"


@pytest.mark.parametrize("total,expected", [(0, "MARGIN_ESTIMATE_RECEIVED"), (100, "MARGIN_ESTIMATE_RECEIVED"),
                                           (True, "INVALID_MARGIN_RESPONSE"), (False, "INVALID_MARGIN_RESPONSE"),
                                           (-1, "INVALID_MARGIN_RESPONSE"), (None, "INVALID_MARGIN_RESPONSE"),
                                           ("NaN", "INVALID_MARGIN_RESPONSE"), ("Infinity", "INVALID_MARGIN_RESPONSE")])
def test_margin_estimates_including_zero_never_establish_order_acceptance(tmp_path, total, expected):
    credentials(tmp_path)
    opener = FakeOpener({("POST", "/margins/orders"): [{"total": total}]})
    proposed = preview("--symbol", "TEST", "--side", "BUY", "--quantity", "2", "--order-type", "LIMIT", "--price", "100")
    row = inspect(tmp_path, opener, order=proposed)
    assert row["order_preview"]["state"] == expected
    assert row["order_preview"]["order_acceptance"] == "NOT_TESTED" and row["orders_submitted"] == 0
    assert opener.calls[-1][0] == ("POST", "/margins/orders")
    assert json.loads(opener.calls[-1][1].data) == [proposed]
    assert len(opener.calls) == 5


@pytest.mark.parametrize("response", [None, {}, [], [{}, {}], ["PRIVATE_MARGIN_DATA"]])
def test_malformed_margin_response_cannot_be_marked_estimated(tmp_path, response):
    credentials(tmp_path)
    opener = FakeOpener({("POST", "/margins/orders"): response})
    proposed = preview("--symbol", "TEST", "--side", "SELL", "--order-type", "MARKET")
    row = inspect(tmp_path, opener, order=proposed)
    assert row["order_preview"]["state"] == "INVALID_MARGIN_RESPONSE"


def test_profile_capability_mismatch_skips_margin_request(tmp_path):
    credentials(tmp_path)
    opener = FakeOpener({("GET", "/user/profile"): {"exchanges": ["NSE"], "products": ["CNC"], "order_types": ["MARKET"]}})
    row = inspect(tmp_path, opener, order=preview("--symbol", "TEST", "--side", "BUY", "--order-type", "MARKET"))
    assert row["order_preview"]["state"] == "ACCOUNT_CAPABILITY_MISMATCH" and len(opener.calls) == 4


@pytest.mark.parametrize("balance", [True, False])
def test_boolean_balance_is_unknown_not_numeric_funds(tmp_path, balance):
    credentials(tmp_path)
    opener = FakeOpener({("GET", "/user/margins/equity"): {"enabled": True, "available": {"live_balance": balance}}})
    row = inspect(tmp_path, opener, order=preview("--symbol", "TEST", "--side", "BUY", "--order-type", "MARKET"))
    assert row["funds_positive"] is None
    assert row["order_preview"]["live_balance_covers_estimate"] is None
    assert row["order_acceptance"] == "NOT_TESTED"


@pytest.mark.parametrize("flags", [
    ["--confirm-live"], ["--activate-live"], ["--auto-arm"], ["--quantity", "0"], ["--quantity", "-1"],
    ["--symbol", "TEST"], ["--side", "BUY"], ["--exchange", "NFO"], ["--product", "NRML"],
    ["--symbol", "../TEST", "--side", "BUY"],
    ["--symbol", "TEST", "--side", "BUY", "--order-type", "LIMIT"],
    ["--symbol", "TEST", "--side", "BUY", "--order-type", "SL-M"],
    ["--symbol", "TEST", "--side", "BUY", "--order-type", "MARKET", "--price", "100"],
    ["--symbol", "TEST", "--side", "BUY", "--price", "NaN"],
    ["--symbol", "TEST", "--side", "BUY", "--order-type", "SL", "--price", "99", "--trigger-price", "100"],
    ["--symbol", "TEST", "--side", "SELL", "--order-type", "SL", "--price", "101", "--trigger-price", "100"],
])
def test_real_order_flags_and_invalid_preview_arguments_rejected(flags):
    with pytest.raises(SystemExit) as caught:
        preview(*flags)
    assert caught.value.code == 2


@pytest.mark.parametrize("kind,extra", [("LIMIT", ["--price", "100"]), ("MARKET", []),
    ("SL", ["--price", "101", "--trigger-price", "100"]), ("SL-M", ["--trigger-price", "100"])])
def test_supported_order_shapes_are_preview_payloads_only(kind, extra):
    order = preview("--symbol", "test", "--side", "buy", "--order-type", kind, *extra)
    assert order["tradingsymbol"] == "TEST" and order["transaction_type"] == "BUY"
    assert order["quantity"] == 1 and order["variety"] == "regular"
    assert preview() is None


def calendar(root):
    path = root / "configs/markets/nse_equity_calendar_2026.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(dict(schema_version="nse_equity_regular_session_calendar_v1", year=2026,
        checked_on="2026-10-12", source="https://example.invalid/fake-calendar", regular_open_ist="09:15",
        regular_close_ist="15:30", holidays=["2026-10-20"], special_sessions_unverified=["2026-11-08"])))
    return path


@pytest.mark.parametrize("clock,state,day", [
    ("2026-10-12T09:14:59+05:30", "BEFORE_REGULAR_SESSION", True),
    ("2026-10-12T09:15:00+05:30", "WITHIN_SCHEDULED_REGULAR_SESSION", True),
    ("2026-10-12T15:29:59+05:30", "WITHIN_SCHEDULED_REGULAR_SESSION", True),
    ("2026-10-12T15:30:00+05:30", "AFTER_REGULAR_SESSION", True),
    ("2026-10-11T10:00:00+05:30", "CLOSED_BY_PUBLISHED_CALENDAR", False),
    ("2026-10-20T10:00:00+05:30", "CLOSED_BY_PUBLISHED_CALENDAR", False),
    ("2026-11-08T18:00:00+05:30", "SPECIAL_SESSION_HOURS_UNVERIFIED", None),
    ("2027-10-12T10:00:00+05:30", "CALENDAR_UNVERIFIED", None),
    ("2026-10-12T03:45:00+00:00", "WITHIN_SCHEDULED_REGULAR_SESSION", True),
    ("2026-10-11T23:59:00+00:00", "BEFORE_REGULAR_SESSION", True),
    ("2026-10-12T09:15:00", "TIMEZONE_UNVERIFIED", None),
])
def test_market_schedule_timezone_and_boundaries_are_not_exchange_confirmation(tmp_path, clock, state, day):
    calendar(tmp_path)
    result = probe.market_schedule(datetime.fromisoformat(clock), root=tmp_path)
    assert result["state"] == state and result["scheduled_trading_day"] is day
    assert result["exchange_open_verified"] is False and result["schedule_only"] is True


def test_malformed_calendar_does_not_claim_market_open(tmp_path):
    path = calendar(tmp_path)
    path.write_text("{broken")
    assert probe.market_schedule(datetime(2026, 10, 12, 10, tzinfo=probe.IST), root=tmp_path)["state"] == "CALENDAR_UNVERIFIED"


def test_credentials_accept_whitespace_and_explicit_key_value_without_exposing_other_fields(tmp_path):
    path = tmp_path / "key.txt"
    path.write_text("key_value PRIVATE_SECRET PRIVATE_PASSWORD\n")
    assert probe.credential_value(path, "API_KEY") == "key_value"
    path.write_text("# configuration\nAPI_SECRET=PRIVATE_SECRET\nAPI_KEY=key_value\n")
    assert probe.credential_value(path, "API_KEY") == "key_value"
    path.write_text("API_KEY=bad value PRIVATE_SECRET\n")
    with pytest.raises(probe.ProbeError) as caught:
        probe.credential_value(path, "API_KEY")
    assert str(caught.value) == "CREDENTIAL_FORMAT_INVALID"


@pytest.mark.parametrize("approved", [False, True])
def test_deployment_metadata_never_establishes_arm_or_scheduler_readiness(tmp_path, approved):
    path = tmp_path / "configs/strategies/active_profile.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(dict(profile="V13_V10_G_3", live_activation_authorized=approved,
                                    deployment_stage="LIVE_APPROVED" if approved else "PAPER_VALIDATION")))
    before = path.read_bytes()
    result = probe.deployment_gate(root=tmp_path)
    assert result["live_activation_authorized"] is approved
    assert result["runtime_arm_and_kill_gates_checked"] is False and result["scheduler_checked"] is False
    assert path.read_bytes() == before


def run_fake_main(tmp_path, monkeypatch, openers, argv=None, *, report_failure=False):
    real_inspect, real_save = probe.inspect_app, probe.save_report
    def inspect_selected(app, **kwargs):
        return real_inspect(app, root=tmp_path, client_factory=factory(openers[app]), delay=0, **kwargs)
    monkeypatch.setattr(probe, "inspect_app", inspect_selected)
    monkeypatch.setattr(probe, "market_schedule", lambda now: {"state": "CALENDAR_UNVERIFIED"})
    monkeypatch.setattr(probe, "deployment_gate", lambda: {"state": "G3_LIVE_DEPLOYMENT_BLOCKED"})
    def save(report):
        if report_failure:
            raise OSError("PRIVATE_PATH_OR_TOKEN")
        return real_save(report, root=tmp_path)
    monkeypatch.setattr(probe, "save_report", save)
    return probe.main(argv or [])


def test_all_apps_partial_failures_do_not_hide_selected_read_success_or_expose_private_data(tmp_path, monkeypatch, capsys):
    for app in ("app1", "app2", "app3"):
        credentials(tmp_path, app)
    openers = {f"app{i}": FakeOpener() for i in range(1, 9)}
    openers["app1"] = FakeOpener({("GET", "/user/profile"): http_error(401, "TokenException")})
    openers["app3"] = FakeOpener({("GET", "/orders"): http_error(403, "PermissionException")})
    assert run_fake_main(tmp_path, monkeypatch, openers) == 0
    report = json.loads((tmp_path / "logs/kite_order_readiness/latest.json").read_text())
    assert report["readable_apps"] == ["app2"] and len(report["apps"]) == 8
    assert report["apps"][2]["state"] == "PARTIAL_READ_ACCESS" and report["apps"][3]["state"] == "CREDENTIALS_MISSING"
    assert report["mode"] == "VALIDATE_ONLY" and report["order_mutations_available"] is False
    assert report["orders_submitted"] == 0 and report["strategy_controls_changed"] is False
    assert report["order_acceptance"] == "NOT_TESTED" and report["static_ip_order_permission"] == "NOT_VERIFIED"
    encoded = json.dumps(report) + capsys.readouterr().out
    assert all(value not in encoded for value in (*PRIVATE, "PRIVATE_API_SECRET", "PRIVATE_PASSWORD", "token_app1", "123456.78"))
    assert len(openers["app1"].calls) == 1 and openers["app4"].calls == []


def test_no_full_access_returns_two_and_report_write_failure_returns_three(tmp_path, monkeypatch, capsys):
    credentials(tmp_path)
    opener = FakeOpener({("GET", "/user/profile"): http_error(401, "TokenException")})
    assert run_fake_main(tmp_path, monkeypatch, {"app1": opener}, ["--app", "app1"]) == 2
    # Restore the function globals before installing the second isolated runner.
    monkeypatch.undo()
    assert run_fake_main(tmp_path, monkeypatch, {"app1": FakeOpener()}, ["--app", "app1"], report_failure=True) == 3
    assert "PRIVATE_PATH_OR_TOKEN" not in capsys.readouterr().out
