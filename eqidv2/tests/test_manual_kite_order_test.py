"""Manual submission flow is exercised only with fake broker transports."""
from __future__ import annotations

import io
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

import pytest

from tools import manual_kite_order_test as manual
from tools import test_kite_order_readiness as validator


@pytest.fixture(autouse=True)
def no_real_network(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("Real network is forbidden in manual-order tests")
    monkeypatch.setattr(manual, "build_opener", fail)
    monkeypatch.setattr(validator, "build_opener", fail)


class Reply:
    def __init__(self, payload):
        self.raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False
    def read(self, count):
        return self.raw[:count]


class FakeReader:
    def __init__(self, *, required=10, balance=100, fail=None):
        self.required, self.balance, self.fail = required, balance, fail
        self.calls = []
    def request(self, method, path, **kwargs):
        assert (method, path) in {("GET", "/user/profile"), ("GET", "/user/margins/equity"), ("POST", "/margins/orders")}
        self.calls.append((method, path, kwargs))
        if self.fail:
            raise validator.ProbeError("SESSION_INVALID_OR_EXPIRED")
        if path == "/user/profile":
            return {"exchanges": ["NSE"], "products": ["MIS", "CNC"], "order_types": ["LIMIT"],
                    "user_name": "PRIVATE_USER_NAME", "user_id": "PRIVATE_USER_ID"}
        if path == "/user/margins/equity":
            return {"enabled": True, "available": {"live_balance": self.balance}}
        return [{"total": self.required}]


class FakeWriterOpener:
    def __init__(self, *, payload=None, error=None, before_send=None):
        self.payload = payload if payload is not None else {"status": "success", "data": {"order_id": "FAKE_ACK_123"}}
        self.error, self.before_send = error, before_send
        self.calls = []
    def open(self, request, timeout):
        url = urlparse(request.full_url)
        assert request.get_method() == "POST" and url.scheme == "https"
        assert url.netloc == "api.kite.trade" and url.path == "/orders/regular" and not url.query
        if self.before_send:
            self.before_send(request)
        self.calls.append(request)
        if self.error:
            raise self.error
        return Reply(self.payload)


@pytest.fixture
def workspace(tmp_path):
    (tmp_path / "api_key2.txt").write_text("FAKE_KEY2 PRIVATE_API_SECRET PRIVATE_PASSWORD PRIVATE_TOTP")
    (tmp_path / "access_token2.txt").write_text("FAKE_TOKEN2")
    return tmp_path


def arguments(*extra):
    return manual.build_parser().parse_args(["--app", "app2", "--symbol", "TEST", "--side", "BUY", "--quantity", "2", "--price", "100", *extra])


def expected_order():
    return {"exchange": "NSE", "tradingsymbol": "TEST", "transaction_type": "BUY", "quantity": 2,
            "product": "MIS", "order_type": "LIMIT", "validity": "DAY", "price": "100.00"}


def execute(workspace, *, extra=(), reader=None, opener=None, confirmation="exact", interactive=True):
    args = arguments(*extra)
    reader = reader or FakeReader()
    opener = opener or FakeWriterOpener()
    created = []
    output = []
    def create_writer(key, token):
        assert (key, token) == ("FAKE_KEY2", "FAKE_TOKEN2")
        created.append(True)
        return manual.SingleOrderWriter(key, token, opener=opener)
    def confirm(_prompt):
        return manual.confirmation_text(args.app, expected_order(), args.intent_id) if confirmation == "exact" else confirmation
    result, code = manual.run(args, root=workspace, reader_factory=lambda *_: reader,
        writer_factory=create_writer, confirm=confirm, interactive=interactive, emit=output.append)
    return result, code, reader, opener, created, output


def test_default_preview_is_physically_readonly_and_does_not_claim_intent(workspace):
    result, code, reader, opener, writers, _ = execute(workspace)
    assert code == 0 and result["state"] == "PREVIEW_ONLY"
    assert result["submission_attempts"] == 0 and result["order_acceptance"] == "NOT_TESTED"
    assert len(reader.calls) == 3 and opener.calls == [] and writers == []
    assert not (workspace / "logs").exists()
    assert ("POST", "/orders/regular") not in validator.ALLOWED


def test_one_submission_requires_exact_confirmation_and_persisted_unique_intent(workspace):
    path = workspace / "logs/manual_kite_order_test/intents/operator-test-001"
    def persisted_before_post(request):
        saved = json.loads((path / "intent.json").read_text())
        fields = parse_qs(request.data.decode())
        assert saved["state"] == "INTENT_COMMITTED" and saved["human_confirmation_received"] is True
        assert saved["submission_attempts"] == 0 and saved["order"] == expected_order()
        assert fields["tag"] == [saved["tag"]] and len(saved["tag"]) == 20
        assert fields["quantity"] == ["2"] and fields["price"] == ["100.00"]
        assert fields["order_type"] == ["LIMIT"] and fields["validity"] == ["DAY"]
        assert request.get_header("Content-type") == "application/x-www-form-urlencoded"
    opener = FakeWriterOpener(before_send=persisted_before_post)
    result, code, _, _, writers, output = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"), opener=opener)
    assert code == 0 and result["state"] == "BROKER_ACKNOWLEDGED" and result["order_id"] == "FAKE_ACK_123"
    assert result["submission_attempts"] == 1 and len(opener.calls) == 1 and len(writers) == 1
    assert result["automatic_retry"] is False and result["app_fallback"] is False
    assert result["strategy_controls_changed"] is False
    assert json.loads((path / "result.json").read_text())["state"] == "BROKER_ACKNOWLEDGED"
    serialized = json.dumps(result) + "\n".join(output) + (path / "intent.json").read_text()
    assert all(value not in serialized for value in ("FAKE_TOKEN2", "FAKE_KEY2", "PRIVATE_API_SECRET", "PRIVATE_PASSWORD", "PRIVATE_USER_ID", "PRIVATE_USER_NAME"))


@pytest.mark.parametrize("confirmation", ["", "yes", "SUBMIT", "SUBMIT app1 BUY 2 TEST NSE MIS LIMIT 100.00 DAY operator-test-001"])
def test_wrong_or_incomplete_confirmation_never_instantiates_writer(workspace, confirmation):
    result, code, _, opener, writers, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"), confirmation=confirmation)
    assert code == 2 and result["state"] == "CONFIRMATION_CANCELLED"
    assert opener.calls == [] and writers == [] and not (workspace / "logs").exists()


def test_submit_noninteractive_blocks_before_credentials_or_network(workspace):
    result, code, reader, opener, writers, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"), interactive=False)
    assert code == 2 and result["state"] == "INTERACTIVE_CONFIRMATION_REQUIRED"
    assert not reader.calls and not opener.calls and not writers


@pytest.mark.parametrize("balance", [0, None, True])
def test_insufficient_or_unknown_margin_blocks_submission(workspace, balance):
    result, code, _, opener, writers, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"), reader=FakeReader(balance=balance))
    assert code == 2 and result["state"] == "MARGIN_COVERAGE_NOT_CONFIRMED"
    assert not opener.calls and not writers


@pytest.mark.parametrize("required", [None, -1, True, "NaN"])
def test_malformed_preview_never_reaches_submission(workspace, required):
    result, code, _, opener, writers, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"), reader=FakeReader(required=required))
    assert code == 2 and result["state"] == "PREVIEW_FAILED"
    assert not opener.calls and not writers


def test_invalid_session_has_no_app_fallback(workspace):
    reader = FakeReader(fail=True)
    result, code, _, opener, writers, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"), reader=reader)
    assert result["state"] == "PREVIEW_FAILED" and code == 2
    assert len(reader.calls) == 1 and not opener.calls and not writers


def test_preview_http_protocol_failure_is_sanitized_and_never_submits(workspace):
    class BrokenReader(FakeReader):
        def request(self, *args, **kwargs):
            self.calls.append(args)
            raise manual.HTTPException("PRIVATE_HTTP_RESPONSE")
    reader = BrokenReader()
    result, code, _, opener, writers, output = execute(workspace,
        extra=("--submit", "--intent-id", "operator-test-001"), reader=reader)
    assert code == 2 and result["state"] == "PREVIEW_FAILED"
    assert len(reader.calls) == 1 and not opener.calls and not writers
    assert "PRIVATE_HTTP_RESPONSE" not in json.dumps(result) + "\n".join(output)


@pytest.mark.parametrize("error", [TimeoutError("PRIVATE_TOKEN"), URLError("PRIVATE_TOKEN")])
def test_ambiguous_network_result_saved_unknown_without_retry_and_intent_blocks_rerun(workspace, error):
    opener = FakeWriterOpener(error=error)
    options = ("--submit", "--intent-id", "operator-test-001")
    result, code, *_ = execute(workspace, extra=options, opener=opener)
    assert code == 2 and result["state"] == "UNKNOWN_ACCEPTANCE" and len(opener.calls) == 1
    assert "PRIVATE_TOKEN" not in json.dumps(result)
    again, code, reader, second, writers, _ = execute(workspace, extra=options)
    assert code == 2 and again["state"] == "INTENT_ALREADY_EXISTS"
    assert not reader.calls and not second.calls and not writers


def test_interrupted_submission_is_saved_as_unknown_and_cannot_be_repeated(workspace):
    opener = FakeWriterOpener(error=KeyboardInterrupt())
    options = ("--submit", "--intent-id", "operator-test-001")
    result, code, _, _, _, _ = execute(workspace, extra=options, opener=opener)
    assert code == 2 and result["state"] == "UNKNOWN_ACCEPTANCE"
    assert result["submission_attempts"] == 1 and len(opener.calls) == 1
    saved = manual.intent_directory(workspace, "operator-test-001") / "result.json"
    assert json.loads(saved.read_text())["state"] == "UNKNOWN_ACCEPTANCE"
    again, code, reader, second, writers, _ = execute(workspace, extra=options)
    assert code == 2 and again["state"] == "INTENT_ALREADY_EXISTS"
    assert not reader.calls and not second.calls and not writers


def test_half_created_intent_after_crash_is_never_reused(workspace):
    directory = manual.intent_directory(workspace, "operator-test-001")
    directory.mkdir(parents=True)
    result, code, reader, opener, writers, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"))
    assert result["state"] == "INTENT_ALREADY_EXISTS" and code == 2
    assert not reader.calls and not opener.calls and not writers


def test_failed_intent_persistence_cannot_send(workspace, monkeypatch):
    def fail(*_):
        raise OSError("PRIVATE_STORAGE_PATH")
    monkeypatch.setattr(manual, "_atomic_json", fail)
    result, code, _, opener, _, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"))
    assert code == 3 and result["state"] == "INTENT_SAVE_FAILED_NO_SUBMISSION" and not opener.calls
    assert manual.intent_directory(workspace, "operator-test-001").exists()


def test_result_write_failure_cannot_retry_or_claim_no_submission(workspace, monkeypatch):
    write = manual._atomic_json
    def fail_result(path, value):
        if path.name == "result.json":
            raise OSError("PRIVATE_STORAGE_PATH")
        return write(path, value)
    monkeypatch.setattr(manual, "_atomic_json", fail_result)
    result, code, _, opener, _, output = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"))
    assert code == 3 and result["submission_attempts"] == 1 and result["result_saved"] is False
    assert len(opener.calls) == 1 and any("a submission was attempted" in line for line in output)
    assert "PRIVATE_STORAGE_PATH" not in json.dumps(result)


def test_interrupted_result_save_keeps_submission_truth(workspace, monkeypatch):
    write = manual._atomic_json
    def interrupt_result(path, value):
        if path.name == "result.json":
            raise KeyboardInterrupt()
        return write(path, value)
    monkeypatch.setattr(manual, "_atomic_json", interrupt_result)
    result, code, _, opener, _, output = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"))
    assert code == 3 and result["submission_attempts"] == 1 and result["result_saved"] is False
    assert len(opener.calls) == 1
    assert any("a submission was attempted" in line for line in output)


def test_failed_console_warning_after_submission_cannot_claim_no_submission(workspace, monkeypatch, capsys):
    write = manual._atomic_json
    run = manual.run
    opener = FakeWriterOpener()
    def fail_result(path, value):
        if path.name == "result.json":
            raise OSError("PRIVATE_STORAGE_PATH")
        return write(path, value)
    def closed_warning(message):
        if message.startswith("RESULT_SAVE_FAILED"):
            raise BrokenPipeError("PRIVATE_CONSOLE_PATH")
    def fake_run(args):
        return run(args, root=workspace, reader_factory=lambda *_: FakeReader(),
            writer_factory=lambda *keys: manual.SingleOrderWriter(*keys, opener=opener),
            confirm=lambda _: manual.confirmation_text(args.app, expected_order(), args.intent_id),
            interactive=True, emit=closed_warning)
    monkeypatch.setattr(manual, "_atomic_json", fail_result)
    monkeypatch.setattr(manual, "run", fake_run)
    code = manual.main(["--app", "app2", "--symbol", "TEST", "--side", "BUY", "--quantity", "2", "--price", "100",
                        "--submit", "--intent-id", "operator-test-001"])
    text = capsys.readouterr().out
    assert code == 3 and len(opener.calls) == 1
    assert "no submission" not in text.lower() and "PRIVATE_" not in text
    assert json.loads(text)["submission_attempts"] == 1


def test_transient_local_rename_retry_never_repeats_broker_submission(workspace, monkeypatch):
    replace = manual.os.replace
    attempts = {}
    sleeps = []
    def transient(source, destination):
        attempts[destination.name] = attempts.get(destination.name, 0) + 1
        if attempts[destination.name] == 1:
            raise PermissionError("Windows temporary sharing violation")
        replace(source, destination)
    monkeypatch.setattr(manual.os, "replace", transient)
    monkeypatch.setattr(manual.time, "sleep", sleeps.append)
    result, code, _, opener, _, _ = execute(workspace, extra=("--submit", "--intent-id", "operator-test-001"))
    assert code == 0 and result["result_saved"] is True
    assert attempts == {"intent.json": 2, "result.json": 2}
    assert sleeps == [.025, .025] and len(opener.calls) == 1


@pytest.mark.parametrize("failed_name,submissions", [("intent.json", 0), ("result.json", 1)])
def test_persistent_local_rename_failure_has_bounded_retry_and_consumes_intent(workspace, monkeypatch, failed_name, submissions):
    replace = manual.os.replace
    attempts = []
    sleeps = []
    def locked(source, destination):
        if destination.name == failed_name:
            attempts.append(destination)
            raise PermissionError("PRIVATE_STORAGE_PATH")
        replace(source, destination)
    monkeypatch.setattr(manual.os, "replace", locked)
    monkeypatch.setattr(manual.time, "sleep", sleeps.append)
    options = ("--submit", "--intent-id", "operator-test-001")
    result, code, _, opener, _, _ = execute(workspace, extra=options)
    assert code == 3 and len(opener.calls) == submissions
    assert len(attempts) == 4 and sleeps == [.025, .05, .1]
    assert sum(sleeps) <= .2
    assert "PRIVATE_STORAGE_PATH" not in json.dumps(result)
    again, code, reader, second, writers, _ = execute(workspace, extra=options)
    assert code == 2 and again["state"] == "INTENT_ALREADY_EXISTS"
    assert not reader.calls and not second.calls and not writers


@pytest.mark.parametrize("method,path", [("DELETE", "/orders/regular/1"), ("PUT", "/orders/regular/1"),
    ("POST", "/orders/amo"), ("DELETE", "/session/token"), ("POST", "/session/token"),
    ("GET", "/orders"), ("POST", "https://evil.invalid/orders/regular")])
def test_writer_only_allows_single_fixed_regular_order_endpoint(method, path):
    opener = FakeWriterOpener()
    writer = manual.SingleOrderWriter("KEY", "TOKEN", opener=opener)
    with pytest.raises(validator.ProbeError, match="ENDPOINT_NOT_ALLOWED"):
        writer.request(method, path, payload={**expected_order(), "tag": "ManualTest123"})
    assert not opener.calls


def test_writer_instance_cannot_retry_even_after_timeout():
    opener = FakeWriterOpener(error=TimeoutError())
    writer = manual.SingleOrderWriter("KEY", "TOKEN", opener=opener)
    payload = {**expected_order(), "tag": "ManualTest123"}
    assert writer.request("POST", "/orders/regular", payload=payload)["state"] == "UNKNOWN_ACCEPTANCE"
    with pytest.raises(validator.ProbeError, match="SUBMISSION_ALREADY_ATTEMPTED"):
        writer.request("POST", "/orders/regular", payload=payload)
    assert len(opener.calls) == 1


@pytest.mark.parametrize("status,expected", [(400, "BROKER_REQUEST_REJECTED"), (403, "BROKER_REQUEST_REJECTED"),
                                          (408, "UNKNOWN_ACCEPTANCE"), (429, "UNKNOWN_ACCEPTANCE"), (500, "UNKNOWN_ACCEPTANCE")])
def test_broker_rejections_and_ambiguous_server_errors_are_sanitized(status, expected):
    error = HTTPError(manual.ORDER_URL, status, "PRIVATE_TOKEN", {},
        io.BytesIO(json.dumps({"status": "error", "error_type": "PermissionException", "message": "PRIVATE_TOKEN"}).encode()))
    opener = FakeWriterOpener(error=error)
    writer = manual.SingleOrderWriter("KEY", "TOKEN", opener=opener)
    result = writer.request("POST", "/orders/regular", payload={**expected_order(), "tag": "ManualTest123"})
    assert result["state"] == expected and "PRIVATE_TOKEN" not in json.dumps(result)
    assert len(opener.calls) == 1


@pytest.mark.parametrize("payload", [b"invalid PRIVATE_TOKEN", {"status": "success", "data": {}},
                                     {"status": "success", "data": {"order_id": "bad private data"}}])
def test_malformed_acknowledgement_is_unknown_not_retried(payload):
    opener = FakeWriterOpener(payload=payload)
    writer = manual.SingleOrderWriter("KEY", "TOKEN", opener=opener)
    result = writer.request("POST", "/orders/regular", payload={**expected_order(), "tag": "ManualTest123"})
    assert result["state"] == "UNKNOWN_ACCEPTANCE" and len(opener.calls) == 1
    assert "PRIVATE_TOKEN" not in json.dumps(result)


@pytest.mark.parametrize("flags", [[], ["--app", "all"], ["--order-type", "MARKET"], ["--yes"],
    ["--price", "NaN"], ["--price", "0"], ["--price", "100.001"], ["--quantity", "0"],
    ["--submit"], ["--submit", "--intent-id", "../escape"], ["--exchange", "NFO"], ["--product", "NRML"]])
def test_invalid_cli_values_fail_before_any_reader_or_writer(flags):
    base = ["--app", "app2", "--symbol", "TEST", "--side", "BUY", "--quantity", "2", "--price", "100"]
    with pytest.raises(SystemExit) as caught:
        manual.main(base + flags if flags else [])
    assert caught.value.code == 2
