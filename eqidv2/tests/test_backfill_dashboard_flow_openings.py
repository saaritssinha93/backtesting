from datetime import date, datetime, timedelta
import json

import pytest

from tools import backfill_dashboard_flow_openings as runner
from dashboard_flow_observer_store import read_json, write_json

DAY = date(2026, 10, 9)
ITEM = {"symbol": "TEST", "contract": "TEST26OCTFUT", "instrument_token": 123,
        "universe_source": "DATED_UNIVERSE"}
NOW = datetime(2026, 10, 12, 18, tzinfo=runner.IST)


def candles(day=DAY, count=6, **extra):
    return [{"date": value.isoformat(), "open": 100, "high": 102, "low": 99,
             "close": 101, "volume": 10, "oi": 1000 + index, **extra}
            for index, value in enumerate(runner.history.opening_minutes(day)[:count])]


@pytest.fixture(autouse=True)
def verified_identity(monkeypatch):
    monkeypatch.setattr(runner.history, "broker_identity_reason", lambda *args: "")


def recover(root, entries=None, **kwargs):
    return runner.recover_contract(root, entries or [(DAY, dict(ITEM))],
        clock=lambda: NOW, sleep=lambda seconds: None, **kwargs)


def test_bounded_ranges_include_at_most_thirty_calendar_dates():
    days = [date(2026, 7, 28) + timedelta(days=index) for index in range(77)]
    batches = runner.bounded_ranges(days)
    assert [len(batch) for batch in batches] == [30, 30, 17]
    assert sum(batches, []) == days
    with pytest.raises(ValueError):
        runner.bounded_ranges(days, 31)


def test_load_and_request_once_for_many_sessions(tmp_path):
    days = [date(2026, 10, day) for day in (1, 5, 6, 7, 8, 9, 12)]
    local_calls, broker_calls = [], []
    def local(root, contract, wanted):
        local_calls.append((contract, list(wanted)))
        return {days[0]: runner.history.valid_candles(candles(days[0]), days[0])}, ["read_only_archive.parquet"]
    class Client:
        def historical_data(self, token, start, end, interval, **kwargs):
            broker_calls.append((token, start, end, interval, kwargs))
            return [row for day in days[1:] for row in candles(day)]
    result = recover(tmp_path, [(day, dict(ITEM)) for day in days], import_local=True, local_loader=local, client=Client())
    assert len(local_calls) == len(broker_calls) == result["requests"] == 1
    assert all(row["state"] == "COMPLETE" for row in result["outcomes"])
    assert broker_calls[0][1].date() == days[1]
    assert broker_calls[0][-1] == {"continuous": False, "oi": True}
    saved = read_json(tmp_path, runner.history.history_path(days[0], ITEM["contract"]))
    assert saved["local_archive_paths"] == ["read_only_archive.parquet"]
    assert saved["exact_opening_snapshot"] is False
    assert all(path.is_relative_to(tmp_path / "dashboard_flow_observer") for path in tmp_path.rglob("*.json"))


def test_saved_minutes_are_not_overwritten_by_broker_conflicts(tmp_path):
    prior = runner.history.valid_candles(candles(count=1), DAY)
    path = runner.history.history_path(DAY, ITEM["contract"])
    write_json(tmp_path, path, {"session_date": str(DAY), "contract": ITEM["contract"], "symbol": ITEM["symbol"],
                              "candles": prior, "acquired_at": "2026-10-09T18:00:00+05:30"})
    class Client:
        def historical_data(self, *args, **kwargs):
            return candles(oi=9999)
    result = recover(tmp_path, client=Client())
    saved = read_json(tmp_path, path)
    assert saved["candles"][0]["oi"] == 1000
    assert len(saved["candles"]) == 6
    assert result["outcomes"][0]["conflicting_minutes_preserved"] == [prior[0]["candle_start"]]
    assert result["outcomes"][0]["added_minutes"] == 5


def test_unchanged_valid_saved_document_is_byte_identical(tmp_path):
    path = runner.history.history_path(DAY, ITEM["contract"])
    saved_path = write_json(tmp_path, path, {"session_date": str(DAY), "contract": ITEM["contract"], "symbol": ITEM["symbol"],
                                          "candles": runner.history.valid_candles(candles(), DAY)})
    before = saved_path.read_bytes()
    class Client:
        def historical_data(self, *args, **kwargs):
            pytest.fail("Complete evidence must not be re-fetched")
    result = recover(tmp_path, client=Client(), import_local=True,
                     local_loader=lambda *args: pytest.fail("Complete contract must not reread local archive"))
    assert result["requests"] == 0
    assert saved_path.read_bytes() == before


def test_conflicting_sources_and_synthetic_rows_are_excluded():
    rows = candles()
    rows.append(dict(rows[0], oi=7777))
    rows[1]["gap_filled"] = True
    rows[2]["candle_end"] = "2026-10-09T09:22:00+05:30"
    result = runner.index_opening_records(rows, [DAY])
    assert len(result[DAY]) == 3
    assert result[DAY][0]["candle_start"].endswith("09:18:00+05:30")


def test_production_guard_rechecked_after_pacing(tmp_path, monkeypatch):
    moment = [NOW]
    def sleep(seconds):
        moment[0] = datetime(2026, 10, 13, 9, tzinfo=runner.IST)
    monkeypatch.setattr(runner.history, "broker_guard", lambda moment: "BLOCKED" if moment.hour == 9 else "")
    class Client:
        def historical_data(self, *args, **kwargs):
            pytest.fail("No fetch in production window")
    result = runner.recover_contract(tmp_path, [(DAY, ITEM)], client=Client(),
        clock=lambda: moment[0], sleep=sleep)
    assert result["requests"] == 0
    assert result["stop_reason"] == "ProductionWindowGuard"


def test_unverified_expired_identity_blocks_broker_but_allows_local(tmp_path, monkeypatch):
    monkeypatch.setattr(runner.history, "broker_identity_reason", lambda *args: "HistoricalContractTokenNotCurrentlyVerified")
    class Client:
        def historical_data(self, *args, **kwargs):
            pytest.fail("Expired token queried")
    result = recover(tmp_path, client=Client(), import_local=True,
        local_loader=lambda *args: ({DAY: runner.history.valid_candles(candles(count=1), DAY)}, ["archive"]))
    row, = result["outcomes"]
    assert row["valid_minutes"] == 1
    assert row["error_type"] == "HistoricalContractTokenNotCurrentlyVerified"
    assert result["requests"] == 0


def test_auth_failure_stops_requests_without_leaking_exception(tmp_path):
    class TokenException(Exception):
        pass
    class Client:
        def historical_data(self, *args, **kwargs):
            raise TokenException("secret-token=abc")
    result = recover(tmp_path, client=Client())
    assert result["requests"] == 1
    assert result["stop_reason"] == "TokenException"
    assert "secret" not in json.dumps(result)


def test_missing_universe_date_is_reported_and_prevents_complete(tmp_path):
    missing = date(2026, 7, 28)
    sessions, grouped = runner.build_plan(tmp_path, missing, missing,
        universe_loader=lambda *args: ([], "MISSING_NEAR_MONTH_CONTRACT_IDENTITY"))
    payload = runner.coverage_payload(missing, missing, sessions, [], 0, planned=0)
    assert grouped == {}
    assert payload["state"] == "PARTIAL"
    assert payload["sessions"][0]["reason"] == "MISSING_NEAR_MONTH_CONTRACT_IDENTITY"


def test_stored_identity_mismatch_is_not_overwritten(tmp_path):
    path = runner.history.history_path(DAY, ITEM["contract"])
    stored = write_json(tmp_path, path, {"session_date": str(DAY), "contract": "OTHER", "symbol": ITEM["symbol"], "candles": []})
    before = stored.read_bytes()
    result = recover(tmp_path, import_local=True, local_loader=lambda *args: pytest.fail("Locked identity loaded"))
    assert result["outcomes"][0]["error_type"] == "StoredHistoricalIdentityMismatch"
    assert stored.read_bytes() == before


def test_conflicting_universe_identity_is_unavailable_even_with_saved_minutes(tmp_path):
    path = runner.history.history_path(DAY, ITEM["contract"])
    stored = write_json(tmp_path, path, {"session_date": str(DAY), "contract": ITEM["contract"], "symbol": ITEM["symbol"],
        "candles": runner.history.valid_candles(candles(), DAY)})
    before = stored.read_bytes()
    result = recover(tmp_path, [(DAY, ITEM | {"identity_conflict": True})], import_local=True)
    row, = result["outcomes"]
    assert row["state"] == "UNAVAILABLE" and row["error_type"] == "UniverseContractIdentityConflict"
    assert stored.read_bytes() == before


def test_plan_is_read_only_and_does_not_make_client(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runner, "build_plan", lambda *args, **kwargs: ([{"session_date": str(DAY), "state": "PLANNED",
        "universe_count": 1}], {(ITEM["contract"], ITEM["symbol"], 123): [(DAY, ITEM)]}))
    monkeypatch.setattr(runner, "make_client", lambda *args: pytest.fail("Plan uses credentials"))
    monkeypatch.setattr(runner, "now_ist", lambda: NOW)
    assert runner.main(["--data-root", str(tmp_path), "--from-date", str(DAY), "--to-date", str(DAY)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["state"] == "PLAN_ONLY" and result["requests"] == 0
    assert result["maximum_broker_ranges_before_local_import"] == 1
    assert list(tmp_path.iterdir()) == []


def test_local_archive_adapter_merges_sources_before_validation(tmp_path, monkeypatch):
    import dashboard_flow_observer_archives as archives
    records = candles()
    monkeypatch.setattr(archives, "load_contract_archives", lambda *args: [
        {"path": "one.parquet", "records": records},
        {"path": "two.parquet", "records": [dict(records[0], oi=99999)]}])
    indexed, paths = runner.load_local_index(tmp_path, ITEM["contract"], [DAY])
    assert len(indexed[DAY]) == 5
    assert paths == ["one.parquet", "two.parquet"]
