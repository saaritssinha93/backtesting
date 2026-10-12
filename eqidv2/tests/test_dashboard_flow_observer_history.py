from datetime import date, datetime, timedelta
import hashlib

import pandas as pd
import pytest

import dashboard_flow_observer_history as history
from dashboard_flow_observer_store import read_json, write_json

DAY = date(2026, 10, 9)
ROW = {"symbol": "TEST", "contract": "TEST26OCTFUT", "instrument_token": 123,
       "universe_source": "DATED_UNIVERSE"}


def candle(index, **extra):
    start = history.opening_minutes(DAY)[index]
    return {"date": start.isoformat(), "open": 100, "high": 102, "low": 99,
            "close": 101, "volume": 10, "oi": 1000 + index, **extra}


def current_master(root, contract="TEST26OCTFUT", day="2026-10-12"):
    path = root / "fno_oi/instrument_master/latest_instrument_master.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"master_date": day, "tradingsymbol": contract, "instrument_token": 123}]).to_parquet(path)


@pytest.mark.parametrize("clock, blocked", [("08:29", False), ("08:30", True), ("15:31", True),
    ("16:59", True), ("17:00", False)])
def test_collection_and_eod_guard(clock, blocked):
    assert bool(history.broker_guard(datetime.fromisoformat(f"2026-10-12T{clock}:00+05:30"))) is blocked


def test_holiday_and_completed_calendar():
    assert not history.broker_guard(datetime(2026, 10, 20, 10, tzinfo=history.IST))
    assert history.latest_completed(datetime(2026, 10, 12, 10, tzinfo=history.IST)) == DAY


def test_missing_minutes_checks_internal_holes_not_last_date():
    rows = history.valid_candles([candle(i) for i in (0, 1, 3, 4, 5)], DAY)
    assert history.missing_minutes(rows, DAY) == [history.opening_minutes(DAY)[2]]


def test_contract_paths_match_adapter_and_do_not_collide():
    from dashboard_flow_opening_data import _safe_contract
    assert history.history_path(DAY, "M&M26OCTFUT") == f"historical/{DAY}/{_safe_contract('M&M26OCTFUT')}.json"
    assert history.history_path(DAY, "M&M26OCTFUT") != history.history_path(DAY, "M_M26OCTFUT")


def test_quality_rejects_synthetic_invalid_and_conflicting_data():
    records = [candle(0, gap_filled=True), candle(1, source="GAP_FILL"), candle(2, oi=None),
               candle(3, high=90), candle(4), candle(4, oi=9999), candle(5)]
    valid = history.valid_candles(records, DAY)
    assert len(valid) == 1
    assert valid[0]["candle_start"].endswith("09:20:00+05:30")
    assert valid[0]["candle_end"].endswith("09:21:00+05:30")


@pytest.mark.parametrize("end", ["2026-10-09T09:20:00+05:30", "2026-10-09T09:15:00+05:30", None, "bad"])
def test_explicit_interval_end_must_be_one_minute(end):
    assert history.valid_candles([candle(0, candle_end=end)], DAY) == []


def test_explicit_valid_minute_end_is_preserved():
    end = "2026-10-09T09:16:00+05:30"
    result, = history.valid_candles([candle(0, candle_end=end)], DAY)
    assert result["candle_end"] == end


def test_recovery_fills_internal_gap_and_never_writes_confirmation_archive(tmp_path):
    current_master(tmp_path)
    directory = tmp_path / "fno_oi/raw_contracts_1m_hist"
    directory.mkdir(parents=True)
    source = directory / "TEST26OCTFUT_1minute.parquet"
    pd.DataFrame([candle(i) for i in (0, 1, 3, 4, 5)]).to_parquet(source)
    original = hashlib.sha256(source.read_bytes()).hexdigest()
    calls = []
    class Client:
        def historical_data(self, token, start, end, interval, **kwargs):
            calls.append((token, start, end, interval, kwargs))
            return [candle(2)]
    result = history.recover_one(tmp_path, DAY, ROW, client=Client(), import_local=True,
                                clock=lambda: datetime(2026, 10, 12, 17, tzinfo=history.IST), sleep=lambda seconds: None)
    assert result["state"] == "COMPLETE" and len(result["candles"]) == 6
    assert calls[0][1] == history.opening_minutes(DAY)[2]
    assert calls[0][2] == history.opening_minutes(DAY)[2] + timedelta(minutes=1)
    assert calls[0][-1] == {"continuous": False, "oi": True}
    assert result["exact_opening_snapshot"] is False
    assert hashlib.sha256(source.read_bytes()).hexdigest() == original
    assert read_json(tmp_path, history.history_path(DAY, ROW["contract"]))["source"] == "KITE_HISTORICAL_MINUTE"


def test_recovery_guard_checked_before_every_request(tmp_path):
    class Client:
        def historical_data(self, *args, **kwargs):
            pytest.fail("broker contacted during production window")
    result = history.recover_one(tmp_path, DAY, ROW, client=Client(),
                clock=lambda: datetime(2026, 10, 12, 9, tzinfo=history.IST), sleep=lambda seconds: None)
    assert result["requests"] == 0 and result["error_type"] == "ProductionWindowGuard"


def test_history_plan_no_writes_or_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr(history, "make_client", lambda *a, **kw: pytest.fail("broker in plan"))
    assert history.main(["--data-root", str(tmp_path), "--from-date", str(DAY), "--to-date", str(DAY)]) == 0
    assert list(tmp_path.iterdir()) == []


def test_broker_error_message_cannot_leak_into_report(tmp_path):
    current_master(tmp_path)
    class TokenException(Exception):
        pass
    class Client:
        def historical_data(self, *args, **kwargs):
            raise TokenException("secret access token and account")
    result = history.recover_one(tmp_path, DAY, ROW, client=Client(),
                clock=lambda: datetime(2026, 10, 12, 17, tzinfo=history.IST), sleep=lambda seconds: None)
    assert result["error_type"] == "TokenException" and result["requests"] == 1
    assert "secret" not in str(result)


def test_recycled_token_rejected_before_broker_call(tmp_path):
    current_master(tmp_path, contract="SOMEOTHER26NOVFUT")
    class Client:
        def historical_data(self, *args, **kwargs):
            pytest.fail("Recycled token must not be queried")
    result = history.recover_one(tmp_path, DAY, ROW, client=Client(),
                clock=lambda: datetime(2026, 10, 12, 17, tzinfo=history.IST), sleep=lambda seconds: None)
    assert result["error_type"] == "HistoricalContractTokenNotCurrentlyVerified"
    assert result["requests"] == 0


def test_stale_master_is_not_current_identity_evidence(tmp_path):
    current_master(tmp_path, day="2026-10-09")
    assert history.broker_identity_reason(tmp_path, ROW, datetime(2026, 10, 12, 17, tzinfo=history.IST)) == "CurrentInstrumentMasterStale"
