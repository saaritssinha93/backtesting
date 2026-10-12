from datetime import date, datetime, timedelta
import hashlib
from pathlib import Path

import pandas as pd
import pytest

import dashboard_flow_observer as observer
from dashboard_flow_observer_store import read_events, read_json

DAY = date(2026, 10, 12)
ROW = {"symbol": "TEST", "contract": "TEST26OCTFUT", "instrument_token": 123,
       "universe_source": "DATED_UNIVERSE"}


def moment(value):
    return datetime.fromisoformat(f"2026-10-12T{value}+05:30")


def test_quote_preserves_exchange_and_receipt_no_backdate():
    received = moment("09:15:02")
    quotes = {"NFO:TEST26OCTFUT": {"timestamp": datetime(2026, 10, 12, 9, 15), "oi": 1000,
                                "last_price": 100, "ohlc": {"open": 99}}}
    event, = observer.quote_events(quotes, [ROW], received)
    assert event["exchange_timestamp"] == moment("09:15:00").isoformat()
    assert event["received_at"] == received.isoformat()
    assert event["source"] == "KITE_FULL_QUOTE"
    assert event["oi"] == 1000


def test_live_0916_quote_requires_same_fresh_contract_and_pre_0920_receipt():
    quote = {"session_date": str(DAY), "symbol": "TEST", "contract": "TEST26OCTFUT",
             "kind": "future", "source": "KITE_FULL_QUOTE", "oi": 1000,
             "exchange_timestamp": moment("09:16:04").isoformat(),
             "received_at": moment("09:16:06").isoformat()}
    good = observer._live_0916_quote(quote, ROW, str(DAY))
    assert good["oi"] == 1000
    assert good["seconds_after_0916"] == 4
    assert good["seconds_to_0920"] == 236
    assert good["quote_age_seconds"] == 2
    assert good["exact_first_minute_candle"] is False
    for changes in (
        {"contract": "TEST26NOVFUT"}, {"source": "KITE_HISTORICAL_MINUTE"},
        {"session_date": "2026-10-09"}, {"oi": 0},
        {"exchange_timestamp": moment("09:15:59").isoformat()},
        {"exchange_timestamp": moment("09:16:16").isoformat()},
        {"received_at": moment("09:16:20").isoformat()},
        {"received_at": moment("09:20:00").isoformat()},
        {"received_at": "2026-10-12T09:16:06"},
    ):
        assert observer._live_0916_quote({**quote, **changes}, ROW, str(DAY)) is None


def test_live_0916_coverage_persists_full_universe_and_no_candle_claim(tmp_path):
    other = {"symbol": "OTHER", "contract": "OTHER26OCTFUT", "instrument_token": 124,
             "universe_source": "DATED_UNIVERSE"}
    current = [moment("09:14:59")]
    ticks = iter([moment("09:15:59"), moment("09:16:04"), moment("09:16:07"),
                  moment("09:20:00"), moment("09:21:00")])
    class Client:
        def quote(self, keys):
            assert keys == ["NSE:TEST", "NFO:TEST26OCTFUT", "NSE:OTHER", "NFO:OTHER26OCTFUT"]
            exchange = moment("09:16:02") if current[0].time() == moment("09:16:07").time() else current[0]
            return {"NFO:TEST26OCTFUT": {"timestamp": exchange, "oi": 1000, "last_price": 100}}
    def sleep(seconds):
        current[0] = next(ticks)
    result = observer.run_collection(tmp_path, [ROW, other], Client(), clock=lambda: current[0], sleep=sleep)
    assert result["state"] == "COMPLETE"
    saved = read_json(tmp_path, f"coverage/live_opening_oi_{DAY}.json")
    assert saved["schema"] == "dashboard_flow_live_opening_oi_v1"
    assert saved["state"] == "FINAL"
    assert saved["universe_count"] == 2
    assert saved["qualified_count"] == saved["missing_count"] == 1
    rows = {row["symbol"]: row for row in saved["rows"]}
    assert rows["TEST"]["status"] == "QUOTE_NEAR_0916_NOT_CANDLE"
    assert rows["TEST"]["exchange_timestamp"] == moment("09:16:02").isoformat()
    assert rows["TEST"]["received_at"] == moment("09:16:07").isoformat()
    assert rows["TEST"]["quote_baseline_available_by_0920"] is True
    assert rows["TEST"]["baseline_kind"] == "PROVISIONAL_FULL_QUOTE"
    assert rows["TEST"]["seconds_to_0920"] == 238
    assert rows["TEST"]["exact_first_minute_candle"] is False
    assert rows["OTHER"]["status"] == "NO_QUALIFIED_QUOTE"
    assert rows["OTHER"]["oi"] is None
    assert "No fresh same-contract" in rows["OTHER"]["missing_reason"]


@pytest.mark.parametrize("timestamp", [None, "bad", "2026-10-09T15:30:00+05:30", "2026-10-12T09:15:03+05:30"])
def test_no_substitute_timestamp_for_missing_old_or_future_quote(timestamp):
    assert observer.quote_events({"NFO:TEST26OCTFUT": {"timestamp": timestamp, "oi": 1000}},
                                 [ROW], moment("09:15:02")) == []


def test_new_cash_quote_with_yesterdays_ltp_is_not_preopen_price():
    quote = {"timestamp": moment("09:10:00"), "last_trade_time": "2026-10-09T15:29:59+05:30", "last_price": 100}
    assert observer.quote_events({"NSE:TEST": quote}, [ROW], moment("09:10:02")) == []
    quote["last_trade_time"] = moment("09:09:59")
    assert observer.quote_events({"NSE:TEST": quote}, [ROW], moment("09:10:02"))[0]["price"] == 100


@pytest.mark.parametrize("clock, expected", [("08:54:59", "BEFORE_OBSERVER_WINDOW"),
    ("08:55:00", ""), ("09:14:59", ""), ("09:15:00", "LATE_START_NO_OPENING_BACKDATE")])
def test_startup_window(clock, expected):
    assert observer.startup_reason(moment(clock)) == expected


def test_calendar_refuses_holiday_and_weekend():
    assert observer.startup_reason(datetime(2026, 10, 10, 8, 55, tzinfo=observer.IST))
    assert observer.startup_reason(datetime(2026, 10, 20, 8, 55, tzinfo=observer.IST))


def test_real_dated_universe_required_and_registry_labelled(tmp_path):
    directory = tmp_path / "fno_oi/universe"
    directory.mkdir(parents=True)
    pd.DataFrame([{"underlying": "TEST", "tradingsymbol": "TEST26OCTFUT", "instrument_token": 123,
                   "expiry": "2026-10-27"}]).to_parquet(directory / "contract_registry.parquet")
    rows, source = observer.load_universe(tmp_path, DAY)
    assert source == "RECONSTRUCTED_NEAR_MONTH_REGISTRY"
    with pytest.raises(ValueError, match="exact dated"):
        observer.run_collection(tmp_path, rows, None, clock=lambda: moment("08:55:00"))
    assert observer.load_universe(tmp_path, date(2026, 5, 15))[0] == []


def test_collection_isolated_no_orders_and_stops_at_deadline(tmp_path):
    protected = tmp_path / "fno_oi/raw_contracts_5m/original.parquet"
    protected.parent.mkdir(parents=True)
    protected.write_bytes(b"immutable production fixture")
    original = hashlib.sha256(protected.read_bytes()).hexdigest()
    current = [moment("09:14:55")]
    class Client:
        def quote(self, keys):
            assert keys == ["NSE:TEST", "NFO:TEST26OCTFUT"]
            return {"NFO:TEST26OCTFUT": {"timestamp": current[0], "oi": 1000, "last_price": 100}}
    def sleep(seconds):
        assert seconds >= 5
        current[0] = moment("09:21:00")
    result = observer.run_collection(tmp_path, [ROW], Client(), clock=lambda: current[0], sleep=sleep)
    assert result["state"] == "COMPLETE"
    assert result["events"] == 1
    assert len(read_events(tmp_path, str(DAY))) == 1
    assert hashlib.sha256(protected.read_bytes()).hexdigest() == original
    assert read_json(tmp_path, observer.STATUS)["status"] == "COMPLETE"
    assert all(path == protected or "dashboard_flow_observer" in path.parts for path in tmp_path.rglob("*") if path.is_file())


def test_missing_expiry_month_never_uses_surviving_next_month(tmp_path):
    directory = tmp_path / "fno_oi/universe"
    directory.mkdir(parents=True)
    pd.DataFrame([
        {"underlying": "TEST", "tradingsymbol": "TEST26AUGFUT", "instrument_token": 123, "expiry": "2026-08-25"},
        {"underlying": "NEW", "tradingsymbol": "NEW26SEPFUT", "instrument_token": 124, "expiry": "2026-09-29"},
    ]).to_parquet(directory / "contract_registry.parquet")
    rows, source = observer.load_universe(tmp_path, date(2026, 7, 28))
    assert rows == [] and source == "MISSING_NEAR_MONTH_CONTRACT_IDENTITY"
    assert [r['contract'] for r in observer.load_universe(tmp_path, date(2026, 7, 29))[0]] == ['TEST26AUGFUT']
    assert [r['contract'] for r in observer.load_universe(tmp_path, date(2026, 8, 25))[0]] == ['TEST26AUGFUT']
    assert [r['contract'] for r in observer.load_universe(tmp_path, date(2026, 8, 26))[0]] == ['NEW26SEPFUT']


def test_observer_expiry_calendar_adjusts_holiday_and_rejects_unreviewed_year():
    assert observer.expected_near_month_expiry(date(2026, 11, 20)) == date(2026, 11, 23)
    assert observer.expected_near_month_expiry(date(2026, 7, 28)) == date(2026, 7, 28)
    assert observer.expected_near_month_expiry(date(2027, 1, 4)) is None


def test_token_failure_waits_reloads_without_refreshing(tmp_path):
    class TokenException(Exception):
        pass
    class BadClient:
        def quote(self, keys):
            raise TokenException("SECRET must not be persisted")
    class GoodClient:
        def quote(self, keys):
            return {}
    current = [moment("08:55:00")]
    sleeps = []
    reloads = []
    def sleep(seconds):
        sleeps.append(seconds)
        current[0] = moment("09:00:00") if len(sleeps) == 1 else moment("09:21:00")
    def reload_client():
        reloads.append(True)
        return GoodClient()
    result = observer.run_collection(tmp_path, [ROW], BadClient(), clock=lambda: current[0], sleep=sleep,
                                     reload_client=reload_client)
    assert result["state"] == "COMPLETE" and reloads == [True] and sleeps[0] == 30
    assert "SECRET" not in (tmp_path / "dashboard_flow_observer" / observer.STATUS).read_text()


def test_single_instance_does_not_steal_or_delete_lock(tmp_path):
    with observer.single_instance(tmp_path, "opening_observer"):
        with pytest.raises(observer.AlreadyRunning):
            with observer.single_instance(tmp_path, "opening_observer"):
                pytest.fail("Duplicate acquired lock")
    with observer.single_instance(tmp_path, "opening_observer"):
        pass


def test_plan_has_no_broker_and_no_disk_writes(tmp_path, monkeypatch):
    monkeypatch.setattr(observer, "make_client", lambda *a, **kw: pytest.fail("broker in plan"))
    assert observer.main(["--data-root", str(tmp_path)]) == 0
    assert list(tmp_path.iterdir()) == []


def test_audit_defensively_excludes_invalid_timestamp(tmp_path, monkeypatch):
    from tools import audit_dashboard_flow_observer as audit_tool
    monkeypatch.setattr(audit_tool, "load_universe", lambda *args: ([ROW], "DATED_UNIVERSE"))
    monkeypatch.setattr(audit_tool, "read_events", lambda *args: [{**ROW, "kind": "future", "oi": 1000,
        "exchange_timestamp": "invalid", "received_at": moment("09:14:59").isoformat()}])
    result = audit_tool.audit(tmp_path, DAY)
    assert result["opening_oi_available"] == 0
    assert result["exact_opening_oi_available"] == 0
