from datetime import date, datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import dashboard_flow_daily_backfill as daily

IST = ZoneInfo("Asia/Kolkata")
NOW = datetime(2026, 10, 12, 11, 0, tzinfo=IST)


def universe(root, symbols=("ALPHA", "BETA"), day="2026-10-12"):
    path = root / "fno_oi" / "universe" / f"near_month_{day}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([dict(equity_symbol=symbol, equity_instrument_token=100 + index,
                       equity_exchange="NSE", is_index_future=False, master_date=day) for index, symbol in enumerate(symbols)]).to_parquet(path)
    return path


def candle(day, close=103, **extra):
    return dict(date=datetime.fromisoformat(day).replace(tzinfo=IST), open=100.0, high=110.0,
                low=95.0, close=close, volume=1000, **extra)


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.value += seconds

    def monotonic(self):
        return self.value


class FakeClient:
    def __init__(self, rows, clock=None, failures=None):
        self.rows, self.calls, self.clock, self.failures = rows, [], clock, list(failures or [])

    def historical_data(self, **request):
        self.calls.append((request, self.clock.value if self.clock else 0))
        if self.failures:
            raise self.failures.pop(0)
        return [row for row in self.rows if request["from_date"].date() <= row["date"].date() <= request["to_date"].date()]


def run(root, client, **kwargs):
    clock = kwargs.pop("clock", FakeClock())
    return daily.backfill(root, "2026-10-08", "2026-10-09", fetch=True, client=client,
                          now=NOW, sleep=clock.sleep, monotonic=clock.monotonic, **kwargs)


def test_dry_run_has_no_network_or_daily_write_and_uses_newest_saved_equity_mapping(tmp_path):
    universe(tmp_path, symbols=("OLD",), day="2026-10-09")
    source = universe(tmp_path)
    client = FakeClient([])
    result = daily.backfill(tmp_path, "2026-10-08", "2026-10-09", client=client, now=NOW)
    assert result["mode"] == "DRY_RUN" and result["calls"] == 0
    assert result["universe"].endswith(source.name)
    assert [row["symbol"] for row in result["symbols"]] == ["ALPHA", "BETA"]
    assert all(row["status"] == "PLANNED" for row in result["symbols"])
    assert not list((tmp_path / "fno_oi" / "raw_equity_daily").glob("*.parquet"))
    assert client.calls == []
    assert json.loads(Path(result["audit_path"]).read_text())["execution_authority"] is False


def test_fetch_writes_valid_daily_schema_atomic_and_reuses_without_second_network_request(tmp_path):
    universe(tmp_path)
    clock = FakeClock()
    client = FakeClient([candle("2026-10-08"), candle("2026-10-09", close=104)], clock)
    result = run(tmp_path, client, clock=clock)
    assert result["state"] == "COMPLETE" and result["calls"] == 2
    assert [stamp for _, stamp in client.calls] == [0.0, 0.45]
    for symbol in ("ALPHA", "BETA"):
        path = tmp_path / "fno_oi" / "raw_equity_daily" / (symbol + "_day.parquet")
        frame = pd.read_parquet(path)
        assert list(frame.columns) == list(daily.COLUMNS)
        assert frame["source"].tolist() == [daily.SOURCE] * 2
        assert frame["quality_state"].tolist() == ["VALID"] * 2
        assert str(frame["date"].dt.tz) == "Asia/Kolkata"
        assert frame["date"].dt.hour.tolist() == [0, 0]
    result2 = run(tmp_path, client, clock=clock)
    assert result2["calls"] == 0 and result2["summary"] == {"UP_TO_DATE": 2}
    assert len(client.calls) == 2
    assert not list((tmp_path / "fno_oi" / "raw_equity_daily").glob("*.tmp"))
    assert all(call["interval"] == "day" and call["continuous"] is False and call["oi"] is False for call, _ in client.calls)


def test_incremental_fetch_only_requests_missing_session_and_keeps_prior_row(tmp_path):
    universe(tmp_path, symbols=("ALPHA",))
    client = FakeClient([candle("2026-10-08", close=101)])
    first = run(tmp_path, client)
    assert first["state"] == "PARTIAL"
    assert first["symbols"][0]["missing_sessions"] == ["2026-10-09"]
    second = FakeClient([candle("2026-10-08", close=109), candle("2026-10-09", close=104)])
    result = run(tmp_path, second)
    assert result["state"] == "COMPLETE"
    assert second.calls[0][0]["from_date"].date() == date(2026, 10, 9)
    stored = pd.read_parquet(tmp_path / "fno_oi" / "raw_equity_daily" / "ALPHA_day.parquet")
    assert stored["close"].tolist() == [101, 104]


@pytest.mark.parametrize("rows, code", [
    ([candle("2026-10-08"), candle("2026-10-08")], "DUPLICATE_SESSION_DATES"),
    ([{**candle("2026-10-08"), "high": 99}], "INVALID_OHLC_BOUNDS"),
    ([{**candle("2026-10-08"), "close": float("nan")}], "INVALID_OHLC_VALUE"),
    ([candle("2026-10-08", gap_filled=True)], "SYNTHETIC_DAILY_ROW"),
    ([candle("2026-10-08", quality_state="GAP_FILLED")], "INVALID_DAILY_QUALITY"),
    ([{**candle("2026-10-08"), "volume": -1}], "INVALID_OHLC_BOUNDS"),
])
def test_malformed_broker_rows_fail_closed_without_writing(tmp_path, rows, code):
    universe(tmp_path, symbols=("ALPHA",))
    result = run(tmp_path, FakeClient(rows))
    assert result["symbols"][0]["error_code"] == code
    assert not (tmp_path / "fno_oi" / "raw_equity_daily" / "ALPHA_day.parquet").exists()


def test_existing_corruption_is_preserved_and_prevents_fetch(tmp_path):
    universe(tmp_path, symbols=("ALPHA",))
    target = tmp_path / "fno_oi" / "raw_equity_daily" / "ALPHA_day.parquet"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"original invalid parquet bytes")
    client = FakeClient([candle("2026-10-08")])
    result = run(tmp_path, client)
    assert result["symbols"][0]["error_code"] == "EXISTING_DAILY_FILE_UNREADABLE"
    assert target.read_bytes() == b"original invalid parquet bytes" and not client.calls


def test_retry_is_bounded_paced_and_error_details_never_leak(tmp_path):
    universe(tmp_path, symbols=("ALPHA",))
    clock = FakeClock()
    client = FakeClient([], clock, failures=[RuntimeError("SECRET-API-TOKEN") for _ in range(4)])
    result = run(tmp_path, client, clock=clock)
    assert result["calls"] == 3
    assert result["symbols"][0]["error_code"] == "BROKER_READ_FAILED"
    assert "SECRET" not in json.dumps(result) and "SECRET" not in Path(result["audit_path"]).read_text()
    stamps = [stamp for _, stamp in client.calls]
    assert all(b - a >= 0.4 for a, b in zip(stamps, stamps[1:]))


def test_authentication_failure_stops_other_symbols_without_retry(tmp_path):
    universe(tmp_path)
    TokenException = type("TokenException", (Exception,), {})
    client = FakeClient([], failures=[TokenException("sensitive response")])
    result = run(tmp_path, client)
    assert len(client.calls) == 1
    assert result["symbols"][0]["error_code"] == "BROKER_AUTHENTICATION_FAILED"
    assert result["symbols"][1]["error_code"] == "SKIPPED_AFTER_AUTHENTICATION_OR_CLIENT_FAILURE"


def test_unfinished_future_and_out_of_range_rows_are_rejected():
    for value in ("2026-10-12", "2026-10-13"):
        with pytest.raises(daily.BackfillError, match="UNFINISHED_OR_FUTURE_SESSION"):
            daily.validate_daily_frame(pd.DataFrame([candle(value)]), now=NOW, provenance=False)
    with pytest.raises(daily.BackfillError, match="OUT_OF_REQUEST_RANGE"):
        daily.validate_daily_frame(pd.DataFrame([candle("2026-10-07")]), now=NOW, provenance=False,
                                   start_date=date(2026, 10, 8), end_date=date(2026, 10, 9))


def test_calendar_skips_holidays_and_auto_cutoff_uses_completed_regular_session(tmp_path):
    assert daily.requested_sessions(date(2026, 10, 1), date(2026, 10, 5)) == [date(2026, 10, 1), date(2026, 10, 5)]
    assert daily.latest_completed_session(NOW) == date(2026, 10, 9)
    assert daily.latest_completed_session(datetime(2026, 10, 12, 16, 5, tzinfo=IST)) == date(2026, 10, 12)
    universe(tmp_path, symbols=("ALPHA",))
    result = daily.backfill(tmp_path, "2026-10-08", now=NOW)
    assert result["end_date"] == "2026-10-09"
    with pytest.raises(daily.BackfillError, match="UNFINISHED_OR_FUTURE_REQUEST"):
        daily.backfill(tmp_path, "2026-10-08", "2026-10-12", now=NOW)


def test_bounded_windows_split_at_cached_sessions_and_range_limit():
    days = [date(2026, 10, day) for day in (5, 6, 7, 8, 9)]
    assert daily.missing_windows(days, {date(2026, 10, 7)}) == [(date(2026, 10, 5), date(2026, 10, 6)), (date(2026, 10, 8), date(2026, 10, 9))]
    assert daily.missing_windows(days, set(), chunk_days=2) == [(days[0], days[1]), (days[2], days[3]), (days[4], days[4])]


def test_duplicate_conflicting_equity_mapping_and_bad_symbols_are_rejected(tmp_path):
    path = universe(tmp_path, symbols=("ALPHA", "ALPHA"))
    with pytest.raises(daily.BackfillError, match="CONFLICTING_EQUITY_TOKENS"):
        daily.load_equity_tokens(tmp_path, now=NOW)
    path.unlink()
    universe(tmp_path, symbols=("ALPHA",))
    with pytest.raises(daily.BackfillError, match="INVALID_SYMBOL_SELECTION"):
        daily.load_equity_tokens(tmp_path, now=NOW, symbols=["../ALPHA"])


def test_replace_failure_preserves_existing_file(tmp_path, monkeypatch):
    universe(tmp_path, symbols=("ALPHA",))
    run(tmp_path, FakeClient([candle("2026-10-08")]))
    target = tmp_path / "fno_oi" / "raw_equity_daily" / "ALPHA_day.parquet"
    original = target.read_bytes()
    replace = daily.os.replace
    def fail_parquet(src, dst):
        if Path(dst).suffix == ".parquet":
            raise OSError("private file details")
        return replace(src, dst)
    monkeypatch.setattr(daily.os, "replace", fail_parquet)
    result = run(tmp_path, FakeClient([candle("2026-10-09")]))
    assert result["symbols"][0]["error_code"] == "ATOMIC_DAILY_WRITE_FAILED"
    assert target.read_bytes() == original


def test_explicit_retained_universe_fetches_old_symbol_without_changing_default_mapping(tmp_path):
    old = universe(tmp_path, symbols=("DALBHARAT",), day="2026-08-20")
    current = universe(tmp_path, symbols=("ALPHA",))
    default, source = daily.load_equity_tokens(tmp_path, now=NOW)
    assert default == {"ALPHA": 100} and source == current
    client = FakeClient([candle("2026-10-08"), candle("2026-10-09")])
    result = run(tmp_path, client, symbols=["DALBHARAT"], universe_path=old)
    assert result["state"] == "COMPLETE"
    assert result["universe"].endswith(old.name)
    assert [row["symbol"] for row in result["symbols"]] == ["DALBHARAT"]
    assert client.calls[0][0]["instrument_token"] == 100
    assert (tmp_path / "fno_oi" / "raw_equity_daily" / "DALBHARAT_day.parquet").exists()


def test_retained_universe_requires_local_dated_path_matching_past_master_date_and_nse_exchange(tmp_path):
    old = universe(tmp_path, symbols=("DALBHARAT",), day="2026-08-20")
    frame = pd.read_parquet(old)
    frame["master_date"] = "2026-08-21"
    frame.to_parquet(old)
    with pytest.raises(daily.BackfillError, match="INVALID_UNIVERSE_MASTER_DATE"):
        daily.load_equity_tokens(tmp_path, now=NOW, universe_path=old)
    frame["master_date"] = "2026-08-20"
    frame["equity_exchange"] = "BSE"
    frame.to_parquet(old)
    with pytest.raises(daily.BackfillError, match="SYMBOL_NOT_IN_SAVED_UNIVERSE"):
        daily.load_equity_tokens(tmp_path, now=NOW, universe_path=old, symbols=["DALBHARAT"])
    future = universe(tmp_path, symbols=("DALBHARAT",), day="2026-10-13")
    with pytest.raises(daily.BackfillError, match="INVALID_RETAINED_UNIVERSE_PATH"):
        daily.load_equity_tokens(tmp_path, now=NOW, universe_path=future)
    outside = tmp_path / "near_month_2026-08-20.parquet"
    frame.to_parquet(outside)
    with pytest.raises(daily.BackfillError, match="INVALID_RETAINED_UNIVERSE_PATH"):
        daily.load_equity_tokens(tmp_path, now=NOW, universe_path=outside)

