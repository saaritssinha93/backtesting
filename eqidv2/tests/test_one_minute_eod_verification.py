from datetime import datetime
import logging
from unittest.mock import Mock

import pandas as pd
import pytest

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min as collector


LOGGER = logging.getLogger("test_one_minute_eod")


def stamp(clock):
    return pd.Timestamp(f"2026-10-09 {clock}", tz="Asia/Kolkata")


def bar(clock="15:30", **changes):
    values = {"date": stamp(clock), "open": 100.0, "high": 101.0,
              "low": 99.0, "close": 100.0, "volume": 10.0}
    values.update(changes)
    return pd.DataFrame([values])


@pytest.fixture
def saved_bar(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, "DIRS", {"1min": {"out": str(tmp_path), "cache": str(tmp_path)}})
    monkeypatch.setenv("EQIDV2_1MIN_SAME_DAY_STALE_OK", "1")
    monkeypatch.setenv("EQIDV2_1MIN_STALE_OK_MIN", "60")
    path = tmp_path / "TEST_stocks_indicators_1min.parquet"
    bar("15:15").to_parquet(path, index=False)
    return path


def test_eod_verification_ignores_intraday_same_day_and_lag_tolerances(saved_bar):
    ok, failed = collector.verify_mode_outputs("1min", ["TEST"], stamp("15:30"), LOGGER)
    assert ok == 0
    assert failed and "stale_last_ts" in failed[0]
    intraday_ok, intraday_failed = collector.verify_mode_outputs("1min", ["TEST"], stamp("15:20"), LOGGER)
    assert intraday_ok == 1
    assert intraday_failed == []


def test_eod_freshness_does_not_skip_the_missing_last_minute(saved_bar):
    bar("15:29").to_parquet(saved_bar, index=False)
    assert not collector.ticker_is_fresh("1min", str(saved_bar), stamp("15:45"), set(), "end")
    bar().to_parquet(saved_bar, index=False)
    assert collector.ticker_is_fresh("1min", str(saved_bar), stamp("15:45"), set(), "end")


@pytest.mark.parametrize("flag", collector.OHLC_QUALITY_FLAGS)
def test_flagged_eod_row_is_refetched_and_cannot_pass_timestamp_check(saved_bar, flag):
    bar(**{flag: 1}).to_parquet(saved_bar, index=False)
    assert not collector.ticker_is_fresh("1min", str(saved_bar), stamp("15:45"), set(), "end")
    ok, failed = collector.verify_mode_outputs("1min", ["TEST"], stamp("15:30"), LOGGER)
    assert ok == 0
    assert failed == [f"TEST:closing_bar=FLAGGED_{flag.upper()}"]


@pytest.mark.parametrize("close", [float("nan"), float("inf"), 0, -1])
def test_invalid_closing_price_cannot_pass_eod_verification(saved_bar, close):
    bar(close=close).to_parquet(saved_bar, index=False)
    ok, failed = collector.verify_mode_outputs("1min", ["TEST"], stamp("15:30"), LOGGER)
    assert ok == 0
    assert failed == ["TEST:closing_bar=INVALID_PRICE"]


def test_future_timestamp_cannot_stand_in_for_missing_exact_close(saved_bar):
    bar("15:31").to_parquet(saved_bar, index=False)
    ok, failed = collector.verify_mode_outputs("1min", ["TEST"], stamp("15:30"), LOGGER)
    assert ok == 0
    assert failed == ["TEST:closing_bar=MISSING"]


@pytest.mark.parametrize("changes,reason", [
    ({"open": float("nan")}, "INVALID_OHLCV"),
    ({"high": 98.0}, "INVALID_OHLCV"),
    ({"low": 102.0}, "INVALID_OHLCV"),
    ({"volume": -1}, "INVALID_OHLCV"),
    ({"volume": 0.5}, "INVALID_OHLCV"),
    ({"volume": True}, "INVALID_OHLCV"),
    ({"source": "PREVIOUS_CLOSE_PROXY"}, "SYNTHETIC_SOURCE"),
    ({"source": "CARRY_FORWARD"}, "SYNTHETIC_SOURCE"),
    ({"source": "carry-forward"}, "SYNTHETIC_SOURCE"),
    ({"quality_state": "SYNTHETIC"}, "INVALID_QUALITY"),
])
def test_full_closing_quality_is_shared_with_fetched_validation(saved_bar, changes, reason):
    frame = bar(**changes)
    frame.to_parquet(saved_bar, index=False)
    assert collector._closing_row_quality(str(saved_bar), stamp("15:30")) == reason
    assert not collector.ticker_is_fresh("1min", str(saved_bar), stamp("15:45"), set(), "end")
    with pytest.raises(ValueError, match=reason):
        collector._prepare_fetched_ohlc(frame, [], stamp("15:30"), stamp("15:30"))


def test_closing_verification_requires_full_ohlcv_and_unique_observation(saved_bar):
    bar().drop(columns="volume").to_parquet(saved_bar, index=False)
    assert collector._closing_row_quality(str(saved_bar), stamp("15:30")) == "MISSING_FIELDS"
    pd.concat([bar(), bar()], ignore_index=True).to_parquet(saved_bar, index=False)
    assert collector._closing_row_quality(str(saved_bar), stamp("15:30")) == "DUPLICATE"


def test_csv_closing_quality_uses_same_metadata_projection(saved_bar):
    path = saved_bar.with_suffix(".csv")
    bar(source="CARRY FORWARD").to_csv(path, index=False)
    assert collector._closing_row_quality(str(path), stamp("15:30")) == "SYNTHETIC_SOURCE"
    bar(source="KITE_HISTORICAL_1M", quality_state="VALID", is_synthetic=False).to_csv(path, index=False)
    assert collector._closing_row_quality(str(path), stamp("15:30")) == "VERIFIED"


def test_incremental_reload_cannot_erase_flag_on_an_unrepaired_closing_row(saved_bar):
    frame = pd.DataFrame({"date": [stamp("15:30")], "open": [100.0], "high": [100.0], "low": [100.0], "close": [100.0], "volume": [0.0], "gap_filled": [1]})
    frame.to_parquet(saved_bar, index=False)
    existing = collector._load_existing_ohlc(str(saved_bar), "end", "1min")
    # An unsuccessful recovery returning only an earlier real minute must not
    # strip the quality marker from the retained, still-unrepaired close.
    earlier = pd.DataFrame({"date": [stamp("15:15")], "open": [100.0], "high": [101.0], "low": [99.0], "close": [100.0], "volume": [10.0]})
    pd.concat([existing, earlier]).sort_values("date").to_parquet(saved_bar, index=False)
    assert collector._closing_row_quality(str(saved_bar), stamp("15:30")) == "FLAGGED_GAP_FILLED"


def test_eod_worker_cap_does_not_change_explicit_intraday_recovery_limits():
    assert collector._recovery_worker_count(10, 213, stamp("15:30"), "end") == 3
    assert collector._recovery_worker_count(20, 213, stamp("15:20"), "end") == 10
    assert collector._recovery_worker_count(2, 213, stamp("15:30"), "end") == 2
    assert collector._recovery_worker_count(10, 1, stamp("15:30"), "end") == 1


def test_eod_recovery_is_default_but_intraday_recovery_remains_opt_in(monkeypatch):
    monkeypatch.delenv("EQIDV2_1MIN_STRICT_VERIFY_RECOVERY", raising=False)
    monkeypatch.delenv("EQIDV2_1MIN_EOD_VERIFY_RECOVERY", raising=False)
    assert collector._verify_recovery_enabled(stamp("15:30"))
    assert not collector._verify_recovery_enabled(stamp("15:20"))
    monkeypatch.setenv("EQIDV2_1MIN_EOD_VERIFY_RECOVERY", "0")
    assert not collector._verify_recovery_enabled(stamp("15:30"))


def test_unresolved_close_is_failure_even_if_optional_recovery_disabled():
    with pytest.raises(RuntimeError, match="End-of-session data incomplete for 1"):
        collector._require_verified_close("1min", ["TEST:stale"], stamp("15:30"))
    collector._require_verified_close("1min", ["TEST:stale"], stamp("15:20"))
    collector._require_verified_close("1min", [], stamp("15:30"))


def test_start_stamped_close_is_1529_not_intraday_1528():
    assert collector._is_closing_target(stamp("15:29"), "start")
    assert not collector._is_closing_target(stamp("15:28"), "start")
    assert not collector._is_closing_target(stamp("15:29"), "end")


@pytest.mark.parametrize("last_minute,expected_end", [(14, "15:15"), (29, "15:30")])
def test_fetch_requests_full_close_preserves_actual_broker_tail_without_filling(monkeypatch, last_minute, expected_end):
    monkeypatch.setattr(collector._time, "sleep", lambda _: None)
    broker = Mock()
    broker.historical_data.return_value = [
        {"date": stamp(f"15:{minute:02d}"), "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10}
        for minute in range(10, last_minute + 1)
    ]
    result = collector.fetch_historical_1min_df(broker, 123, stamp("15:10"), stamp("15:30"), LOGGER, "end")
    assert broker.historical_data.call_args.args[2] == stamp("15:30")
    assert result.date.max() == stamp(expected_end)
    assert len(result) == last_minute - 10 + 1


def test_after_market_run_retries_then_fails_nonzero_without_real_broker(tmp_path, monkeypatch):
    class ClosedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return stamp("15:45").to_pydatetime()

    monkeypatch.setattr(collector, "datetime", ClosedClock)
    monkeypatch.setattr(collector, "load_stocks_universe", lambda *_, **__: (["TEST"], {"TEST": 123}))
    monkeypatch.setattr(collector, "setup_kite_session", lambda: object())
    monkeypatch.setattr(collector, "process_ticker", lambda *_, **__: Mock(status="noop", new_rows_count=0, load_existing_secs=0, fetch_secs=0, indicators_secs=0, persist_secs=0, total_secs=0))
    monkeypatch.setattr(collector, "verify_mode_outputs", lambda *_, **__: (0, ["TEST:stale"]))
    recovery = Mock(return_value=0.01)
    monkeypatch.setattr(collector, "_recover_verify_failures", recovery)
    monkeypatch.delenv("EQIDV2_1MIN_EOD_VERIFY_RECOVERY", raising=False)
    with pytest.raises(RuntimeError, match="End-of-session data incomplete"):
        collector.run_mode("1min", 1, False, "end", set(), False, str(tmp_path), False, 0)
    recovery.assert_called_once()
