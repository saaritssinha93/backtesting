import json

import numpy as np
import pandas as pd
import pytest

import data_for_backtesting_verify as verify


DAY = "2026-10-09"
AFTER_CLOSE = f"{DAY} 15:45"


def candles(*, five_minute=True, day=DAY, through="15:30", column="date"):
    return pd.DataFrame({
        column: pd.date_range(f"{day} {'09:20' if five_minute else '09:16'}", f"{day} {through}",
                              freq="5min" if five_minute else "1min", tz="Asia/Kolkata"),
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 100,
    })


def write(root, frame=None, *, ticker="TEST", five_minute=True):
    if frame is None:
        frame = candles(five_minute=five_minute)
    path = root / f"{ticker}_stocks_indicators_{'5min' if five_minute else '1min'}.parquet"
    frame.to_parquet(path, index=False)
    return path


def scan(root, *, five_minute=True, now=AFTER_CLOSE, **kwargs):
    return verify._scan_parquet_dir(root, DAY, 76 if five_minute else 375, now=now, **kwargs)


@pytest.mark.parametrize("column", ["date", "datetime", "timestamp"])
def test_supported_timestamp_columns_and_75_closed_bars_pass_without_snapshot(tmp_path, column):
    write(tmp_path, candles(column=column))
    result = scan(tmp_path)
    assert result["overall"] == "PASS"
    assert result["ok"] == result["closing_bar_verified"] == 1
    assert result["expected_bars"] == 75
    assert result["expected_first_timestamp"].endswith("09:20:00+05:30")
    assert result["expected_last_timestamp"].endswith("15:30:00+05:30")


@pytest.mark.parametrize("timezone", [None, "UTC"])
def test_naive_ist_and_utc_timestamps_use_same_session_grid(tmp_path, timezone):
    frame = candles()
    frame.date = frame.date.dt.tz_localize(None) if timezone is None else frame.date.dt.tz_convert(timezone)
    write(tmp_path, frame)
    assert scan(tmp_path)["overall"] == "PASS"


def test_optional_flagged_opening_snapshot_is_not_a_required_or_counted_candle(tmp_path):
    frame = candles()
    frame["opening_snapshot"] = False
    frame["source_1m_count"] = 5
    snapshot = frame.iloc[:1].copy()
    snapshot["date"] = pd.Timestamp(f"{DAY} 09:15", tz="Asia/Kolkata")
    snapshot["opening_snapshot"] = True
    snapshot["source_1m_count"] = 0
    snapshot["close"] = np.nan
    write(tmp_path, pd.concat([snapshot, frame], ignore_index=True))
    result = scan(tmp_path)
    assert result["overall"] == "PASS"
    assert result["expected_bars"] == 75


def test_scope_excludes_unrelated_archives(tmp_path):
    write(tmp_path, ticker="FNO1")
    write(tmp_path, candles().iloc[:3], ticker="OLDARCHIVE")
    result = scan(tmp_path, required_tickers={"FNO1"})
    assert result["overall"] == "PASS"
    assert result["total_tickers"] == result["required_tickers"] == 1


def test_missing_required_ticker_fails_closed(tmp_path):
    write(tmp_path, ticker="FNO1")
    result = scan(tmp_path, required_tickers={"FNO1", "MISSING"})
    assert result["overall"] == "FAIL"
    assert result["fail"] == 1
    assert result["worst_tickers"]["MISSING"]["missing_candles"] == 75


@pytest.mark.parametrize("five_minute", [True, False])
def test_interior_gap_fails_despite_valid_close_and_near_full_coverage(tmp_path, five_minute):
    frame = candles(five_minute=five_minute)
    missing = frame.iloc[8].date.isoformat()
    write(tmp_path, frame.drop(index=8), five_minute=five_minute)
    result = scan(tmp_path, five_minute=five_minute)
    row = result["worst_tickers"]["TEST"]
    assert result["overall"] == "FAIL"
    assert result["exit_code"] == 2
    assert row["closing_bar"] == "VERIFIED"
    assert row["missing_timestamps"] == [missing]
    assert row["missing_candles"] == 1


def test_one_interior_gap_cannot_be_hidden_among_many_good_tickers(tmp_path):
    for number in range(10):
        write(tmp_path, ticker=f"GOOD{number}")
    write(tmp_path, candles().drop(index=8), ticker="BAD")
    result = scan(tmp_path)
    assert result["closing_bar_verified"] == 11
    assert result["ok"] == 10
    assert result["fail"] == 1
    assert result["overall"] == "FAIL"
    assert result["exit_code"] == 2


def test_96_percent_minute_coverage_cannot_waive_missing_close(tmp_path):
    write(tmp_path, candles(five_minute=False, through="15:15"), five_minute=False)
    result = scan(tmp_path, five_minute=False)
    assert result["overall"] == "FAIL"
    assert result["closing_bar_failed"] == 1
    assert result["worst_tickers"]["TEST"]["closing_bar"] == "MISSING"
    assert result["worst_tickers"]["TEST"]["missing_candles"] == 15


@pytest.mark.parametrize("flag", verify.QUALITY_FLAGS)
@pytest.mark.parametrize("five_minute", [True, False])
def test_flagged_interior_candle_is_not_real_even_when_grid_is_full(tmp_path, flag, five_minute):
    frame = candles(five_minute=five_minute)
    frame[flag] = "false"
    frame.loc[8, flag] = "true"
    write(tmp_path, frame, five_minute=five_minute)
    result = scan(tmp_path, five_minute=five_minute)
    row = result["worst_tickers"]["TEST"]
    assert result["exit_code"] == 2
    assert row["invalid_candles"] == 1
    assert row["invalid_reasons"] == {"flagged_" + flag: 1}
    assert row["closing_bar"] == "VERIFIED"


def test_synthetic_closing_bar_retains_explicit_failure_reason(tmp_path):
    frame = candles()
    frame["gap_filled"] = 0
    frame.loc[frame.index[-3:], "gap_filled"] = 1
    write(tmp_path, frame)
    result = scan(tmp_path)
    assert result["exit_code"] == 2
    assert result["worst_tickers"]["TEST"]["closing_bar"] == "FLAGGED_GAP_FILLED"


@pytest.mark.parametrize("column,value,reason", [
    ("quality_state", "INVALID", "invalid_quality_state"),
    ("quality_state", " partial ", "invalid_quality_state"),
    ("quality_state", "SYNTHETIC_REPAIR", "invalid_quality_state"),
    ("source", "last_close_PROXY", "invalid_source"),
    ("source", "synthetic_repair", "invalid_source"),
    ("source", "gap-filled", "invalid_source"),
    ("source", "forward_fill", "invalid_source"),
    ("source", "INTERPOLATED", "invalid_source"),
    ("source", "carry forward price", "invalid_source"),
])
@pytest.mark.parametrize("five_minute", [True, False])
def test_explicit_bad_metadata_rejects_otherwise_valid_candle(tmp_path, column, value, reason, five_minute):
    frame = candles(five_minute=five_minute)
    frame[column] = None
    frame.loc[8, column] = value
    write(tmp_path, frame, five_minute=five_minute)
    result = scan(tmp_path, five_minute=five_minute)
    assert result["exit_code"] == 2
    assert result["worst_tickers"]["TEST"]["invalid_reasons"] == {reason: 1}


@pytest.mark.parametrize("column,value,reason", [
    ("quality_state", "INVALID", "INVALID_QUALITY_STATE"),
    ("source", "close_proxy", "INVALID_SOURCE"),
])
def test_explicit_bad_closing_metadata_has_specific_failure_reason(tmp_path, column, value, reason):
    frame = candles()
    frame[column] = None
    frame.loc[frame.index[-1], column] = value
    write(tmp_path, frame)
    result = scan(tmp_path)
    assert result["exit_code"] == 2
    assert result["closing_bar_failed"] == 1
    assert result["worst_tickers"]["TEST"]["closing_bar"] == reason


@pytest.mark.parametrize("quality_state,source", [
    (None, None), ("", ""), (" valid ", "broker_historical_backfill"),
    ("VALID", "broker_repair"), (None, "KITE_HISTORICAL_MINUTE"),
])
def test_legacy_unspecified_and_valid_real_recovery_metadata_remain_eligible(tmp_path, quality_state, source):
    frame = candles()
    frame["quality_state"] = quality_state
    frame["source"] = source
    write(tmp_path, frame)
    result = scan(tmp_path)
    assert result["overall"] == "PASS"
    assert result["closing_bar_verified"] == 1


@pytest.mark.parametrize("column,value", [
    ("open", np.nan), ("high", np.inf), ("low", 0.0), ("close", -1.0),
    ("volume", -1.0), ("volume", np.inf), ("volume", np.nan),
    ("high", 100.0), ("low", 100.1),
])
def test_invalid_finite_positive_price_or_nonnegative_volume_contract_fails(tmp_path, column, value):
    frame = candles()
    frame[column] = frame[column].astype(float)
    frame.loc[8, column] = value
    write(tmp_path, frame)
    result = scan(tmp_path)
    assert result["exit_code"] == 2
    assert result["worst_tickers"]["TEST"]["invalid_reasons"] == {"invalid_ohlcv": 1}


def test_missing_ohlcv_schema_cannot_pass_on_timestamps_and_close_alone(tmp_path):
    write(tmp_path, candles()[["date", "close"]])
    result = scan(tmp_path)
    assert result["exit_code"] == 2
    assert result["worst_tickers"]["TEST"]["invalid_candles"] == 75


@pytest.mark.parametrize("source_count", [4.0, 0.0, np.nan])
def test_provided_5m_minute_provenance_must_prove_all_five_inputs(tmp_path, source_count):
    frame = candles()
    frame["source_1m_count"] = 5.0
    frame.loc[8, "source_1m_count"] = source_count
    write(tmp_path, frame)
    result = scan(tmp_path)
    assert result["exit_code"] == 2
    assert result["worst_tickers"]["TEST"]["invalid_reasons"] == {"incomplete_minute_source": 1}


def test_duplicate_timestamp_cannot_replace_missing_candle(tmp_path):
    frame = candles()
    frame.loc[8, "date"] = frame.loc[7, "date"]
    write(tmp_path, frame)
    result = scan(tmp_path)
    row = result["worst_tickers"]["TEST"]
    assert result["exit_code"] == 2
    assert row["status"] == "DUP"
    assert row["duplicates"] == row["missing_candles"] == 1


def test_off_grid_timestamp_cannot_replace_missing_candle(tmp_path):
    frame = candles()
    frame.loc[8, "date"] += pd.Timedelta(seconds=30)
    write(tmp_path, frame)
    result = scan(tmp_path)
    row = result["worst_tickers"]["TEST"]
    assert result["exit_code"] == 2
    assert row["off_grid_candles"] == row["missing_candles"] == 1


def test_unparseable_timestamp_cannot_be_silently_dropped(tmp_path):
    frame = candles()
    frame.loc[8, "date"] = pd.NaT
    write(tmp_path, frame)
    result = scan(tmp_path)
    assert result["exit_code"] == 2
    assert result["worst_tickers"]["TEST"]["invalid_timestamps"] == 1


@pytest.mark.parametrize("five_minute,expected", [(True, 2), (False, 11)])
def test_current_partial_session_checks_only_completed_candles(tmp_path, five_minute, expected):
    write(tmp_path, candles(five_minute=five_minute, through="09:26"), five_minute=five_minute)
    result = scan(tmp_path, five_minute=five_minute, now=f"{DAY} 09:26:30")
    assert result["overall"] == "PASS"
    assert result["expected_bars"] == expected
    assert result["coverage_state"] == "IN_PROGRESS"
    assert result["closing_bar_requirement"] == "NOT_DUE"


def test_future_rows_do_not_enter_current_partial_quality_audit(tmp_path):
    frame = candles(through="09:30")
    frame.loc[frame.index[-1], "close"] = np.nan
    write(tmp_path, frame)
    result = scan(tmp_path, now=f"{DAY} 09:26:30")
    assert result["overall"] == "PASS"
    assert result["expected_bars"] == 2


def test_live_gap_still_fails_when_later_future_rows_are_present(tmp_path):
    write(tmp_path, candles(through="09:35").drop(index=0))
    result = scan(tmp_path, now=f"{DAY} 09:26:30")
    assert result["exit_code"] == 2
    assert result["worst_tickers"]["TEST"]["missing_candles"] == 1


@pytest.mark.parametrize("day,now,state", [
    (DAY, f"{DAY} 09:15:59", "NOT_DUE"),
    ("2026-10-13", "2026-10-12 16:00", "NOT_DUE"),
    ("2026-10-02", "2026-10-12 16:00", "NOT_A_SESSION"),
    ("2026-10-10", "2026-10-12 16:00", "NOT_A_SESSION"),
])
def test_non_due_dates_do_not_require_files_or_claim_full_session_pass(tmp_path, day, now, state):
    result = verify._scan_parquet_dir(tmp_path / "missing", day, 375, required_tickers={"TEST"}, now=now)
    assert result["exit_code"] == 0
    assert result["overall"] == result["coverage_state"] == state
    assert result["expected_bars"] == 0


def test_regular_special_sunday_session_requires_completed_grid_and_close(tmp_path):
    day = "2026-02-01"
    write(tmp_path, candles(day=day))
    result = verify._scan_parquet_dir(tmp_path, day, 76, now=f"{day} 15:45")
    assert result["overall"] == "PASS"
    assert result["expected_bars"] == 75
    assert verify._closing_bar_requirement(day, f"{day} 15:45") == "REQUIRED"
    assert verify._closing_bar_requirement(day, f"{day} 12:00") == "NOT_DUE"
    assert verify._closing_bar_requirement("2026-02-08", "2026-02-09") == "NOT_A_SESSION"


def test_calendar_fallback_holidays_and_muhurat_do_not_become_regular_sessions(tmp_path, monkeypatch):
    monkeypatch.setattr("nse_market_calendar.HOLIDAY_CSV", tmp_path / "missing.csv")
    assert verify._closing_bar_requirement("2026-10-02", "2026-10-12") == "NOT_A_SESSION"
    assert verify._closing_bar_requirement("2026-11-08", "2026-11-09") == "NOT_A_SESSION"


def test_scoped_holiday_report_needs_no_dated_universe(tmp_path, monkeypatch):
    monkeypatch.setattr(verify, "VERIFY_DIR", tmp_path)
    monkeypatch.setattr(verify, "_fno_equity_universe", lambda _: pytest.fail("Holiday must not load a dated universe"))
    assert verify.run_verify("2026-10-02", "fno", now="2026-10-12") == 0
    payload = json.loads((tmp_path / "data_verify_2026-10-02.json").read_text())
    assert payload["overall_status"] == "NOT_A_SESSION"


def test_missing_store_fails_when_candles_are_due(tmp_path):
    result = scan(tmp_path / "missing")
    assert result["overall"] == "FAIL"
    assert result["exit_code"] == 2


@pytest.mark.parametrize("override,expected", [(False, 2), (True, 0)])
def test_cli_failure_is_nonzero_except_explicit_legacy_override(monkeypatch, capsys, override, expected):
    monkeypatch.setattr(verify, "run_verify", lambda *args, **kwargs: 2)
    monkeypatch.setattr("sys.argv", ["data_for_backtesting_verify.py", "--date", DAY] + (["--no-fail"] if override else []))
    assert verify.main() == expected
    if override:
        assert "EXPLICIT --no-fail override" in capsys.readouterr().out
