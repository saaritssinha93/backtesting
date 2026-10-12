"""EOD QC cannot certify incomplete data or replace today's historical health."""
from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd
import pytest

import fno_oi_eod_qc as qc

DAY = date(2026, 10, 9)
TODAY = date(2026, 10, 12)


def contract(**changes):
    value = dict(underlying="TEST", tradingsymbol="TEST26OCTFUT", instrument_token=123,
                 expiry=pd.Timestamp("2026-10-29"), master_date=pd.Timestamp(DAY), is_index_future=False)
    value.update(changes)
    return pd.Series(value)


def candles(day=DAY):
    stamps = qc.common.expected_slot_ends(day)
    return pd.DataFrame(dict(timestamp=stamps, candle_start=stamps-pd.Timedelta(minutes=5),
        open=100., high=102., low=99., close=101., volume=1000., oi=5000., quality_state="VALID"))


@pytest.fixture
def archive(tmp_path, monkeypatch):
    source = tmp_path / "contract.parquet"
    universe_root, latest_root, output_root = [tmp_path/name for name in ("universe", "latest", "eod_qc")]
    for directory in (universe_root, latest_root, output_root):
        directory.mkdir()
    monkeypatch.setattr(qc.common, "raw_contract_path", lambda symbol: source)
    monkeypatch.setattr(qc.common, "UNIVERSE_DIR", universe_root)
    monkeypatch.setattr(qc.common, "LATEST_DIR", latest_root)
    monkeypatch.setattr(qc.common, "QC_DIR", output_root)
    monkeypatch.setattr(qc.common, "now_ist", lambda: datetime(2026,10,12,16,tzinfo=qc.common.IST))
    monkeypatch.setattr(qc.common, "load_holidays", lambda: set())
    published = []
    monkeypatch.setattr(qc.common, "publish_status", lambda *args, **kwargs: published.append((args, kwargs)))
    def forbidden(*args, **kwargs):
        raise AssertionError("Unit tests must never invoke broker repair")
    monkeypatch.setattr(qc, "_repair_universe", forbidden)
    pd.DataFrame([contract()]).to_parquet(universe_root/f"near_month_{DAY}.parquet", index=False)
    pd.DataFrame([contract(master_date=pd.Timestamp(TODAY))]).to_parquet(universe_root/f"near_month_{TODAY}.parquet", index=False)
    # A deliberately wrong latest universe must never affect a dated audit.
    pd.DataFrame([contract(tradingsymbol="LATEST_OTHER", master_date=pd.Timestamp(TODAY))]).to_parquet(universe_root/"latest_near_month.parquet", index=False)
    return source, published


def inspect(archive, frame=None):
    path, _ = archive
    (candles() if frame is None else frame).to_parquet(path, index=False)
    return qc.inspect_contract(contract(), DAY)


def args(day=DAY, *extra):
    return qc.build_parser().parse_args(["--session-date", str(day), "--no-repair", *extra])


def test_exact_clock_finite_data_is_complete(archive):
    result = inspect(archive)
    assert result["state"] == "COMPLETE"
    assert result["valid_candles"] == result["observed_candles"] == result["expected_candles"] == 75
    assert result["missing_valid_candles"] == 0
    assert result["coverage_ratio"] == 1


@pytest.mark.parametrize("column,value,counter", [
    ("open", float("inf"), "invalid_ohlc"), ("high", float("inf"), "invalid_ohlc"),
    ("low", float("-inf"), "invalid_ohlc"), ("close", float("nan"), "invalid_ohlc"),
    ("open", -1, "invalid_ohlc"), ("low", 0, "invalid_ohlc"),
    ("open", 103, "invalid_ohlc"), ("low", 101, "invalid_ohlc"),
    ("volume", float("inf"), "invalid_volume"), ("volume", float("nan"), "invalid_volume"),
    ("volume", -1, "invalid_volume"), ("oi", float("inf"), "missing_or_negative_oi"),
    ("oi", float("nan"), "missing_or_negative_oi"), ("oi", -1, "missing_or_negative_oi"),
])
def test_invalid_ohlcv_and_oi_reject_even_when_all_timestamps_exist(archive, column, value, counter):
    frame = candles()
    frame.loc[10, column] = value
    result = inspect(archive, frame)
    assert result["state"] == "PARTIAL"
    assert result[counter] == 1
    assert result["observed_candles"] == 75
    assert result["valid_candles"] == 74
    assert result["missing_valid_candles"] == 1


def test_zero_volume_and_zero_oi_are_finite_valid_observations(archive):
    frame = candles()
    frame.loc[1, ["volume", "oi"]] = 0
    assert inspect(archive, frame)["state"] == "COMPLETE"


@pytest.mark.parametrize("conflicting", [False, True])
def test_duplicate_slot_is_not_silently_repaired_or_double_counted(archive, conflicting):
    frame = candles()
    duplicate = frame.iloc[[10]].copy()
    if conflicting:
        duplicate["oi"] = 9999
    result = inspect(archive, pd.concat([frame, duplicate], ignore_index=True))
    assert result["state"] == "PARTIAL"
    assert result["duplicate_candles"] == 1
    assert result["observed_candles"] == 75
    assert result["valid_candles"] == 74
    assert result["missing_candles"] == 0


def test_off_grid_extra_bar_prevents_complete(archive):
    frame = candles()
    extra = frame.iloc[[0]].copy()
    extra["timestamp"] += pd.Timedelta(seconds=1)
    result = inspect(archive, pd.concat([frame, extra], ignore_index=True))
    assert result["state"] == "PARTIAL"
    assert result["unexpected_timestamps"] == 1
    assert result["valid_candles"] == 75


def test_unparseable_archive_timestamp_is_visible(archive):
    frame = candles()
    frame["timestamp"] = frame.timestamp.astype(str)
    frame.loc[5, "timestamp"] = "broken"
    result = inspect(archive, frame)
    assert result["invalid_timestamps"] == 1
    assert result["missing_candles"] == 1
    assert result["state"] == "PARTIAL"


@pytest.mark.parametrize("column,value", [("quality_state", "INVALID_VOLUME"), ("quality_state", None),
    ("gap_filled", True), ("opening_snapshot", True), ("provisional_stale", "true"), ("gap_filled", "unknown")])
def test_declared_bad_quality_is_not_overruled_by_plausible_prices(archive, column, value):
    frame = candles()
    if column not in frame:
        frame[column] = False if isinstance(value, bool) else "false"
    frame.loc[3, column] = value
    result = inspect(archive, frame)
    assert result["state"] == "PARTIAL"
    assert result["invalid_quality"] == 1
    assert result["valid_candles"] == 74


@pytest.mark.parametrize("flag", ["gap_filled", "is_gap_filled", "opening_snapshot", "provisional_stale",
    "is_synthetic", "synthetic", "forward_filled", "is_filled", "interpolated", "is_interpolated",
    "synthetic_repair", "repaired_synthetic"])
def test_all_declared_synthetic_flags_reject_complete_looking_data(archive, flag):
    frame = candles()
    frame[flag] = False
    frame.loc[2, flag] = True
    result = inspect(archive, frame)
    assert result["state"] == "PARTIAL"
    assert result["invalid_quality"] == result["missing_valid_candles"] == 1


@pytest.mark.parametrize("source", ["SYNTHETIC", "historical_gap_fill", "GAPFILL",
    "FORWARD_FILLED", "forward-fill", "CARRY_FORWARD", "carry-forward", "INTERPOLATED", "nearest_contract_proxy"])
def test_synthetic_or_proxy_source_is_not_certified_by_valid_quality_label(archive, source):
    frame = candles()
    frame["source"] = "KITE_HISTORICAL_5M"
    frame.loc[2, "source"] = source
    result = inspect(archive, frame)
    assert result["state"] == "PARTIAL"
    assert result["invalid_quality"] == 1
    assert result["valid_candles"] == 74


def test_real_source_with_explicit_false_optional_flags_remains_valid(archive):
    frame = candles()
    frame["source"] = "KITE_HISTORICAL_5M"
    for flag in ("is_gap_filled", "is_synthetic", "synthetic", "forward_filled", "is_filled",
                 "interpolated", "is_interpolated", "synthetic_repair", "repaired_synthetic"):
        frame[flag] = "false"
    assert inspect(archive, frame)["state"] == "COMPLETE"


def test_candle_start_must_be_exactly_five_minutes_before_end(archive):
    frame = candles()
    frame.loc[2, "candle_start"] = frame.loc[2, "timestamp"]
    result = inspect(archive, frame)
    assert result["state"] == "PARTIAL"
    assert result["invalid_candle_start"] == 1


@pytest.mark.parametrize("column,value", [("tradingsymbol", "WRONG"), ("underlying", "OTHER"),
    ("instrument_token", 999), ("expiry", "2026-11-26")])
def test_raw_rows_cannot_masquerade_as_another_contract(archive, column, value):
    frame = candles()
    expected = contract()[column]
    frame[column] = str(expected) if column == "expiry" else expected
    frame.loc[2, column] = value
    result = inspect(archive, frame)
    assert result["invalid_identity"] == 1
    assert result["state"] == "PARTIAL"


def test_missing_numeric_columns_fail_without_crashing(archive):
    result = inspect(archive, candles().drop(columns="volume"))
    assert result["state"] == "INVALID_SCHEMA"
    assert result["missing_columns"] == ["volume"]


def test_other_session_invalid_price_does_not_poison_requested_date(archive):
    other = candles(DAY.replace(day=8))
    other["volume"] = -1
    assert inspect(archive, pd.concat([candles(), other], ignore_index=True))["state"] == "COMPLETE"


def test_missing_file_and_missing_session_are_explicit(archive):
    assert qc.inspect_contract(contract(), DAY)["state"] == "MISSING_FILE"
    assert inspect(archive, candles(DAY.replace(day=8)))["state"] == "NO_SESSION_ROWS"


def test_naive_and_utc_bars_use_ist_clock(archive):
    frame = candles()
    frame["timestamp"] = frame.timestamp.dt.tz_localize(None)
    frame["candle_start"] = frame.candle_start.dt.tz_localize(None)
    assert inspect(archive, frame)["state"] == "COMPLETE"
    frame = candles()
    frame["timestamp"] = frame.timestamp.dt.tz_convert("UTC")
    frame["candle_start"] = frame.candle_start.dt.tz_convert("UTC")
    assert inspect(archive, frame)["state"] == "COMPLETE"


def test_dated_universe_never_borrows_latest_membership(archive):
    assert list(qc.load_session_universe(DAY).tradingsymbol) == ["TEST26OCTFUT"]


@pytest.mark.parametrize("change", [dict(master_date=pd.Timestamp(TODAY)), dict(master_date=pd.NaT),
    dict(expiry=pd.Timestamp("2026-09-29")), dict(instrument_token=float("inf")), dict(instrument_token=1.5)])
def test_invalid_dated_universe_is_rejected(archive, change):
    pd.DataFrame([contract(**change)]).to_parquet(qc.common.UNIVERSE_DIR/f"near_month_{DAY}.parquet", index=False)
    with pytest.raises(ValueError, match="Dated near-month"):
        qc.load_session_universe(DAY)


def test_duplicate_dated_membership_cannot_fake_contract_completion(archive):
    pd.DataFrame([contract(), contract()]).to_parquet(qc.common.UNIVERSE_DIR/f"near_month_{DAY}.parquet", index=False)
    with pytest.raises(ValueError):
        qc.load_session_universe(DAY)


def test_80_percent_coverage_never_reports_success(archive):
    inspect(archive, candles().iloc[:65])
    assert qc.run(args(DAY, "--min-contract-coverage", "0.80")) == 1
    state = json.loads((qc.common.QC_DIR/f"fno_oi_eod_qc_{DAY}_status.json").read_text())
    assert state["status"] == "PARTIAL"
    assert state["contracts_at_coverage_threshold"] == 1
    assert state["contracts_complete"] == 0


def test_bad_volume_prevents_success_at_complete_timestamp_coverage(archive):
    frame = candles()
    frame.loc[10, "volume"] = -1
    inspect(archive, frame)
    assert qc.run(args()) == 1


@pytest.mark.parametrize("repair_state", ["FAILED", "INVALID_DATA"])
def test_failed_repair_outcome_cannot_report_success(archive, monkeypatch, repair_state):
    inspect(archive, candles().iloc[:-1])
    def isolated_repair(*args):
        # Simulate another writer finishing the data while this repair reports
        # a failure. No broker path is called by this test.
        candles().to_parquet(archive[0], index=False)
        return [dict(state=repair_state)]
    monkeypatch.setattr(qc, "_repair_universe", isolated_repair)
    options = args()
    options.no_repair = False
    assert qc.run(options) == 1
    result = json.loads((qc.common.QC_DIR/f"fno_oi_eod_qc_{DAY}_status.json").read_text())
    assert result["contracts_complete"] == 1
    assert result["repair_failed"] == 1


def test_historical_audit_keeps_live_status_and_latest_files_intact(archive):
    inspect(archive)
    sentinels = {}
    for name in ("latest_fno_oi_eod_qc.csv", "latest_fno_oi_eod_qc.md"):
        path = qc.common.LATEST_DIR/name
        path.write_text("today's report")
        sentinels[path] = path.read_bytes()
    assert qc.run(args()) == 0
    assert archive[1] == []
    assert all(path.read_bytes() == contents for path, contents in sentinels.items())
    for suffix in (".csv", ".parquet", ".md", "_status.json"):
        assert (qc.common.QC_DIR/f"fno_oi_eod_qc_{DAY}{suffix}").is_file()


def test_current_session_updates_its_own_latest_and_runtime_status(archive):
    candles(TODAY).to_parquet(archive[0], index=False)
    assert qc.run(args(TODAY)) == 0
    assert archive[1][-1][0] == (qc.SESSION, "SUCCESS")
    assert (qc.common.LATEST_DIR/"latest_fno_oi_eod_qc.md").is_file()
    assert qc.build_parser().parse_args([]).min_contract_coverage == 1.0


def test_missing_historical_universe_exits_nonzero_without_live_status_write(archive):
    day = DAY.replace(day=8)
    assert qc.main(["--session-date", str(day), "--no-repair"]) == 1
    assert archive[1] == []
    state = json.loads((qc.common.QC_DIR/f"fno_oi_eod_qc_{day}_status.json").read_text())
    assert state["status"] == "FAILED"


def test_historical_skip_and_interrupt_are_scoped(archive, monkeypatch):
    assert qc.run(args(date(2026,10,10))) == 0
    assert archive[1] == []
    def interrupted(*args):
        raise KeyboardInterrupt
    monkeypatch.setattr(qc, "run", interrupted)
    assert qc.main(["--session-date", str(DAY), "--no-repair"]) == 130
    assert archive[1] == []


@pytest.mark.parametrize("threshold", ["nan", "inf", "0", "-1", "1.1"])
def test_invalid_threshold_cannot_bypass_qc(archive, threshold):
    assert qc.main(["--session-date", str(DAY), "--no-repair", "--min-contract-coverage", threshold]) == 1
    assert archive[1] == []
