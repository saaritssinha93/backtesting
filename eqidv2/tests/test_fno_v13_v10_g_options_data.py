from pathlib import Path

import pandas as pd
import pytest

import fno_v13_v10_g_options_data as data


def _snapshot(day="2026-09-01", expiry="2026-09-29", strikes=(90, 110)):
    frame = pd.DataFrame([
        {"underlying": "TEST", "expiry": pd.Timestamp(expiry), "strike": float(strike),
         "lot_size": 500, "tick_size": 0.05, "instrument_token": 1000 + i,
         "tradingsymbol": f"TEST26SEP{strike}CE", "instrument_type": "CE",
         "_expiry_month": expiry[:7]}
        for i, strike in enumerate(strikes)
    ])
    return data.MasterSnapshot(Path(f"master_{day}.parquet"), pd.Timestamp(day), frame)


def _signal(day="2026-09-02"):
    return {"trade_id": "test", "day": day, "side": "LONG", "equity_symbol": "TEST",
            "entry_ts": f"{day} 09:27:00+05:30", "atm_spot": 100.0}


def _minutes():
    return pd.DataFrame({
        "timestamp": pd.date_range("2026-09-02 09:15", periods=5, freq="min", tz="Asia/Kolkata"),
        "open": [10, 11, 12, 13, 14], "high": [12, 13, 14, 15, 16],
        "low": [9, 10, 11, 12, 13], "close": [11, 12, 13, 14, 15],
        "volume": [0, 0, 0, 0, 0], "oi": [100] * 5,
    })


def test_atm_tie_lower_strike_and_dated_master_preferred():
    result = data.map_signal(_signal(), [_snapshot(), _snapshot("2026-09-04", strikes=(100,))])
    assert result["strike"] == 90
    assert result["mapping_status"] == "MAPPED_CAUSAL"
    assert result["lot_size"] == 500


def test_future_metadata_cannot_replace_missing_august_expiry():
    result = data.map_signal(_signal("2026-08-20"), [_snapshot()])
    assert result["mapping_status"] == "MISSING_MONTHLY_OPTION_METADATA"
    assert result["option_symbol"] == ""


def test_same_month_retrospective_metadata_explicitly_labelled():
    result = data.map_signal(_signal("2026-08-26"), [_snapshot()])
    assert result["expiry"] == pd.Timestamp("2026-09-29")
    assert result["mapping_status"] == "MAPPED_RETROSPECTIVE_METADATA"
    assert result["metadata_retrospective"] is True


def test_unfilled_signal_preserved_without_attempting_nan_atm():
    signal = {**_signal(), "signal_status": "UNDERLYING_UNFILLED", "entry_ts": pd.NaT, "atm_spot": float("nan")}
    assert data.map_signal(signal, [_snapshot()])["mapping_status"] == "UNDERLYING_UNFILLED"


def test_bar_end_converted_and_exact_zero_volume_five_minutes_retained():
    raw = _minutes()
    raw["candle_start"] = raw.timestamp
    raw["timestamp"] = raw.timestamp + pd.Timedelta(minutes=1)
    normalized = data.normalize_local_candles(raw, minutes=1, source="general")
    result = data.aggregate_exact_five_minutes(normalized)
    assert len(result) == 1
    assert result.iloc[0].timestamp == pd.Timestamp("2026-09-02 09:15", tz="Asia/Kolkata")
    assert result.iloc[0].candle_end == pd.Timestamp("2026-09-02 09:20", tz="Asia/Kolkata")
    assert (result.iloc[0][["open", "high", "low", "close", "volume"]].tolist()) == [10, 16, 9, 15, 0]


def test_missing_minute_never_synthesized():
    normalized = data.normalize_local_candles(_minutes().drop(index=2), minutes=1, source="research")
    assert data.aggregate_exact_five_minutes(normalized).empty


def test_general_bar_end_without_explicit_start_fails_closed():
    raw = _minutes()
    raw["data_version"] = "fno_options_raw_1m_v1"
    with pytest.raises(ValueError, match="lacks candle_start"):
        data.normalize_local_candles(raw, minutes=1, source="broken")


def test_loader_native_five_minute_fills_only_exact_missing_interval(tmp_path):
    one = tmp_path / "raw_options_1m"
    five = tmp_path / "raw_options_5m"
    one.mkdir()
    five.mkdir()
    _minutes().drop(index=2).to_parquet(one / "TEST26SEP90CE_1minute.parquet")
    raw5 = pd.DataFrame({"timestamp": [pd.Timestamp("2026-09-02 09:20", tz="Asia/Kolkata")],
                         "candle_start": [pd.Timestamp("2026-09-02 09:15", tz="Asia/Kolkata")],
                         "open": [10], "high": [20], "low": [5], "close": [12], "volume": [500], "oi": [100]})
    raw5.to_parquet(five / "TEST26SEP90CE_5minute.parquet")
    result, sources = data.load_contract_five_minutes("TEST26SEP90CE", [(one, 1), (five, 5)])
    assert len(result) == 1
    assert result.iloc[0].source_interval == "NATIVE_5M"
    assert result.iloc[0].high == 20
    assert len(sources) == 2


def test_duplicate_disagreement_and_invalid_primary_recovery_are_audited(tmp_path):
    primary = tmp_path / "primary"
    secondary = tmp_path / "secondary"
    primary.mkdir()
    secondary.mkdir()
    original = _minutes()
    invalid = original.copy()
    invalid.loc[2, "high"] = -1
    invalid.loc[1, "high"] = 30
    invalid.to_parquet(primary / "TEST26SEP90CE_1minute.parquet")
    original.to_parquet(secondary / "TEST26SEP90CE_1minute.parquet")
    result, sources = data.load_contract_five_minutes("TEST26SEP90CE", [(primary, 1), (secondary, 1)])
    assert len(result) == 1
    assert result.iloc[0].high == 30
    conflicts = [row for row in sources if row["kind"] == "DUPLICATE_SOURCE_CONFLICT"]
    assert conflicts[0]["conflicted_bars"] == 1
    recoveries = [row for row in sources if row["kind"] == "INVALID_SOURCE_BAR_REPLACED"]
    assert recoveries[0]["replacement_same_interval_bars"] == 1


def test_native_five_minute_disagreement_flagged_aggregate_kept(tmp_path):
    one = tmp_path / "one"
    five = tmp_path / "five"
    one.mkdir()
    five.mkdir()
    _minutes().to_parquet(one / "TEST26SEP90CE_1minute.parquet")
    raw5 = pd.DataFrame({"timestamp": [pd.Timestamp("2026-09-02 09:15", tz="Asia/Kolkata")],
                         "open": [10], "high": [30], "low": [9], "close": [15], "volume": [0], "oi": [100]})
    raw5.to_parquet(five / "TEST26SEP90CE_5minute.parquet")
    result, sources = data.load_contract_five_minutes("TEST26SEP90CE", [(one, 1), (five, 5)])
    assert result.iloc[0].high == 16
    assert any(row["kind"] == "AGGREGATED_1M_VS_NATIVE_5M_CONFLICT" for row in sources)
