import logging

import pandas as pd
import pytest

import trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_1min as producer


def rows(stamps):
    return pd.DataFrame({"date": pd.to_datetime(stamps).tz_localize("Asia/Kolkata"),
                         "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 1000})


def test_regular_loader_preserves_quality_provenance_without_loading_derived_indicators(tmp_path):
    frame = rows(["2026-08-12 09:16", "2026-08-12 09:17"])
    frame["gap_filled"] = [True, False]
    frame["is_synthetic"] = [True, False]
    frame["quality_state"] = ["GAP_FILLED", "VALID"]
    frame["source"] = ["FORWARD_FILL", "KITE_HISTORICAL_1M"]
    frame["fetched_at"] = "2026-08-12T10:00:00+05:30"
    frame["repair_note"] = ["not recovered", "observed"]
    frame["original_source_file"] = "recorded.parquet"
    frame["EMA_20"] = 100.0
    path = tmp_path / "ABC.parquet"
    frame.to_parquet(path, index=False)
    loaded = producer._load_existing_ohlc(str(path), "end", "1min")
    assert loaded["is_synthetic"].tolist() == [True, False]
    assert loaded["source"].tolist() == ["FORWARD_FILL", "KITE_HISTORICAL_1M"]
    assert loaded["repair_note"].tolist() == ["not recovered", "observed"]
    assert loaded["original_source_file"].tolist() == ["recorded.parquet"] * 2
    assert "fetched_at" in loaded and "quality_state" in loaded
    assert "EMA_20" not in loaded


def test_regular_generation_preserves_unrecovered_flags_and_marks_only_exact_fetched_rows_genuine(tmp_path, monkeypatch):
    path = tmp_path / "ABC_stocks_indicators_1min.parquet"
    existing = rows(["2026-08-12 09:16", "2026-08-12 09:17", "2026-08-12 09:18"])
    existing["is_synthetic"] = True
    existing["gap_filled"] = True
    existing["quality_state"] = "GAP_FILLED"
    existing["source"] = "FORWARD_FILL"
    existing["repair_note"] = "unrecovered original"
    existing.to_parquet(path, index=False)
    fetched = rows(["2026-08-12 09:18", "2026-08-12 09:19"])
    monkeypatch.setitem(producer.DIRS, "1min", {"out": str(tmp_path), "cache": str(tmp_path / "cache")})
    start = pd.Timestamp("2026-08-12 09:18", tz="Asia/Kolkata").to_pydatetime()
    end = pd.Timestamp("2026-08-12 09:19", tz="Asia/Kolkata").to_pydatetime()
    monkeypatch.setattr(producer, "_incremental_start_from_existing", lambda *a, **k: start)
    monkeypatch.setattr(producer, "get_kite_client_rr", lambda: object())
    monkeypatch.setattr(producer, "fetch_historical_1min_df", lambda *a: fetched)
    monkeypatch.setattr(producer, "_write_history_meta", lambda *a: None)
    result = producer.process_ticker("1min", "ABC", 12345, object(), start, end,
        logging.getLogger("quality-test"), set(), False, "end", str(tmp_path / "reports"), False, 5)
    assert result.status == "updated"
    restored = pd.read_parquet(path)
    assert restored["is_synthetic"].tolist() == [True, True, False, False]
    assert restored["gap_filled"].tolist() == [True, True, False, False]
    assert restored["quality_state"].tolist() == ["GAP_FILLED", "GAP_FILLED", "VALID", "VALID"]
    assert restored["source"].tolist() == ["FORWARD_FILL", "FORWARD_FILL", "KITE_HISTORICAL_1M", "KITE_HISTORICAL_1M"]
    assert restored["repair_note"].iloc[:2].tolist() == ["unrecovered original"] * 2
    assert restored["fetched_at"].iloc[2:].notna().all()


@pytest.mark.parametrize("alter", [
    lambda frame: frame.assign(close=float("inf")),
    lambda frame: frame.assign(volume=True),
    lambda frame: frame.assign(gap_filled=True),
    lambda frame: frame.assign(is_synthetic=True),
    lambda frame: frame.assign(synthetic_repair=True),
    lambda frame: frame.assign(repaired_synthetic=True),
    lambda frame: frame.assign(quality_state="GAP_FILLED"),
    lambda frame: frame.assign(source="FORWARD_FILL"),
    lambda frame: frame.assign(source="CARRY_FORWARD"),
])
def test_invalid_or_flagged_provider_rows_cannot_receive_genuine_metadata(alter):
    start = pd.Timestamp("2026-08-12 09:16", tz="Asia/Kolkata").to_pydatetime()
    frame = alter(rows(["2026-08-12 09:16"]))
    with pytest.raises(ValueError):
        producer._prepare_fetched_ohlc(frame, [], start, start)
