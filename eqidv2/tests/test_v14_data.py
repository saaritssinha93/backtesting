from pathlib import Path

import pandas as pd

from v14 import data


def test_kite_start_minutes_become_native_end_without_filling():
    raw = pd.DataFrame({
        "date": ["2026-10-09 09:15:00+05:30", "2026-10-09 09:17:00+05:30", "2026-10-09 15:30:00+05:30"],
        "open": [100, 101, 100], "high": [102, 103, 102],
        "low": [99, 100, 99], "close": [101, 102, 101], "volume": [10, 20, 10],
    })
    result = data.normalize_cash_minutes(raw, "ABC", source="kite", start_labeled=True)
    assert list(result.ts.dt.strftime("%H:%M")) == ["09:16", "09:18"]
    assert str(result.ts.dt.tz) == "Asia/Kolkata"
    assert data.coverage(result, ["2026-10-09"])["2026-10-09"]["missing_minutes"] == 373


def test_synthetic_or_invalid_native_rows_are_excluded():
    raw = pd.DataFrame({
        "date": pd.date_range("2026-10-09 09:16", periods=4, freq="min", tz="Asia/Kolkata"),
        "open": [100, 100, 100, 100], "high": [102, 102, 98, 102],
        "low": [99, 99, 99, 99], "close": [101, 101, 101, 101],
        "volume": [0, 10, 10, -1], "gap_filled": [False, True, False, False],
    })
    result = data.normalize_cash_minutes(raw, "ABC", source="native")
    assert len(result) == 1
    assert result.iloc[0].volume == 0
    assert result.iloc[0].ts.hour == 9


def test_dated_complete_nfo_master_and_explicit_snapshot_limit(tmp_path):
    repo, source, output = tmp_path / "repo", tmp_path / "source", tmp_path / "output"
    repo.mkdir()
    (repo / "filtered_stocks_MIS_v2.py").write_text("# historical eligibility unavailable\nselected_stocks = {'AAA','BBB','CCC'}\n")
    master = source / "fno_oi/instrument_master"
    master.mkdir(parents=True)
    pd.DataFrame({"underlying": ["BBB", "OUTSIDE_MIS", "NIFTY"], "is_index_future": [False, False, True]}).to_parquet(master / "instrument_master_2026-08-10.parquet")
    pd.DataFrame({"underlying": ["CCC", "OUTSIDE_MIS"], "is_index_future": [False, False]}).to_parquet(master / "instrument_master_2026-08-11.parquet")
    manifest = data.build_universe_manifest(["2026-08-07", "2026-08-10", "2026-08-11"], root=output, source_root=source, repo=repo)
    assert manifest["sessions"]["2026-08-10"]["symbols"] == ["AAA", "CCC"]
    assert manifest["sessions"]["2026-08-11"]["symbols"] == ["AAA", "BBB"]
    assert manifest["sessions"]["2026-08-07"]["nfo_membership_status"] == "FUTURE_NFO_SNAPSHOT_APPROXIMATION"
    assert "NOT_HISTORICAL" in manifest["mis_membership_status"]
    assert "OUTSIDE_MIS" in manifest["nfo_snapshots"]["2026-08-10"]["symbols"]


def test_fetch_windows_bounded_and_cover_requested_days():
    days = ["2026-06-16", "2026-07-15", "2026-07-16", "2026-10-09"]
    windows = data._fetch_windows(days)
    assert windows == [("2026-06-16", "2026-07-15"), ("2026-07-16", "2026-07-16"), ("2026-10-09", "2026-10-09")]
    assert all((pd.Timestamp(b) - pd.Timestamp(a)).days < 30 for a, b in windows)


def test_read_native_data_preserves_zero_volume_and_original_end_label(tmp_path):
    raw = pd.DataFrame({"date": pd.date_range("2026-10-09 09:16", periods=2, freq="min", tz="Asia/Kolkata"), "open": [100, 100], "high": [101, 101], "low": [99, 99], "close": [100, 100], "volume": [0, 3], "RSI": [40, 50]})
    path = tmp_path / "ABC_stocks_indicators_1min.parquet"
    raw.to_parquet(path)
    result = data._read_cash(path, "ABC")
    assert result.ts.iloc[0] == raw.date.iloc[0]
    assert result.volume.sum() == 3
    assert "RSI" not in result
