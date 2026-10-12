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


def test_microsecond_input_is_normalized_to_nanosecond_contract():
    raw = pd.DataFrame({"date": pd.date_range("2026-10-09 09:16", periods=1, tz="Asia/Kolkata").as_unit("us"), "open": [100], "high": [101], "low": [99], "close": [100], "volume": [0]})
    result = data.normalize_cash_minutes(raw, "ABC", source="native")
    assert result.ts.dtype.unit == "ns"
    assert result.ts.iloc[0].value == 1791517560000000000


def test_local_prepare_preserves_older_history_and_resumes_without_source_writes(tmp_path, monkeypatch):
    import json
    repo, source, output = tmp_path / "repo", tmp_path / "source", tmp_path / "output"
    repo.mkdir()
    (repo / "filtered_stocks_MIS_v2.py").write_text("selected_stocks = {'AAA', 'BBB'}\n")
    master = source / "fno_oi/instrument_master"
    master.mkdir(parents=True)
    pd.DataFrame({"underlying": ["BBB"], "is_index_future": [False]}).to_parquet(master / "instrument_master_2020-06-01.parquet")
    source_data = source / "stocks_indicators_1min_eq"
    source_data.mkdir()
    raw = pd.DataFrame({"date": pd.to_datetime(["2019-05-01 09:16+05:30", "2020-06-01 09:16+05:30"]), "open": [100, 100], "high": [101, 101], "low": [99, 99], "close": [100, 100], "volume": [0, 3]})
    native = source_data / "AAA_stocks_indicators_1min.parquet"
    raw.to_parquet(native)
    original_hash = data._sha(native)
    monkeypatch.setattr(data, "research_sessions", lambda *args, **kwargs: ([], ["2020-06-01"]))
    monkeypatch.setattr(data, "DEFAULT_FROZEN", tmp_path / "absent.csv")
    manifest_path = data.prepare_data("2020-06-01", "2020-06-01", root=output, source_root=source, repo=repo)
    manifest = json.loads(manifest_path.read_text())
    staged = output / "data/1m/AAA.parquet"
    before_mtime = staged.stat().st_mtime_ns
    assert manifest["total_rows"] == 2
    assert manifest["files"][0]["study_missing_minutes"] == 374
    assert data._sha(native) == original_hash
    assert data.load_symbol_1m("AAA", root=output).ts.iloc[0].year == 2019
    data.prepare_data("2020-06-01", "2020-06-01", root=output, source_root=source, repo=repo)
    assert staged.stat().st_mtime_ns == before_mtime
    assert not (output / "data/fetch.lock").exists()


def test_process_limiter_spaces_concurrent_starts_without_burst():
    from concurrent.futures import ThreadPoolExecutor
    class Clock:
        def __init__(self): self.now = 0.0
        def __call__(self): return self.now
        def sleep(self, delay): self.now += delay
    clock = Clock()
    limiter = data.RequestRateLimiter(0.001, clock=clock, sleep=clock.sleep)
    with ThreadPoolExecutor(max_workers=3) as executor:
        starts = sorted(executor.map(lambda _: limiter.wait(), range(12)))
    assert limiter.minimum_interval == 0.46
    assert len(starts) == 12
    assert all(b - a >= 0.46 - 1e-12 for a, b in zip(starts, starts[1:]))
    assert starts[-1] >= 11 * 0.46 - 1e-12


def test_broker_clients_are_thread_local_and_share_process_limiter(tmp_path, monkeypatch):
    import kiteconnect
    import threading
    from concurrent.futures import ThreadPoolExecutor
    class Clock:
        def __init__(self): self.now = 0.0
        def __call__(self): return self.now
        def sleep(self, delay): self.now += delay
    clock = Clock()
    limiter = data.RequestRateLimiter(clock=clock, sleep=clock.sleep)
    monkeypatch.setattr(data, "_BROKER_RATE_LIMITER", limiter)
    created = []
    class FakeKite:
        def __init__(self, **kwargs):
            self.owner = threading.get_ident()
            created.append(self)
        def set_access_token(self, token): pass
        def profile(self): return {}
        def historical_data(self, *args, **kwargs):
            assert self.owner == threading.get_ident()
            return [{"thread": self.owner}]
    monkeypatch.setattr(kiteconnect, "KiteConnect", FakeKite)
    (tmp_path / "api_key.txt").write_text("test-key")
    (tmp_path / "access_token.txt").write_text("test-token")
    client = data.MarketDataClient(tmp_path)
    second = data.MarketDataClient(tmp_path)
    assert client._limiter is second._limiter is limiter
    barrier = threading.Barrier(3)
    def fetch(_):
        barrier.wait(timeout=5)
        return client.history(123, "2020-06-01", "2020-06-01")[0]["thread"]
    with ThreadPoolExecutor(max_workers=3) as executor:
        owners = list(executor.map(fetch, range(3)))
    assert len(set(owners)) == 3
    assert len(created) == 5  # two main-thread clients and three worker clients
    assert clock.now >= 4 * .46 - 1e-12  # two profiles plus three requests


def test_concurrent_symbol_staging_matches_sequential_and_resumes_partial_days(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    import json
    source = tmp_path / "source"
    (source / "stocks_indicators_1min_eq").mkdir(parents=True)
    repo = tmp_path / "repo"
    repo.mkdir()
    class FakeHistory:
        def __init__(self): self.calls = []; self.lock = threading.Lock()
        def history(self, token, first, last):
            with self.lock: self.calls.append((token, first, last))
            # Deliberate real partial day: no synthetic missing minutes.
            return [{"date": "2020-06-01 09:15:00+05:30", "open": 100, "high": 102, "low": 99, "close": 101, "volume": token}]
    def run(root, workers):
        history = FakeHistory()
        state = data._FetchState(history)
        kwargs = dict(root=root, source_root=source, repo=repo, sessions=["2020-06-01"], warmup=[], study=["2020-06-01"], start_ts=pd.Timestamp("2020-06-01", tz="Asia/Kolkata"), end_ts=pd.Timestamp("2020-06-02", tz="Asia/Kolkata"), tokens={"AAA": 1, "BBB": 2, "CCC": 3}, fetch_state=state, fetch_missing=True, retry_partial=False)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            rows = list(executor.map(lambda symbol: data._prepare_symbol(symbol, **kwargs), ["AAA", "BBB", "CCC"]))
        assert len(history.calls) == 3
        assert all(row[0]["study_missing_minutes"] == 374 for row in rows)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(lambda symbol: data._prepare_symbol(symbol, **kwargs), ["AAA", "BBB", "CCC"]))
        assert len(history.calls) == 3  # partial broker days are cached on resume
        for symbol in ("AAA", "BBB", "CCC"):
            audit = json.loads((root / "data/symbol_audits" / f"{symbol}.json").read_text())
            assert audit["fetched_sessions"] == ["2020-06-01"]
        return root
    sequential = run(tmp_path / "sequential", 1)
    concurrent = run(tmp_path / "concurrent", 3)
    for symbol in ("AAA", "BBB", "CCC"):
        pd.testing.assert_frame_equal(data.load_symbol_1m(symbol, root=sequential), data.load_symbol_1m(symbol, root=concurrent))
