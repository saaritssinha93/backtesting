"""G3 production replay uses raw inputs and sealed publication boundaries."""
from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v13_replay_source as source
import fno_v13_v10_g_3_daily_replay as replay
import backtesting_result_v13_v10_g_3_daily as publisher


DAY = date(2030, 1, 7)


def _minute_frame(day, *, move=True):
    stamps = pd.date_range(f"{day} 09:16", f"{day} 15:15", freq="min", tz="Asia/Kolkata")
    close = 100 + np.arange(len(stamps)) * .001
    frame = pd.DataFrame(dict(date=stamps, open=close - .0005, high=close + .01,
                              low=close - .01, close=close, volume=100., gap_filled=0))
    if move:
        # A clean 10:00 five-minute rise, exact 10:01 volume confirmation,
        # then an entry/target path. All other selected windows stay quiet.
        for clock, price in zip(("09:56", "09:57", "09:58", "09:59", "10:00", "10:01"),
                                (101.2, 101.4, 101.6, 101.8, 102., 102.2)):
            mask = frame.date.dt.strftime("%H:%M").eq(clock)
            frame.loc[mask, ["open", "high", "low", "close"]] = [price - .2, price + .05, price - .21, price]
        frame.loc[frame.date.dt.strftime("%H:%M").eq("10:01"), "volume"] = 130.
        after = frame.date.dt.strftime("%H:%M").ge("10:02")
        price = 102.3 + np.arange(int(after.sum())) * .001
        frame.loc[after, "open"] = price - .001
        frame.loc[after, "high"] = price + .01
        frame.loc[after, "low"] = price - .01
        frame.loc[after, "close"] = price
        after_target = frame.date.dt.strftime("%H:%M").ge("10:05")
        frame.loc[after_target, ["open", "high", "low", "close"]] += 3.5
    return frame


@pytest.fixture
def raw(tmp_path):
    roots = replay.DataRoots(tmp_path / "universe", tmp_path / "equity", tmp_path / "futures")
    for root in (roots.universe, roots.equity_1m, roots.futures_5m):
        root.mkdir()
    expiry = date(2030, 1, 29)
    rows = []
    for underlying, token, index in (("EXAMPLE", 201, False), ("NIFTY", 202, True)):
        symbol = f"{underlying}30JANFUT"
        rows.append(dict(tradingsymbol=symbol, underlying=underlying, exchange="NFO",
                         instrument_token=token, expiry=expiry, master_date=DAY,
                         is_index_future=index, contract_month="30JAN", lot_size=1, tick_size=.05,
                         equity_symbol="" if index else "EXAMPLE", equity_instrument_token=0 if index else 101,
                         futures_tradingsymbol=symbol, futures_instrument_token=token))
    universe = roots.universe / f"near_month_{DAY}.parquet"
    pd.DataFrame(rows).to_parquet(universe, index=False)
    minute = pd.concat([_minute_frame(DAY - timedelta(days=3), move=False), _minute_frame(DAY)], ignore_index=True)
    equity_path = source.hybrid.equity_one_minute_path("EXAMPLE", roots.equity_1m)
    equity_path.parent.mkdir(parents=True, exist_ok=True)
    minute.to_parquet(equity_path, index=False)
    future_stamps = pd.DatetimeIndex([])
    parts = [pd.date_range(f"{d} 09:20", f"{d} 15:15", freq="5min", tz="Asia/Kolkata")
             for d in (DAY - timedelta(days=3), DAY)]
    future_stamps = parts[0].append(parts[1])
    future = pd.DataFrame(dict(timestamp=future_stamps, open=100., high=100.1, low=99.8,
                               close=99.9, volume=1000., oi=100000 * np.power(1.001, np.arange(len(future_stamps)))))
    for row in rows:
        future.to_parquet(roots.futures_5m / f"{row['tradingsymbol']}_5minute.parquet", index=False)
    return roots, equity_path, universe


def test_raw_new_session_has_g3_identity_and_verified_artifacts(raw, tmp_path):
    roots, equity, _ = raw
    # The new LONG threshold admits this candidate below legacy G's 1.20.
    frame = pd.read_parquet(equity)
    frame.loc[frame.date.eq(pd.Timestamp(f"{DAY} 10:01", tz="Asia/Kolkata")), "volume"] = 115.
    frame.to_parquet(equity, index=False)
    result = replay.replay_day(DAY, tmp_path / "run", roots=roots)
    assert result["complete"], result["coverage"]
    assert result["state"] == "SUCCESS"
    assert result["role"] == "production_daily"
    assert result["metrics"]["orders"] == result["metrics"]["fills"] == 1
    ledger = pd.read_csv(tmp_path / "run" / result["artifacts"]["portfolio_trades"]["path"])
    assert ledger.native_stop_pct.tolist() == [1.25]
    assert ledger.native_target_pct.tolist() == [3.]
    assert ledger.v9_1m_volume_ratio.tolist() == [1.15]
    assert ledger.strategy_version.eq(replay.contract.STRATEGY_VERSION).all()
    for item in result["artifacts"].values():
        assert replay.sha256(tmp_path / "run" / item["path"]) == item["sha256"]


def test_missing_execution_minute_blocks_publication_and_preserves_current(raw, tmp_path):
    roots, equity, _ = raw
    root = tmp_path / "published"
    run = root / "production" / "runs" / str(DAY) / "first"
    first = replay.replay_day(DAY, run, roots=roots)
    publisher.publish_result(root, run, first)
    pointer = (root / "latest" / "current_run.json").read_bytes()
    frame = pd.read_parquet(equity)
    frame = frame.loc[~frame.date.eq(pd.Timestamp(f"{DAY} 14:10", tz="Asia/Kolkata"))]
    frame.to_parquet(equity, index=False)
    second_run = root / "production" / "runs" / str(DAY) / "second"
    second = replay.replay_day(DAY, second_run, roots=roots)
    publisher.publish_result(root, second_run, second)
    assert second["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert second["metrics"] is None
    assert "portfolio_trades" not in second["artifacts"]
    assert (root / "latest" / "current_run.json").read_bytes() == pointer
    assert json.loads((root / "status.json").read_text())["complete"] is False


def test_complete_zero_order_session_has_headers_and_zero_metrics(raw, tmp_path):
    roots, equity, _ = raw
    frame = pd.read_parquet(equity)
    frame["volume"] = 100.
    frame.to_parquet(equity, index=False)
    result = replay.replay_day(DAY, tmp_path / "zero", roots=roots)
    assert result["complete"]
    assert result["metrics"]["trades"] == result["metrics"]["orders"] == 0
    ledger = pd.read_csv(tmp_path / "zero" / "portfolio_trades.csv")
    assert ledger.empty and {"signal_id", "day", "portfolio_executed"} <= set(ledger.columns)
    assert len(pd.read_csv(tmp_path / "zero" / "daily_results.csv")) == 1


def test_future_data_never_changes_requested_day(raw, tmp_path):
    roots, equity, _ = raw
    baseline = replay.replay_day(DAY, tmp_path / "before", roots=roots)
    frame = pd.read_parquet(equity)
    future = _minute_frame(DAY + timedelta(days=1))
    future[["open", "high", "low", "close", "volume"]] *= 100
    pd.concat([frame, future], ignore_index=True).to_parquet(equity, index=False)
    observed = replay.replay_day(DAY, tmp_path / "after", roots=roots)
    assert observed["metrics"] == baseline["metrics"]


def test_hash_tamper_and_run_overwrite_are_rejected(raw, tmp_path):
    roots, _, _ = raw
    run = tmp_path / "production" / "runs" / str(DAY) / "run"
    result = replay.replay_day(DAY, run, roots=roots)
    with pytest.raises(FileExistsError):
        replay.replay_day(DAY, run, roots=roots)
    (run / "portfolio_trades.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="manifest hash"):
        publisher.publish_result(tmp_path, run, result)
    assert not (tmp_path / "latest" / "current_run.json").exists()


def test_closed_session_gate_is_after_close_only():
    with pytest.raises(ValueError, match="closed session"):
        publisher._require_closed(DAY, pd.Timestamp(f"{DAY} 11:00", tz="Asia/Kolkata").to_pydatetime())
    publisher._require_closed(DAY, pd.Timestamp(f"{DAY} 15:35", tz="Asia/Kolkata").to_pydatetime())


def test_sealed_pool_must_include_below_legacy_threshold_before_ranking(raw, tmp_path):
    roots, equity, _ = raw
    frame = pd.read_parquet(equity)
    frame.loc[frame.date.eq(pd.Timestamp(f"{DAY} 10:01", tz="Asia/Kolkata")), "volume"] = 115.
    frame.to_parquet(equity, index=False)
    dataset = source.build_day_features(DAY, roots=roots)
    signals = dataset["signals"]
    high_volume = signals.copy()
    high_volume["tradingsymbol"] = "SECOND"
    high_volume["sid"] += 100
    high_volume["v9_1m_volume_ratio"] = 1.3
    signals = pd.concat([signals, high_volume], ignore_index=True)
    features = dataset["feature_ledger"]
    features = pd.concat([features, high_volume], ignore_index=True)
    sealed = tmp_path / "sealed"
    sealed.mkdir()
    signals.to_parquet(sealed / "signals.parquet", index=False)
    features.to_parquet(sealed / "all_5m_features.parquet", index=False)
    pd.DataFrame([dict(day=str(DAY), eligible=True)]).to_csv(sealed / "source_session_eligibility.csv", index=False)
    np.savez(sealed / "paths.npz", **{})
    names = ["signals.parquet", "all_5m_features.parquet", "source_session_eligibility.csv", "paths.npz"]
    def reseal():
        replay.atomic_json(sealed / "dataset_manifest.json", dict(days=[str(DAY)],
            output_sha256={name: replay.sha256(sealed / name) for name in names}))
    reseal()
    assert replay.load_sealed_source(sealed)["provenance"]["long_candidates_below_legacy_120"] == 1
    # Even an internally consistent rehashed narrowed pool must be rejected.
    signals.loc[signals.v9_1m_volume_ratio.ge(1.2)].to_parquet(sealed / "signals.parquet", index=False)
    reseal()
    with pytest.raises(ValueError, match="candidate pool mismatch"):
        replay.load_sealed_source(sealed)


def test_new_publisher_lock_cannot_be_stolen(tmp_path):
    with publisher._lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another G3 publisher"):
            with publisher._lock(tmp_path):
                pass
    assert not (tmp_path / ".production_publisher.lock").exists()


def test_history_excludes_blocked_session_and_preserves_zero_session(raw, tmp_path, monkeypatch):
    roots, _, _ = raw
    prior = DAY - timedelta(days=3)
    bundle = dict(days=[prior], eligibility=pd.DataFrame([dict(day=str(prior), eligible=True)]))
    monkeypatch.setattr(publisher, "_require_closed", lambda day: None)
    monkeypatch.setattr(replay, "load_sealed_source", lambda root: bundle)
    def member(directory, days, complete):
        orders = replay.contract.select_orders(source._empty_signals())
        return replay._finish(directory, days=days, role="cumulative_history" if days == [prior] else "production_daily",
            source_kind="TEST_MARKET_SOURCE", frames=dict(selected_orders=orders, candidate_signals=source._empty_signals()),
            provenance={}, coverage=dict(
                session_coverage=[dict(day=str(d), method="TEST_SOURCE") for d in days],
                blocking_problems=[] if complete else [dict(reason="MISSING_REQUIRED_EQUITY_MINUTES")]),
            complete=complete, paths={})
    monkeypatch.setattr(replay, "replay_sealed_days", lambda bundle, days, directory: member(directory, days, True))
    monkeypatch.setattr(publisher, "_history_raw_worker", lambda day, directory, roots: member(directory, [day], False))
    root = tmp_path / "history-output"
    result = publisher.run_history(DAY, root, roots=roots, jobs=1)
    assert result["complete"] and result["state"] == "SUCCESS_WITH_EXCLUDED_SESSIONS"
    assert result["metrics"]["sessions"] == 1
    assert result["metrics"]["trades"] == 0
    assert result["days"] == [str(prior)]
    assert result["coverage"]["excluded_sessions"][0]["day"] == str(DAY)
    assert not result["coverage"]["requested_calendar_coverage_complete"]
    pointer = json.loads((root / "latest" / "current_history.json").read_text())
    assert pointer["role"] == "cumulative_history"
    assert not (root / "latest" / "current_run.json").exists()


def test_daily_incremental_history_adds_and_replaces_one_day(raw, tmp_path):
    roots, equity, _ = raw
    root = tmp_path / "incremental"
    prior = DAY - timedelta(days=3)
    old_dir = root / "history" / "runs" / str(prior) / "old"
    empty = replay.contract.select_orders(source._empty_signals())
    old = replay._finish(old_dir, days=[prior], role="cumulative_history", source_kind="TEST_SOURCE",
        frames=dict(selected_orders=empty, candidate_signals=source._empty_signals()), provenance={},
        coverage=dict(session_coverage=[dict(day=str(prior), method="TEST_SOURCE")],
                      excluded_sessions=[dict(day=str(DAY), state="BLOCKED_INCOMPLETE_DATA")]), complete=True, paths={})
    publisher.publish_result(root, old_dir, old, history=True)
    old_hash = replay.sha256(old_dir / "run_manifest.json")
    daily_dir = root / "production" / "runs" / str(DAY) / "filled"
    daily = replay.replay_day(DAY, daily_dir, roots=roots)
    result = publisher.refresh_history(root, daily_dir, daily)
    assert result["days"] == [str(prior), str(DAY)]
    assert result["metrics"]["trades"] == 1
    assert result["coverage"]["excluded_sessions"] == []
    assert len(result["coverage"]["session_coverage"]) == 2
    assert result["coverage"]["session_coverage"][-1]["included_stocks"] == 1
    assert replay.sha256(old_dir / "run_manifest.json") == old_hash
    frame = pd.read_parquet(equity)
    frame["volume"] = 100.
    frame.to_parquet(equity, index=False)
    zero_dir = root / "production" / "runs" / str(DAY) / "zero"
    zero = replay.replay_day(DAY, zero_dir, roots=roots)
    corrected = publisher.refresh_history(root, zero_dir, zero)
    assert corrected["metrics"]["trades"] == 0
    assert corrected["metrics"]["sessions"] == 2
    assert corrected["days"] == [str(prior), str(DAY)]
    pointer_path = root / "latest" / "current_history.json"
    pointer = pointer_path.read_bytes()
    assert publisher.refresh_history(root, zero_dir, {"complete": False}) is None
    assert pointer_path.read_bytes() == pointer
    current_dir = root / json.loads(pointer)["run_dir"]
    (current_dir / "portfolio_trades.csv").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="artifact integrity"):
        publisher.refresh_history(root, zero_dir, zero)
    assert pointer_path.read_bytes() == pointer


@pytest.mark.parametrize("field,value", [
    ("schema_version", "research_preview_v1"),
    ("execution_authority", True),
    ("state", "COMPLETE_RESEARCH_REPLAY"),
])
def test_hash_valid_but_nonproduction_manifest_is_rejected(raw, tmp_path, field, value):
    roots, _, _ = raw
    root = tmp_path / "strict-package"
    run = root / "production" / "runs" / str(DAY) / "run"
    result = replay.replay_day(DAY, run, roots=roots)
    publisher.publish_result(root, run, result)
    result[field] = value
    replay.atomic_json(run / "run_manifest.json", result)
    pointer_path = root / "latest" / "current_run.json"
    pointer = json.loads(pointer_path.read_text())
    pointer["manifest_sha256"] = replay.sha256(run / "run_manifest.json")
    replay.atomic_json(pointer_path, pointer)
    with pytest.raises(ValueError):
        publisher._read_verified_pointer(root, "current_run.json", "production_daily")


def test_production_spec_drift_fails_before_input_capture(raw, tmp_path, monkeypatch):
    roots, _, _ = raw
    spec = replay.contract.strategy_payload()
    spec["minimum_confirmation_1m_volume_ratio"]["LONG"] = 1.2
    path = tmp_path / "drifted_contract.json"
    replay.atomic_json(path, spec)
    monkeypatch.setattr(replay, "PRODUCTION_CONFIG_PATH", path)
    with pytest.raises(ValueError, match="accepted normalized contract"):
        replay.replay_day(DAY, tmp_path / "never_created", roots=roots)
    assert not (tmp_path / "never_created").exists()

