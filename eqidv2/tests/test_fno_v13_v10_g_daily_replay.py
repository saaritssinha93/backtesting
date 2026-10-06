"""One-day G reconstruction from raw data, including dates outside research."""
from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_daily_replay as replay


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
    equity_path = replay.hybrid.equity_one_minute_path("EXAMPLE", roots.equity_1m)
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


def _reasons(result):
    return {row["reason"] for row in result["coverage"]["problems"]}


def _assert_same_authoritative_outcome(baseline, observed):
    assert observed["state"] == baseline["state"] == "SUCCESS"
    assert observed["complete"] is baseline["complete"] is True
    assert observed["metrics"] == baseline["metrics"]
    assert observed["coverage"] == baseline["coverage"]
    assert observed["partial_diagnostics"] == baseline["partial_diagnostics"]
    left = pd.read_csv(baseline["artifacts"]["portfolio_trades"])
    right = pd.read_csv(observed["artifacts"]["portfolio_trades"])
    authoritative = [
        column for column in left.columns
        if column in right and not column.endswith("_sha256")
        and column not in {"run_id", "replay_id"}
    ]
    pd.testing.assert_frame_equal(
        left[authoritative], right[authoritative], check_dtype=False
    )


def _assert_telemetry_error(result, *, component, phase):
    evidence = result["observability"]
    assert evidence["state"] == "DEGRADED"
    assert evidence["error_count"] == len(evidence["errors"])
    assert any(
        row["status"] == "TELEMETRY_ERROR"
        and row["component"] == component
        and row["phase"] == phase
        and row["error_type"] == "RuntimeError"
        for row in evidence["errors"]
    )


def test_new_unfrozen_date_replays_only_g_with_complete_dated_artifacts(raw, tmp_path):
    roots, _, _ = raw
    result = replay.replay_day(DAY, tmp_path / "run", roots=roots)
    assert result["state"] == "SUCCESS", result
    assert result["strategy"] == "V13-V10-G"
    assert result["strategy_version"] == replay.config.STRATEGY_VERSION
    assert result["days"] == [DAY.isoformat()]
    assert result["complete"] is True
    assert result["metrics"]["sessions"] == 1
    assert result["metrics"]["orders"] == result["metrics"]["fills"] == 1
    assert result["metrics"]["wins"] == 1
    ledger = pd.read_csv(result["artifacts"]["portfolio_trades"])
    assert ledger.tradingsymbol.tolist() == ["EXAMPLE"]
    assert ledger.setup_id.tolist() == ["1001_LONG"]
    exits = replay.config.load_frozen_config()["exit"]["setups"]["1001_LONG"]
    assert ledger.native_stop_pct.tolist() == [replay.policy.INITIAL_STOP_PCT]
    assert ledger.native_target_pct.tolist() == [exits["target_pct"]]
    for artifact in result["artifacts"].values():
        assert Path(artifact).is_relative_to(tmp_path / "run")
        if artifact.endswith(".csv"):
            frame = pd.read_csv(artifact)
            if "day" in frame:
                assert frame.day.eq(DAY.isoformat()).all()
    manifest = json.loads(Path(result["artifacts"]["source_manifest"]).read_text())
    assert manifest["frozen_config_sha256"] == replay.config.CONFIG_SHA256
    assert all(len(row["sha256"]) == 64 for row in manifest["sources"])
    roles = {row["role"] for row in manifest["sources"]}
    assert {"DATED_UNIVERSE", "NIFTY_FUTURES_CONTEXT", "EQUITY_ONE_MINUTE", "STOCK_FUTURES_OI"} <= roles
    snapshot_manifest = json.loads(
        Path(result["artifacts"]["input_snapshot_manifest"]).read_text()
    )
    assert snapshot_manifest["complete"] is True
    assert snapshot_manifest["snapshot_fingerprint"] == manifest["input_snapshot"]["snapshot_fingerprint"]
    assert all(row["captured"] for row in snapshot_manifest["sources"])


def test_replay_isolated_from_live_source_change_after_snapshot(
        raw, tmp_path, monkeypatch):
    roots, equity, _ = raw
    baseline = replay.replay_day(DAY, tmp_path / "snapshot_baseline", roots=roots)
    original_build = replay.build_day_dataset
    changed = False

    def change_live_source_then_build(day, *, roots=None):
        nonlocal changed
        if not changed:
            frame = pd.read_parquet(equity)
            target = frame.date.eq(pd.Timestamp(f"{DAY} 10:00", tz="Asia/Kolkata"))
            frame.loc[target, ["open", "high", "low", "close"]] += 50
            frame.to_parquet(equity, index=False)
            changed = True
        return original_build(day, roots=roots)

    monkeypatch.setattr(replay, "build_day_dataset", change_live_source_then_build)
    observed = replay.replay_day(DAY, tmp_path / "snapshot_observed", roots=roots)

    assert changed is True
    _assert_same_authoritative_outcome(baseline, observed)
    assert observed["source_fingerprint"] == baseline["source_fingerprint"]
    assert "SOURCE_CHANGED_DURING_REPLAY" not in _reasons(observed)
    source_manifest = json.loads(Path(observed["artifacts"]["source_manifest"]).read_text())
    equity_source = next(
        row for row in source_manifest["sources"] if row["role"] == "EQUITY_ONE_MINUTE"
    )
    assert Path(equity_source["path"]) != equity.resolve()
    assert Path(equity_source["original_path"]) == equity.resolve()
    assert replay._sha(Path(equity_source["path"])) == equity_source["sha256"]
    assert replay._sha(equity) != equity_source["sha256"]


def test_unstable_source_during_snapshot_blocks_without_live_fallback(
        raw, tmp_path, monkeypatch):
    roots, equity, _ = raw
    copyfile = replay.shutil.copyfile
    mutations = 0

    def copy_then_change_source(source, destination, *args, **kwargs):
        nonlocal mutations
        result = copyfile(source, destination, *args, **kwargs)
        if Path(source).resolve() == equity.resolve():
            frame = pd.read_parquet(equity)
            frame.loc[frame.index[0], "volume"] += 1
            frame.to_parquet(equity, index=False)
            mutations += 1
        return result

    monkeypatch.setattr(replay.shutil, "copyfile", copy_then_change_source)
    result = replay.replay_day(DAY, tmp_path / "unstable_snapshot", roots=roots)

    assert mutations == replay.SNAPSHOT_COPY_ATTEMPTS
    assert result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert result["complete"] is False
    assert result["metrics"] is None
    assert "SOURCE_SNAPSHOT_UNSTABLE" in _reasons(result)
    snapshot_manifest = json.loads(
        Path(result["artifacts"]["input_snapshot_manifest"]).read_text()
    )
    assert snapshot_manifest["complete"] is False
    equity_capture = next(
        row for row in snapshot_manifest["sources"] if row["role"] == "EQUITY_ONE_MINUTE"
    )
    assert equity_capture["captured"] is False
    source_manifest = json.loads(Path(result["artifacts"]["source_manifest"]).read_text())
    replay_equity = next(
        row for row in source_manifest["sources"] if row["role"] == "EQUITY_ONE_MINUTE"
    )
    assert replay_equity["exists"] is False
    assert Path(replay_equity["path"]) != equity.resolve()


def test_content_addressed_snapshot_is_reused_and_tampering_is_rejected(raw, tmp_path):
    roots, _, _ = raw
    store = tmp_path / "shared_snapshots"
    first = replay.create_input_snapshot(DAY, store, roots=roots)
    second = replay.create_input_snapshot(DAY, store, roots=roots)

    assert second.fingerprint == first.fingerprint
    assert second.root == first.root
    assert second.manifest_path == first.manifest_path
    assert [path for path in store.iterdir() if not path.name.startswith(".staging-")] == [
        first.root
    ]

    member = next(
        row for row in first.sources if row["role"] == "EQUITY_ONE_MINUTE"
    )
    captured_path = first.root / member["snapshot_relative_path"]
    captured_path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="failed verification"):
        replay.create_input_snapshot(DAY, store, roots=roots)


@pytest.mark.parametrize(("attribute", "component", "phase"), [
    ("evaluate_ohlcv", "data_quality", "evaluate_source"),
    ("canonical_frame_sha256", "input_slice_hash", "hash_effective_history"),
    ("build_v13_v10_g_feature_ledger", "feature_ledger", "build_symbol_ledger"),
    ("canonical_row_sha256", "feature_ledger", "finalize_selection_ledger"),
])
def test_observability_computation_failures_do_not_change_replay_truth(
        raw, tmp_path, monkeypatch, attribute, component, phase):
    roots, _, _ = raw
    baseline = replay.replay_day(DAY, tmp_path / f"baseline_{attribute}", roots=roots)

    def fail_observer(*args, **kwargs):
        del args, kwargs
        raise RuntimeError(f"injected {attribute} failure")

    monkeypatch.setattr(replay, attribute, fail_observer)
    observed = replay.replay_day(DAY, tmp_path / f"fault_{attribute}", roots=roots)

    _assert_same_authoritative_outcome(baseline, observed)
    _assert_telemetry_error(observed, component=component, phase=phase)
    assert "STOCK_SOURCE_BUILD_FAILED" not in _reasons(observed)
    manifest = json.loads(Path(observed["artifacts"]["source_manifest"]).read_text())
    assert manifest["observability"] == observed["observability"]


def test_data_quality_artifact_failure_is_fail_open(raw, tmp_path, monkeypatch):
    roots, _, _ = raw
    baseline = replay.replay_day(DAY, tmp_path / "baseline_quality_write", roots=roots)
    atomic_write_csv = replay.common.atomic_write_csv

    def fail_quality_write(frame, path, *args, **kwargs):
        if Path(path).name == "data_quality.csv":
            raise RuntimeError("injected data-quality persistence failure")
        return atomic_write_csv(frame, path, *args, **kwargs)

    monkeypatch.setattr(replay.common, "atomic_write_csv", fail_quality_write)
    observed = replay.replay_day(DAY, tmp_path / "fault_quality_write", roots=roots)

    _assert_same_authoritative_outcome(baseline, observed)
    _assert_telemetry_error(
        observed, component="data_quality", phase="persist_artifact"
    )
    assert "data_quality" not in observed["artifacts"]


def test_feature_ledger_artifact_failure_is_fail_open(raw, tmp_path, monkeypatch):
    roots, _, _ = raw
    baseline = replay.replay_day(DAY, tmp_path / "baseline_ledger_write", roots=roots)

    def fail_feature_write(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("injected feature-ledger persistence failure")

    monkeypatch.setattr(replay, "write_feature_ledger", fail_feature_write)
    observed = replay.replay_day(DAY, tmp_path / "fault_ledger_write", roots=roots)

    _assert_same_authoritative_outcome(baseline, observed)
    _assert_telemetry_error(
        observed, component="feature_ledger", phase="persist_artifact"
    )
    assert "feature_ledger" not in observed["artifacts"]
    assert "feature_ledger_manifest" not in observed["artifacts"]


def test_missing_date_never_uses_previous_or_frozen_session(raw, tmp_path):
    roots, _, _ = raw
    result = replay.replay_day(DAY + timedelta(days=1), tmp_path / "absent", roots=roots)
    assert result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert result["metrics"] is None
    assert not result["complete"]
    assert "portfolio_trades" not in result["artifacts"]
    assert "REQUIRED_SOURCE_UNAVAILABLE" in _reasons(result)
    for name in ("candidate_signals", "selection_audit", "selected_orders", "coverage"):
        assert pd.read_csv(result["artifacts"][name]).empty


@pytest.mark.parametrize("clock", ["09:24", "10:15", "14:58"])
def test_missing_requested_minute_blocks_even_outside_selection_window(raw, tmp_path, clock):
    roots, equity, _ = raw
    frame = pd.read_parquet(equity)
    mask = frame.date.dt.date.eq(DAY) & frame.date.dt.strftime("%H:%M").eq(clock)
    frame.loc[~mask].to_parquet(equity, index=False)
    result = replay.replay_day(DAY, tmp_path / "missing", roots=roots)
    assert result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert "MISSING_REQUIRED_EQUITY_MINUTES" in _reasons(result)
    assert result["metrics"] is None
    assert "portfolio_trades" not in result["artifacts"]


def test_duplicate_requested_minute_blocks(raw, tmp_path):
    roots, equity, _ = raw
    frame = pd.read_parquet(equity)
    pd.concat([frame, frame.tail(1)]).to_parquet(equity, index=False)
    result = replay.replay_day(DAY, tmp_path / "duplicate", roots=roots)
    assert "DUPLICATE_REQUESTED_DAY_EQUITY_MINUTES" in _reasons(result)
    assert result["metrics"] is None


def test_missing_futures_prior_bar_excludes_stock_without_blocking_session(raw, tmp_path):
    roots, equity, universe = raw
    mapped = pd.read_parquet(universe)
    extra = mapped.loc[mapped.equity_symbol.eq("EXAMPLE")].copy()
    extra["tradingsymbol"] = extra["futures_tradingsymbol"] = "EXAMPLE230JANFUT"
    extra["underlying"] = extra["equity_symbol"] = "EXAMPLE2"
    extra["instrument_token"] = extra["futures_instrument_token"] = 203
    extra["equity_instrument_token"] = 102
    pd.concat([mapped, extra], ignore_index=True).to_parquet(universe, index=False)
    pd.read_parquet(equity).to_parquet(
        replay.hybrid.equity_one_minute_path("EXAMPLE2", roots.equity_1m), index=False)
    pd.read_parquet(roots.futures_5m / "EXAMPLE30JANFUT_5minute.parquet").to_parquet(
        roots.futures_5m / "EXAMPLE230JANFUT_5minute.parquet", index=False)
    path = roots.futures_5m / "EXAMPLE30JANFUT_5minute.parquet"
    future = pd.read_parquet(path)
    mask = future.timestamp.eq(pd.Timestamp(f"{DAY} 09:55", tz="Asia/Kolkata"))
    future.loc[~mask].to_parquet(path, index=False)
    result = replay.replay_day(DAY, tmp_path / "oi_missing", roots=roots)
    assert result["state"] == "SUCCESS"
    assert result["complete"] is True
    assert result["metrics"]["orders"] == result["metrics"]["fills"] == 1
    assert not result["coverage"]["problems"]
    assert {row["reason"] for row in result["coverage"]["ignored_problems"]} == {
        "MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR"
    }
    assert result["coverage"]["excluded_stocks"] == [{
        "symbol": "EXAMPLE", "reasons": ["MISSING_SIGNAL_OR_PRIOR_FUTURES_OI_BAR"]
    }]


def test_wrong_master_date_fails_closed(raw, tmp_path):
    roots, _, universe = raw
    frame = pd.read_parquet(universe)
    frame["master_date"] = DAY - timedelta(days=1)
    frame.to_parquet(universe, index=False)
    result = replay.replay_day(DAY, tmp_path / "wrong", roots=roots)
    assert "REQUIRED_SOURCE_UNAVAILABLE" in _reasons(result)
    assert result["metrics"] is None


@pytest.mark.parametrize("fault,reason", [
    ("missing", "INSUFFICIENT_CONFIRMATION_VOLUME_WARMUP"),
    ("interrupted", "INTERRUPTED_CONFIRMATION_VOLUME_WARMUP"),
    ("non_regular", "NON_REGULAR_EQUITY_HISTORY_MINUTES"),
])
def test_warmup_completeness_is_explicit_guard(raw, tmp_path, fault, reason):
    roots, equity, _ = raw
    frame = pd.read_parquet(equity)
    if fault == "missing":
        frame = frame.loc[frame.date.dt.date.eq(DAY)]
    elif fault == "interrupted":
        mask = frame.date.eq(pd.Timestamp(f"{DAY - timedelta(days=3)} 15:10", tz="Asia/Kolkata"))
        frame = frame.loc[~mask]
    else:
        extra = frame.iloc[[0]].copy()
        extra["date"] = pd.Timestamp(f"{DAY - timedelta(days=3)} 15:31", tz="Asia/Kolkata")
        frame = pd.concat([frame, extra])
    frame.to_parquet(equity, index=False)
    result = replay.replay_day(DAY, tmp_path / fault, roots=roots)
    assert reason in _reasons(result)
    assert result["metrics"] is None


def test_future_bars_cannot_change_requested_day_outcomes(raw):
    roots, equity, _ = raw
    before = replay.build_day_dataset(DAY, roots=roots)
    before_ledger, before_metrics = replay.simulate_day(before)
    frame = pd.read_parquet(equity)
    later = _minute_frame(DAY + timedelta(days=1))
    later[["open", "high", "low", "close", "volume"]] *= 1000
    pd.concat([frame, later]).to_parquet(equity, index=False)
    after = replay.build_day_dataset(DAY, roots=roots)
    after_ledger, after_metrics = replay.simulate_day(after)
    assert before_metrics == after_metrics
    pd.testing.assert_frame_equal(before_ledger, after_ledger)


def test_minimal_features_match_native_general_builder_exactly(raw):
    roots, equity, _ = raw
    minute = replay._load_minute(equity, DAY, [], "EXAMPLE")
    future = replay._load_future(roots.futures_5m / "EXAMPLE30JANFUT_5minute.parquet", DAY, [], "EXAMPLE")
    small = replay._observed_pool(minute, future, day=DAY, symbol="EXAMPLE", future_symbol="EXAMPLE30JANFUT", month="30JAN")
    native = replay.features.observed_pool(minute, future, days={DAY}, equity_symbol="EXAMPLE",
                                          futures_symbol="EXAMPLE30JANFUT", contract_month="30JAN")
    native = native.loc[native.signal_ts.isin(small.signal_ts)].reset_index(drop=True)
    small = small.reset_index(drop=True)
    fields = ["signal_ts", "confirmation_ts", "price_change_pct", "volume_ratio", "traded_value", "oi", "prev_oi",
              "oi_change_pct", "body_ratio", "v9_1m_volume_ratio", "v9_1m_upper_wick_ratio", "v9_1m_lower_wick_ratio",
              "v9_5m_ema9", "v9_5m_ema20", "v9_5m_ema50", "v9_5m_ema_bull", "v9_5m_ema_bear",
              "confirmation_open", "confirmation_high", "confirmation_low", "confirmation_close"]
    pd.testing.assert_frame_equal(small[fields], native[fields], check_exact=True)


def test_complete_zero_trade_session_is_valid_not_missing_data(raw, tmp_path):
    roots, equity, _ = raw
    frame = pd.concat([_minute_frame(DAY - timedelta(days=3), move=False), _minute_frame(DAY, move=False)])
    frame.to_parquet(equity, index=False)
    result = replay.replay_day(DAY, tmp_path / "quiet", roots=roots)
    assert result["state"] == "SUCCESS", result
    assert result["metrics"]["sessions"] == 1
    assert result["metrics"]["orders"] == result["metrics"]["fills"] == 0
    assert pd.read_csv(result["artifacts"]["portfolio_trades"]).empty
    from ai_platform.observability.shadow_automation import _replay_rows
    rows, evidence = _replay_rows(tmp_path / "quiet", result, DAY)
    assert rows == {} and evidence["bytes"] > 0


def test_requires_explicit_date():
    with pytest.raises(TypeError, match="explicit"):
        replay.build_day_dataset(None)
