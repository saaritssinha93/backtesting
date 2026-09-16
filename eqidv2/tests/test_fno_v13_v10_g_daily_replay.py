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
    assert ledger.native_stop_pct.tolist() == [exits["stop_pct"]]
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


def test_requires_explicit_date():
    with pytest.raises(TypeError, match="explicit"):
        replay.build_day_dataset(None)
