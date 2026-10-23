"""Boundary and causality tests for research-only G6 direction refinements."""
import numpy as np
import pandas as pd
import pytest

from research import g6_direction_refinements as r


def stock_case(side="LONG"):
    frame = pd.DataFrame({
        "ts": pd.date_range("2026-10-15 09:16", periods=5, freq="min", tz=r.IST),
        "open": [100., 100., 100., 100., 98.],
        "high": [101., 101., 101., 101., 99.],
        "low": [99., 99., 99., 99., 97.],
        "close": [100., 100., 100., 100., 98.],
        "volume": [10., 10., 10., 10., 20.]})
    row = dict(confirmation_ts=frame.ts.iloc[-1], tradingsymbol="TEST", side=side,
               **{"confirmation_" + col: frame[col].iloc[-1]
                  for col in ("open", "high", "low", "close")})
    return frame, row


def index_case(count=8, side="LONG"):
    opens = np.arange(110., 110. - count, -1.)
    frame = pd.DataFrame({
        "ts": pd.date_range("2026-10-15 09:20", periods=count, freq="5min", tz=r.IST),
        "open": opens, "high": opens + .5, "low": opens - 1.5, "close": opens - 1.})
    return frame, dict(confirmation_ts=frame.ts.iloc[-1], side=side)


def test_stock_vwap_and_prior_structure_excludes_confirmation():
    frame, row = stock_case()
    feature, used = r.stock_features(frame, row)
    assert feature["stock_adverse_vwap"]
    assert feature["stock_adverse_break3"]
    assert feature["stock_confirmed"]
    assert feature["stock_prior3_low"] == 99
    assert feature["stock_vwap"] == pytest.approx((40 * 100 + 20 * 98) / 60)
    assert len(used) == 5


def test_future_stock_changes_cannot_change_features():
    frame, row = stock_case()
    expected, _ = r.stock_features(frame, row)
    future = frame.tail(1).copy()
    future.ts += pd.Timedelta(minutes=1)
    future.loc[:, ["open", "high", "low", "close", "volume"]] = 100000
    actual, used = r.stock_features(pd.concat([frame, future]), row)
    assert actual == expected
    assert len(used) == 5


def test_equivalent_timezone_providers_have_identical_minute_coverage():
    from zoneinfo import ZoneInfo
    frame, row = stock_case()
    expected, _ = r.stock_features(frame, row)
    frame["ts"] = frame.ts.dt.tz_convert(ZoneInfo(r.IST))
    actual, _ = r.stock_features(frame, row)
    assert actual == expected


@pytest.mark.parametrize("issue", ["missing", "duplicate", "flagged", "zero_volume", "changed_ohlc"])
def test_stock_evidence_rejects_invalid_inputs(issue):
    frame, row = stock_case()
    if issue == "missing":
        frame = frame.drop(1)
    elif issue == "duplicate":
        frame = pd.concat([frame, frame.iloc[[1]]])
    elif issue == "flagged":
        frame["gap_filled"] = False
        frame.loc[1, "gap_filled"] = True
    elif issue == "zero_volume":
        frame["volume"] = 0
    else:
        row["confirmation_close"] = 97.9
    with pytest.raises(ValueError):
        r.stock_features(frame, row)


def test_short_confirmation_is_symmetric():
    frame, row = stock_case("SHORT")
    frame["open"] = 200 - frame.open
    frame["close"] = 200 - frame.close
    old_high, old_low = frame.high.copy(), frame.low.copy()
    frame["high"], frame["low"] = 200 - old_low, 200 - old_high
    row.update({"confirmation_" + col: frame[col].iloc[-1]
                for col in ("open", "high", "low", "close")})
    actual, _ = r.stock_features(frame, row)
    assert actual["stock_confirmed"]


def test_index_persistence_uses_completed_candles_and_previous_atr():
    frame, row = index_case()
    actual = r.index_features(frame, row)
    assert actual["primary_veto"] and actual["persistent3_veto"]
    assert actual["atr_ready"]
    assert actual["index_atr_pct"] == pytest.approx(2 / frame.close.iloc[-2] * 100)
    frame.loc[frame.index[-1], "high"] += 1000
    assert r.index_features(frame, row)["index_atr_pct"] == actual["index_atr_pct"]


def test_persistence_mixed_body_does_not_veto():
    frame, row = index_case()
    frame.loc[frame.index[-2], "open"] = frame.close.iloc[-2] - .1
    actual = r.index_features(frame, row)
    assert actual["primary_veto"]
    assert not actual["persistent2_veto"]
    assert not actual["persistent3_veto"]


def test_opening_warmup_preserves_current_primary_veto():
    frame, row = index_case(1)
    actual = r.index_features(frame, row)
    assert not actual["atr_ready"] and not actual["persistence2_ready"]
    assert actual["primary_veto"] and actual["persistent2_veto"]
    assert all(actual["volatility" + suffix + "_veto"] for suffix in ("025", "050", "100"))


def test_future_index_bar_and_incomplete_bar_excluded():
    frame, row = index_case()
    expected = r.index_features(frame, row)
    row["confirmation_ts"] += pd.Timedelta(minutes=2)
    future = frame.tail(1).copy()
    future.ts += pd.Timedelta(minutes=5)
    future.loc[:, ["open", "high", "low", "close"]] = 100000
    assert r.index_features(pd.concat([frame, future]), row) == expected


@pytest.mark.parametrize("issue", ["missing", "duplicate", "bad_side"])
def test_index_rejects_bad_history(issue):
    frame, row = index_case()
    if issue == "missing":
        frame = frame.drop(1)
    elif issue == "duplicate":
        frame = pd.concat([frame, frame.iloc[[1]]])
    else:
        row["side"] = "OTHER"
    with pytest.raises(ValueError):
        r.index_features(frame, row)


def test_policy_boolean_logic_and_threshold_boundary():
    features = pd.DataFrame(dict(
        side=["LONG", "SHORT", "LONG", "LONG"],
        primary_veto=[True, True, True, False], nifty_veto=[False, True, True, True],
        nifty50_cash_veto=[False, False, True, True],
        stock_adverse_vwap=[True, True, False, True],
        stock_adverse_break3=[False, True, True, True],
        primary_slot_pct=[-.02, .03, -.06, .01],
        primary_session_pct=[-.03, .03, -.06, .02],
        persistent2_veto=[True, True, False, False],
        persistent3_veto=[False, True, False, False],
        volatility025_veto=[True, True, True, False],
        volatility050_veto=[True, True, False, False],
        volatility100_veto=[False, False, False, False]))
    actual = r.decisions(features)
    assert len(actual.columns) == 20
    assert not actual.primary_fixed_002.iloc[0]  # equality does not veto
    assert actual.primary_fixed_002.iloc[1]
    assert actual.both_indices_veto.tolist() == [False, True, True, False]
    assert actual.either_index_veto.tolist() == [True, True, True, True]
    assert actual.stock_confirmed.tolist() == [False, True, False, False]
    assert actual.all_three.tolist() == [False, True, False, False]
    features["future_profit"] = [-100, 100, -100, 100]
    pd.testing.assert_frame_equal(actual, r.decisions(features))


def test_confirmation_rejects_naive_or_partial_minute():
    for stamp in ("2026-10-15 09:20", "2026-10-15 09:20:30+05:30"):
        with pytest.raises(ValueError):
            r.checked_stamp(stamp)


@pytest.mark.parametrize("tamper_snapshot", [False, True])
def test_changed_rolling_file_requires_hash_verified_preserved_history(tmp_path, monkeypatch, tamper_snapshot):
    from collections import OrderedDict
    from datetime import date
    from types import SimpleNamespace
    rolling = tmp_path / "rolling"
    preserved = tmp_path / "snapshot" / "equity_1m"
    rolling.mkdir()
    preserved.mkdir(parents=True)
    frame, _ = stock_case()
    frame = frame.rename(columns={"ts": "date"})
    rolling_file = rolling / "TEST_stocks_indicators_1min.parquet"
    saved_file = preserved / rolling_file.name
    frame.to_parquet(rolling_file, index=False)
    frame.to_parquet(saved_file, index=False)
    reader = object.__new__(r.PreservedPrices)
    reader.through = date(2026, 10, 19)
    reader.snapshots = {reader.through: preserved.parent}
    reader.expected = {str(saved_file.resolve()): r.base.sha256(saved_file)}
    reader.baseline_proof = {str(rolling_file.resolve()): {"sha256": "old-baseline-bytes"}}
    reader.cache, reader.proof, reader.resolved = OrderedDict(), {}, {}
    monkeypatch.setattr(r.base.source, "DataRoots", lambda: SimpleNamespace(equity_1m=rolling))
    if tamper_snapshot:
        changed = frame.copy()
        changed["volume"] += 1
        changed.to_parquet(saved_file, index=False)
        with pytest.raises(ValueError, match="snapshot hash differs"):
            reader.frame(date(2026, 10, 15), "TEST")
    else:
        actual, key = reader.frame(date(2026, 10, 15), "TEST")
        assert key == str(saved_file.resolve())
        assert reader.proof[key]["source_kind"] == "PRESERVED_HISTORICAL_SNAPSHOT"
        assert reader.proof[key]["baseline_rolling_sha256"] == "old-baseline-bytes"
        assert len(actual) == len(frame)


def test_chronological_selection_cannot_see_the_next_block(monkeypatch):
    days = pd.date_range("2026-07-01", periods=30).strftime("%Y-%m-%d").tolist()
    books = {name: pd.DataFrame({"day": days, "pnl": np.ones(30)}) for name in r.POLICIES}
    books["volatility025"].loc[:19, "pnl"] = 2.
    books["primary_current"].loc[20:, "pnl"] = 1e9

    def stats(book, cohort):
        value = book.loc[book.day.isin(cohort), "pnl"].sum()
        return dict(net_pnl_rs=value, losing_days=0, negative_trade_sum_abs_rs=0.,
                    daily_close_drawdown_rs=0.)

    monkeypatch.setattr(r, "extra_stats", stats)
    _, choices = r.chronological_selection(books, days)
    assert choices.policy.tolist() == ["volatility025"]
    assert choices.train_end.iloc[0] < choices.test_start.iloc[0]
    books["primary_current"].loc[20:, "pnl"] = -1e9
    _, changed = r.chronological_selection(books, days)
    assert changed.policy.tolist() == choices.policy.tolist()
