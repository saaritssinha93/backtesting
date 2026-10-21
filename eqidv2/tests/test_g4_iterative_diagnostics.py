"""Descriptive path statistics cannot borrow unknown entry/exit ranges."""
import numpy as np
import pandas as pd
import pytest

from research import g4_iterative_diagnostics as diagnostics


def fixture(side="LONG"):
    stamps = pd.date_range("2026-10-16 10:01", periods=8, freq="min", tz="Asia/Kolkata")
    close = np.array([130., 100.2, 100.4, 99.9, 100.6, 100.8, 100.1, 150.])
    high = close + .1
    low = close - .1
    high[0], low[0] = 180., 20.
    if side == "SHORT":
        close, high, low = 200. - close, 200. - low, 200. - high
    path = dict(timestamp_ns=stamps.asi8, open=np.full(8, 100.), high=high, low=low, close=close)
    row = dict(sid=1, day="2026-10-16", setup_id="1000_" + side, tradingsymbol="SYNTHETIC", side=side,
        filled=True, portfolio_executed=True, entry_price=100., entry_ts=stamps[0], entry_path_index=0,
        exit_ts=stamps[7], exit_reason="TARGET", portfolio_net_profit_rupees=4750.)
    return pd.DataFrame([row]), {1: path}


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_full_completed_minutes_exclude_entry_exit_and_are_direction_symmetric(side):
    native, paths = fixture(side)
    row = diagnostics.trade_diagnostics(native, paths).iloc[0]
    assert row.eligible_completed_minutes == 6
    assert row.best_completed_close_profit_pct == pytest.approx(.8)
    assert row.worst_completed_close_adverse_pct == pytest.approx(.1)
    assert row.worst_full_minute_adverse_pct == pytest.approx(.2)
    assert row.first_positive_close_minutes == 1
    for threshold, minute in ((.2, 1), (.4, 2), (.6, 4), (.8, 5)):
        assert row[f"reached_{threshold:.1f}pct"]
        assert row[f"first_{threshold:.1f}pct_minutes"] == minute


def test_future_extremes_and_native_exit_candle_cannot_change_diagnostics():
    native, paths = fixture()
    before = diagnostics.trade_diagnostics(native, paths)
    for field in ("high", "low", "close"):
        paths[1][field][[0, 7]] = [1e8, 1e8]
    pd.testing.assert_frame_equal(diagnostics.trade_diagnostics(native, paths), before)


def test_native_open_exit_excludes_the_signal_time_and_following_bar():
    native, paths = fixture()
    native.loc[0, "exit_ts"] = pd.Timestamp("2026-10-16 10:05", tz="Asia/Kolkata")
    row = diagnostics.trade_diagnostics(native, paths).iloc[0]
    assert row.eligible_completed_minutes == 3
    assert row.best_completed_close_profit_pct == pytest.approx(.4)
    assert not row["reached_0.6pct"]
    assert not row.active_at_5m


def test_no_eligible_minute_is_unknown_and_separate_from_never_profitable_loss():
    native, paths = fixture()
    native.loc[0, "exit_ts"] = native.loc[0, "entry_ts"]
    native.loc[0, "portfolio_net_profit_rupees"] = -6250.
    trades = diagnostics.trade_diagnostics(native, paths)
    assert trades.eligible_completed_minutes.iloc[0] == 0
    assert pd.isna(trades.best_completed_close_profit_pct.iloc[0])
    summary, tables = diagnostics.summarize(native, trades)
    assert summary["no_eligible_completed_minutes"] == 1
    assert tables["loss_profit_buckets"].observed_profit_bucket.tolist() == ["no_eligible_completed_minutes"]
    losses = tables["profit_thresholds"].loc[lambda x: x.outcome.eq("LOSS")]
    assert losses.trades_with_eligible_minutes.eq(0).all()


def test_only_original_allocations_enter_matched_cohort_and_winner_budget():
    native, paths = fixture()
    excluded = native.copy()
    excluded["sid"] = 2
    excluded["portfolio_executed"] = False
    excluded["portfolio_net_profit_rupees"] = 0.
    native = pd.concat([native, excluded], ignore_index=True)
    trades = diagnostics.trade_diagnostics(native, paths)
    summary, tables = diagnostics.summarize(native, trades)
    assert len(trades) == 1
    assert summary["original_winner_combined_net_profit_rs"] == 4750.
    assert summary["maximum_five_percent_winner_profit_loss_rs"] == pytest.approx(237.5)
    assert summary["minimum_original_winner_combined_profit_at_95pct_rs"] == pytest.approx(4512.5)
    assert tables["setup_samples"].selected_orders.iloc[0] == 2
    assert tables["setup_samples"].originally_allocated.iloc[0] == 1


def test_missing_minutes_and_entry_identity_mismatch_are_rejected():
    native, paths = fixture()
    paths[1]["timestamp_ns"][3] += 1
    with pytest.raises(ValueError, match="Contiguous"):
        diagnostics.trade_diagnostics(native, paths)
    native, paths = fixture()
    native.loc[0, "entry_path_index"] = 1
    with pytest.raises(ValueError, match="Entry candle identity"):
        diagnostics.trade_diagnostics(native, paths)
