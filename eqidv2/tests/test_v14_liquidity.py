import numpy as np
import pandas as pd
import pytest

from v14.liquidity import ScreenConfig, add_capacity_features, add_liquidity_features, daily_liquidity, liquidity_decisions, liquidity_mask


def fixture_minutes():
    sessions = pd.bdate_range("2026-07-01", periods=24).strftime("%Y-%m-%d").tolist()
    frames = []
    for day in sessions:
        ts = pd.date_range(day + " 09:16", periods=375, freq="min", tz="Asia/Kolkata")
        frames.append(pd.DataFrame(dict(ts=ts, open=100., high=101., low=99., close=100., volume=10000.)))
    minute = pd.concat(frames, ignore_index=True)
    candidates = pd.DataFrame({"tradingsymbol": ["TEST", "TEST"], "confirmation_ts":
        [pd.Timestamp(sessions[-2] + " 09:26", tz="Asia/Kolkata"), pd.Timestamp(sessions[-1] + " 09:36", tz="Asia/Kolkata")]}, index=[9, 4])
    return minute, candidates, sessions


def build(minute, candidates, sessions):
    return add_liquidity_features(minute, candidates, session_days=sessions, order_notional_rupees=500000.)


def config():
    return ScreenConfig(100000000., .99, .8, .05, min_current_active_fraction=.9)


def test_measures_size_against_cash_turnover_and_preserves_candidate_order():
    minute, candidates, sessions = fixture_minutes()
    result = build(minute, candidates, sessions)
    assert result.index.tolist() == [9, 4]
    assert result.liq_prior_median_turnover_rupees.eq(375000000.).all()
    assert result.liq_current10_turnover_rupees.eq(10000000.).all()
    assert result.liq_order_participation10.eq(.05).all()
    assert result.liq_order_participation_observed1.eq(.5).all()
    assert liquidity_mask(result, config()).all()


def test_future_prices_volume_and_missing_minutes_cannot_change_prior_decision():
    minute, candidates, sessions = fixture_minutes()
    cutoff = candidates.iloc[0].confirmation_ts
    expected = build(minute, candidates.iloc[:1], sessions)
    changed = minute.copy()
    changed.loc[changed.ts.gt(cutoff), ["open", "high", "low", "close", "volume"]] *= 1000
    changed = changed.loc[~(changed.ts.gt(cutoff) & changed.ts.dt.minute.mod(2).eq(0))]
    actual = build(changed, candidates.iloc[:1], sessions)
    pd.testing.assert_frame_equal(actual, expected)
    prefix = build(minute.loc[minute.ts.le(cutoff)], candidates.iloc[:1], sessions)
    pd.testing.assert_frame_equal(prefix, expected)


def test_absent_prior_session_consumes_history_slot_and_fails_closed():
    minute, candidates, sessions = fixture_minutes()
    missing = sessions[-4]
    minute = minute.loc[minute.ts.dt.strftime("%Y-%m-%d").ne(missing)]
    result = build(minute, candidates, sessions)
    assert result.liq_history_sessions.eq(19).all()
    assert result.liq_prior_median_turnover_rupees.isna().all()
    assert not liquidity_mask(result, config()).any()
    assert daily_liquidity(minute, sessions).set_index("day").loc[missing, "minute_coverage"] == 0


def test_gap_and_flagged_bar_in_current_window_are_not_filled():
    minute, candidates, sessions = fixture_minutes()
    gap = candidates.iloc[0].confirmation_ts - pd.Timedelta(minutes=4)
    minute["gap_filled"] = minute.ts.eq(gap)
    result = build(minute, candidates.iloc[:1], sessions)
    assert result.liq_current10_minute_coverage.iloc[0] == .9
    assert pd.isna(result.liq_current10_turnover_rupees.iloc[0])
    assert not result.liq_open_prefix_complete.iloc[0]
    assert not liquidity_mask(result, config()).iloc[0]


def test_low_liquidity_and_zero_volume_rejected_at_confirmation_without_paths():
    minute, candidates, sessions = fixture_minutes()
    cutoff = candidates.iloc[0].confirmation_ts
    in_window = minute.ts.le(cutoff) & minute.ts.gt(cutoff - pd.Timedelta(minutes=10))
    minute.loc[in_window, "volume"] = 0
    result = build(minute, candidates.iloc[:1], sessions)
    decision = liquidity_decisions(result, config()).iloc[0]
    assert not decision.liquidity_pass
    assert "LOW_CURRENT_ACTIVE_FRACTION" in decision.liquidity_reject_reason
    assert "EXCESSIVE_ORDER_PARTICIPATION" in decision.liquidity_reject_reason


def test_unscheduled_candidate_and_mixed_symbols_are_rejected():
    minute, candidates, sessions = fixture_minutes()
    with pytest.raises(ValueError, match="calendar"):
        build(minute, candidates, sessions[:-1])
    candidates.loc[4, "tradingsymbol"] = "OTHER"
    with pytest.raises(ValueError, match="one symbol"):
        build(minute, candidates, sessions)


def test_invalid_policy_values_fail_instead_of_widening_eligibility():
    with pytest.raises(ValueError):
        ScreenConfig(1., .99, 1.1, .05)
    with pytest.raises(ValueError):
        ScreenConfig(1., .99, 1., np.inf)


def test_capacity_diagnostics_use_separate_completed_signal_and_confirmation_bars():
    rows = pd.DataFrame(dict(traded_value=[10000000., 0., np.inf, 10000000.],
        confirmation_close=[100., 100., 100., -100.], confirmation_volume=[50000., 0., 1., -1.]))
    result = add_capacity_features(rows, order_notional_rupees=500000.)
    assert result.liq_order_participation5.iloc[0] == .05
    assert result.liq_order_participation1.iloc[0] == .1
    assert result.liq_order_participation5.iloc[1:3].isna().all()
    assert result.liq_order_participation1.iloc[[1, 3]].isna().all()
