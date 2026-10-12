"""Independent formula, timestamp and failure-mode checks for fixed PF arms."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from v14 import features
from v14 import pf_indicators as pf


def _minutes(days=2, count=31):
    frames = []
    for number, day in enumerate(pd.bdate_range("2026-07-01", periods=days)):
        ts = pd.date_range(f"{day:%Y-%m-%d} 09:16", periods=count, freq="min", tz=features.IST)
        close = 100 + number * 2 + np.arange(count) * .1
        frames.append(pd.DataFrame(dict(ts=ts, open=close - .03, high=close + .05,
                                        low=close - .05, close=close, volume=100.)))
    return pd.concat(frames, ignore_index=True)


def _candidates(minute):
    frame = features.build_features(minute, "TEST", history_sessions=1)
    frame["side"] = "LONG"
    frame.index = np.arange(len(frame)) * 3 + 17
    return frame


def _variant(name):
    return next(v for v in pf.variants() if v.name == name)


def test_declared_fourteen_arms_and_thresholds_do_not_expand():
    assert [v.name for v in pf.variants()] == ["guard_control", "vwap_acceptance",
        "opening5m_range_breakout", "prior5m_range_breakout", "efficiency10", "fast_adx5",
        "ema9_slope5m", "vwap_efficiency", "vwap_adx5", "opening_range_vwap",
        "prior_range_efficiency", "vwap_efficiency_rvol1p5", "vwap_adx5_rvol1p5", "vwap_reclaim"]
    assert len({v.name for v in pf.variants()}) == 14
    assert not any("rsi" in v.name for v in pf.variants())
    assert pf.specification()["execution_authority"] is False


def test_all_features_ready_at_0925_and_candidate_rows_unchanged():
    minute = _minutes()
    candidates = _candidates(minute)
    original_minute, original_candidates = minute.copy(deep=True), candidates.copy(deep=True)
    out = pf.add_features(minute, candidates)
    early = out.loc[out.hhmm.eq("0925")]
    assert len(early) == 2
    assert np.isfinite(early[pf.COMMON_FEATURES]).all().all()
    assert early.pf_session_complete.all()
    assert out.index.equals(candidates.index)
    pd.testing.assert_frame_equal(out[candidates.columns], candidates)
    pd.testing.assert_frame_equal(minute, original_minute)
    pd.testing.assert_frame_equal(candidates, original_candidates)


def test_vwap_opening_range_and_efficiency_match_hand_values():
    minute = _minutes(days=1, count=16)
    minute["close"] = np.arange(100., 116.)
    minute["open"] = minute.close - .2
    minute["high"], minute["low"] = minute.close + 1., minute.close - 1.
    minute["volume"] = np.arange(1., 17.)
    out = pf.add_features(minute, _candidates(minute))
    row = out.loc[out.hhmm.eq("0925")].iloc[0]
    assert row.pf_vwap == pytest.approx(sum((100 + i) * (i + 1) for i in range(10)) / 55.)
    assert row.pf_vwap_start5 == pytest.approx(sum((100 + i) * (i + 1) for i in range(5)) / 15.)
    assert row.pf_vwap_slope5 == pytest.approx(row.pf_vwap - row.pf_vwap_start5)
    assert row.pf_opening5_high == row.pf_prior5_high == 105.
    assert row.pf_opening5_low == row.pf_prior5_low == 99.
    assert row.pf_efficiency10 == 1.
    assert row.pf_adx5 == 100.
    assert row.pf_plus_di5 > 0 and row.pf_minus_di5 == 0


def test_efficiency_counts_nine_returns_between_ten_actual_closes():
    minute = _minutes(days=1, count=11)
    close = np.array([100., 101., 100., 102., 101., 103., 102., 104., 103., 105., 106.])
    minute["close"], minute["open"] = close, close - .05
    minute["high"], minute["low"] = close + .1, close - .1
    row = pf.add_features(minute, _candidates(minute)).iloc[0]
    assert row.pf_efficiency10 == pytest.approx(5 / 13)


def test_adx_wilder_seed_and_recursion_match_independent_fraction_calculation():
    # Initial four transitions: +DM=3, -DM=2, TR=9. Then alternate
    # (+DM=2,TR=3), (-DM=1,TR=2); smooth each sum by4/5 and add.
    high = np.array([10., 11., 10., 12., 11., 13., 12., 14., 13., 15., 14., 16.])
    adx, plus, minus = pf._adx5(high, high - 2, high - 1)
    assert np.isnan(adx[:9]).all()
    np.testing.assert_allclose(adx[9:], [35.874000090849684, 33.19310339332171, 35.289808240277516], atol=1e-12)
    assert plus[9] == pytest.approx(43.35671971827141, abs=1e-12)
    assert minus[9] == pytest.approx(16.78359859135705, abs=1e-12)
    assert all(np.isnan(x).all() for x in pf._adx5(high[:9], high[:9]-2, high[:9]-1))


def test_adx_direction_is_symmetric_and_zero_movement_is_neutral():
    high = np.arange(100., 112.) + 1
    low, close = high - 2, high - 1
    up = pf._adx5(high, low, close)
    down = pf._adx5(300-low, 300-high, 300-close)
    np.testing.assert_allclose(up[0], down[0], equal_nan=True)
    np.testing.assert_allclose(up[1], down[2], equal_nan=True)
    np.testing.assert_allclose(up[2], down[1], equal_nan=True)
    flat = pf._adx5(np.full(12, 100.), np.full(12, 100.), np.full(12, 100.))
    assert flat[0][9:].tolist() == [0., 0., 0.]


def test_future_mutation_and_truncation_leave_completed_features_identical():
    minute = _minutes(days=3)
    candidates = _candidates(minute)
    cutoff = pd.Timestamp(f"{minute.ts.dt.date.max()} 09:30", tz=features.IST)
    before = candidates.loc[candidates.signal_ts.le(cutoff)]
    expected = pf.add_features(minute, before)
    prefix = pf.add_features(minute.loc[minute.ts.le(cutoff)], before)
    pd.testing.assert_frame_equal(expected, prefix)
    altered = minute.copy()
    altered.loc[altered.ts.gt(cutoff), ["open", "high", "low", "close", "volume"]] *= 100
    pd.testing.assert_frame_equal(expected, pf.add_features(altered, before))


@pytest.mark.parametrize("missing_minute", [16, 18, 25])
def test_session_gaps_fail_closed_without_invalidating_earlier_signals(missing_minute):
    minute = _minutes()
    candidates = _candidates(minute)
    last = minute.ts.dt.date.max()
    missing = pd.Timestamp(f"{last} 09:{missing_minute}", tz=features.IST)
    damaged = minute.loc[minute.ts.ne(missing)]
    out = pf.add_features(damaged, candidates)
    current = out.loc[out.day.eq(str(last)) & out.signal_ts.ge(missing)]
    assert not current.pf_session_complete.any()
    assert current.pf_vwap.isna().all() and current.pf_adx5.isna().all()
    previous = candidates.loc[candidates.signal_ts.lt(missing)]
    pd.testing.assert_frame_equal(out.loc[previous.index], pf.add_features(minute, previous))


def test_flagged_or_invalid_minute_is_a_gap_and_next_session_resets():
    minute = _minutes(days=3)
    candidates = _candidates(minute)
    middle = sorted(minute.ts.dt.date.unique())[1]
    minute["gap_filled"] = 0
    minute.loc[minute.ts.eq(pd.Timestamp(f"{middle} 09:18", tz=features.IST)), "gap_filled"] = 1
    out = pf.add_features(minute, candidates)
    assert out.loc[out.day.eq(str(middle)), "pf_vwap"].isna().all()
    last = minute.ts.dt.date.max()
    row = out.loc[out.day.eq(str(last)) & out.hhmm.eq("0925")].iloc[0]
    standalone = pf.add_features(minute.loc[minute.ts.dt.date.eq(last)], candidates.loc[candidates.day.eq(str(last))])
    session_columns = [name for name in pf.COMMON_FEATURES if name != "pf_ema9_slope5"]
    pd.testing.assert_frame_equal(out.loc[standalone.index, session_columns], standalone[session_columns])
    assert row.pf_session_complete and row.pf_adx5 == 100.


def test_ema_slope_preserves_full_native_history_and_exact_prior_bar():
    minute = _minutes(days=3)
    candidates = _candidates(minute)
    out = pf.add_features(minute, candidates)
    five = features.aggregate_five(features.clean_minutes(minute)).set_index("ts")
    expected = candidates.signal_ts.map(five.ema9) - (candidates.signal_ts-pd.Timedelta(minutes=5)).map(five.ema9)
    pd.testing.assert_series_equal(out.pf_ema9_slope5, expected.rename("pf_ema9_slope5"))
    last = minute.ts.dt.date.max()
    isolated = pf.add_features(minute.loc[minute.ts.dt.date.eq(last)], candidates.loc[candidates.day.eq(str(last))])
    assert not np.allclose(out.loc[isolated.index, "pf_ema9_slope5"], isolated.pf_ema9_slope5)


def _gate_rows():
    return pd.DataFrame(dict(side=["LONG", "SHORT"], open=[100., 100.], close=[101., 99.],
        pf_vwap=[100.5, 99.5], pf_vwap_start5=[100.25, 99.75], pf_vwap_slope5=[.25, -.25],
        pf_opening5_high=[100.9, 100.], pf_opening5_low=[100., 99.1],
        pf_prior5_high=[100.9, 100.], pf_prior5_low=[100., 99.1],
        pf_efficiency10=[.5, -.5], pf_adx5=[25., 25.], pf_plus_di5=[30., 10.], pf_minus_di5=[10., 30.],
        pf_ema9_slope5=[.1, -.1], interval_rvol=[1.5, 1.5]), index=[13, 2])


def test_all_masks_side_symmetry_inclusive_thresholds_and_index_preservation():
    rows = _gate_rows()
    original = rows.copy(deep=True)
    for variant in pf.variants():
        mask = pf.participation_mask(rows, variant)
        assert mask.tolist() == [True, True], variant
        assert mask.index.equals(rows.index)
    pd.testing.assert_frame_equal(rows, original)
    rows["pf_adx5"] = 24.999
    assert not pf.participation_mask(rows, _variant("fast_adx5")).any()
    rows["pf_efficiency10"] = [.49999, -.49999]
    assert not pf.participation_mask(rows, _variant("efficiency10")).any()


def test_reclaim_uses_interval_start_vwap_not_ending_vwap():
    rows = _gate_rows().iloc[:1].copy()
    rows["open"] = 100.4  # Below endingVWAP100.5 but above startVWAP100.25.
    assert pf.participation_mask(rows, _variant("vwap_acceptance")).iloc[0]
    assert not pf.participation_mask(rows, _variant("vwap_reclaim")).iloc[0]


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, "invalid"])
def test_enabled_masks_fail_closed_on_nonfinite_required_inputs(bad):
    for variant_name, column in [("vwap_acceptance", "pf_vwap"), ("opening5m_range_breakout", "pf_opening5_high"),
        ("prior5m_range_breakout", "pf_prior5_high"), ("efficiency10", "pf_efficiency10"),
        ("fast_adx5", "pf_plus_di5"), ("ema9_slope5m", "pf_ema9_slope5"),
        ("vwap_efficiency_rvol1p5", "interval_rvol"), ("vwap_reclaim", "pf_vwap_start5")]:
        rows = _gate_rows().astype(object)
        rows[column] = bad
        assert not pf.participation_mask(rows, _variant(variant_name)).any(), (variant_name, column)


def test_strict_side_and_breakout_equalities_fail_closed():
    rows = _gate_rows()
    rows["side"] = "UNKNOWN"
    for variant in pf.variants()[1:]:
        assert not pf.participation_mask(rows, variant).any()
    rows = _gate_rows()
    rows["pf_prior5_high"], rows["pf_prior5_low"] = rows.close, rows.close
    assert not pf.participation_mask(rows, _variant("prior5m_range_breakout")).any()
    rows["pf_vwap_slope5"] = 0.
    assert not pf.participation_mask(rows, _variant("vwap_acceptance")).any()


def test_empty_candidates_and_missing_optional_values_are_safe():
    minute = _minutes()
    candidates = _candidates(minute).iloc[:0]
    out = pf.add_features(minute, candidates)
    assert out.empty and all(name in out for name in pf.COMMON_FEATURES)
    rows = pd.DataFrame(index=[6, 8])
    assert pf.participation_mask(rows, _variant("guard_control")).all()
    for variant in pf.variants()[1:]:
        assert not pf.participation_mask(rows, variant).any()
