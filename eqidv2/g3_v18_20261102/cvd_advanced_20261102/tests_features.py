"""Minute allocation, acceleration causality and prior-session RVOL checks."""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from .features import FEATURE_FIELDS, IST, attach_features, calculate_features


def minutes(n=60, day="2026-10-29", symbol="RELIANCE", volume=100.):
    return pd.DataFrame(dict(symbol=[symbol]*n,
        minute_end=pd.date_range(f"{day} 09:16", periods=n, freq="min", tz=IST),
        open=[100.]*n, high=[110.]*n, low=[90.]*n, close=[105.]*n, volume=[volume]*n))


def history(n=22, length=60):
    days = pd.bdate_range("2026-08-03", periods=n).strftime("%Y-%m-%d").tolist()
    return pd.concat([minutes(length, day) for day in days], ignore_index=True), days


def at(frame, clock="09:45"):
    return frame.loc[frame.bar_end.dt.strftime("%H:%M").eq(clock)]


class TestAdvancedFeatures(unittest.TestCase):
    def test_native_minute_delta_aggregation_and_warmup(self):
        out = calculate_features(minutes(30))
        self.assertEqual(out.bar_end.dt.strftime("%H:%M").tolist(), ["09:20", "09:25", "09:30", "09:35", "09:40", "09:45"])
        np.testing.assert_allclose(out.adv_delta5, [250.]*6)
        np.testing.assert_allclose(out.volume, [500.]*6)
        np.testing.assert_allclose(out.adv_session_cvd, np.arange(1, 7)*250.)
        self.assertEqual(out.adv_ready.tolist(), [False]*5+[True])
        row = out.iloc[-1]
        self.assertEqual(row.adv_recent15_pressure, .5)
        self.assertEqual(row.adv_prior15_pressure, .5)
        self.assertEqual(row.adv_acceleration, 0.)
        self.assertEqual(row.source_1m_count, 5)

    def test_acceleration_extremes_and_sign(self):
        raw = minutes(30)
        raw.loc[:14, "close"] = 90.
        raw.loc[15:, "close"] = 110.
        row = calculate_features(raw).iloc[-1]
        self.assertEqual(row.adv_prior15_delta, -1500.)
        self.assertEqual(row.adv_recent15_delta, 1500.)
        self.assertEqual(row.adv_prior15_pressure, -1.)
        self.assertEqual(row.adv_recent15_pressure, 1.)
        self.assertEqual(row.adv_acceleration, 2.)
        self.assertEqual(row.adv_acceleration_normalized, 1.)
        raw["close"] = 200. - raw.close
        row = calculate_features(raw).iloc[-1]
        self.assertEqual(row.adv_acceleration, -2.)
        self.assertEqual(row.adv_acceleration_normalized, -1.)

    def test_aggregate_ohlcv_uses_exact_source_minutes(self):
        raw = minutes(5)
        raw["open"] = [100., 101., 102., 103., 104.]
        raw["high"] = [111., 113., 110., 115., 112.]
        raw["low"] = [89., 92., 94., 91., 90.]
        raw["close"] = [105., 100., 102., 101., 104.]
        raw["volume"] = [1., 2., 3., 4., 5.]
        row = calculate_features(raw).iloc[0]
        self.assertEqual((row.open, row.high, row.low, row.close, row.volume), (100., 115., 89., 104., 15.))
        expected = (raw.volume * (2*raw.close-raw.high-raw.low)/(raw.high-raw.low)).sum()
        self.assertAlmostEqual(row.adv_delta5, expected)
        aggregate_delta = row.volume * (2*row.close-row.high-row.low)/(row.high-row.low)
        self.assertNotAlmostEqual(row.adv_delta5, aggregate_delta)

    def test_flat_minutes_neutral_without_division_error(self):
        raw = minutes(30)
        raw[["open", "high", "low", "close"]] = 100.
        out = calculate_features(raw)
        self.assertTrue(out.adv_delta5.eq(0).all())
        self.assertEqual(out.adv_acceleration.iloc[-1], 0.)
        self.assertTrue(out.adv_ready.iloc[-1])

    def test_nonoverlapping_windows_no_cumulative_difference(self):
        raw = minutes(45)
        raw.loc[:14, "close"] = 90.
        raw.loc[15:29, "close"] = 100.
        raw.loc[30:, "close"] = 110.
        out = calculate_features(raw)
        self.assertEqual(at(out, "09:45").adv_acceleration.iloc[0], 1.)
        self.assertEqual(at(out, "10:00").adv_acceleration.iloc[0], 1.)
        self.assertEqual(at(out, "10:00").adv_prior15_pressure.iloc[0], 0.)

    def test_session_reset_and_stock_independence(self):
        first = minutes(30)
        second = minutes(30, "2026-10-30")
        second["close"] = 90.
        other = minutes(30, symbol="TCS")
        out = calculate_features(pd.concat([second, first, other]).sample(frac=1, random_state=9))
        own = out.loc[out.symbol.eq("RELIANCE")]
        self.assertEqual(own.adv_ready.tolist(), [False]*5+[True]+[False]*5+[True])
        self.assertEqual(own.adv_session_cvd.iloc[6], -500.)
        self.assertEqual(out.loc[out.symbol.eq("TCS"), "adv_session_cvd"].iloc[-1], 1500.)

    def test_future_minutes_do_not_change_earlier_features(self):
        raw = minutes(60)
        prefix = calculate_features(raw.iloc[:30])
        raw.loc[30:, "close"] = 90.
        raw.loc[30:, "volume"] = 1000000.
        full = calculate_features(raw)
        assert_frame_equal(prefix, full.iloc[:6].reset_index(drop=True))

    def test_gap_invalidates_then_acceleration_recovers_after30(self):
        raw = minutes(70).drop(index=30)  # 09:46 missing
        out = calculate_features(raw)
        self.assertTrue(at(out, "09:45").adv_ready.iloc[0])
        self.assertFalse(out.loc[out.bar_end.dt.strftime("%H:%M").between("09:50", "10:15"), "adv_ready"].any())
        self.assertTrue(at(out, "10:20").adv_ready.iloc[0])
        self.assertTrue(out.loc[out.bar_end.dt.strftime("%H:%M").ge("09:50"), "adv_session_cvd"].isna().all())

    def test_missing_endpoint_never_attaches_prior_minute(self):
        raw = minutes(35).drop(index=29)  # 09:45 boundary missing
        out = calculate_features(raw)
        row = at(out, "09:45").iloc[0]
        self.assertFalse(row.bar_contiguous)
        self.assertFalse(row.adv_ready)
        self.assertTrue(pd.isna(row.adv_acceleration))

    def test_offgrid_minute_and_flagged_minute_fail_closed(self):
        for kind in ("offgrid", "flagged"):
            with self.subTest(kind=kind):
                raw = minutes(60)
                if kind == "offgrid":
                    raw.loc[15, "minute_end"] += pd.Timedelta(seconds=1)
                else:
                    raw["is_synthetic"] = False
                    raw.loc[15, "is_synthetic"] = True
                out = calculate_features(raw)
                self.assertFalse(at(out, "09:45").adv_ready.iloc[0])
                self.assertTrue(at(out, "10:05").adv_ready.iloc[0])

    def test_invalid_values_preserve_bad_aggregation_status(self):
        for field, bad in (("close", 120.), ("high", np.nan), ("volume", -1.), ("volume", np.inf)):
            with self.subTest(field=field, bad=bad):
                raw = minutes(30)
                raw.loc[20, field] = bad
                with np.errstate(invalid="ignore"):
                    out = calculate_features(raw)
                self.assertFalse(out.adv_ready.any())
                self.assertTrue(out.adv_session_cvd.iloc[4:].isna().all())

    def test_zero_volume_in_either_window_fails_ready(self):
        for begin, stop in ((0, 14), (15, 29), (25, 29)):
            with self.subTest(begin=begin):
                raw = minutes(30)
                raw.loc[begin:stop, "volume"] = 0.
                row = calculate_features(raw).iloc[-1]
                self.assertFalse(row.adv_ready)
                self.assertTrue(pd.isna(row.adv_acceleration))

    def test_initial_missing_prefix_can_have_local_acceleration(self):
        out = calculate_features(minutes(40).iloc[5:])
        self.assertTrue(at(out, "09:50").adv_ready.iloc[0])
        self.assertTrue(out.adv_session_cvd.isna().all())

    def test_duplicate_naive_and_missing_symbol_rejected(self):
        raw = minutes(30)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            calculate_features(pd.concat([raw, raw.iloc[[0]]]))
        naive = raw.copy()
        naive["minute_end"] = naive.minute_end.dt.tz_localize(None)
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            calculate_features(naive)
        raw.loc[0, "symbol"] = ""
        with self.assertRaisesRegex(ValueError, "symbol"):
            calculate_features(raw)

    def test_timezone_and_capture_completion(self):
        raw = minutes(32)
        out = calculate_features(raw)
        self.assertEqual(out.bar_end.iloc[-1].strftime("%H:%M"), "09:45")
        utc = raw.copy()
        utc["minute_end"] = utc.minute_end.dt.tz_convert("UTC")
        assert_frame_equal(out, calculate_features(utc))
        with self.assertRaisesRegex(ValueError, "after as_of"):
            calculate_features(raw, as_of=raw.minute_end.iloc[-2])
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            calculate_features(raw, as_of="2026-10-29 09:47")

    def test_rvol_twenty_prior_sessions_and_shift(self):
        raw, days = history()
        raw.loc[raw.minute_end.dt.strftime("%Y-%m-%d").eq(days[20]), "volume"] = 200.
        clocks = at(calculate_features(raw)).reset_index(drop=True)
        self.assertFalse(clocks.adv_rvol_ready.iloc[:20].any())
        self.assertTrue(clocks.adv_rvol20.iloc[:20].isna().all())
        self.assertEqual(clocks.adv_rvol20.iloc[20], 2.)
        self.assertEqual(clocks.adv_rvol20_baseline_volume.iloc[20], 1500.)
        self.assertEqual(clocks.adv_activity_multiplier.iloc[20], 1.)
        self.assertAlmostEqual(clocks.adv_rvol20.iloc[21], 1500. / 1575.)
        self.assertEqual(clocks.adv_rvol_history_count.iloc[20], 20)

    def test_missing_prior_clock_not_replaced_with_older_session(self):
        raw, days = history(23)
        missing = raw.minute_end.eq(pd.Timestamp(f"{days[10]} 09:40", tz=IST))
        out = at(calculate_features(raw.loc[~missing])).reset_index(drop=True)
        self.assertFalse(out.adv_rvol_ready.iloc[-1])
        self.assertEqual(out.adv_rvol_prior_sessions.iloc[-1], 20)
        self.assertEqual(out.adv_rvol_history_count.iloc[-1], 19)
        self.assertTrue(pd.isna(out.adv_rvol20.iloc[-1]))
        self.assertEqual(out.adv_rvol_reason.iloc[-1], "MISSING_PRIOR_EXACT_CLOCK_WINDOW")

    def test_no_provider_date_is_not_in_observed_date_grid(self):
        raw, days = history(23)
        raw = raw.loc[~raw.minute_end.dt.strftime("%Y-%m-%d").eq(days[10])]
        out = at(calculate_features(raw)).reset_index(drop=True)
        self.assertEqual(len(out), 22)
        self.assertTrue(out.adv_rvol_ready.iloc[20])

    def test_future_day_changes_do_not_change_historical_rvol(self):
        raw, days = history(23)
        prefix = raw.loc[raw.minute_end.dt.strftime("%Y-%m-%d").le(days[20])]
        expected = calculate_features(prefix)
        changed = raw.copy()
        future = changed.minute_end.dt.strftime("%Y-%m-%d").gt(days[20])
        changed.loc[future, "volume"] = 1000000.
        changed.loc[future, "close"] = 90.
        actual = calculate_features(changed)
        actual = actual.loc[actual.session_date.le(days[20])].reset_index(drop=True)
        assert_frame_equal(expected, actual)

    def test_current_later_minutes_cannot_affect_same_clock_rvol(self):
        raw, days = history(21)
        before = calculate_features(raw)
        later = raw.minute_end.gt(pd.Timestamp(f"{days[-1]} 09:45", tz=IST))
        raw.loc[later, "volume"] = 1000000.
        after = calculate_features(raw)
        old = at(before).loc[lambda f: f.session_date.eq(days[-1])].reset_index(drop=True)
        new = at(after).loc[lambda f: f.session_date.eq(days[-1])].reset_index(drop=True)
        assert_frame_equal(old, new)

    def test_activity_multiplier_fixed_bounds(self):
        for scale, expected in ((.5, 0.), (1., 0.), (1.5, .5), (2., 1.), (4., 1.)):
            with self.subTest(scale=scale):
                raw, days = history(21, length=30)
                raw.loc[raw.minute_end.dt.strftime("%Y-%m-%d").eq(days[-1]), "volume"] *= scale
                row = at(calculate_features(raw)).iloc[-1]
                self.assertAlmostEqual(row.adv_activity_multiplier, expected)

    def test_exact_attach_preserves_order_and_missing_is_neutral_ready(self):
        enriched = calculate_features(minutes(35))
        stamps = [pd.Timestamp("2026-10-29 09:45", tz=IST), pd.Timestamp("2026-10-29 09:44", tz=IST)]
        raw = pd.DataFrame(dict(tradingsymbol=["RELIANCE", "RELIANCE", "UNKNOWN"], signal_ts=[stamps[0], stamps[1], stamps[0]], sid=[8, 2, 7]), index=[5, 10, 1])
        attached = attach_features(raw, enriched)
        self.assertEqual(attached.index.tolist(), raw.index.tolist())
        self.assertEqual(attached.sid.tolist(), raw.sid.tolist())
        self.assertEqual(attached.adv_ready.tolist(), [True, False, False])
        self.assertEqual(attached.adv_reason.tolist()[1:], ["MISSING_EXACT_BAR", "MISSING_EXACT_BAR"])
        self.assertTrue(attached.adv_acceleration.iloc[1:].isna().all())

    def test_attach_rejects_later_feature_stamp(self):
        enriched = calculate_features(minutes(30))
        raw = pd.DataFrame(dict(tradingsymbol=["RELIANCE"], signal_ts=[enriched.bar_end.iloc[-1]]))
        enriched["adv_feature_ts"] += pd.Timedelta(minutes=5)
        with self.assertRaisesRegex(ValueError, "timestamp"):
            attach_features(raw, enriched)

    def test_empty_inputs_and_missing_columns(self):
        out = calculate_features(minutes(0))
        self.assertEqual(len(out), 0)
        self.assertTrue(set(FEATURE_FIELDS).issubset(out.columns))
        with self.assertRaisesRegex(ValueError, "Missing"):
            calculate_features(minutes().drop(columns="volume"))


if __name__ == "__main__":
    unittest.main()
