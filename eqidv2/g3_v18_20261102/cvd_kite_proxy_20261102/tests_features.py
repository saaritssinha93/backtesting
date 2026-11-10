"""Causality, clock and allocation checks for the five-minute pressure proxy."""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from g3_v18_20261102.cvd_kite_proxy_20261102.features import (
    FEATURE_FIELDS, IST, attach_features, calculate_features,
)


def bars(n=6, day="2026-10-29", symbol="RELIANCE"):
    return pd.DataFrame(dict(
        symbol=[symbol] * n,
        bar_end=pd.date_range(f"{day} 09:20", periods=n, freq="5min", tz=IST),
        open=[100.0] * n, high=[110.0] * n, low=[90.0] * n,
        close=[105.0] * n, volume=[200.0] * n,
    ))


class TestCvdProxyFeatures(unittest.TestCase):
    def test_allocation_and_session_arithmetic(self):
        out = calculate_features(bars(3))
        np.testing.assert_allclose(out.cvd_proxy_delta, [100, 100, 100])
        np.testing.assert_allclose(out.cvd_proxy_buy_volume_est, [150, 150, 150])
        np.testing.assert_allclose(out.cvd_proxy_sell_volume_est, [50, 50, 50])
        np.testing.assert_allclose(out.cvd_proxy_session, [100, 200, 300])
        np.testing.assert_allclose(out.cvd_proxy_session_ratio, [.5, .5, .5])
        self.assertEqual(out.cvd_proxy_recent15.iloc[-1], 300)
        self.assertEqual(out.cvd_proxy_recent15_ratio.iloc[-1], .5)
        self.assertEqual(out.cvd_proxy_ready.tolist(), [False, False, True])

    def test_extremes_and_neutral_allocation(self):
        raw = bars(3)
        raw["close"] = [110., 90., 100.]
        out = calculate_features(raw)
        np.testing.assert_allclose(out.cvd_proxy_delta, [200, -200, 0])
        np.testing.assert_allclose(out.cvd_proxy_buy_volume_est + out.cvd_proxy_sell_volume_est, raw.volume)
        self.assertTrue((out.cvd_proxy_buy_volume_est >= 0).all())
        self.assertTrue((out.cvd_proxy_sell_volume_est >= 0).all())

    def test_body_sensitivity_has_independent_formula(self):
        raw = bars(3)
        raw["open"] = [109., 90., 105.]
        out = calculate_features(raw)
        np.testing.assert_allclose(out.cvd_proxy_body_delta, [-40., 150., 0.])
        self.assertEqual(out.cvd_proxy_body_session.iloc[-1], 110.)
        self.assertEqual(out.cvd_proxy_body_recent15.iloc[-1], 110.)
        self.assertEqual(out.cvd_proxy_session.iloc[-1], 300.)

    def test_flat_candle_is_neutral(self):
        raw = bars(3)
        raw[["open", "high", "low", "close"]] = 100.
        out = calculate_features(raw)
        self.assertTrue(out.cvd_proxy_delta.eq(0).all())
        self.assertTrue(out.cvd_proxy_body_delta.eq(0).all())
        self.assertTrue(out.cvd_proxy_buy_volume_est.eq(100).all())
        self.assertTrue(out.cvd_proxy_sell_volume_est.eq(100).all())
        self.assertTrue(out.cvd_proxy_ready.iloc[-1])

    def test_sessions_reset_without_overnight_window(self):
        first = bars(3)
        second = bars(3, "2026-10-30")
        second["close"] = 90.
        out = calculate_features(pd.concat([first, second]))
        np.testing.assert_allclose(out.cvd_proxy_session, [100, 200, 300, -200, -400, -600])
        self.assertTrue(out.cvd_proxy_recent15.iloc[3:5].isna().all())
        self.assertEqual(out.cvd_proxy_ready.tolist(), [False, False, True, False, False, True])

    def test_stock_groups_are_independent(self):
        second = bars(3, symbol="TCS")
        second["close"] = 90.
        out = calculate_features(pd.concat([second, bars(3)]).sample(frac=1, random_state=7))
        self.assertEqual(out.loc[out.symbol.eq("RELIANCE"), "cvd_proxy_session"].iloc[-1], 300)
        self.assertEqual(out.loc[out.symbol.eq("TCS"), "cvd_proxy_session"].iloc[-1], -600)

    def test_future_changes_do_not_modify_earlier_features(self):
        raw = bars(8)
        prefix = calculate_features(raw.iloc[:4])
        future_changed = raw.copy()
        future_changed.loc[4:, "close"] = 90.
        future_changed.loc[4:, "volume"] = 1000000.
        full = calculate_features(future_changed)
        assert_frame_equal(prefix, full.iloc[:4].reset_index(drop=True))

    def test_missing_bar_poison_session_recent_window_recovers(self):
        raw = bars(8).drop(index=3)
        out = calculate_features(raw)
        self.assertTrue(out.cvd_proxy_session.iloc[3:].isna().all())
        self.assertEqual(out.cvd_proxy_recent15_ready.tolist(), [False, False, True, False, False, True, True])
        self.assertFalse(out.cvd_proxy_ready.iloc[3:].any())

    def test_missing_opening_bar_never_becomes_session_ready(self):
        out = calculate_features(bars(6).iloc[1:])
        self.assertFalse(out.cvd_proxy_prefix_valid.any())
        self.assertFalse(out.cvd_proxy_session_ready.any())
        self.assertTrue(out.cvd_proxy_recent15_ready.iloc[-1])

    def test_invalid_ohlc_fails_closed_and_preserves_row(self):
        for bad in [np.nan, -1., 120.]:
            with self.subTest(close=bad):
                raw = bars(7)
                raw.loc[2, "close"] = bad
                out = calculate_features(raw)
                self.assertEqual(len(out), len(raw))
                self.assertEqual(out.cvd_proxy_reason.iloc[2], "INVALID_OHLC")
                self.assertTrue(out.cvd_proxy_session.iloc[2:].isna().all())
                self.assertFalse(out.cvd_proxy_ready.iloc[2:].any())
                self.assertTrue(out.cvd_proxy_recent15_ready.iloc[-1])

    def test_invalid_volume_fails_closed(self):
        for bad in [np.nan, -1., np.inf, "bad"]:
            with self.subTest(volume=bad):
                raw = bars(5)
                raw["volume"] = raw.volume.astype(object)
                raw.loc[2, "volume"] = bad
                with np.errstate(invalid="ignore"):
                    out = calculate_features(raw)
                self.assertEqual(out.cvd_proxy_reason.iloc[2], "INVALID_VOLUME")
                self.assertFalse(out.cvd_proxy_ready.iloc[2:].any())

    def test_zero_volume_is_no_current_evidence_and_no_gap(self):
        raw = bars(5)
        raw.loc[2, "volume"] = 0.
        out = calculate_features(raw)
        self.assertFalse(out.cvd_proxy_ready.iloc[2])
        self.assertEqual(out.cvd_proxy_reason.iloc[2], "ZERO_CURRENT_VOLUME")
        self.assertEqual(out.cvd_proxy_delta.iloc[2], 0.)
        self.assertTrue(out.cvd_proxy_prefix_valid.all())
        self.assertTrue(out.cvd_proxy_ready.iloc[3])
        raw["volume"] = 0.
        zero = calculate_features(raw)
        self.assertFalse(zero.cvd_proxy_ready.any())
        self.assertTrue(zero.cvd_proxy_session_ratio.isna().all())

    def test_flagged_and_partial_bars_disable_session(self):
        for field, value in [("is_synthetic", True), ("gap_filled", "true"), ("source_1m_count", 4)]:
            with self.subTest(field=field):
                raw = bars(5)
                raw[field] = pd.Series([5 if field == "source_1m_count" else False] * len(raw), dtype=object)
                raw.loc[1, field] = value
                out = calculate_features(raw)
                self.assertEqual(out.cvd_proxy_reason.iloc[1], "FLAGGED_OR_INCOMPLETE_SOURCE_BAR")
                self.assertFalse(out.cvd_proxy_ready.any())

    def test_naive_timestamp_rejected(self):
        raw = bars(3)
        raw["bar_end"] = raw.bar_end.dt.tz_localize(None)
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            calculate_features(raw)

    def test_utc_input_matches_ist_instant(self):
        raw = bars(3)
        expected = calculate_features(raw)
        raw["bar_end"] = raw.bar_end.dt.tz_convert("UTC")
        assert_frame_equal(expected, calculate_features(raw))

    def test_off_grid_bar_disables_prefix(self):
        raw = bars(5)
        raw.loc[2, "bar_end"] += pd.Timedelta(seconds=1)
        out = calculate_features(raw)
        self.assertEqual(out.cvd_proxy_reason.iloc[2], "OUTSIDE_REGULAR_5M_GRID")
        self.assertFalse(out.cvd_proxy_ready.any())

    def test_duplicate_identity_raises(self):
        raw = bars(3)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            calculate_features(pd.concat([raw, raw.iloc[[0]]]))

    def test_completion_cutoff(self):
        raw = bars(3)
        calculate_features(raw, as_of=raw.bar_end.iloc[-1])
        with self.assertRaisesRegex(ValueError, "not yet complete"):
            calculate_features(raw, as_of=raw.bar_end.iloc[-1] - pd.Timedelta(seconds=1))
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            calculate_features(raw, as_of="2026-10-29 09:30")

    def test_empty_frame_has_stable_output_columns(self):
        out = calculate_features(bars(0))
        self.assertTrue(set(FEATURE_FIELDS).issubset(out.columns))
        self.assertEqual(len(out), 0)

    def test_missing_column_or_symbol_raises(self):
        with self.assertRaisesRegex(ValueError, "Missing"):
            calculate_features(bars().drop(columns="volume"))
        raw = bars()
        raw.loc[0, "symbol"] = ""
        with self.assertRaisesRegex(ValueError, "symbol"):
            calculate_features(raw)

    def test_exact_attach_preserves_rows_and_duplicate_candidates(self):
        enriched = calculate_features(bars(3))
        raw = pd.DataFrame({"tradingsymbol": ["RELIANCE", "RELIANCE", "TCS", "RELIANCE"],
            "signal_ts": [enriched.bar_end.iloc[2], enriched.bar_end.iloc[0], enriched.bar_end.iloc[2], enriched.bar_end.iloc[2]],
            "sid": [9, 2, 8, 7]}, index=[10, 4, 100, 1])
        attached = attach_features(raw, enriched)
        self.assertEqual(attached.index.tolist(), raw.index.tolist())
        self.assertEqual(attached.sid.tolist(), raw.sid.tolist())
        self.assertEqual(attached.cvd_proxy_ready.tolist(), [True, False, False, True])
        self.assertEqual(attached.cvd_proxy_reason.iloc[2], "MISSING_EXACT_BAR")
        self.assertTrue(pd.isna(attached.cvd_proxy_session.iloc[2]))

    def test_attach_does_not_look_ahead_or_asof_join(self):
        enriched = calculate_features(bars(3))
        raw = pd.DataFrame({"tradingsymbol": ["RELIANCE", "RELIANCE"],
            "signal_ts": [enriched.bar_end.iloc[2] - pd.Timedelta(seconds=1), enriched.bar_end.iloc[2] + pd.Timedelta(minutes=5)]})
        attached = attach_features(raw, enriched)
        self.assertFalse(attached.cvd_proxy_ready.any())
        self.assertTrue(attached.cvd_proxy_session.isna().all())

    def test_attach_rejects_future_feature_stamp(self):
        enriched = calculate_features(bars(3))
        raw = pd.DataFrame({"tradingsymbol": ["RELIANCE"], "signal_ts": [enriched.bar_end.iloc[2]]})
        enriched["cvd_proxy_feature_ts"] += pd.Timedelta(minutes=5)
        with self.assertRaisesRegex(ValueError, "timestamp"):
            attach_features(raw, enriched)


if __name__ == "__main__":
    unittest.main()
