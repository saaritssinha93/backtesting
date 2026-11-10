"""Feature chronology, missing-data and arithmetic checks; run with python -B."""
import unittest

import numpy as np
import pandas as pd

from g3_v18_20261102.features import add_time_of_day_rvol, enrich_day


def fixtures(bars=8):
    times = pd.date_range("2026-10-29 09:20", periods=bars, freq="5min", tz="Asia/Kolkata")
    rows = []
    for symbol, direction in (("AAA", 1), ("BBB", -1)):
        for j, stamp in enumerate(times):
            opening = 100. + direction * j
            close = opening + direction
            rows.append(dict(tradingsymbol=symbol, signal_ts=stamp, open=opening,
                             high=max(opening, close)+.5, low=min(opening, close)-.5,
                             close=close, volume=100*(j+1), source_1m_count=5))
    raw = pd.DataFrame(rows)
    nifty = pd.DataFrame(dict(date=times, open=1000., high=1002., low=999., close=1001.))
    return raw, nifty


class FeatureTests(unittest.TestCase):
    def test_future_perturbation_never_changes_earlier_features(self):
        raw, nifty = fixtures()
        cutoff = pd.Timestamp("2026-10-29 09:35", tz="Asia/Kolkata")
        original = enrich_day(raw, nifty, 2)
        changed_raw, changed_nifty = raw.copy(), nifty.copy()
        future = changed_raw.signal_ts.gt(cutoff)
        for col in ("open", "high", "low", "close", "volume"):
            changed_raw.loc[future, col] *= 12
        for col in ("open", "high", "low", "close"):
            changed_nifty.loc[changed_nifty.date.gt(cutoff), col] *= 9
        changed = enrich_day(changed_raw, changed_nifty, 2)
        pd.testing.assert_frame_equal(original.loc[raw.signal_ts.le(cutoff)], changed.loc[raw.signal_ts.le(cutoff)])

    def test_missing_stock_bar_disables_later_prefix(self):
        raw, nifty = fixtures()
        raw = raw.drop(index=1)
        result = enrich_day(raw, nifty, 2)
        later = result.tradingsymbol.eq("AAA") & result.signal_ts.gt(pd.Timestamp("2026-10-29 09:20", tz="Asia/Kolkata"))
        self.assertFalse(result.loc[later, "v18_stock_ready"].any())
        self.assertTrue(result.loc[later, "v18_vwap"].isna().all())
        self.assertFalse(result.loc[later, "v18_breadth_ready"].any())
        self.assertTrue(result.loc[result.tradingsymbol.eq("BBB"), "v18_stock_ready"].all())

    def test_missing_nifty_bar_is_not_filled(self):
        raw, nifty = fixtures()
        result = enrich_day(raw, nifty.drop(index=1), 2)
        later = result.signal_ts.ge(pd.Timestamp("2026-10-29 09:25", tz="Asia/Kolkata"))
        self.assertFalse(result.loc[later, "v18_market_ready"].any())
        self.assertTrue(result.loc[later, "v18_relative_strength_pct"].isna().all())

    def test_breadth_exact_clock_and_arithmetic(self):
        raw, nifty = fixtures()
        result = enrich_day(raw, nifty, 2)
        self.assertTrue(result.v18_breadth_above_open_pct.eq(50.).all())
        self.assertTrue(result.v18_breadth_below_open_pct.eq(50.).all())
        self.assertTrue(result.v18_breadth_coverage_pct.eq(100.).all())
        first = result.iloc[0]
        self.assertAlmostEqual(first.v18_vwap, (101.5+99.5+101.)/3)
        self.assertAlmostEqual(first.v18_relative_strength_pct, .9)
        self.assertTrue(np.isnan(first.v18_vwap_slope_pct))
        self.assertTrue(result.v18_efficiency_6.dropna().between(0., 1.+1e-12).all())
        self.assertTrue(result.v18_pressure_6.between(-1., 1.).all())

    def test_flat_stock_is_neither_advance_nor_decline(self):
        raw, nifty = fixtures()
        raw.loc[raw.tradingsymbol.eq("BBB"), ["open", "high", "low", "close"]] = [100., 101., 99., 100.]
        result = enrich_day(raw, nifty, 2)
        self.assertTrue(result.v18_breadth_above_open_pct.eq(50.).all())
        self.assertTrue(result.v18_breadth_below_open_pct.eq(0.).all())

    def test_index_and_input_are_preserved(self):
        raw, nifty = fixtures()
        raw = raw.sample(frac=1, random_state=3)
        original = raw.copy(deep=True)
        result = enrich_day(raw, nifty, 2)
        pd.testing.assert_frame_equal(raw, original)
        pd.testing.assert_frame_equal(result[raw.columns], original)
        self.assertTrue(result.v18_stock_ready.all())

    def test_synthetic_prefix_and_duplicate_spot_fail_closed(self):
        raw, nifty = fixtures()
        raw["is_synthetic"] = False
        raw.loc[2, "is_synthetic"] = True
        result = enrich_day(raw, pd.concat((nifty, nifty.iloc[[1]])), 2)
        self.assertFalse(result.loc[2:7, "v18_stock_ready"].any())
        self.assertFalse(result.loc[result.signal_ts.ge(nifty.date.iloc[1]), "v18_market_ready"].any())

    def test_time_of_day_volume_is_shifted_by_day(self):
        rows = pd.DataFrame(dict(tradingsymbol=["AAA"]*7,
                                 signal_ts=pd.date_range("2026-10-01 09:20", periods=7, freq="D", tz="Asia/Kolkata"),
                                 volume=[100., 100., 100., 100., 100., 200., 9000.]))
        result = add_time_of_day_rvol(rows)
        self.assertTrue(result.v18_tod_rvol.iloc[:5].isna().all())
        self.assertAlmostEqual(result.v18_tod_rvol.iloc[5], 2.)
        changed = rows.copy()
        changed.loc[6, "volume"] = 1e8
        pd.testing.assert_frame_equal(result.iloc[:6], add_time_of_day_rvol(changed).iloc[:6])


if __name__ == "__main__":
    unittest.main()
