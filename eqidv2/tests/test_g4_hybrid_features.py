"""Small arithmetic and no-lookahead checks for G4 OHLCV research features."""
from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from g3_one_minute_extension import last_features
from research.g4_hybrid_features import FEATURE_COLUMNS, build_features


def _frame(count: int = 60) -> pd.DataFrame:
    prices = np.linspace(100.0, 120.0, count)
    return pd.DataFrame({
        "ts": pd.date_range("2026-08-03 09:15", periods=count, freq="min", tz="Asia/Kolkata"),
        "open": prices - 0.2, "high": prices + 1.0, "low": prices - 1.0,
        "close": prices, "volume": np.arange(count, dtype=float) + 10.0,
    })


def _session_frame(count: int = 25) -> pd.DataFrame:
    days = pd.bdate_range("2026-07-01", periods=count, tz="Asia/Kolkata")
    stamps = [day + pd.Timedelta(hours=hour, minutes=minute)
              for day in days for hour, minute in ((9, 15), (9, 16))]
    result = _frame(len(stamps))
    result["ts"] = stamps
    result["volume"] = [value for day in range(count) for value in (10.0 * (day + 1), 100.0 * (day + 1))]
    return result


class G4HybridFeaturesTests(unittest.TestCase):
    def test_ema_atr_match_shared_one_minute_implementation(self):
        raw = _frame()
        actual = build_features(raw)
        for position in (19, 20, 39, 59):
            ema, atr = last_features(raw.iloc[:position], raw.iloc[position].to_dict())
            self.assertAlmostEqual(actual.iloc[position].ema20, ema)
            self.assertAlmostEqual(actual.iloc[position].atr14, atr)
        self.assertTrue(actual.atr14.iloc[:13].isna().all())
        self.assertAlmostEqual(actual.iloc[13].atr14, 2.0)
        self.assertAlmostEqual(actual.iloc[20].roc3_atr, (raw.close.iloc[20] - raw.close.iloc[17]) / 2.0)

    def test_current_and_future_observations_do_not_change_prior_features(self):
        raw = _session_frame()
        original = build_features(raw)
        changed = raw.copy()
        changed.loc[36:, ["open", "high", "low", "close"]] *= 20
        changed.loc[36:, "volume"] *= 100
        revised = build_features(changed)
        pd.testing.assert_frame_equal(original.loc[:35, list(FEATURE_COLUMNS)], revised.loc[:35, list(FEATURE_COLUMNS)])
        prefix = build_features(raw.iloc[:36])
        pd.testing.assert_frame_equal(original.loc[:35, list(FEATURE_COLUMNS)], prefix[list(FEATURE_COLUMNS)])

    def test_session_vwap_uses_incremental_volume_and_resets(self):
        raw = _session_frame(2)
        raw[["open", "high", "low", "close"]] = np.repeat(np.array([100, 110, 200, 220])[:, None], 4, axis=1)
        raw["volume"] = [1, 3, 0, 2]
        actual = build_features(raw)
        np.testing.assert_allclose(actual.session_vwap, [100, 107.5, np.nan, 220], equal_nan=True)

    def test_same_time_relative_volume_uses_prior_sessions_and_twenty_session_window(self):
        raw = _session_frame()
        actual = build_features(raw)
        self.assertTrue(actual.relvol_time.iloc[:10].isna().all())
        self.assertAlmostEqual(actual.relvol_time.iloc[10], 60 / np.mean([10, 20, 30, 40, 50]))
        self.assertAlmostEqual(actual.relvol_time.iloc[11], 600 / np.mean([100, 200, 300, 400, 500]))
        self.assertAlmostEqual(actual.relvol_time.iloc[48], 250 / np.mean(np.arange(5, 25) * 10))
        # Current-day activity cannot alter the same-time baseline used by its own ratio.
        raw.loc[48, "volume"] = 500
        self.assertAlmostEqual(build_features(raw).relvol_time.iloc[48], 2 * actual.relvol_time.iloc[48])

    def test_missing_minutes_do_not_extend_relative_volume_beyond_twenty_sessions(self):
        raw = _session_frame(30)
        # Only five 09:15 samples remain in the preceding 20 sessions of day 30.
        remove = (raw.ts.dt.hour == 9) & (raw.ts.dt.minute == 15) & raw.ts.dt.day.between(15, 31)
        removed = raw.loc[~remove].reset_index(drop=True)
        actual = build_features(removed)
        target = raw.ts.iloc[-2]
        previous_days = sorted(raw.ts.dt.normalize().unique())[-21:-1]
        previous = removed.loc[(removed.ts.dt.normalize().isin(previous_days))
                               & (removed.ts.dt.minute == 15), "volume"]
        expected = raw.volume.iloc[-2] / previous.mean() if len(previous) >= 5 else np.nan
        np.testing.assert_allclose(actual.loc[actual.ts == target, "relvol_time"], [expected], equal_nan=True)

    def test_twenty_bar_relative_volume_excludes_current_bar(self):
        actual = build_features(_frame())
        self.assertTrue(actual.relvol20.iloc[:20].isna().all())
        self.assertAlmostEqual(actual.relvol20.iloc[20], 30 / np.mean(np.arange(10, 30)))

    def test_zero_denominators_remain_missing_and_wick_arithmetic(self):
        raw = _frame(25)
        raw[["open", "high", "low", "close"]] = 100.0
        raw["volume"] = 0.0
        actual = build_features(raw)
        for column in ("session_vwap", "relvol20", "relvol_time", "roc3_atr", "ema_slope3_atr",
                       "upper_wick_ratio", "lower_wick_ratio", "body_ratio"):
            self.assertTrue(actual[column].isna().all(), column)
        raw.loc[24, ["open", "high", "low", "close"]] = [100, 108, 98, 104]
        row = build_features(raw).iloc[-1]
        self.assertAlmostEqual(row.upper_wick_ratio, 0.4)
        self.assertAlmostEqual(row.lower_wick_ratio, 0.2)
        self.assertAlmostEqual(row.body_ratio, 0.4)

    def test_sorting_timezone_extra_columns_and_stored_indicators(self):
        raw = _frame()
        raw["ts"] = raw.ts.dt.tz_convert("UTC")
        raw["ema20"] = -999.0
        raw["atr14"] = -999.0
        raw["gap_filled"] = 0
        original = raw.copy(deep=True)
        actual = build_features(raw.iloc[::-1])
        self.assertTrue(actual.ts.is_monotonic_increasing)
        self.assertEqual(str(actual.ts.dt.tz), "Asia/Kolkata")
        self.assertEqual(actual.ema20.iloc[0], 100)
        self.assertEqual(actual.atr14.iloc[-1], 2)
        self.assertTrue((actual.gap_filled == 0).all())
        pd.testing.assert_frame_equal(raw, original)

    def test_empty_frame_preserves_schema(self):
        actual = build_features(_frame().iloc[:0])
        self.assertTrue(actual.empty)
        self.assertTrue(set(FEATURE_COLUMNS).issubset(actual.columns))

    def test_timestamp_integer_representation_is_always_nanoseconds(self):
        raw = _frame()
        expected = raw.ts.astype("int64").to_numpy()
        raw["ts"] = raw.ts.dt.as_unit("us")
        actual = build_features(raw)
        self.assertEqual(actual.ts.dtype.unit, "ns")
        np.testing.assert_array_equal(actual.ts.astype("int64").to_numpy(), expected)

    def test_invalid_inputs_fail_explicitly(self):
        examples = []
        examples.append(_frame().drop(columns="volume"))
        naive = _frame(); naive["ts"] = naive.ts.dt.tz_localize(None); examples.append(naive)
        missing_ts = _frame(); missing_ts.loc[2, "ts"] = pd.NaT; examples.append(missing_ts)
        duplicate = _frame(); duplicate.loc[2, "ts"] = duplicate.ts.iloc[1]; examples.append(duplicate)
        for column, value in (("close", 0), ("high", np.inf), ("volume", -1), ("volume", np.nan),
                              ("low", 500), ("high", 50)):
            invalid = _frame(); invalid.loc[2, column] = value; examples.append(invalid)
        for invalid in examples:
            with self.subTest(columns=list(invalid.columns), row=invalid.iloc[2].to_dict()):
                with self.assertRaises(ValueError):
                    build_features(invalid)


if __name__ == "__main__":
    unittest.main()
