from __future__ import annotations

import unittest

import pandas as pd

import v17D_exit_resolver as base
import v11_exit_policy_resolver as research


def bars(rows):
    index = pd.date_range("2026-07-21 10:00", periods=len(rows), freq="min", tz="Asia/Kolkata")
    return pd.DataFrame(rows, index=index)


class V11ExitPolicyResolverTests(unittest.TestCase):
    def test_no_policy_matches_shared_resolver(self):
        data = bars([
            {"high": 100.2, "low": 99.8, "close": 100.1},
            {"high": 102.0, "low": 99.7, "close": 101.0},
        ])
        expected = base.resolve(data, "LONG", 100.0, data.index[0], 1.0, 2.0)
        actual = research.resolve(data, "LONG", 100.0, data.index[0], 1.0, 2.0)
        self.assertEqual(actual, expected)

    def test_time_exit_uses_close_at_limit(self):
        data = bars([
            {"high": 100.2, "low": 99.8, "close": 100.0},
            {"high": 100.3, "low": 99.8, "close": 100.1},
            {"high": 100.4, "low": 99.8, "close": 100.25},
        ])
        result = research.resolve(
            data, "LONG", 100.0, data.index[0], 1.0, 3.0,
            {"max_hold_minutes": 2},
        )
        self.assertEqual(result.outcome, "TIME")
        self.assertEqual(result.exit_time_ist, data.index[2])
        self.assertAlmostEqual(result.exit_price, 100.25)

    def test_breakeven_arms_for_following_bar(self):
        data = bars([
            {"high": 101.1, "low": 99.9, "close": 100.8},
            {"high": 100.9, "low": 99.9, "close": 100.0},
        ])
        result = research.resolve(
            data, "LONG", 100.0, data.index[0], 1.0, 3.0,
            {"breakeven_trigger_r": 1.0},
        )
        self.assertEqual(result.outcome, "BREAKEVEN")
        self.assertAlmostEqual(result.exit_price, 100.0)

    def test_trailing_stop_arms_for_following_bar(self):
        data = bars([
            {"high": 101.2, "low": 99.9, "close": 101.0},
            {"high": 101.1, "low": 100.6, "close": 100.8},
        ])
        result = research.resolve(
            data, "LONG", 100.0, data.index[0], 1.0, 3.0,
            {"trailing_trigger_r": 1.0, "trailing_distance_r": 0.5},
        )
        self.assertEqual(result.outcome, "TRAIL")
        self.assertAlmostEqual(result.exit_price, 100.7)

    def test_short_breakeven_is_symmetric(self):
        data = bars([
            {"high": 100.1, "low": 98.9, "close": 99.1},
            {"high": 100.1, "low": 99.0, "close": 100.0},
        ])
        result = research.resolve(
            data, "SHORT", 100.0, data.index[0], 1.0, 3.0,
            {"breakeven_trigger_r": 1.0},
        )
        self.assertEqual(result.outcome, "BREAKEVEN")
        self.assertAlmostEqual(result.exit_price, 100.0)


if __name__ == "__main__":
    unittest.main()
