from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from . import research as r


class ResearchCausalityAndExitTests(unittest.TestCase):
    def test_future_mutation_does_not_change_prior_features(self) -> None:
        dates = pd.date_range("2026-07-01 09:20", periods=55, freq="5min")
        close = np.linspace(100.0, 105.4, len(dates))
        raw = pd.DataFrame({
            "date": dates,
            "open": close - .05,
            "high": close + .20,
            "low": close - .20,
            "close": close,
            "volume": np.linspace(10_000, 20_000, len(dates)),
            "gap_filled": False,
            "opening_snapshot": False,
        })
        before = r.add_features(raw)
        changed = raw.copy()
        changed.loc[50:, ["open", "high", "low", "close", "volume"]] *= 5
        after = r.add_features(changed)
        cols = ["atr", "rsi", "adx", "ema9", "ema20", "avwap", "prev_high10"]
        pd.testing.assert_frame_equal(
            before.loc[:45, cols], after.loc[:45, cols], check_exact=False, rtol=1e-12
        )

    @staticmethod
    def _entry_frame() -> pd.DataFrame:
        return pd.DataFrame([{
            "signal_id": "T|2026-07-01 14:00|compression|bb10",
            "ticker": "T", "session": pd.Timestamp("2026-07-01"),
            "strategy": "compression", "variant": "bb10",
            "date": pd.Timestamp("2026-07-01 14:00"),
            "entry_time": pd.Timestamp("2026-07-01 14:01"),
            "planned_trigger": 100.0, "entry_price": 100.0, "qty": 1000,
            "market_regime": "neutral", "adx": 20.0, "rsi": 60.0,
            "stoch_k": 70.0, "stoch_d": 60.0, "avwap_ext": .2,
            "rel_volume": 1.5, "atr_pct": .3, "obv_up5": True,
            "range_atr": 1.0, "upper_wick_frac": .1, "cancel_level": 99.0,
        }])

    def test_same_minute_target_stop_tie_is_stop(self) -> None:
        paths = {
            "open": np.array([[100.0, np.nan]], np.float32),
            "high": np.array([[101.0, np.nan]], np.float32),
            "low": np.array([[99.0, np.nan]], np.float32),
            "close": np.array([[100.0, np.nan]], np.float32),
            "offset": np.array([[0, -1]], np.int16),
        }
        trade = r.simulate(
            self._entry_frame(), paths, np.array([0]), r.ExitConfig(.75, .70, 60)
        ).iloc[0]
        self.assertEqual(trade["exit_reason"], "STOP")
        self.assertLess(trade["exit_price"], 99.30)

    def test_target_before_later_stop_is_target(self) -> None:
        paths = {
            "open": np.array([[100.0, 100.7]], np.float32),
            "high": np.array([[100.8, 100.8]], np.float32),
            "low": np.array([[99.8, 99.0]], np.float32),
            "close": np.array([[100.7, 99.5]], np.float32),
            "offset": np.array([[0, 1]], np.int16),
        }
        trade = r.simulate(
            self._entry_frame(), paths, np.array([0]), r.ExitConfig(.75, .70, 60)
        ).iloc[0]
        self.assertEqual(trade["exit_reason"], "TARGET")
        self.assertAlmostEqual(trade["exit_price"], 100.75, places=6)


if __name__ == "__main__":
    unittest.main()
