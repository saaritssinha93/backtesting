from __future__ import annotations

import unittest
from datetime import timedelta, timezone

import pandas as pd

import fno_oi_intraday_strategies as strat


IST_OFFSET = timezone(timedelta(hours=5, minutes=30))


def _bars(prices: list[tuple[float, float, float, float]], *, day: str = "2026-08-10",
          start: str = "09:30", symbol: str = "TEST26AUGFUT") -> pd.DataFrame:
    """prices = [(open, high, low, close), ...] on consecutive 5-min bars."""
    base = pd.Timestamp(f"{day} {start}").tz_localize(IST_OFFSET)
    rows = []
    for i, (o, h, l, c) in enumerate(prices):
        rows.append(
            {
                "timestamp": (base + timedelta(minutes=5 * i)).tz_convert("UTC"),
                "ist": base + timedelta(minutes=5 * i),
                "session_date": pd.Timestamp(day).date(),
                "tradingsymbol": symbol,
                "underlying": "TEST",
                "contract_month": "2026-08",
                "is_front_month": True,
                "open": o, "high": h, "low": l, "close": c,
                "volume": 1000, "oi": 10000,
                "traded_value_5m": 1e9,
                "volume_ratio": 3.0,
                "oi_change_pct_5m": 2.0,
                "oi_change_pct_15m": 2.0,
                "price_change_pct_5m": 0.5,
                "price_change_pct_day": 0.5,
                "classification": "LONG_BUILDUP",
                "eligible_for_rank": True,
                "oi_rank_5m": 1.0,
                "oi_zscore_20": 2.0,
                "activity_score": 90.0,
                "day_volume": 100000,
            }
        )
    return pd.DataFrame(rows)


def _setup(side: str = "SHORT", *, target=0.40, stop=0.30, max_bars=6,
           only_first_bar: bool = True) -> strat.Setup:
    def predicate(d: pd.DataFrame) -> pd.Series:
        if only_first_bar:
            first = d["ist"].min()
            return d["ist"].eq(first)
        return pd.Series(True, index=d.index)

    return strat.Setup(
        name="T", side=side, rationale="test", predicate=predicate,
        target_pct=target, stop_pct=stop, max_bars=max_bars,
        entry_from="0900", entry_to="1530",
    )


class EntryTests(unittest.TestCase):
    def test_entry_is_the_bar_after_the_signal(self):
        # Signal on bar 0 (close 100); bar 1 opens at 105 -> entry must be 105.
        panel = _bars([(100, 100, 100, 100), (105, 105, 105, 105), (105, 105, 105, 105)])
        trades = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades.iloc[0]["entry"], 105.0)

    def test_signal_on_last_bar_of_session_is_dropped(self):
        panel = _bars([(100, 100, 100, 100)])
        trades = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        self.assertTrue(trades.empty)

    def test_illiquid_signal_bar_is_filtered_out(self):
        panel = _bars([(100, 100, 100, 100), (100, 100, 100, 100)])
        panel.loc[0, "traded_value_5m"] = 1e5
        trades = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=1e7)
        self.assertTrue(trades.empty)

    def test_ineligible_row_is_filtered_out(self):
        panel = _bars([(100, 100, 100, 100), (100, 100, 100, 100)])
        panel.loc[0, "eligible_for_rank"] = False
        trades = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        self.assertTrue(trades.empty)


class ExitTests(unittest.TestCase):
    def test_long_target_hit(self):
        # entry 100, target +0.4% = 100.4; bar 1 high 101 triggers it.
        panel = _bars([(100, 100, 100, 100), (100, 101, 99.9, 100.5)])
        trades = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        row = trades.iloc[0]
        self.assertEqual(row["exit_reason"], "TARGET")
        self.assertAlmostEqual(row["gross_ret_pct"], 0.40, places=6)

    def test_long_stop_hit(self):
        panel = _bars([(100, 100, 100, 100), (100, 100.1, 99.0, 99.2)])
        trades = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        row = trades.iloc[0]
        self.assertEqual(row["exit_reason"], "STOP")
        self.assertAlmostEqual(row["gross_ret_pct"], -0.30, places=6)

    def test_short_target_is_a_price_fall(self):
        panel = _bars([(100, 100, 100, 100), (100, 100.1, 99.5, 99.6)])
        trades = strat.simulate(panel, _setup("SHORT"), cost_bps=0.0, min_traded_value=0.0)
        row = trades.iloc[0]
        self.assertEqual(row["exit_reason"], "TARGET")
        self.assertAlmostEqual(row["gross_ret_pct"], 0.40, places=6)

    def test_short_stop_is_a_price_rise(self):
        panel = _bars([(100, 100, 100, 100), (100, 100.5, 99.9, 100.4)])
        trades = strat.simulate(panel, _setup("SHORT"), cost_bps=0.0, min_traded_value=0.0)
        row = trades.iloc[0]
        self.assertEqual(row["exit_reason"], "STOP")
        self.assertAlmostEqual(row["gross_ret_pct"], -0.30, places=6)

    def test_stop_wins_when_a_bar_touches_both(self):
        # Bar spans both target (100.4) and stop (99.7); harness must take the stop.
        panel = _bars([(100, 100, 100, 100), (100, 101.0, 99.0, 100.5)])
        trades = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        self.assertEqual(trades.iloc[0]["exit_reason"], "STOP")
        self.assertLess(trades.iloc[0]["gross_ret_pct"], 0)

    def test_time_exit_uses_last_bar_close(self):
        panel = _bars(
            [(100, 100, 100, 100)] + [(100, 100.05, 99.95, 100.02)] * 3,
        )
        trades = strat.simulate(panel, _setup("LONG", max_bars=2), cost_bps=0.0, min_traded_value=0.0)
        row = trades.iloc[0]
        self.assertEqual(row["exit_reason"], "TIME")
        self.assertEqual(row["bars_held"], 2)

    def test_position_does_not_cross_into_the_next_session(self):
        day1 = _bars([(100, 100, 100, 100), (100, 100.01, 99.99, 100.0)], day="2026-08-10")
        day2 = _bars([(200, 200, 200, 200), (200, 200, 200, 200)], day="2026-08-11")
        panel = pd.concat([day1, day2], ignore_index=True)
        trades = strat.simulate(panel, _setup("LONG", max_bars=10), cost_bps=0.0, min_traded_value=0.0)
        for _, row in trades.iterrows():
            self.assertEqual(
                pd.Timestamp(row["entry_ts"]).date(), pd.Timestamp(row["exit_ts"]).date()
            )
        # The day-1 trade must not capture the 100 -> 200 overnight jump.
        first = trades.iloc[0]
        self.assertLess(abs(first["gross_ret_pct"]), 1.0)


class CostTests(unittest.TestCase):
    def test_cost_is_subtracted_round_trip(self):
        panel = _bars([(100, 100, 100, 100), (100, 101, 99.9, 100.5)])
        free = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        costed = strat.simulate(panel, _setup("LONG"), cost_bps=5.0, min_traded_value=0.0)
        self.assertAlmostEqual(
            free.iloc[0]["net_ret_pct"] - costed.iloc[0]["net_ret_pct"], 0.05, places=6
        )

    def test_gross_is_unaffected_by_cost(self):
        panel = _bars([(100, 100, 100, 100), (100, 101, 99.9, 100.5)])
        free = strat.simulate(panel, _setup("LONG"), cost_bps=0.0, min_traded_value=0.0)
        costed = strat.simulate(panel, _setup("LONG"), cost_bps=9.0, min_traded_value=0.0)
        self.assertAlmostEqual(
            free.iloc[0]["gross_ret_pct"], costed.iloc[0]["gross_ret_pct"], places=9
        )


class SummaryTests(unittest.TestCase):
    def _trades(self, rets: list[float], days: list[str]) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "net_ret_pct": rets,
                "gross_ret_pct": rets,
                "session_date": [pd.Timestamp(d).date() for d in days],
                "bars_held": [3] * len(rets),
                "exit_reason": ["TARGET"] * len(rets),
            }
        )

    def test_profit_factor(self):
        t = self._trades([1.0, 1.0, -1.0], ["2026-08-03"] * 3)
        self.assertAlmostEqual(summ := summarize_pf(t), 2.0, places=6)

    def test_day_concentration_is_detected(self):
        # One huge day plus several small ones -> top-2 share must be high.
        rets = [10.0, 0.1, 0.1, 0.1]
        days = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"]
        s = strat.summarize(self._trades(rets, days), "x")
        self.assertGreater(s["top2_day_share"], 0.9)

    def test_verdict_rejects_day_concentrated_setups(self):
        train = {"trades": 100, "profit_factor": 1.5}
        test = {"trades": 100, "profit_factor": 1.5, "top2_day_share": 0.9, "day_win_rate": 0.8}
        self.assertIn("day-concentrated", strat._verdict(train, test))

    def test_verdict_rejects_small_samples(self):
        train = {"trades": 5, "profit_factor": 3.0}
        test = {"trades": 5, "profit_factor": 3.0, "top2_day_share": 0.1, "day_win_rate": 0.9}
        self.assertIn("sample too small", strat._verdict(train, test))

    def test_verdict_accepts_a_clean_setup(self):
        train = {"trades": 200, "profit_factor": 1.4}
        test = {"trades": 200, "profit_factor": 1.3, "top2_day_share": 0.3, "day_win_rate": 0.6}
        self.assertEqual(strat._verdict(train, test), "CANDIDATE")


def summarize_pf(trades: pd.DataFrame) -> float:
    return strat.summarize(trades, "x")["profit_factor"]


if __name__ == "__main__":
    unittest.main()
