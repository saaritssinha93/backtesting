from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from research_v12_hourly_prefilter_backtest import (
    filter_candidates_for_hourly_pools,
    load_hourly_pools,
    ticker_union_by_date,
)


class MultiDayHourlyV12PrefilterAdapterTests(unittest.TestCase):
    def _candidate_file(self, root: Path) -> Path:
        rows = []
        for date_text, tickers in (
            ("2026-06-03", ("OLD1", "OLD2")),
            ("2026-06-04", ("A", "B")),
            ("2026-06-05", ("C", "D")),
        ):
            for hour in range(9, 16):
                slot = pd.Timestamp(f"{date_text} {hour:02d}:20", tz="Asia/Kolkata")
                for rank, ticker in enumerate(tickers, 1):
                    rows.append(
                        {
                            "slot_ist": slot.isoformat(),
                            "ticker": ticker,
                            "selection_rank": rank,
                            "selection_bucket": (
                                "WEIGHTED_STREAM:LONG" if rank == 1 else "WEIGHTED_STREAM:SHORT"
                            ),
                            "primary_side": "LONG" if rank == 1 else "SHORT",
                            "primary_family": "MOMENTUM",
                        }
                    )
        path = root / "hourly_candidates.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def test_range_loader_filters_and_validates_every_day(self):
        with tempfile.TemporaryDirectory() as tmp:
            pools = load_hourly_pools(
                self._candidate_file(Path(tmp)),
                "2026-06-04",
                end_date_text="2026-06-05",
                expected_budget=2,
            )
            self.assertEqual(len(pools), 14)
            self.assertEqual(pools[0].slot_ist.isoformat(), "2026-06-04T09:20:00+05:30")
            self.assertEqual(pools[-1].slot_ist.isoformat(), "2026-06-05T15:20:00+05:30")

    def test_activation_never_carries_across_dates(self):
        with tempfile.TemporaryDirectory() as tmp:
            pools = load_hourly_pools(
                self._candidate_file(Path(tmp)),
                "2026-06-04",
                end_date_text="2026-06-05",
                expected_budget=2,
            )
            candidates = pd.DataFrame(
                {
                    "ticker": ["A", "A", "A", "C", "C"],
                    "side": ["LONG"] * 5,
                    "signal_time_ist": [
                        "2026-06-04 09:20:00+05:30",
                        "2026-06-04 09:25:00+05:30",
                        "2026-06-04 10:20:00+05:30",
                        "2026-06-05 09:20:00+05:30",
                        "2026-06-05 09:25:00+05:30",
                    ],
                }
            )
            kept, _ = filter_candidates_for_hourly_pools(candidates, pools)
            self.assertEqual(
                kept["signal_time_ist"].tolist(),
                [
                    "2026-06-04 09:25:00+05:30",
                    "2026-06-04 10:20:00+05:30",
                    "2026-06-05 09:25:00+05:30",
                ],
            )

    def test_daily_union_contains_only_that_days_pool_members(self):
        with tempfile.TemporaryDirectory() as tmp:
            pools = load_hourly_pools(
                self._candidate_file(Path(tmp)),
                "2026-06-04",
                end_date_text="2026-06-05",
                expected_budget=2,
            )
            union = ticker_union_by_date(pools)
            self.assertEqual(union["2026-06-04"], frozenset({"A", "B"}))
            self.assertEqual(union["2026-06-05"], frozenset({"C", "D"}))


if __name__ == "__main__":
    unittest.main()
