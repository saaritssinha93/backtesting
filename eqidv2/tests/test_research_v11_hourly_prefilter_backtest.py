from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from research_v11_hourly_prefilter_backtest import (
    filter_candidates_for_hourly_pools,
    load_hourly_pools,
)


class HourlyV11PrefilterAdapterTests(unittest.TestCase):
    def _candidate_file(self, root: Path) -> Path:
        rows = []
        for hour in range(9, 16):
            slot = pd.Timestamp(f"2026-08-03 {hour:02d}:20", tz="Asia/Kolkata")
            for rank, ticker in enumerate((f"A{hour}", f"B{hour}"), 1):
                rows.append(
                    {
                        "slot_ist": slot.isoformat(),
                        "ticker": ticker,
                        "selection_rank": rank,
                        "selection_bucket": "WEIGHTED_STREAM:LONG" if rank == 1 else "WEIGHTED_STREAM:SHORT",
                        "primary_side": "LONG" if rank == 1 else "SHORT",
                    }
                )
        path = root / "hourly_candidates.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def test_membership_changes_exactly_on_hourly_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            pools = load_hourly_pools(self._candidate_file(Path(tmp)), "2026-08-03", expected_budget=2)
            candidates = pd.DataFrame(
                {
                    "ticker": ["A9", "A9", "A9", "A9", "A10", "A10", "A10"],
                    "side": ["SHORT", "SHORT", "LONG", "LONG", "SHORT", "SHORT", "LONG"],
                    "scan_slot_ist": [
                        "2026-08-03 09:15:00+05:30",
                        "2026-08-03 09:20:00+05:30",
                        "2026-08-03 09:25:00+05:30",
                        "2026-08-03 10:20:00+05:30",
                        "2026-08-03 10:20:00+05:30",
                        "2026-08-03 10:25:00+05:30",
                        "2026-08-03 11:15:00+05:30",
                    ],
                }
            )
            filtered, stats = filter_candidates_for_hourly_pools(candidates, pools)
            self.assertEqual(filtered["ticker"].tolist(), ["A9", "A9", "A10", "A10"])
            self.assertEqual(filtered["side"].tolist(), ["LONG", "LONG", "SHORT", "LONG"])
            self.assertEqual(stats["rejected_rows"], 3)
            self.assertEqual(
                filtered["prefilter_slot_ist"].tolist(),
                [
                    "2026-08-03T09:20:00+05:30",
                    "2026-08-03T09:20:00+05:30",
                    "2026-08-03T10:20:00+05:30",
                    "2026-08-03T10:20:00+05:30",
                ],
            )

    def test_wrong_schedule_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._candidate_file(Path(tmp))
            frame = pd.read_csv(path)
            frame = frame.loc[~frame["slot_ist"].str.contains("15:20")]
            frame.to_csv(path, index=False)
            with self.assertRaisesRegex(ValueError, "schedule mismatch"):
                load_hourly_pools(path, "2026-08-03", expected_budget=2)


if __name__ == "__main__":
    unittest.main()
