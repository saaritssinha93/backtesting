from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

import fno_oi_ema_confirm_0925_0930_pf_v3 as v3


class FnoOiEmaConfirm09250930PfV3Tests(unittest.TestCase):
    def test_v3_timing_has_exactly_two_confirmation_windows(self) -> None:
        self.assertEqual(v3.SIGNAL_SLOTS, (925, 930))
        self.assertEqual(v3.CONFIRMATION_ENDS, {925: 926, 930: 931})
        self.assertEqual(v3.MAX_PER_SIDE_LIMIT, 5)

    def test_v3_cli_can_limit_search_to_0930_with_two_per_side(self) -> None:
        args = v3.parse_args(["--signal-slot", "930", "--max-per-side", "2"])

        self.assertEqual(args.signal_slot, [930])
        self.assertEqual(args.max_per_side, 2)

    def test_selection_cap_is_applied_per_scan(self) -> None:
        scan_idx = np.array([0, 0, 0, 1, 1, 1], dtype=int)
        eligible = np.ones(scan_idx.size, dtype=bool)
        order = np.array([2, 5, 1, 4, 0, 3], dtype=int)

        selections = v3.select_up_to_per_day(eligible, order, scan_idx, 2)

        self.assertEqual(len(selections), 2)
        for limit, selected in enumerate(selections, start=1):
            counts = np.bincount(scan_idx[selected], minlength=2)
            self.assertEqual(counts.tolist(), [limit, limit])

    def test_daily_curve_aggregates_both_scans_and_labels_entries(self) -> None:
        session = date(2026, 8, 10)
        v3.SLOT_DAYS = np.array([session, session], dtype=object)
        signals = pd.DataFrame(
            {
                "tradingsymbol": ["FIRST", "SECOND"],
                "sid": [1, 2],
                "hhmm_int": [925, 930],
            }
        )
        candidate = v3.Candidate(
            {
                "side": "SHORT",
                "picker": "max_volume",
                "max_per_side": 1,
                "stop_pct": 0.75,
                "target_pct": 2.0,
            },
            np.array([0, 1], dtype=int),
        )

        daily = v3.daily_curve(
            signals,
            np.array([1.95, -0.80]),
            candidate,
            "TEST_MODEL",
            [session],
        )
        row = daily.iloc[0]

        self.assertEqual(int(row["selections"]), 2)
        self.assertEqual(int(row["fills"]), 2)
        self.assertAlmostEqual(float(row["net_return_pct"]), 1.15)
        self.assertIn("[09:26] FIRST=+1.950%", row["trade_details"])
        self.assertIn("[09:31] SECOND=-0.800%", row["trade_details"])


if __name__ == "__main__":
    unittest.main()
