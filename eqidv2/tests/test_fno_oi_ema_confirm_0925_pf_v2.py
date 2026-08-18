from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

import fno_oi_ema_confirm_0925_pf_v2 as v2


class FnoOiEmaConfirm0925PfV2Tests(unittest.TestCase):
    def test_top_n_selection_is_capped_per_side_and_day(self) -> None:
        day_idx = np.array([0] * 7 + [1] * 6, dtype=int)
        eligible = np.ones(day_idx.size, dtype=bool)
        order = np.array([6, 12, 5, 11, 4, 10, 3, 9, 2, 8, 1, 7, 0])

        selections = v2.select_up_to_per_day(eligible, order, day_idx, 5)

        self.assertEqual(len(selections), 5)
        for limit, selected in enumerate(selections, start=1):
            counts = np.bincount(day_idx[selected], minlength=2)
            self.assertTrue((counts <= limit).all())
            self.assertEqual(counts.tolist(), [limit, limit])

    def test_daily_curve_accounts_for_every_selected_contract(self) -> None:
        session = date(2026, 8, 10)
        v2.SLOT_DAYS = np.array([session, session, session], dtype=object)
        signals = pd.DataFrame(
            {
                "tradingsymbol": ["LONG_A", "LONG_B", "LONG_C"],
                "sid": [1, 2, 3],
            }
        )
        candidate = v2.Candidate(
            {
                "side": "LONG",
                "picker": "max_oi",
                "max_per_side": 3,
                "stop_pct": 0.5,
                "target_pct": 2.0,
            },
            np.array([0, 1, 2], dtype=int),
        )

        daily = v2.daily_curve(
            signals,
            np.array([1.0, -0.5, np.nan]),
            candidate,
            "TEST_MODEL",
            [session],
        )
        row = daily.iloc[0]

        self.assertEqual(int(row["selections"]), 3)
        self.assertEqual(int(row["fills"]), 2)
        self.assertEqual(int(row["no_fills"]), 1)
        self.assertAlmostEqual(float(row["net_return_pct"]), 0.5)
        self.assertAlmostEqual(float(row["day_pf"]), 2.0)
        self.assertIn("LONG_A=+1.000%", row["trade_details"])
        self.assertIn("LONG_B=-0.500%", row["trade_details"])
        self.assertIn("LONG_C=NO_FILL", row["trade_details"])

    def test_v2_timing_is_fixed_to_single_confirmation_window(self) -> None:
        self.assertEqual(v2.SIGNAL_SLOT, 925)
        self.assertEqual(v2.CONFIRMATION_END, 926)
        self.assertEqual(v2.MAX_PER_SIDE_LIMIT, 5)


if __name__ == "__main__":
    unittest.main()
