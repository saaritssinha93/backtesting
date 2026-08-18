from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

import fno_oi_ema_confirm_0925_0930_0935_v4 as v4


class FnoOiEmaConfirm092509300935V4Tests(unittest.TestCase):
    def test_v4_timing_and_caps(self) -> None:
        self.assertEqual(v4.V4_SIGNAL_SLOT, 935)
        self.assertEqual(v4.V4_CONFIRMATION_END, 936)
        self.assertEqual(v4.LONG_MAX, 1)
        self.assertEqual(v4.SHORT_MAX, 2)

    def test_v4_cli_uses_existing_guard_defaults(self) -> None:
        args = v4.parse_args([])

        self.assertEqual(args.min_trades, 8)
        self.assertAlmostEqual(args.min_day_win, 0.40)
        self.assertAlmostEqual(args.max_top_profit_share, 0.45)

    def test_v4_curve_adds_new_leg_and_cleans_empty_details(self) -> None:
        session = date(2026, 8, 10)
        locked = pd.DataFrame(
            {
                "day": [session],
                "baseline_long_status": ["WIN"],
                "baseline_long_selections": [1],
                "baseline_long_fills": [1],
                "baseline_long_trade_details": ["[09:26] BASE=+1.000%"],
                "baseline_short_status": ["NO_SIGNAL"],
                "baseline_short_selections": [0],
                "baseline_short_fills": [0],
                "baseline_short_trade_details": [float("nan")],
                "addon_long_status": ["NO_SIGNAL"],
                "addon_long_selections": [0],
                "addon_long_fills": [0],
                "addon_long_trade_details": [float("nan")],
                "addon_short_status": ["NO_SIGNAL"],
                "addon_short_selections": [0],
                "addon_short_fills": [0],
                "addon_short_trade_details": [float("nan")],
                "long_selections": [1],
                "long_fills": [1],
                "long_return_pct": [1.0],
                "long_gross_profit_pct": [1.0],
                "long_gross_loss_pct": [0.0],
                "short_selections": [0],
                "short_fills": [0],
                "short_return_pct": [0.0],
                "short_gross_profit_pct": [0.0],
                "short_gross_loss_pct": [0.0],
                "selections": [1],
                "fills": [1],
                "portfolio_net_return_pct": [1.0],
            }
        )
        empty = v4.empty_leg([session])
        short = v4.empty_leg([session])
        short.loc[0, "selected_symbol"] = "NEW"
        short.loc[0, "trade_details"] = "[09:36] NEW=+0.700%"
        short.loc[0, "status"] = "WIN"
        short.loc[0, "net_return_pct"] = 0.7
        short.loc[0, "selections"] = 1
        short.loc[0, "fills"] = 1
        short.loc[0, "gross_profit_pct"] = 0.7

        curve = v4.build_v4_curve(locked, empty, short, "TEST")
        row = curve.iloc[0]

        self.assertEqual(row["baseline_short_trade_details"], "")
        self.assertEqual(row["addon_long_trade_details"], "")
        self.assertEqual(int(row["selections"]), 2)
        self.assertEqual(int(row["fills"]), 2)
        self.assertAlmostEqual(float(row["portfolio_net_return_pct"]), 1.7)
        self.assertAlmostEqual(float(row["cumulative_net_pct"]), 1.7)
        self.assertEqual(row["confirmation_end"], "09:26,09:31,09:36")


if __name__ == "__main__":
    unittest.main()
