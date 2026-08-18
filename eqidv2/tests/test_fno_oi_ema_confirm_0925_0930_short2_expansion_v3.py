from __future__ import annotations

import unittest

import numpy as np

import fno_oi_ema_confirm_0925_0930_pf_v3 as v3
import fno_oi_ema_confirm_0925_0930_short2_expansion_v3 as short2


class FnoOiEmaConfirm09250930Short2ExpansionV3Tests(unittest.TestCase):
    def test_cli_defaults_to_exact_two_short_experiment_guards(self) -> None:
        args = short2.parse_args([])

        self.assertEqual(short2.LONG_MAX, 1)
        self.assertEqual(short2.SHORT_MAX, 2)
        self.assertEqual(args.min_trades, 8)
        self.assertAlmostEqual(args.min_day_win, 0.40)

    def test_portfolio_values_add_second_short_to_fixed_curve(self) -> None:
        v3.SLOT_DAY_IDX = np.array([0, 0, 1], dtype=int)
        fixed = short2.FixedContext(
            day_net=np.array([1.0, -0.5]),
            trade_profit=2.0,
            trade_loss=1.5,
            net_pct=0.5,
            orders=4,
            fills=4,
        )
        values = {
            "all_orders": 2,
            "all_trades": 2,
            "all_gross_profit_pct": 1.5,
            "all_gross_loss_pct": 0.5,
            "all_net_pct": 1.0,
        }

        short2.add_portfolio_values(
            values,
            np.array([1, 2], dtype=int),
            np.array([99.0, 1.5, -0.5]),
            fixed,
        )

        self.assertEqual(values["portfolio_all_orders"], 6)
        self.assertEqual(values["portfolio_all_fills"], 6)
        self.assertAlmostEqual(values["portfolio_all_trade_pf"], 1.75)
        self.assertAlmostEqual(values["portfolio_all_day_pf"], 2.5)
        self.assertAlmostEqual(values["portfolio_all_net_pct"], 1.5)


if __name__ == "__main__":
    unittest.main()
