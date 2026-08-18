from __future__ import annotations

import math
import unittest

import numpy as np

import fno_v5_0926_day_pf_optimize as focused
import fno_v5_hybrid_optimize as optimizer
import fno_v5_live_config as live_config


class FnoV50926DayPfOptimizeTests(unittest.TestCase):
    def _state(self, *, robust_day_pf: float, day_pf: float, pf: float):
        metrics = {
            "robust_day_pf": robust_day_pf,
            "day_pf": day_pf,
            "worst_fold_pf": 1.1,
            "robust_trade_pf": 1.2,
            "day_win_rate": 0.6,
            "pf": pf,
            "net_pct": 5.0,
            "fills": 25,
        }
        return optimizer.PortfolioState(
            choices=(),
            train_net=np.array([], dtype=float),
            train_day_idx=np.array([], dtype=int),
            train_orders=0,
            train_metrics=metrics,
        )

    def test_primary_key_prefers_robust_day_pf_over_trade_pf(self) -> None:
        day_focused = self._state(robust_day_pf=2.0, day_pf=2.5, pf=1.5)
        trade_focused = self._state(robust_day_pf=1.5, day_pf=1.8, pf=8.0)

        self.assertGreater(
            focused.day_pf_key(day_focused),
            focused.day_pf_key(trade_focused),
        )

    def test_scope_is_only_0925_to_0926_with_v5_caps(self) -> None:
        for setup in focused.current_0926_setups():
            focused.validate_setup_scope(setup)
            cap = 1 if setup.side == "LONG" else 2
            self.assertLessEqual(setup.max_entries, cap)
        self.assertEqual(focused.SIGNAL_SLOT, 925)
        self.assertEqual(focused.CONFIRMATION_END, "09:26")

    def test_output_paths_do_not_overlap_protected_v5(self) -> None:
        focused.validate_output_isolation()
        outputs = {
            focused.REPORT_PATH.resolve(),
            focused.RANKED_PATH.resolve(),
            focused.SETUPS_PATH.resolve(),
            focused.TRADES_PATH.resolve(),
            focused.DAILY_PATH.resolve(),
            focused.MANIFEST_PATH.resolve(),
        }
        self.assertNotIn(live_config.SELECTED_DAILY_PATH.resolve(), outputs)

    def test_defaults_use_frozen_test_and_costs(self) -> None:
        args = focused.parse_args([])

        self.assertEqual(args.split_day, "2026-07-17")
        self.assertTrue(math.isclose(args.cost_bps, 5.0))
        self.assertEqual(args.min_portfolio_train_fills, 20)
        self.assertEqual(args.min_portfolio_train_days, 15)
        self.assertTrue(math.isclose(args.min_day_win, 0.50))
        self.assertTrue(math.isclose(args.min_worst_fold_pf, 1.00))


if __name__ == "__main__":
    unittest.main()
