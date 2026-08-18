from __future__ import annotations

import math
import unittest

import numpy as np

import fno_v5_hybrid_optimize as optimize
import fno_v5_live_config as live_config


class FnoV5HybridOptimizeTests(unittest.TestCase):
    def test_score_vectors_drops_the_best_whole_day(self) -> None:
        stats = optimize.score_vectors(
            np.array([2.0, -1.0]),
            np.array([0, 1]),
            2,
            orders=2,
        )

        self.assertAlmostEqual(stats["pf"], 2.0)
        self.assertAlmostEqual(stats["day_pf"], 2.0)
        self.assertAlmostEqual(stats["robust_trade_pf"], 0.0)
        self.assertAlmostEqual(stats["robust_day_pf"], 0.0)
        self.assertAlmostEqual(stats["net_pct"], 1.0)

    def test_portfolio_guards_require_robust_sample(self) -> None:
        guards = optimize.OptimizerGuards()
        passing = {
            "fills": 40,
            "active_days": 25,
            "wins": 20,
            "losses": 20,
            "net_pct": 10.0,
            "pf": 1.8,
            "day_pf": 1.7,
            "robust_trade_pf": 1.4,
            "robust_day_pf": 1.3,
            "day_win_rate": 0.52,
            "top_day_share": 0.20,
            "positive_folds": 3,
            "worst_fold_pf": 1.10,
        }

        self.assertTrue(optimize.passes_portfolio_guards(passing, guards))
        failing = {**passing, "robust_day_pf": 0.95}
        self.assertFalse(optimize.passes_portfolio_guards(failing, guards))

    def test_optimizer_preserves_v5_times_and_caps(self) -> None:
        self.assertEqual(optimize.SIGNAL_SLOTS, (925, 930, 935, 940, 945))
        for setup in live_config.ACTIVE_SETUPS:
            cap = 1 if setup.side == "LONG" else 2
            self.assertLessEqual(setup.max_entries, cap)
            self.assertEqual(
                setup.confirmation_end,
                live_config.SIGNAL_TO_CONFIRMATION[setup.signal_end],
            )

    def test_optimizer_outputs_do_not_overlap_protected_v5(self) -> None:
        optimize.validate_output_isolation()
        outputs = {
            optimize.REPORT_PATH.resolve(),
            optimize.RANKED_PATH.resolve(),
            optimize.SETUPS_PATH.resolve(),
            optimize.TRADES_PATH.resolve(),
            optimize.DAILY_PATH.resolve(),
        }

        self.assertNotIn(live_config.SELECTED_DAILY_PATH.resolve(), outputs)

    def test_cli_defaults_keep_test_frozen_and_costs_enabled(self) -> None:
        args = optimize.parse_args([])

        self.assertEqual(args.split_day, "2026-07-17")
        self.assertAlmostEqual(args.cost_bps, 5.0)
        self.assertEqual(args.min_portfolio_train_fills, 35)
        self.assertEqual(args.min_portfolio_train_days, 20)
        self.assertTrue(math.isclose(args.max_top_day_share, 0.35))
        self.assertTrue(math.isclose(args.min_worst_fold_pf, 0.80))
        self.assertEqual(args.search_profile, "conservative")


if __name__ == "__main__":
    unittest.main()
