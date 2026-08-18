from __future__ import annotations

import unittest

import fno_v5_0926_all_history_day_pf_optimize as research
import fno_v5_0926_day_pf_optimize as train_only
import fno_v5_live_config as live_config


class FnoV50926AllHistoryDayPfOptimizeTests(unittest.TestCase):
    def test_outputs_are_isolated_from_protected_and_train_only(self) -> None:
        research.validate_output_isolation()
        outputs = {
            research.REPORT_PATH.resolve(),
            research.RANKED_PATH.resolve(),
            research.SETUPS_PATH.resolve(),
            research.TRADES_PATH.resolve(),
            research.DAILY_PATH.resolve(),
            research.MANIFEST_PATH.resolve(),
        }
        self.assertNotIn(live_config.SELECTED_DAILY_PATH.resolve(), outputs)
        self.assertNotIn(train_only.DAILY_PATH.resolve(), outputs)

    def test_defaults_keep_costs_and_sample_guards(self) -> None:
        args = research.parse_args([])

        self.assertEqual(args.split_day, "2026-07-17")
        self.assertEqual(args.cost_bps, 5.0)
        self.assertEqual(args.min_portfolio_fills, 20)
        self.assertEqual(args.min_portfolio_days, 15)


if __name__ == "__main__":
    unittest.main()
