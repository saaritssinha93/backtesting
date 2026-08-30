from __future__ import annotations

import unittest
from pathlib import Path

import log_dashboard_server as dashboard


class DashboardBacktestingCardLabelTests(unittest.TestCase):
    def test_combined_strategy_label_is_used_for_card_and_timeline(self) -> None:
        source = Path(dashboard.__file__).read_text(encoding="utf-8")
        expected = "Backtesting result v6/v8/v10/v11/v12"

        self.assertIn(f'"backtesting_result_v11": "{expected}"', source)
        self.assertIn(
            f'{{ time: "16:20", id: "backtesting_result_v11", label: "{expected}" }}',
            source,
        )
        self.assertNotIn('"Backtesting Result v11"', source)

    def test_rename_preserves_card_runtime_contract(self) -> None:
        card_id = "backtesting_result_v11"

        self.assertEqual(
            dashboard.LOG_FILES[card_id],
            "backtesting_result_v11_latest.log",
        )
        self.assertEqual(
            dashboard.CARD_TASK_NAMES[card_id],
            ("\\EQIDV2_backtesting_result_v11_1600",),
        )
        self.assertEqual(
            dashboard.RESTARTABLE_CARDS[card_id],
            "run_backtesting_result_v11_1600.bat",
        )


if __name__ == "__main__":
    unittest.main()
