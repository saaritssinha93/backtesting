from __future__ import annotations

import unittest

import pandas as pd

from v7_research_layer.eqidv2_v7_light_ops import (
    _merge_open_trade_signal_context,
    _setup_concentration_shadow,
)


class V7LightOpsMergeTests(unittest.TestCase):
    def test_open_state_setup_wins_without_setup_suffix_columns(self) -> None:
        open_df = pd.DataFrame(
            [{"signal_id": "one", "setup": "STATE_SETUP", "progress_r": 0.2}]
        )
        live = pd.DataFrame(
            [{"signal_id": "one", "setup": "LIVE_SETUP", "_flow_key": "k1"}]
        )

        merged = _merge_open_trade_signal_context(open_df, live)

        self.assertEqual(merged.loc[0, "setup"], "STATE_SETUP")
        self.assertEqual(merged.loc[0, "_flow_key"], "k1")
        self.assertNotIn("setup_x", merged.columns)
        self.assertNotIn("setup_y", merged.columns)
        self.assertNotIn("setup_live", merged.columns)

    def test_live_setup_fills_blank_open_state_and_concentration_uses_it(self) -> None:
        open_df = pd.DataFrame(
            [
                {
                    "signal_id": "one",
                    "setup": "",
                    "progress_r": 0.2,
                    "freshness_bucket": "WEAK",
                    "ttp_shadow_trigger": False,
                    "open_unrealized_pnl_rs": -10.0,
                }
            ]
        )
        live = pd.DataFrame(
            [{"signal_id": "one", "setup": "A_MOD_BREAK_C1_LOW"}]
        )

        merged = _merge_open_trade_signal_context(open_df, live)
        summary, rows = _setup_concentration_shadow(merged)

        self.assertEqual(merged.loc[0, "setup"], "A_MOD_BREAK_C1_LOW")
        self.assertEqual(summary["open_dominant_setup"], "A_MOD_BREAK_C1_LOW")
        self.assertEqual(rows.loc[0, "setup"], "A_MOD_BREAK_C1_LOW")


if __name__ == "__main__":
    unittest.main()
