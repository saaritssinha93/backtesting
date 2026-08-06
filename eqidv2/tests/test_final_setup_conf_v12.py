from __future__ import annotations

import unittest

from final_setup_conf_v12 import FINAL_SETUP_CONF


EXPECTED_ORIGINAL = {
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW": (
        [["quality_score", ">=", 123.7606]],
        [["sig5_adx_calc", ">=", 21.4683]],
        {},
    ),
    "D_EMA20_REJECTION": (
        [["ema20_dist_atr", "<=", -0.325336]], [], {"max_slot": "12:30"}
    ),
    "E_ORB_BREAKOUT_LONG": (
        [["rs_pct", ">=", 5.606893], ["vwap_dist_atr", "<=", 0.979716]], [], {}
    ),
    "G_HIGHER_HIGH_BREAK": (
        [["atr_pct", ">=", 0.002102], ["upper_wick_pct", ">=", 0.022387]],
        [["sig5_vol_ratio20", "<=", 3.826296]],
        {"top_n": 3},
    ),
    "L_DOUBLE_BOTTOM_VWAP": (
        [], [["sig5_rsi_dir", ">=", 60.101595]],
        {"min_slot": "10:00", "max_slot": "11:30", "top_n": 1},
    ),
    "B_HUGE_RED_FAILED_BOUNCE": (
        [],
        [
            ["pre3_close_pos", "<=", 0.581797],
            ["sig5_rsi_dir", "<=", 64.104659],
            ["pre5_mom_r", "<=", 0.284145],
        ],
        {},
    ),
    "C_OR_BREAKDOWN": (
        [],
        [["sig5_adx_calc", ">=", 39.670518], ["pre1_adx", "<=", 21.368044]],
        {},
    ),
    "A_MOD_BREAK_C1_LOW": (
        [["vol_ratio", ">=", 1.955814]],
        [["pre5_mom_r", ">=", 0.425861], ["pre3_range_r", "<=", 0.202087]],
        {},
    ),
    "G_LOWER_LOW_BREAK": (
        [["vol_ratio", ">=", 4.129044], ["quality_score", ">=", 76.444124]],
        [["sig5_rsi_dir", ">=", 68.747209]],
        {},
    ),
    "S9_MIDDAY_LOSE": ([], [], {}),
    "DOC5D_AVWAP_RECLAIM_LONG": (
        [["avwap_dist_atr", ">=", 1.027686]], [], {"min_slot": "11:00", "top_n": 2}
    ),
    "L_LATE_BB10_COMPRESSION_BREAKOUT": (
        [["market_breadth", ">=", 0.45], ["nifty_ema_up", ">=", 1.0]],
        [],
        {"min_slot": "14:00", "max_slot": "14:29"},
    ),
    "QUIET_LIQUID_ONE_BAR_DEFER_LONG": (
        [], [], {"min_slot": "09:30", "max_slot": "14:15"}
    ),
}

EXPECTED_EXITS = {
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW": (1.2, 1.5),
    "D_EMA20_REJECTION": (1.0, 3.0),
    "E_ORB_BREAKOUT_LONG": (1.0, 2.75),
    "G_HIGHER_HIGH_BREAK": (1.2, 2.0),
    "L_DOUBLE_BOTTOM_VWAP": (0.9, 2.0),
    "B_HUGE_RED_FAILED_BOUNCE": (0.9, 1.25),
    "C_OR_BREAKDOWN": (0.9, 2.0),
    "A_MOD_BREAK_C1_LOW": (1.1, 1.0),
    "G_LOWER_LOW_BREAK": (0.8, 0.8),
    "S9_MIDDAY_LOSE": (1.25, 2.5),
    "DOC5D_AVWAP_RECLAIM_LONG": (0.6, 2.0),
    "L_LATE_BB10_COMPRESSION_BREAKOUT": (0.7, 0.75),
    "QUIET_LIQUID_ONE_BAR_DEFER_LONG": (1.0, 2.0),
}


class V12RelaxationContractTests(unittest.TestCase):
    def test_all_original_conditions_are_retained_and_only_relaxed(self):
        self.assertEqual(set(FINAL_SETUP_CONF), set(EXPECTED_ORIGINAL))

        for name, cfg in FINAL_SETUP_CONF.items():
            expected_mask, expected_pre, expected_guards = EXPECTED_ORIGINAL[name]
            original = cfg["v12_original_constraints"]
            self.assertEqual(original["mask_terms"], expected_mask, name)
            self.assertEqual(original["pre_momentum_terms"], expected_pre, name)
            self.assertEqual(original["entry_guards"], expected_guards, name)

            for section, expected in (
                ("mask_terms", expected_mask),
                ("pre_momentum_terms", expected_pre),
            ):
                relaxed = cfg[section]
                self.assertEqual(len(relaxed), len(expected), f"{name}:{section}")
                for old, new in zip(expected, relaxed):
                    self.assertEqual(new[:2], old[:2], f"{name}:{section}")
                    if old[1] in (">", ">="):
                        self.assertLessEqual(new[2], old[2], f"{name}:{section}")
                    else:
                        self.assertGreaterEqual(new[2], old[2], f"{name}:{section}")

            relaxed_guards = cfg["entry_guards"]
            self.assertEqual(set(relaxed_guards), set(expected_guards), name)
            if "min_slot" in expected_guards:
                self.assertLessEqual(relaxed_guards["min_slot"], expected_guards["min_slot"], name)
            if "max_slot" in expected_guards:
                self.assertGreaterEqual(relaxed_guards["max_slot"], expected_guards["max_slot"], name)
            if "top_n" in expected_guards:
                self.assertGreaterEqual(relaxed_guards["top_n"], expected_guards["top_n"], name)

    def test_original_exit_settings_are_unchanged(self):
        for name, (sl_pct, target_pct) in EXPECTED_EXITS.items():
            self.assertEqual(FINAL_SETUP_CONF[name]["exit"]["sl_pct"], sl_pct, name)
            self.assertEqual(FINAL_SETUP_CONF[name]["exit"]["tgt_pct"], target_pct, name)

    def test_prefilter_state_machine_is_frozen_and_fail_closed(self):
        cfg = FINAL_SETUP_CONF["QUIET_LIQUID_ONE_BAR_DEFER_LONG"]
        self.assertEqual(cfg["detection"]["state_machine"]["defer_limit_bars"], 1)
        self.assertEqual(cfg["runtime_dependency"]["missing_or_incomplete_policy"], "FAIL_CLOSED")
        self.assertFalse(cfg["provenance"]["production_approved"])
        self.assertTrue(cfg["provenance"]["fresh_forward_holdout_required"])


if __name__ == "__main__":
    unittest.main()
