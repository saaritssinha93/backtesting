from __future__ import annotations

import importlib
import os
import unittest

import pandas as pd


def _mask_for(module_name: str, rows: list[dict]) -> list[bool]:
    old = os.environ.get("EQIDV2_V11_FINAL_SETUP_CONF_MODULE")
    try:
        os.environ["EQIDV2_V11_FINAL_SETUP_CONF_MODULE"] = module_name
        import avwap_5min_ID_v11_backtesting as v11

        v11._load_final_setup_conf_module.cache_clear()
        return v11._final_setup_conf_mask(pd.DataFrame(rows)).tolist()
    finally:
        if old is None:
            os.environ.pop("EQIDV2_V11_FINAL_SETUP_CONF_MODULE", None)
        else:
            os.environ["EQIDV2_V11_FINAL_SETUP_CONF_MODULE"] = old
        import avwap_5min_ID_v11_backtesting as v11
        v11._load_final_setup_conf_module.cache_clear()


class V11ResearchBookTests(unittest.TestCase):
    def test_book_a_contains_only_reduced_core(self):
        book = importlib.import_module("final_setup_conf_v11_book_a_reduced_core")
        self.assertEqual(set(book.FINAL_SETUP_CONF), {
            "E_ORB_BREAKOUT_LONG",
            "C_OR_BREAKDOWN",
            "G_HIGHER_HIGH_BREAK",
            "G_LOWER_LOW_BREAK",
        })

    def test_book_b_time_guards_are_enforced(self):
        base = {
            "side": "SHORT",
            "regime": "BEAR",
            "quality_score": 200.0,
            "vol_ratio": 5.0,
            "atr_pct": 0.01,
            "upper_wick_pct": 0.1,
            "rs_pct": 10.0,
            "vwap_dist_atr": 0.0,
        }
        rows = [
            {**base, "setup": "E_ORB_BREAKOUT_LONG", "signal_minute": 629},
            {**base, "setup": "E_ORB_BREAKOUT_LONG", "signal_minute": 630},
            {**base, "setup": "E_ORB_BREAKOUT_LONG", "signal_minute": 691},
            {**base, "setup": "C_OR_BREAKDOWN", "signal_minute": 749},
            {**base, "setup": "C_OR_BREAKDOWN", "signal_minute": 750},
            {**base, "setup": "A_MOD_BREAK_C1_LOW", "signal_minute": 809},
            {**base, "setup": "A_MOD_BREAK_C1_LOW", "signal_minute": 810},
            {**base, "setup": "B_HUGE_RED_FAILED_BOUNCE", "signal_minute": 811},
        ]
        for row in rows:
            minute = row.pop("signal_minute")
            row["signal_time_ist"] = (
                pd.Timestamp("2026-07-21", tz="Asia/Kolkata")
                + pd.Timedelta(minutes=minute)
            )
        self.assertEqual(
            _mask_for("final_setup_conf_v11_book_b_time_filtered", rows),
            [False, True, False, False, True, False, True, False],
        )

    def test_book_c_variants_keep_book_a_entries(self):
        book_a = importlib.import_module("final_setup_conf_v11_book_a_reduced_core")
        variants = {
            "final_setup_conf_v11_book_c_time90": {"max_hold_minutes": 90},
            "final_setup_conf_v11_book_c_time120": {"max_hold_minutes": 120},
            "final_setup_conf_v11_book_c_breakeven": {"breakeven_trigger_r": 1.0},
            "final_setup_conf_v11_book_c_trailing": {
                "trailing_trigger_r": 1.0,
                "trailing_distance_r": 0.5,
            },
        }
        for module_name, policy in variants.items():
            module = importlib.import_module(module_name)
            self.assertEqual(set(module.FINAL_SETUP_CONF), set(book_a.FINAL_SETUP_CONF))
            for setup, cfg in module.FINAL_SETUP_CONF.items():
                self.assertEqual(cfg["exit_policy"], policy)
                self.assertEqual(cfg["exit"], book_a.FINAL_SETUP_CONF[setup]["exit"])


if __name__ == "__main__":
    unittest.main()
