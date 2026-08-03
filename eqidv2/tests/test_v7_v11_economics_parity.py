from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import backtesting_result_v11_daily as daily_report
import eqidv2_entry_engine_1min_v5_id as live_entry
from nse_intraday_costs import intraday_equity_costs


class V7V11EconomicsParityTests(unittest.TestCase):
    def test_matched_july_28_signals_use_live_risk_quantities(self):
        cases = (
            (553.65, 547.01, 90),   # AIIL
            (2968.60, 2932.98, 16), # CARTRADE
            (492.75, 496.69, 126),  # TEJASNET
        )
        for entry, stop, expected in cases:
            with self.subTest(entry=entry):
                self.assertEqual(live_entry._risk_based_qty(entry, stop), expected)
                self.assertEqual(v11._risk_based_qty(entry, stop), expected)

    def test_v11_entry_resolution_deducts_paper_true_costs(self):
        row = pd.Series(
            {
                "ticker": "AIIL",
                "side": "LONG",
                "setup": "G_HIGHER_HIGH_BREAK",
                "signal_time_ist": "2026-07-28 11:25:00+05:30",
                "v7_signal_entry_time_ist": "2026-07-28 11:26:00+05:30",
                "v7_signal_entry_price": 552.55,
                "v7_signal_stop_price": 545.92,
                "v7_signal_target_price": 563.60,
                "quantity": 90,
            }
        )
        resolved = SimpleNamespace(
            outcome="TARGET",
            exit_price=563.88,
            exit_time_ist=pd.Timestamp("2026-07-28 12:00:00", tz="Asia/Kolkata"),
            bars_held=34,
            pnl_pct_price=2.0,
        )
        previous_model = v11._V11_COST_MODEL
        v11._V11_COST_MODEL = "statutory"
        try:
            with (
                patch.object(v11, "_load_1m_with_open", return_value=pd.DataFrame({"open": [1.0]})),
                patch.object(v11.er, "resolve", return_value=resolved),
            ):
                rec = v11._resolve_v7_entry_engine_signal(
                    row,
                    label="test",
                    entry_fill_model="ltp_on_signal_1m_open",
                )
        finally:
            v11._V11_COST_MODEL = previous_model

        self.assertIsNotNone(rec)
        expected = intraday_equity_costs(
            rec["entry_price_v6"],
            rec["v6_exit_price"],
            90,
            "LONG",
        )
        self.assertAlmostEqual(rec["v6_gross_pnl_rs"], expected.gross_pnl, places=4)
        self.assertAlmostEqual(rec["v6_cost_rs"], expected.total_cost, places=4)
        self.assertAlmostEqual(rec["v6_net_pnl_rs"], expected.net_pnl, places=4)

    def test_matched_gap_columns_remain_numeric_for_correct_average(self):
        ts = "2026-07-28 11:25:00+05:30"
        v11_frame = daily_report._enrich_keys(
            pd.DataFrame(
                [{
                    "ticker": "AIIL",
                    "side": "LONG",
                    "setup": "G_HIGHER_HIGH_BREAK",
                    "signal_time_v8": ts,
                    "entry_price": 553.93,
                    "quantity": 90,
                    "notional_exposure_rs": 49853.70,
                    "total_cost_rs": 53.60,
                    "net_pnl_rs": 941.80,
                }]
            ),
            source="v11",
        )
        paper_frame = daily_report._enrich_keys(
            pd.DataFrame(
                [{
                    "ticker": "AIIL",
                    "side": "LONG",
                    "setup": "G_HIGHER_HIGH_BREAK",
                    "signal_datetime": ts,
                    "entry_price": 552.83,
                    "quantity": 90,
                    "total_cost_rs": 53.52,
                    "pnl_rs": 941.88,
                }]
            ),
            source="paper",
        )
        gaps = daily_report._entry_price_gap(
            v11_frame,
            paper_frame,
            daily_report._match_sets(v11_frame, paper_frame),
        )
        self.assertTrue(pd.api.types.is_numeric_dtype(gaps["pnl_gap_v11_minus_paper"]))
        self.assertAlmostEqual(float(gaps["pnl_gap_v11_minus_paper"].mean()), -0.08, places=2)


if __name__ == "__main__":
    unittest.main()
