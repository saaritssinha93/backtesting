from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

import fno_oi_hybrid_data as hybrid
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5 as v5
import fno_v5_hybrid_backtest as hybrid_backtest
import fno_v5_live_config as live_config


class FnoOiEmaConfirmV5Tests(unittest.TestCase):
    def test_v5_timing_caps_and_selected_objective(self) -> None:
        self.assertEqual(v5.V5_WINDOWS, {940: 941, 945: 946})
        self.assertEqual(v5.LONG_MAX, 1)
        self.assertEqual(v5.SHORT_MAX, 2)
        self.assertEqual(v5.SELECTED_OBJECTIVE, "TRAIN_ONLY_ROBUST_V5_OPTIMIZER")

    def test_v5_cli_uses_guard_and_beam_defaults(self) -> None:
        args = v5.parse_args([])

        self.assertEqual(args.mode, v5.MODE_CURRENT_REPLAY)
        self.assertEqual(args.min_trades, 8)
        self.assertAlmostEqual(args.min_day_win, 0.40)
        self.assertAlmostEqual(args.max_top_profit_share, 0.45)
        self.assertEqual(args.retain_n, 1000)
        self.assertEqual(args.window_retain_n, 2000)
        self.assertEqual(args.full_history_leg_retain_n, 12)
        self.assertEqual(args.full_history_beam_width, 1200)

    def test_v5_cli_exposes_full_history_mode_with_separate_outputs(self) -> None:
        args = v5.parse_args(["--mode", "full-history"])

        self.assertEqual(args.mode, v5.MODE_FULL_HISTORY)
        self.assertEqual(v5.FULL_HISTORY_SELECTED_OBJECTIVE, "BEST_TRADE_PF")
        self.assertNotEqual(v5.FULL_HISTORY_REPORT_PATH, v5.REPORT_PATH)
        self.assertNotEqual(
            v5.FULL_HISTORY_SELECTED_DAILY_OUTPUT_PATH,
            v5.SELECTED_DAILY_OUTPUT_PATH,
        )
        self.assertNotEqual(
            v5.FULL_HISTORY_RANKED_OUTPUT_PATH,
            v5.SELECTED_DAILY_OUTPUT_PATH,
        )

    def test_full_history_contract_accepts_only_equity_prices_and_futures_oi(self) -> None:
        signals = pd.DataFrame(
            {
                "sid": [1],
                "day": [date(2026, 8, 10)],
                "hhmm_int": [940],
                "tradingsymbol": ["TEST"],
                "instrument_token": [101],
                "exchange": ["NSE"],
                "futures_tradingsymbol": ["TEST26AUGFUT"],
                "futures_instrument_token": [1001],
                "data_contract": [hybrid.DATA_CONTRACT_VERSION],
                "price_source": [hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION],
                "oi_source": ["NFO_FUTURE"],
                "oi": [1100.0],
                "prev_oi": [1000.0],
                "oi_change_pct": [10.0],
                "price_change_pct": [0.5],
                "volume_ratio": [2.0],
                "body_ratio": [0.6],
                "wick_ratio": [0.2],
                "trigger": [101.0],
                "traded_value": [20_000_000.0],
            }
        )
        paths = {
            1: {
                "high": np.array([101.5]),
                "low": np.array([100.5]),
                "close": np.array([101.2]),
            }
        }

        meta = v5.validate_cash_equity_signal_contract(signals, paths)

        self.assertEqual(meta["data_contract"], hybrid.DATA_CONTRACT_VERSION)
        self.assertEqual(meta["oi_fields"], ["oi", "prev_oi", "oi_change_pct"])

        futures_priced = signals.assign(tradingsymbol="TEST26AUGFUT")
        with self.assertRaisesRegex(RuntimeError, "futures contract leaked"):
            v5.validate_cash_equity_signal_contract(futures_priced, paths)

        leaked_ohlc = signals.assign(futures_close=500.0)
        with self.assertRaisesRegex(RuntimeError, "Forbidden futures"):
            v5.validate_cash_equity_signal_contract(leaked_ohlc, paths)

    def test_hybrid_backtest_uses_same_short_ranking_and_cap_as_live(self) -> None:
        session = date(2026, 8, 10)
        signals = pd.DataFrame(
            {
                "sid": [1, 2, 3],
                "day": [session] * 3,
                "hhmm_int": [940] * 3,
                "side": ["SHORT"] * 3,
                "tradingsymbol": ["A", "B", "C"],
                "price_change_pct": [-0.5] * 3,
                "oi_change_pct": [1.2, 1.8, 1.5],
                "volume_ratio": [2.0] * 3,
                "body_ratio": [0.5] * 3,
                "wick_ratio": [0.2] * 3,
                "traded_value": [10_000_000.0] * 3,
            }
        )
        setup = live_config.setup_for("09:40", "SHORT")

        selected = hybrid_backtest.select_setup_rows(signals, setup)

        self.assertEqual(selected["tradingsymbol"].tolist(), ["B", "C"])

    def test_v5_curve_adds_both_windows_to_locked_v4(self) -> None:
        session = date(2026, 8, 10)
        locked = pd.DataFrame(
            {
                "day": [session],
                "baseline_long_status": ["WIN"],
                "baseline_long_trade_details": ["[09:26] BASE=+1.000%"],
                "baseline_short_status": ["NO_SIGNAL"],
                "baseline_short_trade_details": [float("nan")],
                "addon_long_status": ["NO_SIGNAL"],
                "addon_long_trade_details": [float("nan")],
                "addon_short_status": ["NO_SIGNAL"],
                "addon_short_trade_details": [float("nan")],
                "v4_long_status": ["NO_SIGNAL"],
                "v4_long_trade_details": [float("nan")],
                "v4_short_status": ["WIN"],
                "v4_short_trade_details": ["[09:36] OLD=+0.700%"],
                "long_selections": [1],
                "long_fills": [1],
                "long_return_pct": [1.0],
                "long_gross_profit_pct": [1.0],
                "long_gross_loss_pct": [0.0],
                "short_selections": [1],
                "short_fills": [1],
                "short_return_pct": [0.7],
                "short_gross_profit_pct": [0.7],
                "short_gross_loss_pct": [0.0],
                "selections": [2],
                "fills": [2],
                "portfolio_net_return_pct": [1.7],
            }
        )
        legs = {
            (slot, side): v5.current_v4.empty_leg([session])
            for slot in v5.V5_WINDOWS
            for side in ("LONG", "SHORT")
        }
        long_941 = legs[(940, "LONG")]
        long_941.loc[0, "selected_symbol"] = "NEWL"
        long_941.loc[0, "trade_details"] = "[09:41] NEWL=+0.500%"
        long_941.loc[0, "status"] = "WIN"
        long_941.loc[0, "net_return_pct"] = 0.5
        long_941.loc[0, "selections"] = 1
        long_941.loc[0, "fills"] = 1
        long_941.loc[0, "gross_profit_pct"] = 0.5

        short_946 = legs[(945, "SHORT")]
        short_946.loc[0, "selected_symbol"] = "NEWS"
        short_946.loc[0, "trade_details"] = "[09:46] NEWS=-0.200%"
        short_946.loc[0, "status"] = "LOSS"
        short_946.loc[0, "net_return_pct"] = -0.2
        short_946.loc[0, "selections"] = 1
        short_946.loc[0, "fills"] = 1
        short_946.loc[0, "gross_loss_pct"] = 0.2

        curve = v5.build_v5_curve(locked, legs, "TEST")
        row = curve.iloc[0]

        self.assertEqual(row["baseline_short_trade_details"], "")
        self.assertEqual(row["v4_long_trade_details"], "")
        self.assertEqual(int(row["long_selections"]), 2)
        self.assertEqual(int(row["short_selections"]), 2)
        self.assertEqual(int(row["selections"]), 4)
        self.assertEqual(int(row["fills"]), 4)
        self.assertAlmostEqual(float(row["v5_addon_net_return_pct"]), 0.3)
        self.assertAlmostEqual(float(row["portfolio_net_return_pct"]), 2.0)
        self.assertAlmostEqual(float(row["cumulative_net_pct"]), 2.0)
        self.assertEqual(
            row["confirmation_end"], "09:26,09:31,09:36,09:41,09:46"
        )


if __name__ == "__main__":
    unittest.main()
