from __future__ import annotations

import unittest

import pandas as pd

import avwap_5min_ID_v12_backtesting as v12


class V12WorkerConfPropagationTests(unittest.TestCase):
    def test_scan_state_round_trip_preserves_conf_only_detectors(self):
        original = v12._capture_v11_worker_scan_state()
        expected_setups = {
            "S9_MIDDAY_LOSE",
            "DOC5D_AVWAP_RECLAIM_LONG",
            "L_LATE_BB10_COMPRESSION_BREAKOUT",
        }
        state = {
            "allowed_setups": tuple(sorted(expected_setups)),
            "filter_to_v8_exit_setups": True,
            "enable_s9_midday_lose": True,
            "enable_doc5d_avwap_reclaim": True,
        }
        try:
            v12._apply_v11_worker_scan_state(state)
            captured = v12._capture_v11_worker_scan_state()
            self.assertEqual(set(captured["allowed_setups"]), expected_setups)
            self.assertTrue(captured["filter_to_v8_exit_setups"])
            self.assertTrue(captured["enable_s9_midday_lose"])
            self.assertTrue(captured["enable_doc5d_avwap_reclaim"])
        finally:
            v12._apply_v11_worker_scan_state(original)

    def test_final_conf_top_n_is_enforced_per_day_and_slot(self):
        rows = []
        for day in ("2026-07-01", "2026-07-02"):
            for ticker, distance in (("LOW", 0.1), ("MID", 0.5), ("HIGH", 0.9)):
                rows.append(
                    {
                        "ticker": f"{ticker}_{day[-2:]}",
                        "setup": "L_DOUBLE_BOTTOM_VWAP",
                        "signal_time_ist": f"{day} 10:00:00+05:30",
                        "vwap_dist_atr": distance,
                    }
                )
        frame = pd.DataFrame(rows)
        mask = v12._final_setup_conf_mask(frame)
        kept = frame.loc[mask, "ticker"].tolist()
        self.assertEqual(len(kept), 4)
        self.assertFalse(any(value.startswith("LOW_") for value in kept))

    def test_d_ema20_distance_is_materialized_when_scanner_row_omits_it(self):
        original_loader = v12._load_5m_ind_bars
        bars = pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-07-01 10:00:00", tz="Asia/Kolkata")],
                "close": [100.0],
                "EMA_20": [101.0],
                "ATR": [2.0],
            }
        )
        try:
            v12._load_5m_ind_bars = lambda ticker: bars
            enriched = v12._selected_strategy_features(
                pd.DataFrame(
                    [{
                        "ticker": "TEST",
                        "setup": "D_EMA20_REJECTION",
                        "signal_time_ist": "2026-07-01 10:00:00+05:30",
                        "signal_close": 100.0,
                    }]
                )
            )
            self.assertAlmostEqual(float(enriched.loc[0, "ema20_dist_atr"]), -0.5)
        finally:
            v12._load_5m_ind_bars = original_loader

    def test_historical_scan_trim_keeps_requested_and_latest_prior_session(self):
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    [
                        "2026-06-29 09:15:00+05:30",
                        "2026-06-30 09:15:00+05:30",
                        "2026-07-01 09:15:00+05:30",
                        "2026-07-02 09:15:00+05:30",
                        "2026-07-03 09:15:00+05:30",
                    ]
                ),
                "close": [1, 2, 3, 4, 5],
            }
        )
        trimmed = v12._trim_historical_scan_input(
            frame, ("2026-07-01", "2026-07-02")
        )
        self.assertEqual(
            trimmed["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2026-06-30", "2026-07-01", "2026-07-02"],
        )

    def test_nifty_short_regime_uses_only_sessions_before_trade_day(self):
        original_loader = v12._historical_nifty_daily_closes
        original_enabled = v12.NIFTY_REGIME_GATE_ENABLED
        original_ma_days = v12.NIFTY_MA_DAYS
        original_multiplier = v12.NIFTY_REGIME_SHORT_SIZE_MULT
        daily = pd.DataFrame(
            {
                "trade_date": pd.date_range(
                    "2026-01-01", periods=6, freq="D", tz="Asia/Kolkata"
                ),
                # Jan 1-5 form a bullish completed-session regime.  Jan 6 is
                # deliberately extreme future information for a Jan 6 entry.
                "close": [10.0, 10.0, 10.0, 11.0, 12.0, 1.0],
            }
        )
        try:
            v12._historical_nifty_daily_closes = lambda: daily
            v12.NIFTY_REGIME_GATE_ENABLED = True
            v12.NIFTY_MA_DAYS = 3
            v12.NIFTY_REGIME_SHORT_SIZE_MULT = 0.5
            v12._historical_nifty_short_mult.cache_clear()

            self.assertEqual(v12._historical_nifty_short_mult("2026-01-06"), 0.5)
            # On Jan 7, the completed Jan 6 close legitimately becomes known.
            self.assertEqual(v12._historical_nifty_short_mult("2026-01-07"), 1.0)
        finally:
            v12._historical_nifty_daily_closes = original_loader
            v12.NIFTY_REGIME_GATE_ENABLED = original_enabled
            v12.NIFTY_MA_DAYS = original_ma_days
            v12.NIFTY_REGIME_SHORT_SIZE_MULT = original_multiplier
            v12._historical_nifty_short_mult.cache_clear()

    def test_first_1m_entry_binary_search_matches_next_minute_contract(self):
        index = pd.to_datetime(
            [
                "2026-07-01 10:00:00+05:30",
                "2026-07-01 10:01:00+05:30",
                "2026-07-01 10:02:00+05:30",
            ]
        )
        bars = pd.DataFrame({"open": [100.0, 101.0, 102.0]}, index=index)

        entry = v12._first_1m_entry(
            bars, pd.Timestamp("2026-07-01 10:00:00+05:30")
        )

        self.assertIsNotNone(entry)
        self.assertEqual(entry[0], index[1])
        self.assertEqual(entry[1], 101.0)
        self.assertIsNone(
            v12._first_1m_entry(
                bars,
                pd.Timestamp("2026-07-01 10:00:00+05:30"),
                decision_ready_at=pd.Timestamp("2026-07-01 10:03:00+05:30"),
            )
        )


if __name__ == "__main__":
    unittest.main()
