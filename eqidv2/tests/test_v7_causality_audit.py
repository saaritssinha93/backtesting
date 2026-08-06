import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v7_candidate_scan as candidate_scan
import avwap_5min_ID_v7_live_scan as legacy_live_scan
import eqidv2_v11_live_overlay as live_overlay
import v7_causality_audit as audit


def _sample_5m_frame(stored_vwap=999.0):
    day = pd.Timestamp("2026-06-09").date()
    dates = pd.date_range(
        "2026-06-09 09:15",
        periods=12,
        freq="5min",
        tz="Asia/Kolkata",
    )
    close = [100.0, 102.0, 103.0, 104.0, 103.5, 105.0, 106.0, 105.5, 107.0, 108.0, 109.0, 110.0]
    open_px = [100.0] + close[:-1]
    volume = [1000.0 + 100.0 * i for i in range(len(dates))]
    return pd.DataFrame(
        {
            "date": dates,
            "date_only": [day] * len(dates),
            "open": open_px,
            "high": [max(o, c) + 1.0 for o, c in zip(open_px, close)],
            "low": [min(o, c) - 1.0 for o, c in zip(open_px, close)],
            "close": close,
            "volume": volume,
            "ATR": [1.0] * len(dates),
            "VWAP": [stored_vwap] * len(dates),
        }
    )


def _scanner_ready_avwap_frame(avwap: float, vwap: float) -> pd.DataFrame:
    dates = pd.date_range("2026-06-09 09:20", periods=23, freq="5min", tz="Asia/Kolkata")
    close = np.full(len(dates), 99.0)
    open_px = np.full(len(dates), 99.0)
    high = np.full(len(dates), 99.5)
    low = np.full(len(dates), 98.5)
    signal_i = 20
    open_px[signal_i] = 99.0
    close[signal_i] = 101.0
    high[signal_i] = 101.5
    low[signal_i] = 98.5
    frame = pd.DataFrame(
        {
            "date": dates,
            "date_only": dates.date,
            "open": open_px,
            "high": high,
            "low": low,
            "close": close,
            "volume": 20_000.0,
            "ATR": 2.0,
            "VWAP": vwap,
            "AVWAP": avwap,
            "range": high - low,
            "body_pct": np.abs(close - open_px) / (high - low),
            "close_loc": (close - low) / (high - low),
            "vol_ratio": 2.0,
            "atr_pct": 2.0 / close,
            "vwap_dist_atr": (close - vwap) / 2.0,
            "avwap_dist_atr": (close - avwap) / 2.0,
            "traded_value_rs": close * 20_000.0,
            "day_value_so_far_rs": 50_000_000.0,
        }
    )
    return frame


class TestV7CausalityAudit(unittest.TestCase):
    def test_prepare_5m_recomputes_completed_real_bar_avwap_without_fallback(self):
        df = _sample_5m_frame(stored_vwap=999.0).iloc[:4].copy()
        df["AVWAP"] = 777.0
        df["gap_filled"] = [0, 0, 1, 0]

        prepared = v2._prepare_5m(df)
        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        expected_last = (
            typical.iloc[1] * df["volume"].iloc[1]
            + typical.iloc[3] * df["volume"].iloc[3]
        ) / (df["volume"].iloc[1] + df["volume"].iloc[3])

        self.assertTrue(np.allclose(prepared["AVWAP_source"], 777.0))
        self.assertTrue(np.isnan(prepared["AVWAP"].iloc[0]))  # 09:15 snapshot
        self.assertEqual(prepared["AVWAP"].iloc[1], typical.iloc[1])
        self.assertTrue(np.isnan(prepared["AVWAP"].iloc[2]))  # synthetic gap
        self.assertAlmostEqual(prepared["AVWAP"].iloc[3], expected_last)
        self.assertTrue(np.isnan(prepared["avwap_dist_atr"].iloc[2]))

    def test_avwap_named_detector_uses_avwap_cross_not_vwap_cross(self):
        anchored_cross = _scanner_ready_avwap_frame(avwap=100.0, vwap=90.0)
        legacy_only_cross = _scanner_ready_avwap_frame(avwap=90.0, vwap=100.0)

        anchored_setups = {
            candidate.setup for candidate in v2._scan_day(anchored_cross, "TEST", {})
        }
        legacy_setups = {
            candidate.setup for candidate in v2._scan_day(legacy_only_cross, "TEST", {})
        }

        self.assertIn("B_AVWAP_RECLAIM_REVERSAL", anchored_setups)
        self.assertNotIn("B_AVWAP_RECLAIM_REVERSAL", legacy_setups)

    def test_v7_prepared_cache_and_candidate_output_preserve_avwap_fields(self):
        prepared = _scanner_ready_avwap_frame(avwap=100.0, vwap=90.0)
        slot = pd.Timestamp("2026-06-09 11:00", tz="Asia/Kolkata")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            prepared.to_parquet(root / "TEST_stocks_indicators_5min.parquet", index=False)
            candidate_scan.reset_scan_caches(shutdown_pool_workers=False)
            try:
                with (
                    patch.object(candidate_scan, "LIVE_5M_DIR", root),
                    patch.object(candidate_scan, "FILTER_TO_V8_EXIT_SETUPS", False),
                    patch.object(candidate_scan.v2, "_prepare_5m", return_value=prepared.copy()),
                ):
                    cold = candidate_scan.scan_ticker_signal_candle("TEST", slot, {})
                    warm_telemetry = candidate_scan._new_ticker_telemetry()
                    warm = candidate_scan.scan_ticker_signal_candle(
                        "TEST", slot, {}, telemetry=warm_telemetry
                    )
            finally:
                candidate_scan.reset_scan_caches(shutdown_pool_workers=False)

        self.assertEqual(warm_telemetry["prepared_cache_hits"], 1)
        b_rows = [item for item in warm if item[0].setup == "B_AVWAP_RECLAIM_REVERSAL"]
        self.assertTrue(b_rows)
        self.assertEqual(b_rows[0][1]["AVWAP"], 100.0)
        self.assertEqual(b_rows[0][1]["avwap_dist_atr"], 0.5)

        output = candidate_scan.candidates_to_dataframe(b_rows, slot)
        self.assertEqual(output.loc[0, "avwap"], 100.0)
        self.assertEqual(output.loc[0, "avwap_dist_atr"], 0.5)
        diagnostics = json.loads(output.loc[0, "diagnostics_json"])
        self.assertEqual(diagnostics["avwap"], 100.0)
        self.assertEqual(diagnostics["avwap_dist_atr"], 0.5)

        legacy_output = legacy_live_scan.candidates_to_dataframe([b_rows[0][0]])
        self.assertEqual(legacy_output.loc[0, "avwap"], 100.0)
        self.assertEqual(legacy_output.loc[0, "avwap_dist_atr"], 0.5)
        legacy_diagnostics = json.loads(legacy_output.loc[0, "diagnostics_json"])
        self.assertEqual(legacy_diagnostics["avwap"], 100.0)
        self.assertEqual(legacy_diagnostics["avwap_dist_atr"], 0.5)

    def test_prepare_5m_overwrites_stale_source_vwap_with_causal_session_vwap(self):
        df = _sample_5m_frame()
        prepared = v2._prepare_5m(df)

        typical = (df["high"] + df["low"] + df["close"]) / 3.0
        expected_vwap = (typical * df["volume"]).cumsum() / df["volume"].cumsum()
        expected_day_value = (df["close"] * df["volume"]).cumsum()

        self.assertTrue(np.allclose(prepared["VWAP_source"], 999.0))
        self.assertTrue(np.allclose(prepared["VWAP"], expected_vwap))
        self.assertTrue(np.allclose(prepared["day_value_so_far_rs"], expected_day_value))
        self.assertTrue(np.allclose(prepared["vwap_dist_atr"], df["close"] - expected_vwap))

    def test_prepare_5m_bounds_vwap_distance_when_atr_is_tiny(self):
        df = _sample_5m_frame()
        df["ATR"] = [0.0, 1e-9] + [0.001] * (len(df) - 2)

        prepared = v2._prepare_5m(df)

        self.assertTrue(prepared["vwap_dist_atr"].abs().le(15.0).all())
        self.assertEqual(float(prepared["vwap_dist_atr"].iloc[-1]), 15.0)

    def test_prepare_5m_recomputes_an_all_null_atr_column(self):
        df = _sample_5m_frame()
        df["ATR"] = np.nan

        prepared = v2._prepare_5m(df)

        self.assertEqual(len(prepared), len(df))
        self.assertTrue(prepared["ATR"].tail(3).notna().all())
        self.assertTrue(prepared["vwap_dist_atr"].dropna().abs().le(15.0).all())

    def test_market_context_uses_return_from_day_open_to_current_bar(self):
        df = _sample_5m_frame()
        ctx = v2._market_context_from_df(df)

        day_key = str(df["date_only"].iloc[0])
        ts = pd.Timestamp(df["date"].iloc[1])
        got = ctx[day_key][ts]["market_ret_pct"]

        self.assertEqual(got, (102.0 / 100.0 - 1.0) * 100.0)

    def test_audit_warns_on_source_vwap_but_passes_prepared_feature_path(self):
        df = _sample_5m_frame()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "NIFTYBEES_stocks_indicators_5min.parquet"
            df.to_parquet(path, index=False)

            summary, rows = audit.run_audit(
                "2026-06-09",
                Path(temp_dir),
                tickers=["NIFTYBEES"],
                max_tickers=1,
            )

        row = rows.iloc[0]
        self.assertEqual(summary["overall_status"], "PASS_WITH_WARNINGS")
        self.assertEqual(row["source_vwap_status"], "FAIL")
        self.assertEqual(row["prepared_vwap_status"], "PASS")
        self.assertEqual(row["day_value_status"], "PASS")
        self.assertEqual(row["market_ret_status"], "PASS")
        self.assertEqual(row["feature_path_status"], "PASS")

    def test_live_overlay_b_avwap_gate_uses_explicit_avwap_distance_and_fails_closed(self):
        base = {
            "setup": "B_AVWAP_RECLAIM_REVERSAL",
            "side": "LONG",
            "signal_time_ist": "2026-06-09 11:00:00+05:30",
            "signal_open": 100.0,
            "signal_high": 102.0,
            "signal_low": 99.0,
            "signal_close": 101.0,
            "vol_ratio": 1.5,
            "vwap_dist_atr": 5.0,
            "market_ret_pct": 0.0,
            "quality_score": 100.0,
            "ranker_score": 1.0,
            "body_pct": 0.7,
            "atr_pct": 0.01,
        }
        signals = pd.DataFrame(
            [
                {**base, "ticker": "PASS", "avwap_dist_atr": 0.61},
                {**base, "ticker": "FAIL", "avwap_dist_atr": 0.59},
                {**base, "ticker": "MISSING"},
            ]
        )

        mask = live_overlay.selected_strategy_mask(
            signals,
            profile="production_core_ab_max_pnl_low_valid",
        )

        self.assertEqual(mask.tolist(), [True, False, False])


if __name__ == "__main__":
    unittest.main()
