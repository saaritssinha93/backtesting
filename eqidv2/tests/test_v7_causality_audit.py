import os
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import avwap_5min_ID_v2_backtesting as v2
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


class TestV7CausalityAudit(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
