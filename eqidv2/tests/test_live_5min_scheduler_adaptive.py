import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import eqidv2_eod_scheduler_for_5mins_data_live_minimal as scheduler


class Live5MinSchedulerAdaptiveTests(unittest.TestCase):
    def test_fno_quality_gate_requires_exact_one_minute_lineage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "TEST_stocks_indicators_5min.parquet"
            timestamps = pd.date_range(
                "2026-08-11 09:15", periods=3, freq="5min", tz=scheduler.IST
            )
            frame = pd.DataFrame(
                {
                    "date": timestamps,
                    "open": [100.0, 100.0, 101.0],
                    "high": [101.0, 101.0, 102.0],
                    "low": [99.0, 99.0, 100.0],
                    "close": [100.5, 100.5, 101.5],
                    "volume": [1000.0, 1000.0, 800.0],
                    "gap_filled": [0, 0, 0],
                    "opening_snapshot": [1, 0, 0],
                    "provisional_stale": [0, 0, 0],
                    "source_1m_count": [0, 5, float("nan")],
                }
            )
            frame.to_parquet(path, index=False)
            target = timestamps[-1].to_pydatetime()
            with patch.object(scheduler, "RUNTIME_DATA_5M_DIR", root):
                generic = scheduler._ticker_has_required_5m_slot_data("TEST", target)
                exact = scheduler._ticker_has_required_5m_slot_data(
                    "TEST", target, require_exact_1m=True
                )
                frame.loc[2, "source_1m_count"] = 5
                frame.to_parquet(path, index=False)
                verified = scheduler._ticker_has_required_5m_slot_data(
                    "TEST", target, require_exact_1m=True
                )

        self.assertTrue(generic[0])
        self.assertFalse(exact[0])
        self.assertTrue(verified[0])

    def test_live_fetch_universe_includes_all_mapped_fno_equities(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_path = root / "latest_near_month.parquet"
            pd.DataFrame(
                {
                    "equity_symbol": ["IDEA", "LTM", None],
                    "equity_instrument_token": [3677697, 4561409, None],
                }
            ).to_parquet(universe_path, index=False)
            logger = Mock()
            with patch.object(scheduler, "runtime_dir", return_value=universe_path):
                tickers, tokens = scheduler._include_mapped_fno_equities(
                    ["RELIANCE"], {"RELIANCE": 738561}, logger
                )

        self.assertEqual(tickers, ["IDEA", "LTM", "RELIANCE"])
        self.assertEqual(tokens["IDEA"], 3677697)
        self.assertEqual(tokens["LTM"], 4561409)

    def test_partition_error_preserves_summary_for_adaptive_throttle(self):
        summary = {
            "total_elapsed_sec": 150.0,
            "max_partition_elapsed_sec": 150.0,
            "sla_warn_sec": 50.0,
            "failures": ["app1: partition_timeout=150.0s"],
        }
        error = scheduler.ParallelPartitionRunError("failed", summary)

        next_total, next_per_app, healthy_streak, reason = scheduler._adapt_worker_budget(
            configured_total=320,
            configured_per_app=40,
            current_total=320,
            current_per_app=40,
            slot_summary=error.summary,
            healthy_streak=1,
        )

        self.assertEqual(next_total, 288)
        self.assertEqual(next_per_app, 36)
        self.assertEqual(healthy_streak, 0)
        self.assertIn("failures=1", reason)


if __name__ == "__main__":
    unittest.main()
