import tempfile
import unittest
from pathlib import Path

import pandas as pd

import data_for_backtesting_verify as verify


class DataForBacktestingVerifyTests(unittest.TestCase):
    def _write_parquet(self, root: Path, column: str, ticker: str = "TEST") -> None:
        pd.DataFrame(
            {
                column: pd.date_range(
                    "2026-06-11 09:15",
                    periods=4,
                    freq="5min",
                    tz="Asia/Kolkata",
                ),
                "close": [100.0, 101.0, 102.0, 103.0],
            }
        ).to_parquet(root / f"{ticker}_stocks_indicators_5min.parquet", index=False)

    def test_scan_accepts_date_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_parquet(root, "date")

            result = verify._scan_parquet_dir(root, "2026-06-11", expected_bars=4)

        self.assertEqual(result["overall"], "PASS")
        self.assertEqual(result["ok"], 1)

    def test_scan_accepts_legacy_datetime_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_parquet(root, "datetime")

            result = verify._scan_parquet_dir(root, "2026-06-11", expected_bars=4)

        self.assertEqual(result["overall"], "PASS")
        self.assertEqual(result["ok"], 1)

    def test_required_tickers_exclude_out_of_scope_archives(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_parquet(root, "date", ticker="FNO1")
            self._write_parquet(root, "date", ticker="OLDARCHIVE")

            result = verify._scan_parquet_dir(
                root,
                "2026-06-11",
                expected_bars=4,
                required_tickers={"FNO1"},
            )

        self.assertEqual(result["overall"], "PASS")
        self.assertEqual(result["total_tickers"], 1)
        self.assertEqual(result["required_tickers"], 1)
        self.assertNotIn("OLDARCHIVE", result["worst_tickers"])

    def test_missing_required_ticker_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_parquet(root, "date", ticker="FNO1")

            result = verify._scan_parquet_dir(
                root,
                "2026-06-11",
                expected_bars=4,
                required_tickers={"FNO1", "MISSING"},
            )

        self.assertEqual(result["overall"], "FAIL")
        self.assertEqual(result["fail"], 1)
        self.assertEqual(result["worst_tickers"]["MISSING"]["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
