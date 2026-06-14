import tempfile
import unittest
from pathlib import Path

import pandas as pd

import data_for_backtesting_verify as verify


class DataForBacktestingVerifyTests(unittest.TestCase):
    def _write_parquet(self, root: Path, column: str) -> None:
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
        ).to_parquet(root / "TEST_stocks_indicators_5min.parquet", index=False)

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


if __name__ == "__main__":
    unittest.main()
