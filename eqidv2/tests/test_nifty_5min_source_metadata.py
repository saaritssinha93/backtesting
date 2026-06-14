import tempfile
import unittest
from pathlib import Path

import trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly as nf5


class _Logger:
    def warning(self, *args, **kwargs):
        pass


class Nifty5MinSourceMetadataTests(unittest.TestCase):
    def test_index_alias_without_metadata_is_rebuilt_for_true_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "NIFTY_stocks_indicators_5min.parquet"
            out_path.write_bytes(b"legacy proxy-risk placeholder")

            self.assertTrue(
                nf5._should_rebuild_for_source(
                    str(out_path),
                    symbol="NIFTY 50",
                    token=123,
                    primary_alias="NIFTY",
                    logger=_Logger(),
                )
            )

    def test_matching_source_metadata_avoids_rebuild(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "NIFTY_stocks_indicators_5min.parquet"
            out_path.write_bytes(b"true index placeholder")
            nf5._write_source_meta(str(out_path), "NIFTY 50", 123, ["NIFTY"], _Logger())

            self.assertFalse(
                nf5._should_rebuild_for_source(
                    str(out_path),
                    symbol="NIFTY 50",
                    token=123,
                    primary_alias="NIFTY",
                    logger=_Logger(),
                )
            )


if __name__ == "__main__":
    unittest.main()
