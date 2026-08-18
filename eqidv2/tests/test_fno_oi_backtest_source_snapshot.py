from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


class FnoBacktestSourceSnapshotTests(unittest.TestCase):
    def test_physical_snapshot_survives_live_source_replacement_and_detects_tamper(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            futures_root = root / "live-futures"
            equity_root = root / "live-equity"
            snapshot_root = root / "snapshots"
            universe_root = root / "universe"
            for path in (futures_root, equity_root, universe_root):
                path.mkdir()

            future_symbol = "TEST26AUGFUT"
            equity_symbol = "TEST"
            future_path = (
                futures_root
                / f"{common.safe_contract_stem(future_symbol)}_5minute.parquet"
            )
            equity_path = hybrid.equity_one_minute_path(
                equity_symbol, equity_root
            )
            pd.DataFrame(
                {
                    "timestamp": [pd.Timestamp("2026-08-11 09:25", tz="Asia/Kolkata")],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000],
                    "oi": [100_000],
                }
            ).to_parquet(future_path, index=False)
            pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-08-11 09:26", tz="Asia/Kolkata")],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000],
                }
            ).to_parquet(equity_path, index=False)

            mapped = pd.DataFrame(
                [
                    {
                        "futures_tradingsymbol": future_symbol,
                        "equity_symbol": equity_symbol,
                    }
                ]
            )
            universe_path = universe_root / "near_month_2026-08-11.parquet"
            mapped.assign(master_date=pd.Timestamp("2026-08-11")).to_parquet(
                universe_path, index=False
            )
            universe_record = {
                "path": str(universe_path.resolve()),
                "file_sha256": provenance.sha256_file(universe_path),
                "universe_sha256": "1" * 64,
                "mapped_universe_sha256": "2" * 64,
                "mapped_symbol_set_sha256": "3" * 64,
            }

            with (
                patch.object(common, "RAW_CONTRACT_DIR", futures_root),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_root),
            ):
                snapshot = provenance.create_source_snapshot(
                    mapped,
                    universe_record,
                    universe_path=universe_path,
                    snapshot_root=snapshot_root,
                )
                loaded, observed = provenance.validate_source_snapshot(
                    snapshot["manifest_path"], mapped, universe_record
                )

                self.assertTrue(loaded["physical_copy"])
                self.assertEqual(observed["missing_count"], 0)
                self.assertEqual(len(loaded["captures"]), 2)
                frozen_future = Path(
                    next(
                        item["snapshot_path"]
                        for item in loaded["captures"]
                        if item["role"] == "NFO_FUTURES_5M"
                    )
                )
                self.assertFalse(os.path.samefile(future_path, frozen_future))
                frozen_hash = provenance.sha256_file(frozen_future)

                # Simulate the live writer replacing the mutable source.
                pd.DataFrame(
                    {
                        "timestamp": [
                            pd.Timestamp("2026-08-11 09:25", tz="Asia/Kolkata")
                        ],
                        "open": [200.0],
                        "high": [201.0],
                        "low": [199.0],
                        "close": [200.5],
                        "volume": [2000],
                        "oi": [200_000],
                    }
                ).to_parquet(future_path, index=False)
                self.assertEqual(provenance.sha256_file(frozen_future), frozen_hash)
                provenance.validate_source_snapshot(
                    snapshot["manifest_path"], mapped, universe_record
                )

                manifest_path = Path(snapshot["manifest_path"])
                manifest_bytes = manifest_path.read_bytes()
                tampered_manifest = json.loads(manifest_bytes)
                tampered_manifest["snapshot_fingerprint"] = "0" * 64
                manifest_path.write_text(json.dumps(tampered_manifest), encoding="utf-8")
                with self.assertRaisesRegex(AssertionError, "fingerprint"):
                    provenance.validate_source_snapshot(
                        manifest_path, mapped, universe_record
                    )
                manifest_path.write_bytes(manifest_bytes)

                # Snapshot mutation is fail-closed.
                with frozen_future.open("ab") as handle:
                    handle.write(b"tamper")
                with self.assertRaises((AssertionError, RuntimeError)):
                    provenance.validate_source_snapshot(
                        snapshot["manifest_path"], mapped, universe_record
                    )


if __name__ == "__main__":
    unittest.main()
