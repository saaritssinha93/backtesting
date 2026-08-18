from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import fno_oi_ema_confirm_optimize as v6_cache
import fno_oi_ema_confirm_sweep as sweep
import fno_oi_ema_confirm_v7_signal_cache as v7_cache


class FnoV7SignalCacheTests(unittest.TestCase):
    def test_cache_paths_are_isolated_from_v6(self) -> None:
        self.assertNotEqual(v7_cache.CACHE_DIR.resolve(), v6_cache.CACHE_DIR.resolve())
        self.assertNotEqual(
            v7_cache.CACHE_MANIFEST_PATH.resolve(),
            v6_cache.CACHE_MANIFEST_PATH.resolve(),
        )
        self.assertEqual(
            v7_cache.CONFIRMATION_POLICY,
            sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
        )

    def test_policy_is_fingerprinted_cache_reused_and_tamper_rebuilt(self) -> None:
        mapped = pd.DataFrame(
            [
                {
                    "futures_tradingsymbol": "TEST26AUGFUT",
                    "equity_symbol": "TEST",
                }
            ]
        )
        universe_record = {
            "file_sha256": "1" * 64,
            "universe_sha256": "2" * 64,
            "mapped_universe_sha256": "3" * 64,
            "mapped_symbol_set_sha256": "4" * 64,
        }
        inventory = {
            "source_fingerprint": "5" * 64,
            "missing_count": 0,
            "entries": [],
        }
        signals = pd.DataFrame(
            {"sid": [0], "day": [date(2026, 8, 11)], "side": ["LONG"]}
        )
        paths = {
            0: {
                "high": np.array([101.0]),
                "low": np.array([99.0]),
                "close": np.array([100.5]),
            }
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir) / "v7-cache"
            manifest_path = cache_dir / "manifest.json"
            with (
                patch.object(v7_cache, "CACHE_DIR", cache_dir),
                patch.object(v7_cache, "CACHE_MANIFEST_PATH", manifest_path),
                patch.object(
                    v7_cache.provenance,
                    "load_backtest_universe",
                    return_value=(mapped, universe_record),
                ),
                patch.object(
                    v7_cache.provenance,
                    "build_source_inventory",
                    return_value=inventory,
                ),
                patch.object(
                    v7_cache.provenance, "validate_source_inventory_readable"
                ),
                patch.object(
                    v7_cache.sweep,
                    "build_signal_table",
                    return_value=(signals, paths),
                ) as build,
            ):
                _, _, first = v7_cache.load_signals(
                    "1530",
                    400,
                    False,
                    require_complete_sources=True,
                    return_provenance=True,
                )
                self.assertEqual(build.call_count, 1)
                self.assertEqual(
                    build.call_args.kwargs["confirmation_policy"],
                    sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
                )
                self.assertEqual(
                    first["input_contract"]["confirmation_policy"],
                    sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
                )

                _, _, reused = v7_cache.load_signals(
                    "1530",
                    400,
                    False,
                    require_complete_sources=True,
                    return_provenance=True,
                )
                self.assertEqual(build.call_count, 1)
                self.assertEqual(
                    reused["input_fingerprint"], first["input_fingerprint"]
                )

                (cache_dir / "paths.npz").write_bytes(b"tampered")
                v7_cache.load_signals(
                    "1530",
                    400,
                    False,
                    require_complete_sources=True,
                )
                self.assertEqual(build.call_count, 2)

                with patch.object(
                    v7_cache,
                    "CONFIRMATION_POLICY",
                    sweep.CONFIRMATION_POLICY_V6_STRICT,
                ):
                    _, _, changed = v7_cache.load_signals(
                        "1530",
                        400,
                        False,
                        require_complete_sources=True,
                        return_provenance=True,
                    )
                self.assertEqual(build.call_count, 3)
                self.assertNotEqual(
                    changed["input_fingerprint"], first["input_fingerprint"]
                )
                self.assertEqual(
                    changed["input_contract"]["confirmation_policy"],
                    sweep.CONFIRMATION_POLICY_V6_STRICT,
                )

    def test_existing_physical_snapshot_is_routed_and_fingerprinted(self) -> None:
        mapped = pd.DataFrame(
            [{"futures_tradingsymbol": "TEST26AUGFUT", "equity_symbol": "TEST"}]
        )
        universe_record = {
            "path": "near_month_2026-08-11.parquet",
            "file_sha256": "1" * 64,
            "universe_sha256": "2" * 64,
            "mapped_universe_sha256": "3" * 64,
            "mapped_symbol_set_sha256": "4" * 64,
        }
        inventory = {
            "source_fingerprint": "5" * 64,
            "inventory_sha256": "6" * 64,
            "missing_count": 0,
            "entries": [],
        }
        signals = pd.DataFrame(
            {"sid": [0], "day": [date(2026, 8, 11)], "side": ["LONG"]}
        )
        paths = {
            0: {
                "high": np.array([101.0]),
                "low": np.array([99.0]),
                "close": np.array([100.5]),
            }
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            cache_dir = root / "cache"
            futures_root = root / "futures"
            equity_root = root / "equity"
            futures_root.mkdir()
            equity_root.mkdir()
            snapshot = {
                "schema_version": "fno_backtest_source_snapshot_v1",
                "manifest_path": str(root / "snapshot" / "manifest.json"),
                "snapshot_fingerprint": "7" * 64,
                "physical_copy": True,
                "capture_scope": (
                    "PER_FILE_STABLE_PHYSICAL_COPY_NOT_GLOBAL_FILESYSTEM_TRANSACTION"
                ),
                "futures_5m_root": str(futures_root),
                "equity_1m_root": str(equity_root),
            }
            with (
                patch.object(v7_cache, "CACHE_DIR", cache_dir),
                patch.object(
                    v7_cache, "CACHE_MANIFEST_PATH", cache_dir / "manifest.json"
                ),
                patch.object(
                    v7_cache.provenance,
                    "load_backtest_universe",
                    return_value=(mapped, universe_record),
                ),
                patch.object(
                    v7_cache.provenance,
                    "load_source_snapshot",
                    return_value=snapshot,
                ),
                patch.object(
                    v7_cache.provenance,
                    "validate_source_snapshot",
                    return_value=(snapshot, inventory),
                ),
                patch.object(
                    v7_cache.provenance,
                    "build_source_inventory",
                    return_value=inventory,
                ) as build_inventory,
                patch.object(
                    v7_cache.sweep,
                    "build_signal_table",
                    return_value=(signals, paths),
                ) as build,
            ):
                _, _, manifest = v7_cache.load_signals(
                    "1530",
                    400,
                    False,
                    source_snapshot_path=snapshot["manifest_path"],
                    return_provenance=True,
                )

            self.assertEqual(build.call_args.kwargs["futures_5m_root"], futures_root)
            self.assertEqual(build.call_args.kwargs["equity_1m_root"], equity_root)
            self.assertEqual(
                build_inventory.call_args.kwargs["futures_5m_root"], futures_root
            )
            contract = manifest["input_contract"]["source_snapshot"]
            self.assertEqual(contract["snapshot_fingerprint"], "7" * 64)
            self.assertTrue(contract["physical_copy"])


if __name__ == "__main__":
    unittest.main()
