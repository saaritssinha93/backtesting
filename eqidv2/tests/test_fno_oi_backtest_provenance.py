from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_optimize as optimizer
import fno_oi_hybrid_data as hybrid


def _universe(day: date = date(2026, 8, 11)) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "exchange": "NFO",
                "tradingsymbol": "RELIANCE26AUGFUT",
                "instrument_token": 101,
                "expiry": pd.Timestamp("2026-08-27"),
                "master_date": pd.Timestamp(day),
                "underlying": "RELIANCE",
                "is_index_future": False,
                "equity_symbol": "RELIANCE",
                "equity_instrument_token": 201,
                "futures_tradingsymbol": "RELIANCE26AUGFUT",
                "futures_instrument_token": 101,
                "lot_size": 250,
                "tick_size": 0.05,
            },
            {
                "exchange": "NFO",
                "tradingsymbol": "NIFTY26AUGFUT",
                "instrument_token": 102,
                "expiry": pd.Timestamp("2026-08-27"),
                "master_date": pd.Timestamp(day),
                "underlying": "NIFTY",
                "is_index_future": True,
                "equity_symbol": None,
                "equity_instrument_token": None,
                "futures_tradingsymbol": "NIFTY26AUGFUT",
                "futures_instrument_token": 102,
                "lot_size": 50,
                "tick_size": 0.05,
            },
        ]
    )


class FnoBacktestProvenanceTests(unittest.TestCase):
    def _roots(self, root: Path):
        universe_dir = root / "universe"
        futures_dir = root / "futures"
        equity_dir = root / "equity"
        universe_dir.mkdir()
        futures_dir.mkdir()
        equity_dir.mkdir()
        return universe_dir, futures_dir, equity_dir

    def _write_universe(self, universe_dir: Path, frame: pd.DataFrame) -> Path:
        master_day = pd.Timestamp(frame["master_date"].iloc[0]).date()
        dated = universe_dir / f"near_month_{master_day.isoformat()}.parquet"
        frame.to_parquet(dated, index=False)
        frame.to_parquet(universe_dir / "latest_near_month.parquet", index=False)
        return dated

    def _write_sources(self, futures_dir: Path, equity_dir: Path) -> tuple[Path, Path]:
        futures = futures_dir / "RELIANCE26AUGFUT_5minute.parquet"
        equity = equity_dir / "RELIANCE_stocks_indicators_1min.parquet"
        self._write_futures(futures, close=100.0)
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-08-11 09:25")],
                "open": [99.0],
                "high": [101.0],
                "low": [98.0],
                "close": [100.0],
                "volume": [1000],
            }
        ).to_parquet(equity, index=False)
        return futures, equity

    def _write_futures(self, path: Path, *, close: float) -> None:
        pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2026-08-11 09:25", tz="Asia/Kolkata")],
                "open": [99.0],
                "high": [101.0],
                "low": [98.0],
                "close": [close],
                "volume": [1000],
                "oi": [100_000],
            }
        ).to_parquet(path, index=False)

    def test_latest_pointer_is_only_resolved_to_canonical_dated_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_dir, _, _ = self._roots(root)
            dated = self._write_universe(universe_dir, _universe())
            with patch.object(common, "UNIVERSE_DIR", universe_dir):
                path, _, record = provenance.resolve_dated_universe()
                self.assertEqual(path, dated.resolve())
                self.assertEqual(record["master_date"], "2026-08-11")
                with self.assertRaisesRegex(ValueError, "may not consume mutable"):
                    provenance.resolve_dated_universe(
                        universe_path=universe_dir / "latest_near_month.parquet"
                    )

    def test_persisted_mapping_is_required_without_token_cache_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_dir, _, _ = self._roots(root)
            frame = _universe()
            frame.loc[0, "equity_instrument_token"] = None
            dated = self._write_universe(universe_dir, frame)
            with patch.object(common, "UNIVERSE_DIR", universe_dir):
                with self.assertRaisesRegex(ValueError, "refuses legacy"):
                    provenance.load_backtest_universe(
                        universe_path=dated,
                        universe_date="2026-08-11",
                        require_persisted_mapping=True,
                    )

    def test_expected_universe_hash_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_dir, _, _ = self._roots(root)
            dated = self._write_universe(universe_dir, _universe())
            with patch.object(common, "UNIVERSE_DIR", universe_dir):
                with self.assertRaisesRegex(AssertionError, "file hash changed"):
                    provenance.resolve_dated_universe(
                        universe_path=dated,
                        expected_file_sha256="0" * 64,
                    )

    def test_inventory_reuses_unchanged_hashes_and_rehashes_only_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _, futures_dir, equity_dir = self._roots(root)
            futures, _ = self._write_sources(futures_dir, equity_dir)
            mapped = _universe().iloc[[0]].copy()
            mapped["futures_lot_size"] = mapped["lot_size"]
            universe_record = {
                "file_sha256": "1" * 64,
                "universe_sha256": "2" * 64,
                "mapped_universe_sha256": "3" * 64,
                "mapped_symbol_set_sha256": "4" * 64,
            }
            real_hash = provenance.sha256_file
            with (
                patch.object(common, "RAW_CONTRACT_DIR", futures_dir),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_dir),
                patch.object(provenance, "sha256_file", wraps=real_hash) as hasher,
            ):
                first = provenance.build_source_inventory(mapped, universe_record)
                self.assertEqual(hasher.call_count, 2)
            self.assertEqual(first["missing_count"], 0)
            self.assertFalse(first["date_sliced"])
            self.assertIn("NOT DATE SLICED", first["inventory_scope"])

            with (
                patch.object(common, "RAW_CONTRACT_DIR", futures_dir),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_dir),
                patch.object(provenance, "sha256_file", wraps=real_hash) as hasher,
            ):
                second = provenance.build_source_inventory(
                    mapped, universe_record, previous_inventory=first
                )
                self.assertEqual(hasher.call_count, 0)
            self.assertEqual(first["source_fingerprint"], second["source_fingerprint"])

            futures.write_bytes(b"futures-source-v2-with-different-size")
            with (
                patch.object(common, "RAW_CONTRACT_DIR", futures_dir),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_dir),
                patch.object(provenance, "sha256_file", wraps=real_hash) as hasher,
            ):
                third = provenance.build_source_inventory(
                    mapped, universe_record, previous_inventory=second
                )
                self.assertEqual(hasher.call_count, 1)
            self.assertNotEqual(second["source_fingerprint"], third["source_fingerprint"])

    def test_inventory_aborts_if_source_changes_during_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _, futures_dir, equity_dir = self._roots(root)
            self._write_sources(futures_dir, equity_dir)
            mapped = _universe().iloc[[0]].copy()
            real_hash = provenance.sha256_file

            def hash_then_mutate(path: Path | str, **_: object) -> str:
                source = Path(path)
                digest = real_hash(source)
                source.write_bytes(source.read_bytes() + b"drift")
                return digest

            with (
                patch.object(common, "RAW_CONTRACT_DIR", futures_dir),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_dir),
                patch.object(provenance, "sha256_file", side_effect=hash_then_mutate),
            ):
                with self.assertRaisesRegex(RuntimeError, "changed while fingerprinting"):
                    provenance.build_source_inventory(
                        mapped,
                        {
                            "file_sha256": "1" * 64,
                            "universe_sha256": "2" * 64,
                            "mapped_universe_sha256": "3" * 64,
                            "mapped_symbol_set_sha256": "4" * 64,
                        },
                    )

    def test_immutable_json_refuses_different_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "run.json"
            provenance.write_immutable_json(path, {"value": 1})
            provenance.write_immutable_json(path, {"value": 1})
            with self.assertRaisesRegex(FileExistsError, "different contents"):
                provenance.write_immutable_json(path, {"value": 2})

    def test_immutable_copy_never_replaces_versioned_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "rolling.csv"
            target = root / "selected_v1.csv"
            source.write_bytes(b"version-one\n")
            expected = provenance.sha256_file(source)
            provenance.publish_immutable_copy(
                source, target, expected_sha256=expected
            )
            self.assertEqual(target.read_bytes(), b"version-one\n")
            source.write_bytes(b"version-two\n")
            with self.assertRaisesRegex(FileExistsError, "different contents"):
                provenance.publish_immutable_copy(source, target)
            self.assertEqual(target.read_bytes(), b"version-one\n")

    def test_cache_rebuilds_on_source_or_artifact_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_dir, futures_dir, equity_dir = self._roots(root)
            dated = self._write_universe(universe_dir, _universe())
            futures, _ = self._write_sources(futures_dir, equity_dir)
            cache_dir = root / "cache"
            manifest_path = cache_dir / "manifest.json"
            signals = pd.DataFrame({"sid": [0], "day": [date(2026, 8, 11)]})
            paths = {
                0: {
                    "high": np.array([1.0]),
                    "low": np.array([0.5]),
                    "close": np.array([0.8]),
                }
            }
            with (
                patch.object(common, "UNIVERSE_DIR", universe_dir),
                patch.object(common, "RAW_CONTRACT_DIR", futures_dir),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_dir),
                patch.object(optimizer, "CACHE_DIR", cache_dir),
                patch.object(optimizer, "CACHE_MANIFEST_PATH", manifest_path),
                patch.object(
                    optimizer.sw,
                    "build_signal_table",
                    return_value=(signals, paths),
                ) as build,
            ):
                _, _, first_manifest = optimizer.load_signals(
                    "1530",
                    400,
                    False,
                    universe_path=dated,
                    universe_date="2026-08-11",
                    require_persisted_mapping=True,
                    require_complete_sources=True,
                    return_provenance=True,
                )
                self.assertEqual(build.call_count, 1)

                self._write_futures(futures, close=100.25)
                _, _, second_manifest = optimizer.load_signals(
                    "1530",
                    400,
                    False,
                    universe_path=dated,
                    universe_date="2026-08-11",
                    require_persisted_mapping=True,
                    require_complete_sources=True,
                    return_provenance=True,
                )
                self.assertEqual(build.call_count, 2)
                self.assertNotEqual(
                    first_manifest["input_fingerprint"],
                    second_manifest["input_fingerprint"],
                )

                (cache_dir / "paths.npz").write_bytes(b"tampered")
                optimizer.load_signals(
                    "1530",
                    400,
                    False,
                    universe_path=dated,
                    universe_date="2026-08-11",
                    require_persisted_mapping=True,
                    require_complete_sources=True,
                )
                self.assertEqual(build.call_count, 3)

    def test_promoted_cache_refuses_missing_mapped_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_dir, futures_dir, equity_dir = self._roots(root)
            dated = self._write_universe(universe_dir, _universe())
            futures = futures_dir / "RELIANCE26AUGFUT_5minute.parquet"
            futures.write_bytes(b"only-futures-exists")
            with (
                patch.object(common, "UNIVERSE_DIR", universe_dir),
                patch.object(common, "RAW_CONTRACT_DIR", futures_dir),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_dir),
                patch.object(optimizer, "CACHE_DIR", root / "cache"),
                patch.object(
                    optimizer, "CACHE_MANIFEST_PATH", root / "cache" / "manifest.json"
                ),
            ):
                with self.assertRaisesRegex(FileNotFoundError, "requires every mapped"):
                    optimizer.load_signals(
                        "1530",
                        400,
                        False,
                        universe_path=dated,
                        require_persisted_mapping=True,
                        require_complete_sources=True,
                    )

    def test_promoted_cache_refuses_unreadable_or_invalid_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_dir, futures_dir, equity_dir = self._roots(root)
            dated = self._write_universe(universe_dir, _universe())
            self._write_sources(futures_dir, equity_dir)
            (equity_dir / "RELIANCE_stocks_indicators_1min.parquet").write_bytes(
                b"not-a-parquet"
            )
            with (
                patch.object(common, "UNIVERSE_DIR", universe_dir),
                patch.object(common, "RAW_CONTRACT_DIR", futures_dir),
                patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", equity_dir),
                patch.object(optimizer, "CACHE_DIR", root / "cache"),
                patch.object(
                    optimizer, "CACHE_MANIFEST_PATH", root / "cache" / "manifest.json"
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "unreadable, or invalid"):
                    optimizer.load_signals(
                        "1530",
                        400,
                        False,
                        universe_path=dated,
                        require_persisted_mapping=True,
                        require_complete_sources=True,
                    )

    def test_promoted_source_validation_refuses_zero_row_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "empty.parquet"
            pd.DataFrame(
                {
                    "date": pd.Series(dtype="datetime64[ns]"),
                    "open": pd.Series(dtype=float),
                    "high": pd.Series(dtype=float),
                    "low": pd.Series(dtype=float),
                    "close": pd.Series(dtype=float),
                    "volume": pd.Series(dtype=float),
                }
            ).to_parquet(path, index=False)
            inventory = {
                "entries": [
                    {
                        "role": "NSE_EQUITY_1M",
                        "logical_symbol": "EMPTY",
                        "resolved_path": str(path),
                        "exists": True,
                    }
                ]
            }
            with self.assertRaisesRegex(RuntimeError, "ZERO_ROWS"):
                provenance.validate_source_inventory_readable(inventory)

    def test_run_provenance_labels_current_whole_file_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output = root / "daily.csv"
            output.write_text("day,value\n2026-08-11,1\n", encoding="utf-8")
            cache_manifest = {
                "input_fingerprint": "a" * 64,
                "universe": {"master_date": "2026-08-11"},
                "source_inventory": {
                    "inventory_scope": "WHOLE_FILE_BYTES; NOT DATE SLICED"
                },
                "artifacts": {},
            }
            payload = provenance.build_run_provenance(
                generated_at=datetime(2026, 8, 18, 1, 2, 3),
                strategy_version="V6",
                objective="BEST_NET",
                strategy_payload={"setup": "frozen"},
                parameters={"through_day": "2026-08-11"},
                backtest_window={"last_session": "2026-08-11"},
                cache_manifest_path=root / "manifest.json",
                cache_manifest=cache_manifest,
                output_paths={"daily": output},
                results={"sessions": 1, "net_pct": 1.0},
            )
            self.assertFalse(payload["original_selection_source_provenance_available"])
            self.assertIn("NOT_ORIGINAL_SELECTION", payload["provenance_claim"])
            self.assertIn("NOT DATE SLICED", payload["source_inventory_scope"])
            self.assertEqual(payload["strategy_payload"], {"setup": "frozen"})
            self.assertEqual(payload["results"]["sessions"], 1)
            self.assertEqual(
                payload["backtest_input_fingerprint"],
                provenance.backtest_input_fingerprint(
                    cache_manifest,
                    strategy_payload=payload["strategy_payload"],
                    parameters=payload["parameters"],
                ),
            )
            self.assertEqual(
                payload["outputs"]["daily"]["sha256"],
                provenance.sha256_file(output),
            )


if __name__ == "__main__":
    unittest.main()
