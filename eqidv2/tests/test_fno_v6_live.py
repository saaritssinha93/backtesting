from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import fno_v6_live_config as config
import log_dashboard_server as dashboard


class FnoV6LiveTests(unittest.TestCase):
    def test_live_uses_exact_v6_best_net_setup_book(self) -> None:
        config.validate_strategy()
        self.assertEqual(config.SELECTED_OBJECTIVE, "BEST_NET")
        self.assertEqual(config.STRATEGY_VERSION, "FNO_V6_BEST_NET_CASH_EQUITY_20260811")
        self.assertEqual(len(config.ACTIVE_SETUPS), 10)
        self.assertEqual(
            {(setup.signal_end, setup.side) for setup in config.ACTIVE_SETUPS},
            {
                (slot, side)
                for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
                for side in ("LONG", "SHORT")
            },
        )

    def test_readiness_policy_is_locked_into_strategy_fingerprint(self) -> None:
        expected = {
            "fetch_marker_schema": "fno_oi_fetch_slot_v2",
            "policy": "verified_stock_no_candle_skip_v1",
            "universe": "MAPPED_STOCK_FUTURES_EXCLUDING_INDEX_FUTURES",
            "minimum_stock_coverage": 0.99,
            "maximum_verified_no_candle_stocks": 2,
            "minimum_no_candle_fetch_attempts": 3,
            "verified_no_candle_action": "SKIPPED_NO_CANDLE",
            "synthetic_or_forward_filled_futures_bars": False,
        }
        self.assertEqual(config.strategy_payload()["futures_readiness"], expected)

        fingerprint = config.strategy_fingerprint()
        with patch.object(config, "MAX_VERIFIED_NO_CANDLE_STOCKS", 3):
            self.assertNotEqual(config.strategy_fingerprint(), fingerprint)

    def test_durable_confirmation_feed_policy_is_locked_into_fingerprint(self) -> None:
        expected = {
            "schema": "fno_equity_1m_slot_v1",
            "policy": "candidate_exact_completed_1m_verified_no_candle_v1",
            "source": "DURABLE_COMPLETED_NSE_EQUITY_1M_FEED",
            "candidate_set_and_scanner_snapshot_hashed": True,
            "immutable_slot_bar_snapshot": True,
            "confirmation_is_read_only_consumer": True,
            "activation_deadline_sec": 90,
            "completed_candle_boundary_buffer_sec": 3.0,
            "candidate_resolution_policy": "ALL_WRITTEN_OR_VERIFIED_NO_CANDLE",
            "minimum_no_candle_observations": 3,
            "minimum_no_candle_verification_age_sec": 15,
            "no_candle_observation_spacing_sec": 2.0,
            "verified_no_candle_action": "INELIGIBLE_NO_CANDLE",
            "verified_no_candle_cap": None,
            "written_bar_minimum_ratio": None,
        }
        self.assertEqual(config.strategy_payload()["confirmation_feed"], expected)

        fingerprint = config.strategy_fingerprint()
        with patch.object(config, "CONFIRMATION_NO_CANDLE_MIN_AGE_SEC", 16):
            self.assertNotEqual(config.strategy_fingerprint(), fingerprint)
        with patch.object(config, "CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC", 2.0):
            self.assertNotEqual(config.strategy_fingerprint(), fingerprint)

    def test_v6_confirmation_runtime_rejects_boundary_buffer_drift(self) -> None:
        env = dict(os.environ)
        env["FNO_LIVE_GENERATION"] = "v6"
        command = (
            "import fno_v5_live as live; "
            "args=live.build_parser().parse_args(["
            "'--role','confirmation-1m','--once','--boundary-buffer-sec','2']); "
            "\ntry: live.run(args)\n"
            "except ValueError as exc: "
            " assert 'fingerprint-locked to 3.0 seconds' in str(exc)\n"
            "else: raise AssertionError('boundary drift was accepted')"
        )
        completed = subprocess.run(
            [sys.executable, "-c", command],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_backtest_source_identity_is_honest_and_frozen(self) -> None:
        source = config.strategy_payload()["backtest_source_identity"]
        self.assertEqual(source["universe_date"], "2026-08-11")
        self.assertEqual(source["universe_path_name"], "near_month_2026-08-11.parquet")
        self.assertEqual(source["mapping_source"], "PERSISTED_DATED_UNIVERSE_ONLY")
        self.assertTrue(source["complete_mapped_sources_required"])
        self.assertFalse(source["original_selection_source_provenance_available"])
        self.assertTrue(source["current_source_replay_provenance_pinned"])
        self.assertEqual(source["replay_revision"], "20260818_V1")
        self.assertIn("NOT_ORIGINAL_SELECTION", source["provenance_claim"])
        self.assertEqual(source["inventory_scope"], "WHOLE_SOURCE_FILES_NOT_DATE_SLICED")
        self.assertEqual(
            source["promoted_backtest_input_fingerprint"],
            config.PROTECTED_SELECTED_INPUT_FINGERPRINT,
        )

    def test_v6_live_backtest_attestation(self) -> None:
        observed = config.attest_selected_backtest()

        self.assertNotEqual(
            config.SELECTED_DAILY_PATH.resolve(),
            config.ROLLING_DAILY_PATH.resolve(),
        )
        self.assertEqual(
            hashlib.sha256(config.SELECTED_DAILY_PATH.read_bytes()).hexdigest(),
            config.PROTECTED_SELECTED_DAILY_SHA256,
        )
        self.assertEqual(observed["sessions"], 53)
        self.assertEqual(observed["orders"], 210)
        self.assertEqual(observed["fills"], 209)
        self.assertAlmostEqual(observed["trade_pf"], 2.811435, places=6)
        self.assertAlmostEqual(observed["day_pf"], 6.061864, places=6)
        self.assertAlmostEqual(observed["net_pct"], 146.710895, places=6)
        self.assertEqual(
            observed["backtest_input_fingerprint"],
            config.PROTECTED_SELECTED_INPUT_FINGERPRINT,
        )
        self.assertNotEqual(
            config.SELECTED_DAILY_PATH.resolve(),
            config.LEGACY_SELECTED_DAILY_PATH.resolve(),
        )
        self.assertEqual(
            hashlib.sha256(config.LEGACY_SELECTED_DAILY_PATH.read_bytes()).hexdigest(),
            config.LEGACY_SELECTED_DAILY_SHA256,
        )

    def test_selected_provenance_attestation_rejects_missing_or_tampered(self) -> None:
        protected = config.attest_selected_backtest_provenance()
        self.assertEqual(
            protected["backtest_input_fingerprint"],
            config.PROTECTED_SELECTED_INPUT_FINGERPRINT,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(FileNotFoundError):
                config.attest_selected_backtest_provenance(
                    Path(temp_dir) / "missing.json"
                )
            tampered = Path(temp_dir) / "selected.provenance.json"
            tampered.write_text('{"schema_version":"tampered"}\n', encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "hash changed"):
                config.attest_selected_backtest_provenance(tampered)

            payload = {
                "schema_version": "fno_backtest_run_provenance_v1",
                "provenance_claim": (
                    "RECREATED_CURRENT_SOURCE_REPLAY_NOT_ORIGINAL_SELECTION_PROVENANCE"
                ),
                "original_selection_source_provenance_available": False,
                "strategy_version": config.STRATEGY_VERSION,
                "objective": config.SELECTED_OBJECTIVE,
                "backtest_input_fingerprint": "a" * 64,
            }
            wrong_input = Path(temp_dir) / "wrong-input.provenance.json"
            wrong_input.write_text(json.dumps(payload), encoding="utf-8")
            with (
                patch.object(
                    config,
                    "PROTECTED_SELECTED_PROVENANCE_SHA256",
                    hashlib.sha256(wrong_input.read_bytes()).hexdigest(),
                ),
                patch.object(
                    config, "PROTECTED_SELECTED_INPUT_FINGERPRINT", "b" * 64
                ),
            ):
                with self.assertRaisesRegex(AssertionError, "input fingerprint changed"):
                    config.attest_selected_backtest_provenance(wrong_input)

    def test_v6_runtime_loads_in_a_clean_process_with_isolated_paths(self) -> None:
        env = dict(os.environ)
        env["FNO_LIVE_GENERATION"] = "v6"
        command = (
            "import fno_v5_live as live; "
            "assert live.LIVE_GENERATION == 'v6'; "
            "assert live.config.SELECTED_OBJECTIVE == 'BEST_NET'; "
            "assert live.LIVE_ROOT.name == 'v6_live'; "
            "assert all('fno_v6_' in value for value in live.ROLE_SESSIONS.values())"
        )
        completed = subprocess.run(
            [sys.executable, "-c", command],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_dashboard_exposes_only_promoted_v6_fno_execution_cards(self) -> None:
        promoted = {
            "fno_v6_scanner_5min",
            "fno_v6_confirmation_1min",
            "fno_v6_live_long",
            "fno_v6_live_short",
            "fno_v6_trade_logger",
            "fno_v6_net_result",
        }
        replaced = {value.replace("v6", "v5") for value in promoted}

        self.assertTrue(promoted.issubset(dashboard.FNO_OI_CARD_REPORTS))
        self.assertTrue(promoted.issubset(dashboard.RESTARTABLE_CARDS))
        self.assertTrue(replaced.isdisjoint(dashboard.FNO_OI_CARD_REPORTS))
        for card_id in promoted:
            self.assertIn("run_fno_v6_", dashboard.RESTARTABLE_CARDS[card_id])


if __name__ == "__main__":
    unittest.main()
