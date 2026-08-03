from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import avwap_5min_ID_v11_backtesting as v11
import eqidv2_entry_engine_1min_v5_id as entry
import eqidv2_pre_momentum as shared_pre
import eqidv2_signal_discovery_v7_5min_id_persistent as scanner
import eqidv2_v11_live_overlay as overlay
from eqidv2_runtime_manifest import (
    assert_final_setup_conf_contract,
    freeze_runtime_manifest,
)


IST = "Asia/Kolkata"


def _raw_1m() -> pd.DataFrame:
    dates = pd.date_range("2026-07-28 09:15", periods=40, freq="min", tz=IST)
    base = np.linspace(100.0, 102.0, len(dates))
    return pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.05,
            "high": base + 0.10,
            "low": base - 0.10,
            "close": base,
            "volume": np.arange(len(dates)) + 1000,
            "ticker": "TEST",
        }
    )


def _candidate(slot: pd.Timestamp) -> dict:
    signal_time = slot.isoformat()
    return {
        "candidate_id": f"TEST|LONG|A_MOD_BREAK_C1_HIGH|{signal_time}",
        "ticker": "TEST",
        "side": "LONG",
        "setup": "A_MOD_BREAK_C1_HIGH",
        "signal_time_ist": signal_time,
        "signal_open": 101.0,
        "signal_high": 102.0,
        "signal_low": 100.8,
        "signal_close": 101.8,
        "signal_volume": 5000,
        "signal_adx": 30.0,
        "signal_rsi": 62.0,
        "signal_vol_ratio20": 1.5,
        "vol_ratio": 1.5,
        "quality_score": 200.0,
    }


class CausalParityTests(unittest.TestCase):
    def test_overlay_merge_uses_replacement_not_union(self):
        universe = sorted(overlay.v11_override_setup_universe(overlay.DEFAULT_SELECTED_STRATEGY_PROFILE))
        self.assertTrue(universe)
        setup = universe[0]
        v7 = pd.DataFrame(
            [
                {
                    "candidate_id": f"X|LONG|{setup}|2026-07-28 10:30:00+05:30",
                    "ticker": "X",
                    "side": "LONG",
                    "setup": setup,
                    "signal_time_ist": "2026-07-28 10:30:00+05:30",
                    "quality_score": 100.0,
                }
            ]
        )
        merged = overlay.merge_v7_and_v11_candidates(
            v7,
            pd.DataFrame(),
            profile=overlay.DEFAULT_SELECTED_STRATEGY_PROFILE,
        )
        self.assertTrue(merged.empty)

    def test_shared_features_are_used_by_live_and_v11(self):
        raw = _raw_1m()
        slot = pd.Timestamp("2026-07-28 09:30", tz=IST)
        candidate = _candidate(slot)
        entry_row = {
            **candidate,
            "entry_price": 101.80,
            "sl_price": 101.00,
            "entry_time_ist": "2026-07-28 09:31:00+05:30",
            "pre_momentum_cutoff_ist": "2026-07-28 09:31:00+05:30",
        }
        live_features, live_reason = entry._pre_entry_momentum_features(
            entry_row, {"TEST": raw}
        )
        bars_indexed = shared_pre.normalise_1m_bars(raw).set_index("date")
        with (
            patch.object(v11, "_entry_bars_for_signal", return_value=(bars_indexed, "immutable_slot_raw_1min")),
            patch.object(v11, "_V11_EXACT_LIVE_PARITY", True),
        ):
            bt_features, bt_reason = v11._pre_entry_momentum_features_v11(
                "TEST",
                "LONG",
                101.80,
                101.00,
                pd.Timestamp("2026-07-28 09:31", tz=IST),
                slot,
                candidate=candidate,
            )
        self.assertEqual(live_reason, "")
        self.assertEqual(bt_reason, "")
        self.assertEqual(set(live_features), set(bt_features))
        for key in live_features:
            if np.isnan(live_features[key]) and np.isnan(bt_features[key]):
                continue
            self.assertAlmostEqual(live_features[key], bt_features[key], places=12)

    def test_causal_entry_never_backdates_before_decision(self):
        idx = pd.date_range("2026-07-28 10:30", periods=5, freq="min", tz=IST)
        bars = pd.DataFrame({"open": [100, 101, 102, 103, 104]}, index=idx)
        result = v11._first_1m_entry(
            bars,
            pd.Timestamp("2026-07-28 10:30", tz=IST),
            max_delay_minutes=3,
            decision_ready_at=pd.Timestamp("2026-07-28 10:31:05", tz=IST),
        )
        self.assertIsNotNone(result)
        self.assertEqual(result[0], pd.Timestamp("2026-07-28 10:32", tz=IST))
        self.assertEqual(result[1], 102.0)

    def test_entry_engine_waits_for_exact_slot_marker_and_never_uses_latest(self):
        slot = pd.Timestamp("2026-07-28 10:30", tz=IST)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            json_dir = root / "json"
            latest_dir = root / "latest"
            json_dir.mkdir()
            latest_dir.mkdir()
            slot_path = json_dir / "candidate_tickers_20260728_1030.json"
            payload = {
                "slot_ist": slot.isoformat(),
                "candidates": [_candidate(slot)],
            }
            slot_path.write_text(json.dumps(payload), encoding="utf-8")
            stale = {
                "slot_ist": "2026-07-28T10:25:00+05:30",
                "candidates": [{**_candidate(slot), "ticker": "STALE"}],
            }
            (latest_dir / "latest_candidate_tickers.json").write_text(
                json.dumps(stale), encoding="utf-8"
            )
            with (
                patch.object(entry, "SIGNAL_DISCOVERY_ROOT", root),
                patch.object(entry, "USE_SLOT_CANDIDATE_JSON", True),
                patch.object(entry, "REQUIRE_SLOT_COMPLETE_MARKER", True),
            ):
                pending = entry._load_candidates_for_slot(slot)
                self.assertTrue(pending.empty)

                marker = {
                    "complete": True,
                    "slot_ist": slot.isoformat(),
                    "decision_ready_at_ist": "2026-07-28T10:31:05+05:30",
                    "candidate_json_sha256": hashlib.sha256(slot_path.read_bytes()).hexdigest(),
                    "runtime_manifest_path": "manifest.json",
                }
                (json_dir / "slot_complete_20260728_1030.json").write_text(
                    json.dumps(marker), encoding="utf-8"
                )
                exact = entry._load_candidates_for_slot(slot)
            self.assertEqual(len(exact), 1)
            self.assertEqual(exact.iloc[0]["ticker"], "TEST")
            self.assertEqual(
                exact.iloc[0]["decision_ready_at_ist"],
                "2026-07-28T10:31:05+05:30",
            )

    def test_scanner_marker_hashes_final_snapshot(self):
        slot = pd.Timestamp("2026-07-28 10:30", tz=IST)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            json_dir = root / "json"
            latest_dir = root / "latest"
            json_dir.mkdir()
            latest_dir.mkdir()
            frame = pd.DataFrame([_candidate(slot)])
            with (
                patch.object(scanner, "JSON_DIR", json_dir),
                patch.object(scanner, "LATEST_DIR", latest_dir),
                patch.object(scanner, "_RUNTIME_MANIFEST_PATH", "manifest.json"),
            ):
                scanner._write_json_snapshots(frame, slot)
                marker_path = scanner._write_slot_complete_marker(
                    slot,
                    decision_ready_at_ist="2026-07-28T10:31:05+05:30",
                    candidate_count=1,
                )
            marker = json.loads(marker_path.read_text(encoding="utf-8"))
            final_path = json_dir / "candidate_tickers_20260728_1030.json"
            self.assertTrue(marker["complete"])
            self.assertEqual(
                marker["candidate_json_sha256"],
                hashlib.sha256(final_path.read_bytes()).hexdigest(),
            )

    def test_manifest_conf_contract_and_frozen_source_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(
                os.environ,
                {
                    "EQIDV2_USE_FINAL_SETUP_CONF": "1",
                    "EQIDV2_FINAL_SETUP_CONF_MODULE": "final_setup_conf_v11_working",
                    "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE": "final_setup_conf_v11_working",
                    "EQIDV2_LAUNCHER_NAME": "unit_test",
                },
                clear=False,
            ):
                path, payload = freeze_runtime_manifest(
                    "unit_test_component",
                    runtime_root=tmp,
                    source_files=[Path(v11.__file__)],
                    resolved_config={"mode": "test"},
                )
                self.assertTrue(path.exists())
                self.assertGreater(payload["final_setup_conf_contract"]["setup_count"], 0)
                self.assertEqual(len(payload["source_files"][0]["sha256"]), 64)
                self.assertEqual(assert_final_setup_conf_contract()["module"], "final_setup_conf_v11_working")

            with patch.dict(
                os.environ,
                {
                    "EQIDV2_USE_FINAL_SETUP_CONF": "1",
                    "EQIDV2_FINAL_SETUP_CONF_MODULE": "final_setup_conf_v11_working",
                    "EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE": "wrong_module",
                },
                clear=False,
            ):
                with self.assertRaises(RuntimeError):
                    assert_final_setup_conf_contract()


if __name__ == "__main__":
    unittest.main()
