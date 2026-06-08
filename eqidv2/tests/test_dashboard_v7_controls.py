from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import eqidv2_signal_discovery_v7_5min_id_persistent as scanner
import log_dashboard_server as dashboard


class DashboardV7ControlsTests(unittest.TestCase):
    def test_active_v7_sessions_are_restartable(self) -> None:
        expected = {
            "signal_discovery_v7_5min_id",
            "entry_engine_1min_v5_id",
            "v7_research_layer",
            "daily_live_v7_research_session",
            "v7_pre_momentum_filter_analyst",
        }
        self.assertTrue(expected.issubset(dashboard.RESTARTABLE_CARDS))
        self.assertNotIn("kite_trade_id_5min_v7", dashboard.RESTARTABLE_CARDS)

    def test_v7_id_kill_switch_paths_match_executors(self) -> None:
        today = "2026-06-08"
        live_state, live_command = dashboard._kill_switch_scope_paths(
            "false_id_5min_v7", today
        )
        paper_state, paper_command = dashboard._kill_switch_scope_paths(
            "true_id_5min_v7", today
        )

        self.assertEqual(
            live_state.name, f"open_live_trades_state_{today}_id_5min_v7.json"
        )
        self.assertEqual(live_command.name, "kill_switch_false_id_5min_v7.json")
        self.assertEqual(
            paper_state.name, f"open_trades_state_{today}_id_5min_v7.json"
        )
        self.assertEqual(paper_command.name, "kill_switch_true_id_5min_v7.json")

    def test_restart_identity_tracks_direct_worker_pid(self) -> None:
        first = dashboard._restart_identity_key(
            {"worker_pid": "100", "worker_start_ts": "2026-06-08_09:00:00"}
        )
        second = dashboard._restart_identity_key(
            {"worker_pid": "200", "worker_start_ts": "2026-06-08_11:00:00"}
        )
        self.assertNotEqual(first, second)

    def test_feed_gate_accepts_live_loop_datetime(self) -> None:
        self.assertEqual(scanner.ENTRY_SIGNAL_TO_ENTRY_LAG_MIN, 1)
        slot = scanner.base_v15.IST.localize(datetime(2026, 6, 8, 11, 15))
        with tempfile.TemporaryDirectory() as temp_dir:
            status_path = Path(temp_dir) / "feed_status.json"
            status_path.write_text(
                json.dumps(
                    {
                        "slot_ist": "2026-06-08 11:15:00+0530",
                        "overall_state": "OK",
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.object(scanner, "FEED_STATUS_JSON", status_path),
                patch.object(scanner, "FEED_GATE_MAX_WAIT_SEC", 1),
                patch.object(scanner, "FEED_GATE_MIN_DELAY_SEC", 0),
                patch.object(scanner, "FEED_GATE_POLL_SEC", 0.01),
            ):
                self.assertTrue(scanner._wait_for_feed_slot(slot))

    def test_restart_never_launches_while_old_worker_survives(self) -> None:
        commands: list[list[str]] = []

        def fake_run(command: list[str], timeout: float):
            commands.append(command)
            return 0, ""

        with (
            patch.object(dashboard, "_read_restart_identity", return_value={}),
            patch.object(dashboard, "_run_cmd_silent", side_effect=fake_run),
            patch.object(
                dashboard,
                "_collect_restart_candidate_pids",
                side_effect=[([101], {}, set()), ([101], {}, set()), ([101], {}, set())],
            ),
            patch.object(dashboard, "_kill_pid_tree"),
            patch.object(dashboard, "_wait_for_pids_exit", side_effect=[[101], [101], [101]]),
        ):
            result = dashboard._restart_card_session("signal_discovery_v7_5min_id")

        self.assertFalse(result["ok"])
        self.assertEqual(
            [command for command in commands if "/Run" in command],
            [],
        )


if __name__ == "__main__":
    unittest.main()
