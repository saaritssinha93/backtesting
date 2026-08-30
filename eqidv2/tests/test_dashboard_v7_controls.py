from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import eqidv2_signal_discovery_v7_5min_id_persistent as scanner
import log_dashboard_server as dashboard


class DashboardV7ControlsTests(unittest.TestCase):
    def test_legacy_one_line_status_is_parsed_into_individual_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "legacy.status"
            path.write_text(
                "status=STOPPED script=worker.py ts=2026-06-10_06:28:25 "
                "reason=stale_manual_override\n",
                encoding="utf-8",
            )
            parsed = dashboard.parse_status_file(path)

        self.assertEqual(parsed["status"], "STOPPED")
        self.assertEqual(parsed["script"], "worker.py")
        self.assertEqual(parsed["ts"], "2026-06-10_06:28:25")
        self.assertEqual(parsed["reason"], "stale_manual_override")

    def test_task_snapshot_timeout_keeps_last_known_good_snapshot(self) -> None:
        previous_cache = dashboard._TASK_SNAPSHOT_CACHE
        previous_cache_at = dashboard._TASK_SNAPSHOT_CACHE_AT
        known = {
            "\\EQIDV2_v11_lab_shadow_monitor_1655": {
                "Scheduled Task State": "Enabled",
                "Status": "Ready",
            }
        }
        try:
            dashboard._TASK_SNAPSHOT_CACHE = dict(known)
            dashboard._TASK_SNAPSHOT_CACHE_AT = None
            with patch.object(
                dashboard.subprocess,
                "run",
                side_effect=dashboard.subprocess.TimeoutExpired("schtasks", 20),
            ):
                observed = dashboard.load_task_scheduler_snapshot(force=True)
        finally:
            dashboard._TASK_SNAPSHOT_CACHE = previous_cache
            dashboard._TASK_SNAPSHOT_CACHE_AT = previous_cache_at

        self.assertEqual(observed, known)

    def test_stale_task_snapshot_returns_immediately_and_refreshes_in_background(self) -> None:
        previous_cache = dashboard._TASK_SNAPSHOT_CACHE
        previous_cache_at = dashboard._TASK_SNAPSHOT_CACHE_AT
        previous_refreshing = dashboard._TASK_SNAPSHOT_REFRESHING
        known = {
            "\\EQIDV2_fno_v6_scanner_5min_0915": {
                "Scheduled Task State": "Enabled",
                "Status": "Ready",
            }
        }
        try:
            dashboard._TASK_SNAPSHOT_CACHE = dict(known)
            dashboard._TASK_SNAPSHOT_CACHE_AT = None
            dashboard._TASK_SNAPSHOT_REFRESHING = False
            with patch.object(dashboard, "_query_task_scheduler_snapshot") as query:
                with patch.object(dashboard.threading, "Thread") as thread_factory:
                    observed = dashboard.load_task_scheduler_snapshot(force=False)

            self.assertEqual(observed, known)
            query.assert_not_called()
            thread_factory.assert_called_once()
            thread_factory.return_value.start.assert_called_once()
            self.assertTrue(dashboard._TASK_SNAPSHOT_REFRESHING)
        finally:
            dashboard._TASK_SNAPSHOT_CACHE = previous_cache
            dashboard._TASK_SNAPSHOT_CACHE_AT = previous_cache_at
            dashboard._TASK_SNAPSHOT_REFRESHING = previous_refreshing

    def test_newer_session_heartbeat_supersedes_old_success(self) -> None:
        merged = dashboard.merge_runtime_status(
            {
                "status": "SUCCESS",
                "ts": "2026-08-27T09:46:15+05:30",
                "phase": "SLOT_DONE",
                "slot": "09:45",
            },
            {
                "state": "WAITING",
                "ts": "2026-08-31T09:50:00+05:30",
                "phase": "WAIT_SCANNER",
                "slot": "09:45",
                "_file_age_sec": 10_000,
            },
        )

        self.assertEqual(merged["status"], "WAITING")
        self.assertEqual(merged["phase"], "WAIT_SCANNER")
        self.assertEqual(merged["ts"], "2026-08-31T09:50:00+05:30")
        self.assertEqual(merged["previous_status"], "SUCCESS")
        self.assertEqual(
            merged["status_scope"], "newer_heartbeat_supersedes_prior_session"
        )

    def test_active_v7_sessions_are_restartable(self) -> None:
        expected = {
            "signal_discovery_v7_5min_id",
            "entry_engine_1min_v5_id",
            "paper_trade_id_5min_v7",
            "v7_research_layer",
            "daily_live_v7_research_session",
            "v7_pre_momentum_filter_analyst",
        }
        self.assertTrue(expected.issubset(dashboard.RESTARTABLE_CARDS))
        self.assertNotIn("kite_trade_id_5min_v7", dashboard.RESTARTABLE_CARDS)
        self.assertEqual(
            dashboard.RESTARTABLE_CARDS["signal_discovery_v7_5min_id"],
            "run_conf_paper_signal_discovery.bat",
        )
        self.assertEqual(
            dashboard.RESTARTABLE_CARDS["entry_engine_1min_v5_id"],
            "run_conf_paper_entry_engine.bat",
        )
        self.assertEqual(
            dashboard.RESTARTABLE_CARDS["paper_trade_id_5min_v7"],
            "run_conf_paper_executor.bat",
        )

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

    def test_v7_monitor_tolerates_verification_only_fetch_marker_failure(self) -> None:
        state, ok, reasons = dashboard._v7_monitor_fetch_verdict(
            {
                "fetch_marker_seen": True,
                "fetch_complete": False,
                "fetch_failed": 1,
                "fetch_verify_failed": 1,
            },
            fetch_lag=49.0,
            due_fetch=True,
        )

        self.assertEqual(state, "YES")
        self.assertTrue(ok)
        self.assertIn("fetch_verify_tolerated=1/", ";".join(reasons))
        self.assertNotIn("fetch_marker_missing", reasons)

    def test_v7_monitor_blocks_non_verification_fetch_marker_failure(self) -> None:
        state, ok, reasons = dashboard._v7_monitor_fetch_verdict(
            {
                "fetch_marker_seen": True,
                "fetch_complete": False,
                "fetch_failed": 2,
                "fetch_verify_failed": 1,
            },
            fetch_lag=49.0,
            due_fetch=True,
        )

        self.assertEqual(state, "NO")
        self.assertFalse(ok)
        self.assertIn("fetch_marker_incomplete fail=2,verify=1", reasons)

    def test_restart_never_launches_while_old_worker_survives(self) -> None:
        commands: list[list[str]] = []

        def fake_run(command: list[str], timeout: float):
            commands.append(command)
            return 0, ""

        with (
            patch.object(
                dashboard,
                "_fresh_task_restart_eligibility",
                return_value=(True, "ENABLED"),
            ),
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

    def test_newer_same_day_auth_session_clears_stale_runner_failure(self) -> None:
        now = datetime(2026, 7, 27, 11, 30, tzinfo=dashboard.IST)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_path = root / "auth_v2_state.json"
            token_path = root / "access_token.txt"
            state_path.write_text(
                json.dumps(
                    {
                        "session_date_ist": "2026-07-27",
                        "updated_at_ist": "2026-07-27 11:07:01+0530",
                    }
                ),
                encoding="utf-8",
            )
            token_path.write_text("non-empty-token", encoding="utf-8")
            recovered_ts = datetime(2026, 7, 27, 11, 7).timestamp()
            os.utime(token_path, (recovered_ts, recovered_ts))

            with (
                patch.object(dashboard, "AUTH_V2_STATE_FILE", state_path),
                patch.object(dashboard, "AUTH_V2_ACCESS_TOKEN_FILE", token_path),
            ):
                status = dashboard.reconcile_authentication_status(
                    {
                        "status": "FAILED",
                        "ts": "2026-07-27_09:05:57",
                        "exit_code": "1",
                    },
                    now_ist=now,
                )

        self.assertEqual(status["status"], "SUCCESS")
        self.assertEqual(status["previous_status"], "FAILED")
        self.assertEqual(
            status["recovery_source"],
            "newer_same_day_auth_state_and_access_token",
        )

    def test_newer_fno_worker_success_preserves_supervisor_failure_as_recovered(self) -> None:
        now = datetime(2026, 8, 12, 18, 0, tzinfo=dashboard.IST)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "fno_oi_fetch_5min.status").write_text(
                "\n".join(
                    (
                        "status=SUCCESS",
                        "ts=2026-08-12T17:58:00+05:30",
                        "phase=SLOT_DONE",
                        "output=recovered-marker.json",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.object(dashboard, "RUNTIME_STATUS_DIR", root):
                status = dashboard.reconcile_fno_worker_recovery(
                    "fno_oi_fetch_5min",
                    {
                        "status": "FAILED",
                        "ts": "2026-08-12_09:24:05",
                        "reason": "max_restarts_exceeded",
                    },
                    now_ist=now,
                )

        self.assertEqual(status["status"], "RECOVERED")
        self.assertEqual(status["previous_status"], "FAILED")
        self.assertEqual(status["previous_reason"], "max_restarts_exceeded")
        self.assertEqual(status["recovery_phase"], "SLOT_DONE")
        self.assertEqual(status["recovery_source"], "newer_same_day_worker_success")
        source = Path(dashboard.__file__).read_text(encoding="utf-8")
        self.assertIn('"RECOVERED", "WAITING", "PARTIAL"', source)

    def test_prior_day_failure_is_scheduled_before_todays_run(self) -> None:
        task_snapshot = {
            "\\EQIDV2_fno_oi_eod_qc_1540": {
                "Scheduled Task State": "Enabled",
                "Status": "Ready",
                "Next Run Time": "13-08-2026 15:40:00",
            }
        }
        now = datetime(2026, 8, 13, 9, 30, tzinfo=dashboard.IST)
        status = dashboard.apply_scheduler_status(
            "fno_oi_eod_qc",
            {
                "status": "FAILED",
                "ts": "2026-08-12T15:40:01+05:30",
                "phase": "FAILED",
                "error": "stale universe",
            },
            task_snapshot,
            now_ist=now,
        )
        status = dashboard.apply_scheduler_status(
            "fno_oi_eod_qc", status, task_snapshot, now_ist=now
        )

        self.assertEqual(status["status"], "SCHEDULED")
        self.assertEqual(status["previous_status"], "FAILED")
        self.assertEqual(status["previous_error"], "stale universe")
        self.assertEqual(status["phase"], "WAIT_SCHEDULE")
        self.assertEqual(status["previous_phase"], "FAILED")
        self.assertEqual(status["status_scope"], "awaiting_today_scheduled_run")

    def test_prior_day_failure_with_ts_ist_is_scheduled_before_todays_run(self) -> None:
        task_snapshot = {
            "\\EQIDV2_v11_lab_shadow_monitor_1655": {
                "Scheduled Task State": "Enabled",
                "Status": "Ready",
                "Next Run Time": "31-08-2026 16:55:00",
            }
        }
        old_timestamp = "2026-08-28T16:56:01.414738+05:30"
        status = dashboard.apply_scheduler_status(
            "v11_lab_shadow_monitor",
            {
                "status": "ERROR",
                "ts_ist": old_timestamp,
                "phase": "FAILED",
                "error": "data verify failed",
            },
            task_snapshot,
            now_ist=datetime(2026, 8, 31, 12, 30, tzinfo=dashboard.IST),
        )

        self.assertEqual(status["status"], "SCHEDULED")
        self.assertEqual(status["previous_status"], "ERROR")
        self.assertEqual(status["previous_status_ts"], old_timestamp)
        self.assertEqual(status["previous_error"], "data verify failed")
        self.assertEqual(status["phase"], "WAIT_SCHEDULE")
        self.assertEqual(status["status_scope"], "awaiting_today_scheduled_run")

    def test_blocked_fail_closed_state_is_a_watch_state_in_dashboard(self) -> None:
        source = Path(dashboard.__file__).read_text(encoding="utf-8")
        self.assertIn(
            's === "BLOCKED" && ["INCOMPLETE_BY_DEADLINE", "UPSTREAM_BLOCKED"].includes(p)',
            source,
        )
        self.assertIn(
            'if (isFailClosedWatch(s, phase)) return "warn"',
            source,
        )
        self.assertNotIn(
            '"PARTIAL", "BLOCKED", "BLOCKED_STALE_ACTIVATION"].includes(s)',
            source,
        )

    def test_same_day_failure_remains_failed(self) -> None:
        status = dashboard.apply_scheduler_status(
            "fno_v6_scanner_5min",
            {"status": "FAILED", "ts": "2026-08-13T09:15:02+05:30"},
            {
                "\\EQIDV2_fno_v6_scanner_5min_0918": {
                    "Scheduled Task State": "Enabled",
                    "Status": "Ready",
                    "Next Run Time": "14-08-2026 09:15:00",
                }
            },
            now_ist=datetime(2026, 8, 13, 9, 30, tzinfo=dashboard.IST),
        )

        self.assertEqual(status["status"], "FAILED")

    def test_disabled_task_does_not_hide_manually_running_worker(self) -> None:
        with patch.object(dashboard, "_pid_is_alive_fast", return_value=True):
            status = dashboard.apply_scheduler_status(
                "eod_1min_data",
                {
                    "status": "RUNNING",
                    "heartbeat_state": "RUNNING",
                    "worker_pid": "39528",
                },
                {
                    "\\EQIDV2_eod_1min_data_0915": {
                        "Scheduled Task State": "Disabled",
                        "Status": "Disabled",
                        "Next Run Time": "01-09-2026 09:15:00",
                    }
                },
                now_ist=datetime(2026, 8, 31, 15, 10, tzinfo=dashboard.IST),
            )

        self.assertEqual(status["status"], "RUNNING")
        self.assertEqual(status["scheduler_state"], "DISABLED")
        self.assertEqual(status["scheduler_status"], "DISABLED")
        self.assertEqual(status["runtime_start_mode"], "MANUAL")
        self.assertEqual(status["scheduler_attention"], "DISABLED_WHILE_RUNNING")
        self.assertIn("automatic scheduled start is disabled", status["derived_status"])

    def test_disabled_task_with_dead_worker_is_disabled_but_section_locked(self) -> None:
        with patch.object(dashboard, "_pid_is_alive_fast", return_value=False):
            status = dashboard.apply_scheduler_status(
                "eod_1min_data",
                {"status": "RUNNING", "worker_pid": "39528"},
                {
                    "\\EQIDV2_eod_1min_data_0915": {
                        "Scheduled Task State": "Disabled",
                        "Status": "Disabled",
                    }
                },
                now_ist=datetime(2026, 8, 31, 15, 20, tzinfo=dashboard.IST),
            )

        self.assertEqual(status["status"], "DISABLED")
        source = Path(dashboard.__file__).read_text(encoding="utf-8")
        locked_block = source.split("const SECTION_LOCKED_DISABLED_IDS", 1)[1].split("]);", 1)[0]
        self.assertIn('"kiteticker_5min_data"', locked_block)
        self.assertIn('"eod_1min_data"', locked_block)


if __name__ == "__main__":
    unittest.main()
