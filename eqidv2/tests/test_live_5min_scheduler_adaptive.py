import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import eqidv2_eod_scheduler_for_5mins_data_live_minimal as scheduler


class Live5MinSchedulerAdaptiveTests(unittest.TestCase):
    def test_failed_session_validation_is_retried_with_unchanged_auth_files(self):
        app_name = "app1"
        kite = Mock()
        kite.profile.side_effect = [
            RuntimeError("temporary profile timeout"),
            {"user_id": "AB1234"},
        ]
        setup = Mock(return_value=kite)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "api_key.txt").write_text("key secret", encoding="utf-8")
            (root / "access_token.txt").write_text("token", encoding="utf-8")
            scheduler._APP_VALIDATION_CACHE.pop(app_name, None)
            try:
                with patch.object(scheduler, "EQIDV2_DIR", root):
                    first = scheduler._validate_app_session(app_name, setup)
                    second = scheduler._validate_app_session(app_name, setup)
                    cached_success = scheduler._validate_app_session(app_name, setup)
            finally:
                scheduler._APP_VALIDATION_CACHE.pop(app_name, None)

        self.assertEqual(first, (False, "temporary profile timeout"))
        self.assertEqual(second, (True, "AB1234"))
        self.assertEqual(cached_success, (True, "AB1234"))
        self.assertEqual(setup.call_count, 2)
        self.assertEqual(kite.profile.call_count, 2)

    def test_partial_auth_failure_repartitions_without_inline_self_heal_by_default(self):
        setup_fns = {f"app{idx}": Mock(name=f"setup_app{idx}") for idx in range(1, 9)}
        valid_apps = {"app2", "app4", "app7"}
        tickers = [f"STOCK{idx:02d}" for idx in range(11)]
        token_map = {ticker: idx for idx, ticker in enumerate(tickers, start=1)}

        def validate(app_name, _setup_fn):
            if app_name in valid_apps:
                return True, f"user-{app_name}"
            return False, "expired access token"

        with (
            patch.object(scheduler, "_setup_fn_map", return_value=setup_fns),
            patch.object(scheduler, "_validate_app_session", side_effect=validate),
            patch.object(scheduler, "_attempt_app_session_self_heal") as self_heal,
        ):
            assignments, failed_apps = scheduler._build_working_app_partitions(
                tickers,
                token_map,
            )

        self_heal.assert_not_called()
        self.assertEqual([row[0] for row in assignments], ["app2", "app4", "app7"])
        self.assertEqual([row[0] for row in failed_apps], ["app1", "app3", "app5", "app6", "app8"])
        self.assertTrue(
            all("self_heal_deferred=preopen_auth_required" in detail for _, detail in failed_apps)
        )
        assigned_tickers = [ticker for _, partition, _, _ in assignments for ticker in partition]
        self.assertEqual(sorted(assigned_tickers), sorted(tickers))
        self.assertEqual(len(assigned_tickers), len(set(assigned_tickers)))
        for _, partition, partition_tokens, _ in assignments:
            self.assertEqual(partition_tokens, {ticker: token_map[ticker] for ticker in partition})

    def test_zero_valid_profiles_fails_without_inline_self_heal_by_default(self):
        setup_fns = {f"app{idx}": Mock(name=f"setup_app{idx}") for idx in range(1, 9)}
        validation = Mock(return_value=(False, "expired access token"))

        with (
            patch.object(scheduler, "_setup_fn_map", return_value=setup_fns),
            patch.object(scheduler, "_validate_app_session", validation),
            patch.object(scheduler, "_attempt_app_session_self_heal") as self_heal,
        ):
            with self.assertRaisesRegex(
                scheduler.NoHealthyKiteSessionsError,
                "No valid Kite sessions available",
            ) as caught:
                scheduler._build_working_app_partitions(["A", "B"], {"A": 1, "B": 2})

        self.assertEqual(validation.call_count, 8)
        self_heal.assert_not_called()
        self.assertEqual(len(caught.exception.failed_apps), 8)

    def test_zero_healthy_sessions_publish_fail_status_and_incomplete_marker(self):
        slot = scheduler.IST.localize(datetime(2026, 8, 31, 9, 25))
        failed_apps = [
            ("app1", "expired access token; self_heal_deferred=preopen_auth_required"),
            ("app2", "expired access token; self_heal_deferred=preopen_auth_required"),
        ]
        no_sessions = scheduler.NoHealthyKiteSessionsError(failed_apps)
        persistent_runner = Mock(name="persistent_runner")
        partition_runner = Mock(name="partition_runner")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ready_dir = root / "slot_ready_5m"
            ready_dir.mkdir()
            status_path = root / "5min.status.json"
            manifest_path = root / "feed_universe_5m.json"

            with (
                patch.object(scheduler, "_read_holidays_set", return_value=set()),
                patch.object(
                    scheduler.core,
                    "load_stocks_universe",
                    return_value=(["A", "B"], {"A": 1, "B": 2}),
                ),
                patch.object(
                    scheduler,
                    "_include_mapped_fno_equities",
                    return_value=(["A", "B"], {"A": 1, "B": 2}),
                ),
                patch.object(
                    scheduler,
                    "_mapped_fno_equities",
                    return_value=pd.DataFrame(
                        {
                            "equity_symbol": ["A", "B"],
                            "equity_instrument_token": [1, 2],
                        }
                    ),
                ),
                patch.object(
                    scheduler,
                    "_build_working_app_partitions",
                    side_effect=no_sessions,
                ),
                patch.object(scheduler, "READY_MARKER_DIR", ready_dir),
                patch.object(scheduler, "SLOT_STATUS_PATH", status_path),
                patch.object(scheduler, "FEED_UNIVERSE_MANIFEST_PATH", manifest_path),
                patch.object(
                    scheduler,
                    "_run_persistent_partition_jobs",
                    persistent_runner,
                ),
                patch.object(scheduler, "_run_partition", partition_runner),
            ):
                with self.assertRaises(scheduler.NoHealthyKiteSessionsError) as caught:
                    scheduler.run_update_5m_once(
                        max_workers=16,
                        max_workers_per_app=4,
                        report_dir=str(root / "reports"),
                        buffer_sec=0,
                        refresh_tokens=False,
                        slot_end=slot,
                        ready_marker_enabled=False,
                    )

            marker_path = ready_dir / "slot_20260831_0925.json"
            status = json.loads(status_path.read_text(encoding="utf-8"))
            marker = json.loads(marker_path.read_text(encoding="utf-8"))

        persistent_runner.assert_not_called()
        partition_runner.assert_not_called()
        self.assertEqual(status["overall_state"], "FAIL")
        self.assertFalse(status["accounting_exact"])
        self.assertEqual(status["unresolved_symbol_count"], 2)
        self.assertTrue(any("auth_no_healthy_apps" in item for item in status["failures"]))
        self.assertEqual(marker["source"], "final")
        self.assertFalse(marker["complete"])
        self.assertEqual(marker["tickers_expected"], 2)
        self.assertEqual(marker["tickers_written"], 0)
        self.assertEqual(marker["unresolved_symbol_count"], 2)
        self.assertEqual(marker["fno_equity_expected"], 2)
        self.assertEqual(marker["fno_equity_ready"], 0)
        self.assertEqual(marker["fno_equity_failed"], 2)
        self.assertTrue(
            any("auth_no_healthy_apps" in item for item in marker["partition_failures"])
        )
        summary = caught.exception.summary
        self.assertFalse(summary["accounting_exact"])
        self.assertEqual(summary["effective_per_app"], 0)
        self.assertEqual(summary["unresolved_symbol_count"], 2)
        self.assertTrue(any("auth_no_healthy_apps" in item for item in summary["failures"]))

    def test_explicit_opt_in_allows_inline_self_heal(self):
        setup_app1 = Mock(name="setup_app1")
        setup_app2 = Mock(name="setup_app2")
        setup_fns = {"app1": setup_app1, "app2": setup_app2}

        def validate(app_name, _setup_fn):
            if app_name == "app1":
                return False, "expired access token"
            return True, "user-app2"

        with (
            patch.object(scheduler, "_setup_fn_map", return_value=setup_fns),
            patch.object(scheduler, "_validate_app_session", side_effect=validate),
            patch.object(
                scheduler,
                "_attempt_app_session_self_heal",
                return_value=(True, "user-app1 (self-healed)"),
            ) as self_heal,
        ):
            assignments, failed_apps = scheduler._build_working_app_partitions(
                ["A", "B", "C"],
                {"A": 1, "B": 2, "C": 3},
                allow_inline_self_heal=True,
            )

        self_heal.assert_called_once_with("app1", setup_app1, "expired access token")
        self.assertEqual([row[0] for row in assignments], ["app1", "app2"])
        self.assertEqual(failed_apps, [])
        assigned_tickers = [ticker for _, partition, _, _ in assignments for ticker in partition]
        self.assertEqual(sorted(assigned_tickers), ["A", "B", "C"])

    def test_fno_quality_gate_requires_exact_one_minute_lineage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "TEST_stocks_indicators_5min.parquet"
            timestamps = pd.date_range(
                "2026-08-11 09:15", periods=3, freq="5min", tz=scheduler.IST
            )
            frame = pd.DataFrame(
                {
                    "date": timestamps,
                    "open": [100.0, 100.0, 101.0],
                    "high": [101.0, 101.0, 102.0],
                    "low": [99.0, 99.0, 100.0],
                    "close": [100.5, 100.5, 101.5],
                    "volume": [1000.0, 1000.0, 800.0],
                    "gap_filled": [0, 0, 0],
                    "opening_snapshot": [1, 0, 0],
                    "provisional_stale": [0, 0, 0],
                    "source_1m_count": [0, 5, float("nan")],
                }
            )
            frame.to_parquet(path, index=False)
            target = timestamps[-1].to_pydatetime()
            with patch.object(scheduler, "RUNTIME_DATA_5M_DIR", root):
                generic = scheduler._ticker_has_required_5m_slot_data("TEST", target)
                exact = scheduler._ticker_has_required_5m_slot_data(
                    "TEST", target, require_exact_1m=True
                )
                frame.loc[2, "source_1m_count"] = 5
                frame.to_parquet(path, index=False)
                verified = scheduler._ticker_has_required_5m_slot_data(
                    "TEST", target, require_exact_1m=True
                )

        self.assertTrue(generic[0])
        self.assertFalse(exact[0])
        self.assertTrue(verified[0])

    def test_live_fetch_universe_includes_all_mapped_fno_equities(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            universe_path = root / "latest_near_month.parquet"
            pd.DataFrame(
                {
                    "equity_symbol": ["IDEA", "LTM", None],
                    "equity_instrument_token": [3677697, 4561409, None],
                }
            ).to_parquet(universe_path, index=False)
            logger = Mock()
            with patch.object(scheduler, "runtime_dir", return_value=universe_path):
                tickers, tokens = scheduler._include_mapped_fno_equities(
                    ["RELIANCE"], {"RELIANCE": 738561}, logger
                )

        self.assertEqual(tickers, ["IDEA", "LTM", "RELIANCE"])
        self.assertEqual(tokens["IDEA"], 3677697)
        self.assertEqual(tokens["LTM"], 4561409)

    def test_partition_error_preserves_summary_for_adaptive_throttle(self):
        summary = {
            "total_elapsed_sec": 150.0,
            "max_partition_elapsed_sec": 150.0,
            "sla_warn_sec": 50.0,
            "failures": ["app1: partition_timeout=150.0s"],
        }
        error = scheduler.ParallelPartitionRunError("failed", summary)

        next_total, next_per_app, healthy_streak, reason = scheduler._adapt_worker_budget(
            configured_total=320,
            configured_per_app=40,
            current_total=320,
            current_per_app=40,
            slot_summary=error.summary,
            healthy_streak=1,
        )

        self.assertEqual(next_total, 288)
        self.assertEqual(next_per_app, 36)
        self.assertEqual(healthy_streak, 0)
        self.assertIn("failures=1", reason)


if __name__ == "__main__":
    unittest.main()
