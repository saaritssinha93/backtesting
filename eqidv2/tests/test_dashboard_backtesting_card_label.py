from __future__ import annotations

import unittest
from pathlib import Path

import log_dashboard_server as dashboard


class DashboardBacktestingCardLabelTests(unittest.TestCase):
    def test_g_only_label_is_used_for_card_and_timeline(self) -> None:
        source = Path(dashboard.__file__).read_text(encoding="utf-8")
        expected = "Backtesting result v13-v10-G"

        self.assertIn(f'"backtesting_result_v13_v10_g": "{expected}"', source)
        self.assertIn(
            f'{{ time: "16:20", id: "backtesting_result_v13_v10_g", label: "{expected}" }}',
            source,
        )
        self.assertNotIn('"Backtesting Result v11"', source)
        self.assertNotIn('"backtesting_result_v11"', source)
        self.assertNotIn('"Backtesting result v6/v8/v10/v11/v12"', source)
        markdown_set = source[source.index("const MD_REPORT_CARDS"):source.index("const FNO_MULTI_PAPER_CARDS")]
        restart_set = source[source.index("const RESTARTABLE_CARDS"):source.index("let ENABLED_RESTARTABLE_CARDS")]
        self.assertIn('"backtesting_result_v13_v10_g"', markdown_set)
        self.assertIn('"backtesting_result_v13_v10_g"', restart_set)

    def test_rename_preserves_card_runtime_contract(self) -> None:
        card_id = "backtesting_result_v13_v10_g"

        self.assertEqual(
            dashboard.LOG_FILES[card_id],
            "backtesting_result_v13_v10_g_latest.log",
        )
        self.assertEqual(
            dashboard.CARD_TASK_NAMES[card_id],
            ("\\EQIDV2_backtesting_result_v13_v10_g_1620",),
        )
        self.assertEqual(
            dashboard.RESTARTABLE_CARDS[card_id],
            "run_backtesting_result_v13_v10_g_1620.bat",
        )
        self.assertEqual(dashboard.STATUS_FILES[card_id], "backtesting_result_v13_v10_g.status")
        self.assertEqual(dashboard.HEARTBEAT_FILES[card_id], "backtesting_result_v13_v10_g.heartbeat")


def test_missing_g_report_does_not_fall_back_to_legacy_comparison(tmp_path, monkeypatch):
    logs = tmp_path / "logs"
    logs.mkdir()
    for filename in ("backtesting_result_v11_latest.log", "backtesting_result_v11_2026-09-15.log"):
        (logs / filename).write_text("OLD_COMPARISON_MUST_NOT_APPEAR", encoding="utf-8")
    report = tmp_path / "backtesting_result_v13_v10_g" / "latest" / "latest_backtesting_result_v13_v10_g.md"
    monkeypatch.setattr(dashboard, "LOG_DIR", logs)
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_REPORT", report)
    resolved, display = dashboard.resolve_log_target("backtesting_result_v13_v10_g")
    assert resolved == report
    assert "backtesting_result_v13_v10_g" in display
    selected, _, text = dashboard._backtesting_v13_v10_g_view(
        {"session_date": "2026-09-15", "status": "SUCCESS"}, today_ist="2026-09-15",
    )
    assert selected == report
    assert "Status: AWAITING_G_REPORT" in text
    assert "Session date: 2026-09-15" in text
    assert "OLD_COMPARISON" not in text


def test_g_current_run_log_precedes_report_only_while_running(tmp_path, monkeypatch):
    logs = tmp_path / "logs"
    logs.mkdir()
    current = logs / "backtesting_result_v13_v10_g_2026-09-15.log"
    current.write_text("G_DAILY_REPLAY_PROGRESS", encoding="utf-8")
    report = tmp_path / "latest_backtesting_result_v13_v10_g.md"
    report.write_text("# Backtesting result v13-v10-G\n\nSession date: 2026-09-15\nStatus: COMPLETE\n\nG_DAILY_ONLY", encoding="utf-8")
    monkeypatch.setattr(dashboard, "LOG_DIR", logs)
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_REPORT", report)
    selected, display, text = dashboard._backtesting_v13_v10_g_view(
        {"session_date": "2026-09-15", "status": "RUNNING"}, today_ist="2026-09-15",
    )
    assert selected == current and display == current.name
    assert "Session date: 2026-09-15" in text and "Status: RUNNING" in text
    assert "G_DAILY_REPLAY_PROGRESS" in text
    selected, _, text = dashboard._backtesting_v13_v10_g_view(
        {"session_date": "2026-09-15", "status": "SUCCESS"}, today_ist="2026-09-15",
    )
    assert selected == report
    assert text.startswith("# Backtesting result v13-v10-G")
    assert "G_DAILY_ONLY" in text and "G_DAILY_REPLAY_PROGRESS" not in text


def test_manual_historical_g_run_reads_its_requested_date_log(tmp_path, monkeypatch):
    current = tmp_path / "backtesting_result_v13_v10_g_2026-09-15.log"
    requested = tmp_path / "backtesting_result_v13_v10_g_2026-09-11.log"
    current.write_text("TODAY_IS_NOT_THE_REQUESTED_RUN", encoding="utf-8")
    requested.write_text("HISTORICAL_G_RUN_PROGRESS", encoding="utf-8")
    monkeypatch.setattr(dashboard, "LOG_DIR", tmp_path)
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_REPORT", tmp_path / "report.md")
    path, _, text = dashboard._backtesting_v13_v10_g_view(
        {"session_date": "2026-09-11", "status": "RUNNING"}, today_ist="2026-09-15",
    )
    assert path == requested
    assert "Session date: 2026-09-11" in text
    assert "HISTORICAL_G_RUN_PROGRESS" in text
    assert "TODAY_IS_NOT_THE_REQUESTED_RUN" not in text
    path, _, text = dashboard._backtesting_v13_v10_g_view(
        {"status": "RUNNING"}, today_ist="2026-09-15",
    )
    assert path == current
    assert "Session date: 2026-09-15" in text


def test_malformed_runtime_date_cannot_select_any_log_or_old_report(tmp_path, monkeypatch):
    report = tmp_path / "report.md"
    report.write_text("REPORT_MUST_NOT_BE_SUBSTITUTED", encoding="utf-8")
    (tmp_path / "backtesting_result_v13_v10_g_2026-09-15.log").write_text("TODAY_MUST_NOT_BE_SUBSTITUTED", encoding="utf-8")
    monkeypatch.setattr(dashboard, "LOG_DIR", tmp_path)
    monkeypatch.setattr(dashboard, "FNO_G_BACKTEST_REPORT", report)
    for invalid in ("20260915", "2026-9-15", "2026-02-31", "../2026-09-15", "2026-09-15T16:20:00"):
        _, _, text = dashboard._backtesting_v13_v10_g_view(
            {"session_date": invalid, "status": "RUNNING"}, today_ist="2026-09-15",
        )
        assert "Status: INVALID_SESSION_DATE" in text
        assert "MUST_NOT_BE_SUBSTITUTED" not in text


if __name__ == "__main__":
    unittest.main()
