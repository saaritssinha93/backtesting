from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BAT = ROOT / "bat"
GATE = BAT / "fno_oi_fast_production_trial_date_gate.ps1"


class FnoOiFastProductionTrialSchedulerTests(unittest.TestCase):
    def _gate(self, role: str, observed_date: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(GATE),
                "-Role",
                role,
                "-TrialDate",
                "2026-09-02",
                "-ObservedDate",
                observed_date,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )

    def test_legacy_is_blocked_only_on_trial_date(self) -> None:
        self.assertEqual(self._gate("Legacy", "2026-09-01").returncode, 0)
        trial = self._gate("Legacy", "2026-09-02")
        self.assertEqual(trial.returncode, 42)
        self.assertIn("[SKIP]", trial.stdout)
        self.assertEqual(self._gate("Legacy", "2026-09-03").returncode, 0)

    def test_trial_is_allowed_only_on_exact_date(self) -> None:
        self.assertEqual(self._gate("Trial", "2026-09-01").returncode, 42)
        trial = self._gate("Trial", "2026-09-02")
        self.assertEqual(trial.returncode, 0)
        self.assertIn("[ALLOW]", trial.stdout)
        self.assertEqual(self._gate("Trial", "2026-09-03").returncode, 42)

    def test_recurring_runners_use_legacy_gate(self) -> None:
        for name in (
            "run_fno_oi_fetch_5min.bat",
            "run_fno_oi_fetch_5min_fast_shadow.bat",
        ):
            content = (BAT / name).read_text(encoding="utf-8")
            self.assertIn("-Role Legacy -TrialDate 2026-09-02", content)
            self.assertIn('if "%TRIAL_GATE_EXIT%"=="42" endlocal & exit /b 0', content)

    def test_trial_runner_is_date_locked_and_full_session_configured(self) -> None:
        content = (BAT / "run_fno_oi_fetch_5min_fast_production.bat").read_text(
            encoding="utf-8"
        )
        self.assertIn("-Role Trial -TrialDate 2026-09-02", content)
        self.assertIn('"--session-date","2026-09-02"', content)
        self.assertIn('"--workers-per-app","2"', content)
        self.assertIn('"--writer-workers","8"', content)
        self.assertIn("assert_fno_oi_fast_production_trial_exclusive.ps1", content)

    def test_task_installer_is_one_time_and_fail_closed(self) -> None:
        content = (
            BAT / "schedule_fno_oi_fast_production_trial_20260902.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn('"2026-09-02 09:05"', content)
        self.assertIn("New-ScheduledTaskTrigger -Once", content)
        self.assertIn("Assert-TrialTaskContract", content)
        self.assertNotIn("-Force", content)


if __name__ == "__main__":
    unittest.main()
