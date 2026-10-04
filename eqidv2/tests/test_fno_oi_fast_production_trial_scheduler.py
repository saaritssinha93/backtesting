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

    def test_production_runner_is_recurring_and_full_session_configured(self) -> None:
        content = (BAT / "run_fno_oi_fetch_5min_fast_production.bat").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("-Role Trial", content)
        self.assertNotIn('"--session-date"', content)
        self.assertIn('"--workers-per-app","2"', content)
        self.assertIn('"--writer-workers","8"', content)
        self.assertIn("assert_fno_oi_fast_production_trial_exclusive.ps1", content)

    def test_weekday_installer_owns_fast_production_schedule(self) -> None:
        content = (
            BAT / "schedule_fno_oi_weekday.ps1"
        ).read_text(encoding="utf-8")
        self.assertRegex(
            content,
            r'Name\s*=\s*"EQIDV2_fno_oi_fetch_5min_fast_production_0905";\s*Time\s*=\s*"09:05"',
        )
        self.assertIn("/SC WEEKLY /D MON,TUE,WED,THU,FRI", content)
        self.assertIn('RepeatMinutes = 5; RepeatDuration = "06:30"; RestartCount = 3', content)
        self.assertIn('RepeatMinutes = 5; RepeatDuration = "02:10"', content)
        self.assertIn('/RI $task.RepeatMinutes /DU $task.RepeatDuration', content)


if __name__ == "__main__":
    unittest.main()
