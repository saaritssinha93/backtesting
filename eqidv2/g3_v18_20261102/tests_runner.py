"""Accounting and isolation checks for the new research runner."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from g3_v18_20261102.run import PACKAGE, ROOT, metrics


class RunnerTests(unittest.TestCase):
    def test_metrics_uses_executed_after_cost_pnl_and_zero_days(self):
        book = pd.DataFrame([
            dict(day="2026-10-01", portfolio_executed=True, filled=True, portfolio_net_profit_rupees=-100., portfolio_cost_rupees=250.),
            dict(day="2026-10-03", portfolio_executed="True", filled="True", portfolio_net_profit_rupees=200., portfolio_cost_rupees=250.),
            dict(day="2026-10-04", portfolio_executed="1", filled="1", portfolio_net_profit_rupees=-50., portfolio_cost_rupees=250.),
            dict(day="2026-10-04", portfolio_executed=False, filled=True, portfolio_net_profit_rupees=99999., portfolio_cost_rupees=250.),
            dict(day="2026-10-05", portfolio_executed=True, filled=True, portfolio_net_profit_rupees=88888., portfolio_cost_rupees=250.),
        ])
        result = metrics(book, ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04"])
        self.assertEqual(result["sessions"], 4)
        self.assertEqual(result["selected_orders"], 4)
        self.assertEqual(result["native_fills"], 4)
        self.assertEqual(result["trades"], 3)
        self.assertEqual(result["wins"], 1)
        self.assertEqual(result["losses"], 2)
        self.assertEqual(result["net_pnl_rs"], 50.)
        self.assertEqual(result["daily_close_drawdown_rs"], 100.)
        self.assertEqual(result["costs_rs"], 750.)
        self.assertAlmostEqual(result["profit_factor"], 200./150.)

    def test_empty_calendar_has_defined_zero_pnl_and_drawdown(self):
        result = metrics(pd.DataFrame(), ["2026-10-01", "2026-10-02"])
        self.assertEqual(result["sessions"], 2)
        self.assertEqual(result["trades"], 0)
        self.assertEqual(result["net_pnl_rs"], 0.)
        self.assertEqual(result["daily_close_drawdown_rs"], 0.)
        self.assertIsNone(result["win_rate_pct"])

    def test_write_guard_allows_new_output_and_rejects_escape(self):
        # Audit hooks cannot be removed, so isolate the guard in a subprocess.
        # All test-created files stay under this newly created package.
        with tempfile.TemporaryDirectory(prefix="guard_test_", dir=PACKAGE) as directory:
            code = """
import sys
from pathlib import Path
from g3_v18_20261102.run import guard_writes
allowed=Path(sys.argv[1])
guard_writes(allowed)
(allowed/'inside.txt').write_text('allowed',encoding='utf-8')
assert (allowed/'inside.txt').read_text()=='allowed'
try:
    (allowed.parent/'guard_escape_should_not_exist.txt').write_text('denied')
except PermissionError:
    pass
else:
    raise AssertionError('write escaped guard')
try:
    (allowed/'inside.txt').rename(allowed.parent/'guard_rename_should_not_exist.txt')
except PermissionError:
    pass
else:
    raise AssertionError('rename escaped guard')
assert (allowed/'inside.txt').is_file()
"""
            result = subprocess.run([sys.executable, "-B", "-c", code, directory], cwd=ROOT,
                                    text=True, capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertFalse((PACKAGE / "guard_escape_should_not_exist.txt").exists())
            self.assertFalse((PACKAGE / "guard_rename_should_not_exist.txt").exists())


if __name__ == "__main__":
    unittest.main()
