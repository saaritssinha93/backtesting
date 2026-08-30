import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import wait_for_data_backtesting_ready as ready


class WaitForDataBacktestingReadyTests(unittest.TestCase):
    def test_latest_rerun_must_reach_its_own_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = Path(tmp)
            day = "2026-09-01"
            (log_dir / f"data_for_backtesting_{day}.log").write_text(
                "START Data for backtesting parallel session\n"
                "END Data for backtesting parallel session (exit=0)\n"
                "START Data for backtesting parallel session\n",
                encoding="utf-8",
            )
            with mock.patch.object(ready, "LOG_DIR", log_dir):
                status, note = ready._data_job_status(day)

        self.assertEqual(status, "WAIT")
        self.assertIn("latest run", note)

    def test_scope_mismatch_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            verify_dir = Path(tmp)
            day = "2026-09-01"
            (verify_dir / f"data_verify_{day}.json").write_text(
                json.dumps({"scope": "all", "overall_exit_code": 0, "overall_status": "PASS"}),
                encoding="utf-8",
            )
            with mock.patch.object(ready, "VERIFY_DIR", verify_dir):
                status, note, _ = ready._verify_status(day, scope="fno")

        self.assertEqual(status, "WAIT")
        self.assertIn("waiting for scope=fno", note)


if __name__ == "__main__":
    unittest.main()
