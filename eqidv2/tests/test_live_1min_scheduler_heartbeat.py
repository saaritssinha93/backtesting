from __future__ import annotations

import time
import unittest
from unittest import mock

import eqidv2_eod_scheduler_for_1min_data_live as scheduler


class LiveOneMinuteSchedulerHeartbeatTests(unittest.TestCase):
    def test_long_slot_refreshes_running_status_until_fetch_finishes(self):
        writes: list[tuple[str, dict]] = []

        def record_status(state: str, detail: dict) -> None:
            writes.append((state, dict(detail)))

        def slow_fetch(*args, **kwargs) -> None:
            time.sleep(0.24)

        with (
            mock.patch.object(scheduler, "STATUS_TOUCH_SEC", 0.05),
            mock.patch.object(scheduler, "_write_status", side_effect=record_status),
            mock.patch.object(scheduler.core, "run_mode", side_effect=slow_fetch),
        ):
            result = scheduler._run_one_slot(
                scheduler.now_ist(),
                max_workers=1,
                intraday_ts="end",
                holidays=set(),
                report_dir="unused",
            )

        running_writes = [
            detail for state, detail in writes if state == "running"
        ]
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(len(running_writes), 2)
        self.assertTrue(
            any("slot_elapsed_sec" in detail for detail in running_writes)
        )


if __name__ == "__main__":
    unittest.main()
