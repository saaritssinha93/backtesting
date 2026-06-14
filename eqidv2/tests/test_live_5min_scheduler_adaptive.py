import unittest

import eqidv2_eod_scheduler_for_5mins_data_live_minimal as scheduler


class Live5MinSchedulerAdaptiveTests(unittest.TestCase):
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
