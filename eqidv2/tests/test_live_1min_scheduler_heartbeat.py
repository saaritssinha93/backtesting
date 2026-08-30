from __future__ import annotations

import json
import logging
import tempfile
import time
import unittest
from pathlib import Path
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
            mock.patch.object(
                scheduler,
                "_five_min_universe_contract",
                return_value={
                    "ok": True,
                    "universe_count": 2,
                    "universe_sha256": "test-sha",
                    "manifest_path": "test-feed-universe.json",
                },
            ),
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

    def test_loader_uses_exact_verified_five_minute_universe(self):
        symbols = ["MCX", "KAYNES"]
        with tempfile.TemporaryDirectory() as temp:
            manifest = Path(temp) / "feed_universe_5m.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": "eqidv2_5m_feed_universe_v1",
                        "symbols": symbols,
                        "universe_count": len(symbols),
                        "universe_sha256": scheduler._universe_sha256(symbols),
                    }
                ),
                encoding="utf-8",
            )
            with (
                mock.patch.object(scheduler, "FIVE_MIN_UNIVERSE_MANIFEST", manifest),
                mock.patch.object(
                    scheduler,
                    "_orig_load_universe",
                    return_value=(["OLD", "MCX"], {"OLD": 111_111, "MCX": 222_222}),
                ),
            ):
                observed, token_map = scheduler._load_stocks_universe_fixed(
                    logging.getLogger("test")
                )

        self.assertEqual(observed, sorted(symbols))
        self.assertEqual(token_map, {"MCX": 222_222})


if __name__ == "__main__":
    unittest.main()
