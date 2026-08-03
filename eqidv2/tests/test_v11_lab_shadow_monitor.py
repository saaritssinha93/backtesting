from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import v11_lab_shadow_monitor as monitor


class V11LabShadowMonitorTests(unittest.TestCase):
    def test_v11_subprocess_receives_shared_frozen_conf_contract(self):
        module_name = "final_setup_conf_v11_conf_d"
        completed = SimpleNamespace(returncode=0, stdout="", stderr="")

        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                mock.patch.object(monitor, "RUNS_DIR", Path(temp_dir)),
                mock.patch.object(
                    monitor.subprocess,
                    "run",
                    return_value=completed,
                ) as run_mock,
            ):
                monitor._run_v11(
                    "2026-07-29",
                    module_name,
                    "candidate_shadow",
                    Path(temp_dir) / "json",
                )

        child_env = run_mock.call_args.kwargs["env"]
        self.assertEqual(
            child_env["EQIDV2_FINAL_SETUP_CONF_MODULE"],
            module_name,
        )
        self.assertEqual(
            child_env["EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE"],
            module_name,
        )
        self.assertEqual(
            child_env["EQIDV2_V11_FINAL_SETUP_CONF_MODULE"],
            module_name,
        )


if __name__ == "__main__":
    unittest.main()
