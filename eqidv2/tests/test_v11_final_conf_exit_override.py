from __future__ import annotations

import unittest
from unittest.mock import patch

import avwap_5min_ID_v11_backtesting as v11


class V11FinalConfExitOverrideTests(unittest.TestCase):
    def test_cached_final_conf_replay_uses_active_conf_exit(self) -> None:
        setup = "E_ORB_BREAKOUT_LONG"
        old_rule = v11.v6.SETUP_EXIT_RULES.get(setup)
        try:
            v11.v6.SETUP_EXIT_RULES[setup] = (1.0, 2.75)
            with (
                patch.object(v11, "_FINAL_CONF_ACTIVE", True),
                patch.object(v11, "_FINAL_CONF_SETUP_KEYS", frozenset({setup})),
            ):
                self.assertEqual(
                    v11._selected_exit_override(setup, v11.FINAL_CONF_PROFILE),
                    (1.0, 2.75),
                )
        finally:
            if old_rule is None:
                v11.v6.SETUP_EXIT_RULES.pop(setup, None)
            else:
                v11.v6.SETUP_EXIT_RULES[setup] = old_rule

    def test_inactive_final_conf_does_not_override_cached_exit(self) -> None:
        with patch.object(v11, "_FINAL_CONF_ACTIVE", False):
            self.assertIsNone(
                v11._selected_exit_override(
                    "E_ORB_BREAKOUT_LONG",
                    v11.FINAL_CONF_PROFILE,
                )
            )


if __name__ == "__main__":
    unittest.main()

