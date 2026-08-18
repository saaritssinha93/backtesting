from __future__ import annotations

import unittest

import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as v6


class FnoOiEmaConfirmV6Tests(unittest.TestCase):
    def test_best_net_configuration_is_frozen(self) -> None:
        v6.validate_configuration()
        self.assertEqual(v6.OBJECTIVE, "BEST_NET")
        self.assertEqual(len(v6.ACTIVE_SETUPS), 10)
        self.assertEqual(v6.EXPECTED_SELECTED_HISTORY["orders"], 206)
        self.assertEqual(v6.EXPECTED_SELECTED_HISTORY["fills"], 205)
        self.assertAlmostEqual(
            float(v6.EXPECTED_SELECTED_HISTORY["net_pct"]), 144.00315457745492
        )

    def test_every_window_has_long_and_short_configuration(self) -> None:
        observed = {(setup.signal_end, setup.side) for setup in v6.ACTIVE_SETUPS}
        expected = {
            (slot, side)
            for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
            for side in ("LONG", "SHORT")
        }
        self.assertEqual(observed, expected)

    def test_promoted_replay_is_pinned_to_audited_dated_universe(self) -> None:
        self.assertEqual(v6.BACKTEST_UNIVERSE_DATE.isoformat(), "2026-08-11")
        self.assertEqual(
            v6.BACKTEST_UNIVERSE_PATH.name, "near_month_2026-08-11.parquet"
        )
        self.assertNotIn("latest", v6.BACKTEST_UNIVERSE_PATH.name.lower())
        self.assertEqual(
            v6.BACKTEST_UNIVERSE_HASHES["mapped_symbol_set_sha256"],
            "d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3",
        )

    def test_current_source_repromotion_is_separate_from_legacy_selection(self) -> None:
        self.assertEqual(v6.CURRENT_SOURCE_REPLAY_REVISION, "20260818_V1")
        self.assertEqual(v6.CURRENT_SOURCE_PROMOTED_HISTORY["orders"], 210)
        self.assertEqual(v6.CURRENT_SOURCE_PROMOTED_HISTORY["fills"], 209)
        self.assertNotEqual(
            v6.CURRENT_SOURCE_SELECTED_DAILY_PATH.resolve(),
            v6.SELECTED_DAILY_PROTECTED_PATH.resolve(),
        )
        self.assertEqual(
            v6.EXPECTED_SELECTED_HISTORY["orders"], 206,
            "legacy evidence must remain unchanged",
        )

    def test_v6_outputs_do_not_overwrite_v5(self) -> None:
        v6_outputs = {
            v6.REPORT_PATH.resolve(),
            v6.DAILY_OUTPUT_PATH.resolve(),
            v6.AUDIT_OUTPUT_PATH.resolve(),
            v6.SETUPS_OUTPUT_PATH.resolve(),
        }
        self.assertNotIn(v6.v5.REPORT_PATH.resolve(), v6_outputs)
        self.assertNotIn(v6.v5.SELECTED_DAILY_OUTPUT_PATH.resolve(), v6_outputs)


if __name__ == "__main__":
    unittest.main()
