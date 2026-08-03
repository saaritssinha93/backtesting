from __future__ import annotations

import importlib
import os
import unittest
from contextlib import contextmanager
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
PARITY_BOOK = "final_setup_conf_v11_working"


@contextmanager
def _environment(**updates: str):
    previous = {name: os.environ.get(name) for name in updates}
    try:
        os.environ.update(updates)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _reset_v11_loader() -> None:
    import avwap_5min_ID_v11_backtesting as v11

    v11._load_final_setup_conf_module.cache_clear()


class V7V11SetupBookParityTests(unittest.TestCase):
    def tearDown(self):
        _reset_v11_loader()

    def test_shared_module_env_is_authoritative_for_v7_and_v11(self):
        with _environment(
            EQIDV2_FINAL_SETUP_CONF_MODULE=PARITY_BOOK,
            EQIDV2_V11_FINAL_SETUP_CONF_MODULE="final_setup_conf",
        ):
            _reset_v11_loader()
            import avwap_5min_ID_v11_backtesting as v11
            import eqidv2_final_conf_live_bootstrap as live

            expected = importlib.import_module(PARITY_BOOK).FINAL_SETUP_CONF
            self.assertEqual(live.conf_source(), PARITY_BOOK)
            self.assertIs(live.conf(), expected)
            self.assertIs(
                v11._load_final_setup_conf_module().FINAL_SETUP_CONF, expected
            )
            self.assertEqual(set(expected), set(live.conf()))
            self.assertEqual(
                set(expected),
                set(v11._load_final_setup_conf_module().FINAL_SETUP_CONF),
            )
            self.assertIn("G_HIGHER_HIGH_BREAK", expected)
            self.assertIn("L_LATE_BB10_COMPRESSION_BREAKOUT", expected)
            self.assertEqual(len(expected), 12)

    def test_live_and_v11_masks_match_for_min_and_max_guards(self):
        with _environment(
            EQIDV2_FINAL_SETUP_CONF_MODULE=(
                "final_setup_conf_v11_book_b_time_filtered"
            )
        ):
            _reset_v11_loader()
            import avwap_5min_ID_v11_backtesting as v11
            import eqidv2_final_conf_live_bootstrap as live

            base = {
                "side": "LONG",
                "regime": "NEUTRAL",
                "quality_score": 200.0,
                "vol_ratio": 1.0,
                "atr_pct": 0.003,
                "upper_wick_pct": 0.1,
                "rs_pct": 6.0,
                "vwap_dist_atr": 0.5,
            }
            rows = [
                {**base, "setup": "E_ORB_BREAKOUT_LONG", "signal_minute": 629},
                {**base, "setup": "E_ORB_BREAKOUT_LONG", "signal_minute": 630},
                {**base, "setup": "E_ORB_BREAKOUT_LONG", "signal_minute": 690},
                {**base, "setup": "E_ORB_BREAKOUT_LONG", "signal_minute": 691},
                {**base, "setup": "C_OR_BREAKDOWN", "signal_minute": 749},
                {**base, "setup": "C_OR_BREAKDOWN", "signal_minute": 750},
            ]
            session = pd.Timestamp("2026-07-21", tz="Asia/Kolkata")
            for row in rows:
                minute = row.pop("signal_minute")
                row["signal_time_ist"] = session + pd.Timedelta(minutes=minute)
            frame = pd.DataFrame(rows)
            live_mask = live.conf_mask(frame).tolist()
            v11_mask = v11._final_setup_conf_mask(frame).tolist()
            self.assertEqual(live_mask, v11_mask)
            self.assertEqual(
                live_mask, [False, True, True, False, False, True]
            )

    def test_live_and_v11_derive_identical_wick_mask_features(self):
        with _environment(EQIDV2_FINAL_SETUP_CONF_MODULE=PARITY_BOOK):
            _reset_v11_loader()
            import avwap_5min_ID_v11_backtesting as v11
            import eqidv2_final_conf_live_bootstrap as live

            row = {
                "ticker": "KAJARIACER",
                "side": "LONG",
                "setup": "G_HIGHER_HIGH_BREAK",
                "signal_time_ist": "2026-07-24 11:25:00+05:30",
                "signal_open": 1200.5,
                "signal_high": 1208.1,
                "signal_low": 1200.5,
                "signal_close": 1207.7,
                "atr_pct": 0.002696980092028539,
                "market_ret_pct": 0.0,
            }
            frame = pd.DataFrame([row])
            self.assertEqual(live.conf_mask(frame).tolist(), [True])
            self.assertEqual(
                live.conf_mask(frame).tolist(),
                v11._final_setup_conf_mask(frame).tolist(),
            )

    def test_all_production_launchers_select_the_shared_parity_book(self):
        launchers = [
            "bat/run_conf_paper_signal_discovery.bat",
            "bat/run_conf_paper_entry_engine.bat",
            "bat/run_conf_paper_executor.bat",
            "bat/run_conf_live_executor.bat",
            "bat/run_backtesting_result_v11_1600.bat",
        ]
        expected = (
            "EQIDV2_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_working"
        )
        for relative_path in launchers:
            content = (ROOT / relative_path).read_text(
                encoding="utf-8", errors="ignore"
            )
            self.assertIn(expected, content, relative_path)

    def test_current_conf_book_skips_redundant_full_universe_scans(self):
        import eqidv2_signal_discovery_v7_5min_id_persistent as scanner

        old_active = scanner._CONF_SCANNER_ACTIVE
        old_include_tier123 = scanner.V11_BACKTESTING_OVERLAY_INCLUDE_TIER123
        old_conf_keys = scanner._conf_boot.conf_keys
        try:
            expected = importlib.import_module(PARITY_BOOK).FINAL_SETUP_CONF
            scanner._CONF_SCANNER_ACTIVE = True
            scanner.V11_BACKTESTING_OVERLAY_INCLUDE_TIER123 = True
            scanner._conf_boot.conf_keys = lambda: set(expected)

            self.assertEqual(scanner._active_conf_tier_c_setups(), set())
            self.assertFalse(scanner._legacy_tier123_eligible(0.0))
        finally:
            scanner._CONF_SCANNER_ACTIVE = old_active
            scanner.V11_BACKTESTING_OVERLAY_INCLUDE_TIER123 = old_include_tier123
            scanner._conf_boot.conf_keys = old_conf_keys

    def test_non_conf_mode_keeps_latency_bounded_legacy_tier123(self):
        import eqidv2_signal_discovery_v7_5min_id_persistent as scanner

        old_active = scanner._CONF_SCANNER_ACTIVE
        old_include_tier123 = scanner.V11_BACKTESTING_OVERLAY_INCLUDE_TIER123
        old_latest_start = scanner.TIER123_LATEST_START_LAG_SEC
        try:
            scanner._CONF_SCANNER_ACTIVE = False
            scanner.V11_BACKTESTING_OVERLAY_INCLUDE_TIER123 = True
            scanner.TIER123_LATEST_START_LAG_SEC = 40.0

            self.assertTrue(scanner._legacy_tier123_eligible(39.9))
            self.assertFalse(scanner._legacy_tier123_eligible(40.1))
        finally:
            scanner._CONF_SCANNER_ACTIVE = old_active
            scanner.V11_BACKTESTING_OVERLAY_INCLUDE_TIER123 = old_include_tier123
            scanner.TIER123_LATEST_START_LAG_SEC = old_latest_start

    def test_conf_handoff_overrides_survive_base_launchers(self):
        entry_launcher = (
            ROOT / "bat/run_eqidv2_entry_engine_1min_v5_id.bat"
        ).read_text(encoding="utf-8", errors="ignore")
        paper_launcher = (
            ROOT / "bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat"
        ).read_text(encoding="utf-8", errors="ignore")

        self.assertIn(
            "if not defined EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC",
            entry_launcher,
        )
        self.assertIn(
            "if not defined EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_SEC",
            entry_launcher,
        )
        self.assertIn(
            "if not defined EQIDV2_LATE_DETECTION_MAX_LAG_SEC",
            paper_launcher,
        )


if __name__ == "__main__":
    unittest.main()
