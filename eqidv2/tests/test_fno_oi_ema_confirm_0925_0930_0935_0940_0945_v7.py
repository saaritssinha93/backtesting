from __future__ import annotations

import unittest
from dataclasses import asdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as v6
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7 as v7
import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sweep
import fno_oi_ema_confirm_v7_signal_cache as v7_signal_cache
import fno_v5_hybrid_backtest as replay


class FnoOiEmaConfirmV7Tests(unittest.TestCase):
    @staticmethod
    def _setup(module, signal_end: str, side: str):
        return next(
            setup
            for setup in module.ACTIVE_SETUPS
            if setup.signal_end == signal_end and setup.side == side
        )

    @staticmethod
    def _eligible_row(setup, **overrides) -> dict[str, object]:
        row: dict[str, object] = {
            "sid": 1,
            "day": date(2026, 8, 18),
            "hhmm_int": int(setup.signal_end.replace(":", "")),
            "side": setup.side,
            "tradingsymbol": "MORPH_FAIL",
            "price_change_pct": (
                setup.price_change_pct
                if setup.side == "LONG"
                else -setup.price_change_pct
            ),
            "oi_change_pct": setup.oi_change_pct,
            "volume_ratio": setup.volume_ratio,
            # These are deliberately outside every strict V6 morphology leg.
            "body_ratio": 0.0,
            "wick_ratio": 1.0,
            "traded_value": max(float(setup.min_traded_value), 30_000_000.0),
        }
        row.update(overrides)
        return row

    def test_v7_copies_v6_setup_book_except_disabled_morphology(self) -> None:
        self.assertEqual(len(v7.ACTIVE_SETUPS), len(v6.ACTIVE_SETUPS))
        self.assertEqual(v7.COPIED_FROM_STRATEGY_VERSION, v6.STRATEGY_VERSION)
        self.assertEqual(
            common.canonical_json_sha256([asdict(item) for item in v6.ACTIVE_SETUPS]),
            v7.COPIED_V6_SETUP_BOOK_SHA256,
        )

        for old, new in zip(v6.ACTIVE_SETUPS, v7.ACTIVE_SETUPS, strict=True):
            old_fields = asdict(old)
            new_fields = asdict(new)
            for field, old_value in old_fields.items():
                with self.subTest(setup=old.setup_id, field=field):
                    if field == "body_ratio":
                        self.assertEqual(new_fields[field], 0.0)
                    elif field == "max_wick_ratio":
                        self.assertEqual(new_fields[field], 1.0)
                    elif field == "source_version":
                        self.assertEqual(new_fields[field], v7.STRATEGY_VERSION)
                        self.assertNotEqual(new_fields[field], old_value)
                    else:
                        self.assertEqual(new_fields[field], old_value)

    def test_v7_keeps_v6_universe_timing_cost_and_brackets(self) -> None:
        self.assertEqual(v7.BACKTEST_UNIVERSE_DATE, v6.BACKTEST_UNIVERSE_DATE)
        self.assertEqual(v7.BACKTEST_UNIVERSE_PATH, v6.BACKTEST_UNIVERSE_PATH)
        self.assertEqual(v7.BACKTEST_UNIVERSE_HASHES, v6.BACKTEST_UNIVERSE_HASHES)

        v6_args = v6.parse_args([])
        v7_args = v7.parse_args([])
        for option in (
            "split_day",
            "through_day",
            "cost_bps",
            "square_off",
            "max_forward_bars",
        ):
            with self.subTest(option=option):
                self.assertEqual(getattr(v7_args, option), getattr(v6_args, option))
        self.assertEqual(v7_args.cost_bps, 5.0)

        frozen_args = v7.parse_args(["--freeze-sources"])
        self.assertTrue(frozen_args.freeze_sources)
        self.assertIsNone(frozen_args.source_snapshot)
        snapshot_args = v7.parse_args(
            ["--source-snapshot", str(Path("snapshot") / "manifest.json")]
        )
        self.assertFalse(snapshot_args.freeze_sources)
        self.assertEqual(
            snapshot_args.source_snapshot,
            Path("snapshot") / "manifest.json",
        )
        with self.assertRaises(SystemExit):
            v7.parse_args(
                [
                    "--freeze-sources",
                    "--source-snapshot",
                    str(Path("snapshot") / "manifest.json"),
                ]
            )

        v6_economics = {
            (setup.signal_end, setup.side): (
                setup.confirmation_end,
                setup.max_entries,
                setup.picker,
                setup.stop_pct,
                setup.target_pct,
            )
            for setup in v6.ACTIVE_SETUPS
        }
        v7_economics = {
            (setup.signal_end, setup.side): (
                setup.confirmation_end,
                setup.max_entries,
                setup.picker,
                setup.stop_pct,
                setup.target_pct,
            )
            for setup in v7.ACTIVE_SETUPS
        }
        self.assertEqual(v7_economics, v6_economics)
        v7.validate_configuration()

    def test_v7_outputs_and_signal_cache_are_disjoint_from_v6(self) -> None:
        output_names = (
            "REPORT_PATH",
            "DAILY_OUTPUT_PATH",
            "AUDIT_OUTPUT_PATH",
            "SETUPS_OUTPUT_PATH",
        )
        v6_outputs = {getattr(v6, name).resolve() for name in output_names}
        v7_outputs = {getattr(v7, name).resolve() for name in output_names}

        self.assertEqual(len(v7_outputs), len(output_names))
        self.assertTrue(v7_outputs.isdisjoint(v6_outputs))
        self.assertNotEqual(
            v7_signal_cache.CACHE_DIR.resolve(), v6.signal_cache.CACHE_DIR.resolve()
        )
        self.assertEqual(v7.CACHE_DIR.resolve(), v7_signal_cache.CACHE_DIR.resolve())
        self.assertEqual(
            v7.CACHE_MANIFEST_PATH.resolve(),
            v7_signal_cache.CACHE_MANIFEST_PATH.resolve(),
        )
        self.assertEqual(
            v7_signal_cache.CACHE_MANIFEST_PATH.parent.resolve(),
            v7_signal_cache.CACHE_DIR.resolve(),
        )

    def test_v7_routes_the_breakout_confirmation_policy(self) -> None:
        self.assertIs(v7.signal_cache, v7_signal_cache)
        self.assertEqual(
            v7.CONFIRMATION_POLICY,
            sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
        )
        self.assertEqual(
            v7_signal_cache.CONFIRMATION_POLICY,
            sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
        )
        self.assertNotEqual(
            v7_signal_cache.CONFIRMATION_POLICY,
            sweep.CONFIRMATION_POLICY_V6_STRICT,
        )
        policy = v7.ONE_MIN_ENTRY_POLICY
        self.assertTrue(policy["finite_positive_ohlc_required"])
        self.assertTrue(policy["valid_ohlc_geometry_required"])
        self.assertTrue(policy["nonnegative_volume_required"])
        self.assertFalse(policy["synthetic_or_stale_rows_allowed"])
        self.assertTrue(policy["positive_range_required"])
        self.assertFalse(policy["candle_colour_required"])
        self.assertFalse(
            policy["close_beyond_five_minute_signal_close_required"]
        )
        self.assertFalse(policy["body_ratio_filter_enabled"])
        self.assertFalse(policy["adverse_wick_ratio_filter_enabled"])
        self.assertEqual(policy["long_trigger"], "CONFIRMATION_CANDLE_HIGH")
        self.assertEqual(policy["short_trigger"], "CONFIRMATION_CANDLE_LOW")
        self.assertFalse(policy["same_confirmation_candle_fill_allowed"])

    def test_v7_selection_admits_v6_morphology_failures(self) -> None:
        v6_setup = self._setup(v6, "09:25", "LONG")
        v7_setup = self._setup(v7, "09:25", "LONG")
        rows = pd.DataFrame(
            [
                self._eligible_row(v7_setup),
                self._eligible_row(
                    v7_setup,
                    sid=2,
                    tradingsymbol="STRICT_OK",
                    body_ratio=1.0,
                    wick_ratio=0.0,
                    traded_value=20_000_000.0,
                ),
            ]
        )

        selected_v7 = replay.select_setup_rows(rows, v7_setup)
        selected_v6 = replay.select_setup_rows(rows, v6_setup)

        self.assertEqual(selected_v7["tradingsymbol"].tolist(), ["MORPH_FAIL"])
        self.assertEqual(selected_v6["tradingsymbol"].tolist(), ["STRICT_OK"])

    def test_v7_selection_retains_all_five_minute_thresholds(self) -> None:
        setup = self._setup(v7, "09:25", "LONG")
        failures = {
            "price": {"price_change_pct": setup.price_change_pct - 0.001},
            "oi": {"oi_change_pct": setup.oi_change_pct - 0.001},
            "volume": {"volume_ratio": setup.volume_ratio - 0.001},
            "traded_value": {"traded_value": setup.min_traded_value - 1.0},
            "side": {"side": "SHORT"},
            "slot": {"hhmm_int": 930},
        }
        for gate, override in failures.items():
            with self.subTest(gate=gate):
                signals = pd.DataFrame([self._eligible_row(setup, **override)])
                self.assertTrue(replay.select_setup_rows(signals, setup).empty)

    def test_v7_selection_retains_picker_and_cap(self) -> None:
        setup = self._setup(v7, "09:25", "SHORT")
        rows = pd.DataFrame(
            [
                self._eligible_row(
                    setup, sid=1, tradingsymbol="LOW", volume_ratio=2.0
                ),
                self._eligible_row(
                    setup, sid=2, tradingsymbol="HIGH", volume_ratio=5.0
                ),
                self._eligible_row(
                    setup, sid=3, tradingsymbol="MID", volume_ratio=4.0
                ),
            ]
        )

        selected = replay.select_setup_rows(rows, setup)

        self.assertEqual(setup.picker, "max_volume")
        self.assertEqual(setup.max_entries, 2)
        self.assertEqual(selected["tradingsymbol"].tolist(), ["HIGH", "MID"])

    def test_stop_entry_ignores_pretrigger_adverse_move_and_keeps_cost(self) -> None:
        # Paths begin with the first candle after the 1m confirmation candle.
        # The first path bar moves below the eventual stop but never reaches the
        # long trigger, so it cannot stop out a position that does not exist.
        signals = pd.DataFrame(
            {
                "sid": [1, 2],
                "side": ["LONG", "SHORT"],
                "trigger": [101.0, 99.0],
            }
        )
        paths = {
            1: {
                "high": np.array([100.99, 101.0]),
                "low": np.array([99.0, 101.0]),
                "close": np.array([100.0, 101.0]),
            },
            2: {
                "high": np.array([100.0, 99.0]),
                "low": np.array([99.01, 99.0]),
                "close": np.array([100.0, 99.0]),
            },
        }

        net = sweep.simulate_bracket(
            signals,
            paths,
            stop_pct=1.0,
            target_pct=3.0,
            cost_bps=5.0,
        )

        np.testing.assert_allclose(net, np.array([-0.05, -0.05]), atol=1e-12)


if __name__ == "__main__":
    unittest.main()
