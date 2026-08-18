from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_sweep as sweep


class ConfirmationPolicyTests(unittest.TestCase):
    @staticmethod
    def _mapped_universe() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "equity_symbol": "TEST",
                    "equity_instrument_token": 101,
                    "futures_tradingsymbol": "TEST26AUGFUT",
                    "futures_instrument_token": 202,
                }
            ]
        )

    @staticmethod
    def _five_minute_signal(side: str) -> pd.DataFrame:
        long_side = side == "LONG"
        return pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp("2026-08-18 09:25", tz=common.IST),
                    "open": 100.0,
                    "high": 102.0,
                    "low": 99.0,
                    "close": 101.0,
                    "volume": 2_000.0,
                    "ema9": 103.0 if long_side else 97.0,
                    "ema20": 102.0 if long_side else 98.0,
                    "ema50": 101.0 if long_side else 99.0,
                    "oi": 110.0,
                    "prev_oi": 100.0,
                    "oi_change_pct": 10.0,
                    "volume_ratio": 2.0,
                    "price_change_pct": 1.0 if long_side else -1.0,
                    "traded_value": 200_000.0,
                }
            ]
        )

    @staticmethod
    def _one_minute_path(side: str) -> pd.DataFrame:
        # Deliberately use the opposite candle colour at 09:26. V6 must reject
        # it, while V7 may record its high/low for the later breakout trigger.
        if side == "LONG":
            opens = [102.0, 101.0, 102.0]
            closes = [100.0, 102.0, 103.0]
        else:
            opens = [100.0, 101.0, 100.0]
            closes = [102.0, 100.0, 99.0]
        return pd.DataFrame(
            {
                "ts": pd.date_range(
                    "2026-08-18 09:26", periods=3, freq="1min", tz=common.IST
                ),
                "open": opens,
                "high": [103.0, 104.0, 105.0],
                "low": [99.0, 98.0, 97.0],
                "close": closes,
                "volume": [100.0, 100.0, 100.0],
            }
        )

    def _build(self, side: str, *, minute: pd.DataFrame | None = None, **kwargs):
        if minute is None:
            minute = self._one_minute_path(side)
        five = self._five_minute_signal(side)
        nonempty = pd.DataFrame({"present": [True]})
        with (
            patch.object(sweep.bt, "load_five_minute", return_value=nonempty),
            patch.object(sweep, "load_minute_history", return_value=minute),
            patch.object(
                sweep.hybrid,
                "aggregate_equity_one_minute_to_five_minute",
                return_value=nonempty,
            ),
            patch.object(
                sweep.hybrid,
                "join_equity_price_with_futures_oi",
                return_value=five,
            ),
        ):
            return sweep.build_signal_table(
                {pd.Timestamp("2026-08-18").date()},
                square_off="1530",
                max_forward_bars=10,
                mapped_universe=self._mapped_universe(),
                **kwargs,
            )

    def test_default_policy_preserves_v6_directional_candle_gate(self) -> None:
        for side in ("LONG", "SHORT"):
            with self.subTest(side=side):
                signals, paths = self._build(side)
                self.assertTrue(signals.empty)
                self.assertEqual(paths, {})

    def test_v7_policy_ignores_morphology_and_keeps_high_low_trigger(self) -> None:
        expected_trigger = {"LONG": 103.0, "SHORT": 99.0}
        for side in ("LONG", "SHORT"):
            with self.subTest(side=side):
                signals, paths = self._build(
                    side,
                    confirmation_policy=sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
                )
                self.assertEqual(len(signals), 1)
                self.assertEqual(signals.iloc[0]["side"], side)
                self.assertEqual(signals.iloc[0]["trigger"], expected_trigger[side])
                self.assertEqual(set(paths), {0})
                self.assertEqual(paths[0]["high"].tolist(), [104.0, 105.0])
                self.assertEqual(paths[0]["low"].tolist(), [98.0, 97.0])

    def test_v7_policy_still_requires_finite_positive_range(self) -> None:
        invalid_candles = {}

        zero_range = self._one_minute_path("LONG")
        zero_range.loc[0, ["open", "high", "low", "close"]] = 100.0
        invalid_candles["zero_range"] = zero_range

        nonfinite = self._one_minute_path("LONG")
        nonfinite.loc[0, "open"] = float("nan")
        invalid_candles["nonfinite"] = nonfinite

        malformed = self._one_minute_path("LONG")
        malformed.loc[0, "high"] = 101.0  # Below the 102.0 open.
        invalid_candles["malformed_ohlc"] = malformed

        negative_volume = self._one_minute_path("LONG")
        negative_volume.loc[0, "volume"] = -1.0
        invalid_candles["negative_volume"] = negative_volume

        flagged = self._one_minute_path("LONG")
        flagged["gap_filled"] = [1, 0, 0]
        invalid_candles["gap_filled"] = flagged

        nonpositive = self._one_minute_path("LONG")
        nonpositive.loc[0, "low"] = 0.0
        invalid_candles["nonpositive"] = nonpositive

        invalid_high = self._one_minute_path("LONG")
        invalid_high.loc[0, "high"] = 101.0
        invalid_candles["high_below_open"] = invalid_high

        invalid_low = self._one_minute_path("LONG")
        invalid_low.loc[0, "low"] = 101.0
        invalid_candles["low_above_close"] = invalid_low

        for label, minute in invalid_candles.items():
            with self.subTest(candle=label):
                signals, paths = self._build(
                    "LONG",
                    minute=minute,
                    confirmation_policy=sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
                )
                self.assertTrue(signals.empty)
                self.assertEqual(paths, {})

    def test_v7_validity_checks_do_not_change_the_v6_default_path(self) -> None:
        minute = self._one_minute_path("LONG")
        minute.loc[0, ["open", "high", "low", "close"]] = [
            100.0,
            103.0,
            99.0,
            102.0,
        ]
        minute["gap_filled"] = [1, 0, 0]

        signals, paths = self._build("LONG", minute=minute)

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals.iloc[0]["trigger"], 103.0)
        self.assertEqual(set(paths), {0})

    def test_unknown_policy_is_rejected_before_data_loading(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported confirmation_policy"):
            sweep.build_signal_table(
                None,
                square_off="1530",
                max_forward_bars=10,
                mapped_universe=self._mapped_universe(),
                confirmation_policy="not-a-policy",
            )

    def test_explicit_source_roots_are_routed_through_the_builder(self) -> None:
        minute = self._one_minute_path("LONG")
        five = self._five_minute_signal("LONG")
        nonempty = pd.DataFrame({"present": [True]})
        futures_root = Path("frozen/futures")
        equity_root = Path("frozen/equity")
        with (
            patch.object(
                sweep, "load_five_minute_history", return_value=nonempty
            ) as futures_loader,
            patch.object(
                sweep, "load_minute_history", return_value=minute
            ) as equity_loader,
            patch.object(
                sweep.hybrid,
                "aggregate_equity_one_minute_to_five_minute",
                return_value=nonempty,
            ),
            patch.object(
                sweep.hybrid,
                "join_equity_price_with_futures_oi",
                return_value=five,
            ),
        ):
            signals, _ = sweep.build_signal_table(
                {pd.Timestamp("2026-08-18").date()},
                square_off="1530",
                max_forward_bars=10,
                mapped_universe=self._mapped_universe(),
                confirmation_policy=sweep.CONFIRMATION_POLICY_V7_BREAKOUT,
                futures_5m_root=futures_root,
                equity_1m_root=equity_root,
            )

        self.assertEqual(len(signals), 1)
        futures_loader.assert_called_once_with(
            "TEST26AUGFUT", root=futures_root
        )
        equity_loader.assert_called_once_with("TEST", root=equity_root)

    def test_explicit_futures_root_uses_v6_columns_and_timestamp_contract(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = root / "TEST26AUGFUT_5minute.parquet"
            pd.DataFrame(
                {
                    "timestamp": [pd.Timestamp("2026-08-18 03:55", tz="UTC")],
                    "open": [100.0],
                    "high": [102.0],
                    "low": [99.0],
                    "close": [101.0],
                    "volume": [1_000.0],
                    "oi": [2_000.0],
                    "ignored": ["not loaded"],
                }
            ).to_parquet(path, index=False)

            loaded = sweep.load_five_minute_history(
                "TEST26AUGFUT", root=root
            )

        self.assertEqual(
            loaded.columns.tolist(),
            ["timestamp", "open", "high", "low", "close", "volume", "oi", "ts"],
        )
        self.assertEqual(loaded.iloc[0]["ts"].strftime("%Y-%m-%d %H:%M"), "2026-08-18 09:25")
        self.assertEqual(str(loaded["ts"].dt.tz), "Asia/Kolkata")

    def test_explicit_equity_root_resolves_alias_inside_that_root(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "LTIM_stocks_indicators_1min.parquet").touch()
            resolved = sweep._resolve_equity_symbol("LTM", root=root)

        self.assertEqual(resolved, "LTIM")


if __name__ == "__main__":
    unittest.main()
