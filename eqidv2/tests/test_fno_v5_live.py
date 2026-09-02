from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v5_live as live
import fno_v5_live_config as config
import log_dashboard_server as dashboard


class FakeBroker:
    def __init__(self, orders: list[dict] | None = None) -> None:
        self.rows = [dict(row) for row in (orders or [])]
        self.placed: list[dict] = []
        self.cancelled: list[str] = []

    def orders(self) -> list[dict]:
        return [dict(row) for row in self.rows]

    def place_order(self, **kwargs) -> str:
        order_id = f"ORDER{len(self.rows) + 1}"
        row = {**kwargs, "order_id": order_id, "status": "OPEN"}
        self.rows.append(row)
        self.placed.append(dict(row))
        return order_id

    def order_history(self, order_id: str) -> list[dict]:
        return [dict(row) for row in self.rows if row.get("order_id") == order_id]

    def cancel_order(self, *, variety: str, order_id: str) -> None:
        self.cancelled.append(order_id)
        for row in self.rows:
            if row.get("order_id") == order_id:
                row["status"] = "CANCELLED"


def _mapped_universe(underlyings: list[str]) -> pd.DataFrame:
    rows = []
    for index, underlying in enumerate(underlyings, start=1):
        futures_symbol = f"{underlying}26AUGFUT"
        rows.append(
            {
                "exchange": "NFO",
                "tradingsymbol": futures_symbol,
                "underlying": underlying,
                "instrument_token": 10_000 + index,
                "expiry": pd.Timestamp("2026-08-27"),
                "lot_size": 100,
                "tick_size": 0.05,
                "futures_tradingsymbol": futures_symbol,
                "futures_instrument_token": 10_000 + index,
                "futures_lot_size": 100,
                "futures_tick_size": 0.05,
                "equity_symbol": underlying,
                "equity_instrument_token": 20_000 + index,
                "equity_tick_size": 0.05,
            }
        )
    return pd.DataFrame(rows)


def _v2_fetch_marker(
    slot: datetime,
    universe: pd.DataFrame,
    skipped: set[str] | None = None,
) -> dict:
    skipped_symbols = {str(value).upper() for value in (skipped or set())}
    expected_symbols = {
        str(value).upper() for value in universe["futures_tradingsymbol"]
    }
    written_symbols = expected_symbols - skipped_symbols
    coverage = len(written_symbols) / len(expected_symbols)
    return {
        "schema_version": common.FNO_FETCH_SLOT_SCHEMA_VERSION,
        "slot_ist": slot.isoformat(),
        "source": "final",
        "state": "SUCCESS",
        "complete": True,
        "outcome_symbol_set_complete": True,
        "stock_outcome_symbol_set_complete": True,
        "contracts_expected": len(expected_symbols),
        "contracts_written": len(written_symbols),
        "no_candle_count": len(skipped_symbols),
        "no_candle_symbols": sorted(skipped_symbols),
        "invalid_data_count": 0,
        "failed_count": 0,
        "stock_universe_sha256": common.universe_sha256(universe),
        "stock_symbol_set_sha256": common.symbol_set_sha256(expected_symbols),
        "stock_contracts_expected": len(expected_symbols),
        "stock_contracts_written": len(written_symbols),
        "stock_written_symbols": sorted(written_symbols),
        "stock_no_candle_count": len(skipped_symbols),
        "stock_no_candle_symbols": sorted(skipped_symbols),
        "stock_verified_no_candle_count": len(skipped_symbols),
        "stock_verified_no_candle_symbols": sorted(skipped_symbols),
        "stock_unverified_no_candle_symbols": [],
        "stock_invalid_data_count": 0,
        "stock_failed_count": 0,
        "stock_coverage_ratio": coverage,
        "stock_complete": True,
        "minimum_coverage": common.MIN_STOCK_FUTURES_COVERAGE,
        "minimum_stock_coverage": common.MIN_STOCK_FUTURES_COVERAGE,
        "maximum_verified_no_candle_stocks": common.MAX_VERIFIED_NO_CANDLE_STOCKS,
        "minimum_no_candle_fetch_attempts": common.MIN_NO_CANDLE_FETCH_ATTEMPTS,
        "readiness_policy": common.VERIFIED_NO_CANDLE_POLICY_VERSION,
        "no_candle_fetch_attempts": {
            symbol: common.MIN_NO_CANDLE_FETCH_ATTEMPTS
            for symbol in skipped_symbols
        },
        "no_candle_observations": {
            symbol: common.MIN_NO_CANDLE_FETCH_ATTEMPTS
            for symbol in skipped_symbols
        },
    }


def _cash_marker(slot: datetime, universe: pd.DataFrame) -> dict:
    count = len(universe)
    return {
        "slot_ist": slot.isoformat(),
        "source": "final",
        "complete": True,
        "tickers_expected": count,
        "tickers_written": count,
        "tickers_complete": count,
        "tickers_failed": 0,
        "fno_equity_expected": count,
        "fno_equity_ready": count,
        "fno_equity_failed": 0,
        "fno_equity_quality_complete": True,
        "fno_equity_universe_sha256": live._equity_universe_sha256(universe),
    }


class FnoV5LiveTests(unittest.TestCase):
    def _sample_signal(self) -> dict:
        session = date(2026, 8, 10)
        candidate = {
            "tradingsymbol": "TEST",
            "exchange": "NSE",
            "underlying": "TEST",
            "instrument_token": 123,
            "lot_size": 1,
            "tick_size": 0.05,
            "futures_tradingsymbol": "TEST26AUGFUT",
            "futures_instrument_token": 456,
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "side": "SHORT",
            "signal_end": "09:40",
            "signal_timestamp": "2026-08-10T09:40:00+05:30",
            "signal_close": 100.4,
            "price_change_pct": -0.5,
            "oi_change_pct": 1.2,
            "volume_ratio": 2.0,
            "traded_value": 20_000_000.0,
            "ema9": 99.0,
            "ema20": 100.0,
            "ema50": 101.0,
            "confirm_open": 100.3,
            "confirm_high": 100.4,
            "confirm_low": 100.0,
            "confirm_close": 100.05,
            "confirm_volume": 500.0,
            "confirmation_timestamp": "2026-08-10T09:41:00+05:30",
            "body_ratio": 0.625,
            "wick_ratio": 0.125,
            "trigger": 100.0,
            "confirmed": True,
            "confirmation_reason": "ok",
        }
        return live.select_entry_signals([candidate], session, "09:40")[0]

    def test_setup_chain_matches_selected_train_only_optimizer(self) -> None:
        expected = {
            ("09:25", "LONG"): ("FORCE_DAILY", 1, "max_liquidity", 1.0, 2.0),
            ("09:25", "SHORT"): ("FILTERED", 2, "max_volume", 0.4, 3.0),
            ("09:40", "SHORT"): ("FILTERED", 2, "max_oi", 0.3, 1.5),
        }

        self.assertEqual(config.SELECTED_OBJECTIVE, "TRAIN_ONLY_ROBUST_V5_OPTIMIZER")
        self.assertEqual(config.CAPITAL_PER_ENTRY_RS, 10_000.0)
        self.assertEqual(config.LEVERAGE, 5.0)
        self.assertEqual(config.TARGET_EXPOSURE_RS, 50_000.0)
        self.assertEqual(
            {
                (setup.signal_end, setup.side): (
                    setup.mode,
                    setup.max_entries,
                    setup.picker,
                    setup.stop_pct,
                    setup.target_pct,
                )
                for setup in config.ACTIVE_SETUPS
            },
            expected,
        )
        self.assertIsNone(config.setup_for("09:30", "LONG"))
        self.assertIsNone(config.setup_for("09:30", "SHORT"))
        self.assertIsNone(config.setup_for("09:35", "LONG"))
        self.assertIsNone(config.setup_for("09:35", "SHORT"))
        self.assertIsNone(config.setup_for("09:40", "LONG"))
        self.assertIsNone(config.setup_for("09:45", "LONG"))
        self.assertIsNone(config.setup_for("09:45", "SHORT"))

    def test_selected_backtest_attestation(self) -> None:
        observed = config.attest_selected_backtest()

        self.assertEqual(observed["sessions"], 52)
        self.assertEqual(observed["orders"], 71)
        self.assertEqual(observed["fills"], 69)
        self.assertAlmostEqual(observed["trade_pf"], 1.976662, places=6)
        self.assertAlmostEqual(observed["day_pf"], 2.232425, places=6)
        self.assertAlmostEqual(observed["net_pct"], 25.004116, places=6)

    def test_five_minute_scanner_uses_continuous_ema_and_loose_base_gates(self) -> None:
        session = date(2026, 8, 10)
        timestamps = pd.date_range(
            end=pd.Timestamp("2026-08-10 09:40", tz=common.IST),
            periods=70,
            freq="5min",
        )
        closes = np.linspace(90.0, 100.0, 70)
        closes[-1] = closes[-2] * 1.005
        volumes = np.full(70, 100.0)
        volumes[-1] = 500.0
        oi = np.linspace(100_000.0, 107_000.0, 70)
        equity_frame = pd.DataFrame(
            {
                "date": timestamps,
                "ts": timestamps,
                "open": closes - 0.1,
                "high": closes + 0.2,
                "low": closes - 0.2,
                "close": closes,
                "volume": volumes,
            }
        )
        futures_frame = pd.DataFrame(
            {
                "timestamp": timestamps,
                "ts": timestamps,
                "open": np.full(70, 500.0),
                "high": np.full(70, 501.0),
                "low": np.full(70, 499.0),
                "close": np.linspace(510.0, 500.0, 70),
                "volume": np.full(70, 1.0),
                "oi": oi,
            }
        )
        universe = pd.DataFrame(
            {
                "tradingsymbol": ["TEST26AUGFUT"],
                "underlying": ["TEST"],
                "instrument_token": [123],
                "lot_size": [100],
                "tick_size": [0.05],
                "futures_tradingsymbol": ["TEST26AUGFUT"],
                "futures_instrument_token": [456],
                "futures_lot_size": [100],
                "futures_tick_size": [0.05],
                "equity_symbol": ["TEST"],
                "equity_instrument_token": [123],
                "equity_tick_size": [0.05],
            }
        )

        with (
            patch.object(live.backtest, "load_five_minute", return_value=futures_frame),
            patch.object(live.hybrid, "load_equity_five_minute", return_value=equity_frame),
        ):
            snapshot = live.scan_five_minute_slot(universe, session, "09:40")

        self.assertEqual(snapshot["contracts_evaluated"], 1)
        self.assertEqual(snapshot["long_candidates"], 1)
        self.assertEqual(snapshot["short_candidates"], 0)
        self.assertEqual(snapshot["candidates"][0]["side"], "LONG")
        self.assertEqual(snapshot["candidates"][0]["tradingsymbol"], "TEST")
        self.assertEqual(
            snapshot["candidates"][0]["futures_tradingsymbol"], "TEST26AUGFUT"
        )
        self.assertAlmostEqual(snapshot["candidates"][0]["signal_close"], closes[-1])
        self.assertNotAlmostEqual(
            snapshot["candidates"][0]["signal_close"], futures_frame.iloc[-1]["close"]
        )
        self.assertGreater(snapshot["candidates"][0]["ema9"], snapshot["candidates"][0]["ema20"])

    def test_scanner_requires_complete_fno_and_live_cash_markers(self) -> None:
        session = date(2026, 8, 10)
        slot = config.slot_datetime(session, "09:25")
        fno_marker = {
            "slot_ist": slot.isoformat(),
            "source": "final",
            "state": "SUCCESS",
            "complete": True,
            "contracts_expected": 2,
            "contracts_written": 2,
            "no_candle_count": 0,
            "invalid_data_count": 0,
            "failed_count": 0,
        }
        cash_marker = {
            "slot_ist": slot.isoformat(),
            "source": "final",
            "complete": True,
            "tickers_expected": 3,
            "tickers_written": 3,
            "tickers_complete": 3,
            "tickers_failed": 0,
            "fno_equity_expected": 3,
            "fno_equity_ready": 3,
            "fno_equity_failures": 0,
            "fno_equity_quality_complete": True,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with (
                patch.object(common, "FETCH_SLOT_DIR", root / "fno"),
                patch.object(common, "CASH_SLOT_DIR", root / "cash"),
            ):
                common.atomic_write_json(common.fetch_slot_path(slot), fno_marker)
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25"),
                    (False, "cash_5m_marker_missing"),
                )

                incomplete_cash = {**cash_marker, "complete": False}
                common.atomic_write_json(common.cash_slot_path(slot), incomplete_cash)
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25"),
                    (False, "cash_5m_marker_incomplete"),
                )

                common.atomic_write_json(common.cash_slot_path(slot), cash_marker)
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25"),
                    (True, "ready"),
                )

                bad_fno_equity_quality = {
                    **cash_marker,
                    "fno_equity_ready": 2,
                    "fno_equity_failures": 1,
                    "fno_equity_quality_complete": False,
                }
                common.atomic_write_json(
                    common.cash_slot_path(slot), bad_fno_equity_quality
                )
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25"),
                    (False, "cash_5m_marker_fno_equity_quality_incomplete"),
                )

                common.atomic_write_json(common.cash_slot_path(slot), cash_marker)

                complete_with_no_candle = {
                    **fno_marker,
                    "contracts_expected": 100,
                    "contracts_written": 99,
                    "no_candle_count": 1,
                    "coverage_ratio": 0.99,
                    "minimum_coverage": 0.80,
                    "attempt_complete": True,
                }
                common.atomic_write_json(
                    common.fetch_slot_path(slot), complete_with_no_candle
                )
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25"),
                    (False, "fno_fetch_marker_legacy_no_candle_unverifiable"),
                )

                mapped = _mapped_universe(
                    [f"STOCK{index:03d}" for index in range(99)] + ["SAIL"]
                )
                sail_future = "SAIL26AUGFUT"
                common.atomic_write_json(
                    common.fetch_slot_path(slot),
                    _v2_fetch_marker(slot, mapped, {sail_future}),
                )
                common.atomic_write_json(
                    common.cash_slot_path(slot), _cash_marker(slot, mapped)
                )
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25", mapped),
                    (True, "ready"),
                )

                partial_fno = {**fno_marker, "contracts_written": 1}
                common.atomic_write_json(common.fetch_slot_path(slot), partial_fno)
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25"),
                    (False, "fno_fetch_marker_incomplete_coverage"),
                )

    def test_v2_marker_rejects_tampered_skip_evidence_and_low_coverage(self) -> None:
        session = date(2026, 8, 10)
        slot = config.slot_datetime(session, "09:25")
        mapped = _mapped_universe(
            [f"STOCK{index:03d}" for index in range(98)] + ["SAIL", "OTHER"]
        )
        sail_future = "SAIL26AUGFUT"

        bad_hash = _v2_fetch_marker(slot, mapped, {sail_future})
        bad_hash["stock_universe_sha256"] = "tampered"
        weak_evidence = _v2_fetch_marker(slot, mapped, {sail_future})
        weak_evidence["no_candle_observations"] = {sail_future: 2}
        weak_policy = _v2_fetch_marker(slot, mapped, {sail_future})
        weak_policy["maximum_verified_no_candle_stocks"] = 999
        bad_partition = _v2_fetch_marker(slot, mapped, {sail_future})
        bad_partition["stock_written_symbols"] = bad_partition[
            "stock_written_symbols"
        ][1:]
        low_coverage = _v2_fetch_marker(
            slot, mapped, {"SAIL26AUGFUT", "OTHER26AUGFUT"}
        )
        cases = {
            "full universe hash": (
                bad_hash,
                "fno_fetch_marker_stock_universe_mismatch",
            ),
            "clean observations": (
                weak_evidence,
                "fno_fetch_marker_no_candle_not_repeatedly_verified",
            ),
            "locked cap": (weak_policy, "fno_fetch_marker_no_candle_cap_mismatch"),
            "exact partition": (
                bad_partition,
                "fno_fetch_marker_stock_partition_mismatch",
            ),
            "recomputed coverage": (
                low_coverage,
                "fno_fetch_marker_stock_incomplete_coverage",
            ),
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with (
                patch.object(common, "FETCH_SLOT_DIR", root / "fno"),
                patch.object(common, "CASH_SLOT_DIR", root / "cash"),
            ):
                common.atomic_write_json(
                    common.cash_slot_path(slot), _cash_marker(slot, mapped)
                )
                for name, (marker, expected_reason) in cases.items():
                    with self.subTest(name=name):
                        common.atomic_write_json(common.fetch_slot_path(slot), marker)
                        self.assertEqual(
                            live._slot_marker_ready(session, "09:25", mapped),
                            (False, expected_reason),
                        )
                stricter = _v2_fetch_marker(slot, mapped)
                stricter["minimum_coverage"] = 1.0
                stricter["minimum_stock_coverage"] = 1.0
                common.atomic_write_json(common.fetch_slot_path(slot), stricter)
                self.assertEqual(
                    live._slot_marker_ready(session, "09:25", mapped),
                    (True, "ready"),
                )

    def test_verified_no_candle_is_skipped_without_hindsight_data_load(self) -> None:
        session = date(2026, 8, 10)
        slot = config.slot_datetime(session, "09:25")
        universe = _mapped_universe(["TEST", "SAIL"])
        featured = pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp(slot),
                    "close": 100.0,
                    "price_change_pct": 0.5,
                    "oi_change_pct": 1.0,
                    "oi": 101_000.0,
                    "prev_oi": 100_000.0,
                    "volume_ratio": 4.0,
                    "traded_value": 25_000_000.0,
                    "ema9": 101.0,
                    "ema20": 100.0,
                    "ema50": 99.0,
                }
            ]
        )

        def load_future(symbol: str) -> pd.DataFrame:
            self.assertEqual(symbol, "TEST26AUGFUT")
            return pd.DataFrame({"present": [True]})

        def load_equity(symbol: str, *, root: Path) -> pd.DataFrame:
            self.assertEqual(symbol, "TEST")
            return pd.DataFrame({"present": [True]})

        with (
            patch.object(live.backtest, "load_five_minute", side_effect=load_future) as future_load,
            patch.object(
                live.hybrid, "load_equity_five_minute", side_effect=load_equity
            ) as equity_load,
            patch.object(
                live.hybrid,
                "join_equity_price_with_futures_oi",
                return_value=featured,
            ),
        ):
            snapshot = live.scan_five_minute_slot(
                universe,
                session,
                "09:25",
                verified_no_candle_symbols={"SAIL26AUGFUT"},
            )

        self.assertEqual(snapshot["state"], "SUCCESS")
        self.assertEqual(snapshot["schema_version"], "fno_v5_scanner_5m_hybrid_v3")
        self.assertEqual(snapshot["contracts_evaluated"], 1)
        self.assertEqual(snapshot["contracts_skipped_no_candle"], 1)
        self.assertEqual(snapshot["skipped_no_candle_symbols"], ["SAIL26AUGFUT"])
        self.assertEqual(snapshot["contracts_unexpected_missing"], 0)
        self.assertEqual(snapshot["long_candidates"], 1)
        self.assertEqual(future_load.call_count, 1)
        self.assertEqual(equity_load.call_count, 1)

        confirmation_args = SimpleNamespace(
            max_retries=1,
            request_interval_sec=0.0,
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )
        with patch.object(
            live,
            "_load_completed_confirmation_feed",
            return_value=({}, {"_feed": "test-marker-missing"}, {}),
        ) as load_feed:
            confirmation = live.process_confirmation_slot(
                snapshot,
                session,
                "09:25",
                SimpleNamespace(),
                confirmation_args,
            )
        self.assertEqual(confirmation["candidate_count"], 1)
        load_feed.assert_called_once_with(snapshot, session, "09:25")
        self.assertFalse(hasattr(live, "fetch_confirmation_bars"))

    def test_unlisted_missing_contract_remains_partial(self) -> None:
        session = date(2026, 8, 10)
        universe = _mapped_universe(["SAIL", "MISSING"])

        with (
            patch.object(live.backtest, "load_five_minute", return_value=pd.DataFrame()),
            patch.object(
                live.hybrid, "load_equity_five_minute", return_value=pd.DataFrame()
            ),
        ):
            snapshot = live.scan_five_minute_slot(
                universe,
                session,
                "09:25",
                verified_no_candle_symbols={"SAIL26AUGFUT"},
            )

        self.assertEqual(snapshot["state"], "PARTIAL")
        self.assertEqual(snapshot["contracts_skipped_no_candle"], 1)
        self.assertEqual(snapshot["contracts_unexpected_missing"], 1)
        self.assertEqual(
            snapshot["unexpected_missing_symbols"], ["MISSING26AUGFUT"]
        )

    def test_confirmation_reads_only_durable_feed_for_scanner_candidates(self) -> None:
        session = date(2026, 8, 10)
        candidates = [
            {"tradingsymbol": "ONE", "instrument_token": 101},
            {"tradingsymbol": "TWO", "instrument_token": 102},
        ]
        snapshot = {
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "session_date": session.isoformat(),
            "signal_end": "09:25",
            "state": "SUCCESS",
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "candidates": candidates,
        }
        args = SimpleNamespace(
            max_retries=1,
            request_interval_sec=0.0,
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )
        feed_result = (
            {},
            {"_feed": "durable_confirmation_marker_missing"},
            {},
        )

        with patch.object(
            live, "_load_completed_confirmation_feed", return_value=feed_result
        ) as load_feed:
            result = live.process_confirmation_slot(
                snapshot,
                session,
                "09:25",
                SimpleNamespace(),
                args,
            )

        load_feed.assert_called_once_with(snapshot, session, "09:25")
        self.assertFalse(hasattr(live, "fetch_confirmation_bars"))
        self.assertEqual(result["candidate_count"], 2)
        self.assertEqual(result["confirmation_bars"], 0)
        self.assertEqual(result["state"], "BLOCKED_INCOMPLETE_DATA")

    def test_direct_confirmation_api_polling_path_is_removed(self) -> None:
        self.assertFalse(hasattr(live, "fetch_confirmation_bars"))
        self.assertFalse(hasattr(live, "candidate_equity_minute_path"))
        self.assertFalse(hasattr(live, "EQUITY_1M_CANDIDATE_ROOT"))

    def test_confirmation_filters_rank_and_enforce_short_cap(self) -> None:
        session = date(2026, 8, 10)
        base = {
            "underlying": "TEST",
            "instrument_token": 123,
            "exchange": "NSE",
            "lot_size": 1,
            "tick_size": 0.05,
            "futures_tradingsymbol": "TEST26AUGFUT",
            "futures_instrument_token": 456,
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "side": "SHORT",
            "signal_end": "09:40",
            "signal_timestamp": "2026-08-10T09:40:00+05:30",
            "signal_close": 100.4,
            "price_change_pct": -0.5,
            "oi_change_pct": 1.2,
            "volume_ratio": 2.0,
            "traded_value": 20_000_000.0,
            "ema9": 99.0,
            "ema20": 100.0,
            "ema50": 101.0,
        }
        candidates = []
        for symbol, oi_change in (("A", 1.2), ("B", 1.8), ("C", 1.5)):
            candidate = {**base, "tradingsymbol": symbol, "oi_change_pct": oi_change}
            candidates.append(
                live.confirmation_metrics(
                    candidate,
                    {
                        "timestamp": "2026-08-10T09:40:00+05:30",
                        "open": 100.3,
                        "high": 100.4,
                        "low": 100.0,
                        "close": 100.05,
                        "volume": 500,
                    },
                )
            )

        signals = live.select_entry_signals(candidates, session, "09:40")

        self.assertEqual(len(signals), 2)
        self.assertEqual([signal["tradingsymbol"] for signal in signals], ["B", "C"])
        self.assertTrue(all(signal["side"] == "SHORT" for signal in signals))
        self.assertEqual([signal["rank_within_scan"] for signal in signals], [1, 2])
        self.assertTrue(all(signal["max_entries"] == 2 for signal in signals))
        self.assertAlmostEqual(signals[0]["trigger_price"], 100.0)
        self.assertAlmostEqual(signals[0]["stop_price"], 100.3)
        self.assertAlmostEqual(signals[0]["target_price"], 98.5)

    def test_position_sizing_is_10000_at_5x_and_live_is_lot_safe(self) -> None:
        paper = config.size_position(250.0, 300, live=False)
        live_ok = config.size_position(250.0, 100, live=True)
        live_blocked = config.size_position(250.0, 300, live=True)

        self.assertEqual(paper.target_exposure_rs, 50_000.0)
        self.assertEqual(paper.quantity, 200)
        self.assertEqual(live_ok.quantity, 200)
        self.assertEqual(live_ok.state, "LIVE_LOT_SIZED")
        self.assertEqual(live_blocked.quantity, 0)
        self.assertEqual(live_blocked.state, "BLOCKED_LOT_EXCEEDS_BUDGET")

    def test_live_quantity_override_persists_exactly_one_share(self) -> None:
        signal = self._sample_signal()

        state = live.create_order_state(signal, "LIVE", live_quantity=1)

        self.assertGreater(signal["live_sizing"]["quantity"], 1)
        self.assertEqual(state["quantity"], 1)
        self.assertEqual(
            state["strategy_sized_quantity"], signal["live_sizing"]["quantity"]
        )
        self.assertEqual(state["execution_quantity_override"], 1)
        self.assertEqual(state["status"], "PENDING_ENTRY")
        live._validate_order_state(state, signal, "LIVE", live_quantity=1)

    def test_live_quantity_override_validation_rejects_tampering(self) -> None:
        signal = self._sample_signal()
        state = live.create_order_state(signal, "LIVE", live_quantity=1)

        tampered_quantity = dict(state)
        tampered_quantity["quantity"] = 2
        with self.assertRaisesRegex(RuntimeError, "invalid quantity 2"):
            live._validate_order_state(
                tampered_quantity,
                signal,
                "LIVE",
                live_quantity=1,
            )

        tampered_override = dict(state)
        tampered_override["execution_quantity_override"] = 2
        with self.assertRaisesRegex(RuntimeError, "required LIVE quantity override 1"):
            live._validate_order_state(
                tampered_override,
                signal,
                "LIVE",
                live_quantity=1,
            )

    def test_live_quantity_override_does_not_change_paper_quantity(self) -> None:
        signal = self._sample_signal()

        state = live.create_order_state(signal, "PAPER")

        self.assertEqual(state["quantity"], signal["paper_sizing"]["quantity"])
        self.assertEqual(
            state["strategy_sized_quantity"], signal["paper_sizing"]["quantity"]
        )
        self.assertIsNone(state["execution_quantity_override"])
        live._validate_order_state(state, signal, "PAPER")

    def test_paper_order_requires_trigger_then_uses_selected_bracket_and_cost(self) -> None:
        signal = {
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "signal_id": "test_signal",
            "session_date": "2026-08-10",
            "signal_end": "09:40",
            "confirmation_end": "09:41",
            "entry_activation_deadline_ist": "2026-08-10T09:42:30+05:30",
            "side": "SHORT",
            "tradingsymbol": "TEST",
            "exchange": "NSE",
            "instrument_token": 123,
            "futures_tradingsymbol": "TEST26AUGFUT",
            "futures_instrument_token": 456,
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "tick_size": 0.05,
            "lot_size": 1,
            "capital_rs": 10_000.0,
            "leverage": 5.0,
            "target_exposure_rs": 50_000.0,
            "trigger_price": 100.0,
            "stop_pct": 0.3,
            "target_pct": 1.5,
            "stop_price": 100.3,
            "target_price": 98.5,
            "round_trip_cost_bps": 5.0,
            "paper_sizing": {
                "quantity": 500,
                "state": "PAPER_EXPOSURE_SIZED",
            },
            "live_sizing": {"quantity": 0, "state": "BLOCKED_LOT_EXCEEDS_BUDGET"},
        }
        state = live.create_order_state(signal, "PAPER")
        before = datetime(2026, 8, 10, 9, 42, tzinfo=common.IST)

        state = live.advance_paper_order(state, 100.1, before)
        self.assertEqual(state["status"], "PENDING_ENTRY")
        state = live.advance_paper_order(state, 99.95, before)
        self.assertEqual(state["status"], "OPEN")
        self.assertAlmostEqual(state["stop_price"], 100.25)
        self.assertAlmostEqual(state["target_price"], 98.45)
        state = live.advance_paper_order(state, 98.40, before)
        self.assertEqual(state["status"], "CLOSED")
        self.assertEqual(state["exit_reason"], "TARGET")
        self.assertGreater(state["gross_pnl_rs"], state["net_pnl_rs"])
        self.assertGreater(state["estimated_cost_rs"], 0)

    def test_pending_paper_entry_cancels_after_activation_deadline(self) -> None:
        signal = self._sample_signal()
        state = live.create_order_state(signal, "PAPER")
        after_deadline = datetime.fromisoformat(
            signal["entry_activation_deadline_ist"]
        ) + pd.Timedelta(seconds=1)

        state = live.advance_paper_order(
            state,
            float(signal["trigger_price"]) - 0.05,
            after_deadline,
        )

        self.assertEqual(state["status"], "CANCELLED")
        self.assertEqual(
            state["status_reason"], "ENTRY_ACTIVATION_DEADLINE_EXPIRED"
        )
        self.assertEqual(state["entry_price"], 0.0)

    def test_live_orders_are_not_armed_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            arm = Path(temp_dir) / "arm.json"
            kill = Path(temp_dir) / "kill.json"
            with (
                patch.object(live, "LIVE_ARM_PATH", arm),
                patch.object(live, "KILL_SWITCH_PATH", kill),
                patch.dict("os.environ", {}, clear=True),
            ):
                armed, reason = live._live_arm_state(date(2026, 8, 10))

        self.assertFalse(armed)
        self.assertEqual(reason, "LIVE_ACK_MISSING")

    def test_incomplete_scanner_snapshot_is_fail_closed(self) -> None:
        session = date(2026, 8, 10)
        signal = self._sample_signal()
        snapshot = {
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "session_date": session.isoformat(),
            "signal_end": "09:40",
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "state": "PARTIAL",
            "candidates": [signal],
        }
        args = SimpleNamespace(
            max_retries=1,
            request_interval_sec=0.0,
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )

        result = live.process_confirmation_slot(
            snapshot, session, "09:40", None, args
        )

        self.assertEqual(result["state"], "BLOCKED_INCOMPLETE_DATA")
        self.assertEqual(result["selected_signal_ids"], [])
        self.assertFalse(result["scanner_complete"])

    def test_confirmation_once_before_completed_boundary_waits_without_commit(self) -> None:
        session = date(2026, 8, 10)
        due = config.slot_datetime(session, "09:26") + pd.Timedelta(seconds=3)
        args = SimpleNamespace(
            once=True,
            boundary_buffer_sec=3.0,
            confirmation_max_wait_sec=90.0,
            poll_sec=0.0,
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )
        with (
            patch.object(live, "_current_slot_snapshot", return_value=False),
            patch.object(live, "_render_confirmation_report", return_value=""),
            patch.object(common, "atomic_write_text"),
            patch.object(common, "now_ist", return_value=due - pd.Timedelta(microseconds=1)),
            patch.object(live, "_read_json") as read_json,
            patch.object(live, "_commit_confirmation_decision") as commit,
            patch.object(live, "_write_confirmation_snapshot") as write_snapshot,
            patch.object(live, "_heartbeat"),
            patch.object(live, "_publish") as publish,
        ):
            result = live.run_confirmation(args, session)

        self.assertEqual(result, 2)
        read_json.assert_not_called()
        commit.assert_not_called()
        write_snapshot.assert_not_called()
        self.assertTrue(
            any(
                call.args[1] == "WAITING"
                and call.kwargs.get("phase") == "WAIT_COMPLETED_CANDLE_BOUNDARY"
                for call in publish.call_args_list
            )
        )

    def test_manual_confirmation_before_completed_boundary_waits_without_snapshot(self) -> None:
        session = date(2026, 8, 10)
        due = config.slot_datetime(session, "09:26") + pd.Timedelta(seconds=3)
        args = live.build_parser().parse_args(
            [
                "--role", "confirmation-1m",
                "--session-date", session.isoformat(),
                "--slot", "09:25",
                "--allow-non-trading-day",
            ]
        )
        with (
            patch.object(config, "attest_selected_backtest", return_value={}),
            patch.object(live, "_write_manifest"),
            patch.object(common, "now_ist", return_value=due - pd.Timedelta(microseconds=1)),
            patch.object(live, "_read_json") as read_json,
            patch.object(live, "_commit_confirmation_decision") as commit,
            patch.object(live, "_write_confirmation_snapshot") as write_snapshot,
            patch.object(live, "_publish") as publish,
        ):
            result = live.run(args)

        self.assertEqual(result, 2)
        read_json.assert_not_called()
        commit.assert_not_called()
        write_snapshot.assert_not_called()
        self.assertEqual(publish.call_args.args[1], "WAITING")
        self.assertEqual(
            publish.call_args.kwargs["phase"], "WAIT_COMPLETED_CANDLE_BOUNDARY"
        )

    def test_manual_confirmation_waits_for_missing_feed_before_deadline(self) -> None:
        session = date(2026, 8, 10)
        now = config.slot_datetime(session, "09:26") + pd.Timedelta(seconds=4)
        source = {
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "session_date": session.isoformat(),
            "signal_end": "09:25",
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
            "state": "SUCCESS",
            "candidates": [{"tradingsymbol": "AMBER", "instrument_token": 101}],
        }
        args = live.build_parser().parse_args(
            [
                "--role", "confirmation-1m",
                "--session-date", session.isoformat(),
                "--slot", "09:25",
                "--allow-non-trading-day",
            ]
        )
        with (
            patch.object(config, "attest_selected_backtest", return_value={}),
            patch.object(live, "_write_manifest"),
            patch.object(common, "now_ist", return_value=now),
            patch.object(live, "_read_json", return_value=source),
            patch.object(
                live,
                "_load_completed_confirmation_feed",
                return_value=(
                    {},
                    {"_feed": "durable_confirmation_marker_missing"},
                    {},
                ),
            ),
            patch.object(live, "_commit_confirmation_decision") as commit,
            patch.object(live, "_write_confirmation_snapshot") as write_snapshot,
            patch.object(live, "_publish") as publish,
        ):
            result = live.run(args)

        self.assertEqual(result, 2)
        commit.assert_not_called()
        write_snapshot.assert_not_called()
        self.assertEqual(publish.call_args.args[1], "WAITING")
        self.assertEqual(publish.call_args.kwargs["phase"], "WAIT_CONFIRM_BAR")

    def test_confirmation_restart_processes_slot_after_early_once_wait(self) -> None:
        session = date(2026, 8, 10)
        confirmation_end = config.slot_datetime(session, "09:26")
        early = confirmation_end + pd.Timedelta(seconds=2)
        ready = confirmation_end + pd.Timedelta(seconds=4)
        source = {"state": "SUCCESS"}
        success = {
            "state": "SUCCESS",
            "scanner_complete": True,
            "error_count": 0,
            "selected_long": 0,
            "selected_short": 0,
            "_selected_signals": [],
        }
        args = SimpleNamespace(
            once=True,
            boundary_buffer_sec=3.0,
            confirmation_max_wait_sec=90.0,
            poll_sec=0.0,
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )
        with (
            patch.object(live, "_current_slot_snapshot", return_value=False),
            patch.object(live, "_render_confirmation_report", return_value=""),
            patch.object(common, "atomic_write_text"),
            patch.object(
                common,
                "now_ist",
                side_effect=[early, ready, ready],
            ),
            patch.object(live, "_read_json", return_value=source),
            patch.object(live, "process_confirmation_slot", return_value=success) as process,
            patch.object(live, "_commit_confirmation_decision") as commit,
            patch.object(live, "_heartbeat"),
            patch.object(live, "_publish"),
        ):
            self.assertEqual(live.run_confirmation(args, session), 2)
            commit.assert_not_called()
            self.assertEqual(live.run_confirmation(args, session), 0)

        process.assert_called_once()
        commit.assert_called_once()

    def test_confirmation_once_waits_for_provisional_feed_before_deadline(self) -> None:
        session = date(2026, 8, 10)
        now = config.slot_datetime(session, "09:26") + pd.Timedelta(seconds=4)
        source = {"state": "SUCCESS"}
        blocked = {
            "state": "BLOCKED_INCOMPLETE_DATA",
            "scanner_complete": True,
            "error_count": 1,
            "selected_long": 0,
            "selected_short": 0,
            "_selected_signals": [],
        }
        args = SimpleNamespace(
            once=True,
            boundary_buffer_sec=3.0,
            confirmation_max_wait_sec=90.0,
            poll_sec=0.0,
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )
        with (
            patch.object(live, "_current_slot_snapshot", return_value=False),
            patch.object(live, "_render_confirmation_report", return_value=""),
            patch.object(common, "atomic_write_text"),
            patch.object(common, "now_ist", side_effect=[now, now]),
            patch.object(live, "_read_json", return_value=source),
            patch.object(live, "process_confirmation_slot", return_value=blocked),
            patch.object(live, "_commit_confirmation_decision") as commit,
            patch.object(live, "_write_confirmation_snapshot") as write_snapshot,
            patch.object(live, "_heartbeat"),
            patch.object(live, "_publish") as publish,
        ):
            result = live.run_confirmation(args, session)

        self.assertEqual(result, 2)
        commit.assert_not_called()
        write_snapshot.assert_not_called()
        self.assertEqual(publish.call_args.args[1], "WAITING")
        self.assertEqual(publish.call_args.kwargs["phase"], "WAIT_CONFIRM_BAR")

    def test_definitive_partial_scanner_is_committed_before_deadline(self) -> None:
        session = date(2026, 8, 10)
        now = config.slot_datetime(session, "09:26") + pd.Timedelta(seconds=4)
        source = {"state": "PARTIAL"}
        blocked = {
            "state": "BLOCKED_INCOMPLETE_DATA",
            "scanner_complete": False,
            "error_count": 0,
            "selected_long": 0,
            "selected_short": 0,
            "_selected_signals": [],
        }
        args = SimpleNamespace(
            once=True,
            boundary_buffer_sec=3.0,
            confirmation_max_wait_sec=90.0,
            poll_sec=0.0,
            capital=config.CAPITAL_PER_ENTRY_RS,
            leverage=config.LEVERAGE,
        )
        with (
            patch.object(live, "_current_slot_snapshot", return_value=False),
            patch.object(live, "_render_confirmation_report", return_value=""),
            patch.object(common, "atomic_write_text"),
            patch.object(common, "now_ist", side_effect=[now, now]),
            patch.object(live, "_read_json", return_value=source),
            patch.object(live, "process_confirmation_slot", return_value=blocked),
            patch.object(live, "_commit_confirmation_decision") as commit,
            patch.object(live, "_heartbeat"),
            patch.object(live, "_publish"),
        ):
            result = live.run_confirmation(args, session)

        self.assertEqual(result, 2)
        commit.assert_called_once()

    def test_complete_scanner_incomplete_confirmation_is_not_terminal_on_restart(self) -> None:
        session = date(2026, 8, 10)
        snapshot = {
            "strategy_version": config.STRATEGY_VERSION,
            "strategy_fingerprint": config.strategy_fingerprint(),
            "session_date": session.isoformat(),
            "signal_end": "09:25",
            "state": "BLOCKED_INCOMPLETE_DATA",
            "scanner_complete": True,
        }
        with patch.object(live, "_read_json", return_value=snapshot):
            self.assertFalse(
                live._current_slot_snapshot(Path("unused"), session, "09:25")
            )

    def test_signal_validation_rejects_strategy_parameter_drift(self) -> None:
        session = date(2026, 8, 10)
        signal = self._sample_signal()
        live._validate_signal(signal, session)
        signal["stop_pct"] = 0.4

        with self.assertRaisesRegex(RuntimeError, "invalid stop_pct"):
            live._validate_signal(signal, session)

    def test_order_state_validation_rejects_persisted_symbol_drift(self) -> None:
        signal = self._sample_signal()
        state = live.create_order_state(signal, "PAPER")
        live._validate_order_state(state, signal, "PAPER")
        state["tradingsymbol"] = "WRONG"

        with self.assertRaisesRegex(RuntimeError, "invalid tradingsymbol"):
            live._validate_order_state(state, signal, "PAPER")

    def test_dearming_does_not_freeze_protection_for_open_live_position(self) -> None:
        state = live.create_order_state(self._sample_signal(), "LIVE")
        state.update(
            status="OPEN",
            entry_price=99.95,
            entry_at_ist="2026-08-10T09:42:00+05:30",
        )
        broker = FakeBroker()
        now = datetime(2026, 8, 10, 9, 42, tzinfo=common.IST)

        with (
            patch.object(live, "_live_arm_state", return_value=(False, "LIVE_ACK_MISSING")),
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(live, "KILL_SWITCH_PATH", Path(temp_dir) / "kill.json"),
        ):
            state = live.advance_live_order(state, broker, now)

        self.assertEqual(state["status"], "OPEN")
        self.assertTrue(state["stop_order_id"])
        self.assertTrue(state["target_order_id"])
        self.assertEqual(
            [row["order_type"] for row in broker.placed], ["SL-M", "LIMIT"]
        )
        self.assertTrue(all(row["exchange"] == "NSE" for row in broker.placed))

    def test_kill_switch_sends_market_squareoff_for_open_live_position(self) -> None:
        state = live.create_order_state(self._sample_signal(), "LIVE")
        state.update(
            status="OPEN",
            entry_price=99.95,
            entry_at_ist="2026-08-10T09:42:00+05:30",
        )
        broker = FakeBroker()
        now = datetime(2026, 8, 10, 9, 42, tzinfo=common.IST)

        with tempfile.TemporaryDirectory() as temp_dir:
            kill_path = Path(temp_dir) / "kill.json"
            common.atomic_write_json(kill_path, {"enabled": True})
            with (
                patch.object(live, "_live_arm_state", return_value=(False, "KILL_SWITCH_ENABLED")),
                patch.object(live, "KILL_SWITCH_PATH", kill_path),
            ):
                state = live.advance_live_order(state, broker, now)

        self.assertEqual(state["status"], "SQUARE_OFF_PENDING")
        self.assertEqual(state["status_reason"], "KILL_SWITCH_SQUARE_OFF")
        self.assertEqual(broker.placed[-1]["order_type"], "MARKET")

    def test_restart_recovers_tagged_entry_instead_of_placing_duplicate(self) -> None:
        state = live.create_order_state(self._sample_signal(), "LIVE")
        tag = live._live_tag(state["signal_id"])
        broker = FakeBroker(
            [
                {
                    "order_id": "EXISTING1",
                    "tag": tag,
                    "tradingsymbol": state["tradingsymbol"],
                    "transaction_type": "SELL",
                    "order_type": "SL-M",
                    "status": "OPEN",
                }
            ]
        )
        now = datetime(2026, 8, 10, 9, 42, tzinfo=common.IST)

        with (
            patch.object(live, "_live_arm_state", return_value=(False, "LIVE_ACK_MISSING")),
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(live, "KILL_SWITCH_PATH", Path(temp_dir) / "kill.json"),
        ):
            state = live.advance_live_order(state, broker, now)

        self.assertEqual(state["entry_order_id"], "EXISTING1")
        self.assertEqual(state["status"], "CANCELLED")
        self.assertEqual(broker.placed, [])

    def test_working_live_entry_is_cancelled_after_activation_deadline(self) -> None:
        signal = self._sample_signal()
        state = live.create_order_state(signal, "LIVE", live_quantity=1)
        state.update(
            entry_order_id="WORKING1",
            entry_order_activated_at_ist="2026-08-10T09:42:00+05:30",
        )
        broker = FakeBroker([{"order_id": "WORKING1", "status": "OPEN"}])
        after_deadline = datetime.fromisoformat(
            signal["entry_activation_deadline_ist"]
        ) + pd.Timedelta(seconds=1)

        with (
            patch.object(live, "_live_arm_state", return_value=(True, "LIVE_ARMED")),
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(live, "KILL_SWITCH_PATH", Path(temp_dir) / "kill.json"),
        ):
            state = live.advance_live_order(state, broker, after_deadline)

        self.assertEqual(state["quantity"], 1)
        self.assertEqual(state["status"], "CANCELLED")
        self.assertEqual(
            state["status_reason"], "ENTRY_ACTIVATION_DEADLINE_EXPIRED"
        )
        self.assertEqual(broker.cancelled, ["WORKING1"])
        self.assertEqual(broker.placed, [])

    def test_late_live_start_does_not_place_retroactive_entry(self) -> None:
        state = live.create_order_state(self._sample_signal(), "LIVE")
        broker = FakeBroker()
        now = datetime(2026, 8, 10, 9, 43, tzinfo=common.IST)

        with (
            patch.object(live, "_live_arm_state", return_value=(True, "LIVE_ARMED")),
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(live, "KILL_SWITCH_PATH", Path(temp_dir) / "kill.json"),
        ):
            state = live.advance_live_order(state, broker, now)

        self.assertEqual(state["status"], "CANCELLED")
        self.assertEqual(state["status_reason"], "LATE_START_NO_RETROACTIVE_ENTRY")
        self.assertEqual(broker.placed, [])

    def test_downstream_role_reports_upstream_block_instead_of_zero_trade_done(self) -> None:
        session = date(2026, 8, 10)
        args = SimpleNamespace(execution_mode="PAPER")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with patch.object(common, "RUNTIME_STATUS_DIR", root):
                common.publish_status(
                    live.ROLE_SESSIONS["scanner-5m"],
                    "BLOCKED",
                    phase="UNIVERSE_NOT_READY",
                    reason="NIFTYFPI was misclassified",
                )
                status_path = common.session_status_path(
                    live.ROLE_SESSIONS["scanner-5m"]
                )
                status_text = status_path.read_text(encoding="utf-8").replace(
                    common.now_ist().date().isoformat(), session.isoformat()
                )
                status_path.write_text(status_text, encoding="utf-8")

                with (
                    patch.object(live, "load_signals", return_value=[]),
                    patch.object(live, "load_order_states", return_value=[]),
                    patch.object(live, "report_path", return_value=root / "long.md"),
                ):
                    result = live.run_worker(args, session, "LONG")

                report = (root / "long.md").read_text(encoding="utf-8")
                downstream = common.session_status_path(
                    live.ROLE_SESSIONS["long-entry"]
                ).read_text(encoding="utf-8")

        self.assertEqual(result, 2)
        self.assertIn("Pipeline state: **BLOCKED**", report)
        self.assertIn("UNIVERSE_NOT_READY", report)
        self.assertIn("upstream pipeline blocked", report)
        self.assertIn("status=BLOCKED", downstream)
        self.assertIn("phase=UPSTREAM_BLOCKED", downstream)

    def test_dashboard_promotes_all_six_fno_v6_sessions(self) -> None:
        expected = {
            "fno_v6_scanner_5min",
            "fno_v6_confirmation_1min",
            "fno_v6_live_long",
            "fno_v6_live_short",
            "fno_v6_trade_logger",
            "fno_v6_net_result",
        }

        self.assertTrue(expected.issubset(dashboard.FNO_OI_CARD_REPORTS))
        self.assertTrue(expected.issubset(dashboard.LOG_FILES))
        self.assertTrue(expected.issubset(dashboard.STATUS_FILES))
        self.assertTrue(expected.issubset(dashboard.HEARTBEAT_FILES))
        self.assertTrue(expected.issubset(dashboard.CARD_TASK_NAMES))
        self.assertTrue(expected.issubset(dashboard.RESTARTABLE_CARDS))
        for card_id in expected:
            self.assertTrue(
                dashboard.FNO_OI_CARD_REPORTS[card_id].startswith("latest_fno_v6_")
            )

        source = Path(dashboard.__file__).read_text(encoding="utf-8", errors="replace")
        for card_id in expected | {"fno_oi_feature_ranker"}:
            self.assertIn(f'{{ time: "09:15", id: "{card_id}"', source)


if __name__ == "__main__":
    unittest.main()
