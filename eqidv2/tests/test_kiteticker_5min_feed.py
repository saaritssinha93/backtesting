from __future__ import annotations

import logging
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

import eqidv2_kiteticker_5min_live as feed
import log_dashboard_server as dashboard


class KiteTickerFiveMinuteFeedTests(unittest.TestCase):
    ROOT = Path(__file__).resolve().parents[1]

    def test_exact_boundary_trade_belongs_to_next_completed_candle(self) -> None:
        trade = feed.IST.localize(datetime(2026, 7, 31, 12, 20, 0))
        self.assertEqual(
            feed._slot_end_for_trade(trade),
            feed.IST.localize(datetime(2026, 7, 31, 12, 25, 0)),
        )

    def test_canonical_universe_is_distributed_equally_across_eight_apps(self) -> None:
        symbols = [f"SYM{i:04d}" for i in range(1237)]
        partitions = feed.split_tickers_evenly(symbols, 8)
        counts = [len(partition) for partition in partitions]
        self.assertEqual(counts, [155, 155, 155, 155, 155, 154, 154, 154])
        self.assertEqual(
            feed._canonical_symbols(symbol for part in partitions for symbol in part),
            sorted(symbols),
        )
        self.assertEqual(sum(counts), len(symbols))

    def test_aggregator_builds_end_stamped_ohlcv_from_cumulative_volume(self) -> None:
        aggregator = feed.TickAggregator({101: "TEST"})
        first = feed.IST.localize(datetime(2026, 7, 31, 9, 15, 1))
        second = feed.IST.localize(datetime(2026, 7, 31, 9, 16, 2))
        aggregator.ingest(
            [
                {
                    "instrument_token": 101,
                    "last_price": 100.0,
                    "volume_traded": 10,
                    "last_trade_time": first,
                },
                {
                    "instrument_token": 101,
                    "last_price": 102.0,
                    "volume_traded": 25,
                    "last_trade_time": second,
                },
                # An identical quote update must not double count the trade.
                {
                    "instrument_token": 101,
                    "last_price": 102.0,
                    "volume_traded": 25,
                    "last_trade_time": second,
                },
            ],
            received_at=second,
        )
        slot = feed.IST.localize(datetime(2026, 7, 31, 9, 20))
        row = aggregator.rows_for_slot(slot)[101]
        self.assertEqual(row["open"], 100.0)
        self.assertEqual(row["high"], 102.0)
        self.assertEqual(row["low"], 100.0)
        self.assertEqual(row["close"], 102.0)
        self.assertEqual(row["volume"], 25.0)
        self.assertTrue(row["_volume_valid"])
        self.assertEqual(row["_tick_count"], 2)

    def test_next_candle_volume_is_day_volume_delta(self) -> None:
        aggregator = feed.TickAggregator({101: "TEST"})
        first = feed.IST.localize(datetime(2026, 7, 31, 9, 19, 59))
        next_tick = feed.IST.localize(datetime(2026, 7, 31, 9, 20, 1))
        aggregator.ingest(
            [
                {
                    "instrument_token": 101,
                    "last_price": 100.0,
                    "volume_traded": 25,
                    "last_trade_time": first,
                },
                {
                    "instrument_token": 101,
                    "last_price": 101.0,
                    "volume_traded": 31,
                    "last_trade_time": next_tick,
                },
            ]
        )
        slot = feed.IST.localize(datetime(2026, 7, 31, 9, 25))
        row = aggregator.rows_for_slot(slot)[101]
        self.assertEqual(row["volume"], 6.0)
        self.assertTrue(row["_volume_valid"])

    def test_stream_coverage_requires_connection_before_interval_start(self) -> None:
        state = feed.AppStreamState("app1", 155, 155)
        slot_start = feed.IST.localize(datetime(2026, 7, 31, 12, 15))
        state.mark_connected(slot_start - timedelta(seconds=1))
        self.assertTrue(state.covers(slot_start))
        state.mark_disconnected("test", slot_start + timedelta(minutes=1))
        self.assertFalse(state.covers(slot_start))
        state.mark_connected(slot_start + timedelta(minutes=2))
        self.assertFalse(state.covers(slot_start))

    def test_status_heartbeat_is_not_overwritten_by_last_slot_timestamp(self) -> None:
        instance = object.__new__(feed.KiteTickerFiveMinuteFeed)
        instance.next_slot_end = None
        instance.last_slot_summary = {
            "overall_state": "OK",
            "updated_at_ist": "2000-01-01 00:00:00+0530",
        }
        instance.apps = {
            "app1": SimpleNamespace(
                symbols=[],
                state=SimpleNamespace(snapshot=lambda: {"connected": True}),
            )
        }
        instance.universe = []
        instance.universe_hash = feed._universe_sha256([])
        instance.write_workers = 1
        instance.rest_repair_workers_per_app = 1
        instance.rest_repair_enabled = True
        instance.output_dir = Path(".")
        instance.aggregator = SimpleNamespace(telemetry=lambda: {})
        instance.started_at = feed.now_ist()

        payload = instance._status_payload("RUNNING")

        self.assertNotEqual(
            payload["updated_at_ist"],
            instance.last_slot_summary["updated_at_ist"],
        )
        self.assertEqual(
            payload["last_slot_summary"]["updated_at_ist"],
            "2000-01-01 00:00:00+0530",
        )

    def test_shadow_persist_reuses_core_indicator_and_atomic_writer(self) -> None:
        logger = logging.getLogger("test_kiteticker_persist")
        slot = feed.IST.localize(datetime(2026, 7, 31, 12, 20))
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "shadow"
            seed_dir = root / "seed"
            seed_dir.mkdir(parents=True)

            dates = pd.date_range(
                end=slot - timedelta(minutes=5),
                periods=60,
                freq="5min",
                tz=feed.IST,
            )
            seed = pd.DataFrame(
                {
                    "date": dates,
                    "open": range(100, 160),
                    "high": [value + 1 for value in range(100, 160)],
                    "low": [value - 1 for value in range(100, 160)],
                    "close": [value + 0.5 for value in range(100, 160)],
                    "volume": [1000] * 60,
                    "gap_filled": [0] * 60,
                }
            )
            feed.core._finalize_and_save(
                seed,
                str(seed_dir / f"TEST{feed.END_5M}"),
            )

            result = feed._persist_symbol_candle(
                "TEST",
                slot,
                {
                    "date": slot,
                    "open": 160.0,
                    "high": 162.0,
                    "low": 159.0,
                    "close": 161.0,
                    "volume": 1500.0,
                    "gap_filled": 0,
                    "_source": "kiteticker",
                },
                fallback_price=None,
                output_dir=output_dir,
                seed_dir=seed_dir,
                logger=logger,
            )

            self.assertTrue(result["ok"], result)
            written = pd.read_parquet(output_dir / f"TEST{feed.END_5M}")
            self.assertEqual(pd.Timestamp(written.iloc[-1]["date"]), pd.Timestamp(slot))
            for column in ("RSI", "ATR", "EMA_20", "EMA_50", "EMA_200", "Stoch_%K", "Stoch_%D", "ADX"):
                self.assertIn(column, written.columns)

    def test_dashboard_has_disabled_non_restartable_kiteticker_card(self) -> None:
        card_id = "kiteticker_5min_data"
        self.assertEqual(
            dashboard.LIVE_FETCH_CARD_TITLES[card_id],
            "Live Data kiteticker Fetch (5mins)",
        )
        self.assertEqual(
            dashboard.CARD_TASK_NAMES[card_id],
            ("\\EQIDV2_kiteticker_5mins_data_0900",),
        )
        self.assertNotIn(card_id, dashboard.RESTARTABLE_CARDS)

    def test_dashboard_keeps_disabled_kiteticker_card_in_market_section(self) -> None:
        source = (self.ROOT / "log_dashboard_server.py").read_text(encoding="utf-8")
        market_group = source.index('key: "market"')
        market_group_end = source.index("]", market_group)
        self.assertIn(
            '"kiteticker_5min_data"',
            source[market_group:market_group_end],
        )
        self.assertIn(
            'const SECTION_LOCKED_DISABLED_IDS = new Set([\n'
            '      "kiteticker_5min_data"',
            source,
        )
        self.assertIn(
            'status !== "DISABLED" || SECTION_LOCKED_DISABLED_IDS.has(id)',
            source,
        )

    def test_schedule_definition_disables_task_after_creation(self) -> None:
        schedule = (
            self.ROOT / "bat" / "schedule_eqidv2_kiteticker_5min_live_weekday.bat"
        ).read_text(encoding="utf-8")
        create_pos = schedule.index("schtasks /Create")
        disable_pos = schedule.index('schtasks /Change /TN "%TASK_NAME%" /DISABLE')
        self.assertLess(create_pos, disable_pos)
        self.assertIn("EQIDV2_kiteticker_5mins_data_0900", schedule)

    def test_launcher_allows_bounded_slot_seal_before_freshness_kill(self) -> None:
        launcher = (
            self.ROOT / "bat" / "run_eqidv2_kiteticker_5min_live.bat"
        ).read_text(encoding="utf-8")
        self.assertIn('set "FRESHNESS_TIMEOUT_SEC=180"', launcher)
        self.assertIn('set "FRESHNESS_GRACE_SEC=180"', launcher)
        self.assertIn(
            'set "EQIDV2_KITETICKER_5M_SEED_DATA_DIR='
            r'C:\TradingData\eqidv2\stocks_indicators_5min_eq_live"',
            launcher,
        )
        self.assertIn(
            'set "EQIDV2_KITETICKER_5M_REST_REPAIR_WORKERS_PER_APP=10"',
            launcher,
        )


if __name__ == "__main__":
    unittest.main()
