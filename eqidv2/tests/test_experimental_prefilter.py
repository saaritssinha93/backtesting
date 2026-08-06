from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from experimental_prefilter.config import PrefilterConfig
from experimental_prefilter.engine import (
    annotate_budget_grid,
    build_features,
    rank_universe,
    select_candidates,
)
from experimental_prefilter.evaluation import evaluate_budget_grid, wilson_lower_bound
from experimental_prefilter.io import (
    ensure_experimental_output_dir,
    load_slot_marker,
    load_bar_directory_through_slot,
    load_universe_manifest,
    universe_sha256,
    validate_bar_snapshot,
    validate_slot_contract,
    write_shadow_outputs,
)
from experimental_prefilter.latency import profile_slot, summarize_latencies
from experimental_prefilter.manifest import candidate_sha256


IST = "Asia/Kolkata"


def _bars(
    tickers: tuple[str, ...] = ("UP", "DOWN", "FLAT"),
    *,
    periods: int = 18,
    start: str = "2026-08-04 09:15",
) -> pd.DataFrame:
    timestamps = pd.date_range(start, periods=periods, freq="5min", tz=IST)
    rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        direction = 1.0 if ticker == "UP" else -1.0 if ticker == "DOWN" else (ticker_index % 5 - 2) * 0.15
        base = 100.0 + ticker_index * 5.0
        for index, timestamp in enumerate(timestamps):
            close = base + direction * index * 0.45
            open_price = close - direction * 0.18
            high = max(open_price, close) + 0.22
            low = min(open_price, close) - 0.22
            rows.append(
                {
                    "ticker": ticker,
                    "date": timestamp,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": 1000.0 + index * (30 + ticker_index),
                    "ATR": 1.2,
                    "EMA_20": close - direction * 0.6,
                    "RSI": 65.0 if direction > 0 else 35.0 if direction < 0 else 50.0,
                    "ADX": 25.0,
                    "gap_filled": 0,
                    "opening_snapshot": index == 0,
                }
            )
    return pd.DataFrame(rows)


class ExperimentalPrefilterEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = PrefilterConfig(
            budget=15,
            budget_grid=(5, 10, 15),
            lookback_bars=32,
            min_bars=1,
            feature_min_observations=8,
        ).validate()

    def test_future_rows_cannot_change_features_or_rank(self):
        bars = _bars()
        slot = pd.Timestamp("2026-08-04 10:30", tz=IST)
        base, _ = build_features(bars, slot, self.config)
        future = bars.copy()
        added = bars.loc[bars["date"].eq(slot)].copy()
        added["date"] = slot + pd.Timedelta(minutes=5)
        added["close"] = 99999.0
        added["high"] = 100000.0
        future = pd.concat([future, added], ignore_index=True)
        with_future, _ = build_features(future, slot, self.config)
        columns = ["ticker", "overall_score", "long_score", "short_score", "date"]
        pd.testing.assert_frame_equal(
            base[columns].reset_index(drop=True),
            with_future[columns].reset_index(drop=True),
        )

    def test_duplicate_timestamp_keeps_last_row(self):
        bars = _bars(("UP",), periods=10)
        slot = bars["date"].max()
        duplicate = bars.tail(1).copy()
        duplicate["close"] = 222.0
        duplicate["high"] = 223.0
        duplicate["low"] = 221.0
        features, _ = build_features(pd.concat([bars, duplicate], ignore_index=True), slot, self.config)
        self.assertAlmostEqual(float(features.loc[0, "close"]), 222.0)

    def test_opening_snapshot_is_not_a_completed_candle(self):
        bars = _bars(("UP",), periods=1)
        features, _ = build_features(bars, bars["date"].max(), self.config)
        self.assertFalse(bool(features.loc[0, "eligible"]))
        self.assertEqual(features.loc[0, "reject_reason"], "opening_snapshot_warmup")

    def test_limited_history_is_neutral_not_nan(self):
        bars = _bars(("UP", "DOWN"), periods=3)
        slot = bars["date"].max()
        features, _ = build_features(bars, slot, self.config)
        self.assertTrue(features["eligible"].all())
        self.assertTrue((features["feature_history_status"] == "WARMUP_NEUTRAL_FEATURES").all())
        score_columns = [
            "overall_score",
            "long_score",
            "short_score",
            "activity_score",
        ]
        self.assertFalse(features[score_columns].isna().any().any())

    def test_gap_filled_zero_volume_bar_is_not_activity_surge(self):
        bars = _bars(("UP", "DOWN"), periods=12)
        last_time = bars["date"].max()
        mask = bars["date"].eq(last_time) & bars["ticker"].eq("UP")
        bars.loc[mask, ["open", "high", "low", "close"]] = 105.0
        bars.loc[mask, "volume"] = 0.0
        bars.loc[mask, "gap_filled"] = 1
        features, _ = build_features(bars, last_time, self.config)
        up = features.set_index("ticker").loc["UP"]
        self.assertTrue(bool(up["current_gap_filled"]))
        self.assertEqual(up["data_quality_reason"], "CURRENT_BAR_GAP_FILLED")
        self.assertLessEqual(float(up["activity_score"]), 0.5)

    def test_mirrored_paths_score_expected_sides(self):
        bars = _bars(("UP", "DOWN"), periods=18)
        features, _ = build_features(bars, bars["date"].max(), self.config)
        indexed = features.set_index("ticker")
        self.assertGreater(indexed.loc["UP", "long_score"], indexed.loc["UP", "short_score"])
        self.assertGreater(indexed.loc["DOWN", "short_score"], indexed.loc["DOWN", "long_score"])

    def test_budget_lists_are_unique_exact_and_nested(self):
        tickers = tuple(f"T{index:02d}" for index in range(30))
        bars = _bars(tickers, periods=18)
        features, _ = build_features(bars, bars["date"].max(), self.config)
        ranked = annotate_budget_grid(rank_universe(features), self.config)
        selected_sets: dict[int, set[str]] = {}
        for budget in self.config.budget_grid:
            selected = select_candidates(ranked, budget, self.config)
            self.assertEqual(len(selected), budget)
            self.assertEqual(selected["ticker"].nunique(), budget)
            selected_sets[budget] = set(selected["ticker"])
        self.assertTrue(selected_sets[5].issubset(selected_sets[10]))
        self.assertTrue(selected_sets[10].issubset(selected_sets[15]))

    def test_input_permutation_is_deterministic(self):
        tickers = tuple(f"T{index:02d}" for index in range(20))
        bars = _bars(tickers, periods=14)
        slot = bars["date"].max()
        first, _ = build_features(bars, slot, self.config)
        second, _ = build_features(bars.sample(frac=1.0, random_state=17), slot, self.config)
        first_selected = select_candidates(rank_universe(first), 15, self.config)
        second_selected = select_candidates(rank_universe(second), 15, self.config)
        self.assertEqual(candidate_sha256(first_selected), candidate_sha256(second_selected))


class ExperimentalPrefilterContractTests(unittest.TestCase):
    def _write_contract(self, root: Path, *, source: str = "final", marker_hash: str | None = None):
        symbols = ["AAA", "BBB"]
        digest = universe_sha256(symbols)
        manifest_path = root / "feed_universe_5m.json"
        marker_path = root / "slot_20260804_1000.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema_version": "eqidv2_5m_feed_universe_v1",
                    "slot_ist": "2026-08-04 10:00:00+0530",
                    "published_at_ist": "2026-08-04 10:00:03+0530",
                    "symbols": symbols,
                    "universe_count": 2,
                    "universe_sha256": digest,
                }
            ),
            encoding="utf-8",
        )
        marker_path.write_text(
            json.dumps(
                {
                    "slot_ist": "2026-08-04 10:00:00+0530",
                    "published_at_ist": "2026-08-04 10:00:34+0530",
                    "source": source,
                    "complete": True,
                    "tickers_expected": 2,
                    "tickers_written": 2,
                    "universe_sha256": marker_hash or digest,
                    "unresolved_symbol_count": 0,
                    "failed_symbol_count": 0,
                    "token_missing_symbol_count": 0,
                    "verification_failed_count": 0,
                    "partition_failures": [],
                    "duration_ms": 34000,
                }
            ),
            encoding="utf-8",
        )
        return manifest_path, marker_path

    def test_final_marker_and_universe_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, marker_path = self._write_contract(Path(tmp))
            manifest = load_universe_manifest(manifest_path)
            marker = load_slot_marker(marker_path)
            validate_slot_contract(marker, manifest)

    def test_watcher_marker_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, marker_path = self._write_contract(Path(tmp), source="watcher")
            with self.assertRaisesRegex(ValueError, "not final"):
                load_slot_marker(marker_path)

    def test_hash_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path, marker_path = self._write_contract(Path(tmp), marker_hash="bad")
            with self.assertRaisesRegex(ValueError, "hash differs"):
                validate_slot_contract(load_slot_marker(marker_path), load_universe_manifest(manifest_path))

    def test_production_output_paths_are_rejected(self):
        with self.assertRaises(ValueError):
            ensure_experimental_output_dir(
                r"C:\TradingData\eqidv2\backtesting_result_v11\experimental_prefilter"
            )

    def test_neutral_temp_research_output_path_is_allowed(self):
        with tempfile.TemporaryDirectory(prefix="prefilter_research_") as tmp:
            path = ensure_experimental_output_dir(Path(tmp) / "research_outputs")
            self.assertTrue(path.exists())

    def test_snapshot_requires_every_symbol_at_exact_slot(self):
        slot = pd.Timestamp("2026-08-04 10:00", tz=IST)
        bars = pd.DataFrame(
            {
                "ticker": ["AAA", "BBB"],
                "date": [slot, slot - pd.Timedelta(minutes=5)],
            }
        )
        with self.assertRaisesRegex(ValueError, "not at final slot"):
            validate_bar_snapshot(bars, ["AAA", "BBB"], slot.isoformat())

    def test_historical_loader_discards_future_rows(self):
        with tempfile.TemporaryDirectory(prefix="prefilter_research_") as tmp:
            root = Path(tmp)
            bars = _bars(("AAA",), periods=6)
            bars.drop(columns="ticker").to_parquet(
                root / "AAA_stocks_indicators_5min.parquet",
                index=False,
            )
            cutoff = bars["date"].iloc[3]
            loaded, stats = load_bar_directory_through_slot(
                root,
                ["AAA"],
                cutoff,
                history_bars=10,
                max_workers=1,
            )
            self.assertEqual(stats.loaded_symbols, 1)
            self.assertEqual(len(loaded), 4)
            self.assertEqual(pd.Timestamp(loaded["date"].max()), cutoff)

    def test_shadow_outputs_remain_inside_research_root(self):
        with tempfile.TemporaryDirectory(prefix="prefilter_research_") as tmp:
            root = Path(tmp) / "research_outputs"
            outputs = write_shadow_outputs(
                root,
                {"slot_ist": "2026-08-04 10:00:00+05:30", "mode": "SHADOW_RESEARCH_ONLY"},
                pd.DataFrame({"ticker": ["AAA"], "overall_score": [0.8]}),
            )
            self.assertTrue(Path(outputs["manifest"]).exists())
            self.assertTrue(Path(outputs["ranking"]).exists())
            payload = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8"))
            self.assertEqual(payload["mode"], "SHADOW_RESEARCH_ONLY")
            self.assertTrue(payload["publication"]["ranking_sha256"])

    def test_package_has_no_v7_or_v11_imports(self):
        package_root = Path(__file__).resolve().parents[1] / "experimental_prefilter"
        forbidden_prefixes = (
            "avwap_5min_ID",
            "eqidv2_signal_discovery",
            "eqidv2_entry_engine",
            "v11_ID_backtesting",
            "eqidv2_v11_live_overlay",
        )
        for source in package_root.glob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            imports: list[str] = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.extend(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.append(node.module)
            self.assertFalse(
                any(name.startswith(forbidden_prefixes) for name in imports),
                f"production import found in {source}: {imports}",
            )


class ExperimentalPrefilterEvaluationTests(unittest.TestCase):
    def test_recall_is_monotonic_across_nested_budgets(self):
        slot = "2026-08-04 10:00:00+05:30"
        ranking = pd.DataFrame(
            {
                "slot_ist": [slot] * 6,
                "ticker": list("ABCDEF"),
                "universe_rank": [1, 2, 3, 4, 5, 6],
                "selected_k2": [True, True, False, False, False, False],
                "selected_k4": [True, True, True, True, False, False],
            }
        )
        oracle = pd.DataFrame(
            {
                "signal_time_ist": [slot, slot, slot],
                "ticker": ["A", "C", "F"],
                "net_pnl_rs": [100.0, 50.0, -20.0],
            }
        )
        result = evaluate_budget_grid(ranking, oracle, [2, 4], universe_count=6)
        self.assertLessEqual(result.loc[0, "all_signal_recall"], result.loc[1, "all_signal_recall"])
        self.assertEqual(result.loc[0, "captured_profitable_signals"], 1)
        self.assertEqual(result.loc[1, "captured_profitable_signals"], 2)
        self.assertAlmostEqual(result.loc[1, "missed_winner_regret_rs"], 0.0)

    def test_zero_signal_oracle_is_honest(self):
        ranking = pd.DataFrame(
            {"slot_ist": ["2026-08-04 10:00+05:30"], "ticker": ["A"], "universe_rank": [1]}
        )
        oracle = pd.DataFrame(columns=["signal_time_ist", "ticker", "net_pnl_rs"])
        result = evaluate_budget_grid(ranking, oracle, [1], universe_count=10)
        self.assertEqual(result.loc[0, "total_oracle_signals"], 0)
        self.assertTrue(np.isnan(result.loc[0, "all_signal_recall"]))
        self.assertAlmostEqual(result.loc[0, "workload_reduction"], 0.9)

    def test_wilson_lower_bound_is_conservative(self):
        lower = wilson_lower_bound(97, 100)
        self.assertLess(lower, 0.97)
        self.assertGreater(lower, 0.90)


class ExperimentalPrefilterLatencyTests(unittest.TestCase):
    def test_profile_slot_derives_feed_and_scan_segments(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            symbols = ["AAA"]
            digest = universe_sha256(symbols)
            feed = root / "slot_20260804_1000.json"
            scanner = root / "slot_complete_20260804_1000.json"
            feed.write_text(
                json.dumps(
                    {
                        "slot_ist": "2026-08-04 10:00:00+0530",
                        "published_at_ist": "2026-08-04 10:00:34+0530",
                        "source": "final",
                        "complete": True,
                        "tickers_expected": 1,
                        "tickers_written": 1,
                        "universe_sha256": digest,
                        "unresolved_symbol_count": 0,
                        "failed_symbol_count": 0,
                        "token_missing_symbol_count": 0,
                        "verification_failed_count": 0,
                        "partition_failures": [],
                        "duration_ms": 34000,
                    }
                ),
                encoding="utf-8",
            )
            scanner.write_text(
                json.dumps(
                    {
                        "slot_ist": "2026-08-04 10:00:00+0530",
                        "decision_ready_at_ist": "2026-08-04 10:00:51+0530",
                        "complete": True,
                        "candidate_count": 2,
                    }
                ),
                encoding="utf-8",
            )
            row = profile_slot(feed, scanner)
            self.assertAlmostEqual(row.feed_publish_lag_seconds, 34.0)
            self.assertAlmostEqual(row.decision_lag_seconds or 0.0, 51.0)
            self.assertAlmostEqual(row.post_feed_scan_seconds or 0.0, 17.0)
            summary = summarize_latencies([row])
            self.assertEqual(summary["matched_slots"], 1)


if __name__ == "__main__":
    unittest.main()
