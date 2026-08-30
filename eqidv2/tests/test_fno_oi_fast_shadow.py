from __future__ import annotations

import unittest
from argparse import Namespace
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pandas as pd

import fno_oi_common as common
import fno_oi_fetch_5min_fast_shadow as shadow


def _contract(symbol: str, token: int, *, is_index: bool = False) -> dict[str, object]:
    underlying = symbol.split("26", 1)[0]
    return {
        "exchange": "NFO",
        "tradingsymbol": symbol,
        "underlying": underlying,
        "instrument_token": token,
        "exchange_token": token + 1000,
        "expiry": pd.Timestamp("2026-09-24"),
        "lot_size": 100,
        "tick_size": 0.05,
        "is_index_future": is_index,
    }


def _record(candle_start: str, token: int) -> dict[str, object]:
    return {
        "date": candle_start,
        "open": 100.0 + token,
        "high": 101.0 + token,
        "low": 99.0 + token,
        "close": 100.5 + token,
        "volume": 1000 + token,
        "oi": 5000 + token,
    }


def _full_test_universe() -> pd.DataFrame:
    stocks = [
        _contract(f"STOCK{index:03d}26SEPFUT", index + 1)
        for index in range(210)
    ]
    index_symbols = (
        "NIFTY26SEPFUT",
        "BANKNIFTY26SEPFUT",
        "FINNIFTY26SEPFUT",
        "MIDCPNIFTY26SEPFUT",
        "SENSEX26SEPFUT",
        "BANKEX26SEPFUT",
    )
    indexes = [
        _contract(symbol, 10_001 + index, is_index=True)
        for index, symbol in enumerate(index_symbols)
    ]
    return pd.DataFrame([*stocks, *indexes])


class _FakeClient:
    def __init__(self, records_by_token: dict[int, list[dict[str, object]]]) -> None:
        self.records_by_token = records_by_token
        self.calls: list[int] = []

    def historical_data(self, token: int, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
        self.calls.append(token)
        return self.records_by_token.get(token, [])


class TokenException(Exception):
    pass


class FnOOIFastShadowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.slot = datetime(2026, 9, 1, 9, 45, tzinfo=common.IST)

    @staticmethod
    def _legacy_to_ist(values: object) -> pd.Series:
        source = values if isinstance(values, pd.Series) else pd.Series(values)

        def convert(value: object) -> pd.Timestamp:
            try:
                stamp = pd.Timestamp(value)
            except Exception:
                return pd.NaT
            if pd.isna(stamp):
                return pd.NaT
            if stamp.tzinfo is None:
                return stamp.tz_localize(common.IST)
            return stamp.tz_convert(common.IST)

        return pd.Series(
            [convert(value) for value in source],
            index=source.index,
            dtype="datetime64[ns, Asia/Kolkata]",
        )

    def test_vectorized_to_ist_matches_legacy_for_native_datetime_dtypes(self) -> None:
        cases = [
            pd.Series(
                pd.date_range("2026-09-01 09:15", periods=4, freq="5min", tz=common.IST),
                index=[4, 3, 2, 1],
                name="aware",
            ),
            pd.Series(
                pd.date_range("2026-09-01 03:45", periods=4, freq="5min", tz="UTC"),
                index=["a", "b", "c", "d"],
                name="utc",
            ),
            pd.Series(
                [pd.Timestamp("2026-09-01 09:15"), pd.NaT],
                index=[10, 10],
                dtype="datetime64[ns]",
                name="naive",
            ),
        ]
        for values in cases:
            with self.subTest(dtype=str(values.dtype)):
                expected = self._legacy_to_ist(values)
                actual = common._to_ist(values)
                pd.testing.assert_series_equal(actual, expected)

    def test_to_ist_keeps_mixed_object_scalar_fallback_semantics(self) -> None:
        values = pd.Series(
            [
                "2026-09-01 09:15",
                pd.Timestamp("2026-09-01 03:50", tz="UTC"),
                "not-a-date",
                None,
            ],
            index=[7, 5, 3, 1],
            dtype="object",
        )
        pd.testing.assert_series_equal(
            common._to_ist(values), self._legacy_to_ist(values)
        )

    def test_dynamic_fetch_is_exact_once_and_never_appends_production_history(self) -> None:
        contracts = [
            _contract("AAA26SEPFUT", 1),
            _contract("BBB26SEPFUT", 2),
            _contract("CCC26SEPFUT", 3),
        ]
        records = {
            token: [_record("2026-09-01 09:40:00+05:30", token)]
            for token in (1, 2, 3)
        }
        clients = [_FakeClient(records), _FakeClient(records)]
        lane = shadow.AppLane("app1", clients, pace_seconds=0.0)
        with mock.patch.object(common, "append_contract_rows") as append_rows:
            outcomes = shadow.fetch_dynamic_batch(
                contracts,
                [lane],
                self.slot,
                max_retries=1,
            )
        self.assertEqual(
            {item["tradingsymbol"] for item in outcomes},
            {contract["tradingsymbol"] for contract in contracts},
        )
        self.assertEqual(sum(len(client.calls) for client in clients), 3)
        self.assertTrue(all(item["state"] == "WRITTEN" for item in outcomes))
        append_rows.assert_not_called()

    def test_request_contract_is_unchanged(self) -> None:
        contract = _contract("AAA26SEPFUT", 1)
        client = mock.Mock()
        client.historical_data.return_value = [
            _record("2026-09-01 09:40:00+05:30", 1)
        ]
        lane = shadow.AppLane("app1", [client], pace_seconds=0.0)

        outcome = shadow.fetch_one_contract(
            lane,
            client,
            contract,
            self.slot,
            max_retries=1,
        )

        self.assertEqual(outcome["state"], "WRITTEN")
        client.historical_data.assert_called_once_with(
            1,
            self.slot - timedelta(minutes=5),
            self.slot,
            "5minute",
            continuous=False,
            oi=True,
        )

    def test_v2_strategy_slots_select_all_stocks_and_exclude_indices(self) -> None:
        universe = _full_test_universe()
        selected_hashes: set[str] = set()

        for minute in (20, 25, 30, 35, 40, 45):
            with self.subTest(minute=minute):
                slot = datetime(2026, 9, 1, 9, minute, tzinfo=common.IST)
                scope = shadow.build_shadow_scope(universe, slot, canary_count=20)
                self.assertEqual(scope.mode, shadow.SCOPE_STRATEGY_FULL)
                self.assertTrue(scope.strategy_slot)
                self.assertEqual(scope.full_universe_contracts, 216)
                self.assertEqual(scope.full_stock_contracts, 210)
                self.assertEqual(scope.selected_contracts, 210)
                self.assertFalse(
                    scope.universe.apply(
                        lambda row: shadow._is_index_row(row), axis=1
                    ).any()
                )
                selected_hashes.add(scope.selected_symbol_set_sha256)

        self.assertEqual(len(selected_hashes), 1)

    def test_v2_rotating_canary_is_order_stable_and_covers_all_stocks(self) -> None:
        universe = _full_test_universe()
        shuffled = universe.sample(frac=1.0, random_state=42).reset_index(drop=True)
        slot = datetime(2026, 9, 1, 10, 5, tzinfo=common.IST)

        ordered_scope = shadow.build_shadow_scope(universe, slot, canary_count=20)
        shuffled_scope = shadow.build_shadow_scope(shuffled, slot, canary_count=20)

        self.assertEqual(ordered_scope.mode, shadow.SCOPE_ROTATING_CANARY)
        self.assertFalse(ordered_scope.strategy_slot)
        self.assertEqual(ordered_scope.selected_contracts, 20)
        self.assertEqual(ordered_scope.rotation_ordinal, 3)
        self.assertEqual(ordered_scope.rotation_offset, 60)
        self.assertEqual(ordered_scope.selected_symbols, shuffled_scope.selected_symbols)
        self.assertEqual(
            ordered_scope.selected_symbol_set_sha256,
            shuffled_scope.selected_symbol_set_sha256,
        )

        covered: set[str] = set()
        first_canary = datetime(2026, 9, 1, 9, 50, tzinfo=common.IST)
        for ordinal in range(11):
            scope = shadow.build_shadow_scope(
                universe,
                first_canary + timedelta(minutes=5 * ordinal),
                canary_count=20,
            )
            self.assertEqual(scope.selected_contracts, 20)
            covered.update(scope.selected_symbols)

        expected_stocks = {
            str(row["tradingsymbol"])
            for row in universe.to_dict("records")
            if not shadow._is_index_row(row)
        }
        self.assertEqual(covered, expected_stocks)

    def test_v2_scope_policy_daily_call_budget_is_2640(self) -> None:
        universe = _full_test_universe()
        slots = list(common.expected_slot_ends(date(2026, 9, 1)))
        scopes = [
            shadow.build_shadow_scope(universe, slot.to_pydatetime(), canary_count=20)
            for slot in slots
        ]

        self.assertEqual(len(scopes), 75)
        self.assertEqual(sum(scope.strategy_slot for scope in scopes), 6)
        self.assertEqual(
            sum(scope.mode == shadow.SCOPE_ROTATING_CANARY for scope in scopes),
            69,
        )
        self.assertEqual(sum(scope.selected_contracts for scope in scopes), 2_640)

    def test_strategy_job_fence_applies_only_to_signal_slots(self) -> None:
        signal_slot = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        with (
            mock.patch.object(
                common,
                "now_ist",
                side_effect=[
                    signal_slot + timedelta(seconds=60),
                    signal_slot + timedelta(seconds=65),
                ],
            ),
            mock.patch.object(shadow.time, "sleep") as sleep,
            mock.patch.object(common, "publish_heartbeat") as heartbeat,
        ):
            signal_result = shadow.wait_for_strategy_job_fence(
                signal_slot, not_before_seconds=65.0
            )

        self.assertTrue(signal_result["applied"])
        self.assertEqual(signal_result["minimum_offset_sec"], 65.0)
        sleep.assert_called_once_with(1.0)
        heartbeat.assert_called_once()

        for slot in (
            datetime(2026, 9, 1, 9, 20, tzinfo=common.IST),
            datetime(2026, 9, 1, 9, 50, tzinfo=common.IST),
        ):
            with (
                self.subTest(slot=slot.time()),
                mock.patch.object(
                    common,
                    "now_ist",
                    return_value=datetime(2026, 9, 1, 10, 0, tzinfo=common.IST),
                ),
                mock.patch.object(shadow.time, "sleep") as sleep,
            ):
                result = shadow.wait_for_strategy_job_fence(
                    slot, not_before_seconds=65.0
                )
                self.assertFalse(result["applied"])
                self.assertEqual(result["reason"], "NOT_REQUIRED")
                sleep.assert_not_called()

    def test_strategy_job_fence_protects_current_window_for_old_target(self) -> None:
        old_target = datetime(2026, 9, 1, 9, 25, tzinfo=common.IST)
        current_signal = datetime(2026, 9, 1, 9, 30, tzinfo=common.IST)
        with (
            mock.patch.object(
                common,
                "now_ist",
                side_effect=[
                    current_signal + timedelta(seconds=60),
                    current_signal + timedelta(seconds=65),
                ],
            ),
            mock.patch.object(shadow.time, "sleep") as sleep,
            mock.patch.object(common, "publish_heartbeat") as heartbeat,
        ):
            result = shadow.wait_for_strategy_job_fence(
                old_target, not_before_seconds=65.0
            )

        self.assertTrue(result["applied"])
        self.assertEqual(result["reason"], "CURRENT_STRATEGY_WINDOW")
        self.assertEqual(
            result["not_before_ist"],
            (current_signal + timedelta(seconds=65)).isoformat(),
        )
        sleep.assert_called_once_with(1.0)
        heartbeat.assert_called_once()

    def test_scope_metadata_uses_selected_hashes_and_remains_validation_only(self) -> None:
        scope = shadow.build_shadow_scope(
            _full_test_universe(),
            datetime(2026, 9, 1, 9, 50, tzinfo=common.IST),
            canary_count=20,
        )
        outcomes = [
            {"tradingsymbol": symbol, "state": "WRITTEN"}
            for symbol in scope.selected_symbols
        ]
        evidence = pd.DataFrame(
            {
                "tradingsymbol": list(scope.selected_symbols),
                "oi_pair_state": ["VALID"] * scope.selected_contracts,
            }
        )
        marker: dict[str, object] = {}

        with mock.patch.object(shadow, "oi_evidence_complete", return_value=True):
            shadow.apply_scope_metadata(
                marker,
                scope,
                outcomes,
                evidence,
                datetime(2026, 9, 1, 9, 50, tzinfo=common.IST),
            )

        self.assertNotEqual(scope.selected_universe_sha256, scope.full_universe_sha256)
        self.assertEqual(marker["universe_sha256"], scope.selected_universe_sha256)
        self.assertEqual(
            marker["stock_universe_sha256"], scope.selected_universe_sha256
        )
        self.assertEqual(
            marker["stock_symbol_set_sha256"], scope.selected_symbol_set_sha256
        )
        self.assertEqual(
            marker["full_universe_sha256"], scope.full_universe_sha256
        )
        self.assertEqual(
            marker["full_stock_universe_sha256"],
            scope.full_stock_universe_sha256,
        )
        self.assertIs(marker["validation_only"], True)
        self.assertIs(marker["strategy_authority"], False)

    def test_v2_fetch_and_evidence_use_exact_ten_minute_oi_pair(self) -> None:
        contract = _contract("AAA26SEPFUT", 1)
        previous = _record("2026-09-01 09:35:00+05:30", 1)
        previous["oi"] = 4_000
        current = _record("2026-09-01 09:40:00+05:30", 1)
        current["oi"] = 5_000
        client = mock.Mock()
        client.historical_data.return_value = [previous, current]
        lane = shadow.AppLane("app1", [client], pace_seconds=0.0)

        outcome = shadow.fetch_one_contract(
            lane,
            client,
            contract,
            self.slot,
            max_retries=1,
            lookback_minutes=10,
            require_oi_pair=True,
        )

        client.historical_data.assert_called_once_with(
            1,
            self.slot - timedelta(minutes=10),
            self.slot,
            "5minute",
            continuous=False,
            oi=True,
        )
        self.assertEqual(outcome["state"], "WRITTEN")
        self.assertEqual(outcome["oi_pair_state"], "VALID")
        self.assertEqual(len(outcome["_frame"]), 1)
        self.assertEqual(len(outcome["_oi_pair_frame"]), 2)

        evidence = shadow.build_oi_evidence(
            pd.DataFrame([contract]), outcome["_oi_pair_frame"], self.slot
        )
        self.assertTrue(shadow.oi_evidence_complete(evidence, self.slot))
        self.assertEqual(float(evidence.iloc[0]["oi"]), 5_000.0)
        self.assertEqual(float(evidence.iloc[0]["prev_oi"]), 4_000.0)
        self.assertAlmostEqual(float(evidence.iloc[0]["oi_change_pct"]), 25.0)
        self.assertEqual(
            evidence.iloc[0]["previous_timestamp"],
            pd.Timestamp("2026-09-01 09:40:00", tz=common.IST),
        )

    def test_v2_missing_previous_oi_fails_closed(self) -> None:
        contract = _contract("AAA26SEPFUT", 1)
        current = _record("2026-09-01 09:40:00+05:30", 1)
        client = _FakeClient({1: [current]})
        lane = shadow.AppLane("app1", [client], pace_seconds=0.0)

        outcome = shadow.fetch_one_contract(
            lane,
            client,
            contract,
            self.slot,
            max_retries=1,
            lookback_minutes=10,
            require_oi_pair=True,
        )
        evidence = shadow.build_oi_evidence(
            pd.DataFrame([contract]), outcome["_oi_pair_frame"], self.slot
        )

        self.assertEqual(outcome["state"], "INVALID_DATA")
        self.assertEqual(outcome["oi_pair_state"], "MISSING_PREVIOUS")
        self.assertEqual(evidence.iloc[0]["oi_pair_state"], "MISSING_PREVIOUS")
        self.assertTrue(pd.isna(evidence.iloc[0]["prev_oi"]))
        self.assertTrue(pd.isna(evidence.iloc[0]["oi_change_pct"]))
        self.assertFalse(shadow.oi_evidence_complete(evidence, self.slot))

    def test_confirmed_auth_failure_is_not_retried_on_the_dead_lane(self) -> None:
        client = mock.Mock()
        client.historical_data.side_effect = TokenException("session expired")
        lane = shadow.AppLane("app1", [client], pace_seconds=0.0)

        with self.assertRaises(TokenException):
            shadow._historical_call(
                lane,
                client,
                _contract("AAA26SEPFUT", 1),
                self.slot - timedelta(minutes=5),
                self.slot,
                max_retries=3,
            )

        self.assertEqual(client.historical_data.call_count, 1)
        self.assertTrue(lane._runtime_auth_failure.is_set())

    def test_app_lane_session_reuses_only_unchanged_credentials_and_config(self) -> None:
        args = Namespace(
            max_apps=8,
            workers_per_app=2,
            timeout_sec=8.0,
            request_interval_sec=0.36,
        )
        credential = common.KiteCredential("app1", "key", "token")
        refreshed = common.KiteCredential("app1", "key", "new-token")
        first_lane = shadow.AppLane("app1", [_FakeClient({})], pace_seconds=0.36)
        second_lane = shadow.AppLane("app1", [_FakeClient({})], pace_seconds=0.36)
        lane_session = shadow.AppLaneSession()

        with (
            mock.patch.object(
                common,
                "discover_kite_credentials",
                side_effect=[[credential], [credential], [refreshed]],
            ),
            mock.patch.object(
                shadow,
                "build_app_lanes",
                side_effect=[
                    ([first_lane], ["app6:TokenException:invalid"]),
                    ([second_lane], []),
                ],
            ) as build,
        ):
            lanes1, failures1, reused1 = lane_session.acquire(args)
            lanes2, failures2, reused2 = lane_session.acquire(args)
            lanes3, failures3, reused3 = lane_session.acquire(args)

        self.assertFalse(reused1)
        self.assertTrue(reused2)
        self.assertFalse(reused3)
        self.assertIs(lanes1, lanes2)
        self.assertIsNot(lanes2, lanes3)
        self.assertEqual(failures1, failures2)
        self.assertEqual(failures3, [])
        self.assertEqual(build.call_count, 2)

    def test_app_lane_session_rebuilds_after_runtime_auth_failure(self) -> None:
        args = Namespace(
            max_apps=8,
            workers_per_app=2,
            timeout_sec=8.0,
            request_interval_sec=0.36,
        )
        credential = common.KiteCredential("app1", "key", "token")
        first_lane = shadow.AppLane("app1", [_FakeClient({})], pace_seconds=0.36)
        second_lane = shadow.AppLane("app1", [_FakeClient({})], pace_seconds=0.36)
        lane_session = shadow.AppLaneSession()

        with (
            mock.patch.object(
                common,
                "discover_kite_credentials",
                return_value=[credential],
            ),
            mock.patch.object(
                shadow,
                "build_app_lanes",
                side_effect=[([first_lane], []), ([second_lane], [])],
            ) as build,
        ):
            lanes1, _, reused1 = lane_session.acquire(args)
            first_lane._runtime_auth_failure.set()
            self.assertTrue(lane_session.invalidate_runtime_auth_failures())
            lanes2, _, reused2 = lane_session.acquire(args)

        self.assertFalse(reused1)
        self.assertFalse(reused2)
        self.assertIsNot(lanes1, lanes2)
        self.assertEqual(build.call_count, 2)

    def test_retries_use_alternate_apps_and_require_three_clean_empty_observations(self) -> None:
        universe = pd.DataFrame([_contract("AAA26SEPFUT", 1)])
        lanes = [
            shadow.AppLane(f"app{index}", [_FakeClient({})], pace_seconds=0.0)
            for index in range(1, 4)
        ]
        args = Namespace(max_retries=1, slot_retry_attempts=2, slot_retry_delay_sec=0.0)
        initial = [{
            "tradingsymbol": "AAA26SEPFUT",
            "app": "app1",
            "state": "NO_CANDLE",
            "_frame": pd.DataFrame(columns=list(common.RAW_COLUMNS)),
        }]
        retry_apps: list[str] = []

        def assigned(assignments: object, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            assignment_list = list(assignments)
            lane = assignment_list[0][1]
            retry_apps.append(lane.app_name)
            return [{
                "tradingsymbol": "AAA26SEPFUT",
                "app": lane.app_name,
                "state": "NO_CANDLE",
                "_frame": pd.DataFrame(columns=list(common.RAW_COLUMNS)),
            }]

        with (
            mock.patch.object(shadow, "fetch_dynamic_batch", return_value=initial),
            mock.patch.object(shadow, "fetch_assigned_batch", side_effect=assigned),
        ):
            outcomes, attempts, observations, retries = shadow.fetch_with_quality_retries(
                universe,
                lanes,
                self.slot,
                args,
            )
        self.assertEqual(retry_apps, ["app2", "app3"])
        self.assertEqual(attempts["AAA26SEPFUT"], 3)
        self.assertEqual(observations["AAA26SEPFUT"], 3)
        self.assertEqual(retries, 2)
        self.assertEqual(outcomes[0]["state"], "NO_CANDLE")

    def test_index_only_no_candle_does_not_retry_or_block_stock_readiness(self) -> None:
        universe = pd.DataFrame(
            [
                _contract("AAA26SEPFUT", 1),
                _contract("NIFTYFPI26SEPFUT", 2, is_index=True),
            ]
        )
        lanes = [
            shadow.AppLane(f"app{index}", [_FakeClient({})], pace_seconds=0.0)
            for index in range(1, 4)
        ]
        args = Namespace(
            max_retries=1,
            slot_retry_attempts=2,
            slot_retry_delay_sec=2.0,
            min_coverage=0.99,
            workers_per_app=2,
        )
        initial = [
            {
                "tradingsymbol": "AAA26SEPFUT",
                "app": "app1",
                "state": "WRITTEN",
                "_frame": pd.DataFrame(),
            },
            {
                "tradingsymbol": "NIFTYFPI26SEPFUT",
                "app": "app1",
                "state": "NO_CANDLE",
                "_frame": pd.DataFrame(columns=list(common.RAW_COLUMNS)),
            },
        ]

        with (
            mock.patch.object(shadow, "fetch_dynamic_batch", return_value=initial),
            mock.patch.object(shadow, "fetch_assigned_batch") as retry_fetch,
            mock.patch.object(shadow.time, "sleep") as sleep,
        ):
            outcomes, attempts, observations, retries = shadow.fetch_with_quality_retries(
                universe,
                lanes,
                self.slot,
                args,
            )

        retry_fetch.assert_not_called()
        sleep.assert_not_called()
        self.assertEqual(retries, 0)
        self.assertEqual(attempts["NIFTYFPI26SEPFUT"], 1)
        self.assertEqual(observations["NIFTYFPI26SEPFUT"], 1)

        marker = shadow.build_quality_marker(
            self.slot,
            universe,
            outcomes,
            attempts,
            observations,
            lanes,
            args,
            retries_used=retries,
        )
        self.assertTrue(marker["complete"])
        self.assertTrue(marker["stock_complete"])
        self.assertFalse(marker["global_complete"])
        self.assertEqual(
            marker["index_no_candle_symbols"], ["NIFTYFPI26SEPFUT"]
        )
        self.assertEqual(
            marker["unverified_no_candle_symbols"], ["NIFTYFPI26SEPFUT"]
        )

    def test_quality_marker_preserves_verified_no_candle_stock_floor(self) -> None:
        universe = pd.DataFrame(
            [_contract(f"STOCK{index:03d}26SEPFUT", index + 1) for index in range(210)]
        )
        missing = "STOCK20926SEPFUT"
        outcomes = [
            {
                "tradingsymbol": symbol,
                "state": "NO_CANDLE" if symbol == missing else "WRITTEN",
                "app": "app1",
                "_frame": pd.DataFrame(),
            }
            for symbol in universe["tradingsymbol"].astype(str)
        ]
        args = Namespace(min_coverage=0.99, workers_per_app=2)
        marker = shadow.build_quality_marker(
            self.slot,
            universe,
            outcomes,
            {symbol: 3 if symbol == missing else 1 for symbol in universe["tradingsymbol"]},
            {missing: 3},
            [shadow.AppLane("app1", [_FakeClient({})], pace_seconds=0.0)],
            args,
            retries_used=2,
        )
        self.assertTrue(marker["complete"])
        self.assertTrue(marker["stock_complete"])
        self.assertEqual(marker["verified_no_candle_symbols"], [missing])
        self.assertAlmostEqual(marker["stock_coverage_ratio"], 209 / 210)

    def test_exact_parity_detects_single_oi_difference(self) -> None:
        universe = pd.DataFrame(
            [
                _contract("AAA26SEPFUT", 1),
                _contract("NIFTYFPI26SEPFUT", 2, is_index=True),
            ]
        )
        rows = common.normalize_historical_candles(
            [_record("2026-09-01 09:40:00+05:30", 1)],
            universe.iloc[0],
            slot_end=self.slot,
        )
        marker = {"no_candle_symbols": ["NIFTYFPI26SEPFUT"]}
        comparison, mismatches = shadow.compare_with_production(
            universe,
            rows,
            rows.copy(),
            marker,
            marker,
        )
        self.assertTrue(comparison["quality_parity"])
        self.assertEqual(comparison["exact_match_symbols"], 2)
        self.assertTrue(mismatches.empty)

        changed = rows.copy()
        changed.loc[:, "oi"] = changed["oi"] + 1
        comparison, mismatches = shadow.compare_with_production(
            universe,
            changed,
            rows,
            marker,
            marker,
        )
        self.assertFalse(comparison["quality_parity"])
        self.assertEqual(comparison["mismatch_symbols"], ["AAA26SEPFUT"])
        self.assertEqual(mismatches.iloc[0]["field"], "oi")

    def test_v2_scoped_compare_ignores_excluded_index_no_candle(self) -> None:
        universe = pd.DataFrame([_contract("AAA26SEPFUT", 1)])
        empty_rows = pd.DataFrame(columns=list(common.RAW_COLUMNS))
        comparison, mismatches = shadow.compare_with_production(
            universe,
            empty_rows,
            empty_rows.copy(),
            {"no_candle_symbols": ["AAA26SEPFUT"]},
            {
                "no_candle_symbols": [
                    "AAA26SEPFUT",
                    "NIFTYFPI26SEPFUT",
                ]
            },
        )

        self.assertTrue(comparison["quality_parity"])
        self.assertTrue(comparison["oi_identity_parity"])
        self.assertTrue(comparison["no_candle_set_equal"])
        self.assertEqual(comparison["shadow_no_candle_symbols"], ["AAA26SEPFUT"])
        self.assertEqual(
            comparison["production_no_candle_symbols"], ["AAA26SEPFUT"]
        )
        self.assertTrue(mismatches.empty)

    def test_v2_ohlcv_only_mismatch_leaves_oi_parity_true(self) -> None:
        universe = pd.DataFrame([_contract("AAA26SEPFUT", 1)])
        production = common.normalize_historical_candles(
            [_record("2026-09-01 09:40:00+05:30", 1)],
            universe.iloc[0],
            slot_end=self.slot,
        )
        shadow_rows = production.copy()
        shadow_rows.loc[:, "close"] = shadow_rows["close"] + 0.1

        comparison, mismatches = shadow.compare_with_production(
            universe,
            shadow_rows,
            production,
            {"no_candle_symbols": []},
            {"no_candle_symbols": []},
        )

        self.assertFalse(comparison["quality_parity"])
        self.assertFalse(comparison["exact_candle_parity"])
        self.assertTrue(comparison["oi_identity_parity"])
        self.assertTrue(comparison["oi_quality_parity"])
        self.assertEqual(comparison["oi_field_mismatch_count"], 0)
        self.assertEqual(mismatches["field"].tolist(), ["close"])

    def test_dataset_reader_is_exactly_equal_to_sequential_reader(self) -> None:
        contracts = [
            _contract("AAA26SEPFUT", 1),
            _contract("BBB26SEPFUT", 2),
            _contract("MISSING26SEPFUT", 3),
        ]
        universe = pd.DataFrame(contracts)
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = {
                contract["tradingsymbol"]: root / f"{contract['tradingsymbol']}.parquet"
                for contract in contracts
            }
            for contract in contracts[:2]:
                rows = common.normalize_historical_candles(
                    [
                        _record("2026-09-01 09:35:00+05:30", int(contract["instrument_token"])),
                        _record("2026-09-01 09:40:00+05:30", int(contract["instrument_token"])),
                    ],
                    contract,
                )
                if contract["tradingsymbol"] == "BBB26SEPFUT":
                    duplicate = rows.tail(1).copy()
                    duplicate.loc[:, "oi"] = duplicate["oi"] + 1
                    rows = pd.concat([rows, duplicate], ignore_index=True)
                rows.to_parquet(paths[contract["tradingsymbol"]], index=False)

            with mock.patch.object(
                common,
                "raw_contract_path",
                side_effect=lambda symbol: paths[str(symbol)],
            ):
                expected = shadow._load_production_rows_sequential(
                    universe,
                    self.slot,
                )
                actual = shadow._load_production_rows_dataset(
                    universe,
                    self.slot,
                )
                routed = shadow.load_production_rows(universe, self.slot)

        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        pd.testing.assert_frame_equal(routed, expected, check_exact=True)
        self.assertEqual(len(actual), 2)
        self.assertEqual(
            int(actual.loc[actual["tradingsymbol"].eq("BBB26SEPFUT"), "oi"].iloc[0]),
            5003,
        )

    def test_dataset_reader_falls_back_without_losing_good_contracts(self) -> None:
        contracts = [_contract("AAA26SEPFUT", 1), _contract("BROKEN26SEPFUT", 2)]
        universe = pd.DataFrame(contracts)
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = {
                contract["tradingsymbol"]: root / f"{contract['tradingsymbol']}.parquet"
                for contract in contracts
            }
            good = common.normalize_historical_candles(
                [_record("2026-09-01 09:40:00+05:30", 1)],
                contracts[0],
            )
            good.to_parquet(paths["AAA26SEPFUT"], index=False)
            good.drop(columns=["oi"]).assign(
                tradingsymbol="BROKEN26SEPFUT",
                instrument_token=2,
            ).to_parquet(paths["BROKEN26SEPFUT"], index=False)

            with mock.patch.object(
                common,
                "raw_contract_path",
                side_effect=lambda symbol: paths[str(symbol)],
            ):
                expected = shadow._load_production_rows_sequential(
                    universe,
                    self.slot,
                )
                actual = shadow.load_production_rows(universe, self.slot)

        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        self.assertEqual(actual["tradingsymbol"].tolist(), ["AAA26SEPFUT"])

    def test_integer_first_mixed_numeric_schema_uses_exact_sequential_fallback(self) -> None:
        contracts = [_contract("AAA26SEPFUT", 1), _contract("BBB26SEPFUT", 2)]
        universe = pd.DataFrame(contracts)
        price_columns = ["open", "high", "low", "close"]
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = {
                contract["tradingsymbol"]: root / f"{contract['tradingsymbol']}.parquet"
                for contract in contracts
            }
            first = common.normalize_historical_candles(
                [_record("2026-09-01 09:40:00+05:30", 1)],
                contracts[0],
            )
            first.loc[:, price_columns] = first[price_columns].round()
            first = first.astype({column: "int64" for column in price_columns})
            second = common.normalize_historical_candles(
                [_record("2026-09-01 09:40:00+05:30", 2)],
                contracts[1],
            )
            second.loc[:, price_columns] = second[price_columns].round()
            second = second.astype({column: "float64" for column in price_columns})
            first.to_parquet(paths["AAA26SEPFUT"], index=False)
            second.to_parquet(paths["BBB26SEPFUT"], index=False)

            with mock.patch.object(
                common,
                "raw_contract_path",
                side_effect=lambda symbol: paths[str(symbol)],
            ):
                expected = shadow._load_production_rows_sequential(
                    universe,
                    self.slot,
                )
                with self.assertRaises(TypeError):
                    shadow._load_production_rows_dataset(universe, self.slot)
                actual = shadow.load_production_rows(universe, self.slot)

        pd.testing.assert_frame_equal(actual, expected, check_exact=True, check_dtype=True)
        self.assertTrue(all(str(expected[column].dtype) == "float64" for column in price_columns))

    def test_continuous_floor_avoids_pre_activation_backfill_and_recovers_restarts(self) -> None:
        production = [
            datetime(2026, 9, 1, 9, 55, tzinfo=common.IST),
            datetime(2026, 9, 1, 10, 0, tzinfo=common.IST),
        ]
        self.assertEqual(shadow.continuous_session_floor(production, []), production[-1])
        self.assertEqual(
            shadow.continuous_session_floor(production, [production[0]]),
            production[0],
        )
        self.assertIsNone(shadow.continuous_session_floor([], []))

    def test_shadow_history_recovers_only_complete_exact_parity_slots(self) -> None:
        day = date(2026, 9, 1)
        with TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir)
            passing = output_root / day.isoformat() / "passing" / "shadow_marker.json"
            partial = output_root / day.isoformat() / "partial" / "shadow_marker.json"
            common.atomic_write_json(
                passing,
                {
                    "slot_ist": "2026-09-01T10:00:00+05:30",
                    "state": "SUCCESS",
                    "complete": True,
                    "comparison": {"quality_parity": True},
                },
            )
            common.atomic_write_json(
                partial,
                {
                    "slot_ist": "2026-09-01T10:05:00+05:30",
                    "state": "PARTIAL",
                    "complete": False,
                    "comparison": {"quality_parity": False},
                },
            )

            observed, successful = shadow.shadow_slot_history(output_root, day)

        self.assertEqual(len(observed), 2)
        self.assertEqual(
            successful,
            {datetime(2026, 9, 1, 10, 0, tzinfo=common.IST)},
        )

    def test_shadow_history_uses_legacy_and_v2_admission_contracts(self) -> None:
        day = date(2026, 9, 1)
        with TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir)
            markers = {
                "legacy_pass": {
                    "slot_ist": "2026-09-01T10:00:00+05:30",
                    "state": "SUCCESS",
                    "complete": True,
                    "comparison": {"quality_parity": True},
                },
                "v2_oi_pass": {
                    "schema_version": shadow.SHADOW_SCHEMA_VERSION,
                    "slot_ist": "2026-09-01T10:05:00+05:30",
                    "state": "SUCCESS",
                    "complete": True,
                    "parity_complete": True,
                    "comparison": {
                        "quality_parity": False,
                        "strategy_oi_parity": True,
                    },
                },
                "v2_oi_fail": {
                    "schema_version": shadow.SHADOW_SCHEMA_VERSION,
                    "slot_ist": "2026-09-01T10:10:00+05:30",
                    "state": "SUCCESS",
                    "complete": True,
                    "parity_complete": False,
                    "comparison": {
                        "quality_parity": True,
                        "strategy_oi_parity": False,
                    },
                },
            }
            for name, marker in markers.items():
                common.atomic_write_json(
                    output_root / day.isoformat() / name / "shadow_marker.json",
                    marker,
                )

            observed, successful = shadow.shadow_slot_history(output_root, day)

        self.assertEqual(
            observed,
            {
                datetime(2026, 9, 1, 10, 0, tzinfo=common.IST),
                datetime(2026, 9, 1, 10, 5, tzinfo=common.IST),
                datetime(2026, 9, 1, 10, 10, tzinfo=common.IST),
            },
        )
        self.assertEqual(
            successful,
            {
                datetime(2026, 9, 1, 10, 0, tzinfo=common.IST),
                datetime(2026, 9, 1, 10, 5, tzinfo=common.IST),
            },
        )

    def test_continuous_first_activation_runs_latest_slot_only(self) -> None:
        slot_0955 = datetime(2026, 9, 1, 9, 55, tzinfo=common.IST)
        slot_1000 = datetime(2026, 9, 1, 10, 0, tzinfo=common.IST)
        args = Namespace(
            session_date="2026-09-01",
            allow_non_trading_day=False,
            output_root="unused",
            end_grace_min=4.0,
            poll_sec=1.0,
            partial_retry_sec=30.0,
        )
        marker = {
            "contracts_written": 216,
            "contracts_expected": 216,
            "fetch_persist_duration_sec": 15.0,
            "speedup_vs_production": 1.1,
            "marker_path": "marker.json",
            "comparison": {"quality_parity": True},
        }
        now_values = [
            datetime(2026, 9, 1, 10, 1, tzinfo=common.IST),
            datetime(2026, 9, 1, 10, 1, tzinfo=common.IST),
            datetime(2026, 9, 1, 15, 35, tzinfo=common.IST),
        ]
        with (
            mock.patch.object(common, "now_ist", side_effect=now_values),
            mock.patch.object(common, "load_holidays", return_value=set()),
            mock.patch.object(common, "is_trading_day", return_value=True),
            mock.patch.object(shadow, "shadow_slot_history", return_value=(set(), set())),
            mock.patch.object(
                shadow,
                "complete_production_slots",
                return_value=[slot_0955, slot_1000],
            ),
            mock.patch.object(shadow, "run_shadow", return_value=(0, marker)) as run,
            mock.patch.object(common, "publish_status"),
            mock.patch.object(common, "publish_heartbeat"),
        ):
            exit_code = shadow.run_continuous(args)

        self.assertEqual(exit_code, 0)
        run.assert_called_once()
        self.assertEqual(run.call_args.args[0].slot, slot_1000.isoformat())
        self.assertIsInstance(run.call_args.kwargs["lane_session"], shadow.AppLaneSession)

    def test_continuous_passes_one_lane_session_across_multiple_slots(self) -> None:
        slot_0950 = datetime(2026, 9, 1, 9, 50, tzinfo=common.IST)
        slot_0955 = datetime(2026, 9, 1, 9, 55, tzinfo=common.IST)
        slot_1000 = datetime(2026, 9, 1, 10, 0, tzinfo=common.IST)
        args = Namespace(
            session_date="2026-09-01",
            allow_non_trading_day=False,
            output_root="unused",
            end_grace_min=4.0,
            poll_sec=1.0,
            partial_retry_sec=30.0,
        )
        marker = {
            "contracts_written": 216,
            "contracts_expected": 216,
            "fetch_persist_duration_sec": 15.0,
            "speedup_vs_production": 1.1,
            "marker_path": "marker.json",
            "comparison": {"quality_parity": True},
        }
        now_values = [
            datetime(2026, 9, 1, 10, 1, tzinfo=common.IST),
            datetime(2026, 9, 1, 10, 1, tzinfo=common.IST),
            datetime(2026, 9, 1, 10, 2, tzinfo=common.IST),
            datetime(2026, 9, 1, 15, 35, tzinfo=common.IST),
        ]
        with (
            mock.patch.object(common, "now_ist", side_effect=now_values),
            mock.patch.object(common, "load_holidays", return_value=set()),
            mock.patch.object(common, "is_trading_day", return_value=True),
            mock.patch.object(
                shadow,
                "shadow_slot_history",
                return_value=({slot_0950}, {slot_0950}),
            ),
            mock.patch.object(
                shadow,
                "complete_production_slots",
                return_value=[slot_0950, slot_0955, slot_1000],
            ),
            mock.patch.object(
                shadow,
                "run_shadow",
                side_effect=[(0, marker), (0, marker)],
            ) as run,
            mock.patch.object(common, "publish_status"),
            mock.patch.object(common, "publish_heartbeat"),
        ):
            exit_code = shadow.run_continuous(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(run.call_count, 2)
        first_session = run.call_args_list[0].kwargs["lane_session"]
        second_session = run.call_args_list[1].kwargs["lane_session"]
        self.assertIs(first_session, second_session)

    def test_partial_slot_is_recorded_once_and_does_not_block_next_slot(self) -> None:
        slot_0950 = datetime(2026, 9, 1, 9, 50, tzinfo=common.IST)
        slot_0955 = datetime(2026, 9, 1, 9, 55, tzinfo=common.IST)
        slot_1000 = datetime(2026, 9, 1, 10, 0, tzinfo=common.IST)
        args = Namespace(
            session_date="2026-09-01",
            allow_non_trading_day=False,
            output_root="unused",
            end_grace_min=4.0,
            poll_sec=1.0,
            partial_retry_sec=30.0,
        )
        partial = {
            "contracts_written": 215,
            "contracts_expected": 216,
            "fetch_persist_duration_sec": 11.0,
            "speedup_vs_production": 1.4,
            "marker_path": "partial.json",
            "comparison": {"quality_parity": False},
        }
        success = {
            **partial,
            "marker_path": "success.json",
            "comparison": {"quality_parity": True},
        }
        now_values = [
            datetime(2026, 9, 1, 10, 1, tzinfo=common.IST),
            datetime(2026, 9, 1, 10, 1, tzinfo=common.IST),
            datetime(2026, 9, 1, 10, 2, tzinfo=common.IST),
            datetime(2026, 9, 1, 15, 35, tzinfo=common.IST),
        ]
        with (
            mock.patch.object(common, "now_ist", side_effect=now_values),
            mock.patch.object(common, "load_holidays", return_value=set()),
            mock.patch.object(common, "is_trading_day", return_value=True),
            mock.patch.object(
                shadow,
                "shadow_slot_history",
                return_value=({slot_0950}, {slot_0950}),
            ),
            mock.patch.object(
                shadow,
                "complete_production_slots",
                return_value=[slot_0950, slot_0955, slot_1000],
            ),
            mock.patch.object(
                shadow,
                "run_shadow",
                side_effect=[(2, partial), (0, success)],
            ) as run,
            mock.patch.object(common, "publish_status"),
            mock.patch.object(common, "publish_heartbeat"),
        ):
            exit_code = shadow.run_continuous(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            [call.args[0].slot for call in run.call_args_list],
            [slot_0955.isoformat(), slot_1000.isoformat()],
        )


if __name__ == "__main__":
    unittest.main()
