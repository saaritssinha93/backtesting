from __future__ import annotations

import ast
import json
import tempfile
import threading
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

import fno_oi_common as common
import fno_v8_combined_paper_market_data as market


SESSION_END = pd.Timestamp("2026-08-20 09:26", tz=common.IST)


@dataclass(frozen=True)
class _Credential:
    app_name: str


class _Client:
    def __init__(self, *, records_by_token=None, profile_error=None) -> None:
        self.records_by_token = records_by_token or {}
        self.profile_error = profile_error
        self.profile_calls = 0
        self.history_calls = []

    def profile(self):
        self.profile_calls += 1
        if self.profile_error:
            raise self.profile_error
        return {"ok": True}

    def historical_data(self, token, start, end, interval, **kwargs):
        self.history_calls.append((token, pd.Timestamp(start), pd.Timestamp(end), interval))
        value = self.records_by_token.get(int(token), [])
        return value() if callable(value) else value


class _FlakyProfileClient(_Client):
    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = int(failures)

    def profile(self):
        self.profile_calls += 1
        if self.profile_calls <= self.failures:
            raise RuntimeError(f"temporary auth failure {self.profile_calls}")
        return {"ok": True}


def _record(end=SESSION_END, *, close=101.0, volume=1000):
    start = end - pd.Timedelta(minutes=1)
    return {
        "date": start.to_pydatetime(),
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": close,
        "volume": volume,
    }


def _runtimes(records_by_token=None):
    return [
        market.AppRuntime(
            app_name=f"app{index}",
            client=_Client(records_by_token=records_by_token),
            pace_seconds=0.0,
        )
        for index in range(1, 9)
    ]


class V8CombinedPaperMarketDataTests(unittest.TestCase):
    def test_authentication_requires_and_profiles_exactly_eight_apps(self) -> None:
        clients = {f"app{i}": _Client() for i in range(1, 9)}

        def loader(**_):
            return [_Credential(f"app{i}") for i in range(1, 9)]

        runtimes = market.authenticate_required_apps(
            credential_loader=loader,
            client_factory=lambda credential, **_: clients[credential.app_name],
            request_interval_sec=0,
        )
        self.assertEqual([item.app_name for item in runtimes], list(market.EXPECTED_APP_NAMES))
        self.assertTrue(all(client.profile_calls == 1 for client in clients.values()))

    def test_missing_credentials_block_but_one_failed_app_returns_seven(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "all eight"):
            market.authenticate_required_apps(
                credential_loader=lambda **_: [_Credential(f"app{i}") for i in range(1, 8)],
                client_factory=lambda *_args, **_kwargs: _Client(),
            )

        clients = {f"app{i}": _Client() for i in range(1, 9)}
        clients["app8"] = _Client(profile_error=RuntimeError("expired"))
        runtimes = market.authenticate_required_apps(
            credential_loader=lambda **_: [_Credential(f"app{i}") for i in range(1, 9)],
            client_factory=lambda credential, **_: clients[credential.app_name],
            request_interval_sec=0,
            authentication_attempts=3,
            authentication_retry_spacing_sec=0,
        )
        self.assertEqual([item.app_name for item in runtimes], list(market.EXPECTED_APP_NAMES[:-1]))
        self.assertEqual(clients["app8"].profile_calls, 3)
        authentication = market.app_authentication_payload(runtimes)
        self.assertEqual(len(authentication), 8)
        self.assertFalse(authentication[-1]["authenticated"])
        self.assertEqual(authentication[-1]["attempts"], 3)
        with self.assertRaisesRegex(RuntimeError, "required=8"):
            market.authenticate_required_apps(
                credential_loader=lambda **_: [_Credential(f"app{i}") for i in range(1, 9)],
                client_factory=lambda credential, **_: clients[credential.app_name],
                request_interval_sec=0,
                authentication_attempts=1,
                authentication_retry_spacing_sec=0,
                minimum_healthy_apps=8,
            )

        clients["app7"] = _Client(profile_error=RuntimeError("expired too"))
        with self.assertRaisesRegex(RuntimeError, "required=7"):
            market.authenticate_required_apps(
                credential_loader=lambda **_: [_Credential(f"app{i}") for i in range(1, 9)],
                client_factory=lambda credential, **_: clients[credential.app_name],
                request_interval_sec=0,
                authentication_attempts=1,
                authentication_retry_spacing_sec=0,
            )

    def test_authentication_retries_independently_and_preserves_evidence(self) -> None:
        clients = {f"app{i}": _Client() for i in range(1, 9)}
        clients["app7"] = _FlakyProfileClient(failures=2)
        runtimes = market.authenticate_required_apps(
            credential_loader=lambda **_: [_Credential(f"app{i}") for i in range(1, 9)],
            client_factory=lambda credential, **_: clients[credential.app_name],
            authentication_attempts=3,
            authentication_retry_spacing_sec=0,
            request_interval_sec=0,
        )
        app7 = next(item for item in runtimes if item.app_name == "app7")
        self.assertEqual(app7.auth_attempts, 3)
        self.assertEqual(
            [row["state"] for row in app7.auth_observations],
            ["AUTH_FAILURE", "AUTH_FAILURE", "AUTHENTICATED"],
        )
        auth_payload = market.app_authentication_payload(runtimes)
        self.assertEqual(auth_payload[6]["attempts"], 3)

    def test_completed_minute_uses_dynamic_shared_queue_across_all_apps(self) -> None:
        records = {token: [_record()] for token in range(1, 17)}
        runtimes = _runtimes(records)
        candidates = [
            market.CandidateRequest(symbol=f"SYM{token:02d}", instrument_token=token)
            for token in range(1, 17)
        ]
        frame, marker = market.fetch_completed_minute(
            candidates,
            runtimes,
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observation_spacing_sec=0,
        )
        self.assertEqual(len(frame), 16)
        self.assertEqual(marker["state"], "SUCCESS")
        self.assertEqual(sum(row["assigned"] for row in marker["app_usage"]), 16)
        self.assertEqual(sum(row["written"] for row in marker["app_usage"]), 16)
        self.assertTrue(all(row["assigned"] >= 1 for row in marker["app_usage"]))
        self.assertEqual(sum(len(runtime.client.history_calls) for runtime in runtimes), 16)
        self.assertTrue(marker["exact_symbol_completeness"])

    def test_boundary_duplicate_and_runtime_order_fail_closed_before_fetch(self) -> None:
        runtimes = _runtimes({1: [_record()]})
        with self.assertRaisesRegex(RuntimeError, "not complete"):
            market.fetch_completed_minute(
                [market.CandidateRequest("ONE", 1)],
                runtimes,
                SESSION_END,
                now=SESSION_END + pd.Timedelta(seconds=2),
            )
        self.assertFalse(any(runtime.client.history_calls for runtime in runtimes))

        with self.assertRaisesRegex(ValueError, "symbols must be unique"):
            market.fetch_completed_minute(
                [market.CandidateRequest("ONE", 1), market.CandidateRequest("ONE", 2)],
                runtimes,
                SESSION_END,
                now=SESSION_END + pd.Timedelta(seconds=3),
                observation_spacing_sec=0,
            )

        reversed_pool = list(reversed(runtimes))
        with self.assertRaisesRegex(RuntimeError, "EXPECTED-order subset"):
            market.fetch_completed_minute(
                [], reversed_pool, SESSION_END, now=SESSION_END + pd.Timedelta(seconds=3)
            )

        with self.assertRaisesRegex(RuntimeError, "at least 7 healthy apps"):
            market.fetch_completed_minute(
                [market.CandidateRequest("ONE", 1)],
                runtimes[:6],
                SESSION_END,
                now=SESSION_END + pd.Timedelta(seconds=3),
            )

    def test_candidate_request_normalizes_and_rejects_invalid_direct_values(self) -> None:
        request = market.CandidateRequest("  one  ", "1")
        self.assertEqual(request, market.CandidateRequest("ONE", 1))
        for symbol, token in (("", 1), ("ONE", 0), (None, 1), ("ONE", "bad")):
            with self.assertRaisesRegex(ValueError, "symbol and positive token"):
                market.CandidateRequest(symbol, token)

    def test_variable_healthy_roster_and_cross_app_retry_keep_exact_completeness(self) -> None:
        def fail():
            raise TimeoutError("app2 timeout")

        runtimes = [
            market.AppRuntime("app2", _Client(records_by_token={1: fail}), pace_seconds=0),
            market.AppRuntime("app3", _Client(records_by_token={1: [_record()]}), pace_seconds=0),
        ]
        frame, marker = market.fetch_completed_minute(
            [market.CandidateRequest("ONE", 1)],
            runtimes,
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observations=2,
            observation_spacing_sec=0,
            minimum_healthy_apps=1,
        )
        self.assertEqual(frame[["symbol", "app_name"]].to_dict("records"), [
            {"symbol": "ONE", "app_name": "app3"}
        ])
        attempts = marker["outcomes"][0]["observations"]
        self.assertEqual(
            [(row["app_name"], row["state"]) for row in attempts],
            [("app2", "API_FAILURE"), ("app3", "WRITTEN")],
        )
        self.assertEqual(marker["healthy_app_count"], 2)
        self.assertTrue(marker["degraded_app_pool"])
        self.assertTrue(marker["complete"])

    def test_invalid_data_is_cross_checked_and_recovered_on_another_app(self) -> None:
        invalid = _record(close=200.0)
        runtimes = [
            market.AppRuntime(
                "app1", _Client(records_by_token={1: [invalid]}), pace_seconds=0
            ),
            market.AppRuntime(
                "app2", _Client(records_by_token={1: [_record()]}), pace_seconds=0
            ),
        ]
        frame, marker = market.fetch_completed_minute(
            [market.CandidateRequest("ONE", 1)],
            runtimes,
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observations=2,
            observation_spacing_sec=0,
            minimum_healthy_apps=1,
        )
        self.assertEqual(frame["app_name"].tolist(), ["app2"])
        self.assertEqual(
            [row["state"] for row in marker["outcomes"][0]["observations"]],
            ["INVALID_DATA", "WRITTEN"],
        )
        self.assertTrue(marker["complete"])

    def test_deadline_exhaustion_returns_incomplete_exact_contract(self) -> None:
        def slow_record():
            time.sleep(0.05)
            return [_record()]

        runtime = market.AppRuntime(
            "app1", _Client(records_by_token={1: slow_record}), pace_seconds=0
        )
        frame, marker = market.fetch_completed_minute(
            [market.CandidateRequest("SLOW", 1)],
            [runtime],
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            deadline_at=SESSION_END + pd.Timedelta(seconds=3.01),
            observations=1,
            observation_spacing_sec=0,
            minimum_healthy_apps=1,
        )
        self.assertTrue(frame.empty)
        self.assertEqual(marker["state"], "DATA_INCOMPLETE")
        self.assertTrue(marker["deadline_exhausted"])
        self.assertEqual(marker["outcomes"][0]["state"], "DEADLINE_EXCEEDED")

    def test_circuit_breaker_quarantines_failed_app_and_requeues_work(self) -> None:
        def fail():
            raise ConnectionError("provider unavailable")

        good_records = {token: [_record()] for token in range(1, 4)}
        runtimes = [
            market.AppRuntime(
                "app1",
                _Client(records_by_token={token: fail for token in range(1, 4)}),
                pace_seconds=0,
            ),
            market.AppRuntime(
                "app2", _Client(records_by_token=good_records), pace_seconds=0
            ),
        ]
        frame, marker = market.fetch_completed_minute(
            [market.CandidateRequest(f"SYM{token}", token) for token in range(1, 4)],
            runtimes,
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observations=2,
            observation_spacing_sec=0,
            circuit_breaker_failures=1,
            circuit_breaker_cooldown_sec=10,
            minimum_healthy_apps=1,
        )
        self.assertEqual(set(frame["symbol"]), {"SYM1", "SYM2", "SYM3"})
        self.assertTrue(marker["complete"])
        app1 = next(row for row in marker["app_usage"] if row["app_name"] == "app1")
        self.assertEqual(app1["api_failed"], 1)
        self.assertEqual(app1["circuit_opened"], 1)

    def test_circuit_breaker_persists_across_completed_minute_calls(self) -> None:
        def fail():
            raise ConnectionError("provider unavailable")

        failed_client = _Client(records_by_token={1: fail, 2: fail})
        healthy_client = _Client(records_by_token={1: [_record()], 2: [_record()]})
        runtimes = [
            market.AppRuntime("app1", failed_client, pace_seconds=0),
            market.AppRuntime("app2", healthy_client, pace_seconds=0),
        ]
        first, first_marker = market.fetch_completed_minute(
            [market.CandidateRequest("ONE", 1)],
            runtimes,
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observations=2,
            observation_spacing_sec=0,
            circuit_breaker_failures=1,
            circuit_breaker_cooldown_sec=10,
            minimum_healthy_apps=1,
        )
        second, second_marker = market.fetch_completed_minute(
            [market.CandidateRequest("TWO", 2)],
            runtimes,
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observations=2,
            observation_spacing_sec=0,
            circuit_breaker_failures=1,
            circuit_breaker_cooldown_sec=10,
            minimum_healthy_apps=1,
        )
        self.assertEqual(first["app_name"].tolist(), ["app2"])
        self.assertEqual(second["app_name"].tolist(), ["app2"])
        self.assertEqual(len(failed_client.history_calls), 1)
        self.assertTrue(first_marker["complete"])
        self.assertTrue(second_marker["complete"])

    def test_deadline_abandoned_call_never_overlaps_later_same_client_call(self) -> None:
        class SerializedClient(_Client):
            def __init__(self) -> None:
                super().__init__()
                self.guard = threading.Lock()
                self.active = 0
                self.max_active = 0

            def historical_data(self, token, start, end, interval, **kwargs):
                with self.guard:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                time.sleep(0.08)
                with self.guard:
                    self.active -= 1
                return [_record()]

        client = SerializedClient()
        runtime = market.AppRuntime("app1", client, pace_seconds=0)
        _, first_marker = market.fetch_completed_minute(
            [market.CandidateRequest("ONE", 1)],
            [runtime],
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            deadline_at=SESSION_END + pd.Timedelta(seconds=3.01),
            observations=1,
            observation_spacing_sec=0,
            circuit_breaker_failures=10,
            minimum_healthy_apps=1,
        )
        _, second_marker = market.fetch_completed_minute(
            [market.CandidateRequest("TWO", 2)],
            [runtime],
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            deadline_at=SESSION_END + pd.Timedelta(seconds=3.02),
            observations=1,
            observation_spacing_sec=0,
            circuit_breaker_failures=10,
            minimum_healthy_apps=1,
        )
        time.sleep(0.1)
        self.assertEqual(first_marker["state"], "DATA_INCOMPLETE")
        self.assertEqual(second_marker["state"], "DATA_INCOMPLETE")
        self.assertEqual(client.max_active, 1)

    def test_no_candle_and_invalid_data_are_never_written(self) -> None:
        invalid = _record(close=200.0)
        records = {1: [], 2: [invalid], 3: [_record()]}
        frame, marker = market.fetch_completed_minute(
            [
                market.CandidateRequest("EMPTY", 1),
                market.CandidateRequest("INVALID", 2),
                market.CandidateRequest("GOOD", 3),
            ],
            _runtimes(records),
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observations=3,
            observation_spacing_sec=0,
        )
        self.assertEqual(frame["symbol"].tolist(), ["GOOD"])
        states = {row["symbol"]: row["state"] for row in marker["outcomes"]}
        self.assertEqual(states["EMPTY"], "VERIFIED_NO_CANDLE")
        self.assertEqual(states["INVALID"], "INVALID_DATA")
        self.assertEqual(states["GOOD"], "WRITTEN")
        self.assertFalse(marker["complete"])

    def test_immutable_snapshot_round_trip_and_tamper_detection(self) -> None:
        frame, marker = market.fetch_completed_minute(
            [market.CandidateRequest("GOOD", 3)],
            _runtimes({3: [_record()]}),
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observation_spacing_sec=0,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            observed = market.publish_minute_snapshot_once(
                root, frame, marker, strategy_fingerprint="f" * 64
            )
            loaded, validated = market.load_validated_minute_snapshot(
                root / "minute_0926.json", strategy_fingerprint="f" * 64
            )
            self.assertEqual(loaded["symbol"].tolist(), ["GOOD"])
            self.assertEqual(validated, observed)
            data_path = Path(validated["data_path"])
            data_path.write_bytes(data_path.read_bytes() + b"tamper")
            with self.assertRaisesRegex(RuntimeError, "missing or changed"):
                market.load_validated_minute_snapshot(
                    root / "minute_0926.json", strategy_fingerprint="f" * 64
                )

    def test_marker_payload_tampering_is_rejected(self) -> None:
        frame, marker = market.fetch_completed_minute(
            [market.CandidateRequest("GOOD", 3)],
            _runtimes({3: [_record()]}),
            SESSION_END,
            now=SESSION_END + pd.Timedelta(seconds=3),
            observation_spacing_sec=0,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            market.publish_minute_snapshot_once(
                root, frame, marker, strategy_fingerprint="f" * 64
            )
            marker_path = root / "minute_0926.json"
            payload = json.loads(marker_path.read_text(encoding="utf-8"))
            payload["outcomes"][0]["app_name"] = "forged"
            marker_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "marker payload changed"):
                market.load_validated_minute_snapshot(
                    marker_path, strategy_fingerprint="f" * 64
                )

    def test_module_has_no_quote_or_broker_order_execution_path(self) -> None:
        path = Path(market.__file__)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        attributes = {
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
        }
        self.assertTrue({"place_order", "modify_order", "cancel_order", "ltp"}.isdisjoint(attributes))


if __name__ == "__main__":
    unittest.main()
