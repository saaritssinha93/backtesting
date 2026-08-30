from __future__ import annotations

import ast
import tempfile
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

    def test_missing_or_failed_app_blocks_the_pool(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "all eight"):
            market.authenticate_required_apps(
                credential_loader=lambda **_: [_Credential(f"app{i}") for i in range(1, 8)],
                client_factory=lambda *_args, **_kwargs: _Client(),
            )

        clients = {f"app{i}": _Client() for i in range(1, 9)}
        clients["app8"] = _Client(profile_error=RuntimeError("expired"))
        with self.assertRaisesRegex(RuntimeError, "app8"):
            market.authenticate_required_apps(
                credential_loader=lambda **_: [_Credential(f"app{i}") for i in range(1, 9)],
                client_factory=lambda credential, **_: clients[credential.app_name],
                request_interval_sec=0,
            )

    def test_completed_minute_is_deterministically_sharded_across_all_apps(self) -> None:
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
        self.assertEqual([row["assigned"] for row in marker["app_usage"]], [2] * 8)
        self.assertEqual([row["written"] for row in marker["app_usage"]], [2] * 8)
        for runtime in runtimes:
            self.assertEqual(len(runtime.client.history_calls), 2)

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
        with self.assertRaisesRegex(RuntimeError, "ordered app1"):
            market.fetch_completed_minute(
                [], reversed_pool, SESSION_END, now=SESSION_END + pd.Timedelta(seconds=3)
            )

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

