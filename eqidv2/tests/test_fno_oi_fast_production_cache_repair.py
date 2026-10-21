from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from unittest import mock

import pandas as pd

import fno_oi_common as common
import fno_oi_fetch_5min_fast_production as producer


def _contract(symbol: str, token: int) -> dict[str, object]:
    return {
        "exchange": "NFO",
        "exchange_token": token + 1000,
        "tradingsymbol": symbol,
        "underlying": symbol.split("26", 1)[0],
        "instrument_token": token,
        "expiry": pd.Timestamp("2026-10-27"),
        "lot_size": 1,
        "tick_size": 0.05,
        "is_index_future": False,
    }


def _slot_frame(contract: dict[str, object], slot: datetime) -> pd.DataFrame:
    return common.normalize_historical_candles(
        [{
            "date": slot - timedelta(minutes=5),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000,
            "oi": 5000,
        }],
        contract,
        fetch_timestamp=slot + timedelta(seconds=1),
        slot_end=slot,
    )


def test_valid_cached_slot_is_reused_while_missing_contract_is_fetched() -> None:
    slot = datetime(2026, 10, 19, 9, 20, tzinfo=common.IST)
    cached_contract = _contract("AAA26OCTFUT", 101)
    missing_contract = _contract("BBB26OCTFUT", 202)
    cache = producer.CanonicalArchiveCache(
        frames={
            "AAA26OCTFUT": _slot_frame(cached_contract, slot),
            "BBB26OCTFUT": pd.DataFrame(columns=list(common.RAW_COLUMNS)),
        }
    )
    lane = producer.fast_core.AppLane("app1", [object()], 0.0)

    def fetch_one(
        _lane: object,
        _client: object,
        contract: dict[str, object],
        _slot: datetime,
        **_kwargs: object,
    ) -> dict[str, object]:
        return {
            "tradingsymbol": contract["tradingsymbol"],
            "underlying": contract["underlying"],
            "app": "app1",
            "state": "WRITTEN",
            "rows": 1,
            "valid_rows": 1,
            "elapsed_sec": 0.01,
            "error": "",
            "_frame": _slot_frame(contract, slot),
        }

    with (
        mock.patch.object(producer.fast_core, "fetch_one_contract", side_effect=fetch_one) as fetch,
        mock.patch.object(common, "atomic_write_parquet"),
        mock.patch.object(common, "publish_heartbeat"),
    ):
        outcomes = producer.fetch_contracts_fast(
            pd.DataFrame([cached_contract, missing_contract]),
            [lane],
            slot - timedelta(minutes=5),
            slot,
            slot_end=slot,
            max_retries=1,
            phase="FETCH_SLOT",
            archive_cache=cache,
        )

    assert fetch.call_count == 1
    assert fetch.call_args.args[2]["tradingsymbol"] == "BBB26OCTFUT"
    assert [item["tradingsymbol"] for item in outcomes] == ["AAA26OCTFUT", "BBB26OCTFUT"]
    assert outcomes[0]["archive_cache_reused"] is True
    assert {item["state"] for item in outcomes} == {"WRITTEN"}


def test_invalid_cached_slot_is_refetched_fail_closed() -> None:
    slot = datetime(2026, 10, 19, 9, 20, tzinfo=common.IST)
    contract = _contract("AAA26OCTFUT", 101)
    invalid = _slot_frame(contract, slot)
    invalid.loc[:, "instrument_token"] = 999
    cache = producer.CanonicalArchiveCache(frames={"AAA26OCTFUT": invalid})
    lane = producer.fast_core.AppLane("app1", [object()], 0.0)
    outcome = {
        "tradingsymbol": "AAA26OCTFUT",
        "underlying": "AAA",
        "app": "app1",
        "state": "NO_CANDLE",
        "rows": 0,
        "valid_rows": 0,
        "elapsed_sec": 0.01,
        "error": "",
    }

    with (
        mock.patch.object(producer.fast_core, "fetch_one_contract", return_value=outcome) as fetch,
        mock.patch.object(common, "publish_heartbeat"),
    ):
        outcomes = producer.fetch_contracts_fast(
            pd.DataFrame([contract]),
            [lane],
            slot - timedelta(minutes=5),
            slot,
            slot_end=slot,
            max_retries=1,
            phase="FETCH_SLOT",
            archive_cache=cache,
        )

    assert fetch.call_count == 1
    assert outcomes[0]["state"] == "NO_CANDLE"
    assert "archive_cache_reused" not in outcomes[0]


def test_partial_slot_repair_fetches_only_three_missing_contracts_and_keeps_policy() -> None:
    slot = datetime(2026, 10, 19, 9, 20, tzinfo=common.IST)
    contracts = [_contract(f"S{i:03d}26OCTFUT", 1000 + i) for i in range(300)]
    missing = {str(item["tradingsymbol"]) for item in contracts[-3:]}
    cache = producer.CanonicalArchiveCache(
        frames={
            str(item["tradingsymbol"]): (
                pd.DataFrame(columns=list(common.RAW_COLUMNS))
                if str(item["tradingsymbol"]) in missing
                else _slot_frame(item, slot)
            )
            for item in contracts
        }
    )
    lane = producer.fast_core.AppLane("app1", [object()], 0.0)
    args = argparse.Namespace(
        writer_workers=2,
        max_retries=1,
        slot_retry_attempts=2,
        slot_retry_delay_sec=0.0,
        min_coverage=0.99,
    )

    def no_candle(
        _lane: object,
        _client: object,
        contract: dict[str, object],
        _slot: datetime,
        **_kwargs: object,
    ) -> dict[str, object]:
        return {
            "tradingsymbol": contract["tradingsymbol"],
            "underlying": contract["underlying"],
            "app": "app1",
            "state": "NO_CANDLE",
            "rows": 0,
            "valid_rows": 0,
            "elapsed_sec": 0.01,
            "error": "",
        }

    with (
        mock.patch.object(producer.fast_core, "fetch_one_contract", side_effect=no_candle) as fetch,
        mock.patch.object(producer.legacy, "_cash_marker_state", return_value=(False, "missing")),
        mock.patch.object(common, "atomic_write_json"),
        mock.patch.object(common, "atomic_write_text"),
        mock.patch.object(common, "append_observation"),
        mock.patch.object(common, "publish_status"),
        mock.patch.object(common, "publish_heartbeat"),
    ):
        marker = producer.run_fast_slot(
            slot,
            pd.DataFrame(contracts),
            [lane],
            args,
            archive_cache=cache,
        )

    assert fetch.call_count == 9  # 3 contracts x initial + two bounded retries
    assert {call.args[2]["tradingsymbol"] for call in fetch.call_args_list} == missing
    assert marker["contracts_expected"] == 300
    assert marker["contracts_written"] == 297
    assert marker["stock_coverage_ratio"] == 0.99
    assert marker["stock_verified_no_candle_count"] == 3
    assert marker["complete"] is False
    assert marker["state"] == "PARTIAL"
