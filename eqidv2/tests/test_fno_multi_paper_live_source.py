from __future__ import annotations

import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

import fno_oi_common as common
import fno_multi_paper_live_source as source
import fno_v8_combined_paper_engine as v8_engine
import fno_v8_combined_paper_market_data as market


END = pd.Timestamp("2026-08-31 09:26", tz=common.IST)
SIGNAL = pd.Timestamp("2026-08-31 09:25", tz=common.IST)


class _Client:
    def __init__(self, records_by_token=None) -> None:
        self.records_by_token = records_by_token or {}
        self.calls = 0

    def historical_data(self, token, *_args, **_kwargs):
        self.calls += 1
        return self.records_by_token.get(int(token), [])


class _FailClient(_Client):
    def historical_data(self, token, *_args, **_kwargs):
        self.calls += 1
        raise RuntimeError(f"dead app for token {token}")


def _runtimes(records_by_token=None, count: int = 8):
    return tuple(
        market.AppRuntime(
            app_name=f"app{index}",
            client=_Client(records_by_token),
            pace_seconds=0.0,
        )
        for index in range(1, count + 1)
    )


def _universe_row(symbol: str, token: int, futures_token: int):
    five = {
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 5000.0,
        "price_change_pct": 0.05,
        "volume_ratio": 0.20,
        "traded_value": 510000.0,
        "ema9": 101.0,
        "ema20": 100.5,
        "ema50": 100.0,
        "source_file_sha256": "a" * 64,
        "causal_prefix_sha256": "b" * 64,
    }
    return {
        "tradingsymbol": symbol,
        "instrument_token": token,
        "futures_tradingsymbol": f"{symbol}FUT",
        "futures_instrument_token": futures_token,
        "tick_size": 0.05,
        "eligible_sides": [],
        "_cash_features": five,
        "_oi_pair": {
            "oi": 1005.0,
            "prev_oi": 1000.0,
            "oi_change_pct": 0.5,
            "source_file_sha256": "c" * 64,
            "rows_sha256": "d" * 64,
        },
    }


def _direct(symbol: str):
    return {
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 5000.0,
        "constituents": [],
        "constituents_sha256": f"direct-{symbol}",
    }


def _exact_direct(symbol: str, app_name: str = "app1"):
    starts = [SIGNAL - pd.Timedelta(minutes=value) for value in range(5, 0, -1)]
    values = [
        (100.0, 101.0, 99.0, 100.5),
        (100.5, 102.0, 100.0, 101.0),
        (101.0, 103.0, 100.5, 102.0),
        (102.0, 102.5, 101.0, 101.5),
        (101.5, 103.0, 101.0, 102.0),
    ]
    constituents = [
        {
            "timestamp": (start + pd.Timedelta(minutes=1)).isoformat(),
            "candle_start": start.isoformat(),
            "open": candle[0],
            "high": candle[1],
            "low": candle[2],
            "close": candle[3],
            "volume": 1000.0,
            "app_name": app_name,
        }
        for start, candle in zip(starts, values)
    ]
    return {
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 5000.0,
        "constituents": constituents,
        "constituents_sha256": common.canonical_json_sha256(constituents),
        "app_name": app_name,
        "source_contract": "DIRECT_KITE_EXACT_COMPLETED_CASH_S_MINUS_4_THROUGH_S_V1",
    }


def _write_morning_universe(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "master_date": END.date(),
                "is_index_future": False,
                "tradingsymbol": "AAAFUT",
                "instrument_token": 101,
                "equity_symbol": "AAA",
                "equity_instrument_token": 1,
            }
        ]
    ).to_parquet(path, index=False)
    return source._sha256_file(path)


def test_metric_source_projects_every_mapped_stock_without_v8_prefilter(tmp_path: Path):
    paths = source.LiveSourcePaths(END.date(), tmp_path)
    authority = {
        "universe_rows": [
            _universe_row("AAA", 1, 101),
            _universe_row("BBB", 2, 102),
        ]
    }
    rows, tokens = source._build_metric_rows(
        paths=paths,
        signal_end="09:25",
        authority=authority,
        direct_audit={"symbols": {"AAA": _direct("AAA"), "BBB": _direct("BBB")}},
        artifact_bindings={"authority_sha256": "e" * 64},
    )

    assert tokens == {"AAA": 1, "BBB": 2}
    assert len(rows) == 4
    assert {(row["setup_id"], row["symbol"]) for row in rows} == {
        ("09:25_LONG", "AAA"),
        ("09:25_LONG", "BBB"),
        ("09:25_SHORT", "AAA"),
        ("09:25_SHORT", "BBB"),
    }
    # Both rows deliberately fail the old V8 move/volume gates.  They still
    # belong in the strategy-neutral source and are filtered downstream.
    assert all(row["price_change_pct"] == 0.05 for row in rows)
    assert all(row["volume_ratio"] == 0.20 for row in rows)
    v8_engine.PaperCandidate.from_object(rows[0])


def test_union_minute_deduplicates_profiles_and_publishes_only_once():
    start = END - pd.Timedelta(minutes=1)
    records = {
        1: [{"date": start.to_pydatetime(), "open": 100, "high": 102, "low": 99, "close": 101, "volume": 10}],
        2: [{"date": start.to_pydatetime(), "open": 200, "high": 203, "low": 199, "close": 202, "volume": 20}],
    }
    runtimes = _runtimes(records)
    candidates = [
        {"symbol": "AAA", "instrument_token": 1},
        {"symbol": "AAA", "instrument_token": 1},
        {"symbol": "BBB", "instrument_token": 2},
    ]
    with tempfile.TemporaryDirectory() as temp_dir:
        paths = source.LiveSourcePaths(END.date(), Path(temp_dir))
        first = source.fetch_and_publish_union_minute(
            paths,
            candidates,
            runtimes,
            END,
            observed_at=END + pd.Timedelta(seconds=3),
        )
        second = source.fetch_and_publish_union_minute(
            paths,
            candidates,
            runtimes,
            END,
            observed_at=END + pd.Timedelta(seconds=3),
        )

        assert first.reused is False
        assert second.reused is True
        assert set(first.bars_by_symbol) == {"AAA", "BBB"}
        assert first.marker["candidate_count"] == 2
        assert first.marker["profile_bundle_fingerprint"] == source.PROFILE_BUNDLE_FINGERPRINT
        assert sum(runtime.client.calls for runtime in runtimes) == 2


def test_source_accepts_seven_approved_apps_but_blocks_six() -> None:
    seven = source._require_runtime_pool(_runtimes(count=7))
    assert [runtime.app_name for runtime in seven] == [
        f"app{index}" for index in range(1, 8)
    ]

    try:
        source._require_runtime_pool(_runtimes(count=6))
    except source.SourceContractError as exc:
        assert "at least 7" in str(exc) or "at least seven" in str(exc)
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("six-app source pool must be blocked")


def test_raw_direct_audit_is_single_flight_across_source_not_ready_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    universe_path = tmp_path / "universe" / "latest_near_month.parquet"
    universe_sha = _write_morning_universe(universe_path)
    paths = source.LiveSourcePaths(
        END.date(), tmp_path, near_month_universe_path=universe_path
    )
    runtimes = _runtimes()
    roster = market.app_roster_payload(runtimes)
    entered = threading.Event()
    release = threading.Event()
    retry_reached_proof = threading.Event()
    fetch_calls = 0
    proof_calls = 0

    def direct_fetcher(snapshot, _paths, signal_end, _pool, **_kwargs):
        nonlocal fetch_calls
        fetch_calls += 1
        entered.set()
        assert release.wait(timeout=5)
        direct = _exact_direct("AAA")
        request_contract = [{"symbol": "AAA", "instrument_token": 1}]
        return {"AAA": direct}, {
            "schema_version": source.v8_source.EVIDENCE_SCHEMA_VERSION,
            "kind": "DIRECT_CASH_SIGNAL_5X1M_AUDIT",
            "session_date": END.date().isoformat(),
            "signal_end": signal_end,
            "signal_timestamp": SIGNAL.isoformat(),
            "candidate_contract_sha256": common.canonical_json_sha256(
                request_contract
            ),
            "app_roster": roster,
            "app_roster_sha256": common.canonical_json_sha256(roster),
            "healthy_app_count": len(roster),
            "candidate_count": 1,
            "outcomes": [
                {
                    "symbol": "AAA",
                    "app_name": "app1",
                    "state": "SUCCESS",
                    "attempts": [],
                }
            ],
            "symbols": {"AAA": direct},
        }

    cash_payload = {
        "near_month_universe_sha256": universe_sha,
        "cash_symbol_set_sha256": common.symbol_set_sha256(["AAA"]),
        "rows": [{"symbol": "AAA", "instrument_token": 1}],
    }

    def proof_loader(*_args, **_kwargs):
        nonlocal proof_calls
        proof_calls += 1
        if proof_calls == 1:
            raise source.SourceNotReadyError("futures marker not final")
        retry_reached_proof.set()
        return {
            "near_month_universe_sha256": universe_sha,
            "cash_symbol_set_sha256": common.symbol_set_sha256(["AAA"]),
            "contracts": [
                {
                    "equity_symbol": "AAA",
                    "equity_instrument_token": 1,
                }
            ],
        }

    def authority_loader(*_args, **_kwargs):
        return {
            "universe_symbol_set_sha256": common.symbol_set_sha256(["AAA"]),
            "universe_rows": [_universe_row("AAA", 1, 101)],
        }

    monkeypatch.setattr(
        source.v8_source,
        "load_immutable_strict_cash_universe_source",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        source.v8_source,
        "load_immutable_universe_oi_proof",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        source.v8_source,
        "load_immutable_independent_candidate_source",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        source.v8_source, "_validate_strict_cash_universe_source", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        source.v8_source, "_validate_universe_oi_proof_payload", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        source.v8_source, "_validate_independent_candidate_source", lambda *_a, **_k: None
    )
    now = (SIGNAL + pd.Timedelta(seconds=4)).to_pydatetime()
    fixed_clock = lambda: (SIGNAL + pd.Timedelta(seconds=5)).to_pydatetime()
    kwargs = {
        "observed_at": now,
        "clock": fixed_clock,
        "strict_cash_loader": lambda *_a, **_k: dict(cash_payload),
        "oi_proof_loader": proof_loader,
        "authority_loader": authority_loader,
        "direct_audit_fetcher": direct_fetcher,
    }

    with pytest.raises(source.SourceNotReadyError):
        source.build_and_publish_five_minute_source(
            paths, "09:25", runtimes, **kwargs
        )
    assert entered.wait(timeout=2)

    with ThreadPoolExecutor(max_workers=1) as executor:
        retry = executor.submit(
            source.build_and_publish_five_minute_source,
            paths,
            "09:25",
            runtimes,
            **kwargs,
        )
        assert retry_reached_proof.wait(timeout=2)
        assert fetch_calls == 1
        release.set()
        result = retry.result(timeout=5)

    assert fetch_calls == 1
    assert result.reused is False
    assert paths.raw_direct_audit_path("09:25").is_file()
    assert result.manifest["raw_direct_audit_sha256"]
    assert result.manifest["cash_symbol_contract_sha256"]
    assert len(result.rows) == 2


def test_resilient_raw_fetch_quarantines_dead_app_and_reassigns_symbols(
    tmp_path: Path,
) -> None:
    records = {}
    for token in range(1, 17):
        records[token] = [
            {
                "date": (SIGNAL - pd.Timedelta(minutes=offset)).to_pydatetime(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10.0,
            }
            for offset in range(5, 0, -1)
        ]
    runtimes = list(_runtimes(records))
    runtimes[0] = market.AppRuntime(
        app_name="app1", client=_FailClient(), pace_seconds=0.0
    )
    snapshot = {
        "candidates": [
            {"tradingsymbol": f"S{token:02d}", "instrument_token": token}
            for token in records
        ]
    }
    fetched, audit = source._fetch_exact_cash_signal_constituents_resilient(
        snapshot,
        source.LiveSourcePaths(END.date(), tmp_path).as_v8_paths(),
        "09:25",
        runtimes,
        observed_at=(SIGNAL + pd.Timedelta(seconds=3)).to_pydatetime(),
        deadline_at=(SIGNAL + pd.Timedelta(minutes=1, seconds=3)).to_pydatetime(),
        observations=3,
        observation_spacing_sec=0.0,
        circuit_breaker_failures=1,
    )

    app1_health = next(
        item for item in audit["app_runtime_health"] if item["app_name"] == "app1"
    )
    assert len(fetched) == len(records)
    assert audit["deadline_aware"] is True
    assert app1_health["request_count"] == 1
    assert app1_health["quarantined"] is True
    assert runtimes[0].client.calls == 1


def test_cross_stage_symbol_contract_mismatch_is_blocked() -> None:
    symbol_hash = common.symbol_set_sha256(["AAA"])
    frozen = {
        "cash_symbol_tokens": {"AAA": 1},
        "near_month_universe_sha256": "a" * 64,
        "cash_symbol_set_sha256": symbol_hash,
    }
    cash = {
        "near_month_universe_sha256": "a" * 64,
        "cash_symbol_set_sha256": symbol_hash,
        "rows": [{"symbol": "AAA", "instrument_token": 1}],
    }
    proof = {
        "near_month_universe_sha256": "a" * 64,
        "cash_symbol_set_sha256": symbol_hash,
        "contracts": [
            {"equity_symbol": "AAA", "equity_instrument_token": 1}
        ],
    }
    authority = {
        "universe_symbol_set_sha256": common.symbol_set_sha256(["BBB"]),
        "universe_rows": [{"tradingsymbol": "BBB", "instrument_token": 2}],
    }
    with pytest.raises(source.SourceContractError, match="symbol contracts differ"):
        source._validate_cross_stage_symbol_binding(
            authority=authority,
            cash=cash,
            proof=proof,
            frozen_metadata=frozen,
        )
