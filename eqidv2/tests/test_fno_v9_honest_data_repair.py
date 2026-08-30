from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v9_honest_data_repair as repair


SESSION = date(2026, 7, 28)
EQUITY = "TEST"
FUTURES = "TEST26AUGFUT"
EQUITY_TOKEN = 101
FUTURES_TOKEN = 202


def _universe_row() -> dict:
    return {
        "instrument_token": FUTURES_TOKEN,
        "exchange_token": 22,
        "tradingsymbol": FUTURES,
        "name": EQUITY,
        "last_price": 0.0,
        "expiry": pd.Timestamp("2026-08-25"),
        "strike": 0.0,
        "tick_size": 0.05,
        "lot_size": 100,
        "instrument_type": "FUT",
        "segment": "NFO-FUT",
        "exchange": "NFO",
        "underlying": EQUITY,
        "is_index_future": False,
        "contract_month": "2026-08",
        "master_date": pd.Timestamp("2026-08-11"),
        "unique_key": f"NFO:{FUTURES}",
        "contract_rank": 1,
        "futures_tradingsymbol": FUTURES,
        "futures_instrument_token": FUTURES_TOKEN,
        "futures_lot_size": 100,
        "futures_tick_size": 0.05,
        "equity_symbol": EQUITY,
        "equity_instrument_token": EQUITY_TOKEN,
        "equity_tick_size": 0.05,
        "equity_exchange": "NSE",
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
    }


def _equity_frame(
    *,
    missing: str | None = None,
    mixed_timezone: bool = False,
    synthetic_tail: int = 0,
) -> pd.DataFrame:
    timestamps = repair.expected_grid(SESSION, repair.ROLE_EQUITY)
    rows = []
    for index, stamp in enumerate(timestamps):
        if missing and stamp.strftime("%H:%M") == missing:
            continue
        base = 100.0 + index / 100.0
        raw_timestamp: object = stamp
        if mixed_timezone:
            raw_timestamp = (
                stamp.strftime("%Y-%m-%d %H:%M:%S")
                if index % 2 == 0
                else stamp.isoformat()
            )
        rows.append(
            {
                "date": raw_timestamp,
                "open": base,
                "high": base + 0.20,
                "low": base - 0.20,
                "close": base + 0.05,
                "volume": 1000 + index,
            }
        )
    if synthetic_tail:
        for row in rows[-synthetic_tail:]:
            row["high"] = row["low"] = row["close"] = row["open"]
            row["volume"] = 0
    return pd.DataFrame(rows)


def _futures_frame(*, missing: str | None = None) -> pd.DataFrame:
    timestamps = repair.expected_grid(SESSION, repair.ROLE_FUTURES)
    rows = []
    for index, stamp in enumerate(timestamps):
        if missing and stamp.strftime("%H:%M") == missing:
            continue
        base = 102.0 + index / 100.0
        rows.append(
            {
                "timestamp": stamp,
                "candle_start": stamp - pd.Timedelta(minutes=5),
                "underlying": EQUITY,
                "tradingsymbol": FUTURES,
                "instrument_token": FUTURES_TOKEN,
                "exchange_token": 22,
                "expiry": pd.Timestamp("2026-08-25"),
                "contract_month": "2026-08",
                "days_to_expiry": 28,
                "lot_size": 100,
                "tick_size": 0.05,
                "is_index_future": False,
                "open": base,
                "high": base + 0.20,
                "low": base - 0.20,
                "close": base + 0.05,
                "volume": 500 + index,
                "oi": 10_000 + index,
                "quality_state": "VALID",
                "fetch_timestamp": pd.Timestamp("2026-08-20 12:00", tz=common.IST),
                "source": "fixture",
                "data_version": "fixture-v1",
            }
        )
    return pd.DataFrame(rows)


def _make_snapshot(
    tmp_path: Path,
    *,
    missing_equity: str | None = "09:51",
    missing_futures: str | None = "09:55",
    mixed_timezone: bool = True,
    synthetic_tail: int = 0,
) -> Path:
    live_futures = tmp_path / "live-futures"
    live_equity = tmp_path / "live-equity"
    snapshot_root = tmp_path / "base-snapshots"
    universe_root = tmp_path / "universe"
    for directory in (live_futures, live_equity, snapshot_root, universe_root):
        directory.mkdir()
    future_path = live_futures / f"{common.safe_contract_stem(FUTURES)}_5minute.parquet"
    equity_path = hybrid.equity_one_minute_path(EQUITY, live_equity)
    _futures_frame(missing=missing_futures).to_parquet(future_path, index=False)
    _equity_frame(
        missing=missing_equity,
        mixed_timezone=mixed_timezone,
        synthetic_tail=synthetic_tail,
    ).to_parquet(equity_path, index=False)

    universe_path = universe_root / "near_month_2026-08-11.parquet"
    pd.DataFrame([_universe_row()]).to_parquet(universe_path, index=False)
    mapped, universe_record = provenance.load_backtest_universe(
        universe_path=universe_path,
        universe_date="2026-08-11",
        contract_month_contains="26AUG",
        require_persisted_mapping=True,
    )
    with (
        patch.object(common, "RAW_CONTRACT_DIR", live_futures),
        patch.object(hybrid, "DEFAULT_BACKTEST_EQUITY_1M_DIR", live_equity),
    ):
        snapshot = provenance.create_source_snapshot(
            mapped,
            universe_record,
            universe_path=universe_path,
            snapshot_root=snapshot_root,
        )
    return Path(snapshot["manifest_path"])


def _raw_candle(role: str, expected_timestamp: str) -> dict:
    expected = pd.Timestamp(expected_timestamp)
    start = expected - pd.Timedelta(
        minutes=1 if role == repair.ROLE_EQUITY else 5
    )
    result = {
        "date": start,
        "open": 111.0,
        "high": 112.0,
        "low": 110.0,
        "close": 111.5,
        "volume": 1234,
    }
    if role == repair.ROLE_FUTURES:
        result["oi"] = 12_345
    return result


class FillingProvider:
    def __init__(self, audit: repair.AuditResult) -> None:
        targets = repair._repair_targets(audit)
        self.records: dict[tuple[str, int], list[dict]] = {}
        for (role, token), group in targets.groupby(
            ["role", "instrument_token"], sort=False
        ):
            self.records[(str(role), int(token))] = [
                _raw_candle(str(role), value)
                for value in group["expected_timestamp"].tolist()
            ]

    def fetch(self, *, role, instrument_token, from_day, through_day, attempt):
        return repair.ProviderResponse(
            records=tuple(self.records.get((role, instrument_token), [])),
            provider_id="fixture",
            request_metadata={"attempt": attempt},
        )


class EmptyOrFailingProvider:
    def fetch(self, *, role, instrument_token, from_day, through_day, attempt):
        if role == repair.ROLE_FUTURES:
            raise TimeoutError("fixture API failure")
        return repair.ProviderResponse(records=(), provider_id="fixture")


def test_endpoint_chunk_boundaries_and_primary_request_count() -> None:
    equity = repair.KiteHistoricalProvider._windows(
        date(2026, 5, 27),
        date(2026, 7, 31),
        max_calendar_days=60,
    )
    futures = repair.KiteHistoricalProvider._windows(
        date(2026, 5, 27),
        date(2026, 7, 31),
        max_calendar_days=100,
    )
    assert equity == [
        (date(2026, 5, 27), date(2026, 7, 25)),
        (date(2026, 7, 26), date(2026, 7, 31)),
    ]
    assert futures == [(date(2026, 5, 27), date(2026, 7, 31))]
    assert all((stop - start).days + 1 <= 60 for start, stop in equity)
    # Full 208+208 universe: 208 cash * two chunks + 208 futures * one.
    assert 208 * len(equity) + 208 * len(futures) == 624

    mapped = pd.DataFrame(
        {
            "equity_symbol": [f"EQ{index}" for index in range(208)],
            "futures_tradingsymbol": [
                f"EQ{index}26AUGFUT" for index in range(208)
            ],
            "equity_instrument_token": [index + 1 for index in range(208)],
            "futures_instrument_token": [index + 1000 for index in range(208)],
        }
    )
    lookup = {}
    issues = []
    for index in range(208):
        equity_symbol = f"EQ{index}"
        futures_symbol = f"EQ{index}26AUGFUT"
        lookup[(repair.ROLE_EQUITY, equity_symbol)] = Path(f"eq-{index}")
        lookup[(repair.ROLE_FUTURES, futures_symbol)] = Path(f"fut-{index}")
        for role, symbol, token, expected in (
            (repair.ROLE_EQUITY, equity_symbol, index + 1, "2026-05-27T09:16:00+05:30"),
            (
                repair.ROLE_FUTURES,
                futures_symbol,
                index + 1000,
                "2026-05-27T09:20:00+05:30",
            ),
        ):
            issues.append(
                {
                    "role": role,
                    "logical_symbol": symbol,
                    "equity_symbol": equity_symbol,
                    "futures_symbol": futures_symbol,
                    "instrument_token": token,
                    "session_date": "2026-05-27",
                    "expected_timestamp": expected,
                    "observed_timestamp": "",
                    "issue_type": "MISSING_TIMESTAMP",
                    "detail": "fixture",
                    "repairable": True,
                }
            )
    contract = repair.SnapshotContract(
        mapped_universe=mapped,
        universe_record={},
        snapshot={},
        inventory={},
        source_lookup=lookup,
    )
    audit = repair.AuditResult(
        source_snapshot_manifest=Path("fixture-manifest"),
        source_snapshot_fingerprint="a" * 64,
        from_day=date(2026, 5, 27),
        through_day=date(2026, 7, 31),
        session_dates=[],
        issues=pd.DataFrame(issues),
        symbol_sessions=pd.DataFrame(),
        summary={},
        contract=contract,
        audit_fingerprint="b" * 64,
    )
    full_plan = repair.build_fetch_plan(audit)
    assert full_plan["mapped_symbol_count"] == 208
    assert full_plan["source_role_file_count"] == 416
    assert full_plan["role_files_with_targets"] == 416
    assert full_plan["first_pass_api_request_count"] == 624
    assert full_plan["maximum_api_request_count"] == 1872


def test_audit_is_fail_closed_for_missing_mixed_timezone_and_synthetic_tail(
    tmp_path: Path,
) -> None:
    manifest = _make_snapshot(tmp_path, synthetic_tail=3)
    audit = repair.audit_snapshot(
        manifest, from_day=SESSION, through_day=SESSION
    )
    issue_types = set(audit.issues["issue_type"])
    assert "MISSING_TIMESTAMP" in issue_types
    assert "MIXED_TIMEZONE_SOURCE" in issue_types
    assert "SUSPECT_SYNTHETIC_FLAT_ZERO_VOLUME" in issue_types
    assert audit.summary["mapped_symbol_count"] == 1
    assert audit.summary["expected_source_role_file_count"] == 2
    assert audit.summary["observed_source_role_file_count"] == 2
    assert audit.summary["suspect_synthetic_row_count"] == 3
    equity_session = audit.symbol_sessions.loc[
        audit.symbol_sessions["role"].eq(repair.ROLE_EQUITY)
    ].iloc[0]
    assert equity_session["trailing_suspect_synthetic_rows"] == 3
    assert not audit.summary["headline_source_complete"]


def test_dry_plan_has_no_network_or_artifact_write(tmp_path: Path) -> None:
    manifest = _make_snapshot(tmp_path)
    audit = repair.audit_snapshot(manifest, from_day=SESSION, through_day=SESSION)
    plan = repair.build_fetch_plan(audit, verification_attempts=3)
    assert plan["source_role_file_count"] == 2
    assert plan["role_files_with_targets"] == 2
    assert plan["first_pass_api_request_count"] == 2
    assert plan["maximum_api_request_count"] == 6


def test_evidence_distinguishes_verified_no_candle_from_api_failure(
    tmp_path: Path,
) -> None:
    manifest = _make_snapshot(tmp_path)
    audit = repair.audit_snapshot(manifest, from_day=SESSION, through_day=SESSION)
    evidence_path = repair.collect_repair_evidence(
        audit,
        EmptyOrFailingProvider(),
        evidence_root=tmp_path / "evidence",
        verification_attempts=3,
    )
    evidence = repair.load_repair_evidence(evidence_path)
    statuses = pd.read_parquet(evidence["artifacts"]["target_status"]["path"])
    by_role = dict(zip(statuses["role"], statuses["state"]))
    assert by_role[repair.ROLE_EQUITY] == "VERIFIED_NO_CANDLE"
    assert by_role[repair.ROLE_FUTURES] == "API_FAILURE"
    assert evidence["verified_no_candle_is_valid_exchange_coverage"] is False
    assert not evidence["all_targets_evidenced"]
    with pytest.raises(RuntimeError, match="Unresolved repair evidence"):
        repair.publish_repaired_snapshot(
            manifest,
            evidence_path,
            snapshot_root=tmp_path / "repaired",
        )


def test_api_flat_zero_volume_candle_is_not_valid_repair_evidence() -> None:
    expected = pd.Timestamp("2026-07-28 09:17:00", tz="Asia/Kolkata")
    normalized, error = repair._normalize_api_record(
        {
            "date": expected - pd.Timedelta(minutes=1),
            "open": 100.0,
            "high": 100.0,
            "low": 100.0,
            "close": 100.0,
            "volume": 0.0,
        },
        role=repair.ROLE_EQUITY,
        expected_timestamp=expected,
    )
    assert normalized is None
    assert error == "suspect_api_flat_zero_volume"


def test_exact_publication_rejects_verified_absence(tmp_path: Path) -> None:
    manifest = _make_snapshot(tmp_path, missing_futures=None)
    audit = repair.audit_snapshot(manifest, from_day=SESSION, through_day=SESSION)
    evidence_path = repair.collect_repair_evidence(
        audit,
        EmptyOrFailingProvider(),
        evidence_root=tmp_path / "evidence",
        verification_attempts=2,
    )
    with pytest.raises(RuntimeError, match="not valid exchange coverage"):
        repair.publish_repaired_snapshot(
            manifest,
            evidence_path,
            snapshot_root=tmp_path / "repaired",
        )


def test_isolated_repair_publication_hashes_lineage_and_preserves_base(
    tmp_path: Path,
) -> None:
    base_manifest = _make_snapshot(tmp_path)
    base_bytes = base_manifest.read_bytes()
    base_payload = json.loads(base_bytes)
    base_source_hashes = {
        item["resolved_path"]: provenance.sha256_file(item["resolved_path"])
        for item in base_payload["source_inventory"]["entries"]
    }
    audit = repair.audit_snapshot(
        base_manifest, from_day=SESSION, through_day=SESSION
    )
    audit_manifest = repair.publish_audit(audit, audit_root=tmp_path / "audits")
    reloaded_audit = repair.audit_from_manifest(audit_manifest)
    assert reloaded_audit.audit_fingerprint == audit.audit_fingerprint
    evidence_path = repair.collect_repair_evidence(
        audit,
        FillingProvider(audit),
        evidence_root=tmp_path / "evidence",
        verification_attempts=3,
    )
    evidence = repair.load_repair_evidence(evidence_path)
    assert evidence["all_targets_filled"]
    repaired_manifest = repair.publish_repaired_snapshot(
        base_manifest,
        evidence_path,
        snapshot_root=tmp_path / "repaired",
    )
    validated = repair.validate_repaired_snapshot(repaired_manifest)
    assert validated["post_audit_summary"]["headline_source_complete"]
    assert repaired_manifest != base_manifest
    assert base_manifest.read_bytes() == base_bytes
    assert {
        path: provenance.sha256_file(path) for path in base_source_hashes
    } == base_source_hashes
    repaired_audit = repair.audit_snapshot(
        repaired_manifest, from_day=SESSION, through_day=SESSION
    )
    assert repaired_audit.summary["missing_bar_count"] == 0
    assert repaired_audit.summary["mixed_timezone_file_count"] == 0

    payload = json.loads(repaired_manifest.read_text(encoding="utf-8"))
    lineage_path = Path(payload["repair_provenance"]["manifest_path"])
    with lineage_path.open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(AssertionError, match="lineage manifest hash"):
        repair.validate_repaired_snapshot(repaired_manifest)
