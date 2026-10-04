from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from ai_platform.adapters.common import ArtifactError, file_sha256
from ai_platform.adapters.daily_replay import read_daily_replay
from ai_platform.adapters.equity_orders import preferred_filled_equity, read_equity_orders
from ai_platform.adapters.evidence import list_evidence, select_evidence
from ai_platform.adapters.options_orders import read_option_orders
from ai_platform.adapters.snapshot import read_coherent_snapshot
from ai_platform.adapters.status import inspect_status
from ai_platform.adapters.strategy import read_strategy_contract
from ai_platform.schemas import ArtifactState
from ai_platform.services.accounting import summarize_options, summarize_options_by_run_kind


FIXTURES = Path(__file__).parent / "fixtures" / "ai_platform"
SESSION = date(2026, 9, 17)
EQUITY_VERSION = "FNO_V13_V10_G_RETAINED_20260914"
OPTION_VERSION = "FNO_V13_V10_G_OPTIONS_ONE_LOT_ATM_SL30_T40P4_20260915"
FINGERPRINT = "fixture-fingerprint"


def _copy(source_name: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(FIXTURES / source_name, destination)


def test_incomplete_daily_replay_never_exposes_metrics() -> None:
    snapshot = read_daily_replay(
        FIXTURES / "daily_running.json",
        expected_strategy_version=EQUITY_VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
    )
    assert snapshot.status == "RUNNING"
    assert snapshot.complete is False
    assert snapshot.metrics is None


def test_complete_daily_replay_exposes_validated_metrics() -> None:
    snapshot = read_daily_replay(
        FIXTURES / "daily_success.json",
        expected_strategy_version=EQUITY_VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
    )
    assert snapshot.complete is True
    assert snapshot.metrics == {"trades": 1, "net_profit_rupees": -125.5}
    assert snapshot.data_verification == {"status": "PASS"}


def test_complete_zero_trade_day_is_not_missing(tmp_path: Path) -> None:
    payload = json.loads((FIXTURES / "daily_success.json").read_text(encoding="utf-8"))
    payload["result"]["metrics"] = {"trades": 0, "fills": 0, "net_profit_rupees": 0.0}
    path = tmp_path / "zero_trade.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    snapshot = read_daily_replay(
        path,
        expected_strategy_version=EQUITY_VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
    )
    assert snapshot.complete is True
    assert snapshot.metrics["trades"] == 0


def test_cancelled_live_order_does_not_replace_filled_paper(tmp_path: Path) -> None:
    paper_root = tmp_path / "PAPER"
    live_root = tmp_path / "LIVE"
    _copy("equity_paper_closed.json", paper_root / SESSION.isoformat() / "paper.json")
    _copy("equity_live_cancelled.json", live_root / SESSION.isoformat() / "live.json")
    records = read_equity_orders(
        paper_root,
        live_root,
        SESSION,
        expected_strategy_version=EQUITY_VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
    )
    selected = preferred_filled_equity(records)
    assert selected["20260917_0951_SHORT_DEMO_fixture"].mode == "PAPER"
    assert selected["20260917_0951_SHORT_DEMO_fixture"].realized_net_pnl_rs == Decimal("-21.0")
    live = next(row for row in records if row.mode == "LIVE")
    assert live.entry_price is None
    assert live.exit_price is None
    assert live.reported_net_pnl_rs is None


def test_option_accounting_separates_realized_and_open_mark(tmp_path: Path) -> None:
    root = tmp_path / "options"
    _copy("option_open.json", root / SESSION.isoformat() / "open.json")
    _copy("option_closed.json", root / SESSION.isoformat() / "closed.json")
    records = read_option_orders(
        root,
        SESSION,
        expected_strategy_version=OPTION_VERSION,
        expected_equity_strategy_version=EQUITY_VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
    )
    with pytest.raises(ValueError, match="cannot implicitly aggregate"):
        summarize_options(records)
    summaries = summarize_options_by_run_kind(records, capital_rs=Decimal("1500000"))
    assert summaries["HISTORICAL_REPLAY"].realized_net_pnl_rs == Decimal("38.00")
    assert summaries["PAPER_QUOTE_MONITOR"].open_mark_net_pnl_rs == Decimal("-12.50")
    assert summaries["HISTORICAL_REPLAY"].realized_return_on_capital_pct == Decimal("0.0025")
    historical = next(row for row in records if row.status == "CLOSED")
    assert historical.run_kind == "HISTORICAL_REPLAY"


def test_unresolved_option_is_not_realized(tmp_path: Path) -> None:
    root = tmp_path / "options"
    _copy("option_unresolved.json", root / SESSION.isoformat() / "unresolved.json")
    record = read_option_orders(
        root,
        SESSION,
        expected_strategy_version=OPTION_VERSION,
        expected_equity_strategy_version=EQUITY_VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
    )[0]
    assert record.status == "UNRESOLVED"
    assert record.exit_price is None
    assert record.reported_net_pnl_rs is None
    assert record.realized_net_pnl_rs is None
    assert record.open_mark_net_pnl_rs is None


def test_evidence_hash_and_as_of_selection(tmp_path: Path) -> None:
    destination = (
        tmp_path
        / SESSION.isoformat()
        / "slot_1120"
        / "confirmation_snapshot"
        / "observed.json"
    )
    _copy("evidence_observed.json", destination)
    records = list_evidence(
        tmp_path,
        session_date=SESSION,
        slot="1120",
        artifact_kind="confirmation_snapshot",
        generation="v6",
        strict=True,
    )
    assert select_evidence(records, mode="observed") == records[0]
    assert select_evidence(
        records,
        as_of_ist=datetime(2026, 9, 17, 5, 50, tzinfo=timezone.utc),
    ) is None

    tampered = json.loads(destination.read_text(encoding="utf-8"))
    tampered["payload"]["decision"] = "ACCEPT"
    destination.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ArtifactError, match="payload_sha256 mismatch"):
        list_evidence(
            tmp_path,
            session_date=SESSION,
            slot="1120",
            artifact_kind="confirmation_snapshot",
            strict=True,
        )


def test_strategy_contract_validates_frozen_config_hash(tmp_path: Path) -> None:
    config = tmp_path / "frozen_config.json"
    config.write_text('{"morning_slots":false,"two_bar_continuation":false}', encoding="utf-8")
    registry = tmp_path / "profiles.json"
    registry.write_text(
        json.dumps(
            {
                "profiles": {
                    "V13_V10_G": {
                        "label": "V13-V10-G",
                        "strategy_version": EQUITY_VERSION,
                        "strategy_fingerprint": FINGERPRINT,
                        "live_generation": "v6",
                        "frozen_config_path": str(config),
                        "frozen_config_sha256": file_sha256(config),
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    contract = read_strategy_contract(registry)
    assert contract.strategy_version == EQUITY_VERSION

    config.write_text('{"morning_slots":true}', encoding="utf-8")
    with pytest.raises(ArtifactError, match="hash mismatch"):
        read_strategy_contract(registry)


def test_status_inspection_preserves_unavailable_stale_and_schema_error(tmp_path: Path) -> None:
    missing = inspect_status(tmp_path / "missing.json")
    assert missing.state is ArtifactState.UNAVAILABLE

    stale = inspect_status(
        FIXTURES / "status_stale.json",
        expected_schema="fixture_status_v1",
        expected_strategy_version=EQUITY_VERSION,
        expected_strategy_fingerprint=FINGERPRINT,
        required_fields=("session_date", "state"),
        max_age=timedelta(minutes=5),
        now=datetime(2026, 9, 17, 4, 30, tzinfo=timezone.utc),
    )
    assert stale.state is ArtifactState.STALE

    malformed_path = tmp_path / "malformed.json"
    malformed_path.write_text("{bad json", encoding="utf-8")
    malformed = inspect_status(malformed_path)
    assert malformed.state is ArtifactState.SCHEMA_ERROR

    partial_path = tmp_path / "partial.json"
    partial_path.write_text(
        json.dumps({"schema_version": "fixture_status_v1", "state": "RUNNING"}),
        encoding="utf-8",
    )
    partial = inspect_status(partial_path, expected_schema="fixture_status_v1")
    assert partial.state is ArtifactState.PARTIAL


def test_coherent_snapshot_rejects_conflicting_generation(tmp_path: Path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(
        json.dumps({"session_date": SESSION.isoformat(), "generation": "a"}),
        encoding="utf-8",
    )
    second.write_text(
        json.dumps({"session_date": SESSION.isoformat(), "generation": "b"}),
        encoding="utf-8",
    )
    with pytest.raises(ArtifactError, match="Conflicting generation"):
        read_coherent_snapshot(
            {"first": first, "second": second},
            equal_fields=("session_date", "generation"),
        )

    second.write_text(
        json.dumps({"session_date": SESSION.isoformat(), "generation": "a"}),
        encoding="utf-8",
    )
    snapshot = read_coherent_snapshot(
        {"first": first, "second": second},
        equal_fields=("session_date", "generation"),
    )
    assert snapshot.snapshot_id
    assert snapshot.payloads["first"]["generation"] == "a"
