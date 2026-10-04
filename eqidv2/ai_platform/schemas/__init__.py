"""Stable data contracts used by adapters, services, and later API stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class SourceMetadata:
    path: Path
    sha256: str
    read_at_ist: datetime
    source_id: str = ""
    modified_at_ist: datetime | None = None


class ArtifactState(str, Enum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"
    SCHEMA_ERROR = "SCHEMA_ERROR"
    INTEGRITY_ERROR = "INTEGRITY_ERROR"


@dataclass(frozen=True)
class StatusSnapshot:
    state: ArtifactState
    source: SourceMetadata | None
    observed_at_ist: datetime | None
    payload: Mapping[str, Any] | None
    detail: str = ""


@dataclass(frozen=True)
class CoherentSnapshot:
    snapshot_id: str
    payloads: Mapping[str, Mapping[str, Any]]
    sources: Mapping[str, SourceMetadata]


@dataclass(frozen=True)
class DailyReplaySnapshot:
    session_date: date
    status: str
    complete: bool
    strategy_version: str
    strategy_fingerprint: str
    metrics: Mapping[str, Any] | None
    coverage: Mapping[str, Any] | None
    data_verification: Mapping[str, Any] | None
    source: SourceMetadata
    reason: str = ""


@dataclass(frozen=True)
class EquityOrderRecord:
    signal_id: str
    session_date: date
    mode: str
    status: str
    strategy_version: str
    strategy_fingerprint: str
    symbol: str
    side: str
    quantity: int
    entry_price: Decimal | None
    exit_price: Decimal | None
    reported_net_pnl_rs: Decimal | None
    realized_net_pnl_rs: Decimal | None
    source: SourceMetadata
    raw: Mapping[str, Any] = field(repr=False)
    event_at_ist: datetime | None = None

    @property
    def has_actual_fill(self) -> bool:
        return (
            self.status in {"OPEN", "CLOSED"}
            and self.entry_price is not None
            and self.entry_price > 0
        )


@dataclass(frozen=True)
class OptionOrderRecord:
    trade_id: str
    signal_id: str
    session_date: date
    mode: str
    status: str
    strategy_version: str
    equity_strategy_version: str
    strategy_fingerprint: str
    source_equity_mode: str
    execution_source: str
    run_kind: str
    option_symbol: str
    quantity: int
    entry_price: Decimal | None
    exit_price: Decimal | None
    reported_net_pnl_rs: Decimal | None
    realized_net_pnl_rs: Decimal | None
    open_mark_net_pnl_rs: Decimal | None
    source: SourceMetadata
    raw: Mapping[str, Any] = field(repr=False)
    event_at_ist: datetime | None = None


@dataclass(frozen=True)
class EvidenceRecord:
    artifact_kind: str
    generation: str
    session_date: date
    slot: str
    observed_at_ist: datetime
    payload_sha256: str
    payload: Mapping[str, Any]
    source: SourceMetadata


@dataclass(frozen=True)
class MoneySummary:
    trades: int
    open_trades: int
    closed_trades: int
    realized_net_pnl_rs: Decimal
    open_mark_net_pnl_rs: Decimal
    realized_return_on_capital_pct: Decimal | None = None
    currency: str = "INR"
    rounding_dp: int = 2
