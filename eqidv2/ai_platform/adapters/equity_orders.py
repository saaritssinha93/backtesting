"""Adapter and source-precedence rule for V13-V10-G equity orders."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Iterable

from ai_platform.schemas import EquityOrderRecord

from .common import (
    ArtifactError,
    decimal_or_none,
    optional_timestamp,
    read_json,
    require_equal,
    source_metadata,
)


EQUITY_ORDER_SCHEMA = "fno_v6_equity_order_state_v2"


def _read_order(
    path: Path,
    *,
    expected_mode: str,
    expected_session_date: date,
    expected_strategy_version: str | None,
    expected_strategy_fingerprint: str | None,
) -> EquityOrderRecord:
    row, source, sha = read_json(path)
    require_equal(row.get("schema_version"), EQUITY_ORDER_SCHEMA, "schema_version", source)
    require_equal(str(row.get("mode", "")).upper(), expected_mode, "mode", source)
    require_equal(row.get("session_date"), expected_session_date.isoformat(), "session_date", source)
    require_equal(row.get("strategy_version"), expected_strategy_version, "strategy_version", source)
    require_equal(
        row.get("strategy_fingerprint"),
        expected_strategy_fingerprint,
        "strategy_fingerprint",
        source,
    )
    status = str(row.get("status", "UNKNOWN")).upper()
    entry = decimal_or_none(row.get("entry_price"))
    exit_price = decimal_or_none(row.get("exit_price"))
    if status in {"OPEN", "CLOSED"} and (entry is None or entry <= 0):
        raise ArtifactError(f"Filled equity state has no positive entry price: {source}")
    if status == "CLOSED" and (exit_price is None or exit_price <= 0):
        raise ArtifactError(f"Closed equity state has no positive exit price: {source}")
    if entry is not None and entry <= 0:
        entry = None
    if status != "CLOSED":
        exit_price = None
    reported = decimal_or_none(row.get("net_pnl_rs")) if status in {"OPEN", "CLOSED"} else None
    realized = reported if status == "CLOSED" else None
    return EquityOrderRecord(
        signal_id=str(row.get("signal_id", "")),
        session_date=expected_session_date,
        mode=expected_mode,
        status=status,
        strategy_version=str(row.get("strategy_version", "")),
        strategy_fingerprint=str(row.get("strategy_fingerprint", "")),
        symbol=str(row.get("tradingsymbol", "")),
        side=str(row.get("side", "")).upper(),
        quantity=int(row.get("quantity", 0) or 0),
        entry_price=entry,
        exit_price=exit_price,
        reported_net_pnl_rs=reported,
        realized_net_pnl_rs=realized,
        source=source_metadata(source, sha, f"equity_{expected_mode.lower()}_orders"),
        raw=row,
        event_at_ist=optional_timestamp(
            row.get("entry_at_ist") or row.get("updated_at_ist") or row.get("created_at_ist"),
            "equity_order.event_at_ist",
        ),
    )


def read_equity_orders(
    paper_root: Path | str,
    live_root: Path | str,
    session_date: date,
    *,
    expected_strategy_version: str | None = None,
    expected_strategy_fingerprint: str | None = None,
) -> list[EquityOrderRecord]:
    records: list[EquityOrderRecord] = []
    for mode, root in (("PAPER", Path(paper_root)), ("LIVE", Path(live_root))):
        directory = root / session_date.isoformat()
        if not directory.exists():
            continue
        if not directory.is_dir():
            raise ArtifactError(f"Order session path is not a directory: {directory}")
        for path in sorted(directory.glob("*.json")):
            records.append(
                _read_order(
                    path,
                    expected_mode=mode,
                    expected_session_date=session_date,
                    expected_strategy_version=expected_strategy_version,
                    expected_strategy_fingerprint=expected_strategy_fingerprint,
                )
            )
    return records


def preferred_filled_equity(
    records: Iterable[EquityOrderRecord],
) -> dict[str, EquityOrderRecord]:
    """Choose an actual LIVE fill; otherwise choose an actual PAPER fill."""

    selected: dict[str, EquityOrderRecord] = {}
    ordered = sorted(records, key=lambda item: 0 if item.mode == "PAPER" else 1)
    for record in ordered:
        if not record.signal_id or not record.has_actual_fill:
            continue
        current = selected.get(record.signal_id)
        if current is None or (record.mode == "LIVE" and current.mode != "LIVE"):
            selected[record.signal_id] = record
    return selected
