"""Adapter for isolated one-lot V13-V10-G option paper states."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_platform.schemas import OptionOrderRecord

from .common import (
    ArtifactError,
    decimal_or_none,
    optional_timestamp,
    read_json,
    require_equal,
    source_metadata,
)


OPTION_ORDER_SCHEMA = "fno_v13_v10_g_options_paper_state_v1"


def _run_kind(execution_source: str) -> str:
    if execution_source == "HISTORICAL_EXACT_5M_REPLAY":
        return "HISTORICAL_REPLAY"
    return "PAPER_QUOTE_MONITOR"


def read_option_orders(
    orders_root: Path | str,
    session_date: date,
    *,
    expected_strategy_version: str | None = None,
    expected_equity_strategy_version: str | None = None,
    expected_strategy_fingerprint: str | None = None,
) -> list[OptionOrderRecord]:
    directory = Path(orders_root) / session_date.isoformat()
    if not directory.exists():
        return []
    if not directory.is_dir():
        raise ArtifactError(f"Option session path is not a directory: {directory}")
    records: list[OptionOrderRecord] = []
    for path in sorted(directory.glob("*.json")):
        row, source, sha = read_json(path)
        require_equal(row.get("schema_version"), OPTION_ORDER_SCHEMA, "schema_version", source)
        require_equal(str(row.get("mode", "")).upper(), "PAPER", "mode", source)
        require_equal(row.get("session_date"), session_date.isoformat(), "session_date", source)
        require_equal(row.get("strategy_version"), expected_strategy_version, "strategy_version", source)
        require_equal(
            row.get("equity_strategy_version"),
            expected_equity_strategy_version,
            "equity_strategy_version",
            source,
        )
        require_equal(
            row.get("strategy_fingerprint"),
            expected_strategy_fingerprint,
            "strategy_fingerprint",
            source,
        )
        source_mode = str(row.get("source_equity_mode", "")).upper()
        if source_mode not in {"PAPER", "LIVE"}:
            raise ArtifactError(f"Invalid source_equity_mode in {source}: {source_mode!r}")
        status = str(row.get("status", "UNKNOWN")).upper()
        entry = decimal_or_none(row.get("entry_price"))
        exit_price = decimal_or_none(row.get("exit_price"))
        if status in {"OPEN", "CLOSED"} and (entry is None or entry <= 0):
            raise ArtifactError(f"Filled option state has no positive entry price: {source}")
        if status == "CLOSED" and (exit_price is None or exit_price <= 0):
            raise ArtifactError(f"Closed option state has no positive exit price: {source}")
        if entry is not None and entry <= 0:
            entry = None
        if status != "CLOSED":
            exit_price = None
        reported = decimal_or_none(row.get("net_pnl_rs")) if status in {"OPEN", "CLOSED"} else None
        realized = reported if status == "CLOSED" else None
        open_mark = reported if status == "OPEN" else None
        execution_source = str(row.get("execution_source", ""))
        records.append(
            OptionOrderRecord(
                trade_id=str(row.get("trade_id", "")),
                signal_id=str(row.get("signal_id", "")),
                session_date=session_date,
                mode="PAPER",
                status=status,
                strategy_version=str(row.get("strategy_version", "")),
                equity_strategy_version=str(row.get("equity_strategy_version", "")),
                strategy_fingerprint=str(row.get("strategy_fingerprint", "")),
                source_equity_mode=source_mode,
                execution_source=execution_source,
                run_kind=_run_kind(execution_source),
                option_symbol=str(row.get("option_symbol", "")),
                quantity=int(row.get("quantity", 0) or 0),
                entry_price=entry,
                exit_price=exit_price,
                reported_net_pnl_rs=reported,
                realized_net_pnl_rs=realized,
                open_mark_net_pnl_rs=open_mark,
                source=source_metadata(source, sha, "option_paper_orders"),
                raw=row,
                event_at_ist=optional_timestamp(
                    row.get("entry_at_ist") or row.get("updated_at_ist") or row.get("created_at_ist"),
                    "option_order.event_at_ist",
                ),
            )
        )
    return records
