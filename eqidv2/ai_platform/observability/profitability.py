"""Conservative execution and profitability attribution helpers.

These functions are descriptive.  They never choose parameters or promote a
strategy.  Missing and censored outcomes remain explicit so an incomplete day
cannot be made to look like a zero-return day.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from statistics import fmean
from typing import Any, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class PerformanceSlice:
    dimensions: Mapping[str, str]
    records: int
    closed_trades: int
    wins: int
    losses: int
    unresolved: int
    gross_pnl_rs: float
    fees_rs: float
    net_pnl_rs: float
    win_rate_pct: float | None
    profit_factor: float | None
    average_net_pnl_rs: float | None
    maximum_drawdown_rs: float | None
    evidence_state: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "dimensions": dict(self.dimensions),
            "records": self.records,
            "closed_trades": self.closed_trades,
            "wins": self.wins,
            "losses": self.losses,
            "unresolved": self.unresolved,
            "gross_pnl_rs": self.gross_pnl_rs,
            "fees_rs": self.fees_rs,
            "net_pnl_rs": self.net_pnl_rs,
            "win_rate_pct": self.win_rate_pct,
            "profit_factor": self.profit_factor,
            "average_net_pnl_rs": self.average_net_pnl_rs,
            "maximum_drawdown_rs": self.maximum_drawdown_rs,
            "evidence_state": self.evidence_state,
        }


def _number(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _first_number(row: Mapping[str, Any], fields: Sequence[str]) -> float | None:
    for field in fields:
        value = _number(row.get(field))
        if value is not None:
            return value
    return None


def _closed(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status", row.get("portfolio_status", ""))).upper()
    if status in {"CLOSED", "EXITED", "COMPLETE", "COMPLETED"}:
        return True
    return bool(row.get("filled")) and _first_number(
        row, ("exit_price", "actual_exit_price")
    ) is not None


def _max_drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def summarize_performance(
    records: Iterable[Mapping[str, Any]],
    *,
    dimensions: Mapping[str, str] | None = None,
    minimum_closed_trades: int = 20,
) -> PerformanceSlice:
    """Summarize one causally defined slice without inventing missing P&L."""

    rows = list(records)
    closed_rows = [row for row in rows if _closed(row)]
    unresolved = len(rows) - len(closed_rows)
    net_values: list[float] = []
    gross_total = 0.0
    fee_total = 0.0
    for row in closed_rows:
        net = _first_number(
            row,
            ("realized_net_pnl_rs", "net_pnl_rs", "portfolio_net_profit_rupees"),
        )
        gross = _first_number(
            row,
            ("gross_pnl_rs", "pre_cost_profit_rupees", "portfolio_gross_profit_rupees"),
        )
        fees = _first_number(
            row, ("fees_rs", "cost_rupees", "portfolio_cost_rupees")
        )
        if net is None and gross is not None:
            net = gross - (fees or 0.0)
        if gross is None and net is not None:
            gross = net + (fees or 0.0)
        if net is None:
            unresolved += 1
            continue
        net_values.append(net)
        gross_total += gross if gross is not None else net
        fee_total += fees or max(0.0, (gross or net) - net)

    wins = sum(value > 0 for value in net_values)
    losses = sum(value < 0 for value in net_values)
    positive = sum(value for value in net_values if value > 0)
    negative = -sum(value for value in net_values if value < 0)
    usable = len(net_values)
    evidence_state = (
        "SUFFICIENT"
        if usable >= minimum_closed_trades and unresolved == 0
        else "INSUFFICIENT_EVIDENCE"
    )
    return PerformanceSlice(
        dimensions=dict(dimensions or {}),
        records=len(rows),
        closed_trades=usable,
        wins=wins,
        losses=losses,
        unresolved=unresolved,
        gross_pnl_rs=round(gross_total, 10),
        fees_rs=round(fee_total, 10),
        net_pnl_rs=round(sum(net_values), 10),
        win_rate_pct=(wins / usable * 100.0) if usable else None,
        profit_factor=(positive / negative) if negative > 0 else None,
        average_net_pnl_rs=fmean(net_values) if net_values else None,
        maximum_drawdown_rs=_max_drawdown(net_values) if net_values else None,
        evidence_state=evidence_state,
    )


def group_performance(
    records: Iterable[Mapping[str, Any]],
    *,
    group_by: Sequence[str],
    minimum_closed_trades: int = 20,
) -> list[PerformanceSlice]:
    """Group records for offline analysis; never use the groups as metric labels."""

    groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in records:
        groups[tuple(str(row.get(field, "UNAVAILABLE")) for field in group_by)].append(row)
    result = []
    for values, rows in sorted(groups.items()):
        dimensions = dict(zip(group_by, values))
        result.append(
            summarize_performance(
                rows,
                dimensions=dimensions,
                minimum_closed_trades=minimum_closed_trades,
            )
        )
    return result


def execution_drag(record: Mapping[str, Any]) -> dict[str, Any]:
    """Attribute observable entry/exit slippage and fees for one completed trade."""

    side = str(record.get("side", "")).upper()
    quantity = _first_number(record, ("quantity", "filled_quantity"))
    expected_entry = _first_number(record, ("trigger_price", "expected_entry_price"))
    actual_entry = _first_number(record, ("entry_price", "actual_entry_price"))
    expected_exit = _first_number(record, ("expected_exit_price",))
    actual_exit = _first_number(record, ("exit_price", "actual_exit_price"))
    fees = _first_number(record, ("fees_rs", "cost_rupees", "portfolio_cost_rupees")) or 0.0

    entry_drag = None
    if quantity is not None and expected_entry is not None and actual_entry is not None:
        entry_drag = (
            (actual_entry - expected_entry) * quantity
            if side == "LONG"
            else (expected_entry - actual_entry) * quantity
            if side == "SHORT"
            else None
        )
    exit_drag = None
    if quantity is not None and expected_exit is not None and actual_exit is not None:
        exit_drag = (
            (expected_exit - actual_exit) * quantity
            if side == "LONG"
            else (actual_exit - expected_exit) * quantity
            if side == "SHORT"
            else None
        )
    known_drag = sum(value for value in (entry_drag, exit_drag, fees) if value is not None)
    complete = entry_drag is not None and fees is not None
    return {
        "signal_id": record.get("signal_id"),
        "entry_slippage_cost_rs": entry_drag,
        "exit_slippage_cost_rs": exit_drag,
        "fees_rs": fees,
        "known_execution_drag_rs": known_drag,
        "state": "COMPLETE" if complete else "PARTIAL",
    }


__all__ = [
    "PerformanceSlice",
    "execution_drag",
    "group_performance",
    "summarize_performance",
]
