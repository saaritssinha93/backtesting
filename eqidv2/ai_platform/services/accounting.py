"""Money aggregation that keeps realized and open mark P&L separate."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable

from ai_platform.schemas import MoneySummary, OptionOrderRecord


def summarize_options(
    records: Iterable[OptionOrderRecord],
    *,
    capital_rs: Decimal | None = None,
    rounding: Decimal = Decimal("0.01"),
) -> MoneySummary:
    rows = list(records)
    identities = {
        (row.mode, row.strategy_version, row.strategy_fingerprint, row.run_kind)
        for row in rows
    }
    if len(identities) > 1:
        raise ValueError("Option accounting cannot implicitly aggregate modes, profiles, or run kinds")
    closed = [row for row in rows if row.status == "CLOSED"]
    opened = [row for row in rows if row.status == "OPEN"]
    realized = sum(
        (row.realized_net_pnl_rs or Decimal("0") for row in closed),
        Decimal("0"),
    ).quantize(rounding, rounding=ROUND_HALF_UP)
    mark = sum(
        (row.open_mark_net_pnl_rs or Decimal("0") for row in opened),
        Decimal("0"),
    ).quantize(rounding, rounding=ROUND_HALF_UP)
    return_pct = None
    if capital_rs is not None:
        if capital_rs <= 0:
            raise ValueError("capital_rs must be positive")
        return_pct = (realized / capital_rs * Decimal("100")).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
    return MoneySummary(
        trades=len(rows),
        open_trades=len(opened),
        closed_trades=len(closed),
        realized_net_pnl_rs=realized,
        open_mark_net_pnl_rs=mark,
        realized_return_on_capital_pct=return_pct,
    )


def summarize_options_by_run_kind(
    records: Iterable[OptionOrderRecord],
    *,
    capital_rs: Decimal | None = None,
) -> dict[str, MoneySummary]:
    grouped: dict[str, list[OptionOrderRecord]] = {}
    for record in records:
        grouped.setdefault(record.run_kind, []).append(record)
    return {
        run_kind: summarize_options(rows, capital_rs=capital_rs)
        for run_kind, rows in grouped.items()
    }
