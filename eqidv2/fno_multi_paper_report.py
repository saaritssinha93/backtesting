"""Deterministic Long/Short/Result/Log views for one combined PAPER session."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Sequence

import fno_multi_paper_profiles as profiles
from fno_multi_paper_parity import PARITY_STATUS


REPORT_SCHEMA_VERSION = "fno_multi_paper_report_v1"


def _number(value: Any, digits: int = 2) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{number:,.{digits}f}" if math.isfinite(number) else "-"


def _text(value: Any) -> str:
    return str(value if value not in {None, ""} else "-").replace("|", "\\|")


def _closed(record: Mapping[str, Any]) -> bool:
    return (
        record.get("portfolio_decision") == "ACCEPTED"
        and record.get("entry_price") is not None
        and record.get("exit_price") is not None
    )


def profile_summary(
    profile: profiles.StrategyProfile,
    records: Sequence[Mapping[str, Any]],
    *,
    runtime_status: str,
    source_complete: bool,
) -> dict[str, Any]:
    accepted_fills = [
        row
        for row in records
        if row.get("portfolio_decision") == "ACCEPTED"
        and row.get("entry_price") is not None
    ]
    closed = [row for row in accepted_fills if _closed(row)]
    open_rows = [row for row in accepted_fills if row.get("exit_price") is None]
    wins = sum(float(row.get("net_pnl_rs") or 0.0) > 0 for row in closed)
    losses = sum(float(row.get("net_pnl_rs") or 0.0) < 0 for row in closed)
    gross_profit = sum(max(0.0, float(row.get("net_pnl_rs") or 0.0)) for row in closed)
    gross_loss = -sum(min(0.0, float(row.get("net_pnl_rs") or 0.0)) for row in closed)
    return {
        "profile_key": profile.key,
        "profile_id": profile.profile_id,
        "profile_fingerprint": profile.fingerprint,
        "status": runtime_status,
        "candidate_count": len(records),
        "long_candidates": sum(str(row.get("side")) == "LONG" for row in records),
        "short_candidates": sum(str(row.get("side")) == "SHORT" for row in records),
        "fill_count": len(accepted_fills),
        "closed_count": len(closed),
        "open_count": len(open_rows),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": (wins / len(closed) * 100.0 if closed else None),
        "profit_factor": (
            gross_profit / gross_loss if gross_loss > 0 else None
        ),
        "net_return_points": sum(float(row.get("net_return_pct") or 0.0) for row in closed),
        "net_pnl_rs": sum(float(row.get("net_pnl_rs") or 0.0) for row in closed),
        "gap_guard_rejections": sum(bool(row.get("gap_guard_rejected")) for row in records),
        "data_incomplete_count": sum(str(row.get("status")) == "DATA_INCOMPLETE" for row in records),
        "source_complete": bool(source_complete),
        "headline_valid": bool(
            runtime_status == "COMPLETE"
            and source_complete
            and not open_rows
            and not any(str(row.get("status")) == "DATA_INCOMPLETE" for row in records)
        ),
    }


def _trade_table(records: Sequence[Mapping[str, Any]], side: str) -> list[str]:
    selected = [row for row in records if str(row.get("side")) == side]
    lines = [
        "| Signal | Setup | Rank | Symbol | Status | Confirm | Trigger | Entry | Exit | Exit reason | Net P&L |",
        "|---|---|---:|---|---|---|---:|---:|---:|---|---:|",
    ]
    if not selected:
        lines.append("| - | - | - | No candidates | - | - | - | - | - | - | - |")
        return lines
    for row in selected:
        signal = str(row.get("signal_time", ""))[11:16] or "-"
        lines.append(
            "| "
            + " | ".join(
                (
                    _text(signal),
                    _text(row.get("setup_id")),
                    _text(row.get("frozen_rank")),
                    _text(row.get("symbol")),
                    _text(row.get("status")),
                    _text(row.get("confirmation_time")),
                    _number(row.get("trigger")),
                    _number(row.get("entry_price")),
                    _number(row.get("exit_price")),
                    _text(row.get("exit_reason")),
                    _number(row.get("net_pnl_rs")),
                )
            )
            + " |"
        )
    return lines


def render_profile_report(
    profile: profiles.StrategyProfile,
    records: Sequence[Mapping[str, Any]],
    selection_records: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    *,
    session_date: date,
    runtime_status: str,
    source_complete: bool,
    message: str,
    generated_at: datetime,
) -> tuple[str, dict[str, Any]]:
    summary = profile_summary(
        profile, records, runtime_status=runtime_status, source_complete=source_complete
    )
    lines = [
        f"# FnO {profile.display_name} PAPER",
        "",
        f"- Session date: `{session_date.isoformat()}`",
        f"- Runtime status: `{runtime_status}`",
        f"- Mode: `PAPER`",
        f"- Profile: `{profile.profile_id}`",
        f"- Profile fingerprint: `{profile.fingerprint}`",
        f"- Generated at: `{generated_at.isoformat()}`",
        f"- Source complete: `{str(source_complete).lower()}`",
        f"- Headline valid: `{str(summary['headline_valid']).lower()}`",
        f"- Parity status: `{PARITY_STATUS}`",
        "- Full-history event parity certified: `false`",
        f"- Message: {_text(message)}",
        "",
        "## Result",
        "",
        "| Candidates | Fills | Closed | Open | W/L | WR | PF | Net points | Net P&L | Gap2 rejects |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        "| "
        + " | ".join(
            (
                str(summary["candidate_count"]),
                str(summary["fill_count"]),
                str(summary["closed_count"]),
                str(summary["open_count"]),
                f"{summary['wins']}/{summary['losses']}",
                _number(summary["win_rate_pct"]),
                _number(summary["profit_factor"], 3),
                _number(summary["net_return_points"], 4),
                f"Rs {_number(summary['net_pnl_rs'])}",
                str(summary["gap_guard_rejections"]),
            )
        )
        + " |",
        "",
        "## 5-Minute Selection",
        "",
        "The complete full-universe selection audit is stored in `selection_audit.csv`; this view lists every selected row and up to 250 rejected rows.",
        "",
        "| Signal | Setup | Symbol | Decision | Reason | Rank | Move % | OI % | Vol ratio | Traded value | EMA9/20/50 |",
        "|---|---|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    visible_selection = [row for row in selection_records if row.get("selected_5m")]
    visible_selection.extend(
        [row for row in selection_records if not row.get("selected_5m")][:250]
    )
    if not visible_selection:
        lines.append("| - | - | - | NOT EVALUATED | No five-minute source yet | - | - | - | - | - | - |")
    for row in visible_selection:
        lines.append(
            "| "
            + " | ".join(
                (
                    _text(str(row.get("signal_time", ""))[11:16]),
                    _text(row.get("setup_id")),
                    _text(row.get("symbol")),
                    _text(row.get("selection_status")),
                    _text(row.get("selection_reason")),
                    _text(row.get("selection_rank")),
                    _number(row.get("price_change_pct"), 4),
                    _number(row.get("oi_change_pct"), 4),
                    _number(row.get("volume_ratio"), 3),
                    _number(row.get("traded_value"), 0),
                    "/".join(
                        _number(row.get(name), 2) for name in ("ema9", "ema20", "ema50")
                    ),
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## 1-Minute Entry Decisions",
            "",
            "| Signal | Setup | Symbol | State | Confirm minute | Confirm time | Confirmation checks | Body/Wick/CLV | Trigger | Gap2 | Entry time | Entry | Stop | Target |",
            "|---|---|---|---|---:|---|---|---|---:|---|---|---:|---:|---:|",
        ]
    )
    if not records:
        lines.append("| - | - | No selected candidates | - | - | - | - | - | - | - | - | - | - | - |")
    for row in records:
        checks = row.get("confirmation_checks") or []
        check_summary = "; ".join(
            f"S+{check.get('minute_index', '?')}:{'PASS' if check.get('passed') else ','.join(check.get('rejection_codes') or ['FAIL'])}"
            for check in checks
        )
        morphology = "; ".join(
            "S+{}={}/{}/{}".format(
                check.get("minute_index", "?"),
                _number(check.get("body_ratio"), 3),
                _number(check.get("adverse_wick_ratio"), 3),
                _number(check.get("close_location"), 3),
            )
            for check in checks
        )
        lines.append(
            "| "
            + " | ".join(
                (
                    _text(str(row.get("signal_time", ""))[11:16]),
                    _text(row.get("setup_id")),
                    _text(row.get("symbol")),
                    _text(row.get("status")),
                    _text(row.get("confirmation_minute")),
                    _text(row.get("confirmation_time")),
                    _text(check_summary),
                    _text(morphology),
                    _number(row.get("trigger")),
                    _text(
                        "REJECTED"
                        if row.get("gap_guard_rejected")
                        else "ACCEPTED"
                        if row.get("gap_guard_observed")
                        else "NOT_OBSERVED"
                    ),
                    _text(row.get("entry_time")),
                    _number(row.get("entry_price")),
                    _number(row.get("stop_price")),
                    _number(row.get("target_price")),
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
        "## Long",
        "",
        *_trade_table(records, "LONG"),
        "",
        "## Short",
        "",
        *_trade_table(records, "SHORT"),
        "",
        "## Logs",
        "",
        "| Time | Candidate | Scope | Before | After | Reason |",
        "|---|---|---|---|---|---|",
        ]
    )
    if not events:
        lines.append("| - | - | - | - | - | No strategy events yet |")
    else:
        for event in events[-250:]:
            lines.append(
                "| "
                + " | ".join(
                    _text(event.get(field))
                    for field in (
                        "event_time",
                        "candidate_id",
                        "scope",
                        "state_before",
                        "state_after",
                        "reason",
                    )
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "This is completed-candle PAPER simulation. It does not place broker orders.",
            "",
        ]
    )
    return "\n".join(lines), summary


def render_combined_report(
    summaries: Mapping[str, Mapping[str, Any]],
    *,
    session_date: date,
    runtime_status: str,
    source_complete: bool,
    message: str,
    generated_at: datetime,
) -> str:
    lines = [
        "# FnO V10/V11/V12 PAPER",
        "",
        f"- Session date: `{session_date.isoformat()}`",
        f"- Runtime status: `{runtime_status}`",
        f"- Generated at: `{generated_at.isoformat()}`",
        f"- Source complete: `{str(source_complete).lower()}`",
        f"- Parity status: `{PARITY_STATUS}`",
        "- Full-history event parity certified: `false`",
        f"- Message: {_text(message)}",
        "",
        "| Strategy | Candidates | Fills | Closed | Open | W/L | WR | PF | Net points | Net P&L | Valid |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for profile in profiles.PROFILES:
        item = summaries[profile.key]
        lines.append(
            "| "
            + " | ".join(
                (
                    profile.display_name,
                    str(item["candidate_count"]),
                    str(item["fill_count"]),
                    str(item["closed_count"]),
                    str(item["open_count"]),
                    f"{item['wins']}/{item['losses']}",
                    _number(item["win_rate_pct"]),
                    _number(item["profit_factor"], 3),
                    _number(item["net_return_points"], 4),
                    f"Rs {_number(item['net_pnl_rs'])}",
                    str(bool(item["headline_valid"])).lower(),
                )
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "V10, V11 and V12 share source candles only. Their candidate state, capacity, positions and P&L remain independent.",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "REPORT_SCHEMA_VERSION",
    "profile_summary",
    "render_combined_report",
    "render_profile_report",
]
