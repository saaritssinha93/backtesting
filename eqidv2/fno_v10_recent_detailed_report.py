"""Build an auditable recent-session detail pack for V10 .50 + Gap2.

The primary window is the last N expected exchange sessions ending on the
source run's last usable session. Missing source sessions are represented as
missing, never as flat. A supplemental last-N-usable window is also reported.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import re
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_backtest_config as locked_config
import fno_v10_experiment_backtest as experiment
import fno_v10_gap_guard_research as gaps
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v10_recent_detailed_result_v1"
DEFAULT_OUTPUT_ROOT = (
    common.FNO_ROOT / "strategy_research" / "v10_recent_detailed_results_v1"
)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(False, index=frame.index, dtype=bool)
    return frame[column].astype(str).str.strip().str.lower().eq("true")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _day_series(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["session_date"], errors="raise").dt.date


def _profit_factor(returns: pd.Series) -> float | None:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    profit = float(values.loc[values.gt(0)].sum())
    loss = float(-values.loc[values.lt(0)].sum())
    if loss > 0:
        return profit / loss
    return math.inf if profit > 0 else None


def _joined(values: Iterable[object]) -> str:
    selected = [str(value) for value in values if str(value).strip()]
    return " | ".join(selected)


def _count_status(frame: pd.DataFrame, status: str) -> int:
    if frame.empty or "status" not in frame:
        return 0
    return int(frame["status"].astype(str).eq(status).sum())


def _parse_checks(value: object) -> list[dict[str, Any]]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        normalized = re.sub(
            r"Timestamp\('([^']+)'(?:, tz='[^']+')?\)",
            lambda match: repr(match.group(1)),
            text,
        )
        decoded = ast.literal_eval(normalized)
    if not isinstance(decoded, list):
        raise ValueError("confirmation_checks must encode a list")
    return [dict(item) for item in decoded]


def _setup_parameter_table() -> pd.DataFrame:
    experiment.configure_engine(locked_config.ACTIVE_VARIANT)
    base_policy = experiment._entry_policy_for_variant(
        locked_config.ACTIVE_VARIANT,
        cost_bps=15.0,
        slippage_bps=0.0,
        square_off="15:30",
        eod_policy="LAST_REAL_BAR_SENSITIVITY",
    )
    records: list[dict[str, Any]] = []
    for setup in engine.ACTIVE_SETUPS:
        policy = engine.policy_for_setup(setup, base_policy)
        record = asdict(setup)
        record["setup_id"] = setup.setup_id
        record.update(
            {
                "effective_buffer_bps": policy.buffer_bps,
                "effective_max_confirmation_minute": (
                    policy.max_confirmation_minute
                ),
                "entry_expiry_minute": policy.entry_expiry_minute,
                "effective_close_location_min": policy.close_location_min,
                "effective_midpoint_invalidation": policy.midpoint_invalidation,
                "post_confirmation_cancel": policy.post_confirmation_cancel,
                "allow_cap_reassignment": policy.allow_cap_reassignment,
                "same_bar_policy": policy.same_bar_policy,
                "square_off": policy.square_off,
                "eod_policy": policy.eod_policy,
                "cost_bps": policy.cost_bps,
                "slippage_bps": policy.slippage_bps,
                "stage7_0940_long_move_min_pct": 0.40,
                "challenger_0935_long_move_max_pct": 0.50,
                "gap_guard_max_adverse_bps": 2.0,
                "five_minute_ema_rule": (
                    "EMA9>EMA20>EMA50"
                    if setup.side == "LONG"
                    else "EMA9<EMA20<EMA50"
                ),
                "five_minute_side_aware_move_min_pct": setup.price_change_pct,
                "five_minute_oi_change_min_pct": setup.oi_change_pct,
                "five_minute_volume_ratio_min": setup.volume_ratio,
                "five_minute_traded_value_min": setup.min_traded_value,
                "one_minute_confirmation_body_ratio_min": setup.body_ratio,
                "one_minute_confirmation_adverse_wick_ratio_max": (
                    setup.max_wick_ratio
                ),
            }
        )
        records.append(record)
    return pd.DataFrame(records).sort_values("setup_id", kind="stable")


def _validate_source_tables(
    decisions: pd.DataFrame,
    selected: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
) -> None:
    for label, frame in (
        ("selection decisions", decisions),
        ("selected candidates", selected),
        ("candidate audit", audit),
        ("closed trades", closed),
    ):
        if frame["candidate_id"].astype(str).duplicated().any():
            raise AssertionError(f"{label} contains duplicate candidate IDs")
    passed_ids = set(
        decisions.loc[_bool_series(decisions, "selection_passed"), "candidate_id"]
        .astype(str)
        .tolist()
    )
    selected_ids = set(selected["candidate_id"].astype(str))
    audit_ids = set(audit["candidate_id"].astype(str))
    closed_ids = set(closed["candidate_id"].astype(str))
    if passed_ids != selected_ids or selected_ids != audit_ids:
        raise AssertionError("Selection decisions, selected candidates and audit differ")
    if not closed_ids.issubset(audit_ids):
        raise AssertionError("Closed trades are not a subset of the audit")


def _selection_detail(
    decisions: pd.DataFrame, audit: pd.DataFrame
) -> pd.DataFrame:
    audit_columns = [
        "candidate_id",
        "setup_cap",
        "status",
        "reason",
        "confirmation_minute",
        "confirmation_time",
        "confirmation_open",
        "confirmation_high",
        "confirmation_low",
        "confirmation_close",
        "confirmation_volume",
        "confirmation_body_ratio",
        "confirmation_adverse_wick_ratio",
        "confirmation_close_location",
        "confirmation_rejection_codes",
        "entry_minute",
        "entry_time",
        "trigger",
        "entry_price",
        "gap_fill",
        "gap_guard_observed",
        "gap_guard_rejected",
        "gap_guard_adverse_bps",
        "stop_price",
        "target_price",
        "exit_time",
        "exit_price",
        "exit_reason",
        "gross_return_pct",
        "net_return_pct",
        "filled",
        "quantity",
        "position_notional_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "portfolio_decision",
        "portfolio_reject_reason",
    ]
    available = [column for column in audit_columns if column in audit]
    detail = decisions.merge(
        audit[available], on="candidate_id", how="left", validate="one_to_one"
    )
    passed = _bool_series(detail, "selection_passed")
    rank = pd.to_numeric(detail["recalculated_frozen_rank"], errors="coerce")
    detail["selection_explanation"] = np.where(
        passed,
        "BASE_5M_PASS;OVERLAYS_PASS;RERANK="
        + rank.astype("Int64").astype(str)
        + ";ONE_MINUTE_STATE="
        + detail["status"].fillna("NOT_AUDITED").astype(str),
        "POST_SELECTION_REJECTED:" + detail["selection_reason"].astype(str),
    )
    detail["base_universe_limitation"] = (
        "Cache contains base-5m-qualified candidates only; symbols failing "
        "the base setup are represented by the setup parameter table, not rows."
    )
    return detail.sort_values(
        ["session_date", "setup_id", "original_frozen_rank", "symbol"],
        kind="stable",
    ).reset_index(drop=True)


def _confirmation_detail(audit: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    identity = [
        "candidate_id",
        "session_date",
        "setup_id",
        "setup_cap",
        "side",
        "symbol",
        "frozen_rank",
        "status",
        "reason",
        "confirmation_minute",
        "confirmation_time",
        "entry_minute",
        "entry_time",
        "entry_price",
        "filled",
    ]
    for row in audit.to_dict("records"):
        base = {column: row.get(column) for column in identity}
        for check in _parse_checks(row.get("confirmation_checks")):
            codes = check.get("rejection_codes", [])
            records.append(
                {
                    **base,
                    "check_minute_index": check.get("minute_index"),
                    "check_timestamp": check.get("timestamp"),
                    "gate_evaluated": check.get("gate_evaluated"),
                    "check_passed": check.get("passed"),
                    "rejection_codes": _joined(codes),
                    "bar_open": check.get("open"),
                    "bar_high": check.get("high"),
                    "bar_low": check.get("low"),
                    "bar_close": check.get("close"),
                    "bar_volume": check.get("volume"),
                    "candle_range": check.get("candle_range"),
                    "body_ratio": check.get("body_ratio"),
                    "adverse_wick_ratio": check.get("adverse_wick_ratio"),
                    "close_location": check.get("close_location"),
                }
            )
    return pd.DataFrame(records).sort_values(
        ["session_date", "setup_id", "frozen_rank", "check_minute_index"],
        kind="stable",
    ).reset_index(drop=True)


def _trade_detail(closed: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "candidate_id",
        "session_date",
        "setup_id",
        "setup_cap",
        "side",
        "symbol",
        "futures_symbol",
        "frozen_rank",
        "picker",
        "picker_value",
        "price_change_pct",
        "oi_change_pct",
        "volume_ratio",
        "traded_value",
        "five_min_open",
        "five_min_high",
        "five_min_low",
        "five_min_close",
        "ema9",
        "ema20",
        "ema50",
        "confirmation_minute",
        "confirmation_time",
        "confirmation_open",
        "confirmation_high",
        "confirmation_low",
        "confirmation_close",
        "confirmation_body_ratio",
        "confirmation_adverse_wick_ratio",
        "confirmation_close_location",
        "entry_minute",
        "entry_time",
        "trigger",
        "entry_price",
        "gap_fill",
        "gap_guard_adverse_bps",
        "stop_price",
        "target_price",
        "exit_time",
        "exit_price",
        "exit_reason",
        "gross_return_pct",
        "cost_bps",
        "slippage_bps",
        "net_return_pct",
        "quantity",
        "position_notional_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
    ]
    available = [column for column in columns if column in closed]
    result = closed[available].copy()
    net = _numeric(result, "net_return_pct")
    result["outcome"] = np.select(
        [net.gt(0), net.lt(0)], ["WIN", "LOSS"], default="FLAT"
    )
    return result.sort_values(
        ["session_date", "entry_time", "setup_id", "frozen_rank"],
        kind="stable",
    ).reset_index(drop=True)


def _aggregate_window(
    closed: pd.DataFrame, sessions: Sequence[date]
) -> dict[str, Any]:
    selected = closed.loc[_day_series(closed).isin(set(sessions))].copy()
    returns = _numeric(selected, "net_return_pct")
    pnl = _numeric(selected, "net_pnl_rs")
    return {
        "expected_sessions": len(sessions),
        "fills": len(selected),
        "wins": int(returns.gt(0).sum()),
        "losses": int(returns.lt(0).sum()),
        "flat_trades": int(returns.eq(0).sum()),
        "win_rate_pct": float(returns.gt(0).mean() * 100.0)
        if len(selected)
        else None,
        "profit_factor": _profit_factor(returns),
        "net_return_points": float(returns.sum()),
        "net_pnl_rs": float(pnl.sum()),
    }


def _daily_summary(
    union_sessions: Sequence[date],
    available_sessions: set[date],
    official_sessions: set[date],
    usable_sessions: set[date],
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
) -> pd.DataFrame:
    decision_days = _day_series(decisions)
    audit_days = _day_series(audit)
    closed_days = _day_series(closed)
    records: list[dict[str, Any]] = []
    for session in union_sessions:
        available = session in available_sessions
        if not available:
            records.append(
                {
                    "session_date": session,
                    "data_available": False,
                    "official_last_n": session in official_sessions,
                    "last_n_usable": session in usable_sessions,
                    "data_status": "MISSING_VALIDATED_CACHE",
                }
            )
            continue
        day_decisions = decisions.loc[decision_days.eq(session)].copy()
        day_audit = audit.loc[audit_days.eq(session)].copy()
        day_closed = closed.loc[closed_days.eq(session)].copy()
        passed = _bool_series(day_decisions, "selection_passed")
        confirmed = _numeric(day_audit, "confirmation_minute").notna()
        filled = _bool_series(day_audit, "filled")
        returns = _numeric(day_closed, "net_return_pct")
        records.append(
            {
                "session_date": session,
                "data_available": True,
                "official_last_n": session in official_sessions,
                "last_n_usable": session in usable_sessions,
                "data_status": "AVAILABLE",
                "raw_base_5m_candidates": len(day_decisions),
                "post_overlay_selected": int(passed.sum()),
                "post_overlay_rejected": int((~passed).sum()),
                "selected_long": int(
                    (passed & day_decisions["side"].astype(str).eq("LONG")).sum()
                ),
                "selected_short": int(
                    (passed & day_decisions["side"].astype(str).eq("SHORT")).sum()
                ),
                "stage7_0940_long_rejections": int(
                    day_decisions["selection_reason"]
                    .astype(str)
                    .eq("STAGE7_0940_LONG_MOVE_BELOW_040")
                    .sum()
                ),
                "0935_long_max050_rejections": int(
                    day_decisions["selection_reason"]
                    .astype(str)
                    .eq("0935_LONG_MOVE_ABOVE_CHALLENGER_MAX")
                    .sum()
                ),
                "one_minute_confirmed": int(confirmed.sum()),
                "filled": int(filled.sum()),
                "filled_long": int(
                    (filled & day_audit["side"].astype(str).eq("LONG")).sum()
                ),
                "filled_short": int(
                    (filled & day_audit["side"].astype(str).eq("SHORT")).sum()
                ),
                "wins": int(returns.gt(0).sum()),
                "losses": int(returns.lt(0).sum()),
                "flat_trades": int(returns.eq(0).sum()),
                "win_rate_pct": float(returns.gt(0).mean() * 100.0)
                if len(day_closed)
                else None,
                "profit_factor": _profit_factor(returns),
                "net_return_points": float(returns.sum()),
                "net_pnl_rs": float(_numeric(day_closed, "net_pnl_rs").sum()),
                "targets": _count_status(day_audit, "TARGETED"),
                "stops": _count_status(day_audit, "STOPPED"),
                "eod_exits": _count_status(day_audit, "SQUARE_OFF"),
                "no_confirmation": _count_status(day_audit, "NO_CONFIRMATION"),
                "preconfirmation_invalidated": _count_status(
                    day_audit, "PRECONF_INVALIDATED"
                ),
                "postconfirmation_cancelled": _count_status(
                    day_audit, "POSTCONF_CANCELLED"
                ),
                "entry_window_expired": _count_status(
                    day_audit, "WINDOW_EXPIRED"
                ),
                "portfolio_rejected": _count_status(
                    day_audit, "PORTFOLIO_REJECTED"
                ),
                "duplicate_symbol_rejected": _count_status(
                    day_audit, "DUPLICATE_REJECTED"
                ),
                "gap2_rejected": int(
                    _bool_series(day_audit, "gap_guard_rejected").sum()
                ),
                "status_counts_json": json.dumps(
                    {
                        str(key): int(value)
                        for key, value in day_audit["status"]
                        .value_counts()
                        .sort_index()
                        .items()
                    },
                    sort_keys=True,
                ),
            }
        )
    return pd.DataFrame(records)


def _slot_summary(
    sessions: Sequence[date],
    available_sessions: set[date],
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
    setup_parameters: pd.DataFrame,
) -> pd.DataFrame:
    decision_days = _day_series(decisions)
    audit_days = _day_series(audit)
    closed_days = _day_series(closed)
    setup_ids = setup_parameters["setup_id"].tolist()
    records: list[dict[str, Any]] = []
    for session in sessions:
        for setup_id in setup_ids:
            if session not in available_sessions:
                records.append(
                    {
                        "session_date": session,
                        "setup_id": setup_id,
                        "data_available": False,
                        "data_status": "MISSING_VALIDATED_CACHE",
                    }
                )
                continue
            day_decisions = decisions.loc[
                decision_days.eq(session)
                & decisions["setup_id"].astype(str).eq(setup_id)
            ].copy()
            day_audit = audit.loc[
                audit_days.eq(session) & audit["setup_id"].astype(str).eq(setup_id)
            ].copy()
            day_closed = closed.loc[
                closed_days.eq(session)
                & closed["setup_id"].astype(str).eq(setup_id)
            ].copy()
            passed = _bool_series(day_decisions, "selection_passed")
            filled = _bool_series(day_audit, "filled")
            returns = _numeric(day_closed, "net_return_pct")
            records.append(
                {
                    "session_date": session,
                    "setup_id": setup_id,
                    "data_available": True,
                    "data_status": "AVAILABLE",
                    "raw_base_5m_candidates": len(day_decisions),
                    "post_overlay_selected": int(passed.sum()),
                    "post_overlay_rejected": int((~passed).sum()),
                    "selected_symbols": _joined(
                        day_decisions.loc[passed, "symbol"].astype(str)
                    ),
                    "rejected_symbols_and_reasons": _joined(
                        day_decisions.loc[~passed].apply(
                            lambda row: f"{row['symbol']}:{row['selection_reason']}",
                            axis=1,
                        )
                    ),
                    "one_minute_confirmed": int(
                        _numeric(day_audit, "confirmation_minute").notna().sum()
                    ),
                    "filled": int(filled.sum()),
                    "filled_symbols": _joined(
                        day_audit.loc[filled, "symbol"].astype(str)
                    ),
                    "wins": int(returns.gt(0).sum()),
                    "losses": int(returns.lt(0).sum()),
                    "net_return_points": float(returns.sum()),
                    "net_pnl_rs": float(_numeric(day_closed, "net_pnl_rs").sum()),
                    "final_statuses_json": json.dumps(
                        {
                            str(key): int(value)
                            for key, value in day_audit["status"]
                            .value_counts()
                            .sort_index()
                            .items()
                        },
                        sort_keys=True,
                    ),
                }
            )
    result = pd.DataFrame(records)
    return result.merge(setup_parameters, on="setup_id", how="left")


def _markdown_report(
    *,
    source_run: Path,
    official_sessions: Sequence[date],
    usable_sessions: Sequence[date],
    missing_official: Sequence[date],
    official_metrics: Mapping[str, Any],
    usable_metrics: Mapping[str, Any],
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
) -> str:
    official_set = set(official_sessions)
    daily_official = daily.loc[daily["session_date"].isin(official_set)]
    status_counts = audit.loc[_day_series(audit).isin(official_set), "status"].value_counts()
    exit_counts = closed.loc[
        _day_series(closed).isin(official_set), "exit_reason"
    ].value_counts()

    def metric_line(label: str, values: Mapping[str, Any]) -> str:
        pf = values.get("profit_factor")
        pf_text = "n/a" if pf is None else f"{float(pf):.4f}"
        return (
            f"- {label}: {values['fills']} fills, {values['wins']}-{values['losses']}, "
            f"WR {float(values['win_rate_pct'] or 0):.2f}%, PF {pf_text}, "
            f"{float(values['net_return_points']):+.4f} points, "
            f"Rs {float(values['net_pnl_rs']):+,.2f}."
        )

    lines = [
        "# V10 .50 + Gap2 recent detailed result",
        "",
        f"Source run: `{source_run}`",
        "Profile: current mixed per-setup limits; 15 bps total modeled cost; "
        "zero slippage; Gap2; Rs 50,000 modeled exposure per fill.",
        "",
        "## Window integrity",
        "",
        f"Official last {len(official_sessions)} exchange sessions: "
        f"{official_sessions[0]} through {official_sessions[-1]}.",
        f"Missing validated sessions: {', '.join(map(str, missing_official)) or 'none'}.",
        "A missing session is not counted as flat or included in trading metrics.",
        "",
        metric_line("Official window, available sessions only", official_metrics),
        metric_line(f"Supplemental last {len(usable_sessions)} usable sessions", usable_metrics),
        "",
        "## Day-wise result",
        "",
        "| Date | Data | Raw 5m | Selected | Confirmed | Fills | W-L | WR | Net pts | Net P&L |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in daily_official.to_dict("records"):
        if not bool(row["data_available"]):
            lines.append(
                f"| {row['session_date']} | MISSING | - | - | - | - | - | - | - | - |"
            )
            continue
        wr = row.get("win_rate_pct")
        wr_text = "n/a" if pd.isna(wr) else f"{float(wr):.2f}%"
        lines.append(
            "| {day} | OK | {raw} | {selected} | {confirmed} | {fills} | "
            "{wins}-{losses} | {wr} | {points:+.4f} | {pnl:+,.2f} |".format(
                day=row["session_date"],
                raw=int(row["raw_base_5m_candidates"]),
                selected=int(row["post_overlay_selected"]),
                confirmed=int(row["one_minute_confirmed"]),
                fills=int(row["filled"]),
                wins=int(row["wins"]),
                losses=int(row["losses"]),
                wr=wr_text,
                points=float(row["net_return_points"]),
                pnl=float(row["net_pnl_rs"]),
            )
        )
    lines.extend(
        [
            "",
            "## Selection and entry mechanics",
            "",
            "1. Each setup's completed 5-minute bar must pass side-specific EMA "
            "alignment, price move, futures OI change, relative-volume and minimum "
            "traded-value rules. Body and wick thresholds apply later to the "
            "1-minute confirmation candle.",
            "2. Stage 7 rejects 09:40 LONG candidates below +0.40%; the .50 "
            "overlay rejects 09:35 LONG candidates above +0.50%.",
            "3. Remaining candidates are reranked inside date + setup by picker, "
            "then traded value, then symbol.",
            "4. The next completed 1-minute candle(s) must have the correct "
            "direction, close beyond the 5-minute close, sufficient body, acceptable "
            "adverse wick and, where configured, close location.",
            "5. A stop-entry trigger is placed beyond the confirmation high for "
            "LONG or low for SHORT. It may fill only on a later bar through S+5. "
            "Gap2 rejects adverse gap-through fills greater than 2 bps.",
            "6. Per-setup caps, the global 12-position/margin ledger and one-position-"
            "per-symbol rule are then enforced. Stop/target ambiguity is stop-first.",
            "",
            "Status counts in the official window's available sessions:",
            "",
        ]
    )
    lines.extend(f"- {key}: {int(value)}" for key, value in status_counts.items())
    lines.extend(["", "Exit counts:", ""])
    lines.extend(f"- {key}: {int(value)}" for key, value in exit_counts.items())
    lines.extend(
        [
            "",
            "## Files in this pack",
            "",
            "- `daily_summary.csv`: official and supplemental membership, funnel and P&L.",
            "- `slot_day_summary.csv`: every date x 5-minute setup/side occurrence.",
            "- `five_minute_selection_detail.csv`: every cached base-qualified 5-minute candidate.",
            "- `one_minute_confirmation_checks.csv`: every evaluated 1-minute confirmation candle.",
            "- `selected_candidate_final_audit.csv`: complete final state of each selected candidate.",
            "- `closed_trade_detail.csv`: all fills with confirmation, entry, exit and economics.",
            "- `setup_parameter_reference.csv`: all 5-minute and effective 1-minute parameters.",
            "",
            "Important: the candidate cache contains symbols that passed each "
            "base 5-minute setup. It does not retain one rejection row for every "
            "symbol that failed the base setup. The exact base thresholds are in "
            "the parameter reference; all post-selection overlay decisions are lossless.",
            "",
            "This remains a research-only diagnostic, not a production-valid or "
            "actual futures-lot P&L result.",
            "",
        ]
    )
    return "\n".join(lines)


def build_report(
    *, source_run: Path, last_n: int, output_root: Path
) -> Path:
    source_run = source_run.expanduser().resolve()
    if last_n <= 0:
        raise ValueError("last_n must be positive")
    provenance_path = source_run / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    if provenance.get("schema_version") != "fno_v10_max050_gap2_full_history_v1":
        raise ValueError("Source run is not a V10 .50 + Gap2 full-history run")
    if not bool(provenance.get("complete")):
        raise ValueError("Source run is incomplete")
    paths = {
        "decisions": source_run / "selection_decisions.csv",
        "selected": source_run / "selected_candidates.csv",
        "audit": source_run / "scenarios" / "reference_15_0" / "candidate_order_audit.csv",
        "closed": source_run / "scenarios" / "reference_15_0" / "closed_trades.csv",
        "daywise": source_run / "scenarios" / "reference_15_0" / "daywise.csv",
    }
    for path in paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    decisions = pd.read_csv(paths["decisions"])
    selected = pd.read_csv(paths["selected"])
    audit = pd.read_csv(paths["audit"])
    closed = pd.read_csv(paths["closed"])
    daywise = pd.read_csv(paths["daywise"])
    _validate_source_tables(decisions, selected, audit, closed)

    available = sorted(set(_day_series(daywise)))
    if len(available) < last_n:
        raise ValueError(f"Source has only {len(available)} usable sessions")
    usable_sessions = available[-last_n:]
    expected_span = engine.expected_regular_session_dates(
        min(available), max(available)
    )
    if len(expected_span) < last_n:
        raise ValueError("Expected exchange calendar is shorter than requested")
    official_sessions = expected_span[-last_n:]
    missing_official = sorted(set(official_sessions) - set(available))
    union_sessions = sorted(set(official_sessions) | set(usable_sessions))
    union_set = set(union_sessions)

    decisions_recent = decisions.loc[_day_series(decisions).isin(union_set)].copy()
    selected_recent = selected.loc[_day_series(selected).isin(union_set)].copy()
    audit_recent = audit.loc[_day_series(audit).isin(union_set)].copy()
    closed_recent = closed.loc[_day_series(closed).isin(union_set)].copy()
    setup_parameters = _setup_parameter_table()
    selection_detail = _selection_detail(decisions_recent, audit_recent)
    confirmation_detail = _confirmation_detail(audit_recent)
    trade_detail = _trade_detail(closed_recent)
    daily = _daily_summary(
        union_sessions,
        set(available),
        set(official_sessions),
        set(usable_sessions),
        decisions_recent,
        audit_recent,
        closed_recent,
    )
    slot = _slot_summary(
        union_sessions,
        set(available),
        decisions_recent,
        audit_recent,
        closed_recent,
        setup_parameters,
    )
    official_metrics = _aggregate_window(closed_recent, official_sessions)
    official_metrics["available_sessions"] = len(
        set(official_sessions) & set(available)
    )
    official_metrics["missing_sessions"] = [
        value.isoformat() for value in missing_official
    ]
    usable_metrics = _aggregate_window(closed_recent, usable_sessions)
    usable_metrics["available_sessions"] = len(usable_sessions)
    usable_metrics["missing_sessions"] = []

    output_root = output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(gaps.IST).strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = output_root / f"last{last_n}_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    outputs = {
        "daily_summary": run_dir / "daily_summary.csv",
        "slot_day_summary": run_dir / "slot_day_summary.csv",
        "five_minute_selection_detail": run_dir / "five_minute_selection_detail.csv",
        "one_minute_confirmation_checks": run_dir / "one_minute_confirmation_checks.csv",
        "selected_candidate_final_audit": run_dir / "selected_candidate_final_audit.csv",
        "closed_trade_detail": run_dir / "closed_trade_detail.csv",
        "setup_parameter_reference": run_dir / "setup_parameter_reference.csv",
        "overview": run_dir / "overview.json",
        "report": run_dir / "report.md",
    }
    common.atomic_write_csv(daily, outputs["daily_summary"])
    common.atomic_write_csv(slot, outputs["slot_day_summary"])
    common.atomic_write_csv(selection_detail, outputs["five_minute_selection_detail"])
    common.atomic_write_csv(confirmation_detail, outputs["one_minute_confirmation_checks"])
    common.atomic_write_csv(
        audit_recent.sort_values(
            ["session_date", "setup_id", "frozen_rank", "symbol"], kind="stable"
        ),
        outputs["selected_candidate_final_audit"],
    )
    common.atomic_write_csv(trade_detail, outputs["closed_trade_detail"])
    common.atomic_write_csv(setup_parameters, outputs["setup_parameter_reference"])
    overview = {
        "schema_version": SCHEMA_VERSION,
        "source_run": str(source_run),
        "profile": "V10_STAGE7_0935_LONG_MAX_050_GAP2_CURRENT_MIXED_LIMITS",
        "official_last_n_sessions": [value.isoformat() for value in official_sessions],
        "official_missing_sessions": [value.isoformat() for value in missing_official],
        "last_n_usable_sessions": [value.isoformat() for value in usable_sessions],
        "official_window_metrics_available_sessions_only": official_metrics,
        "supplemental_last_n_usable_metrics": usable_metrics,
        "row_counts": {
            "five_minute_selection_detail": len(selection_detail),
            "selected_candidate_final_audit": len(audit_recent),
            "one_minute_confirmation_checks": len(confirmation_detail),
            "closed_trade_detail": len(trade_detail),
            "slot_day_summary": len(slot),
        },
        "research_only": True,
        "headline_valid": False,
    }
    common.atomic_write_json(outputs["overview"], gaps._json_ready(overview))
    common.atomic_write_text(
        outputs["report"],
        _markdown_report(
            source_run=source_run,
            official_sessions=official_sessions,
            usable_sessions=usable_sessions,
            missing_official=missing_official,
            official_metrics=official_metrics,
            usable_metrics=usable_metrics,
            daily=daily,
            audit=audit_recent,
            closed=closed_recent,
        ),
    )
    inventory_path = run_dir / "artifact_inventory.json"
    inventory = {
        "schema_version": SCHEMA_VERSION,
        "artifacts": [
            {
                "relative_path": str(path.relative_to(run_dir)).replace("\\", "/"),
                "bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for path in sorted(outputs.values())
        ],
        "source_artifacts": {
            name: {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for name, path in {"provenance": provenance_path, **paths}.items()
        },
    }
    common.atomic_write_json(inventory_path, inventory)
    common.atomic_write_json(
        output_root / "latest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_dir": str(run_dir),
            "official_session_count": len(official_sessions),
            "official_missing_session_count": len(missing_official),
            "usable_session_count": len(usable_sessions),
            "inventory_sha256": _sha256_file(inventory_path),
        },
    )
    return run_dir


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path, required=True)
    parser.add_argument("--last-n", type=int, default=14)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    run_dir = build_report(
        source_run=args.source_run,
        last_n=args.last_n,
        output_root=args.output_root,
    )
    print(f"[V10-RECENT-DETAIL] complete: {run_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
