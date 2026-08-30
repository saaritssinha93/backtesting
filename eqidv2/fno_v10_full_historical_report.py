"""Generate the study-grade full-history V10 report and supporting assets."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_v10_gap_guard_research as gaps
import fno_v10_recent_detailed_report as recent
import fno_v8_windowed_1m_entry_backtest as engine


SCHEMA_VERSION = "fno_v10_full_historical_study_report_v1"


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _days(frame: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(frame["session_date"], errors="raise").dt.date


def _numbers(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def _bools(frame: pd.DataFrame, column: str) -> pd.Series:
    return recent._bool_series(frame, column)


def _safe_div(numerator: float, denominator: float) -> float | None:
    return float(numerator / denominator) if denominator else None


def _streak(values: Iterable[int], target: int) -> int:
    best = 0
    current = 0
    for value in values:
        if value == target:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _trade_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    returns = _numbers(frame, "net_return_pct").dropna()
    pnl = _numbers(frame, "net_pnl_rs").dropna()
    wins = returns.gt(0)
    losses = returns.lt(0)
    gross_profit_points = float(returns.loc[wins].sum())
    gross_loss_points = float(-returns.loc[losses].sum())
    gross_profit_rs = float(pnl.loc[pnl.gt(0)].sum())
    gross_loss_rs = float(-pnl.loc[pnl.lt(0)].sum())
    avg_win = float(returns.loc[wins].mean()) if wins.any() else None
    avg_loss = float(returns.loc[losses].mean()) if losses.any() else None
    return {
        "fills": len(frame),
        "wins": int(wins.sum()),
        "losses": int(losses.sum()),
        "flat_trades": int(returns.eq(0).sum()),
        "win_rate_pct": float(wins.mean() * 100.0) if len(returns) else None,
        "profit_factor": (
            gross_profit_points / gross_loss_points
            if gross_loss_points > 0
            else math.inf
            if gross_profit_points > 0
            else None
        ),
        "net_return_points": float(returns.sum()),
        "net_pnl_rs": float(pnl.sum()),
        "gross_profit_points": gross_profit_points,
        "gross_loss_points": gross_loss_points,
        "gross_profit_rs": gross_profit_rs,
        "gross_loss_rs": gross_loss_rs,
        "average_return_points": float(returns.mean()) if len(returns) else None,
        "median_return_points": float(returns.median()) if len(returns) else None,
        "average_pnl_rs": float(pnl.mean()) if len(pnl) else None,
        "median_pnl_rs": float(pnl.median()) if len(pnl) else None,
        "average_win_points": avg_win,
        "average_loss_points": avg_loss,
        "payoff_ratio": (
            avg_win / abs(avg_loss)
            if avg_win is not None and avg_loss not in (None, 0)
            else None
        ),
    }


def _group_metrics(frame: pd.DataFrame, group: str) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for key, part in frame.groupby(group, dropna=False, sort=False):
        records.append({group: key, **_trade_metrics(part)})
    return pd.DataFrame(records)


def _add_features(audit: pd.DataFrame) -> pd.DataFrame:
    out = audit.copy()
    side_sign = np.where(out["side"].astype(str).eq("LONG"), 1.0, -1.0)
    open_ = _numbers(out, "five_min_open")
    high = _numbers(out, "five_min_high")
    low = _numbers(out, "five_min_low")
    close = _numbers(out, "five_min_close")
    candle_range = high - low
    out["directional_move_pct"] = _numbers(out, "price_change_pct") * side_sign
    out["traded_value_cr"] = _numbers(out, "traded_value") / 10_000_000.0
    out["five_min_body_ratio"] = (close - open_).abs().div(
        candle_range.where(candle_range.gt(0))
    )
    long_wick = (high - pd.concat([open_, close], axis=1).max(axis=1)).div(
        candle_range.where(candle_range.gt(0))
    )
    short_wick = (pd.concat([open_, close], axis=1).min(axis=1) - low).div(
        candle_range.where(candle_range.gt(0))
    )
    out["five_min_adverse_wick_ratio"] = np.where(
        side_sign > 0, long_wick, short_wick
    )
    long_location = (close - low).div(candle_range.where(candle_range.gt(0)))
    short_location = (high - close).div(candle_range.where(candle_range.gt(0)))
    out["five_min_directional_close_location"] = np.where(
        side_sign > 0, long_location, short_location
    )
    out["ema_fast_gap_pct"] = (
        (_numbers(out, "ema9") - _numbers(out, "ema20"))
        * side_sign
        / close
        * 100.0
    )
    out["ema_slow_gap_pct"] = (
        (_numbers(out, "ema20") - _numbers(out, "ema50"))
        * side_sign
        / close
        * 100.0
    )
    out["ema_total_gap_pct"] = (
        (_numbers(out, "ema9") - _numbers(out, "ema50"))
        * side_sign
        / close
        * 100.0
    )
    if "entry_time" in out and "exit_time" in out:
        out["holding_minutes"] = (
            pd.to_datetime(out["exit_time"], errors="coerce", utc=True)
            - pd.to_datetime(out["entry_time"], errors="coerce", utc=True)
        ).dt.total_seconds() / 60.0
    return out


def _funnel_by(
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
    group: str,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    keys = list(dict.fromkeys(decisions[group].astype(str).tolist()))
    for key in keys:
        raw = decisions.loc[decisions[group].astype(str).eq(key)]
        selected = audit.loc[audit[group].astype(str).eq(key)]
        trades = closed.loc[closed[group].astype(str).eq(key)]
        confirmed = _numbers(selected, "confirmation_minute").notna()
        metrics = _trade_metrics(trades)
        records.append(
            {
                group: key,
                "raw_base_5m_candidates": len(raw),
                "post_overlay_selected": len(selected),
                "one_minute_confirmed": int(confirmed.sum()),
                "confirmation_rate_pct": (
                    float(confirmed.mean() * 100.0) if len(selected) else None
                ),
                "fill_rate_pct": (
                    float(len(trades) / len(selected) * 100.0)
                    if len(selected)
                    else None
                ),
                **metrics,
            }
        )
    return pd.DataFrame(records)


def _period_funnel(
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
    sessions: Sequence[date],
    label: str,
) -> dict[str, Any]:
    selected_days = set(sessions)
    raw = decisions.loc[_days(decisions).isin(selected_days)]
    chosen = audit.loc[_days(audit).isin(selected_days)]
    trades = closed.loc[_days(closed).isin(selected_days)]
    return {
        "period": label,
        "sessions": len(sessions),
        "raw_base_5m_candidates": len(raw),
        "post_overlay_selected": len(chosen),
        "one_minute_confirmed": int(
            _numbers(chosen, "confirmation_minute").notna().sum()
        ),
        **_trade_metrics(trades),
    }


def _calendar_group_table(
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
    sessions: Sequence[date],
    labels: Mapping[date, str],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    ordered_labels = list(dict.fromkeys(labels[session] for session in sessions))
    for label in ordered_labels:
        selected_days = [session for session in sessions if labels[session] == label]
        records.append(
            _period_funnel(decisions, audit, closed, selected_days, label)
        )
    return pd.DataFrame(records)


def _binned_funnel(
    audit: pd.DataFrame,
    *,
    indicator: str,
    bins: Sequence[float],
    labels: Sequence[str],
) -> pd.DataFrame:
    work = audit.copy()
    work["indicator_bin"] = pd.cut(
        _numbers(work, indicator),
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    )
    records: list[dict[str, Any]] = []
    for label in labels:
        part = work.loc[work["indicator_bin"].astype(str).eq(label)]
        if part.empty:
            continue
        confirmed = _numbers(part, "confirmation_minute").notna()
        filled = _bools(part, "filled")
        trades = part.loc[filled]
        records.append(
            {
                "indicator": indicator,
                "bin": label,
                "selected": len(part),
                "confirmed": int(confirmed.sum()),
                "fills": int(filled.sum()),
                "confirmation_rate_pct": float(confirmed.mean() * 100.0),
                "fill_rate_pct": float(filled.mean() * 100.0),
                **{
                    key: value
                    for key, value in _trade_metrics(trades).items()
                    if key
                    in {
                        "wins",
                        "losses",
                        "win_rate_pct",
                        "profit_factor",
                        "net_return_points",
                        "net_pnl_rs",
                        "average_return_points",
                    }
                },
            }
        )
    return pd.DataFrame(records)


def _indicator_cohorts(audit: pd.DataFrame) -> pd.DataFrame:
    confirmed = _numbers(audit, "confirmation_minute").notna()
    filled = _bools(audit, "filled")
    returns = _numbers(audit, "net_return_pct")
    cohorts = {
        "ALL_SELECTED": pd.Series(True, index=audit.index),
        "CONFIRMED": confirmed,
        "FILLED": filled,
        "WINNERS": filled & returns.gt(0),
        "LOSERS": filled & returns.lt(0),
    }
    indicators = [
        "directional_move_pct",
        "oi_change_pct",
        "volume_ratio",
        "traded_value_cr",
        "five_min_range_pct",
        "five_min_body_ratio",
        "five_min_adverse_wick_ratio",
        "five_min_directional_close_location",
        "ema_fast_gap_pct",
        "ema_slow_gap_pct",
        "ema_total_gap_pct",
        "confirmation_body_ratio",
        "confirmation_adverse_wick_ratio",
        "confirmation_close_location",
        "trigger_distance_c5_bps",
    ]
    records: list[dict[str, Any]] = []
    for indicator in indicators:
        for cohort, mask in cohorts.items():
            values = _numbers(audit.loc[mask], indicator).dropna()
            if values.empty:
                continue
            records.append(
                {
                    "indicator": indicator,
                    "cohort": cohort,
                    "observations": len(values),
                    "mean": float(values.mean()),
                    "median": float(values.median()),
                    "p25": float(values.quantile(0.25)),
                    "p75": float(values.quantile(0.75)),
                }
            )
    return pd.DataFrame(records)


def _indicator_correlations(audit: pd.DataFrame) -> pd.DataFrame:
    filled = audit.loc[_bools(audit, "filled")].copy()
    indicators = [
        "directional_move_pct",
        "oi_change_pct",
        "volume_ratio",
        "traded_value_cr",
        "five_min_range_pct",
        "five_min_body_ratio",
        "five_min_adverse_wick_ratio",
        "five_min_directional_close_location",
        "ema_fast_gap_pct",
        "ema_slow_gap_pct",
        "ema_total_gap_pct",
        "confirmation_body_ratio",
        "confirmation_adverse_wick_ratio",
        "confirmation_close_location",
        "trigger_distance_c5_bps",
        "confirmation_minute",
        "entry_minute",
        "holding_minutes",
    ]
    records: list[dict[str, Any]] = []
    for indicator in indicators:
        pair = pd.DataFrame(
            {
                "x": _numbers(filled, indicator),
                "y": _numbers(filled, "net_return_pct"),
            }
        ).dropna()
        if len(pair) < 3:
            continue
        records.append(
            {
                "indicator": indicator,
                "observations": len(pair),
                "spearman_vs_net_return": float(
                    pair["x"].corr(pair["y"], method="spearman")
                ),
            }
        )
    return pd.DataFrame(records).sort_values(
        "spearman_vs_net_return", key=lambda values: values.abs(), ascending=False
    )


def _rolling_table(
    daywise: pd.DataFrame, closed: pd.DataFrame, window: int = 10
) -> pd.DataFrame:
    days = daywise["session_date"].tolist()
    records: list[dict[str, Any]] = []
    closed_days = _days(closed)
    for index in range(window - 1, len(days)):
        selected_days = days[index - window + 1 : index + 1]
        trades = closed.loc[closed_days.isin(set(selected_days))]
        records.append(
            {
                "from_day": selected_days[0],
                "through_day": selected_days[-1],
                "sessions": window,
                **_trade_metrics(trades),
            }
        )
    return pd.DataFrame(records)


def _stability_blocks(
    decisions: pd.DataFrame,
    audit: pd.DataFrame,
    closed: pd.DataFrame,
    sessions: Sequence[date],
    block_size: int = 10,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for start in range(0, len(sessions), block_size):
        block = list(sessions[start : start + block_size])
        label = f"B{start // block_size + 1}: {block[0]}..{block[-1]}"
        records.append(_period_funnel(decisions, audit, closed, block, label))
    return pd.DataFrame(records)


def _fmt(value: object, column: str) -> str:
    if value is None or value is pd.NA:
        return "—"
    if isinstance(value, float) and not math.isfinite(value):
        return "∞" if math.isinf(value) and value > 0 else "—"
    if pd.isna(value):
        return "—"
    text_columns = {
        "period",
        "scenario",
        "setup_id",
        "side",
        "signal_end",
        "picker",
        "month",
        "week",
        "weekday",
        "indicator",
        "bin",
        "cohort",
        "exit_reason",
        "rank_bucket",
        "holding_bin",
        "gap_group",
        "symbol",
        "from_day",
        "through_day",
        "selection_reason",
        "status",
        "reason",
        "profile",
    }
    if column in text_columns or isinstance(value, (str, date, pd.Timestamp)):
        return str(value).replace("|", "\\|")
    count_words = (
        "sessions",
        "candidates",
        "selected",
        "confirmed",
        "fills",
        "wins",
        "losses",
        "flat",
        "observations",
        "count",
        "rejections",
        "occurrences",
        "targets",
        "stops",
    )
    if any(word in column for word in count_words):
        return f"{int(float(value)):,}"
    if column.endswith("_pct") or "rate_pct" in column:
        return f"{float(value):.2f}%"
    if "pnl_rs" in column or column.endswith("_rs"):
        return f"{float(value):+,.2f}"
    if "return_points" in column or column.endswith("_points"):
        return f"{float(value):+.4f}"
    if "profit_factor" in column or column == "payoff_ratio":
        return f"{float(value):.4f}"
    if "spearman" in column:
        return f"{float(value):+.3f}"
    return f"{float(value):.3f}" if isinstance(value, (float, np.floating)) else str(value)


def _table(frame: pd.DataFrame, columns: Sequence[str]) -> list[str]:
    if frame.empty:
        return ["_No rows._"]
    headers = [column.replace("_", " ").title() for column in columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in frame.to_dict("records"):
        lines.append(
            "| " + " | ".join(_fmt(row.get(column), column) for column in columns) + " |"
        )
    return lines


def _plot_assets(
    assets_dir: Path,
    daywise: pd.DataFrame,
    setup_metrics: pd.DataFrame,
    monthly: pd.DataFrame,
    funnel: Mapping[str, int],
) -> list[Path]:
    plt.style.use("seaborn-v0_8-whitegrid")
    paths: list[Path] = []

    equity_path = assets_dir / "equity_and_drawdown.png"
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    axes[0].plot(daywise["session_date"], daywise["cumulative_net_pnl_rs"], color="#1769aa", linewidth=2)
    axes[0].axhline(0, color="#555555", linewidth=0.8)
    axes[0].set_ylabel("Cumulative net P&L (Rs)")
    axes[0].set_title("V10 current-mixed: diagnostic cumulative P&L")
    axes[1].fill_between(
        daywise["session_date"],
        daywise["drawdown_pnl_rs"],
        0,
        color="#c62828",
        alpha=0.35,
    )
    axes[1].set_ylabel("Drawdown (Rs)")
    axes[1].set_xlabel("Session")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(equity_path, dpi=160)
    plt.close(fig)
    paths.append(equity_path)

    setup_path = assets_dir / "setup_net_pnl.png"
    ordered = setup_metrics.sort_values("net_pnl_rs")
    colors = ["#2e7d32" if value >= 0 else "#c62828" for value in ordered["net_pnl_rs"]]
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(ordered["setup_id"], ordered["net_pnl_rs"], color=colors)
    ax.axvline(0, color="#333333", linewidth=0.8)
    ax.set_xlabel("Net P&L (Rs)")
    ax.set_title("Contribution by setup")
    for bar, fills in zip(bars, ordered["fills"], strict=True):
        value = bar.get_width()
        ax.text(value, bar.get_y() + bar.get_height() / 2, f" {int(fills)} fills", va="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(setup_path, dpi=160)
    plt.close(fig)
    paths.append(setup_path)

    monthly_path = assets_dir / "monthly_net_pnl.png"
    colors = ["#2e7d32" if value >= 0 else "#c62828" for value in monthly["net_pnl_rs"]]
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(monthly["period"], monthly["net_pnl_rs"], color=colors)
    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.set_ylabel("Net P&L (Rs)")
    ax.set_title("Monthly stability (partial May and August windows)")
    for bar, fills in zip(bars, monthly["fills"], strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{int(fills)} fills", ha="center", va="bottom" if bar.get_height() >= 0 else "top", fontsize=8)
    fig.tight_layout()
    fig.savefig(monthly_path, dpi=160)
    plt.close(fig)
    paths.append(monthly_path)

    funnel_path = assets_dir / "selection_funnel.png"
    labels = list(funnel)
    values = [funnel[label] for label in labels]
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, values, color=["#455a64", "#1976d2", "#7b1fa2", "#ef6c00", "#2e7d32"])
    ax.set_ylabel("Candidates / trades")
    ax.set_title("Selection and entry funnel")
    ax.tick_params(axis="x", rotation=20)
    for bar, value in zip(bars, values, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, value, str(value), ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(funnel_path, dpi=160)
    plt.close(fig)
    paths.append(funnel_path)
    return paths


def build_report(
    *,
    source_run: Path,
    stress_run: Path | None,
    report_path: Path,
    assets_dir: Path,
) -> dict[str, Any]:
    source_run = source_run.expanduser().resolve()
    report_path = report_path.expanduser().resolve()
    assets_dir = assets_dir.expanduser().resolve()
    assets_dir.mkdir(parents=True, exist_ok=True)
    provenance_path = source_run / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    if provenance.get("schema_version") != "fno_v10_max050_gap2_full_history_v1":
        raise ValueError("Unexpected source-run schema")
    paths = {
        "decisions": source_run / "selection_decisions.csv",
        "selected": source_run / "selected_candidates.csv",
        "audit": source_run / "scenarios" / "reference_15_0" / "candidate_order_audit.csv",
        "closed": source_run / "scenarios" / "reference_15_0" / "closed_trades.csv",
        "daywise": source_run / "scenarios" / "reference_15_0" / "daywise.csv",
        "metrics": source_run / "all_period_metrics.csv",
        "benchmark": source_run / "current_mixed_benchmark_verification.json",
    }
    for path in paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    decisions = pd.read_csv(paths["decisions"], low_memory=False)
    selected = pd.read_csv(paths["selected"], low_memory=False)
    audit = pd.read_csv(paths["audit"], low_memory=False)
    closed = pd.read_csv(paths["closed"], low_memory=False)
    daywise = pd.read_csv(paths["daywise"], low_memory=False)
    recent._validate_source_tables(decisions, selected, audit, closed)
    benchmark = json.loads(paths["benchmark"].read_text(encoding="utf-8"))
    if not bool(benchmark.get("verified")):
        raise AssertionError("Pinned benchmark verification is not true")

    audit = _add_features(audit)
    closed = audit.loc[_bools(audit, "filled")].copy()
    sessions = sorted(set(_days(daywise)))
    expected_span = engine.expected_regular_session_dates(min(sessions), max(sessions))
    missing_sessions = sorted(set(expected_span) - set(sessions))
    daywise["session_date"] = pd.to_datetime(daywise["session_date"], errors="raise").dt.date
    daywise["net_return_pct"] = _numbers(daywise, "net_return_pct")
    daywise["net_pnl_rs"] = _numbers(daywise, "net_pnl_rs")
    daywise["cumulative_net_return_points"] = daywise["net_return_pct"].cumsum()
    daywise["cumulative_net_pnl_rs"] = daywise["net_pnl_rs"].cumsum()
    daywise["drawdown_return_points"] = (
        daywise["cumulative_net_return_points"]
        - daywise["cumulative_net_return_points"].cummax().clip(lower=0)
    )
    daywise["drawdown_pnl_rs"] = (
        daywise["cumulative_net_pnl_rs"]
        - daywise["cumulative_net_pnl_rs"].cummax().clip(lower=0)
    )
    daywise["month"] = pd.to_datetime(daywise["session_date"]).dt.strftime("%Y-%m")
    daywise["weekday"] = pd.to_datetime(daywise["session_date"]).dt.day_name()
    iso = pd.to_datetime(daywise["session_date"]).dt.isocalendar()
    daywise["week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)

    full_metrics = _trade_metrics(closed)
    benchmark_expected = dict(benchmark["expected"])
    for field in ("fills", "wins", "losses", "profit_factor", "net_return_points", "net_pnl_rs"):
        observed = full_metrics[field]
        expected = benchmark_expected[field]
        if isinstance(expected, (int, float)) and not math.isclose(
            float(observed), float(expected), rel_tol=0.0, abs_tol=1e-9
        ):
            raise AssertionError(f"Report metric drift: {field}")

    setup_parameters = recent._setup_parameter_table()
    setup_metrics = _funnel_by(decisions, audit, closed, "setup_id")
    setup_metrics = setup_metrics.merge(
        setup_parameters[["setup_id", "max_entries", "picker"]],
        on="setup_id",
        how="left",
    ).sort_values("setup_id", kind="stable").reset_index(drop=True)
    side_metrics = _funnel_by(decisions, audit, closed, "side")
    slot_metrics = _funnel_by(decisions, audit, closed, "signal_end").sort_values(
        "signal_end", kind="stable"
    ).reset_index(drop=True)
    picker_metrics = _funnel_by(decisions, audit, closed, "picker")

    month_labels = {session: session.strftime("%Y-%m") for session in sessions}
    monthly = _calendar_group_table(decisions, audit, closed, sessions, month_labels)
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    weekday_labels = {session: session.strftime("%A") for session in sessions}
    weekday = _calendar_group_table(decisions, audit, closed, sessions, weekday_labels)
    weekday["order"] = weekday["period"].map({value: index for index, value in enumerate(weekday_order)})
    weekday = weekday.sort_values("order").drop(columns="order")
    week_labels = {
        session: f"{session.isocalendar().year}-W{session.isocalendar().week:02d}"
        for session in sessions
    }
    weekly = _calendar_group_table(decisions, audit, closed, sessions, week_labels)
    blocks = _stability_blocks(decisions, audit, closed, sessions)
    rolling = _rolling_table(daywise, closed, 10)

    core_sessions = [session for session in sessions if session < date(2026, 8, 20)]
    forward_sessions = [session for session in sessions if session >= date(2026, 8, 20)]
    first_half = sessions[: len(sessions) // 2]
    second_half = sessions[len(sessions) // 2 :]
    period_comparison = pd.DataFrame(
        [
            _period_funnel(decisions, audit, closed, sessions, "FULL_65"),
            _period_funnel(decisions, audit, closed, core_sessions, "CORE_59"),
            _period_funnel(decisions, audit, closed, forward_sessions, "FORWARD_6"),
            _period_funnel(decisions, audit, closed, first_half, "FIRST_HALF_32"),
            _period_funnel(decisions, audit, closed, second_half, "SECOND_HALF_33"),
            _period_funnel(decisions, audit, closed, sessions[-14:], "LAST_14_USABLE"),
        ]
    )

    rank = _numbers(audit, "frozen_rank")
    audit["rank_bucket"] = np.select(
        [rank.eq(1), rank.eq(2), rank.eq(3), rank.eq(4)],
        ["1", "2", "3", "4"],
        default="5+",
    )
    closed_rank = _numbers(closed, "frozen_rank")
    closed["rank_bucket"] = np.select(
        [
            closed_rank.eq(1),
            closed_rank.eq(2),
            closed_rank.eq(3),
            closed_rank.eq(4),
        ],
        ["1", "2", "3", "4"],
        default="5+",
    )
    rank_metrics = _group_metrics(closed, "rank_bucket")
    rank_funnel = (
        audit.groupby("rank_bucket", sort=False)
        .agg(
            selected=("candidate_id", "size"),
            confirmed=("confirmation_minute", lambda values: pd.to_numeric(values, errors="coerce").notna().sum()),
            fills=("filled", lambda values: values.astype(str).str.lower().eq("true").sum()),
        )
        .reset_index()
        .merge(rank_metrics, on=["rank_bucket", "fills"], how="left")
    )

    closed["exit_reason"] = closed["exit_reason"].astype(str)
    exit_metrics = _group_metrics(closed, "exit_reason")
    closed["gap_group"] = np.where(_bools(closed, "gap_fill"), "GAP_FILL", "TRIGGER_TOUCH")
    gap_metrics = _group_metrics(closed, "gap_group")
    closed["holding_bin"] = pd.cut(
        _numbers(closed, "holding_minutes"),
        [-math.inf, 15, 30, 60, 120, math.inf],
        labels=["<15", "15-30", "30-60", "60-120", "120+"],
        right=False,
    ).astype(str)
    holding_metrics = _group_metrics(closed, "holding_bin")
    confirmation_metrics = _group_metrics(
        closed.assign(
            confirmation_minute=_numbers(closed, "confirmation_minute").astype("Int64").astype(str)
        ),
        "confirmation_minute",
    )
    entry_metrics = _group_metrics(
        closed.assign(entry_minute=_numbers(closed, "entry_minute").astype("Int64").astype(str)),
        "entry_minute",
    ).sort_values("entry_minute", kind="stable").reset_index(drop=True)

    checks = recent._confirmation_detail(audit)
    rejection_codes: list[str] = []
    for value in checks.get("rejection_codes", pd.Series(dtype=object)).fillna(""):
        rejection_codes.extend(part for part in str(value).split(" | ") if part)
    check_rejections = pd.Series(rejection_codes).value_counts().rename_axis("reason").reset_index(name="count")
    status_counts = audit["status"].value_counts().rename_axis("status").reset_index(name="count")
    overlay_rejections = (
        decisions.loc[~_bools(decisions, "selection_passed")]
        .groupby("selection_reason", as_index=False)
        .agg(
            rejections=("candidate_id", "size"),
            affected_sessions=("session_date", "nunique"),
            median_price_change_pct=("price_change_pct", "median"),
        )
    )

    bins = [
        _binned_funnel(audit, indicator="directional_move_pct", bins=[0, 0.3, 0.5, 0.75, 1.0, 1.5, math.inf], labels=["<0.30", "0.30-0.50", "0.50-0.75", "0.75-1.00", "1.00-1.50", "1.50+"]),
        _binned_funnel(audit, indicator="oi_change_pct", bins=[0, 0.5, 1, 2, 5, math.inf], labels=["<0.50", "0.50-1.00", "1.00-2.00", "2.00-5.00", "5.00+"]),
        _binned_funnel(audit, indicator="volume_ratio", bins=[0, 1.5, 2, 3, 5, math.inf], labels=["<1.50", "1.50-2.00", "2.00-3.00", "3.00-5.00", "5.00+"]),
        _binned_funnel(audit, indicator="traded_value_cr", bins=[0, 2.5, 5, 10, 25, math.inf], labels=["<2.5cr", "2.5-5cr", "5-10cr", "10-25cr", "25cr+"]),
        _binned_funnel(audit, indicator="five_min_range_pct", bins=[0, 0.3, 0.5, 0.75, 1.0, 1.5, math.inf], labels=["<0.30", "0.30-0.50", "0.50-0.75", "0.75-1.00", "1.00-1.50", "1.50+"]),
        _binned_funnel(audit, indicator="ema_total_gap_pct", bins=[0, 0.1, 0.25, 0.5, 1, math.inf], labels=["<0.10", "0.10-0.25", "0.25-0.50", "0.50-1.00", "1.00+"]),
        _binned_funnel(audit, indicator="confirmation_body_ratio", bins=[0, 0.4, 0.5, 0.6, 0.75, math.inf], labels=["<0.40", "0.40-0.50", "0.50-0.60", "0.60-0.75", "0.75+"]),
        _binned_funnel(audit, indicator="confirmation_adverse_wick_ratio", bins=[0, 0.1, 0.2, 0.3, 0.4, 0.5, math.inf], labels=["<0.10", "0.10-0.20", "0.20-0.30", "0.30-0.40", "0.40-0.50", "0.50+"]),
        _binned_funnel(audit, indicator="confirmation_close_location", bins=[0, 0.5, 0.6, 0.75, 0.9, math.inf], labels=["<0.50", "0.50-0.60", "0.60-0.75", "0.75-0.90", "0.90+"]),
        _binned_funnel(audit, indicator="trigger_distance_c5_bps", bins=[-math.inf, 0, 10, 20, 30, 50, math.inf], labels=["<0", "0-10", "10-20", "20-30", "30-50", "50+"]),
    ]
    indicator_bins = pd.concat([frame for frame in bins if not frame.empty], ignore_index=True)
    indicator_cohorts = _indicator_cohorts(audit)
    correlations = _indicator_correlations(audit)

    symbol_metrics = _group_metrics(closed, "symbol").sort_values("net_pnl_rs", ascending=False)
    unique_symbols = int(closed["symbol"].nunique())
    top_symbols = symbol_metrics.head(15)
    bottom_symbols = symbol_metrics.sort_values("net_pnl_rs").head(15)
    absolute_total = float(symbol_metrics["net_pnl_rs"].abs().sum())
    top10_abs_share = (
        float(symbol_metrics.assign(abs_pnl=symbol_metrics["net_pnl_rs"].abs()).nlargest(10, "abs_pnl")["abs_pnl"].sum() / absolute_total * 100.0)
        if absolute_total
        else None
    )
    trade_order = closed.sort_values(["entry_time", "candidate_id"], kind="stable")
    outcomes = np.select(
        [_numbers(trade_order, "net_return_pct").gt(0), _numbers(trade_order, "net_return_pct").lt(0)],
        [1, -1],
        default=0,
    )
    best_trades = closed.nlargest(10, "net_pnl_rs")
    worst_trades = closed.nsmallest(10, "net_pnl_rs")
    top_bottom = pd.concat(
        [best_trades.assign(extreme="TOP_10"), worst_trades.assign(extreme="BOTTOM_10")],
        ignore_index=True,
    )

    daily_values = daywise["net_pnl_rs"]
    daily_signs = np.sign(daily_values).astype(int).tolist()
    max_daily_drawdown_rs = float(-daywise["drawdown_pnl_rs"].min())
    total_cost_rs = float(_numbers(closed, "estimated_cost_rs").sum())
    total_notional_rs = float(_numbers(closed, "position_notional_rs").sum())
    extra_break_even_bps = (
        full_metrics["net_pnl_rs"] / total_notional_rs * 10_000.0
        if total_notional_rs
        else None
    )
    risk_summary = {
        "best_day": daywise.loc[daywise["net_pnl_rs"].idxmax(), "session_date"],
        "best_day_pnl_rs": float(daywise["net_pnl_rs"].max()),
        "worst_day": daywise.loc[daywise["net_pnl_rs"].idxmin(), "session_date"],
        "worst_day_pnl_rs": float(daywise["net_pnl_rs"].min()),
        "average_daily_pnl_rs": float(daily_values.mean()),
        "median_daily_pnl_rs": float(daily_values.median()),
        "daily_pnl_std_rs": float(daily_values.std(ddof=1)),
        "positive_days": int(daily_values.gt(0).sum()),
        "negative_days": int(daily_values.lt(0).sum()),
        "flat_days": int(daily_values.eq(0).sum()),
        "max_consecutive_positive_days": _streak(daily_signs, 1),
        "max_consecutive_negative_days": _streak(daily_signs, -1),
        "max_consecutive_winning_trades": _streak(outcomes, 1),
        "max_consecutive_losing_trades": _streak(outcomes, -1),
        "max_daily_drawdown_points": float(-daywise["drawdown_return_points"].min()),
        "max_daily_drawdown_rs": max_daily_drawdown_rs,
        "recovery_factor_pnl": _safe_div(full_metrics["net_pnl_rs"], max_daily_drawdown_rs),
    }

    excursion = {
        "ambiguous_entry_bars": int(_bools(closed, "ambiguous_entry_bar").sum()),
        "ambiguous_excursion_boundaries": int(_bools(closed, "excursion_boundary_ambiguous").sum()),
        "median_mfe_lower_pct": float(_numbers(closed, "mfe_pct_ohlc_lower_bound").median()),
        "median_mfe_upper_pct": float(_numbers(closed, "mfe_pct_ohlc_upper_bound").median()),
        "median_mae_lower_pct": float(_numbers(closed, "mae_pct_ohlc_lower_bound").median()),
        "median_mae_upper_pct": float(_numbers(closed, "mae_pct_ohlc_upper_bound").median()),
        "median_holding_minutes": float(_numbers(closed, "holding_minutes").median()),
        "average_holding_minutes": float(_numbers(closed, "holding_minutes").mean()),
    }

    stress_metrics = pd.DataFrame()
    if stress_run is not None:
        stress_path = stress_run.expanduser().resolve() / "all_period_metrics.csv"
        stress_all = pd.read_csv(stress_path)
        stress_metrics = stress_all.loc[
            stress_all["period"].isin(["FULL_USABLE", "CORE_59", "FORWARD_EXTENSION"])
        ].copy()
        reference_full = stress_metrics.loc[
            stress_metrics["period"].eq("FULL_USABLE")
            & stress_metrics["scenario"].eq("REFERENCE_15_0")
        ].iloc[0]
        if not math.isclose(
            float(reference_full["net_pnl_rs"]),
            float(full_metrics["net_pnl_rs"]),
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise AssertionError("Stress-run reference does not match source run")

    funnel = {
        "Base 5m": len(decisions),
        "Post-overlay": len(audit),
        "1m confirmed": int(_numbers(audit, "confirmation_minute").notna().sum()),
        "Filled": len(closed),
        "Winners": full_metrics["wins"],
    }
    chart_paths = _plot_assets(assets_dir, daywise, setup_metrics, monthly, funnel)

    daily_detail_records: list[dict[str, Any]] = []
    for session in sessions:
        record = _period_funnel(
            decisions, audit, closed, [session], session.isoformat()
        )
        record["session_date"] = session
        daily_detail_records.append(record)
    daily_detail = pd.DataFrame(daily_detail_records).drop(columns="period")
    daily_detail = daily_detail.merge(
        daywise[
            [
                "session_date",
                "cumulative_net_return_points",
                "cumulative_net_pnl_rs",
                "drawdown_return_points",
                "drawdown_pnl_rs",
            ]
        ],
        on="session_date",
        how="left",
        validate="one_to_one",
    )

    asset_frames: dict[str, pd.DataFrame] = {
        "daily_performance.csv": daily_detail,
        "period_comparison.csv": period_comparison,
        "monthly_performance.csv": monthly,
        "weekly_performance.csv": weekly,
        "weekday_performance.csv": weekday,
        "ten_session_blocks.csv": blocks,
        "rolling_10_session_metrics.csv": rolling,
        "setup_metrics.csv": setup_metrics,
        "side_metrics.csv": side_metrics,
        "slot_metrics.csv": slot_metrics,
        "picker_metrics.csv": picker_metrics,
        "rank_metrics.csv": rank_funnel,
        "exit_reason_metrics.csv": exit_metrics,
        "holding_period_metrics.csv": holding_metrics,
        "gap_fill_metrics.csv": gap_metrics,
        "confirmation_minute_metrics.csv": confirmation_metrics,
        "entry_minute_metrics.csv": entry_metrics,
        "selection_overlay_rejections.csv": overlay_rejections,
        "one_minute_rejection_codes.csv": check_rejections,
        "candidate_status_counts.csv": status_counts,
        "indicator_bins.csv": indicator_bins,
        "indicator_cohort_summary.csv": indicator_cohorts,
        "indicator_correlations.csv": correlations,
        "symbol_metrics.csv": symbol_metrics,
        "top_bottom_trades.csv": top_bottom,
        "setup_parameter_reference.csv": setup_parameters,
        "cost_sensitivity.csv": stress_metrics,
    }
    for name, frame in asset_frames.items():
        common.atomic_write_csv(frame, assets_dir / name)

    source_incomplete_total = sum(
        int(segment["source_incomplete_symbol_sessions"])
        for segment in provenance.get("source_segments", [])
    )
    source_expected_total = sum(
        int(segment["expected_symbol_sessions"])
        for segment in provenance.get("source_segments", [])
    )
    source_incomplete_pct = (
        source_incomplete_total / source_expected_total * 100.0
        if source_expected_total
        else 0.0
    )

    lines: list[str] = []
    add = lines.extend
    lines.extend(
        [
            "# V10 FNO Backtesting — full historical study report",
            "",
            f"Generated: {datetime.now(gaps.IST).isoformat()}",
            f"Verified source run: `{source_run}`",
            "Profile: `V10 Stage 7 + 09:35 LONG <= 0.50% + Gap2`, current mixed per-setup limits.",
            "",
            "> This is an auditable research report. It does not convert a diagnostic backtest into live-proof. "
            "The exact run marks itself `research_only=true`, `headline_valid=false`, and `promotion_eligible=false`.",
            "",
            "## Executive conclusion",
            "",
            f"Across {len(sessions)} usable sessions, the model produced **{full_metrics['fills']} fills, "
            f"{full_metrics['wins']}-{full_metrics['losses']}, WR {full_metrics['win_rate_pct']:.2f}%, "
            f"PF {full_metrics['profit_factor']:.4f}, {full_metrics['net_return_points']:+.4f} summed trade-return "
            f"points and Rs {full_metrics['net_pnl_rs']:+,.2f} modeled net P&L**.",
            "",
            f"The result is profitable under all three modeled cost cases, but stability weakened in the six-session "
            f"forward extension (PF {period_comparison.loc[period_comparison['period'].eq('FORWARD_6'), 'profit_factor'].iloc[0]:.4f}) "
            "and the strategy was developed through repeated testing on much of the same history. The correct next step is "
            "frozen forward validation and execution-data repair—not immediate parameter optimization on these 65 sessions.",
            "",
            "## 1. Data contract, scope and honesty checks",
            "",
            f"- Usable sessions: **{len(sessions)}**, {sessions[0]} through {sessions[-1]}.",
            f"- Expected regular sessions in the span: **{len(expected_span)}**.",
            f"- Missing validated sessions: **{', '.join(map(str, missing_sessions)) or 'none'}**.",
            f"- Base-qualified 5-minute candidates before overlays: **{len(decisions):,}**.",
            f"- Candidates after Stage 7 and `.50` overlays: **{len(audit):,}**.",
            f"- Completed fills with finite economics: **{len(closed):,}**.",
            "- Candidate/path input files, output artifacts and the pinned benchmark are SHA-256 bound.",
            "- The candidate cache retains candidates that passed each base 5-minute setup; it does not retain one row "
            "for every universe symbol that failed base eligibility.",
            "- Strict upstream completeness failed for many symbol-sessions; the run uses `LAST_REAL_BAR_SENSITIVITY`.",
            f"  Under the engine's strict full-session definition, {source_incomplete_total:,} of "
            f"{source_expected_total:,} symbol-sessions ({source_incomplete_pct:.2f}%) were incomplete. This does "
            "not mean every bar was absent; it means the symbol-session failed the complete 09:16–15:30 cash path "
            "plus required futures-OI checks.",
            "- Futures OI drives selection, while entry/exit prices are underlying NSE cash 1-minute bars with `lot_size=1`. "
            "Therefore P&L is Rs 50,000 cash-equivalent notional per fill, not actual futures-lot P&L.",
            "",
            "### Source segments",
            "",
        ]
    )
    source_rows = []
    for segment in provenance.get("source_segments", []):
        payload = dict(segment["segment"])
        source_rows.append(
            {
                "period": payload["segment_id"],
                "sessions": segment["session_count"],
                "raw_base_5m_candidates": segment["candidate_count"],
                "contract": payload["contract_month_filter"],
                "source_incomplete_symbol_sessions": segment["source_incomplete_symbol_sessions"],
                "expected_symbol_sessions": segment["expected_symbol_sessions"],
            }
        )
    source_frame = pd.DataFrame(source_rows)
    add(_table(source_frame, ["period", "sessions", "contract", "raw_base_5m_candidates", "source_incomplete_symbol_sessions", "expected_symbol_sessions"]))

    add(["", "## 2. Exact strategy parameters", "", "### 5-minute selection and 1-minute entry book", ""])
    parameter_display = setup_parameters.copy()
    parameter_display["base_move_rule"] = np.where(
        parameter_display["side"].eq("LONG"),
        ">= +" + parameter_display["price_change_pct"].astype(str) + "%",
        "<= -" + parameter_display["price_change_pct"].astype(str) + "%",
    )
    parameter_display["effective_move_rule"] = parameter_display[
        "base_move_rule"
    ]
    parameter_display.loc[
        parameter_display["setup_id"].eq("09:40_LONG"), "effective_move_rule"
    ] = ">= +0.40%"
    parameter_display.loc[
        parameter_display["setup_id"].eq("09:35_LONG"), "effective_move_rule"
    ] = "+0.20% to +0.50%"
    parameter_display["min_traded_value_cr"] = (
        pd.to_numeric(parameter_display["min_traded_value"], errors="coerce")
        / 10_000_000.0
    )
    add(
        _table(
            parameter_display,
            [
                "setup_id",
                "max_entries",
                "picker",
                "base_move_rule",
                "effective_move_rule",
                "oi_change_pct",
                "volume_ratio",
                "min_traded_value_cr",
                "body_ratio",
                "max_wick_ratio",
                "effective_max_confirmation_minute",
                "effective_buffer_bps",
                "stop_pct",
                "target_pct",
            ],
        )
    )
    add(
        [
            "",
            "Parameter interpretation:",
            "",
            "- Every 5-minute LONG requires `EMA9 > EMA20 > EMA50`; every SHORT requires the reverse.",
            "- `body_ratio` and `max_wick_ratio` are 1-minute confirmation gates, not 5-minute filters.",
            "- Stage 7 changes 09:40 LONG to a minimum +0.40% move.",
            "- `.50` imposes a maximum +0.50% move on 09:35 LONG, after which candidates are reranked.",
            "- 09:30 SHORT additionally uses midpoint invalidation and close-location >= 0.50.",
            "- Confirmed stop orders can fill only on a later bar through S+5. Gap2 rejects an adverse gap through "
            "the trigger greater than 2 bps.",
            "- Portfolio: Rs 120,000 modeled capital, Rs 10,000 reserved margin per pending/open entry, maximum 12 "
            "concurrent positions, pending orders reserve margin, one concurrent position per symbol.",
            "- Economics: Rs 50,000 modeled exposure per fill; 15 bps total cost in reference; zero slippage; stop-first "
            "when both stop and target are touched in one 1-minute candle.",
            "",
            "## 3. Selection and entry funnel",
            "",
            "![Selection funnel](report_v10_assets/selection_funnel.png)",
            "",
        ]
    )
    funnel_frame = pd.DataFrame([{"stage": key, "count": value} for key, value in funnel.items()])
    add(_table(funnel_frame, ["stage", "count"]))
    add(["", "### Post-selection overlay rejections", ""])
    add(_table(overlay_rejections, ["selection_reason", "rejections", "affected_sessions", "median_price_change_pct"]))
    add(["", "### Final candidate states", ""])
    add(_table(status_counts, ["status", "count"]))
    add(["", "### 1-minute rejection-code occurrences", ""])
    add(_table(check_rejections, ["reason", "count"]))
    add(
        [
            "",
            "A confirmation candle can contain multiple rejection codes, so rejection-code counts do not sum to candidates.",
            "",
            "## 4. Headline performance and risk",
            "",
        ]
    )
    headline = pd.DataFrame([{**full_metrics, **risk_summary}])
    add(
        _table(
            headline,
            [
                "fills",
                "wins",
                "losses",
                "win_rate_pct",
                "profit_factor",
                "net_return_points",
                "net_pnl_rs",
                "average_return_points",
                "average_pnl_rs",
                "payoff_ratio",
                "max_daily_drawdown_points",
                "max_daily_drawdown_rs",
                "recovery_factor_pnl",
            ],
        )
    )
    add(
        [
            "",
            f"- Gross winning points: **{full_metrics['gross_profit_points']:.4f}**; gross losing points: "
            f"**{full_metrics['gross_loss_points']:.4f}**.",
            f"- Average win/loss: **{full_metrics['average_win_points']:+.4f} / "
            f"{full_metrics['average_loss_points']:+.4f} points**; payoff ratio **{full_metrics['payoff_ratio']:.4f}**.",
            f"- Best day: **{risk_summary['best_day']} Rs {risk_summary['best_day_pnl_rs']:+,.2f}**; worst day: "
            f"**{risk_summary['worst_day']} Rs {risk_summary['worst_day_pnl_rs']:+,.2f}**.",
            f"- Longest winning/losing trade streaks: **{risk_summary['max_consecutive_winning_trades']} / "
            f"{risk_summary['max_consecutive_losing_trades']}**.",
            f"- Longest positive/negative day streaks: **{risk_summary['max_consecutive_positive_days']} / "
            f"{risk_summary['max_consecutive_negative_days']}**.",
            "- `net_return_points` sums trade percentage returns; it is not portfolio percentage return because positions "
            "can overlap and each fill receives its own Rs 50,000 modeled notional.",
            "",
            "![Equity and drawdown](report_v10_assets/equity_and_drawdown.png)",
            "",
            "## 5. Stability through time",
            "",
            "### Core, forward and half-sample comparison",
            "",
        ]
    )
    add(_table(period_comparison, ["period", "sessions", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "The six-session extension remained profitable but had lower WR/PF. It is too small to establish forward robustness.", "", "### Monthly", ""])
    add(_table(monthly, ["period", "sessions", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "![Monthly P&L](report_v10_assets/monthly_net_pnl.png)", "", "### Weekly", ""])
    add(_table(weekly, ["period", "sessions", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Full 65-session day-wise audit", ""])
    add(_table(daily_detail, ["session_date", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "cumulative_net_pnl_rs", "drawdown_pnl_rs"]))
    add(["", "### Sequential 10-session blocks", ""])
    add(_table(blocks, ["period", "sessions", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Day of week", ""])
    weekday_display = weekday.rename(columns={"period": "weekday"})
    add(_table(weekday_display, ["weekday", "sessions", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    best_rolling = rolling.loc[rolling["net_pnl_rs"].idxmax()]
    worst_rolling = rolling.loc[rolling["net_pnl_rs"].idxmin()]
    add(
        [
            "",
            f"Rolling 10-session best: **{best_rolling['from_day']}..{best_rolling['through_day']}**, "
            f"Rs {best_rolling['net_pnl_rs']:+,.2f}, PF {best_rolling['profit_factor']:.4f}.",
            f"Rolling 10-session worst: **{worst_rolling['from_day']}..{worst_rolling['through_day']}**, "
            f"Rs {worst_rolling['net_pnl_rs']:+,.2f}, PF {worst_rolling['profit_factor']:.4f}.",
            "",
            "## 6. Setup, side, slot, picker and rank",
            "",
            "### Setup contribution",
            "",
        ]
    )
    add(_table(setup_metrics, ["setup_id", "max_entries", "picker", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "confirmation_rate_pct", "fills", "fill_rate_pct", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "![Setup contribution](report_v10_assets/setup_net_pnl.png)", "", "### Side", ""])
    add(_table(side_metrics, ["side", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Signal slot", ""])
    add(_table(slot_metrics, ["signal_end", "raw_base_5m_candidates", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Picker", ""])
    add(_table(picker_metrics, ["picker", "post_overlay_selected", "one_minute_confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Recalculated rank", ""])
    add(_table(rank_funnel, ["rank_bucket", "selected", "confirmed", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))

    add(["", "## 7. Five-minute indicator study", "", "These tables are descriptive on the same tested history. A favorable bin is a hypothesis for a new frozen test, not permission to optimize the current sample.", ""])
    for indicator in ["directional_move_pct", "oi_change_pct", "volume_ratio", "traded_value_cr", "five_min_range_pct", "ema_total_gap_pct"]:
        add([f"### {indicator}", ""])
        part = indicator_bins.loc[indicator_bins["indicator"].eq(indicator)]
        add(_table(part, ["bin", "selected", "confirmed", "fills", "confirmation_rate_pct", "fill_rate_pct", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
        add([""])
    add(["### Winner-versus-loser indicator medians", ""])
    cohort_pivot = indicator_cohorts.loc[indicator_cohorts["cohort"].isin(["WINNERS", "LOSERS"])].pivot(index="indicator", columns="cohort", values="median").reset_index()
    cohort_pivot.columns.name = None
    add(_table(cohort_pivot, ["indicator", "WINNERS", "LOSERS"]))
    add(["", "### Spearman correlation with filled-trade net return", ""])
    add(_table(correlations, ["indicator", "observations", "spearman_vs_net_return"]))
    add(["", "Correlations are univariate, non-causal and affected by setup mix. Values near zero mean the indicator did not order outcomes monotonically in this sample.", ""])

    add(["", "## 8. One-minute confirmation and entry quality", ""])
    for indicator in ["confirmation_body_ratio", "confirmation_adverse_wick_ratio", "confirmation_close_location", "trigger_distance_c5_bps"]:
        add([f"### {indicator}", ""])
        part = indicator_bins.loc[indicator_bins["indicator"].eq(indicator)]
        add(_table(part, ["bin", "selected", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
        add([""])
    add(["### Confirmation minute", ""])
    add(_table(confirmation_metrics, ["confirmation_minute", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Entry minute", ""])
    add(_table(entry_metrics, ["entry_minute", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))

    add(["", "## 9. Exit, holding-time, gaps and OHLC ambiguity", "", "### Exit reason", ""])
    add(_table(exit_metrics, ["exit_reason", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Holding time", ""])
    add(_table(holding_metrics, ["holding_bin", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Gap versus trigger-touch fill", ""])
    add(_table(gap_metrics, ["gap_group", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(
        [
            "",
            f"- Median/average holding: **{excursion['median_holding_minutes']:.1f} / {excursion['average_holding_minutes']:.1f} minutes**.",
            f"- Ambiguous entry bars: **{excursion['ambiguous_entry_bars']}**; ambiguous excursion boundaries: "
            f"**{excursion['ambiguous_excursion_boundaries']}**.",
            f"- Median MFE lower/upper bounds: **{excursion['median_mfe_lower_pct']:.4f}% / "
            f"{excursion['median_mfe_upper_pct']:.4f}%**.",
            f"- Median MAE lower/upper bounds: **{excursion['median_mae_lower_pct']:.4f}% / "
            f"{excursion['median_mae_upper_pct']:.4f}%**.",
            "- Same-bar stop/target ambiguity is resolved stop-first, which is conservative but cannot recover tick order from 1-minute OHLC.",
            "",
            "## 10. Symbol concentration and extreme trades",
            "",
            f"The {len(closed)} fills span **{unique_symbols} unique symbols**. The 10 largest absolute symbol contributions account for "
            f"**{top10_abs_share:.2f}%** of total absolute symbol P&L, so concentration must be monitored.",
            "",
            "### Top symbols", "",
        ]
    )
    add(_table(top_symbols, ["symbol", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Bottom symbols", ""])
    add(_table(bottom_symbols, ["symbol", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs"]))
    add(["", "### Best trades", ""])
    add(_table(best_trades, ["session_date", "setup_id", "side", "symbol", "entry_time", "exit_reason", "net_return_pct", "net_pnl_rs"]))
    add(["", "### Worst trades", ""])
    add(_table(worst_trades, ["session_date", "setup_id", "side", "symbol", "entry_time", "exit_reason", "net_return_pct", "net_pnl_rs"]))

    add(["", "## 11. Cost sensitivity and economic assumptions", ""])
    if not stress_metrics.empty:
        add(_table(stress_metrics, ["period", "scenario", "cost_bps", "slippage_bps", "fills", "wins", "losses", "win_rate_pct", "profit_factor", "net_return_points", "net_pnl_rs", "max_daily_drawdown_points"]))
    add(
        [
            "",
            f"- Reference estimated costs deducted: **Rs {total_cost_rs:,.2f}**.",
            f"- Gross modeled P&L before that cost: **Rs {full_metrics['net_pnl_rs'] + total_cost_rs:,.2f}**.",
            f"- Approximate additional one-way-equivalent cost headroom before modeled net P&L reaches zero: "
            f"**{extra_break_even_bps:.2f} bps per entry notional**. This is arithmetic headroom, not a live fill guarantee.",
            "- Reference slippage is zero; the 20/2 and 25/5 scenarios are more appropriate planning cases.",
            "- Actual brokerage, taxes, futures basis, lot rounding, margin changes, impact and rejected/partial orders are not fully modeled.",
            "",
            "## 12. What the evidence currently says", "",
        ]
    )
    best_setup = setup_metrics.loc[setup_metrics["net_pnl_rs"].idxmax()]
    worst_setup = setup_metrics.loc[setup_metrics["net_pnl_rs"].idxmin()]
    add(
        [
            f"1. **The edge is real inside the tested simulator but not yet independently validated.** PF {full_metrics['profit_factor']:.4f} "
            "and positive stress cases are encouraging; repeated parameter search and incomplete source coverage prevent promotion.",
            f"2. **Setup contribution is uneven.** Best contribution: `{best_setup['setup_id']}` Rs {best_setup['net_pnl_rs']:+,.2f}; "
            f"weakest: `{worst_setup['setup_id']}` Rs {worst_setup['net_pnl_rs']:+,.2f}. Removing or tightening a setup must be "
            "tested through the global portfolio ledger because contributions are not additive counterfactuals.",
            "   Every setup is net-positive in the full reference sample, so this report does not identify an obvious safe deletion.",
            f"3. **Forward behavior is weaker.** The six-session extension is 8-13, PF "
            f"{period_comparison.loc[period_comparison['period'].eq('FORWARD_6'), 'profit_factor'].iloc[0]:.4f}; treat this as a warning, not a conclusion.",
            f"4. **July contributes {float(monthly.loc[monthly['period'].eq('2026-07'), 'net_pnl_rs'].iloc[0] / full_metrics['net_pnl_rs'] * 100.0):.2f}% of full net P&L.** "
            "June was only marginal and August had lower WR, so calendar-regime concentration is material.",
            f"5. **End-of-day handling is material.** `{int(closed['exit_reason'].eq('LAST_REAL_BAR_SENSITIVITY').sum())}` fills use the sensitivity exit and contribute "
            f"Rs {float(exit_metrics.loc[exit_metrics['exit_reason'].eq('LAST_REAL_BAR_SENSITIVITY'), 'net_pnl_rs'].iloc[0]):+,.2f}. "
            "Exact square-off data and policy should be required before judging exits.",
            "6. **The indicator relationship is mostly non-monotonic.** For example, OI change 0.50–1.00% was roughly flat while both lower and much higher OI bins were profitable; "
            "volume ratio 1.50–2.00 and 3.00–5.00 outperformed neighboring bins. Directly choosing the best bin would be in-sample overfitting.",
            "7. **Rank is also non-monotonic.** Rank 3 was negative while ranks 4 and 5+ were strong, which points to setup/cap mixture rather than a simple global rank cutoff.",
            "8. **Short holding periods were weak while 120+ minute holds carried the result.** Holding time is outcome-dependent, so it can motivate an exit experiment but cannot be used as an entry-time predictor.",
            f"9. **OHLC excursion precision is limited.** {excursion['ambiguous_excursion_boundaries']} of {len(closed)} filled trades have an ambiguous MFE/MAE boundary, so do not tune exits to tiny excursion differences.",
            "10. **Actual FnO economics remain untested.** Selection uses futures OI, but execution is modeled on cash bars at lot size one.",
            "",
            "## 13. Safe improvement test plan", "",
            "### Stage A — repair validity before optimizing", "",
            "1. Rebuild missing/incomplete symbol-sessions, including 26-Aug, and require exact 15:30 paths.",
            "2. Add an actual near-month futures execution replay using historical futures 1-minute prices, dated lot sizes, tick sizes, "
            "basis, rollover and realistic margin. Keep cash-execution results as a separate diagnostic.",
            "3. Freeze the present strategy hash and collect at least 20–30 genuinely new sessions without changing thresholds.",
            "",
            "### Stage B — one-factor setup ablations", "",
            "4. Test each weak setup as `ON` versus `OFF`, one at a time, through the same global portfolio ledger and all three cost cases.",
            "5. Test LONG and SHORT caps separately. Do not infer a cap from raw trade addition because duplicate-symbol and margin ordering interact.",
            "6. Re-test `.50` and Gap2 independently on the frozen forward window to determine whether each adds value outside the development sample.",
            "",
            "### Stage C — confirmation and exit tests", "",
            "7. For setups with three confirmation minutes, compare S+1-only, S+1..2 and S+1..3 without changing the five-minute selection.",
            "8. Test one confirmation gate at a time: body threshold, adverse wick, close location, and trigger-distance ceiling. Use predeclared "
            "values derived from market logic, not the best bin in this report.",
            "9. Split exits into target/stop/EOD cohorts and test an earlier time stop or trailing rule only where the full OHLC path is exact.",
            "",
            "### Stage D — acceptance criteria", "",
            "10. Require improvement in PF, net P&L, drawdown and forward-window behavior under both stress cases; reject changes that improve only WR.",
            "11. Apply a multiple-testing penalty or keep a final untouched validation block. Record every attempted parameter set, including failures.",
            "12. Promote only after paper/live parity confirms candidate ranking, timestamp availability, order placement, gap behavior and actual costs.",
            "",
            "## 14. Reproducibility and supporting files", "",
            "Report command:", "",
            "```powershell",
            "python -u fno_v10_full_historical_report.py --source-run "
            f"\"{source_run}\" --stress-run \"{stress_run}\" --report report_v10.md --assets-dir report_v10_assets",
            "```",
            "",
            "Supporting CSVs in `report_v10_assets/` contain every table used here, including daily, rolling, setup, indicator-bin, "
            "correlation, symbol, cost-sensitivity and extreme-trade outputs.",
            "",
            "## 15. Glossary", "",
            "- **Raw/base 5m candidate:** passed the setup's EMA, move, futures-OI, relative-volume and traded-value rules.",
            "- **Selected:** passed Stage 7 and `.50` post-selection overlays and was reranked.",
            "- **Confirmed:** a completed eligible 1-minute candle passed direction, close, body, wick and optional close-location gates.",
            "- **Filled:** a later 1-minute bar crossed the stop-entry trigger and survived Gap2 and portfolio constraints.",
            "- **PF:** gross positive net-return points divided by absolute gross negative net-return points.",
            "- **Net return points:** sum of per-trade percentage returns after modeled cost; not portfolio percent return.",
            "- **MDD:** maximum drawdown of cumulative daily summed trade-return points unless explicitly marked Rs.",
            "- **MFE/MAE:** OHLC-derived favorable/adverse excursion bounds; boundary ambiguity is explicitly flagged.",
            "",
        ]
    )
    common.atomic_write_text(report_path, "\n".join(lines))

    output_paths = [report_path, *chart_paths, *(assets_dir / name for name in asset_frames)]
    manifest_path = assets_dir / "report_manifest.json"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_ist": datetime.now(gaps.IST),
        "report": str(report_path),
        "source_run": str(source_run),
        "stress_run": str(stress_run) if stress_run else None,
        "source_files": {
            name: {"path": str(path), "bytes": path.stat().st_size, "sha256": _sha256_file(path)}
            for name, path in {"provenance": provenance_path, **paths}.items()
        },
        "outputs": [
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for path in output_paths
        ],
        "headline": full_metrics,
        "risk": risk_summary,
        "limitations": provenance.get("limitations", []),
        "research_only": True,
        "headline_valid": False,
    }
    common.atomic_write_json(manifest_path, gaps._json_ready(manifest))
    return {
        "report": report_path,
        "assets_dir": assets_dir,
        "manifest": manifest_path,
        "outputs": len(output_paths),
        "headline": full_metrics,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run", type=Path, required=True)
    parser.add_argument("--stress-run", type=Path)
    parser.add_argument("--report", type=Path, default=Path("report_v10.md"))
    parser.add_argument("--assets-dir", type=Path, default=Path("report_v10_assets"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = build_report(
        source_run=args.source_run,
        stress_run=args.stress_run,
        report_path=args.report,
        assets_dir=args.assets_dir,
    )
    print(f"[V10-FULL-REPORT] complete: {result['report']}", flush=True)
    print(f"[V10-FULL-REPORT] outputs: {result['outputs']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
