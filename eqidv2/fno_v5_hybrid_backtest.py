"""Replay the current V5 setup book on cash prices joined to futures OI."""

from __future__ import annotations

import math
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_oi_common as common
import fno_oi_ema_confirm_sweep as simulator
import fno_oi_hybrid_data as hybrid
import fno_v5_live_config as config


OBJECTIVE = config.SELECTED_OBJECTIVE


def _pf(profit: float, loss: float) -> float:
    if loss > 0:
        return float(profit / loss)
    return float("inf") if profit > 0 else float("nan")


def _eligible(signals: pd.DataFrame, setup: config.SetupSpec) -> pd.DataFrame:
    rows = signals.loc[
        signals["hhmm_int"].eq(int(setup.signal_end.replace(":", "")))
        & signals["side"].eq(setup.side)
    ].copy()
    if rows.empty or setup.mode == "FORCE_DAILY":
        return rows
    price_ok = (
        rows["price_change_pct"].ge(setup.price_change_pct)
        if setup.side == "LONG"
        else rows["price_change_pct"].le(-setup.price_change_pct)
    )
    return rows.loc[
        price_ok
        & rows["oi_change_pct"].ge(setup.oi_change_pct)
        & rows["volume_ratio"].ge(setup.volume_ratio)
        & rows["body_ratio"].ge(setup.body_ratio)
        & rows["wick_ratio"].le(setup.max_wick_ratio)
        & rows["traded_value"].ge(setup.min_traded_value)
    ].copy()


def select_setup_rows(
    signals: pd.DataFrame,
    setup: config.SetupSpec,
) -> pd.DataFrame:
    rows = _eligible(signals, setup)
    if rows.empty:
        return rows
    picker_columns = {
        "max_oi": "oi_change_pct",
        "max_volume": "volume_ratio",
        "max_move": "abs_price_change_pct",
        "max_body": "body_ratio",
        "max_liquidity": "traded_value",
    }
    picker_column = picker_columns[setup.picker]
    if picker_column == "abs_price_change_pct":
        rows[picker_column] = rows["price_change_pct"].abs()
    rows = rows.sort_values(
        ["day", picker_column, "traded_value", "tradingsymbol"],
        ascending=[True, False, False, True],
        kind="stable",
    )
    return rows.groupby("day", sort=False, as_index=False).head(setup.max_entries)


def replay_setups(
    signals: pd.DataFrame,
    paths: dict[int, dict[str, np.ndarray]],
    *,
    cost_bps: float,
    setups: Iterable[config.SetupSpec] = config.ACTIVE_SETUPS,
) -> pd.DataFrame:
    audit_parts: list[pd.DataFrame] = []
    for setup in setups:
        selected = select_setup_rows(signals, setup).reset_index(drop=True)
        if selected.empty:
            continue
        selected["net_return_pct"] = simulator.simulate_bracket(
            selected,
            paths,
            stop_pct=setup.stop_pct,
            target_pct=setup.target_pct,
            cost_bps=cost_bps,
        )
        selected["filled"] = selected["net_return_pct"].notna()
        selected["setup_id"] = setup.setup_id
        selected["confirmation_end"] = setup.confirmation_end
        selected["setup_mode"] = setup.mode
        selected["picker"] = setup.picker
        selected["max_entries"] = setup.max_entries
        selected["stop_pct"] = setup.stop_pct
        selected["target_pct"] = setup.target_pct
        audit_parts.append(selected)
    if not audit_parts:
        return pd.DataFrame()
    audit = pd.concat(audit_parts, ignore_index=True, sort=False)
    audit["data_contract"] = hybrid.DATA_CONTRACT_VERSION
    return audit.sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


def _side_daily(audit: pd.DataFrame, days: list[date], side: str) -> pd.DataFrame:
    subset = audit.loc[audit["side"].eq(side)].copy()
    if subset.empty:
        return pd.DataFrame(
            {
                "day": days,
                f"{side.lower()}_selections": 0,
                f"{side.lower()}_fills": 0,
                f"{side.lower()}_return_pct": 0.0,
            }
        )
    grouped = subset.groupby("day", sort=True).agg(
        selections=("sid", "size"),
        fills=("filled", "sum"),
        return_pct=("net_return_pct", "sum"),
    )
    grouped = grouped.reindex(days, fill_value=0).reset_index()
    return grouped.rename(
        columns={
            "selections": f"{side.lower()}_selections",
            "fills": f"{side.lower()}_fills",
            "return_pct": f"{side.lower()}_return_pct",
        }
    )


def build_daily_curve(
    audit: pd.DataFrame,
    days: list[date],
    *,
    split_day: date,
) -> pd.DataFrame:
    daily = pd.DataFrame({"day": days})
    daily = daily.merge(_side_daily(audit, days, "LONG"), on="day", how="left")
    daily = daily.merge(_side_daily(audit, days, "SHORT"), on="day", how="left")
    daily["selections"] = daily["long_selections"] + daily["short_selections"]
    daily["fills"] = daily["long_fills"] + daily["short_fills"]
    daily["portfolio_net_return_pct"] = (
        daily["long_return_pct"] + daily["short_return_pct"]
    )
    daily["cumulative_net_pct"] = daily["portfolio_net_return_pct"].cumsum()
    daily["period"] = np.where(daily["day"].lt(split_day), "TRAIN", "TEST")
    daily["objective"] = OBJECTIVE
    daily["data_contract"] = hybrid.DATA_CONTRACT_VERSION

    cumulative_trade_pf: list[float] = []
    cumulative_day_pf: list[float] = []
    for current_day in days:
        trades = audit.loc[
            audit["day"].le(current_day) & audit["filled"], "net_return_pct"
        ].to_numpy(float)
        trade_profit = float(trades[trades > 0].sum()) if trades.size else 0.0
        trade_loss = float(-trades[trades < 0].sum()) if trades.size else 0.0
        cumulative_trade_pf.append(_pf(trade_profit, trade_loss))
        day_net = daily.loc[
            daily["day"].le(current_day), "portfolio_net_return_pct"
        ].to_numpy(float)
        day_profit = float(day_net[day_net > 0].sum())
        day_loss = float(-day_net[day_net < 0].sum())
        cumulative_day_pf.append(_pf(day_profit, day_loss))
    daily["cumulative_trade_pf"] = cumulative_trade_pf
    daily["cumulative_day_pf"] = cumulative_day_pf
    return daily


def summary_stats(daily: pd.DataFrame, audit: pd.DataFrame) -> dict[str, Any]:
    fills = audit.loc[audit["filled"], "net_return_pct"].to_numpy(float)
    profit = float(fills[fills > 0].sum()) if fills.size else 0.0
    loss = float(-fills[fills < 0].sum()) if fills.size else 0.0
    day_net = daily["portfolio_net_return_pct"].to_numpy(float)
    day_profit = float(day_net[day_net > 0].sum())
    day_loss = float(-day_net[day_net < 0].sum())
    return {
        "sessions": int(len(daily)),
        "orders": int(len(audit)),
        "fills": int(audit["filled"].sum()),
        "wins": int((fills > 0).sum()),
        "losses": int((fills < 0).sum()),
        "trade_pf": _pf(profit, loss),
        "day_pf": _pf(day_profit, day_loss),
        "net_pct": float(fills.sum()) if fills.size else 0.0,
        "positive_days": int((day_net > 0).sum()),
        "negative_days": int((day_net < 0).sum()),
        "flat_days": int((day_net == 0).sum()),
    }


def setup_summary(audit: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in config.ACTIVE_SETUPS:
        trades = audit.loc[audit["setup_id"].eq(setup.setup_id)].copy()
        filled = trades.loc[trades["filled"], "net_return_pct"].to_numpy(float)
        profit = float(filled[filled > 0].sum()) if filled.size else 0.0
        loss = float(-filled[filled < 0].sum()) if filled.size else 0.0
        rows.append(
            {
                **asdict(setup),
                "data_contract": hybrid.DATA_CONTRACT_VERSION,
                "orders": int(len(trades)),
                "fills": int(trades["filled"].sum()) if not trades.empty else 0,
                "pf": _pf(profit, loss),
                "net_pct": float(filled.sum()) if filled.size else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _fmt(value: Any, digits: int = 3) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if math.isinf(number):
        return "INF"
    if math.isnan(number):
        return ""
    return f"{number:.{digits}f}"


def render_report(
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    setups: pd.DataFrame,
    stats: dict[str, Any],
    *,
    split_day: date,
) -> str:
    coverage = hybrid.backtest_historical_coverage()
    period_stats: list[tuple[str, dict[str, Any]]] = []
    for period in ("TRAIN", "TEST"):
        period_daily = daily.loc[daily["period"].eq(period)].copy()
        period_days = set(period_daily["day"])
        period_audit = audit.loc[audit["day"].isin(period_days)].copy()
        period_stats.append((period, summary_stats(period_daily, period_audit)))
    lines = [
        "# FNO V5 Hybrid Cash-Equity Backtest",
        "",
        f"Data contract: `{hybrid.DATA_CONTRACT_VERSION}`",
        f"Historical cash source: `{hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR}`",
        f"Cash 5-minute construction: `{hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION}` (five exact end-labelled 1-minute rows per candle).",
        "Live cash 5-minute counterpart: `Live Data Fetch (5mins)` equity parquet.",
        "Futures source usage: OI and OI percentage change only.",
        "Price, volume, EMA9/20/50, traded value, confirmation, trigger and exits: NSE equity.",
        "Cash 5-minute quality: completed end-labelled exchange bars only; 09:15 opening snapshots, synthetic gaps, provisional stale bars and incomplete 1-minute aggregates are excluded.",
        "OI-history limitation: the available cache uses 26AUG futures OI across this historical period; it is not a historical rolling near-month series.",
        f"Historical cash coverage: {coverage['complete_equity_histories']}/{coverage['mapped_stock_futures']} mapped stock futures; missing: {', '.join(coverage['missing']) or 'none'}.",
        f"Train/test label split: `{split_day.isoformat()}`.",
        "This is a replay of the current V5 setup book, not a fresh parameter optimisation.",
        "",
        "Metric | Value",
        "--- | ---:",
        f"Sessions | {stats['sessions']}",
        f"Orders / fills | {stats['orders']} / {stats['fills']}",
        f"Trade PF | {_fmt(stats['trade_pf'])}",
        f"Day PF | {_fmt(stats['day_pf'])}",
        f"Cumulative trade-return sum | {stats['net_pct']:+.3f}%",
        f"Positive / negative / flat days | {stats['positive_days']} / {stats['negative_days']} / {stats['flat_days']}",
        "",
        "Period | Sessions | Orders / fills | Trade PF | Day PF | Net %",
        "--- | ---: | ---: | ---: | ---: | ---:",
    ]
    for period, period_result in period_stats:
        lines.append(
            f"{period} | {period_result['sessions']} | {period_result['orders']} / "
            f"{period_result['fills']} | {_fmt(period_result['trade_pf'])} | "
            f"{_fmt(period_result['day_pf'])} | {period_result['net_pct']:+.3f}%"
        )
    lines.extend(
        [
        "",
        "## Setup Results",
        "",
        "Confirmation | Side | Mode | Max | Picker | Stop | Target | Orders | Fills | PF | Net %",
        "--- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---:",
        ]
    )
    for row in setups.itertuples(index=False):
        lines.append(
            f"{row.confirmation_end} | {row.side} | {row.mode} | {row.max_entries} | "
            f"{row.picker} | {row.stop_pct:.2f}% | {row.target_pct:.2f}% | "
            f"{row.orders} | {row.fills} | {_fmt(row.pf)} | {row.net_pct:+.3f}%"
        )
    lines.extend(
        [
            "",
            "## Daywise",
            "",
            "Day | Period | L O/F | Long % | S O/F | Short % | Total % | Cum % | Cum trade PF | Cum day PF",
            "--- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:",
        ]
    )
    for row in daily.itertuples(index=False):
        lines.append(
            f"{row.day} | {row.period} | {row.long_selections}/{row.long_fills} | "
            f"{row.long_return_pct:+.3f}% | {row.short_selections}/{row.short_fills} | "
            f"{row.short_return_pct:+.3f}% | {row.portfolio_net_return_pct:+.3f}% | "
            f"{row.cumulative_net_pct:+.3f}% | {_fmt(row.cumulative_trade_pf)} | "
            f"{_fmt(row.cumulative_day_pf)}"
        )
    return "\n".join(lines) + "\n"


def write_outputs(
    daily: pd.DataFrame,
    audit: pd.DataFrame,
    setups: pd.DataFrame,
    report: str,
    *,
    daily_paths: Iterable[Path],
    audit_path: Path,
    setup_path: Path,
    report_path: Path,
) -> None:
    for path in daily_paths:
        common.atomic_write_csv(daily, path)
    common.atomic_write_csv(audit, audit_path)
    common.atomic_write_csv(setups, setup_path)
    common.atomic_write_text(report_path, report)
