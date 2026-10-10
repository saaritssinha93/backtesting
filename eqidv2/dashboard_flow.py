"""Read-only, standard-library data adapter for Dashboard Flow.

Only retained V13-V10-G, G-2 and G-3 backtest CSV files are eligible. Public
run IDs resolve through discovery; callers cannot supply a filesystem path.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any


DATA_ROOT = Path(r"C:\TradingData\eqidv2")
_SOURCES = (
    ("g", "V13-V10-G", "V13-V10-G backtesting", "backtesting_result_v13_v10_g", "runs/*/*/daily_results.csv"),
    ("g-full", "V13-V10-G", "V13-V10-G full backtest", "fno_oi/strategy_research/v13_v10_g_full_history", "run_*/g_backtest/portfolio_trades.csv"),
    ("g2", "V13-V10-G-2", "V13-V10-G-2 backtesting", "fno_oi/strategy_research/v13_corrected_v10_g_2", "run_*/daily_results.csv"),
    ("g3", "V13-V10-G-3", "V13-V10-G-3 backtesting", "backtesting_result_v13_v10_g_3", "runs/*/*/daily_results.csv"),
)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (OSError, ValueError):
        return False


def _number(value: Any) -> float | None:
    try:
        result = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _numeric(row: dict, *keys: str) -> float | None:
    for key in keys:
        if row.get(key) not in (None, ""):
            return _number(row[key])
    return None


def _text(row: dict, *keys: str) -> str:
    for key in keys:
        if row.get(key) not in (None, ""):
            return str(row[key]).strip()
    return ""


def _day(value: str) -> str:
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except (TypeError, ValueError):
        return ""


def _sum(values) -> float | None:
    values = list(values)
    if any(value is None for value in values):
        return None
    try:
        result = math.fsum(values)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _rows(path: Path, warnings: list[str]) -> list[dict]:
    if not path.is_file() or not _inside(path, DATA_ROOT):
        return []
    try:
        with path.open(newline="", encoding="utf-8-sig") as stream:
            return list(csv.DictReader(stream))
    except (OSError, UnicodeError, csv.Error):
        warnings.append(f"The saved {path.name} could not be read.")
        return []


def _full_sessions(run: Path, warnings: list[str]) -> list[str]:
    """Use the saved eligible sessions, including sessions with no executions."""
    metadata_path = run / "run_metadata.json"
    sessions_path = run.parent / "dataset" / "source_session_eligibility.csv"
    if not all(path.is_file() and _inside(path, DATA_ROOT) for path in (metadata_path, sessions_path)):
        return []
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
        first, last = _day(metadata.get("first_session", "")), _day(metadata.get("last_session", ""))
        count = _number(metadata.get("session_count"))
    except (OSError, UnicodeError, ValueError, AttributeError):
        return []
    if not first or not last or first > last or not count:
        return []
    sessions = sorted({
        day for row in _rows(sessions_path, warnings)
        if _text(row, "eligible").lower() in {"true", "1", "1.0", "yes"}
        and (day := _day(_text(row, "day"))) and first <= day <= last
    })
    # Incomplete exports cannot turn missing sessions into zero-trade results.
    return sessions if len(sessions) == count and sessions[0] == first and sessions[-1] == last else []


def _discover() -> list[tuple[dict, Path]]:
    found = []
    for prefix, strategy, source_label, relative, pattern in _SOURCES:
        root = DATA_ROOT / relative
        if not _inside(root, DATA_ROOT):
            continue
        try:
            paths = sorted(root.glob(pattern), reverse=True)
        except OSError:
            continue
        for daily_path in paths:
            if not daily_path.is_file() or not _inside(daily_path, root):
                continue
            full_history = prefix == "g-full"
            sessions = _full_sessions(daily_path.parent, []) if full_history else sorted({
                day for row in _rows(daily_path, [])
                if (day := _day(_text(row, "day", "date")))
            })
            if full_history and not sessions:
                continue
            run = daily_path.parent.parent if full_history else daily_path.parent
            identity = run.relative_to(root).as_posix()
            run_id = prefix + "-" + hashlib.sha256(identity.encode()).hexdigest()[:16]
            meta = {
                "id": run_id,
                "label": f"{strategy} · {run.name}",
                "strategy": strategy,
                "run_name": run.name,
                "source_label": source_label,
                "kind": "full_history" if full_history else "daily" if prefix == "g" else "backtest",
                "period_start": sessions[0] if sessions else None,
                "period_end": sessions[-1] if sessions else None,
                "sessions": len(sessions),
            }
            found.append((meta, daily_path))
    # Newest strategy first, then newest run; retain full-history priority within G.
    family_order = {"V13-V10-G-3": 3, "V13-V10-G-2": 2, "V13-V10-G": 1}
    found.sort(key=lambda entry: (family_order[entry[0]["strategy"]], entry[0]["kind"] == "full_history", entry[0]["run_name"]), reverse=True)
    return found


def _load_trades(run: Path, warnings: list[str]) -> list[dict]:
    path = next((run / name for name in ("trades_with_pullbacks.csv", "portfolio_trades.csv") if (run / name).is_file()), None)
    if path is None or not _inside(path, run):
        return []
    trades = []
    for index, row in enumerate(_rows(path, warnings)):
        # Portfolio sheets include rejected/unfilled orders; those are not trades.
        flag = _text(row, "portfolio_executed", "filled")
        if flag and flag.lower() not in {"true", "1", "1.0", "yes"}:
            continue
        if _text(row, "exit_reason").upper() == "UNFILLED":
            continue
        day = _day(_text(row, "day", "date", "entry_time_ist", "entry_ts"))
        if not day:
            continue
        trade_id = _text(row, "trade_id", "signal_id", "sid") or str(index + 1)
        setup = _text(row, "setup_id", "setup")
        if not setup and "|" in trade_id:
            setup = trade_id.split("|")[1]
        trades.append({
            "id": trade_id,
            "date": day,
            "symbol": _text(row, "tradingsymbol", "ticker", "symbol"),
            "side": _text(row, "side").upper(),
            "setup": setup,
            "entry_time": _text(row, "entry_time_ist", "entry_ts", "entry_time"),
            "exit_time": _text(row, "exit_time_ist", "exit_ts", "exit_time"),
            "entry_price": _numeric(row, "entry_price", "filled_price"),
            "exit_price": _numeric(row, "exit_price"),
            "net_pnl": _numeric(row, "portfolio_net_profit_rupees", "net_pnl", "net_profit_rupees", "net_pnl_rupees"),
            "gross_pnl": _numeric(row, "portfolio_gross_profit_rupees", "gross_pnl", "pre_cost_profit_rupees", "gross_pnl_rupees"),
            "cost": _numeric(row, "portfolio_cost_rupees", "cost", "cost_rupees"),
            "exit_reason": _text(row, "exit_reason", "trade_exit_outcome", "outcome"),
        })
    return sorted(trades, key=lambda row: (row["date"], row["entry_time"], row["id"]))


def _load_daily(path: Path, trades: list[dict], warnings: list[str]) -> list[dict]:
    return _normalize_daily(_rows(path, warnings), trades, warnings)


def _load_full_daily(run: Path, trades: list[dict], warnings: list[str]) -> list[dict]:
    by_day = defaultdict(list)
    for trade in trades:
        by_day[trade["date"]].append(trade)
    rows = []
    for day in _full_sessions(run, warnings):
        executions = by_day[day]
        row = {"day": day, "trades": len(executions)}
        for field in ("net_pnl", "gross_pnl", "cost"):
            row[field] = _sum(trade[field] for trade in executions)
        for field, sign in (("wins", 1), ("losses", -1)):
            row[field] = sum(trade["net_pnl"] * sign > 0 for trade in executions) if all(trade["net_pnl"] is not None for trade in executions) else None
        rows.append(row)
    return _normalize_daily(rows, trades, warnings)


def _normalize_daily(rows: list[dict], trades: list[dict], warnings: list[str]) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        day = _day(_text(row, "day", "date", "session_date"))
        if not day:
            warnings.append("A daily row with an invalid date was excluded.")
            continue
        grouped[day].append(row)
    by_day = defaultdict(list)
    for trade in trades:
        by_day[trade["date"]].append(trade)
    daily = []
    cumulative, peak = 0.0, 0.0
    for day, rows in sorted(grouped.items()):
        item = {"date": day}
        for field, aliases in {
            "trades": ("trades", "fills"),
            "wins": ("wins",),
            "losses": ("losses",),
            "net_pnl": ("net_pnl", "net_profit_rupees", "net_pnl_rupees"),
            "gross_pnl": ("gross_pnl", "gross_pnl_rupees", "pre_cost_profit_rupees"),
            "cost": ("cost", "cost_rupees"),
        }.items():
            item[field] = _sum(_numeric(row, *aliases) for row in rows)
        day_trades = by_day[day]
        if item["trades"] is None and day_trades:
            item["trades"] = len(day_trades)
        complete = item["trades"] == len(day_trades)
        if complete:
            for field in ("gross_pnl", "cost"):
                if item[field] is None:
                    item[field] = _sum(trade[field] for trade in day_trades)
            if all(trade["net_pnl"] is not None for trade in day_trades):
                for field, sign in (("wins", 1), ("losses", -1)):
                    if item[field] is None:
                        item[field] = sum(trade["net_pnl"] * sign > 0 for trade in day_trades)
        if item["net_pnl"] is None or cumulative is None:
            cumulative = None
            item["drawdown"] = None
        else:
            cumulative = _sum((cumulative, item["net_pnl"]))
            if cumulative is not None:
                peak = max(peak, cumulative)
                item["drawdown"] = _number(cumulative - peak)
            else:
                item["drawdown"] = None
        item["cumulative_net_pnl"] = cumulative
        daily.append(item)
    return daily


def _summary(daily: list[dict], trades: list[dict], warnings: list[str]) -> dict:
    result = {field: _sum(row[field] for row in daily) for field in ("trades", "wins", "losses", "net_pnl", "gross_pnl", "cost")}
    count = result["trades"]
    known_outcomes = count is not None and result["wins"] is not None and result["losses"] is not None
    result["breakeven"] = max(0, count - result["wins"] - result["losses"]) if known_outcomes else None
    result["win_rate_pct"] = _number(result["wins"] / count * 100) if count and result["wins"] is not None else None
    result["average_trade"] = _number(result["net_pnl"] / count) if count and result["net_pnl"] is not None else None
    net_values = [row["net_pnl"] for row in daily]
    valid_days = [value for value in net_values if value is not None]
    all_days_known = len(valid_days) == len(daily)
    result.update({
        "sessions": len(daily),
        "period_start": daily[0]["date"] if daily else None,
        "period_end": daily[-1]["date"] if daily else None,
        "best_day": max(valid_days) if valid_days and all_days_known else None,
        "worst_day": min(valid_days) if valid_days and all_days_known else None,
        "winning_days": sum(value > 0 for value in valid_days) if all_days_known else None,
        "losing_days": sum(value < 0 for value in valid_days) if all_days_known else None,
        "flat_days": sum(value == 0 for value in valid_days) if all_days_known else None,
        "max_drawdown": abs(min((row["drawdown"] for row in daily), default=0)) if all_days_known and all(row["drawdown"] is not None for row in daily) else None,
        "profit_factor": None,
        "profit_factor_basis": None,
        "long_trades": None,
        "short_trades": None,
    })
    trade_total = _sum(trade["net_pnl"] for trade in trades)
    complete = count == len(trades) and trade_total is not None and result["net_pnl"] is not None and math.isclose(trade_total, result["net_pnl"], abs_tol=0.02, rel_tol=1e-9)
    if complete:
        result["long_trades"] = sum(trade["side"] in {"LONG", "BUY"} for trade in trades)
        result["short_trades"] = sum(trade["side"] in {"SHORT", "SELL"} for trade in trades)
        if trades:
            gains = _sum(max(trade["net_pnl"], 0) for trade in trades)
            losses = _sum(-min(trade["net_pnl"], 0) for trade in trades)
            ratio = gains / losses if gains is not None and losses else None
            result["profit_factor"] = _number(ratio)
            result["profit_factor_basis"] = "net trade P&L"
            if not losses:
                warnings.append("Profit factor is undefined because the saved trades contain no net losses.")
    elif count:
        warnings.append("The trade ledger does not fully reconcile to the daily results; trade profit factor is unavailable.")
    if not all_days_known:
        warnings.append("Some daily P&L values are missing or non-finite; affected totals and cumulative metrics are unavailable.")
    return result


def load_flow_data(run_id: str | None = None) -> dict:
    """Return normalized historical evidence; reject IDs absent from discovery."""
    discovered = _discover()
    runs = [metadata for metadata, _ in discovered]
    selected = next((entry for entry in discovered if entry[0]["id"] == run_id), None) if run_id else (discovered[0] if discovered else None)
    if run_id and selected is None:
        raise ValueError("Unknown dashboard run.")
    warnings: list[str] = []
    if selected:
        metadata, path = selected
        trades = _load_trades(path.parent, warnings)
        daily = _load_full_daily(path.parent, trades, warnings) if metadata["kind"] == "full_history" else _load_daily(path, trades, warnings)
        # Keep the ledger in the same date scope as the daily summary.
        dates = {row["date"] for row in daily}
        excluded = sum(trade["date"] not in dates for trade in trades)
        if excluded:
            warnings.append("Trades outside the saved daily result dates were excluded.")
        trades = [trade for trade in trades if trade["date"] in dates]
    else:
        metadata, trades, daily = None, [], []
    if not daily:
        warnings.append("No saved daily backtest results are available for this selection.")
    summary = _summary(daily, trades, warnings)
    return {
        "state": "ready" if daily else "empty",
        "runs": runs,
        "selected_run": metadata,
        "daily": daily,
        "trades": trades,
        "summary": summary,
        "warnings": list(dict.fromkeys(warnings)),
        "currency": "INR",
        "data_mode": "Historical backtest",
    }
