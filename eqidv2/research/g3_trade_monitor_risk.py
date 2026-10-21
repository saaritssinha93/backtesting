"""Ledger diagnostics; realized equity does not measure open-position risk.

Concentration shares divide the largest positive group net profits by total
net profit. They can exceed 100% when other groups lose, and are undefined
(``None``) when total net profit is not positive. Sessions are the distinct
days present in the supplied ledger, including days without an executed trade.
"""
from __future__ import annotations

import math
from numbers import Real

import numpy as np
import pandas as pd


def _execution_flag(value: object) -> bool:
    """Parse actual booleans and their CSV representations without truthiness."""
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, Real) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1", "1.0"):
            return True
        if normalized in ("false", "0", "0.0"):
            return False
    raise ValueError(f"Invalid portfolio_executed value: {value!r}")


def _prepared(ledger: pd.DataFrame) -> pd.DataFrame:
    if ledger.empty:
        return pd.DataFrame(columns=["day", "portfolio_executed", "_pnl", "_exit"])
    required = {"day", "portfolio_executed", "portfolio_net_profit_rupees", "exit_ts"}
    missing = required.difference(ledger.columns)
    if missing:
        raise ValueError(f"Missing risk diagnostic columns: {sorted(missing)}")
    result = ledger.copy()
    if result["day"].isna().any():
        raise ValueError("Every ledger row requires a session day")

    def session_day(value: object) -> str:
        timestamp = pd.Timestamp(value)
        if pd.isna(timestamp):
            raise ValueError("Missing session day")
        return timestamp.date().isoformat()

    try:
        result["day"] = result["day"].map(session_day)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("Every ledger row requires a valid session day") from exc
    result["portfolio_executed"] = result["portfolio_executed"].map(_execution_flag)
    executed = result["portfolio_executed"]
    pnl = pd.to_numeric(result.loc[executed, "portfolio_net_profit_rupees"], errors="coerce")
    if not np.isfinite(pnl.to_numpy(dtype=float)).all():
        raise ValueError("Executed trades require finite portfolio net profit")
    exit_values = result["exit_ts"].copy()
    if "exit_execution_ts" in result:
        actual = result["exit_execution_ts"]
        provided = actual.notna() & actual.astype(str).str.strip().ne("")
        exit_values = actual.where(provided, exit_values)
    exits = pd.to_datetime(exit_values.loc[executed], utc=True, format="mixed", errors="coerce")
    if exits.isna().any():
        raise ValueError("Executed trades require valid exit timestamps")
    result["_pnl"] = 0.0
    result.loc[executed, "_pnl"] = pnl
    result["_exit"] = pd.to_datetime(exit_values.where(executed), utc=True,
                                     format="mixed", errors="coerce")
    return result


def _drawdown(profits: pd.Series) -> float:
    if profits.empty:
        return 0.0
    values = profits.to_numpy(dtype=float)
    equity = np.cumsum(values)
    if not np.isfinite(equity).all():
        raise ValueError("Nonfinite cumulative ledger profit")
    peaks = np.maximum.accumulate(np.concatenate(([0.0], equity)))[1:]
    with np.errstate(over="ignore"):
        drawdowns = peaks - equity
    if not np.isfinite(drawdowns).all():
        raise ValueError("Nonfinite ledger drawdown")
    return float(np.max(drawdowns))


def _profit_share(profits: pd.Series, count: int, total: float) -> float | None:
    if total <= 0:
        return None
    share = float(profits.loc[profits > 0].nlargest(count).sum() / total * 100.0)
    if not math.isfinite(share):
        raise ValueError("Nonfinite profit concentration")
    return share


def _diagnostics(prepared: pd.DataFrame) -> dict:
    daily = prepared.groupby("day")["_pnl"].sum().sort_index()
    executed = prepared.loc[prepared["portfolio_executed"].astype(bool)]
    # Same-instant fills settle together. Row ordering must not create a peak.
    realized = executed.groupby("_exit")["_pnl"].sum().sort_index()
    total = float(daily.sum())
    if not math.isfinite(total):
        raise ValueError("Nonfinite total ledger profit")
    symbol_share = None
    if not executed.empty:
        if "tradingsymbol" not in executed or executed["tradingsymbol"].isna().any():
            raise ValueError("Executed trades require tradingsymbol for concentration")
        symbols = executed.groupby("tradingsymbol")["_pnl"].sum()
        symbol_share = _profit_share(symbols, 5, total)
    return {
        "daily_close_drawdown_rs": _drawdown(daily),
        "realized_exit_drawdown_rs": _drawdown(realized),
        "mark_to_market_available": False,
        "executed_trades": int(len(executed)),
        "wins": int((executed["_pnl"] > 0).sum()),
        "losses": int((executed["_pnl"] < 0).sum()),
        "top5_day_profit_share_pct": _profit_share(daily, 5, total),
        "top10_day_profit_share_pct": _profit_share(daily, 10, total),
        "top5_symbol_profit_share_pct": symbol_share,
        "positive_days": int((daily > 0).sum()),
        "total_days": int(len(daily)),
    }


def risk_diagnostics(ledger: pd.DataFrame) -> dict:
    """Return JSON-safe risk metrics for one portfolio ledger.

    Drawdowns start from zero accumulated profit. Unexecuted rows contribute
    zero profit but retain their session days. No mark-to-market series is
    inferred from trade outcomes, MFE, or MAE.
    """
    return _diagnostics(_prepared(ledger))


def breakdowns(ledger: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return all-period, monthly, setup, symbol, and side diagnostics.

    For setup/symbol/side rows, total_days counts represented ledger days for
    that group. Group comparisons are descriptive and are not selection rules.
    """
    prepared = _prepared(ledger)
    prepared["month"] = prepared["day"].str[:7]

    def row(frame: pd.DataFrame) -> dict:
        diagnostics = _diagnostics(frame)
        count = diagnostics["executed_trades"]
        return {
            "net_pnl_rs": float(frame["_pnl"].sum()),
            "win_rate_pct": diagnostics["wins"] / count * 100.0 if count else None,
            **diagnostics,
        }

    result = {"period": pd.DataFrame([{"period": "all", **row(prepared)}])}
    for name, key in (("monthly", "month"), ("setup", "setup_id"),
                      ("symbol", "tradingsymbol"), ("side", "side")):
        rows = []
        if key in prepared:
            rows = [{key: value, **row(group)} for value, group in
                    prepared.groupby(key, dropna=False, sort=True)]
        result[name] = pd.DataFrame(rows, columns=[key, *row(prepared).keys()])
    return result
