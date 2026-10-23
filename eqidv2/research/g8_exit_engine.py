"""Pure, causal single-exit replay and portfolio accounting for G8 research.

Input orders must include every pre-portfolio G7 V2 selected order. Filtering
an already executed-only ledger would prevent later capital-release effects
from being reproduced. Percentage Series and decision Series are keyed by sid.
The inherited fixed-exposure model permits fractional theoretical quantities;
these results are not broker orders or rounded executable share quantities.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import math
import hashlib
import json

import numpy as np
import pandas as pd

from research.g3_sl_target_simulator import simulate_exit
from research.g6_primary_index_direction import ENGINE


VERSION = "V13-V10-G-8-NIFTY-VIX-RESEARCH-V1"


def _number(value, name, *, positive=True):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(f"{name} must be numeric")
    value = float(value)
    if not math.isfinite(value) or (value <= 0 if positive else value < 0):
        raise ValueError(f"{name} must be finite and {'positive' if positive else 'nonnegative'}")
    return value


def _flags(values, name):
    """Accept native bools and canonical CSV bools, never bool('False')."""
    normalized = pd.Series(values, copy=False).map(
        lambda value: str(value).strip().lower() if pd.notna(value) else "")
    if not normalized.isin(["true", "false", "1", "0"]).all():
        raise ValueError(f"{name} must contain only nonmissing boolean values")
    return normalized.isin(["true", "1"])


def _validate_orders(book):
    required = {"sid", "filled", "side", "day", "entry_price", "entry_ts", "entry_path_index"}
    missing = required.difference(book.columns)
    if missing:
        raise ValueError("Missing order columns: " + ", ".join(sorted(missing)))
    if book.sid.isna().any() or book.sid.duplicated().any() or not book.index.is_unique:
        raise ValueError("Order sid and row index must be unique and nonmissing")
    if not book.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Order side must be LONG or SHORT")


def _per_order(value, book, name):
    if isinstance(value, Mapping):
        value = pd.Series(value)
    if isinstance(value, pd.Series):
        if not value.index.is_unique or set(value.index) != set(book.sid):
            raise ValueError(f"{name} Series must have exactly one value per sid")
        result = book.sid.map(value).astype(float)
    elif np.isscalar(value):
        result = pd.Series(_number(value, name), index=book.index)
    else:
        raise ValueError(f"{name} must be a scalar or Series indexed by sid")
    if not np.isfinite(result).all() or not result.between(0, 100, inclusive="neither").all():
        raise ValueError(f"{name} must be between 0 and 100 percent")
    return result


def _size(book, config):
    capital = _number(config["capital_per_entry_rs"], "capital_per_entry_rs")
    leverage = _number(config["leverage"], "leverage")
    sized = ENGINE.v5.apply_fixed_capital_model(book, capital, leverage)
    scale = pd.to_numeric(sized.get("g8_quantity_scale", pd.Series(1., index=sized.index)), errors="coerce")
    if not np.isfinite(scale).all() or not scale.between(0, 1, inclusive="right").all():
        raise ValueError("G8 quantity scale must be positive and cannot exceed baseline exposure")
    # Scaling capital and exposure together retains the inherited leverage.
    # Percent returns therefore stay unchanged; all money columns scale once.
    for column in (
        "capital_per_entry_rupees", "exposure_per_entry_rupees",
        "unleveraged_pre_cost_profit_rupees", "unleveraged_cost_rupees",
        "unleveraged_net_profit_rupees", "pre_cost_profit_rupees", "cost_rupees", "net_profit_rupees",
    ):
        sized[column] = sized[column] * scale
    entry = pd.to_numeric(sized.entry_price, errors="coerce")
    sized["g8_model_quantity"] = np.where(sized.filled, sized.exposure_per_entry_rupees / entry, 0.)
    return sized


def replay_exits(original: pd.DataFrame, paths: Mapping,
                 stop_pct, target_pct, config: Mapping, risk_scale: bool = False) -> pd.DataFrame:
    """Replay fixed exits while preserving every original entry and fill.

    Stops/targets are percentages, or Series indexed by order sid. The inherited
    1.20% stop tightens to 1.00% after 120 minutes by default. For a candidate
    initially below 1.00%, scheduled tightening cannot loosen that stop.
    ``risk_scale`` reduces exposure by min(1, baseline_stop / candidate_stop).
    No sizing increase is permitted when a candidate has a narrower stop.
    """
    _validate_orders(original)
    if not isinstance(risk_scale, (bool, np.bool_)):
        raise ValueError("risk_scale must be boolean")
    work = original.copy(deep=True)
    work["filled"] = _flags(work.filled, "filled")
    stops = _per_order(stop_pct, work, "stop_pct")
    targets = _per_order(target_pct, work, "target_pct")
    costs = _number(config.get("model_cost_bps_round_trip", 5.), "cost_bps", positive=False) / 100
    delay = config.get("tighten_after_minutes", 120)
    timed_stop = config.get("tightened_stop_pct", 1.)
    if (delay is None) != (timed_stop is None):
        raise ValueError("Both scheduled-tightening settings must be set or disabled")
    if delay is not None:
        delay = _number(delay, "tighten_after_minutes")
        timed_stop = _number(timed_stop, "tightened_stop_pct")
    baseline = _number(config.get("initial_stop_pct", 1.2), "initial_stop_pct")
    records = []
    for index, source in work.iterrows():
        row = source.to_dict()
        stop, target = float(stops.loc[index]), float(targets.loc[index])
        row.update(native_stop_pct=stop, initial_stop_pct=stop, stop_pct=stop,
                   native_target_pct=target, target_pct=target, g8_baseline_filled=bool(source.filled),
                   g8_quantity_scale=min(1., baseline / stop) if risk_scale else 1.,
                   g8_exit_mode="SINGLE_EXIT", cost_pct=costs if source.filled else np.nan)
        if source.filled:
            sid = int(source.sid)
            if sid not in paths:
                raise ValueError(f"Missing execution path for sid={sid}")
            raw_index = float(source.entry_path_index)
            if not math.isfinite(raw_index) or raw_index != int(raw_index):
                raise ValueError(f"Invalid entry_path_index for sid={sid}")
            result = simulate_exit(
                paths[sid], int(raw_index), float(source.entry_price), source.side == "LONG",
                stop, target, tighten_after_minutes=delay,
                tightened_stop_pct=min(stop, timed_stop) if timed_stop is not None else None,
            )
            row.update(result)
            row["net_return_pct"] = result["gross_return_pct"] - costs
        else:
            row["gross_return_pct"] = np.nan
            row["net_return_pct"] = np.nan
        records.append(row)
    frame = pd.DataFrame(records, index=work.index) if records else work.copy()
    if not records:
        frame["g8_quantity_scale"] = pd.Series(dtype=float)
    # Prevent a caller from mistaking stale source allocations for new results.
    frame = frame.drop(columns=[name for name in frame if name.startswith("portfolio_")])
    return _size(frame, config)


def allocate_variant(book: pd.DataFrame, allowed: pd.Series, config: Mapping,
                     policy: str, cost_bps: float = 5.) -> pd.DataFrame:
    """Apply sid-aligned entry decisions and replay capital availability.

    Exit times from replay_exits drive capacity release. Entry decisions must
    already have been formed from evidence available at their confirmation.
    """
    _validate_orders(book)
    if not isinstance(allowed, pd.Series) or not allowed.index.is_unique or set(allowed.index) != set(book.sid):
        raise ValueError("allowed must have exactly one boolean decision per sid")
    decisions = _flags(allowed, "allowed")
    permitted = book.sid.map(decisions)
    work = book.copy(deep=True)
    original_fill = _flags(work.get("g8_baseline_filled", work.filled), "filled")
    work["g8_baseline_filled"] = original_fill
    work["g8_direction_allowed"] = permitted.to_numpy(bool)
    work["filled"] = original_fill & permitted
    cost = _number(cost_bps, "cost_bps", positive=False)
    work["cost_pct"] = cost / 100.
    work["net_return_pct"] = pd.to_numeric(work.gross_return_pct) - cost / 100.
    sized = _size(work, config)
    settings = ENGINE.v6.PortfolioConfig(
        portfolio_capital_rupees=_number(config["portfolio_capital_rs"], "portfolio_capital_rs"),
        max_positions=config.get("max_positions", 10),
        max_positions_per_symbol=config.get("max_positions_per_symbol"),
        max_gross_exposure_rupees=config.get("max_gross_exposure_rs"),
        max_open_risk_rupees=config.get("max_open_risk_rs"),
    )
    result, _ = ENGINE.v6.apply_portfolio_constraints(sized, settings)
    blocked = ~permitted.to_numpy(bool)
    result.loc[blocked, "portfolio_status"] = "G8_REGIME_BLOCKED"
    result.loc[blocked, "portfolio_reject_reason"] = policy
    result["research_policy"] = policy
    result["strategy"] = "V13-V10-G-8"
    result["strategy_version"] = config.get("variant_id", VERSION)
    if "strategy_fingerprint" in result:
        result["source_strategy_fingerprint"] = result.get("source_strategy_fingerprint", result.strategy_fingerprint)
    result["strategy_fingerprint"] = hashlib.sha256(json.dumps(
        dict(config=dict(config), policy=policy, cost_bps=cost), sort_keys=True, default=str
    ).encode()).hexdigest()
    result["scope"] = "BACKTEST_ONLY"
    result["execution_authority"] = False
    result["day"] = result.day.astype(str).str[:10]
    return result


def metrics(book: pd.DataFrame, days: Sequence) -> dict:
    """After-cost metrics, with zero-trade days retained in drawdown/day counts."""
    calendar = sorted({str(day)[:10] for day in days})
    cohort = book.loc[book.day.astype(str).str[:10].isin(calendar)]
    executed = _flags(cohort.portfolio_executed, "portfolio_executed")
    trades = cohort.loc[executed]
    pnl = pd.to_numeric(trades.portfolio_net_profit_rupees)
    if not np.isfinite(pnl).all():
        raise ValueError("Nonfinite executed P&L")
    daily = pnl.groupby(trades.day.astype(str).str[:10]).sum().reindex(calendar, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy()]
    wins, losses = int(pnl.gt(1e-8).sum()), int(pnl.lt(-1e-8).sum())
    negatives = float(-pnl.loc[pnl.lt(0)].sum())
    gains = float(pnl.loc[pnl.gt(0)].sum())
    reasons = trades.get("exit_reason", pd.Series("", index=trades.index)).fillna("")
    return dict(
        sessions=len(calendar), selected_orders=len(cohort),
        native_fills=int(_flags(cohort.get("g8_baseline_filled", cohort.filled), "filled").sum()),
        trades=len(trades), wins=wins, losses=losses, breakeven_trades=len(trades)-wins-losses,
        win_rate_pct=100. * wins / len(trades) if len(trades) else 0.,
        net_pnl_rs=float(pnl.sum()), gross_pnl_rs=float(trades.portfolio_gross_profit_rupees.sum()),
        cost_rs=float(trades.portfolio_cost_rupees.sum()),
        profitable_days=int(daily.gt(1e-8).sum()), losing_days=int(daily.lt(-1e-8).sum()),
        flat_days=int(daily.abs().le(1e-8).sum()), worst_day_rs=float(daily.min()) if len(daily) else 0.,
        best_day_rs=float(daily.max()) if len(daily) else 0.,
        losing_day_sum_abs_rs=float(-daily.loc[daily.lt(0)].sum()),
        negative_trade_sum_abs_rs=negatives, negative_trade_pnl_rs=-negatives,
        gross_winner_profit_rs=gains,
        daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
        max_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
        average_net_trade_rs=float(pnl.mean()) if len(pnl) else 0.,
        profit_factor=gains / negatives if negatives else None,
        stop_exits=int(reasons.isin(["STOP", "TIGHTENED_STOP", "PROTECTED_STOP"]).sum()),
        target_exits=int(reasons.eq("TARGET").sum()),
    )
