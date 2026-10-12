"""Retrospective PF diagnostics with causal expanding-date model selection.

All supplied historical partitions may already have been observed. These
functions never label them a fresh holdout, promote a profile, or send orders.
The profit factor numerator and denominator use cost-adjusted trade P&L.
Supplied net P&L must already include the caller's preregistered base costs;
for a 10bps base, the additional5/10bps reports represent15/20bps total.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np
import pandas as pd

RETROSPECTIVE_NOTE = "Historical dates already observed; walk-forward output is retrospective, not a fresh holdout."
PNL = "portfolio_net_profit_rupees"
EXPOSURE = "portfolio_trade_exposure_rupees"


def net_profit_factor(pnl) -> float | None:
    """Sum of positive net trade P&L / absolute sum of negative net P&L.

    No-loss and empty samples are undefined, never an infinite success score.
    Invalid numbers are rejected instead of silently deleting observations.
    """
    values = np.asarray(pnl, dtype=float)
    if values.ndim != 1 or not np.isfinite(values).all():
        raise ValueError("P&L must be a finite one-dimensional sequence")
    loss = float(-values[values < 0].sum())
    return float(values[values > 0].sum() / loss) if loss > 0 else None


def _days(days: Sequence) -> list[str]:
    result = [pd.Timestamp(day).date().isoformat() for day in days]
    if len(result) != len(set(result)):
        raise ValueError("Session dates must be unique")
    if result != sorted(result):
        raise ValueError("Session dates must be chronological")
    return result


def _executed(trades: pd.DataFrame, days: Sequence, additional_bps: float = 0.) -> pd.DataFrame:
    dates = _days(days)
    stress = float(additional_bps)
    if not np.isfinite(stress) or stress < 0:
        raise ValueError("Additional round-trip bps must be finite and nonnegative")
    required = {"day", "portfolio_executed", PNL, EXPOSURE}
    if required.difference(trades):
        raise ValueError(f"Missing ledger columns: {sorted(required.difference(trades))}")
    work = trades.copy()
    work["day"] = pd.to_datetime(work.day, errors="raise").dt.strftime("%Y-%m-%d")
    flags = work.portfolio_executed
    if not flags.astype(str).str.lower().isin({"true", "false"}).all():
        raise ValueError("Execution flags must be explicit booleans")
    work = work.loc[flags.astype(str).str.lower().eq("true") & work.day.isin(dates)].copy()
    pnl = pd.to_numeric(work[PNL], errors="raise").astype(float)
    exposure = pd.to_numeric(work[EXPOSURE], errors="raise").astype(float)
    if not np.isfinite(pnl).all() or not np.isfinite(exposure).all() or exposure.le(0).any():
        raise ValueError("Executed P&L and positive modeled exposure must be finite")
    work["adjusted_net_pnl"] = pnl - exposure * stress / 10000.
    return work


def day_metrics(trades: pd.DataFrame, days: Sequence, additional_bps: float = 0.) -> dict:
    """Trade PF, chronological daily drawdown, and winner concentration."""
    dates = _days(days)
    work = _executed(trades, dates, additional_bps)
    pnl = work.adjusted_net_pnl
    gain = float(pnl.loc[pnl.gt(0)].sum())
    loss = float(-pnl.loc[pnl.lt(0)].sum())
    daily = work.groupby("day").adjusted_net_pnl.sum().reindex(dates, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy(float)]
    winning_days = daily.loc[daily.gt(0)]
    winning_trades = pnl.loc[pnl.gt(0)].sort_values(ascending=False)
    return dict(sessions=len(dates), active_sessions=int(work.day.nunique()), trades=len(work),
        winning_trades=int(pnl.gt(0).sum()), losing_trades=int(pnl.lt(0).sum()),
        breakeven_trades=int(pnl.eq(0).sum()), net_pnl=float(pnl.sum()),
        gross_positive_net_pnl=gain, absolute_negative_net_pnl=loss,
        net_pf=net_profit_factor(pnl.to_numpy(float)),
        win_rate_pct=float(pnl.gt(0).mean() * 100) if len(work) else 0.,
        average_trade=float(pnl.mean()) if len(work) else 0.,
        daily_close_drawdown=float((np.maximum.accumulate(equity) - equity).max()),
        best_trade_fraction_of_gains=float(winning_trades.iloc[0] / gain) if gain else None,
        top5_trade_fraction_of_gains=float(winning_trades.head(5).sum() / gain) if gain else None,
        best_day_fraction_of_positive_days=float(winning_days.max() / winning_days.sum()) if len(winning_days) else None,
        net_without_best_trade=float(pnl.sum() - winning_trades.iloc[0]) if len(winning_trades) else float(pnl.sum()),
        additional_roundtrip_bps=float(additional_bps),
        additional_cost=float(work[EXPOSURE].sum() * additional_bps / 10000.))


def bootstrap_day_pf(trades: pd.DataFrame, days: Sequence, additional_bps: float = 0., *,
                     n_boot: int = 2000, seed: int = 1729, block_sessions: int = 5,
                     confidence: float = .95) -> dict:
    """Circular moving-session-block percentile interval, retaining day clusters.

    Entire days, including no-trade days, are sampled together in consecutive
    blocks. Positive no-loss draws are reported as unbounded upper mass, not
    silently removed to manufacture a finite interval. All-zero draws remain
    undefined; their fraction is reported and makes the interval incomplete.
    """
    dates = _days(days)
    if n_boot < 1 or block_sessions < 1 or not 0 < confidence < 1:
        raise ValueError("Positive bootstrap count/block size and confidence in (0,1) required")
    work = _executed(trades, dates, additional_bps)
    if not dates:
        return dict(lower=None, upper=None, upper_unbounded=False, n_boot=n_boot, seed=seed,
                    block_sessions=block_sessions, confidence=confidence,
                    undefined_fraction=1., no_loss_fraction=1., interval_complete=False)
    work["gain"] = work.adjusted_net_pnl.clip(lower=0)
    work["loss"] = -work.adjusted_net_pnl.clip(upper=0)
    daily = work.groupby("day")[["gain", "loss"]].sum().reindex(dates, fill_value=0.).to_numpy(float)
    n = len(dates)
    block = min(int(block_sessions), n)
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, n, size=(int(n_boot), int(np.ceil(n / block))))
    indices = ((starts[:, :, None] + np.arange(block)) % n).reshape(n_boot, -1)[:, :n]
    sums = daily[indices].sum(axis=1)
    positive_noloss = (sums[:, 1] == 0) & (sums[:, 0] > 0)
    all_zero = (sums[:, 1] == 0) & (sums[:, 0] == 0)
    ratios = np.divide(sums[:, 0], sums[:, 1], out=np.full(n_boot, np.inf), where=sums[:, 1] > 0)
    valid = ratios[~all_zero]
    if len(valid):
        alpha = (1. - confidence) / 2.
        lower, upper = np.quantile(valid, [alpha, 1. - alpha], method="inverted_cdf")
    else:
        lower = upper = np.nan
    return dict(lower=float(lower) if np.isfinite(lower) else None,
        upper=float(upper) if np.isfinite(upper) else None,
        upper_unbounded=bool(np.isposinf(upper)), lower_unbounded=bool(np.isposinf(lower)),
        n_boot=int(n_boot), seed=int(seed), block_sessions=block, confidence=float(confidence),
        undefined_fraction=float(all_zero.mean()), no_loss_fraction=float((positive_noloss | all_zero).mean()),
        interval_complete=bool(not all_zero.any()),
        method="CIRCULAR_MOVING_SESSION_BLOCK_PERCENTILE",
        note="Resampling uncertainty conditional on observed data; not independent validation or a multiple-search correction.")


def select_train_variant(metrics: pd.DataFrame, *, min_trades: int = 20, min_pf: float = 1.3,
                         min_losing_trades: int = 3) -> dict:
    """Select only TRAIN evidence; undefined PF cannot qualify.

    Input row order is the declared grid simplicity tie-break. TEST, ALL,
    validation and stressed rows are ignored when those labels are present.
    """
    rows = metrics.copy()
    if "split" in rows:
        rows = rows.loc[rows.split.eq("TRAIN")]
    if "additional_bps" in rows:
        rows = rows.loc[rows.additional_bps.eq(0)]
    if "additional_roundtrip_bps" in rows:
        rows = rows.loc[rows.additional_roundtrip_bps.eq(0)]
    if rows.variant.duplicated().any():
        raise ValueError("Expected one eligible TRAIN metric row per variant")
    options = []
    for order, row in enumerate(rows.itertuples(index=False)):
        pf = float(row.net_pf) if pd.notna(row.net_pf) else np.nan
        if (row.trades >= min_trades and row.losing_trades >= min_losing_trades
                and np.isfinite(row.net_pnl) and row.net_pnl > 0 and np.isfinite(pf) and pf >= min_pf):
            options.append(dict(variant=row.variant, train_pf=pf, train_net_pnl=float(row.net_pnl),
                                train_trades=int(row.trades), train_losing_trades=int(row.losing_trades),
                                grid_order=int(getattr(row, "grid_order", order))))
    winner = sorted(options, key=lambda x: (-x["train_pf"], -x["train_net_pnl"], x["grid_order"]))[0] if options else None
    return dict(selected_variant=winner["variant"] if winner else None, winner=winner,
                qualified_variants=len(options), candidates=len(rows), min_train_trades=int(min_trades),
                min_train_pf=float(min_pf), min_train_losing_trades=int(min_losing_trades), used_test=False,
                selection_rule="TRAIN PF, then TRAIN net P&L, then declared grid order",
                status="SELECTED" if winner else "NO_QUALIFIED_TRAIN_VARIANT")


def expanding_folds(days: Sequence, *, initial_train_sessions: int, test_sessions: int) -> list[dict]:
    """Date-only expanding folds; every evaluation date appears at most once."""
    dates = _days(days)
    if initial_train_sessions < 1 or test_sessions < 1:
        raise ValueError("Positive initial train and evaluation session counts required")
    return [dict(fold=i, train_days=dates[:start], test_days=dates[start:start + test_sessions])
            for i, start in enumerate(range(initial_train_sessions, len(dates), test_sessions))]


def walk_forward_select(ledgers: Mapping[str, pd.DataFrame], days: Sequence, *,
                        initial_train_sessions: int, test_sessions: int,
                        available_days: Sequence | None = None,
                        min_train_trades: int = 20, min_train_pf: float = 1.3,
                        min_train_losing_trades: int = 3) -> tuple[pd.DataFrame, list[dict]]:
    """Choose on earlier dates, append subsequent original ledger rows only.

    ``days`` is the full requested session calendar, including excluded dates.
    The shared ``available_days`` coverage filters rows INSIDE those fixed
    windows; missing dates never compress the calendar or move boundaries.
    An unqualified fold stays in cash and retains valid evaluation dates.
    """
    if not ledgers:
        raise ValueError("At least one predeclared variant ledger is required")
    dates = _days(days)
    available = set(_days(available_days)) if available_days is not None else set(dates)
    if not available.issubset(dates):
        raise ValueError("Available dates must belong to the requested calendar")
    folds = expanding_folds(dates, initial_train_sessions=initial_train_sessions, test_sessions=test_sessions)
    parts, decisions = [], []
    for fold in folds:
        training = [day for day in fold["train_days"] if day in available]
        evaluating = [day for day in fold["test_days"] if day in available]
        metrics = pd.DataFrame([dict(variant=name, **day_metrics(ledger, training)) for name, ledger in ledgers.items()])
        decision = select_train_variant(metrics, min_trades=min_train_trades, min_pf=min_train_pf,
                                        min_losing_trades=min_train_losing_trades)
        decisions.append({**fold, **decision, "evaluated_train_days": training, "evaluated_test_days": evaluating,
                          "retrospective": True, "fresh_holdout": False})
        chosen = decision["selected_variant"]
        if chosen is not None:
            labels = pd.to_datetime(ledgers[chosen].day).dt.strftime("%Y-%m-%d")
            part = ledgers[chosen].loc[labels.isin(evaluating)].copy()
            part["walk_forward_variant"], part["walk_forward_fold"] = chosen, fold["fold"]
            parts.append(part)
    empty = next(iter(ledgers.values())).iloc[:0].copy() if ledgers else pd.DataFrame(columns=["day", "portfolio_executed", PNL, EXPOSURE])
    return (pd.concat(parts, ignore_index=True) if parts else empty), decisions


def assess_pf_target(trades: pd.DataFrame, days: Sequence, *, target_pf: float = 2.,
                     min_trades: int = 50, min_sessions: int = 20, min_losing_trades: int = 5,
                     n_boot: int = 2000, seed: int = 1729, block_sessions: int = 5) -> dict:
    """Transparent target/sample flags, with +5/+10bps cost stress reported."""
    metrics = [day_metrics(trades, days, stress) for stress in (0., 5., 10.)]
    base = metrics[0]
    sufficient = (base["trades"] >= min_trades and base["sessions"] >= min_sessions
                  and base["losing_trades"] >= min_losing_trades)
    reaches = base["net_pf"] is not None and np.isfinite(base["net_pf"]) and base["net_pf"] >= target_pf
    interval = bootstrap_day_pf(trades, days, n_boot=n_boot, seed=seed, block_sessions=block_sessions)
    lower_above_one = interval["interval_complete"] and interval["lower"] is not None and interval["lower"] > 1.
    stress_pass = metrics[1]["net_pf"] is not None and np.isfinite(metrics[1]["net_pf"]) and metrics[1]["net_pf"] >= 1.5
    robust = bool(lower_above_one and stress_pass)
    return dict(target_pf=float(target_pf), minimum_trades=min_trades, minimum_evaluated_sessions=min_sessions,
        minimum_losing_trades=min_losing_trades, sample_sufficient=bool(sufficient),
        base_net_pf_reaches_target=bool(reaches), retrospective_target_and_sample_pass=bool(sufficient and reaches),
        cost_stress=metrics, bootstrap=interval,
        robustness=dict(pass_fixed_screen=robust, bootstrap_lower_above_1=bool(lower_above_one),
                        plus5bps_pf_at_least_1p5=bool(stress_pass),
                        rule="Complete 95% day-block bootstrap lower PF >1 and additional5bps net PF >=1.5"),
        retrospective_target_sample_and_robustness_pass=bool(sufficient and reaches and robust),
        execution_authority=False, default_profile_promoted=False, fresh_holdout=False,
        note=RETROSPECTIVE_NOTE)
