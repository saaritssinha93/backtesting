"""Past-session-only policy selection for the original G4 executed cohort.

This is deliberately a small policy selector, not a minute-level classifier.
Candidate return columns must come from causal simulations of a prespecified
catalogue. Future outcomes are permitted in the table but never in a fit.
No row is weighted more heavily merely because its trade lasted longer.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

VERSION = "G4-CONFIGURABLE-RETENTION-PAST-SESSION-SELECTOR-V1"
PARAMETERS = dict(
    initial_train_sessions=20, test_block_sessions=5,
    minimum_trades=40, minimum_winners=10, minimum_losers=8,
    minimum_changed_trades=3, minimum_winner_profit_retention=.95,
    setup_development_sessions=35, setup_minimum_trades=30,
)
_EPS = 1e-8


def _day(value):
    return pd.Timestamp(value).date().isoformat()


def _retention(value):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("minimum_winner_profit_retention must be a finite fraction in (0, 1]")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("minimum_winner_profit_retention must be a finite fraction in (0, 1]") from exc
    if not np.isfinite(result) or not 0 < result <= 1:
        raise ValueError("minimum_winner_profit_retention must be a finite fraction in (0, 1]")
    return result


def _parameters(retention):
    return dict(PARAMETERS, minimum_winner_profit_retention=retention)


def _prepare(metadata, returns, days):
    required = {"day", "setup_id", "side", "confirmation_ts"}
    if not required.issubset(metadata.columns):
        raise ValueError(f"Missing metadata fields: {sorted(required - set(metadata.columns))}")
    # The source ledger may be very wide and fragmented. Only point-in-time
    # grouping columns belong in selection; copying them also avoids mutation.
    columns = [name for name in metadata.columns if name in required or name == "sid"]
    meta = metadata.loc[:, columns].copy()
    if "sid" in meta.columns:
        meta = meta.set_index("sid", drop=True)
    if meta.index.has_duplicates or returns.index.has_duplicates:
        raise ValueError("Exactly one row per original executed sid is required")
    if set(meta.index) != set(returns.index):
        raise ValueError("Metadata and return sid sets differ")
    if returns.columns.has_duplicates or "baseline" not in returns.columns:
        raise ValueError("Unique policy columns including baseline are required")
    if not all(isinstance(name, str) for name in returns.columns):
        raise ValueError("Policy identifiers must be strings")
    matrix = returns.reindex(meta.index).astype(float).copy()
    if not np.isfinite(matrix.to_numpy()).all():
        raise ValueError("All original-cohort simulated monetary returns must be finite")
    ordered = [_day(value) for value in days]
    if len(set(ordered)) != len(ordered) or ordered != sorted(ordered):
        raise ValueError("Session days must be unique and chronological")
    meta["day"] = meta.day.map(_day)
    if not set(meta.day).issubset(ordered):
        raise ValueError("Trade day is absent from the ordered session list")
    if not meta.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Unknown trade side")
    groups = []
    for row in meta.itertuples():
        stamp = pd.Timestamp(row.confirmation_ts)
        if pd.isna(stamp):
            raise ValueError("Missing confirmation timestamp")
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert("Asia/Kolkata")
        if stamp.date().isoformat() != row.day:
            raise ValueError("Confirmation and trade day differ")
        minutes = stamp.hour * 60 + stamp.minute
        slot = "before_11" if minutes < 660 else "11_to_13" if minutes < 780 else "from_13"
        groups.append(f"{row.side}:{slot}")
    meta["group"] = groups
    meta.index.name = matrix.index.name = "sid"
    return meta, matrix, ordered


def _metrics(baseline, candidate):
    # A modeled cost-covering exit can leave sub-paise binary roundoff. Keep
    # the same monetary flat tolerance as the main research comparison.
    winners, losers = baseline > _EPS, baseline < -_EPS
    baseline_profit = float(baseline[winners].sum())
    changed = np.abs(candidate - baseline) > _EPS
    return dict(
        trades=int(len(baseline)), baseline_wins=int(winners.sum()),
        baseline_losses=int(losers.sum()),
        candidate_wins=int((candidate > _EPS).sum()),
        candidate_losses=int((candidate < -_EPS).sum()),
        baseline_net=float(baseline.sum()), candidate_net=float(candidate.sum()),
        net_delta=float((candidate - baseline).sum()),
        baseline_loss_amount=float(-baseline[baseline < 0].sum()),
        candidate_loss_amount=float(-candidate[candidate < 0].sum()),
        loss_amount_saved=float(-baseline[baseline < 0].sum() + candidate[candidate < 0].sum()),
        original_winner_profit=baseline_profit,
        retained_original_winner_profit=float(candidate[winners].sum()),
        winner_profit_retention=float(candidate[winners].sum() / baseline_profit) if baseline_profit > 0 else np.nan,
        winners_turned_losses=int((winners & (candidate < -_EPS)).sum()),
        losers_turned_winners=int((losers & (candidate > _EPS)).sum()),
        changed_trades=int(changed.sum()),
    )


def _fit(matrix, minimum_trades=40, *, minimum_winner_profit_retention=.95):
    """Fit only the rows explicitly supplied by the chronological caller."""
    retention = _retention(minimum_winner_profit_retention)
    baseline = matrix.baseline
    enough = (len(matrix) >= minimum_trades and (baseline > _EPS).sum() >= 10
              and (baseline < -_EPS).sum() >= 8)
    baseline_stats = _metrics(baseline, baseline)
    if not enough:
        return "baseline", dict(reason="insufficient_prior_trades", qualified_policies=0,
                                **baseline_stats)
    qualified = []
    for policy in sorted(set(matrix.columns) - {"baseline"}):
        stats = _metrics(baseline, matrix[policy])
        if (stats["winner_profit_retention"] >= retention - _EPS
                and stats["net_delta"] >= -_EPS
                and stats["candidate_losses"] < stats["baseline_losses"]
                and stats["candidate_wins"] > stats["baseline_wins"]
                and stats["loss_amount_saved"] > _EPS
                and stats["changed_trades"] >= 3):
            qualified.append((policy, stats))
    if not qualified:
        return "baseline", dict(reason="no_qualified_prior_policy", qualified_policies=0,
                                **baseline_stats)
    qualified.sort(key=lambda item: (-item[1]["loss_amount_saved"],
                                    -item[1]["net_delta"], item[0]))
    policy, stats = qualified[0]
    return policy, dict(reason="qualified_prior_policy", qualified_policies=len(qualified), **stats)


def _retention_guard(matrix, policy, *, minimum_winner_profit_retention=.95):
    retention = _retention(minimum_winner_profit_retention)
    if policy == "baseline":
        return True
    stats = _metrics(matrix.baseline, matrix[policy])
    return (stats["original_winner_profit"] > 0
            and stats["winner_profit_retention"] >= retention - _EPS)


def _pnl(matrix, policy_for_sid):
    positions = matrix.columns.get_indexer(policy_for_sid)
    if (positions < 0).any():
        raise AssertionError("Selected policy is absent from the simulated catalogue")
    return pd.Series(matrix.to_numpy()[np.arange(len(matrix)), positions],
                     index=matrix.index, name="adaptive_net_pnl_rs")


def select_adaptive(metadata: pd.DataFrame, returns: pd.DataFrame, days, *,
                    minimum_winner_profit_retention=.95) -> dict:
    """Expanding 20-session fit / 5-session prediction, without test selection.

    Return ``pnl`` and ``policy_for_sid`` cover every original trade. The first
    20 sessions retain baseline and are labelled warmup. Side/time groups use
    their own fit with sufficient observations; otherwise a global candidate
    may be used only after a group-specific historical winner-retention guard.
    A whole test block receives its policy before the first block session.
    The retention floor is explicit and defaults to the original 95% rule.
    """
    retention = _retention(minimum_winner_profit_retention)
    meta, matrix, ordered = _prepare(metadata, returns, days)
    choices = pd.Series("baseline", index=matrix.index, name="policy")
    assignments, audits = [], []
    for day in ordered[:20]:
        assignments.append(dict(day=day, group="all", policy="baseline", fold=-1,
                                train_start=None, train_end=None, reason="warmup_baseline"))
    for fold, start in enumerate(range(20, len(ordered), 5)):
        train_days, test_days = ordered[:start], ordered[start:start + 5]
        train_mask = meta.day.isin(train_days)
        train = matrix.loc[train_mask]
        global_policy, global_audit = _fit(train, minimum_winner_profit_retention=retention)
        fold_info = dict(fold=fold, train_start=train_days[0], train_end=train_days[-1],
                         test_start=test_days[0], test_end=test_days[-1],
                         train_sessions=len(train_days), test_sessions=len(test_days))
        audits.append(dict(**fold_info, group="all", policy=global_policy, **global_audit))
        test_meta = meta.loc[meta.day.isin(test_days)]
        for group in sorted(test_meta.group.unique()):
            subgroup = matrix.loc[train_mask & meta.group.eq(group)]
            group_policy, group_audit = _fit(subgroup, minimum_winner_profit_retention=retention)
            if group_audit["reason"] == "insufficient_prior_trades":
                if global_policy != "baseline" and _retention_guard(
                        subgroup, global_policy, minimum_winner_profit_retention=retention):
                    group_policy = global_policy
                    group_audit = dict(reason="global_fallback_group_retention_pass",
                                       qualified_policies=global_audit["qualified_policies"],
                                       **_metrics(subgroup.baseline, subgroup[group_policy]))
                elif global_policy != "baseline":
                    group_audit["reason"] = "global_fallback_group_retention_rejected"
            group_indices = test_meta.index[test_meta.group.eq(group)]
            choices.loc[group_indices] = group_policy
            audits.append(dict(**fold_info, group=group, policy=group_policy, **group_audit))
            for day in test_days:
                assignments.append(dict(day=day, group=group, policy=group_policy,
                                        **{k: fold_info[k] for k in ("fold", "train_start", "train_end")},
                                        reason=group_audit["reason"]))
    return dict(assignments=pd.DataFrame(assignments), pnl=_pnl(matrix, choices),
                policy_for_sid=choices, folds=pd.DataFrame(audits),
                metadata=meta, version=VERSION, parameters=_parameters(retention))


def select_setup_holdout(metadata: pd.DataFrame, returns: pd.DataFrame, days, *,
                         minimum_winner_profit_retention=.95) -> dict:
    """Fixed first-35-session setup fit; sparse setups retain original exits."""
    retention = _retention(minimum_winner_profit_retention)
    meta, matrix, ordered = _prepare(metadata, returns, days)
    choices = pd.Series("baseline", index=matrix.index, name="policy")
    train_days, test_days = ordered[:35], ordered[35:]
    assignments, audits = [], []
    if not test_days:
        return dict(assignments=pd.DataFrame(), pnl=_pnl(matrix, choices),
                    policy_for_sid=choices, folds=pd.DataFrame(), metadata=meta,
                    version=VERSION, parameters=_parameters(retention))
    for setup in sorted(meta.setup_id.unique()):
        in_setup = meta.setup_id.eq(setup)
        train = matrix.loc[in_setup & meta.day.isin(train_days)]
        policy, audit = _fit(train, minimum_trades=30, minimum_winner_profit_retention=retention)
        choices.loc[in_setup & meta.day.isin(test_days)] = policy
        audits.append(dict(group=setup, policy=policy, train_start=train_days[0],
                           train_end=train_days[-1], test_start=test_days[0],
                           test_end=test_days[-1], **audit))
        for day in test_days:
            assignments.append(dict(day=day, group=setup, policy=policy,
                                    train_start=train_days[0], train_end=train_days[-1],
                                    reason=audit["reason"]))
    return dict(assignments=pd.DataFrame(assignments), pnl=_pnl(matrix, choices),
                policy_for_sid=choices, folds=pd.DataFrame(audits), metadata=meta,
                version=VERSION, parameters=_parameters(retention))
