"""Reproducible retrospective robustness audit of the existing G3 exit grid.

This is reused-history research, never an untouched strategy holdout. No new
parameters are searched. Each rolling decision can use only its earlier train
and validation periods, and falls back to the baseline when evidence is weak.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
from html import escape
import json
from pathlib import Path
import secrets

import numpy as np
import pandas as pd

from research import g3_trade_monitor_parameter_search as search
from research.g3_trade_monitor_risk import risk_diagnostics, breakdowns
from eqidv2_runtime_paths import runtime_dir


VERSION = "G3-MONITOR-RETROSPECTIVE-ROBUSTNESS-V1"
SEED = 20261014
NOTIONAL_RS = 500_000.0


def walk_forward_folds(days, initial_train=20, validation_size=5, test_size=5):
    days = list(days)
    if (initial_train <= validation_size or validation_size < 1 or test_size < 1
            or days != sorted(set(days))):
        raise ValueError("Require unique chronological days and nonempty train/validation/test")
    folds = []
    for stop in range(initial_train, len(days), test_size):
        test = days[stop:stop + test_size]
        if len(test) != test_size:
            break  # Never silently shorten a preregistered test window.
        folds.append(dict(fold_id=len(folds) + 1,
            train_days=days[:stop - validation_size],
            validation_days=days[stop - validation_size:stop], test_days=test))
    return folds


def stress_delta(candidate_net, baseline_net, candidate_trades, baseline_trades,
                 monitor_exits, extra_bps, monitor_extra_bps=10,
                 notional_rs=NOTIONAL_RS):
    """Incremental round-trip costs apply symmetrically to BOTH strategies."""
    values = np.asarray([candidate_net, baseline_net, candidate_trades,
        baseline_trades, monitor_exits, extra_bps, monitor_extra_bps, notional_rs], float)
    if not np.isfinite(values).all() or (values[2:] < 0).any():
        raise ValueError("Costs, trade counts and notional must be finite and nonnegative")
    return float(candidate_net - baseline_net - notional_rs / 10_000 * (
        extra_bps * (candidate_trades - baseline_trades) + monitor_extra_bps * monitor_exits))


def _block_indices(length, block_size, samples, seed):
    if length < 1 or block_size < 1 or samples < 1:
        raise ValueError("Bootstrap requires observations and positive block/sample counts")
    block_size = min(int(block_size), length)
    starts = np.random.default_rng(seed).integers(0, length,
        size=(samples, int(np.ceil(length / block_size))))
    return ((starts[..., None] + np.arange(block_size)) % length).reshape(samples, -1)[:, :length]


def moving_block_bootstrap(daily_deltas, block_size=5, samples=5000, seed=SEED):
    values = np.asarray(daily_deltas, float)
    if values.ndim != 1 or not len(values) or not np.isfinite(values).all():
        raise ValueError("Bootstrap requires a nonempty finite daily series")
    means = values[_block_indices(len(values), block_size, samples, seed)].mean(axis=1)
    lower, upper = np.quantile(means, [.025, .975])
    return dict(n_days=len(values), mean=float(values.mean()), ci_lower=float(lower),
        ci_upper=float(upper), positive_probability=float((means > 0).mean()))


def _gate(row, min_exits):
    reasons = []
    required = ["monitor_exit_count", "wins", "executed_trades", "net_pnl_delta_vs_baseline_rs",
        "stress_10bps_net_pnl_delta_vs_baseline_rs", "realized_loss_reduction_rs",
        "max_drawdown_rs", "baseline_max_drawdown_rs", "losing_days",
        "baseline_losing_days", "newly_rejected"]
    if any(key not in row or not np.isfinite(float(row[key])) for key in required):
        return ["missing_or_nonfinite_metrics"]
    if row["monitor_exit_count"] < min_exits:
        reasons.append("insufficient_monitor_exits")
    if row["executed_trades"] <= 0 or row["wins"] < .7 * row["executed_trades"]:
        reasons.append("win_rate_below_70")
    for key in ("net_pnl_delta_vs_baseline_rs", "stress_10bps_net_pnl_delta_vs_baseline_rs",
                "realized_loss_reduction_rs"):
        if row[key] <= 0:
            reasons.append(key + "_not_positive")
    if row["max_drawdown_rs"] > row["baseline_max_drawdown_rs"] + 1e-6:
        reasons.append("daily_close_drawdown_worse")
    if row["losing_days"] > row["baseline_losing_days"]:
        reasons.append("more_losing_days")
    if row["newly_rejected"] > 0:
        reasons.append("baseline_execution_displaced")
    return reasons


def select_candidate(train_results, validation_results):
    train = {r["candidate_id"]: r for r in train_results}
    validation = {r["candidate_id"]: r for r in validation_results}
    if len(train) != len(train_results) or len(validation) != len(validation_results):
        raise ValueError("Duplicate candidate metrics")
    failures = {}
    for key in sorted(train):
        failures[key] = ["train:" + reason for reason in _gate(train[key], 5)]
        failures[key] += (["validation:" + reason for reason in _gate(validation[key], 2)]
            if key in validation else ["missing_validation"])
    eligible = sorted((key for key in train if not failures[key]),
        key=lambda key: (-train[key]["stress_10bps_net_pnl_delta_vs_baseline_rs"], key))
    return dict(selected_id=eligible[0] if eligible else "baseline",
        eligible_ids=eligible, failures=failures)


def _json(path, data):
    search.history.replay.atomic_json(path, search.history.replay.json_ready(data))


def _period_inputs(baseline, features, days):
    allowed = [str(day) for day in days]
    # Physically subset the scoring objects; no later-period rows reach selection.
    book = search.FastBook(baseline.loc[baseline.day.astype(str).isin(allowed)].copy())
    cache = search.DecisionCache(features.loc[features.day.astype(str).isin(allowed)].copy())
    return book, cache


def _candidate_metrics(book, outcome, days):
    metrics = book.metrics(outcome, days, book.baseline)
    original = book.metrics(book.baseline, days)
    metrics.update(baseline_max_drawdown_rs=original["max_drawdown_rs"],
        baseline_losing_days=original["losing_days"])
    return metrics


def _daily(book, outcome, days, candidate_id):
    rows = []
    for day in days:
        mask = book.day == str(day)
        accepted = outcome["accepted"] & mask
        base = book.baseline["accepted"] & mask
        rows.append(dict(day=str(day), candidate_id=candidate_id,
            net_pnl_rs=float(outcome["net"][accepted].sum()),
            baseline_net_pnl_rs=float(book.baseline["net"][base].sum()),
            executed_trades=int(accepted.sum()), baseline_executed_trades=int(base.sum()),
            monitor_exits=int((outcome["modified"] & accepted).sum())))
    for row in rows:
        row["delta_rs"] = row["net_pnl_rs"] - row["baseline_net_pnl_rs"]
        row["stress_delta_rs"] = stress_delta(row["net_pnl_rs"], row["baseline_net_pnl_rs"],
            row["executed_trades"], row["baseline_executed_trades"], row["monitor_exits"], 0)
    return rows


def delayed_events(events, features, minutes):
    """Hold a triggered exit pending for whole-minute execution-delay stress.

    Use observed later opens without re-testing the alert. An intervening
    native exit wins. Missing eligible opens invalidate the scenario instead
    of silently improving it by removing difficult fills.
    """
    if minutes < 0 or int(minutes) != minutes:
        raise ValueError("Delay must be a nonnegative integer number of minutes")
    if minutes == 0:
        return [dict(event) for event in events], 0
    lookup = {(int(row.sid), pd.Timestamp(row.next_bar_open_at_ist)): row
        for row in features.itertuples(index=False)}
    result, intervening = [], 0
    for event in events:
        opened = pd.Timestamp(event["next_bar_open_at_ist"]) + pd.Timedelta(minutes=minutes)
        if pd.Timestamp(event["original_exit_at_ist"]) <= opened:
            intervening += 1
            continue
        row = lookup.get((int(event["sid"]), opened))
        if row is None:
            raise ValueError(f"No verified delayed execution open for sid={event['sid']} at {opened}")
        result.append(dict(event, next_bar_open_at_ist=row.next_bar_open_at_ist,
            modeled_execution_at_ist=row.modeled_execution_at_ist,
            execution_path_index=int(row.execution_path_index),
            next_bar_open_price=float(row.next_bar_open_price)))
    return result, intervening


def _familywise(daily, ids, days):
    """Approximate max-statistic block bootstrap for the disclosed fixed family.

    This cannot adjust unknown earlier research trials or underlying setup
    selection. It is a reused-sample diagnostic, not a promotion probability.
    """
    matrix = daily.pivot(index="day", columns="candidate_id", values="stress_delta_rs").reindex(
        index=[str(day) for day in days], columns=ids).to_numpy(float)
    if not np.isfinite(matrix).all():
        raise ValueError("Candidate daily panel is incomplete")
    averages = matrix.mean(axis=0)
    indices = _block_indices(len(days), 5, 5000, SEED)
    # Chunking keeps the 50 x 64 research family small in memory.
    bootstrap = np.concatenate([matrix[chunk].mean(axis=1)
        for chunk in np.array_split(indices, 10)], axis=0)
    error = (bootstrap - averages).max(axis=1)
    cutoff = float(np.quantile(error, .95))
    return {key: dict(simultaneous_95_lower_daily_delta_rs=float(averages[i] - cutoff),
        familywise_centered_bootstrap_p=float((1 + (error >= averages[i]).sum()) / (len(error) + 1)))
        for i, key in enumerate(ids)}


def _plot(output, diagnostic_daily, rolling):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    frame = pd.DataFrame(diagnostic_daily)
    dates = pd.to_datetime(frame.day)
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
        gridspec_kw={"height_ratios": [2, 1]}, layout="constrained")
    axes[0].plot(dates, frame.delta_rs.cumsum(), color="#2657a5", lw=2,
        label="Source diagnostic: raw incremental P&L")
    axes[0].plot(dates, frame.stress_delta_rs.cumsum(), color="#bb650b", lw=2,
        label="Source diagnostic: after 10 bps exit stress")
    axes[0].plot(pd.to_datetime(rolling.day), rolling.stress_delta_rs.cumsum(),
        color="#1b785c", lw=2, ls="--", label="Rolling selection: stressed incremental P&L")
    axes[0].set_title("G3 exit changes versus the unchanged baseline", loc="left", fontsize=15)
    axes[0].set_ylabel("Cumulative difference (Rs)")
    axes[0].legend(loc="best", fontsize=9)
    axes[1].bar(dates, frame.stress_delta_rs, width=1.2,
        color=np.where(frame.stress_delta_rs >= 0, "#1b785c", "#b84646"))
    axes[1].set_ylabel("Daily stressed\ndifference (Rs)")
    axes[1].set_xlabel("Previously inspected sessions; this is retrospective evidence")
    axes[1].xaxis.set_major_locator(mdates.AutoDateLocator(minticks=5, maxticks=9))
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    for ax in axes:
        ax.axhline(0, color="#596473", lw=.7)
        ax.grid(axis="y", alpha=.2)
        ax.spines[["top", "right"]].set_visible(False)
        ax.ticklabel_format(axis="y", style="plain")
    fig.savefig(output / "cumulative_deltas.png", dpi=150)
    fig.savefig(output / "cumulative_deltas.svg")
    plt.close(fig)


def _execution_correction(before, after):
    if before is None and after is None:
        return []
    if before is None or after is None:
        raise ValueError("Both fixed-policy reference runs are required")
    before, after = Path(before).resolve(), Path(after).resolve()
    summaries = []
    for directory in (before, after):
        manifest = search._read(directory / "manifest.json")
        search.history._check_files(directory, manifest)
        summaries.append(search._read(directory / "summary.json"))
    if (summaries[0]["source"]["manifest_sha256"] != summaries[1]["source"]["manifest_sha256"]
            or summaries[0]["days"] != summaries[1]["days"]):
        raise ValueError("Execution correction references must share source and sessions")
    frames = [search.read_frozen_csv(p / "policy_results.csv") for p in (before, after)]
    joined = frames[0][["period", "policy", "net_pnl_rs"]].merge(
        frames[1][["period", "policy", "net_pnl_rs"]], on=["period", "policy"],
        validate="one_to_one", suffixes=("_before", "_corrected"))
    if len(joined) != len(frames[0]) or len(joined) != len(frames[1]):
        raise ValueError("Execution correction policy inventories differ")
    joined["correction_delta_rs"] = joined.net_pnl_rs_corrected - joined.net_pnl_rs_before
    if not np.allclose(joined.loc[joined.policy.eq("baseline"), "correction_delta_rs"], 0, atol=1e-6):
        raise ValueError("Execution correction unexpectedly changed the native baseline")
    return joined.to_dict("records")


def run(source, output_root, *, fixed_before=None, fixed_after=None):
    source = Path(source).resolve()
    output_root = Path(output_root).resolve()
    if output_root == source or output_root.is_relative_to(source):
        raise ValueError("Robustness output must be separate from its immutable source")
    source_summary, source_manifest = search.verified_package(source)
    source_hash = search.history.sha256(source / "manifest.json")
    code_paths = [Path(__file__), Path(search.__file__), Path(search.fixed.__file__),
        Path(__file__).with_name("g3_trade_monitor_risk.py")]
    code_proof = {str(p.resolve()): search.history.sha256(p) for p in code_paths}
    days = [date.fromisoformat(day) for day in source_summary["days"]]
    execution_correction = _execution_correction(fixed_before, fixed_after)
    baseline = search.read_frozen_csv(source / "ledgers/baseline.csv")
    features = search.read_frozen_csv(source / "decision_features.csv")
    candidates = search.candidate_registry()
    lookup = {c["candidate_id"]: c for c in candidates}
    now = datetime.now(search.history.contract.IST)
    output = output_root / f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_robustness"
    output.mkdir(parents=True, exist_ok=False)
    snapshots = output / "source_code"
    snapshots.mkdir()
    snapshot_index = {}
    for filename, digest in {**source_manifest["code_sha256"], **code_proof}.items():
        path = Path(filename)
        if search.history.sha256(path) != digest:
            raise ValueError(f"Research source changed since replay: {path.name}")
        snapshot = snapshots / f"{digest[:12]}_{path.name}"
        snapshot.write_bytes(path.read_bytes())
        snapshot_index[filename] = dict(sha256=digest, snapshot=snapshot.relative_to(output).as_posix())
    _json(output / "source_code_index.json", snapshot_index)
    protocol = dict(version=VERSION, scope="BACKTEST_ONLY", execution_authority=False,
        configuration_promoted=False, fresh_strategy_holdout=False,
        history_status="REUSED_AND_PREVIOUSLY_INSPECTED_EXPLORATORY_HISTORY",
        source_run=str(source), source_manifest_sha256=source_hash,
        existing_coarse_candidates=len(candidates), new_threshold_candidates=0,
        initial_development_sessions=20, trailing_validation_sessions=5,
        test_sessions_per_fold=5, training_expands=True,
        selection="TRAIN_STRESSED_DELTA_RANKING_WITH_INDEPENDENT_VALIDATION_GATE",
        minimum_train_monitor_exits=5, minimum_validation_monitor_exits=2,
        gates="Positive raw/stressed delta and loss reduction; win>=70%; no worse daily DD or losing-day count; no displaced baseline execution",
        symmetric_extra_round_trip_cost_bps=[0, 5, 10, 20], monitor_extra_exit_bps=10,
        locked_candidate_execution_delay_stress_minutes=[0, 1, 2],
        bootstrap=dict(block_days=5, samples=5000, seed=SEED,
            interpretation="DESCRIPTIVE_REUSED_HISTORY_NOT_POSTERIOR_OR_UNTOUCHED_TEST"),
        drawdown_warning="DAILY_CLOSE_AND_REALIZED_EXIT_DRAWDOWN_ARE_NOT_MARK_TO_MARKET",
        folds=walk_forward_folds(days), candidates=candidates)
    protocol["fixed_policy_references"] = dict(before=str(fixed_before) if fixed_before else None,
        after=str(fixed_after) if fixed_after else None)
    _json(output / "protocol.json", protocol)  # Write protocol before any new scoring.
    locks, metrics_rows, rolling = [], [], []
    for fold in protocol["folds"]:
        scoped_results = {}
        for part in ("train", "validation"):
            scoped_days = fold[part + "_days"]
            book, cache = _period_inputs(baseline, features, scoped_days)
            scoped_results[part] = []
            for candidate in candidates:
                outcome = book.outcome(cache.events(candidate))
                row = dict(candidate_id=candidate["candidate_id"],
                    **_candidate_metrics(book, outcome, scoped_days))
                scoped_results[part].append(row)
                metrics_rows.append(dict(fold_id=fold["fold_id"], period=part, **row))
        selection = select_candidate(scoped_results["train"], scoped_results["validation"])
        lock = dict(**fold, **selection, fresh_holdout=False)
        _json(output / f"fold_{fold['fold_id']:02}_lock.json", lock)
        locks.append(lock)
        # Test rows enter an outcome object only after that fold's lock is on disk.
        book, cache = _period_inputs(baseline, features, fold["test_days"])
        key = selection["selected_id"]
        outcome = book.outcome([] if key == "baseline" else cache.events(lookup[key]))
        rows = _daily(book, outcome, fold["test_days"], key)
        rolling.extend(dict(fold_id=fold["fold_id"], **row) for row in rows)
        print(f"Rolling fold {fold['fold_id']}: {key}; net delta Rs{sum(r['delta_rs'] for r in rows):,.2f}", flush=True)
    rolling_frame = pd.DataFrame(rolling)
    pd.DataFrame(metrics_rows).to_csv(output / "fold_selection_metrics.csv", index=False)
    rolling_frame.to_csv(output / "walk_forward_daily.csv", index=False)
    pd.DataFrame([{key: value for key, value in lock.items() if key != "failures"}
        for lock in locks]).to_csv(output / "fold_decisions.csv", index=False)

    # Full-history candidate comparisons are diagnostics only, after all fold locks.
    book, cache = _period_inputs(baseline, features, days)
    static_rows, candidate_rows, cost_rows = [], [], []
    for candidate in candidates:
        key = candidate["candidate_id"]
        outcome = book.outcome(cache.events(candidate))
        rows = _daily(book, outcome, days, key)
        static_rows.extend(rows)
        metrics = _candidate_metrics(book, outcome, days)
        delta = np.asarray([row["delta_rs"] for row in rows])
        stressed = np.asarray([row["stress_delta_rs"] for row in rows])
        boot = moving_block_bootstrap(stressed)
        candidate_rows.append(dict(candidate_id=key, family=candidate["family"], **metrics,
            positive_delta_days=int((delta > 1e-8).sum()), negative_delta_days=int((delta < -1e-8).sum()),
            unchanged_days=int((abs(delta) <= 1e-8).sum()), best_day_delta_rs=float(delta.max()),
            raw_delta_excluding_best_day_rs=float(delta.sum() - max(0, delta.max())),
            stressed_delta_excluding_best_day_rs=float(stressed.sum() - max(0, stressed.max())),
            stressed_mean_daily_delta_rs=boot["mean"], block95_lower_daily_rs=boot["ci_lower"],
            block95_upper_daily_rs=boot["ci_upper"], bootstrap_positive_fraction=boot["positive_probability"]))
        for extra in protocol["symmetric_extra_round_trip_cost_bps"]:
            candidate_net = sum(r["net_pnl_rs"] for r in rows)
            baseline_net = sum(r["baseline_net_pnl_rs"] for r in rows)
            executions = sum(r["executed_trades"] for r in rows)
            base_executions = sum(r["baseline_executed_trades"] for r in rows)
            exits = sum(r["monitor_exits"] for r in rows)
            cost_rows.append(dict(candidate_id=key, extra_round_trip_bps=extra,
                monitor_extra_exit_bps=10, baseline_net_rs=baseline_net - extra * NOTIONAL_RS / 10000 * base_executions,
                candidate_net_rs=candidate_net - NOTIONAL_RS / 10000 * (extra * executions + 10 * exits),
                delta_rs=stress_delta(candidate_net, baseline_net, executions, base_executions, exits, extra)))
    static = pd.DataFrame(static_rows)
    family = _familywise(static, list(lookup), days)
    for row in candidate_rows:
        row.update(family[row["candidate_id"]])
    result = pd.DataFrame(candidate_rows).sort_values(
        ["stress_10bps_net_pnl_delta_vs_baseline_rs", "candidate_id"], ascending=[False, True])
    static.to_csv(output / "all_candidate_daily_diagnostics.csv", index=False)
    result.to_csv(output / "candidate_robustness.csv", index=False)
    if execution_correction:
        pd.DataFrame(execution_correction).to_csv(output / "execution_correction_comparison.csv", index=False)
    pd.DataFrame(cost_rows).to_csv(output / "cost_sensitivity.csv", index=False)
    source_lock = search._read(source / "selection_lock.json")
    locked = source_lock["locked_candidate"]
    latency_rows = []
    original_events = cache.events(locked) if locked is not None else []
    diagnostic_daily = _daily(book, book.outcome(original_events), days,
        source_lock["locked_candidate_id"] or "baseline")
    _plot(output, diagnostic_daily, rolling_frame)
    for minutes in protocol["locked_candidate_execution_delay_stress_minutes"]:
        try:
            events, intervening = delayed_events(original_events, features, minutes)
        except ValueError as exc:
            latency_rows.append(dict(extra_delay_minutes=minutes, supported=False, error=str(exc)))
            continue
        outcome = book.outcome(events)
        for period, scoped_days in (("all", days), ("last15_reused_history", days[-15:])):
            latency_rows.append(dict(extra_delay_minutes=minutes, supported=True, period=period,
                candidate_id=source_lock["locked_candidate_id"],
                intervening_native_exits_all_history=intervening,
                **_candidate_metrics(book, outcome, scoped_days)))
    pd.DataFrame(latency_rows).to_csv(output / "execution_delay_stress.csv", index=False)
    risk = risk_diagnostics(baseline)
    selected = search.read_frozen_csv(source / "ledgers/locked_candidate.csv")
    risk_selected = risk_diagnostics(selected)
    _json(output / "risk_diagnostics.json", dict(baseline=risk, source_diagnostic_candidate=risk_selected))
    for name, frame in breakdowns(baseline).items():
        frame.to_csv(output / f"baseline_{name}_diagnostics.csv", index=False)
    best = result.iloc[0].to_dict()
    summary = dict(version=VERSION, source_run=str(source), sessions=len(days),
        scope="BACKTEST_ONLY", execution_authority=False, configuration_promoted=False,
        fresh_strategy_holdout=False, recommendation="RETAIN_BASELINE_PENDING_FRESH_FORWARD_EVIDENCE",
        candidate_count=len(candidates), folds=len(locks),
        folds_selecting_baseline=sum(lock["selected_id"] == "baseline" for lock in locks),
        walk_forward_sessions=len(rolling_frame),
        walk_forward_raw_delta_rs=float(rolling_frame.delta_rs.sum()),
        walk_forward_stressed_delta_rs=float(rolling_frame.stress_delta_rs.sum()),
        walk_forward_bootstrap=moving_block_bootstrap(rolling_frame.stress_delta_rs),
        best_full_sample_diagnostic=best,
        candidates_with_positive_raw_delta=int((result.net_pnl_delta_vs_baseline_rs > 0).sum()),
        candidates_with_positive_stressed_delta=int((result.stress_10bps_net_pnl_delta_vs_baseline_rs > 0).sum()),
        candidates_with_positive_simultaneous_lower_bound=int((result.simultaneous_95_lower_daily_delta_rs > 0).sum()),
        baseline_risk=risk, source_candidate_risk=risk_selected,
        execution_correction=execution_correction,
        source_locked_candidate_execution_delay_stress=latency_rows,
        limitations=["All 50 sessions and underlying setups were inspected in earlier research.",
            "The source parameter search preloads all-history features: its lock controls selection, not physical data access. This rolling audit subsets scoring objects by period.",
            "The disclosed family bootstrap does not account for unknown earlier trials or source setup selection.",
            "Minute OHLC cannot validate 15-second polling latency, queue position or actual fills.",
            "Fixed fractional notional and five-times leverage are inherited assumptions, not broker fill validation.",
            "Reported drawdowns exclude open-position mark-to-market risk.",
            "Five bps base round-trip costs are inherited, not a verified current broker fee schedule.",
            "The existing signal universe, data exclusions, and native stop-first OHLC convention remain inherited."])
    _json(output / "summary.json", summary)
    _report(output, summary, locks, result, pd.DataFrame(cost_rows))
    if search.history.sha256(source / "manifest.json") != source_hash:
        raise ValueError("Source manifest changed during robustness audit")
    if code_proof != {str(p.resolve()): search.history.sha256(p) for p in code_paths}:
        raise ValueError("Research code changed during robustness audit")
    artifacts = sorted(p for p in output.rglob("*") if p.is_file())
    _json(output / "manifest.json", dict(version=VERSION, execution_authority=False,
        source_manifest_sha256=source_hash, code_sha256=code_proof,
        files={p.relative_to(output).as_posix(): search.history.sha256(p) for p in artifacts}))
    for name, digest in search._read(output / "manifest.json")["files"].items():
        if search.history.sha256(output / name) != digest:
            raise ValueError("Output hash verification failed")
    print(json.dumps(search.history.replay.json_ready(summary), indent=2), flush=True)
    print(output, flush=True)
    return output, summary


def _report(output, summary, locks, results, costs):
    best = summary["best_full_sample_diagnostic"]
    folds = pd.DataFrame([dict(fold=lock["fold_id"], train_end=str(lock["train_days"][-1]),
        validation_end=str(lock["validation_days"][-1]), test_start=str(lock["test_days"][0]),
        test_end=str(lock["test_days"][-1]), selected=lock["selected_id"],
        eligible=len(lock["eligible_ids"])) for lock in locks])
    columns = ["candidate_id", "monitor_exit_count", "net_pnl_delta_vs_baseline_rs",
        "stress_10bps_net_pnl_delta_vs_baseline_rs", "block95_lower_daily_rs", "block95_upper_daily_rs",
        "simultaneous_95_lower_daily_delta_rs", "stressed_delta_excluding_best_day_rs"]
    tables = results[columns].head(15).to_html(index=False, float_format=lambda x: f"{x:,.2f}")
    risks = pd.DataFrame([dict(policy="baseline", **summary["baseline_risk"]),
        dict(policy="source diagnostic candidate", **summary["source_candidate_risk"])]).to_html(index=False)
    correction = pd.DataFrame(summary["execution_correction"])
    correction_html = ("<h2>Execution-timing correction</h2><p>A later intrabar target or stop no longer cancels an advisory already due at the next open. These are corrections to the simulation, not strategy gains. Native baseline P&amp;L is unchanged.</p><div class=\"scroll\">" +
        correction.loc[correction.period.eq("all")].to_html(index=False, float_format=lambda x:f"{x:,.2f}") + "</div>") if len(correction) else ""
    latency = pd.DataFrame(summary["source_locked_candidate_execution_delay_stress"])
    latency_cols = [c for c in ("extra_delay_minutes", "supported", "period", "monitor_exit_count",
        "net_pnl_delta_vs_baseline_rs", "stress_10bps_net_pnl_delta_vs_baseline_rs", "error") if c in latency]
    body = f"""<!doctype html><html lang="en"><meta charset="utf-8"><title>G3 credible improvement audit</title>
<style>body{{font:16px/1.5 system-ui,sans-serif;max-width:1400px;margin:35px auto;padding:0 22px;color:#172132;background:#f5f7fa}}h1,h2{{line-height:1.2}}table{{border-collapse:collapse;font-size:12px;background:white}}td,th{{border:1px solid #d2d9e3;padding:8px;text-align:right}}th{{background:#e8edf5}}.scroll{{overflow:auto}}.note{{background:#fff2cd;padding:18px;border-left:5px solid #ad7600}}a{{color:#1456a0}}</style>
<h1>G3 exit research: credibility before promotion</h1><p class="note"><b>Retain baseline.</b> These are retrospective results on previously inspected history. No live or paper configuration was changed. No fresh strategy holdout exists in this dataset.</p>
<p>{summary['candidate_count']} existing threshold candidates; {summary['folds']} rolling tests; baseline selected in {summary['folds_selecting_baseline']} folds. Rolling net delta: ₹{summary['walk_forward_raw_delta_rs']:,.2f}; after additional monitor execution stress: ₹{summary['walk_forward_stressed_delta_rs']:,.2f}.</p>
{correction_html}
<p><img src="cumulative_deltas.png" alt="Cumulative and daily incremental P&amp;L against baseline" style="width:100%;max-width:1200px"></p>
<h2>Locked rolling decisions</h2><p>Each fold ranks only earlier training data, gates on the following five validation sessions, writes its lock, then evaluates the next five sessions. Earlier test sessions become available to subsequent folds. This tests the selection procedure; it does not erase historical setup-selection bias.</p><div class="scroll">{folds.to_html(index=False)}</div>
<h2>Full-sample diagnostics, not a recommendation</h2><p>Best observed raw delta: ₹{best['net_pnl_delta_vs_baseline_rs']:,.2f}; stressed delta: ₹{best['stress_10bps_net_pnl_delta_vs_baseline_rs']:,.2f}. {summary['candidates_with_positive_simultaneous_lower_bound']} of {summary['candidate_count']} candidates have a positive simultaneous lower bound in the disclosed grid. Correlated-day bootstrap intervals describe this reused sample and are not a probability of future profitability.</p><div class="scroll">{tables}</div>
<h2>Costs and concentration</h2><p>Base costs remain five bps per round trip. Additional round-trip costs are charged to both strategies; a further ten bps is charged to each advisory exit. At ₹5 lakh notional, ten bps costs ₹500. The last table column above removes the best positive day from each candidate's stressed delta.</p><div class="scroll">{costs.loc[costs.candidate_id.eq(best['candidate_id'])].to_html(index=False,float_format=lambda x:f'{x:,.2f}')}</div>
<h2>Drawdown labels matter</h2><p>Daily close drawdown can hide losses within the trading day. Realized-exit drawdown aggregates simultaneous exits to avoid arbitrary order effects. Neither measure includes open-position mark-to-market losses.</p><div class="scroll">{risks}</div>
<h2>Execution delay stress</h2><p>The source development-locked diagnostic candidate is also delayed by one and two whole minutes, using actual later opens. Alerts remain pending without re-testing their conditions; intervening native exits take precedence. Missing eligible prices invalidate a scenario. These are stress cases, not a measured broker latency distribution.</p><div class="scroll">{latency[latency_cols].to_html(index=False, float_format=lambda x:f'{x:,.2f}')}</div>
<h2>Limits and next evidence</h2><ul>{''.join('<li>'+escape(item)+'</li>' for item in summary['limitations'])}</ul><p>Freeze the corrected implementation and evaluate subsequent complete sessions without retuning. Keep all losing and excluded-data sessions visible; reconcile costs, order latency and actual fills before considering promotion.</p>
<p><a href="candidate_robustness.csv">All candidate diagnostics</a> · <a href="walk_forward_daily.csv">Rolling daily results</a> · <a href="cost_sensitivity.csv">Cost scenarios</a> · <a href="protocol.json">Frozen protocol</a> · <a href="manifest.json">Hashes</a></p>
<p>Method reference: <a href="https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf">Bailey and López de Prado: selection bias and backtest overfitting</a>. No deflated Sharpe claim is made because the complete historical trial count is unknown.</p></html>"""
    (output / "report.html").write_text(body, encoding="utf-8")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Corrected sealed parameter-search run")
    parser.add_argument("--fixed-before", type=Path, help="Original fixed-policy run for timing correction comparison")
    parser.add_argument("--fixed-after", type=Path, help="Corrected fixed-policy run on identical source")
    parser.add_argument("--output-root", type=Path,
        default=runtime_dir("v13_v10_g_3_trade_monitor_robustness"))
    args = parser.parse_args(argv)
    run(args.source, args.output_root, fixed_before=args.fixed_before, fixed_after=args.fixed_after)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
