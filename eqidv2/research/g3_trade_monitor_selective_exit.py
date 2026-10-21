"""One frozen, interpretable exit-value experiment on reused G3 history.

This module never imports the mutable live strategy or changes an execution
configuration. A second threshold is an explicitly predeclared diagnostic of
the same fitted model, not another candidate chosen after evaluation.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from html import escape
import json
from pathlib import Path
import secrets
import shutil
import sys

import numpy as np
import pandas as pd

VERSION = "G3-SELECTIVE-EXIT-RIDGE-V1"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROTOCOL = ROOT / "configs/research/g3_selective_exit_protocol_v1.json"
DEFAULT_OUTPUT = Path("C:/TradingData/eqidv2/v13_v10_g_3_selective_exit_backtest")
KEYS = ["sid", "day", "decision_at_ist", "completed_bar_count"]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False), encoding="utf-8")


def stamps(values):
    return pd.to_datetime(values, utc=True, format="mixed", errors="raise")


def fit_ridge(x, target, features, penalty=10.0):
    """Fit weighted ridge; only the listed decision-time columns are accessed."""
    a = x[features].to_numpy(dtype=float)
    target = np.asarray(target, dtype=float)
    weight = (1 / x.groupby("sid").sid.transform("size")).to_numpy(float)
    if (len(a) == 0 or len(a) != len(target) or not np.isfinite(a).all()
            or not np.isfinite(target).all() or not np.isfinite(penalty) or penalty <= 0):
        raise ValueError("Invalid training data or ridge penalty")
    mean = np.average(a, axis=0, weights=weight)
    scale = np.sqrt(np.average((a-mean)**2, axis=0, weights=weight))
    scale = np.where(scale > 1e-12, scale, 1.0)
    z = (a-mean)/scale
    intercept = float(np.average(target, weights=weight))
    coef = np.linalg.solve(z.T @ (z*weight[:, None]) + penalty*np.eye(len(features)),
                           z.T @ (weight*(target-intercept)))
    return dict(features=list(features), mean=mean.tolist(), scale=scale.tolist(),
                coefficients=coef.tolist(), intercept=intercept, penalty=float(penalty),
                training_rows=len(x), training_trades=int(x.sid.nunique()))


def predict(model, x):
    a = x[model["features"]].to_numpy(float)
    if not np.isfinite(a).all():
        raise ValueError("Nonfinite prediction features")
    return ((a-np.array(model["mean"]))/np.array(model["scale"])) @ np.array(model["coefficients"]) + model["intercept"]


def calibrate_threshold(scores, labels, buffer=0.05):
    scores = np.asarray(scores, float)
    targets = labels.label_native_target.to_numpy(bool)
    if len(scores) != len(targets) or not np.isfinite(scores).all() or buffer < 0:
        raise ValueError("Invalid calibration")
    if not targets.any():
        return None
    return float(max(0.0, float(scores[targets].max())) + buffer)


def validate_inputs(x, y, baseline):
    if x.duplicated(KEYS).any() or y.duplicated(KEYS).any() or baseline.sid.duplicated().any():
        raise ValueError("Duplicate input identity")
    joined = x[KEYS].merge(y, on=KEYS, how="outer", validate="one_to_one", indicator=True)
    if not joined._merge.eq("both").all():
        raise ValueError("Predictor/label identities differ")
    # An outer merge can sort keys even with sort=False. Restore X's exact order
    # before applying chronological masks or dividing labels by per-row risk.
    y = x[KEYS].merge(y, on=KEYS, how="left", sort=False, validate="one_to_one")
    if not x[KEYS].reset_index(drop=True).equals(y[KEYS].reset_index(drop=True)):
        raise ValueError("Predictor/label order mismatch")
    b = baseline.set_index("sid")
    if not set(x.sid).issubset(set(b.index)):
        raise ValueError("Unknown executed trade")
    if not x.day.eq(x.sid.map(b.day)).all():
        raise ValueError("Decision day mismatch")
    if (not np.allclose(y.label_native_net_pnl_rs, y.sid.map(b.portfolio_net_profit_rupees))
            or not y.label_native_exit_reason.eq(y.sid.map(b.exit_reason)).all()):
        raise ValueError("Native outcome mismatch")
    exposure = y.sid.map(b.exposure_per_entry_rupees).to_numpy(float)
    if not np.allclose(y.label_extra_exit_cost_rs, exposure*.001):
        raise ValueError("Dataset must use ten additional exit basis points")
    expected = y.label_exit_now_net_pnl_rs-y.label_native_net_pnl_rs-y.label_extra_exit_cost_rs
    if not np.allclose(expected, y.label_exit_value_after_cost_rs):
        raise ValueError("Economic labels do not reconcile")
    if (not y.label_native_target.eq(y.label_native_exit_reason.eq("TARGET")).all()
            or not y.label_native_stop.eq(y.label_native_exit_reason.isin(["STOP", "TIGHTENED_STOP"])).all()
            or not y.label_native_winner.eq(y.label_native_net_pnl_rs.gt(0)).all()):
        raise ValueError("Outcome label flags do not reconcile")
    decision, executable, mature = (stamps(y[column]) for column in
        ["decision_at_ist", "label_executable_at_ist", "label_available_at_ist"])
    if not (executable.ge(decision) & mature.gt(executable)).all():
        raise ValueError("Noncausal decision or label chronology")
    return y


def assert_mature(labels, cutoff_day):
    cutoff = pd.Timestamp(cutoff_day, tz="Asia/Kolkata").tz_convert("UTC")
    if not stamps(labels.label_available_at_ist).lt(cutoff).all():
        raise ValueError("Training or calibration label not mature before scoring period")


def replay(baseline, predictions, labels, threshold, extra_bps=10):
    """Select using predictions only, then attach outcomes for accounting."""
    if not np.isfinite(extra_bps) or extra_bps < 0:
        raise ValueError("Invalid extra cost")
    if predictions.duplicated(KEYS).any():
        raise ValueError("Duplicate prediction identity")
    if not set(predictions.sid).issubset(set(baseline.sid)):
        raise ValueError("Prediction outside baseline cohort")
    active = predictions.iloc[:0].copy() if threshold is None else predictions.loc[predictions.score_initial_r.gt(threshold)].copy()
    active["_time"] = stamps(active.decision_at_ist)
    active = active.sort_values(["_time", "sid", "completed_bar_count"]).drop_duplicates("sid", keep="first").drop(columns="_time")
    chosen = active.merge(labels, on=KEYS, how="left", validate="one_to_one")
    if len(chosen) and chosen.label_exit_now_net_pnl_rs.isna().any():
        raise ValueError("Missing chosen economic label")
    keep = ["sid", "day", "tradingsymbol", "side", "setup_id", "entry_price", "entry_ts", "exit_ts", "exit_reason",
            "initial_stop_pct", "native_target_pct", "exposure_per_entry_rupees", "portfolio_cost_rupees", "portfolio_net_profit_rupees"]
    ledger = baseline[keep].copy().set_index("sid", drop=False)
    ledger["intervened"] = ledger.sid.isin(chosen.sid)
    ledger["policy_net_pnl_rs"] = ledger.portfolio_net_profit_rupees
    ledger["extra_exit_cost_rs"] = 0.0
    ledger["policy_exit_reason"] = ledger.exit_reason
    ledger["policy_exit_at_ist"] = ledger.exit_ts.astype(str)
    ledger["decision_at_ist"] = ""
    ledger["decision_completed_bars"] = np.nan
    ledger["score_initial_r"] = np.nan
    for row in chosen.itertuples(index=False):
        cost = float(ledger.at[row.sid, "exposure_per_entry_rupees"])*extra_bps/10000
        ledger.at[row.sid, "policy_net_pnl_rs"] = row.label_exit_now_net_pnl_rs-cost
        ledger.at[row.sid, "extra_exit_cost_rs"] = cost
        ledger.at[row.sid, "policy_exit_reason"] = "SELECTIVE_EXIT"
        ledger.at[row.sid, "policy_exit_at_ist"] = str(row.label_executable_at_ist)
        ledger.at[row.sid, "decision_at_ist"] = str(row.decision_at_ist)
        ledger.at[row.sid, "decision_completed_bars"] = row.completed_bar_count
        ledger.at[row.sid, "score_initial_r"] = row.score_initial_r
    ledger["incremental_net_rs"] = ledger.policy_net_pnl_rs-ledger.portfolio_net_profit_rupees
    return ledger.reset_index(drop=True).sort_values(["day", "sid"]).reset_index(drop=True)


def daily_results(ledger, days):
    d = ledger.groupby("day").agg(baseline_net_rs=("portfolio_net_profit_rupees", "sum"),
        policy_net_rs=("policy_net_pnl_rs", "sum"), exits=("intervened", "sum"))
    d = d.reindex(days, fill_value=0).reset_index(names="day")
    d["incremental_net_rs"] = d.policy_net_rs-d.baseline_net_rs
    return d


def drawdown(values):
    equity = np.r_[0.0, np.asarray(values, float).cumsum()]
    return float(np.max(np.maximum.accumulate(equity)-equity))


def metrics(ledger, days):
    d = daily_results(ledger, days)
    native = ledger.portfolio_net_profit_rupees
    policy = ledger.policy_net_pnl_rs
    delta = ledger.incremental_net_rs
    targets = ledger.exit_reason.eq("TARGET")
    stops = ledger.exit_reason.isin(["STOP", "TIGHTENED_STOP"])
    winners = native.gt(0)
    return dict(sessions=len(days), trades=len(ledger), baseline_net_rs=float(native.sum()),
        policy_net_rs=float(policy.sum()), incremental_net_rs=float(delta.sum()),
        early_exits=int(ledger.intervened.sum()), intervention_days=int(d.exits.gt(0).sum()),
        baseline_target_trades=int(targets.sum()), target_trades_touched=int((targets & ledger.intervened).sum()),
        target_profit_sacrificed_rs=float((-delta.loc[targets]).clip(lower=0).sum()),
        baseline_stop_trades=int(stops.sum()), remaining_native_stops=int((stops & ~ledger.intervened).sum()),
        stopped_trades_improved=int((stops & delta.gt(1e-8)).sum()),
        stop_losses_saved_rs=float(delta.loc[stops].clip(lower=0).sum()),
        stopped_trades_worsened=int((stops & delta.lt(-1e-8)).sum()),
        baseline_winners_harmed=int((winners & delta.lt(-1e-8)).sum()),
        winner_profit_sacrificed_rs=float((-delta.loc[winners]).clip(lower=0).sum()),
        winners_turned_nonpositive=int((winners & policy.le(0)).sum()),
        baseline_losing_trades=int(native.lt(0).sum()), policy_losing_trades=int(policy.lt(0).sum()),
        baseline_realized_loss_rs=float(-native.clip(upper=0).sum()),
        policy_realized_loss_rs=float(-policy.clip(upper=0).sum()),
        realized_loss_reduction_rs=float(-native.clip(upper=0).sum()+policy.clip(upper=0).sum()),
        baseline_daily_close_drawdown_rs=drawdown(d.baseline_net_rs),
        policy_daily_close_drawdown_rs=drawdown(d.policy_net_rs),
        baseline_losing_days=int(d.baseline_net_rs.lt(0).sum()),
        policy_losing_days=int(d.policy_net_rs.lt(0).sum()),
        additional_exit_cost_rs=float(ledger.extra_exit_cost_rs.sum()))


def block_interval(values, block_length=3, replicates=5000, seed=20261014):
    values = np.asarray(values, float)
    rng = np.random.default_rng(seed)
    n = len(values)
    starts = rng.integers(0, n, size=(replicates, int(np.ceil(n/block_length))))
    ix = (starts[:, :, None]+np.arange(block_length)) % n
    totals = values[ix.reshape(replicates, -1)[:, :n]].sum(axis=1)
    lo, hi = np.quantile(totals, [.025, .975])
    return dict(lower_95_rs=float(lo), upper_95_rs=float(hi), replicates=replicates,
                block_sessions=block_length, seed=seed,
                interpretation="Descriptive resampling of reused evaluation sessions, not a guarantee or adjustment for prior strategy selection.")


def gate_failures(calibration, evaluation, gates):
    failures = []
    if calibration["early_exits"] < gates["minimum_calibration_exits"]:
        failures.append("too_few_calibration_exits")
    if evaluation["early_exits"] < gates["minimum_evaluation_exits"]:
        failures.append("too_few_evaluation_exits")
    if evaluation["intervention_days"] < gates["minimum_evaluation_intervention_days"]:
        failures.append("too_few_evaluation_intervention_days")
    for name, result in [("calibration", calibration), ("evaluation", evaluation)]:
        if result["target_trades_touched"] > gates["maximum_target_interventions_calibration_and_evaluation"]:
            failures.append(name+"_target_winner_interference")
        if result["incremental_net_rs"] <= 0:
            failures.append(name+"_no_positive_incremental_net")
    if evaluation["realized_loss_reduction_rs"] <= 0:
        failures.append("evaluation_no_realized_loss_reduction")
    if evaluation["policy_daily_close_drawdown_rs"] > evaluation["baseline_daily_close_drawdown_rs"]+1e-6:
        failures.append("evaluation_daily_close_drawdown_worse")
    return failures


def run(protocol_path=DEFAULT_PROTOCOL, output_root=DEFAULT_OUTPUT):
    protocol_path = Path(protocol_path)
    protocol = json.loads(protocol_path.read_text())
    if protocol["version"] != VERSION or protocol["sessions"] != dict(train=25, calibration=10, evaluation=15):
        raise ValueError("Unexpected experiment protocol")
    source = Path(protocol["source_dataset"])
    source_manifest = json.loads((source/"manifest.json").read_text())
    for name, expected in source_manifest["files"].items():
        if sha(source/name) != expected:
            raise ValueError("Changed source artifact: "+name)
    source_summary = json.loads((source/"summary.json").read_text())
    baseline_path = Path(source_summary["source"])/"ledgers/baseline.csv"
    if sha(baseline_path) != source_summary["input_hashes"]["ledgers/baseline.csv"]:
        raise ValueError("Changed baseline")
    baseline = pd.read_csv(baseline_path)
    baseline = baseline.loc[baseline.portfolio_executed.eq(True)].copy()
    x = pd.read_parquet(source/"decision_time_predictors.parquet")
    y = validate_inputs(x, pd.read_parquet(source/"future_labels_do_not_use_as_features.parquet"), baseline)
    days = sorted(baseline.day.unique())
    if len(days) != 50 or not set(x.completed_bar_count).issubset({6, 15, 30}):
        raise ValueError("Unexpected session or decision coverage")
    periods = dict(training25=days[:25], calibration10=days[25:35], evaluation15=days[35:])
    train = x.day.isin(periods["training25"])
    cal = x.day.isin(periods["calibration10"])
    evaluation = x.day.isin(periods["evaluation15"])
    if set(x.loc[train, "sid"]) & set(x.loc[~train, "sid"]):
        raise ValueError("Trade split across periods")
    assert_mature(y.loc[train], periods["calibration10"][0])
    assert_mature(y.loc[cal], periods["evaluation15"][0])
    b = baseline.set_index("sid")
    risk = x.sid.map(b.exposure_per_entry_rupees)*x.sid.map(b.initial_stop_pct)/100
    if not np.isfinite(risk).all() or not risk.gt(0).all():
        raise ValueError("Invalid initial risk")
    output = Path(output_root)/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_"+secrets.token_hex(4)+"_ridge_exit")
    output.mkdir(parents=True, exist_ok=False)
    (output/"inputs").mkdir()
    shutil.copy2(protocol_path, output/"protocol.json")
    shutil.copy2(Path(__file__), output/Path(__file__).name)
    shutil.copy2(baseline_path, output/"inputs/baseline.csv")
    for name in ["decision_time_predictors.parquet", "future_labels_do_not_use_as_features.parquet"]:
        shutil.copy2(source/name, output/"inputs"/name)
    write_json(output/"protocol_lock.json", dict(version=VERSION, locked_at_utc=datetime.now(timezone.utc).isoformat(),
        protocol_sha256=sha(output/"protocol.json"), source_manifest_sha256=sha(source/"manifest.json"),
        input_hashes={p.name:sha(p) for p in (output/"inputs").iterdir()}, periods=periods,
        execution_authority=False, fresh_holdout=False))
    model = fit_ridge(x.loc[train], y.loc[train, "label_exit_value_after_cost_rs"]/risk.loc[train],
                      protocol["features"], protocol["ridge_penalty"])
    write_json(output/"model.json", model)
    calibration_scores = predict(model, x.loc[cal])
    threshold = calibrate_threshold(calibration_scores, y.loc[cal], protocol["protection_buffer_initial_r"])
    write_json(output/"selection_lock.json", dict(locked_at_utc=datetime.now(timezone.utc).isoformat(),
        model_sha256=sha(output/"model.json"), protocol_sha256=sha(output/"protocol.json"),
        protected_threshold_initial_r=threshold, target_calibration_rows=int(y.loc[cal, "label_native_target"].sum()),
        calibration_rows=int(cal.sum()), evaluation_excluded_from_fit_and_calibration=True,
        refit_after_calibration=False, number_of_models_fitted=1, execution_authority=False))
    # The evaluation is scored only after model and calibration threshold are saved.
    predictions = x[KEYS+["tradingsymbol", "side", "setup_id"]].copy()
    predictions["score_initial_r"] = predict(model, x)
    predictions["period"] = np.select([train, cal, evaluation], ["training25", "calibration10", "evaluation15"], default="INVALID")
    predictions.to_csv(output/"decision_predictions.csv", index=False)
    rows = []
    ledgers = {}
    for arm, cutoff in [("protected", threshold), ("unprotected_diagnostic", 0.0)]:
        for cost in protocol["cost_sensitivity_bps"]:
            ledger = replay(baseline, predictions, y, cutoff, cost)
            ledgers[(arm, cost)] = ledger
            ledger.to_csv(output/f"trades_{arm}_{cost}bps.csv", index=False)
            for period, subset_days in {**periods, "all50_mixed_fit_calibration_evaluation":days}.items():
                result = metrics(ledger.loc[ledger.day.isin(subset_days)], subset_days)
                rows.append(dict(arm=arm, extra_exit_cost_bps=cost, period=period, **result))
    results = pd.DataFrame(rows)
    results.to_csv(output/"policy_results.csv", index=False)
    primary = results.loc[results.arm.eq("protected") & results.extra_exit_cost_bps.eq(10)].set_index("period")
    final = primary.loc["evaluation15"].to_dict()
    calibration = primary.loc["calibration10"].to_dict()
    failures = gate_failures(calibration, final, protocol["historical_gates"])
    eval_days = periods["evaluation15"]
    primary_ledger = ledgers[("protected", 10)]
    daily = daily_results(primary_ledger.loc[primary_ledger.day.isin(eval_days)], eval_days)
    daily.to_csv(output/"evaluation_daily_comparison.csv", index=False)
    events = primary_ledger.loc[primary_ledger.day.isin(eval_days) & primary_ledger.intervened]
    events.to_csv(output/"evaluation_exit_events.csv", index=False)
    bootstrap = protocol["bootstrap"]
    interval = block_interval(daily.incremental_net_rs, bootstrap["circular_block_length"], bootstrap["replicates"], bootstrap["seed"])
    unprotected = results.loc[results.arm.eq("unprotected_diagnostic") & results.extra_exit_cost_bps.eq(10) & results.period.eq("evaluation15")].iloc[0].to_dict()
    summary = dict(version=VERSION, output=str(output), source_dataset=str(source),
        predictive_model_trained=True, number_of_models_fitted=1, configuration_promoted=False,
        execution_authority=False, fresh_holdout=False, protected_threshold_initial_r=threshold,
        baseline_executions=len(baseline), snapshots=len(x), trades_with_snapshots=int(x.sid.nunique()),
        periods=periods, evaluation=final, calibration=calibration,
        unprotected_evaluation_diagnostic=unprotected, evaluation_incremental_net_block_interval=interval,
        historical_gates_passed=not failures, historical_gate_failures=failures,
        conclusion="Historical gates passed; fresh forward confirmation still required." if not failures else "No qualified improvement. Retain baseline for this research comparison.",
        limitations=["History and underlying strategy were already researched; chronological evaluation is not a fresh strategy holdout.",
            "Only one fixed six-feature ridge model was tested; its failure or success does not establish the result for all exit models.",
            "Calibration zero target interference does not guarantee evaluation or future zero interference.",
            "Ten basis points are additional monitor-exit cost stress on top of existing base costs, not measured live spread and latency.",
            "Next-open timestamps equal decision timestamps, implying idealized zero processing latency. Extra cost stress is not latency validation. Native stop/target treatment is inherited from the frozen replay.",
            "No snapshots before six completed bars; 12 executed trades have no eligible snapshot and retain baseline outcomes.",
            "All50 mixes training, threshold calibration and evaluation and is not an out-of-sample result.",
            "Drawdown here uses session-close realized P&L and does not measure intraday mark-to-market risk.",
            "The unprotected arm is an ablation diagnostic of the same model, not a post-evaluation promotion candidate."])
    write_json(output/"summary.json", summary)
    write_json(output/"environment.json", dict(python=sys.version, numpy=np.__version__, pandas=pd.__version__))
    write_report(output, summary, results, daily, events, model)
    # Confirm that neither input files nor captured implementation changed mid-run.
    if sha(protocol_path) != sha(output/"protocol.json") or sha(Path(__file__)) != sha(output/Path(__file__).name):
        raise ValueError("Protocol or implementation changed during run")
    for name, expected in source_manifest["files"].items():
        if sha(source/name) != expected:
            raise ValueError("Source changed during run")
    manifest = {str(p.relative_to(output)).replace("\\", "/"):sha(p) for p in output.rglob("*") if p.is_file()}
    write_json(output/"manifest.json", dict(version=VERSION, files=manifest, execution_authority=False))
    print(json.dumps(dict(output=str(output), evaluation=final, diagnostic=unprotected, failures=failures), indent=2))
    return output


def write_report(output, summary, results, daily, events, model):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, 1, figsize=(11, 6), constrained_layout=True)
    positions = np.arange(len(daily)+1)
    axes[0].plot(positions, np.r_[0, daily.baseline_net_rs.cumsum()], label="Original baseline", linewidth=2)
    axes[0].plot(positions, np.r_[0, daily.policy_net_rs.cumsum()], label="Protected model, extra 10 bps", linestyle="--", linewidth=2)
    axes[0].set_ylabel("Cumulative net P&L (INR)")
    axes[0].legend()
    axes[0].set_title("Final 15 chronological sessions: reused history")
    axes[1].bar(np.arange(len(daily)), daily.incremental_net_rs,
                color=np.where(daily.incremental_net_rs.ge(0), "#16794c", "#b54439"))
    axes[1].set_ylabel("Daily change versus baseline (INR)")
    axes[1].set_xticks(np.arange(len(daily)), daily.day.str.slice(5), rotation=45)
    for axis in axes:
        axis.grid(axis="y", alpha=.2)
    fig.savefig(output/"evaluation_equity.png", dpi=140)
    plt.close(fig)
    table = lambda frame: frame.to_html(index=False, border=0, float_format=lambda v:f"{v:,.2f}")
    cols = ["arm", "period", "trades", "policy_net_rs", "incremental_net_rs", "early_exits", "target_trades_touched", "baseline_winners_harmed", "policy_losing_trades", "remaining_native_stops"]
    main = results.loc[results.extra_exit_cost_bps.eq(10), cols]
    costs = results.loc[results.period.eq("evaluation15"), ["arm", "extra_exit_cost_bps", "policy_net_rs", "incremental_net_rs", "target_trades_touched"]]
    event_cols = ["day", "tradingsymbol", "side", "decision_at_ist", "exit_reason", "portfolio_net_profit_rupees", "policy_net_pnl_rs", "incremental_net_rs"]
    coeff = pd.DataFrame(dict(feature=model["features"], training_mean=model["mean"], training_scale=model["scale"], standardized_coefficient=model["coefficients"]))
    limitations = "".join(f"<li>{escape(note)}</li>" for note in summary["limitations"])
    eval_result = summary["evaluation"]
    threshold = summary["protected_threshold_initial_r"]
    threshold_text = "ABSTAIN_ALL" if threshold is None else f"{threshold:.6f} initial R"
    html = f'''<!doctype html><html><head><meta charset="utf-8"><title>G3 selective exit model: backtest result</title>
<style>body{{font:16px system-ui;max-width:1180px;margin:36px auto;padding:0 22px;color:#243747;line-height:1.55}}h1,h2{{color:#14566b}}table{{border-collapse:collapse;font-size:13px;white-space:nowrap}}td,th{{padding:9px;border-bottom:1px solid #dce3e7;text-align:right}}th{{background:#edf3f5}}.scroll{{overflow-x:auto}}.notice{{padding:18px;background:#fff1d5}}img{{width:100%;height:auto}}li{{margin:8px 0}}</style></head><body>
<h1>G3 selective exit model: actual backtest result</h1>
<p class="notice"><strong>{escape(summary['conclusion'])}</strong> Final 15-session protected-model change after extra 10 bps: <strong>INR {eval_result['incremental_net_rs']:,.2f}</strong>. Early exits: {eval_result['early_exits']}; eventual target winners touched: {eval_result['target_trades_touched']}. No live or paper execution configuration was changed by this experiment.</p>
<p>One weighted ridge regression was trained on the first 25 sessions. The next 10 sessions set a target-protection score threshold. The last 15 sessions were scored only after saving the model and threshold. All history has been used in previous strategy research, so this is not fresh confirmation.</p>
<p>Target: the incremental net value of exiting at the cached next open, after an additional 10 bps, divided by initial risk in rupees. Original entries, sizing and portfolio admissions are preserved. An exit occurs at the first eligible 6-, 15- or 30-bar checkpoint above the score threshold. The frozen threshold is <strong>{threshold_text}</strong>; it is the maximum target-winner calibration score or zero, whichever is larger, plus 0.05 R. It is not a calibrated confidence bound.</p>
<h2>Results after additional 10 bps</h2><p>Training results are in-sample. Calibration sets the threshold. All50 combines these with evaluation and must not be interpreted as out-of-sample performance. The unprotected diagnostic uses the same fitted model with a zero threshold.</p><div class="scroll">{table(main)}</div>
<img src="evaluation_equity.png" alt="Evaluation cumulative P&amp;L and daily difference" />
<h2>Evaluation execution-cost sensitivity</h2><div class="scroll">{table(costs)}</div>
<h2>Every protected evaluation exit</h2><div class="scroll">{table(events[event_cols]) if len(events) else '<p>No protected evaluation exits occurred. Unchanged profit is not an improvement.</p>'}</div>
<h2>Historical qualification</h2><p>{escape(', '.join(summary['historical_gate_failures']) or 'Passed the prespecified historical gates; fresh forward confirmation remains required.')}</p>
<p>Three-session circular-block bootstrap 95% range for evaluation incremental net: INR {summary['evaluation_incremental_net_block_interval']['lower_95_rs']:,.2f} to INR {summary['evaluation_incremental_net_block_interval']['upper_95_rs']:,.2f}. This describes resampled historical sessions and does not account for unknown earlier strategy-search multiplicity.</p>
<h2>Fitted model</h2><p>Ridge penalty: 10. Each training trade has total weight one across its available checkpoints. Intercept: {model['intercept']:.6f} R. Coefficients are conditional linear associations, not causal explanations.</p><div class="scroll">{table(coeff)}</div>
<h2>Limits</h2><ul>{limitations}</ul>
<h2>Audit files</h2><p><a href="policy_results.csv">Complete metrics</a> · <a href="evaluation_exit_events.csv">Evaluation exits</a> · <a href="evaluation_daily_comparison.csv">Daily comparison</a> · <a href="decision_predictions.csv">Every prediction</a> · <a href="protocol.json">Frozen protocol</a> · <a href="selection_lock.json">Model and threshold lock</a> · <a href="summary.json">Summary</a> · <a href="manifest.json">Artifact hashes</a></p></body></html>'''
    (output/"report.html").write_text(html, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", type=Path, default=DEFAULT_PROTOCOL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    run(args.protocol, args.output_root)
