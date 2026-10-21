"""Diagnostic foundation for protecting target winners during exit research.

Creates decision-time predictors separately from future economic labels. It
does not fit, select, promote, or execute an exit policy. All history is reused.
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

import numpy as np
import pandas as pd

VERSION = "G3-TARGET-PRESERVATION-DATASET-V1"
LANDMARKS = (6, 15, 30)
DEFAULT_SOURCE = Path("C:/TradingData/eqidv2/v13_v10_g_3_trade_monitor_oi_research/20261014T100121179340_bb898836_oi_exits")
DEFAULT_OUTPUT = Path("C:/TradingData/eqidv2/v13_v10_g_3_exit_design")
MODEL_FEATURES = ["completed_bar_count", "consecutive_adverse_closes", "momentum_5m_pct",
    "signed_move_pct", "mfe_pct", "giveback_pct", "native_target_pct", "initial_stop_pct",
    "elapsed_minutes", "signed_move_initial_r", "mfe_initial_r", "momentum_5m_initial_r"]
METADATA = ["sid", "day", "source_signal_id", "setup_id", "tradingsymbol", "side", "decision_at_ist"]
SOURCES = [
    dict(title="Sweeney: Maximum Adverse Excursion (publisher description and contents)",
        url="https://www.wiley-vch.de/en/areas-interest/finance-economics-law/maximum-adverse-excursion-978-0-471-14152-5",
        lesson="Compare winners' adverse excursions with failures; examine profit trade-offs and volatility. The publisher's outline was reviewed, not the full book."),
    dict(title="Kaminski and Lo: When Do Stop-Loss Rules Stop Losses?",
        url="https://dspace.mit.edu/entities/publication/bb69ca4b-0cdc-487f-831d-63b2e84fafee",
        lesson="Stop rules can add or subtract value depending on return dynamics. Its daily-futures evidence does not establish an intraday G3 exit."),
    dict(title="Leung and Li: Optimal Mean Reversion Trading with Transaction Costs and Stop-Loss Exit",
        url="https://arxiv.org/abs/1411.5062",
        lesson="Compare continuation and liquidation after costs. Its optimal boundaries depend on a specified mean-reverting spread model; that model has not been established for G3."),
    dict(title="Bailey et al.: The Probability of Backtest Overfitting",
        url="https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf",
        lesson="Searching many alternatives can select noise. Record every trial and validate the selection process, including choosing the baseline."),
    dict(title="Bailey and Lopez de Prado: The Deflated Sharpe Ratio",
        url="https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf",
        lesson="Multiple testing, short records, and non-normal returns complicate performance evidence. More indicators and books do not create independent validation."),
    dict(title="Advances in Financial Machine Learning (official contents)",
        url="https://catalogimages.wiley.com/images/db/pdf/9781119482086.toc.pdf",
        lesson="The contents cover labeling, overlapping outcomes, purging, embargo and backtesting pitfalls. These motivate research hygiene, not a claim of profitable machine learning."),
]


def _stamp(series):
    return pd.to_datetime(series, utc=True, format="mixed", errors="coerce")


def build_dataset(baseline, features, extra_exit_cost_bps=10):
    if not np.isfinite(extra_exit_cost_bps) or extra_exit_cost_bps < 0:
        raise ValueError("Extra exit cost must be finite and nonnegative")
    if baseline.sid.duplicated().any():
        raise ValueError("Duplicate baseline sid")
    if not set(features.sid).issubset(set(baseline.sid)):
        raise ValueError("Unknown decision sid")
    b = baseline.copy().set_index("sid", drop=False)
    if "source_signal_id" not in b and "signal_id" in b:
        b["source_signal_id"] = b.signal_id
    f = features.loc[features.completed_bar_count.isin(LANDMARKS)].copy()
    if f.duplicated(["sid", "completed_bar_count"]).any():
        raise ValueError("Duplicate decision landmark")
    for column in ("day", "source_signal_id", "side", "tradingsymbol", "setup_id"):
        if column in b and column in f:
            expected = f.sid.map(b[column]).astype(str)
            if not f[column].astype(str).eq(expected).all():
                raise ValueError("Decision identity mismatch: " + column)
    executed = b.loc[b.portfolio_executed.eq(True)].copy()
    if not executed.filled.eq(True).all():
        raise ValueError("Executed trade was not filled")
    f = f.loc[f.sid.isin(executed.sid)].copy()
    f["initial_stop_pct"] = f.sid.map(executed.initial_stop_pct)
    f["native_target_pct"] = f.sid.map(executed.native_target_pct)
    entry = f.sid.map(executed.entry_price).astype(float)
    exposure = f.sid.map(executed.exposure_per_entry_rupees).astype(float)
    entry_time = _stamp(f.sid.map(executed.entry_ts))
    native_exit = _stamp(f.sid.map(executed.exit_ts))
    decision, opened, modeled = (_stamp(f[column]) for column in
        ("decision_at_ist", "next_bar_open_at_ist", "modeled_execution_at_ist"))
    if (pd.concat([entry_time,native_exit,decision,opened,modeled],axis=1).isna().any().any()
            or not (decision.ge(entry_time) & opened.ge(decision) & opened.lt(native_exit)
                & modeled.sub(opened).eq(pd.Timedelta(minutes=1))).all()):
        raise ValueError("Invalid decision or execution chronology")
    if not f.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Unknown trade side")
    numeric = f[["next_bar_open_price", "initial_stop_pct", "native_target_pct"]].astype(float)
    if (not np.isfinite(numeric.to_numpy()).all() or not numeric.gt(0).all().all()
            or not np.isfinite(entry).all() or not entry.gt(0).all()
            or not np.isfinite(exposure).all() or not exposure.gt(0).all()):
        raise ValueError("Invalid prices, initial risk, target, or exposure")
    f["elapsed_minutes"] = (decision-entry_time).dt.total_seconds()/60
    for original, normalized in [("signed_move_pct","signed_move_initial_r"),
        ("mfe_pct","mfe_initial_r"),("momentum_5m_pct","momentum_5m_initial_r")]:
        f[normalized] = f[original]/f.initial_stop_pct
    if not np.isfinite(f[MODEL_FEATURES].to_numpy(float)).all():
        raise ValueError("Nonfinite predictor")
    predictors = f[METADATA+MODEL_FEATURES].copy()
    predictors["sample_weight"] = 1/predictors.groupby("sid").sid.transform("size")
    native_net = f.sid.map(executed.portfolio_net_profit_rupees).astype(float)
    base_cost = f.sid.map(executed.portfolio_cost_rupees).astype(float)
    if not np.isfinite(native_net).all() or not np.isfinite(base_cost).all() or base_cost.lt(0).any():
        raise ValueError("Invalid native economics")
    direction = np.where(f.side.eq("LONG"), 1., -1.)
    now_net = direction*(f.next_bar_open_price.astype(float)/entry-1)*exposure-base_cost
    extra = exposure*extra_exit_cost_bps/10000
    value = now_net-native_net-extra
    reason = f.sid.map(executed.exit_reason)
    labels = f[["sid","day","decision_at_ist","completed_bar_count"]].copy()
    labels["label_native_exit_reason"] = reason
    labels["label_native_net_pnl_rs"] = native_net
    labels["label_exit_now_net_pnl_rs"] = now_net
    labels["label_extra_exit_cost_rs"] = extra
    labels["label_exit_value_after_cost_rs"] = value
    labels["label_native_target"] = reason.eq("TARGET")
    labels["label_native_winner"] = native_net.gt(0)
    labels["label_native_stop"] = reason.isin(["STOP","TIGHTENED_STOP"])
    labels["label_target_damage_rs"] = np.where(reason.eq("TARGET"), np.maximum(-value,0),0)
    labels["label_executable_at_ist"] = opened.dt.tz_convert("Asia/Kolkata")
    # Conservative bar-end maturity, not an assumed intrabar stop timestamp.
    labels["label_available_at_ist"] = native_exit.dt.tz_convert("Asia/Kolkata")
    observed = set(zip(f.sid, f.completed_bar_count))
    coverage = pd.DataFrame([dict(sid=int(row.sid),day=str(row.day),completed_bar_count=count,
        eligible=(int(row.sid),count) in observed,
        reason="AVAILABLE" if (int(row.sid),count) in observed else "NO_RECORDED_ELIGIBLE_DECISION")
        for row in executed.itertuples(index=False) for count in LANDMARKS])
    return predictors.reset_index(drop=True), labels.reset_index(drop=True), coverage


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(path, data):
    path.write_text(json.dumps(data,indent=2,allow_nan=False),encoding="utf-8")


def run(source=DEFAULT_SOURCE, output_root=DEFAULT_OUTPUT):
    source = Path(source)
    manifest = json.loads((source/"manifest.json").read_text())
    inputs = ["ledgers/baseline.csv","decision_features_oi.parquet"]
    if any(_sha(source/name) != manifest["files"][name] for name in inputs):
        raise ValueError("Frozen study input hashes changed")
    baseline = pd.read_csv(source/inputs[0])
    features = pd.read_parquet(source/inputs[1])
    predictors,labels,coverage = build_dataset(baseline,features)
    output = Path(output_root)/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_"+secrets.token_hex(4)+"_target_preservation")
    output.mkdir(parents=True,exist_ok=False)
    predictors.to_parquet(output/"decision_time_predictors.parquet",index=False)
    labels.to_parquet(output/"future_labels_do_not_use_as_features.parquet",index=False)
    coverage.to_csv(output/"landmark_coverage.csv",index=False)
    executed = baseline.loc[baseline.portfolio_executed.eq(True)].copy()
    observed = features.loc[features.sid.isin(executed.sid)].groupby("sid").signed_move_pct.min()
    joined = executed.set_index("sid").join(observed.rename("minimum_observed_signed_move_pct"))
    overlap = []
    for threshold in (-.3,-.5,-.75,-.9):
        mask = joined.minimum_observed_signed_move_pct.le(threshold)
        targets = mask & joined.exit_reason.eq("TARGET")
        overlap.append(dict(observed_move_pct=threshold,target_trades=int(targets.sum()),
            all_winning_trades=int((mask & joined.portfolio_net_profit_rupees.gt(0)).sum()),
            native_stop_trades=int((mask & joined.exit_reason.isin(["STOP","TIGHTENED_STOP"])).sum()),
            eventual_target_net_pnl_rs=float(joined.loc[targets,"portfolio_net_profit_rupees"].sum())))
    overlap = pd.DataFrame(overlap)
    overlap.to_csv(output/"observed_path_overlap.csv",index=False)
    contributions = executed.groupby("exit_reason").agg(trades=("sid","size"),
        net_pnl_rs=("portfolio_net_profit_rupees","sum"),
        winners=("portfolio_net_profit_rupees",lambda values:int(values.gt(0).sum())),
        losses=("portfolio_net_profit_rupees",lambda values:int(values.lt(0).sum()))).reset_index()
    contributions.to_csv(output/"baseline_exit_contributions.csv",index=False)
    combined = predictors.merge(labels,on=["sid","day","decision_at_ist","completed_bar_count"],validate="one_to_one")
    combined["outcome_group"] = np.select([combined.label_native_target,combined.label_native_stop,
        combined.label_native_winner],["TARGET_WINNER","NATIVE_STOP","OTHER_WINNER"],default="OTHER_LOSER")
    anatomy = combined.groupby(["completed_bar_count","outcome_group"]).agg(trades=("sid","size"),
        median_move_initial_r=("signed_move_initial_r","median"),
        median_mfe_initial_r=("mfe_initial_r","median"),
        median_momentum_initial_r=("momentum_5m_initial_r","median"),
        median_elapsed_minutes=("elapsed_minutes","median")).reset_index()
    anatomy.to_csv(output/"landmark_path_anatomy.csv",index=False)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1,3,figsize=(13,4.5),sharey=True,constrained_layout=True)
    for axis, landmark in zip(axes,LANDMARKS):
        part = combined.loc[combined.completed_bar_count.eq(landmark)]
        values = [part.loc[part.label_native_target,"signed_move_pct"],part.loc[part.label_native_stop,"signed_move_pct"]]
        boxes = axis.boxplot(values,patch_artist=True,showfliers=True,
            tick_labels=[f"Future TARGET\n(n={len(values[0])})",f"Future STOP\n(n={len(values[1])})"],
            medianprops=dict(color="#17252c",linewidth=2))
        for box,color in zip(boxes["boxes"],["#93c9ad","#e8aaa5"]):
            box.set_facecolor(color)
        axis.axhline(0,color="#647984",linewidth=.8)
        axis.set_title(f"After {landmark} completed bars")
        axis.grid(axis="y",alpha=.2)
    axes[0].set_ylabel("Signed completed-close move from entry (%)")
    fig.suptitle("Target winners and stopped trades can occupy overlapping price states")
    fig.savefig(output/"winner_stop_overlap.png",dpi=150)
    plt.close(fig)
    specification = dict(version=VERSION,scope="DIAGNOSTIC_ONLY",execution_authority=False,
        predictive_model_trained=False,configuration_promoted=False,fresh_holdout=False,
        model_feature_allowlist=MODEL_FEATURES,metadata_not_automatically_predictors=METADATA+["sample_weight"],
        labels="SEPARATE_FILES_FUTURE_OUTCOMES_FOR_OFFLINE_TRAINING_AND_EVALUATION_ONLY",
        label_definition="exit_now_base_net - native_final_net - extra_exit_cost",
        landmark_completed_bars=list(LANDMARKS),extra_exit_cost_bps=10,
        native_target_trades_touched_allowed_on_evaluation=0,
        protection_scope="ZERO_OBSERVED_TARGET_INTERVENTIONS_IS_A_RESEARCH_GATE_NOT_A_FUTURE_GUARANTEE",
        default_action="NO_INTERVENTION_NATIVE_STOP_AND_TARGET",
        validation=["Freeze one small interpretable candidate and its thresholds before evaluation.",
            "Split chronologically by session; keep each trade and overlapping outcomes out of other folds.",
            "Only train on labels whose label_available_at precedes the training cutoff.",
            "Treat 457 trades in 50 sessions as clustered observations, not thousands of independent minutes.",
            "Keep original entries and capital allocation in the primary comparison.",
            "Require positive incremental net after costs, zero observed target-trade interventions, and adequate independent evidence.",
            "Evaluate target and other-winner harm, losing time exits, realized loss, drawdown, and tail outcomes separately.",
            "Use only fresh forward observations for promotion; existing last15 sessions are already reused.",
            "A model with no interventions has shown no improvement; uncertainty defaults to no action."],
        future_feature_hypotheses=["Causal volatility and range normalization; not a stand-alone failure signal.",
            "Relative strength against market/sector known at decision time.",
            "Structural break and failed recovery, defined before scoring.",
            "Time-to-progress compared at identical decision ages.",
            "OI only if its incremental value and retained/live receipt provenance are established."],
        feature_hypotheses_status="NOT_TESTED_OR_CLAIMED_PROFITABLE_BY_THIS_DATASET",
        first_seen_forward_record=["strategy/config hash","event and receipt times","bid/ask and freshness",
            "features available at decision","proposal and reason","simulated execution and costs","parallel baseline continuation"],
        search_budget="ONE_FROZEN_SIMPLE_MODEL_FIRST_NO_WIDE_THRESHOLD_OR_REGIME_SEARCH")
    _json(output/"engine_research_specification.json",specification)
    _json(output/"research_sources.json",SOURCES)
    summary=dict(version=VERSION,source=str(source),source_manifest_sha256=_sha(source/"manifest.json"),
        input_hashes={name:manifest["files"][name] for name in inputs},
        original_executions=len(executed),sessions=int(executed.day.nunique()),
        decisions=len(predictors),trades_with_decisions=int(predictors.sid.nunique()),
        target_trades=int(executed.exit_reason.eq("TARGET").sum()),native_stops=int(executed.exit_reason.isin(["STOP","TIGHTENED_STOP"]).sum()),
        execution_authority=False,predictive_model_trained=False,configuration_promoted=False,
        limitations=["Existing strategy-selected history is not fresh holdout evidence.",
            "Observed path cache starts after six completed post-entry bars; absent observations are not zero drawdown.",
            "Observed signed close movement is not full intrabar MAE.",
            "Next-open prices and original final outcomes are label-only information, never predictors.",
            "No perfect classifier or positive exit edge has been established.",
            "OI fields are excluded from this first predictor dataset; no retrospective availability assumptions are silently inherited."])
    _json(output/"summary.json",summary)
    table=lambda frame:frame.to_html(index=False,border=0,float_format=lambda value:f"{value:,.2f}")
    bibliography="".join(f'<li><a href="{escape(item["url"])}">{escape(item["title"])}</a>: {escape(item["lesson"])}</li>' for item in SOURCES)
    coverage_summary=coverage.groupby("completed_bar_count").agg(original_executions=("sid","size"),eligible=("eligible","sum")).reset_index()
    report=f'''<!doctype html><html><head><meta charset="utf-8"><title>G3: protecting target winners</title>
<style>body{{font:16px system-ui;max-width:1150px;margin:40px auto;padding:0 24px;color:#213745;line-height:1.6}}h1,h2{{color:#14566b}}table{{border-collapse:collapse;font-size:14px}}th,td{{padding:9px;border-bottom:1px solid #ddd;text-align:right}}th{{background:#edf5f6}}.note{{padding:18px;background:#fff3d6}}.scroll{{overflow:auto}}li{{margin:10px 0}}</style></head><body>
<h1>Build an exit model that accounts for the cost of cutting winners</h1>
<p class="note">Research foundation only. No predictive model has been trained, no profitable exit edge has been established, and no trading configuration was changed. A guarantee of never interfering with a future target winner cannot be established from finite historical data.</p>
<p>The next task is to estimate whether exiting improves the outcome relative to the existing stop and target, after costs. A future STOP label alone does not answer that question.</p>
<h2>Why tighter exits damaged this baseline</h2><div class="scroll">{table(overlap)}</div>
<p>These are completed-close observations after six bars, not full intrabar drawdown. The threshold probes are descriptive, not proposed trading rules. The profit column is eventual native target profit, not the exact economic cost of an earlier exit.</p>
<img src="winner_stop_overlap.png" style="width:100%" alt="Signed price movement of eventual target winners and native stops at three decision checkpoints"><p>Outcome groups are known only afterward. Boxes show the middle half of observations, with medians and tails. Marginal overlap does not rule out a useful multivariate signal; it shows why a simple adverse-price threshold is insufficient.</p>
<h2>What needs protecting</h2><div class="scroll">{table(contributions)}</div>
<p>Target winners and other profitable trades are different groups. Native stops also do not account for every losing trade. Track all four groups.</p>
<h2>The implemented first step</h2><p>Decision-time predictors and future economic labels are stored in separate files. Fixed checkpoints are 6, 15 and 30 completed bars. Normalization uses initial stop distance known at entry. Original executed trades remain in the coverage denominator, including trades with no eligible checkpoint.</p>
<div class="scroll">{table(coverage_summary)}</div><div class="scroll">{table(anatomy)}</div>
<p><strong>Offline label:</strong> immediate next-open net P&amp;L minus native final net P&amp;L minus additional exit friction. A positive label means that historical intervention would have helped; it is not a prediction available at the decision time.</p>
<h2>Proposed engine</h2><ol><li><strong>Keep the native entry, target and hard-stop policy.</strong> Evaluate a separate optional early-exit decision with no action as the default.</li>
<li><strong>Study comparable trade states.</strong> Compare target winners and failures at the same elapsed stage. Price/risk normalization, relative strength, structural failure and time-to-progress are hypotheses requiring validation.</li>
<li><strong>Fit one small economic model only if the diagnostic supports it.</strong> Estimate the advantage of exiting over continuing and the risk of harming target winners. Do not assume a larger model creates information missing from the data.</li>
<li><strong>Protect winners explicitly.</strong> Under the requested constraint, reject a candidate that intervenes in any native target winner in evaluation. Zero observed harm does not imply zero future harm. Partial exits also reduce participation and are not a free workaround.</li>
<li><strong>Require fresh evidence.</strong> Group by trade and chronological session, mature labels before training, account for overlapping outcomes, costs and all attempted variants, and compare a shadow candidate to the untouched baseline. Insufficient evidence means no intervention.</li></ol>
<h2>What the literature contributes</h2><p>This is a focused review of primary papers and accessible publisher material. It is not a claim to have read every trading book or complete books whose text was unavailable. The architecture above is a proposal; these references do not prove that it will improve G3.</p><ul>{bibliography}</ul>
<h2>Artifacts</h2><p><a href="engine_research_specification.json">Research specification</a> · <a href="summary.json">Scope and source hashes</a> · <a href="landmark_coverage.csv">Coverage</a> · <a href="observed_path_overlap.csv">Observed path overlap</a></p></body></html>'''
    (output/"report.html").write_text(report,encoding="utf-8")
    shutil.copy2(__file__,output/Path(__file__).name)
    files={path.name:_sha(path) for path in output.iterdir() if path.is_file()}
    _json(output/"manifest.json",dict(version=VERSION,execution_authority=False,files=files))
    if any(_sha(output/name)!=digest for name,digest in files.items()):
        raise ValueError("Artifact verification failed")
    print(json.dumps(dict(output=str(output),**summary)),flush=True)
    return output


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source",type=Path,default=DEFAULT_SOURCE)
    parser.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT)
    args=parser.parse_args()
    run(args.source,args.output_root)
