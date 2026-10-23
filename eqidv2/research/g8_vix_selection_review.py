"""Sensitivity and a reproducible candidate package for completed G8 selections.

Nearby settings are diagnostic only; they do not choose or promote a policy.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from research import g8_vix_selection_research as core
from research.g8_exit_engine import metrics
from research.g8_vix_data import VixData

MODEL="logistic_vix_context_C0.1_p0.5"
RULE="persistent_direction_30m_2.0"


def frozen_score(f,model):
    """Score an exported model without reading labels or refitting anything."""
    x=f[model["columns"]].to_numpy(float)
    median=np.asarray(model["imputer_median"],float)
    x=np.where(np.isnan(x),median,x)
    if not np.isfinite(x).all():
        raise ValueError("Invalid candidate feature")
    normalized=(x-np.asarray(model["scaler_mean"]))/np.asarray(model["scaler_scale"])
    z=normalized@np.asarray(model["coefficient"])+model["intercept"]
    return pd.Series(1./(1.+np.exp(-np.clip(z,-700,700))),index=f.index)


def review(run_dir):
    root=Path(run_dir).resolve()
    core.verify(root)
    summary=json.loads((root/"summary.json").read_text(encoding="utf-8"))
    parent=Path(summary["parent_run"])
    core.verify(parent)
    source=core.load_sources()
    f=pd.read_csv(root/"features.csv").set_index("sid",drop=False)
    fast=core.FastSelection(source["unfiltered"],source["config"])
    days=fast.days
    out=root/"review"
    out.mkdir(exist_ok=True)
    core.dump(out/"sensitivity_plan.json",dict(purpose="local stability only; no reselection or promotion",
        fixed_candidate=MODEL,fixed_rule=RULE,rule_lookbacks=[20,30,40],rule_thresholds=[1.5,2.,2.5],
        model_c=[.05,.1,.2],model_threshold=[.45,.5,.55],independent_holdout=False))
    model_records=json.loads((root/"fitted_models.json").read_text(encoding="utf-8"))
    model=next(m for m in model_records if m["inputs"]=="vix_context" and m["c"]==.1)
    scores=frozen_score(f,model)
    known=pd.read_csv(root/"feature_decisions.csv").set_index("sid")
    mask=f.nifty_base_allowed&scores.ge(.5)
    if not np.array_equal(mask,known.historical_best_both_allowed):
        raise ValueError("Exported model does not reproduce saved entry decisions")
    model_book=fast.full_book(mask,MODEL)
    rule_spec=next(s for s in core.rule_specs() if s["id"]==RULE)
    rule_book=fast.full_book(core.allowed_for(f,rule_spec),RULE)
    model_book.to_csv(out/"frozen_model_candidate_trades.csv",index=False)
    rule_book.to_csv(out/"simple_30m_rule_trades.csv",index=False)
    config=deepcopy(source["config"])
    config.update(strategy="V13-V10-G-8",variant_id="V13-V10-G-8-VIX-MODEL-CANDIDATE-V1",scope="BACKTEST_ONLY",execution_authority=False)
    config["vix_selection_candidate"]=dict(id=MODEL,status="EXPERIMENTAL_NOT_PROMOTED",model=model,
        score_threshold=.5,score_is_calibrated_probability=False,
        first_gate="Nifty both opposing directions reject",feature_builder="research.g8_vix_selection_research.build_features",
        scorer="research.g8_vix_selection_review.frozen_score",all_51_slots=True,exits_unchanged=True,
        retrospectively_selected=True,source_run_manifest_sha256=core.sha(root/"manifest.json"))
    core.dump(out/"experimental_model_config.json",config)
    rule_config=deepcopy(source["config"])
    rule_config.update(strategy="V13-V10-G-8",variant_id="V13-V10-G-8-VIX-30M-CANDIDATE-V1",scope="BACKTEST_ONLY",execution_authority=False)
    rule_config["vix_selection_candidate"]=dict(id=RULE,spec=rule_spec,status="EXPERIMENTAL_NOT_PROMOTED",
        first_gate="Nifty both opposing directions reject",retrospectively_selected=True,exits_unchanged=True)
    core.dump(out/"experimental_simple_rule_config.json",rule_config)
    comparisons=[]
    trial=pd.read_csv(root/"parameter_trials.csv")
    labels={"Baseline":core.BASELINE,"Previous G8":"previous_g8_selected","Simple 30m VIX rule":RULE,
        "VIX rule combination":summary["historical_best_rule"],"VIX + context model":MODEL,
        "Context model without VIX":"logistic_context_C0.1_p0.5","VIX-only model":"logistic_vix_C0.1_p0.5"}
    for label,name in labels.items():
        for period in ("all","validation","final_test"):
            row=trial.loc[trial.policy.eq(name)&trial.period.eq(period)].iloc[0].to_dict()
            comparisons.append(dict(label=label,**row))
    comparison=pd.DataFrame(comparisons)
    comparison.to_csv(out/"decision_comparison.csv",index=False)
    # Local rule neighborhood, including early-session fallback used by the fixed rule.
    vix=VixData(parent/"india_vix_5minute.csv")
    sensitivity=[]
    base_execution=fast.execution(f.nifty_base_allowed)
    for window in (20,30,40):
        recent=[]
        for row in f.itertuples(index=False):
            decision=pd.Timestamp(row.confirmation_ts)
            current=vix.features(decision)
            earlier=vix.features(decision-pd.Timedelta(minutes=window))
            recent.append((current["vix_level"]/earlier["vix_level"]-1)*100 if earlier else row.vix_session_pct)
        recent=pd.Series(recent,index=f.index)
        for threshold in (1.5,2.,2.5):
            allow=f.nifty_base_allowed&~((f.side_sign*recent>=threshold)&(f.side_sign*f.vix_session_pct>=threshold))
            execution=fast.execution(allow)
            for period,calendar in (("all",days),("final_test",days[45:])):
                sensitivity.append(dict(family="rule",window=window,c=np.nan,threshold=threshold,period=period,
                    **fast.stats(execution,calendar)))
    # Nearby model settings use the SAME first35-session training set. No future labels enter fit.
    original=source["unfiltered"].set_index("sid").loc[f.index]
    train=f.day.isin(days[:35])&original.filled&f.nifty_base_allowed
    columns=model["columns"]
    matrix=f[columns].astype(float)
    y=original.loc[train,"net_return_pct"].gt(0)
    size=f.loc[train].groupby("day").sid.transform("count").to_numpy()
    weight=1./size
    weight/=weight.mean()
    for c in (.05,.1,.2):
        estimator=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),
            LogisticRegression(C=c,max_iter=2000,random_state=20261022))
        estimator.fit(matrix.loc[train],y,logisticregression__sample_weight=weight)
        proba=pd.Series(estimator.predict_proba(matrix)[:,1],index=f.index)
        if c==.1 and not np.allclose(proba,scores,atol=1e-12,rtol=0):
            raise ValueError("Frozen coefficients differ from fit")
        if c==.2:
            diagnostic_model=dict(model,c=c,coefficient=estimator[-1].coef_[0].tolist(),
                intercept=float(estimator[-1].intercept_[0]),imputer_median=estimator[0].statistics_.tolist(),
                scaler_mean=estimator[1].mean_.tolist(),scaler_scale=estimator[1].scale_.tolist())
            if not np.allclose(frozen_score(f,diagnostic_model),proba,atol=1e-12,rtol=0):
                raise ValueError("Diagnostic model export differs")
            diagnostic_config=deepcopy(config)
            diagnostic_config["variant_id"]="V13-V10-G-8-VIX-MODEL-DIAGNOSTIC-C020"
            diagnostic_config["vix_selection_candidate"].update(id="logistic_vix_context_C0.2_p0.5",
                model=diagnostic_model,status="SENSITIVITY_ONLY_RETROSPECTIVE_CANDIDATE")
            core.dump(out/"diagnostic_model_C020_config.json",diagnostic_config)
            diagnostic_book=fast.full_book(f.nifty_base_allowed&proba.ge(.5),"diagnostic_C020_p050")
            diagnostic_book.to_csv(out/"diagnostic_model_C020_trades.csv",index=False)
            core.dump(out/"diagnostic_model_C020_metrics.json",dict(
                all=metrics(diagnostic_book,days),final_test=metrics(diagnostic_book,days[45:]),
                status="Sensitivity only; not used for promotion"))
        for p in (.45,.5,.55):
            execution=fast.execution(f.nifty_base_allowed&proba.ge(p))
            for period,calendar in (("all",days),("final_test",days[45:])):
                sensitivity.append(dict(family="model",window=np.nan,c=c,threshold=p,period=period,
                    **fast.stats(execution,calendar)))
    sensitivity=pd.DataFrame(sensitivity)
    sensitivity.to_csv(out/"sensitivity_only.csv",index=False)
    # Per-day accounting for the fixed candidates and an explicitly descriptive block interval.
    baseline=fast.full_book(f.nifty_base_allowed,core.BASELINE)
    daily=[]
    for day in days:
        bm=metrics(baseline,[day])
        mm=metrics(model_book,[day])
        rm=metrics(rule_book,[day])
        daily.append(dict(day=day,baseline_net_rs=bm["net_pnl_rs"],model_net_rs=mm["net_pnl_rs"],
            simple_rule_net_rs=rm["net_pnl_rs"],model_delta_rs=mm["net_pnl_rs"]-bm["net_pnl_rs"],
            simple_rule_delta_rs=rm["net_pnl_rs"]-bm["net_pnl_rs"]))
    daily=pd.DataFrame(daily)
    daily.to_csv(out/"candidate_daywise.csv",index=False)
    rng=np.random.default_rng(20261023)
    starts=rng.integers(0,len(days),size=(10000,int(np.ceil(len(days)/5))))
    indices=((starts[:,:,None]+np.arange(5))%len(days)).reshape(10000,-1)[:,:len(days)]
    bootstrap=daily.model_delta_rs.to_numpy()[indices].sum(axis=1)
    interval=np.percentile(bootstrap,[2.5,97.5]).tolist()
    train_table=trial.loc[trial.period.eq("development")].set_index("policy")
    val_table=trial.loc[trial.period.eq("validation")].set_index("policy")
    train_pass=core.training_eligible(train_table.loc[MODEL],train_table.loc[core.BASELINE])
    validation_pass=core.validation_eligible(val_table.loc[MODEL],val_table.loc[core.BASELINE])
    allstats=comparison.loc[comparison.period.eq("all")].set_index("label")
    base=allstats.loc["Baseline"]
    m=allstats.loc["VIX + context model"]
    neighborhood=sensitivity.loc[sensitivity.period.eq("all")]
    sensitivity_summary=[]
    for family,group in neighborhood.groupby("family"):
        improvements=(group.net_pnl_rs>base.net_pnl_rs)&(group.win_rate_pct>base.win_rate_pct)
        sensitivity_summary.append(dict(family=family,settings=len(group),both_improved=int(improvements.sum()),
            lowest_profit_rs=float(group.net_pnl_rs.min()),highest_profit_rs=float(group.net_pnl_rs.max()),
            lowest_win_rate_pct=float(group.win_rate_pct.min()),highest_win_rate_pct=float(group.win_rate_pct.max())))
    final=comparison.loc[comparison.period.eq("final_test")].set_index("label")
    conclusion=dict(fixed_candidate=MODEL,model_train_guard_passed=train_pass,model_validation_guard_passed=validation_pass,
        reason_training="only4 changed executions; minimum5 required",
        reason_validation="3 changed executions occurred on only1 day; minimum2 distinct days required",
        overall_gain_rs=float(m.net_pnl_rs-base.net_pnl_rs),
        win_rate_change_pp=float(m.win_rate_pct-base.win_rate_pct),
        final11_gain_rs=float(final.loc["VIX + context model","net_pnl_rs"]-final.loc["Baseline","net_pnl_rs"]),
        bootstrap_95pct_interval_rs=interval,
        bootstrap_caveat="Descriptive daily blocks only; does not correct repeated selection, model fitting or inherited historical optimization",
        sensitivity=sensitivity_summary,auto_promoted=False,independent_holdout=False,
        recommendation="Experimental candidates are concrete and reproducible; keep baseline until new data supplies evidence.",
        parent_manifest_sha256=core.sha(root/"manifest.json"))
    core.dump(out/"conclusion.json",conclusion)
    def table(frame):
        return frame.to_html(index=False,float_format=lambda v:f"{v:,.2f}")
    cols=["label","trades","wins","losses","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]
    overall=comparison.loc[comparison.period.eq("all"),cols]
    last=comparison.loc[comparison.period.eq("final_test"),cols]
    sensitive_table=pd.DataFrame(sensitivity_summary)
    text=f"""<!doctype html><html><head><meta charset="utf-8"><title>G8 VIX selection — decision review</title>
    <style>body{{font:16px system-ui;margin:32px;color:#23394e}}h1,h2{{color:#173e66}}table{{border-collapse:collapse;font-size:13px}}th,td{{padding:8px;border:1px solid #dbe3eb;text-align:right}}th{{background:#eaf2fa}}.scroll{{overflow:auto}}.note{{background:#fff1cf;padding:18px}}</style></head><body>
    <h1>G8: a better historical VIX candidate, with limited evidence</h1>
    <p>Four iterations evaluated 57 fixed rules, 24 model/threshold choices and 12 training-discovered combinations
    (93 candidates in the final comparison). An additional 18 nearby settings were checked for sensitivity only.
    All work uses 56 eligible sessions through 22 October 2026, original G7 V2 exits and modeled 5bps round-trip costs.</p>
    <div class="note">The fixed VIX + context model increases historical net profit by INR {conclusion['overall_gain_rs']:,.2f},
    improves win rate by {conclusion['win_rate_change_pp']:.2f} percentage points, reduces losing days from 7 to 6,
    and reduces daily-close drawdown. It remains experimental: its changed trades are sparse, and all this history has been
    repeatedly inspected. The automatic chronological selector retained the baseline in every rolling block.</div>
    <h2>Overall comparison</h2><div class="scroll">{table(overall)}</div>
    <p>Model full-history totals include its first 35 training sessions. The model name and threshold were selected retrospectively;
    later-period improvements also cannot be represented as an untouched discovery test.</p>
    <h2>Final 11 sessions: 7–22 October</h2><div class="scroll">{table(last)}</div>
    <h2>Exact candidate</h2><p>First apply the approved Nifty gate: reject if both slot and session moves oppose the trade.
    Then apply a small regularized logistic model using VIX trend, lagged VIX level, Nifty agreement, trade side/time,
    confirmation close position and candle strength, signal move, volume and OI. Retain entries with score at least 0.50.
    The score is not a calibrated promise of a win. Parameters were fitted only through 18 September; all preprocessing is exported.</p>
    <p>The model removes 9 original trades (7 losers and 2 winners) and frees capacity for one additional winner.
    The added profit therefore includes portfolio reallocation, not just subtraction of losing rows.</p>
    <p>The simpler candidate rejects a LONG when VIX has risen at least 2% both since session open and over 30 minutes,
    and rejects a SHORT when VIX has fallen at least 2% over both windows. This is an experimental correlation filter,
    not a guarantee that VIX determines stock direction. Both rules retain existing SL and targets for every valid slot.</p>
    <h2>Why the model was not promoted</h2><p>It changed only 4 training executions, below the declared minimum of 5.
    Its validation benefit came from 3 changed executions on one day, below the minimum of two distinct days.
    Three rolling blocks repeated all discovery and fitting from earlier data; none passed the promotion guards.
    Keeping the baseline follows the declared process and avoids choosing the default from final-period results.</p>
    <h2>Nearby settings</h2><div class="scroll">{table(sensitive_table)}</div>
    <p>The rule neighborhood uses 20/30/40-minute windows and 1.5/2.0/2.5% thresholds.
    The model neighborhood uses regularization C of 0.05/0.10/0.20 and score cutoffs 0.45/0.50/0.55.
    These are sensitivity checks; their best result does not replace the fixed candidate or select the default.</p>
    <p>A descriptive 5-session-block bootstrap interval for the model's total profit change is
    INR {interval[0]:,.0f} to INR {interval[1]:,.0f}. It does not correct model fitting or repeated search,
    so it cannot establish an independent edge.</p>
    <h2>SL, targets and scope</h2><p>Keep the 1.20% initial SL, tightening to 1.00% after 120 minutes;
    original slot targets 0.60–2.00%; and 15:15 square-off. These experiments improve entry selection only.
    All 51 valid slot/side configurations are retained, with no stock blacklist or outcome-based slot removal.
    Modeled capital is INR 10 lakh, INR 1 lakh per position with 5x exposure and at most 10 positions.
    Archived prices, missing-session/opening-OI limitations, fractional quantities, and stop-first gap assumptions remain.</p>
    <h2>Files</h2><p><a href="experimental_model_config.json">Reproducible model candidate config</a> |
    <a href="diagnostic_model_C020_config.json">C=0.20 diagnostic model config</a> |
    <a href="experimental_simple_rule_config.json">Simple rule config</a> |
    <a href="candidate_daywise.csv">Candidate day-wise changes</a> |
    <a href="../slot_side_results.csv">Slot and side results</a> |
    <a href="sensitivity_only.csv">All 18 sensitivity checks</a> |
    <a href="../cost_sensitivity.csv">Cost sensitivity</a> |
    <a href="../historical_best_both_avoided_trades.csv">Trades removed by model</a> |
    <a href="../historical_best_both_newly_allocated_trades.csv">Newly allocated trade</a> |
    <a href="../report.html">All 93 candidates and rolling evaluation</a></p></body></html>"""
    (out/"review.html").write_text(text,encoding="utf-8")
    core.dump(out/"manifest.json",dict(source_run_manifest_sha256=core.sha(root/"manifest.json"),
        code_sha256=core.sha(__file__),files={p.name:core.sha(p) for p in out.iterdir() if p.is_file() and p.name!="manifest.json"}))
    print(json.dumps(conclusion,indent=2))
    print(out/"review.html")
    return out


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir")
    review(parser.parse_args().run_dir)
