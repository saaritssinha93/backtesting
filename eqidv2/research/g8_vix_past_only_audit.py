"""Exact earlier-model rolling benchmark and decision review for G8 V4.

No search here: use the previously reported C=.20, cutoff=.50 model settings.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from research import g8_vix_past_only as newer
from research import g8_vix_selection_research as core
from research.g8_vix_selection_review import frozen_score
from research.g8_exit_engine import metrics


def audit(run_dir):
    root=Path(run_dir).resolve()
    newer.verify(root)
    summary=json.loads((root/"summary.json").read_text(encoding="utf-8"))
    parent=Path(summary["source_parent"])
    core.verify(parent)
    f=pd.read_csv(parent/"features.csv").set_index("sid",drop=False)
    fresh=pd.read_csv(root/"features.csv").set_index("sid",drop=False)
    source=core.load_sources()
    fast=core.FastSelection(source["unfiltered"],source["config"])
    days=source["days"]
    original=source["unfiltered"].set_index("sid").loc[f.index]
    columns=list(dict.fromkeys(core.VIX_FEATURES+core.CONTEXT_FEATURES))
    out=root/"review"
    out.mkdir(exist_ok=True)
    core.dump(out/"exact_model_plan.json",dict(purpose="test prior reported model, no extra parameter search",
        c=.2,cutoff=.5,columns=columns,rolling_start=21,refit_every=7,
        comparators=["original24features_weekly","same24features_with_prior_VIX_warmup_weekly",
            "original_frozen_model_only_from_session35"],independent_holdout=False))
    masks={"baseline":f.nifty_base_allowed.copy(),
        "exact_old_model_weekly":f.nifty_base_allowed.copy(),
        "old_model_warmed_weekly":f.nifty_base_allowed.copy(),
        "old_model_frozen_from_Sep21":f.nifty_base_allowed.copy(),
        "simple_vix30":newer.rule_mask(f,dict(mode="simple"))}
    proofs=[]
    predictions=[]
    for start in range(21,len(days),7):
        training=days[:start]
        fit=f.day.isin(training)&original.filled&f.nifty_base_allowed
        y=original.loc[fit,"net_return_pct"].gt(0)
        counts=f.loc[fit].groupby("day").sid.transform("count").to_numpy(float)
        weights=1./counts
        weights/=weights.mean()
        apply=f.day.isin(days[start:start+7])
        for label,features in (("exact_old_model_weekly",f),("old_model_warmed_weekly",fresh)):
            matrix=features[columns].astype(float)
            model=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),
                LogisticRegression(C=.2,max_iter=2000,random_state=20261022))
            model.fit(matrix.loc[fit],y,logisticregression__sample_weight=weights)
            score=pd.Series(model.predict_proba(matrix)[:,1],index=f.index)
            masks[label].loc[apply]=(f.nifty_base_allowed&score.ge(.5)).loc[apply]
            proof=dict(policy=label,train_through=training[-1],test_from=days[start],test_through=days[min(start+6,len(days)-1)],
                coefficient=model[-1].coef_[0].tolist(),intercept=float(model[-1].intercept_[0]),
                scaler_mean=model[1].mean_.tolist(),scaler_scale=model[1].scale_.tolist(),
                imputer=model[0].statistics_.tolist(),train_orders=int(fit.sum()))
            proofs.append(proof)
            predictions.append(pd.DataFrame(dict(sid=f.loc[apply,"sid"],day=f.loc[apply,"day"],policy=label,
                train_through=training[-1],score=score.loc[apply])))
    model_artifact=json.loads((parent/"review/diagnostic_model_C020_config.json").read_text(encoding="utf-8"))
    frozen=model_artifact["vix_selection_candidate"]["model"]
    if frozen["train_through"]>=days[35]:
        raise ValueError("Frozen model evaluated before its training was complete")
    fixed=frozen_score(f,frozen)
    apply=f.day.isin(days[35:])
    masks["old_model_frozen_from_Sep21"].loc[apply]=(f.nifty_base_allowed&fixed.ge(.5)).loc[apply]
    core.dump(out/"exact_model_fit_proofs.json",proofs)
    predicted=pd.concat(predictions,ignore_index=True)
    if not predicted.train_through.lt(predicted.day).all():
        raise ValueError("Future training in exact-model benchmark")
    predicted.to_csv(out/"exact_model_predictions.csv",index=False)
    books={}
    rows=[]
    costs=[]
    daily=[]
    periods=dict(matched35_forecast_sessions=days[21:],matched21_after_old_model_training=days[35:],
                 final14_assessment=days[42:],full56_including_warmup=days)
    for label,mask in masks.items():
        print("Exact-model full-engine verification:",label,flush=True)
        book=fast.full_book(mask,label)
        books[label]=book
        book.to_csv(out/(label+"_trades.csv"),index=False)
        for period,calendar in periods.items():
            rows.append(dict(label=label,period=period,**metrics(book,calendar)))
        for cost in (5.,10.,15.):
            costs.append(dict(label=label,cost_bps=cost,**fast.stats(fast.execution(mask),days[21:],cost)))
        for day in days:
            daily.append(dict(label=label,day=day,**metrics(book,[day])))
    comparison=pd.DataFrame(rows)
    comparison.to_csv(out/"exact_model_comparison.csv",index=False)
    pd.DataFrame(costs).to_csv(out/"exact_model_costs.csv",index=False)
    daily=pd.DataFrame(daily)
    daily.to_csv(out/"exact_model_daily_long.csv",index=False)
    pivot=daily.pivot(index="day",columns="label",values="net_pnl_rs").reset_index()
    for label in masks:
        if label!="baseline":
            pivot[label+"_change_rs"]=pivot[label]-pivot.baseline
    pivot.to_csv(out/"exact_model_daywise.csv",index=False)
    concentration=[]
    for label in masks:
        if label=="baseline":
            continue
        cohort=pivot.loc[pivot.day.isin(days[21:])]
        delta=cohort[label+"_change_rs"]
        concentration.append(dict(label=label,net_change_rs=float(delta.sum()),
            net_change_without_best_day_rs=float(delta.sum()-delta.max()),
            gain_days=int(delta.gt(1e-8).sum()),loss_days=int(delta.lt(-1e-8).sum())))
    concentration=pd.DataFrame(concentration)
    concentration.to_csv(out/"exact_model_concentration.csv",index=False)
    newer_trials=pd.read_csv(root/"parameter_trials.csv")
    ranked=newer_trials.loc[newer_trials.period.eq("all_past_forecasts")].sort_values("net_pnl_rs",ascending=False)
    ranked[["policy","trades","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]].to_csv(out/"new40_policy_ranking.csv",index=False)
    # Benchmarks are comparisons, not a new model-selection or promotion stage.
    matched=comparison.loc[comparison.period.eq("matched35_forecast_sessions")].set_index("label")
    conclusion=dict(past_only_locked_policy=summary["selected_policy"],
        exact_old_weekly_delta_rs=float(matched.loc["exact_old_model_weekly","net_pnl_rs"]-matched.loc["baseline","net_pnl_rs"]),
        warmed_old_weekly_delta_rs=float(matched.loc["old_model_warmed_weekly","net_pnl_rs"]-matched.loc["baseline","net_pnl_rs"]),
        simple_rule_delta_rs=float(matched.loc["simple_vix30","net_pnl_rs"]-matched.loc["baseline","net_pnl_rs"]),
        independent_holdout=False,auto_promoted=False,
        recommendation="Do not infer a dependable new improvement from this repeatedly inspected sample.")
    core.dump(out/"conclusion.json",conclusion)
    def table(frame):
        return frame.to_html(index=False,float_format=lambda v:f"{v:,.2f}")
    columns=["label","trades","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]
    matched_table=comparison.loc[comparison.period.eq("matched35_forecast_sessions"),columns]
    later_table=comparison.loc[comparison.period.eq("matched21_after_old_model_training"),columns]
    final_table=comparison.loc[comparison.period.eq("final14_assessment"),columns]
    content=f"""<!doctype html><html><head><meta charset="utf-8"><title>G8 — further research audit</title>
    <style>body{{font:16px system-ui;margin:32px;color:#21394e}}h1,h2{{color:#163f69}}table{{border-collapse:collapse;font-size:13px}}td,th{{padding:8px;border:1px solid #d6e1eb;text-align:right}}th{{background:#edf4fb}}.scroll{{overflow:auto}}.note{{background:#fff1cf;padding:18px}}</style></head><body>
    <h1>G8: does the improvement survive a stronger test?</h1>
    <p>40 additional policies compared weekly models trained only on earlier sessions, profit-aware regression,
    payoff-weighted classification, relative stock strength, volatility warmup and conservative veto combinations.
    Initial 21 sessions trained models; the following 35 sessions are matched forecast comparisons.</p>
    <div class="note">The chronological selection process retained the original baseline. Past-only model fitting is a stronger
    accounting check, but all dates and research ideas have already been inspected. This is still retrospective research,
    not an untouched holdout or proof of future profitability.</div>
    <h2>Exact comparison with the earlier model</h2><p>The earlier reported model used 24 inputs. We separately repeated its
    exact features, C=0.20 and score cutoff 0.50 with weekly earlier-session fits. A second version supplies the additional VIX
    warmup. The original frozen model is only applied from 21 September, after its training through 18 September.</p>
    <h2>Matched 35 forecast sessions: 31 August–22 October</h2><div class="scroll">{table(matched_table)}</div>
    <p>The frozen model row has baseline decisions during the first 14 of these sessions; compare its active performance
    only over the 21-session table below. Warmup-period baseline results must not be presented as model predictions.</p>
    <h2>Matched 21 sessions after original model training: 21 September–22 October</h2>
    <div class="scroll">{table(later_table)}</div>
    <h2>Final 14 sessions: 30 September–22 October</h2><div class="scroll">{table(final_table)}</div>
    <h2>Profit concentration</h2><div class="scroll">{table(concentration)}</div>
    <p>Profit excluding the largest gain day helps expose single-event dependence. It does not correct repeated selection bias.
    The simple 30-minute rule's 35-session gain came from two dates; that is limited independent evidence.</p>
    <h2>New 40-policy comparison</h2><div class="scroll">{table(ranked[['policy','trades','win_rate_pct','net_pnl_rs','losing_days']].head(15))}</div>
    <p>These rankings include assessment outcomes and cannot choose the default. Selection used only the first 21 forecast sessions,
    required better profit and win rate plus activity/risk limits, and selected <b>{summary['selected_policy']}</b>.</p>
    <h2>Practical interpretation</h2><p>Earlier full-history maxima remain useful candidate ideas, but they do not justify
    a claim that more tuning has produced a dependable edge. All 51 setups retain their original stop and target rules;
    no winning-day requirement, hindsight stock blacklist or altered capital was used to manufacture a better comparison.</p>
    <p>Costs are modeled 5bps round-trip, with 10/15bps checks. Capital and execution assumptions remain unchanged.
    Daily-close drawdown is not intraday mark-to-market drawdown. Production and paper trading settings remain unchanged.</p>
    <p><a href="../report.html">Full 40-policy study</a> | <a href="exact_model_daywise.csv">Exact-model day-wise CSV</a> |
    <a href="../daywise_results.csv">New-policy day-wise CSV</a> | <a href="../slot_side_results.csv">Slot-side CSV</a> |
    <a href="exact_model_costs.csv">Exact-model cost checks</a> | <a href="../past_only_predictions.csv">New model predictions</a> |
    <a href="exact_model_predictions.csv">Exact old-model predictions</a> | <a href="conclusion.json">Conclusion</a></p></body></html>"""
    (out/"review.html").write_text(content,encoding="utf-8")
    core.dump(out/"manifest.json",dict(parent_manifest_sha256=core.sha(root/"manifest.json"),
        code_sha256=core.sha(__file__),files={p.name:core.sha(p) for p in out.iterdir() if p.is_file() and p.name!="manifest.json"}))
    print(json.dumps(conclusion,indent=2))
    print(out/"review.html")
    return out


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir")
    audit(parser.parse_args().run_dir)
