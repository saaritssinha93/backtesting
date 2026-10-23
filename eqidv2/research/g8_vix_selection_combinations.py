"""Fourth G8 selection iteration: combine complementary weak VIX filters.

Pair discovery uses training data only. Discovery permits a rule with >=95%
profit retention and win rate within0.5pp, but promotion keeps the original
strict requirement of higher profit AND win rate with actual validation activity.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
from itertools import combinations
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g8_vix_selection_research as core
from research.g8_exit_engine import metrics

VERSION="V13-V10-G-8-VIX-SELECTION-COMBINATIONS-V3"


def discover_pairs(specs, table):
    table=table.loc[table.period.eq("development")].set_index("policy")
    baseline=table.loc[core.BASELINE]
    families={}
    for spec in specs:
        if spec["family"] in ("baseline","reference"):
            continue
        row=table.loc[spec["id"]]
        if not (row.net_pnl_rs>=.95*baseline.net_pnl_rs and row.win_rate_pct>=baseline.win_rate_pct-.5
                and row.trades>=.7*baseline.trades and row.changed_executions>=2 and row.changed_days>=2):
            continue
        score=(float(row.net_pnl_rs),float(row.win_rate_pct),spec["id"])
        existing=families.get(spec["family"])
        if existing is None or score>existing[0]:
            families[spec["family"]]=(score,spec)
    chosen=[item[1] for item in sorted(families.values(),key=lambda item:item[0],reverse=True)[:4]]
    out=[]
    for a,b in combinations(chosen,2):
        for join in ("both_filters","consensus_veto"):
            out.append(dict(id=f"{join}__{a['id']}__{b['id']}",family="combined",mode="combined",join=join,first=a,second=b))
    return out


def fit(f,original,fast,training,validation,test):
    periods=dict(development=training,validation=validation,final_test=test,all=fast.days)
    atomic=core.rule_specs()
    masks={s["id"]:core.allowed_for(f,s) for s in atomic}
    single,_=core.evaluate(masks,fast,periods)
    pairs=discover_pairs(atomic,single)
    masks.update({s["id"]:core.allowed_for(f,s) for s in pairs})
    model_masks,models=core.model_masks(f,original,training)
    masks.update(model_masks)
    table,executions=core.evaluate(masks,fast,periods)
    candidate=core.train_choice(table)
    val=table.loc[table.period.eq("validation")].set_index("policy")
    passed=candidate!=core.BASELINE and core.validation_eligible(val.loc[candidate],val.loc[core.BASELINE])
    return dict(table=table,masks=masks,executions=executions,pairs=pairs,models=models,
                chosen=candidate,selected=candidate if passed else core.BASELINE,validation_passed=passed)


def run(parent,output=None):
    parent=Path(parent).resolve()
    core.verify(parent)
    out=Path(output or core.OUTPUT_ROOT/"selection_research"/core.CUTOFF/
        (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_vix_combinations_v3"))
    out.mkdir(parents=True,exist_ok=False)
    code={str(p.relative_to(core.ROOT)):core.sha(p) for p in (Path(__file__),Path(core.__file__),
        core.ROOT/"research/g8_exit_engine.py",core.ROOT/"research/g8_sources.py")}
    core.dump(out/"predeclared_plan.json",dict(version=VERSION,parent_run=str(parent),
        parent_manifest_sha256=core.sha(parent/"manifest.json"),pair_budget=12,
        discovery="top four families on training only: >=95% profit, win rate within0.5pp,70% trades,>=2 changes on>=2 days",
        promotion="unchanged strict training and active validation guards from selection V2",
        why_new_iteration="strict single-rule discovery produced no pairs; test whether consensus of weaker filters helps",
        independent_holdout=False))
    source=core.load_sources()
    f=pd.read_csv(parent/"features.csv").set_index("sid",drop=False)
    config=deepcopy(source["config"])
    config.update(strategy="V13-V10-G-8",variant_id=VERSION,scope="BACKTEST_ONLY",execution_authority=False)
    fast=core.FastSelection(source["unfiltered"],config)
    days=fast.days
    print("Iteration 4: discover complementary pairs on first35 sessions",flush=True)
    fit_result=fit(f,source["unfiltered"],fast,days[:35],days[35:45],days[45:])
    trial=fit_result["table"]
    trial.to_csv(out/"parameter_trials.csv",index=False)
    core.dump(out/"discovered_pairs.json",fit_result["pairs"])
    core.dump(out/"fitted_models.json",fit_result["models"])
    all_rows=trial.loc[trial.period.eq("all")].set_index("policy")
    rule_ids=[s["id"] for s in core.rule_specs()+fit_result["pairs"]]
    best_rule=str(all_rows.loc[rule_ids].sort_values(["net_pnl_rs","win_rate_pct"],ascending=False).index[0])
    b=all_rows.loc[core.BASELINE]
    better=all_rows.loc[(all_rows.net_pnl_rs>b.net_pnl_rs)&(all_rows.win_rate_pct>b.win_rate_pct)]
    best_both=str(better.sort_values(["net_pnl_rs","win_rate_pct"],ascending=False).index[0]) if len(better) else core.BASELINE
    labels=dict(baseline=core.BASELINE,previous_g8="previous_g8_selected",development_candidate=fit_result["chosen"],
        selected=fit_result["selected"],historical_best_rule=best_rule,historical_best_both=best_both)
    periods=dict(development=days[:35],validation=days[35:45],final_test=days[45:],all=days)
    comparisons,books,daily,slots,costs=[],{},[],[],[]
    for label,name in labels.items():
        print("Full allocator check:",label,name,flush=True)
        book=fast.full_book(fit_result["masks"][name],name)
        books[label]=book
        (out/label).mkdir()
        book.to_csv(out/label/"portfolio_trades.csv",index=False)
        for period,calendar in periods.items():
            comparisons.append(dict(label=label,policy=name,period=period,**metrics(book,calendar)))
        for day in days:
            daily.append(dict(label=label,day=day,**metrics(book,[day])))
        for setup in config["active_setups"]:
            slots.append(dict(label=label,setup_id=setup["setup_id"],slot=setup["confirmation_end"],side=setup["side"],
                **metrics(book.loc[book.setup_id.eq(setup["setup_id"])],days)))
        for side in ("LONG","SHORT"):
            slots.append(dict(label=label,setup_id="ALL_"+side,slot="ALL",side=side,
                **metrics(book.loc[book.side.eq(side)],days)))
        for bps in (5.,10.,15.):
            for period,calendar in (("all",days),("final_test",days[45:])):
                costs.append(dict(label=label,period=period,cost_bps=bps,
                    **fast.stats(fit_result["executions"][name],calendar,bps)))
    rolling,choices=[],[]
    for start in (35,42,49):
        print("Rolling pair discovery:",days[start],"to",days[start+6],flush=True)
        r=fit(f,source["unfiltered"],fast,days[:start-7],days[start-7:start],days[start:start+7])
        book=fast.full_book(r["masks"][r["selected"]],r["selected"])
        rolling.append(book.loc[book.day.isin(days[start:start+7])].copy())
        choices.append(dict(train_through=days[start-8],validation_from=days[start-7],validation_through=days[start-1],
            test_from=days[start],test_through=days[start+6],candidate=r["chosen"],selected_policy=r["selected"],
            validation_passed=r["validation_passed"],candidate_count=len(r["masks"]),pair_count=len(r["pairs"])))
        r["table"].to_csv(out/f"rolling_trials_{start}.csv",index=False)
        core.dump(out/f"rolling_fit_{start}.json",dict(pairs=r["pairs"],models=r["models"]))
    rolling=pd.concat(rolling,ignore_index=True)
    rolling.to_csv(out/"rolling_trades.csv",index=False)
    pd.DataFrame(choices).to_csv(out/"rolling_choices.csv",index=False)
    for label,book in (("rolling_selected",rolling),("rolling_matched_baseline",books["baseline"])):
        comparisons.append(dict(label=label,policy="varies" if label=="rolling_selected" else core.BASELINE,
            period="rolling_last21",**metrics(book,days[35:])))
    comparison=pd.DataFrame(comparisons)
    comparison.to_csv(out/"overall_comparison.csv",index=False)
    daily=pd.DataFrame(daily)
    daily.to_csv(out/"daywise_all_policies.csv",index=False)
    pivot=daily.pivot(index="day",columns="label",values="net_pnl_rs").reset_index()
    for label in labels:
        if label!="baseline":
            pivot[label+"_delta_rs"]=pivot[label]-pivot.baseline
    pivot.to_csv(out/"daywise_results.csv",index=False)
    slots=pd.DataFrame(slots)
    slots.to_csv(out/"slot_side_results.csv",index=False)
    pd.DataFrame(costs).to_csv(out/"cost_sensitivity.csv",index=False)
    f.to_csv(out/"features.csv",index=False)
    decisions=f.copy()
    for label,name in labels.items():
        decisions[label+"_allowed"]=fit_result["masks"][name]
    decisions.to_csv(out/"feature_decisions.csv",index=False)
    avoided={}
    baseline=books["baseline"].set_index("sid")
    for label,book in books.items():
        indexed=book.set_index("sid")
        lost=baseline.loc[baseline.portfolio_executed&~indexed.portfolio_executed,
            ["day","setup_id","tradingsymbol","side","portfolio_net_profit_rupees"]]
        gained=indexed.loc[indexed.portfolio_executed&~baseline.portfolio_executed,
            ["day","setup_id","tradingsymbol","side","portfolio_net_profit_rupees"]]
        lost.to_csv(out/(label+"_avoided_trades.csv"))
        gained.to_csv(out/(label+"_newly_allocated_trades.csv"))
        avoided[label]=lost.reset_index()
    specs={s["id"]:s for s in core.rule_specs()+core.model_specs()+fit_result["pairs"]}
    config["g8_selection"]=dict(policy=fit_result["selected"],spec=specs[fit_result["selected"]],
        rollback=core.BASELINE,exits_unchanged=True,independent_holdout=False)
    core.dump(out/"selected_config.json",config)
    top=all_rows.reset_index().sort_values(["net_pnl_rs","win_rate_pct"],ascending=False).head(15)
    top=top[["policy","trades","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]]
    top.to_csv(out/"historical_top15.csv",index=False)
    summary=dict(version=VERSION,through=core.CUTOFF,sessions=len(days),candidate_count=len(fit_result["masks"]),
        pair_count=len(fit_result["pairs"]),development_selected=fit_result["chosen"],selected_policy=fit_result["selected"],
        validation_passed=fit_result["validation_passed"],historical_best_rule=best_rule,historical_best_both=best_both,
        parent_run=str(parent),parent_manifest_sha256=core.sha(parent/"manifest.json"),
        source_cache_manifest_sha256=source["manifest_sha256"],independent_holdout=False,execution_authority=False,
        selected_metrics=metrics(books["selected"],days),baseline_metrics=metrics(books["baseline"],days))
    core.dump(out/"summary.json",summary)
    core.write_report(out,summary,comparison,pivot,slots.loc[slots.label.eq("selected")],avoided["selected"],top)
    if any(core.sha(core.ROOT/name)!=digest for name,digest in code.items()):
        raise ValueError("Implementation changed during iteration")
    core.dump(out/"manifest.json",dict(code=code,files={str(p.relative_to(out)).replace("\\","/"):core.sha(p)
        for p in out.rglob("*") if p.is_file()}))
    core.verify(out)
    core.dump(core.OUTPUT_ROOT/"latest_selection_research.json",dict(run_dir=str(out),manifest_sha256=core.sha(out/"manifest.json"),
        selected_policy=fit_result["selected"],execution_authority=False))
    print(json.dumps(summary,indent=2))
    print(out)
    return out


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("parent")
    parser.add_argument("--output")
    args=parser.parse_args()
    run(args.parent,args.output)
