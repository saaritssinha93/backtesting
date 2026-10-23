"""G8 research with past-only weekly forecasts and payoff-aware vetoes.

All market dates were examined before this experiment: chronological mechanics
are causal, but the research design and policy search remain retrospective.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from research import g8_vix_selection_research as core
from research.g8_exit_engine import metrics
from research.g8_vix_data import VixData, DEFAULT_5M

VERSION="V13-V10-G-8-PAST-ONLY-PAYOFF-V4"
WARMUP=core.OUTPUT_ROOT/"inputs/warmup/india_vix_5minute_2026-06-01_to_2026-07-24.csv"
CONTEXT=["baseline_target_pct","signed_nifty_slot","signed_nifty_session",
    "confirmation_close_position","confirmation_body_direction","stock_relative5_pct",
    "stock_relative_session_pct","confirmation_range_atr","time_fraction"]
VIX=["vix_level_ratio","signed_vix_session","signed_vix_recent",
     "signed_vix_previous_close","vix_acceleration_pct","vix_range30_pct"]


def specifications():
    out=[dict(id=core.BASELINE,mode="baseline"),dict(id="simple_vix30_2",mode="simple")]
    for threshold in (.5,1.):
        for weakness in (0.,-.05):
            out.append(dict(id=f"vix_relative_{threshold}_{weakness}",mode="relative",threshold=threshold,weakness=weakness))
    for window in ("expanding","rolling28"):
        for kind in ("binary","payoff_weighted"):
            for cutoff in (.45,.5,.55,.6):
                out.append(dict(id=f"{window}_{kind}_p{cutoff}",mode=kind,window=window,cutoff=cutoff))
        for alpha in (10.,100.):
            for cutoff in (0.,.1):
                out.append(dict(id=f"{window}_ridge_a{alpha}_r{cutoff}",mode="ridge",window=window,alpha=alpha,cutoff=cutoff))
        for p,r in ((.5,0.),(.55,.1)):
            out.append(dict(id=f"{window}_consensus_p{p}_r{r}",mode="consensus",window=window,p=p,r=r))
        for mode in ("vix_model_consensus","vix_model_combined"):
            out.append(dict(id=f"{window}_{mode}",mode=mode,window=window))
        out.append(dict(id=f"{window}_context_only_p0.5",mode="context_only",window=window,cutoff=.5))
    return out


def fit_forecasts(f,original,train_days):
    """Returns scores fit strictly on supplied earlier sessions, never test labels."""
    source=original.set_index("sid").loc[f.index]
    fit=f.day.isin(train_days)&source.filled&f.nifty_base_allowed
    returns=source.loc[fit,"net_return_pct"].to_numpy(float)
    binary=returns>0
    if len(binary)<80 or min(np.bincount(binary.astype(int),minlength=2))<10:
        raise ValueError("Insufficient past-only model data")
    sizes=f.loc[fit].groupby("day").sid.transform("count").to_numpy(float)
    daily_weight=1./sizes
    daily_weight/=daily_weight.mean()
    weighted=daily_weight*np.clip(np.abs(returns),.25,1.5)
    weighted/=weighted.mean()
    x=f[CONTEXT+VIX].astype(float)
    context=f[CONTEXT].astype(float)
    scores={}
    evidence=[]
    for kind,weights,matrix in (("binary",daily_weight,x),("payoff_weighted",weighted,x),("context_only",daily_weight,context)):
        model=make_pipeline(SimpleImputer(strategy="median",keep_empty_features=True),StandardScaler(),
            LogisticRegression(C=.2,max_iter=3000,random_state=20261023))
        model.fit(matrix.loc[fit],binary,logisticregression__sample_weight=weights)
        scores[kind]=pd.Series(model.predict_proba(matrix)[:,1],index=f.index)
        evidence.append(dict(kind=kind,columns=list(matrix.columns),coefficient=model[-1].coef_[0].tolist(),
            intercept=float(model[-1].intercept_[0]),imputer=model[0].statistics_.tolist(),
            scaler_mean=model[1].mean_.tolist(),scaler_scale=model[1].scale_.tolist()))
    for alpha in (10.,100.):
        model=make_pipeline(SimpleImputer(strategy="median",keep_empty_features=True),StandardScaler(),Ridge(alpha=alpha))
        model.fit(x.loc[fit],np.clip(returns,-1.5,2.),ridge__sample_weight=daily_weight)
        scores[f"ridge_{alpha}"]=pd.Series(model.predict(x),index=f.index)
        evidence.append(dict(kind="ridge",alpha=alpha,columns=list(x.columns),coefficient=model[-1].coef_.tolist(),
            intercept=float(model[-1].intercept_),imputer=model[0].statistics_.tolist(),
            scaler_mean=model[1].mean_.tolist(),scaler_scale=model[1].scale_.tolist()))
    return scores,dict(train_from=min(train_days),train_through=max(train_days),
        train_sessions=len(train_days),native_training_trades=int(fit.sum()),training_wins=int(binary.sum()),models=evidence)


def rule_mask(f,spec):
    base=f.nifty_base_allowed.astype(bool)
    if spec["mode"]=="baseline":
        return base
    if spec["mode"]=="simple":
        return base&~((f.side_sign*f.vix_session_pct>=2.)&(f.side_sign*f.vix_30_pct>=2.))
    if spec["mode"]=="relative":
        stress=(f.side_sign*f.vix_session_pct>=spec["threshold"])&(f.side_sign*f.vix_15_pct>=spec["threshold"])
        weak=f.stock_relative5_pct.lt(spec["weakness"])&f.stock_relative_session_pct.lt(spec["weakness"])
        return base&~(stress&weak)
    raise ValueError("Model specification needs forecasts")


def model_mask(f,spec,scores):
    base=f.nifty_base_allowed.astype(bool)
    mode=spec["mode"]
    if mode in ("binary","payoff_weighted","context_only"):
        return base&scores[mode].ge(spec["cutoff"])
    if mode=="ridge":
        return base&scores[f"ridge_{spec['alpha']}"].ge(spec["cutoff"])
    if mode=="consensus":
        # Reject only if BOTH win-score and expected-profit forecasts are poor.
        return base&~(scores["payoff_weighted"].lt(spec["p"])&scores["ridge_100.0"].lt(spec["r"]))
    simple=rule_mask(f,dict(mode="simple"))
    model=base&scores["binary"].ge(.5)
    if mode=="vix_model_consensus":
        return simple|model
    if mode=="vix_model_combined":
        return simple&model
    raise ValueError("Unknown model rule")


def past_only_masks(f,original,days):
    specs=specifications()
    masks={spec["id"]:f.nifty_base_allowed.copy() for spec in specs}
    for spec in specs:
        if "window" not in spec:
            masks[spec["id"]]=rule_mask(f,spec)
    proofs=[]
    predictions=[]
    for start in range(21,len(days),7):
        future=days[start:start+7]
        for window in ("expanding","rolling28"):
            training=days[:start] if window=="expanding" else days[max(0,start-28):start]
            if max(training)>=min(future):
                raise ValueError("Training overlaps forecast sessions")
            print(f"Past-only fit {window}: {training[0]}..{training[-1]} -> {future[0]}..{future[-1]}",flush=True)
            scores,proof=fit_forecasts(f,original,training)
            proofs.append(dict(window=window,test_from=future[0],test_through=future[-1],**proof))
            apply=f.day.isin(future)
            for spec in specs:
                if spec.get("window")==window:
                    decisions=model_mask(f,spec,scores)
                    masks[spec["id"]].loc[apply]=decisions.loc[apply]
            record=f.loc[apply,["sid","day","setup_id","side"]].copy()
            record["window"]=window
            record["train_through"]=max(training)
            for name,values in scores.items():
                record[name]=values.loc[apply]
            predictions.append(record)
    return masks,proofs,pd.concat(predictions,ignore_index=True)


def select_policy(trial):
    dev=trial.loc[trial.period.eq("past_forecast_development")].set_index("policy",drop=False)
    b=dev.loc[core.BASELINE]
    good=dev.loc[(dev.net_pnl_rs>b.net_pnl_rs+1e-6)&(dev.win_rate_pct>b.win_rate_pct+.25)&
        (dev.trades>=.8*b.trades)&(dev.losing_days<=b.losing_days)&
        (dev.daily_close_drawdown_rs<=max(1.,1.1*b.daily_close_drawdown_rs))&
        (dev.changed_executions>=5)&(dev.changed_days>=3)]
    if good.empty:
        return core.BASELINE
    return str(good.reset_index(drop=True).sort_values(["net_pnl_rs","win_rate_pct","policy"],
        ascending=[False,False,True]).iloc[0].policy)


def run(parent,output=None):
    from research.g8_stock_context import load_stock_context
    parent=Path(parent).resolve()
    core.verify(parent)
    out=Path(output or core.OUTPUT_ROOT/"past_only_research"/core.CUTOFF/
        (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_past_only_v4"))
    out.mkdir(parents=True,exist_ok=False)
    code={str(p.relative_to(core.ROOT)):core.sha(p) for p in (Path(__file__),Path(core.__file__),
        core.ROOT/"research/g8_exit_engine.py",core.ROOT/"research/g8_stock_context.py",
        core.ROOT/"research/g8_vix_data.py",core.ROOT/"research/g8_nifty_vix_backtest.py",
        core.ROOT/"research/g8_sources.py")}
    core.dump(out/"predeclared_plan.json",dict(version=VERSION,cutoff=core.CUTOFF,candidates=specifications(),
        model_features=CONTEXT+VIX,initial_training_sessions=21,refit_every_sessions=7,
        development_forecast_sessions="21..41",locked_assessment_sessions="42..55",
        weights="equal session weights; payoff-logistic multiplies by abs(netreturn) clipped0.25..1.5",
        regression_target="net return percentage clipped -1.5..2.0",independent_holdout=False,
        instruction="No later assessment result selects the locked policy; historical maxima remain exploratory"))
    source=core.load_sources()
    # Freeze extended VIX history, retaining provenance for both source files.
    original_vix_proof=json.loads((DEFAULT_5M.parent/"india_vix_provenance.json").read_text(encoding="utf-8"))
    warmup_proof=json.loads((WARMUP.parent/"india_vix_warmup_2026-06-01_to_2026-07-24_provenance.json").read_text(encoding="utf-8"))
    input_hashes={str(p):core.sha(p) for p in (DEFAULT_5M,WARMUP)}
    if input_hashes[str(DEFAULT_5M)]!=original_vix_proof["files"][DEFAULT_5M.name]:
        raise ValueError("Original VIX provenance differs")
    # The warmup retrieval records its two interval files under the same files map.
    if input_hashes[str(WARMUP)]!=warmup_proof["files"][WARMUP.name]:
        raise ValueError("Warmup VIX provenance differs")
    old=pd.read_csv(DEFAULT_5M)
    warm=pd.read_csv(WARMUP)
    vix=pd.concat([warm,old],ignore_index=True)
    if pd.to_datetime(vix.date,utc=True).duplicated().any():
        raise ValueError("Overlapping warmup candles")
    vix=vix.sort_values("date")
    vix.to_csv(out/"extended_vix.csv",index=False)
    if any(core.sha(p)!=digest for p,digest in input_hashes.items()):
        raise ValueError("VIX input changed while freezing")
    f=core.build_features(source,VixData(out/"extended_vix.csv"))
    if not f.vix_level_ready.all():
        raise ValueError("Insufficient VIX warmup for a stock session")
    stock=load_stock_context(source)
    if set(stock.index)!=set(f.index):
        raise ValueError("Missing causal stock context")
    f=f.join(stock.drop(columns=[c for c in stock if c in f]),how="left",validate="one_to_one")
    f.to_csv(out/"features.csv",index=False)
    config=deepcopy(source["config"])
    config.update(strategy="V13-V10-G-8",variant_id=VERSION,scope="BACKTEST_ONLY",execution_authority=False)
    fast=core.FastSelection(source["unfiltered"],config)
    days=source["days"]
    masks,proofs,predictions=past_only_masks(f,source["unfiltered"],days)
    core.dump(out/"model_fit_proofs.json",proofs)
    predictions.to_csv(out/"past_only_predictions.csv",index=False)
    periods=dict(past_forecast_development=days[21:42],locked_assessment=days[42:],
        all_past_forecasts=days[21:],full_history_including_warmup=days)
    trial,executions=core.evaluate(masks,fast,periods)
    trial.to_csv(out/"parameter_trials.csv",index=False)
    chosen=select_policy(trial)
    whole=trial.loc[trial.period.eq("all_past_forecasts")].set_index("policy")
    b=whole.loc[core.BASELINE]
    better=whole.loc[(whole.net_pnl_rs>b.net_pnl_rs)&(whole.win_rate_pct>b.win_rate_pct)]
    historical=str(better.sort_values("net_pnl_rs",ascending=False).index[0]) if len(better) else core.BASELINE
    labels=dict(baseline=core.BASELINE,locked_selected=chosen,exploratory_best_past_forecasts=historical,
        simple_vix30="simple_vix30_2")
    books={}
    comparisons=[]
    costs=[]
    folds=[]
    daily=[]
    slots=[]
    robustness=[]
    for label,name in labels.items():
        print("Full portfolio verification:",label,name,flush=True)
        book=fast.full_book(masks[name],name)
        books[label]=book
        (out/label).mkdir()
        book.to_csv(out/label/"portfolio_trades.csv",index=False)
        for period,calendar in periods.items():
            comparisons.append(dict(label=label,policy=name,period=period,**metrics(book,calendar)))
        for cost in (5.,10.,15.):
            for period in ("all_past_forecasts","locked_assessment"):
                costs.append(dict(label=label,policy=name,cost_bps=cost,period=period,
                    **fast.stats(executions[name],periods[period],cost)))
        for start in range(21,len(days),7):
            calendar=days[start:start+7]
            folds.append(dict(label=label,policy=name,from_day=calendar[0],through_day=calendar[-1],**metrics(book,calendar)))
        for day in days:
            daily.append(dict(label=label,day=day,**metrics(book,[day])))
        for setup in config["active_setups"]:
            slots.append(dict(label=label,setup_id=setup["setup_id"],side=setup["side"],slot=setup["confirmation_end"],
                **metrics(book.loc[book.setup_id.eq(setup["setup_id"])],days[21:])))
    comparison=pd.DataFrame(comparisons)
    comparison.to_csv(out/"overall_comparison.csv",index=False)
    pd.DataFrame(costs).to_csv(out/"cost_sensitivity.csv",index=False)
    pd.DataFrame(folds).to_csv(out/"weekly_results.csv",index=False)
    daily=pd.DataFrame(daily)
    daily.to_csv(out/"daywise_long.csv",index=False)
    pivot=daily.pivot(index="day",columns="label",values="net_pnl_rs").reset_index()
    for label in labels:
        if label!="baseline":
            pivot[label+"_delta_rs"]=pivot[label]-pivot.baseline
    pivot.to_csv(out/"daywise_results.csv",index=False)
    pd.DataFrame(slots).to_csv(out/"slot_side_results.csv",index=False)
    # Concentration check: exclude best improvement day; never select with this assessment.
    for label,name in labels.items():
        if label=="baseline":
            continue
        deltas=pivot.loc[pivot.day.isin(days[21:]),label+"_delta_rs"]
        dates=pivot.loc[pivot.day.isin(days[21:]),"day"]
        net_delta=float(deltas.sum())
        largest=float(deltas.max())
        robustness.append(dict(label=label,total_net_change_rs=net_delta,best_gain_day=str(dates.loc[deltas.idxmax()]),
            best_day_change_rs=largest,net_change_without_best_day_rs=net_delta-largest,
            improvement_days=int(deltas.gt(1e-8).sum()),deterioration_days=int(deltas.lt(-1e-8).sum())))
    pd.DataFrame(robustness).to_csv(out/"gain_concentration.csv",index=False)
    core.dump(out/"selected_config.json",dict(source_configuration=config,selected_policy=chosen,
        spec=next(s for s in specifications() if s["id"]==chosen),weekly_fit=True,
        status="RESEARCH_ONLY",selection_at=days[42],selection_uses_forecasts_through=days[41],
        independent_holdout=False,execution_authority=False))
    decisions=f[["sid","day","setup_id","confirmation_ts"]].copy()
    for label,name in labels.items():
        decisions[label+"_allowed"]=masks[name]
    decisions.to_csv(out/"entry_decisions.csv",index=False)
    summary=dict(version=VERSION,through=core.CUTOFF,sessions=len(days),forecast_sessions=len(days[21:]),
        candidate_count=len(masks),selected_policy=chosen,exploratory_best_past_forecasts=historical,
        source_parent=str(parent),source_parent_manifest_sha256=core.sha(parent/"manifest.json"),
        input_hashes=input_hashes,source_cache_manifest_sha256=source["manifest_sha256"],
        independent_holdout=False,execution_authority=False,
        limitations=["Model training uses only earlier sessions, but dates and research design were already examined.",
            "First21 sessions are model warmup; compare matched35 forecast sessions or final14 assessment sessions.",
            "Model readiness flags are excluded; volatility levels use prior sessions with extended warmup.",
            "No new stock ranking/replacement, target change, position size increase, or hindsight stock blacklist.",
            "Five weekly blocks and56 sessions are limited evidence, especially after repeated research."])
    core.dump(out/"summary.json",summary)
    write_report(out,summary,comparison,pivot,pd.DataFrame(robustness))
    if any(core.sha(core.ROOT/p)!=digest for p,digest in code.items()):
        raise ValueError("Implementation changed during run")
    core.dump(out/"manifest.json",dict(code=code,files={str(p.relative_to(out)).replace("\\","/"):core.sha(p)
        for p in out.rglob("*") if p.is_file()}))
    verify(out)
    core.dump(core.OUTPUT_ROOT/"latest_past_only_research.json",dict(run_dir=str(out),manifest_sha256=core.sha(out/"manifest.json"),
        selected_policy=chosen,execution_authority=False))
    print(json.dumps(summary,indent=2))
    print(out)
    return out


def write_report(out,summary,comparison,daily,concentration):
    def table(frame):
        return frame.to_html(index=False,float_format=lambda v:f"{v:,.2f}")
    cols=["label","period","trades","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]
    content=f"""<!doctype html><html><head><meta charset="utf-8"><title>G8 past-only forecasts</title>
    <style>body{{font:15px system-ui;margin:32px;color:#21384d}}table{{border-collapse:collapse;font-size:13px}}td,th{{padding:8px;border:1px solid #d7e0ea;text-align:right}}th{{background:#eaf2fa}}.scroll{{overflow:auto}}.note{{background:#fff1cf;padding:18px}}</style></head><body>
    <h1>G8: past-only forecasts, payoff-aware selection</h1><p>{summary['candidate_count']} policies; 56 eligible sessions through 22 October 2026.
    Initial 21 sessions train models. Next 35 sessions receive forecasts from models fitted only on earlier sessions.
    A policy is selected on the first 21 forecast sessions and locked for the final 14, while model coefficients refit weekly using earlier days.</p>
    <div class="note">All these historical dates have already been examined. Removing direct future-label use does not remove
    researcher selection bias or establish an independent trading edge. The full-history figures include 21 baseline warmup sessions.</div>
    <h2>Results</h2><div class="scroll">{table(comparison[cols])}</div>
    <h2>Selection</h2><p>Locked policy: <b>{summary['selected_policy']}</b>. Historical best across all forecast sessions:
    <b>{summary['exploratory_best_past_forecasts']}</b> (exploratory only).</p>
    <p>Selection requires higher development profit and win rate, at least 80% of baseline trades, no extra losing days,
    drawdown within 110%, and at least 5 changed executions across 3 days. Later assessment outcomes never choose this policy.</p>
    <h2>What changed</h2><p>Models compare ordinary win prediction, payoff-weighted win prediction, and regularized expected net return.
    Consensus vetoes require both low win score and poor expected return before blocking an entry. Expanding and latest-28-session
    training windows test adaptation. Context-only models assess the incremental role of VIX. Scores are not calibrated probabilities.</p>
    <p>Stock-relative strength uses stock and Nifty prices over matching completed intervals. Confirmation range is normalized by
    strictly earlier current-session ATR; insufficient early history remains missing and is imputed from training only.
    VIX levels have 39 additional prior sessions for warmup. No readiness/calendar-start indicator is fed into models.</p>
    <h2>Concentration</h2><div class="scroll">{table(concentration)}</div>
    <h2>Day-wise results</h2><div class="scroll">{table(daily)}</div>
    <h2>Costs and execution</h2><p>Figures use modeled 5bps round-trip costs, with 10/15bps sensitivity in the linked file.
    Original SL 1.20%, tightening to 1.00% after 120 minutes, original slot targets 0.60–2.00%, 15:15 square-off.
    INR 10 lakh portfolio, INR 1 lakh capital per position and 5x exposure; original stop-first/gap conventions and fractional quantities.
    Drawdown is daily-close. No production or paper settings changed.</p>
    <p><a href="parameter_trials.csv">All policies</a> | <a href="daywise_results.csv">Day-wise CSV</a> |
    <a href="slot_side_results.csv">Slot-side CSV</a> | <a href="weekly_results.csv">Weekly results</a> |
    <a href="past_only_predictions.csv">Predictions and train cutoffs</a> | <a href="model_fit_proofs.json">Fit evidence</a> |
    <a href="cost_sensitivity.csv">Trading cost checks</a> | <a href="selected_config.json">Selected config</a></p></body></html>"""
    (out/"report.html").write_text(content,encoding="utf-8")


def verify(out):
    out=Path(out).resolve()
    manifest=json.loads((out/"manifest.json").read_text(encoding="utf-8"))
    for name,digest in manifest["files"].items():
        p=(out/name).resolve()
        if not p.is_relative_to(out) or core.sha(p)!=digest:
            raise ValueError("Changed result artifact: "+name)
    for name,digest in manifest["code"].items():
        p=(core.ROOT/name).resolve()
        if not p.is_relative_to(core.ROOT) or core.sha(p)!=digest:
            raise ValueError("Changed implementation: "+name)
    prediction=pd.read_csv(out/"past_only_predictions.csv")
    if not (prediction.train_through<prediction.day).all():
        raise ValueError("Future training date")
    if prediction.duplicated(["sid","window"]).any():
        raise ValueError("Duplicate forecast")
    f=pd.read_csv(out/"features.csv")
    if not (pd.to_datetime(f.vix_available_at,utc=True)<=pd.to_datetime(f.confirmation_ts,utc=True)).all():
        raise ValueError("Future VIX feature")
    summary=json.loads((out/"summary.json").read_text(encoding="utf-8"))
    trial=pd.read_csv(out/"parameter_trials.csv")
    if select_policy(trial)!=summary["selected_policy"]:
        raise ValueError("Policy selection differs")
    return True


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("parent")
    parser.add_argument("--output")
    parser.add_argument("--verify",action="store_true")
    args=parser.parse_args()
    print(verify(args.parent)) if args.verify else run(args.parent,args.output)
