"""Selective observer research; never edits frozen G-3 trades or live rules.

Dates, not minute rows, separate fitting, selection and historical evaluation.
All history has previously been inspected: this is not prospective validation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import shutil
import sys

import numpy as np
import pandas as pd
import joblib
import sklearn
from scipy.stats import beta
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research.g3_pullback_selective_features import FEATURES, build_features
from research.g3_freeze import load_frozen
from research.g3_pullback_tuning import _boolean

BASE = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_3")
SOURCE = BASE / "pullback_monitor_v1_20261008_resolved_exits"
STAGES = ("ENTRY_RISK", "PROFIT_GIVEBACK")
THRESHOLDS = (.20, .30, .40, .50, .60, .70, .80, .90, .95, .99)
PROTOCOL = {
    "version": "G3_SELECTIVE_PULLBACK_V2",
    "observer_only": True,
    "outcomes": "New adverse move >=0.30%/5m or >=0.50%/30m before earlier of horizon or frozen exit; unknown intrabar ordering excluded",
    "stages": {"ENTRY_RISK": "age <=30 minutes", "PROFIT_GIVEBACK": "age >30 minutes and best completed-close favorable excursion >=0.30%", "otherwise": "LATE_NO_ESTABLISHED_PROFIT: abstain"},
    "split": "first20 fit; next10 select model and thresholds; last16 historical evaluation, already exposed by previous research",
    "training": "all resolved minute rows; equal total weight per trade within stage/horizon; no class balancing; preprocessing fitted on first20 only",
    "models": ["constant empirical reference", "logistic C=0.1", "random forest 200 trees depth3 min_leaf20 max_features1.0 seed20261008"],
    "model_selection": "lowest CHECK10 trade-balanced Brier score; must beat constant reference; no refit after selection",
    "thresholds": THRESHOLDS,
    "watch_gate": "CHECK10: >=5 resolved alert episodes on >=3 trades and >=3 dates; both grid and episode precision >=0.50, grid recall >=0.10, episode precision >=1.5 times grid event prevalence; maximize grid F0.5 then higher threshold",
    "high_gate": "CHECK10: >=20 resolved alerts on >=10 trades and >=5 dates; grid and episode precision >=0.95; grid recall >=0.10; one-sided 95% exact-binomial lower bound >=0.90 and day-bootstrap 95% lower >=0.90; no unresolved alerts; maximize grid recall then higher threshold",
    "cooldown": "horizon minutes per trade and policy, shared across stages; no use of labels to decide cooldown",
    "combined_gate": "After stage-local selection, reject any selected stage threshold that fails its gate on CHECK10 under shared cooldown; repeat until stable; never replace rejected thresholds",
    "evaluation": "fixed nonoverlapping anchors for recall/no-warning risk; every-minute alerts with cooldown for precision; first alerts also shown; stage subgroup alerts come from full policy cooldown",
    "score_interpretation": "Uncalibrated exploratory risk scores, not validated probabilities; no signal is not an all-clear",
    "promotion": "No live promotion from reused history. Prospective validation required even if historical gates pass.",
    "confidence_caution": "Date-cluster intervals are descriptive pointwise intervals. Binomial bounds assume independence and do not prove reliability of correlated alerts or selected models.",
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def ratio(a, b):
    return float(a / b) if b else math.nan


def clean_json(value):
    if isinstance(value, dict):
        return {str(k): clean_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def write_json(path, value):
    Path(path).write_text(json.dumps(clean_json(value), indent=2, allow_nan=False), encoding="utf-8")


def balanced_weights(frame):
    counts = frame.groupby("trade_id").trade_id.transform("size")
    weights = 1.0 / counts.to_numpy()
    return weights * len(weights) / weights.sum() if len(weights) else weights


def alert_mask(frame, warnings):
    """Shared trade/horizon cooldown is based only on emitted warnings."""
    result = pd.Series(False, index=frame.index)
    flagged = frame.loc[warnings].sort_values("decision_ts")
    for _, group in flagged.groupby(["trade_id", "horizon_minutes"], sort=False):
        next_allowed = None
        for idx, stamp, horizon in zip(group.index, group.decision_ts, group.horizon_minutes):
            stamp = pd.Timestamp(stamp)
            if next_allowed is None or stamp >= next_allowed:
                result.loc[idx] = True
                next_allowed = stamp + pd.Timedelta(minutes=int(horizon))
    return result


def exact_lower(hits, total):
    return float(beta.ppf(.05, hits, total-hits+1)) if hits and total else (0. if total else math.nan)


def cluster_interval(alerts, days, reps=2000):
    if alerts.empty:
        return math.nan, math.nan
    daily = alerts.groupby("day").event.agg(["sum", "count"]).reindex(days, fill_value=0)
    rng = np.random.default_rng(20261008)
    weights = rng.multinomial(len(days), np.full(len(days), 1/len(days)), size=reps)
    counts = weights @ daily.to_numpy()
    valid = counts[:, 1] > 0
    return tuple(np.quantile(counts[valid, 0]/counts[valid, 1], [.025, .975]))


def score_policy(frame, warnings, days, emitted=None, bootstrap=True):
    grid = frame.loc[frame.is_grid & frame.outcome_available]
    w, y = _boolean(warnings.loc[grid.index]), _boolean(grid.event)
    tp, fp, fn, tn = (int((w & y).sum()), int((w & ~y).sum()), int((~w & y).sum()), int((~w & ~y).sum()))
    emitted = alert_mask(frame, warnings) if emitted is None else emitted
    all_alerts = frame.loc[emitted]
    alerts = all_alerts.loc[all_alerts.outcome_available]
    hits, n = int(alerts.event.sum()), len(alerts)
    first = all_alerts.sort_values("decision_ts").drop_duplicates(["trade_id", "horizon_minutes"])
    first_resolved = first.loc[first.outcome_available]
    low, high = cluster_interval(alerts, days) if bootstrap else (math.nan, math.nan)
    full = grid.loc[grid.full_horizon_eligible]
    full_no = full.loc[~warnings.loc[full.index]]
    return dict(grid_rows=int(frame.is_grid.sum()), resolved_grid_rows=len(grid), grid_events=tp+fn,
        grid_warnings=tp+fp, grid_tp=tp, grid_fp=fp, grid_fn=fn, grid_tn=tn,
        grid_precision=ratio(tp,tp+fp), grid_recall=ratio(tp,tp+fn), grid_prevalence=ratio(tp+fn,len(grid)),
        grid_f05=ratio(1.25*tp,1.25*tp+.25*fn+fp), no_warning_event_rate=ratio(fn,fn+tn), npv=ratio(tn,fn+tn),
        full_horizon_no_warning_rows=len(full_no), full_horizon_no_warning_event_rate=ratio(int(full_no.event.sum()),len(full_no)),
        alert_count=len(all_alerts), resolved_alerts=n, unknown_alerts=len(all_alerts)-n,
        successful_alerts=hits, false_alerts=n-hits, alert_precision=ratio(hits,n),
        precision_identification_lower=ratio(hits,len(all_alerts)),
        warned_trades=all_alerts.trade_id.nunique(), warned_days=all_alerts.day.nunique(),
        resolved_warned_trades=alerts.trade_id.nunique(), resolved_warned_days=alerts.day.nunique(),
        median_lead_minutes=float(alerts.loc[alerts.event,"lead_minutes"].median()),
        alert_precision_low=low, alert_precision_high=high, exact_independent_lower=exact_lower(hits,n),
        first_alert_count=len(first), first_resolved_alerts=len(first_resolved),
        first_alert_precision=ratio(int(first_resolved.event.sum()),len(first_resolved)))


def make_model(name):
    if name == "logistic":
        return make_pipeline(SimpleImputer(strategy="median", add_indicator=True), StandardScaler(),
                             LogisticRegression(C=.1, max_iter=2000, random_state=20261008))
    if name == "forest":
        return make_pipeline(SimpleImputer(strategy="median", add_indicator=True),
            RandomForestClassifier(n_estimators=200,max_depth=3,min_samples_leaf=20,
                                   max_features=1.0,random_state=20261008,n_jobs=1))
    raise ValueError(name)


def eligibility(stat, beats_constant):
    enough=stat["resolved_alerts"]>=5 and stat["resolved_warned_trades"]>=3 and stat["resolved_warned_days"]>=3
    watch=bool(beats_constant and enough and stat["alert_precision"]>=.5 and
        stat["grid_precision"]>=.5 and stat["grid_recall"]>=.1 and
        stat["alert_precision"]>=1.5*stat["grid_prevalence"])
    high=bool(beats_constant and stat["resolved_alerts"]>=20 and stat["resolved_warned_trades"]>=10 and
        stat["resolved_warned_days"]>=5 and stat["unknown_alerts"]==0 and stat["alert_precision"]>=.95 and
        stat["grid_precision"]>=.95 and stat["grid_recall"]>=.1 and
        stat["exact_independent_lower"]>=.90 and stat["alert_precision_low"]>=.90)
    return watch,high


def select_models(frame, days):
    """Later dates and their outcomes are excluded before any model operations."""
    development = frame.loc[frame.day.isin(days[:30])].copy()
    for column in ("event","outcome_available","is_grid","full_horizon_eligible"):
        development[column]=_boolean(development[column])
    selected, fitted, model_rows, threshold_rows = {}, {}, [], []
    for stage in STAGES:
        for horizon in (5,30):
            key = f"{stage}_{horizon}"
            subset = development.loc[development.model_stage.eq(stage) & development.horizon_minutes.eq(horizon)]
            fit = subset.loc[subset.day.isin(days[:20]) & subset.outcome_available]
            check = subset.loc[subset.day.isin(days[20:30])]
            resolved = check.loc[check.outcome_available]
            info = dict(stage=stage,horizon_minutes=horizon,fit_rows=len(fit),fit_trades=fit.trade_id.nunique(),
                        check_rows=len(resolved),check_trades=resolved.trade_id.nunique(),model=None,
                        watch_threshold=None,high_threshold=None,status="INSUFFICIENT_TRAINING_SUPPORT")
            selected[key] = info
            if fit.trade_id.nunique()<5 or fit.event.nunique()<2 or resolved.event.nunique()<2:
                continue
            fit_w, check_w = balanced_weights(fit), balanced_weights(resolved)
            base = float(np.average(fit.event,weights=fit_w))
            base_brier = float(np.average((base-resolved.event.astype(float))**2,weights=check_w))
            model_rows.append(dict(key=key,model="constant",check_brier=base_brier,fit_base_rate=base))
            options=[]
            for name in ("logistic","forest"):
                model=make_model(name)
                last=list(model.named_steps)[-1]
                model.fit(fit.loc[:,FEATURES],fit.event,**{f"{last}__sample_weight":fit_w})
                p = model.predict_proba(resolved.loc[:,FEATURES])[:,1]
                brier=float(np.average((p-resolved.event.astype(float))**2,weights=check_w))
                model_rows.append(dict(key=key,model=name,check_brier=brier,fit_base_rate=base))
                options.append((brier,name,model))
            brier,name,model=min(options,key=lambda x:(x[0],x[1]))
            info.update(model=name,check_brier=brier,constant_brier=base_brier,
                        status="NO_ELIGIBLE_THRESHOLD",beats_constant=brier<base_brier)
            fitted[key]=model
            scores=pd.Series(model.predict_proba(check.loc[:,FEATURES])[:,1],index=check.index)
            candidates=[]
            for threshold in THRESHOLDS:
                stat=score_policy(check,scores.ge(threshold),days[20:30])
                watch,high=eligibility(stat,brier<base_brier)
                row=dict(key=key,model=name,threshold=threshold,watch_eligible=watch,high_eligible=high,**stat)
                threshold_rows.append(row); candidates.append(row)
            watches=[r for r in candidates if r["watch_eligible"]]
            highs=[r for r in candidates if r["high_eligible"]]
            if watches:
                best=max(watches,key=lambda r:(r["grid_f05"],r["threshold"]))
                info["watch_threshold"]=best["threshold"]
                info["status"]="RESEARCH_WATCH_ONLY"
            if highs:
                best=max(highs,key=lambda r:(r["grid_recall"],r["threshold"]))
                info["high_threshold"]=best["threshold"]
                info["status"]="HISTORICAL_HIGH_GATE_PASSED_REQUIRES_PROSPECTIVE"
    return selected,fitted,pd.DataFrame(model_rows),pd.DataFrame(threshold_rows)


def apply_models(frame, selection, fitted):
    result=frame.copy()
    result["risk_score"]=np.nan
    result["research_watch"]=False
    result["high_confidence"]=False
    result["signal_state"]="INSUFFICIENT_EVIDENCE"
    result["signal_reason"]="No validated model or qualifying threshold"
    for key,info in selection.items():
        mask=result.model_stage.eq(info["stage"]) & result.horizon_minutes.eq(info["horizon_minutes"])
        if not mask.any() or key not in fitted:
            continue
        scores=fitted[key].predict_proba(result.loc[mask,FEATURES])[:,1]
        result.loc[mask,"risk_score"]=scores
        if info["watch_threshold"] is not None:
            result.loc[mask,"research_watch"]=scores>=info["watch_threshold"]
            result.loc[mask,"signal_state"]="NO_QUALIFYING_SIGNAL"
            result.loc[mask,"signal_reason"]="Below research-watch threshold; pullback remains possible"
        if info["high_threshold"] is not None:
            result.loc[mask,"high_confidence"]=scores>=info["high_threshold"]
    result.loc[result.research_watch,"signal_state"]="RESEARCH_WATCH"
    result.loc[result.research_watch,"signal_reason"]="Historical candidate only; no near-certainty claim"
    result.loc[result.high_confidence,"signal_state"]="HISTORICAL_HIGH_CONFIDENCE_CANDIDATE"
    result.loc[result.high_confidence,"signal_reason"]="Passed historical gate; prospective validation still required"
    gap=result.model_stage.eq("LATE_NO_ESTABLISHED_PROFIT")
    result.loc[gap,"signal_reason"]="Age >30m without >=0.30% prior favorable close: outside both model scopes"
    for policy,col in (("ORIGINAL","warning"),("RESEARCH_WATCH","research_watch"),("HIGH_CONFIDENCE","high_confidence")):
        result[f"alert_{policy}"]=alert_mask(result,result[col])
    return result


def enforce_combined_gates(frame,days,selection,fitted):
    """Reject only, on development dates, if combined cooldown invalidates a gate."""
    check=frame.loc[frame.day.isin(days[20:30])]
    audit=[]
    while True:
        predicted=apply_models(check,selection,fitted)
        rejected=[]
        for key,info in selection.items():
            part=predicted.loc[predicted.model_stage.eq(info["stage"]) &
                               predicted.horizon_minutes.eq(info["horizon_minutes"])]
            for field,policy,col,which in (("watch_threshold","RESEARCH_WATCH","research_watch",0),
                                            ("high_threshold","HIGH_CONFIDENCE","high_confidence",1)):
                if info[field] is None: continue
                stat=score_policy(part,part[col],days[20:30],part[f"alert_{policy}"])
                passed=eligibility(stat,info["beats_constant"])[which]
                audit.append(dict(key=key,policy=policy,threshold=info[field],passed=passed,**stat))
                if not passed: rejected.append((key,field))
        if not rejected: break
        for key,field in rejected:
            selection[key][field]=None
            selection[key]["combined_gate_rejection"]=True
            if selection[key]["high_threshold"] is None:
                selection[key]["status"]=("RESEARCH_WATCH_ONLY" if selection[key]["watch_threshold"] is not None
                                          else "NO_ELIGIBLE_THRESHOLD_AFTER_SHARED_COOLDOWN")
    return pd.DataFrame(audit)


def summarize_policies(frame,days):
    records=[]
    for split,calendar in (("FIT20",days[:20]),("CHECK10",days[20:30]),("LATER16",days[30:]),("ALL",days)):
        period=frame.loc[frame.day.isin(calendar)]
        for policy,col in (("ORIGINAL","warning"),("RESEARCH_WATCH","research_watch"),("HIGH_CONFIDENCE","high_confidence")):
            for horizon in (5,30):
                for side in ("BOTH","LONG","SHORT"):
                    for stage in ("ALL",*STAGES,"LATE_NO_ESTABLISHED_PROFIT"):
                        part=period.loc[period.horizon_minutes.eq(horizon)]
                        if side!="BOTH": part=part.loc[part.side.eq(side)]
                        if stage!="ALL": part=part.loc[part.model_stage.eq(stage)]
                        stat=score_policy(part,part[col],calendar,part[f"alert_{policy}"],bootstrap=stage=="ALL")
                        records.append(dict(split=split,policy=policy,side=side,horizon_minutes=horizon,model_stage=stage,**stat))
    return pd.DataFrame(records)


def coverage_tables(frame,trade_source,days):
    records=[]
    for tr in trade_source.itertuples(index=False):
        for horizon in (5,30):
            x=frame.loc[frame.trade_id.eq(tr.trade_id)&frame.horizon_minutes.eq(horizon)]
            grid=x.loc[x.is_grid & x.outcome_available]
            records.append(dict(day=tr.day,trade_id=tr.trade_id,tradingsymbol=tr.tradingsymbol,side=tr.side,
                horizon_minutes=horizon,split="FIT20" if tr.day in days[:20] else "CHECK10" if tr.day in days[20:30] else "LATER16",
                monitoring_rows=len(x),high_confidence_alerts=int(x.alert_HIGH_CONFIDENCE.sum()),
                research_watch_alerts=int(x.alert_RESEARCH_WATCH.sum()),
                no_high_confidence_signal=not bool(x.high_confidence.any()),
                no_qualifying_watch=not bool(x.research_watch.any()),
                insufficient_evidence_rows=int(x.signal_state.eq("INSUFFICIENT_EVIDENCE").sum()),
                grid_events=int(grid.event.sum()),missed_grid_events=int((grid.event & ~grid.research_watch).sum()),
                first_watch_ts=str(x.loc[x.alert_RESEARCH_WATCH,"decision_ts"].min().tz_convert("Asia/Kolkata")) if x.alert_RESEARCH_WATCH.any() else "",
                availability="MONITORED" if len(x) else "NO_POST_ENTRY_OBSERVATION",
                interpretation="No signal is not proof of no pullback"))
    trades=pd.DataFrame(records)
    daily=[]
    for day in days:
        for horizon in (5,30):
            t=trades.loc[trades.day.eq(day)&trades.horizon_minutes.eq(horizon)]
            daily.append(dict(day=day,horizon_minutes=horizon,trades=len(t),
                monitored_trades=int(t.monitoring_rows.gt(0).sum()),
                high_confidence_alerts=int(t.high_confidence_alerts.sum()),research_watch_alerts=int(t.research_watch_alerts.sum()),
                trades_without_watch=int(t.no_qualifying_watch.sum()),trades_without_high_confidence=int(t.no_high_confidence_signal.sum()),
                grid_events=int(t.grid_events.sum()),missed_grid_events=int(t.missed_grid_events.sum()),
                status="NO_TRADES" if t.empty else "RESEARCH_WATCH_PRESENT" if t.research_watch_alerts.any() else "NO_QUALIFYING_SIGNAL_OR_INSUFFICIENT_EVIDENCE"))
    return trades,pd.DataFrame(daily)


def risk_bands(frame):
    x=frame.loc[frame.day_split.eq("LATER16") & frame.is_grid & frame.outcome_available & frame.risk_score.notna()].copy()
    x["score_band"]=pd.cut(x.risk_score,[-.001,.1,.2,.4,.6,.8,1.],right=True).astype(str)
    rows=[]
    for (stage,horizon,band),part in x.groupby(["model_stage","horizon_minutes","score_band"]):
        rows.append(dict(model_stage=stage,horizon_minutes=horizon,score_band=band,rows=len(part),
                         trades=part.trade_id.nunique(),events=int(part.event.sum()),observed_event_rate=float(part.event.mean()),
                         mean_raw_score=float(part.risk_score.mean())))
    return pd.DataFrame(rows)


def run(source,out):
    out.mkdir(parents=True,exist_ok=False)
    write_json(out/"protocol.json",dict(PROTOCOL,features=list(FEATURES)))
    provenance=json.loads((source/"provenance.json").read_text(encoding="utf-8"))
    days=provenance["frozen_manifest"]["days"]
    if len(days)!=46 or sorted(set(days))!=days: raise ValueError("Expected ordered 46-session source")
    frozen=Path(provenance["frozen_path"])
    frozen_book=load_frozen(frozen)
    if frozen_book["days"]!=days: raise ValueError("Source calendar does not match frozen calendar")
    paths=[source/"predictions_1m.csv",source/"trade_report.csv",source/"provenance.json",
           frozen/"daily_results.csv",frozen/"frozen_config.json",frozen/"manifest.json"]
    paths=[p for p in paths if p.is_file()]
    before={str(p):sha(p) for p in paths}
    dense=pd.read_csv(source/"predictions_1m.csv")
    trade_source=pd.read_csv(source/"trade_report.csv")
    executed=frozen_book["trades"].loc[frozen_book["trades"].portfolio_executed]
    if len(executed)!=len(trade_source) or not np.isclose(
        executed.portfolio_net_profit_rupees.sum(),trade_source.net_pnl.sum(),atol=1e-6,rtol=0):
        raise ValueError("Monitor trade report does not match verified frozen ledger")
    frame=build_features(dense,trade_source)
    frame["day_split"]=np.select([frame.day.isin(days[:20]),frame.day.isin(days[20:30])],["FIT20","CHECK10"],default="LATER16")
    selected,fitted,checks,thresholds=select_models(frame,days)
    combined=enforce_combined_gates(frame,days,selected,fitted)
    # Persist the decision BEFORE applying models to the later16 feature rows.
    write_json(out/"locked_selection.json",selected)
    locked_hash=sha(out/"locked_selection.json")
    joblib.dump(dict(models=fitted,selection=selected,features=FEATURES,
                     sklearn_version=sklearn.__version__,observer_only=True),out/"research_models.joblib")
    snapshot=out/"source_snapshot"
    snapshot.mkdir()
    source_hashes={}
    for name in ("g3_pullback_selective.py","g3_pullback_selective_features.py","g3_pullback_selective_report.py",
                 "g3_pullback_tuning.py","g3_pullback_metrics.py","g3_freeze.py"):
        path=Path(__file__).with_name(name)
        shutil.copyfile(path,snapshot/name)
        source_hashes[name]=sha(path)
    checks.to_csv(out/"model_selection.csv",index=False)
    thresholds.to_csv(out/"threshold_selection.csv",index=False)
    combined.to_csv(out/"combined_gate_audit.csv",index=False)
    predicted=apply_models(frame,selected,fitted)
    predicted["decision_time_ist"]=predicted.decision_ts.dt.tz_convert("Asia/Kolkata")
    importance=[]
    for key,model in fitted.items():
        estimator=model.steps[-1][1]
        names=model.steps[0][1].get_feature_names_out(FEATURES)
        values=estimator.feature_importances_ if hasattr(estimator,"feature_importances_") else estimator.coef_[0]
        kind="impurity_importance" if hasattr(estimator,"feature_importances_") else "standardized_logistic_coefficient"
        importance.extend(dict(model=key,feature=f,value=float(v),kind=kind) for f,v in zip(names,values))
    pd.DataFrame(importance).to_csv(out/"model_feature_weights.csv",index=False)
    metrics=summarize_policies(predicted,days)
    trades,daywise=coverage_tables(predicted,trade_source,days)
    bands=risk_bands(predicted)
    predicted.to_csv(out/"predictions_1m.csv",index=False)
    metrics.to_csv(out/"metrics.csv",index=False)
    trades.to_csv(out/"trade_signal_coverage.csv",index=False)
    daywise.to_csv(out/"daywise_signal_coverage.csv",index=False)
    bands.to_csv(out/"historical_score_bands.csv",index=False)
    predicted.loc[predicted.alert_RESEARCH_WATCH | predicted.alert_HIGH_CONFIDENCE].to_csv(out/"selective_alerts.csv",index=False)
    after={str(p):sha(p) for p in paths}
    assert before==after,"Frozen/source artifacts changed"
    assert sha(out/"locked_selection.json")==locked_hash
    primary=metrics.loc[metrics.split.eq("LATER16")&metrics.side.eq("BOTH")&metrics.model_stage.eq("ALL")]
    summary=dict(version=PROTOCOL["version"],status="EXPLORATORY_ONLY_NO_LIVE_PROMOTION",sessions=len(days),
        trades=len(trade_source),monitored_trades=int(trade_source.monitor_available.sum()),
        frozen_net_pnl=float(trade_source.net_pnl.sum()),input_hashes_unchanged=True,input_hashes=before,
        selection_sha256=locked_hash,train_days=days[:20],check_days=days[20:30],evaluation_days=days[30:],
        source_sha256=source_hashes,dependency_versions=dict(python=sys.version,pandas=pd.__version__,sklearn=sklearn.__version__),
        high_confidence_models=sum(v["high_threshold"] is not None for v in selected.values()),
        research_watch_models=sum(v["watch_threshold"] is not None for v in selected.values()),
        primary_results=primary.to_dict("records"),
        validation={"frozen_manifest_verified":True,"frozen_inputs_unchanged":True,"selection_locked_before_later16_scoring":True,
                    "all_trades_have_two_coverage_rows":len(trades)==2*len(trade_source)},
        sources=["https://scikit-learn.org/stable/modules/calibration.html",
                 "https://www.itl.nist.gov/div898/handbook/prc/section2/prc241.htm"])
    write_json(out/"summary.json",summary)
    from research.g3_pullback_selective_report import write_report
    write_report(out,summary,metrics,selected,daywise,trades,bands)
    write_json(out/"artifact_hashes.json",{p.name:sha(p) for p in out.iterdir() if p.is_file()})
    print(json.dumps(clean_json({"out":str(out),"selection":selected,"primary_results":primary[["policy","horizon_minutes",
        "resolved_alerts","successful_alerts","alert_precision","grid_events","grid_tp","grid_recall"]].to_dict("records")}),indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source",type=Path,default=SOURCE)
    parser.add_argument("--out",type=Path,default=BASE/"pullback_selective_v2_20261008")
    args=parser.parse_args()
    run(args.source,args.out)
