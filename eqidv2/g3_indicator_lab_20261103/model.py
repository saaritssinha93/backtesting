"""Small chronological AI ranking baselines on native, realizable labels.

Models rank the same prequota OR-eligible pool.  Their target is positive net
return conditional on an independent native fill, not expected portfolio P&L.
All learned transforms and labels come from earlier whole-day observations.
No model is calibrated on its evaluation observations.  ``predict_proba`` is
recorded as an unvalidated ranking score, with predictive calibration audited.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

PACKAGE = Path(__file__).resolve().parent
PROTOCOL = PACKAGE / "protocol.json"
MODELS = ("logistic_or_rank", "histgb_or_rank")
FEATURE_GROUPS = {
    "market": ("signed_nifty_return", "directional_breadth", "breadth_coverage"),
    "strength": ("signed_relative_strength", "signed_vwap_distance", "signed_vwap_slope", "efficiency"),
    "participation": ("oi_change_pct", "log_volume5", "log_volume1", "same_clock_rvol"),
    "entry": ("body_ratio", "adverse_wick", "extension_atr", "clock_hhmm", "side_sign"),
}
FEATURES = tuple(name for names in FEATURE_GROUPS.values() for name in names)


def _num(frame, key):
    return pd.to_numeric(frame.get(key, pd.Series(np.nan, index=frame.index)), errors="coerce")


def feature_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    """Exactly16 causal predictors; no symbol, identity, targets or outcomes."""
    sign = pd.Series(np.where(frame.side.eq("LONG"), 1., -1.), index=frame.index)
    invalid_side = ~frame.side.isin(("LONG", "SHORT"))
    sign.loc[invalid_side] = np.nan
    vol5, vol1 = _num(frame, "volume_ratio"), _num(frame, "v9_1m_volume_ratio")
    out = pd.DataFrame({
        "signed_nifty_return": sign*_num(frame,"v18_nifty_from_open_pct"),
        "directional_breadth": np.where(sign.gt(0), _num(frame,"v18_breadth_above_open_pct"),
                                         _num(frame,"v18_breadth_below_open_pct")),
        "breadth_coverage": _num(frame,"v18_breadth_coverage_pct"),
        "signed_relative_strength": sign*_num(frame,"v18_relative_strength_pct"),
        "signed_vwap_distance": sign*_num(frame,"v18_vwap_distance_pct"),
        "signed_vwap_slope": sign*_num(frame,"v18_vwap_slope_pct"),
        "efficiency": _num(frame,"v18_efficiency_6"),
        "oi_change_pct": _num(frame,"oi_change_pct"),
        "log_volume5": np.log1p(vol5.where(vol5.ge(0))),
        "log_volume1": np.log1p(vol1.where(vol1.ge(0))),
        "same_clock_rvol": _num(frame,"lab_tod_rvol"),
        "body_ratio": _num(frame,"body_ratio"),
        "adverse_wick": _num(frame,"wick_ratio"),
        "extension_atr": _num(frame,"lab_extension_atr"),
        "clock_hhmm": _num(frame,"hhmm_int"),
        "side_sign": sign,
    }, index=frame.index)
    return out.loc[:,FEATURES].replace([np.inf,-np.inf],np.nan)


def _pipeline(model, cfg):
    estimator = (LogisticRegression(C=cfg["logistic_C"],max_iter=1000,
                                   random_state=cfg["random_state"])
                 if model=="logistic_or_rank" else
                 HistGradientBoostingClassifier(max_iter=cfg["histgb_max_iter"],
                     max_leaf_nodes=cfg["histgb_max_leaf_nodes"],
                     min_samples_leaf=cfg["histgb_min_samples_leaf"],
                     l2_regularization=cfg["histgb_l2_regularization"],
                     early_stopping=False,random_state=cfg["random_state"]))
    return Pipeline([("impute",SimpleImputer(strategy="median",keep_empty_features=True)),
                     ("scale",StandardScaler()),("model",estimator)])


def _filled(frame):
    return frame.filled.astype(str).str.lower().isin(("true","1","1.0"))


def _metrics(y, scores, constant, **context):
    y=np.asarray(y,dtype=int)
    scores=np.clip(np.asarray(scores,dtype=float),1e-12,1-1e-12)
    constants=np.full(len(y),float(constant)) if np.ndim(constant)==0 else np.asarray(constant,dtype=float)
    constants=np.clip(constants,1e-12,1-1e-12)
    return dict(**context,filled_labels=len(y),positive_labels=int(y.sum()),
                observed_positive_rate=float(y.mean()) if len(y) else np.nan,
                mean_score=float(scores.mean()) if len(y) else np.nan,
                roc_auc=float(roc_auc_score(y,scores)) if len(np.unique(y))==2 else np.nan,
                log_loss=float(log_loss(y,scores,labels=[0,1])) if len(y) else np.nan,
                brier=float(brier_score_loss(y,scores)) if len(y) else np.nan,
                constant_log_loss=float(log_loss(y,constants,labels=[0,1])) if len(y) else np.nan,
                constant_brier=float(brier_score_loss(y,constants)) if len(y) else np.nan)


def _calibration(y, scores, **context):
    frame=pd.DataFrame({"label":np.asarray(y,dtype=int),"score":np.asarray(scores,dtype=float)})
    frame["bin"]=np.minimum((frame.score.clip(0,1)*10).astype(int),9)
    return [dict(**context,bin=int(index),lower=float(index/10),upper=float((index+1)/10),
                 count=len(part),mean_score=float(part.score.mean()),observed_positive_rate=float(part.label.mean()))
            for index,part in frame.groupby("bin",sort=True)]


def _clean(value):
    if value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value,dict):
        return {str(k):_clean(v) for k,v in value.items()}
    if isinstance(value,(list,tuple,np.ndarray)):
        return [_clean(v) for v in value]
    if isinstance(value,(np.integer,)):
        return int(value)
    if isinstance(value,(np.bool_,)):
        return bool(value)
    if isinstance(value,(float,np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value,(Path,pd.Timestamp)):
        return str(value)
    return value


def fit_predict_walkforward(pool: pd.DataFrame, days: list[str], output: Path, *, write_reports=True) -> pd.DataFrame:
    """Expanding prior-day training followed by frozen five-session blocks.

    Every input candidate receives an output row. Initial training sessions
    and insufficient-history blocks have ``model_ready=False`` and missing
    scores; the runner must use and label its predeclared control fallback.
    The optional ``write_reports=False`` supports in-memory causality checks.
    """
    protocol_bytes=PROTOCOL.read_bytes()
    cfg=json.loads(protocol_bytes)["model_validation"]
    chronological=[str(day)[:10] for day in days]
    if chronological!=sorted(set(chronological)):
        raise ValueError("days must be unique, chronologically sorted whole sessions")
    required={"sid","day","signal_ts","side","filled","net_return_pct","entry_ts","exit_ts"}
    if required-set(pool):
        raise ValueError(f"Missing candidate fields: {sorted(required-set(pool))}")
    work=pool.copy().reset_index(drop=True)
    work["day"]=work.day.astype(str).str[:10]
    if work.sid.isna().any() or work.sid.duplicated().any():
        raise ValueError("Model candidate sid must be globally unique")
    if not set(work.day).issubset(chronological):
        raise ValueError("Model pool includes a session outside the frozen calendar")
    signal=pd.to_datetime(work.signal_ts,utc=True,errors="coerce")
    entry=pd.to_datetime(work.entry_ts,utc=True,errors="coerce")
    exit_time=pd.to_datetime(work.exit_ts,utc=True,errors="coerce")
    if signal.isna().any() or not signal.dt.tz_convert("Asia/Kolkata").dt.strftime("%Y-%m-%d").eq(work.day).all():
        raise ValueError("Candidate signal timestamp is missing or differs from its session")
    filled=_filled(work)
    net=_num(work,"net_return_pct")
    valid_label=(filled & np.isfinite(net) & entry.notna() & exit_time.notna()
                 & entry.ge(signal) & exit_time.ge(entry))
    labels=net.gt(0).astype(int)
    x=feature_matrix(work)
    result=work[["sid","day"]].copy()
    for model in MODELS:
        result[model]=np.nan
    result["model_ready"]=False
    result["block_id"]=-1
    result["training_base_rate"]=np.nan
    folds,metrics,calibration,coefficients,importance,transforms=[],[],[],[],[],[]
    first,step=cfg["initial_train_days"],cfg["test_block_days"]
    for block_id,start in enumerate(range(first,len(chronological),step)):
        train_days=chronological[:start]
        test_days=chronological[start:start+step]
        test=work.day.isin(test_days)
        result.loc[test,"block_id"]=block_id
        first_test_signal=signal.loc[test].min()
        # All labels must have matured before the first decision in the block.
        before=work.day.isin(train_days)
        matured=(exit_time.lt(first_test_signal) if pd.notna(first_test_signal)
                 else exit_time.lt(pd.Timestamp(test_days[0],tz="Asia/Kolkata").tz_convert("UTC")))
        train=before & valid_label & matured
        y_train=labels.loc[train]
        counts=y_train.value_counts()
        ready=(len(y_train)>=cfg["minimum_filled_labels"]
               and counts.get(0,0)>=cfg["minimum_labels_each_class"]
               and counts.get(1,0)>=cfg["minimum_labels_each_class"] and test.any())
        base_rate=float(y_train.mean()) if len(y_train) else np.nan
        fold=dict(block_id=block_id,train_first_day=train_days[0],train_last_day=train_days[-1],
                  test_first_day=test_days[0],test_last_day=test_days[-1],train_sessions=len(train_days),
                  test_sessions=len(test_days),train_filled_labels=int(train.sum()),
                  train_positive_labels=int(counts.get(1,0)),train_negative_labels=int(counts.get(0,0)),
                  excluded_invalid_train_filled=int((before & filled & ~valid_label).sum()),
                  purged_unmatured_labels=int((before & valid_label & ~matured).sum()),
                  test_candidates=int(test.sum()),test_filled_labels=int((test & valid_label).sum()),
                  training_base_rate=base_rate,max_train_exit=exit_time.loc[train].max(),
                  first_test_signal=first_test_signal,model_ready=bool(ready),
                  status="READY" if ready else "NO_TEST_CANDIDATES" if not test.any() else "INSUFFICIENT_PRIOR_LABELS")
        folds.append(fold)
        if not ready:
            continue
        if not exit_time.loc[train].max()<first_test_signal:
            raise AssertionError("Training label extends into evaluation")
        result.loc[test,"model_ready"]=True
        result.loc[test,"training_base_rate"]=base_rate
        x_train,x_test=x.loc[train],x.loc[test]
        test_labeled=test & valid_label
        x_eval=x.loc[test_labeled]
        y_eval=labels.loc[test_labeled]
        eval_days=work.loc[test_labeled,"day"]
        for model_index,model in enumerate(MODELS):
            fitted=_pipeline(model,cfg).fit(x_train,y_train)
            scores=fitted.predict_proba(x_test)[:,1]
            result.loc[test,model]=scores
            eval_scores=result.loc[test_labeled,model].to_numpy()
            metric=_metrics(y_eval,eval_scores,base_rate,model=model,block_id=block_id,
                            test_first_day=test_days[0],test_last_day=test_days[-1])
            metrics.append(metric)
            calibration.extend(_calibration(y_eval,eval_scores,model=model,block_id=block_id))
            transforms.append(dict(model=model,block_id=block_id,features=list(FEATURES),
                imputer_medians=fitted.named_steps["impute"].statistics_.tolist(),
                scaler_means=fitted.named_steps["scale"].mean_.tolist(),
                scaler_scales=fitted.named_steps["scale"].scale_.tolist()))
            if model=="logistic_or_rank":
                estimator=fitted.named_steps["model"]
                coefficients.extend(dict(block_id=block_id,feature=feature,
                    group=next(group for group,names in FEATURE_GROUPS.items() if feature in names),
                    standardized_coefficient=float(weight),intercept=float(estimator.intercept_[0]))
                    for feature,weight in zip(FEATURES,estimator.coef_[0]))
            if len(y_eval):
                baseline_loss=metric["log_loss"]
                for group_index,(group,names) in enumerate(FEATURE_GROUPS.items()):
                    for repeat in range(5):
                        rng=np.random.default_rng(cfg["random_state"]+block_id*1000+model_index*100+group_index*10+repeat)
                        permuted=x_eval.copy()
                        for _,indices in eval_days.groupby(eval_days,sort=True).groups.items():
                            positions=np.asarray(indices)
                            permuted.loc[positions,list(names)]=x_eval.loc[rng.permutation(positions),list(names)].to_numpy()
                        changed=fitted.predict_proba(permuted)[:,1]
                        permuted_loss=float(log_loss(y_eval,changed,labels=[0,1]))
                        importance.append(dict(model=model,block_id=block_id,group=group,repeat=repeat,
                            filled_labels=len(y_eval),baseline_log_loss=baseline_loss,
                            permuted_log_loss=permuted_loss,log_loss_increase=permuted_loss-baseline_loss,
                            permutation="JOINT_GROUP_SHUFFLE_WITHIN_TEST_DAY"))
    assess=result.model_ready & valid_label
    for model in MODELS:
        overall=_metrics(labels.loc[assess],result.loc[assess,model],result.loc[assess,"training_base_rate"],
                          model=model,block_id="ALL_READY",test_first_day=work.loc[assess,"day"].min() if assess.any() else None,
                          test_last_day=work.loc[assess,"day"].max() if assess.any() else None)
        metrics.append(overall)
        calibration.extend(_calibration(labels.loc[assess],result.loc[assess,model],model=model,block_id="ALL_READY"))
    diagnostics=dict(scope="BACKTEST_ONLY",execution_authority=False,
        protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),model_validation=cfg,
        prediction_target="NATIVE_INDEPENDENT_FILL_NET_RETURN_AT_5BPS_GT_ZERO",
        score_interpretation="UNCALIBRATED_MODEL_OUTPUT_USED_FOR_RANKING; NOT_VALIDATED_WIN_PROBABILITY",
        training_pool="ONLY_FILLED_PRIOR_WHOLE_DAY_CANDIDATES_WITH_MATURED_VALID_OUTCOMES",
        evaluation_pool="ALL_OR_ELIGIBLE_PREQUOTA_CANDIDATES; PREDICTIVE_METRICS_CONDITIONAL_ON_VALID_FILL",
        initial_training_sessions=min(first,len(chronological)),days=chronological,
        candidate_count=len(work),filled_labels=int(valid_label.sum()),model_ready_candidates=int(result.model_ready.sum()),
        model_ready_filled_labels=int(assess.sum()),features=list(FEATURES),feature_groups=FEATURE_GROUPS,
        normalization="TRAINING_MEDIAN_IMPUTATION_THEN_TRAINING_STANDARD_SCALER; NO_TEST_FITTING",
        calibration="AUDITED_ONLY; NO_TEST_CALIBRATION_FIT",folds=folds,metrics=metrics,training_transforms=transforms,
        limitations=["Frozen slot policy reused historical outcomes through2026-10-27.",
            "Chronological model evaluation is not a virgin strategy holdout.",
            "Prediction target conditional on fill ignores average win/loss magnitude; final judgment requires portfolio replay.",
            "Within-day grouped permutation audits predictive log-loss; it is not a trading P&L attribution.",
            "Raw model outputs have not been calibrated for live use."])
    if write_reports:
        output=Path(output).resolve()
        if not output.is_relative_to(PACKAGE.resolve()):
            raise ValueError("Model reports must stay inside the isolated research directory")
        output.mkdir(parents=True,exist_ok=True)
        outputs={"model_folds.csv":folds,"model_metrics.csv":metrics,"model_calibration.csv":calibration,
                 "model_coefficients.csv":coefficients,"model_group_importance.csv":importance}
        for name in (*outputs,"model_diagnostics.json"):
            if (output/name).exists():
                raise FileExistsError(output/name)
        for name,rows in outputs.items():
            path=output/name
            pd.DataFrame(rows).to_csv(path,index=False)
        (output/"model_diagnostics.json").write_text(json.dumps(_clean(diagnostics),indent=2,allow_nan=False)+"\n",encoding="utf-8")
    result.attrs["diagnostics"]=_clean(diagnostics)
    return result
