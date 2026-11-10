"""Fixed relaxed CVD gates with explicit native-G3 opening warmup bypass."""
from __future__ import annotations

import hashlib
import json
import numpy as np
import pandas as pd
from g3_v18_20261102 import selection as native

VERSION = "G3-V18-RELAXED-CVD-RESEARCH-20261102"
RULES = {
    "relaxed_and": "P >= 0 AND A >= -0.05",
    "either_nonnegative": "P >= 0 OR A >= 0",
    "either_soft": "P >= -0.05 OR A >= -0.05",
    "opposition_veto": "P > -0.10 OR A > -0.05",
    "strong_opposition_veto": "P > -0.15 OR A > -0.10",
    "pressure_tolerance": "P >= -0.10",
}
VARIANTS = ("base_control", "available_control", *RULES)
MATCHED_CONTROLS = {name: "available_control" for name in RULES}


def configuration():
    return dict(version=VERSION,variants=list(VARIANTS),rules=RULES,
        matched_controls=MATCHED_CONTROLS,execution_authority=False,scope="BACKTEST_ONLY",
        direction="LONG=+1, SHORT=-1; P=direction*adv_recent15_pressure; A=direction*adv_acceleration",
        feature_definition="One-minute candle close-location-weighted volume; raw acceleration=current15m pressure minus previous nonoverlapping15m pressure, not divided by2",
        controls={"base_control":"Unchanged original G3", "available_control":"Native G3 before09:45; exact valid CVD features required from09:45; no pressure threshold"},
        warmup="Preserve valid native candidate signals09:20 through09:40 as explicit NATIVE_G3_WARMUP_BYPASS; native filters/ranking/quotas still apply. No CVD evidence is claimed for these trades.",
        missing_policy="At/after09:45, every relaxed OR/AND rule fails closed if exact feature, readiness, finite pressure/raw acceleration or direction is missing/invalid. Never fill unavailable CVD with zero.",
        unchanged="Original source fields, dated futures OI, native filters, ranking of survivors, core-first/extra quotas, confirmation, execution paths, exits and portfolio model",
        eligibility="Filter complete native strict candidate pool before native reranking; preserve rejected rows for audit",
        search="Six predeclared rules; fixed thresholds; no additions or retuning after outcomes",
        chronological_selection="September choice locked using July-August; October choice locked using July-September before those replay periods are loaded",
        independent_holdout=False,all_history_previously_viewed=True,
        reference_strategy_fingerprint=native.control.strategy_fingerprint())


def flags(series):
    return series.astype(str).str.lower().isin(("true","1","1.0"))


def gate(raw,variant):
    if variant not in VARIANTS:
        raise ValueError(f"Unknown relaxed CVD variant: {variant}")
    passed=pd.Series(True,index=raw.index,dtype=bool)
    reason=pd.Series("PASS",index=raw.index,dtype=object)
    if variant=="base_control":
        return passed,reason.mask(passed,"BASELINE_UNFILTERED")
    signal=pd.to_datetime(raw.get("signal_ts",pd.Series(pd.NaT,index=raw.index)),utc=True,errors="coerce")
    clock=signal.dt.tz_convert("Asia/Kolkata").dt.strftime("%H:%M")
    valid_clock=signal.notna() & clock.ge("09:20") & clock.le("15:30") & signal.dt.second.eq(0) & signal.dt.microsecond.eq(0) & signal.dt.nanosecond.eq(0) & signal.dt.minute.mod(5).eq(0)
    early=valid_clock & clock.lt("09:45")
    def require(condition,message):
        nonlocal passed
        condition=condition.fillna(False).astype(bool)
        reason.loc[passed & ~condition]=message
        passed &= condition
    require(valid_clock,"INVALID_SIGNAL_CLOCK")
    sign=raw.get("side",pd.Series("",index=raw.index)).map({"LONG":1.,"SHORT":-1.})
    require(sign.notna(),"INVALID_DIRECTION")
    feature=pd.to_datetime(raw.get("adv_feature_ts",pd.Series(pd.NaT,index=raw.index)),utc=True,errors="coerce")
    require(early | (feature.eq(signal) & feature.notna()),"MISSING_EXACT_FEATURE")
    require(early | flags(raw.get("adv_ready",pd.Series(False,index=raw.index))),"CVD_FEATURE_UNAVAILABLE")
    pressure=pd.to_numeric(raw.get("adv_recent15_pressure",pd.Series(np.nan,index=raw.index)),errors="coerce")
    acceleration=pd.to_numeric(raw.get("adv_acceleration",pd.Series(np.nan,index=raw.index)),errors="coerce")
    require(early | (np.isfinite(pressure) & pressure.between(-1,1) & np.isfinite(acceleration) & acceleration.between(-2,2)),"INVALID_PRESSURE_OR_ACCELERATION")
    p,a=sign*pressure,sign*acceleration
    conditions={
        "relaxed_and":p.ge(0) & a.ge(-.05),
        "either_nonnegative":p.ge(0) | a.ge(0),
        "either_soft":p.ge(-.05) | a.ge(-.05),
        "opposition_veto":p.gt(-.10) | a.gt(-.05),
        "strong_opposition_veto":p.gt(-.15) | a.gt(-.10),
        "pressure_tolerance":p.ge(-.10),
    }
    if variant in conditions:
        require(early | conditions[variant],"RELAXED_CVD_RULE_REJECTED")
    reason.loc[passed & early]="NATIVE_G3_WARMUP_BYPASS"
    reason.loc[passed & ~early]="LATE_CVD_AVAILABLE" if variant=="available_control" else "RELAXED_CVD_RULE_PASSED"
    return passed,reason


def _audit(strict,original,variant):
    if variant=="base_control":
        return original.copy()
    allowed,_=gate(strict,variant)
    accepted=native.control.selection_audit(strict.loc[allowed].copy())
    rejected=original.loc[~original.sid.isin(strict.loc[allowed,"sid"])].copy()
    rejected["v9_selected"]=False
    rejected["v9_filter_pass"]=False
    rejected["v10_g_f_core"]=False
    rejected["v9_rank_in_setup_day"]=np.nan
    rejected["v9_decision"]="RELAXED_CVD_GATE_REJECTED"
    parts=[part for part in (accepted,rejected) if len(part)]
    audit=pd.concat(parts,ignore_index=True,sort=False) if parts else accepted
    audit=audit.sort_values(["day","hhmm_int","side","setup_id","tradingsymbol"],kind="stable").reset_index(drop=True)
    prior=original.set_index("sid")
    for source,target in (("v9_selected","relaxed_original_selected"),("v9_filter_pass","relaxed_native_filter_pass"),("v9_rank_in_setup_day","relaxed_original_rank"),("v9_decision","relaxed_original_decision")):
        audit[target]=audit.sid.map(prior[source])
    audit["relaxed_gate_pass"],audit["relaxed_gate_reason"]=gate(audit,variant)
    audit["relaxed_selected_changed"]=flags(audit.v9_selected).ne(flags(audit.relaxed_original_selected))
    audit["research_variant"]=variant
    audit["execution_authority"]=False
    audit["scope"]="BACKTEST_ONLY"
    audit["source_strategy_version"]=native.control.STRATEGY_VERSION
    audit["source_signal_id"]=audit.sid.map(prior.signal_id)
    version=VERSION+"-"+variant.upper()
    audit["strategy_version"]=version
    audit["strategy_fingerprint"]=hashlib.sha256(json.dumps({**configuration(),"variant":variant},sort_keys=True).encode()).hexdigest()
    for i,row in audit.iterrows():
        setup=native.control._ACTIVE_BY_ID[row.setup_id]
        audit.loc[i,"signal_id"]=native.production.canonical_signal_id(version,row.day,setup.signal_end,setup.confirmation_end,setup.side,row.tradingsymbol)
    return audit


def select_all(raw):
    strict=native.strict_signals(raw)
    original=native.control.selection_audit(strict)
    result={}
    for variant in VARIANTS:
        audit=_audit(strict,original,variant)
        result[variant]=(audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True),audit)
    return result


def select(raw,variant="base_control"):
    if variant not in VARIANTS:
        raise ValueError(f"Unknown relaxed CVD variant: {variant}")
    return select_all(raw)[variant]
