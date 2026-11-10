"""Fixed relaxed-CVD replay, with choices locked before subsequent months."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import io
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from g3_v18_20261102 import run as common
from g3_v18_20261102.cvd_strict_20261102 import run as prior
from . import selection

PACKAGE=Path(__file__).resolve().parent
REFERENCE=prior.REFERENCE
SOURCE=prior.SOURCE
EXPECTED_FEATURE_SHA256=prior.EXPECTED_FEATURE_SHA256
IST=ZoneInfo("Asia/Kolkata")
COSTS=(5,9,15)
DECISION_COLUMNS=prior.DECISION_COLUMNS
checks=prior.checks
LOCKS=(("2026-08-31","2026-09"),("2026-09-30","2026-10"))


def fresh_output(path):
    path=Path(path).resolve()
    if not path.is_relative_to(PACKAGE) or path==PACKAGE:
        raise ValueError("Output must be a NEW child of the relaxed CVD directory")
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {path}")
    return path


def metrics(book,days):
    """Keep loss-day and loss-trade amounts distinct, including zero sessions."""
    result=common.metrics(book,days)
    cohort=book.loc[book.day.astype(str).str[:10].isin(days)] if len(book) else book
    executed=common.flags(cohort.portfolio_executed) if "portfolio_executed" in cohort else pd.Series(False,index=cohort.index)
    trades=cohort.loc[executed]
    pnl=pd.to_numeric(trades.get("portfolio_net_profit_rupees",pd.Series(dtype=float)),errors="raise")
    day_net=(trades.assign(_pnl=pnl).groupby(trades.day.astype(str).str[:10])._pnl.sum() if len(trades) else pd.Series(dtype=float)).reindex(days,fill_value=0.)
    active=set(trades.day.astype(str).str[:10]) if len(trades) else set()
    result.update(losing_days=int(day_net.lt(0).sum()),winning_days=int(day_net.gt(0).sum()),
        zero_pnl_days=int(day_net.eq(0).sum()),zero_trade_days=len(days)-len(active),
        loss_day_pnl_abs_rs=float(-day_net.clip(upper=0).sum()),loss_trade_pnl_abs_rs=float(-pnl.clip(upper=0).sum()))
    return result


def add_retention(frame,keys):
    out=frame.copy()
    reference=frame.set_index(["variant",*keys])
    base_ratios,matched_ratios,matched_names=[],[],[]
    for row in out.to_dict("records"):
        suffix=tuple(row[key] for key in keys)
        base=float(reference.loc[("base_control",*suffix),"trades"])
        matched=selection.MATCHED_CONTROLS.get(row["variant"],"base_control")
        matched_count=float(reference.loc[(matched,*suffix),"trades"])
        base_ratios.append(row["trades"]/base if base else np.nan)
        matched_ratios.append(row["trades"]/matched_count if matched_count else np.nan)
        matched_names.append(matched)
    out["matched_reference"]=matched_names
    out["trade_retention_vs_base"]=base_ratios
    out["trade_retention_vs_matched"]=matched_ratios
    out["trade_retention"]=matched_ratios
    return out


def verify_audit(original,changed,variant):
    mutable={"v9_selected","v9_filter_pass","v10_g_f_core","v9_decision","v9_rank_in_setup_day","strategy_version",
        "strategy_fingerprint","signal_id","source_signal_id","source_strategy_version","research_variant","execution_authority","scope"}
    a,b=(frame.set_index("sid").sort_index() for frame in (original,changed))
    if not a.index.equals(b.index):
        raise ValueError("Relaxed gate lost original audit candidates")
    for name in a:
        if name not in mutable:
            try:
                pd.testing.assert_series_equal(a[name],b[name],check_dtype=False,check_names=False,check_exact=True)
            except AssertionError as error:
                raise ValueError(f"Relaxed gate altered original input: {name}") from error
    allowed,_=selection.gate(b,variant)
    expected_filter=common.flags(a.v9_filter_pass)&allowed
    if not common.flags(b.v9_filter_pass).equals(expected_filter):
        raise ValueError("Eligibility differs from native filters plus the frozen gate")
    if (common.flags(b.v9_selected)&~expected_filter).any():
        raise ValueError("Ineligible candidate selected")
    clock=pd.to_datetime(a.signal_ts,utc=True).dt.tz_convert(IST).dt.strftime("%H:%M")
    early=clock.lt("09:45")
    if not common.flags(a.loc[early,"v9_selected"]).equals(common.flags(b.loc[early,"v9_selected"])):
        raise ValueError("Original valid native opening selection changed")
    for key,group in b.groupby(["day","setup_id"],sort=False):
        setup=selection.native.control._ACTIVE_BY_ID[key[1]]
        core=selection.native.control._CORE_BY_ID[key[1]]
        eligible=group.loc[common.flags(group.v9_filter_pass)]
        rows={i:row for i,row in eligible.iterrows()}
        ranked=sorted(rows,key=lambda i:selection.native.production._rank_key(rows[i],setup))
        session=date.fromisoformat(str(key[0])[:10])
        chosen_core=[i for i in ranked if selection.native.control.passes_selected_filters(rows[i],core,session_date=session)][:core.max_entries]
        chosen=chosen_core+[i for i in ranked if i not in chosen_core][:setup.max_entries-len(chosen_core)]
        actual=group.index[common.flags(group.v9_selected)].tolist()
        if set(actual)!=set(chosen):
            raise ValueError("Native ranking/core/extras selection was not preserved")
    return True


def compatible_audit(audit):
    """Adapt a copy for immutable reporting helpers; never patch their globals."""
    out=audit.copy()
    if "relaxed_gate_reason" in out:
        out["strict_gate_reason"]=out.relaxed_gate_reason
    return out


def decision_groups(original,audit,variant):
    return prior.decision_groups(original,compatible_audit(audit),variant)


def attribution(reference_book,book,variant,reference,audit):
    return prior.attribution(reference_book,book,variant,reference,compatible_audit(audit))


def calibration_lock(books,processed_days,changed_groups,cutoff,evaluation_month,before_loading_day,allocator,chooser):
    """Reject future outcomes before handing the training-only table to policy."""
    if before_loading_day<=cutoff or any(day>cutoff for day in processed_days):
        raise ValueError("Calibration lock received a future day or invalid boundary")
    train_days=sorted(set(processed_days))
    if len(train_days)!=len(processed_days):
        raise ValueError("Duplicate calibration calendar day")
    for book in books.values():
        if len(book) and not book.day.astype(str).str[:10].isin(train_days).all():
            raise ValueError("Calibration ledger contains future or unlisted outcomes")
    rows=[]
    if train_days:
        for variant,book in books.items():
            for cost in COSTS:
                stressed=book if cost==5 else checks.stressed_book(book,cost,allocator)
                rows.append(dict(variant=variant,cost_bps=cost,**metrics(stressed,train_days)))
    frame=pd.DataFrame(rows)
    changed={variant:any(item["variant"]==variant and item["day"] in train_days and item["selected_changed"] for item in changed_groups)
             for variant in selection.MATCHED_CONTROLS}
    choice=chooser(frame,changed)
    digest=hashlib.sha256(json.dumps(common.clean(rows),sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()
    lock=dict(cutoff=cutoff,evaluation_month=evaluation_month,before_loading_day=before_loading_day,
        latest_consumed_day=max(train_days) if train_days else None,training_days=train_days,training_dates=train_days,training_sessions=len(train_days),
        training_metrics_sha256=digest,training_metrics=rows,changed_selected_stock=changed,policy=choice,choice_full_proof=choice,
        selected_variant=choice["selected_variant"],reference_variant=choice["reference_variant"],
        locked_at_ist=datetime.now(IST).isoformat(),future_outcomes_used_for_policy=False,
        future_variant_replay_computed_before_lock=False,baseline_reference_loaded_for_parity=True,persisted_before_future_day=True,
        interpretation="Retrospective chronological selection on already-viewed history, not an unseen holdout")
    return lock,frame


def verify_daily_risk_metrics(overall,daily,periods):
    checked=0
    for row in overall.to_dict("records"):
        own=daily.loc[daily.variant.eq(row["variant"])&daily.cost_bps.eq(row["cost_bps"])&daily.day.isin(periods[row["period"]])]
        net=own.net_pnl_rs
        expected=dict(losing_days=int(net.lt(0).sum()),winning_days=int(net.gt(0).sum()),zero_pnl_days=int(net.eq(0).sum()),
            zero_trade_days=int(own.trades.eq(0).sum()),loss_day_pnl_abs_rs=float(-net.clip(upper=0).sum()),
            loss_trade_pnl_abs_rs=float(own.loss_trade_pnl_abs_rs.sum()),worst_day_rs=float(net.min()))
        for name,value in expected.items():
            if not np.isclose(row[name],value,rtol=0,atol=1e-6):
                raise ValueError(f"Daily loss metric reconciliation failed: {row['variant']}/{row['period']}/{row['cost_bps']}/{name}")
        checked+=1
    return dict(passed=True,period_cost_variant_rows=checked,loss_day_and_trade_amounts_distinct=True)


def run(args):
    from g3_v18_20261102 import data
    from g3_v18_20261102.cvd_advanced_20261102 import features
    from fno_v13_v10_g_3_daily_replay import simulate
    from research.g3_0920_backtest import allocate
    from .policy import choose_policy
    output=fresh_output(args.output)
    input_bytes,provenance_bytes,bundle_bytes=prior.verified_inputs(args.features,args.feature_provenance)
    original_summary=json.loads((REFERENCE/"summary.json").read_text(encoding="utf-8"))
    days=sorted(set(data.available_days(args.through)).intersection(original_summary["days"]))
    if args.days:
        if set(args.days)-set(days):
            raise ValueError("Requested day lacks a sealed source and pinned control")
        days=[day for day in days if day in args.days]
    if not days:
        raise ValueError("No sealed replay sessions")
    enriched=checks.validate_feature_calendar(pd.read_parquet(io.BytesIO(input_bytes)),days)
    reference_path=REFERENCE/"control_trades.csv"
    reference_bytes=reference_path.read_bytes()
    saved=pd.read_csv(io.BytesIO(reference_bytes),low_memory=False)
    methodology_path=PACKAGE/"methodology.json"
    methodology_bytes=methodology_path.read_bytes()
    consumed={str(Path(m.__file__).resolve()):common.sha(m.__file__) for name,m in list(sys.modules.items())
        if name.startswith(("fno_v13","research.","g3_v18_20261102","strategy_slot_selection","g3_one_minute","eqidv2_runtime"))
        and getattr(m,"__file__",None) and Path(m.__file__).suffix==".py"}
    consumed[str(Path(__file__).resolve())]=common.sha(__file__)
    config={**selection.configuration(),"requested_through":args.through,"planned_calendar":days,"costs_bps":list(COSTS),
        "source_type":"REUSED_IMMUTABLE_ONE_MINUTE_OHLCV_CVD_ESTIMATE","input_path":str(args.features.resolve()),
        "input_sha256":hashlib.sha256(input_bytes).hexdigest(),"input_bundle_manifest_sha256":hashlib.sha256(bundle_bytes).hexdigest(),
        "input_bundle_verified":True,"reference_control_sha256":hashlib.sha256(reference_bytes).hexdigest(),
        "source_code_sha256":consumed,"methodology_sha256":hashlib.sha256(methodology_bytes).hexdigest(),
        "methodology":json.loads(methodology_bytes),"feature_provenance_sha256":hashlib.sha256(provenance_bytes).hexdigest(),
        "chronological_locks":[dict(cutoff=cutoff,evaluation_month=month) for cutoff,month in LOCKS],
        "loss_day_definition":"Daily executed net P&L<0, retaining every requested zero-trade day",
        "loss_amount_definitions":"loss_day_pnl_abs_rs=-sum negative daily net; loss_trade_pnl_abs_rs=-sum negative executed trade net"}
    common.guard_writes(PACKAGE)
    output.mkdir(parents=True)
    common.dump(output/"config.json",config)
    common.dump(output/"status.json",dict(state="RUNNING",performance_complete=False))
    for name,content in (("input_features.parquet",input_bytes),("feature_provenance.json",provenance_bytes),
                         ("input_bundle_manifest.json",bundle_bytes),("methodology.json",methodology_bytes)):
        (output/name).write_bytes(content)
    fingerprint=common.sha(output/"config.json")
    ledgers,audits=({v:[] for v in selection.VARIANTS} for _ in range(2))
    proofs,parity_daily,groups,matched_groups,coverage,reasons=[],[],[],[],[],[]
    locked={}
    processed=[]
    by_day={str(day):frame for day,frame in enriched.groupby(enriched.bar_end.dt.strftime("%Y-%m-%d"),sort=False)}
    for number,day in enumerate(days):
        # This boundary precedes loading/simulating the first future sealed day.
        for cutoff,month in LOCKS:
            if day>cutoff and month not in locked:
                past_books={v:pd.concat(parts,ignore_index=True,sort=False) for v,parts in ledgers.items()} if processed else {}
                lock,training=calibration_lock(past_books,processed,matched_groups,cutoff,month,day,allocate,choose_policy)
                locked[month]=lock
                common.dump(output/"calibrationlock.json",locked)
                common.dump(output/"calibration_locks.json",locked)
                training.to_csv(output/f"calibration_metrics_{cutoff.replace('-','')}.csv",index=False)
                print(f"LOCK {month} through {cutoff}: {lock['selected_variant']} BEFORE loading {day}",flush=True)
        raw,minutes,proof=data.load_day(day)
        raw=raw.reset_index(drop=True)
        raw["sid"]=number*1_000_000+np.arange(len(raw),dtype=np.int64)
        raw=features.attach_features(raw,by_day[day])
        results=selection.select_all(raw)
        original=results["base_control"][1]
        proofs.append({**proof,"day":day})
        coverage.append(dict(day=day,source_rows=len(raw),source_symbols=raw.tradingsymbol.nunique(),
            exact_features=int(raw.adv_feature_ts.notna().sum()),cvd_ready=int(common.flags(raw.adv_ready).sum()),restricted_oct28=day=="2026-10-28"))
        counts=[]
        for variant,(orders,audit) in results.items():
            if variant!="base_control":
                verify_audit(original,audit,variant)
                for reason,part in audit.groupby("relaxed_gate_reason",sort=False):
                    reasons.append(dict(variant=variant,day=day,reason=reason,candidates=len(part),
                        native_eligible=int(common.flags(part.relaxed_native_filter_pass).sum()),originally_selected=int(common.flags(part.relaxed_original_selected).sum())))
            groups.extend(decision_groups(original,audit,variant))
            if variant in selection.MATCHED_CONTROLS:
                reference=selection.MATCHED_CONTROLS[variant]
                matched_groups.extend({**record,"reference_variant":reference} for record in decision_groups(results[reference][1],audit,variant))
            ledger,_=simulate(orders,data.execution_paths(orders,minutes),[date.fromisoformat(day)])
            ledger["variant"],ledger["strategy_version"],ledger["strategy_fingerprint"],ledger["execution_authority"]=variant,selection.VERSION,fingerprint,False
            ledgers[variant].append(ledger)
            audits[variant].append(audit)
            counts.append(f"{variant}={len(ledger)}")
        parity_daily.append({"day":day,**checks.baseline_parity(ledgers["base_control"][-1],saved,[day])})
        processed.append(day)
        print(f"{day} [{number+1}/{len(days)}] "+" ".join(counts),flush=True)
    books={v:pd.concat(parts,ignore_index=True,sort=False) for v,parts in ledgers.items()}
    all_audits={v:pd.concat(parts,ignore_index=True,sort=False) for v,parts in audits.items()}
    parity=checks.baseline_parity(books["base_control"],saved,days)
    periods={"all":days,"calibration_jul_aug":[d for d in days if d<="2026-08-31"],
        "calibration_jul_sep":[d for d in days if d<="2026-09-30"],"september":[d for d in days if d.startswith("2026-09")],
        "october":[d for d in days if d.startswith("2026-10")],
        "october_without_restricted":[d for d in days if d.startswith("2026-10") and d!="2026-10-28"],
        "post_slot_selection":[d for d in days if d>"2026-10-27"]}
    overall_rows,daily_rows,monthly_rows=[],[],[]
    for variant,book in books.items():
        book.to_csv(output/f"{variant}_trades.csv",index=False)
        all_audits[variant].to_csv(output/f"{variant}_candidate_audit.csv",index=False)
        for cost in COSTS:
            stressed=book if cost==5 else checks.stressed_book(book,cost,allocate)
            for period,cohort in periods.items():
                if cohort:
                    overall_rows.append(dict(variant=variant,cost_bps=cost,period=period,**metrics(stressed,cohort)))
            daily_rows.extend(dict(variant=variant,cost_bps=cost,day=day,**metrics(stressed,[day])) for day in days)
            monthly_rows.extend(dict(variant=variant,cost_bps=cost,month=month,**metrics(stressed,[d for d in days if d.startswith(month)])) for month in sorted({d[:7] for d in days}))
    overall=add_retention(pd.DataFrame(overall_rows),["cost_bps","period"])
    daily=add_retention(pd.DataFrame(daily_rows),["cost_bps","day"])
    monthly=add_retention(pd.DataFrame(monthly_rows),["cost_bps","month"])
    accounting=checks.verify_accounting(books,overall,daily,monthly,days)
    risk_checks=verify_daily_risk_metrics(overall,daily,periods)
    group_frame=pd.DataFrame(groups,columns=DECISION_COLUMNS)
    matched_frame=pd.DataFrame(matched_groups,columns=(*DECISION_COLUMNS,"reference_variant"))
    attribution_parts,attribution_checks=[],[]
    pairs=[(v,"base_control") for v in selection.VARIANTS[1:]]+list(selection.MATCHED_CONTROLS.items())
    for variant,reference in pairs:
        detail,proof=attribution(books[reference],books[variant],variant,reference,all_audits[variant])
        attribution_parts.append(detail)
        attribution_checks.append(proof)
    attributed=pd.concat(attribution_parts,ignore_index=True)
    analysis=prior.trade_change_analysis(attributed,pairs=pairs)
    for name,frame in (("overall_comparison",overall),("daily_results",daily),("monthly_results",monthly),
        ("coverage",pd.DataFrame(coverage)),("gate_reasons",pd.DataFrame(reasons,columns=["variant","day","reason","candidates","native_eligible","originally_selected"])),
        ("decision_groups",group_frame),("matched_decision_groups",matched_frame),("pnl_attribution",attributed),("trade_change_analysis",analysis)):
        frame.to_csv(output/f"{name}.csv",index=False)
    changes=[name for name,digest in consumed.items() if common.sha(name)!=digest]
    if changes or common.sha(reference_path)!=config["reference_control_sha256"] or common.sha(methodology_path)!=config["methodology_sha256"] or common.sha(args.features)!=EXPECTED_FEATURE_SHA256:
        raise ValueError(f"Code, methodology or immutable source changed during replay: {changes}")
    summary=dict(version=selection.VERSION,execution_authority=False,scope="BACKTEST_ONLY",created_at_ist=datetime.now(IST).isoformat(),
        requested_through=args.through,days=days,sessions=len(days),first_date=days[0],last_date=days[-1],requested_day_included=args.through in days,
        variants=list(selection.VARIANTS),source_type=config["source_type"],matched_controls=selection.MATCHED_CONTROLS,
        independent_holdout=False,all_sessions_previously_viewed=True,baseline_parity=parity,config_sha256=fingerprint,
        original_source_metrics_unchanged=True,native_ranking_and_quotas_preserved=True,native_opening_selections_preserved=True,
        existing_consumed_files_unchanged=True,calibration_locks=locked,overall_metrics=overall.to_dict("records"),
        conclusion="Fixed relaxed-rule comparison completed. Subsequent-month policy choices were locked using only preceding replay outcomes; history remains previously viewed.")
    common.dump(output/"source_provenance.json",dict(sessions=proofs,consumed_code=consumed,feature_sha256=EXPECTED_FEATURE_SHA256,
        input_bundle_manifest_sha256=config["input_bundle_manifest_sha256"],methodology_sha256=config["methodology_sha256"],reference_control_sha256=config["reference_control_sha256"]))
    common.dump(output/"baseline_parity.json",dict(overall=parity,daily=parity_daily))
    common.dump(output/"verification.json",dict(passed=True,checks=accounting,daily_risk_metrics=risk_checks,original_control_parity=parity,
        original_source_fields_unchanged=True,gate_before_native_ranking_verified=True,native_core_extras_quotas_preserved=True,
        valid_native_opening_preserved=True,exact_feature_source_bundle_verified=True,existing_consumed_files_unchanged=True,
        complete_pnl_attribution=attribution_checks,chronological_lock_months=sorted(locked)))
    common.dump(output/"summary.json",summary)
    common.dump(output/"status.json",dict(state="COMPLETE",performance_complete=True))
    common.dump(output/"manifest.json",dict(files={p.name:common.sha(p) for p in output.iterdir() if p.is_file()},execution_authority=False))
    print(json.dumps(dict(output=str(output),sessions=len(days),through=days[-1],baseline_parity=True,
        selected_by_month={month:value["selected_variant"] for month,value in locked.items()}),indent=2),flush=True)
    return summary


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--features",type=Path,default=SOURCE/"features.parquet")
    p.add_argument("--feature-provenance",type=Path,default=SOURCE/"configuration.json")
    p.add_argument("--through",default="2026-11-02",type=lambda s:date.fromisoformat(s).isoformat())
    p.add_argument("--days",nargs="+")
    p.add_argument("--output",type=Path,default=PACKAGE/("results_"+datetime.now(IST).strftime("%Y%m%d_%H%M%S")))
    return p


if __name__=="__main__":
    args=parser().parse_args()
    existed_before=args.output.exists()
    try:
        run(args)
    except Exception as error:
        if not existed_before and args.output.resolve().is_relative_to(PACKAGE) and (args.output/"status.json").is_file():
            common.dump(args.output/"status.json",dict(state="FAILED",performance_complete=False,error=str(error)))
        raise
