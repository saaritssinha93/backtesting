"""Hindsight pullback-to-exit diagnostics on the immutable G-3 ledger.

Windows retain the original fixed-grid definitions. Recovery means reaching
the observation close again, at a completed close or a known exit fill.
Neither future excursions nor these classifications are predictive inputs.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys

import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research.g3_freeze import DEFAULT_FROZEN,load_frozen
from research.g3_daywise_backtest import SELECTIVE,local_time,sha,write_json

SIDEWAYS_RANGE_PCT=.30
SIDEWAYS_NET_PCT=.15
MIN_SIDEWAYS_MINUTES=15
OUTCOME_TEXT={
    "SL_BEFORE_CLOSE_RECOVERY":"Stop reached before a completed-close recovery to the window start price",
    "RECOVERED_THEN_LATER_SL":"Pullback recovered first; a stop was hit later in the trade",
    "TARGET_AFTER_PULLBACK":"Pullback did not reach the stop before recovery; trade later reached target",
    "SIDEWAYS_TIME_EXIT":"Time exit; closes stayed within the stated sideways band after the hit",
    "TIME_EXIT_NOT_SIDEWAYS":"Time exit; the post-hit close path does not meet the sideways test",
    "TIME_EXIT_TOO_SHORT":"Time exit; fewer than 15 minutes to assess sideways movement",
    "AMBIGUOUS_STOP_TARGET_ORDER":"Recorded stop-first exit; stop and target were touched in the same candle",
}


def sign_of(tr): return 1 if tr["side"]=="LONG" else -1
def target_price(tr): return float(tr["entry_price"])*(1+sign_of(tr)*float(tr["first_target_pct"])/100)
def activation(tr): return pd.Timestamp(tr["entry_ts"])+pd.Timedelta(minutes=120)
def stop_at_clock(tr,stamp):
    pct=1.0 if pd.Timestamp(stamp)>=activation(tr) else float(tr["initial_stop_pct"])
    return float(tr["entry_price"])*(1-sign_of(tr)*pct/100)
def stop_for_bar(tr,bar_end): return stop_at_clock(tr,pd.Timestamp(bar_end)-pd.Timedelta(minutes=1))


def build_path(tr,minute):
    """Known closes/fills and safely ordered extrema; never terminal intrabar wicks."""
    entry,exit_=pd.Timestamp(tr["entry_ts"]),pd.Timestamp(tr["exit_ts"])
    bar_end=pd.Timestamp(tr["exit_bar_end_ts"])
    mode=str(tr["exit_event"])
    m=minute.copy().sort_values("ts")
    m["ts"]=pd.to_datetime(m.ts,utc=True)
    rows=[]
    def append(stamp,price,kind,low=np.nan,high=np.nan,start=None,end=None,bar_stop=None):
        stamp=pd.Timestamp(stamp)
        rows.append(dict(ts=stamp,point_kind=kind,price=float(price),
            stop_price=stop_at_clock(tr,stamp),stop_during_bar=stop_at_clock(tr,stamp) if bar_stop is None else bar_stop,
            target_price=target_price(tr),entry_price=float(tr["entry_price"]),observed_low=low,observed_high=high,
            bar_start=stamp if start is None else start,bar_end=stamp if end is None else end))
    append(entry,tr["entry_price"],"ENTRY_FILL",bar_stop=stop_for_bar(tr,entry))
    allowed=m.ts.le(bar_end) if mode=="CLOSE" else m.ts.lt(bar_end)
    complete=m.loc[m.ts.ge(entry)&m.ts.le(exit_)&allowed]
    for r in complete.itertuples(index=False):
        if r.ts==entry and exit_<=entry: continue
        # The entry-bar close is after fill, but its extrema may precede fill.
        full=r.ts>entry
        append(r.ts,r.close,"COMPLETED_CLOSE",float(r.low) if full else np.nan,float(r.high) if full else np.nan,
               r.ts-pd.Timedelta(minutes=1),r.ts,stop_for_bar(tr,r.ts))
    if mode in ("INTRABAR","OPEN"):
        last=m.loc[m.ts.eq(bar_end)]
        if len(last)!=1: raise ValueError("Missing unique exit candle")
        opening=bar_end-pd.Timedelta(minutes=1)
        if entry<=opening<=exit_:
            append(opening,last.open.iloc[0],"EXIT_BAR_OPEN",bar_stop=stop_for_bar(tr,bar_end))
    append(exit_,tr["exit_price"],"EXIT_FILL",start=bar_end-pd.Timedelta(minutes=1) if mode=="INTRABAR" else exit_,
           end=bar_end if mode=="INTRABAR" else exit_,bar_stop=stop_for_bar(tr,bar_end))
    rows[-1]["stop_price"]=float(tr["entry_price"])*(1-sign_of(tr)*float(tr["active_stop_pct_at_exit"])/100)
    path=pd.DataFrame(rows).sort_values("ts",kind="stable").reset_index(drop=True)
    return path


def sideways_metrics(points,reference,sign):
    """Close/fill-only range; sufficient duration is required, not merely a time exit."""
    x=points.loc[points.point_kind.isin(["COMPLETED_CLOSE","EXIT_FILL"])].sort_values("ts",kind="stable")
    x=x.drop_duplicates("ts",keep="last")
    duration=float((x.ts.max()-x.ts.min()).total_seconds()/60) if len(x) else 0.
    span=100*float(x.price.max()-x.price.min())/reference if len(x) else np.nan
    move=100*sign*float(x.price.iloc[-1]-x.price.iloc[0])/reference if len(x) else np.nan
    enough=bool(duration>=MIN_SIDEWAYS_MINUTES and len(x)>=MIN_SIDEWAYS_MINUTES+1)
    return dict(sideways_close_range_pct=span,sideways_net_move_pct=move,sideways_observation_minutes=duration,
                sideways_test_passed=bool(enough and span<=SIDEWAYS_RANGE_PCT+1e-10 and abs(move)<=SIDEWAYS_NET_PCT+1e-10),
                sideways_sufficient=enough)


def _hit_bounds(tr,stamp,source):
    stamp=pd.Timestamp(stamp)
    if source=="COMPLETED_BAR": return stamp-pd.Timedelta(minutes=1),stamp
    if source=="ACTUAL_EXIT_FILL" and tr["exit_event"]=="INTRABAR":
        end=pd.Timestamp(tr["exit_bar_end_ts"])
        return end-pd.Timedelta(minutes=1),end
    return stamp,stamp


def analyze_window(tr,window,path):
    """Describe one previously resolved event window through recovery and frozen exit."""
    sign=sign_of(tr);entry=float(tr["entry_price"])
    anchor=float(window["decision_close"])
    decision=pd.Timestamp(window["decision_ts"]);hit=pd.Timestamp(window["event_ts"])
    exit_=pd.Timestamp(tr["exit_ts"]);source=str(window["event_source"])
    hit_from,hit_by=_hit_bounds(tr,hit,source)
    close_candidates=path.loc[path.point_kind.eq("COMPLETED_CLOSE") &
        (path.ts.ge(hit) if source=="COMPLETED_BAR" else path.ts.gt(hit)) & path.ts.gt(decision)]
    close_candidates=close_candidates.loc[(sign*(close_candidates.price-anchor)).ge(-1e-9)]
    fill=path.loc[path.point_kind.eq("EXIT_FILL")&path.ts.ge(hit)&(sign*(path.price-anchor)).ge(-1e-9)]
    recoveries=pd.concat([close_candidates,fill]).sort_values("ts",kind="stable")
    recovery=recoveries.iloc[0] if len(recoveries) else None
    recovery_ts=recovery.ts if recovery is not None else pd.NaT
    end=recovery_ts if recovery is not None else exit_
    # An exit-bar open at the decision timestamp occurs after the previous close.
    candidates=path.loc[(path.ts.gt(decision)|(path.point_kind.eq("EXIT_BAR_OPEN")&path.ts.ge(decision)))&path.ts.le(end)].copy()
    if recovery is not None:
        # Equal clock times can label the prior close and the next candle open.
        # The stable path ordering retains that close-before-open sequence.
        candidates=candidates.loc[candidates.index<=recovery.name]
    candidates["adverse_price"]=candidates.observed_low if sign==1 else candidates.observed_high
    candidates["adverse_price"]=candidates.adverse_price.fillna(candidates.price)
    candidates["depth"]=100*(-sign)*(candidates.adverse_price/anchor-1)
    candidates["headroom"]=100*sign*(candidates.adverse_price-candidates.stop_during_bar)/entry
    if candidates.empty: raise ValueError("Resolved event has no ordered future path")
    worst=candidates.loc[candidates.depth.idxmax()]
    min_headroom=float(candidates.headroom.min())
    if min_headroom < -1e-5 and tr["exit_event"]!="OPEN":
        raise ValueError("Safe pre-exit path crossed active stop before frozen exit")
    after_hit=path.loc[path.ts.ge(hit)]
    lateral=sideways_metrics(after_hit,anchor,sign)
    stop=str(tr["exit_reason"]) in ("STOP","TIGHTENED_STOP")
    ambiguous=bool(tr.get("same_bar_ambiguous",False))
    exit_index=path.index[path.point_kind.eq("EXIT_FILL")][0]
    recovered_before_exit=recovery is not None and recovery.name<exit_index
    if ambiguous and stop: code="AMBIGUOUS_STOP_TARGET_ORDER"
    elif stop: code="RECOVERED_THEN_LATER_SL" if recovered_before_exit else "SL_BEFORE_CLOSE_RECOVERY"
    elif tr["exit_reason"]=="TARGET": code="TARGET_AFTER_PULLBACK"
    elif lateral["sideways_test_passed"]: code="SIDEWAYS_TIME_EXIT"
    elif not lateral["sideways_sufficient"]: code="TIME_EXIT_TOO_SHORT"
    else: code="TIME_EXIT_NOT_SIDEWAYS"
    hit_closes=path.loc[path.ts.eq(hit)&path.point_kind.eq("COMPLETED_CLOSE")]
    stop_hit=stop_for_bar(tr,hit) if source=="COMPLETED_BAR" else stop_at_clock(tr,hit)
    if source=="ACTUAL_EXIT_FILL": stop_hit=float(path.loc[path.point_kind.eq("EXIT_FILL"),"stop_price"].iloc[0])
    depth_point_is_bar=pd.notna(worst.observed_low) and pd.notna(worst.observed_high)
    return dict(trade_id=window["trade_id"],day=window["day"],tradingsymbol=window["tradingsymbol"],side=tr["side"],
        horizon_minutes=int(window["horizon_minutes"]),threshold_pct=float(window["threshold_pct"]),
        split=window["day_split"],window_start_time_ist=local_time(decision),anchor_price=anchor,
        threshold_hit_from_ist=local_time(hit_from),threshold_hit_by_ist=local_time(hit_by),hit_source=source,
        hit_bar_close=float(hit_closes.price.iloc[0]) if len(hit_closes) else np.nan,
        sl_at_window_start=stop_at_clock(tr,decision),sl_at_hit=stop_hit,target_price=target_price(tr),
        headroom_at_start_pct_entry=100*sign*(anchor-stop_at_clock(tr,decision))/entry,
        worst_time_from_ist=local_time(worst.bar_start if depth_point_is_bar or worst.point_kind=="EXIT_FILL" else worst.ts),
        worst_time_by_ist=local_time(worst.bar_end if depth_point_is_bar or worst.point_kind=="EXIT_FILL" else worst.ts),
        worst_price=float(worst.adverse_price),worst_adverse_pct_anchor=max(0.,float(worst.depth)),
        min_stop_headroom_pct_entry=min_headroom,recovery_time_ist=local_time(recovery_ts),
        recovery_price=float(recovery.price) if recovery is not None else np.nan,
        recovery_basis=str(recovery.point_kind) if recovery is not None else "NO_CONFIRMED_CLOSE_RECOVERY",
        minutes_hit_to_recovery=float((recovery_ts-hit).total_seconds()/60) if recovery is not None else np.nan,
        minutes_hit_to_exit=float((exit_-hit).total_seconds()/60),exit_time_ist=local_time(exit_),
        exit_price=float(tr["exit_price"]),exit_reason=tr["exit_reason"],net_pnl=float(tr["portfolio_net_profit_rupees"]),
        outcome_code=code,outcome_text=OUTCOME_TEXT[code],**{k:v for k,v in lateral.items() if k!="sideways_sufficient"},
        partial_intrabar_path=True,same_bar_ambiguous=ambiguous,research_watch_at_anchor=bool(window["research_watch"]))


def summarize_path(tr,path,episodes):
    entry=float(tr["entry_price"]);sign=sign_of(tr)
    adverse=path.observed_low.fillna(path.price) if sign==1 else path.observed_high.fillna(path.price)
    favorable=path.observed_high.fillna(path.price) if sign==1 else path.observed_low.fillna(path.price)
    final=path.loc[path.ts.ge(pd.Timestamp(tr["exit_ts"])-pd.Timedelta(minutes=30))]
    lateral=sideways_metrics(final,entry,sign)
    if tr["exit_reason"] in ("STOP","TIGHTENED_STOP"): outcome="STOP_RECORDED"
    elif tr["exit_reason"]=="TARGET": outcome="TARGET_REACHED"
    elif lateral["sideways_test_passed"]: outcome="TIME_EXIT_SIDEWAYS_FINAL30"
    elif not lateral["sideways_sufficient"]: outcome="TIME_EXIT_TOO_SHORT_FOR_SIDEWAYS"
    else: outcome="TIME_EXIT_NOT_SIDEWAYS_FINAL30"
    count=lambda code: sum(e["outcome_code"]==code for e in episodes)
    recovered=[e["recovery_time_ist"] for e in episodes if e["recovery_time_ist"]]
    return dict(trade_id=f"{tr['day']}|{tr['setup_id']}|{tr['tradingsymbol']}|{tr['sid']}",day=str(tr["day"]),
        trade_exit_outcome=outcome,post_entry_path_points=int(path.point_kind.eq("COMPLETED_CLOSE").sum()),
        stop_price_initial=entry*(1-sign*float(tr["initial_stop_pct"])/100),stop_price_tightened=entry*(1-sign*.01),
        stop_activation_time_ist=local_time(activation(tr)),target_price=target_price(tr),
        observed_max_adverse_from_entry_pct=max(0.,float((-sign*(adverse/entry-1)*100).max())),
        observed_max_favorable_from_entry_pct=max(0.,float((sign*(favorable/entry-1)*100).max())),
        final30_close_range_pct=lateral["sideways_close_range_pct"],final30_net_move_pct=lateral["sideways_net_move_pct"],
        final30_observation_minutes=lateral["sideways_observation_minutes"],final30_sideways=lateral["sideways_test_passed"],
        actual_pullback_windows=len(episodes),unrecovered_sl_windows=count("SL_BEFORE_CLOSE_RECOVERY"),
        recovered_then_later_sl_windows=count("RECOVERED_THEN_LATER_SL"),target_after_pullback_windows=count("TARGET_AFTER_PULLBACK"),
        sideways_time_exit_windows=count("SIDEWAYS_TIME_EXIT"),
        first_pullback_hit_by_ist=min([e["threshold_hit_by_ist"] for e in episodes],default=""),
        first_recovery_time_ist=min(recovered,default=""),
        diagnostic_note="Known closes/fills and ordered extrema only; entry and terminal intrabar wicks excluded. No event window does not prove no intrabar pullback.")


def backup_report(out):
    backup=out/"before_followthrough"
    if backup.exists(): return backup
    backup.mkdir()
    for p in out.iterdir():
        if p.is_file(): shutil.copy2(p,backup/p.name)
    return backup


def run(out):
    out=Path(out);baseline=backup_report(out)
    frozen=load_frozen(DEFAULT_FROZEN)
    metadata=json.loads((SELECTIVE/"summary.json").read_text())
    dense=pd.read_csv(SELECTIVE/"predictions_1m.csv")
    for col in ("decision_ts","event_ts"): dense[col]=pd.to_datetime(dense[col],utc=True)
    windows=dense.loc[dense.is_grid&dense.outcome_available&dense.event]
    book=frozen["trades"].loc[frozen["trades"].portfolio_executed]
    context=frozen["minutes"].copy();context["ts"]=pd.to_datetime(context.ts,utc=True)
    contexts={key:group for key,group in context.groupby(["day","tradingsymbol"])}
    details=[];summaries=[];paths=[]
    for tr in book.to_dict("records"):
        trade_id=f"{tr['day']}|{tr['setup_id']}|{tr['tradingsymbol']}|{tr['sid']}"
        path=build_path(tr,contexts[(str(tr["day"]),tr["tradingsymbol"])])
        events=[analyze_window(tr,w,path) for w in windows.loc[windows.trade_id.eq(trade_id)].to_dict("records")]
        summaries.append(summarize_path(tr,path,events));details.extend(events)
        path["trade_id"]=trade_id;path["day"]=str(tr["day"])
        path["time_ist"]=path.ts.map(local_time);path["bar_start_time_ist"]=path.bar_start.map(local_time);path["bar_end_time_ist"]=path.bar_end.map(local_time)
        paths.append(path.drop(columns=["ts","bar_start","bar_end"]))
    events=pd.DataFrame(details).sort_values(["day","trade_id","window_start_time_ist","horizon_minutes"]).reset_index(drop=True)
    events.insert(0,"event_id",[f"PB{i+1:04d}" for i in range(len(events))])
    path_summary=pd.DataFrame(summaries);timeline=pd.concat(paths,ignore_index=True)
    trades=pd.read_csv(baseline/"trades_with_pullbacks.csv").merge(path_summary.drop(columns="day"),on="trade_id",validate="one_to_one")
    daily=pd.read_csv(baseline/"daily_results.csv")
    day_records=[]
    for day in daily.day:
        t=trades.loc[trades.day.eq(day)];has=t.actual_pullback_windows.gt(0)
        day_records.append(dict(day=day,target_exits=int(t.exit_reason.eq("TARGET").sum()),stop_exits=int(t.exit_reason.eq("STOP").sum()),
            tightened_stop_exits=int(t.exit_reason.eq("TIGHTENED_STOP").sum()),time_exits=int(t.exit_reason.eq("TIME_EXIT_1515").sum()),
            sideways_final30_time_exits=int(t.trade_exit_outcome.eq("TIME_EXIT_SIDEWAYS_FINAL30").sum()),
            trades_with_pullbacks_ending_sl=int((has&t.exit_reason.isin(["STOP","TIGHTENED_STOP"])).sum()),
            trades_with_pullbacks_ending_target=int((has&t.exit_reason.eq("TARGET")).sum()),
            trades_without_observed_pullback=int((~has).sum())))
    daily=daily.merge(pd.DataFrame(day_records),on="day",validate="one_to_one")
    for name,frame in (("pullback_followthrough.csv",events),("trade_path_summary.csv",path_summary),
        ("minute_trade_paths.csv",timeline),("trades_with_pullbacks.csv",trades),("daily_results.csv",daily)):
        frame.to_csv(out/name,index=False)
    summary=json.loads((baseline/"summary.json").read_text())
    summary["followthrough"]=dict(window_outcomes=events.outcome_code.value_counts().to_dict(),
        trade_exit_outcomes=path_summary.trade_exit_outcome.value_counts().to_dict(),
        sideways_rule="At least15min of completed closes/fill, range<=0.30% and abs directional net move<=0.15%; perwindow hit-to-exit, pertrade final30min",
        recovery_rule="First completed close at/beyond window-start price after threshold hit, or known exit fill; same hit-candle close can confirm recovery",
        excursion_rule="Worst known adverse price from observation close until first close recovery or exit; terminal intrabar wicks excluded",
        timing_rule="Threshold/worst times are one-minute bar bounds, not exact tick times; entry/exit fill timestamps use frozen bar labels",
        causal_claim=False)
    summary["report_artifacts"].update(followthrough="pullback_followthrough.csv",paths="minute_trade_paths.csv",
        path_summary="trade_path_summary.csv",followthrough_validation="followthrough_validation.json")
    write_json(out/"summary.json",summary)
    workbook=json.loads((baseline/"workbook_data.json").read_text())
    workbook.update(summary=summary,daywise=daily.to_dict("records"),trades=trades.to_dict("records"),
        pullback_followthrough=events.to_dict("records"),path_summaries=path_summary.to_dict("records"),trade_paths=timeline.to_dict("records"))
    write_json(out/"workbook_data.json",workbook)
    assert len(trades)==93 and len(daily)==46 and len(events)==len(windows)
    assert np.isclose(trades.net_pnl.sum(),metadata["frozen_net_pnl"],rtol=0,atol=1e-7)
    validation=dict(status="PASS",trades=len(trades),sessions=len(daily),resolved_event_windows=len(events),
        source_manifest_sha256=sha(DEFAULT_FROZEN/"manifest.json"),selective_predictions_sha256=sha(SELECTIVE/"predictions_1m.csv"),
        source_code_sha256=sha(Path(__file__)),frozen_pnl_reconciled=True,
        observed_stop_headroom_nonnegative=bool(events.min_stop_headroom_pct_entry.ge(-1e-5).all()),
        new_signals_recomputed=False,trade_actions_changed=False)
    write_json(out/"followthrough_validation.json",validation)
    from research.g3_daywise_report import write_report
    actual=pd.read_csv(out/"actual_pullback_windows.csv");alerts=pd.read_csv(out/"warning_alerts.csv")
    write_report(out,summary,daily,trades,actual,alerts)
    print(json.dumps(summary["followthrough"],indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--out",type=Path,required=True)
    run(parser.parse_args().out)
