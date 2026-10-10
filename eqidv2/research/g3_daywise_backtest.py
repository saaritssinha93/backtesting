"""Package verified frozen G-3 results with daywise observed pullbacks and warnings.

This exports the existing backtest, not a new signal search or an exit overlay.
Actual pullbacks use the audited fixed-grid outcomes; warning episodes retain
the original shared cooldown. Their denominators are deliberately separate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research.g3_freeze import DEFAULT_FROZEN, export_archived_replay

SELECTIVE=DEFAULT_FROZEN.parent/"pullback_selective_v2_20261008_final"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def clean(value):
    if isinstance(value,dict): return {str(k):clean(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)): return [clean(v) for v in value]
    if isinstance(value,(float,np.floating)): return float(value) if np.isfinite(value) else None
    if isinstance(value,np.integer): return int(value)
    if isinstance(value,np.bool_): return bool(value)
    return value


def write_json(path,value):
    Path(path).write_text(json.dumps(clean(value),indent=2,allow_nan=False),encoding="utf-8")


def local_time(value):
    if pd.isna(value): return ""
    stamp=pd.Timestamp(value)
    if stamp.tzinfo is None: raise ValueError("Expected an aware source timestamp")
    return stamp.tz_convert("Asia/Kolkata").strftime("%Y-%m-%d %H:%M:%S")


def phase(day,metadata):
    if day in metadata["train_days"]: return "FIT20"
    if day in metadata["check_days"]: return "CHECK10"
    if day in metadata["evaluation_days"]: return "LATER16"
    raise ValueError(f"Unexpected date {day}")


COUNT_FIELDS=("evaluated_windows","unknown_windows","pullback_windows","caught_windows","missed_windows",
              "watch_alerts","correct_alerts","false_alerts","unknown_alerts","high_confidence_alerts")


def horizon_stats(part):
    """Do not confuse grid event recall with dense cooldown alert precision."""
    grid=part.loc[part.is_grid]
    resolved=grid.loc[grid.outcome_available]
    event=resolved.loc[resolved.event]
    alerts=part.loc[part.alert_RESEARCH_WATCH]
    scored=alerts.loc[alerts.outcome_available]
    return dict(evaluated_windows=len(resolved),unknown_windows=len(grid)-len(resolved),
        pullback_windows=len(event),caught_windows=int(event.research_watch.sum()),
        missed_windows=int((~event.research_watch).sum()),watch_alerts=len(alerts),
        correct_alerts=int(scored.event.sum()),false_alerts=int((~scored.event).sum()),
        unknown_alerts=len(alerts)-len(scored),high_confidence_alerts=int(part.alert_HIGH_CONFIDENCE.sum()),
        first_pullback_time_ist=local_time(event.event_ts.dropna().min()) if len(event) else "",
        max_adverse_pct=float(resolved.max_future_adverse_pct.max()) if len(resolved) else np.nan)


def build_trade_results(executed,predicted,metadata):
    rows=[]
    for tr in executed.itertuples(index=False):
        trade_id=f"{tr.day}|{tr.setup_id}|{tr.tradingsymbol}|{tr.sid}"
        part=predicted.loc[predicted.trade_id.eq(trade_id)]
        monitors=part.decision_ts.nunique()
        watches=part.loc[part.alert_RESEARCH_WATCH]
        r=dict(trade_id=trade_id,day=str(tr.day),split=phase(str(tr.day),metadata),tradingsymbol=tr.tradingsymbol,
            side=tr.side,entry_time_ist=local_time(tr.entry_ts),exit_time_ist=local_time(tr.exit_ts),
            entry_price=float(tr.entry_price),exit_price=float(tr.exit_price),exit_reason=tr.exit_reason,
            initial_stop_pct=float(tr.initial_stop_pct),active_stop_pct_at_exit=float(tr.active_stop_pct_at_exit),
            first_target_pct=float(tr.first_target_pct),runner_target_pct=float(tr.runner_target_pct),
            gross_pnl=float(tr.portfolio_gross_profit_rupees),cost=float(tr.portfolio_cost_rupees),
            net_pnl=float(tr.portfolio_net_profit_rupees),monitoring_minutes=monitors,
            first_watch_time_ist=local_time(watches.decision_ts.min()) if len(watches) else "")
        for horizon,prefix in ((5,"fast"),(30,"slow")):
            r.update({f"{prefix}_{key}":value for key,value in horizon_stats(part.loc[part.horizon_minutes.eq(horizon)]).items()})
        r["signal_status"]=("MONITOR_UNAVAILABLE" if not monitors else "RESEARCH_WATCH" if len(watches) else "NO_QUALIFYING_SIGNAL")
        r["pullback_status"]=("MONITOR_UNAVAILABLE" if not monitors else
            "DEFINED_PULLBACK_OBSERVED" if r["fast_pullback_windows"]+r["slow_pullback_windows"] else
            "UNRESOLVED_WINDOWS" if r["fast_unknown_windows"]+r["slow_unknown_windows"] else "NO_DEFINED_PULLBACK_IN_EVALUATED_WINDOWS")
        r["insufficient_evidence_minutes"]=part.loc[part.signal_state.eq("INSUFFICIENT_EVIDENCE"),"decision_ts"].nunique()
        rows.append(r)
    return pd.DataFrame(rows)


def build_daily_results(frozen_daily,trades,metadata):
    rows=[]
    for base in frozen_daily.sort_values("day").itertuples(index=False):
        t=trades.loc[trades.day.eq(str(base.day))]
        r=dict(day=str(base.day),split=phase(str(base.day),metadata),trades=len(t),wins=int(t.net_pnl.gt(0).sum()),
            losses=int(t.net_pnl.lt(0).sum()),net_pnl=float(t.net_pnl.sum()),cost=float(t.cost.sum()),
            gross_pnl=float(t.gross_pnl.sum()),long_trades=int(t.side.eq("LONG").sum()),
            short_trades=int(t.side.eq("SHORT").sum()),monitored_trades=int(t.monitoring_minutes.gt(0).sum()),
            no_signal_trades=int(t.signal_status.eq("NO_QUALIFYING_SIGNAL").sum()),
            unavailable_trades=int(t.signal_status.eq("MONITOR_UNAVAILABLE").sum()))
        for prefix in ("fast","slow"):
            for field in COUNT_FIELDS:
                name=f"{prefix}_{field}"
                r[name]=int(t[name].sum())
        r["signal_status"]=("NO_TRADES" if not len(t) else "RESEARCH_WATCH_PRESENT" if r["fast_watch_alerts"]+r["slow_watch_alerts"]
                            else "MONITOR_UNAVAILABLE" if not r["monitored_trades"] else "NO_QUALIFYING_SIGNAL")
        if r["trades"]!=int(base.trades) or not np.isclose(r["net_pnl"],float(base.net_pnl),rtol=0,atol=1e-7):
            raise ValueError(f"Frozen daily reconciliation failed: {base.day}")
        if not np.isclose(r["cost"],float(base.cost),rtol=0,atol=1e-7): raise ValueError("Daily cost drift")
        rows.append(r)
    daily=pd.DataFrame(rows)
    daily["cumulative_net_pnl"]=daily.net_pnl.cumsum()
    return daily


def detail_rows(predicted):
    fields=["trade_id","day","tradingsymbol","side","horizon_minutes","threshold_pct","decision_close",
        "max_future_adverse_pct","lead_minutes","research_watch","signal_state","outcome_available","event",
        "early_exit","full_horizon_eligible","observed_exposure_minutes","risk_score","unknown_reason",
        "trade_age_minutes","model_stage"]
    detail=predicted.loc[:,fields].copy()
    detail["split"]=predicted.day_split
    detail["decision_time_ist"]=predicted.decision_ts.map(local_time)
    detail["event_time_ist"]=predicted.event_ts.map(local_time)
    end=predicted.decision_ts+pd.to_timedelta(predicted.observed_exposure_minutes,unit="m")
    detail["window_end_time_ist"]=end.map(local_time)
    detail["interpretation"]="New adverse move from window start close, bounded by frozen exit"
    grid=detail.loc[predicted.is_grid].copy()
    actual=grid.loc[grid.event & grid.outcome_available].copy()
    alerts=detail.loc[predicted.alert_RESEARCH_WATCH].copy()
    alerts["alert_outcome"]=np.select([~alerts.outcome_available,alerts.event],["UNKNOWN","SUCCESS"],default="FALSE_ALARM")
    return grid,actual,alerts


def run(out,selective=SELECTIVE,frozen=DEFAULT_FROZEN):
    out=Path(out)
    out.mkdir(parents=True,exist_ok=False)
    metadata=json.loads((selective/"summary.json").read_text(encoding="utf-8"))
    source_hashes=json.loads((selective/"artifact_hashes.json").read_text(encoding="utf-8"))
    for relative,expected in source_hashes.items():
        p=(selective/relative).resolve()
        if not p.is_relative_to(selective.resolve()) or sha(p)!=expected: raise ValueError(f"Selective artifact drift: {relative}")
    frozen_data=export_archived_replay(out/"frozen_replay",frozen)
    executed=frozen_data["trades"].loc[frozen_data["trades"].portfolio_executed].copy()
    frozen_daily=pd.read_csv(frozen/"daily_results.csv")
    predicted=pd.read_csv(selective/"predictions_1m.csv")
    for name in ("decision_ts","event_ts"):
        predicted[name]=pd.to_datetime(predicted[name],utc=True)
    expected_ids={f"{t.day}|{t.setup_id}|{t.tradingsymbol}|{t.sid}" for t in executed.itertuples(index=False)}
    if not set(predicted.trade_id)<=expected_ids: raise ValueError("Predictions contain a nonexecuted trade")
    trades=build_trade_results(executed,predicted,metadata)
    daily=build_daily_results(frozen_daily,trades,metadata)
    grid,actual,alerts=detail_rows(predicted)
    if len(trades)!=93 or len(daily)!=46: raise ValueError("Unexpected frozen book size")
    for horizon,prefix in ((5,"fast"),(30,"slow")):
        g=grid.loc[grid.horizon_minutes.eq(horizon)&grid.outcome_available]
        a=alerts.loc[alerts.horizon_minutes.eq(horizon)]
        assert int(daily[f"{prefix}_pullback_windows"].sum())==int(g.event.sum())
        assert int(daily[f"{prefix}_watch_alerts"].sum())==len(a)
        assert (daily[f"{prefix}_caught_windows"]+daily[f"{prefix}_missed_windows"]).equals(daily[f"{prefix}_pullback_windows"])
    for path,frame in (("daily_results.csv",daily),("trades_with_pullbacks.csv",trades),
                        ("pullback_evaluation_windows.csv",grid),("actual_pullback_windows.csv",actual),("warning_alerts.csv",alerts)):
        frame.to_csv(out/path,index=False)
    equity=daily.cumulative_net_pnl
    high=np.maximum.accumulate(np.r_[0.,equity.to_numpy()])[1:]
    summary=dict(strategy="V13-V10-G-3",variant="G3_W1_V1p1",period_start=min(daily.day),period_end=max(daily.day),
        sessions=len(daily),trades=len(trades),monitored_trades=int(trades.monitoring_minutes.gt(0).sum()),
        wins=int(trades.net_pnl.gt(0).sum()),losses=int(trades.net_pnl.lt(0).sum()),
        win_rate_pct=100*float(trades.net_pnl.gt(0).mean()),gross_pnl=float(trades.gross_pnl.sum()),
        cost=float(trades.cost.sum()),net_pnl=float(trades.net_pnl.sum()),max_day_end_drawdown=float((high-equity).max()),
        mode="VERIFIED_ARCHIVED_BACKTEST_WITH_OBSERVER_PULLBACKS",new_signals_recomputed=False,trade_actions_changed=False,
        monitor_model="Frozen selective V2, first20 fit / next10 selection / later16 reused historical evaluation",
        no_signal_trades=int(trades.signal_status.eq("NO_QUALIFYING_SIGNAL").sum()),
        high_confidence_alerts=int(predicted.alert_HIGH_CONFIDENCE.sum()),
        later16_watch_success=int(alerts.loc[alerts.split.eq("LATER16") & alerts.outcome_available,"event"].sum()),
        later16_watch_alerts=int(alerts.split.eq("LATER16").sum()),
        limitations=["October 1 excluded: incomplete source session. Data ends October 7.",
            "Observed pullbacks are fixed-grid adverse-move windows after the first completed post-entry observation, not unique market swings.",
            "5-minute threshold 0.30%; 30-minute threshold 0.50%; horizons stop at frozen exit. Do not add both horizon event counts together.",
            "Actual pullbacks are hindsight labels; watch features use information available at the decision close.",
            "Training and selection-date signals are fitted examples, not forecasts available live on those dates. Only later16 was scored after model and threshold selection.",
            "No qualifying signal is not an all-clear. Unmonitorable and unresolved observations remain explicit.",
            "Model-fit and selection dates are not validation; the later16 dates are reused exploratory history.",
            "Warnings do not change entries, stops, targets, costs or P&L. No new full-universe backtest was run."],
        source_frozen=str(frozen),source_selective=str(selective),
        report_artifacts=dict(workbook="g3_daywise_backtest.xlsx",daily="daily_results.csv",trades="trades_with_pullbacks.csv",
            windows="actual_pullback_windows.csv",alerts="warning_alerts.csv",summary="summary.json",validation="validation.json"))
    write_json(out/"summary.json",summary)
    write_json(out/"workbook_data.json",dict(summary=summary,daywise=daily.to_dict("records"),trades=trades.to_dict("records"),
        pullback_windows=actual.to_dict("records"),warnings=alerts.to_dict("records")))
    validation=dict(status="PASS",source_artifacts_hash_verified=len(source_hashes),
        frozen_manifest_verified=True,daily_pnl_and_cost_reconciled=True,trade_count_reconciled=True,
        pullback_windows_and_warning_episodes_reconciled=True,source_files_changed=False,
        input_hashes={str(selective/"predictions_1m.csv"):sha(selective/"predictions_1m.csv"),str(frozen/"manifest.json"):sha(frozen/"manifest.json")},
        source_code_sha256=sha(Path(__file__)))
    write_json(out/"validation.json",validation)
    from research.g3_daywise_report import write_report
    write_report(out,summary,daily,trades,actual,alerts)
    write_json(out/"artifact_hashes.json",{str(p.relative_to(out)).replace("\\","/"):sha(p) for p in out.rglob("*") if p.is_file()})
    print(json.dumps(clean(summary),indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--selective",type=Path,default=SELECTIVE)
    parser.add_argument("--frozen",type=Path,default=DEFAULT_FROZEN)
    args=parser.parse_args()
    run(args.out,args.selective,args.frozen)
