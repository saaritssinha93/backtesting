"""Audited additional-day G8 baseline versus the previously defined shock rule."""
from __future__ import annotations

import argparse
import contextlib
from datetime import date, datetime, timezone
import io
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g8_regime_exit_research as study
from research import g7_daily_backtest as daily
from research.g8_vix_data import VixData, load_bars, IST
import kite_vix_fetch_5m_15m as fetcher

core=study.core
SHOCK="shock_s0.875_t0.5"


def get_vix(day,out):
    with contextlib.redirect_stdout(io.StringIO()):
        kite=fetcher.setup_kite_session()
    start,end=fetcher.build_range(str(day),str(day),None)
    frames={}
    for interval,minutes in (("5minute",5),("15minute",15)):
        rows=kite.historical_data(instrument_token=264969,from_date=start,to_date=end,interval=interval,oi=False)
        f=pd.DataFrame(rows)
        if f.empty: raise ValueError("No VIX data for "+str(day))
        f["date"]=pd.to_datetime(f.date,utc=True).dt.tz_convert(IST)
        f=f[["date","open","high","low","close","volume"]].sort_values("date")
        path=out/("vix_"+interval+".csv")
        f.to_csv(path,index=False)
        validated=load_bars(path,minutes)
        expected=pd.date_range(f"{day} 09:15",f"{day} 15:{'25' if minutes==5 else '15'}",freq=f"{minutes}min",tz=IST)
        if not pd.DatetimeIndex(validated.date).equals(expected):
            raise ValueError("Incomplete/unexpected VIX session: "+interval)
        frames[interval]=validated
    aggregate=frames["5minute"].set_index("date").resample("15min",origin="start_day").agg(
        dict(open="first",high="max",low="min",close="last"))
    independent=frames["15minute"].set_index("date")[["open","high","low","close"]]
    if not aggregate.index.equals(independent.index) or not np.allclose(aggregate,independent,atol=1e-6,rtol=0):
        raise ValueError("VIX independent 15-minute aggregation differs")
    core.dump(out/"vix_provenance.json",dict(source="Kite historical_data, read-only",instrument_token=264969,
        session_date=str(day),retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
        timestamp_semantics="candle start; close available at start plus interval",bars_5m=75,bars_15m=25,
        independently_cross_checked=True,execution_authority=False,
        files={p.name:core.sha(p) for p in out.glob("vix_*minute.csv")}))


def verify(out):
    out=Path(out).resolve()
    m=json.loads((out/"manifest.json").read_text())
    for name,digest in m["files"].items():
        p=(out/name).resolve()
        if not p.is_relative_to(out) or core.sha(p)!=digest: raise ValueError("Changed artifact: "+name)
    for name,digest in m["code"].items():
        if core.sha(core.ROOT/name)!=digest: raise ValueError("Changed code: "+name)
    summary=json.loads((out/"summary.json").read_text())
    f=pd.read_csv(out/"features.csv").set_index("sid",drop=False)
    if not (pd.to_datetime(f.vix_available_at,utc=True)<=pd.to_datetime(f.confirmation_ts,utc=True)).all():
        raise ValueError("Future VIX observation")
    if not (f.vix_level_last_reference_day<f.day).all():raise ValueError("Future VIX level reference")
    comparison=pd.read_csv(out/"overall_comparison.csv")
    timeline=pd.read_csv(out/"daywise_comparison.csv")
    if timeline.day.duplicated().any() or timeline.day.max()!=summary["through"]:
        raise ValueError("Duplicate or wrong cumulative day")
    for label in ("baseline","shock"):
        book=pd.read_csv(out/label/"combined_portfolio_trades.csv")
        for period,days in summary["periods"].items():
            actual=study.metrics(book,days)
            row=comparison.loc[comparison.label.eq(label)&comparison.period.eq(period)].iloc[0]
            for key in ("trades","wins","losses","net_pnl_rs","gross_pnl_rs","cost_rs","losing_days"):
                if not np.isclose(actual[key],row[key],atol=1e-6,rtol=0):raise ValueError("Accounting: "+key)
        for row in timeline.itertuples():
            expected=getattr(row,label+"_net_pnl_rs")
            if not np.isclose(study.metrics(book,[row.day])["net_pnl_rs"],expected,atol=1e-6,rtol=0):
                raise ValueError("Daywise mismatch: "+row.day)
    return True


def run(day,output=None):
    # Inputs through22 remain immutable. Additional days get separate packages.
    if day.isoformat()<=core.CUTOFF:raise ValueError("Use the pinned historical report for earlier days")
    parent=Path(json.loads((core.OUTPUT_ROOT/"latest_exit_regime_research.json").read_text())["run_dir"])
    study.verify(parent)
    prior=json.loads((parent/"summary.json").read_text())
    config=core.load_sources()["config"]
    if config!=daily.g7.configuration(daily.g7.V2_CONFIG):raise ValueError("G7 V2 configuration changed")
    directory,payload,provenance=daily.load_source(day)
    source_digest=core.sha(directory/"run_manifest.json")
    pointer_hashes={str(p):core.sha(p) for p in [core.OUTPUT_ROOT/"latest_baseline.json",
        daily.g7.G7_ROOT/"latest/daily_backtest.json",daily.g7.G7_ROOT/"latest/bounded_exit.json"] if p.is_file()}
    out=Path(output or core.OUTPUT_ROOT/"daily_comparison"/str(day)/
        (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_g8_baseline_vs_shock"))
    out.mkdir(parents=True,exist_ok=False)
    core.dump(out/"plan.json",dict(day=str(day),baseline_policy=study.adopted.POLICY,shock_policy=SHOCK,
        prior_run=str(parent),no_parameter_search=True,execution_authority=False))
    evidence={k:provenance[k] for k in ("snapshot_manifest","snapshot_manifest_sha256")}
    prices=daily.g7.Prices(dict(daily_path_evidence=[evidence]))
    orders,pool,changes,slot_audit=daily.select_orders(day,directory,payload,prices,config)
    if orders.empty:raise ValueError("No selected orders; explicit empty-session handling needed")
    orders["source_sid"]=orders.sid
    orders["sid"]=int(day.strftime("%Y%m%d"))*10000+np.arange(len(orders))
    paths=prices.paths(orders.sort_values(["tradingsymbol","confirmation_ts"]))
    original=daily.bounded_replay(orders,paths,day,config)
    audit,proof,nifty_inputs=daily.nifty.order_audit(original,prices)
    print("Verified closed-session inputs; selected orders:",len(orders),flush=True)
    get_vix(day,out)
    # Prior warmup and through22 feature inputs were already independently audited.
    study.prior.verify(study.FEATURE_PARENT)
    historic_vix=pd.read_csv(study.FEATURE_PARENT/"extended_vix.csv")
    current_vix=pd.read_csv(out/"vix_5minute.csv")
    extended=pd.concat([historic_vix,current_vix],ignore_index=True).sort_values("date")
    if pd.to_datetime(extended.date,utc=True).duplicated().any():raise ValueError("Duplicate VIX bar")
    extended.to_csv(out/"extended_vix.csv",index=False)
    source=dict(config=config,unfiltered=original,orders=orders,audit=audit,paths=paths)
    f=core.build_features(source,VixData(out/"extended_vix.csv"))
    f["exit_regime"]=study.classify(f)
    allowed=core.allowed_for(f,dict(mode="persistent_direction",window=30,threshold=2.))
    f["g8_allowed"]=allowed
    books={}
    for label,policy in (("baseline",study.BASE),("shock",SHOCK)):
        spec=next(s for s in study.specifications() if s["id"]==policy)
        settings=study.settings(f,spec)
        raw=study.replay(source,settings)
        books[label]=study.allocate_variant(raw,allowed,config,policy)
        (out/label).mkdir()
        settings.to_csv(out/label/"entry_exit_settings.csv",index_label="sid")
        books[label].to_csv(out/label/"portfolio_trades.csv",index=False)
    # Reconcile the default exit engine with native G7 V2 single exits.
    native=study.allocate_variant(original,allowed,config,study.BASE)
    if not np.array_equal(native.portfolio_executed,books["baseline"].portfolio_executed) or not np.allclose(
        native.portfolio_net_profit_rupees,books["baseline"].portfolio_net_profit_rupees,atol=1e-6,rtol=0):
        raise ValueError("G8 native baseline parity failure")
    frames=dict(selected_orders=orders,guard_candidate_audit=pool,guard_selection_changes=changes,
        slot_entry_filter_audit=slot_audit,nifty_direction_audit=audit,nifty_source_proof=proof,features=f)
    for name,frame in frames.items():frame.to_csv(out/(name+".csv"),index=False)
    pd.concat([pd.DataFrame(p).assign(sid=sid) for sid,p in paths.items()],ignore_index=True).to_parquet(out/"minute_paths.parquet",index=False)
    (out/"nifty_inputs").mkdir()
    for d,frame in nifty_inputs.items():frame.to_parquet(out/"nifty_inputs"/(d+".parquet"),index=False)
    core.dump(out/"price_sources.json",prices.proof)
    core.dump(out/"source_manifest.json",provenance)
    core.dump(out/"config.json",config)
    core.dump(out/"source_coverage.json",payload.get("coverage",{}))
    # Each report includes the fixed history plus this requested day. Never
    # silently count a missing intervening session as a zero-profit day.
    days=sorted(prior["days"]+[str(day)])
    periods=dict(today=[str(day)],october=[d for d in days if d.startswith("2026-10-")],all=days)
    summary_rows,daily_rows,trade_rows=[],[],[]
    oldlabels=dict(baseline="baseline",shock="exploratory_risk_candidate")
    for label,today in books.items():
        history=pd.read_csv(parent/oldlabels[label]/"portfolio_trades.csv")
        combined=pd.concat([history,today],ignore_index=True,sort=False)
        if combined.sid.duplicated().any():raise ValueError("SID collision across dates")
        combined.to_csv(out/label/"combined_portfolio_trades.csv",index=False)
        for period,calendar in periods.items():summary_rows.append(dict(label=label,period=period,**study.metrics(combined,calendar)))
        for d in days:daily_rows.append(dict(label=label,day=d,**study.metrics(combined,[d])))
        for row in today.itertuples():
            trade_rows.append(dict(label=label,symbol=row.tradingsymbol,setup_id=row.setup_id,side=row.side,
                sid=row.sid,regime=f.loc[row.sid,"exit_regime"],g8_allowed=bool(allowed.loc[row.sid]),
                executed=bool(row.portfolio_executed),entry_ts=row.entry_ts,exit_ts=row.exit_ts,
                exit_reason=row.exit_reason,sl_pct=row.stop_pct,target_pct=row.target_pct,
                net_pnl_rs=row.portfolio_net_profit_rupees))
    comparison=pd.DataFrame(summary_rows)
    daily_results=pd.DataFrame(daily_rows)
    pivot=daily_results.pivot(index="day",columns="label",values=["net_pnl_rs","trades","wins","losses"])
    pivot.columns=[label+"_"+field for field,label in pivot.columns]
    pivot["shock_change_rs"]=pivot.shock_net_pnl_rs-pivot.baseline_net_pnl_rs
    comparison.to_csv(out/"overall_comparison.csv",index=False)
    daily_results.to_csv(out/"daywise_long.csv",index=False)
    pivot.to_csv(out/"daywise_comparison.csv")
    pivot.loc[periods["october"]].to_csv(out/"october_daywise_comparison.csv")
    pd.DataFrame(trade_rows).to_csv(out/"today_trade_comparison.csv",index=False)
    summary=dict(through=str(day),complete=True,periods=periods,scope="BACKTEST_ONLY",execution_authority=False,
        source_run=str(directory),source_manifest_sha256=source_digest,coverage=payload.get("coverage",{}),
        prior_run=str(parent),prior_manifest_sha256=core.sha(parent/"manifest.json"),
        feature_parent_manifest_sha256=core.sha(study.FEATURE_PARENT/"manifest.json"),
        retained_history_days=prior["days"],new_days=[str(day)],baseline_policy=study.adopted.POLICY,
        shock_policy=SHOCK,baseline_promotion=False,limitations=["Same after-close stock eligibility exclusions as the source G3 package.",
        "Original historical results remain retrospective. No new parameters were fitted to this day.",
        "5bps modeled round-trip costs, fractional quantities, conservative minute-bar execution."])
    core.dump(out/"summary.json",summary)
    report="<!doctype html><meta charset='utf-8'><title>G8 daily comparison</title><style>body{font:15px system-ui;margin:30px}td,th{padding:8px;border:1px solid #ccc}table{border-collapse:collapse}</style>"
    report+=f"<h1>G8 baseline versus selective shock rule through {day}</h1><p>Completed-session replay. Existing parameters, 5bps costs. Shock rule: initial SL1.05%, half original target only when the predefined shock/rebound condition is present.</p>"
    report+=comparison[["label","period","sessions","trades","wins","losses","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]].to_html(index=False,float_format=lambda x:f"{x:,.2f}")
    report+="<h2>October daywise</h2>"+pivot.loc[periods["october"]].to_html(float_format=lambda x:f"{x:,.2f}")
    report+="<h2>Today's orders</h2>"+pd.DataFrame(trade_rows).to_html(index=False,float_format=lambda x:f"{x:,.2f}")
    report+="<p>Source coverage exclusions are retained in source_coverage.json. Earlier history has missing sessions, including1October. G8 baseline configuration is unchanged.</p><p><a href='october_daywise_comparison.csv'>October CSV</a> | <a href='daywise_comparison.csv'>All dates CSV</a> | <a href='today_trade_comparison.csv'>Today trade CSV</a></p>"
    (out/"report.html").write_text(report,encoding="utf-8")
    for path,item in prices.proof.items():
        if core.sha(Path(path))!=item["sha256"]:raise ValueError("Stock input changed")
    for row in proof.itertuples():
        if core.sha(Path(row.path))!=row.sha256:raise ValueError("Nifty input changed")
    if core.sha(directory/"run_manifest.json")!=source_digest:raise ValueError("G3 source changed")
    if any(core.sha(Path(p))!=digest for p,digest in pointer_hashes.items()):raise ValueError("Unrelated publication pointer changed")
    code_paths=[Path(__file__),Path(study.__file__),Path(daily.__file__),Path(core.__file__),
        core.ROOT/"research/g8_exit_engine.py",core.ROOT/"research/g8_vix_data.py",core.ROOT/"research/g3_sl_target_simulator.py"]
    core.dump(out/"manifest.json",dict(code={str(p.relative_to(core.ROOT)):core.sha(p) for p in code_paths},
        files={str(p.relative_to(out)):core.sha(p) for p in out.rglob("*") if p.is_file()}))
    verify(out)
    core.dump(core.OUTPUT_ROOT/"latest_daily_comparison.json",dict(run_dir=str(out),through=str(day),
        manifest_sha256=core.sha(out/"manifest.json"),complete=True,execution_authority=False))
    print("VERIFIED",out)
    print(comparison[["label","period","sessions","trades","wins","losses","win_rate_pct","net_pnl_rs","losing_days","daily_close_drawdown_rs"]].to_string(index=False))
    return out


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day",type=date.fromisoformat)
    parser.add_argument("--output")
    parser.add_argument("--verify")
    args=parser.parse_args()
    if args.verify:print(verify(args.verify))
    elif args.day:run(args.day,args.output)
    else:parser.error("--day or --verify required")
