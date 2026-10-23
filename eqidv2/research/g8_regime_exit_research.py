"""Finite SL/target study against the adopted G8 gate; publishes research only."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g8_main_baseline as adopted
from research import g8_vix_selection_research as core
from research import g8_vix_past_only as prior
from research.g8_exit_engine import allocate_variant, metrics, replay_exits
from research.g3_sl_target_simulator import simulate_exit

BASE = "adopted_g8_original_exits"
REGIMES = ["shock_or_rebound", "elevated_aligned", "elevated_mixed", "calm_aligned", "normal_mixed"]
FEATURE_PARENT = core.OUTPUT_ROOT / "past_only_research/2026-10-22/20261023T105040587682_past_only_v4"
THRESHOLDS = dict(high_vix_ratio=1.10, fast_vix_abs_5m_pct=1.0,
                  nifty_slot_pct=.05, nifty_session_pct=.10)


def action(stop=1., target=1., protect=False, quantity=1.):
    return dict(stop_multiplier=stop, target_multiplier=target,
                protect=protect, quantity_multiplier=quantity)


def specifications():
    specs = [dict(id=BASE, actions={})]
    def add(name, regimes, a):
        specs.append(dict(id=name, actions={r:deepcopy(a) for r in regimes}))
    for sm, tm in ((.75,.5),(.75,.75),(.875,.5),(.875,.75),(1.,.75)):
        add(f"shock_s{sm}_t{tm}", [REGIMES[0]], action(sm,tm))
    add("shock_profit_lock", [REGIMES[0]], action(protect=True))
    add("shock_quick_lock", [REGIMES[0]], action(.875,.75,True))
    for sm, tm in ((1.,.75),(1.25,.75),(1.25,1.),(1.,1.1)):
        add(f"elevated_trend_s{sm}_t{tm}", [REGIMES[1]], action(sm,tm))
    for sm, tm in ((.75,.5),(.75,.75),(.875,.75),(1.,.5)):
        add(f"elevated_mixed_s{sm}_t{tm}", [REGIMES[2]], action(sm,tm))
    add("calm_trend_target110", [REGIMES[3]], action(target=1.1))
    add("calm_trend_target120", [REGIMES[3]], action(target=1.2))
    add("all_profit_lock", REGIMES, action(protect=True))
    add("all_fast_control", REGIMES, action(.75,.75))
    add("all_tight_sl_control", REGIMES, action(.75,1.))
    add("risk_only_stress75", REGIMES[:3], action(quantity=.75))
    add("risk_only_all75", REGIMES, action(quantity=.75))
    specs += [
        dict(id="balanced_scenarios", actions={REGIMES[0]:action(.875,.75,True),
            REGIMES[1]:action(1.,.9),REGIMES[2]:action(.875,.75),REGIMES[3]:action(1.,1.1)}),
        dict(id="defensive_scenarios", actions={REGIMES[0]:action(.75,.5),
            REGIMES[1]:action(1.,.75),REGIMES[2]:action(.75,.75)}),
        dict(id="noise_buffer_scenarios", actions={REGIMES[0]:action(1.,.75,True),
            REGIMES[1]:action(1.25,.9),REGIMES[2]:action(.875,.75)}),
        dict(id="protect_winners_scenarios", actions={REGIMES[0]:action(protect=True),
            REGIMES[2]:action(protect=True)}),
    ]
    return specs


def classify(f):
    """Entry-only classification; signed Nifty changes are relative to trade side."""
    required = ["vix_level_ratio","vix_5m_pct","signed_nifty_slot","signed_nifty_session"]
    if not np.isfinite(f[required].to_numpy(float)).all():
        raise ValueError("Missing causal regime inputs")
    if not f.vix_level_ready.all():
        raise ValueError("Prior-session VIX level reference is not ready")
    high = f.vix_level_ratio.ge(THRESHOLDS["high_vix_ratio"])
    aligned = (f.signed_nifty_slot.ge(THRESHOLDS["nifty_slot_pct"])
               & f.signed_nifty_session.ge(THRESHOLDS["nifty_session_pct"]))
    reversal = (f.signed_nifty_slot.le(-THRESHOLDS["nifty_slot_pct"])
                & f.signed_nifty_session.ge(THRESHOLDS["nifty_session_pct"]))
    shock = f.vix_5m_pct.abs().ge(THRESHOLDS["fast_vix_abs_5m_pct"]) | reversal
    return pd.Series(np.select([shock,high&aligned,high&~aligned,~high&aligned],
        REGIMES[:4],default=REGIMES[4]),index=f.index,name="exit_regime")


def settings(f, spec):
    frame = pd.DataFrame(index=f.index)
    frame["regime"] = classify(f)
    for key, value in action().items():
        frame[key] = value
    for regime, changes in spec["actions"].items():
        for key, value in changes.items():
            frame.loc[frame.regime.eq(regime),key] = value
    frame["stop_pct"] = f.baseline_stop_pct * frame.stop_multiplier
    frame["target_pct"] = f.baseline_target_pct * frame.target_multiplier
    frame["quantity_scale"] = np.minimum(1.,f.baseline_stop_pct/frame.stop_pct)*frame.quantity_multiplier
    frame["protect_after_close_pct"] = np.where(frame.protect,np.minimum(.4,frame.target_pct*.5),np.nan)
    frame["protected_profit_pct"] = np.where(frame.protect,.10,0.)
    if (frame.loc[frame.protect,"protect_after_close_pct"] <= .10).any():
        raise ValueError("Profit protection trigger must exceed its gross lock")
    return frame


def replay(source, parameters):
    original = source["unfiltered"]
    raw = replay_exits(original, source["paths"], parameters.stop_pct,
                       parameters.target_pct, source["config"], risk_scale=True)
    # Allocation sizes from final gross-return fields, so protection is applied
    # before allocating and monetary fields are calculated afresh once.
    for index, row in raw.iterrows():
        p = parameters.loc[row.sid]
        raw.loc[index,"g8_quantity_scale"] = p.quantity_scale
        if row.g8_baseline_filled and p.protect:
            result = simulate_exit(source["paths"][int(row.sid)],int(row.entry_path_index),
                float(row.entry_price),row.side=="LONG",p.stop_pct,p.target_pct,
                protect_after_close_pct=p.protect_after_close_pct,protected_profit_pct=p.protected_profit_pct,
                tighten_after_minutes=source["config"].get("tighten_after_minutes",120),
                tightened_stop_pct=min(p.stop_pct,source["config"].get("tightened_stop_pct",1.)))
            for field, value in result.items():
                raw.at[index,field] = value
            raw.at[index,"net_return_pct"] = result["gross_return_pct"]-.05
    return raw


def changed_count(book, base, days):
    left=book.loc[book.day.isin(days)].set_index("sid")
    right=base.loc[base.day.isin(days)].set_index("sid").reindex(left.index)
    delta=left.portfolio_net_profit_rupees-right.portfolio_net_profit_rupees
    changed=delta.abs().gt(1e-6)|left.portfolio_executed.ne(right.portfolio_executed)
    return int(changed.sum()),int(left.loc[changed,"day"].nunique())


def eligible(row, base, *, development=False):
    """Capital-preservation floor predeclared before outcomes are inspected."""
    return bool(row.net_pnl_rs >= .95*base.net_pnl_rs
        and row.negative_trade_sum_abs_rs <= .95*base.negative_trade_sum_abs_rs
        and row.win_rate_pct >= base.win_rate_pct
        and row.losing_days <= base.losing_days
        and row.daily_close_drawdown_rs <= base.daily_close_drawdown_rs+1e-6
        and row.trades >= .90*base.trades
        and row.changed_trades >= (5 if development else 3)
        and row.changed_days >= (3 if development else 2))


def select(trials):
    train=trials.loc[trials.period.eq("development")].set_index("policy")
    ok=[name for name,row in train.iterrows() if name!=BASE and eligible(row,train.loc[BASE],development=True)]
    if not ok:
        return BASE,BASE
    ranked=train.loc[ok].sort_values(["losing_day_sum_abs_rs","negative_trade_sum_abs_rs","net_pnl_rs"],
        ascending=[True,True,False],kind="stable")
    candidate=str(ranked.index[0])
    validation=trials.loc[trials.period.eq("validation")].set_index("policy")
    selected=candidate if eligible(validation.loc[candidate],validation.loc[BASE]) else BASE
    return candidate,selected


def verify(out):
    out=Path(out).resolve()
    manifest=json.loads((out/"manifest.json").read_text())
    for name,digest in manifest["files"].items():
        p=(out/name).resolve()
        if not p.is_relative_to(out) or core.sha(p)!=digest:
            raise ValueError("Changed artifact: "+name)
    for name,digest in manifest["code"].items():
        if core.sha(core.ROOT/name)!=digest:
            raise ValueError("Changed code: "+name)
    summary=json.loads((out/"summary.json").read_text())
    trials=pd.read_csv(out/"parameter_trials.csv")
    if select(trials)!=(summary["development_candidate"],summary["validated_selection"]):
        raise ValueError("Selection reconciliation")
    f=pd.read_csv(out/"features.csv").set_index("sid",drop=False)
    if not classify(f).equals(f.exit_regime.rename("exit_regime")):
        raise ValueError("Regime mismatch")
    if not (pd.to_datetime(f.vix_available_at,utc=True)<=pd.to_datetime(f.confirmation_ts,utc=True)).all():
        raise ValueError("Future VIX close")
    comparison=pd.read_csv(out/"overall_comparison.csv")
    for label in summary["labels"]:
        book=pd.read_csv(out/label/"portfolio_trades.csv")
        expected=comparison.loc[comparison.label.eq(label)&comparison.period.eq("all")].iloc[0]
        actual=metrics(book,summary["days"])
        for field in ("trades","wins","losses","net_pnl_rs","gross_pnl_rs","cost_rs","losing_days"):
            if not np.isclose(actual[field],expected[field],atol=1e-6,rtol=0):
                raise ValueError("Metrics mismatch: "+label+"/"+field)
    return True


def run(output=None):
    pointer_path=core.OUTPUT_ROOT/"latest_baseline.json"
    pointer=json.loads(pointer_path.read_text())
    baseline_path=Path(pointer["run_dir"])
    adopted.verify(baseline_path)
    if pointer["selected_policy"] != adopted.POLICY:
        raise ValueError("This study requires the adopted 30-minute G8 baseline")
    prior.verify(FEATURE_PARENT)
    out=Path(output or core.OUTPUT_ROOT/"exit_regime_research"/core.CUTOFF/
        (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")+"_g8_exit_regimes"))
    out.mkdir(parents=True,exist_ok=False)
    pointer_hashes={name:core.sha(core.OUTPUT_ROOT/name) for name in
        ("latest_baseline.json","latest_research.json")}
    specs=specifications()
    core.dump(out/"predeclared_plan.json",dict(thresholds=THRESHOLDS,regime_precedence=REGIMES,
        candidates=specs,entry_gate=adopted.POLICY,split="first35 development, next10 validation, last11 assessment",
        selection="95% net retention, >=5% lower losing-trade sum, no worse win rate/drawdown/losing days, 90% trades; development >=5 changes on3 dates, validation >=3 on2 dates",
        risk_sizing="quantity=min(1,original_stop/new_stop)*quantity_multiplier; no size increase",
        protection="After a full post-entry minute CLOSE reaches min(0.4%,half candidate target), move stop to gross +0.10% from next candle; gaps use observed open",
        independent_holdout=False,feature_sampling="Regime fixed at entry; no intratrade Nifty/VIX refresh"))
    source=core.load_sources()
    f=pd.read_csv(FEATURE_PARENT/"features.csv").set_index("sid",drop=False)
    f["exit_regime"]=classify(f)
    f.to_csv(out/"features.csv",index=False)
    config=deepcopy(source["config"])
    config.update(strategy="V13-V10-G-8",variant_id="G8-EXIT-REGIME-RESEARCH-V1",execution_authority=False)
    allowed=core.allowed_for(f,dict(mode="persistent_direction",window=30,threshold=2.))
    days=source["days"]
    periods=dict(development=days[:35],validation=days[35:45],assessment=days[45:],all=days)
    books,raws,plans,records={},{},{},[]
    for spec in specs:
        name=spec["id"]
        p=settings(f,spec)
        plans[name]=p
        raw=replay(source,p)
        book=allocate_variant(raw,allowed,config,name)
        books[name],raws[name]=book,raw
        for period,calendar in periods.items():
            changes,dates=changed_count(book,books[BASE],calendar)
            records.append(dict(policy=name,period=period,changed_trades=changes,changed_days=dates,
                                **metrics(book,calendar)))
        print("Replayed",name,flush=True)
    base=books[BASE]
    sealed=pd.read_csv(baseline_path/"G8_selected/portfolio_trades.csv").set_index("sid")
    check=base.set_index("sid").reindex(sealed.index)
    if not np.array_equal(check.portfolio_executed,sealed.portfolio_executed) or not np.allclose(
            check.portfolio_net_profit_rupees,sealed.portfolio_net_profit_rupees,atol=1e-6,rtol=0):
        raise ValueError("Adopted G8 per-trade baseline does not reproduce")
    trial=pd.DataFrame(records)
    trial.to_csv(out/"parameter_trials.csv",index=False)
    candidate,selected=select(trial)
    all_rows=trial.loc[trial.period.eq("all")].set_index("policy")
    best_net=str(all_rows.net_pnl_rs.idxmax())
    floor=all_rows.loc[all_rows.net_pnl_rs.ge(.95*all_rows.loc[BASE,"net_pnl_rs"])]
    risk_best=str(floor.sort_values(["losing_day_sum_abs_rs","negative_trade_sum_abs_rs","net_pnl_rs"],ascending=[True,True,False]).index[0])
    labels=dict(baseline=BASE,development_candidate=candidate,validated_selection=selected,
        exploratory_best_profit=best_net,exploratory_risk_candidate=risk_best)
    comparisons,daily,costs,attribution,folds=[],[],[],[],[]
    for label,name in labels.items():
        book=books[name]
        (out/label).mkdir()
        book.to_csv(out/label/"portfolio_trades.csv",index=False)
        plans[name].to_csv(out/label/"entry_exit_settings.csv",index_label="sid")
        for period,calendar in periods.items():
            comparisons.append(dict(label=label,policy=name,period=period,**metrics(book,calendar)))
        for day in days:
            daily.append(dict(label=label,day=day,**metrics(book,[day])))
        for bps in (5.,10.,15.):
            stress=allocate_variant(raws[name],allowed,config,name,cost_bps=bps)
            for period,calendar in periods.items():
                costs.append(dict(label=label,cost_bps=bps,period=period,**metrics(stress,calendar)))
        b=base.set_index("sid"); c=book.set_index("sid").reindex(b.index)
        for sid,row in b.iterrows():
            delta=float(c.loc[sid,"portfolio_net_profit_rupees"]-row.portfolio_net_profit_rupees)
            if abs(delta)>1e-6 or bool(c.loc[sid,"portfolio_executed"])!=bool(row.portfolio_executed):
                attribution.append(dict(label=label,sid=sid,day=row.day,symbol=row.tradingsymbol,
                    setup_id=row.setup_id,regime=f.loc[sid,"exit_regime"],baseline_executed=bool(row.portfolio_executed),
                    candidate_executed=bool(c.loc[sid,"portfolio_executed"]),baseline_pnl=row.portfolio_net_profit_rupees,
                    candidate_pnl=c.loc[sid,"portfolio_net_profit_rupees"],delta_rs=delta))
    # Past-only rolling choices use the same guard, never later block outcomes.
    for start in (21,28,35,42,49):
        history=days[:start]; future=days[start:start+7]
        rows=[]
        for name,book in books.items():
            changes,dates=changed_count(book,base,history)
            rows.append(dict(policy=name,changed_trades=changes,changed_days=dates,**metrics(book,history)))
        past=pd.DataFrame(rows).set_index("policy")
        good=[name for name,row in past.iterrows() if name!=BASE and eligible(row,past.loc[BASE],development=True)]
        pick=BASE if not good else str(past.loc[good].sort_values(["losing_day_sum_abs_rs","negative_trade_sum_abs_rs","net_pnl_rs"],ascending=[True,True,False]).index[0])
        folds.append(dict(train_through=history[-1],test_from=future[0],test_through=future[-1],policy=pick,
            baseline_net_rs=metrics(base,future)["net_pnl_rs"],**metrics(books[pick],future)))
    comparison=pd.DataFrame(comparisons); daily=pd.DataFrame(daily)
    comparison.to_csv(out/"overall_comparison.csv",index=False)
    pd.DataFrame(costs).to_csv(out/"cost_sensitivity.csv",index=False)
    pd.DataFrame(attribution).to_csv(out/"changed_trades.csv",index=False)
    pd.DataFrame(folds).to_csv(out/"rolling_choices.csv",index=False)
    daily.to_csv(out/"daywise_long.csv",index=False)
    pivot=daily.pivot(index="day",columns="label",values="net_pnl_rs")
    for label in labels:
        if label!="baseline": pivot[label+"_delta_rs"]=pivot[label]-pivot.baseline
    pivot.to_csv(out/"daywise_comparison.csv")
    # All 51 slot/side rows are mapped to shared scenario actions. No slot is
    # optimized independently using its later profits or losses.
    slot_rows,coverage=[],[]
    for setup in config["active_setups"]:
        for regime in REGIMES:
            sidset=f.index[f.setup_id.eq(setup["setup_id"])&f.exit_regime.eq(regime)]
            subset=base.loc[base.sid.isin(sidset)]
            support={period+"_trades":metrics(subset,calendar)["trades"] for period,calendar in periods.items()}
            coverage.append(dict(setup_id=setup["setup_id"],regime=regime,**support))
            for label in ("validated_selection","development_candidate","exploratory_risk_candidate","exploratory_best_profit"):
                spec=next(s for s in specs if s["id"]==labels[label])
                a=spec["actions"].get(regime,action())
                stop=setup["stop_pct"]*a["stop_multiplier"]; target=setup["target_pct"]*a["target_multiplier"]
                slot_rows.append(dict(label=label,policy=labels[label],setup_id=setup["setup_id"],
                    slot=setup["confirmation_end"],side=setup["side"],regime=regime,
                    original_sl_pct=setup["stop_pct"],original_target_pct=setup["target_pct"],
                    proposed_sl_pct=stop,proposed_target_pct=target,
                    sl_change_percent=(a["stop_multiplier"]-1)*100,target_change_percent=(a["target_multiplier"]-1)*100,
                    quantity_percent=100*min(1.,setup["stop_pct"]/stop)*a["quantity_multiplier"],
                    profit_lock_activation_pct=min(.4,target*.5) if a["protect"] else None,
                    profit_lock_gross_pct=.1 if a["protect"] else None,
                    support="SPARSE: fewer than20 historical trades" if support["all_trades"]<20 else "Historical only; not independent validation",**support))
    slots=pd.DataFrame(slot_rows)
    slots.to_csv(out/"slot_scenario_plan.csv",index=False)
    pd.DataFrame(coverage).to_csv(out/"slot_regime_coverage.csv",index=False)
    for label in ("validated_selection","development_candidate","exploratory_risk_candidate","exploratory_best_profit"):
        slots.loc[slots.label.eq(label)].to_csv(out/(label+"_slot_plan.csv"),index=False)
    regime_rows=[]
    for regime in REGIMES:
        members=f.index[f.exit_regime.eq(regime)]
        for label,name in labels.items():
            regime_rows.append(dict(regime=regime,label=label,**metrics(books[name].loc[books[name].sid.isin(members)],days)))
    regime_results=pd.DataFrame(regime_rows)
    regime_results.to_csv(out/"regime_comparison.csv",index=False)
    summary=dict(through=core.CUTOFF,days=days,candidate_count=len(specs),development_candidate=candidate,
        validated_selection=selected,labels=labels,baseline_run=str(baseline_path),baseline_manifest=pointer["manifest_sha256"],
        feature_parent=str(FEATURE_PARENT),feature_manifest_sha256=core.sha(FEATURE_PARENT/"manifest.json"),
        source_cache_manifest_sha256=source["manifest_sha256"],execution_authority=False,independent_holdout=False,
        no_baseline_promotion=True,thresholds=THRESHOLDS,
        limitations=["All dates were used in previous research; splits are retrospective checks.",
        "Entry-only market regimes. Profit protection uses completed stock closes and next-bar stops.",
        "The minute-bar path cannot establish real executable fills; gaps can exceed stops.",
        "Stop width changes do not forecast individual-stock volatility.",
        "A lower absolute loss achieved through smaller quantity is risk reduction, not necessarily better selection."])
    core.dump(out/"summary.json",summary)
    write_report(out,summary,comparison,pivot,slots,regime_results,trial,pd.DataFrame(folds))
    code_paths=[Path(__file__),Path(adopted.__file__),Path(core.__file__),Path(prior.__file__),
        core.ROOT/"research/g8_sources.py",core.ROOT/"research/g8_exit_engine.py",core.ROOT/"research/g3_sl_target_simulator.py"]
    core.dump(out/"manifest.json",dict(code={str(p.relative_to(core.ROOT)):core.sha(p) for p in code_paths},
        files={str(p.relative_to(out)):core.sha(p) for p in out.rglob("*") if p.is_file()}))
    verify(out)
    if any(core.sha(core.OUTPUT_ROOT/name)!=value for name,value in pointer_hashes.items()):
        raise ValueError("Adopted publication pointer changed during research")
    core.dump(core.OUTPUT_ROOT/"latest_exit_regime_research.json",dict(run_dir=str(out),
        manifest_sha256=core.sha(out/"manifest.json"),validated_selection=selected,execution_authority=False))
    print(json.dumps(summary,indent=2),flush=True)
    return out


def write_report(out,summary,comparison,pivot,slots,regimes,trial,folds):
    def table(df):
        return df.to_html(index=False,float_format=lambda x:f"{x:,.2f}",escape=True)
    headline=comparison.loc[comparison.period.eq("all"),["label","policy","trades","win_rate_pct","net_pnl_rs","losing_days","losing_day_sum_abs_rs","negative_trade_sum_abs_rs","daily_close_drawdown_rs"]]
    chosen=summary["labels"]["exploratory_risk_candidate"]
    grid=slots.loc[slots.label.eq("exploratory_risk_candidate")].copy()
    grid["SL / target %"]=grid.proposed_sl_pct.map(lambda x:f"{x:.2f}")+" / "+grid.proposed_target_pct.map(lambda x:f"{x:.2f}")
    grid=grid.pivot(index=["setup_id","original_sl_pct","original_target_pct"],columns="regime",values="SL / target %").reset_index()
    report="""<!doctype html><meta charset='utf-8'><title>G8 SL and target regime study</title><style>body{font:15px system-ui;margin:30px;color:#172333}table{border-collapse:collapse;font-size:13px}th,td{border:1px solid #ccd3dc;padding:7px;text-align:right}th{background:#eef2f7}section{overflow:auto;margin-bottom:28px}a{color:#17519a}</style><h1>G8 VIX / Nifty SL and target study</h1>"""
    report+="<p>56 eligible sessions, 29 July–22 October 2026. Fixed adopted G8 entry filter, original stock entries, 5bps costs. Original targets are per slot. Scenarios are determined at entry.</p>"
    report+="<p>Regimes in order: shock/rebound (absolute 5-minute VIX move ≥1%, or Nifty recent move ≥0.05% against the trade while session is ≥0.10% in favour); elevated aligned (VIX ≥110% of prior20-session median and Nifty slot/session ≥0.05%/0.10% in favour); elevated mixed; calm aligned; normal mixed.</p>"
    report+="<p>Development candidate: "+html.escape(summary["development_candidate"])+". After validation: "+html.escape(summary["validated_selection"])+". No default baseline changes. All dates were previously researched; none is an untouched holdout.</p>"
    report+="<section>"+table(headline)+"</section><h2>Period checks</h2><section>"+table(comparison[["label","period","trades","win_rate_pct","net_pnl_rs","losing_days","negative_trade_sum_abs_rs"]])+"</section>"
    report+="<h2>All 51 slots: exploratory risk candidate</h2><p>"+html.escape(chosen)+". Each cell is SL% / target%. A 25% reduction means multiplying the existing value by0.75, not subtracting25 percentage points. Shared scenario rules, no individual slot fitting.</p><section>"+table(grid)+"</section>"
    report+="<p>Exact quantity percentages, gross profit-lock triggers and observation counts are in <a href='slot_scenario_plan.csv'>slot_scenario_plan.csv</a>. Sparse cells are not reliable individual-slot estimates.</p>"
    report+="<h2>Market scenario performance</h2><section>"+table(regimes[["regime","label","trades","win_rate_pct","net_pnl_rs","negative_trade_sum_abs_rs"]])+"</section>"
    report+="<h2>Daily results</h2><section>"+table(pivot.reset_index())+"</section><h2>Rolling earlier-data selection</h2><section>"+table(folds[["train_through","test_from","test_through","policy","baseline_net_rs","net_pnl_rs","losing_days"]])+"</section>"
    report+="<p><a href='parameter_trials.csv'>Every policy and period</a> | <a href='cost_sensitivity.csv'>5/10/15bps costs</a> | <a href='changed_trades.csv'>Changed trades</a> | <a href='daywise_comparison.csv'>Daily CSV</a> | <a href='validated_selection_slot_plan.csv'>Validation-selected slot plan</a></p>"
    report+="<p>Profit protection: a full post-entry minute must close at min(0.40%,half the candidate target) in favour; only the next bar can use the gross+0.10% stop. Gaps and costs can still produce a loss. Wider stops reduce quantity. Stops cannot loosen after entry.</p>"
    report+="<p>Sources: <a href='https://www.nseindia.com/static/products-services/indices-indiavix-index'>NSE India VIX definition</a>; <a href='https://www.investor.gov/introduction-investing/investing-basics/glossary/stop-order'>stop order execution limitations</a>. VIX is annualised expected Nifty volatility over30calendar days, not an intraday stock target.</p>"
    (out/"report.html").write_text(report,encoding="utf-8")


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output")
    parser.add_argument("--verify")
    args=parser.parse_args()
    print(verify(args.verify)) if args.verify else run(args.output)
