"""Isolated full available-history confirmation-volume sensitivity, no live writes."""
from __future__ import annotations
import sys,json
from pathlib import Path
from datetime import date
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research import g2_session_forensic_audit as audit
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
import fno_v13_v10_g_2_0925_comparison as comp

def orders_for(signals,bundle,threshold):
    work=signals.copy()
    # Native ranking does not consume 1m volume. Scaling changes only its fixed
    # 1.20 gate; restore the true observations in every output immediately.
    work['research_original_1m_volume_ratio']=work.v9_1m_volume_ratio
    work['research_original_oi_change_pct']=work.oi_change_pct
    if threshold=='short0936_oi0p10':
        patch=work.side.eq('SHORT')&work.hhmm_int.eq(935)&work.oi_change_pct.ge(.10)&work.oi_change_pct.lt(.50)
        # This setup ranks liquidity, not OI. Lower its core/expanded OI floor
        # from .50 to .10 with all prior strict gates already evaluated.
        work.loc[patch,'oi_change_pct']=.50
    else:
        work['v9_1m_volume_ratio']=work.v9_1m_volume_ratio*(1.2/threshold)
    orders=g2.g.select_orders(work,bundle['v9_config'],g2.g.SelectionChange(**bundle['source_g']['selection_change']),core_first=True)
    if len(orders):
        orders['v9_1m_volume_ratio']=orders.research_original_1m_volume_ratio
        orders['oi_change_pct']=orders.research_original_oi_change_pct
    return orders

def simulate(orders,paths,source):
    if orders.empty: return pd.DataFrame()
    trades=g2.simulate_staged(ext._apply_retained_g_exits(orders,source),paths,cost_bps=source['cost_bps'],max_entry_delay_minutes=10)
    base=ext._portfolio_config(source)
    trades=g2.g.v9.v5.apply_fixed_capital_model(trades,base.capital_per_entry_rupees,base.leverage_factor)
    return g2.g.v9.v6.apply_portfolio_constraints(trades,base.portfolio_config())[0]

def run(out):
    print('Verify frozen history',flush=True)
    bundle=g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE,g2.DEFAULT_G_CONFIG)
    thresholds=[1.2,1.1,1.0,'short0936_oi0p10']; records={t:[] for t in thresholds}; days=list(bundle['days']); provenance=[]
    selections={t:orders_for(bundle['signals'],bundle,t) for t in thresholds}
    needed=set(int(v) for x in selections.values() for v in x.sid)
    paths={}
    with np.load(bundle['source']/'dataset/paths.npz',allow_pickle=False) as z:
        for key in z.files:
            sid,field=key.split('_',1)
            if int(sid) in needed: paths.setdefault(int(sid),{})[field]=z[key]
    for t,orders in selections.items():
        g2.g.v9.validate_paths(orders,paths)
        records[t].append(simulate(orders,paths,bundle['source_g']).assign(segment='SEALED_38'))
        print('Frozen',t,audit.metrics(records[t][-1]),flush=True)
    verified={}
    for day in [date(2026,9,d) for d in [24,25,28,29,30]]+[date(2026,10,d) for d in [5,6,7]]:
        run,result,snapshot=comp.snapshot_for(day,verified)
        if (run/'feature_ledger.csv').exists():
            raw=audit.normalize(pd.read_csv(run/'feature_ledger.csv'))
            meta=g2.read_json(run/'feature_ledger.csv.manifest.json')
            assert g2.sha256(run/'feature_ledger.csv')==meta['artifact_sha256']
            signals=replay._strict_signals(raw,float(raw.nifty_first_bar_return_pct.dropna().iloc[0]))
        else:
            signals=pd.read_csv(run/'candidate_signals.csv')
            for c in ['signal_ts','confirmation_ts','v9_1m_feature_ts']:
                signals[c]=pd.to_datetime(signals[c],utc=True).dt.tz_convert(audit.IST)
            signals['day']=pd.to_datetime(signals.day).dt.date
        selected={t:orders_for(signals,bundle,t) for t in thresholds}
        union=pd.concat(selected.values(),ignore_index=True).drop_duplicates('sid')
        paths=ext._selected_paths(union,day,snapshot) if len(union) else {}
        for t,orders in selected.items():
            l=simulate(orders,paths,bundle['source_g'])
            if len(l): records[t].append(l.assign(segment=str(day)))
        days.append(day); provenance.append(dict(day=str(day),run=str(run),snapshot=str(snapshot),result_state=result['state']))
        print('Extension',day,'orders', {t:len(x) for t,x in selected.items()},flush=True)
    ledgers={t:pd.concat(items,ignore_index=True) for t,items in records.items()}
    base=ledgers[1.2]
    def keys(x): return set(zip(x.day.astype(str),x.tradingsymbol,x.setup_id))
    bk=keys(base.loc[base.portfolio_executed])
    daily=[]; results=[]; changes=[]
    for t,l in ledgers.items():
        l['trade_key']=l.day.astype(str)+'|'+l.tradingsymbol+'|'+l.setup_id
        executed=l.loc[l.portfolio_executed].copy()
        added=executed.loc[~executed.apply(lambda z:(str(z.day),z.tradingsymbol,z.setup_id) in bk,axis=1)]
        removed=bk-keys(executed)
        metric=audit.metrics(l)
        metric.update(variant=str(t),sessions=len(days),first_day=str(min(days)),last_day=str(max(days)),
          added_trades=len(added),added_losers=int(added.portfolio_net_profit_rupees.lt(0).sum()),
          added_winners=int(added.portfolio_net_profit_rupees.gt(0).sum()),removed_baseline_trades=len(removed),
          net_delta_vs_baseline=metric['net_pnl']-audit.metrics(base)['net_pnl'])
        results.append(metric)
        for day in days:
            d=l.loc[l.day.astype(str).eq(str(day))]
            daily.append(dict(date=str(day),variant=str(t),**audit.metrics(d)))
        if len(added): changes.append(added.assign(change_type='ADDED',variant=str(t)))
        removedrows=base.loc[base.apply(lambda z:(str(z.day),z.tradingsymbol,z.setup_id) in removed,axis=1)]
        if len(removedrows): changes.append(removedrows.assign(change_type='REMOVED',variant=str(t)))
        audit.csv(out/f'history_trades_minvol_{str(t).replace(".","p")}.csv',l)
    d=pd.DataFrame(daily); b=d.loc[d.variant.eq('1.2')].set_index('date').net_pnl
    d['net_delta_vs_baseline']=d.net_pnl-d.date.map(b)
    audit.csv(out/'historical_volume_comparison.csv',pd.DataFrame(results))
    audit.csv(out/'historical_volume_daywise.csv',d)
    audit.csv(out/'historical_volume_added_removed_trades.csv',pd.concat(changes,ignore_index=True) if changes else pd.DataFrame())
    audit.dump(out/'historical_volume_provenance.json',dict(source_bundle=str(bundle['source']),source_manifest_sha256=g2.sha256(bundle['source']/'dataset/dataset_manifest.json'),
      snapshot_hashes=verified,extensions=provenance,days=[str(d) for d in days],
      validation_status='ALL_46_AVAILABLE_COMPLETE_SESSIONS_REPLAYED_NOT_OUT_OF_SAMPLE',
      missing_sessions='No complete Oct 1 replay. Dates absent from the sealed 38-session manifest are not synthesized.',
      narrow_scope='Three confirmation-volume settings (1.20 baseline, 1.10, 1.00); separate 09:35 SHORT setup OI minimum .50 -> .10. Each rule applied across universe. All other strict gates, setup thresholds, ranking, quota, exit and cost unchanged.',
      missing_opportunity_metric='Historical +2%/-2% counts require official-close movement reconciliation on every date; not inferred from candidate-only histories.'))
    print(pd.DataFrame(results).to_string(index=False),flush=True)

if __name__=='__main__':run(Path(sys.argv[1]))
