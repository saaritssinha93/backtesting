"""Isolated retrospective G2 session audit. Never modifies strategy or sends orders."""
from __future__ import annotations
import argparse, hashlib, html, io, json, sys, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext
import fno_v13_v10_g_daily_replay as replay
import fno_oi_hybrid_data as hybrid

IST='Asia/Kolkata'
ROOT=Path(r'C:\TradingData\eqidv2\fno_oi\strategy_research\v13_g2_session_audit')
LIVE=Path(r'C:\TradingData\eqidv2\fno_oi\v13_v10_g_live')
def dump(p,x): p.write_text(json.dumps(x,indent=2,default=str,allow_nan=True),encoding='utf-8')
def csv(p,x): x.to_csv(p,index=False)
def flag(frame):
    out=pd.Series(False,index=frame.index)
    for c in ['gap_filled','opening_snapshot','provisional_stale']:
        if c in frame: out|=frame[c].astype(str).str.lower().isin(['true','1','1.0','yes'])
    return out

def bhav(day,out):
    name=f'BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv.zip'
    url='https://nsearchives.nseindia.com/content/cm/'+name
    dest=out/name
    if not dest.exists():
        with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0'}),timeout=30) as r:
            dest.write_bytes(r.read())
    with zipfile.ZipFile(dest) as z:
        member=z.namelist()[0]; data=z.read(member); (out/member).write_bytes(data)
    frame=pd.read_csv(io.BytesIO(data)); frame=frame.loc[frame.SctySrs.eq('EQ')].copy()
    assert frame.TckrSymb.is_unique
    assert set(frame.TradDt)=={str(day)}
    return frame.set_index('TckrSymb'),dict(url=url,path=str(dest),sha256=g2.sha256(dest),fetched_at=datetime.now().isoformat())

def minute_day(path,day):
    names=pq.ParquetFile(path).schema.names
    cols=[c for c in ['date','open','high','low','close','volume','Prev_Day_Close','gap_filled','opening_snapshot','provisional_stale'] if c in names]
    df=pd.read_parquet(path,columns=cols)
    df['ts']=hybrid._to_ist(df.date).dt.as_unit('ns')
    return df.loc[df.ts.dt.date.eq(day)].sort_values('ts').reset_index(drop=True)

def movement(contract,day,snapshot,prev,today,excluded):
    s=contract['equity_symbol']; p=snapshot/'equity_1m'/f'{s}_stocks_indicators_1min.parquet'
    row=dict(symbol=s,minute_source=str(p),g2_data_status='EXCLUDED_MISSING_OI' if s in excluded else 'INCLUDED')
    m=minute_day(p,day) if p.exists() else pd.DataFrame()
    if m.empty: return {**row,'movement_status':'DATA_UNAVAILABLE'},m
    nums=m[['open','high','low','close','volume']].astype(float)
    valid=(np.isfinite(nums).all(axis=1)&nums[['open','high','low','close']].gt(0).all(axis=1)
        &nums.high.ge(nums[['open','close','low']].max(axis=1))&nums.low.le(nums[['open','close','high']].min(axis=1))
        &nums.volume.gt(0)&~flag(m)&~m.ts.duplicated(keep=False))
    valid &= m.ts.ge(pd.Timestamp(f'{day} 09:16',tz=IST)) & m.ts.le(pd.Timestamp(f'{day} 15:15',tz=IST))
    v=m.loc[valid].copy(); expected=pd.date_range(f'{day} 09:16',f'{day} 15:15',freq='min',tz=IST)
    five=hybrid.aggregate_equity_one_minute_to_five_minute(m.loc[m.ts.isin(expected)].copy())
    fvalid=five.source_1m_count.eq(5)&~flag(five)&five.volume.gt(0)
    row.update(minute_rows=len(m),valid_minutes=len(v),expected_continuous_minutes=len(expected),
        valid_minute_coverage_pct=100*len(v)/len(expected),invalid_or_zero_volume_minutes=int((~valid).sum()),
        missing_minutes=';'.join(expected.difference(pd.DatetimeIndex(m.ts)).strftime('%H:%M')),
        valid_5m_bars=int(fvalid.sum()),expected_5m_bars=72,
        first_valid_time=str(v.ts.min()),last_valid_time=str(v.ts.max()),
        cached_previous_close=float(m.Prev_Day_Close.iloc[0]) if 'Prev_Day_Close' in m else np.nan)
    if s not in prev.index or s not in today.index or v.empty:
        return {**row,'movement_status':'OFFICIAL_REFERENCE_OR_INTRADAY_MISSING'},v
    a,b=prev.loc[s],today.loc[s]; pc=float(a.ClsPric); hi=float(v.high.max()); lo=float(v.low.min())
    row.update(movement_status='USABLE',official_prev_date=str(a.TradDt),official_previous_close=pc,
        official_day_previous_close=float(b.PrvsClsgPric),official_previous_close_matches=abs(pc-b.PrvsClsgPric)<.011,
        cached_previous_close_error_rupees=row['cached_previous_close']-pc,
        intraday_high=hi,intraday_low=lo,peak_time=str(v.loc[v.high.eq(hi),'ts'].iloc[0]),
        trough_time=str(v.loc[v.low.eq(lo),'ts'].iloc[0]),
        peak_last_time=str(v.loc[v.high.eq(hi),'ts'].iloc[-1]),trough_last_time=str(v.loc[v.low.eq(lo),'ts'].iloc[-1]),
        peak_pct=(hi/pc-1)*100,trough_pct=(lo/pc-1)*100,
        official_high=float(b.HghPric),official_low=float(b.LwPric),official_close=float(b.ClsPric),
        official_peak_pct=(float(b.HghPric)/pc-1)*100,official_trough_pct=(float(b.LwPric)/pc-1)*100,
        close_return_pct=(float(b.ClsPric)/pc-1)*100,continuous_last_price=float(v.close.iloc[-1]),
        official_high_matches_observed=abs(hi-b.HghPric)<.011,official_low_matches_observed=abs(lo-b.LwPric)<.011)
    row['official_peak_time']=row['peak_time'] if row['official_high_matches_observed'] else 'UNAVAILABLE_DAILY_EXTREME_NOT_IN_MINUTE_PATH'
    row['official_trough_time']=row['trough_time'] if row['official_low_matches_observed'] else 'UNAVAILABLE_DAILY_EXTREME_NOT_IN_MINUTE_PATH'
    # Execution reuses the native path, including zero-volume minute placeholders;
    # movement extrema above deliberately require traded bars. Do not alter G2 fills.
    return row,m.loc[m.ts.isin(expected)].copy()

def normalize(raw):
    r=raw.copy()
    for c in ['signal_ts','confirmation_ts']: r[c]=pd.to_datetime(r[c],utc=True).dt.tz_convert(IST)
    r['day']=r.signal_ts.dt.date; r['hhmm_int']=r.signal_ts.dt.strftime('%H%M').astype(int)
    r['v9_1m_feature_ts']=r.confirmation_ts
    r['v9_exact_confirmation_present']=r.confirmation_close.notna()
    r['v9_5m_ema_bull']=r.ema9.gt(r.ema20)&r.ema20.gt(r.ema50)
    r['v9_5m_ema_bear']=r.ema9.lt(r.ema20)&r.ema20.lt(r.ema50)
    r['sid']=np.arange(len(r))
    return r

def checks(r,s):
    sign=1 if s.side=='LONG' else -1
    # Positive margin passes; strict comparisons are explicitly marked.
    vals=[('price_data',float(all(np.isfinite(r.get(k,np.nan)) for k in ['open','high','low','close','volume'])),1,'>=','DATA'),
      ('oi_positive',min(r.oi,r.prev_oi),0,'>','DATA'),
      ('oi_increasing',r.oi-r.prev_oi,0,'>','5M'),
      ('oi_base_min',r.oi_change_pct,.05,'>=','5M'),('oi_max',r.oi_change_pct,1.,'<=','5M'),
      ('volume_base',r.volume_ratio,.8,'>=','5M'),
      ('ema9_20',sign*(r.ema9-r.ema20),0,'>','5M'),('ema20_50',sign*(r.ema20-r.ema50),0,'>','5M'),
      ('price_base',sign*r.price_change_pct,.1,'>=','5M'),
      ('setup_price',sign*r.price_change_pct,s.price_change_pct,'>=','5M'),
      ('setup_oi',r.oi_change_pct,s.oi_change_pct,'>=','5M'),
      ('setup_volume',r.volume_ratio,s.volume_ratio,'>=','5M'),
      ('setup_liquidity',r.traded_value,s.min_traded_value,'>=','5M'),
      ('confirmation_present',float(r.v9_exact_confirmation_present),1,'>=','1M'),
      ('confirmation_range',r.confirmation_high-r.confirmation_low,0,'>','1M'),
      ('confirmation_direction',sign*(r.confirmation_close-r.confirmation_open),0,'>','1M'),
      ('confirmation_beyond_signal',sign*(r.confirmation_close-r.signal_close),0,'>','1M'),
      ('setup_body',r.body_ratio,s.body_ratio,'>=','1M'),
      ('setup_wick',r.v9_1m_upper_wick_ratio if sign==1 else r.v9_1m_lower_wick_ratio,s.max_wick_ratio,'<=','1M'),
      ('confirmation_volume',r.v9_1m_volume_ratio,1.2,'>=','1M'),
      ('exact_confirmation_clock',(r.confirmation_ts-r.signal_ts).total_seconds(),60,'==','1M')]
    if s.side=='SHORT' and s.signal_end=='09:25': vals.append(('nifty_first_5m_return',r.nifty_first_bar_return_pct,-.05,'<=','5M'))
    return vals

def passed(v,t,op):
    if not np.isfinite(v): return False
    return v>=t if op=='>=' else v>t if op=='>' else v<=t if op=='<=' else v==t

def select(raw,pairs,policy=None):
    """Independent complete-universe selection, matching native core-first semantics."""
    selected=[]; eligible=[]; policy=policy or {}
    for core,setup in pairs:
        part=raw.loc[raw.signal_ts.dt.strftime('%H:%M').eq(setup.signal_end)].copy()
        key=setup.setup_id; changes=policy.get('changes',{}) if policy.get('setup_id')==key else {}
        records=[]
        for _,r in part.iterrows():
            ck=checks(r,setup)
            ok=all(True if k in changes and changes[k]=='BYPASS' else passed(v,changes.get(k,t),op) for k,v,t,op,_ in ck)
            if not ok: continue
            core_ok=all(True if k in changes and changes[k]=='BYPASS' else passed(v,changes.get(k,t),op) for k,v,t,op,_ in checks(r,core))
            rr=r.to_dict(); rr.update(side=setup.side,setup_id=key,picker=setup.picker,max_entries=setup.max_entries,
              abs_price_change_pct=abs(r.price_change_pct),trigger=r.confirmation_high if setup.side=='LONG' else r.confirmation_low,
              wick_ratio=r.v9_1m_upper_wick_ratio if setup.side=='LONG' else r.v9_1m_lower_wick_ratio,core_eligible=core_ok)
            records.append(rr)
        if not records: continue
        ranked=pd.DataFrame(records)
        picker=g2.g.v9.PICKER_COLUMNS[setup.picker]
        ranked=ranked.sort_values([picker,'traded_value','tradingsymbol'],ascending=[False,False,True],kind='stable')
        core_ids=ranked.loc[ranked.core_eligible].head(core.max_entries).sid.tolist()
        ranked['core_reserved']=ranked.sid.isin(core_ids)
        ranked=ranked.sort_values('core_reserved',ascending=False,kind='stable')
        ranked['effective_rank']=np.arange(1,len(ranked)+1)
        quota=policy.get('quota',setup.max_entries) if policy.get('setup_id')==key else setup.max_entries
        ranked['selected']=ranked.effective_rank.le(quota)
        eligible.append(ranked); selected.append(ranked.loc[ranked.selected])
    template=raw.iloc[:0].copy()
    for c in ['side','setup_id','picker','trigger','wick_ratio']: template[c]=pd.Series(dtype='object')
    return (pd.concat(selected,ignore_index=True) if selected else template,
            pd.concat(eligible,ignore_index=True) if eligible else template)

def simulate(orders,minutes,source):
    if orders.empty: return pd.DataFrame()
    paths={}
    for r in orders.itertuples():
        m=minutes[r.tradingsymbol]; path=m.loc[m.ts.gt(r.confirmation_ts)&m.ts.le(replay._cutoff(r.day))]
        paths[int(r.sid)]={'timestamp_ns':path.ts.astype('int64').to_numpy(),**{k:path[k].to_numpy(float) for k in ['open','high','low','close']}}
    work=ext._apply_retained_g_exits(orders,source)
    trades=g2.simulate_staged(work,paths,cost_bps=source['cost_bps'],max_entry_delay_minutes=10)
    base=ext._portfolio_config(source)
    trades=g2.g.v9.v5.apply_fixed_capital_model(trades,base.capital_per_entry_rupees,base.leverage_factor)
    return g2.g.v9.v6.apply_portfolio_constraints(trades,base.portfolio_config())[0]

def metrics(ledger):
    e=ledger.loc[ledger.portfolio_executed.eq(True)] if len(ledger) else ledger
    if e.empty: return dict(trades=0,wins=0,losses=0,net_pnl=0.,cost=0.,win_rate_pct=None,profit_factor=None,closed_trade_drawdown=0.)
    pnl=e.sort_values('exit_ts').portfolio_net_profit_rupees
    equity=pd.Series([0.,*pnl.cumsum().tolist()])
    return dict(trades=len(e),wins=int(pnl.gt(0).sum()),losses=int(pnl.lt(0).sum()),net_pnl=float(pnl.sum()),
      cost=float(e.portfolio_cost_rupees.sum()),win_rate_pct=100*float(pnl.gt(0).mean()),
      profit_factor=float(pnl[pnl>0].sum()/-pnl[pnl<0].sum()) if pnl.lt(0).any() else None,
      closed_trade_drawdown=float((equity.cummax()-equity).max()))

def live_evidence(day,out):
    inventory=[]; records=[]
    for category in ['scanner_5m','confirmation_1m','signals','orders','order_events','evidence']:
        for p in (LIVE/category).rglob('*'):
            if not p.is_file() or not (str(day) in str(p) or day.strftime('%Y%m%d') in p.name): continue
            inventory.append(dict(category=category,path=str(p),sha256=g2.sha256(p),bytes=p.stat().st_size))
            if p.suffix=='.json':
                try:
                    data=json.loads(p.read_text(encoding='utf-8'))
                    records.append(dict(category=category,path=str(p),record=data))
                except Exception as exc: records.append(dict(category=category,path=str(p),error=str(exc)))
    csv(out/'live_artifact_inventory.csv',pd.DataFrame(inventory)); dump(out/'live_recorded_evidence.json',records)
    return records

def run(day,out):
    out.mkdir(parents=True,exist_ok=True); sources=out/'official_sources'; sources.mkdir(exist_ok=True)
    print('Verifying frozen bundle and dated snapshot',flush=True)
    bundle=g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE,g2.DEFAULT_G_CONFIG); source=bundle['source_g']
    run,result=ext._latest_successful_run(ext.DEFAULT_DAILY_ROOT,day)
    manifest=g2.read_json(run/'source_manifest.json'); snap=Path(manifest['input_snapshot']['root'])
    replay._verify_input_snapshot(snap,g2.read_json(snap/'snapshot_manifest.json'))
    feature_meta=g2.read_json(run/'feature_ledger.csv.manifest.json')
    assert g2.sha256(run/'feature_ledger.csv')==feature_meta['artifact_sha256']
    uni=pd.read_parquet(snap/'universe'/f'near_month_{day}.parquet')
    uni=uni.loc[~uni.underlying.isin(ext.common.INDEX_UNDERLYINGS)]
    assert uni.equity_symbol.is_unique
    csv(out/'dated_stock_universe.csv',uni)
    print('Downloading official NSE closes',flush=True)
    today,tsrc=bhav(day,sources)
    previous=day-timedelta(days=1)
    prev,psrc=bhav(previous,sources)
    excluded={r['symbol'] for r in result.get('excluded_stocks',[])}
    if not excluded: excluded={r['symbol'] for r in manifest.get('excluded_stocks',[])}
    print('Reconciling all stock price paths',flush=True)
    with ThreadPoolExecutor(max_workers=6) as pool:
        data=list(pool.map(lambda r:movement(r,day,snap,prev,today,excluded),uni.to_dict('records')))
    moves=pd.DataFrame([r for r,_ in data]); minutes={r['symbol']:m for r,m in data}
    csv(out/'universe_movements.csv',moves)
    usable=moves.loc[moves.movement_status.eq('USABLE')]
    up=usable.loc[usable.official_peak_pct.ge(2)].sort_values('official_peak_pct',ascending=False)
    down=usable.loc[usable.official_trough_pct.le(-2)].sort_values('official_trough_pct')
    topup=usable.nlargest(10,'official_peak_pct'); topdown=usable.nsmallest(10,'official_trough_pct')
    for name,frame in [('gainers_ge_2pct',up),('decliners_le_minus2pct',down),('top10_long',topup),('top10_short',topdown)]: csv(out/f'{name}.csv',frame)
    raw=normalize(pd.read_csv(run/'feature_ledger.csv')); raw['evidence_csv_row']=raw.index+2
    csv(out/'raw_slot_features.csv',raw)
    pairs=[g2.g.setup_pair(s,g2.g.SelectionChange(**source['selection_change'])) for s in g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES['higher_frequency'])]
    dump(out/'frozen_g2_config.json',g2.config(source)); dump(out/'setup_thresholds.json',[dict(core=asdict(a),expanded=asdict(b)) for a,b in pairs])
    orders,eligible=select(raw,pairs)
    strict=replay._strict_signals(raw,float(raw.nifty_first_bar_return_pct.dropna().iloc[0]))
    native=g2.g.select_orders(strict,bundle['v9_config'],g2.g.SelectionChange(**source['selection_change']),core_first=True)
    keys=lambda x:set(zip(x.tradingsymbol,x.setup_id))
    assert keys(orders)==keys(native),'Independent selection differs from native frozen G2'
    csv(out/'frozen_selected_orders.csv',orders); csv(out/'frozen_eligible_ranking.csv',eligible)
    ledger=simulate(orders,minutes,source); csv(out/'frozen_portfolio_trades.csv',ledger)
    print('Frozen session result',metrics(ledger),flush=True)
    gates=[]; slots=[]; qualifiers=set(up.symbol)|set(down.symbol)
    orderkeys=keys(orders)
    for symbol in uni.equity_symbol:
        for core,s in pairs:
            subset=raw.loc[raw.tradingsymbol.eq(symbol)&raw.signal_ts.dt.strftime('%H:%M').eq(s.signal_end)]
            status=dict(symbol=symbol,setup_id=s.setup_id,side=s.side,signal_time=f'{day} {s.signal_end}',confirmation_time=f'{day} {s.confirmation_end}',qualifies_2pct=symbol in qualifiers)
            if subset.empty:
                slots.append({**status,'selection_status':'DATA_UNAVAILABLE','failures':'WHOLE_SYMBOL_EXCLUSION_MISSING_EARLY_OI' if symbol in excluded else 'NO_FEATURE_ROW','broker_submission':'NOT_RECORDED_FOR_FROZEN_RECONSTRUCTION'})
                continue
            r=subset.iloc[0]; failures=[]; ck=checks(r,s)
            prepassed=all(passed(v,t,op) for k,v,t,op,stage in ck if stage in ['DATA','5M'])
            for k,v,t,op,stage in ck:
                ok=passed(v,t,op); margin=t-v if op=='<=' else v-t
                if not ok: failures.append(k)
                gates.append({**status,'gate':k,'stage':stage,'observed':v,'operator':op,'required':t,'margin':margin,
                  'threshold_result':'PASS' if ok else 'DATA_UNAVAILABLE' if not np.isfinite(v) else 'FAIL',
                  'pipeline_evaluation':'RECONSTRUCTED' if stage!='1M' or prepassed else 'NOT_REACHED_DIAGNOSTIC_ONLY',
                  'decision_time':str(r.confirmation_ts if stage=='1M' else r.signal_ts),
                  'source':str(run/'feature_ledger.csv'),'source_row':r.evidence_csv_row,'input_slice_sha256':r.input_slice_sha256})
            er=eligible.loc[eligible.tradingsymbol.eq(symbol)&eligible.setup_id.eq(s.setup_id)] if len(eligible) else eligible
            lr=ledger.loc[ledger.tradingsymbol.eq(symbol)&ledger.setup_id.eq(s.setup_id)] if len(ledger) else ledger
            state='FILTER_FAILURE' if failures else 'FILTER_PASS_NOT_SELECTED'
            if (symbol,s.setup_id) in orderkeys: state='SELECTED'
            extra={}
            if len(lr):
                x=lr.iloc[0]
                state='SIMULATED_FILLED' if x.portfolio_executed else 'SIMULATED_'+str(x.portfolio_status)
                extra={k:x.get(k) for k in ['entry_ts','entry_price','exit_ts','exit_price','exit_reason','portfolio_net_profit_rupees','portfolio_reject_reason']}
                peak=moves.loc[moves.symbol.eq(symbol),'peak_time' if s.side=='LONG' else 'trough_time'].iloc[0]
                extra['entry_before_observed_extreme']=bool(x.portfolio_executed and pd.Timestamp(x.entry_ts)<pd.Timestamp(peak))
            slots.append({**status,'selection_status':state,'failures':';'.join(failures),'failed_gate_count':len(failures),
               'confirmation_reached':prepassed,'effective_rank':er.effective_rank.iloc[0] if len(er) else np.nan,
               'slot_quota':s.max_entries,'symbol_day_quota':'NOT_CONFIGURED',
               'capital_guard':'PASSED' if len(lr) and bool(lr.iloc[0].portfolio_executed) else 'NOT_REACHED',
               'broker_submission':'NOT_APPLICABLE_RESEARCH_SIMULATION',**extra})
    gate=pd.DataFrame(gates); slot=pd.DataFrame(slots)
    csv(out/'all_stock_slot_audit.csv',slot); csv(out/'qualifying_stock_slot_audit.csv',slot.loc[slot.qualifies_2pct])
    csv(out/'all_indicator_checks.csv',gate)
    misses=gate.loc[gate.threshold_result.eq('FAIL')].copy(); misses['absolute_margin']=misses.margin.abs()
    csv(out/'narrowest_misses_by_gate.csv',misses.sort_values(['gate','absolute_margin']))
    print('Testing top-10 counterfactual policies across entire dated universe',flush=True)
    counter=[]; policies={}; outcomes=[]
    structural={'price_data','oi_positive','oi_increasing','confirmation_present','confirmation_range','confirmation_direction','confirmation_beyond_signal','exact_confirmation_clock'}
    for side,top in [('LONG',topup),('SHORT',topdown)]:
      for sym in top.symbol:
       for core,s in pairs:
        if s.side!=side: continue
        part=raw.loc[raw.tradingsymbol.eq(sym)&raw.signal_ts.dt.strftime('%H:%M').eq(s.signal_end)]
        record=dict(symbol=sym,side=side,setup_id=s.setup_id,signal_time=f'{day} {s.signal_end}',confirmation_time=f'{day} {s.confirmation_end}')
        if part.empty: counter.append({**record,'status':'DATA_UNAVAILABLE'}); continue
        r=part.iloc[0]; failed=[(k,v,t,op) for k,v,t,op,stage in checks(r,s) if not passed(v,t,op)]
        record['original_failed_checks']=';'.join(k for k,*_ in failed)
        hard=[k for k,v,t,op in failed if k in structural or not np.isfinite(v)]
        if side=='LONG' and r.price_change_pct<=0 or side=='SHORT' and r.price_change_pct>=0: hard.append('FIVE_MINUTE_DIRECTION_WRONG')
        if hard:
            counter.append({**record,'status':'NO_FEASIBLE_THRESHOLD_ONLY_CHANGE_PRESERVING_DIRECTION_AND_DATA','structural_blockers':';'.join(hard)}); continue
        changes={k:('BYPASS' if k.startswith('ema') or k=='nifty_first_5m_return' else float(v)) for k,v,t,op in failed}
        if any(k.startswith('ema') for k in changes): changes.update(ema9_20='BYPASS',ema20_50='BYPASS')
        policy=dict(setup_id=s.setup_id,changes=changes)
        po,pe=select(raw,pairs,policy)
        target=pe.loc[pe.tradingsymbol.eq(sym)&pe.setup_id.eq(s.setup_id)]
        if target.empty: counter.append({**record,'status':'STILL_BLOCKED_AFTER_RELAXATION','changes':json.dumps(changes)}); continue
        rank=int(target.effective_rank.iloc[0])
        if rank>s.max_entries:
            policy['quota']=rank; po,pe=select(raw,pairs,policy)
        policyid=hashlib.sha256(json.dumps(policy,sort_keys=True).encode()).hexdigest()[:12]
        if policyid not in policies:
            pl=simulate(po,minutes,source); policies[policyid]=policy
            if len(pl): outcomes.append(pl.assign(policy_id=policyid))
        else:
            pl=next(x for x in outcomes if len(x) and x.policy_id.iloc[0]==policyid)
        target=pl.loc[pl.tradingsymbol.eq(sym)&pl.setup_id.eq(s.setup_id)]
        x=target.iloc[0]; added=pl.loc[~pl.apply(lambda z:(z.tradingsymbol,z.setup_id) in orderkeys,axis=1)]
        extreme=pd.Timestamp(top.loc[top.symbol.eq(sym),'peak_time' if side=='LONG' else 'trough_time'].iloc[0])
        record.update(policy_id=policyid,changes=json.dumps(policy,sort_keys=True),effective_rank=rank,
          status='SIMULATED_FILLED' if x.portfolio_executed else str(x.portfolio_status),
          change_count=len([k for k in changes if not k.startswith('ema')])+int('ema9_20' in changes)+int('quota' in policy),
          entry_before_observed_extreme=bool(x.portfolio_executed and pd.Timestamp(x.entry_ts)<extreme),
          full_universe_session_net_pnl=metrics(pl)['net_pnl'],added_losers=int((added.portfolio_executed&added.portfolio_net_profit_rupees.lt(0)).sum()),
          historical_validation='UNVALIDATED_NOT_REPLAYED_OVER_ALL_HISTORY')
        record.update({k:x.get(k) for k in ['entry_ts','entry_price','exit_ts','exit_price','exit_reason','portfolio_net_profit_rupees','portfolio_reject_reason','filled','portfolio_executed']})
        counter.append(record)
    cf=pd.DataFrame(counter); csv(out/'top20_slot_counterfactuals.csv',cf); dump(out/'counterfactual_policies.json',policies)
    if outcomes: csv(out/'counterfactual_universe_trades.csv',pd.concat(outcomes,ignore_index=True))
    live=live_evidence(day,out)
    summary=dict(session=str(day),dated_stock_universe=len(uni),movement_usable=len(usable),g2_feature_symbols=raw.tradingsymbol.nunique(),
      g2_excluded_symbols=sorted(set(uni.equity_symbol)-set(raw.tradingsymbol)),official_close_references=psrc,
      official_session_prices=tsrc,official_prices_status='PUBLISHED_FINAL_BHAVCOPY',
      intraday_status='CONTINUOUS_SESSION_PATHS_SEALED_AUCTION_TIMES_NOT_RECORDED',
      plus2_count=len(up),minus2_count=len(down),both_count=len(set(up.symbol)&set(down.symbol)),
      plus2_pct=100*len(up)/len(usable),minus2_pct=100*len(down)/len(usable),
      smaller_intraday_loss_count=int(usable.official_trough_pct.between(-2,0,inclusive='neither').sum()),
      smaller_closing_loss_count=int(usable.close_return_pct.between(-2,0,inclusive='neither').sum()),
      equal_weight_official_close_return_pct=float(usable.close_return_pct.mean()),
      frozen_g2_metrics=metrics(ledger),recorded_run=str(run),recorded_strategy_policy=result.get('strategy_policy'),
      source_bundle=str(g2.DEFAULT_SOURCE_BUNDLE),g_config_path=str(g2.DEFAULT_G_CONFIG),
      snapshot_root=str(snap),feature_ledger_sha256=g2.sha256(run/'feature_ledger.csv'),
      cached_prev_close_mismatches=int(usable.cached_previous_close_error_rupees.abs().gt(.011).sum()),
      official_high_not_in_minute_path=int((~usable.official_high_matches_observed).sum()),
      official_low_not_in_minute_path=int((~usable.official_low_matches_observed).sum()),
      counterfactual_cases=len(cf),counterfactual_policy_count=len(policies),
      caveats=['Counterfactual thresholds are hindsight-fitted diagnostic boundaries, not recommended live settings.',
               'Minimal means relaxing only failed checks within the threshold/EMA/NIFTY-bypass family, then minimum slot quota needed. It is not a proof of a globally optimal strategy.',
               'All counterfactuals apply to every usable stock in that setup, not a ticker-specific exception.',
               'Per-minute extremes are candle-end labels, not tick timestamps. Entry and extreme within one minute have unknown ordering.',
               'Trade drawdown here is realized closed-trade equity drawdown, not intraday mark-to-market drawdown.',
               'The strategy remains frozen, including 5bps round-trip costs and fractional fixed-exposure sizing. Costs are model costs, not a broker contract-note estimate.'])
    summary['move_statistics']={c:{k:float(getattr(usable[c],k)()) for k in ['mean','median','min','max']} for c in ['official_peak_pct','official_trough_pct','close_return_pct']}
    summary['descriptive_sum_of_peak_moves_pct_not_portfolio_return']=float(usable.official_peak_pct.sum())
    dump(out/'summary.json',summary)
    srcfiles=[Path(__file__),g2.DEFAULT_G_CONFIG,Path(g2.__file__),Path(g2.g.__file__),Path(replay.__file__),Path(hybrid.__file__),run/'source_manifest.json',snap/'snapshot_manifest.json',run/'feature_ledger.csv']
    dump(out/'provenance.json',dict(generated_at=datetime.now().isoformat(),input_hashes={str(p):g2.sha256(p) for p in srcfiles},frozen_bundle_verified=True,snapshot_verified=True,independent_native_selection_parity=True))
    report(out,summary,moves,up,down,topup,topdown,slot,gate,cf,ledger)
    print(json.dumps(summary,indent=2,default=str),flush=True)
    print('OUTPUT',out,flush=True)

def report(out,s,moves,up,down,topup,topdown,slots,gates,cf,ledger):
    parts=['<!doctype html><html><head><meta charset="utf-8"><title>Frozen G2 audit '+s['session']+'</title><style>body{font:15px Arial;margin:30px;color:#17212b}h1,h2{color:#173857}table{border-collapse:collapse;font-size:13px;margin:15px 0}th,td{padding:7px;border-bottom:1px solid #ddd;text-align:right}th{background:#edf2f7;position:sticky;top:0}td:first-child{text-align:left}.scroll{overflow:auto}summary{cursor:pointer;padding:12px;background:#edf2f7}pre{white-space:pre-wrap}a{color:#0757a1}</style></head><body>',f'<h1>Frozen V13-V10-G-2 audit — {s["session"]} IST</h1>']
    def para(t): parts.append('<p>'+html.escape(t)+'</p>')
    def table(title,frame): parts.append('<h2>'+html.escape(title)+'</h2><div class="scroll">'+frame.to_html(index=False,na_rep='Unavailable',float_format=lambda x:f'{x:,.4f}')+'</div>')
    para('Standard frozen G selections, staged stop 1.25% tightened to 1.00% after 120 minutes; all other execution and cost rules unchanged. The recorded daily replay used a different promoted configuration and is not silently treated as the baseline.')
    para(f'Dated stocks: {s["dated_stock_universe"]}; movement usable: {s["movement_usable"]}; strategy feature usable: {s["g2_feature_symbols"]}. Data exclusions: {s["g2_excluded_symbols"]}. Official final bhavcopy is published; intraday price paths cover continuous trading, not auction tick timing.')
    para(f'At least +2%: {s["plus2_count"]} ({s["plus2_pct"]:.2f}%); at most -2%: {s["minus2_count"]} ({s["minus2_pct"]:.2f}%). Intraday trough strictly between -2% and 0%: {s["smaller_intraday_loss_count"]}. Closing loss strictly between -2% and 0%: {s["smaller_closing_loss_count"]}. Equal-weight official close-to-close return: {s["equal_weight_official_close_return_pct"]:.4f}%.')
    para('Movement denominator is the prior trading day official NSE EQ close, never cached last minute. Whole-session extrema use official daily high/low. Intraday time is supplied only where a valid minute bar matches that official extreme. Intraday-only extrema and both first/last occurrences are retained in universe_movements.csv.')
    para(f'Cached previous close mismatches: {s["cached_prev_close_mismatches"]}. Official high absent from minute path: {s["official_high_not_in_minute_path"]}; official low absent: {s["official_low_not_in_minute_path"]}. No timestamp is invented for these cases.')
    table('Move statistics (percentage points)',pd.DataFrame(s['move_statistics']).T.reset_index())
    table('Frozen baseline results',pd.DataFrame([s['frozen_g2_metrics']]))
    if len(ledger): table('Frozen baseline simulated trades',ledger[[c for c in ['tradingsymbol','side','setup_id','entry_ts','entry_price','exit_ts','exit_price','exit_reason','portfolio_net_profit_rupees'] if c in ledger]])
    cols=['symbol','official_previous_close','official_peak_pct','official_peak_time','official_trough_pct','official_trough_time','close_return_pct','valid_minutes','valid_5m_bars']
    for title,frame in [('Top 10 LONG opportunity rankings (hindsight)',topup),('Top 10 SHORT opportunity rankings (hindsight)',topdown),('All stocks reaching +2% or higher',up),('All stocks reaching -2% or lower',down)]: table(title,frame[cols])
    parts.append('<h2>Slot-by-slot audit for every qualifying stock</h2>')
    para('PASS/FAIL is reconstructed from sealed raw features using the frozen baseline, not a claim that the live scanner recorded the decision. Subsequent gates can be calculated diagnostically even when not reached. Full per-indicator values, operators, margins, clocks and evidence CSV row numbers are in all_indicator_checks.csv. Positive margin passes except strict > checks, where zero still fails. Margins of different units must not be compared.')
    for symbol in sorted(set(up.symbol)|set(down.symbol)):
        x=slots.loc[slots.symbol.eq(symbol)]
        parts.append('<details><summary>'+html.escape(symbol)+'</summary><div class="scroll">'+x.to_html(index=False,na_rep='Not reached',float_format=lambda x:f'{x:.4f}')+'</div></details>')
    parts.append('<h2>Top-20 counterfactuals</h2>')
    para('Each feasible change set was applied to the entire usable universe in that setup. Existing native core priority is retained. If required, the slot quota is increased to the target’s causal rank, admitting all higher-ranked stocks as well. Directional confirmation and positive/increasing OI remain required. Failed structural conditions are not fabricated away. Policies are diagnostic, hindsight-calibrated and unvalidated over the full historical dataset; no change is approved for promotion.')
    for symbol in sorted(set(topup.symbol)|set(topdown.symbol)):
        parts.append('<details><summary>'+html.escape(symbol)+'</summary><div class="scroll">'+cf.loc[cf.symbol.eq(symbol)].to_html(index=False,na_rep='Unavailable',float_format=lambda x:f'{x:,.4f}')+'</div></details>')
    parts.append('<h2>Conclusions and limits</h2>')
    para('No new threshold is recommended as historically validated by this session-only forensic replay. Prioritize official-close/reference consistency and coverage provenance first. Only then compare simple, prespecified universe-wide changes on all history and an untouched forward sample. Thresholds fitted to these winners can admit added losing trades, and hitting +2%/-2% before a signal is not a capturable trade.')
    for c in s['caveats']: para(c)
    parts.append('<h2>Exact evidence and research files</h2><ul>')
    for p in sorted(out.glob('*')):
        if p.is_file() and p.name!='report.html': parts.append(f'<li><a href="{html.escape(p.name)}">{html.escape(p.name)}</a></li>')
    parts.append('</ul>'); para('Recorded run: '+s['recorded_run']); para('Frozen source bundle: '+s['source_bundle']); para('Input snapshot: '+s['snapshot_root'])
    for key in ['official_close_references','official_session_prices']:
        url=s[key]['url']; parts.append(f'<p><a href="{html.escape(url)}">Official NSE source — {key}</a></p>')
    parts.append('<p><a href="https://www.nseindia.com/static/products-services/closing-auction-session">NSE closing auction session rules</a></p></body></html>')
    (out/'report.html').write_text('\n'.join(parts),encoding='utf-8')

if __name__=='__main__':
    p=argparse.ArgumentParser(); p.add_argument('--date',type=date.fromisoformat,required=True);p.add_argument('--output',type=Path)
    a=p.parse_args();run(a.date,a.output or ROOT/f'{a.date}_audit_{datetime.now():%H%M%S}')
