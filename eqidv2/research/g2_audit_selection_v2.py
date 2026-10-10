"""Read-only frozen G2 selection audit and isolated retrospective policies.

No ticker-specific strategy, production write, live order, or baseline mutation.
Policies are causal in feature use but hindsight fitted, not validated proposals.
"""
from __future__ import annotations
import argparse, hashlib, json, sys
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from research import g2_session_forensic_audit as old
g2, ext, replay, hybrid = old.g2, old.ext, old.replay, old.hybrid
IST = 'Asia/Kolkata'
DEFAULT_RUN = Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g\runs\2026-10-09\20261009T225259748185')
UNITS = {'price_data':'boolean', 'oi_positive':'provider_OI_units', 'oi_increasing':'provider_OI_units',
 'oi_base_min':'percentage_points', 'oi_max':'percentage_points', 'volume_base':'ratio',
 'ema9_20':'rupees', 'ema20_50':'rupees', 'price_base':'percentage_points', 'setup_price':'percentage_points',
 'setup_oi':'percentage_points', 'setup_volume':'ratio', 'setup_liquidity':'rupees',
 'confirmation_present':'boolean', 'confirmation_range':'rupees', 'confirmation_direction':'rupees',
 'confirmation_beyond_signal':'rupees', 'setup_body':'fraction', 'setup_wick':'fraction',
 'confirmation_volume':'ratio', 'exact_confirmation_clock':'seconds', 'nifty_first_5m_return':'percentage_points'}
STRUCTURAL = {'price_data','oi_positive','oi_increasing','confirmation_present','confirmation_range',
              'confirmation_direction','confirmation_beyond_signal','exact_confirmation_clock'}
EMPTY_TRADES = ['tradingsymbol','side','setup_id','signal_ts','confirmation_ts','entry_ts','entry_price','exit_ts','exit_price',
                'filled','portfolio_executed','portfolio_status','portfolio_reject_reason','portfolio_net_profit_rupees']

def dump(p, x):
    def clean(v):
        if isinstance(v, dict): return {str(k):clean(a) for k,a in v.items()}
        if isinstance(v, (list,tuple)): return [clean(a) for a in v]
        if isinstance(v, (float,np.floating)) and not np.isfinite(v): return None
        if isinstance(v, np.generic): return v.item()
        return v
    p.write_text(json.dumps(clean(x), indent=2, default=str, allow_nan=False), encoding='utf-8')

def csv(p,x): x.to_csv(p,index=False)
def keys(x): return sorted(zip(x.tradingsymbol.astype(str),x.setup_id.astype(str),x.signal_ts.astype(str)))
def checks(r,s): return old.checks(r,s)

def select(raw,pairs,policy=None):
    """Universe-wide changed setup; retain ORIGINAL F-core priority reservation.

    Original core candidates are not redefined by a lower expanded threshold.
    This explicitly corrects the legacy forensic helper's policy/core coupling.
    """
    policy=policy or {}; orders=[]; ranks=[]
    for core,s in pairs:
        part=raw.loc[raw.signal_ts.dt.strftime('%H:%M').eq(s.signal_end)]
        changes=policy.get('changes',{}) if policy.get('setup_id')==s.setup_id else {}
        records=[]
        for _,r in part.iterrows():
            if not all(changes.get(k)=='BYPASS' or old.passed(v,changes.get(k,t),op) for k,v,t,op,_ in checks(r,s)): continue
            rr=r.to_dict()
            rr.update(side=s.side,setup_id=s.setup_id,picker=s.picker,max_entries=s.max_entries,
                abs_price_change_pct=abs(r.price_change_pct),trigger=r.confirmation_high if s.side=='LONG' else r.confirmation_low,
                wick_ratio=r.v9_1m_upper_wick_ratio if s.side=='LONG' else r.v9_1m_lower_wick_ratio,
                core_eligible=all(old.passed(v,t,op) for k,v,t,op,_ in checks(r,core)))
            records.append(rr)
        if not records: continue
        ranked=pd.DataFrame(records)
        ranked=ranked.sort_values(['day',g2.g.v9.PICKER_COLUMNS[s.picker],'traded_value','tradingsymbol'],
                                  ascending=[True,False,False,True],kind='stable')
        core_ids=ranked.loc[ranked.core_eligible].groupby('day',sort=False).head(core.max_entries).sid
        ranked['core_reserved']=ranked.sid.isin(core_ids)
        ranked=ranked.sort_values(['day','core_reserved'],ascending=[True,False],kind='stable')
        ranked['effective_rank']=ranked.groupby('day',sort=False).cumcount()+1
        quota=policy.get('quota',s.max_entries) if policy.get('setup_id')==s.setup_id else s.max_entries
        ranked['selected']=ranked.effective_rank.le(quota)
        ranks.append(ranked); orders.append(ranked.loc[ranked.selected])
    template=raw.iloc[:0].copy()
    for c in ['side','setup_id','picker','trigger','wick_ratio','effective_rank','selected']: template[c]=pd.Series(dtype='object')
    return (pd.concat(orders,ignore_index=True) if orders else template,
            pd.concat(ranks,ignore_index=True) if ranks else template)

def before_extreme(entry,extreme,interval_start=None,interval_end=None):
    a=pd.to_datetime(entry,utc=True,errors='coerce'); b=pd.to_datetime(extreme,utc=True,errors='coerce')
    if pd.isna(a): return 'UNKNOWN_ENTRY_TIME'
    if pd.isna(b):
        lo=pd.to_datetime(interval_start,utc=True,errors='coerce')
        hi=pd.to_datetime(interval_end,utc=True,errors='coerce')
        # The execution timestamp is the latest bound of its native entry bar.
        # A full minute separates an after-interval assertion conservatively.
        if pd.notna(lo) and a<lo: return 'YES_BEFORE_EXTREME_INTERVAL'
        if pd.notna(hi) and a-pd.Timedelta(minutes=1)>hi: return 'NO_AFTER_EXTREME_INTERVAL'
        if pd.notna(lo) and pd.notna(hi): return 'UNKNOWN_WITHIN_EXTREME_INTERVAL'
        return 'UNKNOWN_EXTREME_TIME'
    if a==b: return 'UNKNOWN_WITHIN_SAME_MINUTE'
    return 'YES' if a<b else 'NO'

def simulate(orders,minutes,source):
    """Native fills/exits and portfolio, including an all-unfilled ledger schema."""
    if orders.empty: return pd.DataFrame(columns=EMPTY_TRADES)
    paths={}
    for r in orders.itertuples():
        m=minutes[r.tradingsymbol]
        path=m.loc[m.ts.gt(r.confirmation_ts)&m.ts.le(replay._cutoff(r.day))]
        paths[int(r.sid)]={'timestamp_ns':path.ts.astype('int64').to_numpy(),
                          **{k:path[k].to_numpy(float) for k in ('open','high','low','close')}}
    work=ext._apply_retained_g_exits(orders,source)
    trades=g2.simulate_staged(work,paths,cost_bps=source['cost_bps'],max_entry_delay_minutes=10)
    for col,default in [('filled',False),('entry_ts',pd.NaT),('exit_ts',pd.NaT),
                        ('net_return_pct',np.nan),('gross_return_pct',np.nan),('cost_pct',np.nan)]:
        if col not in trades: trades[col]=default
    base=ext._portfolio_config(source)
    trades=g2.g.v9.v5.apply_fixed_capital_model(trades,base.capital_per_entry_rupees,base.leverage_factor)
    return g2.g.v9.v6.apply_portfolio_constraints(trades,base.portfolio_config())[0]

def self_tests():
    assert not old.passed(0.,0.,'>')
    assert not old.passed(np.nan,1.,'>=')
    assert before_extreme('2026-10-09 10:00+05:30','unknown')=='UNKNOWN_EXTREME_TIME'
    assert before_extreme('2026-10-09 10:00+05:30','2026-10-09 10:00+05:30')=='UNKNOWN_WITHIN_SAME_MINUTE'
    assert before_extreme('2026-10-09 09:57+05:30',None,'2026-10-09 15:28+05:30','2026-10-09 15:35+05:30')=='YES_BEFORE_EXTREME_INTERVAL'
    assert old.metrics(pd.DataFrame())['trades']==0

def prepare(run,out):
    print('Verifying sealed baseline and dated source',flush=True)
    self_tests(); bundle=g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE,g2.DEFAULT_G_CONFIG)
    source=bundle['source_g']; manifest=g2.read_json(run/'source_manifest.json')
    snapshot=Path(manifest['input_snapshot']['root']); replay._verify_input_snapshot(snapshot,g2.read_json(snapshot/'snapshot_manifest.json'))
    fhash=g2.sha256(run/'feature_ledger.csv')
    assert fhash==g2.read_json(run/'feature_ledger.csv.manifest.json')['artifact_sha256']
    raw=old.normalize(pd.read_csv(run/'feature_ledger.csv')); raw['evidence_csv_row']=raw.index+2
    assert not raw.duplicated(['tradingsymbol','signal_ts']).any()
    day=date.fromisoformat(manifest['session_date'])
    universe=pd.read_parquet(snapshot/'universe'/f'near_month_{day}.parquet')
    universe=universe.loc[~universe.underlying.isin(ext.common.INDEX_UNDERLYINGS)].copy()
    assert universe.equity_symbol.is_unique
    change=g2.g.SelectionChange(**source['selection_change'])
    pairs=[g2.g.setup_pair(s,change) for s in g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES['higher_frequency'])]
    assert not source.get('morning_slots',False) and not source.get('two_bar_continuation',False)
    assert not bundle['v9_config'].uses_features
    native_strict=replay._strict_signals(raw,float(raw.nifty_first_bar_return_pct.dropna().iloc[0]))
    native=g2.g.select_orders(native_strict,bundle['v9_config'],change,core_first=source['core_first'])
    orders,eligible=select(raw,pairs)
    assert keys(native)==keys(orders),'Native/independent baseline selection drift'
    # Exercise nonempty selector parity too: synthetic, uniform causal candidates,
    # with multiple symbols and both core/expanded SHORT boundaries.
    synthetic=raw.loc[raw.signal_ts.dt.strftime('%H:%M').eq('09:40')].head(5).copy()
    for col,val in dict(oi=110.,prev_oi=100.,oi_change_pct=.3,volume_ratio=2.,price_change_pct=-.3,
        ema9=90.,ema20=100.,ema50=110.,confirmation_open=101.,confirmation_close=99.,signal_close=100.,
        confirmation_high=101.,confirmation_low=98.,body_ratio=2/3,v9_1m_volume_ratio=2.,
        v9_1m_upper_wick_ratio=0.,v9_1m_lower_wick_ratio=1/3,traded_value=1e8).items(): synthetic[col]=val
    synthetic['v9_5m_ema_bear']=True; synthetic['v9_5m_ema_bull']=False
    sn=replay._strict_signals(synthetic,float(raw.nifty_first_bar_return_pct.dropna().iloc[0]))
    expected=g2.g.select_orders(sn,bundle['v9_config'],change,core_first=True)
    got,_=select(synthetic,pairs)
    assert len(expected)>0 and keys(expected)==keys(got),'Nonempty selector parity drift'
    csv(out/'native_strict_signals.csv',native_strict); csv(out/'frozen_selected_orders.csv',orders)
    csv(out/'frozen_eligible_ranking.csv',eligible); csv(out/'frozen_portfolio_trades.csv',pd.DataFrame(columns=EMPTY_TRADES))
    assert orders.empty, 'This session audit requires explicit baseline simulation if baseline orders exist'
    csv(out/'dated_stock_universe.csv',universe); csv(out/'raw_slot_features.csv',raw)
    dump(out/'frozen_g2_config.json',g2.config(source)); dump(out/'setup_thresholds.json',[dict(core=asdict(c),expanded=asdict(s)) for c,s in pairs])
    dump(out/'portfolio_configuration.json',asdict(bundle['v9_config'].portfolio_config()))
    provenance=dict(generated_at=datetime.now().isoformat(),session=str(day),feature_source_run=str(run),
      source_run_policy=g2.read_json(run/'replay_result.json')['strategy_policy'],
      baseline_policy='ORIGINAL_FROZEN_G2_NO_RELAXED_0925_NO_TICKER_EXCEPTIONS',
      source_bundle=str(g2.DEFAULT_SOURCE_BUNDLE),g_config=str(g2.DEFAULT_G_CONFIG),snapshot=str(snapshot),
      sealed_bundle_verified=True,snapshot_verified=True,native_independent_empty_and_nonempty_selection_parity=True,
      code_hashes={str(Path(p)):g2.sha256(Path(p)) for p in [__file__,old.__file__,g2.__file__,g2.g.__file__,replay.__file__,g2.DEFAULT_G_CONFIG]},
      input_hashes={str(p):g2.sha256(p) for p in [run/'source_manifest.json',run/'feature_ledger.csv',snapshot/'snapshot_manifest.json',g2.DEFAULT_SOURCE_BUNDLE/'bundle_manifest.json']})
    dump(out/'provenance.json',provenance)
    return bundle,source,raw,universe,pairs,snapshot,day,provenance

def audit(raw,universe,pairs,run,out,day):
    gates=[]; slots=[]; fhash=g2.sha256(run/'feature_ledger.csv')
    live_root=old.LIVE/'scanner_5m'/str(day)
    live={p.stem.replace('slot_',''):g2.read_json(p) for p in live_root.glob('slot_*.json')}
    for sym in universe.equity_symbol:
      for core,s in pairs:
        part=raw.loc[raw.tradingsymbol.eq(sym)&raw.signal_ts.dt.strftime('%H:%M').eq(s.signal_end)]
        base=dict(symbol=sym,setup_id=s.setup_id,side=s.side,signal_time=f'{day} {s.signal_end}+05:30',
          confirmation_time=f'{day} {s.confirmation_end}+05:30',decision_evidence='RECONSTRUCTED_FROZEN_G2_NOT_LIVE_RECORDED',
          live_promoted_scanner_slot_state=live.get(s.signal_end.replace(':',''),{}).get('state','NO_RECORDED_SCANNER_SNAPSHOT'),
          slot_quota=s.max_entries,picker=s.picker,symbol_day_quota='NOT_CONFIGURED',max_positions='NOT_CONFIGURED',
          max_positions_per_symbol='NOT_CONFIGURED',broker_submission='NOT_APPLICABLE_RESEARCH_NO_LIVE_ORDER_AUTHORITY')
        if part.empty:
            slots.append({**base,'selection_status':'DATA_UNAVAILABLE','failures':'NO_FEATURE_ROW',
                'ranking_guard':'NOT_REACHED','capital_guard':'NOT_REACHED','entry_trigger':'NOT_REACHED'}); continue
        r=part.iloc[0]; ck=checks(r,s); failures=[k for k,v,t,op,stage in ck if not old.passed(v,t,op)]
        pre=all(old.passed(v,t,op) for k,v,t,op,stage in ck if stage in ('DATA','5M'))
        finite=all(np.isfinite(v) for k,v,t,op,stage in ck)
        raw_available=all(np.isfinite(r.get(k,np.nan)) for k in ['open','high','low','close','volume','oi','prev_oi',
            'ema9','ema20','ema50','price_change_pct','oi_change_pct','volume_ratio','traded_value',
            'confirmation_open','confirmation_high','confirmation_low','confirmation_close','confirmation_volume','v9_1m_volume_ratio'])
        zero_range=bool(r.v9_exact_confirmation_present and np.isfinite(r.confirmation_high)
                        and np.isfinite(r.confirmation_low) and r.confirmation_high==r.confirmation_low)
        for k,v,t,op,stage in ck:
            ok=old.passed(v,t,op); missing=not np.isfinite(v)
            undefined=missing and zero_range and k in ('setup_body','setup_wick')
            gates.append({**base,'gate':k,'stage':stage,'observed':v,'required':t,'operator':op,
                'margin':t-v if op=='<=' else v-t,'unit':UNITS[k],'strict_zero_margin_fails':op=='>',
                'threshold_result':'UNDEFINED_ZERO_RANGE' if undefined else 'DATA_UNAVAILABLE' if missing else 'PASS' if ok else 'FAIL',
                'raw_data_available':raw_available,'undefined_reason':'CONFIRMATION_HIGH_EQUALS_LOW' if undefined else '',
                'pipeline_evaluation':'NOT_REACHED_DIAGNOSTIC_ONLY' if stage=='1M' and not pre else 'RECONSTRUCTED',
                'decision_time':str(r.confirmation_ts if stage=='1M' else r.signal_ts),
                'evidence_source':str(run/'feature_ledger.csv'),'evidence_csv_row':int(r.evidence_csv_row),
                'evidence_file_sha256':fhash,'ledger_row_sha256':r.ledger_row_sha256,'input_slice_sha256':r.input_slice_sha256})
        slots.append({**base,'selection_status':'FILTER_FAILURE' if raw_available else 'DATA_UNAVAILABLE',
          'failures':';'.join(failures),'first_diagnostic_failed_gate':failures[0] if failures else '',
          'failed_gate_count':len(failures),'data_usable':raw_available,'raw_data_available':raw_available,
          'all_derived_values_defined':finite,'confirmation_zero_range':zero_range,
          'data_availability':'RAW_PRESENT_DERIVED_RATIO_UNDEFINED' if raw_available and not finite else 'AVAILABLE' if raw_available else 'MISSING_OR_NONFINITE_RAW_INPUT',
          'price_oi_usable':all(old.passed(v,t,op) for k,v,t,op,stage in ck if stage=='DATA'),
          'five_minute_pass':pre,'confirmation_present':bool(r.v9_exact_confirmation_present),'confirmation_reached':pre,
          'confirmation_pass_diagnostic':all(old.passed(v,t,op) for k,v,t,op,stage in ck if stage=='1M'),
          'ranking_guard':'NOT_REACHED','slot_capacity_guard':'NOT_REACHED','capital_guard':'NOT_REACHED',
          'entry_trigger':'NOT_REACHED','entry_expiry':'NOT_REACHED','selected':False,'filled':False,
          'evidence_source':str(run/'feature_ledger.csv'),'evidence_csv_row':int(r.evidence_csv_row),'evidence_file_sha256':fhash})
    slot=pd.DataFrame(slots); gate=pd.DataFrame(gates)
    assert slot.failed_gate_count.min()>0
    csv(out/'all_stock_slot_audit.csv',slot); csv(out/'all_indicator_checks.csv',gate)
    failed=gate.loc[gate.threshold_result.eq('FAIL')].copy(); failed['absolute_margin']=failed.margin.abs()
    csv(out/'narrowest_misses_by_gate.csv',failed.sort_values(['gate','absolute_margin','symbol']))
    single=slot.loc[slot.failed_gate_count.eq(1),['symbol','setup_id']]
    csv(out/'single_blocker_misses.csv',failed.merge(single,on=['symbol','setup_id']).sort_values(['gate','absolute_margin']))
    csv(out/'single_blocker_candidates.csv',failed.merge(single,on=['symbol','setup_id']).sort_values(['gate','absolute_margin']))
    csv(out/'stage_counts_by_setup.csv',slot.groupby(['setup_id','side']).agg(stock_setup_rows=('symbol','size'),
       data_usable=('data_usable','sum'),five_minute_pass=('five_minute_pass','sum'),confirmation_reached=('confirmation_reached','sum'),
       confirmation_pass_diagnostic=('confirmation_pass_diagnostic','sum'),selected=('selected','sum'),filled=('filled','sum')).reset_index())
    summary=dict(session=str(day),dated_stocks=len(universe),feature_symbols=raw.tradingsymbol.nunique(),feature_rows=len(raw),
      setups=len(pairs),stock_setup_cases=len(slot),indicator_checks=len(gate),baseline_metrics=old.metrics(pd.DataFrame()),
      selected_orders=0,all_checks_failed_rows=len(slot),single_blocker_cases=len(single),
      raw_data_unavailable_cases=int((~slot.raw_data_available).sum()),
      present_confirmation_zero_range_cases=int(slot.confirmation_zero_range.sum()),
      live_scanner_slots={k:v.get('state') for k,v in live.items()},
      note='Reconstructed filter decisions from finalized-source features are distinct from recorded live promoted-G scanner decisions.')
    dump(out/'audit_summary.json',summary); print(json.dumps(summary),flush=True)
    dump(out/'summary.json',summary)
    return slot,gate

def counterfactuals(raw,pairs,source,snapshot,day,out,movement,slot):
    topup=pd.read_csv(movement/'top10_long.csv'); topdown=pd.read_csv(movement/'top10_short.csv')
    allmoves=pd.read_csv(movement/'universe_movements.csv')
    qualifiers=set(pd.read_csv(movement/'gainers_ge_2pct.csv').symbol)|set(pd.read_csv(movement/'decliners_le_minus2pct.csv').symbol)
    csv(out/'qualifying_stock_slot_audit.csv',slot.loc[slot.symbol.isin(qualifiers)])
    minutes={}; policies={}; outcomes={}; counts={}; counter=[]
    def simulate_policy(p):
        pid=hashlib.sha256(json.dumps(p,sort_keys=True).encode()).hexdigest()[:16]
        if pid in outcomes: return pid,outcomes[pid],counts[pid]
        po,pe=select(raw,pairs,p)
        for sym in po.tradingsymbol.unique():
            if sym not in minutes: minutes[sym]=old.minute_day(snapshot/'equity_1m'/f'{sym}_stocks_indicators_1min.parquet',day)
        ledger=simulate(po,minutes,source)
        policies[pid]=p; outcomes[pid]=ledger; counts[pid]=dict(orders=len(po),eligible=len(pe),**old.metrics(ledger))
        csv(out/f'policy_{pid}_selected_orders.csv',po); csv(out/f'policy_{pid}_eligible_ranking.csv',pe)
        return pid,ledger,counts[pid]
    for side,top in [('LONG',topup),('SHORT',topdown)]:
      for sym in top.symbol:
       for core,s in pairs:
        if s.side!=side: continue
        row=raw.loc[raw.tradingsymbol.eq(sym)&raw.signal_ts.dt.strftime('%H:%M').eq(s.signal_end)]
        base=dict(symbol=sym,side=side,setup_id=s.setup_id,signal_time=f'{day} {s.signal_end}+05:30',
                  confirmation_time=f'{day} {s.confirmation_end}+05:30',frozen_slot_quota=s.max_entries,
                  validation='UNVALIDATED_HINDSIGHT_BOUNDARY_NOT_FULL_HISTORY',
                  evidence='RECONSTRUCTED_ALL_UNIVERSE_SESSION_POLICY')
        if row.empty: counter.append({**base,'status':'DATA_UNAVAILABLE'}); continue
        r=row.iloc[0]; failed=[(k,v,t,op) for k,v,t,op,stage in checks(r,s) if not old.passed(v,t,op)]
        base['original_failed_checks']=';'.join(k for k,*_ in failed)
        hard=[k for k,v,t,op in failed if k in STRUCTURAL or not np.isfinite(v)]
        if side=='LONG' and r.price_change_pct<=0 or side=='SHORT' and r.price_change_pct>=0: hard.append('FIVE_MINUTE_DIRECTION_WRONG')
        if hard:
            counter.append({**base,'status':'NOT_POSSIBLE_WITHIN_TESTED_FAMILIES','structural_blockers':';'.join(hard)}); continue
        emas=[k for k,v,t,op in failed if k.startswith('ema')]
        if emas:
            counter.append({**base,'family':'NUMERIC_THRESHOLDS_EMA_PRESERVED','status':'NOT_POSSIBLE_WITHIN_TESTED_FAMILY','structural_blockers':';'.join(emas)})
        changes={k:('BYPASS' if k.startswith('ema') else float(v)) for k,v,t,op in failed}
        family='NUMERIC_PLUS_FAILED_EMA_COMPARISON_BYPASS' if emas else 'NUMERIC_THRESHOLDS_EMA_PRESERVED'
        policy=dict(setup_id=s.setup_id,changes=changes,family=family,core_priority='ORIGINAL_FROZEN_CORE',
                    scope='ALL_STOCKS_AT_THIS_SETUP',causal_features_only=True)
        po,pe=select(raw,pairs,policy)
        target=pe.loc[pe.tradingsymbol.eq(sym)&pe.setup_id.eq(s.setup_id)]
        if target.empty: counter.append({**base,'family':family,'status':'STILL_FILTER_BLOCKED','changes':json.dumps(changes)}); continue
        rank=int(target.effective_rank.iloc[0]); pid,ledger,met=simulate_policy(policy)
        trial=[('UNCHANGED_QUOTA',pid,ledger,met,policy)]
        if rank>s.max_entries:
            expanded={**policy,'quota':rank}; qid,ql,qm=simulate_policy(expanded)
            trial.append(('MINIMUM_QUOTA_FOR_CAUSAL_RANK',qid,ql,qm,expanded))
        for variant,pid,ledger,met,p in trial:
            x=ledger.loc[ledger.tradingsymbol.eq(sym)&ledger.setup_id.eq(s.setup_id)] if len(ledger) else ledger
            record={**base,'family':family,'quota_variant':variant,'policy_id':pid,'changes':json.dumps(p,sort_keys=True),
              'effective_rank':rank,'effective_quota':p.get('quota',s.max_entries),
              'changed_checks':len(changes)+int('quota' in p),
              'changed_independent_conditions':len(set({'oi_base_min':'oi_min','setup_oi':'oi_min',
                  'volume_base':'five_minute_volume','setup_volume':'five_minute_volume',
                  'price_base':'five_minute_price','setup_price':'five_minute_price',
                  'ema9_20':'ema_alignment','ema20_50':'ema_alignment'}.get(k,k) for k in changes))+int('quota' in p),
              'session_all_universe_orders':met['orders'],'session_all_universe_trades':met['trades'],
              'session_all_universe_net_pnl':met['net_pnl'],'session_added_losers':met['losses'],
              'confirmation_volume_ratio':r.v9_1m_volume_ratio,
              'margin_vector_by_unit':json.dumps([dict(gate=k,from_value=t,to_value=v,change=v-t,unit=UNITS[k]) for k,v,t,op in failed])}
            if x.empty: record['status']='FILTER_PASS_NOT_SELECTED_RANK_CAPACITY'
            else:
                trade=x.iloc[0]
                record['status']='SIMULATED_FILLED' if trade.portfolio_executed else 'SIMULATED_'+str(trade.portfolio_status)
                record.update({k:trade.get(k) for k in EMPTY_TRADES if k not in ('tradingsymbol','side','setup_id','signal_ts','confirmation_ts')})
                record.update({k:trade.get(k) for k in ['exit_reason','entry_path_index','entry_bar_end_ts','trigger','native_target_pct','mfe_pct','mae_pct','cost_rupees']})
                mv=top.loc[top.symbol.eq(sym)].iloc[0]
                # Movement agent explicitly names full-session extrema; never silently
                # substitute an observed 15:15 extreme for an unobserved EOD extreme.
                clock='official_peak_time' if side=='LONG' else 'official_trough_time'
                if clock not in mv: clock='peak_time' if side=='LONG' else 'trough_time'
                prefix='peak' if side=='LONG' else 'trough'
                record['measured_extreme_time']=mv.get(clock)
                record['measured_extreme_time_precision']=mv.get(prefix+'_time_precision')
                record['measured_extreme_interval_start']=mv.get(prefix+'_time_interval_start')
                record['measured_extreme_interval_end']=mv.get(prefix+'_time_interval_end')
                record['entry_before_measured_extreme']=before_extreme(trade.entry_ts,mv.get(clock),
                    mv.get(prefix+'_time_interval_start'),mv.get(prefix+'_time_interval_end')) if trade.portfolio_executed else 'NOT_ENTERED'
            counter.append(record)
        print('Counterfactual',sym,s.setup_id,rank,len(trial),'policies',len(policies),flush=True)
    cf=pd.DataFrame(counter); csv(out/'top20_slot_counterfactuals.csv',cf)
    dump(out/'counterfactual_policies.json',policies)
    csv(out/'counterfactual_policy_session_metrics.csv',pd.DataFrame([dict(policy_id=k,**v) for k,v in counts.items()]))
    csv(out/'counterfactual_session_policy_comparison.csv',pd.DataFrame([dict(policy_id=k,baseline_trades=0,baseline_net_pnl=0,**v) for k,v in counts.items()]))
    frames=[v.assign(policy_id=k) for k,v in outcomes.items() if len(v)]
    csv(out/'counterfactual_universe_trades.csv',pd.concat(frames,ignore_index=True) if frames else pd.DataFrame(columns=['policy_id',*EMPTY_TRADES]))
    filled=cf.loc[cf.status.eq('SIMULATED_FILLED')].copy()
    if len(filled):
        filled['entry_sort']=pd.to_datetime(filled.entry_ts,utc=True)
        earliest=filled.sort_values(['symbol','side','entry_sort','changed_independent_conditions','policy_id']).drop_duplicates(['symbol','side']).drop(columns='entry_sort')
    else: earliest=cf.iloc[:0]
    csv(out/'earliest_counterfactual_entries.csv',earliest)
    csv(out/'top20_earliest_counterfactual_entries.csv',earliest)
    summary=dict(top_long=topup.symbol.tolist(),top_short=topdown.symbol.tolist(),counterfactual_cases=len(cf),
      policies=len(policies),status_counts=cf.status.value_counts().to_dict(),earliest_filled_symbols=earliest.symbol.tolist(),
      methodology='Minimize changed failed conditions within direction/data-preserving family; EMA bypass separate. Numeric boundaries set at observed values. Test unchanged quota first, then minimum required causal rank. No cross-unit global minimum claim.',
      baseline=dict(trades=0,net_pnl=0),historical_validation='UNVALIDATED_PENDING_FULL_RAW_HISTORY_REPLAY',
      cautions=['No future extremes or P&L are used in ranking or policy simulation; choice of targets/thresholds is hindsight fitted.',
                'Policies are independent experiments and their P&Ls cannot be added.',
                'Bar-end fill timestamps use native OHLC model; within-minute sequencing unknown.',
                'Native 5 bps round-trip costs, fractional 5x exposure sizing, 10-minute trigger expiry, staged stops, targets and 15:15 squareoff unchanged.',
                'No symbol/day quota or concurrent symbol cap is configured; capital cap remains Rs1,000,000.'])
    dump(out/'counterfactual_summary.json',summary); print(json.dumps(summary),flush=True)
    combined=g2.read_json(out/'audit_summary.json'); combined['counterfactuals']=summary; dump(out/'summary.json',combined)

def main():
    p=argparse.ArgumentParser(); p.add_argument('--run',type=Path,default=DEFAULT_RUN); p.add_argument('--output',type=Path,required=True)
    p.add_argument('--movement',type=Path); p.add_argument('--counterfactual-only',action='store_true'); a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=True)
    bundle,source,raw,uni,pairs,snap,day,prov=prepare(a.run,a.output)
    slot,gate=audit(raw,uni,pairs,a.run,a.output,day)
    if a.movement:
        counterfactuals(raw,pairs,source,snap,day,a.output,a.movement,slot)
        for name in ('top10_long.csv','top10_short.csv','universe_movements.csv','gainers_ge_2pct.csv','decliners_le_minus2pct.csv'):
            path=a.movement/name; prov['input_hashes'][str(path)]=g2.sha256(path)
    old.live_evidence(day,a.output)
    prov['code_unchanged_after_run']=all(g2.sha256(Path(k))==v for k,v in prov['code_hashes'].items())
    assert prov['code_unchanged_after_run']; dump(a.output/'provenance.json',prov)

if __name__=='__main__': main()
