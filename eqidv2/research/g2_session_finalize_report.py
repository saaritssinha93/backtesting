"""Join recorded execution evidence and isolated historical tests into audit report."""
from pathlib import Path
import sys,json,html
import numpy as np
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research import g2_session_forensic_audit as a

def run(out):
    s=json.loads((out/'summary.json').read_text()); day=s['session']
    records=json.loads((out/'live_recorded_evidence.json').read_text())
    observed=[]; recorded_gates=[]; orders=[]; artifacts=[]
    for item in records:
        r=item.get('record',{}); cat=item['category']
        if not isinstance(r,dict): continue
        artifacts.append(dict(category=cat,path=item['path'],published_at=r.get('published_at_ist'),state=r.get('state'),
          signal_end=r.get('signal_end'),confirmation_end=r.get('confirmation_end'),candidate_count=r.get('candidate_count'),
          contracts_evaluated=r.get('contracts_evaluated'),selected_long=r.get('selected_long'),selected_short=r.get('selected_short')))
        if cat in ['scanner_5m','confirmation_1m']:
            for idx,x in enumerate(r.get('feature_evaluations',[])):
                common=dict(category=cat,source=item['path'],json_feature_index=idx,published_at=r.get('published_at_ist'),
                    symbol=x.get('tradingsymbol'),signal_end=x.get('signal_end'),confirmation_end=x.get('confirmation_end'))
                observed.append({**x,**common})
                for k,v in x.items():
                    if k.startswith('gate_'):
                        recorded_gates.append({**common,'gate':k,'recorded_result':v,'first_failed_gate':x.get('first_failed_gate'),
                          'recorded_base_side':x.get('base_side'),'actual_variant':'PROMOTED_RELAXED_0925_NOT_FROZEN'})
        if cat=='orders':
            cols=['tradingsymbol','side','setup_id','mode','status','status_reason','quantity','strategy_sized_quantity',
             'trigger_price','entry_price','entry_at_ist','exit_price','exit_at_ist','exit_reason','net_pnl_rs',
             'first_entry_blocker_reason','last_entry_blocker_reason','entry_terminal_cause','entry_submission_reason','entry_submission_attempt_count']
            orders.append({k:r.get(k) for k in cols}|{'source':item['path']})
    obs=pd.DataFrame(observed); od=pd.DataFrame(orders)
    a.csv(out/'recorded_scanner_confirmation_features.csv',obs)
    a.csv(out/'recorded_scanner_confirmation_gates.csv',pd.DataFrame(recorded_gates))
    a.csv(out/'recorded_live_paper_orders.csv',od); a.csv(out/'recorded_slot_publications.csv',pd.DataFrame(artifacts))
    slots=pd.read_csv(out/'all_stock_slot_audit.csv'); slots['signal_end']=slots.signal_time.str[-5:]
    recorded=[]
    for x in slots.itertuples():
        ev=obs.loc[obs.symbol.eq(x.symbol)&obs.signal_end.eq(x.signal_end)]
        scanner=ev.loc[ev.category.eq('scanner_5m')]
        conf=ev.loc[ev.category.eq('confirmation_1m')&ev.base_side.eq(x.side)]
        oo=od.loc[od.tradingsymbol.eq(x.symbol)&od.setup_id.eq(x.setup_id)]
        recorded.append(dict(symbol=x.symbol,setup_id=x.setup_id,
          recorded_scanner_evaluated=bool(len(scanner)),recorded_confirmation_evaluated=bool(len(conf)),
          recorded_confirmation_status='EVALUATED' if len(conf) else 'NO_RECORDED_EVALUATION',
          scanner_first_failed_gate=scanner.first_failed_gate.iloc[0] if len(scanner) else None,
          scanner_recorded_base_side=scanner.base_side.iloc[0] if len(scanner) else None,
          confirmation_first_failed_gate=conf.first_failed_gate.iloc[0] if len(conf) else None,
          scanner_source=scanner.source.iloc[0] if len(scanner) else None,
          confirmation_source=conf.source.iloc[0] if len(conf) else None,
          recorded_live_order_status=';'.join(oo.loc[oo['mode'].eq('LIVE'),'status']) or 'NO_RECORDED_ORDER',
          recorded_paper_order_status=';'.join(oo.loc[oo['mode'].eq('PAPER'),'status']) or 'NO_RECORDED_ORDER',
          recorded_rulebook='PROMOTED_RELAXED_0925_NOT_FROZEN'))
    recframe=pd.DataFrame(recorded)
    slots=slots.drop(columns=[c for c in recframe if c in slots and c not in ['symbol','setup_id']])
    slots=slots.merge(recframe,on=['symbol','setup_id'],validate='one_to_one')
    a.csv(out/'all_stock_slot_audit.csv',slots);a.csv(out/'qualifying_stock_slot_audit.csv',slots.loc[slots.qualifies_2pct])
    cf=pd.read_csv(out/'top20_slot_counterfactuals.csv'); top=pd.concat([pd.read_csv(out/'top10_long.csv').assign(side='LONG'),pd.read_csv(out/'top10_short.csv').assign(side='SHORT')])
    earliest=[]
    for row in top.itertuples():
        choices=cf.loc[cf.symbol.eq(row.symbol)&cf.side.eq(row.side)&cf.status.eq('SIMULATED_FILLED')].sort_values('entry_ts')
        if len(choices): earliest.append(choices.iloc[0].to_dict())
        else: earliest.append(dict(symbol=row.symbol,side=row.side,status='NO_FILLED_CASE_WITHIN_TESTED_DIRECTION_PRESERVING_FAMILY'))
    a.csv(out/'top20_earliest_counterfactual_entries.csv',pd.DataFrame(earliest))
    gateframe=pd.read_csv(out/'all_indicator_checks.csv')
    onlyone=slots.loc[slots.failed_gate_count.eq(1),['symbol','setup_id']]
    single=gateframe.loc[gateframe.threshold_result.eq('FAIL')].merge(onlyone,on=['symbol','setup_id'],validate='many_to_one')
    a.csv(out/'single_blocker_candidates.csv',single)
    allcf=pd.read_csv(out/'counterfactual_universe_trades.csv'); baseline=pd.read_csv(out/'frozen_portfolio_trades.csv')
    bk=set(zip(baseline.tradingsymbol,baseline.setup_id)); policies=[]
    for pid,group in allcf.groupby('policy_id'):
        group=group.copy();group['portfolio_executed']=group.portfolio_executed.eq(True)
        added=group.loc[~group.apply(lambda r:(r.tradingsymbol,r.setup_id) in bk,axis=1)&group.portfolio_executed]
        policies.append(dict(policy_id=pid,**a.metrics(group),added_losers=int(added.portfolio_net_profit_rupees.lt(0).sum()),
             added_winners=int(added.portfolio_net_profit_rupees.gt(0).sum()),historical_status='UNVALIDATED_EXACT_DIAGNOSTIC_POLICY'))
    a.csv(out/'counterfactual_session_policy_comparison.csv',pd.DataFrame(policies))
    moves=pd.read_csv(out/'universe_movements.csv');up=pd.read_csv(out/'gainers_ge_2pct.csv');down=pd.read_csv(out/'decliners_le_minus2pct.csv')
    s.update(observed_continuous_plus2_count=int(moves.peak_pct.ge(2).sum()),observed_continuous_minus2_count=int(moves.trough_pct.le(-2).sum()),
      smaller_intraday_loss_pct=100*s['smaller_intraday_loss_count']/s['movement_usable'],
      smaller_closing_loss_pct=100*s['smaller_closing_loss_count']/s['movement_usable'],
      observed_move_statistics={c:{k:float(getattr(moves[c],k)()) for k in ['mean','median','min','max']} for c in ['peak_pct','trough_pct']})
    history=pd.read_csv(out/'historical_volume_comparison.csv',dtype={'variant':str}); daywise=pd.read_csv(out/'historical_volume_daywise.csv',dtype={'variant':str})
    capture=[]
    for variant in history.variant:
        trades=pd.read_csv(out/f'history_trades_minvol_{variant.replace(".","p")}.csv')
        trades=trades.loc[trades.day.astype(str).eq(day)&trades.portfolio_executed.eq(True)]
        long_captured=set();short_captured=set()
        for tr in trades.itertuples():
            candidates=up if tr.side=='LONG' else down
            found=candidates.loc[candidates.symbol.eq(tr.tradingsymbol)]
            if found.empty: continue
            stamp=found.iloc[0]['official_peak_time' if tr.side=='LONG' else 'official_trough_time']
            if str(stamp).startswith(day) and pd.Timestamp(tr.entry_ts)<pd.Timestamp(stamp):
                (long_captured if tr.side=='LONG' else short_captured).add(tr.tradingsymbol)
        capture.append(dict(variant=variant,date=day,plus2_before_peak_entries=len(long_captured),minus2_before_trough_entries=len(short_captured),
           plus2_no_proven_prepeak_entry=len(up)-len(long_captured),minus2_no_proven_pretrough_entry=len(down)-len(short_captured),
           unknown_extreme_time_long=int(up.official_peak_time.str.startswith('UNAVAILABLE').sum()),
           unknown_extreme_time_short=int(down.official_trough_time.str.startswith('UNAVAILABLE').sum())))
    a.csv(out/'session_opportunity_capture_comparison.csv',pd.DataFrame(capture))
    labels={'1.2':'Frozen G2: confirmation volume >=1.20x','1.1':'Confirmation volume >=1.10x','1.0':'Confirmation volume >=1.00x','short0936_oi0p10':'09:35 SHORT setup OI minimum 0.50% -> 0.10%'}
    history['rule']=history.variant.map(labels); s['historical_tests']=history.to_dict('records')
    a.dump(out/'summary.json',s)
    a.report(out,s,moves,up,down,pd.read_csv(out/'top10_long.csv'),pd.read_csv(out/'top10_short.csv'),slots,pd.read_csv(out/'all_indicator_checks.csv'),cf,baseline)
    body=(out/'report.html').read_text(encoding='utf-8')
    section='<h2>Historical comparison: all 46 available complete sessions</h2>'
    section+='<p>July 29–October 7, 2026. These are reused historical observations, not an untouched forward test. October 1 has no complete replay and is excluded, as are dates absent from the sealed manifest. The exact 21 hindsight-fitted policy combinations above remain unvalidated; these three simple alternatives were separately replayed across all 46 available sessions.</p>'
    section+='<div class="scroll">'+history.to_html(index=False,float_format=lambda x:f'{x:,.2f}')+'</div>'
    section+='<p>1.10x confirmation volume increases historical net P&amp;L but reduces win rate and profit factor; it is a candidate for an untouched forward test, not an approved replacement. 1.00x materially degrades this sample. Lowering the 09:35 SHORT OI floor can capture NATIONALUM on October 7 but must be judged by the full-history comparison, not that one profitable example.</p>'
    section+='<h3>Day-by-day net P&amp;L (INR)</h3>'+daywise.pivot(index='date',columns='variant',values='net_pnl').rename(columns=labels).reset_index().to_html(index=False,float_format=lambda x:f'{x:,.2f}')
    section+='<h3>October 7 opportunity coverage</h3>'+pd.DataFrame(capture).to_html(index=False)+'<p>A captured qualifier means a same-direction simulated entry before its first documented whole-session extreme. It does not mean a profitable trade: SUPREMEIND qualifies but loses. Unknown extreme times are explicitly included in the no-proven-entry count rather than invented. Full-history movement-based counts have not been reconstructed.</p>'
    section+='<h3>Recorded LIVE and PAPER execution — not simulated fills</h3><div class="scroll">'+od.to_html(index=False,na_rep='Not recorded')+'</div>'
    section+='<p>The live SUPREMEIND order was rejected by the broker for an IP allowlist PermissionException. No live fill was recorded. The PAPER trade used actual paper timestamps and integer quantity, so its P&amp;L differs from the frozen candle-based fractional-exposure backtest. No broker settings or orders were changed.</p>'
    section+='<h3>Earliest simulated fills among tested counterfactuals</h3><div class="scroll">'+pd.DataFrame(earliest)[[c for c in ['symbol','side','setup_id','status','entry_ts','entry_price','exit_reason','portfolio_net_profit_rupees','entry_before_observed_extreme','change_count'] if c in pd.DataFrame(earliest)]].to_html(index=False,na_rep='Unavailable',float_format=lambda x:f'{x:,.4f}')+'</div>'
    section+='<p>ADANIGREEN reached its high at 09:21, and TITAN reached its low at 09:24, both before the first 09:25 signal. Their peak/trough rankings do not imply those full moves were tradable by G2.</p>'
    section+='<h3>What is not yet established</h3><p>A globally smallest change for every missed stock has not been proven. The bounded counterfactual family preserves directional candles and increasing positive OI. Alternative structural rules need separate design and replay. All exact fitted combinations need a complete rejected-feature/path reconstruction across history, including the older extension without a feature ledger; that replay has not been completed. Historical +2%/-2% missed-opportunity counts and intraday mark-to-market drawdown are not established by the candidate/trade archives. These limitations must not be interpreted as zero risk or zero missed opportunities.</p>'
    section+='<h3>Single-blocker candidates</h3><div class="scroll">'+single[['symbol','setup_id','gate','observed','operator','required','margin','decision_time']].to_html(index=False,float_format=lambda x:f'{x:,.6f}')+'</div>'
    section+='<p>These four cases fail just one measured filter, but still require a ranking, quota and fill replay before any entry is claimed. NATIONALUM has that replay in the counterfactual tables. A small threshold margin alone does not establish a profitable missed trade.</p>'
    section+='<h3>Continuous-session measurement checks</h3><p>All 213 stocks have 360 timestamped one-minute bars and 72 complete five-minute aggregations. Of these, 199 have positive volume in every minute; 14 have one or more zero-volume minutes (excluded from traded-bar extrema, retained in native execution paths to keep the baseline unchanged). Whole-session official extrema and observed continuous-session extrema yield identical +2%/-2% membership for this date. Four official highs and nine official lows have no matching observed minute, so their exact times remain unavailable.</p>'
    section+='<div class="scroll">'+pd.DataFrame(s['observed_move_statistics']).T.reset_index().to_html(index=False,float_format=lambda x:f'{x:.6f}')+'</div>'
    section+='<p>The sum of official individual peak moves is '+f'{s["descriptive_sum_of_peak_moves_pct_not_portfolio_return"]:.4f}'+' percentage points, a descriptive opportunity measure, not a realizable portfolio return. It includes negative peak returns for stocks that never traded above the prior official close.</p>'
    body=body.replace('<h2>Conclusions and limits</h2>',section+'<h2>Conclusions and limits</h2>')
    body=body.replace('No new threshold is recommended as historically validated by this session-only forensic replay.','No new threshold is approved for promotion. The three simple historical sensitivities below provide in-sample evidence only.')
    (out/'report.html').write_text(body,encoding='utf-8')
    # Independent invariants and final output hashes.
    assert len(moves)==213 and moves.symbol.is_unique
    assert len(slots)==len(moves)*14 and not slots.duplicated(['symbol','setup_id']).any()
    assert len(cf)==140
    assert np.allclose((moves.official_close/moves.official_previous_close-1)*100,moves.close_return_pct)
    assert len(up)==moves.peak_pct.ge(2).sum() and len(down)==moves.trough_pct.le(-2).sum()
    assert len(history)==4 and len(daywise)==46*4
    assert np.allclose(daywise.groupby('variant').net_pnl.sum().sort_index(),history.set_index('variant').net_pnl.sort_index())
    assert history.loc[history.variant.eq('1.2'),'trades'].iloc[0]==90
    assert abs(daywise.loc[daywise.date.eq(day)&daywise.variant.eq('1.2'),'net_pnl'].iloc[0]+5250)<1e-6
    source=json.loads((out/'provenance.json').read_text())
    for p,checksum in source['input_hashes'].items():
        # Builder modified after first generation only if explicitly documented;
        # the finalized builder is also pinned in the final manifest.
        if p.endswith('g2_session_forensic_audit.py'): continue
        assert a.g2.sha256(Path(p))==checksum,p
    final=dict(checks_passed=['universe_uniqueness','213x14_slot_coverage','140_counterfactual_cases','official_return_arithmetic','observed_official_qualifier_count_parity','46x4_history_coverage','daywise_total_reconciliation','baseline_source_hashes_unchanged'],
      outputs={p.name:a.g2.sha256(p) for p in out.iterdir() if p.is_file() and p.name!='final_validation.json'},
      research_builders={str(Path(__file__)):a.g2.sha256(Path(__file__)),str(Path(a.__file__)):a.g2.sha256(Path(a.__file__)),str(Path(__file__).with_name('g2_session_historical_volume_test.py')):a.g2.sha256(Path(__file__).with_name('g2_session_historical_volume_test.py'))})
    a.dump(out/'final_validation.json',final)
    print('VALIDATION PASSED',len(slots),'slot rows;',len(cf),'counterfactuals;',len(history),'historical variants',flush=True)
    print(history.to_string(index=False),flush=True)

if __name__=='__main__': run(Path(sys.argv[1]))
