"""Build a research report from the completed, locked G3 exit study.

This module reads saved artifacts only. It cannot place orders, tune parameters,
run the study, change a selection lock or update any production configuration.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STUDY = Path(r'C:/TradingData/eqidv2/g3_sl_target_research/20261016/study_v1')
AUDIT = Path(__file__).resolve().parent/'kite/broker_audit.json'
POLICIES = ('original_g3', 'bounded_current', 'locked_global', 'locked_pooled')
LABELS = {'original_g3': 'Original G3', 'bounded_current': 'Bounded current',
          'locked_global': 'Locked global comparison', 'locked_pooled': 'Primary pooled research policy'}
PERIODS = (('all','All 52 sessions'), ('development35','First 35 sessions'), ('later17','Later 17 sessions'))


def load_json(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def n(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f'Nonfinite report value: {value}')
    return result


def yes(value):
    return str(value).strip().lower() in ('true','1','1.0')


def money(value):
    value = n(value)
    return ('−' if value < 0 else '') + '₹' + f'{abs(value):,.2f}'


def signed_money(value):
    value = n(value)
    return ('+' if value > 0 else '') + money(value)


def fmt(value, kind):
    if kind == 'money': return money(value)
    if kind == 'delta': return signed_money(value)
    if kind == 'int': return f'{int(n(value)):,}'
    if kind == 'pct': return f'{n(value):.2f}%'
    return str(value)


def table(rows, columns, *, table_id='', scroll_class='', primary=False):
    head=''.join('<th scope="col">'+html.escape(label)+'</th>' for _,label,_ in columns)
    bodies=[]
    for r in rows:
        css=' class="primary-row"' if primary and r.get('policy')=='locked_pooled' else ''
        cells=''.join('<td>'+html.escape(fmt(r.get(key,''),kind))+'</td>' for key,_,kind in columns)
        bodies.append(f'<tr{css}>{cells}</tr>')
    identity=f' id="{html.escape(table_id)}"' if table_id else ''
    return f'<div class="table-wrap {scroll_class}"><table{identity}><thead><tr>{head}</tr></thead><tbody>'+''.join(bodies)+'</tbody></table></div>'


def params(record, current_target=None):
    family=record['family']
    stop=n(record['stop_pct'])
    target=record.get('target_pct')
    if target is None:
        if current_target is None:
            target_text='Current target clipped to 0.60–2.00%'
        else:
            target=min(2.,max(.6,n(current_target)))
            target_text=f'{target:.2f}%'
    else:
        target=n(target);target_text=f'{target:.2f}%'
    if family=='bounded_current':
        rule='Tighten stop to 1.00% after 120 minutes'
    elif family=='protected':
        rule=f"After completed close ≥+{n(record['protect_after_close_pct']):.2f}%, protect +{n(record['protected_profit_pct']):.2f}% from following minute"
    else:
        rule='Fixed stop and target; no staged tightening'
    assert stop <=1.2+1e-9
    if target is not None: assert .6-1e-9 <=target<=2.+1e-9
    return stop,target,target_text,rule


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--study-dir',type=Path,default=DEFAULT_STUDY)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    study=args.study_dir.resolve();output=(args.output or study/'report.html').resolve()
    summary=load_json(study/'summary.json')
    protocol=load_json(study/'protocol.json')
    lock=load_json(study/'selection_lock.json')
    broker=load_json(AUDIT)
    metrics=read_csv(study/'policy_comparison.csv')
    setups=read_csv(study/'setup_recommendations.csv')
    daily=read_csv(study/'daily_comparison.csv')
    live=read_csv(study/'live_cost_comparison.csv')
    rolling=read_csv(study/'development_rolling_check.csv')
    development_candidates=read_csv(study/'development_candidates.csv')
    followup_dir=study/'followup_stop_timing'
    followup=load_json(followup_dir/'summary.json')
    followup_metrics=read_csv(followup_dir/'policy_comparison.csv')
    followup_live=read_csv(followup_dir/'live_cost_comparison.csv')
    assert followup['protocol']['status']=='POST_HOC_EXPLORATORY'
    assert followup['production_changed'] is False
    registry={r['candidate_id']:r for r in protocol['grid']}
    protection_levels=sorted({n(r['protected_profit_pct']) for r in registry.values() if r['family']=='protected'})
    if len(protection_levels)!=1:
        raise ValueError('Report narrative requires a single frozen protection level')
    protection_level=protection_levels[0]
    assert summary['configuration_promoted'] is False
    assert lock['selection_uses_later_outcomes'] is False
    assert len(setups)==summary['active_setups']
    lookup={(r['period'],r['policy'],int(n(r['extra_roundtrip_bps']))):r for r in metrics}
    for period,_ in PERIODS:
        for policy in POLICIES:
            assert (period,policy,0) in lookup and (period,policy,10) in lookup
    original=lookup['all','original_g3',0]
    pooled=lookup['all','locked_pooled',0]
    original_later=lookup['later17','original_g3',0]
    pooled_later=lookup['later17','locked_pooled',0]
    live_lookup={(r['period'],r['policy'],r['cost_case']):r for r in live}
    live_case='net_after_fees_and_5bps_slippage_rs'
    live_original=live_lookup['all','original_g3',live_case]
    live_pooled=live_lookup['all','locked_pooled',live_case]
    live_global=live_lookup['all','locked_global',live_case]
    original_ledger={r['sid']:r for r in read_csv(study/'ledgers/original_g3.csv')}
    pooled_ledger=read_csv(study/'ledgers/locked_pooled.csv')
    midday_changes=[]
    for r in pooled_ledger:
        if r['group']!='midday_SHORT' or r['day'] in lock['development_days'] or not yes(r['portfolio_executed']):continue
        b=original_ledger[r['sid']]
        if not yes(b['portfolio_executed']):continue
        old=n(b['portfolio_net_profit_rupees']);new=n(r['portfolio_net_profit_rupees'])
        transition=('Winner to stop loss' if old>0 and new<0 and 'STOP' in r['exit_reason'] else
                    'Loss to winner' if old<0 and new>0 else '')
        if transition:midday_changes.append(dict(day=r['day'],setup=r['setup_id'],symbol=r['tradingsymbol'],transition=transition,original=old,pooled=new,delta=new-old))
    assert sum(r['transition']=='Winner to stop loss' for r in midday_changes)==4
    assert sum(r['transition']=='Loss to winner' for r in midday_changes)==1
    assert math.isclose(n(original['net_pnl_rs']), next(n(r['net_pnl_rs']) for r in summary['all_base_cost_metrics'] if r['policy']=='original_g3'),abs_tol=1e-5)
    days=sorted({r['day'] for r in daily})
    assert len(days)==summary['sessions']
    daily_lookup={(r['day'],r['policy']):r for r in daily}
    for policy in POLICIES:
        assert math.isclose(sum(n(daily_lookup[d,policy]['net_pnl_rs']) for d in days),n(lookup['all',policy,0]['net_pnl_rs']),abs_tol=1e-5)
    display_metrics=[]
    for period,label in PERIODS:
        for policy in POLICIES:
            row=dict(lookup[period,policy,0]);row['label']=LABELS[policy];row['period_label']=label
            row['delta']=n(row['net_pnl_rs'])-n(lookup[period,'original_g3',0]['net_pnl_rs'])
            display_metrics.append(row)
    css='''
    :root{color-scheme:light;--bg:#f5f7fa;--card:#fff;--fg:#142231;--muted:#4d6072;--line:#d4dde7;--head:#e9eff6;--accent:#075c9d;--highlight:#e6f2fc;--warn:#7d4b00;--warnbg:#fff3d9}
    @media(prefers-color-scheme:dark){:root{color-scheme:dark;--bg:#101821;--card:#182532;--fg:#e8f0f8;--muted:#b7c7d7;--line:#3b5064;--head:#263b50;--accent:#79c1ff;--highlight:#203e55;--warn:#ffd795;--warnbg:#3b2c16}}
    *{box-sizing:border-box}body{font:15px/1.55 system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--fg);margin:0}.page{max-width:1600px;margin:auto;padding:32px 26px 72px}h1{font-size:32px;line-height:1.2;margin:4px 0 18px}h2{font-size:24px;line-height:1.3;margin:36px 0 12px}h3{font-size:18px;margin:22px 0 10px}p{max-width:1120px;margin:10px 0}a{color:var(--accent)}.eyebrow{font-weight:650;color:var(--muted)}.hero,.note{padding:22px;background:var(--card);border:1px solid var(--line);border-radius:12px}.warning{background:var(--warnbg);color:var(--warn);border-color:var(--warn)}.cards{display:grid;grid-template-columns:repeat(4,minmax(180px,1fr));gap:12px;margin:20px 0}.metric{padding:16px;background:var(--card);border:1px solid var(--line);border-radius:9px}.metric strong{display:block;font-size:22px;margin:5px 0}.metric span{color:var(--muted);font-size:13px}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:9px;background:var(--card)}table{width:100%;border-collapse:separate;border-spacing:0;font-size:13px;font-variant-numeric:tabular-nums}th,td{padding:10px 12px;border-bottom:1px solid var(--line);text-align:right;vertical-align:top;white-space:nowrap}th{background:var(--head);font-size:12px}th:first-child,td:first-child{text-align:left}tbody tr:last-child td{border-bottom:0}.primary-row td{background:var(--highlight);font-weight:600}.tall{max-height:650px}.tall th{position:sticky;top:0;z-index:1}.wide-rule td:nth-child(7){white-space:normal;min-width:330px;max-width:450px;text-align:left}.muted,.small{color:var(--muted)}.small{font-size:13px}.downloads{display:flex;flex-wrap:wrap;gap:9px 22px;margin:15px 0}.search{padding:11px 14px;max-width:460px;width:100%;background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:7px;margin:10px 0}details{margin:14px 0}summary{cursor:pointer;font-weight:650}li{margin-bottom:8px;max-width:1120px}.formula{font-family:ui-monospace,Consolas,monospace;padding:12px;background:var(--head);border-radius:7px;max-width:1120px}.anchor-nav{display:flex;flex-wrap:wrap;gap:10px 20px;margin:18px 0}.setup-count{display:inline-block;margin-left:12px;color:var(--muted)}@media(max-width:900px){.cards{grid-template-columns:repeat(2,minmax(130px,1fr))}.page{padding:20px 14px}h1{font-size:26px}}@media print{.page{padding:0;max-width:none}.table-wrap,.tall{overflow:visible;max-height:none}.search,.anchor-nav{display:none}table{font-size:8px}th,td{padding:4px;white-space:normal}h2{break-after:avoid}.cards{grid-template-columns:repeat(4,1fr)}a{color:inherit}.hero,.note{break-inside:avoid}}
    '''
    page=['<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 stop and target research through 15 October 2026</title><style>'+css+'</style></head><body><main class="page">']
    page.append('<div class="eyebrow">Historical exit research · No configuration deployed</div><h1>G3 stop-loss and target comparison</h1>')
    page.append(f'<p>{summary["sessions"]} sessions from {html.escape(summary["first_date"])} through {html.escape(summary["source_date"])}; {summary["active_setups"]} active setups. Initial stop ≤1.20%; target 0.60–2.00%.</p>')
    page.append('<div class="hero"><p><strong>The locked pooled policy is the primary research policy.</strong> Six time/side groups were selected using only the first 35 sessions. The global policy is a comparison; later outcomes do not select which policy is primary.</p>')
    page.append(f'<p>Across all sessions, the pooled proposal changes net PnL by <strong>{signed_money(n(pooled["net_pnl_rs"])-n(original["net_pnl_rs"]))}</strong> versus original G3, and losing days from <strong>{int(n(original["losing_days"]))} to {int(n(pooled["losing_days"]))}</strong>. Over the later 17 sessions its net difference is <strong>{signed_money(n(pooled_later["net_pnl_rs"])-n(original_later["net_pnl_rs"]))}</strong>.</p>')
    if n(pooled['net_pnl_rs']) < n(original['net_pnl_rs']) and n(pooled['profitable_days']) <= n(original['profitable_days']):
        sacrificed=100*(n(original['net_pnl_rs'])-n(pooled['net_pnl_rs']))/n(original['net_pnl_rs'])
        page.append(f'<p><strong>These results do not support promoting the pooled rules:</strong> they sacrifice {sacrificed:.2f}% of historical net profit without adding profitable days. The setup table is a research proposal, not a claim of live-best settings.</p>')
    global_all=lookup['all','locked_global',0]
    if n(global_all['net_pnl_rs']) < n(original['net_pnl_rs']):
        sacrificed=100*(n(original['net_pnl_rs'])-n(global_all['net_pnl_rs']))/n(original['net_pnl_rs'])
        extra_days=int(n(global_all['profitable_days'])-n(original['profitable_days']))
        page.append(f'<p>The uniform global comparison sacrifices {sacrificed:.2f}% of historical net profit for {extra_days:+d} profitable days. It remains a comparison and does not replace the preselected pooled policy.</p>')
    page.append('<p class="small">The underlying strategy was previously researched on this history. The later period is a chronological diagnostic, not an untouched holdout or evidence of live readiness.</p></div>')
    page.append(f'<div class="note warning" style="margin-top:16px"><p><strong>Deployment assessment: the proposed exit changes fail to improve the stated objective at current live size.</strong> With ₹50,000 exposure, indicative fees and an additional 5bps round-trip slippage, original G3 has {int(n(live_original["profitable_days"]))} profitable / {int(n(live_original["losing_days"]))} losing days and {money(live_original["net_pnl_rs"])} net. Pooled rules produce {int(n(live_pooled["profitable_days"]))} / {int(n(live_pooled["losing_days"]))} and {money(live_pooled["net_pnl_rs"])}; the global comparison produces {int(n(live_global["profitable_days"]))} / {int(n(live_global["losing_days"]))} and {money(live_global["net_pnl_rs"])}. Both reduce profit and increase losing days in this projection.</p><p>The pooled later-period trade win rate at the historical base-cost model is {n(pooled_later["win_rate_pct"]):.2f}%, versus {n(original_later["win_rate_pct"]):.2f}% originally. Do not promote the pooled 0.80% midday SHORT stop: it turns four later original winners into stop losses, while only one later original loser becomes a winner. The bounded control below is the least disruptive paper candidate within the requested bounds, not a demonstrated improvement or live recommendation.</p></div>')
    page.append('<nav class="anchor-nav"><a href="#comparison">Results</a><a href="#policy">Locked rules</a><a href="#setups">All setups</a><a href="#losses">Losses and profit</a><a href="#timing">Timer diagnostic</a><a href="#live">Live-size costs</a><a href="#daily">Daywise</a><a href="#deployment">Execution gaps</a><a href="#limits">Limits</a></nav>')
    page.append('<div class="cards">'+''.join(f'<div class="metric"><span>{html.escape(label)}</span><strong>{html.escape(value)}</strong><span>{html.escape(detail)}</span></div>' for label,value,detail in [
        ('Pooled historical net',money(pooled['net_pnl_rs']),'₹500,000 modeled exposure per trade'),
        ('Pooled trade win rate',fmt(pooled['win_rate_pct'],'pct'),f'{int(n(pooled["wins"]))} wins / {int(n(pooled["trades"]))} executions'),
        ('Pooled profitable days',f'{int(n(pooled["profitable_days"]))} / {summary["sessions"]}',f'{int(n(pooled["losing_days"]))} losing; {int(n(pooled["zero_days"]))} zero days'),
        ('Pooled daily-close drawdown',money(pooled['daily_close_drawdown_rs']),'Does not measure intraday mark-to-market risk')])+'</div>')
    page.append('<div class="downloads"><a href="policy_comparison.csv">Policy results CSV</a><a href="setup_configuration_comparison.csv">All setup configurations CSV</a><a href="daily_comparison.csv">Daywise CSV</a><a href="live_cost_comparison.csv">Live-size cost CSV</a><a href="selection_lock.json">Frozen selection record</a><a href="protocol.json">Study protocol</a></div>')
    page.append('<h2 id="comparison">Original, bounded and locked policies</h2><p>These tables use ₹100,000 capital and five-times exposure per entry, with the same 5bps modeled round-trip cost. Portfolio capacity is allocated again after the exit changes. Zero-trade days stay in each period’s denominator.</p>')
    columns=[('label','Policy','text'),('trades','Trades','int'),('win_rate_pct','Win rate','pct'),('net_pnl_rs','Net PnL','money'),('delta','Change vs original','delta'),('profitable_days','Profitable days','int'),('losing_days','Losing days','int'),('zero_days','Zero days','int'),('negative_trade_sum_abs_rs','Absolute losing-trade sum','money'),('daily_close_drawdown_rs','Daily-close drawdown','money')]
    for period,label in PERIODS:
        page.append('<h3>'+label+'</h3>'+table([r for r in display_metrics if r['period']==period],columns,primary=True))
    page.append('<p class="small">Original G3 is a control and can exceed the requested bounds: its initial stop is 1.25%, and some accepted targets are below 0.60% or above 2.00%. Bounded current uses SL1.20%, clips existing targets to0.60–2.00%, and keeps tightening to1.00% after120minutes.</p>')
    page.append('<h2 id="policy">Frozen pooled policy and selection method</h2>')
    group_rows=[]
    for group,candidate_id in lock['group_candidates'].items():
        stop,_,target,rule=params(registry[candidate_id])
        times=sorted(r['signal_time'] for r in setups if r['group']==group)
        group_rows.append(dict(group=group,signals=f'{times[0]}–{times[-1]}' if times else '',candidate=candidate_id,stop=stop,target=target,rule=rule))
    page.append(table(group_rows,[('group','Group','text'),('signals','Signal times','text'),('candidate','Frozen candidate','text'),('stop','Initial SL','pct'),('target','Target','text'),('rule','Exit rule','text')]))
    page.append(f'<p><strong>Global comparison:</strong> {html.escape(lock["global_candidate"])}. It is not substituted for pooled rules after inspecting later performance.</p>')
    chosen_families={registry[c]['family'] for c in [lock['global_candidate'],*lock['group_candidates'].values()]}
    if 'protected' not in chosen_families:
        page.append('<p><strong>No protected-stop candidate was selected</strong> by the frozen global or pooled development rules. Their complete development results remain visible below.</p>')
    page.append(f'<p>{len(registry)} candidates were declared: bounded current, fixed SL/TP combinations, and completed-close profit protection. Ranking prioritizes profitable days, then trade win rate, net PnL and lower daily-close drawdown. Candidates require ≥90% native-fill retention and positive development net with an additional10bps round-trip cost.</p>')
    page.append('<p>Selection scores use the historical flat5bps round-trip model at ₹500,000 exposure. The separate ₹50,000 projection below applies current indicative broker fees and is not fed back into parameter selection. These bounded comparisons do not establish a global optimum.</p>')
    page.append(f'<p><strong>Profit protection:</strong> after a full post-entry one-minute candle closes above the selected favorable-move threshold, the stop moves to entry+{protection_level:.2f}% for a LONG or entry−{protection_level:.2f}% for a SHORT, effective from the following minute. It never widens.</p>')
    page.append(f'<p class="note warning">The +{protection_level:.2f}% protective level is a gross price move. Around ₹50,000 exposure, a +0.20% exact exit produces ₹100 gross; indicative fees of about ₹53 plus 5bps slippage of ₹25 leave about ₹22. Gaps, partial fills and actual costs can erase that cushion. Structural/ATR stops were not tested where historical anchors were missing.</p>')
    page.append('<h3>All predeclared development candidates</h3><p>The same first35 sessions are shown for every candidate, including weaker results. Different stop widths trade off room for price fluctuation against the size of each loss; a higher win rate alone does not imply higher net profit. <a href="development_candidates.csv">Download all development scores</a>.</p>')
    candidate_rows=[]
    candidate_lookup={r['candidate_id']:r for r in development_candidates}
    assert set(candidate_lookup)==set(registry)
    for candidate_id,candidate in registry.items():
        r=dict(candidate_lookup[candidate_id]);stop,_,target,rule=params(candidate)
        selected=[]
        if lock['global_candidate']==candidate_id:selected.append('Global comparison')
        selected.extend(group for group,choice in lock['group_candidates'].items() if choice==candidate_id)
        r.update(sl=stop,tp=target,rule=rule,selected='; '.join(selected) or 'Not selected')
        candidate_rows.append(r)
    page.append(table(candidate_rows,[('candidate_id','Candidate','text'),('sl','Initial SL','pct'),('tp','Target','text'),('profitable_days','Profitable days /35','int'),('win_rate_pct','Trade win rate','pct'),('trades','Trades','int'),('net_pnl_rs','Net at5bps','money'),('stress_net_pnl_rs','Net at15bps','money'),('daily_close_drawdown_rs','Daily-close drawdown','money'),('selected','Frozen selection use','text')],scroll_class='tall'))
    page.append('<details><summary>Development-only rolling stability checks</summary><p>These three checks train on the first20/25/30 development sessions and evaluate the following five. They compare the development-selected global candidate with bounded current; they do not validate pooled choices or override the frozen selection.</p>'+table(rolling,[('train_sessions','Training sessions','int'),('evaluation_first','Evaluation from','text'),('evaluation_last','Evaluation through','text'),('policy','Comparison','text'),('candidate_id','Candidate','text'),('net_pnl_rs','Net PnL','money'),('win_rate_pct','Win rate','pct'),('profitable_days','Profitable days','int'),('losing_days','Losing days','int')])+'</details>')
    page.append(f'<h2 id="setups">All {len(setups)} setup configurations</h2><p><strong>Bounded control for paper comparison:</strong> keep each setup’s target, clipped to 0.60–2.00%; change initial SL to 1.20%; retain tightening to 1.00% after 120 minutes and 15:15 exit. This is the least disruptive tested policy that satisfies the requested SL/TP bounds, but it still trails original net profit and does not establish live readiness.</p><p>The locked pooled proposal and uniform global comparison are shown beside it for transparency. Their stronger development scores did not translate to improved live-size outcomes. Per-setup training winners are exploratory and are not substituted into these configurations. <a href="setup_configuration_comparison.csv">Download the complete configuration comparison</a>.</p>')
    page.append(f'<p>{summary["bespoke_qualified_count"]} setups meet the predeclared bespoke sample threshold of20 development native fills on15 distinct development days. Meeting that threshold alone is not independent validation.</p>')
    setup_display=[]
    for r in sorted(setups,key=lambda r:(r['signal_time'],r['side'])):
        par=json.loads(r['pooled_parameters']);stop,_,target,rule=params(par,r['current_target_pct'])
        setup_display.append(dict(setup=f'{r["signal_time"]} → {r["confirmation_time"]} {r["side"]}',group=r['group'],current_sl=r['current_initial_stop_pct'],current_tp=r['current_target_pct'],bounded_sl='1.20% → 1.00% after 120m',bounded_tp=min(2.,max(.6,n(r['current_target_pct']))),global_pair='SL 1.20% / TP 0.60%',proposed_sl=stop,proposed_tp=target,rule=rule,dev_fills=r['development_native_fills'],dev_days=r['development_fill_days'],later_trades=r['pooled_later_trades'],later_win=fmt(r['pooled_later_win_rate_pct'],'pct') if n(r['pooled_later_trades'])>0 else '— / no later fills',later_net=r['pooled_later_net_pnl_rs'],all_net=r['pooled_all_net_pnl_rs'],all_delta=n(r['pooled_all_net_pnl_rs'])-n(r['baseline_all_net_pnl_rs']),bespoke='Sample threshold met' if yes(r['qualifies_for_bespoke_recommendation']) else 'Sparse; pooled group'))
    export_names={'proposed_sl':'pooled_research_sl_pct','proposed_tp':'pooled_research_target',
                  'rule':'pooled_research_management_rule',
                  'global_pair':'global_comparison_only_sl_target',
                  **{key:'pooled_research_'+key for key in setup_display[0] if key.startswith(('later_','all_'))}}
    export_rows=[]
    for row in setup_display:
        export_row={export_names.get(key,key):value for key,value in row.items()}
        export_row.update(bounded_status='Least disruptive within-bounds paper candidate; not validated or deployed',
                          pooled_research_status='Failed deployment assessment; do not promote',
                          global_status='Research comparison only; do not promote')
        export_rows.append(export_row)
    with (study/'setup_configuration_comparison.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(export_rows[0]))
        writer.writeheader();writer.writerows(export_rows)
    page.append('<input class="search" id="setup-search" aria-label="Filter setup configurations" placeholder="Filter by time, LONG, SHORT or group"><span class="setup-count" id="setup-count"></span>')
    page.append(table(setup_display,[('setup','Signal → confirmation / side','text'),('current_sl','Current SL','pct'),('current_tp','Current target','pct'),('bounded_sl','Bounded control SL','text'),('bounded_tp','Bounded control target','pct'),('global_pair','Global comparison','text'),('proposed_sl','Pooled SL','pct'),('proposed_tp','Pooled target','text'),('rule','Pooled management rule','text'),('group','Group','text'),('dev_fills','Development native fills','int'),('dev_days','Development fill days','int'),('later_trades','Later pooled executions','int'),('later_win','Later pooled win rate','text'),('later_net','Later pooled net PnL','money'),('all_net','All-period pooled net','money'),('all_delta','Pooled change vs original','delta'),('bespoke','Sample assessment','text')],table_id='setups-table',scroll_class='tall'))
    page.append('<p class="small">Per-setup later percentages with few executions are highly uncertain. A dash means no later executions; no win rate can be estimated. The original setup research CSV also includes training-only exploratory choices for inspection.</p>')
    page.append('<h2 id="losses">Loss reduction versus profit sacrificed</h2><p>These quantities reconcile exactly: change in net PnL equals reduction in absolute losing-trade losses plus change in the sum of positive net trade PnL. They include portfolio admission changes, rather than pretending the same execution list always survives.</p>')
    loss_rows=[]
    for period,label in PERIODS:
        base=lookup[period,'original_g3',0]
        for policy in POLICIES:
            r=lookup[period,policy,0]
            saved=n(base['negative_trade_sum_abs_rs'])-n(r['negative_trade_sum_abs_rs'])
            positive_change=n(r['net_pnl_rs'])+n(r['negative_trade_sum_abs_rs'])-n(base['net_pnl_rs'])-n(base['negative_trade_sum_abs_rs'])
            delta=n(r['net_pnl_rs'])-n(base['net_pnl_rs'])
            assert math.isclose(saved+positive_change,delta,abs_tol=1e-5)
            loss_rows.append(dict(period=label,policy=policy,label=LABELS[policy],losses=r['losses'],loss_sum=r['negative_trade_sum_abs_rs'],saved=saved,positive_change=positive_change,delta=delta,initial=r['initial_stop_exits'],protected=r['protected_stop_exits'],all_stops=r['stop_exits']))
    page.append(table(loss_rows,[('period','Period','text'),('label','Policy','text'),('losses','Losing trades','int'),('loss_sum','Absolute losses','money'),('saved','Loss reduction','delta'),('positive_change','Change in positive trade PnL','delta'),('delta','Net change','delta'),('initial','Initial stop exits','int'),('protected','Protected stop exits','int'),('all_stops','All protective stop exits','int')],primary=True))
    page.append('<p class="small">Initial, tightened and protected stops are all included in the final column. A lower initial-stop count alone is not evidence of better economics.</p>')
    page.append('<h3>Why the 0.80% midday SHORT stop should not be promoted</h3><p>Among later-period midday SHORT trades executed by both policies, four original winners become stop losses under the narrower pooled stop; only one original loser becomes a winner. This paired diagnostic is post hoc and explains the failure; it does not select replacement parameters. Other trade changes and portfolio admissions are outside this small table.</p>')
    page.append(table(sorted(midday_changes,key=lambda r:(r['day'],r['setup'])),[('day','Date','text'),('setup','Setup','text'),('symbol','Stock','text'),('transition','Outcome change','text'),('original','Original net','money'),('pooled','Pooled net','money'),('delta','Difference','delta')]))
    page.append('<h3>Additional10bps round-trip execution stress</h3><p>The following deducts an additional10bps of entry exposure on every executed trade, on top of the base5bps. It preserves simulated fills and allocations; it does not claim to reconstruct broker slippage.</p>')
    stress=[]
    for period,label in PERIODS:
        for policy in POLICIES:
            r=dict(lookup[period,policy,10]);r.update(label=LABELS[policy],period_label=label)
            stress.append(r)
    page.append(table(stress,[('period_label','Period','text'),('label','Policy','text'),('net_pnl_rs','Net PnL at15bps total','money'),('win_rate_pct','Win rate','pct'),('profitable_days','Profitable days','int'),('losing_days','Losing days','int'),('daily_close_drawdown_rs','Daily-close drawdown','money')],primary=True))
    page.append('<h2 id="timing">Separate diagnostic: remove the 120-minute tightening?</h2><p><strong>Post hoc exploration, after the main results were known.</strong> One additional policy holds SL at 1.20% all session, keeps each existing target clipped to 0.60–2.00%, and retains the 15:15 exit. Its direct control is bounded current, which tightens SL to 1.00% after 120 minutes. This did not alter the frozen 30-candidate study or its primary selection. <a href="followup_stop_timing/summary.json">Diagnostic record</a> · <a href="followup_stop_timing/policy_comparison.csv">All policy scores</a> · <a href="followup_stop_timing/timed_stop_transitions.csv">Timed-stop trade changes</a>.</p>')
    followup_lookup={(r['period'],r['policy'],int(n(r['extra_roundtrip_bps']))):r for r in followup_metrics}
    followup_policy='bounded_targets_fixed_stop'
    fixed_all=followup_lookup['all',followup_policy,0]
    timed_all=lookup['all','bounded_current',0]
    fixed_later=followup_lookup['later17',followup_policy,0]
    timed_later=lookup['later17','bounded_current',0]
    page.append(f'<p class="note warning"><strong>Fewer stop exits did not improve the overall outcome.</strong> Removing the timer changes stop exits from {int(n(timed_all["stop_exits"]))} to {int(n(fixed_all["stop_exits"]))}, but profitable days fall from {int(n(timed_all["profitable_days"]))} to {int(n(fixed_all["profitable_days"]))}, net PnL changes by {signed_money(n(fixed_all["net_pnl_rs"])-n(timed_all["net_pnl_rs"]))}, and daily-close drawdown rises from {money(timed_all["daily_close_drawdown_rs"])} to {money(fixed_all["daily_close_drawdown_rs"])}. In the later 17 sessions, profitable days fall from {int(n(timed_later["profitable_days"]))} to {int(n(fixed_later["profitable_days"]))}, with net lower by {money(n(timed_later["net_pnl_rs"])-n(fixed_later["net_pnl_rs"]))}. This does not justify removing the timer.</p>')
    timing_rows=[]
    timing_live_rows=[]
    for period,label in PERIODS:
        for policy in ('original_g3','bounded_current',followup_policy):
            row=dict(followup_lookup[period,policy,0])
            if policy in POLICIES:
                for field in ('net_pnl_rs','profitable_days','stop_exits'):
                    assert math.isclose(n(row[field]),n(lookup[period,policy,0][field]),abs_tol=1e-5)
            row.update(period_label=label,label=LABELS.get(policy,'Fixed 1.20% stop, bounded setup targets'))
            timing_rows.append(row)
            live_row=next(dict(r) for r in followup_live if r['period']==period and r['policy']==policy and r['cost_case']==live_case)
            live_row.update(period_label=label,label=row['label'])
            timing_live_rows.append(live_row)
    page.append(table(timing_rows,[('period_label','Period','text'),('label','Policy','text'),('trades','Trades','int'),('win_rate_pct','Win rate','pct'),('net_pnl_rs','Net at historical 5bps','money'),('profitable_days','Profitable days','int'),('losing_days','Losing days','int'),('stop_exits','Stop exits','int'),('daily_close_drawdown_rs','Daily-close drawdown','money')]))
    page.append(f'<p>Of {followup["timed_stop_count"]} bounded-control timed-stop rows, {followup["timed_stops_to_positive_gross"]} reach a positive gross exit without the timer. Across all {followup["timed_stop_count"]} timed-stop rows, the combined net change is {signed_money(followup["net_change_on_timed_stop_rows"])}. This selected subset is not the portfolio result: other exits and later admissions also change, so the complete comparison above governs the conclusion.</p>')
    page.append('<details><summary>Timer diagnostic at ₹50,000: fees plus 5bps slippage</summary>'+table(timing_live_rows,[('period_label','Period','text'),('label','Policy','text'),('net_pnl_rs','Projected net','money'),('win_rate_pct','Win rate','pct'),('profitable_days','Profitable days','int'),('losing_days','Losing days','int'),('daily_close_drawdown_rs','Daily-close drawdown','money')])+'</details>')
    page.append('<h2 id="live">₹50,000 live-size fee projection</h2><p>Current LIVE sizing is₹10,000 capital×5, compared with₹100,000×5 in the historical tables. Integer shares are floor(₹50,000/trigger price). These projections reuse modeled entry/exit fills and portfolio allocation; they are not broker executions or a fresh live-sized capital simulation.</p>')
    live_rows=[]
    for period,label in PERIODS:
        for policy in POLICIES:
            for case in ('net_after_fees_rs','net_after_fees_and_5bps_slippage_rs'):
                r=next(dict(r) for r in live if r['period']==period and r['policy']==policy and r['cost_case']==case)
                r.update(label=LABELS[policy],period_label=label,cost_label='Indicative actual fees' if case=='net_after_fees_rs' else 'Fees +5bps round-trip slippage')
                live_rows.append(r)
    page.append(table(live_rows,[('period_label','Period','text'),('label','Policy','text'),('cost_label','Cost scenario','text'),('net_pnl_rs','Projected net PnL','money'),('trades','Trades','int'),('win_rate_pct','Win rate','pct'),('profitable_days','Profitable days','int'),('losing_days','Losing days','int'),('daily_close_drawdown_rs','Daily-close drawdown','money')],primary=True))
    page.append('<p>The fee model uses current NSE cash-intraday brokerage, STT, exchange fees, SEBI fees, stamp duty, IPFT andGST. Around₹50,000 each side, indicative round-trip fees are₹53.14 (10.63bps), while flat5bps would be₹25. At₹500,000 each side, fees are approximately₹224.61 (4.49bps). Contract-note rounding and exceptional charges are excluded. <a href="https://www.zerodha.com/charges">Official Zerodha charges</a>.</p>')
    page.append('<p>For example, TP0.60% with SL1.20% requires about72.6% full-target winners to break even at₹50,000 exposure after indicative fees, or about75.3% with an additional5bps round-trip slippage. That illustration assumes every trade exits at its full target or full stop; actual time and protection exits have different payoffs. A minimum target and maximum stop are bounds, not proof of a profitable reward/risk combination.</p>')
    page.append('<h2 id="daily">Daywise comparison</h2>')
    day_rows=[]
    for day in days:
        row={'day':day,'cohort':'Development' if day in lock['development_days'] else 'Later diagnostic'}
        for policy in POLICIES: row[policy]=daily_lookup[day,policy]['net_pnl_rs']
        row['delta']=n(row['locked_pooled'])-n(row['original_g3'])
        row['trades']=daily_lookup[day,'locked_pooled']['trades']
        day_rows.append(row)
    page.append(table(day_rows,[('day','Date','text'),('cohort','Cohort','text'),('original_g3','Original net','money'),('bounded_current','Bounded current net','money'),('locked_global','Global net','money'),('locked_pooled','Pooled net','money'),('delta','Pooled change vs original','delta'),('trades','Pooled trades','int')],scroll_class='tall'))
    page.append('<h2 id="deployment">What must change before a deployment</h2><p>This report does not change the current strategy, LIVE/PAPER parameters, dashboard sessions, broker orders or activation controls.</p><ol>')
    deployment=[
        'Version the proposed per-setup exit contract across historical, paper and live code. Current production uses1.25% initial risk and a120-minute tightening rule; fixed/protected candidates in this report are different exit policies.',
        'Implement the completed-close activation clock with persisted state. A protection stop can take effect only on the following minute, never retroactively within the candle that activated it.',
        'Recompute levels from actual average entry fill. Validate actual tick size and round directionally so intended SL remains≤1.20% and target≥0.60%. Current bracket code uses nearest-tick rounding.',
        'Preserve broker-order reconciliation and monotonic tightening. Current G3 places separate MIS SL-M and LIMIT exits and manages cancellation in software. They are not an atomic OCO bracket.',
        'Handle partially filled entries, cancellation acknowledgements, residual exits and disconnect recovery. Current partial-entry handling waits for the entry remainder to cancel before creating protection for acquired shares; that interval and exit-order races require explicit testing.',
        'Confirm each stop modification through broker state. Use few meaningful changes; Kite permits25 modifications per order. Returned order IDs alone do not prove fills or accepted protection.',
        'Account for market protection leaving unexecuted LIMIT residuals. An intended1.20% trigger distance cannot guarantee realized losses stay within1.20%.',
        'Retain independent broker position/order reconciliation and test retries without duplicate orders. GTT product support does not make GTT OCO a replacement for this MIS flow.',
        'Check the day’s RMS schedule and margin availability. The16October bulletin lists non-CAS equity auto-squareoff15:25 and CAS15:12; the strategy currently requests exit15:15. CAS handling must precede its earlier deadline.',
        'Rehearse the locked rules in paper observation with actual spreads and integer sizes. Later source sessions in this report are not prospective evidence.'
    ]
    page.append(''.join('<li>'+html.escape(x)+'</li>' for x in deployment)+'</ol>')
    page.append('<h3>Official Kite/Zerodha references checked16October2026</h3><ul>')
    for source in broker['official_sources']:
        page.append('<li><a href="'+html.escape(source['url'])+'">'+html.escape(source['supports'])+'</a></li>')
    page.append('</ul><h2 id="limits">Evidence and limits</h2><ul>')
    limits=list(summary['limitations'])+[
        protocol['structure'],
        'The TP cap2.00% belongs to the predeclared0.60–2.00% candidate range. It is not a discovery made after examining later results.',
        'The six pooled groups share parameters because many individual setups have sparse development data. Bespoke training winners remain exploratory, even when they meet the sample threshold.',
        'Selection is frozen before later candidate exits are evaluated. This prevents using later outcomes for this selection step, but does not erase prior research on the underlying strategy or source universe.',
        'Daily-close drawdown can conceal larger intraday risk. Broker latency, spread, order-book depth and queue position cannot be recovered from one-minute OHLC.',
        'A historical result is not a guarantee of future win rate, daily profit or loss limits.'
    ]
    page.append(''.join('<li>'+html.escape(x)+'</li>' for x in limits)+'</ul>')
    page.append('<p class="small">Version: '+html.escape(summary['version'])+'. Research only. Source parity refers to reproducing the original saved native fills/allocation, not certifying the original strategy as unbiased.</p>')
    page.append('''</main><script>const search=document.getElementById('setup-search');const rows=Array.from(document.querySelectorAll('#setups-table tbody tr'));const count=document.getElementById('setup-count');function filter(){let shown=0;const q=search.value.toLowerCase().trim();for(const r of rows){const match=r.textContent.toLowerCase().includes(q);r.hidden=!match;if(match)shown++;}count.textContent=shown+' of '+rows.length+' setups';}search.addEventListener('input',filter);filter();</script></body></html>''')
    content=''.join(page)
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(content,encoding='utf-8')
    print(json.dumps({'report':str(output),'setup_rows':len(setups),'days':len(days),'primary_policy':'locked_pooled','all_net':n(pooled['net_pnl_rs']),'later_net':n(pooled_later['net_pnl_rs']),'bytes':len(content.encode('utf-8'))},indent=2))


if __name__=='__main__':main()
