"""Publish all relaxed results, prior-period choices and daywise comparisons."""
from __future__ import annotations
import csv
import hashlib
import html
import json
from pathlib import Path

from . import selection, policy

ROOT=Path(__file__).resolve().parent
RESULTS=ROOT/'results_20261102'
LABELS={'base_control':'Original G3','available_control':'Availability control','relaxed_and':'Relaxed AND','either_nonnegative':'Either nonnegative','either_soft':'Either soft','opposition_veto':'Opposing-pressure veto','strong_opposition_veto':'Strong opposing-pressure veto','pressure_tolerance':'Pressure tolerance'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(name):
    with (RESULTS/name).open(encoding='utf-8-sig',newline='') as stream:
        return list(csv.DictReader(stream))


def number(row,key):
    return float(row[key])


def money(value,signed=False):
    sign='−' if value<0 else '+' if signed and value>0 else ''
    return f'{sign}₹{abs(value):,.0f}'


def percent(value):
    return '—' if value is None else f'{value:.1f}%'


def table(headers,rows):
    return '<div class="scroll"><table><thead><tr>'+''.join(f'<th>{html.escape(str(v))}</th>' for v in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join(f'<td>{html.escape(str(v))}</td>' for v in row)+'</tr>' for row in rows)+'</tbody></table></div>'


def aggregate(rows):
    rows=sorted(rows,key=lambda r:r['day'])
    net=peak=drawdown=0.
    for row in rows:
        net+=number(row,'net_pnl_rs');peak=max(peak,net);drawdown=max(drawdown,peak-net)
    trades=sum(int(r['trades']) for r in rows)
    wins=sum(int(r['wins']) for r in rows)
    return dict(sessions=len(rows),trades=trades,wins=wins,losses=sum(int(r['losses']) for r in rows),win_rate_pct=100*wins/trades if trades else None,
        net_pnl_rs=net,losing_days=sum(number(r,'net_pnl_rs')<0 for r in rows),
        loss_day_pnl_abs_rs=-sum(min(0,number(r,'net_pnl_rs')) for r in rows),
        loss_trade_pnl_abs_rs=sum(number(r,'loss_trade_pnl_abs_rs') for r in rows),
        daily_close_drawdown_rs=drawdown,worst_day_rs=min([number(r,'net_pnl_rs') for r in rows] or [0]))


def pareto(rows):
    objectives={'net_pnl_rs':1,'win_rate_pct':1,'losses':-1,'losing_days':-1,'loss_day_pnl_abs_rs':-1,'loss_trade_pnl_abs_rs':-1,'daily_close_drawdown_rs':-1}
    frontier=[]
    for candidate in rows:
        dominated=False
        for other in rows:
            if other['variant']==candidate['variant']:
                continue
            differences=[sign*(other[key]-candidate[key]) for key,sign in objectives.items()]
            if all(value>=-1e-6 for value in differences) and any(value>1e-6 for value in differences):
                dominated=True;break
        if not dominated:
            frontier.append(candidate['variant'])
    return frontier


def main():
    manifest=json.loads((RESULTS/'manifest.json').read_text(encoding='utf-8'))
    for name,expected in manifest['files'].items():
        assert sha(RESULTS/name)==expected, name
    summary=json.loads((RESULTS/'summary.json').read_text(encoding='utf-8'))
    metrics={(r['variant'],r['period'],int(r['cost_bps'])):r for r in summary['overall_metrics']}
    daily=read_csv('daily_results.csv')
    groups=read_csv('matched_decision_groups.csv')
    changed={variant:any(r['variant']==variant and r['selected_changed'].lower()=='true' for r in groups) for variant in selection.RULES}
    all_metrics=[metrics[v,'all',5] for v in ('base_control',*selection.RULES)]
    best_relaxed=max(selection.RULES,key=lambda v:(metrics[v,'all',5]['net_pnl_rs'],-list(selection.RULES).index(v)))
    overall_best=max(('base_control',*selection.RULES),key=lambda v:metrics[v,'all',5]['net_pnl_rs'])
    retrospective_screen=policy.choose_policy([r for r in summary['overall_metrics'] if r['period']=='all'],changed)
    frontier=pareto(all_metrics)
    reference=metrics['available_control','all',5]
    best=metrics[best_relaxed,'all',5]
    locks=summary['calibration_locks']
    folds=[]
    rolling={cost:[] for cost in (5,9,15)}
    for month,lock in sorted(locks.items()):
        variant=lock['selected_variant']
        for cost in (5,9,15):
            own_rows=[r for r in daily if r['variant']==variant and int(r['cost_bps'])==cost and r['day'].startswith(month)]
            control_rows=[r for r in daily if r['variant']=='available_control' and int(r['cost_bps'])==cost and r['day'].startswith(month)]
            if own_rows:
                own,control=aggregate(own_rows),aggregate(control_rows)
                folds.append(dict(month=month,cutoff=lock['cutoff'],variant=variant,cost_bps=cost,own=own,control=control,net_pnl_delta_rs=own['net_pnl_rs']-control['net_pnl_rs']))
                rolling[cost].extend(own_rows)
    rolling_results=[]
    for cost,own_rows in rolling.items():
        days={r['day'] for r in own_rows}
        control=aggregate([r for r in daily if r['variant']=='available_control' and int(r['cost_bps'])==cost and r['day'] in days])
        rolling_results.append(dict(cost_bps=cost,own=aggregate(own_rows),control=control))
    concentrations=[]
    for variant in selection.RULES:
        own={r['day']:number(r,'net_pnl_rs') for r in daily if r['variant']==variant and int(r['cost_bps'])==5}
        base={r['day']:number(r,'net_pnl_rs') for r in daily if r['variant']=='available_control' and int(r['cost_bps'])==5}
        deltas=sorted(((day,own[day]-base[day]) for day in own),key=lambda x:x[1],reverse=True)
        positive=sum(max(0,value) for _,value in deltas)
        concentrations.append(dict(variant=variant,net_improvement_rs=sum(value for _,value in deltas),
            improved_days=sum(value>1e-6 for _,value in deltas),worse_days=sum(value<-1e-6 for _,value in deltas),unchanged_days=sum(abs(value)<=1e-6 for _,value in deltas),
            total_positive_daily_differences_rs=positive,largest_positive_day_share=sum(max(0,v) for _,v in deltas[:1])/positive if positive else None,
            top_three_positive_day_share=sum(max(0,v) for _,v in deltas[:3])/positive if positive else None,
            net_improvement_excluding_oct28_rs=sum(value for day,value in deltas if day!='2026-10-28'),largest_daily_differences=deltas[:3]))
    assessment=dict(best_relaxed_net_variant=best_relaxed,overall_best_net_variant=overall_best,
        interpretation='Full-history leaders and Pareto frontier are retrospective comparisons, not unseen validation or live deployment instructions',
        all_history_screen=retrospective_screen,pareto_frontier=frontier,chronological_folds=folds,
        rolling_september_october=rolling_results,daily_improvement_concentration=concentrations,
        independent_holdout=False,execution_authority=False)
    (ROOT/'research_assessment.json').write_text(json.dumps(assessment,indent=2)+'\n',encoding='utf-8')
    records=[]
    for cost in (5,9,15):
        index={(r['variant'],r['day']):r for r in daily if int(r['cost_bps'])==cost}
        for day in summary['days']:
            record={'day':day,'cost_bps':cost}
            for variant in LABELS:
                row=index[variant,day]
                for key in ('trades','wins','losses','net_pnl_rs'):
                    record[variant+'_'+key]=row[key]
            record['best_relaxed_variant']=best_relaxed
            record['best_relaxed_minus_original_pnl_rs']=number(index[best_relaxed,day],'net_pnl_rs')-number(index['base_control',day],'net_pnl_rs')
            records.append(record)
    with (ROOT/'daywise_comparison.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(records[0]));writer.writeheader();writer.writerows(records)
    def performance(period,cost=5):
        rows=[]
        for variant in ('base_control',*selection.RULES):
            r=metrics[variant,period,cost]
            rows.append([LABELS[variant],r['trades'],percent(r['win_rate_pct']),money(r['net_pnl_rs']),r['losses'],r['losing_days'],money(r['loss_day_pnl_abs_rs']),money(r['daily_close_drawdown_rs'])])
        return table(['Variant','Trades','Win rate','Net profit','Losing trades','Losing days','Loss on losing days','Daily drawdown'],rows)
    if best['net_pnl_rs']>reference['net_pnl_rs']+1e-6:
        lead=f"{LABELS[best_relaxed]} had the highest full-history profit among the six relaxed rules: {money(best['net_pnl_rs'])}, {money(best['net_pnl_rs']-reference['net_pnl_rs'],True)} versus G3. This ranking uses the complete history; the prior-period choices are shown separately below."
    elif best['net_pnl_rs']<reference['net_pnl_rs']-1e-6:
        lead=f"The best relaxed CVD rule by full-history profit was {LABELS[best_relaxed]} at {money(best['net_pnl_rs'])}. Original G3 remained ahead at {money(reference['net_pnl_rs'])}. Relaxation alone did not produce a higher-profit replacement."
    else:
        lead=f"The best relaxed CVD rule by full-history profit was {LABELS[best_relaxed]}, which matched original G3 at {money(reference['net_pnl_rs'])}. This test found no full-history profit improvement."
    fold_table=table(['Evaluation period','Training through','Rule chosen beforehand','Trades','Win rate','Net profit','Profit vs control','Losing days','Daily drawdown'],[[r['month'],r['cutoff'],LABELS[r['variant']],r['own']['trades'],percent(r['own']['win_rate_pct']),money(r['own']['net_pnl_rs']),money(r['net_pnl_delta_rs'],True),r['own']['losing_days'],money(r['own']['daily_close_drawdown_rs'])] for r in folds if r['cost_bps']==5])
    screen_names=retrospective_screen['ranked_qualified_variants']
    screening='Rules meeting every full-history improvement constraint: '+(', '.join(LABELS[v] for v in screen_names) if screen_names else 'none')+'. This is a retrospective screen, not a new policy choice.'
    old_root=ROOT.parent/'cvd_strict_20261102/results_20261102'
    old_manifest=json.loads((old_root/'manifest.json').read_text(encoding='utf-8'))
    assert sha(old_root/'summary.json')==old_manifest['files']['summary.json']
    old_summary=json.loads((old_root/'summary.json').read_text(encoding='utf-8'))
    old=next(r for r in old_summary['overall_metrics'] if r['variant']=='strict_gate' and r['period']=='all' and r['cost_bps']==5)
    comparison=table(['Version','Trades','Win rate','Net profit','Daily realized drawdown'],[[LABELS['base_control'],reference['trades'],percent(reference['win_rate_pct']),money(reference['net_pnl_rs']),money(reference['daily_close_drawdown_rs'])],['Previous strict CVD',old['trades'],percent(old['win_rate_pct']),money(old['net_pnl_rs']),money(old['daily_close_drawdown_rs'])],[LABELS[best_relaxed],best['trades'],percent(best['win_rate_pct']),money(best['net_pnl_rs']),money(best['daily_close_drawdown_rs'])]])
    concentration=table(['Rule','Days improved','Days worse','Net difference','Difference excluding Oct 28','Largest 3 days / positive gains'],[[LABELS[r['variant']],r['improved_days'],r['worse_days'],money(r['net_improvement_rs'],True),money(r['net_improvement_excluding_oct28_rs'],True),percent(None if r['top_three_positive_day_share'] is None else 100*r['top_three_positive_day_share'])] for r in concentrations])
    rules=table(['Rule','Requirement after 09:45'],[[LABELS[v],rule] for v,rule in selection.RULES.items()])
    cost_table=table(['Variant','Cost (bps)','Trades','Net profit','Win rate','Losing days','Daily drawdown'],[[LABELS[v],cost,metrics[v,'all',cost]['trades'],money(metrics[v,'all',cost]['net_pnl_rs']),percent(metrics[v,'all',cost]['win_rate_pct']),metrics[v,'all',cost]['losing_days'],money(metrics[v,'all',cost]['daily_close_drawdown_rs'])] for v in ('base_control',*selection.RULES) for cost in (5,9,15)])
    page='''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 relaxed CVD results</title><style>
*{box-sizing:border-box}body{margin:0;background:#f1f5f8;color:#193249;font:15px/1.55 system-ui,sans-serif}main{max-width:1360px;margin:auto;padding:38px 24px 60px}h1{font-size:36px;line-height:1.2;margin:8px 0 18px}h2{font-size:23px;margin:0 0 14px}.eyebrow{font-size:12px;color:#577083;letter-spacing:.12em;text-transform:uppercase}.lead{font-size:18px}.notice{padding:18px;background:#fff1ce;border-left:4px solid #b87a12;margin:24px 0}.muted{color:#587085;font-size:13px}section{background:white;border:1px solid #dce5ed;border-radius:10px;padding:24px;margin:20px 0}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:11px 13px;white-space:nowrap;text-align:right;border-bottom:1px solid #e2eaf1}th{font-size:12px;background:#edf3f8;color:#4f6679}th:first-child,td:first-child{text-align:left}.controls{display:flex;gap:14px;flex-wrap:wrap;margin:18px 0}.controls label{display:flex;flex-direction:column;font-size:12px;gap:5px}select{font:14px system-ui;padding:9px;border:1px solid #c8d4df;border-radius:6px;background:white}.pos{color:#08724f}.neg{color:#b72536}.daily{max-height:620px;overflow:auto}.daily th{position:sticky;top:0}.links{display:flex;gap:12px 22px;flex-wrap:wrap}a{color:#086a95}details summary{cursor:pointer;font-size:19px;font-weight:600;margin-bottom:16px}@media(max-width:700px){main{padding:24px 12px}section{padding:16px}h1{font-size:28px}}
</style></head><body><main><div class="eyebrow">G3 V18 · relaxed estimated CVD · isolated research</div><h1>Relaxed CVD and OR-rule results</h1><p class="lead">__LEAD__</p><div class="notice">All 62 sessions were previously viewed. These results cannot establish future profits or an optimal threshold. No live trading settings were changed.</div>
<section><h2>Original, strict and best relaxed comparison</h2>__COMPARISON__<p class="muted">29 July–30 October 2026, 5 bps total modeled round-trip friction. The new variants retain native G3 opening trades before CVD is ready at 09:45; the previous strict experiment skipped them. Compare new rules with original G3 to measure their net contribution. The matched availability control reproduced original G3 on this capture.</p></section>
<section><h2>Every tested rule · all 62 sessions</h2>__ALL__<p>__SCREEN__</p><p class="muted">Losing-day rupees sum negative daily portfolio results; losing-trade rupees and counts are recorded separately in the downloadable metrics. Drawdown uses cumulative daily realized P&amp;L. The non-dominated set across profit, win rate and loss measures is: __PARETO__. Different objectives may favor different rules.</p></section>
<section><h2>Choices made using earlier sessions only</h2>__FOLDS__<p>September's rule was locked using July–August. October's rule was locked using July–September. A rule needed at least 80% trade retention and 40 trades, no deterioration in the declared profit/loss/win-rate measures, and resilience at 15 bps costs. If none qualified, the control was retained.</p><p class="muted">This is retrospective chronological evaluation. October is one evaluation fold, even when shown in several tables. Original slot selection and earlier research had already used this history.</p></section>
<section><h2>October performance of every rule</h2>__OCTOBER__</section>
<section><h2>Day-wise comparison</h2><p class="muted">The initial selection is the highest-profit relaxed rule over the complete history. It is labeled as a retrospective comparison and is separate from the earlier-period choices above.</p><div class="controls"><label>Variant<select id="variant"></select></label><label>Cost<select id="cost"><option value="5">5 bps</option><option value="9">9 bps</option><option value="15">15 bps</option></select></label><label>Period<select id="month"><option value="">All 62 sessions</option><option value="2026-07">July</option><option value="2026-08">August</option><option value="2026-09">September</option><option value="2026-10">October</option></select></label><label>Date order<select id="order"><option value="asc">Oldest first</option><option value="desc">Newest first</option></select></label></div><p id="dailySummary"></p><div class="scroll daily"><table><thead><tr><th>Date</th><th>Trades</th><th>Wins / losses</th><th>Variant net</th><th>Original G3 net</th><th>Difference</th><th>Variant cumulative</th></tr></thead><tbody id="dailyRows"></tbody></table></div><p class="muted">Cumulative profit resets at the selected period start. Zero-trade days remain. Amounts are INR after selected costs.</p></section>
<section><h2>Daily improvement and concentration</h2>__CONCENTRATION__<p class="muted">The last column measures how much of all positive paired daily improvements came from the largest three days. It is not a share of net profit. Improvements concentrated in few days are fragile. The October 28 exclusion uses the same frozen rules.</p></section>
<section><details><summary>Cost sensitivity: 5, 9 and 15 bps</summary>__COSTS__</details></section><section><h2>Frozen rules and interpretation</h2>__RULES__<p>P is the trade-direction-adjusted recent 15-minute estimated delta/volume. A is the direction-adjusted change from the preceding 15 minutes, measured in pressure percentage points. For example, −0.05 means −5 percentage points. OR rules still require both inputs to be valid.</p><p>Before 09:45, valid native G3 trades use an explicit warmup bypass; no CVD confirmation is claimed. After warmup, missing or invalid CVD rejects the candidate. Gates run before native ranking, allowing eligible replacements while retaining original filters, quotas, dated futures OI, execution and capital rules.</p><p class="muted">CVD is estimated from Kite one-minute OHLCV close location, not observed aggressor volume. The six rules were fixed before this replay and were not retuned. Historical universe and opening-OI limitations remain. October 28 is the inherited restricted 215-stock cohort. November 2 has no sealed G3 replay.</p><div class="links"><a href="daywise_comparison.csv">Daily comparison CSV</a><a href="results_20261102/overall_comparison.csv">All risk and performance metrics</a><a href="results_20261102/monthly_results.csv">Monthly results</a><a href="results_20261102/pnl_attribution.csv">Trade attribution</a><a href="research_assessment.json">Assessment and chronological folds</a><a href="results_20261102/calibration_locks.json">Choices locked before evaluation</a><a href="methodology.json">Frozen methodology</a><a href="results_20261102/verification.json">Replay verification</a><a href="preservation_verified.json">Previous-file preservation</a><a href="README.md">Reproduction instructions</a></div></section></main>
<script>const data=__DAILY__,labels=__LABELS__,best=__BEST__;const $=id=>document.getElementById(id),money=n=>(n<0?'−':'')+'₹'+Math.abs(n).toLocaleString('en-IN',{maximumFractionDigits:2,minimumFractionDigits:2}),cell=n=>`<td class="${n<0?'neg':n>0?'pos':''}">${money(n)}</td>`;for(const [value,label] of Object.entries(labels)){const option=document.createElement('option');option.value=value;option.textContent=label;$('variant').append(option);}$('variant').value=best;const params=new URLSearchParams(location.search);for(const id of ['variant','cost','month','order']){if(params.has(id)&&[...$(id).options].some(o=>o.value===params.get(id)))$(id).value=params.get(id);}
function render(){const variant=$('variant').value,cost=+$('cost').value,month=$('month').value;const rows=data.filter(r=>r.variant===variant&&r.cost_bps===cost&&r.day.startsWith(month)).sort((a,b)=>a.day.localeCompare(b.day)),base=new Map(data.filter(r=>r.variant==='base_control'&&r.cost_bps===cost).map(r=>[r.day,r]));let total=0,control=0,trades=0,wins=0,lossdays=0;const display=rows.map(r=>{total+=r.net_pnl_rs;control+=base.get(r.day).net_pnl_rs;trades+=r.trades;wins+=r.wins;lossdays+=r.net_pnl_rs<0?1:0;return {...r,cumulative:total,base:base.get(r.day).net_pnl_rs};});if($('order').value==='desc')display.reverse();$('dailyRows').innerHTML=display.map(r=>`<tr><td>${r.day}</td><td>${r.trades}</td><td>${r.wins} / ${r.losses}</td>${cell(r.net_pnl_rs)}${cell(r.base)}${cell(r.net_pnl_rs-r.base)}${cell(r.cumulative)}</tr>`).join('');$('dailySummary').textContent=labels[variant]+': '+money(total)+' net · '+trades+' trades · '+(trades?(100*wins/trades).toFixed(1)+'% wins':'no trades')+' · '+lossdays+' losing days · '+money(total-control)+' vs original G3';}for(const id of ['variant','cost','month','order'])$(id).addEventListener('change',render);render();</script></body></html>'''
    payload=[{key:(int(r[key]) if key in ('cost_bps','trades','wins','losses') else float(r[key]) if key=='net_pnl_rs' else r[key]) for key in ('variant','cost_bps','day','trades','wins','losses','net_pnl_rs')} for r in daily]
    replacements={'__LEAD__':html.escape(lead),'__COMPARISON__':comparison,'__ALL__':performance('all'),'__SCREEN__':html.escape(screening),'__PARETO__':html.escape(', '.join(LABELS[v] for v in frontier)),'__FOLDS__':fold_table,'__OCTOBER__':performance('october'),'__CONCENTRATION__':concentration,'__COSTS__':cost_table,'__RULES__':rules,'__DAILY__':json.dumps(payload,separators=(',',':')),'__LABELS__':json.dumps(LABELS),'__BEST__':json.dumps(best_relaxed)}
    for key,value in replacements.items():
        page=page.replace(key,value)
    (ROOT/'report.html').write_text(page,encoding='utf-8')
    (ROOT/'START_HERE.html').write_text('<!doctype html><html lang="en"><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=report.html"><title>Relaxed CVD results</title><a href="report.html">Open relaxed CVD results</a></html>',encoding='utf-8')
    print(json.dumps({'report':str(ROOT/'report.html'),'best_relaxed_net_variant':best_relaxed,'overall_best_net_variant':overall_best,'pareto_frontier':frontier,'retrospective_qualified':screen_names,'locked_choices':{m:v['selected_variant'] for m,v in locks.items()}}))


if __name__=='__main__':
    main()
