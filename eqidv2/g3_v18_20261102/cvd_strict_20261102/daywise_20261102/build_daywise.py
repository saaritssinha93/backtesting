"""Create daywise views from the frozen replay; never rerun or change it."""
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT.parent / 'results_20261102'
LABELS = {'base_control':'Original G3','cvd_ready_control':'CVD-ready control','rvol_ready_control':'RVOL-ready control','aligned_gate':'Aligned CVD','strict_gate':'Strict CVD','strict_rvol_gate':'Strict CVD + volume'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest = json.loads((SOURCE/'manifest.json').read_text(encoding='utf-8'))
    for name in ('daily_results.csv','summary.json'):
        assert sha(SOURCE/name) == manifest['files'][name], f'Source changed: {name}'
    with (SOURCE/'daily_results.csv').open(encoding='utf-8-sig',newline='') as stream:
        original = list(csv.DictReader(stream))
    rows = []
    for r in original:
        rows.append({'variant':r['variant'],'cost':int(r['cost_bps']),'day':r['day'],
            'trades':int(r['trades']),'wins':int(r['wins']),'losses':int(r['losses']),
            'net':float(r['net_pnl_rs']),'costs':float(r['costs_rs'])})
    summary = json.loads((SOURCE/'summary.json').read_text(encoding='utf-8'))
    days = summary['days']
    index = {(r['variant'],r['cost'],r['day']):r for r in rows}
    assert len(index)==len(rows)==62*6*3
    checks = []
    for variant in LABELS:
        for cost in (5,9,15):
            cohort=[index[variant,cost,day] for day in days]
            expected=next(r for r in summary['overall_metrics'] if r['variant']==variant and r['cost_bps']==cost and r['period']=='all')
            cumulative=peak=max_dd=0.
            for r in cohort:
                cumulative+=r['net']; peak=max(peak,cumulative); max_dd=max(max_dd,peak-cumulative)
            assert abs(cumulative-expected['net_pnl_rs'])<1e-6
            assert sum(r['trades'] for r in cohort)==expected['trades']
            assert abs(max_dd-expected['daily_close_drawdown_rs'])<1e-6
            checks.append({'variant':variant,'cost_bps':cost,'trades':expected['trades'],'net_pnl_rs':cumulative,'max_daily_realized_drawdown_rs':max_dd})
    fields=['day','cost_bps']+[f'{variant}_{field}' for variant in LABELS for field in ('trades','wins','losses','net_pnl_rs')]+['strict_minus_cvd_ready_net_pnl_rs']
    with (ROOT/'daywise_comparison.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields); writer.writeheader()
        for cost in (5,9,15):
            for day in days:
                record={'day':day,'cost_bps':cost}
                for variant in LABELS:
                    r=index[variant,cost,day]
                    for key in ('trades','wins','losses'):
                        record[f'{variant}_{key}']=r[key]
                    record[f'{variant}_net_pnl_rs']=r['net']
                record['strict_minus_cvd_ready_net_pnl_rs']=index['strict_gate',cost,day]['net']-index['cvd_ready_control',cost,day]['net']
                writer.writerow(record)
    template=r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 CVD day-wise results</title><style>
*{box-sizing:border-box}body{margin:0;background:#f3f6fa;color:#172d42;font:15px/1.5 system-ui,sans-serif}main{max-width:1360px;margin:auto;padding:34px 24px}h1{font-size:32px;line-height:1.2;margin:8px 0 12px}h2{font-size:20px}.eyebrow{font-size:12px;color:#586d80;letter-spacing:.1em;text-transform:uppercase}.muted,small{color:#586d80}.controls,.cards{display:flex;flex-wrap:wrap;gap:14px;margin:22px 0}.controls label{display:flex;flex-direction:column;gap:5px;font-size:12px;font-weight:600}select{font:14px system-ui;padding:10px;border:1px solid #c9d5df;border-radius:6px;background:white;color:#172d42}.cards>div{flex:1;min-width:180px;padding:18px;background:white;border:1px solid #dce5ed;border-radius:9px}.cards strong{display:block;font-size:25px;margin:6px 0}.scroll{overflow:auto;border:1px solid #dce5ed;border-radius:9px;background:white;max-height:720px}table{border-collapse:collapse;width:100%;font-size:14px}th,td{text-align:right;padding:11px 14px;border-bottom:1px solid #e4ebf2;white-space:nowrap}th{background:#eaf0f6;font-size:12px;color:#475f74;position:sticky;top:0;z-index:1}th:first-child,td:first-child{text-align:left}tbody tr:hover{background:#f5f9fc}.pos{color:#08734f}.neg{color:#b32937}.neutral{color:#52677b}tfoot{font-weight:700;background:#edf3f8}a{color:#076c91}.links{display:flex;flex-wrap:wrap;gap:12px 22px;margin:18px 0}details{margin:26px 0}summary{cursor:pointer;font-size:19px;font-weight:600;margin-bottom:14px}.note{background:#fff6de;border-left:4px solid #b78223;padding:13px 16px}.small{font-size:13px}@media print{.controls{display:none}.scroll{max-height:none;overflow:visible}th{position:static}main{padding:0}body{font-size:11px}.cards{break-inside:avoid}}
</style></head><body><main><div class="eyebrow">G3 V18 · strict estimated CVD experiment</div><h1>Day-wise backtest results</h1><p>All <b>62 sealed sessions</b>, 29 July–30 October 2026. Defaults compare strict CVD with the control using the same CVD data availability. Choose a cost assumption, variant or month below.</p><div class="note small">November 2 has no sealed backtest P&amp;L. October 28 uses the inherited restricted 215-stock cohort. These are results on previously viewed history, using candle-based CVD estimates.</div>
<div class="controls"><label>Variant<select id="variant"></select></label><label>Comparison<select id="reference"></select></label><label>Total modeled round-trip cost<select id="cost"><option value="5">5 bps</option><option value="9">9 bps</option><option value="15">15 bps</option></select></label><label>Period<select id="month"><option value="">All 62 sessions</option><option value="2026-07">July 2026</option><option value="2026-08">August 2026</option><option value="2026-09">September 2026</option><option value="2026-10">October 2026</option></select></label><label>Date order<select id="order"><option value="asc">Oldest first</option><option value="desc">Newest first</option></select></label></div>
<div class="cards"><div><small id="ownLabel"></small><strong id="ownNet"></strong><span id="ownStats" class="small muted"></span></div><div><small id="refLabel"></small><strong id="refNet"></strong><span id="refStats" class="small muted"></span></div><div><small>Net profit difference</small><strong id="delta"></strong><span class="small muted">Variant minus comparison</span></div><div><small>Variant daily realized drawdown</small><strong id="drawdown"></strong><span id="periodStats" class="small muted"></span></div></div>
<h2 id="tableTitle"></h2><div class="scroll"><table><thead><tr><th>Date</th><th>Variant trades</th><th>Wins / losses</th><th>Variant net P&amp;L</th><th>Comparison trades</th><th>Comparison net P&amp;L</th><th>Difference</th><th>Variant cumulative</th></tr></thead><tbody id="daily"></tbody><tfoot id="totals"></tfoot></table></div><p class="small muted">All amounts in INR, after the selected modeled costs. Cumulative profit and drawdown start at zero at the beginning of the selected period. Zero-trade days remain included. CVD gates start at 09:45; relative-volume availability first begins August 26 in this capture.</p>
<details><summary>Compare all six variants day by day</summary><div class="scroll"><table><thead id="allHeader"></thead><tbody id="allDaily"></tbody></table></div><p class="small muted">Each cell shows net P&amp;L, followed by executed trade count. Uses the same selected period, cost and date order.</p></details>
<div class="links"><a href="outputs/g3_strict_daywise/G3_strict_CVD_daywise.xlsx">Download Excel workbook</a><a href="daywise_comparison.csv">Download daily comparison CSV</a><a href="../results_20261102/daily_results.csv">Original daily records</a><a href="../START_HERE.html">Overall strict-CVD report</a><a href="daywise_verification.json">Source and daily-total checks</a></div><p class="small muted">Strict CVD requires directional pressure ≥10% and raw acceleration ≥5 percentage points. Strict CVD + volume also requires RVOL ≥1.20×. Original strategy and previous result files are preserved.</p>
<script>
const data=__DATA__,labels=__LABELS__;
const $=id=>document.getElementById(id),money=n=>(n<0?'−':'')+'₹'+Math.abs(n).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2}),cls=n=>n>0?'pos':n<0?'neg':'neutral',cell=n=>`<td class="${cls(n)}">${money(n)}</td>`;
for(const [key,label] of Object.entries(labels)){for(const id of ['variant','reference']){const option=document.createElement('option');option.value=key;option.textContent=label;$(id).append(option);}}
$('variant').value='strict_gate';$('reference').value='cvd_ready_control';
const mapping={aligned_gate:'cvd_ready_control',strict_gate:'cvd_ready_control',strict_rvol_gate:'rvol_ready_control',base_control:'cvd_ready_control',cvd_ready_control:'base_control',rvol_ready_control:'base_control'};
const params=new URLSearchParams(location.search);for(const id of ['variant','cost','month','order']){if(params.has(id)&&[...$(id).options].some(o=>o.value===params.get(id)))$(id).value=params.get(id);} $('reference').value=mapping[$('variant').value];if(params.has('reference')&&Object.hasOwn(labels,params.get('reference')))$('reference').value=params.get('reference');
function render(){
 const variant=$('variant').value,reference=$('reference').value,cost=+$('cost').value,month=$('month').value;
 const filtered=data.filter(r=>r.cost===cost&&r.day.startsWith(month)),dates=[...new Set(filtered.map(r=>r.day))].sort(),byKey=new Map(filtered.map(r=>[r.variant+'|'+r.day,r]));
 let ownNet=0,refNet=0,trades=0,refTrades=0,wins=0,losses=0,peak=0,dd=0;
 const rendered=dates.map(day=>{const own=byKey.get(variant+'|'+day),ref=byKey.get(reference+'|'+day);ownNet+=own.net;refNet+=ref.net;trades+=own.trades;refTrades+=ref.trades;wins+=own.wins;losses+=own.losses;peak=Math.max(peak,ownNet);dd=Math.max(dd,peak-ownNet);return {day,own,ref,cumulative:ownNet};});
 if($('order').value==='desc')rendered.reverse();
 $('daily').innerHTML=rendered.map(r=>`<tr><td>${r.day}</td><td>${r.own.trades}</td><td>${r.own.wins} / ${r.own.losses}</td>${cell(r.own.net)}<td>${r.ref.trades}</td>${cell(r.ref.net)}${cell(r.own.net-r.ref.net)}${cell(r.cumulative)}</tr>`).join('');
 $('totals').innerHTML=`<tr><td>Total · ${dates.length} sessions</td><td>${trades}</td><td>${wins} / ${losses}</td>${cell(ownNet)}<td>${refTrades}</td>${cell(refNet)}${cell(ownNet-refNet)}${cell(ownNet)}</tr>`;
 $('ownLabel').textContent=labels[variant];$('refLabel').textContent=labels[reference];$('ownNet').textContent=money(ownNet);$('refNet').textContent=money(refNet);$('delta').textContent=money(ownNet-refNet);$('delta').className=cls(ownNet-refNet);$('drawdown').textContent=money(dd);$('ownStats').textContent=trades+' trades · '+(trades?(100*wins/trades).toFixed(1)+'% wins':'no executed trades');$('refStats').textContent=refTrades+' executed trades';$('periodStats').textContent=dates.length+' sessions · '+cost+' bps costs';$('tableTitle').textContent=labels[variant]+' compared with '+labels[reference];
 $('allHeader').innerHTML='<tr><th>Date</th>'+Object.values(labels).map(label=>`<th>${label}</th>`).join('')+'</tr>';
 $('allDaily').innerHTML=rendered.map(({day})=>'<tr><td>'+day+'</td>'+Object.keys(labels).map(v=>{const r=byKey.get(v+'|'+day);return `<td class="${cls(r.net)}">${money(r.net)} <small>(${r.trades})</small></td>`;}).join('')+'</tr>').join('');
}
$('variant').addEventListener('change',()=>{$('reference').value=mapping[$('variant').value];render();});for(const id of ['reference','cost','month','order'])$(id).addEventListener('change',render);render();
</script></main></body></html>'''
    body=template.replace('__DATA__',json.dumps(rows,separators=(',',':'))).replace('__LABELS__',json.dumps(LABELS))
    (ROOT/'daywise_report.html').write_text(body,encoding='utf-8')
    proof={'source_daily_sha256':sha(SOURCE/'daily_results.csv'),'source_summary_sha256':sha(SOURCE/'summary.json'),'source_rows':len(rows),'sessions':len(days),'variants':len(LABELS),'cost_scenarios':[5,9,15],'wide_csv_rows':len(days)*3,'all_totals_and_drawdowns_reconciled':True,'comparisons':checks,'execution_authority':False}
    (ROOT/'daywise_verification.json').write_text(json.dumps(proof,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'report':str(ROOT/'daywise_report.html'),'source_rows':len(rows),'sessions':len(days),'reconciled_variant_cost_totals':len(checks)}))


if __name__=='__main__':
    main()
