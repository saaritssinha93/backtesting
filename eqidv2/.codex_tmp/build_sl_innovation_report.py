"""Publish a five-rule comparison without altering any strategy configuration."""
from __future__ import annotations
import hashlib
import html
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
OUT=ROOT/'outputs/v13_sl_innovation_20261004'
SELECTED='TRAIL_A1.25_KEEP0.25_STAGED'

def read(filename):
    return json.loads((OUT/filename).read_text(encoding='utf-8'))

def money(n):return f'₹{n:,.0f}'

def table(headers,rows):
    esc=lambda x:html.escape(str(x))
    return '<div class="scroll"><table><thead><tr>'+''.join(f'<th>{esc(h)}</th>' for h in headers)+'</tr></thead><tbody>'+''.join('<tr>'+''.join(f'<td>{esc(c)}</td>' for c in row)+'</tr>' for row in rows)+'</tbody></table></div>'

def key(t):return t['segment'],t['sid'],t['setup_id']

def qualifies(r,b):
    return (r['wins']>=b['wins'] and r['positive_sessions']>=b['positive_sessions']
        and (r['wins']>b['wins'] or r['positive_sessions']>b['positive_sessions'])
        and r['net_profit_rupees']>b['net_profit_rupees']+1e-7
        and r['average_loss_magnitude_rupees']<=b['average_loss_magnitude_rupees']+1e-7
        and r['minute_close_drawdown_rupees']<=b['minute_close_drawdown_rupees']+1e-7
        and r['rule']['hard_stop']<=1.25)

def main():
    files=['progress_analysis.json','confirmation_analysis.json','volatility_analysis.json']
    research=[read(f) for f in files]
    results=[r for data in research for r in data['results']]
    assert len(results)==68
    assert len({r['rule']['name'] for r in results})==68
    baselines=research[0]['controls']
    staged=baselines[2]
    chosen=next(r for r in results if r['rule']['name']==SELECTED)
    passing=[r['rule']['name'] for r in results if qualifies(r,staged)]
    assert passing==[SELECTED],passing
    names=['Fixed 1.00%','Fixed 1.25%','Staged 120 min','Fixed 2.75%','New profit ratchet']
    finalists=[*baselines,chosen]
    dates=sorted(staged['daily_net'])
    assert len(dates)==43
    for r in [*results,*baselines]:
        assert r['trades']==85 and r['portfolio_rejected_trades']==0
        assert sorted(r['daily_net'])==dates
        assert abs(sum(r['daily_net'].values())-r['net_profit_rupees'])<1e-6
    for data in research[1:]:
        for r in data['controls']:
            b=next(v for v in baselines if v['rule']['name']==r['rule']['name'])
            assert abs(r['net_profit_rupees']-b['net_profit_rupees'])<1e-6
    base_lookup={key(t):t for t in staged['trades_detail']}
    changes=[]
    for t in chosen['trades_detail']:
        old=base_lookup[key(t)]
        delta=t['portfolio_net_profit_rupees']-old['portfolio_net_profit_rupees']
        if abs(delta)>1e-7:
            changes.append(dict(day=t['day'][:10],symbol=t['tradingsymbol'],side=t['side'],setup=t['setup_id'],
                staged_net=old['portfolio_net_profit_rupees'],new_net=t['portfolio_net_profit_rupees'],delta=delta,
                old_exit=old['exit_reason'],new_exit=t['exit_reason']))
    assert len(changes)==5
    assert abs(sum(t['delta'] for t in changes)-(chosen['net_profit_rupees']-staged['net_profit_rupees']))<1e-6
    daily=[dict(day=d,**{r['rule']['name']:r['daily_net'][d] for r in finalists}) for d in dates]
    export=dict(status='EXPLORATORY_REUSED_HISTORY_NOT_VALIDATED',window=[dates[0],dates[-1]],sessions=43,
        selected_rule=chosen['rule'],selection_gate_pass=passing,
        names=dict(zip([r['rule']['name'] for r in finalists],names)),
        results=finalists,daywise_comparison=daily,changes_vs_staged=changes,
        note='All five have85trades. Costs5bps,exposure500000rupees. Oct1excluded incomplete. No live changes.')
    (OUT/'FIVE_RULE_DAYWISE_COMPARISON.json').write_text(json.dumps(export,indent=2,allow_nan=False),encoding='utf-8')

    parts=['''<h1>V13-v10-G-2: innovative stop-loss tests</h1>
    <p class="meta">4 October 2026 · 29 July–30 September 2026 · 43 recorded sessions · 85 executed trades</p>
    <div class="callout"><h2>Result: a small improvement, not a proven best solution</h2>
    <p>The strongest balanced candidate in this new grid adds a <strong>profit-protection ratchet</strong> to the existing two-hour staged stop. It produces <strong>60 wins, ₹231,123 net and 25 profitable days</strong>. That is one more win and ₹1,206 more net than staged alone, with unchanged observed worst loss and drawdown.</p>
    <p>The gain is only 0.52%, depends heavily on one rescued trade and is weaker in September. Keep fixed 1% as the present default. Both the original staged rule and the new profit-ratchet rule remain unvalidated research candidates. These data do not justify declaring either superior for future trading or automatically replacing the current configuration.</p></div>
    <p>October 1 is excluded because its final scan is incomplete. The entire history and original target selection have already been reviewed repeatedly. No chronological split or resampling result below is an untouched validation set.</p>
    <h2>The new rule, exactly</h2>
    <ol><li>Start with the same 1.25% hard stop from the actual entry price.</li>
    <li>After 120 minutes since entry, tighten to 1% from entry, unless a tighter profit stop is already active.</li>
    <li>Wait until a <strong>completed, fully post-entry five-minute candle closes at least 1.25% in profit</strong>. Do not use a fleeting high/low touch.</li>
    <li>Then protect <strong>25% of the largest favorable five-minute closing return</strong> seen since entry. Update only after completed five-minute closes, with each update effective on the following minute. Never loosen protection.</li>
    <li>Keep targets, entries, sizing, costs, and 15:15 exit unchanged. Hard stops and targets remain active between updates. If price gaps through a stop, use the available price.</li></ol>
    <p><strong>The profit ratchet can activate before or after 120 minutes.</strong> Its qualifying-close condition and the two-hour timer operate independently. Whichever stop is tighter controls; the timer never resets a protected profit back to a loss allowance.</p>
    <p>Example: after a +1.25% qualifying close, the profit floor is +0.3125% gross from entry. If the best qualifying closing gain later becomes +2%, the floor rises to +0.50%. These are price gains before costs. For shorts, apply the same signed-return calculation in the opposite price direction.</p>
    <h2>All five rules at the same exposure</h2>
    <p>₹100,000 capital per trade at 5× gives ₹500,000 exposure. Portfolio capital is ₹1,000,000. Modeled round-trip cost is 5 bps of exposure (₹250/trade). No candidate loses a trade to capital constraints. Values below are net of those costs; loss columns show positive magnitudes.</p>''']
    parts.append(table(['Rule','Wins / 85','Win rate','Net','Profit factor','Average loss','Worst observed loss','Daily-close DD','Minute-close DD','Positive / 34 active'],
        [[label,r['wins'],f'{r["win_rate_pct"]:.2f}%',money(r['net_profit_rupees']),f'{r["profit_factor"]:.3f}',
          money(r['average_loss_magnitude_rupees']),money(r['worst_loss_rupees']),money(r['daily_close_drawdown_rupees']),
          money(r['minute_close_drawdown_rupees']),r['positive_sessions']] for label,r in zip(names,finalists)]))
    parts.append('''<p>Minute-close drawdown includes open-position P&amp;L at minute closes, not tick-level or intra-minute extremes. Nine sessions have no trades. The new rule has 25/34 profitable active days (73.53%), or 25/43 of all sessions (58.14%). Neither is the same as its 70.59% trade win rate.</p>
    <p>The new rule matches the 2.75% stop's trade win rate with a tighter initial price-risk cap and lower observed worst loss (₹6,500 versus ₹11,431), but earns ₹12,889 less. It is not superior on every objective. Compared with fixed 1%, it still permits a larger worst loss.</p>
    <h2>Full daywise comparison</h2>
    <p>Net P&amp;L in rupees. Totals use unrounded values; display rounding can cause small summation differences. Zero means no executed trades on these rows.</p>''')
    rows=[[d,*[money(r['daily_net'][d]) for r in finalists]] for d in dates]
    rows.append(['TOTAL',*[money(r['net_profit_rupees']) for r in finalists]])
    parts.append(table(['Date',*names],rows))
    parts.append('<h2>What actually changed versus staged alone</h2>')
    parts.append(table(['Date','Symbol','Side','Setup','Staged net','New net','Difference'],
        [[t['day'],t['symbol'],t['side'],t['setup'],money(t['staged_net']),money(t['new_net']),money(t['delta'])] for t in changes]))
    parts.append('''<p>Only five trades change: two improve, three worsen. PAYTM contributes +₹7,857, while cutting SAGILITY early gives back ₹6,169. Removing the PAYTM benefit turns the overall improvement negative. The chosen result has zero same-minute stop/target ambiguities and zero gap-through fills in this sample, but those risks remain possible outside it.</p>
    <h2>Stability, execution costs and risk sizing</h2>''')
    parts.append(table(['Rule','Jul–Aug wins / 53','Jul–Aug net','September wins / 32','September net'],
        [[label,r['slices']['jul_aug']['wins'],money(r['slices']['jul_aug']['net']),r['slices']['september']['wins'],money(r['slices']['september']['net'])] for label,r in zip(names,finalists)]))
    parts.append('''<p>Compared with staged alone, the new rule gains ₹1,938 in July–August but loses ₹732 in September. Its late-September results are identical. Higher uniform costs do not change net-profit differences when every variant trades the same exposure, but they reduce the number of profitable trades.</p>''')
    parts.append(table(['Total cost bps','Staged net','Staged wins','New net','New wins'],
        [[a['total_cost_bps'],money(a['net']),a['wins'],money(b['net']),b['wins']] for a,b in zip(staged['cost_stress'],chosen['cost_stress'])]))
    parts.append('''<p>A relative execution penalty of just 5 bps on the five changed exits would cost ₹1,250, slightly more than the ₹1,206 improvement. This is a sensitivity assumption, not a prediction. It illustrates how small the apparent advantage is.</p>
    <p>To keep the current fixed-1% nominal initial loss budget of ₹5,250 including modeled costs, a 1.25% initial stop requires 80.77% of current size: ₹403,846 exposure and ₹80,769 capital at 5×. The new rule's net then scales to ₹186,677, compared with fixed 1% at current size earning ₹220,659. Gaps and actual costs can exceed nominal budgets.</p>''')
    audit_path=OUT/'finalist_audit.json'
    if audit_path.exists():
        audit=read('finalist_audit.json')
        assert audit['source_sha256']==hashlib.sha256((OUT/'progress_analysis.json').read_bytes()).hexdigest()
        comparison=next(c for c in audit['comparisons'] if c['reference']==staged['rule']['name'])
        lo,hi=comparison['paired_circular_5session_block_bootstrap']['percentile_95_interval_rupees']
        parts.append(f'<p>The paired five-session-block 95% resampling interval for the new-minus-staged profit difference is <strong>{money(lo)} to {money(hi)}</strong>. It includes no improvement and substantial underperformance. This interval is not adjusted for the 68-rule selection or previous searches.</p>')
        parts.append('<p>Independent trade attribution, concentration and paired five-session-block resampling are recorded in <a href="finalist_audit.json">finalist_audit.json</a>. Resampling cannot undo selecting a rule from repeatedly inspected history.</p>')
    neighbor_path=OUT/'neighbor_analysis.json'
    if neighbor_path.exists():
        neighbors=read('neighbor_analysis.json')
        nr=neighbors['results']
        nets=[r['net_profit_rupees'] for r in nr]
        wins=[r['wins'] for r in nr]
        parts.append(f'<p>A separate, post-selection check ran {len(nr)} nearby settings (activation 1.00%–1.50%, retention 20%–30%). The original candidate remains frozen. Nearby net ranges from {money(min(nets))} to {money(max(nets))}, with {min(wins)}–{max(wins)} winners. These checks measure sensitivity; they are not independent confirmation and were not used to switch the chosen rule.</p>')
        good=[r for r in nr if qualifies(r,staged)]
        assert len(good)==15
        parts.append(f'<p>All 15 tested combinations with activation 1.25%–1.50% and retention 20%–30% pass the original historical screen. They retain 60 wins and 25 positive days, with net {money(min(r["net_profit_rupees"] for r in good))}–{money(max(r["net_profit_rupees"] for r in good))}. The gain is locally persistent but remains small. Lower activation settings can make more profit but increase average losing-trade size, failing the stated trade-off.</p>')
        parts.append('<details><summary>All post-selection neighbors</summary>')
        parts.append(table(['Rule','Wins','Net','Positive days','Average loss','Minute-close DD'],[[r['rule']['name'],r['wins'],money(r['net_profit_rupees']),r['positive_sessions'],money(r['average_loss_magnitude_rupees']),money(r['minute_close_drawdown_rupees'])] for r in nr]))
        parts.append('</details>')
    parts.append('''<h2>Rejected ideas and selection discipline</h2>
    <p>The original grid was registered before reading new P&amp;L: 36 progress/recovery rules, 8 soft-stop confirmation rules, and 24 volatility rules. Entries, targets, position size and cost assumptions remained fixed. Only one of the 68 passed the combined historical screen: improve win rate or profitable-day rate without worsening the other; improve net; do not worsen average loss or minute-close drawdown; initial hard stop no wider than 1.25%.</p>
    <p>This screen identifies a sample trade-off, not a future winner. A later neighborhood check is explicitly post-selection and does not reset the candidate.</p>
    <ul><li>Volatility-based trailing raised wins as high as 67/85 (78.82%), but the best-profit rule within that win-rate group made only ₹157,464 and had ₹5,198 average losses. Protecting too aggressively truncated profitable trends.</li>
    <li>The best soft-confirmation result made ₹221,658 with 58 wins, versus staged alone at ₹229,917 with 59 wins. Waiting for persistent weakness still cut an eventual recovery too early.</li>
    <li>Most early breakeven locks and recovery-based ratchets also reduced profit. A breakeven-price exit is a loss after costs.</li></ul>
    <details><summary>All 68 newly tested rules</summary>''')
    parts.append(table(['Rule','Wins','Positive days','Net','Average loss','Worst loss','Minute-close DD','Historical screen'],
        [[r['rule']['name'],r['wins'],r['positive_sessions'],money(r['net_profit_rupees']),money(r['average_loss_magnitude_rupees']),money(r['worst_loss_rupees']),money(r['minute_close_drawdown_rupees']),'Pass' if qualifies(r,staged) else 'Not passed'] for r in results]))
    parts.append('''</details><h2>Audit and limitations</h2>
    <ul><li>All four controls reproduce the previous net totals, daywise P&amp;Ls and drawdowns. All new rules retain the same 85 executions and report no portfolio rejection.</li>
    <li>Progress/recovery, confirmation and ATR implementations pass 11, 12 and 10 synthetic checks respectively, including timing, long/short direction, gaps and session boundaries.</li>
    <li>Decisions use completed post-entry data and become effective on following bars. The original one-minute engine uses stop-first precedence if stop and target both touch in the same bar.</li>
    <li>The entry timestamp is a candle-end proxy; real fill time is only known within the entry minute. New five-minute indicators require a full interval after that proxy. Exact intrabar ordering and live execution latency remain unknown.</li>
    <li>Adaptive stop levels use continuous prices; exchange tick-size rounding and broker-specific trigger rules are not modeled. Real stop execution is not a guaranteed cap.</li>
    <li>Every result is exploratory on reused history; original target calibration also used these data. Neither September nor resampled sessions are untouched out-of-sample evidence.</li>
    <li>No live orders, live settings, frozen configurations or earlier reports were changed.</li></ul>
    <p>General references: <a href="https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf">Bailey et al. on backtest overfitting</a>; <a href="https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-bulletins-15">Investor.gov on stop-order execution risk</a>. All numerical results above come from the local research outputs.</p>
    <h2>Reproducibility files</h2>''')
    files+=['FIVE_RULE_DAYWISE_COMPARISON.json','neighbor_analysis.json','finalist_audit.json']
    files=[f for f in files if (OUT/f).exists()]
    parts.append('<ul>'+''.join(f'<li><a href="{f}">{f}</a></li>' for f in files)+'</ul>')
    protocol=json.loads((ROOT/'.codex_tmp/sl_innovation_protocol.json').read_text())
    (OUT/'selection_protocol.json').write_text(json.dumps(protocol,indent=2),encoding='utf-8')
    parts.append('<p>Initial grid and selection rule: <a href="selection_protocol.json">selection_protocol.json</a>. Source and script fingerprints: <a href="research_manifest.json">research_manifest.json</a>.</p>')
    style='''body{font:16px/1.6 system-ui,sans-serif;background:#f4f7fa;color:#162c40;margin:0}main{max-width:1350px;margin:auto;background:white;padding:36px}h1{font-size:30px;line-height:1.25}h2{font-size:22px;margin-top:30px}.meta{color:#5b6c7e}.callout{background:#eef5ff;border-left:5px solid #2860a5;padding:10px 22px}.callout h2{margin-top:10px}.scroll{overflow-x:auto;margin:18px 0}table{border-collapse:collapse;font-size:13px;width:100%;line-height:1.5}th,td{padding:9px 10px;border-bottom:1px solid #dce3eb;white-space:nowrap;text-align:right}th{background:#e9eff7}th:first-child,td:first-child{text-align:left}tr:nth-child(even){background:#f7f9fb}details{border:1px solid #d9e1ea;padding:12px;margin:16px 0;border-radius:6px}summary{cursor:pointer;font-weight:600}a{color:#245e9a}li{margin:8px 0}@media print{main{padding:0}.scroll{overflow:visible}td,th{font-size:9px;white-space:normal;padding:3px}}'''
    document='<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>V13 G2 five-rule SL comparison</title><style>'+style+'</style></head><body><main>'+'\n'.join(parts)+'</main></body></html>'
    dest=OUT/'SL_INNOVATION_DAYWISE_REPORT.html'
    dest.write_text(document,encoding='utf-8')
    files+=['selection_protocol.json','SL_INNOVATION_DAYWISE_REPORT.html']
    manifest={f:hashlib.sha256((OUT/f).read_bytes()).hexdigest() for f in files}
    scripts=['sl_innovation_common.py','sl_innovation_progress.py','sl_innovation_confirmation.py','sl_innovation_volatility.py','sl_innovation_neighbors.py','sl_innovation_finalist_audit.py','build_sl_innovation_report.py']
    for f in scripts:
        path=ROOT/'.codex_tmp'/f
        if path.exists():manifest['.codex_tmp/'+f]=hashlib.sha256(path.read_bytes()).hexdigest()
    (OUT/'research_manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(json.dumps(dict(report=str(dest),selected=SELECTED,screen_passes=passing,all_new_rules=len(results),daily_rows=len(daily),checks='PASS'),indent=2))

if __name__=='__main__':main()
