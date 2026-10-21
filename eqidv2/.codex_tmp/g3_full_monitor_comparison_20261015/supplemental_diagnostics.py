"""Read-only supplemental analysis of a completed fixed-policy monitor run.

Does not replay, tune, or modify the source run. Extra-cost scenarios preserve
fills and allocations and deduct an additional 5 or 10 bps of executed exposure
only when the monitor changed that executed trade's exit.
"""
import argparse
import csv
import hashlib
import html
import json
import math
from pathlib import Path

POLICIES = ('baseline', 'trend_failure', 'profit_giveback', 'combined')
TITLES = {'baseline': 'Current G3', 'trend_failure': 'Trend failure',
          'profit_giveback': 'Profit giveback', 'combined': 'Combined'}
EPS = 1e-7


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))


def yes(value):
    return str(value).strip().lower() in ('true', '1', '1.0')


def num(value):
    result = float(value or 0)
    if not math.isfinite(result):
        raise ValueError(f'Nonfinite value: {value}')
    return result


def prepare(rows):
    for r in rows:
        r['_key'] = r.get('source_signal_id') or r.get('signal_id') or r['sid']
        r['_executed'] = yes(r['portfolio_executed'])
        r['_monitor'] = yes(r.get('monitor_exit')) and r['_executed']
        r['_pnl'] = num(r['portfolio_net_profit_rupees'])
        r['_cost'] = num(r['portfolio_cost_rupees'])
        r['_exposure'] = num(r['portfolio_trade_exposure_rupees'])
        if r['_executed'] and r['_exposure'] <= 0:
            raise ValueError('Executed trade has no positive exposure')
    if len({r['_key'] for r in rows}) != len(rows):
        raise ValueError('Duplicate trade identity')
    return rows


def stats(rows, days, extra_bps=0):
    done = [r for r in rows if r['_executed']]
    pnls = [(r, r['_pnl'] - (r['_exposure']*extra_bps/10000 if r['_monitor'] else 0)) for r in done]
    daily = {day: sum(p for r, p in pnls if r['day'] == day) for day in days}
    equity = peak = drawdown = 0.
    for value in daily.values():
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak-equity)
    positive = sum(p for _, p in pnls if p > EPS)
    absolute_loss = -sum(p for _, p in pnls if p < -EPS)
    wins = sum(p > EPS for _, p in pnls)
    losses = sum(p < -EPS for _, p in pnls)
    worst = min(daily, key=daily.get)
    return dict(sessions=len(days), trades=len(done), wins=wins, losing_trades=losses,
                breakeven_trades=len(done)-wins-losses,
                win_rate_pct=100*wins/len(done) if done else 0,
                positive_net_trade_sum_rs=positive, absolute_net_trade_loss_sum_rs=absolute_loss,
                net_pnl_rs=sum(p for _, p in pnls), losing_days=sum(p < -EPS for p in daily.values()),
                stop_exits=sum(r['exit_reason'] in ('STOP','TIGHTENED_STOP') for r in done),
                original_stop_exits=sum(r['exit_reason'] == 'STOP' for r in done),
                tightened_stop_exits=sum(r['exit_reason'] == 'TIGHTENED_STOP' for r in done),
                monitor_exits=sum(r['_monitor'] for r in done),
                modeled_costs_rs=sum(r['_cost'] for r in done),
                extra_monitor_exit_cost_rs=sum(r['_exposure']*extra_bps/10000 for r in done if r['_monitor']),
                worst_day=worst, worst_day_net_rs=daily[worst], daily_close_max_drawdown_rs=drawdown,
                profit_factor=positive/absolute_loss if absolute_loss else None), daily


def paired(base, rows):
    original = {r['_key']:r for r in base}
    out = dict(common_executed=0, baseline_losers_improved=0, baseline_losers_worsened=0,
               baseline_losers_turned_positive=0, paired_loser_net_improvement_rs=0.,
               baseline_winners_cut=0, baseline_winners_turned_nonpositive=0,
               winner_net_profit_reduction_rs=0., baseline_winners_improved=0,
               winner_net_profit_increase_rs=0., newly_admitted=0, newly_admitted_net_rs=0.,
               newly_rejected=0, newly_rejected_baseline_net_rs=0.)
    reconciled = 0.
    for r in rows:
        b = original[r['_key']]
        if b['_executed'] and r['_executed']:
            out['common_executed'] += 1
            delta = r['_pnl']-b['_pnl']
            reconciled += delta
            if b['_pnl'] < -EPS:
                out['paired_loser_net_improvement_rs'] += delta
                out['baseline_losers_improved'] += delta > EPS
                out['baseline_losers_worsened'] += delta < -EPS
                out['baseline_losers_turned_positive'] += r['_pnl'] > EPS
            elif b['_pnl'] > EPS:
                if delta < -EPS:
                    out['baseline_winners_cut'] += 1
                    out['winner_net_profit_reduction_rs'] -= delta
                    out['baseline_winners_turned_nonpositive'] += r['_pnl'] <= EPS
                elif delta > EPS:
                    out['baseline_winners_improved'] += 1
                    out['winner_net_profit_increase_rs'] += delta
        elif r['_executed']:
            out['newly_admitted'] += 1
            out['newly_admitted_net_rs'] += r['_pnl']
            reconciled += r['_pnl']
        elif b['_executed']:
            out['newly_rejected'] += 1
            out['newly_rejected_baseline_net_rs'] += b['_pnl']
            reconciled -= b['_pnl']
    out['paired_net_delta_reconciled_rs'] = reconciled
    return out


def write_csv(path, rows):
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def table(rows, columns):
    def cell(value):
        if isinstance(value, float):
            return f'{value:,.2f}'
        return html.escape(str(value))
    head = ''.join(f'<th>{html.escape(label)}</th>' for _,label in columns)
    body = ''.join('<tr>'+''.join(f'<td>{cell(r[key])}</td>' for key,_ in columns)+'</tr>' for r in rows)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_dir', type=Path)
    parser.add_argument('--output-dir', required=True, type=Path)
    args = parser.parse_args()
    source, output = args.run_dir.resolve(), args.output_dir.resolve()
    if output == source or source in output.parents:
        raise ValueError('Output must be outside the immutable source run')
    originals = read_csv(source/'policy_results.csv')
    original_daily = read_csv(source/'daily_comparison.csv')
    ledgers = {p:prepare(read_csv(source/'ledgers'/f'{p}.csv')) for p in POLICIES}
    baseline = ledgers['baseline']
    for policy,rows in ledgers.items():
        if {r['_key'] for r in rows} != {r['_key'] for r in baseline}:
            raise ValueError(f'{policy} drops source candidates')
    periods = {}
    for r in originals:
        if r['policy']=='baseline':
            periods[r['period']] = sorted({d['day'] for d in original_daily if r['first_date']<=d['day']<=r['last_date']})
    days = periods['all']
    risk, costs, daily_by_policy = [], [], {}
    for period, cohort in periods.items():
        b = [r for r in baseline if r['day'] in cohort]
        bm,_ = stats(b,cohort)
        for policy, rows in ledgers.items():
            selected = [r for r in rows if r['day'] in cohort]
            sm,daily = stats(selected,cohort)
            expected = next(r for r in originals if r['period']==period and r['policy']==policy)
            assert abs(sm['net_pnl_rs']-num(expected['net_pnl_rs']))<1e-5
            assert sm['trades']==int(expected['executed_trades'])
            pairing = paired(b,selected)
            delta = sm['net_pnl_rs']-bm['net_pnl_rs']
            assert abs(delta-pairing['paired_net_delta_reconciled_rs'])<1e-5
            saved = bm['absolute_net_trade_loss_sum_rs']-sm['absolute_net_trade_loss_sum_rs']
            positive_change = sm['positive_net_trade_sum_rs']-bm['positive_net_trade_sum_rs']
            assert abs(delta-saved-positive_change)<1e-5
            risk.append(dict(period=period,policy=policy,first_date=cohort[0],last_date=cohort[-1],**sm,
                             net_delta_vs_baseline_rs=delta,absolute_loss_sum_reduction_rs=saved,
                             positive_net_trade_sum_change_rs=positive_change,**pairing))
            for extra in (0,5,10):
                stressed,_ = stats(selected,cohort,extra)
                costs.append(dict(period=period,policy=policy,extra_bps_each_monitor_exit=extra,**stressed,
                                  net_delta_vs_baseline_rs=stressed['net_pnl_rs']-bm['net_pnl_rs']))
            if period=='all':
                daily_by_policy[policy]=daily
    daywise=[]
    for day in days:
        row={'day':day}
        for policy in POLICIES:
            row[f'{policy}_net_pnl_rs']=daily_by_policy[policy][day]
            if policy!='baseline':
                row[f'{policy}_delta_vs_baseline_rs']=daily_by_policy[policy][day]-daily_by_policy['baseline'][day]
        daywise.append(row)
    output.mkdir(parents=True,exist_ok=True)
    write_csv(output/'risk_comparison.csv',risk)
    write_csv(output/'daywise_net_comparison.csv',daywise)
    write_csv(output/'cost_sensitivity.csv',costs)
    notes = [
        'Fixed monitor policies only. No threshold search or strategy promotion.',
        'One-minute completed-bar observations and following-bar open fills; no synthetic 15-second candles.',
        'Portfolio allocation is recomputed by the original monitor replay, so newly admitted trades are disclosed.',
        'Absolute loss reduction plus change in the sum of positive net trades exactly reconciles the net PnL difference.',
        'A reduced count of STOP exits can simply mean losers were closed earlier under a MONITOR label; losing trades and absolute loss are reported separately.',
        'Extra 5/10 bps is deducted once per executed monitor exit using its actual allocated exposure, on top of existing modeled round-trip costs. Other fills and capital allocation are held fixed.',
        'Drawdown here uses cumulative daily-close net PnL with a zero starting reference. It does not measure intraday mark-to-market drawdown.',
        'The original 35/15 chronological split stays fixed; newer sessions appear under daily_extensions. These are diagnostic cohorts, not independent validation of previously selected strategy targets.',
        'Inherited source limitations, including historical target selection and after-close stock-day input eligibility, still apply.',
    ]
    summary={'source_run':str(source),'source_report':str(source/'report.html'),
             'source_sha256':{name:hashlib.sha256((source/name).read_bytes()).hexdigest() for name in ('policy_results.csv','daily_comparison.csv')},
             'periods':periods,'method_notes':notes,'all_metrics':[r for r in risk if r['period']=='all']}
    (output/'supplemental_summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    all_rows=[r for r in risk if r['period']=='all']
    comparisons=[('policy','Policy'),('trades','Trades'),('win_rate_pct','Win rate %'),('net_pnl_rs','Net PnL Rs'),
                 ('net_delta_vs_baseline_rs','Change Rs'),('losing_trades','Losing trades'),('losing_days','Losing days'),
                 ('stop_exits','Stop exits'),('absolute_net_trade_loss_sum_rs','Absolute losses Rs'),
                 ('positive_net_trade_sum_rs','Positive net trade sum Rs'),('daily_close_max_drawdown_rs','Daily-close drawdown Rs')]
    page='''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G3 fixed monitor policy comparison</title><style>
    :root{color-scheme:light dark;--bg:#f7f9fc;--fg:#172333;--line:#ced7e1;--panel:#fff;--head:#eaf0f6} @media(prefers-color-scheme:dark){:root{--bg:#111923;--fg:#e8eef5;--line:#405165;--panel:#1b2734;--head:#293b4f}}body{font:15px/1.55 system-ui,sans-serif;background:var(--bg);color:var(--fg);max-width:1500px;margin:24px auto;padding:0 20px}h1{font-size:28px}h2{font-size:21px;margin-top:30px}a{color:#168ce0}.scroll{overflow-x:auto;background:var(--panel);border:1px solid var(--line);border-radius:8px}table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{padding:10px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}th{background:var(--head)}th:first-child,td:first-child{text-align:left}.note{max-width:1100px}li{margin-bottom:7px}</style><body>'''
    page+='<h1>G3 fixed monitor policy comparison</h1>'
    page+=f'<p>{len(days)} sessions, {days[0]} through {days[-1]}. Rupee figures include the original modeled costs.</p>'
    page+=f'<p><a href="{html.escape((source/"report.html").as_uri())}">Original monitor replay report</a> · <a href="risk_comparison.csv">Risk comparison CSV</a> · <a href="daywise_net_comparison.csv">Daywise CSV</a> · <a href="cost_sensitivity.csv">Cost sensitivity CSV</a></p>'
    page+='<h2>Full-history results</h2>'+table(all_rows,comparisons)
    page+='<h2>Loss reduction and sacrificed profit</h2>'+table(all_rows,[('policy','Policy'),('absolute_loss_sum_reduction_rs','Absolute loss reduction Rs'),('positive_net_trade_sum_change_rs','Change in positive net trade sum Rs'),('net_delta_vs_baseline_rs','Net change Rs'),('baseline_losers_improved','Original losers improved'),('baseline_winners_cut','Original winners cut'),('winner_net_profit_reduction_rs','Profit cut from original winners Rs'),('newly_admitted','Newly admitted'),('newly_admitted_net_rs','Newly admitted net Rs')])
    page+='<h2>Chronological cohorts</h2>'+table([r for r in risk if r['period']!='all'],[('period','Cohort'),*comparisons[:6],('absolute_net_trade_loss_sum_rs','Absolute losses Rs'),('daily_close_max_drawdown_rs','Daily-close drawdown Rs')])
    page+='<h2>Additional monitor-exit cost</h2>'+table([r for r in costs if r['period']=='all'],[('policy','Policy'),('extra_bps_each_monitor_exit','Extra bps per exit'),('monitor_exits','Monitor exits'),('extra_monitor_exit_cost_rs','Extra cost Rs'),('net_pnl_rs','Net PnL Rs'),('net_delta_vs_baseline_rs','Change vs baseline Rs'),('losing_days','Losing days'),('daily_close_max_drawdown_rs','Daily-close drawdown Rs')])
    day_columns=[('day','Day')]
    for policy in POLICIES:
        day_columns.append((f'{policy}_net_pnl_rs',TITLES[policy]+' net Rs'))
        if policy!='baseline':day_columns.append((f'{policy}_delta_vs_baseline_rs','Change Rs'))
    page+='<h2>Daywise comparison</h2>'+table(daywise,day_columns)
    page+='<h2>Interpretation and limits</h2><ul class="note">'+''.join('<li>'+html.escape(n)+'</li>' for n in notes)+'</ul></body></html>'
    (output/'comparison.html').write_text(page,encoding='utf-8')
    print(json.dumps({'output_dir':str(output),'all_metrics':all_rows},indent=2))


if __name__=='__main__':
    main()
