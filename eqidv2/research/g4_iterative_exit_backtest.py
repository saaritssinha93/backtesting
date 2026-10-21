"""Bounded iterative post-entry research on the pinned original G4 cohort.

Every threshold is declared before replay. Selection uses the first35 sessions;
later sessions are chronological diagnostics on history already used for G4.
No live strategy or result pointer is written.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
from itertools import combinations
import json
from pathlib import Path
import secrets
import shutil

import numpy as np
import pandas as pd

from research import g4_iterative_dataset as dataset
from research import g4_iterative_exit_rules as rules
from research import g4_iterative_pattern_model as adaptive
from research import g4_iterative_diagnostics as diagnostics
from research import g4_g3_mirror_backtest as base
from research import g4_failed_breakout_research as sealed
from research import g4_trade_monitor_backtest as previous
from research import g3_trade_monitor_backtest as exit_helper
from research import g4_bounded_exit_backtest as bounded

VERSION = 'G4-ITERATIVE-POST-ENTRY-RESEARCH-V1'
DEFAULT_DATA = base.G4_ROOT / 'research/20261016_iterative_sealed_dataset_v1'
SOURCES = [
    ('Fidelity: ATR measures volatility', 'https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr'),
    ('StockCharts: Chandelier Exit and ATR buffers', 'https://chartschool.stockcharts.com/table-of-contents/technical-indicators-and-overlays/technical-overlays/chandelier-exit'),
    ('Schwab: volume can accompany continuation or exhaustion', 'https://www.schwab.com/learn/story/ways-volume-can-help-confirm-price-trends'),
    ('TradingView: relative volume at the same historical time', 'https://www.tradingview.com/support/solutions/43000705489-relative-volume-at-time/'),
    ('Fidelity: chart patterns and failed breakouts', 'https://www.fidelity.com/bin-public/060_www_fidelity_com/documents/learning-center/Idenitfying-Chart-Patterns.pdf'),
    ('Fidelity: bar-count time exits', 'https://www.fidelity.com/products/pdf/wealthscript.pdf'),
    ('CME: partial-position exit orders', 'https://www.cmegroup.com/education/courses/futures-trading-mechanics-and-regulation/futures-order-types'),
]


def configuration(catalog):
    return dict(version=VERSION, scope='BACKTEST_ONLY', execution_authority=False,
        winner_profit_retention_minimum=.95, keep_original_entries=True,
        primary_cohort='Exact original466 portfolio-executed trades; no skipped or substituted entries',
        portfolio_check='Separately replay all533 selected orders for static selected policies',
        stage1='Predeclared causal rule families and parameters',
        stage2='First35 only: top2 per family with >=95% winner retention and no net decline; combine different-family pairs by earliest event',
        stage3='Historical-pattern adaptive selector trained only on earlier sessions; expanding20 then5-session blocks; baseline fallback',
        stage4='Per51setup diagnostics, chronological comparisons, day-cluster bootstrap and extra execution costs',
        selection='On first35: winner profit>=95%, net>=baseline, more winning trades, fewer losing trades and lower negative trade sum; >=3 changed trades. Rank loss money saved, then net, then id.',
        minimum_changed_trades=3, original_g4_initial_stop_pct=1.2,
        original_g4_tightened_stop_pct=1., tighten_after_minutes=120,
        original_g4_targets='unchanged 0.60 through 2.00 percent',
        round_trip_cost_bps=5., fixed_exposure_rs=500000.,
        exit_execution='Completed full post-entry close; following observed open; reserve capital through next bar end',
        native_priority='Original exit at/before the open wins; later intrabar extrema do not cancel a prior market-exit decision',
        cost_stress='Additional5bps on monitor exits, or5bps on every trade; costs are not a latency model',
        development_sessions=35, diagnostics_sessions=17,
        walk_forward_initial_sessions=20, walk_forward_block_sessions=5,
        bootstrap_seed=314159, bootstrap_repetitions=5000,
        policy_catalog=catalog, sources=SOURCES)


def metrics(baseline, candidate, meta, days):
    """All comparisons use the same originally executed trades."""
    mask = meta.day.isin(days)
    before, after = baseline.loc[mask], candidate.loc[mask]
    winners, losers = before.gt(1e-8), before.lt(-1e-8)
    winner_sum = float(before.loc[winners].sum())
    loss_sum = float(-before.loc[losers].sum())
    new_loss = float(-after.loc[after.lt(0)].sum())
    daily = after.groupby(meta.loc[mask, 'day']).sum().reindex(days, fill_value=0.)
    baseline_daily = before.groupby(meta.loc[mask, 'day']).sum().reindex(days, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy()]
    n = len(after)
    return dict(sessions=len(days), trades=n, wins=int(after.gt(1e-8).sum()),
        losses=int(after.lt(-1e-8).sum()), win_rate_pct=float(100*after.gt(1e-8).mean()) if n else 0.,
        net_pnl_rs=float(after.sum()), baseline_net_rs=float(before.sum()), net_change_rs=float((after-before).sum()),
        negative_trade_sum_abs_rs=new_loss, loss_saved_rs=loss_sum-new_loss,
        loss_reduction_pct=100*(loss_sum-new_loss)/loss_sum if loss_sum else np.nan,
        original_winner_profit_rs=winner_sum,
        original_winner_profit_after_rs=float(after.loc[winners].sum()),
        original_winner_profit_retention_pct=100*after.loc[winners].sum()/winner_sum if winner_sum else np.nan,
        original_winners_turned_losses=int((winners & after.lt(-1e-8)).sum()),
        original_losers_turned_winners=int((losers & after.gt(1e-8)).sum()),
        original_winners_harmed=int((winners & after.lt(before-1e-8)).sum()),
        changed_trades=int((after-before).abs().gt(1e-7).sum()),
        losing_days=int(daily.lt(-1e-8).sum()), baseline_losing_days=int(baseline_daily.lt(-1e-8).sum()),
        daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
        worst_trade_rs=float(after.min()) if n else 0.,
        baseline_losses=int(losers.sum()), baseline_wins=int(winners.sum()))


def qualifies(item):
    return bool(item['original_winner_profit_retention_pct'] >= 95.-1e-8
        and item['net_change_rs'] >= -1e-7 and item['losses'] < item['baseline_losses']
        and item['wins'] > item['baseline_wins'] and item['loss_saved_rs'] > 1e-7
        and item['changed_trades'] >= 3)


def choose(table):
    qualified = table.loc[table.apply(qualifies, axis=1)]
    if qualified.empty:
        return 'baseline'
    return str(qualified.sort_values(['loss_saved_rs', 'net_change_rs', 'policy'],
        ascending=[False, False, True], kind='stable').iloc[0].policy)


def validate_event(row, path, event):
    if not bool(row['filled']):
        raise ValueError('Unfilled order cannot be exited')
    index = int(event['execution_path_index'])
    if index <= int(row['entry_path_index'])+1 or index >= len(path['timestamp_ns']):
        raise ValueError('Noncausal or inconsistent event index')
    end = pd.Timestamp(int(path['timestamp_ns'][index]), tz='UTC').tz_convert('Asia/Kolkata')
    decision = pd.Timestamp(event['decision_at_ist'])
    open_at = end-pd.Timedelta(minutes=1)
    if (pd.Timestamp(event['modeled_execution_at_ist']) != end
            or pd.Timestamp(event['next_bar_open_at_ist']) != end-pd.Timedelta(minutes=1)
            or decision.value != int(path['timestamp_ns'][index-1])
            or decision-pd.Timedelta(minutes=1) < pd.Timestamp(row['entry_ts'])
            or decision >= pd.Timestamp(row['exit_ts'])
            or open_at < decision or open_at >= pd.Timestamp(row['exit_ts'])
            or not np.isclose(event['next_bar_open_price'], path['open'][index], rtol=0, atol=1e-12)):
        raise ValueError('Noncausal or inconsistent event')


def event_net(row, path, event):
    validate_event(row, path, event)
    sign = 1 if row['side'] == 'LONG' else -1
    return (sign*(float(event['next_bar_open_price'])/float(row['entry_price'])-1)*100-.05)*5000.


def evaluate(native, contexts, paths, catalog):
    native_index = native.set_index('sid', drop=False)
    ids = [item['id'] for item in catalog]
    base_net = pd.to_numeric(native_index.net_return_pct).fillna(0.)*5000.
    returns = pd.DataFrame({name: base_net.copy() for name in ids})
    events = {name: {} for name in ids}
    for count, row in enumerate(native.to_dict('records'), 1):
        sid = int(row['sid'])
        found = rules.events_for_trade(row, contexts[sid], paths[sid], catalog)
        for name, event in found.items():
            if event is not None:
                returns.loc[sid, name] = event_net(row, paths[sid], event)
                events[name][sid] = event
        if count % 100 == 0 or count == len(native):
            print(f'Prespecified policies: {count}/{len(native)} original orders', flush=True)
    if events['baseline']:
        raise ValueError('Baseline has exit events')
    actual = native_index.loc[native_index.portfolio_executed, 'portfolio_net_profit_rupees']
    if not np.allclose(actual, returns.loc[actual.index, 'baseline'], atol=1e-6, rtol=0):
        raise ValueError('Candidate matrix does not reproduce original accounting')
    return returns, events


def combine_candidates(catalog, returns, events, native, paths, dev_metrics):
    """Development-only pair selection; never uses later outcomes."""
    shortlisted = []
    for family, group in dev_metrics.loc[dev_metrics.family.ne('baseline')].groupby('family', sort=True):
        good = group.loc[group.original_winner_profit_retention_pct.ge(95.-1e-8)
            & group.net_change_rs.ge(-1e-7) & group.changed_trades.ge(3)]
        shortlisted += good.sort_values(['loss_saved_rs','net_change_rs','policy'],
            ascending=[False,False,True]).head(2).policy.tolist()
    families = {item['id']:item['family'] for item in catalog}
    original = native.set_index('sid').to_dict('index')
    extra, columns = [], {}
    for a, b in combinations(shortlisted, 2):
        if families[a] == families[b]:
            continue
        name = f'combo_{len(extra)+1:03d}'
        extra.append(dict(id=name, family='development_combination', kind='combined',
            params=dict(members=[a,b]), rationale='Earliest of two development-shortlisted independent exit families'))
        columns[name] = returns.baseline.copy()
        events[name] = {}
        for sid in sorted(set(events[a]) | set(events[b])):
            options = [events[x][sid] for x in (a,b) if sid in events[x]]
            event = min(options, key=lambda x:(x['execution_path_index'], x['flags'])).copy()
            events[name][sid] = event
            columns[name].loc[sid] = event_net(original[sid], paths[sid], event)
    if columns:
        returns = pd.concat([returns, pd.DataFrame(columns)], axis=1)
    return returns, events, extra, shortlisted


def comparison_table(returns, meta, catalog, days):
    rows = []
    periods = dict(all=days, development35=days[:35], later17=days[35:],
                   evaluation15=days[35:50], latest2=days[50:])
    families = {item['id']:item['family'] for item in catalog}
    for period, cohort in periods.items():
        for name in returns:
            item = metrics(returns.baseline, returns[name], meta, cohort)
            rows.append(dict(period=period, policy=name, family=families.get(name, 'adaptive'),
                qualifies=qualifies(item), **item))
    return pd.DataFrame(rows)


def selected_ledger(native, paths, events, name):
    raw = native.copy()
    raw['monitor_exit'] = False
    raw['monitor_flags'] = '[]'
    raw['monitor_decision_at_ist'] = ''
    raw['monitor_open_observed_at_ist'] = ''
    for index, row in raw.iterrows():
        event = events.get(int(row.sid))
        if event is None:
            continue
        validate_event(row, paths[int(row.sid)], event)
        exit_helper._apply_monitor_exit(raw, index, event, paths[int(row.sid)], name)
        active_stop = 1. if (pd.Timestamp(event['next_bar_open_at_ist']) >=
            pd.Timestamp(row.entry_ts)+pd.Timedelta(minutes=120)) else 1.2
        raw.loc[index, 'active_stop_pct_at_exit'] = active_stop
        raw.loc[index, 'active_stop_price'] = float(row.entry_price)*(1-(1 if row.side=='LONG' else -1)*active_stop/100)
    ledger = previous.allocate(raw, name)
    ledger['strategy_version'] = VERSION
    if name == 'baseline':
        sealed.parity(ledger, native)
    return ledger


def bootstrap_delta(before, after, meta, days):
    keep = meta.day.isin(days)
    delta = (after.loc[keep]-before.loc[keep]).groupby(meta.loc[keep,'day']).sum().reindex(days,fill_value=0.).to_numpy()
    rng = np.random.default_rng(314159)
    draws = rng.choice(delta, size=(5000,len(days)),replace=True).sum(axis=1)
    return dict(net_change_rs=float(delta.sum()), low95_rs=float(np.quantile(draws,.025)),
                high95_rs=float(np.quantile(draws,.975)), repetitions=5000,
                label='Descriptive paired-day bootstrap; no multiple-search correction or untouched validation')


def json_write(path, data):
    path.write_text(json.dumps(base.source._json_ready(data), indent=2, allow_nan=False)+'\n', encoding='utf-8')


def report(output, summary, table, setups, daily):
    selected = summary['development_selected_policy']
    display = ['baseline', selected, 'adaptive_previous_sessions']
    display = list(dict.fromkeys(display))
    show = table.loc[table.policy.isin(display), ['period','policy','trades','wins','losses','win_rate_pct',
        'negative_trade_sum_abs_rs','loss_reduction_pct','net_pnl_rs','net_change_rs',
        'original_winner_profit_retention_pct','original_winners_turned_losses','losing_days','daily_close_drawdown_rs']]
    qualified = table.loc[table.period.eq('development35') & table.qualifies]
    best = table.loc[table.period.eq('all') & table.policy.isin(qualified.policy)].sort_values('loss_saved_rs',ascending=False).head(15)
    style = 'body{background:#0c1422;color:#e5edf7;font:15px/1.55 system-ui;margin:0;padding:32px}main{max-width:1560px;margin:auto}a{color:#85cfff}table{border-collapse:collapse;font-size:12px}th,td{padding:8px;text-align:right;border-bottom:1px solid #334257;white-space:nowrap}th{background:#203149}td:first-child,th:first-child{text-align:left}.scroll{overflow:auto}section{background:#152236;padding:22px;margin:20px 0;border-radius:10px}strong{color:#a3e6ce}img{max-width:100%}'
    doc = f'<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 iterative exit research</title><style>{style}</style></head><body><main><h1>G4: iterative post-entry protection</h1>'
    doc += f'<p>Original V12 · 51 setups · {len(summary["days"])} completed sessions · {html.escape(summary["days"][0])} to {html.escape(summary["days"][-1])} · original 466 executed trades.</p>'
    doc += '<section><strong>Research result, not a live strategy change.</strong><p>'+html.escape(summary['conclusion'])+'</p>'
    doc += '<p>Your constraint: retain at least 95% of the original winners’ combined net profit. Qualification additionally requires higher win rate, lower total loss money, and no reduction in net profit on the development period.</p></section>'
    labels = {'period':'Period','policy':'Policy','trades':'Trades','wins':'Wins','losses':'Losses','win_rate_pct':'Win rate %','negative_trade_sum_abs_rs':'Total loss ₹','loss_reduction_pct':'Loss reduction %','net_pnl_rs':'Net profit ₹','net_change_rs':'Net change ₹','original_winner_profit_retention_pct':'Original winner profit retained %','original_winners_turned_losses':'Winners turned losses','losing_days':'Losing days','daily_close_drawdown_rs':'Daily-close drawdown ₹','setup_id':'Setup','baseline_losses':'Original losses'}
    doc += '<section><h2>Original trades: selected policies</h2><p>Amounts are rupees after the original 5 bps round-trip cost. Every row uses the same original trades. “Later17” is chronological diagnostic history, not an untouched holdout.</p><div class="scroll">'+show.rename(columns=labels).to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    if (output/'comparison.png').exists():
        doc += '<section><h2>Daily outcome</h2><img src="comparison.png" alt="Cumulative net profit and daily profit differences against original G4"></section>'
    doc += '<section><h2>What was tried</h2><p>'+html.escape(summary['search_description'])+'</p><div class="scroll">'+pd.DataFrame(summary['families']).to_html(index=False,border=0)+'</div><p>All candidate results, including rejected variants, are saved. The selected candidate is chosen on first35 only. Per-setup customization requires enough prior trades; no setup is fitted independently from a handful of outcomes.</p></section>'
    if not best.empty:
        doc += '<section><h2>Development-qualified candidates, full-history diagnostics</h2><p>Sorted here for inspection only; this table does not choose the policy.</p><div class="scroll">'+best[['policy','family','losses','loss_reduction_pct','net_change_rs','original_winner_profit_retention_pct']].to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>Each of the 51 original setups</h2><p>Results apply the development-selected global policy. Setup rows with little history cannot establish a reliable independent exit rule.</p><div class="scroll">'+setups.rename(columns=labels).to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    doc += '<section><h2>Execution and evidence limits</h2><ul>'+''.join('<li>'+html.escape(x)+'</li>' for x in summary['limitations'])+'</ul></section>'
    doc += '<section><h2>Method sources</h2><p>These references motivate the tested families; none demonstrates an edge for G4. One-minute parameters and time-failure conditions are custom research hypotheses. ATR variants here exit after a completed-close breach at the next open, rather than claiming exact resting Chandelier-stop fills.</p><ul>'+''.join(f'<li><a href="{url}">{html.escape(title)}</a></li>' for title,url in SOURCES)+'</ul></section>'
    artifacts = [('comparison.csv','Every policy and period'),('daily_comparison.csv','Daily results'),('setup_results.csv','All51setup results'),('trade_changes.csv','Every original trade'),('candidate_net_returns.csv','Complete candidate outcome matrix'),('policy_events.csv','Auditable first exit events'),('development_selection.json','Development decisions'),('adaptive_assignments.csv','Previous-session policy assignments'),('adaptive_folds.csv','Previous-session training evidence'),('stress_tests.csv','Execution-cost sensitivity'),('bootstrap.json','Paired-day intervals'),('config.json','Frozen initial catalogue'),('summary.json','Summary and provenance')]
    doc += '<section><h2>Files</h2><ul>'+''.join(f'<li><a href="{f}">{label}</a></li>' for f,label in artifacts)+'</ul></section></main></body></html>'
    (output/'report.html').write_text(doc,encoding='utf-8')


def plot(output, daily, selected):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    pivot = daily.pivot(index='day',columns='policy',values='net_pnl_rs')
    names = list(dict.fromkeys(['baseline',selected,'adaptive_previous_sessions']))
    fig, axes = plt.subplots(2,1,figsize=(13,7),sharex=True,gridspec_kw={'height_ratios':[2,1]})
    for name in names:
        axes[0].plot(np.arange(len(pivot)),pivot[name].cumsum()/100000,label=name,linewidth=1.8)
    axes[0].set_ylabel('Cumulative net profit (lakh rupees)')
    axes[0].legend(fontsize=8)
    axes[0].axvline(34.5,color='gray',linestyle='--')
    axes[0].set_title('Same original466 trades; first35 selection / later17 chronological diagnostics')
    axes[1].bar(np.arange(len(pivot)),pivot[selected]-pivot.baseline,color='#287aa9')
    axes[1].axhline(0,color='black',linewidth=.7)
    axes[1].axvline(34.5,color='gray',linestyle='--')
    axes[1].set_ylabel('Selected minus baseline (Rs)')
    ticks = np.arange(0,len(pivot),5)
    axes[1].set_xticks(ticks, pivot.index[ticks],rotation=35,ha='right')
    fig.tight_layout()
    fig.savefig(output/'comparison.png',dpi=160)
    plt.close(fig)


def run(output, data_dir=DEFAULT_DATA):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT/'research').resolve()):
        raise ValueError('Fresh isolated research output required')
    native, contexts, paths, proof = dataset.load(data_dir)
    days = proof['days']
    native['day'] = native.day.astype(str).str[:10]
    native = native.sort_values(['day','confirmation_ts','sid'],kind='stable').reset_index(drop=True)
    meta = native.loc[native.portfolio_executed].set_index('sid',drop=False)
    catalog = rules.policy_catalog()
    initial_ids = [x['id'] for x in catalog]
    output.mkdir(parents=True)
    modules = [__file__,dataset.__file__,rules.__file__,adaptive.__file__,diagnostics.__file__,base.__file__,sealed.__file__,previous.__file__,exit_helper.__file__,bounded.__file__,
        sealed.simulate_exit.__code__.co_filename,bounded.history.simulator.g.v9.__file__,
        bounded.history.simulator.g.v9.v5.__file__,bounded.history.simulator.g.v9.v6.__file__]
    code = {str(Path(p).resolve()):base.sha256(Path(p)) for p in modules}
    (output/'code').mkdir()
    for filename in code:
        shutil.copyfile(filename,output/'code'/Path(filename).name)
    pointers = {str(p):base.sha256(p) for p in [base.G4_ROOT/'latest/bounded_exit.json',base.G4_ROOT/'latest/g3_mirror.json'] if p.exists()}
    json_write(output/'config.json',configuration(catalog))
    config_hash = base.sha256(output/'config.json')
    diagnostics.run(output/'historical_patterns')
    returns, events = evaluate(native,contexts,paths,catalog)
    fixed = returns.loc[meta.index]
    stage1 = comparison_table(fixed,meta,catalog,days[:35])
    dev = stage1.loc[stage1.period.eq('development35')]
    returns, events, extra, shortlisted = combine_candidates(catalog,returns,events,native,paths,dev)
    catalog += extra
    fixed = returns.loc[meta.index].copy()
    table = comparison_table(fixed,meta,catalog,days)
    selected = choose(table.loc[table.period.eq('development35')])
    print(f'Development-selected policy: {selected}; {len(extra)} combined policies',flush=True)
    json_write(output/'development_selection.json',dict(shortlisted=shortlisted,combined_catalog=extra,
        selected_policy=selected,selected_parameters=next(x for x in catalog if x['id']==selected),
        training_days=days[:35],selection_did_not_use_days=days[35:]))
    print('Evaluating expanding-window historical-pattern selector',flush=True)
    model = adaptive.select_adaptive(meta, fixed[initial_ids], days)
    fixed['adaptive_previous_sessions'] = model['pnl'].reindex(fixed.index)
    if fixed.adaptive_previous_sessions.isna().any():
        raise ValueError('Adaptive returns missing trades')
    model['assignments'].to_csv(output/'adaptive_assignments.csv',index=False)
    pd.DataFrame(model['folds']).to_csv(output/'adaptive_folds.csv',index=False)
    setup_model = adaptive.select_setup_holdout(meta, fixed[initial_ids], days)
    setup_model['assignments'].to_csv(output/'setup_selection_assignments.csv',index=False)
    setup_model['folds'].to_csv(output/'setup_selection_evidence.csv',index=False)
    # Adaptive assignments on initial20 are baseline; they are not test results.
    table = comparison_table(fixed,meta,catalog,days)
    adaptive_test = metrics(fixed.baseline,fixed.adaptive_previous_sessions,meta,days[20:])
    table = pd.concat([table,pd.DataFrame([dict(period='adaptive_evaluated32',policy='adaptive_previous_sessions',
        family='adaptive',qualifies=qualifies(adaptive_test),**adaptive_test)])],ignore_index=True)
    selection_rows = table.loc[table.period.eq('all') & table.policy.eq(selected)]
    static_names = list(dict.fromkeys(['baseline',selected]))
    # Provide the best one from every family according to first35, including failures.
    for _,group in dev.groupby('family',sort=True):
        static_names.append(choose(group))
    static_names = list(dict.fromkeys(static_names))
    portfolio = []
    (output/'ledgers').mkdir()
    for name in static_names:
        book = selected_ledger(native,paths,events[name],name)
        book.to_csv(output/'ledgers'/(name+'.csv'),index=False)
        saved = base.read_frozen_csv(output/'ledgers'/(name+'.csv')).set_index('sid')
        if not np.allclose(saved.loc[meta.index,'net_profit_rupees'],returns.loc[meta.index,name],atol=1e-6,rtol=0):
            raise ValueError('Full replay differs from candidate matrix')
        for period,cohort in dict(all=days,development35=days[:35],later17=days[35:]).items():
            portfolio.append(dict(policy=name,period=period,**bounded.stats(book,cohort),
                newly_admitted=int((saved.portfolio_executed & ~native.set_index('sid').portfolio_executed & saved.day.isin(cohort)).sum()),
                originally_executed_now_rejected=int((~saved.portfolio_executed & native.set_index('sid').portfolio_executed & saved.day.isin(cohort)).sum())))
    pd.DataFrame(portfolio).to_csv(output/'portfolio_replay.csv',index=False)
    daily,changes,setups,stress = [],[],[],[]
    displayed = list(dict.fromkeys(['baseline',selected,'adaptive_previous_sessions']))
    for name in displayed:
        for day in days:
            daily.append(dict(day=day,policy=name,**metrics(fixed.baseline,fixed[name],meta,[day])))
        for sid,row in meta.iterrows():
            policy_id = model['policy_for_sid'].loc[sid] if name=='adaptive_previous_sessions' else name
            event = events[str(policy_id)].get(int(sid))
            changes.append(dict(policy=name,applied_policy=policy_id,sid=sid,day=row.day,setup_id=row.setup_id,
                symbol=row.tradingsymbol,side=row.side,baseline_net_rs=fixed.loc[sid,'baseline'],
                new_net_rs=fixed.loc[sid,name],delta_net_rs=fixed.loc[sid,name]-fixed.loc[sid,'baseline'],
                original_exit_ts=row.exit_ts,original_exit_reason=row.exit_reason,monitor_exit=event is not None,
                decision_at_ist=event['decision_at_ist'] if event else '',
                exit_execution_at_ist=event['next_bar_open_at_ist'] if event else row.exit_execution_ts,
                exit_price=event['next_bar_open_price'] if event else row.exit_price))
        flags = pd.Series({sid:bool(events[str(model['policy_for_sid'].loc[sid] if name=='adaptive_previous_sessions' else name)].get(int(sid))) for sid in meta.index})
        for mode, charge in [('original5bps',0.*flags),('extra5bps_monitor_exits',250.*flags),('extra5bps_all_trades',pd.Series(250.,index=meta.index))]:
            adjusted_before = fixed.baseline-(250. if mode=='extra5bps_all_trades' else 0.)
            for period,cohort in dict(all=days,later17=days[35:]).items():
                stress.append(dict(policy=name,cost_scenario=mode,period=period,
                    **metrics(adjusted_before,fixed[name]-charge,meta,cohort)))
    for setup in sorted(native.setup_id.unique()):
        members = meta.loc[meta.setup_id.eq(setup)]
        for period,cohort in dict(all=days,development35=days[:35],later17=days[35:]).items():
            item = metrics(fixed.loc[members.index,'baseline'],fixed.loc[members.index,selected],members,cohort)
            setups.append(dict(setup_id=setup,period=period,policy=selected,**item))
    daily,changes,setups = pd.DataFrame(daily),pd.DataFrame(changes),pd.DataFrame(setups)
    table.to_csv(output/'comparison.csv',index=False)
    daily.to_csv(output/'daily_comparison.csv',index=False)
    changes.to_csv(output/'trade_changes.csv',index=False)
    setups.to_csv(output/'setup_results.csv',index=False)
    pd.DataFrame(stress).to_csv(output/'stress_tests.csv',index=False)
    returns.reset_index().to_csv(output/'candidate_net_returns.csv',index=False)
    event_rows = [dict(policy=name,sid=sid,**event) for name,group in events.items() for sid,event in group.items()]
    pd.DataFrame(event_rows).to_csv(output/'policy_events.csv',index=False)
    intervals = {name:{period:bootstrap_delta(fixed.baseline,fixed[name],meta,cohort)
        for period,cohort in dict(all=days,later17=days[35:]).items()} for name in displayed if name!='baseline'}
    json_write(output/'bootstrap.json',intervals)
    selected_all = selection_rows.iloc[0].to_dict()
    selected_later = table.loc[table.policy.eq(selected)&table.period.eq('later17')].iloc[0].to_dict()
    if selected == 'baseline':
        conclusion = 'No candidate qualified on the first35 sessions under the requested95% winner-profit floor plus higher win rate, lower loss money, and no net-profit reduction. Original exits remain the research fallback.'
    else:
        conclusion = (f'Development selected {selected}. On all52 sessions it changes total loss money by {selected_all["loss_reduction_pct"]:.2f}% reduction and net profit by Rs{selected_all["net_change_rs"]:,.2f}; original winners retain {selected_all["original_winner_profit_retention_pct"]:.2f}%. '
            + ('The same constraints hold on the later17 sessions.' if selected_later['qualifies'] else 'The full qualification does not hold on the later17 sessions.'))
    limitations = [
        'This is a bounded exploratory search, not every trading method. Hundreds of related candidates increase selection bias.',
        'The original G4 entries/setups and previous exit research already used this history. Chronological splits and expanding-window checks are diagnostics, not untouched future validation.',
        'The primary comparison fixes the original466 executed trades, exposure and entry prices. Separate portfolio replay can admit previously rejected orders after earlier exits.',
        'No entry filters, reranking, partial sales, pyramiding, targets beyond native targets, or widened original stops are used.',
        'Entry-candle extrema cannot arm protection; completed full subsequent minutes can. Original stops and targets keep priority before a decision.',
        'One-minute OHLCV cannot recover tick order, true bid/ask order flow, spread, queue position, or broker latency. The following open is a modeled fill.',
        'The original5bps round-trip costs are retained. Extra5bps stresses do not model path-dependent execution delays or market impact.',
        'ATR, VWAP and relative volume provide context, not a certain distinction between a pullback and reversal. VWAP here approximates transactions using minute HLC3.',
        'The95% limit concerns aggregate profit of the original winning cohort, not every winner individually or a guarantee on future winners.',
        'Daily-close drawdown excludes intraday portfolio drawdown; paired-day bootstrap intervals are descriptive and unadjusted for model selection.',
        'Per-setup history is sparse. Global and broad-group rules are evaluated for every setup, but small samples do not justify fitting51 separate rules.',
        'The completed pinned cohort ends October15. Partial October16 is excluded. No strategy configuration, latest-result pointer or live/paper order was changed.'
    ]
    summary = dict(variant_id=VERSION,execution_authority=False,scope='BACKTEST_ONLY',
        created_at_ist=datetime.now(base.production.IST).isoformat(),days=days,active_setups=51,
        orders=len(native),original_executed_trades=len(meta),dataset=str(Path(data_dir).resolve()),
        dataset_manifest_sha256=base.sha256(Path(data_dir)/'manifest.json'),original_source=proof['original_source'],
        original_manifest_sha256=proof['original_manifest_sha256'],native_control_exactly_reproduced=True,
        code=code,config_sha256=config_hash,unchanged_pointers=pointers,
        stage1_policies=len(initial_ids)-1,stage2_combinations=len(extra),
        families=pd.DataFrame(catalog).groupby('family').size().rename('candidates').reset_index().to_dict('records'),
        development_selected_policy=selected,selected_policy_parameters=next(x for x in catalog if x['id']==selected),
        development_qualified=int(table.loc[table.period.eq('development35')].qualifies.sum()),
        selected_all=selected_all,selected_later17=selected_later,
        adaptive_evaluated32=adaptive_test,conclusion=conclusion,
        setup_training_decisions=setup_model['folds'].reason.value_counts().to_dict(),
        search_description=f'{len(initial_ids)-1} fixed candidates, {len(extra)} development-only two-family combinations, then expanding-window historical-pattern selection with a baseline fallback.',
        limitations=limitations,sources=SOURCES)
    json_write(output/'summary.json',summary)
    plot(output,daily,selected)
    setup_display = setups.loc[setups.period.eq('all'),['setup_id','trades','baseline_losses','losses','win_rate_pct','net_pnl_rs','net_change_rs','loss_reduction_pct','original_winner_profit_retention_pct','original_winners_turned_losses']]
    report(output,summary,table,setup_display,daily)
    for filename,digest in code.items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError('Code changed during evaluation')
    if base.sha256(output/'config.json') != config_hash:
        raise ValueError('Initial catalogue changed during evaluation')
    for filename,digest in pointers.items():
        if base.sha256(Path(filename)) != digest:
            raise ValueError('Research changed a strategy pointer')
    json_write(output/'manifest.json',dict(variant_id=VERSION,execution_authority=False,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*') if p.is_file()}))
    print(conclusion,flush=True)
    print(output,flush=True)
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--dataset',type=Path,default=DEFAULT_DATA)
    args = parser.parse_args()
    stamp = datetime.now(base.production.IST)
    run(args.output_dir or base.G4_ROOT/'research'/f'{stamp:%Y%m%dT%H%M%S}_{secrets.token_hex(3)}_iterative_exits_v1',args.dataset)
