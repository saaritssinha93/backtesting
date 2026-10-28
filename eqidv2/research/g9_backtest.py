"""Run the predefined G9 sizing experiment; never publish to G8/live/paper."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.g8_exit_engine import metrics, _flags
from research.g9_sources import load_sources, sha, ROOT, G8_ROOT, THROUGH
from research.g9_risk_permission import permission_timeline, entry_permissions, allocate

CONFIG = ROOT/'configs/strategies/v13_v10_g_9_g8_risk_permission_v1.json'
SCHEMA = 'g9_risk_permission_backtest_v1'


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False)+'\n', encoding='utf-8')


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def definition():
    d = read(CONFIG)
    if d['execution_authority'] is not False or d['scope'] != 'BACKTEST_ONLY':
        raise ValueError('G9 is backtest-only')
    if d['baseline_policy'] != 'persistent_direction_30m_2.0' or d['active_setup_count'] != 51:
        raise ValueError('G8 replica identity changed')
    if len({s['id'] for s in d['experiments']}) != len(d['experiments']):
        raise ValueError('Duplicate experiment')
    return d


def protection_snapshot():
    paths = list(G8_ROOT.glob('latest*.json')) + [
        ROOT/'configs/strategies/active_profile.json', ROOT/'fno_strategy_registry.py',
        ROOT/'fno_v13_v10_g_3_cutoff_production.py',
        ROOT/'configs/strategies/v13_v10_g_8_g7_v2_nifty_vix_v2.json']
    return {str(p):sha(p) for p in paths if p.is_file()}


def enriched_metrics(book, days):
    m = metrics(book, days)
    b = book.loc[book.day.isin(days) & _flags(book.portfolio_executed, 'portfolio_executed')]
    exposure = float(b.portfolio_exposure_rupees.sum()) if 'portfolio_exposure_rupees' in b else float(b.exposure_per_entry_rupees.sum())
    m.update(net_return_on_fixed_10lakh_pct=m['net_pnl_rs']/1e6*100,
        total_traded_exposure_rs=exposure,
        net_pnl_per_lakh_exposure_rs=m['net_pnl_rs']/exposure*100000 if exposure else 0.,
        mean_executed_quantity_scale=float(b.g9_effective_quantity_scale.mean()) if len(b) else 0.)
    return m


def attribution(base, alternative, days):
    a = base.loc[base.day.isin(days) & base.portfolio_executed].set_index('sid')
    b = alternative.loc[alternative.day.isin(days) & alternative.portfolio_executed].set_index('sid')
    removed, added, common = a.index.difference(b.index), b.index.difference(a.index), a.index.intersection(b.index)
    rp = a.loc[removed, 'portfolio_net_profit_rupees']
    cp = b.loc[common, 'portfolio_net_profit_rupees']-a.loc[common, 'portfolio_net_profit_rupees']
    ap = b.loc[added, 'portfolio_net_profit_rupees']
    delta = float(b.portfolio_net_profit_rupees.sum()-a.portfolio_net_profit_rupees.sum())
    reconciled = float(-rp.sum()+cp.sum()+ap.sum())
    if not np.isclose(delta, reconciled, atol=1e-6, rtol=0):
        raise ValueError('Attribution failed')
    loss_reduction = float(-rp[rp.lt(0)].sum()+cp.loc[a.loc[common, 'portfolio_net_profit_rupees'].lt(0)].sum())
    winner_reduction = float(rp[rp.gt(0)].sum()-cp.loc[a.loc[common, 'portfolio_net_profit_rupees'].gt(0)].sum())
    return dict(delta_net_rs=delta, removed_trades=len(removed), removed_winners=int(rp.gt(0).sum()),
        removed_losers=int(rp.lt(0).sum()), newly_allocated_trades=len(added), newly_allocated_pnl_rs=float(ap.sum()),
        loss_value_reduction_rs=loss_reduction, winner_profit_surrendered_rs=winner_reduction,
        retained_trade_pnl_change_rs=float(cp.sum()), reconciliation_error_rs=delta-reconciled)


def verify(directory):
    out = Path(directory).resolve()
    manifest = read(out/'manifest.json')
    if manifest['schema_version'] != SCHEMA:
        raise ValueError('Wrong G9 result schema')
    for name, digest in manifest['files'].items():
        path = (out/name).resolve()
        if not path.is_relative_to(out) or sha(path) != digest:
            raise ValueError('Changed G9 artifact: '+name)
    for name, digest in manifest['code'].items():
        if sha(ROOT/name) != digest:
            raise ValueError('Changed G9 implementation: '+name)
    summary = read(out/'summary.json')
    overall = pd.read_csv(out/'overall_comparison.csv').set_index(['policy', 'period'])
    monthly = pd.read_csv(out/'monthly_comparison.csv').set_index(['policy', 'month'])
    daywise = pd.read_csv(out/'daywise_all_policies.csv').set_index(['policy', 'day'])
    baseline = pd.read_csv(out/'g8_baseline/portfolio_trades.csv', low_memory=False)
    for spec in summary['experiments']:
        policy = spec['id']
        b = pd.read_csv(out/policy/'portfolio_trades.csv', low_memory=False)
        if not np.array_equal(b.sid, baseline.sid):
            raise ValueError('Candidate inventory changed')
        for col in ('entry_price', 'exit_price', 'gross_return_pct', 'stop_pct', 'target_pct'):
            if not np.allclose(b[col], baseline[col], equal_nan=True, atol=1e-10, rtol=0):
                raise ValueError('G9 changed a price exit: '+col)
        if b.execution_authority.any():
            raise ValueError('G9 cannot authorize execution')
        decisions = pd.read_csv(out/policy/'entry_permissions.csv')
        available = pd.to_datetime(decisions.available_at, utc=True)
        confirm = pd.to_datetime(decisions.confirmation_ts, utc=True)
        if ((available > confirm) & decisions.quantity_scale.gt(0)).any():
            raise ValueError('Future feature in G9')
        for period, days in summary['periods'].items():
            actual = enriched_metrics(b, days)
            for key in ('trades', 'wins', 'losses', 'net_pnl_rs', 'gross_pnl_rs', 'cost_rs', 'losing_days', 'daily_close_drawdown_rs'):
                if not np.isclose(actual[key], overall.loc[(policy, period),key], atol=1e-6, rtol=0):
                    raise ValueError('G9 accounting mismatch: '+policy+'/'+period+'/'+key)
        for month in sorted({d[:7] for d in summary['days']}):
            days = [d for d in summary['days'] if d.startswith(month)]
            value = metrics(b, days)['net_pnl_rs']
            if not np.isclose(value, monthly.loc[(policy,month),'net_pnl_rs'], atol=1e-6, rtol=0):
                raise ValueError('Monthly reconciliation')
        for day in summary['days']:
            value = metrics(b, [day])['net_pnl_rs']
            if not np.isclose(value, daywise.loc[(policy,day),'net_pnl_rs'], atol=1e-6, rtol=0):
                raise ValueError('Daily reconciliation')
    expected = read(out/'source_provenance.json')['baseline_metrics']
    for key in ('trades', 'wins', 'losses', 'net_pnl_rs', 'cost_rs'):
        if not np.isclose(expected[key], overall.loc[('g8_baseline','all'),key], atol=1e-6, rtol=0):
            raise ValueError('Copied G8 baseline parity failed')
    return True


def report(out, overall, monthly, daily, coverage, comparisons, descriptor):
    def table(f):
        f = f.copy()
        if 'trades' in f and 'win_rate_pct' in f:
            f.loc[f.trades.eq(0),'win_rate_pct'] = np.nan
        return '<div class="table">'+f.to_html(index=False, na_rep='—', float_format=lambda x:f'{x:,.2f}')+'</div>'
    primary = descriptor['primary_policy']
    cols = ['policy','trades','wins','losses','win_rate_pct','net_pnl_rs','losing_days','worst_day_rs','daily_close_drawdown_rs']
    main = ['g8_baseline','availability_only',primary,'constant_half']
    totals = overall.loc[overall.period.eq('all')].set_index('policy')
    primary_row, control_row = totals.loc[primary], totals.loc['availability_only']
    verdict = (f"On the data-eligible control, fixed G9 profit is ₹{primary_row.net_pnl_rs:,.2f} versus "
        f"₹{control_row.net_pnl_rs:,.2f} without risk scaling. Losing days are "
        f"{int(primary_row.losing_days)} versus {int(control_row.losing_days)}. "
        "These are retrospective, coverage-limited results; no diagnostic winner is automatically adopted.")
    p = ['<!doctype html><html lang="en"><meta charset="utf-8"><title>G9 risk permission experiment</title>',
        '<style>body{font:15px/1.5 system-ui;color:#26364a;margin:32px auto;max-width:1400px;padding:0 20px}h1{font-size:28px}h2{font-size:20px;margin-top:30px}.table{overflow:auto}table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #ddd;text-align:right}th{background:#edf1f6}td:first-child,th:first-child{text-align:left}a{color:#174c9c}</style>',
        '<h1>G9: G8 with volatility-based position sizing</h1>',
        '<p>Through 23 October 2026. Fixed proposed rule, not a best-history selection. G8 entry gates, all 51 setups, original slot targets and stop rules remain unchanged. No live or paper configuration changes.</p>',
        '<p>'+html.escape(verdict)+'</p>',
        '<p>Permission is 100% / 50% / 25% for zero / one / two warnings. Warnings: same-clock Nifty realized-volatility ratio &gt;1.5, or absolute completed five-minute VIX change ≥1%. A reduction is immediate; recovery requires two consecutive completed five-minute observations. The indicated state resets each session; missing inputs block new entries. Permission is frozen at confirmation for the pending order, not changed at a later fill.</p>',
        '<p>The reference requires all 20 previous same-clock observations, with no short-history substitution. Missing prior/current index data is not a calm market. The availability-only control isolates these exclusions from the sizing effect. Constant-half applies to the same availability/recovery-eligible sample, not half of the original entire G8 book. Quantity scaling alone cannot improve the win rate of an unchanged trade set. A dash for win rate means no executed trades, not a zero-percent observed success rate.</p>',
        '<h2>Full available history</h2>',table(overall.loc[overall.period.eq('all') & overall.policy.isin(main),cols]),
        '<h2>October</h2>',table(overall.loc[overall.period.eq('october') & overall.policy.isin(main),cols]),
        '<h2>Monthly comparison</h2>',table(monthly.loc[monthly.policy.isin(main),['month']+cols]),
        '<h2>Today: 23 October</h2>',table(overall.loc[overall.period.eq('today') & overall.policy.isin(main),cols]),
        '<h2>Coverage and permission</h2>',table(coverage),
        '<h2>Separating lost winners, reduced losses and new allocations</h2>',table(comparisons),
        '<h2>Predeclared diagnostics, not promoted alternatives</h2>',table(overall.loc[overall.period.eq('all'),cols]),
        '<h2>Daily comparison</h2>',table(daily),
        '<h2>Important limits</h2><p>These dates have already been used for research. Earlier/later blocks are retrospective stability checks, not independent validation. Historical after-close stock-day eligibility remains inherited. July and some early/incomplete sessions have unavailable strict 20-session reference data; inspect market_provenance.json and coverage.csv.</p>',
        '<p>Fixed ₹100,000 capital allocation per full-size entry, 5× exposure, ₹1,000,000 portfolio, inherited maximum 10 concurrent positions, 5bps modeled round-trip costs. Reallocation uses all selected candidates, not only originally executed trades. No extra daily stop or trade-count cap has been invented. Main results retain fractional theoretical quantities for G8 comparability; whole-share variants and 10/15bps costs are separate diagnostics. Drawdown is daily-close equity drawdown, not intraday mark-to-market. Actual broker costs, liquidity, circuit limits and slippage can differ.</p>',
        '<p><a href="monthly_comparison.csv">Monthly CSV</a> · <a href="daywise_comparison.csv">Daily CSV</a> · <a href="overall_comparison.csv">All policy metrics</a> · <a href="trade_comparison.csv">Every candidate and permission</a> · <a href="slot_side_comparison.csv">Slot and side results</a> · <a href="cost_sensitivity.csv">Cost checks</a> · <a href="coverage.csv">Coverage</a> · <a href="attribution.csv">Attribution</a></p>',
        '<p>Rollback for a new G9 run: <code>python fno_v13_v10_g_9_backtest.py --policy g8_baseline</code>. G8 itself was never changed. Only G9 result pointers may be published.</p></html>']
    (out/'report.html').write_text('\n'.join(p), encoding='utf-8')


def run(through=THROUGH, output=None, policy=None, publish=True):
    from research.g9_market_features import build_features
    descriptor = definition()
    if through != descriptor['through']:
        raise ValueError('This pinned revision ends at '+descriptor['through'])
    if policy is not None:
        if policy not in {x['id'] for x in descriptor['experiments']}:
            raise ValueError('Unknown policy')
        descriptor['primary_policy'] = policy
    protected = protection_snapshot()
    source = load_sources(through=through)
    outroot = Path(descriptor['output_root']).resolve()
    out = Path(output or outroot/'runs'/through/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'_g9_risk_permission')).resolve()
    if not out.is_relative_to(outroot):
        raise ValueError('G9 output must stay within its isolated output root')
    out.mkdir(parents=True, exist_ok=False)
    # Record the experiment before calculating any candidate results.
    dump(out/'experiment_plan.json',descriptor)
    dump(out/'source_provenance.json',source['provenance'])
    repair_audit = outroot/'inputs/retrieved/20261023_front_contract_gap_audit/retrieval_provenance.json'
    if repair_audit.is_file():
        dump(out/'historical_gap_retrieval_audit.json',dict(source_path=str(repair_audit),
            source_sha256=sha(repair_audit),audit=read(repair_audit)))
    config = deepcopy(source['config'])
    config.update(strategy='V13-V10-G-9', variant_id=descriptor['variant_id'],
        scope='BACKTEST_ONLY', execution_authority=False, g9_definition=descriptor)
    dump(out/'config.json',config)
    dump(out/'copied_g8_configuration.json',source['config'])
    pd.DataFrame(config['active_setups']).to_csv(out/'slot_settings.csv',index=False)
    days = source['days']
    market, provenance = build_features(days, source['vix_path'], out/'inputs')
    market['available_at'] = pd.to_datetime(market.available_at,utc=True).dt.tz_convert('Asia/Kolkata')
    market.to_csv(out/'market_observations.csv',index=False)
    dump(out/'market_provenance.json',provenance)
    months = sorted({d[:7] for d in days})
    periods = dict(all=days, october=[d for d in days if d.startswith('2026-10')],today=[through],
        earlier_research=days[:35], later_research=days[35:])
    overall, monthly, dayrows, slots, sides, cover, attributions, costs = [], [], [], [], [], [], [], []
    books, decisions = {}, {}
    for spec in descriptor['experiments']:
        name = spec['id']
        timeline = permission_timeline(market, spec, descriptor['risk_permission'])
        decision = entry_permissions(source['book'], timeline)
        book = allocate(source['book'], decision, config, name, whole_shares=spec.get('whole_shares',False))
        books[name], decisions[name] = book, decision
        dest = out/name
        dest.mkdir()
        timeline.to_csv(dest/'permission_timeline.csv',index=False)
        decision.to_csv(dest/'entry_permissions.csv',index=False)
        book.to_csv(dest/'portfolio_trades.csv',index=False)
        for period, dates in periods.items():
            overall.append(dict(policy=name,period=period,**enriched_metrics(book,dates)))
        for month in months:
            monthly.append(dict(policy=name,month=month,**enriched_metrics(book,[d for d in days if d.startswith(month)])))
        for day in days:
            dayrows.append(dict(policy=name,day=day,**metrics(book,[day])))
        for side in ['LONG','SHORT']:
            sides.append(dict(policy=name,side=side,**metrics(book.loc[book.side.eq(side)],days)))
        for setup in config['active_setups']:
            for period, dates in [('all',days),('october',periods['october'])]:
                slots.append(dict(policy=name,period=period,setup_id=setup['setup_id'],side=setup['side'],
                    **metrics(book.loc[book.setup_id.eq(setup['setup_id'])],dates)))
        print(name,metrics(book,days)['trades'],round(metrics(book,days)['net_pnl_rs'],2),flush=True)
    baseline = books['g8_baseline']
    for col in ['portfolio_net_profit_rupees','portfolio_cost_rupees','portfolio_gross_profit_rupees']:
        if not np.allclose(baseline[col],source['expected_baseline'][col],atol=1e-6,rtol=0):
            raise ValueError('G8 copy did not reproduce baseline')
    for name, book in books.items():
        for period, dates in periods.items():
            for reference in ['g8_baseline','availability_only']:
                attributions.append(dict(policy=name,reference=reference,period=period,**attribution(books[reference],book,dates)))
    selected = descriptor['primary_policy']
    d = decisions[selected]
    native = source['book'].set_index('sid')
    cohort = native.loc[native.portfolio_executed].join(d.drop(columns=['sid','day','confirmation_ts']),rsuffix='_decision')
    for month in months:
        p = cohort.loc[cohort.day.str.startswith(month)]
        cover.append(dict(month=month,baseline_executed_trades=len(p),feature_ready_trades=int(p.features_ready.fillna(False).sum()),
            permission_100pct=int(p.quantity_scale.eq(1).sum()),permission_50pct=int(p.quantity_scale.eq(.5).sum()),
            permission_25pct=int(p.quantity_scale.eq(.25).sum()),blocked_trades=int(p.quantity_scale.eq(0).sum()),
            blocked_baseline_net_rs=float(p.loc[p.quantity_scale.eq(0),'portfolio_net_profit_rupees'].sum())))
    for name in ['g8_baseline',selected]:
        for cost in [5.,10.,15.]:
            spec = next(s for s in descriptor['experiments'] if s['id']==name)
            b = allocate(source['book'],decisions[name],config,name,cost_bps=cost,whole_shares=spec.get('whole_shares',False))
            for period, dates in [('all',days),('october',periods['october'])]:
                costs.append(dict(policy=name,cost_bps=cost,period=period,**metrics(b,dates)))
    overall, monthly, dayrows = pd.DataFrame(overall),pd.DataFrame(monthly),pd.DataFrame(dayrows)
    for name, frame in [('overall_comparison',overall),('monthly_comparison',monthly),('daywise_all_policies',dayrows),
        ('slot_side_comparison',pd.DataFrame(slots)),('long_short_comparison',pd.DataFrame(sides)),
        ('coverage',pd.DataFrame(cover)),('attribution',pd.DataFrame(attributions)),('cost_sensitivity',pd.DataFrame(costs))]:
        frame.to_csv(out/(name+'.csv'),index=False)
    focus = list(dict.fromkeys(['g8_baseline','availability_only',selected,'constant_half']))
    pivot = dayrows.loc[dayrows.policy.isin(focus)].pivot(index='day',columns='policy',values=['net_pnl_rs','trades','win_rate_pct'])
    pivot.columns = [p+'_'+field for field,p in pivot.columns]
    pivot['g9_change_from_g8_rs'] = pivot[selected+'_net_pnl_rs']-pivot.g8_baseline_net_pnl_rs
    pivot['g9_change_from_available_rs'] = pivot[selected+'_net_pnl_rs']-pivot.availability_only_net_pnl_rs
    pivot.reset_index().to_csv(out/'daywise_comparison.csv',index=False)
    tradecols = ['sid','day','setup_id','tradingsymbol','side','confirmation_ts','entry_ts','exit_ts','entry_price','exit_price','exit_reason','stop_pct','target_pct','portfolio_executed','portfolio_net_profit_rupees']
    trades = source['book'][tradecols].rename(columns={'portfolio_executed':'g8_executed','portfolio_net_profit_rupees':'g8_net_pnl_rs'}).set_index('sid')
    trades = trades.join(d[['available_at','rv_ratio','vix_change_5m_pct','rv_warning','vix_warning','features_ready','quantity_scale','permission_reason']])
    new = books[selected].set_index('sid')
    trades['g9_executed'] = new.portfolio_executed
    trades['g9_net_pnl_rs'] = new.portfolio_net_profit_rupees
    trades['delta_net_rs'] = trades.g9_net_pnl_rs-trades.g8_net_pnl_rs
    trades.reset_index().to_csv(out/'trade_comparison.csv',index=False)
    summary = dict(schema_version=SCHEMA, through=through, days=days, periods=periods,
        experiments=descriptor['experiments'],primary_policy=selected, scope='BACKTEST_ONLY',execution_authority=False,
        independent_holdout=False,auto_selected_best_policy=False,baseline_reproduced=True,
        baseline_metrics=metrics(baseline,days),primary_metrics=metrics(books[selected],days),
        protected_files=protected,limitations=source['provenance']['limitations'],
        rollback_command='python fno_v13_v10_g_9_backtest.py --policy g8_baseline')
    dump(out/'summary.json',summary)
    attr = pd.DataFrame(attributions)
    report(out,overall,monthly,pivot.reset_index(),pd.DataFrame(cover),
        attr.loc[attr.policy.eq(selected)&attr.period.isin(['all','october'])],descriptor)
    if protected != protection_snapshot():
        raise ValueError('A protected G8/live file changed during G9 run')
    paths = [CONFIG,ROOT/'fno_v13_v10_g_9_backtest.py',Path(__file__),ROOT/'research/g9_sources.py',
             ROOT/'research/g9_risk_permission.py',ROOT/'research/g9_market_features.py',ROOT/'research/g8_exit_engine.py']
    dump(out/'manifest.json',dict(schema_version=SCHEMA,
        code={str(p.relative_to(ROOT)):sha(p) for p in paths},
        files={str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file()}))
    verify(out)
    if publish:
        dump(outroot/'latest_backtest.json',dict(run_dir=str(out),through=through,primary_policy=selected,
            manifest_sha256=sha(out/'manifest.json'),scope='BACKTEST_ONLY',execution_authority=False))
    print('VERIFIED',out,flush=True)
    print(overall.loc[overall.policy.isin(focus)&overall.period.isin(['all','october','today']),
        ['policy','period','trades','win_rate_pct','net_pnl_rs','losing_days','daily_close_drawdown_rs']].to_string(index=False))
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--through',default=THROUGH)
    parser.add_argument('--output')
    parser.add_argument('--policy')
    parser.add_argument('--no-publish',action='store_true')
    parser.add_argument('--plan',action='store_true')
    parser.add_argument('--verify')
    args = parser.parse_args()
    if args.plan:
        print(json.dumps(definition(),indent=2))
    elif args.verify:
        print('verified',verify(args.verify))
    else:
        run(args.through,args.output,args.policy,not args.no_publish)


if __name__ == '__main__':
    main()
