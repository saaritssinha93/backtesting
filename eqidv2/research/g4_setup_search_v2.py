"""Replacement-aware, chronological G4 research; never publish strategy defaults.

Training ranks entry sets, then a small fixed exit catalogue. Validation selects
one configuration per setup. October is inspected only after choices are saved.
All dates have appeared in earlier research; this is not a pristine holdout.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g4_setup_candidate_source_v2 as source
from research import g4_setup_entry_engine_v2 as entry
from research import g4_setup_exit_engine_v2 as exits
from research import g4_complete_inventory_backtest as complete
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base
from research import g4_bounded_exit_backtest as bounded

VERSION = 'G4-49-SETUP-SEARCH-V2'
EPS = 1e-6


def write_json(path, value):
    path.write_text(json.dumps(base.source._json_ready(value), indent=2,
                               allow_nan=False, default=str)+'\n', encoding='utf-8')


def metrics(before, after, days, setup_id=None):
    old = before.loc[before.day.isin(days)]
    new = after.loc[after.day.isin(days)]
    if setup_id is not None:
        old, new = (x.loc[x.setup_id.eq(setup_id)] for x in (old, new))
    a, b = bounded.stats(old, days), bounded.stats(new, days)
    old_pnl = old.set_index('sid').portfolio_net_profit_rupees.where(
        old.set_index('sid').portfolio_executed, 0.).fillna(0.)
    new_pnl = new.set_index('sid').portfolio_net_profit_rupees.where(
        new.set_index('sid').portfolio_executed, 0.).fillna(0.)
    winners = old_pnl[old_pnl.gt(EPS)]
    retained = new_pnl.reindex(winners.index, fill_value=0.)
    index = old_pnl.index.union(new_pnl.index)
    changed = new_pnl.reindex(index, fill_value=0.)-old_pnl.reindex(index, fill_value=0.)
    n, wins = b['trades'], b['wins']
    if n:
        z, p = 1.959963984540054, wins/n
        centre = (p+z*z/(2*n))/(1+z*z/n)
        half = z*np.sqrt(p*(1-p)/n+z*z/(4*n*n))/(1+z*z/n)
        low, high = 100*(centre-half), 100*(centre+half)
    else:
        low, high = None, None
    return dict(**b, **{'before_'+key: value for key, value in a.items()},
        net_delta_rs=b['net_pnl_rs']-a['net_pnl_rs'],
        loss_reduction_rs=a['negative_trade_sum_abs_rs']-b['negative_trade_sum_abs_rs'],
        original_winner_profit_rs=float(winners.sum()),
        retained_original_winner_profit_rs=float(retained.sum()),
        winner_retention_pct=float(100*retained.sum()/winners.sum()) if len(winners) else 100.,
        original_winners_removed=int((~winners.index.isin(new_pnl.index)).sum()),
        original_winners_turned_losses=int(retained.lt(-EPS).sum()),
        changed_trades=int(changed.abs().gt(EPS).sum()),
        selected_added=len(set(new.sid)-set(old.sid)), selected_removed=len(set(old.sid)-set(new.sid)),
        win_rate_wilson95_low=low, win_rate_wilson95_high=high)


def economic(row, *, positive=True):
    return (row['net_delta_rs'] > EPS if positive else row['net_delta_rs'] >= -EPS) and (
        row['loss_reduction_rs'] >= -EPS and row['winner_retention_pct'] >= 90.-EPS)


def rank_key(row):
    # Goal first, then incremental net profit. Sample adequacy is disclosed
    # separately, not silently treated as evidence of no economic improvement.
    return (-int(row['win_rate_pct'] >= 75.-EPS), -row['net_delta_rs'],
            -row['loss_reduction_rs'], row['complexity'], row['policy'])


def selection_key(frame, days=None):
    if days is not None:
        frame = frame.loc[frame.day.isin(days)]
    return tuple((int(r.sid), bool(r.v10_g_f_core)) for r in frame.sort_values('sid').itertuples())


def selected_raw(raw, selection):
    chosen = raw.set_index('sid').loc[selection.sid.astype(int)].reset_index()
    selected = selection.set_index('sid')
    for field in ('v10_g_f_core','v9_rank_in_setup_day','v9_selected','v9_filter_pass'):
        chosen[field] = chosen.sid.map(selected[field])
    return chosen


class DayAllocator:
    """Reuse identical session replays; G4 positions always close the same day.

    Only one setup varies in a search branch. Shared capital is still allocated
    against every other setup of that session. No session carries positions or
    equity-dependent sizing into the next session in the fixed G4 model.
    """
    def __init__(self, baseline):
        self.baseline=baseline
        filled=baseline.loc[baseline.filled.eq(True)]
        if not pd.to_datetime(filled.entry_ts).dt.date.equals(pd.to_datetime(filled.exit_ts).dt.date):
            raise ValueError('Session allocation cache requires same-day exits')
        self.by_day={day:frame for day,frame in baseline.groupby('day',sort=False)}
        self.cache={}
        self.session_replays=0

    def allocate(self, raw, setup_id, policy):
        changed=raw.loc[raw.setup_id.eq(setup_id)]
        source_days=set(self.baseline.loc[self.baseline.setup_id.eq(setup_id),'day'])|set(changed.day)
        pieces=[self.baseline.loc[~self.baseline.day.isin(source_days)]]
        for day in sorted(source_days):
            subset=changed.loc[changed.day.eq(day)].sort_values('sid')
            fields=['sid','filled','entry_ts','entry_price','exit_ts','exit_price','exit_reason',
                    'gross_return_pct','net_return_pct','cost_pct','initial_stop_pct',
                    'native_stop_pct','native_target_pct','v10_g_f_core','v9_rank_in_setup_day']
            signature=tuple(tuple(None if pd.isna(v) else str(v) for v in row)
                            for row in subset[fields].itertuples(index=False,name=None))
            key=(setup_id,day,signature)
            if key not in self.cache:
                self.cache[key]=exits.allocate(raw.loc[raw.day.eq(day)].copy(),policy)
                self.session_replays+=1
            pieces.append(self.cache[key])
        result=pd.concat(pieces,ignore_index=True,sort=False)
        result['monitor_policy']=policy
        return result


def prepare(output):
    output.mkdir(parents=True, exist_ok=False)
    pool, proof = source.load()
    config = proof['declared_configuration']
    guards = base.guard_limits(proof['effective_selection']['guards'])
    grid = {sid: values for sid, values in entry.configurations(config, guards).items()
            if sid in source.SETUP_IDS}
    days = proof['days']
    plan = dict(version=VERSION, days=days, train=days[:35], validation=days[35:43],
        final_historical_diagnostic=days[43:], winner_profit_retention_min_pct=90,
        win_rate_goal_pct=75, costs_bps=[5,10,15], grid=grid,
        exits={sid:[v.configuration() for v in exits.variants_for_setup(values[0]['active']['target_pct'])]
               for sid,values in grid.items()},
        selection='Top three distinct training entry sets with positive net delta, non-increased loss money, 90% winner retention and at least four train trades; always include baseline. Apply fixed exits. From train-qualified pairs choose using non-worsening validation economics and retention. Rank observed 75% goal, net delta, loss reduction, simplicity. No October selection.',
        evidence_minimums=dict(train_trades=8, validation_trades=3, october_trades=4),
        evidence_note='These small sample floors only separate watchlist from stronger historical evidence; they do not establish a future 75% win rate.',
        no_fresh_holdout=True, defaults_changed=False,
        configuration_discrepancies=proof['configuration_discrepancies'])
    write_json(output/'search_plan.json', plan)
    selector = entry.PreparedSelection(pool, config, guards)
    selections = {sid:{v['name']:selector.select(sid,v) for v in variants}
                  for sid,variants in grid.items()}
    original = base.read_frozen_csv(source.BASELINE_RUN/'g4_bounded_exit/portfolio_trades.csv')
    baseline_selection = pd.concat([selections[sid]['baseline'] for sid in grid], ignore_index=True)
    old49 = original.loc[original.setup_id.isin(grid)]
    if base.selected_keys(baseline_selection) != base.selected_keys(old49):
        raise ValueError('Research baseline selection differs from saved G4')
    for sid, chosen in selections.items():
        a = chosen['baseline'].set_index('sid').v10_g_f_core.astype(bool).sort_index()
        b = old49.loc[old49.setup_id.eq(sid)].set_index('sid').v10_g_f_core.astype(bool).sort_index()
        if not a.equals(b):
            raise ValueError('Baseline core allocation priority differs: '+sid)
    union = pd.concat([original, *[f for values in selections.values() for f in values.values()]],
                      ignore_index=True, sort=False).drop_duplicates('sid', keep='first')
    union = complete.Identity(config).identify(union)
    union.to_pickle(output/'union_orders.pkl')
    pd.to_pickle(selections, output/'selections.pkl')
    prices = complete.Prices(proof['g3_summary'])
    paths = {}
    for index, (_, row) in enumerate(union.sort_values(['tradingsymbol','day']).iterrows(), 1):
        frame, _ = prices.frame(pd.Timestamp(row.day).date(), row.tradingsymbol)
        confirm = frame.loc[frame.ts.eq(pd.Timestamp(row.confirmation_ts))]
        if len(confirm) != 1 or any(not np.isclose(confirm.iloc[0][field], row['confirmation_'+field], atol=.001, rtol=0)
                                    for field in ('open','high','low','close')):
            raise ValueError('Candidate confirmation OHLC mismatch: '+str(row.sid))
        paths.update(prices.paths(pd.DataFrame([row])))
        if index % 100 == 0:
            print(f'Validated minute paths {index}/{len(union)}', flush=True)
    np.savez_compressed(output/'paths.npz', **{f'{sid}_{key}':value for sid,path in paths.items() for key,value in path.items()})
    write_json(output/'price_proof.json', prices.proof)
    raw = sealed.replay(union, paths, 'current_g4')
    raw.to_pickle(output/'union_native.pkl')
    baseline = exits.allocate(selected_raw(raw, original), 'baseline')
    sealed.parity(baseline, original)
    baseline.to_pickle(output/'baseline.pkl')
    baseline.to_csv(output/'baseline_trades.csv', index=False)
    write_json(output/'preparation.json', dict(baseline_exact_parity=True, candidates=len(pool),
        fixed_gate_valid=int(pool.source_gate_pass.sum()), original_orders=len(original),
        union_paths=len(paths), numerical_entry_configurations=sum(map(len,grid.values())),
        distinct_full_entry_sets=sum(len({selection_key(f) for f in values.values()}) for values in selections.values()),
        source_manifest_sha256=source.BASELINE_MANIFEST_SHA256,
        pool_manifest_sha256=base.sha256(source.DEFAULT_CACHE/'manifest.json'),
        search_plan_sha256=base.sha256(output/'search_plan.json')))
    print('Exact selection, fills, exits and portfolio baseline parity passed.', flush=True)


def load_paths(path):
    result = {}
    with np.load(path) as data:
        for key in data.files:
            sid, field = key.split('_', 1)
            result.setdefault(int(sid), {})[field] = data[key]
    return result


def search(output):
    if (output/'manifest.json').exists():
        raise ValueError('Completed research is sealed; use a fresh output directory')
    plan = base.read(output/'search_plan.json')
    preparation = base.read(output/'preparation.json')
    if base.sha256(output/'search_plan.json') != preparation['search_plan_sha256']:
        raise ValueError('Frozen plan changed')
    raw, baseline = pd.read_pickle(output/'union_native.pkl'), pd.read_pickle(output/'baseline.pkl')
    selections, paths = pd.read_pickle(output/'selections.pkl'), load_paths(output/'paths.npz')
    train, val = plan['train'], plan['validation']
    allocator=DayAllocator(baseline)
    train_results, pair_results, decisions = [], [], []
    choices, selected_books, entries_tested, pairs_tested = {}, {}, 0, 0
    for setup_id, variants in plan['grid'].items():
        other = baseline.loc[~baseline.setup_id.eq(setup_id)]
        book_cache, entry_rows = {}, []
        lookup = {v['name']:v for v in variants}
        for variant in variants:
            selection = selections[setup_id][variant['name']]
            key = selection_key(selection)
            if key not in book_cache:
                combined = pd.concat([other, selected_raw(raw, selection)], ignore_index=True, sort=False)
                book_cache[key] = allocator.allocate(combined,setup_id,variant['name'])
                entries_tested += 1
            book = book_cache[key]
            row = dict(setup_id=setup_id, policy=variant['name'], complexity=variant['complexity'],
                       **metrics(baseline, book, train, setup_id))
            row['portfolio_net_delta_rs'] = metrics(baseline, book, train)['net_delta_rs']
            entry_rows.append(row)
        train_results.extend(entry_rows)
        eligible = sorted([r for r in entry_rows if economic(r) and r['trades'] >= 4
                           and r['portfolio_net_delta_rs'] > EPS], key=rank_key)
        shortlist, seen = ['baseline'], {selection_key(selections[setup_id]['baseline'], train)}
        for row in eligible:
            key = selection_key(selections[setup_id][row['policy']], train)
            if key not in seen:
                shortlist.append(row['policy'])
                seen.add(key)
            if len(shortlist) == 4:
                break
        pairs, candidates = {}, []
        for entry_name in shortlist:
            selection = selections[setup_id][entry_name]
            entry_raw = pd.concat([other, selected_raw(raw, selection)], ignore_index=True, sort=False)
            for exit_config in plan['exits'][setup_id]:
                exit_variant = exits.ExitVariant(**exit_config)
                name = entry_name+'__'+exit_variant.name
                changed = exits.replay_raw(entry_raw, paths, setup_id, exit_variant,
                                           verify_baseline=False)
                book = allocator.allocate(changed,setup_id,name)
                pairs[name] = book
                pairs_tested += 1
                complexity = lookup[entry_name]['complexity']+int(exit_variant.name!='baseline')
                tm = dict(setup_id=setup_id, policy=name, period='train', complexity=complexity,
                          **metrics(baseline, book, train, setup_id))
                vm = dict(setup_id=setup_id, policy=name, period='validation', complexity=complexity,
                          **metrics(baseline, book, val, setup_id))
                pair_results.extend([tm, vm])
                portfolio_train = metrics(baseline, book, train)['net_delta_rs']
                portfolio_val = metrics(baseline, book, val)['net_delta_rs']
                if economic(tm) and tm['trades'] >= 4 and portfolio_train > EPS and economic(vm, positive=False) and portfolio_val >= -EPS:
                    candidates.append((vm, tm, entry_name, exit_config))
        if candidates:
            candidates.sort(key=lambda x:(*rank_key(x[0])[:3], *rank_key(x[1])[:3], x[0]['complexity'], x[0]['policy']))
            vm, tm, entry_name, exit_config = candidates[0]
            chosen = vm['policy']
            reason = 'TRAIN_IMPROVED_VALIDATION_NONWORSENING'
        else:
            chosen, entry_name, exit_config = 'baseline__baseline', 'baseline', plan['exits'][setup_id][0]
            reason = 'NO_PAIR_PASSED_TRAIN_AND_VALIDATION_ECONOMICS'
        choices[setup_id] = dict(policy=chosen, entry=lookup[entry_name], exit=exit_config,
                                reason=reason, shortlisted_entries=shortlist)
        selected_books[setup_id] = pairs[chosen]
        decisions.append(dict(setup_id=setup_id, policy=chosen, reason=reason,
            entry_variants=len(variants), distinct_training_sets=len({selection_key(f,train) for f in selections[setup_id].values()}),
            exit_entry_pairs=len(pairs)))
        print(f'{setup_id}: evaluated {len(variants)} entry settings, {len(pairs)} pairs; frozen {chosen}', flush=True)
    # Freeze decisions before looking at final historical diagnostic metrics.
    write_json(output/'frozen_choices.json', choices)
    # Independently reallocate the entire 51-setup book for every frozen choice,
    # checking that the session cache did not alter capital competition.
    for sid,book in selected_books.items():
        sealed.parity(exits.allocate(book,'independent_full_allocation'),book)
    print('All 49 frozen choices passed independent full-book allocation parity.',flush=True)
    pd.DataFrame(train_results).to_csv(output/'entry_training_results.csv', index=False)
    pd.DataFrame(pair_results).to_csv(output/'pair_train_validation_results.csv', index=False)
    pd.DataFrame(decisions).to_csv(output/'decisions.csv', index=False)
    periods = dict(train=train, validation=val, october=plan['final_historical_diagnostic'],
                   today=plan['days'][-1:], all=plan['days'])
    comparisons = [dict(setup_id=sid, policy=choices[sid]['policy'], period=period,
                       **metrics(baseline, book, days, sid))
                   for sid,book in selected_books.items() for period,days in periods.items()]
    table = pd.DataFrame(comparisons)
    table.to_csv(output/'setup_comparison.csv', index=False)
    combined_raw = baseline.loc[~baseline.setup_id.isin(choices)].copy()
    for sid,book in selected_books.items():
        combined_raw = pd.concat([combined_raw,book.loc[book.setup_id.eq(sid)]], ignore_index=True, sort=False)
    combined = exits.allocate(combined_raw, 'all_frozen_candidates')
    combined.to_csv(output/'combined_candidate_trades.csv', index=False)
    combined.loc[combined.day.ge('2026-10-01')].to_csv(output/'october_candidate_trades.csv', index=False)
    portfolio_rows = [dict(period=period, **metrics(baseline, combined, days)) for period,days in periods.items()]
    pd.DataFrame(portfolio_rows).to_csv(output/'portfolio_comparison.csv', index=False)
    original49_before=baseline.loc[baseline.setup_id.isin(choices)]
    original49_after=combined.loc[combined.setup_id.isin(choices)]
    original49_rows=[dict(period=period,**metrics(original49_before,original49_after,days))
                     for period,days in periods.items()]
    pd.DataFrame(original49_rows).to_csv(output/'original49_comparison.csv',index=False)
    pd.DataFrame([dict(day=day,**metrics(baseline,combined,[day])) for day in plan['days']]).to_csv(
        output/'daily_comparison.csv',index=False)
    trade_fields=['sid','day','setup_id','tradingsymbol','side','portfolio_executed','entry_ts',
                  'exit_ts','exit_reason','portfolio_net_profit_rupees']
    trade_changes=baseline[trade_fields].merge(combined[trade_fields],on='sid',how='outer',
        suffixes=('_before','_after'),indicator=True,validate='one_to_one')
    trade_changes['net_delta_rs']=trade_changes.portfolio_net_profit_rupees_after.fillna(0)-trade_changes.portfolio_net_profit_rupees_before.fillna(0)
    trade_changes.to_csv(output/'trade_changes.csv',index=False)
    cost_rows = []
    for cost in plan['costs_bps']:
        adjusted = []
        for book in (baseline, combined):
            frame = book.copy()
            frame['cost_pct'] = cost/100
            frame['net_return_pct'] = frame.gross_return_pct-cost/100
            adjusted.append(exits.allocate(frame, 'cost_'+str(cost)))
        for period,days in periods.items():
            cost_rows.append(dict(cost_bps=cost, period=period, **metrics(*adjusted,days)))
    pd.DataFrame(cost_rows).to_csv(output/'cost_sensitivity.csv', index=False)
    overview = []
    for sid,choice in choices.items():
        rows = {r['period']:r for r in comparisons if r['setup_id']==sid}
        tr,va,oc,al = (rows[k] for k in ('train','validation','october','all'))
        changed = choice['policy'] != 'baseline__baseline'
        stronger = tr['trades']>=8 and va['trades']>=3 and oc['trades']>=4
        october_pass = economic(oc,positive=False) and oc['win_rate_pct']>=75.-EPS
        all_pass = economic(al) and al['win_rate_pct']>=75.-EPS
        status = ('UNCHANGED' if not changed else 'HISTORICAL_CANDIDATE' if october_pass and all_pass and stronger
                  else 'IMPROVED_BUT_SMALL_SAMPLE' if october_pass and all_pass else 'FAILED_OCTOBER_OR_OVERALL_GUARDS')
        overview.append(dict(setup_id=sid,policy=choice['policy'],status=status,
            train_trades=tr['trades'],validation_trades=va['trades'],october_trades=oc['trades'],
            before_trades=al['before_trades'],after_trades=al['trades'],
            before_win_rate_pct=al['before_win_rate_pct'],after_win_rate_pct=al['win_rate_pct'],
            net_delta_rs=al['net_delta_rs'],loss_reduction_rs=al['loss_reduction_rs'],
            winner_retention_pct=al['winner_retention_pct'],
            october_before_net_rs=oc['before_net_pnl_rs'],october_after_net_rs=oc['net_pnl_rs'],
            october_win_rate_pct=oc['win_rate_pct'],october_retention_pct=oc['winner_retention_pct'],
            today_before_net_rs=rows['today']['before_net_pnl_rs'],today_after_net_rs=rows['today']['net_pnl_rs']))
    overview = pd.DataFrame(overview)
    overview.to_csv(output/'setup_overview.csv', index=False)
    changes = []
    for sid,choice in choices.items():
        before_entry, before_exit = plan['grid'][sid][0],plan['exits'][sid][0]
        for scope in ('active','core'):
            for field,old in before_entry[scope].items():
                new = choice['entry'][scope][field]
                if new != old:
                    changes.append(dict(setup_id=sid,scope=scope,parameter=field,before=old,after=new))
        for field in ('confirmation_volume','extension_max'):
            if choice['entry'][field] != before_entry[field]:
                changes.append(dict(setup_id=sid,scope='entry',parameter=field,before=before_entry[field],after=choice['entry'][field]))
        for field,old in before_exit.items():
            if field!='name' and choice['exit'][field]!=old:
                changes.append(dict(setup_id=sid,scope='exit',parameter=field,before=old,after=choice['exit'][field]))
    config_changes=pd.DataFrame(changes,columns=['setup_id','scope','parameter','before','after'])
    config_changes.to_csv(output/'configuration_changes.csv',index=False)
    summary=dict(version=VERSION,scope='BACKTEST_ONLY',execution_authority=False,defaults_changed=False,
        source_run=str(source.BASELINE_RUN),days=plan['days'],preparation=preparation,
        actual_unique_entry_replays=entries_tested,actual_entry_exit_pairs=pairs_tested,
        actual_changed_session_allocations=allocator.session_replays,
        frozen_changed_setup_count=int(overview.policy.ne('baseline__baseline').sum()),
        classifications=overview.status.value_counts().to_dict(),portfolio=portfolio_rows,
        original49_portfolio=original49_rows,
        configuration_discrepancies=plan['configuration_discrepancies'],
        limitations=['All 53 sessions were examined before this study; October is a chronological historical diagnostic, not a pristine holdout.',
            'Search was bounded and predeclared; it is not every indicator or every parameter combination.',
            'Small setup samples cannot substantiate a stable future 75% win rate. Wilson intervals are descriptive and do not correct selection bias.',
            'Opening OI reconstruction is retrospective; live feature availability is not established by this backtest.',
            '5 bps round-trip model costs; 10/15 bps sensitivity supplied. One-minute bars use stop-first ambiguity handling; no tick queue/liquidity model.',
            'Combined frozen candidates include configurations that failed October. They show the honest outcome of the selection procedure, not an approved deployment.',
            'A historical candidate passing all checks still needs fresh forward validation before a performance claim.'],
        methodology_sources=['https://carmamaths.org/resources/jon/backtest2.pdf',
            'https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html'])
    write_json(output/'summary.json',summary)
    render(output,summary,overview,pd.DataFrame(portfolio_rows),config_changes,table)
    code=output/'code';code.mkdir(exist_ok=True)
    for module in (source,entry,exits):
        shutil.copyfile(module.__file__,code/Path(module.__file__).name)
    shutil.copyfile(__file__,code/Path(__file__).name)
    write_json(output/'manifest.json',dict(version=VERSION,complete=True,
        files={p.relative_to(output).as_posix():base.sha256(p) for p in output.rglob('*') if p.is_file() and p.name!='manifest.json'}))
    print(json.dumps(dict(output=str(output),classifications=summary['classifications'],portfolio=portfolio_rows),indent=2),flush=True)


def render(output,summary,overview,portfolio,changes,comparison):
    css='body{font:15px/1.5 system-ui;margin:30px auto;max-width:1600px;background:#101927;color:#edf4ff}a{color:#8ed6ff}.scroll{overflow:auto}table{border-collapse:collapse;font-size:12px}td,th{padding:8px;border-bottom:1px solid #41526b;white-space:nowrap}th{background:#243449}section{padding:18px;background:#19283d;margin:16px 0;border-radius:8px}'
    doc=f'<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G4 49 setup research V2</title><style>{css}</style></head><body><h1>G4: 49 setup entry and exit research</h1>'
    doc+='<p>29 July–16 October 2026 · 35 train / 8 validation / 10 available October diagnostic sessions (5–16 October). Original G4 unchanged. 90% original-winner profit retention required; observed win-rate goal 75%. “Today” in the tables means 16 October.</p>'
    doc+='<section><p>These are actual replacement-aware entry searches and full-minute exit replays. The combined result contains every configuration frozen after validation, including subsequent October failures.</p><p>'+html.escape(str(summary['classifications']))+'</p></section>'
    octrow=next(row for row in summary['portfolio'] if row['period']=='october')
    doc+=('<section><h2>Decision: retain original G4 parameters</h2><p>The full frozen bundle is unsuitable for promotion: '
          f'October net P&amp;L changes by Rs {octrow["net_delta_rs"]:,.2f}, win rate becomes {octrow["win_rate_pct"]:.2f}%, '
          f'and original-winner profit retention is {octrow["winner_retention_pct"]:.2f}%. '
          'Individual positive candidates are research comparisons. The HISTORICAL_CANDIDATE label only means the stated small sample floors were met; it is not proof of reliable performance.</p>')
    for sid in overview.loc[overview.status.eq('HISTORICAL_CANDIDATE'),'setup_id']:
        rows=comparison.loc[comparison.setup_id.eq(sid)].set_index('period')
        doc+=(f'<p>{html.escape(sid)} meets those sample floors. Training win rate: {rows.loc["train","win_rate_pct"]:.2f}%. '
              f'P&amp;L improvement: training Rs {rows.loc["train","net_delta_rs"]:,.2f}; '
              f'validation Rs {rows.loc["validation","net_delta_rs"]:,.2f}; October Rs {rows.loc["october","net_delta_rs"]:,.2f}. '
              'Review those separate periods before considering a configuration change.</p>')
    doc+='</section>'
    def section(title,frame):
        return '<section><h2>'+title+'</h2><div class="scroll">'+frame.to_html(index=False,border=0,float_format=lambda x:f'{x:,.2f}')+'</div></section>'
    cols=['period','before_trades','trades','before_win_rate_pct','win_rate_pct','before_net_pnl_rs','net_pnl_rs','net_delta_rs','loss_reduction_rs','winner_retention_pct']
    doc+=section('Full 51-setup portfolio: baseline vs frozen candidates',portfolio[cols])
    doc+=section('Requested 49 setups, after shared portfolio allocation',pd.DataFrame(summary['original49_portfolio'])[cols])
    doc+=section('Each setup changed individually within the full portfolio',overview)
    doc+='<p>Individual rows do not include the effects of applying all other candidate changes at once. The combined portfolio above does. The audit also supplies combined_setup_comparison.csv for the joint per-setup results.</p>'
    doc+=section('Exact proposed parameter differences',changes)
    doc+='<details><summary>All period results, including 16 October</summary>'+section('Per-setup periods',comparison)+'</details>'
    doc+='<section><h2>Evidence and limits</h2><ul>'+''.join('<li>'+html.escape(s)+'</li>' for s in summary['limitations'])+'</ul>'
    doc+='<p>Declared/effective baseline configuration discrepancies: '+html.escape(str(summary['configuration_discrepancies']))+'</p>'
    doc+='<p><a href="https://carmamaths.org/resources/jon/backtest2.pdf">Backtest overfitting research</a> · <a href="https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html">Chronological splitting</a></p></section>'
    doc+='<section>'+ ' | '.join(f'<a href="{name}">{name}</a>' for name in ('setup_overview.csv','setup_comparison.csv','combined_setup_comparison.csv','configuration_changes.csv','october_candidate_trades.csv','trade_changes.csv','daily_comparison.csv','cost_sensitivity.csv','search_plan.json','frozen_choices.json','summary.json'))+'</section></body></html>'
    (output/'report.html').write_text(doc,encoding='utf-8')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--resume',action='store_true')
    args=parser.parse_args()
    if not args.resume:
        prepare(args.output.resolve())
    if not args.prepare_only:
        search(args.output.resolve())
