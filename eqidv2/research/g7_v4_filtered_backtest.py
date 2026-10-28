"""User-selected G7 V4: low-efficiency extreme-entry veto plus +0.6/+0.1 protection."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_v4_indicator_iteration as model
from research import g7_v4_selected_backtest as previous
from research.g7_entry_quality_20261026 import same_book, verify_members
from research.g8_exit_engine import _flags, allocate_variant, metrics, replay_exits
from research.g8_vix_selection_research import dump, sha

ROOT = previous.ROOT
CONFIG = ROOT / 'configs/strategies/v13_v10_g_7_v4_low_efficiency_backtest.json'
OUT_ROOT = previous.OUT_ROOT
VERSION = 'V13-V10-G-7-V4-LOW-EFFICIENCY-PROTECTION'
FILTER = dict(policy='low_efficiency', efficiency_period=10, efficiency_below=.3,
    rsi_period=14, rsi_weakening_minutes=3,
    extreme='VWAP_Z_GT_2P5_OR_PRIOR_SAME_CLOCK_HISTORICAL_95',
    minimum_session_bars=30, decision_at='ORIGINAL_CONFIRMATION_CLOSE',
    insufficient_warmup='PRESERVE_ENTRY')
PROTECTION = dict(protect_after_close_pct=.6, protected_profit_pct=.1,
    activation='COMPLETED_FULL_POST_ENTRY_CLOSE', effective='NEXT_MINUTE_OPEN')


def validate_settings(settings):
    if settings.get('variant_id') != VERSION or settings.get('scope') != 'BACKTEST_ONLY':
        raise ValueError('Unexpected G7 V4 backtest identity or scope')
    if settings.get('execution_authority') is not False or settings.get('selection_basis') != 'USER_REQUEST':
        raise ValueError('Explicit backtest selection without execution authority required')
    cap = settings.get('max_positions_per_symbol')
    if isinstance(cap, bool) or cap != 1:
        raise ValueError('One active position per symbol is required')
    if settings.get('entry_filter') != FILTER or settings.get('exit_policy') != PROTECTION:
        raise ValueError('G7 V4 must use the selected exact entry filter and protection rules')
    if settings.get('research_qualification') is not False:
        raise ValueError('User choice must not be relabeled as research qualification')


def protection_settings(settings):
    return dict(variant_id='V13-V10-G-7-V4-PROFIT-PROTECTION', scope='BACKTEST_ONLY',
        execution_authority=False, selection_basis='USER_REQUEST', max_positions_per_symbol=1,
        exit_policy=settings['exit_policy'])


def reference_book(native, paths, f, config):
    """Rebuild V2 permission metadata from original SID-aligned decisions."""
    index=native.set_index('sid')
    raw=replay_exits(native,paths,index.native_stop_pct,index.native_target_pct,config)
    return allocate_variant(raw,_flags(f.allowed_g7_v2,'nifty'),config,'g7_v2',
        cost_bps=config.get('model_cost_bps_round_trip',5.))


def make_book(native, paths, f, base_config, settings):
    validate_settings(settings)
    risk = model.decisions(f)['low_efficiency']
    nifty = _flags(f.allowed_g7_v2, 'nifty')
    if set(native.sid) != set(f.index) or not f.index.is_unique:
        raise ValueError('Feature inventory differs from selected opportunities')
    raw = previous.replay(native, paths, base_config, protection_settings(settings))
    cfg = dict(base_config, variant_id=VERSION, max_positions_per_symbol=1,
        v4_entry_filter=settings['entry_filter'], v4_exit_policy=settings['exit_policy'])
    book = allocate_variant(raw, nifty & ~risk, cfg, 'g7_v4',
        cost_bps=base_config.get('model_cost_bps_round_trip', 5.))
    book['strategy'] = 'V13-V10-G-7'
    book['strategy_version'] = VERSION
    book['v4_policy'] = 'low_efficiency_with_protect_0p1_after_0p6'
    book['selection_basis'] = 'USER_REQUEST'
    book['research_qualification'] = False
    book['v4_indicator_veto'] = book.sid.map(risk)
    blocked = book.sid.map(nifty & risk)
    book.loc[blocked, 'portfolio_status'] = 'V4_EXTREME_ENTRY_BLOCKED'
    book.loc[blocked, 'portfolio_reject_reason'] = 'LOW_EFFICIENCY_EXTREME_RSI'
    return book


def load_inputs(settings, cache_path=None):
    validate_settings(settings)
    src = settings['source_research']
    research = Path(src['output'])
    verify_members(research, src['manifest_sha256'])
    # Full archived candidate replay; old research code and results stay intact.
    model.verify(research)
    old = model.base.read(ROOT / settings['previous_protection_config'])
    cache = Path(cache_path or old['default_cache']).resolve()
    native, v2, original, paths, proof = previous.verify_sources(old, cache)
    summary = model.base.read(research / 'summary.json')
    exact = cache == Path(summary['cache']).resolve()
    if exact:
        f = pd.read_csv(research / 'entry_features_and_decisions.csv').set_index('sid', drop=False)
    else:
        f = model.build_features(original, native)
    return native, v2, f, paths, proof, cache, exact


def write_report(out, summary, comparison, daily, settings):
    show = lambda frame: '<div class="scroll">' + frame.to_html(index=False, float_format=lambda x:f'{x:,.2f}') + '</div>'
    html = '''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G7 V4 backtest</title><style>body{font:16px/1.5 system-ui;margin:30px;max-width:1500px;color:#23364a}.result{padding:18px;border-left:5px solid #387096;background:#edf5f9}.scroll{overflow:auto}table{border-collapse:collapse;font-size:13px}th,td{padding:8px;border:1px solid #ced8e1;text-align:right}a{color:#155d98}</style></head><body>'''
    html += '<h1>G7 V4: efficiency entry filter and profit protection</h1><p class="result">Selected by the user for backtesting. Reject only when price is extreme, directional RSI weakens over3 completed minutes, and10-minute price efficiency is below0.30. Keep gross+0.1% protection after a completed post-entry close reaches+0.6%, effective next minute.</p>'
    html += '<p>Extreme means directional VWAP z&gt;2.5 or a prior-session same-clock95th-percentile extreme. Indicators stop at original confirmation; fewer than30 valid session bars preserve entries. All selected opportunities are allocated again with one active position per symbol. Original targets, timed stops, Nifty permission and fixed sizing remain in force.</p>'
    html += f'<p>{summary["sessions"]} available sessions from {summary["first_day"]} through {summary["last_day"]}; {summary["opportunities"]} selected opportunities. Net figures include{summary["model_cost_bps"]}bps modeled round-trip costs.</p>'
    for p in ('all', 'october'):
        html += '<h2>' + p + '</h2>' + show(comparison.loc[comparison.period.eq(p), ['strategy','trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs']])
    html += '<h2>October daywise</h2>' + show(daily.loc[daily.day.str.startswith('2026-10')])
    html += '<h2>Selection and limitations</h2><ul>' + ''.join('<li>'+x+'</li>' for x in settings['limitations']) + '</ul><p>No paper/live configuration changed. Drawdown uses daily-close realized equity. Missing source sessions24/25 August and1 October are excluded.15/30bps stresses are exported.</p>'
    html += '<h2>Files</h2><ul>' + ''.join(f'<li><a href="{n}">{n}</a></li>' for n in ('daywise_comparison.csv','october_daywise_comparison.csv','portfolio_trades.csv','executed_trades.csv','entry_features_and_decisions.csv','comparison.csv','cost_sensitivity.csv','config.json','summary.json')) + '</ul></body></html>'
    (out/'report.html').write_text(html, encoding='utf-8')


def run(cache_path=None):
    settings = model.base.read(CONFIG)
    validate_settings(settings)
    digest = sha(CONFIG)
    protected = model.base.protected_hashes()
    for p in (previous.CONFIG, model.OUT_ROOT/'latest.json', model.base.OUT_ROOT/'latest.json'):
        protected[str(p)] = sha(p)
    old_pointer = model.base.read(OUT_ROOT/'latest.json')
    native, v2, f, paths, proof, cache, exact = load_inputs(settings, cache_path)
    cfg = proof['base_config']
    replayed_v2=reference_book(native,paths,f,cfg)
    same_book(replayed_v2,v2)
    v2=replayed_v2
    old = previous.make_book(native, paths, _flags(f.allowed_g7_v2,'nifty'), cfg, protection_settings(settings))
    final = make_book(native, paths, f, cfg, settings)
    if exact:
        same_book(final, pd.read_csv(Path(settings['source_research']['output'])/'low_efficiency/portfolio_trades.csv'))
    periods = dict(proof['periods'], october=[d for d in proof['periods']['all'] if d.startswith('2026-10')])
    days = periods['all']
    out = OUT_ROOT/'runs'/days[-1]/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'_low_efficiency_protection')
    out.mkdir(parents=True, exist_ok=False)
    (out/'config.json').write_bytes(CONFIG.read_bytes())
    f.to_csv(out/'entry_features_and_decisions.csv', index=False)
    final.to_csv(out/'portfolio_trades.csv', index=False)
    final.loc[_flags(final.portfolio_executed,'executed')].to_csv(out/'executed_trades.csv',index=False)
    books = dict(g7_v2=v2, previous_g7_v4=old, g7_v4=final)
    rows, costs, daily_rows = [], [], []
    for name, book in books.items():
        for p, dates in periods.items(): rows.append(dict(strategy=name, period=p, **metrics(book, dates)))
        for cost in (5,15,30):
            mask = _flags(book.set_index('sid').g8_direction_allowed,'allowed')
            stress = allocate_variant(book, mask, cfg if name=='g7_v2' else dict(cfg,max_positions_per_symbol=1),name,cost_bps=cost)
            for p, dates in periods.items(): costs.append(dict(strategy=name,period=p,cost_bps=cost,**metrics(stress,dates)))
    for day in days:
        stats = {name:metrics(book,[day]) for name,book in books.items()}
        a,b=stats['g7_v4'],stats['previous_g7_v4']
        daily_rows.append(dict(day=day,g7_v2_net_rs=stats['g7_v2']['net_pnl_rs'],previous_g7_v4_net_rs=b['net_pnl_rs'],
            g7_v4_net_rs=a['net_pnl_rs'],net_change_rs=a['net_pnl_rs']-b['net_pnl_rs'],g7_v4_trades=a['trades'],
            g7_v4_wins=a['wins'],g7_v4_losses=a['losses'],g7_v4_gross_rs=a['gross_pnl_rs'],g7_v4_cost_rs=a['cost_rs']))
    comparison,daily=pd.DataFrame(rows),pd.DataFrame(daily_rows)
    comparison.to_csv(out/'comparison.csv',index=False)
    daily.to_csv(out/'daywise_comparison.csv',index=False)
    daily.loc[daily.day.str.startswith('2026-10')].to_csv(out/'october_daywise_comparison.csv',index=False)
    pd.DataFrame(costs).to_csv(out/'cost_sensitivity.csv',index=False)
    migration=settings['entrypoint_migration']
    archived=ROOT/migration['archived_copy']
    if sha(archived)!=migration['previous_sha256']: raise ValueError('Previous backtest entrypoint archive differs')
    summary=dict(variant_id=VERSION,selection_basis='USER_REQUEST',scope='BACKTEST_ONLY',execution_authority=False,
        research_qualification=False,independent_forward_validation=False,cache=str(cache),cache_manifest_sha256=sha(cache/'manifest.json'),
        config_sha256=digest,periods=periods,first_day=days[0],last_day=days[-1],sessions=len(days),opportunities=len(native),
        model_cost_bps=cfg['model_cost_bps_round_trip'],source_research_reproduced=exact,previous_default=old_pointer,
        archived_previous_entrypoint_sha256=sha(archived),protected_files_unchanged=protected,
        all=metrics(final,days),october=metrics(final,periods['october']))
    dump(out/'summary.json',summary)
    write_report(out,summary,comparison,daily,settings)
    if sha(CONFIG)!=digest or any(sha(Path(p))!=h for p,h in protected.items()): raise ValueError('Protected configuration changed')
    code=[Path(__file__),CONFIG,Path(model.__file__),Path(previous.__file__),previous.CONFIG,
        ROOT/'fno_v13_v10_g_7_v4_backtest.py',archived,ROOT/'research/g7_v4_extremes.py',
        ROOT/'research/g3_sl_target_simulator.py',ROOT/'research/g8_exit_engine.py',ROOT/'tests/test_g7_v4_filtered_backtest.py']
    model.base.seal(out,code={str(p.relative_to(ROOT)):sha(p) for p in code})
    verify(out)
    dump(OUT_ROOT/'latest.json',dict(output=str(out),manifest_sha256=sha(out/'manifest.json'),variant_id=VERSION,
        entry_policy='low_efficiency',selection_basis='USER_REQUEST',execution_authority=False,
        config_path=str(CONFIG),daywise_comparison=str(out/'daywise_comparison.csv'),october_daywise_comparison=str(out/'october_daywise_comparison.csv')))
    print(comparison.loc[comparison.period.isin(['all','october']),['strategy','period','trades','wins','losses','net_pnl_rs']].to_string(index=False),flush=True)
    print('G7 V4 OUTPUT',out,flush=True)
    return out


def verify(out):
    out=Path(out)
    verify_members(out)
    settings,summary=model.base.read(out/'config.json'),model.base.read(out/'summary.json')
    validate_settings(settings)
    for p,h in model.base.read(out/'manifest.json')['code'].items():
        if sha(ROOT/p)!=h: raise ValueError('Backtest dependency changed: '+p)
    if sha(out/'config.json')!=summary['config_sha256']: raise ValueError('Config snapshot differs')
    cache=Path(summary['cache'])
    verify_members(cache,summary['cache_manifest_sha256'])
    src=settings['source_research'];verify_members(src['output'],src['manifest_sha256'])
    native,v2,_,paths,proof=model.base.load_cache(cache)
    f=pd.read_csv(out/'entry_features_and_decisions.csv').set_index('sid',drop=False)
    cfg=proof['base_config']
    replayed_v2=reference_book(native,paths,f,cfg)
    same_book(replayed_v2,v2)
    v2=replayed_v2
    actual=make_book(native,paths,f,cfg,settings)
    saved=pd.read_csv(out/'portfolio_trades.csv')
    same_book(actual,saved)
    for col in ('strategy','strategy_version','v4_policy','selection_basis','portfolio_status','portfolio_reject_reason','exit_reason'):
        if not actual[col].fillna('').astype(str).equals(saved[col].fillna('').astype(str)): raise ValueError('Metadata differs: '+col)
    if not _flags(actual.v4_indicator_veto,'veto').equals(_flags(saved.v4_indicator_veto,'veto')): raise ValueError('Indicator decisions differ')
    if summary['source_research_reproduced']: same_book(actual,pd.read_csv(Path(src['output'])/'low_efficiency/portfolio_trades.csv'))
    old=previous.make_book(native,paths,_flags(f.allowed_g7_v2,'nifty'),cfg,protection_settings(settings))
    books=dict(g7_v2=v2,previous_g7_v4=old,g7_v4=actual)
    table=pd.read_csv(out/'comparison.csv').set_index(['strategy','period'])
    costs=pd.read_csv(out/'cost_sensitivity.csv').set_index(['strategy','cost_bps','period'])
    for name,book in books.items():
        for p,days in summary['periods'].items():
            stats=metrics(book,days)
            for key in ('trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs'):
                if not np.isclose(stats[key],table.loc[(name,p),key],rtol=0,atol=1e-6): raise ValueError('Comparison differs')
        for cost in (5,15,30):
            mask=_flags(book.set_index('sid').g8_direction_allowed,'allowed')
            stress=allocate_variant(book,mask,cfg if name=='g7_v2' else dict(cfg,max_positions_per_symbol=1),name,cost_bps=cost)
            for p,days in summary['periods'].items():
                stats=metrics(stress,days)
                for key in ('trades','wins','losses','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs'):
                    if not np.isclose(stats[key],costs.loc[(name,cost,p),key],rtol=0,atol=1e-6): raise ValueError('Cost stress differs')
    daily=pd.read_csv(out/'daywise_comparison.csv').set_index('day')
    for day in summary['periods']['all']:
        stats=metrics(actual,[day])
        for col,key in [('g7_v4_net_rs','net_pnl_rs'),('g7_v4_trades','trades'),('g7_v4_wins','wins'),('g7_v4_losses','losses')]:
            if not np.isclose(daily.loc[day,col],stats[key],rtol=0,atol=1e-6): raise ValueError('Daywise figures differ')
    for p in ('all','october'):
        for key in ('trades','wins','losses','net_pnl_rs'):
            if not np.isclose(metrics(actual,summary['periods'][p])[key],summary[p][key],rtol=0,atol=1e-6): raise ValueError('Summary differs')
    return True


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache',type=Path)
    parser.add_argument('--verify',type=Path)
    args=parser.parse_args()
    print('VERIFIED',verify(args.verify)) if args.verify else run(args.cache)


if __name__=='__main__':
    main()
