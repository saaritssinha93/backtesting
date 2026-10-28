"""User-selected G7 V4 backtest: protect gross +0.1% after completed-close +0.6%.

The canonical entrypoint runs only this selected policy. Archived research
results and paper/live strategy controls are not changed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_v4_backtest as history
from research import g7_v4_profit_protection as prior
from research.g3_sl_target_simulator import simulate_exit
from research.g7_entry_quality_20261026 import same_book, verify_members
from research.g8_exit_engine import _flags, allocate_variant, metrics, replay_exits
from research.g8_vix_selection_research import dump, sha

ROOT = history.ROOT
CONFIG = ROOT / 'configs/strategies/v13_v10_g_7_v4_backtest.json'
OUT_ROOT = history.daily.g7.G7_ROOT / 'v4_backtesting'


def validate_settings(settings):
    if settings.get('scope') != 'BACKTEST_ONLY' or settings.get('execution_authority') is not False:
        raise ValueError('G7 V4 selected runner is backtest only; no execution authority')
    if settings.get('selection_basis') != 'USER_REQUEST':
        raise ValueError('Explicit user selection must be recorded')
    if settings.get('variant_id') != 'V13-V10-G-7-V4-PROFIT-PROTECTION':
        raise ValueError('Unexpected G7 V4 variant identity')
    cap = settings.get('max_positions_per_symbol')
    if isinstance(cap, bool) or cap != 1:
        raise ValueError('Exactly one active position per symbol is required')
    policy = settings['exit_policy']
    if policy.get('activation') != 'COMPLETED_FULL_POST_ENTRY_CLOSE' or policy.get('effective') != 'NEXT_MINUTE_OPEN':
        raise ValueError('Protection requires a full post-entry close and next-open activation')
    numbers = [policy['protect_after_close_pct'], policy['protected_profit_pct']]
    if any(isinstance(x, (bool, str)) or not np.isfinite(x) for x in numbers):
        raise ValueError('Protection thresholds must be finite numbers')
    activation, profit = numbers
    if not 0 <= profit < activation < 100:
        raise ValueError('Protection must be nonnegative and smaller than activation')


def replay(native, paths, base_config, settings):
    validate_settings(settings)
    index = native.set_index('sid')
    raw = replay_exits(native, paths, index.native_stop_pct, index.native_target_pct, base_config)
    policy = settings['exit_policy']
    records = []
    for row in raw.to_dict('records'):
        row['protection_activation_pct'] = policy['protect_after_close_pct']
        row['protected_profit_pct'] = policy['protected_profit_pct']
        if row['g8_baseline_filled']:
            row.update(simulate_exit(paths[int(row['sid'])], int(row['entry_path_index']),
                float(row['entry_price']), row['side'] == 'LONG', float(row['native_stop_pct']),
                float(row['native_target_pct']),
                protect_after_close_pct=policy['protect_after_close_pct'],
                protected_profit_pct=policy['protected_profit_pct'],
                tighten_after_minutes=base_config['tighten_after_minutes'],
                tightened_stop_pct=min(float(row['native_stop_pct']), base_config['tightened_stop_pct'])))
            row['net_return_pct'] = row['gross_return_pct'] - row['cost_pct']
        records.append(row)
    return pd.DataFrame(records)


def make_book(native, paths, allowed, base_config, settings):
    raw = replay(native, paths, base_config, settings)
    config = dict(base_config, variant_id=settings['variant_id'], max_positions_per_symbol=1,
                  v4_exit_policy=settings['exit_policy'])
    book = allocate_variant(raw, allowed, config, 'g7_v4',
                            cost_bps=base_config.get('model_cost_bps_round_trip', 5.))
    book['strategy'] = 'V13-V10-G-7'
    book['strategy_version'] = settings['variant_id']
    book['selection_basis'] = settings['selection_basis']
    book['v4_policy'] = 'protect_0p1_after_0p6'
    return book


def verify_archived_code(first, settings):
    """Resolve only the explicitly superseded CLI to its byte-identical archive."""
    migration = settings['entrypoint_migration']
    archived = ROOT / migration['archived_copy']
    if sha(archived) != migration['previous_sha256']:
        raise ValueError('Archived research entrypoint changed')
    for name, digest in history.read(first / 'manifest.json')['code'].items():
        path = ROOT / name
        if name.replace('\\', '/') == migration['previous_entrypoint']:
            if digest != migration['previous_sha256']:
                raise ValueError('Unknown historical entrypoint hash')
            path = archived
        if sha(path) != digest:
            raise ValueError('Archived research dependency changed: ' + name)


def verify_sources(settings, cache):
    source = settings['source_research']
    directory = Path(source['output'])
    verify_members(directory, source['manifest_sha256'])
    # This independently reproduces the previously reported protection results.
    prior.verify(directory)
    first = Path(history.read(directory / 'summary.json')['round1_output'])
    verify_members(first, history.read(directory / 'protocol.json')['round1_manifest_sha256'])
    verify_archived_code(first, settings)
    if cache.resolve() == Path(settings['default_cache']).resolve():
        verify_members(cache, settings['default_cache_manifest_sha256'])
    native, baseline, f, paths, provenance = history.load_cache(cache)
    for filename, digest in f[['stock_source_path', 'stock_source_sha256']].drop_duplicates().itertuples(index=False, name=None):
        if sha(Path(filename)) != digest:
            raise ValueError('Stock price evidence changed')
    for package in provenance['source_packages']:
        verify_members(package['path'], package['manifest_sha256'])
    if sha(Path(provenance['final_source_run']) / 'run_manifest.json') != provenance['final_source_manifest_sha256']:
        raise ValueError('Final source run changed')
    return native, baseline, f, paths, provenance


def write_report(out, settings, summary, comparison, daily):
    show = lambda f: '<div class="scroll">' + f.to_html(index=False, float_format=lambda x: f'{x:,.2f}') + '</div>'
    html = '''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G7 V4 selected backtest</title><style>body{font:16px/1.5 system-ui;margin:30px;max-width:1450px;color:#20334a}.result{padding:18px;background:#edf4f8;border-left:5px solid #396b95}.scroll{overflow-x:auto}td,th{padding:8px;border:1px solid #ccd5de;text-align:right}table{border-collapse:collapse;font-size:13px}a{color:#145e99}</style></head><body>'''
    html += '<h1>G7 V4: Protect +0.1% after +0.6%</h1><p class="result">Selected by the user for backtesting. Completed full post-entry close at +0.6% arms a gross +0.1% stop for the following minute open. Both LONG and SHORT are supported. One active position per symbol; re-entry is allowed after exit.</p>'
    html += '<p>Original G7 V2 entries, Nifty permission, targets, initial stop, timed tightening and portfolio sizing are retained. Full opportunity inventory is reallocated after exit changes. Protection never loosens a tighter stop; gaps can fill below the protected profit. Net results include modeled costs.</p>'
    html += f'<p>Coverage: {summary["sessions"]} available sessions, {summary["first_day"]} through {summary["last_day"]}; {summary["opportunities"]} selected opportunities. Rs500,000 exposure per trade under the inherited fixed-capital model. Base round-trip cost: {summary["model_cost_bps"]}bps.</p>'
    for period in ('all', 'october'):
        html += '<h2>' + period + '</h2>' + show(comparison.loc[comparison.period.eq(period), ['strategy', 'trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs']])
    html += '<h2>October daywise</h2>' + show(daily.loc[daily.day.str.startswith('2026-10'), ['day', 'g7_v2_net_rs', 'v4_symbol_guard_net_rs', 'g7_v4_net_rs', 'g7_v4_trades', 'g7_v4_wins', 'g7_v4_losses']])
    html += '<h2>Selection and limitations</h2><p>This is an explicit user selection, not a claim that the rule passed the earlier research screen. Earlier experiments remain archived. No paper/live profile was changed.</p><ul>'
    html += ''.join('<li>' + item + '</li>' for item in settings.get('limitations', []))
    html += '</ul><p>Missing source sessions24/25 August and1 October are excluded; only available sessions are reported. Drawdown uses daily-close realized equity, not intraday mark-to-market.15/30bps cost sensitivity is included.</p><h2>Files</h2><ul>'
    html += ''.join(f'<li><a href="{name}">{name}</a></li>' for name in ('daywise_comparison.csv', 'october_daywise_comparison.csv', 'portfolio_trades.csv', 'executed_trades.csv', 'comparison.csv', 'cost_sensitivity.csv', 'config.json', 'summary.json'))
    html += '</ul></body></html>'
    (out / 'report.html').write_text(html, encoding='utf-8')


def run(cache_path=None):
    settings = history.read(CONFIG)
    validate_settings(settings)
    config_digest = sha(CONFIG)
    protected = history.protected_hashes()
    old_research_latest = history.OUT_ROOT / 'latest.json'
    protected[str(old_research_latest)] = sha(old_research_latest)
    cache = Path(cache_path or settings['default_cache']).resolve()
    native, baseline, f, paths, source = verify_sources(settings, cache)
    allowed = _flags(f.allowed_g7_v2, 'nifty')
    base_config = source['base_config']
    index = native.set_index('sid')
    raw = replay_exits(native, paths, index.native_stop_pct, index.native_target_pct, base_config)
    reference = allocate_variant(raw, allowed, base_config, 'g7_v2', cost_bps=base_config['model_cost_bps_round_trip'])
    same_book(reference, baseline)
    guard = allocate_variant(raw, allowed, dict(base_config, max_positions_per_symbol=1), 'v4_symbol_guard', cost_bps=base_config['model_cost_bps_round_trip'])
    final = make_book(native, paths, allowed, base_config, settings)
    source_match = cache == Path(settings['default_cache']).resolve()
    if source_match:
        same_book(final, pd.read_csv(Path(settings['source_research']['output']) / settings['source_research']['policy'] / 'portfolio_trades.csv'))
    days = source['periods']['all']
    periods = dict(source['periods'], october=[d for d in days if d.startswith('2026-10')])
    out = OUT_ROOT / 'runs' / days[-1] / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f') + '_protect_0p1_after_0p6')
    out.mkdir(parents=True, exist_ok=False)
    (out / 'config.json').write_bytes(CONFIG.read_bytes())
    dump(out / 'base_config.json', base_config)
    final.to_csv(out / 'portfolio_trades.csv', index=False)
    final.loc[_flags(final.portfolio_executed, 'executed')].to_csv(out / 'executed_trades.csv', index=False)
    books = dict(g7_v2=reference, v4_symbol_guard=guard, g7_v4=final)
    comparisons, daily_rows, costs = [], [], []
    for name, book in books.items():
        for period, dates in periods.items():
            comparisons.append(dict(strategy=name, period=period, **metrics(book, dates)))
        for cost in (5, 15, 30):
            cfg = base_config if name == 'g7_v2' else dict(base_config, max_positions_per_symbol=1)
            stress = allocate_variant(book, allowed, cfg, name, cost_bps=cost)
            for period, dates in periods.items():
                costs.append(dict(strategy=name, cost_bps=cost, period=period, **metrics(stress, dates)))
    for day in days:
        stats = {name: metrics(book, [day]) for name, book in books.items()}
        daily_rows.append(dict(day=day, g7_v2_net_rs=stats['g7_v2']['net_pnl_rs'],
            v4_symbol_guard_net_rs=stats['v4_symbol_guard']['net_pnl_rs'], g7_v4_net_rs=stats['g7_v4']['net_pnl_rs'],
            net_change_vs_v2_rs=stats['g7_v4']['net_pnl_rs'] - stats['g7_v2']['net_pnl_rs'],
            g7_v4_trades=stats['g7_v4']['trades'], g7_v4_wins=stats['g7_v4']['wins'], g7_v4_losses=stats['g7_v4']['losses'],
            g7_v4_gross_rs=stats['g7_v4']['gross_pnl_rs'], g7_v4_cost_rs=stats['g7_v4']['cost_rs']))
    comparison, daily = pd.DataFrame(comparisons), pd.DataFrame(daily_rows)
    comparison.to_csv(out / 'comparison.csv', index=False)
    daily.to_csv(out / 'daywise_comparison.csv', index=False)
    daily.loc[daily.day.str.startswith('2026-10')].to_csv(out / 'october_daywise_comparison.csv', index=False)
    pd.DataFrame(costs).to_csv(out / 'cost_sensitivity.csv', index=False)
    summary = dict(variant_id=settings['variant_id'], scope='BACKTEST_ONLY', execution_authority=False,
        selection_basis='USER_REQUEST', selected_policy='protect_0p1_after_0p6', research_qualification=False,
        independent_forward_validation=False, config_sha256=config_digest, cache=str(cache),
        cache_manifest_sha256=sha(cache / 'manifest.json'), periods=periods, first_day=days[0], last_day=days[-1],
        sessions=len(days), opportunities=len(native), model_cost_bps=base_config['model_cost_bps_round_trip'],
        source_cushion_reproduced=source_match, original_v2_reproduced=True,
        protected_files_unchanged=protected, previous_research_entrypoint=settings['entrypoint_migration'],
        all=metrics(final, days), october=metrics(final, periods['october']))
    dump(out / 'summary.json', summary)
    write_report(out, settings, summary, comparison, daily)
    if sha(CONFIG) != config_digest or any(sha(Path(p)) != h for p, h in protected.items()):
        raise ValueError('Selected config or protected paper/live/research controls changed')
    code = [Path(__file__), CONFIG, ROOT / 'fno_v13_v10_g_7_v4_backtest.py',
        ROOT / settings['entrypoint_migration']['archived_copy'], ROOT / 'research/g3_sl_target_simulator.py',
        ROOT / 'research/g8_exit_engine.py', ROOT / 'tests/test_g7_v4_selected_backtest.py']
    history.seal(out, code={str(p.relative_to(ROOT)): sha(p) for p in code})
    verify(out)
    dump(OUT_ROOT / 'latest.json', dict(output=str(out), manifest_sha256=sha(out / 'manifest.json'),
        variant_id=settings['variant_id'], selection_basis='USER_REQUEST', execution_authority=False,
        daywise_comparison=str(out / 'daywise_comparison.csv'), october_daywise_comparison=str(out / 'october_daywise_comparison.csv')))
    print(comparison.loc[comparison.period.isin(['all', 'october']), ['strategy', 'period', 'trades', 'wins', 'losses', 'net_pnl_rs']].to_string(index=False), flush=True)
    print('G7 V4 OUTPUT', out, flush=True)
    return out


def verify(out):
    out = Path(out)
    verify_members(out)
    summary, settings = history.read(out / 'summary.json'), history.read(out / 'config.json')
    validate_settings(settings)
    for name, digest in history.read(out / 'manifest.json')['code'].items():
        if sha(ROOT / name) != digest:
            raise ValueError('Selected backtest dependency changed: ' + name)
    if sha(out / 'config.json') != summary['config_sha256']:
        raise ValueError('Saved configuration differs')
    cache = Path(summary['cache'])
    verify_members(cache, summary['cache_manifest_sha256'])
    native, baseline, f, paths, source = history.load_cache(cache)
    cfg = source['base_config']
    if history.read(out / 'base_config.json') != cfg:
        raise ValueError('Baseline configuration changed')
    allowed = _flags(f.allowed_g7_v2, 'nifty')
    actual = make_book(native, paths, allowed, cfg, settings)
    saved = pd.read_csv(out / 'portfolio_trades.csv')
    same_book(actual, saved)
    for col in ('exit_reason', 'exit_event', 'strategy', 'strategy_version', 'v4_policy', 'selection_basis'):
        if not actual[col].fillna('').astype(str).equals(saved[col].fillna('').astype(str)):
            raise ValueError('Saved decision metadata differs: ' + col)
    for col in ('exit_price', 'exit_path_index', 'protection_activation_pct', 'protected_profit_pct', 'protection_armed_path_index', 'protection_effective_path_index'):
        np.testing.assert_allclose(actual[col], saved[col], equal_nan=True, rtol=0, atol=1e-8)
    daily = pd.read_csv(out / 'daywise_comparison.csv').set_index('day')
    for day in summary['periods']['all']:
        stats = metrics(actual, [day])
        for field, metric in [('g7_v4_net_rs', 'net_pnl_rs'), ('g7_v4_trades', 'trades'), ('g7_v4_wins', 'wins'), ('g7_v4_losses', 'losses')]:
            if not np.isclose(daily.loc[day, field], stats[metric], rtol=0, atol=1e-6):
                raise ValueError('Daily accounting differs: ' + field)
    for period in ('all', 'october'):
        stats = metrics(actual, summary['periods'][period])
        for field in ('trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs'):
            if not np.isclose(stats[field], summary[period][field], rtol=0, atol=1e-6):
                raise ValueError('Summary accounting differs: ' + field)
    if summary['source_cushion_reproduced']:
        same_book(actual, pd.read_csv(Path(settings['source_research']['output']) / settings['source_research']['policy'] / 'portfolio_trades.csv'))
    index = native.set_index('sid')
    raw = replay_exits(native, paths, index.native_stop_pct, index.native_target_pct, cfg)
    reference = allocate_variant(raw, allowed, cfg, 'g7_v2', cost_bps=cfg['model_cost_bps_round_trip'])
    same_book(reference, baseline)
    guard = allocate_variant(raw, allowed, dict(cfg, max_positions_per_symbol=1), 'v4_symbol_guard', cost_bps=cfg['model_cost_bps_round_trip'])
    books = dict(g7_v2=reference, v4_symbol_guard=guard, g7_v4=actual)
    comparison = pd.read_csv(out / 'comparison.csv').set_index(['strategy', 'period'])
    costs = pd.read_csv(out / 'cost_sensitivity.csv').set_index(['strategy', 'cost_bps', 'period'])
    for name, book in books.items():
        for period, days in summary['periods'].items():
            stats = metrics(book, days)
            for col in ('trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs'):
                if not np.isclose(stats[col], comparison.loc[(name, period), col], atol=1e-6, rtol=0):
                    raise ValueError('Comparison differs: ' + name + '/' + col)
        for cost in (5, 15, 30):
            cost_config = cfg if name == 'g7_v2' else dict(cfg, max_positions_per_symbol=1)
            stressed = allocate_variant(book, allowed, cost_config, name, cost_bps=cost)
            for period, days in summary['periods'].items():
                stats = metrics(stressed, days)
                for col in ('trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs'):
                    if not np.isclose(stats[col], costs.loc[(name, cost, period), col], atol=1e-6, rtol=0):
                        raise ValueError('Cost sensitivity differs: ' + name + '/' + col)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', type=Path, help='Use an explicitly supplied sealed V4 cache; default is the pinned historical source')
    parser.add_argument('--verify', type=Path, help='Reproduce a selected G7 V4 result without publishing')
    args = parser.parse_args()
    if args.verify:
        print('VERIFIED', verify(args.verify))
    else:
        run(args.cache)


if __name__ == '__main__':
    main()
