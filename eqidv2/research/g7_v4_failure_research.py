"""Three fixed G7 V4 failure/continuation exit experiments, research only.

python -m research.g7_v4_failure_research --round1 PATH
No new thresholds, fallback selection, live configuration or orders.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_v4_backtest as base
from research import g7_v4_profit_protection as prior
from research import g7_v4_failure_exits as engine
from research.g7_entry_quality_20261026 import same_book, verify_members
from research.g8_exit_engine import _flags, allocate_variant, metrics, replay_exits
from research.g8_vix_selection_research import dump, sha

VERSION = 'V13-V10-G-7-V4-FAILURE-EXIT-RESEARCH'
OUT_ROOT = base.OUT_ROOT / 'failure_research'
POLICIES = {
    'confirmation_failure': 'Two consecutive full post-entry closes beyond adverse confirmation-candle edge; exit next open.',
    'stalled_30m': 'Once at first full close elapsed >=30min: maximum completed-close gain <0.25 initial R and current return <0; exit next open.',
    'confirmed_trail': 'Once >=3 full post-entry bars exist AND that close reaches half native target, arm last3-bar low/high trail; early threshold touches are not latched; activate/update next open; never loosen.',
}
SELECTION = dict(
    primary_reference='v4_symbol_guard', net_profit='>= control', wins='> control',
    win_rate='>= control', losses='< control', losing_amount='<=95% of control',
    daily_close_drawdown='<= control', trade_retention='>=90% of control',
    original_v2_net_floor='>=95% of original V2', changed_executions_min=5,
    changed_days_min=3, ranking='highest development net, then lowest losing amount, then wins, then name',
    selection_period='development only', validation='same criteria, selected rule only; no replacement',
    assessment='same criteria, selected rule only; no replacement',
    cost_robustness='Selected rule must also beat guard net profit in combined validation+assessment at15/30bps',
)


def replay(native, paths, config, policy=None):
    if policy is not None and policy not in POLICIES:
        raise ValueError('Unknown fixed policy')
    indexed = native.set_index('sid')
    raw = replay_exits(native, paths, indexed.native_stop_pct, indexed.native_target_pct, config)
    records = []
    for row in raw.to_dict('records'):
        if row['g8_baseline_filled'] and policy:
            row.update(engine.simulate_exit(
                paths[int(row['sid'])], int(row['entry_path_index']), float(row['entry_price']),
                row['side'] == 'LONG', float(row['native_stop_pct']), float(row['native_target_pct']),
                policy=policy, confirmation_low=float(row['confirmation_low']),
                confirmation_high=float(row['confirmation_high']),
                tighten_after_minutes=config['tighten_after_minutes'],
                tightened_stop_pct=config['tightened_stop_pct']))
            row['net_return_pct'] = row['gross_return_pct'] - row['cost_pct']
        records.append(row)
    result = pd.DataFrame(records)
    # Exit changes must never alter original opportunities or entry evidence.
    for col in ('sid', 'entry_price', 'entry_path_index', 'native_stop_pct', 'native_target_pct'):
        np.testing.assert_allclose(result[col], raw[col], equal_nan=True, rtol=0, atol=0)
    if not pd.to_datetime(result.entry_ts, utc=True).equals(pd.to_datetime(raw.entry_ts, utc=True)):
        raise ValueError('An exit policy changed entry times')
    if not _flags(result.g8_baseline_filled, 'fills').equals(_flags(raw.g8_baseline_filled, 'fills')):
        raise ValueError('An exit policy changed fill permission')
    return result


def comparisons(books, periods):
    table = prior.comparisons(books, periods)
    # Additional market exits are distinct from exchange stop/target exits.
    counts = {}
    for name, book in books.items():
        for period, days in periods.items():
            b = book.loc[book.day.isin(days) & _flags(book.portfolio_executed, 'executed')]
            counts[(name, period)] = int(b.exit_reason.isin(['CONFIRMATION_FAILURE', 'STALLED_30M']).sum())
    table['failure_market_exits'] = [counts[(r.policy, r.period)] for r in table.itertuples()]
    return table


def select(table, policies, period='development'):
    """Stronger objective fixed before outcomes; never read a later period."""
    t = table.loc[table.period.eq(period) & table.reference.eq('v4_symbol_guard')].set_index('policy')
    b, original = t.loc['v4_symbol_guard'], t.loc['g7_v2']
    audit, eligible = [], []
    for name in policies:
        r = t.loc[name]
        reasons = []
        if r.net_pnl_rs < b.net_pnl_rs - 1e-6:
            reasons.append('net profit below current V4')
        if r.wins <= b.wins:
            reasons.append('winning trade count not increased')
        if r.win_rate_pct < b.win_rate_pct - 1e-8:
            reasons.append('win rate reduced')
        if r.losses >= b.losses:
            reasons.append('losing trade count not reduced')
        if r.negative_trade_sum_abs_rs > .95 * b.negative_trade_sum_abs_rs + 1e-6:
            reasons.append('losing amount reduction below5%')
        if r.daily_close_drawdown_rs > b.daily_close_drawdown_rs + 1e-6:
            reasons.append('larger daily-close drawdown')
        if r.trades < .9 * b.trades:
            reasons.append('trade retention below90%')
        if r.net_pnl_rs < .95 * original.net_pnl_rs - 1e-6:
            reasons.append('net profit below95% of original V2')
        if r.changed_executions < 5 or r.changed_days < 3:
            reasons.append('insufficient changed executions/days')
        audit.append(dict(policy=name, period=period, eligible=not reasons, failure_reasons='; '.join(reasons)))
        if not reasons:
            eligible.append((r.net_pnl_rs, -r.negative_trade_sum_abs_rs, r.wins, name))
    return (max(eligible)[3] if eligible else None), audit


def tag(book, name, digest):
    book = book.copy()
    book['strategy'] = 'V13-V10-G-7'
    book['strategy_version'] = VERSION
    book['v4_failure_policy'] = name
    book['v4_protocol_sha256'] = digest
    book['strategy_fingerprint'] = sha256((str(book.strategy_fingerprint.iloc[0]) + digest).encode()).hexdigest()
    return book


def protected_hashes():
    files = base.protected_hashes()
    for p in (base.OUT_ROOT / 'latest.json', prior.OUT_ROOT / 'latest.json'):
        if p.is_file():
            files[str(p)] = sha(p)
    return files


def write_report(out, summary, table, daywise, audit, costs):
    show = lambda f: '<div class="scroll">' + f.to_html(index=False, float_format=lambda x: f'{x:,.2f}') + '</div>'
    status = ('No candidate passed the stronger development criteria.' if summary['selected_candidate'] is None else
              f"Development selected {summary['selected_candidate']}. Validation pass: {summary['validation_qualified']}; assessment pass: {summary['assessment_qualified']}; higher-cost net check: {summary['cost_qualified']}.")
    html = '''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>G7 V4 failed-breakout research</title>
<style>body{font:16px/1.5 system-ui;color:#223248;margin:30px;max-width:1550px}h1{font-size:27px}.result{background:#eff4f8;padding:18px;border-left:5px solid #46789f}.scroll{overflow-x:auto}table{border-collapse:collapse;font-size:13px}th,td{border:1px solid #d1dae3;padding:8px;text-align:right}a{color:#135f9a}</style></head><body>'''
    html += '<h1>G7 V4: failed-breakout and trailing-exit research</h1><p class="result">' + status + ' All variants remain research only. No live or paper changes.</p>'
    html += '<p>Goal: more winning trades, fewer losing trades and at least5% less money lost, without sacrificing net profit or increasing daily-close drawdown versus the V4 symbol-guard control. Also require no lower win rate,90% trade retention,95% original V2 net retention and at least5 changed trades across3 days. The rules and criteria were written before this run.</p><ul>'
    html += ''.join('<li><b>' + k + '</b>: ' + v + '</li>' for k, v in POLICIES.items()) + '</ul>'
    html += '<p>Every candidate preserves original entries, native targets,1.2% initial stop, the1.0% stop after120 minutes, Nifty permission, fixed sizing and one active position per symbol. Completed-close signals act at the next open. Gaps use actual observed opens. All603 selected opportunities are reallocated after changed exits.</p>'
    primary = table.loc[table.reference.eq('v4_symbol_guard')]
    cols = ['policy', 'trades', 'wins', 'losses', 'win_rate_pct', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs']
    for period in ('all', 'october', 'development', 'validation', 'assessment', 'known_oct26', 'final_oct27'):
        html += '<h2>' + period + '</h2>' + show(primary.loc[primary.period.eq(period), cols])
    html += '<h2>Which trades helped or hurt?</h2>' + show(primary.loc[primary.period.eq('all') & primary.policy.isin(POLICIES),
        ['policy', 'net_change_rs', 'loss_amount_reduction_rs', 'loser_improved', 'loser_to_winner', 'winner_harmed', 'winner_to_loser', 'new_allocations']])
    html += '<h2>Development gate</h2>' + show(audit)
    html += '<h2>Cost sensitivity: all available sessions</h2><p>Round-trip basis points charged against exposure. A profitable gross exit may still be a net losing trade.</p>' + show(costs.loc[costs.period.eq('all'), ['policy', 'cost_bps', 'trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs']])
    html += '<h2>October daywise net P&amp;L</h2>' + show(daywise.loc[daywise.index.str.startswith('2026-10')].reset_index())
    html += '<h2>What this evidence can support</h2><p>59 available sessions from29 July through27 October2026. First35 sessions choose once; next14 and next8 test the locked candidate;26/27 October remain diagnostics. All dates have already been inspected in earlier research. These partitions do not create fresh out-of-sample evidence. No threshold changes, combinations or replacement selection are allowed after outcomes. The17 candidates in the preceding V4 entry/protection rounds were disclosed; this run adds exactly3.</p>'
    html += '<p>The inherited stock-day universe uses after-close eligibility and is not a validated live causal universe. Setup targets and other baseline parameters were previously examined/tuned. Thus even a rule passing these retrospective criteria would require a separately causal universe and unseen forward evidence. No later local session was found in date metadata. Missing source sessions24/25 August and1 October are excluded. Reported drawdown is daily-close, not intraday mark-to-market. Fixed Rs500,000 exposure permits theoretical fractional quantities and excludes separately modeled broker order constraints.</p>'
    html += '<h2>Files</h2><ul>' + ''.join(f'<li><a href="{p}">{p}</a></li>' for p in ('comparison.csv', 'october_daywise_net.csv', 'daywise_details.csv', 'trade_changes.csv', 'period_qualification.csv', 'cost_sensitivity.csv', 'protocol.json', 'selection_lock.json', 'summary.json')) + '</ul></body></html>'
    (out / 'report.html').write_text(html, encoding='utf-8')


def run(first):
    first = Path(first).resolve()
    protected = protected_hashes()
    out = OUT_ROOT / 'runs' / base.FINAL_DAY / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f') + '_three_fixed_rules')
    out.mkdir(parents=True, exist_ok=False)
    protocol = dict(version=VERSION, scope='BACKTEST_ONLY', execution_authority=False,
        round1_output=str(first), round1_manifest_sha256=sha(first / 'manifest.json'),
        candidates=POLICIES, candidate_count=3, selection=SELECTION,
        confirmation_failure=dict(consecutive_closes=2, edge='exact original confirmation candle, adverse side', strict=True),
        stalled_30m=dict(elapsed_minutes=30, max_gain_r=.25, current_return_lt_pct=0, once_only=True),
        confirmed_trail=dict(activation_target_fraction=.5, full_post_entry_bars=3, early_threshold_latched=False, next_open=True, never_loosen=True),
        costs_bps=[5, 15, 30], independent_holdout=False,
        stop_rule='Exactly3 candidates; no additional thresholds/combinations or later-period selection.',
        inherited_limitations=['after-close stock-day universe', 'previously inspected/tuned targets', 'all dates previously inspected'])
    dump(out / 'protocol.json', protocol)
    digest = sha(out / 'protocol.json')
    print('FIXED PROTOCOL', out / 'protocol.json', flush=True)
    base.verify(first)
    cache = Path(base.read(first / 'summary.json')['cache'])
    native, baseline, f, paths, source = base.load_cache(cache)
    cfg = dict(source['base_config'], max_positions_per_symbol=1)
    allowed = _flags(f.allowed_g7_v2, 'nifty')
    raw = replay(native, paths, source['base_config'])
    books = dict(g7_v2=allocate_variant(raw, allowed, source['base_config'], 'g7_v2'),
                 v4_symbol_guard=allocate_variant(raw, allowed, cfg, 'v4_symbol_guard'))
    same_book(books['g7_v2'], baseline)
    same_book(books['v4_symbol_guard'], pd.read_csv(first / 'v4_symbol_guard/portfolio_trades.csv'))
    for name in POLICIES:
        books[name] = allocate_variant(replay(native, paths, cfg, name), allowed, cfg, name)
    books = {name: tag(book, name, digest) for name, book in books.items()}
    training = comparisons(books, {'development': source['periods']['development']})
    chosen, audit = select(training, list(POLICIES))
    dump(out / 'selection_lock.json', dict(selected_candidate=chosen, selected_from='DEVELOPMENT_ONLY',
        locked_at_utc=datetime.now(timezone.utc).isoformat(), protocol_sha256=digest, replacement_forbidden=True))
    print('DEVELOPMENT SELECTED', chosen, flush=True)
    periods = dict(source['periods'], october=[d for d in source['periods']['all'] if d.startswith('2026-10')],
                   later_blocks=source['periods']['validation'] + source['periods']['assessment'])
    table = comparisons(books, periods)
    valid, va = select(table, [chosen], 'validation') if chosen else (None, [])
    assess, aa = select(table, [chosen], 'assessment') if chosen else (None, [])
    # All other period results are descriptive; they can never select a rule.
    descriptive = [r for period in periods for r in select(table, list(POLICIES), period)[1]]
    daily, costs, changes = [], [], []
    for name, book in books.items():
        folder = out / name
        folder.mkdir()
        book.to_csv(folder / 'portfolio_trades.csv', index=False)
        for day in periods['all']:
            daily.append(dict(policy=name, day=day, **metrics(book, [day])))
        for ref in prior.REFERENCES:
            changes.append(prior.differences(book, books[ref]).assign(policy=name, reference=ref))
        for cost in (5, 15, 30):
            stress = allocate_variant(book, allowed, source['base_config'] if name == 'g7_v2' else cfg, name, cost_bps=cost)
            for period, days in periods.items():
                costs.append(dict(policy=name, cost_bps=cost, period=period, **metrics(stress, days)))
    daily, costs = pd.DataFrame(daily), pd.DataFrame(costs)
    wide = daily.pivot(index='day', columns='policy', values='net_pnl_rs').reindex(columns=books)
    cost_ok = False
    if chosen:
        ct = costs.loc[costs.period.eq('later_blocks')].set_index(['policy', 'cost_bps'])
        cost_ok = all(ct.loc[(chosen, cost)].net_pnl_rs >= ct.loc[('v4_symbol_guard', cost)].net_pnl_rs - 1e-6 for cost in (15, 30))
    table.to_csv(out / 'comparison.csv', index=False)
    daily.to_csv(out / 'daywise_details.csv', index=False)
    wide.to_csv(out / 'daywise_net.csv')
    wide.loc[periods['october']].to_csv(out / 'october_daywise_net.csv')
    costs.to_csv(out / 'cost_sensitivity.csv', index=False)
    pd.concat(changes, ignore_index=True).to_csv(out / 'trade_changes.csv', index=False)
    pd.DataFrame(audit).to_csv(out / 'development_selection.csv', index=False)
    pd.DataFrame(descriptive).to_csv(out / 'period_qualification.csv', index=False)
    pd.DataFrame(va + aa, columns=['policy', 'period', 'eligible', 'failure_reasons']).to_csv(out / 'locked_candidate_checks.csv', index=False)
    summary = dict(version=VERSION, scope='BACKTEST_ONLY', execution_authority=False,
        cache=str(cache), cache_manifest_sha256=sha(cache / 'manifest.json'),
        round1_output=str(first), periods=periods, candidate_count=3, protocol_sha256=digest,
        selected_candidate=chosen, validation_qualified=chosen is not None and valid == chosen,
        assessment_qualified=chosen is not None and assess == chosen, cost_qualified=bool(cost_ok),
        retrospective_qualified=bool(chosen and valid == chosen and assess == chosen and cost_ok),
        independent_holdout=False, live_eligible=False, protected_files_unchanged=protected,
        selected_opportunities=len(native), model_cost_bps_round_trip=5)
    dump(out / 'summary.json', summary)
    write_report(out, summary, table, wide, pd.DataFrame(audit), costs)
    if protected_hashes() != protected or sha(out / 'protocol.json') != digest:
        raise ValueError('Protected files or fixed protocol changed')
    code = [Path(__file__), Path(engine.__file__), Path(prior.__file__), Path(base.__file__), base.CONFIG,
        base.ROOT / 'research/g8_exit_engine.py', base.ROOT / 'research/g3_sl_target_simulator.py',
        base.ROOT / 'tests/test_g7_v4_failure_exits.py', base.ROOT / 'tests/test_g7_v4_failure_research.py']
    base.seal(out, code={str(p.relative_to(base.ROOT)): sha(p) for p in code})
    verify(out)
    dump(OUT_ROOT / 'latest.json', dict(output=str(out), manifest_sha256=sha(out / 'manifest.json'), execution_authority=False))
    print(table.loc[table.reference.eq('v4_symbol_guard') & table.period.isin(['all', 'october', 'development', 'validation', 'assessment']),
        ['policy', 'period', 'trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs']].to_string(index=False), flush=True)
    print('OUTPUT', out, flush=True)
    return out


def verify(out):
    out = Path(out)
    verify_members(out)
    summary, manifest = base.read(out / 'summary.json'), base.read(out / 'manifest.json')
    for filename, digest in manifest['code'].items():
        if sha(base.ROOT / filename) != digest:
            raise ValueError('Code changed: ' + filename)
    if sha(out / 'protocol.json') != summary['protocol_sha256']:
        raise ValueError('Protocol changed')
    cache = Path(summary['cache'])
    verify_members(cache, summary['cache_manifest_sha256'])
    native, baseline, f, paths, source = base.load_cache(cache)
    allowed = _flags(f.allowed_g7_v2, 'nifty')
    books = {}
    for name in [*prior.REFERENCES, *POLICIES]:
        cfg = source['base_config'] if name == 'g7_v2' else dict(source['base_config'], max_positions_per_symbol=1)
        actual = allocate_variant(replay(native, paths, cfg, name if name in POLICIES else None), allowed, cfg, name)
        saved = pd.read_csv(out / name / 'portfolio_trades.csv')
        same_book(actual, saved)
        if name == 'g7_v2':
            same_book(actual, baseline)
        books[name] = saved
    table = comparisons(books, summary['periods'])
    saved = pd.read_csv(out / 'comparison.csv')
    for col in ('trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs', 'changed_executions', 'loser_to_winner', 'winner_to_loser'):
        np.testing.assert_allclose(table[col], saved[col], atol=1e-6, rtol=0)
    chosen, _ = select(table, list(POLICIES))
    if chosen != summary['selected_candidate'] or chosen != base.read(out / 'selection_lock.json')['selected_candidate']:
        raise ValueError('Selection changed')
    costs = pd.read_csv(out / 'cost_sensitivity.csv')
    exposure = source['base_config']['capital_per_entry_rs'] * source['base_config']['leverage']
    np.testing.assert_allclose(costs.cost_rs, costs.trades * exposure * costs.cost_bps / 10000, atol=1e-6, rtol=0)
    np.testing.assert_allclose(costs.net_pnl_rs, costs.gross_pnl_rs - costs.cost_rs, atol=1e-6, rtol=0)
    rebuilt_costs = []
    for name, book in books.items():
        cfg = source['base_config'] if name == 'g7_v2' else dict(source['base_config'], max_positions_per_symbol=1)
        for cost in (5, 15, 30):
            stress = allocate_variant(book, allowed, cfg, name, cost_bps=cost)
            for period, days in summary['periods'].items():
                rebuilt_costs.append(dict(policy=name, cost_bps=cost, period=period, **metrics(stress, days)))
    costs_actual = pd.DataFrame(rebuilt_costs)
    for col in ('trades', 'wins', 'losses', 'net_pnl_rs', 'cost_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs'):
        np.testing.assert_allclose(costs[col], costs_actual[col], rtol=0, atol=1e-6)
    valid = select(table, [chosen], 'validation')[0] if chosen else None
    assess = select(table, [chosen], 'assessment')[0] if chosen else None
    cost_ok = False
    if chosen:
        ct = costs_actual.loc[costs_actual.period.eq('later_blocks')].set_index(['policy', 'cost_bps'])
        cost_ok = all(ct.loc[(chosen, cost)].net_pnl_rs >= ct.loc[('v4_symbol_guard', cost)].net_pnl_rs - 1e-6 for cost in (15, 30))
    conclusions = dict(validation_qualified=chosen is not None and valid == chosen,
        assessment_qualified=chosen is not None and assess == chosen, cost_qualified=bool(cost_ok),
        retrospective_qualified=bool(chosen and valid == chosen and assess == chosen and cost_ok))
    for key, value in conclusions.items():
        if summary[key] != value:
            raise ValueError('Saved conclusion differs from replay: ' + key)
    daily = pd.read_csv(out / 'daywise_net.csv').set_index('day')
    for name, book in books.items():
        for day in summary['periods']['all']:
            if not np.isclose(daily.loc[day, name], metrics(book, [day])['net_pnl_rs'], atol=1e-6, rtol=0):
                raise ValueError('Daily P&L mismatch')
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--round1', type=Path)
    mode.add_argument('--verify', type=Path)
    args = parser.parse_args()
    if args.verify:
        print('VERIFIED', verify(args.verify))
    else:
        run(args.round1)
