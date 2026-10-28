"""Bounded G7 V4 exit research; never changes entries or publishes a strategy.

Run: python -m research.g7_v4_profit_protection --round1 PATH
Verify: python -m research.g7_v4_profit_protection --verify PATH
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_v4_backtest as base
from research.g3_sl_target_simulator import simulate_exit
from research.g7_entry_quality_20261026 import same_book, verify_members
from research.g8_exit_engine import _flags, allocate_variant, metrics, replay_exits
from research.g8_vix_selection_research import dump, sha

VERSION = 'V13-V10-G-7-V4-PROFIT-PROTECTION-RESEARCH'
POLICIES = {
    'breakeven_half_r': dict(activation_r=.5, protected_profit_pct=0.),
    'breakeven_one_r': dict(activation_r=1., protected_profit_pct=0.),
    'cushion_half_r': dict(activation_r=.5, protected_profit_pct=.1),
}
REFERENCES = ('g7_v2', 'v4_symbol_guard')
OUT_ROOT = base.OUT_ROOT / 'exit_research'


def replay(native, paths, config, policy=None):
    """Replay all selected opportunities before allocating the portfolio again."""
    if policy is not None and policy not in POLICIES:
        raise ValueError('Unknown fixed policy')
    indexed = native.set_index('sid')
    raw = replay_exits(native, paths, indexed.native_stop_pct,
                       indexed.native_target_pct, config)
    raw['protection_activation_pct'] = np.nan
    raw['protected_profit_pct'] = 0.
    if policy is None:
        return raw
    spec = POLICIES[policy]
    for index, row in raw.iterrows():
        activation = float(row.native_stop_pct) * spec['activation_r']
        raw.at[index, 'protection_activation_pct'] = activation
        raw.at[index, 'protected_profit_pct'] = spec['protected_profit_pct']
        if not row.g8_baseline_filled:
            continue
        result = simulate_exit(
            paths[int(row.sid)], int(row.entry_path_index), float(row.entry_price),
            row.side == 'LONG', float(row.native_stop_pct), float(row.native_target_pct),
            protect_after_close_pct=activation,
            protected_profit_pct=spec['protected_profit_pct'],
            tighten_after_minutes=config['tighten_after_minutes'],
            tightened_stop_pct=config['tightened_stop_pct'])
        for field, value in result.items():
            raw.at[index, field] = value
        raw.at[index, 'net_return_pct'] = result['gross_return_pct'] - row.cost_pct
    # Monetary amounts are sized afresh by allocate_variant, after exit replay.
    return raw


def differences(book, reference):
    """Exit-aware comparison; an unchanged entry can have a changed outcome."""
    a = book.set_index('sid')
    b = reference.set_index('sid').loc[a.index]
    ax = _flags(a.portfolio_executed, 'executed')
    bx = _flags(b.portfolio_executed, 'reference executed')
    both = ax & bx
    price_changed = ~np.isclose(a.exit_price, b.exit_price, atol=1e-8, rtol=0, equal_nan=True)
    time_changed = pd.to_datetime(a.exit_ts, utc=True).ne(pd.to_datetime(b.exit_ts, utc=True))
    delta = a.portfolio_net_profit_rupees - b.portfolio_net_profit_rupees
    changed = ax.ne(bx) | (both & (price_changed | time_changed | delta.abs().gt(1e-6)))
    return pd.DataFrame(dict(
        sid=a.index, day=a.day, tradingsymbol=a.tradingsymbol, side=a.side,
        entry_ts=a.entry_ts, reference_executed=bx, variant_executed=ax,
        reference_exit_ts=b.exit_ts, variant_exit_ts=a.exit_ts,
        reference_exit_price=b.exit_price, variant_exit_price=a.exit_price,
        reference_exit_reason=b.exit_reason, variant_exit_reason=a.exit_reason,
        reference_net_rs=b.portfolio_net_profit_rupees,
        variant_net_rs=a.portfolio_net_profit_rupees, net_change_rs=delta,
        changed_execution=changed,
        loser_improved=both & b.portfolio_net_profit_rupees.lt(0) & delta.gt(1e-6),
        loser_to_winner=both & b.portfolio_net_profit_rupees.lt(0) & a.portfolio_net_profit_rupees.gt(1e-8),
        winner_harmed=both & b.portfolio_net_profit_rupees.gt(0) & delta.lt(-1e-6),
        winner_to_loser=both & b.portfolio_net_profit_rupees.gt(0) & a.portfolio_net_profit_rupees.lt(-1e-8),
        new_allocation=ax & ~bx, removed_allocation=bx & ~ax,
    )).reset_index(drop=True)


def comparison(book, reference, days):
    stats = base.comparison_row(book, reference, days)
    delta = differences(book, reference)
    delta = delta.loc[delta.day.isin(days)]
    changed = delta.changed_execution
    stats.update(changed_executions=int(changed.sum()),
                 changed_days=int(delta.loc[changed, 'day'].nunique()))
    for column in ('loser_improved', 'loser_to_winner', 'winner_harmed', 'winner_to_loser'):
        stats[column] = int(delta[column].sum())
    executed = book.loc[book.day.isin(days) & _flags(book.portfolio_executed, 'executed')]
    stats['protected_stop_exits'] = int(executed.exit_reason.eq('PROTECTED_STOP').sum())
    stats['protection_applied_trades'] = int(_flags(executed.protection_applied, 'protected').sum())
    return stats


def comparisons(books, periods):
    return pd.DataFrame([
        dict(policy=name, reference=ref, period=period, **comparison(book, books[ref], days))
        for name, book in books.items() for ref in REFERENCES
        for period, days in periods.items()
    ])


def select(table, policies, period='development'):
    """Keep the previous acceptance floors against BOTH original and guard control.

    Only the named period is read. Rank eligible candidates by money saved on
    losing trades against the guard control, then net profit, then policy name.
    """
    current = table.loc[table.period.eq(period)].set_index(['reference', 'policy'])
    audit, eligible = [], []
    for policy in policies:
        failed = False
        for ref in REFERENCES:
            r, b = current.loc[(ref, policy)], current.loc[(ref, ref)]
            reasons = []
            if r.net_pnl_rs < .95 * b.net_pnl_rs - 1e-6:
                reasons.append('net profit retention below 95%')
            if r.negative_trade_sum_abs_rs > .95 * b.negative_trade_sum_abs_rs + 1e-6:
                reasons.append('losing-trade amount reduction below 5%')
            if r.trades < .70 * b.trades:
                reasons.append('trade retention below 70%')
            if r.wins < .85 * b.wins:
                reasons.append('winner retention below 85%')
            if r.daily_close_drawdown_rs > b.daily_close_drawdown_rs + 1e-6:
                reasons.append('larger daily-close drawdown')
            if r.changed_executions < 5 or r.changed_days < 3:
                reasons.append('insufficient changed trades/days')
            failed |= bool(reasons)
            audit.append(dict(policy=policy, reference=ref, period=period,
                              eligible=not reasons, failure_reasons='; '.join(reasons)))
        if not failed:
            r = current.loc[('v4_symbol_guard', policy)]
            eligible.append((r.loss_amount_reduction_rs, r.net_pnl_rs, policy))
    return (max(eligible)[2] if eligible else None), audit


def giveback_audit(book, paths):
    """Diagnostic only: exclude entry and exit bars with unknown intrabar order."""
    records = []
    for row in book.loc[_flags(book.portfolio_executed, 'executed')].itertuples():
        start, end = int(row.entry_path_index) + 1, int(row.exit_path_index)
        closes = paths[int(row.sid)]['close'][start:end]
        signed = (1 if row.side == 'LONG' else -1) * (closes / row.entry_price - 1) * 100
        best = float(np.max(signed)) if len(signed) else np.nan
        records.append(dict(sid=row.sid, day=row.day, tradingsymbol=row.tradingsymbol,
            side=row.side, entry_ts=row.entry_ts, exit_ts=row.exit_ts,
            exit_reason=row.exit_reason, net_pnl_rs=row.portfolio_net_profit_rupees,
            observed_full_post_entry_closes=len(closes), best_completed_close_pct=best,
            losing_after_half_r=bool(row.portfolio_net_profit_rupees < 0 and best >= .6),
            losing_after_one_r=bool(row.portfolio_net_profit_rupees < 0 and best >= 1.2)))
    return pd.DataFrame(records)


def tagged(book, name, digest):
    book = book.copy()
    book['strategy'] = 'V13-V10-G-7'
    book['strategy_version'] = VERSION
    book['v4_exit_policy'] = name
    book['v4_protocol_sha256'] = digest
    book['strategy_fingerprint'] = sha256((str(book.strategy_fingerprint.iloc[0]) + digest).encode()).hexdigest()
    return book


def report(out, summary, table, daily, audit, giveback):
    show = lambda frame: '<div class="scroll">' + frame.to_html(index=False, float_format=lambda x: f'{x:,.2f}') + '</div>'
    selected = summary['selected_candidate']
    status = ('No candidate passed the fixed development screen.' if selected is None else
              f'Development selected {selected}; validation passed: {summary["validation_qualified"]}; assessment passed: {summary["assessment_qualified"]}.')
    html = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>G7 V4 profit protection</title><style>body{font:16px/1.5 system-ui;margin:30px;color:#203047;max-width:1500px}table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border:1px solid #ccd4de;text-align:right}.scroll{overflow-x:auto}.result{background:#edf3f8;padding:18px;border-left:5px solid #47779d}a{color:#165f9c}</style></head><body>"""
    html += '<h1>G7 V4: profit protection through 27 October 2026</h1><p class="result">' + status + ' Research only; no live or paper strategy changed.</p>'
    html += '<p>Three rules were fixed before this run: move the stop to entry after a completed close at +0.6% (half the initial risk) or +1.2% (one initial risk), or protect +0.1% after +0.6%. Protection starts at the next candle open. Original setup targets, Nifty entry permission, sizing and the 120-minute stop tightening remain in force.</p>'
    html += '<p>Each candidate keeps one active position per symbol and reallocates the full set of603 selected opportunities after exits change. Compare with both G7 V2 and the symbol-guard control. A stop at entry still loses the modeled transaction cost; stop gaps can lose more.</p>'
    cols = ['policy', 'trades', 'wins', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs']
    primary = table.loc[table.reference.eq('v4_symbol_guard')]
    for period in ('all', 'october', 'development', 'validation', 'assessment', 'known_oct26', 'final_oct27'):
        html += '<h2>' + period + '</h2>' + show(primary.loc[primary.period.eq(period), cols])
    html += '<h2>Effect beyond the symbol guard</h2>' + show(primary.loc[primary.period.eq('all') & primary.policy.isin(POLICIES), ['policy', 'net_change_rs', 'loss_amount_reduction_rs', 'loser_improved', 'loser_to_winner', 'winner_harmed', 'winner_to_loser', 'new_allocations']])
    html += '<h2>Selection audit</h2><p>The same 95% net-profit, 5% loss-reduction, 70% trade-retention and 85% winner-retention floors apply against BOTH controls. Daily-close drawdown may not increase; at least5 changed trades over3 days are required. Development alone chooses once; validation cannot select a replacement.</p>' + show(audit)
    losses = giveback.loc[giveback.net_pnl_rs.lt(0)]
    html += f'<h2>Reversal diagnosis</h2><p>Of {len(losses)} losing trades in the symbol-guard control, {int(losses.losing_after_half_r.sum())} had a full post-entry close at least+0.6%, and {int(losses.losing_after_one_r.sum())} at least+1.2%, before their exit candle. These observations are diagnostic and never feed entry decisions.</p>'
    html += '<h2>October daywise net P&amp;L</h2>' + show(daily.loc[daily.index.str.startswith('2026-10')].reset_index())
    html += '<h2>Validation and limits</h2><p>59 available sessions,29 July to27 October. Chronology:35 development,14 validation,8 assessment, then the known26 October and retrospective27 October checks. All dates were previously inspected: this is not fresh out-of-sample evidence. Three missing source sessions (24/25 August,1 October) are excluded. No result is filled in as zero for missing data.</p>'
    html += '<p>Net results use5bps round-trip cost on fixed Rs500,000 exposure per trade; sensitivity at15/30bps is exported. Fractional theoretical shares, original universe/target-selection limitations and minute-bar stop-first execution assumptions are inherited. Drawdown is measured at daily close, not intraday mark-to-market. All candidates are disclosed; none is chosen by October performance.</p>'
    html += '<h2>Files</h2><ul>' + ''.join(f'<li><a href="{name}">{name}</a></li>' for name in ('daywise_net.csv', 'october_daywise_net.csv', 'comparison.csv', 'trade_changes.csv', 'giveback_audit.csv', 'cost_sensitivity.csv', 'protocol.json', 'selection_lock.json', 'summary.json')) + '</ul></body></html>'
    (out / 'report.html').write_text(html, encoding='utf-8')


def run(first):
    first = Path(first).resolve()
    protected = base.protected_hashes()
    out = OUT_ROOT / 'runs' / base.FINAL_DAY / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f') + '_fixed_exit_rules')
    out.mkdir(parents=True, exist_ok=False)
    protocol = dict(version=VERSION, scope='BACKTEST_ONLY', execution_authority=False,
        candidates=POLICIES, candidate_count=3, round1_output=str(first),
        round1_manifest_sha256=sha(first / 'manifest.json'),
        selection='Same prior acceptance floors applied against BOTH V2 and symbol-guard control; select development once, no replacement after validation.',
        ranking='Maximum development losing-trade amount reduction vs guard, then net profit, then policy name.',
        assessment='Selected candidate must have net >= both controls, losing amount < both controls and daily-close drawdown <= both controls.',
        timing='Completed post-entry close only; entry candle excluded; protective stop active next candle open; gaps fill at observed open; same-bar tie stop first.',
        unchanged='Entries, Nifty permission, setup targets, initial1.2% stop, timed1% stop after120 minutes, fixed exposure.',
        independent_holdout=False, new_threshold_search_forbidden=True,
        thresholds=base.read(base.CONFIG)['selection'])
    dump(out / 'protocol.json', protocol)
    digest = sha(out / 'protocol.json')
    print('PROTOCOL', out / 'protocol.json', flush=True)
    base.verify(first)
    cache = Path(base.read(first / 'summary.json')['cache'])
    native, baseline, features, paths, source = base.load_cache(cache)
    cfg = dict(source['base_config'], max_positions_per_symbol=1)
    allowed = _flags(features.allowed_g7_v2, 'nifty')
    raw = replay(native, paths, source['base_config'])
    books = {'g7_v2': allocate_variant(raw, allowed, source['base_config'], 'g7_v2'),
             'v4_symbol_guard': allocate_variant(raw, allowed, cfg, 'v4_symbol_guard')}
    same_book(books['g7_v2'], baseline)
    same_book(books['v4_symbol_guard'], pd.read_csv(first / 'v4_symbol_guard/portfolio_trades.csv'))
    for name in POLICIES:
        books[name] = allocate_variant(replay(native, paths, cfg, name), allowed, cfg, name)
    books = {name: tagged(book, name, digest) for name, book in books.items()}
    training = comparisons(books, {'development': source['periods']['development']})
    chosen, audit = select(training, list(POLICIES))
    dump(out / 'selection_lock.json', dict(selected_candidate=chosen, selected_from='DEVELOPMENT_ONLY',
        locked_at_utc=datetime.now(timezone.utc).isoformat(), protocol_sha256=digest,
        validation_cannot_select_replacement=True))
    print('DEVELOPMENT SELECTED', chosen, flush=True)
    periods = dict(source['periods'], october=[d for d in source['periods']['all'] if d.startswith('2026-10')])
    table = comparisons(books, periods)
    validated, validation_audit = select(table, [chosen], 'validation') if chosen else (None, [])
    assessed = False
    if chosen and validated == chosen:
        t = table.loc[table.period.eq('assessment')].set_index(['reference', 'policy'])
        assessed = all(t.loc[(ref, chosen)].net_pnl_rs >= t.loc[(ref, ref)].net_pnl_rs - 1e-6
            and t.loc[(ref, chosen)].negative_trade_sum_abs_rs < t.loc[(ref, ref)].negative_trade_sum_abs_rs
            and t.loc[(ref, chosen)].daily_close_drawdown_rs <= t.loc[(ref, ref)].daily_close_drawdown_rs + 1e-6 for ref in REFERENCES)
    daily_rows, costs, changes = [], [], []
    for name, book in books.items():
        folder = out / name
        folder.mkdir()
        book.to_csv(folder / 'portfolio_trades.csv', index=False)
        for day in periods['all']:
            daily_rows.append(dict(policy=name, day=day, **metrics(book, [day])))
        for ref in REFERENCES:
            changes.append(differences(book, books[ref]).assign(policy=name, reference=ref))
        for cost in (5, 15, 30):
            stressed = allocate_variant(book, allowed, source['base_config'] if name == 'g7_v2' else cfg, name, cost_bps=cost)
            for period, days in periods.items():
                costs.append(dict(policy=name, cost_bps=cost, period=period, **metrics(stressed, days)))
    daily = pd.DataFrame(daily_rows)
    wide = daily.pivot(index='day', columns='policy', values='net_pnl_rs').reindex(columns=books)
    diagnostic = giveback_audit(books['v4_symbol_guard'], paths)
    table.to_csv(out / 'comparison.csv', index=False)
    daily.to_csv(out / 'daywise_details.csv', index=False)
    wide.to_csv(out / 'daywise_net.csv')
    wide.loc[periods['october']].to_csv(out / 'october_daywise_net.csv')
    pd.DataFrame(audit).to_csv(out / 'development_selection.csv', index=False)
    pd.DataFrame(validation_audit, columns=['policy', 'reference', 'period', 'eligible', 'failure_reasons']).to_csv(out / 'validation_selection.csv', index=False)
    pd.concat(changes, ignore_index=True).to_csv(out / 'trade_changes.csv', index=False)
    diagnostic.to_csv(out / 'giveback_audit.csv', index=False)
    pd.DataFrame(costs).to_csv(out / 'cost_sensitivity.csv', index=False)
    summary = dict(version=VERSION, scope='BACKTEST_ONLY', execution_authority=False,
        cache=str(cache), cache_manifest_sha256=sha(cache / 'manifest.json'),
        round1_output=str(first), periods=periods, candidate_count=3, protocol_sha256=digest,
        selected_candidate=chosen, validation_qualified=chosen is not None and validated == chosen,
        assessment_qualified=bool(assessed), independent_holdout=False,
        protected_files_unchanged=protected, total_selected_opportunities=len(native))
    dump(out / 'summary.json', summary)
    report(out, summary, table, wide, pd.DataFrame(audit), diagnostic)
    if sha(out / 'protocol.json') != digest or base.protected_hashes() != protected:
        raise ValueError('Protected file or protocol changed during research')
    code = [Path(__file__), Path(base.__file__), base.CONFIG,
            base.ROOT / 'research/g3_sl_target_simulator.py', base.ROOT / 'research/g8_exit_engine.py',
            base.ROOT / 'tests/test_g7_v4_profit_protection.py']
    base.seal(out, code={str(p.relative_to(base.ROOT)): sha(p) for p in code})
    verify(out)
    dump(OUT_ROOT / 'latest.json', dict(output=str(out), manifest_sha256=sha(out / 'manifest.json'), execution_authority=False))
    print(table.loc[table.reference.eq('v4_symbol_guard') & table.period.isin(['all', 'october', 'development', 'validation', 'assessment']),
          ['policy', 'period', 'trades', 'losses', 'net_pnl_rs', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs']].to_string(index=False), flush=True)
    print('OUTPUT', out, flush=True)
    return out


def verify(out):
    out = Path(out)
    verify_members(out)
    summary, manifest = base.read(out / 'summary.json'), base.read(out / 'manifest.json')
    for filename, digest in manifest['code'].items():
        if sha(base.ROOT / filename) != digest:
            raise ValueError('Research code changed: ' + filename)
    if sha(out / 'protocol.json') != summary['protocol_sha256']:
        raise ValueError('Protocol changed')
    cache = Path(summary['cache'])
    verify_members(cache, summary['cache_manifest_sha256'])
    native, baseline, f, paths, source = base.load_cache(cache)
    allowed = _flags(f.allowed_g7_v2, 'nifty')
    books = {}
    for name in [*REFERENCES, *POLICIES]:
        cfg = source['base_config'] if name == 'g7_v2' else dict(source['base_config'], max_positions_per_symbol=1)
        raw = replay(native, paths, cfg, name if name in POLICIES else None)
        actual = allocate_variant(raw, allowed, cfg, name)
        saved = pd.read_csv(out / name / 'portfolio_trades.csv')
        same_book(actual, saved)
        if name == 'g7_v2':
            same_book(actual, baseline)
        books[name] = saved
        executed = _flags(saved.portfolio_executed, 'executed')
        np.testing.assert_allclose(saved.loc[executed, 'portfolio_net_profit_rupees'],
            saved.loc[executed, 'portfolio_gross_profit_rupees'] - saved.loc[executed, 'portfolio_cost_rupees'], rtol=0, atol=1e-6)
    actual = comparisons(books, summary['periods'])
    saved = pd.read_csv(out / 'comparison.csv')
    for key in ('net_pnl_rs', 'losses', 'negative_trade_sum_abs_rs', 'daily_close_drawdown_rs', 'changed_executions', 'changed_days', 'winner_harmed', 'loser_improved'):
        np.testing.assert_allclose(actual[key], saved[key], rtol=0, atol=1e-6)
    selected, _ = select(actual, list(POLICIES))
    if selected != summary['selected_candidate'] or selected != base.read(out / 'selection_lock.json')['selected_candidate']:
        raise ValueError('Selection changed')
    costs = pd.read_csv(out / 'cost_sensitivity.csv')
    exposure = source['base_config']['capital_per_entry_rs'] * source['base_config']['leverage']
    np.testing.assert_allclose(costs.cost_rs, costs.trades * exposure * costs.cost_bps / 10000, atol=1e-6, rtol=0)
    np.testing.assert_allclose(costs.net_pnl_rs, costs.gross_pnl_rs - costs.cost_rs, atol=1e-6, rtol=0)
    daily = pd.read_csv(out / 'daywise_net.csv').set_index('day')
    for name, book in books.items():
        for day in summary['periods']['all']:
            if not np.isclose(daily.loc[day, name], metrics(book, [day])['net_pnl_rs'], atol=1e-6, rtol=0):
                raise ValueError('Daily accounting changed')
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--round1', type=Path)
    mode.add_argument('--verify', type=Path)
    args = parser.parse_args()
    if args.verify:
        print('VERIFIED', verify(args.verify))
    else:
        run(args.round1)


if __name__ == '__main__':
    main()
