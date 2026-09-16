"""Finite, registered V10-G threshold study on previously reviewed history."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g
from fno_v13_v10_d_research import detailed_daily

f, r, C = g.f, g.r, g.SelectionChange


def candidates():
    atoms = {
        'PRICE085': dict(price_multiplier=.85), 'PRICE075': dict(price_multiplier=.75),
        'PRICE065': dict(price_multiplier=.65), 'OI075': dict(oi_multiplier=.75),
        'OI050': dict(oi_multiplier=.5), 'BODY005': dict(body_reduction=.05),
        'BODY010': dict(body_reduction=.1), 'WICK010': dict(wick_increase=.1),
        'PRICE085_BODY005': dict(price_multiplier=.85, body_reduction=.05),
        'PRICE075_BODY005': dict(price_multiplier=.75, body_reduction=.05),
        'PRICE085_OI075': dict(price_multiplier=.85, oi_multiplier=.75),
        'BODY005_OI075': dict(body_reduction=.05, oi_multiplier=.75),
    }
    result = {'F_CONTROL': C(), 'EXTRA_BOTH': C(extra_setup_entries=1)}
    for side in ('LONG', 'SHORT', 'BOTH'):
        for name, params in atoms.items():
            for extra in (0, 1):
                result[f'{name}_{side}_EXTRA{extra}'] = C(**params, expansion_side=side, extra_setup_entries=extra)
    assert len(result) == 74
    return result


def plan():
    return dict(candidate_count=74, candidates={name: asdict(c) for name, c in candidates().items()},
        fixed='F first-choice selections; both volume gates; all 14 setup times; 10min expiry; frozen F/B exits; native 1m stop-first replay; flat 5bps costs',
        portfolio=dict(allocated_per_trade=100_000, total_capacity=1_000_000, leverage=5, max_positions=None),
        acceptance=dict(minimum_trades_per_session=2.8, maximum_trades_per_session=3.2,
            minimum_win_rate_pct=62., minimum_profit_factor=3.30,
            maximum_daily_close_drawdown_vs_F=1.20, minimum_net_vs_F=1., added_trades_net_positive=True),
        selection='Passing full objective: fewest changed parameter families, closest to 3/day, highest PF, name. No adaptive follow-up sweep.',
        fallback='If no full pass: quality-passing expansion with most trades up to 3.2/day, then fewest changed families, PF, name. If none: F control. Explicitly mark frequency objective unmet.',
        diagnostic='One native-reranking comparator at the chosen thresholds; not eligible for choice. 9bps stress, months, extra-trade contribution and 1x sizing are descriptive.',
        evidence=g.EVIDENCE,
        effective_floors='Frozen strict signals already require 5m price >=0.10% and OI >=0.05%; no claim to test below these floors.',
        scope='No per-date, per-symbol, per-setup or outcome-dependent optimization. Body/wick changes apply to 1m confirmation candle.',
    )


def keys(frame):
    return set(zip(frame.sid.astype(int), frame.setup_id.astype(str)))


def executed(frame):
    return frame.loc[frame.portfolio_executed.eq(True)]


def signature(orders):
    return hashlib.sha256(repr(sorted(keys(orders))).encode()).hexdigest()


def assess(metrics, change, baseline, added, days):
    row = dict(metrics, changed_families=change.complexity(), trades_per_day=metrics['trades'] / len(days),
               extra_trades=added['trades'], extra_net_rupees=added['net_profit_rupees'],
               extra_win_rate_pct=added['win_rate_pct'], extra_profit_factor=added['profit_factor'])
    failures = []
    if row['win_rate_pct'] < 62.: failures.append('WIN_RATE')
    if row['profit_factor'] < 3.30: failures.append('PF')
    if row['daily_close_drawdown_rupees'] > baseline['daily_close_drawdown_rupees'] * 1.20 + 1e-7: failures.append('DRAWDOWN')
    if row['net_profit_rupees'] < baseline['net_profit_rupees'] - 1e-7: failures.append('NET')
    if change.complexity() and added['net_profit_rupees'] <= 0: failures.append('ADDITIONAL_TRADES_NET')
    row['quality_pass'] = not failures
    row['frequency_pass'] = 2.8 <= row['trades_per_day'] <= 3.2
    row['objective_pass'] = row['quality_pass'] and row['frequency_pass']
    row['failed_gates'] = '|'.join(failures + ([] if row['frequency_pass'] else ['FREQUENCY']))
    row['distance_from_three'] = abs(row['trades_per_day'] - 3.)
    return row


def choose(table):
    passing = table.loc[table.objective_pass]
    if not passing.empty:
        ordered = passing.sort_values(['changed_families', 'distance_from_three', 'profit_factor', 'candidate'],
                                      ascending=[True, True, False, True], kind='stable')
        return ordered.iloc[0].candidate, 'HISTORICAL_OBJECTIVE_MET_EXPLORATORY'
    partial = table.loc[table.quality_pass & table.trades.gt(table.loc[table.candidate.eq('F_CONTROL'), 'trades'].iloc[0])
                        & table.trades_per_day.le(3.2)]
    if not partial.empty:
        ordered = partial.sort_values(['trades', 'changed_families', 'profit_factor', 'candidate'],
                                      ascending=[False, True, False, True], kind='stable')
        return ordered.iloc[0].candidate, 'QUALITY_PRESERVED_FREQUENCY_OBJECTIVE_NOT_MET'
    return 'F_CONTROL', 'NO_QUALIFYING_IMPROVEMENT_RETAIN_F'


def finish_manifest(output):
    files = [Path(__file__), Path(g.__file__), Path(f.__file__), Path(f.b.__file__),
             Path(f.v10.__file__), Path(f.v9.__file__), Path(f.v9.v5.__file__), Path(f.v9.v6.__file__),
             Path(f.v9.v5.replay.__file__), Path(r.__file__), Path('fno_v13_v10_d_research.py'),
             Path('tests/test_fno_v13_v10_g.py')]
    snapshot = output / 'source_snapshot'
    snapshot.mkdir(exist_ok=True)
    for path in files:
        if path.is_file(): shutil.copy2(path, snapshot / path.name)
    r.dump(output / 'research_manifest.json', dict(complete=True,
        code_sha256={str(p.resolve()): f.v10.sha(p) for p in files if p.is_file()},
        artifacts={str(p.relative_to(output)): f.v10.sha(p) for p in sorted(output.rglob('*'))
                   if p.is_file() and p.name != 'research_manifest.json'}))


def run(output=g.THRESHOLD_OUTPUT, *, plan_only=False):
    output.mkdir(parents=True, exist_ok=True)
    protocol = plan()
    path = output / 'experiment_plan.json'
    if path.exists():
        previous = json.loads(path.read_text())
        if previous['protocol'] != protocol:
            raise ValueError('Existing registered plan differs; do not silently change experiments')
    else:
        r.dump(path, dict(registered_utc=datetime.now(timezone.utc).isoformat(), protocol=protocol))
    if plan_only:
        print(f'Registered {len(candidates())} configurations: {path}')
        return
    source = g.load_source()
    settings_f = g.frozen_f_settings()
    settings_exit = settings_f['exit']
    r.dump(output / 'source_verification.json', source['source_verification'])
    days = source['days']
    groups = {'FULL': days, **{name: [d for d in days if d.startswith(prefix)]
              for name, prefix in [('JULY', '2026-07'), ('AUGUST', '2026-08'), ('SEPTEMBER', '2026-09')]}}
    book = candidates()
    # Finish all decisions from frozen signal features before reading forward paths.
    audits = {name: g.selection_audit(source['signals'], source['v9_config'], change) for name, change in book.items()}
    orders = {name: audit.loc[audit.v9_selected].reset_index(drop=True) for name, audit in audits.items()}
    expected_f = f.select_orders(source['signals'], source['v9_config'])
    assert keys(orders['F_CONTROL']) == keys(expected_f)
    assert all(keys(expected_f).issubset(keys(frame)) for frame in orders.values())
    opportunity = pd.DataFrame([dict(candidate=name, eligible=len(audits[name]), selected=len(frame),
                                    selection_sha256=signature(frame)) for name, frame in orders.items()])
    opportunity.to_csv(output / 'outcome_blind_opportunity_counts.csv', index=False)
    union = pd.concat(list(orders.values()), ignore_index=True).drop_duplicates(['sid', 'setup_id'])
    with np.load(g.DEFAULT_SOURCE / 'dataset/paths.npz', allow_pickle=False) as archive:
        shared = f.orders_dataset(source, source['signals'], archive, union)
    print(f'All frozen artifacts verified; {len(book)} configurations, {opportunity.selection_sha256.nunique()} unique selections, {len(union)} union orders.', flush=True)
    results, rows, assessments = {}, [], []
    for i, (name, change) in enumerate(book.items()):
        settings = g.config(settings_exit, change)
        result = g.evaluate({**shared, 'orders': orders[name]}, settings)
        results[name] = result
        ledger = result[1]
        r.save(output / 'candidates' / name, *result)
        if name == 'F_CONTROL':
            corrected_manifest = json.loads((g.F_CORRECTED.parent / 'manifest.json').read_text())
            corrected_hashes = {k.replace('\\', '/'): v for k, v in corrected_manifest['artifacts'].items()}
            corrected_key = 'CAPACITY_CORRECTED_5X/portfolio_trades.csv'
            if f.v10.sha(g.F_CORRECTED / 'portfolio_trades.csv') != corrected_hashes[corrected_key]:
                raise ValueError('Corrected F ledger drift')
            prior = pd.read_csv(g.F_CORRECTED / 'portfolio_trades.csv', float_precision='round_trip')
            proof = f.v9.assert_control_parity(prior, ledger)
            prior_pnl = prior.set_index(['sid', 'setup_id']).portfolio_net_profit_rupees.sort_index()
            current_pnl = ledger.set_index(['sid', 'setup_id']).portfolio_net_profit_rupees.sort_index()
            np.testing.assert_allclose(prior_pnl, current_pnl, atol=1e-7, rtol=0)
            proof['portfolio_pnl_parity'] = True
            r.dump(output / 'corrected_f_parity.json', proof)
            baseline = r.metric(ledger, days)
            core_executed = keys(executed(ledger))
        new = ledger.loc[[(int(row.sid), str(row.setup_id)) not in keys(expected_f) for row in ledger.itertuples()]]
        full = r.metric(ledger, days)
        assessment = assess(full, change, baseline, r.metric(new, days), days)
        assessment.update(candidate=name, retained_f_executions=len(keys(executed(ledger)) & core_executed))
        assessments.append(assessment)
        for period, period_days in groups.items():
            for cost in (5, 9):
                rows.append(dict(candidate=name, period=period, sessions=len(period_days),
                                 **r.metric(ledger, period_days, cost_bps=cost)))
        if i % 10 == 0:
            print(f'Replayed {i+1}/{len(book)}: {name}', flush=True)
    sweep = pd.DataFrame(rows)
    sweep.to_csv(output / 'parameter_sweep_all_periods.csv', index=False)
    assessment = pd.DataFrame(assessments).merge(opportunity, on='candidate', validate='one_to_one')
    assessment.to_csv(output / 'candidate_assessment.csv', index=False)
    chosen, status = choose(assessment)
    change = book[chosen]
    frozen = g.config(settings_exit, change)
    r.dump(output / 'frozen_config.json', frozen)
    control, final = results['F_CONTROL'], results[chosen]
    for name, result in [('V13_V10_F_CONTROL', control), ('V13_V10_G', final)]:
        r.save(output / 'final' / name, *result)
    audits[chosen].to_csv(output / 'selection_audit.csv', index=False)
    # Native reranking is diagnostic only, never another candidate to choose.
    native_orders = g.select_orders(source['signals'], source['v9_config'], change, core_first=False)
    with np.load(g.DEFAULT_SOURCE / 'dataset/paths.npz', allow_pickle=False) as archive:
        native_data = f.orders_dataset(source, source['signals'], archive, native_orders)
    native = g.evaluate(native_data, g.config(settings_exit, change, core_first=False))
    r.save(output / 'diagnostics/native_reranking', *native)
    ledger = final[1]
    core_keys = keys(executed(control[1]))
    added = ledger.loc[[(int(row.sid), str(row.setup_id)) not in keys(expected_f) for row in ledger.itertuples()]]
    added.to_csv(output / 'additional_entries.csv', index=False)
    comparison = []
    for name, frame in [('V10-F', control[1]), ('V10-G', ledger), ('G_ADDITIONAL', added), ('G_NATIVE_DIAGNOSTIC', native[1])]:
        for period, period_days in groups.items():
            for cost in (5, 9):
                comparison.append(dict(version=name, period=period, sessions=len(period_days),
                                       **r.metric(frame, period_days, cost_bps=cost)))
    comparison = pd.DataFrame(comparison)
    comparison.to_csv(output / 'comparison_metrics.csv', index=False)
    daily_f, daily_g = detailed_daily(control[1], days), detailed_daily(ledger, days)
    daily_g.to_csv(output / 'daily_detailed.csv', index=False)
    daily_compare = daily_f.merge(daily_g, on='day', suffixes=('_F', '_G'), validate='one_to_one')
    daily_compare.to_csv(output / 'daily_comparison.csv', index=False)
    threshold_rows = []
    for original in f.v9.v5.profile_setups(f.v9.v5.PROFILES['higher_frequency']):
        core, expanded = g.setup_pair(original, change)
        pair = settings_exit['setups'].get(original.setup_id, settings_exit['default'])
        threshold_rows.append(dict(setup_id=original.setup_id, signal_time=original.signal_end,
            f_price_pct=core.price_change_pct, g_price_pct=expanded.price_change_pct,
            f_oi_pct=core.oi_change_pct, g_oi_pct=expanded.oi_change_pct,
            f_body=core.body_ratio, g_body=expanded.body_ratio, f_wick=core.max_wick_ratio,
            g_wick=expanded.max_wick_ratio, volume_5m_minimum=expanded.volume_ratio,
            f_quota=core.max_entries, g_quota=expanded.max_entries, **pair))
    thresholds = pd.DataFrame(threshold_rows)
    thresholds.to_csv(output / 'setup_thresholds_and_exits.csv', index=False)
    # The alternative Rs1 lakh total-position-value reading changes amounts, not fills.
    one_trades = f.v9.v5.apply_fixed_capital_model(final[0], 100_000., 1.)
    one_base = replace(source['v9_config'], leverage_factor=1.)
    one_ledger, one_summary = f.v9.v6.apply_portfolio_constraints(one_trades, one_base.portfolio_config())
    assert one_ledger.portfolio_executed.equals(ledger.portfolio_executed)
    np.testing.assert_allclose(one_ledger.portfolio_net_profit_rupees * 5, ledger.portfolio_net_profit_rupees, atol=1e-7, rtol=0)
    r.save(output / 'diagnostics/one_lakh_position_value_1x', one_trades, one_ledger, one_summary)
    larger, _ = f.v9.v6.apply_portfolio_constraints(final[0], replace(source['v9_config'], portfolio_capital_rupees=2_000_000.).portfolio_config())
    capacity_invariant = larger.portfolio_executed.equals(ledger.portfolio_executed)
    decision = dict(candidate=chosen, status=status, settings=asdict(change), tested_configurations=len(book),
        unique_selection_sets=int(opportunity.selection_sha256.nunique()), objective_passes=int(assessment.objective_pass.sum()),
        quality_passes=int(assessment.quality_pass.sum()), core_selected_retained=len(keys(orders[chosen]) & keys(expected_f)),
        f_executions_retained=len(keys(executed(ledger)) & core_keys), f_executions_removed=len(core_keys - keys(executed(ledger))),
        additional_executions=len(keys(executed(ledger)) - core_keys),
        peak_concurrent_positions=final[2]['peak_concurrent_positions'],
        peak_reserved_capital_rupees=final[2]['peak_reserved_capital_rupees'],
        doubling_portfolio_changes_no_executions=capacity_invariant,
        no_trade_days=int(daily_g.executed.eq(0).sum()), days_at_least_three_trades=int(daily_g.executed.ge(3).sum()),
        median_daily_trades=float(daily_g.executed.median()), maximum_daily_trades=int(daily_g.executed.max()),
        evidence=g.EVIDENCE)
    r.dump(output / 'decision.json', decision)
    mcols = ['version', 'period', 'sessions', 'selected_orders', 'trades', 'wins', 'losses', 'win_rate_pct',
             'profit_factor', 'net_profit_rupees', 'daily_close_drawdown_rupees']
    displayed = comparison.loc[comparison.cost_bps.eq(5) & comparison.version.isin(['V10-F', 'V10-G']), mcols]
    daily_cols = ['day', 'selected_F', 'executed_F', 'wins_F', 'losses_F', 'net_profit_rupees_F',
                  'selected_G', 'executed_G', 'wins_G', 'losses_G', 'win_rate_pct_G', 'profit_factor_G',
                  'net_profit_rupees_G', 'drawdown_rupees_G']
    finalists = assessment.loc[assessment.quality_pass].sort_values('trades', ascending=False)
    near_frequency = assessment.loc[assessment.frequency_pass].sort_values(['profit_factor', 'win_rate_pct'], ascending=False).head(10)
    report = '\n\n'.join([
        '# V13-v10-G: new careful threshold study',
        f'**{status}**. Chosen registered candidate: **{chosen}**. This run replaces the deleted, rejected old G; it starts from F with corrected capital.',
        '## What changed', json.dumps(asdict(change), indent=2),
        'Multipliers are relative to F. F-qualified top choices are retained at each setup timestamp; additional candidates fill spare quota using the same native ranking. No rule consults future fills, P&L, symbol-specific outcomes or calendar dates.',
        'Both volume filters are unchanged: native setup-specific 5m minimum and confirmation 1m volume ratio >=1.20 before ranking. All 14 setup times, exact next-minute directional confirmation, upstream 5m EMA checks, 10-minute trigger expiry, F/B stop/target table, full exits, no breakeven and 15:15 square-off remain fixed.',
        '## Comparable results',
        'All results below use Rs10 lakh portfolio capacity, Rs1 lakh allocated per trade, inherited 5x exposure (Rs5 lakh position value), and flat 5bps modeled round-trip costs. If Rs1 lakh means total position value, use the separately saved 1x replay: rupee P&L and drawdown divide by five, while selections, executions, win rate and PF are unchanged.',
        displayed.to_markdown(index=False, floatfmt='.2f'),
        f"Frequency uses all {len(days)} available sessions including zero-trade days. G has {decision['days_at_least_three_trades']} days with at least 3 trades, {decision['no_trade_days']} zero-trade days, median {decision['median_daily_trades']:.0f}, maximum {decision['maximum_daily_trades']}. This is an average-frequency objective, not a daily trade quota.",
        f"F selections retained: {decision['core_selected_retained']}/{len(expected_f)}; F executions retained: {decision['f_executions_retained']}/{len(core_keys)}; additional executions: {decision['additional_executions']}. Peak positions {decision['peak_concurrent_positions']}, peak allocated capital Rs{decision['peak_reserved_capital_rupees']:,.0f}; doubling capital leaves executions unchanged: {capacity_invariant}.",
        '## Additional trades and cost stress',
        comparison.loc[comparison.version.eq('G_ADDITIONAL') & comparison.cost_bps.eq(5), mcols].to_markdown(index=False, floatfmt='.2f'),
        comparison.loc[comparison.period.eq('FULL') & comparison.version.isin(['V10-F', 'V10-G']), mcols+['cost_bps']].to_markdown(index=False, floatfmt='.2f'),
        '## Daily comparison', daily_compare[daily_cols].to_markdown(index=False, floatfmt='.2f'),
        '## Exact setup thresholds and unchanged exits', thresholds.to_markdown(index=False, floatfmt='.3f'),
        '## Registered selection and alternatives',
        f"Registered {len(book)} configurations before this run's outcomes; {decision['unique_selection_sets']} distinct selection sets, {decision['objective_passes']} full objective passes. Quality requires win >=62%, PF >=3.30, daily-close DD <=1.2x F, net >=F and profitable added trades. Frequency band is 2.8–3.2 trades/session. Choice prioritizes fewest changed families, proximity to three, then PF. No adaptive second sweep.",
        finalists[['candidate','trades','win_rate_pct','profit_factor','net_profit_rupees','daily_close_drawdown_rupees','failed_gates']].to_markdown(index=False, floatfmt='.2f'),
        'Closest-frequency alternatives (not automatically accepted):',
        near_frequency[['candidate','trades','win_rate_pct','profit_factor','net_profit_rupees','daily_close_drawdown_rupees','failed_gates']].to_markdown(index=False, floatfmt='.2f'),
        'Native reranking was replayed once at the chosen parameters as a diagnostic and was not eligible for selection.',
        '## Evidence and reproducibility',
        'This is exploratory historical fitting. July, August and September have all been repeatedly reviewed, and the inherited exits were fitted on this history. Month tables and cost stress do not constitute an untouched test. No future win rate or PF is established.',
        'The source contains only 31 available sessions (July 29–31, 19 August sessions, 9 September sessions through September 11), not three complete months. Daily-close drawdown is realized P&L, not intraday mark-to-market. Replay inherits stop-first ambiguous OHLC handling, adverse stop gaps, and same-timestamp release of portfolio capital. Stops are configured distances; realized gap losses can exceed them.',
        'All frozen dataset artifacts and raw sources were hashed. Two explicitly pinned metadata changes are recorded in source_verification.json: refreshed contract registry and common calendar code. Frozen signals and 1m paths were not rebuilt. Corrected F parity is required before the new candidate loop continues.',
        '`python -B fno_v13_v10_g_research.py` reproduces the fixed study. `python -B fno_v13_v10_g_backtest.py` replays the chosen frozen configuration.',
    ])
    (output / 'V13_V10_G_DETAILED_RESULTS.md').write_text(report, encoding='utf-8')
    finish_manifest(output)
    print(json.dumps(decision, indent=2), flush=True)
    print(displayed.to_string(index=False), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=g.THRESHOLD_OUTPUT)
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    run(args.output_dir, plan_only=args.plan_only)
