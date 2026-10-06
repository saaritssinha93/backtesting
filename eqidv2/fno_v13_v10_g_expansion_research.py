"""Four fixed opportunity-expansion cases for G; no parameter sweep."""
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g
from fno_v13_v10_d_research import detailed_daily

f, r = g.f, g.r
CASES = {
    'G_CONTROL': dict(morning_slots=False, two_bar_continuation=False),
    'MORNING_ONLY': dict(morning_slots=True, two_bar_continuation=False),
    'TWO_BAR_ONLY': dict(morning_slots=False, two_bar_continuation=True),
    'BOTH': dict(morning_slots=True, two_bar_continuation=True),
}


def keys(frame):
    return set(zip(frame.sid.astype(int), frame.setup_id.astype(str)))


def passed_quality(candidate, baseline, extra, retained):
    reasons = []
    if candidate['win_rate_pct'] < 63.: reasons.append('WIN_RATE_BELOW_63')
    if candidate['profit_factor'] < 3.30: reasons.append('PF_BELOW_3.30')
    if candidate['daily_close_drawdown_rupees'] > 1.20 * baseline['daily_close_drawdown_rupees'] + 1e-7:
        reasons.append('DRAWDOWN_OVER_1.20X')
    if candidate['net_profit_rupees'] < baseline['net_profit_rupees'] - 1e-7:
        reasons.append('NET_BELOW_G')
    if extra['net_profit_rupees'] <= 0: reasons.append('ADDITIONAL_TRADES_NOT_PROFITABLE')
    if retained != baseline['trades']: reasons.append('EXISTING_G_EXECUTIONS_DISPLACED')
    return reasons


def choose(assessment):
    passing = assessment.loc[assessment.quality_pass & assessment.frequency_pass]
    if not passing.empty:
        winner = passing.sort_values(['changes', 'profit_factor', 'trades', 'case'],
                                      ascending=[True, False, False, True], kind='stable').iloc[0]
        return winner['case'], 'HISTORICAL_FREQUENCY_AND_QUALITY_OBJECTIVE_MET'
    partial = assessment.loc[assessment.quality_pass & assessment.trades.gt(
        assessment.loc[assessment.case.eq('G_CONTROL'), 'trades'].iloc[0])]
    if not partial.empty:
        winner = partial.sort_values(['trades', 'changes', 'profit_factor', 'case'],
                                      ascending=[False, True, False, True], kind='stable').iloc[0]
        return winner['case'], 'QUALITY_PRESERVED_FREQUENCY_TARGET_NOT_MET'
    return 'G_CONTROL', 'EXTENSIONS_REJECTED_EXISTING_G_RETAINED'


def protocol():
    return dict(cases=CASES, original_g_config=str(g.THRESHOLD_OUTPUT / 'frozen_config.json'),
        donor_map=g.MORNING_DONORS,
        morning_exits={'0951_LONG': {'stop_pct': .60, 'target_pct': .90},
                       '0956_SHORT': {'stop_pct': .60, 'target_pct': .93},
                       '1001_SHORT': {'stop_pct': .60, 'target_pct': .93}},
        morning_gates='Exact unrelaxed F donor thresholds, quota one, both volume filters; no G price multiplier on new slots',
        two_bar='Exact same-session/symbol/contract t,t-5,t-10 closes; net directional two-bar change meets setup price minimum; latest close-to-close move and candle body both directional; alternate price eligibility only; original prices retained for ranking',
        preserve='All previous G orders selected first. In BOTH, native single-bar morning choices also precede two-bar additions. No extra quota or delayed confirmation.',
        quality=dict(minimum_win_rate_pct=63., minimum_pf=3.30, maximum_daily_close_dd_vs_G=1.20,
                     minimum_net_vs_G=1., added_trades_positive_net=True, retain_all_G_executions=True),
        target='At least 2.4 executed trades per available session, including zero-trade sessions. 2.4-2.6 is an initial objective, not an upper cap.',
        choice='Full passes: fewest enabled changes, highest PF, most trades, case name. If no full pass: quality-passing increase with most trades, fewest changes, PF, name. Otherwise retain previous G.',
        no_adaptive_stage='Four cases only; individual-slot outcome contributions are diagnostic, not selectable variants. No stop/target or threshold sweep.',
        fixed_sizing=dict(portfolio_capacity=1_000_000, capital_per_trade=100_000, leverage=5, cost_bps=5),
        evidence=g.EVIDENCE)


def register(output):
    output.mkdir(parents=True, exist_ok=True)
    path = output / 'experiment_plan.json'
    value = protocol()
    # JSON canonicalization also normalizes tuple donor definitions to lists.
    value = json.loads(json.dumps(value))
    if path.exists():
        if json.loads(path.read_text())['protocol'] != value:
            raise ValueError('Do not change the registered expansion protocol')
    else:
        r.dump(path, dict(registered_utc=datetime.now(timezone.utc).isoformat(), protocol=value))


def finish_manifest(output):
    files = [Path(__file__), Path(g.__file__), Path('fno_v13_v10_g_two_bar.py'), Path(f.__file__),
             Path(f.v9.__file__), Path(f.v9.v5.__file__), Path(f.v9.v6.__file__),
             Path(f.v9.v5.replay.__file__), Path(f.b.__file__), Path(f.v10.__file__), Path(r.__file__),
             Path('fno_v13_v9_data.py'), Path('fno_v13_v5_derivative_data.py'), Path('fno_oi_common.py'),
             Path('fno_v13_v10_d_research.py'), Path('tests/test_fno_v13_v10_g.py'),
             Path('tests/test_fno_v13_v10_g_morning.py'), Path('tests/test_fno_v13_v10_g_two_bar.py')]
    files.append(Path('tests/test_fno_v13_v10_g_expansion.py'))
    snapshot = output / 'source_snapshot'
    snapshot.mkdir(exist_ok=True)
    for path in files:
        if path.is_file(): shutil.copy2(path, snapshot / path.name)
    r.dump(output / 'research_manifest.json', dict(complete=True,
        code_sha256={str(path.resolve()): f.v10.sha(path) for path in files if path.is_file()},
        artifacts={str(path.relative_to(output)): f.v10.sha(path) for path in sorted(output.rglob('*'))
                   if path.is_file() and path.name != 'research_manifest.json'}))


def run(output=g.EXPANSION_OUTPUT, *, plan_only=False):
    register(output)
    if plan_only:
        print(f'Registered four cases: {output / "experiment_plan.json"}')
        return
    import fno_v13_v10_g_two_bar as two_bar
    old_manifest = json.loads((g.THRESHOLD_OUTPUT / 'research_manifest.json').read_text())
    artifacts = {name.replace('\\', '/'): checksum for name, checksum in old_manifest['artifacts'].items()}
    required = ['frozen_config.json', 'final/V13_V10_G/portfolio_trades.csv']
    for name in required:
        if f.v10.sha(g.THRESHOLD_OUTPUT / name) != artifacts[name]:
            raise ValueError(f'Original G artifact drift: {name}')
    old_settings = json.loads((g.THRESHOLD_OUTPUT / 'frozen_config.json').read_text())
    g.checked_settings(old_settings)
    change = g.SelectionChange(**old_settings['selection_change'])
    source = g.load_source(two_bar_continuation=True)
    source['signals'].to_parquet(output / 'augmented_signals.parquet', index=False)
    days = source['days']
    groups = {'FULL': days, **{name: [day for day in days if day.startswith(prefix)]
              for name, prefix in [('JULY', '2026-07'), ('AUGUST', '2026-08'), ('SEPTEMBER', '2026-09')]}}
    settings = {name: g.config(old_settings['exit'], change, **flags) for name, flags in CASES.items()}
    audits = {name: g.selection_audit(source['signals'], source['v9_config'], change, **flags)
              for name, flags in CASES.items()}
    orders = {name: audit.loc[audit.v9_selected].reset_index(drop=True) for name, audit in audits.items()}
    core_selected = keys(orders['G_CONTROL'])
    assert all(core_selected.issubset(keys(frame)) for frame in orders.values())
    opportunities = []
    for name in CASES:
        r.dump(output / 'cases' / name / 'frozen_config.json', settings[name])
        audits[name].to_csv(output / 'cases' / name / 'selection_audit.csv', index=False)
        opportunities.append(dict(case=name, selected=len(orders[name]), eligible=len(audits[name]),
                                  additional_selected=len(keys(orders[name]) - core_selected)))
    pd.DataFrame(opportunities).to_csv(output / 'outcome_blind_opportunities.csv', index=False)
    print('Decisions fixed before forward-path loading: ' + json.dumps(opportunities), flush=True)
    union = pd.concat(list(orders.values()), ignore_index=True).drop_duplicates(['sid', 'setup_id'])
    paths = two_bar.load_paths(source, union)
    np.savez_compressed(output / 'selected_paths.npz',
                        **{f'{sid}_{field}': values for sid, path in paths.items() for field, values in path.items()})
    r.dump(output / 'source_verification.json', dict(
        original_g_artifacts={name: artifacts[name] for name in required},
        source=source['source_verification'],
        augmentation=source.get('two_bar_reconstruction_proof', source.get('two_bar_proof', {})),
        paths=source.get('two_bar_path_proof', {})))
    results, rows, assessments, contributions = {}, [], [], []
    for name, flags in CASES.items():
        result = g.evaluate({**source, 'orders': orders[name], 'paths': paths}, settings[name])
        results[name] = result
        ledger = result[1]
        r.save(output / 'cases' / name, *result)
        daily = detailed_daily(ledger, days)
        daily.to_csv(output / 'cases' / name / 'daily_detailed.csv', index=False)
        full = r.metric(ledger, days)
        if name == 'G_CONTROL':
            prior = pd.read_csv(g.THRESHOLD_OUTPUT / required[1], float_precision='round_trip')
            proof = f.v9.assert_control_parity(prior, ledger)
            a = prior.set_index(['sid', 'setup_id']).portfolio_net_profit_rupees.sort_index()
            b = ledger.set_index(['sid', 'setup_id']).portfolio_net_profit_rupees.sort_index()
            np.testing.assert_allclose(a, b, atol=1e-7, rtol=0)
            proof['portfolio_pnl_parity'] = True
            r.dump(output / 'previous_g_parity.json', proof)
            baseline = full
            core_executed = keys(ledger.loc[ledger.portfolio_executed])
        extra = ledger.loc[[(int(row.sid), str(row.setup_id)) not in core_selected for row in ledger.itertuples()]]
        extra.to_csv(output / 'cases' / name / 'additional_entries.csv', index=False)
        retained = len(keys(ledger.loc[ledger.portfolio_executed]) & core_executed)
        extra_full = r.metric(extra, days)
        failures = [] if name == 'G_CONTROL' else passed_quality(full, baseline, extra_full, retained)
        assessments.append(dict(case=name, **full, trades_per_day=full['trades'] / len(days),
            changes=sum(flags.values()), extra_selected=len(extra), extra_trades=extra_full['trades'],
            extra_win_rate_pct=extra_full['win_rate_pct'], extra_profit_factor=extra_full['profit_factor'],
            extra_net_profit_rupees=extra_full['net_profit_rupees'], retained_g_executions=retained,
            quality_pass=not failures, frequency_pass=full['trades'] / len(days) >= 2.4,
            failed_gates='|'.join(failures)))
        for period, period_days in groups.items():
            for cost in (5, 9):
                rows.append(dict(case=name, period=period, sessions=len(period_days),
                                 **r.metric(ledger, period_days, cost_bps=cost)))
        for setup_id, frame in extra.groupby('setup_id'):
            contributions.append(dict(case=name, setup_id=setup_id, **r.metric(frame, days)))
        print(f'{name}: {full["trades"]} trades, win {full["win_rate_pct"]:.2f}%, PF {full["profit_factor"]:.4f}; {failures}', flush=True)
    assessment = pd.DataFrame(assessments)
    assessment.to_csv(output / 'case_assessment.csv', index=False)
    comparison = pd.DataFrame(rows)
    comparison.to_csv(output / 'comparison_metrics.csv', index=False)
    contribution = pd.DataFrame(contributions)
    contribution.to_csv(output / 'additional_trade_contributions.csv', index=False)
    chosen, status = choose(assessment)
    final = results[chosen]
    r.dump(output / 'frozen_config.json', settings[chosen])
    r.save(output / 'final/V13_V10_G', *final)
    daily_final = detailed_daily(final[1], days)
    daily_final.to_csv(output / 'daily_detailed.csv', index=False)
    daily_comparison = detailed_daily(results['G_CONTROL'][1], days).merge(
        daily_final, on='day', suffixes=('_before', '_after'), validate='one_to_one')
    daily_comparison.to_csv(output / 'daily_comparison.csv', index=False)
    daily_all_cases = pd.DataFrame({'day': days})
    detailed_cases = []
    for name, result in results.items():
        daily_case = detailed_daily(result[1], days)
        detailed_cases.append(daily_case.assign(case=name))
        compact = daily_case[['day', 'executed', 'net_profit_rupees']].rename(
            columns={'executed': f'{name}_trades', 'net_profit_rupees': f'{name}_net_rupees'})
        daily_all_cases = daily_all_cases.merge(compact, on='day', validate='one_to_one')
    pd.concat(detailed_cases, ignore_index=True).to_csv(output / 'daily_case_detail.csv', index=False)
    daily_all_cases.to_csv(output / 'daily_all_cases.csv', index=False)
    larger, _ = f.v9.v6.apply_portfolio_constraints(final[0], replace(source['v9_config'], portfolio_capital_rupees=2_000_000.).portfolio_config())
    capacity_invariant = larger.portfolio_executed.equals(final[1].portfolio_executed)
    one_trades = f.v9.v5.apply_fixed_capital_model(final[0], 100_000., 1.)
    one_ledger, one_summary = f.v9.v6.apply_portfolio_constraints(one_trades, replace(source['v9_config'], leverage_factor=1.).portfolio_config())
    assert one_ledger.portfolio_executed.equals(final[1].portfolio_executed)
    np.testing.assert_allclose(one_ledger.portfolio_net_profit_rupees * 5, final[1].portfolio_net_profit_rupees, atol=1e-7, rtol=0)
    r.save(output / 'diagnostics/one_lakh_position_value_1x', one_trades, one_ledger, one_summary)
    decision = dict(case=chosen, status=status, rules=CASES[chosen], evidence=g.EVIDENCE,
        peak_positions=final[2]['peak_concurrent_positions'], peak_allocated_capital_rupees=final[2]['peak_reserved_capital_rupees'],
        doubling_portfolio_leaves_fills_unchanged=capacity_invariant,
        no_trade_days=int(daily_final.executed.eq(0).sum()), days_at_least_three=int(daily_final.executed.ge(3).sum()),
        median_daily_trades=float(daily_final.executed.median()), max_daily_trades=int(daily_final.executed.max()),
        selected_metrics=assessment.loc[assessment.case.eq(chosen)].iloc[0].to_dict())
    r.dump(output / 'decision.json', decision)
    metric_cols = ['case', 'period', 'sessions', 'selected_orders', 'trades', 'wins', 'losses', 'win_rate_pct',
                   'profit_factor', 'net_profit_rupees', 'daily_close_drawdown_rupees']
    assessment_cols = ['case', 'trades', 'trades_per_day', 'win_rate_pct', 'profit_factor', 'net_profit_rupees',
                       'daily_close_drawdown_rupees', 'extra_trades', 'extra_win_rate_pct', 'extra_profit_factor',
                       'extra_net_profit_rupees', 'failed_gates']
    daily_cols = ['day', 'selected_before', 'executed_before', 'wins_before', 'losses_before', 'net_profit_rupees_before',
                  'selected_after', 'executed_after', 'wins_after', 'losses_after', 'win_rate_pct_after', 'profit_factor_after',
                  'net_profit_rupees_after', 'drawdown_rupees_after']
    report = '\n\n'.join([
        '# V13-v10-G: morning slots and two-candle continuation',
        f'**{status}**. The active frozen G configuration is **{chosen}**. All four cases were registered before their replay results; no individual clock or stock was chosen by outcome.',
        '## Fixed changes tested',
        'MORNING_ONLY adds 09:50 LONG (confirmation 09:51) using 09:55 LONG F gates/exits, and 09:55/10:00 SHORT (confirmations 09:56/10:01) using 09:50 SHORT F gates/exits. Price minimum stays 0.20% for new slots, OI minimum 0.10% LONG / 0.05% SHORT, confirmation body >=0.40, adverse wick <=0.60, 5m volume ratio >=1, confirmation 1m volume ratio >=1.20, maximum one order per slot/day, ranked by liquidity. New LONG exit is 0.60% SL / 0.90% target; new SHORT exits are 0.60% SL / 0.93% target.',
        'TWO_BAR_ONLY fills vacant existing G setup slots when an exact two-completed-bar directional net move meets the existing price threshold. Both latest close-to-close movement and latest candle body must be directional. Exact t-10,t-5,t observations must be in the same session, symbol and contract. Original single-bar selections retain priority. Original price fields remain unchanged for ranking; all other signal, OI, EMA, volume and 1m confirmation guards remain in force. BOTH applies both predefined changes, with native single-bar morning candidates preceding two-bar candidates.',
        'All existing G exits and selections are preserved. Full positions exit at target, stop or 15:15; no partial exit or break-even rule. Entry starts after exact next-minute confirmation with the existing ten-minute expiry. Every case receives a fresh chronological portfolio allocation.',
        '## Full-history comparison', assessment[assessment_cols].to_markdown(index=False, floatfmt='.2f'),
        'Quality requires win >=63%, PF >=3.30, daily-close drawdown <=1.2x previous G, net >=previous G, profitable extra executions and retention of previous G executions. Target is >=2.4 trades/session. Full passes prioritize fewer enabled changes, then PF, then trades. Partial improvement is explicitly labeled if frequency remains below target. If no expansion passes quality, the previous G remains active.',
        '## Monthly comparison', comparison.loc[comparison.cost_bps.eq(5), metric_cols].to_markdown(index=False, floatfmt='.2f'),
        '## Added executions by setup (diagnostic, not independently selectable)',
        contribution[['case','setup_id','selected_orders','trades','wins','losses','win_rate_pct','profit_factor','net_profit_rupees']].to_markdown(index=False, floatfmt='.2f'),
        '## Daily comparison of all four cases', daily_all_cases.to_markdown(index=False, floatfmt='.2f'),
        'Full daywise winners, losers, win rate, PF, target/stop/time exits and cumulative drawdown are in daily_case_detail.csv and each cases/<case>/daily_detailed.csv. daily_comparison.csv separately compares the prior G with the selected active configuration.',
        f"Frequency uses all {len(days)} available sessions, including {decision['no_trade_days']} active-G zero-trade days; {decision['days_at_least_three']} days have at least three trades. Median {decision['median_daily_trades']}, maximum {decision['max_daily_trades']}. Peak allocated capital Rs{decision['peak_allocated_capital_rupees']:,.0f} across {decision['peak_positions']} concurrent positions. Doubling portfolio capacity changes no fills: {capacity_invariant}.",
        '## Costs and sizing',
        'Rupee amounts use Rs10 lakh portfolio capacity, Rs1 lakh allocated capital per trade and inherited 5x modeled exposure (Rs5 lakh position value), with flat 5bps round-trip costs. The separate 1x replay models Rs1 lakh total position value: identical fills, win rate and PF, with one-fifth rupee P&L and drawdown.',
        comparison.loc[comparison.period.eq('FULL'), metric_cols+['cost_bps']].to_markdown(index=False, floatfmt='.2f'),
        '## Evidence and limitations',
        'All 31 available sessions have been reviewed previously: three July sessions, nineteen August sessions and nine September sessions through September 11. The inherited exits were fitted on this history. This is exploratory analysis, not an untouched test or a forecast. Individual additional groups are small; apparent improvement must be checked on new sessions with frozen settings.',
        'Frozen source and dataset hashes are checked; the prior G control must reproduce selections, fills, exits and portfolio P&L exactly. The broader signal pool is reconstructed from frozen all-5m features, including sub-0.10% latest moves; existing IDs are preserved and new IDs are allocated beyond every annotated SID. New minute paths are materialized only after selections are frozen and validated for exact timing/coverage. Proofs and the selected path archive are saved alongside the report.',
        'Daily-close drawdown is realized P&L, not intraday mark-to-market. The existing simulator uses stop-first ordering for ambiguous bars and adverse gap stop fills; same-timestamp position exits release capacity under its inherited convention. No live dashboard or execution service is changed.',
        '`python -B fno_v13_v10_g_expansion_research.py` reproduces these four cases. `python -B fno_v13_v10_g_backtest.py --frozen-research` replays the active frozen G. The prior 74-case study remains reproducible with `python -B fno_v13_v10_g_research.py`.',
    ])
    (output / 'V13_V10_G_EXPANSION_RESULTS.md').write_text(report, encoding='utf-8')
    finish_manifest(output)
    print(json.dumps(decision, indent=2), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=g.EXPANSION_OUTPUT)
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    run(args.output_dir, plan_only=args.plan_only)
