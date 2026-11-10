"""Replay and publish the user-adopted 80%-target breakeven V23 baseline."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106 import run as original
from g3_v23_adx_rsi_20261106.keep_entries_20261106.publish import verify_manifest
from g3_v23_adx_rsi_20261106.through_20261105.run import assert_entries
from g3_v23_adx_rsi_20261106.trailing_20261106 import data
from g3_v23_adx_rsi_20261106.target_trailing_20261106 import engine, run as study

PACKAGE = Path(__file__).resolve().parent
SOURCE = PACKAGE.parent / 'target_trailing_20261106/results_through_20261105'
OUTPUT = PACKAGE / 'results_through_20261105'
BASELINE = 'breakeven_eighty_target'


def same_metric(value, expected):
    # Undefined ratios are None before CSV serialization and NaN after reading.
    if value is None or pd.isna(value):
        return bool(pd.isna(expected))
    return bool(np.isclose(value, expected, atol=1e-5, rtol=0))


def run(output=OUTPUT):
    output = Path(output).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh result folder inside breakeven_baseline_20261106')
    verify_manifest(SOURCE)
    provenance = json.loads((SOURCE / 'provenance.json').read_text())
    for path, expected in provenance['code_sha256'].items():
        if original.sha(path) != expected:
            raise ValueError('Source implementation changed: ' + path)
    old_config = json.loads((SOURCE / 'config.json').read_text())
    settings = engine.CANDIDATES[BASELINE]
    if settings != old_config['policy_settings'][BASELINE]:
        raise ValueError('Adopted baseline differs from the saved candidate')
    inputs = data.load()
    prefix = {sid: engine.build_features(path) for sid, path in inputs['paths'].items()}
    trades, changes, _, updates = study.replay_book(inputs, prefix, BASELINE, settings)
    saved = pd.read_csv(SOURCE / (BASELINE + '_trades.csv'))
    assert_entries(saved, trades)
    left, right = saved.set_index('sid').sort_index(), trades.set_index('sid').sort_index()
    for field in ('exit_ts', 'exit_reason'):
        if not left[field].equals(right[field]):
            raise ValueError('Adopted exit replay differs: ' + field)
    for field in ('exit_price', 'portfolio_net_profit_rupees'):
        if not np.allclose(left[field], right[field], atol=1e-7, rtol=0):
            raise ValueError('Adopted result replay differs: ' + field)

    comparison = pd.read_csv(SOURCE / 'overall.csv')
    daily_source = pd.read_csv(SOURCE / 'daily.csv')
    overall = comparison.loc[comparison.policy.eq(BASELINE)].copy()
    daily = daily_source.loc[daily_source.policy.eq(BASELINE)].sort_values('day').copy()
    days = daily.day.tolist()
    if len(days) != 66 or len(trades) != 790 or days != old_config['historydays']:
        raise ValueError('Adoption changed the historical inventory')
    for row in overall.itertuples(index=False):
        metrics = study.metric(trades, int(row.selected_orders), old_config['periods'][row.period])
        for field, value in metrics.items():
            expected = getattr(row, field)
            if not same_metric(value, expected):
                raise ValueError('Adopted aggregate differs: ' + row.period + '/' + field)
    for row in daily.itertuples(index=False):
        metrics = study.metric(trades, int(row.selected_orders), [row.day])
        for field, value in metrics.items():
            if not same_metric(value, getattr(row, field)):
                raise ValueError('Adopted daily result differs: ' + row.day + '/' + field)
    full = overall.loc[overall.period.eq('full_66')].iloc[0]
    previous = comparison.loc[comparison.period.eq('full_66') & comparison.policy.eq('fixed_1p50')].iloc[0]
    config = dict(version='G3-V23-BASELINE-TARGET80-BE-20261106', scope='BACKTEST_ONLY',
        execution_authority=False, live_changed=False, baseline_policy=BASELINE,
        selected_policy=BASELINE, selected_exit_policy=BASELINE, selected_sizing_policy='baseline_fixed_size',
        user_authorized_baseline_change=True, adoption_reason='User requested keeping the 80%-target breakeven rule as the V23 baseline.',
        selection_outcome='USER_ADOPTED_TARGET80_BREAKEVEN', automatic_research_qualification=False,
        prior_research_rejection_reasons=old_config['rejection_reasons'], previous_baseline_policy='fixed_1p50',
        baseline_initial_stop_pct=1.50, baseline_timed_tightening=False, baseline_native_target=True,
        baseline_trigger_fraction_of_target=.80, baseline_stop_after_trigger_return_pct=.05,
        baseline_settings=settings, entry_veto_enabled=False, selected_orders=891, filled_trades=790,
        sessions=66, historydays=days, periods=old_config['periods'], history_through=days[-1],
        notional_per_trade_rs=500000, cost_per_trade_rs=250, total_cost_rs=197500,
        unlimited_concurrent_capital=True, retrospective_only=True,
        activation='Completed postentry one-minute close reaches80% of the original target distance; entry bar excluded; stop effective next bar.',
        long_stop_after_trigger='actual entry * 1.0005', short_stop_after_trigger='actual entry * 0.9995',
        native_target_retained=True, time_exit='15:15 Asia/Kolkata',
        coverage_notes=old_config['coverage_notes'], source_results=str(SOURCE.relative_to(PACKAGE.parent)),
        trade_exit_replay_parity='PASS_ALL_790', daily_reconciliation='PASS_ALL_66')

    output.mkdir(parents=True)
    trades.to_csv(output / 'baseline_trades.csv', index=False)
    trades.to_csv(output / 'selected_trades.csv', index=False)
    overall.to_csv(output / 'overall.csv', index=False)
    daily.to_csv(output / 'daily.csv', index=False)
    daily.to_csv(output / 'day_wise_results.csv', index=False)
    comparison.loc[comparison.policy.isin(['fixed_1p50', BASELINE])].to_csv(output / 'comparison.csv', index=False)
    changes.to_csv(output / 'changes_vs_previous_fixed_stop.csv', index=False)
    updates.to_csv(output / 'stop_updates.csv', index=False)
    stress = pd.read_csv(SOURCE / 'cost_stress.csv')
    stress.loc[stress.policy.eq(BASELINE)].to_csv(output / 'cost_stress.csv', index=False)
    (output / 'config.json').write_text(json.dumps(config, indent=2) + '\n', encoding='utf-8')
    day_lines = ['# V23 adopted baseline - day-wise results', '',
        '**Current baseline: 80%-target breakeven, with initial1.50% SL and original targets.** '
        'Adopted explicitly by the user; previous research qualification failure remains recorded.', '',
        f'{len(days)} sessions from {days[0]} through {days[-1]}; all790 entries retained. '
        'Net amounts include Rs250 modeled costs per Rs500,000 exposure. Flat trades are not wins.', '',
        '| Day | Trades | Wins | Losses | Flat | Win rate | Net P&L (Rs) | Loss amount (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|--:|']
    for row in daily.itertuples(index=False):
        day_lines.append(f'| {row.day} | {int(row.trades)} | {int(row.wins)} | {int(row.losses)} | '
            f'{int(row.breakeven_trades)} | {row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | {row.gross_loss_rs:,.2f} |')
    day_lines += [f'| **Total** | **{int(full.trades)}** | **{int(full.wins)}** | **{int(full.losses)}** | '
        f'**{int(full.breakeven_trades)}** | **{full.win_rate_pct:.2f}%** | **{full.net_pnl_rs:,.2f}** | **{full.gross_loss_rs:,.2f}** |', '',
        'Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners. '
        'Results are retrospective. Slippage can turn exact modeled breakeven exits into losses.', '']
    (output / 'DAY_WISE_RESULTS.md').write_text('\n'.join(day_lines), encoding='utf-8')
    report = ['# V23 current baseline: 80%-target breakeven', '',
        '**The user explicitly adopted this rule as the V23 backtesting baseline.** '
        'This supersedes the previous fixed-stop default; it does not rewrite the archived research qualification decision.', '',
        '- Initial stop:1.50% from actual entry. Original target retained; no two-hour tightening.',
        '- Trigger: completed postentry one-minute close reaches80% of the original target distance.',
        '- From the next bar, tighten to+0.05% signed return from entry: LONG entry x1.0005; SHORT entry x0.9995.',
        '- Entry-bar triggers excluded. Stop only tightens. Conservative stop-first ambiguous bars and adverse gap-open fills retained. Exit by15:15.',
        '- Example: LONG entry100,target100.80; qualifying close100.64 activates stop100.05 from the following bar.', '',
        '| Policy | Trades | Wins | Losses | Flat | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|--:|--:|']
    for label, row in [('Previous fixed1.50% stop', previous), ('Current adopted80%-target breakeven', full)]:
        report.append(f'| {label} | {int(row.trades)} | {int(row.wins)} | {int(row.losses)} | {int(row.breakeven_trades)} | '
            f'{row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | {row.gross_loss_rs:,.2f} | {row.daily_close_drawdown_rs:,.2f} |')
    report += ['', f'Net profit is lower by Rs{previous.net_pnl_rs-full.net_pnl_rs:,.2f}; loss amount is lower by '
        f'Rs{previous.gross_loss_rs-full.gross_loss_rs:,.2f}; daily-close drawdown is lower by '
        f'Rs{previous.daily_close_drawdown_rs-full.daily_close_drawdown_rs:,.2f}. This trade-off is adopted by user instruction.', '',
        'All790 saved candidate exits and all66 daily aggregates were independently replayed and reconciled before publication. '
        '891 selected orders, fixedRs500,000 exposure per filled trade, Rs250 modeled round-trip costs, unlimited concurrent capital. '
        'The73 configured slot/direction pairs through14:00, their existing caps and all entry filters remain. ADX/RSI entry vetoes remain disabled.', '',
        'All history through5November2026 was previously observed. These are retrospective results. Daily-close drawdown is not intraday drawdown. '
        'The29 flat trades become losing trades under one extra basis point of adverse exit slippage; exact cost-covered exits are not guaranteed.', '',
        '[All66 day-wise results](DAY_WISE_RESULTS.md) | [Daily CSV](day_wise_results.csv) | '
        '[Current config](config.json) | [All current trades](selected_trades.csv) | [Slippage sensitivity](cost_stress.csv)', '',
        'Earlier results are preserved. LIVE execution, schedules and position management are unchanged.', '']
    (output / 'REPORT.md').write_text('\n'.join(report), encoding='utf-8')
    provenance['source_manifests'][str(SOURCE)] = original.sha(SOURCE / 'manifest.json')
    provenance['code_sha256'][str(Path(__file__).resolve())] = original.sha(__file__)
    (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n', encoding='utf-8')
    (output / 'validation.json').write_text(json.dumps(dict(replayed_exits=790, reconciled_days=66,
        selected_policy=BASELINE, user_adoption=True, live_changed=False), indent=2) + '\n')
    (output / 'manifest.json').write_text(json.dumps({p.name: original.sha(p) for p in output.iterdir() if p.is_file()}, indent=2) + '\n')
    print(f'PASS: adopted {BASELINE};790 identical exits,66 daily results; net Rs{full.net_pnl_rs:,.2f}.', flush=True)
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    run(parser.parse_args().output)
