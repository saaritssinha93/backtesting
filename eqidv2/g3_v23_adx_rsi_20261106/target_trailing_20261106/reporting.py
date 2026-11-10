"""Reconciled reports for native-target, early trailing-stop experiments."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.trailing_20261106.reporting import (
    BASELINE, _validate_changes, diagnostics, _number)
from g3_v23_adx_rsi_20261106.loss_control_20261106.reporting import (
    ADDITIVE, compare_days, summarize_days, _day_table, _metrics_table)


def write_reports(output, overall, daily, changes, config):
    """Render a frozen study; all selection decisions must come from the runner."""
    if config.get('baseline_policy') != BASELINE:
        raise ValueError('Report requires the user-adopted fixed1.50 baseline')
    selected = config['selected_policy']
    training = config['training_candidate'] or BASELINE
    diagnostic = config.get('comparison_candidate') or training
    promoted = config['promotion_pass']
    if not isinstance(promoted, (bool, np.bool_)):
        raise ValueError('Promotion status must be Boolean')
    if not promoted and selected != BASELINE:
        raise ValueError('Failed promotion cannot activate an alternative')
    if promoted and (selected == BASELINE or selected != training):
        raise ValueError('Promoted policy must be the fitting-period candidate')
    if not {BASELINE, selected, training, diagnostic}.issubset(set(daily.policy)):
        raise ValueError('Missing baseline, selected or comparison policy')
    if daily.duplicated(['policy', 'day']).any() or overall.duplicated(['policy', 'period']).any():
        raise ValueError('Duplicate policy/day or policy/period rows')
    baseline = daily.loc[daily.policy.eq(BASELINE)].sort_values('day')
    days = baseline.day.tolist()
    if not days or config.get('historydays', days) != days:
        raise ValueError('Daily history differs from the declared inventory')
    full = overall.loc[overall.period.eq(f'full_{len(days)}')].set_index('policy')
    if set(full.index) != set(daily.policy):
        raise ValueError('Full and daily policy inventories differ')
    for name, part in daily.groupby('policy'):
        compare_days(baseline, part)
        values = summarize_days(part)
        for field in (*ADDITIVE, 'win_rate_pct', 'daily_close_drawdown_rs'):
            if not np.isclose(values[field], full.loc[name, field], atol=1e-5, rtol=0):
                raise ValueError('Daily/full reconciliation failed: ' + name + '/' + field)
    if 'target_mode' not in changes or not changes.target_mode.eq('native').all():
        raise ValueError('Every arm must preserve the native profit target')
    if 'initial_risk_pct' not in changes or not np.allclose(changes.initial_risk_pct, 1.50, atol=0, rtol=0):
        raise ValueError('Every arm must start with fixed1.50% risk')
    effects = _validate_changes(changes, daily, full)
    for name, part in changes.groupby('policy'):
        for field, actual in [('average_trade_rs', part.candidate_pnl_rs.mean()),
                              ('median_trade_rs', part.candidate_pnl_rs.median())]:
            if field in full and not np.isclose(full.loc[name, field], actual, atol=1e-5, rtol=0):
                raise ValueError('Trade payoff reconciliation failed: ' + name + '/' + field)
    diag = diagnostics(changes)
    current = compare_days(baseline, daily.loc[daily.policy.eq(selected)])
    candidate = compare_days(baseline, daily.loc[daily.policy.eq(diagnostic)])
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    current.to_csv(output/'day_wise_results.csv', index=False)
    candidate.to_csv(output/'candidate_day_wise_results.csv', index=False)
    effects.to_csv(output/'trade_effect_summary.csv', index=False)
    diag.to_csv(output/'diagnostics.csv', index=False)
    current_note = (f'Baseline: **fixed 1.50% SL**. Research-selected policy: **{selected}**. '
        'Every arm preserves original targets, entry identities, times, prices and exposure. ')
    current_note += ('The trailing candidate passed the declared retrospective checks.' if promoted else
                     '**No early-trailing alternative qualified; the fixed 1.50% baseline remains selected.**')
    current_note += ' The current V23 default remains fixed1p50; this research does not change that pointer.'
    candidate_note = (f'Comparison policy: **{diagnostic}**, against fixed 1.50% SL with original targets. '
        + ('This is the selected research result.' if diagnostic == selected and promoted else
           '**Exploratory comparison only; not the research-selected policy.**'))
    (output/'DAY_WISE_RESULTS.md').write_text(_day_table(current,
        'V23 research-selected day-wise results - target-relative trailing study', current_note), encoding='utf-8')
    (output/'CANDIDATE_DAY_WISE_RESULTS.md').write_text(_day_table(candidate,
        'V23 early-trailing candidate day-wise comparison', candidate_note), encoding='utf-8')

    lines = [f'# V23 target-relative trailing study through {days[-1]}', '',
        f'All **{int(full.loc[BASELINE,"trades"])} entries across {len(days)} sessions** from '
        f'{days[0]} through {days[-1]} are retained. The comparison baseline is the adopted '
        f'**fixed 1.50% SL**, with net profit **Rs {full.loc[BASELINE,"net_pnl_rs"]:,.2f}** '
        'after modelled costs. Every alternative retains the original profit target and starts '
        'with the same 1.50% stop. Entry selection, fill prices, times and position sizes are unchanged.', '']
    if promoted:
        lines += [f'**Qualified retrospective candidate: {selected}.** It passed the declared '
            'first44 selection and next15, final7 and full-history checks. This is historical '
            'evidence, not a guarantee of future profit preservation.', '']
    else:
        lines += ['**No early-trailing alternative qualified for replacement.** Fixed 1.50% remains '
            'the research-selected V23 backtest policy. Alternative results are retained as research '
            'comparisons, including any improvements that did not survive every period check.', '']
    lines += ['The current V23 default remains **fixed1p50**, regardless of this research selection. '
        'No current-policy pointer or live execution setting is changed by this study.', '']
    if config.get('rejection_reasons'):
        lines += ['Recorded checks: ' + '; '.join(map(str, config['rejection_reasons'])), '']
    lines += ['## Full-history comparison', ''] + _metrics_table(list(full.iterrows()))
    lines += ['', 'Loss amount is the absolute sum of negative after-cost trade P&L before offsetting '
        'winners. Flat trades are neither wins nor losses; win rate uses all fills. Daily-close '
        'drawdown is not intraday drawdown.', '', '## Expectancy and payoff', '',
        '| Policy | Mean net/trade (Rs) | Median net/trade (Rs) | Profit factor | Mean winner (Rs) | Mean losing amount (Rs) |',
        '|:--|--:|--:|--:|--:|--:|']
    for name, row in full.iterrows():
        lines.append(f'| {name} | {_number(row,"average_trade_rs")} | {_number(row,"median_trade_rs")} | '
            f'{_number(row,"profit_factor")} | {_number(row,"average_win_rs")} | {_number(row,"average_loss_rs")} |')
    lines += ['', '## Why these triggers differ', '',
        'The previous R-based experiment waited for a move measured against the 1.50% initial stop. '
        'Many original targets are smaller than 1R. These new rules instead measure completed-close '
        'progress toward each trade\'s original target T, allowing protection before the target is hit.', '',
        'For example, with a 0.80% target, half-target progress is a favourable 0.40% close. The '
        'half-target cushion then moves the stop from -1.50% to -1.00% of entry. It limits remaining '
        'risk while leaving pullback room; it does not lock in a profit. A +0.05% stop covers the '
        'modelled round-trip costs at an exact fill, but gaps or extra slippage can still lose money.', '',
        'All six alternatives retain the native target throughout the trade. No target is removed, '
        'no initial stop is tightened before entry, and no partial position is sold.', '']
    for name, definition in config.get('definitions', {}).items():
        value = definition if isinstance(definition, str) else json.dumps(definition, sort_keys=True)
        lines.append(f'- **{name}:** {value}')
    lines += ['', 'The patient variants require at least five completed post-entry bars and two '
        'consecutive qualifying closes. The ATR hybrid trails the best completed post-entry close '
        'with a distance of max(0.35% of entry, 3 ATR), starting from a -0.75% stop floor. ATR uses '
        'the causal confirmation-path prefix, with verified confirmation ATR during warm-up. '
        'It is not a full-session or overnight ATR definition.', '',
        'Entry-bar extrema and entry-bar closes cannot activate a new guard. All newly observed '
        'stop updates apply from the next bar and can only tighten. Native targets remain active; '
        'adverse opening gaps fill at the open, ambiguous stop/target bars use stop-first handling, '
        'and every position still exits by 15:15.', '', '## Selection and period checks', '',
        f'Fitting-period candidate: **{training}**. Research-selected policy: **{selected}**. Diagnostic '
        f'comparison: **{diagnostic}**. Selection uses only the first44 sessions through 5 October, '
        'requiring net profit at least the fixed1.50 baseline, strictly lower loss amount and no '
        'worse daily-close drawdown. The single chosen candidate is then checked separately on '
        'the next15 sessions through 27 October, final7 through 5 November, and full66. A failed '
        'candidate is not replaced using later outcomes.', '',
        '**All 66 sessions were already observed in prior research.** These are retrospective '
        'stability checks, not an untouched forward holdout. Multiple experiments increase the '
        'risk of finding a historical coincidence; these results do not guarantee improvement.', '']
    wanted = list(dict.fromkeys([BASELINE, training, selected, diagnostic]))
    for period, label in [('fit_44','First44: selection'), ('validation_15','Next15: validation'),
                          ('later_7','Final7: later check'), ('nov05','5 November only')]:
        part = overall.loc[overall.period.eq(period) & overall.policy.isin(wanted)]
        if not part.empty:
            lines += ['### ' + label, ''] + _metrics_table(
                [(row.policy, row._asdict()) for row in part.itertuples(index=False)]) + ['']
    lines += ['## Effect on existing winners and losers', '',
        'These outcome labels are computed after replay for diagnosis; they never determine an '
        'entry or exit decision. Preserving aggregate profit does not preserve every original winner.', '',
        '| Policy | Baseline winners worsened | Baseline losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|']
    for row in effects.itertuples(index=False):
        lines.append(f'| {row.policy} | {row.baseline_winners_harmed} | {row.baseline_losers_improved} | '
            f'{row.winners_turned_losses} | {row.losers_turned_wins} | {row.winner_pnl_change_rs:+,.2f} | '
            f'{row.loser_pnl_change_rs:+,.2f} |')
    lines += ['', '## Activation and exit diagnostics', '',
        '| Policy | Trades with activation | Stop updates | Changed exits | Exit followed by later original-target touch |',
        '|:--|--:|--:|--:|--:|']
    for _, row in diag.iterrows():
        lines.append(f'| {row.policy} | {_number(row,"trades_with_activation_count",0)} | '
            f'{_number(row,"stop_update_count",0)} | {_number(row,"exit_changed",0)} | '
            f'{_number(row,"premature_exit_then_target",0)} |')
    lines += ['', 'A later original-target touch and full-session MFE use future prices only for '
        'post-replay diagnostics. Neither is a trading signal, and a later target touch does not '
        'prove that holding through the intervening drawdown was safely executable.', '',
        f'The diagnostic comparison improves net P&L on {int(candidate.pnl_change_rs.gt(1e-7).sum())} '
        f'days and worsens it on {int(candidate.pnl_change_rs.lt(-1e-7).sum())} days. All dates, '
        'including unchanged and zero-trade dates, remain in the day-wise reports.', '',
        '## Costs, source limits and scope', '',
        '- Fixed Rs500,000 exposure per entry, Rs100,000 capital at5x, unlimited concurrent capital '
        'and Rs250 modelled round-trip costs per fill. Extra adverse slippage of0,1,2 and5bps on '
        'changed exits is a sensitivity check, not a selection input.',
        '- The initial1.50% stop permits Rs7,500 nominal price loss plus Rs250 costs per trade. '
        'Adverse gaps can exceed this amount; a stop is not a guaranteed fill price.',
        '- All73 setup/direction pairs through14:00 remain enabled, with final confirmation '
        'at14:01. Original pairs retain cap3; new pairs retain cap1 and their existing volume gate.',
        '- Kite historical candles provide timestamp and OHLCV fields (with optional OI); '
        'the inference from this candle format is that it cannot establish exact intrabar ordering, '
        'bid/ask spreads or queue position. This motivates the next-bar and stop-first conventions. '
        '[Kite historical-data documentation](https://kite.trade/docs/connect/v3/historical/). '
        'No tick, volume-delta or volatility-index information is inferred from these paths; '
        'no new API data fetch was required for this replay.',
        '- LIVE execution, approvals, scheduling and broker order handling are unchanged.']
    lines += ['- ' + str(note) for note in config.get('coverage_notes', [])]
    lines += ['', '[Research-selected day-wise results](DAY_WISE_RESULTS.md) | [Research-selected CSV](day_wise_results.csv) | '
        '[Candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) | [All policies](overall.csv) | '
        '[All daily rows](daily.csv) | [Every trade comparison](trade_changes.csv) | '
        '[Extra-slippage sensitivity](cost_stress.csv) | [Stop updates](stop_updates.csv) | '
        '[Configuration](config.json) | [Research-selected trades](selected_trades.csv)', '']
    (output/'REPORT.md').write_text('\n'.join(lines), encoding='utf-8')
