"""Reconciled reports for stop distances fitted using entry-time information."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.loss_control_20261106.reporting import (
    ADDITIVE, BASELINE, compare_days, summarize_days, trade_effects,
    _day_table, _metrics_table)


def _validate_changes(changes, daily, full):
    if set(changes.policy) != set(daily.policy):
        raise ValueError('Trade-effect policies do not cover the daily policies')
    baseline = changes.loc[changes.policy.eq(BASELINE)].set_index('sid').sort_index()
    if baseline.empty or not baseline.index.is_unique:
        raise ValueError('Invalid baseline trade identities')
    for name, part in changes.groupby('policy'):
        other = part.set_index('sid').sort_index()
        if not other.index.is_unique or not other.index.equals(baseline.index):
            raise ValueError('Trade-effect identities changed: ' + name)
        if not other.day.equals(baseline.day):
            raise ValueError('Trade-effect day identity changed: ' + name)
        if not np.allclose(other.baseline_pnl_rs, baseline.baseline_pnl_rs, atol=1e-7, rtol=0):
            raise ValueError('Trade-effect baseline outcome changed: ' + name)
    effects = trade_effects(changes)
    for row in effects.itertuples(index=False):
        if row.compared_trades != int(full.loc[BASELINE, 'trades']):
            raise ValueError('Trade effects do not retain every baseline fill')
        expected = full.loc[row.policy, 'net_pnl_rs'] - full.loc[BASELINE, 'net_pnl_rs']
        if not np.isclose(row.total_pnl_change_rs, expected, atol=1e-5, rtol=0):
            raise ValueError('Trade-effect net difference does not reconcile')
    return effects


def write_reports(output, overall, daily, changes, config, fitparams):
    """Write reconciled entry-SL results without fitting or selecting policies.

    Required config: selected_policy, training_candidate and promotion_pass.
    Optional comparison_candidate names the diagnostic policy. Fitted values
    remain in the runner's fit.json; this renderer does not infer its schema.
    """
    if not isinstance(fitparams, dict):
        raise ValueError('Fitted parameter evidence must be a JSON-style mapping')
    output = Path(output)
    selected = config['selected_policy']
    training = config['training_candidate'] or BASELINE
    diagnostic = config.get('comparison_candidate') or training
    promoted = config['promotion_pass']
    if not isinstance(promoted, (bool, np.bool_)):
        raise ValueError('Promotion status must be Boolean')
    if not promoted and selected != BASELINE:
        raise ValueError('Failed promotion cannot select a new policy')
    if promoted and (selected == BASELINE or selected != training):
        raise ValueError('Promoted policy must be the fitted-period candidate')
    required = {BASELINE, selected, training, diagnostic}
    if not required.issubset(set(daily.policy)):
        raise ValueError('Missing baseline, selected or comparison policy')
    if daily.duplicated(['policy','day']).any() or overall.duplicated(['policy','period']).any():
        raise ValueError('Duplicate daily or period result')
    baseline = daily.loc[daily.policy.eq(BASELINE)].sort_values('day')
    days = baseline.day.tolist()
    if not days or config.get('historydays',days) != days:
        raise ValueError('Report history differs from declared history')
    full = overall.loc[overall.period.eq(f'full_{len(days)}')].set_index('policy')
    if set(full.index) != set(daily.policy):
        raise ValueError('Full-history policy inventory differs from daily results')
    for name, part in daily.groupby('policy'):
        compare_days(baseline,part)
        summary = summarize_days(part)
        for field in (*ADDITIVE,'win_rate_pct','daily_close_drawdown_rs'):
            if not np.isclose(summary[field],full.loc[name,field],atol=1e-5,rtol=0):
                raise ValueError('Daily/full reconciliation failed: '+name+'/'+field)
    effects = _validate_changes(changes,daily,full)
    current = compare_days(baseline,daily.loc[daily.policy.eq(selected)])
    candidate = compare_days(baseline,daily.loc[daily.policy.eq(diagnostic)])
    output.mkdir(parents=True,exist_ok=True)
    current.to_csv(output/'day_wise_results.csv',index=False)
    candidate.to_csv(output/'candidate_day_wise_results.csv',index=False)
    effects.to_csv(output/'trade_effect_summary.csv',index=False)
    current_note = f'Current selected stop policy: **{selected}**. All entry identities, prices, times and position sizes are retained.'
    if not promoted:
        current_note += ' **No entry-stop alternative qualified; baseline stops remain selected.**'
    candidate_note = f'Entry-stop comparison policy: **{diagnostic}**. '
    candidate_note += ('This is the selected retrospective research policy.' if diagnostic==selected and promoted else
                       '**Exploratory comparison only; not selected for the current V23 backtest.**')
    (output/'DAY_WISE_RESULTS.md').write_text(_day_table(current,'V23 entry-relative stops: current day-wise results',current_note),encoding='utf-8')
    (output/'CANDIDATE_DAY_WISE_RESULTS.md').write_text(_day_table(candidate,'V23 entry-relative stops: candidate day-wise comparison',candidate_note),encoding='utf-8')
    lines = [f'# V23 entry-relative stop study through {days[-1]}','',
        f'All **{int(full.loc[BASELINE,"trades"])} baseline fills across {len(days)} sessions** are retained. '
        f'The verified baseline net profit is **Rs {full.loc[BASELINE,"net_pnl_rs"]:,.2f}**. '
        'This study changes stop distances using information available at entry; it does not remove trades or increase their size.','',
        '**The baseline already uses the entry price:** its initial stop is 1.25% from entry, '
        'tightening to 1% after the native 120-minute interval. The new study tests whether a distance '
        'learned from earlier trade excursions, entry volatility or confirmed price structure can improve that rule.','']
    if promoted:
        lines += [f'**Qualified retrospective candidate: {selected}.** It passed fitting-period selection '
            'and the declared next-15, later-seven and full-history checks. This is a historical result, '
            'not a guarantee that future profit or every original winning trade will be preserved.','']
    else:
        lines += ['**No entry-stop alternative qualified for the current V23 backtest.** Baseline stops '
            'remain selected. No loss reduction is claimed for the current mode; unselected policies '
            'remain diagnostic comparisons.','']
    if config.get('rejection_reasons'):
        lines += ['Recorded qualification checks: '+'; '.join(map(str,config['rejection_reasons']))+'.','']
    lines += ['## All-session comparison','']+_metrics_table([(name,row) for name,row in full.iterrows()])
    lines += ['','Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners. '
        'Exactly flat trades count as neither wins nor losses; win rate uses all fills.','',
        '## Entry-relative stop logic','',
        '- LONG stop price = actual entry price × (1 − stop distance % / 100).',
        '- SHORT stop price = actual entry price × (1 + stop distance % / 100).',
        '- The fitted distance is established using permitted entry-time information. ATR-based '
        'models use confirmed volatility, and structure-based models use a previously completed '
        'price boundary; the price level is then calculated from the actual fill.',
        '- The initial distance cannot exceed the baseline 1.25%. The native 120-minute tightening '
        'takes the smaller of the initial distance and 1%, so an already tighter stop never widens.',
        '- For example, a LONG entry at Rs 100 with a 0.8% stop has a planned stop at Rs 99.20; '
        'the corresponding SHORT stop is Rs 100.80. The stop percentage is a price distance before costs. '
        'An opening gap can create a larger realised loss.','',
        'The exact fitted constants are preserved in [fit.json](fit.json), and the planned price level '
        'for each trade is available in [stop_levels.csv](stop_levels.csv). These files distinguish '
        'the rule from its realised execution price.','']
    for name,definition in config.get('definitions',{}).items():
        description = definition if isinstance(definition,str) else json.dumps(definition,sort_keys=True)
        lines.append(f'- **{name}:** {description}')
    lines += ['','## Fitting and period checks','',
        f'Fitting-period candidate: **{training}**. Current selected policy: **{selected}**. '
        f'Diagnostic comparison: **{diagnostic}**. Stop parameters are fitted on the first **44 sessions '
        'through 5 October 2026**. Candidate selection also uses that fitting period only, requiring '
        'net profit at least baseline, lower loss amount and no worse daily-close drawdown.','',
        'The selected candidate is then checked on the next **15 sessions, 6–27 October**, the final '
        '**seven sessions, 28 October–5 November**, and the full 66-session result. Those checks do '
        'not refit thresholds or select a different runner-up. All history has already been inspected '
        'in earlier research; these are retrospective stability checks, not untouched forward validation.','']
    wanted = list(dict.fromkeys([BASELINE,training,selected,diagnostic]))
    for period,label in [('fit_44','First 44: fitting'),('validation_15','Next 15: validation diagnostic'),
                         ('later_7','Final seven: later diagnostic'),('nov05','5 November only')]:
        part = overall.loc[overall.period.eq(period)&overall.policy.isin(wanted)]
        if not part.empty:
            lines += ['### '+label,'']+_metrics_table([(r.policy,r._asdict()) for r in part.itertuples(index=False)])+['']
    diagnostic_row=full.loc[diagnostic]
    if all(field in diagnostic_row and np.isfinite(diagnostic_row[field]) for field in ('average_trade_rs','median_trade_rs')):
        lines += [f'The diagnostic candidate averages Rs {diagnostic_row.average_trade_rs:,.2f} per trade, '
                  f'with median Rs {diagnostic_row.median_trade_rs:,.2f}.','']
    lines += ['## Effects on original winners and losers','',
        'Winner/loser labels below describe completed outcomes after replay. They are never used '
        'to decide the stop for an individual later trade. A smaller stop can reduce a loss while '
        'also stopping a trade that would eventually have recovered.','',
        '| Policy | Original winners worsened | Original losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|']
    for row in effects.itertuples(index=False):
        lines.append(f'| {row.policy} | {row.baseline_winners_harmed} | {row.baseline_losers_improved} | '
            f'{row.winners_turned_losses} | {row.losers_turned_wins} | {row.winner_pnl_change_rs:+,.2f} | '
            f'{row.loser_pnl_change_rs:+,.2f} |')
    better=int(candidate.pnl_change_rs.gt(1e-7).sum())
    worse=int(candidate.pnl_change_rs.lt(-1e-7).sum())
    lines += ['',f'The diagnostic policy improves net profit on {better} days and reduces it on {worse} days. '
        'Every day, including unchanged and zero-trade days, remains in the tables.','',
        '## Source, execution and cost limits','',
        '- The initial stop applies from the entry bar using the inherited conservative full-bar, '
        'stop-first convention. One-minute OHLC cannot prove whether that bar’s adverse extreme '
        'occurred before or after an intrabar entry. This ambiguity can make a tight-stop result '
        'conservative; it is not resolved by pretending to know the intrabar path.',
        '- An adverse gap after the entry bar fills at the actual opening price. Native targets and '
        'the 15:15 time exit remain unchanged. The native entry-bar open is not used as a stop fill.',
        '- Exposure remains Rs 500,000 per fill, based on Rs 100,000 capital at 5x, with Rs 250 '
        'modelled round-trip costs and unlimited concurrent capital. Initial stop distances do not '
        'include these costs. Extra adverse exit-slippage sensitivity is reported separately.',
        '- All 73 setup/direction pairs through 14:00 remain configured, with last confirmation '
        'at 14:01. Original pairs retain cap 3; the 39 new pairs retain cap 1 and their volume gate.',
        '- Results are retrospective and do not guarantee that only losses can be reduced. LIVE '
        'execution, scheduling, approvals and position management are unchanged.']
    for note in config.get('coverage_notes',[]):
        lines.append('- '+str(note))
    if (output/'mae_study.md').is_file():
        lines += ['','[Entry excursion study and ambiguity treatment](mae_study.md)']
    if (output/'feature_provenance.json').is_file():
        lines += ['','[Entry feature provenance](feature_provenance.json)']
    lines += ['','[Current day-wise results](DAY_WISE_RESULTS.md) · [Current CSV](day_wise_results.csv) · '
        '[Diagnostic candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · [Candidate CSV](candidate_day_wise_results.csv)','',
        '[All totals](overall.csv) · [All daily rows](daily.csv) · [Per-trade changes](trade_changes.csv) · '
        '[Trade effect summary](trade_effect_summary.csv) · [Planned stop levels](stop_levels.csv) · '
        '[Cost stress](cost_stress.csv) · [Native parity](baseline_parity.csv) · [Fitted parameters](fit.json) · '
        '[Configuration](config.json)','']
    (output/'REPORT.md').write_text('\n'.join(lines),encoding='utf-8')
