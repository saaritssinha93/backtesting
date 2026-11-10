"""Reconciled reports for the fixed-entry V23 loss-control experiments."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

BASELINE = 'baseline_v21_g7'
ADDITIVE = ('trades', 'wins', 'losses', 'net_pnl_rs', 'gross_loss_rs')
DAILY_FIELDS = ('day', 'trades', 'wins', 'losses', 'win_rate_pct', 'net_pnl_rs', 'gross_loss_rs')


def summarize_days(frame):
    result = {key: float(frame[key].sum()) for key in ADDITIVE}
    result['sessions'] = len(frame)
    result['win_rate_pct'] = 100 * result['wins'] / result['trades'] if result['trades'] else 0.
    equity = np.r_[0., frame.sort_values('day').net_pnl_rs.cumsum().to_numpy()]
    result['daily_close_drawdown_rs'] = float((np.maximum.accumulate(equity) - equity).max())
    return result


def compare_days(baseline, candidate):
    for label, frame in [('baseline', baseline), ('candidate', candidate)]:
        if frame.empty or frame.day.duplicated().any() or frame[list(DAILY_FIELDS)].isna().any().any():
            raise ValueError('Invalid daily inventory: ' + label)
        numeric = frame[list(DAILY_FIELDS)[1:]].to_numpy(float)
        if not np.isfinite(numeric).all():
            raise ValueError('Non-finite daily result: ' + label)
        if (frame.trades < frame.wins + frame.losses).any():
            raise ValueError('Win/loss counts exceed total trades')
        expected_rate = np.divide(100 * frame.wins.to_numpy(float), frame.trades.to_numpy(float),
                                  out=np.zeros(len(frame)), where=frame.trades.to_numpy() != 0)
        if not np.allclose(expected_rate, frame.win_rate_pct, atol=1e-7, rtol=0):
            raise ValueError('Invalid daily win rate')
    if set(baseline.day) != set(candidate.day):
        raise ValueError('Candidate does not cover every baseline session')
    merged = baseline[list(DAILY_FIELDS)].merge(candidate[list(DAILY_FIELDS)], on='day',
        suffixes=('_baseline', '_v23'), validate='one_to_one').sort_values('day').reset_index(drop=True)
    if not merged.trades_baseline.eq(merged.trades_v23).all():
        raise ValueError('Candidate changed the daily trade count')
    merged['pnl_change_rs'] = merged.net_pnl_rs_v23 - merged.net_pnl_rs_baseline
    merged['gross_loss_change_rs'] = merged.gross_loss_rs_v23 - merged.gross_loss_rs_baseline
    return merged


def _day_table(rows, title, note):
    totals = rows.select_dtypes(include='number').sum()
    rate = 100 * totals.wins_v23 / totals.trades_v23 if totals.trades_v23 else 0.
    lines = [f'# {title}', '', note, '',
        f'**{len(rows)} sessions, {rows.day.min()} through {rows.day.max()}**, including the full '
        'November 5 strategy session. Signals run through 14:00, final confirmation through 14:01, '
        'and execution through 15:15. All baseline entries are retained on every day.', '',
        'Net amounts include modelled costs of Rs 250 per trade on Rs 500,000 exposure '
        '(Rs 100,000 capital at 5x); concurrent capital is unlimited. Loss amount is the absolute '
        'sum of negative after-cost trade P&L before offsetting winners. All history has already '
        'been observed; this is retrospective research.', '',
        '| Day | Trades (both) | Baseline net (Rs) | V23 net (Rs) | Change (Rs) | V23 wins | V23 losses | V23 win rate | V23 loss amount (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|--:|--:|']
    for r in rows.itertuples(index=False):
        lines.append(f'| {r.day} | {int(r.trades_v23)} | {r.net_pnl_rs_baseline:,.2f} | '
            f'{r.net_pnl_rs_v23:,.2f} | {r.pnl_change_rs:+,.2f} | {int(r.wins_v23)} | '
            f'{int(r.losses_v23)} | {r.win_rate_pct_v23:.2f}% | {r.gross_loss_rs_v23:,.2f} |')
    lines += [f'| **Total** | **{int(totals.trades_v23)}** | **{totals.net_pnl_rs_baseline:,.2f}** | '
        f'**{totals.net_pnl_rs_v23:,.2f}** | **{totals.pnl_change_rs:+,.2f}** | '
        f'**{int(totals.wins_v23)}** | **{int(totals.losses_v23)}** | **{rate:.2f}%** | '
        f'**{totals.gross_loss_rs_v23:,.2f}** |', '',
        'Total win rate is weighted by trades. Zero-trade days remain listed. A positive aggregate '
        'change does not mean that every day or every original winner improved.', '']
    return '\n'.join(lines)


def _metrics_table(rows):
    lines = ['| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|']
    for label, row in rows:
        lines.append(f'| {label} | {int(row["trades"])} | {int(row["wins"])} | '
            f'{int(row["losses"])} | {row["win_rate_pct"]:.2f}% | {row["net_pnl_rs"]:,.2f} | '
            f'{row["gross_loss_rs"]:,.2f} | {row["daily_close_drawdown_rs"]:,.2f} |')
    return lines


def trade_effects(changes):
    """Describe effects using outcomes only after the causal backtest is over."""
    needed = ['policy', 'sid', 'day', 'baseline_pnl_rs', 'candidate_pnl_rs']
    if changes[needed].isna().any().any() or changes.duplicated(['policy', 'sid']).any():
        raise ValueError('Invalid per-trade comparison inventory')
    rows = []
    for name, part in changes.groupby('policy', sort=False):
        before, after = part.baseline_pnl_rs.astype(float), part.candidate_pnl_rs.astype(float)
        if not np.isfinite(np.c_[before, after]).all():
            raise ValueError('Non-finite trade comparison')
        delta = after - before
        rows.append(dict(policy=name, compared_trades=len(part),
            baseline_winners_harmed=int((before.gt(0) & delta.lt(-1e-7)).sum()),
            baseline_losers_improved=int((before.lt(0) & delta.gt(1e-7)).sum()),
            winners_turned_losses=int((before.gt(0) & after.lt(0)).sum()),
            losers_turned_wins=int((before.lt(0) & after.gt(0)).sum()),
            winner_pnl_change_rs=float(delta.loc[before.gt(0)].sum()),
            loser_pnl_change_rs=float(delta.loc[before.lt(0)].sum()),
            total_pnl_change_rs=float(delta.sum())))
    return pd.DataFrame(rows)


def write_reports(output, overall, daily, changes, config):
    """Required config: selected_policy, training_candidate, promotion_pass.

    comparison_candidate may name an unselected diagnostic alternative. Optional
    definitions, rejection_reasons, coverage_notes and historydays add context.
    """
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
        raise ValueError('Promoted policy must be the earlier-period candidate')
    required = {BASELINE, selected, training, diagnostic}
    if not required.issubset(set(daily.policy)):
        raise ValueError('Missing current or comparison policy')
    if daily.duplicated(['policy', 'day']).any() or overall.duplicated(['policy', 'period']).any():
        raise ValueError('Duplicate daily or overall policy row')
    baseline = daily.loc[daily.policy.eq(BASELINE)].sort_values('day')
    days = baseline.day.tolist()
    if config.get('historydays', days) != days:
        raise ValueError('Report history differs from declared history')
    full = overall.loc[overall.period.eq(f'full_{len(days)}')].set_index('policy')
    for name, frame in daily.groupby('policy'):
        compare_days(baseline, frame)
        if name not in full.index:
            raise ValueError('Missing full-history aggregate: ' + name)
        summary = summarize_days(frame)
        for field in (*ADDITIVE, 'win_rate_pct', 'daily_close_drawdown_rs'):
            if not np.isclose(summary[field], full.loc[name, field], atol=1e-5, rtol=0):
                raise ValueError('Daily/full reconciliation failed: ' + name + '/' + field)
    effects = trade_effects(changes)
    for row in effects.itertuples(index=False):
        if row.policy not in full.index or row.compared_trades != int(full.loc[BASELINE, 'trades']):
            raise ValueError('Trade effects do not retain every baseline trade')
        expected = full.loc[row.policy, 'net_pnl_rs'] - full.loc[BASELINE, 'net_pnl_rs']
        if not np.isclose(row.total_pnl_change_rs, expected, atol=1e-5, rtol=0):
            raise ValueError('Trade effect net difference does not reconcile')
    current = compare_days(baseline, daily.loc[daily.policy.eq(selected)])
    candidate = compare_days(baseline, daily.loc[daily.policy.eq(diagnostic)])
    output.mkdir(parents=True, exist_ok=True)
    current.to_csv(output / 'day_wise_results.csv', index=False)
    candidate.to_csv(output / 'candidate_day_wise_results.csv', index=False)
    effects.to_csv(output / 'trade_effect_summary.csv', index=False)
    current_note = f'Current backtest exit policy: **{selected}**. Entry count, prices, times and sizing remain fixed.'
    if not promoted:
        current_note += ' **No loss-control alternative qualified; baseline exits remain selected.**'
    candidate_note = f'Comparison policy: **{diagnostic}**. '
    candidate_note += ('This is the selected research policy.' if diagnostic == selected and promoted else
        '**Exploratory comparison only; not selected for the current V23 backtest.**')
    (output / 'DAY_WISE_RESULTS.md').write_text(_day_table(current,
        'V23 current day-wise results', current_note), encoding='utf-8')
    (output / 'CANDIDATE_DAY_WISE_RESULTS.md').write_text(_day_table(candidate,
        'V23 loss-control comparison by day', candidate_note), encoding='utf-8')
    lines = [f'# V23 indicator-based loss-control study through {days[-1]}', '',
        f'All **{int(full.loc[BASELINE, "trades"])} entries across {len(days)} sessions** are retained. '
        f'The verified baseline net P&L is **Rs {full.loc[BASELINE, "net_pnl_rs"]:,.2f}**. '
        'Rules manage exits after entry; they do not remove a selected stock or increase position size.', '']
    if promoted:
        lines += [f'**Qualified retrospective candidate: {selected}.** It passed the predeclared '
            'earlier-period selection and the later-period/full-history checks. This is an observed '
            'historical improvement, not a guarantee of future profit preservation.', '']
    else:
        lines += ['**No loss-control alternative qualified for the current V23 backtest.** '
            'Baseline exits remain selected, and no loss reduction is claimed for the current mode. '
            'The diagnostic candidate below remains exploratory.', '']
    if config.get('rejection_reasons'):
        lines += ['Recorded promotion checks: ' + '; '.join(map(str, config['rejection_reasons'])) + '.', '']
    lines += ['## All-session comparison', ''] + _metrics_table([(name, row) for name, row in full.iterrows()])
    lines += ['', 'Loss amount is the absolute sum of negative after-cost trade P&L before offsetting '
        'winners. Wins and losses exclude exactly flat trades; win rate uses all entries.', '',
        '## Selection and period checks', '',
        f'Earlier-period candidate: **{training}**. Current selection: **{selected}**. '
        f'Diagnostic comparison: **{diagnostic}**. Candidate parameters were fixed before these outcomes '
        'were calculated. Selection uses the earlier 59 sessions ending '
        f'{config.get("training_cutoff", config.get("earliercutoff", "2026-10-27"))}. '
        'The chosen rule is then checked over the later seven and full history without retuning. '
        'All history had already been observed in prior research, so these are retrospective '
        'stability checks, not untouched validation.', '']
    cohort_names = [('early_59', 'Earlier 59'), ('later_7', 'Later seven'), ('nov05', 'November 5'),
                    ('block_first22', 'First chronological block'), ('block_middle22', 'Middle chronological block'),
                    ('block_last22', 'Last chronological block')]
    check_policies = list(dict.fromkeys([BASELINE, training, selected, diagnostic]))
    for period, label in cohort_names:
        part = overall.loc[overall.period.eq(period) & overall.policy.isin(check_policies)]
        if not part.empty:
            lines += [f'### {label}', ''] + _metrics_table([(r.policy, r._asdict()) for r in part.itertuples(index=False)]) + ['']
    lines += ['## Changes to existing winners and losers', '',
        'These outcome labels are used only for this report, never to decide an exit. '
        'Preserving total net profit does not preserve every original winner or every day.', '',
        '| Policy | Original winners worsened | Original losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|']
    for r in effects.itertuples(index=False):
        lines.append(f'| {r.policy} | {r.baseline_winners_harmed} | {r.baseline_losers_improved} | '
            f'{r.winners_turned_losses} | {r.losers_turned_wins} | {r.winner_pnl_change_rs:+,.2f} | '
            f'{r.loser_pnl_change_rs:+,.2f} |')
    worse_days = int(candidate.pnl_change_rs.lt(-1e-7).sum())
    better_days = int(candidate.pnl_change_rs.gt(1e-7).sum())
    lines += ['', f'The diagnostic candidate changes net profit on {better_days} days for the better '
        f'and {worse_days} days for the worse. Unchanged days remain in the day-wise tables.', '',
        '## Indicator and rule definitions', '',
        'EMA9/EMA20, RSI14 and efficiency ratio ER10 here use each trade execution path starting '
        'after its completed confirmation. They are **confirmation-anchored indicators**, not '
        'full-session or overnight-continuous platform readings. Only completed prefix bars are used, '
        'and warm-up requirements must be met. ER measures price progress relative to the sum of '
        'absolute price changes; RSI and direction tests are normalized for long versus short trades.', '',
        'Maximum favorable/adverse excursions use observed post-entry bars. The entry-bar high/low '
        'cannot activate a new guard. Newly observed exit or stop decisions apply on the following '
        'bar; opening gaps can fill worse than a stop price. Native targets, staged stops, stop-first '
        'ties and the 15:15 time exit remain the reference conventions.', '']
    for name, definition in config.get('definitions', {}).items():
        description = definition if isinstance(definition, str) else json.dumps(definition, sort_keys=True)
        lines.append(f'- **{name}:** {description}')
    lines += ['', '## Coverage and cost assumptions', '',
        '- All 73 setup/direction pairs through the 14:00 signal remain configured. Original pairs '
        'retain cap 3; the 39 new pairs retain cap 1 and their volume gate. Final confirmation is '
        '14:01 and execution runs through 15:15.',
        '- Fixed exposure is Rs 500,000 per entry, Rs 100,000 capital at 5x, with Rs 250 modelled '
        'round-trip costs and unlimited concurrent capital. No position-size increase pays for a loss reduction.',
        '- Extra adverse exit slippage is reported separately in the cost-stress file. It is not '
        'silently added to, or omitted from, the baseline comparison.',
        '- One-minute OHLC cannot reveal the exact intrabar path or executable bid/ask prices. '
        'All results are retrospective; LIVE execution, approvals and scheduling are unchanged.']
    for note in config.get('coverage_notes', []):
        lines.append('- ' + str(note))
    lines += ['', '[Current day-wise results](DAY_WISE_RESULTS.md) · [Current day-wise CSV](day_wise_results.csv) · '
        '[Diagnostic day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · [Diagnostic day-wise CSV](candidate_day_wise_results.csv)', '',
        '[All totals](overall.csv) · [All daily results](daily.csv) · [Per-trade changes](trade_changes.csv) · '
        '[Trade effect summary](trade_effect_summary.csv) · [Cost stress](cost_stress.csv) · [Configuration](config.json)', '']
    (output / 'REPORT.md').write_text('\n'.join(lines), encoding='utf-8')
